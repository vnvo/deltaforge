//! The activation window (`docs/design/mysql-proof-seek.md`, "Layer 1: the
//! activation window").
//!
//! A row of a table with no positional proof at its position is proved
//! inside its dispatch, with the stream paused, so a burst of first touches
//! is many proofs in a row. Before such an event's position advances, the
//! window reads further events into a bounded lookahead queue - without
//! advancing the position or dispatching - and collects every newly
//! touched unproven table with its own start `A_i` (the executed set
//! before its transaction). It closes at the first of: its time bound, its
//! event or byte bound, a statement that can change a schema (any query
//! other than transaction control), or a heartbeat (the stream is idle).
//!
//! Then one batched capture locks every batch table's metadata in sorted
//! order and reads all their shapes between two positions; `B` is the
//! second. One request on the shared proof scanner answers every table over
//! its own `(A_i, B]`. Each table's capture and answer are kept as its
//! prepared proof, which the lazy baseline of its rows consumes in place of
//! a capture and a scan of its own ([`super::mysql_baseline::establish_at`]):
//! every table is then admitted or refused exactly as without a window.
//! The head event and the queue are dispatched in order afterwards
//! (`RunCtx::lookahead`), so output and checkpoints are unchanged.
//!
//! Outcomes are all or nothing and bounded: at most
//! [`CAPTURE_ATTEMPTS`] batched captures per window, never a per-table
//! capture; a capture that cannot lock fails the window closed with a
//! `mysql_capture_contention` incident, and a failed shared extension fails
//! it closed with the scanner's error. Either way nothing of the window is
//! dispatched and the checkpoint stays at the boundary before it.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::{Duration, Instant};

use deltaforge_core::{CheckpointOrder, SourceError};
use mysql_binlog_connector_rust::binlog_stream::BinlogStream;
use mysql_binlog_connector_rust::event::event_data::EventData;
use mysql_binlog_connector_rust::event::event_header::EventHeader;
use schema_registry::SourceSchema as _;
use tracing::{info, warn};

use super::mysql_binlog_scan::{
    BatchCaptureError, BatchCaptured, CLASSIFIER_VERSION, MAX_BATCH_TABLES,
    ProofError, ProofKind, ProofWork, ScanReport, ScanTag, capture_batch,
    trace_enabled, trace_proof,
};
use super::mysql_event::read_next_event;
use super::{LoopControl, MySqlCheckpoint, RunCtx};
use crate::durable_checkpoint::{
    WmPos, mysql_checkpoint_position, order_positions,
};
use crate::gtid_set::GtidSet;

/// Batched captures per window, in total.
pub(crate) const CAPTURE_ATTEMPTS: u32 = 3;

/// A table's proof, prepared by a window for its rows at `at`.
#[derive(Debug, Clone)]
pub(crate) struct Prepared {
    /// The start of the table's interval: its rows' evaluation position.
    pub at: WmPos,
    pub outcome: PreparedOutcome,
}

#[derive(Debug, Clone)]
pub(crate) enum PreparedOutcome {
    /// The window decided the table's lazy baseline is not established
    /// (the outcome as traced: `not_covered`, `relevant_ddl`,
    /// `not_decisive`, or `no_binding`); its rows go on as without one.
    Refused(&'static str),
    /// The table did not exist when the window captured.
    NoCapture,
}

/// What one window did (the qualification trace and the tests).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct WindowTrace {
    pub tables: usize,
    pub events: usize,
    pub bytes: u64,
    pub closed_by: &'static str,
    pub capture_attempts: u32,
    pub work: ProofWork,
}

/// Transaction control: neither a schema change nor a statement that could
/// be one.
fn is_transaction_control(sql: &str) -> bool {
    let s = sql.trim().trim_end_matches(';').trim();
    [
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "XA START",
        "XA END",
        "XA COMMIT",
    ]
    .iter()
    .any(|c| {
        s.len() >= c.len()
            && s[..c.len()].eq_ignore_ascii_case(c)
            && s[c.len()..]
                .chars()
                .next()
                .is_none_or(|ch| !ch.is_ascii_alphanumeric() && ch != '_')
    })
}

fn ends_transaction(sql: &str) -> bool {
    let s = sql.trim().trim_end_matches(';').trim();
    s.eq_ignore_ascii_case("COMMIT") || s.eq_ignore_ascii_case("ROLLBACK")
}

fn rows_table(data: &EventData) -> Option<u64> {
    match data {
        EventData::WriteRows(e) => Some(e.table_id),
        EventData::UpdateRows(e) => Some(e.table_id),
        EventData::DeleteRows(e) => Some(e.table_id),
        _ => None,
    }
}

/// The stream's position while the window reads ahead: transaction
/// boundaries with an exact running executed set, and the tables mapped so
/// far. Mirrors what dispatching would record, without touching `ctx`.
struct Ahead {
    file: String,
    /// The executed set (GTID mode) at the last boundary.
    done: Option<GtidSet>,
    open_gtid: Option<String>,
    /// The last boundary: the start of the next transaction's interval.
    boundary: MySqlCheckpoint,
    table_map: HashMap<u64, (String, String)>,
    /// A GTID that does not parse: the running set is unknown from here
    /// (dispatching that transaction fails closed).
    broken: bool,
}

impl Ahead {
    fn commit(&mut self, end_pos: u32) {
        if let (Some(done), Some(g)) =
            (self.done.as_mut(), self.open_gtid.take())
            && !done.insert(&g)
        {
            self.broken = true;
        }
        self.boundary = MySqlCheckpoint {
            file: self.file.clone(),
            pos: u64::from(end_pos),
            gtid_set: self.done.as_ref().map(|d| d.to_string()),
            lineage: None,
            snapshot_completed: None,
            snapshot_chain: None,
        };
    }
}

/// The tables of `data` (a rows event, or each one of a compressed
/// transaction) that `ctx` would prove at their rows, with the table maps
/// the event itself carries.
fn touched(
    ahead: &mut Ahead,
    header: &EventHeader,
    data: &EventData,
    out: &mut Vec<(String, String, MySqlCheckpoint)>,
) {
    match data {
        EventData::TransactionPayload(tp) => {
            for (_, d) in &tp.uncompressed_events {
                touched(ahead, header, d, out);
            }
        }
        EventData::TableMap(tm) => {
            ahead.table_map.insert(
                tm.table_id,
                (tm.database_name.clone(), tm.table_name.clone()),
            );
        }
        EventData::Gtid(g) => ahead.open_gtid = Some(g.gtid.clone()),
        EventData::Xid(_) => ahead.commit(header.next_event_position),
        EventData::Query(q) if ends_transaction(&q.query) => {
            ahead.commit(header.next_event_position)
        }
        d => {
            if let Some(id) = rows_table(d)
                && let Some((db, table)) = ahead.table_map.get(&id)
            {
                out.push((db.clone(), table.clone(), ahead.boundary.clone()));
            }
        }
    }
}

/// The tables a rows event (or each rows event of a compressed
/// transaction) touches, at `at`: names from `table_map`, or from the table
/// maps the compressed transaction itself carries.
fn head_tables(
    table_map: &HashMap<
        u64,
        mysql_binlog_connector_rust::event::table_map_event::TableMapEvent,
    >,
    data: &EventData,
    at: &MySqlCheckpoint,
    out: &mut Vec<(String, String, MySqlCheckpoint)>,
) {
    match data {
        EventData::TransactionPayload(tp) => {
            let mut inner: HashMap<u64, (&str, &str)> = HashMap::new();
            for (_, d) in &tp.uncompressed_events {
                match d {
                    EventData::TableMap(tm) => {
                        inner.insert(
                            tm.table_id,
                            (tm.database_name.as_str(), tm.table_name.as_str()),
                        );
                    }
                    d => {
                        if let Some(id) = rows_table(d)
                            && let Some((db, table)) =
                                inner.get(&id).copied().or_else(|| {
                                    table_map.get(&id).map(|tm| {
                                        (
                                            tm.database_name.as_str(),
                                            tm.table_name.as_str(),
                                        )
                                    })
                                })
                        {
                            out.push((
                                db.to_string(),
                                table.to_string(),
                                at.clone(),
                            ));
                        }
                    }
                }
            }
        }
        d => {
            if let Some(id) = rows_table(d)
                && let Some(tm) = table_map.get(&id)
            {
                out.push((
                    tm.database_name.clone(),
                    tm.table_name.clone(),
                    at.clone(),
                ));
            }
        }
    }
}

/// Run a window before `header`'s position advances, when its event is the
/// first rows of a table without a positional proof (see the module docs).
/// Returns with nothing done otherwise. On success, the read-ahead events
/// are queued in `ctx.lookahead` and the batch's proofs in
/// `ctx.prepared`; the caller then dispatches the head event. An error is
/// the loop's control: the stream was lost while reading ahead (the queue
/// is discarded and the position never passed the head event), or the
/// window failed closed.
pub(super) async fn maybe_window(
    ctx: &mut RunCtx,
    stream: &mut BinlogStream,
    header: &EventHeader,
    data: &EventData,
) -> Result<Option<WindowTrace>, LoopControl> {
    if !ctx.lookahead.is_empty() || ctx.transaction_open_without_eval() {
        return Ok(None);
    }
    let Some(a_head) = ctx.txn_eval_cp.clone() else {
        return Ok(None);
    };
    // The head event's tables, from the stream's own table map: no copy
    // of it unless a window opens (this runs for every rows event).
    let mut found = Vec::new();
    head_tables(&ctx.table_map, data, &a_head, &mut found);
    if found.is_empty() {
        return Ok(None);
    }
    let mut batch: BTreeMap<(String, String), MySqlCheckpoint> =
        BTreeMap::new();
    // First-touch order: the order the batch commits in.
    let mut order: Vec<(String, String)> = Vec::new();
    let mut full = admit(ctx, &mut batch, &mut order, found)
        .await
        .map_err(LoopControl::Fail)?;
    if batch.is_empty() {
        return Ok(None);
    }
    let mut ahead = Ahead {
        file: ctx.last_file.clone(),
        done: ctx.last_gtid.clone(),
        open_gtid: ctx.current_gtid.clone(),
        boundary: a_head.clone(),
        table_map: ctx
            .table_map
            .iter()
            .map(|(id, tm)| {
                (*id, (tm.database_name.clone(), tm.table_name.clone()))
            })
            .collect(),
        broken: false,
    };
    // The head event's own table maps and boundaries (a compressed
    // transaction carries them).
    touched(&mut ahead, header, data, &mut Vec::new());

    // Read ahead, without advancing the position or dispatching.
    let cfg = ctx.proofs.clone();
    let opened = Instant::now();
    let bound = Duration::from_millis(cfg.activation_window_ms);
    let mut queue: VecDeque<(EventHeader, EventData)> = VecDeque::new();
    let mut bytes = 0u64;
    let closed_by =
        loop {
            if full {
                break "tables";
            }
            if queue.len() >= cfg.activation_window_events {
                break "events";
            }
            if bytes >= cfg.activation_window_bytes {
                break "bytes";
            }
            let Some(left) = bound.checked_sub(opened.elapsed()) else {
                break "time";
            };
            // A cancelled read loses nothing (the vendored channel keeps what
            // it read), so the window's time bound can interrupt it.
            let (h, d) =
                match tokio::time::timeout(left, read_next_event(stream, ctx))
                    .await
                {
                    Err(_) => break "time",
                    Ok(Ok(ev)) => ev,
                    Ok(Err(control)) => return Err(control),
                };
            bytes += u64::from(h.event_length);
            let close = match &d {
                EventData::HeartBeat => Some("heartbeat"),
                EventData::Query(q) if !is_transaction_control(&q.query) => {
                    Some("statement")
                }
                EventData::Rotate(r) => {
                    ahead.file = r.binlog_filename.clone();
                    None
                }
                _ => None,
            };
            if close.is_none() {
                let mut found = Vec::new();
                touched(&mut ahead, &h, &d, &mut found);
                if !ahead.broken {
                    full = admit(ctx, &mut batch, &mut order, found)
                        .await
                        .map_err(LoopControl::Fail)?;
                }
            }
            queue.push_back((h, d));
            if let Some(why) = close {
                break why;
            }
            if ahead.broken {
                break "framing";
            }
        };
    let read = opened.elapsed();

    let server_uuid = ctx.expected_uuid().map_err(LoopControl::Fail)?;
    let lineage = ctx
        .registry_scope
        .current()
        .map_err(|e| LoopControl::Fail(e.into()))?
        .lineage()
        .lineage_hash
        .clone();
    let source_id = ctx.source_id.clone();
    let tag = ScanTag {
        source_id: &source_id,
        kind: ProofKind::Lazy,
    };

    // One batched capture, retried in the same sorted order.
    let tables: Vec<(String, String)> = batch.keys().cloned().collect();
    let mut attempts = 0;
    let t = Instant::now();
    let cap = loop {
        attempts += 1;
        match capture_batch(
            ctx.dsn.expose(),
            &server_uuid,
            &lineage,
            &tables,
            cfg.capture_lock_wait_secs,
            None,
        )
        .await
        {
            Ok(c) => break c,
            Err(BatchCaptureError::Failed(ProofError::OtherServer(found))) => {
                return Err(LoopControl::Fail(SourceError::Lineage {
                    details: format!(
                        "an activation capture reached another server ({found}), \
                         expected {server_uuid}"
                    )
                    .into(),
                }));
            }
            Err(e) if attempts < CAPTURE_ATTEMPTS => {
                warn!(source_id = %ctx.source_id, attempt = attempts, error = ?e, tables = tables.len(), "activation window: the batched capture failed; retrying");
            }
            Err(e) => {
                return Err(LoopControl::Fail(capture_failure(
                    &ctx.source_id,
                    &tables,
                    attempts,
                    &a_head,
                    e,
                )));
            }
        }
    };
    let captured = t.elapsed();

    // One scanner request: every table over its own (A_i, B].
    let t = Instant::now();
    let froms: Vec<MySqlCheckpoint> = batch
        .values()
        .map(|a| MySqlCheckpoint {
            lineage: Some(lineage.clone()),
            ..a.clone()
        })
        .collect();
    let head = MySqlCheckpoint {
        lineage: Some(lineage.clone()),
        ..a_head.clone()
    };
    let anchor = ctx.stream_anchor(&server_uuid, &lineage);
    let answers = match super::mysql_proof_scanner::shared_intervals(
        &ctx.proof_scanner,
        ctx.dsn.expose(),
        &source_id,
        &server_uuid,
        &lineage,
        &froms,
        &cap.position,
        anchor.as_ref(),
        Some(&head),
        &tag,
    )
    .await
    {
        Ok(a) => a,
        Err(ProofError::OtherServer(found)) => {
            return Err(LoopControl::Fail(SourceError::Lineage {
                details: format!(
                    "an activation scan reached another server ({found}), \
                     expected {server_uuid}"
                )
                .into(),
            }));
        }
        Err(e) => {
            return Err(LoopControl::Fail(SourceError::Schema {
                details: format!(
                    "the activation window's shared proof scan of {} table(s) \
                     failed ({e}); no table of the window was admitted, \
                     nothing was dispatched and the checkpoint stays before it",
                    tables.len()
                )
                .into(),
            }));
        }
    };
    let scanned = t.elapsed();

    let work = answers.first().map(|r| r.work).unwrap_or_default();
    let mut proved: HashMap<(String, String), (MySqlCheckpoint, ScanReport)> =
        batch
            .into_iter()
            .zip(answers)
            .map(|((k, at), r)| (k, (at, r)))
            .collect();
    let tables_in_order: Vec<_> = order
        .into_iter()
        .filter_map(|k| proved.remove(&k).map(|(at, r)| (k, at, r)))
        .collect();
    let t = Instant::now();
    admit_batch(ctx, &lineage, &cap, tables_in_order)
        .await
        .map_err(LoopControl::Fail)?;
    let committed = t.elapsed();
    ctx.lookahead = queue;
    let trace = WindowTrace {
        tables: tables.len(),
        events: ctx.lookahead.len(),
        bytes,
        closed_by,
        capture_attempts: attempts,
        work,
    };
    observe_window(&source_id, &trace, read, captured, scanned, committed);
    Ok(Some(trace))
}

/// Conflicting re-reads of an atomic batch before the window fails closed.
const BATCH_CONFLICT_RETRIES: usize = 3;
/// Retries of a batch whose outcome a storage error left unknown.
const BATCH_STORAGE_RETRIES: usize = 3;

/// One table's decision before the batch: refused (and why), or a
/// candidate whose shape is registered and whose baseline is proposed.
enum Decision {
    Refused(&'static str),
    Candidate { binds: Option<String>, s: WmPos },
}

/// Admit the window's tables in one atomic state-store batch
/// (`docs/design/mysql-proof-seek.md`, revision 4). Every table is decided
/// exactly as its lazy baseline decides (`establish_at`), from the same
/// capture, scan answer and timeline; then all registrations and baseline
/// records commit together, in first-touch order, or none do. A conflict
/// re-reads and decides again (bounded); any other failure fails the
/// window closed, with every table's cached timeline discarded. Caches
/// change only from the committed result.
async fn admit_batch(
    ctx: &mut RunCtx,
    lineage: &str,
    cap: &BatchCaptured,
    tables: Vec<((String, String), MySqlCheckpoint, ScanReport)>,
) -> Result<(), SourceError> {
    use super::mysql_activation::{
        ACTIVATION_NS, BARRIER_NS, Kind, Record, Stored, baseline_capture_id,
        encode_record, table_stream,
    };
    use super::mysql_baseline::{affects, barrier_to_bind};
    use super::mysql_selection::{
        PlannedVersions, admit_decisive_planned, timeline,
    };
    use storage::adapters::{RegistrationPlanning, RegistrationRequest};
    use storage::{
        AtomicBatch, BatchOp, CommitResult, Precondition, commit_atomic,
    };

    let source_id = ctx.source_id.clone();
    let tag = ScanTag {
        source_id: &source_id,
        kind: ProofKind::Lazy,
    };
    let lctn = ctx.lower_case_table_names;
    // A table gone before the capture has no shape: nothing to decide.
    let (tables, gone): (Vec<_>, Vec<_>) = tables
        .into_iter()
        .partition(|((db, table), _, _)| cap.table(db, table).is_some());
    for ((db, table), at, _) in gone {
        warn!(source_id = %ctx.source_id, %db, %table, "no lazy baseline: the table did not exist at the window's capture");
        trace_proof(&tag, &db, &table, None, None, lctn, "no_capture");
        if let Some(e) =
            mysql_checkpoint_position(&at.file, at.pos, at.gtid_set.as_deref())
        {
            ctx.prepared.insert(
                (db, table),
                Prepared {
                    at: e,
                    outcome: PreparedOutcome::NoCapture,
                },
            );
        }
    }
    let scope = ctx.registry_scope.current()?;
    let keys: Vec<_> = tables
        .iter()
        .map(|((db, table), _, _)| scope.key(db, table))
        .collect();
    let discard = |ctx: &mut RunCtx| {
        for k in &keys {
            ctx.selection.invalidate(k);
        }
    };
    let pos = |at: &MySqlCheckpoint| {
        mysql_checkpoint_position(&at.file, at.pos, at.gtid_set.as_deref())
            .ok_or_else(|| {
                SourceError::Other(anyhow::anyhow!(
                    "unparseable window position {at:?}"
                ))
            })
    };

    // Failover drift, for every table, before anything is decided: an
    // incident stops the whole window with nothing of it written.
    for (((db, table), at, _), key) in tables.iter().zip(&keys) {
        let t = timeline(ctx, key).await?;
        let all = t.all();
        let at_cp = MySqlCheckpoint {
            lineage: Some(lineage.to_string()),
            ..at.clone()
        };
        super::mysql_failover_drift::check_in_stream(
            ctx,
            db,
            table,
            super::mysql_failover_drift::FirstEvent::Rows {
                at: &pos(at)?,
                at_cp: &at_cp,
                timeline: &all,
            },
        )
        .await?;
    }

    let mut attempt = 0;
    loop {
        attempt += 1;
        // Decide, as `establish_at` does, up to the registration.
        let mut decisions = Vec::with_capacity(tables.len());
        let mut reqs = Vec::new();
        let mut req_of = Vec::with_capacity(tables.len());
        let mut timelines = Vec::with_capacity(tables.len());
        for (((db, table), at, report), key) in tables.iter().zip(&keys) {
            let t = timeline(ctx, key).await?;
            let e = pos(at)?;
            let tcap = cap.table(db, table).expect("captured tables only");
            let decision = match barrier_to_bind(&t.records, &t.barriers, &e) {
                Err(why) => {
                    info!(source_id = %ctx.source_id, %db, %table, why, "no lazy baseline");
                    Decision::Refused("no_binding")
                }
                Ok(binds) => {
                    let s = pos(&tcap.position)?;
                    if !super::mysql_binlog_scan::covers(&tcap, &e, &s) {
                        Decision::Refused("not_covered")
                    } else if report
                        .statements
                        .iter()
                        .any(|st| affects(st, db, table, lctn))
                    {
                        Decision::Refused("relevant_ddl")
                    } else {
                        Decision::Candidate { binds, s }
                    }
                }
            };
            req_of.push(match &decision {
                Decision::Candidate { .. } => {
                    let e_cp = MySqlCheckpoint {
                        lineage: Some(lineage.to_string()),
                        ..at.clone()
                    };
                    reqs.push(RegistrationRequest {
                        key: key.clone(),
                        hash: tcap.schema.fingerprint().to_string(),
                        schema_json: serde_json::to_value(&tcap.schema)
                            .map_err(|e| SourceError::Other(e.into()))?,
                        checkpoint: Some(
                            serde_json::to_vec(&e_cp)
                                .map_err(|e| SourceError::Other(e.into()))?,
                        ),
                    });
                    Some(reqs.len() - 1)
                }
                Decision::Refused(_) => None,
            });
            decisions.push(decision);
            timelines.push(t);
        }
        let registry = ctx.schema.registry();
        let planning = registry
            .plan_registrations(&reqs, chrono::Utc::now())
            .await
            .map_err(|e| {
                SourceError::Other(e.context("plan the window's registrations"))
            })?;
        let (reg_pre, regs) = match planning {
            RegistrationPlanning::Planned {
                preconditions,
                registrations,
            } => (preconditions, registrations),
            RegistrationPlanning::NeedsRepair(broken) => {
                for k in &broken {
                    registry.repair_key(k).await.map_err(SourceError::Other)?;
                }
                if attempt > BATCH_CONFLICT_RETRIES {
                    discard(ctx);
                    return Err(SourceError::Other(anyhow::anyhow!(
                        "the activation window's registrations still need repair after \
                         {attempt} attempts"
                    )));
                }
                continue;
            }
        };
        // Each candidate's baseline, decided against its planned version.
        let mut batch = AtomicBatch {
            preconditions: reg_pre,
            ops: Vec::new(),
        };
        let mut admitted: Vec<
            Option<(usize, Stored, super::mysql_selection::Timeline)>,
        > = Vec::with_capacity(tables.len());
        let mut reg_entry: Vec<Option<(usize, usize)>> = vec![None; regs.len()];
        let mut outcomes: Vec<&'static str> = Vec::with_capacity(tables.len());
        for (i, (((db, table), at, report), key)) in
            tables.iter().zip(&keys).enumerate()
        {
            let t = &timelines[i];
            let Decision::Candidate { binds, s } = &decisions[i] else {
                let Decision::Refused(why) = decisions[i] else {
                    unreachable!()
                };
                admitted.push(None);
                outcomes.push(why);
                continue;
            };
            let reg = &regs[req_of[i].expect("a candidate's request")];
            let base = batch.push_group(reg.ops.clone());
            if let Some(e) = reg.entry_op {
                reg_entry[req_of[i].unwrap()] = Some((base, base + e));
            }
            let stream = table_stream(key);
            let record = Record::new(
                pos(at)?,
                Kind::Baseline {
                    version: reg.version,
                    schema_hash: reg.hash.clone(),
                    to: s.clone(),
                    scan_digest: report.digest.clone(),
                    binds: binds.clone(),
                    classifier_version: CLASSIFIER_VERSION.to_string(),
                },
            );
            let id = baseline_capture_id(lineage, &stream, &record)
                .map_err(SourceError::Other)?;
            let proposed = Stored {
                capture_id: id.clone(),
                record: record.clone(),
            };
            let planned =
                PlannedVersions::from([(reg.version, reg.hash.clone())]);
            match admit_decisive_planned(
                registry,
                key,
                t,
                proposed.clone(),
                &pos(at)?,
                &planned,
            )
            .await
            {
                Ok(augmented) => {
                    batch.preconditions.push(Precondition::LogHead {
                        ns: ACTIVATION_NS.into(),
                        key: stream.clone(),
                        seq: t.table_head,
                    });
                    for (b, head) in &t.barrier_heads {
                        batch.preconditions.push(Precondition::LogHead {
                            ns: BARRIER_NS.into(),
                            key: b.clone(),
                            seq: *head,
                        });
                    }
                    let op = batch.ops.len();
                    batch.ops.push(BatchOp::LogAppendIfAbsent {
                        ns: ACTIVATION_NS.into(),
                        key: stream,
                        capture_id: id,
                        value: encode_record(&record)
                            .map_err(SourceError::Other)?,
                    });
                    admitted.push(Some((op, proposed, augmented)));
                    outcomes.push("proven");
                }
                Err(why) => {
                    info!(source_id = %ctx.source_id, %db, %table, why, "no lazy baseline");
                    admitted.push(None);
                    outcomes.push("not_decisive");
                }
            }
        }
        let committed =
            if batch.ops.is_empty() && batch.preconditions.is_empty() {
                CommitResult::Committed(Vec::new())
            } else {
                match commit_atomic(
                    ctx.registry_backend.as_ref(),
                    &batch,
                    BATCH_STORAGE_RETRIES,
                )
                .await
                {
                    Ok(c) => c,
                    Err(e) => {
                        discard(ctx);
                        return Err(SourceError::Other(e.context(
                            "commit the activation window's admissions",
                        )));
                    }
                }
            };
        let seqs = match committed {
            CommitResult::Committed(seqs) => seqs,
            CommitResult::Conflict(why) => {
                discard(ctx);
                if attempt > BATCH_CONFLICT_RETRIES {
                    return Err(SourceError::Other(anyhow::anyhow!(
                        "the activation window's admissions conflicted {attempt} times \
                         (last: {why}); nothing of the window was written"
                    )));
                }
                warn!(source_id = %ctx.source_id, attempt, why, "activation window: the batch conflicted; deciding again");
                continue;
            }
        };
        // Committed: the caches learn it, and only now.
        for (reg, entry) in regs.iter().zip(&reg_entry) {
            if let Some((_, op)) = entry {
                registry.admit_committed(
                    reg,
                    seqs[*op].expect("a committed log write"),
                );
            }
        }
        for (i, (((db, table), at, report), key)) in
            tables.iter().zip(&keys).enumerate()
        {
            let tcap = cap.table(db, table);
            match admitted[i].take() {
                Some((op, _, mut augmented)) => {
                    augmented.table_head =
                        seqs[op].expect("a committed log write");
                    ctx.selection
                        .install_committed(key, std::sync::Arc::new(augmented));
                }
                None => {
                    if let Ok(e) = pos(at) {
                        ctx.prepared.insert(
                            (db.clone(), table.clone()),
                            Prepared {
                                at: e,
                                outcome: PreparedOutcome::Refused(outcomes[i]),
                            },
                        );
                    }
                }
            }
            trace_proof(
                &tag,
                db,
                table,
                tcap.as_ref(),
                Some(report),
                lctn,
                outcomes[i],
            );
        }
        return Ok(());
    }
}

/// Why a window's batched capture finally failed, keeping its class:
/// exhausted lock waits or deadlocks are the `mysql_capture_contention`
/// incident, a connection failure a connection error, anything else a
/// schema error. Nothing of the window was dispatched.
fn capture_failure(
    source_id: &str,
    tables: &[(String, String)],
    attempts: u32,
    at: &MySqlCheckpoint,
    e: BatchCaptureError,
) -> SourceError {
    let unchanged = "nothing of the window was dispatched and the checkpoint \
                     stays before it";
    match e {
        BatchCaptureError::Contention { code, why } => {
            crate::incident_drafts::mysql_capture_contention(
                source_id, tables, attempts, code, at, &why,
            )
        }
        BatchCaptureError::Failed(ProofError::Open(why)) => {
            SourceError::Connect {
                details: format!(
                    "the activation window's capture of {} table(s) could not \
                 connect, {attempts} attempt(s) ({why}); {unchanged}",
                    tables.len()
                )
                .into(),
            }
        }
        BatchCaptureError::Failed(e) => SourceError::Schema {
            details: format!(
                "the activation window's capture of {} table(s) failed, \
                 {attempts} attempt(s) ({e}); {unchanged}",
                tables.len()
            )
            .into(),
        },
    }
}

/// Add each newly touched table that has no positional proof at its rows'
/// start (and is in scope) to the batch, at its first start. Returns
/// whether the batch is full ([`MAX_BATCH_TABLES`]): the window closes. A
/// table that does not fit (one transaction touching more new tables than
/// are left) stays out, counted and logged: its rows take the per-table
/// proof when they are dispatched.
async fn admit(
    ctx: &mut RunCtx,
    batch: &mut BTreeMap<(String, String), MySqlCheckpoint>,
    order: &mut Vec<(String, String)>,
    found: Vec<(String, String, MySqlCheckpoint)>,
) -> Result<bool, SourceError> {
    for (db, table, at) in found {
        let k = (db, table);
        if batch.contains_key(&k) || !ctx.allow.matches(&k.0, &k.1) {
            continue;
        }
        let Some(row) =
            mysql_checkpoint_position(&at.file, at.pos, at.gtid_set.as_deref())
        else {
            continue;
        };
        if !super::mysql_selection::unproven_at(ctx, &k.0, &k.1, &row).await? {
            continue;
        }
        if batch.len() >= MAX_BATCH_TABLES {
            warn!(source_id = %ctx.source_id, db = %k.0, table = %k.1, "activation window full: the table is proved on its own");
            metrics::counter!(
                "deltaforge_mysql_activation_window_overflow_total",
                "source" => ctx.source_id.clone(),
            )
            .increment(1);
            continue;
        }
        order.push(k.clone());
        batch.insert(k, at);
    }
    Ok(batch.len() >= MAX_BATCH_TABLES)
}

impl RunCtx {
    /// A transaction is open but its evaluation position is unknown: no
    /// lazy baseline can be prepared for it.
    fn transaction_open_without_eval(&self) -> bool {
        self.transaction_open() && self.txn_eval.is_none()
    }

    /// The window-prepared proof of `db.table` for rows at `at`, consumed.
    /// One prepared at another start is dropped.
    pub(crate) fn take_prepared(
        &mut self,
        db: &str,
        table: &str,
        at: &WmPos,
    ) -> Option<PreparedOutcome> {
        let p = self.prepared.remove(&(db.to_string(), table.to_string()))?;
        matches!(order_positions(&p.at, at), CheckpointOrder::Equal)
            .then_some(p.outcome)
    }
}

/// Bounded window metrics; the qualification record while the trace target
/// is on. Never fails.
fn observe_window(
    source_id: &str,
    t: &WindowTrace,
    read: Duration,
    captured: Duration,
    scanned: Duration,
    committed: Duration,
) {
    let source = [("source", source_id.to_string())];
    metrics::counter!(
        "deltaforge_mysql_activation_windows_total",
        "source" => source_id.to_string(),
        "closed_by" => t.closed_by,
    )
    .increment(1);
    metrics::histogram!("deltaforge_mysql_activation_window_tables", &source)
        .record(t.tables as f64);
    metrics::histogram!("deltaforge_mysql_activation_window_seconds", &source)
        .record((read + captured + scanned + committed).as_secs_f64());
    info!(
        source_id,
        tables = t.tables,
        events = t.events,
        closed_by = t.closed_by,
        capture_attempts = t.capture_attempts,
        "activation window"
    );
    if !trace_enabled() {
        return;
    }
    let record = serde_json::json!({
        "record": "window",
        "source": source_id,
        "tables": t.tables,
        "events": t.events,
        "bytes": t.bytes,
        "closed_by": t.closed_by,
        "capture_attempts": t.capture_attempts,
        "extensions_committed": t.work.extensions_committed,
        "positioned_attempts": t.work.positioned_attempts,
        "unanchored_attempts": t.work.unanchored_attempts,
        "stream_openings": t.work.stream_openings,
        "read_ms": read.as_secs_f64() * 1e3,
        "capture_ms": captured.as_secs_f64() * 1e3,
        "scan_ms": scanned.as_secs_f64() * 1e3,
        "commit_ms": committed.as_secs_f64() * 1e3,
    });
    tracing::info!(target: "deltaforge::proof_trace", "{record}");
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Exhausted lock attempts are the contention incident; other failures
    /// keep their connection or schema class (never the incident).
    #[test]
    fn capture_failures_keep_their_class() {
        let tables = [("d".to_string(), "t".to_string())];
        let at = MySqlCheckpoint {
            file: "binlog.000001".into(),
            pos: 4,
            gtid_set: Some("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5".into()),
            lineage: None,
            snapshot_completed: None,
            snapshot_chain: None,
        };
        let fail = |e| capture_failure("s", &tables, 3, &at, e);
        for code in [1205, 1213] {
            let e = fail(BatchCaptureError::Contention {
                code,
                why: "lock".into(),
            });
            assert_eq!(
                e.draft().map(|d| d.reason_code),
                Some(deltaforge_core::incident::ReasonCode::MysqlCaptureContention)
            );
        }
        let e = fail(BatchCaptureError::Failed(ProofError::Open(
            "refused".into(),
        )));
        assert!(matches!(e, SourceError::Connect { .. }), "{e:?}");
        assert!(e.draft().is_none());
        let e = fail(BatchCaptureError::Failed(ProofError::Capture(
            "1142".into(),
        )));
        assert!(matches!(e, SourceError::Schema { .. }), "{e:?}");
        assert!(e.draft().is_none());
    }

    #[test]
    fn transaction_control_never_closes_a_window() {
        for sql in ["BEGIN", "begin", "COMMIT", "ROLLBACK;", " XA START 'x'"] {
            assert!(is_transaction_control(sql), "{sql}");
        }
        for sql in [
            "ALTER TABLE t ADD c INT",
            "BEGINNING",
            "COMMITTED",
            "CREATE TABLE t (a INT)",
            "DROP TABLE t",
            "RENAME TABLE a TO b",
            "SAVEPOINT s",
            "SET @x = 1",
        ] {
            assert!(!is_transaction_control(sql), "{sql}");
        }
        assert!(ends_transaction("commit"));
        assert!(!ends_transaction("BEGIN"));
    }
}
