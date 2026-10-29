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
use super::mysql_window_speculation::{
    Collector, Covers, Governs, Held, NextWindow, Pending, SpecInputs,
    check_required, first_affecting, first_uncovered, reserved_rows_covered,
    statement_before,
};
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
    /// Which of the table's later rows the decision also covers.
    pub covers: Covers,
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
    /// Events read from the stream during the admission, for the next
    /// window.
    pub prefetched: usize,
    /// For a window resumed from its speculation: how many tables the
    /// speculation captured (its required tables, and any it discards).
    pub speculated: Option<usize>,
    /// What became of the next window during the admission.
    pub next: &'static str,
    /// The next window's speculative candidates.
    pub next_candidates: usize,
    /// Whether the queue was cut before a row no decision covers.
    pub cut: bool,
    /// The window's number, and when its admission committed (unix
    /// microseconds; measurement only).
    pub window_seq: u64,
    pub commit_unix_us: u128,
}

/// Transaction control: neither a schema change nor a statement that could
/// be one.
pub(crate) fn is_transaction_control(sql: &str) -> bool {
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
#[derive(Clone)]
pub(crate) struct Ahead {
    pub file: String,
    /// The executed set (GTID mode) at the last boundary.
    done: Option<GtidSet>,
    open_gtid: Option<String>,
    /// The last boundary: the start of the next transaction's interval.
    boundary: MySqlCheckpoint,
    table_map: HashMap<u64, (String, String)>,
    /// A GTID that does not parse: the running set is unknown from here
    /// (dispatching that transaction fails closed).
    pub broken: bool,
    /// The open transaction's stream anchor (its GTID event's start, with
    /// the executed set before it), as dispatching records it
    /// (`RunCtx::txn_anchor`).
    pub txn_anchor: Option<super::mysql_binlog_scan::Anchor>,
    /// Inside an explicit transaction (after `BEGIN`), and the server
    /// version: what decides whether a statement ends its transaction, as
    /// dispatching decides it (`RunCtx::in_explicit_txn`).
    explicit: bool,
    server_version: Option<u32>,
}

impl Ahead {
    fn commit(&mut self, end_pos: u32) {
        self.txn_anchor = None;
        self.explicit = false;
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
pub(crate) fn touched(
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
        EventData::Gtid(g) => {
            ahead.txn_anchor = ahead.done.as_ref().map(|set_before| {
                super::mysql_binlog_scan::Anchor {
                    server_uuid: String::new(),
                    lineage: String::new(),
                    file: ahead.file.clone(),
                    offset: u64::from(
                        header
                            .next_event_position
                            .saturating_sub(header.event_length),
                    ),
                    set_before: set_before.clone(),
                    next_gtid: Some(g.gtid.clone()),
                }
            });
            ahead.open_gtid = Some(g.gtid.clone());
        }
        EventData::Xid(_) => ahead.commit(header.next_event_position),
        // The dispatcher's own rule: a DDL completes its implicit
        // transaction before the next event's start is assigned.
        EventData::Query(q) => {
            match super::mysql_event::query_boundary(
                &q.query,
                ahead.explicit,
                ahead.server_version,
            ) {
                super::mysql_event::QueryBoundary::Opens => {
                    ahead.explicit = true
                }
                super::mysql_event::QueryBoundary::Completes => {
                    ahead.commit(header.next_event_position)
                }
                super::mysql_event::QueryBoundary::Continues => {}
            }
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

/// Known tables the cache no longer holds (evicted) return here: when a
/// head table's timeline is not cached, the stream's next events are held
/// (`ctx.prefetched`, dispatched before the stream is read again, windows
/// opening over them as over any held events), within the window's own
/// bounds, and every uncached table among them is loaded with the head's in
/// one bounded batch (`mysql_selection::load_timelines`), not one table at
/// a time. A read error ends the hold and is raised where the stream is
/// read next.
async fn prefetch_histories(
    ctx: &mut RunCtx,
    stream: &mut BinlogStream,
    head: &[(String, String, MySqlCheckpoint)],
) -> Result<(), LoopControl> {
    if ctx.pending.is_some()
        || !ctx.prefetched.is_empty()
        || ctx.prefetch_error.is_some()
    {
        return Ok(());
    }
    let Ok(scope) = ctx.registry_scope.current() else {
        return Ok(());
    };
    let max = ctx.selection.batch_room();
    let mut keys: Vec<storage::adapters::SchemaKey> = Vec::new();
    let add = |ctx: &mut RunCtx,
               keys: &mut Vec<storage::adapters::SchemaKey>,
               db: &str,
               table: &str| {
        let k = scope.key(db, table);
        if keys.len() < max
            && ctx.allow.matches(db, table)
            && !keys.contains(&k)
            && ctx.selection.timeline(&k.backend_key()).is_none()
        {
            keys.push(k);
        }
    };
    for (db, table, _) in head {
        add(ctx, &mut keys, db, table);
    }
    if keys.is_empty() {
        return Ok(());
    }
    let cfg = ctx.proofs.clone();
    let deadline =
        ctx.last_received + Duration::from_millis(cfg.activation_window_ms);
    let (mut events, mut bytes) = (0usize, 0u64);
    while events < cfg.activation_window_events
        && bytes < cfg.activation_window_bytes
        && keys.len() < max
    {
        let Some(left) = deadline
            .checked_duration_since(Instant::now())
            .filter(|left| !left.is_zero())
        else {
            break;
        };
        // A cancelled read loses nothing (the vendored channel keeps what
        // it read), so the time bound can interrupt it.
        let read = tokio::select! {
            biased;
            _ = ctx.cancel.cancelled() => return Err(LoopControl::Stop),
            r = tokio::time::timeout(left, stream.read()) => r,
        };
        match read {
            Err(_) => break,
            Ok(Err(e)) => {
                ctx.prefetch_error = Some(e);
                break;
            }
            Ok(Ok((h, d))) => {
                events += 1;
                bytes += u64::from(h.event_length);
                let mut tables = Vec::new();
                held_tables(&d, &mut tables);
                for (db, table) in tables {
                    add(ctx, &mut keys, &db, &table);
                }
                let seq = ctx.next_held_seq;
                ctx.next_held_seq += 1;
                ctx.prefetched.push_back((Instant::now(), seq, h, d));
            }
        }
    }
    super::mysql_selection::load_timelines(ctx, &keys).await;
    Ok(())
}

/// The tables an event's table maps name (a compressed transaction's too).
fn held_tables(d: &EventData, out: &mut Vec<(String, String)>) {
    match d {
        EventData::TableMap(tm) => {
            out.push((tm.database_name.clone(), tm.table_name.clone()))
        }
        EventData::TransactionPayload(tp) => {
            for (_, d) in &tp.uncompressed_events {
                held_tables(d, out);
            }
        }
        _ => {}
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
    // A speculative window whose head passed without a window opening
    // there is dropped unused (its task aborted).
    if let Some(p) = &ctx.pending
        && (ctx.last_seq == 0 || ctx.last_seq > p.head_seq)
    {
        ctx.pending = None;
    }
    let at_pending_head = ctx
        .pending
        .as_ref()
        .is_some_and(|p| p.head_seq == ctx.last_seq);
    // The speculative window is current now: its tables' timelines are
    // read in one snapshot before its required set is recomputed.
    if at_pending_head && let Ok(scope) = ctx.registry_scope.current() {
        let keys: Vec<_> = ctx
            .pending
            .as_ref()
            .map(|p| p.order.iter().map(|(db, t)| scope.key(db, t)).collect())
            .unwrap_or_default();
        super::mysql_selection::load_timelines(ctx, &keys).await;
    }
    let not_here = |ctx: &mut RunCtx| {
        if at_pending_head {
            ctx.pending = None;
        }
        Ok(None)
    };
    if !ctx.lookahead.is_empty() || ctx.transaction_open_without_eval() {
        return not_here(ctx);
    }
    let Some(a_head) = ctx.txn_eval_cp.clone() else {
        return not_here(ctx);
    };
    ctx.prune_prepared(&a_head);
    // The head event's tables, from the stream's own table map: no copy
    // of it unless a window opens (this runs for every rows event).
    let mut found = Vec::new();
    head_tables(&ctx.table_map, data, &a_head, &mut found);
    if found.is_empty() {
        return not_here(ctx);
    }
    prefetch_histories(ctx, stream, &found).await?;
    let mut batch: BTreeMap<(String, String), MySqlCheckpoint> =
        BTreeMap::new();
    // First-touch order: the order the batch commits in.
    let mut order: Vec<(String, String)> = Vec::new();
    let mut full = admit(ctx, &mut batch, &mut order, found)
        .await
        .map_err(LoopControl::Fail)?;
    // At a speculative window's head the required set is computed over
    // all its held events, not just the head's tables.
    if batch.is_empty() && !at_pending_head {
        return not_here(ctx);
    }
    let pending = if at_pending_head {
        ctx.pending.take()
    } else if ctx.pending.is_some() {
        return Err(LoopControl::Fail(invariant(
            "a window opened before the speculative window's head",
        )));
    } else {
        None
    };
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
        txn_anchor: ctx.txn_anchor.clone(),
        explicit: ctx.in_explicit_txn,
        server_version: ctx.binlog_server_version,
    };
    // The head event's own table maps and boundaries (a compressed
    // transaction carries them).
    touched(&mut ahead, header, data, &mut Vec::new());

    // Read ahead, without advancing the position or dispatching. The
    // time bound counts from when the head event was received: events
    // held during the previous window's admission were received within it
    // if their receipt is, and are taken without waiting. A window resumed
    // from its speculation reads exactly the held events it was collected
    // from.
    let cfg = ctx.proofs.clone();
    let started = Instant::now();
    let deadline =
        ctx.last_received + Duration::from_millis(cfg.activation_window_ms);
    let mut queue: VecDeque<(EventHeader, EventData)> = VecDeque::new();
    let mut receipts: Vec<(Instant, u64)> = Vec::new();
    // Every queued row of a table, with its transaction's start.
    let mut rows_at: Vec<(usize, (String, String), MySqlCheckpoint)> =
        Vec::new();
    let mut bytes = 0u64;
    let closed_by = loop {
        if let Some(p) = &pending {
            if ctx.last_seq == p.end_seq {
                break p.closed_by;
            }
            if !ctx.prefetched.front().is_some_and(|(_, seq, ..)| {
                *seq > ctx.last_seq && *seq <= p.end_seq
            }) {
                return Err(LoopControl::Fail(invariant(
                    "the speculative window's held events are not next",
                )));
            }
        } else {
            if full {
                break "tables";
            }
            if queue.len() >= cfg.activation_window_events {
                break "events";
            }
            if bytes >= cfg.activation_window_bytes {
                break "bytes";
            }
        }
        let received_in_time = pending.is_some()
            || ctx
                .prefetched
                .front()
                .is_some_and(|(received, ..)| *received <= deadline);
        let next = if received_in_time {
            read_next_event(stream, ctx).await
        } else {
            let Some(left) = deadline
                .checked_duration_since(Instant::now())
                .filter(|left| !left.is_zero())
            else {
                break "time";
            };
            // A cancelled read loses nothing (the vendored channel keeps
            // what it read), so the time bound can interrupt it.
            match tokio::time::timeout(left, read_next_event(stream, ctx)).await
            {
                Err(_) => break "time",
                Ok(next) => next,
            }
        };
        let (h, d) = match next {
            Ok(ev) => ev,
            Err(control) => return Err(control),
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
        // Every event passes through the tracker exactly once, the closing
        // one included (a closing DDL completes its transaction there).
        let mut found = Vec::new();
        touched(&mut ahead, &h, &d, &mut found);
        if close.is_none() {
            for (db, table, at) in &found {
                rows_at.push((
                    queue.len(),
                    (db.clone(), table.clone()),
                    at.clone(),
                ));
            }
            if !ahead.broken {
                full = admit(ctx, &mut batch, &mut order, found)
                    .await
                    .map_err(LoopControl::Fail)?;
            }
        }
        receipts.push((ctx.last_received, ctx.last_seq));
        queue.push_back((h, d));
        if let Some(why) = close {
            if pending.as_ref().is_some_and(|p| ctx.last_seq != p.end_seq) {
                return Err(LoopControl::Fail(invariant(
                    "the speculative window closed elsewhere than its own reading",
                )));
            }
            break why;
        }
        if ahead.broken {
            if pending.as_ref().is_some_and(|p| ctx.last_seq != p.end_seq) {
                return Err(LoopControl::Fail(invariant(
                    "the speculative window closed elsewhere than its own reading",
                )));
            }
            break "framing";
        }
    };
    let read = started.elapsed();
    if batch.is_empty() {
        // A speculative window none of whose tables is required (all
        // proven or governed): discarded unused, its events dispatched in
        // order with nothing admitted.
        drop(pending);
        ctx.lookahead = queue;
        return Ok(None);
    }

    let scope = ctx
        .registry_scope
        .current()
        .map_err(|e| LoopControl::Fail(e.into()))?;
    let lineage = scope.lineage().lineage_hash.clone();
    // The rule: the required set (this window's batch, from the unchanged
    // functions) is a subset of the speculation, each table at exactly the
    // same start; unused candidates are discarded. A difference is a
    // defect, never repaired by a fresh capture.
    let speculated = match &pending {
        Some(p) => {
            if p.lineage != lineage {
                return Err(LoopControl::Fail(invariant(&format!(
                    "the speculation was captured under lineage {}, the window \
                     decides under {lineage}",
                    p.lineage
                ))));
            }
            check_required(&batch, &p.batch)
                .map_err(|why| LoopControl::Fail(invariant(&why)))?;
            Some(p.batch.len())
        }
        None => None,
    };

    // The admission, with the stream read meanwhile: what arrives while it
    // captures, scans and commits is held (never dispatched or decoded
    // past its framing), and the next window is collected from it and
    // captured speculatively (nothing of `ctx` is held by that).
    let server_uuid = ctx.expected_uuid().map_err(LoopControl::Fail)?;
    let capture = CaptureScan {
        capture: ctx.capture.clone(),
        anchor: ctx.stream_anchor(&server_uuid, &lineage),
        server_uuid,
        lineage: lineage.clone(),
        source_id: ctx.source_id.clone(),
        lock_wait_secs: cfg.capture_lock_wait_secs,
        scanner: std::sync::Arc::clone(&ctx.proof_scanner),
    };
    let mut collector = Collector::new(
        ahead,
        ctx.allow.clone(),
        scope,
        ctx.selection.proven_keys(),
        batch.keys().cloned().collect(),
        &cfg,
        SpecInputs {
            capture: capture.clone(),
            lineage: lineage.clone(),
        },
    );
    for (received, seq, h, d) in &ctx.prefetched {
        collector.observe(*seq, *received, h, d);
    }
    let mut prefetch = Prefetch::new(ctx, &cfg);
    let cancel = ctx.cancel.clone();
    // Measurement only: the admission order, to trace each table's release.
    let admitted_order = trace_enabled().then(|| order.clone());
    let admission = async {
        match pending {
            Some(p) => admit_speculated(ctx, p, &lineage, &batch, order).await,
            None => {
                admit_window(ctx, &capture, &a_head, batch.clone(), order).await
            }
        }
    };
    let admitted = prefetch
        .during(stream, admission, &mut collector, &cancel)
        .await;
    let prefetched = prefetch.events.len();
    ctx.next_held_seq = prefetch.next_seq;
    ctx.prefetched.extend(prefetch.events);
    if prefetch.error.is_some() {
        ctx.prefetch_error = prefetch.error;
    }
    // A failed or cancelled admission drops the collector: its task is
    // aborted. Nothing of the window was dispatched.
    let Some(admitted) = admitted else {
        return Err(LoopControl::Stop);
    };
    let admitted = match admitted {
        // A stop that cancelled the window's capture (or the speculated
        // one) first: nothing of the window was dispatched and the stream
        // position is still before it, so the run stops as on any stop.
        Err(LoopControl::Fail(SourceError::Cancelled)) => {
            return Err(LoopControl::Stop);
        }
        other => other?,
    };
    let commit_unix_us = unix_us();
    ctx.window_seq += 1;
    if let Some(order) = admitted_order {
        ctx.release_watch.clear();
        for (rank, k) in order.into_iter().enumerate() {
            ctx.release_watch.insert(k, (ctx.window_seq, rank));
        }
    }

    // How each decision governs the table's other held rows: the queue is
    // cut before the first row no decision covers (a later window decides
    // it), and a speculation that cannot hold an uncovered row of a
    // reserved table is abandoned before it decides.
    let decisions: HashMap<(String, String), (MySqlCheckpoint, Governs)> =
        admitted
            .governs
            .iter()
            .map(|(k, a, g)| (k.clone(), (a.clone(), g.clone())))
            .collect();
    let cut = first_uncovered(&rows_at, &decisions);
    let candidates = collector.candidates();
    let covered = reserved_rows_covered(&collector.reserved_rows, &decisions);
    let (mut next, speculation) = collector.finish();
    if cut.is_some() || !covered {
        if next == NextWindow::Formed {
            next = NextWindow::Abandoned;
        }
        drop(speculation);
    } else {
        ctx.pending = speculation;
    }
    if let Some(i) = cut {
        let rest = queue.split_off(i);
        let rest_receipts = receipts.split_off(i);
        for ((h, d), (received, seq)) in
            rest.into_iter().zip(rest_receipts).rev()
        {
            ctx.prefetched.push_front((received, seq, h, d));
        }
    }
    ctx.lookahead = queue;
    let trace = WindowTrace {
        tables: admitted.tables,
        events: ctx.lookahead.len(),
        bytes,
        closed_by,
        capture_attempts: admitted.attempts,
        work: admitted.work,
        prefetched,
        speculated,
        next: next.as_str(),
        next_candidates: candidates,
        cut: cut.is_some(),
        window_seq: ctx.window_seq,
        commit_unix_us,
    };
    observe_window(&ctx.source_id, &trace, read, &admitted);
    Ok(Some(trace))
}

fn unix_us() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_micros())
}

/// [`dispatch_event`](super::mysql_event::dispatch_event), and while the
/// proof trace is on, a `release` record when a table of the last committed
/// window has its first row handed on (measurement only: nothing else
/// changes, and nothing is looked up while no window table is awaited).
pub(super) async fn dispatch_watched(
    ctx: &mut RunCtx,
    header: &EventHeader,
    data: EventData,
) -> deltaforge_core::SourceResult<()> {
    let watched = if ctx.release_watch.is_empty() {
        None
    } else {
        rows_table(&data)
            .and_then(|id| ctx.table_map.get(&id))
            .map(|tm| (tm.database_name.clone(), tm.table_name.clone()))
            .filter(|k| ctx.release_watch.contains_key(k))
    };
    super::mysql_event::dispatch_event(ctx, header, data).await?;
    if let Some(k) = watched
        && let Some((window, rank)) = ctx.release_watch.remove(&k)
    {
        let record = serde_json::json!({
            "record": "release",
            "source": ctx.source_id,
            "window": window,
            "rank": rank,
            "db": k.0,
            "table": k.1,
            "unix_us": unix_us(),
        });
        tracing::info!(target: "deltaforge::proof_trace", "{record}");
    }
    Ok(())
}

/// An internal invariant violation of the speculative window: it fails the
/// window closed.
fn invariant(why: &str) -> SourceError {
    SourceError::Schema {
        details: format!(
            "activation window invariant violated: {why}; nothing of the \
             window was admitted or dispatched and the checkpoint stays before it"
        )
        .into(),
    }
}

/// What reading the stream during an admission may hold: at most a
/// window's events and bytes in all (with what is already held); nothing
/// after a read error, which is kept and raised where the stream is read
/// next.
struct Prefetch {
    events: VecDeque<Held>,
    error: Option<mysql_binlog_connector_rust::binlog_error::BinlogError>,
    events_left: usize,
    bytes_left: u64,
    next_seq: u64,
}

impl Prefetch {
    fn new(ctx: &RunCtx, cfg: &deltaforge_config::MysqlProofCfg) -> Self {
        let held: u64 = ctx
            .prefetched
            .iter()
            .map(|(_, _, h, _)| u64::from(h.event_length))
            .sum();
        let stopped = ctx.prefetch_error.is_some();
        Self {
            events: VecDeque::new(),
            error: None,
            events_left: if stopped {
                0
            } else {
                cfg.activation_window_events
                    .saturating_sub(ctx.prefetched.len())
            },
            bytes_left: cfg.activation_window_bytes.saturating_sub(held),
            next_seq: ctx.next_held_seq,
        }
    }

    /// Run `admission` to completion, reading the stream while it runs and
    /// within bounds, and feeding the next window's collector. A read in
    /// flight when it completes is cancelled, losing nothing (the vendored
    /// channel keeps what it read). `None`: the run was stopped first (the
    /// admission is dropped).
    async fn during<T>(
        &mut self,
        stream: &mut BinlogStream,
        admission: impl std::future::Future<Output = T>,
        collector: &mut Collector,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> Option<T> {
        tokio::pin!(admission);
        loop {
            let reading = self.events_left > 0 && self.bytes_left > 0;
            let deadline = collector.deadline();
            let time_up = async move {
                match deadline {
                    Some(d) => tokio::time::sleep_until(d.into()).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                biased;
                done = &mut admission => return Some(done),
                _ = cancel.cancelled() => return None,
                _ = time_up => collector.time_up(),
                read = stream.read(), if reading => match read {
                    Ok((h, d)) => {
                        let received = Instant::now();
                        let seq = self.next_seq;
                        self.next_seq += 1;
                        collector.observe(seq, received, &h, &d);
                        self.events_left -= 1;
                        self.bytes_left =
                            self.bytes_left.saturating_sub(u64::from(h.event_length));
                        self.events.push_back((received, seq, h, d));
                    }
                    Err(e) => {
                        self.error = Some(e);
                        self.events_left = 0;
                    }
                },
            }
        }
    }
}

/// A window's admission as done: how long each part took, and how each
/// table's decision governs its other held rows.
struct Admitted {
    tables: usize,
    attempts: u32,
    work: ProofWork,
    captured: Duration,
    scanned: Duration,
    committed: Duration,
    capture: super::mysql_binlog_scan::CaptureTrace,
    batch: BatchStats,
    governs: Vec<((String, String), MySqlCheckpoint, Governs)>,
}

/// What one window's batched capture and shared scan need, owned (a
/// speculative window's task holds no part of `RunCtx`).
#[derive(Clone)]
pub(crate) struct CaptureScan {
    /// The run's capture context: its DSN and stop (an in-flight capture,
    /// also a speculative one, is cancelled).
    pub capture: super::mysql_binlog_scan::CaptureCtx,
    pub server_uuid: String,
    pub lineage: String,
    pub source_id: String,
    pub lock_wait_secs: u32,
    pub scanner: std::sync::Arc<super::mysql_proof_scanner::ProofScanner>,
    pub anchor: Option<super::mysql_binlog_scan::Anchor>,
}

/// A window's capture and every table's scan answer (in the sorted order
/// of its tables).
pub(crate) struct CapturedWindow {
    pub cap: BatchCaptured,
    pub answers: Vec<ScanReport>,
    pub attempts: u32,
    pub captured: Duration,
    pub scanned: Duration,
}

/// One batched capture of the sorted `tables` (retried in the same order,
/// at most [`CAPTURE_ATTEMPTS`] in all), then one scanner request answering
/// every table over its own `(from_i, B]`. Failures keep their class.
pub(crate) async fn capture_and_scan(
    cs: &CaptureScan,
    tables: &[(String, String)],
    froms: &[MySqlCheckpoint],
    head: &MySqlCheckpoint,
) -> Result<CapturedWindow, SourceError> {
    let tag = ScanTag {
        source_id: &cs.source_id,
        kind: ProofKind::Lazy,
    };
    let mut attempts = 0;
    let t = Instant::now();
    let cap = loop {
        attempts += 1;
        // A stop ends the window (`?`), never retried: nothing of it was
        // dispatched.
        match capture_batch(
            &cs.capture,
            &cs.server_uuid,
            &cs.lineage,
            tables,
            cs.lock_wait_secs,
            None,
        )
        .await?
        {
            Ok(c) => break c,
            Err(BatchCaptureError::Failed(ProofError::OtherServer(found))) => {
                return Err(SourceError::Lineage {
                    details: format!(
                        "an activation capture reached another server ({found}), \
                         expected {}",
                        cs.server_uuid
                    )
                    .into(),
                });
            }
            // Saturation already waited the capture's whole budget: not
            // retried in place; the restart retries.
            Err(
                e @ BatchCaptureError::Failed(ProofError::Saturated { .. }),
            ) => {
                return Err(capture_failure(
                    &cs.source_id,
                    tables,
                    attempts,
                    head,
                    e,
                ));
            }
            Err(e) if attempts < CAPTURE_ATTEMPTS => {
                warn!(source_id = %cs.source_id, attempt = attempts, error = ?e, tables = tables.len(), "activation window: the batched capture failed; retrying");
            }
            Err(e) => {
                return Err(capture_failure(
                    &cs.source_id,
                    tables,
                    attempts,
                    head,
                    e,
                ));
            }
        }
    };
    let captured = t.elapsed();
    let t = Instant::now();
    let answers = match super::mysql_proof_scanner::shared_intervals(
        &cs.scanner,
        cs.capture.dsn().current().expose(),
        &cs.source_id,
        &cs.server_uuid,
        &cs.lineage,
        froms,
        &cap.position,
        cs.anchor.as_ref(),
        Some(head),
        &tag,
    )
    .await
    {
        Ok(a) => a,
        Err(ProofError::OtherServer(found)) => {
            return Err(SourceError::Lineage {
                details: format!(
                    "an activation scan reached another server ({found}), \
                     expected {}",
                    cs.server_uuid
                )
                .into(),
            });
        }
        Err(e) => {
            return Err(SourceError::Schema {
                details: format!(
                    "the activation window's shared proof scan of {} table(s) \
                     failed ({e}); no table of the window was admitted, \
                     nothing was dispatched and the checkpoint stays before it",
                    tables.len()
                )
                .into(),
            });
        }
    };
    Ok(CapturedWindow {
        cap,
        answers,
        attempts,
        captured,
        scanned: t.elapsed(),
    })
}

/// A fresh window: its capture and scan, then the atomic admission of all
/// its tables.
async fn admit_window(
    ctx: &mut RunCtx,
    capture: &CaptureScan,
    a_head: &MySqlCheckpoint,
    batch: BTreeMap<(String, String), MySqlCheckpoint>,
    order: Vec<(String, String)>,
) -> Result<Admitted, LoopControl> {
    // First-touch order: the capture reads schemas in it and stops at the
    // budget (`mysql_window_budget`).
    let tables: Vec<(String, String)> = order.clone();
    let froms: Vec<MySqlCheckpoint> = tables
        .iter()
        .map(|k| MySqlCheckpoint {
            lineage: Some(capture.lineage.clone()),
            ..batch[k].clone()
        })
        .collect();
    let head = MySqlCheckpoint {
        lineage: Some(capture.lineage.clone()),
        ..a_head.clone()
    };
    let done = capture_and_scan(capture, &tables, &froms, &head)
        .await
        .map_err(LoopControl::Fail)?;
    decide_and_commit(ctx, &capture.lineage, done, &batch, &tables, order).await
}

/// A speculated window: its capture and scan (awaited: a failure
/// remembered by the task surfaces only now, in order), then the atomic
/// admission of its required tables. Every answer is used only for the
/// table and start it was obtained for.
async fn admit_speculated(
    ctx: &mut RunCtx,
    mut pending: Pending,
    lineage: &str,
    required: &BTreeMap<(String, String), MySqlCheckpoint>,
    order: Vec<(String, String)>,
) -> Result<Admitted, LoopControl> {
    let done = (&mut pending.task.0)
        .await
        .map_err(|e| {
            LoopControl::Fail(SourceError::Other(anyhow::anyhow!(
                "the speculative capture task ended: {e}"
            )))
        })?
        .map_err(LoopControl::Fail)?;
    let speculated: Vec<(String, String)> = pending.order.clone();
    decide_and_commit(ctx, lineage, done, required, &speculated, order).await
}

/// The decisions and the atomic commit of `required`'s tables, from a
/// capture and answers obtained for `captured` (sorted; a superset).
async fn decide_and_commit(
    ctx: &mut RunCtx,
    lineage: &str,
    mut done: CapturedWindow,
    required: &BTreeMap<(String, String), MySqlCheckpoint>,
    captured: &[(String, String)],
    order: Vec<(String, String)>,
) -> Result<Admitted, LoopControl> {
    let index: HashMap<&(String, String), usize> =
        captured.iter().enumerate().map(|(i, k)| (k, i)).collect();
    let work = done.answers.first().map(|r| r.work).unwrap_or_default();
    let mut tables_in_order = Vec::with_capacity(required.len());
    for k in order {
        let (Some(at), Some(i)) = (required.get(&k), index.get(&k)) else {
            return Err(LoopControl::Fail(invariant(&format!(
                "table {}.{} has no capture or answer",
                k.0, k.1
            ))));
        };
        let report = done.answers[*i].clone();
        tables_in_order.push((k, at.clone(), report));
    }
    let capture = std::mem::take(&mut done.cap.trace);
    let t = Instant::now();
    let (batch, governs) =
        admit_batch(ctx, lineage, &done.cap, tables_in_order)
            .await
            .map_err(LoopControl::Fail)?;
    let decided = governs
        .iter()
        .filter(|(_, _, g)| *g != Governs::Deferred)
        .count();
    Ok(Admitted {
        tables: decided,
        attempts: done.attempts,
        work,
        captured: done.captured,
        scanned: done.scanned,
        committed: t.elapsed(),
        capture,
        batch,
        governs,
    })
}

/// Conflicting re-reads of an atomic batch before the window fails closed.
const BATCH_CONFLICT_RETRIES: usize = 3;
/// Retries of a batch whose outcome a storage error left unknown.
const BATCH_STORAGE_RETRIES: usize = 3;

/// One table's decision before the batch: refused (and why), or a
/// candidate whose shape is registered and whose baseline is proposed.
enum Decision {
    Refused(&'static str, Covers),
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
) -> Result<
    (
        BatchStats,
        Vec<((String, String), MySqlCheckpoint, Governs)>,
    ),
    SourceError,
> {
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
        AtomicBatch, BatchOp, CommitResult, Precondition, commit_atomic_counted,
    };

    // One store holds the registry and the activation records, or no
    // batch can commit them together.
    if !std::sync::Arc::ptr_eq(
        ctx.schema.registry().backend(),
        &ctx.registry_backend,
    ) {
        return Err(SourceError::Other(anyhow::anyhow!(
            "the activation window needs the schema registry and the activation \
             records in one state store"
        )));
    }
    let source_id = ctx.source_id.clone();
    let tag = ScanTag {
        source_id: &source_id,
        kind: ProofKind::Lazy,
    };
    let lctn = ctx.lower_case_table_names;
    let scope = ctx.registry_scope.current()?;
    // The window's prefix (`mysql_window_budget`): its tables in first-touch
    // order, each with its complete cost, up to the first that would take
    // the plan over a cap. The rest are deferred: nothing of them is
    // decided, recorded or written, and their rows go back to the stream
    // for the next window, whose first table one of them is.
    let mut tables = tables;
    let deferred = {
        use super::mysql_window_budget::{Caps, Prefix, table_cost};
        let other_lineages = storage::adapters::source_lineage::load(
            &ctx.registry_backend,
            &ctx.tenant,
            &ctx.source_id,
        )
        .await
        .map_err(SourceError::Other)?
        .map_or(0, |r| r.prior_lineage_hashes.len() as u64);
        let mut prefix = Prefix::new(Caps::current());
        let mut cut = tables.len();
        for (i, ((db, table), at, _)) in tables.iter().enumerate() {
            // Past the capture's budget: unread, so the prefix ends here. The
            // window's first table is unread only when its own rows exceed
            // the store's item limit.
            if cap.deferred.contains(&(db.clone(), table.clone())) {
                if i == 0 {
                    let key = scope.key(db, table);
                    let too = storage::StateItemTooLarge {
                        size: cap.oversized.unwrap_or_default() as usize,
                        key_digest: storage::state_key_digest(
                            storage::adapters::schema_registry::V1_NS,
                            &key.backend_key(),
                        ),
                    };
                    return Err(crate::incident_drafts::state_item_too_large(
                        &ctx.source_id,
                        db,
                        table,
                        &too,
                        at,
                    ));
                }
                cut = i;
                break;
            }
            // A table gone before the capture holds nothing.
            let cost = match cap.schemas.get(&(db.clone(), table.clone())) {
                None => Default::default(),
                Some(schema) => {
                    let key = scope.key(db, table);
                    let t = timeline(ctx, &key).await?;
                    table_cost(
                        serde_json::to_vec(schema)
                            .map_err(|e| SourceError::Other(e.into()))?
                            .len() as u64,
                        &t,
                        other_lineages,
                        key.backend_key().len() as u64,
                    )
                    .map_err(SourceError::Other)?
                }
            };
            match prefix.offer(cost) {
                Ok(true) => {}
                Ok(false) => {
                    cut = i;
                    break;
                }
                Err(invalid) => {
                    return Err(crate::incident_drafts::mysql_table_too_large(
                        &ctx.source_id,
                        db,
                        table,
                        &invalid,
                        at,
                    ));
                }
            }
        }
        tables.split_off(cut)
    };
    if !deferred.is_empty() {
        metrics::counter!(
            "deltaforge_mysql_activation_window_deferred_total",
            "source" => ctx.source_id.clone(),
        )
        .increment(deferred.len() as u64);
    }
    // A table gone before the capture has no shape: nothing to decide.
    let (tables, gone): (Vec<_>, Vec<_>) = tables
        .into_iter()
        .partition(|((db, table), _, _)| cap.table(db, table).is_some());
    // Its rows are covered up to the statement that dropped or renamed it.
    // With no such statement in its proof answer the outcome is uncertain:
    // the window fails closed, before anything is written.
    let mut governs = Vec::with_capacity(tables.len() + gone.len());
    let mut gone_until = Vec::with_capacity(gone.len());
    for ((db, table), _, report) in &gone {
        let Some(d) = first_affecting(&report.statements, |st| {
            affects(st, db, table, lctn)
        }) else {
            return Err(SourceError::Schema {
                details: format!(
                    "table {db}.{table} did not exist at the activation window's \
                     capture and nothing in its proof interval drops or renames \
                     it: an uncertain outcome; nothing of the window was \
                     admitted or dispatched and the checkpoint stays before it"
                )
                .into(),
            });
        };
        gone_until.push(d.clone());
    }
    for (((db, table), at, _), d) in gone.into_iter().zip(gone_until) {
        warn!(source_id = %ctx.source_id, %db, %table, "no lazy baseline: the table did not exist at the window's capture");
        trace_proof(&tag, &db, &table, None, None, lctn, "no_capture");
        if let Some(e) =
            mysql_checkpoint_position(&at.file, at.pos, at.gtid_set.as_deref())
        {
            ctx.prepared.insert(
                (db.clone(), table.clone()),
                Prepared {
                    at: e,
                    covers: Covers::UntilStatement(d.clone()),
                    outcome: PreparedOutcome::NoCapture,
                },
            );
        }
        governs.push((
            (db, table),
            at,
            Governs::Refused(Covers::UntilStatement(d)),
        ));
    }
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

    // Failover drift, for every table, before anything is decided, from
    // the window's own capture and scan answers: an incident from any of
    // them stops the whole window with nothing written. Its outcomes
    // (registrations and markers) commit in the window's batch.
    let drift = {
        use super::mysql_failover_drift::{WindowTable, window_verdicts};
        let mut window_tables = Vec::with_capacity(tables.len());
        for (((db, table), at, report), key) in tables.iter().zip(&keys) {
            let t = timeline(ctx, key).await?;
            window_tables.push(WindowTable {
                db,
                table,
                at: pos(at)?,
                at_cp: MySqlCheckpoint {
                    lineage: Some(lineage.to_string()),
                    ..at.clone()
                },
                timeline: t.all(),
                capture: cap.table(db, table),
                report,
            });
        }
        window_verdicts(ctx, &window_tables).await?
    };
    // A table's drift registration (the shape at F, checkpoint F), if any.
    let drift_shape = |i: usize| match &drift.verdicts[i] {
        Some(super::mysql_failover_drift::DriftVerdict::Marker {
            register: Some(shape),
            ..
        }) => Some(shape.as_ref()),
        _ => None,
    };

    let mut attempt = 0;
    let mut stats = BatchStats::default();
    loop {
        attempt += 1;
        // Decide, as `establish_at` does, up to the registration.
        let mut decisions = Vec::with_capacity(tables.len());
        let mut reqs = Vec::new();
        let mut req_of = Vec::with_capacity(tables.len());
        let mut drift_req_of: Vec<Option<usize>> =
            Vec::with_capacity(tables.len());
        let mut timelines = Vec::with_capacity(tables.len());
        for (((db, table), at, report), key) in tables.iter().zip(&keys) {
            let t = timeline(ctx, key).await?;
            let e = pos(at)?;
            let tcap = cap.table(db, table).expect("captured tables only");
            let barriers: Vec<Stored> = t.barriers().cloned().collect();
            let decision = match barrier_to_bind(&t.records, &barriers, &e) {
                Err(why) => {
                    info!(source_id = %ctx.source_id, %db, %table, why, "no lazy baseline");
                    Decision::Refused("no_binding", Covers::Transaction)
                }
                Ok(binds) => {
                    let s = pos(&tcap.position)?;
                    if !super::mysql_binlog_scan::covers(&tcap, &e, &s) {
                        // The capture does not contain the rows: the stream
                        // and the capture disagree. Uncertain.
                        discard(ctx);
                        return Err(SourceError::Schema {
                            details: format!(
                                "the activation window's capture of {db}.{table} does \
                                 not contain its rows' position: an uncertain \
                                 outcome; nothing of the window was admitted or \
                                 dispatched and the checkpoint stays before it"
                            )
                            .into(),
                        });
                    } else if let Some(d) =
                        first_affecting(&report.statements, |st| {
                            affects(st, db, table, lctn)
                        })
                    {
                        Decision::Refused(
                            "relevant_ddl",
                            Covers::UntilStatement(d.clone()),
                        )
                    } else {
                        Decision::Candidate { binds, s }
                    }
                }
            };
            // One registration per table: drift's (checkpoint F) when it
            // registers, the window's own otherwise; both only when they
            // are the same registration.
            let i = decisions.len();
            let drift_req = match (drift_shape(i), &drift.anchor) {
                (Some(shape), Some(anchor)) => {
                    if matches!(decision, Decision::Candidate { .. })
                        && (shape.fingerprint() != tcap.schema.fingerprint()
                            || serde_json::to_value(shape).ok()
                                != serde_json::to_value(&tcap.schema).ok())
                    {
                        discard(ctx);
                        return Err(SourceError::Schema {
                            details: format!(
                                "table {db}.{table}: its failover drift \
                                 registration and its window registration \
                                 differ; nothing of the window was admitted \
                                 or dispatched and the checkpoint stays \
                                 before it"
                            )
                            .into(),
                        });
                    }
                    reqs.push(super::mysql_failover_drift::drift_request(
                        key, anchor, shape,
                    )?);
                    Some(reqs.len() - 1)
                }
                _ => None,
            };
            drift_req_of.push(drift_req);
            req_of.push(match &decision {
                Decision::Candidate { .. } if drift_req.is_some() => drift_req,
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
                Decision::Refused(..) => None,
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
        let mut outcomes: Vec<(&'static str, Covers)> =
            Vec::with_capacity(tables.len());
        // Each table's ops, to name the table of an item the store refuses.
        let mut op_table: Vec<usize> = Vec::new();
        for (i, (((db, table), at, report), key)) in
            tables.iter().zip(&keys).enumerate()
        {
            let t = &timelines[i];
            let Decision::Candidate { binds, s } = &decisions[i] else {
                let Decision::Refused(why, covers) = &decisions[i] else {
                    unreachable!()
                };
                // Refused here, registered by its drift check.
                if let Some(r) = drift_req_of[i] {
                    let reg = &regs[r];
                    let base = batch.push_group(reg.ops.clone());
                    if let Some(e) = reg.entry_op {
                        reg_entry[r] = Some((base, base + e));
                    }
                    op_table.resize(batch.ops.len(), i);
                }
                admitted.push(None);
                outcomes.push((why, covers.clone()));
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
                    for (b, head) in &t.barrier_heads() {
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
                    outcomes.push(("proven", Covers::Transaction));
                    op_table.resize(batch.ops.len(), i);
                }
                Err(why) => {
                    info!(source_id = %ctx.source_id, %db, %table, why, "no lazy baseline");
                    admitted.push(None);
                    outcomes.push(("not_decisive", Covers::Transaction));
                    op_table.resize(batch.ops.len(), i);
                }
            }
        }
        // Each drift outcome's marker, naming its planned registration.
        if let Some(anchor) = &drift.anchor {
            for (i, v) in drift.verdicts.iter().enumerate() {
                if let Some(
                    super::mysql_failover_drift::DriftVerdict::Marker {
                        key,
                        previous,
                        outcome,
                        ..
                    },
                ) = v
                {
                    let current = drift_req_of[i]
                        .map(|r| (regs[r].version, regs[r].hash.clone()));
                    batch.ops.push(super::mysql_failover_drift::marker_op(
                        key,
                        anchor,
                        previous.clone(),
                        current,
                        *outcome,
                    )?);
                    op_table.resize(batch.ops.len(), i);
                }
            }
        }
        let committed =
            if batch.ops.is_empty() && batch.preconditions.is_empty() {
                CommitResult::Committed(Vec::new())
            } else {
                match commit_atomic_counted(
                    ctx.registry_backend.as_ref(),
                    &batch,
                    BATCH_STORAGE_RETRIES,
                    &mut stats.applies,
                )
                .await
                {
                    Ok(c) => c,
                    Err(e) => {
                        discard(ctx);
                        // An item over the store's limit: the table it
                        // belongs to, classified.
                        if let Some(too) = e.chain().find_map(|c| {
                            c.downcast_ref::<storage::StateItemTooLarge>()
                        }) && let Some(i) =
                            refused_table(&batch, &op_table, &too.key_digest)
                        {
                            let ((db, table), at, _) = &tables[i];
                            return Err(
                                crate::incident_drafts::state_item_too_large(
                                    &ctx.source_id,
                                    db,
                                    table,
                                    too,
                                    at,
                                ),
                            );
                        }
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
                stats.conflicts += 1;
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
            let (why, covers) = outcomes[i].clone();
            match admitted[i].take() {
                Some((op, _, mut augmented)) => {
                    augmented.table_head =
                        seqs[op].expect("a committed log write");
                    ctx.selection
                        .install_committed(key, std::sync::Arc::new(augmented));
                    governs.push((
                        (db.clone(), table.clone()),
                        at.clone(),
                        Governs::Admitted,
                    ));
                }
                None => {
                    if let Ok(e) = pos(at) {
                        ctx.prepared.insert(
                            (db.clone(), table.clone()),
                            Prepared {
                                at: e,
                                covers: covers.clone(),
                                outcome: PreparedOutcome::Refused(why),
                            },
                        );
                    }
                    governs.push((
                        (db.clone(), table.clone()),
                        at.clone(),
                        Governs::Refused(covers),
                    ));
                }
            }
            trace_proof(
                &tag,
                db,
                table,
                tcap.as_ref(),
                Some(report),
                lctn,
                why,
            );
        }
        // Drift-checked for this run once committed.
        let checked: Vec<(String, String)> =
            tables.iter().map(|(k, ..)| k.clone()).collect();
        super::mysql_failover_drift::window_checked(ctx, &checked).await?;
        for ((db, table), at, _) in &deferred {
            trace_proof(&tag, db, table, None, None, lctn, "deferred");
            governs.push((
                (db.clone(), table.clone()),
                at.clone(),
                Governs::Deferred,
            ));
        }
        return Ok((stats, governs));
    }
}

/// The table (index into the window's decided tables) whose op writes the
/// item with `key_digest`.
fn refused_table(
    batch: &storage::AtomicBatch,
    op_table: &[usize],
    key_digest: &str,
) -> Option<usize> {
    batch.ops.iter().zip(op_table).find_map(|(op, i)| {
        let (storage::BatchOp::LogAppendIfAbsent { ns, key, .. }
        | storage::BatchOp::KvPut { ns, key, .. }) = op
        else {
            return None;
        };
        (storage::state_key_digest(ns, key) == key_digest).then_some(*i)
    })
}

/// How a window's atomic batch went: the conflicts that made it decide
/// again, and every `apply_batch` call (one when nothing was retried).
#[derive(Debug, Clone, Copy, Default)]
struct BatchStats {
    conflicts: usize,
    applies: usize,
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
        BatchCaptureError::Failed(ProofError::Saturated { scope, .. }) => {
            crate::incident_drafts::mysql_control_plane_saturated(
                source_id,
                "activation window",
                tables,
                scope,
                at,
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
        // An earlier window's decision covers these rows: they go on as it
        // decided, never captured again.
        if ctx.take_prepared(&k.0, &k.1, &row, &at).is_some() {
            continue;
        }
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

    /// The window's decision of `db.table`, if it covers rows whose
    /// transaction starts at `at` (`boundary`): its decided transaction, or
    /// every row before the statement that ends its coverage. Kept for
    /// every row it covers; pruned once the stream is past them.
    pub(crate) fn take_prepared(
        &self,
        db: &str,
        table: &str,
        at: &WmPos,
        boundary: &MySqlCheckpoint,
    ) -> Option<PreparedOutcome> {
        let p = self.prepared.get(&(db.to_string(), table.to_string()))?;
        let order = order_positions(&p.at, at);
        let covered = match &p.covers {
            Covers::Transaction => matches!(order, CheckpointOrder::Equal),
            Covers::UntilStatement(d) => {
                matches!(
                    order,
                    CheckpointOrder::Before | CheckpointOrder::Equal
                ) && statement_before(d, boundary) == Some(false)
            }
        };
        covered.then(|| p.outcome.clone())
    }

    /// Drop the decisions whose rows are all behind the transaction
    /// starting at `head`.
    pub(crate) fn prune_prepared(&mut self, head: &MySqlCheckpoint) {
        let Some(h) = mysql_checkpoint_position(
            &head.file,
            head.pos,
            head.gtid_set.as_deref(),
        ) else {
            return;
        };
        self.prepared.retain(|_, p| match &p.covers {
            Covers::Transaction => {
                !matches!(order_positions(&p.at, &h), CheckpointOrder::Before)
            }
            Covers::UntilStatement(d) => {
                statement_before(d, head) != Some(true)
            }
        });
    }
}

/// Bounded window metrics; the qualification record while the trace target
/// is on. Never fails.
fn observe_window(
    source_id: &str,
    t: &WindowTrace,
    read: Duration,
    admitted: &Admitted,
) {
    let Admitted {
        captured,
        scanned,
        committed,
        capture,
        batch,
        ..
    } = admitted;
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
        .record((read + *captured + *scanned + *committed).as_secs_f64());
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
        "prefetched": t.prefetched,
        "speculated": t.speculated,
        "next": t.next,
        "next_candidates": t.next_candidates,
        "cut": t.cut,
        "window_seq": t.window_seq,
        "commit_unix_us": t.commit_unix_us,
        "extensions_committed": t.work.extensions_committed,
        "positioned_attempts": t.work.positioned_attempts,
        "unanchored_attempts": t.work.unanchored_attempts,
        "stream_openings": t.work.stream_openings,
        "read_ms": read.as_secs_f64() * 1e3,
        "capture_ms": captured.as_secs_f64() * 1e3,
        "scan_ms": scanned.as_secs_f64() * 1e3,
        "commit_ms": committed.as_secs_f64() * 1e3,
        "batch_conflicts": batch.conflicts,
        "batch_applies": batch.applies,
        "capture_connect_ms": capture.connect.as_secs_f64() * 1e3,
        "capture_lock_ms": capture.lock_wait.as_secs_f64() * 1e3,
        "capture_schema_ms": capture.schema.as_secs_f64() * 1e3,
        "capture_positions_ms": capture.positions.as_secs_f64() * 1e3,
    });
    tracing::info!(target: "deltaforge::proof_trace", "{record}");
}

#[cfg(test)]
mod tests {
    use super::*;

    mod batch {
        //! The window's atomic admission on a real (in-memory) store.
        use super::*;
        use crate::gtid_set::GtidSet;
        use crate::mysql::mysql_activation::{
            ACTIVATION_NS, Kind, Record, Stored, append, read_table_head,
            table_stream,
        };
        use crate::mysql::mysql_ddl_attribution::{DdlEffect, TableName};
        use crate::mysql::mysql_schema_loader::MySqlSchemaLoader;
        use crate::mysql::mysql_table_schema::{MySqlColumn, MySqlTableSchema};
        use storage::ArcStorageBackend;
        use tokio::sync::mpsc;

        const U: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562";

        async fn ctx_on(backend: ArcStorageBackend) -> RunCtx {
            let (tx, _rx) = mpsc::channel(8);
            let mut ctx = crate::mysql::mysql_event::tests::make_runctx(tx);
            ctx.registry_backend = backend.clone();
            let registry = storage::DurableSchemaRegistry::new(backend.clone())
                .await
                .unwrap();
            ctx.schema = MySqlSchemaLoader::new(
                "mysql://fake",
                registry,
                &ctx.tenant,
                ctx.registry_scope.clone(),
            );
            crate::registry_scope::establish_scope(
                &backend,
                &ctx.registry_scope,
                &ctx.tenant,
                &ctx.source_id,
                storage::adapters::LineageDescriptor::mysql(U).unwrap(),
            )
            .await
            .unwrap();
            ctx.last_gtid = Some(GtidSet::parse(&format!("{U}:1-50")).unwrap());
            ctx
        }

        fn at(k: u64) -> MySqlCheckpoint {
            MySqlCheckpoint {
                file: "mysql-bin.000001".into(),
                pos: 4 + 100 * k,
                gtid_set: Some(format!("{U}:1-{k}")),
                lineage: None,
                snapshot_completed: None,
                snapshot_chain: None,
            }
        }

        fn wm(k: u64) -> WmPos {
            let p = at(k);
            mysql_checkpoint_position(&p.file, p.pos, p.gtid_set.as_deref())
                .unwrap()
        }

        fn schema(cols: &[&str]) -> MySqlTableSchema {
            MySqlTableSchema::new(
                cols.iter()
                    .enumerate()
                    .map(|(i, c)| {
                        MySqlColumn::new(*c, "int", "int", true, i as u32 + 1)
                    })
                    .collect(),
            )
        }

        fn capture(ctx: &RunCtx, tables: &[&str]) -> BatchCaptured {
            let lineage = ctx
                .registry_scope
                .current()
                .unwrap()
                .lineage()
                .lineage_hash
                .clone();
            let mut from = at(50);
            from.lineage = Some(lineage.clone());
            let mut position = at(60);
            position.lineage = Some(lineage);
            BatchCaptured {
                from,
                position,
                schemas: tables
                    .iter()
                    .map(|t| {
                        (("d".to_string(), t.to_string()), schema(&["id", t]))
                    })
                    .collect(),
                missing: Vec::new(),
                deferred: Vec::new(),
                oversized: None,
                trace: Default::default(),
            }
        }

        fn report(
            statements: Vec<super::super::super::mysql_binlog_scan::Statement>,
        ) -> ScanReport {
            ScanReport {
                statements,
                events: 0,
                bytes: 0,
                duration: Duration::ZERO,
                digest: "digest".into(),
                trace: Default::default(),
                frontier: None,
                session_anchor: None,
                positioned_at: None,
                work: Default::default(),
            }
        }

        fn entry(
            t: &str,
            k: u64,
        ) -> ((String, String), MySqlCheckpoint, ScanReport) {
            (("d".into(), t.into()), at(k), report(Vec::new()))
        }

        fn key(ctx: &RunCtx, t: &str) -> storage::adapters::SchemaKey {
            ctx.registry_scope.current().unwrap().key("d", t)
        }

        /// (registry entry sequence, activation record sequence) of a table.
        async fn seqs(ctx: &RunCtx, t: &str) -> (Option<u64>, Option<u64>) {
            let k = key(ctx, t);
            let reg = ctx
                .schema
                .registry()
                .get_latest(&k)
                .await
                .unwrap()
                .map(|v| v.sequence);
            let (records, head) = read_table_head(
                &ctx.registry_backend,
                ctx.schema.registry(),
                &k,
            )
            .await
            .unwrap();
            (reg, (!records.is_empty()).then_some(head))
        }

        async fn admit(
            ctx: &mut RunCtx,
            tables: &[&str],
            entries: Vec<((String, String), MySqlCheckpoint, ScanReport)>,
        ) -> Result<
            (
                BatchStats,
                Vec<((String, String), MySqlCheckpoint, Governs)>,
            ),
            SourceError,
        > {
            let lineage = ctx
                .registry_scope
                .current()
                .unwrap()
                .lineage()
                .lineage_hash
                .clone();
            let cap = capture(ctx, tables);
            admit_batch(ctx, &lineage, &cap, entries).await
        }

        mod timelines {
            //! The batched timeline read against the per-table reads.
            use super::*;
            use crate::mysql::mysql_activation::{
                BARRIER_NS, BarrierScope, barrier_stream, read_barriers_head,
                table_stream,
            };
            use crate::mysql::mysql_history::{self, Loaded};
            use storage::StorageBackend as _;
            use storage::adapters::test_util::FaultBackend;

            fn key_in(
                ctx: &RunCtx,
                db: &str,
                t: &str,
            ) -> storage::adapters::SchemaKey {
                ctx.registry_scope.current().unwrap().key(db, t)
            }

            /// Tables in two databases: empty streams, several table
            /// histories, a lineage barrier and a barrier of each database.
            async fn fixture(
                ctx: &RunCtx,
            ) -> Vec<storage::adapters::SchemaKey> {
                let mut keys = Vec::new();
                for (i, db) in ["d", "e"].iter().enumerate() {
                    for t in 0..15 {
                        let k = key_in(ctx, db, &format!("t{t:02}"));
                        for r in 0..(t % 4) {
                            append(
                                &ctx.registry_backend,
                                ctx.schema.registry(),
                                &k,
                                ACTIVATION_NS,
                                &table_stream(&k),
                                &format!("ddl-{r}"),
                                &Record::new(wm(10 + r as u64), Kind::Ddl),
                            )
                            .await
                            .unwrap();
                        }
                        keys.push(k);
                    }
                    let scope = BarrierScope::Database { db: db.to_string() };
                    append(
                        &ctx.registry_backend,
                        ctx.schema.registry(),
                        &keys[i * 15],
                        BARRIER_NS,
                        &barrier_stream(&keys[i * 15], &scope),
                        "db-barrier",
                        &Record::new(wm(7), Kind::Barrier { scope }),
                    )
                    .await
                    .unwrap();
                }
                append(
                    &ctx.registry_backend,
                    ctx.schema.registry(),
                    &keys[0],
                    BARRIER_NS,
                    &barrier_stream(&keys[0], &BarrierScope::Lineage),
                    "lineage-barrier",
                    &Record::new(
                        wm(3),
                        Kind::Barrier {
                            scope: BarrierScope::Lineage,
                        },
                    ),
                )
                .await
                .unwrap();
                keys
            }

            /// Within its bounds a batched history read takes the barrier
            /// streams, then the tables in the order asked while their
            /// streams fit whole (the first one not exempt); the rest are
            /// left (read when reached), and nothing is usable when the
            /// barriers alone do not fit.
            #[tokio::test]
            async fn a_bounded_history_read_stops_in_the_order_asked() {
                let ctx =
                    ctx_on(std::sync::Arc::new(FaultBackend::new())).await;
                let keys = fixture(&ctx).await;
                let mut asked = keys.clone();
                asked.reverse();
                let none = std::collections::HashMap::new();
                // The bytes of the barriers and of each asked table's stream.
                let streams: Vec<(String, String)> = asked
                    .iter()
                    .map(|k| (ACTIVATION_NS.to_string(), table_stream(k)))
                    .collect();
                let per_table = ctx
                    .registry_backend
                    .log_read_streams(&streams)
                    .await
                    .unwrap();
                let mut barrier_streams: Vec<(String, String)> = Vec::new();
                for k in &asked {
                    for b in crate::mysql::mysql_activation::barrier_streams(k)
                    {
                        let s = (BARRIER_NS.to_string(), b);
                        if !barrier_streams.contains(&s) {
                            barrier_streams.push(s);
                        }
                    }
                }
                let barrier_bytes: u64 = ctx
                    .registry_backend
                    .log_read_streams(&barrier_streams)
                    .await
                    .unwrap()
                    .iter()
                    .flatten()
                    .map(storage::log_entry_bytes)
                    .sum();
                let bytes = |n: usize| -> u64 {
                    barrier_bytes
                        + per_table[..n]
                            .iter()
                            .flatten()
                            .map(storage::log_entry_bytes)
                            .sum::<u64>()
                };
                for n in [1, 7, asked.len()] {
                    let got = mysql_history::read(
                        &ctx.registry_backend,
                        ctx.schema.registry(),
                        &asked,
                        &none,
                        bytes(n),
                        u64::MAX,
                    )
                    .await
                    .unwrap();
                    // An empty stream after the last that fits still fits.
                    let fits: Vec<String> = asked
                        .iter()
                        .enumerate()
                        .take_while(|(i, _)| bytes(i + 1) <= bytes(n))
                        .map(|(_, k)| k.backend_key())
                        .collect();
                    let mut keys: Vec<String> =
                        got.tables.keys().cloned().collect();
                    keys.sort();
                    let mut want = fits.clone();
                    want.sort();
                    assert_eq!(keys, want, "bounds for {n} tables");
                    assert_eq!(got.cut.is_some(), fits.len() < asked.len());
                }
                let short = mysql_history::read(
                    &ctx.registry_backend,
                    ctx.schema.registry(),
                    &asked,
                    &none,
                    barrier_bytes.saturating_sub(1),
                    u64::MAX,
                )
                .await
                .unwrap();
                assert!(barrier_bytes == 0 || short.tables.is_empty());
            }

            #[tokio::test]
            async fn the_batch_equals_the_per_table_reads_reading_each_stream_once()
             {
                let faults = std::sync::Arc::new(FaultBackend::new());
                let ctx = ctx_on(faults.clone()).await;
                let keys = fixture(&ctx).await;
                let mut asked = keys.clone();
                asked.extend(keys[..5].iter().cloned());
                asked.reverse();
                faults.stream_reads.lock().unwrap().clear();
                let mut have = std::collections::HashMap::new();
                let got = mysql_history::read_each(
                    &ctx.registry_backend,
                    ctx.schema.registry(),
                    &asked,
                    &mut have,
                    |_| true,
                )
                .await
                .unwrap();
                let reads = faults.stream_reads.lock().unwrap().clone();
                assert_eq!(
                    faults
                        .batched_stream_reads
                        .load(std::sync::atomic::Ordering::SeqCst),
                    1
                );
                // 30 table streams, one lineage barrier, two database barriers.
                assert_eq!(reads.len(), 33, "{reads:?}");
                let unique: std::collections::BTreeSet<_> =
                    reads.iter().collect();
                assert_eq!(unique.len(), reads.len(), "no stream read twice");
                // The shared barriers first (canonical order), then each
                // table once, in the order asked.
                let (barriers, tables) = reads.split_at(3);
                assert!(barriers.iter().all(|(ns, _)| ns == BARRIER_NS));
                assert!(
                    barriers.windows(2).all(|w| w[0] <= w[1]),
                    "canonical order"
                );
                let mut want: Vec<(String, String)> = Vec::new();
                for k in &asked {
                    let s = (ACTIVATION_NS.to_string(), table_stream(k));
                    if !want.contains(&s) {
                        want.push(s);
                    }
                }
                assert_eq!(tables, &want[..], "the order asked");
                assert_eq!(got.len(), asked.len());
                for k in &keys {
                    let i = asked.iter().position(|a| a == k).unwrap();
                    let Loaded::Ok { table, streams, .. } = &got[i] else {
                        panic!("{:?}", got[i]);
                    };
                    let (records, head) = (table.records.clone(), table.head);
                    let barriers: Vec<_> = streams
                        .iter()
                        .flat_map(|s| s.records.iter().cloned())
                        .collect();
                    let heads: Vec<_> = streams
                        .iter()
                        .map(|s| (s.stream.clone(), s.head))
                        .collect();
                    let one = read_table_head(
                        &ctx.registry_backend,
                        ctx.schema.registry(),
                        k,
                    )
                    .await
                    .unwrap();
                    let b = read_barriers_head(&ctx.registry_backend, k)
                        .await
                        .unwrap();
                    assert_eq!((records, head), one, "{}", k.backend_key());
                    assert_eq!((barriers, heads), b, "{}", k.backend_key());
                }
            }

            /// A malformed record fails its table exactly as its own read
            /// does; the other tables are unaffected. A failed store read
            /// gives nothing, and installs nothing.
            #[tokio::test]
            async fn failures_keep_their_classification_and_nothing_partial_is_used()
             {
                let faults = std::sync::Arc::new(FaultBackend::new());
                let mut ctx = ctx_on(faults.clone()).await;
                let keys = fixture(&ctx).await;
                faults
                    .log_append_if_absent(
                        ACTIVATION_NS,
                        &table_stream(&keys[3]),
                        "bad",
                        b"{not json",
                    )
                    .await
                    .unwrap();
                let got = mysql_history::read_each(
                    &ctx.registry_backend,
                    ctx.schema.registry(),
                    &keys,
                    &mut std::collections::HashMap::new(),
                    |_| true,
                )
                .await
                .unwrap();
                let own = read_table_head(
                    &ctx.registry_backend,
                    ctx.schema.registry(),
                    &keys[3],
                )
                .await
                .unwrap_err();
                let Loaded::Invalid(e) = &got[3] else {
                    panic!("{:?}", got[3]);
                };
                assert_eq!(e.to_string(), own.to_string());
                assert!(matches!(got[4], Loaded::Ok { .. }));
                super::super::super::super::mysql_selection::load_timelines(
                    &mut ctx, &keys,
                )
                .await;
                assert!(
                    ctx.selection.timeline(&keys[3].backend_key()).is_none()
                );
                assert!(
                    ctx.selection.timeline(&keys[4].backend_key()).is_some()
                );

                let mut fresh = ctx_on(faults.clone()).await;
                faults
                    .fail_log_read_meta
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                assert!(
                    mysql_history::read_each(
                        &fresh.registry_backend,
                        fresh.schema.registry(),
                        &keys,
                        &mut std::collections::HashMap::new(),
                        |_| true,
                    )
                    .await
                    .is_err()
                );
                super::super::super::super::mysql_selection::load_timelines(
                    &mut fresh, &keys,
                )
                .await;
                for k in &keys {
                    assert!(
                        fresh.selection.timeline(&k.backend_key()).is_none()
                    );
                }
            }

            /// Installed timelines equal what `timeline` reads, and a batch
            /// takes at most its bound of tables.
            #[tokio::test]
            async fn installed_timelines_equal_individual_reads_up_to_the_bound()
             {
                let backend: ArcStorageBackend =
                    std::sync::Arc::new(storage::MemoryStorageBackend::new());
                let mut ctx = ctx_on(backend.clone()).await;
                let keys = fixture(&ctx).await;
                super::super::super::super::mysql_selection::load_timelines(
                    &mut ctx, &keys,
                )
                .await;
                let mut other = ctx_on(backend.clone()).await;
                for k in &keys {
                    let a = ctx.selection.timeline(&k.backend_key()).unwrap();
                    let b =
                        super::super::super::super::mysql_selection::timeline(
                            &mut other, k,
                        )
                        .await
                        .unwrap();
                    assert_eq!(*a, *b, "{}", k.backend_key());
                }
                let max = mysql_history::BATCH_MAX_TABLES;
                let many: Vec<_> = (0..=max)
                    .map(|i| key_in(&ctx, "d", &format!("m{i:04}")))
                    .collect();
                let none = std::collections::HashMap::new();
                let (backend, registry) =
                    (&ctx.registry_backend, ctx.schema.registry());
                let full = mysql_history::read(
                    backend,
                    registry,
                    &many[..max],
                    &none,
                    u64::MAX,
                    u64::MAX,
                )
                .await
                .unwrap();
                assert_eq!(full.tables.len(), max);
                assert!(
                    mysql_history::read(
                        backend,
                        registry,
                        &many,
                        &none,
                        u64::MAX,
                        u64::MAX
                    )
                    .await
                    .is_err()
                );
            }

            /// A change between the batched read and the commit is caught by
            /// the batch's preconditions: decided again from a fresh read.
            #[tokio::test]
            async fn a_change_after_the_batched_read_is_caught_at_commit() {
                let backend: ArcStorageBackend =
                    std::sync::Arc::new(storage::MemoryStorageBackend::new());
                let mut ctx = ctx_on(backend).await;
                let k1 = key(&ctx, "t1");
                super::super::super::super::mysql_selection::load_timelines(
                    &mut ctx,
                    std::slice::from_ref(&k1),
                )
                .await;
                assert!(ctx.selection.timeline(&k1.backend_key()).is_some());
                append(
                    &ctx.registry_backend,
                    ctx.schema.registry(),
                    &k1,
                    ACTIVATION_NS,
                    &table_stream(&k1),
                    "unknown-at-2",
                    &Record::new(wm(2), Kind::Unknown),
                )
                .await
                .unwrap();
                let (stats, _) = admit(&mut ctx, &["t1"], vec![entry("t1", 5)])
                    .await
                    .unwrap();
                assert_eq!(stats.conflicts, 1, "the stale timeline conflicted");
            }
        }

        /// Every table admitted, its registry entry then its baseline, in
        /// first-touch order (not name order), one contiguous range; each
        /// table's cached timeline is the committed one and proves its rows.
        #[tokio::test]
        async fn tables_commit_together_in_first_touch_order() {
            let mut ctx = ctx_on(std::sync::Arc::new(
                storage::MemoryStorageBackend::new(),
            ))
            .await;
            admit(
                &mut ctx,
                &["t1", "t2"],
                vec![entry("t2", 3), entry("t1", 5)],
            )
            .await
            .unwrap();
            let (r2, a2) = seqs(&ctx, "t2").await;
            let (r1, a1) = seqs(&ctx, "t1").await;
            let all = [r2.unwrap(), a2.unwrap(), r1.unwrap(), a1.unwrap()];
            assert!(
                all.windows(2).all(|w| w[1] == w[0] + 1),
                "contiguous, first touch first: {all:?}"
            );
            for (t, k) in [("t2", 3), ("t1", 5)] {
                let kt = key(&ctx, t);
                let cached = super::super::super::mysql_selection::timeline(
                    &mut ctx, &kt,
                )
                .await
                .unwrap();
                assert!(matches!(
                    crate::mysql::mysql_activation::select(
                        &cached.all(),
                        &wm(k)
                    ),
                    crate::mysql::mysql_activation::Selection::Proven {
                        version: 1
                    }
                ));
            }
            assert!(ctx.prepared.is_empty());
        }

        async fn stored(
            c: &RunCtx,
        ) -> (Vec<Stored>, i32, String, serde_json::Value, Option<Vec<u8>>)
        {
            let k = key(c, "t1");
            let records = crate::mysql::mysql_activation::read_stream(
                &c.registry_backend,
                ACTIVATION_NS,
                &table_stream(&k),
            )
            .await
            .unwrap();
            let v = c.schema.registry().get_latest(&k).await.unwrap().unwrap();
            (records, v.version, v.hash, v.schema_json, v.checkpoint)
        }

        /// The batch stores what the one-table steps store (`establish_at`:
        /// register the captured shape, then append the decided baseline):
        /// the same version and hash, and the same record bytes and identity.
        #[tokio::test]
        async fn the_batch_stores_what_one_table_admission_stores() {
            let mut batch = ctx_on(std::sync::Arc::new(
                storage::MemoryStorageBackend::new(),
            ))
            .await;
            admit(&mut batch, &["t1"], vec![entry("t1", 5)])
                .await
                .unwrap();
            let one = ctx_on(std::sync::Arc::new(
                storage::MemoryStorageBackend::new(),
            ))
            .await;
            let k = key(&one, "t1");
            let e_cp = MySqlCheckpoint {
                lineage: Some(k.lineage_hash.clone()),
                ..at(5)
            };
            let (version, hash) = one
                .schema
                .register_captured(
                    "d",
                    "t1",
                    &schema(&["id", "t1"]),
                    &serde_json::to_vec(&e_cp).unwrap(),
                )
                .await
                .unwrap();
            let record = Record::new(
                wm(5),
                Kind::Baseline {
                    version,
                    schema_hash: hash.clone(),
                    to: wm(60),
                    scan_digest: "digest".into(),
                    binds: None,
                    classifier_version: CLASSIFIER_VERSION.into(),
                },
            );
            let id = crate::mysql::mysql_activation::baseline_capture_id(
                &k.lineage_hash,
                &table_stream(&k),
                &record,
            )
            .unwrap();
            append(
                &one.registry_backend,
                one.schema.registry(),
                &k,
                ACTIVATION_NS,
                &table_stream(&k),
                &id,
                &record,
            )
            .await
            .unwrap();
            assert_eq!(stored(&batch).await, stored(&one).await);
        }

        /// Refused tables: a relevant DDL is decided before registering
        /// (nothing written for it); a baseline that is not decisive (a
        /// record incomparable with the rows) is registered, as one table at
        /// a time registers it, but no record is written. Both leave their outcome for their rows; the other table
        /// of the batch is admitted.
        #[tokio::test]
        async fn refused_tables_write_no_baseline() {
            let mut ctx = ctx_on(std::sync::Arc::new(
                storage::MemoryStorageBackend::new(),
            ))
            .await;
            let k4 = key(&ctx, "t4");
            append(
                &ctx.registry_backend,
                ctx.schema.registry(),
                &k4,
                ACTIVATION_NS,
                &table_stream(&k4),
                "incomparable-ddl",
                &Record::new(
                    WmPos::MysqlGtid {
                        gtid_set: "4e11fa47-71ca-11e1-9e33-c80aa9429562:1-2"
                            .into(),
                    },
                    Kind::Ddl,
                ),
            )
            .await
            .unwrap();
            let relevant = super::super::super::mysql_binlog_scan::Statement {
                event: crate::mysql::mysql_activation::EventIdentity::Gtid {
                    gtid: format!("{U}:55"),
                    ordinal: 1,
                },
                effect: DdlEffect::Tables(vec![TableName {
                    db: "d".into(),
                    table: "t3".into(),
                }]),
            };
            let (_, governs) = admit(
                &mut ctx,
                &["t1", "t3", "t4"],
                vec![
                    entry("t1", 5),
                    (
                        ("d".into(), "t3".into()),
                        at(5),
                        report(vec![relevant.clone()]),
                    ),
                    entry("t4", 4),
                ],
            )
            .await
            .unwrap();
            let governs: HashMap<_, _> =
                governs.into_iter().map(|((_, t), _, g)| (t, g)).collect();
            assert_eq!(governs["t1"], Governs::Admitted);
            assert_eq!(
                governs["t3"],
                Governs::Refused(Covers::UntilStatement(
                    relevant.event.clone()
                )),
                "a relevant DDL covers the rows before it"
            );
            assert_eq!(
                governs["t4"],
                Governs::Refused(Covers::Transaction),
                "not decisive covers its own transaction only"
            );
            assert_eq!(
                seqs(&ctx, "t3").await,
                (None, None),
                "relevant DDL: nothing written"
            );
            let (r4, _) = seqs(&ctx, "t4").await;
            assert!(r4.is_some(), "registered");
            let records = crate::mysql::mysql_activation::read_table(
                &ctx.registry_backend,
                ctx.schema.registry(),
                &k4,
            )
            .await
            .unwrap();
            assert_eq!(
                records.len(),
                1,
                "no baseline beside the incomparable ddl"
            );
            // Every covered row keeps its outcome; uncovered rows have none.
            for k in [5, 9, 54] {
                assert!(
                    matches!(
                        ctx.take_prepared("d", "t3", &wm(k), &at(k)),
                        Some(PreparedOutcome::Refused("relevant_ddl"))
                    ),
                    "t3 rows at {k}, before the DDL at 55"
                );
            }
            assert!(
                ctx.take_prepared("d", "t3", &wm(55), &at(55)).is_none(),
                "t3 rows after the DDL are not covered"
            );
            assert!(matches!(
                ctx.take_prepared("d", "t4", &wm(4), &at(4)),
                Some(PreparedOutcome::Refused("not_decisive"))
            ));
            assert!(
                ctx.take_prepared("d", "t4", &wm(6), &at(6)).is_none(),
                "t4's later transactions are not covered"
            );
            assert!(
                seqs(&ctx, "t1").await.1.is_some(),
                "the other table is admitted"
            );
        }

        /// A store that cannot commit: the window fails closed, nothing of it
        /// is written, and no cached timeline holds an uncommitted record.
        #[tokio::test]
        async fn a_failed_batch_writes_nothing_and_caches_nothing() {
            let fault = std::sync::Arc::new(
                storage::adapters::test_util::FaultBackend::new(),
            );
            let backend: ArcStorageBackend = fault.clone();
            let mut ctx = ctx_on(backend).await;
            fault
                .fail_writes_to
                .lock()
                .unwrap()
                .push(ACTIVATION_NS.to_string());
            let err = admit(
                &mut ctx,
                &["t1", "t2"],
                vec![entry("t1", 5), entry("t2", 6)],
            )
            .await
            .unwrap_err();
            assert!(
                format!("{err:#}").contains("commit the activation window"),
                "{err:#}"
            );
            fault.fail_writes_to.lock().unwrap().clear();
            for t in ["t1", "t2"] {
                assert_eq!(
                    seqs(&ctx, t).await,
                    (None, None),
                    "{t}: nothing written"
                );
                let kt = key(&ctx, t);
                let t = super::super::super::mysql_selection::timeline(
                    &mut ctx, &kt,
                )
                .await
                .unwrap();
                assert!(t.records.is_empty(), "no uncommitted record cached");
            }
            assert!(ctx.prepared.is_empty());
        }

        /// A conflict (the store changed after the decision was read):
        /// nothing is written by that attempt, the cached timelines are
        /// discarded, and the decision is made again from a fresh read.
        #[tokio::test]
        async fn a_conflict_is_decided_again_from_a_fresh_read() {
            let mut ctx = ctx_on(std::sync::Arc::new(
                storage::MemoryStorageBackend::new(),
            ))
            .await;
            let k1 = key(&ctx, "t1");
            // The cache holds the empty timeline; the store gains a record.
            super::super::super::mysql_selection::timeline(&mut ctx, &k1)
                .await
                .unwrap();
            append(
                &ctx.registry_backend,
                ctx.schema.registry(),
                &k1,
                ACTIVATION_NS,
                &table_stream(&k1),
                "unknown-at-2",
                &Record::new(wm(2), Kind::Unknown),
            )
            .await
            .unwrap();
            let (stats, _) = admit(&mut ctx, &["t1"], vec![entry("t1", 5)])
                .await
                .unwrap();
            assert_eq!(
                (stats.conflicts, stats.applies),
                (1, 2),
                "one conflict, decided again and applied once more"
            );
            let records = crate::mysql::mysql_activation::read_table(
                &ctx.registry_backend,
                ctx.schema.registry(),
                &k1,
            )
            .await
            .unwrap();
            assert_eq!(
                records.len(),
                2,
                "the record read again, then the baseline"
            );
            let cached =
                super::super::super::mysql_selection::timeline(&mut ctx, &k1)
                    .await
                    .unwrap();
            assert_eq!(
                cached.records, records,
                "the cache is the committed timeline"
            );
            let _ = Stored::clone(&records[0]);
        }
    }

    /// Exhausted lock attempts are the contention incident and saturation
    /// the saturation incident; other failures keep their connection or
    /// schema class (never an incident).
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
        // No control-plane connection within the wait: the saturation
        // incident, naming the bound that stayed full.
        let e = fail(BatchCaptureError::Failed(ProofError::Saturated {
            scope: "runner",
            why: "saturated".into(),
        }));
        let draft = e.draft().expect("an incident");
        assert_eq!(
            draft.reason_code,
            deltaforge_core::incident::ReasonCode::MysqlControlPlaneSaturated
        );
        assert_eq!(
            draft
                .evidence
                .text_of(deltaforge_core::incident::EvidenceKey::ReasonClass),
            Some("runner")
        );
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

    /// The read-ahead tracker ends transactions where the dispatcher does:
    /// a DDL (plain or in an executable comment) completes its implicit
    /// transaction, so the next transaction starts after it; a control
    /// statement inside an explicit transaction does not.
    #[test]
    fn the_tracker_ends_a_ddls_transaction_as_dispatch_does() {
        use mysql_binlog_connector_rust::event::{
            gtid_event::GtidEvent, query_event::QueryEvent,
        };
        const U: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
        let header = |end: u32| EventHeader {
            timestamp: 0,
            event_type: 0,
            server_id: 1,
            event_length: 50,
            next_event_position: end,
            event_flags: 0,
        };
        let gtid = |n: u64| {
            EventData::Gtid(GtidEvent {
                flags: 0,
                gtid: format!("{U}:{n}"),
            })
        };
        let query = |sql: &str| {
            EventData::Query(QueryEvent {
                thread_id: 1,
                exec_time: 0,
                error_code: 0,
                schema: "d".into(),
                query: sql.into(),
            })
        };
        for ddl in [
            "ALTER TABLE t ADD COLUMN w INT",
            "/*!50700 ALTER TABLE t ADD COLUMN w INT */",
        ] {
            let mut ahead = Ahead {
                file: "mysql-bin.000001".into(),
                done: GtidSet::parse(&format!("{U}:1-4")),
                open_gtid: None,
                boundary: MySqlCheckpoint {
                    file: "mysql-bin.000001".into(),
                    pos: 100,
                    gtid_set: Some(format!("{U}:1-4")),
                    lineage: None,
                    snapshot_completed: None,
                    snapshot_chain: None,
                },
                table_map: HashMap::new(),
                broken: false,
                txn_anchor: None,
                explicit: false,
                server_version: Some(80409),
            };
            let mut found = Vec::new();
            touched(&mut ahead, &header(150), &gtid(5), &mut found);
            touched(&mut ahead, &header(300), &query(ddl), &mut found);
            assert_eq!(ahead.boundary.pos, 300, "{ddl}");
            assert_eq!(
                ahead.boundary.gtid_set.as_deref(),
                Some(format!("{U}:1-5").as_str()),
                "{ddl}"
            );
            // An explicit transaction: a SAVEPOINT continues it, its COMMIT
            // ends it.
            touched(&mut ahead, &header(350), &gtid(6), &mut found);
            touched(&mut ahead, &header(400), &query("BEGIN"), &mut found);
            touched(
                &mut ahead,
                &header(450),
                &query("SAVEPOINT s"),
                &mut found,
            );
            assert_eq!(ahead.boundary.pos, 300, "{ddl}: still inside");
            touched(&mut ahead, &header(500), &query("COMMIT"), &mut found);
            assert_eq!(ahead.boundary.pos, 500, "{ddl}");
            assert!(found.is_empty());
        }
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
    }
}
