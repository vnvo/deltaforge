//! Forward proof after a live DDL (design spec 7.15, binding rulings Rounds
//! 24, 25 and 30).
//!
//! When the stream reads a DDL that may change a tracked table, the table's
//! `ddl` record (3b-1) makes the shape after it pending. Before the DDL event
//! is emitted, and so before any later event is read, the source proves the
//! post-DDL shape:
//!
//! 1. a stable capture of the table's shape (position, shape, position equal
//!    on one verified connection) at S_t;
//! 2. D (the DDL's position) and S (the last capture's position, at or after
//!    every S_t) comparable in the verified lineage with D <= S;
//! 3. one complete scan of (D, S] in which nothing affects the table: no DDL
//!    attributed to it, no database barrier for its database, no lineage
//!    barrier;
//! 4. only then: the captured shape's registration and
//!    `Observed { binds: ddl_id, proof }` at D commit in one atomic batch
//!    (`docs/design/mysql-ddl-proof-transactions.md`, C1). Every table is
//!    wholly in one batch; a multi-table DDL whose tables do not fit one is
//!    committed in several, in order.
//!
//! The pending `ddl` record is written before the capture, on its own. Any
//! failure leaves the DDL pending (rows after it stay unproven), with no
//! registration or binding from an uncommitted batch and no checkpoint
//! advance; a connection that reached another server stops the source. A
//! DROP gets no forward proof. Replay: a binding already present for the
//! `ddl` record is the durable evidence only once `resolve_binding`
//! validates it; nothing is then captured or appended again. A crash before
//! a batch commits leaves its tables pending, and the restart proves them
//! again.

use deltaforge_core::{CheckpointOrder, SourceError, SourceResult};
use tracing::{info, warn};

use super::mysql_activation::{self, ForwardProof, resolve_binding};
use super::mysql_baseline::affects;
use super::mysql_binlog_scan::{
    CLASSIFIER_VERSION, ProofError, ProofKind, ScanTag, capture, covers,
    observe_capture, trace_proof,
};
use super::mysql_binlog_scan::{Captured, ScanReport};
use super::mysql_selection::{PlannedVersions, Timeline};
use super::mysql_window_budget::{Caps, Cost, Prefix, proof_table_cost};
use super::{MySqlCheckpoint, RunCtx, WindowInvalid};
use crate::durable_checkpoint::{
    WmPos, mysql_checkpoint_position, order_positions,
};
use crate::registry_scope::RegistryScope;
use schema_registry::SourceSchema;
use std::sync::Arc;
use storage::DurableSchemaRegistry;
use storage::adapters::{RegistrationPlanning, RegistrationRequest, SchemaKey};

/// A tracked table whose shape a DDL left pending.
pub(crate) struct Pending {
    pub db: String,
    pub table: String,
    /// The exact capture identity of the table's pending `ddl` record.
    pub ddl_id: String,
    /// Its cached timeline before the `ddl` record, when C3 may install.
    pub snapshot: Option<Snapshot>,
}

/// A pending table's cached timeline, taken before its `ddl` record was
/// appended (`docs/design/mysql-ddl-proof-transactions.md`, C3), with the
/// sequence that append stored the record at (an append that found the
/// record already stored is a replay: no snapshot).
pub(crate) struct Snapshot {
    pub timeline: Arc<Timeline>,
    pub ddl_seq: u64,
}

/// A table's timeline after its proof committed, found current by
/// [`fresh_timelines`]: the caller installs it if its cache still holds
/// `snapshot`.
pub(crate) struct Fresh {
    pub key: SchemaKey,
    pub snapshot: Arc<Timeline>,
    pub timeline: Timeline,
}

/// What a forward proof's batches committed for one table: its binding's
/// sequence, and the version it binds with that version's hash.
struct Committed {
    table: usize,
    binding_seq: u64,
    version: i32,
    hash: String,
}

fn other_server(expected: &str, found: &str) -> SourceError {
    SourceError::Lineage {
        details: format!(
            "a forward-proof connection reached another server ({found}), \
             expected {expected}"
        )
        .into(),
    }
}

/// Prove the post-DDL shape of every `pending` table at the DDL's position
/// (the stream's current position). Returns the committed tables' timelines
/// found current (C3); the caller invalidates every other touched table.
pub(crate) async fn prove(
    ctx: &RunCtx,
    pending: Vec<Pending>,
) -> SourceResult<Vec<Fresh>> {
    if pending.is_empty() {
        return Ok(Vec::new());
    }
    let scope = ctx.registry_scope.current()?;
    let lineage = scope.lineage().lineage_hash.clone();
    let server_uuid = ctx.expected_uuid()?;
    let d_cp = MySqlCheckpoint {
        file: ctx.last_file.clone(),
        pos: ctx.last_pos,
        // The DDL statement completes its transaction: D includes it.
        gtid_set: ctx.executed_through_current().map(|s| s.to_string()),
        lineage: Some(lineage.clone()),
        snapshot_completed: None,
        snapshot_chain: None,
    };
    let Some(d) = mysql_checkpoint_position(
        &d_cp.file,
        d_cp.pos,
        d_cp.gtid_set.as_deref(),
    ) else {
        warn!(source_id = %ctx.source_id, "unparseable DDL position: the DDL stays pending");
        return Ok(Vec::new());
    };
    let storage = |e: anyhow::Error| {
        SourceError::Other(e.context("forward proof: activation timeline"))
    };

    // Replay: a binding already recorded for the pending DDL is the proof.
    let mut open = Vec::new();
    for p in pending {
        let key = scope.key(&p.db, &p.table);
        let records = mysql_activation::read_table(
            &ctx.registry_backend,
            ctx.schema.registry(),
            &key,
        )
        .await
        .map_err(storage)?;
        // A binding is the proof only once validated as complete,
        // consistent evidence; an invalid one stops here (never a fresh
        // proof over later server state).
        match resolve_binding(
            ctx.schema.registry(),
            &key,
            &records,
            &p.ddl_id,
            CLASSIFIER_VERSION,
        )
        .await
        {
            Ok(Some(_)) => {}
            Ok(None) => open.push(p),
            Err(e) => {
                return Err(SourceError::Schema {
                    details: format!("{e:#}").into(),
                });
            }
        }
    }
    if open.is_empty() {
        return Ok(Vec::new());
    }

    let tag = ScanTag {
        source_id: &ctx.source_id,
        kind: ProofKind::Forward,
    };
    let lctn = ctx.lower_case_table_names;
    // 1. Captures, bound to their intervals by step 3.
    let mut captured = Vec::new();
    for p in open {
        // A stop ends the proof (`?`): nothing is decided.
        match capture(
            &ctx.capture,
            &server_uuid,
            &lineage,
            &p.db,
            &p.table,
            None,
        )
        .await?
        {
            Ok(cap) => {
                observe_capture(&tag, &cap);
                captured.push((p, cap))
            }
            Err(ProofError::OtherServer(found)) => {
                return Err(other_server(&server_uuid, &found));
            }
            // Saturation never defers: a pending DDL's next rows have no
            // lazy proof. The source stops before the DDL is emitted or
            // checkpointed, nothing of the proof written; the restart
            // replays the DDL and proves it again.
            Err(ProofError::Saturated { scope, .. }) => {
                return Err(
                    crate::incident_drafts::mysql_control_plane_saturated(
                        &ctx.source_id,
                        "forward proof",
                        &[(p.db.clone(), p.table.clone())],
                        scope,
                        &before_ddl(ctx, &lineage),
                    ),
                );
            }
            Err(e) => {
                warn!(source_id = %ctx.source_id, db = %p.db, table = %p.table, error = ?e, "forward proof: no capture; the DDL stays pending");
                trace_proof(
                    &tag,
                    &p.db,
                    &p.table,
                    None,
                    None,
                    lctn,
                    "no_capture",
                );
            }
        }
    }
    let Some(s_cp) = captured.last().map(|(_, cap)| cap.position.clone())
    else {
        return Ok(Vec::new());
    };
    let Some(s) = mysql_checkpoint_position(
        &s_cp.file,
        s_cp.pos,
        s_cp.gtid_set.as_deref(),
    ) else {
        return Ok(Vec::new());
    };

    // 2. D <= S in the verified lineage.
    if !matches!(
        order_positions(&d, &s),
        CheckpointOrder::Before | CheckpointOrder::Equal
    ) {
        warn!(source_id = %ctx.source_id, "forward proof: the capture is not at or after the DDL; the DDL stays pending");
        return Ok(Vec::new());
    }

    // 3. One complete scan of (D, S].
    let report = match super::mysql_proof_scanner::shared_interval(
        &ctx.proof_scanner,
        ctx.dsn.current().expose(),
        &ctx.source_id,
        &server_uuid,
        &lineage,
        &d_cp,
        &s_cp,
        None,
        Some(&d_cp),
        &tag,
    )
    .await
    {
        Ok(report) => report,
        Err(ProofError::OtherServer(found)) => {
            return Err(other_server(&server_uuid, &found));
        }
        Err(e) => {
            warn!(source_id = %ctx.source_id, error = ?e, "forward proof: the interval scan failed; the DDL stays pending");
            for (p, cap) in &captured {
                trace_proof(
                    &tag,
                    &p.db,
                    &p.table,
                    Some(cap),
                    None,
                    lctn,
                    "scan_failed",
                );
            }
            return Ok(Vec::new());
        }
    };

    // 4. Register and bind: the tables the scan proves, in atomic batches.
    let checkpoint =
        serde_json::to_vec(&d_cp).map_err(|e| SourceError::Other(e.into()))?;
    let mut proven = Vec::with_capacity(captured.len());
    for (p, cap) in captured {
        if !covers(&cap, &d, &s) {
            warn!(source_id = %ctx.source_id, db = %p.db, table = %p.table, "forward proof: the scan does not cover the capture interval; the DDL stays pending");
            trace_proof(
                &tag,
                &p.db,
                &p.table,
                Some(&cap),
                Some(&report),
                lctn,
                "not_covered",
            );
            continue;
        }
        if report
            .statements
            .iter()
            .any(|st| affects(st, &p.db, &p.table, ctx.lower_case_table_names))
        {
            info!(source_id = %ctx.source_id, db = %p.db, table = %p.table, "forward proof: a DDL or barrier in (D, S]; the DDL stays pending");
            trace_proof(
                &tag,
                &p.db,
                &p.table,
                Some(&cap),
                Some(&report),
                lctn,
                "relevant_ddl",
            );
            continue;
        }
        proven.push((p, cap));
    }
    let bound = Bound {
        d: &d,
        s: &s,
        report: &report,
        checkpoint: &checkpoint,
        lineage: &lineage,
    };
    let committed = commit_proofs(ctx, &scope, &tag, &bound, &proven).await?;
    let written = committed.len();
    info!(source_id = %ctx.source_id, bindings = written, "forward proof after DDL");
    Ok(fresh_timelines(ctx, &scope, &proven, &committed).await)
}

/// What every binding of one forward proof shares.
struct Bound<'a> {
    d: &'a WmPos,
    s: &'a WmPos,
    report: &'a ScanReport,
    /// The registration checkpoint: D.
    checkpoint: &'a [u8],
    lineage: &'a str,
}

/// The checkpoint before the DDL: where a stopped proof leaves the source.
fn before_ddl(ctx: &RunCtx, lineage: &str) -> MySqlCheckpoint {
    MySqlCheckpoint {
        file: ctx.last_file.clone(),
        pos: ctx.last_event_start,
        gtid_set: ctx.gtid_text(),
        lineage: Some(lineage.to_string()),
        snapshot_completed: None,
        snapshot_chain: None,
    }
}

/// One proven table's request and cost, from its capture.
fn proof_request(
    key: &SchemaKey,
    cap: &Captured,
    checkpoint: &[u8],
    other_lineages: u64,
) -> SourceResult<(RegistrationRequest, Cost)> {
    let schema_json = serde_json::to_value(&cap.schema)
        .map_err(|e| SourceError::Other(e.into()))?;
    let schema_bytes = serde_json::to_vec(&schema_json)
        .map_err(|e| SourceError::Other(e.into()))?
        .len() as u64;
    let cost = proof_table_cost(
        schema_bytes,
        other_lineages,
        key.backend_key().len() as u64,
    );
    Ok((
        RegistrationRequest {
            key: key.clone(),
            hash: cap.schema.fingerprint().to_string(),
            schema_json,
            checkpoint: Some(checkpoint.to_vec()),
        },
        cost,
    ))
}

/// The tables of a batch, in order, cut where the next one would take the
/// batch over a cap; `Err` names a table no batch can take alone.
fn group(
    caps: Caps,
    costs: &[(usize, Cost)],
) -> Result<Vec<Vec<usize>>, (usize, WindowInvalid)> {
    let mut groups = Vec::new();
    let mut current = Vec::new();
    let mut prefix = Prefix::new(caps);
    for (i, cost) in costs {
        Prefix::fits_alone(caps, *cost).map_err(|e| (*i, e))?;
        if !prefix.offer(*cost).map_err(|e| (*i, e))? {
            groups.push(std::mem::take(&mut current));
            prefix = Prefix::new(caps);
            prefix.offer(*cost).map_err(|e| (*i, e))?;
        }
        current.push(*i);
    }
    if !current.is_empty() {
        groups.push(current);
    }
    Ok(groups)
}

/// Commit the registrations and bindings of the `proven` tables
/// (`docs/design/mysql-ddl-proof-transactions.md`, C1): every table wholly
/// in one atomic batch, its registration and its binding together. The
/// preflight sizes every table before the first commit, keeping one cost
/// per table; a table no batch can take alone stops the source with
/// nothing of the proof written. A batch's plan exists only while it is
/// committed. A conflict plans the batch again from fresh reads and sizes
/// it again (bounded); a batch that no longer fits is split. The pending
/// `ddl` records stay as written; a stop leaves every table not yet
/// committed pending. Returns how many bindings were committed.
async fn commit_proofs(
    ctx: &RunCtx,
    scope: &RegistryScope,
    tag: &ScanTag<'_>,
    bound: &Bound<'_>,
    proven: &[(Pending, Captured)],
) -> SourceResult<Vec<Committed>> {
    use super::mysql_activation::{
        ACTIVATION_NS, Kind, Record, encode_record, forward_capture_id,
        table_stream,
    };
    use super::mysql_activation_window::{
        BATCH_CONFLICT_RETRIES, BATCH_STORAGE_RETRIES,
    };
    use storage::{AtomicBatch, BatchOp, CommitResult, commit_atomic_counted};

    if proven.is_empty() {
        return Ok(Vec::new());
    }
    // One store holds the registry and the activation records, or no batch
    // can commit them together.
    if !std::sync::Arc::ptr_eq(
        ctx.schema.registry().backend(),
        &ctx.registry_backend,
    ) {
        return Err(SourceError::Other(anyhow::anyhow!(
            "the forward proof needs the schema registry and the activation \
             records in one state store"
        )));
    }
    let lctn = ctx.lower_case_table_names;
    let keys: Vec<SchemaKey> = proven
        .iter()
        .map(|(p, _)| scope.key(&p.db, &p.table))
        .collect();
    let stopped = |i: usize, invalid: WindowInvalid| {
        let (p, _) = &proven[i];
        crate::incident_drafts::mysql_table_too_large(
            &ctx.source_id,
            &p.db,
            &p.table,
            &invalid,
            &before_ddl(ctx, bound.lineage),
        )
    };
    let too_large = |i: usize, too: &storage::StateItemTooLarge| {
        let (p, _) = &proven[i];
        crate::incident_drafts::state_item_too_large(
            &ctx.source_id,
            &p.db,
            &p.table,
            too,
            &before_ddl(ctx, bound.lineage),
        )
    };
    let other_lineages = || async {
        storage::adapters::source_lineage::load(
            &ctx.registry_backend,
            &ctx.tenant,
            &ctx.source_id,
        )
        .await
        .map(|r| r.map_or(0, |r| r.prior_lineage_hashes.len() as u64))
        .map_err(SourceError::Other)
    };
    // The costs of the tables in `of`, from their captures; an entry over
    // the store's item limit stops the source here.
    let costs = |of: &[usize],
                 others: u64|
     -> SourceResult<Vec<(usize, Cost)>> {
        let mut out = Vec::with_capacity(of.len());
        for &i in of {
            let (req, cost) = proof_request(
                &keys[i],
                &proven[i].1,
                bound.checkpoint,
                others,
            )?;
            let size = DurableSchemaRegistry::registration_entry_bound(&req)
                .map_err(SourceError::Other)?;
            if size > storage::MAX_STATE_ITEM_BYTES {
                return Err(too_large(
                    i,
                    &storage::StateItemTooLarge {
                        size,
                        key_digest: storage::state_key_digest(
                            storage::adapters::schema_registry::V1_NS,
                            &keys[i].backend_key(),
                        ),
                    },
                ));
            }
            out.push((i, cost));
        }
        Ok(out)
    };

    // Preflight: every table sized, one cost each, before any commit.
    let caps = Caps::current();
    let all: Vec<usize> = (0..proven.len()).collect();
    let mut queue: std::collections::VecDeque<Vec<usize>> =
        group(caps, &costs(&all, other_lineages().await?)?)
            .map_err(|(i, e)| stopped(i, e))?
            .into();

    let registry = ctx.schema.registry();
    let mut out = Vec::with_capacity(proven.len());
    let mut applies = 0;
    while let Some(mut batch_tables) = queue.pop_front() {
        let mut attempt = 0;
        loop {
            attempt += 1;
            if attempt > 1 {
                // Sized again from fresh reads: a batch that no longer fits
                // is split, its rest committed after it.
                let mut regrouped = group(
                    caps,
                    &costs(&batch_tables, other_lineages().await?)?,
                )
                .map_err(|(i, e)| stopped(i, e))?;
                batch_tables = regrouped.remove(0);
                for rest in regrouped.into_iter().rev() {
                    queue.push_front(rest);
                }
            }
            let mut reqs = Vec::with_capacity(batch_tables.len());
            for &i in &batch_tables {
                reqs.push(
                    proof_request(&keys[i], &proven[i].1, bound.checkpoint, 0)?
                        .0,
                );
            }
            let (preconditions, regs) = match registry
                .plan_registrations(&reqs, chrono::Utc::now())
                .await
                .map_err(|e| {
                    SourceError::Other(
                        e.context("plan the forward proof's registrations"),
                    )
                })? {
                RegistrationPlanning::Planned {
                    preconditions,
                    registrations,
                } => (preconditions, registrations),
                RegistrationPlanning::NeedsRepair(broken) => {
                    for k in &broken {
                        registry
                            .repair_key(k)
                            .await
                            .map_err(SourceError::Other)?;
                    }
                    if attempt > BATCH_CONFLICT_RETRIES {
                        return Err(SourceError::Other(anyhow::anyhow!(
                            "the forward proof's registrations still need repair \
                             after {attempt} attempts; the DDL stays pending"
                        )));
                    }
                    continue;
                }
            };
            let mut batch = AtomicBatch {
                preconditions,
                ops: Vec::new(),
            };
            // Each table's registration entry op, and the table of every op
            // (to name the table of an item the store refuses).
            let mut entry_ops = Vec::with_capacity(regs.len());
            let mut binding_ops = Vec::with_capacity(regs.len());
            let mut op_table = Vec::new();
            for (&i, reg) in batch_tables.iter().zip(&regs) {
                let (p, _) = &proven[i];
                let key = &keys[i];
                let base = batch.push_group(reg.ops.clone());
                entry_ops.push(reg.entry_op.map(|e| base + e));
                let record = Record::new(
                    bound.d.clone(),
                    Kind::Observed {
                        version: reg.version,
                        binds: Some(p.ddl_id.clone()),
                        proof: Some(ForwardProof {
                            classifier_version: CLASSIFIER_VERSION.to_string(),
                            to: bound.s.clone(),
                            scan_digest: bound.report.digest.clone(),
                            schema_hash: reg.hash.clone(),
                        }),
                        full: None,
                    },
                );
                let stream = table_stream(key);
                let capture_id =
                    forward_capture_id(&key.lineage_hash, &stream, &record)
                        .map_err(SourceError::Other)?;
                binding_ops.push(batch.ops.len());
                batch.ops.push(BatchOp::LogAppendIdempotent {
                    ns: ACTIVATION_NS.into(),
                    key: stream,
                    capture_id,
                    value: encode_record(&record)
                        .map_err(SourceError::Other)?,
                });
                op_table.resize(batch.ops.len(), i);
            }
            let committed = match commit_atomic_counted(
                ctx.registry_backend.as_ref(),
                &batch,
                BATCH_STORAGE_RETRIES,
                &mut applies,
            )
            .await
            {
                Ok(c) => c,
                Err(e) => {
                    if let Some(too) = e.chain().find_map(|c| {
                        c.downcast_ref::<storage::StateItemTooLarge>()
                    }) && let Some(i) =
                        super::mysql_activation_window::refused_table(
                            &batch,
                            &op_table,
                            &too.key_digest,
                        )
                    {
                        return Err(too_large(i, too));
                    }
                    return Err(SourceError::Other(
                        e.context("persist a forward-proof binding"),
                    ));
                }
            };
            let seqs = match committed {
                CommitResult::Committed(seqs) => seqs,
                CommitResult::Conflict(why) => {
                    if attempt > BATCH_CONFLICT_RETRIES {
                        return Err(SourceError::Other(anyhow::anyhow!(
                            "the forward proof's batch conflicted {attempt} times \
                             (last: {why}); the DDL stays pending"
                        )));
                    }
                    warn!(source_id = %ctx.source_id, attempt, why, "forward proof: the batch conflicted; planning again");
                    continue;
                }
            };
            // Committed: the registry's cache learns it, and only now.
            for (reg, op) in regs.iter().zip(&entry_ops) {
                if let Some(op) = op {
                    registry.admit_committed(
                        reg,
                        seqs[*op].expect("a committed log write"),
                    );
                }
            }
            for ((&i, reg), op) in
                batch_tables.iter().zip(&regs).zip(&binding_ops)
            {
                let (p, cap) = &proven[i];
                trace_proof(
                    tag,
                    &p.db,
                    &p.table,
                    Some(cap),
                    Some(bound.report),
                    lctn,
                    "proven",
                );
                out.push(Committed {
                    table: i,
                    binding_seq: seqs[*op].expect("a committed log write"),
                    version: reg.version,
                    hash: reg.hash.clone(),
                });
            }
            break;
        }
    }
    Ok(out)
}

/// The committed tables' timelines that are still current (C3), each its
/// snapshot followed by exactly its `ddl` record and its binding, decoded
/// from the store as a load decodes them and validated as a load validates
/// them. Current means, at this read: the table stream holds exactly those
/// two entries after the snapshot's head, and every barrier stream is where
/// the snapshot read it. Freshness holds at the read, not through the
/// install; the interval after it is safe only because no other writer
/// exists, so a table has a snapshot only under the store-gate witness
/// ([`storage::adapters::store_gate::server_holds`], checked where the
/// snapshot is taken). A table that fails any check, or a read error, is
/// left out (the caller invalidates it).
async fn fresh_timelines(
    ctx: &RunCtx,
    scope: &RegistryScope,
    proven: &[(Pending, Captured)],
    committed: &[Committed],
) -> Vec<Fresh> {
    use super::mysql_activation::{ACTIVATION_NS, BARRIER_NS, table_stream};
    use storage::{BatchRead, BatchReadResult};

    let backend = &ctx.registry_backend;
    let candidates: Vec<(&Committed, &Snapshot)> = committed
        .iter()
        .filter_map(|c| proven[c.table].0.snapshot.as_ref().map(|s| (c, s)))
        .collect();
    if candidates.is_empty() {
        return Vec::new();
    }
    let reads: Vec<BatchRead> = candidates
        .iter()
        .flat_map(|(_, s)| {
            s.timeline.streams.iter().map(|b| BatchRead::LogHead {
                ns: BARRIER_NS.into(),
                key: b.stream.clone(),
            })
        })
        .collect();
    let heads = match backend.read_batch(&reads).await {
        Ok(h) if h.len() == reads.len() => h,
        other => {
            warn!(source_id = %ctx.source_id, outcome = ?other.map(|h| h.len()), "forward proof: barrier heads unread; the proven timelines reload");
            return Vec::new();
        }
    };
    let mut heads = heads.into_iter();
    let mut out = Vec::new();
    for (c, snap) in candidates {
        let t = &snap.timeline;
        // Every head is consumed, current or not, to keep the rest aligned.
        let mut barriers_current = true;
        for b in &t.streams {
            barriers_current &= matches!(
                heads.next(),
                Some(BatchReadResult::Head(h)) if h == b.head
            );
        }
        if !barriers_current {
            continue;
        }
        let (p, _) = &proven[c.table];
        let key = scope.key(&p.db, &p.table);
        let stream = table_stream(&key);
        let after = match backend
            .log_read_meta_since(ACTIVATION_NS, &stream, t.table_head, 3)
            .await
        {
            Ok(after) => after,
            Err(e) => {
                warn!(source_id = %ctx.source_id, error = ?e, "forward proof: the table stream is unread; its timeline reloads");
                continue;
            }
        };
        if !matches!(
            after.as_slice(),
            [ddl, binding] if ddl.seq == snap.ddl_seq && binding.seq == c.binding_seq
        ) {
            continue;
        }
        let Ok((records, head)) =
            super::mysql_activation::decode_stream(&stream, &after)
        else {
            continue;
        };
        let table = super::mysql_history::TableStream {
            records: t.records.iter().cloned().chain(records).collect(),
            head,
            bytes: t.bytes
                + after.iter().map(storage::log_entry_bytes).sum::<u64>(),
        };
        if super::mysql_history::within_limit(&key, &table, &t.streams).is_err()
        {
            continue;
        }
        let timeline = Timeline {
            records: table.records,
            table_head: table.head,
            streams: t.streams.clone(),
            bytes: table.bytes,
        };
        let planned = PlannedVersions::from([(c.version, c.hash.clone())]);
        if super::mysql_selection::validate_planned(
            ctx.schema.registry(),
            &key,
            &timeline,
            &planned,
        )
        .await
        .is_err()
        {
            continue;
        }
        out.push(Fresh {
            key,
            snapshot: t.clone(),
            timeline,
        });
    }
    out
}

/// Test seams of C3's interval between the freshness check and the install.
#[cfg(test)]
pub(crate) mod hooks {
    use super::RunCtx;
    use std::sync::Mutex;

    /// What [`BEFORE_INSTALL`] runs.
    pub(crate) type BeforeInstall = Box<dyn FnOnce(&mut RunCtx) + Send>;

    /// One-shot: runs on the source's task after the proof's freshness
    /// check, before the install (a writer acting in that interval).
    pub(crate) static BEFORE_INSTALL: Mutex<Option<BeforeInstall>> =
        Mutex::new(None);

    /// Every timeline installed, with its table's backend key and the
    /// snapshot's estimated bytes.
    pub(crate) static INSTALLED: Mutex<
        Vec<(String, u64, super::super::mysql_selection::Timeline)>,
    > = Mutex::new(Vec::new());

    pub(crate) fn before_install(ctx: &mut RunCtx) {
        let hook = BEFORE_INSTALL.lock().unwrap().take();
        if let Some(hook) = hook {
            hook(ctx);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Caps, Cost, WindowInvalid, group};

    fn caps() -> Caps {
        Caps {
            plan: Cost {
                bytes: 100,
                read_items: 50,
                read_key_bytes: 10_000,
                write_ops: 20,
            },
            table_bytes: 1000,
        }
    }

    fn cost(bytes: u64, read_items: u64) -> (usize, Cost) {
        (
            0,
            Cost {
                bytes,
                read_items,
                read_key_bytes: read_items,
                write_ops: 7,
            },
        )
    }

    fn costs(of: &[(u64, u64)]) -> Vec<(usize, Cost)> {
        of.iter()
            .enumerate()
            .map(|(i, (b, r))| (i, cost(*b, *r).1))
            .collect()
    }

    /// Tables are batched in order, each batch closing before the table
    /// that would take it over a cap; every table is in exactly one batch.
    #[test]
    fn tables_are_batched_in_order_within_the_caps() {
        assert_eq!(
            group(caps(), &costs(&[(40, 1), (40, 1), (40, 1), (10, 1)])),
            Ok(vec![vec![0, 1], vec![2, 3]])
        );
        assert_eq!(
            group(caps(), &costs(&[(1, 30), (1, 30), (1, 10)])),
            Ok(vec![vec![0], vec![1, 2]])
        );
        assert_eq!(group(caps(), &[]), Ok(Vec::<Vec<usize>>::new()));
    }

    /// A table no batch can take alone is refused, by its index, even
    /// under the per-table limit (over a batch cap): nothing is grouped.
    #[test]
    fn a_table_over_a_batch_cap_is_refused_before_any_batch() {
        assert_eq!(
            group(caps(), &costs(&[(10, 1), (101, 1)])),
            Err((
                1,
                WindowInvalid::StateTooLarge {
                    bytes: 101,
                    limit: 100
                }
            ))
        );
        assert_eq!(
            group(caps(), &costs(&[(10, 51)])),
            Err((
                0,
                WindowInvalid::RegistryTooLarge {
                    read_items: 51,
                    limit: 50
                }
            ))
        );
    }
    /// Live MySQL 8.4 (Docker).
    mod live {
        use super::super::*;
        use crate::mysql::MySqlSource;
        use crate::mysql::mysql_activation::{
            Kind, Selection, Stored, read_barriers, read_table, select,
        };
        use checkpoints::{CheckpointStore, MemCheckpointStore};
        use deltaforge_config::{OnSchemaDrift, SnapshotCfg, SnapshotMode};
        use deltaforge_core::{Source, SourceHandle, SourceItem};
        use gate_ownership::GateOwned;
        use mysql_async::prelude::Queryable;
        use std::sync::Arc;
        use std::time::{Duration, Instant};
        use storage::adapters::test_util::FaultBackend;
        use storage::{
            ArcStorageBackend, DurableSchemaRegistry, MemoryStorageBackend,
        };
        use testcontainers::core::WaitFor;
        use testcontainers::runners::AsyncRunner;
        use testcontainers::{ContainerAsync, GenericImage, ImageExt};
        use tokio::sync::{OnceCell, mpsc};

        static GTID: OnceCell<(ContainerAsync<GenericImage>, u16)> =
            OnceCell::const_new();
        static FILEPOS: OnceCell<(ContainerAsync<GenericImage>, u16)> =
            OnceCell::const_new();

        async fn start(gtid: bool) -> (ContainerAsync<GenericImage>, u16) {
            let mut cmd = vec![
                "--server-id=61",
                "--log-bin=mysql-bin",
                "--binlog-format=ROW",
                "--binlog-checksum=NONE",
            ];
            if gtid {
                cmd.extend(["--gtid-mode=ON", "--enforce-gtid-consistency=ON"]);
            }
            let c = GenericImage::new("mysql", "8.4")
                .with_wait_for(WaitFor::message_on_stderr(
                    "ready for connections. Version: '8.4",
                ))
                .with_env_var("MYSQL_ROOT_PASSWORD", "pw")
                .with_cmd(cmd)
                .gate_owned()
                .start()
                .await
                .expect("start mysql");
            let port = c.get_host_port_ipv4(3306).await.unwrap();
            for _ in 0..60 {
                if mysql_async::Conn::from_url(dsn(port)).await.is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            (c, port)
        }

        async fn server(gtid: bool) -> u16 {
            let cell = if gtid { &GTID } else { &FILEPOS };
            cell.get_or_init(|| start(gtid)).await.1
        }

        fn dsn(port: u16) -> String {
            format!("mysql://root:pw@127.0.0.1:{port}/")
        }

        async fn sql(port: u16, stmts: &[&str]) {
            let mut c = mysql_async::Conn::from_url(dsn(port)).await.unwrap();
            for s in stmts {
                c.query_drop(*s)
                    .await
                    .unwrap_or_else(|e| panic!("{s}: {e}"));
            }
            c.disconnect().await.ok();
        }

        struct State {
            backend: ArcStorageBackend,
            registry: Arc<DurableSchemaRegistry>,
            ckpt: Arc<dyn CheckpointStore>,
            scope: crate::registry_scope::SharedRegistryScope,
        }

        async fn state(backend: ArcStorageBackend) -> State {
            State {
                registry: DurableSchemaRegistry::new(backend.clone())
                    .await
                    .unwrap(),
                ckpt: Arc::new(MemCheckpointStore::new().unwrap()),
                scope: crate::registry_scope::SharedRegistryScope::default(),
                backend,
            }
        }

        struct Run {
            handle: SourceHandle,
            rx: mpsc::Receiver<SourceItem>,
        }

        async fn run(st: &State, port: u16, id: &str, tables: &[&str]) -> Run {
            let src = MySqlSource {
                id: id.into(),
                dsn: dsn(port).as_str().into(),
                tables: tables.iter().map(|t| t.to_string()).collect(),
                tenant: "acme".into(),
                pipeline: "test".into(),
                registry: st.registry.clone(),
                registry_scope: st.scope.clone(),
                backend: st.backend.clone(),
                outbox_tables: common::AllowList::default(),
                snapshot_cfg: SnapshotCfg {
                    mode: SnapshotMode::Never,
                    ..Default::default()
                },
                on_schema_drift: OnSchemaDrift::Adapt,
                table_options: Default::default(),
                rotation: None,
                snapshot_cohort: Default::default(),
                proofs: Default::default(),
                type_mapping: deltaforge_core::TypeMapping::V2,
            };
            let (tx, rx) = mpsc::channel(4096);
            let handle = src.run(tx, st.ckpt.clone()).await;
            tokio::time::sleep(Duration::from_secs(3)).await;
            Run { handle, rx }
        }

        async fn stop(handle: SourceHandle) {
            handle.stop();
            tokio::time::timeout(Duration::from_secs(30), handle.join)
                .await
                .ok();
        }

        /// Wait for the DDL event containing `needle`.
        async fn ddl_event(rx: &mut mpsc::Receiver<SourceItem>, needle: &str) {
            let deadline = Instant::now() + Duration::from_secs(60);
            while let Some(left) =
                deadline.checked_duration_since(Instant::now())
            {
                match tokio::time::timeout(left, rx.recv()).await {
                    Ok(Some(SourceItem::Event(e)))
                        if e.ddl.as_ref().is_some_and(|d| {
                            d.to_string().contains(needle)
                        }) =>
                    {
                        return;
                    }
                    Ok(Some(_)) => {}
                    _ => break,
                }
            }
            panic!("no DDL event containing {needle:?}");
        }

        async fn records(st: &State, db: &str, table: &str) -> Vec<Stored> {
            let key = st.scope.current().unwrap().key(db, table);
            read_table(&st.backend, &st.registry, &key).await.unwrap()
        }

        async fn barriers(st: &State, db: &str, table: &str) -> Vec<Stored> {
            let key = st.scope.current().unwrap().key(db, table);
            read_barriers(&st.backend, &key).await.unwrap()
        }

        fn ddls(recs: &[Stored]) -> Vec<&Stored> {
            recs.iter().filter(|s| s.record.kind == Kind::Ddl).collect()
        }

        /// The number of registered versions of `db.table`.
        async fn versions(st: &State, db: &str, table: &str) -> usize {
            let key = st.scope.current().unwrap().key(db, table);
            crate::mysql::mysql_activation::tests_support::history(
                &st.registry,
                &key,
            )
            .await
            .len()
        }

        /// The store writes made, one per call (`batch`: one atomic batch),
        /// to the registry and activation namespaces.
        fn schema_writes(fault: &FaultBackend) -> Vec<String> {
            fault
                .write_calls
                .lock()
                .unwrap()
                .iter()
                .filter(|ns| ns.starts_with("schemas") || *ns == "batch")
                .cloned()
                .collect()
        }

        /// The validated forward binding of `ddl` (panics on invalid
        /// evidence).
        async fn bound(
            st: &State,
            db: &str,
            table: &str,
            recs: &[Stored],
            ddl: &Stored,
        ) -> Option<i32> {
            let key = st.scope.current().unwrap().key(db, table);
            resolve_binding(
                &st.registry,
                &key,
                recs,
                &ddl.capture_id,
                CLASSIFIER_VERSION,
            )
            .await
            .expect("valid binding evidence")
        }

        async fn prepare(port: u16, db: &str) {
            sql(
                port,
                &[
                    &format!("DROP DATABASE IF EXISTS {db}"),
                    &format!("CREATE DATABASE {db}"),
                    &format!("CREATE TABLE {db}.t (id INT PRIMARY KEY, v INT)"),
                    &format!("CREATE TABLE {db}.other (id INT PRIMARY KEY)"),
                ],
            )
            .await;
        }

        async fn a_live_ddl_is_bound(gtid: bool) {
            let port = server(gtid).await;
            let db = if gtid { "fwd_g" } else { "fwd_f" };
            prepare(port, db).await;
            let st = state(Arc::new(MemoryStorageBackend::new())).await;
            // A committed position before the DDL (for the replay below).
            let r = run(&st, port, db, &[&format!("{db}.t")]).await;
            stop(r.handle).await;
            let mut r = run(&st, port, db, &[&format!("{db}.t")]).await;
            sql(port, &[&format!("ALTER TABLE {db}.t ADD COLUMN w INT")]).await;
            ddl_event(&mut r.rx, "ADD COLUMN w").await;
            // Durable before the DDL event: the pending DDL and its binding.
            let recs = records(&st, db, "t").await;
            let d = ddls(&recs);
            assert_eq!(d.len(), 1, "{recs:?}");
            let version = bound(&st, db, "t", &recs, d[0])
                .await
                .expect("the DDL is bound");
            let key = st.scope.current().unwrap().key(db, "t");
            let shape = st.registry.version_hash(&key, version).await.unwrap();
            assert!(shape.is_some());
            let all: Vec<Stored> = recs
                .iter()
                .cloned()
                .chain(barriers(&st, db, "t").await)
                .collect();
            assert_eq!(
                select(&all, &d[0].record.position),
                Selection::Proven { version }
            );
            let history =
                crate::mysql::mysql_activation::tests_support::history(
                    &st.registry,
                    &key,
                )
                .await;
            assert!(
                history.last().unwrap().contains("\"w\""),
                "the bound version has the new column"
            );

            // A crash after the binding (no checkpoint past the DDL): the
            // replay re-derives nothing new.
            let before = records(&st, db, "t").await;
            r.handle.join.abort();
            // The server moves on before the restart: a re-proof would capture
            // a different S (another record); the existing binding is reused.
            sql(port, &[&format!("INSERT INTO {db}.other VALUES (1)")]).await;
            let mut r = run(&st, port, db, &[&format!("{db}.t")]).await;
            ddl_event(&mut r.rx, "ADD COLUMN w").await;
            assert_eq!(records(&st, db, "t").await, before);
            stop(r.handle).await;
        }

        /// A saturated control plane for the next capture: a runner budget
        /// whose only ordinary permit another source holds (released by
        /// dropping what is returned); the capture waits at most 1 s.
        async fn starve_next_capture(
            dsn: &str,
        ) -> crate::mysql::mysql_control::ControlConn {
            use crate::mysql::mysql_control::{Budget, Class, ControlPool};
            let mut c = mysql_async::Conn::from_url(dsn).await.unwrap();
            let uuid: String = c
                .query_first("SELECT @@GLOBAL.server_uuid")
                .await
                .unwrap()
                .unwrap();
            c.disconnect().await.ok();
            let budget: &'static Budget = Box::leak(Box::new(Budget::new(
                crate::mysql::mysql_binlog_scan::KILL_RESERVE + 1,
            )));
            let held = ControlPool::detached(budget, "other", 2)
                .connect(Class::Catalog, dsn, &uuid, Duration::from_secs(5))
                .await
                .unwrap();
            *crate::mysql::mysql_binlog_scan::STARVE_NEXT.lock().unwrap() =
                Some((
                    ControlPool::detached(budget, "starved", 2),
                    Duration::from_secs(1),
                ));
            held
        }

        /// A saturated control plane at a DDL's forward proof is the
        /// `mysql_control_plane_saturated` incident (AutoRetry), never a
        /// deferral: the source stops before the DDL is emitted or
        /// checkpointed, reads no later row, and keeps no proof of it (no
        /// binding, no new version). The restart replays the DDL, binds it
        /// and delivers the row after it exactly once. The incident carries
        /// no DSN or credential.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_saturated_forward_proof_stops_and_the_restart_binds_the_ddl()
        {
            let port = server(true).await;
            let db = "fwd_saturated";
            prepare(port, db).await;
            let st = state(Arc::new(MemoryStorageBackend::new())).await;
            let t = format!("{db}.t");
            // A committed position before the DDL.
            let r = run(&st, port, db, &[&t]).await;
            stop(r.handle).await;
            let committed = st.ckpt.get_raw(db).await.unwrap();
            assert!(committed.is_some());
            let versions = || {
                let key = st.scope.current().unwrap().key(db, "t");
                let reg = st.registry.clone();
                async move {
                    crate::mysql::mysql_activation::tests_support::history(
                        &reg, &key,
                    )
                    .await
                    .len()
                }
            };
            let registered = versions().await;
            let deferrals = crate::mysql::mysql_control::saturation_deferrals();

            let mut r = run(&st, port, db, &[&t]).await;
            let held = starve_next_capture(&dsn(port)).await;
            sql(
                port,
                &[
                    &format!("ALTER TABLE {t} ADD COLUMN w INT"),
                    &format!("INSERT INTO {t} VALUES (1, 1, 7)"),
                ],
            )
            .await;
            let ended =
                tokio::time::timeout(Duration::from_secs(60), r.handle.join)
                    .await
                    .expect("stops")
                    .expect("task");
            drop(held);
            let err = ended.expect_err("the saturated proof stops the source");
            assert_eq!(
                err.draft().map(|d| d.reason_code),
                Some(
                    deltaforge_core::incident::ReasonCode::MysqlControlPlaneSaturated
                ),
                "{err}"
            );
            for text in [format!("{err}"), format!("{err:?}")] {
                assert!(
                    !text.contains("root:") && !text.contains(":pw@"),
                    "{text}"
                );
            }
            let mut emitted = Vec::new();
            while let Ok(item) = r.rx.try_recv() {
                if let SourceItem::Event(e) = item {
                    emitted.push(e);
                }
            }
            assert!(
                emitted.iter().all(|e| e.source.table != "t"),
                "neither the DDL nor the row after it: {emitted:?}"
            );
            assert_eq!(st.ckpt.get_raw(db).await.unwrap(), committed);
            let recs = records(&st, db, "t").await;
            for d in ddls(&recs) {
                assert_eq!(bound(&st, db, "t", &recs, d).await, None);
            }
            assert_eq!(versions().await, registered, "no version from it");
            assert_eq!(
                crate::mysql::mysql_control::saturation_deferrals(),
                deferrals,
                "not a deferral"
            );

            // The restart: the DDL replays and is bound; the row after it
            // is delivered once, decoded with the new column.
            let mut r = run(&st, port, db, &[&t]).await;
            ddl_event(&mut r.rx, "ADD COLUMN w").await;
            let recs = records(&st, db, "t").await;
            let d = ddls(&recs);
            assert_eq!(d.len(), 1, "{recs:?}");
            assert!(bound(&st, db, "t", &recs, d[0]).await.is_some());
            let seven = deltaforge_core::canonical::fixture::value(
                &serde_json::json!(7),
            );
            let mut rows = 0;
            let deadline = Instant::now() + Duration::from_secs(10);
            while let Some(left) =
                deadline.checked_duration_since(Instant::now())
            {
                match tokio::time::timeout(left, r.rx.recv()).await {
                    Ok(Some(SourceItem::Event(e)))
                        if e.ddl.is_none() && e.source.table == "t" =>
                    {
                        assert_eq!(
                            e.after.as_ref().unwrap().get("w"),
                            Some(&seven),
                            "decoded with the new column"
                        );
                        rows += 1;
                    }
                    Ok(Some(_)) => {}
                    _ => break,
                }
            }
            assert_eq!(rows, 1, "the row after the DDL, exactly once");
            stop(r.handle).await;
        }

        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_live_ddl_is_bound_by_its_forward_proof_gtid() {
            a_live_ddl_is_bound(true).await;
        }

        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_live_ddl_is_bound_by_its_forward_proof_file_position() {
            a_live_ddl_is_bound(false).await;
        }

        /// The registration and the binding are one atomic batch: a crash at
        /// the binding's write leaves neither (the DDL pending, no version
        /// from it), and the restart proves it again with one new version.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_crash_at_the_binding_leaves_no_registration() {
            let port = server(true).await;
            let db = "fwd_crash";
            prepare(port, db).await;
            let fault = Arc::new(FaultBackend::new());
            let st = state(fault.clone()).await;
            let r = run(&st, port, db, &[&format!("{db}.t")]).await;
            stop(r.handle).await;
            let before = versions(&st, db, "t").await;
            let r = run(&st, port, db, &[&format!("{db}.t")]).await;
            // The DDL's activation writes: its `ddl` record, then the
            // binding, where the process crashes.
            *fault.crash_after_writes_to.lock().unwrap() =
                Some((crate::mysql::mysql_activation::ACTIVATION_NS.into(), 1));
            sql(port, &[&format!("ALTER TABLE {db}.t ADD COLUMN z INT")]).await;
            let res =
                tokio::time::timeout(Duration::from_secs(60), r.handle.join)
                    .await
                    .expect("stops")
                    .expect("task");
            assert!(res.is_err(), "the failed batch stops the source");
            let recs = records(&st, db, "t").await;
            let d = ddls(&recs);
            assert_eq!(d.len(), 1, "the pending record stays");
            assert_eq!(bound(&st, db, "t", &recs, d[0]).await, None);
            assert_eq!(
                versions(&st, db, "t").await,
                before,
                "no registration without its binding"
            );

            // Restart: the DDL replays, is proven and bound once.
            *fault.crash_after_writes_to.lock().unwrap() = None;
            let mut r = run(&st, port, db, &[&format!("{db}.t")]).await;
            ddl_event(&mut r.rx, "ADD COLUMN z").await;
            let recs = records(&st, db, "t").await;
            let d = ddls(&recs);
            assert_eq!(d.len(), 1);
            assert!(bound(&st, db, "t", &recs, d[0]).await.is_some());
            assert_eq!(versions(&st, db, "t").await, before + 1);
            stop(r.handle).await;
        }

        #[tokio::test]
        #[ignore = "requires docker"]
        async fn only_a_clean_interval_proves_the_post_ddl_shape() {
            let port = server(true).await;
            let db = "fwd_scan";
            prepare(port, db).await;
            let st = state(Arc::new(MemoryStorageBackend::new())).await;
            let tables =
                [format!("{db}.t"), format!("{db}.x"), format!("{db}.u")];
            let tables: Vec<&str> = tables.iter().map(String::as_str).collect();
            let r = run(&st, port, db, &tables).await;
            stop(r.handle).await;

            // While stopped (the restart replays them, so each proof's
            // interval holds the later statements):
            sql(
                port,
                &[
                    // a second DDL on t inside the first one's (D, S];
                    &format!("ALTER TABLE {db}.t ADD COLUMN a INT"),
                    &format!("ALTER TABLE {db}.t ADD COLUMN b INT"),
                    // an unrelated table's DDL does not block other's proof;
                    &format!("ALTER TABLE {db}.other ADD COLUMN c INT"),
                    &format!("CREATE TABLE {db}.u (id INT PRIMARY KEY)"),
                    // a DROP gets no proof; the recreate is its own DDL;
                    &format!("DROP TABLE {db}.other"),
                    &format!(
                        "CREATE TABLE {db}.other (id INT PRIMARY KEY, n INT)"
                    ),
                    // a rename binds its destination.
                    &format!("CREATE TABLE {db}.y (id INT PRIMARY KEY)"),
                    &format!("RENAME TABLE {db}.y TO {db}.x"),
                ],
            )
            .await;
            let tracked = [
                format!("{db}.t"),
                format!("{db}.x"),
                format!("{db}.u"),
                format!("{db}.other"),
            ];
            let tracked: Vec<&str> =
                tracked.iter().map(String::as_str).collect();
            let mut r = run(&st, port, db, &tracked).await;
            ddl_event(&mut r.rx, &format!("RENAME TABLE {db}.y TO {db}.x"))
                .await;

            let t = records(&st, db, "t").await;
            let td = ddls(&t);
            assert_eq!(td.len(), 2, "{t:?}");
            assert_eq!(
                bound(&st, db, "t", &t, td[0]).await,
                None,
                "a second DDL in (D, S]"
            );
            assert!(bound(&st, db, "t", &t, td[1]).await.is_some());

            let u = records(&st, db, "u").await;
            assert!(
                bound(&st, db, "u", &u, ddls(&u)[0]).await.is_some(),
                "unrelated DDLs do not block"
            );

            let o = records(&st, db, "other").await;
            let od = ddls(&o);
            assert_eq!(od.len(), 3, "{o:?}"); // ALTER, DROP, CREATE
            assert_eq!(
                bound(&st, db, "other", &o, od[1]).await,
                None,
                "a DROP gets no proof"
            );
            assert!(
                bound(&st, db, "other", &o, od[2]).await.is_some(),
                "the recreate is proven"
            );

            let x = records(&st, db, "x").await;
            assert!(
                bound(&st, db, "x", &x, ddls(&x)[0]).await.is_some(),
                "the rename destination"
            );
            stop(r.handle).await;

            // A lineage barrier in (D, S] leaves the DDL pending.
            sql(
                port,
                &[
                    &format!("ALTER TABLE {db}.u ADD COLUMN q INT"),
                    &format!("CREATE TABLE {db}.w (id INT) COMMENT \"w\""),
                ],
            )
            .await;
            let mut r = run(&st, port, db, &tracked).await;
            ddl_event(&mut r.rx, "CREATE TABLE").await;
            let u = records(&st, db, "u").await;
            let ud = ddls(&u);
            assert_eq!(
                bound(&st, db, "u", &u, ud[ud.len() - 1]).await,
                None,
                "barrier in (D, S]"
            );
            stop(r.handle).await;
        }

        #[tokio::test]
        #[ignore = "requires docker"]
        async fn an_invalid_binding_stops_the_replay_without_a_new_proof() {
            let port = server(true).await;
            let db = "fwd_invalid";
            prepare(port, db).await;
            let st = state(Arc::new(MemoryStorageBackend::new())).await;
            let r = run(&st, port, db, &[&format!("{db}.t")]).await;
            stop(r.handle).await;
            let mut r = run(&st, port, db, &[&format!("{db}.t")]).await;
            sql(port, &[&format!("ALTER TABLE {db}.t ADD COLUMN k INT")]).await;
            ddl_event(&mut r.rx, "ADD COLUMN k").await;
            r.handle.join.abort();

            // A second record binding the same DDL (duplicate evidence).
            let key = st.scope.current().unwrap().key(db, "t");
            let stream = crate::mysql::mysql_activation::table_stream(&key);
            let recs = records(&st, db, "t").await;
            let binding = recs
                .iter()
                .find(|s| matches!(s.record.kind, Kind::Observed { .. }))
                .unwrap();
            st.backend
                .log_append_if_absent(
                    crate::mysql::mysql_activation::ACTIVATION_NS,
                    &stream,
                    "forged",
                    &serde_json::to_vec(&binding.record).unwrap(),
                )
                .await
                .unwrap();
            let tampered = records(&st, db, "t").await;

            // The replay stops on the invalid evidence: no new proof, no DDL
            // event.
            let mut r = run(&st, port, db, &[&format!("{db}.t")]).await;
            let res =
                tokio::time::timeout(Duration::from_secs(60), r.handle.join)
                    .await
                    .expect("stops")
                    .expect("task");
            assert!(
                matches!(&res, Err(SourceError::Schema { details }) if details.contains("2 bindings")),
                "{res:?}"
            );
            while let Ok(item) = r.rx.try_recv() {
                if let SourceItem::Event(e) = item {
                    assert!(e.ddl.is_none(), "the DDL event was emitted");
                }
            }
            assert_eq!(records(&st, db, "t").await, tampered);
        }

        /// A forward proof writes two store transactions: the pending `ddl`
        /// record, then one atomic batch holding the registration and the
        /// binding.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_forward_proof_is_two_write_transactions() {
            let port = server(true).await;
            let db = "fwd_txns";
            prepare(port, db).await;
            let fault = Arc::new(FaultBackend::new());
            let st = state(fault.clone()).await;
            let t = format!("{db}.t");
            let r = run(&st, port, db, &[&t]).await;
            stop(r.handle).await;
            let mut r = run(&st, port, db, &[&t]).await;
            fault.write_calls.lock().unwrap().clear();
            sql(port, &[&format!("ALTER TABLE {t} ADD COLUMN w INT")]).await;
            ddl_event(&mut r.rx, "ADD COLUMN w").await;
            let writes = schema_writes(&fault);
            stop(r.handle).await;
            let recs = records(&st, db, "t").await;
            let d = ddls(&recs);
            assert_eq!(d.len(), 1, "{recs:?}");
            assert!(bound(&st, db, "t", &recs, d[0]).await.is_some());
            assert_eq!(
                writes,
                [crate::mysql::mysql_activation::ACTIVATION_NS, "batch"],
                "the pending record, then one batch"
            );
        }

        /// The tables of a multi-table DDL that do not fit one batch are
        /// committed in several, each table wholly in one. A crash between
        /// them leaves the first bound and the second pending with no
        /// version from the proof; the restart validates the first binding
        /// and proves the second.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_multi_table_ddl_over_the_budget_commits_per_table_batches() {
            let port = server(true).await;
            let db = "fwd_split";
            sql(
                port,
                &[
                    &format!("DROP DATABASE IF EXISTS {db}"),
                    &format!("CREATE DATABASE {db}"),
                    &format!("CREATE TABLE {db}.a (id INT PRIMARY KEY)"),
                    &format!("CREATE TABLE {db}.c (id INT PRIMARY KEY)"),
                ],
            )
            .await;
            let fault = Arc::new(FaultBackend::new());
            let st = state(fault.clone()).await;
            let tracked = [format!("{db}.b"), format!("{db}.d")];
            let tracked: Vec<&str> =
                tracked.iter().map(String::as_str).collect();
            let r = run(&st, port, db, &tracked).await;
            stop(r.handle).await;
            // One table per batch: a table's plan reads fit, two do not.
            crate::stream_probe::set_window_caps(Some((
                crate::mysql::mysql_window_budget::PLAN_BYTES,
                40,
                crate::mysql::mysql_window_budget::TABLE_BYTES,
            )));
            let r = run(&st, port, db, &tracked).await;
            fault.write_calls.lock().unwrap().clear();
            // The `ddl` records of both names of both tables and the first
            // batch's binding, then a crash at the second batch.
            *fault.crash_after_writes_to.lock().unwrap() =
                Some((crate::mysql::mysql_activation::ACTIVATION_NS.into(), 5));
            sql(
                port,
                &[&format!("RENAME TABLE {db}.a TO {db}.b, {db}.c TO {db}.d")],
            )
            .await;
            let res =
                tokio::time::timeout(Duration::from_secs(60), r.handle.join)
                    .await
                    .expect("stops")
                    .expect("task");
            let writes = schema_writes(&fault);
            assert!(res.is_err(), "the failed batch stops the source");
            let activation = crate::mysql::mysql_activation::ACTIVATION_NS;
            assert_eq!(
                writes,
                [activation, activation, activation, activation, "batch"],
                "the pending records, then the first table's batch; the \
                 second table's never reached the store"
            );
            let b = records(&st, db, "b").await;
            assert!(bound(&st, db, "b", &b, ddls(&b)[0]).await.is_some());
            let d = records(&st, db, "d").await;
            assert_eq!(bound(&st, db, "d", &d, ddls(&d)[0]).await, None);
            assert_eq!(versions(&st, db, "d").await, 0, "no version for d");

            *fault.crash_after_writes_to.lock().unwrap() = None;
            let mut r = run(&st, port, db, &tracked).await;
            ddl_event(&mut r.rx, "RENAME TABLE").await;
            crate::stream_probe::set_window_caps(None);
            for t in ["b", "d"] {
                let recs = records(&st, db, t).await;
                assert!(
                    bound(&st, db, t, &recs, ddls(&recs)[0]).await.is_some()
                );
                assert_eq!(versions(&st, db, t).await, 1, "{t}");
            }
            stop(r.handle).await;
        }

        /// A table whose own contribution exceeds an empty batch stops the
        /// proof with the classified incident before anything of it is
        /// written: the DDL stays pending, no version, no checkpoint
        /// advance.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn an_oversized_table_stops_the_proof_with_nothing_written() {
            let port = server(true).await;
            let db = "fwd_oversized";
            prepare(port, db).await;
            let st = state(Arc::new(MemoryStorageBackend::new())).await;
            let t = format!("{db}.t");
            let r = run(&st, port, db, &[&t]).await;
            stop(r.handle).await;
            let committed = st.ckpt.get_raw(db).await.unwrap();
            let before = versions(&st, db, "t").await;
            crate::stream_probe::set_window_caps(Some((
                crate::mysql::mysql_window_budget::PLAN_BYTES,
                16_384,
                10,
            )));
            let r = run(&st, port, db, &[&t]).await;
            sql(port, &[&format!("ALTER TABLE {t} ADD COLUMN w INT")]).await;
            let res =
                tokio::time::timeout(Duration::from_secs(60), r.handle.join)
                    .await
                    .expect("stops")
                    .expect("task");
            crate::stream_probe::set_window_caps(None);
            let err = res.expect_err("the oversized table stops the source");
            assert_eq!(
                err.draft().map(|d| d.reason_code),
                Some(deltaforge_core::incident::ReasonCode::MysqlTableStateTooLarge),
                "{err}"
            );
            let recs = records(&st, db, "t").await;
            let d = ddls(&recs);
            assert_eq!(d.len(), 1, "the pending record stays");
            assert_eq!(bound(&st, db, "t", &recs, d[0]).await, None);
            assert_eq!(versions(&st, db, "t").await, before);
            assert_eq!(st.ckpt.get_raw(db).await.unwrap(), committed);
        }

        /// Another registration of the same table between the plan and the
        /// commit fails the batch's preconditions: nothing is written, the
        /// proof plans again and numbers after it, as the serial path does.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_concurrent_registration_replans_the_proof() {
            let port = server(true).await;
            let db = "fwd_conflict";
            prepare(port, db).await;
            let fault = Arc::new(FaultBackend::new());
            let st = state(fault.clone()).await;
            let t = format!("{db}.t");
            let r = run(&st, port, db, &[&t]).await;
            stop(r.handle).await;
            let mut r = run(&st, port, db, &[&t]).await;
            let key = st.scope.current().unwrap().key(db, "t");
            let injected = Arc::new(std::sync::Mutex::new(None));
            let seen = injected.clone();
            let k = key.clone();
            *fault.before_next_batch.lock().unwrap() =
                Some(storage::adapters::test_util::BatchHook(Box::new(
                    move |inner| {
                        Box::pin(async move {
                            let other = DurableSchemaRegistry::new(inner)
                                .await
                                .unwrap();
                            let v = other
                                .register_with_checkpoint(
                                    &k,
                                    "concurrent",
                                    &serde_json::json!({"concurrent": true}),
                                    None,
                                )
                                .await
                                .unwrap();
                            *seen.lock().unwrap() = Some(v);
                        })
                    },
                )));
            sql(port, &[&format!("ALTER TABLE {t} ADD COLUMN w INT")]).await;
            ddl_event(&mut r.rx, "ADD COLUMN w").await;
            stop(r.handle).await;
            let other = injected.lock().unwrap().expect("the hook ran");
            let recs = records(&st, db, "t").await;
            let d = ddls(&recs);
            let version = bound(&st, db, "t", &recs, d[0])
                .await
                .expect("the DDL is bound");
            assert_eq!(version, other + 1, "numbered after the other");
            let shape = st.registry.version_hash(&key, version).await.unwrap();
            assert_ne!(shape.as_deref(), Some("concurrent"));
        }

        /// A registration that conflicts on every attempt stops the source
        /// after the bounded re-plans: the DDL stays pending, unbound, with
        /// no version from the proof.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_proof_that_always_conflicts_stops_with_the_ddl_pending() {
            fn arm(
                fault: Arc<FaultBackend>,
                key: storage::adapters::SchemaKey,
                n: Arc<std::sync::atomic::AtomicUsize>,
            ) {
                let again = fault.clone();
                *fault.before_next_batch.lock().unwrap() =
                    Some(storage::adapters::test_util::BatchHook(Box::new(
                        move |inner| {
                            Box::pin(async move {
                                let i = n.fetch_add(
                                    1,
                                    std::sync::atomic::Ordering::SeqCst,
                                );
                                let other = DurableSchemaRegistry::new(inner)
                                    .await
                                    .unwrap();
                                other
                                    .register_with_checkpoint(
                                        &key,
                                        &format!("concurrent-{i}"),
                                        &serde_json::json!({"concurrent": i}),
                                        None,
                                    )
                                    .await
                                    .unwrap();
                                arm(again, key, n);
                            })
                        },
                    )));
            }
            let port = server(true).await;
            let db = "fwd_conflict_always";
            prepare(port, db).await;
            let fault = Arc::new(FaultBackend::new());
            let st = state(fault.clone()).await;
            let t = format!("{db}.t");
            let r = run(&st, port, db, &[&t]).await;
            stop(r.handle).await;
            let committed = st.ckpt.get_raw(db).await.unwrap();
            let r = run(&st, port, db, &[&t]).await;
            let key = st.scope.current().unwrap().key(db, "t");
            let conflicts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            arm(fault.clone(), key.clone(), conflicts.clone());
            sql(port, &[&format!("ALTER TABLE {t} ADD COLUMN w INT")]).await;
            let res =
                tokio::time::timeout(Duration::from_secs(60), r.handle.join)
                    .await
                    .expect("stops")
                    .expect("task");
            *fault.before_next_batch.lock().unwrap() = None;
            assert!(res.is_err(), "{res:?}");
            let recs = records(&st, db, "t").await;
            let d = ddls(&recs);
            assert_eq!(d.len(), 1, "the pending record stays");
            assert_eq!(bound(&st, db, "t", &recs, d[0]).await, None);
            let history =
                crate::mysql::mysql_activation::tests_support::history(
                    &st.registry,
                    &key,
                )
                .await;
            assert!(
                history.iter().all(|h| !h.contains("\"w\"")),
                "no version from the proof: {history:?}"
            );
            assert_eq!(st.ckpt.get_raw(db).await.unwrap(), committed);
        }

        // ---- C3: installing a proven table's timeline -------------------

        /// The table stream reads of `db.table` made since the last clear.
        fn table_reads(
            fault: &FaultBackend,
            st: &State,
            db: &str,
            t: &str,
        ) -> usize {
            let key = st.scope.current().unwrap().key(db, t);
            let stream = crate::mysql::mysql_activation::table_stream(&key);
            fault
                .stream_reads
                .lock()
                .unwrap()
                .iter()
                .filter(|(ns, k)| {
                    ns == crate::mysql::mysql_activation::ACTIVATION_NS
                        && *k == stream
                })
                .count()
        }

        /// The timelines installed for `db.table` since the last clear.
        fn installed(
            st: &State,
            db: &str,
            t: &str,
        ) -> Vec<(u64, crate::mysql::mysql_selection::Timeline)> {
            let key = st.scope.current().unwrap().key(db, t).backend_key();
            super::super::hooks::INSTALLED
                .lock()
                .unwrap()
                .iter()
                .filter(|(k, _, _)| *k == key)
                .map(|(_, b, t)| (*b, t.clone()))
                .collect()
        }

        /// Wait for a row event of `table` whose `w` is `w`.
        async fn row_with_w(
            rx: &mut mpsc::Receiver<SourceItem>,
            table: &str,
            w: i64,
        ) {
            let want = deltaforge_core::canonical::fixture::value(
                &serde_json::json!(w),
            );
            let deadline = Instant::now() + Duration::from_secs(60);
            while let Some(left) =
                deadline.checked_duration_since(Instant::now())
            {
                match tokio::time::timeout(left, rx.recv()).await {
                    Ok(Some(SourceItem::Event(e)))
                        if e.ddl.is_none()
                            && e.source.table == table
                            && e.after.as_ref().and_then(|a| a.get("w"))
                                == Some(&want) =>
                    {
                        return;
                    }
                    Ok(Some(_)) => {}
                    _ => break,
                }
            }
            panic!("no row of {table} with w = {w}");
        }

        /// A record appended to `stream` by another writer, straight into
        /// `backend` (no source, no cache).
        async fn foreign_append(
            backend: &ArcStorageBackend,
            ns: &str,
            stream: &str,
            id: &str,
            kind: Kind,
        ) {
            let record = crate::mysql::mysql_activation::Record::new(
                crate::durable_checkpoint::WmPos::MysqlGtid {
                    gtid_set: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-1000000"
                        .into(),
                },
                kind,
            );
            backend
                .log_append_if_absent(
                    ns,
                    stream,
                    id,
                    &crate::mysql::mysql_activation::encode_record(&record)
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        /// A tracked table, cached by a row before its DDL, on a store whose
        /// gate this process holds (`gated`) or not, with the C3 probes
        /// cleared; returns the run and the table's key.
        async fn cached_table(
            db: &str,
            gated: bool,
        ) -> (Arc<FaultBackend>, State, Run, u16) {
            let port = server(true).await;
            prepare(port, db).await;
            let fault = Arc::new(FaultBackend::new());
            let st = state(fault.clone()).await;
            if gated {
                let backend: ArcStorageBackend = fault.clone();
                storage::adapters::store_gate::acquire(
                    &backend,
                    storage::adapters::store_gate::GateRole::Server,
                )
                .await
                .unwrap();
                assert!(storage::adapters::store_gate::server_holds(
                    &st.backend
                ));
            }
            let t = format!("{db}.t");
            let r = run(&st, port, db, &[&t]).await;
            stop(r.handle).await;
            let mut r = run(&st, port, db, &[&t]).await;
            sql(port, &[&format!("INSERT INTO {t} VALUES (1, 1)")]).await;
            let mut seen = false;
            let deadline = Instant::now() + Duration::from_secs(30);
            while !seen && Instant::now() < deadline {
                if let Ok(Some(SourceItem::Event(e))) =
                    tokio::time::timeout(Duration::from_secs(1), r.rx.recv())
                        .await
                {
                    seen = e.ddl.is_none() && e.source.table == "t";
                }
            }
            assert!(seen, "the row before the DDL caches the timeline");
            fault.stream_reads.lock().unwrap().clear();
            fault
                .batched_stream_reads
                .store(0, std::sync::atomic::Ordering::SeqCst);
            super::super::hooks::INSTALLED.lock().unwrap().clear();
            *super::super::hooks::BEFORE_INSTALL.lock().unwrap() = None;
            (fault, st, r, port)
        }

        /// With the store-gate witness, a cached table's proven timeline is
        /// installed: the row after the DDL needs no history load (the table
        /// stream is read twice: the replay check and the freshness check),
        /// and the installed timeline equals a fresh read of the store
        /// (records, head and bytes).
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_cached_proven_table_takes_its_records_without_a_reload() {
            let db = "fwd_install";
            let (fault, st, mut r, port) = cached_table(db, true).await;
            sql(
                port,
                &[
                    &format!("ALTER TABLE {db}.t ADD COLUMN w INT"),
                    &format!("INSERT INTO {db}.t VALUES (2, 2, 7)"),
                ],
            )
            .await;
            ddl_event(&mut r.rx, "ADD COLUMN w").await;
            row_with_w(&mut r.rx, "t", 7).await;
            assert_eq!(
                table_reads(&fault, &st, db, "t"),
                2,
                "the replay check and the freshness check"
            );
            assert_eq!(
                fault
                    .batched_stream_reads
                    .load(std::sync::atomic::Ordering::SeqCst),
                0,
                "no history load"
            );
            stop(r.handle).await;
            let got = installed(&st, db, "t");
            assert_eq!(got.len(), 1);
            let key = st.scope.current().unwrap().key(db, "t");
            let stream = crate::mysql::mysql_activation::table_stream(&key);
            let entries = st
                .backend
                .log_read_meta_since(
                    crate::mysql::mysql_activation::ACTIVATION_NS,
                    &stream,
                    0,
                    10_000,
                )
                .await
                .unwrap();
            let fresh = records(&st, db, "t").await;
            let (snapshot_bytes, got) = &got[0];
            assert_eq!(got.records, fresh, "the records a load reads");
            assert_eq!(got.table_head, entries.last().unwrap().seq);
            assert_eq!(
                got.bytes - snapshot_bytes,
                entries[entries.len() - 2..]
                    .iter()
                    .map(storage::log_entry_bytes)
                    .sum::<u64>(),
                "the snapshot's estimate and its two new entries, as a load \
                 counts them"
            );
        }

        /// Without the witness, nothing is installed: the proven table
        /// reloads, and its row after the DDL is decoded with its new shape.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn without_the_store_gate_witness_nothing_is_installed() {
            let db = "fwd_no_witness";
            let (fault, st, mut r, port) = cached_table(db, false).await;
            sql(
                port,
                &[
                    &format!("ALTER TABLE {db}.t ADD COLUMN w INT"),
                    &format!("INSERT INTO {db}.t VALUES (2, 2, 7)"),
                ],
            )
            .await;
            ddl_event(&mut r.rx, "ADD COLUMN w").await;
            row_with_w(&mut r.rx, "t", 7).await;
            stop(r.handle).await;
            assert!(installed(&st, db, "t").is_empty());
            assert!(
                fault
                    .batched_stream_reads
                    .load(std::sync::atomic::Ordering::SeqCst)
                    >= 1,
                "the table reloads"
            );
        }

        /// Another writer's append to the table stream, or to a barrier
        /// stream of the table, before the proof's commit: the freshness
        /// check sees it and nothing is installed.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn an_append_or_barrier_before_the_check_prevents_the_install() {
            use crate::mysql::mysql_activation::{
                ACTIVATION_NS, BARRIER_NS, BarrierScope, barrier_stream,
                table_stream,
            };
            for (i, barrier) in [false, true].into_iter().enumerate() {
                let db = if barrier {
                    "fwd_pre_barrier"
                } else {
                    "fwd_pre_append"
                };
                let (fault, st, mut r, port) = cached_table(db, true).await;
                let key = st.scope.current().unwrap().key(db, "t");
                let (ns, stream, kind) = if barrier {
                    let scope = BarrierScope::Database { db: db.into() };
                    (
                        BARRIER_NS,
                        barrier_stream(&key, &scope),
                        Kind::Barrier { scope },
                    )
                } else {
                    (ACTIVATION_NS, table_stream(&key), Kind::Ddl)
                };
                *fault.before_next_batch.lock().unwrap() =
                    Some(storage::adapters::test_util::BatchHook(Box::new(
                        move |inner| {
                            Box::pin(async move {
                                foreign_append(
                                    &inner,
                                    ns,
                                    &stream,
                                    &format!("foreign-{i}"),
                                    kind,
                                )
                                .await;
                            })
                        },
                    )));
                sql(port, &[&format!("ALTER TABLE {db}.t ADD COLUMN w INT")])
                    .await;
                ddl_event(&mut r.rx, "ADD COLUMN w").await;
                stop(r.handle).await;
                assert!(installed(&st, db, "t").is_empty(), "{db}");
            }
        }

        /// An in-process barrier or eviction between the check and the
        /// install: the cache no longer holds the snapshot, and nothing is
        /// installed.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn an_invalidation_after_the_check_prevents_the_install() {
            use crate::mysql::mysql_activation::{
                BarrierScope, barrier_stream,
            };
            for evict in [false, true] {
                let db = if evict {
                    "fwd_post_evict"
                } else {
                    "fwd_post_barrier"
                };
                let (_fault, st, mut r, port) = cached_table(db, true).await;
                let key = st.scope.current().unwrap().key(db, "t");
                let lineage = barrier_stream(&key, &BarrierScope::Lineage);
                *super::super::hooks::BEFORE_INSTALL.lock().unwrap() =
                    Some(Box::new(move |ctx| {
                        if evict {
                            ctx.selection.invalidate_all();
                        } else {
                            ctx.selection.barrier_appended(&lineage);
                        }
                    }));
                sql(port, &[&format!("ALTER TABLE {db}.t ADD COLUMN w INT")])
                    .await;
                ddl_event(&mut r.rx, "ADD COLUMN w").await;
                stop(r.handle).await;
                assert!(installed(&st, db, "t").is_empty(), "{db}");
            }
        }

        /// Another process's write between the check and the install is not
        /// seen: freshness holds at the check only. With the witness the
        /// install goes ahead without it (the timeline then misses the
        /// write), which is why C3 rests on ownership: while this process
        /// holds the gate, no other server can start on the store. Without
        /// the witness the same write is followed by a reload.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_write_after_the_check_is_excluded_only_by_ownership() {
            use crate::mysql::mysql_activation::{ACTIVATION_NS, table_stream};
            for gated in [true, false] {
                let db = if gated {
                    "fwd_post_gated"
                } else {
                    "fwd_post_open"
                };
                let (_fault, st, mut r, port) = cached_table(db, gated).await;
                let key = st.scope.current().unwrap().key(db, "t");
                let stream = table_stream(&key);
                let backend = st.backend.clone();
                *super::super::hooks::BEFORE_INSTALL.lock().unwrap() =
                    Some(Box::new(move |_ctx| {
                        // Another process: its own runtime, on its own thread.
                        std::thread::spawn(move || {
                            tokio::runtime::Builder::new_current_thread()
                                .build()
                                .unwrap()
                                .block_on(foreign_append(
                                    &backend,
                                    ACTIVATION_NS,
                                    &stream,
                                    "foreign-after-check",
                                    Kind::Ddl,
                                ));
                        })
                        .join()
                        .unwrap();
                    }));
                sql(port, &[&format!("ALTER TABLE {db}.t ADD COLUMN w INT")])
                    .await;
                ddl_event(&mut r.rx, "ADD COLUMN w").await;
                stop(r.handle).await;
                let got = installed(&st, db, "t");
                if gated {
                    assert_eq!(got.len(), 1);
                    assert_ne!(
                        got[0].1.records,
                        records(&st, db, "t").await,
                        "not seen: the interval is excluded by ownership only"
                    );
                    let err = storage::adapters::store_gate::acquire(
                        &st.backend,
                        storage::adapters::store_gate::GateRole::Server,
                    )
                    .await
                    .expect_err("a second server cannot start on the store");
                    assert!(matches!(
                        err,
                        storage::adapters::store_gate::GateError::Held(_)
                    ));
                } else {
                    assert!(got.is_empty());
                }
            }
        }
    }
}
