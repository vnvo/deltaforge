//! Activation baselines (design spec 7.7, binding rulings Rounds 25 and 29).
//!
//! When the stream starts at R0 and a tracked table's timeline does not
//! already prove its version there (a first start, a snapshot anchor, a
//! deployment upgraded from a version without timelines), its version is
//! established read-only:
//!
//! 1. a stable capture of the table's shape (position, shape, position equal
//!    on one verified connection) at S_t;
//! 2. one complete scan of (R0, S], S being the last capture's position (at
//!    or after every S_t), with the approved classifier;
//! 3. only if the scan proves no applicable table DDL and no barrier-class
//!    statement for the table in (R0, S]: the captured shape is registered
//!    through the normal registry path and a `baseline` record is appended at
//!    R0. The shape captured at S_t is the shape at S (nothing changed it in
//!    (S_t, S]) and at R0 (nothing changed it in (R0, S_t]).
//!
//! A baseline at the same position as the start's discontinuity barrier
//! carries that barrier's exact capture identity (`binds`); selection lets it
//! supersede that barrier only. Anything uncertain writes no activation
//! proof (a captured shape may already be registered): the table stays
//! unproven at R0.

use deltaforge_core::{CheckpointOrder, SourceError, SourceResult};
use schema_registry::SourceSchema as _;
use tracing::{info, warn};

use super::mysql_activation::{
    self, Kind, Record, Selection, Stored, baseline_capture_id, select,
    table_stream,
};
use super::mysql_activation_window::PreparedOutcome;
use super::mysql_binlog_scan::{
    CLASSIFIER_VERSION, ProofError, ProofKind, ScanReport, ScanTag, Statement,
    capture, covers, observe_capture, trace_proof,
};
use super::mysql_ddl_attribution::{BarrierScopeOf, DdlEffect, same_name};
use super::mysql_selection::Timeline;
use super::{MySqlCheckpoint, RunCtx};
use crate::durable_checkpoint::{
    WmPos, mysql_checkpoint_position, order_positions,
};
use storage::adapters::SchemaKey;

fn other_server(expected: &str, found: &str) -> SourceError {
    SourceError::Lineage {
        details: format!(
            "a baseline connection reached another server ({found}), \
             expected {expected}"
        )
        .into(),
    }
}

/// Whether a statement in (R0, S] may have changed `db.table` or invalidated
/// positional proof for it.
pub(crate) fn affects(
    statement: &Statement,
    db: &str,
    table: &str,
    lower_case_table_names: u8,
) -> bool {
    let lctn = lower_case_table_names;
    match &statement.effect {
        DdlEffect::None | DdlEffect::SameShape(_) => false,
        DdlEffect::Barrier(BarrierScopeOf::Lineage) => true,
        // Names compared case-insensitively but stored as written: not
        // mappable with certainty.
        _ if lctn == 2 => true,
        DdlEffect::Barrier(BarrierScopeOf::Database(d)) => {
            same_name(d, db, lctn)
        }
        DdlEffect::Tables(tables) => tables.iter().any(|t| {
            same_name(&t.db, db, lctn) && same_name(&t.table, table, lctn)
        }),
    }
}

/// The capture identity of the one barrier exactly at `r0` (the start's
/// discontinuity barrier) for the baseline to bind. `Err` when the position
/// is shared with anything a baseline cannot supersede: several barriers, or
/// a table record.
pub(crate) fn barrier_to_bind(
    table: &[Stored],
    barriers: &[Stored],
    r0: &WmPos,
) -> Result<Option<String>, &'static str> {
    let at = |s: &&Stored| {
        order_positions(&s.record.position, r0) == CheckpointOrder::Equal
    };
    if table.iter().any(|s| at(&s)) {
        return Err("a table record at the start position");
    }
    let mut here = barriers.iter().filter(at);
    match (here.next(), here.next()) {
        (None, _) => Ok(None),
        (Some(b), None) => Ok(Some(b.capture_id.clone())),
        _ => Err("several barriers at the start position"),
    }
}

struct Candidate {
    db: String,
    table: String,
    binds: Option<String>,
}

/// Test only: [`establish`] takes the per-table path ([`establish_per_table`],
/// the one before batched admission), so its stored outcome can be compared.
#[cfg(test)]
pub(crate) static PER_TABLE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Test hooks of the batched [`establish`].
#[cfg(test)]
pub(crate) mod hooks {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize};

    /// Run `establish` again at the same R0 right after it; what the
    /// replay wrote lands in `REPLAYED`.
    pub static REPLAY: AtomicBool = AtomicBool::new(false);
    pub static REPLAYED: AtomicUsize = AtomicUsize::new(usize::MAX);
    /// Fail the run after this many chunks committed (0: never).
    pub static FAIL_AFTER_CHUNKS: AtomicUsize = AtomicUsize::new(0);
    /// Chunks committed or decided.
    pub static CHUNKS: AtomicUsize = AtomicUsize::new(0);
    /// Run once just before a chunk's commit.
    pub static BEFORE_COMMIT: Mutex<Option<Box<dyn FnOnce() + Send>>> =
        Mutex::new(None);
}

/// Establish baselines at the stream's start position (the snapshot anchor:
/// called only after a snapshot, for the tables it copied) for every table
/// not already proven there, through batched admission
/// (`docs/design/mysql-mass-invalidation.md`, section 4): the candidates,
/// in discovery order, in chunks of at most `MAX_BATCH_TABLES`, each with
/// one capture of all its tables, its own end S, one scan of (R0, S] and
/// one atomic commit of its registrations and baselines. A chunk the
/// window limits cut short continues with its untouched suffix. The
/// per-table decisions are unchanged: an uncertain or failed proof only
/// leaves the table unproven (logged); storage failures are fatal.
pub(crate) async fn establish(
    ctx: &RunCtx,
    tracked: &[(String, String)],
) -> SourceResult<usize> {
    #[cfg(test)]
    if PER_TABLE.load(std::sync::atomic::Ordering::SeqCst) {
        return establish_per_table(ctx, tracked).await;
    }
    let scope = ctx.registry_scope.current()?;
    let lineage = scope.lineage().lineage_hash.clone();
    let r0_cp = MySqlCheckpoint {
        file: ctx.last_file.clone(),
        pos: ctx.last_pos,
        gtid_set: ctx.gtid_text(),
        lineage: Some(lineage.clone()),
        snapshot_completed: None,
        snapshot_chain: None,
    };
    let Some(r0) = mysql_checkpoint_position(
        &r0_cp.file,
        r0_cp.pos,
        r0_cp.gtid_set.as_deref(),
    ) else {
        warn!(source_id = %ctx.source_id, "unparseable start position: no baselines");
        return Ok(0);
    };
    let candidates = candidates(ctx, tracked, &r0).await?;
    let mut written = 0;
    let mut at = 0;
    while at < candidates.len() {
        let end = (at + super::mysql_binlog_scan::MAX_BATCH_TABLES)
            .min(candidates.len());
        let chunk =
            establish_chunk(ctx, &candidates[at..end], &r0, &r0_cp).await?;
        written += chunk.written;
        at += chunk.consumed.max(1);
        #[cfg(test)]
        {
            use std::sync::atomic::Ordering::SeqCst;
            let n = hooks::CHUNKS.fetch_add(1, SeqCst) + 1;
            if hooks::FAIL_AFTER_CHUNKS.load(SeqCst) == n {
                return Err(SourceError::Other(anyhow::anyhow!(
                    "injected failure after {n} chunk(s)"
                )));
            }
        }
    }
    #[cfg(test)]
    if hooks::REPLAY.swap(false, std::sync::atomic::Ordering::SeqCst) {
        let again = Box::pin(establish(ctx, tracked)).await?;
        hooks::REPLAYED.store(again, std::sync::atomic::Ordering::SeqCst);
    }
    if written > 0 {
        info!(source_id = %ctx.source_id, baselines = written, "activation baselines established");
    }
    Ok(written)
}

/// Each tracked table not proven at R0, in the given (discovery) order,
/// with its timeline (records, barriers and their heads) and the barrier it
/// binds.
async fn candidates(
    ctx: &RunCtx,
    tracked: &[(String, String)],
    r0: &WmPos,
) -> SourceResult<Vec<(Candidate, Timeline)>> {
    let scope = ctx.registry_scope.current()?;
    let storage = |e: anyhow::Error| {
        SourceError::Other(e.context("read the activation timeline"))
    };
    let mut out = Vec::new();
    for (db, table) in tracked {
        let key = scope.key(db, table);
        let (records, table_head) = mysql_activation::read_table_head(
            &ctx.registry_backend,
            ctx.schema.registry(),
            &key,
        )
        .await
        .map_err(storage)?;
        let (barriers, barrier_heads) =
            mysql_activation::read_barriers_head(&ctx.registry_backend, &key)
                .await
                .map_err(storage)?;
        let all: Vec<Stored> =
            records.iter().chain(barriers.iter()).cloned().collect();
        if matches!(select(&all, r0), Selection::Proven { .. }) {
            continue;
        }
        match barrier_to_bind(&records, &barriers, r0) {
            Ok(binds) => out.push((
                Candidate {
                    db: db.clone(),
                    table: table.clone(),
                    binds,
                },
                Timeline {
                    records,
                    barriers,
                    table_head,
                    barrier_heads,
                },
            )),
            Err(why) => {
                warn!(source_id = %ctx.source_id, %db, %table, why, "no baseline");
            }
        }
    }
    Ok(out)
}

/// What one chunk did: the leading candidates it decided (at least one),
/// and the baselines it wrote.
struct ChunkDone {
    consumed: usize,
    written: usize,
}

/// One chunk: one batched capture in discovery order, S its end, one scan
/// of (R0, S], then the decisions of its prefix (`mysql_window_budget`)
/// and one atomic commit of their registrations (checkpoint R0) and
/// baselines. Tables past the prefix are untouched, decided by the next
/// chunk.
async fn establish_chunk(
    ctx: &RunCtx,
    chunk: &[(Candidate, Timeline)],
    r0: &WmPos,
    r0_cp: &MySqlCheckpoint,
) -> SourceResult<ChunkDone> {
    use super::mysql_binlog_scan::{BatchCaptureError, capture_batch};
    use super::mysql_window_budget::{Caps, Prefix, table_cost};
    use storage::adapters::{RegistrationPlanning, RegistrationRequest};
    use storage::{
        AtomicBatch, BatchOp, CommitResult, Precondition, commit_atomic_counted,
    };

    let scope = ctx.registry_scope.current()?;
    let lineage = scope.lineage().lineage_hash.clone();
    let server_uuid = ctx.expected_uuid()?;
    let source_id = ctx.source_id.clone();
    let tag = ScanTag {
        source_id: &source_id,
        kind: ProofKind::Snapshot,
    };
    let lctn = ctx.lower_case_table_names;
    let skip_all = |why: &'static str| {
        for (c, _) in chunk {
            trace_proof(&tag, &c.db, &c.table, None, None, lctn, why);
        }
        Ok(ChunkDone {
            consumed: chunk.len(),
            written: 0,
        })
    };
    let tables: Vec<(String, String)> = chunk
        .iter()
        .map(|(c, _)| (c.db.clone(), c.table.clone()))
        .collect();

    // 1. One capture of every table, in discovery order. A stop ends the
    // baselines (`?`); another server is never a "no baseline".
    let cap = match capture_batch(
        &ctx.capture,
        &server_uuid,
        &lineage,
        &tables,
        ctx.proofs.capture_lock_wait_secs,
        None,
    )
    .await?
    {
        Ok(cap) => cap,
        Err(BatchCaptureError::Failed(ProofError::OtherServer(found))) => {
            return Err(other_server(&server_uuid, &found));
        }
        Err(e) => {
            warn!(source_id = %ctx.source_id, error = ?e, tables = chunk.len(), "no baselines: the chunk's capture failed");
            return skip_all("no_capture");
        }
    };
    // The capture's own budget: the chunk ends at its first unread table,
    // which leads the next chunk; unread as the first, its own rows exceed
    // the item limit.
    let mut limit = chunk
        .iter()
        .position(|(c, _)| {
            cap.deferred.contains(&(c.db.clone(), c.table.clone()))
        })
        .unwrap_or(chunk.len());
    if limit == 0 {
        let (c, _) = &chunk[0];
        let too = storage::StateItemTooLarge {
            size: cap.oversized.unwrap_or_default() as usize,
            key_digest: storage::state_key_digest(
                storage::adapters::schema_registry::V1_NS,
                &scope.key(&c.db, &c.table).backend_key(),
            ),
        };
        return Err(crate::incident_drafts::state_item_too_large(
            &ctx.source_id,
            &c.db,
            &c.table,
            &too,
            r0_cp,
        ));
    }
    // The window limits: the longest prefix whose complete costs fit.
    let mut prefix = Prefix::new(Caps::current());
    for (i, (c, t)) in chunk[..limit].iter().enumerate() {
        let key = scope.key(&c.db, &c.table);
        let cost = match cap.schemas.get(&(c.db.clone(), c.table.clone())) {
            None => Default::default(),
            Some(schema) => table_cost(
                serde_json::to_vec(schema)
                    .map_err(|e| SourceError::Other(e.into()))?
                    .len() as u64,
                t,
                0,
                key.backend_key().len() as u64,
            )
            .map_err(SourceError::Other)?,
        };
        match prefix.offer(cost) {
            Ok(true) => {}
            Ok(false) => {
                limit = i;
                break;
            }
            Err(invalid) => {
                return Err(crate::incident_drafts::mysql_table_too_large(
                    &ctx.source_id,
                    &c.db,
                    &c.table,
                    &invalid,
                    r0_cp,
                ));
            }
        }
    }
    let chunk = &chunk[..limit.max(1)];
    let s_cp = cap.position.clone();
    let Some(s) = mysql_checkpoint_position(
        &s_cp.file,
        s_cp.pos,
        s_cp.gtid_set.as_deref(),
    ) else {
        return skip_all("no_capture");
    };

    // 2. One complete scan of (R0, S].
    let report: ScanReport = match super::mysql_proof_scanner::shared_interval(
        &ctx.proof_scanner,
        ctx.dsn.current().expose(),
        &ctx.source_id,
        &server_uuid,
        &lineage,
        r0_cp,
        &s_cp,
        None,
        Some(r0_cp),
        &tag,
    )
    .await
    {
        Ok(report) => report,
        Err(ProofError::OtherServer(found)) => {
            return Err(other_server(&server_uuid, &found));
        }
        Err(e) => {
            warn!(source_id = %ctx.source_id, error = ?e, "no baselines: the interval scan failed");
            for (c, _) in chunk {
                let tcap = cap.table(&c.db, &c.table);
                trace_proof(
                    &tag,
                    &c.db,
                    &c.table,
                    tcap.as_ref(),
                    None,
                    lctn,
                    "scan_failed",
                );
            }
            return Ok(ChunkDone {
                consumed: chunk.len(),
                written: 0,
            });
        }
    };

    // 3. Each table's decision, exactly as one at a time: no capture, not
    // covered or a relevant statement leaves it unproven, with nothing
    // registered or recorded.
    let checkpoint =
        serde_json::to_vec(r0_cp).map_err(|e| SourceError::Other(e.into()))?;
    let mut proven: Vec<(&Candidate, super::mysql_binlog_scan::Captured)> =
        Vec::new();
    for (c, _) in chunk {
        let Some(tcap) = cap.table(&c.db, &c.table) else {
            warn!(source_id = %ctx.source_id, db = %c.db, table = %c.table, "no baseline: no capture");
            trace_proof(&tag, &c.db, &c.table, None, None, lctn, "no_capture");
            continue;
        };
        observe_capture(&tag, &tcap);
        if !covers(&tcap, r0, &s) {
            warn!(source_id = %ctx.source_id, db = %c.db, table = %c.table, "no baseline: the scan does not cover the capture interval");
            trace_proof(
                &tag,
                &c.db,
                &c.table,
                Some(&tcap),
                Some(&report),
                lctn,
                "not_covered",
            );
            continue;
        }
        if report
            .statements
            .iter()
            .any(|st| affects(st, &c.db, &c.table, lctn))
        {
            info!(source_id = %ctx.source_id, db = %c.db, table = %c.table, "no baseline: a DDL or barrier in the scanned interval");
            trace_proof(
                &tag,
                &c.db,
                &c.table,
                Some(&tcap),
                Some(&report),
                lctn,
                "relevant_ddl",
            );
            continue;
        }
        proven.push((c, tcap));
    }
    if proven.is_empty() {
        return Ok(ChunkDone {
            consumed: chunk.len(),
            written: 0,
        });
    }

    // 4. One atomic commit of the registrations and baselines.
    let registry = ctx.schema.registry();
    if !std::sync::Arc::ptr_eq(registry.backend(), &ctx.registry_backend) {
        return Err(SourceError::Other(anyhow::anyhow!(
            "snapshot baselines need the schema registry and the activation \
             records in one state store"
        )));
    }
    let mut attempt = 0;
    loop {
        attempt += 1;
        let reqs: Vec<RegistrationRequest> = proven
            .iter()
            .map(|(c, tcap)| RegistrationRequest {
                key: scope.key(&c.db, &c.table),
                hash: tcap.schema.fingerprint().to_string(),
                schema_json: serde_json::to_value(&tcap.schema)
                    .expect("a schema serializes"),
                checkpoint: Some(checkpoint.clone()),
            })
            .collect();
        let (pre, regs) = match registry
            .plan_registrations(&reqs, chrono::Utc::now())
            .await
            .map_err(|e| {
                SourceError::Other(
                    e.context("plan the baselines' registrations"),
                )
            })? {
            RegistrationPlanning::Planned {
                preconditions,
                registrations,
            } => (preconditions, registrations),
            RegistrationPlanning::NeedsRepair(broken) => {
                for k in &broken {
                    registry.repair_key(k).await.map_err(SourceError::Other)?;
                }
                if attempt > BATCH_RETRIES {
                    return Err(SourceError::Other(anyhow::anyhow!(
                        "the snapshot baselines' registrations still need repair \
                         after {attempt} attempts"
                    )));
                }
                continue;
            }
        };
        let mut batch = AtomicBatch {
            preconditions: pre,
            ops: Vec::new(),
        };
        let mut entries = Vec::with_capacity(regs.len());
        let mut op_table = Vec::new();
        for (i, ((c, _), reg)) in proven.iter().zip(&regs).enumerate() {
            let base = batch.push_group(reg.ops.clone());
            entries.push(reg.entry_op.map(|e| base + e));
            let key = scope.key(&c.db, &c.table);
            let stream = table_stream(&key);
            let t = chunk
                .iter()
                .find(|(k, _)| k.db == c.db && k.table == c.table)
                .map(|(_, t)| t)
                .expect("a chunk table");
            let (table_head, barrier_heads) = if attempt == 1 {
                (t.table_head, t.barrier_heads.clone())
            } else {
                let (_, head) = mysql_activation::read_table_head(
                    &ctx.registry_backend,
                    registry,
                    &key,
                )
                .await
                .map_err(SourceError::Other)?;
                let (_, heads) = mysql_activation::read_barriers_head(
                    &ctx.registry_backend,
                    &key,
                )
                .await
                .map_err(SourceError::Other)?;
                (head, heads)
            };
            batch.preconditions.push(Precondition::LogHead {
                ns: mysql_activation::ACTIVATION_NS.into(),
                key: stream.clone(),
                seq: table_head,
            });
            for (b, head) in barrier_heads {
                batch.preconditions.push(Precondition::LogHead {
                    ns: mysql_activation::BARRIER_NS.into(),
                    key: b,
                    seq: head,
                });
            }
            let record = Record::new(
                r0.clone(),
                Kind::Baseline {
                    version: reg.version,
                    schema_hash: reg.hash.clone(),
                    to: s.clone(),
                    scan_digest: report.digest.clone(),
                    binds: c.binds.clone(),
                    classifier_version: CLASSIFIER_VERSION.to_string(),
                },
            );
            let id = baseline_capture_id(&lineage, &stream, &record)
                .map_err(SourceError::Other)?;
            // Idempotent, as one append at a time was.
            batch.ops.push(BatchOp::LogAppendIdempotent {
                ns: mysql_activation::ACTIVATION_NS.into(),
                key: stream,
                capture_id: id,
                value: mysql_activation::encode_record(&record)
                    .map_err(SourceError::Other)?,
            });
            op_table.resize(batch.ops.len(), i);
        }
        #[cfg(test)]
        if let Some(hook) = hooks::BEFORE_COMMIT.lock().unwrap().take() {
            hook();
        }
        let mut applies = 0;
        let seqs = match commit_atomic_counted(
            ctx.registry_backend.as_ref(),
            &batch,
            BATCH_RETRIES,
            &mut applies,
        )
        .await
        {
            Ok(CommitResult::Committed(seqs)) => seqs,
            Ok(CommitResult::Conflict(why)) => {
                if attempt > BATCH_RETRIES {
                    return Err(SourceError::Other(anyhow::anyhow!(
                        "the snapshot baselines conflicted {attempt} times (last: \
                         {why}); nothing of the chunk was written"
                    )));
                }
                warn!(source_id = %ctx.source_id, attempt, why, "snapshot baselines: the batch conflicted; deciding again");
                continue;
            }
            Err(e) => {
                if let Some(too) = e.chain().find_map(|c| {
                    c.downcast_ref::<storage::StateItemTooLarge>()
                }) && let Some(i) =
                    batch.ops.iter().zip(&op_table).find_map(|(op, i)| {
                        let (BatchOp::LogAppendIfAbsent { ns, key, .. }
                        | BatchOp::LogAppendIdempotent { ns, key, .. }
                        | BatchOp::KvPut { ns, key, .. }) = op
                        else {
                            return None;
                        };
                        (storage::state_key_digest(ns, key) == too.key_digest)
                            .then_some(*i)
                    })
                {
                    let c = proven[i].0;
                    return Err(crate::incident_drafts::state_item_too_large(
                        &ctx.source_id,
                        &c.db,
                        &c.table,
                        too,
                        r0_cp,
                    ));
                }
                return Err(SourceError::Other(
                    e.context("commit the snapshot baselines"),
                ));
            }
        };
        // Committed: the registry's cache learns it, and only now.
        for (reg, entry) in regs.iter().zip(&entries) {
            if let Some(op) = entry {
                registry.admit_committed(
                    reg,
                    seqs[*op].expect("a committed log write"),
                );
            }
        }
        for (c, tcap) in &proven {
            trace_proof(
                &tag,
                &c.db,
                &c.table,
                Some(tcap),
                Some(&report),
                lctn,
                "proven",
            );
        }
        return Ok(ChunkDone {
            consumed: chunk.len(),
            written: proven.len(),
        });
    }
}

/// Conflicting re-reads, or retries of a batch whose outcome a storage
/// error left unknown, before the chunk fails closed.
const BATCH_RETRIES: usize = 3;

/// The per-table path [`establish`] replaced: a capture, registration and
/// append per table (the reference its stored outcome is compared with).
#[cfg(test)]
async fn establish_per_table(
    ctx: &RunCtx,
    tracked: &[(String, String)],
) -> SourceResult<usize> {
    let scope = ctx.registry_scope.current()?;
    let lineage = scope.lineage().lineage_hash.clone();
    let server_uuid = ctx.expected_uuid()?;
    let tag = ScanTag {
        source_id: &ctx.source_id,
        kind: ProofKind::Snapshot,
    };
    let lctn = ctx.lower_case_table_names;
    let r0_cp = MySqlCheckpoint {
        file: ctx.last_file.clone(),
        pos: ctx.last_pos,
        gtid_set: ctx.gtid_text(),
        lineage: Some(lineage.clone()),
        snapshot_completed: None,
        snapshot_chain: None,
    };
    let Some(r0) = mysql_checkpoint_position(
        &r0_cp.file,
        r0_cp.pos,
        r0_cp.gtid_set.as_deref(),
    ) else {
        warn!(source_id = %ctx.source_id, "unparseable start position: no baselines");
        return Ok(0);
    };
    let storage = |e: anyhow::Error| {
        SourceError::Other(e.context("read the activation timeline"))
    };

    // Tables whose version is not proven at R0, and the barrier each binds.
    let mut candidates = Vec::new();
    for (db, table) in tracked {
        let key = scope.key(db, table);
        let records = mysql_activation::read_table(
            &ctx.registry_backend,
            ctx.schema.registry(),
            &key,
        )
        .await
        .map_err(storage)?;
        let barriers =
            mysql_activation::read_barriers(&ctx.registry_backend, &key)
                .await
                .map_err(storage)?;
        let all: Vec<Stored> =
            records.iter().chain(barriers.iter()).cloned().collect();
        if matches!(select(&all, &r0), Selection::Proven { .. }) {
            continue;
        }
        match barrier_to_bind(&records, &barriers, &r0) {
            Ok(binds) => candidates.push(Candidate {
                db: db.clone(),
                table: table.clone(),
                binds,
            }),
            Err(why) => {
                warn!(source_id = %ctx.source_id, %db, %table, why, "no baseline");
            }
        }
    }
    if candidates.is_empty() {
        return Ok(0);
    }

    // 1. A capture of each table's shape, bound to its interval by step 2.
    let mut captured = Vec::new();
    for c in candidates {
        // A stop ends the baselines (`?`): nothing is decided.
        match capture(
            &ctx.capture,
            &server_uuid,
            &lineage,
            &c.db,
            &c.table,
            None,
        )
        .await?
        {
            Ok(cap) => {
                observe_capture(&tag, &cap);
                captured.push((c, cap))
            }
            // The connection reached another server: never a "no baseline".
            Err(ProofError::OtherServer(found)) => {
                return Err(other_server(&server_uuid, &found));
            }
            Err(e) => {
                warn!(source_id = %ctx.source_id, db = %c.db, table = %c.table, error = ?e, "no baseline: no capture");
                trace_proof(
                    &tag,
                    &c.db,
                    &c.table,
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
        return Ok(0);
    };
    let Some(s) = mysql_checkpoint_position(
        &s_cp.file,
        s_cp.pos,
        s_cp.gtid_set.as_deref(),
    ) else {
        return Ok(0);
    };

    // 2. One complete scan of (R0, S].
    let report: ScanReport = match super::mysql_proof_scanner::shared_interval(
        &ctx.proof_scanner,
        ctx.dsn.current().expose(),
        &ctx.source_id,
        &server_uuid,
        &lineage,
        &r0_cp,
        &s_cp,
        None,
        Some(&r0_cp),
        &tag,
    )
    .await
    {
        Ok(report) => report,
        Err(ProofError::OtherServer(found)) => {
            return Err(other_server(&server_uuid, &found));
        }
        Err(e) => {
            warn!(source_id = %ctx.source_id, error = ?e, "no baselines: the interval scan failed");
            for (c, cap) in &captured {
                trace_proof(
                    &tag,
                    &c.db,
                    &c.table,
                    Some(cap),
                    None,
                    lctn,
                    "scan_failed",
                );
            }
            return Ok(0);
        }
    };

    // 3. A baseline for every table the scan proves untouched.
    let checkpoint =
        serde_json::to_vec(&r0_cp).map_err(|e| SourceError::Other(e.into()))?;
    let mut written = 0;
    for (c, cap) in captured {
        if !covers(&cap, &r0, &s) {
            warn!(source_id = %ctx.source_id, db = %c.db, table = %c.table, "no baseline: the scan does not cover the capture interval");
            trace_proof(
                &tag,
                &c.db,
                &c.table,
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
            .any(|st| affects(st, &c.db, &c.table, ctx.lower_case_table_names))
        {
            info!(source_id = %ctx.source_id, db = %c.db, table = %c.table, "no baseline: a DDL or barrier in the scanned interval");
            trace_proof(
                &tag,
                &c.db,
                &c.table,
                Some(&cap),
                Some(&report),
                lctn,
                "relevant_ddl",
            );
            continue;
        }
        let (version, schema_hash) = ctx
            .schema
            .register_captured(&c.db, &c.table, &cap.schema, &checkpoint)
            .await?;
        let key = scope.key(&c.db, &c.table);
        let stream = table_stream(&key);
        let record = Record::new(
            r0.clone(),
            Kind::Baseline {
                version,
                schema_hash,
                to: s.clone(),
                scan_digest: report.digest.clone(),
                binds: c.binds,
                classifier_version: CLASSIFIER_VERSION.to_string(),
            },
        );
        let id = baseline_capture_id(&lineage, &stream, &record)
            .map_err(SourceError::Other)?;
        mysql_activation::append(
            &ctx.registry_backend,
            ctx.schema.registry(),
            &key,
            mysql_activation::ACTIVATION_NS,
            &stream,
            &id,
            &record,
        )
        .await
        .map_err(|e| SourceError::Other(e.context("persist a baseline")))?;
        trace_proof(
            &tag,
            &c.db,
            &c.table,
            Some(&cap),
            Some(&report),
            lctn,
            "proven",
        );
        written += 1;
    }
    info!(
        source_id = %ctx.source_id,
        baselines = written,
        scanned_events = report.events,
        "activation baselines established"
    );
    Ok(written)
}

/// A lazy baseline (binding ruling Round 36, Q1) for one table whose rows
/// at the evaluation position E are not positionally proven: a stable
/// capture of its complete shape at S, E <= S in the verified lineage, and
/// one complete scan of (E, S] in which nothing applies to the table. The
/// captured shape is registered and a `baseline` at E (binding the one
/// barrier exactly at E, if any) is appended - before the rows are decoded -
/// only if it then decides the rows (see `admit_decisive`). Anything
/// uncertain writes no activation proof and returns `false` (a candidate
/// schema already registered from the capture may remain registered, as in
/// the FULL fallback); a connection on another server or a storage failure
/// is an error.
pub(crate) async fn establish_at(
    ctx: &mut RunCtx,
    db: &str,
    table: &str,
    key: &SchemaKey,
    current: &Timeline,
) -> SourceResult<bool> {
    let (Some(e), Some(mut e_cp)) =
        (ctx.txn_eval.clone(), ctx.txn_eval_cp.clone())
    else {
        return Ok(false);
    };
    let lineage = key.lineage_hash.clone();
    e_cp.lineage = Some(lineage.clone());
    let server_uuid = ctx.expected_uuid()?;
    let source_id = ctx.source_id.clone();
    let tag = ScanTag {
        source_id: &source_id,
        kind: ProofKind::Lazy,
    };
    let lctn = ctx.lower_case_table_names;
    let binds = match barrier_to_bind(&current.records, &current.barriers, &e) {
        Ok(binds) => binds,
        Err(why) => {
            info!(source_id = %ctx.source_id, %db, %table, why, "no lazy baseline");
            return Ok(false);
        }
    };
    // An activation window already decided this table (refused, or not
    // captured): its rows go on as without a lazy baseline. One it admitted
    // is proven by its committed baseline and never reaches here.
    if let Some(outcome) = ctx.take_prepared(db, table, &e, &e_cp) {
        let why = match outcome {
            PreparedOutcome::Refused(why) => why,
            PreparedOutcome::NoCapture => "no_capture",
        };
        info!(source_id = %ctx.source_id, %db, %table, why, "no lazy baseline (decided by the activation window)");
        return Ok(false);
    }
    // A stop ends the baseline (`?`): nothing is decided.
    let cap = match capture(
        &ctx.capture,
        &server_uuid,
        &lineage,
        db,
        table,
        None,
    )
    .await?
    {
        Ok(cap) => {
            observe_capture(&tag, &cap);
            cap
        }
        Err(ProofError::OtherServer(found)) => {
            return Err(other_server(&server_uuid, &found));
        }
        Err(err) => {
            warn!(source_id = %ctx.source_id, %db, %table, error = ?err, "no lazy baseline: no capture");
            trace_proof(&tag, db, table, None, None, lctn, "no_capture");
            return Ok(false);
        }
    };
    let s_cp = cap.position.clone();
    let Some(s) = mysql_checkpoint_position(
        &s_cp.file,
        s_cp.pos,
        s_cp.gtid_set.as_deref(),
    ) else {
        return Ok(false);
    };
    if !covers(&cap, &e, &s) {
        warn!(source_id = %ctx.source_id, %db, %table, "no lazy baseline: the capture did not start at or after the rows");
        trace_proof(&tag, db, table, Some(&cap), None, lctn, "not_covered");
        return Ok(false);
    }
    let scanned = super::mysql_proof_scanner::shared_interval(
        &ctx.proof_scanner,
        ctx.dsn.current().expose(),
        &source_id,
        &server_uuid,
        &lineage,
        &e_cp,
        &s_cp,
        ctx.stream_anchor(&server_uuid, &lineage).as_ref(),
        Some(&e_cp),
        &tag,
    )
    .await;
    let report = match scanned {
        Ok(report) => report,
        Err(ProofError::OtherServer(found)) => {
            return Err(other_server(&server_uuid, &found));
        }
        Err(err) => {
            warn!(source_id = %ctx.source_id, %db, %table, error = ?err, "no lazy baseline: the interval scan failed");
            trace_proof(&tag, db, table, Some(&cap), None, lctn, "scan_failed");
            return Ok(false);
        }
    };
    if report
        .statements
        .iter()
        .any(|st| affects(st, db, table, ctx.lower_case_table_names))
    {
        info!(source_id = %ctx.source_id, %db, %table, "no lazy baseline: a DDL or barrier after the rows");
        trace_proof(
            &tag,
            db,
            table,
            Some(&cap),
            Some(&report),
            lctn,
            "relevant_ddl",
        );
        return Ok(false);
    }
    let checkpoint = serde_json::to_vec(&e_cp)
        .map_err(|err| SourceError::Other(err.into()))?;
    let (version, schema_hash) = match ctx
        .schema
        .register_captured(db, table, &cap.schema, &checkpoint)
        .await
    {
        Ok(v) => v,
        Err(e) => {
            ctx.selection.invalidate(key);
            return Err(e);
        }
    };
    let stream = table_stream(key);
    let record = Record::new(
        e.clone(),
        Kind::Baseline {
            version,
            schema_hash,
            to: s,
            scan_digest: report.digest.clone(),
            binds,
            classifier_version: CLASSIFIER_VERSION.to_string(),
        },
    );
    let id = baseline_capture_id(&lineage, &stream, &record)
        .map_err(SourceError::Other)?;
    let proposed = Stored {
        capture_id: id.clone(),
        record: record.clone(),
    };
    let augmented = match super::mysql_selection::admit_decisive_timeline(
        ctx.schema.registry(),
        key,
        current,
        proposed,
        &e,
    )
    .await
    {
        Ok(t) => t,
        Err(why) => {
            info!(source_id = %ctx.source_id, %db, %table, why, "no lazy baseline");
            trace_proof(
                &tag,
                db,
                table,
                Some(&cap),
                Some(&report),
                lctn,
                "not_decisive",
            );
            return Ok(false);
        }
    };
    // Appended, then the validated timeline replaces the cached one (no
    // second read and validation), or the cached one is discarded.
    super::mysql_selection::append_admitted(
        &ctx.registry_backend,
        ctx.schema.registry(),
        &mut ctx.selection,
        key,
        &id,
        &record,
        augmented,
    )
    .await
    .map_err(|err| SourceError::Other(err.context("persist a baseline")))?;
    info!(source_id = %ctx.source_id, %db, %table, version, scanned_events = report.events, "lazy baseline established");
    trace_proof(&tag, db, table, Some(&cap), Some(&report), lctn, "proven");
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mysql::mysql_activation::{BarrierScope, EventIdentity};
    use crate::mysql::mysql_ddl_attribution::TableName;

    fn stmt(effect: DdlEffect) -> Statement {
        Statement {
            event: EventIdentity::FilePos {
                file: "b.000001".into(),
                end_pos: 9,
                ordinal: 0,
            },
            effect,
        }
    }

    fn t(db: &str, table: &str) -> TableName {
        TableName {
            db: db.into(),
            table: table.into(),
        }
    }

    #[test]
    fn only_applicable_ddl_and_barriers_prevent_a_baseline() {
        let tables = |v: Vec<TableName>| stmt(DdlEffect::Tables(v));
        assert!(affects(&tables(vec![t("d", "a")]), "d", "a", 0));
        assert!(affects(
            &tables(vec![t("x", "y"), t("d", "a")]),
            "d",
            "a",
            0
        ));
        assert!(!affects(&tables(vec![t("d", "b")]), "d", "a", 0));
        assert!(!affects(&tables(vec![t("e", "a")]), "d", "a", 0));
        assert!(!affects(
            &stmt(DdlEffect::SameShape(vec![t("d", "a")])),
            "d",
            "a",
            0
        ));
        assert!(affects(
            &stmt(DdlEffect::Barrier(BarrierScopeOf::Lineage)),
            "d",
            "a",
            0
        ));
        assert!(affects(
            &stmt(DdlEffect::Barrier(BarrierScopeOf::Database("d".into()))),
            "d",
            "a",
            0
        ));
        assert!(!affects(
            &stmt(DdlEffect::Barrier(BarrierScopeOf::Database("e".into()))),
            "d",
            "a",
            0
        ));
        // Case per lower_case_table_names.
        assert!(!affects(&tables(vec![t("D", "A")]), "d", "a", 0));
        assert!(affects(&tables(vec![t("D", "A")]), "d", "a", 1));
        assert!(affects(&tables(vec![t("e", "z")]), "d", "a", 2));
        let db = |d: &str| {
            stmt(DdlEffect::Barrier(BarrierScopeOf::Database(d.into())))
        };
        assert!(!affects(&db("D"), "d", "a", 0));
        assert!(affects(&db("D"), "d", "a", 1));
        assert!(!affects(&db("e"), "d", "a", 1));
        assert!(affects(&db("e"), "d", "a", 2));
    }

    fn st(id: &str, pos: u64, kind: Kind) -> Stored {
        Stored {
            capture_id: id.into(),
            record: Record::new(
                WmPos::MysqlBinlog {
                    file_base: "b".into(),
                    file_index: 1,
                    pos,
                },
                kind,
            ),
        }
    }

    #[test]
    fn a_baseline_binds_only_the_one_barrier_at_its_position() {
        let r0 = WmPos::MysqlBinlog {
            file_base: "b".into(),
            file_index: 1,
            pos: 10,
        };
        let lineage = || Kind::Barrier {
            scope: BarrierScope::Lineage,
        };
        assert_eq!(barrier_to_bind(&[], &[], &r0), Ok(None));
        assert_eq!(
            barrier_to_bind(&[], &[st("old", 5, lineage())], &r0),
            Ok(None)
        );
        assert_eq!(
            barrier_to_bind(
                &[],
                &[st("old", 5, lineage()), st("start", 10, lineage())],
                &r0
            ),
            Ok(Some("start".into()))
        );
        assert!(
            barrier_to_bind(
                &[],
                &[st("a", 10, lineage()), st("b", 10, lineage())],
                &r0
            )
            .is_err()
        );
        assert!(
            barrier_to_bind(&[st("ddl", 10, Kind::Ddl)], &[], &r0).is_err()
        );
    }

    /// Live MySQL 8.4 (Docker): startup establishes nothing; a table's first
    /// unproven rows get a lazy baseline at their evaluation position, which
    /// selection over the stored records then proves. They share one server
    /// and pace phases by time: run them serially
    /// (`-- --include-ignored --test-threads=1 mysql_baseline`).
    mod live {
        use super::*;
        use crate::mysql::MySqlSource;
        use crate::mysql::mysql_activation::{read_barriers, read_table};
        use checkpoints::{
            CheckpointStore, CheckpointStoreExt, MemCheckpointStore,
        };
        use deltaforge_config::{OnSchemaDrift, SnapshotCfg, SnapshotMode};
        use deltaforge_core::Source;
        use gate_ownership::GateOwned;
        use mysql_async::prelude::Queryable;
        use std::sync::Arc;
        use std::time::Duration;
        use storage::{
            ArcStorageBackend, DurableSchemaRegistry, MemoryStorageBackend,
        };
        use testcontainers::core::WaitFor;
        use testcontainers::runners::AsyncRunner;
        use testcontainers::{ContainerAsync, GenericImage, ImageExt};
        use tokio::sync::OnceCell;

        static GTID: OnceCell<(ContainerAsync<GenericImage>, u16)> =
            OnceCell::const_new();
        static FILEPOS: OnceCell<(ContainerAsync<GenericImage>, u16)> =
            OnceCell::const_new();

        async fn start(gtid: bool) -> (ContainerAsync<GenericImage>, u16) {
            let mut cmd = vec![
                "--server-id=51",
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
                if mysql_async::Conn::from_url(dsn(port, "")).await.is_ok() {
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

        fn dsn(port: u16, db: &str) -> String {
            format!("mysql://root:pw@127.0.0.1:{port}/{db}")
        }

        async fn sql(port: u16, stmts: &[String]) {
            let mut c =
                mysql_async::Conn::from_url(dsn(port, "")).await.unwrap();
            for s in stmts {
                c.query_drop(s).await.unwrap_or_else(|e| panic!("{s}: {e}"));
            }
            c.disconnect().await.ok();
        }

        struct State {
            backend: ArcStorageBackend,
            registry: Arc<DurableSchemaRegistry>,
            ckpt: Arc<dyn CheckpointStore>,
            scope: crate::registry_scope::SharedRegistryScope,
        }

        async fn state() -> State {
            let backend: ArcStorageBackend =
                Arc::new(MemoryStorageBackend::new());
            State {
                registry: DurableSchemaRegistry::new(backend.clone())
                    .await
                    .unwrap(),
                ckpt: Arc::new(MemCheckpointStore::new().unwrap()),
                scope: crate::registry_scope::SharedRegistryScope::default(),
                backend,
            }
        }

        /// Run the source on `tables` (no snapshot unless `mode` says so),
        /// execute `during` once it is streaming (phases separated by an
        /// empty statement), then stop it. Returns the
        /// rows it emitted and how it ended.
        async fn run(
            st: &State,
            port: u16,
            id: &str,
            tables: &[&str],
            mode: SnapshotMode,
            during: &[String],
        ) -> (Vec<deltaforge_core::Event>, SourceResult<()>) {
            let src = MySqlSource {
                id: id.into(),
                dsn: dsn(port, "").as_str().into(),
                tables: tables.iter().map(|t| t.to_string()).collect(),
                tenant: "acme".into(),
                pipeline: "test".into(),
                registry: st.registry.clone(),
                registry_scope: st.scope.clone(),
                backend: st.backend.clone(),
                outbox_tables: common::AllowList::default(),
                snapshot_cfg: SnapshotCfg {
                    mode,
                    ..Default::default()
                },
                on_schema_drift: OnSchemaDrift::Adapt,
                table_options: Default::default(),
                rotation: None,
                snapshot_cohort: Default::default(),
                proofs: Default::default(),
                type_mapping: deltaforge_core::TypeMapping::V2,
            };
            // One required sink; its barriers commit through the runner's
            // own barrier commit, so a generation can start and complete.
            src.set_snapshot_cohort(deltaforge_core::SnapshotCohort {
                policy: deltaforge_core::CohortPolicy::Required,
                sinks: vec![deltaforge_core::CohortSink {
                    id: "sink".into(),
                    required: true,
                }],
            });
            let commit = runner::coordinator::build_barrier_fn(
                st.ckpt.clone(),
                format!("{id}::sink::sink"),
                Arc::new(src.clone()),
            );
            let (tx, mut rx) = tokio::sync::mpsc::channel(1024);
            let handle = src.run(tx, st.ckpt.clone()).await;
            let rows = tokio::spawn(async move {
                use deltaforge_core::{BarrierKind, SourceItem};
                use runner::coordinator::BarrierCommit;
                let mut rows = Vec::new();
                while let Some(item) = rx.recv().await {
                    match item {
                        SourceItem::Barrier { barrier } => {
                            let c = match barrier.kind {
                                BarrierKind::GenerationStart(s) => {
                                    BarrierCommit::Start(s)
                                }
                                BarrierKind::Terminal => {
                                    BarrierCommit::Checkpoint(
                                        barrier.boundary.checkpoint,
                                    )
                                }
                            };
                            if commit(c).await.is_err() {
                                break;
                            }
                        }
                        SourceItem::Event(e) if e.ddl.is_none() => rows.push(e),
                        _ => {}
                    }
                }
                rows
            });
            tokio::time::sleep(Duration::from_secs(4)).await;
            // An empty statement separates phases: the source catches up
            // before the next one runs.
            for phase in during.split(|s| s.is_empty()) {
                sql(port, phase).await;
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
            handle.stop();
            let res =
                tokio::time::timeout(Duration::from_secs(30), handle.join)
                    .await
                    .expect("stops")
                    .expect("task");
            let rows = rows.await.unwrap();
            (rows, res)
        }

        /// The table's records and its barriers.
        async fn timeline(
            st: &State,
            db: &str,
            table: &str,
        ) -> (Vec<Stored>, Vec<Stored>) {
            let key = st.scope.current().unwrap().key(db, table);
            (
                read_table(&st.backend, &st.registry, &key).await.unwrap(),
                read_barriers(&st.backend, &key).await.unwrap(),
            )
        }

        async fn stored_position(st: &State, id: &str) -> WmPos {
            let cp: crate::MySqlCheckpoint =
                st.ckpt.get(id).await.unwrap().unwrap();
            mysql_checkpoint_position(&cp.file, cp.pos, cp.gtid_set.as_deref())
                .unwrap()
        }

        fn baselines(records: &[Stored]) -> Vec<&Stored> {
            records
                .iter()
                .filter(|s| matches!(s.record.kind, Kind::Baseline { .. }))
                .collect()
        }

        fn proven(
            table: &[Stored],
            barriers: &[Stored],
            at: &WmPos,
        ) -> Selection {
            let all: Vec<Stored> =
                table.iter().chain(barriers.iter()).cloned().collect();
            select(&all, at)
        }

        async fn prepare(port: u16, db: &str, tables: &[&str]) {
            let mut s = vec![
                format!("DROP DATABASE IF EXISTS {db}"),
                format!("CREATE DATABASE {db}"),
            ];
            for t in tables {
                s.push(format!(
                    "CREATE TABLE {db}.{t} (id INT PRIMARY KEY, v INT)"
                ));
            }
            sql(port, &s).await;
        }

        async fn fresh_start(gtid: bool) {
            let port = server(gtid).await;
            let db = if gtid { "base_g" } else { "base_f" };
            prepare(port, db, &["t"]).await;
            let st = state().await;
            let t = format!("{db}.t");

            // Startup alone establishes nothing.
            let (rows, res) =
                run(&st, port, db, &[&t], SnapshotMode::Never, &[]).await;
            res.unwrap();
            assert!(rows.is_empty());
            let r0 = stored_position(&st, db).await;
            let (table, barriers) = timeline(&st, db, "t").await;
            assert!(table.is_empty(), "{table:?}");
            let start: Vec<&Stored> = barriers
                .iter()
                .filter(|b| {
                    order_positions(&b.record.position, &r0)
                        == CheckpointOrder::Equal
                })
                .collect();
            assert_eq!(start.len(), 1, "{barriers:?}");

            // The first rows (evaluated at the start position) get a lazy
            // baseline there, binding the start barrier.
            let st2 = state().await;
            let (rows, res) = run(
                &st2,
                port,
                db,
                &[&t],
                SnapshotMode::Never,
                &[format!("INSERT INTO {t} VALUES (1, 1)")],
            )
            .await;
            res.unwrap();
            assert_eq!(rows.len(), 1);
            let (table, barriers) = timeline(&st2, db, "t").await;
            assert_eq!(barriers.len(), 1, "{barriers:?}");
            let r0 = barriers[0].record.position.clone();
            let base = baselines(&table);
            assert_eq!(base.len(), 1, "{table:?}");
            assert_eq!(
                order_positions(&base[0].record.position, &r0),
                CheckpointOrder::Equal
            );
            let start: Vec<&Stored> = barriers
                .iter()
                .filter(|b| {
                    order_positions(&b.record.position, &r0)
                        == CheckpointOrder::Equal
                })
                .collect();
            let Kind::Baseline { version, binds, .. } = &base[0].record.kind
            else {
                unreachable!()
            };
            assert_eq!(binds.as_deref(), Some(start[0].capture_id.as_str()));
            assert_eq!(
                proven(&table, &barriers, &r0),
                Selection::Proven { version: *version }
            );

            // A restart at the committed (already proven) position adds no
            // record, and its next rows reuse the proof.
            let (rows, res) = run(
                &st2,
                port,
                db,
                &[&t],
                SnapshotMode::Never,
                &[format!("INSERT INTO {t} VALUES (2, 2)")],
            )
            .await;
            res.unwrap();
            assert_eq!(rows.len(), 1);
            let (again, barriers_again) = timeline(&st2, db, "t").await;
            assert_eq!(again, table);
            assert_eq!(barriers_again, barriers);
        }

        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_first_rows_baseline_binds_the_start_barrier_gtid() {
            fresh_start(true).await;
        }

        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_first_rows_baseline_binds_the_start_barrier_file_position() {
            fresh_start(false).await;
        }

        /// A DDL of the table after its rows (in (E, S]) prevents its lazy
        /// baseline and the source stops before those rows; another table's
        /// earlier rows still get theirs.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_later_ddl_of_the_table_prevents_its_lazy_baseline() {
            let port = server(true).await;
            let db = "base_scan";
            prepare(port, db, &["t", "u"]).await;
            let st = state().await;
            let (t, u) = (format!("{db}.t"), format!("{db}.u"));
            run(&st, port, db, &[&t, &u], SnapshotMode::Never, &[])
                .await
                .1
                .unwrap();
            let committed = st.ckpt.get_raw(db).await.unwrap().unwrap();
            // While stopped: u's row, t's row, then a DDL of t.
            sql(
                port,
                &[
                    format!("INSERT INTO {u} VALUES (1, 1)"),
                    format!("INSERT INTO {t} VALUES (1, 1)"),
                    format!("ALTER TABLE {t} ADD COLUMN w INT"),
                ],
            )
            .await;
            let (rows, res) =
                run(&st, port, db, &[&t, &u], SnapshotMode::Never, &[]).await;
            let err = format!("{:#}", res.expect_err("t's rows are refused"));
            assert!(err.contains("no positional proof"), "{err}");
            assert_eq!(rows.len(), 1, "only u's row");
            assert_eq!(rows[0].source.table, "u");
            assert_eq!(baselines(&timeline(&st, db, "u").await.0).len(), 1);
            assert!(baselines(&timeline(&st, db, "t").await.0).is_empty());
            assert_eq!(st.ckpt.get_raw(db).await.unwrap().unwrap(), committed);
        }

        /// After a barrier (here a versioned-comment DDL, unattributable), a
        /// table's next rows restore proof by a lazy baseline: later
        /// authoritative evidence for that table supersedes the barrier.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_lazy_baseline_restores_proof_after_a_barrier() {
            let port = server(true).await;
            let db = "base_barrier";
            prepare(port, db, &["t"]).await;
            let st = state().await;
            let t = format!("{db}.t");
            let (rows, res) = run(
                &st,
                port,
                db,
                &[&t],
                SnapshotMode::Never,
                &[
                    format!("INSERT INTO {t} VALUES (1, 1)"),
                    String::new(),
                    // Unattributable (a possible ANSI_QUOTES identifier):
                    // a lineage barrier.
                    format!("ALTER TABLE {t} ADD COLUMN w INT COMMENT \"w\""),
                    format!("INSERT INTO {t} VALUES (2, 2, 2)"),
                ],
            )
            .await;
            res.unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(
                rows[1].after.as_ref().unwrap().get("w"),
                Some(&deltaforge_core::canonical::fixture::value(
                    &serde_json::json!(2)
                ))
            );
            let (table, barriers) = timeline(&st, db, "t").await;
            let base = baselines(&table);
            assert_eq!(base.len(), 2, "{table:?}");
            // The second baseline follows the in-stream barrier.
            let barrier = barriers.last().unwrap();
            assert_eq!(
                order_positions(
                    &barrier.record.position,
                    &base[1].record.position
                ),
                CheckpointOrder::Equal
            );
            assert!(matches!(
                &base[1].record.kind,
                Kind::Baseline { binds: Some(b), .. } if *b == barrier.capture_id
            ));
        }

        /// Stored schema history that cannot be read is corrupt: a load of it
        /// (here by a snapshot) fails closed, never replaced by the live
        /// catalog, and nothing is emitted.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_corrupt_stored_schema_fails_closed() {
            let port = server(true).await;
            let db = "base_corrupt";
            prepare(port, db, &["t"]).await;
            sql(port, &[format!("INSERT INTO {db}.t VALUES (1, 1)")]).await;
            let st = state().await;
            let t = format!("{db}.t");
            // Establish the lineage, then store an unreadable latest version.
            run(&st, port, db, &[&t], SnapshotMode::Never, &[])
                .await
                .1
                .unwrap();
            let key = st.scope.current().unwrap().key(db, "t");
            st.registry
                .register_with_checkpoint(
                    &key,
                    "corrupt",
                    &serde_json::json!({ "columns": 5 }),
                    None,
                )
                .await
                .unwrap();
            let (rows, res) =
                run(&st, port, db, &[&t], SnapshotMode::Always, &[]).await;
            let err = format!("{:#}", res.expect_err("fails closed"));
            assert!(err.contains("is unreadable"), "{err}");
            assert!(rows.is_empty(), "{rows:?}");
        }

        /// The test hooks back at rest.
        fn reset_hooks() {
            use std::sync::atomic::Ordering::SeqCst;
            super::super::PER_TABLE.store(false, SeqCst);
            super::super::hooks::REPLAY.store(false, SeqCst);
            super::super::hooks::REPLAYED.store(usize::MAX, SeqCst);
            super::super::hooks::FAIL_AFTER_CHUNKS.store(0, SeqCst);
            super::super::hooks::CHUNKS.store(0, SeqCst);
            *super::super::hooks::BEFORE_COMMIT.lock().unwrap() = None;
            crate::stream_probe::set_window_caps(None);
        }

        /// One baseline per table, each at the anchor and binding its
        /// barrier; returns the baselines.
        async fn one_baseline_each(
            st: &State,
            db: &str,
            tables: &[&str],
        ) -> Vec<Stored> {
            let mut out = Vec::new();
            for t in tables {
                let (table, barriers) = timeline(st, db, t).await;
                let base = baselines(&table);
                assert_eq!(base.len(), 1, "{t}: {table:?}");
                let anchor = barriers.last().expect("the anchor barrier");
                assert_eq!(
                    order_positions(
                        &anchor.record.position,
                        &base[0].record.position
                    ),
                    CheckpointOrder::Equal,
                    "{t}"
                );
                assert!(matches!(
                    &base[0].record.kind,
                    Kind::Baseline { binds: Some(b), .. } if *b == anchor.capture_id
                ));
                out.push(base[0].clone());
            }
            out
        }

        /// The batched path stores what the per-table path stored, at the
        /// same anchor (a quiet server between the runs): the same
        /// registrations (version, hash, schema, checkpoint, sequence) and
        /// the same baselines but for the timing-derived fields (S, the
        /// scan digest, the derived capture identity).
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn batched_baselines_store_what_the_per_table_path_stored() {
            reset_hooks();
            let port = server(true).await;
            let db = "base_eq";
            let tables = ["a", "b", "c"];
            prepare(port, db, &tables).await;
            for t in &tables {
                sql(port, &[format!("INSERT INTO {db}.{t} VALUES (1, 1)")])
                    .await;
            }
            let names: Vec<String> =
                tables.iter().map(|t| format!("{db}.{t}")).collect();
            let names: Vec<&str> = names.iter().map(String::as_str).collect();
            let old = state().await;
            super::super::PER_TABLE
                .store(true, std::sync::atomic::Ordering::SeqCst);
            let (_, res) =
                run(&old, port, db, &names, SnapshotMode::Initial, &[]).await;
            res.unwrap();
            super::super::PER_TABLE
                .store(false, std::sync::atomic::Ordering::SeqCst);
            let new = state().await;
            let (_, res) =
                run(&new, port, db, &names, SnapshotMode::Initial, &[]).await;
            res.unwrap();
            let a = one_baseline_each(&old, db, &tables).await;
            let b = one_baseline_each(&new, db, &tables).await;
            for (i, t) in tables.iter().enumerate() {
                let ka = old.scope.current().unwrap().key(db, t);
                let kb = new.scope.current().unwrap().key(db, t);
                let ra = old.registry.get_latest(&ka).await.unwrap().unwrap();
                let rb = new.registry.get_latest(&kb).await.unwrap().unwrap();
                assert_eq!(
                    (
                        ra.version,
                        &ra.hash,
                        &ra.schema_json,
                        &ra.checkpoint,
                        ra.sequence
                    ),
                    (
                        rb.version,
                        &rb.hash,
                        &rb.schema_json,
                        &rb.checkpoint,
                        rb.sequence
                    ),
                    "{t}: registration"
                );
                assert_eq!(
                    order_positions(
                        &a[i].record.position,
                        &b[i].record.position
                    ),
                    CheckpointOrder::Equal,
                    "{t}: the same anchor"
                );
                let strip = |k: &Kind| match k {
                    Kind::Baseline {
                        version,
                        schema_hash,
                        classifier_version,
                        ..
                    } => (
                        *version,
                        schema_hash.clone(),
                        classifier_version.clone(),
                    ),
                    other => panic!("{other:?}"),
                };
                assert_eq!(
                    strip(&a[i].record.kind),
                    strip(&b[i].record.kind),
                    "{t}"
                );
                assert_eq!(
                    a[i].record.format_version,
                    b[i].record.format_version
                );
            }
        }

        /// Establish replayed at the same anchor writes nothing: every table
        /// is proven there already.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn establish_replayed_at_the_same_anchor_writes_nothing() {
            reset_hooks();
            let port = server(true).await;
            let db = "base_replay";
            let tables = ["a", "b"];
            prepare(port, db, &tables).await;
            let st = state().await;
            super::super::hooks::REPLAY
                .store(true, std::sync::atomic::Ordering::SeqCst);
            let names = [format!("{db}.a"), format!("{db}.b")];
            let (_, res) = run(
                &st,
                port,
                db,
                &[&names[0], &names[1]],
                SnapshotMode::Initial,
                &[],
            )
            .await;
            res.unwrap();
            assert_eq!(
                super::super::hooks::REPLAYED
                    .load(std::sync::atomic::Ordering::SeqCst),
                0,
                "the replay wrote nothing"
            );
            one_baseline_each(&st, db, &tables).await;
            reset_hooks();
        }

        /// A generation interrupted after its first chunk committed (small
        /// caps make one table a chunk) is replaced: the next run
        /// establishes every table exactly once at the new anchor R0', each
        /// binding R0''s barrier; a baseline of the old generation at R0 does
        /// not prove the table at R0'.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn an_interrupted_generation_is_replaced_and_proves_each_table_once()
         {
            reset_hooks();
            let port = server(true).await;
            let db = "base_interrupt";
            let tables = ["a", "b", "c"];
            prepare(port, db, &tables).await;
            let names: Vec<String> =
                tables.iter().map(|t| format!("{db}.{t}")).collect();
            let names: Vec<&str> = names.iter().map(String::as_str).collect();
            let st = state().await;
            // One table per chunk.
            crate::stream_probe::set_window_caps(Some((1, 16_384, 1 << 30)));
            super::super::hooks::FAIL_AFTER_CHUNKS
                .store(1, std::sync::atomic::Ordering::SeqCst);
            let (_, res) =
                run(&st, port, db, &names, SnapshotMode::Initial, &[]).await;
            assert!(res.is_err(), "the injected failure ends the run");
            let (first, _) = timeline(&st, db, "a").await;
            assert_eq!(baselines(&first).len(), 1, "the first chunk committed");
            let (second, _) = timeline(&st, db, "b").await;
            assert!(baselines(&second).is_empty(), "the second never ran");
            reset_hooks();
            crate::stream_probe::set_window_caps(Some((1, 16_384, 1 << 30)));
            sql(port, &[format!("INSERT INTO {db}.a VALUES (9, 9)")]).await;
            let (_, res) =
                run(&st, port, db, &names, SnapshotMode::Initial, &[]).await;
            crate::stream_probe::set_window_caps(None);
            res.unwrap();
            for t in &tables {
                let (table, barriers) = timeline(&st, db, t).await;
                let anchor = barriers.last().unwrap();
                let at_new: Vec<_> = baselines(&table)
                    .into_iter()
                    .filter(|b| {
                        order_positions(
                            &anchor.record.position,
                            &b.record.position,
                        ) == CheckpointOrder::Equal
                    })
                    .collect();
                assert_eq!(
                    at_new.len(),
                    1,
                    "{t}: one baseline at R0': {table:?}"
                );
                assert!(matches!(
                    &at_new[0].record.kind,
                    Kind::Baseline { binds: Some(b), .. } if *b == anchor.capture_id
                ));
            }
            // The old generation's baseline of `a` is at R0, before R0'.
            let (a, _) = timeline(&st, db, "a").await;
            assert_eq!(baselines(&a).len(), 2, "{a:?}");
        }

        /// A chunk whose commit acknowledgement is lost resolves through its
        /// durable marker: one registration and one baseline per table.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn an_ambiguous_chunk_commit_resolves_through_its_marker() {
            reset_hooks();
            let port = server(true).await;
            let db = "base_ack";
            let tables = ["a", "b"];
            prepare(port, db, &tables).await;
            let mem = Arc::new(MemoryStorageBackend::new());
            let backend: ArcStorageBackend = mem.clone();
            let st = State {
                registry: DurableSchemaRegistry::new(backend.clone())
                    .await
                    .unwrap(),
                ckpt: Arc::new(MemCheckpointStore::new().unwrap()),
                scope: crate::registry_scope::SharedRegistryScope::default(),
                backend,
            };
            *super::super::hooks::BEFORE_COMMIT.lock().unwrap() =
                Some(Box::new(move || {
                    use storage::BatchFaults as _;
                    mem.set_batch_fault(Some(storage::BatchFault::AfterCommit));
                }));
            let names = [format!("{db}.a"), format!("{db}.b")];
            let (_, res) = run(
                &st,
                port,
                db,
                &[&names[0], &names[1]],
                SnapshotMode::Initial,
                &[],
            )
            .await;
            res.unwrap();
            assert!(
                super::super::hooks::BEFORE_COMMIT.lock().unwrap().is_none(),
                "the commit ran"
            );
            one_baseline_each(&st, db, &tables).await;
            for t in &tables {
                let key = st.scope.current().unwrap().key(db, t);
                assert_eq!(
                    st.registry
                        .get_latest(&key)
                        .await
                        .unwrap()
                        .unwrap()
                        .version,
                    1
                );
            }
            reset_hooks();
        }

        /// A snapshot proves every table it copied at its anchor (they are
        /// already enumerated and loaded): one baseline there, binding the
        /// anchor's barrier, before any CDC rows.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_snapshot_proves_its_tables_at_the_anchor() {
            let port = server(true).await;
            let db = "base_snap";
            prepare(port, db, &["t"]).await;
            sql(port, &[format!("INSERT INTO {db}.t VALUES (1, 1)")]).await;
            let st = state().await;
            let t = format!("{db}.t");
            // No CDC rows: the proof exists before any first use.
            let (rows, res) =
                run(&st, port, db, &[&t], SnapshotMode::Initial, &[]).await;
            res.unwrap();
            assert_eq!(rows.len(), 1, "the snapshot row only");
            let (table, barriers) = timeline(&st, db, "t").await;
            assert_eq!(barriers.len(), 1, "{barriers:?}");
            let anchor = &barriers[0];
            let base = baselines(&table);
            assert_eq!(base.len(), 1, "{table:?}");
            assert_eq!(
                order_positions(
                    &anchor.record.position,
                    &base[0].record.position
                ),
                CheckpointOrder::Equal
            );
            assert!(matches!(
                &base[0].record.kind,
                Kind::Baseline { binds: Some(b), .. } if *b == anchor.capture_id
            ));
        }
    }
}
