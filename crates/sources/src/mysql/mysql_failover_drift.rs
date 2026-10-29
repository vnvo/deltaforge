//! Lazy per-table failover drift (binding rulings Round 36 Q2, Rounds 37-38).
//!
//! After a failover, every table's schema on the new server is compared with
//! its latest version under the immediate predecessor lineage before the
//! table's first snapshot rows, CDC rows or DDL under the new lineage - never
//! by enumerating tables or predecessor registry keys:
//!
//! 1. The failover's durable anchor names the predecessor lineage, the
//!    current lineage, the lineage transition (the lineage record's durable
//!    random `transition_id`, so every transition - also a return to an
//!    earlier server - gets its own anchor, never derived from the clock) and the exact failover position F (`None` when it could not be
//!    proven on the new server: every comparison is then unprovable).
//! 2. The predecessor's latest version of the table is read lazily. None: a
//!    newly observed table. Unreadable: an error.
//! 3. The shape at F is proven or the comparison is unprovable:
//!    - rows evaluated at E: no record of the table and no applicable barrier
//!      in this lineage's timeline in (F, E] (anything incomparable is
//!      unprovable), a stable capture at S >= E and a complete clean scan of
//!      (E, S];
//!    - a snapshot: a stable capture at S and a complete clean scan of (F, S]
//!      (a purged interval or an incomplete scan is unprovable);
//!    - a DDL as the table's first event: unprovable.
//! 4. `on_schema_drift` is applied before anything is registered, baselined,
//!    decoded or emitted: under `halt`, drift or an unprovable comparison
//!    stops the source and writes nothing; otherwise the proven shape is
//!    registered, then a durable marker binds the predecessor and current
//!    lineage, the transition, both schema hashes and the outcome (an unprovable
//!    comparison is recorded as `adapted_unprovable` and proof continues
//!    normally). A primary-key change is a hard stop under every policy, as
//!    in the eager reconciliation it replaces.
//!
//! The marker makes the check rerun-safe: a crash before it repeats the
//! check; a completed check is never inferred from a registered version.

use anyhow::Context;
use deltaforge_core::{CheckpointOrder, SourceError, SourceResult};
use serde::{Deserialize, Serialize};
use storage::ArcStorageBackend;
use storage::adapters::SchemaKey;

use crate::registry_scope::RegistryScope;
use tracing::{info, warn};

use super::MySqlCheckpoint;
use super::mysql_activation::Stored;
use super::mysql_baseline::affects;
use super::mysql_binlog_scan::{
    ProofError, ProofKind, ScanLimits, ScanTag, capture, covers,
    observe_capture, scan_interval, trace_proof,
};
use super::mysql_schema_loader::MySqlSchemaLoader;
use super::mysql_table_schema::MySqlTableSchema;
use crate::durable_checkpoint::{
    WmPos, mysql_checkpoint_position, order_positions,
};
use crate::failover::reconciler::{
    ReconcileOutcome, extract_stored_columns, reconcile_table,
};

pub(crate) const ANCHOR_NS: &str = "schemas.v1.failover.anchor";
pub(crate) const MARKER_NS: &str = "schemas.v1.failover.drift";
/// Version 3: anchors and markers hold exact GTID sets
/// (`super::GTID_FORMAT`); earlier ones may over-claim and are refused.
const FORMAT_VERSION: u32 = 3;

/// A failover into the current lineage (written once per transition).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FailoverAnchor {
    pub format_version: u32,
    pub previous_lineage: String,
    pub current_lineage: String,
    /// The current lineage record's `transition_id`.
    pub transition: String,
    /// F: where the stream continued on the current server, proven there.
    pub position: Option<MySqlCheckpoint>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Outcome {
    NewTable,
    Unchanged,
    Adapted,
    AdaptedUnprovable,
}

/// A completed per-table check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DriftMarker {
    pub format_version: u32,
    pub previous_lineage: String,
    pub current_lineage: String,
    pub transition: String,
    /// The predecessor's latest version compared (none: a new table).
    pub previous_schema_version: Option<i32>,
    pub previous_schema_hash: Option<String>,
    /// The current-lineage version registered from the proven shape (none:
    /// a new table, or adapted unprovable).
    pub current_schema_version: Option<i32>,
    pub current_schema_hash: Option<String>,
    pub outcome: Outcome,
}

/// A registry version and its hash.
type SchemaRef = (i32, String);

/// What a table's first event under the new lineage is.
pub(crate) enum FirstEvent<'a> {
    /// Rows evaluated at `at` (`at_cp` as binlog coordinates), with the
    /// table's current-lineage timeline (records and applicable barriers).
    Rows {
        at: &'a WmPos,
        at_cp: &'a MySqlCheckpoint,
        timeline: &'a [Stored],
    },
    /// A DDL of the table.
    Ddl,
    /// A snapshot of the table.
    Snapshot,
}

/// What a check needs, from a run context or from the snapshot path.
pub(crate) struct DriftEnv<'a> {
    pub backend: &'a ArcStorageBackend,
    pub loader: &'a MySqlSchemaLoader,
    pub scope: &'a RegistryScope,
    /// The run's capture context (its DSN and stop).
    pub capture: &'a super::mysql_binlog_scan::CaptureCtx,
    pub server_uuid: &'a str,
    pub source_id: &'a str,
    pub halt: bool,
    pub lower_case_table_names: u8,
    /// The source's shared proof scanner (rows evaluated in the stream);
    /// `None` scans directly.
    pub proofs: Option<&'a super::mysql_proof_scanner::ProofScanner>,
}

/// One stream per lineage transition, holding at most its one anchor (read with
/// `log_latest`: no enumeration).
fn anchor_stream(
    tenant: &str,
    source_id: &str,
    lineage: &str,
    transition: &str,
) -> String {
    format!(
        "{}/anchor-{transition}",
        SchemaKey::source_prefix(tenant, source_id, lineage)
    )
}

/// The anchor of the current lineage transition, if it was entered by
/// a failover. An unreadable or inconsistent anchor fails.
pub(crate) async fn load_anchor(
    backend: &ArcStorageBackend,
    tenant: &str,
    source_id: &str,
) -> anyhow::Result<Option<FailoverAnchor>> {
    let Some(record) =
        storage::adapters::source_lineage::load(backend, tenant, source_id)
            .await
            .context("read the source lineage record")?
    else {
        return Ok(None);
    };
    let current = record.current.lineage_hash.clone();
    let transition = transition_of(&record)?;
    let stream = anchor_stream(tenant, source_id, &current, &transition);
    let Some((_, bytes)) = backend
        .log_latest(ANCHOR_NS, &stream)
        .await
        .context("read the failover anchor")?
    else {
        return Ok(None);
    };
    let a: FailoverAnchor =
        serde_json::from_slice(&bytes).context("unreadable failover anchor")?;
    anyhow::ensure!(
        a.format_version == FORMAT_VERSION
            && a.current_lineage == current
            && a.transition == transition,
        "failover anchor of another format, lineage or transition: {a:?}"
    );
    Ok(Some(a))
}

/// Record the failover into the current lineage transition once. An
/// existing anchor for the transition is kept (the first recorded failover position is
/// the failover's; a later start may already have replaced the checkpoint,
/// e.g. by a snapshot); one naming another predecessor fails.
pub(crate) async fn record_anchor(
    backend: &ArcStorageBackend,
    tenant: &str,
    source_id: &str,
    previous_lineage: &str,
    position: Option<MySqlCheckpoint>,
) -> anyhow::Result<FailoverAnchor> {
    let record =
        storage::adapters::source_lineage::load(backend, tenant, source_id)
            .await
            .context("read the source lineage record")?
            .context("no source lineage record")?;
    if let Some(existing) = load_anchor(backend, tenant, source_id).await? {
        anyhow::ensure!(
            existing.previous_lineage == previous_lineage,
            "the failover anchor names predecessor {} not {previous_lineage}",
            existing.previous_lineage
        );
        return Ok(existing);
    }
    let current = record.current.lineage_hash.clone();
    let anchor = FailoverAnchor {
        format_version: FORMAT_VERSION,
        previous_lineage: previous_lineage.to_string(),
        current_lineage: current.clone(),
        transition: transition_of(&record)?,
        position,
    };
    let bytes = serde_json::to_vec(&anchor)?;
    backend
        .log_append_if_absent(
            ANCHOR_NS,
            &anchor_stream(tenant, source_id, &current, &anchor.transition),
            "anchor",
            &bytes,
        )
        .await
        .context("persist the failover anchor")?;
    info!(source_id, previous = previous_lineage, current = %current, position = ?anchor.position, "failover anchor recorded");
    Ok(anchor)
}

/// The lineage record's durable transition identity. Every record has one
/// once `establish` has run (older records get one on their first load);
/// none here is never guessed from the clock.
fn transition_of(
    record: &storage::adapters::source_lineage::SourceLineageRecord,
) -> anyhow::Result<String> {
    record
        .transition_id
        .clone()
        .context("the source lineage record has no transition identity")
}

fn marker_id(anchor: &FailoverAnchor) -> String {
    format!("{}-{}", anchor.previous_lineage, anchor.transition)
}

/// Whether the table's check for this failover is complete: its one marker
/// for this anchor, fully validated - supported format, the outcome
/// consistent with the schemas it names, the predecessor version it names
/// still the predecessor's latest with that hash, and the current version
/// it names registered under the current key with that hash. Anything else
/// (several markers, an unreadable or inconsistent one) fails closed.
async fn completed(
    env: &DriftEnv<'_>,
    key: &SchemaKey,
    previous_key: &SchemaKey,
    anchor: &FailoverAnchor,
) -> SourceResult<bool> {
    let invalid = |why: String| SourceError::Schema {
        details: format!(
            "failover drift marker of {}: {why}",
            key.backend_key()
        )
        .into(),
    };
    let mut mine = Vec::new();
    for (_, bytes) in env
        .backend
        .log_list(MARKER_NS, &key.backend_key())
        .await
        .map_err(|e| {
            SourceError::Other(e.context("read failover drift markers"))
        })?
    {
        let m: DriftMarker = serde_json::from_slice(&bytes)
            .map_err(|e| invalid(format!("unreadable ({e})")))?;
        if m.previous_lineage == anchor.previous_lineage
            && m.current_lineage == anchor.current_lineage
            && m.transition == anchor.transition
        {
            mine.push(m);
        }
    }
    let m = match mine.as_slice() {
        [] => return Ok(false),
        [one] => one.clone(),
        _ => return Err(invalid("several markers for one failover".into())),
    };
    if m.format_version != FORMAT_VERSION {
        return Err(invalid(format!(
            "unsupported format {}",
            m.format_version
        )));
    }
    let previous = m
        .previous_schema_version
        .zip(m.previous_schema_hash.clone());
    let current = m.current_schema_version.zip(m.current_schema_hash.clone());
    let none_current =
        m.current_schema_version.is_none() && m.current_schema_hash.is_none();
    let shapes_ok = match m.outcome {
        Outcome::NewTable => {
            m.previous_schema_version.is_none()
                && m.previous_schema_hash.is_none()
                && none_current
        }
        Outcome::AdaptedUnprovable => previous.is_some() && none_current,
        Outcome::Unchanged | Outcome::Adapted => {
            previous.is_some() && current.is_some()
        }
    };
    if !shapes_ok {
        return Err(invalid(format!(
            "outcome {:?} inconsistent with its schemas",
            m.outcome
        )));
    }
    let registry = env.loader.registry();
    let storage = |e: anyhow::Error| {
        SourceError::Other(e.context("validate a failover drift marker"))
    };
    let latest = registry
        .get_latest(previous_key)
        .await
        .map_err(storage)?
        .map(|v| (v.version, v.hash));
    if latest != previous {
        return Err(invalid(format!(
            "it names predecessor schema {previous:?}, the predecessor's \
             latest is {latest:?}"
        )));
    }
    if let Some((version, hash)) = &current
        && registry
            .version_hash(key, *version)
            .await
            .map_err(storage)?
            .as_deref()
            != Some(hash.as_str())
    {
        return Err(invalid(format!(
            "current schema version {version} is not registered with hash \
             {hash}"
        )));
    }
    Ok(true)
}

/// A marker's bytes (deterministic: no timestamp).
fn marker_bytes(
    anchor: &FailoverAnchor,
    previous: Option<SchemaRef>,
    current: Option<SchemaRef>,
    outcome: Outcome,
) -> SourceResult<Vec<u8>> {
    let (previous_schema_version, previous_schema_hash) = previous.unzip();
    let (current_schema_version, current_schema_hash) = current.unzip();
    serde_json::to_vec(&DriftMarker {
        format_version: FORMAT_VERSION,
        previous_lineage: anchor.previous_lineage.clone(),
        current_lineage: anchor.current_lineage.clone(),
        transition: anchor.transition.clone(),
        previous_schema_version,
        previous_schema_hash,
        current_schema_version,
        current_schema_hash,
        outcome,
    })
    .map_err(|e| SourceError::Other(e.into()))
}

/// A marker as a batch op: appended idempotently under its deterministic
/// identity, so a re-applied or resolved batch holds it once.
pub(crate) fn marker_op(
    key: &SchemaKey,
    anchor: &FailoverAnchor,
    previous: Option<SchemaRef>,
    current: Option<SchemaRef>,
    outcome: Outcome,
) -> SourceResult<storage::BatchOp> {
    Ok(storage::BatchOp::LogAppendIdempotent {
        ns: MARKER_NS.into(),
        key: key.backend_key(),
        capture_id: marker_id(anchor),
        value: marker_bytes(anchor, previous, current, outcome)?,
    })
}

/// A drift registration as a request: the shape at F, with F as its
/// checkpoint (what one registration at a time recorded).
pub(crate) fn drift_request(
    key: &SchemaKey,
    anchor: &FailoverAnchor,
    shape: &MySqlTableSchema,
) -> SourceResult<storage::adapters::RegistrationRequest> {
    use schema_registry::SourceSchema as _;
    Ok(storage::adapters::RegistrationRequest {
        key: key.clone(),
        hash: shape.fingerprint().to_string(),
        schema_json: serde_json::to_value(shape)
            .map_err(|e| SourceError::Other(e.into()))?,
        checkpoint: Some(
            serde_json::to_vec(&anchor.position)
                .map_err(|e| SourceError::Other(e.into()))?,
        ),
    })
}

/// Commit drift verdicts atomically: every registration (the shape at F,
/// checkpoint F) and every marker, or none. A completed verdict writes
/// nothing. A conflict decides the registrations again (bounded).
pub(crate) async fn commit_verdicts(
    env: &DriftEnv<'_>,
    anchor: &FailoverAnchor,
    verdicts: Vec<DriftVerdict>,
) -> SourceResult<()> {
    use storage::adapters::RegistrationPlanning;
    use storage::{AtomicBatch, CommitResult, commit_atomic_counted};
    let marks: Vec<_> = verdicts
        .into_iter()
        .filter_map(|v| match v {
            DriftVerdict::Completed => None,
            DriftVerdict::Marker {
                key,
                previous,
                register,
                outcome,
            } => Some((key, previous, register, outcome)),
        })
        .collect();
    if marks.is_empty() {
        return Ok(());
    }
    let registry = env.loader.registry();
    if !std::sync::Arc::ptr_eq(registry.backend(), env.backend) {
        return Err(SourceError::Other(anyhow::anyhow!(
            "failover drift needs the schema registry and its markers in one \
             state store"
        )));
    }
    let mut attempt = 0;
    loop {
        attempt += 1;
        let mut reqs = Vec::new();
        let mut req_of = Vec::with_capacity(marks.len());
        for (key, _, register, _) in &marks {
            req_of.push(match register {
                Some(shape) => {
                    reqs.push(drift_request(key, anchor, shape)?);
                    Some(reqs.len() - 1)
                }
                None => None,
            });
        }
        let (pre, regs) = match registry
            .plan_registrations(&reqs, chrono::Utc::now())
            .await
            .map_err(|e| {
                SourceError::Other(e.context("plan the drift registrations"))
            })? {
            RegistrationPlanning::Planned {
                preconditions,
                registrations,
            } => (preconditions, registrations),
            RegistrationPlanning::NeedsRepair(broken) => {
                for k in &broken {
                    registry.repair_key(k).await.map_err(SourceError::Other)?;
                }
                if attempt > DRIFT_BATCH_RETRIES {
                    return Err(SourceError::Other(anyhow::anyhow!(
                        "the drift registrations still need repair after \
                         {attempt} attempts"
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
        for reg in &regs {
            let base = batch.push_group(reg.ops.clone());
            entries.push(reg.entry_op.map(|e| base + e));
        }
        for ((key, previous, _, outcome), r) in marks.iter().zip(&req_of) {
            let current = r.map(|r| (regs[r].version, regs[r].hash.clone()));
            batch.ops.push(marker_op(
                key,
                anchor,
                previous.clone(),
                current,
                *outcome,
            )?);
        }
        let mut applies = 0;
        match commit_atomic_counted(
            env.backend.as_ref(),
            &batch,
            DRIFT_BATCH_RETRIES,
            &mut applies,
        )
        .await
        .map_err(|e| {
            SourceError::Other(e.context("commit the failover drift outcomes"))
        })? {
            CommitResult::Committed(seqs) => {
                for (reg, entry) in regs.iter().zip(&entries) {
                    if let Some(op) = entry {
                        registry.admit_committed(
                            reg,
                            seqs[*op].expect("a committed log write"),
                        );
                    }
                }
                return Ok(());
            }
            CommitResult::Conflict(why) => {
                if attempt > DRIFT_BATCH_RETRIES {
                    return Err(SourceError::Other(anyhow::anyhow!(
                        "the failover drift outcomes conflicted {attempt} \
                         times (last: {why}); nothing was written"
                    )));
                }
                warn!(
                    source_id = env.source_id,
                    attempt,
                    why,
                    "failover drift: the batch conflicted; planning again"
                );
            }
        }
    }
}

/// Conflicting re-plans, or retries of a batch whose outcome a storage
/// error left unknown, before a drift commit fails closed.
const DRIFT_BATCH_RETRIES: usize = 3;

/// The snapshot's drift check of a discovery page (first event: the
/// snapshot), batched: each table's marker and predecessor read, then the
/// tables needing their shape at F in chunks of at most `MAX_BATCH_TABLES`
/// in discovery order, each with one capture (its own budget may cut it,
/// the rest leading the next chunk), one scan of (F, S] and one atomic
/// commit of its verdicts. A halt stops before anything of its chunk is
/// written; the decisions are [`assess`]'s, one table at a time.
pub(crate) async fn check_snapshot_tables(
    env: &DriftEnv<'_>,
    anchor: &FailoverAnchor,
    tables: &[(String, String)],
    lock_wait_secs: u32,
) -> SourceResult<()> {
    use super::mysql_binlog_scan::{BatchCaptureError, capture_batch};
    let mut decided = Vec::new();
    let mut pending = Vec::new();
    for (db, table) in tables {
        match pre_assess(env, anchor, db, table).await? {
            Pre::Done(v) => decided.push(v),
            Pre::Shape { key, previous } => {
                pending.push((db.clone(), table.clone(), key, previous))
            }
        }
    }
    // Decided without a shape: committed with the first chunk (alone when
    // no table needs its shape), so a halt there leaves nothing written.
    if pending.is_empty() {
        return commit_verdicts(env, anchor, decided).await;
    }
    let from = proof_start(anchor, &FirstEvent::Snapshot);
    let lctn = env.lower_case_table_names;
    let tag = ScanTag {
        source_id: env.source_id,
        kind: ProofKind::FailoverDrift,
    };
    let mut rest = &pending[..];
    while !rest.is_empty() {
        let chunk =
            &rest[..rest.len().min(super::mysql_binlog_scan::MAX_BATCH_TABLES)];
        let names: Vec<(String, String)> = chunk
            .iter()
            .map(|(d, t, ..)| (d.clone(), t.clone()))
            .collect();
        // Each table's shape at F, from the chunk's capture and scan.
        let mut shapes: Vec<Option<MySqlTableSchema>> = vec![None; chunk.len()];
        let mut taken = chunk.len();
        if let Some(from) = &from {
            let cap = match capture_batch(
                env.capture,
                env.server_uuid,
                &anchor.current_lineage,
                &names,
                lock_wait_secs,
                None,
            )
            .await?
            {
                Ok(cap) => Some(cap),
                Err(BatchCaptureError::Failed(ProofError::OtherServer(
                    found,
                ))) => {
                    return Err(SourceError::Lineage {
                        details: format!(
                            "a failover drift capture reached another server \
                             ({found}), expected {}",
                            env.server_uuid
                        )
                        .into(),
                    });
                }
                // Saturation is never an unprovable drift outcome: nothing
                // is decided or recorded, and a restart retries.
                Err(BatchCaptureError::Failed(ProofError::Saturated {
                    scope,
                    ..
                })) => {
                    return Err(
                        crate::incident_drafts::mysql_control_plane_saturated(
                            env.source_id,
                            "failover drift",
                            &names,
                            scope,
                            from,
                        ),
                    );
                }
                Err(e) => {
                    warn!(source_id = env.source_id, error = ?e, tables = chunk.len(), "failover drift: no capture");
                    for (db, table, ..) in chunk {
                        trace_proof(
                            &tag,
                            db,
                            table,
                            None,
                            None,
                            lctn,
                            "no_capture",
                        );
                    }
                    None
                }
            };
            if let Some(cap) = cap {
                // Past the capture's budget: the next chunk's.
                taken = chunk
                    .iter()
                    .position(|(d, t, ..)| {
                        cap.deferred.contains(&(d.clone(), t.clone()))
                    })
                    .unwrap_or(chunk.len())
                    .max(1);
                let scanned = match env.proofs {
                    Some(proofs) => {
                        super::mysql_proof_scanner::shared_interval(
                            proofs,
                            env.capture.dsn().current().expose(),
                            env.source_id,
                            env.server_uuid,
                            &anchor.current_lineage,
                            from,
                            &cap.position,
                            None,
                            None,
                            &tag,
                        )
                        .await
                    }
                    None => {
                        scan_interval(
                            env.capture.dsn().current().expose(),
                            super::mysql_helpers::derive_server_id(&format!(
                                "{}/failover-drift",
                                env.source_id
                            )),
                            env.server_uuid,
                            &anchor.current_lineage,
                            from,
                            &cap.position,
                            &ScanLimits::default(),
                            &tag,
                        )
                        .await
                    }
                };
                match scanned {
                    Ok(report) => {
                        for (i, (db, table, ..)) in
                            chunk[..taken].iter().enumerate()
                        {
                            shapes[i] = match cap.table(db, table) {
                                Some(tcap) => {
                                    observe_capture(&tag, &tcap);
                                    shape_from(
                                        env.source_id,
                                        lctn,
                                        db,
                                        table,
                                        from,
                                        &tcap,
                                        &report,
                                    )
                                }
                                None => {
                                    trace_proof(
                                        &tag,
                                        db,
                                        table,
                                        None,
                                        None,
                                        lctn,
                                        "no_capture",
                                    );
                                    None
                                }
                            };
                        }
                    }
                    Err(ProofError::OtherServer(found)) => {
                        return Err(SourceError::Lineage {
                            details: format!(
                                "a failover drift scan reached another server \
                                 ({found}), expected {}",
                                env.server_uuid
                            )
                            .into(),
                        });
                    }
                    Err(e) => {
                        warn!(source_id = env.source_id, error = ?e, "failover drift: the interval scan failed");
                    }
                }
            }
        }
        let mut verdicts = std::mem::take(&mut decided);
        for ((db, table, key, previous), shape) in
            chunk[..taken].iter().zip(shapes)
        {
            verdicts.push(finish_assess(
                env,
                db,
                table,
                key.clone(),
                previous.clone(),
                shape,
            )?);
        }
        commit_verdicts(env, anchor, verdicts).await?;
        rest = &rest[taken..];
    }
    Ok(())
}

async fn write_marker(
    backend: &ArcStorageBackend,
    key: &SchemaKey,
    anchor: &FailoverAnchor,
    previous: Option<SchemaRef>,
    current: Option<SchemaRef>,
    outcome: Outcome,
) -> SourceResult<()> {
    let (previous_schema_version, previous_schema_hash) = previous.unzip();
    let (current_schema_version, current_schema_hash) = current.unzip();
    let marker = DriftMarker {
        format_version: FORMAT_VERSION,
        previous_lineage: anchor.previous_lineage.clone(),
        current_lineage: anchor.current_lineage.clone(),
        transition: anchor.transition.clone(),
        previous_schema_version,
        previous_schema_hash,
        current_schema_version,
        current_schema_hash,
        outcome,
    };
    let bytes = serde_json::to_vec(&marker)
        .map_err(|e| SourceError::Other(e.into()))?;
    backend
        .log_append_if_absent(
            MARKER_NS,
            &key.backend_key(),
            &marker_id(anchor),
            &bytes,
        )
        .await
        .map_err(|e| {
            SourceError::Other(e.context("persist a failover drift marker"))
        })?;
    Ok(())
}

fn halt_error(
    source_id: &str,
    db: &str,
    table: &str,
    why: &str,
) -> SourceError {
    crate::incident_drafts::schema_drift_blocked(
        source_id,
        &format!("{db}.{table}"),
        why,
        SourceError::Schema {
            details: format!(
                "table {db}.{table} after failover: {why} and \
                 on_schema_drift=halt. Nothing was registered or emitted. \
                 Verify the table on the new server and apply any missing \
                 migrations, or restart with on_schema_drift=adapt."
            )
            .into(),
        },
    )
}

/// Where the interval proving a table's shape at the failover position F
/// starts, for its first event (its lineage the current one): F for a
/// snapshot, the rows' transaction start for rows. `None` when no proof is
/// possible: a DDL first event, F after the rows, or a record of the table
/// or an applicable barrier in (F, E] (anything incomparable included).
pub(crate) fn proof_start(
    anchor: &FailoverAnchor,
    event: &FirstEvent<'_>,
) -> Option<MySqlCheckpoint> {
    let f_cp = anchor.position.clone()?;
    let f = mysql_checkpoint_position(
        &f_cp.file,
        f_cp.pos,
        f_cp.gtid_set.as_deref(),
    )?;
    let from = match event {
        FirstEvent::Ddl => return None,
        FirstEvent::Snapshot => f_cp.clone(),
        FirstEvent::Rows {
            at,
            at_cp,
            timeline,
        } => {
            if !matches!(
                order_positions(&f, at),
                CheckpointOrder::Before | CheckpointOrder::Equal
            ) {
                return None;
            }
            // Nothing of this table, and no applicable barrier, between F
            // and the rows; anything incomparable is unprovable.
            for s in timeline.iter() {
                let p = &s.record.position;
                match (order_positions(&f, p), order_positions(p, at)) {
                    (CheckpointOrder::After | CheckpointOrder::Equal, _) => {}
                    (CheckpointOrder::Before, CheckpointOrder::After) => {}
                    // A record of the table or an applicable barrier (the
                    // timeline holds only those) in (F, E].
                    (
                        CheckpointOrder::Before,
                        CheckpointOrder::Before | CheckpointOrder::Equal,
                    ) => return None,
                    _ => return None,
                }
            }
            (*at_cp).clone()
        }
    };
    let mut from = from;
    from.lineage = Some(anchor.current_lineage.clone());
    Some(from)
}

/// The table's shape proven at F, or `None` (unprovable).
async fn shape_at_failover(
    env: &DriftEnv<'_>,
    anchor: &FailoverAnchor,
    db: &str,
    table: &str,
    event: &FirstEvent<'_>,
) -> SourceResult<Option<MySqlTableSchema>> {
    let Some(from) = proof_start(anchor, event) else {
        return Ok(None);
    };
    let tag = ScanTag {
        source_id: env.source_id,
        kind: ProofKind::FailoverDrift,
    };
    let lctn = env.lower_case_table_names;
    // A stop ends the check (`?`): nothing is decided.
    let cap = match capture(
        env.capture,
        env.server_uuid,
        &anchor.current_lineage,
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
            return Err(SourceError::Lineage {
                details: format!(
                    "a failover drift capture reached another server \
                     ({found}), expected {}",
                    env.server_uuid
                )
                .into(),
            });
        }
        // Saturation is never an unprovable drift outcome: nothing is
        // decided or recorded, and a restart retries.
        Err(ProofError::Saturated { scope, .. }) => {
            return Err(crate::incident_drafts::mysql_control_plane_saturated(
                env.source_id,
                "failover drift",
                &[(db.to_string(), table.to_string())],
                scope,
                &from,
            ));
        }
        Err(e) => {
            warn!(source_id = env.source_id, %db, %table, error = ?e, "failover drift: no capture");
            trace_proof(&tag, db, table, None, None, lctn, "no_capture");
            return Ok(None);
        }
    };
    // The scan (F, capture end] must cover the capture interval.
    let covered = mysql_checkpoint_position(
        &from.file,
        from.pos,
        from.gtid_set.as_deref(),
    )
    .zip(mysql_checkpoint_position(
        &cap.position.file,
        cap.position.pos,
        cap.position.gtid_set.as_deref(),
    ))
    .is_some_and(|(f, end)| covers(&cap, &f, &end));
    if !covered {
        warn!(source_id = env.source_id, %db, %table, "failover drift: the capture started before F");
        trace_proof(&tag, db, table, Some(&cap), None, lctn, "not_covered");
        return Ok(None);
    }
    let scanned = match env.proofs {
        // The shared scanner. Rows in the stream: their position is the
        // stream's boundary. A snapshot: the stream has not started, the
        // failover position F (this request's start) is pinned while the
        // request waits and runs.
        Some(proofs) => {
            let boundary =
                matches!(event, FirstEvent::Rows { .. }).then_some(&from);
            super::mysql_proof_scanner::shared_interval(
                proofs,
                env.capture.dsn().current().expose(),
                env.source_id,
                env.server_uuid,
                &anchor.current_lineage,
                &from,
                &cap.position,
                None,
                boundary,
                &tag,
            )
            .await
        }
        _ => {
            scan_interval(
                env.capture.dsn().current().expose(),
                super::mysql_helpers::derive_server_id(&format!(
                    "{}/failover-drift",
                    env.source_id
                )),
                env.server_uuid,
                &anchor.current_lineage,
                &from,
                &cap.position,
                &ScanLimits::default(),
                &tag,
            )
            .await
        }
    };
    let report = match scanned {
        Ok(report) => report,
        Err(ProofError::OtherServer(found)) => {
            return Err(SourceError::Lineage {
                details: format!(
                    "a failover drift scan reached another server ({found}), \
                     expected {}",
                    env.server_uuid
                )
                .into(),
            });
        }
        Err(e) => {
            warn!(source_id = env.source_id, %db, %table, error = ?e, "failover drift: the interval scan failed");
            trace_proof(&tag, db, table, Some(&cap), None, lctn, "scan_failed");
            return Ok(None);
        }
    };
    Ok(shape_from(
        env.source_id,
        lctn,
        db,
        table,
        &from,
        &cap,
        &report,
    ))
}

/// The shape a capture proves at F, from a scan of (`from`, the capture's
/// end]: the capture inside the scanned interval and nothing in it
/// affecting the table; `None` (traced) otherwise. The one decision of
/// [`shape_at_failover`] and of the batched checks, whatever took the
/// capture and the scan.
pub(crate) fn shape_from(
    source_id: &str,
    lctn: u8,
    db: &str,
    table: &str,
    from: &MySqlCheckpoint,
    cap: &super::mysql_binlog_scan::Captured,
    report: &super::mysql_binlog_scan::ScanReport,
) -> Option<MySqlTableSchema> {
    let tag = ScanTag {
        source_id,
        kind: ProofKind::FailoverDrift,
    };
    let covered = mysql_checkpoint_position(
        &from.file,
        from.pos,
        from.gtid_set.as_deref(),
    )
    .zip(mysql_checkpoint_position(
        &cap.position.file,
        cap.position.pos,
        cap.position.gtid_set.as_deref(),
    ))
    .is_some_and(|(f, end)| covers(cap, &f, &end));
    if !covered {
        warn!(source_id, %db, %table, "failover drift: the capture started before F");
        trace_proof(&tag, db, table, Some(cap), None, lctn, "not_covered");
        return None;
    }
    if report
        .statements
        .iter()
        .any(|st| affects(st, db, table, lctn))
    {
        info!(source_id, %db, %table, "failover drift: a DDL or barrier in the interval");
        trace_proof(
            &tag,
            db,
            table,
            Some(cap),
            Some(report),
            lctn,
            "relevant_ddl",
        );
        return None;
    }
    trace_proof(&tag, db, table, Some(cap), Some(report), lctn, "proven");
    Some(cap.schema.clone())
}

/// Apply `on_schema_drift` to the table's first event under a lineage
/// entered by `anchor` (module docs). Returns once the table may proceed.
pub(crate) async fn check(
    env: &DriftEnv<'_>,
    anchor: &FailoverAnchor,
    db: &str,
    table: &str,
    event: FirstEvent<'_>,
) -> SourceResult<()> {
    let verdict = assess(env, anchor, db, table, event).await?;
    record(env, anchor, db, table, verdict).await
}

/// What a check concluded, before it writes anything.
pub(crate) enum DriftVerdict {
    /// The check completed earlier (its marker exists).
    Completed,
    /// The marker to write; `register`: the shape at the failover position
    /// to register first (its version and hash are the marker's current).
    Marker {
        key: SchemaKey,
        previous: Option<SchemaRef>,
        register: Option<Box<MySqlTableSchema>>,
        outcome: Outcome,
    },
}

/// [`check`]'s reads and decision: everything up to its writes. A halt (or
/// a hard stop) is its error; nothing is written either way.
pub(crate) async fn assess(
    env: &DriftEnv<'_>,
    anchor: &FailoverAnchor,
    db: &str,
    table: &str,
    event: FirstEvent<'_>,
) -> SourceResult<DriftVerdict> {
    match pre_assess(env, anchor, db, table).await? {
        Pre::Done(verdict) => Ok(verdict),
        Pre::Shape { key, previous } => {
            let shape =
                shape_at_failover(env, anchor, db, table, &event).await?;
            finish_assess(env, db, table, key, previous, shape)
        }
    }
}

/// What [`pre_assess`] concluded without the table's shape at F.
pub(crate) enum Pre {
    /// Decided: completed earlier, or a new table (no predecessor).
    Done(DriftVerdict),
    /// The predecessor's latest schema: the verdict needs the shape at F.
    Shape {
        key: SchemaKey,
        previous: schema_registry::SchemaVersion,
    },
}

/// [`assess`]'s reads before the shape at F: a completed marker, or the
/// predecessor's latest (none: a new table; unreadable: an error).
pub(crate) async fn pre_assess(
    env: &DriftEnv<'_>,
    anchor: &FailoverAnchor,
    db: &str,
    table: &str,
) -> SourceResult<Pre> {
    let key = env.scope.key(db, table);
    let previous_key = SchemaKey::new(
        &key.tenant,
        &key.source_id,
        &anchor.previous_lineage,
        db,
        table,
    );
    if completed(env, &key, &previous_key, anchor).await? {
        return Ok(Pre::Done(DriftVerdict::Completed));
    }
    let previous = env
        .loader
        .registry()
        .get_latest(&previous_key)
        .await
        .map_err(|e| {
            SourceError::Other(e.context("read the predecessor schema"))
        })?;
    let Some(previous) = previous else {
        info!(source_id = env.source_id, %db, %table, "failover drift: no predecessor history (a new table)");
        return Ok(Pre::Done(DriftVerdict::Marker {
            key,
            previous: None,
            register: None,
            outcome: Outcome::NewTable,
        }));
    };
    serde_json::from_value::<MySqlTableSchema>(previous.schema_json.clone())
        .map_err(|e| SourceError::Schema {
            details: format!(
                "predecessor schema of {db}.{table} (version {}) is \
                 unreadable: {e}",
                previous.version
            )
            .into(),
        })?;
    Ok(Pre::Shape { key, previous })
}

/// [`assess`]'s decision from the shape proven at F (`None`: unprovable):
/// the on_schema_drift policy applied, a halt or a hard stop its error.
pub(crate) fn finish_assess(
    env: &DriftEnv<'_>,
    db: &str,
    table: &str,
    key: SchemaKey,
    previous: schema_registry::SchemaVersion,
    shape: Option<MySqlTableSchema>,
) -> SourceResult<DriftVerdict> {
    let Some(shape) = shape else {
        if env.halt {
            return Err(halt_error(
                env.source_id,
                db,
                table,
                "its schema at the failover position cannot be proven",
            ));
        }
        warn!(source_id = env.source_id, %db, %table, "failover drift unprovable: adapted");
        return Ok(DriftVerdict::Marker {
            key,
            previous: Some((previous.version, previous.hash.clone())),
            register: None,
            outcome: Outcome::AdaptedUnprovable,
        });
    };
    let current_json = serde_json::to_value(&shape)
        .map_err(|e| SourceError::Other(e.into()))?;
    let deltas = match reconcile_table(
        Some(&previous.schema_json),
        Some(&extract_stored_columns(&current_json)),
    ) {
        ReconcileOutcome::RequiresStop { reason } => {
            return Err(SourceError::Schema {
                details: format!(
                    "table {db}.{table} after failover: {reason} (a hard \
                     stop under every on_schema_drift policy)"
                )
                .into(),
            });
        }
        ReconcileOutcome::Reconcilable(deltas) => deltas,
    };
    let drift = !deltas.is_empty();
    if drift && env.halt {
        return Err(halt_error(
            env.source_id,
            db,
            table,
            &format!(
                "schema drift since the failover ({} change(s))",
                deltas.len()
            ),
        ));
    }
    if drift {
        warn!(source_id = env.source_id, %db, %table, changes = deltas.len(), "failover drift adapted");
    }
    Ok(DriftVerdict::Marker {
        key,
        previous: Some((previous.version, previous.hash.clone())),
        register: Some(Box::new(shape)),
        outcome: if drift {
            Outcome::Adapted
        } else {
            Outcome::Unchanged
        },
    })
}

/// [`check`]'s writes, for a verdict of [`assess`].
pub(crate) async fn record(
    env: &DriftEnv<'_>,
    anchor: &FailoverAnchor,
    db: &str,
    table: &str,
    verdict: DriftVerdict,
) -> SourceResult<()> {
    let DriftVerdict::Marker {
        key,
        previous,
        register,
        outcome,
    } = verdict
    else {
        return Ok(());
    };
    let current = match register {
        Some(shape) => {
            let checkpoint = serde_json::to_vec(&anchor.position)
                .map_err(|e| SourceError::Other(e.into()))?;
            Some(
                env.loader
                    .register_captured(db, table, shape.as_ref(), &checkpoint)
                    .await?,
            )
        }
        None => None,
    };
    write_marker(env.backend, &key, anchor, previous, current, outcome).await
}

/// [`check`] from the stream, once per table and run: the anchor is loaded
/// once per lineage (`RunCtx::failover`); a table already checked in this
/// run (or completed earlier, by its marker) proceeds at once.
pub(crate) async fn check_in_stream(
    ctx: &mut super::RunCtx,
    db: &str,
    table: &str,
    event: FirstEvent<'_>,
) -> SourceResult<()> {
    let scope = ctx.registry_scope.current()?;
    let key = scope.key(db, table).backend_key();
    if ctx.drift_checked.contains(&key) {
        return Ok(());
    }
    let anchor = match &ctx.failover {
        Some(anchor) => anchor.clone(),
        None => {
            let anchor =
                load_anchor(&ctx.registry_backend, &ctx.tenant, &ctx.source_id)
                    .await
                    .map_err(SourceError::Other)?
                    .map(std::sync::Arc::new);
            ctx.failover = Some(anchor.clone());
            anchor
        }
    };
    if let Some(anchor) = anchor {
        let server_uuid = ctx.expected_uuid()?;
        let env = DriftEnv {
            backend: &ctx.registry_backend,
            loader: &ctx.schema,
            scope: &scope,
            capture: &ctx.capture,
            server_uuid: &server_uuid,
            source_id: &ctx.source_id,
            halt: ctx.on_schema_drift == deltaforge_config::OnSchemaDrift::Halt,
            lower_case_table_names: ctx.lower_case_table_names,
            proofs: Some(&ctx.proof_scanner),
        };
        check(&env, &anchor, db, table, event).await?;
    }
    ctx.drift_checked.insert(key);
    // The table passed its drift check: accepted.
    ctx.drift_resolver.accepted(&format!("{db}.{table}")).await;
    Ok(())
}

/// One of a window's tables, for its row-path drift check from the
/// window's own capture and scan answer.
pub(crate) struct WindowTable<'a> {
    pub db: &'a str,
    pub table: &'a str,
    /// Its rows' transaction start E (the window's scan starts there).
    pub at: WmPos,
    pub at_cp: MySqlCheckpoint,
    pub timeline: Vec<Stored>,
    /// Its shape in the window's capture (`None`: not captured).
    pub capture: Option<super::mysql_binlog_scan::Captured>,
    /// The window's scan answer for it: the interval (E, S].
    pub report: &'a super::mysql_binlog_scan::ScanReport,
}

/// A window's row-path drift: the anchor checked against, and per window
/// table its verdict (`None`: nothing to check, already checked in this run
/// or no failover).
pub(crate) struct WindowDrift {
    pub anchor: Option<std::sync::Arc<FailoverAnchor>>,
    pub verdicts: Vec<Option<DriftVerdict>>,
}

/// The row-path drift of a window's tables, decided from the window's own
/// capture and each table's scan answer (the interval (E, S] its proof
/// starts at) by [`pre_assess`], [`proof_start`], [`shape_from`] and
/// [`finish_assess`]: [`assess`]'s decision, without a capture and a scan
/// of its own per table. A halt (or a hard stop) of any table is the
/// error, before anything of the window is written; the verdicts are the
/// window batch's to commit ([`drift_request`], [`marker_op`]), and
/// [`window_checked`] notes them once it committed.
pub(crate) async fn window_verdicts(
    ctx: &mut super::RunCtx,
    tables: &[WindowTable<'_>],
) -> SourceResult<WindowDrift> {
    let scope = ctx.registry_scope.current()?;
    let mut verdicts: Vec<Option<DriftVerdict>> =
        tables.iter().map(|_| None).collect();
    let pending: Vec<usize> = (0..tables.len())
        .filter(|i| {
            !ctx.drift_checked.contains(
                &scope.key(tables[*i].db, tables[*i].table).backend_key(),
            )
        })
        .collect();
    if pending.is_empty() {
        return Ok(WindowDrift {
            anchor: None,
            verdicts,
        });
    }
    let anchor = match &ctx.failover {
        Some(anchor) => anchor.clone(),
        None => {
            let anchor =
                load_anchor(&ctx.registry_backend, &ctx.tenant, &ctx.source_id)
                    .await
                    .map_err(SourceError::Other)?
                    .map(std::sync::Arc::new);
            ctx.failover = Some(anchor.clone());
            anchor
        }
    };
    let Some(anchor) = anchor else {
        return Ok(WindowDrift {
            anchor: None,
            verdicts,
        });
    };
    let server_uuid = ctx.expected_uuid()?;
    let env = DriftEnv {
        backend: &ctx.registry_backend,
        loader: &ctx.schema,
        scope: &scope,
        capture: &ctx.capture,
        server_uuid: &server_uuid,
        source_id: &ctx.source_id,
        halt: ctx.on_schema_drift == deltaforge_config::OnSchemaDrift::Halt,
        lower_case_table_names: ctx.lower_case_table_names,
        proofs: Some(&ctx.proof_scanner),
    };
    for i in pending {
        verdicts[i] = Some(verdict_from(&env, &anchor, &tables[i]).await?);
    }
    Ok(WindowDrift {
        anchor: Some(anchor),
        verdicts,
    })
}

/// One window table's verdict from the window's capture and scan answer:
/// [`assess`]'s decision (its reads, the proof's start, the shape the
/// capture proves, the policy), with no capture or scan of its own.
pub(crate) async fn verdict_from(
    env: &DriftEnv<'_>,
    anchor: &FailoverAnchor,
    t: &WindowTable<'_>,
) -> SourceResult<DriftVerdict> {
    match pre_assess(env, anchor, t.db, t.table).await? {
        Pre::Done(v) => Ok(v),
        Pre::Shape { key, previous } => {
            let event = FirstEvent::Rows {
                at: &t.at,
                at_cp: &t.at_cp,
                timeline: &t.timeline,
            };
            let shape = match (proof_start(anchor, &event), &t.capture) {
                (Some(from), Some(cap)) => shape_from(
                    env.source_id,
                    env.lower_case_table_names,
                    t.db,
                    t.table,
                    &from,
                    cap,
                    t.report,
                ),
                _ => None,
            };
            finish_assess(env, t.db, t.table, key, previous, shape)
        }
    }
}

/// Note a window's drift-checked tables once its batch committed: each is
/// checked for this run, and its resolver learns it.
pub(crate) async fn window_checked(
    ctx: &mut super::RunCtx,
    tables: &[(String, String)],
) -> SourceResult<()> {
    let scope = ctx.registry_scope.current()?;
    for (db, table) in tables {
        if ctx.drift_checked.insert(scope.key(db, table).backend_key()) {
            ctx.drift_resolver.accepted(&format!("{db}.{table}")).await;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use storage::adapters::LineageDescriptor;
    use storage::adapters::source_lineage;

    const A: &str = "11111111-1111-1111-1111-111111111111";
    const B: &str = "22222222-2222-2222-2222-222222222222";

    fn hash(uuid: &str) -> String {
        LineageDescriptor::mysql(uuid).unwrap().lineage_hash()
    }

    fn at(set: &str) -> Option<MySqlCheckpoint> {
        Some(MySqlCheckpoint {
            file: String::new(),
            pos: 0,
            gtid_set: Some(format!("{A}:{set}")),
            lineage: None,
            snapshot_completed: None,
            snapshot_chain: None,
        })
    }

    async fn enter(backend: &ArcStorageBackend, uuid: &str) {
        source_lineage::establish(
            backend,
            "t",
            "s",
            LineageDescriptor::mysql(uuid).unwrap(),
        )
        .await
        .unwrap();
        // Distinct timestamps are not relied on; keep them apart anyway.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    /// An anchor belongs to one lineage transition: the first recorded
    /// failover position is kept, another predecessor fails, and a later
    /// transition into the same lineage starts without an anchor.
    #[tokio::test]
    async fn anchors_are_bound_to_the_lineage_transition() {
        let backend: ArcStorageBackend =
            Arc::new(storage::MemoryStorageBackend::new());
        enter(&backend, A).await;
        assert!(load_anchor(&backend, "t", "s").await.unwrap().is_none());
        enter(&backend, B).await;
        let first = record_anchor(&backend, "t", "s", &hash(A), at("1-5"))
            .await
            .unwrap();
        assert_eq!(first.current_lineage, hash(B));
        // A later reconciliation (e.g. after a snapshot moved the
        // checkpoint) keeps the first position.
        let again = record_anchor(&backend, "t", "s", &hash(A), at("1-9"))
            .await
            .unwrap();
        assert_eq!(again, first);
        assert_eq!(load_anchor(&backend, "t", "s").await.unwrap(), Some(first));
        assert!(
            record_anchor(&backend, "t", "s", &hash(B), at("1-5"))
                .await
                .is_err()
        );
        // B -> A -> B: a new transition has no anchor until one is recorded.
        enter(&backend, A).await;
        enter(&backend, B).await;
        assert!(load_anchor(&backend, "t", "s").await.unwrap().is_none());
        let second = record_anchor(&backend, "t", "s", &hash(A), at("1-7"))
            .await
            .unwrap();
        assert_eq!(second.position, at("1-7"));
        assert_eq!(
            load_anchor(&backend, "t", "s").await.unwrap(),
            Some(second)
        );
    }

    /// Reviewer regression: A -> B with a completed table marker, B -> A,
    /// then A -> B again with the very same `established_at_ms` as the first
    /// A -> B. The old anchor and marker cannot satisfy the new transition:
    /// they are bound to the first transition's durable identity, not to
    /// the clock.
    #[tokio::test]
    async fn a_repeated_transition_never_reuses_an_old_anchor_or_marker() {
        use crate::registry_scope::SharedRegistryScope;
        let backend: ArcStorageBackend =
            Arc::new(storage::MemoryStorageBackend::new());
        let registry = storage::DurableSchemaRegistry::for_testing();
        let shared = SharedRegistryScope::new("s");
        shared.publish_for_test("t", LineageDescriptor::mysql(B).unwrap());
        let scope = shared.current().unwrap();
        let loader = MySqlSchemaLoader::new(
            "mysql://none@127.0.0.1:1/none",
            registry.clone(),
            "t",
            shared.clone(),
        );
        let env = DriftEnv {
            backend: &backend,
            loader: &loader,
            scope: &scope,
            capture: &super::super::mysql_binlog_scan::CaptureCtx::detached(""),
            server_uuid: B,
            source_id: "s",
            halt: true,
            lower_case_table_names: 0,
            proofs: None,
        };
        let key = scope.key("d", "x");
        let previous_key = SchemaKey::new("t", "s", hash(A), "d", "x");

        // 1. A -> B, and the table's check completes.
        enter(&backend, A).await;
        enter(&backend, B).await;
        let first_at =
            storage::adapters::source_lineage::load(&backend, "t", "s")
                .await
                .unwrap()
                .unwrap()
                .established_at_ms;
        let first = record_anchor(&backend, "t", "s", &hash(A), at("1-5"))
            .await
            .unwrap();
        write_marker(&backend, &key, &first, None, None, Outcome::NewTable)
            .await
            .unwrap();
        assert!(completed(&env, &key, &previous_key, &first).await.unwrap());

        // 2. B -> A. 3. A -> B again, at the same clock millisecond.
        enter(&backend, A).await;
        enter(&backend, B).await;
        storage::adapters::test_util::rewrite_lineage_record(
            &backend,
            "t",
            "s",
            |r| r.established_at_ms = first_at,
        )
        .await
        .unwrap();

        // 4. Neither the old anchor nor its marker belongs to it.
        assert!(load_anchor(&backend, "t", "s").await.unwrap().is_none());
        let second = record_anchor(&backend, "t", "s", &hash(A), at("1-9"))
            .await
            .unwrap();
        assert_ne!(second.transition, first.transition);
        assert_eq!(second.position, at("1-9"), "not the old position");
        assert!(
            !completed(&env, &key, &previous_key, &second).await.unwrap(),
            "the old marker does not complete the new transition"
        );
    }

    /// A completed marker is trusted only after validation: exactly one per
    /// failover, a supported format, an outcome consistent with the schemas
    /// it names, the predecessor version still the predecessor's latest
    /// with that hash, and the current version registered with that hash.
    #[tokio::test]
    async fn a_completed_marker_is_validated_before_it_is_trusted() {
        use crate::registry_scope::SharedRegistryScope;
        let registry = storage::DurableSchemaRegistry::for_testing();
        let shared = SharedRegistryScope::new("s");
        shared.publish_for_test("t", LineageDescriptor::mysql(B).unwrap());
        let scope = shared.current().unwrap();
        let loader = MySqlSchemaLoader::new(
            "mysql://none@127.0.0.1:1/none",
            registry.clone(),
            "t",
            shared.clone(),
        );
        let key = scope.key("d", "x");
        let previous_key = SchemaKey::new("t", "s", hash(A), "d", "x");
        let json = serde_json::json!({ "columns": [] });
        let pv = registry
            .register_with_checkpoint(&previous_key, "hp", &json, None)
            .await
            .unwrap();
        let cv = registry
            .register_with_checkpoint(&key, "hc", &json, None)
            .await
            .unwrap();
        let anchor = FailoverAnchor {
            format_version: FORMAT_VERSION,
            previous_lineage: hash(A),
            current_lineage: key.lineage_hash.clone(),
            transition: "7".repeat(32),
            position: None,
        };
        let valid = DriftMarker {
            format_version: FORMAT_VERSION,
            previous_lineage: hash(A),
            current_lineage: key.lineage_hash.clone(),
            transition: "7".repeat(32),
            previous_schema_version: Some(pv),
            previous_schema_hash: Some("hp".into()),
            current_schema_version: Some(cv),
            current_schema_hash: Some("hc".into()),
            outcome: Outcome::Adapted,
        };
        let check = |markers: Vec<DriftMarker>| {
            let (registry, loader, scope, key, previous_key, anchor) =
                (&registry, &loader, &scope, &key, &previous_key, &anchor);
            async move {
                let _ = registry;
                let backend: ArcStorageBackend =
                    Arc::new(storage::MemoryStorageBackend::new());
                for (i, m) in markers.iter().enumerate() {
                    backend
                        .log_append_if_absent(
                            MARKER_NS,
                            &key.backend_key(),
                            &format!("m{i}"),
                            &serde_json::to_vec(m).unwrap(),
                        )
                        .await
                        .unwrap();
                }
                let env = DriftEnv {
                    backend: &backend,
                    loader,
                    scope,
                    capture:
                        &super::super::mysql_binlog_scan::CaptureCtx::detached(
                            "",
                        ),
                    server_uuid: B,
                    source_id: "s",
                    halt: true,
                    lower_case_table_names: 0,
                    proofs: None,
                };
                completed(&env, key, previous_key, anchor).await
            }
        };
        assert!(!check(vec![]).await.unwrap());
        assert!(check(vec![valid.clone()]).await.unwrap());
        // Another transition's marker is not this failover's.
        let other_transition = DriftMarker {
            transition: "8".repeat(32),
            ..valid.clone()
        };
        assert!(!check(vec![other_transition]).await.unwrap());
        let unchanged_new = DriftMarker {
            outcome: Outcome::NewTable,
            ..valid.clone()
        };
        let adapted_without_current = DriftMarker {
            current_schema_version: None,
            current_schema_hash: None,
            ..valid.clone()
        };
        let wrong_current = DriftMarker {
            current_schema_hash: Some("hx".into()),
            ..valid.clone()
        };
        let future_format = DriftMarker {
            format_version: FORMAT_VERSION + 1,
            ..valid.clone()
        };
        for bad in [
            vec![unchanged_new],
            vec![adapted_without_current],
            vec![wrong_current],
            vec![future_format],
            vec![valid.clone(), valid.clone()],
        ] {
            assert!(check(bad.clone()).await.is_err(), "{bad:?}");
        }
        // The predecessor moved on: the marker no longer names its latest.
        registry
            .register_with_checkpoint(
                &previous_key,
                "hp2",
                &serde_json::json!({ "columns": [1] }),
                None,
            )
            .await
            .unwrap();
        assert!(check(vec![valid]).await.is_err());
    }

    /// Snapshot-time drift through batched admission against the per-table
    /// path, on a live server: the same outcomes, atomic chunks.
    mod live {
        use super::*;
        use crate::mysql::mysql_binlog_scan::CaptureCtx;
        use crate::mysql::mysql_binlog_scan::tests::live::{
            position, server, sql, uuid,
        };
        use crate::registry_scope::SharedRegistryScope;
        use schema_registry::SourceSchema as _;
        use storage::DurableSchemaRegistry;

        const PREV: &str = "33333333-3333-3333-3333-333333333333";

        struct Store {
            backend: ArcStorageBackend,
            loader: MySqlSchemaLoader,
            registry: Arc<DurableSchemaRegistry>,
            scope: Arc<RegistryScope>,
            capture: CaptureCtx,
            anchor: FailoverAnchor,
        }

        /// The fixture's tables: `u` unchanged since its predecessor, `c`
        /// changed before F, `n` new (no predecessor), `x` changed after F.
        const TABLES: [&str; 4] = ["u", "c", "n", "x"];

        /// The server prepared once per database: the predecessor shapes,
        /// then F, then the change after it.
        async fn prepare(
            dsn: &str,
            db: &str,
        ) -> (MySqlCheckpoint, Vec<(String, MySqlTableSchema)>) {
            sql(
                dsn,
                &[
                    &format!("CREATE DATABASE {db}"),
                    &format!("CREATE TABLE {db}.u (id INT PRIMARY KEY)"),
                    &format!("CREATE TABLE {db}.c (id INT PRIMARY KEY)"),
                    &format!("CREATE TABLE {db}.x (id INT PRIMARY KEY)"),
                ],
            )
            .await;
            let mut conn = mysql_async::Conn::from_url(dsn).await.unwrap();
            let shapes =
                crate::mysql::mysql_schema_loader::fetch_table_schemas_on(
                    &mut conn,
                    &[(db, "u"), (db, "c"), (db, "x")],
                )
                .await
                .unwrap();
            let previous = ["u", "c", "x"]
                .iter()
                .map(|t| {
                    (
                        t.to_string(),
                        shapes[&(db.to_string(), t.to_string())].clone(),
                    )
                })
                .collect();
            sql(
                dsn,
                &[
                    &format!("ALTER TABLE {db}.c ADD COLUMN extra INT"),
                    &format!("CREATE TABLE {db}.n (id INT PRIMARY KEY)"),
                ],
            )
            .await;
            let f = position(dsn).await;
            sql(dsn, &[&format!("ALTER TABLE {db}.x ADD COLUMN later INT")])
                .await;
            (f, previous)
        }

        /// A store that entered this server's lineage from `PREV`, with the
        /// predecessor shapes registered and the failover anchored at F.
        async fn store(
            backend: ArcStorageBackend,
            dsn: &str,
            db: &str,
            f: &MySqlCheckpoint,
            previous: &[(String, MySqlTableSchema)],
        ) -> Store {
            let here = uuid(dsn).await;
            enter(&backend, PREV).await;
            enter(&backend, &here).await;
            let registry =
                DurableSchemaRegistry::new(backend.clone()).await.unwrap();
            for (t, schema) in previous {
                let k = SchemaKey::new("t", "s", hash(PREV), db, t);
                registry
                    .register_version(
                        &k,
                        &schema.fingerprint(),
                        &serde_json::to_value(schema).unwrap(),
                        None,
                    )
                    .await
                    .unwrap();
            }
            let shared = SharedRegistryScope::new("s");
            let scope = shared.publish_for_test(
                "t",
                LineageDescriptor::mysql(&here).unwrap(),
            );
            let anchor =
                record_anchor(&backend, "t", "s", &hash(PREV), Some(f.clone()))
                    .await
                    .unwrap();
            Store {
                loader: MySqlSchemaLoader::new(
                    dsn,
                    registry.clone(),
                    "t",
                    shared,
                ),
                registry,
                scope,
                capture: CaptureCtx::detached(dsn),
                anchor,
                backend,
            }
        }

        fn env<'a>(
            st: &'a Store,
            server_uuid: &'a str,
            halt: bool,
        ) -> DriftEnv<'a> {
            DriftEnv {
                backend: &st.backend,
                loader: &st.loader,
                scope: &st.scope,
                capture: &st.capture,
                server_uuid,
                source_id: "s",
                halt,
                lower_case_table_names: 0,
                proofs: None,
            }
        }

        fn tables(db: &str) -> Vec<(String, String)> {
            TABLES
                .iter()
                .map(|t| (db.to_string(), t.to_string()))
                .collect()
        }

        /// Each table's markers, the store's transition identity masked.
        async fn markers(st: &Store, db: &str, t: &str) -> Vec<String> {
            st.backend
                .log_list(MARKER_NS, &st.scope.key(db, t).backend_key())
                .await
                .unwrap()
                .into_iter()
                .map(|(_, b)| {
                    let mut m: DriftMarker =
                        serde_json::from_slice(&b).unwrap();
                    m.transition = String::new();
                    serde_json::to_string(&m).unwrap()
                })
                .collect()
        }

        async fn latest(
            st: &Store,
            db: &str,
            t: &str,
        ) -> Option<(i32, String, serde_json::Value, Option<Vec<u8>>)> {
            st.registry
                .get_latest(&st.scope.key(db, t))
                .await
                .unwrap()
                .map(|v| (v.version, v.hash, v.schema_json, v.checkpoint))
        }

        /// The batched check stores exactly what the per-table check stored:
        /// the same markers (byte for byte, but for each store's transition
        /// identity) and the same registrations, for an unchanged, a
        /// changed, a new and an unprovable (changed after F) table.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn batched_snapshot_drift_stores_what_the_per_table_check_stored()
        {
            let dsn = server(true).await;
            let db = "drift_eq";
            let (f, previous) = prepare(&dsn, db).await;
            let here = uuid(&dsn).await;
            let old = store(
                Arc::new(storage::MemoryStorageBackend::new()),
                &dsn,
                db,
                &f,
                &previous,
            )
            .await;
            for (d, t) in tables(db) {
                check(
                    &env(&old, &here, false),
                    &old.anchor,
                    &d,
                    &t,
                    FirstEvent::Snapshot,
                )
                .await
                .unwrap();
            }
            let new = store(
                Arc::new(storage::MemoryStorageBackend::new()),
                &dsn,
                db,
                &f,
                &previous,
            )
            .await;
            check_snapshot_tables(
                &env(&new, &here, false),
                &new.anchor,
                &tables(db),
                8,
            )
            .await
            .unwrap();
            for (t, outcome) in TABLES.iter().zip([
                "unchanged",
                "adapted",
                "new_table",
                "adapted_unprovable",
            ]) {
                let (a, b) =
                    (markers(&old, db, t).await, markers(&new, db, t).await);
                assert_eq!(a.len(), 1, "{t}: {a:?}");
                assert_eq!(a, b, "{t}: markers");
                assert!(
                    a[0].contains(&format!("\"outcome\":\"{outcome}\"")),
                    "{t}: {a:?}"
                );
                assert_eq!(
                    latest(&old, db, t).await,
                    latest(&new, db, t).await,
                    "{t}: registration"
                );
            }
        }

        /// Under halt a drifted table refuses its whole chunk before any
        /// write: no marker and no registration for any table of it.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_halt_refuses_the_whole_chunk_before_any_write() {
            let dsn = server(true).await;
            let db = "drift_halt";
            let (f, previous) = prepare(&dsn, db).await;
            let here = uuid(&dsn).await;
            let st = store(
                Arc::new(storage::MemoryStorageBackend::new()),
                &dsn,
                db,
                &f,
                &previous,
            )
            .await;
            let err = check_snapshot_tables(
                &env(&st, &here, true),
                &st.anchor,
                &tables(db),
                8,
            )
            .await
            .unwrap_err();
            assert_eq!(
                err.draft().map(|d| d.reason_code),
                Some(deltaforge_core::incident::ReasonCode::SchemaDriftBlocked),
                "{err}"
            );
            for t in TABLES {
                assert!(markers(&st, db, t).await.is_empty(), "{t}");
                assert_eq!(latest(&st, db, t).await, None, "{t}");
            }
        }

        /// A chunk whose commit acknowledgement is lost resolves through its
        /// marker to the original complete outcome; checking again finds
        /// every table completed and writes nothing.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn an_ambiguous_drift_commit_resolves_to_the_original_outcome() {
            use storage::BatchFaults as _;
            let dsn = server(true).await;
            let db = "drift_ack";
            let (f, previous) = prepare(&dsn, db).await;
            let here = uuid(&dsn).await;
            let mem = Arc::new(storage::MemoryStorageBackend::new());
            let st = store(mem.clone(), &dsn, db, &f, &previous).await;
            mem.set_batch_fault(Some(storage::BatchFault::AfterCommit));
            check_snapshot_tables(
                &env(&st, &here, false),
                &st.anchor,
                &tables(db),
                8,
            )
            .await
            .unwrap();
            let first: Vec<_> = all_markers(&st, db).await;
            check_snapshot_tables(
                &env(&st, &here, false),
                &st.anchor,
                &tables(db),
                8,
            )
            .await
            .unwrap();
            assert_eq!(
                all_markers(&st, db).await,
                first,
                "nothing more written"
            );
            for m in &first {
                assert_eq!(m.len(), 1);
            }
        }

        /// A drift check whose capture gets no control-plane connection
        /// (batched and per table) stops with `mysql_control_plane_saturated`
        /// and decides nothing: no marker and no registration. Saturation is
        /// never an unprovable drift outcome.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_saturated_drift_capture_decides_nothing() {
            use crate::mysql::mysql_control::{Budget, Class, ControlPool};
            let dsn = server(true).await;
            let db = "drift_saturated";
            let (f, previous) = prepare(&dsn, db).await;
            let here = uuid(&dsn).await;
            let mut st = store(
                Arc::new(storage::MemoryStorageBackend::new()),
                &dsn,
                db,
                &f,
                &previous,
            )
            .await;
            // G = R + 1: another source holds the only ordinary permit.
            let budget: &'static Budget = Box::leak(Box::new(Budget::new(
                crate::mysql::mysql_binlog_scan::KILL_RESERVE + 1,
            )));
            let other = ControlPool::detached(budget, "other", 2);
            let held = other
                .connect(
                    Class::Catalog,
                    &dsn,
                    &here,
                    std::time::Duration::from_secs(5),
                )
                .await
                .unwrap();
            st.capture = CaptureCtx::detached_on(
                dsn.as_str(),
                ControlPool::detached(budget, "starved", 2),
            );
            let saturated = |e: &SourceError| {
                e.draft().map(|d| d.reason_code)
                    == Some(
                        deltaforge_core::incident::ReasonCode::MysqlControlPlaneSaturated,
                    )
            };
            let err = check_snapshot_tables(
                &env(&st, &here, false),
                &st.anchor,
                &tables(db),
                1,
            )
            .await
            .unwrap_err();
            assert!(saturated(&err), "{err}");
            let err = check(
                &env(&st, &here, false),
                &st.anchor,
                db,
                "c",
                FirstEvent::Snapshot,
            )
            .await
            .unwrap_err();
            assert!(saturated(&err), "{err}");
            for t in TABLES {
                assert!(markers(&st, db, t).await.is_empty(), "{t}");
                assert_eq!(latest(&st, db, t).await, None, "{t}");
            }
            held.release().await;
        }

        async fn all_markers(st: &Store, db: &str) -> Vec<Vec<String>> {
            let mut out = Vec::new();
            for t in TABLES {
                out.push(markers(st, db, t).await);
            }
            out
        }

        /// A chunk whose commit fails leaves nothing durable: no marker and
        /// no registration; checking again records each table once.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_failed_drift_commit_leaves_nothing_and_a_rerun_records_once()
        {
            let dsn = server(true).await;
            let db = "drift_fail";
            let (f, previous) = prepare(&dsn, db).await;
            let here = uuid(&dsn).await;
            let faults =
                Arc::new(storage::adapters::test_util::FaultBackend::new());
            let st = store(faults.clone(), &dsn, db, &f, &previous).await;
            faults.fail_writes_to.lock().unwrap().push(MARKER_NS.into());
            assert!(
                check_snapshot_tables(
                    &env(&st, &here, false),
                    &st.anchor,
                    &tables(db),
                    8
                )
                .await
                .is_err()
            );
            for t in TABLES {
                assert!(markers(&st, db, t).await.is_empty(), "{t}");
                assert_eq!(latest(&st, db, t).await, None, "{t}");
            }
            faults.fail_writes_to.lock().unwrap().clear();
            check_snapshot_tables(
                &env(&st, &here, false),
                &st.anchor,
                &tables(db),
                8,
            )
            .await
            .unwrap();
            for m in all_markers(&st, db).await {
                assert_eq!(m.len(), 1, "{m:?}");
            }
        }

        /// A verdict, summarized for comparison: its outcome, predecessor
        /// and registered shape, or the incident of its error.
        fn summary(v: SourceResult<DriftVerdict>) -> String {
            match v {
                Ok(DriftVerdict::Completed) => "completed".into(),
                Ok(DriftVerdict::Marker {
                    previous,
                    register,
                    outcome,
                    ..
                }) => format!(
                    "{outcome:?} {previous:?} {:?}",
                    register.map(|s| s.fingerprint().to_string())
                ),
                Err(e) => {
                    format!("error {:?}", e.draft().map(|d| d.reason_code))
                }
            }
        }

        /// Every table's verdict both ways, under one anchor, timeline and
        /// policy: the per-table check (its own capture and scan) and the
        /// window's inputs (one capture of every table, one scan of
        /// (E, S]).
        #[allow(clippy::too_many_arguments)]
        async fn compare(
            st: &Store,
            dsn: &str,
            db: &str,
            e_cp: &MySqlCheckpoint,
            anchor: &FailoverAnchor,
            timeline: &[Stored],
            halt: bool,
            case: &str,
        ) -> Vec<String> {
            use crate::mysql::mysql_binlog_scan::{
                capture_batch, scan_interval,
            };
            let here = uuid(dsn).await;
            let env = env(st, &here, halt);
            let e = mysql_checkpoint_position(
                &e_cp.file,
                e_cp.pos,
                e_cp.gtid_set.as_deref(),
            )
            .unwrap();
            let names = tables(db);
            let mut out = Vec::new();
            let cap = capture_batch(
                &st.capture,
                &here,
                &anchor.current_lineage,
                &names,
                8,
                None,
            )
            .await
            .expect("not cancelled")
            .unwrap();
            let report = scan_interval(
                dsn,
                4_000_777,
                &here,
                &anchor.current_lineage,
                e_cp,
                &cap.position,
                &ScanLimits::default(),
                &ScanTag {
                    source_id: "s",
                    kind: ProofKind::FailoverDrift,
                },
            )
            .await
            .unwrap();
            for (d, t) in &names {
                let old = assess(
                    &env,
                    anchor,
                    d,
                    t,
                    FirstEvent::Rows {
                        at: &e,
                        at_cp: e_cp,
                        timeline,
                    },
                )
                .await;
                let new = verdict_from(
                    &env,
                    anchor,
                    &WindowTable {
                        db: d,
                        table: t,
                        at: e.clone(),
                        at_cp: e_cp.clone(),
                        timeline: timeline.to_vec(),
                        capture: cap.table(d, t),
                        report: &report,
                    },
                )
                .await;
                let (old, new) = (summary(old), summary(new));
                assert_eq!(old, new, "{case}: {t}");
                out.push(old);
            }
            out
        }

        /// Row-path drift decided from a window's inputs is exactly the
        /// per-table check's verdict: unchanged, changed, new and
        /// later-changed tables; the halt policy; a record between F and E;
        /// an interval a DDL affects; rows before F.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn reused_window_inputs_give_the_per_table_verdict() {
            let dsn = server(true).await;
            let db = "drift_reuse";
            let (f, previous) = prepare(&dsn, db).await;
            let st = store(
                Arc::new(storage::MemoryStorageBackend::new()),
                &dsn,
                db,
                &f,
                &previous,
            )
            .await;
            let e_cp = MySqlCheckpoint {
                lineage: Some(st.anchor.current_lineage.clone()),
                ..position(&dsn).await
            };
            let e = mysql_checkpoint_position(
                &e_cp.file,
                e_cp.pos,
                e_cp.gtid_set.as_deref(),
            )
            .unwrap();
            // The fixture's tables: u, c, n, x.
            let adapt =
                compare(&st, &dsn, db, &e_cp, &st.anchor, &[], false, "adapt")
                    .await;
            assert!(adapt[0].starts_with("Unchanged"), "{adapt:?}");
            assert!(adapt[1].starts_with("Adapted "), "{adapt:?}");
            assert!(adapt[2].starts_with("NewTable"), "{adapt:?}");
            let halt =
                compare(&st, &dsn, db, &e_cp, &st.anchor, &[], true, "halt")
                    .await;
            assert!(halt[0].starts_with("Unchanged"), "{halt:?}");
            assert!(halt[1].starts_with("error"), "{halt:?}");
            let between = Stored {
                capture_id: "between".into(),
                record: crate::mysql::mysql_activation::Record::new(
                    e,
                    crate::mysql::mysql_activation::Kind::Ddl,
                ),
            };
            let recorded = compare(
                &st,
                &dsn,
                db,
                &e_cp,
                &st.anchor,
                &[between],
                false,
                "a record in (F, E]",
            )
            .await;
            assert!(
                recorded[0].starts_with("AdaptedUnprovable"),
                "{recorded:?}"
            );
            sql(&dsn, &[&format!("ALTER TABLE {db}.u ADD COLUMN y INT")]).await;
            let ddl = compare(
                &st,
                &dsn,
                db,
                &e_cp,
                &st.anchor,
                &[],
                false,
                "a DDL in (E, S]",
            )
            .await;
            assert!(ddl[0].starts_with("AdaptedUnprovable"), "{ddl:?}");
            assert!(ddl[1].starts_with("Adapted "), "{ddl:?}");
            let later = FailoverAnchor {
                position: Some(position(&dsn).await),
                ..st.anchor.clone()
            };
            let before = compare(
                &st,
                &dsn,
                db,
                &e_cp,
                &later,
                &[],
                false,
                "rows before F",
            )
            .await;
            assert!(before[0].starts_with("AdaptedUnprovable"), "{before:?}");
            let refused = compare(
                &st,
                &dsn,
                db,
                &e_cp,
                &later,
                &[],
                true,
                "rows before F, halt",
            )
            .await;
            assert!(refused[0].starts_with("error"), "{refused:?}");
        }
    }
}
