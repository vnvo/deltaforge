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
const FORMAT_VERSION: u32 = 2;

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
    pub dsn: &'a str,
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

/// The table's shape proven at F, or `None` (unprovable).
async fn shape_at_failover(
    env: &DriftEnv<'_>,
    anchor: &FailoverAnchor,
    db: &str,
    table: &str,
    event: &FirstEvent<'_>,
) -> SourceResult<Option<MySqlTableSchema>> {
    let Some(f_cp) = anchor.position.clone() else {
        return Ok(None);
    };
    let Some(f) = mysql_checkpoint_position(
        &f_cp.file,
        f_cp.pos,
        f_cp.gtid_set.as_deref(),
    ) else {
        return Ok(None);
    };
    let from = match event {
        FirstEvent::Ddl => return Ok(None),
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
                return Ok(None);
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
                    ) => return Ok(None),
                    _ => return Ok(None),
                }
            }
            (*at_cp).clone()
        }
    };
    let mut from = from;
    from.lineage = Some(anchor.current_lineage.clone());
    let tag = ScanTag {
        source_id: env.source_id,
        kind: ProofKind::FailoverDrift,
    };
    let lctn = env.lower_case_table_names;
    let cap = match capture(
        env.dsn,
        env.server_uuid,
        &anchor.current_lineage,
        db,
        table,
        None,
    )
    .await
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
    let scanned = match (env.proofs, event) {
        // Rows in the stream: the shared scanner (the rows' position is the
        // stream's boundary).
        (Some(proofs), FirstEvent::Rows { .. }) => {
            super::mysql_proof_scanner::shared_interval(
                proofs,
                env.dsn,
                env.source_id,
                env.server_uuid,
                &anchor.current_lineage,
                &from,
                &cap.position,
                Some(&from),
                &tag,
            )
            .await
        }
        _ => {
            scan_interval(
                env.dsn,
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
    if report
        .statements
        .iter()
        .any(|st| affects(st, db, table, env.lower_case_table_names))
    {
        info!(source_id = env.source_id, %db, %table, "failover drift: a DDL or barrier in the interval");
        trace_proof(
            &tag,
            db,
            table,
            Some(&cap),
            Some(&report),
            lctn,
            "relevant_ddl",
        );
        return Ok(None);
    }
    trace_proof(&tag, db, table, Some(&cap), Some(&report), lctn, "proven");
    Ok(Some(cap.schema))
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
    let key = env.scope.key(db, table);
    let previous_key = SchemaKey::new(
        &key.tenant,
        &key.source_id,
        &anchor.previous_lineage,
        db,
        table,
    );
    if completed(env, &key, &previous_key, anchor).await? {
        return Ok(());
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
        return write_marker(
            env.backend,
            &key,
            anchor,
            None,
            None,
            Outcome::NewTable,
        )
        .await;
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

    let Some(shape) = shape_at_failover(env, anchor, db, table, &event).await?
    else {
        if env.halt {
            return Err(halt_error(
                env.source_id,
                db,
                table,
                "its schema at the failover position cannot be proven",
            ));
        }
        warn!(source_id = env.source_id, %db, %table, "failover drift unprovable: adapted");
        return write_marker(
            env.backend,
            &key,
            anchor,
            Some((previous.version, previous.hash.clone())),
            None,
            Outcome::AdaptedUnprovable,
        )
        .await;
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
    let checkpoint = serde_json::to_vec(&anchor.position)
        .map_err(|e| SourceError::Other(e.into()))?;
    let (current_version, current_hash) = env
        .loader
        .register_captured(db, table, &shape, &checkpoint)
        .await?;
    if drift {
        warn!(source_id = env.source_id, %db, %table, changes = deltas.len(), "failover drift adapted");
    }
    write_marker(
        env.backend,
        &key,
        anchor,
        Some((previous.version, previous.hash.clone())),
        Some((current_version, current_hash)),
        if drift {
            Outcome::Adapted
        } else {
            Outcome::Unchanged
        },
    )
    .await
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
            dsn: ctx.dsn.expose(),
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
            dsn: "",
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
                    dsn: "",
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
}
