//! PostgreSQL CDC source implementation using logical replication.

use std::{
    collections::HashMap,
    sync::{Arc, atomic::AtomicBool},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use deltaforge_config::{OnSchemaDrift, SnapshotMode};
use pgwire_replication::{Lsn, ReplicationClient};
use serde::{Deserialize, Serialize};
use storage::{ArcStorageBackend, DurableSchemaRegistry};
use tokio::sync::{Mutex, Notify, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use checkpoints::{CheckpointStore, CheckpointStoreExt};
use common::{AllowList, RetryPolicy, pause_until_resumed};
use deltaforge_core::incident::{
    ActionCode, CauseCode, Component, EvidenceKey as K, IncidentDraft,
    ReasonCode, Retryability, SafetyState,
};
use deltaforge_core::{
    CheckpointOrder, Source, SourceError, SourceHandle, SourceItem,
    SourceResult,
};
use storage::BackendCheckpointStore;
use storage::adapters::incidents::IncidentStore;

use crate::snapshot_generation::PersistedLineage;
use postgres_snapshot::IdentitySpec;

mod postgres_errors;
use postgres_errors::LoopControl;
pub use postgres_errors::{PostgresSourceError, PostgresSourceResult};

mod postgres_checkpoint_chain;
mod postgres_continuity;
mod postgres_helpers;
use postgres_helpers::{
    connect_replication_with_retries, ensure_publication_exists,
    ensure_slot_and_publication, prepare_replication_client,
};

pub mod postgres_slot_owner;

pub mod postgres_rotation;
use postgres_slot_owner::prepare_snapshot_slot_anchor;

pub mod postgres_object;

mod postgres_schema_loader;
pub use postgres_schema_loader::{LoadedSchema, PostgresSchemaLoader};

mod postgres_event;
pub use postgres_event::RelationInfo;

pub mod postgres_event_id;
use postgres_event::*;
pub use postgres_event_id::pg_row_event_id;

pub mod postgres_identity;
pub use postgres_identity::{
    PgIdentityError, PgIdentityRaw, pg_identity_cell, pg_identity_kind,
    quote_ident, resolve_identity_kinds,
};

pub mod postgres_table_schema;
pub use postgres_table_schema::{PostgresColumn, PostgresTableSchema};

mod postgres_logical_message;

pub mod postgres_snapshot;
pub use postgres_snapshot::SnapshotProgress;

pub mod postgres_health;

use crate::failover::identity::{
    IdentityComparison, IdentityStore, ServerIdentity,
};
use crate::postgres::postgres_health::{
    PositionReachability, PostgresServerIdentity, check_position_reachability,
};
use crate::registry_scope::{
    RegistryError, ScopeChange, SharedRegistryScope, establish_scope,
};
use storage::adapters::LineageDescriptor;

// ============================================================================
// Checkpoint
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresCheckpoint {
    pub lsn: String,
    pub tx_id: Option<u32>,
    /// The continuity stamp of the stream the position was read on: timeline,
    /// chain id and transition within the chain. All absent in checkpoints
    /// written before continuity was recorded; never partially present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeline: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chain: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transition: Option<u64>,
}

/// A resume position read from the checkpoint store.
#[derive(Debug, Clone)]
pub(crate) enum PgResumePosition {
    /// A snapshot no sink acknowledged as complete (an incomplete-snapshot
    /// position, or a pre-format snapshot checkpoint: the anchor LSN as
    /// text). It is not a stream position: the snapshot runs again. `anchor`
    /// is where that snapshot's stream started.
    Snapshot { anchor: Lsn },
    /// A stream position (CDC, or the completing snapshot boundary at its
    /// anchor).
    Stream(PostgresCheckpoint),
}

/// Classify stored checkpoint bytes; anything else fails closed.
pub(crate) fn classify_pg_checkpoint(
    raw: &[u8],
) -> Result<PgResumePosition, String> {
    let anchor = |s: &str| {
        Lsn::parse(s).map_err(|e| format!("invalid snapshot anchor '{s}': {e}"))
    };
    if let Some((_, a)) = crate::snapshot_position::decode::<String>(raw)? {
        return Ok(PgResumePosition::Snapshot {
            anchor: anchor(&a)?,
        });
    }
    if let Ok(cp) = serde_json::from_slice::<PostgresCheckpoint>(raw) {
        return Ok(PgResumePosition::Stream(cp));
    }
    match std::str::from_utf8(raw) {
        Ok(text)
            if text.contains('/') && !text.trim_start().starts_with('{') =>
        {
            Ok(PgResumePosition::Snapshot {
                anchor: anchor(text)?,
            })
        }
        _ => Err("unrecognised checkpoint".to_string()),
    }
}

// ============================================================================
// Source Configuration
// ============================================================================

#[derive(Debug, Clone)]
pub struct PostgresSource {
    pub id: String,
    /// Protected connection DSN (redacted `Debug`, no serialization). Built at
    /// startup from inline config or resolved secret references.
    pub dsn: crate::credentials::ProtectedDsn,
    pub slot: String,
    pub publication: String,
    pub tables: Vec<String>,
    pub tenant: String,
    pub pipeline: String,
    pub registry: Arc<DurableSchemaRegistry>,
    /// Verified-lineage scope shared with the pipeline's schema loaders. The
    /// source establishes it before any registry access.
    pub registry_scope: crate::registry_scope::SharedRegistryScope,
    pub backend: ArcStorageBackend,
    pub outbox_prefixes: AllowList,
    pub snapshot_cfg: deltaforge_config::SnapshotCfg,
    pub on_schema_drift: OnSchemaDrift,
    /// Per-table options (identity_columns, assume_unique), keyed by
    /// fully-qualified `schema.table`.
    pub table_options:
        std::collections::BTreeMap<String, deltaforge_config::TableOptions>,
    /// Controlled credential-rotation spec, when configured and file-backed.
    /// `None` disables rotation for this source.
    pub rotation: Option<Arc<crate::rotation_manager::RotationSpec>>,
}

// ============================================================================
// Runtime Context
// ============================================================================

pub(crate) struct RunCtx {
    pub source_id: String,
    pub pipeline: String,
    pub tenant: String,
    #[allow(dead_code)]
    pub host: String,
    pub default_schema: String,
    pub dsn: crate::credentials::ProtectedDsn,
    pub tx: mpsc::Sender<SourceItem>,
    #[allow(dead_code)]
    pub chkpt: Arc<dyn CheckpointStore>,
    pub cancel: CancellationToken,
    pub paused: Arc<AtomicBool>,
    pub pause_notify: Arc<Notify>,
    pub schema: PostgresSchemaLoader,
    pub allow: AllowList,
    pub retry: RetryPolicy,
    pub inactivity: Duration,
    pub relation_map: HashMap<u32, RelationInfo>,
    pub last_lsn: Lsn,
    /// Where an in-process reconnect resumes: the end of the last transaction
    /// whose commit was handed to the coordinator (or the start position).
    /// Never the read position, which keepalives and in-transaction messages
    /// move: a transaction cut off mid-stream must be decoded again in full.
    pub resume_lsn: Lsn,
    /// Resolves this source's schema-drift incidents as tables are accepted.
    pub drift_resolver: crate::incident_drafts::DriftResolver,
    /// Events handed to the coordinator for the open transaction.
    pub open_tx_events: u64,
    pub current_tx_id: Option<u32>,
    pub current_tx_commit_time: Option<i64>,
    /// The current transaction's final LSN (from `BEGIN`) - the stable identity
    /// coordinate for row/message changes in this transaction. `None` outside a
    /// transaction.
    pub current_final_lsn: Option<String>,
    /// Per-transaction change ordinal: reset at `BEGIN`, incremented for every
    /// identity-bearing change (insert/update/delete/truncate/transactional
    /// message) before filtering.
    pub change_ordinal: u32,
    /// Cluster `system_identifier` (per-connection lineage) - captured once at
    /// startup and mixed into provisional row/DDL/message ids. `0` if the
    /// catalog function was unavailable (provisional ids are then skipped).
    pub system_identifier: u64,
    /// Logical-message ordinal: reset at every transaction boundary (BEGIN and
    /// COMMIT), incremented **before filtering** for each logical message so a
    /// filtered message never renumbers a retained one.
    pub message_ordinal: u32,
    pub repl_client: Arc<Mutex<ReplicationClient>>,
    pub outbox_prefixes: AllowList,
    pub identity_store: IdentityStore,
    /// Verified-lineage registry scope (shared with the pipeline's loaders).
    pub registry_scope: SharedRegistryScope,
    /// Backend holding the durable source-lineage record.
    pub registry_backend: ArcStorageBackend,
    /// Cached metrics counter handles keyed by (qualified_table_name, op).
    /// Avoids hash-lookup + key-comparison in the metrics registry per event.
    pub counter_cache: HashMap<(Arc<str>, &'static str), metrics::Counter>,
    /// Cached LSN string to avoid re-formatting the same LSN on consecutive events.
    pub cached_lsn: Option<(Lsn, String)>,
    /// The continuity stamp of the authoritative stream, set only when a
    /// stream is activated (`StreamProof::activate`).
    pub stamp: Arc<std::sync::RwLock<Option<postgres_continuity::Stamp>>>,
    /// That stamp as checkpoint JSON members, refreshed whenever another
    /// stream becomes authoritative (before its first event is read).
    pub stamp_members: Arc<str>,
}

impl RunCtx {
    /// Take the stamp of the stream that just became authoritative.
    pub(crate) fn refresh_stamp(&mut self) {
        self.stamp_members = self
            .stamp
            .read()
            .expect("not poisoned")
            .as_ref()
            .map_or(Arc::from(""), |s| Arc::from(s.checkpoint_members()));
    }

    fn active_stamp(&self) -> Option<postgres_continuity::Stamp> {
        self.stamp.read().expect("not poisoned").clone()
    }
}

// ============================================================================
// Source Implementation
// ============================================================================

const MAX_STARTUP_BACKOFF_SECS: u64 = 60;

/// A table of the snapshot plan: its identity columns with their kinds.
pub type PgPlannedTable = crate::snapshot_plan::PlannedTable<Vec<IdentitySpec>>;

/// Outcome of the pre-snapshot discover/prepare/allocate flow.
struct SnapshotPlan {
    generation: u64,
    lineage: PersistedLineage,
    /// Every table to copy, in discovery order.
    tables: Vec<PgPlannedTable>,
}

impl PostgresSource {
    /// Discover the tables to copy (keyset pages of the catalog, filtered
    /// by the CDC matcher), prepare each exactly once - one schema
    /// resolution, identity resolution and type validation, cursor kind -
    /// into a compact plan entry and the streaming generation fingerprint,
    /// freeze the cluster lineage and atomically allocate (or resume) the
    /// snapshot generation - all **before** any row is emitted. Keyless
    /// tables or unsupported identity types fail here, before allocation.
    async fn prepare_snapshot(
        &self,
        loader: &PostgresSchemaLoader,
    ) -> SourceResult<SnapshotPlan> {
        use crate::identity_resolution::{
            IdentitySchemaView, resolve_identity,
        };
        use crate::snapshot_generation::{
            AllocationMode, SnapshotFingerprintBuilder, allocate_generation,
        };
        use tokio_postgres::NoTls;

        let started = std::time::Instant::now();
        let fetches_before = loader.live_fetch_count();
        // One catalog session: discovery, identity kinds and lineage, all in
        // one repeatable-read transaction, so every discovery page (and every
        // catalog read of the preparation) sees the same catalog snapshot:
        // a concurrent CREATE, DROP or RENAME cannot change the table set
        // between pages. Discovery runs after the slot anchor, so a table
        // created after this snapshot is the CDC stream's.
        let (client, conn) = tokio_postgres::connect(self.dsn.expose(), NoTls)
            .await
            .map_err(|e| SourceError::Other(e.into()))?;
        let conn_task = tokio::spawn(async move {
            let _ = conn.await;
        });
        client
            .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await
            .map_err(|e| SourceError::Other(e.into()))?;
        crate::snapshot_probe::record_fixed(
            crate::snapshot_probe::FixedOp::CatalogSession,
        );

        let mut discovery = crate::snapshot_discovery::Discovery::new(
            &self.tables,
            self.snapshot_cfg.discovery_page_size,
        );
        let mut fingerprint =
            SnapshotFingerprintBuilder::new("postgres", &self.tables);
        let mut tables: Vec<PgPlannedTable> = Vec::new();
        while !discovery.is_done() {
            let t = std::time::Instant::now();
            let rows = postgres_schema_loader::discovery_page(
                &client,
                &self.tables,
                discovery.after(),
                discovery.page_size(),
            )
            .await?;
            crate::snapshot_probe::record_discovery_time(t.elapsed());
            let page = discovery.accept(rows)?;
            crate::snapshot_probe::after_discovery_page().await;
            for (schema, table) in page {
                // On the preparation session: the schema is the one this
                // catalog snapshot shows, like the table set and the
                // identity kinds below.
                let loaded =
                    loader.load_schema_on(&client, &schema, &table).await?;
                let s = &loaded.schema;
                let col_names: Vec<String> =
                    s.columns.iter().map(|c| c.name.clone()).collect();
                let fqn = format!("{schema}.{table}");
                let opts = self.table_options.get(&fqn);
                let view = IdentitySchemaView {
                    columns: &col_names,
                    primary_key: &s.primary_key,
                    // Unique constraints are not captured in the schema
                    // today; non-PK identity columns require assume_unique.
                    unique_constraints: &[],
                };
                let resolved = resolve_identity(
                    &schema,
                    &table,
                    &view,
                    opts.and_then(|o| o.identity_columns.as_deref()),
                    opts.map(|o| o.assume_unique).unwrap_or(false),
                )
                .map_err(|e| SourceError::Other(anyhow::anyhow!(e)))?;

                // Resolve + validate the identity column types from the
                // catalog (rejects unsupported types before allocation).
                let kinds = resolve_identity_kinds(
                    &client,
                    &schema,
                    &table,
                    &resolved.columns,
                )
                .await
                .map_err(SourceError::Other)?;
                let cursor_kind = postgres_snapshot::pg_cursor_kind(s);
                fingerprint.table(
                    &schema,
                    Some(&schema),
                    &table,
                    &resolved.columns,
                    cursor_kind,
                    loaded.registry_version,
                );
                crate::snapshot_probe::record_prepared_table();
                tables.push(PgPlannedTable {
                    qualifier: schema,
                    table,
                    identity: kinds
                        .into_iter()
                        .map(|(name, kind)| IdentitySpec { name, kind })
                        .collect(),
                    cursor_kind,
                    signature: postgres_snapshot::pg_schema_signature(s),
                });
            }
        }

        // Freeze lineage from the existing system_identifier authority.
        let lineage = self.capture_snapshot_lineage(&client).await?;
        crate::snapshot_probe::record_fixed(
            crate::snapshot_probe::FixedOp::LineageCapture,
        );
        client.batch_execute("COMMIT").await.ok();
        conn_task.abort();

        let fingerprint = fingerprint.finish();
        let mode = if self.snapshot_cfg.mode == SnapshotMode::Always {
            AllocationMode::ForceNew
        } else {
            AllocationMode::Resume
        };
        let store = BackendCheckpointStore::new(self.backend.clone());
        let key = format!("snapshot_generation:{}", self.id);
        let alloc =
            allocate_generation(&store, &key, lineage, &fingerprint, mode)
                .await
                .map_err(|e| SourceError::Other(anyhow::anyhow!(e)))?;
        crate::snapshot_probe::record_fixed(
            crate::snapshot_probe::FixedOp::GenerationAllocation,
        );
        crate::snapshot_probe::record_preparation(
            started.elapsed(),
            crate::snapshot_plan::plan_bytes(&tables, |ids| {
                ids.iter()
                    .map(|i| std::mem::size_of::<IdentitySpec>() + i.name.len())
                    .sum()
            }),
            loader.live_fetch_count() - fetches_before,
        );

        info!(
            source_id = %self.id,
            generation = alloc.record.generation,
            tables = tables.len(),
            "snapshot generation allocated"
        );
        Ok(SnapshotPlan {
            generation: alloc.record.generation,
            lineage: alloc.record.lineage,
            tables,
        })
    }

    /// Whether unfinished legacy snapshot progress is ambiguous: among the
    /// tables the configured patterns expand to now (paged discovery, the
    /// same canonical `schema.table` identities the snapshot records), some
    /// are done and some pending.
    async fn legacy_progress_is_ambiguous(
        &self,
        progress: &postgres_snapshot::SnapshotProgress,
    ) -> SourceResult<bool> {
        let (client, conn) =
            tokio_postgres::connect(self.dsn.expose(), tokio_postgres::NoTls)
                .await
                .map_err(|e| SourceError::Connect {
                    details: format!(
                        "discover tables to check snapshot progress: {e}"
                    )
                    .into(),
                })?;
        let conn_task = tokio::spawn(async move {
            let _ = conn.await;
        });
        // One catalog snapshot for every page.
        client
            .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await
            .map_err(|e| SourceError::Other(e.into()))?;
        let mut discovery = crate::snapshot_discovery::Discovery::new(
            &self.tables,
            self.snapshot_cfg.discovery_page_size,
        );
        let mut scan = crate::snapshot_frontier::LegacyProgressScan::default();
        while !discovery.is_done() && !scan.settled() {
            let rows = postgres_schema_loader::discovery_page(
                &client,
                &self.tables,
                discovery.after(),
                discovery.page_size(),
            )
            .await?;
            for (schema, table) in discovery.accept(rows)? {
                scan.observe(progress.table_done(&schema, &table));
            }
        }
        conn_task.abort();
        Ok(scan.ambiguous(progress.finished))
    }

    /// Freeze the PostgreSQL cluster lineage from `system_identifier` - the same
    /// authority used by the failover identity path (`pg_control_system()`, no
    /// superuser). Fails with an actionable error if it is unavailable.
    async fn capture_snapshot_lineage(
        &self,
        client: &tokio_postgres::Client,
    ) -> SourceResult<PersistedLineage> {
        let row = client
            .query_opt("SELECT system_identifier FROM pg_control_system()", &[])
            .await
            .map_err(|e| {
                SourceError::Other(anyhow::anyhow!(
                    "reading system_identifier (pg_control_system): {e}; the \
                     CDC role needs EXECUTE on pg_control_system()"
                ))
            })?;
        let sysid: i64 = row
            .ok_or_else(|| {
                SourceError::Other(anyhow::anyhow!(
                    "system_identifier unavailable; cannot mint stable \
                     snapshot ids"
                ))
            })?
            .get(0);
        Ok(PersistedLineage::Postgres {
            system_identifier: sysid as u64,
        })
    }

    async fn run_inner(
        &self,
        tx: mpsc::Sender<SourceItem>,
        chkpt_store: Arc<dyn CheckpointStore>,
        cancel: CancellationToken,
        paused: Arc<AtomicBool>,
        pause_notify: Arc<Notify>,
        ready: deltaforge_core::SourceReady,
    ) -> SourceResult<()> {
        // Verify the physical lineage (cluster system_identifier + database OID)
        // and establish the schema-registry scope BEFORE any registry access:
        // persist it durably (fail closed), then publish it to this pipeline's
        // loaders. A replaced database therefore gets a fresh schema namespace
        // instead of inheriting schemas through a reused source id.
        establish_registry_scope(
            self.dsn.expose(),
            &self.backend,
            &self.registry_scope,
            &self.tenant,
            &self.id,
        )
        .await?;

        let (components, config, last_checkpoint, incomplete_snapshot) =
            prepare_replication_client(
                self.dsn.expose(),
                &self.id,
                &self.slot,
                &self.publication,
                &chkpt_store,
            )
            .await?;

        let mut startup_retry = RetryPolicy::default();

        // A snapshot whose completion no sink acknowledged (its boundaries
        // are incomplete-snapshot positions) runs again in full; it is never
        // resumed as a stream, which would skip its undelivered rows.
        if incomplete_snapshot && self.snapshot_cfg.mode == SnapshotMode::Never
        {
            return Err(SourceError::Checkpoint {
                details: format!(
                    "source {}: the resume checkpoint belongs to a snapshot \
                     that never completed, and snapshot mode 'never' cannot \
                     complete it; set mode 'initial' to snapshot again",
                    self.id
                )
                .into(),
            });
        }
        if incomplete_snapshot {
            warn!(
                source_id = %self.id,
                "the snapshot was not acknowledged as complete; it runs again \
                 in full"
            );
        }
        let needs_snapshot = match self.snapshot_cfg.mode {
            SnapshotMode::Initial => last_checkpoint.is_none(),
            SnapshotMode::Always => true,
            SnapshotMode::Never => false,
        };

        let start_lsn = if last_checkpoint.is_none() {
            loop {
                let ensure_result = if needs_snapshot {
                    // The slot anchor is established by
                    // prepare_snapshot_slot_anchor (below) at the slot's
                    // consistent point; here we only verify the publication.
                    ensure_publication_exists(
                        self.dsn.expose(),
                        &self.publication,
                        &self.tables,
                    )
                    .await
                    .map(|_| Lsn::from(0u64))
                } else {
                    ensure_slot_and_publication(
                        self.dsn.expose(),
                        &self.slot,
                        &self.publication,
                        &self.tables,
                    )
                    .await
                };
                match ensure_result {
                    Ok(lsn) => break lsn,
                    Err(LoopControl::Reconnect) => {
                        let delay = startup_retry
                            .next_backoff()
                            .min(Duration::from_secs(MAX_STARTUP_BACKOFF_SECS));
                        warn!(
                            source_id = %self.id,
                            delay_secs = delay.as_secs(),
                            "startup check failed, retrying after backoff"
                        );
                        tokio::select! {
                            _ = tokio::time::sleep(delay) => {}
                            _ = cancel.cancelled() => {
                                info!(source_id = %self.id, "cancelled during startup retry");
                                return Err(SourceError::Cancelled);
                            }
                        }
                    }
                    Err(LoopControl::Stop) => {
                        info!(source_id = %self.id, "stop requested during startup");
                        return Err(SourceError::Cancelled);
                    }
                    Err(LoopControl::Fail(e)) => {
                        error!(source_id = %self.id, error = %e, "fatal error during startup");
                        return Err(e);
                    }
                    Err(LoopControl::ReloadSchema { .. }) => continue,
                    // The ensure/startup path does not decode Relation messages,
                    // so drift cannot originate here; reload-and-retry defensively.
                    Err(LoopControl::SchemaDrift(_)) => continue,
                }
            }
        } else {
            // Resuming from a checkpoint: the saved LSN is trusted only once
            // the slot is verified to hold it. Fails closed: replication never
            // opens while that is unknown; a plausibly transient cause is
            // retried (cancellably, for a bounded time), anything else stops.
            verify_resume_position(
                self.dsn.expose(),
                &self.id,
                &self.slot,
                &config.start_lsn.to_string(),
                &cancel,
                REACHABILITY_RETRY_WINDOW,
                &IncidentStore::new(Arc::clone(&self.backend), &self.pipeline),
            )
            .await?;
            config.start_lsn
        };

        let schema_loader = PostgresSchemaLoader::new(
            self.dsn.clone(),
            self.registry.clone(),
            &self.tenant,
            self.registry_scope.clone(),
        );
        // No schema is enumerated or loaded at a CDC start: each table is
        // resolved at its first Relation (design spec 7.23). Only a snapshot
        // discovers the tables it copies (paged, in `prepare_snapshot`); the
        // per-Relation replica-identity warning covers what the startup loop
        // used to report.

        let start_lsn = if needs_snapshot {
            info!(source_id = %self.id, "starting initial snapshot");

            // Establish the CDC anchor at the replication slot's consistent point
            // (PG-A-lite): create the slot, or safely re-anchor an owned inactive
            // slot (full re-snapshot), or fail closed. This replaces the removed
            // pg_current_wal_lsn anchor and closes the snapshot->CDC seam.
            let anchor = prepare_snapshot_slot_anchor(
                self.dsn.expose(),
                &self.slot,
                &self.pipeline,
                &self.id,
                &chkpt_store,
            )
            .await?;

            // Every snapshot scans every table: reset table-level progress so
            // no completed table (or a stale `finished`) is skipped - nothing
            // in it was acknowledged by the sinks (generation is separately
            // bumped via ForceNew for mode 'always'). Fail closed if the reset
            // does not persist - snapshotting on stale progress would skip
            // tables and reintroduce loss.
            postgres_slot_owner::reset_snapshot_progress(
                &chkpt_store,
                &self.id,
            )
            .await
            .map_err(SourceError::Other)?;

            // Discover + validate every table, freeze lineage, allocate the
            // generation BEFORE emitting any row (keyless/unsupported tables
            // fail here).
            let plan = self.prepare_snapshot(&schema_loader).await?;

            let snapshot_ctx = postgres_snapshot::PgSnapshotCtx {
                dsn: self.dsn.expose(),
                source_id: &self.id,
                pipeline: &self.pipeline,
                tenant: &self.tenant,
                cfg: &self.snapshot_cfg,
                schema_loader: &schema_loader,
                chkpt_store: chkpt_store.clone(),
                tx: tx.clone(),
                cancel: cancel.clone(),
                slot_name: Some(&self.slot),
                generation: plan.generation,
                lineage: plan.lineage,
            };
            postgres_snapshot::run_snapshot(&snapshot_ctx, &plan.tables, anchor)
                .await
                .map_err(SourceError::Other)?
        } else {
            start_lsn
        };

        // Flag a completed snapshot taken under the legacy (pre-hardening) anchor,
        // which could have lost rows committed during snapshot setup. The gauge is
        // held at 1 with a structured startup warning until a safe-anchor
        // re-snapshot records the current anchor version, then it reads 0.
        {
            let progress: SnapshotProgress = chkpt_store
                .get_raw(&postgres_snapshot::progress_key(&self.id))
                .await
                .ok()
                .flatten()
                .and_then(|b| serde_json::from_slice(&b).ok())
                .unwrap_or_default();
            let unsafe_legacy = progress.finished
                && progress.anchor_version
                    < postgres_snapshot::SNAPSHOT_ANCHOR_VERSION;
            if unsafe_legacy {
                warn!(
                    source_id = %self.id,
                    pipeline = %self.pipeline,
                    "initial snapshot was taken under the legacy anchor that could \
                     lose rows committed during snapshot setup; re-snapshot \
                     (snapshot mode 'always' once, or drop the checkpoint) to \
                     record the safe anchor and clear deltaforge_snapshot_unsafe_anchor"
                );
            }
            metrics::gauge!(
                "deltaforge_snapshot_unsafe_anchor",
                "pipeline" => self.pipeline.clone(),
                "source" => self.id.clone(),
            )
            .set(if unsafe_legacy { 1.0 } else { 0.0 });
        }

        // Re-verify the lineage immediately before opening replication (a
        // snapshot may have run since the startup read, and the server may have
        // changed meanwhile): another cluster is refused before
        // START_REPLICATION, and a first-seen identity is persisted first. The
        // verified read also gives the provisional row/DDL/message ids. If the
        // lineage cannot be verified we fail closed rather than open
        // replication on an unknown server.
        let stream_lineage =
            fetch_pg_lineage_verified(self.dsn.expose()).await?;
        refuse_server_change(
            &self.backend,
            &self.tenant,
            &self.id,
            &stream_lineage,
        )
        .await?;
        let system_identifier =
            stream_lineage.identity.system_identifier as u64;
        verify_identity_before_connect(
            &ServerIdentity::from(stream_lineage.identity.clone()),
            &IdentityStore::new(Arc::clone(&self.backend)),
            &self.id,
        )
        .await?;

        info!(
            source_id = %self.id, host = %components.host, slot = %self.slot,
            publication = %self.publication, start_lsn = %start_lsn,
            "postgres source starting"
        );

        let config = config.with_start_lsn(start_lsn);
        // The safety boundary of every stream this run opens: continuity of
        // the durable checkpoint, proven on each stream's own session before
        // its START_REPLICATION.
        let stream_proof = postgres_helpers::StreamProof {
            dsn: self.dsn.clone(),
            slot: self.slot.clone(),
            source_id: self.id.clone(),
            pipeline: self.pipeline.clone(),
            tenant: self.tenant.clone(),
            checkpoints: chkpt_store.clone(),
            backend: Arc::clone(&self.backend),
            active: Default::default(),
            recovery_window: REACHABILITY_RETRY_WINDOW,
            recovery_retry: Default::default(),
        };

        let client = connect_replication_with_retries(
            &self.id,
            config.clone(),
            &stream_proof,
            true,
            &cancel,
            RetryPolicy::default(),
        )
        .await?;

        let backend = Arc::clone(&self.backend);
        let mut ctx = RunCtx {
            source_id: self.id.clone(),
            pipeline: self.pipeline.clone(),
            tenant: self.tenant.clone(),
            host: components.host.clone(),
            default_schema: "public".to_string(),
            dsn: self.dsn.clone(),
            tx,
            chkpt: chkpt_store.clone(),
            cancel,
            paused,
            pause_notify,
            schema: schema_loader,
            allow: AllowList::new(&self.tables),
            retry: RetryPolicy::default(),
            inactivity: Duration::from_secs(60),
            relation_map: HashMap::new(),
            last_lsn: start_lsn,
            resume_lsn: start_lsn,
            drift_resolver: crate::incident_drafts::DriftResolver::new(
                Arc::clone(&self.backend),
                &self.pipeline,
                &self.id,
            ),
            open_tx_events: 0,
            current_tx_id: None,
            current_tx_commit_time: None,
            current_final_lsn: None,
            change_ordinal: 0,
            system_identifier,
            message_ordinal: 0,
            repl_client: Arc::new(Mutex::new(client)),
            outbox_prefixes: self.outbox_prefixes.clone(),
            identity_store: IdentityStore::new(Arc::clone(&backend)),
            registry_scope: self.registry_scope.clone(),
            registry_backend: Arc::clone(&backend),
            counter_cache: HashMap::new(),
            cached_lsn: None,
            stamp: Arc::clone(&stream_proof.active),
            stamp_members: Arc::from(""),
        };
        ctx.refresh_stamp();

        // The server may have changed between the check above and the open:
        // verify again before the first message is read.
        check_identity_post_reconnect(&mut ctx).await?;

        // Controlled credential rotation (opt-in, file-backed credentials only).
        // The runtime owns the watcher/manager task (a child of the source cancel
        // token); it is cancelled and joined after the loop on every exit path
        // (`shutdown` below), with `Drop` as the abort backstop for panics.
        let mut rotation = match &self.rotation {
            Some(spec) => Some(postgres_rotation::RotationRuntime::spawn(
                spec,
                ctx.dsn.clone(),
                self.id.clone(),
                self.pipeline.clone(),
                self.slot.clone(),
                self.publication.clone(),
                chkpt_store.clone(),
                stream_proof.clone(),
                &ctx.cancel,
            )?),
            None => None,
        };

        // Startup checks passed and the stream is open on the verified
        // server: the source half of the verified-running barrier.
        ready.mark();
        info!("entering replication loop");
        // Durable delivery frontier (separate from `ctx.last_lsn`, the read position):
        // the acknowledged LSN reported to PostgreSQL as flushed, sourced only from the
        // per-sink checkpoint store. Refreshed change-driven via
        // `CheckpointStore::await_checkpoint_change` (the coordinator signals after each
        // per-sink commit; a long fallback re-confirms during idle) so idle sources do
        // not poll the control store. Idle keepalives are emitted by the replication
        // worker, carrying whatever frontier is set.
        let mut wal_feedback = WalFeedback::new();
        // The loop runs inside an async block so its result can be captured and the
        // rotation runtime cancelled+joined before teardown, on both the normal and
        // fatal exit paths. A fatal result (including gate-6 rotation failures) is
        // re-propagated after join and before the teardown checkpoint put.
        let loop_result: SourceResult<()> = async {
        loop {
            if !pause_until_resumed(&ctx.cancel, &ctx.paused, &ctx.pause_notify)
                .await
            {
                break;
            }

            // Rotation: schedule Stage-A preflight concurrently with the stream,
            // and apply the Stage-B swap only at a whole-transaction boundary.
            if let Some(rt) = rotation.as_mut() {
                rt.drive_preflight(&ctx);
                if ctx.current_tx_id.is_none() {
                    // A CloseUncertain/FailedClosed outcome returns an error from
                    // this block (gate 6). After join, `loop_result?` re-propagates
                    // it before the teardown checkpoint put, so the checkpoint never
                    // advances past the last committed transaction.
                    rt.apply_at_boundary(&mut ctx).await?;
                    // The replacement (or the restored old stream) is the
                    // authoritative one now, before its first event.
                    ctx.refresh_stamp();
                }
            }

            // Report the durable delivery frontier to PostgreSQL before blocking on the
            // next read. Runs after every processed event and on each idle ticker wake,
            // so WAL is released only up to what the sinks have durably acknowledged.
            // Fails closed (stops the source before the teardown put) on a malformed
            // checkpoint or prolonged checkpoint-store unavailability.
            advance_wal_feedback(&ctx, &chkpt_store, &mut wal_feedback).await?;

            debug!(source_id = %self.id, "reading next event");

            // The read future is dropped when the rotation or feedback arm fires; both
            // are cancellation-safe (the mpsc receiver keeps any buffered event and the
            // client lock is released), so the next iteration simply reads again.
            let event_result = match rotation.as_mut() {
                // Idle-source wakeup: race the read against rotation activity so a
                // rotation applies even when no events are flowing.
                Some(rt) => {
                    tokio::select! {
                        r = read_next_event(&ctx) => match r {
                            Ok(Some(event)) => {
                                dispatch_event(&mut ctx, event).await
                            }
                            Ok(None) => {
                                info!(source_id = %self.id, "replication stream ended");
                                break;
                            }
                            Err(ctrl) => Err(ctrl),
                        },
                        _ = rt.wait_activity() => continue,
                        _ = chkpt_store.await_checkpoint_change() => continue,
                    }
                }
                None => {
                    tokio::select! {
                        r = read_next_event(&ctx) => match r {
                            Ok(Some(event)) => {
                                dispatch_event(&mut ctx, event).await
                            }
                            Ok(None) => {
                                info!(source_id = %self.id, "replication stream ended");
                                break;
                            }
                            Err(ctrl) => Err(ctrl),
                        },
                        _ = chkpt_store.await_checkpoint_change() => continue,
                    }
                }
            };

            match event_result {
                Ok(()) => {}
                Err(LoopControl::ReloadSchema { schema, table }) => {
                    // Fail closed if the reload fails: continuing would decode the
                    // rows that triggered the reload against a stale cached schema.
                    // (MySQL already propagates here; this matches it.)
                    if let (Some(s), Some(t)) = (schema, table) {
                        info!(schema = %s, table = %t, "reloading schema");
                        ctx.schema.reload_schema(&s, &t).await?;
                    } else {
                        // Never enumerates: forget every cached schema; each
                        // table reloads at its next use.
                        info!("clearing cached schemas");
                        ctx.schema.clear_cache().await;
                    }
                }
                Err(LoopControl::SchemaDrift(drift)) => {
                    // Apply the policy, failing closed on a reload error under
                    // Adapt or on Halt. The drift Relation precedes the
                    // transaction's rows, so failing here leaves the open
                    // transaction uncommitted (the coordinator discards it, no
                    // sink checkpoint advances past the last committed pre-drift
                    // transaction) and skips the graceful-stop checkpoint put.
                    apply_schema_drift(
                        &ctx.schema,
                        &self.on_schema_drift,
                        &drift,
                        &self.id,
                    )
                    .await?;
                    // Adapted: the table's changed schema is accepted.
                    ctx.drift_resolver
                        .accepted(&format!("{}.{}", drift.schema, drift.table))
                        .await;
                }
                Err(LoopControl::Reconnect) => {
                    let delay = ctx.retry.next_backoff();
                    warn!(
                        source_id = %self.id,
                        delay_ms = delay.as_millis(),
                        "scheduling reconnect after backoff"
                    );

                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {}
                        _ = ctx.cancel.cancelled() => {
                            info!(source_id = %self.id, "cancelled during reconnect backoff");
                            break;
                        }
                    }

                    // Never send START_REPLICATION to another cluster.
                    if !verify_before_reconnect(&mut ctx).await? {
                        info!(source_id = %self.id, "cancelled during reconnect");
                        break;
                    }
                    // Resume at the last commit handed on; a transaction cut
                    // off mid-stream is abandoned here and sent again in full
                    // (from its BEGIN) by the new stream.
                    if let Some(xid) = ctx.current_tx_id.take() {
                        let abandoned = std::mem::take(&mut ctx.open_tx_events);
                        warn!(
                            source_id = %self.id,
                            pipeline = %self.pipeline,
                            xid,
                            final_lsn = ?ctx.current_final_lsn,
                            last_complete_position = %ctx.resume_lsn,
                            abandoned_events = abandoned,
                            duplicates_possible =
                                "only with batch.respect_source_tx=false",
                            "stream ended inside a transaction: abandoning it and \
                             replaying it in full from the last complete position"
                        );
                        metrics::counter!(
                            "deltaforge_source_transaction_aborts_total",
                            "pipeline" => self.pipeline.clone(),
                            "source" => self.id.clone(),
                        )
                        .increment(1);
                        metrics::counter!(
                            "deltaforge_source_replayed_events_total",
                            "pipeline" => self.pipeline.clone(),
                            "source" => self.id.clone(),
                        )
                        .increment(abandoned);
                        ctx.tx
                            .send(SourceItem::TxAbort {
                                tx_id: xid.to_string(),
                            })
                            .await
                            .map_err(|e| SourceError::Other(e.into()))?;
                        ctx.current_tx_commit_time = None;
                        ctx.current_final_lsn = None;
                    }
                    ctx.last_lsn = ctx.resume_lsn;
                    // The current credentials (a rotation may have replaced
                    // the ones the source started with).
                    let reconnect_config = postgres_helpers::build_replication_config(
                        &postgres_helpers::parse_dsn(ctx.dsn.expose())?,
                        &self.slot,
                        &self.publication,
                        ctx.resume_lsn,
                    );

                    match connect_replication_with_retries(
                        &self.id,
                        reconnect_config,
                        &stream_proof.with_dsn(ctx.dsn.clone()),
                        true,
                        &ctx.cancel,
                        ctx.retry.clone(),
                    )
                    .await
                    {
                        Ok(new_client) => {
                            *ctx.repl_client.lock().await = new_client;
                            ctx.refresh_stamp();
                            ctx.retry.reset();
                            info!(source_id = %self.id, "reconnected successfully");
                            check_identity_post_reconnect(&mut ctx).await?;
                        }
                        Err(e) => {
                            error!(
                                source_id = %self.id, error = %e,
                                "reconnect failed after retries"
                            );
                            return Err(e);
                        }
                    }
                }
                Err(LoopControl::Stop) => {
                    info!(source_id = %self.id, "stop requested");
                    break;
                }
                Err(LoopControl::Fail(e)) => {
                    error!(source_id = %self.id, error = %e, "unrecoverable error");
                    return Err(e);
                }
            }
        }
        Ok(())
        }
        .await;

        // Cancel and join the rotation tasks on every exit path (normal or fatal),
        // so no watcher/preflight task outlives the source.
        if let Some(rt) = rotation.take() {
            rt.shutdown().await;
        }

        // Re-propagate a fatal loop outcome (e.g. a gate-6 rotation failure) before
        // the teardown checkpoint put, so the checkpoint never advances past the
        // last committed transaction.
        loop_result?;

        // Persist the read position as the aggregate checkpoint ONLY when this store
        // does not derive the resume position from per-sink checkpoints. In production
        // the coordinator writes per-sink checkpoints (only after sink acknowledgement)
        // and the resume position is their minimum; writing the read position here would
        // resume ahead of un-acknowledged deliveries and lose them on restart (e.g. a
        // clean stop during a sink outage).
        if !chkpt_store.manages_per_sink_checkpoints() {
            let _ = chkpt_store
                .put(
                    &self.id,
                    PostgresCheckpoint {
                        lsn: ctx.last_lsn.to_string(),
                        tx_id: None,
                        timeline: ctx.active_stamp().map(|s| s.timeline),
                        chain: ctx.active_stamp().map(|s| s.chain_id),
                        transition: ctx.active_stamp().map(|s| s.transition),
                    },
                )
                .await;
        }

        if let Err(e) = ctx.repl_client.lock().await.shutdown().await {
            warn!(error = %e, "error during replication client shutdown");
        }

        Ok(())
    }
}

/// How long the durable checkpoint store may be transiently unavailable before the
/// source stops fail-closed rather than keep consuming with stale recovery authority
/// (and unbounded retained WAL).
const FEEDBACK_STORE_UNAVAILABLE_STOP: Duration = Duration::from_secs(60);

/// Mutable state for WAL feedback across loop iterations: the last durable frontier, the
/// start of a transient store-unavailability window, and an edge latch so a persistent
/// storage failure warns once, not every poll.
struct WalFeedback {
    frontier: Option<Lsn>,
    unavailable_since: Option<Instant>,
    warned_unavailable: bool,
}

impl WalFeedback {
    fn new() -> Self {
        Self {
            frontier: None,
            unavailable_since: None,
            warned_unavailable: false,
        }
    }
}

/// Classification of a durable-frontier read for WAL feedback.
#[derive(Debug, PartialEq, Eq)]
enum FeedbackOutcome {
    /// A usable durable frontier (strictly newer than the prior).
    Frontier(Lsn),
    /// Nothing durable yet (`Ok(None)`), or a valid checkpoint at/behind the prior
    /// frontier: hold the prior frontier, no error.
    Hold,
    /// The checkpoint store was transiently unavailable (I/O / backend): hold, but count
    /// toward a bounded stop threshold.
    Transient(String),
    /// The durable checkpoint is malformed or incomparable, or the store cannot serve it:
    /// terminal, fail closed.
    FailClosed(String),
}

/// Read and classify the durable delivery frontier from the checkpoint store. Never
/// collapses distinct failure modes into "hold": `Ok(None)` and a valid older checkpoint
/// hold; a malformed/incomparable checkpoint fails closed; transient storage
/// unavailability is reported so the caller can bound it.
async fn classify_durable_frontier(
    chkpt: &Arc<dyn CheckpointStore>,
    source_id: &str,
    prev: Option<Lsn>,
) -> FeedbackOutcome {
    let bytes = match chkpt.get_raw(source_id).await {
        Ok(Some(bytes)) => bytes,
        Ok(None) => return FeedbackOutcome::Hold, // nothing durable yet
        Err(e) => return classify_store_error(&e),
    };
    match serde_json::from_slice::<PostgresCheckpoint>(&bytes)
        .ok()
        .and_then(|cp| Lsn::parse(&cp.lsn).ok())
    {
        // Monotonic: a valid checkpoint at/behind the frontier holds; newer advances.
        Some(lsn) => match prev {
            Some(p) if lsn <= p => FeedbackOutcome::Hold,
            _ => FeedbackOutcome::Frontier(lsn),
        },
        None => FeedbackOutcome::FailClosed(format!(
            "durable checkpoint for source '{source_id}' is malformed (unparseable); \
             refusing to advance WAL feedback"
        )),
    }
}

/// Map a checkpoint-store error onto a feedback outcome. Malformed/incomparable/
/// unserviceable errors are terminal; I/O and backend errors are transient.
fn classify_store_error(e: &checkpoints::CheckpointError) -> FeedbackOutcome {
    use checkpoints::CheckpointError as E;
    match e {
        // Malformed, incomparable (proxy fold), or a store that cannot serve the
        // checkpoint: terminal.
        E::Data(_)
        | E::Serde(_)
        | E::NotSupported(_)
        | E::UnsupportedAtomicOperation(_) => FeedbackOutcome::FailClosed(
            format!("durable checkpoint is unusable: {e}"),
        ),
        // Storage unavailability: transient, bounded by the caller.
        E::Io(_) | E::Database(_) | E::Other(_) => {
            FeedbackOutcome::Transient(e.to_string())
        }
    }
}

/// Advance the WAL feedback sent to PostgreSQL to the durable delivery frontier - the
/// minimum per-sink checkpoint every required sink has acknowledged - never the
/// read/enqueued position. The store only ever holds committed, acknowledged checkpoints
/// at transaction boundaries, so the reported LSN can never be inside an open transaction
/// or ahead of an unacknowledged/backpressured batch.
///
/// Fails closed (returns `Err`, stopping the source before the teardown checkpoint put)
/// on a malformed/incomparable durable checkpoint, or when the store has been transiently
/// unavailable for longer than [`FEEDBACK_STORE_UNAVAILABLE_STOP`] - so the source never
/// keeps consuming indefinitely against unavailable recovery authority.
async fn advance_wal_feedback(
    ctx: &RunCtx,
    chkpt: &Arc<dyn CheckpointStore>,
    state: &mut WalFeedback,
) -> SourceResult<()> {
    match classify_durable_frontier(chkpt, &ctx.source_id, state.frontier).await
    {
        FeedbackOutcome::Frontier(lsn) => {
            state.frontier = Some(lsn);
            state.unavailable_since = None;
            state.warned_unavailable = false;
            ctx.repl_client.lock().await.update_applied_lsn(lsn);
        }
        FeedbackOutcome::Hold => {
            state.unavailable_since = None;
            state.warned_unavailable = false;
            if let Some(f) = state.frontier {
                ctx.repl_client.lock().await.update_applied_lsn(f);
            }
        }
        FeedbackOutcome::Transient(detail) => {
            let now = Instant::now();
            let since = *state.unavailable_since.get_or_insert(now);
            if !state.warned_unavailable {
                warn!(source_id = %ctx.source_id, error = %detail,
                    "durable checkpoint store unavailable; holding WAL feedback");
                state.warned_unavailable = true; // edge-latched
            }
            if now.duration_since(since) >= FEEDBACK_STORE_UNAVAILABLE_STOP {
                return Err(SourceError::Other(anyhow::anyhow!(
                    "durable checkpoint store unavailable for {}s for source '{}'; \
                     stopping fail-closed to avoid unbounded WAL retention ({detail})",
                    FEEDBACK_STORE_UNAVAILABLE_STOP.as_secs(),
                    ctx.source_id
                )));
            }
            // Hold at the prior frontier while unavailability is within bounds.
            if let Some(f) = state.frontier {
                ctx.repl_client.lock().await.update_applied_lsn(f);
            }
        }
        FeedbackOutcome::FailClosed(detail) => {
            return Err(SourceError::Other(anyhow::anyhow!(detail)));
        }
    }
    Ok(())
}

/// Order two PostgreSQL checkpoints by LSN, failing closed.
///
/// `PostgresCheckpoint` is `{ lsn: "X/Y" (hex), tx_id: Option<u32> }`. A
/// checkpoint that does not parse, or whose LSN is not a valid `hi/lo` hex pair,
/// is [`CheckpointOrder::Incomparable`] rather than silently treated as an
/// orderable position - the per-sink fold turns that into a hard error instead
/// of resuming ahead of a sink.
///
/// Cross-lineage comparison (two LSNs from different PostgreSQL systems) is a
/// known gap: v1 checkpoints carry no lineage token, so same-lineage is assumed
/// here. See `docs/specs/postgres-checkpoint-comparison-lineage-design.md`.
///
/// Continuity: timeline numbers prove no ancestry (timelines 2 and 3 can both
/// fork from 1), so checkpoints order only within one proven continuity
/// chain. Same chain and transition: by LSN (a different timeline there is
/// corrupt, incomparable). Same chain, different transitions: by the proven
/// transition (a later transition at a lower LSN is corrupt, incomparable).
/// Different chains, or a partial stamp: incomparable. A checkpoint written
/// before continuity was recorded (no stamp) orders by LSN with another such
/// checkpoint and with the first link (transition 0) of a chain, which was
/// adopted from them on a single history or started fresh; never with a
/// later transition.
///
/// An incomplete-snapshot position (or a pre-format snapshot checkpoint, the
/// anchor LSN as text) equals only the same position of the same generation
/// and anchor, and orders before any stream position at or after its anchor
/// (the completing boundary's, or CDC after it). Against any other position -
/// another generation or anchor, or a stream position before the anchor - it
/// is incomparable.
pub fn compare_pg_checkpoints(a: &[u8], b: &[u8]) -> CheckpointOrder {
    crate::snapshot_position::order(&PgOrder, a, b)
}

/// PostgreSQL's part of the snapshot position order.
struct PgOrder;

impl crate::snapshot_position::EngineOrder for PgOrder {
    /// The anchor LSN as text.
    type Anchor = String;

    fn stream_order(&self, a: &[u8], b: &[u8]) -> CheckpointOrder {
        compare_pg_stream_checkpoints(a, b)
    }

    fn anchor_vs_stream(
        &self,
        anchor: &String,
        stream: &[u8],
    ) -> CheckpointOrder {
        let (Ok(anchor), Some(lsn)) = (
            Lsn::parse(anchor),
            serde_json::from_slice::<PostgresCheckpoint>(stream)
                .ok()
                .and_then(|cp| Lsn::parse(&cp.lsn).ok()),
        ) else {
            return CheckpointOrder::Incomparable;
        };
        match u64::from(anchor).cmp(&u64::from(lsn)) {
            std::cmp::Ordering::Less => CheckpointOrder::Before,
            std::cmp::Ordering::Equal => CheckpointOrder::Equal,
            std::cmp::Ordering::Greater => CheckpointOrder::After,
        }
    }

    fn completion_mark(&self, _stream: &[u8]) -> Option<(Option<String>, u64)> {
        None
    }

    fn bare_legacy(&self, raw: &[u8]) -> Option<String> {
        match classify_pg_checkpoint(raw) {
            Ok(PgResumePosition::Snapshot { anchor })
                if !raw.starts_with(b"{") =>
            {
                Some(anchor.to_string())
            }
            _ => None,
        }
    }
}

/// [`compare_pg_checkpoints`] of two stream positions.
fn compare_pg_stream_checkpoints(a: &[u8], b: &[u8]) -> CheckpointOrder {
    #[derive(serde::Deserialize)]
    struct Cp {
        lsn: String,
        #[serde(default)]
        timeline: Option<u32>,
        #[serde(default)]
        chain: Option<String>,
        #[serde(default)]
        transition: Option<u64>,
    }

    /// Whether two checkpoints share a proven continuity (see above).
    fn continuity_comparable(a: &Cp, la: u64, b: &Cp, lb: u64) -> bool {
        // `Some(None)`: no stamp (legacy); `None`: a partial stamp.
        let stamp = |c: &Cp| match (&c.chain, c.transition, c.timeline) {
            (Some(chain), Some(tr), Some(tl)) => {
                Some(Some((chain.clone(), tr, tl)))
            }
            (None, None, None) => Some(None),
            _ => None,
        };
        match (stamp(a), stamp(b)) {
            (Some(None), Some(None)) => true,
            (Some(None), Some(Some((_, 0, _))))
            | (Some(Some((_, 0, _))), Some(None)) => true,
            (Some(Some((ca, ta, tla))), Some(Some((cb, tb, tlb)))) => {
                ca == cb
                    && match ta.cmp(&tb) {
                        std::cmp::Ordering::Equal => tla == tlb,
                        std::cmp::Ordering::Less => la <= lb,
                        std::cmp::Ordering::Greater => la >= lb,
                    }
            }
            _ => false,
        }
    }

    fn parse_lsn(s: &str) -> Option<u64> {
        let (hi, lo) = s.split_once('/')?;
        // Each half is a 32-bit word. Parse as u32 so an out-of-range or
        // over-long hex component (e.g. "100000000") is rejected rather than
        // silently truncated or aliased into the combined u64.
        let hi = u32::from_str_radix(hi, 16).ok()?;
        let lo = u32::from_str_radix(lo, 16).ok()?;
        Some(((hi as u64) << 32) | (lo as u64))
    }

    let a: Cp = match serde_json::from_slice(a) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, "incomparable checkpoint a: parse failed");
            return CheckpointOrder::Incomparable;
        }
    };
    let b: Cp = match serde_json::from_slice(b) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, "incomparable checkpoint b: parse failed");
            return CheckpointOrder::Incomparable;
        }
    };
    let (Some(la), Some(lb)) = (parse_lsn(&a.lsn), parse_lsn(&b.lsn)) else {
        tracing::warn!(
            lsn_a = %a.lsn,
            lsn_b = %b.lsn,
            "incomparable checkpoint: malformed LSN"
        );
        return CheckpointOrder::Incomparable;
    };
    if !continuity_comparable(&a, la, &b, lb) {
        tracing::warn!(
            lsn_a = %a.lsn, chain_a = ?a.chain, transition_a = ?a.transition,
            timeline_a = ?a.timeline, lsn_b = %b.lsn, chain_b = ?b.chain,
            transition_b = ?b.transition, timeline_b = ?b.timeline,
            "incomparable checkpoints: no common proven continuity"
        );
        return CheckpointOrder::Incomparable;
    }
    // Same chain, different proven transitions: the transition orders them.
    if let (Some(ta), Some(tb)) = (a.transition, b.transition)
        && ta != tb
    {
        return if ta < tb {
            CheckpointOrder::Before
        } else {
            CheckpointOrder::After
        };
    }
    match la.cmp(&lb) {
        std::cmp::Ordering::Less => CheckpointOrder::Before,
        std::cmp::Ordering::Equal => CheckpointOrder::Equal,
        std::cmp::Ordering::Greater => CheckpointOrder::After,
    }
}

#[async_trait]
impl Source for PostgresSource {
    async fn run(
        &self,
        tx: mpsc::Sender<SourceItem>,
        chkpt_store: Arc<dyn CheckpointStore>,
    ) -> SourceHandle {
        let cancel = CancellationToken::new();
        let paused = Arc::new(AtomicBool::new(false));
        let pause_notify = Arc::new(Notify::new());

        let this = self.clone();
        let cancel_for_task = cancel.clone();
        let paused_for_task = paused.clone();
        let pause_notify_for_task = pause_notify.clone();
        let ready = deltaforge_core::SourceReady::new();
        let ready_for_task = ready.clone();

        let join = tokio::spawn(async move {
            let res = this
                .run_inner(
                    tx,
                    chkpt_store,
                    cancel_for_task,
                    paused_for_task,
                    pause_notify_for_task,
                    ready_for_task,
                )
                .await;
            if let Err(e) = &res {
                error!(error = ?e, "postgres source ended with error");
            }
            res
        });

        SourceHandle {
            cancel,
            paused,
            pause_notify,
            join,
            ready,
        }
    }

    fn compare_checkpoints(&self, a: &[u8], b: &[u8]) -> CheckpointOrder {
        compare_pg_checkpoints(a, b)
    }

    fn checkpoint_generation_start(
        &self,
        prev: Option<&[u8]>,
        start: &deltaforge_core::GenerationStart,
    ) -> deltaforge_core::CheckpointStart {
        crate::snapshot_position::checkpoint_start(&PgOrder, prev, None, start)
    }

    async fn check_durable_snapshot_startup(
        &self,
        checkpoint_store: &dyn CheckpointStore,
    ) -> Result<(), SourceError> {
        let progress: postgres_snapshot::SnapshotProgress = checkpoint_store
            .get_raw(&postgres_snapshot::progress_key(&self.id))
            .await
            .ok()
            .flatten()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default();
        if !progress.finished
            && !progress.done_tables.is_empty()
            && self.legacy_progress_is_ambiguous(&progress).await?
        {
            return Err(SourceError::Other(anyhow::anyhow!(
                "durable_v2: interrupted legacy snapshot progress for source \
                 {} cannot be adopted (some tables done, some pending); finish \
                 it under legacy mode or start a new snapshot generation",
                self.id
            )));
        }
        Ok(())
    }
}

// ============================================================================
// Failover detection + reconciliation
// ============================================================================

/// Number of bounded attempts to obtain a verified server identity before
/// failing closed.
const IDENTITY_FETCH_ATTEMPTS: u32 = 5;

/// Verified PostgreSQL lineage: the schema-registry descriptor and the failover
/// identity, derived from the same live read.
#[derive(Debug, Clone)]
pub(crate) struct VerifiedPgLineage {
    pub descriptor: LineageDescriptor,
    pub identity: PostgresServerIdentity,
}

/// Fetch and verify the PostgreSQL lineage (cluster `system_identifier` +
/// current database OID), failing closed.
///
/// Retries a bounded number of times on transient fetch errors, then returns an
/// error rather than proceeding on an unverified lineage. A `None` result
/// (`pg_control_system()` restricted/unsupported) or a zero identifier is
/// unverifiable: we refuse to stream rather than freeze a bogus lineage into
/// event IDs, the failover check, or schema-registry keys. This is the single
/// authority for provisional event IDs, identity comparison, and the registry
/// scope.
async fn fetch_pg_lineage_verified(
    dsn: &str,
) -> SourceResult<VerifiedPgLineage> {
    let mut attempt = 0u32;
    loop {
        match postgres_health::fetch_registry_lineage(dsn).await {
            Ok(Some((sysid, dboid))) => {
                if sysid == 0 || dboid == 0 {
                    return Err(SourceError::Other(anyhow::anyhow!(
                        "server lineage has a zero system_identifier or \
                         database oid; cannot verify cluster lineage - \
                         refusing to stream"
                    )));
                }
                let descriptor =
                    LineageDescriptor::postgres(sysid as u64, dboid as u64)
                        .map_err(SourceError::Other)?;
                return Ok(VerifiedPgLineage {
                    descriptor,
                    identity: PostgresServerIdentity {
                        system_identifier: sysid,
                    },
                });
            }
            Ok(None) => {
                return Err(SourceError::Other(anyhow::anyhow!(
                    "server identity unavailable (pg_control_system() \
                     restricted or unsupported); cannot verify cluster \
                     lineage - refusing to stream"
                )));
            }
            Err(e) => {
                attempt += 1;
                if attempt >= IDENTITY_FETCH_ATTEMPTS {
                    return Err(SourceError::Other(anyhow::anyhow!(
                        "failed to fetch server identity after {attempt} \
                         attempts: {e}; refusing to stream on unverified \
                         identity"
                    )));
                }
                warn!(
                    attempt, error = %e,
                    "postgres identity fetch failed; retrying before failing closed"
                );
                tokio::time::sleep(Duration::from_millis(
                    200 * 2u64.pow(attempt.min(5)),
                ))
                .await;
            }
        }
    }
}

/// Verify the live PostgreSQL lineage and establish it as the registry scope of
/// `(tenant, source_id)`: durably recorded (fail closed), then published into
/// `shared`. This is the startup step every PostgreSQL source runs before any
/// schema-registry access.
pub async fn establish_registry_scope(
    dsn: &str,
    backend: &ArcStorageBackend,
    shared: &SharedRegistryScope,
    tenant: &str,
    source_id: &str,
) -> SourceResult<ScopeChange> {
    let lineage = fetch_pg_lineage_verified(dsn).await?;
    refuse_server_change(backend, tenant, source_id, &lineage).await?;
    Ok(
        establish_scope(backend, shared, tenant, source_id, lineage.descriptor)
            .await?,
    )
}

/// Cross-cluster refusal: a PostgreSQL source never continues on another
/// cluster. Its checkpoint LSN and slot position belong to one cluster's WAL
/// history: a server with another `system_identifier`, or the same cluster
/// with a replaced database, cannot inherit them, however comparable its LSNs
/// look. This does NOT detect a promoted physical standby (it keeps the
/// `system_identifier`): continuity across a promotion or timeline change is
/// not proven here (design spec 7.24). Checked against both
/// durable authorities (the source lineage record and the failover identity)
/// before anything is persisted, snapshotted or streamed, and again before
/// every replication stream opens. Recovery is a new source id, which starts
/// with a snapshot.
async fn refuse_server_change(
    backend: &ArcStorageBackend,
    tenant: &str,
    source_id: &str,
    live: &VerifiedPgLineage,
) -> SourceResult<()> {
    if let Some(record) =
        storage::adapters::source_lineage::load(backend, tenant, source_id)
            .await
            .map_err(SourceError::Other)?
        && lineage_transition(&record.current.descriptor, &live.descriptor)
            != LineageTransition::Same
    {
        return Err(server_changed(
            source_id,
            &record.current.descriptor,
            &live.descriptor,
        ));
    }
    match IdentityStore::new(Arc::clone(backend))
        .compare(source_id, &ServerIdentity::from(live.identity.clone()))
        .await
        .map_err(SourceError::Other)?
    {
        IdentityComparison::Changed { previous, current } => {
            Err(server_changed(source_id, &previous, &current))
        }
        _ => Ok(()),
    }
}

/// How long a plausibly transient failure to verify the resume position is
/// retried before the source stops.
const REACHABILITY_RETRY_WINDOW: Duration = Duration::from_secs(120);

/// Verify the slot still holds the checkpoint position before replication
/// opens. A confirmed loss or an error answer from the server stops at once;
/// an unknown answer is retried with backoff while it is plausibly transient.
/// While it retries, the condition is an open auto-retry
/// `pg_continuity_unproven` incident (recorded in `incidents`); when the
/// window ends the source stops on the same incident, which then needs an
/// operator. Every stop is operator action; cancellation stops without one.
/// Nothing is streamed.
async fn verify_resume_position(
    dsn: &str,
    source_id: &str,
    slot: &str,
    checkpoint: &str,
    cancel: &CancellationToken,
    window: Duration,
    incidents: &IncidentStore,
) -> SourceResult<()> {
    let deadline = Instant::now() + window;
    let mut delay = Duration::from_secs(1);
    let mut retrying = None;
    loop {
        let outcome = check_position_reachability(dsn, slot)
            .await
            .unwrap_or_else(|e| PositionReachability::Unknown {
                transient: false,
                reason: format!("{e:#}"),
            });
        match outcome {
            PositionReachability::Reachable => return Ok(()),
            PositionReachability::Lost { class, reason } => {
                error!(
                    source_id, slot, %reason,
                    "resume position unreachable through the slot; halting"
                );
                return Err(continuity_unproven(
                    source_id,
                    slot,
                    checkpoint,
                    class.as_str(),
                    SourceError::Checkpoint {
                        details: format!(
                            "replication slot '{slot}' cannot resume from \
                             {checkpoint}: {reason}. Re-snapshot required."
                        )
                        .into(),
                    },
                ));
            }
            PositionReachability::Unknown { transient, reason } => {
                if transient && Instant::now() + delay < deadline {
                    if retrying.is_none() {
                        retrying = crate::incident_drafts::record_retrying(
                            incidents,
                            &continuity_unproven_draft(
                                source_id,
                                slot,
                                checkpoint,
                                "unknown_unreachable",
                                Retryability::AutoRetry,
                                CauseCode::SourceConnect,
                            ),
                        )
                        .await;
                    }
                    warn!(
                        source_id, slot, %reason,
                        retry_in_secs = delay.as_secs(),
                        "cannot verify the resume position yet; retrying \
                         (replication stays closed)"
                    );
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {}
                        _ = cancel.cancelled() => {
                            crate::incident_drafts::cancel_retrying(
                                incidents,
                                retrying,
                            )
                            .await;
                            return Err(SourceError::Cancelled);
                        }
                    }
                    delay = (delay * 2).min(Duration::from_secs(15));
                    continue;
                }
                error!(
                    source_id, slot, %reason, transient,
                    "cannot verify the resume position; halting before \
                     replication opens"
                );
                let class = if transient {
                    "unknown_unreachable"
                } else {
                    "unknown_query_failed"
                };
                let details = format!(
                    "cannot verify that replication slot '{slot}' holds the \
                     resume position {checkpoint}: {reason}"
                );
                let cause = if transient {
                    SourceError::Connect {
                        details: details.into(),
                    }
                } else {
                    SourceError::Checkpoint {
                        details: details.into(),
                    }
                };
                return Err(continuity_unproven(
                    source_id, slot, checkpoint, class, cause,
                ));
            }
        }
    }
}

/// The `pg_continuity_unproven` incident around `cause`: the source stops,
/// so it needs an operator.
fn continuity_unproven(
    source_id: &str,
    slot: &str,
    checkpoint: &str,
    class: &str,
    cause: SourceError,
) -> SourceError {
    let draft = continuity_unproven_draft(
        source_id,
        slot,
        checkpoint,
        class,
        Retryability::OperatorAction,
        cause.cause_code(),
    );
    SourceError::incident(draft, cause)
}

/// The `pg_continuity_unproven` incident for a slot that is not at or before
/// the durable checkpoint (`slot_beyond_checkpoint`), is missing, or reports
/// no position: streaming would skip changes or rest on an unknown position.
/// The slot's positions are evidence.
pub(super) fn slot_bound_incident(
    source_id: &str,
    slot: &str,
    checkpoint: &str,
    class: &str,
    restart: Option<String>,
    confirmed: Option<String>,
) -> SourceError {
    let details = match class {
        "slot_beyond_checkpoint" => format!(
            "replication slot '{slot}' is beyond the durable checkpoint \
             {checkpoint} (restart {}, confirmed {}): resuming would skip the \
             changes in between. Re-snapshot required.",
            restart.as_deref().unwrap_or("none"),
            confirmed.as_deref().unwrap_or("none")
        ),
        "slot_missing" => format!(
            "replication slot '{slot}' does not exist; it cannot resume from \
             {checkpoint}. Re-snapshot required."
        ),
        _ => format!(
            "replication slot '{slot}' reports no restart/confirmed position; \
             resuming from {checkpoint} cannot be proven"
        ),
    };
    let cause = SourceError::Checkpoint {
        details: details.into(),
    };
    let draft = continuity_unproven_draft(
        source_id,
        slot,
        checkpoint,
        class,
        Retryability::OperatorAction,
        cause.cause_code(),
    )
    .with_evidence(|e| {
        if let Some(r) = &restart {
            e.text(K::SlotRestartPosition, r);
        }
        if let Some(c) = &confirmed {
            e.text(K::SlotConfirmedPosition, c);
        }
    });
    SourceError::incident(draft, cause)
}

/// The `pg_continuity_unproven` incident of a continuity proof that failed
/// on the replication session (see `postgres_continuity`): the source stops
/// before START_REPLICATION. Timelines, the switch point, slot bounds, the WAL
/// flush and read positions are evidence.
pub(super) fn continuity_refusal(
    source_id: &str,
    slot: &str,
    checkpoint: Option<Lsn>,
    class: &str,
    ev: &postgres_continuity::RefusalEvidence,
) -> SourceError {
    let checkpoint = checkpoint.map_or("none".to_string(), |f| f.to_string());
    let shown = |v: Option<String>| v.unwrap_or_else(|| "unknown".into());
    let recorded = shown(ev.recorded_timeline.map(|t| t.to_string()));
    let live = shown(ev.live_timeline.map(|t| t.to_string()));
    let details = match class {
        "timeline_unrecorded" => format!(
            "no timeline is recorded for checkpoint {checkpoint}, and the \
             server (timeline {live}) has switched timeline: continuity of the \
             checkpoint cannot be proven. Adopt the current timeline \
             explicitly after verifying it, or re-snapshot."
        ),
        "timeline_not_descended" => format!(
            "the server's timeline {live} does not descend from timeline \
             {recorded}, on which checkpoint {checkpoint} was written: it is \
             another history. Re-snapshot required."
        ),
        "switch_before_checkpoint" | "switch_before_read_position" => format!(
            "timeline {recorded} ended at {} on this server, before the \
             position the source continues from (checkpoint {checkpoint}, \
             read position {}): changes after the switch point are not part \
             of this server's history. Re-snapshot required.",
            shown(ev.switch_point.map(|l| l.to_string())),
            shown(ev.start.map(|l| l.to_string())),
        ),
        "slot_not_synced" => format!(
            "the server switched from timeline {recorded} to {live}, but slot \
             '{slot}' is not a synchronized failover slot: its position \
             cannot be trusted to continue checkpoint {checkpoint}. \
             Re-snapshot required."
        ),
        "failover_unsupported_version" => format!(
            "the server switched from timeline {recorded} to {live}; \
             continuation across a timeline switch needs PostgreSQL 17 \
             failover slots. Re-snapshot required."
        ),
        "checkpoint_chain_mismatch" => format!(
            "checkpoint {checkpoint} carries a continuity stamp (chain {}, \
             transition {}) that the recorded continuity (chain {}, \
             transition {}) does not contain: it cannot be continued. \
             Inspect the source's state; re-snapshot required.",
            shown(ev.checkpoint_chain.clone()),
            shown(ev.checkpoint_transition.map(|t| t.to_string())),
            shown(ev.recorded_chain.clone()),
            shown(ev.recorded_transition.map(|t| t.to_string())),
        ),
        "server_in_recovery" => format!(
            "the server is a standby in recovery (timeline {live}): it can \
             switch timeline while a stream is open, so continuity of \
             checkpoint {checkpoint} cannot be proven on it. Point the source \
             at the primary."
        ),
        "wal_behind_checkpoint" => format!(
            "the server's WAL ends at {}, before checkpoint {checkpoint}: it \
             does not contain the source's history. Re-snapshot required.",
            shown(ev.flush.map(|l| l.to_string())),
        ),
        other => format!(
            "replication slot '{slot}' cannot continue checkpoint \
             {checkpoint} ({other})"
        ),
    };
    error!(
        source_id, slot, checkpoint = %checkpoint, class,
        recorded_timeline = %recorded, live_timeline = %live,
        "continuity of the resume position is not proven; refusing to stream"
    );
    let cause = SourceError::Checkpoint {
        details: details.into(),
    };
    let draft = continuity_unproven_draft(
        source_id,
        slot,
        &checkpoint,
        class,
        Retryability::OperatorAction,
        cause.cause_code(),
    )
    .with_evidence(|e| {
        let mut put = |k, v: Option<String>| {
            if let Some(v) = v {
                e.text(k, &v);
            }
        };
        put(
            K::RecordedTimeline,
            ev.recorded_timeline.map(|t| t.to_string()),
        );
        put(K::LiveTimeline, ev.live_timeline.map(|t| t.to_string()));
        put(
            K::TimelineSwitchPosition,
            ev.switch_point.map(|l| l.to_string()),
        );
        put(K::SlotRestartPosition, ev.restart.map(|l| l.to_string()));
        put(
            K::SlotConfirmedPosition,
            ev.confirmed.map(|l| l.to_string()),
        );
        put(K::WalFlushPosition, ev.flush.map(|l| l.to_string()));
        put(K::ReadPosition, ev.start.map(|l| l.to_string()));
        put(K::CheckpointChain, ev.checkpoint_chain.clone());
        put(
            K::CheckpointTransition,
            ev.checkpoint_transition.map(|t| t.to_string()),
        );
        put(K::RecordedChain, ev.recorded_chain.clone());
        put(
            K::RecordedTransition,
            ev.recorded_transition.map(|t| t.to_string()),
        );
    });
    SourceError::incident(draft, cause)
}

/// The running-degraded `pg_failover_slot_unavailable` incident: the slot of
/// a PostgreSQL 17+ server is not a failover slot. Resolved when a later
/// stream proves the slot is one.
pub(super) fn failover_slot_unavailable_draft(
    source_id: &str,
    slot: &str,
) -> IncidentDraft {
    IncidentDraft::new(
        ReasonCode::PgFailoverSlotUnavailable,
        Component::Source {
            id: source_id.to_string(),
        },
        Retryability::OperatorAction,
        SafetyState::RunningDegraded,
        CauseCode::SourceIncompatible,
    )
    .discriminate("slot", slot)
    .with_evidence(|e| {
        e.text(K::SourceId, source_id).text(K::Slot, slot);
    })
    .with_actions(&[ActionCode::EnableFailoverSlot])
}

/// The `pg_continuity_unproven` draft. Its identity is the slot, checkpoint
/// and class, never the retryability, so an automatic retry that exhausts
/// its window stays one incident.
pub(super) fn continuity_unproven_draft(
    source_id: &str,
    slot: &str,
    checkpoint: &str,
    class: &str,
    retryability: Retryability,
    cause_code: CauseCode,
) -> IncidentDraft {
    let lost = !class.starts_with("unknown_");
    let actions: &[ActionCode] = if class == "server_in_recovery" {
        // Route the source to the writable primary.
        &[ActionCode::VerifyEndpoint, ActionCode::InspectLogs]
    } else if class == "checkpoint_chain_mismatch" {
        // A recovery operation may later prove a route; for now inspect and
        // re-snapshot.
        &[ActionCode::InspectLogs, ActionCode::Resnapshot]
    } else if lost {
        &[ActionCode::Resnapshot, ActionCode::UseNewSourceId]
    } else {
        &[ActionCode::VerifyEndpoint, ActionCode::InspectLogs]
    };
    IncidentDraft::new(
        ReasonCode::PgContinuityUnproven,
        Component::Source {
            id: source_id.to_string(),
        },
        retryability,
        SafetyState::HaltedSafe,
        cause_code,
    )
    .discriminate("slot", slot)
    .discriminate("checkpoint", checkpoint)
    .discriminate("class", class)
    .with_evidence(|e| {
        e.text(K::SourceId, source_id)
            .text(K::Slot, slot)
            .text(K::CheckpointPosition, checkpoint)
            .text(K::ReasonClass, class);
    })
    .with_actions(actions)
}

/// The PostgreSQL identity a refusal compares: cluster `system_identifier`
/// and, when known, the database OID.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct PgEndpoint {
    system_identifier: Option<u64>,
    database_oid: Option<u64>,
}

pub(super) trait AsPgEndpoint: std::fmt::Debug {
    fn endpoint(&self) -> PgEndpoint;
}

impl AsPgEndpoint for PgEndpoint {
    fn endpoint(&self) -> PgEndpoint {
        *self
    }
}

impl AsPgEndpoint for LineageDescriptor {
    fn endpoint(&self) -> PgEndpoint {
        match self {
            LineageDescriptor::Postgres {
                system_identifier,
                database_oid,
            } => PgEndpoint {
                system_identifier: Some(*system_identifier),
                database_oid: Some(*database_oid),
            },
            _ => PgEndpoint::default(),
        }
    }
}

impl AsPgEndpoint for ServerIdentity {
    fn endpoint(&self) -> PgEndpoint {
        match self {
            ServerIdentity::Postgres(id) => PgEndpoint {
                system_identifier: Some(id.system_identifier as u64),
                database_oid: None,
            },
            _ => PgEndpoint::default(),
        }
    }
}

impl PgEndpoint {
    fn canonical(&self) -> String {
        let show =
            |v: Option<u64>| v.map_or("-".to_string(), |v| v.to_string());
        format!(
            "{}:{}",
            show(self.system_identifier),
            show(self.database_oid)
        )
    }
}

/// The `pg_different_cluster` incident: the source reached another cluster
/// (or a replaced database) and was refused before anything was recorded,
/// snapshotted or streamed.
fn different_cluster_draft(
    source_id: &str,
    expected: PgEndpoint,
    live: PgEndpoint,
) -> deltaforge_core::IncidentDraft {
    IncidentDraft::new(
        ReasonCode::PgDifferentCluster,
        Component::Source {
            id: source_id.to_string(),
        },
        Retryability::OperatorAction,
        SafetyState::HaltedSafe,
        CauseCode::SourceLineage,
    )
    .discriminate("expected", expected.canonical())
    .discriminate("live", live.canonical())
    .with_evidence(|e| {
        e.text(K::SourceId, source_id);
        if let Some(v) = expected.system_identifier {
            e.text(K::ExpectedSystemIdentifier, &v.to_string());
        }
        if let Some(v) = live.system_identifier {
            e.text(K::LiveSystemIdentifier, &v.to_string());
        }
        if let Some(v) = expected.database_oid {
            e.text(K::ExpectedDatabaseOid, &v.to_string());
        }
        if let Some(v) = live.database_oid {
            e.text(K::LiveDatabaseOid, &v.to_string());
        }
    })
    .with_actions(&[ActionCode::VerifyEndpoint, ActionCode::UseNewSourceId])
}

pub(super) fn server_changed(
    source_id: &str,
    expected: &impl AsPgEndpoint,
    live: &impl AsPgEndpoint,
) -> SourceError {
    error!(
        source_id, expected = ?expected, live = ?live,
        "connected to a different PostgreSQL server; refusing to resume"
    );
    SourceError::incident(
        different_cluster_draft(
            source_id,
            expected.endpoint(),
            live.endpoint(),
        ),
        SourceError::Lineage {
            details: format!(
                "source '{source_id}' is bound to PostgreSQL {expected:?} but \
                 is connected to {live:?}. Its checkpoint and replication \
                 slot position belong to the first server's WAL history and \
                 are never resumed on another cluster; nothing was streamed, \
                 snapshotted or recorded. To capture this server, configure a \
                 new source id (it starts with a snapshot)."
            )
            .into(),
        },
    )
}

/// Check a lineage just verified against the live server against the published
/// registry scope before any further registry access: any change (another
/// cluster, or a replaced database) fails closed. The scope is never moved to
/// another cluster mid-run.
fn sync_registry_lineage(
    ctx: &RunCtx,
    live: &LineageDescriptor,
) -> SourceResult<()> {
    let current = ctx.registry_scope.current()?;
    let expected = &current.lineage().descriptor;
    match lineage_transition(expected, live) {
        LineageTransition::Same => Ok(()),
        LineageTransition::ReplacedDatabase => {
            Err(RegistryError::LineageMismatch {
                source_id: ctx.source_id.clone(),
                expected: format!("{expected:?}"),
                live: format!("{live:?}"),
            }
            .into())
        }
        LineageTransition::OtherCluster => {
            Err(server_changed(&ctx.source_id, expected, live))
        }
    }
}

/// How a running PostgreSQL source's verified lineage moved.
#[derive(Debug, PartialEq, Eq)]
enum LineageTransition {
    Same,
    /// A different cluster: never resumed (`refuse_server_change`).
    OtherCluster,
    /// Same cluster, different database OID: the database was replaced under a
    /// running source. Not a failover; fail closed.
    ReplacedDatabase,
}

fn lineage_transition(
    expected: &LineageDescriptor,
    live: &LineageDescriptor,
) -> LineageTransition {
    if expected == live {
        return LineageTransition::Same;
    }
    match (expected, live) {
        (
            LineageDescriptor::Postgres {
                system_identifier: a,
                ..
            },
            LineageDescriptor::Postgres {
                system_identifier: b,
                ..
            },
        ) if a == b => LineageTransition::ReplacedDatabase,
        _ => LineageTransition::OtherCluster,
    }
}

#[cfg(test)]
mod lineage_transition_tests {
    use super::*;

    fn pg(sysid: u64, dboid: u64) -> LineageDescriptor {
        LineageDescriptor::postgres(sysid, dboid).unwrap()
    }

    #[test]
    fn unchanged_lineage_is_same() {
        assert_eq!(
            lineage_transition(&pg(1, 5), &pg(1, 5)),
            LineageTransition::Same
        );
    }

    #[test]
    fn different_cluster_is_another_cluster() {
        assert_eq!(
            lineage_transition(&pg(1, 5), &pg(2, 5)),
            LineageTransition::OtherCluster
        );
    }

    #[test]
    fn same_cluster_new_database_fails_closed() {
        assert_eq!(
            lineage_transition(&pg(1, 5), &pg(1, 6)),
            LineageTransition::ReplacedDatabase
        );
    }
}

/// Resolves the cluster identity **before** opening the replication stream,
/// against the durable authority:
///
/// - `FirstSeen`: persist the verified identity. A durable-write failure fails
///   closed here so no stream is ever opened on an unpersisted identity.
/// - `Same`: nothing to do.
/// - `Changed`: refused (`refuse_server_change`); the start position is never
///   moved to another cluster's slot.
///
/// The comparison and the persistence fail closed.
async fn verify_identity_before_connect(
    live: &ServerIdentity,
    id_store: &IdentityStore,
    source_id: &str,
) -> SourceResult<()> {
    match id_store
        .compare(source_id, live)
        .await
        .map_err(SourceError::Other)?
    {
        IdentityComparison::FirstSeen => {
            // Persist the verified identity BEFORE the stream opens. If the
            // durable write fails we stop startup with no replication opened,
            // rather than opening and only failing on the deferred write.
            id_store
                .store(source_id, live)
                .await
                .map_err(SourceError::Other)
        }
        IdentityComparison::Changed { previous, current } => {
            Err(server_changed(source_id, &previous, &current))
        }
        IdentityComparison::Same => Ok(()),
    }
}

/// Before a reconnect sends START_REPLICATION: wait, backing off and
/// cancellably, until the server answers with a verified lineage, then refuse
/// another cluster. An unreachable or unverifiable server is retried like any
/// connection failure, never streamed from. `false`: cancelled.
async fn verify_before_reconnect(ctx: &mut RunCtx) -> SourceResult<bool> {
    loop {
        match fetch_pg_lineage_verified(ctx.dsn.expose()).await {
            Ok(live) => {
                refuse_server_change(
                    &ctx.registry_backend,
                    &ctx.tenant,
                    &ctx.source_id,
                    &live,
                )
                .await?;
                sync_registry_lineage(ctx, &live.descriptor)?;
                return Ok(true);
            }
            Err(e) => {
                let delay = ctx.retry.next_backoff();
                warn!(
                    source_id = %ctx.source_id, error = %e,
                    delay_ms = delay.as_millis(),
                    "server not verifiable before reconnect; retrying"
                );
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = ctx.cancel.cancelled() => return Ok(false),
                }
            }
        }
    }
}

/// After a replication stream opens, before its first message is read: the
/// server it reached must still be the source's. An unverifiable identity
/// fails closed rather than skipping the check.
async fn check_identity_post_reconnect(ctx: &mut RunCtx) -> SourceResult<()> {
    let verified = fetch_pg_lineage_verified(ctx.dsn.expose()).await?;
    let (descriptor, live) =
        (verified.descriptor, ServerIdentity::from(verified.identity));
    // The registry scope never moves: another lineage fails closed before any
    // registry access on this connection.
    sync_registry_lineage(ctx, &descriptor)?;

    match ctx
        .identity_store
        .compare(&ctx.source_id, &live)
        .await
        .map_err(SourceError::Other)?
    {
        IdentityComparison::FirstSeen => {
            ctx.identity_store
                .store(&ctx.source_id, &live)
                .await
                .map_err(SourceError::Other)?;
        }
        IdentityComparison::Same => {}
        IdentityComparison::Changed { previous, current } => {
            return Err(server_changed(&ctx.source_id, &previous, &current));
        }
    }

    Ok(())
}

/// Apply the configured `on_schema_drift` policy to a detected drift, failing
/// closed. Under Adapt, reload the table's schema and continue only if the reload
/// succeeds - a reload failure fails closed rather than proceeding to the first
/// changed row with an unverified schema. Under Halt, return a typed, actionable
/// error naming the table, the change, and the remediation. In both error cases
/// the caller returns before emitting any post-drift row or advancing the
/// checkpoint.
pub(crate) async fn apply_schema_drift(
    loader: &PostgresSchemaLoader,
    policy: &OnSchemaDrift,
    drift: &postgres_errors::SchemaDrift,
    source_id: &str,
) -> SourceResult<()> {
    match policy {
        OnSchemaDrift::Adapt => {
            info!(
                schema = %drift.schema, table = %drift.table, change = %drift.detail,
                "schema drift; reloading (on_schema_drift=adapt)"
            );
            loader
                .reload_schema(&drift.schema, &drift.table)
                .await
                .map_err(|e| {
                    error!(
                        schema = %drift.schema, table = %drift.table, error = %e,
                        "schema reload failed under on_schema_drift=adapt; failing closed"
                    );
                    e
                })?;
            Ok(())
        }
        OnSchemaDrift::Halt => {
            error!(
                schema = %drift.schema, table = %drift.table, change = %drift.detail,
                "schema drift and on_schema_drift=halt; failing closed"
            );
            Err(crate::incident_drafts::schema_drift_blocked(
                source_id,
                &format!("{}.{}", drift.schema, drift.table),
                &drift.detail,
                SourceError::Schema {
                    details: format!(
                        "schema drift on table \"{}.{}\" ({}) and on_schema_drift=halt. \
                         No events under the changed schema were emitted and the \
                         checkpoint was not advanced. Review the schema change; to \
                         continue past it, restart with on_schema_drift=adapt.",
                        drift.schema, drift.table, drift.detail
                    )
                    .into(),
                },
            ))
        }
    }
}

async fn fetch_slot_confirmed_lsn(
    dsn: &str,
    slot: &str,
) -> anyhow::Result<Lsn> {
    let (client, conn) =
        tokio_postgres::connect(dsn, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        conn.await.ok();
    });
    let row = client
        .query_one(
            "SELECT confirmed_flush_lsn::text \
             FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await?;
    let s: &str = row.get(0);
    s.parse::<Lsn>().map_err(|e| anyhow::anyhow!("{e}"))
}

#[cfg(test)]
mod wal_feedback_tests {
    use super::{FeedbackOutcome, Lsn, classify_durable_frontier};
    use checkpoints::{
        CheckpointError, CheckpointResult, CheckpointStore, MemCheckpointStore,
    };
    use std::sync::Arc;

    fn cp(lsn: &str) -> Vec<u8> {
        format!(r#"{{"lsn":"{lsn}","tx_id":null}}"#).into_bytes()
    }

    /// A store whose reads fail with a caller-chosen error kind.
    struct FailingStore(fn() -> CheckpointError);
    #[async_trait::async_trait]
    impl CheckpointStore for FailingStore {
        async fn get_raw(
            &self,
            _key: &str,
        ) -> CheckpointResult<Option<Vec<u8>>> {
            Err((self.0)())
        }
        async fn put_raw(
            &self,
            _key: &str,
            _bytes: &[u8],
        ) -> CheckpointResult<()> {
            Ok(())
        }
        async fn delete(&self, _key: &str) -> CheckpointResult<bool> {
            Ok(false)
        }
        async fn list(&self) -> CheckpointResult<Vec<String>> {
            Ok(vec![])
        }
    }

    #[tokio::test]
    async fn no_durable_checkpoint_holds() {
        let store: Arc<dyn CheckpointStore> =
            Arc::new(MemCheckpointStore::new().unwrap());
        assert_eq!(
            classify_durable_frontier(&store, "s", None).await,
            FeedbackOutcome::Hold
        );
    }

    #[tokio::test]
    async fn newer_checkpoint_advances_frontier() {
        let store: Arc<dyn CheckpointStore> =
            Arc::new(MemCheckpointStore::new().unwrap());
        store.put_raw("s", &cp("0/200")).await.unwrap();
        assert_eq!(
            classify_durable_frontier(&store, "s", None).await,
            FeedbackOutcome::Frontier(Lsn::parse("0/200").unwrap())
        );
    }

    #[tokio::test]
    async fn valid_older_checkpoint_holds_monotonic() {
        // A committed checkpoint at/behind the prior frontier holds (never regresses).
        let store: Arc<dyn CheckpointStore> =
            Arc::new(MemCheckpointStore::new().unwrap());
        store.put_raw("s", &cp("0/100")).await.unwrap();
        let prev = Some(Lsn::parse("0/300").unwrap());
        assert_eq!(
            classify_durable_frontier(&store, "s", prev).await,
            FeedbackOutcome::Hold
        );
    }

    #[tokio::test]
    async fn transient_store_error_is_transient() {
        // I/O and backend errors are transient (bounded hold), not terminal.
        let io = FailingStore(|| {
            CheckpointError::Io(std::io::Error::other("unreachable"))
        });
        let store: Arc<dyn CheckpointStore> = Arc::new(io);
        assert!(matches!(
            classify_durable_frontier(&store, "s", None).await,
            FeedbackOutcome::Transient(_)
        ));
        let db: Arc<dyn CheckpointStore> =
            Arc::new(FailingStore(|| CheckpointError::Database("down".into())));
        assert!(matches!(
            classify_durable_frontier(&db, "s", None).await,
            FeedbackOutcome::Transient(_)
        ));
    }

    #[tokio::test]
    async fn incomparable_or_malformed_store_error_fails_closed() {
        // The proxy's incomparable/corrupt error is CheckpointError::Data -> terminal.
        let store: Arc<dyn CheckpointStore> = Arc::new(FailingStore(|| {
            CheckpointError::Data("incomparable per-sink checkpoints".into())
        }));
        assert!(matches!(
            classify_durable_frontier(&store, "s", None).await,
            FeedbackOutcome::FailClosed(_)
        ));
    }

    #[tokio::test]
    async fn corrupt_checkpoint_bytes_fail_closed() {
        // A malformed durable checkpoint is terminal, not a silent hold.
        let store: Arc<dyn CheckpointStore> =
            Arc::new(MemCheckpointStore::new().unwrap());
        store.put_raw("s", b"{ not json").await.unwrap();
        assert!(matches!(
            classify_durable_frontier(&store, "s", None).await,
            FeedbackOutcome::FailClosed(_)
        ));
    }
}

#[cfg(test)]
mod compare_checkpoints_tests {
    use super::{
        PostgresCheckpoint, compare_pg_checkpoints, postgres_continuity,
        postgres_helpers,
    };
    use deltaforge_core::CheckpointOrder;

    fn cp(lsn: &str) -> Vec<u8> {
        format!(r#"{{"lsn":"{lsn}","tx_id":null}}"#).into_bytes()
    }

    #[test]
    fn orders_valid_lsns() {
        assert_eq!(
            compare_pg_checkpoints(&cp("0/100"), &cp("0/200")),
            CheckpointOrder::Before
        );
        assert_eq!(
            compare_pg_checkpoints(&cp("0/200"), &cp("0/100")),
            CheckpointOrder::After
        );
        assert_eq!(
            compare_pg_checkpoints(&cp("0/100"), &cp("0/100")),
            CheckpointOrder::Equal
        );
    }

    #[test]
    fn high_word_dominates_low_word_boundary() {
        // 0/FFFFFFFF must be strictly before 1/0 (the hi word carries).
        assert_eq!(
            compare_pg_checkpoints(&cp("0/FFFFFFFF"), &cp("1/0")),
            CheckpointOrder::Before
        );
        assert_eq!(
            compare_pg_checkpoints(&cp("1/0"), &cp("0/FFFFFFFF")),
            CheckpointOrder::After
        );
    }

    #[test]
    fn malformed_is_incomparable_never_equal() {
        // Non-JSON, missing field, non-hex LSN, missing '/' separator all fail
        // closed - and specifically must NOT read as Equal (the old bug).
        for bad in [
            &b"not json"[..],
            &b"{}"[..],
            br#"{"lsn":"zz/yy","tx_id":null}"#,
            br#"{"lsn":"12345","tx_id":null}"#,
        ] {
            assert_eq!(
                compare_pg_checkpoints(bad, &cp("0/100")),
                CheckpointOrder::Incomparable,
                "malformed a must be Incomparable"
            );
            assert_eq!(
                compare_pg_checkpoints(&cp("0/100"), bad),
                CheckpointOrder::Incomparable,
                "malformed b must be Incomparable"
            );
            assert_ne!(
                compare_pg_checkpoints(bad, &cp("0/100")),
                CheckpointOrder::Equal
            );
        }
    }

    #[test]
    fn checkpoints_order_only_within_one_continuity_chain() {
        use CheckpointOrder::*;
        let at = |lsn: &str, chain: &str, transition: u64, timeline: u32| {
            format!(
                r#"{{"lsn":"{lsn}","tx_id":null,"timeline":{timeline},"chain":"{chain}","transition":{transition}}}"#
            )
            .into_bytes()
        };
        let cmp = |a: Vec<u8>, b: Vec<u8>| compare_pg_checkpoints(&a, &b);
        // Sibling timelines: 2 (lower LSN) and 3 (higher LSN) both forked
        // from 1. Timeline numbers prove nothing.
        let timeline_only = |lsn: &str, timeline: u32| {
            format!(r#"{{"lsn":"{lsn}","tx_id":null,"timeline":{timeline}}}"#)
                .into_bytes()
        };
        assert_eq!(
            cmp(timeline_only("0/100", 2), timeline_only("0/200", 3)),
            Incomparable
        );
        assert_eq!(
            cmp(at("0/100", "x", 1, 2), at("0/200", "y", 1, 3)),
            Incomparable,
            "two chains"
        );
        assert_eq!(
            cmp(at("0/100", "x", 1, 2), at("0/200", "y", 2, 3)),
            Incomparable,
            "two chains"
        );
        assert_eq!(
            cmp(at("0/100", "x", 1, 2), at("0/200", "x", 1, 3)),
            Incomparable,
            "one transition on two timelines is corrupt"
        );
        // The same chain with ordered proven transitions.
        assert_eq!(cmp(at("0/100", "x", 1, 2), at("0/200", "x", 2, 3)), Before);
        assert_eq!(cmp(at("0/200", "x", 2, 3), at("0/100", "x", 1, 2)), After);
        assert_eq!(cmp(at("0/100", "x", 0, 1), at("0/100", "x", 1, 2)), Before);
        assert_eq!(
            cmp(at("0/200", "x", 1, 2), at("0/100", "x", 2, 3)),
            Incomparable,
            "a later transition below an earlier position is corrupt"
        );
        // Within one transition: by LSN.
        assert_eq!(cmp(at("0/100", "x", 1, 2), at("0/100", "x", 1, 2)), Equal);
        assert_eq!(cmp(at("0/100", "x", 1, 2), at("0/180", "x", 1, 2)), Before);
        // Legacy (no stamp): with legacy and the first link only.
        assert_eq!(cmp(cp("0/100"), cp("0/200")), Before);
        assert_eq!(cmp(cp("0/100"), at("0/100", "x", 0, 1)), Equal);
        assert_eq!(cmp(cp("0/100"), at("0/200", "x", 1, 2)), Incomparable);
    }
    #[test]
    fn checkpoint_meta_round_trips_with_and_without_timeline() {
        use pgwire_replication::Lsn;
        let lsn = Lsn::parse("1/2A").unwrap();
        let stamp = postgres_continuity::Stamp {
            chain_id: "c0ffee".into(),
            transition: 2,
            timeline: 3,
        };
        for (tx, stamp) in
            [(None, None), (Some(7), None), (Some(7), Some(&stamp))]
        {
            let members =
                stamp.map_or(String::new(), |s| s.checkpoint_members());
            let meta =
                postgres_helpers::make_checkpoint_meta(&lsn, tx, &members);
            let cp: PostgresCheckpoint =
                serde_json::from_slice(meta.as_bytes()).unwrap();
            assert_eq!(cp.lsn, lsn.to_string());
            assert_eq!(cp.tx_id, tx);
            assert_eq!(cp.timeline, stamp.map(|s| s.timeline));
            assert_eq!(cp.chain, stamp.map(|s| s.chain_id.clone()));
            assert_eq!(cp.transition, stamp.map(|s| s.transition));
        }
        // A checkpoint written before timelines were recorded still reads.
        let legacy: PostgresCheckpoint =
            serde_json::from_slice(br#"{"lsn":"0/1","tx_id":null}"#).unwrap();
        assert_eq!(legacy.timeline, None);
    }

    #[test]
    fn well_formed_is_reflexively_equal() {
        // The per-sink fold relies on self-comparison to validate a lone
        // checkpoint: a well-formed one must be Equal to itself.
        assert_eq!(
            compare_pg_checkpoints(&cp("A/B"), &cp("A/B")),
            CheckpointOrder::Equal
        );
    }

    #[test]
    fn out_of_range_lsn_components_are_incomparable() {
        // Each PostgreSQL LSN half is a 32-bit word; a component that does not
        // fit u32 (0x100000000), or an excessively long hex run, must be
        // rejected rather than truncated/aliased into the combined u64.
        for bad_lsn in [
            "100000000/0",        // high half = 2^32, one past u32::MAX
            "0/100000000",        // low half = 2^32
            "FFFFFFFFF/0",        // 9 hex digits, overflows u32
            "0/FFFFFFFFFFFFFFFF", // 16 hex digits, overflows u32
        ] {
            assert_eq!(
                compare_pg_checkpoints(&cp(bad_lsn), &cp("0/100")),
                CheckpointOrder::Incomparable,
                "out-of-range LSN {bad_lsn} (a) must be Incomparable"
            );
            assert_eq!(
                compare_pg_checkpoints(&cp("0/100"), &cp(bad_lsn)),
                CheckpointOrder::Incomparable,
                "out-of-range LSN {bad_lsn} (b) must be Incomparable"
            );
        }
    }
}

#[cfg(test)]
mod schema_drift_policy_tests {
    use super::apply_schema_drift;
    use super::postgres_errors::SchemaDrift;
    use super::{OnSchemaDrift, PostgresSchemaLoader};
    use std::sync::Arc;
    use storage::{DurableSchemaRegistry, MemoryStorageBackend};

    async fn loader(dsn: &str) -> PostgresSchemaLoader {
        let backend = Arc::new(MemoryStorageBackend::new());
        let registry =
            DurableSchemaRegistry::new(backend).await.expect("registry");
        let scope = crate::registry_scope::SharedRegistryScope::new("src");
        scope.publish_for_test(
            "acme",
            storage::adapters::LineageDescriptor::postgres(1, 1).unwrap(),
        );
        PostgresSchemaLoader::new(dsn, registry, "acme", scope)
    }

    fn drift() -> SchemaDrift {
        SchemaDrift {
            schema: "public".into(),
            table: "orders".into(),
            detail: "columns [id] -> [id, status]".into(),
        }
    }

    #[tokio::test]
    async fn halt_fails_closed_with_typed_actionable_error() {
        // DSN is unused on the Halt path (no reload).
        let l = loader("host=127.0.0.1 port=1 dbname=x").await;
        let err = apply_schema_drift(&l, &OnSchemaDrift::Halt, &drift(), "src")
            .await
            .expect_err("halt must fail closed");
        // A schema_drift_blocked incident: evidence names the table, the
        // recommendations include restarting with adapt; the typed cause keeps
        // the detailed message for the logs.
        use deltaforge_core::incident::{
            ActionCode, EvidenceKey, EvidenceValue, ReasonCode,
        };
        let draft = err.draft().expect("an incident");
        assert_eq!(draft.reason_code, ReasonCode::SchemaDriftBlocked);
        assert_eq!(
            draft.evidence.get(EvidenceKey::Table),
            Some(&EvidenceValue::Text {
                value: "public.orders".into()
            })
        );
        assert!(draft.actions.contains(&ActionCode::RestartWithAdapt));
        assert!(draft.resolve_scope.is_some(), "resolved per table");
        let deltaforge_core::SourceError::Schema { details } = err.root()
        else {
            panic!("typed cause: {err:?}");
        };
        assert!(details.contains("on_schema_drift=adapt"));
        assert!(err.to_string().contains("public.orders"));
    }

    #[tokio::test]
    async fn adapt_fails_closed_when_reload_fails() {
        // Unreachable DSN: reload_schema -> load_schema -> connect fails, so Adapt
        // must NOT continue - it fails closed instead of proceeding with an
        // unverified schema.
        let l = loader("host=127.0.0.1 port=1 dbname=x").await;
        let r = apply_schema_drift(&l, &OnSchemaDrift::Adapt, &drift(), "src")
            .await;
        assert!(
            r.is_err(),
            "adapt must fail closed when the schema reload fails"
        );
    }
}

#[cfg(test)]
mod identity_fail_closed_tests {
    //! R3-C2: PostgreSQL identity/lineage authority fails closed.
    //!
    //! These exercise the real production functions (`fetch_pg_lineage_verified`,
    //! `verify_identity_before_connect`, `refuse_server_change`) directly.
    //! Container-backed refusal is covered by `tests/failover_e2e.rs`; here we
    //! prove that when the identity store is unavailable, or the server is
    //! another one, we refuse to open the stream.
    use super::*;
    use crate::failover::identity::{IdentityStore, ServerIdentity};
    use std::sync::Arc;
    use storage::{MemoryStorageBackend, StorageBackend};

    // Port 1 is not bound; a replication/identity connect fails fast rather than
    // hanging, and the short connect_timeout bounds the OS-level wait.
    const UNREACHABLE_DSN: &str =
        "host=127.0.0.1 port=1 user=none dbname=none connect_timeout=1";

    fn pg_identity(n: i64) -> ServerIdentity {
        ServerIdentity::Postgres(PostgresServerIdentity {
            system_identifier: n,
        })
    }

    /// A durable store whose identity read and/or write fail, standing in for an
    /// unavailable identity backend. Every other method delegates to a real
    /// in-memory backend so the double stays faithful to the trait contract.
    #[derive(Debug)]
    struct IdentityStoreDown {
        inner: MemoryStorageBackend,
        fail_get: bool,
        fail_put: bool,
    }

    impl IdentityStoreDown {
        /// Both the identity read and write fail (store fully unavailable).
        fn new() -> Self {
            Self {
                inner: MemoryStorageBackend::new(),
                fail_get: true,
                fail_put: true,
            }
        }
        /// Reads succeed (so a fresh source sees `FirstSeen`) but the durable
        /// write fails - the FirstSeen persistence-failure case.
        fn write_only_down() -> Self {
            Self {
                inner: MemoryStorageBackend::new(),
                fail_get: false,
                fail_put: true,
            }
        }
        fn boom() -> anyhow::Error {
            anyhow::anyhow!("identity store unavailable")
        }
    }

    #[async_trait::async_trait]
    impl StorageBackend for IdentityStoreDown {
        async fn kv_get(
            &self,
            ns: &str,
            key: &str,
        ) -> anyhow::Result<Option<Vec<u8>>> {
            if self.fail_get {
                return Err(Self::boom());
            }
            self.inner.kv_get(ns, key).await
        }
        async fn kv_put(
            &self,
            ns: &str,
            key: &str,
            value: &[u8],
        ) -> anyhow::Result<()> {
            if self.fail_put {
                return Err(Self::boom());
            }
            self.inner.kv_put(ns, key, value).await
        }
        async fn kv_put_with_ttl(
            &self,
            ns: &str,
            key: &str,
            value: &[u8],
            ttl_secs: u64,
        ) -> anyhow::Result<()> {
            self.inner.kv_put_with_ttl(ns, key, value, ttl_secs).await
        }
        async fn kv_delete(&self, ns: &str, key: &str) -> anyhow::Result<bool> {
            self.inner.kv_delete(ns, key).await
        }
        async fn kv_list(
            &self,
            ns: &str,
            prefix: Option<&str>,
        ) -> anyhow::Result<Vec<String>> {
            self.inner.kv_list(ns, prefix).await
        }
        async fn log_append(
            &self,
            ns: &str,
            key: &str,
            value: &[u8],
        ) -> anyhow::Result<u64> {
            self.inner.log_append(ns, key, value).await
        }
        async fn log_list(
            &self,
            ns: &str,
            key: &str,
        ) -> anyhow::Result<Vec<(u64, Vec<u8>)>> {
            self.inner.log_list(ns, key).await
        }
        async fn log_since(
            &self,
            ns: &str,
            key: &str,
            since_seq: u64,
        ) -> anyhow::Result<Vec<(u64, Vec<u8>)>> {
            self.inner.log_since(ns, key, since_seq).await
        }
        async fn log_latest(
            &self,
            ns: &str,
            key: &str,
        ) -> anyhow::Result<Option<(u64, Vec<u8>)>> {
            self.inner.log_latest(ns, key).await
        }
        async fn log_ns_max_seq(&self, ns: &str) -> anyhow::Result<u64> {
            self.inner.log_ns_max_seq(ns).await
        }
        async fn log_append_if_absent(
            &self,
            ns: &str,
            key: &str,
            capture_id: &str,
            value: &[u8],
        ) -> anyhow::Result<storage::LogAppendOutcome> {
            self.inner
                .log_append_if_absent(ns, key, capture_id, value)
                .await
        }
        async fn log_truncate(
            &self,
            ns: &str,
            key: &str,
            req: storage::LogTruncateRequest,
        ) -> anyhow::Result<storage::LogTruncateOutcome> {
            self.inner.log_truncate(ns, key, req).await
        }
        async fn log_stream_meta(
            &self,
            ns: &str,
            key: &str,
        ) -> anyhow::Result<storage::LogStreamMeta> {
            self.inner.log_stream_meta(ns, key).await
        }
        async fn log_read_meta_since(
            &self,
            ns: &str,
            key: &str,
            since_seq: u64,
            limit: usize,
        ) -> anyhow::Result<Vec<storage::LogEntryMeta>> {
            self.inner
                .log_read_meta_since(ns, key, since_seq, limit)
                .await
        }
        async fn slot_upsert(
            &self,
            ns: &str,
            key: &str,
            state: &[u8],
        ) -> anyhow::Result<u64> {
            self.inner.slot_upsert(ns, key, state).await
        }
        async fn slot_get(
            &self,
            ns: &str,
            key: &str,
        ) -> anyhow::Result<Option<(u64, Vec<u8>)>> {
            self.inner.slot_get(ns, key).await
        }
        async fn slot_cas(
            &self,
            ns: &str,
            key: &str,
            expected_version: u64,
            state: &[u8],
        ) -> anyhow::Result<bool> {
            self.inner.slot_cas(ns, key, expected_version, state).await
        }
        async fn slot_create(
            &self,
            ns: &str,
            key: &str,
            state: &[u8],
        ) -> anyhow::Result<Option<u64>> {
            self.inner.slot_create(ns, key, state).await
        }
        async fn slot_delete(
            &self,
            ns: &str,
            key: &str,
        ) -> anyhow::Result<bool> {
            self.inner.slot_delete(ns, key).await
        }
        async fn slot_list(
            &self,
            ns: &str,
            prefix: Option<&str>,
            cursor: Option<&str>,
            limit: usize,
        ) -> anyhow::Result<storage::SlotPage> {
            self.inner.slot_list(ns, prefix, cursor, limit).await
        }
        async fn queue_push(
            &self,
            ns: &str,
            key: &str,
            value: &[u8],
        ) -> anyhow::Result<u64> {
            self.inner.queue_push(ns, key, value).await
        }
        async fn queue_peek(
            &self,
            ns: &str,
            key: &str,
            limit: usize,
        ) -> anyhow::Result<Vec<(u64, Vec<u8>)>> {
            self.inner.queue_peek(ns, key, limit).await
        }
        async fn queue_ack(
            &self,
            ns: &str,
            key: &str,
            up_to_id: u64,
        ) -> anyhow::Result<usize> {
            self.inner.queue_ack(ns, key, up_to_id).await
        }
        async fn queue_len(&self, ns: &str, key: &str) -> anyhow::Result<u64> {
            self.inner.queue_len(ns, key).await
        }
        async fn queue_drop_oldest(
            &self,
            ns: &str,
            key: &str,
            count: usize,
        ) -> anyhow::Result<usize> {
            self.inner.queue_drop_oldest(ns, key, count).await
        }
    }

    #[tokio::test]
    async fn fetch_identity_verified_fails_closed_when_query_unavailable() {
        // The live identity query cannot reach the server: after bounded retries
        // this must return an error, never a silently-absent identity that would
        // let the stream open on an unknown lineage.
        let err = fetch_pg_lineage_verified(UNREACHABLE_DSN)
            .await
            .expect_err("unreachable server must fail closed");
        let msg = err.to_string();
        assert!(
            msg.contains("refusing to stream"),
            "error should refuse to stream, got: {msg}"
        );
    }

    /// A `pg_different_cluster` incident around a typed lineage error, with
    /// bounded evidence and actionable recommendations.
    fn is_cluster_refusal(r: &SourceResult<()>) -> bool {
        use deltaforge_core::incident::{ActionCode, ReasonCode, SafetyState};
        let Err(e) = r else { return false };
        let Some(d) = e.draft() else { return false };
        matches!(e.root(), SourceError::Lineage { .. })
            && d.reason_code == ReasonCode::PgDifferentCluster
            && d.safety_state == SafetyState::HaltedSafe
            && d.actions.contains(&ActionCode::UseNewSourceId)
            && !d.evidence.is_empty()
            && d.evidence.len()
                <= deltaforge_core::incident::MAX_EVIDENCE_ENTRIES
    }

    fn continuity_class(r: &SourceResult<()>) -> (String, String) {
        use deltaforge_core::incident::{EvidenceKey, EvidenceValue};
        let d = r.as_ref().unwrap_err().draft().expect("an incident");
        assert_eq!(
            d.reason_code,
            deltaforge_core::incident::ReasonCode::PgContinuityUnproven
        );
        assert_eq!(
            d.safety_state,
            deltaforge_core::incident::SafetyState::HaltedSafe
        );
        let class = match d.evidence.get(EvidenceKey::ReasonClass) {
            Some(EvidenceValue::Text { value }) => value.clone(),
            other => panic!("{other:?}"),
        };
        (class, d.retryability.as_str().to_string())
    }

    fn dead_dsn() -> String {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        drop(l);
        format!("host=127.0.0.1 port={port} user=u dbname=d")
    }

    fn memory_incidents() -> IncidentStore {
        IncidentStore::new(Arc::new(MemoryStorageBackend::new()), "p")
    }

    /// An unreachable server (a plausibly transient cause) is retried until
    /// the window ends, then stops before replication opens. While it
    /// retries the condition is an open auto-retry incident; the stop turns
    /// that same incident into operator action (the injected window stands
    /// in for the two-minute budget).
    #[tokio::test]
    async fn an_exhausted_retry_turns_the_same_incident_into_operator_action() {
        let incidents = memory_incidents();
        let dsn = dead_dsn();
        let started = Instant::now();
        let task = {
            let incidents = incidents.clone();
            tokio::spawn(async move {
                verify_resume_position(
                    &dsn,
                    "src",
                    "slot",
                    "0/16B3748",
                    &CancellationToken::new(),
                    Duration::from_secs(3),
                    &incidents,
                )
                .await
            })
        };
        let retrying = loop {
            if let Some(rec) = incidents.list().await.unwrap().pop() {
                break rec;
            }
            assert!(!task.is_finished(), "recorded while it retries");
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        assert!(!task.is_finished());
        assert_eq!(retrying.retryability.as_str(), "auto_retry");
        assert_eq!(retrying.safety_state, SafetyState::HaltedSafe);

        let r = task.await.unwrap();
        assert!(started.elapsed() >= Duration::from_secs(1), "it retried");
        assert_eq!(
            continuity_class(&r),
            ("unknown_unreachable".into(), "operator_action".into())
        );
        // The supervisor records the stop bound to the recovery epoch: the
        // same incident, now operator action.
        let stopped = storage::adapters::incidents::bind_epoch(
            r.unwrap_err().draft().unwrap().clone(),
            incidents.recovery_epoch().await.unwrap(),
        );
        assert_eq!(stopped.incident_id("p"), retrying.incident_id);
        incidents.raise(&stopped, 1).await.unwrap();
        let all = incidents.list().await.unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].incident_id, retrying.incident_id);
        assert_eq!(all[0].retryability.as_str(), "operator_action");
        assert_eq!(all[0].safety_state, SafetyState::HaltedSafe);
    }

    /// Cancellation during the retry window stops promptly, without an
    /// operator incident: the auto-retry incident is withdrawn as
    /// `operation_cancelled`, so nothing blocks a later start.
    #[tokio::test]
    async fn cancellation_during_the_retry_stops_without_an_operator_incident()
    {
        let incidents = memory_incidents();
        let cancel = CancellationToken::new();
        let dsn = dead_dsn();
        let task = {
            let (incidents, cancel) = (incidents.clone(), cancel.clone());
            tokio::spawn(async move {
                verify_resume_position(
                    &dsn,
                    "src",
                    "slot",
                    "0/16B3748",
                    &cancel,
                    Duration::from_secs(120),
                    &incidents,
                )
                .await
            })
        };
        while incidents.list().await.unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        cancel.cancel();
        let r = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("prompt")
            .unwrap();
        assert!(matches!(r, Err(SourceError::Cancelled)), "{r:?}");
        crate::incident_drafts::test_util::assert_cancelled_withdrawn(
            &incidents,
        )
        .await;
    }

    /// A server that answers with an error (not transient) stops at once.
    #[tokio::test]
    async fn a_server_error_answer_halts_without_retrying() {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            while let Ok((mut sock, _)) = l.accept().await {
                let mut buf = [0u8; 1024];
                let _ = sock.read(&mut buf).await;
                // ErrorResponse: FATAL 28000 (invalid authorization).
                let mut body = Vec::new();
                for (k, v) in
                    [(b'S', "FATAL"), (b'C', "28000"), (b'M', "denied")]
                {
                    body.push(k);
                    body.extend_from_slice(v.as_bytes());
                    body.push(0);
                }
                body.push(0);
                let mut msg = vec![b'E'];
                msg.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
                msg.extend_from_slice(&body);
                let _ = sock.write_all(&msg).await;
            }
        });
        let dsn = format!("host=127.0.0.1 port={port} user=u dbname=d");
        let started = Instant::now();
        let r = verify_resume_position(
            &dsn,
            "src",
            "slot",
            "0/16B3748",
            &CancellationToken::new(),
            Duration::from_secs(30),
            &memory_incidents(),
        )
        .await;
        assert!(started.elapsed() < Duration::from_secs(5), "no retry");
        assert_eq!(
            continuity_class(&r),
            ("unknown_query_failed".into(), "operator_action".into())
        );
    }

    #[tokio::test]
    async fn identity_check_fails_closed_when_store_unavailable() {
        // Startup: the durable identity store is down, so the identity comparison
        // cannot be made. We must fail closed rather than assume Same and proceed.
        let store = IdentityStore::new(Arc::new(IdentityStoreDown::new()));
        let result =
            verify_identity_before_connect(&pg_identity(42), &store, "src1")
                .await;
        assert!(
            result.is_err(),
            "unavailable identity store must fail closed"
        );
    }

    #[tokio::test]
    async fn a_changed_identity_is_refused_before_connect() {
        // A different identity is already recorded: the live server is another
        // cluster. It is refused, never resumed from any position.
        let backend = Arc::new(MemoryStorageBackend::new());
        let store = IdentityStore::new(backend);
        store
            .store("src1", &pg_identity(111))
            .await
            .expect("seed previous identity");
        let result =
            verify_identity_before_connect(&pg_identity(222), &store, "src1")
                .await;
        assert!(
            is_cluster_refusal(&result),
            "another cluster must be refused: {result:?}"
        );
        assert!(
            matches!(
                store.compare("src1", &pg_identity(111)).await.unwrap(),
                IdentityComparison::Same
            ),
            "the recorded identity must not move"
        );
    }

    #[tokio::test]
    async fn identity_check_fails_closed_when_firstseen_persist_fails() {
        // Fresh source (FirstSeen): the identity read succeeds but the durable
        // write fails. The FirstSeen identity MUST be persisted before the stream
        // opens, so a persist failure fails closed here and no stream is ever
        // opened.
        let store =
            IdentityStore::new(Arc::new(IdentityStoreDown::write_only_down()));
        let result =
            verify_identity_before_connect(&pg_identity(99), &store, "src_new")
                .await;
        assert!(
            result.is_err(),
            "FirstSeen persist failure must fail closed before opening the stream"
        );
    }

    /// Either durable authority alone refuses another cluster, before
    /// anything is written: the lineage record (another cluster or a replaced
    /// database) and the identity store (another cluster).
    #[tokio::test]
    async fn another_cluster_is_refused_by_either_authority() {
        fn live(sysid: i64, dboid: u64) -> VerifiedPgLineage {
            VerifiedPgLineage {
                descriptor: LineageDescriptor::postgres(sysid as u64, dboid)
                    .unwrap(),
                identity: PostgresServerIdentity {
                    system_identifier: sysid,
                },
            }
        }
        let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
        refuse_server_change(&backend, "acme", "src", &live(1, 5))
            .await
            .expect("nothing recorded: first use");
        storage::adapters::source_lineage::establish(
            &backend,
            "acme",
            "src",
            live(1, 5).descriptor,
        )
        .await
        .unwrap();
        refuse_server_change(&backend, "acme", "src", &live(1, 5))
            .await
            .expect("the same server");
        for other in [live(2, 5), live(1, 6)] {
            let r = refuse_server_change(&backend, "acme", "src", &other).await;
            assert!(is_cluster_refusal(&r), "{other:?} must be refused: {r:?}");
        }

        // No lineage record, only a recorded identity.
        let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
        IdentityStore::new(Arc::clone(&backend))
            .store("src", &pg_identity(1))
            .await
            .unwrap();
        let r =
            refuse_server_change(&backend, "acme", "src", &live(2, 5)).await;
        assert!(is_cluster_refusal(&r), "{r:?}");
        refuse_server_change(&backend, "acme", "src", &live(1, 5))
            .await
            .expect("the recorded identity");
    }

    #[tokio::test]
    async fn identity_store_persist_failure_fails_closed() {
        // The persistence step after reconciliation (and the FirstSeen store at
        // startup) relies on IdentityStore::store surfacing backend errors. A
        // failed durable write must propagate, never be discarded.
        let store = IdentityStore::new(Arc::new(IdentityStoreDown::new()));
        let result = store.store("src1", &pg_identity(7)).await;
        assert!(
            result.is_err(),
            "a failed identity persist must propagate, not be swallowed"
        );
    }
}
