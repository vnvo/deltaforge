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
use deltaforge_core::{
    CheckpointOrder, Source, SourceError, SourceHandle, SourceItem,
    SourceResult,
};
use storage::BackendCheckpointStore;

use crate::snapshot_generation::PersistedLineage;
use postgres_snapshot::IdentitySpec;

mod postgres_errors;
use postgres_errors::LoopControl;
pub use postgres_errors::{PostgresSourceError, PostgresSourceResult};

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
use crate::failover::reconciler::{ReconcileInput, SchemaReconciler};
use crate::postgres::postgres_health::{
    PositionReachability, PostgresServerIdentity, check_position_reachability,
};
use crate::registry_scope::{
    RegistryError, ScopeChange, SharedRegistryScope, establish_scope,
    previous_scope,
};
use storage::adapters::LineageDescriptor;

// ============================================================================
// Checkpoint
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresCheckpoint {
    pub lsn: String,
    pub tx_id: Option<u32>,
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
    pub slot: String,
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
    pub reconciler: SchemaReconciler,
    /// Verified-lineage registry scope (shared with the pipeline's loaders).
    pub registry_scope: SharedRegistryScope,
    /// Backend holding the durable source-lineage record.
    pub registry_backend: ArcStorageBackend,
    pub on_schema_drift: OnSchemaDrift,
    /// Cached metrics counter handles keyed by (qualified_table_name, op).
    /// Avoids hash-lookup + key-comparison in the metrics registry per event.
    pub counter_cache: HashMap<(Arc<str>, &'static str), metrics::Counter>,
    /// Cached LSN string to avoid re-formatting the same LSN on consecutive events.
    pub cached_lsn: Option<(Lsn, String)>,
}

// ============================================================================
// Source Implementation
// ============================================================================

const MAX_STARTUP_BACKOFF_SECS: u64 = 60;

/// Outcome of the pre-snapshot validate/allocate flow.
struct SnapshotPlan {
    generation: u64,
    lineage: PersistedLineage,
    /// `schema.table` → resolved identity columns (name + kind).
    identity_map: HashMap<String, Vec<IdentitySpec>>,
}

impl PostgresSource {
    /// Validate every selected table's identity (resolution + catalog type
    /// support), freeze the cluster lineage, compute the config fingerprint, and
    /// atomically allocate (or resume) the snapshot generation - all **before**
    /// any row is emitted. Keyless tables or unsupported identity types fail
    /// here, before allocation.
    async fn prepare_snapshot_generation(
        &self,
        loader: &PostgresSchemaLoader,
        tracked: &[(String, String)],
    ) -> SourceResult<SnapshotPlan> {
        use crate::identity_resolution::{
            IdentitySchemaView, resolve_identity,
        };
        use crate::snapshot_generation::{
            AllocationMode, SnapshotConfigFingerprint, TableIdentitySpec,
            allocate_generation,
        };
        use tokio_postgres::NoTls;

        // One catalog connection for identity-kind resolution + lineage.
        let (client, conn) = tokio_postgres::connect(self.dsn.expose(), NoTls)
            .await
            .map_err(|e| SourceError::Other(e.into()))?;
        let conn_task = tokio::spawn(async move {
            let _ = conn.await;
        });

        let mut specs: Vec<TableIdentitySpec> =
            Vec::with_capacity(tracked.len());
        let mut identity_map: HashMap<String, Vec<IdentitySpec>> =
            HashMap::new();

        for (schema, table) in tracked {
            let loaded = loader.load_schema(schema, table).await?;
            let s = &loaded.schema;
            let col_names: Vec<String> =
                s.columns.iter().map(|c| c.name.clone()).collect();
            let fqn = format!("{schema}.{table}");
            let opts = self.table_options.get(&fqn);
            let view = IdentitySchemaView {
                columns: &col_names,
                primary_key: &s.primary_key,
                // Unique constraints are not captured in the schema today;
                // non-PK identity columns require assume_unique.
                unique_constraints: &[],
            };
            let resolved = resolve_identity(
                schema,
                table,
                &view,
                opts.and_then(|o| o.identity_columns.as_deref()),
                opts.map(|o| o.assume_unique).unwrap_or(false),
            )
            .map_err(|e| SourceError::Other(anyhow::anyhow!(e)))?;

            // Resolve + validate the identity column types from the catalog
            // (rejects unsupported types before allocation).
            let kinds = resolve_identity_kinds(
                &client,
                schema,
                table,
                &resolved.columns,
            )
            .await
            .map_err(SourceError::Other)?;

            specs.push(TableIdentitySpec {
                db: schema.clone(),
                schema: Some(schema.clone()),
                table: table.clone(),
                identity_columns: resolved.columns.clone(),
            });
            identity_map.insert(
                fqn,
                kinds
                    .into_iter()
                    .map(|(name, kind)| IdentitySpec { name, kind })
                    .collect(),
            );
        }

        // Freeze lineage from the existing system_identifier authority.
        let lineage = self.capture_snapshot_lineage(&client).await?;
        conn_task.abort();

        let fingerprint = SnapshotConfigFingerprint::compute(&specs);
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

        info!(
            source_id = %self.id,
            generation = alloc.record.generation,
            tables = tracked.len(),
            "snapshot generation allocated"
        );
        Ok(SnapshotPlan {
            generation: alloc.record.generation,
            lineage: alloc.record.lineage,
            identity_map,
        })
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

        let (components, config, last_checkpoint) = prepare_replication_client(
            self.dsn.expose(),
            &self.id,
            &self.slot,
            &self.publication,
            &chkpt_store,
        )
        .await?;

        let mut startup_retry = RetryPolicy::default();

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
            // Resuming from a checkpoint: verify the replication slot still exists
            // before trusting the saved LSN. A dropped slot means the WAL position
            // is permanently lost - halt rather than silently reconnecting.
            match check_position_reachability(self.dsn.expose(), &self.slot)
                .await
            {
                Ok(PositionReachability::Lost { reason }) => {
                    error!(
                        source_id = %self.id,
                        slot = %self.slot,
                        %reason,
                        "replication slot lost - checkpoint position is unreachable, halting"
                    );
                    return Err(SourceError::Checkpoint {
                        details: format!(
                            "replication slot '{}' is gone: {reason}. \
                             Re-snapshot required.",
                            self.slot
                        )
                        .into(),
                    });
                }
                Ok(PositionReachability::Unknown { reason }) => {
                    warn!(
                        source_id = %self.id,
                        slot = %self.slot,
                        %reason,
                        "could not verify slot reachability, resuming anyway"
                    );
                }
                Ok(PositionReachability::Reachable) => {}
                Err(e) => {
                    warn!(
                        source_id = %self.id,
                        slot = %self.slot,
                        error = %e,
                        "slot reachability check failed, resuming anyway"
                    );
                }
            }
            config.start_lsn
        };

        let schema_loader = PostgresSchemaLoader::new(
            self.dsn.clone(),
            self.registry.clone(),
            &self.tenant,
            self.registry_scope.clone(),
        );
        let tracked = schema_loader.preload(&self.tables).await?;
        info!(tables = tracked.len(), "schemas preloaded");

        for (schema, table) in &tracked {
            if let Ok(loaded) = schema_loader.load_schema(schema, table).await {
                if let Some(ref identity) = loaded.schema.replica_identity {
                    if identity != "full" {
                        warn!(
                            schema = %schema, table = %table,
                            replica_identity = %identity,
                            "table does not have REPLICA IDENTITY FULL - before images will be incomplete"
                        );
                    }
                }
            }
        }

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

            // An explicit re-snapshot re-scans every table: reset table-level
            // progress so completed tables are not skipped (generation is
            // separately bumped via ForceNew below). Fail closed if the reset
            // does not persist - snapshotting on stale completed-table progress
            // would skip tables and reintroduce loss.
            if self.snapshot_cfg.mode == SnapshotMode::Always {
                postgres_slot_owner::reset_snapshot_progress(
                    &chkpt_store,
                    &self.id,
                )
                .await
                .map_err(SourceError::Other)?;
            }

            // Validate every table + freeze lineage + allocate the generation
            // BEFORE emitting any row (keyless/unsupported tables fail here).
            let plan = self
                .prepare_snapshot_generation(&schema_loader, &tracked)
                .await?;

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
                identity_map: plan.identity_map,
            };
            postgres_snapshot::run_snapshot(&snapshot_ctx, &tracked, anchor)
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
        // failed over meanwhile). The same verified read is reused for the
        // pre-connect LSN adjustment, the provisional row/DDL/message ids, the
        // initial failover identity check, and the registry-scope comparison.
        // If the lineage cannot be verified we fail closed rather than open
        // replication on an unknown server.
        let stream_lineage =
            fetch_pg_lineage_verified(self.dsn.expose()).await?;
        let system_identifier =
            stream_lineage.identity.system_identifier as u64;
        let startup_server_identity =
            ServerIdentity::from(stream_lineage.identity.clone());

        // Adjust start_lsn BEFORE opening the replication stream.
        // If a failover has occurred, START_REPLICATION with A's stale LSN would
        // advance B's slot.confirmed_flush_lsn past B's uncommitted changes, making
        // them permanently invisible even if we reconnect from the correct LSN later.
        let start_lsn = {
            let id_store = IdentityStore::new(Arc::clone(&self.backend));
            pre_connect_lsn_adjust(
                self.dsn.expose(),
                &self.slot,
                start_lsn,
                &startup_server_identity,
                &id_store,
                &self.id,
            )
            .await?
        };

        info!(
            source_id = %self.id, host = %components.host, slot = %self.slot,
            publication = %self.publication, start_lsn = %start_lsn,
            "postgres source starting"
        );

        let config = config.with_start_lsn(start_lsn);

        let client = connect_replication_with_retries(
            &self.id,
            config.clone(),
            &cancel,
            RetryPolicy::default(),
        )
        .await?;

        let backend = Arc::clone(&self.backend);
        let cancel_ref = cancel.clone();
        let mut ctx = RunCtx {
            source_id: self.id.clone(),
            pipeline: self.pipeline.clone(),
            tenant: self.tenant.clone(),
            host: components.host.clone(),
            default_schema: "public".to_string(),
            dsn: self.dsn.clone(),
            slot: self.slot.clone(),
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
            current_tx_id: None,
            current_tx_commit_time: None,
            current_final_lsn: None,
            change_ordinal: 0,
            system_identifier,
            message_ordinal: 0,
            repl_client: Arc::new(Mutex::new(client)),
            outbox_prefixes: self.outbox_prefixes.clone(),
            identity_store: IdentityStore::new(Arc::clone(&backend)),
            reconciler: SchemaReconciler::new(
                Arc::clone(&self.registry),
                Arc::clone(&backend),
            ),
            registry_scope: self.registry_scope.clone(),
            registry_backend: Arc::clone(&backend),
            on_schema_drift: self.on_schema_drift.clone(),
            counter_cache: HashMap::new(),
            cached_lsn: None,
        };

        // Store initial server identity (FirstSeen path). Reuse the lineage
        // already verified above rather than re-fetching it.
        check_identity_post_reconnect(
            &mut ctx,
            Some((stream_lineage.descriptor, startup_server_identity)),
        )
        .await?;

        // If failover was detected, ctx.last_lsn was reset to B's slot position.
        // The existing stream was opened from A's stale LSN - reconnect from the correct point.
        if ctx.last_lsn != start_lsn {
            let reconnect_config = config.clone().with_start_lsn(ctx.last_lsn);
            let new_client = connect_replication_with_retries(
                &self.id,
                reconnect_config,
                &cancel_ref,
                RetryPolicy::default(),
            )
            .await?;
            *ctx.repl_client.lock().await = new_client;
        }

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
                &ctx.cancel,
            )?),
            None => None,
        };

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
                        info!("reloading all schemas");
                        ctx.schema.reload_all(&self.tables).await?;
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
                    )
                    .await?;
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

                    let reconnect_config =
                        config.clone().with_start_lsn(ctx.last_lsn);

                    match connect_replication_with_retries(
                        &self.id,
                        reconnect_config,
                        &ctx.cancel,
                        ctx.retry.clone(),
                    )
                    .await
                    {
                        Ok(new_client) => {
                            *ctx.repl_client.lock().await = new_client;
                            ctx.retry.reset();
                            info!(source_id = %self.id, "reconnected successfully");
                            check_identity_post_reconnect(&mut ctx, None).await?;
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
pub fn compare_pg_checkpoints(a: &[u8], b: &[u8]) -> CheckpointOrder {
    #[derive(serde::Deserialize)]
    struct Cp {
        lsn: String,
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

        let join = tokio::spawn(async move {
            let res = this
                .run_inner(
                    tx,
                    chkpt_store,
                    cancel_for_task,
                    paused_for_task,
                    pause_notify_for_task,
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
        }
    }

    fn compare_checkpoints(&self, a: &[u8], b: &[u8]) -> CheckpointOrder {
        compare_pg_checkpoints(a, b)
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
        if crate::snapshot_frontier::is_ambiguous_legacy_progress(
            &self.tables,
            &progress.done_tables,
            progress.finished,
        ) {
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
    Ok(
        establish_scope(backend, shared, tenant, source_id, lineage.descriptor)
            .await?,
    )
}

/// Bring the published registry scope in line with a lineage just verified
/// against the live server, before any further registry access.
///
/// - Unchanged: nothing to do (loader caches stay warm).
/// - A different cluster (`system_identifier` changed): a failover to another
///   server; the new lineage is durably recorded and published, which gives it
///   a fresh schema namespace and invalidates caches from the old lineage.
///   Failure to persist fails closed.
/// - Same cluster but a different database OID: the database was replaced under
///   the running source. That is not a failover; fail closed.
async fn sync_registry_lineage(
    ctx: &RunCtx,
    live: &LineageDescriptor,
) -> SourceResult<()> {
    let current = ctx.registry_scope.current()?;
    let expected = &current.lineage().descriptor;
    match lineage_transition(expected, live) {
        LineageTransition::Same => return Ok(()),
        LineageTransition::ReplacedDatabase => {
            return Err(RegistryError::LineageMismatch {
                source_id: ctx.source_id.clone(),
                expected: format!("{expected:?}"),
                live: format!("{live:?}"),
            }
            .into());
        }
        LineageTransition::Failover => {}
    }
    let change = establish_scope(
        &ctx.registry_backend,
        &ctx.registry_scope,
        &ctx.tenant,
        &ctx.source_id,
        live.clone(),
    )
    .await?;
    warn!(
        source_id = %ctx.source_id,
        lineage = %change.scope.lineage().lineage_hash,
        "source lineage changed; schema registry re-scoped to the new server"
    );
    Ok(())
}

/// How a running PostgreSQL source's verified lineage moved.
#[derive(Debug, PartialEq, Eq)]
enum LineageTransition {
    Same,
    /// A different cluster: failover to another server (supported).
    Failover,
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
        _ => LineageTransition::Failover,
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
    fn different_cluster_is_a_failover() {
        assert_eq!(
            lineage_transition(&pg(1, 5), &pg(2, 5)),
            LineageTransition::Failover
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

/// Resolves the cluster identity and the `start_lsn` **before** opening the
/// replication stream. Compares the verified live identity against the durable
/// authority and acts before any stream opens:
///
/// - `FirstSeen`: persist the verified identity. A durable-write failure fails
///   closed here so no stream is ever opened on an unpersisted identity.
/// - `Same`: nothing to do.
/// - `Changed`: adjust `start_lsn` to B's slot position (the schema
///   reconciliation runs post-connect but before any row is consumed).
///
/// `START_REPLICATION` immediately advances the slot's `confirmed_flush_lsn` to
/// `max(start_lsn, slot.confirmed_flush_lsn)`.  If we start with A's stale checkpoint
/// LSN on a fresh B whose slot is behind that checkpoint, PostgreSQL will skip any
/// changes B committed between its slot creation LSN and A's checkpoint - even if we
/// reconnect from the correct LSN afterwards.
///
/// By fetching the correct start LSN before the first replication connection, we avoid
/// permanently advancing the slot past unread data. `live` is the identity already
/// verified once at startup; the comparison, persistence, and any position lookup
/// fail closed.
async fn pre_connect_lsn_adjust(
    dsn: &str,
    slot: &str,
    start_lsn: Lsn,
    live: &ServerIdentity,
    id_store: &IdentityStore,
    source_id: &str,
) -> SourceResult<Lsn> {
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
                .map_err(SourceError::Other)?;
            Ok(start_lsn)
        }
        IdentityComparison::Changed { .. } => {
            // Failover detected: use the slot's actual confirmed_flush_lsn on B.
            // If B's slot position cannot be resolved we must NOT fall back to
            // A's stale checkpoint LSN - doing so would advance B's slot past
            // unread data. The identity authority is unavailable, so fail closed.
            let slot_lsn =
                fetch_slot_confirmed_lsn(dsn, slot).await.map_err(|e| {
                    SourceError::Other(anyhow::anyhow!(
                        "failover detected but could not resolve slot '{slot}' \
                         confirmed_flush_lsn on the new server: {e}; refusing to \
                         open replication on a stale checkpoint LSN"
                    ))
                })?;
            debug!(
                source_id = %source_id,
                original_lsn = %start_lsn,
                slot_lsn = %slot_lsn,
                "pre-connect failover adjustment: using slot LSN"
            );
            Ok(slot_lsn)
        }
        _ => Ok(start_lsn),
    }
}

async fn check_identity_post_reconnect(
    ctx: &mut RunCtx,
    prefetched: Option<(LineageDescriptor, ServerIdentity)>,
) -> SourceResult<()> {
    // Reuse a lineage already verified by the caller (startup), or fetch and
    // verify one here (reconnect). Either way a live identity is required: an
    // unverifiable identity fails closed rather than silently skipping the
    // failover check.
    let (descriptor, live) = match prefetched {
        Some(pair) => pair,
        None => {
            let verified = fetch_pg_lineage_verified(ctx.dsn.expose()).await?;
            (verified.descriptor, ServerIdentity::from(verified.identity))
        }
    };
    // Re-scope the registry to the live lineage before any registry access on
    // this connection (fails closed on a replaced database or a persist error).
    sync_registry_lineage(ctx, &descriptor).await?;

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
            warn!(
                source_id = %ctx.source_id,
                prev = ?previous,
                new = ?current,
                "server identity changed - failover detected, reconciling"
            );
            run_failover_reconciliation(ctx, previous, current).await?;
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
            Err(SourceError::Schema {
                details: format!(
                    "schema drift on table \"{}.{}\" ({}) and on_schema_drift=halt. \
                     No events under the changed schema were emitted and the \
                     checkpoint was not advanced. Review the schema change; to \
                     continue past it, restart with on_schema_drift=adapt.",
                    drift.schema, drift.table, drift.detail
                )
                .into(),
            })
        }
    }
}

async fn run_failover_reconciliation(
    ctx: &mut RunCtx,
    previous: ServerIdentity,
    current: ServerIdentity,
) -> SourceResult<()> {
    let existing = ctx
        .reconciler
        .already_completed(&ctx.source_id, &previous, &current)
        .await
        .unwrap_or(None);

    if existing.is_none() {
        // Position reachability via slot state.
        match check_position_reachability(ctx.dsn.expose(), &ctx.slot)
            .await
            .unwrap_or(PositionReachability::Unknown {
                reason: "reachability check failed".into(),
            }) {
            PositionReachability::Reachable => {}
            PositionReachability::Unknown { reason } => {
                warn!(
                    source_id = %ctx.source_id,
                    %reason,
                    "could not verify position reachability after failover - resuming anyway"
                );
            }
            PositionReachability::Lost { reason } => {
                return Err(SourceError::Other(anyhow::anyhow!(
                    "position lost after failover: {reason}. Re-snapshot required."
                )));
            }
        }

        if let Ok(slot_lsn) =
            fetch_slot_confirmed_lsn(ctx.dsn.expose(), &ctx.slot).await
        {
            ctx.last_lsn = slot_lsn;
        }

        // Schema diff against live catalog.
        let tracked = ctx.schema.cached_tables();
        let mut inputs = Vec::with_capacity(tracked.len());
        for (schema, table) in &tracked {
            let live_cols: Option<
                Vec<crate::failover::reconciler::ColumnSnapshot>,
            > = postgres_health::fetch_live_columns(
                ctx.dsn.expose(),
                schema,
                table,
            )
            .await
            .ok()
            .flatten()
            .map(|cols| cols.into_iter().map(Into::into).collect());
            inputs.push(ReconcileInput {
                db: schema.clone(),
                table: table.clone(),
                live_columns: live_cols,
            });
        }

        // Diff against the last-known schemas of the lineage the source ran
        // under before this failover (read explicitly from its namespace).
        let prior =
            previous_scope(&ctx.registry_backend, &ctx.tenant, &ctx.source_id)
                .await?;
        let record = ctx
            .reconciler
            .run(&ctx.source_id, &previous, &current, prior.as_ref(), &inputs)
            .await
            .map_err(SourceError::Other)?;

        for result in &record.table_results {
            if !result.deltas.is_empty() {
                let _ =
                    ctx.schema.reload_schema(&result.db, &result.table).await;
            }
        }

        let has_drift =
            record.table_results.iter().any(|r| !r.deltas.is_empty());
        if has_drift {
            warn!(pipeline=%ctx.pipeline, source_id=%ctx.source_id, "schema drift detected after failover");
            if ctx.on_schema_drift == deltaforge_config::OnSchemaDrift::Halt {
                return Err(SourceError::Other(anyhow::anyhow!(
                    "schema drift detected after failover and on_schema_drift=halt. \
                Verify B's schema and apply any missing migrations before restarting."
                )));
            }
        }
    }

    // Persist the new identity only after reconciliation succeeds. A failure to
    // record the new lineage must fail closed: silently discarding it would let
    // the next reconnect re-detect the same "change" or, worse, treat a later
    // failover as first-seen.
    ctx.identity_store
        .store(&ctx.source_id, &current)
        .await
        .map_err(SourceError::Other)?;

    info!(source_id = %ctx.source_id, "failover reconciliation complete");
    Ok(())
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
    use super::compare_pg_checkpoints;
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
        let err = apply_schema_drift(&l, &OnSchemaDrift::Halt, &drift())
            .await
            .expect_err("halt must fail closed");
        let msg = err.to_string();
        assert!(
            msg.contains("public.orders")
                && msg.contains("on_schema_drift=adapt"),
            "error names the table and remediation: {msg}"
        );
    }

    #[tokio::test]
    async fn adapt_fails_closed_when_reload_fails() {
        // Unreachable DSN: reload_schema -> load_schema -> connect fails, so Adapt
        // must NOT continue - it fails closed instead of proceeding with an
        // unverified schema.
        let l = loader("host=127.0.0.1 port=1 dbname=x").await;
        let r = apply_schema_drift(&l, &OnSchemaDrift::Adapt, &drift()).await;
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
    //! `pre_connect_lsn_adjust`) directly. Container-backed failover behaviour is
    //! covered by `tests/failover_e2e.rs`; here we prove that when the identity
    //! store or the live position authority is unavailable, we refuse to open the
    //! stream and never fall back to a stale LSN.
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

    #[tokio::test]
    async fn pre_connect_lsn_adjust_fails_closed_when_store_unavailable() {
        // Startup: the durable identity store is down, so the identity comparison
        // cannot be made. We must fail closed rather than assume Same and proceed.
        let store = IdentityStore::new(Arc::new(IdentityStoreDown::new()));
        let start = Lsn::from(0x1000u64);
        let result = pre_connect_lsn_adjust(
            UNREACHABLE_DSN,
            "slot_a",
            start,
            &pg_identity(42),
            &store,
            "src1",
        )
        .await;
        assert!(
            result.is_err(),
            "unavailable identity store must fail closed, not return an LSN"
        );
    }

    #[tokio::test]
    async fn pre_connect_lsn_adjust_never_falls_back_to_old_lsn_on_change() {
        // A different identity is already recorded, so the live server is a
        // failover target (Changed). The new server's slot position cannot be
        // resolved (unreachable). We must NOT fall back to A's stale checkpoint
        // LSN - doing so would advance B's slot past unread data.
        let backend = Arc::new(MemoryStorageBackend::new());
        let store = IdentityStore::new(backend);
        store
            .store("src1", &pg_identity(111))
            .await
            .expect("seed previous identity");

        let stale = Lsn::from(0xDEAD_BEEFu64);
        let result = pre_connect_lsn_adjust(
            UNREACHABLE_DSN,
            "slot_a",
            stale,
            &pg_identity(222), // different -> Changed
            &store,
            "src1",
        )
        .await;
        match result {
            Err(_) => {}
            Ok(lsn) => panic!(
                "identity changed but position authority was unavailable; \
                 must fail closed, instead returned {lsn}"
            ),
        }
    }

    #[tokio::test]
    async fn pre_connect_lsn_adjust_fails_closed_when_firstseen_persist_fails()
    {
        // Fresh source (FirstSeen): the identity read succeeds but the durable
        // write fails. The FirstSeen identity MUST be persisted before the stream
        // opens, so a persist failure fails closed here - no LSN is returned and
        // therefore no stream is ever opened.
        let store =
            IdentityStore::new(Arc::new(IdentityStoreDown::write_only_down()));
        let start = Lsn::from(0x2000u64);
        let result = pre_connect_lsn_adjust(
            UNREACHABLE_DSN,
            "slot_a",
            start,
            &pg_identity(99),
            &store,
            "src_new",
        )
        .await;
        assert!(
            result.is_err(),
            "FirstSeen persist failure must fail closed before opening the stream"
        );
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
