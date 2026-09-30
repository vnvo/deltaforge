use std::{
    collections::HashMap,
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

use async_trait::async_trait;
use deltaforge_config::SnapshotMode;
use mysql_binlog_connector_rust::{
    binlog_client::BinlogClient, binlog_stream::BinlogStream,
    event::table_map_event::TableMapEvent,
};
use serde::{Deserialize, Serialize};
use storage::{ArcStorageBackend, DurableSchemaRegistry};
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use checkpoints::{CheckpointStore, CheckpointStoreExt};
use common::{AllowList, RetryPolicy, pause_until_resumed};
use storage::BackendCheckpointStore;

use crate::snapshot_generation::PersistedLineage;
use deltaforge_core::{
    CheckpointOrder, Source, SourceError, SourceHandle, SourceItem,
    SourceResult,
};
mod mysql_errors;
pub use mysql_errors::{LoopControl, MySqlSourceError, MySqlSourceResult};

mod mysql_helpers;
use mysql_helpers::prepare_client;

pub mod mysql_object;

mod mysql_schema_loader;
pub use mysql_schema_loader::{LoadedSchema, MySqlSchemaLoader};

pub mod mysql_rotation;

mod mysql_event;

pub mod mysql_event_id;
use mysql_event::*;
pub use mysql_event_id::mysql_row_event_id;

pub mod mysql_identity;
pub use mysql_identity::{
    MysqlIdentityError, mysql_identity_cell, mysql_identity_kind,
};

mod mysql_table_map_check;
mod mysql_table_schema;
use crate::mysql::mysql_helpers::{
    connect_binlog_with_retries, resolve_binlog_tail,
};
pub use mysql_table_schema::{MySqlColumn, MySqlTableSchema};

pub mod mysql_snapshot;
pub use mysql_snapshot::{MysqlSnapshotProgress, progress_key};

pub mod mysql_health;

use crate::failover::identity::{
    IdentityComparison, IdentityStore, ServerIdentity,
};
use crate::failover::reconciler::{ReconcileInput, SchemaReconciler};
use crate::mysql::mysql_health::{
    PositionReachability, check_position_reachability, fetch_server_identity,
};
use crate::registry_scope::{
    ScopeChange, SharedRegistryScope, establish_scope, previous_scope,
};
use storage::adapters::LineageDescriptor;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MySqlCheckpoint {
    pub file: String,
    pub pos: u64,
    pub gtid_set: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MySqlSource {
    pub id: String,
    /// Protected connection DSN (redacted `Debug`, no serialization). Built at
    /// startup from inline config or resolved secret references.
    pub dsn: crate::credentials::ProtectedDsn,
    pub tables: Vec<String>,
    pub tenant: String,
    pub pipeline: String,
    pub registry: Arc<DurableSchemaRegistry>,
    /// Verified-lineage scope shared with the pipeline's schema loaders. The
    /// source establishes it before any registry access.
    pub registry_scope: crate::registry_scope::SharedRegistryScope,
    pub backend: ArcStorageBackend,
    pub outbox_tables: AllowList,
    pub snapshot_cfg: deltaforge_config::SnapshotCfg,
    pub on_schema_drift: deltaforge_config::OnSchemaDrift,
    /// Per-table options (identity_columns, assume_unique), keyed by
    /// fully-qualified `db.table`.
    pub table_options:
        std::collections::BTreeMap<String, deltaforge_config::TableOptions>,
    /// Controlled credential-rotation spec, when configured and file-backed.
    /// `None` disables rotation for this source. Requires GTID mode.
    pub rotation: Option<Arc<crate::rotation_manager::RotationSpec>>,
}

pub(crate) const HEARTBEAT_INTERVAL_SECS: u64 = 15;
pub(crate) const READ_TIMEOUT: u64 = 90;

pub(crate) struct RunCtx {
    source_id: String,
    pipeline: String,
    tenant: String,
    dsn: crate::credentials::ProtectedDsn,
    #[allow(dead_code)]
    host: String,
    default_db: String,
    server_id: u64,
    tx: mpsc::Sender<SourceItem>,
    chkpt: Arc<dyn CheckpointStore>,
    cancel: CancellationToken,
    paused: Arc<AtomicBool>,
    pause_notify: Arc<Notify>,
    schema: MySqlSchemaLoader,
    allow: AllowList,
    retry: RetryPolicy,
    inactivity: Duration,
    table_map: HashMap<u64, TableMapEvent>,
    last_file: String,
    last_pos: u64,
    last_gtid: Option<String>,
    /// The current transaction's exact GTID (`uuid:gno`), captured from the GTID
    /// event before it is merged into `last_gtid`'s accumulated executed set.
    /// Used as the immutable per-event identity coordinate.
    current_gtid: Option<String>,
    /// Whether the current GTID transaction has entered an explicit `BEGIN` block
    /// (multi-event transaction ending in `Xid`/`COMMIT`/`ROLLBACK`). A GTID-backed
    /// autocommit statement (DDL, `FLUSH`, ...) never sets this, so it is its own
    /// single-statement transaction closed by its `QueryEvent`. Used to tell a
    /// control statement inside a transaction (e.g. `SAVEPOINT`) from a standalone
    /// autocommit statement.
    in_explicit_txn: bool,
    /// DDL message ordinal: reset per transaction (GTID / BEGIN), incremented
    /// **before filtering** for each DDL so a skipped DDL never renumbers a
    /// retained one.
    message_ordinal: u32,
    /// Original checkpoint position, preserved even after a pre-connect failover
    /// adjustment clears last_gtid/last_file. Used by check_position_reachability
    /// to verify whether A's position actually exists on B.
    checkpoint_gtid: Option<String>,
    checkpoint_file: String,
    tables: Vec<String>,
    outbox_tables: AllowList,
    identity_store: IdentityStore,
    reconciler: SchemaReconciler,
    /// Verified-lineage registry scope (shared with the pipeline's loaders).
    registry_scope: SharedRegistryScope,
    /// Backend holding the durable source-lineage record.
    registry_backend: ArcStorageBackend,
    on_schema_drift: deltaforge_config::OnSchemaDrift,
    /// Frozen source lineage captured once at startup, for durable CDC
    /// watermarks. `None` if lineage could not be resolved (durable mode then
    /// fails closed at the sink; non-durable pipelines are unaffected).
    durable_lineage: Option<PersistedLineage>,
}

/// Outcome of the pre-snapshot validate/allocate flow: the frozen generation,
/// its persisted lineage, and the per-table resolved identity columns.
struct SnapshotPlan {
    generation: u64,
    lineage: PersistedLineage,
    /// `db.table` → resolved identity column names (in identity order).
    identity_map: HashMap<String, Vec<String>>,
}

impl MySqlSource {
    /// Validate every selected table's identity, freeze source lineage, compute
    /// the config fingerprint, and atomically allocate (or resume) the snapshot
    /// generation - all **before** any row is emitted. Keyless tables or
    /// unsupported identity types fail here, before allocation.
    async fn prepare_snapshot_generation(
        &self,
        loader: &MySqlSchemaLoader,
        tracked: &[(String, String)],
        lineage: PersistedLineage,
    ) -> SourceResult<SnapshotPlan> {
        use crate::identity_resolution::{
            IdentitySchemaView, resolve_identity,
        };
        use crate::snapshot_generation::{
            AllocationMode, SnapshotConfigFingerprint, TableIdentitySpec,
            allocate_generation,
        };

        let mut specs: Vec<TableIdentitySpec> =
            Vec::with_capacity(tracked.len());
        let mut identity_map: HashMap<String, Vec<String>> = HashMap::new();

        // Steps 2-4: schema, identity resolution, and type validation for
        // EVERY table - so an invalid table fails before a generation is
        // allocated or any row emitted.
        for (db, table) in tracked {
            let loaded = loader.load_schema(db, table).await?;
            let schema = &loaded.schema;
            let col_names: Vec<String> =
                schema.columns.iter().map(|c| c.name.clone()).collect();
            let fqn = format!("{db}.{table}");
            let opts = self.table_options.get(&fqn);
            let view = IdentitySchemaView {
                columns: &col_names,
                primary_key: &schema.primary_key,
                unique_constraints: &[],
            };
            let resolved = resolve_identity(
                db,
                table,
                &view,
                opts.and_then(|o| o.identity_columns.as_deref()),
                opts.map(|o| o.assume_unique).unwrap_or(false),
            )
            .map_err(|e| SourceError::Other(anyhow::anyhow!(e)))?;
            for name in &resolved.columns {
                let col = schema.column(name).ok_or_else(|| {
                    SourceError::Other(anyhow::anyhow!(
                        "identity column {name:?} vanished from schema"
                    ))
                })?;
                mysql_identity_kind(col)
                    .map_err(|e| SourceError::Other(anyhow::anyhow!(e)))?;
            }
            specs.push(TableIdentitySpec {
                db: db.clone(),
                schema: None,
                table: table.clone(),
                identity_columns: resolved.columns.clone(),
            });
            identity_map.insert(fqn, resolved.columns);
        }

        // Step 5: lineage is captured once by the caller and passed in. Step 6:
        // fingerprint. Step 7: allocate.
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

    /// Freeze the source lineage for snapshot identity: prefer the stable
    /// `@@server_uuid` (file-free, rotation-immune); fall back to
    /// `server_id` + the current binlog file.
    async fn capture_snapshot_lineage(&self) -> SourceResult<PersistedLineage> {
        use mysql_async::{Pool, Row, prelude::Queryable};
        let pool = Pool::new(self.dsn.expose());
        let mut conn = pool
            .get_conn()
            .await
            .map_err(|e| SourceError::Other(e.into()))?;
        let uuid: Option<String> = conn
            .query_first("SELECT @@global.server_uuid")
            .await
            .map_err(|e| SourceError::Other(e.into()))?;
        if let Some(u) = uuid {
            if let Some(bytes) = mysql_event_id::parse_uuid16(&u) {
                return Ok(PersistedLineage::MysqlGtid { source_uuid: bytes });
            }
        }
        // Non-GTID fallback lineage = (server_id, current binlog file). Both must
        // be real: a zero server_id or an empty/absent binlog file is an unverified
        // lineage. The build step returns an error rather than a bogus anchor, and
        // the caller propagates it with `?` at startup, so the source fails closed
        // before opening a stream instead of binding to a meaningless identity.
        let server_id: u32 = conn
            .query_first("SELECT @@server_id")
            .await
            .map_err(|e| SourceError::Other(e.into()))?
            .unwrap_or(0);
        let row: Option<Row> =
            match conn.query_first("SHOW BINARY LOG STATUS").await {
                Ok(r) => r,
                Err(_) => conn
                    .query_first("SHOW MASTER STATUS")
                    .await
                    .map_err(|e| SourceError::Other(e.into()))?,
            };
        let file: String = row
            .and_then(|mut r| r.take::<String, _>(0))
            .unwrap_or_default();
        mysql_server_lineage(server_id, file)
    }

    async fn run_inner(
        &self,
        tx: mpsc::Sender<SourceItem>,
        chkpt_store: Arc<dyn CheckpointStore>,
        cancel: CancellationToken,
        paused: Arc<AtomicBool>,
        pause_notify: Arc<Notify>,
    ) -> SourceResult<()> {
        // Verify the source lineage ONCE, before any snapshot, stream, or RunCtx.
        // Fail closed if it cannot be established: a swallowed error here would
        // proceed to snapshot/stream with no durable-watermark identity, leaving
        // correctness to downstream consumers instead of stopping the source. The
        // same verified value anchors both the snapshot generation and the CDC
        // durable watermarks.
        let durable_lineage = self.capture_snapshot_lineage().await?;

        // Verify the server_uuid lineage and establish the schema-registry scope
        // BEFORE any registry access (the snapshot preload below included), in
        // every binlog mode: persist it durably (fail closed), then publish it
        // to this pipeline's loaders.
        establish_registry_scope(
            self.dsn.expose(),
            &self.backend,
            &self.registry_scope,
            &self.tenant,
            &self.id,
        )
        .await?;

        // snapshot (if configured)
        let snapshot_progress: Option<MysqlSnapshotProgress> = chkpt_store
            .get_raw(&mysql_snapshot::progress_key(&self.id))
            .await
            .ok()
            .flatten()
            .and_then(|b| serde_json::from_slice(&b).ok());

        let needs_snapshot = match self.snapshot_cfg.mode {
            SnapshotMode::Initial => !snapshot_progress
                .as_ref()
                .map(|p| p.finished)
                .unwrap_or(false),
            SnapshotMode::Always => true,
            SnapshotMode::Never => false,
        };

        if needs_snapshot {
            if self.snapshot_cfg.mode == SnapshotMode::Always {
                if let Ok(bytes) =
                    serde_json::to_vec(&MysqlSnapshotProgress::default())
                {
                    let _ = chkpt_store
                        .put_raw(
                            &mysql_snapshot::progress_key(&self.id),
                            &bytes,
                        )
                        .await;
                }
            }
            info!(source_id = %self.id, "starting mysql snapshot");

            let snap_schema_loader = MySqlSchemaLoader::new(
                self.dsn.clone(),
                self.registry.clone(),
                &self.tenant,
                self.registry_scope.clone(),
            );
            let tracked = snap_schema_loader.preload(&self.tables).await?;

            // Validate every table + freeze lineage + allocate the generation
            // BEFORE emitting any row (keyless/unsupported tables fail here).
            let plan = self
                .prepare_snapshot_generation(
                    &snap_schema_loader,
                    &tracked,
                    durable_lineage.clone(),
                )
                .await?;

            let snapshot_ctx = mysql_snapshot::SnapshotCtx {
                dsn: self.dsn.expose(),
                source_id: &self.id,
                pipeline: &self.pipeline,
                tenant: &self.tenant,
                cfg: &self.snapshot_cfg,
                schema_loader: &snap_schema_loader,
                chkpt_store: chkpt_store.clone(),
                tx: tx.clone(),
                cancel: cancel.clone(),
                generation: plan.generation,
                lineage: plan.lineage,
                identity_map: plan.identity_map,
            };
            let snapshot_position =
                mysql_snapshot::run_snapshot(&snapshot_ctx, &tracked)
                    .await
                    // Preserve a typed preflight refusal (Permission/Incompatible)
                    // if run_snapshot produced one; otherwise wrap as Other.
                    .map_err(|e| {
                        e.downcast::<SourceError>()
                            .unwrap_or_else(SourceError::Other)
                    })?;

            chkpt_store
                .put(&self.id, snapshot_position)
                .await
                .map_err(|e| SourceError::Other(e.into()))?;

            info!(source_id = %self.id, "snapshot complete, starting binlog streaming");
        }

        // binlog streaming - retry prepare_client on transient connection errors
        // (MySQL or toxiproxy may not be ready yet during startup).
        let (host, default_db, server_id, mut client) = {
            let mut retry = common::retry::RetryPolicy::default();
            loop {
                match prepare_client(self.dsn.expose(), &self.id, &chkpt_store)
                    .await
                {
                    Ok(result) => break result,
                    Err(e) => {
                        let source_err: SourceError = e.into();
                        let retryable = matches!(
                            &source_err,
                            SourceError::Connect { .. }
                                | SourceError::Io(_)
                                | SourceError::Timeout { .. }
                        );
                        if retryable {
                            let delay = retry
                                .next_backoff()
                                .min(std::time::Duration::from_secs(60));
                            warn!(
                                source_id = %self.id,
                                error = %source_err,
                                delay_secs = delay.as_secs(),
                                "prepare_client failed, retrying after backoff"
                            );
                            tokio::select! {
                                _ = tokio::time::sleep(delay) => {}
                                _ = cancel.cancelled() => {
                                    return Err(SourceError::Cancelled);
                                }
                            }
                        } else {
                            return Err(source_err);
                        }
                    }
                }
            }
        };

        // Capture the original checkpoint position BEFORE any adjustment.
        // This is used later by check_position_reachability to determine whether
        // A's position actually exists on B - even if we've already switched the
        // connection to B's tail.
        let checkpoint_gtid =
            client.gtid_enabled.then(|| client.gtid_set.clone());
        let checkpoint_file = client.binlog_filename.clone();

        // Fetch and verify the live server identity ONCE, in every mode, before
        // opening the binlog stream. Reused below for the pre-connect position
        // decision and for the durable identity resolution. Fails closed: the
        // stream must not open on an unverified server.
        let live = fetch_identity_verified(self.dsn.expose()).await?;
        let id_store = IdentityStore::new(Arc::clone(&self.backend));

        // Pre-connect failover position: if the server changed, A's checkpoint
        // GTID/file is meaningless on B. Switch to B's binlog tail before
        // capturing the init position so the first stream opens cleanly - in
        // file/pos mode too, not only GTID. Fail closed if the tail cannot be
        // resolved rather than open on A's stale position against B.
        if matches!(
            id_store
                .compare(&self.id, &live)
                .await
                .map_err(SourceError::Other)?,
            IdentityComparison::Changed { .. }
        ) {
            match resolve_binlog_tail(self.dsn.expose()).await {
                Ok((fname, fpos)) => {
                    warn!(
                        source_id = %self.id,
                        "pre-connect failover: switching from A's position to B's binlog tail"
                    );
                    client.gtid_enabled = false;
                    client.gtid_set = String::new();
                    client.binlog_filename = fname;
                    client.binlog_position = fpos as u32;
                }
                Err(e) => {
                    return Err(SourceError::Other(anyhow::anyhow!(
                        "failover detected but could not resolve the new \
                         server's binlog tail: {e}; refusing to stream on a \
                         stale position"
                    )));
                }
            }
        }

        let init_gtid = client.gtid_enabled.then(|| client.gtid_set.clone());
        let init_file = client.binlog_filename.clone();
        let init_pos = client.binlog_position as u64;

        info!(source_id=%self.id, "prepare_client finished, loading schemas");
        let schema_loader = MySqlSchemaLoader::new(
            self.dsn.clone(),
            self.registry.clone(),
            &self.tenant,
            self.registry_scope.clone(),
        );

        // NOTE: preload deferred until after check_identity_post_reconnect so the
        // registry still holds A's schema when the reconciler computes the drift diff.

        info!(
            source_id=%self.id,
            host=%host,
            db=%default_db,
            "mysql source starting ...");

        let backend = Arc::clone(&self.backend);

        let mut ctx = RunCtx {
            source_id: self.id.clone(),
            pipeline: self.pipeline.clone(),
            tenant: self.tenant.clone(),
            dsn: self.dsn.clone(),
            host,
            default_db,
            server_id,
            tx,
            chkpt: chkpt_store,
            cancel,
            paused,
            pause_notify,
            schema: schema_loader,
            allow: AllowList::new(&self.tables),
            retry: RetryPolicy::default(),
            identity_store: IdentityStore::new(Arc::clone(&backend)),
            reconciler: SchemaReconciler::new(
                Arc::clone(&self.registry),
                Arc::clone(&backend),
            ),
            registry_scope: self.registry_scope.clone(),
            registry_backend: Arc::clone(&backend),
            inactivity: Duration::from_secs(60),
            table_map: HashMap::new(),
            last_file: init_file,
            last_pos: init_pos,
            last_gtid: init_gtid,
            current_gtid: None,
            in_explicit_txn: false,
            message_ordinal: 0,
            checkpoint_gtid,
            checkpoint_file,
            tables: self.tables.clone(),
            outbox_tables: self.outbox_tables.clone(),
            on_schema_drift: self.on_schema_drift.clone(),
            // Verified once above, before the stream opens.
            durable_lineage: Some(durable_lineage),
        };

        // Resolve the identity BEFORE opening the stream: persist FirstSeen,
        // reconcile a detected change, or verify a Same position is still
        // reachable. Reuse the identity already verified above. A failure here
        // means no binlog stream is opened. The registry still holds A's schema
        // for the reconciler's drift diff; preload stays deferred until after.
        check_identity_post_reconnect(&mut ctx, Some(live)).await?;

        info!(source_id=%self.id, "connecting for binlog stream ..");
        let mut stream = connect_first_stream(&ctx, client).await?;

        // Safe to preload now: reconciliation has run, registry reflects post-reconcile state.
        let tracked = ctx.schema.preload(&self.tables).await?;
        info!(source_id=%self.id, tables = tracked.len(), "schemas preloaded");

        // Controlled credential rotation (opt-in, file-backed credentials only).
        // GTID mode is mandatory for live rotation - fail startup otherwise. The
        // runtime owns the watcher/manager task; it is cancelled and joined after
        // the loop on every exit path, with `Drop` as the abort backstop.
        let mut rotation = match &self.rotation {
            Some(spec) => {
                mysql_rotation::require_gtid_mode(self.dsn.expose()).await?;
                Some(mysql_rotation::MySqlRotationRuntime::spawn(
                    spec,
                    ctx.dsn.clone(),
                    self.id.clone(),
                    self.pipeline.clone(),
                    Arc::clone(&self.backend),
                    ctx.server_id,
                    ctx.default_db.clone(),
                    &ctx.cancel,
                )?)
            }
            None => None,
        };

        info!("entering binlog read loop");
        // The loop runs inside an async block so its result can be captured and the
        // rotation runtime cancelled+joined before teardown, on both the normal and
        // fatal exit paths. A fatal result (including gate-6 rotation failures) is
        // re-propagated after join and before the teardown checkpoint put.
        let loop_result: SourceResult<()> = async {
        loop {
            if !pause_until_resumed(&ctx.cancel, &ctx.paused, &ctx.pause_notify)
                .await
            {
                info!(source_id=%ctx.source_id, "resuming ..");
                break;
            }

            // Rotation: schedule Stage-A preflight concurrently with the stream,
            // and apply the Stage-B swap only at a whole-transaction GTID boundary
            // (between transactions, with an established GTID position).
            if let Some(rt) = rotation.as_mut() {
                rt.drive_preflight(&ctx);
                if ctx.current_gtid.is_none() && ctx.last_gtid.is_some() {
                    // A CloseUncertain/FailedClosed outcome returns an error from
                    // this block (gate 6); `loop_result?` re-propagates it before the
                    // teardown checkpoint put, so the checkpoint never advances.
                    stream = rt.apply_at_boundary(&mut ctx, stream).await?;
                }
            }

            debug!(source_id=%ctx.source_id, "reading the next event ..");
            // Idle-source wakeup: race the read against rotation activity so a
            // rotation applies even when no binlog events are flowing.
            let control: Result<(), LoopControl> = match rotation.as_mut() {
                Some(rt) => tokio::select! {
                    r = read_next_event(&mut stream, &ctx) => match r {
                        Ok((header, data)) => {
                            ctx.last_pos = header.next_event_position as u64;
                            dispatch_event(&mut ctx, &header, data).await?;
                            Ok(())
                        }
                        Err(ctrl) => Err(ctrl),
                    },
                    _ = rt.wait_activity() => continue,
                },
                None => match read_next_event(&mut stream, &ctx).await {
                    Ok((header, data)) => {
                        ctx.last_pos = header.next_event_position as u64;
                        dispatch_event(&mut ctx, &header, data).await?;
                        Ok(())
                    }
                    Err(ctrl) => Err(ctrl),
                },
            };

            match control {
                Ok(()) => {}
                Err(LoopControl::ReloadSchema { db, table }) => {
                    if let (Some(d), Some(t)) = (db, table) {
                        let _ = ctx.schema.reload_schema(&d, &t).await?;
                    } else {
                        let _ = ctx.schema.reload_all(&self.tables).await?;
                    }
                    match do_reconnect(&mut ctx).await? {
                        Some(s) => stream = s,
                        None => continue,
                    }
                }
                Err(LoopControl::Reconnect) => {
                    match do_reconnect(&mut ctx).await? {
                        Some(s) => stream = s,
                        None => continue,
                    }
                }
                Err(LoopControl::Stop) => break,
                Err(LoopControl::Fail(e)) => return Err(e),
            }
        }
        Ok(())
        }
        .await;

        // Cancel and join the rotation tasks on every exit path (normal or fatal).
        if let Some(rt) = rotation.take() {
            rt.shutdown().await;
        }
        loop_result?;

        // Best-effort final checkpoint update. Persist the read position as the
        // aggregate checkpoint ONLY when this store does not derive the resume position
        // from per-sink checkpoints. In production the coordinator writes per-sink
        // checkpoints (only after sink acknowledgement) and the resume position is their
        // minimum; writing the read position here would resume ahead of un-acknowledged
        // deliveries and lose them on restart (a clean stop during a sink outage). MySQL
        // has no consumer-driven binlog purge, so there is no server-side WAL feedback to
        // fix - only this resume checkpoint.
        if !ctx.chkpt.manages_per_sink_checkpoints() {
            let _ = ctx
                .chkpt
                .put(
                    &ctx.source_id,
                    MySqlCheckpoint {
                        file: ctx.last_file,
                        pos: ctx.last_pos,
                        gtid_set: ctx.last_gtid,
                    },
                )
                .await;
        }

        Ok(())
    }
}

/// Order two MySQL checkpoints by binlog `(file, pos)`, failing closed.
///
/// `MySqlCheckpoint` is `{ file: String, pos: u64, gtid_set: Option<String> }`.
/// An unparseable checkpoint is [`CheckpointOrder::Incomparable`] rather than
/// silently treated as an orderable position (which could select a resume point
/// ahead of a sink and drop its events).
pub fn compare_mysql_checkpoints(a: &[u8], b: &[u8]) -> CheckpointOrder {
    #[derive(serde::Deserialize)]
    struct Cp {
        file: String,
        pos: u64,
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
    match a.file.cmp(&b.file).then(a.pos.cmp(&b.pos)) {
        std::cmp::Ordering::Less => CheckpointOrder::Before,
        std::cmp::Ordering::Equal => CheckpointOrder::Equal,
        std::cmp::Ordering::Greater => CheckpointOrder::After,
    }
}

#[async_trait]
impl Source for MySqlSource {
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
                error!(error=?e, "run task ended with error");
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
        compare_mysql_checkpoints(a, b)
    }

    async fn check_durable_snapshot_startup(
        &self,
        checkpoint_store: &dyn CheckpointStore,
    ) -> Result<(), SourceError> {
        let progress: mysql_snapshot::MysqlSnapshotProgress = checkpoint_store
            .get_raw(&mysql_snapshot::progress_key(&self.id))
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
// Stream helpers
// ============================================================================

async fn connect_first_stream(
    ctx: &RunCtx,
    client: BinlogClient,
) -> SourceResult<BinlogStream> {
    let init_gtid = client.gtid_enabled.then(|| client.gtid_set.clone());
    let init_file = if client.gtid_enabled {
        None
    } else {
        Some(client.binlog_filename.clone())
    };
    let init_pos = if client.gtid_enabled {
        None
    } else {
        Some(client.binlog_position)
    };

    let dsn = ctx.dsn.clone();
    let sid = ctx.server_id;
    let make_client = move || {
        let mut c = BinlogClient {
            url: dsn.expose().to_string(),
            server_id: sid,
            heartbeat_interval_secs: HEARTBEAT_INTERVAL_SECS,
            timeout_secs: READ_TIMEOUT,
            ..Default::default()
        };

        if let Some(gtid) = init_gtid.clone() {
            c.gtid_enabled = true;
            c.gtid_set = gtid
        } else if let (Some(fname), Some(pos)) = (init_file.clone(), init_pos) {
            c.gtid_enabled = false;
            c.binlog_filename = fname;
            c.binlog_position = pos;
        }
        c
    };

    connect_binlog_with_retries(
        &ctx.source_id,
        make_client,
        &ctx.cancel,
        &ctx.default_db,
        ctx.retry.clone(),
    )
    .await
}

/// Reconnect using the best available resume position, then run the identity
/// check. If a failover is detected, reconciliation runs before returning the
/// stream - callers see a ready stream regardless.
async fn reconnect_stream(ctx: &mut RunCtx) -> SourceResult<BinlogStream> {
    let (gtid_to_use, file_to_use, pos_to_use) = if let Some(g) = &ctx.last_gtid
    {
        (Some(g.clone()), None, None)
    } else if !ctx.last_file.is_empty() && ctx.last_pos > 0 {
        (None, Some(ctx.last_file.clone()), Some(ctx.last_pos as u32))
    } else {
        match resolve_binlog_tail(ctx.dsn.expose()).await {
            Ok((f, p)) => (None, Some(f), Some(p as u32)),
            Err(_) => {
                return Err(SourceError::Connect {
                    details: "could not resolve binlog tail during reconnect"
                        .into(),
                });
            }
        }
    };

    let dsn = ctx.dsn.clone();
    let sid = ctx.server_id;
    let make_client = move || {
        let mut c = BinlogClient {
            url: dsn.expose().to_string(),
            server_id: sid,
            heartbeat_interval_secs: HEARTBEAT_INTERVAL_SECS,
            timeout_secs: READ_TIMEOUT,
            ..Default::default()
        };

        if let Some(g) = gtid_to_use.clone() {
            c.gtid_enabled = true;
            c.gtid_set = g;
        } else if let (Some(f), Some(p)) = (file_to_use.clone(), pos_to_use) {
            c.gtid_enabled = false;
            c.binlog_filename = f;
            c.binlog_position = p;
        }
        c
    };

    let stream = connect_binlog_with_retries(
        &ctx.source_id,
        make_client,
        &ctx.cancel,
        &ctx.default_db,
        ctx.retry.clone(),
    )
    .await?;

    ctx.retry.reset();

    check_identity_post_reconnect(ctx, None).await?;

    Ok(stream)
}

/// Apply backoff, sleep (cancel-aware), reconnect, and absorb transient errors.
///
/// Returns:
/// - `Ok(Some(stream))` - reconnected, caller assigns the new stream.
/// - `Ok(None)` - cancelled during sleep or transient connect error;
///   caller should `continue` the loop (next iteration will either break
///   on cancel or retry with fresh backoff).
/// - `Err(e)`           - fatal error, caller propagates.
async fn do_reconnect(ctx: &mut RunCtx) -> SourceResult<Option<BinlogStream>> {
    let delay = ctx.retry.next_backoff();
    warn!(
        source_id = %ctx.source_id,
        delay_ms = delay.as_millis(),
        "scheduling reconnect after backoff"
    );

    tokio::select! {
        _ = tokio::time::sleep(delay) => {}
        _ = ctx.cancel.cancelled() => return Ok(None),
    }

    match reconnect_stream(ctx).await {
        Ok(s) => Ok(Some(s)),
        Err(SourceError::Connect { .. }) | Err(SourceError::Io(_)) => Ok(None),
        Err(e) => Err(e),
    }
}

// ============================================================================
// Failover detection + reconciliation
// ============================================================================

/// Build a non-GTID MySQL lineage from `(server_id, binlog file)`, failing closed
/// on an unverified anchor. A zero `server_id` (server exposed neither a GTID
/// `server_uuid` nor a real `server_id`) or an empty binlog file (binary logging
/// off, or `SHOW BINARY LOG STATUS` returned nothing) must not be recorded: the
/// error is propagated by the caller (with `?`) at startup, so the source fails
/// closed before opening a stream rather than binding to a meaningless identity.
fn mysql_server_lineage(
    server_id: u32,
    file: String,
) -> SourceResult<PersistedLineage> {
    if server_id == 0 {
        return Err(SourceError::Other(anyhow::anyhow!(
            "cannot capture MySQL lineage: server exposes neither a server_uuid \
             (GTID) nor a nonzero server_id; refusing to record an unverified \
             lineage"
        )));
    }
    if file.is_empty() {
        return Err(SourceError::Other(anyhow::anyhow!(
            "cannot capture MySQL lineage: no current binlog file (is binary \
             logging enabled?); refusing to record an empty-file lineage"
        )));
    }
    Ok(PersistedLineage::MysqlServer { server_id, file })
}

/// Bounded attempts to fetch the live server identity before failing closed.
const IDENTITY_FETCH_ATTEMPTS: u32 = 5;

/// Fetch the live MySQL server identity with bounded retries, failing closed if
/// it cannot be obtained or is absent. Identity is a correctness authority: the
/// source must never continue against an unverified server, so a persistent
/// fetch failure (or a server that exposes no identity) stops the source rather
/// than being silently skipped.
async fn fetch_identity_verified(dsn: &str) -> SourceResult<ServerIdentity> {
    let mut attempt = 0u32;
    loop {
        match fetch_server_identity(dsn).await {
            Ok(Some(id)) => return Ok(ServerIdentity::from(id)),
            Ok(None) => {
                return Err(SourceError::Other(anyhow::anyhow!(
                    "server identity unavailable (no server_uuid/server_id); \
                     cannot verify server lineage - refusing to stream"
                )));
            }
            Err(e) => {
                attempt += 1;
                if attempt >= IDENTITY_FETCH_ATTEMPTS {
                    return Err(SourceError::Other(anyhow::anyhow!(
                        "failed to fetch server identity after {attempt} \
                         attempts: {e}; refusing to stream on unverified identity"
                    )));
                }
                warn!(
                    source_id = "?", attempt, error = %e,
                    "identity fetch failed; retrying before failing closed"
                );
                tokio::time::sleep(Duration::from_millis(
                    200 * 2u64.pow(attempt.min(5)),
                ))
                .await;
            }
        }
    }
}

/// The schema-registry lineage of a verified MySQL identity: its `server_uuid`,
/// in every binlog mode (GTID is not required to read `@@server_uuid`). An
/// empty or all-zero UUID fails closed - `server_id` is never a substitute,
/// since it is operator-assigned and reused after replacement.
fn mysql_registry_lineage(
    live: &ServerIdentity,
) -> SourceResult<LineageDescriptor> {
    match live {
        ServerIdentity::MySql(id) => LineageDescriptor::mysql(&id.server_uuid)
            .map_err(|e| SourceError::Incompatible {
                details: format!("{e:#}").into(),
            }),
        other => Err(SourceError::Other(anyhow::anyhow!(
            "expected a MySQL server identity, got {other:?}"
        ))),
    }
}

/// Verify the live MySQL `server_uuid` lineage and establish it as the registry
/// scope of `(tenant, source_id)`: durably recorded (fail closed), then
/// published into `shared`. This is the startup step every MySQL source runs
/// before any schema-registry access, in every binlog mode.
pub async fn establish_registry_scope(
    dsn: &str,
    backend: &ArcStorageBackend,
    shared: &SharedRegistryScope,
    tenant: &str,
    source_id: &str,
) -> SourceResult<ScopeChange> {
    let live = fetch_identity_verified(dsn).await?;
    Ok(establish_scope(
        backend,
        shared,
        tenant,
        source_id,
        mysql_registry_lineage(&live)?,
    )
    .await?)
}

/// Bring the published registry scope in line with a lineage just verified
/// against the live server, before any further registry access. A changed
/// `server_uuid` is a failover to another server: the new lineage is durably
/// recorded (fail closed) and published, giving it a fresh schema namespace and
/// invalidating caches from the old lineage.
async fn sync_registry_lineage(
    ctx: &RunCtx,
    live: &ServerIdentity,
) -> SourceResult<()> {
    let descriptor = mysql_registry_lineage(live)?;
    if ctx.registry_scope.current()?.lineage().descriptor == descriptor {
        return Ok(());
    }
    let change = establish_scope(
        &ctx.registry_backend,
        &ctx.registry_scope,
        &ctx.tenant,
        &ctx.source_id,
        descriptor,
    )
    .await?;
    warn!(
        source_id = %ctx.source_id,
        lineage = %change.scope.lineage().lineage_hash,
        "source lineage changed; schema registry re-scoped to the new server"
    );
    Ok(())
}

/// Compare the live server identity against the stored one.
///
/// - `FirstSeen`: store and continue (clean start or wiped state).
/// - `Same`: normal reconnect, nothing to do.
/// - `Changed`: run full failover reconciliation before returning.
///
/// Identity fetch, comparison, and persistence are correctness authority: any
/// failure fails closed (propagated), never mapped to `Same`.
///
/// `prefetched` reuses an identity already verified by the caller (startup path)
/// so the live server is queried once; `None` fetches and verifies here (the
/// reconnect path).
async fn check_identity_post_reconnect(
    ctx: &mut RunCtx,
    prefetched: Option<ServerIdentity>,
) -> SourceResult<()> {
    let live = match prefetched {
        Some(live) => live,
        None => fetch_identity_verified(ctx.dsn.expose()).await?,
    };
    // Re-scope the registry to the live lineage before any registry access on
    // this connection (fails closed if the new lineage cannot be persisted).
    sync_registry_lineage(ctx, &live).await?;

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
        IdentityComparison::Same => {
            // A RESET BINARY LOGS AND GTIDS wipes the GTID history without
            // changing the server UUID.  Run the position check here too so
            // we catch that case on the first reconnect after a purge.
            // Skip when there is no GTID checkpoint (file/pos mode or fresh
            // start) to avoid false positives from the file-presence fallback.
            if ctx.checkpoint_gtid.is_some() {
                match check_position_reachability(
                    ctx.dsn.expose(),
                    &ctx.checkpoint_file,
                    ctx.checkpoint_gtid.as_deref(),
                )
                .await
                .unwrap_or(PositionReachability::Unknown {
                    reason: "reachability check failed".into(),
                }) {
                    PositionReachability::Reachable
                    | PositionReachability::Unknown { .. } => {}
                    PositionReachability::Lost { reason } => {
                        return Err(SourceError::Other(anyhow::anyhow!(
                            "checkpoint GTID set no longer reachable on \
                             this server (binlog purge?): {reason}. \
                             Re-snapshot required."
                        )));
                    }
                }
            }
        }
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

async fn run_failover_reconciliation(
    ctx: &mut RunCtx,
    previous: ServerIdentity,
    current: ServerIdentity,
) -> SourceResult<()> {
    // Idempotency: skip catalog queries if this transition already reconciled.
    let existing = ctx
        .reconciler
        .already_completed(&ctx.source_id, &previous, &current)
        .await
        .unwrap_or(None);

    if existing.is_none() {
        // Position reachability - use the original checkpoint position, not the
        // (potentially adjusted) streaming position in last_gtid/last_file.
        match check_position_reachability(
            ctx.dsn.expose(),
            &ctx.checkpoint_file,
            ctx.checkpoint_gtid.as_deref(),
        )
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

        // Schema diff - use ctx.tables (configured patterns) since the schema cache
        // may be empty (preload is intentionally deferred until after reconciliation).
        let mut inputs = Vec::new();
        for pattern in &ctx.tables {
            let parts: Vec<&str> = pattern.splitn(2, '.').collect();
            if parts.len() != 2 || parts[1].contains('*') {
                continue;
            }
            let (db, table) = (parts[0].to_owned(), parts[1].to_owned());
            let live_cols: Option<
                Vec<crate::failover::reconciler::ColumnSnapshot>,
            > = mysql_health::fetch_live_columns(ctx.dsn.expose(), &db, &table)
                .await
                .ok()
                .flatten()
                .map(|cols| cols.into_iter().map(Into::into).collect());
            inputs.push(ReconcileInput {
                db,
                table,
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

        // Invalidate schema loader cache for changed tables so the next row
        // event triggers a fresh load and registry registration.
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

    // Persist new identity only after reconciliation completes. Fail closed if
    // the durable write does not commit: continuing would leave stale identity
    // authority and re-run reconciliation (or miss a later change).
    ctx.identity_store
        .store(&ctx.source_id, &current)
        .await
        .map_err(SourceError::Other)?;

    // Clear streaming position so subsequent reconnects resolve B's binlog tail
    // rather than re-sending A's GTID. Covers mid-run failovers where the stream
    // was already open when the switch happened.
    ctx.last_gtid = None;
    ctx.current_gtid = None;
    ctx.message_ordinal = 0;
    ctx.last_file = String::new();
    ctx.last_pos = 0;

    info!(source_id = %ctx.source_id, "failover reconciliation complete");
    Ok(())
}

#[cfg(test)]
mod server_lineage_tests {
    //! R3-C8: the non-GTID fallback lineage fails closed on a zero server_id or
    //! an empty binlog file, rather than persisting an unverified anchor.
    use super::{PersistedLineage, mysql_server_lineage};

    #[test]
    fn rejects_zero_server_id() {
        assert!(
            mysql_server_lineage(0, "binlog.000001".into()).is_err(),
            "a zero server_id is an unverified lineage"
        );
    }

    #[test]
    fn rejects_empty_binlog_file() {
        assert!(
            mysql_server_lineage(5, String::new()).is_err(),
            "an empty binlog file must not be recorded as lineage"
        );
    }

    #[test]
    fn accepts_valid_server_and_file() {
        match mysql_server_lineage(5, "binlog.000007".into()) {
            Ok(PersistedLineage::MysqlServer { server_id, file }) => {
                assert_eq!(server_id, 5);
                assert_eq!(file, "binlog.000007");
            }
            other => panic!("expected a MysqlServer lineage, got {other:?}"),
        }
    }
}

#[cfg(test)]
mod compare_checkpoints_tests {
    use super::compare_mysql_checkpoints;
    use deltaforge_core::CheckpointOrder;

    fn cp(file: &str, pos: u64) -> Vec<u8> {
        format!(r#"{{"file":"{file}","pos":{pos},"gtid_set":null}}"#)
            .into_bytes()
    }

    // Regression: the (file, pos) ordering must be preserved exactly through the
    // CheckpointOrder conversion.
    #[test]
    fn orders_by_pos_within_same_file() {
        assert_eq!(
            compare_mysql_checkpoints(
                &cp("bin.000001", 100),
                &cp("bin.000001", 200)
            ),
            CheckpointOrder::Before
        );
        assert_eq!(
            compare_mysql_checkpoints(
                &cp("bin.000001", 200),
                &cp("bin.000001", 100)
            ),
            CheckpointOrder::After
        );
        assert_eq!(
            compare_mysql_checkpoints(
                &cp("bin.000001", 100),
                &cp("bin.000001", 100)
            ),
            CheckpointOrder::Equal
        );
    }

    #[test]
    fn file_dominates_pos() {
        // A later file with a smaller pos is still After.
        assert_eq!(
            compare_mysql_checkpoints(
                &cp("bin.000002", 1),
                &cp("bin.000001", 999)
            ),
            CheckpointOrder::After
        );
    }

    #[test]
    fn malformed_is_incomparable_never_equal() {
        for bad in [&b"not json"[..], &b"{}"[..], br#"{"file":"x"}"#] {
            assert_eq!(
                compare_mysql_checkpoints(bad, &cp("bin.000001", 1)),
                CheckpointOrder::Incomparable
            );
            assert_eq!(
                compare_mysql_checkpoints(&cp("bin.000001", 1), bad),
                CheckpointOrder::Incomparable
            );
            assert_ne!(
                compare_mysql_checkpoints(bad, &cp("bin.000001", 1)),
                CheckpointOrder::Equal
            );
        }
    }

    #[test]
    fn well_formed_is_reflexively_equal() {
        assert_eq!(
            compare_mysql_checkpoints(
                &cp("bin.000001", 42),
                &cp("bin.000001", 42)
            ),
            CheckpointOrder::Equal
        );
    }
}

#[cfg(test)]
mod identity_fail_closed_tests {
    //! R3-C2: MySQL live identity fetch fails closed.
    //!
    //! Container-backed failover behaviour (identity change detection, GTID tail
    //! switch, reconciliation) is covered by `tests/failover_e2e.rs`. Here we
    //! prove the shared `fetch_identity_verified` helper - used by both the
    //! pre-connect and post-reconnect paths - refuses to return when the live
    //! identity query cannot reach the server, instead of yielding an absent
    //! identity that would let the stream open on an unverified lineage.
    use super::fetch_identity_verified;

    // Port 1 is not bound; the connect is refused fast (ECONNREFUSED).
    const UNREACHABLE_DSN: &str = "mysql://root:none@127.0.0.1:1/none";

    #[tokio::test]
    async fn fetch_identity_verified_fails_closed_when_query_unavailable() {
        let err = fetch_identity_verified(UNREACHABLE_DSN)
            .await
            .expect_err("unreachable server must fail closed");
        let msg = err.to_string();
        assert!(
            msg.contains("refusing to stream"),
            "error should refuse to stream on unverified identity, got: {msg}"
        );
    }
}
