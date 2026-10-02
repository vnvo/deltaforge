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

mod mysql_activation;
mod mysql_baseline;
mod mysql_binlog_scan;
mod mysql_checkpoint_lineage;
mod mysql_ddl_attribution;
mod mysql_forward_proof;
mod mysql_helpers;
mod mysql_selection;
mod mysql_session;
mod mysql_signature;
use mysql_helpers::{checkpoint_lineage, prepare_client};

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

mod mysql_table_schema;
use crate::mysql::mysql_helpers::{
    Opened, connect_binlog_with_retries, resolve_binlog_tail,
};
use crate::mysql::mysql_session::{SessionError, open_control_connection};
pub use mysql_table_schema::{MySqlColumn, MySqlTableSchema};

pub mod mysql_snapshot;
pub use mysql_snapshot::{MysqlSnapshotProgress, progress_key};

pub mod mysql_health;

use crate::failover::identity::{
    IdentityComparison, IdentityStore, ServerIdentity,
};
use crate::failover::reconciler::{ReconcileInput, SchemaReconciler};
use crate::mysql::mysql_health::{
    PositionReachability, check_position_reachability_on, fetch_server_identity,
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
    /// Verified registry lineage hash of the server this position belongs to
    /// (MySQL `server_uuid`). `None` only in checkpoints written before
    /// lineage was recorded; see [`compare_mysql_checkpoints`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lineage: Option<String>,
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
    /// QueryEvent ordinal within the current GTID transaction (reset at each
    /// GTID, incremented for every QueryEvent): the event identity of the
    /// activation records a statement establishes, as the binlog scanner
    /// numbers them.
    query_ordinal: u32,
    /// The server's `lower_case_table_names` (read at startup on a verified
    /// connection): how DDL table names map to registry keys.
    lower_case_table_names: u8,
    /// Evaluation position of rows (spec 7.3): the executed state immediately
    /// before the current transaction - the stream position after the last
    /// commit boundary (or the position the stream (re)started from).
    txn_eval: Option<crate::durable_checkpoint::WmPos>,
    /// Validated activation timelines and row-time selections.
    selection: mysql_selection::Caches,
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
        // to this pipeline's loaders. The snapshot lineage above was read on
        // another connection: both must name one server before anything is
        // recorded. From here on every connection proves it is this server
        // before it is trusted.
        let root = fetch_identity_verified(self.dsn.expose()).await?;
        let same_server = match (&durable_lineage, &root) {
            (
                PersistedLineage::MysqlGtid { source_uuid },
                ServerIdentity::MySql(id),
            ) => {
                mysql_event_id::parse_uuid16(&id.server_uuid)
                    == Some(*source_uuid)
            }
            _ => false,
        };
        if !same_server {
            return Err(SourceError::Lineage {
                details: format!(
                    "startup connections disagree on the server: snapshot \
                     lineage {durable_lineage:?}, identity {root:?}"
                )
                .into(),
            });
        }
        establish_scope(
            &self.backend,
            &self.registry_scope,
            &self.tenant,
            &self.id,
            mysql_registry_lineage(&root)?,
        )
        .await?;

        // Checkpoints carry the verified lineage. Before anything reads
        // snapshot progress or the resume position, reconcile the stored
        // checkpoints with it: adopt pre-lineage ones (no lineage change ever
        // recorded), carry a failover predecessor's GTID checkpoints over after
        // verifying them on the current server, and otherwise STOP here.
        let scope = self.registry_scope.current()?;
        let lineage_hash = scope.lineage().lineage_hash.clone();
        let storage::adapters::LineageDescriptor::Mysql { server_uuid } =
            scope.lineage().descriptor.clone()
        else {
            return Err(SourceError::Other(anyhow::anyhow!(
                "MySQL source established a non-MySQL lineage"
            )));
        };
        mysql_checkpoint_lineage::reconcile_checkpoint_lineage(
            chkpt_store.as_ref(),
            &self.backend,
            &self.tenant,
            &self.id,
            &lineage_hash,
            &mysql_checkpoint_lineage::LiveGtidAvailability {
                dsn: self.dsn.expose(),
                server_uuid: server_uuid.clone(),
            },
        )
        .await?;

        // snapshot (if configured)
        let snapshot_progress: Option<MysqlSnapshotProgress> = chkpt_store
            .get_raw(&mysql_snapshot::progress_key(&self.id))
            .await
            .ok()
            .flatten()
            .and_then(|b| serde_json::from_slice(&b).ok());

        // Whether this start continues from a committed resume position: a
        // start without one (first start, or "from end") or a snapshot (a new
        // anchor) is a stream discontinuity (spec 7.5).
        let committed_resume = chkpt_store
            .get::<MySqlCheckpoint>(&self.id)
            .await
            .map_err(|e| SourceError::Checkpoint {
                details: e.to_string().into(),
            })?
            .is_some();

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
                checkpoint_lineage: checkpoint_lineage(&self.registry_scope),
                dsn: self.dsn.expose(),
                expected_uuid: &server_uuid,
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
        let (host, default_db, server_id, client) = {
            let mut retry = common::retry::RetryPolicy::default();
            loop {
                match prepare_client(
                    self.dsn.expose(),
                    &server_uuid,
                    &self.id,
                    &chkpt_store,
                )
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
        let live =
            fetch_identity_verified_as(self.dsn.expose(), &server_uuid).await?;
        let lower_case_table_names =
            fetch_lower_case_table_names(self.dsn.expose(), &server_uuid)
                .await?;

        // A failover (stored identity != live) is reconciled below, before
        // the stream opens, against the EXACT position the stream will start
        // from: the (carried-over) checkpoint GTID set. A file/position start
        // across servers cannot be proven and stops there.
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
            query_ordinal: 0,
            lower_case_table_names,
            txn_eval: None,
            selection: Default::default(),
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
        let resume = ResumeAt::of(&client);
        check_identity_post_reconnect(&mut ctx, Some(live), &resume).await?;

        // A start that does not continue from the committed resume position
        // invalidates positional proof for every table from here: a
        // lineage-wide barrier at the start position, durable before the
        // stream opens. An ordinary restart at the committed position writes
        // none.
        if needs_snapshot || !committed_resume {
            record_stream_start(&ctx).await?;
        }

        info!(source_id=%self.id, "connecting for binlog stream ..");
        let mut stream = connect_first_stream(&ctx, client).await?;
        ctx.mark_transaction_boundary();

        // Safe to preload now: reconciliation has run, registry reflects post-reconcile state.
        let tracked = ctx.schema.preload(&self.tables).await?;
        info!(source_id=%self.id, tables = tracked.len(), "schemas preloaded");

        // Establish activation baselines at the start position for tracked
        // tables whose version the timeline does not prove there, before any
        // event is read.
        mysql_baseline::establish(&ctx, &tracked).await?;

        // Controlled credential rotation (opt-in, file-backed credentials only).
        // GTID mode is mandatory for live rotation - fail startup otherwise. The
        // runtime owns the watcher/manager task; it is cancelled and joined after
        // the loop on every exit path, with `Drop` as the abort backstop.
        let mut rotation = match &self.rotation {
            Some(spec) => {
                mysql_rotation::require_gtid_mode(
                    self.dsn.expose(),
                    &ctx.expected_uuid()?,
                )
                .await?;
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
                if !pause_until_resumed(
                    &ctx.cancel,
                    &ctx.paused,
                    &ctx.pause_notify,
                )
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
                                advance_position(&mut ctx, &header);
                                dispatch_event(&mut ctx, &header, data).await?;
                                Ok(())
                            }
                            Err(ctrl) => Err(ctrl),
                        },
                        _ = rt.wait_activity() => continue,
                    },
                    None => match read_next_event(&mut stream, &ctx).await {
                        Ok((header, data)) => {
                            advance_position(&mut ctx, &header);
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
                            Some(s) => {
                                stream = s;
                                ctx.mark_transaction_boundary();
                            }
                            None => continue,
                        }
                    }
                    Err(LoopControl::Reconnect) => {
                        match do_reconnect(&mut ctx).await? {
                            Some(s) => {
                                stream = s;
                                ctx.mark_transaction_boundary();
                            }
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
                        lineage: checkpoint_lineage(&ctx.registry_scope),
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
    // Positions of different servers are never ordered: two checkpoints that
    // both carry a lineage must carry the same one; a lineage-bearing and a
    // legacy (pre-lineage) checkpoint are Incomparable; two legacy checkpoints
    // keep the position-only comparison for upgrade compatibility. Positions
    // then go to the shared comparator: GTID sets by inclusion, binlog
    // coordinates by (base, numeric index, pos) - never lexically - and a GTID
    // checkpoint against a file/pos one is Incomparable.
    let parse = |raw: &[u8], which: &str| -> Option<MySqlCheckpoint> {
        match serde_json::from_slice(raw) {
            Ok(v) => Some(v),
            Err(e) => {
                tracing::warn!(error = %e, "incomparable checkpoint {which}: parse failed");
                None
            }
        }
    };
    let position = |cp: &MySqlCheckpoint, which: &str| {
        let p = crate::durable_checkpoint::mysql_checkpoint_position(
            &cp.file,
            cp.pos,
            cp.gtid_set.as_deref(),
        );
        if p.is_none() {
            tracing::warn!(file = %cp.file, "incomparable checkpoint {which}: unrecognised binlog file name");
        }
        p
    };
    let (Some(ca), Some(cb)) = (parse(a, "a"), parse(b, "b")) else {
        return CheckpointOrder::Incomparable;
    };
    // A lineage must be a canonical hash; two identical malformed strings
    // are not evidence of the same server.
    let malformed = |cp: &MySqlCheckpoint| {
        cp.lineage
            .as_deref()
            .is_some_and(|l| !mysql_checkpoint_lineage::is_canonical_lineage(l))
    };
    if malformed(&ca) || malformed(&cb) {
        tracing::warn!("incomparable checkpoints: malformed lineage");
        return CheckpointOrder::Incomparable;
    }
    if ca.lineage != cb.lineage {
        tracing::warn!(
            a = ?ca.lineage,
            b = ?cb.lineage,
            "incomparable checkpoints: different (or missing) server lineage"
        );
        return CheckpointOrder::Incomparable;
    }
    match (position(&ca, "a"), position(&cb, "b")) {
        (Some(a), Some(b)) => {
            crate::durable_checkpoint::order_positions(&a, &b)
        }
        _ => CheckpointOrder::Incomparable,
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

    let expected = ctx.expected_uuid()?;
    match connect_binlog_with_retries(
        &ctx.source_id,
        &expected,
        make_client,
        &ctx.cancel,
        &ctx.default_db,
        ctx.retry.clone(),
    )
    .await?
    {
        Opened::Stream(stream) => Ok(stream),
        // Startup verified this server on every earlier connection: a
        // replication session elsewhere is split routing, never a failover.
        Opened::OtherServer(found) => Err(SourceError::Lineage {
            details: format!(
                "startup replication session reached server {found}, \
                 expected {expected}"
            )
            .into(),
        }),
    }
}

/// Reconnect using the best available resume position. Every replication
/// session is verified as the expected server before its dump command. A
/// session that reached another server U was closed before the dump: U is a
/// failover candidate, accepted only after reconciliation proves it on
/// connections verified as U and records it durably; a fresh session verified
/// as U is then opened. Callers see a ready stream regardless.
async fn reconnect_stream(ctx: &mut RunCtx) -> SourceResult<BinlogStream> {
    let expected = ctx.expected_uuid()?;
    let (gtid_to_use, file_to_use, pos_to_use) = if let Some(g) = &ctx.last_gtid
    {
        (Some(g.clone()), None, None)
    } else if !ctx.last_file.is_empty() && ctx.last_pos > 0 {
        (None, Some(ctx.last_file.clone()), Some(ctx.last_pos as u32))
    } else {
        match resolve_binlog_tail(ctx.dsn.expose(), &expected).await {
            Ok((f, p)) => (None, Some(f), Some(p as u32)),
            Err(MySqlSourceError::Lineage(details)) => {
                return Err(SourceError::Lineage {
                    details: details.into(),
                });
            }
            Err(_) => {
                return Err(SourceError::Connect {
                    details: "could not resolve binlog tail during reconnect"
                        .into(),
                });
            }
        }
    };

    // The exact position the replacement stream starts from: what any
    // failover below must prove on the new server.
    let resume = match &gtid_to_use {
        Some(g) => ResumeAt::Gtid(g.clone()),
        None => ResumeAt::FilePos {
            file: file_to_use.clone().unwrap_or_default(),
            pos: pos_to_use.unwrap_or_default().into(),
        },
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

    let opened = connect_binlog_with_retries(
        &ctx.source_id,
        &expected,
        make_client.clone(),
        &ctx.cancel,
        &ctx.default_db,
        ctx.retry.clone(),
    )
    .await?;
    let found = match opened {
        Opened::Stream(stream) => {
            ctx.retry.reset();
            check_identity_post_reconnect(ctx, None, &resume).await?;
            return Ok(stream);
        }
        Opened::OtherServer(found) => found,
    };

    // Failover candidate. Nothing has been read from it and nothing durable
    // has changed; reconciliation proves it (on connections verified as
    // `found`) and only then records it.
    warn!(
        source_id = %ctx.source_id,
        %expected,
        candidate = %found,
        "reconnect reached another server; reconciling a possible failover"
    );
    let previous = mysql_identity(&expected);
    let current = mysql_identity(&found);
    run_failover_reconciliation(ctx, previous, current, &resume).await?;

    // The candidate is now the verified lineage: the fresh session must prove
    // it is that server before its dump command.
    let expected = ctx.expected_uuid()?;
    match connect_binlog_with_retries(
        &ctx.source_id,
        &expected,
        make_client,
        &ctx.cancel,
        &ctx.default_db,
        ctx.retry.clone(),
    )
    .await?
    {
        Opened::Stream(stream) => {
            ctx.retry.reset();
            Ok(stream)
        }
        Opened::OtherServer(other) => Err(SourceError::Lineage {
            details: format!(
                "after failover to {expected}, the replication session \
                 reached server {other}"
            )
            .into(),
        }),
    }
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

impl RunCtx {
    /// The stream is at a transaction boundary: rows of the next transaction
    /// are evaluated at the current position.
    pub(crate) fn mark_transaction_boundary(&mut self) {
        self.txn_eval = crate::durable_checkpoint::mysql_checkpoint_position(
            &self.last_file,
            self.last_pos,
            self.last_gtid.as_deref(),
        );
    }

    /// The verified `server_uuid` (the published registry lineage) that every
    /// connection of this run must prove it is before it is trusted.
    pub(crate) fn expected_uuid(&self) -> SourceResult<String> {
        match &self.registry_scope.current()?.lineage().descriptor {
            LineageDescriptor::Mysql { server_uuid } => Ok(server_uuid.clone()),
            other => Err(SourceError::Lineage {
                details: format!(
                    "MySQL source scoped to a non-MySQL lineage {other:?}"
                )
                .into(),
            }),
        }
    }
}

/// `@@lower_case_table_names` of the verified server: how DDL table names map
/// to registry keys (0: as written; 1: stored lower case; 2: compared case
/// insensitively but stored as written).
async fn fetch_lower_case_table_names(
    dsn: &str,
    expected_uuid: &str,
) -> SourceResult<u8> {
    use mysql_async::prelude::Queryable;
    let mut conn =
        open_control_connection(dsn, expected_uuid, CONTROL_CONNECT_TIMEOUT)
            .await
            .map_err(|e| e.into_source_error(expected_uuid))?;
    let value: Result<Option<u8>, _> =
        conn.query_first("SELECT @@lower_case_table_names").await;
    conn.disconnect().await.ok();
    match value {
        Ok(Some(v @ 0..=2)) => Ok(v),
        Ok(other) => Err(SourceError::Incompatible {
            details: format!("unexpected lower_case_table_names {other:?}")
                .into(),
        }),
        Err(e) => Err(SourceError::Connect {
            details: format!("read lower_case_table_names: {e}").into(),
        }),
    }
}

/// The lineage-wide barrier of a stream discontinuity at the position the
/// stream starts from (its identity is that position, so a repeated start at
/// the same position re-derives it).
async fn record_stream_start(ctx: &RunCtx) -> SourceResult<()> {
    use mysql_activation::{BarrierScope, EventIdentity, record_barrier};
    let position = crate::durable_checkpoint::mysql_checkpoint_position(
        &ctx.last_file,
        ctx.last_pos,
        ctx.last_gtid.as_deref(),
    )
    .ok_or_else(|| SourceError::Checkpoint {
        details: format!(
            "unparseable start position {}:{} {:?}",
            ctx.last_file, ctx.last_pos, ctx.last_gtid
        )
        .into(),
    })?;
    let key = ctx.registry_scope.current()?.key("", "");
    record_barrier(
        &ctx.registry_backend,
        ctx.schema.registry(),
        &key,
        BarrierScope::Lineage,
        &EventIdentity::StreamStart {
            position: position.clone(),
        },
        position,
    )
    .await
    .map_err(|e| {
        SourceError::Other(e.context("persist the stream-start barrier"))
    })?;
    info!(source_id = %ctx.source_id, "stream discontinuity: activation barrier at the start position");
    Ok(())
}

/// Advance the file/position cursor to the end of `header`'s event. The
/// server's artificial events at the start of a dump (the format description
/// after the fake rotate) carry no position (`0`): they never move it.
fn advance_position(
    ctx: &mut RunCtx,
    header: &mysql_binlog_connector_rust::event::event_header::EventHeader,
) {
    if header.next_event_position != 0 {
        ctx.last_pos = u64::from(header.next_event_position);
    }
}

/// Connect timeout for identity-verified control connections.
const CONTROL_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

fn mysql_identity(server_uuid: &str) -> ServerIdentity {
    ServerIdentity::MySql(mysql_health::MySqlServerIdentity {
        server_uuid: server_uuid.to_string(),
    })
}

/// The live identity, proven on a control connection to be `expected_uuid`
/// (the verified registry lineage). Transient connect failures are retried
/// (bounded); another server or an unverifiable identity is a lineage error.
async fn fetch_identity_verified_as(
    dsn: &str,
    expected_uuid: &str,
) -> SourceResult<ServerIdentity> {
    let mut attempt = 0u32;
    loop {
        match open_control_connection(
            dsn,
            expected_uuid,
            CONTROL_CONNECT_TIMEOUT,
        )
        .await
        {
            Ok(conn) => {
                conn.disconnect().await.ok();
                return Ok(mysql_identity(expected_uuid));
            }
            Err(SessionError::Connect(e)) => {
                attempt += 1;
                if attempt >= IDENTITY_FETCH_ATTEMPTS {
                    return Err(SourceError::Connect {
                        details: format!(
                            "failed to verify server identity after {attempt} \
                             attempts: {e}; refusing to stream on an unverified \
                             server"
                        )
                        .into(),
                    });
                }
                warn!(
                    attempt, error = %e,
                    "identity verification connect failed; retrying"
                );
                tokio::time::sleep(Duration::from_millis(
                    200 * 2u64.pow(attempt.min(5)),
                ))
                .await;
            }
            Err(e) => return Err(e.into_source_error(expected_uuid)),
        }
    }
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
/// failure fails closed (propagated), never mapped to `Same`. The live
/// identity is the run's verified lineage, proven on a control connection.
///
/// `prefetched` reuses an identity already verified by the caller (startup path)
/// so the live server is queried once; `None` fetches and verifies here (the
/// reconnect path).
async fn check_identity_post_reconnect(
    ctx: &mut RunCtx,
    prefetched: Option<ServerIdentity>,
    resume: &ResumeAt,
) -> SourceResult<()> {
    let expected = ctx.expected_uuid()?;
    let live = match prefetched {
        Some(live) => live,
        None => fetch_identity_verified_as(ctx.dsn.expose(), &expected).await?,
    };

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
                let reach = match open_control_connection(
                    ctx.dsn.expose(),
                    &expected,
                    CONTROL_CONNECT_TIMEOUT,
                )
                .await
                {
                    Ok(mut conn) => {
                        let r = check_position_reachability_on(
                            &mut conn,
                            &ctx.checkpoint_file,
                            ctx.checkpoint_gtid.as_deref(),
                        )
                        .await;
                        conn.disconnect().await.ok();
                        r.unwrap_or(PositionReachability::Unknown {
                            reason: "reachability check failed".into(),
                        })
                    }
                    Err(SessionError::Connect(e)) => {
                        PositionReachability::Unknown { reason: e }
                    }
                    Err(e) => return Err(e.into_source_error(&expected)),
                };
                match reach {
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
            run_failover_reconciliation(ctx, previous, current, resume).await?;
        }
    }

    Ok(())
}

/// The exact position a stream (re)opens from, which a failover must prove on
/// the new server.
#[derive(Debug, Clone)]
pub(crate) enum ResumeAt {
    Gtid(String),
    FilePos { file: String, pos: u64 },
}

impl ResumeAt {
    fn of(client: &BinlogClient) -> Self {
        if client.gtid_enabled {
            Self::Gtid(client.gtid_set.clone())
        } else {
            Self::FilePos {
                file: client.binlog_filename.clone(),
                pos: client.binlog_position.into(),
            }
        }
    }
}

/// Reconcile a failover from `previous` to `current` and only then make
/// `current` the verified lineage.
///
/// Cross-server failover is supported only in GTID mode: the exact GTID set
/// the stream resumes from must be proven executed by `current`
/// (`GTID_SUBSET` exactly 1); a file/position resume is never comparable
/// across servers and stops. Every live-server fact is read on ONE control
/// connection that first proves it is `current`.
///
/// The reconciliation record (loaded when a previous attempt persisted it,
/// else created) is the single source of the drift decision: drift is
/// derived from it and `on_schema_drift=halt` applied on every attempt.
/// Order: proof, record, halt, durable lineage edge (the expected server of
/// every later connection), schema reloads (failures stop: no stream opens),
/// and only then the identity record, so a restart after any failure
/// re-enters this path from the stored record. A crash before the lineage
/// edge keeps the previous lineage; after it, startup carries eligible GTID
/// checkpoints over.
async fn run_failover_reconciliation(
    ctx: &mut RunCtx,
    previous: ServerIdentity,
    current: ServerIdentity,
    resume: &ResumeAt,
) -> SourceResult<()> {
    let ServerIdentity::MySql(current_id) = &current else {
        return Err(SourceError::Lineage {
            details: format!("failover to a non-MySQL identity {current:?}")
                .into(),
        });
    };
    let current_uuid = current_id.server_uuid.clone();
    let resume_set = match resume {
        ResumeAt::Gtid(set) => set,
        ResumeAt::FilePos { file, pos } => {
            return Err(SourceError::Lineage {
                details: format!(
                    "failover from {previous:?} to {current_uuid} at \
                     {file}:{pos}: binlog positions are not comparable across \
                     servers; cross-server failover requires GTID mode"
                )
                .into(),
            });
        }
    };

    let mut conn = open_control_connection(
        ctx.dsn.expose(),
        &current_uuid,
        CONTROL_CONNECT_TIMEOUT,
    )
    .await
    .map_err(|e| e.into_source_error(&current_uuid))?;
    let record =
        failover_record(ctx, &mut conn, &previous, &current, resume_set).await;
    conn.disconnect().await.ok();
    let record = record?;

    let drifted: Vec<(String, String)> = record
        .table_results
        .iter()
        .filter(|r| !r.deltas.is_empty())
        .map(|r| (r.db.clone(), r.table.clone()))
        .collect();
    if !drifted.is_empty() {
        warn!(pipeline=%ctx.pipeline, source_id=%ctx.source_id, "schema drift detected after failover");
        if ctx.on_schema_drift == deltaforge_config::OnSchemaDrift::Halt {
            return Err(SourceError::Other(anyhow::anyhow!(
                "schema drift detected after failover and on_schema_drift=halt. \
                Verify B's schema and apply any missing migrations before restarting."
            )));
        }
    }

    // Reconciled: record the lineage edge (fail closed) and publish it - from
    // here on every connection must prove it is `current`.
    sync_registry_lineage(ctx, &current).await?;

    // Reload every drifted table under the new lineage before any stream
    // opens; a failure stops here and a restart repeats it from the record.
    for (db, table) in &drifted {
        ctx.schema.reload_schema(db, table).await?;
    }

    // Persist new identity only after everything above completed. Fail closed
    // if the durable write does not commit.
    ctx.identity_store
        .store(&ctx.source_id, &current)
        .await
        .map_err(SourceError::Other)?;

    // The stream resumes from the proven set: keep it (a later reconnect
    // must not lose the previous server's part of it). File/position
    // coordinates of the previous server mean nothing here.
    ctx.last_gtid = Some(resume_set.clone());
    ctx.current_gtid = None;
    ctx.message_ordinal = 0;
    ctx.query_ordinal = 0;
    ctx.last_file = String::new();
    ctx.last_pos = 0;

    info!(source_id = %ctx.source_id, "failover reconciliation complete");
    Ok(())
}

/// Prove the exact resume set on `conn` (verified as the failover candidate
/// by the caller), then return the reconciliation record: the one a previous
/// attempt persisted, else a new one built from the live columns of every
/// configured table read on `conn`.
async fn failover_record(
    ctx: &RunCtx,
    conn: &mut mysql_async::Conn,
    previous: &ServerIdentity,
    current: &ServerIdentity,
    resume_set: &str,
) -> SourceResult<crate::failover::reconciler::ReconciliationRecord> {
    mysql_health::require_gtid_executed(conn, resume_set)
        .await
        .map_err(|why| SourceError::Checkpoint {
            details: format!(
                "failover to {current:?}: the resume position {resume_set:?} \
                 is not proven on the new server ({why}). Re-snapshot required."
            )
            .into(),
        })?;

    if let Some(record) = ctx
        .reconciler
        .already_completed(&ctx.source_id, previous, current)
        .await
        .map_err(SourceError::Other)?
    {
        return Ok(record);
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
        // A failed read is not "table dropped": stop (retried by reconnect).
        let live_cols = mysql_health::fetch_live_columns_on(conn, &db, &table)
            .await
            .map_err(|e| SourceError::Connect {
                details: format!(
                    "failover live columns of {db}.{table}: {e:#}"
                )
                .into(),
            })?
            .map(|cols| cols.into_iter().map(Into::into).collect());
        inputs.push(ReconcileInput {
            db,
            table,
            live_columns: live_cols,
        });
    }

    // Diff against the last-known schemas of the lineage the source ran
    // under before this failover: the published scope while it still is
    // that lineage (reconnect), else its recorded predecessor (startup,
    // where the scope was established on the new server already).
    let scope_now = ctx.registry_scope.current()?;
    let prior_recorded;
    let prior = if scope_now.lineage().descriptor
        == mysql_registry_lineage(current)?
    {
        prior_recorded =
            previous_scope(&ctx.registry_backend, &ctx.tenant, &ctx.source_id)
                .await?;
        prior_recorded.as_ref()
    } else {
        Some(scope_now.as_ref())
    };
    ctx.reconciler
        .run(&ctx.source_id, previous, current, prior, &inputs)
        .await
        .map_err(SourceError::Other)
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

    fn gcp(file: &str, pos: u64, gtid: &str) -> Vec<u8> {
        format!(r#"{{"file":"{file}","pos":{pos},"gtid_set":"{gtid}"}}"#)
            .into_bytes()
    }

    /// Lexical file order is wrong once the index outgrows its zero padding;
    /// the numeric index decides.
    #[test]
    fn binlog_index_is_numeric_not_lexical() {
        assert_eq!(
            compare_mysql_checkpoints(
                &cp("bin.999999", 500),
                &cp("bin.1000000", 4)
            ),
            CheckpointOrder::Before
        );
        assert_eq!(
            compare_mysql_checkpoints(&cp("bin.000010", 4), &cp("bin.9", 900)),
            CheckpointOrder::After
        );
    }

    #[test]
    fn different_binlog_bases_are_incomparable() {
        assert_eq!(
            compare_mysql_checkpoints(
                &cp("bin.000001", 1),
                &cp("other.000001", 1)
            ),
            CheckpointOrder::Incomparable
        );
    }

    /// GTID checkpoints are ordered by set inclusion; the file/pos they also
    /// carry does not override it.
    #[test]
    fn gtid_sets_order_by_inclusion() {
        let u = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
        let small = gcp("bin.000009", 900, &format!("{u}:1-5"));
        let large = gcp("bin.000001", 4, &format!("{u}:1-9"));
        assert_eq!(
            compare_mysql_checkpoints(&small, &large),
            CheckpointOrder::Before
        );
        assert_eq!(
            compare_mysql_checkpoints(&large, &small),
            CheckpointOrder::After
        );
        assert_eq!(
            compare_mysql_checkpoints(&small, &small),
            CheckpointOrder::Equal
        );
        // Multi-line Executed_Gtid_Set formatting is accepted.
        let v = "4f2a0b1c-71ca-11e1-9e33-c80aa9429562";
        let a = gcp("bin.000001", 4, &format!("{u}:1-5,\\n{v}:1-2"));
        let b = gcp("bin.000001", 4, &format!("{u}:1-6,{v}:1-2"));
        assert_eq!(compare_mysql_checkpoints(&a, &b), CheckpointOrder::Before);
    }

    #[test]
    fn disjoint_gtid_sets_are_incomparable() {
        let u = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
        let v = "4f2a0b1c-71ca-11e1-9e33-c80aa9429562";
        assert_eq!(
            compare_mysql_checkpoints(
                &gcp("bin.000001", 1, &format!("{u}:1-5")),
                &gcp("bin.000001", 2, &format!("{v}:1-5"))
            ),
            CheckpointOrder::Incomparable
        );
    }

    #[test]
    fn gtid_against_file_pos_is_incomparable() {
        let u = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
        assert_eq!(
            compare_mysql_checkpoints(
                &gcp("bin.000001", 1, &format!("{u}:1-5")),
                &cp("bin.000002", 1)
            ),
            CheckpointOrder::Incomparable
        );
    }

    #[test]
    fn malformed_gtid_or_file_is_incomparable() {
        assert_eq!(
            compare_mysql_checkpoints(
                &gcp("bin.000001", 1, "not-a-gtid-set"),
                &gcp("bin.000001", 1, "not-a-gtid-set")
            ),
            CheckpointOrder::Incomparable
        );
        assert_eq!(
            compare_mysql_checkpoints(&cp("binlog", 1), &cp("binlog", 1)),
            CheckpointOrder::Incomparable
        );
    }

    const LA: &str = "0123456789abcdef0123456789abcdef";
    const LB: &str = "fedcba9876543210fedcba9876543210";

    /// Identical but malformed lineage strings never make two checkpoints
    /// comparable.
    #[test]
    fn malformed_lineages_are_incomparable_even_when_identical() {
        for bad in ["lineage-a", "0123456789ABCDEF0123456789ABCDEF", ""] {
            let a = lcp("binlog.000010", 400, None, bad);
            assert_eq!(
                compare_mysql_checkpoints(&a, &a),
                CheckpointOrder::Incomparable,
                "{bad:?}"
            );
        }
    }

    fn lcp(file: &str, pos: u64, gtid: Option<&str>, lineage: &str) -> Vec<u8> {
        serde_json::to_vec(&super::MySqlCheckpoint {
            file: file.into(),
            pos,
            gtid_set: gtid.map(str::to_string),
            lineage: Some(lineage.into()),
        })
        .unwrap()
    }

    /// The same coordinates on two different servers are never ordered - in
    /// file/pos mode (identical binlog names are common) and in GTID mode.
    #[test]
    fn same_coordinates_on_different_lineages_are_incomparable() {
        let u = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
        let g = format!("{u}:1-5");
        for gtid in [None, Some(g.as_str())] {
            let a = lcp("binlog.000010", 400, gtid, LA);
            let b = lcp("binlog.000010", 400, gtid, LB);
            assert_eq!(
                compare_mysql_checkpoints(&a, &b),
                CheckpointOrder::Incomparable,
                "gtid {gtid:?}"
            );
            let later = lcp("binlog.000011", 4, gtid, LB);
            assert_eq!(
                compare_mysql_checkpoints(&a, &later),
                CheckpointOrder::Incomparable,
                "gtid {gtid:?}"
            );
        }
    }

    #[test]
    fn same_lineage_orders_by_position() {
        let u = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
        assert_eq!(
            compare_mysql_checkpoints(
                &lcp("binlog.000010", 400, None, LA),
                &lcp("binlog.000011", 4, None, LA)
            ),
            CheckpointOrder::Before
        );
        assert_eq!(
            compare_mysql_checkpoints(
                &lcp("binlog.000010", 400, Some(&format!("{u}:1-9")), LA),
                &lcp("binlog.000010", 400, Some(&format!("{u}:1-5")), LA)
            ),
            CheckpointOrder::After
        );
    }

    /// A lineage-bearing checkpoint and a legacy one are never ordered; two
    /// legacy checkpoints keep the position comparison.
    #[test]
    fn legacy_and_lineage_checkpoints_do_not_mix() {
        let u = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
        let g = format!("{u}:1-5");
        assert_eq!(
            compare_mysql_checkpoints(
                &lcp("binlog.000010", 400, None, LA),
                &cp("binlog.000011", 4)
            ),
            CheckpointOrder::Incomparable
        );
        assert_eq!(
            compare_mysql_checkpoints(
                &gcp("binlog.000010", 400, &g),
                &lcp("binlog.000010", 400, Some(&g), LA)
            ),
            CheckpointOrder::Incomparable
        );
        assert_eq!(
            compare_mysql_checkpoints(
                &cp("binlog.000010", 400),
                &cp("binlog.000011", 4)
            ),
            CheckpointOrder::Before
        );
    }

    /// Checkpoints written before lineage existed still parse.
    #[test]
    fn pre_lineage_checkpoint_bytes_parse_as_legacy() {
        let cp: super::MySqlCheckpoint = serde_json::from_slice(
            br#"{"file":"binlog.000001","pos":4,"gtid_set":null}"#,
        )
        .unwrap();
        assert_eq!(cp.lineage, None);
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
