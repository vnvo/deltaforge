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
use deltaforge_core::{Event, Source, SourceError, SourceHandle, SourceResult};
mod mysql_errors;
pub use mysql_errors::{LoopControl, MySqlSourceError, MySqlSourceResult};

mod mysql_helpers;
use mysql_helpers::prepare_client;

mod mysql_object;

mod mysql_schema_loader;
pub use mysql_schema_loader::{LoadedSchema, MySqlSchemaLoader};

mod mysql_event;
use mysql_event::*;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MySqlCheckpoint {
    pub file: String,
    pub pos: u64,
    pub gtid_set: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MySqlSource {
    pub id: String,
    pub dsn: String,
    pub tables: Vec<String>,
    pub tenant: String,
    pub pipeline: String,
    pub registry: Arc<DurableSchemaRegistry>,
    pub backend: ArcStorageBackend,
    pub outbox_tables: AllowList,
    pub snapshot_cfg: deltaforge_config::SnapshotCfg,
    pub on_schema_drift: deltaforge_config::OnSchemaDrift,
}

const HEARTBEAT_INTERVAL_SECS: u64 = 15;
const READ_TIMEOUT: u64 = 90;

struct RunCtx {
    source_id: String,
    pipeline: String,
    tenant: String,
    dsn: String,
    #[allow(dead_code)]
    host: String,
    default_db: String,
    server_id: u64,
    tx: mpsc::Sender<Event>,
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
    /// Original checkpoint position, preserved even after a pre-connect failover
    /// adjustment clears last_gtid/last_file. Used by check_position_reachability
    /// to verify whether A's position actually exists on B.
    checkpoint_gtid: Option<String>,
    checkpoint_file: String,
    tables: Vec<String>,
    outbox_tables: AllowList,
    identity_store: IdentityStore,
    reconciler: SchemaReconciler,
    on_schema_drift: deltaforge_config::OnSchemaDrift,
}

impl MySqlSource {
    async fn run_inner(
        &self,
        tx: mpsc::Sender<Event>,
        chkpt_store: Arc<dyn CheckpointStore>,
        cancel: CancellationToken,
        paused: Arc<AtomicBool>,
        pause_notify: Arc<Notify>,
    ) -> SourceResult<()> {
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
                &self.dsn,
                self.registry.clone(),
                &self.tenant,
            );
            let tracked = snap_schema_loader.preload(&self.tables).await?;
            let snapshot_ctx = mysql_snapshot::SnapshotCtx {
                dsn: &self.dsn,
                source_id: &self.id,
                pipeline: &self.pipeline,
                tenant: &self.tenant,
                cfg: &self.snapshot_cfg,
                schema_loader: &snap_schema_loader,
                chkpt_store: chkpt_store.clone(),
                tx: tx.clone(),
                cancel: cancel.clone(),
            };
            let snapshot_position =
                mysql_snapshot::run_snapshot(&snapshot_ctx, &tracked)
                    .await
                    .map_err(SourceError::Other)?;

            chkpt_store
                .put(&self.id, snapshot_position)
                .await
                .map_err(|e| SourceError::Other(e.into()))?;

            info!(source_id = %self.id, "snapshot complete, starting binlog streaming");
        }

        // binlog streaming
        let (host, default_db, server_id, mut client) =
            prepare_client(&self.dsn, &self.id, &chkpt_store).await?;

        // Capture the original checkpoint position BEFORE any adjustment.
        // This is used later by check_position_reachability to determine whether
        // A's position actually exists on B - even if we've already switched the
        // connection to B's tail.
        let checkpoint_gtid =
            client.gtid_enabled.then(|| client.gtid_set.clone());
        let checkpoint_file = client.binlog_filename.clone();

        // Pre-connect failover adjustment: if A's GTID checkpoint would be rejected
        // by B ("purged required binary logs"), switch to B's tail before capturing
        // init_gtid so the first stream opens cleanly. Without this, the "purged"
        // error fires on the first stream read and the reconnect loop re-sends the
        // stale GTID forever (identity_store already stores B after reconciliation,
        // so the in-loop pre-connect check always sees Same).
        if client.gtid_enabled {
            let id_store = IdentityStore::new(Arc::clone(&self.backend));
            if let Ok(Some(live_id)) = fetch_server_identity(&self.dsn).await {
                let live = ServerIdentity::from(live_id);
                if matches!(
                    id_store.compare(&self.id, &live).await,
                    Ok(IdentityComparison::Changed { .. })
                ) {
                    match resolve_binlog_tail(&self.dsn).await {
                        Ok((fname, fpos)) => {
                            warn!(
                                source_id = %self.id,
                                "pre-connect failover: switching from A's GTID to B's binlog tail"
                            );
                            client.gtid_enabled = false;
                            client.gtid_set = String::new();
                            client.binlog_filename = fname;
                            client.binlog_position = fpos as u32;
                        }
                        Err(e) => {
                            warn!(source_id = %self.id, error = %e,
                                "pre-connect: could not resolve B's tail, will attempt with stale GTID");
                        }
                    }
                }
            }
        }

        let init_gtid = client.gtid_enabled.then(|| client.gtid_set.clone());
        let init_file = client.binlog_filename.clone();
        let init_pos = client.binlog_position as u64;

        info!(source_id=%self.id, "prepare_client finished, loading schemas");
        let schema_loader = MySqlSchemaLoader::new(
            &self.dsn,
            self.registry.clone(),
            &self.tenant,
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
                self.tenant.clone(),
            ),
            inactivity: Duration::from_secs(60),
            table_map: HashMap::new(),
            last_file: init_file,
            last_pos: init_pos,
            last_gtid: init_gtid,
            checkpoint_gtid,
            checkpoint_file,
            tables: self.tables.clone(),
            outbox_tables: self.outbox_tables.clone(),
            on_schema_drift: self.on_schema_drift.clone(),
        };

        info!(source_id=%self.id, "connecting for binlog stream ..");
        let mut stream = connect_first_stream(&ctx, client).await?;

        // Identity check before preload: registry still holds A's schema here.
        check_identity_post_reconnect(&mut ctx).await?;

        // Safe to preload now: reconciliation has run, registry reflects post-reconcile state.
        let tracked = ctx.schema.preload(&self.tables).await?;
        info!(source_id=%self.id, tables = tracked.len(), "schemas preloaded");

        info!("entering binlog read loop");
        loop {
            if !pause_until_resumed(&ctx.cancel, &ctx.paused, &ctx.pause_notify)
                .await
            {
                info!(source_id=%ctx.source_id, "resuming ..");
                break;
            }

            debug!(source_id=%ctx.source_id, "reading the next event ..");
            match read_next_event(&mut stream, &ctx).await {
                Ok((header, data)) => {
                    ctx.last_pos = header.next_event_position as u64;
                    dispatch_event(&mut ctx, &header, data).await?;
                }
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

        // best-effort final checkpoint update
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

        Ok(())
    }
}

#[async_trait]
impl Source for MySqlSource {
    async fn run(
        &self,
        tx: mpsc::Sender<Event>,
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
            url: dsn.clone(),
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
        match resolve_binlog_tail(&ctx.dsn).await {
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
            url: dsn.clone(),
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

    check_identity_post_reconnect(ctx).await?;

    Ok(stream)
}

/// Apply backoff, sleep (cancel-aware), reconnect, and absorb transient errors.
///
/// Returns:
/// - `Ok(Some(stream))` - reconnected, caller assigns the new stream.
/// - `Ok(None)` — cancelled during sleep or transient connect error;
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

/// Compare the live server identity against the stored one.
///
/// - `FirstSeen`: store and continue (clean start or wiped state).
/// - `Same`: normal reconnect, nothing to do.
/// - `Changed`: run full failover reconciliation before returning.
async fn check_identity_post_reconnect(ctx: &mut RunCtx) -> SourceResult<()> {
    let live_mysql = match fetch_server_identity(&ctx.dsn).await {
        Ok(Some(id)) => id,
        Ok(None) => return Ok(()), // MySQL < 5.6 or unavailable - skip silently
        Err(e) => {
            warn!(source_id = %ctx.source_id, error = %e, "could not fetch server identity, skipping check");
            return Ok(());
        }
    };
    let live = ServerIdentity::from(live_mysql);

    match ctx
        .identity_store
        .compare(&ctx.source_id, &live)
        .await
        .unwrap_or(IdentityComparison::Same) // transient storage error -> treat as same
    {
        IdentityComparison::FirstSeen => {
            let _ = ctx.identity_store.store(&ctx.source_id, &live).await;
        }
        IdentityComparison::Same => {
            // A RESET BINARY LOGS AND GTIDS wipes the GTID history without
            // changing the server UUID.  Run the position check here too so
            // we catch that case on the first reconnect after a purge.
            // Skip when there is no GTID checkpoint (file/pos mode or fresh
            // start) to avoid false positives from the file-presence fallback.
            if ctx.checkpoint_gtid.is_some() {
                match check_position_reachability(
                    &ctx.dsn,
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
                "server identity changed — failover detected, reconciling"
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
        // Position reachability — use the original checkpoint position, not the
        // (potentially adjusted) streaming position in last_gtid/last_file.
        match check_position_reachability(
            &ctx.dsn,
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
                    "could not verify position reachability after failover — resuming anyway"
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
            > = mysql_health::fetch_live_columns(&ctx.dsn, &db, &table)
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

        let record = ctx
            .reconciler
            .run(&ctx.source_id, &previous, &current, &inputs)
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

    // Persist new identity only after reconciliation completes.
    let _ = ctx.identity_store.store(&ctx.source_id, &current).await;

    // Clear streaming position so subsequent reconnects resolve B's binlog tail
    // rather than re-sending A's GTID. Covers mid-run failovers where the stream
    // was already open when the switch happened.
    ctx.last_gtid = None;
    ctx.last_file = String::new();
    ctx.last_pos = 0;

    info!(source_id = %ctx.source_id, "failover reconciliation complete");
    Ok(())
}
