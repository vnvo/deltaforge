//! PostgreSQL CDC source implementation using logical replication.

use std::{
    collections::HashMap,
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
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
use deltaforge_core::{Event, Source, SourceError, SourceHandle, SourceResult};

mod postgres_errors;
use postgres_errors::LoopControl;
pub use postgres_errors::{PostgresSourceError, PostgresSourceResult};

mod postgres_helpers;
use postgres_helpers::{
    connect_replication_with_retries, ensure_slot_and_publication,
    prepare_replication_client,
};

mod postgres_object;

mod postgres_schema_loader;
pub use postgres_schema_loader::{LoadedSchema, PostgresSchemaLoader};

mod postgres_event;
pub use postgres_event::RelationInfo;
use postgres_event::*;

mod postgres_table_schema;
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
    PositionReachability, check_position_reachability, fetch_server_identity,
};

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
    pub dsn: String,
    pub slot: String,
    pub publication: String,
    pub tables: Vec<String>,
    pub tenant: String,
    pub pipeline: String,
    pub registry: Arc<DurableSchemaRegistry>,
    pub backend: ArcStorageBackend,
    pub outbox_prefixes: AllowList,
    pub snapshot_cfg: deltaforge_config::SnapshotCfg,
    pub on_schema_drift: OnSchemaDrift,
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
    pub dsn: String,
    pub slot: String,
    pub tx: mpsc::Sender<Event>,
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
    pub repl_client: Arc<Mutex<ReplicationClient>>,
    pub outbox_prefixes: AllowList,
    pub identity_store: IdentityStore,
    pub reconciler: SchemaReconciler,
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

impl PostgresSource {
    async fn run_inner(
        &self,
        tx: mpsc::Sender<Event>,
        chkpt_store: Arc<dyn CheckpointStore>,
        cancel: CancellationToken,
        paused: Arc<AtomicBool>,
        pause_notify: Arc<Notify>,
    ) -> SourceResult<()> {
        let (components, config, last_checkpoint) = prepare_replication_client(
            &self.dsn,
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
                match ensure_slot_and_publication(
                    &self.dsn,
                    &self.slot,
                    &self.publication,
                    &self.tables,
                )
                .await
                {
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
                }
            }
        } else {
            // Resuming from a checkpoint: verify the replication slot still exists
            // before trusting the saved LSN. A dropped slot means the WAL position
            // is permanently lost — halt rather than silently reconnecting.
            match check_position_reachability(&self.dsn, &self.slot).await {
                Ok(PositionReachability::Lost { reason }) => {
                    error!(
                        source_id = %self.id,
                        slot = %self.slot,
                        %reason,
                        "replication slot lost — checkpoint position is unreachable, halting"
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
            &self.dsn,
            self.registry.clone(),
            &self.tenant,
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
            let snapshot_ctx = postgres_snapshot::PgSnapshotCtx {
                dsn: &self.dsn,
                source_id: &self.id,
                pipeline: &self.pipeline,
                tenant: &self.tenant,
                cfg: &self.snapshot_cfg,
                schema_loader: &schema_loader,
                chkpt_store: chkpt_store.clone(),
                tx: tx.clone(),
                cancel: cancel.clone(),
                slot_name: Some(&self.slot),
            };
            postgres_snapshot::run_snapshot(&snapshot_ctx, &tracked)
                .await
                .map_err(SourceError::Other)?
        } else {
            start_lsn
        };

        // Adjust start_lsn BEFORE opening the replication stream.
        // If a failover has occurred, START_REPLICATION with A's stale LSN would
        // advance B's slot.confirmed_flush_lsn past B's uncommitted changes, making
        // them permanently invisible even if we reconnect from the correct LSN later.
        let start_lsn = {
            let id_store = IdentityStore::new(Arc::clone(&self.backend));
            pre_connect_lsn_adjust(
                &self.dsn, &self.slot, start_lsn, &id_store, &self.id,
            )
            .await
        };

        info!(
            source_id = %self.id, host = %components.host, slot = %self.slot,
            publication = %self.publication, start_lsn = %start_lsn,
            "postgres source starting"
        );

        let config = pgwire_replication::ReplicationConfig {
            start_lsn,
            ..config
        };

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
            repl_client: Arc::new(Mutex::new(client)),
            outbox_prefixes: self.outbox_prefixes.clone(),
            identity_store: IdentityStore::new(Arc::clone(&backend)),
            reconciler: SchemaReconciler::new(
                Arc::clone(&self.registry),
                Arc::clone(&backend),
                self.tenant.clone(),
            ),
            on_schema_drift: self.on_schema_drift.clone(),
            counter_cache: HashMap::new(),
            cached_lsn: None,
        };

        // Store initial server identity (FirstSeen path).
        check_identity_post_reconnect(&mut ctx).await?;

        // If failover was detected, ctx.last_lsn was reset to B's slot position.
        // The existing stream was opened from A's stale LSN — reconnect from the correct point.
        if ctx.last_lsn != start_lsn {
            let reconnect_config = pgwire_replication::ReplicationConfig {
                start_lsn: ctx.last_lsn,
                ..config.clone()
            };
            let new_client = connect_replication_with_retries(
                &self.id,
                reconnect_config,
                &cancel_ref,
                RetryPolicy::default(),
            )
            .await?;
            *ctx.repl_client.lock().await = new_client;
        }

        info!("entering replication loop");
        loop {
            if !pause_until_resumed(&ctx.cancel, &ctx.paused, &ctx.pause_notify)
                .await
            {
                break;
            }

            debug!(source_id = %self.id, "reading next event");

            let event_result = match read_next_event(&ctx).await {
                Ok(Some(event)) => dispatch_event(&mut ctx, event).await,
                Ok(None) => {
                    info!(source_id = %self.id, "replication stream ended");
                    break;
                }
                Err(ctrl) => Err(ctrl),
            };

            match event_result {
                Ok(()) => {}
                Err(LoopControl::ReloadSchema { schema, table }) => {
                    if let (Some(s), Some(t)) = (schema, table) {
                        info!(schema = %s, table = %t, "reloading schema");
                        let _ = ctx.schema.reload_schema(&s, &t).await;
                    } else {
                        info!("reloading all schemas");
                        let _ = ctx.schema.reload_all(&self.tables).await;
                    }
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
                        pgwire_replication::ReplicationConfig {
                            start_lsn: ctx.last_lsn,
                            ..config.clone()
                        };

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

        let _ = chkpt_store
            .put(
                &self.id,
                PostgresCheckpoint {
                    lsn: ctx.last_lsn.to_string(),
                    tx_id: None,
                },
            )
            .await;

        if let Err(e) = ctx.repl_client.lock().await.shutdown().await {
            warn!(error = %e, "error during replication client shutdown");
        }

        Ok(())
    }
}

#[async_trait]
impl Source for PostgresSource {
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
}

// ============================================================================
// Failover detection + reconciliation
// ============================================================================

/// Adjusts `start_lsn` for a possible failover **before** opening the replication stream.
///
/// `START_REPLICATION` immediately advances the slot's `confirmed_flush_lsn` to
/// `max(start_lsn, slot.confirmed_flush_lsn)`.  If we start with A's stale checkpoint
/// LSN on a fresh B whose slot is behind that checkpoint, PostgreSQL will skip any
/// changes B committed between its slot creation LSN and A's checkpoint — even if we
/// reconnect from the correct LSN afterwards.
///
/// By fetching the correct start LSN before the first replication connection, we avoid
/// permanently advancing the slot past unread data.
async fn pre_connect_lsn_adjust(
    dsn: &str,
    slot: &str,
    start_lsn: Lsn,
    id_store: &IdentityStore,
    source_id: &str,
) -> Lsn {
    let live_pg = match fetch_server_identity(dsn).await {
        Ok(Some(id)) => id,
        _ => return start_lsn,
    };
    let live = ServerIdentity::from(live_pg);

    match id_store
        .compare(source_id, &live)
        .await
        .unwrap_or(IdentityComparison::Same)
    {
        IdentityComparison::Changed { .. } => {
            // Failover detected: use the slot's actual confirmed_flush_lsn on B.
            match fetch_slot_confirmed_lsn(dsn, slot).await {
                Ok(slot_lsn) => {
                    debug!(
                        source_id = %source_id,
                        original_lsn = %start_lsn,
                        slot_lsn = %slot_lsn,
                        "pre-connect failover adjustment: using slot LSN"
                    );
                    slot_lsn
                }
                Err(e) => {
                    warn!(
                        source_id = %source_id, error = %e,
                        "could not fetch slot LSN for pre-connect adjustment, using checkpoint LSN"
                    );
                    start_lsn
                }
            }
        }
        _ => start_lsn,
    }
}

async fn check_identity_post_reconnect(ctx: &mut RunCtx) -> SourceResult<()> {
    let live_pg = match fetch_server_identity(&ctx.dsn).await {
        Ok(Some(id)) => id,
        Ok(None) => return Ok(()),
        Err(e) => {
            warn!(
                source_id = %ctx.source_id, error = %e,
                "could not fetch server identity, skipping check"
            );
            return Ok(());
        }
    };
    let live = ServerIdentity::from(live_pg);

    match ctx
        .identity_store
        .compare(&ctx.source_id, &live)
        .await
        .unwrap_or(IdentityComparison::Same)
    {
        IdentityComparison::FirstSeen => {
            let _ = ctx.identity_store.store(&ctx.source_id, &live).await;
        }
        IdentityComparison::Same => {}
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
    let existing = ctx
        .reconciler
        .already_completed(&ctx.source_id, &previous, &current)
        .await
        .unwrap_or(None);

    if existing.is_none() {
        // Position reachability via slot state.
        match check_position_reachability(&ctx.dsn, &ctx.slot)
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

        if let Ok(slot_lsn) =
            fetch_slot_confirmed_lsn(&ctx.dsn, &ctx.slot).await
        {
            ctx.last_lsn = slot_lsn;
        }

        // Schema diff against live catalog.
        let tracked = ctx.schema.cached_tables();
        let mut inputs = Vec::with_capacity(tracked.len());
        for (schema, table) in &tracked {
            let live_cols: Option<
                Vec<crate::failover::reconciler::ColumnSnapshot>,
            > = postgres_health::fetch_live_columns(&ctx.dsn, schema, table)
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

        let record = ctx
            .reconciler
            .run(&ctx.source_id, &previous, &current, &inputs)
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

    let _ = ctx.identity_store.store(&ctx.source_id, &current).await;

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
