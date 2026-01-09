//! PostgreSQL CDC source implementation using logical replication.

use std::{
    collections::HashMap,
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

use async_trait::async_trait;
use pgwire_replication::{Lsn, ReplicationClient};
use schema_registry::InMemoryRegistry;
use serde::{Deserialize, Serialize};
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

// ============================================================================
// Checkpoint
// ============================================================================

/// PostgreSQL checkpoint structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresCheckpoint {
    pub lsn: String,
    pub tx_id: Option<u32>,
}

// ============================================================================
// Source Configuration
// ============================================================================

/// PostgreSQL CDC source.
#[derive(Debug, Clone)]
pub struct PostgresSource {
    pub id: String,
    pub checkpoint_key: String,
    pub dsn: String,
    pub slot: String,
    pub publication: String,
    pub tables: Vec<String>,
    pub tenant: String,
    pub pipeline: String,
    pub registry: Arc<InMemoryRegistry>,
}

// ============================================================================
// Runtime Context
// ============================================================================

/// Shared state for the PostgreSQL source run loop.
pub(crate) struct RunCtx {
    pub source_id: String,
    pub pipeline: String,
    pub tenant: String,
    pub host: String,
    pub default_schema: String,
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
}

// ============================================================================
// Source Implementation
// ============================================================================

/// Maximum backoff for startup retries (publication/slot verification).
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

        // Verify publication exists and ensure slot exists.
        // Retries with backoff if publication missing (admin can create it).
        let mut startup_retry = RetryPolicy::default();
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
                    Err(LoopControl::ReloadSchema { .. }) => {
                        // Shouldn't happen during startup, treat as retry
                        continue;
                    }
                }
            }
        } else {
            config.start_lsn
        };

        let config = pgwire_replication::ReplicationConfig {
            start_lsn,
            ..config
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
                            schema = %schema, table = %table, replica_identity = %identity,
                            "table does not have REPLICA IDENTITY FULL - before images will be incomplete"
                        );
                    }
                }
            }
        }

        info!(
            source_id = %self.id, host = %components.host, slot = %self.slot,
            publication = %self.publication, start_lsn = %start_lsn,
            "postgres source starting"
        );

        let client = connect_replication_with_retries(
            &self.id,
            config.clone(),
            &cancel,
            RetryPolicy::default(),
        )
        .await?;

        let mut ctx = RunCtx {
            source_id: self.id.clone(),
            pipeline: self.pipeline.clone(),
            tenant: self.tenant.clone(),
            host: components.host.clone(),
            default_schema: "public".to_string(),
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
        };

        info!("entering replication loop");
        loop {
            if !pause_until_resumed(&ctx.cancel, &ctx.paused, &ctx.pause_notify)
                .await
            {
                break;
            }

            debug!(source_id = %self.id, "reading next event");

            // Read next event, may return LoopControl on error
            let event_result = match read_next_event(&ctx).await {
                Ok(Some(event)) => {
                    // Dispatch event, may return LoopControl for schema reload
                    dispatch_event(&mut ctx, event).await
                }
                Ok(None) => {
                    info!(source_id = %self.id, "replication stream ended");
                    break;
                }
                Err(ctrl) => Err(ctrl),
            };

            // Handle LoopControl from either read or dispatch
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
                    // Continue without reconnect for schema changes detected in-stream
                    // (the relation message already updated our relation_map)
                }
                Err(LoopControl::Reconnect) => {
                    let delay = ctx.retry.next_backoff();
                    warn!(source_id = %self.id, delay_ms = delay.as_millis(), "scheduling reconnect after backoff");

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
                        }
                        Err(e) => {
                            error!(source_id = %self.id, error = %e, "reconnect failed after retries");
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

        // Best-effort final checkpoint
        let _ = chkpt_store
            .put(
                &self.id,
                PostgresCheckpoint {
                    lsn: ctx.last_lsn.to_string(),
                    tx_id: None,
                },
            )
            .await;

        // Shutdown replication client
        if let Err(e) = ctx.repl_client.lock().await.shutdown().await {
            warn!(error = %e, "error during replication client shutdown");
        }

        Ok(())
    }
}

#[async_trait]
impl Source for PostgresSource {
    fn checkpoint_key(&self) -> &str {
        &self.checkpoint_key
    }

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
