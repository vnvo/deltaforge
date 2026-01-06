//! PostgreSQL CDC source implementation using logical replication.
//!
//! This source uses the pgwire-replication crate to connect to PostgreSQL's
//! logical replication protocol and stream changes via the pgoutput plugin.
//!
//! # Requirements
//!
//! PostgreSQL must be configured for logical replication:
//! ```sql
//! -- postgresql.conf
//! wal_level = logical
//! max_replication_slots = 10
//! max_wal_senders = 10
//!
//! -- Create replication slot and publication
//! SELECT pg_create_logical_replication_slot('my_slot', 'pgoutput');
//! CREATE PUBLICATION my_pub FOR ALL TABLES;
//! ```
//!
//! # Example Configuration
//!
//! ```yaml
//! source:
//!   type: postgres
//!   config:
//!     id: orders-postgres
//!     dsn: "postgres://user:pass@localhost:5432/mydb"
//!     slot: my_slot
//!     publication: my_pub
//!     tables:
//!       - public.orders
//!       - public.order_items
//! ```

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
use tracing::{error, info, warn};

use checkpoints::{CheckpointStore, CheckpointStoreExt};
use deltaforge_core::{Event, Source, SourceHandle, SourceResult};

mod postgres_errors;
pub use postgres_errors::PostgresSourceError;

mod postgres_helpers;
use postgres_helpers::{
    AllowList, connect_replication_with_retries, ensure_slot_and_publication,
    parse_dsn, pause_until_resumed, prepare_replication_client,
};

mod postgres_object;

mod postgres_schema_loader;
pub use postgres_schema_loader::{LoadedSchema, PostgresSchemaLoader};

mod postgres_event;
use postgres_event::*;

mod postgres_table_schema;
pub use postgres_table_schema::{PostgresColumn, PostgresTableSchema};

use crate::conn_utils::RetryPolicy;

pub type PostgresSourceResult<T> = Result<T, PostgresSourceError>;

// ============================================================================
// Checkpoint
// ============================================================================

/// PostgreSQL checkpoint structure.
///
/// Tracks position using LSN (Log Sequence Number).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresCheckpoint {
    /// WAL LSN in format "X/Y" (e.g., "0/16B6C50")
    pub lsn: String,
    /// Transaction ID (if within a transaction)
    pub tx_id: Option<u32>,
}

// ============================================================================
// Source Configuration
// ============================================================================

/// PostgreSQL CDC source.
#[derive(Debug, Clone)]
pub struct PostgresSource {
    /// Source identifier (used for checkpoints and metrics)
    pub id: String,
    /// Checkpoint storage key
    pub checkpoint_key: String,
    /// PostgreSQL DSN (URL or key=value format)
    pub dsn: String,
    /// Replication slot name
    pub slot: String,
    /// Publication name
    pub publication: String,
    /// Tables to capture (supports wildcards)
    pub tables: Vec<String>,
    /// Tenant identifier
    pub tenant: String,
    /// Pipeline name
    pub pipeline: String,
    /// Schema registry
    pub registry: Arc<InMemoryRegistry>,
}

// ============================================================================
// Runtime Context
// ============================================================================

/// Shared state for the PostgreSQL source run loop.
pub(super) struct RunCtx {
    pub source_id: String,
    pub pipeline: String,
    pub tenant: String,
    pub host: String,
    pub default_schema: String,
    pub tx: mpsc::Sender<Event>,
    pub chkpt: Arc<dyn CheckpointStore>,
    pub cancel: CancellationToken,
    pub paused: Arc<AtomicBool>,
    pub pause_notify: Arc<Notify>,
    pub schema: PostgresSchemaLoader,
    pub allow: AllowList,
    pub retry: RetryPolicy,
    pub inactivity: Duration,
    /// Relation metadata cache
    pub relation_map: HashMap<u32, RelationInfo>,
    /// Current LSN position
    pub last_lsn: Lsn,
    /// Current transaction ID (during transaction)
    pub current_tx_id: Option<u32>,
    /// Current transaction commit time
    pub current_tx_commit_time: Option<i64>,
    /// Replication client (wrapped in mutex for interior mutability)
    pub repl_client: Arc<Mutex<ReplicationClient>>,
}

// ============================================================================
// Source Implementation
// ============================================================================

impl PostgresSource {
    async fn run_inner(
        &self,
        tx: mpsc::Sender<Event>,
        chkpt_store: Arc<dyn CheckpointStore>,
        cancel: CancellationToken,
        paused: Arc<AtomicBool>,
        pause_notify: Arc<Notify>,
    ) -> SourceResult<()> {
        // Parse DSN and prepare configuration
        let (components, config, last_checkpoint) = prepare_replication_client(
            &self.dsn,
            &self.id,
            &self.slot,
            &self.publication,
            &chkpt_store,
        )
        .await?;

        // Ensure publication and slot exist if no checkpoint
        let start_lsn = if last_checkpoint.is_none() {
            ensure_slot_and_publication(
                &self.dsn,
                &self.slot,
                &self.publication,
                &self.tables,
            )
            .await?
        } else {
            config.start_lsn
        };

        // Update config with start LSN
        let config = pgwire_replication::ReplicationConfig {
            start_lsn,
            ..config
        };

        // Create schema loader
        let schema_loader = PostgresSchemaLoader::new(
            &self.dsn,
            self.registry.clone(),
            &self.tenant,
        );

        // Preload schemas
        let tracked = schema_loader.preload(&self.tables).await?;
        info!(tables = tracked.len(), "schemas preloaded");

        for (schema, table) in &tracked {
            if let Ok(loaded) = schema_loader.load_schema(schema, table).await {
                if let Some(ref identity) = loaded.schema.replica_identity {
                    if identity != "full" {
                        warn!(
                            schema = %schema,
                            table = %table,
                            replica_identity = %identity,
                            "table does not have REPLICA IDENTITY FULL - before images will be incomplete. \
                            Consider: ALTER TABLE {}.{} REPLICA IDENTITY FULL",
                            schema, table
                        );
                    }
                }
            }
        }

        info!(
            source_id = %self.id,
            host = %components.host,
            slot = %self.slot,
            publication = %self.publication,
            start_lsn = %start_lsn,
            "postgres source starting ..."
        );

        // Connect to replication
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

            match read_next_event(&ctx).await {
                Ok(Some(event)) => {
                    dispatch_event(&mut ctx, event).await?;
                }
                Ok(None) => {
                    info!(source_id = %self.id, "replication stream ended");
                    break;
                }
                Err(LoopControl::Reconnect) => {
                    // Reconnect logic
                    info!(source_id = %self.id, "reconnecting to replication...");
                    let delay = ctx.retry.next_backoff();
                    warn!(
                        source_id = %self.id,
                        delay_ms = delay.as_millis(),
                        "scheduling reconnect after backoff"
                    );

                    // Wait with cancellation support
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
                            ctx.retry.reset(); // Reset backoff on successful reconnect
                        }
                        Err(e) => {
                            error!(source_id = %self.id, error = %e, "reconnect failed after retries");
                            return Err(e);
                        }
                    }
                }
                Err(LoopControl::Stop) => break,
                Err(LoopControl::Fail(e)) => return Err(e),
            }
        }

        // Best-effort final checkpoint update
        let _ = chkpt_store
            .put(
                &self.id,
                PostgresCheckpoint {
                    lsn: ctx.last_lsn.to_string(),
                    tx_id: None,
                },
            )
            .await;

        // Stop the replication client
        {
            let mut client = ctx.repl_client.lock().await;
            client.shutdown().await.map_err(PostgresSourceError::from)?;
        }

        Ok(())
    }
}

#[async_trait]
impl Source for PostgresSource {
    fn checkpoint_key(&self) -> &str {
        &self.checkpoint_key
    }

    /// Start the source in a background task and return a control handle.
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
                error!(error = ?e, "run task ended with error");
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
