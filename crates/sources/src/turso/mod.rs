//! TursoDB CDC source implementation.
//!
//! **STATUS: EXPERIMENTAL / PAUSED**
//!
//! This source is not yet ready for production use. Native CDC in Turso/libSQL
//! is still evolving and has limitations:
//! - CDC is per-connection (only changes from the enabling connection are captured)
//! - File locking prevents concurrent access (DeltaForge uses open/close pattern)
//! - sqld Docker image doesn't have CDC support yet
//!
//! The code is kept for future development when Turso CDC stabilizes.
//!
//! ---
//!
//! **Architecture (when enabled)**
//!
//! The application must enable CDC on its connection with:
//! ```sql
//! PRAGMA unstable_capture_data_changes_conn('full');
//! ```
//!
//! DeltaForge then reads from the `turso_cdc` table to capture changes.
//! This is per-connection - only changes made by connections that enabled
//! the pragma are captured.
//!
//! # Architecture
//!
//! 1. Application opens database and enables CDC on its connection
//! 2. Application makes changes (INSERT/UPDATE/DELETE)
//! 3. Changes are logged to `turso_cdc` table
//! 4. DeltaForge polls `turso_cdc` and streams events to sinks
//!
//! # Supported URLs
//!
//! - Local file: `/path/to/database.db` or `file:///path/to/database.db`
//! - Remote HTTP: `http://localhost:8080` or `https://...`
//! - Turso libsql: `libsql://your-db.turso.io`
//!
//! # Example
//!
//! ```bash
//! # Create database and enable CDC with tursodb CLI
//! tursodb /tmp/myapp.db
//! > PRAGMA unstable_capture_data_changes_conn('full');
//! > CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
//! > INSERT INTO users (name) VALUES ('Alice');
//!
//! # DeltaForge config
//! source:
//!   type: turso
//!   config:
//!     url: "/tmp/myapp.db"
//!     tables: ["users"]
//! ```

mod turso_schema_loader;
mod turso_table_schema;

pub use turso_schema_loader::{LoadedSchema, TursoSchemaLoader};
pub use turso_table_schema::{SqliteAffinity, TursoColumn, TursoTableSchema};

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use checkpoints::{CheckpointStore, CheckpointStoreExt};
use chrono::Utc;
use deltaforge_config::TursoSrcCfg;
use deltaforge_core::{
    CheckpointMeta, Event, Op, Source, SourceError, SourceHandle, SourceMeta,
    SourceResult,
};
use libsql::Connection;
use metrics::counter;
use schema_registry::InMemoryRegistry;
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{Notify, mpsc},
    time::sleep,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

// ============================================================================
// Checkpoint
// ============================================================================

/// Turso checkpoint structure.
///
/// Tracks position using `last_change_id` in the `turso_cdc` table.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TursoCheckpoint {
    /// Last processed change ID from CDC table
    pub last_change_id: Option<i64>,

    /// Timestamp of last checkpoint
    pub timestamp_ms: i64,
}

impl TursoCheckpoint {
    /// Serialize checkpoint to bytes for CheckpointMeta
    fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    /// Create a checkpoint with updated change_id
    fn with_change_id(&self, change_id: i64) -> Self {
        Self {
            last_change_id: Some(change_id),
            timestamp_ms: Utc::now().timestamp_millis(),
        }
    }
}

// ============================================================================
// TursoSource
// ============================================================================

/// Turso CDC source.
///
/// Captures changes from Turso/libSQL databases using native CDC support.
#[derive(Clone)]
pub struct TursoSource {
    pub id: String,
    pub checkpoint_key: String,
    pub cfg: TursoSrcCfg,
    pub tenant: String,
    pub pipeline: String,
    pub registry: Arc<InMemoryRegistry>,
}

impl TursoSource {
    /// Create a new Turso source from configuration.
    pub fn new(
        cfg: TursoSrcCfg,
        tenant: String,
        pipeline: String,
        registry: Arc<InMemoryRegistry>,
    ) -> Self {
        let id = cfg.id.clone();
        Self {
            checkpoint_key: format!("turso-{}", id),
            id,
            cfg,
            tenant,
            pipeline,
            registry,
        }
    }

    /// Establish connection to Turso/SQLite database.
    ///
    /// Supports:
    /// - Local files: `/path/to/db.sqlite` or `file:///path/to/db.sqlite`
    /// - Remote HTTP: `http://localhost:8080`
    /// - Turso cloud: `libsql://your-db.turso.io`
    async fn connect(&self) -> SourceResult<Connection> {
        use libsql::Builder;

        let url = &self.cfg.url;

        let db = if url.starts_with("libsql://")
            || url.starts_with("http://")
            || url.starts_with("https://")
        {
            // Remote: Turso cloud or libsql server
            debug!(url = %redact_auth(url), "connecting to remote database");
            Builder::new_remote(
                url.clone(),
                self.cfg.auth_token.clone().unwrap_or_default(),
            )
            .build()
            .await
            .map_err(|e| SourceError::Connect {
                details: format!(
                    "Failed to connect to {}: {}",
                    redact_auth(url),
                    e
                )
                .into(),
            })?
        } else {
            // Local file path
            let path = url.strip_prefix("file://").unwrap_or(url);
            debug!(path = %path, "opening local database file");

            // Check file exists
            if !std::path::Path::new(path).exists() {
                return Err(SourceError::Connect {
                    details: format!(
                        "Database file not found: {}. \
                         Create it with: tursodb {}",
                        path, path
                    )
                    .into(),
                });
            }

            Builder::new_local(path).build().await.map_err(|e| {
                SourceError::Connect {
                    details: format!("Failed to open {}: {}", path, e).into(),
                }
            })?
        };

        let conn = db.connect().map_err(|e| SourceError::Connect {
            details: e.to_string().into(),
        })?;

        Ok(conn)
    }

    /// Get the CDC table name (default: turso_cdc).
    fn cdc_table_name(&self) -> &str {
        self.cfg.cdc_table_name.as_deref().unwrap_or("turso_cdc")
    }

    /// Verify the CDC table exists.
    ///
    /// The application must enable CDC on its connection. DeltaForge just
    /// reads from the resulting `turso_cdc` table.
    async fn verify_cdc_table(&self, conn: &Connection) -> SourceResult<()> {
        let cdc_table = self.cdc_table_name();

        // Check if CDC table exists
        let check_sql = format!(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='{}'",
            cdc_table
        );

        let mut rows = conn.query(&check_sql, ()).await.map_err(|e| {
            SourceError::Connect {
                details: format!("Failed to check for CDC table: {}", e).into(),
            }
        })?;

        if rows
            .next()
            .await
            .map_err(|e| SourceError::Connect {
                details: format!(
                    "Failed to read CDC table check result: {}",
                    e
                )
                .into(),
            })?
            .is_none()
        {
            return Err(SourceError::Connect {
                details: format!(
                    "CDC table '{}' not found. The application must enable CDC with:\n\
                     \n\
                     PRAGMA unstable_capture_data_changes_conn('full');\n\
                     \n\
                     Run this in your app or via tursodb CLI before making changes.",
                    cdc_table
                ).into(),
            });
        }

        info!(table = %cdc_table, "CDC table found");
        Ok(())
    }

    /// Create source metadata for events.
    fn source_meta(&self) -> SourceMeta {
        SourceMeta {
            kind: "turso".into(),
            host: extract_host(&self.cfg.url),
            db: "main".into(),
        }
    }

    /// Check if this is a local file (requires open/close pattern to avoid locking)
    fn is_local_file(&self) -> bool {
        !self.cfg.url.starts_with("libsql://")
            && !self.cfg.url.starts_with("http://")
            && !self.cfg.url.starts_with("https://")
    }

    /// Main run loop.
    async fn run_inner(
        &self,
        tx: mpsc::Sender<Event>,
        chkpt_store: Arc<dyn CheckpointStore>,
        cancel: CancellationToken,
        paused: Arc<AtomicBool>,
        pause_notify: Arc<Notify>,
    ) -> SourceResult<()> {
        info!(
            source_id = %self.id,
            url = %redact_auth(&self.cfg.url),
            local_file = %self.is_local_file(),
            "connecting to Turso"
        );

        // Initial connection to verify CDC table and discover tables
        let init_conn = Arc::new(self.connect().await?);

        // Verify CDC table exists (app must have enabled CDC)
        self.verify_cdc_table(&init_conn).await?;

        // Expand table patterns to get tracked tables
        let tracked = self.expand_table_patterns(&init_conn).await?;
        info!(tables = tracked.len(), "tables discovered");

        // For remote connections, keep connection alive and create schema loader
        // For local files, we'll reconnect each poll to avoid locking
        let persistent_conn = if self.is_local_file() {
            info!(
                "using open/close pattern for local file (avoids lock contention)"
            );
            drop(init_conn); // Close initial connection
            None
        } else {
            // Create schema loader for API access (remote only)
            let schema_loader = TursoSchemaLoader::new(
                init_conn.clone(),
                self.registry.clone(),
                &self.tenant,
                None,
            );

            // Preload schemas for API
            if let Err(e) = schema_loader.preload(&self.cfg.tables).await {
                warn!(error = %e, "schema preload failed, continuing anyway");
            }

            Some(init_conn)
        };

        // Load checkpoint
        let mut checkpoint: TursoCheckpoint = chkpt_store
            .get(&self.checkpoint_key)
            .await
            .map_err(|e| SourceError::Other(e.into()))?
            .unwrap_or_default();

        info!(
            source_id = %self.id,
            tables = ?tracked,
            last_change_id = ?checkpoint.last_change_id,
            "turso source starting"
        );

        let poll_interval = Duration::from_millis(self.cfg.poll_interval_ms);
        let source_meta = self.source_meta();

        // Main CDC loop
        loop {
            if cancel.is_cancelled() {
                break;
            }

            // Handle pause
            while paused.load(Ordering::SeqCst) {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = pause_notify.notified() => {}
                }
            }

            if cancel.is_cancelled() {
                break;
            }

            // Get connection (reuse for remote, reconnect for local)
            let poll_conn: Arc<Connection> = if let Some(ref conn) =
                persistent_conn
            {
                conn.clone()
            } else {
                // Reconnect for local files
                match self.connect().await {
                    Ok(c) => Arc::new(c),
                    Err(e) => {
                        warn!(error = %e, "reconnect failed, retrying after interval");
                        tokio::select! {
                            _ = cancel.cancelled() => break,
                            _ = sleep(poll_interval) => {}
                        }
                        continue;
                    }
                }
            };

            // Poll for changes
            let poll_result = self
                .poll_cdc_changes(
                    &poll_conn,
                    &tx,
                    checkpoint.clone(),
                    &source_meta,
                )
                .await;

            // For local files, drop connection immediately after poll
            if persistent_conn.is_none() {
                drop(poll_conn);
            }

            let (updated_checkpoint, changes_found) = poll_result?;
            checkpoint = updated_checkpoint;

            // Sleep if no changes
            if !changes_found {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = sleep(poll_interval) => {}
                }
            }
        }

        info!(source_id = %self.id, "turso source stopped");
        Ok(())
    }

    /// Expand table patterns to concrete table names.
    async fn expand_table_patterns(
        &self,
        conn: &Arc<Connection>,
    ) -> SourceResult<Vec<String>> {
        let temp_loader = TursoSchemaLoader::new(
            conn.clone(),
            self.registry.clone(),
            &self.tenant,
            None,
        );
        temp_loader.expand_patterns(&self.cfg.tables).await
    }

    /// Create an event with proper structure.
    fn create_event(
        &self,
        source_meta: &SourceMeta,
        table: &str,
        op: Op,
        before: Option<serde_json::Value>,
        after: Option<serde_json::Value>,
        checkpoint: &TursoCheckpoint,
    ) -> Event {
        let checkpoint_bytes = checkpoint.to_bytes();
        let size_estimate =
            before.as_ref().map(|v| v.to_string().len()).unwrap_or(0)
                + after.as_ref().map(|v| v.to_string().len()).unwrap_or(0);

        let mut event = Event::new_row(
            self.tenant.clone(),
            source_meta.clone(),
            table.to_string(),
            op,
            before,
            after,
            Utc::now().timestamp_millis(),
            size_estimate,
        );

        event.checkpoint = Some(CheckpointMeta::from_vec(checkpoint_bytes));
        event.schema_sequence = Some(self.registry.current_sequence());

        event
    }

    /// Poll for changes using native Turso CDC.
    ///
    /// Turso CDC table schema:
    /// - change_id: INTEGER PRIMARY KEY AUTOINCREMENT
    /// - change_time: INTEGER (Unix timestamp)
    /// - change_type: INTEGER (-1=delete, 0=update, 1=insert)
    /// - table_name: TEXT
    /// - id: rowid of affected row
    /// - before: BLOB (row data before change)
    /// - after: BLOB (row data after change)
    async fn poll_cdc_changes(
        &self,
        conn: &Connection,
        tx: &mpsc::Sender<Event>,
        mut checkpoint: TursoCheckpoint,
        source_meta: &SourceMeta,
    ) -> SourceResult<(TursoCheckpoint, bool)> {
        let last_id = checkpoint.last_change_id.unwrap_or(0);
        let cdc_table = self.cdc_table_name();
        let batch_size = self.cfg.batch_size;
        let tracked_tables = &self.cfg.tables;

        let mut max_id = last_id;
        let mut total_count = 0;

        // Query each tracked table separately for JSON conversion
        for table_pattern in tracked_tables {
            let table_name =
                if table_pattern == "*" || table_pattern.contains('%') {
                    None // Wildcard - query all
                } else {
                    Some(table_pattern.as_str())
                };

            let (sql, params): (String, Vec<libsql::Value>) = if let Some(tbl) =
                table_name
            {
                // Query with JSON conversion for specific table
                (
                    format!(
                        "SELECT change_id, table_name, change_type, \
                         bin_record_json_object(table_columns_json_array('{}'), before) as before_json, \
                         bin_record_json_object(table_columns_json_array('{}'), after) as after_json \
                         FROM {} WHERE change_id > ? AND table_name = ? ORDER BY change_id LIMIT ?",
                        tbl, tbl, cdc_table
                    ),
                    vec![
                        last_id.into(),
                        tbl.into(),
                        (batch_size as i64).into(),
                    ],
                )
            } else {
                // Query all tables - decode per-row
                (
                    format!(
                        "SELECT change_id, table_name, change_type, before, after \
                         FROM {} WHERE change_id > ? ORDER BY change_id LIMIT ?",
                        cdc_table
                    ),
                    vec![last_id.into(), (batch_size as i64).into()],
                )
            };

            let mut rows = conn
                .query(&sql, libsql::params_from_iter(params))
                .await
                .map_err(|e| SourceError::Connect {
                    details: format!("CDC query failed: {}", e).into(),
                })?;

            use libsql::Value;
            while let Ok(Some(row)) = rows.next().await {
                let change_id = match row.get_value(0) {
                    Ok(Value::Integer(i)) => i,
                    _ => 0,
                };
                let table = match row.get_value(1) {
                    Ok(Value::Text(s)) => s,
                    _ => continue,
                };
                let change_type = match row.get_value(2) {
                    Ok(Value::Integer(i)) => i,
                    _ => 0,
                };

                // Parse before/after
                let (before, after) = if table_name.is_some() {
                    // JSON strings from bin_record_json_object
                    let before_json = match row.get_value(3) {
                        Ok(Value::Text(s)) => Some(s),
                        _ => None,
                    };
                    let after_json = match row.get_value(4) {
                        Ok(Value::Text(s)) => Some(s),
                        _ => None,
                    };
                    (
                        before_json.and_then(|s| serde_json::from_str(&s).ok()),
                        after_json.and_then(|s| serde_json::from_str(&s).ok()),
                    )
                } else {
                    // Raw blobs - decode with separate query
                    let before_json = self
                        .decode_cdc_blob(conn, &table, "before", change_id)
                        .await
                        .ok()
                        .flatten();
                    let after_json = self
                        .decode_cdc_blob(conn, &table, "after", change_id)
                        .await
                        .ok()
                        .flatten();
                    (before_json, after_json)
                };

                // Convert change_type to Op
                let op = match change_type {
                    1 => Op::Insert,
                    0 => Op::Update,
                    -1 => Op::Delete,
                    _ => {
                        warn!(change_type, "unknown CDC change_type");
                        continue;
                    }
                };

                let event_checkpoint = checkpoint.with_change_id(change_id);
                let event = self.create_event(
                    source_meta,
                    &table,
                    op,
                    before,
                    after,
                    &event_checkpoint,
                );

                if tx.send(event).await.is_err() {
                    break;
                }

                counter!(
                    "deltaforge_source_events_total",
                    "pipeline" => self.pipeline.clone(),
                    "source" => self.id.clone(),
                    "table" => table,
                )
                .increment(1);

                max_id = max_id.max(change_id);
                total_count += 1;
            }

            // For wildcard, only need one query
            if table_name.is_none() {
                break;
            }
        }

        if total_count > 0 {
            checkpoint = checkpoint.with_change_id(max_id);
            debug!(count = total_count, max_id, "CDC changes processed");
        }

        Ok((checkpoint, total_count > 0))
    }

    /// Decode a CDC blob using bin_record_json_object.
    async fn decode_cdc_blob(
        &self,
        conn: &Connection,
        table_name: &str,
        column: &str,
        change_id: i64,
    ) -> SourceResult<Option<serde_json::Value>> {
        let cdc_table = self.cdc_table_name();
        let sql = format!(
            "SELECT bin_record_json_object(table_columns_json_array('{}'), {}) \
             FROM {} WHERE change_id = ?",
            table_name, column, cdc_table
        );

        let mut rows = conn
            .query(&sql, libsql::params![change_id])
            .await
            .map_err(|e| {
                SourceError::Other(anyhow::anyhow!("blob decode failed: {}", e))
            })?;

        use libsql::Value;
        if let Ok(Some(row)) = rows.next().await {
            let json_str = match row.get_value(0) {
                Ok(Value::Text(s)) => Some(s),
                _ => None,
            };
            Ok(json_str.and_then(|s| serde_json::from_str(&s).ok()))
        } else {
            Ok(None)
        }
    }
}

// ============================================================================
// Source Trait Implementation
// ============================================================================

#[async_trait]
impl Source for TursoSource {
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
                error!(error = ?e, "turso source ended with error");
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
// Helpers
// ============================================================================

/// Redact auth token from URL for logging.
fn redact_auth(url: &str) -> String {
    if let Some(idx) = url.find("authToken=") {
        let end = url[idx..].find('&').unwrap_or(url.len() - idx);
        format!("{}authToken=***{}", &url[..idx], &url[idx + end..])
    } else {
        url.to_string()
    }
}

/// Extract host from URL for source metadata.
fn extract_host(url: &str) -> String {
    url.split("://")
        .nth(1)
        .and_then(|s| s.split('/').next())
        .and_then(|s| s.split('?').next())
        .and_then(|s| s.split('@').next_back())
        .unwrap_or("unknown")
        .to_string()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redact_auth() {
        assert_eq!(
            redact_auth("libsql://db.turso.io?authToken=secret123"),
            "libsql://db.turso.io?authToken=***"
        );
        assert_eq!(redact_auth("libsql://db.turso.io"), "libsql://db.turso.io");
    }

    #[test]
    fn test_extract_host() {
        assert_eq!(extract_host("libsql://mydb.turso.io"), "mydb.turso.io");
        assert_eq!(
            extract_host("https://user:pass@db.example.com/path"),
            "db.example.com"
        );
    }

    #[test]
    fn test_checkpoint_default() {
        let cp = TursoCheckpoint::default();
        assert!(cp.last_change_id.is_none());
        assert_eq!(cp.timestamp_ms, 0);
    }

    #[test]
    fn test_checkpoint_serde() {
        let cp = TursoCheckpoint {
            last_change_id: Some(42),
            ..Default::default()
        };

        let json = serde_json::to_string(&cp).unwrap();
        let parsed: TursoCheckpoint = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.last_change_id, Some(42));
    }

    #[test]
    fn test_checkpoint_to_bytes() {
        let cp = TursoCheckpoint {
            last_change_id: Some(42),
            ..Default::default()
        };

        let bytes = cp.to_bytes();
        assert!(!bytes.is_empty());

        let parsed: TursoCheckpoint = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed.last_change_id, Some(42));
    }
}
