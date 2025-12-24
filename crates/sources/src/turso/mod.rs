//! TursoDB/SQLite CDC source implementation.
//!
//! Supports multiple CDC modes:
//! - **Native** (default): Uses Turso's built-in CDC via `turso_cdc` table (v0.1.2+)
//! - **Triggers**: Shadow tables populated by triggers (for standard SQLite)
//! - **Polling**: Track changes via rowid/timestamp columns (inserts only)
//! - **Auto**: Try native → triggers → polling fallback chain
//!
//! # Schema Loading Strategy
//!
//! - **Native mode**: JSON provided by Turso's `bin_record_json_object()` - schema loader optional
//! - **Triggers mode**: JSON embedded in shadow table via `json_object()` - schema loader optional
//! - **Polling mode**: Requires schema loader to build JSON from raw SELECT results
//!
//! The schema loader is always available for REST API schema endpoints regardless of CDC mode.

mod turso_schema_loader;
mod turso_table_schema;

// Schema types
pub use turso_table_schema::{SqliteAffinity, TursoColumn, TursoTableSchema};

// Schema loader (internal LoadedSchema + loader)
pub use turso_schema_loader::{LoadedSchema, TursoSchemaLoader};

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use checkpoints::{CheckpointStore, CheckpointStoreExt};
use chrono::Utc;
use deltaforge_config::{TursoCdcMode, TursoSrcCfg};
use deltaforge_core::{
    CheckpointMeta, Event, Op, Source, SourceError, SourceHandle, SourceMeta, SourceResult,
};
use libsql::Connection;
use metrics::counter;
use schema_registry::InMemoryRegistry;
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{mpsc, Notify},
    time::sleep,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

// ============================================================================
// Checkpoint
// ============================================================================

/// Turso/SQLite checkpoint structure.
///
/// Tracks position using:
/// - `last_change_id`: Position in `turso_cdc` table (native mode) or shadow table (triggers)
/// - `table_positions`: Per-table rowid tracking (polling mode fallback)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TursoCheckpoint {
    /// Per-table tracking: table_name -> last seen rowid (for polling mode)
    pub table_positions: HashMap<String, i64>,

    /// Last processed change ID from CDC table (native or trigger mode)
    pub last_change_id: Option<i64>,

    /// Timestamp of last checkpoint
    pub timestamp_ms: i64,

    /// Active CDC mode (for resumption)
    #[serde(default)]
    pub active_mode: Option<String>,
}

impl Default for TursoCheckpoint {
    fn default() -> Self {
        Self {
            table_positions: HashMap::new(),
            last_change_id: None,
            timestamp_ms: 0,
            active_mode: None,
        }
    }
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
            ..self.clone()
        }
    }

    /// Create a checkpoint with updated table position
    fn with_table_position(&self, table: &str, rowid: i64) -> Self {
        let mut positions = self.table_positions.clone();
        positions.insert(table.to_string(), rowid);
        Self {
            table_positions: positions,
            timestamp_ms: Utc::now().timestamp_millis(),
            ..self.clone()
        }
    }
}

// ============================================================================
// TursoSource
// ============================================================================

/// Turso CDC source.
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
    async fn connect(&self) -> SourceResult<Connection> {
        use libsql::Builder;

        let db = if self.cfg.url.starts_with("libsql://")
            || self.cfg.url.starts_with("http://")
            || self.cfg.url.starts_with("https://")
        {
            // Remote: Turso cloud, libsql server, or HTTP endpoint
            Builder::new_remote(
                self.cfg.url.clone(),
                self.cfg.auth_token.clone().unwrap_or_default(),
            )
            .build()
            .await
            .map_err(|e| SourceError::Connect {
                details: e.to_string().into(),
            })?
        } else if self.cfg.url.starts_with("file://") {
            // Local SQLite file with file:// prefix
            let path = self.cfg.url.trim_start_matches("file://");
            Builder::new_local(path)
                .build()
                .await
                .map_err(|e| SourceError::Connect {
                    details: e.to_string().into(),
                })?
        } else {
            // Assume it's a file path
            Builder::new_local(&self.cfg.url)
                .build()
                .await
                .map_err(|e| SourceError::Connect {
                    details: e.to_string().into(),
                })?
        };

        let conn = db.connect().map_err(|e| SourceError::Connect {
            details: e.to_string().into(),
        })?;

        Ok(conn)
    }

    /// Get the CDC table name (default: turso_cdc).
    fn cdc_table_name(&self) -> &str {
        self.cfg
            .cdc_table_name
            .as_deref()
            .unwrap_or("turso_cdc")
    }

    /// Enable native CDC on the connection.
    /// Must be called before any changes you want to capture.
    async fn enable_native_cdc(&self, conn: &Connection) -> SourceResult<()> {
        let level = self.cfg.native_cdc_level.pragma_value();
        let pragma_value = if let Some(ref table_name) = self.cfg.cdc_table_name {
            format!("{},{}", level, table_name)
        } else {
            level.to_string()
        };

        let sql = format!(
            "PRAGMA unstable_capture_data_changes_conn('{}');",
            pragma_value
        );

        debug!(pragma = %sql, "enabling native CDC");

        conn.execute(&sql, ())
            .await
            .map_err(|e| SourceError::Other(anyhow::anyhow!("failed to enable CDC: {}", e)))?;

        info!(
            level = %level,
            table = %self.cdc_table_name(),
            "native CDC enabled on connection"
        );

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

    /// Main run loop.
    async fn run_inner(
        &self,
        tx: mpsc::Sender<Event>,
        chkpt_store: Arc<dyn CheckpointStore>,
        cancel: CancellationToken,
        paused: Arc<AtomicBool>,
        pause_notify: Arc<Notify>,
    ) -> SourceResult<()> {
        info!(source_id = %self.id, url = %redact_auth(&self.cfg.url), "connecting to Turso");

        let conn = self.connect().await?;
        let conn = Arc::new(conn);

        // Expand table patterns to get tracked tables
        let tracked = self.expand_table_patterns(&conn).await?;
        info!(tables = tracked.len(), "tables discovered");

        // Determine effective CDC mode and set up infrastructure
        let effective_mode = self.setup_cdc_mode(&conn, &tracked).await?;
        info!(requested = ?self.cfg.cdc_mode, effective = ?effective_mode, "CDC mode determined");

        // Create schema loader - used for polling mode and available for API
        let schema_loader = TursoSchemaLoader::new(
            conn.clone(),
            self.registry.clone(),
            &self.tenant,
            None,
        );

        // Preload schemas (useful for all modes - enables API access)
        if let Err(e) = schema_loader.preload(&self.cfg.tables).await {
            warn!(error = %e, "schema preload failed, continuing anyway");
        }

        // Load checkpoint
        let mut checkpoint: TursoCheckpoint = chkpt_store
            .get(&self.checkpoint_key)
            .await
            .map_err(|e| SourceError::Other(e.into()))?
            .unwrap_or_default();

        checkpoint.active_mode = Some(format!("{:?}", effective_mode));

        info!(
            source_id = %self.id,
            tables = ?tracked,
            cdc_mode = ?effective_mode,
            "turso source starting"
        );

        let poll_interval = Duration::from_millis(self.cfg.poll_interval_ms);
        let source_meta = self.source_meta();

        // Main CDC loop
        loop {
            // Check for cancellation
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

            // Poll for changes based on effective mode
            // Clone checkpoint to avoid borrow/move conflicts across loop iterations
            let (updated_checkpoint, changes_found) = match effective_mode {
                TursoCdcMode::Native => {
                    self.poll_native_cdc_changes(&conn, &tx, checkpoint.clone(), &source_meta)
                        .await?
                }
                TursoCdcMode::Triggers => {
                    self.poll_trigger_changes(&conn, &tx, checkpoint.clone(), &source_meta)
                        .await?
                }
                TursoCdcMode::Polling => {
                    self.poll_rowid_changes(
                        &conn,
                        &tx,
                        checkpoint.clone(),
                        &source_meta,
                        &tracked,
                        &schema_loader,
                    )
                    .await?
                }
                TursoCdcMode::Auto => {
                    // Should have been resolved to a concrete mode
                    warn!("auto mode should have been resolved");
                    (checkpoint.clone(), false)
                }
            };

            // Update checkpoint with new state
            checkpoint = updated_checkpoint;
            checkpoint.timestamp_ms = Utc::now().timestamp_millis();
            
            // Serialize and save using put_raw to avoid lifetime issues with generic put()
            let checkpoint_bytes = serde_json::to_vec(&checkpoint).unwrap_or_default();
            if let Err(e) = chkpt_store.put_raw(&self.checkpoint_key, &checkpoint_bytes).await {
                warn!(error = %e, "failed to save checkpoint");
            }

            // If no changes, sleep before next poll
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
    async fn expand_table_patterns(&self, conn: &Connection) -> SourceResult<Vec<String>> {
        let temp_loader = TursoSchemaLoader::new(
            Arc::new(conn.clone()),
            self.registry.clone(),
            &self.tenant,
            None,
        );
        temp_loader.expand_patterns(&self.cfg.tables).await
    }

    /// Determine and set up the appropriate CDC mode.
    async fn setup_cdc_mode(
        &self,
        conn: &Connection,
        tables: &[String],
    ) -> SourceResult<TursoCdcMode> {
        match self.cfg.cdc_mode {
            TursoCdcMode::Native => {
                // Try to enable native CDC - this creates the turso_cdc table
                match self.enable_native_cdc(conn).await {
                    Ok(()) => {
                        // Verify the table was created
                        if self.check_native_cdc_available(conn).await? {
                            Ok(TursoCdcMode::Native)
                        } else {
                            warn!("native CDC enabled but table not found, falling back to triggers");
                            self.setup_cdc_triggers(conn, tables).await?;
                            Ok(TursoCdcMode::Triggers)
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "failed to enable native CDC, falling back to triggers");
                        self.setup_cdc_triggers(conn, tables).await?;
                        Ok(TursoCdcMode::Triggers)
                    }
                }
            }
            TursoCdcMode::Triggers => {
                self.setup_cdc_triggers(conn, tables).await?;
                Ok(TursoCdcMode::Triggers)
            }
            TursoCdcMode::Polling => Ok(TursoCdcMode::Polling),
            TursoCdcMode::Auto => {
                // Try native first
                match self.enable_native_cdc(conn).await {
                    Ok(()) if self.check_native_cdc_available(conn).await.unwrap_or(false) => {
                        info!("auto-detected native CDC support");
                        Ok(TursoCdcMode::Native)
                    }
                    _ => {
                        // Check if triggers already exist
                        if self.check_triggers_available(conn).await? {
                            info!("auto-detected existing CDC triggers");
                            Ok(TursoCdcMode::Triggers)
                        } else {
                            // Set up triggers as fallback
                            info!("auto-setting up CDC triggers");
                            self.setup_cdc_triggers(conn, tables).await?;
                            Ok(TursoCdcMode::Triggers)
                        }
                    }
                }
            }
        }
    }

    /// Check if native CDC is available (Turso with CDC enabled).
    async fn check_native_cdc_available(&self, conn: &Connection) -> SourceResult<bool> {
        let table = self.cdc_table_name();
        let sql = format!("SELECT 1 FROM {} LIMIT 0", table);
        let result = conn.query(&sql, ()).await;
        Ok(result.is_ok())
    }

    /// Check if CDC triggers are already set up.
    async fn check_triggers_available(&self, conn: &Connection) -> SourceResult<bool> {
        let result = conn.query("SELECT 1 FROM _df_cdc_changes LIMIT 0", ()).await;
        Ok(result.is_ok())
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
        let size_estimate = before.as_ref().map(|v| v.to_string().len()).unwrap_or(0)
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

        // Set checkpoint and schema info
        event.checkpoint = Some(CheckpointMeta::from_vec(checkpoint_bytes));
        event.schema_sequence = Some(self.registry.current_sequence());

        event
    }

    /// Poll for changes using native Turso CDC.
    /// Takes ownership of checkpoint and returns (updated_checkpoint, changes_found).
    ///
    /// Turso CDC table schema:
    /// - change_id: INTEGER PRIMARY KEY AUTOINCREMENT
    /// - change_time: INTEGER (Unix timestamp)
    /// - change_type: INTEGER (-1=delete, 0=update, 1=insert)
    /// - table_name: TEXT
    /// - id: rowid of affected row
    /// - before: BLOB (row data before change)
    /// - after: BLOB (row data after change)
    async fn poll_native_cdc_changes(
        &self,
        conn: &Connection,
        tx: &mpsc::Sender<Event>,
        mut checkpoint: TursoCheckpoint,
        source_meta: &SourceMeta,
    ) -> SourceResult<(TursoCheckpoint, bool)> {
        let last_id = checkpoint.last_change_id.unwrap_or(0);
        let cdc_table = self.cdc_table_name();
        let batch_size = self.cfg.batch_size;

        // Get list of tables we're tracking
        let tracked_tables = &self.cfg.tables;

        let mut max_id = last_id;
        let mut total_count = 0;

        // Query each tracked table separately so we can use table_columns_json_array()
        // with a literal table name (required by the function)
        for table_pattern in tracked_tables {
            // For wildcards, we need to query all tables and filter
            // For now, handle exact table names; wildcards can be expanded later
            let table_name = if table_pattern == "*" || table_pattern.contains('%') {
                // For wildcards, query without table filter and handle in Rust
                None
            } else {
                Some(table_pattern.as_str())
            };

            let (sql, params): (String, Vec<libsql::Value>) = if let Some(tbl) = table_name {
                // Query with JSON conversion for specific table
                (
                    format!(
                        "SELECT change_id, table_name, change_type, \
                         bin_record_json_object(table_columns_json_array('{}'), before) as before_json, \
                         bin_record_json_object(table_columns_json_array('{}'), after) as after_json \
                         FROM {} WHERE change_id > ? AND table_name = ? ORDER BY change_id LIMIT ?",
                        tbl, tbl, cdc_table
                    ),
                    vec![last_id.into(), tbl.into(), (batch_size as i64).into()],
                )
            } else {
                // Query raw blobs for wildcard - we'll decode per-row
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

                // Parse before/after - either JSON string or raw blob
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
                    // Raw blobs - need separate query to decode
                    // For now, we'll make an additional query per row (can optimize later)
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

                // Convert change_type integer to Op
                let op = match change_type {
                    1 => Op::Insert,
                    0 => Op::Update,
                    -1 => Op::Delete,
                    _ => {
                        warn!(change_type, "unknown CDC change_type");
                        continue;
                    }
                };

                // Create checkpoint for this event
                let event_checkpoint = checkpoint.with_change_id(change_id);
                let event =
                    self.create_event(source_meta, &table, op, before, after, &event_checkpoint);

                if tx.send(event).await.is_err() {
                    break;
                }

                counter!(
                    "deltaforge_source_events_total",
                    "pipeline" => self.pipeline.clone(),
                    "source" => self.id.clone(),
                    "table" => table,
                    "mode" => "native",
                )
                .increment(1);

                max_id = max_id.max(change_id);
                total_count += 1;
            }

            // If tracking all tables with wildcard, only need one query
            if table_name.is_none() {
                break;
            }
        }

        if total_count > 0 {
            checkpoint = checkpoint.with_change_id(max_id);
            debug!(count = total_count, max_id, "native CDC changes processed");
        }

        Ok((checkpoint, total_count > 0))
    }

    /// Decode a CDC blob using bin_record_json_object for a specific change.
    async fn decode_cdc_blob(
        &self,
        conn: &Connection,
        table_name: &str,
        column: &str, // "before" or "after"
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
            .map_err(|e| SourceError::Other(anyhow::anyhow!("blob decode failed: {}", e)))?;

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

    /// Poll for changes using trigger-based CDC.
    /// Takes ownership of checkpoint and returns (updated_checkpoint, changes_found).
    async fn poll_trigger_changes(
        &self,
        conn: &Connection,
        tx: &mpsc::Sender<Event>,
        mut checkpoint: TursoCheckpoint,
        source_meta: &SourceMeta,
    ) -> SourceResult<(TursoCheckpoint, bool)> {
        let last_id = checkpoint.last_change_id.unwrap_or(0);

        let mut rows = conn
            .query(
                "SELECT id, table_name, operation, row_data, old_data \
                 FROM _df_cdc_changes WHERE id > ? ORDER BY id LIMIT 1000",
                libsql::params![last_id],
            )
            .await
            .map_err(|e| SourceError::Connect {
                details: e.to_string().into(),
            })?;

        let mut max_id = last_id;
        let mut count = 0;

        use libsql::Value;
        while let Ok(Some(row)) = rows.next().await {
            let id = match row.get_value(0) {
                Ok(Value::Integer(i)) => i,
                _ => 0,
            };
            let table = match row.get_value(1) {
                Ok(Value::Text(s)) => s,
                _ => continue,
            };
            let operation = match row.get_value(2) {
                Ok(Value::Text(s)) => s,
                _ => continue,
            };
            let row_data = match row.get_value(3) {
                Ok(Value::Text(s)) => Some(s),
                _ => None,
            };
            let old_data = match row.get_value(4) {
                Ok(Value::Text(s)) => Some(s),
                _ => None,
            };

            let op = match operation.as_str() {
                "INSERT" => Op::Insert,
                "UPDATE" => Op::Update,
                "DELETE" => Op::Delete,
                _ => continue,
            };

            let after = row_data.as_ref().and_then(|s| serde_json::from_str(s).ok());
            let before = old_data.as_ref().and_then(|s| serde_json::from_str(s).ok());

            // Create checkpoint for this event
            let event_checkpoint = checkpoint.with_change_id(id);
            let event = self.create_event(source_meta, &table, op, before, after, &event_checkpoint);

            if tx.send(event).await.is_err() {
                break;
            }

            counter!(
                "deltaforge_source_events_total",
                "pipeline" => self.pipeline.clone(),
                "source" => self.id.clone(),
                "table" => table,
                "mode" => "triggers",
            )
            .increment(1);

            max_id = max_id.max(id);
            count += 1;
        }

        if count > 0 {
            checkpoint.last_change_id = Some(max_id);
        }

        Ok((checkpoint, count > 0))
    }

    /// Poll for changes using rowid tracking (polling mode).
    /// Takes ownership of checkpoint and returns (updated_checkpoint, changes_found).
    async fn poll_rowid_changes(
        &self,
        conn: &Connection,
        tx: &mpsc::Sender<Event>,
        mut checkpoint: TursoCheckpoint,
        source_meta: &SourceMeta,
        tables: &[String],
        schema_loader: &TursoSchemaLoader,
    ) -> SourceResult<(TursoCheckpoint, bool)> {
        let mut total_changes = 0;

        for table in tables {
            let (updated, count) = self
                .poll_table_rowid(conn, tx, checkpoint, source_meta, table, schema_loader)
                .await?;
            checkpoint = updated;
            total_changes += count;
        }

        Ok((checkpoint, total_changes > 0))
    }

    /// Poll a single table for rowid changes.
    /// Takes ownership of checkpoint and returns (updated_checkpoint, count).
    async fn poll_table_rowid(
        &self,
        conn: &Connection,
        tx: &mpsc::Sender<Event>,
        mut checkpoint: TursoCheckpoint,
        source_meta: &SourceMeta,
        table: &str,
        schema_loader: &TursoSchemaLoader,
    ) -> SourceResult<(TursoCheckpoint, usize)> {
        let last_rowid = checkpoint.table_positions.get(table).copied().unwrap_or(0);

        // Get column names for building JSON
        let column_names = schema_loader.column_names(table).await?;

        // Build SELECT with all columns
        let columns_sql = column_names
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect::<Vec<_>>()
            .join(", ");

        let query = format!(
            "SELECT _rowid_, {} FROM \"{}\" WHERE _rowid_ > ? ORDER BY _rowid_ LIMIT 1000",
            columns_sql,
            table.replace('"', "\"\"")
        );

        let mut rows = conn
            .query(&query, libsql::params![last_rowid])
            .await
            .map_err(|e| SourceError::Connect {
                details: e.to_string().into(),
            })?;

        let mut max_rowid = last_rowid;
        let mut count = 0;

        use libsql::Value;
        while let Ok(Some(row)) = rows.next().await {
            let rowid = match row.get_value(0) {
                Ok(Value::Integer(i)) => i,
                _ => 0,
            };

            // Build JSON object from columns
            let mut obj = serde_json::Map::new();
            for (i, col_name) in column_names.iter().enumerate() {
                let value = self.extract_value(&row, i + 1)?;
                obj.insert(col_name.clone(), value);
            }

            // Create checkpoint for this event
            let event_checkpoint = checkpoint.with_table_position(table, rowid);
            let event = self.create_event(
                source_meta,
                table,
                Op::Insert, // Polling only detects inserts
                None,
                Some(serde_json::Value::Object(obj)),
                &event_checkpoint,
            );

            if tx.send(event).await.is_err() {
                break;
            }

            counter!(
                "deltaforge_source_events_total",
                "pipeline" => self.pipeline.clone(),
                "source" => self.id.clone(),
                "table" => table.to_string(),
                "mode" => "polling",
            )
            .increment(1);

            max_rowid = max_rowid.max(rowid);
            count += 1;
        }

        if count > 0 {
            checkpoint.table_positions.insert(table.to_string(), max_rowid);
        }

        Ok((checkpoint, count))
    }

    /// Extract a SQLite value and convert to JSON.
    fn extract_value(&self, row: &libsql::Row, index: usize) -> SourceResult<serde_json::Value> {
        use libsql::Value;
        
        match row.get_value(index as i32) {
            Ok(Value::Integer(i)) => Ok(serde_json::Value::Number(i.into())),
            Ok(Value::Real(f)) => {
                if let Some(n) = serde_json::Number::from_f64(f) {
                    Ok(serde_json::Value::Number(n))
                } else {
                    Ok(serde_json::Value::Null)
                }
            }
            Ok(Value::Text(s)) => Ok(serde_json::Value::String(s)),
            Ok(Value::Blob(b)) => {
                // Return blob as base64
                use base64::Engine;
                Ok(serde_json::json!({
                    "_base64": base64::engine::general_purpose::STANDARD.encode(&b)
                }))
            }
            Ok(Value::Null) | Err(_) => Ok(serde_json::Value::Null),
        }
    }

    /// Setup CDC triggers for tracked tables.
    async fn setup_cdc_triggers(&self, conn: &Connection, tables: &[String]) -> SourceResult<()> {
        info!("setting up CDC triggers for {} tables", tables.len());

        // Create CDC changes table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS _df_cdc_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name TEXT NOT NULL,
                operation TEXT NOT NULL,
                row_data TEXT,
                old_data TEXT,
                created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now') * 1000)
            )",
            (),
        )
        .await
        .map_err(|e| SourceError::Other(e.into()))?;

        // Create index for efficient queries
        conn.execute(
            "CREATE INDEX IF NOT EXISTS _df_cdc_changes_idx ON _df_cdc_changes(id)",
            (),
        )
        .await
        .map_err(|e| SourceError::Other(e.into()))?;

        for table in tables {
            self.create_triggers_for_table(conn, table).await?;
        }

        Ok(())
    }

    /// Create INSERT/UPDATE/DELETE triggers for a table.
    async fn create_triggers_for_table(&self, conn: &Connection, table: &str) -> SourceResult<()> {
        // Get columns for this table
        let mut rows = conn
            .query(&format!("PRAGMA table_info('{}')", table), ())
            .await
            .map_err(|e| SourceError::Connect {
                details: e.to_string().into(),
            })?;

        use libsql::Value;
        let mut columns: Vec<String> = Vec::new();
        while let Ok(Some(row)) = rows.next().await {
            if let Ok(Value::Text(s)) = row.get_value(1) {
                columns.push(s);
            }
        }

        if columns.is_empty() {
            warn!(table = %table, "no columns found, skipping trigger creation");
            return Ok(());
        }

        // Build JSON object expressions
        let new_json = columns
            .iter()
            .map(|c| format!("'{}', NEW.\"{}\"", c, c))
            .collect::<Vec<_>>()
            .join(", ");

        let old_json = columns
            .iter()
            .map(|c| format!("'{}', OLD.\"{}\"", c, c))
            .collect::<Vec<_>>()
            .join(", ");

        // INSERT trigger
        let insert_trigger = format!(
            "CREATE TRIGGER IF NOT EXISTS _df_cdc_insert_{table}
             AFTER INSERT ON \"{table}\"
             BEGIN
                 INSERT INTO _df_cdc_changes (table_name, operation, row_data)
                 VALUES ('{table}', 'INSERT', json_object({new_json}));
             END"
        );

        // UPDATE trigger
        let update_trigger = format!(
            "CREATE TRIGGER IF NOT EXISTS _df_cdc_update_{table}
             AFTER UPDATE ON \"{table}\"
             BEGIN
                 INSERT INTO _df_cdc_changes (table_name, operation, row_data, old_data)
                 VALUES ('{table}', 'UPDATE', json_object({new_json}), json_object({old_json}));
             END"
        );

        // DELETE trigger
        let delete_trigger = format!(
            "CREATE TRIGGER IF NOT EXISTS _df_cdc_delete_{table}
             AFTER DELETE ON \"{table}\"
             BEGIN
                 INSERT INTO _df_cdc_changes (table_name, operation, old_data)
                 VALUES ('{table}', 'DELETE', json_object({old_json}));
             END"
        );

        // Execute trigger creation
        for (trigger_sql, op) in [
            (insert_trigger, "INSERT"),
            (update_trigger, "UPDATE"),
            (delete_trigger, "DELETE"),
        ] {
            if let Err(e) = conn.execute(&trigger_sql, ()).await {
                warn!(table = %table, op = %op, error = %e, "failed to create trigger");
            } else {
                debug!(table = %table, op = %op, "trigger created");
            }
        }

        Ok(())
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
    if url.starts_with("file://") || !url.contains("://") {
        return "local".to_string();
    }

    url.split("://")
        .nth(1)
        .and_then(|s| s.split('/').next())
        .and_then(|s| s.split('?').next())
        .and_then(|s| s.split('@').last())
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
        assert_eq!(extract_host("file://./test.db"), "local");
        assert_eq!(extract_host("./test.db"), "local");
        assert_eq!(
            extract_host("https://user:pass@db.example.com/path"),
            "db.example.com"
        );
    }

    #[test]
    fn test_checkpoint_default() {
        let cp = TursoCheckpoint::default();
        assert!(cp.table_positions.is_empty());
        assert!(cp.last_change_id.is_none());
        assert_eq!(cp.timestamp_ms, 0);
    }

    #[test]
    fn test_checkpoint_serde() {
        let mut cp = TursoCheckpoint::default();
        cp.last_change_id = Some(42);
        cp.table_positions.insert("users".to_string(), 100);

        let json = serde_json::to_string(&cp).unwrap();
        let parsed: TursoCheckpoint = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.last_change_id, Some(42));
        assert_eq!(parsed.table_positions.get("users"), Some(&100));
    }

    #[test]
    fn test_checkpoint_with_change_id() {
        let cp = TursoCheckpoint::default();
        let updated = cp.with_change_id(42);

        assert_eq!(updated.last_change_id, Some(42));
        assert!(updated.timestamp_ms > 0);
    }

    #[test]
    fn test_checkpoint_with_table_position() {
        let cp = TursoCheckpoint::default();
        let updated = cp.with_table_position("users", 100);

        assert_eq!(updated.table_positions.get("users"), Some(&100));
        assert!(updated.timestamp_ms > 0);
    }

    #[test]
    fn test_checkpoint_to_bytes() {
        let mut cp = TursoCheckpoint::default();
        cp.last_change_id = Some(42);

        let bytes = cp.to_bytes();
        assert!(!bytes.is_empty());

        // Should be valid JSON
        let parsed: TursoCheckpoint = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed.last_change_id, Some(42));
    }
}