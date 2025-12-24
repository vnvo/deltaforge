//! Turso/SQLite schema loader with pattern expansion and registry integration.
//!
//! Provides schema discovery via SQLite's PRAGMA commands:
//! - `PRAGMA table_info(table)` for column metadata
//! - `PRAGMA table_list` for table enumeration
//! - `sqlite_master` for table definitions and trigger inspection
//!
//! # Usage Modes
//!
//! - **Native mode**: JSON provided by Turso's `bin_record_json_object()` - schema loader optional
//! - **Triggers mode**: JSON embedded in shadow table via `json_object()` - schema loader optional
//! - **Polling mode**: Requires schema loader to build JSON from raw SELECT results
//!
//! The schema loader is always useful for:
//! - REST API schema endpoints
//! - Schema registry integration
//! - Schema version tracking

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use chrono::Utc;
use deltaforge_core::{SourceError, SourceResult};
use libsql::Connection;
use schema_registry::{InMemoryRegistry, SourceSchema};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use super::turso_table_schema::{SqliteAffinity, TursoColumn, TursoTableSchema};
use crate::schema_loader::{
    LoadedSchema as ApiLoadedSchema, SchemaListEntry, SourceSchemaLoader,
};

// ============================================================================
// Types
// ============================================================================

/// Loaded schema with metadata (internal representation).
///
/// Used by CDC code for schema-aware event processing.
#[derive(Debug, Clone)]
pub struct LoadedSchema {
    /// The parsed table schema
    pub schema: TursoTableSchema,
    /// Version number in schema registry
    pub registry_version: i32,
    /// Content fingerprint for change detection
    pub fingerprint: Arc<str>,
    /// Global sequence number for replay ordering
    pub sequence: u64,
    /// Column names for efficient access
    pub column_names: Arc<Vec<String>>,
}

// ============================================================================
// TursoSchemaLoader
// ============================================================================

/// Schema loader for Turso/SQLite databases.
///
/// Provides:
/// - Schema discovery from INFORMATION_SCHEMA equivalent (PRAGMA commands)
/// - Wildcard pattern expansion for table selection
/// - Caching with invalidation support
/// - Schema registry integration for versioning
#[derive(Clone)]
pub struct TursoSchemaLoader {
    conn: Arc<Connection>,
    cache: Arc<RwLock<HashMap<String, LoadedSchema>>>,
    registry: Arc<InMemoryRegistry>,
    tenant: String,
    db_name: String,
}

impl TursoSchemaLoader {
    /// Create a new schema loader with an existing connection.
    ///
    /// # Arguments
    /// * `conn` - Shared database connection
    /// * `registry` - Schema registry for version tracking
    /// * `tenant` - Tenant identifier for multi-tenancy
    /// * `db_name` - Database name for registry (defaults to "main")
    pub fn new(
        conn: Arc<Connection>,
        registry: Arc<InMemoryRegistry>,
        tenant: &str,
        db_name: Option<&str>,
    ) -> Self {
        Self {
            conn,
            cache: Arc::new(RwLock::new(HashMap::new())),
            registry,
            tenant: tenant.to_string(),
            db_name: db_name.unwrap_or("main").to_string(),
        }
    }

    /// Get the database name.
    pub fn db_name(&self) -> &str {
        &self.db_name
    }

    /// Get the current global sequence number.
    pub fn current_sequence(&self) -> u64 {
        self.registry.current_sequence()
    }

    // ========================================================================
    // Pattern Expansion
    // ========================================================================

    /// Expand wildcard patterns and preload all matching schemas.
    ///
    /// # Patterns
    /// - `table` - exact match
    /// - `prefix%` - tables starting with prefix
    /// - `*` or empty - all user tables
    ///
    /// # Returns
    /// List of table names that were successfully loaded.
    pub async fn preload(&self, patterns: &[String]) -> SourceResult<Vec<String>> {
        let t0 = Instant::now();
        let tables = self.expand_patterns(patterns).await?;

        info!(
            patterns = ?patterns,
            matched_tables = tables.len(),
            "expanded table patterns"
        );

        let mut loaded = Vec::with_capacity(tables.len());
        for table in &tables {
            match self.load_schema(table).await {
                Ok(_) => loaded.push(table.clone()),
                Err(e) => warn!(table = %table, error = %e, "failed to preload schema"),
            }
        }

        let elapsed = t0.elapsed();
        info!(
            tables_loaded = loaded.len(),
            elapsed_ms = elapsed.as_millis(),
            "schema preload complete"
        );

        Ok(loaded)
    }

    /// Expand wildcard patterns to actual table list.
    pub async fn expand_patterns(&self, patterns: &[String]) -> SourceResult<Vec<String>> {
        let all_tables = self.list_all_tables().await?;
        let mut results = Vec::new();

        // Empty patterns = all tables
        if patterns.is_empty() {
            return Ok(all_tables);
        }

        for pattern in patterns {
            if pattern == "*" {
                // All tables
                for table in &all_tables {
                    if !results.contains(table) {
                        results.push(table.clone());
                    }
                }
            } else if pattern.ends_with('%') {
                // Prefix match
                let prefix = pattern.trim_end_matches('%');
                for table in &all_tables {
                    if table.starts_with(prefix) && !results.contains(table) {
                        results.push(table.clone());
                    }
                }
            } else if pattern.contains('.') {
                // db.table format - extract table name
                let table = pattern.split('.').last().unwrap_or(pattern);
                if all_tables.contains(&table.to_string()) && !results.contains(&table.to_string())
                {
                    results.push(table.to_string());
                }
            } else {
                // Exact match
                if all_tables.contains(pattern) && !results.contains(pattern) {
                    results.push(pattern.clone());
                }
            }
        }

        Ok(results)
    }

    /// List all user tables in the database.
    async fn list_all_tables(&self) -> SourceResult<Vec<String>> {
        use libsql::Value;
        
        let mut tables = Vec::new();

        // Try PRAGMA table_list first (SQLite 3.37+)
        // Columns: schema, name, type, ncol, wr, strict
        if let Ok(mut rows) = self.conn.query("PRAGMA table_list", ()).await {
            debug!("querying PRAGMA table_list");

            while let Ok(Some(row)) = rows.next().await {
                // Column 1 = name, Column 2 = type
                let name = match row.get_value(1) {
                    Ok(Value::Text(s)) => s,
                    other => {
                        debug!(?other, "unexpected value type for name column");
                        continue;
                    }
                };
                let table_type = match row.get_value(2) {
                    Ok(Value::Text(s)) => s,
                    other => {
                        debug!(?other, "unexpected value type for type column");
                        continue;
                    }
                };

                debug!(name = %name, table_type = %table_type, "parsed table_list row");

                // Skip system tables, views, and our CDC infrastructure
                if table_type == "table"
                    && !name.starts_with("sqlite_")
                    && !name.starts_with("_df_")
                    && !name.starts_with("_litestream")
                    && !name.starts_with("_turso")
                    && name != "turso_cdc"
                {
                    tables.push(name);
                }
            }

            if !tables.is_empty() {
                debug!(tables = ?tables, "found tables via PRAGMA table_list");
                return Ok(tables);
            }
        }

        // Fallback to sqlite_master
        debug!("falling back to sqlite_master query");
        let mut rows = self
            .conn
            .query(
                "SELECT name FROM sqlite_master WHERE type='table' \
                 AND name NOT LIKE 'sqlite_%' \
                 AND name NOT LIKE '_df_%' \
                 AND name NOT LIKE '_litestream%' \
                 AND name NOT LIKE '_turso%' \
                 AND name != 'turso_cdc' \
                 ORDER BY name",
                (),
            )
            .await
            .map_err(|e| SourceError::Connect {
                details: e.to_string().into(),
            })?;

        while let Ok(Some(row)) = rows.next().await {
            if let Ok(Value::Text(name)) = row.get_value(0) {
                debug!(name = %name, "found table in sqlite_master");
                tables.push(name);
            }
        }

        debug!(tables = ?tables, "found tables via sqlite_master");
        Ok(tables)
    }

    // ========================================================================
    // Schema Loading
    // ========================================================================

    /// Load full schema for a table (uses cache if available).
    pub async fn load_schema(&self, table: &str) -> SourceResult<LoadedSchema> {
        self.load_schema_at_checkpoint(table, None).await
    }

    /// Load schema with optional checkpoint correlation.
    ///
    /// The checkpoint bytes are stored in the registry for replay correlation.
    pub async fn load_schema_at_checkpoint(
        &self,
        table: &str,
        checkpoint: Option<&[u8]>,
    ) -> SourceResult<LoadedSchema> {
        // Check cache first
        if let Some(cached) = self.cache.read().await.get(table) {
            debug!(table = %table, "schema cache hit");
            return Ok(cached.clone());
        }

        let t0 = Instant::now();
        let schema = self.fetch_schema(table).await?;
        let fingerprint = schema.fingerprint();
        let column_names: Arc<Vec<String>> =
            Arc::new(schema.columns.iter().map(|c| c.name.clone()).collect());

        // Register with schema registry
        let schema_json =
            serde_json::to_value(&schema).map_err(|e| SourceError::Other(e.into()))?;

        let version = self
            .registry
            .register_with_checkpoint(
                &self.tenant,
                &self.db_name,
                table,
                &fingerprint,
                &schema_json,
                checkpoint,
            )
            .await
            .map_err(SourceError::Other)?;

        let loaded = LoadedSchema {
            schema,
            registry_version: version,
            fingerprint: fingerprint.into(),
            sequence: self.registry.current_sequence(),
            column_names,
        };

        // Cache it
        self.cache
            .write()
            .await
            .insert(table.to_string(), loaded.clone());

        let elapsed = t0.elapsed();
        if elapsed.as_millis() > 200 {
            warn!(table = %table, ms = elapsed.as_millis(), "slow schema load");
        } else {
            debug!(table = %table, version = version, ms = elapsed.as_millis(), "schema loaded");
        }

        Ok(loaded)
    }

    /// Fetch schema from database using PRAGMA commands.
    async fn fetch_schema(&self, table: &str) -> SourceResult<TursoTableSchema> {
        let mut columns = Vec::new();

        // Get column info via PRAGMA table_info
        // Returns columns: cid, name, type, notnull, dflt_value, pk
        let sql = format!("PRAGMA table_info('{}')", escape_table_name(table));
        debug!(sql = %sql, "fetching schema");
        
        let mut rows = self
            .conn
            .query(&sql, ())
            .await
            .map_err(|e| SourceError::Connect {
                details: e.to_string().into(),
            })?;

        use libsql::Value;
        
        while let Ok(Some(row)) = rows.next().await {
            // Extract columns using Value enum: cid(0), name(1), type(2), notnull(3), dflt_value(4), pk(5)
            let cid = match row.get_value(0) {
                Ok(Value::Integer(i)) => i,
                _ => 0,
            };
            let name = match row.get_value(1) {
                Ok(Value::Text(s)) => s,
                other => {
                    warn!(?other, "failed to get column name");
                    continue;
                }
            };
            let col_type = match row.get_value(2) {
                Ok(Value::Text(s)) => s,
                _ => String::new(),
            };
            let notnull = match row.get_value(3) {
                Ok(Value::Integer(i)) => i,
                _ => 0,
            };
            // dflt_value can be NULL
            let dflt_value = match row.get_value(4) {
                Ok(Value::Text(s)) => Some(s),
                Ok(Value::Null) => None,
                _ => None,
            };
            let pk = match row.get_value(5) {
                Ok(Value::Integer(i)) => i,
                _ => 0,
            };

            debug!(cid, name = %name, col_type = %col_type, notnull, pk, "parsed column");

            let mut col = TursoColumn::new(
                &name,
                &col_type,
                notnull == 0,
                cid as usize,
            );
            col.default_value = dflt_value;
            col.is_primary_key = pk > 0;
            columns.push(col);
        }

        if columns.is_empty() {
            return Err(SourceError::Schema {
                details: format!("table '{}' not found or has no columns", table).into(),
            });
        }

        // Check if WITHOUT ROWID
        let without_rowid = self.check_without_rowid(table).await?;

        // Check for CDC triggers
        let has_cdc_triggers = self.check_cdc_triggers(table).await?;

        // Get CREATE statement
        let create_sql = self.get_create_statement(table).await?;

        // Check for autoincrement in primary key columns
        if let Some(ref sql) = create_sql {
            let upper = sql.to_uppercase();
            if upper.contains("AUTOINCREMENT") {
                for col in &mut columns {
                    if col.is_primary_key && col.affinity == SqliteAffinity::Integer {
                        col.is_autoincrement = true;
                    }
                }
            }
        }

        let primary_key: Vec<String> = columns
            .iter()
            .filter(|c| c.is_primary_key)
            .map(|c| c.name.clone())
            .collect();

        Ok(TursoTableSchema {
            columns,
            primary_key,
            without_rowid,
            has_cdc_triggers,
            create_sql,
        })
    }

    /// Check if table is WITHOUT ROWID.
    async fn check_without_rowid(&self, table: &str) -> SourceResult<bool> {
        // Try PRAGMA table_list first (SQLite 3.37+)
        // Columns: schema, name, type, ncol, wr, strict
        if let Ok(mut rows) = self
            .conn
            .query(
                &format!("PRAGMA table_list('{}')", escape_table_name(table)),
                (),
            )
            .await
        {
            use libsql::Value;
            while let Ok(Some(row)) = rows.next().await {
                // wr (WITHOUT ROWID flag) is column 4
                if let Ok(Value::Integer(wr)) = row.get_value(4) {
                    return Ok(wr != 0);
                }
            }
        }

        // Fallback: check if _rowid_ is accessible
        let result = self
            .conn
            .query(
                &format!(
                    "SELECT _rowid_ FROM \"{}\" LIMIT 0",
                    escape_table_name(table)
                ),
                (),
            )
            .await;

        Ok(result.is_err())
    }

    /// Check if CDC triggers exist for this table.
    async fn check_cdc_triggers(&self, table: &str) -> SourceResult<bool> {
        let mut rows = self
            .conn
            .query(
                "SELECT COUNT(*) FROM sqlite_master \
                 WHERE type='trigger' AND tbl_name=? AND name LIKE '_df_cdc_%'",
                libsql::params![table],
            )
            .await
            .map_err(|e| SourceError::Connect {
                details: e.to_string().into(),
            })?;

        use libsql::Value;
        if let Ok(Some(row)) = rows.next().await {
            if let Ok(Value::Integer(count)) = row.get_value(0) {
                return Ok(count > 0);
            }
        }

        Ok(false)
    }

    /// Get the CREATE TABLE statement.
    async fn get_create_statement(&self, table: &str) -> SourceResult<Option<String>> {
        let mut rows = self
            .conn
            .query(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
                libsql::params![table],
            )
            .await
            .map_err(|e| SourceError::Connect {
                details: e.to_string().into(),
            })?;

        use libsql::Value;
        if let Ok(Some(row)) = rows.next().await {
            if let Ok(Value::Text(sql)) = row.get_value(0) {
                return Ok(Some(sql));
            }
        }

        Ok(None)
    }

    // ========================================================================
    // Cache Management
    // ========================================================================

    /// Force reload schema from database (bypasses cache).
    pub async fn reload_schema(&self, table: &str) -> SourceResult<LoadedSchema> {
        self.cache.write().await.remove(table);
        self.load_schema(table).await
    }

    /// Reload all schemas matching patterns.
    pub async fn reload_all_patterns(&self, patterns: &[String]) -> SourceResult<Vec<String>> {
        self.cache.write().await.clear();
        self.preload(patterns).await
    }

    /// Invalidate all cached schemas.
    pub async fn invalidate_all(&self) {
        self.cache.write().await.clear();
    }

    /// Invalidate schema for a specific table.
    pub async fn invalidate(&self, table: &str) {
        self.cache.write().await.remove(table);
    }

    /// Get cached schema without loading from database.
    pub async fn get_cached(&self, table: &str) -> Option<LoadedSchema> {
        self.cache.read().await.get(table).cloned()
    }

    /// List all cached schemas.
    pub async fn list_cached_internal(&self) -> Vec<(String, LoadedSchema)> {
        self.cache
            .read()
            .await
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Get column names for a table.
    pub async fn column_names(&self, table: &str) -> SourceResult<Arc<Vec<String>>> {
        let loaded = self.load_schema(table).await?;
        Ok(Arc::clone(&loaded.column_names))
    }
}

// ============================================================================
// SourceSchemaLoader Implementation (for REST API)
// ============================================================================

#[async_trait]
impl SourceSchemaLoader for TursoSchemaLoader {
    fn source_type(&self) -> &'static str {
        "turso"
    }

    async fn load(&self, db: &str, table: &str) -> anyhow::Result<ApiLoadedSchema> {
        // For SQLite, db is typically "main" - we use our configured db_name
        let _ = db;
        let loaded = self.load_schema(table).await?;
        Ok(to_api_schema(&self.db_name, table, &loaded))
    }

    async fn reload(&self, db: &str, table: &str) -> anyhow::Result<ApiLoadedSchema> {
        let _ = db;
        let loaded = self.reload_schema(table).await?;
        Ok(to_api_schema(&self.db_name, table, &loaded))
    }

    async fn reload_all(&self, patterns: &[String]) -> anyhow::Result<Vec<(String, String)>> {
        self.cache.write().await.clear();
        let tables = self.preload(patterns).await?;
        // Convert Vec<String> to Vec<(String, String)> with db_name
        Ok(tables
            .into_iter()
            .map(|t| (self.db_name.clone(), t))
            .collect())
    }

    async fn list_cached(&self) -> Vec<SchemaListEntry> {
        self.cache
            .read()
            .await
            .iter()
            .map(|(table, loaded)| SchemaListEntry {
                database: self.db_name.clone(),
                table: table.clone(),
                column_count: loaded.schema.columns.len(),
                primary_key: loaded.schema.primary_key.clone(),
                fingerprint: loaded.fingerprint.to_string(),
                registry_version: loaded.registry_version,
            })
            .collect()
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Convert internal LoadedSchema to API LoadedSchema.
fn to_api_schema(db: &str, table: &str, loaded: &LoadedSchema) -> ApiLoadedSchema {
    ApiLoadedSchema {
        database: db.to_string(),
        table: table.to_string(),
        schema_json: serde_json::to_value(&loaded.schema).unwrap_or_default(),
        columns: loaded.column_names.iter().cloned().collect(),
        primary_key: loaded.schema.primary_key.clone(),
        fingerprint: loaded.fingerprint.to_string(),
        registry_version: loaded.registry_version,
        loaded_at: Utc::now(),
    }
}

/// Escape table name for use in SQL (prevent injection).
fn escape_table_name(name: &str) -> String {
    name.replace('\'', "''")
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_table_name() {
        assert_eq!(escape_table_name("users"), "users");
        assert_eq!(escape_table_name("user's"), "user''s");
        assert_eq!(escape_table_name("my'table'"), "my''table''");
    }

    #[test]
    fn test_pattern_matching_logic() {
        // Test the pattern matching logic without database
        let all_tables = vec![
            "users".to_string(),
            "orders".to_string(),
            "order_items".to_string(),
            "products".to_string(),
        ];

        // Exact match
        assert!(all_tables.contains(&"users".to_string()));

        // Prefix match simulation
        let prefix = "order";
        let matches: Vec<_> = all_tables
            .iter()
            .filter(|t| t.starts_with(prefix))
            .collect();
        assert_eq!(matches.len(), 2);

        // db.table format
        let pattern = "main.users";
        let table = pattern.split('.').last().unwrap();
        assert_eq!(table, "users");
    }

    #[tokio::test]
    async fn test_loaded_schema_clone() {
        // Ensure LoadedSchema can be cloned efficiently
        let schema = TursoTableSchema::new(vec![TursoColumn::new("id", "INTEGER", false, 0)]);

        let loaded = LoadedSchema {
            schema,
            registry_version: 1,
            fingerprint: "sha256:abc123".into(),
            sequence: 42,
            column_names: Arc::new(vec!["id".to_string()]),
        };

        let cloned = loaded.clone();
        assert_eq!(cloned.registry_version, 1);
        assert_eq!(cloned.sequence, 42);
        assert!(Arc::ptr_eq(&loaded.column_names, &cloned.column_names));
    }
}