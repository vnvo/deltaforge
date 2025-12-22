//! MySQL schema loader with wildcard expansion and registry integration.
//!
//! Provides schema preloading at startup with support for:
//! - Wildcard table patterns (e.g., `orders.*`, `%.audit_log`)
//! - Full schema loading from INFORMATION_SCHEMA
//! - Schema registry integration with fingerprinting
//! - On-demand reload capability

use mysql_async::{Pool, Row, prelude::Queryable};
use schema_registry::{InMemoryRegistry, SourceSchema};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use super::mysql_table_schema::{MySqlColumn, MySqlTableSchema};
use crate::mysql::mysql_helpers::redact_password;
use deltaforge_core::{SchemaRegistry, SourceError, SourceResult};

/// Loaded schema with metadata.
#[derive(Debug, Clone)]
pub struct LoadedSchema {
    pub schema: MySqlTableSchema,
    pub registry_version: i32,
    pub fingerprint: String,
    pub sequence: u64,
}

/// Schema loader with caching and registry integration.
#[derive(Clone)]
pub struct MySqlSchemaLoader {
    pool: Pool,
    dsn: String,
    /// Cache: (db, table) -> LoadedSchema
    cache: Arc<RwLock<HashMap<(String, String), LoadedSchema>>>,
    /// Schema registry for versioning
    registry: Arc<InMemoryRegistry>,
    tenant: String,
}

impl MySqlSchemaLoader {
    /// Create a new schema loader.
    pub fn new(
        dsn: &str,
        registry: Arc<InMemoryRegistry>,
        tenant: &str,
    ) -> Self {
        info!("creating mysql schema loader for {}", redact_password(dsn));
        Self {
            pool: Pool::new(dsn),
            dsn: dsn.to_string(),
            cache: Arc::new(RwLock::new(HashMap::new())),
            registry,
            tenant: tenant.to_string(),
        }
    }

    pub fn current_sequence(&self) -> u64 {
        self.registry.current_sequence()
    }

    /// Expand wildcard patterns and preload all matching schemas.
    ///
    /// Patterns support:
    /// - `db.table` - exact match
    /// - `db.*` - all tables in db
    /// - `db.prefix%` - tables starting with prefix
    /// - `%.table` - table in any database
    /// - `*` or empty - all tables (use with caution)
    pub async fn preload(
        &self,
        patterns: &[String],
    ) -> SourceResult<Vec<(String, String)>> {
        let t0 = Instant::now();
        let tables = self.expand_patterns(patterns).await?;

        info!(
            dns=redact_password(&self.dsn),
            patterns = ?patterns,
            matched_tables = tables.len(),
            "expanded table patterns"
        );

        for (db, table) in &tables {
            if let Err(e) = self.load_schema(db, table).await {
                warn!(db = %db, table = %table, error = %e, "failed to preload schema");
            }
        }

        let elapsed = t0.elapsed();
        info!(
            tables_loaded = tables.len(),
            elapsed_ms = elapsed.as_millis(),
            "schema preload complete"
        );

        Ok(tables)
    }

    /// Expand wildcard patterns to actual table list.
    pub async fn expand_patterns(
        &self,
        patterns: &[String],
    ) -> SourceResult<Vec<(String, String)>> {
        let mut conn = self.pool.get_conn().await.map_err(conn_error)?;
        let mut results = Vec::new();

        // Handle empty patterns = all tables
        if patterns.is_empty() {
            let rows: Vec<Row> = conn
                .query(
                    "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES 
                     WHERE TABLE_TYPE = 'BASE TABLE' 
                     AND TABLE_SCHEMA NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')",
                )
                .await
                .map_err(query_error)?;

            for mut row in rows {
                let db: String = row.take("TABLE_SCHEMA").unwrap();
                let table: String = row.take("TABLE_NAME").unwrap();
                results.push((db, table));
            }
            return Ok(results);
        }

        for pattern in patterns {
            let (db_pattern, table_pattern) = parse_pattern(pattern);

            let query = build_pattern_query(&db_pattern, &table_pattern);
            let rows: Vec<Row> =
                conn.query(&query).await.map_err(query_error)?;

            for mut row in rows {
                let db: String = row.take("TABLE_SCHEMA").unwrap();
                let table: String = row.take("TABLE_NAME").unwrap();
                if !results.contains(&(db.clone(), table.clone())) {
                    results.push((db, table));
                }
            }
        }

        Ok(results)
    }

    /// Load full schema for a table.
    pub async fn load_schema(
        &self,
        db: &str,
        table: &str,
    ) -> SourceResult<LoadedSchema> {
        self.load_schema_at_checkpoint(db, table, None).await
    }

    pub async fn load_schema_at_checkpoint(
        &self,
        db: &str,
        table: &str,
        checkpoint: Option<&[u8]>,
    ) -> SourceResult<LoadedSchema> {
        if let Some(cached) = self
            .cache
            .read()
            .await
            .get(&(db.to_string(), table.to_string()))
        {
            debug!(db = %db, table = %table, "schema cache hit");
            return Ok(cached.clone());
        }

        let t0 = Instant::now();
        let schema = self.fetch_schema(db, table).await?;
        let fingerprint = schema.fingerprint();

        // Register with schema registry
        let schema_json = serde_json::to_value(&schema)
            .map_err(|e| SourceError::Other(e.into()))?;
        // Register with checkpoint
        let version = self
            .registry
            .register_with_checkpoint(
                &self.tenant,
                db,
                table,
                &fingerprint,
                &schema_json,
                checkpoint,
            )
            .await
            .map_err(|e| SourceError::Other(e))?;
        let loaded = LoadedSchema {
            schema,
            registry_version: version,
            fingerprint,
            sequence: self.registry.current_sequence(),
        };

        // Cache it
        self.cache
            .write()
            .await
            .insert((db.to_string(), table.to_string()), loaded.clone());

        let elapsed = t0.elapsed();
        if elapsed.as_millis() > 200 {
            warn!(db = %db, table = %table, ms = elapsed.as_millis(), "slow schema load");
        } else {
            debug!(db = %db, table = %table, version = version, ms = elapsed.as_millis(), "schema loaded");
        }

        Ok(loaded)
    }
    /// Force reload schema from database (bypasses cache).
    pub async fn reload_schema(
        &self,
        db: &str,
        table: &str,
    ) -> SourceResult<LoadedSchema> {
        // Remove from cache
        self.cache
            .write()
            .await
            .remove(&(db.to_string(), table.to_string()));

        // Reload
        self.load_schema(db, table).await
    }

    /// Reload all schemas matching patterns.
    pub async fn reload_all(
        &self,
        patterns: &[String],
    ) -> SourceResult<Vec<(String, String)>> {
        // Clear cache
        self.cache.write().await.clear();

        // Re-expand and reload
        self.preload(patterns).await
    }

    /// Get cached schema (without loading from DB).
    pub fn get_cached(&self, db: &str, table: &str) -> Option<LoadedSchema> {
        // Note: This is sync because we're using try_read to avoid blocking
        self.cache.try_read().ok().and_then(|guard| {
            guard.get(&(db.to_string(), table.to_string())).cloned()
        })
    }

    /// Get all cached schemas.
    pub async fn list_cached(&self) -> Vec<((String, String), LoadedSchema)> {
        self.cache
            .read()
            .await
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Check if schema has changed and update if needed.
    pub async fn check_and_update(
        &self,
        db: &str,
        table: &str,
    ) -> SourceResult<bool> {
        let current = self.fetch_schema(db, table).await?;
        let current_fp = current.fingerprint();

        let changed = {
            let cache = self.cache.read().await;
            cache
                .get(&(db.to_string(), table.to_string()))
                .map(|cached| cached.fingerprint != current_fp)
                .unwrap_or(true)
        };

        if changed {
            info!(db = %db, table = %table, "schema change detected, reloading");
            self.reload_schema(db, table).await?;
        }

        Ok(changed)
    }

    /// Invalidate cache for a database (called on DDL).
    pub async fn invalidate_db(&self, db: &str) {
        let before = self.cache.read().await.len();
        self.cache.write().await.retain(|(d, _), _| d != db);
        let after = self.cache.read().await.len();
        info!(db = %db, removed = before.saturating_sub(after), "schema cache invalidated");
    }

    /// Fetch schema from INFORMATION_SCHEMA.
    async fn fetch_schema(
        &self,
        db: &str,
        table: &str,
    ) -> SourceResult<MySqlTableSchema> {
        let mut conn = self.pool.get_conn().await.map_err(conn_error)?;

        // Fetch columns
        let col_rows: Vec<Row> = conn
            .exec(
                r#"
                SELECT 
                    COLUMN_NAME,
                    COLUMN_TYPE,
                    DATA_TYPE,
                    IS_NULLABLE,
                    ORDINAL_POSITION,
                    COLUMN_DEFAULT,
                    EXTRA,
                    COLUMN_COMMENT,
                    CHARACTER_MAXIMUM_LENGTH,
                    NUMERIC_PRECISION,
                    NUMERIC_SCALE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
                "#,
                (db, table),
            )
            .await
            .map_err(query_error)?;

        if col_rows.is_empty() {
            return Err(SourceError::Other(anyhow::anyhow!(
                "table {}.{} not found or has no columns",
                db,
                table
            )));
        }

        let columns: Vec<MySqlColumn> = col_rows
            .into_iter()
            .map(|mut row| MySqlColumn {
                name: row.take("COLUMN_NAME").unwrap(),
                column_type: row.take("COLUMN_TYPE").unwrap(),
                data_type: row.take("DATA_TYPE").unwrap(),
                nullable: row.take::<String, _>("IS_NULLABLE").unwrap()
                    == "YES",
                ordinal_position: row.take("ORDINAL_POSITION").unwrap(),
                // nullable columns
                default_value: row
                    .take::<Option<String>, _>("COLUMN_DEFAULT")
                    .unwrap(),
                extra: row.take::<Option<String>, _>("EXTRA").unwrap(),
                comment: row
                    .take::<Option<String>, _>("COLUMN_COMMENT")
                    .unwrap(),
                char_max_length: row
                    .take::<Option<i64>, _>("CHARACTER_MAXIMUM_LENGTH")
                    .unwrap(),
                numeric_precision: row
                    .take::<Option<i64>, _>("NUMERIC_PRECISION")
                    .unwrap(),
                numeric_scale: row
                    .take::<Option<i64>, _>("NUMERIC_SCALE")
                    .unwrap(),
            })
            .collect();

        // Fetch primary key
        let pk_rows: Vec<Row> = conn
            .exec(
                r#"
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'
                ORDER BY ORDINAL_POSITION
                "#,
                (db, table),
            )
            .await
            .map_err(query_error)?;

        let primary_key: Vec<String> = pk_rows
            .into_iter()
            .map(|mut row| row.take("COLUMN_NAME").unwrap())
            .collect();

        // Fetch table metadata
        let table_row: Option<Row> = conn
            .exec_first(
                r#"
                SELECT ENGINE, TABLE_COLLATION
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                "#,
                (db, table),
            )
            .await
            .map_err(query_error)?;

        let (engine, collation) = if let Some(mut row) = table_row {
            (row.take("ENGINE"), row.take("TABLE_COLLATION"))
        } else {
            (None, None)
        };

        Ok(MySqlTableSchema {
            columns,
            primary_key,
            engine,
            charset: None,
            collation,
        })
    }

    /// Get column names only (for backward compatibility with event handling).
    pub async fn column_names(
        &self,
        db: &str,
        table: &str,
    ) -> SourceResult<Arc<Vec<String>>> {
        let loaded = self.load_schema(db, table).await?;
        Ok(Arc::new(
            loaded
                .schema
                .columns
                .iter()
                .map(|c| c.name.clone())
                .collect(),
        ))
    }

    /// Create a loader with pre-populated cache (for testing only).
    /// Does not connect to any database.
    #[cfg(test)]
    pub(crate) fn from_static(
        cols: HashMap<(String, String), Arc<Vec<String>>>,
    ) -> Self {
        use schema_registry::InMemoryRegistry;

        // Convert column-only map to LoadedSchema map
        let cache: HashMap<(String, String), LoadedSchema> = cols
            .into_iter()
            .map(|((db, table), col_names)| {
                let columns: Vec<MySqlColumn> = col_names
                    .iter()
                    .enumerate()
                    .map(|(i, name)| {
                        MySqlColumn::new(
                            name,
                            "varchar(255)",
                            "varchar",
                            true,
                            i as u32 + 1,
                        )
                    })
                    .collect();
                let schema = MySqlTableSchema::new(columns);
                let fingerprint = schema.fingerprint();
                let loaded = LoadedSchema {
                    schema,
                    registry_version: 1,
                    fingerprint,
                    sequence: 0,
                };
                ((db, table), loaded)
            })
            .collect();

        Self {
            pool: Pool::new("mysql://localhost/ignored"),
            dsn: "mysql://localhost/ignored".to_string(),
            cache: Arc::new(RwLock::new(cache)),
            registry: Arc::new(InMemoryRegistry::new()),
            tenant: "test".to_string(),
        }
    }
}

/// Parse a pattern into (db_pattern, table_pattern).
fn parse_pattern(pattern: &str) -> (String, String) {
    if let Some((db, table)) = pattern.split_once('.') {
        (db.to_string(), table.to_string())
    } else {
        // Just table name, match any database
        ("%".to_string(), pattern.to_string())
    }
}

/// Build SQL query for pattern matching.
fn build_pattern_query(db_pattern: &str, table_pattern: &str) -> String {
    let db_clause = if db_pattern == "*" || db_pattern == "%" {
        "TABLE_SCHEMA NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')".to_string()
    } else if db_pattern.contains('%') || db_pattern.contains('_') {
        format!("TABLE_SCHEMA LIKE '{}'", escape_like(db_pattern))
    } else {
        format!("TABLE_SCHEMA = '{}'", escape_sql(db_pattern))
    };

    let table_clause = if table_pattern == "*" || table_pattern == "%" {
        "1=1".to_string()
    } else if table_pattern.contains('%') || table_pattern.contains('_') {
        format!("TABLE_NAME LIKE '{}'", escape_like(table_pattern))
    } else {
        format!("TABLE_NAME = '{}'", escape_sql(table_pattern))
    };

    format!(
        "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES \
         WHERE TABLE_TYPE = 'BASE TABLE' AND {} AND {}",
        db_clause, table_clause
    )
}

fn escape_sql(s: &str) -> String {
    s.replace('\'', "''")
}

fn escape_like(s: &str) -> String {
    // For LIKE patterns, we don't escape % and _ as they're wildcards
    s.replace('\'', "''")
}

fn conn_error(e: mysql_async::Error) -> SourceError {
    SourceError::Connect {
        details: format!("mysql connection: {}", e).into(),
    }
}

fn query_error(e: mysql_async::Error) -> SourceError {
    SourceError::Other(anyhow::anyhow!("mysql query: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_pattern() {
        assert_eq!(parse_pattern("db.table"), ("db".into(), "table".into()));
        assert_eq!(parse_pattern("db.*"), ("db".into(), "*".into()));
        assert_eq!(parse_pattern("%.audit"), ("%".into(), "audit".into()));
        assert_eq!(parse_pattern("table"), ("%".into(), "table".into()));
    }

    #[test]
    fn test_build_pattern_query() {
        let q = build_pattern_query("orders", "items");
        assert!(q.contains("TABLE_SCHEMA = 'orders'"));
        assert!(q.contains("TABLE_NAME = 'items'"));

        let q = build_pattern_query("orders", "*");
        assert!(q.contains("TABLE_SCHEMA = 'orders'"));
        assert!(q.contains("1=1"));

        let q = build_pattern_query("%", "audit%");
        assert!(q.contains("TABLE_SCHEMA NOT IN"));
        assert!(q.contains("TABLE_NAME LIKE 'audit%'"));
    }
}
