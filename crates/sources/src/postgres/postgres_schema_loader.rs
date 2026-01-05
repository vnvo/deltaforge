//! PostgreSQL schema loader with wildcard expansion and registry integration.
//!
//! Provides schema preloading at startup with support for:
//! - Wildcard table patterns (e.g., `public.*`, `%.audit_log`)
//! - Full schema loading from information_schema
//! - Schema registry integration with fingerprinting
//! - On-demand reload capability

use schema_registry::{InMemoryRegistry, SourceSchema};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tokio_postgres::{Client, NoTls};
use tracing::{debug, info, warn};

use super::postgres_table_schema::{PostgresColumn, PostgresTableSchema};
use crate::postgres::postgres_helpers::redact_password;
use crate::schema_loader::{
    LoadedSchema as ApiLoadedSchema, SchemaListEntry, SourceSchemaLoader,
};
use deltaforge_core::{SourceError, SourceResult};

/// Loaded schema with metadata.
#[derive(Debug, Clone)]
pub struct LoadedSchema {
    pub schema: PostgresTableSchema,
    pub registry_version: i32,
    pub fingerprint: Arc<str>,
    pub sequence: u64,
    pub column_names: Arc<Vec<String>>,
}

/// Schema loader with caching and registry integration.
#[derive(Clone)]
pub struct PostgresSchemaLoader {
    dsn: String,
    /// Cache: (schema, table) -> LoadedSchema
    cache: Arc<RwLock<HashMap<(String, String), LoadedSchema>>>,
    /// Schema registry for versioning
    registry: Arc<InMemoryRegistry>,
    tenant: String,
}

impl PostgresSchemaLoader {
    /// Create a new schema loader.
    pub fn new(dsn: &str, registry: Arc<InMemoryRegistry>, tenant: &str) -> Self {
        info!(
            "creating postgres schema loader for {}",
            redact_password(dsn)
        );
        Self {
            dsn: dsn.to_string(),
            cache: Arc::new(RwLock::new(HashMap::new())),
            registry,
            tenant: tenant.to_string(),
        }
    }

    /// Get a database connection.
    async fn connect(&self) -> SourceResult<Client> {
        let (client, conn) = tokio_postgres::connect(&self.dsn, NoTls)
            .await
            .map_err(|e| SourceError::Connect {
                details: format!("postgres connect: {}", e).into(),
            })?;

        // Spawn connection task
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::error!("postgres connection error: {}", e);
            }
        });

        Ok(client)
    }

    pub fn current_sequence(&self) -> u64 {
        self.registry.current_sequence()
    }

    /// Expand wildcard patterns and preload all matching schemas.
    ///
    /// Patterns support:
    /// - `schema.table` - exact match
    /// - `schema.*` - all tables in schema
    /// - `schema.prefix%` - tables starting with prefix
    /// - `%.table` - table in any schema
    /// - `*` or empty - all tables (use with caution)
    pub async fn preload(
        &self,
        patterns: &[String],
    ) -> SourceResult<Vec<(String, String)>> {
        let t0 = Instant::now();
        let tables = self.expand_patterns(patterns).await?;

        info!(
            dsn = redact_password(&self.dsn),
            patterns = ?patterns,
            matched_tables = tables.len(),
            "expanded table patterns"
        );

        for (schema, table) in &tables {
            if let Err(e) = self.load_schema(schema, table).await {
                warn!(schema = %schema, table = %table, error = %e, "failed to preload schema");
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
        let client = self.connect().await?;
        let mut results = Vec::new();

        // Handle empty patterns = all tables
        if patterns.is_empty() {
            let rows = client
                .query(
                    "SELECT table_schema, table_name 
                     FROM information_schema.tables 
                     WHERE table_type = 'BASE TABLE' 
                     AND table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')",
                    &[],
                )
                .await
                .map_err(query_error)?;

            for row in rows {
                let schema: String = row.get(0);
                let table: String = row.get(1);
                results.push((schema, table));
            }
            return Ok(results);
        }

        for pattern in patterns {
            let (schema_pattern, table_pattern) = parse_pattern(pattern);

            let query = build_pattern_query(&schema_pattern, &table_pattern);
            let rows = client.query(&query, &[]).await.map_err(query_error)?;

            for row in rows {
                let schema: String = row.get(0);
                let table: String = row.get(1);
                if !results.contains(&(schema.clone(), table.clone())) {
                    results.push((schema, table));
                }
            }
        }

        Ok(results)
    }

    /// Load full schema for a table.
    pub async fn load_schema(
        &self,
        schema: &str,
        table: &str,
    ) -> SourceResult<LoadedSchema> {
        self.load_schema_at_checkpoint(schema, table, None).await
    }

    pub async fn load_schema_at_checkpoint(
        &self,
        schema: &str,
        table: &str,
        checkpoint: Option<&[u8]>,
    ) -> SourceResult<LoadedSchema> {
        if let Some(cached) = self
            .cache
            .read()
            .await
            .get(&(schema.to_string(), table.to_string()))
        {
            debug!(schema = %schema, table = %table, "schema cache hit");
            return Ok(cached.clone());
        }

        let t0 = Instant::now();
        let pg_schema = self.fetch_schema(schema, table).await?;
        let fingerprint = pg_schema.fingerprint();
        let column_names: Arc<Vec<String>> = Arc::new(
            pg_schema.columns.iter().map(|c| c.name.clone()).collect(),
        );

        // Register with schema registry
        let schema_json = serde_json::to_value(&pg_schema)
            .map_err(|e| SourceError::Other(e.into()))?;

        // Register with checkpoint
        let version = self
            .registry
            .register_with_checkpoint(
                &self.tenant,
                schema,
                table,
                &fingerprint,
                &schema_json,
                checkpoint,
            )
            .await
            .map_err(SourceError::Other)?;

        let loaded = LoadedSchema {
            schema: pg_schema,
            registry_version: version,
            fingerprint: fingerprint.into(),
            sequence: self.registry.current_sequence(),
            column_names,
        };

        // Cache it
        self.cache
            .write()
            .await
            .insert((schema.to_string(), table.to_string()), loaded.clone());

        let elapsed = t0.elapsed();
        if elapsed.as_millis() > 200 {
            warn!(schema = %schema, table = %table, ms = elapsed.as_millis(), "slow schema load");
        } else {
            debug!(schema = %schema, table = %table, version = version, ms = elapsed.as_millis(), "schema loaded");
        }

        Ok(loaded)
    }

    /// Force reload schema from database (bypasses cache).
    pub async fn reload_schema(
        &self,
        schema: &str,
        table: &str,
    ) -> SourceResult<LoadedSchema> {
        // Remove from cache
        self.cache
            .write()
            .await
            .remove(&(schema.to_string(), table.to_string()));

        // Reload
        self.load_schema(schema, table).await
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
    pub fn get_cached(&self, schema: &str, table: &str) -> Option<LoadedSchema> {
        // Note: This is sync because we're using try_read to avoid blocking
        self.cache
            .try_read()
            .ok()
            .and_then(|c| c.get(&(schema.to_string(), table.to_string())).cloned())
    }

    /// Fetch full schema from information_schema.
    async fn fetch_schema(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> SourceResult<PostgresTableSchema> {
        let client = self.connect().await?;

        // Fetch column information
        let col_rows = client
            .query(
                r#"
                SELECT 
                    c.column_name,
                    c.data_type,
                    c.udt_name,
                    c.is_nullable,
                    c.ordinal_position,
                    c.column_default,
                    c.character_maximum_length,
                    c.numeric_precision,
                    c.numeric_scale,
                    c.is_identity,
                    c.identity_generation,
                    c.is_generated
                FROM information_schema.columns c
                WHERE c.table_schema = $1 AND c.table_name = $2
                ORDER BY c.ordinal_position
                "#,
                &[&schema_name, &table_name],
            )
            .await
            .map_err(query_error)?;

        if col_rows.is_empty() {
            return Err(SourceError::Schema {
                details: format!("table {}.{} not found", schema_name, table_name).into(),
            });
        }

        let columns: Vec<PostgresColumn> = col_rows
            .iter()
            .map(|row| {
                let name: String = row.get(0);
                let data_type: String = row.get(1);
                let udt_name: String = row.get(2);
                let is_nullable: String = row.get(3);
                let ordinal: i32 = row.get(4);
                let default: Option<String> = row.get(5);
                let char_max_len: Option<i32> = row.get(6);
                let num_precision: Option<i32> = row.get(7);
                let num_scale: Option<i32> = row.get(8);
                let is_identity: String = row.get(9);
                let identity_gen: Option<String> = row.get(10);
                let is_generated: String = row.get(11);

                let is_array = data_type == "ARRAY";
                let effective_type = if is_array {
                    format!("{}[]", udt_name.trim_start_matches('_'))
                } else {
                    data_type.clone()
                };

                let mut col = PostgresColumn::new(
                    &name,
                    &effective_type,
                    is_nullable == "YES",
                    ordinal,
                );

                if let Some(def) = default {
                    col = col.with_default(def);
                }
                if let Some(len) = char_max_len {
                    col = col.with_char_max_length(len);
                }
                if let (Some(prec), Some(scale)) = (num_precision, num_scale) {
                    col = col.with_numeric(prec, scale);
                }
                if is_identity == "YES" {
                    col = col.with_identity(identity_gen.unwrap_or_default());
                }
                if is_generated == "ALWAYS" {
                    col.is_generated = true;
                }
                if is_array {
                    col = col.as_array(udt_name.trim_start_matches('_'));
                }
                col.udt_name = Some(udt_name);

                col
            })
            .collect();

        // Fetch primary key
        let pk_rows = client
            .query(
                r#"
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = ($1 || '.' || $2)::regclass
                AND i.indisprimary
                ORDER BY array_position(i.indkey, a.attnum)
                "#,
                &[&schema_name, &table_name],
            )
            .await
            .map_err(query_error)?;

        let primary_key: Vec<String> = pk_rows.iter().map(|r| r.get(0)).collect();

        // Fetch replica identity
        let identity_row = client
            .query_opt(
                r#"
                SELECT relreplident
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = $1 AND c.relname = $2
                "#,
                &[&schema_name, &table_name],
            )
            .await
            .map_err(query_error)?;

        let replica_identity = identity_row.map(|r| {
            let ident: i8 = r.get(0);
            match ident as u8 as char {
                'd' => "default".to_string(),
                'n' => "nothing".to_string(),
                'f' => "full".to_string(),
                'i' => "index".to_string(),
                _ => "unknown".to_string(),
            }
        });

        // Fetch table OID
        let oid_row = client
            .query_opt(
                "SELECT ($1 || '.' || $2)::regclass::oid",
                &[&schema_name, &table_name],
            )
            .await
            .map_err(query_error)?;

        let oid: Option<u32> = oid_row.map(|r| r.get::<_, u32>(0));

        Ok(PostgresTableSchema {
            columns,
            primary_key,
            replica_identity,
            oid,
            schema_name: Some(schema_name.to_string()),
        })
    }

    /// Get column names only (for backward compatibility with event handling).
    pub async fn column_names(
        &self,
        schema: &str,
        table: &str,
    ) -> SourceResult<Arc<Vec<String>>> {
        let loaded = self.load_schema(schema, table).await?;
        Ok(Arc::clone(&loaded.column_names))
    }

    /// Create a loader with pre-populated cache (for testing only).
    #[cfg(test)]
    pub(crate) fn from_static(
        cols: HashMap<(String, String), Arc<Vec<String>>>,
    ) -> Self {
        use schema_registry::InMemoryRegistry;

        // Convert column-only map to LoadedSchema map
        let cache: HashMap<(String, String), LoadedSchema> = cols
            .into_iter()
            .map(|((schema, table), col_names)| {
                let columns: Vec<PostgresColumn> = col_names
                    .iter()
                    .enumerate()
                    .map(|(i, name)| {
                        PostgresColumn::new(name, "text", true, i as i32 + 1)
                    })
                    .collect();
                let pg_schema = PostgresTableSchema::new(columns);
                let fingerprint = pg_schema.fingerprint();
                let loaded = LoadedSchema {
                    schema: pg_schema,
                    registry_version: 1,
                    fingerprint: fingerprint.into(),
                    sequence: 0,
                    column_names: col_names,
                };
                ((schema, table), loaded)
            })
            .collect();

        Self {
            dsn: "host=localhost".to_string(),
            cache: Arc::new(RwLock::new(cache)),
            registry: Arc::new(InMemoryRegistry::new()),
            tenant: "test".to_string(),
        }
    }
}

/// Parse a pattern into (schema_pattern, table_pattern).
fn parse_pattern(pattern: &str) -> (String, String) {
    if let Some((schema, table)) = pattern.split_once('.') {
        (schema.to_string(), table.to_string())
    } else {
        // Just table name, default to public schema
        ("public".to_string(), pattern.to_string())
    }
}

/// Build SQL query for pattern matching.
fn build_pattern_query(schema_pattern: &str, table_pattern: &str) -> String {
    let schema_clause = if schema_pattern == "*" || schema_pattern == "%" {
        "table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')".to_string()
    } else if schema_pattern.contains('%') || schema_pattern.contains('_') {
        format!("table_schema LIKE '{}'", escape_like(schema_pattern))
    } else {
        format!("table_schema = '{}'", escape_sql(schema_pattern))
    };

    let table_clause = if table_pattern == "*" || table_pattern == "%" {
        "1=1".to_string()
    } else if table_pattern.contains('%') || table_pattern.contains('_') {
        format!("table_name LIKE '{}'", escape_like(table_pattern))
    } else {
        format!("table_name = '{}'", escape_sql(table_pattern))
    };

    format!(
        "SELECT table_schema, table_name FROM information_schema.tables \
         WHERE table_type = 'BASE TABLE' AND {} AND {}",
        schema_clause, table_clause
    )
}

fn escape_sql(s: &str) -> String {
    s.replace('\'', "''")
}

fn escape_like(s: &str) -> String {
    // For LIKE patterns, we don't escape % and _ as they're wildcards
    s.replace('\'', "''")
}

fn query_error(e: tokio_postgres::Error) -> SourceError {
    SourceError::Other(anyhow::anyhow!("postgres query: {}", e))
}

#[async_trait::async_trait]
impl SourceSchemaLoader for PostgresSchemaLoader {
    fn source_type(&self) -> &'static str {
        "postgres"
    }

    async fn load(&self, schema: &str, table: &str) -> anyhow::Result<ApiLoadedSchema> {
        let loaded = self.load_schema(schema, table).await?;
        Ok(to_api_schema(schema, table, &loaded))
    }

    async fn reload(&self, schema: &str, table: &str) -> anyhow::Result<ApiLoadedSchema> {
        let loaded = self.reload_schema(schema, table).await?;
        Ok(to_api_schema(schema, table, &loaded))
    }

    async fn reload_all(&self, patterns: &[String]) -> anyhow::Result<Vec<(String, String)>> {
        // Clear cache and re-preload
        self.cache.write().await.clear();
        self.preload(patterns).await.map_err(Into::into)
    }

    async fn list_cached(&self) -> Vec<SchemaListEntry> {
        self.cache
            .read()
            .await
            .iter()
            .map(|((schema, table), loaded)| SchemaListEntry {
                database: schema.clone(),
                table: table.clone(),
                column_count: loaded.schema.columns.len(),
                primary_key: loaded.schema.primary_key.clone(),
                fingerprint: loaded.fingerprint.to_string(),
                registry_version: loaded.registry_version,
            })
            .collect()
    }
}

/// Convert internal LoadedSchema to API LoadedSchema
fn to_api_schema(schema: &str, table: &str, loaded: &LoadedSchema) -> ApiLoadedSchema {
    ApiLoadedSchema {
        database: schema.to_string(),
        table: table.to_string(),
        schema_json: serde_json::to_value(&loaded.schema).unwrap_or_default(),
        columns: loaded.column_names.iter().cloned().collect(),
        primary_key: loaded.schema.primary_key.clone(),
        fingerprint: loaded.fingerprint.to_string(),
        registry_version: loaded.registry_version,
        loaded_at: chrono::Utc::now(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_pattern() {
        assert_eq!(parse_pattern("public.users"), ("public".into(), "users".into()));
        assert_eq!(parse_pattern("myschema.*"), ("myschema".into(), "*".into()));
        assert_eq!(parse_pattern("%.audit"), ("%".into(), "audit".into()));
        assert_eq!(parse_pattern("orders"), ("public".into(), "orders".into()));
    }

    #[test]
    fn test_build_pattern_query() {
        let q = build_pattern_query("public", "users");
        assert!(q.contains("table_schema = 'public'"));
        assert!(q.contains("table_name = 'users'"));

        let q = build_pattern_query("public", "*");
        assert!(q.contains("table_schema = 'public'"));
        assert!(q.contains("1=1"));

        let q = build_pattern_query("%", "audit%");
        assert!(q.contains("table_schema NOT IN"));
        assert!(q.contains("table_name LIKE 'audit%'"));
    }
}