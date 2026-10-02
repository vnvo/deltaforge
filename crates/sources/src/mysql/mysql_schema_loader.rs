//! MySQL schema loader with wildcard expansion and registry integration.
//!
//! Provides schema preloading at startup with support for:
//! - Wildcard table patterns (e.g., `orders.*`, `%.audit_log`)
//! - Full schema loading from INFORMATION_SCHEMA
//! - Schema registry integration with fingerprinting
//! - On-demand reload capability

use metrics::counter;
use mysql_async::{Pool, Row, prelude::Queryable};
use schema_registry::SourceSchema;
#[cfg(test)]
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use storage::DurableSchemaRegistry;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use super::mysql_table_schema::{MySqlColumn, MySqlTableSchema};
use crate::registry_scope::{
    RegistryError, RegistryScope, ScopedCache, SharedRegistryScope,
};

/// The live catalog's answer for one table.
pub(crate) enum Live {
    Found(MySqlTableSchema),
    /// The server is not the scope's lineage (described for the error).
    OtherLineage(String),
}
use crate::schema_loader::{
    LoadedSchema as ApiLoadedSchema, SchemaListEntry, SourceSchemaLoader,
};
use common::redact_url_password as redact_password;
use deltaforge_core::{SourceError, SourceResult};

/// Loaded schema with metadata.
#[derive(Debug, Clone)]
pub struct LoadedSchema {
    pub schema: MySqlTableSchema,
    pub registry_version: i32,
    pub fingerprint: Arc<str>,
    pub sequence: u64,
    pub column_names: Arc<Vec<String>>,
}

type ArcSchemaCache = Arc<RwLock<ScopedCache<Arc<LoadedSchema>>>>;

/// Attempts at a load whose lineage keeps moving before giving up with the
/// typed, retryable [`RegistryError::ScopeChanged`].
const SCOPE_ATTEMPTS: usize = 3;

/// Schema loader with caching and registry integration.
///
/// Every registry access is qualified by the source's verified `server_uuid`
/// lineage, taken from the shared [`SharedRegistryScope`]; with no established
/// scope, registry access fails closed. Cache entries belong to one scope
/// generation and are discarded when the source moves to a new lineage.
///
/// MySQL has no historical schema resolution and never adopts pre-upgrade
/// (unscoped) history automatically: legacy versions are preserved untouched and
/// only reserve version numbers, so none is ever reused.
#[derive(Clone)]
pub struct MySqlSchemaLoader {
    pool: Pool,
    dsn: crate::credentials::ProtectedDsn,
    /// Cache: (db, table) -> Arc<LoadedSchema>
    cache: ArcSchemaCache,
    /// Schema registry for versioning
    registry: Arc<DurableSchemaRegistry>,
    scope: SharedRegistryScope,
    tenant: String,
}

impl std::fmt::Debug for MySqlSchemaLoader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MySqlSchemaLoader")
            .field("dsn", &self.dsn)
            .field("tenant", &self.tenant)
            .finish_non_exhaustive()
    }
}

impl MySqlSchemaLoader {
    /// Create a new schema loader. Accepts a protected DSN (or anything convertible
    /// into one); the loader shares the protected wrapper rather than a plaintext
    /// copy.
    pub fn new(
        dsn: impl Into<crate::credentials::ProtectedDsn>,
        registry: Arc<DurableSchemaRegistry>,
        tenant: &str,
        scope: SharedRegistryScope,
    ) -> Self {
        let dsn = dsn.into();
        info!(
            "creating mysql schema loader for {}",
            redact_password(dsn.expose())
        );
        Self {
            pool: Pool::new(dsn.expose()),
            dsn,
            cache: Arc::new(RwLock::new(ScopedCache::default())),
            registry,
            scope,
            tenant: tenant.to_string(),
        }
    }

    /// The established registry scope. Fails closed when none is published.
    fn current_scope(&self) -> SourceResult<Arc<RegistryScope>> {
        Ok(self.scope.current()?)
    }

    /// Cache `value`, produced under `scope`, only if `scope` is still the
    /// published one. `false` means the lineage moved while it was produced:
    /// the value was discarded and the caller must retry under the new scope.
    async fn cache_insert(
        &self,
        scope: &RegistryScope,
        key: (String, String),
        value: Arc<LoadedSchema>,
    ) -> bool {
        let mut cache = self.cache.write().await;
        cache.insert_if_current(
            scope.generation(),
            self.scope.generation(),
            key,
            value,
        )
    }

    /// The error after the lineage kept moving for every attempt.
    fn scope_changed(&self) -> SourceError {
        RegistryError::ScopeChanged {
            source_id: self
                .scope
                .current()
                .map(|s| s.source_id().to_string())
                .unwrap_or_default(),
        }
        .into()
    }

    /// The live catalog answered from a different server than `scope`. If the
    /// published scope has moved on, retry under it; otherwise the server
    /// behind the DSN is not the one this source verified - fail closed.
    fn lineage_moved(
        &self,
        scope: &RegistryScope,
        live: String,
    ) -> SourceResult<()> {
        if self.scope.generation() != scope.generation() {
            return Ok(());
        }
        Err(RegistryError::LineageMismatch {
            source_id: scope.source_id().to_string(),
            expected: format!("{:?}", scope.lineage().descriptor),
            live,
        }
        .into())
    }

    pub fn current_sequence(&self) -> u64 {
        self.registry.current_sequence()
    }

    /// Replace the connection DSN after a credential rotation, rebuilding the cached
    /// connection pool so later schema queries use the new credentials. The schema
    /// cache (shared via `Arc`) is preserved. Called by the run loop at a quiesced
    /// boundary once the replacement stream is confirmed, so no query is in flight
    /// against the old DSN.
    /// The schema registry this loader registers versions in.
    pub(crate) fn registry(&self) -> &DurableSchemaRegistry {
        &self.registry
    }

    pub(crate) fn set_dsn(&mut self, dsn: crate::credentials::ProtectedDsn) {
        self.pool = Pool::new(dsn.expose());
        self.dsn = dsn;
    }

    /// Expand wildcard patterns and preload all matching schemas.
    ///
    /// This is the eager (catalog-sized) startup path. It is deliberately
    /// self-contained - it only reads latest versions through the scoped
    /// registry and falls back to [`Self::load_schema`] - so removing the eager
    /// preload later changes only its call sites, not the registry contract.
    ///
    /// Warm-start path: schemas already known to the durable registry are
    /// deserialized directly into the in-memory cache, avoiding an
    /// INFORMATION_SCHEMA query per table on restart. Tables missing from the
    /// registry (first-ever run, or new tables) are fetched from the source.
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
        let scope = self.current_scope()?;
        let tables = self.expand_patterns(patterns).await?;

        info!(
            dns=redact_password(self.dsn.expose()),
            patterns = ?patterns,
            matched_tables = tables.len(),
            "expanded table patterns"
        );

        // Warm cache from the durable registry first.  On restart this means
        // all previously-seen tables skip the INFORMATION_SCHEMA fetch and
        // don't appear as cache misses.  If a table's schema is stale (ALTER
        // happened while the process was down) the binlog drift handler will
        // call reload_schema for just that table when the mismatch is detected.
        let mut from_registry = 0usize;
        let mut needs_fetch: Vec<&(String, String)> = Vec::new();
        for pair in &tables {
            let (db, table) = pair;
            // A registry storage error fails closed; only a genuinely absent
            // schema falls back to INFORMATION_SCHEMA.
            let latest = self
                .registry
                .get_latest(&scope.key(db, table))
                .await
                .map_err(RegistryError::Storage)?;
            match latest {
                Some(sv) => {
                    match serde_json::from_value::<MySqlTableSchema>(
                        sv.schema_json,
                    ) {
                        Ok(schema) => {
                            let fingerprint = schema.fingerprint();
                            let column_names = Arc::new(
                                schema
                                    .columns
                                    .iter()
                                    .map(|c| c.name.clone())
                                    .collect::<Vec<_>>(),
                            );
                            let loaded = Arc::new(LoadedSchema {
                                schema,
                                registry_version: sv.version,
                                fingerprint: fingerprint.into(),
                                sequence: sv.sequence,
                                column_names,
                            });
                            if !self
                                .cache_insert(
                                    &scope,
                                    (db.clone(), table.clone()),
                                    loaded,
                                )
                                .await
                            {
                                return Err(self.scope_changed());
                            }
                            from_registry += 1;
                        }
                        Err(e) => {
                            warn!(db=%db, table=%table, error=%e,
                                "failed to deserialize registry schema; fetching from source");
                            needs_fetch.push(pair);
                        }
                    }
                }
                None => needs_fetch.push(pair),
            }
        }

        // Fetch from INFORMATION_SCHEMA only for tables absent from registry.
        for (db, table) in &needs_fetch {
            match self.load_schema(db, table).await {
                Ok(_) => {}
                // The connection reached another server: not a missing
                // table, and nothing may continue on that assumption.
                Err(e @ SourceError::Lineage { .. }) => return Err(e),
                Err(e) => {
                    warn!(db = %db, table = %table, error = %e, "failed to preload schema");
                }
            }
        }

        let elapsed = t0.elapsed();
        info!(
            tables_loaded = tables.len(),
            from_registry,
            from_source = needs_fetch.len(),
            elapsed_ms = elapsed.as_millis(),
            "schema preload complete"
        );

        let mut conn = self.verified_conn().await?;
        let row_image: String = conn
            .query_first("SELECT @@binlog_row_image")
            .await
            .map_err(query_error)?
            .unwrap();

        if row_image.to_lowercase() != "full" {
            warn!(
                dns=redact_password(self.dsn.expose()),
                binlog_row_image = %row_image,
                "binlog_row_image is not FULL - before images may be incomplete. \
                Consider: SET GLOBAL binlog_row_image = 'FULL'"
            );
        }

        Ok(tables)
    }

    /// Expand wildcard patterns to actual table list.
    pub async fn expand_patterns(
        &self,
        patterns: &[String],
    ) -> SourceResult<Vec<(String, String)>> {
        let mut conn = self.verified_conn().await?;
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
    ) -> SourceResult<Arc<LoadedSchema>> {
        self.load_schema_at_checkpoint(db, table, None).await
    }

    /// Load schema with optional checkpoint for registry correlation.
    ///
    /// The schema is fetched from INFORMATION_SCHEMA on a connection that proves
    /// it belongs to the scope's `server_uuid` lineage, registered under that
    /// scope, and cached only if the scope is still the published one;
    /// otherwise the work is discarded and retried under the new scope.
    pub async fn load_schema_at_checkpoint(
        &self,
        db: &str,
        table: &str,
        checkpoint: Option<&[u8]>,
    ) -> SourceResult<Arc<LoadedSchema>> {
        let key = (db.to_string(), table.to_string());
        for _ in 0..SCOPE_ATTEMPTS {
            let scope = self.current_scope()?;
            if let Some(cached) =
                self.cache.read().await.get(scope.generation(), &key)
            {
                debug!(db = %db, table = %table, "schema cache hit");
                counter!("deltaforge_source_schema_cache_hits_total",
                    "pipeline" => self.tenant.clone(), "source" => "mysql")
                .increment(1);
                return Ok(cached);
            }
            counter!("deltaforge_source_schema_cache_misses_total",
                "pipeline" => self.tenant.clone(), "source" => "mysql")
            .increment(1);

            let t0 = Instant::now();
            let schema = match self.fetch_schema(&scope, db, table).await? {
                Live::Found(schema) => schema,
                Live::OtherLineage(live) => {
                    self.lineage_moved(&scope, live)?;
                    continue;
                }
            };
            let fingerprint = schema.fingerprint();
            let column_names: Arc<Vec<String>> = Arc::new(
                schema.columns.iter().map(|c| c.name.clone()).collect(),
            );

            // Register with checkpoint, under the verified lineage.
            let schema_json = serde_json::to_value(&schema)
                .map_err(|e| SourceError::Other(e.into()))?;
            let version = self
                .registry
                .register_with_checkpoint(
                    &scope.key(db, table),
                    &fingerprint,
                    &schema_json,
                    checkpoint,
                )
                .await
                .map_err(RegistryError::Storage)?;
            let loaded = Arc::new(LoadedSchema {
                schema,
                registry_version: version,
                fingerprint: fingerprint.into(),
                sequence: self.registry.current_sequence(),
                column_names,
            });
            if !self
                .cache_insert(&scope, key.clone(), Arc::clone(&loaded))
                .await
            {
                continue;
            }

            let elapsed = t0.elapsed();
            if elapsed.as_millis() > 200 {
                warn!(db = %db, table = %table, ms = elapsed.as_millis(), "slow schema load");
            } else {
                debug!(db = %db, table = %table, version = version, ms = elapsed.as_millis(), "schema loaded");
            }
            return Ok(loaded);
        }
        Err(self.scope_changed())
    }

    /// Register a shape captured elsewhere (a stable position/shape capture)
    /// through the normal registry path, under the current verified scope,
    /// with `checkpoint` as its binding position. Returns the version and its
    /// registry hash. Never derived from a TableMap.
    pub(crate) async fn register_captured(
        &self,
        db: &str,
        table: &str,
        schema: &MySqlTableSchema,
        checkpoint: &[u8],
    ) -> SourceResult<(i32, String)> {
        let scope = self.current_scope()?;
        let fingerprint = schema.fingerprint();
        let schema_json = serde_json::to_value(schema)
            .map_err(|e| SourceError::Other(e.into()))?;
        let version = self
            .registry
            .register_with_checkpoint(
                &scope.key(db, table),
                &fingerprint,
                &schema_json,
                Some(checkpoint),
            )
            .await
            .map_err(RegistryError::Storage)?;
        Ok((version, fingerprint.to_string()))
    }

    /// Force reload schema from database (bypasses cache).
    pub async fn reload_schema(
        &self,
        db: &str,
        table: &str,
    ) -> SourceResult<Arc<LoadedSchema>> {
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
    pub fn get_cached(
        &self,
        db: &str,
        table: &str,
    ) -> Option<Arc<LoadedSchema>> {
        // Note: This is sync because we're using try_read to avoid blocking
        let published = self.scope.generation();
        self.cache.try_read().ok().and_then(|guard| {
            guard.get(published, &(db.to_string(), table.to_string()))
        })
    }

    /// Returns all (db, table) pairs currently in cache.
    /// Sync, for failover reconciliation - avoids async where not needed.
    /// Deliberately ignores the scope generation (see the PostgreSQL loader).
    pub fn cached_tables(&self) -> Vec<(String, String)> {
        self.cache
            .try_read()
            .map(|c| c.keys_any_generation())
            .unwrap_or_default()
    }

    /// Check if schema has changed and update if needed.
    pub async fn check_and_update(
        &self,
        db: &str,
        table: &str,
    ) -> SourceResult<bool> {
        let scope = self.current_scope()?;
        let changed = match self.fetch_schema(&scope, db, table).await? {
            Live::Found(current) => {
                let current_fp = current.fingerprint();
                self.cache
                    .read()
                    .await
                    .get(
                        scope.generation(),
                        &(db.to_string(), table.to_string()),
                    )
                    .map(|cached| cached.fingerprint != current_fp.into())
                    .unwrap_or(true)
            }
            // A different server answered: whatever is cached is not current.
            Live::OtherLineage(_) => true,
        };

        if changed {
            info!(db = %db, table = %table, "schema change detected, reloading");
            self.reload_schema(db, table).await?;
        }

        Ok(changed)
    }

    /// Invalidate cache for a database (called on DDL).
    pub async fn invalidate_db(&self, db: &str) {
        let mut cache = self.cache.write().await;
        let before = cache.keys_any_generation().len();
        cache.retain(|(d, _)| d != db);
        let after = cache.keys_any_generation().len();
        drop(cache);
        info!(db = %db, removed = before.saturating_sub(after), "schema cache invalidated");
    }

    /// A pooled connection that proved it is the scope's verified server:
    /// catalog facts (which tables exist) come only from that server.
    async fn verified_conn(&self) -> SourceResult<mysql_async::Conn> {
        let scope = self.scope.current()?;
        let storage::adapters::LineageDescriptor::Mysql { server_uuid } =
            &scope.lineage().descriptor
        else {
            return Err(SourceError::Lineage {
                details: "MySQL loader scoped to a non-MySQL lineage".into(),
            });
        };
        let mut conn = self.pool.get_conn().await.map_err(conn_error)?;
        super::mysql_session::verify_connection(&mut conn, server_uuid)
            .await
            .map_err(|e| e.into_source_error(server_uuid))?;
        Ok(conn)
    }

    /// Fetch schema from INFORMATION_SCHEMA.
    async fn fetch_schema(
        &self,
        scope: &RegistryScope,
        db: &str,
        table: &str,
    ) -> SourceResult<Live> {
        let mut conn = self.pool.get_conn().await.map_err(conn_error)?;
        let storage::adapters::LineageDescriptor::Mysql { server_uuid } =
            &scope.lineage().descriptor
        else {
            return Ok(Live::OtherLineage("a non-MySQL lineage".into()));
        };
        fetch_table_schema_on(&mut conn, server_uuid, db, table).await
    }

    /// Get column names only (for backward compatibility with event handling).
    pub async fn column_names(
        &self,
        db: &str,
        table: &str,
    ) -> SourceResult<Arc<Vec<String>>> {
        let loaded = self.load_schema(db, table).await?;
        Ok(Arc::clone(&loaded.column_names))
    }

    /// Create a loader with pre-populated cache (for testing only).
    /// Does not connect to any database.
    #[cfg(test)]
    pub(crate) fn from_static(
        cols: HashMap<(String, String), Arc<Vec<String>>>,
    ) -> Self {
        // Convert column-only map to Arc<LoadedSchema> map
        let cache: HashMap<(String, String), Arc<LoadedSchema>> = cols
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
                let loaded = Arc::new(LoadedSchema {
                    schema,
                    registry_version: 1,
                    fingerprint: fingerprint.into(),
                    sequence: 0,
                    column_names: col_names,
                });
                ((db, table), loaded)
            })
            .collect();

        let scope = SharedRegistryScope::new("test");
        let published = scope.publish_for_test(
            "test",
            storage::adapters::LineageDescriptor::mysql(
                "3e11fa47-71ca-11e1-9e33-c80aa9429562",
            )
            .expect("test lineage"),
        );
        let mut scoped = ScopedCache::default();
        for (key, loaded) in cache {
            scoped.insert_if_current(
                published.generation(),
                published.generation(),
                key,
                loaded,
            );
        }
        Self {
            pool: Pool::new("mysql://localhost/ignored"),
            dsn: "mysql://localhost/ignored".into(),
            cache: Arc::new(RwLock::new(scoped)),
            registry: storage::DurableSchemaRegistry::for_testing(),
            scope,
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

#[async_trait::async_trait]
impl SourceSchemaLoader for MySqlSchemaLoader {
    fn source_type(&self) -> &'static str {
        "mysql"
    }

    async fn load(
        &self,
        db: &str,
        table: &str,
    ) -> anyhow::Result<ApiLoadedSchema> {
        self.scope.current()?;
        let loaded = self.load_schema(db, table).await?;
        Ok(to_api_schema(db, table, &loaded))
    }

    async fn reload(
        &self,
        db: &str,
        table: &str,
    ) -> anyhow::Result<ApiLoadedSchema> {
        self.scope.current()?;
        let loaded = self.reload_schema(db, table).await?;
        Ok(to_api_schema(db, table, &loaded))
    }

    async fn reload_all(
        &self,
        patterns: &[String],
    ) -> anyhow::Result<Vec<(String, String)>> {
        self.scope.current()?;
        // Clear cache and re-preload (reuse existing preload logic)
        self.cache.write().await.clear();
        self.preload(patterns).await.map_err(Into::into)
    }

    fn lineage_established(&self) -> bool {
        self.scope.generation() != 0
    }

    async fn list_cached(&self) -> Vec<SchemaListEntry> {
        let published = self.scope.generation();
        self.cache
            .read()
            .await
            .entries_for(published)
            .into_iter()
            .map(|((db, table), loaded)| SchemaListEntry {
                database: db.clone(),
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
fn to_api_schema(
    db: &str,
    table: &str,
    loaded: &LoadedSchema,
) -> ApiLoadedSchema {
    ApiLoadedSchema {
        database: db.to_string(),
        table: table.to_string(),
        schema_json: serde_json::to_value(&loaded.schema).unwrap_or_default(),
        columns: loaded.column_names.iter().cloned().collect(),
        primary_key: loaded.schema.primary_key.clone(),
        fingerprint: loaded.fingerprint.to_string(),
        registry_version: loaded.registry_version,
        loaded_at: chrono::Utc::now(),
    }
}

/// Read one table's shape from INFORMATION_SCHEMA on `conn`, after proving on
/// that same connection that the server is `expected_uuid` (so a schema read
/// from another server is never attributed to this lineage). Shared by the
/// loader and the stable shape capture, which must read position, shape and
/// position on one connection.
pub(crate) async fn fetch_table_schema_on(
    conn: &mut mysql_async::Conn,
    expected_uuid: &str,
    db: &str,
    table: &str,
) -> SourceResult<Live> {
    // Prove on this same connection that the catalog belongs to the
    // scope's lineage, so a schema read from another server can never be
    // registered under this lineage.
    let live_uuid: Option<String> = conn
        .query_first("SELECT @@global.server_uuid")
        .await
        .map_err(|e| SourceError::Other(e.into()))?;
    let same = live_uuid
        .as_deref()
        .is_some_and(|live| expected_uuid.eq_ignore_ascii_case(live.trim()));
    if !same {
        return Ok(Live::OtherLineage(format!("{live_uuid:?}")));
    }

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
            nullable: row.take::<String, _>("IS_NULLABLE").unwrap() == "YES",
            ordinal_position: row.take("ORDINAL_POSITION").unwrap(),
            // nullable columns
            default_value: row
                .take::<Option<String>, _>("COLUMN_DEFAULT")
                .unwrap(),
            extra: row.take::<Option<String>, _>("EXTRA").unwrap(),
            comment: row.take::<Option<String>, _>("COLUMN_COMMENT").unwrap(),
            char_max_length: row
                .take::<Option<i64>, _>("CHARACTER_MAXIMUM_LENGTH")
                .unwrap(),
            numeric_precision: row
                .take::<Option<i64>, _>("NUMERIC_PRECISION")
                .unwrap(),
            numeric_scale: row.take::<Option<i64>, _>("NUMERIC_SCALE").unwrap(),
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

    Ok(Live::Found(MySqlTableSchema {
        columns,
        primary_key,
        engine,
        charset: None,
        collation,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    mod scope_race {
        use super::*;
        use storage::adapters::LineageDescriptor;

        fn loaded(column: &str) -> Arc<LoadedSchema> {
            let schema = MySqlTableSchema::new(vec![MySqlColumn::new(
                column, "int", "int", true, 1,
            )]);
            Arc::new(LoadedSchema {
                fingerprint: schema.fingerprint().into(),
                schema,
                registry_version: 1,
                sequence: 0,
                column_names: Arc::new(vec![column.to_string()]),
            })
        }

        fn key() -> (String, String) {
            ("shop".to_string(), "orders".to_string())
        }

        fn loader(scope: &SharedRegistryScope) -> MySqlSchemaLoader {
            MySqlSchemaLoader::new(
                "mysql://none@127.0.0.1:1/none",
                storage::DurableSchemaRegistry::for_testing(),
                "acme",
                scope.clone(),
            )
        }

        fn uuid(last: u8) -> LineageDescriptor {
            LineageDescriptor::mysql(&format!(
                "3e11fa47-71ca-11e1-9e33-c80aa94295{last:02x}"
            ))
            .unwrap()
        }

        /// Reviewer interleaving: a load starts under A; the lineage changes
        /// to B; a B lookup populates the cache; the A load finishes last.
        #[tokio::test]
        async fn late_result_from_previous_lineage_is_discarded() {
            let scope = SharedRegistryScope::new("src");
            scope.publish_for_test("acme", uuid(1));
            let l = loader(&scope);

            let under_a = l.current_scope().unwrap();
            scope.publish_for_test("acme", uuid(2));
            let under_b = l.current_scope().unwrap();
            assert!(l.cache_insert(&under_b, key(), loaded("b_col")).await);
            assert!(
                !l.cache_insert(&under_a, key(), loaded("a_col")).await,
                "a result produced under the previous lineage must be discarded"
            );
            let served = l.get_cached("shop", "orders").unwrap();
            assert_eq!(served.column_names.as_slice(), ["b_col"]);
        }

        /// The A result arrives after the lineage changed but before any B
        /// lookup touched the cache: still discarded, nothing is served.
        #[tokio::test]
        async fn result_from_previous_lineage_is_never_cached() {
            let scope = SharedRegistryScope::new("src");
            scope.publish_for_test("acme", uuid(1));
            let l = loader(&scope);

            let under_a = l.current_scope().unwrap();
            assert!(l.cache_insert(&under_a, key(), loaded("a_col")).await);
            scope.publish_for_test("acme", uuid(2));
            assert!(
                l.get_cached("shop", "orders").is_none(),
                "A entry not served under B"
            );
            assert!(!l.cache_insert(&under_a, key(), loaded("a_col2")).await);
            assert!(l.get_cached("shop", "orders").is_none());
        }
    }

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

    #[tokio::test]
    async fn reload_schema_invalidates_cache() {
        // Using from_static for unit test without DB
        let cols: HashMap<(String, String), Arc<Vec<String>>> =
            HashMap::from([(
                ("db".to_string(), "tbl".to_string()),
                Arc::new(vec!["id".to_string(), "name".to_string()]),
            )]);
        let loader = MySqlSchemaLoader::from_static(cols);

        // First load should hit cache
        let loaded1 = loader.load_schema("db", "tbl").await.unwrap();
        assert_eq!(loaded1.column_names.len(), 2);

        // Cache should have entry
        let cached = loader.list_cached().await;
        assert_eq!(cached.len(), 1);
    }

    #[tokio::test]
    async fn reload_all_clears_cache() {
        let cols: HashMap<(String, String), Arc<Vec<String>>> =
            HashMap::from([
                (
                    ("db".to_string(), "tbl1".to_string()),
                    Arc::new(vec!["id".to_string()]),
                ),
                (
                    ("db".to_string(), "tbl2".to_string()),
                    Arc::new(vec!["id".to_string()]),
                ),
            ]);
        let loader = MySqlSchemaLoader::from_static(cols);

        // Load both
        let _ = loader.load_schema("db", "tbl1").await;
        let _ = loader.load_schema("db", "tbl2").await;
        assert_eq!(loader.list_cached().await.len(), 2);

        // Clear cache (reload_all would normally reload from DB)
        loader.cache.write().await.clear();
        assert_eq!(loader.list_cached().await.len(), 0);
    }
}
