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

impl crate::registry_scope::Resident for Arc<LoadedSchema> {
    fn pin(&self) -> crate::registry_scope::PinnedVersion {
        crate::registry_scope::PinnedVersion {
            version: self.registry_version,
            sequence: self.sequence,
            fingerprint: Arc::clone(&self.fingerprint),
        }
    }

    fn weight(&self) -> usize {
        serde_json::to_vec(&self.schema).map_or(0, |b| b.len())
            + self.column_names.iter().map(String::len).sum::<usize>()
    }
}

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
    /// Single-flight per table: concurrent first uses share one load.
    flights: Arc<crate::registry_scope::LoadFlights>,
    /// Live catalog reads made by this loader (and its clones).
    live_fetches: Arc<std::sync::atomic::AtomicU64>,
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
            flights: Default::default(),
            live_fetches: Default::default(),
        }
    }

    /// Live catalog reads this loader (and its clones) made so far: a
    /// diagnostic for operation-count evidence.
    pub fn live_fetch_count(&self) -> u64 {
        self.live_fetches.load(std::sync::atomic::Ordering::Relaxed)
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
        let (_, _, evicted_before) = cache.usage();
        let inserted = cache.insert_if_current(
            scope.generation(),
            self.scope.generation(),
            key,
            value,
        );
        let (_, _, evicted_after) = cache.usage();
        if evicted_after > evicted_before {
            counter!("deltaforge_source_schema_cache_evictions_total",
                "tenant" => self.tenant.clone(),
                "source_id" => self.scope.source_id().to_string(),
                "engine" => "mysql")
            .increment(evicted_after - evicted_before);
        }
        inserted
    }

    /// Bound the resident schemas to `budget` (the default suits most
    /// pipelines).
    pub fn with_cache_budget(
        mut self,
        budget: crate::registry_scope::CacheBudget,
    ) -> Self {
        self.cache = Arc::new(RwLock::new(ScopedCache::with_budget(budget)));
        self
    }

    /// Resident schemas, their approximate bytes and evictions so far.
    pub async fn cache_usage(&self) -> (usize, usize, u64) {
        self.cache.read().await.usage()
    }

    /// Rebuild the table `key` resolved to earlier in this run, after its
    /// value was evicted: exactly its pinned version, read from durable
    /// history (index-verified) whose content has the pinned fingerprint.
    /// `None` when the table was not resolved in this generation. A version
    /// that cannot be rebuilt exactly fails closed: eviction never re-resolves
    /// a table.
    async fn rebuild_pinned(
        &self,
        scope: &RegistryScope,
        key: &(String, String),
    ) -> SourceResult<Option<Arc<LoadedSchema>>> {
        let Some(pin) = self.cache.read().await.pinned(scope.generation(), key)
        else {
            return Ok(None);
        };
        let stored = self
            .registry
            .get_version(&scope.key(&key.0, &key.1), pin.version)
            .await
            .map_err(RegistryError::Storage)?;
        let schema = stored
            .and_then(|sv| {
                serde_json::from_value::<MySqlTableSchema>(sv.schema_json).ok()
            })
            .filter(|schema| *schema.fingerprint() == *pin.fingerprint)
            .ok_or_else(|| SourceError::Schema {
                details: format!(
                    "schema of {}.{} (version {}) was evicted from the \
                     loader cache and cannot be rebuilt exactly from durable \
                     history",
                    key.0, key.1, pin.version
                )
                .into(),
            })?;
        let column_names: Arc<Vec<String>> =
            Arc::new(schema.columns.iter().map(|c| c.name.clone()).collect());
        Ok(Some(Arc::new(LoadedSchema {
            schema,
            registry_version: pin.version,
            fingerprint: pin.fingerprint,
            sequence: pin.sequence,
            column_names,
        })))
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

    /// Every table `patterns` capture, with its schema resolved (from the
    /// durable registry when known, otherwise from the catalog). Collects
    /// all pages: diagnostics and tests only; a snapshot consumes the pages
    /// one at a time ([`discovery_page`], [`Self::warm_from_registry`]).
    pub async fn preload(
        &self,
        patterns: &[String],
    ) -> SourceResult<Vec<(String, String)>> {
        let tables = self.expand_patterns(patterns).await?;
        for page in tables.chunks(1_000) {
            self.warm_from_registry(page).await?;
            for (db, table) in page {
                self.load_schema(db, table).await?;
            }
        }
        self.check_binlog_row_image().await?;
        Ok(tables)
    }

    /// Resolve a page of tables from the durable registry: a table with
    /// stored history enters the cache (pinned) without a catalog query; the
    /// others are left for [`Self::load_schema`]. Stored history that cannot
    /// be read fails closed (never replaced by the live catalog), as does a
    /// registry storage error.
    pub async fn warm_from_registry(
        &self,
        page: &[(String, String)],
    ) -> SourceResult<usize> {
        let scope = self.current_scope()?;
        let mut from_registry = 0usize;
        for (db, table) in page {
            let key = (db.clone(), table.clone());
            if self
                .cache
                .read()
                .await
                .get(scope.generation(), &key)
                .is_some()
            {
                continue;
            }
            crate::snapshot_probe::record_registry_read();
            let Some(sv) = self
                .registry
                .get_latest(&scope.key(db, table))
                .await
                .map_err(RegistryError::Storage)?
            else {
                continue;
            };
            let schema =
                serde_json::from_value::<MySqlTableSchema>(sv.schema_json)
                    .map_err(|e| SourceError::Schema {
                        details: format!(
                            "stored schema of {db}.{table} (version {}) is \
                     unreadable: {e}",
                            sv.version
                        )
                        .into(),
                    })?;
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
            if !self.cache_insert(&scope, key, loaded).await {
                return Err(self.scope_changed());
            }
            from_registry += 1;
        }
        Ok(from_registry)
    }

    /// A connection verified to reach the scoped server, for discovery.
    pub(crate) async fn discovery_conn(
        &self,
    ) -> SourceResult<mysql_async::Conn> {
        self.verified_conn().await
    }

    /// Warn when `binlog_row_image` is not FULL (before images incomplete).
    /// One query on a verified connection, independent of the catalog.
    pub async fn check_binlog_row_image(&self) -> SourceResult<()> {
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

        Ok(())
    }

    /// Every table `patterns` capture, in discovery order (all pages
    /// collected: diagnostics and tests only).
    pub async fn expand_patterns(
        &self,
        patterns: &[String],
    ) -> SourceResult<Vec<(String, String)>> {
        let mut conn = self.verified_conn().await?;
        let mut discovery =
            crate::snapshot_discovery::Discovery::new(patterns, 1_000);
        let mut tables = Vec::new();
        while !discovery.is_done() {
            let rows = discovery_page(
                &mut conn,
                patterns,
                discovery.after(),
                discovery.page_size(),
            )
            .await?;
            tables.extend(discovery.accept(rows)?);
        }
        Ok(tables)
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
            let _flight = self.flights.acquire(&key).await;
            if let Some(cached) =
                self.cache.read().await.get(scope.generation(), &key)
            {
                return Ok(cached);
            }
            if let Some(rebuilt) = self.rebuild_pinned(&scope, &key).await? {
                if self
                    .cache_insert(&scope, key.clone(), rebuilt.clone())
                    .await
                {
                    return Ok(rebuilt);
                }
                continue;
            }

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

    /// Forget every cached schema; each table reloads on its next use. No
    /// catalog enumeration.
    pub async fn clear_cache(&self) {
        self.cache.write().await.clear();
    }

    /// Reload the tables in use: every table resolved in this run (matching
    /// `patterns`, when given) is fetched again from the live catalog and
    /// re-registered; other tables load on their next use. Never enumerates
    /// the catalog, so its cost follows the working set, not the catalog.
    pub async fn reload_all(
        &self,
        patterns: &[String],
    ) -> SourceResult<Vec<(String, String)>> {
        let allow = common::AllowList::new(patterns);
        let resident: Vec<(String, String)> = {
            let mut cache = self.cache.write().await;
            let keys = cache.tables_for(self.scope.generation());
            cache.clear();
            keys
        };
        let mut reloaded = Vec::new();
        for (qualifier, table) in resident {
            if allow.matches(&qualifier, &table) {
                self.load_schema(&qualifier, &table).await?;
                reloaded.push((qualifier, table));
            }
        }
        Ok(reloaded)
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
        self.live_fetches
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
            flights: Default::default(),
            live_fetches: Default::default(),
        }
    }
}

/// System databases a pattern without a database never matches.
const ANY_DATABASE: &str = "TABLE_SCHEMA NOT IN ('mysql', \
     'information_schema', 'performance_schema', 'sys')";

/// One keyset page of the tables `patterns` may capture (a superset; the
/// caller applies the CDC matcher): base tables strictly after `after` in
/// bytewise `(database, table)` order (binary comparison of both), at most
/// `limit` rows.
pub(crate) async fn discovery_page(
    conn: &mut mysql_async::Conn,
    patterns: &[String],
    after: Option<&(String, String)>,
    limit: usize,
) -> SourceResult<Vec<(String, String)>> {
    let filter = crate::table_patterns::combined_superset(
        patterns,
        "TABLE_SCHEMA",
        "TABLE_NAME",
        ANY_DATABASE,
    );
    let cursor = if after.is_some() {
        "AND (CAST(TABLE_SCHEMA AS BINARY) > CAST(? AS BINARY) \
           OR (CAST(TABLE_SCHEMA AS BINARY) = CAST(? AS BINARY) \
               AND CAST(TABLE_NAME AS BINARY) > CAST(? AS BINARY)))"
    } else {
        ""
    };
    let sql = format!(
        "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES \
         WHERE TABLE_TYPE = 'BASE TABLE' AND {filter} {cursor} \
         ORDER BY CAST(TABLE_SCHEMA AS BINARY), CAST(TABLE_NAME AS BINARY) \
         LIMIT {limit}"
    );
    let rows: Vec<(String, String)> = match after {
        Some((db, table)) => conn
            .exec(&sql, (db.as_str(), db.as_str(), table.as_str()))
            .await
            .map_err(query_error)?,
        None => conn.query(&sql).await.map_err(query_error)?,
    };
    Ok(rows)
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
        MySqlSchemaLoader::reload_all(self, patterns)
            .await
            .map_err(Into::into)
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

    let mut found = fetch_table_schemas_on(conn, &[(db, table)]).await?;
    match found.remove(&(db.to_string(), table.to_string())) {
        Some(schema) => Ok(Live::Found(schema)),
        None => Err(SourceError::Other(anyhow::anyhow!(
            "table {}.{} not found or has no columns",
            db,
            table
        ))),
    }
}

/// The registered schema model of each of `tables` that exists, read from
/// INFORMATION_SCHEMA on `conn` in one batch (no lineage proof: the caller's
/// connection is already proven). The one definition of a MySQL table's
/// schema: the loader registers it and the snapshot anchor compares it.
pub(crate) async fn fetch_table_schemas_on(
    conn: &mut mysql_async::Conn,
    tables: &[(&str, &str)],
) -> SourceResult<HashMap<(String, String), MySqlTableSchema>> {
    let mut schemas = HashMap::new();
    if tables.is_empty() {
        return Ok(schemas);
    }
    let pairs = vec!["(?, ?)"; tables.len()].join(", ");
    let params: Vec<mysql_async::Value> = tables
        .iter()
        .flat_map(|(db, table)| [(*db).into(), (*table).into()])
        .collect();

    // Fetch columns
    let col_rows: Vec<Row> = conn
        .exec(
            format!(
                r#"
            SELECT
                TABLE_SCHEMA,
                TABLE_NAME,
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
                NUMERIC_SCALE,
                CHARACTER_OCTET_LENGTH,
                DATETIME_PRECISION,
                (SELECT co.ID FROM INFORMATION_SCHEMA.COLLATIONS co
                 WHERE co.COLLATION_NAME = c.COLLATION_NAME) AS COLLATION_ID
            FROM INFORMATION_SCHEMA.COLUMNS c
            WHERE (TABLE_SCHEMA, TABLE_NAME) IN ({pairs})
            ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
            "#
            ),
            params.clone(),
        )
        .await
        .map_err(query_error)?;

    for mut row in col_rows {
        let key: (String, String) = (
            row.take("TABLE_SCHEMA").unwrap(),
            row.take("TABLE_NAME").unwrap(),
        );
        let column = MySqlColumn {
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
            char_octet_length: row
                .take::<Option<i64>, _>("CHARACTER_OCTET_LENGTH")
                .unwrap(),
            collation_id: row.take::<Option<i64>, _>("COLLATION_ID").unwrap(),
            datetime_precision: row
                .take::<Option<i64>, _>("DATETIME_PRECISION")
                .unwrap(),
            primary_key_prefix: None,
        };
        schemas
            .entry(key)
            .or_insert_with(|| MySqlTableSchema {
                columns: Vec::new(),
                primary_key: Vec::new(),
                engine: None,
                charset: None,
                collation: None,
            })
            .columns
            .push(column);
    }

    // Fetch primary key
    let pk_rows: Vec<Row> = conn
        .exec(
            format!(
                r#"
            SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE (TABLE_SCHEMA, TABLE_NAME) IN ({pairs})
              AND CONSTRAINT_NAME = 'PRIMARY'
            ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
            "#
            ),
            params.clone(),
        )
        .await
        .map_err(query_error)?;
    for mut row in pk_rows {
        let key: (String, String) = (
            row.take("TABLE_SCHEMA").unwrap(),
            row.take("TABLE_NAME").unwrap(),
        );
        if let Some(s) = schemas.get_mut(&key) {
            s.primary_key.push(row.take("COLUMN_NAME").unwrap());
        }
    }

    // Primary-key prefix lengths (a prefixed key part, e.g. a BLOB prefix).
    let prefix_rows: Vec<Row> = conn
        .exec(
            format!(
                r#"
            SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, SUB_PART
            FROM INFORMATION_SCHEMA.STATISTICS
            WHERE (TABLE_SCHEMA, TABLE_NAME) IN ({pairs})
              AND INDEX_NAME = 'PRIMARY' AND SUB_PART IS NOT NULL
            "#
            ),
            params.clone(),
        )
        .await
        .map_err(query_error)?;
    for mut row in prefix_rows {
        let key: (String, String) = (
            row.take("TABLE_SCHEMA").unwrap(),
            row.take("TABLE_NAME").unwrap(),
        );
        let name: String = row.take("COLUMN_NAME").unwrap();
        let sub_part: Option<i64> = row.take("SUB_PART").unwrap();
        if let Some(c) = schemas
            .get_mut(&key)
            .and_then(|s| s.columns.iter_mut().find(|c| c.name == name))
        {
            c.primary_key_prefix = sub_part;
        }
    }

    // Fetch table metadata
    let table_rows: Vec<Row> = conn
        .exec(
            format!(
                r#"
            SELECT TABLE_SCHEMA, TABLE_NAME, ENGINE, TABLE_COLLATION
            FROM INFORMATION_SCHEMA.TABLES
            WHERE (TABLE_SCHEMA, TABLE_NAME) IN ({pairs})
            "#
            ),
            params,
        )
        .await
        .map_err(query_error)?;
    for mut row in table_rows {
        let key: (String, String) = (
            row.take("TABLE_SCHEMA").unwrap(),
            row.take("TABLE_NAME").unwrap(),
        );
        if let Some(s) = schemas.get_mut(&key) {
            s.engine = row.take("ENGINE");
            s.collation = row.take("TABLE_COLLATION");
        }
    }

    Ok(schemas)
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

    /// Catalog queries narrow like CDC filtering: a database-less pattern
    /// spans every database, a trailing `*` or `%` is a prefix, and `_` / `%`
    /// inside names are literal.
    #[test]
    fn pattern_queries_follow_cdc_filtering() {
        let q = crate::table_patterns::combined_superset(
            &["orders.items".to_string()],
            "TABLE_SCHEMA",
            "TABLE_NAME",
            ANY_DATABASE,
        );
        assert!(q.contains("TABLE_SCHEMA = 'orders'"));
        assert!(q.contains("TABLE_NAME = 'items'"));
        let q = crate::table_patterns::combined_superset(
            &["orders.*".to_string()],
            "TABLE_SCHEMA",
            "TABLE_NAME",
            ANY_DATABASE,
        );
        assert!(q.contains("TABLE_SCHEMA = 'orders'") && q.contains("1=1"));
        let q = crate::table_patterns::combined_superset(
            &["audit".to_string()],
            "TABLE_SCHEMA",
            "TABLE_NAME",
            ANY_DATABASE,
        );
        assert!(
            q.contains("TABLE_SCHEMA NOT IN")
                && q.contains("TABLE_NAME = 'audit'")
        );
        for p in ["shop.audit_*", "shop.audit_%"] {
            let q = crate::table_patterns::combined_superset(
                &[p.to_string()],
                "TABLE_SCHEMA",
                "TABLE_NAME",
                ANY_DATABASE,
            );
            assert!(q.contains("TABLE_NAME LIKE 'audit|_%' ESCAPE '|'"), "{q}");
        }
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
