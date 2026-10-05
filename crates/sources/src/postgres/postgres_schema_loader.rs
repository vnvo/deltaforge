//! PostgreSQL schema loader with wildcard expansion and registry integration.
//!
//! Provides schema preloading at startup with support for:
//! - Wildcard table patterns (e.g., `public.*`, `%.audit_log`)
//! - Full schema loading from information_schema
//! - Schema registry integration with fingerprinting
//! - On-demand reload capability

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use metrics::counter;
use schema_registry::SourceSchema;
use storage::DurableSchemaRegistry;
use storage::adapters::{LineageDescriptor, SchemaKey};
use tokio::sync::RwLock;
use tokio_postgres::NoTls;
use tracing::{debug, info, warn};

use deltaforge_core::{SourceError, SourceResult};

use super::postgres_helpers::redact_password;
use super::postgres_table_schema::{
    PostgresColumn, PostgresTableSchema, RelationIdentity,
};
use crate::registry_scope::{
    RegistryError, RegistryScope, ScopedCache, SharedRegistryScope,
};
use crate::schema_loader::{
    LoadedSchema as ApiLoadedSchema, SchemaListEntry, SourceSchemaLoader,
};

/// Page size for durable history scans (memory is O(page), never O(history)).
const HISTORY_PAGE: usize = 256;

/// Attempts at a load whose lineage keeps moving before giving up with the
/// typed, retryable [`RegistryError::ScopeChanged`].
const SCOPE_ATTEMPTS: usize = 3;

/// The live catalog's answer for one table.
enum Live {
    Found(PostgresTableSchema),
    Missing,
    /// The catalog is not the scope's lineage (described for the error).
    OtherLineage(String),
}

/// Loaded schema with metadata.
///
/// Wrapped fields use `Arc` to avoid deep-cloning per event on cache hits.
#[derive(Debug, Clone)]
pub struct LoadedSchema {
    pub schema: Arc<PostgresTableSchema>,
    pub registry_version: i32,
    pub fingerprint: Arc<str>,
    pub sequence: u64,
    pub column_names: Arc<Vec<String>>,
}

impl crate::registry_scope::Resident for LoadedSchema {
    fn pin(&self) -> crate::registry_scope::PinnedVersion {
        crate::registry_scope::PinnedVersion {
            version: self.registry_version,
            sequence: self.sequence,
            fingerprint: Arc::clone(&self.fingerprint),
        }
    }

    fn weight(&self) -> usize {
        serde_json::to_vec(&*self.schema).map_or(0, |b| b.len())
            + self.column_names.iter().map(String::len).sum::<usize>()
    }
}

/// Schema loader with caching and registry integration.
///
/// Every registry access is qualified by the source's verified lineage, taken
/// from the shared [`SharedRegistryScope`]; with no established scope, registry
/// access fails closed. Cache entries belong to one scope generation and are
/// discarded when the source moves to a new lineage.
#[derive(Clone)]
pub struct PostgresSchemaLoader {
    dsn: crate::credentials::ProtectedDsn,
    cache: Arc<RwLock<ScopedCache<LoadedSchema>>>,
    registry: Arc<DurableSchemaRegistry>,
    scope: SharedRegistryScope,
    tenant: String,
    /// Single-flight per table: concurrent first uses share one load.
    flights: Arc<crate::registry_scope::LoadFlights>,
    /// Live catalog reads made by this loader (and its clones).
    live_fetches: Arc<std::sync::atomic::AtomicU64>,
}

impl std::fmt::Debug for PostgresSchemaLoader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresSchemaLoader")
            .field("dsn", &self.dsn)
            .field("tenant", &self.tenant)
            .finish_non_exhaustive()
    }
}

impl PostgresSchemaLoader {
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
            "creating postgres schema loader for {}",
            redact_password(dsn.expose())
        );
        Self {
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
        value: LoadedSchema,
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
                "engine" => "postgres")
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
    ) -> SourceResult<Option<LoadedSchema>> {
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
                serde_json::from_value::<PostgresTableSchema>(sv.schema_json)
                    .ok()
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
        Ok(Some(LoadedSchema {
            schema: Arc::new(schema),
            registry_version: pin.version,
            fingerprint: pin.fingerprint,
            sequence: pin.sequence,
            column_names,
        }))
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

    /// The live catalog answered from a different lineage than `scope`. If the
    /// published scope has moved on, retry under it; otherwise the database
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

    /// Replace the connection DSN after a credential rotation. The loader shares
    /// the protected wrapper; the cache (shared via `Arc`) is preserved, and only
    /// subsequent connections use the new credentials. Called by the run loop at a
    /// quiesced boundary once the replacement stream is confirmed, so no schema
    /// query is in flight against the old DSN.
    pub(crate) fn set_dsn(&mut self, dsn: crate::credentials::ProtectedDsn) {
        self.dsn = dsn;
    }

    /// Get a database connection.
    async fn connect(&self) -> SourceResult<tokio_postgres::Client> {
        let (client, conn) = tokio_postgres::connect(self.dsn.expose(), NoTls)
            .await
            .map_err(|e| SourceError::Connect {
                details: format!("postgres connect: {}", e).into(),
            })?;

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
            for (schema, table) in page {
                self.load_schema(schema, table).await?;
            }
        }
        Ok(tables)
    }

    /// Every table `patterns` capture, in discovery order (all pages
    /// collected: diagnostics and tests only).
    pub async fn expand_patterns(
        &self,
        patterns: &[String],
    ) -> SourceResult<Vec<(String, String)>> {
        let client = self.connect().await?;
        client
            .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await
            .map_err(query_error)?;
        let mut discovery =
            crate::snapshot_discovery::Discovery::new(patterns, 1_000);
        let mut tables = Vec::new();
        while !discovery.is_done() {
            let rows = discovery_page(
                &client,
                patterns,
                discovery.after(),
                discovery.page_size(),
            )
            .await?;
            tables.extend(discovery.accept(rows)?);
        }
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
        for (schema, table) in page {
            let key = (schema.clone(), table.clone());
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
                .get_latest(&scope.key(schema, table))
                .await
                .map_err(RegistryError::Storage)?
            else {
                continue;
            };
            let pg_schema =
                serde_json::from_value::<PostgresTableSchema>(sv.schema_json)
                    .map_err(|e| SourceError::Schema {
                    details: format!(
                        "stored schema of {schema}.{table} (version {}) is \
                     unreadable: {e}",
                        sv.version
                    )
                    .into(),
                })?;
            let fingerprint = pg_schema.fingerprint();
            let column_names = Arc::new(
                pg_schema
                    .columns
                    .iter()
                    .map(|c| c.name.clone())
                    .collect::<Vec<_>>(),
            );
            let loaded = LoadedSchema {
                schema: Arc::new(pg_schema),
                registry_version: sv.version,
                fingerprint: fingerprint.into(),
                sequence: sv.sequence,
                column_names,
            };
            if !self.cache_insert(&scope, key, loaded).await {
                return Err(self.scope_changed());
            }
            from_registry += 1;
        }
        Ok(from_registry)
    }

    /// Resolve `schema.table` from the catalog as `client`'s session sees it
    /// (the snapshot preparation's repeatable-read session), register it and
    /// cache it. Never answered from the cache or another connection, so a
    /// plan prepared on one session uses one coherent catalog view.
    pub(crate) async fn load_schema_on(
        &self,
        client: &tokio_postgres::Client,
        schema: &str,
        table: &str,
    ) -> SourceResult<LoadedSchema> {
        let key = (schema.to_string(), table.to_string());
        for _ in 0..SCOPE_ATTEMPTS {
            let scope = self.current_scope()?;
            let pg_schema = match self
                .fetch_live_on(client, &scope, schema, table)
                .await?
            {
                Live::Found(s) => s,
                Live::Missing => {
                    return Err(SourceError::Schema {
                        details: format!("table {schema}.{table} not found")
                            .into(),
                    });
                }
                Live::OtherLineage(live) => {
                    self.lineage_moved(&scope, live)?;
                    continue;
                }
            };
            crate::snapshot_probe::record_registry_read();
            let loaded = self
                .register(&scope, schema, table, pg_schema, None)
                .await?;
            if self.cache_insert(&scope, key.clone(), loaded.clone()).await {
                return Ok(loaded);
            }
        }
        Err(self.scope_changed())
    }

    /// Load full schema for a table.
    pub async fn load_schema(
        &self,
        schema: &str,
        table: &str,
    ) -> SourceResult<LoadedSchema> {
        self.load_schema_at_checkpoint(schema, table, None).await
    }

    /// Load schema with optional checkpoint for registry correlation.
    ///
    /// The schema is fetched from the live catalog on a connection that proves
    /// it belongs to the scope's lineage, registered under that scope, and
    /// cached only if the scope is still the published one; otherwise the work
    /// is discarded and retried under the new scope (bounded).
    pub async fn load_schema_at_checkpoint(
        &self,
        schema: &str,
        table: &str,
        checkpoint: Option<&[u8]>,
    ) -> SourceResult<LoadedSchema> {
        let key = (schema.to_string(), table.to_string());
        for _ in 0..SCOPE_ATTEMPTS {
            let scope = self.current_scope()?;
            if let Some(cached) =
                self.cache.read().await.get(scope.generation(), &key)
            {
                debug!(schema = %schema, table = %table, "schema cache hit");
                counter!("deltaforge_source_schema_cache_hits_total",
                    "pipeline" => self.tenant.clone(), "source" => "postgres")
                .increment(1);
                return Ok(cached);
            }
            counter!("deltaforge_source_schema_cache_misses_total",
                "pipeline" => self.tenant.clone(), "source" => "postgres")
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
            let pg_schema = match self.fetch_live(&scope, schema, table).await?
            {
                Live::Found(s) => s,
                Live::Missing => {
                    return Err(SourceError::Schema {
                        details: format!("table {schema}.{table} not found")
                            .into(),
                    });
                }
                Live::OtherLineage(live) => {
                    self.lineage_moved(&scope, live)?;
                    continue;
                }
            };
            let loaded = self
                .register(&scope, schema, table, pg_schema, checkpoint)
                .await?;
            if !self.cache_insert(&scope, key.clone(), loaded.clone()).await {
                continue;
            }

            let elapsed = t0.elapsed();
            if elapsed.as_millis() > 200 {
                warn!(schema = %schema, table = %table, ms = elapsed.as_millis(), "slow schema load");
            } else {
                debug!(schema = %schema, table = %table, version = loaded.registry_version, ms = elapsed.as_millis(), "schema loaded");
            }
            return Ok(loaded);
        }
        Err(self.scope_changed())
    }

    /// Register a freshly fetched schema under `scope` (not cached here).
    async fn register(
        &self,
        scope: &RegistryScope,
        schema: &str,
        table: &str,
        pg_schema: PostgresTableSchema,
        checkpoint: Option<&[u8]>,
    ) -> SourceResult<LoadedSchema> {
        let fingerprint = pg_schema.fingerprint();
        let column_names: Arc<Vec<String>> = Arc::new(
            pg_schema.columns.iter().map(|c| c.name.clone()).collect(),
        );
        let schema_json = serde_json::to_value(&pg_schema)
            .map_err(|e| SourceError::Other(e.into()))?;
        let version = self
            .registry
            .register_with_checkpoint(
                &scope.key(schema, table),
                &fingerprint,
                &schema_json,
                checkpoint,
            )
            .await
            .map_err(RegistryError::Storage)?;
        Ok(LoadedSchema {
            schema: Arc::new(pg_schema),
            registry_version: version,
            fingerprint: fingerprint.into(),
            sequence: self.registry.current_sequence(),
            column_names,
        })
    }

    /// Load a table's schema for decoding a pgoutput row, using the durable historical
    /// schema when the live table no longer exists.
    ///
    /// Resolution order (each attempt under one captured scope, cached only if
    /// that scope is still published - otherwise retried):
    /// 1. cache hit for THIS relation -> return it;
    /// 2. live catalog (proven to be the scope's lineage) has this relation ->
    ///    register and cache it (normal path);
    /// 3. otherwise recover from this source's lineage-scoped history via
    ///    [`resolve_retained_relation`], which fails closed on zero or several
    ///    matches and never adopts unproven pre-upgrade history.
    pub(crate) async fn load_schema_for_relation(
        &self,
        schema: &str,
        table: &str,
        checkpoint: Option<&[u8]>,
        rel: &RelationIdentity,
    ) -> SourceResult<LoadedSchema> {
        let key = (schema.to_string(), table.to_string());
        for _ in 0..SCOPE_ATTEMPTS {
            let scope = self.current_scope()?;
            // 1. Cached schema, ONLY if it is THIS relation's schema. A cache entry
            //    for the same name but a different relation (e.g. a recreated
            //    table) must not be used.
            if let Some(cached) =
                self.cache.read().await.get(scope.generation(), &key)
                && cached.schema.matches_relation(rel)
            {
                return Ok(cached);
            }
            let _flight = self.flights.acquire(&key).await;
            if let Some(cached) =
                self.cache.read().await.get(scope.generation(), &key)
                && cached.schema.matches_relation(rel)
            {
                return Ok(cached);
            }
            // An evicted table is rebuilt exactly; only if the relation no
            // longer matches it (a changed or recreated table) is it resolved
            // again below.
            if let Some(rebuilt) = self.rebuild_pinned(&scope, &key).await?
                && rebuilt.schema.matches_relation(rel)
            {
                if self
                    .cache_insert(&scope, key.clone(), rebuilt.clone())
                    .await
                {
                    return Ok(rebuilt);
                }
                continue;
            }

            // 2. Live catalog, ONLY if it matches this relation identity. A live
            //    table whose OID/signature/replica differs from the retained
            //    relation (dropped and recreated under the same name) must NOT be
            //    used to decode the retained WAL; fall through to history.
            let loaded = match self.fetch_live(&scope, schema, table).await? {
                Live::OtherLineage(live) => {
                    self.lineage_moved(&scope, live)?;
                    continue;
                }
                Live::Found(pg_schema) if pg_schema.matches_relation(rel) => {
                    self.register(&scope, schema, table, pg_schema, checkpoint)
                        .await?
                }
                Live::Found(_) | Live::Missing => {
                    // 3. Durable history under this source's verified lineage.
                    let (version, sequence, pg_schema) =
                        resolve_retained_relation(
                            &self.registry,
                            &scope.key(schema, table),
                            rel,
                        )
                        .await?;
                    info!(
                        schema = %schema, table = %table, version,
                        relation_oid = rel.oid,
                        "decoding retained WAL for dropped table from durable schema"
                    );
                    let fingerprint = pg_schema.fingerprint();
                    let column_names: Arc<Vec<String>> = Arc::new(
                        pg_schema
                            .columns
                            .iter()
                            .map(|c| c.name.clone())
                            .collect(),
                    );
                    LoadedSchema {
                        schema: Arc::new(pg_schema),
                        registry_version: version,
                        fingerprint: fingerprint.into(),
                        sequence,
                        column_names,
                    }
                }
            };
            if self.cache_insert(&scope, key.clone(), loaded.clone()).await {
                return Ok(loaded);
            }
        }
        Err(self.scope_changed())
    }

    /// Force reload schema from database (bypasses cache).
    pub async fn reload_schema(
        &self,
        schema: &str,
        table: &str,
    ) -> SourceResult<LoadedSchema> {
        self.cache
            .write()
            .await
            .remove(&(schema.to_string(), table.to_string()));
        self.load_schema(schema, table).await
    }

    /// The durably persisted latest schema of `schema.table` under the
    /// current lineage, read through the registry (single-flight per key),
    /// never from this loader's cache: `None` = never registered (first use);
    /// unreadable stored history fails closed.
    pub(crate) async fn persisted(
        &self,
        schema: &str,
        table: &str,
    ) -> SourceResult<Option<PostgresTableSchema>> {
        let scope = self.current_scope()?;
        let Some(sv) = self
            .registry
            .get_latest(&scope.key(schema, table))
            .await
            .map_err(RegistryError::Storage)?
        else {
            return Ok(None);
        };
        serde_json::from_value::<PostgresTableSchema>(sv.schema_json)
            .map(Some)
            .map_err(|e| SourceError::Schema {
                details: format!(
                    "stored schema of {schema}.{table} (version {}) is \
                     unreadable: {e}",
                    sv.version
                )
                .into(),
            })
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
        schema: &str,
        table: &str,
    ) -> Option<LoadedSchema> {
        let published = self.scope.generation();
        self.cache.try_read().ok().and_then(|c| {
            c.get(published, &(schema.to_string(), table.to_string()))
        })
    }

    /// Fetch the live schema of `schema_name.table_name`, proving on the SAME
    /// connection that the catalog belongs to `scope`'s lineage, so a schema read
    /// from another server can never be registered under this lineage.
    async fn fetch_live(
        &self,
        scope: &RegistryScope,
        schema_name: &str,
        table_name: &str,
    ) -> SourceResult<Live> {
        let client = self.connect().await?;
        self.fetch_live_on(&client, scope, schema_name, table_name)
            .await
    }

    /// [`Self::fetch_live`] on a given session (every query on `client`).
    async fn fetch_live_on(
        &self,
        client: &tokio_postgres::Client,
        scope: &RegistryScope,
        schema_name: &str,
        table_name: &str,
    ) -> SourceResult<Live> {
        self.live_fetches
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let live = client
            .query_opt(
                "SELECT s.system_identifier, d.oid::int8 \
                 FROM pg_control_system() s, pg_database d \
                 WHERE d.datname = current_database()",
                &[],
            )
            .await
            .map_err(query_error)?
            .map(|r| (r.get::<_, i64>(0) as u64, r.get::<_, i64>(1) as u64));
        let same = matches!(
            (&scope.lineage().descriptor, live),
            (
                LineageDescriptor::Postgres { system_identifier, database_oid },
                Some((sysid, dboid)),
            ) if *system_identifier == sysid && *database_oid == dboid
        );
        if !same {
            return Ok(Live::OtherLineage(format!("{live:?}")));
        }

        let mut found =
            fetch_tables_on(client, &[(schema_name, table_name)]).await?;
        Ok(
            match found
                .remove(&(schema_name.to_string(), table_name.to_string()))
            {
                Some(schema) => Live::Found(schema),
                None => Live::Missing,
            },
        )
    }

    /// Get column names only (for backward compatibility).
    pub async fn column_names(
        &self,
        schema: &str,
        table: &str,
    ) -> SourceResult<Arc<Vec<String>>> {
        Ok(Arc::clone(
            &self.load_schema(schema, table).await?.column_names,
        ))
    }

    /// Returns all (db, table) pairs currently in cache.
    /// Sync, for failover reconciliation — avoids async where not needed.
    /// Deliberately ignores the scope generation: right after a lineage change
    /// these are the tables that were tracked under the previous lineage, which
    /// is exactly the set failover reconciliation must diff.
    pub fn cached_tables(&self) -> Vec<(String, String)> {
        self.cache
            .try_read()
            .map(|c| c.keys_any_generation())
            .unwrap_or_default()
    }
}

/// Resolve the schema of a retained pgoutput relation from durable history.
///
/// Only this source's lineage-scoped history can supply the schema: exactly one
/// version matching the full relation identity (OID + ordered
/// `(name, type_oid)` + replica identity) is used; several are ambiguous.
///
/// Pre-upgrade (unscoped) history is never adopted here. Its flat key carries no
/// source or database lineage, and PostgreSQL relation OIDs are database-local,
/// so a matching OID and column shape cannot prove that the stream belongs to
/// this database. When the scoped history has no match, the legacy history is
/// only inspected to fail closed with an actionable error
/// ([`RegistryError::LegacyOwnershipUnproven`]) if it holds candidates;
/// adopting them requires the explicit operator mapping of the migration
/// command. Nothing is written by this function.
///
/// Memory is O(page): only the first match is held while counting.
pub(crate) async fn resolve_retained_relation(
    registry: &DurableSchemaRegistry,
    key: &SchemaKey,
    rel: &RelationIdentity,
) -> Result<(i32, u64, PostgresTableSchema), RegistryError> {
    let table = format!("{}.{}", key.db, key.table);
    let relation = format!(
        "oid={}, replica identity '{}'",
        rel.oid, rel.replica_identity
    );
    let mut seen_any = false;

    let mut found: Option<(i32, u64, PostgresTableSchema)> = None;
    let mut matches = 0usize;
    let mut cursor = None;
    loop {
        let page = registry
            .history_page(key, cursor, HISTORY_PAGE)
            .await
            .map_err(RegistryError::Storage)?;
        for sv in page.versions {
            seen_any = true;
            if let Ok(s) =
                serde_json::from_value::<PostgresTableSchema>(sv.schema_json)
                && s.matches_relation(rel)
            {
                matches += 1;
                if found.is_none() {
                    found = Some((sv.version, sv.sequence, s));
                }
            }
        }
        match page.next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    match (matches, found) {
        (1, Some(hit)) => return Ok(hit),
        (0, _) => {}
        (n, _) => {
            return Err(RegistryError::AmbiguousHistory {
                table,
                relation,
                matches: n,
            });
        }
    }

    if !registry.migration_enabled() {
        return Err(no_match(seen_any, table, relation));
    }

    // A `/` inside any segment makes the pre-upgrade flat key ambiguous, so it
    // is never read; its existence (a conservative count) still blocks
    // decoding, since unmapped pre-upgrade history might hold this relation.
    if [&key.tenant, &key.db, &key.table]
        .iter()
        .any(|s| s.contains('/'))
    {
        let legacy = registry
            .legacy_len(&key.tenant, &key.db, &key.table)
            .await
            .map_err(RegistryError::Storage)?;
        if legacy > 0 {
            return Err(RegistryError::LegacyOwnershipUnproven {
                table,
                relation,
                detail: "its pre-upgrade history key is ambiguous (a name \
                         contains `/`)"
                    .into(),
            });
        }
        return Err(no_match(seen_any, table, relation));
    }

    let mut candidates = 0usize;
    let mut cursor = None;
    loop {
        let page = registry
            .legacy_page(&key.tenant, &key.db, &key.table, cursor, HISTORY_PAGE)
            .await
            .map_err(RegistryError::Storage)?;
        for lv in page.versions {
            seen_any = true;
            if serde_json::from_value::<PostgresTableSchema>(lv.schema_json)
                .is_ok_and(|s| s.matches_relation(rel))
            {
                candidates += 1;
            }
        }
        match page.next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    if candidates > 0 {
        return Err(RegistryError::LegacyOwnershipUnproven {
            table,
            relation,
            detail: format!(
                "{candidates} pre-upgrade version(s) match its relation identity"
            ),
        });
    }
    Err(no_match(seen_any, table, relation))
}

fn no_match(seen_any: bool, table: String, relation: String) -> RegistryError {
    if seen_any {
        RegistryError::NoMatch { table, relation }
    } else {
        RegistryError::NoHistory { table, relation }
    }
}

/// Build PostgresColumn from query row.
/// The registered schema model of each of `tables` that exists, read on
/// `client` in one batch and in that session's catalog snapshot (no lineage
/// proof: the caller's session is already proven). The one definition of a
/// PostgreSQL table's schema: the loader registers it and the snapshot
/// anchor compares it.
pub(crate) async fn fetch_tables_on(
    client: &tokio_postgres::Client,
    tables: &[(&str, &str)],
) -> SourceResult<HashMap<(String, String), PostgresTableSchema>> {
    let mut schemas: HashMap<(String, String), PostgresTableSchema> =
        HashMap::new();
    if tables.is_empty() {
        return Ok(schemas);
    }
    let names: Vec<&str> = tables.iter().map(|(s, _)| *s).collect();
    let rels: Vec<&str> = tables.iter().map(|(_, t)| *t).collect();

    // `a.atttypid` is the column's pgoutput type OID; it is joined in so the
    // persisted schema carries the same (name, type_oid) signature the
    // logical-replication Relation message sends, enabling drift detection at
    // first resolution with no extra catalog query.
    let col_rows = client
        .query(
            r#"
            SELECT
                c.column_name, c.data_type, c.udt_name, c.is_nullable,
                c.ordinal_position, c.column_default, c.character_maximum_length,
                c.numeric_precision, c.numeric_scale, c.is_identity,
                c.identity_generation, c.is_generated, a.atttypid,
                a.atttypmod, c.table_schema::text, c.table_name::text
            FROM unnest($1::text[], $2::text[]) AS k(s, t)
            JOIN information_schema.columns c
                ON c.table_schema = k.s AND c.table_name = k.t
            JOIN pg_catalog.pg_namespace nsp
                ON nsp.nspname = c.table_schema
            JOIN pg_catalog.pg_class cl
                ON cl.relname = c.table_name AND cl.relnamespace = nsp.oid
            JOIN pg_catalog.pg_attribute a
                ON a.attrelid = cl.oid AND a.attname = c.column_name
                AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY c.table_schema, c.table_name, c.ordinal_position
            "#,
            &[&names, &rels],
        )
        .await
        .map_err(query_error)?;
    for row in &col_rows {
        schemas
            .entry((row.get(14), row.get(15)))
            .or_insert_with(|| PostgresTableSchema {
                columns: Vec::new(),
                primary_key: Vec::new(),
                replica_identity: None,
                oid: None,
                schema_name: Some(row.get(14)),
            })
            .columns
            .push(build_column(row));
    }

    let pk_rows = client
        .query(
            r#"
            SELECT k.s, k.t, a.attname
            FROM unnest($1::text[], $2::text[]) AS k(s, t)
            JOIN pg_namespace n ON n.nspname = k.s
            JOIN pg_class c ON c.relname = k.t AND c.relnamespace = n.oid
            JOIN pg_index i ON i.indrelid = c.oid AND i.indisprimary
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            ORDER BY k.s, k.t, array_position(i.indkey, a.attnum)
            "#,
            &[&names, &rels],
        )
        .await
        .map_err(query_error)?;
    for row in pk_rows {
        if let Some(s) = schemas.get_mut(&(row.get(0), row.get(1))) {
            s.primary_key.push(row.get(2));
        }
    }

    // Catalog joins, not a `::regclass` cast: a cast resolves against the
    // latest catalog, not the session's snapshot.
    let rel_rows = client
        .query(
            r#"
            SELECT k.s, k.t, c.oid, c.relreplident
            FROM unnest($1::text[], $2::text[]) AS k(s, t)
            JOIN pg_namespace n ON n.nspname = k.s
            JOIN pg_class c ON c.relname = k.t AND c.relnamespace = n.oid
            "#,
            &[&names, &rels],
        )
        .await
        .map_err(query_error)?;
    for row in rel_rows {
        if let Some(s) = schemas.get_mut(&(row.get(0), row.get(1))) {
            s.oid = Some(row.get::<_, u32>(2));
            s.replica_identity = Some(
                match row.get::<_, i8>(3) as u8 as char {
                    'd' => "default",
                    'n' => "nothing",
                    'f' => "full",
                    'i' => "index",
                    _ => "unknown",
                }
                .to_string(),
            );
        }
    }

    Ok(schemas)
}

fn build_column(row: &tokio_postgres::Row) -> PostgresColumn {
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
    let type_oid: u32 = row.get(12);
    let type_modifier: i32 = row.get(13);

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
    col.type_oid = Some(type_oid);
    // -1: the type takes no modifier.
    col.type_modifier = (type_modifier >= 0).then_some(type_modifier);

    col
}

/// System schemas a pattern without a schema never matches.
const ANY_SCHEMA: &str =
    "table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')";

/// One keyset page of the tables `patterns` may capture (a superset; the
/// caller applies the CDC matcher): base tables strictly after `after` in
/// bytewise `(schema, table)` order (`COLLATE "C"`), at most `limit` rows.
pub(crate) async fn discovery_page(
    client: &tokio_postgres::Client,
    patterns: &[String],
    after: Option<&(String, String)>,
    limit: usize,
) -> SourceResult<Vec<(String, String)>> {
    let filter = crate::table_patterns::combined_superset(
        patterns,
        "table_schema",
        "table_name",
        ANY_SCHEMA,
    );
    let sql = format!(
        "SELECT table_schema::text, table_name::text \
         FROM information_schema.tables \
         WHERE table_type = 'BASE TABLE' AND {filter} \
           AND ($1::text IS NULL \
             OR table_schema::text COLLATE \"C\" > $1::text COLLATE \"C\" \
             OR (table_schema::text COLLATE \"C\" = $1::text COLLATE \"C\" \
                 AND table_name::text COLLATE \"C\" > $2::text COLLATE \"C\")) \
         ORDER BY table_schema::text COLLATE \"C\", \
                  table_name::text COLLATE \"C\" \
         LIMIT $3"
    );
    let (schema, table) = match after {
        Some((s, t)) => (Some(s.as_str()), Some(t.as_str())),
        None => (None, None),
    };
    let rows = client
        .query(&sql, &[&schema, &table, &(limit as i64)])
        .await
        .map_err(query_error)?;
    Ok(rows.into_iter().map(|r| (r.get(0), r.get(1))).collect())
}

fn query_error(e: tokio_postgres::Error) -> SourceError {
    SourceError::Other(anyhow::anyhow!("postgres query: {}", e))
}

#[async_trait::async_trait]
impl SourceSchemaLoader for PostgresSchemaLoader {
    fn source_type(&self) -> &'static str {
        "postgres"
    }

    async fn load(
        &self,
        schema: &str,
        table: &str,
    ) -> anyhow::Result<ApiLoadedSchema> {
        self.scope.current()?;
        let loaded = self.load_schema(schema, table).await?;
        Ok(ApiLoadedSchema {
            database: schema.to_string(),
            table: table.to_string(),
            schema_json: serde_json::to_value(&*loaded.schema)
                .unwrap_or_default(),
            columns: loaded.column_names.iter().cloned().collect(),
            primary_key: loaded.schema.primary_key.clone(),
            fingerprint: loaded.fingerprint.to_string(),
            registry_version: loaded.registry_version,
            loaded_at: chrono::Utc::now(),
        })
    }

    async fn reload(
        &self,
        schema: &str,
        table: &str,
    ) -> anyhow::Result<ApiLoadedSchema> {
        self.scope.current()?;
        let loaded = self.reload_schema(schema, table).await?;
        Ok(ApiLoadedSchema {
            database: schema.to_string(),
            table: table.to_string(),
            schema_json: serde_json::to_value(&*loaded.schema)
                .unwrap_or_default(),
            columns: loaded.column_names.iter().cloned().collect(),
            primary_key: loaded.schema.primary_key.clone(),
            fingerprint: loaded.fingerprint.to_string(),
            registry_version: loaded.registry_version,
            loaded_at: chrono::Utc::now(),
        })
    }

    async fn reload_all(
        &self,
        patterns: &[String],
    ) -> anyhow::Result<Vec<(String, String)>> {
        self.scope.current()?;
        PostgresSchemaLoader::reload_all(self, patterns)
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

#[cfg(test)]
mod tests {
    use super::*;

    mod scope_race {
        use super::*;

        fn loaded(column: &str) -> LoadedSchema {
            let pg_schema =
                PostgresTableSchema::new(vec![PostgresColumn::new(
                    column, "integer", true, 1,
                )]);
            LoadedSchema {
                fingerprint: pg_schema.fingerprint().into(),
                schema: Arc::new(pg_schema),
                registry_version: 1,
                sequence: 0,
                column_names: Arc::new(vec![column.to_string()]),
            }
        }

        fn key() -> (String, String) {
            ("public".to_string(), "orders".to_string())
        }

        fn loader(scope: &SharedRegistryScope) -> PostgresSchemaLoader {
            PostgresSchemaLoader::new(
                "host=127.0.0.1 port=1 user=none dbname=none",
                DurableSchemaRegistry::for_testing(),
                "acme",
                scope.clone(),
            )
        }

        fn pg(n: u64) -> LineageDescriptor {
            LineageDescriptor::postgres(n, n).unwrap()
        }

        /// Evictions are counted with truthful bounded labels: tenant, the
        /// configured source id and the engine.
        #[test]
        fn evictions_are_labelled_by_tenant_source_and_engine() {
            use metrics_util::debugging::{DebugValue, DebuggingRecorder};
            let recorder = DebuggingRecorder::new();
            let snap = recorder.snapshotter();
            metrics::with_local_recorder(&recorder, || {
                tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap()
                    .block_on(async {
                        let scope = SharedRegistryScope::new("src");
                        scope.publish_for_test("acme", pg(1));
                        let l = loader(&scope).with_cache_budget(
                            crate::registry_scope::CacheBudget {
                                max_entries: 1,
                                max_bytes: usize::MAX,
                            },
                        );
                        let current = l.current_scope().unwrap();
                        let other = ("public".to_string(), "items".to_string());
                        l.cache_insert(&current, key(), loaded("a")).await;
                        l.cache_insert(&current, other, loaded("b")).await;
                    });
            });
            let (labels, value) = snap
                .snapshot()
                .into_vec()
                .into_iter()
                .find_map(|(ck, _, _, v)| match v {
                    DebugValue::Counter(n)
                        if ck.key().name()
                            == "deltaforge_source_schema_cache_evictions_total" =>
                    {
                        let mut labels: Vec<(String, String)> = ck
                            .key()
                            .labels()
                            .map(|l| (l.key().to_string(), l.value().to_string()))
                            .collect();
                        labels.sort();
                        Some((labels, n))
                    }
                    _ => None,
                })
                .expect("eviction counted");
            assert_eq!(value, 1);
            assert_eq!(
                labels,
                [
                    ("engine".to_string(), "postgres".to_string()),
                    ("source_id".to_string(), "src".to_string()),
                    ("tenant".to_string(), "acme".to_string()),
                ]
            );
        }

        /// An evicted table is rebuilt exactly from durable history (its
        /// pinned version, sequence and fingerprint, no live read); a pinned
        /// version that is gone or differs fails closed instead of being
        /// re-resolved.
        #[tokio::test]
        async fn an_evicted_table_is_rebuilt_exactly_or_fails_closed() {
            let scope = SharedRegistryScope::new("src");
            scope.publish_for_test("acme", pg(1));
            let l = loader(&scope).with_cache_budget(
                crate::registry_scope::CacheBudget {
                    max_entries: 1,
                    max_bytes: usize::MAX,
                },
            );
            let current = l.current_scope().unwrap();
            let mut orders = loaded("id");
            orders.registry_version = l
                .registry
                .register_with_checkpoint(
                    &current.key("public", "orders"),
                    &orders.fingerprint,
                    &serde_json::to_value(&*orders.schema).unwrap(),
                    None,
                )
                .await
                .unwrap();
            orders.sequence = 42;
            assert!(l.cache_insert(&current, key(), orders.clone()).await);
            let other = ("public".to_string(), "items".to_string());
            assert!(l.cache_insert(&current, other.clone(), loaded("x")).await);
            assert!(l.get_cached("public", "orders").is_none(), "evicted");

            let rebuilt =
                l.rebuild_pinned(&current, &key()).await.unwrap().unwrap();
            assert_eq!(
                (
                    rebuilt.registry_version,
                    rebuilt.sequence,
                    &rebuilt.fingerprint
                ),
                (orders.registry_version, 42, &orders.fingerprint)
            );
            assert_eq!(rebuilt.schema, orders.schema);
            assert_eq!(l.live_fetch_count(), 0);

            // `items` was pinned to version 1, which was never registered.
            let version = orders.registry_version;
            l.cache_insert(&current, key(), orders).await;
            let gone = l.rebuild_pinned(&current, &other).await.unwrap_err();
            assert!(matches!(gone, SourceError::Schema { .. }), "{gone:?}");

            // A pin whose stored version (which exists) holds other content.
            let mut differs = loaded("other");
            differs.registry_version = version;
            l.cache_insert(&current, key(), differs).await;
            l.cache_insert(&current, other, loaded("x")).await;
            let changed = l.rebuild_pinned(&current, &key()).await.unwrap_err();
            assert!(matches!(changed, SourceError::Schema { .. }));
        }

        /// The first-resolution baseline is read from durable history, not
        /// the cache: none means first use, a readable version is returned,
        /// and unreadable stored history fails closed.
        #[tokio::test]
        async fn persisted_history_is_read_durably_and_corrupt_fails_closed() {
            let scope = SharedRegistryScope::new("src");
            scope.publish_for_test("acme", pg(1));
            let l = loader(&scope);
            assert!(l.persisted("public", "orders").await.unwrap().is_none());
            let key = l.current_scope().unwrap().key("public", "orders");
            let good =
                serde_json::to_value(PostgresTableSchema::new(vec![])).unwrap();
            l.registry
                .register_with_checkpoint(&key, "h1", &good, None)
                .await
                .unwrap();
            assert!(l.persisted("public", "orders").await.unwrap().is_some());
            l.registry
                .register_with_checkpoint(
                    &key,
                    "h2",
                    &serde_json::json!({ "columns": 5 }),
                    None,
                )
                .await
                .unwrap();
            let err = l.persisted("public", "orders").await.unwrap_err();
            assert!(format!("{err}").contains("unreadable"), "{err}");
        }

        /// Reviewer interleaving: a load starts under A; the lineage changes
        /// to B; a B lookup populates the cache; the A load finishes last.
        #[tokio::test]
        async fn late_result_from_previous_lineage_is_discarded() {
            let scope = SharedRegistryScope::new("src");
            scope.publish_for_test("acme", pg(1));
            let l = loader(&scope);

            let under_a = l.current_scope().unwrap();
            scope.publish_for_test("acme", pg(2));
            let under_b = l.current_scope().unwrap();
            assert!(l.cache_insert(&under_b, key(), loaded("b_col")).await);
            assert!(
                !l.cache_insert(&under_a, key(), loaded("a_col")).await,
                "a result produced under the previous lineage must be discarded"
            );
            let served = l.get_cached("public", "orders").unwrap();
            assert_eq!(served.column_names.as_slice(), ["b_col"]);
        }

        /// The A result arrives after the lineage changed but before any B
        /// lookup touched the cache: still discarded, nothing is served.
        #[tokio::test]
        async fn result_from_previous_lineage_is_never_cached() {
            let scope = SharedRegistryScope::new("src");
            scope.publish_for_test("acme", pg(1));
            let l = loader(&scope);

            let under_a = l.current_scope().unwrap();
            assert!(l.cache_insert(&under_a, key(), loaded("a_col")).await);
            scope.publish_for_test("acme", pg(2));
            assert!(
                l.get_cached("public", "orders").is_none(),
                "A entry not served under B"
            );
            assert!(!l.cache_insert(&under_a, key(), loaded("a_col2")).await);
            assert!(l.get_cached("public", "orders").is_none());
            assert!(
                l.cached_tables().contains(&key()),
                "reconciliation still sees A's tables"
            );
        }
    }

    mod retained_resolution {
        use super::*;
        use serde_json::json;
        use std::sync::atomic::Ordering as AtomicOrdering;
        use storage::adapters::test_util::FaultBackend;
        use storage::adapters::{RegistryConfig, SchemaKey};
        use storage::{ArcStorageBackend, MemoryStorageBackend};

        const ORDERS: &[(&str, u32)] = &[("id", 23), ("sku", 25)];

        fn table(
            oid: u32,
            cols: &[(&str, u32)],
            pk: &[&str],
        ) -> PostgresTableSchema {
            let columns = cols
                .iter()
                .enumerate()
                .map(|(i, (name, type_oid))| {
                    let mut c = PostgresColumn::new(
                        *name,
                        "integer",
                        true,
                        i as i32 + 1,
                    );
                    c.type_oid = Some(*type_oid);
                    c
                })
                .collect();
            PostgresTableSchema {
                columns,
                primary_key: pk.iter().map(|s| s.to_string()).collect(),
                replica_identity: Some("full".into()),
                oid: Some(oid),
                schema_name: Some("public".into()),
            }
        }

        fn rel(oid: u32) -> RelationIdentity {
            RelationIdentity {
                oid,
                signature: ORDERS
                    .iter()
                    .map(|(n, t)| (n.to_string(), *t))
                    .collect(),
                replica_identity: 'f',
            }
        }

        fn key() -> SchemaKey {
            SchemaKey::new("t", "src", "lin", "public", "orders")
        }

        async fn registry(
            backend: &ArcStorageBackend,
        ) -> Arc<DurableSchemaRegistry> {
            DurableSchemaRegistry::new(Arc::clone(backend))
                .await
                .unwrap()
        }

        async fn register(
            r: &DurableSchemaRegistry,
            s: &PostgresTableSchema,
        ) -> i32 {
            r.register_with_checkpoint(
                &key(),
                &s.fingerprint(),
                &serde_json::to_value(s).unwrap(),
                None,
            )
            .await
            .unwrap()
        }

        /// A pre-upgrade entry under the legacy flat key; returns its sequence.
        async fn seed_legacy(
            b: &ArcStorageBackend,
            s: &PostgresTableSchema,
        ) -> u64 {
            let entry = json!({
                "hash": s.fingerprint(),
                "schema_json": serde_json::to_value(s).unwrap(),
                "registered_at": serde_json::to_value(chrono::Utc::now()).unwrap(),
                "checkpoint": null,
            });
            b.log_append(
                "schemas",
                "t/public/orders",
                &serde_json::to_vec(&entry).unwrap(),
            )
            .await
            .unwrap()
        }

        async fn scoped_versions(r: &DurableSchemaRegistry) -> Vec<i32> {
            r.history_page(&key(), None, 100)
                .await
                .unwrap()
                .versions
                .iter()
                .map(|v| v.version)
                .collect()
        }

        fn mem() -> ArcStorageBackend {
            Arc::new(MemoryStorageBackend::new())
        }

        #[tokio::test]
        async fn unique_scoped_match_wins_and_legacy_is_untouched() {
            let b = mem();
            seed_legacy(&b, &table(10, ORDERS, &[])).await;
            let r = registry(&b).await;
            let v = register(&r, &table(10, ORDERS, &[])).await;
            let (version, _, schema) =
                resolve_retained_relation(&r, &key(), &rel(10))
                    .await
                    .unwrap();
            assert_eq!(version, v);
            assert_eq!(schema.oid, Some(10));
            assert_eq!(scoped_versions(&r).await, vec![v], "nothing adopted");
        }

        #[tokio::test]
        async fn several_scoped_matches_are_ambiguous() {
            let b = mem();
            let r = registry(&b).await;
            register(&r, &table(10, ORDERS, &[])).await;
            // Same relation identity, different fingerprint (primary key).
            register(&r, &table(10, ORDERS, &["id"])).await;
            let err = resolve_retained_relation(&r, &key(), &rel(10))
                .await
                .unwrap_err();
            assert!(
                matches!(
                    err,
                    RegistryError::AmbiguousHistory { matches: 2, .. }
                ),
                "{err:?}"
            );
        }

        #[tokio::test]
        async fn a_unique_legacy_match_does_not_prove_ownership() {
            // Relation OIDs are database-local: another database in the cluster
            // (or another source of the tenant) can supply the only legacy
            // version with this OID and shape. It must not be adopted.
            let b = mem();
            seed_legacy(&b, &table(99, ORDERS, &[])).await;
            seed_legacy(&b, &table(10, ORDERS, &[])).await;
            seed_legacy(&b, &table(98, ORDERS, &[])).await;
            let r = registry(&b).await;
            let err = resolve_retained_relation(&r, &key(), &rel(10))
                .await
                .unwrap_err();
            assert!(
                matches!(err, RegistryError::LegacyOwnershipUnproven { ref detail, .. }
                    if detail.contains("1 pre-upgrade version")),
                "{err:?}"
            );
            assert!(scoped_versions(&r).await.is_empty(), "nothing adopted");
        }

        /// Two legacy versions match the retained relation. Only the COMPLETE
        /// migration makes that visible (ambiguous, fail closed); the partial
        /// state after a failed apply resolves to a unique match - wrong - which
        /// is why the store gate stays held until the migration completes.
        #[tokio::test]
        async fn a_partial_migration_of_two_matching_versions_is_a_false_unique_match()
         {
            use storage::adapters::schema_migration::{
                self, Filters, Mapping, MappingEntry, TableRef,
            };
            use storage::adapters::source_lineage;

            let mut saw_partial = false;
            for budget in 0..16u64 {
                let f = Arc::new(FaultBackend::new());
                let b: ArcStorageBackend = f.clone();
                let lh = source_lineage::establish(
                    &b,
                    "t",
                    "src",
                    LineageDescriptor::postgres(7, 8).unwrap(),
                )
                .await
                .unwrap()
                .record
                .current
                .lineage_hash;
                let k =
                    SchemaKey::new("t", "src", lh.as_str(), "public", "orders");
                seed_legacy(&b, &table(10, ORDERS, &[])).await;
                seed_legacy(&b, &table(10, ORDERS, &["id"])).await;
                let r = registry(&b).await;
                let m = Mapping {
                    mappings: vec![MappingEntry {
                        tenant: "t".into(),
                        source_id: "src".into(),
                        lineage_hash: lh.clone(),
                        tables: vec![TableRef {
                            db: "public".into(),
                            table: "orders".into(),
                        }],
                    }],
                };
                let f0 = Filters::default();
                let p = schema_migration::plan(&b, &r, &m, &f0).await.unwrap();

                f.allow_writes(budget);
                let failed = schema_migration::apply(&b, &r, &m, &f0, &p.proof)
                    .await
                    .is_err();
                f.allow_writes(u64::MAX);
                let adopted =
                    r.history_page(&k, None, 10).await.unwrap().versions.len();
                if failed && adopted == 1 {
                    saw_partial = true;
                    // The hazard: the incomplete history looks unique.
                    assert!(
                        resolve_retained_relation(&r, &k, &rel(10))
                            .await
                            .is_ok()
                    );
                }
                // Resuming under the original proof completes it...
                let again =
                    schema_migration::plan(&b, &r, &m, &f0).await.unwrap();
                assert_eq!(again.proof, p.proof, "budget {budget}");
                schema_migration::apply(&b, &r, &m, &f0, &p.proof)
                    .await
                    .unwrap();
                // ...and only then is the true answer visible: ambiguous.
                let err = resolve_retained_relation(&r, &k, &rel(10))
                    .await
                    .unwrap_err();
                assert!(
                    matches!(
                        err,
                        RegistryError::AmbiguousHistory { matches: 2, .. }
                    ),
                    "budget {budget}: {err:?}"
                );
            }
            assert!(saw_partial, "no write budget produced a partial adoption");
        }

        #[tokio::test]
        async fn several_legacy_matches_also_fail_closed() {
            let b = mem();
            seed_legacy(&b, &table(10, ORDERS, &[])).await;
            seed_legacy(&b, &table(10, ORDERS, &["id"])).await;
            let r = registry(&b).await;
            let err = resolve_retained_relation(&r, &key(), &rel(10))
                .await
                .unwrap_err();
            assert!(
                matches!(err, RegistryError::LegacyOwnershipUnproven { ref detail, .. }
                    if detail.contains("2 pre-upgrade version")),
                "{err:?}"
            );
            assert!(scoped_versions(&r).await.is_empty(), "nothing adopted");
        }

        #[tokio::test]
        async fn legacy_history_without_a_match_is_no_match() {
            let b = mem();
            seed_legacy(&b, &table(55, ORDERS, &[])).await;
            let r = registry(&b).await;
            let err = resolve_retained_relation(&r, &key(), &rel(10))
                .await
                .unwrap_err();
            assert!(matches!(err, RegistryError::NoMatch { .. }), "{err:?}");
        }

        #[tokio::test]
        async fn ambiguous_legacy_key_is_never_read() {
            let b = mem();
            // `public` + `a/b` and `public/a` + `b` share the flat key.
            let entry = serde_json::json!({
                "hash": "h",
                "schema_json": serde_json::to_value(table(10, ORDERS, &[])).unwrap(),
                "registered_at": serde_json::to_value(chrono::Utc::now()).unwrap(),
                "checkpoint": null,
            });
            b.log_append(
                "schemas",
                "t/public/a/b",
                &serde_json::to_vec(&entry).unwrap(),
            )
            .await
            .unwrap();
            let r = registry(&b).await;
            let slash_key = SchemaKey::new("t", "src", "lin", "public", "a/b");
            let err = resolve_retained_relation(&r, &slash_key, &rel(10))
                .await
                .unwrap_err();
            assert!(
                matches!(err, RegistryError::LegacyOwnershipUnproven { ref detail, .. }
                    if detail.contains("ambiguous")),
                "{err:?}"
            );
            // Without any legacy history under that flat key it is simply absent.
            let other = SchemaKey::new("t", "src", "lin", "public", "x/y");
            let err = resolve_retained_relation(&r, &other, &rel(10))
                .await
                .unwrap_err();
            assert!(matches!(err, RegistryError::NoHistory { .. }), "{err:?}");
        }

        #[tokio::test]
        async fn no_history_and_no_match_are_distinct() {
            let b = mem();
            let r = registry(&b).await;
            let err = resolve_retained_relation(&r, &key(), &rel(10))
                .await
                .unwrap_err();
            assert!(matches!(err, RegistryError::NoHistory { .. }), "{err:?}");

            register(&r, &table(77, ORDERS, &[])).await;
            let err = resolve_retained_relation(&r, &key(), &rel(10))
                .await
                .unwrap_err();
            assert!(matches!(err, RegistryError::NoMatch { .. }), "{err:?}");
        }

        #[tokio::test]
        async fn migration_disabled_never_consults_legacy() {
            let b = mem();
            seed_legacy(&b, &table(10, ORDERS, &[])).await;
            let r = DurableSchemaRegistry::with_config(
                Arc::clone(&b),
                RegistryConfig {
                    migration_enabled: false,
                    ..RegistryConfig::default()
                },
            )
            .await
            .unwrap();
            let err = resolve_retained_relation(&r, &key(), &rel(10))
                .await
                .unwrap_err();
            assert!(matches!(err, RegistryError::NoHistory { .. }), "{err:?}");
            assert!(scoped_versions(&r).await.is_empty());
        }

        #[tokio::test]
        async fn storage_failure_is_distinct_from_absence() {
            let f = Arc::new(FaultBackend::new());
            let b: ArcStorageBackend = f.clone();
            let r = registry(&b).await;
            f.fail_log_read_meta.store(true, AtomicOrdering::SeqCst);
            let err = resolve_retained_relation(&r, &key(), &rel(10))
                .await
                .unwrap_err();
            assert!(matches!(err, RegistryError::Storage(_)), "{err:?}");
        }
    }

    /// Catalog queries narrow like CDC filtering: a schema-less pattern
    /// spans every schema, a trailing `*` or `%` is a prefix, and `_` / `%`
    /// inside names are literal.
    #[test]
    fn pattern_queries_follow_cdc_filtering() {
        let q = crate::table_patterns::combined_superset(
            &["public.users".to_string()],
            "table_schema",
            "table_name",
            ANY_SCHEMA,
        );
        assert!(q.contains("table_schema = 'public'"));
        assert!(q.contains("table_name = 'users'"));
        let q = crate::table_patterns::combined_superset(
            &["public.*".to_string()],
            "table_schema",
            "table_name",
            ANY_SCHEMA,
        );
        assert!(q.contains("table_schema = 'public'") && q.contains("1=1"));
        let q = crate::table_patterns::combined_superset(
            &["orders".to_string()],
            "table_schema",
            "table_name",
            ANY_SCHEMA,
        );
        assert!(
            q.contains("table_schema NOT IN")
                && q.contains("table_name = 'orders'")
        );
        for p in ["public.audit_*", "public.audit_%"] {
            let q = crate::table_patterns::combined_superset(
                &[p.to_string()],
                "table_schema",
                "table_name",
                ANY_SCHEMA,
            );
            assert!(q.contains("table_name LIKE 'audit|_%' ESCAPE '|'"), "{q}");
        }
    }
}
