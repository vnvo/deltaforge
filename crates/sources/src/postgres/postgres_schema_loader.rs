//! PostgreSQL schema loader with wildcard expansion and registry integration.
//!
//! Provides schema preloading at startup with support for:
//! - Wildcard table patterns (e.g., `public.*`, `%.audit_log`)
//! - Full schema loading from information_schema
//! - Schema registry integration with fingerprinting
//! - On-demand reload capability

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use metrics::counter;
use schema_registry::SourceSchema;
use storage::DurableSchemaRegistry;
use storage::adapters::{LegacyVersion, SchemaKey};
use tokio::sync::RwLock;
use tokio_postgres::NoTls;
use tracing::{debug, info, warn};

use deltaforge_core::{SourceError, SourceResult};

use super::postgres_helpers::redact_password;
use super::postgres_table_schema::{
    PostgresColumn, PostgresTableSchema, RelationIdentity,
};
use crate::registry_scope::{
    RegistryError, RegistryScope, SharedRegistryScope,
};
use crate::schema_loader::{
    LoadedSchema as ApiLoadedSchema, SchemaListEntry, SourceSchemaLoader,
};

/// Page size for durable history scans (memory is O(page), never O(history)).
const HISTORY_PAGE: usize = 256;

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

/// Schema loader with caching and registry integration.
///
/// Every registry access is qualified by the source's verified lineage, taken
/// from the shared [`SharedRegistryScope`]; with no established scope, registry
/// access fails closed. Cache entries belong to one scope generation and are
/// discarded when the source moves to a new lineage.
#[derive(Clone)]
pub struct PostgresSchemaLoader {
    dsn: crate::credentials::ProtectedDsn,
    cache: Arc<RwLock<HashMap<(String, String), LoadedSchema>>>,
    /// Scope generation the cache entries were loaded under (0 = none).
    cache_generation: Arc<AtomicU64>,
    registry: Arc<DurableSchemaRegistry>,
    scope: SharedRegistryScope,
    tenant: String,
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
            cache: Arc::new(RwLock::new(HashMap::new())),
            cache_generation: Arc::new(AtomicU64::new(0)),
            registry,
            scope,
            tenant: tenant.to_string(),
        }
    }

    /// The established registry scope. Fails closed when none is published.
    /// Discards cache entries loaded under an earlier lineage.
    async fn scope(&self) -> SourceResult<Arc<RegistryScope>> {
        let scope = self.scope.current()?;
        if self.cache_generation.load(Ordering::Acquire) != scope.generation() {
            let mut cache = self.cache.write().await;
            if self.cache_generation.load(Ordering::Acquire)
                != scope.generation()
            {
                cache.clear();
                self.cache_generation
                    .store(scope.generation(), Ordering::Release);
            }
        }
        Ok(scope)
    }

    /// Whether cache entries belong to the currently published scope.
    fn cache_valid(&self) -> bool {
        let generation = self.scope.generation();
        generation != 0
            && generation == self.cache_generation.load(Ordering::Acquire)
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

    /// Expand wildcard patterns and preload all matching schemas.
    ///
    /// This is the eager (catalog-sized) startup path. It is deliberately
    /// self-contained - it only reads latest versions through the scoped
    /// registry and falls back to [`Self::load_schema`] - so removing the eager
    /// preload later changes only its call sites, not the registry contract.
    ///
    /// Warm-start path: schemas already known to the durable registry are
    /// deserialized directly into the in-memory cache, avoiding a
    /// information_schema query per table on restart. Tables missing from the
    /// registry (first-ever run, or new tables) are fetched from the source.
    pub async fn preload(
        &self,
        patterns: &[String],
    ) -> SourceResult<Vec<(String, String)>> {
        let t0 = Instant::now();
        let scope = self.scope().await?;
        let tables = self.expand_patterns(patterns).await?;

        info!(
            dsn = redact_password(self.dsn.expose()),
            patterns = ?patterns,
            matched_tables = tables.len(),
            "expanded table patterns"
        );

        // Warm cache from the durable registry first. A registry storage error
        // fails closed; only a genuinely absent schema falls back to the source.
        let mut from_registry = 0usize;
        let mut needs_fetch: Vec<&(String, String)> = Vec::new();
        for pair in &tables {
            let (schema, table) = pair;
            let latest = self
                .registry
                .get_latest(&scope.key(schema, table))
                .await
                .map_err(RegistryError::Storage)?;
            {
                let mut cache = self.cache.write().await;
                match latest {
                    Some(sv) => {
                        match serde_json::from_value::<PostgresTableSchema>(
                            sv.schema_json,
                        ) {
                            Ok(pg_schema) => {
                                let fingerprint = pg_schema.fingerprint();
                                let column_names = Arc::new(
                                    pg_schema
                                        .columns
                                        .iter()
                                        .map(|c| c.name.clone())
                                        .collect::<Vec<_>>(),
                                );
                                cache.insert(
                                    (schema.clone(), table.clone()),
                                    LoadedSchema {
                                        schema: Arc::new(pg_schema),
                                        registry_version: sv.version,
                                        fingerprint: fingerprint.into(),
                                        sequence: sv.sequence,
                                        column_names,
                                    },
                                );
                                from_registry += 1;
                            }
                            Err(e) => {
                                warn!(schema=%schema, table=%table, error=%e,
                                    "failed to deserialize registry schema; fetching from source");
                                needs_fetch.push(pair);
                            }
                        }
                    }
                    None => needs_fetch.push(pair),
                }
            }
        }

        for (schema, table) in &needs_fetch {
            if let Err(e) = self.load_schema(schema, table).await {
                warn!(schema = %schema, table = %table, error = %e, "failed to preload schema");
            }
        }

        info!(
            tables_loaded = tables.len(),
            from_registry,
            from_source = needs_fetch.len(),
            elapsed_ms = t0.elapsed().as_millis(),
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

        if patterns.is_empty() {
            let rows = client
                .query(
                    "SELECT table_schema, table_name FROM information_schema.tables \
                     WHERE table_type = 'BASE TABLE' \
                     AND table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')",
                    &[],
                )
                .await
                .map_err(query_error)?;

            for row in rows {
                results.push((row.get(0), row.get(1)));
            }
            return Ok(results);
        }

        for pattern in patterns {
            let (schema_pattern, table_pattern) = parse_pattern(pattern);
            let query = build_pattern_query(&schema_pattern, &table_pattern);
            let rows = client.query(&query, &[]).await.map_err(query_error)?;

            for row in rows {
                let entry: (String, String) = (row.get(0), row.get(1));
                if !results.contains(&entry) {
                    results.push(entry);
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

    /// Load schema with optional checkpoint for registry correlation.
    pub async fn load_schema_at_checkpoint(
        &self,
        schema: &str,
        table: &str,
        checkpoint: Option<&[u8]>,
    ) -> SourceResult<LoadedSchema> {
        let key = (schema.to_string(), table.to_string());

        if self.cache_valid()
            && let Some(cached) = self.cache.read().await.get(&key)
        {
            debug!(schema = %schema, table = %table, "schema cache hit");
            counter!("deltaforge_source_schema_cache_hits_total",
                "pipeline" => self.tenant.clone(), "source" => "postgres")
            .increment(1);
            return Ok(cached.clone());
        }
        counter!("deltaforge_source_schema_cache_misses_total",
            "pipeline" => self.tenant.clone(), "source" => "postgres")
        .increment(1);

        let t0 = Instant::now();
        let pg_schema = self.fetch_schema(schema, table).await?;
        let loaded = self
            .register_and_cache(schema, table, pg_schema, checkpoint)
            .await?;

        let elapsed = t0.elapsed();
        if elapsed.as_millis() > 200 {
            warn!(schema = %schema, table = %table, ms = elapsed.as_millis(), "slow schema load");
        } else {
            debug!(schema = %schema, table = %table, version = loaded.registry_version, ms = elapsed.as_millis(), "schema loaded");
        }

        Ok(loaded)
    }

    /// Register a freshly fetched schema in the durable registry and cache it.
    async fn register_and_cache(
        &self,
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
        let scope = self.scope().await?;
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
        let loaded = LoadedSchema {
            schema: Arc::new(pg_schema),
            registry_version: version,
            fingerprint: fingerprint.into(),
            sequence: self.registry.current_sequence(),
            column_names,
        };
        self.cache
            .write()
            .await
            .insert((schema.to_string(), table.to_string()), loaded.clone());
        Ok(loaded)
    }

    /// Load a table's schema for decoding a pgoutput row, using the durable historical
    /// schema when the live table no longer exists.
    ///
    /// Resolution order:
    /// 1. cache hit -> return it;
    /// 2. live catalog has the table -> fetch, register, cache (normal path);
    /// 3. live table is GONE -> recover from the durable registry, selecting the **one**
    ///    historical version whose relation identity ([`RelationIdentity`]: table OID +
    ///    ordered `(name, type_oid)` signature + replica identity) matches the retained
    ///    relation. Searches the full version history (not just the latest), so WAL
    ///    encoded under an earlier schema version (A of an A->B evolution) still decodes.
    ///    Fails closed - never guesses, skips, or advances the checkpoint - when there is
    ///    no history, no matching version, or more than one matching version (ambiguous
    ///    lineage, e.g. a same-named relation in another database whose OID collides).
    pub(crate) async fn load_schema_for_relation(
        &self,
        schema: &str,
        table: &str,
        checkpoint: Option<&[u8]>,
        rel: &RelationIdentity,
    ) -> SourceResult<LoadedSchema> {
        let key = (schema.to_string(), table.to_string());
        // 1. Cached schema, ONLY if it is THIS relation's schema. A cache entry for the
        //    same name but a different relation (e.g. a recreated table) must not be used.
        if self.cache_valid()
            && let Some(cached) = self.cache.read().await.get(&key)
            && cached.schema.matches_relation(rel)
        {
            return Ok(cached.clone());
        }

        // 2. Live catalog, ONLY if it matches this relation identity. A live table whose
        //    OID/signature/replica differs from the retained relation - e.g. the table
        //    was dropped and recreated under the same name with a new OID - must NOT be
        //    used to decode the retained WAL; fall through to the durable history search.
        if let Some(pg_schema) = self.fetch_schema_opt(schema, table).await?
            && pg_schema.matches_relation(rel)
        {
            return self
                .register_and_cache(schema, table, pg_schema, checkpoint)
                .await;
        }

        // 3. Neither the cache nor the live catalog is this relation. Recover from the
        //    durable history under this source's verified lineage (paged), then - only
        //    if that has no match - from the pre-upgrade unscoped history, adopting the
        //    single uniquely matching version. Zero or multiple matches fail closed.
        let scope = self.scope().await?;
        let (version, sequence, pg_schema) = resolve_retained_relation(
            &self.registry,
            &scope.key(schema, table),
            rel,
        )
        .await?;
        let fingerprint = pg_schema.fingerprint();
        let column_names: Arc<Vec<String>> = Arc::new(
            pg_schema.columns.iter().map(|c| c.name.clone()).collect(),
        );
        let loaded = LoadedSchema {
            schema: Arc::new(pg_schema),
            registry_version: version,
            fingerprint: fingerprint.into(),
            sequence,
            column_names,
        };
        self.cache.write().await.insert(key, loaded.clone());
        info!(
            schema = %schema, table = %table, version,
            relation_oid = rel.oid,
            "decoding retained WAL for dropped table from durable schema"
        );
        Ok(loaded)
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

    /// Reload all schemas matching patterns.
    pub async fn reload_all(
        &self,
        patterns: &[String],
    ) -> SourceResult<Vec<(String, String)>> {
        self.cache.write().await.clear();
        self.preload(patterns).await
    }

    /// Get cached schema (without loading from DB).
    pub fn get_cached(
        &self,
        schema: &str,
        table: &str,
    ) -> Option<LoadedSchema> {
        if !self.cache_valid() {
            return None;
        }
        self.cache.try_read().ok().and_then(|c| {
            c.get(&(schema.to_string(), table.to_string())).cloned()
        })
    }

    /// Fetch full schema from information_schema, mapping a missing table to the
    /// `table not found` schema error (the historical behavior).
    async fn fetch_schema(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> SourceResult<PostgresTableSchema> {
        self.fetch_schema_opt(schema_name, table_name)
            .await?
            .ok_or_else(|| SourceError::Schema {
                details: format!("table {schema_name}.{table_name} not found")
                    .into(),
            })
    }

    /// Fetch full schema from information_schema, returning `Ok(None)` when the table
    /// no longer exists in the live catalog (so callers can fall back to the durable
    /// historical schema instead of failing).
    async fn fetch_schema_opt(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> SourceResult<Option<PostgresTableSchema>> {
        let client = self.connect().await?;

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
                    c.identity_generation, c.is_generated, a.atttypid
                FROM information_schema.columns c
                JOIN pg_catalog.pg_namespace nsp
                    ON nsp.nspname = c.table_schema
                JOIN pg_catalog.pg_class cl
                    ON cl.relname = c.table_name AND cl.relnamespace = nsp.oid
                JOIN pg_catalog.pg_attribute a
                    ON a.attrelid = cl.oid AND a.attname = c.column_name
                    AND a.attnum > 0 AND NOT a.attisdropped
                WHERE c.table_schema = $1 AND c.table_name = $2
                ORDER BY c.ordinal_position
                "#,
                &[&schema_name, &table_name],
            )
            .await
            .map_err(query_error)?;

        if col_rows.is_empty() {
            return Ok(None);
        }

        let columns: Vec<PostgresColumn> =
            col_rows.iter().map(build_column).collect();

        let pk_rows = client
            .query(
                r#"
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = ($1 || '.' || $2)::regclass AND i.indisprimary
                ORDER BY array_position(i.indkey, a.attnum)
                "#,
                &[&schema_name, &table_name],
            )
            .await
            .map_err(query_error)?;

        let primary_key: Vec<String> =
            pk_rows.iter().map(|r| r.get(0)).collect();

        let identity_row = client
            .query_opt(
                r#"
                SELECT relreplident FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = $1 AND c.relname = $2
                "#,
                &[&schema_name, &table_name],
            )
            .await
            .map_err(query_error)?;

        let replica_identity = identity_row.map(|r| {
            match r.get::<_, i8>(0) as u8 as char {
                'd' => "default",
                'n' => "nothing",
                'f' => "full",
                'i' => "index",
                _ => "unknown",
            }
            .to_string()
        });

        let oid = client
            .query_opt(
                "SELECT ($1 || '.' || $2)::regclass::oid",
                &[&schema_name, &table_name],
            )
            .await
            .map_err(query_error)?
            .map(|r| r.get::<_, u32>(0));

        Ok(Some(PostgresTableSchema {
            columns,
            primary_key,
            replica_identity,
            oid,
            schema_name: Some(schema_name.to_string()),
        }))
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
            .map(|c| c.keys().cloned().collect())
            .unwrap_or_default()
    }
}

/// Resolve the schema of a retained pgoutput relation from durable history.
///
/// 1. Page the version history under the source's verified lineage (`key`);
///    exactly one version matching the full relation identity (OID + ordered
///    `(name, type_oid)` + replica identity) is used; several are ambiguous.
/// 2. Only if (1) has no match and migration is enabled: page the pre-upgrade
///    unscoped history for the same schema/table. A legacy flat key carries no
///    lineage, so only the single uniquely matching version is adopted - with
///    its version number, hash and sequence preserved - and every other legacy
///    version stays unmigrated. Several matches are ambiguous and nothing is
///    adopted.
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

    let mut legacy_found: Option<(LegacyVersion, PostgresTableSchema)> = None;
    let mut legacy_matches = 0usize;
    let mut cursor = None;
    loop {
        let page = registry
            .legacy_page(&key.tenant, &key.db, &key.table, cursor, HISTORY_PAGE)
            .await
            .map_err(RegistryError::Storage)?;
        for lv in page.versions {
            seen_any = true;
            if let Ok(s) = serde_json::from_value::<PostgresTableSchema>(
                lv.schema_json.clone(),
            ) && s.matches_relation(rel)
            {
                legacy_matches += 1;
                if legacy_found.is_none() {
                    legacy_found = Some((lv, s));
                }
            }
        }
        match page.next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    match (legacy_matches, legacy_found) {
        (1, Some((lv, s))) => {
            registry
                .adopt_legacy_version(key, &lv)
                .await
                .map_err(RegistryError::Storage)?;
            info!(
                table = %table, version = lv.version,
                "adopted the uniquely matching pre-upgrade schema version"
            );
            Ok((lv.version, lv.sequence, s))
        }
        (0, _) => Err(no_match(seen_any, table, relation)),
        (n, _) => Err(RegistryError::AmbiguousLegacy {
            table,
            relation,
            matches: n,
        }),
    }
}

fn no_match(seen_any: bool, table: String, relation: String) -> RegistryError {
    if seen_any {
        RegistryError::NoMatch { table, relation }
    } else {
        RegistryError::NoHistory { table, relation }
    }
}

/// Build PostgresColumn from query row.
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

    col
}

fn parse_pattern(pattern: &str) -> (String, String) {
    pattern
        .split_once('.')
        .map(|(s, t)| (s.to_string(), t.to_string()))
        .unwrap_or_else(|| ("public".to_string(), pattern.to_string()))
}

fn build_pattern_query(schema_pattern: &str, table_pattern: &str) -> String {
    // Use LIKE only when the pattern contains a glob wildcard (*).
    // The `_` character is common in table names and should NOT trigger
    // LIKE matching — it's a literal underscore, not a wildcard.
    let schema_clause = match schema_pattern {
        "*" | "%" => "table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')".to_string(),
        s if s.contains('*') => format!("table_schema LIKE '{}'", escape_like(s)),
        s => format!("table_schema = '{}'", escape_sql(s)),
    };

    let table_clause = match table_pattern {
        "*" | "%" => "1=1".to_string(),
        t if t.contains('*') => {
            format!("table_name LIKE '{}'", escape_like(t))
        }
        t => format!("table_name = '{}'", escape_sql(t)),
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

/// Convert a glob-style pattern (using `*` as wildcard) to a SQL LIKE pattern.
/// Escapes SQL LIKE metacharacters (`%`, `_`) as literals, then replaces
/// glob `*` with LIKE `%`.
fn escape_like(s: &str) -> String {
    s.replace('\'', "''")
        .replace('%', "\\%")
        .replace('_', "\\_")
        .replace('*', "%")
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
        if !self.cache_valid() {
            return Vec::new();
        }
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

#[cfg(test)]
mod tests {
    use super::*;

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
        async fn only_the_unique_legacy_match_is_adopted() {
            let b = mem();
            seed_legacy(&b, &table(99, ORDERS, &[])).await;
            let seq = seed_legacy(&b, &table(10, ORDERS, &[])).await;
            seed_legacy(&b, &table(98, ORDERS, &[])).await;
            let r = registry(&b).await;
            let (version, sequence, schema) =
                resolve_retained_relation(&r, &key(), &rel(10))
                    .await
                    .unwrap();
            // Pre-upgrade identity preserved: positional version 2, original sequence.
            assert_eq!((version, sequence, schema.oid), (2, seq, Some(10)));
            // The two unproven legacy versions stay unmigrated.
            assert_eq!(scoped_versions(&r).await, vec![2]);
            // Resolving again is served from the scoped history.
            let again = resolve_retained_relation(&r, &key(), &rel(10))
                .await
                .unwrap();
            assert_eq!(again.0, 2);
            assert_eq!(scoped_versions(&r).await, vec![2]);
        }

        #[tokio::test]
        async fn ambiguous_legacy_adopts_nothing() {
            let b = mem();
            seed_legacy(&b, &table(10, ORDERS, &[])).await;
            seed_legacy(&b, &table(10, ORDERS, &["id"])).await;
            let r = registry(&b).await;
            let err = resolve_retained_relation(&r, &key(), &rel(10))
                .await
                .unwrap_err();
            assert!(
                matches!(
                    err,
                    RegistryError::AmbiguousLegacy { matches: 2, .. }
                ),
                "{err:?}"
            );
            assert!(scoped_versions(&r).await.is_empty(), "nothing adopted");
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

    #[test]
    fn test_parse_pattern() {
        assert_eq!(
            parse_pattern("public.users"),
            ("public".into(), "users".into())
        );
        assert_eq!(
            parse_pattern("myschema.*"),
            ("myschema".into(), "*".into())
        );
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

        // Glob `*` triggers LIKE; literal `%` is treated as exact match.
        let q = build_pattern_query("*", "audit*");
        assert!(q.contains("table_schema NOT IN"));
        assert!(q.contains("table_name LIKE 'audit%'"));

        // Literal `%` in table name — exact match, not LIKE.
        let q = build_pattern_query("public", "audit%");
        assert!(q.contains("table_name = 'audit%'"));
    }
}
