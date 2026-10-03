//! Verified schema-registry scope shared by a source and its schema loaders.
//!
//! Every schema-registry key is qualified by the source's *verified physical
//! lineage* ([`LineageDescriptor`]). The source verifies that lineage against
//! the live server, durably records it ([`establish_scope`]), and only then
//! publishes a [`RegistryScope`] into the [`SharedRegistryScope`] its loaders
//! hold. A loader can only build a registry key from a published scope; before
//! one exists every registry access fails closed with
//! [`RegistryError::NotEstablished`]. There is no fallback to an unqualified
//! key.
//!
//! A lineage change while running (failover to a server with a different
//! identity) re-establishes the scope: the new lineage is persisted first, then
//! published with a new generation, which invalidates loader caches built under
//! the old lineage.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

use deltaforge_core::SourceError;
use storage::ArcStorageBackend;
use storage::adapters::source_lineage::{self, LineageRef};
use storage::adapters::{LineageDescriptor, SchemaKey};

/// Distinct registry outcomes. `Ok(None)` ("schema absent") is not an error;
/// every other outcome has its own variant so callers never conflate a storage
/// failure, a missing lineage, or an ambiguous match with absence.
#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error(
        "schema registry lineage is not established for source '{source_id}': \
         the source has not yet verified its live server identity; refusing \
         registry access (fail-closed)"
    )]
    NotEstablished { source_id: String },

    #[error("schema registry storage failure: {0:#}")]
    Storage(anyhow::Error),

    #[error(
        "source '{source_id}' moved to a new server lineage while a schema was \
         being loaded; the result was discarded rather than attributed to the \
         wrong lineage. Retry."
    )]
    ScopeChanged { source_id: String },

    #[error(
        "source '{source_id}': live lineage {live} does not match the \
         established registry lineage {expected}; refusing to continue on a \
         replaced database (fail-closed)"
    )]
    LineageMismatch {
        source_id: String,
        expected: String,
        live: String,
    },

    #[error(
        "table {table}: no durable schema history is available to decode its \
         retained changes (relation {relation}); cannot decode (fail-closed). \
         Reset the source position past these changes to continue."
    )]
    NoHistory { table: String, relation: String },

    #[error(
        "table {table}: the retained relation ({relation}) does not match any \
         cached, live, or historical schema - the table is absent or was \
         replaced under the same name; refusing to decode (fail-closed)."
    )]
    NoMatch { table: String, relation: String },

    #[error(
        "table {table}: {matches} durable schema versions match the retained \
         relation ({relation}) - ambiguous lineage; refusing to decode to avoid \
         applying the wrong schema (fail-closed)."
    )]
    AmbiguousHistory {
        table: String,
        relation: String,
        matches: usize,
    },

    #[error(
        "table {table}: the retained relation ({relation}) is not in this \
         source's lineage-scoped schema history, and {detail}. Pre-upgrade \
         (unscoped) history cannot be attributed to this database \
         automatically - PostgreSQL relation OIDs are database-local, so a \
         matching OID and column shape do not prove ownership. Refusing to \
         decode (fail-closed); map the pre-upgrade history to this source \
         explicitly with the schema migration command."
    )]
    LegacyOwnershipUnproven {
        table: String,
        relation: String,
        detail: String,
    },
}

impl From<RegistryError> for SourceError {
    fn from(e: RegistryError) -> Self {
        match e {
            RegistryError::Storage(err) => SourceError::Other(
                err.context("schema registry storage failure"),
            ),
            // The connected server is not the verified lineage.
            e @ RegistryError::LineageMismatch { .. } => SourceError::Lineage {
                details: e.to_string().into(),
            },
            e @ (RegistryError::NotEstablished { .. }
            | RegistryError::ScopeChanged { .. }) => {
                SourceError::Incompatible {
                    details: e.to_string().into(),
                }
            }
            e => SourceError::Schema {
                details: e.to_string().into(),
            },
        }
    }
}

/// Whether `err` (anywhere in its chain) says the source's registry lineage is
/// temporarily unavailable: not yet established
/// ([`RegistryError::NotEstablished`]) or changing under the request
/// ([`RegistryError::ScopeChanged`]). Callers should report this as
/// "unavailable, retry" - never as an internal failure and never as "no schema".
pub fn is_lineage_unavailable(err: &anyhow::Error) -> bool {
    err.chain().any(|e| {
        matches!(
            e.downcast_ref::<RegistryError>(),
            Some(
                RegistryError::NotEstablished { .. }
                    | RegistryError::ScopeChanged { .. }
            )
        )
    })
}

/// Single-flight per table for a schema loader: concurrent first uses of one
/// `(schema, table)` share one load. A loader takes the table's flight after a
/// cache miss and checks the cache again before loading, so only the first
/// caller fetches and registers; the others are served from the cache it
/// filled. Holds only the tables being loaded right now.
#[derive(Debug, Default)]
pub(crate) struct LoadFlights {
    flights: std::sync::Mutex<HashMap<(String, String), Flight>>,
}

/// One table's load in progress (gone once every waiter released it).
type Flight = std::sync::Weak<tokio::sync::Mutex<()>>;

impl LoadFlights {
    /// Wait for, then hold, the flight of `key` until the guard drops.
    pub(crate) async fn acquire(
        &self,
        key: &(String, String),
    ) -> tokio::sync::OwnedMutexGuard<()> {
        let flight = {
            let mut flights = self.flights.lock().expect("load flights");
            match flights.get(key).and_then(std::sync::Weak::upgrade) {
                Some(flight) => flight,
                None => {
                    // Forget finished flights first: the map stays the size
                    // of the loads in progress.
                    flights.retain(|_, f| f.strong_count() > 0);
                    let flight = Arc::new(tokio::sync::Mutex::new(()));
                    flights.insert(key.clone(), Arc::downgrade(&flight));
                    flight
                }
            }
        };
        flight.lock_owned().await
    }
}

/// What a cached schema is pinned to: the exact registry version a table
/// resolved to in this run, the sequence its events were stamped with, and
/// its fingerprint. Compact (no schema), kept while the heavyweight entry may
/// be evicted, so an evicted table is rebuilt exactly from durable history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PinnedVersion {
    pub(crate) version: i32,
    pub(crate) sequence: u64,
    pub(crate) fingerprint: Arc<str>,
}

/// A cached loader value: its pin and its approximate resident size.
pub(crate) trait Resident: Clone {
    fn pin(&self) -> PinnedVersion;
    /// Approximate resident bytes (the schema's serialized size).
    fn weight(&self) -> usize;
}

/// How much heavyweight schema state one loader keeps resident.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheBudget {
    pub max_entries: usize,
    pub max_bytes: usize,
}

impl Default for CacheBudget {
    fn default() -> Self {
        Self {
            max_entries: 4_096,
            max_bytes: 64 * 1024 * 1024,
        }
    }
}

struct Slot<V> {
    value: V,
    weight: usize,
    used: std::sync::atomic::AtomicU64,
}

/// A schema-loader cache whose entries always belong to exactly one scope
/// generation. A value may only be inserted if the generation it was produced
/// under is still the published one, so a load that finished after a lineage
/// change can never be served under the new lineage.
///
/// Bounded: beyond its [`CacheBudget`] the least recently used entries are
/// evicted (an entry larger than the whole budget is kept alone). Eviction
/// only drops the heavyweight value: the table's [`PinnedVersion`] stays, so
/// the loader rebuilds exactly that version from durable history, never
/// re-resolving it. Pins go with an explicit removal, a clear or a new
/// generation.
pub(crate) struct ScopedCache<V> {
    generation: u64,
    budget: CacheBudget,
    entries: HashMap<(String, String), Slot<V>>,
    pins: HashMap<(String, String), PinnedVersion>,
    bytes: usize,
    clock: std::sync::atomic::AtomicU64,
    evictions: u64,
}

impl<V> std::fmt::Debug for ScopedCache<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScopedCache")
            .field("generation", &self.generation)
            .field("entries", &self.entries.len())
            .field("bytes", &self.bytes)
            .field("pins", &self.pins.len())
            .finish()
    }
}

impl<V: Resident> Default for ScopedCache<V> {
    fn default() -> Self {
        Self::with_budget(CacheBudget::default())
    }
}

impl<V: Resident> ScopedCache<V> {
    pub(crate) fn with_budget(budget: CacheBudget) -> Self {
        Self {
            generation: 0,
            budget,
            entries: HashMap::new(),
            pins: HashMap::new(),
            bytes: 0,
            clock: Default::default(),
            evictions: 0,
        }
    }

    fn tick(&self) -> u64 {
        self.clock
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            .wrapping_add(1)
    }

    /// The entry for `key`, only if the cache belongs to `published` (the
    /// generation currently published).
    pub(crate) fn get(
        &self,
        published: u64,
        key: &(String, String),
    ) -> Option<V> {
        if published == 0 || self.generation != published {
            return None;
        }
        let slot = self.entries.get(key)?;
        slot.used
            .store(self.tick(), std::sync::atomic::Ordering::Relaxed);
        Some(slot.value.clone())
    }

    /// The version `key` resolved to in `published`, also once its value was
    /// evicted.
    pub(crate) fn pinned(
        &self,
        published: u64,
        key: &(String, String),
    ) -> Option<PinnedVersion> {
        if published == 0 || self.generation != published {
            return None;
        }
        self.pins.get(key).cloned()
    }

    /// Insert `value`, produced under generation `produced`, only if that is
    /// still the `published` generation. Otherwise nothing is inserted and
    /// `false` is returned; the caller must retry under the new scope.
    pub(crate) fn insert_if_current(
        &mut self,
        produced: u64,
        published: u64,
        key: (String, String),
        value: V,
    ) -> bool {
        // Generations only move forward: never let an older generation's value
        // (even one paired with a stale reading of the published generation)
        // replace a newer cache.
        if produced == 0 || produced != published || produced < self.generation
        {
            return false;
        }
        if self.generation != produced {
            self.clear();
            self.generation = produced;
        }
        self.remove_value(&key);
        let weight = value.weight();
        self.pins.insert(key.clone(), value.pin());
        self.bytes += weight;
        self.entries.insert(
            key.clone(),
            Slot {
                value,
                weight,
                used: self.tick().into(),
            },
        );
        self.evict_beyond_budget(&key);
        true
    }

    /// Evict least recently used values (never `keep`, the one just
    /// inserted) until the budget holds.
    fn evict_beyond_budget(&mut self, keep: &(String, String)) {
        while self.entries.len() > 1
            && (self.entries.len() > self.budget.max_entries
                || self.bytes > self.budget.max_bytes)
        {
            let Some(victim) = self
                .entries
                .iter()
                .filter(|(k, _)| *k != keep)
                .min_by_key(|(_, s)| {
                    s.used.load(std::sync::atomic::Ordering::Relaxed)
                })
                .map(|(k, _)| k.clone())
            else {
                break;
            };
            self.remove_value(&victim);
            self.evictions += 1;
        }
    }

    fn remove_value(&mut self, key: &(String, String)) {
        if let Some(slot) = self.entries.remove(key) {
            self.bytes -= slot.weight;
        }
    }

    /// Forget `key` entirely (an explicit reload re-resolves it).
    pub(crate) fn remove(&mut self, key: &(String, String)) {
        self.remove_value(key);
        self.pins.remove(key);
    }

    pub(crate) fn clear(&mut self) {
        self.entries.clear();
        self.pins.clear();
        self.bytes = 0;
    }

    /// Drop entries (and pins) whose key does not satisfy `keep`.
    pub(crate) fn retain(
        &mut self,
        mut keep: impl FnMut(&(String, String)) -> bool,
    ) {
        let drop: Vec<_> =
            self.pins.keys().filter(|k| !keep(k)).cloned().collect();
        for key in drop {
            self.remove(&key);
        }
    }

    /// Keys of the cached entries regardless of generation (failover
    /// reconciliation diffs exactly the tables tracked before a lineage change).
    pub(crate) fn keys_any_generation(&self) -> Vec<(String, String)> {
        self.pins.keys().cloned().collect()
    }

    /// Entries belonging to `published`, for listing.
    pub(crate) fn entries_for(
        &self,
        published: u64,
    ) -> Vec<((String, String), V)> {
        if published == 0 || self.generation != published {
            return Vec::new();
        }
        self.entries
            .iter()
            .map(|(k, s)| (k.clone(), s.value.clone()))
            .collect()
    }

    /// Tables resolved in `published` (resident or evicted).
    pub(crate) fn tables_for(&self, published: u64) -> Vec<(String, String)> {
        if published == 0 || self.generation != published {
            return Vec::new();
        }
        self.pins.keys().cloned().collect()
    }

    /// Resident entries, resident bytes and evictions so far.
    pub(crate) fn usage(&self) -> (usize, usize, u64) {
        (self.entries.len(), self.bytes, self.evictions)
    }
}

/// The verified lineage a source's registry keys are qualified by.
#[derive(Debug, Clone)]
pub struct RegistryScope {
    tenant: String,
    source_id: String,
    lineage: LineageRef,
    /// 0 for a detached scope (predecessor reads); published scopes are >= 1.
    generation: u64,
}

impl RegistryScope {
    /// A scope that is never published: used only to READ a predecessor
    /// lineage (failover reconciliation).
    pub(crate) fn detached(
        tenant: &str,
        source_id: &str,
        lineage: LineageRef,
    ) -> Self {
        Self {
            tenant: tenant.to_string(),
            source_id: source_id.to_string(),
            lineage,
            generation: 0,
        }
    }

    /// The fully-qualified key for `(db, table)` under this lineage. For
    /// PostgreSQL `db` is the schema name; for MySQL it is the database.
    pub fn key(&self, db: &str, table: &str) -> SchemaKey {
        SchemaKey::new(
            self.tenant.as_str(),
            self.source_id.as_str(),
            self.lineage.lineage_hash.as_str(),
            db,
            table,
        )
    }

    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    pub fn source_id(&self) -> &str {
        &self.source_id
    }

    pub fn lineage(&self) -> &LineageRef {
        &self.lineage
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }
}

/// Published scope and the last generation handed out, under ONE lock so the
/// scope and its generation can never be observed out of step.
#[derive(Debug, Default)]
struct ScopeState {
    current: Option<Arc<RegistryScope>>,
    last_generation: u64,
}

/// Handle shared by one source and every schema loader of its pipeline.
#[derive(Debug, Clone, Default)]
pub struct SharedRegistryScope {
    state: Arc<RwLock<ScopeState>>,
    source_id: Arc<str>,
}

impl SharedRegistryScope {
    pub fn new(source_id: &str) -> Self {
        Self {
            state: Arc::default(),
            source_id: source_id.into(),
        }
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, ScopeState> {
        self.state.read().unwrap_or_else(|p| p.into_inner())
    }

    /// Generation of the published scope; 0 until one is established. Read
    /// from the same state as [`Self::current`], so the two always agree.
    pub fn generation(&self) -> u64 {
        self.read().current.as_ref().map_or(0, |s| s.generation)
    }

    /// The published scope, or [`RegistryError::NotEstablished`].
    pub fn current(&self) -> Result<Arc<RegistryScope>, RegistryError> {
        self.read().current.clone().ok_or_else(|| {
            RegistryError::NotEstablished {
                source_id: self.source_id.to_string(),
            }
        })
    }

    fn publish(
        &self,
        tenant: &str,
        source_id: &str,
        lineage: LineageRef,
    ) -> Arc<RegistryScope> {
        let mut state = self.state.write().unwrap_or_else(|p| p.into_inner());
        state.last_generation = state.last_generation.saturating_add(1);
        let scope = Arc::new(RegistryScope {
            tenant: tenant.to_string(),
            source_id: source_id.to_string(),
            lineage,
            generation: state.last_generation,
        });
        state.current = Some(scope.clone());
        scope
    }

    /// Test-only: publish a scope without persisting a lineage record.
    #[cfg(test)]
    pub(crate) fn publish_for_test(
        &self,
        tenant: &str,
        descriptor: LineageDescriptor,
    ) -> Arc<RegistryScope> {
        let source_id = self.source_id.to_string();
        self.publish(tenant, &source_id, LineageRef::new(descriptor))
    }
}

/// Outcome of [`establish_scope`].
#[derive(Debug, Clone)]
pub struct ScopeChange {
    /// The published scope now in effect.
    pub scope: Arc<RegistryScope>,
    /// The predecessor lineage when this call changed the lineage.
    pub previous: Option<RegistryScope>,
}

/// Make `descriptor` (verified against the live server by the caller) the
/// registry lineage of `(tenant, source_id)`: durably record it, then publish
/// it. Fails closed - nothing is published - if the lineage record cannot be
/// read or persisted, so a source never continues on a stale lineage after a
/// replacement has been detected.
///
/// Re-publishing the lineage already in effect keeps the current generation,
/// so loader caches stay warm across reconnects to the same server.
pub async fn establish_scope(
    backend: &ArcStorageBackend,
    shared: &SharedRegistryScope,
    tenant: &str,
    source_id: &str,
    descriptor: LineageDescriptor,
) -> Result<ScopeChange, RegistryError> {
    let established =
        source_lineage::establish(backend, tenant, source_id, descriptor)
            .await
            .map_err(RegistryError::Storage)?;
    let lineage = established.record.current.clone();
    if let Ok(scope) = shared.current()
        && scope.lineage == lineage
        && scope.tenant == tenant
    {
        return Ok(ScopeChange {
            scope,
            previous: None,
        });
    }
    let scope = shared.publish(tenant, source_id, lineage);
    Ok(ScopeChange {
        scope,
        previous: established
            .changed_from
            .map(|prev| RegistryScope::detached(tenant, source_id, prev)),
    })
}

/// The lineage this source ran under immediately before its current one, as a
/// read-only scope, if the lineage has ever changed. Storage failures are
/// errors, never "no predecessor".
pub async fn previous_scope(
    backend: &ArcStorageBackend,
    tenant: &str,
    source_id: &str,
) -> Result<Option<RegistryScope>, RegistryError> {
    Ok(source_lineage::load(backend, tenant, source_id)
        .await
        .map_err(RegistryError::Storage)?
        .and_then(|rec| rec.previous)
        .map(|prev| RegistryScope::detached(tenant, source_id, prev)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage::MemoryStorageBackend;

    /// One table's flight is exclusive while held, and finished flights are
    /// forgotten: the map holds only the loads in progress, however many
    /// tables were loaded.
    #[tokio::test]
    async fn load_flights_are_exclusive_per_table_and_forgotten_when_done() {
        let flights = LoadFlights::default();
        let key = |t: &str| ("s".to_string(), t.to_string());
        let held = flights.acquire(&key("a")).await;
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(50),
                flights.acquire(&key("a")),
            )
            .await
            .is_err(),
            "a second load of the same table waits"
        );
        drop(flights.acquire(&key("b")).await);
        drop(held);
        for n in 0..1_000 {
            drop(flights.acquire(&key(&n.to_string())).await);
        }
        assert_eq!(flights.flights.lock().unwrap().len(), 1);
    }

    fn pg(sysid: u64, dboid: u64) -> LineageDescriptor {
        LineageDescriptor::postgres(sysid, dboid).unwrap()
    }

    fn backend() -> ArcStorageBackend {
        Arc::new(MemoryStorageBackend::new())
    }

    #[test]
    fn unestablished_scope_fails_closed() {
        let shared = SharedRegistryScope::new("src");
        assert_eq!(shared.generation(), 0);
        assert!(matches!(
            shared.current(),
            Err(RegistryError::NotEstablished { .. })
        ));
    }

    #[tokio::test]
    async fn establish_publishes_a_qualified_scope() {
        let b = backend();
        let shared = SharedRegistryScope::new("src");
        let change = establish_scope(&b, &shared, "acme", "src", pg(1, 2))
            .await
            .unwrap();
        assert!(change.previous.is_none());
        assert_eq!(shared.generation(), 1);
        let key = shared.current().unwrap().key("public", "orders");
        assert_eq!(key.lineage_hash, pg(1, 2).lineage_hash());
        assert_eq!(
            (key.tenant.as_str(), key.source_id.as_str()),
            ("acme", "src")
        );
    }

    #[tokio::test]
    async fn same_lineage_keeps_generation() {
        let b = backend();
        let shared = SharedRegistryScope::new("src");
        establish_scope(&b, &shared, "acme", "src", pg(1, 2))
            .await
            .unwrap();
        establish_scope(&b, &shared, "acme", "src", pg(1, 2))
            .await
            .unwrap();
        assert_eq!(
            shared.generation(),
            1,
            "reconnect to the same server keeps caches"
        );
    }

    #[tokio::test]
    async fn lineage_change_bumps_generation_and_reports_predecessor() {
        let b = backend();
        let shared = SharedRegistryScope::new("src");
        establish_scope(&b, &shared, "acme", "src", pg(1, 2))
            .await
            .unwrap();
        let change = establish_scope(&b, &shared, "acme", "src", pg(3, 4))
            .await
            .unwrap();
        assert_eq!(shared.generation(), 2);
        let prev = change.previous.unwrap();
        assert_eq!(prev.lineage().lineage_hash, pg(1, 2).lineage_hash());
        assert_eq!(
            prev.generation(),
            0,
            "predecessor scope is never published"
        );
        let from_record =
            previous_scope(&b, "acme", "src").await.unwrap().unwrap();
        assert_eq!(from_record.lineage(), prev.lineage());
    }

    #[tokio::test]
    async fn persistence_failure_publishes_nothing() {
        use std::sync::atomic::Ordering;
        use storage::adapters::test_util::FaultBackend;
        let f = Arc::new(FaultBackend::new());
        f.fail_kv_put.store(true, Ordering::SeqCst);
        let b: ArcStorageBackend = f.clone();
        let shared = SharedRegistryScope::new("src");
        let err = establish_scope(&b, &shared, "acme", "src", pg(1, 2))
            .await
            .unwrap_err();
        assert!(matches!(err, RegistryError::Storage(_)));
        assert!(matches!(
            shared.current(),
            Err(RegistryError::NotEstablished { .. })
        ));
    }

    #[tokio::test]
    async fn replacement_that_cannot_be_persisted_keeps_old_scope_unpublished()
    {
        use std::sync::atomic::Ordering;
        use storage::adapters::test_util::FaultBackend;
        let f = Arc::new(FaultBackend::new());
        let b: ArcStorageBackend = f.clone();
        let shared = SharedRegistryScope::new("src");
        establish_scope(&b, &shared, "acme", "src", pg(1, 2))
            .await
            .unwrap();
        f.fail_kv_put.store(true, Ordering::SeqCst);
        assert!(
            establish_scope(&b, &shared, "acme", "src", pg(9, 9))
                .await
                .is_err()
        );
        // The replacement was not published; the caller must stop, and the
        // durable record still names the old lineage.
        assert_eq!(
            shared.current().unwrap().lineage().lineage_hash,
            pg(1, 2).lineage_hash()
        );
    }

    #[test]
    fn not_established_is_detectable_through_context() {
        let err = anyhow::Error::new(RegistryError::NotEstablished {
            source_id: "src".into(),
        });
        assert!(is_lineage_unavailable(&err));
        let wrapped = err.context("loading schema for public.orders");
        assert!(is_lineage_unavailable(&wrapped));
        assert!(!is_lineage_unavailable(&anyhow::anyhow!("table not found")));
        let storage =
            anyhow::Error::new(RegistryError::Storage(anyhow::anyhow!("io")));
        assert!(!is_lineage_unavailable(&storage));
    }

    #[tokio::test]
    async fn loaders_report_unestablished_lineage_as_typed_error() {
        use crate::schema_loader::SourceSchemaLoader;
        let registry = storage::DurableSchemaRegistry::for_testing();
        let scope = SharedRegistryScope::new("src");
        // The DSN is never contacted: the scope check comes first.
        let pg = crate::postgres::PostgresSchemaLoader::new(
            "host=127.0.0.1 port=1 user=none dbname=none",
            registry.clone(),
            "acme",
            scope.clone(),
        );
        let my = crate::mysql::MySqlSchemaLoader::new(
            "mysql://none@127.0.0.1:1/none",
            registry,
            "acme",
            scope.clone(),
        );
        let loaders: [&dyn SourceSchemaLoader; 2] = [&pg, &my];
        for loader in loaders {
            assert!(!loader.lineage_established());
            let err = loader.load("public", "orders").await.unwrap_err();
            assert!(is_lineage_unavailable(&err), "{err:#}");
            let err = loader.reload("public", "orders").await.unwrap_err();
            assert!(is_lineage_unavailable(&err), "{err:#}");
            let err = loader.reload_all(&[]).await.unwrap_err();
            assert!(is_lineage_unavailable(&err), "{err:#}");
        }
        scope.publish_for_test("acme", pg_desc());
        assert!(pg.lineage_established() && my.lineage_established());
    }

    fn pg_desc() -> LineageDescriptor {
        pg(1, 2)
    }

    /// A test value: its name is its fingerprint, `size` its weight.
    #[derive(Clone, Debug, PartialEq)]
    struct V(&'static str, usize);

    impl Resident for V {
        fn pin(&self) -> PinnedVersion {
            PinnedVersion {
                version: 1,
                sequence: 1,
                fingerprint: self.0.into(),
            }
        }
        fn weight(&self) -> usize {
            self.1
        }
    }

    impl From<&'static str> for V {
        fn from(s: &'static str) -> Self {
            V(s, 1)
        }
    }

    /// Beyond the budget (entries or bytes) the least recently used value is
    /// evicted, but its pin stays, so the table is rebuilt exactly rather than
    /// re-resolved; a removal, a clear or a new generation drops pins too.
    #[test]
    fn scoped_cache_evicts_lru_values_and_keeps_their_pins() {
        let k = |t: &str| ("s".to_string(), t.to_string());
        let mut cache = ScopedCache::with_budget(CacheBudget {
            max_entries: 2,
            max_bytes: 100,
        });
        assert!(cache.insert_if_current(1, 1, k("a"), V("a", 10)));
        assert!(cache.insert_if_current(1, 1, k("b"), V("b", 10)));
        assert!(cache.get(1, &k("a")).is_some()); // b is now least recent
        assert!(cache.insert_if_current(1, 1, k("c"), V("c", 10)));
        assert!(cache.get(1, &k("b")).is_none(), "LRU evicted");
        assert!(
            cache.get(1, &k("a")).is_some() && cache.get(1, &k("c")).is_some()
        );
        assert_eq!(cache.pinned(1, &k("b")).unwrap().fingerprint.as_ref(), "b");
        assert_eq!(cache.usage(), (2, 20, 1));

        // Bytes bound: a large value evicts the others but is kept alone.
        assert!(cache.insert_if_current(1, 1, k("big"), V("big", 500)));
        assert_eq!(cache.usage().0, 1);
        assert!(cache.get(1, &k("big")).is_some());
        assert_eq!(cache.tables_for(1).len(), 4, "every resolved table pinned");

        cache.remove(&k("b"));
        assert!(cache.pinned(1, &k("b")).is_none());
        assert!(cache.insert_if_current(2, 2, k("a"), V("a2", 1)));
        assert!(cache.pinned(2, &k("c")).is_none(), "new generation");
        assert_eq!(
            cache.pinned(2, &k("a")).unwrap().fingerprint.as_ref(),
            "a2"
        );
    }

    #[test]
    fn scoped_cache_only_accepts_values_from_the_published_generation() {
        let key = ("public".to_string(), "orders".to_string());
        let mut cache = ScopedCache::<V>::default();
        // Produced under generation 1 while 1 is published: accepted.
        assert!(cache.insert_if_current(1, 1, key.clone(), "a".into()));
        assert_eq!(cache.get(1, &key), Some("a".into()));
        // Generation 2 is published: the generation-1 entry is not served...
        assert_eq!(cache.get(2, &key), None);
        // ...and a late generation-1 value is rejected.
        assert!(!cache.insert_if_current(1, 2, key.clone(), "stale".into()));
        // A generation-2 value replaces everything from generation 1.
        assert!(cache.insert_if_current(2, 2, key.clone(), "b".into()));
        assert_eq!(cache.get(2, &key), Some("b".into()));
        // Once the cache is at generation 2, a late generation-1 value is still
        // rejected even if it claims generation 1 is published (stale reader).
        assert!(!cache.insert_if_current(1, 1, key.clone(), "stale".into()));
        assert_eq!(cache.get(2, &key), Some("b".into()));
        // Unpublished (0) is never cached or served.
        assert!(!cache.insert_if_current(0, 0, key.clone(), "x".into()));
        assert_eq!(cache.get(0, &key), None);
    }

    #[test]
    fn published_scope_and_generation_always_agree() {
        let shared = SharedRegistryScope::new("src");
        assert_eq!(shared.generation(), 0);
        for i in 1..=5u64 {
            let s = shared.publish_for_test("acme", pg(i, i));
            assert_eq!(s.generation(), i);
            assert_eq!(
                shared.generation(),
                shared.current().unwrap().generation()
            );
        }
    }

    #[test]
    fn outcomes_map_to_distinct_source_errors() {
        let storage: SourceError =
            RegistryError::Storage(anyhow::anyhow!("io")).into();
        assert!(matches!(storage, SourceError::Other(_)));
        let missing: SourceError = RegistryError::NotEstablished {
            source_id: "s".into(),
        }
        .into();
        assert!(matches!(missing, SourceError::Incompatible { .. }));
        let moved: SourceError = RegistryError::LineageMismatch {
            source_id: "s".into(),
            expected: "a".into(),
            live: "b".into(),
        }
        .into();
        assert!(matches!(moved, SourceError::Lineage { .. }));
        let ambiguous: SourceError = RegistryError::LegacyOwnershipUnproven {
            table: "t".into(),
            relation: "r".into(),
            detail: "2 pre-upgrade versions match".into(),
        }
        .into();
        assert!(matches!(ambiguous, SourceError::Schema { .. }));
    }
}
