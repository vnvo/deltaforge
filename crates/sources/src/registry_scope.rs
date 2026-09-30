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

use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

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
        "table {table}: {matches} pre-upgrade (unscoped) schema versions match \
         the retained relation ({relation}); their ownership cannot be proven, \
         so none is adopted (fail-closed). Map the legacy history explicitly \
         with the schema migration command."
    )]
    AmbiguousLegacy {
        table: String,
        relation: String,
        matches: usize,
    },
}

impl From<RegistryError> for SourceError {
    fn from(e: RegistryError) -> Self {
        match e {
            RegistryError::Storage(err) => SourceError::Other(
                err.context("schema registry storage failure"),
            ),
            e @ (RegistryError::NotEstablished { .. }
            | RegistryError::LineageMismatch { .. }) => {
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

/// Whether `err` (anywhere in its chain) is [`RegistryError::NotEstablished`]:
/// the source has not yet verified its server, so schemas are temporarily
/// unavailable. Callers should report this as "unavailable, retry" - never as
/// an internal failure and never as "no schema".
pub fn is_lineage_not_established(err: &anyhow::Error) -> bool {
    err.chain().any(|e| {
        matches!(
            e.downcast_ref::<RegistryError>(),
            Some(RegistryError::NotEstablished { .. })
        )
    })
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

#[derive(Debug, Default)]
struct Inner {
    current: RwLock<Option<Arc<RegistryScope>>>,
    generation: AtomicU64,
}

/// Handle shared by one source and every schema loader of its pipeline.
#[derive(Debug, Clone, Default)]
pub struct SharedRegistryScope {
    inner: Arc<Inner>,
    source_id: Arc<str>,
}

impl SharedRegistryScope {
    pub fn new(source_id: &str) -> Self {
        Self {
            inner: Arc::default(),
            source_id: source_id.into(),
        }
    }

    /// Generation of the published scope; 0 until one is established. Cheap
    /// (one atomic load) so loaders can validate cache entries per event.
    pub fn generation(&self) -> u64 {
        self.inner.generation.load(Ordering::Acquire)
    }

    /// The published scope, or [`RegistryError::NotEstablished`].
    pub fn current(&self) -> Result<Arc<RegistryScope>, RegistryError> {
        self.inner
            .current
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
            .ok_or_else(|| RegistryError::NotEstablished {
                source_id: self.source_id.to_string(),
            })
    }

    fn publish(
        &self,
        tenant: &str,
        source_id: &str,
        lineage: LineageRef,
    ) -> Arc<RegistryScope> {
        let mut slot = self
            .inner
            .current
            .write()
            .unwrap_or_else(|p| p.into_inner());
        let generation = self
            .inner
            .generation
            .load(Ordering::Acquire)
            .saturating_add(1);
        let scope = Arc::new(RegistryScope {
            tenant: tenant.to_string(),
            source_id: source_id.to_string(),
            lineage,
            generation,
        });
        *slot = Some(scope.clone());
        self.inner.generation.store(generation, Ordering::Release);
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
        assert!(is_lineage_not_established(&err));
        let wrapped = err.context("loading schema for public.orders");
        assert!(is_lineage_not_established(&wrapped));
        assert!(!is_lineage_not_established(&anyhow::anyhow!(
            "table not found"
        )));
        let storage =
            anyhow::Error::new(RegistryError::Storage(anyhow::anyhow!("io")));
        assert!(!is_lineage_not_established(&storage));
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
            assert!(is_lineage_not_established(&err), "{err:#}");
            let err = loader.reload("public", "orders").await.unwrap_err();
            assert!(is_lineage_not_established(&err), "{err:#}");
            let err = loader.reload_all(&[]).await.unwrap_err();
            assert!(is_lineage_not_established(&err), "{err:#}");
        }
        scope.publish_for_test("acme", pg_desc());
        assert!(pg.lineage_established() && my.lineage_established());
    }

    fn pg_desc() -> LineageDescriptor {
        pg(1, 2)
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
        let ambiguous: SourceError = RegistryError::AmbiguousLegacy {
            table: "t".into(),
            relation: "r".into(),
            matches: 2,
        }
        .into();
        assert!(matches!(ambiguous, SourceError::Schema { .. }));
    }
}
