//! The pipeline's schema context: an event's schema reference resolved to
//! the declared column types of exactly that registered version, for the
//! processors (see `deltaforge_core::canonical::schema`).
//!
//! Versions are read through the source's schema loader and typed by the
//! source's own type mapping (`sources::declared_types`). Resolved versions
//! are immutable, so they are cached by their exact (table, fingerprint,
//! sequence) in a bounded cache (oldest evicted first).

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use async_trait::async_trait;
use deltaforge_core::canonical::schema::{
    ColumnTypes, SchemaContext, SchemaContextError, SchemaRef,
};
use parking_lot::Mutex;
use sources::ArcSchemaLoader;

/// Resolved versions kept at most.
pub const DEFAULT_CAPACITY: usize = 4096;

/// A bounded first-in-first-out cache of resolved versions.
struct Bounded {
    capacity: usize,
    map: HashMap<SchemaRef, Arc<ColumnTypes>>,
    order: VecDeque<SchemaRef>,
}

impl Bounded {
    fn get(&self, r: &SchemaRef) -> Option<Arc<ColumnTypes>> {
        self.map.get(r).cloned()
    }

    fn insert(&mut self, r: SchemaRef, t: Arc<ColumnTypes>) {
        if self.map.insert(r.clone(), t).is_none() {
            self.order.push_back(r);
            while self.order.len() > self.capacity {
                if let Some(old) = self.order.pop_front() {
                    self.map.remove(&old);
                }
            }
        }
    }
}

/// The schema context of a pipeline whose source has a schema loader.
pub struct LoaderSchemaContext {
    loader: ArcSchemaLoader,
    cache: Mutex<Bounded>,
}

impl LoaderSchemaContext {
    pub fn new(loader: ArcSchemaLoader, capacity: usize) -> Self {
        Self {
            loader,
            cache: Mutex::new(Bounded {
                capacity: capacity.max(1),
                map: HashMap::new(),
                order: VecDeque::new(),
            }),
        }
    }

    #[cfg(test)]
    fn cached(&self) -> usize {
        self.cache.lock().map.len()
    }
}

#[async_trait]
impl SchemaContext for LoaderSchemaContext {
    async fn resolve(
        &self,
        r: &SchemaRef,
    ) -> Result<Arc<ColumnTypes>, SchemaContextError> {
        if let Some(t) = self.cache.lock().get(r) {
            return Ok(t);
        }
        let table = format!("{}.{}", r.namespace, r.table);
        let loaded = match self
            .loader
            .load_version(&r.namespace, &r.table, &r.fingerprint, r.sequence)
            .await
        {
            Ok(Some(loaded)) => loaded,
            Ok(None) => {
                return Err(SchemaContextError::Conflict(format!(
                    "{table}: schema version {} is not registered",
                    r.fingerprint
                )));
            }
            Err(e)
                if e.downcast_ref::<sources::schema_loader::SchemaVersionConflict>()
                    .is_some() =>
            {
                return Err(SchemaContextError::Conflict(format!("{table}: {e:#}")));
            }
            Err(e) => {
                return Err(SchemaContextError::Unavailable(format!("{table}: {e:#}")));
            }
        };
        if loaded.fingerprint != r.fingerprint {
            return Err(SchemaContextError::Conflict(format!(
                "{table}: loaded version {} for reference {}",
                loaded.fingerprint, r.fingerprint
            )));
        }
        let types = sources::declared_types::column_types(
            &r.connector,
            &loaded.schema_json,
            r.fingerprint.as_str().into(),
            r.sequence,
        )
        .map_err(|e| {
            SchemaContextError::Conflict(format!(
                "{table}: incompatible source metadata: {e:#}"
            ))
        })?;
        let types = Arc::new(types);
        self.cache.lock().insert(r.clone(), Arc::clone(&types));
        Ok(types)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_cache_is_bounded_oldest_first() {
        let mut c = Bounded {
            capacity: 2,
            map: HashMap::new(),
            order: VecDeque::new(),
        };
        let r = |fp: &str| SchemaRef {
            connector: "mysql".into(),
            database: "d".into(),
            namespace: "d".into(),
            table: "t".into(),
            fingerprint: fp.into(),
            sequence: Some(1),
        };
        let t = Arc::new(ColumnTypes::new("x".into(), Some(1), vec![]));
        c.insert(r("a"), t.clone());
        c.insert(r("b"), t.clone());
        c.insert(r("a"), t.clone());
        c.insert(r("c"), t.clone());
        assert!(c.get(&r("a")).is_none(), "oldest evicted");
        assert!(c.get(&r("b")).is_some() && c.get(&r("c")).is_some());
        assert_eq!(c.map.len(), 2);
        // The exact sequence is part of the key.
        let mut other = r("c");
        other.sequence = Some(2);
        assert!(c.get(&other).is_none());
        let _ = LoaderSchemaContext::cached;
    }
}
