//! Stored table definitions shared by the selection cache's entries
//! (`docs/specs/mysql-schema-template-sharing-design.md`, local).
//!
//! Schema-per-tenant catalogs hold many copies of one product schema. A
//! selected schema and a read-ahead version carry, besides their table's
//! own version number and sequence, a definition that is the same for every
//! copy: the stored schema, its column names, its value decoder and its
//! registry hash. The pool holds each distinct definition once, keyed by its
//! exact bytes (the hash and the stored schema's serialization), and the
//! cache's entries point at it. Nothing about a table is shared: two
//! entries share a definition only when those bytes are equal, and every
//! definition object is immutable.

use std::collections::HashMap;
use std::sync::Arc;

use super::mysql_cache_cost::{allocated, buckets_for, table};
use super::mysql_schema_loader::LoadedSchema;

/// One bucket of the pool's map: its key and its slot.
const SLOT: usize = std::mem::size_of::<(Arc<[u8]>, Held)>();
/// An `Arc`'s two counters.
const ARC_HEADER: u64 = 2 * std::mem::size_of::<usize>() as u64;

/// A definition held, the entries holding it and its bytes (the definition
/// and its key, counted once).
#[derive(Debug)]
struct Held {
    shared: Arc<LoadedSchema>,
    refs: usize,
    bytes: u64,
}

/// The definitions the cache's entries hold, each once.
#[derive(Debug, Default)]
pub(crate) struct Definitions {
    by_bytes: HashMap<Arc<[u8]>, Held>,
    /// The definitions' bytes (the map's buckets not included).
    held: u64,
}

/// The exact bytes `l`'s definition is shared under: its registry hash,
/// length-prefixed, then its stored schema's serialization (which decodes
/// back to the same schema, so equal bytes are equal definitions).
pub(crate) fn key_of(l: &LoadedSchema) -> Vec<u8> {
    let mut out = Vec::with_capacity(l.fingerprint.len() + 512);
    out.extend_from_slice(&(l.fingerprint.len() as u64).to_le_bytes());
    out.extend_from_slice(l.fingerprint.as_bytes());
    serde_json::to_writer(&mut out, &*l.schema).expect("a schema serializes");
    out
}

/// `l` with its definition taken from `shared` (its own version number and
/// sequence kept).
fn rebuilt(l: &LoadedSchema, shared: &LoadedSchema) -> LoadedSchema {
    LoadedSchema {
        schema: shared.schema.clone(),
        registry_version: l.registry_version,
        fingerprint: shared.fingerprint.clone(),
        sequence: l.sequence,
        column_names: shared.column_names.clone(),
        decoder: shared.decoder.clone(),
    }
}

/// Whether `a` and `b` have the same definition, field by field.
fn same_definition(a: &LoadedSchema, b: &LoadedSchema) -> bool {
    *a.schema == *b.schema
        && a.fingerprint == b.fingerprint
        && a.column_names == b.column_names
}

impl Definitions {
    /// What one held definition costs: the definition (schema, column
    /// names, fingerprint, decoder), the selected schema it was taken from
    /// (kept with it), and its key's bytes and `Arc`.
    fn cost(key: &[u8], shared: &LoadedSchema) -> u64 {
        super::mysql_cache_cost::definition(shared)
            + super::mysql_cache_cost::loaded_own(shared)
            + allocated(ARC_HEADER + key.len() as u64)
    }

    /// What holding `l`'s definition (under `key`) would add now.
    pub(crate) fn would_add(&self, key: &[u8], l: &LoadedSchema) -> u64 {
        if self.by_bytes.contains_key(key) {
            return 0;
        }
        Self::cost(key, l) + self.growth()
    }

    /// The buckets one more definition would add to the map.
    fn growth(&self) -> u64 {
        let have = self.by_bytes.capacity();
        let need = have.max(self.by_bytes.len() + 1);
        table(buckets_for(need), SLOT) - table(buckets_for(have), SLOT)
    }

    /// Hold `l`'s definition once more: the pooled one if `key`'s is held
    /// (then `l` is rebuilt on it), else `l`'s own. `l` with the pooled
    /// definition, and the key it is held under; `None` (nothing held) only
    /// if equal bytes held a different definition, which they cannot.
    pub(crate) fn hold(
        &mut self,
        key: Vec<u8>,
        l: &Arc<LoadedSchema>,
    ) -> Option<(Arc<LoadedSchema>, Arc<[u8]>)> {
        if let Some(k) =
            self.by_bytes.get_key_value(&*key).map(|(k, _)| k.clone())
        {
            let held = self.by_bytes.get_mut(&k).expect("held");
            if !same_definition(&held.shared, l) {
                debug_assert!(
                    false,
                    "equal definition bytes, unequal definitions"
                );
                return None;
            }
            held.refs += 1;
            let shared = if Arc::ptr_eq(&held.shared.schema, &l.schema)
                && Arc::ptr_eq(&held.shared.decoder, &l.decoder)
                && Arc::ptr_eq(&held.shared.column_names, &l.column_names)
                && Arc::ptr_eq(&held.shared.fingerprint, &l.fingerprint)
            {
                l.clone()
            } else {
                Arc::new(rebuilt(l, &held.shared))
            };
            return Some((shared, k));
        }
        let key: Arc<[u8]> = key.into();
        let bytes = Self::cost(&key, l);
        self.held += bytes;
        self.by_bytes.insert(
            key.clone(),
            Held {
                shared: l.clone(),
                refs: 1,
                bytes,
            },
        );
        Some((l.clone(), key))
    }

    /// One holder of `key`'s definition is gone; the last frees it.
    pub(crate) fn release(&mut self, key: &Arc<[u8]>) {
        let held = self
            .by_bytes
            .get_mut(key)
            .expect("a released definition is held");
        held.refs -= 1;
        if held.refs == 0 {
            let gone = self.by_bytes.remove(key).expect("held");
            self.held -= gone.bytes;
            // The last one gone: the map's buckets go too.
            if self.by_bytes.is_empty() {
                self.by_bytes = HashMap::new();
            }
        }
    }

    /// The definitions' bytes and the map's buckets.
    pub(crate) fn bytes(&self) -> u64 {
        self.held + table(buckets_for(self.by_bytes.capacity()), SLOT)
    }

    /// The definitions held.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.by_bytes.len()
    }

    /// Check the accounting against `holders`, every entry's key and
    /// selected schema (tests): each definition held as often as entries
    /// hold it, its bytes recounted, and each entry pointing at it.
    #[cfg(test)]
    pub(crate) fn audit<'a>(
        &self,
        holders: impl Iterator<Item = (&'a Arc<[u8]>, &'a Arc<LoadedSchema>)>,
    ) {
        let mut refs: HashMap<&[u8], usize> = HashMap::new();
        for (k, l) in holders {
            let held = &self.by_bytes[k];
            assert_eq!(**k, *key_of(l), "an entry's key is its definition's");
            assert!(
                Arc::ptr_eq(&held.shared.schema, &l.schema),
                "schema shared"
            );
            assert!(
                Arc::ptr_eq(&held.shared.decoder, &l.decoder),
                "decoder shared"
            );
            assert!(
                Arc::ptr_eq(&held.shared.column_names, &l.column_names),
                "names shared"
            );
            assert!(
                Arc::ptr_eq(&held.shared.fingerprint, &l.fingerprint),
                "fingerprint shared"
            );
            *refs.entry(&**k).or_default() += 1;
        }
        assert_eq!(refs.len(), self.by_bytes.len(), "definitions held");
        let mut held = 0;
        for (k, h) in &self.by_bytes {
            assert_eq!(refs.get(&**k), Some(&h.refs), "holders");
            assert_eq!(h.bytes, Self::cost(k, &h.shared), "bytes");
            held += h.bytes;
        }
        assert_eq!(held, self.held, "definitions' bytes");
    }
}
