//! A byte-bounded cache of Avro schema state (prepared schemas, registered
//! IDs, built schema JSON), with second-chance (CLOCK) eviction.
//!
//! Every entry can be rebuilt (from the source's schema history and the
//! Schema Registry), so the cache bounds memory, never correctness: an
//! evicted entry is resolved again on its next use. Lookups take a shared
//! lock and mark the entry used; an insert past the bound evicts entries
//! not used since the clock hand last passed them.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use parking_lot::RwLock;

/// The bytes a cache may hold and the bytes it holds. Entries may charge
/// more after their insert (see [`Budget::try_charge`]).
#[derive(Debug)]
pub struct Budget {
    limit: u64,
    used: AtomicU64,
}

impl Budget {
    pub fn new(limit: Option<u64>) -> Arc<Self> {
        Arc::new(Self {
            limit: limit.unwrap_or(u64::MAX),
            used: AtomicU64::new(0),
        })
    }

    /// The bound (`u64::MAX` when unbounded).
    pub fn limit(&self) -> u64 {
        self.limit
    }

    /// The bytes charged.
    pub fn used(&self) -> u64 {
        self.used.load(Ordering::Relaxed)
    }

    /// Charge `bytes` if they fit within the bound.
    pub fn try_charge(&self, bytes: u64) -> bool {
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |u| {
                u.checked_add(bytes).filter(|n| *n <= self.limit)
            })
            .is_ok()
    }

    fn charge(&self, bytes: u64) {
        self.used.fetch_add(bytes, Ordering::AcqRel);
    }

    pub(crate) fn release(&self, bytes: u64) {
        let _ =
            self.used
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |u| {
                    Some(u.saturating_sub(bytes))
                });
    }
}

/// A cached value: what it charged after its insert, released when it is
/// evicted (and never charged again afterwards).
pub trait Evict {
    /// Stop charging the budget; the bytes charged after the insert.
    fn evict(&self) -> u64 {
        0
    }
}

impl Evict for u32 {}
impl Evict for Arc<str> {}

struct Slot<V> {
    value: V,
    bytes: u64,
    used: AtomicBool,
}

struct Inner<V> {
    map: HashMap<Arc<str>, Slot<V>>,
    ring: VecDeque<Arc<str>>,
}

/// A byte-bounded map from string keys to `V`.
pub struct SchemaCache<V> {
    budget: Arc<Budget>,
    inner: RwLock<Inner<V>>,
    evictions: AtomicU64,
}

/// What a cache entry costs beyond its value: its key (twice: map and
/// ring), the map bucket, the ring slot and the slot's fields.
pub fn entry_overhead(key: &str) -> u64 {
    (2 * key.len()
        + 2 * std::mem::size_of::<Arc<str>>()
        + 2 * 16 // two `Arc<str>` headers (strong and weak counts)
        + std::mem::size_of::<Slot<()>>()
        + 2 * std::mem::size_of::<usize>()) as u64
}

impl<V: Clone + Evict> SchemaCache<V> {
    pub fn new(budget: Arc<Budget>) -> Self {
        Self {
            budget,
            inner: RwLock::new(Inner {
                map: HashMap::new(),
                ring: VecDeque::new(),
            }),
            evictions: AtomicU64::new(0),
        }
    }

    pub fn budget(&self) -> &Arc<Budget> {
        &self.budget
    }

    /// The value of `key`, marked used.
    pub fn get(&self, key: &str) -> Option<V> {
        let inner = self.inner.read();
        let slot = inner.map.get(key)?;
        if !slot.used.load(Ordering::Relaxed) {
            slot.used.store(true, Ordering::Relaxed);
        }
        Some(slot.value.clone())
    }

    /// Insert `value` for `key`, charging `bytes` (its estimated size, the
    /// key's included), and evict until the budget holds what is charged.
    /// The new entry itself is never evicted by its own insert, so an entry
    /// larger than the whole bound is still cached alone. A value already
    /// cached for `key` is kept and returned instead.
    pub fn insert(&self, key: &str, value: V, bytes: u64) -> V {
        let mut inner = self.inner.write();
        if let Some(slot) = inner.map.get(key) {
            slot.used.store(true, Ordering::Relaxed);
            return slot.value.clone();
        }
        let key: Arc<str> = key.into();
        self.budget.charge(bytes);
        inner.map.insert(
            key.clone(),
            Slot {
                value: value.clone(),
                bytes,
                used: AtomicBool::new(false),
            },
        );
        inner.ring.push_back(key.clone());
        // Each entry gets one second chance per pass: two passes at most.
        let mut budget_steps = 2 * inner.ring.len();
        while self.budget.used() > self.budget.limit && budget_steps > 0 {
            budget_steps -= 1;
            let Some(k) = inner.ring.pop_front() else {
                break;
            };
            if Arc::ptr_eq(&k, &key) {
                inner.ring.push_back(k);
                if inner.ring.len() == 1 {
                    break;
                }
                continue;
            }
            let second_chance = inner
                .map
                .get(&k)
                .is_some_and(|s| s.used.swap(false, Ordering::Relaxed));
            if second_chance {
                inner.ring.push_back(k);
                continue;
            }
            if let Some(slot) = inner.map.remove(&k) {
                self.budget.release(slot.bytes + slot.value.evict());
                self.evictions.fetch_add(1, Ordering::Relaxed);
            }
        }
        value
    }

    /// Drop every entry.
    pub fn clear(&self) {
        let mut inner = self.inner.write();
        for (_, slot) in inner.map.drain() {
            self.budget.release(slot.bytes + slot.value.evict());
        }
        inner.ring.clear();
    }

    /// Entries evicted so far (a clear evicts none).
    pub fn evictions(&self) -> u64 {
        self.evictions.load(Ordering::Relaxed)
    }

    pub fn len(&self) -> usize {
        self.inner.read().map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// The schema cache bound of an Avro sink that sets none: a sixth of the
/// process's memory limit (its cgroup's), within 64 MiB to 4 GiB; 1 GiB when
/// no limit is set. Measured at a 6 GiB limit with 40,000 tables in turn:
/// a 1 GiB bound peaked at 2.2 GiB, a 3 GiB bound at 5.4 GiB (allocator
/// overhead grows with eviction churn).
pub fn default_schema_cache_bytes() -> u64 {
    static DEFAULT: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *DEFAULT.get_or_init(|| default_bound(crate::memory_limit::memory_limit()))
}

const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;

fn default_bound(memory_limit: Option<u64>) -> u64 {
    match memory_limit {
        Some(limit) => (limit / 6).clamp(64 * MIB, 4 * GIB),
        None => GIB,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_bound_is_a_sixth_of_the_memory_limit_within_its_range() {
        assert_eq!(default_bound(Some(6 * GIB)), GIB);
        assert_eq!(default_bound(Some(12 * GIB)), 2 * GIB);
        assert_eq!(default_bound(Some(256 * MIB)), 64 * MIB, "floor");
        assert_eq!(default_bound(Some(64 * GIB)), 4 * GIB, "cap");
        assert_eq!(default_bound(None), GIB, "no limit");
    }

    fn cache(limit: u64) -> SchemaCache<u32> {
        SchemaCache::new(Budget::new(Some(limit)))
    }

    #[test]
    fn holds_entries_within_the_bound_and_evicts_past_it() {
        let c = cache(300);
        for (i, k) in ["a", "b", "c"].iter().enumerate() {
            c.insert(k, i as u32, 100);
        }
        assert_eq!(c.len(), 3);
        assert_eq!(c.budget().used(), 300);
        c.insert("d", 3, 100);
        assert_eq!(c.len(), 3);
        assert_eq!(c.budget().used(), 300);
        assert_eq!(c.evictions(), 1);
        // The oldest entry, never used, went first.
        assert_eq!(c.get("a"), None);
        assert_eq!(c.get("d"), Some(3));
    }

    #[test]
    fn a_used_entry_gets_a_second_chance() {
        let c = cache(300);
        for (i, k) in ["a", "b", "c"].iter().enumerate() {
            c.insert(k, i as u32, 100);
        }
        assert_eq!(c.get("a"), Some(0));
        c.insert("d", 3, 100);
        assert_eq!(c.get("a"), Some(0), "used since its insert: kept");
        assert_eq!(c.get("b"), None, "the oldest unused entry is evicted");
    }

    #[test]
    fn an_entry_larger_than_the_bound_is_cached_alone() {
        let c = cache(100);
        c.insert("a", 0, 50);
        c.insert("big", 1, 500);
        assert_eq!(c.get("big"), Some(1));
        assert_eq!(c.get("a"), None);
        assert_eq!(c.budget().used(), 500);
        c.insert("b", 2, 50);
        assert_eq!(c.get("big"), None, "evicted by the next insert");
        assert_eq!(c.budget().used(), 50);
    }

    #[test]
    fn an_existing_key_keeps_its_value_and_charge() {
        let c = cache(1000);
        assert_eq!(c.insert("a", 1, 100), 1);
        assert_eq!(c.insert("a", 2, 100), 1);
        assert_eq!(c.budget().used(), 100);
    }

    #[test]
    fn eviction_releases_what_the_value_charged_after_its_insert() {
        #[derive(Clone)]
        struct Grows(Arc<AtomicU64>);
        impl Evict for Grows {
            fn evict(&self) -> u64 {
                self.0.swap(0, Ordering::AcqRel)
            }
        }
        let budget = Budget::new(Some(1000));
        let c = SchemaCache::new(budget.clone());
        let extra = Arc::new(AtomicU64::new(0));
        c.insert("a", Grows(extra.clone()), 100);
        assert!(budget.try_charge(200));
        extra.fetch_add(200, Ordering::AcqRel);
        assert!(!budget.try_charge(701), "past the bound");
        assert_eq!(budget.used(), 300);
        c.clear();
        assert_eq!(budget.used(), 0);
        assert!(c.is_empty());
    }

    #[test]
    fn unbounded_never_evicts() {
        let c: SchemaCache<u32> = SchemaCache::new(Budget::new(None));
        for i in 0..1000 {
            c.insert(&i.to_string(), i, 1 << 40);
        }
        assert_eq!(c.len(), 1000);
        assert_eq!(c.evictions(), 0);
    }
}
