//! The selection cache's entries by database and table: what a barrier,
//! or a table's invalidation, visits instead of the whole cache
//! (`docs/design/selection-cache-diagnosis.md`, section 7).
//!
//! A table key is `v/tenant/source/lineage/db/table` with every segment
//! percent-encoded, so no segment holds a `/`: its database's barrier
//! stream is the key up to its last `/`, and its lineage's is a prefix of
//! that. A barrier on stream `s` applies to the entries whose table key
//! starts with `s`. Under database stream `d` that is all of them when `d`
//! starts with `s`, those whose table key starts with `s` when `s` starts
//! with `d`, and none otherwise (the two differ before either ends). So
//! visiting the databases, one map entry each, finds exactly the entries a
//! scan of the whole cache finds.
//!
//! Each table's keys are a vector, and each entry keeps its position there
//! (its slot): a removal swaps the last key into it, and the moved key's
//! entry is told its new slot, so a table with a long history costs no more
//! per removal than a small one. A table's string is held once, shared by
//! its keys, and counted here, with the maps' buckets and the vectors at
//! capacity, in the cache's estimate.

use std::collections::HashMap;
use std::mem::size_of;
use std::sync::Arc;

use super::mysql_cache_cost::{allocated, buckets_for, shared_str, table};
use super::mysql_selection::CacheKey;

/// One key in a table's vector.
const KEY: usize = size_of::<CacheKey>();
/// One bucket of a database's map of tables.
const TABLE_SLOT: usize = size_of::<(Arc<str>, Vec<CacheKey>)>();
/// One bucket of the map of databases.
const DB_SLOT: usize = size_of::<(Arc<str>, Db)>();
/// The fewest keys a table's vector holds once it has one (the standard
/// library's first allocation for elements of this size).
const FIRST_KEYS: usize = 4;

/// The database barrier stream of a table key: the key up to its last
/// `/` (empty for a key without one).
pub(super) fn db_stream(table: &str) -> &str {
    table.rfind('/').map_or("", |i| &table[..=i])
}

/// The slots a hash table of `buckets` buckets can fill (hashbrown: all
/// but one below 8 buckets, seven eighths from 8).
fn full(buckets: usize) -> usize {
    if buckets < 8 {
        buckets.saturating_sub(1)
    } else {
        buckets / 8 * 7
    }
}

/// A map's buckets after an insert, from `known` before it: a map grows
/// only on insert and never shrinks, and right after growing its capacity
/// is the new table's full capacity.
fn tracked(capacity: usize, known: usize) -> usize {
    if capacity > full(known) {
        buckets_for(capacity)
    } else {
        known
    }
}

/// What one more insert into a map may add to its table: nothing while
/// it has room left, else (at most) a resize past its full capacity. Room
/// taken by removed entries' markers is not counted as room, so this is an
/// upper bound (the insert may reuse such a slot, or rehash in place).
fn map_growth(len: usize, capacity: usize, buckets: usize, slot: usize) -> u64 {
    if capacity > len {
        return 0;
    }
    table(buckets_for(len.max(full(buckets)) + 1), slot) - table(buckets, slot)
}

/// A table's vector of keys at `capacity`.
fn keys(capacity: usize) -> u64 {
    allocated((capacity * KEY) as u64)
}

#[derive(Debug, Default)]
struct Db {
    tables: HashMap<Arc<str>, Vec<CacheKey>>,
    /// The tables map's buckets.
    buckets: usize,
}

/// The cache's keys by database stream and table key, with the estimated
/// bytes it allocates beyond the keys themselves: the maps' buckets, the
/// database streams' and tables' strings (once each) and the tables'
/// vectors at capacity.
#[derive(Debug, Default)]
pub(super) struct Index {
    dbs: HashMap<Arc<str>, Db>,
    /// The databases map's buckets.
    buckets: usize,
    bytes: u64,
}

impl Index {
    /// The estimated bytes the index allocates.
    pub(super) fn bytes(&self) -> u64 {
        self.bytes
    }

    /// What indexing `key` may add (an upper bound; see [`map_growth`]).
    pub(super) fn growth(&self, key: &CacheKey) -> u64 {
        let t = key.table();
        let d = db_stream(t);
        let new_table = shared_str(t) + keys(FIRST_KEYS);
        match self.dbs.get(d) {
            None => {
                map_growth(
                    self.dbs.len(),
                    self.dbs.capacity(),
                    self.buckets,
                    DB_SLOT,
                ) + shared_str(d)
                    + table(buckets_for(1), TABLE_SLOT)
                    + new_table
            }
            Some(db) => match db.tables.get(t) {
                None => {
                    map_growth(
                        db.tables.len(),
                        db.tables.capacity(),
                        db.buckets,
                        TABLE_SLOT,
                    ) + new_table
                }
                Some(v) if v.len() == v.capacity() => {
                    keys((2 * v.capacity()).max(FIRST_KEYS))
                        - keys(v.capacity())
                }
                Some(_) => 0,
            },
        }
    }

    /// Index `key`: the key to cache, sharing its table's string with the
    /// table's other keys, and its slot.
    pub(super) fn insert(&mut self, key: CacheKey) -> (CacheKey, u32) {
        let name = {
            let t = key.table();
            let d = db_stream(t);
            if !self.dbs.contains_key(d) {
                self.dbs.insert(d.into(), Db::default());
                let b = tracked(self.dbs.capacity(), self.buckets);
                self.bytes += table(b, DB_SLOT) - table(self.buckets, DB_SLOT)
                    + shared_str(d);
                self.buckets = b;
            }
            let db = self.dbs.get_mut(d).expect("just inserted");
            match db.tables.get_key_value(t) {
                Some((name, _)) => name.clone(),
                None => {
                    let name: Arc<str> = t.into();
                    db.tables.insert(name.clone(), Vec::new());
                    let b = tracked(db.tables.capacity(), db.buckets);
                    self.bytes += table(b, TABLE_SLOT)
                        - table(db.buckets, TABLE_SLOT)
                        + shared_str(t);
                    db.buckets = b;
                    name
                }
            }
        };
        let key = key.with_table(name);
        let t = key.table();
        let v = self
            .dbs
            .get_mut(db_stream(t))
            .and_then(|db| db.tables.get_mut(t))
            .expect("just inserted");
        let before = v.capacity();
        v.push(key.clone());
        self.bytes += keys(v.capacity()) - keys(before);
        let slot = u32::try_from(v.len() - 1).expect("a table's keys fit u32");
        (key, slot)
    }

    /// Unindex `key`, held at `slot`. The key moved into that slot, if any:
    /// its entry's slot is `slot` now. A table's vector a quarter full or
    /// less (of at least 16) is shrunk to twice its keys; a table without
    /// keys, and a database without tables, are dropped.
    pub(super) fn remove(
        &mut self,
        key: &CacheKey,
        slot: u32,
    ) -> Option<CacheKey> {
        let t = key.table();
        let d = db_stream(t);
        let slot = slot as usize;
        let db = self.dbs.get_mut(d).expect("an indexed key's database");
        let v = db.tables.get_mut(t).expect("an indexed key's table");
        assert!(v.get(slot) == Some(key), "{key:?} not at slot {slot}");
        let before = v.capacity();
        v.swap_remove(slot);
        let moved = v.get(slot).cloned();
        if !v.is_empty() {
            if before >= 16 && v.len() * 4 <= before {
                v.shrink_to(v.len() * 2);
            }
            self.bytes -= keys(before) - keys(v.capacity());
            return moved;
        }
        db.tables.remove(t);
        self.bytes -= keys(before) + shared_str(t);
        if db.tables.is_empty() {
            self.bytes -= table(db.buckets, TABLE_SLOT) + shared_str(d);
            self.dbs.remove(d);
            if self.dbs.is_empty() {
                // Its buckets too: an empty index holds nothing.
                self.bytes -= table(self.buckets, DB_SLOT);
                self.dbs = HashMap::new();
                self.buckets = 0;
            }
        }
        moved
    }

    /// Whether a barrier on `stream` applies to every entry (also when
    /// there is none).
    pub(super) fn covers_all(&self, stream: &str) -> bool {
        self.dbs.keys().all(|d| d.starts_with(stream))
    }

    /// The keys of the entries a barrier on `stream` applies to: exactly
    /// those whose table key starts with it.
    pub(super) fn reached(&self, stream: &str) -> Vec<CacheKey> {
        let mut out = Vec::new();
        for (d, db) in &self.dbs {
            if d.starts_with(stream) {
                out.extend(db.tables.values().flatten().cloned());
            } else if stream.starts_with(&**d) {
                for (t, v) in &db.tables {
                    if t.starts_with(stream) {
                        out.extend(v.iter().cloned());
                    }
                }
            }
        }
        out
    }

    /// The keys of the table `table`'s entries.
    pub(super) fn of_table(&self, table: &str) -> Vec<CacheKey> {
        self.dbs
            .get(db_stream(table))
            .and_then(|db| db.tables.get(table))
            .cloned()
            .unwrap_or_default()
    }

    /// Recount the index against `entries`, each key with its slot, and
    /// check its running bytes: every key indexed once, at its slot, under
    /// its table and database, sharing its table's string; no empty table
    /// or database (tests).
    #[cfg(test)]
    pub(super) fn audit<'a>(
        &self,
        entries: impl Iterator<Item = (&'a CacheKey, u32)>,
    ) {
        let mut n = 0;
        for (key, slot) in entries {
            n += 1;
            let t = key.table();
            let (name, v) = self.dbs[db_stream(t)]
                .tables
                .get_key_value(t)
                .unwrap_or_else(|| panic!("{key:?} not indexed"));
            assert_eq!(v.get(slot as usize), Some(key), "{key:?} slot");
            assert!(
                key.shares_table(name),
                "{key:?} shares its table's string"
            );
        }
        let mut bytes = table(self.buckets, DB_SLOT);
        let mut indexed = 0;
        for (d, db) in &self.dbs {
            assert!(!db.tables.is_empty(), "{d} without tables");
            assert_eq!(db.buckets, tracked(db.tables.capacity(), db.buckets));
            bytes += shared_str(d) + table(db.buckets, TABLE_SLOT);
            for (t, v) in &db.tables {
                assert!(!v.is_empty(), "{t} without keys");
                assert_eq!(db_stream(t), &**d);
                indexed += v.len();
                bytes += shared_str(t) + keys(v.capacity());
            }
        }
        assert_eq!(indexed, n, "keys indexed");
        assert_eq!(bytes, self.bytes, "index bytes");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_alloc::held;
    use rand::{Rng, SeedableRng, seq::SliceRandom};
    use std::collections::HashSet;

    fn table_key(db: usize, t: usize) -> String {
        format!("1/acme/s/0123456789abcdef/db{db}/t{t}")
    }

    fn keys_of(t: &str, versions: i32, selections: usize) -> Vec<CacheKey> {
        let t: Arc<str> = t.into();
        let mut v = vec![CacheKey::Timeline(t.clone())];
        v.extend((1..=versions).map(|n| CacheKey::Version(t.clone(), n)));
        v.extend((0..selections).map(|s| {
            CacheKey::Selection(
                t.clone(),
                format!("decisive-{s}").into(),
                "sig".into(),
            )
        }));
        v
    }

    /// The index with what the cache would hold: each key and its slot.
    struct Indexed {
        index: Index,
        slots: HashMap<CacheKey, u32>,
    }

    impl Indexed {
        fn new() -> Self {
            Self {
                index: Index::default(),
                slots: HashMap::new(),
            }
        }

        fn insert(&mut self, key: CacheKey) {
            let (key, slot) = self.index.insert(key);
            self.slots.insert(key, slot);
        }

        fn remove(&mut self, key: &CacheKey) {
            let slot = self.slots.remove(key).expect("held");
            if let Some(moved) = self.index.remove(key, slot) {
                *self.slots.get_mut(&moved).expect("held") = slot;
            }
        }

        fn audit(&self) {
            self.index.audit(self.slots.iter().map(|(k, s)| (k, *s)));
        }
    }

    /// The keys with the database stream of their table, and an index's
    /// estimate equals what it allocates (jemalloc, this thread): over many
    /// small tables, a long history, removals that shrink a vector and
    /// removals that drop tables and databases.
    #[test]
    fn the_index_allocates_its_estimate() {
        let mut keys = Vec::new();
        for t in 0..3_000 {
            keys.extend(keys_of(&table_key(t % 30, t), 1, 0));
        }
        keys.extend(keys_of(&table_key(0, 99_999), 2_000, 0));
        // The keys exist before the index does; it holds its own string
        // per database and table, and its maps and vectors.
        let mut index = Index::default();
        let mut slots = Vec::with_capacity(keys.len());
        let before = held();
        for k in &keys {
            slots.push(index.insert(k.clone()).1);
        }
        assert_eq!(held() - before, index.bytes() as i64, "filled");
        let mut rng = rand::rngs::StdRng::seed_from_u64(1);
        let mut order: Vec<usize> = (0..keys.len()).collect();
        order.shuffle(&mut rng);
        let mut at: HashMap<CacheKey, u32> =
            keys.iter().cloned().zip(slots).collect();
        // Only removals from here: the test's own maps no longer allocate.
        let before = held() - index.bytes() as i64;
        for (n, i) in order.iter().enumerate() {
            let k = &keys[*i];
            let slot = at.remove(k).unwrap();
            if let Some(moved) = index.remove(k, slot) {
                *at.get_mut(&moved).unwrap() = slot;
            }
            if n % 997 == 0 {
                assert_eq!(held() - before, index.bytes() as i64, "at {n}");
            }
        }
        assert_eq!(index.bytes(), 0);
        assert_eq!(held(), before, "everything released");
    }

    /// What admission counts for a key is never less than what indexing
    /// it adds, whether it starts a database, a table, or grows a vector or
    /// a map (also over removed entries' markers).
    #[test]
    fn growth_bounds_what_an_insert_adds() {
        let mut ix = Indexed::new();
        let mut rng = rand::rngs::StdRng::seed_from_u64(2);
        let mut held_keys: Vec<CacheKey> = Vec::new();
        for i in 0..20_000 {
            if !held_keys.is_empty() && rng.random_bool(0.4) {
                let k =
                    held_keys.swap_remove(rng.random_range(0..held_keys.len()));
                ix.remove(&k);
                continue;
            }
            let t =
                table_key(rng.random_range(0..40), rng.random_range(0..400));
            let k = CacheKey::Version(t.as_str().into(), i);
            let bound = ix.index.growth(&k);
            let before = ix.index.bytes();
            ix.insert(k.clone());
            let added = ix.index.bytes() - before;
            assert!(added <= bound, "{added} > {bound} at {i}");
            held_keys.push(k);
        }
        ix.audit();
    }

    /// A table's keys share one string, held by the index.
    #[test]
    fn a_tables_keys_share_its_string() {
        let mut ix = Indexed::new();
        for k in keys_of(&table_key(1, 1), 3, 2) {
            ix.insert(k);
        }
        let shared = ix.index.of_table(&table_key(1, 1));
        assert_eq!(shared.len(), 6);
        ix.audit();
    }

    /// A barrier reaches exactly the keys whose table key starts with its
    /// stream (a scan's answer): lineages, databases, deeper prefixes,
    /// prefixes cutting a segment, and streams reaching nothing; also as
    /// keys come and go.
    #[test]
    fn a_barrier_reaches_what_a_scan_finds() {
        let mut ix = Indexed::new();
        let mut held_keys: HashSet<CacheKey> = HashSet::new();
        let mut rng = rand::rngs::StdRng::seed_from_u64(3);
        let lineages = ["0123456789abcdef", "fedcba9876543210"];
        let key = |rng: &mut rand::rngs::StdRng| {
            let t = format!(
                "1/acme/s/{}/db{}/t{}",
                lineages[rng.random_range(0..2)],
                rng.random_range(0..12),
                rng.random_range(0..30)
            );
            match rng.random_range(0..3) {
                0 => CacheKey::Timeline(t.into()),
                1 => CacheKey::Version(t.into(), rng.random_range(1..4)),
                _ => CacheKey::Selection(
                    t.into(),
                    format!("d{}", rng.random_range(0..3)).into(),
                    "sig".into(),
                ),
            }
        };
        let streams = |rng: &mut rand::rngs::StdRng| -> String {
            let l = lineages[rng.random_range(0..2)];
            let db = rng.random_range(0..12);
            match rng.random_range(0..6) {
                0 => format!("1/acme/s/{l}/"),
                1 => format!("1/acme/s/{l}/db{db}/"),
                2 => {
                    format!("1/acme/s/{l}/db{db}/t{}", rng.random_range(0..30))
                }
                3 => format!("1/acme/s/{l}/db1"),
                4 => "1/acme/".into(),
                _ => format!("1/acme/s/{l}/db99/"),
            }
        };
        for _ in 0..30_000 {
            let k = key(&mut rng);
            if held_keys.contains(&k) {
                ix.remove(&k);
                held_keys.remove(&k);
            } else {
                ix.insert(k.clone());
                held_keys.insert(k);
            }
            if rng.random_ratio(1, 50) {
                let s = streams(&mut rng);
                let reached: HashSet<CacheKey> =
                    ix.index.reached(&s).into_iter().collect();
                let scan: HashSet<CacheKey> = held_keys
                    .iter()
                    .filter(|k| k.table().starts_with(&s))
                    .cloned()
                    .collect();
                assert_eq!(reached, scan, "{s}");
                assert_eq!(
                    ix.index.covers_all(&s),
                    held_keys.iter().all(|k| k.table().starts_with(&s)),
                    "{s}"
                );
            }
        }
        ix.audit();
    }

    /// Removing a long history one key at a time, in any order, keeps the
    /// slots exact and shrinks the vector as it empties.
    #[test]
    fn a_long_history_is_removed_one_key_at_a_time() {
        let mut ix = Indexed::new();
        let t = table_key(0, 0);
        let mut ks = keys_of(&t, 50_000, 0);
        for k in &ks {
            ix.insert(k.clone());
        }
        let full = ix.index.bytes();
        ks.shuffle(&mut rand::rngs::StdRng::seed_from_u64(4));
        for (n, k) in ks.iter().enumerate() {
            ix.remove(k);
            if n % 4_999 == 0 {
                ix.audit();
            }
            if n == ks.len() - 10 {
                assert!(ix.index.bytes() < full / 100, "shrunk");
            }
        }
        assert_eq!(ix.index.bytes(), 0);
        ix.audit();
    }
}
