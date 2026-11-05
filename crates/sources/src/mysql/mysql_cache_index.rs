//! The selection cache's entries by database and table: what a barrier,
//! or a table's invalidation, visits instead of the whole cache, and what
//! decides whether an entry is current
//! (`docs/design/selection-cache-diagnosis.md`, sections 7 and 8).
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
//! **Currency.** Every admission takes the cache's next stamp
//! ([`Stamps`]), which never repeats. The index, each database and each
//! table hold a stamp at or before which their entries are stale: a full
//! clear in place advances the index's, a barrier the databases' (or the
//! tables' it reaches below a database), an invalidation its table's. So
//! invalidating is O(1) per record and nothing is destroyed: a stale
//! entry stays where it is, charged, until a sweep step, an eviction or a
//! replacement removes it. A record outlives every key indexed under it,
//! and a new record's entries take stamps after every earlier one, so no
//! stale entry can become current again.
//!
//! **Sweeps.** A table whose entries went stale is queued once; a
//! database is queued once and its sweep queues its tables one at a time.
//! The queues hold the records' own names and never allocate when they
//! are pushed: their capacity is reserved, and counted, when a record is
//! created. A queued record dropped before its turn leaves its name
//! queued, still counted, until the sweep reaches it.
//!
//! Each table's keys are a vector, and each entry keeps its position there
//! (its slot): a removal swaps the last key into it, and the moved key's
//! entry is told its new slot, so a table with a long history costs no more
//! per removal than a small one. A database's tables are a vector too, with
//! a map from name to position, so a sweep can hold its place in it. A
//! table's string is held once, shared by its keys, and counted here, with
//! the maps' buckets, the vectors and the queues at capacity, in the
//! cache's estimate.

use std::collections::{HashMap, VecDeque};
use std::mem::size_of;
use std::sync::Arc;

use super::mysql_cache_cost::{allocated, buckets_for, shared_str, table};
use super::mysql_selection::CacheKey;

/// One key in a table's vector.
const KEY: usize = size_of::<CacheKey>();
/// One table record in a database's vector.
const TABLE: usize = size_of::<Table>();
/// One bucket of a database's map from table name to position.
const NAME_SLOT: usize = size_of::<(Arc<str>, u32)>();
/// One bucket of the map of databases.
const DB_SLOT: usize = size_of::<(Arc<str>, Db)>();
/// One queued name.
const QUEUED: usize = size_of::<Arc<str>>();
/// The fewest elements a vector or queue holds once it has one (the
/// standard library's first allocation for elements of these sizes).
const FIRST: usize = 4;
/// Current keys a sweep checks before it reports progress.
const CHECKS: usize = 64;

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

/// A vector (or queue) of `capacity` elements of `size` bytes.
fn vec_bytes(capacity: usize, size: usize) -> u64 {
    allocated((capacity * size) as u64)
}

/// The capacity a vector grows to when an element is pushed while full.
fn grown(capacity: usize) -> usize {
    (2 * capacity).max(FIRST)
}

/// What a push into a full vector of `capacity` adds.
fn vec_growth(capacity: usize, size: usize) -> u64 {
    vec_bytes(grown(capacity), size) - vec_bytes(capacity, size)
}

/// The cache's stamps: every admission, and every invalidation that makes
/// entries stale, takes the next. A stamp is never taken twice: once the
/// last is taken the stamps are exhausted, and only a cache that holds
/// nothing resets them ([`Self::reset`]).
#[derive(Debug, Default)]
pub(super) struct Stamps {
    last: u64,
    exhausted: bool,
}

impl Stamps {
    /// The next stamp; `None` once they are exhausted.
    pub(super) fn next(&mut self) -> Option<u64> {
        if self.exhausted {
            return None;
        }
        match self.last.checked_add(1) {
            Some(n) => {
                self.last = n;
                Some(n)
            }
            None => {
                self.exhausted = true;
                None
            }
        }
    }

    pub(super) fn exhausted(&self) -> bool {
        self.exhausted
    }

    /// Start again from the first stamp: only for a cache that holds no
    /// entry (none can carry an earlier stamp).
    pub(super) fn reset(&mut self) {
        *self = Self::default();
    }

    /// Continue from `last` (tests: exhaustion).
    #[cfg(test)]
    pub(super) fn set_last_for_test(&mut self, last: u64) {
        self.last = last;
    }
}

#[derive(Debug)]
struct Table {
    name: Arc<str>,
    keys: Vec<CacheKey>,
    /// Its entries stamped at or before this are stale.
    since: u64,
    /// The bytes of its current entries, counted since `live_at` (the
    /// latest of its database's and the index's `since` when counted).
    live: u64,
    live_at: u64,
    /// Queued for a sweep, which checks its keys from `cursor` down.
    queued: bool,
    cursor: u32,
}

#[derive(Debug, Default)]
struct Db {
    tables: Vec<Table>,
    names: HashMap<Arc<str>, u32>,
    /// The names map's buckets.
    buckets: usize,
    /// Its entries stamped at or before this are stale.
    since: u64,
    /// The bytes of its current entries, counted since `live_at` (the
    /// index's `since` when counted).
    live: u64,
    live_at: u64,
    /// Queued for a sweep, which queues its tables from `cursor` down.
    queued: bool,
    cursor: u32,
}

impl Db {
    /// The live count, reset if a full clear has passed since it was
    /// counted.
    fn refresh(&mut self, since: u64) {
        if self.live_at != since {
            self.live = 0;
            self.live_at = since;
        }
    }
}

impl Table {
    /// The live count, reset if a barrier or full clear has passed since
    /// it was counted (`base`: the latest of the two).
    fn refresh(&mut self, base: u64) {
        if self.live_at != base {
            self.live = 0;
            self.live_at = base;
        }
    }
}

/// Where a sweep is.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Sweep {
    /// This key's entry is stale: the caller removes it (at this slot).
    Stale(CacheKey, u32),
    /// Progress without a stale entry found yet.
    Checked,
    /// Nothing is queued.
    Idle,
}

/// The cache's keys by database stream and table key, with what decides
/// currency, the bytes of the current entries, the sweep queues, and the
/// estimated bytes it allocates beyond the keys themselves: the maps'
/// buckets, the database streams' and tables' strings (once each), the
/// vectors and the queues at capacity.
#[derive(Debug, Default)]
pub(super) struct Index {
    dbs: HashMap<Arc<str>, Db>,
    /// The databases map's buckets.
    buckets: usize,
    bytes: u64,
    /// Every entry stamped at or before this is stale (a full clear in
    /// place).
    since: u64,
    /// The bytes of the current entries.
    live: u64,
    /// Table records held.
    tables: usize,
    dirty_tables: VecDeque<Arc<str>>,
    dirty_dbs: VecDeque<Arc<str>>,
    /// Queued names whose record was dropped before its sweep.
    orphan_tables: usize,
    orphan_dbs: usize,
}

impl Index {
    /// The estimated bytes the index allocates.
    pub(super) fn bytes(&self) -> u64 {
        self.bytes
    }

    /// The bytes of the current entries.
    pub(super) fn live(&self) -> u64 {
        self.live
    }

    /// No record and nothing queued.
    pub(super) fn is_empty(&self) -> bool {
        self.dbs.is_empty()
            && self.dirty_tables.is_empty()
            && self.dirty_dbs.is_empty()
    }

    /// Whether a sweep has work.
    pub(super) fn dirty(&self) -> bool {
        !self.dirty_tables.is_empty() || !self.dirty_dbs.is_empty()
    }

    /// The stamp at or before which every entry is stale.
    pub(super) fn since(&self) -> u64 {
        self.since
    }

    /// Whether the entry of `key`, stamped `stamp`, is current.
    pub(super) fn is_current(&self, key: &CacheKey, stamp: u64) -> bool {
        if stamp <= self.since {
            return false;
        }
        let t = key.table();
        let Some(db) = self.dbs.get(db_stream(t)) else {
            return false;
        };
        let Some(&i) = db.names.get(t) else {
            return false;
        };
        stamp > db.since && stamp > db.tables[i as usize].since
    }

    /// What growing a queue holding `needed - 1` names by one adds.
    fn queue_growth(q: &VecDeque<Arc<str>>, needed: usize) -> u64 {
        if q.capacity() >= needed {
            0
        } else {
            vec_growth(q.capacity(), QUEUED)
        }
    }

    /// Make room for `needed` names in a queue, counted.
    fn reserve(q: &mut VecDeque<Arc<str>>, needed: usize, bytes: &mut u64) {
        let before = q.capacity();
        if before >= needed {
            return;
        }
        q.reserve_exact(grown(before).max(needed) - q.len());
        *bytes += vec_bytes(q.capacity(), QUEUED) - vec_bytes(before, QUEUED);
    }

    /// What indexing `key` may add (an upper bound; see [`map_growth`]).
    pub(super) fn growth(&self, key: &CacheKey) -> u64 {
        let t = key.table();
        let d = db_stream(t);
        let tables_queue = Self::queue_growth(
            &self.dirty_tables,
            self.tables + self.orphan_tables + 1,
        );
        let new_table = shared_str(t) + vec_bytes(FIRST, KEY) + tables_queue;
        match self.dbs.get(d) {
            None => {
                map_growth(
                    self.dbs.len(),
                    self.dbs.capacity(),
                    self.buckets,
                    DB_SLOT,
                ) + shared_str(d)
                    + table(buckets_for(1), NAME_SLOT)
                    + vec_bytes(FIRST, TABLE)
                    + Self::queue_growth(
                        &self.dirty_dbs,
                        self.dbs.len() + self.orphan_dbs + 1,
                    )
                    + new_table
            }
            Some(db) => match db.names.get(t) {
                None => {
                    map_growth(
                        db.names.len(),
                        db.names.capacity(),
                        db.buckets,
                        NAME_SLOT,
                    ) + if db.tables.len() == db.tables.capacity() {
                        vec_growth(db.tables.capacity(), TABLE)
                    } else {
                        0
                    } + new_table
                }
                Some(&i) => {
                    let v = &db.tables[i as usize].keys;
                    if v.len() == v.capacity() {
                        vec_growth(v.capacity(), KEY)
                    } else {
                        0
                    }
                }
            },
        }
    }

    /// Index `key`, whose entry of `bytes` is stamped `stamp` (after every
    /// record's `since`, so current): the key to cache, sharing its
    /// table's string with the table's other keys, and its slot.
    pub(super) fn insert(
        &mut self,
        key: CacheKey,
        bytes: u64,
        stamp: u64,
    ) -> (CacheKey, u32) {
        let since = self.since;
        let t = key.table();
        let d = db_stream(t);
        if !self.dbs.contains_key(d) {
            Self::reserve(
                &mut self.dirty_dbs,
                self.dbs.len() + self.orphan_dbs + 1,
                &mut self.bytes,
            );
            self.dbs.insert(
                d.into(),
                Db {
                    live_at: since,
                    ..Db::default()
                },
            );
            let b = tracked(self.dbs.capacity(), self.buckets);
            self.bytes += table(b, DB_SLOT) - table(self.buckets, DB_SLOT)
                + shared_str(d);
            self.buckets = b;
        }
        let db = self.dbs.get_mut(d).expect("just inserted");
        db.refresh(since);
        let i = match db.names.get(t) {
            Some(&i) => i as usize,
            None => {
                Self::reserve(
                    &mut self.dirty_tables,
                    self.tables + self.orphan_tables + 1,
                    &mut self.bytes,
                );
                let name: Arc<str> = t.into();
                let i = db.tables.len();
                let before = db.tables.capacity();
                db.tables.push(Table {
                    name: name.clone(),
                    keys: Vec::new(),
                    since: 0,
                    live: 0,
                    live_at: db.since.max(since),
                    queued: false,
                    cursor: 0,
                });
                db.names.insert(name, i as u32);
                let b = tracked(db.names.capacity(), db.buckets);
                self.bytes += table(b, NAME_SLOT)
                    - table(db.buckets, NAME_SLOT)
                    + vec_bytes(db.tables.capacity(), TABLE)
                    - vec_bytes(before, TABLE)
                    + shared_str(t);
                db.buckets = b;
                self.tables += 1;
                i
            }
        };
        let base = db.since.max(since);
        let tb = &mut db.tables[i];
        debug_assert!(stamp > base && stamp > tb.since);
        tb.refresh(base);
        tb.live += bytes;
        db.live += bytes;
        self.live += bytes;
        let key = key.with_table(tb.name.clone());
        let before = tb.keys.capacity();
        tb.keys.push(key.clone());
        self.bytes +=
            vec_bytes(tb.keys.capacity(), KEY) - vec_bytes(before, KEY);
        let slot =
            u32::try_from(tb.keys.len() - 1).expect("a table's keys fit u32");
        (key, slot)
    }

    /// Unindex `key`, held at `slot`, whose entry of `bytes` is `current`
    /// or not. The key moved into that slot, if any: its entry's slot is
    /// `slot` now. A vector a quarter full or less (of at least 16) is
    /// shrunk to twice its elements; a table without keys, and a database
    /// without tables, are dropped (a queued one's name stays queued, and
    /// counted, until its sweep).
    pub(super) fn remove(
        &mut self,
        key: &CacheKey,
        slot: u32,
        bytes: u64,
        current: bool,
    ) -> Option<CacheKey> {
        let since = self.since;
        let t = key.table();
        let d = db_stream(t);
        let slot = slot as usize;
        let db = self.dbs.get_mut(d).expect("an indexed key's database");
        let i = *db.names.get(t).expect("an indexed key's table") as usize;
        let base = db.since.max(since);
        if current {
            db.refresh(since);
            let tb = &mut db.tables[i];
            tb.refresh(base);
            tb.live -= bytes;
            db.live -= bytes;
            self.live -= bytes;
        }
        let tb = &mut db.tables[i];
        let v = &mut tb.keys;
        assert!(v.get(slot) == Some(key), "{key:?} not at slot {slot}");
        let before = v.capacity();
        v.swap_remove(slot);
        let moved = v.get(slot).cloned();
        if !v.is_empty() {
            shrink(v);
            self.bytes -= vec_bytes(before, KEY) - vec_bytes(v.capacity(), KEY);
            return moved;
        }
        // The table goes.
        let before = db.tables.capacity();
        let gone = db.tables.swap_remove(i);
        db.names.remove(t);
        if let Some(m) = db.tables.get(i) {
            *db.names.get_mut(&m.name).expect("a table's name") = i as u32;
        }
        shrink(&mut db.tables);
        self.bytes -= vec_bytes(gone.keys.capacity(), KEY)
            + vec_bytes(before, TABLE)
            - vec_bytes(db.tables.capacity(), TABLE);
        self.tables -= 1;
        if gone.queued {
            self.orphan_tables += 1;
        } else {
            self.bytes -= shared_str(t);
        }
        if db.tables.is_empty() {
            self.bytes -= table(db.buckets, NAME_SLOT)
                + vec_bytes(db.tables.capacity(), TABLE);
            let queued = db.queued;
            self.dbs.remove(d);
            if queued {
                self.orphan_dbs += 1;
            } else {
                self.bytes -= shared_str(d);
            }
            self.release_if_empty();
        }
        moved
    }

    /// An index without records releases its buckets and, once nothing
    /// is queued, its queues: an empty index holds nothing.
    fn release_if_empty(&mut self) {
        if !self.dbs.is_empty() {
            return;
        }
        if self.buckets > 0 {
            self.bytes -= table(self.buckets, DB_SLOT);
            self.dbs = HashMap::new();
            self.buckets = 0;
        }
        for q in [&mut self.dirty_tables, &mut self.dirty_dbs] {
            if q.is_empty() && q.capacity() > 0 {
                self.bytes -= vec_bytes(q.capacity(), QUEUED);
                *q = VecDeque::new();
            }
        }
    }

    /// Whether a barrier on `stream` applies to every entry (also when
    /// there is none).
    pub(super) fn covers_all(&self, stream: &str) -> bool {
        self.dbs.keys().all(|d| d.starts_with(stream))
    }

    /// Whether the table `table` has a record (has entries).
    pub(super) fn holds(&self, table: &str) -> bool {
        self.dbs
            .get(db_stream(table))
            .is_some_and(|db| db.names.contains_key(table))
    }

    /// The table `table`'s entries are stale from `stamp` on (an
    /// invalidation): queued for a sweep.
    pub(super) fn advance_table(&mut self, table: &str, stamp: u64) {
        let since = self.since;
        let Some(db) = self.dbs.get_mut(db_stream(table)) else {
            return;
        };
        let Some(&i) = db.names.get(table) else {
            return;
        };
        db.refresh(since);
        let base = db.since.max(since);
        let tb = &mut db.tables[i as usize];
        tb.refresh(base);
        db.live -= tb.live;
        self.live -= tb.live;
        tb.live = 0;
        tb.since = stamp;
        tb.cursor = tb.keys.len() as u32;
        if !tb.queued {
            tb.queued = true;
            self.dirty_tables.push_back(tb.name.clone());
        }
    }

    /// A barrier on `stream` makes every entry it applies to stale from
    /// `stamp` on: each database it covers advances (O(databases)), and
    /// below a database each table it covers.
    pub(super) fn barrier(&mut self, stream: &str, stamp: u64) {
        let since = self.since;
        for (d, db) in self.dbs.iter_mut() {
            if d.starts_with(stream) {
                db.refresh(since);
                self.live -= db.live;
                db.live = 0;
                db.since = stamp;
                db.cursor = db.tables.len() as u32;
                if !db.queued {
                    db.queued = true;
                    self.dirty_dbs.push_back(d.clone());
                }
            } else if stream.starts_with(&**d) {
                db.refresh(since);
                let base = db.since.max(since);
                for tb in db.tables.iter_mut() {
                    if !tb.name.starts_with(stream) {
                        continue;
                    }
                    tb.refresh(base);
                    db.live -= tb.live;
                    self.live -= tb.live;
                    tb.live = 0;
                    tb.since = stamp;
                    tb.cursor = tb.keys.len() as u32;
                    if !tb.queued {
                        tb.queued = true;
                        self.dirty_tables.push_back(tb.name.clone());
                    }
                }
            }
        }
    }

    /// Every entry is stale from `stamp` on (a full clear in place): O(1).
    /// Entries stamped at or before it are swept from the LRU's end.
    pub(super) fn clear(&mut self, stamp: u64) {
        self.since = stamp;
        self.live = 0;
    }

    /// The stamps are exhausted: nothing is current until the cache is
    /// empty.
    pub(super) fn fail_closed(&mut self) {
        self.since = u64::MAX;
        self.live = 0;
    }

    /// Start again with the stamps (only an empty index).
    pub(super) fn reset_since(&mut self) {
        debug_assert!(self.is_empty());
        self.since = 0;
    }

    /// One sweep step: queued tables first (their keys from the end down,
    /// [`CHECKS`] current keys at a time), then queued databases (each of
    /// their tables queued, one per step). `stamp_of`: the stamp of an
    /// indexed key's entry.
    pub(super) fn sweep(
        &mut self,
        stamp_of: impl Fn(&CacheKey) -> u64,
    ) -> Sweep {
        let since = self.since;
        if let Some(name) = self.dirty_tables.front() {
            let found = self.dbs.get_mut(db_stream(name)).and_then(|db| {
                let i = *db.names.get(&**name)?;
                let base = db.since.max(since);
                let tb = &mut db.tables[i as usize];
                Arc::ptr_eq(&tb.name, name).then_some((tb, base))
            });
            let Some((tb, base)) = found else {
                // Its record went before its turn.
                let name = self.dirty_tables.pop_front().expect("the front");
                self.orphan_tables -= 1;
                self.bytes -= shared_str(&name);
                self.release_if_empty();
                return Sweep::Checked;
            };
            for _ in 0..CHECKS {
                let c = (tb.cursor as usize).min(tb.keys.len());
                if c == 0 {
                    tb.queued = false;
                    self.dirty_tables.pop_front();
                    return Sweep::Checked;
                }
                let at = c - 1;
                tb.cursor = at as u32;
                let key = &tb.keys[at];
                let stamp = stamp_of(key);
                if stamp <= base || stamp <= tb.since {
                    return Sweep::Stale(key.clone(), at as u32);
                }
            }
            return Sweep::Checked;
        }
        if let Some(name) = self.dirty_dbs.front() {
            let found = self
                .dbs
                .get_key_value(&**name)
                .is_some_and(|(k, _)| Arc::ptr_eq(k, name));
            if !found {
                let name = self.dirty_dbs.pop_front().expect("the front");
                self.orphan_dbs -= 1;
                self.bytes -= shared_str(&name);
                self.release_if_empty();
                return Sweep::Checked;
            }
            let db = self.dbs.get_mut(&**name).expect("found");
            let c = (db.cursor as usize).min(db.tables.len());
            if c == 0 {
                db.queued = false;
                self.dirty_dbs.pop_front();
                return Sweep::Checked;
            }
            db.cursor = (c - 1) as u32;
            let tb = &mut db.tables[c - 1];
            tb.cursor = tb.keys.len() as u32;
            if !tb.queued {
                tb.queued = true;
                self.dirty_tables.push_back(tb.name.clone());
            }
            return Sweep::Checked;
        }
        Sweep::Idle
    }

    /// Recount the index against `entries` (each key with its slot, its
    /// entry's stamp and bytes) and check its running state: every key
    /// indexed once, at its slot, under its table and database, sharing
    /// its table's string; no empty table or database; the live bytes;
    /// each queued record queued once, and the queues' reserved room; the
    /// running bytes (tests).
    #[cfg(test)]
    pub(super) fn audit<'a>(
        &self,
        entries: impl Iterator<Item = (&'a CacheKey, u32, u64, u64)>,
    ) {
        let mut n = 0;
        let mut live = 0;
        let mut table_live: HashMap<&str, u64> = HashMap::new();
        for (key, slot, stamp, bytes) in entries {
            n += 1;
            let t = key.table();
            let db = &self.dbs[db_stream(t)];
            let tb = &db.tables[*db
                .names
                .get(t)
                .unwrap_or_else(|| panic!("{key:?} not indexed"))
                as usize];
            assert_eq!(tb.keys.get(slot as usize), Some(key), "{key:?} slot");
            assert!(
                key.shares_table(&tb.name),
                "{key:?} shares its table's string"
            );
            if self.is_current(key, stamp) {
                live += bytes;
                *table_live.entry(t).or_default() += bytes;
            }
        }
        assert_eq!(live, self.live, "live bytes");
        let mut bytes = table(self.buckets, DB_SLOT);
        let mut indexed = 0;
        let mut tables = 0;
        for (d, db) in &self.dbs {
            assert!(!db.tables.is_empty(), "{d} without tables");
            assert_eq!(db.names.len(), db.tables.len(), "{d} names");
            assert_eq!(db.buckets, tracked(db.names.capacity(), db.buckets));
            bytes += shared_str(d)
                + table(db.buckets, NAME_SLOT)
                + vec_bytes(db.tables.capacity(), TABLE);
            let db_live = if db.live_at == self.since { db.live } else { 0 };
            let mut sum = 0;
            for (i, tb) in db.tables.iter().enumerate() {
                assert_eq!(
                    db.names[&tb.name], i as u32,
                    "{} position",
                    tb.name
                );
                assert!(!tb.keys.is_empty(), "{} without keys", tb.name);
                assert_eq!(db_stream(&tb.name), &**d);
                indexed += tb.keys.len();
                tables += 1;
                bytes +=
                    shared_str(&tb.name) + vec_bytes(tb.keys.capacity(), KEY);
                let base = db.since.max(self.since);
                let tb_live = if tb.live_at == base { tb.live } else { 0 };
                assert_eq!(
                    tb_live,
                    table_live.get(&*tb.name).copied().unwrap_or(0),
                    "{} live",
                    tb.name
                );
                sum += tb_live;
                let queued = self
                    .dirty_tables
                    .iter()
                    .filter(|q| Arc::ptr_eq(q, &tb.name))
                    .count();
                assert_eq!(queued, tb.queued as usize, "{} queued", tb.name);
            }
            assert_eq!(db_live, sum, "{d} live");
            let queued =
                self.dirty_dbs.iter().filter(|q| Arc::ptr_eq(q, d)).count();
            assert_eq!(queued, db.queued as usize, "{d} queued");
        }
        assert_eq!(indexed, n, "keys indexed");
        assert_eq!(tables, self.tables, "tables");
        assert_eq!(
            self.dirty_tables.len(),
            self.dirty_tables
                .iter()
                .filter(|q| self.holds_name(q))
                .count()
                + self.orphan_tables,
            "orphaned tables"
        );
        assert_eq!(
            self.dirty_dbs.len(),
            self.dirty_dbs
                .iter()
                .filter(|q| self
                    .dbs
                    .get_key_value(&***q)
                    .is_some_and(|(k, _)| Arc::ptr_eq(k, q)))
                .count()
                + self.orphan_dbs,
            "orphaned databases"
        );
        assert!(
            self.dirty_tables.capacity() >= self.tables + self.orphan_tables,
            "room to queue every table"
        );
        assert!(
            self.dirty_dbs.capacity() >= self.dbs.len() + self.orphan_dbs,
            "room to queue every database"
        );
        bytes += vec_bytes(self.dirty_tables.capacity(), QUEUED)
            + vec_bytes(self.dirty_dbs.capacity(), QUEUED);
        for q in &self.dirty_tables {
            if !self.holds_name(q) {
                bytes += shared_str(q);
            }
        }
        for q in &self.dirty_dbs {
            if !self
                .dbs
                .get_key_value(&**q)
                .is_some_and(|(k, _)| Arc::ptr_eq(k, q))
            {
                bytes += shared_str(q);
            }
        }
        assert_eq!(bytes, self.bytes, "index bytes");
    }

    /// Whether `name` is the name a table record holds (tests).
    #[cfg(test)]
    fn holds_name(&self, name: &Arc<str>) -> bool {
        self.dbs.get(db_stream(name)).is_some_and(|db| {
            db.names.get(&**name).is_some_and(|&i| {
                Arc::ptr_eq(&db.tables[i as usize].name, name)
            })
        })
    }
}

/// Shrink a vector a quarter full or less (of at least 16) to twice its
/// elements.
fn shrink<T>(v: &mut Vec<T>) {
    if v.capacity() >= 16 && v.len() * 4 <= v.capacity() {
        v.shrink_to(v.len() * 2);
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

    /// The index with what the cache would hold: each key with its slot,
    /// stamp and bytes.
    struct Indexed {
        index: Index,
        stamps: Stamps,
        held: HashMap<CacheKey, (u32, u64, u64)>,
    }

    impl Indexed {
        fn new() -> Self {
            Self {
                index: Index::default(),
                stamps: Stamps::default(),
                held: HashMap::new(),
            }
        }

        fn insert(&mut self, key: CacheKey) {
            let bytes = 100 + key.table().len() as u64;
            let stamp = self.stamps.next().unwrap();
            let (key, slot) = self.index.insert(key, bytes, stamp);
            self.held.insert(key, (slot, stamp, bytes));
        }

        fn remove(&mut self, key: &CacheKey) {
            let (slot, stamp, bytes) = self.held.remove(key).expect("held");
            let current = self.index.is_current(key, stamp);
            if let Some(moved) = self.index.remove(key, slot, bytes, current) {
                self.held.get_mut(&moved).expect("held").0 = slot;
            }
        }

        fn current(&self, key: &CacheKey) -> bool {
            self.index.is_current(key, self.held[key].1)
        }

        /// Sweep until idle: the stale keys removed.
        fn sweep(&mut self) -> usize {
            let mut removed = 0;
            loop {
                let held = &self.held;
                match self.index.sweep(|k| held[k].1) {
                    Sweep::Stale(k, slot) => {
                        assert_eq!(self.held[&k].0, slot);
                        assert!(!self.current(&k));
                        self.remove(&k);
                        removed += 1;
                    }
                    Sweep::Checked => {}
                    Sweep::Idle => return removed,
                }
            }
        }

        fn audit(&self) {
            self.index.audit(
                self.held.iter().map(|(k, (slot, stamp, bytes))| {
                    (k, *slot, *stamp, *bytes)
                }),
            );
        }
    }

    /// The keys with the database stream of their table, and an index's
    /// estimate equals what it allocates (jemalloc, this thread): over many
    /// small tables, a long history, invalidations that queue tables and
    /// databases, removals that shrink a vector and removals that drop
    /// queued and unqueued tables and databases.
    #[test]
    fn the_index_allocates_its_estimate() {
        let mut keys = Vec::new();
        for t in 0..3_000 {
            keys.extend(keys_of(&table_key(t % 30, t), 1, 0));
        }
        keys.extend(keys_of(&table_key(0, 99_999), 2_000, 0));
        // The keys exist before the index does; it holds its own string
        // per database and table, and its maps, vectors and queues.
        let mut index = Index::default();
        let mut slots = Vec::with_capacity(keys.len());
        let before = held();
        for (n, k) in keys.iter().enumerate() {
            let stamp = n as u64 + 1;
            slots.push((index.insert(k.clone(), 1, stamp).1, stamp));
        }
        assert_eq!(held() - before, index.bytes() as i64, "filled");
        // Invalidations queue without allocating.
        let queued = held();
        for t in (0..3_000).step_by(7) {
            index.advance_table(&table_key(t % 30, t), 1_000_000);
        }
        for db in 0..5 {
            index.barrier(
                &format!("1/acme/s/0123456789abcdef/db{db}/"),
                1_000_001,
            );
        }
        assert_eq!(held(), queued, "queueing allocates nothing");
        let mut rng = rand::rngs::StdRng::seed_from_u64(1);
        let mut order: Vec<usize> = (0..keys.len()).collect();
        order.shuffle(&mut rng);
        let mut at: HashMap<CacheKey, (u32, u64)> =
            keys.iter().cloned().zip(slots).collect();
        // Only removals from here: the test's own maps no longer allocate.
        let before = held() - index.bytes() as i64;
        for (n, i) in order.iter().enumerate() {
            let k = &keys[*i];
            let (slot, stamp) = at.remove(k).unwrap();
            let current = index.is_current(k, stamp);
            if let Some(moved) = index.remove(k, slot, 1, current) {
                at.get_mut(&moved).unwrap().0 = slot;
            }
            if n % 997 == 0 {
                assert_eq!(held() - before, index.bytes() as i64, "at {n}");
            }
        }
        assert_eq!(index.live(), 0);
        assert!(index.bytes() > 0, "orphaned names stay queued");
        while index.sweep(|_| unreachable!()) != Sweep::Idle {}
        assert_eq!(index.bytes(), 0);
        assert!(index.is_empty());
        assert_eq!(held(), before, "everything released");
    }

    /// What admission counts for a key is never less than what indexing
    /// it adds, whether it starts a database, a table, or grows a vector,
    /// a map or a queue (also over removed entries' markers and orphaned
    /// names).
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
            if rng.random_ratio(1, 50) {
                let s = ix.stamps.next().unwrap();
                let t = table_key(
                    rng.random_range(0..40),
                    rng.random_range(0..400),
                );
                ix.index.advance_table(&t, s);
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
        assert_eq!(ix.index.tables, 1);
        ix.audit();
    }

    /// A barrier makes stale exactly the keys whose table key starts with
    /// its stream (a scan's answer), and a table's invalidation exactly
    /// its keys: lineages, databases, deeper prefixes, prefixes cutting a
    /// segment, and streams reaching nothing; keys admitted afterwards are
    /// current; a sweep removes exactly the stale keys. Checked against a
    /// reference model as keys come and go.
    #[test]
    fn staleness_matches_a_scan() {
        let mut ix = Indexed::new();
        let mut stale: HashSet<CacheKey> = HashSet::new();
        let mut rng = rand::rngs::StdRng::seed_from_u64(3);
        let lineages = ["0123456789abcdef", "fedcba9876543210"];
        let table = |rng: &mut rand::rngs::StdRng| {
            format!(
                "1/acme/s/{}/db{}/t{}",
                lineages[rng.random_range(0..2)],
                rng.random_range(0..12),
                rng.random_range(0..30)
            )
        };
        let key = |rng: &mut rand::rngs::StdRng| {
            let t = table(rng);
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
        for i in 0..30_000 {
            let k = key(&mut rng);
            if ix.held.contains_key(&k) {
                ix.remove(&k);
                stale.remove(&k);
            } else {
                ix.insert(k.clone());
            }
            if rng.random_ratio(1, 50) {
                let s = streams(&mut rng);
                assert_eq!(
                    ix.index.covers_all(&s),
                    ix.held.keys().all(|k| k.table().starts_with(&s)),
                    "{s}"
                );
                let stamp = ix.stamps.next().unwrap();
                ix.index.barrier(&s, stamp);
                stale.extend(
                    ix.held
                        .keys()
                        .filter(|k| k.table().starts_with(&s))
                        .cloned(),
                );
            }
            if rng.random_ratio(1, 40) {
                let t = table(&mut rng);
                let stamp = ix.stamps.next().unwrap();
                ix.index.advance_table(&t, stamp);
                stale
                    .extend(ix.held.keys().filter(|k| k.table() == t).cloned());
            }
            if rng.random_ratio(1, 3_000) {
                let stamp = ix.stamps.next().unwrap();
                ix.index.clear(stamp);
                stale.extend(ix.held.keys().cloned());
            }
            for k in ix.held.keys() {
                assert_eq!(ix.current(k), !stale.contains(k), "{k:?} at {i}");
            }
            if rng.random_ratio(1, 200) {
                let before = ix.held.len();
                let removed = ix.sweep();
                assert_eq!(before - removed, ix.held.len());
                stale.retain(|k| ix.held.contains_key(k));
                // What a sweep leaves stale is a full clear's alone (swept
                // from the LRU's end instead).
                for k in &stale {
                    assert!(ix.held[k].1 <= ix.index.since(), "{k:?} left");
                }
                ix.audit();
            }
        }
        ix.audit();
    }

    /// Removing a long history one key at a time, in any order, keeps the
    /// slots exact and shrinks the vector as it empties; a sweep over a
    /// long stale history keeps its place across steps.
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

        for k in keys_of(&t, 50_000, 0) {
            ix.insert(k);
        }
        let s = ix.stamps.next().unwrap();
        ix.index.advance_table(&t, s);
        // Keys admitted after the invalidation stay.
        let fresh: Vec<CacheKey> = (50_001..50_100)
            .map(|v| CacheKey::Version(t.as_str().into(), v))
            .collect();
        for k in &fresh {
            ix.insert(k.clone());
        }
        assert_eq!(ix.sweep(), 50_001);
        assert_eq!(ix.held.len(), fresh.len());
        ix.audit();
    }

    /// A record dropped while queued leaves its name queued and counted
    /// until its turn; a record created again under the same name is
    /// queued on its own, and its keys are never confused with the old
    /// ones.
    #[test]
    fn a_dropped_queued_record_stays_counted_until_its_turn() {
        let mut ix = Indexed::new();
        let t = table_key(3, 3);
        let k = CacheKey::Timeline(t.as_str().into());
        ix.insert(k.clone());
        let s = ix.stamps.next().unwrap();
        ix.index.barrier("1/acme/s/0123456789abcdef/db3/", s);
        ix.index.advance_table(&t, s);
        assert!(!ix.current(&k));
        ix.remove(&k);
        assert_eq!((ix.index.orphan_tables, ix.index.orphan_dbs), (1, 1));
        ix.audit();
        ix.insert(k.clone());
        assert!(ix.current(&k), "a new record's key is current");
        let s = ix.stamps.next().unwrap();
        ix.index.advance_table(&t, s);
        ix.audit();
        assert_eq!(ix.sweep(), 1);
        assert!(ix.held.is_empty() && ix.index.is_empty());
        assert_eq!(ix.index.bytes(), 0);
        ix.audit();
    }

    /// Stamps never repeat: the last one is taken once, then none.
    #[test]
    fn stamps_are_exhausted_after_the_last() {
        let mut s = Stamps::default();
        s.set_last_for_test(u64::MAX - 1);
        assert_eq!(s.next(), Some(u64::MAX));
        assert_eq!(s.next(), None);
        assert!(s.exhausted());
        assert_eq!(s.next(), None);
        s.reset();
        assert_eq!(s.next(), Some(1));
    }
}
