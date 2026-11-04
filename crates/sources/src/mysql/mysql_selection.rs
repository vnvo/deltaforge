//! Row-time schema selection (design spec 7.6, binding rulings Rounds 32-33).
//!
//! Rows are decoded with the schema version in effect at their evaluation
//! position (the executed state immediately before their transaction):
//!
//! 1. **Positional proof:** the unique maximal applicable activation record
//!    (`baseline`, forward-proof `observed`, FULL `observed`) names the
//!    version. Without one, a lazy baseline at the rows' position is
//!    attempted first (`mysql_baseline::establish_at`). Before any record is trusted, every piece of decisive evidence
//!    the table's timeline holds is validated; invalid, duplicate or
//!    unsupported evidence fails closed. The proven version must not
//!    contradict any comparable field of the row's TableMap.
//! 2. **FULL metadata, no positional proof:** the live shape is stable-
//!    captured and registered; among the key's versions through the
//!    registry's high-water, exactly one distinct complete schema must match
//!    the TableMap signature and none may be unverifiable. That choice is
//!    recorded durably (`FullProof`, binding the one DDL or applicable barrier
//!    at the row's position, if any) before the row is emitted.
//! 3. **MINIMAL metadata without positional proof:** fail closed.
//!
//! Nothing is ever registered from a TableMap. Every row operation is
//! stamped with the selected version's hash and its own registry sequence.

use std::sync::Arc;

use anyhow::Context;
use deltaforge_core::{CheckpointOrder, SourceError, SourceResult};
use mysql_binlog_connector_rust::event::table_map_event::TableMapEvent;
use sha2::{Digest, Sha256};
use storage::DurableSchemaRegistry;
use storage::adapters::SchemaKey;
use tracing::{info, warn};

use super::RunCtx;
use super::mysql_activation::{
    self, ACTIVATION_NS, EventIdentity, FullProof, Kind, Record, Selection,
    Stored, TimelineError, baseline_capture_id, full_capture_id,
    select_decisive, table_stream,
};
use super::mysql_binlog_scan::{
    CLASSIFIER_VERSION, ProofError, ScanLimits, capture_proven,
};
use super::mysql_history::BarrierStream;
use super::mysql_schema_loader::LoadedSchema;
use super::mysql_signature::{
    SIGNATURE_FORMAT, Signature, Verdict, compare, is_full,
};
use super::mysql_table_schema::MySqlTableSchema;
use crate::durable_checkpoint::{WmPos, order_positions};

/// Capture attempts for the live shape at a FULL fallback (retried only when
/// a DDL or barrier affecting the table lands inside the capture interval).
const CAPTURE_ATTEMPTS: u32 = 5;

/// A table's validated timeline: its records and the barrier streams that
/// apply to it (its lineage's, then its database's), shared with every
/// timeline they apply to.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Timeline {
    pub records: Vec<Stored>,
    /// The sequence of the table stream's last record when it was read (0
    /// when empty): an append's proof that nothing else was stored since.
    pub table_head: u64,
    /// The barrier streams read, with their records and last sequences:
    /// what an atomic batch's preconditions rest on.
    pub streams: Vec<Arc<BarrierStream>>,
    /// The estimated bytes of `records` (its streams are accounted once
    /// each, where they are cached).
    pub bytes: u64,
}

/// Versions an atomic batch plans for one table but has not committed:
/// version to schema hash. Validation of a proposed record reads them in
/// place of the registry; nothing else ever sees them.
pub(crate) type PlannedVersions = std::collections::HashMap<i32, String>;

impl Timeline {
    /// Selection at `row` over this timeline, without copying it.
    pub(crate) fn select_decisive(
        &self,
        row: &WmPos,
    ) -> (Selection, Option<String>) {
        mysql_activation::select_decisive_in(
            self.records.iter().chain(self.barriers()),
            row,
        )
    }

    /// The barriers that apply, in their streams' order.
    pub(crate) fn barriers(&self) -> impl Iterator<Item = &Stored> + Clone {
        self.streams.iter().flat_map(|s| s.records.iter())
    }

    /// The barrier streams read, with their last sequences.
    pub(crate) fn barrier_heads(&self) -> Vec<(String, u64)> {
        self.streams
            .iter()
            .map(|s| (s.stream.clone(), s.head))
            .collect()
    }

    pub(crate) fn all(&self) -> Vec<Stored> {
        self.records
            .iter()
            .chain(self.barriers())
            .cloned()
            .collect()
    }

    /// A timeline of `records` with `barriers` as one barrier stream
    /// (tests).
    #[cfg(test)]
    pub(crate) fn of(records: Vec<Stored>, barriers: Vec<Stored>) -> Self {
        Self {
            records,
            table_head: 0,
            streams: vec![Arc::new(BarrierStream {
                stream: "barriers".into(),
                records: barriers,
                head: 0,
                bytes: 0,
            })],
            bytes: 0,
        }
    }
}

/// The cache's bound: at most this many estimated bytes
/// ([`mysql_cache_cost`]) of entries (timelines, selected schemas and
/// read-ahead versions, with their keys and list nodes), barrier streams
/// (each once) and the maps' buckets. No count of tables: as many as fit.
///
/// [`mysql_cache_cost`]: super::mysql_cache_cost
pub(crate) const SELECTION_CACHE_BYTES: u64 = 256 * 1024 * 1024;

/// One entry's node in the LRU list: its key, its value and two links.
const NODE: u64 = (std::mem::size_of::<CacheKey>()
    + std::mem::size_of::<Entry>()
    + 2 * std::mem::size_of::<usize>()) as u64;
/// One bucket of the LRU's map: a key reference and the node's pointer.
const LRU_SLOT: usize = 2 * std::mem::size_of::<usize>();
/// One bucket of the barrier streams' map.
const BARRIER_SLOT: usize =
    std::mem::size_of::<(String, (Arc<BarrierStream>, usize))>();

/// What an entry costs beyond its value: its node and its key's strings.
fn key_cost(key: &CacheKey) -> u64 {
    use super::mysql_cache_cost::allocated;
    let s = |s: &String| allocated(s.capacity() as u64);
    allocated(NODE)
        + match key {
            CacheKey::Timeline(t) | CacheKey::Version(t, _) => s(t),
            CacheKey::Selection(t, d, g) => s(t) + s(d) + s(g),
        }
}

/// `deltaforge_mysql_selection_cache_evictions_total{pipeline,source}`.
pub(crate) const METRIC_EVICTIONS: &str =
    "deltaforge_mysql_selection_cache_evictions_total";
/// `deltaforge_mysql_selection_cache_bytes{pipeline,source}`: the
/// estimated bytes held (entries, barrier streams and the maps' buckets).
pub(crate) const METRIC_BYTES: &str = "deltaforge_mysql_selection_cache_bytes";
/// `deltaforge_mysql_selection_cache_timelines{pipeline,source}`.
pub(crate) const METRIC_TIMELINES: &str =
    "deltaforge_mysql_selection_cache_timelines";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum CacheKey {
    /// A validated timeline, by table key.
    Timeline(String),
    /// A selected schema by (table key, decisive record, TableMap
    /// signature). The key carries the lineage; the decisive record carries
    /// the row position's effective proof.
    Selection(String, String, String),
    /// A registered version of a table, read ahead with its timeline.
    Version(String, i32),
}

impl CacheKey {
    fn table(&self) -> &str {
        match self {
            CacheKey::Timeline(t)
            | CacheKey::Selection(t, _, _)
            | CacheKey::Version(t, _) => t,
        }
    }
}

#[derive(Debug, Clone)]
enum Cached {
    /// A timeline, and whether the table's last selection over it proved a
    /// version (then an activation window need not select it again).
    Timeline(Arc<Timeline>, bool),
    Selection(Arc<LoadedSchema>),
    Version(Arc<schema_registry::SchemaVersion>),
}

/// An entry: its value, its estimated bytes (key and node included), and
/// the load that admitted it (0: none).
#[derive(Debug, Clone)]
struct Entry {
    cached: Cached,
    bytes: u64,
    load: u64,
}

/// What one load ([`install`]) may still admit: half the byte bound, so a
/// load leaves room for what the cache held; its first timeline may take the
/// whole bound (a table read alone always could). What a load admitted is
/// never evicted by the same load.
pub(crate) struct Admission {
    left: u64,
    first: bool,
    /// An entry after the first did not fit.
    refused: bool,
}

impl Admission {
    /// Nothing more is admitted: its room is used up, or an entry after the
    /// first did not fit (what the load has not read yet is left for when
    /// it is reached).
    fn spent(&self) -> bool {
        self.refused || (!self.first && self.left == 0)
    }
}

/// Per-run caches of timelines, selections and read-ahead versions,
/// bounded together by their estimated bytes
/// (`docs/design/mysql-mass-invalidation.md`, section 6): admission evicts
/// the least recently used entries until the new one fits, before inserting
/// it. Barrier streams are held once, shared by the timelines they apply
/// to, and dropped with the last of them. Only this process appends
/// activation records and barriers for this source (the store gate), so
/// invalidating on every append keeps them exact; an evicted entry is
/// rebuilt exactly from the durable records.
pub(crate) struct Caches {
    entries: lru::LruCache<CacheKey, Entry>,
    /// Barrier streams, by stream key, with the cached timelines holding
    /// each.
    barriers: std::collections::HashMap<String, (Arc<BarrierStream>, usize)>,
    timelines: usize,
    /// The estimated bytes of the entries and barrier streams held.
    held: u64,
    /// Of which the timelines' and versions' (a batch's size per table).
    table_bytes: u64,
    /// The most entries held since the LRU's map was last replaced: its
    /// buckets, kept after eviction and invalidation, are sized by it.
    peak: usize,
    max_bytes: u64,
    /// The current load, if one is admitting ([`Admission`]).
    load: u64,
    loading: bool,
    meters: Meters,
}

/// The cache's metrics: evictions, and its own accounting (estimated bytes
/// and timelines held), published after every change.
struct Meters {
    evictions: metrics::Counter,
    bytes: metrics::Gauge,
    timelines: metrics::Gauge,
}

impl Meters {
    fn noop() -> Self {
        Self {
            evictions: metrics::Counter::noop(),
            bytes: metrics::Gauge::noop(),
            timelines: metrics::Gauge::noop(),
        }
    }
}

impl std::fmt::Debug for Caches {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Caches")
            .field("entries", &self.entries.len())
            .field("timelines", &self.timelines)
            .field("bytes", &self.bytes())
            .finish()
    }
}

impl Default for Caches {
    fn default() -> Self {
        Self::with_bytes(SELECTION_CACHE_BYTES, Meters::noop())
    }
}

impl Caches {
    /// The source's caches, metered under its labels.
    pub(crate) fn new(pipeline: &str, source: &str) -> Self {
        Self::new_with(pipeline, source, |name| std::env::var(name).ok())
    }

    /// [`Self::new`] with `var` for the process environment. The bound: a
    /// test's, else a measurement build's from `var` (perf-phases; a default
    /// build ignores it), else the code's.
    fn new_with(
        pipeline: &str,
        source: &str,
        var: impl Fn(&str) -> Option<String>,
    ) -> Self {
        let bytes = crate::stream_probe::selection_cache_bytes()
            .or_else(|| crate::perf_phases::cache_bytes_override(var))
            .unwrap_or(SELECTION_CACHE_BYTES);
        crate::perf_phases::cache_limit(bytes);
        let labels = [
            ("pipeline", pipeline.to_string()),
            ("source", source.to_string()),
        ];
        let caches = Self::with_bytes(
            bytes,
            Meters {
                evictions: metrics::counter!(METRIC_EVICTIONS, &labels),
                bytes: metrics::gauge!(METRIC_BYTES, &labels),
                timelines: metrics::gauge!(METRIC_TIMELINES, &labels),
            },
        );
        caches.publish();
        caches
    }

    fn with_bytes(max_bytes: u64, meters: Meters) -> Self {
        Self {
            entries: lru::LruCache::unbounded(),
            barriers: Default::default(),
            timelines: 0,
            held: 0,
            table_bytes: 0,
            peak: 0,
            max_bytes,
            load: 0,
            loading: false,
            meters,
        }
    }

    /// The maps' buckets at `entries` entries held at most: the LRU's
    /// (with its two sentinel nodes) and the barrier streams'.
    fn maps_at(&self, entries: usize, barriers: usize) -> u64 {
        use super::mysql_cache_cost::{allocated, buckets_for, table};
        let lru = if entries == 0 {
            0
        } else {
            table(buckets_for(entries), LRU_SLOT) + 2 * allocated(NODE)
        };
        lru + table(buckets_for(barriers), BARRIER_SLOT)
    }

    /// The estimated bytes held: entries, barrier streams and the maps'
    /// buckets.
    pub(crate) fn bytes(&self) -> u64 {
        self.held + self.maps_at(self.peak, self.barriers.capacity())
    }

    /// What one more entry, and `streams` more barrier streams, add to the
    /// maps' buckets.
    fn growth(&self, streams: usize) -> u64 {
        let entries = self.peak.max(self.entries.len() + 1);
        let barriers =
            self.barriers.capacity().max(self.barriers.len() + streams);
        self.maps_at(entries, barriers)
            - self.maps_at(self.peak, self.barriers.capacity())
    }

    /// Publish the accounting (after every change to it).
    fn publish(&self) {
        self.meters.bytes.set(self.bytes() as f64);
        self.meters.timelines.set(self.timelines as f64);
    }

    /// Timelines held.
    #[cfg(test)]
    pub(crate) fn timelines(&self) -> usize {
        self.timelines
    }

    /// The barrier streams held.
    pub(crate) fn barrier_streams(
        &self,
    ) -> std::collections::HashMap<String, Arc<BarrierStream>> {
        self.barriers
            .iter()
            .map(|(k, (s, _))| (k.clone(), s.clone()))
            .collect()
    }

    /// Remove `key`'s entry, with its accounting.
    fn remove(&mut self, key: &CacheKey) {
        if let Some(e) = self.entries.pop(key) {
            self.forget(e);
        }
    }

    fn forget(&mut self, e: Entry) {
        self.held -= e.bytes;
        match e.cached {
            Cached::Timeline(t, _) => {
                self.timelines -= 1;
                self.table_bytes -= e.bytes;
                // Its barrier streams go with the last timeline holding
                // them.
                for s in &t.streams {
                    if let Some((held, refs)) = self.barriers.get_mut(&s.stream)
                        && Arc::ptr_eq(held, s)
                    {
                        *refs -= 1;
                        if *refs == 0 {
                            let gone = super::mysql_cache_cost::stream(held);
                            self.barriers.remove(&s.stream);
                            self.held -= gone;
                        }
                    }
                }
            }
            Cached::Version(_) => self.table_bytes -= e.bytes,
            Cached::Selection(_) => {}
        }
        self.publish();
    }

    /// Evict the least recently used entry; `false` when there is none, or
    /// when it is one the current load admitted (a load never evicts what
    /// it admitted).
    fn evict_one(&mut self) -> bool {
        if self.loading
            && self
                .entries
                .peek_lru()
                .is_some_and(|(_, e)| e.load == self.load)
        {
            return false;
        }
        let Some((_, e)) = self.entries.pop_lru() else {
            return false;
        };
        crate::perf_phases::cache_eviction(match &e.cached {
            Cached::Timeline(..) => "timeline",
            Cached::Selection(_) => "selection",
            Cached::Version(_) => "version",
        });
        self.forget(e);
        self.meters.evictions.increment(1);
        true
    }

    /// Insert `key`'s entry of `bytes` (its value's), evicting until it
    /// fits; one that cannot fit is not cached. Whether it was.
    fn put(&mut self, key: CacheKey, value: Cached, bytes: u64) -> bool {
        self.remove(&key);
        let bytes = bytes + key_cost(&key);
        while self.bytes() + bytes + self.growth(0) > self.max_bytes {
            if !self.evict_one() {
                return false;
            }
        }
        self.push(key, value, bytes);
        true
    }

    /// Insert an entry that fits.
    fn push(&mut self, key: CacheKey, cached: Cached, bytes: u64) {
        self.held += bytes;
        if !matches!(cached, Cached::Selection(_)) {
            self.table_bytes += bytes;
        }
        let load = if self.loading { self.load } else { 0 };
        self.entries.push(
            key,
            Entry {
                cached,
                bytes,
                load,
            },
        );
        self.peak = self.peak.max(self.entries.len());
        self.publish();
    }

    /// Start a load: what it may admit ([`Admission`]).
    fn begin_load(&mut self) -> Admission {
        self.load += 1;
        self.loading = true;
        Admission {
            left: self.max_bytes / 2,
            first: true,
            refused: false,
        }
    }

    fn end_load(&mut self) {
        self.loading = false;
    }

    /// Whether a load may admit `cost` more bytes; if so, they are spent.
    fn admit(&self, adm: &mut Admission, cost: u64) -> bool {
        let ok = cost <= adm.left || (adm.first && cost <= self.max_bytes);
        if ok {
            adm.left = adm.left.saturating_sub(cost);
            adm.first = false;
        } else if !adm.first {
            adm.refused = true;
        }
        ok
    }

    /// The most tables one batched load brings in: as many as half the
    /// bound holds at the held tables' average cost (timeline and version),
    /// at most one statement's, at least one. Before anything is held: one
    /// statement's.
    pub(crate) fn batch_room(&self) -> usize {
        let max = super::mysql_history::BATCH_MAX_TABLES;
        if self.timelines == 0 {
            return max;
        }
        let each = (self.table_bytes / self.timelines as u64).max(1);
        ((self.max_bytes / 2 / each) as usize).clamp(1, max)
    }

    pub(crate) fn timeline(&mut self, key: &str) -> Option<Arc<Timeline>> {
        match self.entries.get(&CacheKey::Timeline(key.to_string()))? {
            Entry {
                cached: Cached::Timeline(t, _),
                ..
            } => Some(t.clone()),
            _ => None,
        }
    }

    /// The backend keys of every table flagged proven, read without
    /// touching recency (a speculative window's candidate filter).
    pub(crate) fn proven_keys(&self) -> std::collections::HashSet<String> {
        self.entries
            .iter()
            .filter_map(|(k, e)| match (k, &e.cached) {
                (CacheKey::Timeline(key), Cached::Timeline(_, true)) => {
                    Some(key.clone())
                }
                _ => None,
            })
            .collect()
    }

    /// Whether the table's last selection proved a version since its
    /// timeline last changed.
    pub(crate) fn is_proven(&mut self, key: &SchemaKey) -> bool {
        matches!(
            self.entries.peek(&CacheKey::Timeline(key.backend_key())),
            Some(Entry {
                cached: Cached::Timeline(_, true),
                ..
            })
        )
    }

    /// The table's last selection proved a version over its cached
    /// timeline (no extra entry: a flag on that timeline's).
    fn mark_proven(&mut self, key: &SchemaKey) {
        if let Some(Entry {
            cached: Cached::Timeline(_, proven),
            ..
        }) = self
            .entries
            .peek_mut(&CacheKey::Timeline(key.backend_key()))
        {
            *proven = true;
        }
    }

    /// The barrier streams of `timeline` the cache does not hold (this
    /// read of them).
    fn unheld<'a>(
        &self,
        timeline: &'a Timeline,
    ) -> impl Iterator<Item = &'a Arc<BarrierStream>> {
        timeline.streams.iter().filter(|s| {
            self.barriers
                .get(&s.stream)
                .is_none_or(|(held, _)| held != *s)
        })
    }

    /// What caching `timeline` for `key` adds: the entry, and the barrier
    /// streams of it not held.
    fn timeline_cost(&self, key: &CacheKey, timeline: &Timeline) -> u64 {
        super::mysql_cache_cost::timeline(timeline)
            + key_cost(key)
            + self
                .unheld(timeline)
                .map(|s| super::mysql_cache_cost::stream(s))
                .sum::<u64>()
    }

    /// Cache `timeline`, holding its barrier streams once: a stream already
    /// held is shared, a new one is held (and accounted) from here. The
    /// timeline as cached (sharing the held streams), or `None` when it
    /// does not fit.
    fn put_timeline(
        &mut self,
        key: String,
        timeline: Arc<Timeline>,
    ) -> Option<Arc<Timeline>> {
        self.put_timeline_as(CacheKey::Timeline(key), timeline)
    }

    fn put_timeline_as(
        &mut self,
        key: CacheKey,
        timeline: Arc<Timeline>,
    ) -> Option<Arc<Timeline>> {
        self.remove(&key);
        // Evict until the timeline, and those of its streams not held, fit
        // (an eviction may drop a stream it would have shared).
        loop {
            let cost = self.timeline_cost(&key, &timeline);
            let streams = self.unheld(&timeline).count();
            if self.bytes() + cost + self.growth(streams) <= self.max_bytes {
                break;
            }
            if !self.evict_one() {
                return None;
            }
        }
        let mut shared =
            Arc::try_unwrap(timeline).unwrap_or_else(|t| (*t).clone());
        for s in shared.streams.iter_mut() {
            match self.barriers.get_mut(&s.stream) {
                Some((held, refs)) if *held == *s => {
                    *s = held.clone();
                    *refs += 1;
                }
                // Another read of the stream: the newer one is held (an
                // append invalidates every timeline of the older).
                _ => {
                    if let Some((old, _)) = self.barriers.remove(&s.stream) {
                        self.held -= super::mysql_cache_cost::stream(&old);
                    }
                    self.barriers.insert(s.stream.clone(), (s.clone(), 1));
                    self.held += super::mysql_cache_cost::stream(s);
                }
            }
        }
        self.timelines += 1;
        let shared = Arc::new(shared);
        let bytes = super::mysql_cache_cost::timeline(&shared) + key_cost(&key);
        self.push(key, Cached::Timeline(shared.clone(), false), bytes);
        Some(shared)
    }

    /// A load's [`Self::put_timeline`]: admitted only while the load may
    /// admit it. The timeline to use (cached or not).
    fn admit_timeline(
        &mut self,
        adm: &mut Admission,
        key: String,
        timeline: Arc<Timeline>,
    ) -> Arc<Timeline> {
        let key = CacheKey::Timeline(key);
        let cost = self.timeline_cost(&key, &timeline);
        if !self.admit(adm, cost) {
            return timeline;
        }
        match self.put_timeline_as(key, timeline.clone()) {
            Some(cached) => cached,
            None => timeline,
        }
    }

    fn selection(
        &mut self,
        (table, decisive, signature): &(String, String, String),
    ) -> Option<Arc<LoadedSchema>> {
        let key = CacheKey::Selection(
            table.clone(),
            decisive.clone(),
            signature.clone(),
        );
        match self.entries.get(&key)? {
            Entry {
                cached: Cached::Selection(s),
                ..
            } => Some(s.clone()),
            _ => None,
        }
    }

    fn version(
        &mut self,
        table: &str,
        version: i32,
    ) -> Option<Arc<schema_registry::SchemaVersion>> {
        match self
            .entries
            .get(&CacheKey::Version(table.to_string(), version))?
        {
            Entry {
                cached: Cached::Version(v),
                ..
            } => Some(v.clone()),
            _ => None,
        }
    }

    fn put_version(
        &mut self,
        table: String,
        v: schema_registry::SchemaVersion,
    ) -> bool {
        let bytes = super::mysql_cache_cost::version(&v);
        self.put(
            CacheKey::Version(table, v.version),
            Cached::Version(Arc::new(v)),
            bytes,
        )
    }

    /// A load's [`Self::put_version`]: admitted only while the load may
    /// admit it.
    fn admit_version(
        &mut self,
        adm: &mut Admission,
        table: String,
        v: schema_registry::SchemaVersion,
    ) {
        let key = CacheKey::Version(table, v.version);
        let bytes = super::mysql_cache_cost::version(&v);
        if self.admit(adm, bytes + key_cost(&key)) {
            self.put(key, Cached::Version(Arc::new(v)), bytes);
        }
    }

    fn put_selection(
        &mut self,
        (table, decisive, signature): (String, String, String),
        loaded: Arc<LoadedSchema>,
    ) {
        let bytes = super::mysql_cache_cost::loaded(&loaded);
        self.put(
            CacheKey::Selection(table, decisive, signature),
            Cached::Selection(loaded),
            bytes,
        );
    }

    /// Change the bound, keeping what is held (tests).
    #[cfg(test)]
    pub(crate) fn set_limit_for_test(&mut self, bytes: u64) {
        self.max_bytes = bytes;
    }

    #[cfg(test)]
    pub(crate) fn seed_for_test(&mut self, key: &SchemaKey) {
        self.put_timeline(
            key.backend_key(),
            Arc::new(Timeline {
                records: Vec::new(),
                table_head: 0,
                streams: Vec::new(),
                bytes: 0,
            }),
        );
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    /// The estimated bytes of the versions held for the table `key`
    /// (tests).
    #[cfg(test)]
    pub(crate) fn version_bytes_for(&self, key: &SchemaKey) -> u64 {
        let k = key.backend_key();
        self.entries
            .iter()
            .filter(|(e, _)| matches!(e, CacheKey::Version(t, _) if *t == k))
            .map(|(_, e)| e.bytes)
            .sum()
    }

    /// The entries held for the table `key` (tests).
    #[cfg(test)]
    pub(crate) fn held_for(&self, key: &SchemaKey) -> usize {
        let k = key.backend_key();
        self.entries.iter().filter(|(e, _)| e.table() == k).count()
    }

    /// The estimated bytes of the entries and barrier streams held, without
    /// the maps' own buckets (tests).
    #[cfg(test)]
    pub(crate) fn entries_bytes_for_test(&self) -> u64 {
        self.held
    }

    /// Recount what the cache holds and check its running accounting: the
    /// bytes (each entry's from its value and key, each held stream's, the
    /// maps' buckets), the timelines, and each barrier stream's holders
    /// (tests).
    #[cfg(test)]
    pub(crate) fn audit(&self) {
        use super::mysql_cache_cost as cost;
        let mut holders: std::collections::HashMap<&str, usize> =
            Default::default();
        let (mut timelines, mut held, mut table_bytes) = (0, 0, 0);
        for (key, e) in self.entries.iter() {
            let bytes = key_cost(key)
                + match &e.cached {
                    Cached::Timeline(t, _) => {
                        timelines += 1;
                        for s in &t.streams {
                            let (h, _) = &self.barriers[&s.stream];
                            assert!(
                                Arc::ptr_eq(h, s),
                                "{} not shared",
                                s.stream
                            );
                            *holders.entry(s.stream.as_str()).or_default() += 1;
                        }
                        cost::timeline(t)
                    }
                    Cached::Selection(l) => cost::loaded(l),
                    Cached::Version(v) => cost::version(v),
                };
            assert_eq!(e.bytes, bytes, "{key:?}");
            held += bytes;
            if !matches!(e.cached, Cached::Selection(_)) {
                table_bytes += bytes;
            }
        }
        held += self
            .barriers
            .values()
            .map(|(s, _)| cost::stream(s))
            .sum::<u64>();
        assert_eq!(timelines, self.timelines, "timelines");
        assert_eq!(holders.len(), self.barriers.len(), "streams held");
        for (name, (_, refs)) in &self.barriers {
            assert_eq!(holders.get(name.as_str()), Some(refs), "{name}");
        }
        assert_eq!(held, self.held, "bytes held");
        assert_eq!(table_bytes, self.table_bytes, "tables' bytes");
        assert!(self.peak >= self.entries.len(), "peak");
        assert!(self.bytes() <= self.max_bytes || self.held == 0, "bound");
    }

    /// A record was appended for `key`.
    pub(crate) fn invalidate(&mut self, key: &SchemaKey) {
        let k = key.backend_key();
        self.invalidate_where(|t| t == k);
    }

    fn invalidate_where(&mut self, stale: impl Fn(&str) -> bool) {
        let keys: Vec<CacheKey> = self
            .entries
            .iter()
            .map(|(key, _)| key)
            .filter(|key| stale(key.table()))
            .cloned()
            .collect();
        for key in keys {
            self.remove(&key);
        }
    }

    /// The record admitted into `timeline` was appended durably, and
    /// nothing else since it was read: it is the table's timeline now.
    fn install(&mut self, key: &SchemaKey, timeline: Arc<Timeline>) {
        self.invalidate(key);
        self.put_timeline(key.backend_key(), timeline);
    }

    /// An atomic batch committed the record admitted into `timeline`: it is
    /// the table's timeline now (see [`Self::install`]).
    pub(crate) fn install_committed(
        &mut self,
        key: &SchemaKey,
        timeline: Arc<Timeline>,
    ) {
        self.install(key, timeline);
    }

    /// Install `timeline` for `key` only while the cache still holds
    /// `snapshot` itself (the same `Arc`): an eviction, a barrier or any
    /// invalidation since the snapshot wins, and the table reloads. Whether
    /// it was installed (an install that does not fit the bound leaves the
    /// table uncached, never stale).
    pub(crate) fn install_if_current(
        &mut self,
        key: &SchemaKey,
        snapshot: &Arc<Timeline>,
        timeline: Arc<Timeline>,
    ) -> bool {
        let current = matches!(
            self.entries.peek(&CacheKey::Timeline(key.backend_key())),
            Some(Entry { cached: Cached::Timeline(t, _), .. })
                if Arc::ptr_eq(t, snapshot)
        );
        if current {
            self.install(key, timeline);
        }
        current
    }

    /// A barrier was appended to `stream` (a lineage's or a database's):
    /// the stream, and every timeline and selection of a table it applies
    /// to, are stale. Every other table stays cached.
    pub(crate) fn barrier_appended(&mut self, stream: &str) {
        self.invalidate_where(|t| t.starts_with(stream));
        if let Some((s, _)) = self.barriers.remove(stream) {
            self.held -= super::mysql_cache_cost::stream(&s);
        }
        self.publish();
    }

    /// Everything is stale (a reload request without a table). The maps
    /// are replaced, so their buckets are released too.
    pub(crate) fn invalidate_all(&mut self) {
        self.entries = lru::LruCache::unbounded();
        self.barriers = Default::default();
        self.timelines = 0;
        self.held = 0;
        self.table_bytes = 0;
        self.peak = 0;
        self.publish();
    }
}

/// What [`CacheProbe::fill`] caches: `tables` tables spread over
/// `databases` databases of one lineage, each with a timeline of `records`
/// baselines (sharing the lineage's and its database's barrier streams, of
/// 3 records each), and, when asked, a selected schema and a registered
/// version of `columns` mixed columns.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct ProbeMix {
    pub tables: usize,
    pub databases: usize,
    pub records: usize,
    pub columns: usize,
    pub selections: bool,
    pub versions: bool,
}

/// A selection cache built as a loaded cache holds its entries, for
/// measuring its estimate against the memory its entries allocate
/// (`tests/mysql_window_memory_e2e.rs`): a measurement seam.
#[doc(hidden)]
pub struct CacheProbe {
    caches: Caches,
}

impl CacheProbe {
    /// An unbounded cache filled with `mix`.
    pub fn fill(mix: ProbeMix) -> Self {
        let mut caches = Caches::with_bytes(u64::MAX / 2, Meters::noop());
        probe_fill(&mut caches, mix);
        Self { caches }
    }

    /// The bytes the cache estimates it holds.
    pub fn estimate(&self) -> u64 {
        self.caches.bytes()
    }

    /// A barrier appended to each of the first `n` databases' streams.
    pub fn invalidate_databases(&mut self, n: usize) {
        for db in 0..n {
            self.caches.barrier_appended(&probe_key(db, 0).0);
        }
    }

    /// Evict the least recently used entries until the estimate is at most
    /// `bytes` (as admission does under a lower bound).
    pub fn evict_to(&mut self, bytes: u64) {
        while self.caches.bytes() > bytes && self.caches.evict_one() {}
    }

    /// Everything invalidated.
    pub fn invalidate_all(&mut self) {
        self.caches.invalidate_all();
    }
}

/// The probe's database barrier stream and table key for `db`, `t`.
fn probe_key(db: usize, t: usize) -> (String, SchemaKey) {
    const L: &str = "0123456789abcdef0123456789abcdef";
    let key =
        SchemaKey::new("probe", "s", L, format!("db{db}"), format!("t{t}"));
    let stream = mysql_activation::barrier_stream(
        &key,
        &mysql_activation::BarrierScope::Database {
            db: format!("db{db}"),
        },
    );
    (stream, key)
}

fn probe_fill(caches: &mut Caches, mix: ProbeMix) {
    use super::mysql_table_schema::MySqlColumn;
    const U: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
    let bytes_of = |s: &Stored| -> u64 {
        let value =
            mysql_activation::encode_record(&s.record).expect("encodes");
        storage::log_entry_bytes(&storage::LogEntryMeta {
            seq: 0,
            stored_at_ms: 0,
            capture_id: Some(s.capture_id.clone()),
            content_hash: None,
            value,
        })
    };
    let stream_of = |name: String| {
        let records: Vec<Stored> = (1..=3)
            .map(|i| Stored {
                capture_id: format!("{i:064x}"),
                record: Record::new(
                    WmPos::MysqlGtid {
                        gtid_set: format!("{U}:1-{i}"),
                    },
                    Kind::Barrier {
                        scope: mysql_activation::BarrierScope::Lineage,
                    },
                ),
            })
            .collect();
        let bytes = records.iter().map(bytes_of).sum();
        Arc::new(BarrierStream {
            stream: name,
            records,
            head: 3,
            bytes,
        })
    };
    // A registered schema of mixed columns, decoded as the registry does.
    let kinds = [
        ("bigint(20) unsigned", "bigint"),
        ("varchar(255)", "varchar"),
        ("enum('new','paid','shipped','returned')", "enum"),
        ("datetime(3)", "datetime"),
        ("decimal(12,2)", "decimal"),
    ];
    let encoded = serde_json::to_vec(&MySqlTableSchema::new(
        (0..mix.columns)
            .map(|i| {
                let (ct, dt) = kinds[i % kinds.len()];
                let mut c = MySqlColumn::new(
                    format!("column_{i}"),
                    ct,
                    dt,
                    i % 2 == 0,
                    i as u32 + 1,
                );
                // Every third column fully described (default, comment,
                // lengths): objects of more keys.
                if i % 3 == 0 {
                    c.default_value = Some("0".into());
                    c.extra = Some("DEFAULT_GENERATED".into());
                    c.comment = Some(format!("what column {i} holds"));
                    c.char_max_length = Some(255);
                    c.char_octet_length = Some(1020);
                    c.collation_id = Some(255);
                    c.numeric_precision = Some(12);
                    c.numeric_scale = Some(2);
                    c.datetime_precision = Some(3);
                }
                c
            })
            .collect(),
    ))
    .expect("encodes");
    let lineage = {
        let (_, key) = probe_key(0, 0);
        stream_of(mysql_activation::barrier_stream(
            &key,
            &mysql_activation::BarrierScope::Lineage,
        ))
    };
    let databases: Vec<Arc<BarrierStream>> = (0..mix.databases.max(1))
        .map(|db| stream_of(probe_key(db, 0).0))
        .collect();
    for t in 0..mix.tables {
        let db = t % databases.len();
        let (_, key) = probe_key(db, t);
        let bk = key.backend_key();
        let records: Vec<Stored> = (0..mix.records)
            .map(|i| Stored {
                capture_id: format!("{:064x}", t * 1_000_000 + i),
                record: Record::new(
                    WmPos::MysqlGtid {
                        gtid_set: format!("{U}:1-{}", 10 + i),
                    },
                    Kind::Baseline {
                        version: 1 + i as i32,
                        schema_hash: format!("{i:064x}"),
                        to: WmPos::MysqlGtid {
                            gtid_set: format!("{U}:1-{}", 11 + i),
                        },
                        scan_digest: format!("{i:064x}"),
                        binds: None,
                        classifier_version: CLASSIFIER_VERSION.into(),
                    },
                ),
            })
            .collect();
        let bytes = records.iter().map(bytes_of).sum();
        caches.put_timeline(
            bk.clone(),
            Arc::new(Timeline {
                records,
                table_head: 1,
                streams: vec![lineage.clone(), databases[db].clone()],
                bytes,
            }),
        );
        let json: serde_json::Value =
            serde_json::from_slice(&encoded).expect("decodes");
        if mix.selections {
            let schema = schema_of(&json).expect("a schema");
            caches.put_selection(
                (bk.clone(), format!("{t:064x}"), format!("{t:064x}")),
                Arc::new(LoadedSchema::new(
                    schema,
                    1,
                    format!("{t:064x}").into(),
                    t as u64,
                )),
            );
        }
        if mix.versions {
            caches.put_version(
                bk,
                schema_registry::SchemaVersion {
                    version: 1,
                    hash: format!("{t:064x}"),
                    schema_json: json,
                    registered_at: chrono::Utc::now(),
                    sequence: t as u64,
                    checkpoint: None,
                },
            );
        }
    }
}

fn invalid(key: &SchemaKey, why: impl Into<String>) -> anyhow::Error {
    TimelineError(format!("{}: {}", key.backend_key(), why.into())).into()
}

fn schema_of(json: &serde_json::Value) -> anyhow::Result<MySqlTableSchema> {
    serde_json::from_value(json.clone())
        .context("decode a registered MySQL schema")
}

/// Every distinct candidate through `high_water` (versions in order; the
/// first version of each hash) and its verdict against `signature`, with the
/// deterministic digest of that set.
pub(crate) async fn evaluate_candidates(
    registry: &DurableSchemaRegistry,
    key: &SchemaKey,
    high_water: i32,
    signature: &Signature,
) -> anyhow::Result<(Vec<(i32, String, Verdict)>, String)> {
    let tm = signature.table_map();
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for v in 1..=high_water {
        let Some(version) = registry.get_version(key, v).await? else {
            continue;
        };
        if !seen.insert(version.hash.clone()) {
            continue;
        }
        let verdict = compare(&schema_of(&version.schema_json)?, &tm);
        out.push((v, version.hash.clone(), verdict));
    }
    let mut h = Sha256::new();
    h.update(b"DeltaForge.FullCandidates.v1\0");
    h.update(SIGNATURE_FORMAT.as_bytes());
    h.update(high_water.to_be_bytes());
    for (v, hash, verdict) in &out {
        let tag: &[u8] = match verdict {
            Verdict::Match => b"match",
            Verdict::Mismatch(_) => b"mismatch",
            Verdict::Unverifiable(_) => b"unverifiable",
        };
        h.update(v.to_be_bytes());
        h.update((hash.len() as u64).to_be_bytes());
        h.update(hash.as_bytes());
        h.update(tag);
    }
    Ok((out, hex::encode(h.finalize())))
}

/// The unique match: exactly one distinct candidate matches and none is
/// unverifiable.
fn unique_match(
    candidates: &[(i32, String, Verdict)],
) -> Result<(i32, String), String> {
    let unverifiable = candidates
        .iter()
        .filter(|c| matches!(c.2, Verdict::Unverifiable(_)))
        .count();
    let matches: Vec<&(i32, String, Verdict)> = candidates
        .iter()
        .filter(|c| c.2 == Verdict::Match)
        .collect();
    match (matches.as_slice(), unverifiable) {
        ([one], 0) => Ok((one.0, one.1.clone())),
        ([], 0) => Err("no recorded or live shape matches the TableMap".into()),
        (_, 0) => Err(format!(
            "{} distinct shapes match the TableMap",
            matches.len()
        )),
        (_, n) => Err(format!("{n} candidate shapes cannot be verified")),
    }
}

/// Validate every piece of decisive evidence in a table's timeline.
#[cfg(test)]
pub(crate) async fn validate(
    registry: &DurableSchemaRegistry,
    key: &SchemaKey,
    timeline: &Timeline,
) -> anyhow::Result<()> {
    validate_planned(registry, key, timeline, &PlannedVersions::new()).await
}

/// [`validate`], with `planned` versions read before the registry's.
pub(crate) async fn validate_planned(
    registry: &DurableSchemaRegistry,
    key: &SchemaKey,
    timeline: &Timeline,
    planned: &PlannedVersions,
) -> anyhow::Result<()> {
    let stream = table_stream(key);
    let mut full_positions: Vec<&WmPos> = Vec::new();
    let mut bound_barriers: Vec<&str> = Vec::new();
    for s in &timeline.records {
        match &s.record.kind {
            Kind::Observed {
                version,
                binds,
                proof,
                full,
            } => match (proof, full) {
                (Some(_), None) => {
                    let ddl = binds.as_deref().ok_or_else(|| {
                        invalid(key, "a forward binding binds nothing")
                    })?;
                    match mysql_activation::resolve_binding_known(
                        registry,
                        key,
                        &timeline.records,
                        ddl,
                        CLASSIFIER_VERSION,
                        planned,
                    )
                    .await?
                    {
                        Some(v) if v == *version => {}
                        _ => {
                            return Err(invalid(
                                key,
                                "inconsistent forward binding",
                            ));
                        }
                    }
                }
                (None, Some(full)) => {
                    if full_positions.contains(&&s.record.position) {
                        return Err(invalid(
                            key,
                            "two FULL observations at one evaluation position",
                        ));
                    }
                    full_positions.push(&s.record.position);
                    validate_full(
                        registry, key, &stream, s, *version, binds, full,
                        timeline,
                    )
                    .await?;
                    if let Some(b) = binds {
                        if timeline.barriers().any(|x| &x.capture_id == b) {
                            if bound_barriers.contains(&b.as_str()) {
                                return Err(invalid(
                                    key,
                                    "two bindings of one barrier",
                                ));
                            }
                            bound_barriers.push(b);
                        }
                    }
                }
                _ => {
                    return Err(invalid(
                        key,
                        "an observed record carries neither or both kinds of evidence",
                    ));
                }
            },
            Kind::Baseline { .. } => {
                validate_baseline(registry, key, &stream, s, timeline, planned)
                    .await?
            }
            Kind::Ddl | Kind::Unknown | Kind::Barrier { .. } => {}
        }
    }
    Ok(())
}

async fn validate_baseline(
    registry: &DurableSchemaRegistry,
    key: &SchemaKey,
    stream: &str,
    s: &Stored,
    timeline: &Timeline,
    planned: &PlannedVersions,
) -> anyhow::Result<()> {
    let Kind::Baseline {
        version,
        schema_hash,
        to,
        binds,
        classifier_version,
        ..
    } = &s.record.kind
    else {
        unreachable!("validate_baseline takes baselines");
    };
    if classifier_version != CLASSIFIER_VERSION {
        return Err(invalid(
            key,
            format!("baseline classifier {classifier_version:?} unsupported"),
        ));
    }
    let hash = match planned.get(version) {
        Some(h) => Some(h.clone()),
        None => registry.version_hash(key, *version).await?,
    };
    if hash.as_deref() != Some(schema_hash.as_str()) {
        return Err(invalid(key, "baseline schema hash is not its version's"));
    }
    if !matches!(
        order_positions(&s.record.position, to),
        CheckpointOrder::Before | CheckpointOrder::Equal
    ) {
        return Err(invalid(key, "baseline R0 is not at or before S"));
    }
    if let Some(b) = binds {
        let bound: Vec<&Stored> =
            timeline.barriers().filter(|x| &x.capture_id == b).collect();
        match bound.as_slice() {
            [one] if one.record.position == s.record.position => {}
            _ => {
                return Err(invalid(
                    key,
                    "baseline binds no barrier at its position",
                ));
            }
        }
    }
    if baseline_capture_id(&key.lineage_hash, stream, &s.record)?
        != s.capture_id
    {
        return Err(invalid(
            key,
            "baseline capture identity does not match its evidence",
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn validate_full(
    registry: &DurableSchemaRegistry,
    key: &SchemaKey,
    stream: &str,
    s: &Stored,
    version: i32,
    binds: &Option<String>,
    full: &FullProof,
    timeline: &Timeline,
) -> anyhow::Result<()> {
    if full.signature_format != SIGNATURE_FORMAT {
        return Err(invalid(
            key,
            format!("signature format {:?} unsupported", full.signature_format),
        ));
    }
    if full.row_position != s.record.position {
        return Err(invalid(
            key,
            "FULL observation is not at its row's position",
        ));
    }
    match (&full.row_event, &full.row_position) {
        (EventIdentity::Gtid { ordinal, .. }, WmPos::MysqlGtid { .. })
        | (EventIdentity::FilePos { ordinal, .. }, WmPos::MysqlBinlog { .. }) =>
        {
            // A rows event always has its ordinal (from 1); 0 is a
            // statement identity, never a FULL row.
            if *ordinal == 0 {
                return Err(invalid(
                    key,
                    "FULL row event identity has no rows-event ordinal",
                ));
            }
        }
        _ => {
            return Err(invalid(
                key,
                "row event identity does not fit its position",
            ));
        }
    }
    // Only FULL metadata selects by signature (MINIMAL never does).
    if !is_full(&full.signature.table_map()) {
        return Err(invalid(
            key,
            "FULL observation of a TableMap without FULL metadata",
        ));
    }
    // The live shape was captured after the row, in the same lineage (the
    // table's stream) and position mode: an earlier, other-mode or otherwise
    // incomparable live position cannot be a post-row capture.
    if !matches!(
        order_positions(&full.row_position, &full.live_position),
        CheckpointOrder::Before | CheckpointOrder::Equal
    ) {
        return Err(invalid(
            key,
            "the live capture is not at or after the row's position",
        ));
    }
    if full.signature.digest() != full.signature_digest {
        return Err(invalid(key, "TableMap signature digest mismatch"));
    }
    let (candidates, digest) =
        evaluate_candidates(registry, key, full.high_water, &full.signature)
            .await?;
    if digest != full.candidates_digest {
        return Err(invalid(key, "candidate set does not reproduce"));
    }
    let (selected, hash) = unique_match(&candidates)
        .map_err(|why| invalid(key, format!("no unique FULL match: {why}")))?;
    if hash != full.schema_hash
        || registry.version_hash(key, version).await?.as_deref()
            != Some(hash.as_str())
        || registry.version_hash(key, selected).await?.as_deref()
            != Some(hash.as_str())
    {
        return Err(invalid(key, "selected version is not the unique match"));
    }
    if !candidates.iter().any(|c| c.1 == full.live_schema_hash) {
        return Err(invalid(
            key,
            "the live capture is not among the candidates",
        ));
    }
    if let Some(b) = binds {
        let bound: Vec<&Stored> = timeline
            .records
            .iter()
            .filter(|x| x.record.kind == Kind::Ddl)
            .chain(timeline.barriers())
            .filter(|x| &x.capture_id == b)
            .collect();
        match bound.as_slice() {
            [one] if one.record.position == s.record.position => {}
            _ => {
                return Err(invalid(
                    key,
                    "FULL observation binds no ddl or barrier at its position",
                ));
            }
        }
    }
    if full_capture_id(&key.lineage_hash, stream, &s.record)? != s.capture_id {
        return Err(invalid(
            key,
            "FULL capture identity does not match its evidence",
        ));
    }
    Ok(())
}

/// The table's validated timeline: cached, or read now. A miss reads it
/// together with the other uncached tables the stream reaches next (read
/// ahead or held), at most [`BATCH_MAX_TABLES`], in one bounded batch
/// (`mysql_history`): each installed as validated exactly as its own read
/// would. A timeline over its limit stops the source
/// (`mysql_timeline_too_large`) before the table's rows are dispatched.
///
/// [`BATCH_MAX_TABLES`]: super::mysql_history::BATCH_MAX_TABLES
pub(crate) async fn timeline(
    ctx: &mut RunCtx,
    key: &SchemaKey,
) -> SourceResult<Arc<Timeline>> {
    let k = key.backend_key();
    if let Some(t) = ctx.selection.timeline(&k) {
        crate::perf_phases::cache_lookup("timeline", true);
        return Ok(t);
    }
    crate::perf_phases::cache_lookup("timeline", false);
    let mut keys = vec![key.clone()];
    if let Ok(scope) = ctx.registry_scope.current() {
        let room = ctx.selection.batch_room();
        for (db, table) in upcoming(ctx) {
            if keys.len() >= room {
                break;
            }
            let other = scope.key(&db, &table);
            if ctx.allow.matches(&db, &table)
                && !keys.contains(&other)
                && ctx.selection.timeline(&other.backend_key()).is_none()
            {
                keys.push(other);
            }
        }
    }
    let _phase = crate::perf_phases::Phase::start("history_load");
    let mine = install(ctx, &keys, |k| k == key, "history").await?;
    mine.expect("the missed table is read")
}

/// Read the timelines of `keys` not already cached in bounded batches
/// (`mysql_history`) and install each one that decodes, fits its limit and
/// validates exactly as [`timeline`] would. A table left out is read (and
/// reported) when it is reached; a failed store read installs nothing.
/// Called only once a window is current: a speculative window never
/// touches the cache.
pub(crate) async fn load_timelines(ctx: &mut RunCtx, keys: &[SchemaKey]) {
    let mut missing: Vec<SchemaKey> = Vec::new();
    for k in keys {
        if ctx.selection.timeline(&k.backend_key()).is_none()
            && !missing.contains(k)
        {
            missing.push(k.clone());
        }
    }
    if missing.is_empty() {
        return;
    }
    let _phase = crate::perf_phases::Phase::start("history_prefetch");
    let _ = install(ctx, &missing, |_| false, "history_prefetch").await;
}

/// Read `keys`' histories (those `alone` names read alone if their batch
/// could not take them) and install every valid timeline within its limit,
/// with the versions their last proofs name read ahead. One load
/// ([`Admission`]): what it admits never exceeds half the cache (its first
/// timeline may take all of it) and is never evicted by itself. A
/// statement's tables ([`BATCH_MAX_TABLES`]) are read and admitted before
/// the next statement's are read, so a load holds one statement's histories
/// at a time; once nothing more fits, the rest is left unread (read when
/// reached). The outcome for the key `alone` names, if any: its timeline
/// whether or not it was cached.
///
/// [`BATCH_MAX_TABLES`]: super::mysql_history::BATCH_MAX_TABLES
async fn install(
    ctx: &mut RunCtx,
    keys: &[SchemaKey],
    alone: impl Fn(&SchemaKey) -> bool,
    read_kind: &'static str,
) -> SourceResult<Option<SourceResult<Arc<Timeline>>>> {
    let mut adm = ctx.selection.begin_load();
    let out = install_load(ctx, keys, &alone, read_kind, &mut adm).await;
    ctx.selection.end_load();
    out
}

async fn install_load(
    ctx: &mut RunCtx,
    keys: &[SchemaKey],
    alone: &impl Fn(&SchemaKey) -> bool,
    read_kind: &'static str,
    adm: &mut Admission,
) -> SourceResult<Option<SourceResult<Arc<Timeline>>>> {
    use super::mysql_history::Loaded;
    let registry = ctx.schema.registry_arc();
    let (mut requested, mut read_bytes) = (0u64, 0u64);
    let mut mine = None;
    for chunk in keys.chunks(super::mysql_history::BATCH_MAX_TABLES) {
        if adm.spent() && !chunk.iter().any(alone) {
            break;
        }
        let mut have = ctx.selection.barrier_streams();
        let read = super::mysql_history::read_each(
            &ctx.registry_backend,
            &registry,
            chunk,
            &mut have,
            alone,
        )
        .await
        .map_err(timeline_error)?;
        requested += chunk.len() as u64;
        read_bytes += read
            .iter()
            .map(|l| match l {
                Loaded::Ok { table, .. } => table.bytes,
                _ => 0,
            })
            .sum::<u64>();
        let mut installed = Vec::new();
        for (key, loaded) in chunk.iter().zip(read) {
            let outcome = match loaded {
                Loaded::Ok {
                    table,
                    streams,
                    hashes,
                } => {
                    let t = Timeline {
                        records: table.records,
                        table_head: table.head,
                        streams,
                        bytes: table.bytes,
                    };
                    match validate_planned(&registry, key, &t, &hashes).await {
                        Ok(()) => {
                            let t = ctx.selection.admit_timeline(
                                adm,
                                key.backend_key(),
                                Arc::new(t),
                            );
                            installed.push(key);
                            Ok(t)
                        }
                        Err(e) => Err(timeline_error(e)),
                    }
                }
                Loaded::Invalid(e) => Err(timeline_error(e.into())),
                Loaded::TooLarge(e) => Err(timeline_too_large(ctx, &e)),
                Loaded::Unread => continue,
            };
            if alone(key) {
                mine = Some(outcome);
            }
        }
        // The versions the cached tables' last proofs name, read ahead in
        // one batch and admitted in the tables' order: a returning table's
        // selection then reads nothing more.
        let mut wanted = Vec::new();
        for key in installed {
            let bk = key.backend_key();
            let Some(t) = ctx.selection.timeline(&bk) else {
                continue;
            };
            let last =
                t.records.iter().rev().find_map(|r| match &r.record.kind {
                    Kind::Observed { version, .. }
                    | Kind::Baseline { version, .. } => Some(*version),
                    _ => None,
                });
            if let Some(v) = last
                && ctx.selection.version(&bk, v).is_none()
            {
                wanted.push((key.clone(), v));
            }
        }
        if !wanted.is_empty() && !adm.spent() {
            crate::perf_phases::store_read("versions", wanted.len() as u64, 0);
            let _phase = crate::perf_phases::Phase::start("version_prefetch");
            if let Ok(read) = registry.get_versions(&wanted).await {
                for ((key, _), v) in wanted.iter().zip(read) {
                    if let Some(v) = v {
                        ctx.selection.admit_version(adm, key.backend_key(), v);
                    }
                }
            }
        }
    }
    if requested > 0 {
        crate::perf_phases::store_read(read_kind, requested, read_bytes);
    }
    Ok(mine)
}

/// Append a barrier for `scope` and invalidate what it makes stale: the
/// barrier stream, and the timelines and selections of every table it
/// applies to (`Caches::barrier_appended`). The only way the source writes
/// a barrier (its two writers: the stream start and DDL handling), so every
/// append keeps the cache exact. Invalidated whatever the append returned:
/// a failed append may still have been stored.
pub(crate) async fn append_barrier(
    backend: &storage::ArcStorageBackend,
    registry: &DurableSchemaRegistry,
    selection: &mut Caches,
    key: &SchemaKey,
    scope: mysql_activation::BarrierScope,
    event: &EventIdentity,
    position: WmPos,
) -> anyhow::Result<storage::AppendStatus> {
    let stream = mysql_activation::barrier_stream(key, &scope);
    let appended = mysql_activation::record_barrier(
        backend, registry, key, scope, event, position,
    )
    .await;
    selection.barrier_appended(&stream);
    appended
}

/// The tables the stream reaches next: those of the events read ahead and
/// held, in order, each once.
fn upcoming(ctx: &RunCtx) -> Vec<(String, String)> {
    use mysql_binlog_connector_rust::event::event_data::EventData;
    fn visit(d: &EventData, out: &mut Vec<(String, String)>) {
        match d {
            EventData::TableMap(tm) => {
                let t = (tm.database_name.clone(), tm.table_name.clone());
                if !out.contains(&t) {
                    out.push(t);
                }
            }
            EventData::TransactionPayload(tp) => {
                for (_, d) in &tp.uncompressed_events {
                    visit(d, out);
                }
            }
            _ => {}
        }
    }
    let mut out = Vec::new();
    for (_, d) in &ctx.lookahead {
        visit(d, &mut out);
    }
    for (_, _, _, d) in &ctx.prefetched {
        visit(d, &mut out);
    }
    out
}

/// The `mysql_timeline_too_large` stop: nothing of the table's rows is
/// dispatched, the checkpoint stays before them.
fn timeline_too_large(
    ctx: &RunCtx,
    e: &super::mysql_history::TooLarge,
) -> SourceError {
    let at = super::MySqlCheckpoint {
        file: ctx.last_file.clone(),
        pos: ctx.last_event_start,
        gtid_set: ctx.gtid_text(),
        lineage: None,
        snapshot_completed: None,
        snapshot_chain: None,
    };
    crate::incident_drafts::mysql_timeline_too_large(&ctx.source_id, e, &at)
}

fn timeline_error(e: anyhow::Error) -> SourceError {
    SourceError::Schema {
        details: format!("schema activation timeline: {e:#}").into(),
    }
}

fn fail(db: &str, table: &str, why: &str) -> SourceError {
    SourceError::Schema {
        details: format!(
            "table {db}.{table}: {why}. The rows' schema version cannot be \
             proven, so no event was emitted and the checkpoint was not \
             advanced (fail-closed). Remediation: re-snapshot (restart once \
             with snapshot mode 'always')."
        )
        .into(),
    }
}

/// [`loaded`], from the version read ahead with the table's timeline when
/// the cache holds it.
async fn loaded_cached(
    ctx: &mut RunCtx,
    key: &SchemaKey,
    version: i32,
) -> SourceResult<(Arc<LoadedSchema>, MySqlTableSchema)> {
    match ctx.selection.version(&key.backend_key(), version) {
        Some(v) => {
            crate::perf_phases::cache_lookup("version", true);
            loaded_from(&v, version)
        }
        None => {
            crate::perf_phases::cache_lookup("version", false);
            crate::perf_phases::store_read("version", 1, 0);
            let _phase = crate::perf_phases::Phase::start("version_load");
            loaded(ctx.schema.registry(), key, version).await
        }
    }
}

async fn loaded(
    registry: &DurableSchemaRegistry,
    key: &SchemaKey,
    version: i32,
) -> SourceResult<(Arc<LoadedSchema>, MySqlTableSchema)> {
    let v = registry
        .get_version(key, version)
        .await
        .map_err(timeline_error)?
        .ok_or_else(|| {
            timeline_error(anyhow::anyhow!("version {version} is missing"))
        })?;
    loaded_from(&v, version)
}

fn loaded_from(
    v: &schema_registry::SchemaVersion,
    version: i32,
) -> SourceResult<(Arc<LoadedSchema>, MySqlTableSchema)> {
    debug_assert_eq!(v.version, version);
    let schema = schema_of(&v.schema_json).map_err(timeline_error)?;
    Ok((
        Arc::new(LoadedSchema::new(
            schema.clone(),
            v.version,
            v.hash.as_str().into(),
            v.sequence,
        )),
        schema,
    ))
}

/// Whether `db.table` has no positional proof of its schema at `at` (the
/// rows of a transaction starting there would need a lazy baseline). The
/// same cached timeline and selection as [`select_for_rows`].
pub(crate) async fn unproven_at(
    ctx: &mut RunCtx,
    db: &str,
    table: &str,
    at: &WmPos,
) -> SourceResult<bool> {
    let key = ctx.registry_scope.current()?.key(db, table);
    // Proven at its last selection, and unchanged since: still proven.
    if ctx.selection.is_proven(&key) {
        return Ok(false);
    }
    let t = timeline(ctx, &key).await?;
    Ok(matches!(t.select_decisive(at).0, Selection::Unproven(_)))
}

/// The schema to decode `tm`'s rows with (see the module docs).
pub(crate) async fn select_for_rows(
    ctx: &mut RunCtx,
    tm: &TableMapEvent,
) -> SourceResult<Arc<LoadedSchema>> {
    let (db, table) = (tm.database_name.as_str(), tm.table_name.as_str());
    let key = ctx.registry_scope.current()?.key(db, table);
    let Some(row) = ctx.txn_eval.clone() else {
        return Err(fail(db, table, "the rows have no evaluation position"));
    };
    let signature = Signature::of(tm);
    let sig_digest = signature.digest();
    let mut t = timeline(ctx, &key).await?;
    // After a failover, the table's drift policy applies before anything is
    // registered, baselined, decoded or emitted for these rows (once per
    // table and run: a checked table needs no copy of its timeline).
    if let Some(row_cp) = ctx.txn_eval_cp.clone()
        && !ctx.drift_checked.contains(&key.backend_key())
    {
        let all = t.all();
        super::mysql_failover_drift::check_in_stream(
            ctx,
            db,
            table,
            super::mysql_failover_drift::FirstEvent::Rows {
                at: &row,
                at_cp: &row_cp,
                timeline: &all,
            },
        )
        .await?;
    }
    let (mut selection, mut decisive) = t.select_decisive(&row);
    // No positional proof yet: a lazy baseline at the rows' position, if it
    // can be proven and decides the rows (before any FULL fallback).
    if let Selection::Unproven(_) = selection
        && super::mysql_baseline::establish_at(ctx, db, table, &key, &t).await?
    {
        t = timeline(ctx, &key).await?;
        (selection, decisive) = t.select_decisive(&row);
    }
    match selection {
        Selection::Proven { version } => {
            ctx.selection.mark_proven(&key);
            let decisive = decisive.unwrap_or_default();
            let cache_key = (key.backend_key(), decisive, sig_digest);
            if let Some(hit) = ctx.selection.selection(&cache_key) {
                crate::perf_phases::cache_lookup("selection", true);
                return Ok(hit);
            }
            crate::perf_phases::cache_lookup("selection", false);
            let (loaded, schema) = loaded_cached(ctx, &key, version).await?;
            // The proven version comes from authoritative complete-schema
            // evidence; it must not contradict the TableMap.
            if let Verdict::Mismatch(why) = compare(&schema, tm) {
                return Err(fail(
                    db,
                    table,
                    &format!(
                        "the proven version {version} contradicts the binlog rows ({why})"
                    ),
                ));
            }
            ctx.selection.put_selection(cache_key, loaded.clone());
            Ok(loaded)
        }
        Selection::Unproven(reason) if !is_full(tm) => Err(fail(
            db,
            table,
            &format!(
                "no positional proof of its schema at these rows ({reason:?}) and \
                 binlog_row_metadata is not FULL"
            ),
        )),
        Selection::Unproven(reason) => {
            info!(source_id = %ctx.source_id, %db, %table, ?reason, "no positional proof; FULL signature selection");
            full_fallback(ctx, tm, &key, &row, signature, sig_digest, &t).await
        }
    }
}

/// A proposed record (a FULL observation or a lazy baseline) is admitted
/// only if, added to the current timeline, the whole timeline still
/// validates and selection at `row` proves the record's version with the
/// record itself as the decisive one. Any other decisive record at that
/// position (an unmatched ddl or barrier, an `unknown`, an incomparable
/// record) keeps the row unproven.
pub(crate) async fn admit_decisive(
    registry: &DurableSchemaRegistry,
    key: &SchemaKey,
    current: &Timeline,
    proposed: Stored,
    row: &WmPos,
) -> Result<(), String> {
    admit_decisive_timeline(registry, key, current, proposed, row)
        .await
        .map(drop)
}

/// [`admit_decisive`], returning the validated timeline with the proposed
/// record added (the timeline once it is appended).
pub(crate) async fn admit_decisive_timeline(
    registry: &DurableSchemaRegistry,
    key: &SchemaKey,
    current: &Timeline,
    proposed: Stored,
    row: &WmPos,
) -> Result<Timeline, String> {
    admit_decisive_planned(
        registry,
        key,
        current,
        proposed,
        row,
        &PlannedVersions::new(),
    )
    .await
}

/// [`admit_decisive_timeline`] for an atomic batch: the proposed record's
/// version is `planned`, not yet registered.
pub(crate) async fn admit_decisive_planned(
    registry: &DurableSchemaRegistry,
    key: &SchemaKey,
    current: &Timeline,
    proposed: Stored,
    row: &WmPos,
    planned: &PlannedVersions,
) -> Result<Timeline, String> {
    let version = match &proposed.record.kind {
        Kind::Observed { version, .. } | Kind::Baseline { version, .. } => {
            *version
        }
        _ => return Err("the proposed record proves no version".into()),
    };
    let mut augmented = current.clone();
    augmented.records.push(proposed.clone());
    validate_planned(registry, key, &augmented, planned)
        .await
        .map_err(|e| format!("{e:#}"))?;
    match select_decisive(&augmented.all(), row) {
        (Selection::Proven { version: v }, Some(decisive))
            if v == version && decisive == proposed.capture_id =>
        {
            Ok(augmented)
        }
        (selection, _) => Err(format!(
            "the proposed record would not resolve selection at the rows ({selection:?})"
        )),
    }
}

/// Append the record [`admit_decisive_timeline`] admitted into `augmented`,
/// and keep `caches` exact. The augmented timeline (already validated) is
/// installed only when the record was newly stored and the table stream
/// holds nothing else since `augmented` was read: then it is what a fresh
/// read would return. A record already present, or a stream that changed
/// meanwhile, discards the cached timeline (the next use reads it again).
/// A failed write or check discards it too, and fails.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn append_admitted(
    backend: &storage::ArcStorageBackend,
    registry: &DurableSchemaRegistry,
    caches: &mut Caches,
    key: &SchemaKey,
    capture_id: &str,
    record: &super::mysql_activation::Record,
    mut augmented: Timeline,
) -> anyhow::Result<()> {
    let stream = mysql_activation::table_stream(key);
    let out = match mysql_activation::append_outcome(
        backend,
        registry,
        key,
        mysql_activation::ACTIVATION_NS,
        &stream,
        capture_id,
        record,
    )
    .await
    {
        Ok(out) => out,
        Err(e) => {
            caches.invalidate(key);
            return Err(e);
        }
    };
    let alone = match out.status {
        storage::AppendStatus::Inserted => {
            match mysql_activation::appended_alone(
                backend,
                key,
                augmented.table_head,
                out.seq,
            )
            .await
            {
                Ok(alone) => alone,
                Err(e) => {
                    caches.invalidate(key);
                    return Err(e);
                }
            }
        }
        storage::AppendStatus::AlreadyPresent => false,
    };
    if alone {
        augmented.table_head = out.seq;
        caches.install(key, Arc::new(augmented));
    } else {
        caches.invalidate(key);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn full_fallback(
    ctx: &mut RunCtx,
    tm: &TableMapEvent,
    key: &SchemaKey,
    row: &WmPos,
    signature: Signature,
    signature_digest: String,
    t: &Timeline,
) -> SourceResult<Arc<LoadedSchema>> {
    let (db, table) = (tm.database_name.as_str(), tm.table_name.as_str());
    let server_uuid = ctx.expected_uuid()?;
    let lineage = key.lineage_hash.clone();

    // The exact DDL or applicable barrier this observation closes (at most
    // one at the row's position).
    let at_row: Vec<&Stored> = t
        .records
        .iter()
        .filter(|s| s.record.kind == Kind::Ddl)
        .chain(t.barriers())
        .filter(|s| {
            order_positions(&s.record.position, row) == CheckpointOrder::Equal
        })
        .collect();
    let binds = match at_row.as_slice() {
        [] => None,
        [one] => Some(one.capture_id.clone()),
        _ => {
            return Err(fail(
                db,
                table,
                "several DDL/barrier records at the rows' position",
            ));
        }
    };

    // The live shape, captured with its own interval proven free of anything
    // affecting the table, and only then registered through the normal
    // registry path (never derived from the TableMap).
    // A stop ends the selection (`?`): nothing is decided.
    let cap = match capture_proven(
        &ctx.capture,
        super::mysql_helpers::derive_server_id(&format!(
            "{}/capture",
            ctx.source_id
        )),
        &server_uuid,
        &lineage,
        db,
        table,
        ctx.lower_case_table_names,
        CAPTURE_ATTEMPTS,
        &ScanLimits::default(),
        None,
        &super::mysql_binlog_scan::ScanTag {
            source_id: &ctx.source_id,
            kind: super::mysql_binlog_scan::ProofKind::FullFallback,
        },
        Some(super::mysql_binlog_scan::SharedProofs {
            scanner: &ctx.proof_scanner,
            boundary: ctx.txn_eval_cp.as_ref(),
            // The capture interval starts at the capture's own position
            // read, never a stream boundary: no stream anchor.
            anchor: None,
        }),
    )
    .await?
    {
        Ok(cap) => cap,
        Err(ProofError::OtherServer(found)) => {
            return Err(SourceError::Lineage {
                details: format!("a schema capture reached another server ({found}), expected {server_uuid}").into(),
            });
        }
        Err(e) => {
            return Err(fail(
                db,
                table,
                &format!("no proven live capture ({e})"),
            ));
        }
    };
    let checkpoint = serde_json::to_vec(&cap.position)
        .map_err(|e| SourceError::Other(e.into()))?;
    let (_, live_hash) = ctx
        .schema
        .register_captured(db, table, &cap.schema, &checkpoint)
        .await?;
    let live_position = crate::durable_checkpoint::mysql_checkpoint_position(
        &cap.position.file,
        cap.position.pos,
        cap.position.gtid_set.as_deref(),
    )
    .ok_or_else(|| fail(db, table, "unparseable live capture position"))?;

    let registry = ctx.schema.registry();
    let high_water = registry
        .get_latest(key)
        .await
        .map_err(timeline_error)?
        .map(|v| v.version)
        .unwrap_or(0);
    let (candidates, candidates_digest) =
        evaluate_candidates(registry, key, high_water, &signature)
            .await
            .map_err(timeline_error)?;
    let (version, schema_hash) =
        unique_match(&candidates).map_err(|why| fail(db, table, &why))?;

    // The rows event that establishes it: its transaction and its ordinal
    // among that transaction's rows events.
    let row_event = match &ctx.current_gtid {
        Some(gtid) => EventIdentity::Gtid {
            gtid: gtid.clone(),
            ordinal: ctx.rows_ordinal,
        },
        None => EventIdentity::FilePos {
            file: ctx.last_file.clone(),
            end_pos: ctx.last_pos,
            ordinal: ctx.rows_ordinal,
        },
    };
    let record = Record::new(
        row.clone(),
        Kind::Observed {
            version,
            binds,
            proof: None,
            full: Some(Box::new(FullProof {
                signature_format: SIGNATURE_FORMAT.to_string(),
                row_event,
                row_position: row.clone(),
                signature,
                signature_digest,
                schema_hash,
                live_position,
                live_schema_hash: live_hash,
                high_water,
                candidates_digest,
            })),
        },
    );
    let stream = table_stream(key);
    let id =
        full_capture_id(&lineage, &stream, &record).map_err(timeline_error)?;
    // Nothing is written, and the row not emitted, unless this observation
    // resolves selection at the row once it is part of the timeline.
    let proposed = Stored {
        capture_id: id.clone(),
        record: record.clone(),
    };
    admit_decisive(registry, key, t, proposed, row)
        .await
        .map_err(|why| fail(db, table, &why))?;
    mysql_activation::append(
        &ctx.registry_backend,
        registry,
        key,
        ACTIVATION_NS,
        &stream,
        &id,
        &record,
    )
    .await
    .map_err(|e| {
        SourceError::Other(e.context("persist a FULL schema observation"))
    })?;
    ctx.selection.invalidate(key);
    warn!(source_id = %ctx.source_id, %db, %table, version, "schema version selected by its FULL TableMap signature");
    let (loaded, _) = loaded(ctx.schema.registry(), key, version).await?;
    Ok(loaded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mysql::mysql_table_schema::MySqlColumn;

    /// Under churn the cache never holds more than its bytes, and evicts
    /// (counted under the source's labels) only to make room.
    #[test]
    fn the_byte_bound_holds_under_churn() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};
        let rec = DebuggingRecorder::new();
        let puts = metrics::with_local_recorder(&rec, || {
            let mut caches = Caches::new("p", "s");
            caches.max_bytes = 20 * 1024;
            let timeline = Arc::new(Timeline {
                table_head: 0,
                ..Timeline::of(Vec::new(), Vec::new())
            });
            let loaded = Arc::new(LoadedSchema::new(
                MySqlTableSchema::new(Vec::new()),
                1,
                "h".into(),
                1,
            ));
            let mut puts = 0u64;
            for i in 0..2_000 {
                let table = format!("t{}", i % 300);
                caches.put_timeline(table.clone(), timeline.clone());
                caches.put_selection(
                    (table, format!("d{i}"), "sig".into()),
                    loaded.clone(),
                );
                puts += 2;
                assert!(caches.bytes() <= caches.max_bytes);
            }
            assert!(caches.timelines() > 0);
            caches.audit();
            puts
        });
        let evictions: Vec<_> = rec
            .snapshotter()
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(k, ..)| k.key().name() == METRIC_EVICTIONS)
            .collect();
        assert_eq!(evictions.len(), 1, "one series per source");
        let (key, _, _, value) = &evictions[0];
        let labels: Vec<_> =
            key.key().labels().map(|l| l.key().to_string()).collect();
        assert_eq!(labels, ["pipeline", "source"], "no table label");
        // Re-putting a cached timeline replaces it (not an eviction).
        let DebugValue::Counter(n) = value else {
            panic!("a counter")
        };
        assert!(*n > 0 && *n < puts);
    }

    /// The cache's own accounting (estimated bytes and timelines) is
    /// exported under the source's labels after every change: puts,
    /// evictions, a barrier append and a full invalidation.
    #[test]
    fn the_cache_accounting_is_exported() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};
        let rec = DebuggingRecorder::new();
        let snap = rec.snapshotter();
        let gauge = |name: &str| -> (Vec<String>, f64) {
            let found: Vec<_> = snap
                .snapshot()
                .into_vec()
                .into_iter()
                .filter(|(k, ..)| k.key().name() == name)
                .collect();
            assert_eq!(found.len(), 1, "one {name} series per source");
            let (key, _, _, value) = &found[0];
            let DebugValue::Gauge(v) = value else {
                panic!("{name} is a gauge")
            };
            let labels = key.key().labels().map(|l| l.key().to_string());
            (labels.collect(), v.into_inner())
        };
        metrics::with_local_recorder(&rec, || {
            let mut caches = Caches::new("p", "s");
            caches.max_bytes = 20 * 1024;
            let stream = Arc::new(BarrierStream {
                stream: "lineage/x".into(),
                records: Vec::new(),
                head: 0,
                bytes: 512,
            });
            let timeline = Arc::new(Timeline {
                table_head: 0,
                streams: vec![stream],
                ..Timeline::of(Vec::new(), Vec::new())
            });
            let loaded = Arc::new(LoadedSchema::new(
                MySqlTableSchema::new(Vec::new()),
                1,
                "h".into(),
                1,
            ));
            let check = |caches: &Caches| {
                let (labels, bytes) = gauge(METRIC_BYTES);
                assert_eq!(labels, ["pipeline", "source"], "no table label");
                assert_eq!(bytes, caches.bytes() as f64);
                let (_, timelines) = gauge(METRIC_TIMELINES);
                assert_eq!(timelines, caches.timelines() as f64);
            };
            for i in 0..40 {
                let table = format!("lineage/x/t{i}");
                caches.put_timeline(table.clone(), timeline.clone());
                caches.put_selection(
                    (table, format!("d{i}"), "sig".into()),
                    loaded.clone(),
                );
                check(&caches);
            }
            assert!(caches.timelines() > 0 && caches.bytes() > 0);
            caches.barrier_appended("lineage/x");
            check(&caches);
            caches.put_timeline("lineage/y/t".into(), timeline.clone());
            check(&caches);
            caches.invalidate_all();
            check(&caches);
            assert_eq!(gauge(METRIC_BYTES).1, 0.0);
        });
    }

    #[test]
    fn invalidation_drops_only_the_tables_entries() {
        let mut caches = Caches::default();
        let t = Arc::new(Timeline {
            table_head: 0,
            ..Timeline::of(Vec::new(), Vec::new())
        });
        let loaded = Arc::new(LoadedSchema::new(
            MySqlTableSchema::new(Vec::new()),
            1,
            "h".into(),
            1,
        ));
        for table in ["a", "b"] {
            caches.put_timeline(table.into(), t.clone());
            caches.put_selection(
                (table.into(), "d".into(), "s".into()),
                loaded.clone(),
            );
        }
        let key = SchemaKey::new(
            "t",
            "s",
            "0123456789abcdef0123456789abcdef",
            "d",
            "x",
        );
        caches.put_timeline(key.backend_key(), t.clone());
        caches
            .put_selection((key.backend_key(), "d".into(), "s".into()), loaded);
        assert_eq!(caches.len(), 6);
        caches.invalidate(&key);
        assert_eq!(caches.len(), 4);
        assert!(caches.timeline("a").is_some());
        assert!(caches.timeline(&key.backend_key()).is_none());
        caches.invalidate_all();
        assert_eq!(caches.len(), 0);
    }

    #[test]
    fn a_unique_match_needs_exactly_one_match_and_nothing_unverifiable() {
        let c = |v: i32, h: &str, verdict: Verdict| (v, h.to_string(), verdict);
        let mis = || Verdict::Mismatch("x".into());
        let unv = || Verdict::Unverifiable("x".into());
        assert_eq!(
            unique_match(&[c(1, "a", mis()), c(2, "b", Verdict::Match)]),
            Ok((2, "b".into()))
        );
        assert!(
            unique_match(&[
                c(1, "a", Verdict::Match),
                c(2, "b", Verdict::Match)
            ])
            .is_err()
        );
        assert!(unique_match(&[c(1, "a", mis())]).is_err());
        assert!(
            unique_match(&[c(1, "a", Verdict::Match), c(2, "b", unv())])
                .is_err()
        );
    }

    #[tokio::test]
    async fn the_candidate_digest_binds_versions_hashes_and_verdicts() {
        let backend: storage::ArcStorageBackend =
            Arc::new(storage::MemoryStorageBackend::new());
        let reg = DurableSchemaRegistry::new(backend.clone()).await.unwrap();
        let key = SchemaKey::new(
            "t",
            "s",
            "0123456789abcdef0123456789abcdef",
            "d",
            "x",
        );
        let schema = |n: &str| {
            let mut c = MySqlColumn::new(n, "int", "int", true, 1);
            c.char_octet_length = None;
            MySqlTableSchema::new(vec![c])
        };
        for (h, s) in [("h1", schema("a")), ("h2", schema("b"))] {
            reg.register_with_checkpoint(
                &key,
                h,
                &serde_json::to_value(&s).unwrap(),
                None,
            )
            .await
            .unwrap();
        }
        let sig = Signature {
            column_types: vec![3],
            column_metas: vec![0],
            null_bits: vec![true],
            table_metadata: None,
        };
        let (c1, d1) = evaluate_candidates(&reg, &key, 2, &sig).await.unwrap();
        assert_eq!(c1.len(), 2);
        let (_, again) =
            evaluate_candidates(&reg, &key, 2, &sig).await.unwrap();
        assert_eq!(d1, again);
        // Fewer versions examined, or another signature: another digest.
        let (_, d_hw) = evaluate_candidates(&reg, &key, 1, &sig).await.unwrap();
        assert_ne!(d1, d_hw);
        let other = Signature {
            column_types: vec![8],
            ..sig.clone()
        };
        let (_, d_sig) =
            evaluate_candidates(&reg, &key, 2, &other).await.unwrap();
        assert_ne!(d1, d_sig);
        // Later versions do not change a stored high-water's set.
        reg.register_with_checkpoint(
            &key,
            "h3",
            &serde_json::to_value(schema("c")).unwrap(),
            None,
        )
        .await
        .unwrap();
        let (_, later) =
            evaluate_candidates(&reg, &key, 2, &sig).await.unwrap();
        assert_eq!(d1, later);
    }

    const UUID: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562";

    fn gtid(set: &str) -> WmPos {
        WmPos::MysqlGtid {
            gtid_set: format!("{UUID}:{set}"),
        }
    }

    fn row_event(ordinal: u32) -> EventIdentity {
        EventIdentity::Gtid {
            gtid: format!("{UUID}:6"),
            ordinal,
        }
    }

    /// One registered version (a single INT column `a`) and its FULL and
    /// MINIMAL TableMap signatures.
    struct Fixture {
        backend: storage::ArcStorageBackend,
        reg: Arc<DurableSchemaRegistry>,
        key: SchemaKey,
        full: Signature,
        minimal: Signature,
    }

    async fn fixture() -> Fixture {
        use mysql_binlog_connector_rust::event::table_map::table_metadata::{
            ColumnMetadata, TableMetadata,
        };
        let backend: storage::ArcStorageBackend =
            Arc::new(storage::MemoryStorageBackend::new());
        let reg = DurableSchemaRegistry::new(backend.clone()).await.unwrap();
        let key = SchemaKey::new(
            "t",
            "s",
            "0123456789abcdef0123456789abcdef",
            "d",
            "x",
        );
        let schema = MySqlTableSchema::new(vec![MySqlColumn::new(
            "a", "int", "int", true, 1,
        )]);
        reg.register_with_checkpoint(
            &key,
            "h1",
            &serde_json::to_value(&schema).unwrap(),
            None,
        )
        .await
        .unwrap();
        let signature = |metadata: Option<TableMetadata>| Signature {
            column_types: vec![3],
            column_metas: vec![0],
            null_bits: vec![true],
            table_metadata: metadata,
        };
        let full = signature(Some(TableMetadata {
            default_charset: None,
            enum_and_set_default_charset: None,
            columns: vec![ColumnMetadata {
                column_name: Some("a".into()),
                is_signed: Some(true),
                ..Default::default()
            }],
            primary_key: Vec::new(),
        }));
        assert_eq!(compare(&schema, &full.table_map()), Verdict::Match);
        Fixture {
            backend,
            reg,
            key,
            full,
            minimal: signature(None),
        }
    }

    /// A FULL observation at `row` over the key's only version, otherwise
    /// complete and consistent (digests and capture identity derived).
    async fn full_observation(
        f: &Fixture,
        signature: Signature,
        row: WmPos,
        live: WmPos,
        event: EventIdentity,
        binds: Option<&str>,
    ) -> Stored {
        let (candidates, candidates_digest) =
            evaluate_candidates(&f.reg, &f.key, 1, &signature)
                .await
                .unwrap();
        let (version, hash) = unique_match(&candidates).unwrap();
        let record = Record::new(
            row.clone(),
            Kind::Observed {
                version,
                binds: binds.map(str::to_string),
                proof: None,
                full: Some(Box::new(FullProof {
                    signature_format: SIGNATURE_FORMAT.to_string(),
                    row_event: event,
                    row_position: row,
                    signature_digest: signature.digest(),
                    signature,
                    schema_hash: hash.clone(),
                    live_position: live,
                    live_schema_hash: hash,
                    high_water: 1,
                    candidates_digest,
                })),
            },
        );
        let capture_id = full_capture_id(
            &f.key.lineage_hash,
            &table_stream(&f.key),
            &record,
        )
        .unwrap();
        Stored { capture_id, record }
    }

    async fn check(f: &Fixture, records: Vec<Stored>) -> Result<(), String> {
        let t = Timeline {
            table_head: 0,
            ..Timeline::of(records, Vec::new())
        };
        validate(&f.reg, &f.key, &t)
            .await
            .map_err(|e| format!("{e:#}"))
    }

    #[tokio::test]
    async fn full_evidence_needs_full_metadata_and_a_live_capture_after_the_row()
     {
        let f = fixture().await;
        let row = gtid("1-5");
        let obs = |sig: &Signature, live: WmPos| {
            full_observation(
                &f,
                sig.clone(),
                row.clone(),
                live,
                row_event(1),
                None,
            )
        };

        // A live capture at or after the row: valid.
        for live in [gtid("1-7"), row.clone()] {
            check(&f, vec![obs(&f.full, live).await]).await.unwrap();
        }
        // Earlier, or incomparable (another server's set, another mode):
        // never a post-row capture.
        for live in [
            gtid("1-3"),
            WmPos::MysqlGtid {
                gtid_set: "4e11fa47-71ca-11e1-9e33-c80aa9429562:1-9".into(),
            },
            WmPos::MysqlBinlog {
                file_base: "mysql-bin".into(),
                file_index: 9,
                pos: 4,
            },
        ] {
            let err =
                check(&f, vec![obs(&f.full, live).await]).await.unwrap_err();
            assert!(err.contains("not at or after the row"), "{err}");
        }
        // A MINIMAL signature never stands as FULL evidence.
        let err = check(&f, vec![obs(&f.minimal, gtid("1-7")).await])
            .await
            .unwrap_err();
        assert!(err.contains("without FULL metadata"), "{err}");
    }

    /// A FULL row identity always names a rows event (ordinal from 1), in
    /// GTID and in file/position mode; ordinal 0 is a statement identity.
    #[tokio::test]
    async fn full_row_identities_need_a_rows_event_ordinal() {
        let f = fixture().await;
        let binlog = |pos: u64| WmPos::MysqlBinlog {
            file_base: "mysql-bin".into(),
            file_index: 1,
            pos,
        };
        let file_event = |ordinal: u32| EventIdentity::FilePos {
            file: "mysql-bin.000001".into(),
            end_pos: 150,
            ordinal,
        };
        let cases = [
            (gtid("1-5"), gtid("1-7"), row_event(0), row_event(1)),
            (binlog(100), binlog(200), file_event(0), file_event(1)),
        ];
        for (row, live, zero, one) in cases {
            let ok = full_observation(
                &f,
                f.full.clone(),
                row.clone(),
                live.clone(),
                one,
                None,
            )
            .await;
            check(&f, vec![ok]).await.unwrap();
            let forged =
                full_observation(&f, f.full.clone(), row, live, zero, None)
                    .await;
            let err = check(&f, vec![forged]).await.unwrap_err();
            assert!(err.contains("no rows-event ordinal"), "{err}");
        }
    }

    /// A FULL observation is admitted only when, added to the timeline, it
    /// is the decisive record proving its version at the row.
    #[tokio::test]
    async fn a_full_observation_is_admitted_only_if_it_decides_the_row() {
        let f = fixture().await;
        let row = gtid("1-5");
        let at = |id: &str, position: WmPos, kind: Kind| Stored {
            capture_id: id.into(),
            record: Record::new(position, kind),
        };
        let ddl = |id: &str| at(id, row.clone(), Kind::Ddl);
        let barrier = at(
            "b1",
            row.clone(),
            Kind::Barrier {
                scope: mysql_activation::BarrierScope::Lineage,
            },
        );
        let proposal = |binds: Option<&'static str>| {
            full_observation(
                &f,
                f.full.clone(),
                row.clone(),
                gtid("1-7"),
                row_event(1),
                binds,
            )
        };
        let admit = |records: Vec<Stored>, barriers: Vec<Stored>, p: Stored| {
            let current = Timeline {
                table_head: 0,
                ..Timeline::of(records, barriers)
            };
            let (f, row) = (&f, row.clone());
            async move { admit_decisive(&f.reg, &f.key, &current, p, &row).await }
        };

        // It binds the one ddl, or the one barrier, at the row: admitted.
        admit(vec![ddl("d1")], vec![], proposal(Some("d1")).await)
            .await
            .unwrap();
        admit(vec![], vec![barrier.clone()], proposal(Some("b1")).await)
            .await
            .unwrap();

        let rejected = [
            // A ddl plus another distinct ddl, or a barrier, at the row.
            (
                vec![ddl("d1"), ddl("d2")],
                vec![],
                proposal(Some("d1")).await,
            ),
            (
                vec![ddl("d1")],
                vec![barrier.clone()],
                proposal(Some("d1")).await,
            ),
            // An `unknown` at the row.
            (
                vec![at("u1", row.clone(), Kind::Unknown)],
                vec![],
                proposal(None).await,
            ),
            // An earlier record incomparable with the row.
            (
                vec![at(
                    "x1",
                    WmPos::MysqlGtid {
                        gtid_set: "4e11fa47-71ca-11e1-9e33-c80aa9429562:1-2"
                            .into(),
                    },
                    Kind::Ddl,
                )],
                vec![],
                proposal(None).await,
            ),
            // Still ambiguous once inserted: it closes nothing at the row.
            (vec![ddl("d1")], vec![], proposal(None).await),
        ];
        for (records, barriers, p) in rejected {
            let err = admit(records, barriers, p).await.unwrap_err();
            assert!(err.contains("would not resolve selection"), "{err}");
        }

        // The version is proven, but by another record: a later valid
        // observation decides a later row.
        let later = full_observation(
            &f,
            f.full.clone(),
            gtid("1-6"),
            gtid("1-7"),
            row_event(1),
            None,
        )
        .await;
        let current = Timeline {
            table_head: 0,
            ..Timeline::of(vec![later], Vec::new())
        };
        let err = admit_decisive(
            &f.reg,
            &f.key,
            &current,
            proposal(None).await,
            &gtid("1-6"),
        )
        .await
        .unwrap_err();
        assert!(err.contains("would not resolve selection"), "{err}");

        // It would decide the row, but its own evidence is invalid (a live
        // capture before the row): the augmented timeline does not validate.
        let invalid = full_observation(
            &f,
            f.full.clone(),
            row.clone(),
            gtid("1-3"),
            row_event(1),
            None,
        )
        .await;
        let err = admit(vec![], vec![], invalid).await.unwrap_err();
        assert!(err.contains("not at or after the row"), "{err}");
    }

    /// A table's proven flag lives on its cached timeline, so it goes when
    /// the timeline changes (or a barrier changes every timeline), and costs
    /// no cache entry of its own.
    #[test]
    fn the_proven_marker_goes_with_its_timeline() {
        let key = SchemaKey::new(
            "t",
            "s",
            "0123456789abcdef0123456789abcdef",
            "d",
            "x",
        );
        let other = SchemaKey::new(
            "t",
            "s",
            "0123456789abcdef0123456789abcdef",
            "d",
            "y",
        );
        let mut c = Caches::default();
        // A flag on the cached timeline: none cached, nothing to mark.
        c.mark_proven(&key);
        assert!(!c.is_proven(&key));
        c.seed_for_test(&key);
        c.seed_for_test(&other);
        assert!(!c.is_proven(&key));
        c.mark_proven(&key);
        c.mark_proven(&other);
        assert!(c.is_proven(&key));
        c.invalidate(&key);
        assert!(!c.is_proven(&key));
        assert!(c.is_proven(&other));
        c.invalidate_all();
        assert!(!c.is_proven(&other));
    }

    mod admitted {
        //! The admitted baseline's timeline installed in the cache equals what
        //! a fresh read returns (the path that always re-read), in every case.
        use super::*;
        use crate::mysql::mysql_activation::{
            ACTIVATION_NS, Record, append, baseline_capture_id, read_barriers,
            read_table_head,
        };

        async fn fresh(f: &Fixture) -> Timeline {
            let (records, table_head) =
                read_table_head(&f.backend, &f.reg, &f.key).await.unwrap();
            let barriers = read_barriers(&f.backend, &f.key).await.unwrap();
            Timeline {
                table_head,
                ..Timeline::of(records, barriers)
            }
        }

        /// What the next row sees: the cached timeline, or a fresh read.
        async fn seen(f: &Fixture, caches: &mut Caches) -> Timeline {
            match caches.timeline(&f.key.backend_key()) {
                Some(t) => Timeline::clone(&t),
                None => fresh(f).await,
            }
        }

        fn same(a: &Timeline, b: &Timeline) -> bool {
            a.records == b.records
                && a.barriers().eq(b.barriers())
                && a.table_head == b.table_head
        }

        /// The old path's outcome and the cache agree, at `row`.
        async fn equivalent(f: &Fixture, caches: &mut Caches, row: &WmPos) {
            let (cached, read) = (seen(f, caches).await, fresh(f).await);
            assert!(same(&cached, &read), "{cached:?} != {read:?}");
            assert_eq!(
                select_decisive(&cached.all(), row),
                select_decisive(&read.all(), row)
            );
        }

        fn baseline(f: &Fixture, at: WmPos, digest: &str) -> Stored {
            let record = Record::new(
                at,
                Kind::Baseline {
                    version: 1,
                    schema_hash: "h1".into(),
                    to: gtid("1-20"),
                    scan_digest: digest.into(),
                    binds: None,
                    classifier_version: CLASSIFIER_VERSION.into(),
                },
            );
            let stream = table_stream(&f.key);
            Stored {
                capture_id: baseline_capture_id(
                    &f.key.lineage_hash,
                    &stream,
                    &record,
                )
                .unwrap(),
                record,
            }
        }

        async fn store(f: &Fixture, s: &Stored) {
            append(
                &f.backend,
                &f.reg,
                &f.key,
                ACTIVATION_NS,
                &table_stream(&f.key),
                &s.capture_id,
                &s.record,
            )
            .await
            .unwrap();
        }

        /// Admit `p` at `row` over the cached timeline and append it, as the
        /// lazy baseline does.
        async fn admit_and_append(
            f: &Fixture,
            caches: &mut Caches,
            p: Stored,
            row: &WmPos,
        ) -> Result<(), String> {
            let current = seen(f, caches).await;
            caches.put_timeline(f.key.backend_key(), Arc::new(current.clone()));
            let augmented = admit_decisive_timeline(
                &f.reg,
                &f.key,
                &current,
                p.clone(),
                row,
            )
            .await?;
            append_admitted(
                &f.backend,
                &f.reg,
                caches,
                &f.key,
                &p.capture_id,
                &p.record,
                augmented,
            )
            .await
            .map_err(|e| format!("{e:#}"))
        }

        #[tokio::test]
        async fn accepted_baselines_install_the_fresh_timeline() {
            let f = fixture().await;
            let mut caches = Caches::default();
            admit_and_append(
                &f,
                &mut caches,
                baseline(&f, gtid("1-5"), "a"),
                &gtid("1-5"),
            )
            .await
            .unwrap();
            assert!(
                caches.timeline(&f.key.backend_key()).is_some(),
                "installed"
            );
            equivalent(&f, &mut caches, &gtid("1-5")).await;
            // A later baseline, over a timeline with a record already.
            admit_and_append(
                &f,
                &mut caches,
                baseline(&f, gtid("1-9"), "b"),
                &gtid("1-9"),
            )
            .await
            .unwrap();
            assert!(caches.timeline(&f.key.backend_key()).is_some());
            equivalent(&f, &mut caches, &gtid("1-9")).await;
            assert_eq!(fresh(&f).await.records.len(), 2);
        }

        #[tokio::test]
        async fn a_rejected_baseline_changes_nothing() {
            let f = fixture().await;
            let mut caches = Caches::default();
            store(
                &f,
                &Stored {
                    capture_id: "d1".into(),
                    record: Record::new(gtid("1-5"), Kind::Ddl),
                },
            )
            .await;
            let before = fresh(&f).await;
            // Ambiguous with the ddl at the row: not decisive, not appended.
            let err = admit_and_append(
                &f,
                &mut caches,
                baseline(&f, gtid("1-5"), "a"),
                &gtid("1-5"),
            )
            .await
            .unwrap_err();
            assert!(err.contains("would not resolve"), "{err}");
            assert!(same(&fresh(&f).await, &before));
            equivalent(&f, &mut caches, &gtid("1-5")).await;
        }

        #[tokio::test]
        async fn a_baseline_already_present_discards_the_cache() {
            let f = fixture().await;
            let mut caches = Caches::default();
            let b = baseline(&f, gtid("1-5"), "a");
            store(&f, &b).await;
            // The cache does not know it yet (a stale entry): appending it
            // again stores nothing, and the cache is discarded.
            caches.put_timeline(
                f.key.backend_key(),
                Arc::new(Timeline {
                    table_head: 0,
                    ..Timeline::of(Vec::new(), Vec::new())
                }),
            );
            let current = Timeline::clone(
                &caches.timeline(&f.key.backend_key()).unwrap(),
            );
            let augmented = admit_decisive_timeline(
                &f.reg,
                &f.key,
                &current,
                b.clone(),
                &gtid("1-5"),
            )
            .await
            .unwrap();
            append_admitted(
                &f.backend,
                &f.reg,
                &mut caches,
                &f.key,
                &b.capture_id,
                &b.record,
                augmented,
            )
            .await
            .unwrap();
            assert!(
                caches.timeline(&f.key.backend_key()).is_none(),
                "discarded"
            );
            equivalent(&f, &mut caches, &gtid("1-5")).await;
            assert_eq!(fresh(&f).await.records.len(), 1);
        }

        #[tokio::test]
        async fn a_conflicting_baseline_fails_and_discards_the_cache() {
            let f = fixture().await;
            let mut caches = Caches::default();
            let b = baseline(&f, gtid("1-5"), "a");
            // Other bytes under the same capture identity.
            f.backend
                .log_append_if_absent(
                    ACTIVATION_NS,
                    &table_stream(&f.key),
                    &b.capture_id,
                    b"other",
                )
                .await
                .unwrap();
            caches.put_timeline(
                f.key.backend_key(),
                Arc::new(Timeline {
                    table_head: 0,
                    ..Timeline::of(Vec::new(), Vec::new())
                }),
            );
            let current = Timeline::clone(
                &caches.timeline(&f.key.backend_key()).unwrap(),
            );
            let augmented = admit_decisive_timeline(
                &f.reg,
                &f.key,
                &current,
                b.clone(),
                &gtid("1-5"),
            )
            .await
            .unwrap();
            let err = append_admitted(
                &f.backend,
                &f.reg,
                &mut caches,
                &f.key,
                &b.capture_id,
                &b.record,
                augmented,
            )
            .await
            .unwrap_err();
            assert!(format!("{err:#}").contains("already exists"), "{err:#}");
            assert!(
                caches.timeline(&f.key.backend_key()).is_none(),
                "discarded"
            );
        }

        #[tokio::test]
        async fn a_stream_that_drifted_under_the_cache_is_read_again() {
            let f = fixture().await;
            let mut caches = Caches::default();
            let current = fresh(&f).await;
            caches.put_timeline(f.key.backend_key(), Arc::new(current.clone()));
            // Stored after the cache was read: the cache does not have it.
            store(
                &f,
                &Stored {
                    capture_id: "u1".into(),
                    record: Record::new(gtid("1-3"), Kind::Unknown),
                },
            )
            .await;
            let b = baseline(&f, gtid("1-5"), "a");
            let augmented = admit_decisive_timeline(
                &f.reg,
                &f.key,
                &current,
                b.clone(),
                &gtid("1-5"),
            )
            .await
            .unwrap();
            append_admitted(
                &f.backend,
                &f.reg,
                &mut caches,
                &f.key,
                &b.capture_id,
                &b.record,
                augmented,
            )
            .await
            .unwrap();
            assert!(
                caches.timeline(&f.key.backend_key()).is_none(),
                "not installed"
            );
            equivalent(&f, &mut caches, &gtid("1-5")).await;
            assert_eq!(fresh(&f).await.records.len(), 2);
        }
    }

    /// The per-timeline hard limit (design section 6): a timeline over
    /// 100,000 records (the table's stream with its barrier streams) fails
    /// closed as `mysql_timeline_too_large`, never loaded whole.
    mod timeline_limit {
        use super::*;
        use crate::gtid_set::GtidSet;
        use crate::mysql::mysql_activation::{BARRIER_NS, BarrierScope};
        use crate::mysql::mysql_schema_loader::MySqlSchemaLoader;
        use storage::ArcStorageBackend;
        use tokio::sync::mpsc;

        const U: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562";

        async fn ctx_on(backend: ArcStorageBackend) -> RunCtx {
            let (tx, _rx) = mpsc::channel(8);
            let mut ctx = crate::mysql::mysql_event::tests::make_runctx(tx);
            ctx.registry_backend = backend.clone();
            let registry = storage::DurableSchemaRegistry::new(backend.clone())
                .await
                .unwrap();
            ctx.schema = MySqlSchemaLoader::new(
                "mysql://fake",
                registry,
                &ctx.tenant,
                ctx.registry_scope.clone(),
            );
            crate::registry_scope::establish_scope(
                &backend,
                &ctx.registry_scope,
                &ctx.tenant,
                &ctx.source_id,
                storage::adapters::LineageDescriptor::mysql(U).unwrap(),
            )
            .await
            .unwrap();
            ctx.last_gtid = Some(GtidSet::parse(&format!("{U}:1-50")).unwrap());
            ctx
        }

        /// `n` raw entries in `ns`/`stream` of `size` bytes each: the limit
        /// stops the read before anything is decoded.
        async fn raw(
            ctx: &RunCtx,
            ns: &str,
            stream: &str,
            n: u64,
            size: usize,
        ) {
            for _ in 0..n {
                ctx.registry_backend
                    .log_append(ns, stream, &vec![b'x'; size])
                    .await
                    .unwrap();
            }
        }

        fn too_large(err: &SourceError) -> bool {
            err.draft().map(|d| d.reason_code.as_str())
                == Some("mysql_timeline_too_large")
        }

        /// Over the record limit by its lineage barriers alone: every
        /// table of the lineage fails closed, never loaded whole.
        #[tokio::test]
        async fn a_timeline_over_its_record_limit_fails_closed() {
            let backend: ArcStorageBackend =
                Arc::new(storage::MemoryStorageBackend::new());
            let mut ctx = ctx_on(backend).await;
            let key = ctx.registry_scope.current().unwrap().key("d", "x");
            let lineage = crate::mysql::mysql_activation::barrier_stream(
                &key,
                &BarrierScope::Lineage,
            );
            raw(&ctx, BARRIER_NS, &lineage, 100_001, 8).await;
            let Err(err) = timeline(&mut ctx, &key).await else {
                panic!("over the limit, yet loaded whole");
            };
            assert!(too_large(&err), "{err}");
            assert!(ctx.selection.timeline(&key.backend_key()).is_none());
        }

        /// Over the byte limit by its own stream: the table fails closed;
        /// another table of the lineage is unaffected.
        #[tokio::test]
        async fn a_timeline_over_its_byte_limit_fails_closed() {
            let backend: ArcStorageBackend =
                Arc::new(storage::MemoryStorageBackend::new());
            let mut ctx = ctx_on(backend).await;
            let scope = ctx.registry_scope.current().unwrap();
            let (big, small) = (scope.key("d", "big"), scope.key("d", "small"));
            raw(&ctx, ACTIVATION_NS, &table_stream(&big), 65, 1 << 20).await;
            let Err(err) = timeline(&mut ctx, &big).await else {
                panic!("over the limit, yet loaded whole");
            };
            assert!(too_large(&err), "{err}");
            let t = timeline(&mut ctx, &small).await.unwrap();
            assert!(t.records.is_empty());
        }

        /// `n` valid records of `kind` in `ns`/`stream`, written raw.
        async fn records(
            ctx: &RunCtx,
            ns: &str,
            stream: &str,
            n: u64,
            kind: Kind,
        ) {
            for i in 1..=n {
                let r = Record::new(
                    WmPos::MysqlGtid {
                        gtid_set: format!("{U}:1-{i}"),
                    },
                    kind.clone(),
                );
                ctx.registry_backend
                    .log_append_if_absent(
                        ns,
                        stream,
                        &format!("r{i}"),
                        &crate::mysql::mysql_activation::encode_record(&r)
                            .unwrap(),
                    )
                    .await
                    .unwrap();
            }
        }

        /// Over the limit only with the barrier streams the cache already
        /// holds: the table's own read fits, the timeline does not; it
        /// fails closed.
        #[tokio::test]
        async fn cached_barrier_streams_count_toward_the_limit() {
            let backend: ArcStorageBackend =
                Arc::new(storage::MemoryStorageBackend::new());
            let mut ctx = ctx_on(backend).await;
            let scope = ctx.registry_scope.current().unwrap();
            let (first, big) = (scope.key("d", "first"), scope.key("d", "big"));
            let lineage = crate::mysql::mysql_activation::barrier_stream(
                &first,
                &BarrierScope::Lineage,
            );
            records(
                &ctx,
                BARRIER_NS,
                &lineage,
                60_000,
                Kind::Barrier {
                    scope: BarrierScope::Lineage,
                },
            )
            .await;
            // The lineage stream is cached with the first table.
            timeline(&mut ctx, &first).await.unwrap();
            records(
                &ctx,
                ACTIVATION_NS,
                &table_stream(&big),
                50_000,
                Kind::Unknown,
            )
            .await;
            let Err(err) = timeline(&mut ctx, &big).await else {
                panic!("over the limit with its cached barriers, yet loaded");
            };
            assert!(too_large(&err), "{err}");
        }

        /// The cache's estimated total never exceeds its byte bound, with
        /// timelines just under the per-timeline limit sharing one barrier
        /// stream (held, and counted, once).
        #[test]
        fn a_full_cache_stays_within_its_bytes() {
            let shared = Arc::new(BarrierStream {
                stream: "lineage/".into(),
                records: Vec::new(),
                head: 1,
                bytes: 10 << 20,
            });
            let mut caches = Caches::default();
            for i in 0..20 {
                caches.put_timeline(
                    format!("lineage/d/t{i}"),
                    Arc::new(Timeline {
                        records: Vec::new(),
                        table_head: 0,
                        streams: vec![shared.clone()],
                        bytes: super::super::super::mysql_history::TIMELINE_MAX_BYTES
                            - (10 << 20),
                    }),
                );
                assert!(
                    caches.bytes() <= SELECTION_CACHE_BYTES,
                    "{}",
                    caches.bytes()
                );
            }
            // 4 timelines of 54 MiB (and their entries) and the 10 MiB
            // stream once: about 226 MiB, a fifth would not fit.
            assert_eq!(caches.timelines(), 4);
            let held = caches.entries_bytes_for_test();
            assert!(held > 4 * (54 << 20) + (10 << 20), "{held}");
            assert_eq!(caches.barrier_streams().len(), 1);
            caches.audit();
            // The last timeline holding the stream takes it with it; the
            // map's buckets stay counted until a full clear.
            caches.barrier_appended("lineage/");
            assert_eq!(
                (caches.timelines(), caches.entries_bytes_for_test()),
                (0, 0)
            );
            caches.audit();
            caches.invalidate_all();
            assert_eq!(caches.bytes(), 0);
        }

        /// A barrier invalidates exactly what it applies to: a database's,
        /// that database's tables; a lineage's, every table; nothing else.
        #[tokio::test]
        async fn a_barrier_invalidates_what_it_applies_to() {
            let backend: ArcStorageBackend =
                Arc::new(storage::MemoryStorageBackend::new());
            let mut ctx = ctx_on(backend).await;
            let scope = ctx.registry_scope.current().unwrap();
            let keys = [
                scope.key("a", "x"),
                scope.key("a", "y"),
                scope.key("b", "z"),
            ];
            async fn load(ctx: &mut RunCtx, keys: &[SchemaKey]) {
                for k in keys {
                    timeline(ctx, k).await.unwrap();
                }
            }
            let cached = |ctx: &mut RunCtx| {
                keys.iter()
                    .map(|k| ctx.selection.timeline(&k.backend_key()).is_some())
                    .collect::<Vec<_>>()
            };
            load(&mut ctx, &keys).await;
            let at = |i: u64| WmPos::MysqlGtid {
                gtid_set: format!("{U}:1-{i}"),
            };
            let db_a = BarrierScope::Database { db: "a".into() };
            append_barrier(
                &ctx.registry_backend.clone(),
                &ctx.schema.registry_arc(),
                &mut ctx.selection,
                &keys[0],
                db_a,
                &EventIdentity::Gtid {
                    gtid: format!("{U}:51"),
                    ordinal: 0,
                },
                at(51),
            )
            .await
            .unwrap();
            assert_eq!(cached(&mut ctx), [false, false, true]);
            load(&mut ctx, &keys).await;
            // The reloaded timelines hold the new barrier.
            let x = timeline(&mut ctx, &keys[0]).await.unwrap();
            assert_eq!(x.barriers().count(), 1);
            append_barrier(
                &ctx.registry_backend.clone(),
                &ctx.schema.registry_arc(),
                &mut ctx.selection,
                &keys[0],
                BarrierScope::Lineage,
                &EventIdentity::Gtid {
                    gtid: format!("{U}:52"),
                    ordinal: 0,
                },
                at(52),
            )
            .await
            .unwrap();
            assert_eq!(cached(&mut ctx), [false, false, false]);
            // Only the maps' buckets stay counted, until a full clear.
            assert_eq!(ctx.selection.entries_bytes_for_test(), 0);
            load(&mut ctx, &keys).await;
            let z = timeline(&mut ctx, &keys[2]).await.unwrap();
            assert_eq!(z.barriers().count(), 1, "the lineage barrier only");
        }

        /// A timeline read reads ahead, in one batch, the version its last
        /// proof names; a measurement build times that read
        /// (`version_prefetch`, inside the row's `history_load`) and counts
        /// it as one `versions` read.
        #[cfg(feature = "perf-phases")]
        #[test]
        fn perf_the_version_read_ahead_is_timed_and_counted() {
            use crate::mysql::mysql_activation::{
                ACTIVATION_NS, append, baseline_capture_id,
            };
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let seen =
                crate::mysql::mysql_selection::perf_tests::perf_metrics(|| {
                    rt.block_on(async {
                        let backend: ArcStorageBackend =
                            Arc::new(storage::MemoryStorageBackend::new());
                        let mut ctx = ctx_on(backend).await;
                        let key =
                            ctx.registry_scope.current().unwrap().key("d", "x");
                        let registry = ctx.schema.registry_arc();
                        let schema =
                            MySqlTableSchema::new(vec![MySqlColumn::new(
                                "a", "int", "int", true, 1,
                            )]);
                        let version = registry
                            .register_with_checkpoint(
                                &key,
                                "h1",
                                &serde_json::to_value(&schema).unwrap(),
                                None,
                            )
                            .await
                            .unwrap();
                        let record = Record::new(
                            WmPos::MysqlGtid {
                                gtid_set: format!("{U}:1-3"),
                            },
                            Kind::Baseline {
                                version,
                                schema_hash: "h1".into(),
                                to: WmPos::MysqlGtid {
                                    gtid_set: format!("{U}:1-4"),
                                },
                                scan_digest: "s".into(),
                                binds: None,
                                classifier_version: CLASSIFIER_VERSION.into(),
                            },
                        );
                        let stream = table_stream(&key);
                        append(
                            &ctx.registry_backend,
                            &registry,
                            &key,
                            ACTIVATION_NS,
                            &stream,
                            &baseline_capture_id(
                                &key.lineage_hash,
                                &stream,
                                &record,
                            )
                            .unwrap(),
                            &record,
                        )
                        .await
                        .unwrap();
                        timeline(&mut ctx, &key).await.unwrap();
                        assert!(
                            ctx.selection
                                .version(&key.backend_key(), version)
                                .is_some(),
                            "the version is read ahead"
                        );
                    })
                });
            let of = |prefix: &str| {
                seen.iter()
                    .filter(|l| l.starts_with(prefix))
                    .cloned()
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                of("deltaforge_perf_phase_latency_seconds"),
                [
                    "deltaforge_perf_phase_latency_seconds phase=history_load 1",
                    "deltaforge_perf_phase_latency_seconds phase=version_prefetch 1",
                ]
            );
            assert_eq!(
                of("deltaforge_perf_selection_store_reads_total"),
                [
                    "deltaforge_perf_selection_store_reads_total read=history 1",
                    "deltaforge_perf_selection_store_reads_total read=versions 1",
                ]
            );
        }

        /// A record naming a version the registry does not have fails its
        /// table exactly as the table's own read does, and caches nothing;
        /// the batch's other tables are unaffected.
        #[tokio::test]
        async fn a_record_naming_a_missing_version_fails_its_table() {
            let backend: ArcStorageBackend =
                Arc::new(storage::MemoryStorageBackend::new());
            let mut ctx = ctx_on(backend).await;
            let scope = ctx.registry_scope.current().unwrap();
            let (bad, good) = (scope.key("d", "bad"), scope.key("d", "good"));
            let record = Record::new(
                WmPos::MysqlGtid {
                    gtid_set: format!("{U}:1-3"),
                },
                Kind::Baseline {
                    version: 7,
                    schema_hash: "h".into(),
                    to: WmPos::MysqlGtid {
                        gtid_set: format!("{U}:1-4"),
                    },
                    scan_digest: "s".into(),
                    binds: None,
                    classifier_version: CLASSIFIER_VERSION.into(),
                },
            );
            ctx.registry_backend
                .log_append_if_absent(
                    ACTIVATION_NS,
                    &table_stream(&bad),
                    "baseline-7",
                    &crate::mysql::mysql_activation::encode_record(&record)
                        .unwrap(),
                )
                .await
                .unwrap();
            let own = crate::mysql::mysql_activation::read_table_head(
                &ctx.registry_backend,
                ctx.schema.registry(),
                &bad,
            )
            .await;
            let Err(own) = own else {
                panic!("the per-table read accepts a missing version");
            };
            let mut have = std::collections::HashMap::new();
            let read = crate::mysql::mysql_history::read_each(
                &ctx.registry_backend,
                ctx.schema.registry(),
                &[bad.clone(), good.clone()],
                &mut have,
                |_| true,
            )
            .await
            .unwrap();
            let crate::mysql::mysql_history::Loaded::Invalid(e) = &read[0]
            else {
                panic!("{:?}", read[0]);
            };
            assert_eq!(e.to_string(), own.to_string());
            assert!(matches!(
                read[1],
                crate::mysql::mysql_history::Loaded::Ok { .. }
            ));
            let Err(err) = timeline(&mut ctx, &bad).await else {
                panic!("a missing version is accepted");
            };
            assert!(err.to_string().contains("version 7"), "{err}");
            assert!(ctx.selection.timeline(&bad.backend_key()).is_none());
        }

        /// The only barrier writer is `append_barrier`: a new one appending
        /// without invalidating the cache fails this test.
        #[test]
        fn every_barrier_append_goes_through_append_barrier() {
            let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/src/mysql");
            let mut calls = Vec::new();
            for e in std::fs::read_dir(dir).unwrap() {
                let path = e.unwrap().path();
                if path.extension().is_none_or(|x| x != "rs") {
                    continue;
                }
                let text = std::fs::read_to_string(&path).unwrap();
                // Production code only: the test modules may write raw.
                let code =
                    text.split("\n#[cfg(test)]\nmod tests").next().unwrap();
                for (i, line) in code.lines().enumerate() {
                    if line.contains("record_barrier(")
                        && !line.trim_start().starts_with("//")
                        && !line.contains("fn record_barrier(")
                    {
                        calls.push(format!(
                            "{}:{}",
                            path.file_name().unwrap().to_string_lossy(),
                            i + 1
                        ));
                    }
                }
            }
            assert_eq!(calls.len(), 1, "{calls:?}");
            assert!(calls[0].starts_with("mysql_selection.rs"), "{calls:?}");
        }

        /// `key` with a registered version of `columns` columns and a
        /// baseline naming it: its timeline's load reads that version
        /// ahead.
        async fn baselined(ctx: &RunCtx, key: &SchemaKey, columns: usize) {
            use crate::mysql::mysql_activation::{append, baseline_capture_id};
            let registry = ctx.schema.registry_arc();
            let schema = MySqlTableSchema::new(
                (0..columns)
                    .map(|i| {
                        MySqlColumn::new(
                            format!("column_{i}"),
                            "varchar(255)",
                            "varchar",
                            true,
                            i as u32 + 1,
                        )
                    })
                    .collect(),
            );
            let hash = format!("{:064x}", columns);
            let version = registry
                .register_with_checkpoint(
                    key,
                    &hash,
                    &serde_json::to_value(&schema).unwrap(),
                    None,
                )
                .await
                .unwrap();
            let record = Record::new(
                WmPos::MysqlGtid {
                    gtid_set: format!("{U}:1-3"),
                },
                Kind::Baseline {
                    version,
                    schema_hash: hash,
                    to: WmPos::MysqlGtid {
                        gtid_set: format!("{U}:1-4"),
                    },
                    scan_digest: "s".into(),
                    binds: None,
                    classifier_version: CLASSIFIER_VERSION.into(),
                },
            );
            let stream = table_stream(key);
            append(
                &ctx.registry_backend,
                &registry,
                key,
                ACTIVATION_NS,
                &stream,
                &baseline_capture_id(&key.lineage_hash, &stream, &record)
                    .unwrap(),
                &record,
            )
            .await
            .unwrap();
        }

        /// A load admits its tables in order while they fit, and never
        /// evicts what it admitted, read-ahead versions included: what it
        /// leaves cached is a prefix of its tables, and a version only
        /// with its timeline. Older entries make room first.
        #[tokio::test]
        async fn a_load_never_evicts_what_it_admitted() {
            let backend: ArcStorageBackend =
                Arc::new(storage::MemoryStorageBackend::new());
            let mut ctx = ctx_on(backend).await;
            let scope = ctx.registry_scope.current().unwrap();
            let keys: Vec<SchemaKey> = (0..12)
                .map(|i| scope.key("d", &format!("t{i:02}")))
                .collect();
            for k in &keys {
                baselined(&ctx, k, 40).await;
            }
            // One table's cost, timeline and version.
            load_timelines(&mut ctx, &keys[..1]).await;
            assert_eq!(ctx.selection.held_for(&keys[0]), 2);
            let one = ctx.selection.bytes();
            // Room for about six tables, half of it held by older ones.
            ctx.selection = Caches::default();
            ctx.selection.set_limit_for_test(6 * one);
            for i in 0..3 {
                ctx.selection
                    .seed_for_test(&scope.key("old", &format!("{i}")));
            }
            load_timelines(&mut ctx, &keys).await;
            let held: Vec<usize> =
                keys.iter().map(|k| ctx.selection.held_for(k)).collect();
            let admitted = held.iter().take_while(|h| **h > 0).count();
            assert!(admitted >= 1, "nothing admitted: {held:?}");
            assert!(
                held[admitted..].iter().all(|h| *h == 0),
                "admitted out of order or evicted by its own load: {held:?}"
            );
            let versioned = held.iter().take_while(|h| **h == 2).count();
            assert!(
                held[versioned..admitted].iter().all(|h| *h == 1),
                "a version without its timeline's predecessors: {held:?}"
            );
            assert!(ctx.selection.bytes() <= 6 * one);
            // The load took at most half: the older tables kept theirs.
            for i in 0..3 {
                let old = scope.key("old", &format!("{i}"));
                assert_eq!(ctx.selection.held_for(&old), 1, "{i} evicted");
            }
        }

        /// Nothing fits the cache: the requested table's timeline is still
        /// served (uncached), so its rows go on; each use reads it again.
        #[tokio::test]
        async fn a_table_that_cannot_be_cached_is_still_served() {
            let backend: ArcStorageBackend =
                Arc::new(storage::MemoryStorageBackend::new());
            let mut ctx = ctx_on(backend).await;
            let key = ctx.registry_scope.current().unwrap().key("d", "x");
            baselined(&ctx, &key, 8).await;
            ctx.selection.set_limit_for_test(1);
            for _ in 0..3 {
                let t = timeline(&mut ctx, &key).await.unwrap();
                assert_eq!(t.records.len(), 1);
                assert_eq!(ctx.selection.len(), 0, "nothing cached");
            }
        }

        /// `n` tables of `d`, each with `records_each` records.
        async fn tables(
            ctx: &RunCtx,
            prefix: &str,
            n: usize,
            records_each: u64,
        ) -> Vec<SchemaKey> {
            let scope = ctx.registry_scope.current().unwrap();
            let mut out = Vec::new();
            for i in 0..n {
                let k = scope.key("d", &format!("{prefix}{i:04}"));
                records(
                    ctx,
                    ACTIVATION_NS,
                    &table_stream(&k),
                    records_each,
                    Kind::Unknown,
                )
                .await;
                out.push(k);
            }
            out
        }

        /// The peak of this thread's allocations while `keys` load into a
        /// cache of `max_bytes`, above what was held before.
        async fn load_peak(
            ctx: &mut RunCtx,
            keys: &[SchemaKey],
            max_bytes: u64,
        ) -> u64 {
            ctx.selection = Caches::default();
            ctx.selection.set_limit_for_test(max_bytes);
            crate::test_alloc::reset_peak();
            load_timelines(ctx, keys).await;
            crate::test_alloc::peak()
        }

        /// A load holds at most one bounded statement's histories at a
        /// time: three statements' worth of tables peak about as high as
        /// one statement's, not three times as high.
        #[tokio::test(flavor = "current_thread")]
        async fn a_load_holds_one_statement_at_a_time() {
            let backend: ArcStorageBackend =
                Arc::new(storage::MemoryStorageBackend::new());
            let mut ctx = ctx_on(backend).await;
            let batch = crate::mysql::mysql_history::BATCH_MAX_TABLES;
            let one = tables(&ctx, "a", batch, 100).await;
            let three = tables(&ctx, "b", 3 * batch, 100).await;
            let max = 1 << 20;
            let p1 = load_peak(&mut ctx, &one, max).await;
            let p3 = load_peak(&mut ctx, &three, max).await;
            eprintln!(
                "load peak: {p1} bytes for {} tables, {p3} bytes for {}",
                one.len(),
                three.len()
            );
            assert!(p3 <= p1 + p1 / 2, "{p3} bytes > 1.5 x {p1}");
        }

        /// Once a read-ahead load admits nothing more, it reads no further
        /// statements: the rest is read when the stream reaches it.
        #[tokio::test]
        async fn a_read_ahead_stops_reading_once_nothing_fits() {
            use std::sync::atomic::Ordering;
            use storage::adapters::test_util::FaultBackend;
            let fault = Arc::new(FaultBackend::new());
            let backend: ArcStorageBackend = fault.clone();
            let mut ctx = ctx_on(backend).await;
            let batch = crate::mysql::mysql_history::BATCH_MAX_TABLES;
            let keys = tables(&ctx, "a", 3 * batch, 2).await;
            ctx.selection = Caches::default();
            ctx.selection.set_limit_for_test(16 << 10);
            let before = fault.batched_stream_reads.load(Ordering::SeqCst);
            load_timelines(&mut ctx, &keys).await;
            let reads =
                fault.batched_stream_reads.load(Ordering::SeqCst) - before;
            let cached = keys
                .iter()
                .filter(|k| ctx.selection.held_for(k) > 0)
                .count();
            assert!(cached > 0 && cached < batch, "{cached} cached");
            assert_eq!(reads, 1, "statements read for {} tables", keys.len());
        }

        /// A load that stops early counts only the tables it asked the
        /// store for (one statement's), not every table it was given; still
        /// one read per load.
        #[cfg(feature = "perf-phases")]
        #[test]
        fn perf_a_stopped_load_counts_the_tables_it_requested() {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let backend: ArcStorageBackend =
                Arc::new(storage::MemoryStorageBackend::new());
            let mut ctx = rt.block_on(ctx_on(backend));
            let batch = crate::mysql::mysql_history::BATCH_MAX_TABLES;
            let keys = rt.block_on(tables(&ctx, "a", 3 * batch, 2));
            ctx.selection = Caches::default();
            ctx.selection.set_limit_for_test(16 << 10);
            let seen =
                crate::mysql::mysql_selection::perf_tests::perf_metrics(|| {
                    rt.block_on(load_timelines(&mut ctx, &keys))
                });
            let read = |name: &str| {
                let prefix = format!(
                    "deltaforge_perf_selection_{name} read=history_prefetch "
                );
                seen.iter()
                    .find_map(|l| l.strip_prefix(&prefix))
                    .unwrap_or_else(|| panic!("no {name} in {seen:?}"))
                    .to_string()
            };
            assert_eq!(read("store_reads_total"), "1");
            assert_eq!(read("store_tables_total"), batch.to_string());
        }

        /// The peak of this thread's allocations while `key`'s oversized
        /// timeline is refused.
        async fn refused_peak(ctx: &mut RunCtx, key: &SchemaKey) -> u64 {
            crate::test_alloc::reset_peak();
            let Err(err) = timeline(ctx, key).await else {
                panic!("over the limit, yet loaded whole");
            };
            assert!(too_large(&err), "{err}");
            crate::test_alloc::peak()
        }

        /// An oversized history is never read whole: its read stops one
        /// record past the limit, so a history three times over the limit
        /// peaks about as high as one just over it.
        #[tokio::test(flavor = "current_thread")]
        async fn an_oversized_history_is_read_only_to_its_limit() {
            let backend: ArcStorageBackend =
                Arc::new(storage::MemoryStorageBackend::new());
            let mut ctx = ctx_on(backend).await;
            let scope = ctx.registry_scope.current().unwrap();
            let (near, far) = (scope.key("d", "near"), scope.key("d", "far"));
            let limit = crate::mysql::mysql_history::TIMELINE_MAX_RECORDS;
            raw(&ctx, ACTIVATION_NS, &table_stream(&near), limit + 1, 8).await;
            raw(&ctx, ACTIVATION_NS, &table_stream(&far), 3 * limit, 8).await;
            let near_peak = refused_peak(&mut ctx, &near).await;
            let far_peak = refused_peak(&mut ctx, &far).await;
            eprintln!(
                "oversized read peak: {near_peak} bytes just over the limit, \
                 {far_peak} bytes at three times it"
            );
            assert!(
                far_peak <= near_peak + near_peak / 4,
                "{far_peak} bytes > 1.25 x {near_peak}"
            );
        }
    }

    mod byte_budget {
        //! The cache is bounded by its estimated bytes alone: what it
        //! counts, how a batch is sized, and invalidation at scale.
        use super::*;
        use crate::mysql::mysql_activation::{BarrierScope, barrier_stream};
        use std::collections::HashMap;
        use std::time::{Duration, Instant};

        const L: &str = "0123456789abcdef0123456789abcdef";

        fn key(db: usize, t: usize) -> SchemaKey {
            SchemaKey::new("acme", "s", L, format!("db{db}"), format!("t{t}"))
        }

        fn lineage() -> String {
            barrier_stream(&key(0, 0), &BarrierScope::Lineage)
        }

        fn database(db: usize) -> String {
            barrier_stream(
                &key(db, 0),
                &BarrierScope::Database {
                    db: format!("db{db}"),
                },
            )
        }

        /// `key`'s timeline: `bytes` of records, and its lineage's and
        /// database's barrier streams, one read of each stream shared.
        fn timeline_of(
            key: &SchemaKey,
            bytes: u64,
            read: &mut HashMap<String, Arc<BarrierStream>>,
        ) -> Arc<Timeline> {
            let streams = crate::mysql::mysql_activation::barrier_streams(key)
                .into_iter()
                .map(|name| {
                    read.entry(name.clone())
                        .or_insert_with(|| {
                            Arc::new(BarrierStream {
                                stream: name,
                                records: Vec::new(),
                                head: 1,
                                bytes: 256,
                            })
                        })
                        .clone()
                })
                .collect();
            Arc::new(Timeline {
                records: Vec::new(),
                table_head: 1,
                streams,
                bytes,
            })
        }

        /// A table of `n` columns of mixed kinds (enums carry labels).
        fn schema(n: usize) -> MySqlTableSchema {
            let kinds = [
                ("bigint(20) unsigned", "bigint"),
                ("varchar(255)", "varchar"),
                ("enum('new','paid','shipped','returned')", "enum"),
                ("datetime(3)", "datetime"),
                ("decimal(12,2)", "decimal"),
            ];
            MySqlTableSchema::new(
                (0..n)
                    .map(|i| {
                        let (ct, dt) = kinds[i % kinds.len()];
                        MySqlColumn::new(
                            format!("column_{i}"),
                            ct,
                            dt,
                            i % 2 == 0,
                            i as u32 + 1,
                        )
                    })
                    .collect(),
            )
        }

        fn loaded(n: usize) -> Arc<LoadedSchema> {
            Arc::new(LoadedSchema::new(schema(n), 1, "h".into(), 1))
        }

        /// A registered version as the registry decodes it.
        fn version(v: i32, n: usize) -> schema_registry::SchemaVersion {
            schema_registry::SchemaVersion {
                version: v,
                hash: format!("{v:064x}"),
                schema_json: serde_json::from_slice(
                    &serde_json::to_vec(&schema(n)).unwrap(),
                )
                .unwrap(),
                registered_at: chrono::Utc::now(),
                sequence: v as u64,
                checkpoint: None,
            }
        }

        /// More tables than the old 4,096-table bound stay cached while
        /// their bytes fit: a table count no longer limits the cache.
        #[test]
        fn more_tables_than_the_old_bound_stay_cached_while_they_fit() {
            let mut c = Caches::default();
            let mut read = HashMap::new();
            for t in 0..10_000 {
                let k = key(t % 10, t);
                c.put_timeline(
                    k.backend_key(),
                    timeline_of(&k, 300, &mut read),
                );
            }
            assert_eq!(c.timelines(), 10_000);
            assert!(c.bytes() <= SELECTION_CACHE_BYTES);
            c.audit();
        }

        /// Under churn of every entry kind (timelines sharing barrier
        /// streams, selections, versions) the cache never holds more than
        /// its byte bound, and its running accounting always equals a
        /// recount of what it holds.
        #[test]
        fn the_byte_bound_holds_under_churn_of_every_entry_kind() {
            let max = 512 * 1024;
            let mut c = Caches::default();
            c.set_limit_for_test(max);
            let mut read = HashMap::new();
            let selection = loaded(12);
            for i in 0..5_000 {
                let k = key(i % 7, i % 900);
                let bk = k.backend_key();
                c.put_timeline(bk.clone(), timeline_of(&k, 300, &mut read));
                c.put_selection(
                    (bk.clone(), format!("d{i}"), "sig".into()),
                    selection.clone(),
                );
                c.put_version(bk, version(1 + (i % 3) as i32, 12));
                assert!(c.bytes() <= max, "{} > {max}", c.bytes());
                if i % 97 == 0 {
                    c.audit();
                }
            }
            c.audit();
        }

        /// The buckets the cache's maps keep after evictions and
        /// invalidations are counted until a full clear releases them.
        #[test]
        fn retained_map_capacity_is_counted_until_a_full_clear() {
            let mut c = Caches::default();
            let mut read = HashMap::new();
            for t in 0..20_000 {
                let k = key(t % 20, t);
                c.put_timeline(
                    k.backend_key(),
                    timeline_of(&k, 300, &mut read),
                );
            }
            for db in 0..19 {
                c.barrier_appended(&database(db));
            }
            assert_eq!(c.timelines(), 1_000);
            assert!(
                c.bytes() > c.entries_bytes_for_test(),
                "the emptied buckets are counted: {} <= {}",
                c.bytes(),
                c.entries_bytes_for_test()
            );
            c.audit();
            c.invalidate_all();
            assert_eq!(c.bytes(), 0);
            c.audit();
        }

        /// Tens of thousands of tables with a selection and a version
        /// each: a database barrier drops exactly that database's entries,
        /// a lineage barrier every table's, a full clear everything; the
        /// accounting and the shared barrier streams stay exact throughout.
        #[test]
        fn invalidation_at_scale_is_exact() {
            const DBS: usize = 40;
            const PER: usize = 1_000;
            // Room for all of them (at the default bound about 31,000
            // tables with a selection and a version each fit).
            let max = 1 << 30;
            let mut c = Caches::default();
            c.set_limit_for_test(max);
            let mut read = HashMap::new();
            let selection = loaded(4);
            let fill = |c: &mut Caches,
                        read: &mut HashMap<String, Arc<BarrierStream>>,
                        dbs: usize| {
                for db in 0..dbs {
                    for t in 0..PER {
                        let k = key(db, t);
                        let bk = k.backend_key();
                        c.put_timeline(bk.clone(), timeline_of(&k, 300, read));
                        c.put_selection(
                            (bk.clone(), "d".into(), "sig".into()),
                            selection.clone(),
                        );
                        c.put_version(bk, version(1, 4));
                    }
                }
            };
            let t0 = Instant::now();
            fill(&mut c, &mut read, DBS);
            let filled = t0.elapsed();
            assert_eq!(c.timelines(), DBS * PER);
            assert_eq!(c.len(), 3 * DBS * PER);
            assert!(c.bytes() <= max);
            c.audit();

            let t0 = Instant::now();
            c.barrier_appended(&database(7));
            let one_db = t0.elapsed();
            assert_eq!(c.timelines(), (DBS - 1) * PER);
            assert_eq!(c.held_for(&key(7, 3)), 0);
            assert_eq!(c.held_for(&key(8, 3)), 3);
            assert!(!c.barrier_streams().contains_key(&database(7)));
            assert!(c.barrier_streams().contains_key(&database(8)));
            assert!(c.barrier_streams().contains_key(&lineage()));
            c.audit();

            let t0 = Instant::now();
            c.barrier_appended(&lineage());
            let all = t0.elapsed();
            assert_eq!((c.timelines(), c.len()), (0, 0));
            assert!(c.barrier_streams().is_empty());
            c.audit();

            read.clear();
            fill(&mut c, &mut read, 10);
            let t0 = Instant::now();
            c.invalidate_all();
            let cleared = t0.elapsed();
            assert_eq!((c.timelines(), c.len(), c.bytes()), (0, 0, 0));
            c.audit();
            eprintln!(
                "{} entries: filled in {filled:?}; a database barrier \
                 {one_db:?}, a lineage barrier {all:?}, a full clear of \
                 {} entries {cleared:?}",
                3 * DBS * PER,
                3 * 10 * PER
            );
            // A linear pass: far below this even unoptimized.
            for d in [one_db, all, cleared] {
                assert!(d < Duration::from_secs(30), "{d:?}");
            }
        }

        /// What invalidation costs at the default bound filled with the
        /// most entries it can hold: the smallest real tables (a one-record
        /// timeline, a one-column selection and version) over 100
        /// databases. Times a database barrier, a lineage barrier and a
        /// full clear, each on a full cache, five times. A measurement for
        /// a release build (`docs/design/selection-cache-diagnosis.md`,
        /// section 6): `cargo test --release -p sources --lib --
        /// --ignored --nocapture invalidation_cost_at_the_default_bound`.
        #[test]
        #[ignore = "timing: run in a release build"]
        fn invalidation_cost_at_the_default_bound() {
            const DBS: usize = 100;
            let selection = loaded(1);
            let fill = |c: &mut Caches, tables: usize| {
                let mut read = HashMap::new();
                for t in 0..tables {
                    let k = key(t % DBS, t / DBS);
                    let bk = k.backend_key();
                    c.put_timeline(bk.clone(), timeline_of(&k, 64, &mut read));
                    c.put_selection(
                        (bk.clone(), "d".into(), "sig".into()),
                        selection.clone(),
                    );
                    c.put_version(bk, version(1, 1));
                }
            };
            let full = || {
                let mut c = Caches::default();
                assert_eq!(c.max_bytes, SELECTION_CACHE_BYTES);
                fill(&mut c, 1);
                let per = c.entries_bytes_for_test().max(1);
                fill(&mut c, (SELECTION_CACHE_BYTES / per) as usize * 11 / 10);
                c
            };
            let mut times: [Vec<Duration>; 3] = Default::default();
            let mut held = 0;
            for _ in 0..5 {
                let mut c = full();
                held = c.len();
                let t0 = Instant::now();
                c.barrier_appended(&database(7));
                times[0].push(t0.elapsed());
                assert_eq!(c.held_for(&key(7, 3)), 0);

                let mut c = full();
                let t0 = Instant::now();
                c.barrier_appended(&lineage());
                times[1].push(t0.elapsed());
                assert_eq!(c.len(), 0);

                let mut c = full();
                let t0 = Instant::now();
                c.invalidate_all();
                times[2].push(t0.elapsed());
                assert_eq!(c.bytes(), 0);
            }
            let names = ["database barrier", "lineage barrier", "full clear"];
            for (name, t) in names.iter().zip(&mut times) {
                t.sort();
                eprintln!(
                    "{held} entries at {SELECTION_CACHE_BYTES} bytes: {name} \
                     median {:?}, max {:?}",
                    t[t.len() / 2],
                    t[t.len() - 1]
                );
            }
        }

        /// While a load admits, what it admitted is never evicted to make
        /// room: an entry that would need that is left uncached. Older
        /// entries make room first.
        #[test]
        fn a_load_never_evicts_its_own_entries() {
            let mut c = Caches::default();
            let mut read = HashMap::new();
            let older = key(0, 99);
            c.put_timeline(
                older.backend_key(),
                timeline_of(&older, 1000, &mut read),
            );
            let one = c.bytes();
            let probe = key(0, 98);
            c.put_timeline(
                probe.backend_key(),
                timeline_of(&probe, 1000, &mut read),
            );
            let each = c.bytes() - one;
            c.invalidate(&probe);
            // Room for the older timeline and two more.
            c.set_limit_for_test(one + 2 * each);
            let keys: Vec<SchemaKey> = (0..5).map(|t| key(0, t)).collect();
            let _load = c.begin_load();
            let admitted: Vec<bool> = keys
                .iter()
                .map(|k| {
                    c.put_timeline(
                        k.backend_key(),
                        timeline_of(k, 1000, &mut read),
                    )
                    .is_some()
                })
                .collect();
            c.end_load();
            assert_eq!(c.held_for(&older), 0, "the older one made room");
            let n = admitted.iter().take_while(|a| **a).count();
            assert!(n >= 1 && n < keys.len(), "{admitted:?}");
            assert!(admitted[n..].iter().all(|a| !a), "{admitted:?}");
            for k in &keys[..n] {
                assert_eq!(
                    c.held_for(k),
                    1,
                    "evicted by its own load: {admitted:?}"
                );
            }
            c.audit();
        }

        /// The bound holds whenever an insert grows the maps' buckets:
        /// across many bounds, every insert leaves the estimate within it.
        #[test]
        fn the_bound_holds_when_the_maps_grow() {
            let mut read = HashMap::new();
            for kib in 2..160u64 {
                let max = kib * 1024 + 37 * kib;
                let mut c = Caches::default();
                c.set_limit_for_test(max);
                for t in 0..400 {
                    let k = key(0, t);
                    c.put_timeline(
                        k.backend_key(),
                        timeline_of(&k, 100, &mut read),
                    );
                    assert!(c.bytes() <= max, "{} > {max} at {t}", c.bytes());
                }
            }
        }

        /// A batch is sized in bytes: at most half the budget at the held
        /// timelines' average cost, never more than one statement's
        /// tables, and at least one.
        #[test]
        fn a_batch_is_sized_by_the_byte_budget() {
            let mut c = Caches::default();
            assert_eq!(
                c.batch_room(),
                crate::mysql::mysql_history::BATCH_MAX_TABLES
            );
            let mut read = HashMap::new();
            c.set_limit_for_test(64 << 20);
            for t in 0..4 {
                let k = key(0, t);
                c.put_timeline(
                    k.backend_key(),
                    timeline_of(&k, 1 << 20, &mut read),
                );
            }
            let room = c.batch_room();
            assert!((20..=32).contains(&room), "{room}");
            c.set_limit_for_test(1 << 20);
            assert_eq!(c.batch_room(), 1);
        }
    }
}

/// The measurement-build cache override and instrumentation
/// (`docs/design/selection-cache-diagnosis.md`).
#[cfg(test)]
mod perf_tests {
    use super::*;
    use crate::perf_phases::ENV_CACHE_BYTES;

    /// An environment holding `bytes` for the override.
    fn env(bytes: Option<String>) -> impl Fn(&str) -> Option<String> {
        move |name| bytes.clone().filter(|_| name == ENV_CACHE_BYTES)
    }

    /// A default build ignores the variable: the code's bound.
    #[cfg(not(feature = "perf-phases"))]
    #[test]
    fn perf_default_builds_ignore_the_cache_override() {
        let c = Caches::new_with("p", "s", env(Some("1024".into())));
        assert_eq!(c.max_bytes, SELECTION_CACHE_BYTES);
    }

    /// The counters a measurement build exports, as `name labels value`
    /// (a phase timing's value is its number of samples).
    #[cfg(feature = "perf-phases")]
    pub(super) fn perf_metrics(f: impl FnOnce()) -> Vec<String> {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};
        let rec = DebuggingRecorder::new();
        let snap = rec.snapshotter();
        metrics::with_local_recorder(&rec, f);
        let mut seen = Vec::new();
        for (key, _, _, value) in snap.snapshot().into_vec() {
            let name = key.key().name().to_string();
            if !name.starts_with("deltaforge_perf_selection_")
                && name != "deltaforge_perf_phase_latency_seconds"
            {
                continue;
            }
            let labels: Vec<String> = key
                .key()
                .labels()
                .map(|l| format!("{}={}", l.key(), l.value()))
                .collect();
            let v = match value {
                DebugValue::Counter(c) => c as f64,
                DebugValue::Gauge(g) => g.0,
                DebugValue::Histogram(h) => h.len() as f64,
            };
            seen.push(format!("{name} {} {v}", labels.join(",")));
        }
        seen.sort();
        seen
    }

    /// A measurement build takes the byte bound from its environment,
    /// publishes it, and counts what it evicts by entry kind.
    #[cfg(feature = "perf-phases")]
    #[test]
    fn perf_a_measurement_build_takes_the_cache_override() {
        let key =
            |t: &str| SchemaKey::new("acme", "s", "f".repeat(32), "db", t);
        // Room for two of these timelines, not three.
        let two = {
            let mut c = Caches::default();
            c.seed_for_test(&key("a"));
            c.seed_for_test(&key("b"));
            c.bytes()
        };
        let seen = perf_metrics(|| {
            let mut c = Caches::new_with("p", "s", env(Some(two.to_string())));
            assert_eq!(c.max_bytes, two);
            for t in ["a", "b", "c"] {
                c.seed_for_test(&key(t));
            }
            assert_eq!(c.timelines(), 2);
        });
        assert_eq!(
            seen,
            [
                format!(
                    "deltaforge_perf_selection_cache_limit bound=bytes {two}"
                ),
                "deltaforge_perf_selection_evictions_total entry=timeline 1"
                    .to_string(),
            ]
        );
    }

    /// Lookups by entry and result, and store reads by kind (reads, tables
    /// asked for, stored bytes installed).
    #[cfg(feature = "perf-phases")]
    #[test]
    fn perf_lookups_and_store_reads_are_counted() {
        let seen = perf_metrics(|| {
            crate::perf_phases::cache_lookup("timeline", true);
            crate::perf_phases::cache_lookup("timeline", false);
            crate::perf_phases::cache_lookup("selection", false);
            crate::perf_phases::store_read("history", 5, 4096);
            crate::perf_phases::store_read("version", 1, 0);
        });
        assert_eq!(
            seen,
            [
                "deltaforge_perf_selection_lookups_total entry=selection,result=miss 1",
                "deltaforge_perf_selection_lookups_total entry=timeline,result=hit 1",
                "deltaforge_perf_selection_lookups_total entry=timeline,result=miss 1",
                "deltaforge_perf_selection_store_bytes_total read=history 4096",
                "deltaforge_perf_selection_store_bytes_total read=version 0",
                "deltaforge_perf_selection_store_reads_total read=history 1",
                "deltaforge_perf_selection_store_reads_total read=version 1",
                "deltaforge_perf_selection_store_tables_total read=history 5",
                "deltaforge_perf_selection_store_tables_total read=version 1",
            ]
        );
    }
}
