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
/// ([`mysql_cache_cost`]) charged at once: entries (timelines, selected
/// schemas and read-ahead versions, with their keys and list nodes),
/// current or stale, barrier streams (each once), the index, the maps'
/// buckets, values awaiting a step, and a cache handed to the reclamation
/// thread until it is freed. No count of tables: as many as fit.
///
/// [`mysql_cache_cost`]: super::mysql_cache_cost
pub(crate) const SELECTION_CACHE_BYTES: u64 = 256 * 1024 * 1024;

/// A value charged more than this is never freed by a cache operation
/// itself, only by a step ([`Caches::step`]) or the reclamation thread;
/// and one top-level operation frees at most this much inline in total: a
/// put, or everything inside a load or a batch of installs
/// ([`Caches::begin_operation`]), which share one allowance.
pub(crate) const INLINE_BYTES: u64 = 16 * 1024;
/// Values detached by an operation and awaiting a step: a fixed ring.
const DEFERRED: usize = 16;
/// What one step aims to stay within: a target (a step overruns it by at
/// most the item it is freeing).
const STEP: std::time::Duration = std::time::Duration::from_millis(1);

/// One entry's node in the LRU list: its key, its value and two links.
const NODE: u64 = (std::mem::size_of::<CacheKey>()
    + std::mem::size_of::<Entry>()
    + 2 * std::mem::size_of::<usize>()) as u64;
/// One bucket of the LRU's map: a key reference and the node's pointer.
const LRU_SLOT: usize = 2 * std::mem::size_of::<usize>();
/// One bucket of the barrier streams' map.
const BARRIER_SLOT: usize = std::mem::size_of::<(String, Held)>();
/// One bucket of the map of stale barrier streams still held.
const STALE_SLOT: usize = std::mem::size_of::<(usize, Held)>();

/// What an entry costs beyond its value: its node and its key's own
/// strings (the table's string is shared by the table's keys and counted
/// once, by the index).
fn key_cost(key: &CacheKey) -> u64 {
    use super::mysql_cache_cost::{allocated, shared_str};
    allocated(NODE)
        + match key {
            CacheKey::Timeline(_) | CacheKey::Version(..) => 0,
            CacheKey::Selection(_, d, g) => shared_str(d) + shared_str(g),
        }
}

/// `deltaforge_mysql_selection_cache_evictions_total{pipeline,source}`:
/// entries evicted to make room, current or stale.
pub(crate) const METRIC_EVICTIONS: &str =
    "deltaforge_mysql_selection_cache_evictions_total";
/// `..._stale_evictions_total{pipeline,source}`: of which stale.
pub(crate) const METRIC_STALE_EVICTIONS: &str =
    "deltaforge_mysql_selection_cache_stale_evictions_total";
/// `deltaforge_mysql_selection_cache_bytes{pipeline,source}`: the
/// estimated bytes charged against the bound ([`SELECTION_CACHE_BYTES`]).
pub(crate) const METRIC_BYTES: &str = "deltaforge_mysql_selection_cache_bytes";
/// `deltaforge_mysql_selection_cache_timelines{pipeline,source}`: the
/// timelines held, current or stale.
pub(crate) const METRIC_TIMELINES: &str =
    "deltaforge_mysql_selection_cache_timelines";
/// `..._live_bytes{pipeline,source}`: the current entries' bytes.
pub(crate) const METRIC_LIVE_BYTES: &str =
    "deltaforge_mysql_selection_cache_live_bytes";
/// `..._stale_bytes{pipeline,source}`: stale entries' bytes, awaiting a
/// sweep, an eviction or a replacement.
pub(crate) const METRIC_STALE_BYTES: &str =
    "deltaforge_mysql_selection_cache_stale_bytes";
/// `..._deferred_bytes{pipeline,source}`: detached values awaiting a step.
pub(crate) const METRIC_DEFERRED_BYTES: &str =
    "deltaforge_mysql_selection_cache_deferred_bytes";
/// `..._handed_bytes{pipeline,source}`: a cache handed to the reclamation
/// thread and not yet freed.
pub(crate) const METRIC_HANDED_BYTES: &str =
    "deltaforge_mysql_selection_cache_handed_bytes";
/// `..._reclaimed_total{pipeline,source,mode}`: values freed, by where:
/// `inline` (within an operation's budget), `step`, `thread`.
pub(crate) const METRIC_RECLAIMED: &str =
    "deltaforge_mysql_selection_cache_reclaimed_total";
/// `..._refused_total{pipeline,source,why}`: values not cached: `bound`
/// (no room could be made), `deferred` (the room is in values awaiting the
/// next step), `deferred_full` (making room would need a deferred slot and
/// none is free), `exhausted` (the stamps are).
pub(crate) const METRIC_REFUSED: &str =
    "deltaforge_mysql_selection_cache_refused_total";

/// A cache key. Its strings are shared: a cached key's table string is the
/// one its table's other keys and the index hold.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) enum CacheKey {
    /// A validated timeline, by table key.
    Timeline(Arc<str>),
    /// A selected schema by (table key, decisive record, TableMap
    /// signature). The key carries the lineage; the decisive record carries
    /// the row position's effective proof.
    Selection(Arc<str>, Arc<str>, Arc<str>),
    /// A registered version of a table, read ahead with its timeline.
    Version(Arc<str>, i32),
}

impl CacheKey {
    pub(super) fn table(&self) -> &str {
        match self {
            CacheKey::Timeline(t)
            | CacheKey::Selection(t, _, _)
            | CacheKey::Version(t, _) => t,
        }
    }

    /// The key with `table` (the same string) as its table's.
    pub(super) fn with_table(self, table: Arc<str>) -> Self {
        debug_assert_eq!(*table, *self.table());
        match self {
            CacheKey::Timeline(_) => CacheKey::Timeline(table),
            CacheKey::Selection(_, d, g) => CacheKey::Selection(table, d, g),
            CacheKey::Version(_, v) => CacheKey::Version(table, v),
        }
    }

    /// Whether the key holds `table` itself as its table's string (tests).
    #[cfg(test)]
    pub(super) fn shares_table(&self, table: &Arc<str>) -> bool {
        match self {
            CacheKey::Timeline(t)
            | CacheKey::Selection(t, _, _)
            | CacheKey::Version(t, _) => Arc::ptr_eq(t, table),
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

/// An entry: its value, its estimated bytes (key and node included), the
/// load that admitted it (0: none), its key's slot in the index and its
/// stamp (what decides whether it is current; see
/// [`super::mysql_cache_index`]).
#[derive(Debug, Clone)]
struct Entry {
    cached: Cached,
    bytes: u64,
    load: u64,
    slot: u32,
    stamp: u64,
}

/// A barrier stream held for the cached timelines sharing it: how many
/// do, the stamp of the admission that first held it, and whether a
/// barrier was appended to it since. Current while not cut and stamped
/// after the last full clear in place.
#[derive(Debug)]
struct Held {
    stream: Arc<BarrierStream>,
    refs: usize,
    stamp: u64,
    cut: bool,
}

/// A stale stream's key among those still held: its allocation's address
/// (unique while held).
fn addr(s: &Arc<BarrierStream>) -> usize {
    Arc::as_ptr(s) as usize
}

/// Values detached from the cache and awaiting a step, with what each
/// still charges: a fixed ring, part of the cache itself.
#[derive(Debug, Default)]
struct Ring {
    slots: [Option<(Cached, u64)>; DEFERRED],
    head: usize,
    len: usize,
    bytes: u64,
}

impl Ring {
    fn full(&self) -> bool {
        self.len == DEFERRED
    }

    fn push(&mut self, value: Cached, charge: u64) {
        assert!(!self.full(), "a deferred slot was reserved");
        self.slots[(self.head + self.len) % DEFERRED] = Some((value, charge));
        self.len += 1;
        self.bytes += charge;
    }

    fn pop(&mut self) -> Option<(Cached, u64)> {
        if self.len == 0 {
            return None;
        }
        let (value, charge) =
            self.slots[self.head].take().expect("a deferred value");
        self.head = (self.head + 1) % DEFERRED;
        self.len -= 1;
        self.bytes -= charge;
        Some((value, charge))
    }
}

/// How a detached entry's value is freed.
#[derive(Clone, Copy)]
enum Fate {
    /// Now, within the operation's inline budget.
    Inline,
    /// By a later step: it stays charged in the ring until then.
    Defer,
    /// Now, by a step.
    Step,
}

/// Making room for one more entry.
enum Evicted {
    One,
    /// Nothing evictable is left (or only the current load's entries).
    Nothing,
    /// The next entry would need a deferred slot, and none is free.
    Full,
}

/// Every structure a cache held, handed to the reclamation thread by a
/// full clear.
struct Unit {
    entries: lru::LruCache<CacheKey, Entry>,
    barriers: std::collections::HashMap<String, Held>,
    stale_streams: std::collections::HashMap<usize, Held>,
    index: super::mysql_cache_index::Index,
    ring: Ring,
}

/// A unit and what it charges its cache until it is freed.
pub(crate) struct Job {
    unit: Unit,
    charge: u64,
    values: u64,
    handed: Arc<std::sync::atomic::AtomicU64>,
    reclaimed: metrics::Counter,
    labels: Arc<(String, String)>,
}

/// Free a handed-off unit, then release its charge. A panic while freeing
/// it aborts the process: its memory could no longer be accounted, and a
/// pipeline restarted in this process would add to it. The values freed
/// (strings, vectors, `Arc`s, JSON trees) do not panic when dropped; this
/// guards against a bug here.
fn reclaim(job: Job) {
    let Job {
        unit,
        charge,
        values,
        handed,
        reclaimed,
        labels,
    } = job;
    let freed =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            drop(unit)
        }));
    if freed.is_err() {
        tracing::error!(
            pipeline = %labels.0,
            source = %labels.1,
            charge,
            "freeing a selection cache handed to the reclamation thread \
             panicked; aborting: its memory could no longer be accounted"
        );
        std::process::abort();
    }
    reclaimed.increment(values);
    handed.fetch_sub(charge, std::sync::atomic::Ordering::AcqRel);
}

/// The process's reclamation thread (a queue of two units), started on
/// first use; `None` when it could not start (then every full clear is
/// made in place).
fn reclaimer() -> Option<std::sync::mpsc::SyncSender<Job>> {
    static THREAD: std::sync::OnceLock<
        Option<std::sync::mpsc::SyncSender<Job>>,
    > = std::sync::OnceLock::new();
    THREAD
        .get_or_init(|| {
            let (tx, rx) = std::sync::mpsc::sync_channel::<Job>(2);
            match std::thread::Builder::new()
                .name("selection-reclaim".into())
                .spawn(move || rx.into_iter().for_each(reclaim))
            {
                Ok(_) => Some(tx),
                Err(e) => {
                    warn!(
                        error = %e,
                        "the selection cache reclamation thread did not \
                         start: full clears are made in place"
                    );
                    None
                }
            }
        })
        .clone()
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
/// (`docs/design/mysql-mass-invalidation.md`, section 6, and
/// `docs/design/selection-cache-diagnosis.md`, section 8): admission
/// evicts the least recently used entries until the new one fits, before
/// inserting it. Barrier streams are held once, shared by the timelines
/// they apply to, and dropped with the last of them. Only this process
/// appends activation records and barriers for this source (the store
/// gate), so invalidating on every append keeps them exact; an evicted
/// entry is rebuilt exactly from the durable records.
///
/// Invalidating destroys nothing on the source's loop: a table's
/// invalidation and a barrier make entries stale (O(1) per table or
/// database reached), and a full clear hands every structure to the
/// reclamation thread, or, when that is busy with this cache's last one
/// or full, makes everything stale in place (O(1)). A stale entry is never
/// returned. It stays charged until a step ([`Self::step`], once per loop
/// iteration) sweeps it, or an eviction or replacement detaches it. A
/// detached value larger than [`INLINE_BYTES`], or past an operation's
/// inline budget, waits in a fixed ring of [`DEFERRED`] for a step, still
/// charged; with the ring full, the operation refuses instead (the value is
/// served uncached).
pub(crate) struct Caches {
    entries: lru::LruCache<CacheKey, Entry>,
    /// Barrier streams, by stream key, with the cached timelines holding
    /// each.
    barriers: std::collections::HashMap<String, Held>,
    /// Streams no longer current that stale timelines still hold, by
    /// address.
    stale_streams: std::collections::HashMap<usize, Held>,
    /// The entries' keys by database and table, and their currency.
    index: super::mysql_cache_index::Index,
    stamps: super::mysql_cache_index::Stamps,
    ring: Ring,
    timelines: usize,
    /// The estimated bytes of the entries and barrier streams held
    /// (current or stale).
    held: u64,
    /// Of which the barrier streams'.
    streams: u64,
    /// Of which the timelines' and versions' (a batch's size per table).
    table_bytes: u64,
    /// The most entries held since the LRU's map was last replaced: its
    /// buckets, kept after eviction and invalidation, are sized by it.
    peak: usize,
    max_bytes: u64,
    /// The current load, if one is admitting ([`Admission`]).
    load: u64,
    loading: bool,
    /// What the current top-level operation may still free inline.
    inline_left: u64,
    /// Open operations ([`Self::begin_operation`]): while any is, puts
    /// share its inline allowance.
    operations: u32,
    /// Bytes freed inline so far (tests).
    #[cfg(test)]
    freed_inline: u64,
    reclaim: Reclaim,
    /// A unit was handed off and its release not yet published.
    handing: bool,
    meters: Meters,
}

/// Where a full clear's structures go, and what they still charge.
struct Reclaim {
    thread: Option<std::sync::mpsc::SyncSender<Job>>,
    handed: Arc<std::sync::atomic::AtomicU64>,
    labels: Arc<(String, String)>,
}

/// The cache's metrics: evictions, reclamation and refusals, and its own
/// accounting, published after every change.
struct Meters {
    evictions: metrics::Counter,
    stale_evictions: metrics::Counter,
    bytes: metrics::Gauge,
    timelines: metrics::Gauge,
    live: metrics::Gauge,
    stale: metrics::Gauge,
    deferred: metrics::Gauge,
    handed: metrics::Gauge,
    /// By mode: inline, step, thread.
    reclaimed: [metrics::Counter; 3],
    /// By reason: bound, deferred, deferred_full, exhausted.
    refused: [metrics::Counter; 4],
}

const RECLAIMED: [&str; 3] = ["inline", "step", "thread"];
const REFUSED: [&str; 4] = ["bound", "deferred", "deferred_full", "exhausted"];
const BOUND: usize = 0;
const ROOM_DEFERRED: usize = 1;
const DEFERRED_FULL: usize = 2;
const EXHAUSTED: usize = 3;

impl Meters {
    fn noop() -> Self {
        Self {
            evictions: metrics::Counter::noop(),
            stale_evictions: metrics::Counter::noop(),
            bytes: metrics::Gauge::noop(),
            timelines: metrics::Gauge::noop(),
            live: metrics::Gauge::noop(),
            stale: metrics::Gauge::noop(),
            deferred: metrics::Gauge::noop(),
            handed: metrics::Gauge::noop(),
            reclaimed: std::array::from_fn(|_| metrics::Counter::noop()),
            refused: std::array::from_fn(|_| metrics::Counter::noop()),
        }
    }

    fn labelled(pipeline: &str, source: &str) -> Self {
        let labels = [
            ("pipeline", pipeline.to_string()),
            ("source", source.to_string()),
        ];
        let with = |name: &'static str, what: &'static str, v: &str| {
            let mut l = labels.to_vec();
            l.push((what, v.to_string()));
            metrics::counter!(name, &l)
        };
        Self {
            evictions: metrics::counter!(METRIC_EVICTIONS, &labels),
            stale_evictions: metrics::counter!(METRIC_STALE_EVICTIONS, &labels),
            bytes: metrics::gauge!(METRIC_BYTES, &labels),
            timelines: metrics::gauge!(METRIC_TIMELINES, &labels),
            live: metrics::gauge!(METRIC_LIVE_BYTES, &labels),
            stale: metrics::gauge!(METRIC_STALE_BYTES, &labels),
            deferred: metrics::gauge!(METRIC_DEFERRED_BYTES, &labels),
            handed: metrics::gauge!(METRIC_HANDED_BYTES, &labels),
            reclaimed: RECLAIMED.map(|m| with(METRIC_RECLAIMED, "mode", m)),
            refused: REFUSED.map(|w| with(METRIC_REFUSED, "why", w)),
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
        Self::with_bytes(SELECTION_CACHE_BYTES, Meters::noop(), None)
    }
}

impl Caches {
    /// The source's caches, metered under its labels, with the process's
    /// reclamation thread.
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
        let mut caches = Self::with_bytes(
            bytes,
            Meters::labelled(pipeline, source),
            reclaimer(),
        );
        caches.reclaim.labels =
            Arc::new((pipeline.to_string(), source.to_string()));
        caches.publish();
        caches
    }

    fn with_bytes(
        max_bytes: u64,
        meters: Meters,
        thread: Option<std::sync::mpsc::SyncSender<Job>>,
    ) -> Self {
        Self {
            entries: lru::LruCache::unbounded(),
            barriers: Default::default(),
            stale_streams: Default::default(),
            index: Default::default(),
            stamps: Default::default(),
            ring: Ring::default(),
            timelines: 0,
            held: 0,
            streams: 0,
            table_bytes: 0,
            peak: 0,
            max_bytes,
            load: 0,
            loading: false,
            inline_left: INLINE_BYTES,
            operations: 0,
            #[cfg(test)]
            freed_inline: 0,
            reclaim: Reclaim {
                thread,
                handed: Default::default(),
                labels: Default::default(),
            },
            handing: false,
            meters,
        }
    }

    /// A cache whose full clears go to `thread` (tests: a queue the test
    /// drains with [`reclaim`]).
    #[cfg(test)]
    fn with_thread(thread: std::sync::mpsc::SyncSender<Job>) -> Self {
        Self::with_bytes(SELECTION_CACHE_BYTES, Meters::noop(), Some(thread))
    }

    /// The maps' buckets at `entries` entries held at most: the LRU's
    /// (with its two sentinel nodes) and the barrier streams' two maps.
    fn maps_at(&self, entries: usize, barriers: usize, stale: usize) -> u64 {
        use super::mysql_cache_cost::{allocated, buckets_for, table};
        let lru = if entries == 0 {
            0
        } else {
            table(buckets_for(entries), LRU_SLOT) + 2 * allocated(NODE)
        };
        lru + table(buckets_for(barriers), BARRIER_SLOT)
            + table(buckets_for(stale), STALE_SLOT)
    }

    /// What the cache's own structures charge (all but a handed-off unit).
    fn local(&self) -> u64 {
        self.held
            + self.index.bytes()
            + self.maps_at(
                self.peak,
                self.barriers.capacity(),
                self.stale_streams.capacity(),
            )
            + self.ring.bytes
    }

    /// A handed-off unit's charge, until the thread has freed it.
    fn handed(&self) -> u64 {
        self.reclaim
            .handed
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// The estimated bytes charged against the bound: entries (current or
    /// stale), barrier streams, the index, the maps' buckets, deferred
    /// values and a handed-off unit.
    pub(crate) fn bytes(&self) -> u64 {
        self.local() + self.handed()
    }

    /// What one more entry, `streams` more barrier streams, and `moved`
    /// streams moved to the stale map add to the maps' buckets.
    fn growth(&self, streams: usize, moved: usize) -> u64 {
        let entries = self.peak.max(self.entries.len() + 1);
        let barriers =
            self.barriers.capacity().max(self.barriers.len() + streams);
        let stale = self
            .stale_streams
            .capacity()
            .max(self.stale_streams.len() + moved);
        self.maps_at(entries, barriers, stale)
            - self.maps_at(
                self.peak,
                self.barriers.capacity(),
                self.stale_streams.capacity(),
            )
    }

    /// Publish the accounting (after every change to it).
    fn publish(&self) {
        let live = self.index.live();
        self.meters.bytes.set(self.bytes() as f64);
        self.meters.timelines.set(self.timelines as f64);
        self.meters.live.set(live as f64);
        self.meters
            .stale
            .set((self.held - self.streams - live) as f64);
        self.meters.deferred.set(self.ring.bytes as f64);
        self.meters.handed.set(self.handed() as f64);
    }

    /// A value not cached (`why`), after whatever was detached making
    /// room: counted, and the accounting published.
    fn refuse(&self, why: usize) {
        self.meters.refused[why].increment(1);
        self.publish();
    }

    /// Timelines held.
    #[cfg(test)]
    pub(crate) fn timelines(&self) -> usize {
        self.timelines
    }

    /// Whether the entry of `key` is current.
    fn current(&self, key: &CacheKey, e: &Entry) -> bool {
        self.index.is_current(key, e.stamp)
    }

    /// Whether a held stream is current (may be shared by a new read).
    fn held_current(&self, h: &Held) -> bool {
        !h.cut && h.stamp > self.index.since()
    }

    /// The barrier streams held and current: those a load may reuse.
    pub(crate) fn barrier_streams(
        &self,
    ) -> std::collections::HashMap<String, Arc<BarrierStream>> {
        self.barriers
            .iter()
            .filter(|(_, h)| self.held_current(h))
            .map(|(k, h)| (k.clone(), h.stream.clone()))
            .collect()
    }

    /// The current entry of `key`, marked used. A stale one is a miss: it
    /// is neither returned nor freed here.
    fn get(&mut self, key: &CacheKey) -> Option<&Entry> {
        let e = self.entries.peek(key)?;
        if !self.current(key, e) {
            return None;
        }
        self.entries.get(key)
    }

    /// The holder of `s` among the held streams.
    fn holder(&self, s: &Arc<BarrierStream>) -> Option<&Held> {
        self.barriers
            .get(&s.stream)
            .filter(|h| Arc::ptr_eq(&h.stream, s))
            .or_else(|| self.stale_streams.get(&addr(s)))
    }

    /// What freeing `e` releases: its bytes, and those of the barrier
    /// streams it is the last holder of.
    fn release_of(&self, e: &Entry) -> u64 {
        let Cached::Timeline(t, _) = &e.cached else {
            return e.bytes;
        };
        e.bytes
            + t.streams
                .iter()
                .filter_map(|s| self.holder(s))
                .filter(|h| h.refs == 1)
                .map(|h| super::mysql_cache_cost::stream(&h.stream))
                .sum::<u64>()
    }

    /// How an operation frees a value releasing `release` bytes: inline
    /// within its budget, else in a deferred slot; `None` when that would
    /// need a slot and none is free (the caller then leaves the entry).
    fn fate(&self, release: u64) -> Option<Fate> {
        if release <= INLINE_BYTES && release <= self.inline_left {
            Some(Fate::Inline)
        } else if !self.ring.full() {
            Some(Fate::Defer)
        } else {
            None
        }
    }

    /// Unindex `key`, held at `slot`; the key moved into that slot is told.
    fn unindex(
        &mut self,
        key: &CacheKey,
        slot: u32,
        bytes: u64,
        current: bool,
    ) {
        if let Some(moved) = self.index.remove(key, slot, bytes, current) {
            self.entries
                .peek_mut(&moved)
                .expect("an indexed key is cached")
                .slot = slot;
        }
    }

    /// Unlink `key`'s entry (already taken from the LRU) from the index
    /// and the accounting, then free its value as `fate` says.
    fn detach(&mut self, key: &CacheKey, e: Entry, fate: Fate) {
        let current = self.current(key, &e);
        self.unindex(key, e.slot, e.bytes, current);
        let released = self.forget(&e);
        match fate {
            Fate::Inline => {
                self.inline_left = self.inline_left.saturating_sub(released);
                #[cfg(test)]
                {
                    self.freed_inline += released;
                }
                drop(e);
                self.meters.reclaimed[0].increment(1);
            }
            Fate::Step => {
                drop(e);
                self.meters.reclaimed[1].increment(1);
            }
            Fate::Defer => self.ring.push(e.cached, released),
        }
    }

    /// Take `e`'s accounting out (its value is freed by the caller, or
    /// deferred): what freeing it releases.
    fn forget(&mut self, e: &Entry) -> u64 {
        self.held -= e.bytes;
        let mut released = e.bytes;
        match &e.cached {
            Cached::Timeline(t, _) => {
                self.timelines -= 1;
                self.table_bytes -= e.bytes;
                // Its barrier streams go with the last timeline holding
                // them.
                for s in &t.streams {
                    released += self.unhold(s);
                }
            }
            Cached::Version(_) => self.table_bytes -= e.bytes,
            Cached::Selection(_) => {}
        }
        released
    }

    /// One holder of `s` fewer; the bytes released when it was the last.
    fn unhold(&mut self, s: &Arc<BarrierStream>) -> u64 {
        let mut last = |h: &mut Held| {
            h.refs -= 1;
            h.refs == 0
        };
        let gone = match self
            .barriers
            .get_mut(&s.stream)
            .filter(|h| Arc::ptr_eq(&h.stream, s))
            .map(&mut last)
        {
            Some(true) => self.barriers.remove(&s.stream),
            Some(false) => None,
            None => match self.stale_streams.get_mut(&addr(s)).map(last) {
                Some(true) => self.stale_streams.remove(&addr(s)),
                Some(false) => None,
                None => {
                    debug_assert!(false, "{} not held", s.stream);
                    None
                }
            },
        };
        gone.map_or(0, |h| {
            let b = super::mysql_cache_cost::stream(&h.stream);
            self.held -= b;
            self.streams -= b;
            b
        })
    }

    /// Make way for `key`'s new entry: an entry held under it, current or
    /// stale, is detached (its value freed inline or deferred). `false`
    /// when that would need a deferred slot and none is free: it stays,
    /// and the new value is not cached.
    fn replace(&mut self, key: &CacheKey) -> bool {
        let Some(e) = self.entries.peek(key) else {
            return true;
        };
        let Some(fate) = self.fate(self.release_of(e)) else {
            return false;
        };
        let (k, e) = self.entries.pop_entry(key).expect("peeked");
        self.detach(&k, e, fate);
        true
    }

    /// Evict the least recently used entry (a stale one too). A load never
    /// evicts what it admitted.
    fn evict_one(&mut self) -> Evicted {
        let Some((key, e)) = self.entries.peek_lru() else {
            return Evicted::Nothing;
        };
        if self.loading && e.load == self.load {
            return Evicted::Nothing;
        }
        let Some(fate) = self.fate(self.release_of(e)) else {
            return Evicted::Full;
        };
        let stale = !self.current(key, e);
        let (key, e) = self.entries.pop_lru().expect("peeked");
        crate::perf_phases::cache_eviction(match &e.cached {
            Cached::Timeline(..) => "timeline",
            Cached::Selection(_) => "selection",
            Cached::Version(_) => "version",
        });
        self.meters.evictions.increment(1);
        if stale {
            self.meters.stale_evictions.increment(1);
        }
        self.detach(&key, e, fate);
        Evicted::One
    }

    /// Evict until `need` more bytes fit; `false` (counted) when they
    /// cannot. Values awaiting a step count as room to come: eviction stops
    /// once they cover what is missing (evicting more would not make room
    /// any sooner), and the value is refused until a step has freed them.
    fn make_room(&mut self, need: impl Fn(&Self) -> u64) -> bool {
        loop {
            let need = need(self);
            if self.bytes() + need <= self.max_bytes {
                return true;
            }
            if self.bytes() - self.ring.bytes + need <= self.max_bytes {
                self.refuse(ROOM_DEFERRED);
                return false;
            }
            match self.evict_one() {
                Evicted::One => {}
                Evicted::Nothing => {
                    self.refuse(BOUND);
                    return false;
                }
                Evicted::Full => {
                    self.refuse(DEFERRED_FULL);
                    return false;
                }
            }
        }
    }

    /// Open a top-level operation: every put until it is closed
    /// ([`Self::end_operation`]), nested ones included, shares one inline
    /// allowance of [`INLINE_BYTES`]. A load is one; so is a batch of
    /// installs. A put outside any is its own.
    pub(crate) fn begin_operation(&mut self) {
        if self.operations == 0 {
            self.inline_left = INLINE_BYTES;
        }
        self.operations += 1;
    }

    pub(crate) fn end_operation(&mut self) {
        self.operations -= 1;
    }

    /// Start a put: its inline allowance (its own, or its operation's), and
    /// whether it may admit at all (the stamps are not exhausted) and
    /// replace what `key` holds.
    fn begin_put(&mut self, key: &CacheKey) -> bool {
        if self.operations == 0 {
            self.inline_left = INLINE_BYTES;
        }
        if self.stamps.exhausted() {
            self.refuse(EXHAUSTED);
            return false;
        }
        if !self.replace(key) {
            self.refuse(DEFERRED_FULL);
            return false;
        }
        true
    }

    /// The next stamp; on exhaustion the cache fails closed (nothing is
    /// current or admitted until it holds nothing).
    fn stamp(&mut self) -> Option<u64> {
        let s = self.stamps.next();
        if s.is_none() {
            self.index.fail_closed();
            warn!(
                "the selection cache's stamps are exhausted: nothing is \
                 cached until it is empty"
            );
        }
        s
    }

    /// Insert `key`'s entry of `bytes` (its value's), evicting until it
    /// fits; one that cannot fit is not cached. Whether it was.
    fn put(&mut self, key: CacheKey, value: Cached, bytes: u64) -> bool {
        if !self.begin_put(&key) {
            return false;
        }
        let bytes = bytes + key_cost(&key);
        if !self.make_room(|c| bytes + c.growth(0, 0) + c.index.growth(&key)) {
            return false;
        }
        let Some(stamp) = self.stamp() else {
            self.refuse(EXHAUSTED);
            return false;
        };
        self.push(key, value, bytes, stamp);
        true
    }

    /// Insert an entry that fits, stamped `stamp`.
    fn push(&mut self, key: CacheKey, cached: Cached, bytes: u64, stamp: u64) {
        self.held += bytes;
        if !matches!(cached, Cached::Selection(_)) {
            self.table_bytes += bytes;
        }
        let load = if self.loading { self.load } else { 0 };
        let (key, slot) = self.index.insert(key, bytes, stamp);
        let replaced = self.entries.push(
            key,
            Entry {
                cached,
                bytes,
                load,
                slot,
                stamp,
            },
        );
        debug_assert!(replaced.is_none(), "replaced before");
        self.peak = self.peak.max(self.entries.len());
        self.publish();
    }

    /// Start a load: what it may admit ([`Admission`]).
    fn begin_load(&mut self) -> Admission {
        self.begin_operation();
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
        self.end_operation();
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
    /// bound holds at the held tables' average cost (timeline and version,
    /// current or stale), at most one statement's, at least one. Before
    /// anything is held: one statement's.
    pub(crate) fn batch_room(&self) -> usize {
        let max = super::mysql_history::BATCH_MAX_TABLES;
        if self.timelines == 0 {
            return max;
        }
        let each = (self.table_bytes / self.timelines as u64).max(1);
        ((self.max_bytes / 2 / each) as usize).clamp(1, max)
    }

    pub(crate) fn timeline(&mut self, key: &str) -> Option<Arc<Timeline>> {
        match self.get(&CacheKey::Timeline(key.into()))? {
            Entry {
                cached: Cached::Timeline(t, _),
                ..
            } => Some(t.clone()),
            _ => None,
        }
    }

    /// The backend keys of every current table flagged proven, read
    /// without touching recency (a speculative window's candidate filter).
    pub(crate) fn proven_keys(&self) -> std::collections::HashSet<String> {
        self.entries
            .iter()
            .filter_map(|(k, e)| match (k, &e.cached) {
                (CacheKey::Timeline(key), Cached::Timeline(_, true))
                    if self.current(k, e) =>
                {
                    Some(key.to_string())
                }
                _ => None,
            })
            .collect()
    }

    /// Whether the table's last selection proved a version since its
    /// timeline last changed (its current timeline is flagged).
    pub(crate) fn is_proven(&mut self, key: &SchemaKey) -> bool {
        let k = CacheKey::Timeline(key.backend_key().into());
        matches!(
            self.entries.peek(&k),
            Some(e @ Entry {
                cached: Cached::Timeline(_, true),
                ..
            }) if self.current(&k, e)
        )
    }

    /// The table's last selection proved a version over its cached
    /// timeline (no extra entry: a flag on that timeline's, if current).
    fn mark_proven(&mut self, key: &SchemaKey) {
        let k = CacheKey::Timeline(key.backend_key().into());
        if !self.entries.peek(&k).is_some_and(|e| self.current(&k, e)) {
            return;
        }
        if let Some(Entry {
            cached: Cached::Timeline(_, proven),
            ..
        }) = self.entries.peek_mut(&k)
        {
            *proven = true;
        }
    }

    /// The barrier streams of `timeline` the cache does not hold current
    /// (this read of them).
    fn unheld<'a>(
        &'a self,
        timeline: &'a Timeline,
    ) -> impl Iterator<Item = &'a Arc<BarrierStream>> {
        timeline.streams.iter().filter(|s| {
            self.barriers
                .get(&s.stream)
                .is_none_or(|h| !self.held_current(h) || *h.stream != ***s)
        })
    }

    /// Of those, how many replace a held read of the same stream (which
    /// moves to the stale streams).
    fn displaced(&self, timeline: &Timeline) -> usize {
        self.unheld(timeline)
            .filter(|s| self.barriers.contains_key(&s.stream))
            .count()
    }

    /// What caching `timeline` for `key` adds: the entry, its indexing,
    /// and the barrier streams of it not held.
    fn timeline_cost(&self, key: &CacheKey, timeline: &Timeline) -> u64 {
        super::mysql_cache_cost::timeline(timeline)
            + key_cost(key)
            + self.index.growth(key)
            + self
                .unheld(timeline)
                .map(|s| super::mysql_cache_cost::stream(s))
                .sum::<u64>()
    }

    /// Cache `timeline`, holding its barrier streams once: a stream already
    /// held and current is shared, a new one is held (and accounted) from
    /// here. The timeline as cached (sharing the held streams), or `None`
    /// when it does not fit.
    fn put_timeline(
        &mut self,
        key: String,
        timeline: Arc<Timeline>,
    ) -> Option<Arc<Timeline>> {
        self.put_timeline_as(CacheKey::Timeline(key.into()), timeline)
    }

    fn put_timeline_as(
        &mut self,
        key: CacheKey,
        timeline: Arc<Timeline>,
    ) -> Option<Arc<Timeline>> {
        if !self.begin_put(&key) {
            return None;
        }
        // Evict until the timeline, and those of its streams not held, fit
        // (an eviction may drop a stream it would have shared).
        if !self.make_room(|c| {
            c.timeline_cost(&key, &timeline)
                + c.growth(c.unheld(&timeline).count(), c.displaced(&timeline))
        }) {
            return None;
        }
        let Some(stamp) = self.stamp() else {
            self.refuse(EXHAUSTED);
            return None;
        };
        let mut shared =
            Arc::try_unwrap(timeline).unwrap_or_else(|t| (*t).clone());
        for s in shared.streams.iter_mut() {
            let share = self
                .barriers
                .get(&s.stream)
                .is_some_and(|h| self.held_current(h) && *h.stream == **s);
            if share {
                let h = self.barriers.get_mut(&s.stream).expect("held");
                *s = h.stream.clone();
                h.refs += 1;
                continue;
            }
            // Another read of the stream, or one a barrier or a full
            // clear made stale: this one is held; the old one stays,
            // charged, with the stale timelines holding it.
            if let Some(old) = self.barriers.remove(&s.stream) {
                self.stale_streams.insert(addr(&old.stream), old);
            }
            self.barriers.insert(
                s.stream.clone(),
                Held {
                    stream: s.clone(),
                    refs: 1,
                    stamp,
                    cut: false,
                },
            );
            let b = super::mysql_cache_cost::stream(s);
            self.held += b;
            self.streams += b;
        }
        self.timelines += 1;
        let shared = Arc::new(shared);
        let bytes = super::mysql_cache_cost::timeline(&shared) + key_cost(&key);
        self.push(key, Cached::Timeline(shared.clone(), false), bytes, stamp);
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
        let key = CacheKey::Timeline(key.into());
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
            table.as_str().into(),
            decisive.as_str().into(),
            signature.as_str().into(),
        );
        match self.get(&key)? {
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
        match self.get(&CacheKey::Version(table.into(), version))? {
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
            CacheKey::Version(table.into(), v.version),
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
        let key = CacheKey::Version(table.into(), v.version);
        let bytes = super::mysql_cache_cost::version(&v);
        if self.admit(adm, bytes + key_cost(&key) + self.index.growth(&key)) {
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
            CacheKey::Selection(
                table.into(),
                decisive.into(),
                signature.into(),
            ),
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

    /// Entries held, current or stale (tests).
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
            .filter(|(e, _)| matches!(e, CacheKey::Version(t, _) if **t == *k))
            .map(|(_, e)| e.bytes)
            .sum()
    }

    /// The entries held for the table `key`, current or stale (tests).
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

    /// Make every later full clear in place (tests).
    #[cfg(test)]
    pub(crate) fn without_thread_for_test(&mut self) {
        self.reclaim.thread = None;
    }

    /// Continue the stamps from `last` (tests: exhaustion).
    #[cfg(test)]
    fn set_stamp_for_test(&mut self, last: u64) {
        self.stamps.set_last_for_test(last);
    }

    /// Step until nothing is left to reclaim, without a time limit (tests).
    #[cfg(test)]
    pub(crate) fn settle(&mut self) {
        while self.step_one() {}
        self.reset_if_exhausted();
        self.publish();
    }

    /// Recount what the cache holds and check its running accounting: the
    /// bytes (each entry's from its value and key, each held stream's,
    /// current or stale, the deferred values', the maps' buckets), the
    /// timelines, each held stream's holders, and the index (slots, live
    /// bytes, queues) (tests).
    #[cfg(test)]
    pub(crate) fn audit(&self) {
        use super::mysql_cache_cost as cost;
        let mut holders: std::collections::HashMap<usize, usize> =
            Default::default();
        let (mut timelines, mut held, mut table_bytes) = (0, 0, 0);
        for (key, e) in self.entries.iter() {
            let bytes = key_cost(key)
                + match &e.cached {
                    Cached::Timeline(t, _) => {
                        timelines += 1;
                        for s in &t.streams {
                            assert!(
                                self.holder(s).is_some(),
                                "{} not held",
                                s.stream
                            );
                            *holders.entry(addr(s)).or_default() += 1;
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
        let mut streams = 0;
        for (name, h) in &self.barriers {
            assert_eq!(*name, h.stream.stream);
            assert_eq!(holders.get(&addr(&h.stream)), Some(&h.refs), "{name}");
            streams += cost::stream(&h.stream);
        }
        for (a, h) in &self.stale_streams {
            assert_eq!(*a, addr(&h.stream));
            assert!(
                !self.held_current(h)
                    || self
                        .barriers
                        .get(&h.stream.stream)
                        .is_some_and(|c| !Arc::ptr_eq(&c.stream, &h.stream)),
                "a stale stream"
            );
            assert_eq!(holders.get(a), Some(&h.refs), "{}", h.stream.stream);
            streams += cost::stream(&h.stream);
        }
        assert_eq!(
            holders.len(),
            self.barriers.len() + self.stale_streams.len(),
            "streams held"
        );
        assert_eq!(timelines, self.timelines, "timelines");
        assert_eq!(streams, self.streams, "streams' bytes");
        assert_eq!(held + streams, self.held, "bytes held");
        assert_eq!(table_bytes, self.table_bytes, "tables' bytes");
        let deferred: u64 = (0..self.ring.len)
            .map(|i| {
                self.ring.slots[(self.ring.head + i) % DEFERRED]
                    .as_ref()
                    .expect("a deferred value")
                    .1
            })
            .sum();
        assert_eq!(deferred, self.ring.bytes, "deferred bytes");
        self.index.audit(
            self.entries
                .iter()
                .map(|(k, e)| (k, e.slot, e.stamp, e.bytes)),
        );
        assert!(self.peak >= self.entries.len(), "peak");
        assert!(self.bytes() <= self.max_bytes || self.held == 0, "bound");
    }

    /// A record was appended for `key`: the table's entries are stale (a
    /// sweep removes them; none is freed here).
    pub(crate) fn invalidate(&mut self, key: &SchemaKey) {
        let t = key.backend_key();
        if !self.index.holds(&t) {
            return;
        }
        if let Some(s) = self.stamp() {
            self.index.advance_table(&t, s);
        }
        self.publish();
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
    /// `snapshot` itself (the same `Arc`) as the table's current timeline:
    /// an eviction, a barrier, a full clear or any invalidation since the
    /// snapshot wins (the entry is gone or stale), and the table reloads.
    /// Whether it was installed (an install that does not fit the bound
    /// leaves the table uncached, never stale).
    pub(crate) fn install_if_current(
        &mut self,
        key: &SchemaKey,
        snapshot: &Arc<Timeline>,
        timeline: Arc<Timeline>,
    ) -> bool {
        let k = CacheKey::Timeline(key.backend_key().into());
        let current = matches!(
            self.entries.peek(&k),
            Some(e @ Entry { cached: Cached::Timeline(t, _), .. })
                if Arc::ptr_eq(t, snapshot) && self.current(&k, e)
        );
        if current {
            self.install(key, timeline);
        }
        current
    }

    /// A barrier was appended to `stream` (a lineage's or a database's):
    /// the stream, and every entry of a table it applies to (whose key
    /// starts with it), are stale. Every other table stays current. The
    /// index reaches those entries through their databases (O(databases))
    /// without visiting the others. A barrier that applies to every entry
    /// is a full clear.
    pub(crate) fn barrier_appended(&mut self, stream: &str) {
        if self.index.covers_all(stream) {
            self.invalidate_all();
            return;
        }
        if let Some(h) = self.barriers.get_mut(stream) {
            h.cut = true;
        }
        if let Some(s) = self.stamp() {
            self.index.barrier(stream, s);
        }
        self.publish();
    }

    /// Everything is stale (a reload request without a table, or a
    /// barrier over every entry). Every structure is handed to the
    /// reclamation thread, charged until it is freed there; when the
    /// thread is unavailable, this cache's last unit is still there, or
    /// its queue is full, everything is made stale in place instead (O(1))
    /// and swept by steps. Nothing is freed here.
    pub(crate) fn invalidate_all(&mut self) {
        if self.local() == 0 {
            return;
        }
        if !self.hand_off() {
            if let Some(s) = self.stamp() {
                self.index.clear(s);
            }
        }
        self.publish();
    }

    /// Hand every structure to the reclamation thread, if it is there,
    /// holds no unit of this cache and has room (never waits). Whether it
    /// took them: when it does not, they are back in place, intact.
    fn hand_off(&mut self) -> bool {
        use std::sync::atomic::Ordering;
        let Some(thread) = &self.reclaim.thread else {
            return false;
        };
        if self.handed() != 0 {
            return false;
        }
        let charge = self.local();
        let values = (self.entries.len() + self.ring.len) as u64;
        let unit = Unit {
            entries: std::mem::replace(
                &mut self.entries,
                lru::LruCache::unbounded(),
            ),
            barriers: std::mem::take(&mut self.barriers),
            stale_streams: std::mem::take(&mut self.stale_streams),
            index: std::mem::take(&mut self.index),
            ring: std::mem::take(&mut self.ring),
        };
        self.reclaim.handed.store(charge, Ordering::Release);
        let job = Job {
            unit,
            charge,
            values,
            handed: self.reclaim.handed.clone(),
            reclaimed: self.meters.reclaimed[2].clone(),
            labels: self.reclaim.labels.clone(),
        };
        match thread.try_send(job) {
            Ok(()) => {
                self.handing = true;
                self.timelines = 0;
                self.held = 0;
                self.streams = 0;
                self.table_bytes = 0;
                self.peak = 0;
                true
            }
            Err(
                std::sync::mpsc::TrySendError::Full(job)
                | std::sync::mpsc::TrySendError::Disconnected(job),
            ) => {
                // Not taken: everything back where it was.
                self.reclaim.handed.store(0, Ordering::Release);
                let Unit {
                    entries,
                    barriers,
                    stale_streams,
                    index,
                    ring,
                } = job.unit;
                self.entries = entries;
                self.barriers = barriers;
                self.stale_streams = stale_streams;
                self.index = index;
                self.ring = ring;
                false
            }
        }
    }

    /// Reclaim for up to [`STEP`] (once per source-loop iteration): values
    /// deferred first, then the stale entries the sweeps find (queued
    /// tables, then queued databases), then those a full clear in place
    /// left at the LRU's end, then the buckets of emptied maps. Each item
    /// is freed whole, so a step overruns its target by at most one item.
    /// A source without events runs no steps: what is pending stays
    /// charged, within the bound, with no completion deadline.
    pub(crate) fn step(&mut self) {
        // Between iterations no operation is open; one left open (a path
        // that returned early) would only defer more, so it is closed.
        debug_assert_eq!(self.operations, 0, "an operation left open");
        self.operations = 0;
        if self.handing {
            // Its release is published once the thread has freed it.
            self.handing = self.handed() != 0;
            self.publish();
        }
        if !self.pending() {
            return;
        }
        let deadline = std::time::Instant::now() + STEP;
        while self.step_one() {
            if std::time::Instant::now() >= deadline {
                break;
            }
        }
        self.reset_if_exhausted();
        self.publish();
    }

    /// Whether a step has anything to do (O(1)).
    fn pending(&self) -> bool {
        self.ring.len > 0
            || self.index.dirty()
            || self.stale_at_end()
            || self.emptied()
            || (self.stamps.exhausted() && self.is_empty())
    }

    /// The LRU's last entry is stale by a full clear (every such entry is
    /// behind every later one: a stale entry is never moved up).
    fn stale_at_end(&self) -> bool {
        self.entries
            .peek_lru()
            .is_some_and(|(_, e)| e.stamp <= self.index.since())
    }

    /// No entry is held, but the maps keep buckets.
    fn emptied(&self) -> bool {
        self.entries.is_empty()
            && (self.peak > 0
                || self.barriers.capacity() > 0
                || self.stale_streams.capacity() > 0)
    }

    /// Nothing held at all (a handed-off unit aside).
    fn is_empty(&self) -> bool {
        self.entries.is_empty()
            && self.ring.len == 0
            && self.index.is_empty()
            && self.barriers.is_empty()
            && self.stale_streams.is_empty()
    }

    /// Exhausted stamps start again once nothing is held: no entry can
    /// carry an earlier stamp.
    fn reset_if_exhausted(&mut self) {
        if self.stamps.exhausted() && self.is_empty() {
            self.stamps.reset();
            self.index.reset_since();
        }
    }

    /// Free one item; `false` when there is none.
    fn step_one(&mut self) -> bool {
        use super::mysql_cache_index::Sweep;
        if let Some((value, _)) = self.ring.pop() {
            drop(value);
            self.meters.reclaimed[1].increment(1);
            return true;
        }
        let entries = &self.entries;
        match self
            .index
            .sweep(|k| entries.peek(k).expect("an indexed key is cached").stamp)
        {
            Sweep::Stale(key, _) => {
                let (k, e) = self.entries.pop_entry(&key).expect("swept");
                self.detach(&k, e, Fate::Step);
                return true;
            }
            Sweep::Checked => return true,
            Sweep::Idle => {}
        }
        if self.stale_at_end() {
            let (k, e) = self.entries.pop_lru().expect("stale");
            self.detach(&k, e, Fate::Step);
            return true;
        }
        if self.emptied() {
            self.entries = lru::LruCache::unbounded();
            self.peak = 0;
            self.barriers = Default::default();
            self.stale_streams = Default::default();
            return true;
        }
        false
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
        let mut caches = Caches::with_bytes(u64::MAX / 2, Meters::noop(), None);
        probe_fill(&mut caches, mix);
        Self { caches }
    }

    /// The bytes the cache estimates it holds.
    pub fn estimate(&self) -> u64 {
        self.caches.bytes()
    }

    /// A barrier appended to each of the first `n` databases' streams,
    /// and what it made stale reclaimed.
    pub fn invalidate_databases(&mut self, n: usize) {
        for db in 0..n {
            self.caches.barrier_appended(&probe_key(db, 0).0);
        }
        self.settle();
    }

    /// Evict the least recently used entries until the estimate is at most
    /// `bytes` (as admission does under a lower bound, with steps between
    /// when deferred values fill the ring).
    pub fn evict_to(&mut self, bytes: u64) {
        while self.caches.bytes() > bytes {
            self.caches.inline_left = INLINE_BYTES;
            match self.caches.evict_one() {
                Evicted::One => {}
                Evicted::Full => self.settle(),
                Evicted::Nothing => break,
            }
        }
        self.settle();
    }

    /// Everything invalidated (in place: the probe has no thread) and
    /// reclaimed.
    pub fn invalidate_all(&mut self) {
        self.caches.invalidate_all();
        self.settle();
    }

    /// Step until nothing is left to reclaim.
    fn settle(&mut self) {
        while self.caches.step_one() {}
        self.caches.publish();
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
            // Full clears go to a thread this test drives.
            let (thread, units) = std::sync::mpsc::sync_channel(2);
            caches.reclaim.thread = Some(thread);
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
            // A barrier over every entry: handed off, charged until freed.
            let held = caches.bytes();
            caches.barrier_appended("lineage/x");
            check(&caches);
            assert_eq!(gauge(METRIC_HANDED_BYTES).1, held as f64);
            reclaim(units.try_recv().unwrap());
            caches.step();
            check(&caches);
            assert_eq!(gauge(METRIC_BYTES).1, 0.0);
            caches.put_timeline("lineage/y/t".into(), timeline.clone());
            check(&caches);
            caches.invalidate_all();
            check(&caches);
            reclaim(units.try_recv().unwrap());
            caches.step();
            check(&caches);
            assert_eq!(gauge(METRIC_BYTES).1, 0.0);
            assert_eq!(gauge(METRIC_HANDED_BYTES).1, 0.0);
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
        // Stale at once, freed by the steps.
        assert!(caches.timeline(&key.backend_key()).is_none());
        assert_eq!(caches.len(), 6);
        caches.settle();
        assert_eq!(caches.len(), 4);
        assert!(caches.timeline("a").is_some());
        caches.audit();
        caches.invalidate_all();
        assert!(caches.timeline("a").is_none());
        caches.settle();
        assert_eq!((caches.len(), caches.bytes()), (0, 0));
        caches.audit();
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
                // Each a loop iteration: its step frees what was evicted.
                caches.step();
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
            // A barrier over every entry clears the cache: the stream goes
            // with its last timeline, and the maps' buckets are released.
            caches.barrier_appended("lineage/");
            assert!(caches.timeline("lineage/d/t19").is_none());
            caches.settle();
            assert_eq!((caches.timelines(), caches.bytes()), (0, 0));
            caches.audit();
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
            ctx.selection.settle();
            assert_eq!(ctx.selection.bytes(), 0);
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
        /// invalidations are counted until a full clear (or a barrier over
        /// every entry) releases them.
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
            c.settle();
            assert_eq!(c.timelines(), 1_000);
            assert!(
                c.bytes() > c.entries_bytes_for_test(),
                "the emptied buckets are counted: {} <= {}",
                c.bytes(),
                c.entries_bytes_for_test()
            );
            c.audit();
            c.invalidate_all();
            c.settle();
            assert_eq!(c.bytes(), 0);
            c.audit();
        }

        /// A barrier that applies to every cached entry is a full clear:
        /// handed to the reclamation thread, or, without one, made stale in
        /// place and swept by steps. Either way nothing is current at once,
        /// the cache counts nothing once it is reclaimed, and memory is
        /// back to exactly what it was before the entries were cached.
        #[test]
        fn a_barrier_over_every_entry_releases_them_and_the_maps() {
            let selection = loaded(4);
            for thread in [false, true] {
                let (tx, units) = std::sync::mpsc::sync_channel(2);
                let mut c = if thread {
                    Caches::with_thread(tx)
                } else {
                    Caches::default()
                };
                let before = crate::test_alloc::held();
                {
                    let mut read = HashMap::new();
                    for t in 0..2_000 {
                        let k = key(t % 20, t);
                        let bk = k.backend_key();
                        c.put_timeline(
                            bk.clone(),
                            timeline_of(&k, 300, &mut read),
                        );
                        c.put_selection(
                            (bk.clone(), "d".into(), "sig".into()),
                            selection.clone(),
                        );
                        c.put_version(bk, version(1, 4));
                    }
                }
                c.barrier_appended(&lineage());
                assert!(c.timeline(&key(3, 3).backend_key()).is_none());
                assert!(c.barrier_streams().is_empty());
                if thread {
                    assert_eq!(c.len(), 0, "handed off");
                    assert!(c.bytes() > 0, "charged until freed");
                    reclaim(units.try_recv().unwrap());
                }
                c.settle();
                assert_eq!((c.len(), c.timelines(), c.bytes()), (0, 0, 0));
                c.audit();
                assert_eq!(crate::test_alloc::held(), before, "{thread}");
            }
        }

        /// A barrier over an empty cache leaves it empty. Entries all
        /// invalidated are stale at once, swept by steps, and once none is
        /// left a step releases the buckets they left.
        #[test]
        fn a_barrier_over_an_empty_cache_releases_its_buckets() {
            let mut c = Caches::default();
            c.barrier_appended(&lineage());
            assert_eq!((c.len(), c.bytes()), (0, 0));
            c.audit();
            let mut read = HashMap::new();
            let keys: Vec<SchemaKey> = (0..500).map(|t| key(0, t)).collect();
            for k in &keys {
                c.put_timeline(k.backend_key(), timeline_of(k, 300, &mut read));
            }
            for k in &keys {
                c.invalidate(k);
            }
            assert!(
                keys.iter().all(|k| c.timeline(&k.backend_key()).is_none())
            );
            assert_eq!(c.len(), 500, "stale until swept");
            c.audit();
            c.settle();
            assert_eq!((c.len(), c.bytes()), (0, 0));
            c.barrier_appended(&database(1));
            assert_eq!(c.bytes(), 0);
            c.audit();
        }

        /// Selections and versions held without their tables' timelines go
        /// with a barrier over them and with a full clear; another
        /// lineage's stay until a barrier over theirs.
        #[test]
        fn selections_and_versions_without_timelines_are_invalidated() {
            let selection = loaded(4);
            let other = SchemaKey::new(
                "acme",
                "s",
                "fedcba9876543210fedcba9876543210",
                "db0",
                "t0",
            );
            let sel =
                |k: &SchemaKey| (k.backend_key(), "d".into(), "sig".into());
            let put = |c: &mut Caches, k: &SchemaKey| {
                c.put_selection(sel(k), selection.clone());
                c.put_version(k.backend_key(), version(1, 4));
            };
            let mut c = Caches::default();
            for t in 0..100 {
                put(&mut c, &key(t % 3, t));
            }
            put(&mut c, &other);
            c.barrier_appended(&lineage());
            assert!(c.selection(&sel(&key(1, 1))).is_none());
            c.settle();
            assert_eq!(c.len(), 2, "the other lineage's selection and version");
            assert!(c.selection(&sel(&other)).is_some());
            assert!(c.version(&other.backend_key(), 1).is_some());
            assert!(c.selection(&sel(&key(0, 0))).is_none());
            assert!(c.version(&key(0, 0).backend_key(), 1).is_none());
            c.audit();
            for t in 0..100 {
                put(&mut c, &key(t % 3, t));
            }
            c.barrier_appended(&barrier_stream(&other, &BarrierScope::Lineage));
            assert!(c.selection(&sel(&other)).is_none());
            c.settle();
            assert_eq!(c.len(), 200);
            c.audit();
            c.barrier_appended(&lineage());
            c.settle();
            assert_eq!((c.len(), c.timelines(), c.bytes()), (0, 0, 0));
            c.audit();
            for t in 0..100 {
                put(&mut c, &key(t % 3, t));
            }
            c.invalidate_all();
            assert!(c.version(&key(1, 1).backend_key(), 1).is_none());
            c.settle();
            assert_eq!((c.len(), c.bytes()), (0, 0));
            c.audit();
        }

        /// A barrier over one lineage of a cache holding two keeps every
        /// entry of the other, with the barrier streams its timelines
        /// share; the first lineage's shared streams go with its last
        /// timeline.
        #[test]
        fn a_lineage_barrier_keeps_another_lineages_entries() {
            let selection = loaded(4);
            let other = SchemaKey::new(
                "acme",
                "s",
                "fedcba9876543210fedcba9876543210",
                "db0",
                "t0",
            );
            let mut c = Caches::default();
            let mut read = HashMap::new();
            for k in (0..200).map(|t| key(t % 4, t)).chain([other.clone()]) {
                let bk = k.backend_key();
                c.put_timeline(bk.clone(), timeline_of(&k, 300, &mut read));
                c.put_selection(
                    (bk.clone(), "d".into(), "sig".into()),
                    selection.clone(),
                );
                c.put_version(bk, version(1, 4));
            }
            assert_eq!(c.barrier_streams().len(), 4 + 1 + 2);
            c.barrier_appended(&lineage());
            // Only the other lineage's streams may be reused at once.
            assert_eq!(c.barrier_streams().len(), 2 + 4);
            assert!(!c.barrier_streams().contains_key(&lineage()));
            c.audit();
            c.settle();
            assert_eq!((c.len(), c.timelines()), (3, 1));
            assert!(c.timeline(&other.backend_key()).is_some());
            let mut held: Vec<String> =
                c.barrier_streams().into_keys().collect();
            held.sort();
            let mut want =
                crate::mysql::mysql_activation::barrier_streams(&other);
            want.sort();
            assert_eq!(held, want);
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
            assert!(c.timeline(&key(7, 3).backend_key()).is_none());
            assert!(c.timeline(&key(8, 3).backend_key()).is_some());
            c.audit();
            let t0 = Instant::now();
            c.settle();
            let one_db_swept = t0.elapsed();
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
            let t0 = Instant::now();
            c.settle();
            let all_swept = t0.elapsed();
            assert_eq!((c.timelines(), c.len()), (0, 0));
            assert!(c.barrier_streams().is_empty());
            c.audit();

            read.clear();
            fill(&mut c, &mut read, 10);
            let t0 = Instant::now();
            c.invalidate_all();
            let cleared = t0.elapsed();
            let t0 = Instant::now();
            c.settle();
            let cleared_swept = t0.elapsed();
            assert_eq!((c.timelines(), c.len(), c.bytes()), (0, 0, 0));
            c.audit();
            eprintln!(
                "{} entries: filled in {filled:?}; a database barrier \
                 {one_db:?} (swept {one_db_swept:?}), a lineage barrier \
                 {all:?} (swept {all_swept:?}), a full clear of {} entries \
                 {cleared:?} (swept {cleared_swept:?})",
                3 * DBS * PER,
                3 * 10 * PER
            );
            // A linear pass: far below this even unoptimized.
            for d in
                [one_db, all, cleared, one_db_swept, all_swept, cleared_swept]
            {
                assert!(d < Duration::from_secs(30), "{d:?}");
            }
        }

        /// This thread's CPU time (Linux: `/proc/thread-self/schedstat`).
        fn thread_cpu() -> Duration {
            let s = std::fs::read_to_string("/proc/thread-self/schedstat")
                .expect("schedstat");
            Duration::from_nanos(
                s.split_whitespace().next().unwrap().parse().unwrap(),
            )
        }

        /// Median, max and count of `t`.
        fn summary(mut t: Vec<Duration>) -> String {
            if t.is_empty() {
                return "none".into();
            }
            t.sort();
            format!(
                "median {:?}, max {:?}, n {}",
                t[t.len() / 2],
                t[t.len() - 1],
                t.len()
            )
        }

        /// Steps as the source's loop runs them, until nothing is pending:
        /// each step's time.
        fn run_steps(c: &mut Caches) -> Vec<Duration> {
            let mut v = Vec::new();
            while c.pending() {
                let t0 = Instant::now();
                c.step();
                v.push(t0.elapsed());
            }
            v
        }

        /// Each item reclaimed, one at a time, until none is left: its
        /// time.
        fn run_items(c: &mut Caches) -> Vec<Duration> {
            let mut v = Vec::new();
            loop {
                let t0 = Instant::now();
                if !c.step_one() {
                    return v;
                }
                v.push(t0.elapsed());
            }
        }

        /// Where the timing populations put table `t`: over 100 databases
        /// evenly, or 90% of them in database 0 and the rest over the
        /// other 99.
        #[derive(Clone, Copy, PartialEq)]
        enum Spread {
            Even,
            Concentrated,
        }

        const DBS: usize = 100;

        fn placed(spread: Spread, t: usize) -> SchemaKey {
            match spread {
                Spread::Even => key(t % DBS, t / DBS),
                Spread::Concentrated if t % 10 < 9 => key(0, t),
                Spread::Concentrated => key(1 + t % (DBS - 1), t),
            }
        }

        /// The smallest real tables (a one-record timeline, a one-column
        /// selection and version), `tables` of them.
        fn fill_small(
            c: &mut Caches,
            spread: Spread,
            tables: std::ops::Range<usize>,
        ) {
            let selection = loaded(1);
            let mut read = HashMap::new();
            for t in tables {
                let k = placed(spread, t);
                let bk = k.backend_key();
                c.put_timeline(bk.clone(), timeline_of(&k, 64, &mut read));
                c.put_selection(
                    (bk.clone(), "d".into(), "sig".into()),
                    selection.clone(),
                );
                c.put_version(bk, version(1, 1));
            }
        }

        /// The tables that fill the default bound (with a tenth more
        /// offered, so it is full).
        fn tables_at_the_bound(spread: Spread) -> usize {
            let mut c = Caches::default();
            fill_small(&mut c, spread, 0..1);
            let per = c.entries_bytes_for_test().max(1);
            (SELECTION_CACHE_BYTES / per) as usize * 11 / 10
        }

        /// `c` filled to the bound, its LRU order as cached or reshuffled
        /// by `seed` (every entry used once in a random order).
        fn fill_full(c: &mut Caches, spread: Spread, seed: Option<u64>) {
            assert_eq!(c.max_bytes, SELECTION_CACHE_BYTES);
            fill_small(
                c,
                spread,
                1_000_000..1_000_000 + tables_at_the_bound(spread),
            );
            if let Some(seed) = seed {
                use rand::{SeedableRng, seq::SliceRandom};
                let mut keys: Vec<CacheKey> =
                    c.entries.iter().map(|(k, _)| k.clone()).collect();
                keys.shuffle(&mut rand::rngs::StdRng::seed_from_u64(seed));
                for k in &keys {
                    c.entries.get(k);
                }
            }
        }

        /// What invalidation costs at the default bound filled with the
        /// most entries it can hold, on the source's loop and as it is
        /// reclaimed (`docs/design/selection-cache-diagnosis.md`, sections
        /// 6.5 and 8). Populations: tables over 100 databases, in the order
        /// cached and reshuffled by five fixed seeds; and 90% of them in
        /// one database (in the order cached, and one seed). Operations: a
        /// database barrier (a small database, and in the concentrated
        /// population the 90% one), a lineage barrier and a full clear (both
        /// full clears), each five times on a full cache. A full clear in
        /// three modes, each made certain: the thread available (a thread
        /// this test runs takes the unit, timing its wall and CPU), the
        /// thread busy with this cache's earlier unit (in place), and
        /// repeated clears in place during a refill. Reports the time on
        /// the loop, the steps that reclaim (median, max, those over the 1
        /// ms target, how far past it each ended, their total) and the
        /// thread's time. A measurement for
        /// a release build: `cargo test --release -p sources --lib --
        /// --ignored --nocapture invalidation_cost_at_the_default_bound`.
        #[test]
        #[ignore = "timing: run in a release build"]
        fn invalidation_cost_at_the_default_bound() {
            let populations =
                std::iter::once((
                    "even, insertion order".to_string(),
                    Spread::Even,
                    None,
                ))
                .chain((1..=5).map(|s| {
                    (format!("even, seed {s}"), Spread::Even, Some(s))
                }))
                .chain([
                    (
                        "concentrated, insertion order".to_string(),
                        Spread::Concentrated,
                        None,
                    ),
                    (
                        "concentrated, seed 1".to_string(),
                        Spread::Concentrated,
                        Some(1),
                    ),
                ]);
            let report =
                |population: &str,
                 held: usize,
                 name: &str,
                 on_loop: Vec<Duration>,
                 steps: Vec<Duration>,
                 thread: Vec<(Duration, Duration)>| {
                    // A step ends once past its target: how far past.
                    let over: Vec<Duration> =
                        steps.iter().map(|d| d.saturating_sub(STEP)).collect();
                    let total: Duration = steps.iter().sum();
                    let runs = format!("{on_loop:?}");
                    eprintln!(
                        "{held} entries at {SELECTION_CACHE_BYTES} bytes, \
                     {population}: {name}: on the loop {}, runs in order \
                     {runs}; steps {}, past {STEP:?} by {}, total {total:?}; \
                     thread wall {}, cpu {}",
                        summary(on_loop),
                        summary(steps),
                        summary(over),
                        summary(thread.iter().map(|t| t.0).collect()),
                        summary(thread.iter().map(|t| t.1).collect()),
                    );
                };
            for (population, spread, seed) in populations {
                let mut held = 0;
                // Database barriers: always in place.
                let mut barriers =
                    vec![("database barrier, a small database", 7)];
                if spread == Spread::Concentrated {
                    barriers.push(("database barrier, the 90% database", 0));
                }
                for (name, db) in barriers {
                    let (mut on_loop, mut steps) = (vec![], vec![]);
                    for _ in 0..5 {
                        let mut c = Caches::default();
                        fill_full(&mut c, spread, seed);
                        held = c.len();
                        let t0 = Instant::now();
                        c.barrier_appended(&database(db));
                        on_loop.push(t0.elapsed());
                        assert_eq!(c.len(), held, "in place");
                        steps.extend(run_steps(&mut c));
                        assert!(c.len() < held);
                        c.audit();
                    }
                    report(&population, held, name, on_loop, steps, vec![]);
                }
                type Op = (&'static str, fn(&mut Caches));
                let ops: [Op; 2] = [
                    ("lineage barrier", |c| c.barrier_appended(&lineage())),
                    ("full clear", |c| c.invalidate_all()),
                ];
                for (op, run) in ops {
                    // The thread available.
                    let (mut on_loop, mut steps, mut thread) =
                        (vec![], vec![], vec![]);
                    for _ in 0..5 {
                        let (tx, rx) = std::sync::mpsc::sync_channel::<Job>(2);
                        let mut c = Caches::with_thread(tx);
                        fill_full(&mut c, spread, seed);
                        let reclaimer = std::thread::spawn(move || {
                            let job = rx.recv().expect("a unit");
                            let (w0, c0) = (Instant::now(), thread_cpu());
                            reclaim(job);
                            (w0.elapsed(), thread_cpu() - c0)
                        });
                        let t0 = Instant::now();
                        run(&mut c);
                        on_loop.push(t0.elapsed());
                        assert_eq!(c.len(), 0, "handed off");
                        thread.push(reclaimer.join().expect("reclaimed"));
                        assert_eq!(c.handed(), 0);
                        steps.extend(run_steps(&mut c));
                        assert_eq!(c.bytes(), 0);
                    }
                    report(
                        &population,
                        held,
                        &format!("{op} [thread available]"),
                        on_loop,
                        steps,
                        thread,
                    );
                    // The thread busy with this cache's earlier unit: in
                    // place; then repeated clears during a refill.
                    for repeated in [false, true] {
                        let (mut on_loop, mut steps) = (vec![], vec![]);
                        for _ in 0..5 {
                            let (tx, rx) =
                                std::sync::mpsc::sync_channel::<Job>(2);
                            let mut c = Caches::with_thread(tx);
                            fill_small(&mut c, spread, 0..1);
                            c.invalidate_all();
                            assert!(c.handed() > 0, "the earlier unit");
                            fill_full(&mut c, spread, seed);
                            let len = c.len();
                            let t0 = Instant::now();
                            run(&mut c);
                            on_loop.push(t0.elapsed());
                            assert_eq!(c.len(), len, "in place");
                            if repeated {
                                let tenth = len / 30;
                                for round in 0..5 {
                                    for _ in 0..50 {
                                        let t0 = Instant::now();
                                        c.step();
                                        steps.push(t0.elapsed());
                                    }
                                    let at = 2_000_000 + round * tenth;
                                    fill_small(&mut c, spread, at..at + tenth);
                                    assert!(c.bytes() <= c.max_bytes);
                                    let t0 = Instant::now();
                                    run(&mut c);
                                    on_loop.push(t0.elapsed());
                                    assert!(c.handed() > 0, "still in place");
                                }
                            }
                            steps.extend(run_steps(&mut c));
                            assert_eq!(c.len(), 0);
                            reclaim(rx.recv().expect("the earlier unit"));
                            c.step();
                            assert_eq!(c.bytes(), 0);
                            c.audit();
                        }
                        let mode = if repeated {
                            "repeated in place during a refill"
                        } else {
                            "thread busy: in place"
                        };
                        report(
                            &population,
                            held,
                            &format!("{op} [{mode}]"),
                            on_loop,
                            steps,
                            vec![],
                        );
                    }
                }
            }
        }

        /// A timeline whose records really hold about `bytes` (a 1 KiB
        /// capture id each).
        fn populated(bytes: usize) -> Arc<Timeline> {
            let n = bytes / 1_100;
            let records: Vec<Stored> = (0..n)
                .map(|i| Stored {
                    capture_id: format!("{i:01024}"),
                    record: Record::new(
                        WmPos::MysqlGtid {
                            gtid_set: format!("u:1-{i}"),
                        },
                        Kind::Barrier {
                            scope: BarrierScope::Lineage,
                        },
                    ),
                })
                .collect();
            Arc::new(Timeline {
                records,
                table_head: 1,
                streams: Vec::new(),
                bytes: (n * 1_024) as u64,
            })
        }

        /// What the largest single items cost, on the loop and as a step
        /// frees each (a step overruns by at most one item): a 64 MiB
        /// timeline whose records really hold that much, replaced and
        /// evicted (deferred, freed by a step); the same value refused by
        /// admission, dropped synchronously by its caller (outside the
        /// cache's retirement, measured separately); the widest version
        /// (4,096 columns); a 50,000-key history swept; and the LRU's
        /// bucket array at the default bound released after a full clear
        /// in place. Five runs each; a measurement for a release build
        /// (`cargo test --release -p sources --lib -- --ignored
        /// --nocapture reclamation_items_at_the_default_bound`).
        #[test]
        #[ignore = "timing: run in a release build"]
        fn reclamation_items_at_the_default_bound() {
            const BIG: usize = 64 << 20;
            let mut rows: Vec<(&str, Vec<Duration>)> = vec![
                ("64 MiB timeline: replaced, on the loop", vec![]),
                ("64 MiB timeline: replaced, the step freeing it", vec![]),
                ("64 MiB timeline: evicted, on the loop", vec![]),
                ("64 MiB timeline: evicted, the step freeing it", vec![]),
                ("64 MiB timeline: refused, the put", vec![]),
                ("64 MiB timeline: refused, dropped by its caller", vec![]),
                (
                    "widest version (4,096 columns): the step freeing it",
                    vec![],
                ),
                ("50,000-key history: largest item swept", vec![]),
                ("50,000-key history: all its steps", vec![]),
                ("full bound, cleared in place: largest item", vec![]),
                ("full bound, cleared in place: the LRU's buckets", vec![]),
            ];
            let (a, b) = (key(0, 1).backend_key(), key(0, 2).backend_key());
            for _ in 0..5 {
                let mut c = Caches::default();
                c.put_timeline(a.clone(), populated(BIG));
                let next = populated(BIG);
                let t0 = Instant::now();
                assert!(c.put_timeline(a.clone(), next).is_some());
                rows[0].1.push(t0.elapsed());
                assert_eq!(c.ring.len, 1);
                let t0 = Instant::now();
                c.step();
                rows[1].1.push(t0.elapsed());
                assert_eq!(c.ring.len, 0);

                c.set_limit_for_test(c.bytes() + c.bytes() / 2);
                // The caller keeps the refused value (dropping it is
                // measured below).
                let next = populated(BIG);
                let t0 = Instant::now();
                assert!(c.put_timeline(b.clone(), next.clone()).is_none());
                rows[2].1.push(t0.elapsed());
                drop(next);
                assert_eq!(c.ring.len, 1, "evicted and deferred");
                let t0 = Instant::now();
                c.step();
                rows[3].1.push(t0.elapsed());

                let mut c = Caches::default();
                c.set_limit_for_test(1 << 20);
                let value = populated(BIG);
                let t0 = Instant::now();
                assert!(c.put_timeline(a.clone(), value.clone()).is_none());
                rows[4].1.push(t0.elapsed());
                let t0 = Instant::now();
                drop(value);
                rows[5].1.push(t0.elapsed());

                let mut c = Caches::default();
                c.put_version(a.clone(), version(1, 4_096));
                c.invalidate(&key(0, 1));
                let t0 = Instant::now();
                c.step();
                rows[6].1.push(t0.elapsed());
                assert_eq!(c.len(), 0);

                let mut c = Caches::default();
                for v in 0..50_000 {
                    c.put_version(a.clone(), version(v, 1));
                }
                assert_eq!(c.len(), 50_000);
                c.invalidate(&key(0, 1));
                let items = run_items(&mut c);
                rows[7].1.push(*items.iter().max().unwrap());
                c.put_version(b.clone(), version(1, 1));
                for v in 0..50_000 {
                    c.put_version(a.clone(), version(v, 1));
                }
                c.invalidate(&key(0, 1));
                rows[8].1.extend(run_steps(&mut c));

                let mut c = Caches::default();
                fill_full(&mut c, Spread::Even, None);
                c.invalidate_all();
                let items = run_items(&mut c);
                rows[9].1.push(*items.iter().max().unwrap());
                rows[10].1.push(*items.last().unwrap());
                assert_eq!(c.bytes(), 0);
            }
            for (name, t) in rows {
                let runs = format!("{t:?}");
                eprintln!(
                    "reclamation item: {name}: {}; runs {runs}",
                    summary(t)
                );
            }
        }

        /// What keeping the cache costs per operation at the default bound
        /// (index upkeep included): filling it with small tables, admitting
        /// tables that evict, invalidating single tables, and a table with
        /// a long history (20,000 versions) cached, evicted one version at
        /// a time and invalidated. Five runs each; a measurement for a
        /// release build (`cargo test --release -p sources --lib --
        /// --ignored --nocapture upkeep_at_the_default_bound`).
        #[test]
        #[ignore = "timing: run in a release build"]
        fn upkeep_at_the_default_bound() {
            const DBS: usize = 100;
            let selection = loaded(1);
            let put = |c: &mut Caches, t: usize, v: i32| {
                let k = key(t % DBS, t / DBS);
                let bk = k.backend_key();
                let mut read = HashMap::new();
                c.put_timeline(bk.clone(), timeline_of(&k, 64, &mut read));
                c.put_selection(
                    (bk.clone(), "d".into(), "sig".into()),
                    selection.clone(),
                );
                c.put_version(bk, version(v, 1));
            };
            let per = |d: Duration, n: usize| {
                Duration::from_nanos((d.as_nanos() / n as u128) as u64)
            };
            let mut times: Vec<(&str, Vec<Duration>)> = vec![
                ("fill, per table (3 entries)", vec![]),
                ("admit evicting, per table", vec![]),
                ("invalidate a table", vec![]),
                ("long history: cache, per version", vec![]),
                ("long history: evict, per version", vec![]),
                ("long history: invalidate the table", vec![]),
                ("long history: reclaimed", vec![]),
                ("lookup hit, per lookup", vec![]),
            ];
            let tables = 60_000;
            for _ in 0..5 {
                let mut c = Caches::default();
                let t0 = Instant::now();
                for t in 0..tables {
                    put(&mut c, t, 1);
                }
                times[0].1.push(per(t0.elapsed(), tables));
                let t0 = Instant::now();
                for t in tables..tables + 20_000 {
                    put(&mut c, t, 1);
                }
                times[1].1.push(per(t0.elapsed(), 20_000));
                let t0 = Instant::now();
                for t in (tables..tables + 20_000).step_by(2) {
                    c.invalidate(&key(t % DBS, t / DBS));
                }
                times[2].1.push(per(t0.elapsed(), 10_000));
                // Batch sizing over every held table (stale ones too)
                // against the current ones alone: batching only.
                let (mut n, mut b) = (0u64, 0u64);
                for (k, e) in c.entries.iter() {
                    if c.current(k, e)
                        && !matches!(e.cached, Cached::Selection(_))
                    {
                        b += e.bytes;
                        n += matches!(e.cached, Cached::Timeline(..)) as u64;
                    }
                }
                let live = ((c.max_bytes / 2 / (b / n).max(1)) as usize)
                    .clamp(1, crate::mysql::mysql_history::BATCH_MAX_TABLES);
                eprintln!(
                    "upkeep: batch room {} counting stale tables, {live} \
                     current ones alone",
                    c.batch_room()
                );
                // The currency checks on the read path.
                let hits: Vec<String> = (tables + 1..tables + 20_000)
                    .step_by(2)
                    .map(|t| key(t % DBS, t / DBS).backend_key())
                    .collect();
                let t0 = Instant::now();
                for k in &hits {
                    assert!(c.timeline(k).is_some());
                }
                times[7].1.push(per(t0.elapsed(), hits.len()));

                let long = key(0, 1_000_000);
                let bk = long.backend_key();
                let mut c = Caches::default();
                let t0 = Instant::now();
                for v in 0..20_000 {
                    c.put_version(bk.clone(), version(v, 1));
                }
                times[3].1.push(per(t0.elapsed(), 20_000));
                let held = c.bytes();
                c.set_limit_for_test(held);
                let t0 = Instant::now();
                for v in 20_000..30_000 {
                    c.put_version(bk.clone(), version(v, 1));
                }
                times[4].1.push(per(t0.elapsed(), 10_000));
                let t0 = Instant::now();
                c.invalidate(&long);
                times[5].1.push(t0.elapsed());
                let t0 = Instant::now();
                c.settle();
                times[6].1.push(t0.elapsed());
                assert_eq!(c.held_for(&long), 0);
            }
            for (name, t) in &mut times {
                let runs = format!("{t:?}");
                t.sort();
                eprintln!(
                    "upkeep: {name} median {:?}, max {:?}, runs in order \
                     {runs}",
                    t[t.len() / 2],
                    t[t.len() - 1]
                );
            }
        }

        /// A timeline that allocates about `bytes` (its records' vector,
        /// reserved), without barrier streams.
        fn heavy(bytes: usize) -> Arc<Timeline> {
            Arc::new(Timeline {
                records: Vec::with_capacity(
                    bytes / std::mem::size_of::<Stored>(),
                ),
                table_head: 1,
                streams: Vec::new(),
                bytes: 0,
            })
        }

        /// The allocator's bytes held by this thread.
        fn held() -> i64 {
            crate::test_alloc::held()
        }

        /// A stale entry is a miss for every reader (timeline, selection,
        /// version, proven flags) and is neither returned nor freed by
        /// the read: its memory stays allocated, and charged, until a
        /// step sweeps it.
        #[test]
        fn a_stale_entry_is_a_miss_and_nothing_is_freed_with_it() {
            let mut c = Caches::default();
            let k = key(0, 1);
            let bk = k.backend_key();
            let sel = (bk.clone(), "d".to_string(), "sig".to_string());
            c.put_timeline(bk.clone(), heavy(1 << 20));
            c.put_selection(sel.clone(), loaded(4));
            c.put_version(bk.clone(), version(1, 4));
            c.mark_proven(&k);
            assert!(c.is_proven(&k) && c.proven_keys().contains(&bk));
            c.invalidate(&k);
            let (before, charged) = (held(), c.bytes());
            assert!(c.timeline(&bk).is_none());
            assert!(c.selection(&sel).is_none());
            assert!(c.version(&bk, 1).is_none());
            assert!(!c.is_proven(&k) && c.proven_keys().is_empty());
            c.mark_proven(&k);
            assert_eq!((held(), c.bytes(), c.len()), (before, charged, 3));
            assert_eq!(c.index.live(), 0);
            c.audit();
            c.settle();
            assert!(held() < before - (1 << 20), "freed by the sweep");
            assert_eq!((c.len(), c.bytes()), (0, 0));
        }

        /// Replacing or evicting a 64 MiB timeline frees nothing in the
        /// operation: the value waits, charged, for the next step. An
        /// admission that needs that room is refused until the step has
        /// freed it (counted as `deferred`), and evicts nothing more.
        #[test]
        fn replacement_and_eviction_never_free_a_large_value_inline() {
            const BIG: usize = 64 << 20;
            let mut c = Caches::default();
            let a = key(0, 1).backend_key();
            let b = key(0, 2).backend_key();
            c.put_timeline(a.clone(), heavy(BIG));
            let one = c.bytes();
            let before = held();
            // Replacement: the old value is deferred.
            assert!(c.put_timeline(a.clone(), heavy(BIG)).is_some());
            assert!(held() >= before + BIG as i64, "nothing freed inline");
            assert_eq!(c.ring.len, 1);
            assert!(c.bytes() >= 2 * one - 4096 && c.bytes() <= c.max_bytes);
            c.audit();
            let t0 = Instant::now();
            c.step();
            eprintln!("a step freeing a 64 MiB timeline: {:?}", t0.elapsed());
            assert_eq!(c.ring.len, 0);
            assert!(held() < before + 4096, "freed by the step");
            // Eviction: room for one, so caching B evicts A, which waits.
            c.set_limit_for_test(one + one / 2);
            let before = held();
            assert!(c.put_timeline(b.clone(), heavy(BIG)).is_none());
            assert!(c.timeline(&a).is_none() && c.timeline(&b).is_none());
            assert_eq!(c.ring.len, 1, "A evicted and deferred");
            assert!(held() >= before - 4096, "nothing freed inline");
            c.audit();
            // Before the step: refused again, evicting nothing more.
            assert!(c.put_timeline(b.clone(), heavy(BIG)).is_none());
            assert_eq!(c.ring.len, 1);
            c.step();
            assert!(c.put_timeline(b.clone(), heavy(BIG)).is_some());
            c.audit();
        }

        /// An operation frees small values inline only up to its budget;
        /// past it they are deferred, charged.
        #[test]
        fn an_operation_frees_at_most_its_inline_budget() {
            let mut c = Caches::default();
            let small = loaded(12);
            let each = super::super::super::mysql_cache_cost::loaded(&small);
            for t in 0..400 {
                let bk = key(0, t).backend_key();
                c.put_selection((bk, "d".into(), "sig".into()), small.clone());
            }
            let full = c.bytes();
            // Room for a timeline of 40 such selections' bytes.
            c.set_limit_for_test(full);
            let before = held();
            let reclaimed_before = c.ring.len;
            assert_eq!(reclaimed_before, 0);
            let want = (40 * each) as usize;
            let _ = c.put_timeline(key(1, 1).backend_key(), heavy(want));
            let freed = before - held();
            eprintln!(
                "freed inline {freed} bytes of {each}-byte selections; \
                 deferred {}",
                c.ring.len
            );
            assert!(c.ring.len > 0, "past the budget: deferred");
            assert!(
                freed <= (INLINE_BYTES + c.ring.len as u64 * 512) as i64,
                "{freed}"
            );
            c.audit();
        }

        /// A load, or a batch of installs, is one operation: its puts share
        /// one inline allowance, so many small evictions inside it free at
        /// most that much inline, and the rest wait for a step. The same
        /// puts made separately each have their own.
        #[test]
        fn an_operation_shares_one_inline_allowance() {
            let small = loaded(12);
            for how in ["separate", "load", "batch"] {
                let mut c = Caches::default();
                // Each its own value: evicting one frees it.
                for t in 0..400 {
                    let bk = key(0, t).backend_key();
                    c.put_selection((bk, "d".into(), "sig".into()), loaded(12));
                }
                c.set_limit_for_test(c.bytes());
                let each =
                    super::super::super::mysql_cache_cost::loaded(&small);
                // Each needs about one selection's room (two evictions at
                // most): within one put's own allowance.
                assert!(2 * each < INLINE_BYTES);
                let values: Vec<_> =
                    (0..10).map(|_| heavy(each as usize)).collect();
                let before = c.freed_inline;
                let _load = match how {
                    "load" => Some(c.begin_load()),
                    "batch" => {
                        c.begin_operation();
                        None
                    }
                    _ => None,
                };
                // Ten tables.
                for (t, v) in values.into_iter().enumerate() {
                    let _ = c.put_timeline(key(1, t).backend_key(), v);
                }
                match how {
                    "load" => c.end_load(),
                    "batch" => c.end_operation(),
                    _ => {}
                }
                let freed = c.freed_inline - before;
                eprintln!(
                    "{how}: freed inline {freed}, deferred {}, entries {}",
                    c.ring.len,
                    c.len()
                );
                if how == "separate" {
                    // Each put has its own allowance: far more freed inline.
                    assert_eq!(c.ring.len, 0, "{how}");
                    assert!(freed > 4 * INLINE_BYTES, "{freed}");
                } else {
                    assert!(c.ring.len > 0, "{how}: past the shared allowance");
                    assert!(freed <= INLINE_BYTES, "{how}: {freed}");
                }
                c.audit();
            }
        }

        /// Every capacity eviction is counted, stale entries' too (and
        /// those also by themselves).
        #[test]
        fn stale_evictions_are_counted_with_all_evictions() {
            use metrics_util::debugging::{DebugValue, DebuggingRecorder};
            let rec = DebuggingRecorder::new();
            let left = metrics::with_local_recorder(&rec, || {
                let mut c = Caches::new("p", "s");
                let mut read = HashMap::new();
                for t in 0..10 {
                    let k = key(0, t);
                    c.put_timeline(
                        k.backend_key(),
                        timeline_of(&k, 300, &mut read),
                    );
                }
                c.invalidate(&key(0, 0));
                c.invalidate(&key(0, 1));
                // Room for nine: two puts evict the two stale ones, then a
                // current one.
                c.set_limit_for_test(c.bytes());
                for t in 10..13 {
                    let k = key(0, t);
                    c.put_timeline(
                        k.backend_key(),
                        timeline_of(&k, 300, &mut read),
                    );
                }
                assert!(c.timeline(&key(0, 12).backend_key()).is_some());
                c.len()
            });
            let count = |name: &str| {
                rec.snapshotter()
                    .snapshot()
                    .into_vec()
                    .into_iter()
                    .find(|(k, ..)| k.key().name() == name)
                    .map(|(.., v)| match v {
                        DebugValue::Counter(n) => n,
                        _ => panic!("a counter"),
                    })
                    .unwrap_or(0)
            };
            // Nothing else removed an entry (no step ran).
            assert_eq!(count(METRIC_EVICTIONS), 13 - left as u64);
            assert!(count(METRIC_EVICTIONS) > 2, "a current one too");
            assert_eq!(count(METRIC_STALE_EVICTIONS), 2);
        }

        /// With every deferred slot taken, an operation that would need one
        /// refuses before it detaches anything: the entry it would have
        /// replaced or evicted stays, with its value and accounting, and
        /// one step later it goes ahead.
        #[test]
        fn a_full_ring_leaves_everything_in_place_and_refuses() {
            const BIG: usize = 64 << 10;
            let mut c = Caches::default();
            let keys: Vec<String> =
                (0..DEFERRED + 2).map(|t| key(0, t).backend_key()).collect();
            for k in &keys {
                c.put_timeline(k.clone(), heavy(BIG));
            }
            for k in &keys[..DEFERRED] {
                assert!(c.put_timeline(k.clone(), heavy(BIG)).is_some());
            }
            assert!(c.ring.full());
            let held_at = c.timeline(&keys[DEFERRED]).unwrap();
            let (len, bytes, mem) = (c.len(), c.bytes(), held());
            // Replacement refused: the old entry stays current.
            assert!(
                c.put_timeline(keys[DEFERRED].clone(), heavy(BIG)).is_none()
            );
            assert!(Arc::ptr_eq(
                &c.timeline(&keys[DEFERRED]).unwrap(),
                &held_at
            ));
            assert_eq!((c.len(), c.bytes()), (len, bytes));
            assert!(held() <= mem, "nothing new held");
            c.audit();
            // Eviction refused: nothing is evicted.
            c.set_limit_for_test(bytes);
            let other = key(1, 1).backend_key();
            assert!(c.put_timeline(other.clone(), heavy(BIG)).is_none());
            assert_eq!((c.len(), c.bytes()), (len, bytes));
            assert!(c.timeline(&keys[DEFERRED + 1]).is_some());
            c.audit();
            c.step();
            assert!(c.ring.len < DEFERRED);
            assert!(
                c.put_timeline(keys[DEFERRED].clone(), heavy(BIG)).is_some()
            );
            c.audit();
        }

        /// The cache with `tables` small tables cached (timeline, selection
        /// and version each).
        fn filled(c: &mut Caches, tables: std::ops::Range<usize>) {
            let mut read = HashMap::new();
            for t in tables {
                let k = key(t % 4, t);
                let bk = k.backend_key();
                c.put_timeline(bk.clone(), timeline_of(&k, 300, &mut read));
                c.put_selection(
                    (bk.clone(), "d".into(), "sig".into()),
                    loaded(4),
                );
                c.put_version(bk, version(1, 4));
            }
        }

        /// A full clear hands everything to the thread without waiting or
        /// freeing anything, charged until the thread has freed it. While
        /// that unit is there, clears are made in place: repeated clears
        /// during a refill allocate nothing, stay within the bound, and
        /// leave nothing current.
        #[test]
        fn clears_while_a_unit_is_on_the_thread_are_made_in_place() {
            let (tx, units) = std::sync::mpsc::sync_channel(2);
            let mut c = Caches::with_thread(tx);
            let base = held();
            filled(&mut c, 0..500);
            let (charged, mem) = (c.bytes(), held());
            c.invalidate_all();
            // Nothing freed; the one allocation is the new empty list's two
            // sentinel nodes (uncounted while empty, as in a new cache).
            assert_eq!(
                held() - mem,
                2 * super::super::super::mysql_cache_cost::allocated(NODE)
                    as i64,
                "nothing freed"
            );
            assert_eq!((c.len(), c.bytes(), c.handed()), (0, charged, charged));
            for round in 0..5 {
                filled(&mut c, 0..200);
                assert!(c.bytes() <= c.max_bytes);
                let (len, bytes, mem) = (c.len(), c.bytes(), held());
                c.invalidate_all();
                assert_eq!(held(), mem, "round {round}: nothing allocated");
                assert_eq!((c.len(), c.bytes()), (len, bytes), "in place");
                assert!(c.timeline(&key(1, 1).backend_key()).is_none());
                assert!(c.barrier_streams().is_empty());
                c.audit();
                for _ in 0..50 {
                    c.step();
                }
            }
            assert!(units.try_recv().is_ok_and(|u| {
                reclaim(u);
                true
            }));
            assert!(units.try_recv().is_err(), "one unit per cache");
            assert_eq!(c.handed(), 0);
            c.settle();
            assert_eq!(c.bytes(), 0);
            c.audit();
            assert_eq!(held(), base);
        }

        /// A handoff the queue rejects (full, or the thread gone) leaves
        /// every structure in place, intact and unfreed, and the clear is
        /// made in place; without a thread every clear is.
        #[test]
        fn a_rejected_handoff_keeps_everything_and_clears_in_place() {
            for case in ["full", "gone", "none"] {
                let (tx, units) = std::sync::mpsc::sync_channel(2);
                let mut others: Vec<Caches> = Vec::new();
                let mut c = match case {
                    "none" => Caches::default(),
                    _ => Caches::with_thread(tx.clone()),
                };
                if case == "full" {
                    for _ in 0..2 {
                        let mut o = Caches::with_thread(tx.clone());
                        filled(&mut o, 0..10);
                        o.invalidate_all();
                        assert!(o.handed() > 0);
                        others.push(o);
                    }
                }
                if case == "gone" {
                    drop(units);
                } else {
                    std::mem::forget(units);
                }
                filled(&mut c, 0..300);
                let k = key(1, 1).backend_key();
                let t = c.timeline(&k).unwrap();
                let (len, bytes, mem) = (c.len(), c.bytes(), held());
                c.invalidate_all();
                assert_eq!(held(), mem, "{case}: nothing freed");
                assert_eq!((c.len(), c.bytes(), c.handed()), (len, bytes, 0));
                assert!(c.timeline(&k).is_none(), "{case}: stale");
                let kept = c
                    .entries
                    .peek(&CacheKey::Timeline(k.as_str().into()))
                    .unwrap();
                assert!(
                    matches!(&kept.cached, Cached::Timeline(x, _) if Arc::ptr_eq(x, &t)),
                    "{case}: the same value, back in place"
                );
                c.audit();
                c.settle();
                assert_eq!(c.bytes(), 0, "{case}");
            }
        }

        /// install_if_current installs only over the snapshot as the
        /// table's current timeline: after a database barrier, a lineage
        /// barrier, a full clear (in place or handed off), an invalidation
        /// or an eviction between the snapshot and the install it refuses,
        /// also while the stale entry still holds the snapshot itself.
        #[test]
        fn an_install_needs_its_snapshot_current() {
            for case in [
                "nothing",
                "database barrier",
                "lineage barrier",
                "clear in place",
                "clear handed off",
                "invalidation",
                "eviction",
            ] {
                let (tx, _units) = std::sync::mpsc::sync_channel(2);
                let mut c = Caches::with_thread(tx);
                let mut read = HashMap::new();
                let k = key(1, 1);
                let bk = k.backend_key();
                // Another lineage's table, so a lineage barrier is not a
                // full clear.
                let other = SchemaKey::new(
                    "acme",
                    "s",
                    "fedcba9876543210fedcba9876543210",
                    "db0",
                    "t0",
                );
                c.put_timeline(
                    other.backend_key(),
                    timeline_of(&other, 300, &mut read),
                );
                let snap = c
                    .put_timeline(bk.clone(), timeline_of(&k, 300, &mut read))
                    .unwrap();
                match case {
                    "database barrier" => c.barrier_appended(&database(1)),
                    "lineage barrier" => c.barrier_appended(&lineage()),
                    "clear in place" => {
                        c.reclaim.thread = None;
                        c.invalidate_all();
                    }
                    "clear handed off" => c.invalidate_all(),
                    "invalidation" => c.invalidate(&k),
                    "eviction" => {
                        // The table is the least recently used.
                        c.timeline(&other.backend_key());
                        c.set_limit_for_test(c.bytes());
                        let n = key(2, 2);
                        c.put_timeline(
                            n.backend_key(),
                            timeline_of(&n, 300, &mut read),
                        );
                    }
                    _ => {}
                }
                let still_held = c
                    .entries
                    .peek(&CacheKey::Timeline(bk.as_str().into()))
                    .is_some_and(|e| matches!(&e.cached, Cached::Timeline(t, _) if Arc::ptr_eq(t, &snap)));
                let installed = c.install_if_current(
                    &k,
                    &snap,
                    timeline_of(&k, 400, &mut read),
                );
                assert_eq!(installed, case == "nothing", "{case}");
                if matches!(
                    case,
                    "database barrier"
                        | "lineage barrier"
                        | "clear in place"
                        | "invalidation"
                ) {
                    assert!(
                        still_held,
                        "{case}: the stale entry holds the snapshot"
                    );
                    assert!(c.timeline(&bk).is_none(), "{case}");
                }
                c.audit();
            }
        }

        /// A table's record dropped and created again, or a database's,
        /// never makes a retained snapshot current: the new entries are
        /// new values under new stamps.
        #[test]
        fn a_recreated_record_does_not_revive_a_snapshot() {
            let mut c = Caches::default();
            let mut read = HashMap::new();
            let k = key(1, 1);
            let bk = k.backend_key();
            let snap = c
                .put_timeline(bk.clone(), timeline_of(&k, 300, &mut read))
                .unwrap();
            for scope in [None, Some(database(1))] {
                match &scope {
                    None => c.invalidate(&k),
                    Some(s) => {
                        // Keep another database, so this is no full clear.
                        let o = key(2, 2);
                        c.put_timeline(
                            o.backend_key(),
                            timeline_of(&o, 300, &mut read),
                        );
                        c.barrier_appended(s);
                    }
                }
                c.settle();
                assert_eq!(c.held_for(&k), 0, "{scope:?}: record dropped");
                c.put_timeline(bk.clone(), timeline_of(&k, 300, &mut read));
                assert!(c.timeline(&bk).is_some());
                assert!(!c.install_if_current(
                    &k,
                    &snap,
                    timeline_of(&k, 400, &mut read)
                ));
                c.audit();
            }
        }

        /// Exhausted stamps fail closed: with stale entries, deferred
        /// values and a handed-off unit present, nothing is current or
        /// admitted, and no snapshot installs. Once the cache holds nothing
        /// the stamps start again; the unit is still freed by the thread
        /// and released; no snapshot from before becomes current, though
        /// stamps and records are created again.
        #[test]
        fn exhausted_stamps_fail_closed_until_the_cache_is_empty() {
            for path in ["put", "invalidate", "barrier", "clear"] {
                let (tx, units) = std::sync::mpsc::sync_channel(2);
                let mut c = Caches::with_thread(tx);
                let mut read = HashMap::new();
                let base = held();
                filled(&mut c, 0..100);
                let a = key(1, 1);
                let snap_a = c.timeline(&a.backend_key()).unwrap();
                c.invalidate_all();
                assert!(c.handed() > 0, "{path}: a unit on the thread");
                filled(&mut c, 0..100);
                c.barrier_appended(&database(2));
                c.put_timeline(key(3, 3).backend_key(), heavy(1 << 20));
                c.put_timeline(key(3, 3).backend_key(), heavy(1 << 20));
                assert_eq!(c.ring.len, 1, "{path}: a deferred value");
                let snap_b = c.timeline(&a.backend_key()).unwrap();
                c.set_stamp_for_test(u64::MAX);
                match path {
                    "put" => assert!(
                        c.put_timeline(
                            key(0, 999).backend_key(),
                            timeline_of(&key(0, 999), 300, &mut read)
                        )
                        .is_none()
                    ),
                    "invalidate" => c.invalidate(&key(0, 4)),
                    "barrier" => c.barrier_appended(&database(0)),
                    _ => c.invalidate_all(),
                }
                assert!(c.stamps.exhausted(), "{path}");
                assert!(c.timeline(&a.backend_key()).is_none(), "{path}");
                assert!(
                    c.barrier_streams().is_empty()
                        && c.proven_keys().is_empty()
                );
                assert!(!c.install_if_current(
                    &a,
                    &snap_b,
                    timeline_of(&a, 400, &mut read)
                ));
                assert!(
                    c.put_timeline(
                        a.backend_key(),
                        timeline_of(&a, 300, &mut read)
                    )
                    .is_none(),
                    "{path}: nothing admitted"
                );
                assert_eq!(c.index.live(), 0);
                c.audit();
                c.settle();
                assert!(!c.stamps.exhausted(), "{path}: empty, so reset");
                assert_eq!(c.bytes() - c.handed(), 0);
                // Records and stamps start again: the old snapshots stay
                // dead.
                let fresh = c
                    .put_timeline(
                        a.backend_key(),
                        timeline_of(&a, 300, &mut read),
                    )
                    .unwrap();
                assert!(
                    c.timeline(&a.backend_key())
                        .is_some_and(|t| Arc::ptr_eq(&t, &fresh))
                );
                for snap in [&snap_a, &snap_b] {
                    assert!(!c.install_if_current(
                        &a,
                        snap,
                        timeline_of(&a, 400, &mut read)
                    ));
                }
                c.audit();
                reclaim(units.try_recv().unwrap());
                assert_eq!(c.handed(), 0);
                c.invalidate_all();
                reclaim(units.try_recv().unwrap());
                c.settle();
                assert_eq!(c.bytes(), 0, "{path}");
                // The test's own key and reads of the barrier streams too.
                drop((snap_a, snap_b, fresh, read, a));
                assert_eq!(held(), base, "{path}: everything released");
            }
        }

        /// A barrier cuts its stream: the stream is no longer offered for
        /// reuse, a new read of it is held for the timelines read after,
        /// and the old read stays held, and charged, for the stale
        /// timelines holding it until they go.
        #[test]
        fn a_cut_stream_stays_held_for_its_stale_timelines_only() {
            let mut c = Caches::default();
            let mut read = HashMap::new();
            let (a, b, z) = (key(0, 1), key(0, 2), key(1, 3));
            c.put_timeline(a.backend_key(), timeline_of(&a, 300, &mut read));
            c.put_timeline(z.backend_key(), timeline_of(&z, 300, &mut read));
            let old = read[&database(0)].clone();
            c.barrier_appended(&database(0));
            assert!(!c.barrier_streams().contains_key(&database(0)));
            assert!(c.barrier_streams().contains_key(&database(1)));
            // A new read of the stream, appended to since.
            read.insert(
                database(0),
                Arc::new(BarrierStream {
                    stream: database(0),
                    records: Vec::new(),
                    head: 2,
                    bytes: 256,
                }),
            );
            c.put_timeline(b.backend_key(), timeline_of(&b, 300, &mut read));
            assert_eq!(c.stale_streams.len(), 1);
            assert!(Arc::ptr_eq(&c.stale_streams[&addr(&old)].stream, &old));
            assert!(c.barrier_streams()[&database(0)].head == 2);
            c.audit();
            c.settle();
            assert!(c.stale_streams.is_empty(), "gone with its last holder");
            c.audit();
        }

        /// A step stops near its target with work left, and resumes where
        /// it stopped; a source without work runs none.
        #[test]
        fn a_step_is_bounded_and_resumes() {
            let mut c = Caches::default();
            c.set_limit_for_test(1 << 30);
            filled(&mut c, 0..20_000);
            assert!(!c.pending());
            c.barrier_appended(&database(1));
            let len = c.len();
            let t0 = Instant::now();
            c.step();
            let took = t0.elapsed();
            let after = c.len();
            assert!(after < len && c.pending(), "progress, with work left");
            eprintln!("a step: {took:?}, {} entries freed", len - after);
            while c.pending() {
                c.step();
            }
            assert_eq!(c.len(), 3 * 15_000);
            c.audit();
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
