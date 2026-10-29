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

/// The cache's table bound: at most this many tables' timelines.
pub(crate) const SELECTION_CACHE_ENTRIES: usize = 4096;
/// The cache's byte bound: at most this many estimated bytes of timelines,
/// barrier streams (each once) and selected schemas.
pub(crate) const SELECTION_CACHE_BYTES: u64 = 256 * 1024 * 1024;

/// `deltaforge_mysql_selection_cache_evictions_total{pipeline,source}`.
pub(crate) const METRIC_EVICTIONS: &str =
    "deltaforge_mysql_selection_cache_evictions_total";

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

/// Per-run caches of timelines and selections, bounded together by
/// tables and estimated bytes (`docs/design/mysql-mass-invalidation.md`,
/// section 6): admission evicts the least recently used entries until the
/// new one fits, before inserting it. Barrier streams are held once, shared
/// by the timelines they apply to, and dropped with the last of them. Only
/// this process appends activation records and barriers for this source
/// (the store gate), so invalidating on every append keeps them exact; an
/// evicted entry is rebuilt exactly from the durable records.
pub(crate) struct Caches {
    entries: lru::LruCache<CacheKey, (Cached, u64)>,
    /// Barrier streams, by stream key, with the cached timelines holding
    /// each.
    barriers: std::collections::HashMap<String, (Arc<BarrierStream>, usize)>,
    timelines: usize,
    bytes: u64,
    max_tables: usize,
    max_bytes: u64,
    evictions: metrics::Counter,
}

impl std::fmt::Debug for Caches {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Caches")
            .field("entries", &self.entries.len())
            .field("timelines", &self.timelines)
            .field("bytes", &self.bytes)
            .finish()
    }
}

impl Default for Caches {
    fn default() -> Self {
        Self::with_limits(
            SELECTION_CACHE_ENTRIES,
            SELECTION_CACHE_BYTES,
            metrics::Counter::noop(),
        )
    }
}

impl Caches {
    /// The source's caches, counting evictions under its labels.
    pub(crate) fn new(pipeline: &str, source: &str) -> Self {
        let (tables, bytes) = crate::stream_probe::selection_cache_limits()
            .unwrap_or((SELECTION_CACHE_ENTRIES, SELECTION_CACHE_BYTES));
        Self::with_limits(
            tables,
            bytes,
            metrics::counter!(
                METRIC_EVICTIONS,
                "pipeline" => pipeline.to_string(),
                "source" => source.to_string()
            ),
        )
    }

    fn with_limits(
        max_tables: usize,
        max_bytes: u64,
        evictions: metrics::Counter,
    ) -> Self {
        assert!(max_tables > 0, "a positive table bound");
        Self {
            entries: lru::LruCache::unbounded(),
            barriers: Default::default(),
            timelines: 0,
            bytes: 0,
            max_tables,
            max_bytes,
            evictions,
        }
    }

    /// The estimated bytes held: entries and barrier streams.
    #[cfg(test)]
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
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
        if let Some((cached, bytes)) = self.entries.pop(key) {
            self.forget(cached, bytes);
        }
    }

    fn forget(&mut self, cached: Cached, bytes: u64) {
        self.bytes -= bytes;
        if let Cached::Timeline(t, _) = cached {
            self.timelines -= 1;
            // Its barrier streams go with the last timeline holding them.
            for s in &t.streams {
                if let Some((held, refs)) = self.barriers.get_mut(&s.stream)
                    && Arc::ptr_eq(held, s)
                {
                    *refs -= 1;
                    if *refs == 0 {
                        let gone = held.bytes;
                        self.barriers.remove(&s.stream);
                        self.bytes -= gone;
                    }
                }
            }
        }
    }

    /// Evict the least recently used entry; `false` when there is none.
    fn evict_one(&mut self) -> bool {
        let Some((_, (cached, b))) = self.entries.pop_lru() else {
            return false;
        };
        self.forget(cached, b);
        self.evictions.increment(1);
        true
    }

    /// Insert a selection of `bytes`, evicting until it fits (never above
    /// the bounds; one that cannot fit is not cached).
    fn put(&mut self, key: CacheKey, value: Cached, bytes: u64) {
        self.remove(&key);
        while self.bytes + bytes > self.max_bytes {
            if !self.evict_one() {
                return;
            }
        }
        self.bytes += bytes;
        self.entries.push(key, (value, bytes));
    }

    /// The most tables one batched load brings in: never more than half
    /// the cache, so a batch never evicts what it loaded.
    pub(crate) fn batch_room(&self) -> usize {
        (self.max_tables / 2).clamp(1, super::mysql_history::BATCH_MAX_TABLES)
    }

    pub(crate) fn timeline(&mut self, key: &str) -> Option<Arc<Timeline>> {
        match self.entries.get(&CacheKey::Timeline(key.to_string()))? {
            (Cached::Timeline(t, _), _) => Some(t.clone()),
            _ => None,
        }
    }

    /// The backend keys of every table flagged proven, read without
    /// touching recency (a speculative window's candidate filter).
    pub(crate) fn proven_keys(&self) -> std::collections::HashSet<String> {
        self.entries
            .iter()
            .filter_map(|(k, v)| match (k, v) {
                (CacheKey::Timeline(key), (Cached::Timeline(_, true), _)) => {
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
            Some((Cached::Timeline(_, true), _))
        )
    }

    /// The table's last selection proved a version over its cached
    /// timeline (no extra entry: a flag on that timeline's).
    fn mark_proven(&mut self, key: &SchemaKey) {
        if let Some((Cached::Timeline(_, proven), _)) = self
            .entries
            .peek_mut(&CacheKey::Timeline(key.backend_key()))
        {
            *proven = true;
        }
    }

    /// Cache `timeline`, holding its barrier streams once: a stream already
    /// held is shared, a new one is held (and accounted) from here.
    fn put_timeline(&mut self, key: String, timeline: Arc<Timeline>) {
        self.remove(&CacheKey::Timeline(key.clone()));
        // Evict until the timeline, and those of its streams not held, fit
        // (an eviction may drop a stream it would have shared).
        loop {
            let new: u64 = timeline
                .streams
                .iter()
                .filter(|s| {
                    self.barriers
                        .get(&s.stream)
                        .is_none_or(|(held, _)| held != *s)
                })
                .map(|s| s.bytes)
                .sum();
            if self.timelines < self.max_tables
                && self.bytes + timeline.bytes + new <= self.max_bytes
            {
                break;
            }
            if !self.evict_one() {
                return;
            }
        }
        let mut shared = (*timeline).clone();
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
                        self.bytes -= old.bytes;
                    }
                    self.barriers.insert(s.stream.clone(), (s.clone(), 1));
                    self.bytes += s.bytes;
                }
            }
        }
        self.timelines += 1;
        self.bytes += shared.bytes;
        let bytes = shared.bytes;
        self.entries.push(
            CacheKey::Timeline(key),
            (Cached::Timeline(Arc::new(shared), false), bytes),
        );
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
            (Cached::Selection(s), _) => Some(s.clone()),
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
            (Cached::Version(v), _) => Some(v.clone()),
            _ => None,
        }
    }

    fn put_version(
        &mut self,
        table: String,
        v: schema_registry::SchemaVersion,
    ) {
        let bytes = serde_json::to_vec(&v.schema_json)
            .map_or(0, |b| b.len() as u64)
            + 64;
        self.put(
            CacheKey::Version(table, v.version),
            Cached::Version(Arc::new(v)),
            bytes,
        );
    }

    fn put_selection(
        &mut self,
        (table, decisive, signature): (String, String, String),
        loaded: Arc<LoadedSchema>,
    ) {
        let bytes = schema_bytes(&loaded);
        self.put(
            CacheKey::Selection(table, decisive, signature),
            Cached::Selection(loaded),
            bytes,
        );
    }

    #[cfg(test)]
    pub(crate) fn with_budget_for_test(budget: usize) -> Self {
        Self::with_limits(
            budget,
            SELECTION_CACHE_BYTES,
            metrics::Counter::noop(),
        )
    }

    /// Change the bounds, keeping what is held (tests).
    #[cfg(test)]
    pub(crate) fn set_limits_for_test(&mut self, tables: usize, bytes: u64) {
        self.max_tables = tables;
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

    /// The entries held for the table `key` (tests).
    #[cfg(test)]
    pub(crate) fn held_for(&self, key: &SchemaKey) -> usize {
        let k = key.backend_key();
        self.entries.iter().filter(|(e, _)| e.table() == k).count()
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

    /// A barrier was appended to `stream` (a lineage's or a database's):
    /// the stream, and every timeline and selection of a table it applies
    /// to, are stale. Every other table stays cached.
    pub(crate) fn barrier_appended(&mut self, stream: &str) {
        self.invalidate_where(|t| t.starts_with(stream));
        if let Some((s, _)) = self.barriers.remove(stream) {
            self.bytes -= s.bytes;
        }
    }

    /// Everything is stale (a reload request without a table).
    pub(crate) fn invalidate_all(&mut self) {
        self.entries.clear();
        self.barriers.clear();
        self.timelines = 0;
        self.bytes = 0;
    }
}

/// A selected schema's estimated bytes: its encoding.
fn schema_bytes(loaded: &LoadedSchema) -> u64 {
    serde_json::to_vec(&loaded.schema).map_or(0, |b| b.len() as u64) + 64
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
        return Ok(t);
    }
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
    let mine = install(ctx, &keys, |k| k == key).await?;
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
    let _ = install(ctx, &missing, |_| false).await;
}

/// Read `keys`' histories (those `alone` names read alone if their batch
/// could not take them) and install every valid timeline within its limit.
/// The outcome for the key `alone` names, if any.
async fn install(
    ctx: &mut RunCtx,
    keys: &[SchemaKey],
    alone: impl Fn(&SchemaKey) -> bool,
) -> SourceResult<Option<SourceResult<Arc<Timeline>>>> {
    use super::mysql_history::Loaded;
    let registry = ctx.schema.registry_arc();
    let mut have = ctx.selection.barrier_streams();
    let read = super::mysql_history::read_each(
        &ctx.registry_backend,
        &registry,
        keys,
        &mut have,
        &alone,
    )
    .await
    .map_err(timeline_error)?;
    let mut mine = None;
    for (key, loaded) in keys.iter().zip(read) {
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
                        let t = Arc::new(t);
                        ctx.selection
                            .put_timeline(key.backend_key(), t.clone());
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
    // The versions the installed tables' last proofs name, read ahead in
    // one batch: a returning table's selection then reads nothing more.
    let mut wanted = Vec::new();
    for key in keys {
        let bk = key.backend_key();
        let Some(t) = ctx.selection.timeline(&bk) else {
            continue;
        };
        let last = t.records.iter().rev().find_map(|r| match &r.record.kind {
            Kind::Observed { version, .. } | Kind::Baseline { version, .. } => {
                Some(*version)
            }
            _ => None,
        });
        if let Some(v) = last
            && ctx.selection.version(&bk, v).is_none()
        {
            wanted.push((key.clone(), v));
        }
    }
    if !wanted.is_empty()
        && let Ok(read) = registry.get_versions(&wanted).await
    {
        for ((key, _), v) in wanted.iter().zip(read) {
            if let Some(v) = v {
                ctx.selection.put_version(key.backend_key(), v);
            }
        }
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
        Some(v) => loaded_from(&v, version),
        None => loaded(ctx.schema.registry(), key, version).await,
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
                return Ok(hit);
            }
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

    /// Under churn the cache never holds more than its tables and its
    /// bytes, and evicts (counted under the source's labels) only to make
    /// room.
    #[test]
    fn the_table_and_byte_bounds_hold_under_churn() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};
        let rec = DebuggingRecorder::new();
        let budget = 16;
        let puts = metrics::with_local_recorder(&rec, || {
            let mut caches = Caches::new("p", "s");
            caches.max_tables = budget;
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
                assert!(caches.timelines() <= budget);
                assert!(caches.bytes() <= caches.max_bytes);
            }
            assert_eq!(caches.timelines(), budget);
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

    #[test]
    fn invalidation_drops_only_the_tables_entries() {
        let mut caches = Caches::with_budget_for_test(64);
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
        let mut c = Caches::with_budget_for_test(16);
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
            let mut caches = Caches::with_budget_for_test(64);
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
            let mut caches = Caches::with_budget_for_test(64);
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
            let mut caches = Caches::with_budget_for_test(64);
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
            let mut caches = Caches::with_budget_for_test(64);
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
            let mut caches = Caches::with_budget_for_test(64);
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
            // 4 timelines of 54 MiB and the 10 MiB stream once: 226 MiB.
            assert_eq!(caches.timelines(), 4);
            assert_eq!(caches.bytes(), 4 * (54 << 20) + (10 << 20));
            assert_eq!(caches.barrier_streams().len(), 1);
            // The last timeline holding the stream takes it with it.
            caches.barrier_appended("lineage/");
            assert_eq!((caches.timelines(), caches.bytes()), (0, 0));
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
            assert_eq!(ctx.selection.bytes(), 0);
            load(&mut ctx, &keys).await;
            let z = timeline(&mut ctx, &keys[2]).await.unwrap();
            assert_eq!(z.barriers().count(), 1, "the lineage barrier only");
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
    }
}
