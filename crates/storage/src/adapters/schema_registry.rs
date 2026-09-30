//! Durable schema registry backed by the [`StorageBackend`] Log primitive.
//!
//! # Scale model
//! - **No startup scan.** Construction reads one durable record (the schema
//!   sequence high-water); it never enumerates or replays streams.
//! - **Latest lookup loads and caches the latest version only**, via
//!   `log_latest` on one fully-qualified stream.
//! - **History is paged.** [`DurableSchemaRegistry::history_page`] and
//!   [`DurableSchemaRegistry::legacy_page`] read bounded pages with explicit
//!   continuation; nothing materializes a table's full history.
//! - **Bounded memory.** The cache is weighted: it is bounded by resident
//!   bytes (primary) and resident entries (defensive secondary). An entry whose
//!   weight exceeds the byte budget is served transiently and never admitted.
//!   Eviction only forces a reload, so it affects performance, never
//!   correctness.
//!
//! # Identity
//! Every v1 entry carries its **explicit version number** and, when adopted from
//! the legacy flat key, its **original legacy sequence**, so selected-version
//! adoption never renumbers anything. New registrations are numbered after
//! `max(main head, legacy stream length)`, so a version number previously
//! observed by consumers is never reused for a different schema.
//!
//! # Layout
//! - `schemas.v1` log: main stream `v1/<tenant>/<source>/<lineage>/<db>/<table>`
//!   (strictly ascending versions; `log_latest` is the true latest) and a side
//!   history stream `<main>/~history` for adopted legacy versions older than the
//!   main head. KV `schemas.v1/<main>` is the scoped enumeration index.
//! - `schemas` log: legacy flat keys `<tenant>/<db>/<table>`; read-only here.
//! - `schemas.v1.index` KV: `v/<main>/<version>` version index,
//!   `h/<main>/<hash>` first-assigned version per hash, `f/<main>` legacy
//!   numbering floor, and the schema sequence high-water.
//! - `schemas.v1.migration` KV: whole-stream migration markers.

use std::collections::HashMap;
use std::mem::size_of;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use chrono::Utc;
use lru::LruCache;
use schema_registry::SchemaVersion;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Mutex as AsyncMutex;
use tracing::info;

use super::schema_key::SchemaKey;
use super::source_lineage;
use crate::ArcStorageBackend;

const V1_NS: &str = "schemas.v1";
const LEGACY_NS: &str = "schemas";
const INDEX_NS: &str = "schemas.v1.index";
const MIGRATION_NS: &str = "schemas.v1.migration";
const HW_KEY: &str = "__schema_seq_hw";
const SIDE_SUFFIX: &str = "/~history";

const CURRENT_FORMAT_VERSION: u32 = 1;
const CURRENT_RECORD_VERSION: u32 = 1;

/// Hard cap on any single history/legacy page.
pub const MAX_HISTORY_PAGE: usize = 1024;

/// Conservative per-entry overhead of a JSON object map node.
const MAP_ENTRY_OVERHEAD: usize = 32;
/// Conservative per-entry overhead of an LRU node (links + hash slot).
const CACHE_NODE_OVERHEAD: usize = 64;

fn legacy_format_version() -> u32 {
    0
}

/// Serialization format for log entries. Legacy (untagged) entries have
/// `format_version = 0` and no explicit `version`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LogEntry {
    #[serde(default = "legacy_format_version")]
    format_version: u32,
    #[serde(default)]
    record_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    version: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    origin_sequence: Option<u64>,
    hash: String,
    schema_json: Value,
    registered_at: chrono::DateTime<chrono::Utc>,
    checkpoint: Option<Vec<u8>>,
}

fn parse_v1(bytes: &[u8], log_seq: u64, stream: &str) -> Result<SchemaVersion> {
    let e: LogEntry = serde_json::from_slice(bytes).with_context(|| {
        format!("schema registry: corrupt entry in {stream}")
    })?;
    anyhow::ensure!(
        e.format_version == CURRENT_FORMAT_VERSION,
        "schema registry: unexpected format_version {} in {stream}",
        e.format_version
    );
    let version = e.version.with_context(|| {
        format!("schema registry: v1 entry without version in {stream}")
    })?;
    Ok(SchemaVersion {
        version,
        hash: e.hash,
        schema_json: e.schema_json,
        registered_at: e.registered_at,
        sequence: e.origin_sequence.unwrap_or(log_seq),
        checkpoint: e.checkpoint,
    })
}

fn legacy_key(tenant: &str, db: &str, table: &str) -> String {
    format!("{tenant}/{db}/{table}")
}

/// Which v1 stream a version lives in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HistoryStream {
    /// Strictly ascending versions; its last entry is the table's latest.
    Main,
    /// Adopted legacy versions older than the main head.
    Side,
}

fn stream_key(main: &str, stream: HistoryStream) -> String {
    match stream {
        HistoryStream::Main => main.to_string(),
        HistoryStream::Side => format!("{main}{SIDE_SUFFIX}"),
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct VersionIndex {
    stream: HistoryStream,
    hash: String,
    sequence: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct KeyMeta {
    legacy_floor: i32,
}

#[derive(Debug, Serialize, Deserialize)]
struct MigrationMarker {
    migrated_at_ms: i64,
    format_version: u32,
}

/// Registry construction/behaviour knobs.
#[derive(Debug, Clone)]
pub struct RegistryConfig {
    /// Primary bound: maximum resident bytes of cached latest versions, by the
    /// conservative owned-allocation estimate in [`entry_weight`].
    pub cache_max_bytes: usize,
    /// Defensive secondary bound: maximum resident cached entries.
    pub cache_max_entries: usize,
    /// Whether legacy adoption is permitted. When false, only v1 records are
    /// ever read or written.
    pub migration_enabled: bool,
    /// Page size used by internal paged scans (clamped to [`MAX_HISTORY_PAGE`]).
    pub history_page_size: usize,
}

impl Default for RegistryConfig {
    fn default() -> Self {
        Self {
            cache_max_bytes: 64 * 1024 * 1024,
            cache_max_entries: 50_000,
            migration_enabled: true,
            history_page_size: 256,
        }
    }
}

/// (tenant, source_id, lineage_hash) - the accounting/isolation unit.
type SourceId = (String, String, String);

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct SourceAccount {
    entries: usize,
    bytes: usize,
}

#[derive(Debug)]
struct Cached {
    version: Arc<SchemaVersion>,
    source: SourceId,
    weight: usize,
}

#[derive(Debug)]
struct CacheState {
    lru: LruCache<String, Cached>,
    bytes: usize,
    per_source: HashMap<SourceId, SourceAccount>,
}

impl CacheState {
    fn account_add(&mut self, c: &Cached) {
        self.bytes += c.weight;
        let e = self.per_source.entry(c.source.clone()).or_default();
        e.entries += 1;
        e.bytes += c.weight;
    }

    fn account_remove(&mut self, c: &Cached) {
        self.bytes = self.bytes.saturating_sub(c.weight);
        if let Some(e) = self.per_source.get_mut(&c.source) {
            e.entries = e.entries.saturating_sub(1);
            e.bytes = e.bytes.saturating_sub(c.weight);
            if e.entries == 0 {
                self.per_source.remove(&c.source);
            }
        }
    }
}

#[derive(Debug, Default)]
struct Counters {
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    backend_loads: AtomicU64,
    coalesced_loads: AtomicU64,
    evictions: AtomicU64,
    oversized_uncached: AtomicU64,
}

/// Snapshot of registry counters + residency, for the measurement harness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegistryMetrics {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub backend_loads: u64,
    pub coalesced_loads: u64,
    pub evictions: u64,
    pub oversized_uncached: u64,
    pub resident_entries: usize,
    pub resident_bytes: usize,
}

/// Continuation for [`DurableSchemaRegistry::history_page`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryCursor {
    stream: HistoryStream,
    after_seq: u64,
}

/// One bounded page of a qualified table's version history.
#[derive(Debug, Clone)]
pub struct HistoryPage {
    /// Versions in stream order, each with its explicit version number.
    pub versions: Vec<SchemaVersion>,
    /// `Some` while more versions may remain.
    pub next: Option<HistoryCursor>,
}

/// A legacy (pre-lineage) schema version read from the flat key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyVersion {
    /// Positional version number, identical to the pre-lineage registry's.
    pub version: i32,
    pub hash: String,
    pub schema_json: Value,
    pub registered_at: chrono::DateTime<chrono::Utc>,
    /// Original legacy log sequence; preserved on adoption.
    pub sequence: u64,
    pub checkpoint: Option<Vec<u8>>,
}

/// Continuation for [`DurableSchemaRegistry::legacy_page`]. Carries the next
/// positional version so numbering stays identical across pages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyCursor {
    after_seq: u64,
    next_version: i32,
}

/// One bounded page of a legacy flat-key stream.
#[derive(Debug, Clone)]
pub struct LegacyPage {
    pub versions: Vec<LegacyVersion>,
    pub next: Option<LegacyCursor>,
}

/// Outcome of adopting one legacy version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdoptOutcome {
    /// Copied into the given v1 stream with its identity preserved.
    Adopted { stream: HistoryStream },
    /// This version (same number and hash) was already present.
    AlreadyPresent,
}

/// Durable schema registry that survives process restarts.
#[derive(Debug)]
pub struct DurableSchemaRegistry {
    backend: ArcStorageBackend,
    config: RegistryConfig,
    cache: StdMutex<CacheState>,
    /// Single-flight / mutation serialization per fully-qualified key.
    key_locks: StdMutex<HashMap<String, Arc<AsyncMutex<()>>>>,
    hw_lock: AsyncMutex<()>,
    /// In-process mirror of the durable schema sequence high-water.
    sequence: AtomicU64,
    counters: Counters,
}

impl DurableSchemaRegistry {
    /// Build over an existing backend. Performs no stream enumeration or replay:
    /// it reads the durable schema sequence high-water (O(1)), bootstrapping it
    /// once from the schema namespace if the record does not yet exist.
    pub async fn new(backend: ArcStorageBackend) -> Result<Arc<Self>> {
        Self::with_config(backend, RegistryConfig::default()).await
    }

    pub async fn with_config(
        backend: ArcStorageBackend,
        config: RegistryConfig,
    ) -> Result<Arc<Self>> {
        let seq = load_or_bootstrap_hw(&backend).await?;
        info!(seq, "DurableSchemaRegistry: lazy init (no startup replay)");
        Ok(Arc::new(Self::build(backend, config, seq)))
    }

    /// Sync constructor for unit tests - fresh memory backend, empty cache.
    pub fn for_testing() -> Arc<Self> {
        Arc::new(Self::build(
            Arc::new(crate::MemoryStorageBackend::new()),
            RegistryConfig::default(),
            0,
        ))
    }

    fn build(
        backend: ArcStorageBackend,
        config: RegistryConfig,
        seq: u64,
    ) -> Self {
        Self {
            backend,
            cache: StdMutex::new(CacheState {
                lru: LruCache::unbounded(),
                bytes: 0,
                per_source: HashMap::new(),
            }),
            config,
            key_locks: StdMutex::new(HashMap::new()),
            hw_lock: AsyncMutex::new(()),
            sequence: AtomicU64::new(seq),
            counters: Counters::default(),
        }
    }

    /// The schema sequence high-water: the largest sequence of any registered
    /// schema version. Reproduces the pre-lineage registry's value.
    pub fn current_sequence(&self) -> u64 {
        self.sequence.load(Ordering::SeqCst)
    }

    pub fn migration_enabled(&self) -> bool {
        self.config.migration_enabled
    }

    // ---- latest (cached, single-flight, fail-closed) ----------------------

    /// Latest version of a fully-qualified table. Loads only the latest entry
    /// on a miss. `Ok(None)`: the qualified stream is empty. `Err`: a storage
    /// failure, never conflated with absence.
    pub async fn get_latest(
        &self,
        key: &SchemaKey,
    ) -> Result<Option<SchemaVersion>> {
        let bkey = key.backend_key();
        if let Some(v) = self.cache_get(&bkey) {
            self.counters.cache_hits.fetch_add(1, Ordering::SeqCst);
            return Ok(Some((*v).clone()));
        }
        self.counters.cache_misses.fetch_add(1, Ordering::SeqCst);

        let lock = self.key_lock(&bkey);
        let guard = lock.lock().await;
        let out = match self.cache_get(&bkey) {
            Some(v) => {
                self.counters.coalesced_loads.fetch_add(1, Ordering::SeqCst);
                Ok(Some((*v).clone()))
            }
            None => self.load_latest(&bkey, key).await,
        };
        drop(guard);
        self.release_key_lock(&bkey, &lock);
        out
    }

    async fn load_latest(
        &self,
        bkey: &str,
        key: &SchemaKey,
    ) -> Result<Option<SchemaVersion>> {
        let latest = self.read_stream_latest(bkey).await?;
        if let Some(sv) = &latest {
            self.cache_admit(bkey, key, sv.clone());
        }
        Ok(latest)
    }

    async fn read_stream_latest(
        &self,
        stream: &str,
    ) -> Result<Option<SchemaVersion>> {
        self.counters.backend_loads.fetch_add(1, Ordering::SeqCst);
        match self.backend.log_latest(V1_NS, stream).await.with_context(
            || format!("schema registry: failed to read latest of {stream}"),
        )? {
            None => Ok(None),
            Some((seq, bytes)) => Ok(Some(parse_v1(&bytes, seq, stream)?)),
        }
    }

    // ---- history (paged, uncached) ----------------------------------------

    /// One bounded page of a qualified table's history: the main stream first,
    /// then the side history stream. Pass `next` back to continue.
    pub async fn history_page(
        &self,
        key: &SchemaKey,
        cursor: Option<HistoryCursor>,
        limit: usize,
    ) -> Result<HistoryPage> {
        let limit = limit.clamp(1, MAX_HISTORY_PAGE);
        let main = key.backend_key();
        let cur = cursor.unwrap_or(HistoryCursor {
            stream: HistoryStream::Main,
            after_seq: 0,
        });
        let skey = stream_key(&main, cur.stream);
        let rows = self
            .backend
            .log_read_meta_since(V1_NS, &skey, cur.after_seq, limit)
            .await
            .with_context(|| {
                format!("schema registry: failed to page history of {skey}")
            })?;
        let mut versions = Vec::with_capacity(rows.len());
        for r in &rows {
            versions.push(parse_v1(&r.value, r.seq, &skey)?);
        }
        let next = match rows.last() {
            Some(last) if rows.len() == limit => Some(HistoryCursor {
                stream: cur.stream,
                after_seq: last.seq,
            }),
            _ if cur.stream == HistoryStream::Main => Some(HistoryCursor {
                stream: HistoryStream::Side,
                after_seq: 0,
            }),
            _ => None,
        };
        Ok(HistoryPage { versions, next })
    }

    /// The version with the greatest sequence `<= sequence`, found by a bounded
    /// paged scan (memory O(page)).
    pub async fn get_at_sequence(
        &self,
        key: &SchemaKey,
        sequence: u64,
    ) -> Result<Option<SchemaVersion>> {
        let mut best: Option<SchemaVersion> = None;
        let mut cursor = None;
        loop {
            let page = self
                .history_page(key, cursor, self.config.history_page_size)
                .await?;
            for v in page.versions {
                if v.sequence <= sequence
                    && best.as_ref().is_none_or(|b| v.sequence > b.sequence)
                {
                    best = Some(v);
                }
            }
            match page.next {
                Some(n) => cursor = Some(n),
                None => return Ok(best),
            }
        }
    }

    // ---- register ---------------------------------------------------------

    /// Register a schema version under a fully-qualified key. Idempotent by
    /// hash via the hash index (no history scan). A new version is numbered
    /// after `max(main head, legacy floor)`.
    pub async fn register_with_checkpoint(
        &self,
        key: &SchemaKey,
        hash: &str,
        schema_json: &Value,
        checkpoint: Option<&[u8]>,
    ) -> Result<i32> {
        let bkey = key.backend_key();
        let lock = self.key_lock(&bkey);
        let guard = lock.lock().await;
        let out = self
            .register_locked(&bkey, key, hash, schema_json, checkpoint)
            .await;
        drop(guard);
        self.release_key_lock(&bkey, &lock);
        out
    }

    async fn register_locked(
        &self,
        bkey: &str,
        key: &SchemaKey,
        hash: &str,
        schema_json: &Value,
        checkpoint: Option<&[u8]>,
    ) -> Result<i32> {
        let main_latest = self.repair_indexes(bkey).await?;
        if let Some(existing) = self.hash_index(bkey, hash).await? {
            return Ok(existing);
        }
        let floor = self
            .key_floor(key, bkey)
            .await?
            .max(self.other_lineage_head(key).await?);
        let main_max = main_latest.as_ref().map_or(0, |v| v.version);
        let version = main_max
            .max(floor)
            .checked_add(1)
            .context("schema registry: version number overflow")?;
        if main_latest.is_none() {
            self.backend.kv_put(V1_NS, bkey, b"1").await?;
        }
        let entry = LogEntry {
            format_version: CURRENT_FORMAT_VERSION,
            record_version: CURRENT_RECORD_VERSION,
            version: Some(version),
            origin_sequence: None,
            hash: hash.to_string(),
            schema_json: schema_json.clone(),
            registered_at: Utc::now(),
            checkpoint: checkpoint.map(|c| c.to_vec()),
        };
        let seq = self
            .backend
            .log_append(V1_NS, bkey, &serde_json::to_vec(&entry)?)
            .await?;
        let sv = SchemaVersion {
            version,
            hash: entry.hash,
            schema_json: entry.schema_json,
            registered_at: entry.registered_at,
            sequence: seq,
            checkpoint: entry.checkpoint,
        };
        self.ensure_indexed(bkey, HistoryStream::Main, &sv).await?;
        self.bump_hw(seq).await?;
        self.cache_admit(bkey, key, sv);
        Ok(version)
    }

    // ---- legacy reads + proof-gated adoption mechanics --------------------

    /// Number of versions in the legacy flat-key stream (0 if absent).
    pub async fn legacy_len(
        &self,
        tenant: &str,
        db: &str,
        table: &str,
    ) -> Result<u64> {
        let lk = legacy_key(tenant, db, table);
        let exists = self
            .backend
            .log_latest(LEGACY_NS, &lk)
            .await
            .with_context(|| {
                format!("schema registry: failed to read legacy {lk}")
            })?
            .is_some();
        if !exists {
            return Ok(0);
        }
        Ok(self
            .backend
            .log_stream_meta(LEGACY_NS, &lk)
            .await
            .with_context(|| {
                format!("schema registry: failed to read legacy meta {lk}")
            })?
            .len)
    }

    /// One bounded page of the legacy flat-key stream, numbered exactly as the
    /// pre-lineage registry numbered it. Storage error => `Err`.
    pub async fn legacy_page(
        &self,
        tenant: &str,
        db: &str,
        table: &str,
        cursor: Option<LegacyCursor>,
        limit: usize,
    ) -> Result<LegacyPage> {
        let limit = limit.clamp(1, MAX_HISTORY_PAGE);
        let lk = legacy_key(tenant, db, table);
        let cur = cursor.unwrap_or(LegacyCursor {
            after_seq: 0,
            next_version: 1,
        });
        let rows = self
            .backend
            .log_read_meta_since(LEGACY_NS, &lk, cur.after_seq, limit)
            .await
            .with_context(|| {
                format!("schema registry: failed to page legacy {lk}")
            })?;
        let mut versions = Vec::with_capacity(rows.len());
        for (i, r) in rows.iter().enumerate() {
            let e: LogEntry =
                serde_json::from_slice(&r.value).with_context(|| {
                    format!("schema registry: corrupt legacy entry in {lk}")
                })?;
            versions.push(LegacyVersion {
                version: cur.next_version + i as i32,
                hash: e.hash,
                schema_json: e.schema_json,
                registered_at: e.registered_at,
                sequence: r.seq,
                checkpoint: e.checkpoint,
            });
        }
        let next = match rows.last() {
            Some(last) if rows.len() == limit => Some(LegacyCursor {
                after_seq: last.seq,
                next_version: cur.next_version + rows.len() as i32,
            }),
            _ => None,
        };
        Ok(LegacyPage { versions, next })
    }

    /// Adopt ONE legacy version into the qualified stream, preserving its
    /// version number, hash and original sequence. Idempotent; a version already
    /// present with a different hash is an identity conflict and fails closed.
    ///
    /// The caller must already have proven that this version belongs to `key`'s
    /// verified live lineage. This performs only the durable mechanics.
    pub async fn adopt_legacy_version(
        &self,
        key: &SchemaKey,
        lv: &LegacyVersion,
    ) -> Result<AdoptOutcome> {
        anyhow::ensure!(
            self.config.migration_enabled,
            "schema registry: migration is disabled"
        );
        let bkey = key.backend_key();
        let lock = self.key_lock(&bkey);
        let guard = lock.lock().await;
        let out = self.adopt_locked(&bkey, key, lv).await;
        drop(guard);
        self.release_key_lock(&bkey, &lock);
        out
    }

    async fn adopt_locked(
        &self,
        bkey: &str,
        key: &SchemaKey,
        lv: &LegacyVersion,
    ) -> Result<AdoptOutcome> {
        let main_latest = self.repair_indexes(bkey).await?;
        if let Some(existing) = self.version_index(bkey, lv.version).await? {
            anyhow::ensure!(
                existing.hash == lv.hash,
                "schema registry: identity conflict adopting legacy version {} into {bkey}: \
                 already present with a different hash",
                lv.version
            );
            return Ok(AdoptOutcome::AlreadyPresent);
        }
        let floor = self.key_floor(key, bkey).await?;
        anyhow::ensure!(
            lv.version >= 1 && lv.version <= floor,
            "schema registry: legacy version {} outside the recorded legacy range 1..={floor} \
             for {bkey}",
            lv.version
        );
        let main_max = main_latest.as_ref().map_or(0, |v| v.version);
        let stream = if lv.version > main_max {
            HistoryStream::Main
        } else {
            HistoryStream::Side
        };
        if stream == HistoryStream::Main && main_latest.is_none() {
            self.backend.kv_put(V1_NS, bkey, b"1").await?;
        }
        let entry = LogEntry {
            format_version: CURRENT_FORMAT_VERSION,
            record_version: CURRENT_RECORD_VERSION,
            version: Some(lv.version),
            origin_sequence: Some(lv.sequence),
            hash: lv.hash.clone(),
            schema_json: lv.schema_json.clone(),
            registered_at: lv.registered_at,
            checkpoint: lv.checkpoint.clone(),
        };
        self.backend
            .log_append(
                V1_NS,
                &stream_key(bkey, stream),
                &serde_json::to_vec(&entry)?,
            )
            .await?;
        let sv = SchemaVersion {
            version: lv.version,
            hash: entry.hash,
            schema_json: entry.schema_json,
            registered_at: entry.registered_at,
            sequence: lv.sequence,
            checkpoint: entry.checkpoint,
        };
        self.ensure_indexed(bkey, stream, &sv).await?;
        self.bump_hw(lv.sequence).await?;
        if stream == HistoryStream::Main {
            self.cache_invalidate(bkey);
        }
        Ok(AdoptOutcome::Adopted { stream })
    }

    /// Record that the whole legacy stream for `key` has been migrated.
    pub async fn mark_migrated(&self, key: &SchemaKey) -> Result<()> {
        anyhow::ensure!(
            self.config.migration_enabled,
            "schema registry: migration is disabled"
        );
        let marker = MigrationMarker {
            migrated_at_ms: Utc::now().timestamp_millis(),
            format_version: CURRENT_FORMAT_VERSION,
        };
        self.backend
            .kv_put(
                MIGRATION_NS,
                &key.backend_key(),
                &serde_json::to_vec(&marker)?,
            )
            .await
            .context("schema registry: failed to write migration marker")
    }

    /// Whether a whole-stream migration marker exists for `key`.
    pub async fn is_migrated(&self, key: &SchemaKey) -> Result<bool> {
        Ok(self
            .backend
            .kv_get(MIGRATION_NS, &key.backend_key())
            .await
            .context("schema registry: failed to read migration marker")?
            .is_some())
    }

    // ---- indexes + sequence high-water ------------------------------------

    /// Index the latest entry of each stream if a crash left it unindexed
    /// (appends precede their index writes; under the per-key lock only a
    /// stream's last entry can be unindexed). Returns the main stream latest.
    async fn repair_indexes(
        &self,
        bkey: &str,
    ) -> Result<Option<SchemaVersion>> {
        let main = self.read_stream_latest(bkey).await?;
        if let Some(sv) = &main {
            self.ensure_indexed(bkey, HistoryStream::Main, sv).await?;
            self.bump_hw(sv.sequence).await?;
        }
        let side = stream_key(bkey, HistoryStream::Side);
        if let Some(sv) = self.read_stream_latest(&side).await? {
            self.ensure_indexed(bkey, HistoryStream::Side, &sv).await?;
            self.bump_hw(sv.sequence).await?;
        }
        Ok(main)
    }

    async fn ensure_indexed(
        &self,
        bkey: &str,
        stream: HistoryStream,
        sv: &SchemaVersion,
    ) -> Result<()> {
        match self.version_index(bkey, sv.version).await? {
            Some(existing) => anyhow::ensure!(
                existing.hash == sv.hash,
                "schema registry: version index conflict for {bkey} v{}",
                sv.version
            ),
            None => {
                let idx = VersionIndex {
                    stream,
                    hash: sv.hash.clone(),
                    sequence: sv.sequence,
                };
                self.backend
                    .kv_put(
                        INDEX_NS,
                        &format!("v/{bkey}/{}", sv.version),
                        &serde_json::to_vec(&idx)?,
                    )
                    .await?;
            }
        }
        // First-assigned version per hash is stable once written.
        if self.hash_index(bkey, &sv.hash).await?.is_none() {
            self.backend
                .kv_put(
                    INDEX_NS,
                    &format!("h/{bkey}/{}", sv.hash),
                    &serde_json::to_vec(&sv.version)?,
                )
                .await?;
        }
        Ok(())
    }

    async fn version_index(
        &self,
        bkey: &str,
        version: i32,
    ) -> Result<Option<VersionIndex>> {
        match self
            .backend
            .kv_get(INDEX_NS, &format!("v/{bkey}/{version}"))
            .await
            .context("schema registry: failed to read version index")?
        {
            Some(b) => Ok(Some(serde_json::from_slice(&b)?)),
            None => Ok(None),
        }
    }

    async fn hash_index(&self, bkey: &str, hash: &str) -> Result<Option<i32>> {
        match self
            .backend
            .kv_get(INDEX_NS, &format!("h/{bkey}/{hash}"))
            .await
            .context("schema registry: failed to read hash index")?
        {
            Some(b) => Ok(Some(serde_json::from_slice(&b)?)),
            None => Ok(None),
        }
    }

    /// Legacy numbering floor for `key`, recorded at the first v1 mutation so
    /// new versions never reuse a number the legacy registry already assigned.
    async fn key_floor(&self, key: &SchemaKey, bkey: &str) -> Result<i32> {
        let fk = format!("f/{bkey}");
        if let Some(b) = self
            .backend
            .kv_get(INDEX_NS, &fk)
            .await
            .context("schema registry: failed to read numbering floor")?
        {
            return Ok(serde_json::from_slice::<KeyMeta>(&b)?.legacy_floor);
        }
        let len = self.legacy_len(&key.tenant, &key.db, &key.table).await?;
        let legacy_floor = i32::try_from(len)
            .context("schema registry: legacy stream too long")?;
        self.backend
            .kv_put(
                INDEX_NS,
                &fk,
                &serde_json::to_vec(&KeyMeta { legacy_floor })?,
            )
            .await?;
        Ok(legacy_floor)
    }

    /// Highest main-stream version this source reached under any OTHER lineage
    /// (predecessors after a failover or replacement, or a later lineage after a
    /// failback). Only version numbers are read - no schema from another lineage
    /// is ever served - so numbering stays monotonic across lineage changes
    /// without inheriting schemas. Computed per registration, never persisted, so
    /// it cannot go stale on a failback.
    async fn other_lineage_head(&self, key: &SchemaKey) -> Result<i32> {
        let Some(rec) =
            source_lineage::load(&self.backend, &key.tenant, &key.source_id)
                .await?
        else {
            return Ok(0);
        };
        let mut head = 0;
        for hash in rec.all_lineage_hashes() {
            if hash == key.lineage_hash {
                continue;
            }
            let other = SchemaKey {
                lineage_hash: hash.to_string(),
                ..key.clone()
            };
            if let Some(sv) =
                self.read_stream_latest(&other.backend_key()).await?
            {
                head = head.max(sv.version);
            }
        }
        Ok(head)
    }

    /// Raise the durable high-water, then the in-process mirror.
    async fn bump_hw(&self, seq: u64) -> Result<()> {
        if seq <= self.sequence.load(Ordering::SeqCst) {
            return Ok(());
        }
        let _g = self.hw_lock.lock().await;
        if seq <= self.sequence.load(Ordering::SeqCst) {
            return Ok(());
        }
        self.backend
            .kv_put(INDEX_NS, HW_KEY, &serde_json::to_vec(&seq)?)
            .await
            .context(
                "schema registry: failed to persist sequence high-water",
            )?;
        self.sequence.store(seq, Ordering::SeqCst);
        Ok(())
    }

    // ---- observability ----------------------------------------------------

    pub fn metrics(&self) -> RegistryMetrics {
        let (resident_entries, resident_bytes) = {
            let st = self.cache.lock().unwrap();
            (st.lru.len(), st.bytes)
        };
        RegistryMetrics {
            cache_hits: self.counters.cache_hits.load(Ordering::SeqCst),
            cache_misses: self.counters.cache_misses.load(Ordering::SeqCst),
            backend_loads: self.counters.backend_loads.load(Ordering::SeqCst),
            coalesced_loads: self
                .counters
                .coalesced_loads
                .load(Ordering::SeqCst),
            evictions: self.counters.evictions.load(Ordering::SeqCst),
            oversized_uncached: self
                .counters
                .oversized_uncached
                .load(Ordering::SeqCst),
            resident_entries,
            resident_bytes,
        }
    }

    /// Per-source residency: `(tenant, source_id, lineage_hash) -> (entries, bytes)`.
    pub fn per_source_accounting(&self) -> Vec<(SourceId, usize, usize)> {
        self.cache
            .lock()
            .unwrap()
            .per_source
            .iter()
            .map(|(k, v)| (k.clone(), v.entries, v.bytes))
            .collect()
    }

    // ---- cache internals --------------------------------------------------

    fn cache_get(&self, bkey: &str) -> Option<Arc<SchemaVersion>> {
        self.cache
            .lock()
            .unwrap()
            .lru
            .get(bkey)
            .map(|c| c.version.clone())
    }

    /// Admit `sv` as the cached latest for `bkey`, then evict LRU entries until
    /// both budgets hold. An entry heavier than the whole byte budget is not
    /// admitted (the caller still returns it transiently).
    fn cache_admit(&self, bkey: &str, key: &SchemaKey, sv: SchemaVersion) {
        let source = source_id_of(key);
        let weight = entry_weight(bkey, &source, &sv);
        let max_bytes = self.config.cache_max_bytes;
        let max_entries = self.config.cache_max_entries.max(1);
        let mut st = self.cache.lock().unwrap();
        if let Some(old) = st.lru.pop(bkey) {
            st.account_remove(&old);
        }
        if weight > max_bytes {
            self.counters
                .oversized_uncached
                .fetch_add(1, Ordering::SeqCst);
            return;
        }
        let cached = Cached {
            version: Arc::new(sv),
            source,
            weight,
        };
        st.account_add(&cached);
        st.lru.put(bkey.to_string(), cached);
        while (st.bytes > max_bytes || st.lru.len() > max_entries)
            && st.lru.len() > 1
        {
            match st.lru.pop_lru() {
                Some((_, victim)) => {
                    st.account_remove(&victim);
                    self.counters.evictions.fetch_add(1, Ordering::SeqCst);
                }
                None => break,
            }
        }
    }

    fn cache_invalidate(&self, bkey: &str) {
        let mut st = self.cache.lock().unwrap();
        if let Some(old) = st.lru.pop(bkey) {
            st.account_remove(&old);
        }
    }

    fn key_lock(&self, bkey: &str) -> Arc<AsyncMutex<()>> {
        self.key_locks
            .lock()
            .unwrap()
            .entry(bkey.to_string())
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone()
    }

    /// Drop the per-key lock entry once no other caller references it, so
    /// `key_locks` scales with in-flight concurrency, not with total keys.
    fn release_key_lock(&self, bkey: &str, lock: &Arc<AsyncMutex<()>>) {
        let mut m = self.key_locks.lock().unwrap();
        // 1 (map) + 1 (our `lock`) == 2 means no other waiter holds a clone.
        if Arc::strong_count(lock) <= 2 {
            m.remove(bkey);
        }
    }
}

/// Read the durable schema sequence high-water, bootstrapping it once from the
/// schema namespaces when absent. The bootstrap value equals what the
/// pre-lineage registry derived at startup (max sequence over schema entries),
/// so the externally stamped `schema_sequence` is unchanged across the upgrade.
async fn load_or_bootstrap_hw(backend: &ArcStorageBackend) -> Result<u64> {
    if let Some(b) = backend
        .kv_get(INDEX_NS, HW_KEY)
        .await
        .context("schema registry: failed to read sequence high-water")?
    {
        return serde_json::from_slice(&b)
            .context("schema registry: corrupt sequence high-water");
    }
    let legacy = backend
        .log_ns_max_seq(LEGACY_NS)
        .await
        .context("schema registry: failed to bootstrap sequence high-water")?;
    let v1 = backend
        .log_ns_max_seq(V1_NS)
        .await
        .context("schema registry: failed to bootstrap sequence high-water")?;
    let hw = legacy.max(v1);
    backend
        .kv_put(INDEX_NS, HW_KEY, &serde_json::to_vec(&hw)?)
        .await
        .context("schema registry: failed to persist sequence high-water")?;
    info!(
        hw,
        "DurableSchemaRegistry: bootstrapped schema sequence high-water (one-time)"
    );
    Ok(hw)
}

fn source_id_of(key: &SchemaKey) -> SourceId {
    (
        key.tenant.clone(),
        key.source_id.clone(),
        key.lineage_hash.clone(),
    )
}

/// Conservative owned-allocation estimate of one cached latest version: node
/// and struct overheads, the key and source strings, the hash and checkpoint,
/// and the recursive heap footprint of the schema JSON.
pub(crate) fn entry_weight(
    bkey: &str,
    source: &SourceId,
    sv: &SchemaVersion,
) -> usize {
    CACHE_NODE_OVERHEAD
        + size_of::<Cached>()
        + size_of::<SchemaVersion>()
        + bkey.len()
        + source.0.capacity()
        + source.1.capacity()
        + source.2.capacity()
        + sv.hash.capacity()
        + sv.checkpoint.as_ref().map_or(0, |c| c.capacity())
        + value_heap_bytes(&sv.schema_json)
}

fn value_heap_bytes(v: &Value) -> usize {
    match v {
        Value::Null | Value::Bool(_) | Value::Number(_) => 0,
        Value::String(s) => s.capacity(),
        Value::Array(a) => {
            a.capacity() * size_of::<Value>()
                + a.iter().map(value_heap_bytes).sum::<usize>()
        }
        Value::Object(m) => m
            .iter()
            .map(|(k, v)| {
                MAP_ENTRY_OVERHEAD
                    + size_of::<String>()
                    + k.capacity()
                    + size_of::<Value>()
                    + value_heap_bytes(v)
            })
            .sum(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::schema_key::LineageDescriptor;
    use crate::adapters::source_lineage;
    use crate::adapters::test_util::FaultBackend;
    use serde_json::json;

    fn skey(source: &str, lineage: &str, table: &str) -> SchemaKey {
        SchemaKey::new("t", source, lineage, "db", table)
    }

    /// Memory backend wrapped so `log_list` (full-history materialization) always
    /// fails: every registry path must work without it.
    fn backend() -> (Arc<FaultBackend>, ArcStorageBackend) {
        let f = Arc::new(FaultBackend::new());
        f.fail_log_list.store(true, Ordering::SeqCst);
        let b: ArcStorageBackend = f.clone();
        (f, b)
    }

    async fn reg(b: &ArcStorageBackend) -> Arc<DurableSchemaRegistry> {
        DurableSchemaRegistry::new(Arc::clone(b)).await.unwrap()
    }

    async fn reg_cfg(
        b: &ArcStorageBackend,
        config: RegistryConfig,
    ) -> Arc<DurableSchemaRegistry> {
        DurableSchemaRegistry::with_config(Arc::clone(b), config)
            .await
            .unwrap()
    }

    async fn register(
        r: &DurableSchemaRegistry,
        key: &SchemaKey,
        hash: &str,
    ) -> i32 {
        r.register_with_checkpoint(key, hash, &json!({ "h": hash }), None)
            .await
            .unwrap()
    }

    async fn register_big(
        r: &DurableSchemaRegistry,
        key: &SchemaKey,
        hash: &str,
        bytes: usize,
    ) {
        r.register_with_checkpoint(
            key,
            hash,
            &json!({ "pad": "x".repeat(bytes) }),
            None,
        )
        .await
        .unwrap();
    }

    /// Append a pre-lineage entry to the legacy flat key; returns its sequence.
    async fn seed_legacy(
        b: &ArcStorageBackend,
        table: &str,
        hash: &str,
    ) -> u64 {
        let entry = json!({
            "hash": hash,
            "schema_json": { "h": hash },
            "registered_at": serde_json::to_value(Utc::now()).unwrap(),
            "checkpoint": null,
        });
        b.log_append(
            LEGACY_NS,
            &format!("t/db/{table}"),
            &serde_json::to_vec(&entry).unwrap(),
        )
        .await
        .unwrap()
    }

    async fn all_history(
        r: &DurableSchemaRegistry,
        key: &SchemaKey,
        page: usize,
    ) -> Vec<SchemaVersion> {
        let mut out = Vec::new();
        let mut cursor = None;
        loop {
            let p = r.history_page(key, cursor, page).await.unwrap();
            assert!(p.versions.len() <= page, "page exceeded its bound");
            out.extend(p.versions);
            match p.next {
                Some(n) => cursor = Some(n),
                None => return out,
            }
        }
    }

    async fn all_legacy(
        r: &DurableSchemaRegistry,
        table: &str,
        page: usize,
    ) -> Vec<LegacyVersion> {
        let mut out = Vec::new();
        let mut cursor = None;
        loop {
            let p =
                r.legacy_page("t", "db", table, cursor, page).await.unwrap();
            assert!(p.versions.len() <= page, "legacy page exceeded its bound");
            out.extend(p.versions);
            match p.next {
                Some(n) => cursor = Some(n),
                None => return out,
            }
        }
    }

    // ---- no scan / latest-only / bounded memory ---------------------------

    #[tokio::test]
    async fn new_performs_no_startup_scan() {
        let (_f, b) = backend();
        {
            let r = reg(&b).await;
            for i in 0..3 {
                register(&r, &skey("s", "lin", &format!("t{i}")), "h1").await;
            }
        }
        let r2 = reg(&b).await;
        assert_eq!(r2.metrics().backend_loads, 0);
        assert_eq!(r2.metrics().resident_entries, 0);
        r2.get_latest(&skey("s", "lin", "t0"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r2.metrics().backend_loads, 1);
        assert_eq!(r2.metrics().resident_entries, 1);
    }

    #[tokio::test]
    async fn latest_lookup_caches_only_the_latest_version() {
        let (_f, b) = backend();
        let key = skey("s", "lin", "wide");
        {
            let r = reg(&b).await;
            for i in 0..8 {
                register_big(&r, &key, &format!("h{i}"), 10_000).await;
            }
        }
        let r2 = reg(&b).await;
        let latest = r2.get_latest(&key).await.unwrap().unwrap();
        assert_eq!(latest.version, 8);
        assert_eq!(latest.hash, "h7");
        let m = r2.metrics();
        assert_eq!(m.resident_entries, 1);
        assert!(
            m.resident_bytes < 2 * 10_000,
            "only one ~10KB version may be resident, got {} bytes",
            m.resident_bytes
        );
    }

    #[tokio::test]
    async fn byte_budget_bounds_resident_memory() {
        let (_f, b) = backend();
        {
            let r = reg(&b).await;
            for i in 0..10 {
                register_big(
                    &r,
                    &skey("s", "lin", &format!("t{i}")),
                    "h",
                    4_000,
                )
                .await;
            }
        }
        let budget = 3 * 5_000;
        let r2 = reg_cfg(
            &b,
            RegistryConfig {
                cache_max_bytes: budget,
                cache_max_entries: 1_000,
                ..RegistryConfig::default()
            },
        )
        .await;
        for i in 0..10 {
            r2.get_latest(&skey("s", "lin", &format!("t{i}")))
                .await
                .unwrap()
                .unwrap();
            assert!(
                r2.metrics().resident_bytes <= budget,
                "byte budget exceeded"
            );
        }
        let m = r2.metrics();
        assert!(m.evictions >= 1);
        assert!(m.resident_entries < 10);
        // Eviction is performance-only: the evicted table reloads intact.
        let v = r2
            .get_latest(&skey("s", "lin", "t0"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!((v.version, v.hash.as_str()), (1, "h"));
    }

    #[tokio::test]
    async fn entry_limit_is_a_secondary_bound() {
        let (_f, b) = backend();
        {
            let r = reg(&b).await;
            for i in 0..5 {
                register(&r, &skey("s", "lin", &format!("t{i}")), "h").await;
            }
        }
        let r2 = reg_cfg(
            &b,
            RegistryConfig {
                cache_max_entries: 2,
                ..RegistryConfig::default()
            },
        )
        .await;
        for i in 0..5 {
            r2.get_latest(&skey("s", "lin", &format!("t{i}")))
                .await
                .unwrap()
                .unwrap();
        }
        assert_eq!(r2.metrics().resident_entries, 2);
    }

    #[tokio::test]
    async fn oversized_entry_is_served_but_not_admitted() {
        let (_f, b) = backend();
        let key = skey("s", "lin", "huge");
        {
            let r = reg(&b).await;
            register_big(&r, &key, "h1", 50_000).await;
        }
        let r2 = reg_cfg(
            &b,
            RegistryConfig {
                cache_max_bytes: 10_000,
                ..RegistryConfig::default()
            },
        )
        .await;
        let v = r2.get_latest(&key).await.unwrap().unwrap();
        assert_eq!(v.hash, "h1");
        let m = r2.metrics();
        assert_eq!(m.resident_entries, 0);
        assert_eq!(m.resident_bytes, 0);
        assert_eq!(m.oversized_uncached, 1);
    }

    #[tokio::test]
    async fn single_flight_coalesces_concurrent_loads() {
        let (_f, b) = backend();
        {
            let r = reg(&b).await;
            register(&r, &skey("s", "lin", "t0"), "h1").await;
        }
        let r2 = reg(&b).await;
        let mut handles = Vec::new();
        for _ in 0..16 {
            let r = Arc::clone(&r2);
            handles.push(tokio::spawn(async move {
                r.get_latest(&skey("s", "lin", "t0")).await.unwrap()
            }));
        }
        for h in handles {
            h.await.unwrap().unwrap();
        }
        let m = r2.metrics();
        assert_eq!(m.backend_loads, 1, "concurrent loads must coalesce to one");
        assert!(m.coalesced_loads + m.cache_hits >= 15);
    }

    #[tokio::test]
    async fn history_pages_are_bounded_with_continuation() {
        let (_f, b) = backend();
        let r = reg(&b).await;
        let key = skey("s", "lin", "orders");
        for i in 1..=7 {
            register(&r, &key, &format!("h{i}")).await;
        }
        let all = all_history(&r, &key, 3).await;
        let numbers: Vec<i32> = all.iter().map(|v| v.version).collect();
        assert_eq!(numbers, (1..=7).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn register_is_idempotent_after_restart_via_index() {
        let (_f, b) = backend();
        let key = skey("s", "lin", "orders");
        {
            let r = reg(&b).await;
            for h in ["h1", "h2", "h3"] {
                register(&r, &key, h).await;
            }
        }
        let r2 = reg(&b).await;
        assert_eq!(
            register(&r2, &key, "h1").await,
            1,
            "reverted schema keeps its version"
        );
        assert_eq!(register(&r2, &key, "h4").await, 4);
    }

    // ---- fail-closed + isolation ------------------------------------------

    #[tokio::test]
    async fn storage_error_fails_closed_not_absent() {
        let (f, b) = backend();
        let r = reg(&b).await;
        f.fail_log_latest.store(true, Ordering::SeqCst);
        assert!(
            r.get_latest(&skey("s", "lin", "t0")).await.is_err(),
            "a storage read failure must be Err, never Ok(None)"
        );
    }

    #[tokio::test]
    async fn replaced_database_does_not_inherit_schemas() {
        let (_f, b) = backend();
        let r = reg(&b).await;
        register(&r, &skey("s", "lineage-old", "orders"), "h1").await;
        // Same tenant, source_id and table, but a different verified lineage.
        assert!(
            r.get_latest(&skey("s", "lineage-new", "orders"))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn per_source_accounting_isolates_sources() {
        let (_f, b) = backend();
        let r = reg(&b).await;
        register(&r, &skey("srcA", "linA", "orders"), "h1").await;
        register(&r, &skey("srcB", "linB", "orders"), "h1").await;
        let acct = r.per_source_accounting();
        assert_eq!(acct.len(), 2);
        for (src, entries, bytes) in acct {
            assert_eq!(entries, 1, "{src:?}");
            assert!(bytes > 0);
        }
    }

    // ---- identity: numbering + sequence -----------------------------------

    #[tokio::test]
    async fn new_versions_number_after_legacy_history() {
        let (_f, b) = backend();
        for h in ["h1", "h2", "h3"] {
            seed_legacy(&b, "orders", h).await;
        }
        let r = reg(&b).await;
        // Consumers already saw legacy versions 1..=3; a new schema must not reuse them.
        assert_eq!(register(&r, &skey("s", "lin", "orders"), "hNew").await, 4);
    }

    #[tokio::test]
    async fn sequence_mirror_bootstraps_from_schema_namespace_only() {
        let (_f, b) = backend();
        seed_legacy(&b, "orders", "h1").await;
        let legacy_max = seed_legacy(&b, "orders", "h2").await;
        // Unrelated log traffic raises the global sequence beyond any schema entry.
        for _ in 0..5 {
            b.log_append("journal", "x", b"e").await.unwrap();
        }
        let r = reg(&b).await;
        assert_eq!(
            r.current_sequence(),
            legacy_max,
            "must equal the pre-lineage value (max schema seq), not the global seq"
        );
        drop(r);
        // The bootstrapped value is durable: restart reads it in O(1).
        assert_eq!(reg(&b).await.current_sequence(), legacy_max);
    }

    #[tokio::test]
    async fn sequence_continues_after_restart() {
        let (_f, b) = backend();
        let before = {
            let r = reg(&b).await;
            register(&r, &skey("s", "lin", "t"), "h1").await;
            r.current_sequence()
        };
        let r2 = reg(&b).await;
        assert_eq!(r2.current_sequence(), before);
        register(&r2, &skey("s", "lin", "t"), "h2").await;
        assert!(r2.current_sequence() > before);
    }

    #[tokio::test]
    async fn numbering_never_reused_across_lineage_change_and_failback() {
        let (_f, b) = backend();
        let r = reg(&b).await;
        let a = LineageDescriptor::postgres(1, 10).unwrap();
        let bb = LineageDescriptor::postgres(2, 20).unwrap();
        let key =
            |d: &LineageDescriptor| skey("s", &d.lineage_hash(), "orders");
        // SchemaKey tenant is "t" in `skey`.
        source_lineage::establish(&b, "t", "s", a.clone())
            .await
            .unwrap();
        assert_eq!(register(&r, &key(&a), "h1").await, 1);
        assert_eq!(register(&r, &key(&a), "h2").await, 2);

        // Failover/replacement to lineage B: nothing is inherited...
        source_lineage::establish(&b, "t", "s", bb.clone())
            .await
            .unwrap();
        assert!(r.get_latest(&key(&bb)).await.unwrap().is_none());
        // ...and even an identical schema gets a number never used before.
        assert_eq!(register(&r, &key(&bb), "h1").await, 3);

        // Failback to A: numbering continues past B's head.
        source_lineage::establish(&b, "t", "s", a.clone())
            .await
            .unwrap();
        assert_eq!(register(&r, &key(&a), "h3").await, 4);
        // An already-registered schema on A keeps its identity.
        assert_eq!(register(&r, &key(&a), "h2").await, 2);
    }

    // ---- selected-version adoption ----------------------------------------

    #[tokio::test]
    async fn selected_adoption_preserves_number_hash_and_sequence() {
        let (_f, b) = backend();
        seed_legacy(&b, "orders", "h1").await;
        let s2 = seed_legacy(&b, "orders", "h2").await;
        seed_legacy(&b, "orders", "h3").await;
        let r = reg(&b).await;
        let key = skey("s", "lin", "orders");
        let legacy = all_legacy(&r, "orders", 2).await;
        let v2 = legacy.iter().find(|v| v.version == 2).unwrap().clone();

        let out = r.adopt_legacy_version(&key, &v2).await.unwrap();
        assert_eq!(
            out,
            AdoptOutcome::Adopted {
                stream: HistoryStream::Main
            }
        );

        let latest = r.get_latest(&key).await.unwrap().unwrap();
        assert_eq!(
            (latest.version, latest.hash.as_str(), latest.sequence),
            (2, "h2", s2)
        );
        // Unproven legacy versions 1 and 3 were NOT adopted.
        let hist = all_history(&r, &key, 10).await;
        assert_eq!(hist.iter().map(|v| v.version).collect::<Vec<_>>(), vec![2]);
        // The live schema matching the adopted version keeps its identity...
        assert_eq!(register(&r, &key, "h2").await, 2);
        // ...and a new schema numbers after the whole legacy range.
        assert_eq!(register(&r, &key, "h9").await, 4);
    }

    #[tokio::test]
    async fn older_adoption_goes_to_history_and_latest_is_unchanged() {
        let (_f, b) = backend();
        let s1 = seed_legacy(&b, "orders", "h1").await;
        seed_legacy(&b, "orders", "h2").await;
        let r = reg(&b).await;
        let key = skey("s", "lin", "orders");
        assert_eq!(register(&r, &key, "hNew").await, 3);
        let v1 = all_legacy(&r, "orders", 10).await.remove(0);

        let out = r.adopt_legacy_version(&key, &v1).await.unwrap();
        assert_eq!(
            out,
            AdoptOutcome::Adopted {
                stream: HistoryStream::Side
            }
        );
        let latest = r.get_latest(&key).await.unwrap().unwrap();
        assert_eq!(
            latest.version, 3,
            "an older adoption never replaces the latest"
        );
        let hist = all_history(&r, &key, 10).await;
        let old = hist.iter().find(|v| v.version == 1).unwrap();
        assert_eq!((old.hash.as_str(), old.sequence), ("h1", s1));
    }

    #[tokio::test]
    async fn adoption_is_idempotent_and_rejects_identity_conflicts() {
        let (_f, b) = backend();
        seed_legacy(&b, "orders", "h1").await;
        let r = reg(&b).await;
        let key = skey("s", "lin", "orders");
        let v1 = all_legacy(&r, "orders", 10).await.remove(0);
        r.adopt_legacy_version(&key, &v1).await.unwrap();
        assert_eq!(
            r.adopt_legacy_version(&key, &v1).await.unwrap(),
            AdoptOutcome::AlreadyPresent
        );
        let forged = LegacyVersion {
            hash: "other".into(),
            ..v1.clone()
        };
        assert!(r.adopt_legacy_version(&key, &forged).await.is_err());
        let beyond = LegacyVersion {
            version: 7,
            hash: "h7".into(),
            ..v1
        };
        assert!(
            r.adopt_legacy_version(&key, &beyond).await.is_err(),
            "a version outside the recorded legacy range fails closed"
        );
    }

    #[tokio::test]
    async fn whole_stream_migration_runs_in_bounded_pages() {
        let (_f, b) = backend();
        let mut seqs = Vec::new();
        for i in 1..=5 {
            seqs.push(seed_legacy(&b, "orders", &format!("h{i}")).await);
        }
        let r = reg(&b).await;
        let key = skey("s", "lin", "orders");
        let mut cursor = None;
        loop {
            let page =
                r.legacy_page("t", "db", "orders", cursor, 2).await.unwrap();
            assert!(page.versions.len() <= 2);
            for lv in &page.versions {
                r.adopt_legacy_version(&key, lv).await.unwrap();
            }
            match page.next {
                Some(n) => cursor = Some(n),
                None => break,
            }
        }
        r.mark_migrated(&key).await.unwrap();
        assert!(r.is_migrated(&key).await.unwrap());
        let hist = all_history(&r, &key, 2).await;
        assert_eq!(
            hist.iter().map(|v| v.version).collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );
        assert_eq!(hist.iter().map(|v| v.sequence).collect::<Vec<_>>(), seqs);
        assert_eq!(r.get_latest(&key).await.unwrap().unwrap().version, 5);
    }

    #[tokio::test]
    async fn adoption_rejected_when_migration_disabled() {
        let (_f, b) = backend();
        seed_legacy(&b, "orders", "h1").await;
        let r = reg_cfg(
            &b,
            RegistryConfig {
                migration_enabled: false,
                ..RegistryConfig::default()
            },
        )
        .await;
        let v1 = all_legacy(&r, "orders", 10).await.remove(0);
        assert!(
            r.adopt_legacy_version(&skey("s", "lin", "orders"), &v1)
                .await
                .is_err()
        );
        assert!(r.mark_migrated(&skey("s", "lin", "orders")).await.is_err());
    }
}
