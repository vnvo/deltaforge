//! Unified durable storage abstraction for DeltaForge operational runtime state.
//!
//! # Architecture
//!
//! [`StorageBackend`] exposes four primitives:
//! - **KV** - mutable point-in-time state with optional TTL (checkpoints, FSM, leases, dedup)
//! - **Log** - append-only monotonic history with global sequence (schema registry)
//! - **Slot** - mutable record with compare-and-swap (snapshot cursors, leader election)
//! - **Queue** - ordered bounded FIFO (quarantine buffer, DLQ)
//!
//! Two implementations are provided: [`MemoryStorageBackend`] (testing) and
//! [`SqliteStorageBackend`] (single-node production). A PostgreSQL backend
//! is planned for HA deployments.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

pub mod adapters;
pub mod memory;
pub mod postgres;
pub mod sqlite;

pub use memory::MemoryStorageBackend;
#[cfg(feature = "postgres")]
pub use postgres::PostgresStorageBackend;
#[cfg(feature = "sqlite")]
pub use sqlite::SqliteStorageBackend;

// Adapter re-exports for ergonomic top-level imports
pub use adapters::BackendCheckpointStore;
pub use adapters::DurableSchemaRegistry;

/// Unified storage backend trait.
///
/// All four primitives operate within a `(ns, key)` address space.
/// Namespaces are enforced by convention - see the namespace table in the spec.
#[async_trait]
pub trait StorageBackend: Send + Sync + std::fmt::Debug {
    async fn kv_get(&self, ns: &str, key: &str) -> Result<Option<Vec<u8>>>;
    async fn kv_put(&self, ns: &str, key: &str, value: &[u8]) -> Result<()>;
    /// Store with TTL. Lazy expiry on read + periodic sweep.
    async fn kv_put_with_ttl(
        &self,
        ns: &str,
        key: &str,
        value: &[u8],
        ttl_secs: u64,
    ) -> Result<()>;
    /// Returns `true` if the key existed.
    async fn kv_delete(&self, ns: &str, key: &str) -> Result<bool>;
    /// List keys, optionally filtered by prefix.
    async fn kv_list(
        &self,
        ns: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<String>>;

    /// Append a value; returns the **global** monotonic sequence number.
    async fn log_append(
        &self,
        ns: &str,
        key: &str,
        value: &[u8],
    ) -> Result<u64>;
    async fn log_list(
        &self,
        ns: &str,
        key: &str,
    ) -> Result<Vec<(u64, Vec<u8>)>>;
    /// Returns entries with seq > since_seq.
    async fn log_since(
        &self,
        ns: &str,
        key: &str,
        since_seq: u64,
    ) -> Result<Vec<(u64, Vec<u8>)>>;
    async fn log_latest(
        &self,
        ns: &str,
        key: &str,
    ) -> Result<Option<(u64, Vec<u8>)>>;

    /// Highest seq among entries currently retained in namespace `ns` (0 if
    /// none), computed backend-side without returning any entry. Intended for
    /// one-time bootstraps of a durable per-namespace high-water record, not for
    /// the hot path.
    async fn log_ns_max_seq(&self, ns: &str) -> Result<u64>;

    /// Append `value` under a deterministic capture identity, idempotently and
    /// atomically. The backend computes the content digest from `value` itself and
    /// never trusts a caller-supplied one.
    ///
    /// - `capture_id` absent for this `(ns, key)` -> insert; `{ seq, Inserted }`.
    /// - present and the stored bytes equal `value` -> no-op; `{ existing_seq,
    ///   AlreadyPresent }` (digest match confirmed by exact bytes).
    /// - present and the stored bytes differ -> `Err(LogError::CaptureIdentityConflict)`;
    ///   never overwrite, never duplicate.
    ///
    /// The insert and the stream `head_seq` update commit together. `capture_id` is
    /// the caller's dedup key; integrity is enforced by the backend digest, not the id.
    async fn log_append_if_absent(
        &self,
        ns: &str,
        key: &str,
        capture_id: &str,
        value: &[u8],
    ) -> Result<LogAppendOutcome>;

    /// Remove old entries from a stream, oldest-first, and advance the stream's
    /// durable `min_valid_from_seq` horizon - both in a SINGLE atomic operation
    /// (one DB transaction / one critical section). Never removes an entry with
    /// `seq >= req.pin_seq`. See [`LogTruncateRequest`] / [`LogTruncateOutcome`].
    async fn log_truncate(
        &self,
        ns: &str,
        key: &str,
        req: LogTruncateRequest,
    ) -> Result<LogTruncateOutcome>;

    /// Read a stream's durable metadata (horizon + head + oldest + len) without
    /// mutating it (beyond safe lazy initialization of the metadata row).
    async fn log_stream_meta(
        &self,
        ns: &str,
        key: &str,
    ) -> Result<LogStreamMeta>;

    /// Read up to `limit` entries with `seq > since_seq`, each with the metadata the
    /// replay reader verifies on load: append time, `capture_id`, and `content_hash`.
    async fn log_read_meta_since(
        &self,
        ns: &str,
        key: &str,
        since_seq: u64,
        limit: usize,
    ) -> Result<Vec<LogEntryMeta>>;

    /// Upsert a slot; returns the new version number.
    async fn slot_upsert(
        &self,
        ns: &str,
        key: &str,
        state: &[u8],
    ) -> Result<u64>;
    async fn slot_get(
        &self,
        ns: &str,
        key: &str,
    ) -> Result<Option<(u64, Vec<u8>)>>;
    /// Compare-and-swap. Returns `false` on version mismatch (not an error).
    async fn slot_cas(
        &self,
        ns: &str,
        key: &str,
        expected_version: u64,
        state: &[u8],
    ) -> Result<bool>;
    /// Atomically create a slot **only if absent**. Returns `Some(1)` when this
    /// call created it, or `None` if it already existed. Unlike `slot_upsert`,
    /// this never overwrites - it makes initial allocation race-free (a plain
    /// version-based CAS cannot express expect-absent).
    async fn slot_create(
        &self,
        ns: &str,
        key: &str,
        state: &[u8],
    ) -> Result<Option<u64>>;
    /// Returns `true` if the slot existed.
    async fn slot_delete(&self, ns: &str, key: &str) -> Result<bool>;

    /// Keyset-paginated discovery of slots in `ns` whose key starts with `prefix`
    /// (all keys when `None`), in ascending key order, starting strictly after
    /// `cursor` (an opaque token from a prior page; `None` starts at the beginning),
    /// returning at most `limit` records plus a `next_cursor` when more remain.
    ///
    /// - **Bounded:** never returns more than `limit` records (itself capped at
    ///   [`SLOT_LIST_MAX_LIMIT`]); never an unbounded vector.
    /// - **Eventually complete, not transactionally consistent:** slots may be
    ///   created, changed, or removed between pages. A full scan observes every slot
    ///   that exists for the whole scan; slots mutated mid-scan may or may not
    ///   appear. Callers reconcile via idempotent re-reads / CAS.
    /// - Ordering is byte-wise on the key (matching Rust `str` order) on every
    ///   backend, so pagination never skips or duplicates a stable key.
    /// - A `cursor` not produced by a prior `slot_list` is a [`SlotError::MalformedCursor`].
    async fn slot_list(
        &self,
        ns: &str,
        prefix: Option<&str>,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<SlotPage>;

    /// Push a value; returns the entry id.
    async fn queue_push(
        &self,
        ns: &str,
        key: &str,
        value: &[u8],
    ) -> Result<u64>;
    /// Peek at up to `limit` oldest entries without consuming them.
    async fn queue_peek(
        &self,
        ns: &str,
        key: &str,
        limit: usize,
    ) -> Result<Vec<(u64, Vec<u8>)>>;
    /// Acknowledge (delete) all entries with id <= up_to_id. Returns count deleted.
    async fn queue_ack(
        &self,
        ns: &str,
        key: &str,
        up_to_id: u64,
    ) -> Result<usize>;
    async fn queue_len(&self, ns: &str, key: &str) -> Result<u64>;
    /// Drop the oldest `count` entries. Returns count actually dropped.
    async fn queue_drop_oldest(
        &self,
        ns: &str,
        key: &str,
        count: usize,
    ) -> Result<usize>;

    /// Read `reads` from one consistent snapshot (see [`AtomicBatch`]). The
    /// default reads them one by one and is for test doubles only; every
    /// production backend overrides it.
    async fn read_batch(
        &self,
        reads: &[BatchRead],
    ) -> Result<Vec<BatchReadResult>> {
        let mut out = Vec::with_capacity(reads.len());
        for r in reads {
            out.push(match r {
                BatchRead::KvGet { ns, key } => {
                    BatchReadResult::Kv(self.kv_get(ns, key).await?)
                }
                BatchRead::LogLatest { ns, key } => {
                    BatchReadResult::Latest(self.log_latest(ns, key).await?)
                }
                BatchRead::LogHead { ns, key } => BatchReadResult::Head(
                    self.log_latest(ns, key).await?.map_or(0, |(seq, _)| seq),
                ),
                BatchRead::LogLen { ns, key } => BatchReadResult::Len(
                    self.log_stream_meta(ns, key).await?.len,
                ),
            });
        }
        Ok(out)
    }

    /// Every entry of each `(ns, key)` stream, exactly as
    /// `log_read_meta_since(ns, key, 0, ..)` pages them out, all from one
    /// consistent snapshot; one result per request, in request order (a
    /// repeated request is answered twice). The default reads stream by
    /// stream and is for test doubles only; every production backend
    /// overrides it.
    async fn log_read_streams(
        &self,
        streams: &[(String, String)],
    ) -> Result<Vec<Vec<LogEntryMeta>>> {
        let mut out = Vec::with_capacity(streams.len());
        for (ns, key) in streams {
            let mut entries: Vec<LogEntryMeta> = Vec::new();
            loop {
                let since = entries.last().map_or(0, |e| e.seq);
                let page =
                    self.log_read_meta_since(ns, key, since, 1024).await?;
                let done = page.len() < 1024;
                entries.extend(page);
                if done {
                    break;
                }
            }
            out.push(entries);
        }
        Ok(out)
    }

    /// [`log_read_streams`](Self::log_read_streams) within `budget` bytes
    /// ([`log_entry_bytes`]): the leading streams, in request order, whose
    /// entries fit together, the first one always. Nothing past the first
    /// stream that does not fit is kept; the result answers that many
    /// requests. The default reads everything first and is for test doubles
    /// only; every production backend stops reading at the budget.
    async fn log_read_streams_within(
        &self,
        streams: &[(String, String)],
        budget: u64,
    ) -> Result<Vec<Vec<LogEntryMeta>>> {
        let all = self.log_read_streams(streams).await?;
        let mut total = 0u64;
        let mut out = Vec::with_capacity(all.len());
        for (i, entries) in all.into_iter().enumerate() {
            let bytes: u64 = entries.iter().map(log_entry_bytes).sum();
            if i > 0 && total + bytes > budget {
                break;
            }
            total += bytes;
            out.push(entries);
        }
        Ok(out)
    }

    /// Apply `batch` atomically (see [`AtomicBatch`]). The default refuses:
    /// a backend that cannot commit atomically never pretends to.
    async fn apply_batch(&self, _batch: &AtomicBatch) -> Result<BatchOutcome> {
        Err(anyhow::anyhow!(
            "this storage backend does not support atomic batches"
        ))
    }

    /// The commit marker of batch `batch_id` ([`BatchOp::CommitMarker`]), if
    /// that batch committed. Read only after every transaction that could
    /// still write it has ended (on PostgreSQL, under the store lock). The
    /// default refuses, like [`StorageBackend::apply_batch`].
    async fn batch_commit_get(
        &self,
        _batch_id: &str,
    ) -> Result<Option<BatchCommitRecord>> {
        Err(anyhow::anyhow!(
            "this storage backend does not support atomic batches"
        ))
    }

    /// Delete at most `limit` commit markers committed before `before_ms`,
    /// oldest first, in one transaction (one store-lock acquisition where
    /// there is a lock); returns how many. The default has none to delete.
    async fn batch_commit_sweep(
        &self,
        _before_ms: i64,
        _limit: usize,
    ) -> Result<usize> {
        Ok(0)
    }

    /// The entry stored under `capture_id` in `(ns, key)`, if any.
    async fn log_find_capture(
        &self,
        ns: &str,
        key: &str,
        capture_id: &str,
    ) -> Result<Option<(u64, Vec<u8>)>> {
        let mut since = 0;
        loop {
            let page = self.log_read_meta_since(ns, key, since, 1024).await?;
            for e in &page {
                if e.capture_id.as_deref() == Some(capture_id) {
                    return Ok(Some((e.seq, e.value.clone())));
                }
            }
            match page.last() {
                Some(last) if page.len() == 1024 => since = last.seq,
                _ => return Ok(None),
            }
        }
    }
}

// ---- atomic batches ---------------------------------------------------------

/// One read of [`StorageBackend::read_batch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchRead {
    KvGet {
        ns: String,
        key: String,
    },
    /// The last entry of a stream.
    LogLatest {
        ns: String,
        key: String,
    },
    /// The sequence of a stream's last entry (0 when empty).
    LogHead {
        ns: String,
        key: String,
    },
    /// The number of entries a stream retains.
    LogLen {
        ns: String,
        key: String,
    },
}

/// The result of one [`BatchRead`], in the same position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchReadResult {
    Kv(Option<Vec<u8>>),
    Latest(Option<(u64, Vec<u8>)>),
    Head(u64),
    Len(u64),
}

/// What a batch's decisions rest on, checked inside its transaction before
/// any write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Precondition {
    /// The stream's last sequence is still `seq` (0: still empty).
    LogHead { ns: String, key: String, seq: u64 },
    /// The key is still absent (`None`) or holds exactly these bytes.
    KvIs {
        ns: String,
        key: String,
        value: Option<Vec<u8>>,
    },
}

/// A KV value: bytes, or a template into which the backend renders the
/// sequence of the batch's log write `of` (in place of the decimal digits
/// of [`SEQ_SENTINEL`], which must occur exactly once).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchValue {
    Bytes(Vec<u8>),
    SeqTemplate { template: Vec<u8>, of: usize },
}

/// The number whose decimal digits a [`BatchValue::SeqTemplate`] carries in
/// place of a sequence.
pub const SEQ_SENTINEL: u64 = u64::MAX;

/// One write of an [`AtomicBatch`]. `of` names an earlier log write of the
/// same batch by its position in `ops`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchOp {
    /// A new entry under a capture identity that must not exist yet: a
    /// stored entry under it is a conflict.
    LogAppendIfAbsent {
        ns: String,
        key: String,
        capture_id: String,
        value: Vec<u8>,
    },
    /// An entry under a capture identity that may exist already with these
    /// exact bytes (the single-call `log_append_if_absent`'s rule): a stored
    /// identical entry answers with its original sequence, one with other
    /// bytes fails closed (`LogError::CaptureIdentityConflict`).
    LogAppendIdempotent {
        ns: String,
        key: String,
        capture_id: String,
        value: Vec<u8>,
    },
    KvPut {
        ns: String,
        key: String,
        value: BatchValue,
    },
    /// The key becomes the larger of its current number (JSON) and the
    /// sequence of log write `of`.
    KvMaxSeq { ns: String, key: String, of: usize },
    /// The key becomes the larger of its current number (JSON) and `value`.
    KvMax { ns: String, key: String, value: u64 },
    /// The batch's durable commit identity, written by [`commit_atomic`] as
    /// the transaction's last write: `batch_id`, the request `digest` and
    /// the encoded outcome of every earlier op ([`encode_batch_result`]).
    /// Resolving an ambiguous outcome reads only this marker.
    CommitMarker { batch_id: String, digest: Vec<u8> },
}

/// The most ops one atomic batch may hold, its commit marker excluded.
pub const MAX_BATCH_OPS: usize = 16_384;
/// The largest encoded outcome a commit marker may store.
pub const MAX_BATCH_RESULT_BYTES: usize = 256 * 1024;
/// How long one [`commit_atomic`] call may take: every attempt and the
/// resolution of an ambiguous outcome included.
pub const COMMIT_DEADLINE: std::time::Duration =
    std::time::Duration::from_secs(300);
/// How long a commit marker is kept: far longer than [`COMMIT_DEADLINE`],
/// the only time it is read.
pub const BATCH_MARKER_RETENTION_MS: i64 = 24 * 3600 * 1000;
/// Markers deleted per transaction (one store-lock acquisition).
pub const BATCH_MARKER_SWEEP_CHUNK: usize = 1024;
/// Chunks per sweep run, at most.
pub const BATCH_MARKER_SWEEP_MAX_CHUNKS: usize = 64;
/// Wall time per sweep run, at most.
pub const BATCH_MARKER_SWEEP_BUDGET: std::time::Duration =
    std::time::Duration::from_secs(1);

/// A stored commit marker (see [`BatchOp::CommitMarker`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchCommitRecord {
    pub digest: Vec<u8>,
    pub result: Vec<u8>,
    pub committed_at_ms: i64,
}

/// A commit marker exists under this batch id with another request digest:
/// two different batches claimed one identity. Never retried.
#[derive(Debug, thiserror::Error)]
#[error("batch identity conflict: marker {0} records another request")]
pub struct BatchIdentityConflict(pub String);

/// The most bytes one newly written state item (a kv value or a log entry)
/// may hold: every batch item, and every single write to a guarded
/// namespace (`docs/design/mysql-mass-invalidation.md`, "Chunking and size
/// limits"). Reads are never limited: an item written before the limit
/// stays readable, counted ([`oversized_reads`]).
pub const MAX_STATE_ITEM_BYTES: usize = 16 * 1024 * 1024;

/// A write refused before any statement: one item over
/// [`MAX_STATE_ITEM_BYTES`]. It names no table (the store knows none); the
/// caller that knows the relation classifies it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "a state item of {size} bytes exceeds the {MAX_STATE_ITEM_BYTES}-byte limit (key digest {key_digest})"
)]
pub struct StateItemTooLarge {
    pub size: usize,
    /// SHA-256 of the item's namespace and key, hex.
    pub key_digest: String,
}

/// The SHA-256 (hex) of a state item's namespace and key.
pub fn state_key_digest(ns: &str, key: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(ns.as_bytes());
    h.update([0u8]);
    h.update(key.as_bytes());
    hex::encode(h.finalize())
}

/// Refuse a new item of `size` bytes over the limit.
pub fn check_state_item(
    ns: &str,
    key: &str,
    size: usize,
) -> std::result::Result<(), StateItemTooLarge> {
    if size > MAX_STATE_ITEM_BYTES {
        return Err(StateItemTooLarge {
            size,
            key_digest: state_key_digest(ns, key),
        });
    }
    Ok(())
}

/// A single write's limit: guarded namespaces only (the registry and
/// activation state a window, baseline or drift writes).
pub(crate) fn check_guarded_write(
    ns: &str,
    key: &str,
    size: usize,
) -> Result<()> {
    if is_guarded_namespace(ns) {
        check_state_item(ns, key, size)?;
    }
    Ok(())
}

static OVERSIZED_READS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Reads of items over [`MAX_STATE_ITEM_BYTES`] since the process started.
pub fn oversized_reads() -> u64 {
    OVERSIZED_READS.load(std::sync::atomic::Ordering::SeqCst)
}

/// Count a read item over the limit (written before the limit existed),
/// logged once per item with its size and key digest.
pub(crate) fn note_read(ns: &str, key: &str, size: usize) {
    if size <= MAX_STATE_ITEM_BYTES {
        return;
    }
    OVERSIZED_READS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    metrics::counter!("deltaforge_state_oversized_reads_total").increment(1);
    static LOGGED: std::sync::Mutex<Option<std::collections::HashSet<String>>> =
        std::sync::Mutex::new(None);
    let digest = state_key_digest(ns, key);
    let mut logged = LOGGED.lock().expect("oversized reads");
    if logged
        .get_or_insert_with(Default::default)
        .insert(digest.clone())
    {
        tracing::warn!(
            size,
            key_digest = %digest,
            limit = MAX_STATE_ITEM_BYTES,
            "a state item over the write limit was read (written before the limit)"
        );
    }
}

/// A log entry's size as read: its value, its identifiers and a fixed
/// overhead.
pub fn log_entry_bytes(e: &LogEntryMeta) -> u64 {
    (e.value.len()
        + e.capture_id.as_ref().map_or(0, String::len)
        + e.content_hash.as_ref().map_or(0, String::len)) as u64
        + 32
}

/// Accumulates [`log_entry_bytes`] of stream reads within a budget: a
/// stream fits while the total does (the first stream always).
pub(crate) struct StreamBudget {
    budget: u64,
    total: u64,
}

impl StreamBudget {
    pub(crate) fn new(budget: u64) -> Self {
        Self { budget, total: 0 }
    }

    /// Count an entry of stream `i`; `false`: it does not fit, and the read
    /// stops before stream `i`.
    pub(crate) fn take(&mut self, i: usize, e: &LogEntryMeta) -> bool {
        let bytes = log_entry_bytes(e);
        if i > 0 && self.total + bytes > self.budget {
            return false;
        }
        self.total += bytes;
        true
    }
}

/// Count the oversized items among a batched read's results.
pub(crate) fn note_batch_reads(reads: &[BatchRead], got: &[BatchReadResult]) {
    for (r, g) in reads.iter().zip(got) {
        let (BatchRead::KvGet { ns, key } | BatchRead::LogLatest { ns, key }) =
            r
        else {
            continue;
        };
        match g {
            BatchReadResult::Kv(Some(v))
            | BatchReadResult::Latest(Some((_, v))) => {
                note_read(ns, key, v.len())
            }
            _ => {}
        }
    }
}

/// Count the oversized entries among stream reads.
pub(crate) fn note_stream_reads(
    streams: &[(String, String)],
    got: &[Vec<LogEntryMeta>],
) {
    for ((ns, key), entries) in streams.iter().zip(got) {
        for e in entries {
            note_read(ns, key, e.value.len());
        }
    }
}

/// The canonical digest of a batch request (its preconditions and ops, a
/// commit marker excluded), the identity a marker is checked against.
pub fn batch_digest(batch: &AtomicBatch) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    fn put(h: &mut Sha256, b: &[u8]) {
        h.update((b.len() as u64).to_be_bytes());
        h.update(b);
    }
    let mut h = Sha256::new();
    h.update(b"deltaforge.batch.v1");
    for p in &batch.preconditions {
        match p {
            Precondition::LogHead { ns, key, seq } => {
                put(&mut h, b"LogHead");
                put(&mut h, ns.as_bytes());
                put(&mut h, key.as_bytes());
                put(&mut h, &seq.to_be_bytes());
            }
            Precondition::KvIs { ns, key, value } => {
                put(&mut h, b"KvIs");
                put(&mut h, ns.as_bytes());
                put(&mut h, key.as_bytes());
                match value {
                    Some(v) => {
                        put(&mut h, b"some");
                        put(&mut h, v);
                    }
                    None => put(&mut h, b"none"),
                }
            }
        }
    }
    for op in &batch.ops {
        match op {
            BatchOp::LogAppendIfAbsent {
                ns,
                key,
                capture_id,
                value,
            } => {
                put(&mut h, b"LogAppendIfAbsent");
                put(&mut h, ns.as_bytes());
                put(&mut h, key.as_bytes());
                put(&mut h, capture_id.as_bytes());
                put(&mut h, value);
            }
            BatchOp::LogAppendIdempotent {
                ns,
                key,
                capture_id,
                value,
            } => {
                put(&mut h, b"LogAppendIdempotent");
                put(&mut h, ns.as_bytes());
                put(&mut h, key.as_bytes());
                put(&mut h, capture_id.as_bytes());
                put(&mut h, value);
            }
            BatchOp::KvPut { ns, key, value } => {
                put(&mut h, b"KvPut");
                put(&mut h, ns.as_bytes());
                put(&mut h, key.as_bytes());
                match value {
                    BatchValue::Bytes(b) => {
                        put(&mut h, b"bytes");
                        put(&mut h, b);
                    }
                    BatchValue::SeqTemplate { template, of } => {
                        put(&mut h, b"template");
                        put(&mut h, template);
                        put(&mut h, &(*of as u64).to_be_bytes());
                    }
                }
            }
            BatchOp::KvMaxSeq { ns, key, of } => {
                put(&mut h, b"KvMaxSeq");
                put(&mut h, ns.as_bytes());
                put(&mut h, key.as_bytes());
                put(&mut h, &(*of as u64).to_be_bytes());
            }
            BatchOp::KvMax { ns, key, value } => {
                put(&mut h, b"KvMax");
                put(&mut h, ns.as_bytes());
                put(&mut h, key.as_bytes());
                put(&mut h, &value.to_be_bytes());
            }
            BatchOp::CommitMarker { .. } => {}
        }
    }
    h.finalize().to_vec()
}

const RESULT_FORMAT: u8 = 1;
const STATUS_LOG_INSERTED: u8 = 1;
const STATUS_KV_APPLIED: u8 = 2;

fn put_varint(out: &mut Vec<u8>, mut n: u64) {
    loop {
        let b = (n & 0x7f) as u8;
        n >>= 7;
        if n == 0 {
            out.push(b);
            return;
        }
        out.push(b | 0x80);
    }
}

fn get_varint(b: &[u8], at: &mut usize) -> Result<u64> {
    let mut n = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *b
            .get(*at)
            .ok_or_else(|| anyhow::anyhow!("batch result: truncated"))?;
        *at += 1;
        n |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(n);
        }
    }
    anyhow::bail!("batch result: a varint is too long")
}

/// The outcome a commit marker stores: a format byte, the op count, then
/// per op its status (a log write inserted at a sequence, or a kv write
/// applied) and its sequence. At most [`MAX_BATCH_RESULT_BYTES`]; a batch of
/// [`MAX_BATCH_OPS`] ops encodes in at most 16,384 x 11 bytes plus a header.
pub fn encode_batch_result(
    ops: &[BatchOp],
    seqs: &[Option<u64>],
) -> Result<Vec<u8>> {
    anyhow::ensure!(
        ops.len() == seqs.len(),
        "batch result: ops and outcomes differ in number"
    );
    let mut out = vec![RESULT_FORMAT];
    put_varint(&mut out, ops.len() as u64);
    for (op, seq) in ops.iter().zip(seqs) {
        match (op, seq) {
            (
                BatchOp::LogAppendIfAbsent { .. }
                | BatchOp::LogAppendIdempotent { .. },
                Some(s),
            ) => {
                out.push(STATUS_LOG_INSERTED);
                put_varint(&mut out, *s);
            }
            (
                BatchOp::LogAppendIfAbsent { .. }
                | BatchOp::LogAppendIdempotent { .. },
                None,
            ) => {
                anyhow::bail!("batch result: a log write has no sequence")
            }
            (BatchOp::CommitMarker { .. }, _) => {
                anyhow::bail!("batch result: a marker is not an outcome")
            }
            _ => out.push(STATUS_KV_APPLIED),
        }
    }
    check_result_size(&out)?;
    Ok(out)
}

fn check_result_size(result: &[u8]) -> Result<()> {
    anyhow::ensure!(
        result.len() <= MAX_BATCH_RESULT_BYTES,
        "batch result of {} bytes exceeds the {MAX_BATCH_RESULT_BYTES}-byte limit",
        result.len()
    );
    Ok(())
}

/// The sequences a marker's `result` records for a batch of `n` ops.
pub fn decode_batch_result(
    result: &[u8],
    n: usize,
) -> Result<Vec<Option<u64>>> {
    check_result_size(result)?;
    anyhow::ensure!(
        result.first() == Some(&RESULT_FORMAT),
        "batch result: unknown format"
    );
    let mut at = 1;
    let count = get_varint(result, &mut at)? as usize;
    anyhow::ensure!(
        count == n,
        "batch result records {count} ops, the batch has {n}"
    );
    let mut seqs = Vec::with_capacity(n);
    for _ in 0..n {
        match result.get(at).copied() {
            Some(STATUS_LOG_INSERTED) => {
                at += 1;
                seqs.push(Some(get_varint(result, &mut at)?));
            }
            Some(STATUS_KV_APPLIED) => {
                at += 1;
                seqs.push(None);
            }
            other => anyhow::bail!("batch result: unknown status {other:?}"),
        }
    }
    anyhow::ensure!(at == result.len(), "batch result: trailing bytes");
    Ok(seqs)
}

/// The namespaces whose data batch preconditions read and whose writers
/// serialize on the store's write lock: the schema registry's, the source
/// lineage records and every activation, barrier and failover stream
/// (`schemas`, `schemas.v1` and `schemas.v1.*`, `schema_lineage`).
///
/// On PostgreSQL every writer of these namespaces holds the store lock for
/// its whole transaction, and a batch takes it before it reads anything, so
/// a precondition that holds when read holds until the batch commits.
/// Guarded keys never carry a TTL (the expiry sweep does not take the
/// lock).
pub fn is_guarded_namespace(ns: &str) -> bool {
    ns == "schemas"
        || ns == "schemas.v1"
        || ns.starts_with("schemas.v1.")
        || ns == "schema_lineage"
}

/// Writes decided from reads, committed atomically:
///
/// - every precondition is checked first, inside the transaction; any
///   failure writes nothing ([`BatchOutcome::Conflict`]);
/// - the log writes receive one contiguous range of global sequences, in
///   the order of `ops`, invisible until the commit;
/// - all writes commit together or none do.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AtomicBatch {
    pub preconditions: Vec<Precondition>,
    pub ops: Vec<BatchOp>,
}

/// The outcome of [`StorageBackend::apply_batch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchOutcome {
    /// The committed sequence of every op (`Some` for log writes).
    Committed { seqs: Vec<Option<u64>> },
    /// Nothing was written: a precondition no longer holds (or a log write's
    /// identity already exists).
    Conflict { why: String },
}

/// A commit marker whose stored outcome does not fit its batch: the store
/// did not behave atomically.
#[derive(Debug, thiserror::Error)]
#[error("atomic batch violated: {0}")]
pub struct AtomicityViolation(pub String);

impl AtomicBatch {
    /// Append a group of ops whose `of` indices are local to the group;
    /// returns the position of its first op in the batch.
    pub fn push_group(&mut self, ops: Vec<BatchOp>) -> usize {
        let base = self.ops.len();
        for op in ops {
            self.ops.push(match op {
                BatchOp::KvPut {
                    ns,
                    key,
                    value: BatchValue::SeqTemplate { template, of },
                } => BatchOp::KvPut {
                    ns,
                    key,
                    value: BatchValue::SeqTemplate {
                        template,
                        of: of + base,
                    },
                },
                BatchOp::KvMaxSeq { ns, key, of } => BatchOp::KvMaxSeq {
                    ns,
                    key,
                    of: of + base,
                },
                other => other,
            });
        }
        base
    }

    /// Check the batch's own shape before any backend work: every `of`
    /// names an earlier log write, and every template carries the sentinel
    /// exactly once.
    pub fn validate(&self) -> Result<()> {
        let sentinel = SEQ_SENTINEL.to_string();
        let marked =
            matches!(self.ops.last(), Some(BatchOp::CommitMarker { .. }));
        let ops = self.ops.len() - usize::from(marked);
        anyhow::ensure!(
            ops <= MAX_BATCH_OPS,
            "a batch of {ops} ops exceeds the {MAX_BATCH_OPS}-op limit"
        );
        for op in &self.ops {
            // A rendered template is never longer than its sentinel form.
            let item = match op {
                BatchOp::LogAppendIfAbsent { ns, key, value, .. }
                | BatchOp::LogAppendIdempotent { ns, key, value, .. } => {
                    Some((ns, key, value.len()))
                }
                BatchOp::KvPut { ns, key, value } => Some((
                    ns,
                    key,
                    match value {
                        BatchValue::Bytes(b) => b.len(),
                        BatchValue::SeqTemplate { template, .. } => {
                            template.len()
                        }
                    },
                )),
                _ => None,
            };
            if let Some((ns, key, size)) = item {
                check_state_item(ns, key, size)?;
            }
        }
        // A capture identity repeated within the request: identical bytes
        // collapse onto its first position, other bytes fail closed.
        canonical_log_ops(&self.ops)?;
        for (i, op) in self.ops.iter().enumerate() {
            let of = match op {
                BatchOp::CommitMarker { .. } => {
                    anyhow::ensure!(
                        i + 1 == self.ops.len(),
                        "batch op {i}: a commit marker must be the last op"
                    );
                    continue;
                }
                BatchOp::LogAppendIfAbsent { .. }
                | BatchOp::LogAppendIdempotent { .. } => continue,
                BatchOp::KvPut {
                    value: BatchValue::Bytes(_),
                    ..
                } => continue,
                BatchOp::KvPut {
                    value: BatchValue::SeqTemplate { template, of },
                    ..
                } => {
                    let text = String::from_utf8_lossy(template);
                    anyhow::ensure!(
                        text.matches(&sentinel).count() == 1,
                        "batch op {i}: the template must carry the sequence sentinel once"
                    );
                    *of
                }
                BatchOp::KvMaxSeq { of, .. } => *of,
                BatchOp::KvMax { .. } => continue,
            };
            anyhow::ensure!(
                of < i && self.ops[of].log_write().is_some(),
                "batch op {i}: `of` {of} is not an earlier log write"
            );
        }
        Ok(())
    }
}

impl BatchOp {
    /// A log write's stream, capture identity and bytes, and whether a
    /// stored identical entry answers it (`LogAppendIdempotent`).
    pub fn log_write(&self) -> Option<(&str, &str, &str, &[u8], bool)> {
        match self {
            BatchOp::LogAppendIfAbsent {
                ns,
                key,
                capture_id,
                value,
            } => Some((ns, key, capture_id, value, false)),
            BatchOp::LogAppendIdempotent {
                ns,
                key,
                capture_id,
                value,
            } => Some((ns, key, capture_id, value, true)),
            _ => None,
        }
    }
}

/// For each op, the position its log write collapses onto: its own, or the
/// first earlier write of the same `(ns, key, capture_id)` with identical
/// bytes (a repeated identity with other bytes is
/// `LogError::CaptureIdentityConflict`, refused before any statement). Ops
/// that are not log writes map to themselves.
pub(crate) fn canonical_log_ops(ops: &[BatchOp]) -> Result<Vec<usize>> {
    let mut first: std::collections::HashMap<(&str, &str, &str), usize> =
        std::collections::HashMap::new();
    let mut out = Vec::with_capacity(ops.len());
    for (i, op) in ops.iter().enumerate() {
        let Some((ns, key, capture_id, value, _)) = op.log_write() else {
            out.push(i);
            continue;
        };
        match first.get(&(ns, key, capture_id)) {
            Some(&j) => {
                let same = ops[j].log_write().is_some_and(|w| w.3 == value);
                if !same {
                    return Err(LogError::CaptureIdentityConflict {
                        capture_id: capture_id.to_string(),
                    }
                    .into());
                }
                out.push(j);
            }
            None => {
                first.insert((ns, key, capture_id), i);
                out.push(i);
            }
        }
    }
    Ok(out)
}

/// `template` with the sequence `seq` in place of the sentinel.
pub fn render_seq(template: &[u8], seq: u64) -> Vec<u8> {
    let text = String::from_utf8_lossy(template);
    text.replacen(&SEQ_SENTINEL.to_string(), &seq.to_string(), 1)
        .into_bytes()
}

/// The larger of a stored JSON number (absent: 0) and `seq`, as JSON.
pub fn max_seq_value(current: Option<&[u8]>, seq: u64) -> Result<Vec<u8>> {
    let cur = match current {
        Some(b) => serde_json::from_slice::<u64>(b).map_err(|e| {
            anyhow::anyhow!("a sequence value is not a number: {e}")
        })?,
        None => 0,
    };
    Ok(serde_json::to_vec(&cur.max(seq))?)
}

/// The result of [`commit_atomic`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitResult {
    /// Committed (now, or by an earlier attempt whose acknowledgement was
    /// lost): the sequence of every op.
    Committed(Vec<Option<u64>>),
    /// A precondition failed: re-read, decide again and retry.
    Conflict(String),
}

/// Commit `batch`, resolving ambiguous outcomes from its durable commit
/// identity. The batch gets a random id and its request digest
/// ([`batch_digest`]) once, and every attempt writes them, with the outcome
/// of every op, as its last write ([`BatchOp::CommitMarker`]). After an
/// `apply_batch` error, only the marker decides:
///
/// - a marker with the same digest: the batch committed, and its stored
///   sequences are returned (later writers may since have changed its kv
///   values; that never matters);
/// - no marker: it did not commit, and it is applied again (at most
///   `retries` more times);
/// - a marker with another digest: [`BatchIdentityConflict`], never retried;
/// - a stored outcome that does not fit the batch: [`AtomicityViolation`].
///
/// The whole call, every attempt included, ends within [`COMMIT_DEADLINE`].
pub async fn commit_atomic(
    be: &dyn StorageBackend,
    batch: &AtomicBatch,
    retries: usize,
) -> Result<CommitResult> {
    commit_atomic_counted(be, batch, retries, &mut 0).await
}

/// [`commit_atomic`], adding to `applies` every `apply_batch` it makes
/// (one when the first succeeds or conflicts).
pub async fn commit_atomic_counted(
    be: &dyn StorageBackend,
    batch: &AtomicBatch,
    retries: usize,
    applies: &mut usize,
) -> Result<CommitResult> {
    commit_atomic_within(be, batch, retries, applies, COMMIT_DEADLINE).await
}

/// [`commit_atomic_counted`] with an explicit deadline (tests).
pub(crate) async fn commit_atomic_within(
    be: &dyn StorageBackend,
    batch: &AtomicBatch,
    retries: usize,
    applies: &mut usize,
    deadline: std::time::Duration,
) -> Result<CommitResult> {
    batch.validate()?;
    anyhow::ensure!(
        !batch
            .ops
            .iter()
            .any(|o| matches!(o, BatchOp::CommitMarker { .. })),
        "a batch's commit marker is added by commit_atomic only"
    );
    // One identity for every attempt of this logical batch.
    let batch_id = uuid::Uuid::new_v4().simple().to_string();
    let digest = batch_digest(batch);
    let mut marked = batch.clone();
    marked.ops.push(BatchOp::CommitMarker {
        batch_id: batch_id.clone(),
        digest: digest.clone(),
    });
    let attempts = commit_attempts(
        be,
        &marked,
        batch.ops.len(),
        &batch_id,
        &digest,
        retries,
        applies,
    );
    match tokio::time::timeout(deadline, attempts).await {
        Ok(result) => result,
        Err(_) => Err(anyhow::anyhow!(
            "atomic batch {batch_id}: the {deadline:?} commit deadline passed; outcome unknown"
        )),
    }
}

async fn commit_attempts(
    be: &dyn StorageBackend,
    marked: &AtomicBatch,
    n: usize,
    batch_id: &str,
    digest: &[u8],
    retries: usize,
    applies: &mut usize,
) -> Result<CommitResult> {
    let mut attempt = 0;
    loop {
        *applies += 1;
        let err = match be.apply_batch(marked).await {
            Ok(BatchOutcome::Committed { mut seqs }) => {
                seqs.truncate(n);
                return Ok(CommitResult::Committed(seqs));
            }
            Ok(BatchOutcome::Conflict { why }) => {
                return Ok(CommitResult::Conflict(why));
            }
            Err(e) => e,
        };
        // Refused before any write: nothing to resolve, never retried.
        if err.chain().any(|c| {
            c.downcast_ref::<LogError>().is_some()
                || c.downcast_ref::<StateItemTooLarge>().is_some()
        }) {
            return Err(err);
        }
        attempt += 1;
        // Only the marker decides; its lookup may fail transiently and is
        // retried, never skipped.
        let mut reads = 0;
        let found = loop {
            match be.batch_commit_get(batch_id).await {
                Ok(found) => break found,
                Err(e) if reads < retries => {
                    reads += 1;
                    tracing::warn!(error = %e, "atomic batch: marker lookup failed; retrying it");
                }
                Err(e) => {
                    return Err(e.context(format!(
                        "atomic batch {batch_id}: outcome unknown after the error {err:#}"
                    )));
                }
            }
        };
        match found {
            Some(marker) if marker.digest == digest => {
                return match decode_batch_result(&marker.result, n) {
                    Ok(seqs) => Ok(CommitResult::Committed(seqs)),
                    Err(e) => Err(AtomicityViolation(format!(
                        "marker {batch_id}: {e:#}"
                    ))
                    .into()),
                };
            }
            Some(_) => {
                return Err(BatchIdentityConflict(batch_id.into()).into());
            }
            None if attempt <= retries => {
                tracing::warn!(error = %err, attempt, "atomic batch: not committed; applying it again");
            }
            None => {
                return Err(err.context(format!(
                    "atomic batch: not committed after {attempt} attempts"
                )));
            }
        }
    }
}

/// One marker sweep run's result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarkerSweep {
    pub deleted: usize,
    pub chunks: usize,
    /// Expired markers remained when the run's budget ended.
    pub backlog: bool,
}

/// Delete commit markers older than [`BATCH_MARKER_RETENTION_MS`]: chunks
/// of at most [`BATCH_MARKER_SWEEP_CHUNK`], each its own transaction (one
/// store-lock acquisition), at most [`BATCH_MARKER_SWEEP_MAX_CHUNKS`] chunks
/// or [`BATCH_MARKER_SWEEP_BUDGET`] per run. Falling behind is harmless: a
/// marker is read only by its own commit call, within its deadline.
pub async fn sweep_batch_markers(
    be: &dyn StorageBackend,
    now_ms: i64,
) -> Result<MarkerSweep> {
    sweep_batch_markers_within(
        be,
        now_ms - BATCH_MARKER_RETENTION_MS,
        BATCH_MARKER_SWEEP_MAX_CHUNKS,
        BATCH_MARKER_SWEEP_BUDGET,
    )
    .await
}

pub(crate) async fn sweep_batch_markers_within(
    be: &dyn StorageBackend,
    before_ms: i64,
    max_chunks: usize,
    budget: std::time::Duration,
) -> Result<MarkerSweep> {
    let started = std::time::Instant::now();
    let mut run = MarkerSweep {
        deleted: 0,
        chunks: 0,
        backlog: false,
    };
    while run.chunks < max_chunks && started.elapsed() < budget {
        let n = be
            .batch_commit_sweep(before_ms, BATCH_MARKER_SWEEP_CHUNK)
            .await?;
        run.chunks += 1;
        run.deleted += n;
        run.backlog = n == BATCH_MARKER_SWEEP_CHUNK;
        if !run.backlog {
            break;
        }
    }
    metrics::counter!("deltaforge_store_batch_markers_deleted_total")
        .increment(run.deleted as u64);
    metrics::gauge!("deltaforge_store_batch_markers_backlog")
        .set(if run.backlog { 1.0 } else { 0.0 });
    metrics::histogram!("deltaforge_store_batch_marker_sweep_seconds")
        .record(started.elapsed().as_secs_f64());
    Ok(run)
}

/// Test-only fault points of `apply_batch` (each backend honors them).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchFault {
    /// Fail before the transaction starts.
    BeforeTransaction,
    /// Fail after this many writes are staged, before the commit.
    AfterWrites(usize),
    /// Commit, then report an error (an acknowledgement lost).
    AfterCommit,
}

/// Fault injection into `apply_batch`, for the conformance suite.
pub trait BatchFaults {
    fn set_batch_fault(&self, fault: Option<BatchFault>);
}

pub type ArcStorageBackend = Arc<dyn StorageBackend>;

/// KV namespace of source checkpoints (see [`adapters::BackendCheckpointStore`]).
pub const CHECKPOINTS_NS: &str = "checkpoints";

/// Whether an idempotent append inserted a new entry or matched an existing one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendStatus {
    Inserted,
    AlreadyPresent,
}

/// Outcome of [`StorageBackend::log_append_if_absent`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogAppendOutcome {
    pub seq: u64,
    pub status: AppendStatus,
}

/// One slot record returned by [`StorageBackend::slot_list`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotRecord {
    pub key: String,
    /// The backend slot version (authoritative CAS token for `slot_cas`).
    pub version: u64,
    pub value: Vec<u8>,
}

/// A bounded page of [`SlotRecord`]s plus an opaque cursor for the next page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotPage {
    pub records: Vec<SlotRecord>,
    /// Opaque cursor to pass back as `cursor` for the next page; `None` when the
    /// scan is complete. Treat as opaque - only ever pass a value `slot_list`
    /// returned.
    pub next_cursor: Option<String>,
}

/// Typed errors for the slot primitives.
#[derive(Debug, thiserror::Error)]
pub enum SlotError {
    /// A `slot_list` cursor is not a well-formed token (bad tag/hex/checksum), so it
    /// was not produced by `slot_list` or was corrupted in transit. The checksum is
    /// **corruption detection only** - it is unkeyed and not authenticated, so it
    /// does not defend against a deliberately forged cursor.
    #[error("malformed slot cursor")]
    MalformedCursor,
    /// A well-formed cursor whose bound namespace/prefix does not match the current
    /// `slot_list` call: it belongs to a different scan and must not be reused here.
    #[error("slot cursor namespace/prefix mismatch")]
    CursorMismatch,
}

/// Upper bound on a single `slot_list` page, so a caller-supplied `limit` can never
/// request an unbounded scan.
pub const SLOT_LIST_MAX_LIMIT: usize = 4096;

/// Versioned-cursor tag.
const SLOT_CURSOR_TAG: &str = "slc1:";

/// The scan a cursor belongs to, plus its exclusive last key. Bound into the cursor
/// so a cursor cannot be silently reused across a different namespace or prefix.
#[derive(serde::Serialize, serde::Deserialize)]
struct SlotCursorPayload {
    ns: String,
    /// Normalized prefix (`None` prefix normalizes to `""`).
    prefix: String,
    /// Exclusive last key of the page that produced this cursor.
    key: String,
}

fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn from_hex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

/// 8-byte unkeyed checksum over the cursor payload, for **corruption detection**
/// (not authentication - it is trivially recomputable, so it does not make a cursor
/// unforgeable).
fn cursor_checksum(payload: &[u8]) -> [u8; 8] {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(payload);
    let mut out = [0u8; 8];
    out.copy_from_slice(&digest[..8]);
    out
}

/// Encode a versioned `slot_list` cursor binding `(ns, normalized prefix, exclusive
/// last key)` with a corruption-detection checksum.
pub(crate) fn encode_slot_cursor(
    ns: &str,
    prefix_norm: &str,
    key: &str,
) -> String {
    let payload = serde_json::to_vec(&SlotCursorPayload {
        ns: ns.to_string(),
        prefix: prefix_norm.to_string(),
        key: key.to_string(),
    })
    .expect("cursor payload serializes");
    let sum = cursor_checksum(&payload);
    format!("{SLOT_CURSOR_TAG}{}:{}", to_hex(&sum), to_hex(&payload))
}

/// Decode and validate a `slot_list` cursor against the current `(ns, normalized
/// prefix)`, returning the exclusive last key. A token that does not parse or whose
/// checksum fails is [`SlotError::MalformedCursor`]; a well-formed token bound to a
/// different scan is [`SlotError::CursorMismatch`].
pub(crate) fn decode_slot_cursor(
    cursor: &str,
    ns: &str,
    prefix_norm: &str,
) -> Result<String> {
    let rest = cursor
        .strip_prefix(SLOT_CURSOR_TAG)
        .ok_or(SlotError::MalformedCursor)?;
    let (sum_hex, payload_hex) =
        rest.split_once(':').ok_or(SlotError::MalformedCursor)?;
    let sum = from_hex(sum_hex).ok_or(SlotError::MalformedCursor)?;
    let payload = from_hex(payload_hex).ok_or(SlotError::MalformedCursor)?;
    if sum.len() != 8 || cursor_checksum(&payload)[..] != sum[..] {
        return Err(SlotError::MalformedCursor.into());
    }
    let p: SlotCursorPayload = serde_json::from_slice(&payload)
        .map_err(|_| SlotError::MalformedCursor)?;
    if p.ns != ns || p.prefix != prefix_norm {
        return Err(SlotError::CursorMismatch.into());
    }
    Ok(p.key)
}

/// The exclusive upper-bound key for "starts with `prefix`": the smallest string
/// greater than every string beginning with `prefix`. `None` when `prefix` is empty
/// or is all `char::MAX` (no finite upper bound - scan to the end). Byte-wise /
/// scalar order, matching Rust `str` ordering, so SQL range scans agree with the
/// in-memory `starts_with`.
pub(crate) fn prefix_successor(prefix: &str) -> Option<String> {
    let mut chars: Vec<char> = prefix.chars().collect();
    while let Some(&last) = chars.last() {
        let mut n = last as u32 + 1;
        // Skip the UTF-16 surrogate gap so the result is a valid scalar.
        if (0xD800..=0xDFFF).contains(&n) {
            n = 0xE000;
        }
        if let Some(nc) = char::from_u32(n) {
            *chars.last_mut().expect("non-empty") = nc;
            return Some(chars.into_iter().collect());
        }
        chars.pop(); // last was char::MAX; carry to the previous char
    }
    None
}

/// Typed storage errors surfaced through `anyhow` for callers to downcast.
#[derive(Debug, thiserror::Error)]
pub enum LogError {
    /// A different value was appended under an existing `capture_id`. The stored
    /// entry is never overwritten.
    #[error(
        "capture identity '{capture_id}' already present with different bytes"
    )]
    CaptureIdentityConflict { capture_id: String },
}

/// Retention request for [`StorageBackend::log_truncate`]. Removal candidates are
/// entries with `seq < pin_seq` AND (older than `older_than_ms` OR beyond a capacity
/// cap counted oldest-first); removal is a contiguous oldest prefix.
#[derive(Debug, Clone, Copy)]
pub struct LogTruncateRequest {
    /// Absolute cutoff in unix milliseconds: remove entries appended strictly before
    /// it. The caller computes `now - max_age`; the backend does no clock math.
    /// `None` disables age-based removal.
    pub older_than_ms: Option<i64>,
    /// Hard floor: never remove an entry with `seq >= pin_seq` (the minimum sequence
    /// still needed by any pinning replay job). Use `u64::MAX` when nothing pins.
    pub pin_seq: u64,
    /// Optional capacity caps, enforced oldest-first but never past `pin_seq`.
    pub max_entries: Option<u64>,
    pub max_bytes: Option<u64>,
}

/// Outcome of [`StorageBackend::log_truncate`], committed atomically with the removal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogTruncateOutcome {
    pub removed: usize,
    pub removed_bytes: u64,
    /// Oldest seq still retained after truncation (`None` if the stream is now empty).
    pub oldest_seq: Option<u64>,
    /// Highest seq removed from THIS stream by this call (`None` if none removed).
    pub highest_removed_seq: Option<u64>,
    /// The durable exclusive-cursor horizon after this call. A replay `from_seq`
    /// below this is invalid; `from_seq == min_valid_from_seq` is valid.
    pub min_valid_from_seq: u64,
    /// Highest seq ever appended to this stream (never rewinds).
    pub head_seq: u64,
    /// A capacity cap could not be honored because `pin_seq` blocked further removal.
    pub capacity_pinned: bool,
}

/// Durable metadata for one log stream, from [`StorageBackend::log_stream_meta`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogStreamMeta {
    /// Exclusive-cursor horizon: the highest seq ever removed from this stream (0 if
    /// none). `from_seq < min_valid_from_seq` is invalid; `==` is valid. Survives an
    /// empty stream, so "all truncated" (`> 0`, empty) differs from "never had data"
    /// (`0`, `head_seq == 0`).
    pub min_valid_from_seq: u64,
    /// Oldest seq currently retained (`None` if empty).
    pub oldest_seq: Option<u64>,
    /// Highest seq ever appended to this stream (never rewinds).
    pub head_seq: u64,
    /// Number of entries currently retained.
    pub len: u64,
}

/// The backend-computed content digest over the exact stored bytes. Delegates to
/// `deltaforge_core::replay::content_digest` so the backend that writes it and the
/// replay reader that verifies it share one implementation. The backend derives it from
/// the bytes it stores and never trusts a caller-supplied digest.
pub(crate) fn content_digest(value: &[u8]) -> String {
    deltaforge_core::replay::content_digest(value)
}

/// One log entry with the metadata the replay reader needs for fail-closed loads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntryMeta {
    pub seq: u64,
    /// Append time in unix milliseconds (falls back to `ts * 1000` for pre-ms rows).
    pub stored_at_ms: i64,
    pub capture_id: Option<String>,
    pub content_hash: Option<String>,
    pub value: Vec<u8>,
}

/// Shared contract suite for the replay log primitives, run against every backend.
#[cfg(test)]
pub(crate) mod log_contract_suite {
    use super::*;

    fn is_conflict(err: &anyhow::Error) -> bool {
        matches!(
            err.downcast_ref::<LogError>(),
            Some(LogError::CaptureIdentityConflict { .. })
        )
    }

    /// Identical (capture_id, value) is idempotent: one entry, stable seq.
    pub async fn idempotency(be: Arc<dyn StorageBackend>, ns: &str) {
        let a = be
            .log_append_if_absent(ns, "s:replay", "cap-1", b"payload-1")
            .await
            .unwrap();
        assert_eq!(a.status, AppendStatus::Inserted);
        let b = be
            .log_append_if_absent(ns, "s:replay", "cap-1", b"payload-1")
            .await
            .unwrap();
        assert_eq!(b.status, AppendStatus::AlreadyPresent);
        assert_eq!(a.seq, b.seq, "identical retry must return the same seq");
        assert_eq!(be.log_since(ns, "s:replay", 0).await.unwrap().len(), 1);
    }

    /// Same capture_id with different bytes is rejected; the original is untouched.
    pub async fn conflict_rejected(be: Arc<dyn StorageBackend>, ns: &str) {
        let a = be
            .log_append_if_absent(ns, "s:replay", "cap-1", b"original")
            .await
            .unwrap();
        let err = be
            .log_append_if_absent(ns, "s:replay", "cap-1", b"tampered")
            .await
            .unwrap_err();
        assert!(is_conflict(&err), "expected CaptureIdentityConflict: {err}");
        let entries = be.log_since(ns, "s:replay", 0).await.unwrap();
        assert_eq!(entries, vec![(a.seq, b"original".to_vec())]);
    }

    /// The horizon is the highest seq actually removed from THIS stream, not
    /// `oldest_retained - 1` - global sequence gaps belong to other keys.
    pub async fn horizon_with_global_gaps(
        be: Arc<dyn StorageBackend>,
        ns: &str,
    ) {
        // Interleave appends to another key so the target stream has global gaps.
        be.log_append(ns, "other", b"x").await.unwrap();
        let s1 = be
            .log_append_if_absent(ns, "s:replay", "c1", b"a")
            .await
            .unwrap()
            .seq;
        be.log_append(ns, "other", b"x").await.unwrap();
        let s2 = be
            .log_append_if_absent(ns, "s:replay", "c2", b"b")
            .await
            .unwrap()
            .seq;
        be.log_append(ns, "other", b"x").await.unwrap();
        let s3 = be
            .log_append_if_absent(ns, "s:replay", "c3", b"c")
            .await
            .unwrap()
            .seq;
        assert!(s1 < s2 && s2 < s3);
        // Remove everything below s3 (pin protects s3), by age (cutoff in the future).
        let out = be
            .log_truncate(
                ns,
                "s:replay",
                LogTruncateRequest {
                    older_than_ms: Some(i64::MAX),
                    pin_seq: s3,
                    max_entries: None,
                    max_bytes: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(out.removed, 2);
        assert_eq!(out.highest_removed_seq, Some(s2));
        assert_eq!(
            out.min_valid_from_seq, s2,
            "horizon = highest removed, not oldest-1"
        );
        assert_eq!(out.oldest_seq, Some(s3));
        // Exclusive cursor: from_seq == min_valid_from_seq returns the oldest retained.
        let from_horizon = be
            .log_since(ns, "s:replay", out.min_valid_from_seq)
            .await
            .unwrap();
        assert_eq!(from_horizon, vec![(s3, b"c".to_vec())]);
    }

    /// Retention never removes an entry with seq >= pin_seq; a capacity cap that
    /// cannot be honored sets `capacity_pinned`.
    pub async fn pin_invariant(be: Arc<dyn StorageBackend>, ns: &str) {
        let mut seqs = Vec::new();
        for i in 0..5u8 {
            seqs.push(
                be.log_append_if_absent(ns, "s:replay", &format!("c{i}"), &[i])
                    .await
                    .unwrap()
                    .seq,
            );
        }
        // Pin at the 3rd entry: only the first two are removable.
        let out = be
            .log_truncate(
                ns,
                "s:replay",
                LogTruncateRequest {
                    older_than_ms: None,
                    pin_seq: seqs[2],
                    max_entries: Some(1), // wants to remove 4, but pin blocks past 2
                    max_bytes: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(out.removed, 2, "never removes seq >= pin_seq");
        assert!(
            out.capacity_pinned,
            "cap could not be honored under the pin"
        );
        let remaining: Vec<u64> = be
            .log_since(ns, "s:replay", 0)
            .await
            .unwrap()
            .into_iter()
            .map(|(s, _)| s)
            .collect();
        assert_eq!(remaining, vec![seqs[2], seqs[3], seqs[4]]);
    }

    /// A truncated-empty stream keeps its horizon, distinguishing it from a stream
    /// that never held data.
    pub async fn empty_vs_truncated(be: Arc<dyn StorageBackend>, ns: &str) {
        // Never-had-data.
        let fresh = be.log_stream_meta(ns, "s:fresh").await.unwrap();
        assert_eq!(fresh.min_valid_from_seq, 0);
        assert_eq!(fresh.head_seq, 0);
        assert_eq!(fresh.oldest_seq, None);
        assert_eq!(fresh.len, 0);

        let s1 = be
            .log_append_if_absent(ns, "s:t", "c1", b"a")
            .await
            .unwrap()
            .seq;
        let s2 = be
            .log_append_if_absent(ns, "s:t", "c2", b"b")
            .await
            .unwrap()
            .seq;
        let out = be
            .log_truncate(
                ns,
                "s:t",
                LogTruncateRequest {
                    older_than_ms: None,
                    pin_seq: u64::MAX,
                    max_entries: Some(0),
                    max_bytes: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(out.removed, 2);
        assert_eq!(out.oldest_seq, None);
        let meta = be.log_stream_meta(ns, "s:t").await.unwrap();
        assert_eq!(meta.oldest_seq, None);
        assert_eq!(meta.len, 0);
        assert_eq!(meta.min_valid_from_seq, s2, "horizon preserved when empty");
        assert_eq!(meta.head_seq, s2, "head never rewinds");
        assert!(s1 < s2);
    }

    /// Concurrent identical appends collapse to one entry; concurrent same-id
    /// different-value appends yield exactly one insert and one conflict.
    pub async fn concurrent_appends(be: Arc<dyn StorageBackend>, ns: &str) {
        // Owned copies so the spawned ('static) tasks capture no borrows.
        let ns = ns.to_string();
        // Identical.
        let (b1, b2) = (Arc::clone(&be), Arc::clone(&be));
        let (n1, n2) = (ns.clone(), ns.clone());
        let (r1, r2) = tokio::join!(
            tokio::spawn(async move {
                b1.log_append_if_absent(&n1, "s:c", "cap", b"same").await
            }),
            tokio::spawn(async move {
                b2.log_append_if_absent(&n2, "s:c", "cap", b"same").await
            }),
        );
        let (o1, o2) = (r1.unwrap().unwrap(), r2.unwrap().unwrap());
        assert_eq!(o1.seq, o2.seq);
        let inserts = [o1.status, o2.status]
            .iter()
            .filter(|s| **s == AppendStatus::Inserted)
            .count();
        assert_eq!(
            inserts, 1,
            "exactly one insert for identical concurrent appends"
        );
        assert_eq!(be.log_since(&ns, "s:c", 0).await.unwrap().len(), 1);

        // Same id, different value.
        let (b3, b4) = (Arc::clone(&be), Arc::clone(&be));
        let (n3, n4) = (ns.clone(), ns.clone());
        let (r3, r4) = tokio::join!(
            tokio::spawn(async move {
                b3.log_append_if_absent(&n3, "s:d", "cap", b"one").await
            }),
            tokio::spawn(async move {
                b4.log_append_if_absent(&n4, "s:d", "cap", b"two").await
            }),
        );
        let results = [r3.unwrap(), r4.unwrap()];
        let oks = results.iter().filter(|r| r.is_ok()).count();
        let conflicts = results
            .iter()
            .filter(|r| r.as_ref().err().is_some_and(is_conflict))
            .count();
        assert_eq!(oks, 1, "exactly one insert wins");
        assert_eq!(conflicts, 1, "the other is a conflict");
        assert_eq!(be.log_since(&ns, "s:d", 0).await.unwrap().len(), 1);
    }

    /// `log_ns_max_seq` sees only its own namespace and returns 0 when empty.
    pub async fn ns_max_seq_scoped(be: Arc<dyn StorageBackend>, ns: &str) {
        let other = format!("{ns}_other");
        assert_eq!(be.log_ns_max_seq(ns).await.unwrap(), 0);
        be.log_append(ns, "a", b"1").await.unwrap();
        let top = be.log_append(ns, "b", b"2").await.unwrap();
        be.log_append(&other, "x", b"3").await.unwrap();
        assert_eq!(be.log_ns_max_seq(ns).await.unwrap(), top);
        assert!(be.log_ns_max_seq(&other).await.unwrap() > top);
    }
}

/// Shared contract for `kv_list` prefix semantics, run against every backend
/// (Memory, SQLite in-crate; PostgreSQL env-gated).
#[cfg(test)]
pub(crate) mod kv_list_contract_suite {
    use super::*;

    /// `kv_list` with a prefix is a literal, byte-wise, case-sensitive
    /// "starts with": SQL wildcard and escape characters in the prefix match
    /// only themselves, and a prefix never matches a longer sibling id's keys
    /// unless it is literally their prefix.
    pub async fn literal_prefix(be: Arc<dyn StorageBackend>, ns: &str) {
        let keys = [
            "orders::sink::kafka",
            "orders::sink::s3",
            "orders-archive::sink::kafka",
            "ORDERS::sink::kafka",
            "ord%rs::sink::a",
            "ordXrs::sink::a",
            "ord_rs::sink::a",
            "ordYrs::sink::a",
            "a\\b::x",
            "aXb::x",
            "a!b::x",
        ];
        for k in keys {
            be.kv_put(ns, k, b"v").await.unwrap();
        }
        be.kv_put(&format!("{ns}_other"), "orders::sink::kafka", b"v")
            .await
            .unwrap();
        for prefix in [
            "orders::sink::",
            "orders",
            "ORDERS",
            "ord%",
            "ord%rs::",
            "ord_",
            "ord_rs::",
            "a\\",
            "a!",
            "",
            "zzz",
        ] {
            let mut want: Vec<String> = keys
                .iter()
                .filter(|k| k.starts_with(prefix))
                .map(|k| k.to_string())
                .collect();
            want.sort();
            let got = be.kv_list(ns, Some(prefix)).await.unwrap();
            assert_eq!(got, want, "prefix {prefix:?}");
        }
    }
}

/// Shared contract suite for the keyset-paginated `slot_list` primitive, run against
/// every backend (Memory, SQLite in-crate; PostgreSQL env-gated).
#[cfg(test)]
mod batch_commit_identity_tests {
    use super::*;
    use crate::adapters::test_util::FaultBackend;

    /// A batch whose acknowledgement was lost committed; another valid
    /// writer then changed one of its keys before the outcome was resolved.
    /// The batch is still reported committed, with its own sequences: later
    /// key values never decide whether a transaction committed.
    #[tokio::test]
    async fn a_lost_acknowledgement_resolves_as_committed_after_a_later_write()
    {
        let mem = Arc::new(MemoryStorageBackend::new());
        let fb = FaultBackend::wrap(mem.clone());
        let batch = AtomicBatch {
            preconditions: Vec::new(),
            ops: vec![
                BatchOp::LogAppendIfAbsent {
                    ns: "schemas.v1".into(),
                    key: "s".into(),
                    capture_id: "c1".into(),
                    value: b"entry".to_vec(),
                },
                BatchOp::KvPut {
                    ns: "schemas.v1.index".into(),
                    key: "k".into(),
                    value: BatchValue::Bytes(b"batch".to_vec()),
                },
            ],
        };
        mem.set_batch_fault(Some(BatchFault::AfterCommit));
        *fb.write_after_batch_error.lock().unwrap() =
            Some(("schemas.v1.index".into(), "k".into(), b"later".to_vec()));
        let out = commit_atomic(&fb, &batch, 3).await.unwrap();
        let committed_seq = mem
            .log_find_capture("schemas.v1", "s", "c1")
            .await
            .unwrap()
            .unwrap()
            .0;
        assert_eq!(
            out,
            CommitResult::Committed(vec![Some(committed_seq), None])
        );
        assert_eq!(
            mem.kv_get("schemas.v1.index", "k")
                .await
                .unwrap()
                .as_deref(),
            Some(&b"later"[..])
        );
    }

    fn kv_ops(n: usize) -> AtomicBatch {
        AtomicBatch {
            preconditions: Vec::new(),
            ops: (0..n)
                .map(|i| BatchOp::KvPut {
                    ns: "ns".into(),
                    key: format!("k{i}"),
                    value: BatchValue::Bytes(vec![1]),
                })
                .collect(),
        }
    }

    /// At most `MAX_BATCH_OPS` ops; the commit marker is not counted and
    /// must be last.
    #[test]
    fn a_batch_holds_at_most_the_op_limit() {
        assert!(kv_ops(MAX_BATCH_OPS).validate().is_ok());
        let mut marked = kv_ops(MAX_BATCH_OPS);
        marked.ops.push(BatchOp::CommitMarker {
            batch_id: "b".into(),
            digest: vec![],
        });
        assert!(marked.validate().is_ok());
        let err = kv_ops(MAX_BATCH_OPS + 1).validate().unwrap_err();
        assert!(format!("{err:#}").contains("16384-op limit"), "{err:#}");
        let mut early = kv_ops(2);
        early.ops.insert(
            0,
            BatchOp::CommitMarker {
                batch_id: "b".into(),
                digest: vec![],
            },
        );
        assert!(early.validate().is_err());
    }

    /// The largest batch's outcome encodes well within the result limit, and
    /// a result over the limit is refused when encoded and when decoded.
    #[test]
    fn a_stored_outcome_is_bounded() {
        let ops: Vec<BatchOp> = (0..MAX_BATCH_OPS)
            .map(|i| BatchOp::LogAppendIfAbsent {
                ns: "ns".into(),
                key: "s".into(),
                capture_id: format!("c{i}"),
                value: vec![],
            })
            .collect();
        let seqs: Vec<Option<u64>> =
            (0..MAX_BATCH_OPS).map(|_| Some(u64::MAX)).collect();
        let result = encode_batch_result(&ops, &seqs).unwrap();
        assert!(result.len() <= MAX_BATCH_OPS * 11 + 4, "{}", result.len());
        assert!(result.len() < MAX_BATCH_RESULT_BYTES);
        assert_eq!(decode_batch_result(&result, MAX_BATCH_OPS).unwrap(), seqs);
        assert!(
            check_result_size(&vec![0; MAX_BATCH_RESULT_BYTES + 1]).is_err()
        );
        assert!(
            decode_batch_result(
                &vec![RESULT_FORMAT; MAX_BATCH_RESULT_BYTES + 1],
                1
            )
            .is_err()
        );
    }

    /// A store that stops answering cannot hold a commit call past its
    /// deadline; the deadline is far shorter than the marker retention.
    #[tokio::test]
    async fn a_commit_call_ends_at_its_deadline() {
        let fb = FaultBackend::new();
        fb.stall_batches
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let started = std::time::Instant::now();
        let err = commit_atomic_within(
            &fb,
            &kv_ops(1),
            3,
            &mut 0,
            std::time::Duration::from_millis(50),
        )
        .await
        .unwrap_err();
        assert!(format!("{err:#}").contains("commit deadline"), "{err:#}");
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
        assert_eq!(COMMIT_DEADLINE, std::time::Duration::from_secs(300));
        assert!(
            BATCH_MARKER_RETENTION_MS
                >= 100 * COMMIT_DEADLINE.as_millis() as i64
        );
    }
}

#[cfg(test)]
pub(crate) mod slot_list_contract_suite {
    use super::*;

    /// Drain every page for `(prefix)` at `page_size`, returning the keys in the
    /// order observed. Bounded by a generous iteration cap so a pagination bug
    /// fails loudly instead of hanging.
    async fn drain_keys(
        be: &Arc<dyn StorageBackend>,
        ns: &str,
        prefix: Option<&str>,
        page_size: usize,
    ) -> Vec<String> {
        let mut out = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..10_000 {
            let page = be
                .slot_list(ns, prefix, cursor.as_deref(), page_size)
                .await
                .unwrap();
            assert!(page.records.len() <= page_size, "page must be bounded");
            out.extend(page.records.into_iter().map(|r| r.key));
            match page.next_cursor {
                Some(c) => cursor = Some(c),
                None => return out,
            }
        }
        panic!("slot_list pagination did not terminate");
    }

    /// Slots are scoped to their namespace; a list never bleeds across namespaces.
    pub async fn namespace_isolation(be: Arc<dyn StorageBackend>, ns: &str) {
        let a = format!("{ns}_a");
        let b = format!("{ns}_b");
        be.slot_upsert(&a, "k1", b"a1").await.unwrap();
        be.slot_upsert(&a, "k2", b"a2").await.unwrap();
        be.slot_upsert(&b, "k1", b"b1").await.unwrap();
        let keys = drain_keys(&be, &a, None, 10).await;
        assert_eq!(keys, vec!["k1".to_string(), "k2".to_string()]);
        let page = be.slot_list(&a, None, None, 10).await.unwrap();
        // Values come from namespace `a`, not `b`.
        let v = &page.records.iter().find(|r| r.key == "k1").unwrap().value;
        assert_eq!(v, b"a1");
    }

    /// Prefix is a byte-wise "starts with", case-sensitive, and excludes keys that
    /// merely share a shorter prefix.
    pub async fn prefix_correctness(be: Arc<dyn StorageBackend>, ns: &str) {
        for (k, v) in [
            ("app:1", b"1".as_slice()),
            ("app:2", b"2"),
            ("apple", b"3"), // starts with "app" but NOT "app:"
            ("App:9", b"4"), // different case
            ("zoo", b"5"),
        ] {
            be.slot_upsert(ns, k, v).await.unwrap();
        }
        assert_eq!(
            drain_keys(&be, ns, Some("app:"), 10).await,
            vec!["app:1".to_string(), "app:2".to_string()]
        );
        assert_eq!(
            drain_keys(&be, ns, Some("app"), 10).await,
            vec![
                "app:1".to_string(),
                "app:2".to_string(),
                "apple".to_string()
            ]
        );
        // A wildcard-like prefix is a literal prefix, not a pattern.
        assert!(drain_keys(&be, ns, Some("%"), 10).await.is_empty());
    }

    /// Paging across multiple pages yields every key exactly once, in order.
    pub async fn pagination_without_duplicates(
        be: Arc<dyn StorageBackend>,
        ns: &str,
    ) {
        let mut expected: Vec<String> = Vec::new();
        for i in 0..20u32 {
            let k = format!("key{i:03}");
            be.slot_upsert(ns, &k, k.as_bytes()).await.unwrap();
            expected.push(k);
        }
        expected.sort();
        let got = drain_keys(&be, ns, Some("key"), 7).await;
        assert_eq!(got, expected, "every key exactly once, ascending");
        let mut dedup = got.clone();
        dedup.dedup();
        assert_eq!(dedup.len(), got.len(), "no duplicates across pages");
    }

    /// A scan tolerates concurrent create/change/delete: no crash, no duplicates,
    /// and every key that existed for the whole scan is observed.
    pub async fn concurrent_mutation_tolerance(
        be: Arc<dyn StorageBackend>,
        ns: &str,
    ) {
        for i in 0..10u32 {
            let k = format!("key{i}");
            be.slot_upsert(ns, &k, b"v").await.unwrap();
        }
        // First page.
        let p1 = be.slot_list(ns, None, None, 3).await.unwrap();
        assert_eq!(p1.records.len(), 3);
        let mut seen: Vec<String> =
            p1.records.iter().map(|r| r.key.clone()).collect();

        // Mutate mid-scan: delete an unseen key, change a seen key, add a new key.
        assert!(be.slot_delete(ns, "key9").await.unwrap()); // unseen -> gone
        be.slot_upsert(ns, "key0", b"changed").await.unwrap(); // seen
        be.slot_upsert(ns, "keyA", b"new").await.unwrap(); // new, sorts last

        // Continue from the first page's cursor.
        let mut cursor = p1.next_cursor;
        for _ in 0..10_000 {
            let Some(c) = cursor else { break };
            let page = be.slot_list(ns, None, Some(&c), 3).await.unwrap();
            seen.extend(page.records.into_iter().map(|r| r.key));
            cursor = page.next_cursor;
        }

        let mut dedup = seen.clone();
        dedup.sort();
        dedup.dedup();
        assert_eq!(
            dedup.len(),
            seen.len(),
            "no duplicate keys across the scan"
        );
        // key0..key8 existed for the whole scan and must all appear.
        for i in 0..9u32 {
            let k = format!("key{i}");
            assert!(seen.contains(&k), "stable key {k} must be observed");
        }
        // The deleted key must not appear.
        assert!(!seen.contains(&"key9".to_string()));
    }

    /// A cursor not produced by `slot_list` is a typed malformed-cursor error.
    pub async fn malformed_cursor_rejected(
        be: Arc<dyn StorageBackend>,
        ns: &str,
    ) {
        be.slot_upsert(ns, "k1", b"v").await.unwrap();
        let err = be
            .slot_list(ns, None, Some("not-a-real-cursor"), 10)
            .await
            .unwrap_err();
        assert!(
            matches!(
                err.downcast_ref::<SlotError>(),
                Some(SlotError::MalformedCursor)
            ),
            "expected MalformedCursor, got: {err}"
        );
    }

    /// A zero limit returns an empty, complete page (bounded, no cursor).
    pub async fn zero_limit_is_empty(be: Arc<dyn StorageBackend>, ns: &str) {
        be.slot_upsert(ns, "k1", b"v").await.unwrap();
        let page = be.slot_list(ns, None, None, 0).await.unwrap();
        assert!(page.records.is_empty());
        assert!(page.next_cursor.is_none());
    }

    /// Multibyte Unicode keys page and order identically to Rust byte-wise `str`
    /// order on every backend (byte-wise / `COLLATE "C"`).
    pub async fn unicode_ordering_and_pagination(
        be: Arc<dyn StorageBackend>,
        ns: &str,
    ) {
        let keys = [
            "a", "z", "\u{7f}", // 1-byte boundary
            "\u{80}", // 2-byte start
            "é",      // U+00E9
            "€",      // U+20AC (3 bytes)
            "日",     // U+65E5
            "日本",   // multi-char CJK
            "日z",    // CJK then ASCII
            "😀",     // U+1F600 (4 bytes)
        ];
        for k in keys {
            be.slot_upsert(ns, k, k.as_bytes()).await.unwrap();
        }
        let mut expected: Vec<String> =
            keys.iter().map(|s| s.to_string()).collect();
        expected.sort(); // Rust str order == byte-wise
        // Small page size forces cursor round-trips through multibyte keys.
        let got = drain_keys(&be, ns, None, 3).await;
        assert_eq!(got, expected, "byte-wise order across all backends");
        let mut dedup = got.clone();
        dedup.dedup();
        assert_eq!(dedup.len(), got.len(), "no duplicates across pages");
    }

    /// A prefix whose last scalar's successor changes UTF-8 length selects exactly
    /// the keys starting with it, on every backend.
    pub async fn prefix_across_utf8_length_boundary(
        be: Arc<dyn StorageBackend>,
        ns: &str,
    ) {
        // U+07FF (2 bytes) -> successor U+0800 (3 bytes); U+007F (1) -> U+0080 (2).
        for k in [
            "\u{7f}",
            "\u{7f}9",
            "\u{80}",
            "\u{07ff}",
            "\u{07ff}A",
            "\u{0800}",
            "z",
        ] {
            be.slot_upsert(ns, k, b"v").await.unwrap();
        }
        // Page size 1 forces pagination + cursor across the boundary.
        assert_eq!(
            drain_keys(&be, ns, Some("\u{07ff}"), 1).await,
            vec!["\u{07ff}".to_string(), "\u{07ff}A".to_string()],
            "U+07FF prefix excludes U+0800"
        );
        assert_eq!(
            drain_keys(&be, ns, Some("\u{7f}"), 1).await,
            vec!["\u{7f}".to_string(), "\u{7f}9".to_string()],
            "U+007F prefix excludes U+0080"
        );
    }

    /// Cursor validation: a well-formed cursor is bound to its scan; mismatches and
    /// corruption are typed errors.
    pub async fn cursor_validation(be: Arc<dyn StorageBackend>, ns: &str) {
        for i in 0..6u32 {
            be.slot_upsert(ns, &format!("p{i}"), b"v").await.unwrap();
            be.slot_upsert(ns, &format!("q{i}"), b"v").await.unwrap();
        }
        // A real cursor bound to (ns, prefix "p").
        let page = be.slot_list(ns, Some("p"), None, 2).await.unwrap();
        let cursor = page.next_cursor.expect("more pages under prefix p");

        // Legitimate reuse (same ns + prefix) works.
        assert!(
            be.slot_list(ns, Some("p"), Some(&cursor), 2).await.is_ok(),
            "same-scan cursor is accepted"
        );

        let is_mismatch = |e: &anyhow::Error| {
            matches!(
                e.downcast_ref::<SlotError>(),
                Some(SlotError::CursorMismatch)
            )
        };
        let is_malformed = |e: &anyhow::Error| {
            matches!(
                e.downcast_ref::<SlotError>(),
                Some(SlotError::MalformedCursor)
            )
        };

        // Namespace mismatch.
        let e = be
            .slot_list("other_ns", Some("p"), Some(&cursor), 2)
            .await
            .unwrap_err();
        assert!(is_mismatch(&e), "namespace mismatch: {e}");

        // Prefix mismatch (a cursor from a different scan/prefix).
        let e = be
            .slot_list(ns, Some("q"), Some(&cursor), 2)
            .await
            .unwrap_err();
        assert!(is_mismatch(&e), "prefix mismatch: {e}");

        // Corrupted checksum/payload: flip the last hex char.
        let mut corrupt = cursor.clone();
        let last = corrupt.pop().unwrap();
        corrupt.push(if last == '0' { '1' } else { '0' });
        let e = be
            .slot_list(ns, Some("p"), Some(&corrupt), 2)
            .await
            .unwrap_err();
        assert!(is_malformed(&e), "corrupted cursor: {e}");

        // Arbitrary "slc1:*" input.
        for bad in ["slc1:zzzz", "slc1:aa:zz", "slc1:", "not-a-cursor"] {
            let e =
                be.slot_list(ns, Some("p"), Some(bad), 2).await.unwrap_err();
            assert!(is_malformed(&e), "arbitrary input {bad:?}: {e}");
        }
    }
}

/// Shared conformance suite of atomic batches, run against every backend.
#[cfg(test)]
pub(crate) mod atomic_batch_suite {
    use super::*;

    fn log(ns: &str, key: &str, id: &str, value: &[u8]) -> BatchOp {
        BatchOp::LogAppendIfAbsent {
            ns: ns.into(),
            key: key.into(),
            capture_id: id.into(),
            value: value.to_vec(),
        }
    }

    /// Two log writes, a version-index template and a high-water maximum.
    fn sample(ns: &str, tag: &str) -> AtomicBatch {
        AtomicBatch {
            preconditions: vec![
                Precondition::LogHead {
                    ns: ns.into(),
                    key: format!("{tag}/a"),
                    seq: 0,
                },
                Precondition::KvIs {
                    ns: ns.into(),
                    key: format!("{tag}/idx"),
                    value: None,
                },
            ],
            ops: vec![
                log(ns, &format!("{tag}/a"), "reg/1/h", b"entry-a"),
                log(ns, &format!("{tag}/b"), "act/1", b"record-b"),
                BatchOp::KvPut {
                    ns: ns.into(),
                    key: format!("{tag}/idx"),
                    value: BatchValue::SeqTemplate {
                        template: format!("{{\"sequence\":{SEQ_SENTINEL}}}")
                            .into_bytes(),
                        of: 0,
                    },
                },
                BatchOp::KvMaxSeq {
                    ns: ns.into(),
                    key: format!("{tag}/hw"),
                    of: 1,
                },
            ],
        }
    }

    async fn nothing_written(be: &dyn StorageBackend, ns: &str, tag: &str) {
        assert_eq!(
            be.log_find_capture(ns, &format!("{tag}/a"), "reg/1/h")
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            be.log_find_capture(ns, &format!("{tag}/b"), "act/1")
                .await
                .unwrap(),
            None
        );
        assert_eq!(be.kv_get(ns, &format!("{tag}/idx")).await.unwrap(), None);
        assert_eq!(be.kv_get(ns, &format!("{tag}/hw")).await.unwrap(), None);
    }

    fn committed(o: BatchOutcome) -> Vec<Option<u64>> {
        match o {
            BatchOutcome::Committed { seqs } => seqs,
            other => panic!("expected a commit: {other:?}"),
        }
    }

    /// One contiguous range in the batch's order; templates rendered with
    /// the committed sequences; the reads see it all.
    pub async fn commits_atomically(be: Arc<dyn StorageBackend>, ns: &str) {
        let seqs = committed(be.apply_batch(&sample(ns, "ok")).await.unwrap());
        let (a, b) = (seqs[0].unwrap(), seqs[1].unwrap());
        assert_eq!(b, a + 1, "contiguous, in order: {seqs:?}");
        assert_eq!((seqs[2], seqs[3]), (None, None));
        let reads = be
            .read_batch(&[
                BatchRead::LogHead {
                    ns: ns.into(),
                    key: "ok/a".into(),
                },
                BatchRead::LogLatest {
                    ns: ns.into(),
                    key: "ok/b".into(),
                },
                BatchRead::KvGet {
                    ns: ns.into(),
                    key: "ok/idx".into(),
                },
                BatchRead::KvGet {
                    ns: ns.into(),
                    key: "ok/hw".into(),
                },
                BatchRead::LogLen {
                    ns: ns.into(),
                    key: "ok/a".into(),
                },
                BatchRead::LogHead {
                    ns: ns.into(),
                    key: "ok/none".into(),
                },
            ])
            .await
            .unwrap();
        assert_eq!(
            reads,
            vec![
                BatchReadResult::Head(a),
                BatchReadResult::Latest(Some((b, b"record-b".to_vec()))),
                BatchReadResult::Kv(Some(
                    format!("{{\"sequence\":{a}}}").into_bytes()
                )),
                BatchReadResult::Kv(Some(b.to_string().into_bytes())),
                BatchReadResult::Len(1),
                BatchReadResult::Head(0),
            ]
        );
        assert_eq!(
            be.log_find_capture(ns, "ok/a", "reg/1/h").await.unwrap(),
            Some((a, b"entry-a".to_vec()))
        );
    }

    /// A stale precondition, or an identity that already exists, writes
    /// nothing at all.
    pub async fn conflicts_write_nothing(
        be: Arc<dyn StorageBackend>,
        ns: &str,
    ) {
        // The stream moved since it was read.
        be.log_append(ns, "c1/a", b"other").await.unwrap();
        let o = be.apply_batch(&sample(ns, "c1")).await.unwrap();
        assert!(matches!(o, BatchOutcome::Conflict { .. }), "{o:?}");
        assert_eq!(
            be.log_find_capture(ns, "c1/b", "act/1").await.unwrap(),
            None
        );
        assert_eq!(be.kv_get(ns, "c1/idx").await.unwrap(), None);
        // A key that is no longer absent.
        be.kv_put(ns, "c2/idx", b"x").await.unwrap();
        let o = be.apply_batch(&sample(ns, "c2")).await.unwrap();
        assert!(matches!(o, BatchOutcome::Conflict { .. }), "{o:?}");
        assert_eq!(
            be.log_find_capture(ns, "c2/a", "reg/1/h").await.unwrap(),
            None
        );
        assert_eq!(be.kv_get(ns, "c2/hw").await.unwrap(), None);
        // An identity of the batch already stored (a later write's).
        be.log_append_if_absent(ns, "c3/b", "act/1", b"record-b")
            .await
            .unwrap();
        let o = be.apply_batch(&sample(ns, "c3")).await.unwrap();
        assert!(matches!(o, BatchOutcome::Conflict { .. }), "{o:?}");
        assert_eq!(
            be.log_find_capture(ns, "c3/a", "reg/1/h").await.unwrap(),
            None
        );
        assert_eq!(be.kv_get(ns, "c3/idx").await.unwrap(), None);
    }

    /// A failure before the transaction, or midway through its writes,
    /// leaves nothing; `commit_atomic` then applies the batch again.
    pub async fn failures_leave_nothing(
        be: Arc<dyn StorageBackend>,
        faults: &dyn BatchFaults,
        ns: &str,
    ) {
        faults.set_batch_fault(Some(BatchFault::BeforeTransaction));
        assert!(be.apply_batch(&sample(ns, "f0")).await.is_err());
        nothing_written(be.as_ref(), ns, "f0").await;
        for at in 0..=4 {
            let tag = format!("f{}", at + 1);
            faults.set_batch_fault(Some(BatchFault::AfterWrites(at)));
            assert!(be.apply_batch(&sample(ns, &tag)).await.is_err(), "{at}");
            nothing_written(be.as_ref(), ns, &tag).await;
        }
        // Resolved: nothing was committed, so the batch is applied again.
        faults.set_batch_fault(Some(BatchFault::AfterWrites(2)));
        match commit_atomic(be.as_ref(), &sample(ns, "f9"), 3)
            .await
            .unwrap()
        {
            CommitResult::Committed(seqs) => assert!(seqs[0].is_some()),
            other => panic!("{other:?}"),
        }
    }

    /// A commit whose acknowledgement was lost is recognized by read-back,
    /// with its original sequences; nothing new is allocated.
    pub async fn lost_acknowledgement_is_recovered(
        be: Arc<dyn StorageBackend>,
        faults: &dyn BatchFaults,
        ns: &str,
    ) {
        faults.set_batch_fault(Some(BatchFault::AfterCommit));
        let batch = sample(ns, "ack");
        let seqs = match commit_atomic(be.as_ref(), &batch, 3).await.unwrap() {
            CommitResult::Committed(seqs) => seqs,
            other => panic!("{other:?}"),
        };
        let stored = be
            .log_find_capture(ns, "ack/a", "reg/1/h")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(seqs[0], Some(stored.0));
        assert_eq!(seqs[1], Some(stored.0 + 1));
        // Nothing allocated after the original commit.
        let next = be.log_append(ns, "ack/next", b"n").await.unwrap();
        assert_eq!(
            next,
            stored.0 + 2,
            "no replacement sequences were allocated"
        );
        assert_eq!(be.log_stream_meta(ns, "ack/a").await.unwrap().len, 1);
    }

    /// A failure before the commit leaves no marker: the same batch is
    /// applied again and commits once.
    pub async fn an_absent_marker_reapplies(
        be: Arc<dyn StorageBackend>,
        faults: &dyn BatchFaults,
        ns: &str,
    ) {
        let mut batch = sample(ns, "r");
        batch.preconditions.clear();
        faults.set_batch_fault(Some(BatchFault::AfterWrites(1)));
        let out = commit_atomic(be.as_ref(), &batch, 3).await.unwrap();
        assert!(matches!(out, CommitResult::Committed(_)), "{out:?}");
        assert_eq!(be.log_stream_meta(ns, "r/a").await.unwrap().len, 1);
    }

    /// With no marker, an existing item under the batch's capture id does
    /// not make the batch committed: it is applied again and conflicts.
    pub async fn an_existing_item_without_a_marker_conflicts(
        be: Arc<dyn StorageBackend>,
        faults: &dyn BatchFaults,
        ns: &str,
    ) {
        be.log_append_if_absent(ns, "p/a", "reg/1/h", b"entry-a")
            .await
            .unwrap();
        let mut batch = sample(ns, "p");
        batch.preconditions.clear();
        faults.set_batch_fault(Some(BatchFault::BeforeTransaction));
        let out = commit_atomic(be.as_ref(), &batch, 3).await.unwrap();
        assert!(matches!(out, CommitResult::Conflict(_)), "{out:?}");
    }

    /// A marker under the batch's id with another request digest is an
    /// identity conflict, never a success and never a retry.
    pub async fn a_marker_with_another_digest_is_an_identity_conflict(
        be: Arc<dyn StorageBackend>,
        faults: &dyn BatchFaults,
        ns: &str,
    ) {
        let mut first = sample(ns, "d");
        first.preconditions.clear();
        first.ops.push(BatchOp::CommitMarker {
            batch_id: format!("{ns}-shared"),
            digest: batch_digest(&first),
        });
        assert!(matches!(
            be.apply_batch(&first).await.unwrap(),
            BatchOutcome::Committed { .. }
        ));
        let mut second = sample(ns, "e");
        second.preconditions.clear();
        let digest = batch_digest(&second);
        let n = second.ops.len();
        second.ops.push(BatchOp::CommitMarker {
            batch_id: format!("{ns}-shared"),
            digest: digest.clone(),
        });
        faults.set_batch_fault(Some(BatchFault::BeforeTransaction));
        let err = commit_attempts(
            be.as_ref(),
            &second,
            n,
            &format!("{ns}-shared"),
            &digest,
            3,
            &mut 0,
        )
        .await
        .unwrap_err();
        assert!(
            err.downcast_ref::<BatchIdentityConflict>().is_some(),
            "{err:#}"
        );
    }

    /// The stored outcome is exactly what the commit returned: every log
    /// write's sequence and every kv write as applied.
    pub async fn the_stored_outcome_is_the_commit_result(
        be: Arc<dyn StorageBackend>,
        ns: &str,
    ) {
        let mut batch = sample(ns, "o");
        batch.preconditions.clear();
        let n = batch.ops.len();
        let digest = batch_digest(&batch);
        batch.ops.push(BatchOp::CommitMarker {
            batch_id: format!("{ns}-o"),
            digest: digest.clone(),
        });
        let BatchOutcome::Committed { mut seqs } =
            be.apply_batch(&batch).await.unwrap()
        else {
            panic!("did not commit");
        };
        seqs.truncate(n);
        let marker = be
            .batch_commit_get(&format!("{ns}-o"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(marker.digest, digest);
        assert_eq!(decode_batch_result(&marker.result, n).unwrap(), seqs);
        assert!(
            seqs.iter().any(|s| s.is_some())
                && seqs.iter().any(|s| s.is_none())
        );
    }

    /// The sweep deletes only expired markers, at most one chunk per call,
    /// within its chunk budget, and reports a backlog it left.
    pub async fn the_sweep_is_chunked_and_bounded(
        be: Arc<dyn StorageBackend>,
        ns: &str,
    ) {
        let marker = |id: String| AtomicBatch {
            preconditions: Vec::new(),
            ops: vec![
                BatchOp::KvMax {
                    ns: format!("{ns}.m"),
                    key: "k".into(),
                    value: 1,
                },
                BatchOp::CommitMarker {
                    batch_id: id,
                    digest: vec![0; 32],
                },
            ],
        };
        // Markers are store-wide: clear those of earlier cases (and of
        // earlier runs on a shared database) so only this case's count.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        sweep_batch_markers_within(
            be.as_ref(),
            now_ms_for_tests(),
            usize::MAX,
            std::time::Duration::from_secs(600),
        )
        .await
        .unwrap();
        for i in 0..2_500 {
            be.apply_batch(&marker(format!("{ns}-old-{i}")))
                .await
                .unwrap();
        }
        // Everything written so far is older than this cutoff.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let cutoff = now_ms_for_tests();
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        be.apply_batch(&marker(format!("{ns}-fresh")))
            .await
            .unwrap();
        let two = sweep_batch_markers_within(
            be.as_ref(),
            cutoff,
            2,
            std::time::Duration::from_secs(60),
        )
        .await
        .unwrap();
        assert_eq!(
            (two.deleted, two.chunks, two.backlog),
            (2 * BATCH_MARKER_SWEEP_CHUNK, 2, true)
        );
        let rest = sweep_batch_markers_within(
            be.as_ref(),
            cutoff,
            64,
            std::time::Duration::from_secs(60),
        )
        .await
        .unwrap();
        assert_eq!(
            (rest.deleted, rest.chunks, rest.backlog),
            (2_500 - 2 * BATCH_MARKER_SWEEP_CHUNK, 1, false)
        );
        assert!(
            be.batch_commit_get(&format!("{ns}-old-0"))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            be.batch_commit_get(&format!("{ns}-fresh"))
                .await
                .unwrap()
                .is_some()
        );
    }

    fn now_ms_for_tests() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }

    /// Concurrent batches each take one contiguous range; the ranges are
    /// disjoint.
    pub async fn concurrent_batches_take_disjoint_ranges(
        be: Arc<dyn StorageBackend>,
        ns: &str,
    ) {
        let mut tasks = Vec::new();
        for t in 0..8 {
            let (be, ns) = (be.clone(), ns.to_string());
            tasks.push(tokio::spawn(async move {
                let ops = (0..5)
                    .map(|i| {
                        log(&ns, &format!("cc{t}/{i}"), &format!("id{i}"), b"v")
                    })
                    .collect();
                let batch = AtomicBatch {
                    preconditions: vec![],
                    ops,
                };
                committed(be.apply_batch(&batch).await.unwrap())
                    .into_iter()
                    .map(|s| s.unwrap())
                    .collect::<Vec<u64>>()
            }));
        }
        let mut all = Vec::new();
        for t in tasks {
            let seqs = t.await.unwrap();
            assert!(
                seqs.windows(2).all(|w| w[1] == w[0] + 1),
                "contiguous: {seqs:?}"
            );
            all.extend(seqs);
        }
        let n = all.len();
        all.sort();
        all.dedup();
        assert_eq!(all.len(), n, "disjoint");
    }

    /// The whole suite.
    /// `KvMax` raises a number and never lowers it, inside a batch and
    /// alone.
    pub async fn kv_max_never_lowers(be: Arc<dyn StorageBackend>, ns: &str) {
        let max = |value: u64| AtomicBatch {
            preconditions: Vec::new(),
            ops: vec![BatchOp::KvMax {
                ns: ns.into(),
                key: "hw".into(),
                value,
            }],
        };
        let read = || async {
            serde_json::from_slice::<u64>(
                &be.kv_get(ns, "hw").await.unwrap().unwrap(),
            )
            .unwrap()
        };
        for (value, expect) in [(7, 7), (3, 7), (12, 12), (12, 12)] {
            let out = commit_atomic(be.as_ref(), &max(value), 3).await.unwrap();
            assert!(matches!(out, CommitResult::Committed(_)), "{out:?}");
            assert_eq!(read().await, expect, "after KvMax {value}");
        }
    }

    /// A key in a guarded namespace never carries a TTL.
    pub async fn guarded_keys_refuse_a_ttl(be: Arc<dyn StorageBackend>) {
        for ns in [
            "schemas",
            "schemas.v1",
            "schemas.v1.index",
            "schema_lineage",
        ] {
            assert!(
                be.kv_put_with_ttl(ns, "k", b"v", 60).await.is_err(),
                "{ns} accepted a TTL"
            );
        }
    }

    pub async fn all(
        be: Arc<dyn StorageBackend>,
        faults: &dyn BatchFaults,
        ns: &str,
    ) {
        kv_max_never_lowers(be.clone(), &format!("{ns}_max")).await;
        guarded_keys_refuse_a_ttl(be.clone()).await;
        commits_atomically(be.clone(), &format!("{ns}_ok")).await;
        conflicts_write_nothing(be.clone(), &format!("{ns}_conf")).await;
        failures_leave_nothing(be.clone(), faults, &format!("{ns}_fail")).await;
        lost_acknowledgement_is_recovered(
            be.clone(),
            faults,
            &format!("{ns}_ack"),
        )
        .await;
        an_absent_marker_reapplies(be.clone(), faults, &format!("{ns}_absent"))
            .await;
        an_existing_item_without_a_marker_conflicts(
            be.clone(),
            faults,
            &format!("{ns}_part"),
        )
        .await;
        a_marker_with_another_digest_is_an_identity_conflict(
            be.clone(),
            faults,
            &format!("{ns}_digest"),
        )
        .await;
        the_stored_outcome_is_the_commit_result(
            be.clone(),
            &format!("{ns}_outcome"),
        )
        .await;
        the_sweep_is_chunked_and_bounded(be.clone(), &format!("{ns}_sweep"))
            .await;
        concurrent_batches_take_disjoint_ranges(
            be.clone(),
            &format!("{ns}_conc"),
        )
        .await;
        stream_reads_match_individual_reads(be.clone(), &format!("{ns}_sr"))
            .await;
        stream_reads_are_one_snapshot(be.clone(), &format!("{ns}_srsnap"))
            .await;
        max_ops_see_the_batchs_own_writes(be.clone(), &format!("{ns}_rw"))
            .await;
        contradictory_preconditions_conflict(be.clone(), &format!("{ns}_pre"))
            .await;
        an_expired_value_reads_as_absent(be.clone(), &format!("{ns}_ttl"))
            .await;
        large_groups_keep_request_order(be.clone(), &format!("{ns}_big")).await;
        oversized_items_are_refused(be.clone(), &format!("{ns}_m")).await;
        stream_reads_stop_at_their_budget(be.clone(), &format!("{ns}_within"))
            .await;
        identical_duplicates_collapse(be.clone(), &format!("{ns}_collapse"))
            .await;
        a_duplicate_with_other_bytes_fails_closed(
            be.clone(),
            &format!("{ns}_dupconf"),
        )
        .await;
        absent_or_identical_maps_existing_and_new(
            be.clone(),
            &format!("{ns}_aoi"),
        )
        .await;
        a_resolved_mapping_is_the_original(
            be.clone(),
            faults,
            &format!("{ns}_orig"),
        )
        .await;
        oversized_legacy_items_stay_readable(
            be.clone(),
            &format!("{ns}_legacy"),
        )
        .await;
    }

    /// A max sees the batch's own earlier writes of its key, and repeated
    /// writes of one key end as applied in op order.
    pub async fn max_ops_see_the_batchs_own_writes(
        be: Arc<dyn StorageBackend>,
        ns: &str,
    ) {
        be.kv_put(ns, "hw", b"5").await.unwrap();
        let batch = AtomicBatch {
            preconditions: vec![],
            ops: vec![
                log(ns, "s", "e1", b"one"),
                log(ns, "s", "e2", b"two"),
                // A put then a max of the same key: the max starts from the
                // put, not from the stored value.
                BatchOp::KvPut {
                    ns: ns.into(),
                    key: "p".into(),
                    value: BatchValue::Bytes(b"1".to_vec()),
                },
                BatchOp::KvMax {
                    ns: ns.into(),
                    key: "p".into(),
                    value: 3,
                },
                // Several maxes of one stored key, and a put after them.
                BatchOp::KvMaxSeq {
                    ns: ns.into(),
                    key: "hw".into(),
                    of: 0,
                },
                BatchOp::KvMaxSeq {
                    ns: ns.into(),
                    key: "hw".into(),
                    of: 1,
                },
                BatchOp::KvMax {
                    ns: ns.into(),
                    key: "hw".into(),
                    value: 2,
                },
                BatchOp::KvPut {
                    ns: ns.into(),
                    key: "q".into(),
                    value: BatchValue::Bytes(b"first".to_vec()),
                },
                BatchOp::KvPut {
                    ns: ns.into(),
                    key: "q".into(),
                    value: BatchValue::Bytes(b"last".to_vec()),
                },
            ],
        };
        let seqs = committed(be.apply_batch(&batch).await.unwrap());
        let (s1, s2) = (seqs[0].unwrap(), seqs[1].unwrap());
        assert_eq!(s2, s1 + 1);
        assert_eq!(be.kv_get(ns, "p").await.unwrap(), Some(b"3".to_vec()));
        let hw = s2.max(5).to_string().into_bytes();
        assert_eq!(be.kv_get(ns, "hw").await.unwrap(), Some(hw));
        assert_eq!(be.kv_get(ns, "q").await.unwrap(), Some(b"last".to_vec()));
    }

    /// Two different expectations of one stream or key cannot both hold:
    /// the batch conflicts and writes nothing; the same expectation twice
    /// holds.
    pub async fn contradictory_preconditions_conflict(
        be: Arc<dyn StorageBackend>,
        ns: &str,
    ) {
        let head = |seq| Precondition::LogHead {
            ns: ns.into(),
            key: "s".into(),
            seq,
        };
        let is = |value: Option<&[u8]>| Precondition::KvIs {
            ns: ns.into(),
            key: "k".into(),
            value: value.map(<[u8]>::to_vec),
        };
        let ops = || vec![log(ns, "s", "e", b"v")];
        for preconditions in
            [vec![head(0), head(7)], vec![is(None), is(Some(b"x"))]]
        {
            let batch = AtomicBatch {
                preconditions,
                ops: ops(),
            };
            assert!(matches!(
                be.apply_batch(&batch).await.unwrap(),
                BatchOutcome::Conflict { .. }
            ));
            assert!(be.log_list(ns, "s").await.unwrap().is_empty());
        }
        let batch = AtomicBatch {
            preconditions: vec![head(0), is(None), head(0), is(None)],
            ops: ops(),
        };
        committed(be.apply_batch(&batch).await.unwrap());
    }

    /// An expired value reads as absent: for a precondition, a max and a
    /// read.
    pub async fn an_expired_value_reads_as_absent(
        be: Arc<dyn StorageBackend>,
        ns: &str,
    ) {
        be.kv_put_with_ttl(ns, "gone", b"99", 1).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(2100)).await;
        let batch = AtomicBatch {
            preconditions: vec![Precondition::KvIs {
                ns: ns.into(),
                key: "gone".into(),
                value: None,
            }],
            ops: vec![BatchOp::KvMax {
                ns: ns.into(),
                key: "gone".into(),
                value: 4,
            }],
        };
        committed(be.apply_batch(&batch).await.unwrap());
        assert_eq!(be.kv_get(ns, "gone").await.unwrap(), Some(b"4".to_vec()));
        be.kv_put_with_ttl(ns, "gone2", b"1", 1).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(2100)).await;
        let read = be
            .read_batch(&[BatchRead::KvGet {
                ns: ns.into(),
                key: "gone2".into(),
            }])
            .await
            .unwrap();
        assert!(matches!(read[..], [BatchReadResult::Kv(None)]));
    }

    /// Groups larger than one array statement (by items and by bytes) keep
    /// request order: contiguous sequences in op order, every entry and
    /// key stored, every read answered at its position.
    pub async fn large_groups_keep_request_order(
        be: Arc<dyn StorageBackend>,
        ns: &str,
    ) {
        let n = 5000;
        let mut ops: Vec<BatchOp> = (0..n)
            .map(|i| {
                log(
                    ns,
                    &format!("s{}", i % 7),
                    &format!("e{i}"),
                    format!("v{i}").as_bytes(),
                )
            })
            .collect();
        // Three entries of 2 MiB: more than one statement's payload.
        for i in 0..3 {
            ops.push(log(
                ns,
                "big",
                &format!("b{i}"),
                &vec![i as u8; 2 * 1024 * 1024],
            ));
        }
        for i in 0..n {
            ops.push(BatchOp::KvPut {
                ns: ns.into(),
                key: format!("k{i}"),
                value: BatchValue::SeqTemplate {
                    template: format!("{SEQ_SENTINEL}").into_bytes(),
                    of: i,
                },
            });
        }
        let batch = AtomicBatch {
            preconditions: (0..n)
                .map(|i| Precondition::KvIs {
                    ns: ns.into(),
                    key: format!("k{i}"),
                    value: None,
                })
                .collect(),
            ops,
        };
        let seqs = committed(be.apply_batch(&batch).await.unwrap());
        let first = seqs[0].unwrap();
        for (i, s) in seqs.iter().take(n + 3).enumerate() {
            assert_eq!(*s, Some(first + i as u64), "op {i}");
        }
        let reads: Vec<BatchRead> = (0..n)
            .rev()
            .map(|i| BatchRead::KvGet {
                ns: ns.into(),
                key: format!("k{i}"),
            })
            .chain([BatchRead::LogLen {
                ns: ns.into(),
                key: "big".into(),
            }])
            .collect();
        let got = be.read_batch(&reads).await.unwrap();
        for (j, r) in got.iter().take(n).enumerate() {
            let i = n - 1 - j;
            let want = (first + i as u64).to_string().into_bytes();
            assert!(
                matches!(r, BatchReadResult::Kv(Some(v)) if *v == want),
                "k{i}: {r:?}"
            );
        }
        assert!(matches!(got[n], BatchReadResult::Len(3)), "{:?}", got[n]);
        let big = be.log_list(ns, "big").await.unwrap();
        assert_eq!(big.len(), 3);
        assert!(big.iter().all(|(_, v)| v.len() == 2 * 1024 * 1024));
    }

    /// A newly written state item may not exceed `MAX_STATE_ITEM_BYTES`: a
    /// batch item over it is refused before any statement with its size and
    /// key digest, and nothing is written; one at the limit commits. A
    /// single write to a guarded namespace is refused the same way.
    pub async fn oversized_items_are_refused(
        be: Arc<dyn StorageBackend>,
        ns: &str,
    ) {
        let at = vec![7u8; MAX_STATE_ITEM_BYTES];
        let over = vec![7u8; MAX_STATE_ITEM_BYTES + 1];
        let batch = |log_value: &[u8], kv_value: &[u8]| AtomicBatch {
            preconditions: vec![],
            ops: vec![
                log(ns, "s", "e", log_value),
                BatchOp::KvPut {
                    ns: ns.into(),
                    key: "k".into(),
                    value: BatchValue::Bytes(kv_value.to_vec()),
                },
            ],
        };
        for (log_value, kv_value, key) in [(&over, &at, "s"), (&at, &over, "k")]
        {
            let err = be
                .apply_batch(&batch(log_value, kv_value))
                .await
                .unwrap_err();
            let too = err
                .downcast_ref::<StateItemTooLarge>()
                .unwrap_or_else(|| panic!("{err:#}"));
            assert_eq!(too.size, MAX_STATE_ITEM_BYTES + 1);
            assert_eq!(too.key_digest, state_key_digest(ns, key));
            assert!(be.log_list(ns, "s").await.unwrap().is_empty());
            assert_eq!(be.kv_get(ns, "k").await.unwrap(), None);
        }
        committed(be.apply_batch(&batch(&at, &at)).await.unwrap());
        let guarded = "schemas.v1.oversize";
        let err = be.kv_put(guarded, ns, &over).await.unwrap_err();
        assert!(err.downcast_ref::<StateItemTooLarge>().is_some(), "{err:#}");
        let err = be.log_append(guarded, ns, &over).await.unwrap_err();
        assert!(err.downcast_ref::<StateItemTooLarge>().is_some(), "{err:#}");
    }

    /// An item over the limit written before it existed (an unguarded one
    /// here) stays readable through batched and stream reads, and each such
    /// read is counted.
    pub async fn oversized_legacy_items_stay_readable(
        be: Arc<dyn StorageBackend>,
        ns: &str,
    ) {
        let over = vec![3u8; MAX_STATE_ITEM_BYTES + 10];
        be.kv_put(ns, "legacy", &over).await.unwrap();
        be.log_append(ns, "legacy", &over).await.unwrap();
        let before = oversized_reads();
        let got = be
            .read_batch(&[
                BatchRead::KvGet {
                    ns: ns.into(),
                    key: "legacy".into(),
                },
                BatchRead::LogLatest {
                    ns: ns.into(),
                    key: "legacy".into(),
                },
            ])
            .await
            .unwrap();
        assert!(
            matches!(&got[0], BatchReadResult::Kv(Some(v)) if v.len() == over.len())
        );
        assert!(
            matches!(&got[1], BatchReadResult::Latest(Some((_, v))) if v.len() == over.len())
        );
        let streams = be
            .log_read_streams(&[(ns.into(), "legacy".into())])
            .await
            .unwrap();
        assert_eq!(streams[0][0].value.len(), over.len());
        assert!(
            oversized_reads() >= before + 3,
            "{} {before}",
            oversized_reads()
        );
    }

    /// A budgeted stream read answers the leading streams, in request
    /// order, whose entries fit the budget together: exactly as the full
    /// read answers them, the first one always, and none past the first that
    /// does not fit (an empty stream fits).
    pub async fn stream_reads_stop_at_their_budget(
        be: Arc<dyn StorageBackend>,
        ns: &str,
    ) {
        for (key, n) in [("a", 3), ("c", 3), ("d", 1)] {
            for i in 0..n {
                be.log_append(ns, key, &vec![i as u8; 1000]).await.unwrap();
            }
        }
        let streams: Vec<(String, String)> = ["a", "b", "c", "d"]
            .iter()
            .map(|k| (ns.to_string(), k.to_string()))
            .collect();
        let full = be.log_read_streams(&streams).await.unwrap();
        let a_bytes: u64 = full[0].iter().map(log_entry_bytes).sum();
        let c_bytes: u64 = full[2].iter().map(log_entry_bytes).sum();
        for (budget, answered) in [
            (0, 2),
            (a_bytes, 2),
            (a_bytes + c_bytes - 1, 2),
            (a_bytes + c_bytes, 3),
            (u64::MAX, 4),
        ] {
            let got =
                be.log_read_streams_within(&streams, budget).await.unwrap();
            assert_eq!(got.len(), answered, "budget {budget}");
            assert_eq!(&got[..], &full[..answered], "budget {budget}");
        }
        assert!(be.log_read_streams_within(&[], 0).await.unwrap().is_empty());
    }

    fn idem(ns: &str, key: &str, id: &str, value: &[u8]) -> BatchOp {
        BatchOp::LogAppendIdempotent {
            ns: ns.into(),
            key: key.into(),
            capture_id: id.into(),
            value: value.to_vec(),
        }
    }

    fn just(batch: Vec<BatchOp>) -> AtomicBatch {
        AtomicBatch {
            preconditions: vec![],
            ops: batch,
        }
    }

    /// Identical duplicates of a capture id within one request collapse
    /// onto the first: every position maps to its one sequence, new entries
    /// are numbered in first-input order, and the stream holds one entry.
    pub async fn identical_duplicates_collapse(
        be: Arc<dyn StorageBackend>,
        ns: &str,
    ) {
        for mode in [log as fn(&str, &str, &str, &[u8]) -> BatchOp, idem] {
            let key = format!(
                "dup-{}",
                matches!(
                    mode(ns, "k", "i", b""),
                    BatchOp::LogAppendIdempotent { .. }
                )
            );
            let seqs = committed(
                be.apply_batch(&just(vec![
                    mode(ns, &key, "x", b"one"),
                    mode(ns, &key, "y", b"two"),
                    mode(ns, &key, "x", b"one"),
                ]))
                .await
                .unwrap(),
            );
            assert_eq!(seqs[0], seqs[2], "{key}");
            assert_eq!(seqs[1], Some(seqs[0].unwrap() + 1), "{key}");
            assert_eq!(be.log_list(ns, &key).await.unwrap().len(), 2, "{key}");
        }
    }

    /// The same capture id with different bytes within one request fails
    /// closed before any statement: nothing is written or applied.
    pub async fn a_duplicate_with_other_bytes_fails_closed(
        be: Arc<dyn StorageBackend>,
        ns: &str,
    ) {
        for mode in [log as fn(&str, &str, &str, &[u8]) -> BatchOp, idem] {
            let mut applies = 0;
            let err = commit_atomic_counted(
                be.as_ref(),
                &just(vec![
                    mode(ns, "d", "x", b"one"),
                    mode(ns, "d", "x", b"two"),
                ]),
                3,
                &mut applies,
            )
            .await
            .unwrap_err();
            assert!(
                matches!(
                    err.downcast_ref::<LogError>(),
                    Some(LogError::CaptureIdentityConflict { .. })
                ),
                "{err:#}"
            );
            assert_eq!(applies, 0, "refused before any statement");
            assert!(be.log_list(ns, "d").await.unwrap().is_empty());
        }
    }

    /// AbsentOrIdentical accepts a durable identical entry with its
    /// original sequence (a template of it renders that sequence), while
    /// new ids, interleaved, are numbered in first-input order; a durable
    /// entry with other bytes fails closed, applied once, never retried.
    pub async fn absent_or_identical_maps_existing_and_new(
        be: Arc<dyn StorageBackend>,
        ns: &str,
    ) {
        let first = committed(
            be.apply_batch(&just(vec![idem(ns, "s", "e", b"held")]))
                .await
                .unwrap(),
        );
        let e = first[0].unwrap();
        let seqs = committed(
            be.apply_batch(&just(vec![
                idem(ns, "s", "a", b"a"),
                idem(ns, "s", "e", b"held"),
                log(ns, "t", "b", b"b"),
                idem(ns, "s", "e", b"held"),
                idem(ns, "s", "c", b"c"),
                BatchOp::KvPut {
                    ns: ns.into(),
                    key: "of-e".into(),
                    value: BatchValue::SeqTemplate {
                        template: format!("{SEQ_SENTINEL}").into_bytes(),
                        of: 1,
                    },
                },
            ]))
            .await
            .unwrap(),
        );
        let a = seqs[0].unwrap();
        assert_eq!(seqs[1], Some(e), "the original sequence");
        assert_eq!(seqs[3], Some(e));
        assert_eq!(seqs[2], Some(a + 1), "first-input order");
        assert_eq!(seqs[4], Some(a + 2));
        assert_eq!(
            be.kv_get(ns, "of-e").await.unwrap(),
            Some(e.to_string().into_bytes())
        );
        assert_eq!(be.log_list(ns, "s").await.unwrap().len(), 3);
        let mut applies = 0;
        let err = commit_atomic_counted(
            be.as_ref(),
            &just(vec![idem(ns, "s", "f", b"f"), idem(ns, "s", "e", b"other")]),
            3,
            &mut applies,
        )
        .await
        .unwrap_err();
        assert!(
            matches!(
                err.downcast_ref::<LogError>(),
                Some(LogError::CaptureIdentityConflict { .. })
            ),
            "{err:#}"
        );
        assert_eq!(applies, 1, "never retried");
        assert!(be.log_find_capture(ns, "s", "f").await.unwrap().is_none());
    }

    /// A lost acknowledgement of a batch with existing and new ids resolves
    /// from its marker to the original mapping; no other block is taken.
    pub async fn a_resolved_mapping_is_the_original(
        be: Arc<dyn StorageBackend>,
        faults: &dyn BatchFaults,
        ns: &str,
    ) {
        let held = committed(
            be.apply_batch(&just(vec![idem(ns, "r", "e", b"held")]))
                .await
                .unwrap(),
        )[0];
        faults.set_batch_fault(Some(BatchFault::AfterCommit));
        let batch = just(vec![
            idem(ns, "r", "n1", b"1"),
            idem(ns, "r", "e", b"held"),
            idem(ns, "r", "n2", b"2"),
        ]);
        let seqs = match commit_atomic(be.as_ref(), &batch, 3).await.unwrap() {
            CommitResult::Committed(seqs) => seqs,
            other => panic!("{other:?}"),
        };
        assert_eq!(seqs[1], held);
        let n1 = be.log_find_capture(ns, "r", "n1").await.unwrap().unwrap().0;
        let n2 = be.log_find_capture(ns, "r", "n2").await.unwrap().unwrap().0;
        assert_eq!((seqs[0], seqs[2]), (Some(n1), Some(n2)));
        assert_eq!(n2, n1 + 1);
        let next = committed(
            be.apply_batch(&just(vec![idem(ns, "r", "later", b"l")]))
                .await
                .unwrap(),
        )[0]
        .unwrap();
        assert_eq!(next, n2 + 1, "no block taken by a re-apply");
    }

    /// Every stream as paged out one by one.
    async fn one_by_one(
        be: &Arc<dyn StorageBackend>,
        streams: &[(String, String)],
    ) -> Vec<Vec<LogEntryMeta>> {
        let mut out = Vec::new();
        for (ns, key) in streams {
            let mut entries: Vec<LogEntryMeta> = Vec::new();
            loop {
                let since = entries.last().map_or(0, |e| e.seq);
                let page =
                    be.log_read_meta_since(ns, key, since, 7).await.unwrap();
                let done = page.len() < 7;
                entries.extend(page);
                if done {
                    break;
                }
            }
            out.push(entries);
        }
        out
    }

    /// `log_read_streams` returns, byte for byte, what reading each stream
    /// on its own returns: empty streams, streams of several lengths across
    /// two namespaces, a repeated request (answered twice, in place), and
    /// 2,048 streams in one request.
    pub async fn stream_reads_match_individual_reads(
        be: Arc<dyn StorageBackend>,
        ns: &str,
    ) {
        let other = format!("{ns}_b");
        for k in 0..40usize {
            let n = if k % 2 == 1 { &other } else { ns };
            for e in 0..(k % 9) {
                be.log_append_if_absent(
                    n,
                    &format!("s{k}"),
                    &format!("c{e}"),
                    format!("{k}-{e}").repeat(1 + e).as_bytes(),
                )
                .await
                .unwrap();
            }
        }
        let mut streams: Vec<(String, String)> = (0..40usize)
            .map(|k| {
                let n = if k % 2 == 1 { &other } else { ns };
                (n.to_string(), format!("s{k}"))
            })
            .collect();
        streams.push((ns.to_string(), "never".into()));
        streams.push((ns.to_string(), "s4".into()));
        streams.insert(0, (other.clone(), "s3".into()));
        assert_eq!(
            be.log_read_streams(&streams).await.unwrap(),
            one_by_one(&be, &streams).await
        );
        let big: Vec<(String, String)> = (0..2048usize)
            .map(|k| (ns.to_string(), format!("s{}", k % 50)))
            .collect();
        let got = be.log_read_streams(&big).await.unwrap();
        assert_eq!(got.len(), 2048);
        assert_eq!(got, one_by_one(&be, &big).await);
    }

    /// One snapshot: while atomic batches append to two streams together,
    /// every read of both sees them at the same length.
    pub async fn stream_reads_are_one_snapshot(
        be: Arc<dyn StorageBackend>,
        ns: &str,
    ) {
        let streams = vec![
            (ns.to_string(), "a".to_string()),
            (ns.to_string(), "b".to_string()),
        ];
        let writer = {
            let be = be.clone();
            let ns = ns.to_string();
            tokio::spawn(async move {
                for i in 0..60 {
                    let batch = AtomicBatch {
                        preconditions: vec![],
                        ops: vec![
                            log(&ns, "a", &format!("a{i}"), b"x"),
                            log(&ns, "b", &format!("b{i}"), b"y"),
                        ],
                    };
                    be.apply_batch(&batch).await.unwrap();
                    tokio::task::yield_now().await;
                }
            })
        };
        let mut reads = 0;
        while !writer.is_finished() || reads < 5 {
            let got = be.log_read_streams(&streams).await.unwrap();
            assert_eq!(got[0].len(), got[1].len(), "one snapshot: {got:?}");
            reads += 1;
        }
        writer.await.unwrap();
        let got = be.log_read_streams(&streams).await.unwrap();
        assert_eq!((got[0].len(), got[1].len()), (60, 60));
    }
}
