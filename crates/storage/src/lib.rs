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

    /// Apply `batch` atomically (see [`AtomicBatch`]). The default refuses:
    /// a backend that cannot commit atomically never pretends to.
    async fn apply_batch(&self, _batch: &AtomicBatch) -> Result<BatchOutcome> {
        Err(anyhow::anyhow!(
            "this storage backend does not support atomic batches"
        ))
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
    /// A new entry under a capture identity that must not exist yet.
    LogAppendIfAbsent {
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

/// A batch whose read-back finds only part of it, or different bytes: the
/// store did not behave atomically.
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
        for (i, op) in self.ops.iter().enumerate() {
            let of = match op {
                BatchOp::LogAppendIfAbsent { .. } => continue,
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
            };
            anyhow::ensure!(
                of < i
                    && matches!(
                        self.ops[of],
                        BatchOp::LogAppendIfAbsent { .. }
                    ),
                "batch op {i}: `of` {of} is not an earlier log write"
            );
        }
        Ok(())
    }
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

/// Commit `batch`, resolving ambiguous outcomes. An `apply_batch` error may
/// or may not have committed, so every log write's identity is read back:
/// none stored, the same batch is applied again (at most `retries` more
/// times); all stored with exactly the intended bytes, the earlier commit
/// succeeded and its stored sequences are returned (templated KV values
/// checked against them, no new allocation); anything else is an
/// [`AtomicityViolation`].
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
    batch.validate()?;
    let mut attempt = 0;
    loop {
        *applies += 1;
        let err = match be.apply_batch(batch).await {
            Ok(BatchOutcome::Committed { seqs }) => {
                return Ok(CommitResult::Committed(seqs));
            }
            Ok(BatchOutcome::Conflict { why }) => {
                return Ok(CommitResult::Conflict(why));
            }
            Err(e) => e,
        };
        attempt += 1;
        // The read-back itself may fail transiently: retried, never skipped.
        let mut reads = 0;
        let found = loop {
            match read_back(be, batch).await {
                Ok(found) => break found,
                Err(e) if reads < retries => {
                    reads += 1;
                    tracing::warn!(error = %e, "atomic batch: read-back failed; retrying it");
                }
                Err(e) => {
                    return Err(e.context(format!(
                        "atomic batch: outcome unknown after the error {err:#}"
                    )));
                }
            }
        };
        match found {
            ReadBack::None if attempt <= retries => {
                tracing::warn!(error = %err, attempt, "atomic batch: not committed; applying it again");
            }
            ReadBack::None => {
                return Err(err.context(format!(
                    "atomic batch: not committed after {attempt} attempts"
                )));
            }
            ReadBack::All(seqs) => return Ok(CommitResult::Committed(seqs)),
            ReadBack::Partial(why) => {
                return Err(AtomicityViolation(why).into());
            }
        }
    }
}

enum ReadBack {
    None,
    All(Vec<Option<u64>>),
    Partial(String),
}

async fn read_back(
    be: &dyn StorageBackend,
    batch: &AtomicBatch,
) -> Result<ReadBack> {
    let mut seqs = vec![None; batch.ops.len()];
    let (mut present, mut absent) = (0usize, 0usize);
    for (i, op) in batch.ops.iter().enumerate() {
        if let BatchOp::LogAppendIfAbsent {
            ns,
            key,
            capture_id,
            value,
        } = op
        {
            match be.log_find_capture(ns, key, capture_id).await? {
                Some((seq, stored)) if &stored == value => {
                    seqs[i] = Some(seq);
                    present += 1;
                }
                Some(_) => {
                    return Ok(ReadBack::Partial(format!(
                        "{capture_id} in {key} holds other bytes"
                    )));
                }
                None => absent += 1,
            }
        }
    }
    if present == 0 {
        return Ok(ReadBack::None);
    }
    if absent > 0 {
        return Ok(ReadBack::Partial(format!(
            "{present} of {} log writes are stored",
            present + absent
        )));
    }
    // Every log write is stored: its KV writes must be too.
    for op in &batch.ops {
        match op {
            BatchOp::KvPut { ns, key, value } => {
                let want = match value {
                    BatchValue::Bytes(b) => b.clone(),
                    BatchValue::SeqTemplate { template, of } => {
                        render_seq(template, seqs[*of].expect("validated"))
                    }
                };
                if be.kv_get(ns, key).await?.as_deref() != Some(want.as_slice())
                {
                    return Ok(ReadBack::Partial(format!(
                        "{key} does not hold its write"
                    )));
                }
            }
            BatchOp::KvMaxSeq { ns, key, of } => {
                let stored = be.kv_get(ns, key).await?;
                let n = stored
                    .as_deref()
                    .map(serde_json::from_slice::<u64>)
                    .transpose()?
                    .unwrap_or(0);
                if n < seqs[*of].expect("validated") {
                    return Ok(ReadBack::Partial(format!(
                        "{key} is below its write"
                    )));
                }
            }
            BatchOp::LogAppendIfAbsent { .. } => {}
        }
    }
    Ok(ReadBack::All(seqs))
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

    /// Only part of a batch stored is an atomicity violation, never a
    /// success and never a retry.
    pub async fn partial_read_back_is_a_violation(
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
        let err = commit_atomic(be.as_ref(), &batch, 3).await.unwrap_err();
        assert!(
            err.downcast_ref::<AtomicityViolation>().is_some(),
            "{err:#}"
        );
        // Other bytes under an intended identity: also a violation.
        be.log_append_if_absent(ns, "q/a", "reg/1/h", b"other")
            .await
            .unwrap();
        let mut batch = sample(ns, "q");
        batch.preconditions.clear();
        faults.set_batch_fault(Some(BatchFault::BeforeTransaction));
        let err = commit_atomic(be.as_ref(), &batch, 3).await.unwrap_err();
        assert!(
            err.downcast_ref::<AtomicityViolation>().is_some(),
            "{err:#}"
        );
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
    pub async fn all(
        be: Arc<dyn StorageBackend>,
        faults: &dyn BatchFaults,
        ns: &str,
    ) {
        commits_atomically(be.clone(), &format!("{ns}_ok")).await;
        conflicts_write_nothing(be.clone(), &format!("{ns}_conf")).await;
        failures_leave_nothing(be.clone(), faults, &format!("{ns}_fail")).await;
        lost_acknowledgement_is_recovered(
            be.clone(),
            faults,
            &format!("{ns}_ack"),
        )
        .await;
        partial_read_back_is_a_violation(
            be.clone(),
            faults,
            &format!("{ns}_part"),
        )
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
