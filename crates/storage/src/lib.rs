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
}

pub type ArcStorageBackend = Arc<dyn StorageBackend>;

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

/// The backend-computed content digest over the exact stored bytes. Domain-separated
/// so it can never collide with a hash taken for another purpose. Used for a cheap
/// inequality check before the authoritative exact-bytes comparison; the backend
/// derives it from the bytes it stores and never trusts a caller-supplied digest.
pub(crate) fn content_digest(value: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(b"deltaforge:replay:content:v1");
    h.update(value);
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

/// Shared contract suite for the replay log primitives, run against every backend.
#[cfg(test)]
pub(crate) mod log_contract_suite {
    use super::*;

    const NS: &str = "journal";

    fn is_conflict(err: &anyhow::Error) -> bool {
        matches!(
            err.downcast_ref::<LogError>(),
            Some(LogError::CaptureIdentityConflict { .. })
        )
    }

    /// Identical (capture_id, value) is idempotent: one entry, stable seq.
    pub async fn idempotency(be: Arc<dyn StorageBackend>) {
        let a = be
            .log_append_if_absent(NS, "s:replay", "cap-1", b"payload-1")
            .await
            .unwrap();
        assert_eq!(a.status, AppendStatus::Inserted);
        let b = be
            .log_append_if_absent(NS, "s:replay", "cap-1", b"payload-1")
            .await
            .unwrap();
        assert_eq!(b.status, AppendStatus::AlreadyPresent);
        assert_eq!(a.seq, b.seq, "identical retry must return the same seq");
        assert_eq!(be.log_since(NS, "s:replay", 0).await.unwrap().len(), 1);
    }

    /// Same capture_id with different bytes is rejected; the original is untouched.
    pub async fn conflict_rejected(be: Arc<dyn StorageBackend>) {
        let a = be
            .log_append_if_absent(NS, "s:replay", "cap-1", b"original")
            .await
            .unwrap();
        let err = be
            .log_append_if_absent(NS, "s:replay", "cap-1", b"tampered")
            .await
            .unwrap_err();
        assert!(is_conflict(&err), "expected CaptureIdentityConflict: {err}");
        let entries = be.log_since(NS, "s:replay", 0).await.unwrap();
        assert_eq!(entries, vec![(a.seq, b"original".to_vec())]);
    }

    /// The horizon is the highest seq actually removed from THIS stream, not
    /// `oldest_retained - 1` - global sequence gaps belong to other keys.
    pub async fn horizon_with_global_gaps(be: Arc<dyn StorageBackend>) {
        // Interleave appends to another key so the target stream has global gaps.
        be.log_append(NS, "other", b"x").await.unwrap();
        let s1 = be
            .log_append_if_absent(NS, "s:replay", "c1", b"a")
            .await
            .unwrap()
            .seq;
        be.log_append(NS, "other", b"x").await.unwrap();
        let s2 = be
            .log_append_if_absent(NS, "s:replay", "c2", b"b")
            .await
            .unwrap()
            .seq;
        be.log_append(NS, "other", b"x").await.unwrap();
        let s3 = be
            .log_append_if_absent(NS, "s:replay", "c3", b"c")
            .await
            .unwrap()
            .seq;
        assert!(s1 < s2 && s2 < s3);
        // Remove everything below s3 (pin protects s3), by age (cutoff in the future).
        let out = be
            .log_truncate(
                NS,
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
            .log_since(NS, "s:replay", out.min_valid_from_seq)
            .await
            .unwrap();
        assert_eq!(from_horizon, vec![(s3, b"c".to_vec())]);
    }

    /// Retention never removes an entry with seq >= pin_seq; a capacity cap that
    /// cannot be honored sets `capacity_pinned`.
    pub async fn pin_invariant(be: Arc<dyn StorageBackend>) {
        let mut seqs = Vec::new();
        for i in 0..5u8 {
            seqs.push(
                be.log_append_if_absent(NS, "s:replay", &format!("c{i}"), &[i])
                    .await
                    .unwrap()
                    .seq,
            );
        }
        // Pin at the 3rd entry: only the first two are removable.
        let out = be
            .log_truncate(
                NS,
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
            .log_since(NS, "s:replay", 0)
            .await
            .unwrap()
            .into_iter()
            .map(|(s, _)| s)
            .collect();
        assert_eq!(remaining, vec![seqs[2], seqs[3], seqs[4]]);
    }

    /// A truncated-empty stream keeps its horizon, distinguishing it from a stream
    /// that never held data.
    pub async fn empty_vs_truncated(be: Arc<dyn StorageBackend>) {
        // Never-had-data.
        let fresh = be.log_stream_meta(NS, "s:fresh").await.unwrap();
        assert_eq!(fresh.min_valid_from_seq, 0);
        assert_eq!(fresh.head_seq, 0);
        assert_eq!(fresh.oldest_seq, None);
        assert_eq!(fresh.len, 0);

        let s1 = be
            .log_append_if_absent(NS, "s:t", "c1", b"a")
            .await
            .unwrap()
            .seq;
        let s2 = be
            .log_append_if_absent(NS, "s:t", "c2", b"b")
            .await
            .unwrap()
            .seq;
        let out = be
            .log_truncate(
                NS,
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
        let meta = be.log_stream_meta(NS, "s:t").await.unwrap();
        assert_eq!(meta.oldest_seq, None);
        assert_eq!(meta.len, 0);
        assert_eq!(meta.min_valid_from_seq, s2, "horizon preserved when empty");
        assert_eq!(meta.head_seq, s2, "head never rewinds");
        assert!(s1 < s2);
    }

    /// Concurrent identical appends collapse to one entry; concurrent same-id
    /// different-value appends yield exactly one insert and one conflict.
    pub async fn concurrent_appends(be: Arc<dyn StorageBackend>) {
        // Identical.
        let b1 = Arc::clone(&be);
        let b2 = Arc::clone(&be);
        let (r1, r2) = tokio::join!(
            tokio::spawn(async move {
                b1.log_append_if_absent(NS, "s:c", "cap", b"same").await
            }),
            tokio::spawn(async move {
                b2.log_append_if_absent(NS, "s:c", "cap", b"same").await
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
        assert_eq!(be.log_since(NS, "s:c", 0).await.unwrap().len(), 1);

        // Same id, different value.
        let b3 = Arc::clone(&be);
        let b4 = Arc::clone(&be);
        let (r3, r4) = tokio::join!(
            tokio::spawn(async move {
                b3.log_append_if_absent(NS, "s:d", "cap", b"one").await
            }),
            tokio::spawn(async move {
                b4.log_append_if_absent(NS, "s:d", "cap", b"two").await
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
        assert_eq!(be.log_since(NS, "s:d", 0).await.unwrap().len(), 1);
    }
}
