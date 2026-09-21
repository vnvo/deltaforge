//! Atomic, versioned state for stable-snapshot **generation allocation**.
//!
//! Snapshot generations must be allocated so that two starts can never mint the
//! same generation for two different snapshot passes. That requires an atomic
//! compare-and-swap; a non-atomic default would let code compile while silently
//! violating the guarantee, so this is a *small, dedicated* interface separate
//! from [`CheckpointStore`](crate::CheckpointStore) with **no** blanket default.
//! Every implementation provides real atomicity or returns
//! [`CheckpointError::UnsupportedAtomicOperation`](crate::CheckpointError::UnsupportedAtomicOperation).
//!
//! **Boundary:** CAS guarantees a *unique* allocation, but it does not prevent
//! two separate process owners from each running a *different* generation
//! concurrently. Until leases/fencing exist, snapshot execution retains a
//! single-owner constraint — CAS alone is not full multi-instance snapshot
//! safety.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::CheckpointResult;

/// On-store envelope used by the flat key/value backends (mem/file/sqlite) to
/// carry a version alongside the value. The storage-backed adapter uses native
/// versioned slots instead and does not use this.
#[derive(Serialize, Deserialize)]
pub(crate) struct VersionedRecord {
    pub version: u64,
    pub payload: Vec<u8>,
}

impl VersionedRecord {
    /// Decode a `(version, payload)` pair from stored bytes.
    pub(crate) fn decode(bytes: &[u8]) -> CheckpointResult<(u64, Vec<u8>)> {
        let rec: VersionedRecord = serde_json::from_slice(bytes)?;
        Ok((rec.version, rec.payload))
    }

    /// Encode a versioned value for storage.
    pub(crate) fn encode(
        version: u64,
        payload: &[u8],
    ) -> CheckpointResult<Vec<u8>> {
        Ok(serde_json::to_vec(&VersionedRecord {
            version,
            payload: payload.to_vec(),
        })?)
    }
}

/// Decide the outcome of a CAS given the current record and the expectation.
/// Returns `Some(new_version)` to write, or `None` to reject as a mismatch.
pub(crate) fn cas_decision(
    current: &Option<(u64, Vec<u8>)>,
    expected_version: Option<u64>,
) -> Option<u64> {
    let matches = match (current, expected_version) {
        (None, None) => true,
        (Some((v, _)), Some(e)) => *v == e,
        _ => false,
    };
    if !matches {
        return None;
    }
    Some(current.as_ref().map(|(v, _)| v + 1).unwrap_or(1))
}

/// Outcome of a [`SnapshotStateStore::compare_and_swap`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CasOutcome {
    /// The write committed. Carries the new version.
    Committed { version: u64 },
    /// The expectation failed (version mismatch, or the key existed when
    /// create-if-absent was requested, or vice-versa). Carries the current
    /// record so the caller can retry without a separate read.
    Mismatch { current: Option<(u64, Vec<u8>)> },
}

/// Atomic, versioned key/value used to allocate snapshot generations safely.
///
/// Versions start at `1` on first creation and increment by one per successful
/// swap. `expected_version` is an [`Option`]: `None` means *expect-absent* (the
/// atomic-create case), `Some(v)` means *expect the stored version to be `v`*.
/// Using an `Option` — rather than a bare `u64` — is what makes initial creation
/// race-free; a plain `expected_version: u64` leaves the create race unresolved.
#[async_trait]
pub trait SnapshotStateStore: Send + Sync {
    /// Read the current versioned record, or `None` if absent.
    async fn get_versioned(
        &self,
        key: &str,
    ) -> CheckpointResult<Option<(u64, Vec<u8>)>>;

    /// Atomically write `value` iff the stored version matches
    /// `expected_version` (`None` = expect the key to be absent).
    async fn compare_and_swap(
        &self,
        key: &str,
        expected_version: Option<u64>,
        value: &[u8],
    ) -> CheckpointResult<CasOutcome>;
}

/// Shared contract suite exercised by every [`SnapshotStateStore`]
/// implementation (in this crate and in the storage-backed adapter). Kept in
/// the library — behind `test-support`, and always for this crate's own tests —
/// so all backends prove *identical* semantics rather than each re-deriving
/// them.
#[cfg(any(test, feature = "test-support"))]
pub async fn assert_snapshot_state_contract<S: SnapshotStateStore>(store: &S) {
    let key = "snapshot_generation:contract";

    // Absent to begin with.
    assert_eq!(store.get_versioned(key).await.unwrap(), None);

    // Create-if-absent succeeds at version 1.
    match store.compare_and_swap(key, None, b"g1").await.unwrap() {
        CasOutcome::Committed { version } => assert_eq!(version, 1),
        other => panic!("expected create to commit v1, got {other:?}"),
    }
    assert_eq!(
        store.get_versioned(key).await.unwrap(),
        Some((1, b"g1".to_vec()))
    );

    // A second create-if-absent must NOT overwrite — it reports the current
    // record so the racing loser can back off.
    match store.compare_and_swap(key, None, b"g1-dup").await.unwrap() {
        CasOutcome::Mismatch { current } => {
            assert_eq!(current, Some((1, b"g1".to_vec())));
        }
        other => panic!("expected create-if-absent to reject, got {other:?}"),
    }

    // CAS with a stale version is rejected and does not write.
    match store
        .compare_and_swap(key, Some(99), b"stale")
        .await
        .unwrap()
    {
        CasOutcome::Mismatch { current } => {
            assert_eq!(current, Some((1, b"g1".to_vec())));
        }
        other => panic!("expected stale CAS to reject, got {other:?}"),
    }

    // CAS with the matching version commits at version 2.
    match store.compare_and_swap(key, Some(1), b"g2").await.unwrap() {
        CasOutcome::Committed { version } => assert_eq!(version, 2),
        other => panic!("expected matching CAS to commit v2, got {other:?}"),
    }
    assert_eq!(
        store.get_versioned(key).await.unwrap(),
        Some((2, b"g2".to_vec()))
    );
}
