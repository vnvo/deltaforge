//! Conditional-write abstraction for crash-durable S3 acknowledgements (P0.4).
//!
//! The durable path fences writers and enforces immutability with S3 conditional
//! writes:
//! - create-only (`If-None-Match: *`) for immutable data objects and manifest
//!   entries, and
//! - compare-and-swap (`If-Match: <etag>`) for advancing the manifest HEAD.
//!
//! This module wraps `object_store`'s native conditional support
//! (`PutMode::Create` / `PutMode::Update`) behind a small trait so the durable
//! sink, recovery, compaction and reconciliation code depend on the capability,
//! not the backend. A non-destructive startup probe
//! ([`probe_conditional_writes`]) verifies the backend actually honors the
//! conditions; durable mode must refuse to start on a backend that does not.
//!
//! Staged scaffolding: the abstraction and probe land first (P0.4 commit 1); the
//! durable sink, recovery, compaction and reconciliation wire it in over the
//! following commits. The module-level `allow(dead_code)` is removed as each item
//! gains a non-test caller.
#![allow(dead_code)]

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use object_store::{
    Error as OsError, ObjectStore, ObjectStoreExt, PutMode, PutOptions,
    PutPayload, UpdateVersion, path::Path,
};

/// Outcome of a conditional write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PutOutcome {
    /// The object was written by this call (create succeeded, or CAS matched).
    /// Carries the new ETag when the backend returns one.
    Written { etag: Option<String> },
    /// A create-only write found the key already present (`If-None-Match: *`
    /// rejected). An idempotent retry of the same content lands here.
    AlreadyExists,
    /// A CAS write found the expected ETag no longer current (`If-Match`
    /// rejected) - the writer is stale or lost a race and must reconcile.
    Conflict,
}

/// A conditional-write failure that is *not* a normal conditional outcome
/// (`AlreadyExists`/`Conflict` are outcomes, not errors).
#[derive(Debug, thiserror::Error)]
pub enum CondError {
    #[error("object store error: {0}")]
    Store(String),
    /// The backend does not honor conditional writes (probe failed). Durable
    /// mode must fail closed rather than degrade to unsafe overwrites.
    #[error("conditional writes unsupported by object store: {0}")]
    Unsupported(String),
}

pub type CondResult<T> = Result<T, CondError>;

/// Create-only and compare-and-swap object writes with ETag capture. The single
/// fencing primitive for durable S3 acknowledgements.
#[async_trait]
pub trait ConditionalStore: Send + Sync {
    /// Create `key` only if absent (`If-None-Match: *`). `AlreadyExists` proves
    /// only that the key is present - **not** that its content matches. Callers
    /// treating a retry as idempotent success MUST read the existing object and
    /// validate its hash/content first; otherwise a hash collision or a
    /// conflicting object could be silently accepted.
    async fn put_if_absent(
        &self,
        key: &Path,
        bytes: Bytes,
    ) -> CondResult<PutOutcome>;

    /// Overwrite `key` only if its current ETag equals `expected` (`If-Match`).
    /// `Conflict` means the ETag moved on (stale writer / lost race).
    async fn cas_put(
        &self,
        key: &Path,
        bytes: Bytes,
        expected_etag: &str,
    ) -> CondResult<PutOutcome>;

    /// Read `key`'s bytes and current ETag, or `None` if absent.
    async fn get_with_etag(
        &self,
        key: &Path,
    ) -> CondResult<Option<(Bytes, Option<String>)>>;

    /// Delete `key`. Used by reconciliation/compaction only - never on the ack
    /// path. Deleting an absent key is a no-op.
    async fn delete(&self, key: &Path) -> CondResult<()>;
}

/// [`ConditionalStore`] backed by an `object_store` client.
pub struct ObjectStoreConditional {
    store: Arc<dyn ObjectStore>,
}

impl ObjectStoreConditional {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl ConditionalStore for ObjectStoreConditional {
    async fn put_if_absent(
        &self,
        key: &Path,
        bytes: Bytes,
    ) -> CondResult<PutOutcome> {
        let opts = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        match self
            .store
            .put_opts(key, PutPayload::from(bytes), opts)
            .await
        {
            Ok(r) => Ok(PutOutcome::Written { etag: r.e_tag }),
            Err(OsError::AlreadyExists { .. }) => Ok(PutOutcome::AlreadyExists),
            Err(e) => Err(CondError::Store(format!("{e}"))),
        }
    }

    async fn cas_put(
        &self,
        key: &Path,
        bytes: Bytes,
        expected_etag: &str,
    ) -> CondResult<PutOutcome> {
        let opts = PutOptions {
            mode: PutMode::Update(UpdateVersion {
                e_tag: Some(expected_etag.to_string()),
                version: None,
            }),
            ..Default::default()
        };
        match self
            .store
            .put_opts(key, PutPayload::from(bytes), opts)
            .await
        {
            Ok(r) => Ok(PutOutcome::Written { etag: r.e_tag }),
            // Precondition = ETag moved on; NotFound = the object we meant to
            // replace is gone. Both mean "stale writer, reconcile", not failure.
            Err(OsError::Precondition { .. })
            | Err(OsError::NotFound { .. }) => Ok(PutOutcome::Conflict),
            Err(e) => Err(CondError::Store(format!("{e}"))),
        }
    }

    async fn get_with_etag(
        &self,
        key: &Path,
    ) -> CondResult<Option<(Bytes, Option<String>)>> {
        match self.store.get(key).await {
            Ok(res) => {
                let etag = res.meta.e_tag.clone();
                let bytes = res
                    .bytes()
                    .await
                    .map_err(|e| CondError::Store(format!("{e}")))?;
                Ok(Some((bytes, etag)))
            }
            Err(OsError::NotFound { .. }) => Ok(None),
            Err(e) => Err(CondError::Store(format!("{e}"))),
        }
    }

    async fn delete(&self, key: &Path) -> CondResult<()> {
        match self.store.delete(key).await {
            Ok(()) | Err(OsError::NotFound { .. }) => Ok(()),
            Err(e) => Err(CondError::Store(format!("{e}"))),
        }
    }
}

/// Non-destructive capability probe. Verifies the backend honors create-only and
/// CAS conditions by exercising them **only under a key this probe creates** under
/// `prefix/_probe/`, then deleting exactly that key. It never issues an
/// unconditional delete and never touches a key it did not create, so it cannot
/// disturb a concurrent probe on another host.
///
/// Returns `Err` if the backend silently ignores a condition
/// (`CondError::Unsupported`), or if the probe created an object but could not
/// clean it up (`CondError::Store`) - a successful probe must leave no trace, so
/// cleanup failure is surfaced rather than silently reported as success. Durable
/// mode must treat any `Err` as a fatal, fail-closed startup error.
pub async fn probe_conditional_writes<S: ConditionalStore + ?Sized>(
    store: &S,
    prefix: &str,
) -> CondResult<()> {
    // A UUIDv4 collision is astronomically unlikely; retry a few times only to be
    // safe, and never delete a colliding key - it may belong to another process.
    const MAX_ATTEMPTS: usize = 5;
    for _ in 0..MAX_ATTEMPTS {
        let key =
            Path::from(format!("{prefix}/_probe/{}", uuid::Uuid::new_v4()));
        let created = store
            .put_if_absent(&key, Bytes::from_static(b"probe-1"))
            .await?;
        match created {
            PutOutcome::Written { .. } => {
                // We own this key. Run the checks, then delete exactly it.
                let checks = probe_steps(store, &key).await;
                let cleanup = store.delete(&key).await;
                checks?;
                cleanup.map_err(|e| {
                    CondError::Store(format!(
                        "probe checks passed but cleanup of {key} failed: {e}"
                    ))
                })?;
                return Ok(());
            }
            PutOutcome::AlreadyExists => {
                // Collision with another probe's key: try a fresh key; do NOT
                // delete it - it is not ours.
                continue;
            }
            other => {
                return Err(CondError::Unsupported(format!(
                    "create-only on a fresh probe key returned {other:?}"
                )));
            }
        }
    }
    Err(CondError::Store(format!(
        "probe could not acquire a fresh key after {MAX_ATTEMPTS} attempts"
    )))
}

/// The probe's conditional-write checks on a key this probe already created with
/// `b"probe-1"`. Cleanup of the key is the caller's responsibility.
async fn probe_steps<S: ConditionalStore + ?Sized>(
    store: &S,
    key: &Path,
) -> CondResult<()> {
    // Capture the create's ETag and verify the create stored what we wrote.
    let etag = match store.get_with_etag(key).await? {
        Some((bytes, Some(etag))) => {
            if bytes.as_ref() != b"probe-1" {
                return Err(CondError::Unsupported(
                    "create-only did not store the expected bytes".into(),
                ));
            }
            etag
        }
        Some((_, None)) => {
            return Err(CondError::Unsupported(
                "backend did not return an ETag".into(),
            ));
        }
        None => {
            return Err(CondError::Unsupported(
                "created object not found on read-back".into(),
            ));
        }
    };

    // A second create-only MUST be rejected (not overwrite).
    match store
        .put_if_absent(key, Bytes::from_static(b"probe-2"))
        .await?
    {
        PutOutcome::AlreadyExists => {}
        other => {
            return Err(CondError::Unsupported(format!(
                "If-None-Match ignored: second create returned {other:?}"
            )));
        }
    }

    // CAS with a wrong ETag MUST conflict.
    match store
        .cas_put(key, Bytes::from_static(b"probe-3"), "\"definitely-wrong\"")
        .await?
    {
        PutOutcome::Conflict => {}
        other => {
            return Err(CondError::Unsupported(format!(
                "If-Match ignored: stale CAS returned {other:?}"
            )));
        }
    }

    // CAS with the correct ETag MUST succeed and yield a fresh ETag.
    let new_etag = match store
        .cas_put(key, Bytes::from_static(b"probe-4"), &etag)
        .await?
    {
        PutOutcome::Written { etag: Some(e) } => e,
        PutOutcome::Written { etag: None } => {
            return Err(CondError::Unsupported(
                "CAS succeeded but returned no ETag".into(),
            ));
        }
        other => {
            return Err(CondError::Unsupported(format!(
                "If-Match rejected a matching CAS: {other:?}"
            )));
        }
    };
    if new_etag == etag {
        return Err(CondError::Unsupported(
            "CAS did not advance the ETag".into(),
        ));
    }

    // Re-read and verify the CAS actually took effect and a usable ETag exists.
    match store.get_with_etag(key).await? {
        Some((bytes, Some(cur))) => {
            if bytes.as_ref() != b"probe-4" {
                return Err(CondError::Unsupported(
                    "CAS reported success but content did not change".into(),
                ));
            }
            if cur.is_empty() {
                return Err(CondError::Unsupported(
                    "no usable current ETag after CAS".into(),
                ));
            }
        }
        _ => {
            return Err(CondError::Unsupported(
                "object missing or ETag-less after CAS".into(),
            ));
        }
    }

    // The stale pre-CAS ETag MUST no longer be accepted.
    match store
        .cas_put(key, Bytes::from_static(b"probe-5"), &etag)
        .await?
    {
        PutOutcome::Conflict => {}
        other => {
            return Err(CondError::Unsupported(format!(
                "stale ETag accepted after CAS: {other:?}"
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::sync::Mutex;

    /// In-memory store with correct conditional semantics + monotonic ETags.
    #[derive(Default)]
    struct MockCond {
        // key -> (bytes, etag)
        map: Mutex<HashMap<String, (Bytes, String)>>,
        etag_seq: AtomicU64,
        fail_delete: std::sync::atomic::AtomicBool,
    }

    impl MockCond {
        fn next_etag(&self) -> String {
            format!("\"e{}\"", self.etag_seq.fetch_add(1, Ordering::Relaxed))
        }
    }

    #[async_trait]
    impl ConditionalStore for MockCond {
        async fn put_if_absent(
            &self,
            key: &Path,
            bytes: Bytes,
        ) -> CondResult<PutOutcome> {
            let mut m = self.map.lock().await;
            let k = key.to_string();
            if m.contains_key(&k) {
                return Ok(PutOutcome::AlreadyExists);
            }
            let etag = self.next_etag();
            m.insert(k, (bytes, etag.clone()));
            Ok(PutOutcome::Written { etag: Some(etag) })
        }

        async fn cas_put(
            &self,
            key: &Path,
            bytes: Bytes,
            expected_etag: &str,
        ) -> CondResult<PutOutcome> {
            let mut m = self.map.lock().await;
            let k = key.to_string();
            match m.get(&k) {
                Some((_, cur)) if cur == expected_etag => {
                    let etag = self.next_etag();
                    m.insert(k, (bytes, etag.clone()));
                    Ok(PutOutcome::Written { etag: Some(etag) })
                }
                _ => Ok(PutOutcome::Conflict),
            }
        }

        async fn get_with_etag(
            &self,
            key: &Path,
        ) -> CondResult<Option<(Bytes, Option<String>)>> {
            let m = self.map.lock().await;
            Ok(m.get(&key.to_string())
                .map(|(b, e)| (b.clone(), Some(e.clone()))))
        }

        async fn delete(&self, key: &Path) -> CondResult<()> {
            if self.fail_delete.load(Ordering::Relaxed) {
                return Err(CondError::Store("injected delete failure".into()));
            }
            self.map.lock().await.remove(&key.to_string());
            Ok(())
        }
    }

    /// A backend that ignores conditions: every write overwrites and reports
    /// success. Models a non-conforming provider the probe must reject.
    #[derive(Default)]
    struct IgnoresConditions {
        map: Mutex<HashMap<String, Bytes>>,
    }

    #[async_trait]
    impl ConditionalStore for IgnoresConditions {
        async fn put_if_absent(
            &self,
            key: &Path,
            bytes: Bytes,
        ) -> CondResult<PutOutcome> {
            self.map.lock().await.insert(key.to_string(), bytes);
            Ok(PutOutcome::Written {
                etag: Some("\"x\"".into()),
            })
        }
        async fn cas_put(
            &self,
            key: &Path,
            bytes: Bytes,
            _expected: &str,
        ) -> CondResult<PutOutcome> {
            self.map.lock().await.insert(key.to_string(), bytes);
            Ok(PutOutcome::Written {
                etag: Some("\"x\"".into()),
            })
        }
        async fn get_with_etag(
            &self,
            key: &Path,
        ) -> CondResult<Option<(Bytes, Option<String>)>> {
            Ok(self
                .map
                .lock()
                .await
                .get(&key.to_string())
                .map(|b| (b.clone(), Some("\"x\"".to_string()))))
        }
        async fn delete(&self, key: &Path) -> CondResult<()> {
            self.map.lock().await.remove(&key.to_string());
            Ok(())
        }
    }

    #[tokio::test]
    async fn put_if_absent_is_create_only() {
        let s = MockCond::default();
        let k = Path::from("a/b");
        assert!(matches!(
            s.put_if_absent(&k, Bytes::from_static(b"1")).await.unwrap(),
            PutOutcome::Written { .. }
        ));
        assert_eq!(
            s.put_if_absent(&k, Bytes::from_static(b"2")).await.unwrap(),
            PutOutcome::AlreadyExists
        );
        // The first content is preserved (no overwrite).
        let (bytes, _) = s.get_with_etag(&k).await.unwrap().unwrap();
        assert_eq!(&bytes[..], b"1");
    }

    #[tokio::test]
    async fn cas_put_matches_and_conflicts_on_etag() {
        let s = MockCond::default();
        let k = Path::from("head");
        s.put_if_absent(&k, Bytes::from_static(b"v0"))
            .await
            .unwrap();
        let etag = s.get_with_etag(&k).await.unwrap().unwrap().1.unwrap();

        // Stale ETag → Conflict, content unchanged.
        assert_eq!(
            s.cas_put(&k, Bytes::from_static(b"vX"), "\"stale\"")
                .await
                .unwrap(),
            PutOutcome::Conflict
        );
        // Correct ETag → Written.
        assert!(matches!(
            s.cas_put(&k, Bytes::from_static(b"v1"), &etag)
                .await
                .unwrap(),
            PutOutcome::Written { .. }
        ));
        // The old ETag is now stale.
        assert_eq!(
            s.cas_put(&k, Bytes::from_static(b"v2"), &etag)
                .await
                .unwrap(),
            PutOutcome::Conflict
        );
    }

    #[tokio::test]
    async fn probe_passes_on_conforming_store_and_leaves_no_trace() {
        let s = MockCond::default();
        probe_conditional_writes(&s, "pfx").await.unwrap();
        // Non-destructive: the probe cleaned up after itself.
        assert!(s.map.lock().await.is_empty());
    }

    #[tokio::test]
    async fn probe_fails_on_store_that_ignores_conditions() {
        let s = IgnoresConditions::default();
        let err = probe_conditional_writes(&s, "pfx").await.unwrap_err();
        assert!(matches!(err, CondError::Unsupported(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn probe_surfaces_cleanup_failure_instead_of_claiming_success() {
        // Conditional semantics are correct, but the probe object cannot be
        // removed. A successful probe must leave no trace, so this is an error,
        // not a silent success.
        let s = MockCond {
            fail_delete: std::sync::atomic::AtomicBool::new(true),
            ..Default::default()
        };
        let err = probe_conditional_writes(&s, "pfx").await.unwrap_err();
        assert!(
            matches!(err, CondError::Store(_)),
            "cleanup failure must surface as an error, got {err:?}"
        );
    }
}
