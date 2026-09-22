//! CAS-protected manifest HEAD and the durable acknowledgement boundary (P0.4).
//!
//! `_manifest/HEAD` is the single authoritative pointer for a pipeline. A batch
//! is durable-and-acknowledged only when a HEAD compare-and-swap that references
//! its manifest entry succeeds. The HEAD CAS is therefore the *sole* publication
//! and acknowledgement boundary: uploading objects and writing a manifest entry
//! are necessary but never sufficient.
//!
//! Fencing: a writer acquires an epoch by conditionally advancing HEAD and may
//! publish only while HEAD still carries that epoch. Observing a higher epoch
//! fences the writer permanently (it must not reacquire and continue); this is
//! what prevents a stale writer from regressing the watermark after a newer epoch
//! has published. Epoch overflow, a corrupt/inconsistent HEAD, or a missing
//! referenced entry are hard integrity failures, never silently repaired.
//!
//! Staged scaffolding: the Sink integration wires this in a later commit.
#![allow(dead_code)]

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use super::batch_upload::{DurableError, TableObject, upload_batch};
use super::manifest::{
    ManifestEntry, ManifestObject, PrevRef, WrittenEntry, propose_seq,
    write_entry,
};
use super::store_cond::{ConditionalStore, PutOutcome};

/// Canonical HEAD version. Bump if the HEAD byte layout changes.
pub const HEAD_VERSION: u16 = 1;

/// Bounded reconcile attempts for a single publish before giving up (retryable).
const MAX_PUBLISH_ATTEMPTS: usize = 8;

/// The authoritative HEAD state. Genesis is `epoch >= 1, seq == 0` with no entry
/// or watermark; after the first publish `seq >= 1` with entry + watermark set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Head {
    pub version: u16,
    pub epoch: u64,
    pub seq: u64,
    pub head_entry_key: Option<String>,
    pub head_entry_hash: Option<String>,
    pub rollup_key: Option<String>,
    pub rollup_hash: Option<String>,
    pub watermark_hex: Option<String>,
}

impl Head {
    fn genesis(epoch: u64) -> Self {
        Self {
            version: HEAD_VERSION,
            epoch,
            seq: 0,
            head_entry_key: None,
            head_entry_hash: None,
            rollup_key: None,
            rollup_hash: None,
            watermark_hex: None,
        }
    }

    fn canonical_bytes(&self) -> bytes::Bytes {
        bytes::Bytes::from(serde_json::to_vec(self).expect("head serializes"))
    }

    fn parse(raw: &[u8]) -> Result<Self, HeadError> {
        let h: Head = serde_json::from_slice(raw).map_err(|e| {
            HeadError::Integrity(format!("HEAD not parseable: {e}"))
        })?;
        h.verify_consistency()?;
        Ok(h)
    }

    /// Structural invariants that must hold for any stored HEAD.
    fn verify_consistency(&self) -> Result<(), HeadError> {
        if self.version > HEAD_VERSION {
            return Err(HeadError::Integrity(format!(
                "HEAD version {} newer than supported {}",
                self.version, HEAD_VERSION
            )));
        }
        if self.epoch == 0 {
            return Err(HeadError::Integrity("HEAD epoch is zero".into()));
        }
        let entry_set = self.head_entry_key.is_some();
        // key/hash/watermark travel together; and seq==0 iff there is no entry.
        if entry_set != self.head_entry_hash.is_some()
            || entry_set != self.watermark_hex.is_some()
        {
            return Err(HeadError::Integrity(
                "HEAD entry key/hash/watermark not all-set-or-all-unset".into(),
            ));
        }
        if entry_set == (self.seq == 0) {
            return Err(HeadError::Integrity(
                "HEAD seq/entry inconsistent (seq==0 iff genesis)".into(),
            ));
        }
        if self.rollup_key.is_some() != self.rollup_hash.is_some() {
            return Err(HeadError::Integrity(
                "HEAD rollup key/hash not both-set-or-both-unset".into(),
            ));
        }
        Ok(())
    }

    fn references(&self, entry: &WrittenEntry) -> bool {
        self.head_entry_key.as_deref() == Some(entry.key.as_str())
            && self.head_entry_hash.as_deref() == Some(entry.hash.as_str())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HeadError {
    /// Transient; the coordinator may replay the batch (idempotent).
    #[error("object store: {0}")]
    Store(String),
    /// Corrupt/inconsistent HEAD, missing referenced entry, epoch regression, or
    /// epoch overflow. Never silently repaired.
    #[error("integrity: {0}")]
    Integrity(String),
    /// A higher epoch has published: this writer is permanently fenced and must
    /// not reacquire or continue.
    #[error("fenced: our epoch {our} superseded by epoch {observed}")]
    Fenced { our: u64, observed: u64 },
    /// HEAD is missing but manifest state exists: a recovery condition, not an
    /// empty initialization.
    #[error("recovery required: {0}")]
    RecoveryRequired(String),
    /// Bounded reconcile attempts exhausted; retryable.
    #[error("publish did not converge after {0} attempts")]
    RetriesExhausted(usize),
    /// A data-object or manifest-entry integrity failure while publishing.
    #[error("durable: {0}")]
    Durable(String),
}

impl HeadError {
    /// Fatal errors stop the pipeline; non-fatal ones are retried by replay.
    pub fn is_fatal(&self) -> bool {
        matches!(
            self,
            HeadError::Integrity(_)
                | HeadError::Fenced { .. }
                | HeadError::RecoveryRequired(_)
        )
    }
}

fn head_key(prefix: &str, pipeline: &str) -> object_store::path::Path {
    object_store::path::Path::from_iter(
        prefix
            .split('/')
            .filter(|p| !p.is_empty())
            .chain([pipeline, "_manifest", "HEAD"])
            .map(str::to_string),
    )
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Per-instance mutable state, guarded so concurrent `send_batch` calls on one
/// sink instance serialize their HEAD publication and cannot race.
struct WriterState {
    epoch: u64,
    head: Head,
    /// ETag of the last-known HEAD, for the next `If-Match` CAS.
    etag: String,
    /// Once fenced, every further publish fails permanently.
    fenced: Option<u64>,
}

/// Owns a writer epoch and publishes batches through the fenced HEAD chain.
pub struct DurableWriter<S: ConditionalStore + ?Sized> {
    store: Arc<S>,
    prefix: String,
    pipeline: String,
    source_id: String,
    sink_id: String,
    state: Mutex<WriterState>,
}

impl<S: ConditionalStore + ?Sized> DurableWriter<S> {
    /// Acquire an epoch for `(prefix, pipeline)`.
    ///
    /// - Existing HEAD: verify it and its referenced entry, then CAS-advance the
    ///   epoch (overflow is fatal). This fences any prior writer.
    /// - No HEAD, no manifest entries: create the genesis HEAD create-only.
    /// - No HEAD but entries exist: `RecoveryRequired` (never empty-init).
    pub async fn acquire(
        store: Arc<S>,
        prefix: &str,
        pipeline: &str,
        source_id: &str,
        sink_id: &str,
    ) -> Result<Self, HeadError> {
        let hkey = head_key(prefix, pipeline);
        let (head, etag) = match store
            .get_with_etag(&hkey)
            .await
            .map_err(|e| HeadError::Store(e.to_string()))?
        {
            Some((raw, Some(etag))) => {
                let head = Head::parse(&raw)?;
                Self::verify_referenced_entry(store.as_ref(), &head).await?;
                let new_epoch = head.epoch.checked_add(1).ok_or_else(|| {
                    HeadError::Integrity("epoch overflow".into())
                })?;
                let next = Head {
                    epoch: new_epoch,
                    ..head.clone()
                };
                match store
                    .cas_put(&hkey, next.canonical_bytes(), &etag)
                    .await
                    .map_err(|e| HeadError::Store(e.to_string()))?
                {
                    PutOutcome::Written {
                        etag: Some(new_etag),
                    } => (next, new_etag),
                    PutOutcome::Written { etag: None } => {
                        return Err(HeadError::Store(
                            "HEAD CAS returned no ETag".into(),
                        ));
                    }
                    PutOutcome::Conflict => {
                        return Err(HeadError::Store(
                            "lost the epoch-acquisition race; retry acquire"
                                .into(),
                        ));
                    }
                    PutOutcome::AlreadyExists => unreachable!("cas_put"),
                }
            }
            Some((_, None)) => {
                return Err(HeadError::Integrity(
                    "HEAD has no ETag; cannot fence".into(),
                ));
            }
            None => {
                // No HEAD. If any manifest entries exist this is recovery.
                let entries_prefix = object_store::path::Path::from_iter(
                    prefix
                        .split('/')
                        .filter(|p| !p.is_empty())
                        .chain([pipeline, "_manifest", "entries"])
                        .map(str::to_string),
                );
                let existing = store
                    .list(&entries_prefix)
                    .await
                    .map_err(|e| HeadError::Store(e.to_string()))?;
                if !existing.is_empty() {
                    return Err(HeadError::RecoveryRequired(format!(
                        "HEAD missing but {} manifest entr(ies) exist",
                        existing.len()
                    )));
                }
                let genesis = Head::genesis(1);
                match store
                    .put_if_absent(&hkey, genesis.canonical_bytes())
                    .await
                    .map_err(|e| HeadError::Store(e.to_string()))?
                {
                    PutOutcome::Written { etag: Some(etag) } => (genesis, etag),
                    PutOutcome::Written { etag: None } => {
                        return Err(HeadError::Store(
                            "genesis HEAD create returned no ETag".into(),
                        ));
                    }
                    PutOutcome::AlreadyExists => {
                        return Err(HeadError::Store(
                            "HEAD created concurrently; retry acquire".into(),
                        ));
                    }
                    PutOutcome::Conflict => unreachable!("put_if_absent"),
                }
            }
        };

        let epoch = head.epoch;
        Ok(Self {
            store,
            prefix: prefix.to_string(),
            pipeline: pipeline.to_string(),
            source_id: source_id.to_string(),
            sink_id: sink_id.to_string(),
            state: Mutex::new(WriterState {
                epoch,
                head,
                etag,
                fenced: None,
            }),
        })
    }

    async fn verify_referenced_entry(
        store: &S,
        head: &Head,
    ) -> Result<(), HeadError> {
        if let (Some(key), Some(hash)) =
            (&head.head_entry_key, &head.head_entry_hash)
        {
            let path = object_store::path::Path::from(key.as_str());
            match store
                .get_with_etag(&path)
                .await
                .map_err(|e| HeadError::Store(e.to_string()))?
            {
                Some((raw, _)) => {
                    // The stored entry's canonical hash must match HEAD's ref.
                    let entry: ManifestEntry = serde_json::from_slice(&raw)
                        .map_err(|e| {
                            HeadError::Integrity(format!(
                                "referenced entry unparseable: {e}"
                            ))
                        })?;
                    if entry.entry_hash() != *hash {
                        return Err(HeadError::Integrity(format!(
                            "referenced entry {key} hash mismatch"
                        )));
                    }
                }
                None => {
                    return Err(HeadError::Integrity(format!(
                        "HEAD references missing entry {key}"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Current epoch (test/observability).
    pub async fn epoch(&self) -> u64 {
        self.state.lock().await.epoch
    }

    /// Publish one batch and acknowledge only on success.
    ///
    /// Returns `Ok(())` **only** after a HEAD CAS that references this batch's
    /// entry succeeds, or after reconciliation proves HEAD already references our
    /// exact entry (idempotent). Any other outcome is an error and must not be
    /// treated as an acknowledgement.
    pub async fn publish(
        &self,
        watermark: &[u8],
        objects: Vec<TableObject>,
        event_count: u64,
    ) -> Result<(), HeadError> {
        let mut st = self.state.lock().await;
        if let Some(observed) = st.fenced {
            return Err(HeadError::Fenced {
                our: st.epoch,
                observed,
            });
        }

        // 1. Make the data objects durable (idempotent; integrity is fatal).
        let uploaded = upload_batch(
            self.store.as_ref(),
            &self.prefix,
            &self.pipeline,
            watermark,
            objects,
        )
        .await
        .map_err(map_durable)?;
        let mobjects: Vec<ManifestObject> = uploaded
            .objects
            .iter()
            .map(ManifestObject::from_uploaded)
            .collect();
        let watermark_hex = hex(watermark);
        let hkey = head_key(&self.prefix, &self.pipeline);

        // 2. Bounded propose -> write-entry -> HEAD-CAS reconcile loop.
        for _ in 0..MAX_PUBLISH_ATTEMPTS {
            let seq = propose_seq(st.head.seq);
            let prev = match (&st.head.head_entry_key, &st.head.head_entry_hash)
            {
                (Some(k), Some(h)) => Some(PrevRef {
                    key: k.clone(),
                    hash: h.clone(),
                }),
                _ => None, // genesis
            };
            let entry = ManifestEntry {
                version: super::manifest::MANIFEST_ENTRY_VERSION,
                pipeline: self.pipeline.clone(),
                source_id: self.source_id.clone(),
                sink_id: self.sink_id.clone(),
                epoch: st.epoch,
                seq,
                watermark_hex: watermark_hex.clone(),
                event_count,
                objects: mobjects.clone(),
                prev,
            };
            let written =
                write_entry(self.store.as_ref(), &self.prefix, &entry)
                    .await
                    .map_err(map_manifest)?;

            let next_head = Head {
                version: HEAD_VERSION,
                epoch: st.epoch,
                seq,
                head_entry_key: Some(written.key.clone()),
                head_entry_hash: Some(written.hash.clone()),
                // Rollup references are preserved unchanged during ordinary
                // batch publication.
                rollup_key: st.head.rollup_key.clone(),
                rollup_hash: st.head.rollup_hash.clone(),
                watermark_hex: Some(watermark_hex.clone()),
            };

            let cas = self
                .store
                .cas_put(&hkey, next_head.canonical_bytes(), &st.etag)
                .await;

            match cas {
                Ok(PutOutcome::Written {
                    etag: Some(new_etag),
                }) => {
                    st.head = next_head;
                    st.etag = new_etag;
                    return Ok(()); // ACKNOWLEDGE
                }
                Ok(PutOutcome::Written { etag: None }) => {
                    return Err(HeadError::Store(
                        "HEAD CAS returned no ETag".into(),
                    ));
                }
                Ok(PutOutcome::AlreadyExists) => unreachable!("cas_put"),
                // Conflict or a lost/ambiguous response: reread and disambiguate.
                Ok(PutOutcome::Conflict) | Err(_) => {
                    let (raw, etag) = match self
                        .store
                        .get_with_etag(&hkey)
                        .await
                        .map_err(|e| HeadError::Store(e.to_string()))?
                    {
                        Some((raw, Some(etag))) => (raw, etag),
                        Some((_, None)) => {
                            return Err(HeadError::Integrity(
                                "HEAD lost its ETag".into(),
                            ));
                        }
                        None => {
                            return Err(HeadError::Integrity(
                                "HEAD vanished during publish".into(),
                            ));
                        }
                    };
                    let cur = Head::parse(&raw)?;

                    // Lost/ambiguous response that actually succeeded: HEAD
                    // references our exact entry -> idempotent success.
                    if cur.references(&written) {
                        st.head = cur;
                        st.etag = etag;
                        return Ok(()); // ACKNOWLEDGE (idempotent)
                    }
                    // A higher epoch published: fenced, permanently.
                    if cur.epoch > st.epoch {
                        st.fenced = Some(cur.epoch);
                        return Err(HeadError::Fenced {
                            our: st.epoch,
                            observed: cur.epoch,
                        });
                    }
                    // A lower epoch is impossible (epochs are monotonic).
                    if cur.epoch < st.epoch {
                        return Err(HeadError::Integrity(format!(
                            "HEAD epoch regressed: {} < our {}",
                            cur.epoch, st.epoch
                        )));
                    }
                    // Same epoch, different head: adopt it and retry with a
                    // fresh seq + prev. (The already-written entry becomes an
                    // unreferenced reconciliation candidate.)
                    st.head = cur;
                    st.etag = etag;
                    continue;
                }
            }
        }
        Err(HeadError::RetriesExhausted(MAX_PUBLISH_ATTEMPTS))
    }
}

fn map_durable(e: DurableError) -> HeadError {
    match e {
        DurableError::Integrity { .. } => HeadError::Integrity(e.to_string()),
        DurableError::Store(s) => HeadError::Store(s),
    }
}

fn map_manifest(e: super::manifest::ManifestError) -> HeadError {
    use super::manifest::ManifestError;
    match e {
        ManifestError::Integrity { .. } => HeadError::Integrity(e.to_string()),
        ManifestError::Store(s) => HeadError::Store(s),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::keys::EncodingDomain;
    use crate::s3::store_cond::{
        CondError, CondResult, ObjectStoreConditional,
    };
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use object_store::path::Path;

    fn tobj(table: &str, bytes: &'static [u8]) -> TableObject {
        TableObject {
            table: table.to_string(),
            bytes: Bytes::from_static(bytes),
            domain: EncodingDomain::new("jsonl", 1, "s1"),
            ext: "jsonl",
        }
    }

    fn cond() -> Arc<ObjectStoreConditional> {
        Arc::new(ObjectStoreConditional::new(Arc::new(InMemory::new())))
    }

    /// Extract the error without requiring the Ok type to be `Debug`.
    fn expect_err<T>(r: Result<T, HeadError>) -> HeadError {
        match r {
            Err(e) => e,
            Ok(_) => panic!("expected Err"),
        }
    }

    async fn writer(
        store: Arc<ObjectStoreConditional>,
    ) -> DurableWriter<ObjectStoreConditional> {
        DurableWriter::acquire(store, "pfx", "pipe", "src", "sink")
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn genesis_acquire_and_first_publish_acknowledges() {
        let s = cond();
        let w = writer(Arc::clone(&s)).await;
        assert_eq!(w.epoch().await, 1);
        w.publish(b"wm1", vec![tobj("orders", b"a")], 1)
            .await
            .unwrap();
        let st = w.state.lock().await;
        assert_eq!(st.head.seq, 1);
        assert!(st.head.head_entry_key.is_some());
        assert_eq!(st.head.watermark_hex.as_deref(), Some("776d31")); // "wm1"
    }

    #[tokio::test]
    async fn second_publish_advances_seq_and_chains() {
        let s = cond();
        let w = writer(Arc::clone(&s)).await;
        w.publish(b"wm1", vec![tobj("orders", b"a")], 1)
            .await
            .unwrap();
        w.publish(b"wm2", vec![tobj("orders", b"b")], 1)
            .await
            .unwrap();
        let st = w.state.lock().await;
        assert_eq!(st.head.seq, 2);
        // rollup refs untouched by ordinary publication.
        assert!(st.head.rollup_key.is_none() && st.head.rollup_hash.is_none());
    }

    #[tokio::test]
    async fn second_acquire_bumps_epoch_and_fences_the_first() {
        let s = cond();
        let a = writer(Arc::clone(&s)).await; // epoch 1
        let b = writer(Arc::clone(&s)).await; // epoch 2
        assert_eq!(a.epoch().await, 1);
        assert_eq!(b.epoch().await, 2);

        // Stale writer a is fenced and stays fenced (no auto-reacquire).
        let e1 = a
            .publish(b"wmX", vec![tobj("orders", b"x")], 1)
            .await
            .unwrap_err();
        assert!(matches!(
            e1,
            HeadError::Fenced {
                our: 1,
                observed: 2
            }
        ));
        assert!(e1.is_fatal());
        let e2 = a
            .publish(b"wmY", vec![tobj("orders", b"y")], 1)
            .await
            .unwrap_err();
        assert!(matches!(e2, HeadError::Fenced { .. }));

        // The newer epoch publishes fine.
        b.publish(b"wm2", vec![tobj("orders", b"z")], 1)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn missing_head_with_existing_entries_requires_recovery() {
        let s = cond();
        // Plant a manifest entry but no HEAD.
        let entry_path = Path::from(
            "pfx/pipe/_manifest/entries/00000000000000000001-deadbeef.json",
        );
        s.put_if_absent(&entry_path, Bytes::from_static(b"{}"))
            .await
            .unwrap();
        let err = expect_err(
            DurableWriter::acquire(
                Arc::clone(&s),
                "pfx",
                "pipe",
                "src",
                "sink",
            )
            .await,
        );
        assert!(matches!(err, HeadError::RecoveryRequired(_)));
        assert!(err.is_fatal());
    }

    #[tokio::test]
    async fn corrupt_head_is_integrity_failure() {
        let s = cond();
        let hkey = head_key("pfx", "pipe");
        s.put_if_absent(&hkey, Bytes::from_static(b"not json"))
            .await
            .unwrap();
        let err = expect_err(
            DurableWriter::acquire(
                Arc::clone(&s),
                "pfx",
                "pipe",
                "src",
                "sink",
            )
            .await,
        );
        assert!(matches!(err, HeadError::Integrity(_)));
    }

    #[tokio::test]
    async fn acquire_rejects_head_referencing_missing_entry() {
        let s = cond();
        let hkey = head_key("pfx", "pipe");
        let bad = Head {
            version: HEAD_VERSION,
            epoch: 1,
            seq: 1,
            head_entry_key: Some("pfx/pipe/_manifest/entries/x.json".into()),
            head_entry_hash: Some("abc".into()),
            rollup_key: None,
            rollup_hash: None,
            watermark_hex: Some("00".into()),
        };
        s.put_if_absent(&hkey, bad.canonical_bytes()).await.unwrap();
        let err = expect_err(
            DurableWriter::acquire(
                Arc::clone(&s),
                "pfx",
                "pipe",
                "src",
                "sink",
            )
            .await,
        );
        assert!(
            matches!(err, HeadError::Integrity(_)),
            "missing referenced entry must be integrity: {err:?}"
        );
    }

    #[tokio::test]
    async fn same_epoch_conflict_reconciles_and_retries() {
        // Acquire, publish once. Then out-of-band advance HEAD at the SAME epoch
        // (simulating a same-epoch race), invalidating the writer's cached ETag.
        // The next publish must reconcile from the new HEAD and still commit.
        let s = cond();
        let w = writer(Arc::clone(&s)).await; // epoch 1, seq 0 -> after publish seq 1
        w.publish(b"wm1", vec![tobj("orders", b"a")], 1)
            .await
            .unwrap();

        // Out-of-band: read HEAD, advance seq to 2 at epoch 1 referencing a
        // (fake but present) entry, using the current ETag.
        let hkey = head_key("pfx", "pipe");
        let (raw, etag) = s.get_with_etag(&hkey).await.unwrap().unwrap();
        let mut cur = Head::parse(&raw).unwrap();
        let fake_entry_key =
            "pfx/pipe/_manifest/entries/00000000000000000002-face.json";
        s.put_if_absent(&Path::from(fake_entry_key), Bytes::from_static(b"{}"))
            .await
            .unwrap();
        cur.seq = 2;
        cur.head_entry_key = Some(fake_entry_key.to_string());
        cur.head_entry_hash = Some("face".into());
        cur.watermark_hex = Some("aa".into());
        s.cas_put(&hkey, cur.canonical_bytes(), etag.as_deref().unwrap())
            .await
            .unwrap();

        // Writer's cached ETag is now stale; publish reconciles (same epoch,
        // different head) and commits at seq 3.
        w.publish(b"wm3", vec![tobj("orders", b"c")], 1)
            .await
            .unwrap();
        let st = w.state.lock().await;
        assert_eq!(st.epoch, 1);
        assert_eq!(st.head.seq, 3);
    }

    // A store wrapper whose Nth cas_put applies the write but reports a Store
    // error, modelling a lost/ambiguous response.
    struct LossyCas {
        inner: Arc<ObjectStoreConditional>,
        fail_on: std::sync::atomic::AtomicUsize,
        seen: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl ConditionalStore for LossyCas {
        async fn put_if_absent(
            &self,
            key: &Path,
            bytes: Bytes,
        ) -> CondResult<PutOutcome> {
            self.inner.put_if_absent(key, bytes).await
        }
        async fn cas_put(
            &self,
            key: &Path,
            bytes: Bytes,
            expected: &str,
        ) -> CondResult<PutOutcome> {
            let n = self.seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let out = self.inner.cas_put(key, bytes, expected).await;
            if n == self.fail_on.load(std::sync::atomic::Ordering::SeqCst) {
                // Apply happened; report the response as lost.
                return Err(CondError::Store("injected lost response".into()));
            }
            out
        }
        async fn get_with_etag(
            &self,
            key: &Path,
        ) -> CondResult<Option<(Bytes, Option<String>)>> {
            self.inner.get_with_etag(key).await
        }
        async fn delete(&self, key: &Path) -> CondResult<()> {
            self.inner.delete(key).await
        }
        async fn list(&self, prefix: &Path) -> CondResult<Vec<Path>> {
            self.inner.list(prefix).await
        }
    }

    #[tokio::test]
    async fn lost_cas_response_rereads_and_acknowledges() {
        use std::sync::atomic::AtomicUsize;
        let inner = cond();
        // Genesis + first publish happen via cas count 0 (acquire genesis uses
        // put_if_absent, not cas). Fail the publish's cas (the 0th cas_put).
        let lossy = Arc::new(LossyCas {
            inner: Arc::clone(&inner),
            fail_on: AtomicUsize::new(0),
            seen: AtomicUsize::new(0),
        });
        let w = DurableWriter::acquire(lossy, "pfx", "pipe", "src", "sink")
            .await
            .unwrap();
        // The publish's HEAD CAS applies but the response is lost; publish must
        // reread, see HEAD referencing its entry, and acknowledge.
        w.publish(b"wm1", vec![tobj("orders", b"a")], 1)
            .await
            .expect("lost response but write succeeded -> ack");
        let st = w.state.lock().await;
        assert_eq!(st.head.seq, 1);
    }
}
