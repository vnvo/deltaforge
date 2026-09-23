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

use deltaforge_core::{CheckpointComparator, CheckpointOrder};

use super::batch_upload::{DurableError, TableObject, upload_batch};
use super::compaction::{
    CompactionError, CompactionRecord, check_compatible, compacted_object_key,
    load_record, verify_originals_present, write_record,
};
use super::keys::content_hash;
use super::manifest::{
    ManifestEntry, ManifestObject, PrevRef, WrittenEntry, propose_seq,
    write_entry,
};
use super::store_cond::{ConditionalStore, PutOutcome};

/// Canonical HEAD version. Bump if the HEAD byte layout changes.
pub const HEAD_VERSION: u16 = 1;

/// Bounded reconcile attempts for a single publish before giving up (retryable).
const MAX_PUBLISH_ATTEMPTS: usize = 8;

/// Bounded acquisition attempts (each is a verify + CAS; a CAS conflict restarts).
const MAX_ACQUIRE_ATTEMPTS: usize = 8;

/// Hard cap on manifest-chain length walked during recovery (cycle / runaway
/// guard). Chains longer than this fail closed rather than walk forever.
const MAX_CHAIN_WALK: usize = 5_000_000;

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
    /// Reference to the latest compaction record (the head of the immutable
    /// compaction history). Independent of the acknowledgement chain: compaction
    /// only ever sets these two fields via a HEAD CAS that preserves epoch, seq,
    /// entry reference, watermark and rollup references. `None` = no compactions.
    #[serde(default)]
    pub compaction_key: Option<String>,
    #[serde(default)]
    pub compaction_hash: Option<String>,
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
            compaction_key: None,
            compaction_hash: None,
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
        if self.compaction_key.is_some() != self.compaction_hash.is_some() {
            return Err(HeadError::Integrity(
                "HEAD compaction key/hash not both-set-or-both-unset".into(),
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

/// Test-only accessor for the HEAD key, so crash-boundary tests can read the
/// authoritative HEAD directly from the store (with no live writer).
#[cfg(test)]
pub(crate) fn head_key_for(
    prefix: &str,
    pipeline: &str,
) -> object_store::path::Path {
    head_key(prefix, pipeline)
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

async fn load_entry<S: ConditionalStore + ?Sized>(
    store: &S,
    key: &str,
) -> Result<ManifestEntry, HeadError> {
    match store
        .get_with_etag(&object_store::path::Path::from(key))
        .await
        .map_err(|e| HeadError::Store(e.to_string()))?
    {
        Some((raw, _)) => serde_json::from_slice(&raw).map_err(|e| {
            HeadError::Integrity(format!("entry {key} unparseable: {e}"))
        }),
        None => Err(HeadError::Integrity(format!("missing entry {key}"))),
    }
}

/// Verify HEAD's authoritative chain and all referenced data objects. Walks from
/// the head entry back to genesis (rollup-aware walking lands with the rollup
/// commit; a HEAD that references a rollup fails closed here rather than trusting
/// an unvalidated one). Every failure is fatal - no `Before`/`Equal` skipping is
/// enabled unless this returns `Ok`.
async fn verify_chain<S: ConditionalStore + ?Sized>(
    store: &S,
    _prefix: &str,
    pipeline: &str,
    source_id: &str,
    sink_id: &str,
    head: &Head,
    comparator: &dyn CheckpointComparator,
) -> Result<(), HeadError> {
    // Understand the authoritative compaction mapping before trusting HEAD (and
    // before any later GC deletes originals). This never depends on, or alters,
    // the acknowledgement chain below.
    verify_compaction_chain(store, pipeline, source_id, sink_id, head).await?;

    // Genesis HEAD (no entry) has nothing to walk; structure was checked by
    // Head::parse.
    let Some(head_entry_key) = head.head_entry_key.clone() else {
        return Ok(());
    };
    let head_entry_hash = head
        .head_entry_hash
        .clone()
        .ok_or_else(|| HeadError::Integrity("HEAD hash missing".into()))?;

    // Rollups are not produced yet; a HEAD claiming one is unexpected and cannot
    // be validated, so fail closed rather than trust it.
    if head.rollup_key.is_some() {
        return Err(HeadError::Integrity(
            "HEAD references a rollup, but rollup verification is not yet \
             supported"
                .into(),
        ));
    }

    let mut cur_key = head_entry_key;
    let mut expected_hash = head_entry_hash;
    let mut expected_seq: Option<u64> = None;
    let mut later_epoch: Option<u64> = None;
    let mut later_wm: Option<Vec<u8>> = None;
    let mut is_head_entry = true;

    for _ in 0..MAX_CHAIN_WALK {
        let entry = load_entry(store, &cur_key).await?;

        // 1. Bytes match the referenced hash (and thus the key hash).
        if entry.entry_hash() != expected_hash {
            return Err(HeadError::Integrity(format!(
                "entry {cur_key} hash mismatch"
            )));
        }
        // 2. Identity.
        if entry.pipeline != pipeline
            || entry.source_id != source_id
            || entry.sink_id != sink_id
        {
            return Err(HeadError::Integrity(format!(
                "entry {cur_key} identity mismatch"
            )));
        }
        // 3. Sequence continuity.
        if let Some(es) = expected_seq {
            if entry.seq != es {
                return Err(HeadError::Integrity(format!(
                    "sequence discontinuity at {cur_key}: expected {es}, got {}",
                    entry.seq
                )));
            }
        }
        // 4. Epoch never regresses toward HEAD (earlier entry <= later entry;
        //    jumps are allowed).
        if let Some(le) = later_epoch {
            if entry.epoch > le {
                return Err(HeadError::Integrity(format!(
                    "epoch regression at {cur_key}: {} > later {le}",
                    entry.epoch
                )));
            }
        }
        // 5. Watermark strictly advances toward HEAD (later After earlier).
        let cur_wm = unhex(&entry.watermark_hex).ok_or_else(|| {
            HeadError::Integrity(format!("entry {cur_key} watermark not hex"))
        })?;
        if let Some(lw) = &later_wm {
            match comparator.order(lw, &cur_wm) {
                CheckpointOrder::After => {}
                other => {
                    return Err(HeadError::Integrity(format!(
                        "non-monotonic watermark at {cur_key}: later is {other:?} \
                         relative to it"
                    )));
                }
            }
        }
        // HEAD watermark exactly matches its selected (head) entry.
        if is_head_entry
            && head.watermark_hex.as_deref()
                != Some(entry.watermark_hex.as_str())
        {
            return Err(HeadError::Integrity(
                "HEAD watermark does not match its selected entry".into(),
            ));
        }
        // 6. Every referenced data object exists and matches.
        for mobj in &entry.objects {
            verify_data_object(store, mobj).await?;
        }

        // Advance to the previous entry, or stop at genesis.
        match &entry.prev {
            None => {
                if entry.seq != 1 {
                    return Err(HeadError::Integrity(format!(
                        "genesis entry {cur_key} has seq {} (expected 1)",
                        entry.seq
                    )));
                }
                return Ok(());
            }
            Some(PrevRef { key, hash }) => {
                let next_seq = entry.seq.checked_sub(1).ok_or_else(|| {
                    HeadError::Integrity("sequence underflow".into())
                })?;
                if next_seq == 0 {
                    return Err(HeadError::Integrity(format!(
                        "entry {cur_key} has a prev link but seq would reach 0"
                    )));
                }
                later_epoch = Some(entry.epoch);
                later_wm = Some(cur_wm);
                expected_seq = Some(next_seq);
                expected_hash = hash.clone();
                cur_key = key.clone();
                is_head_entry = false;
            }
        }
    }
    Err(HeadError::Integrity(
        "manifest chain exceeded the maximum walk length (cycle or runaway)"
            .into(),
    ))
}

/// Verify the compaction history reachable from HEAD's compaction reference:
/// each record parses and matches its referenced hash, has matching identity, is
/// domain-consistent (every original shares the replacement's table + encoding
/// domain), and its replacement object is present with the recorded content hash
/// and size. The `prev` chain is walked bounded and cycle-checked (immutable,
/// non-cyclic history). This is independent of the acknowledgement chain and
/// never inspects the source watermark. (Cross-referencing originals against the
/// ack chain and row-exactness are the pre-deletion gate that lands with GC;
/// this commit deletes nothing.)
async fn verify_compaction_chain<S: ConditionalStore + ?Sized>(
    store: &S,
    pipeline: &str,
    source_id: &str,
    sink_id: &str,
    head: &Head,
) -> Result<(), HeadError> {
    let (Some(mut cur_key), Some(mut expected_hash)) =
        (head.compaction_key.clone(), head.compaction_hash.clone())
    else {
        return Ok(());
    };
    let mut seen = std::collections::HashSet::new();
    let mut claimed_originals = std::collections::HashSet::new();
    for _ in 0..MAX_CHAIN_WALK {
        if !seen.insert(cur_key.clone()) {
            return Err(HeadError::Integrity(
                "compaction history contains a cycle".into(),
            ));
        }
        let rec = load_record(store, &cur_key, &expected_hash)
            .await
            .map_err(map_compaction)?;
        if rec.pipeline != pipeline
            || rec.source_id != source_id
            || rec.sink_id != sink_id
        {
            return Err(HeadError::Integrity(format!(
                "compaction record {cur_key} identity mismatch"
            )));
        }
        // Domain consistency: never a cross-table / cross-domain compaction.
        check_compatible(&rec.table, &rec.domain(), &rec.originals)
            .map_err(map_compaction)?;
        // No original may appear in two records (a recompaction of an
        // already-superseded object is a conflicting active compaction).
        for o in &rec.originals {
            if !claimed_originals.insert(o.key.clone()) {
                return Err(HeadError::Integrity(format!(
                    "original {} appears in conflicting compactions",
                    o.key
                )));
            }
        }
        // The replacement must be durably present with its recorded hash + size.
        verify_data_object(store, &rec.replacement).await?;
        match &rec.prev {
            None => return Ok(()),
            Some(PrevRef { key, hash }) => {
                expected_hash = hash.clone();
                cur_key = key.clone();
            }
        }
    }
    Err(HeadError::Integrity(
        "compaction history exceeded the maximum walk length".into(),
    ))
}

/// Verify only HEAD's directly-referenced entry (exists, hash matches, and its
/// watermark matches HEAD). Cheap check used when a same-epoch reconcile adopts
/// a HEAD mid-publish; the full chain was already verified at acquisition.
async fn verify_head_entry<S: ConditionalStore + ?Sized>(
    store: &S,
    head: &Head,
) -> Result<(), HeadError> {
    let (Some(key), Some(hash)) = (&head.head_entry_key, &head.head_entry_hash)
    else {
        return Ok(()); // genesis: nothing referenced
    };
    let entry = load_entry(store, key).await?;
    if entry.entry_hash() != *hash {
        return Err(HeadError::Integrity(format!(
            "adopted HEAD entry {key} hash mismatch"
        )));
    }
    if head.watermark_hex.as_deref() != Some(entry.watermark_hex.as_str()) {
        return Err(HeadError::Integrity(
            "adopted HEAD watermark does not match its entry".into(),
        ));
    }
    Ok(())
}

async fn verify_data_object<S: ConditionalStore + ?Sized>(
    store: &S,
    mobj: &ManifestObject,
) -> Result<(), HeadError> {
    match store
        .get_with_etag(&object_store::path::Path::from(mobj.key.as_str()))
        .await
        .map_err(|e| HeadError::Store(e.to_string()))?
    {
        Some((bytes, _)) => {
            if bytes.len() as u64 != mobj.byte_len {
                return Err(HeadError::Integrity(format!(
                    "data object {} size {} != manifest {}",
                    mobj.key,
                    bytes.len(),
                    mobj.byte_len
                )));
            }
            let domain = mobj.encoding_domain();
            if content_hash(&bytes, &domain) != mobj.content_hash {
                return Err(HeadError::Integrity(format!(
                    "data object {} content hash mismatch",
                    mobj.key
                )));
            }
            Ok(())
        }
        None => Err(HeadError::Integrity(format!(
            "referenced data object {} is missing",
            mobj.key
        ))),
    }
}

/// Proof that a HEAD (and the authoritative chain it selects) was fully verified
/// and then acquired under a fenced epoch. It is only constructed by a successful
/// [`DurableWriter::acquire`] (recovery) or extended by a successful publication
/// (verified by construction). A `Before`/`Equal` acknowledgement - which lets
/// the sink skip a write - is only ever made against a `VerifiedHead`, so it can
/// never happen before validation.
struct VerifiedHead {
    epoch: u64,
    head: Head,
    /// ETag of the verified HEAD, for the next `If-Match` CAS.
    etag: String,
}

/// Per-instance mutable state, guarded so concurrent `send_batch` calls on one
/// sink instance serialize their HEAD publication and cannot race.
struct WriterState {
    verified: VerifiedHead,
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
    /// Source-aware checkpoint ordering. Enforces that HEAD only ever advances,
    /// even when this (valid, current-epoch) writer receives replayed older
    /// batches after a crash between the HEAD CAS and the coordinator checkpoint.
    comparator: Arc<dyn CheckpointComparator>,
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
        comparator: Arc<dyn CheckpointComparator>,
    ) -> Result<Self, HeadError> {
        let hkey = head_key(prefix, pipeline);

        for _ in 0..MAX_ACQUIRE_ATTEMPTS {
            match store
                .get_with_etag(&hkey)
                .await
                .map_err(|e| HeadError::Store(e.to_string()))?
            {
                Some((raw, Some(etag))) => {
                    let head = Head::parse(&raw)?;
                    // Verify HEAD + the authoritative chain + referenced data
                    // objects BEFORE acquiring, using the same ETag we will CAS
                    // against (steps 2-3).
                    verify_chain(
                        store.as_ref(),
                        prefix,
                        pipeline,
                        source_id,
                        sink_id,
                        &head,
                        comparator.as_ref(),
                    )
                    .await?;

                    let new_epoch =
                        head.epoch.checked_add(1).ok_or_else(|| {
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
                        } => {
                            return Ok(Self::build(
                                store,
                                prefix,
                                pipeline,
                                source_id,
                                sink_id,
                                comparator,
                                VerifiedHead {
                                    epoch: new_epoch,
                                    head: next,
                                    etag: new_etag,
                                },
                            ));
                        }
                        PutOutcome::Written { etag: None } => {
                            return Err(HeadError::Store(
                                "HEAD CAS returned no ETag".into(),
                            ));
                        }
                        // Someone advanced HEAD between verify and acquire:
                        // discard the verification and restart (step 5).
                        PutOutcome::Conflict => continue,
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
                        PutOutcome::Written { etag: Some(etag) } => {
                            return Ok(Self::build(
                                store,
                                prefix,
                                pipeline,
                                source_id,
                                sink_id,
                                comparator,
                                VerifiedHead {
                                    epoch: 1,
                                    head: genesis,
                                    etag,
                                },
                            ));
                        }
                        PutOutcome::Written { etag: None } => {
                            return Err(HeadError::Store(
                                "genesis HEAD create returned no ETag".into(),
                            ));
                        }
                        // Concurrent create: restart and take the CAS path.
                        PutOutcome::AlreadyExists => continue,
                        PutOutcome::Conflict => unreachable!("put_if_absent"),
                    }
                }
            }
        }
        Err(HeadError::RetriesExhausted(MAX_ACQUIRE_ATTEMPTS))
    }

    #[allow(clippy::too_many_arguments)]
    fn build(
        store: Arc<S>,
        prefix: &str,
        pipeline: &str,
        source_id: &str,
        sink_id: &str,
        comparator: Arc<dyn CheckpointComparator>,
        verified: VerifiedHead,
    ) -> Self {
        Self {
            store,
            prefix: prefix.to_string(),
            pipeline: pipeline.to_string(),
            source_id: source_id.to_string(),
            sink_id: sink_id.to_string(),
            comparator,
            state: Mutex::new(WriterState {
                verified,
                fenced: None,
            }),
        }
    }

    /// Current epoch (test/observability).
    pub async fn epoch(&self) -> u64 {
        self.state.lock().await.verified.epoch
    }

    /// Order a proposed watermark against the current HEAD watermark.
    /// `Ok(None)` means HEAD has no watermark yet (genesis) so any proposal is
    /// the first and proceeds.
    fn order_vs_head(
        &self,
        head: &Head,
        proposed: &[u8],
    ) -> Result<Option<CheckpointOrder>, HeadError> {
        match &head.watermark_hex {
            None => Ok(None),
            Some(h) => {
                let head_wm = unhex(h).ok_or_else(|| {
                    HeadError::Integrity(
                        "HEAD watermark is not valid hex".into(),
                    )
                })?;
                Ok(Some(self.comparator.order(proposed, &head_wm)))
            }
        }
    }

    /// Verify a HEAD adopted during same-epoch reconciliation. A single new entry
    /// is trusted only when it is exactly one step past our current verified head
    /// (its `prev` references our head entry and its seq is ours + 1); its entry
    /// hash, referenced data objects and strict watermark advance are then
    /// checked. If it jumped farther, the entire intervening tail is re-verified.
    async fn verify_adopted_head(
        &self,
        our: &Head,
        cur: &Head,
    ) -> Result<(), HeadError> {
        let cur_entry_key = cur.head_entry_key.as_ref().ok_or_else(|| {
            HeadError::Integrity("adopted HEAD has no entry".into())
        })?;
        let cur_entry = load_entry(self.store.as_ref(), cur_entry_key).await?;

        let one_step = cur.seq == our.seq.saturating_add(1)
            && matches!(
                (&cur_entry.prev, &our.head_entry_key, &our.head_entry_hash),
                (Some(p), Some(ok), Some(oh)) if p.key == *ok && p.hash == *oh
            );

        if one_step {
            // Light verification of the single new entry.
            verify_head_entry(self.store.as_ref(), cur).await?;
            for mobj in &cur_entry.objects {
                verify_data_object(self.store.as_ref(), mobj).await?;
            }
            // Its watermark must strictly follow ours.
            if let Some(ourw) = &our.watermark_hex {
                let ow = unhex(ourw).ok_or_else(|| {
                    HeadError::Integrity("our watermark not hex".into())
                })?;
                let cw = unhex(&cur_entry.watermark_hex).ok_or_else(|| {
                    HeadError::Integrity("adopted watermark not hex".into())
                })?;
                if self.comparator.order(&cw, &ow) != CheckpointOrder::After {
                    return Err(HeadError::Integrity(
                        "adopted HEAD watermark does not advance".into(),
                    ));
                }
            }
            Ok(())
        } else {
            // Jumped farther: re-verify the whole chain the adopted HEAD selects.
            verify_chain(
                self.store.as_ref(),
                &self.prefix,
                &self.pipeline,
                &self.source_id,
                &self.sink_id,
                cur,
                self.comparator.as_ref(),
            )
            .await
        }
    }

    /// Publish one batch and acknowledge only on success.
    ///
    /// Source-aware ordering against the current HEAD watermark decides the path:
    /// - **After**: publish normally (HEAD advances).
    /// - **Equal / Before**: an already-durable (replayed) batch - acknowledge
    ///   without uploading a new entry or moving HEAD. Safe because the manifest
    ///   chain asserts every position through HEAD's watermark is durable.
    /// - **Incomparable**: different lineage/generation - fail closed.
    ///
    /// So HEAD never transitions Before or Incomparable. `Ok(())` is returned
    /// only after a HEAD CAS referencing this batch's entry succeeds, after
    /// reconciliation proves HEAD already references our exact entry, or for an
    /// already-durable replay (Equal/Before).
    pub async fn publish(
        &self,
        watermark: &[u8],
        objects: Vec<TableObject>,
        event_count: u64,
    ) -> Result<(), HeadError> {
        let mut st = self.state.lock().await;
        if let Some(observed) = st.fenced {
            return Err(HeadError::Fenced {
                our: st.verified.epoch,
                observed,
            });
        }

        // Ordering gate: only an After (or genesis) proposal does any work.
        match self.order_vs_head(&st.verified.head, watermark)? {
            None | Some(CheckpointOrder::After) => {}
            // Already durable at or beyond this position (a valid replay under a
            // possibly-newer epoch): acknowledge without moving HEAD.
            Some(CheckpointOrder::Equal) | Some(CheckpointOrder::Before) => {
                return Ok(());
            }
            Some(CheckpointOrder::Incomparable) => {
                return Err(HeadError::Integrity(format!(
                    "proposed checkpoint is incomparable to HEAD watermark for \
                     pipeline {} (lineage/generation mismatch)",
                    self.pipeline
                )));
            }
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
            // HEAD may have advanced (same-epoch race, or catch-up reaching an
            // already-published position). Re-order before proposing again.
            match self.order_vs_head(&st.verified.head, watermark)? {
                None | Some(CheckpointOrder::After) => {}
                Some(CheckpointOrder::Equal)
                | Some(CheckpointOrder::Before) => {
                    // The batch is already covered by the current HEAD; the
                    // objects uploaded above are harmless reconciliation
                    // candidates. Acknowledge without moving HEAD.
                    return Ok(());
                }
                Some(CheckpointOrder::Incomparable) => {
                    return Err(HeadError::Integrity(format!(
                        "proposed checkpoint became incomparable to HEAD for \
                         pipeline {}",
                        self.pipeline
                    )));
                }
            }
            let seq = propose_seq(st.verified.head.seq);
            let prev = match (
                &st.verified.head.head_entry_key,
                &st.verified.head.head_entry_hash,
            ) {
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
                epoch: st.verified.epoch,
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
                epoch: st.verified.epoch,
                seq,
                head_entry_key: Some(written.key.clone()),
                head_entry_hash: Some(written.hash.clone()),
                // Rollup references are preserved unchanged during ordinary
                // batch publication.
                rollup_key: st.verified.head.rollup_key.clone(),
                rollup_hash: st.verified.head.rollup_hash.clone(),
                watermark_hex: Some(watermark_hex.clone()),
                // Ordinary publication preserves the current compaction reference.
                compaction_key: st.verified.head.compaction_key.clone(),
                compaction_hash: st.verified.head.compaction_hash.clone(),
            };

            let cas = self
                .store
                .cas_put(&hkey, next_head.canonical_bytes(), &st.verified.etag)
                .await;

            match cas {
                Ok(PutOutcome::Written {
                    etag: Some(new_etag),
                }) => {
                    st.verified.head = next_head;
                    st.verified.etag = new_etag;
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
                        st.verified.head = cur;
                        st.verified.etag = etag;
                        return Ok(()); // ACKNOWLEDGE (idempotent)
                    }
                    // A higher epoch published: fenced, permanently.
                    if cur.epoch > st.verified.epoch {
                        st.fenced = Some(cur.epoch);
                        return Err(HeadError::Fenced {
                            our: st.verified.epoch,
                            observed: cur.epoch,
                        });
                    }
                    // A lower epoch is impossible (epochs are monotonic).
                    if cur.epoch < st.verified.epoch {
                        return Err(HeadError::Integrity(format!(
                            "HEAD epoch regressed: {} < our {}",
                            cur.epoch, st.verified.epoch
                        )));
                    }
                    // HEAD did not move (same ETag): our CAS failed transiently
                    // (a rejected or lost-then-not-applied response), not a race.
                    // Retry the CAS against the same HEAD - there is nothing new
                    // to adopt or verify (this also covers the genesis case where
                    // `cur` has no entry to verify).
                    if etag == st.verified.etag {
                        continue;
                    }
                    // Same epoch, different head: verify the adopted HEAD before
                    // trusting its watermark for a later Before/Equal skip, then
                    // retry with a fresh seq + prev. (Our already-written entry
                    // becomes an unreferenced reconciliation candidate.)
                    let our = st.verified.head.clone();
                    self.verify_adopted_head(&our, &cur).await?;
                    st.verified.head = cur;
                    st.verified.etag = etag;
                    continue;
                }
            }
        }
        Err(HeadError::RetriesExhausted(MAX_PUBLISH_ATTEMPTS))
    }

    /// Publish a compaction: replace `originals` (already-durable objects for one
    /// table) with the immutable `replacement`, WITHOUT touching the source
    /// watermark, seq, or the acknowledgement chain. Steps: compatibility gate ->
    /// read/verify originals -> upload the replacement (immutable, create-only) ->
    /// write the compaction record -> CAS HEAD to set ONLY the compaction
    /// reference, preserving epoch/seq/entry/watermark/rollup. Serialized through
    /// the same writer mutex as batch publication. Originals are NOT deleted (the
    /// record marks them eligible for later GC).
    pub async fn compact(
        &self,
        replacement: TableObject,
        originals: Vec<ManifestObject>,
    ) -> Result<(), HeadError> {
        let domain = replacement.domain.clone();
        let table = replacement.table.clone();

        // 1. Never compact across tables or incompatible encoding domains.
        check_compatible(&table, &domain, &originals)
            .map_err(map_compaction)?;

        // 2. Read the originals: each must be present with its recorded hash/size.
        verify_originals_present(self.store.as_ref(), &originals)
            .await
            .map_err(map_compaction)?;

        // 3. Upload the immutable replacement, content-addressed (idempotent).
        let ch = content_hash(&replacement.bytes, &domain);
        let key = compacted_object_key(
            &self.prefix,
            &self.pipeline,
            &table,
            &ch,
            replacement.ext,
        );
        let byte_len = replacement.bytes.len() as u64;
        match self
            .store
            .put_if_absent(&key, replacement.bytes.clone())
            .await
            .map_err(|e| HeadError::Store(e.to_string()))?
        {
            PutOutcome::Written { .. } => {}
            PutOutcome::AlreadyExists => {
                // Idempotent: the existing object must be byte-identical.
                match self
                    .store
                    .get_with_etag(&key)
                    .await
                    .map_err(|e| HeadError::Store(e.to_string()))?
                {
                    Some((existing, _))
                        if content_hash(&existing, &domain) == ch => {}
                    _ => {
                        return Err(HeadError::Integrity(format!(
                            "compacted object at {key} does not match its hash"
                        )));
                    }
                }
            }
            PutOutcome::Conflict => {
                return Err(HeadError::Store(format!(
                    "unexpected CAS conflict on create-only object at {key}"
                )));
            }
        }
        let replacement_obj = ManifestObject {
            key: key.to_string(),
            table: table.clone(),
            content_hash: ch,
            byte_len,
            format: domain.format.clone(),
            format_version: domain.format_version,
            schema_id: domain.schema_id.clone(),
        };

        // Everything from here mutates HEAD, so take the same lock batch
        // publication uses (compaction HEAD updates serialize with it).
        let mut st = self.state.lock().await;
        if let Some(observed) = st.fenced {
            return Err(HeadError::Fenced {
                our: st.verified.epoch,
                observed,
            });
        }

        // 4. Write the immutable compaction record, chained to the current one.
        let prev = match (
            &st.verified.head.compaction_key,
            &st.verified.head.compaction_hash,
        ) {
            (Some(k), Some(h)) => Some(PrevRef {
                key: k.clone(),
                hash: h.clone(),
            }),
            _ => None,
        };
        let record = CompactionRecord {
            version: super::compaction::COMPACTION_RECORD_VERSION,
            pipeline: self.pipeline.clone(),
            source_id: self.source_id.clone(),
            sink_id: self.sink_id.clone(),
            table,
            replacement: replacement_obj,
            originals,
            prev,
        };
        let written = write_record(self.store.as_ref(), &self.prefix, &record)
            .await
            .map_err(map_compaction)?;

        // 5. CAS HEAD to set ONLY the compaction reference; everything else is
        // preserved. On conflict/lost response, reconcile.
        let hkey = head_key(&self.prefix, &self.pipeline);
        for _ in 0..MAX_PUBLISH_ATTEMPTS {
            let next = Head {
                compaction_key: Some(written.key.clone()),
                compaction_hash: Some(written.hash.clone()),
                ..st.verified.head.clone()
            };
            let cas = self
                .store
                .cas_put(&hkey, next.canonical_bytes(), &st.verified.etag)
                .await;
            match cas {
                Ok(PutOutcome::Written {
                    etag: Some(new_etag),
                }) => {
                    st.verified.head = next;
                    st.verified.etag = new_etag;
                    return Ok(());
                }
                Ok(PutOutcome::Written { etag: None }) => {
                    return Err(HeadError::Store(
                        "HEAD CAS returned no ETag".into(),
                    ));
                }
                Ok(PutOutcome::AlreadyExists) => unreachable!("cas_put"),
                Ok(PutOutcome::Conflict) | Err(_) => {
                    let (raw, etag) = match self
                        .store
                        .get_with_etag(&hkey)
                        .await
                        .map_err(|e| HeadError::Store(e.to_string()))?
                    {
                        Some((raw, Some(etag))) => (raw, etag),
                        _ => {
                            return Err(HeadError::Integrity(
                                "HEAD missing/eTagless during compaction"
                                    .into(),
                            ));
                        }
                    };
                    let cur = Head::parse(&raw)?;
                    // Lost response that succeeded: HEAD already references our
                    // record -> idempotent success.
                    if cur.compaction_hash.as_deref()
                        == Some(written.hash.as_str())
                    {
                        st.verified.head = cur;
                        st.verified.etag = etag;
                        return Ok(());
                    }
                    // A higher epoch fenced us: compaction is abandoned; the
                    // uploaded object + record are harmless orphans.
                    if cur.epoch > st.verified.epoch {
                        st.fenced = Some(cur.epoch);
                        return Err(HeadError::Fenced {
                            our: st.verified.epoch,
                            observed: cur.epoch,
                        });
                    }
                    if cur.epoch < st.verified.epoch {
                        return Err(HeadError::Integrity(format!(
                            "HEAD epoch regressed during compaction: {} < {}",
                            cur.epoch, st.verified.epoch
                        )));
                    }
                    // Same epoch (a transient error, or our own earlier batch
                    // advanced HEAD): adopt the current HEAD and re-apply the
                    // compaction reference on top, preserving its ack state.
                    st.verified.head = cur;
                    st.verified.etag = etag;
                    continue;
                }
            }
        }
        Err(HeadError::RetriesExhausted(MAX_PUBLISH_ATTEMPTS))
    }
}

fn map_compaction(e: CompactionError) -> HeadError {
    match e {
        CompactionError::Integrity(_) | CompactionError::Incompatible(_) => {
            HeadError::Integrity(e.to_string())
        }
        CompactionError::Store(s) => HeadError::Store(s),
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

    /// Test comparator modelling structured source semantics: a watermark is
    /// `[lineage_byte, 8-byte BE position]`. Different lineage -> Incomparable
    /// (like a MySQL failover / snapshot-generation change); otherwise the
    /// position orders. Unparseable input is Incomparable (fail closed).
    struct MonoCmp;
    impl CheckpointComparator for MonoCmp {
        fn order(&self, a: &[u8], b: &[u8]) -> CheckpointOrder {
            fn parse(x: &[u8]) -> Option<(u8, u64)> {
                if x.len() != 9 {
                    return None;
                }
                let mut p = [0u8; 8];
                p.copy_from_slice(&x[1..9]);
                Some((x[0], u64::from_be_bytes(p)))
            }
            match (parse(a), parse(b)) {
                (Some((la, xa)), Some((lb, xb))) => {
                    if la != lb {
                        CheckpointOrder::Incomparable
                    } else {
                        use std::cmp::Ordering::*;
                        match xa.cmp(&xb) {
                            Less => CheckpointOrder::Before,
                            Equal => CheckpointOrder::Equal,
                            Greater => CheckpointOrder::After,
                        }
                    }
                }
                _ => CheckpointOrder::Incomparable,
            }
        }
    }

    fn cmp() -> Arc<dyn CheckpointComparator> {
        Arc::new(MonoCmp)
    }

    /// Build a structured watermark: lineage + position.
    fn wm(lineage: u8, pos: u64) -> Vec<u8> {
        let mut v = vec![lineage];
        v.extend_from_slice(&pos.to_be_bytes());
        v
    }

    fn wm_hex(lineage: u8, pos: u64) -> String {
        super::hex(&wm(lineage, pos))
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
        DurableWriter::acquire(store, "pfx", "pipe", "src", "sink", cmp())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn genesis_acquire_and_first_publish_acknowledges() {
        let s = cond();
        let w = writer(Arc::clone(&s)).await;
        assert_eq!(w.epoch().await, 1);
        w.publish(&wm(0, 1), vec![tobj("orders", b"a")], 1)
            .await
            .unwrap();
        let st = w.state.lock().await;
        assert_eq!(st.verified.head.seq, 1);
        assert!(st.verified.head.head_entry_key.is_some());
        assert_eq!(
            st.verified.head.watermark_hex.as_deref(),
            Some(wm_hex(0, 1).as_str())
        );
    }

    #[tokio::test]
    async fn second_publish_advances_seq_and_chains() {
        let s = cond();
        let w = writer(Arc::clone(&s)).await;
        w.publish(&wm(0, 1), vec![tobj("orders", b"a")], 1)
            .await
            .unwrap();
        w.publish(&wm(0, 2), vec![tobj("orders", b"b")], 1)
            .await
            .unwrap();
        let st = w.state.lock().await;
        assert_eq!(st.verified.head.seq, 2);
        // rollup refs untouched by ordinary publication.
        assert!(
            st.verified.head.rollup_key.is_none()
                && st.verified.head.rollup_hash.is_none()
        );
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
            .publish(&wm(0, 10), vec![tobj("orders", b"x")], 1)
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
            .publish(&wm(0, 11), vec![tobj("orders", b"y")], 1)
            .await
            .unwrap_err();
        assert!(matches!(e2, HeadError::Fenced { .. }));

        // The newer epoch publishes fine.
        b.publish(&wm(0, 2), vec![tobj("orders", b"z")], 1)
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
                cmp(),
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
                cmp(),
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
            compaction_key: None,
            compaction_hash: None,
        };
        s.put_if_absent(&hkey, bad.canonical_bytes()).await.unwrap();
        let err = expect_err(
            DurableWriter::acquire(
                Arc::clone(&s),
                "pfx",
                "pipe",
                "src",
                "sink",
                cmp(),
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
        w.publish(&wm(0, 1), vec![tobj("orders", b"a")], 1)
            .await
            .unwrap();

        // Out-of-band: read HEAD, advance seq to 2 at epoch 1 referencing a
        // REAL entry (so the adopted HEAD passes verify_head_entry), using the
        // current ETag.
        let hkey = head_key("pfx", "pipe");
        let (raw, etag) = s.get_with_etag(&hkey).await.unwrap().unwrap();
        let mut cur = Head::parse(&raw).unwrap();
        let entry2 = super::super::manifest::ManifestEntry {
            version: super::super::manifest::MANIFEST_ENTRY_VERSION,
            pipeline: "pipe".into(),
            source_id: "src".into(),
            sink_id: "sink".into(),
            epoch: 1,
            seq: 2,
            watermark_hex: wm_hex(0, 2),
            event_count: 1,
            objects: vec![],
            prev: Some(super::super::manifest::PrevRef {
                key: cur.head_entry_key.clone().unwrap(),
                hash: cur.head_entry_hash.clone().unwrap(),
            }),
        };
        let w2 =
            super::super::manifest::write_entry(s.as_ref(), "pfx", &entry2)
                .await
                .unwrap();
        cur.seq = 2;
        cur.head_entry_key = Some(w2.key);
        cur.head_entry_hash = Some(w2.hash);
        cur.watermark_hex = Some(wm_hex(0, 2));
        s.cas_put(&hkey, cur.canonical_bytes(), etag.as_deref().unwrap())
            .await
            .unwrap();

        // Writer's cached ETag is now stale; publish reconciles (same epoch,
        // different head) and commits at seq 3.
        w.publish(&wm(0, 3), vec![tobj("orders", b"c")], 1)
            .await
            .unwrap();
        let st = w.state.lock().await;
        assert_eq!(st.verified.epoch, 1);
        assert_eq!(st.verified.head.seq, 3);
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
        let w =
            DurableWriter::acquire(lossy, "pfx", "pipe", "src", "sink", cmp())
                .await
                .unwrap();
        // The publish's HEAD CAS applies but the response is lost; publish must
        // reread, see HEAD referencing its entry, and acknowledge.
        w.publish(&wm(0, 1), vec![tobj("orders", b"a")], 1)
            .await
            .expect("lost response but write succeeded -> ack");
        let st = w.state.lock().await;
        assert_eq!(st.verified.head.seq, 1);
    }

    async fn entry_count(s: &ObjectStoreConditional) -> usize {
        s.list(&Path::from("pfx/pipe/_manifest/entries"))
            .await
            .unwrap()
            .len()
    }

    /// Crash after the HEAD CAS but before the coordinator checkpoint: a new
    /// epoch is acquired and the source resumes from an OLDER checkpoint. The
    /// replayed older batch must be acknowledged without moving HEAD backward.
    #[tokio::test]
    async fn replay_older_batch_under_newer_epoch_does_not_move_head() {
        let s = cond();
        let w1 = writer(Arc::clone(&s)).await; // epoch 1
        w1.publish(&wm(0, 5), vec![tobj("orders", b"a")], 1)
            .await
            .unwrap(); // HEAD at pos 5, seq 1
        let entries_before = entry_count(&s).await;

        // "Crash": drop w1, acquire a newer epoch. HEAD still at pos 5.
        drop(w1);
        let w2 = DurableWriter::acquire(
            Arc::clone(&s),
            "pfx",
            "pipe",
            "src",
            "sink",
            cmp(),
        )
        .await
        .unwrap();
        assert_eq!(w2.epoch().await, 2);

        // Source resumes from an older checkpoint (pos 3) -> Before -> ack, no
        // new entry, HEAD unchanged (no backward transition).
        w2.publish(&wm(0, 3), vec![tobj("orders", b"replay")], 1)
            .await
            .unwrap();
        let st = w2.state.lock().await;
        assert_eq!(st.verified.head.seq, 1, "HEAD must not advance on replay");
        assert_eq!(
            st.verified.head.watermark_hex.as_deref(),
            Some(wm_hex(0, 5).as_str()),
            "HEAD watermark must not move backward"
        );
        drop(st);
        assert_eq!(
            entry_count(&s).await,
            entries_before,
            "a Before replay must not write a new manifest entry"
        );
    }

    /// An exactly-equal watermark retry is acknowledged without moving HEAD.
    #[tokio::test]
    async fn equal_watermark_retry_acknowledges_without_moving_head() {
        let s = cond();
        let w = writer(Arc::clone(&s)).await;
        w.publish(&wm(0, 4), vec![tobj("orders", b"a")], 1)
            .await
            .unwrap();
        let entries = entry_count(&s).await;
        w.publish(&wm(0, 4), vec![tobj("orders", b"a")], 1)
            .await
            .unwrap(); // Equal -> ack, no move
        let st = w.state.lock().await;
        assert_eq!(st.verified.head.seq, 1);
        drop(st);
        assert_eq!(entry_count(&s).await, entries);
    }

    /// A proposal from a different lineage/generation fails closed and never
    /// moves HEAD.
    #[tokio::test]
    async fn incomparable_lineage_fails_closed() {
        let s = cond();
        let w = writer(Arc::clone(&s)).await;
        w.publish(&wm(0, 2), vec![tobj("orders", b"a")], 1)
            .await
            .unwrap();
        // lineage 1 != lineage 0 -> Incomparable.
        let err = expect_err(
            w.publish(&wm(1, 9), vec![tobj("orders", b"b")], 1).await,
        );
        assert!(matches!(err, HeadError::Integrity(_)));
        assert!(err.is_fatal());
        let st = w.state.lock().await;
        assert_eq!(
            st.verified.head.watermark_hex.as_deref(),
            Some(wm_hex(0, 2).as_str())
        );
    }

    /// After catching up (Before then Equal), a strictly newer watermark
    /// publishes and advances HEAD.
    #[tokio::test]
    async fn catch_up_then_newer_watermark_publishes() {
        let s = cond();
        let w1 = writer(Arc::clone(&s)).await;
        w1.publish(&wm(0, 5), vec![tobj("orders", b"a")], 1)
            .await
            .unwrap();
        drop(w1);
        let w2 = DurableWriter::acquire(
            Arc::clone(&s),
            "pfx",
            "pipe",
            "src",
            "sink",
            cmp(),
        )
        .await
        .unwrap();
        // Replay below head, then reach head, then surpass it.
        w2.publish(&wm(0, 3), vec![tobj("orders", b"x")], 1)
            .await
            .unwrap(); // Before
        w2.publish(&wm(0, 5), vec![tobj("orders", b"y")], 1)
            .await
            .unwrap(); // Equal
        w2.publish(&wm(0, 6), vec![tobj("orders", b"z")], 1)
            .await
            .unwrap(); // After -> advances
        let st = w2.state.lock().await;
        assert_eq!(
            st.verified.head.seq, 2,
            "the newer watermark advanced HEAD"
        );
        assert_eq!(
            st.verified.head.watermark_hex.as_deref(),
            Some(wm_hex(0, 6).as_str())
        );
    }

    // ── Recovery / chain verification ───────────────────────────────────────

    use object_store::ObjectStore;
    use object_store::ObjectStoreExt;

    /// A store whose inner InMemory is also held directly, so tests can tamper
    /// objects behind the conditional layer.
    fn cond_with_inner() -> (Arc<dyn ObjectStore>, Arc<ObjectStoreConditional>)
    {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cs = Arc::new(ObjectStoreConditional::new(Arc::clone(&inner)));
        (inner, cs)
    }

    /// Publish a two-entry chain and return the store.
    async fn build_chain() -> (Arc<dyn ObjectStore>, Arc<ObjectStoreConditional>)
    {
        let (inner, cs) = cond_with_inner();
        let w = DurableWriter::acquire(
            Arc::clone(&cs),
            "pfx",
            "pipe",
            "src",
            "sink",
            cmp(),
        )
        .await
        .unwrap();
        w.publish(&wm(0, 1), vec![tobj("orders", b"a")], 1)
            .await
            .unwrap();
        w.publish(&wm(0, 2), vec![tobj("orders", b"bb")], 1)
            .await
            .unwrap();
        (inner, cs)
    }

    async fn head_of(cs: &ObjectStoreConditional) -> Head {
        let (raw, _) = cs
            .get_with_etag(&head_key("pfx", "pipe"))
            .await
            .unwrap()
            .unwrap();
        Head::parse(&raw).unwrap()
    }

    async fn recover(cs: Arc<ObjectStoreConditional>) -> Result<(), HeadError> {
        DurableWriter::acquire(cs, "pfx", "pipe", "src", "sink", cmp())
            .await
            .map(|_| ())
    }

    #[tokio::test]
    async fn recovery_verifies_valid_chain_and_bumps_epoch() {
        let (_inner, cs) = build_chain().await;
        let w = DurableWriter::acquire(
            Arc::clone(&cs),
            "pfx",
            "pipe",
            "src",
            "sink",
            cmp(),
        )
        .await
        .unwrap();
        // Genesis writer was epoch 1; recovery bumps to 2.
        assert_eq!(w.epoch().await, 2);
        assert_eq!(w.state.lock().await.verified.head.seq, 2);
    }

    #[tokio::test]
    async fn recovery_rejects_tampered_entry() {
        let (inner, cs) = build_chain().await;
        let head = head_of(&cs).await;
        // Overwrite the head entry with different (but valid-JSON) bytes.
        let key = Path::from(head.head_entry_key.unwrap().as_str());
        inner
            .put(
                &key,
                object_store::PutPayload::from(Bytes::from_static(
                    br#"{"version":1,"pipeline":"pipe","source_id":"src","sink_id":"sink","epoch":1,"seq":2,"watermark_hex":"00","event_count":1,"objects":[],"prev":null}"#,
                )),
            )
            .await
            .unwrap();
        let err = expect_err(recover(cs).await);
        assert!(matches!(err, HeadError::Integrity(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn recovery_rejects_corrupted_data_object() {
        let (inner, cs) = build_chain().await;
        let head = head_of(&cs).await;
        let entry =
            load_entry(cs.as_ref(), head.head_entry_key.as_ref().unwrap())
                .await
                .unwrap();
        let dkey = Path::from(entry.objects[0].key.as_str());
        // Same length as "bb" so the size check passes but the hash fails.
        inner
            .put(
                &dkey,
                object_store::PutPayload::from(Bytes::from_static(b"XX")),
            )
            .await
            .unwrap();
        let err = expect_err(recover(cs).await);
        assert!(matches!(err, HeadError::Integrity(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn recovery_rejects_missing_data_object() {
        let (inner, cs) = build_chain().await;
        let head = head_of(&cs).await;
        let entry =
            load_entry(cs.as_ref(), head.head_entry_key.as_ref().unwrap())
                .await
                .unwrap();
        inner
            .delete(&Path::from(entry.objects[0].key.as_str()))
            .await
            .unwrap();
        let err = expect_err(recover(cs).await);
        assert!(matches!(err, HeadError::Integrity(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn recovery_rejects_head_referencing_rollup() {
        let (inner, cs) = build_chain().await;
        let mut head = head_of(&cs).await;
        // Claim a rollup that we cannot validate.
        head.rollup_key = Some("pfx/pipe/_manifest/rollups/x.json".into());
        head.rollup_hash = Some("deadbeef".into());
        inner
            .put(
                &head_key("pfx", "pipe"),
                object_store::PutPayload::from(head.canonical_bytes()),
            )
            .await
            .unwrap();
        let err = expect_err(recover(cs).await);
        assert!(matches!(err, HeadError::Integrity(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn recovery_rejects_head_watermark_not_matching_entry() {
        let (inner, cs) = build_chain().await;
        let mut head = head_of(&cs).await;
        head.watermark_hex = Some(wm_hex(0, 99)); // does not match the entry
        inner
            .put(
                &head_key("pfx", "pipe"),
                object_store::PutPayload::from(head.canonical_bytes()),
            )
            .await
            .unwrap();
        let err = expect_err(recover(cs).await);
        assert!(matches!(err, HeadError::Integrity(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn recovery_rejects_truncated_tail() {
        // Delete the genesis (seq 1) entry so the chain cannot terminate at a
        // valid genesis: the walk hits a missing prev and fails closed.
        let (inner, cs) = build_chain().await;
        let head = head_of(&cs).await;
        let head_entry =
            load_entry(cs.as_ref(), head.head_entry_key.as_ref().unwrap())
                .await
                .unwrap();
        let genesis_key = head_entry.prev.unwrap().key; // seq 1
        inner
            .delete(&Path::from(genesis_key.as_str()))
            .await
            .unwrap();
        let err = expect_err(recover(cs).await);
        assert!(matches!(err, HeadError::Integrity(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn same_epoch_jump_reverifies_tail_and_publishes() {
        // Out-of-band advance HEAD by TWO valid entries at the same epoch, so the
        // adopted HEAD is not one step past ours: the writer must re-verify the
        // whole intervening tail, then publish on top.
        let (_inner, cs) = build_chain().await; // seq 1..=2, epoch 1 (genesis writer)
        let w = DurableWriter::acquire(
            Arc::clone(&cs),
            "pfx",
            "pipe",
            "src",
            "sink",
            cmp(),
        )
        .await
        .unwrap(); // epoch 2
        // Publish one batch so our verified head is at some seq S.
        w.publish(&wm(0, 3), vec![tobj("orders", b"c")], 1)
            .await
            .unwrap();
        let our = head_of(&cs).await;

        // Build two further valid entries out-of-band at the SAME epoch (2),
        // chained onto `our`, then point HEAD at the second one.
        let mk = |seq: u64, wmv: u64, prev: PrevRef| ManifestEntry {
            version: super::super::manifest::MANIFEST_ENTRY_VERSION,
            pipeline: "pipe".into(),
            source_id: "src".into(),
            sink_id: "sink".into(),
            epoch: 2,
            seq,
            watermark_hex: wm_hex(0, wmv),
            event_count: 0,
            objects: vec![],
            prev: Some(prev),
        };
        let e_next = mk(
            our.seq + 1,
            4,
            PrevRef {
                key: our.head_entry_key.clone().unwrap(),
                hash: our.head_entry_hash.clone().unwrap(),
            },
        );
        let w_next = write_entry(cs.as_ref(), "pfx", &e_next).await.unwrap();
        let e_last = mk(
            our.seq + 2,
            5,
            PrevRef {
                key: w_next.key.clone(),
                hash: w_next.hash.clone(),
            },
        );
        let w_last = write_entry(cs.as_ref(), "pfx", &e_last).await.unwrap();

        let hkey = head_key("pfx", "pipe");
        let (_raw, etag) = cs.get_with_etag(&hkey).await.unwrap().unwrap();
        let jumped = Head {
            version: HEAD_VERSION,
            epoch: 2,
            seq: our.seq + 2,
            head_entry_key: Some(w_last.key),
            head_entry_hash: Some(w_last.hash),
            rollup_key: None,
            rollup_hash: None,
            watermark_hex: Some(wm_hex(0, 5)),
            compaction_key: None,
            compaction_hash: None,
        };
        cs.cas_put(&hkey, jumped.canonical_bytes(), etag.as_deref().unwrap())
            .await
            .unwrap();

        // Publishing now reconciles: the adopted HEAD jumped two steps, so the
        // tail is fully re-verified (it is valid), then we publish seq+3.
        w.publish(&wm(0, 6), vec![tobj("orders", b"d")], 1)
            .await
            .unwrap();
        let st = w.state.lock().await;
        assert_eq!(st.verified.head.seq, our.seq + 3);
        assert_eq!(
            st.verified.head.watermark_hex.as_deref(),
            Some(wm_hex(0, 6).as_str())
        );
    }
}
