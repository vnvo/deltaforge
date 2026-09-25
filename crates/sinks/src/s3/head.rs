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
    CompactionError, CompactionIndex, CompactionRecord, check_compatible,
    compacted_object_key, load_record, verify_originals_present, write_record,
};
use super::equivalence::verify_jsonl;
use super::gc::{
    Alarm, EntryRef, GC_MARK_VERSION, GcConfig, GcMark, GcPlan, GcRun,
    HeadBinding, InventoryRef, OriginalRef, SkipReason, horizon_seq, mark_key,
    marks_prefix,
};
use super::keys::content_hash;
use super::manifest::{
    ManifestEntry, ManifestObject, PrevRef, WrittenEntry, entry_key,
    propose_seq, write_entry,
};
use super::rollup::{
    InventoryIndex, RollupError, RollupRecord,
    load_inventory as rollup_load_inventory, load_record as rollup_load,
    object_digest as rollup_object_digest,
    write_inventory as rollup_write_inventory, write_record as rollup_write,
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

/// Wall-clock milliseconds since the Unix epoch. Used only to stamp the rollup
/// publication time into HEAD; GC compares it against an injected `now` and fails
/// closed on rollback/future skew, so this is not a correctness-critical clock.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

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
    /// The previous rollup reference, retained so a corrupt/missing current
    /// rollup can fall back without a full chain walk. Set together with
    /// `rollup_key/hash` when a new rollup is published.
    #[serde(default)]
    pub prev_rollup_key: Option<String>,
    #[serde(default)]
    pub prev_rollup_hash: Option<String>,
    /// Wall-clock time (ms since epoch) at which the current rollup became
    /// HEAD-published. This is the authoritative-age source for the GC safety
    /// window: it begins when the rollup becomes HEAD-published, NOT when its
    /// immutable object was uploaded. Stamped at the rollup CAS, preserved across
    /// ordinary batch/compaction publications, moved to `prev_rollup_published_at_ms`
    /// when a new rollup supersedes it, reconciled to the same value on a lost CAS
    /// response. Set together with `rollup_key/hash`.
    #[serde(default)]
    pub rollup_published_at_ms: Option<u64>,
    #[serde(default)]
    pub prev_rollup_published_at_ms: Option<u64>,
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
            prev_rollup_key: None,
            prev_rollup_hash: None,
            rollup_published_at_ms: None,
            prev_rollup_published_at_ms: None,
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
        if self.prev_rollup_key.is_some() != self.prev_rollup_hash.is_some() {
            return Err(HeadError::Integrity(
                "HEAD prev-rollup key/hash not both-set-or-both-unset".into(),
            ));
        }
        // The publication time travels with its rollup ref (present iff the ref is).
        if self.rollup_key.is_some() != self.rollup_published_at_ms.is_some() {
            return Err(HeadError::Integrity(
                "HEAD rollup ref/publication-time not both-set-or-both-unset"
                    .into(),
            ));
        }
        if self.prev_rollup_key.is_some()
            != self.prev_rollup_published_at_ms.is_some()
        {
            return Err(HeadError::Integrity(
                "HEAD prev-rollup ref/publication-time not both-set-or-both-unset"
                    .into(),
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
    load_entry_opt(store, key)
        .await?
        .ok_or_else(|| HeadError::Integrity(format!("missing entry {key}")))
}

/// Like [`load_entry`] but distinguishes a genuinely absent object (`Ok(None)`,
/// e.g. an entry authorized-GC'd below the recovery horizon) from a store/parse
/// error (`Err`). Recovery uses `None` on a `prev` link as a truncation boundary,
/// never as corruption.
async fn load_entry_opt<S: ConditionalStore + ?Sized>(
    store: &S,
    key: &str,
) -> Result<Option<ManifestEntry>, HeadError> {
    match store
        .get_with_etag(&object_store::path::Path::from(key))
        .await
        .map_err(|e| HeadError::Store(e.to_string()))?
    {
        Some((raw, _)) => serde_json::from_slice(&raw).map(Some).map_err(|e| {
            HeadError::Integrity(format!("entry {key} unparseable: {e}"))
        }),
        None => Ok(None),
    }
}

/// Per-seq summary collected while walking the authoritative chain, used to
/// verify rollups (boundary hashes, watermark, object digest) against retained
/// entries and to build the ack-chain object inventory.
struct EntrySummary {
    entry_hash: String,
    /// The hash of the entry immediately before this one (`prev.hash`), or `None`
    /// at genesis. Used to verify a rollup's end boundary against the first
    /// retained tail entry after covered entries are gone.
    prev_hash: Option<String>,
    watermark_hex: String,
    objects: Vec<ManifestObject>,
}

/// The result of fully verifying the retained manifest entries reachable from HEAD.
struct RetainedWalk {
    /// seq -> summary for every retained entry.
    entries: std::collections::BTreeMap<u64, EntrySummary>,
    /// Every object key referenced by a retained entry.
    inventory: std::collections::BTreeMap<String, ManifestObject>,
    /// The walk terminated at a valid genesis (intact chain).
    reached_genesis: bool,
    /// The walk stopped at a truncation boundary (authorized GC below the horizon):
    /// the highest seq whose entry is gone. A rollup selected for recovery must
    /// cover at least `[1 .. covered_end]`.
    truncated_covered_end: Option<u64>,
}

/// Walk HEAD -> genesis, fully verifying each retained entry (bytes/hash, identity,
/// seq continuity, epoch non-regression, strictly-advancing watermark, HEAD
/// watermark match, and every referenced data object) and collecting the per-seq
/// summaries + object inventory. A missing `prev` object is a truncation boundary
/// (authorized GC), not corruption; a missing HEAD entry, or any verification
/// failure on a present entry, is fatal.
async fn walk_retained_entries<S: ConditionalStore + ?Sized>(
    store: &S,
    pipeline: &str,
    source_id: &str,
    sink_id: &str,
    head: &Head,
    comparator: &dyn CheckpointComparator,
) -> Result<RetainedWalk, HeadError> {
    let mut inventory: std::collections::BTreeMap<String, ManifestObject> =
        std::collections::BTreeMap::new();
    let mut entries: std::collections::BTreeMap<u64, EntrySummary> =
        std::collections::BTreeMap::new();
    let mut reached_genesis = false;
    let mut truncated_covered_end: Option<u64> = None;

    if let Some(head_entry_key) = head.head_entry_key.clone() {
        let head_entry_hash = head
            .head_entry_hash
            .clone()
            .ok_or_else(|| HeadError::Integrity("HEAD hash missing".into()))?;

        let mut cur_key = head_entry_key;
        let mut expected_hash = head_entry_hash;
        let mut expected_seq: Option<u64> = None;
        let mut later_epoch: Option<u64> = None;
        let mut later_wm: Option<Vec<u8>> = None;
        let mut is_head_entry = true;
        let mut resolved = false;

        for _ in 0..MAX_CHAIN_WALK {
            let entry = match load_entry_opt(store, &cur_key).await? {
                Some(e) => e,
                None => {
                    // The HEAD entry itself must always exist.
                    if is_head_entry {
                        return Err(HeadError::Integrity(format!(
                            "HEAD references a missing entry {cur_key}"
                        )));
                    }
                    // A missing `prev` is a truncation boundary: entries at and
                    // below `expected_seq` were authorized-GC'd.
                    truncated_covered_end = Some(
                        expected_seq
                            .expect("expected_seq set for any non-head entry"),
                    );
                    resolved = true;
                    break;
                }
            };

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
            // 4. Epoch never regresses toward HEAD.
            if let Some(le) = later_epoch {
                if entry.epoch > le {
                    return Err(HeadError::Integrity(format!(
                        "epoch regression at {cur_key}: {} > later {le}",
                        entry.epoch
                    )));
                }
            }
            // 5. Watermark strictly advances toward HEAD.
            let cur_wm = unhex(&entry.watermark_hex).ok_or_else(|| {
                HeadError::Integrity(format!(
                    "entry {cur_key} watermark not hex"
                ))
            })?;
            if let Some(lw) = &later_wm {
                match comparator.order(lw, &cur_wm) {
                    CheckpointOrder::After => {}
                    other => {
                        return Err(HeadError::Integrity(format!(
                            "non-monotonic watermark at {cur_key}: later is \
                             {other:?} relative to it"
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
            // 6. Collect every referenced data object into the ack inventory as its
            // COMPLETE authoritative ManifestObject. The same key appearing twice
            // with different metadata is corruption. Object EXISTENCE is verified
            // later (in `verify_state`, after the compaction index is known), because
            // a compacted original may have been GC-deleted while its live entry
            // still references it - such an original is verified via its replacement.
            for mobj in &entry.objects {
                match inventory.get(&mobj.key) {
                    Some(existing) if existing != mobj => {
                        return Err(HeadError::Integrity(format!(
                            "object {} appears with conflicting metadata across \
                             entries",
                            mobj.key
                        )));
                    }
                    _ => {
                        inventory.insert(mobj.key.clone(), mobj.clone());
                    }
                }
            }
            entries.insert(
                entry.seq,
                EntrySummary {
                    entry_hash: entry.entry_hash(),
                    prev_hash: entry.prev.as_ref().map(|p| p.hash.clone()),
                    watermark_hex: entry.watermark_hex.clone(),
                    objects: entry.objects.clone(),
                },
            );

            match &entry.prev {
                None => {
                    if entry.seq != 1 {
                        return Err(HeadError::Integrity(format!(
                            "genesis entry {cur_key} has seq {} (expected 1)",
                            entry.seq
                        )));
                    }
                    reached_genesis = true;
                    resolved = true;
                    break;
                }
                Some(PrevRef { key, hash }) => {
                    let next_seq =
                        entry.seq.checked_sub(1).ok_or_else(|| {
                            HeadError::Integrity("sequence underflow".into())
                        })?;
                    if next_seq == 0 {
                        return Err(HeadError::Integrity(format!(
                            "entry {cur_key} has a prev link but seq reaches 0"
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
        if !resolved {
            return Err(HeadError::Integrity(
                "manifest chain exceeded the maximum walk length (cycle or \
                 runaway)"
                    .into(),
            ));
        }
    }

    Ok(RetainedWalk {
        entries,
        inventory,
        reached_genesis,
        truncated_covered_end,
    })
}

/// The fully-verified authoritative state reachable from HEAD, shared by recovery
/// (acquire) and GC planning so they cannot diverge. `ack_inventory` includes the
/// covered-range keys adopted from a verified rollup inventory after authorized GC
/// truncated the entry chain, so compaction-index membership is proven the same way
/// in both paths.
struct VerifiedState {
    entries: std::collections::BTreeMap<u64, EntrySummary>,
    ack_inventory: std::collections::BTreeMap<String, ManifestObject>,
    compactions: VerifiedCompactions,
    truncated_covered_end: Option<u64>,
}

/// Verify HEAD's authoritative chain and all referenced data objects; recover the
/// covered-range inventory from a verified rollup + inventory when authorized GC
/// truncated the entry chain (else advisory rollup verification on an intact chain);
/// then build + verify the authoritative compaction result against the complete ack
/// inventory. Every entry-chain, truncation-recovery, or compaction failure is fatal.
async fn verify_state<S: ConditionalStore + ?Sized>(
    store: &S,
    pipeline: &str,
    source_id: &str,
    sink_id: &str,
    head: &Head,
    comparator: &dyn CheckpointComparator,
) -> Result<VerifiedState, HeadError> {
    let RetainedWalk {
        entries,
        mut inventory,
        reached_genesis,
        truncated_covered_end,
    } = walk_retained_entries(
        store, pipeline, source_id, sink_id, head, comparator,
    )
    .await?;

    if let Some(covered_end) = truncated_covered_end {
        // Authorized-GC truncation: reconstruct the covered inventory from a
        // verified cumulative rollup + inventory (current, else previous fallback,
        // else hard halt), enforcing the one-generation horizon + nested pair.
        let covered = recover_covered_inventory(
            store,
            pipeline,
            source_id,
            sink_id,
            head,
            covered_end,
            &entries,
        )
        .await?;
        for o in &covered {
            match inventory.get(&o.key) {
                Some(existing) if existing != o => {
                    return Err(HeadError::Integrity(format!(
                        "covered-range object {} conflicts with a retained-tail \
                         object of the same key",
                        o.key
                    )));
                }
                _ => {
                    inventory.insert(o.key.clone(), o.clone());
                }
            }
        }
    } else if reached_genesis {
        // Intact chain: entries are ground truth, so the rollup is advisory
        // (verify + alarm on damage, never fatal).
        let _ = verify_rollup_chain(
            store, pipeline, source_id, sink_id, head, &entries,
        )
        .await;
    }

    let compactions = build_verified_compactions(
        store, pipeline, source_id, sink_id, head, &inventory,
    )
    .await?;

    // Verify existence of every retained-tail data object EXCEPT originals superseded
    // by a verified compaction: a superseded original may have been GC-deleted while
    // its live entry still references it, and its durable copy is the replacement
    // (already verified by build_verified_compactions). Covered-range objects (below a
    // truncation boundary) are attested by the adopted rollup inventory, not re-read.
    for summ in entries.values() {
        for o in &summ.objects {
            if compactions.index.active_replacement(&o.key).is_none() {
                verify_data_object(store, o).await?;
            }
        }
    }

    Ok(VerifiedState {
        entries,
        ack_inventory: inventory,
        compactions,
        truncated_covered_end,
    })
}

/// Recovery entry point: returns the authoritative compaction index. No
/// `Before`/`Equal` skipping is enabled unless this returns `Ok`.
async fn verify_chain<S: ConditionalStore + ?Sized>(
    store: &S,
    _prefix: &str,
    pipeline: &str,
    source_id: &str,
    sink_id: &str,
    head: &Head,
    comparator: &dyn CheckpointComparator,
) -> Result<CompactionIndex, HeadError> {
    Ok(
        verify_state(store, pipeline, source_id, sink_id, head, comparator)
            .await?
            .compactions
            .index,
    )
}

/// Verify that the current and previous rollup records form ONE valid nested
/// generation pair, so HEAD cannot select two individually-valid but unrelated
/// snapshots as a deletion proof: the current record's `prev` must equal HEAD's
/// retained previous reference, both must be cumulative (`start_seq == 1`), the
/// previous must end strictly before the current, and both publication timestamps
/// must be present (their ordering is checked by the safety-window skew guard).
fn verify_rollup_pair(
    current: &RollupRecord,
    previous: &RollupRecord,
    head: &Head,
) -> Result<(), HeadError> {
    match &current.prev {
        Some(PrevRef { key, hash })
            if Some(key.as_str()) == head.prev_rollup_key.as_deref()
                && Some(hash.as_str()) == head.prev_rollup_hash.as_deref() => {}
        _ => {
            return Err(HeadError::Integrity(
                "rollup pair: current rollup's prev does not equal HEAD's \
                 retained previous rollup reference"
                    .into(),
            ));
        }
    }
    if current.start_seq != 1 || previous.start_seq != 1 {
        return Err(HeadError::Integrity(
            "rollup pair: both generations must be cumulative (start_seq == 1)"
                .into(),
        ));
    }
    if previous.end_seq >= current.end_seq {
        return Err(HeadError::Integrity(format!(
            "rollup pair: previous end_seq {} is not before current end_seq {}",
            previous.end_seq, current.end_seq
        )));
    }
    if head.rollup_published_at_ms.is_none()
        || head.prev_rollup_published_at_ms.is_none()
    {
        return Err(HeadError::Integrity(
            "rollup pair: missing publication timestamp for a retained generation"
                .into(),
        ));
    }
    Ok(())
}

/// One authoritative, HEAD-reachable compaction record after full verification
/// (identity, domain compatibility, replacement durability, ack-inventory
/// membership, no conflicts). This is the ONLY structure GC planning may derive
/// data-deletion candidates from; it must never re-walk raw records with weaker
/// checks.
#[derive(Debug, Clone)]
struct VerifiedCompaction {
    record_hash: String,
    replacement: ManifestObject,
    originals: Vec<ManifestObject>,
    /// The equivalence proof bound into the (HEAD-reachable) record at publication.
    /// Only `true` records authorize deleting their originals.
    equivalence_result: bool,
}

/// The authoritative compaction result: the original -> replacement index (for
/// recovery) and the verified records in HEAD-reachable order (for GC planning +
/// equivalence). Both consumers share this single verified walk.
#[derive(Debug, Clone, Default)]
struct VerifiedCompactions {
    index: CompactionIndex,
    records: Vec<VerifiedCompaction>,
}

async fn build_verified_compactions<S: ConditionalStore + ?Sized>(
    store: &S,
    pipeline: &str,
    source_id: &str,
    sink_id: &str,
    head: &Head,
    ack_inventory: &std::collections::BTreeMap<String, ManifestObject>,
) -> Result<VerifiedCompactions, HeadError> {
    let mut out = VerifiedCompactions::default();
    let (Some(mut cur_key), Some(mut expected_hash)) =
        (head.compaction_key.clone(), head.compaction_hash.clone())
    else {
        return Ok(out);
    };
    let mut seen = std::collections::HashSet::new();
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
        // The replacement must be durably present with its recorded hash + size.
        verify_data_object(store, &rec.replacement).await?;
        for o in &rec.originals {
            // Every original must be an authoritative ack-chain object with EXACTLY
            // the acknowledged metadata (key + content_hash + byte_len + table +
            // full encoding domain). A record naming an authoritative key but
            // substituting any other field is rejected, so GC can never delete a
            // real acknowledged object under a mismatched replacement. This also
            // rejects transitive compaction (a replacement is never in the ack
            // inventory).
            match ack_inventory.get(&o.key) {
                Some(authoritative) if authoritative == o => {}
                Some(_) => {
                    return Err(HeadError::Integrity(format!(
                        "compaction original {} metadata does not match the \
                         acknowledged object",
                        o.key
                    )));
                }
                None => {
                    return Err(HeadError::Integrity(format!(
                        "compaction original {} is not referenced by the \
                         acknowledgement chain (missing or transitive)",
                        o.key
                    )));
                }
            }
            // No original may appear in two records (conflicting active
            // compaction).
            if out
                .index
                .insert(o.key.clone(), rec.replacement.clone())
                .is_some()
            {
                return Err(HeadError::Integrity(format!(
                    "original {} appears in conflicting compactions",
                    o.key
                )));
            }
        }
        out.records.push(VerifiedCompaction {
            record_hash: expected_hash.clone(),
            replacement: rec.replacement.clone(),
            originals: rec.originals.clone(),
            equivalence_result: rec.equivalence_result,
        });
        match &rec.prev {
            None => return Ok(out),
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

/// Result of verifying the rollup chain against retained entries. In 9A entries
/// are the ground truth, so a damaged rollup never fails recovery - it alarms and
/// falls back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RollupCheck {
    /// No rollup, or the current rollup fully verified.
    Healthy,
    /// The current rollup was damaged; fell back (alarm raised).
    DamagedFellBack,
}

/// Advisory rollup verification for the INTACT chain (entries are ground truth):
/// verify the current cumulative rollup + its inventory index; on damage, warn and
/// try the previous generation; never fatal. Discards the recovered inventory.
async fn verify_rollup_chain<S: ConditionalStore + ?Sized>(
    store: &S,
    pipeline: &str,
    source_id: &str,
    sink_id: &str,
    head: &Head,
    entries: &std::collections::BTreeMap<u64, EntrySummary>,
) -> RollupCheck {
    let Some((key, hash)) =
        head.rollup_key.clone().zip(head.rollup_hash.clone())
    else {
        return RollupCheck::Healthy; // no rollup
    };

    match verify_cumulative_rollup(
        store, pipeline, source_id, sink_id, head, &key, &hash, 0, entries,
    )
    .await
    {
        Ok(_) => RollupCheck::Healthy,
        Err(e) => {
            tracing::warn!(
                pipeline = %pipeline,
                error = %e,
                "durable S3 current rollup failed verification; falling back \
                 (entries retained) - this is an alarm, not silent trust"
            );
            if let Some((pk, ph)) = head
                .prev_rollup_key
                .clone()
                .zip(head.prev_rollup_hash.clone())
            {
                if let Err(e2) = verify_cumulative_rollup(
                    store, pipeline, source_id, sink_id, head, &pk, &ph, 0,
                    entries,
                )
                .await
                {
                    tracing::warn!(
                        pipeline = %pipeline,
                        error = %e2,
                        "durable S3 previous rollup also failed; falling back \
                         to the full verified entry chain"
                    );
                }
            }
            RollupCheck::DamagedFellBack
        }
    }
}

/// Recover the authoritative covered-range inventory after authorized GC truncated
/// the entry chain. The PREVIOUS generation is required: it defines the
/// one-generation fallback horizon and independently covers every removed entry. A
/// gap ABOVE that horizon (a hole in the promised retained tail) is corruption, not
/// authorized GC, and HALTS. Then verify the nested pair, try the current rollup,
/// else fall back to the previous, else HALT. Never walks beyond the previous
/// generation (a dangling `prev.prev` after authorized GC is expected).
async fn recover_covered_inventory<S: ConditionalStore + ?Sized>(
    store: &S,
    pipeline: &str,
    source_id: &str,
    sink_id: &str,
    head: &Head,
    covered_end: u64,
    entries: &std::collections::BTreeMap<u64, EntrySummary>,
) -> Result<Vec<ManifestObject>, HeadError> {
    // The previous generation authorizes truncation and defines the horizon.
    let (pk, ph) = head
        .prev_rollup_key
        .clone()
        .zip(head.prev_rollup_hash.clone())
        .ok_or_else(|| {
            HeadError::Integrity(format!(
                "entries were removed through seq {covered_end} but there is no \
                 previous rollup generation to authorize or cover them (hard halt)"
            ))
        })?;
    let prev_rec = rollup_load(store, &pk, &ph).await.map_err(|e| {
        HeadError::Integrity(format!(
            "previous rollup unreadable; cannot establish the fallback horizon or \
             authorize truncation (hard halt): {e}"
        ))
    })?;
    let horizon = prev_rec.end_seq;
    // The removed range must lie within the previous generation's coverage.
    if covered_end > horizon {
        return Err(HeadError::Integrity(format!(
            "missing entry at seq {covered_end} is within the promised retained \
             tail above the fallback horizon {horizon}: not authorized GC (hard \
             halt)"
        )));
    }

    // Try the current generation (verifying the nested pair first), then fall back
    // to the previous generation, else halt.
    if let Some((ck, chash)) =
        head.rollup_key.clone().zip(head.rollup_hash.clone())
    {
        match rollup_load(store, &ck, &chash).await {
            Ok(cur_rec) => {
                verify_rollup_pair(&cur_rec, &prev_rec, head)?;
                match verify_cumulative_rollup(
                    store,
                    pipeline,
                    source_id,
                    sink_id,
                    head,
                    &ck,
                    &chash,
                    covered_end,
                    entries,
                )
                .await
                {
                    Ok(objs) => return Ok(objs),
                    Err(e) => tracing::warn!(
                        pipeline = %pipeline,
                        error = %e,
                        "durable S3 current rollup unusable for covered-range \
                         recovery; falling back to the previous generation - alarm"
                    ),
                }
            }
            Err(e) => tracing::warn!(
                pipeline = %pipeline,
                error = %e,
                "durable S3 current rollup record unreadable; falling back to the \
                 previous generation - alarm"
            ),
        }
    }

    verify_cumulative_rollup(
        store, pipeline, source_id, sink_id, head, &pk, &ph, covered_end,
        entries,
    )
    .await
    .map_err(|e2| {
        HeadError::Integrity(format!(
            "both retained rollup generations unusable; cannot recover the covered \
             range (hard halt): {e2}"
        ))
    })
}

/// Verify a single CUMULATIVE rollup `(key, hash)` and its bound inventory index
/// against the retained `entries`, returning the covered inventory objects. Checks:
/// hash/identity; cumulative invariants (`start_seq == 1`, covers at least
/// `[1 .. min_covered_end]`); the inventory index binds (record hash, declared
/// range == rollup range, count == object_count, object digest == rollup digest);
/// the end boundary + watermark against HEAD (if it ends at HEAD) or against the
/// first retained tail entry's `prev` (and the end entry itself when retained); and,
/// when the full covered range is still retained (intact chain), exact equality
/// between the inventory and the reconstructed-from-entries inventory. Verifies ONE
/// generation only - never walks `prev`.
#[allow(clippy::too_many_arguments)]
async fn verify_cumulative_rollup<S: ConditionalStore + ?Sized>(
    store: &S,
    pipeline: &str,
    source_id: &str,
    sink_id: &str,
    head: &Head,
    key: &str,
    hash: &str,
    min_covered_end: u64,
    entries: &std::collections::BTreeMap<u64, EntrySummary>,
) -> Result<Vec<ManifestObject>, HeadError> {
    let rec = rollup_load(store, key, hash).await.map_err(map_rollup)?;
    if rec.pipeline != pipeline
        || rec.source_id != source_id
        || rec.sink_id != sink_id
    {
        return Err(HeadError::Integrity(format!(
            "rollup {key} identity mismatch"
        )));
    }
    // Cumulative invariants.
    if rec.start_seq != 1 {
        return Err(HeadError::Integrity(format!(
            "rollup {key} is not cumulative: start_seq {} != 1",
            rec.start_seq
        )));
    }
    if rec.end_seq < 1 {
        return Err(HeadError::Integrity(format!(
            "rollup {key} has invalid end_seq {}",
            rec.end_seq
        )));
    }
    if rec.end_seq < min_covered_end {
        return Err(HeadError::Integrity(format!(
            "rollup {key} end_seq {} does not cover the removed range down to \
             {min_covered_end}",
            rec.end_seq
        )));
    }
    // Load + verify the bound inventory index.
    let inv = rollup_load_inventory(
        store,
        &rec.inventory_key,
        &rec.inventory_record_hash,
    )
    .await
    .map_err(map_rollup)?;
    if inv.pipeline != pipeline
        || inv.source_id != source_id
        || inv.sink_id != sink_id
    {
        return Err(HeadError::Integrity(format!(
            "inventory index {} identity mismatch",
            rec.inventory_key
        )));
    }
    if inv.start_seq != rec.start_seq || inv.end_seq != rec.end_seq {
        return Err(HeadError::Integrity(format!(
            "inventory index {} range [{}, {}] differs from rollup [{}, {}]",
            rec.inventory_key,
            inv.start_seq,
            inv.end_seq,
            rec.start_seq,
            rec.end_seq
        )));
    }
    if inv.objects.len() as u64 != rec.object_count
        || rec.inventory_count != rec.object_count
    {
        return Err(HeadError::Integrity(format!(
            "inventory index {} count mismatch (inventory {}, rollup object \
             {}, rollup inventory {})",
            rec.inventory_key,
            inv.objects.len(),
            rec.object_count,
            rec.inventory_count
        )));
    }
    if rollup_object_digest(&inv.objects) != rec.object_digest {
        return Err(HeadError::Integrity(format!(
            "inventory index {} object digest does not match the rollup",
            rec.inventory_key
        )));
    }
    // End boundary + watermark.
    if rec.end_seq == head.seq {
        if head.head_entry_hash.as_deref() != Some(rec.end_entry_hash.as_str())
        {
            return Err(HeadError::Integrity(format!(
                "rollup {key} end hash does not match the HEAD entry"
            )));
        }
        if head.watermark_hex.as_deref() != Some(rec.watermark_hex.as_str()) {
            return Err(HeadError::Integrity(format!(
                "rollup {key} watermark does not match HEAD"
            )));
        }
    } else {
        // Ends below HEAD: the entry just after the rollup must be retained and
        // link back to the rollup's end hash (this is the post-GC boundary check).
        let tail = entries.get(&(rec.end_seq + 1)).ok_or_else(|| {
            HeadError::Integrity(format!(
                "rollup {key} end_seq {} + 1 is not retained: cannot verify the \
                 boundary (gap)",
                rec.end_seq
            ))
        })?;
        if tail.prev_hash.as_deref() != Some(rec.end_entry_hash.as_str()) {
            return Err(HeadError::Integrity(format!(
                "rollup {key} end hash does not match the first retained tail \
                 entry's prev"
            )));
        }
        // If the end entry itself is retained (intact chain), cross-check it.
        if let Some(end_entry) = entries.get(&rec.end_seq) {
            if end_entry.entry_hash != rec.end_entry_hash {
                return Err(HeadError::Integrity(format!(
                    "rollup {key} end entry hash mismatch"
                )));
            }
            if end_entry.watermark_hex != rec.watermark_hex {
                return Err(HeadError::Integrity(format!(
                    "rollup {key} watermark does not match its end entry"
                )));
            }
        }
    }
    // Equality proof when the whole covered range is still retained (intact chain):
    // the inventory index must EXACTLY equal the reconstructed-from-entries
    // inventory (same objects, same canonical order).
    let full_present = (1..=rec.end_seq).all(|s| entries.contains_key(&s));
    if full_present {
        let mut recon: Vec<ManifestObject> = Vec::new();
        for s in 1..=rec.end_seq {
            recon.extend(entries[&s].objects.iter().cloned());
        }
        if recon != inv.objects {
            return Err(HeadError::Integrity(format!(
                "inventory index {} does not equal the reconstructed-from-entries \
                 inventory",
                rec.inventory_key
            )));
        }
    }
    Ok(inv.objects)
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
    /// The authoritative compaction index built and verified at acquire. GC
    /// consumes this rather than rescanning objects. (A live writer's own
    /// compactions after acquire are not reflected here; GC re-acquires.)
    compaction_index: CompactionIndex,
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
                    // against (steps 2-3). Also builds the authoritative
                    // compaction index and verifies the rollup chain.
                    let compaction_index = verify_chain(
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
                                compaction_index,
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
                                CompactionIndex::default(),
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
        compaction_index: CompactionIndex,
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
            compaction_index,
        }
    }

    /// Current epoch (test/observability).
    pub async fn epoch(&self) -> u64 {
        self.state.lock().await.verified.epoch
    }

    /// The authoritative compaction index built at acquire (for GC in 9B).
    pub fn compaction_index(&self) -> &CompactionIndex {
        &self.compaction_index
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
            // (The rebuilt compaction index is not needed on this mid-publish
            // path; it is captured at acquire.)
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
            .map(|_| ())
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
                // Ordinary publication preserves BOTH rollup references and the
                // compaction reference untouched.
                rollup_key: st.verified.head.rollup_key.clone(),
                rollup_hash: st.verified.head.rollup_hash.clone(),
                prev_rollup_key: st.verified.head.prev_rollup_key.clone(),
                prev_rollup_hash: st.verified.head.prev_rollup_hash.clone(),
                rollup_published_at_ms: st.verified.head.rollup_published_at_ms,
                prev_rollup_published_at_ms: st
                    .verified
                    .head
                    .prev_rollup_published_at_ms,
                watermark_hex: Some(watermark_hex.clone()),
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

                    // Fencing is decided FIRST, before any reference match: a higher
                    // epoch may preserve our exact entry reference, and a stale writer
                    // must never mistake that for its own lost response.
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
                    // Same epoch only: a lost/ambiguous response that actually
                    // succeeded leaves HEAD referencing our exact entry -> idempotent
                    // success.
                    if cur.references(&written) {
                        st.verified.head = cur;
                        st.verified.etag = etag;
                        return Ok(()); // ACKNOWLEDGE (idempotent)
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

    /// Production compaction: replace a selection of authoritative `originals`
    /// (already-durable ack-chain objects for ONE table) with a replacement built
    /// INTERNALLY from their exact verified bytes by trusted code - callers never
    /// supply replacement bytes. Verifies every original is authoritative, orders
    /// them by acknowledgement sequence + per-entry object order, decodes and
    /// deterministically re-encodes the replacement, then publishes it (immutable
    /// object + compaction record + HEAD CAS setting only the compaction
    /// reference). Deletes nothing; marks nothing deletion-eligible (that is 9B).
    pub async fn compact(
        &self,
        originals: Vec<ManifestObject>,
    ) -> Result<(), HeadError> {
        let first = originals.first().ok_or_else(|| {
            HeadError::Integrity("no originals to compact".into())
        })?;
        let table = first.table.clone();
        let domain = first.encoding_domain();
        check_compatible(&table, &domain, &originals)
            .map_err(map_compaction)?;

        // Order by acknowledgement sequence (and per-entry object order), and
        // verify every original is an authoritative ack-chain object.
        let ordered = self.order_originals_by_ack(originals).await?;
        // Build the replacement from the originals' exact verified bytes.
        let replacement =
            self.build_replacement(&table, &domain, &ordered).await?;
        self.publish_compaction(replacement, ordered).await
    }

    /// Order `originals` by their position in the authoritative acknowledgement
    /// chain (sequence, then per-entry object index) and confirm each is an
    /// ack-chain object with matching content hash. Rejects any original not
    /// found in the chain (not authoritative / already superseded to a
    /// non-ack-chain object).
    async fn order_originals_by_ack(
        &self,
        originals: Vec<ManifestObject>,
    ) -> Result<Vec<ManifestObject>, HeadError> {
        // Snapshot the current head entry to walk from (brief lock).
        let (mut cur_key, mut cur_hash) = {
            let st = self.state.lock().await;
            match (
                st.verified.head.head_entry_key.clone(),
                st.verified.head.head_entry_hash.clone(),
            ) {
                (Some(k), Some(h)) => (k, h),
                _ => {
                    return Err(HeadError::Integrity(
                        "cannot compact: HEAD has no entries".into(),
                    ));
                }
            }
        };
        // object key -> (seq, index-in-entry, authoritative content_hash).
        let mut pos: std::collections::HashMap<String, (u64, usize, String)> =
            std::collections::HashMap::new();
        for _ in 0..MAX_CHAIN_WALK {
            let entry = load_entry(self.store.as_ref(), &cur_key).await?;
            if entry.entry_hash() != cur_hash {
                return Err(HeadError::Integrity(format!(
                    "compaction walk: entry {cur_key} hash mismatch"
                )));
            }
            for (idx, o) in entry.objects.iter().enumerate() {
                pos.entry(o.key.clone()).or_insert((
                    entry.seq,
                    idx,
                    o.content_hash.clone(),
                ));
            }
            match &entry.prev {
                Some(PrevRef { key, hash }) => {
                    cur_key = key.clone();
                    cur_hash = hash.clone();
                }
                None => break,
            }
        }

        let mut keyed: Vec<((u64, usize), ManifestObject)> =
            Vec::with_capacity(originals.len());
        for o in originals {
            match pos.get(&o.key) {
                Some((seq, idx, chash)) => {
                    if *chash != o.content_hash {
                        return Err(HeadError::Integrity(format!(
                            "compaction original {} content hash does not match \
                             the acknowledgement chain",
                            o.key
                        )));
                    }
                    keyed.push(((*seq, *idx), o));
                }
                None => {
                    return Err(HeadError::Integrity(format!(
                        "compaction original {} is not an authoritative \
                         acknowledgement-chain object",
                        o.key
                    )));
                }
            }
        }
        keyed.sort_by_key(|(pos, _)| *pos);
        Ok(keyed.into_iter().map(|(_, o)| o).collect())
    }

    /// Build the replacement object from the originals' exact verified bytes,
    /// deterministically. 9A supports JSONL (canonical lines concatenated in ack
    /// order - a deterministic re-encode); Parquet internal compaction needs the
    /// Arrow schema and lands later.
    async fn build_replacement(
        &self,
        table: &str,
        domain: &super::keys::EncodingDomain,
        ordered: &[ManifestObject],
    ) -> Result<TableObject, HeadError> {
        if domain.format != "jsonl" {
            return Err(HeadError::Integrity(format!(
                "internal compaction supports only jsonl in 9A, not {}",
                domain.format
            )));
        }
        let mut bytes: Vec<u8> = Vec::new();
        for o in ordered {
            let (raw, _) = self
                .store
                .get_with_etag(&object_store::path::Path::from(o.key.clone()))
                .await
                .map_err(|e| HeadError::Store(e.to_string()))?
                .ok_or_else(|| {
                    HeadError::Integrity(format!(
                        "original {} missing while building replacement",
                        o.key
                    ))
                })?;
            if content_hash(&raw, domain) != o.content_hash {
                return Err(HeadError::Integrity(format!(
                    "original {} content changed while building replacement",
                    o.key
                )));
            }
            bytes.extend_from_slice(&raw);
        }
        Ok(TableObject {
            table: table.to_string(),
            bytes: bytes::Bytes::from(bytes),
            domain: domain.clone(),
            ext: "jsonl",
        })
    }

    /// Test-only: publish a compaction with a caller-supplied replacement. NOT
    /// available in production (GC never trusts an arbitrarily-supplied
    /// replacement); production uses [`DurableWriter::compact`].
    #[cfg(test)]
    pub async fn compact_with_replacement(
        &self,
        replacement: TableObject,
        originals: Vec<ManifestObject>,
    ) -> Result<(), HeadError> {
        self.publish_compaction(replacement, originals).await
    }

    /// Shared compaction publication protocol: compatibility gate -> read/verify
    /// originals -> upload the replacement (immutable, create-only) -> write the
    /// compaction record -> CAS HEAD to set ONLY the compaction reference,
    /// preserving epoch/seq/entry/watermark/rollup. Serialized through the writer
    /// mutex. Originals are NOT deleted.
    async fn publish_compaction(
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
            compression: domain.compression.clone(),
            partition_spec: domain.partition_spec.clone(),
            partition_version: domain.partition_version,
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
        // Prove equivalence BEFORE publication: the independent row-wise validator
        // decodes both sides from stored bytes and compares. A compaction is published
        // ONLY if equivalence holds - a validator failure (non-equivalent,
        // non-JSONL/unsupported, or a transient read error) propagates here WITHOUT
        // writing the record or advancing HEAD, leaving the uploaded replacement as a
        // harmless orphan. Every published v3 record therefore carries a successful
        // proof (`equivalence_result == true`), and a HEAD-reachable record with
        // `false` is treated as corruption at recovery.
        verify_jsonl(self.store.as_ref(), &originals, &replacement_obj)
            .await
            .map_err(|e| {
                HeadError::Integrity(format!(
                    "compaction replacement failed equivalence; not published: {e}"
                ))
            })?;
        let record = CompactionRecord {
            version: super::compaction::COMPACTION_RECORD_VERSION,
            pipeline: self.pipeline.clone(),
            source_id: self.source_id.clone(),
            sink_id: self.sink_id.clone(),
            table,
            replacement: replacement_obj,
            originals,
            equivalence_algo: super::compaction::EQUIVALENCE_ALGO.to_string(),
            equivalence_version: super::compaction::EQUIVALENCE_VERSION,
            equivalence_result: true,
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
                    // Fencing FIRST: a higher epoch may have published the same
                    // content-addressed compaction record, so a matching hash must
                    // never be mistaken for our own lost response. The uploaded
                    // object + record are harmless orphans.
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
                    // Same epoch only: a lost response that actually succeeded leaves
                    // HEAD at the EXACT proposed transition (not merely a matching
                    // compaction hash) -> idempotent success.
                    if cur == next {
                        st.verified.head = cur;
                        st.verified.etag = etag;
                        return Ok(());
                    }
                    // Same epoch, different head (a transient error, or our own
                    // earlier batch advanced HEAD): adopt it and re-apply the
                    // compaction reference on top, preserving its ack state.
                    st.verified.head = cur;
                    st.verified.etag = etag;
                    continue;
                }
            }
        }
        Err(HeadError::RetriesExhausted(MAX_PUBLISH_ATTEMPTS))
    }

    /// Publish a CUMULATIVE rollup snapshot of `[1 .. current HEAD seq]` of the
    /// already-verified chain, with a retained inventory index. Writes the
    /// inventory index + an immutable rollup record chained to the current rollup
    /// (nested: same start seq 1, strictly greater end seq), then CASes HEAD to set
    /// the rollup + previous-rollup references and publication times, preserving
    /// epoch, seq, entry, watermark and compaction references. Serialized through
    /// the writer mutex; reversible (summarizes retained entries, deletes nothing).
    pub async fn rollup(&self) -> Result<(), HeadError> {
        let mut st = self.state.lock().await;
        if let Some(observed) = st.fenced {
            return Err(HeadError::Fenced {
                our: st.verified.epoch,
                observed,
            });
        }
        let end_seq = st.verified.head.seq;
        if end_seq == 0 {
            return Err(HeadError::Integrity(
                "nothing to roll up (genesis HEAD)".into(),
            ));
        }
        let end_entry_hash =
            st.verified.head.head_entry_hash.clone().ok_or_else(|| {
                HeadError::Integrity("HEAD has no entry to roll up".into())
            })?;
        let watermark_hex =
            st.verified.head.watermark_hex.clone().ok_or_else(|| {
                HeadError::Integrity("HEAD has no watermark".into())
            })?;

        // Cumulative snapshots always start at genesis and are nested (strictly
        // greater end_seq). The next inventory is BASE + TAIL, where BASE is the
        // current cumulative inventory (covering [1 .. cur_end]) and TAIL is the
        // retained entries strictly above cur_end. Building on the current
        // inventory (not a walk back to seq 1) is what lets a rollup be created
        // AFTER entries below the horizon have been GC'd. For the first rollup
        // (no current) BASE is empty and TAIL is [1 .. end_seq] from genesis.
        let start_seq: u64 = 1;
        let head_entry_key =
            st.verified.head.head_entry_key.clone().ok_or_else(|| {
                HeadError::Integrity("HEAD has no entry key".into())
            })?;
        let (mut objects, base_start_hash, cur_end): (
            Vec<ManifestObject>,
            Option<String>,
            u64,
        ) = match (
            st.verified.head.rollup_key.clone(),
            st.verified.head.rollup_hash.clone(),
        ) {
            (Some(k), Some(h)) => {
                let cur = rollup_load(self.store.as_ref(), &k, &h)
                    .await
                    .map_err(map_rollup)?;
                if cur.end_seq >= end_seq {
                    return Err(HeadError::Integrity(
                        "rollup range is empty (already summarized to HEAD)"
                            .into(),
                    ));
                }
                let inv = rollup_load_inventory(
                    self.store.as_ref(),
                    &cur.inventory_key,
                    &cur.inventory_record_hash,
                )
                .await
                .map_err(map_rollup)?;
                if inv.start_seq != 1 || inv.end_seq != cur.end_seq {
                    return Err(HeadError::Integrity(
                        "current rollup inventory range does not match its record"
                            .into(),
                    ));
                }
                (inv.objects, Some(cur.start_entry_hash.clone()), cur.end_seq)
            }
            _ => (Vec::new(), None, 0),
        };

        // Walk the retained tail HEAD -> (cur_end + 1), collecting objects in
        // canonical order and, for a first rollup, the genesis boundary hash.
        let stop_at = cur_end + 1;
        let mut cur_key = head_entry_key;
        let mut cur_hash = end_entry_hash.clone();
        let mut tail: Vec<((u64, usize), ManifestObject)> = Vec::new();
        let mut genesis_hash: Option<String> = None;
        for _ in 0..MAX_CHAIN_WALK {
            let entry = load_entry(self.store.as_ref(), &cur_key).await?;
            if entry.entry_hash() != cur_hash {
                return Err(HeadError::Integrity(format!(
                    "rollup walk: entry {cur_key} hash mismatch"
                )));
            }
            for (idx, o) in entry.objects.iter().enumerate() {
                tail.push(((entry.seq, idx), o.clone()));
            }
            if entry.seq == stop_at {
                if cur_end == 0 {
                    genesis_hash = Some(entry.entry_hash());
                }
                break;
            }
            match &entry.prev {
                Some(PrevRef { key, hash }) => {
                    cur_key = key.clone();
                    cur_hash = hash.clone();
                }
                None => {
                    return Err(HeadError::Integrity(format!(
                        "rollup walk reached genesis before seq {stop_at}: the \
                         retained tail is incomplete"
                    )));
                }
            }
        }
        tail.sort_by_key(|(pos, _)| *pos);
        objects.extend(tail.into_iter().map(|(_, o)| o));

        let start_entry_hash = match (base_start_hash, genesis_hash) {
            (Some(h), _) => h, // inherited from the current cumulative rollup
            (None, Some(h)) => h, // first rollup: the genesis entry hash
            (None, None) => {
                return Err(HeadError::Integrity(
                    "could not determine the genesis boundary hash for the rollup"
                        .into(),
                ));
            }
        };

        // Build + write the retained inventory index, then bind it into the record.
        let inventory = InventoryIndex {
            version: super::rollup::INVENTORY_INDEX_VERSION,
            pipeline: self.pipeline.clone(),
            source_id: self.source_id.clone(),
            sink_id: self.sink_id.clone(),
            start_seq,
            end_seq,
            objects: objects.clone(),
        };
        let written_inv = rollup_write_inventory(
            self.store.as_ref(),
            &self.prefix,
            &inventory,
        )
        .await
        .map_err(map_rollup)?;

        let prev =
            match (&st.verified.head.rollup_key, &st.verified.head.rollup_hash)
            {
                (Some(k), Some(h)) => Some(PrevRef {
                    key: k.clone(),
                    hash: h.clone(),
                }),
                _ => None,
            };
        let record = RollupRecord {
            version: super::rollup::ROLLUP_RECORD_VERSION,
            pipeline: self.pipeline.clone(),
            source_id: self.source_id.clone(),
            sink_id: self.sink_id.clone(),
            start_seq,
            end_seq,
            start_entry_hash,
            end_entry_hash,
            watermark_hex,
            object_count: objects.len() as u64,
            object_digest: rollup_object_digest(&objects),
            inventory_key: written_inv.key.clone(),
            inventory_record_hash: written_inv.record_hash.clone(),
            inventory_count: written_inv.count,
            prev,
        };
        let written = rollup_write(self.store.as_ref(), &self.prefix, &record)
            .await
            .map_err(map_rollup)?;

        // The current generation's authoritative age begins now, at HEAD
        // publication (not at object upload). Computed once so a lost-response
        // retry reconciles to the value that actually landed.
        let published_now = now_ms();

        // CAS HEAD: set the rollup + previous-rollup references (and publication
        // times), preserving everything else. Reconcile a lost response only if
        // HEAD references our exact rollup.
        let hkey = head_key(&self.prefix, &self.pipeline);
        for _ in 0..MAX_PUBLISH_ATTEMPTS {
            let next = Head {
                rollup_key: Some(written.key.clone()),
                rollup_hash: Some(written.hash.clone()),
                prev_rollup_key: st.verified.head.rollup_key.clone(),
                prev_rollup_hash: st.verified.head.rollup_hash.clone(),
                rollup_published_at_ms: Some(published_now),
                prev_rollup_published_at_ms: st
                    .verified
                    .head
                    .rollup_published_at_ms,
                ..st.verified.head.clone()
            };
            match self
                .store
                .cas_put(&hkey, next.canonical_bytes(), &st.verified.etag)
                .await
            {
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
                                "HEAD missing/eTagless during rollup".into(),
                            ));
                        }
                    };
                    let cur = Head::parse(&raw)?;
                    // Fencing FIRST: a higher epoch permanently fences us, even if
                    // it happened to publish the same immutable rollup object.
                    if cur.epoch > st.verified.epoch {
                        st.fenced = Some(cur.epoch);
                        return Err(HeadError::Fenced {
                            our: st.verified.epoch,
                            observed: cur.epoch,
                        });
                    }
                    if cur.epoch < st.verified.epoch {
                        return Err(HeadError::Integrity(format!(
                            "HEAD epoch regressed during rollup: {} < {}",
                            cur.epoch, st.verified.epoch
                        )));
                    }
                    // Lost-response success requires the EXACT proposed transition
                    // (epoch, both rollup refs, publication times, and all preserved
                    // HEAD state), not merely a matching rollup hash - so another
                    // writer's identical rollup at our epoch is never mistaken for
                    // our own landed write.
                    if cur == next {
                        st.verified.head = cur;
                        st.verified.etag = etag;
                        return Ok(());
                    }
                    // Same epoch, different HEAD: adopt it and retry on top.
                    st.verified.head = cur;
                    st.verified.etag = etag;
                    continue;
                }
            }
        }
        Err(HeadError::RetriesExhausted(MAX_PUBLISH_ATTEMPTS))
    }

    /// Compute a DRY-RUN GC plan against the current verified HEAD (P0.4, 9B.1).
    /// Read-only, owner-fenced, and bound to the exact HEAD identity: it computes
    /// what WOULD be deleted and DELETES/MARKS NOTHING. Manifest-side eligibility
    /// requires the current cumulative rollup + its inventory to verify (as a
    /// subsequent generation over the retained entries), the previous fallback
    /// generation to verify, the safety window to have elapsed with a sane clock,
    /// and entries at `seq <= horizon`. Data-side eligibility requires an original
    /// to be superseded by a durable, equivalence-proven, HEAD-reachable compaction
    /// record. Serialized through the writer mutex; a fenced writer or any HEAD
    /// drift produces no plan.
    pub async fn plan_gc(
        &self,
        now_ms: u64,
        cfg: GcConfig,
    ) -> Result<GcPlan, HeadError> {
        let st = self.state.lock().await;
        if let Some(observed) = st.fenced {
            return Err(HeadError::Fenced {
                our: st.verified.epoch,
                observed,
            });
        }
        let head = st.verified.head.clone();
        let etag = st.verified.etag.clone();

        // Re-read HEAD immediately before planning: any drift from our verified view
        // means concurrent activity - refuse rather than plan against stale state.
        let hkey = head_key(&self.prefix, &self.pipeline);
        let (raw, cur_etag) = match self
            .store
            .get_with_etag(&hkey)
            .await
            .map_err(|e| HeadError::Store(e.to_string()))?
        {
            Some((raw, Some(e))) => (raw, e),
            _ => {
                return Err(HeadError::Integrity(
                    "HEAD missing/eTagless during GC planning".into(),
                ));
            }
        };
        if cur_etag != etag {
            let cur = Head::parse(&raw)?;
            if cur.epoch > head.epoch {
                return Err(HeadError::Fenced {
                    our: head.epoch,
                    observed: cur.epoch,
                });
            }
            return Err(HeadError::Store(
                "HEAD changed during GC planning; recompute".into(),
            ));
        }

        let mut alarms: Vec<Alarm> = Vec::new();
        let mut skipped: Vec<SkipReason> = Vec::new();
        let mut manifest_entries_eligible: Vec<EntryRef> = Vec::new();
        // Out-of-horizon inventory-index / older-generation rollup GC is
        // deliberately OUT OF SCOPE for 9B.1 and 9B.2: those objects stay ineligible
        // (never listed) until a later milestone implements older-generation
        // planning. NOTE: older immutable rollup + inventory objects (generations
        // before `previous`) DO exist once several rollups have been published; they
        // are simply retained indefinitely and excluded from GC here, NOT absent.
        // Later reconciliation must not misclassify them as sweepable orphans.
        let inventory_indexes_eligible: Vec<InventoryRef> = Vec::new();
        let mut data_originals_eligible: Vec<OriginalRef> = Vec::new();

        // Shared authoritative verification, identical to recovery: retained entries
        // + covered-inventory adoption + the verified compaction result. Data-side
        // candidates come ONLY from `state.compactions.records`, which are already
        // proven for identity, domain compatibility, ack-inventory membership (incl.
        // the adopted covered range), and conflicts.
        let state = verify_state(
            self.store.as_ref(),
            &self.pipeline,
            &self.source_id,
            &self.sink_id,
            &head,
            self.comparator.as_ref(),
        )
        .await?;

        // ---- Manifest-side eligibility ----
        let mut horizon = 0u64;
        let mut cur_inv_hash: Option<String> = None;
        let mut prev_inv_hash: Option<String> = None;
        if let (Some(rk), Some(rh)) =
            (head.rollup_key.clone(), head.rollup_hash.clone())
        {
            let cur_rec = rollup_load(self.store.as_ref(), &rk, &rh)
                .await
                .map_err(map_rollup)?;
            cur_inv_hash = Some(cur_rec.inventory_record_hash.clone());

            // Manifest GC requires a previous generation forming a valid NESTED PAIR
            // with the current one (the one-generation fallback), both verifying
            // against the retained entries, and the safety window elapsed.
            let eligible = match (
                head.prev_rollup_key.clone(),
                head.prev_rollup_hash.clone(),
            ) {
                (Some(pk), Some(ph)) => {
                    let prev_rec = rollup_load(self.store.as_ref(), &pk, &ph)
                        .await
                        .map_err(map_rollup)?;
                    prev_inv_hash =
                        Some(prev_rec.inventory_record_hash.clone());
                    if let Err(e) =
                        verify_rollup_pair(&cur_rec, &prev_rec, &head)
                    {
                        alarms.push(Alarm {
                            message: format!(
                                "rollup pair invalid; no manifest GC: {e}"
                            ),
                        });
                        false
                    } else {
                        let cur_ok = verify_cumulative_rollup(
                            self.store.as_ref(),
                            &self.pipeline,
                            &self.source_id,
                            &self.sink_id,
                            &head,
                            &rk,
                            &rh,
                            0,
                            &state.entries,
                        )
                        .await;
                        let prev_ok = verify_cumulative_rollup(
                            self.store.as_ref(),
                            &self.pipeline,
                            &self.source_id,
                            &self.sink_id,
                            &head,
                            &pk,
                            &ph,
                            0,
                            &state.entries,
                        )
                        .await;
                        let window_ok =
                            safety_window_ok(&head, now_ms, cfg, &mut alarms);
                        match (&cur_ok, &prev_ok, window_ok) {
                            (Ok(_), Ok(_), true) => {
                                horizon = horizon_seq(Some(prev_rec.end_seq));
                                true
                            }
                            (Err(e), _, _) => {
                                alarms.push(Alarm {
                                    message: format!(
                                        "current rollup unusable; no manifest \
                                         GC: {e}"
                                    ),
                                });
                                false
                            }
                            (_, Err(e), _) => {
                                alarms.push(Alarm {
                                    message: format!(
                                        "previous rollup unusable; no manifest \
                                         GC: {e}"
                                    ),
                                });
                                false
                            }
                            // Window not elapsed or skew (alarm already recorded).
                            _ => false,
                        }
                    }
                }
                // No previous generation: no horizon, nothing manifest-eligible.
                _ => false,
            };

            if eligible {
                for (seq, summ) in state.entries.iter() {
                    if *seq <= horizon {
                        let key = entry_key(
                            &self.prefix,
                            &self.pipeline,
                            *seq,
                            &summ.entry_hash,
                        )
                        .to_string();
                        manifest_entries_eligible.push(EntryRef {
                            key,
                            seq: *seq,
                            entry_hash: summ.entry_hash.clone(),
                        });
                    }
                }
            }
        }

        // ---- Data-side eligibility (from the SHARED verified compaction result) ----
        // The equivalence proof was bound into the HEAD-published record at
        // compaction; here we only read its authoritative result.
        for rec in &state.compactions.records {
            if rec.equivalence_result {
                for o in &rec.originals {
                    data_originals_eligible.push(OriginalRef {
                        key: o.key.clone(),
                        content_hash: o.content_hash.clone(),
                        replacement_key: rec.replacement.key.clone(),
                        compaction_record_hash: rec.record_hash.clone(),
                    });
                }
            } else {
                for o in &rec.originals {
                    skipped.push(SkipReason {
                        key: o.key.clone(),
                        reason: "compaction record not equivalence-proven"
                            .into(),
                    });
                }
                alarms.push(Alarm {
                    message: format!(
                        "compaction record {} is not equivalence-proven; its \
                         originals are not GC-eligible",
                        rec.record_hash
                    ),
                });
            }
        }

        let bound =
            HeadBinding::capture(&head, &etag, cur_inv_hash, prev_inv_hash);
        Ok(GcPlan {
            bound,
            horizon_seq: horizon,
            manifest_entries_eligible,
            inventory_indexes_eligible,
            data_originals_eligible,
            skipped,
            alarms,
        })
    }

    /// Read HEAD + ETag for a GC recheck.
    async fn read_head_etag(&self) -> Result<(Head, String), HeadError> {
        let hkey = head_key(&self.prefix, &self.pipeline);
        match self
            .store
            .get_with_etag(&hkey)
            .await
            .map_err(|e| HeadError::Store(e.to_string()))?
        {
            Some((raw, Some(etag))) => Ok((Head::parse(&raw)?, etag)),
            _ => Err(HeadError::Integrity(
                "HEAD missing/eTagless during GC".into(),
            )),
        }
    }

    /// Re-read HEAD immediately before a destructive unit of work and classify:
    /// a higher epoch fences us; a lower epoch is an integrity failure; a HEAD that
    /// no longer matches the plan binding is stale (stop); otherwise proceed against
    /// the freshly-read HEAD.
    /// Read live HEAD for a GC step and apply fencing: a higher epoch fences us; a
    /// lower epoch is an integrity failure; otherwise the live HEAD is returned. GC
    /// eligibility is MONOTONIC (compaction chains only grow, the horizon only
    /// advances, replacements are immutable), so an authorization proven earlier
    /// remains valid across an epoch bump - the durable authorization plus this live
    /// re-verification, not fencing alone, is what closes the recheck/delete race.
    async fn gc_live_head(&self, our_epoch: u64) -> Result<Head, HeadError> {
        let (head, _etag) = self.read_head_etag().await?;
        if head.epoch > our_epoch {
            return Err(HeadError::Fenced {
                our: our_epoch,
                observed: head.epoch,
            });
        }
        if head.epoch < our_epoch {
            return Err(HeadError::Integrity(format!(
                "HEAD epoch regressed during GC: {} < {}",
                head.epoch, our_epoch
            )));
        }
        Ok(head)
    }

    /// Re-verify (against live objects) that a manifest entry at `seq` is still covered
    /// by BOTH retained generations forming a valid nested pair with intact, bound
    /// inventories, and lies at/below the horizon. Any integrity problem is an error
    /// (the caller stops); `Ok(false)` means simply not eligible now.
    async fn revalidate_entry_coverage(
        &self,
        head: &Head,
        seq: u64,
    ) -> Result<bool, HeadError> {
        let (Some(rk), Some(rh)) =
            (head.rollup_key.clone(), head.rollup_hash.clone())
        else {
            return Ok(false);
        };
        let (Some(pk), Some(ph)) =
            (head.prev_rollup_key.clone(), head.prev_rollup_hash.clone())
        else {
            return Ok(false);
        };
        let cur = rollup_load(self.store.as_ref(), &rk, &rh)
            .await
            .map_err(map_rollup)?;
        let prev = rollup_load(self.store.as_ref(), &pk, &ph)
            .await
            .map_err(map_rollup)?;
        verify_rollup_pair(&cur, &prev, head)?;
        let cinv = rollup_load_inventory(
            self.store.as_ref(),
            &cur.inventory_key,
            &cur.inventory_record_hash,
        )
        .await
        .map_err(map_rollup)?;
        if cinv.start_seq != 1 || cinv.end_seq != cur.end_seq {
            return Err(HeadError::Integrity(
                "current inventory range mismatch during GC".into(),
            ));
        }
        let pinv = rollup_load_inventory(
            self.store.as_ref(),
            &prev.inventory_key,
            &prev.inventory_record_hash,
        )
        .await
        .map_err(map_rollup)?;
        if pinv.start_seq != 1 || pinv.end_seq != prev.end_seq {
            return Err(HeadError::Integrity(
                "previous inventory range mismatch during GC".into(),
            ));
        }
        Ok(seq <= prev.end_seq)
    }

    /// Recompute, from live authoritative state, the set of entry-expiry authorizations
    /// (marks) that are currently justified: both retained generations verify as a
    /// nested pair, the safety window has elapsed with a sane clock, and each entry is
    /// at/below the horizon. Callers never supply the eligible set - this is derived
    /// here, so a forged plan cannot widen it.
    async fn authoritative_marks(
        &self,
        head: &Head,
        now_ms: u64,
        cfg: GcConfig,
    ) -> Result<Vec<GcMark>, HeadError> {
        let (Some(rk), Some(rh)) =
            (head.rollup_key.clone(), head.rollup_hash.clone())
        else {
            return Ok(vec![]);
        };
        let (Some(pk), Some(ph)) =
            (head.prev_rollup_key.clone(), head.prev_rollup_hash.clone())
        else {
            return Ok(vec![]);
        };
        let state = verify_state(
            self.store.as_ref(),
            &self.pipeline,
            &self.source_id,
            &self.sink_id,
            head,
            self.comparator.as_ref(),
        )
        .await?;
        let cur = rollup_load(self.store.as_ref(), &rk, &rh)
            .await
            .map_err(map_rollup)?;
        let prev = rollup_load(self.store.as_ref(), &pk, &ph)
            .await
            .map_err(map_rollup)?;
        verify_rollup_pair(&cur, &prev, head)?;
        // Both generations must verify against the retained entries.
        verify_cumulative_rollup(
            self.store.as_ref(),
            &self.pipeline,
            &self.source_id,
            &self.sink_id,
            head,
            &rk,
            &rh,
            0,
            &state.entries,
        )
        .await?;
        verify_cumulative_rollup(
            self.store.as_ref(),
            &self.pipeline,
            &self.source_id,
            &self.sink_id,
            head,
            &pk,
            &ph,
            0,
            &state.entries,
        )
        .await?;
        let mut alarms = Vec::new();
        if !safety_window_ok(head, now_ms, cfg, &mut alarms) {
            return Ok(vec![]);
        }
        let horizon = horizon_seq(Some(prev.end_seq));
        let mut marks = Vec::new();
        for (seq, summ) in state.entries.iter() {
            if *seq <= horizon {
                let entry_key = entry_key(
                    &self.prefix,
                    &self.pipeline,
                    *seq,
                    &summ.entry_hash,
                )
                .to_string();
                marks.push(GcMark {
                    version: GC_MARK_VERSION,
                    pipeline: self.pipeline.clone(),
                    source_id: self.source_id.clone(),
                    sink_id: self.sink_id.clone(),
                    seq: *seq,
                    entry_key,
                    entry_hash: summ.entry_hash.clone(),
                });
            }
        }
        Ok(marks)
    }

    /// Recompute, from LIVE authoritative state, the compaction records whose
    /// originals are deletable: HEAD-reachable, fully verified (identity, domain,
    /// exact ack-inventory metadata, no conflicts), and carrying a bound
    /// `equivalence_result == true` proof. Because the proof rides in the record that
    /// was published through the HEAD CAS, a HEAD-reachable `true` record is
    /// authoritative provenance - a forged standalone authorization cannot exist.
    async fn authoritative_deletable_records(
        &self,
        head: &Head,
    ) -> Result<Vec<VerifiedCompaction>, HeadError> {
        let state = verify_state(
            self.store.as_ref(),
            &self.pipeline,
            &self.source_id,
            &self.sink_id,
            head,
            self.comparator.as_ref(),
        )
        .await?;
        Ok(state
            .compactions
            .records
            .into_iter()
            .filter(|r| r.equivalence_result)
            .collect())
    }

    /// GC actor A (manifest-side, step 1 of 2): AUTHORIZE entry expiry by writing
    /// immutable, self-authenticating [`GcMark`] records for every entry the LIVE
    /// authoritative state proves eligible. Owner-fenced; the eligible set is derived
    /// here (a caller cannot supply it); marks are create-only and, on `AlreadyExists`,
    /// re-read byte-for-byte (a non-identical existing mark halts). Marks nothing but
    /// manifest-entry authorizations; touches no rollup, inventory, or data object.
    pub async fn gc_mark_entries(
        &self,
        now_ms: u64,
        cfg: GcConfig,
    ) -> Result<GcRun, HeadError> {
        let mut st = self.state.lock().await;
        if let Some(observed) = st.fenced {
            return Err(HeadError::Fenced {
                our: st.verified.epoch,
                observed,
            });
        }
        let our_epoch = st.verified.epoch;
        let head = match self.gc_live_head(our_epoch).await {
            Ok(h) => h,
            Err(e) => {
                if let HeadError::Fenced { observed, .. } = &e {
                    st.fenced = Some(*observed);
                }
                return Err(e);
            }
        };
        let marks = self.authoritative_marks(&head, now_ms, cfg).await?;
        let mut run = GcRun::default();
        for mk in marks {
            let key = mark_key(&self.prefix, &self.pipeline, &mk.entry_hash);
            let body = bytes::Bytes::from(mk.canonical_bytes());
            match self
                .store
                .put_if_absent(&key, body.clone())
                .await
                .map_err(|e| HeadError::Store(e.to_string()))?
            {
                PutOutcome::Written { .. } => run.marked += 1,
                PutOutcome::AlreadyExists => {
                    match self
                        .store
                        .get_with_etag(&key)
                        .await
                        .map_err(|e| HeadError::Store(e.to_string()))?
                    {
                        Some((existing, _))
                            if existing.as_ref() == body.as_ref() =>
                        {
                            run.marked += 1
                        }
                        _ => {
                            run.stopped = Some(
                                "existing GC mark is not byte-identical \
                                 (tampered)"
                                    .into(),
                            );
                            break;
                        }
                    }
                }
                PutOutcome::Conflict => {
                    run.stopped =
                        Some("unexpected conflict writing GC mark".into());
                    break;
                }
            }
        }
        Ok(run)
    }

    /// GC actor A (manifest-side, step 2 of 2): EXPIRE (delete) manifest entries that
    /// carry a valid, self-authenticating mark and still pass live coverage
    /// revalidation. Consumes the DURABLE marks (not an in-memory plan), so it resumes
    /// after a crash. Owner-fenced; per entry it re-reads HEAD (fencing first), then
    /// re-reads, byte-verifies, and parses the mark, re-derives the canonical entry key
    /// internally (never a caller path), and re-verifies coverage against the live
    /// rollups. Deletes entries only; delete is idempotent.
    pub async fn gc_expire_entries(&self) -> Result<GcRun, HeadError> {
        let mut st = self.state.lock().await;
        if let Some(observed) = st.fenced {
            return Err(HeadError::Fenced {
                our: st.verified.epoch,
                observed,
            });
        }
        let our_epoch = st.verified.epoch;
        let keys = self
            .store
            .list(&marks_prefix(&self.prefix, &self.pipeline))
            .await
            .map_err(|e| HeadError::Store(e.to_string()))?;
        let mut run = GcRun::default();
        for mkey in keys {
            let head = match self.gc_live_head(our_epoch).await {
                Ok(h) => h,
                Err(e) => {
                    if let HeadError::Fenced { observed, .. } = &e {
                        st.fenced = Some(*observed);
                    }
                    return Err(e);
                }
            };
            let raw = match self
                .store
                .get_with_etag(&mkey)
                .await
                .map_err(|e| HeadError::Store(e.to_string()))?
            {
                Some((raw, _)) => raw,
                None => continue, // vanished concurrently; nothing to expire
            };
            let mk: GcMark = match serde_json::from_slice(&raw) {
                Ok(m) => m,
                Err(_) => {
                    run.stopped = Some(format!("malformed GC mark at {mkey}"));
                    break;
                }
            };
            // Immutable + self-authenticating: bytes must be canonical, version and
            // identity must match, and the entry key must be the canonical one.
            if mk.canonical_bytes() != raw.as_ref()
                || mk.version != GC_MARK_VERSION
                || mk.pipeline != self.pipeline
                || mk.source_id != self.source_id
                || mk.sink_id != self.sink_id
            {
                run.stopped =
                    Some(format!("GC mark at {mkey} failed verification"));
                break;
            }
            let canonical =
                entry_key(&self.prefix, &self.pipeline, mk.seq, &mk.entry_hash)
                    .to_string();
            if canonical != mk.entry_key {
                run.stopped = Some(format!(
                    "GC mark at {mkey} entry_key is not canonical"
                ));
                break;
            }
            match self.revalidate_entry_coverage(&head, mk.seq).await {
                Ok(true) => {
                    match self
                        .store
                        .delete(&object_store::path::Path::from(canonical))
                        .await
                    {
                        Ok(()) => run.deleted += 1,
                        Err(err) => {
                            run.stopped =
                                Some(format!("ambiguous delete: {err}"));
                            break;
                        }
                    }
                }
                Ok(false) => run.skipped.push(SkipReason {
                    key: mk.entry_key.clone(),
                    reason: "entry no longer covered/eligible".into(),
                }),
                Err(err) => {
                    run.stopped =
                        Some(format!("integrity during revalidation: {err}"));
                    break;
                }
            }
        }
        Ok(run)
    }

    /// GC actor B (data-side): DELETE compacted originals superseded by a HEAD-reachable,
    /// equivalence-proven compaction record. The authorization is the record itself,
    /// made authoritative by its HEAD CAS at publication - there is no separate
    /// authorization object to forge, and an unreferenced compaction record is a
    /// harmless orphan (never HEAD-reachable, so never consumed). Owner-fenced; consumes
    /// only durable HEAD-reachable records, so it resumes after a crash / re-acquisition.
    /// Per record it re-reads HEAD (fencing first) and re-verifies the surviving
    /// REPLACEMENT is durable + content-verified before deleting, so the last copy is
    /// never lost. Deletes originals only; delete is idempotent; stops on any
    /// stale/fenced/integrity/ambiguous condition.
    pub async fn gc_delete_originals(&self) -> Result<GcRun, HeadError> {
        let mut st = self.state.lock().await;
        if let Some(observed) = st.fenced {
            return Err(HeadError::Fenced {
                our: st.verified.epoch,
                observed,
            });
        }
        let our_epoch = st.verified.epoch;
        let head0 = match self.gc_live_head(our_epoch).await {
            Ok(h) => h,
            Err(e) => {
                if let HeadError::Fenced { observed, .. } = &e {
                    st.fenced = Some(*observed);
                }
                return Err(e);
            }
        };
        let records = self.authoritative_deletable_records(&head0).await?;
        let mut run = GcRun::default();
        for rec in &records {
            // Per-unit fencing: a higher epoch that appeared mid-run stops us. GC
            // eligibility is monotonic, so a record deletable at head0 stays deletable
            // at the same epoch; a bump fences.
            match self.gc_live_head(our_epoch).await {
                Ok(_) => {}
                Err(e) => {
                    if let HeadError::Fenced { observed, .. } = &e {
                        st.fenced = Some(*observed);
                    }
                    return Err(e);
                }
            }
            // The surviving replacement must be durable + content-verified before any
            // original is removed (never remove the last durable copy).
            if let Err(err) =
                verify_data_object(self.store.as_ref(), &rec.replacement).await
            {
                run.stopped =
                    Some(format!("replacement not durable, refusing: {err}"));
                break;
            }
            let mut ambiguous = None;
            for o in &rec.originals {
                match self
                    .store
                    .delete(&object_store::path::Path::from(o.key.clone()))
                    .await
                {
                    Ok(()) => run.deleted += 1,
                    Err(err) => {
                        ambiguous = Some(format!("ambiguous delete: {err}"));
                        break;
                    }
                }
            }
            if let Some(reason) = ambiguous {
                run.stopped = Some(reason);
                break;
            }
        }
        Ok(run)
    }
}

/// The safety window measures how long the CURRENT rollup generation has been
/// HEAD-published (`rollup_published_at_ms`). Fail conservatively on a missing,
/// future, or rolled-back timestamp (alarm + not-ok); otherwise require the age to
/// meet `cfg.safety_window_ms`.
fn safety_window_ok(
    head: &Head,
    now_ms: u64,
    cfg: GcConfig,
    alarms: &mut Vec<Alarm>,
) -> bool {
    match head.rollup_published_at_ms {
        None => {
            alarms.push(Alarm {
                message: "rollup ref present without a publication time; no \
                          manifest GC"
                    .into(),
            });
            false
        }
        Some(pub_at) => {
            if now_ms < pub_at {
                alarms.push(Alarm {
                    message:
                        "future rollup publication timestamp; no manifest GC"
                            .into(),
                });
                return false;
            }
            if let Some(prev_pub) = head.prev_rollup_published_at_ms {
                if prev_pub > pub_at {
                    alarms.push(Alarm {
                        message:
                            "rollup publication time rollback; no manifest GC"
                                .into(),
                    });
                    return false;
                }
            }
            now_ms - pub_at >= cfg.safety_window_ms
        }
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

fn map_rollup(e: RollupError) -> HeadError {
    match e {
        RollupError::Integrity(_) => HeadError::Integrity(e.to_string()),
        RollupError::Store(s) => HeadError::Store(s),
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
            domain: EncodingDomain::new("jsonl", 1, "s1", "none", "table", 1),
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
            prev_rollup_key: None,
            prev_rollup_hash: None,
            rollup_published_at_ms: None,
            prev_rollup_published_at_ms: None,
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
    async fn recovery_falls_back_when_current_rollup_damaged() {
        // A HEAD referencing a rollup we cannot validate is an alarm, not a
        // failure: rollups are advisory in 9A, so recovery falls back to the
        // retained entry chain (ground truth) and succeeds.
        let (inner, cs) = build_chain().await;
        let mut head = head_of(&cs).await;
        head.rollup_key = Some("pfx/pipe/_manifest/rollups/x.json".into());
        head.rollup_hash = Some("deadbeef".into());
        head.rollup_published_at_ms = Some(1);
        inner
            .put(
                &head_key("pfx", "pipe"),
                object_store::PutPayload::from(head.canonical_bytes()),
            )
            .await
            .unwrap();
        recover(cs)
            .await
            .expect("recovery falls back to entry chain");
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
            prev_rollup_key: None,
            prev_rollup_hash: None,
            rollup_published_at_ms: None,
            prev_rollup_published_at_ms: None,
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

    // ── Cumulative rollup + inventory verification (verify_cumulative_rollup) ──
    //
    // These exercise the single-generation detection logic directly against a
    // synthetic entries map, a written inventory index, and a rollup record that
    // binds it, so each rejection path (cumulative start, digest, inventory range,
    // count, boundary hash/watermark vs HEAD or tail prev, and the exact
    // inventory-equals-reconstructed proof) is asserted in isolation.

    use super::super::rollup::{
        INVENTORY_INDEX_VERSION, InventoryIndex, ROLLUP_RECORD_VERSION,
        RollupRecord as RR,
    };
    use std::collections::BTreeMap;

    fn mobj(key: &str) -> ManifestObject {
        ManifestObject {
            key: key.to_string(),
            table: "orders".into(),
            content_hash: format!("h-{key}"),
            byte_len: 1,
            format: "jsonl".into(),
            format_version: 1,
            schema_id: "s1".into(),
            compression: "none".into(),
            partition_spec: "table".into(),
            partition_version: 1,
        }
    }

    /// `n` retained, chained entries seq 1..=n, one object each. Each entry's
    /// `prev_hash` links to the entry before it (genesis has None).
    fn synth_entries(n: u64) -> BTreeMap<u64, EntrySummary> {
        let mut m = BTreeMap::new();
        for seq in 1u64..=n {
            m.insert(
                seq,
                EntrySummary {
                    entry_hash: format!("eh-{seq}"),
                    prev_hash: (seq > 1).then(|| format!("eh-{}", seq - 1)),
                    watermark_hex: wm_hex(0, seq),
                    objects: vec![mobj(&format!("o{seq}"))],
                },
            );
        }
        m
    }

    /// A HEAD whose selected entry is `entries[seq]`.
    fn head_at(entries: &BTreeMap<u64, EntrySummary>, seq: u64) -> Head {
        Head {
            version: HEAD_VERSION,
            epoch: 1,
            seq,
            head_entry_key: Some(format!("k-{seq}")),
            head_entry_hash: Some(entries[&seq].entry_hash.clone()),
            rollup_key: None,
            rollup_hash: None,
            prev_rollup_key: None,
            prev_rollup_hash: None,
            rollup_published_at_ms: None,
            prev_rollup_published_at_ms: None,
            watermark_hex: Some(entries[&seq].watermark_hex.clone()),
            compaction_key: None,
            compaction_hash: None,
        }
    }

    /// Objects for the cumulative range [1, end], in canonical order.
    fn range_objects(
        entries: &BTreeMap<u64, EntrySummary>,
        end: u64,
    ) -> Vec<ManifestObject> {
        let mut v = Vec::new();
        for s in 1..=end {
            v.extend(entries[&s].objects.iter().cloned());
        }
        v
    }

    /// Write an inventory index for `[1, end]` holding `inv_objects`, returning its
    /// bound (key, record_hash, count).
    async fn put_inv(
        cs: &ObjectStoreConditional,
        end: u64,
        inv_objects: Vec<ManifestObject>,
    ) -> (String, String, u64) {
        let inv = InventoryIndex {
            version: INVENTORY_INDEX_VERSION,
            pipeline: "pipe".into(),
            source_id: "src".into(),
            sink_id: "sink".into(),
            start_seq: 1,
            end_seq: end,
            objects: inv_objects,
        };
        let w = rollup_write_inventory(cs, "pfx", &inv).await.unwrap();
        (w.key, w.record_hash, w.count)
    }

    /// Build a cumulative rollup record over `[1, end]` binding `(inv_key,
    /// inv_hash, inv_count)`, with digest/count taken from `digest_objects`.
    #[allow(clippy::too_many_arguments)]
    fn cumulative_record(
        entries: &BTreeMap<u64, EntrySummary>,
        end: u64,
        inv_key: String,
        inv_hash: String,
        inv_count: u64,
        digest_objects: &[ManifestObject],
    ) -> RR {
        RR {
            version: ROLLUP_RECORD_VERSION,
            pipeline: "pipe".into(),
            source_id: "src".into(),
            sink_id: "sink".into(),
            start_seq: 1,
            end_seq: end,
            start_entry_hash: entries[&1].entry_hash.clone(),
            end_entry_hash: entries[&end].entry_hash.clone(),
            watermark_hex: entries[&end].watermark_hex.clone(),
            object_count: digest_objects.len() as u64,
            object_digest: rollup_object_digest(digest_objects),
            inventory_key: inv_key,
            inventory_record_hash: inv_hash,
            inventory_count: inv_count,
            prev: None,
        }
    }

    async fn verify(
        cs: &ObjectStoreConditional,
        head: &Head,
        rec: &RR,
        min_covered_end: u64,
        entries: &BTreeMap<u64, EntrySummary>,
    ) -> Result<Vec<ManifestObject>, HeadError> {
        let w = rollup_write(cs, "pfx", rec).await.unwrap();
        verify_cumulative_rollup(
            cs,
            "pipe",
            "src",
            "sink",
            head,
            &w.key,
            &w.hash,
            min_covered_end,
            entries,
        )
        .await
    }

    #[tokio::test]
    async fn cumulative_rollup_verifies_at_head() {
        let (_i, cs) = cond_with_inner();
        let e = synth_entries(3);
        let head = head_at(&e, 3);
        let objs = range_objects(&e, 3);
        let (ik, ih, ic) = put_inv(&cs, 3, objs.clone()).await;
        let rec = cumulative_record(&e, 3, ik, ih, ic, &objs);
        let got = verify(&cs, &head, &rec, 0, &e)
            .await
            .expect("valid cumulative rollup verifies");
        assert_eq!(got, objs, "returns the covered inventory");
    }

    #[tokio::test]
    async fn cumulative_rollup_rejects_non_genesis_start() {
        let (_i, cs) = cond_with_inner();
        let e = synth_entries(3);
        let head = head_at(&e, 3);
        let objs = range_objects(&e, 3);
        let (ik, ih, ic) = put_inv(&cs, 3, objs.clone()).await;
        let mut rec = cumulative_record(&e, 3, ik, ih, ic, &objs);
        rec.start_seq = 2; // not cumulative
        let err = verify(&cs, &head, &rec, 0, &e).await.unwrap_err();
        assert!(
            matches!(err, HeadError::Integrity(ref m) if m.contains("cumulative")),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn cumulative_rollup_rejects_wrong_object_digest() {
        let (_i, cs) = cond_with_inner();
        let e = synth_entries(3);
        let head = head_at(&e, 3);
        let objs = range_objects(&e, 3);
        let (ik, ih, ic) = put_inv(&cs, 3, objs.clone()).await;
        let mut rec = cumulative_record(&e, 3, ik, ih, ic, &objs);
        rec.object_digest = "deadbeef".into();
        assert!(verify(&cs, &head, &rec, 0, &e).await.is_err());
    }

    #[tokio::test]
    async fn cumulative_rollup_rejects_inventory_range_mismatch() {
        let (_i, cs) = cond_with_inner();
        let e = synth_entries(3);
        let head = head_at(&e, 3);
        let objs = range_objects(&e, 3);
        // Inventory declares end 2 but the rollup declares end 3.
        let (ik, ih, ic) = put_inv(&cs, 2, objs.clone()).await;
        let rec = cumulative_record(&e, 3, ik, ih, ic, &objs);
        let err = verify(&cs, &head, &rec, 0, &e).await.unwrap_err();
        assert!(
            matches!(err, HeadError::Integrity(ref m) if m.contains("range")),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn cumulative_rollup_rejects_end_hash_vs_head() {
        let (_i, cs) = cond_with_inner();
        let e = synth_entries(3);
        let head = head_at(&e, 3);
        let objs = range_objects(&e, 3);
        let (ik, ih, ic) = put_inv(&cs, 3, objs.clone()).await;
        let mut rec = cumulative_record(&e, 3, ik, ih, ic, &objs);
        rec.end_entry_hash = "eh-wrong".into();
        let err = verify(&cs, &head, &rec, 0, &e).await.unwrap_err();
        assert!(
            matches!(err, HeadError::Integrity(ref m) if m.contains("HEAD")),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn cumulative_rollup_rejects_watermark_vs_head() {
        let (_i, cs) = cond_with_inner();
        let e = synth_entries(3);
        let head = head_at(&e, 3);
        let objs = range_objects(&e, 3);
        let (ik, ih, ic) = put_inv(&cs, 3, objs.clone()).await;
        let mut rec = cumulative_record(&e, 3, ik, ih, ic, &objs);
        rec.watermark_hex = wm_hex(0, 99);
        assert!(verify(&cs, &head, &rec, 0, &e).await.is_err());
    }

    #[tokio::test]
    async fn cumulative_rollup_rejects_inventory_not_equal_reconstructed() {
        let (_i, cs) = cond_with_inner();
        let e = synth_entries(3);
        let head = head_at(&e, 3);
        // Tamper the inventory (swap an object), but make the rollup's digest match
        // the tampered inventory so the digest check passes and the equality proof
        // (inventory vs reconstructed-from-entries) is what fails.
        let mut tampered = range_objects(&e, 3);
        tampered[1] = mobj("intruder");
        let (ik, ih, ic) = put_inv(&cs, 3, tampered.clone()).await;
        let rec = cumulative_record(&e, 3, ik, ih, ic, &tampered);
        let err = verify(&cs, &head, &rec, 0, &e).await.unwrap_err();
        assert!(
            matches!(err, HeadError::Integrity(ref m) if m.contains("reconstructed")),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn cumulative_rollup_ends_below_head_verifies_via_tail_prev() {
        // Rollup covers [1, 2]; HEAD is at 3; the boundary is verified against the
        // first retained tail entry (seq 3)'s prev_hash == rollup end hash.
        let (_i, cs) = cond_with_inner();
        let e = synth_entries(3);
        let head = head_at(&e, 3);
        let objs = range_objects(&e, 2);
        let (ik, ih, ic) = put_inv(&cs, 2, objs.clone()).await;
        let rec = cumulative_record(&e, 2, ik, ih, ic, &objs);
        verify(&cs, &head, &rec, 2, &e)
            .await
            .expect("verifies via retained tail prev");
    }

    #[tokio::test]
    async fn cumulative_rollup_rejects_tail_prev_mismatch() {
        let (_i, cs) = cond_with_inner();
        let mut e = synth_entries(3);
        let head = head_at(&e, 3);
        let objs = range_objects(&e, 2);
        let (ik, ih, ic) = put_inv(&cs, 2, objs.clone()).await;
        let rec = cumulative_record(&e, 2, ik, ih, ic, &objs);
        // Corrupt the tail entry's prev link so it no longer abuts the rollup.
        e.get_mut(&3).unwrap().prev_hash = Some("eh-bogus".into());
        let err = verify(&cs, &head, &rec, 2, &e).await.unwrap_err();
        assert!(
            matches!(err, HeadError::Integrity(ref m) if m.contains("tail")),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn cumulative_rollup_rejects_min_covered_not_met() {
        // The rollup only reaches seq 2 but recovery needs coverage down to 3.
        let (_i, cs) = cond_with_inner();
        let e = synth_entries(3);
        let head = head_at(&e, 3);
        let objs = range_objects(&e, 2);
        let (ik, ih, ic) = put_inv(&cs, 2, objs.clone()).await;
        let rec = cumulative_record(&e, 2, ik, ih, ic, &objs);
        let err = verify(&cs, &head, &rec, 3, &e).await.unwrap_err();
        assert!(
            matches!(err, HeadError::Integrity(ref m) if m.contains("cover")),
            "got {err:?}"
        );
    }

    // ── Nested-pair verifier (verify_rollup_pair) ───────────────────────────

    /// A minimal rollup record for pair tests (only start/end/prev matter).
    fn rr(start: u64, end: u64, prev: Option<PrevRef>) -> RR {
        RR {
            version: ROLLUP_RECORD_VERSION,
            pipeline: "pipe".into(),
            source_id: "src".into(),
            sink_id: "sink".into(),
            start_seq: start,
            end_seq: end,
            start_entry_hash: "eh-1".into(),
            end_entry_hash: format!("eh-{end}"),
            watermark_hex: wm_hex(0, end),
            object_count: 0,
            object_digest: "d".into(),
            inventory_key: "ik".into(),
            inventory_record_hash: "ih".into(),
            inventory_count: 0,
            prev,
        }
    }

    /// A HEAD whose current rollup is `rk/rh` and previous is `pk/ph`, both with
    /// publication times set.
    fn pair_head() -> Head {
        Head {
            version: HEAD_VERSION,
            epoch: 1,
            seq: 9,
            head_entry_key: Some("k".into()),
            head_entry_hash: Some("eh-9".into()),
            rollup_key: Some("rk".into()),
            rollup_hash: Some("rh".into()),
            prev_rollup_key: Some("pk".into()),
            prev_rollup_hash: Some("ph".into()),
            rollup_published_at_ms: Some(200),
            prev_rollup_published_at_ms: Some(100),
            watermark_hex: Some(wm_hex(0, 9)),
            compaction_key: None,
            compaction_hash: None,
        }
    }

    fn prev_ref() -> PrevRef {
        PrevRef {
            key: "pk".into(),
            hash: "ph".into(),
        }
    }

    #[test]
    fn rollup_pair_valid_nested() {
        let cur = rr(1, 3, Some(prev_ref()));
        let prev = rr(1, 2, None);
        verify_rollup_pair(&cur, &prev, &pair_head())
            .expect("a valid nested pair verifies");
    }

    #[test]
    fn rollup_pair_rejects_prev_ref_mismatch() {
        // current.prev points somewhere other than HEAD's retained previous.
        let cur = rr(
            1,
            3,
            Some(PrevRef {
                key: "other".into(),
                hash: "x".into(),
            }),
        );
        let prev = rr(1, 2, None);
        let err = verify_rollup_pair(&cur, &prev, &pair_head()).unwrap_err();
        assert!(
            matches!(err, HeadError::Integrity(ref m) if m.contains("prev")),
            "got {err:?}"
        );
    }

    #[test]
    fn rollup_pair_rejects_non_cumulative() {
        let cur = rr(2, 3, Some(prev_ref())); // start_seq != 1
        let prev = rr(1, 2, None);
        let err = verify_rollup_pair(&cur, &prev, &pair_head()).unwrap_err();
        assert!(
            matches!(err, HeadError::Integrity(ref m) if m.contains("cumulative")),
            "got {err:?}"
        );
    }

    #[test]
    fn rollup_pair_rejects_non_increasing_generations() {
        let cur = rr(1, 2, Some(prev_ref()));
        let prev = rr(1, 2, None); // prev.end >= cur.end
        let err = verify_rollup_pair(&cur, &prev, &pair_head()).unwrap_err();
        assert!(
            matches!(err, HeadError::Integrity(ref m) if m.contains("before")),
            "got {err:?}"
        );
    }

    #[test]
    fn rollup_pair_rejects_missing_publication_time() {
        let cur = rr(1, 3, Some(prev_ref()));
        let prev = rr(1, 2, None);
        let mut head = pair_head();
        head.prev_rollup_published_at_ms = None;
        let err = verify_rollup_pair(&cur, &prev, &head).unwrap_err();
        assert!(
            matches!(err, HeadError::Integrity(ref m) if m.contains("publication")),
            "got {err:?}"
        );
    }
}
