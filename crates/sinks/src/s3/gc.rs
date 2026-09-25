//! Garbage-collection eligibility model + dry-run plan for the durable S3 path
//! (P0.4, 9B.1). NON-DESTRUCTIVE: this module computes what WOULD be deleted; it
//! never deletes, tags, or marks anything. The lifecycle/deletion actor is 9B.2.
//!
//! Two independent deletion domains (a rollup never makes a data object eligible):
//! - MANIFEST-SIDE: manifest entries covered by a verified cumulative rollup, and
//!   out-of-horizon inventory indexes, governed by the shared [`horizon_seq`].
//! - DATA-SIDE: originals superseded by a verified, HEAD-reachable, equivalence-
//!   proven compaction record.
//!
//! See `docs/specs/s3-durable-acks-9b1-design.md` sections 2, 5, 6, 7.

#![allow(dead_code)]

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::head::Head;

/// Retained rollup generations behind HEAD. Ships as 1 (current + previous
/// fallback); structurally configurable but only the one-generation case is
/// implemented and tested in 9B.
pub const RETAINED_ROLLUP_GENERATIONS: u64 = 1;

/// The single shared fallback horizon: the highest manifest sequence whose entry
/// may become GC-eligible. It is the PREVIOUS rollup generation's end sequence, so
/// every deleted entry is independently covered by BOTH the current and the
/// retained previous cumulative snapshot. `None` (no previous generation) => 0
/// (retain everything). Both retention and recovery derive their floor from this
/// one function so they cannot drift.
pub fn horizon_seq(prev_rollup_end: Option<u64>) -> u64 {
    prev_rollup_end.unwrap_or(0)
}

/// Configuration for GC planning.
#[derive(Debug, Clone, Copy)]
pub struct GcConfig {
    /// The current rollup generation must have been HEAD-published at least this
    /// long ago (authoritative age) before covered entries are eligible.
    pub safety_window_ms: u64,
}

/// The exact HEAD identity a dry-run plan was computed against. If any field
/// differs at execution time the whole plan is discarded and recomputed, so a
/// valid-but-stale plan can never become destructive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadBinding {
    pub head_etag: String,
    pub epoch: u64,
    pub head_entry_hash: Option<String>,
    pub rollup_hash: Option<String>,
    pub prev_rollup_hash: Option<String>,
    /// Current + previous rollups' inventory-index identities (from the loaded
    /// rollup records). Transitively covered by the rollup hashes too, bound
    /// explicitly for auditability.
    pub inventory_record_hash: Option<String>,
    pub prev_inventory_record_hash: Option<String>,
    pub rollup_published_at_ms: Option<u64>,
    pub compaction_hash: Option<String>,
}

impl HeadBinding {
    pub fn capture(
        head: &Head,
        head_etag: &str,
        inventory_record_hash: Option<String>,
        prev_inventory_record_hash: Option<String>,
    ) -> Self {
        Self {
            head_etag: head_etag.to_string(),
            epoch: head.epoch,
            head_entry_hash: head.head_entry_hash.clone(),
            rollup_hash: head.rollup_hash.clone(),
            prev_rollup_hash: head.prev_rollup_hash.clone(),
            inventory_record_hash,
            prev_inventory_record_hash,
            rollup_published_at_ms: head.rollup_published_at_ms,
            compaction_hash: head.compaction_hash.clone(),
        }
    }

    /// True only if the live HEAD still matches this binding exactly. A changed
    /// ETag alone (any HEAD write) invalidates; the field checks are defensive.
    pub fn still_valid(&self, head: &Head, head_etag: &str) -> bool {
        self.head_etag == head_etag
            && self.epoch == head.epoch
            && self.head_entry_hash == head.head_entry_hash
            && self.rollup_hash == head.rollup_hash
            && self.prev_rollup_hash == head.prev_rollup_hash
            && self.rollup_published_at_ms == head.rollup_published_at_ms
            && self.compaction_hash == head.compaction_hash
    }
}

/// A manifest entry that would be deleted (manifest-side GC).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryRef {
    pub key: String,
    pub seq: u64,
    pub entry_hash: String,
}

/// An out-of-horizon inventory index that would be deleted (manifest-side GC).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InventoryRef {
    pub key: String,
    pub record_hash: String,
}

/// A data original that would be deleted (data-side GC), with the replacement that
/// supersedes it and the compaction record proving it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OriginalRef {
    pub key: String,
    pub content_hash: String,
    pub replacement_key: String,
    pub compaction_record_hash: String,
}

/// A candidate that was considered but excluded, with why (auditable).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkipReason {
    pub key: String,
    pub reason: String,
}

/// A non-fatal alarm surfaced while planning. Fatal conditions return `Err`
/// instead of a plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Alarm {
    pub message: String,
}

/// An ADVISORY, read-only preview of what GC would collect, bound to the exact HEAD
/// it was computed against. It is NOT an authorization: the destructive actors never
/// consume it - they re-derive the authoritative eligible set from live state and act
/// only on durable, self-authenticating authorizations. Fields are crate-private so
/// callers read them through accessors and cannot forge an eligible set the actors
/// would trust.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcPlan {
    pub(crate) bound: HeadBinding,
    pub(crate) horizon_seq: u64,
    pub(crate) manifest_entries_eligible: Vec<EntryRef>,
    pub(crate) inventory_indexes_eligible: Vec<InventoryRef>,
    pub(crate) data_originals_eligible: Vec<OriginalRef>,
    pub(crate) skipped: Vec<SkipReason>,
    pub(crate) alarms: Vec<Alarm>,
}

impl GcPlan {
    /// True only against the exact HEAD identity captured at plan time.
    pub fn still_valid(&self, head: &Head, head_etag: &str) -> bool {
        self.bound.still_valid(head, head_etag)
    }
    pub fn horizon_seq(&self) -> u64 {
        self.horizon_seq
    }
    pub fn manifest_entries_eligible(&self) -> &[EntryRef] {
        &self.manifest_entries_eligible
    }
    pub fn inventory_indexes_eligible(&self) -> &[InventoryRef] {
        &self.inventory_indexes_eligible
    }
    pub fn data_originals_eligible(&self) -> &[OriginalRef] {
        &self.data_originals_eligible
    }
    pub fn skipped(&self) -> &[SkipReason] {
        &self.skipped
    }
    pub fn alarms(&self) -> &[Alarm] {
        &self.alarms
    }
}

/// The outcome of one destructive GC actor run (9B.2). Actors stop at the first
/// unsafe condition (`stopped` set) and never continue past it.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct GcRun {
    /// Entries marked GC-eligible (lifecycle marking).
    pub marked: usize,
    /// Objects physically removed (or already absent under a proving plan).
    pub deleted: usize,
    /// Targets already absent (idempotent success under the same valid plan).
    pub already_absent: usize,
    /// Targets considered but not acted on, with why.
    pub skipped: Vec<SkipReason>,
    /// Set when the actor stopped early (stale plan, epoch/ETag change, missing
    /// state, integrity failure, ambiguous provider response). Deletion never
    /// proceeds past this.
    pub stopped: Option<String>,
}

pub const GC_MARK_VERSION: u16 = 1;

const MARK_DOMAIN: &[u8] = b"deltaforge/s3/gc-entry-mark/v1";

fn hash_domain(domain: &[u8], canonical: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(domain);
    h.update((canonical.len() as u64).to_be_bytes());
    h.update(canonical);
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

/// An IMMUTABLE, self-authenticating authorization to expire ONE manifest entry
/// (9B.2, manifest-side). It records the canonical entry identity plus the exact
/// rollup + inventory proof references (both retained generations) and the horizon
/// under which the entry was proven covered. `auth_hash` is a domain-separated hash
/// over the complete canonical contents; the expiry actor re-reads, re-parses, and
/// re-verifies it, and derives the canonical entry key itself - it never trusts a
/// caller-supplied path.
///
/// Data-GC needs no analogous object: the equivalence proof is bound into the
/// compaction record and made authoritative by that record's HEAD CAS, so a
/// HEAD-reachable compaction record is itself the durable data-deletion authorization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcMark {
    pub version: u16,
    pub pipeline: String,
    pub source_id: String,
    pub sink_id: String,
    pub seq: u64,
    /// The canonical manifest-entry key, derived internally at authorization from
    /// (prefix, pipeline, seq, entry_hash). Re-derived + checked at expiry. Coverage
    /// is re-proven at expiry against the LIVE rollups (monotonic), so the mark records
    /// only the entry identity, not transient rollup/inventory references.
    pub entry_key: String,
    pub entry_hash: String,
}

impl GcMark {
    pub fn canonical_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("gc mark serializes")
    }
    pub fn auth_hash(&self) -> String {
        hash_domain(MARK_DOMAIN, &self.canonical_bytes())
    }
}

/// Key for an entry expiry authorization: `.../_manifest/gc/marks/<entry_hash>.json`
/// (deterministic per entry, so expiry can find it).
pub fn mark_key(
    prefix: &str,
    pipeline: &str,
    entry_hash: &str,
) -> object_store::path::Path {
    object_store::path::Path::from_iter(
        prefix
            .split('/')
            .filter(|p| !p.is_empty())
            .chain([pipeline, "_manifest", "gc", "marks"])
            .map(str::to_string)
            .chain([format!("{entry_hash}.json")]),
    )
}

/// Directory prefix for listing durable entry-expiry marks during execution.
pub fn marks_prefix(prefix: &str, pipeline: &str) -> object_store::path::Path {
    object_store::path::Path::from_iter(
        prefix
            .split('/')
            .filter(|p| !p.is_empty())
            .chain([pipeline, "_manifest", "gc", "marks"])
            .map(str::to_string),
    )
}

/// Parse the manifest sequence from an entry key of the form
/// `.../_manifest/entries/{seq:020}-{hash}.json`. Returns `None` if the shape is
/// not recognized.
pub fn entry_seq_from_key(key: &str) -> Option<u64> {
    let file = key.rsplit('/').next()?;
    let digits = file.split('-').next()?;
    digits.parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn horizon_is_prev_rollup_end_or_zero() {
        assert_eq!(horizon_seq(None), 0);
        assert_eq!(horizon_seq(Some(0)), 0);
        assert_eq!(horizon_seq(Some(42)), 42);
    }

    #[test]
    fn entry_seq_parses_zero_padded_key() {
        let k = "pfx/pipe/_manifest/entries/00000000000000000007-abc.json";
        assert_eq!(entry_seq_from_key(k), Some(7));
        assert_eq!(entry_seq_from_key("nope"), None);
    }
}
