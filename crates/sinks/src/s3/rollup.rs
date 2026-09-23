//! Manifest rollups for the durable S3 path (P0.4, 9A).
//!
//! A rollup summarizes one CONTIGUOUS manifest sequence range `[start_seq,
//! end_seq]` of the already-verified acknowledgement chain: the boundary entry
//! hashes, the end watermark, and a digest of the authoritative object inventory
//! in that range. Rollups form an immutable, hash-linked chain (via `prev`) that
//! lets recovery skip re-walking summarized entries.
//!
//! Rollups are advisory in 9A: manifest entries are retained, so the entry chain
//! remains the authoritative ground truth. A corrupt/missing rollup is an alarm
//! and a fall back to the previous rollup or a full entry walk - never silently
//! trusted. Rollups NEVER touch the source watermark, seq, entry or compaction
//! references; publishing one only sets HEAD's rollup + prev-rollup references.

#![allow(dead_code)]

use object_store::path::Path;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::manifest::{ManifestObject, PrevRef};
use super::store_cond::{ConditionalStore, PutOutcome};

pub const ROLLUP_RECORD_VERSION: u16 = 1;
const ROLLUP_HASH_DOMAIN: &[u8] = b"deltaforge/s3/rollup-record/v1";
const ROLLUP_DIGEST_DOMAIN: &[u8] = b"deltaforge/s3/rollup-object-digest/v1";

/// An immutable summary of a contiguous manifest range.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RollupRecord {
    pub version: u16,
    pub pipeline: String,
    pub source_id: String,
    pub sink_id: String,
    /// Inclusive sequence range summarized.
    pub start_seq: u64,
    pub end_seq: u64,
    /// Hash of the entry at `start_seq` and at `end_seq` (boundary linkage).
    pub start_entry_hash: String,
    pub end_entry_hash: String,
    /// The end entry's watermark (hex).
    pub watermark_hex: String,
    /// Number of authoritative data objects summarized, and their digest.
    pub object_count: u64,
    pub object_digest: String,
    /// Previous rollup in the chain (None = first).
    pub prev: Option<PrevRef>,
}

impl RollupRecord {
    pub fn canonical_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("rollup record serializes")
    }
    pub fn record_hash(&self) -> String {
        hash_domain(ROLLUP_HASH_DOMAIN, &self.canonical_bytes())
    }
}

/// Stable digest over an object inventory: sort by (key, content_hash), then
/// domain-separated hash of the length-prefixed pairs. Order-independent.
pub fn object_digest(objects: &[ManifestObject]) -> String {
    let mut pairs: Vec<(&str, &str)> = objects
        .iter()
        .map(|o| (o.key.as_str(), o.content_hash.as_str()))
        .collect();
    pairs.sort_unstable();
    let mut buf = Vec::new();
    for (k, h) in pairs {
        buf.extend_from_slice(&(k.len() as u64).to_be_bytes());
        buf.extend_from_slice(k.as_bytes());
        buf.extend_from_slice(&(h.len() as u64).to_be_bytes());
        buf.extend_from_slice(h.as_bytes());
    }
    hash_domain(ROLLUP_DIGEST_DOMAIN, &buf)
}

fn hash_domain(domain: &[u8], canonical: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(domain);
    h.update((canonical.len() as u64).to_be_bytes());
    h.update(canonical);
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

pub fn record_key(prefix: &str, pipeline: &str, hash: &str) -> Path {
    Path::from_iter(
        prefix
            .split('/')
            .filter(|p| !p.is_empty())
            .chain([pipeline, "_manifest", "rollups"])
            .map(str::to_string)
            .chain([format!("{hash}.json")]),
    )
}

#[derive(Debug, thiserror::Error)]
pub enum RollupError {
    #[error("object store: {0}")]
    Store(String),
    #[error("integrity: {0}")]
    Integrity(String),
}

impl From<super::store_cond::CondError> for RollupError {
    fn from(e: super::store_cond::CondError) -> Self {
        RollupError::Store(e.to_string())
    }
}

/// A durably-written rollup record, for the HEAD layer to reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WrittenRollup {
    pub key: String,
    pub hash: String,
}

/// Write the rollup record create-only; idempotent on byte-identical retry.
pub async fn write_record<S: ConditionalStore + ?Sized>(
    store: &S,
    prefix: &str,
    record: &RollupRecord,
) -> Result<WrittenRollup, RollupError> {
    let canonical = record.canonical_bytes();
    let hash = hash_domain(ROLLUP_HASH_DOMAIN, &canonical);
    let key = record_key(prefix, &record.pipeline, &hash);
    match store
        .put_if_absent(&key, bytes::Bytes::from(canonical.clone()))
        .await?
    {
        PutOutcome::Written { .. } => {}
        PutOutcome::AlreadyExists => match store.get_with_etag(&key).await? {
            Some((existing, _))
                if existing.as_ref() == canonical.as_slice() => {}
            Some(_) => {
                return Err(RollupError::Integrity(format!(
                    "rollup record at {key} does not match"
                )));
            }
            None => {
                return Err(RollupError::Store(format!(
                    "rollup record at {key} vanished after AlreadyExists"
                )));
            }
        },
        PutOutcome::Conflict => {
            return Err(RollupError::Store(format!(
                "unexpected CAS conflict on create-only rollup at {key}"
            )));
        }
    }
    Ok(WrittenRollup {
        key: key.to_string(),
        hash,
    })
}

/// Load and parse a rollup record, verifying its bytes match `expected_hash`.
pub async fn load_record<S: ConditionalStore + ?Sized>(
    store: &S,
    key: &str,
    expected_hash: &str,
) -> Result<RollupRecord, RollupError> {
    let (raw, _) = store
        .get_with_etag(&Path::from(key.to_string()))
        .await?
        .ok_or_else(|| {
            RollupError::Integrity(format!("rollup record {key} missing"))
        })?;
    if hash_domain(ROLLUP_HASH_DOMAIN, &raw) != expected_hash {
        return Err(RollupError::Integrity(format!(
            "rollup record {key} hash mismatch"
        )));
    }
    serde_json::from_slice(&raw).map_err(|e| {
        RollupError::Integrity(format!("rollup {key} not parseable: {e}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obj(key: &str) -> ManifestObject {
        ManifestObject {
            key: key.to_string(),
            table: "t".into(),
            content_hash: format!("h-{key}"),
            byte_len: 1,
            format: "jsonl".into(),
            format_version: 1,
            schema_id: "s".into(),
            compression: "none".into(),
            partition_spec: "table".into(),
            partition_version: 1,
        }
    }

    #[test]
    fn object_digest_is_order_independent() {
        let a = object_digest(&[obj("a"), obj("b"), obj("c")]);
        let b = object_digest(&[obj("c"), obj("a"), obj("b")]);
        assert_eq!(a, b);
        // A different inventory differs.
        let c = object_digest(&[obj("a"), obj("b")]);
        assert_ne!(a, c);
    }
}
