//! Manifest rollups + retained inventory index for the durable S3 path (P0.4,
//! 9A/9B).
//!
//! A rollup summarizes one CONTIGUOUS manifest sequence range `[start_seq,
//! end_seq]` of the already-verified acknowledgement chain: the boundary entry
//! hashes, the end watermark, and a digest of the authoritative object inventory
//! in that range. Rollups form an immutable, hash-linked chain (via `prev`) that
//! lets recovery skip re-walking summarized entries.
//!
//! 9B adds a retained, immutable, content-addressed INVENTORY INDEX bound into
//! each rollup: the complete `ManifestObject` metadata for every authoritative
//! object in the covered range, in canonical order. It preserves the acknowledged
//! object set across future manifest-entry deletion, so there is no verification
//! blind spot at the deletion boundary (covered keys stay enumerable + provable).
//! See `docs/specs/s3-durable-acks-9b1-design.md`.
//!
//! Rollups NEVER touch the source watermark, seq, entry or compaction references;
//! publishing one only sets HEAD's rollup + prev-rollup references (and the
//! rollup publication time). A corrupt/missing rollup or a required inventory
//! index drives fallback or a hard halt - never silent trust, never partial
//! recovery.

#![allow(dead_code)]

use object_store::path::Path;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::manifest::{ManifestObject, PrevRef};
use super::store_cond::{ConditionalStore, PutOutcome};

/// Bumped to 2 for the inventory-index binding (clean bump - durable_v2 unshipped).
pub const ROLLUP_RECORD_VERSION: u16 = 2;
pub const INVENTORY_INDEX_VERSION: u16 = 1;
const ROLLUP_HASH_DOMAIN: &[u8] = b"deltaforge/s3/rollup-record/v1";
const ROLLUP_DIGEST_DOMAIN: &[u8] = b"deltaforge/s3/rollup-object-digest/v1";
const INVENTORY_HASH_DOMAIN: &[u8] = b"deltaforge/s3/rollup-inventory/v1";

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
    /// The retained inventory index for this covered range. `inventory_record_hash`
    /// is the complete-index identity (covers version + identity + range + objects),
    /// used both as the index's key and as this binding. `inventory_count` must
    /// equal `object_count`.
    pub inventory_key: String,
    pub inventory_record_hash: String,
    pub inventory_count: u64,
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

/// The retained authoritative object inventory for one covered range. Immutable
/// and content-addressed by [`InventoryIndex::record_hash`], which covers the
/// COMPLETE index (identity + range + objects) so that identical object arrays in
/// different identities/ranges never collide at the same key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryIndex {
    pub version: u16,
    pub pipeline: String,
    pub source_id: String,
    pub sink_id: String,
    pub start_seq: u64,
    pub end_seq: u64,
    /// Complete `ManifestObject` metadata for every authoritative object in the
    /// range, in canonical order (ack sequence, then per-entry object order).
    pub objects: Vec<ManifestObject>,
}

impl InventoryIndex {
    pub fn canonical_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("inventory index serializes")
    }
    /// Domain-separated hash over the complete canonical index bytes (the struct's
    /// serialization in fixed field order, objects in canonical order).
    pub fn record_hash(&self) -> String {
        hash_domain(INVENTORY_HASH_DOMAIN, &self.canonical_bytes())
    }
    /// Object-list attestation (order-independent), for cross-checking the rollup.
    pub fn object_digest(&self) -> String {
        object_digest(&self.objects)
    }
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

pub fn inventory_key(prefix: &str, pipeline: &str, hash: &str) -> Path {
    Path::from_iter(
        prefix
            .split('/')
            .filter(|p| !p.is_empty())
            .chain([pipeline, "_manifest", "rollups", "inventory"])
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

/// A durably-written inventory index, for the rollup record to bind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WrittenInventory {
    pub key: String,
    pub record_hash: String,
    pub count: u64,
}

/// Write the inventory index create-only; idempotent on byte-identical retry. On
/// `AlreadyExists` the COMPLETE canonical object is re-read and verified (recompute
/// the record hash and require byte-identical content), never just a digest.
pub async fn write_inventory<S: ConditionalStore + ?Sized>(
    store: &S,
    prefix: &str,
    index: &InventoryIndex,
) -> Result<WrittenInventory, RollupError> {
    let canonical = index.canonical_bytes();
    let record_hash = hash_domain(INVENTORY_HASH_DOMAIN, &canonical);
    let key = inventory_key(prefix, &index.pipeline, &record_hash);
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
                    "inventory index at {key} does not match"
                )));
            }
            None => {
                return Err(RollupError::Store(format!(
                    "inventory index at {key} vanished after AlreadyExists"
                )));
            }
        },
        PutOutcome::Conflict => {
            return Err(RollupError::Store(format!(
                "unexpected CAS conflict on create-only inventory at {key}"
            )));
        }
    }
    Ok(WrittenInventory {
        key: key.to_string(),
        record_hash,
        count: index.objects.len() as u64,
    })
}

/// Load and parse an inventory index, verifying the complete-index hash matches
/// `expected_record_hash`.
pub async fn load_inventory<S: ConditionalStore + ?Sized>(
    store: &S,
    key: &str,
    expected_record_hash: &str,
) -> Result<InventoryIndex, RollupError> {
    let (raw, _) = store
        .get_with_etag(&Path::from(key.to_string()))
        .await?
        .ok_or_else(|| {
            RollupError::Integrity(format!("inventory index {key} missing"))
        })?;
    if hash_domain(INVENTORY_HASH_DOMAIN, &raw) != expected_record_hash {
        return Err(RollupError::Integrity(format!(
            "inventory index {key} hash mismatch"
        )));
    }
    serde_json::from_slice(&raw).map_err(|e| {
        RollupError::Integrity(format!("inventory {key} not parseable: {e}"))
    })
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
