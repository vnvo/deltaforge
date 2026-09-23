//! Asynchronous compaction, decoupled from source acknowledgement (P0.4).
//!
//! Compaction replaces several already-durable, already-acknowledged data
//! objects for ONE table with a single immutable compacted object. It NEVER
//! touches the source watermark, batch sequence, or the acknowledgement manifest
//! chain: it writes an immutable compaction record (mapping the original object
//! keys/hashes to the replacement) and publishes it through a HEAD CAS that
//! preserves epoch, seq, entry reference, watermark and rollup references,
//! setting only the compaction reference.
//!
//! This commit publishes and verifies compactions WITHOUT deleting originals -
//! the record's `originals` list marks them eligible for later GC. Storage
//! inefficiency is safer than premature deletion.

#![allow(dead_code)]

use object_store::path::Path;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::keys::EncodingDomain;
use super::manifest::{ManifestObject, PrevRef};
use super::store_cond::{ConditionalStore, PutOutcome};

// v2: embedded ManifestObjects now carry the full encoding domain (compression
// codec + partition spec/version). Clean bump; durable v2 has not shipped.
pub const COMPACTION_RECORD_VERSION: u16 = 2;
const RECORD_HASH_DOMAIN: &[u8] = b"deltaforge/s3/compaction-record/v1";

/// An immutable record mapping the original object keys/hashes for one table to
/// the replacement object. `prev` chains the compaction history (absent for the
/// first record); the chain is walked bounded and acyclically at recovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionRecord {
    pub version: u16,
    pub pipeline: String,
    pub source_id: String,
    pub sink_id: String,
    /// Fully-qualified table; all originals and the replacement share it.
    pub table: String,
    /// The compacted object that supersedes `originals`.
    pub replacement: ManifestObject,
    /// The superseded objects (canonically sorted). Eligible for later GC once
    /// the replacement is proven authoritative.
    pub originals: Vec<ManifestObject>,
    /// Previous compaction record in the history (None = first).
    pub prev: Option<PrevRef>,
}

/// The authoritative compaction index, built at recovery from the records
/// reachable from HEAD and verified against the acknowledgement chain. Maps each
/// superseded original object key to its single active replacement. GC (9B)
/// consumes this verified index rather than rescanning arbitrary objects.
#[derive(Debug, Clone, Default)]
pub struct CompactionIndex {
    by_original: std::collections::HashMap<String, ManifestObject>,
}

impl CompactionIndex {
    /// Record that `original_key` is superseded by `replacement`. Returns the
    /// previous mapping if the original was already claimed (a conflict).
    pub(crate) fn insert(
        &mut self,
        original_key: String,
        replacement: ManifestObject,
    ) -> Option<ManifestObject> {
        self.by_original.insert(original_key, replacement)
    }

    /// The active replacement for an original object key, if it was compacted.
    pub fn active_replacement(
        &self,
        original_key: &str,
    ) -> Option<&ManifestObject> {
        self.by_original.get(original_key)
    }

    pub fn len(&self) -> usize {
        self.by_original.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_original.is_empty()
    }
}

impl CompactionRecord {
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut c = self.clone();
        c.originals.sort_by(|a, b| {
            (a.table.as_str(), a.key.as_str())
                .cmp(&(b.table.as_str(), b.key.as_str()))
        });
        serde_json::to_vec(&c).expect("compaction record serializes")
    }

    pub fn record_hash(&self) -> String {
        hash_canonical(&self.canonical_bytes())
    }

    /// The domain the replacement (and every original) must share.
    pub fn domain(&self) -> EncodingDomain {
        self.replacement.encoding_domain()
    }
}

fn hash_canonical(canonical: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(RECORD_HASH_DOMAIN);
    h.update((canonical.len() as u64).to_be_bytes());
    h.update(canonical);
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

/// Content-addressed key for a compaction record.
pub fn record_key(prefix: &str, pipeline: &str, hash: &str) -> Path {
    Path::from_iter(
        prefix
            .split('/')
            .filter(|p| !p.is_empty())
            .chain([pipeline, "_manifest", "compactions"])
            .map(str::to_string)
            .chain([format!("{hash}.json")]),
    )
}

/// Content-addressed key for a compacted data object.
pub fn compacted_object_key(
    prefix: &str,
    pipeline: &str,
    table: &str,
    hash: &str,
    ext: &str,
) -> Path {
    Path::from_iter(
        prefix
            .split('/')
            .filter(|p| !p.is_empty())
            .chain([pipeline, table, "compacted"])
            .map(str::to_string)
            .chain([format!("{hash}.{ext}")]),
    )
}

#[derive(Debug, thiserror::Error)]
pub enum CompactionError {
    #[error("object store: {0}")]
    Store(String),
    /// An original/replacement key exists but its stored bytes differ from the
    /// expected content hash. Fatal integrity error.
    #[error("integrity: {0}")]
    Integrity(String),
    /// The proposed compaction mixes tables or encoding domains, or is empty.
    #[error("incompatible compaction: {0}")]
    Incompatible(String),
}

impl CompactionError {
    pub fn is_fatal(&self) -> bool {
        matches!(
            self,
            CompactionError::Integrity(_) | CompactionError::Incompatible(_)
        )
    }
}

impl From<super::store_cond::CondError> for CompactionError {
    fn from(e: super::store_cond::CondError) -> Self {
        CompactionError::Store(e.to_string())
    }
}

/// Compatibility gate: every original must share `table` and the exact encoding
/// domain of the replacement. The domain now includes the compression codec and
/// the partition spec/version as explicit fields, so Snappy vs Zstd, or a
/// different partition scheme, are rejected here - not silently accepted. Never
/// compact across an incompatible schema, encoding, compression or partition
/// domain.
pub fn check_compatible(
    table: &str,
    replacement_domain: &EncodingDomain,
    originals: &[ManifestObject],
) -> Result<(), CompactionError> {
    if originals.is_empty() {
        return Err(CompactionError::Incompatible(
            "no original objects to compact".into(),
        ));
    }
    for o in originals {
        if o.table != table {
            return Err(CompactionError::Incompatible(format!(
                "original {} is table {}, expected {table}",
                o.key, o.table
            )));
        }
        if &o.encoding_domain() != replacement_domain {
            return Err(CompactionError::Incompatible(format!(
                "original {} domain {:?} != replacement domain {:?}",
                o.key,
                o.encoding_domain(),
                replacement_domain
            )));
        }
    }
    Ok(())
}

/// Read each original and verify it is present with the recorded content hash and
/// byte length. This is the "read originals" precondition; a missing or mismatched
/// original fails closed (never compact away content that is not verifiably the
/// authoritative object). Returns integrity errors as fatal.
pub async fn verify_originals_present<S: ConditionalStore + ?Sized>(
    store: &S,
    originals: &[ManifestObject],
) -> Result<(), CompactionError> {
    for o in originals {
        let key = Path::from(o.key.clone());
        match store.get_with_etag(&key).await? {
            Some((bytes, _)) => {
                if bytes.len() as u64 != o.byte_len {
                    return Err(CompactionError::Integrity(format!(
                        "original {} byte_len {} != recorded {}",
                        o.key,
                        bytes.len(),
                        o.byte_len
                    )));
                }
                let got =
                    super::keys::content_hash(&bytes, &o.encoding_domain());
                if got != o.content_hash {
                    return Err(CompactionError::Integrity(format!(
                        "original {} content hash mismatch",
                        o.key
                    )));
                }
            }
            None => {
                return Err(CompactionError::Integrity(format!(
                    "original {} is missing",
                    o.key
                )));
            }
        }
    }
    Ok(())
}

/// A durably-written compaction record, for the HEAD layer to reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WrittenRecord {
    pub key: String,
    pub hash: String,
}

/// Write the compaction record create-only. On `AlreadyExists`, the existing
/// bytes must match ours exactly (idempotent retry); a mismatch is fatal.
pub async fn write_record<S: ConditionalStore + ?Sized>(
    store: &S,
    prefix: &str,
    record: &CompactionRecord,
) -> Result<WrittenRecord, CompactionError> {
    let canonical = record.canonical_bytes();
    let hash = hash_canonical(&canonical);
    let key = record_key(prefix, &record.pipeline, &hash);
    let payload = bytes::Bytes::from(canonical.clone());

    match store.put_if_absent(&key, payload).await? {
        PutOutcome::Written { .. } => {}
        PutOutcome::AlreadyExists => match store.get_with_etag(&key).await? {
            Some((existing, _))
                if existing.as_ref() == canonical.as_slice() => {}
            Some(_) => {
                return Err(CompactionError::Integrity(format!(
                    "compaction record at {key} does not match"
                )));
            }
            None => {
                return Err(CompactionError::Store(format!(
                    "compaction record at {key} vanished after AlreadyExists"
                )));
            }
        },
        PutOutcome::Conflict => {
            return Err(CompactionError::Store(format!(
                "unexpected CAS conflict on create-only record at {key}"
            )));
        }
    }
    Ok(WrittenRecord {
        key: key.to_string(),
        hash,
    })
}

/// Load and parse a compaction record, verifying its bytes match `expected_hash`.
pub async fn load_record<S: ConditionalStore + ?Sized>(
    store: &S,
    key: &str,
    expected_hash: &str,
) -> Result<CompactionRecord, CompactionError> {
    let (raw, _) = store
        .get_with_etag(&Path::from(key.to_string()))
        .await?
        .ok_or_else(|| {
            CompactionError::Integrity(format!(
                "compaction record {key} missing"
            ))
        })?;
    if hash_canonical(&raw) != expected_hash {
        return Err(CompactionError::Integrity(format!(
            "compaction record {key} hash mismatch"
        )));
    }
    serde_json::from_slice(&raw).map_err(|e| {
        CompactionError::Integrity(format!("record {key} not parseable: {e}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::keys::EncodingDomain;

    fn obj(table: &str, key: &str) -> ManifestObject {
        ManifestObject {
            key: key.to_string(),
            table: table.to_string(),
            content_hash: "h".to_string(),
            byte_len: 1,
            format: "jsonl".to_string(),
            format_version: 1,
            schema_id: "s1".to_string(),
            compression: "none".to_string(),
            partition_spec: "table".to_string(),
            partition_version: 1,
        }
    }

    #[test]
    fn check_compatible_rejects_mixed_table_or_domain() {
        let dom = EncodingDomain::new("jsonl", 1, "s1", "none", "table", 1);
        // Empty is rejected.
        assert!(check_compatible("shop.orders", &dom, &[]).is_err());
        // Mixed table.
        let mixed_tbl = vec![obj("shop.orders", "a"), obj("shop.users", "b")];
        assert!(check_compatible("shop.orders", &dom, &mixed_tbl).is_err());
        // Mixed domain (different schema fingerprint).
        let mut o = obj("shop.orders", "b");
        o.schema_id = "s2".into();
        let mixed_dom = vec![obj("shop.orders", "a"), o];
        assert!(check_compatible("shop.orders", &dom, &mixed_dom).is_err());
        // Compatible.
        let ok = vec![obj("shop.orders", "a"), obj("shop.orders", "b")];
        assert!(check_compatible("shop.orders", &dom, &ok).is_ok());
    }

    #[test]
    fn check_compatible_rejects_mixed_compression() {
        // A Snappy replacement can never absorb a Zstd original with the same
        // schema (and vice versa): compression is part of the domain identity.
        let snappy =
            EncodingDomain::new("parquet", 1, "s1", "snappy", "table", 1);
        let mut zstd_obj = obj("shop.orders", "b");
        zstd_obj.format = "parquet".into();
        zstd_obj.compression = "zstd".into();
        let mut snappy_obj = obj("shop.orders", "a");
        snappy_obj.format = "parquet".into();
        snappy_obj.compression = "snappy".into();
        let mixed = vec![snappy_obj.clone(), zstd_obj];
        assert!(check_compatible("shop.orders", &snappy, &mixed).is_err());
        // All-snappy is fine.
        assert!(
            check_compatible("shop.orders", &snappy, &[snappy_obj]).is_ok()
        );
    }

    #[test]
    fn check_compatible_rejects_mixed_partition() {
        // Different partition spec or version is incompatible even with identical
        // format/schema/compression.
        let dom = EncodingDomain::new("jsonl", 1, "s1", "none", "table", 1);
        let mut other_ver = obj("shop.orders", "b");
        other_ver.partition_version = 2;
        assert!(
            check_compatible(
                "shop.orders",
                &dom,
                &[obj("shop.orders", "a"), other_ver]
            )
            .is_err()
        );
        let mut other_spec = obj("shop.orders", "c");
        other_spec.partition_spec = "table/date".into();
        assert!(check_compatible("shop.orders", &dom, &[other_spec]).is_err());
    }

    #[test]
    fn record_hash_is_stable_regardless_of_original_order() {
        let dom = EncodingDomain::new("jsonl", 1, "s1", "none", "table", 1);
        let mk = |order: Vec<ManifestObject>| CompactionRecord {
            version: COMPACTION_RECORD_VERSION,
            pipeline: "p".into(),
            source_id: "src".into(),
            sink_id: "sink".into(),
            table: "shop.orders".into(),
            replacement: {
                let mut r = obj("shop.orders", "compacted");
                r.content_hash = "rh".into();
                r
            },
            originals: order,
            prev: None,
        };
        let _ = &dom;
        let a = mk(vec![obj("shop.orders", "a"), obj("shop.orders", "b")]);
        let b = mk(vec![obj("shop.orders", "b"), obj("shop.orders", "a")]);
        assert_eq!(a.record_hash(), b.record_hash());
    }
}
