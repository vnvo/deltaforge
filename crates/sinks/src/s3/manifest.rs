//! Manifest entries for the durable S3 path (P0.4).
//!
//! A manifest entry is the immutable, hash-chained record that a batch's objects
//! exist. It is written create-only; the entry's content hash chains to the
//! previous entry so the log is verifiable. **Creating an entry is not an
//! acknowledgement and never advances the durable watermark** - only the HEAD
//! CAS (a later commit) selects the authoritative entry for a sequence and
//! publishes it.
//!
//! Determinism: the canonical bytes (which are both hashed and stored) contain no
//! wall-clock time, random ids, ETags, or map iteration order. Per-table object
//! records are sorted before serialization, and the entry hash is
//! domain-separated so it cannot be confused with any other SHA-256 in the
//! system.
//!
//! Staged scaffolding: consumed by the HEAD/CAS layer in a later P0.4 commit.
#![allow(dead_code)]

use object_store::path::Path;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::batch_upload::UploadedObject;
use super::store_cond::{CondError, ConditionalStore, PutOutcome};

/// Canonical manifest entry version. Bump if the canonical byte layout changes.
pub const MANIFEST_ENTRY_VERSION: u16 = 1;

/// Domain-separation tag folded into the entry hash so a manifest-entry digest
/// can never collide with a data-object or any other SHA-256 in the system.
const ENTRY_HASH_DOMAIN: &[u8] = b"deltaforge:s3:manifest-entry:v1";

/// Hash-critical object metadata recorded in an entry. Note the deliberate
/// absence of any ETag: ETags are storage-assigned and non-deterministic, so
/// they must never enter hash-critical content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestObject {
    pub key: String,
    pub table: String,
    pub content_hash: String,
    pub byte_len: u64,
    pub format: String,
    pub format_version: u16,
    pub schema_id: String,
}

impl ManifestObject {
    /// Build from an [`UploadedObject`], dropping the (non-deterministic) ETag.
    pub fn from_uploaded(o: &UploadedObject) -> Self {
        Self {
            key: o.key.clone(),
            table: o.table.clone(),
            content_hash: o.content_hash.clone(),
            byte_len: o.byte_len,
            format: o.encoding_domain.format.to_string(),
            format_version: o.encoding_domain.format_version,
            schema_id: o.encoding_domain.schema_id.clone(),
        }
    }

    /// Total order for deterministic serialization: (table, key) is unique per
    /// batch (one object per table) but we sort on both for stability.
    fn sort_key(&self) -> (&str, &str) {
        (self.table.as_str(), self.key.as_str())
    }
}

/// Reference to the previous entry in the chain (absent for genesis).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrevRef {
    pub key: String,
    pub hash: String,
}

/// An immutable manifest entry. `prev == None` is the explicit genesis form.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub version: u16,
    pub pipeline: String,
    pub source_id: String,
    pub sink_id: String,
    pub epoch: u64,
    /// Proposed sequence (`HEAD.seq + 1`). NOT independently authoritative - two
    /// writers may legitimately propose the same seq with different content; only
    /// the HEAD CAS selects the winner.
    pub seq: u64,
    /// Opaque source checkpoint, hex-encoded (never assumed path-safe/sortable).
    pub watermark_hex: String,
    pub event_count: u64,
    pub objects: Vec<ManifestObject>,
    /// `None` = genesis (no previous entry); `Some` records both the previous
    /// entry's key and its hash.
    pub prev: Option<PrevRef>,
}

/// Propose the sequence for a new entry given the current HEAD sequence.
/// Centralized so the "HEAD.seq + 1" rule is not open-coded.
pub fn propose_seq(head_seq: u64) -> u64 {
    head_seq + 1
}

impl ManifestEntry {
    /// Whether this is the genesis entry (no predecessor).
    pub fn is_genesis(&self) -> bool {
        self.prev.is_none()
    }

    /// Canonical bytes: sort objects, then serialize deterministically. These are
    /// exactly the bytes stored in S3 and the bytes fed to the hash.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut c = self.clone();
        c.objects.sort_by(|a, b| a.sort_key().cmp(&b.sort_key()));
        // serde_json emits struct fields in declaration order and has no maps
        // here, so this is deterministic given the sorted object vec.
        serde_json::to_vec(&c).expect("manifest entry serializes")
    }

    /// Domain-separated SHA-256 of the canonical bytes, as lowercase hex.
    pub fn entry_hash(&self) -> String {
        hash_canonical(&self.canonical_bytes())
    }
}

fn hash_canonical(canonical: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(ENTRY_HASH_DOMAIN);
    h.update((canonical.len() as u64).to_be_bytes());
    h.update(canonical);
    let d = h.finalize();
    let mut s = String::with_capacity(d.len() * 2);
    for b in d {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Entry object key. Contains the proposed seq and the entry hash; competing
/// writers proposing the same seq with different content produce different hashes
/// and therefore different keys (both can be created), so the HEAD CAS alone
/// decides which is authoritative.
pub fn entry_key(prefix: &str, pipeline: &str, seq: u64, hash: &str) -> Path {
    Path::from_iter(
        prefix
            .split('/')
            .filter(|p| !p.is_empty())
            .chain([pipeline, "_manifest", "entries"])
            .map(str::to_string)
            .chain([format!("{seq:020}-{hash}.json")]),
    )
}

/// A durably-written manifest entry, for the HEAD layer to reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WrittenEntry {
    pub key: String,
    pub hash: String,
    pub seq: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    #[error("object store: {0}")]
    Store(String),
    /// An entry already exists at the key but its stored bytes differ from our
    /// canonical bytes. Fatal: a hash collision or a conflicting entry.
    #[error("integrity: existing manifest entry at {key} does not match")]
    Integrity { key: String },
}

impl From<CondError> for ManifestError {
    fn from(e: CondError) -> Self {
        ManifestError::Store(e.to_string())
    }
}

impl ManifestError {
    pub fn is_fatal(&self) -> bool {
        matches!(self, ManifestError::Integrity { .. })
    }
}

/// Create the entry create-only. On `AlreadyExists`, the existing object must
/// match our canonical bytes exactly (idempotent retry); a mismatch is a fatal
/// integrity error. **Writing an entry never advances the durable watermark and
/// never acknowledges** - that is the HEAD CAS's job.
pub async fn write_entry<S: ConditionalStore + ?Sized>(
    store: &S,
    prefix: &str,
    entry: &ManifestEntry,
) -> Result<WrittenEntry, ManifestError> {
    let canonical = entry.canonical_bytes();
    let hash = hash_canonical(&canonical);
    let key = entry_key(prefix, &entry.pipeline, entry.seq, &hash);
    let payload = bytes::Bytes::from(canonical.clone());

    match store.put_if_absent(&key, payload).await? {
        PutOutcome::Written { .. } => {}
        PutOutcome::AlreadyExists => match store.get_with_etag(&key).await? {
            Some((existing, _etag)) => {
                if existing.as_ref() != canonical.as_slice() {
                    return Err(ManifestError::Integrity {
                        key: key.to_string(),
                    });
                }
            }
            None => {
                return Err(ManifestError::Store(format!(
                    "entry at {key} vanished after AlreadyExists"
                )));
            }
        },
        PutOutcome::Conflict => {
            return Err(ManifestError::Store(format!(
                "unexpected CAS conflict on create-only entry at {key}"
            )));
        }
    }

    Ok(WrittenEntry {
        key: key.to_string(),
        hash,
        seq: entry.seq,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::keys::EncodingDomain;
    use crate::s3::store_cond::ObjectStoreConditional;
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;
    use std::sync::Arc;

    fn mobj(table: &str, key: &str) -> ManifestObject {
        ManifestObject {
            key: key.to_string(),
            table: table.to_string(),
            content_hash: format!("hash-of-{table}"),
            byte_len: 100,
            format: "parquet".into(),
            format_version: 1,
            schema_id: "s1".into(),
        }
    }

    fn entry(
        seq: u64,
        prev: Option<PrevRef>,
        objs: Vec<ManifestObject>,
    ) -> ManifestEntry {
        ManifestEntry {
            version: MANIFEST_ENTRY_VERSION,
            pipeline: "pipe".into(),
            source_id: "src".into(),
            sink_id: "sink".into(),
            epoch: 7,
            seq,
            watermark_hex: "0a0b".into(),
            event_count: 3,
            objects: objs,
            prev,
        }
    }

    fn store() -> ObjectStoreConditional {
        ObjectStoreConditional::new(Arc::new(InMemory::new()))
    }

    #[test]
    fn canonical_bytes_sort_objects_independent_of_input_order() {
        let a = entry(1, None, vec![mobj("users", "k2"), mobj("orders", "k1")]);
        let b = entry(1, None, vec![mobj("orders", "k1"), mobj("users", "k2")]);
        assert_eq!(
            a.canonical_bytes(),
            b.canonical_bytes(),
            "object input order must not affect canonical bytes"
        );
        assert_eq!(a.entry_hash(), b.entry_hash());
    }

    #[test]
    fn canonical_bytes_are_stable_across_calls() {
        let e = entry(1, None, vec![mobj("orders", "k1")]);
        assert_eq!(e.canonical_bytes(), e.canonical_bytes());
        assert_eq!(e.entry_hash(), e.entry_hash());
    }

    #[test]
    fn tampered_prev_hash_changes_entry_hash() {
        let base = entry(
            2,
            Some(PrevRef {
                key: "k1".into(),
                hash: "prev-hash".into(),
            }),
            vec![mobj("orders", "k1")],
        );
        let tampered = entry(
            2,
            Some(PrevRef {
                key: "k1".into(),
                hash: "DIFFERENT".into(),
            }),
            vec![mobj("orders", "k1")],
        );
        assert_ne!(base.entry_hash(), tampered.entry_hash());
    }

    #[test]
    fn altered_object_metadata_changes_entry_hash() {
        let base = entry(1, None, vec![mobj("orders", "k1")]);
        let mut altered_obj = mobj("orders", "k1");
        altered_obj.byte_len = 999;
        let altered = entry(1, None, vec![altered_obj]);
        assert_ne!(base.entry_hash(), altered.entry_hash());
    }

    #[test]
    fn genesis_and_non_genesis_differ_and_are_explicit() {
        let genesis = entry(1, None, vec![mobj("orders", "k1")]);
        assert!(genesis.is_genesis());
        let child = entry(
            2,
            Some(PrevRef {
                key: genesis.entry_hash(),
                hash: genesis.entry_hash(),
            }),
            vec![mobj("orders", "k1")],
        );
        assert!(!child.is_genesis());
        assert_ne!(genesis.entry_hash(), child.entry_hash());
    }

    #[test]
    fn propose_seq_is_head_plus_one() {
        assert_eq!(propose_seq(0), 1);
        assert_eq!(propose_seq(41), 42);
    }

    #[tokio::test]
    async fn write_entry_is_idempotent_on_retry() {
        let s = store();
        let e = entry(1, None, vec![mobj("orders", "k1")]);
        let a = write_entry(&s, "pfx", &e).await.unwrap();
        let b = write_entry(&s, "pfx", &e).await.unwrap();
        assert_eq!(a, b, "retrying an identical entry is idempotent");
        assert!(a.key.contains("_manifest/entries/"));
        assert!(a.key.contains(&format!("{:020}-", 1)));
    }

    #[tokio::test]
    async fn competing_same_seq_entries_get_distinct_keys() {
        let s = store();
        // Same seq, different content (different objects) -> different hash ->
        // different key -> both create-only writes succeed. HEAD CAS (later)
        // picks the winner.
        let e1 = entry(5, None, vec![mobj("orders", "k1")]);
        let e2 = entry(5, None, vec![mobj("users", "k9")]);
        let w1 = write_entry(&s, "pfx", &e1).await.unwrap();
        let w2 = write_entry(&s, "pfx", &e2).await.unwrap();
        assert_eq!(w1.seq, w2.seq);
        assert_ne!(w1.hash, w2.hash);
        assert_ne!(w1.key, w2.key);
    }

    #[tokio::test]
    async fn existing_key_with_different_bytes_is_fatal_integrity() {
        let e = entry(1, None, vec![mobj("orders", "k1")]);
        let hash = e.entry_hash();
        let key = entry_key("pfx", "pipe", 1, &hash);
        let inner: Arc<dyn object_store::ObjectStore> =
            Arc::new(InMemory::new());
        inner
            .put(
                &key,
                object_store::PutPayload::from(bytes::Bytes::from_static(
                    b"tampered",
                )),
            )
            .await
            .unwrap();
        let s = ObjectStoreConditional::new(inner);
        let err = write_entry(&s, "pfx", &e).await.unwrap_err();
        assert!(err.is_fatal());
        assert!(matches!(err, ManifestError::Integrity { .. }));
    }

    #[test]
    fn manifest_object_drops_etag() {
        let up = UploadedObject {
            key: "k".into(),
            table: "orders".into(),
            content_hash: "ch".into(),
            byte_len: 10,
            encoding_domain: EncodingDomain::new("parquet", 1, "s1"),
            etag: Some("\"etag-should-not-appear\"".into()),
        };
        let mo = ManifestObject::from_uploaded(&up);
        let json = serde_json::to_string(&mo).unwrap();
        assert!(
            !json.contains("etag"),
            "ETag must never enter hash-critical content: {json}"
        );
    }
}
