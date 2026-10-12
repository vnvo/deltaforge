//! Immutable per-(batch, table) object upload for the durable S3 path (P0.4).
//!
//! One coordinator batch becomes one immutable object per table, each written
//! create-only at a deterministic content-addressed key. This module owns key
//! construction and the create-only + idempotency-verify upload; encoding lives
//! in `durable_encode`, and grouping events by table + schema resolution is the
//! sink's job (a later commit).
//!
//! **Uploading every table object is necessary but not sufficient for
//! durability.** A batch is not acknowledgeable until its manifest entry and the
//! HEAD CAS land (later commits). [`BatchUpload`] is the input those layers
//! consume, not an acknowledgement.
//!
//! Staged scaffolding: consumed by the durable sink in a later P0.4 commit.
#![allow(dead_code)]

use bytes::Bytes;
use object_store::path::Path;

use super::keys::{
    EncodingDomain, WATERMARK_SEGMENT_V2, check_key_bounds, content_hash,
};
use super::store_cond::{CondError, ConditionalStore, PutOutcome};

/// An encoded, ready-to-upload object for one table in a batch.
pub struct TableObject {
    pub table: String,
    pub bytes: Bytes,
    pub domain: EncodingDomain,
    /// File extension without the dot, e.g. `"parquet"` or `"jsonl"`.
    pub ext: &'static str,
}

/// Record of one durably-stored object, for the manifest layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadedObject {
    pub key: String,
    pub table: String,
    pub content_hash: String,
    pub byte_len: u64,
    pub encoding_domain: EncodingDomain,
    pub etag: Option<String>,
}

/// The set of objects a batch made durable. NOT an acknowledgement - the batch
/// is acknowledgeable only after its manifest entry + HEAD CAS commit.
#[derive(Debug, Clone, Default)]
pub struct BatchUpload {
    pub objects: Vec<UploadedObject>,
}

/// A failure while making a batch's objects durable.
#[derive(Debug, thiserror::Error)]
pub enum DurableError {
    #[error("object store: {0}")]
    Store(String),
    /// A different object already exists at a content-addressed key: the stored
    /// bytes do not match the content hash the key encodes. This is a **fatal**
    /// integrity error (a hash collision or a conflicting object) - never a
    /// silent idempotent success.
    #[error(
        "integrity: object at content-addressed key {key} does not match its \
         content hash (expected {expected}, found {found})"
    )]
    Integrity {
        key: String,
        expected: String,
        found: String,
    },
    /// The object key would exceed a backend limit (a path segment or the
    /// whole key, see `keys::check_key_bounds`). Fatal: retrying cannot
    /// shorten it, and no shortened key is ever substituted.
    #[error("object key for table {table} exceeds a backend limit: {reason}")]
    KeyBound { table: String, reason: String },
}

impl From<CondError> for DurableError {
    fn from(e: CondError) -> Self {
        DurableError::Store(e.to_string())
    }
}

impl DurableError {
    /// Integrity failures are fatal (stop the pipeline); store failures are
    /// retryable (the coordinator replays the batch).
    pub fn is_fatal(&self) -> bool {
        matches!(
            self,
            DurableError::Integrity { .. } | DurableError::KeyBound { .. }
        )
    }
}

/// Build the deterministic object key (key layout v2, see
/// `keys::KEY_LAYOUT_VERSION`).
///
/// `{prefix}/{pipeline}/{table}/wm2-{watermark_digest}/{content_hash}.{ext}`
///
/// `pipeline` and `table` are passed as raw parts to `Path::from_iter`, which
/// percent-encodes each single part (the `/` delimiter, `.`/`..`, `%` and other
/// unsafe bytes), so a hostile identifier can never inject an extra path segment
/// or a traversal - and there is no double-encoding. The watermark segment is
/// the bounded digest of the canonical watermark (`keys::watermark_key_digest`),
/// never the position itself; the full position is in the manifest entry.
/// Ordering never depends on keys (the manifest sequence is authoritative).
///
/// A key whose segments or length exceed `keys::check_key_bounds` is refused
/// ([`DurableError::KeyBound`]), never shortened.
fn build_object_key(
    prefix: &str,
    pipeline: &str,
    table: &str,
    watermark_digest: &str,
    content_hash_hex: &str,
    ext: &str,
) -> Result<Path, DurableError> {
    let mut parts: Vec<String> = Vec::new();
    // `prefix` is operator-configured; honor a multi-segment prefix, each segment
    // still encoded as its own part.
    for seg in prefix.split('/').filter(|p| !p.is_empty()) {
        parts.push(seg.to_string());
    }
    parts.push(pipeline.to_string());
    // The relation's bounded segment (readable, or its digest).
    parts.push(super::keys::table_key_segment(table));
    parts.push(format!("{WATERMARK_SEGMENT_V2}{watermark_digest}"));
    // content_hash is lowercase hex and ext a fixed literal - both already safe.
    parts.push(format!("{content_hash_hex}.{ext}"));
    let key = Path::from_iter(parts.iter().map(String::as_str));
    check_key_bounds(key.as_ref()).map_err(|reason| {
        DurableError::KeyBound {
            table: table.to_string(),
            reason,
        }
    })?;
    Ok(key)
}

/// Upload every table object create-only and return their durable records.
///
/// - Create-only (`If-None-Match: *`). On `AlreadyExists`, the existing object is
///   read and its recomputed content hash is compared to the key's hash; a match
///   is an idempotent success, a mismatch is a fatal [`DurableError::Integrity`].
/// - On any failure the function returns immediately. Objects already uploaded in
///   this call are **left in place** as unreferenced reconciliation candidates;
///   they are never deleted on the acknowledgement path.
pub async fn upload_batch<S: ConditionalStore + ?Sized>(
    store: &S,
    prefix: &str,
    pipeline: &str,
    watermark_digest: &str,
    objects: Vec<TableObject>,
) -> Result<BatchUpload, DurableError> {
    let mut out = BatchUpload::default();
    for obj in objects {
        let ch = content_hash(&obj.bytes, &obj.domain);
        let key = build_object_key(
            prefix,
            pipeline,
            &obj.table,
            watermark_digest,
            &ch,
            obj.ext,
        )?;
        let byte_len = obj.bytes.len() as u64;

        let etag = match store.put_if_absent(&key, obj.bytes.clone()).await? {
            PutOutcome::Written { etag } => etag,
            PutOutcome::AlreadyExists => {
                // Verify the existing object is byte-for-byte our content before
                // accepting the retry as idempotent.
                match store.get_with_etag(&key).await? {
                    Some((existing, etag)) => {
                        let found = content_hash(&existing, &obj.domain);
                        if found != ch {
                            return Err(DurableError::Integrity {
                                key: key.to_string(),
                                expected: ch,
                                found,
                            });
                        }
                        etag
                    }
                    None => {
                        // Present then absent: a concurrent delete/race. Treat as
                        // retryable rather than durable.
                        return Err(DurableError::Store(format!(
                            "object at {key} vanished after AlreadyExists"
                        )));
                    }
                }
            }
            PutOutcome::Conflict => {
                // Create-only never returns Conflict; surface defensively.
                return Err(DurableError::Store(format!(
                    "unexpected CAS conflict on create-only put at {key}"
                )));
            }
        };

        out.objects.push(UploadedObject {
            key: key.to_string(),
            table: obj.table,
            content_hash: ch,
            byte_len,
            encoding_domain: obj.domain,
            etag,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::store_cond::ObjectStoreConditional;
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;
    use std::sync::Arc;

    fn dom() -> EncodingDomain {
        EncodingDomain::new("jsonl", 1, "schema-1", "none", "table", 1)
    }

    fn obj(table: &str, bytes: &'static [u8]) -> TableObject {
        TableObject {
            table: table.to_string(),
            bytes: Bytes::from_static(bytes),
            domain: dom(),
            ext: "jsonl",
        }
    }

    /// A watermark key digest (64 hex digits).
    fn wmd() -> String {
        "d".repeat(64)
    }

    fn store() -> ObjectStoreConditional {
        ObjectStoreConditional::new(Arc::new(InMemory::new()))
    }

    #[test]
    fn keys_are_safe_and_deterministic() {
        let k1 = build_object_key(
            "root/pfx",
            "pipe",
            "public.orders",
            &wmd(),
            "abc123",
            "parquet",
        )
        .unwrap();
        let k2 = build_object_key(
            "root/pfx",
            "pipe",
            "public.orders",
            &wmd(),
            "abc123",
            "parquet",
        )
        .unwrap();
        assert_eq!(k1, k2, "key must be deterministic");
        assert_eq!(
            k1.to_string(),
            format!("root/pfx/pipe/public.orders/wm2-{}/abc123.parquet", wmd())
        );

        // A '/' inside an identifier must not create extra path segments.
        let k =
            build_object_key("p", "a/b", "c/d", &wmd(), "h", "jsonl").unwrap();
        let s = k.to_string();
        assert!(
            s.contains("a%2Fb") && s.contains("c%2Fd"),
            "embedded '/' must be encoded, not split: {s}"
        );
        // parts: prefix(p), pipeline, table, wm2-, filename = 5 - the hostile
        // '/'s did not add segments.
        assert_eq!(k.parts().count(), 5, "injection changed the segment count");

        // A whole-component ".." (traversal) must be encoded, never literal.
        let dots =
            build_object_key("p", "pipe", "..", &wmd(), "h", "jsonl").unwrap();
        assert!(
            dots.to_string().contains("%2E%2E")
                && !dots.parts().any(|p| p.as_ref() == ".."),
            "traversal component must be encoded: {dots}"
        );
    }

    #[tokio::test]
    async fn uploads_one_object_per_table_with_records() {
        let s = store();
        let up = upload_batch(
            &s,
            "pfx",
            "pipe",
            &wmd(),
            vec![obj("orders", b"orders-bytes"), obj("users", b"users-bytes")],
        )
        .await
        .unwrap();
        assert_eq!(up.objects.len(), 2);
        assert_eq!(up.objects[0].table, "orders");
        assert_eq!(up.objects[1].table, "users");
        for o in &up.objects {
            assert!(o.etag.is_some(), "InMemory returns an ETag");
            assert!(o.byte_len > 0);
            assert!(o.key.contains("/wm2-"));
        }
    }

    #[tokio::test]
    async fn retry_is_idempotent_same_keys() {
        let s = store();
        let mk = || vec![obj("orders", b"same-bytes")];
        let a = upload_batch(&s, "pfx", "pipe", &wmd(), mk()).await.unwrap();
        // Second run hits AlreadyExists and verifies content: idempotent success.
        let b = upload_batch(&s, "pfx", "pipe", &wmd(), mk()).await.unwrap();
        assert_eq!(a.objects[0].key, b.objects[0].key);
        assert_eq!(a.objects[0].content_hash, b.objects[0].content_hash);
    }

    #[tokio::test]
    async fn content_mismatch_at_key_is_fatal_integrity_error() {
        let s = store();
        let table = obj("orders", b"real-bytes");
        // Pre-place DIFFERENT bytes at exactly the key this upload will target.
        let ch = content_hash(&table.bytes, &table.domain);
        let key =
            build_object_key("pfx", "pipe", "orders", &wmd(), &ch, "jsonl")
                .unwrap();
        let inner: Arc<dyn object_store::ObjectStore> =
            Arc::new(InMemory::new());
        inner
            .put(
                &key,
                object_store::PutPayload::from(Bytes::from_static(b"tampered")),
            )
            .await
            .unwrap();
        let s2 = ObjectStoreConditional::new(inner);

        let err = upload_batch(&s2, "pfx", "pipe", &wmd(), vec![table])
            .await
            .unwrap_err();
        assert!(err.is_fatal(), "integrity error must be fatal: {err:?}");
        assert!(matches!(err, DurableError::Integrity { .. }));
        // Discard unused first store.
        let _ = s;
    }

    #[tokio::test]
    async fn partial_failure_preserves_earlier_objects() {
        // First table uploads cleanly; the second collides with tampered content
        // and fails. The first object must remain (reconciliation candidate),
        // never deleted on the ack path.
        let good = obj("orders", b"orders-ok");
        let bad = obj("users", b"users-real");
        let bad_ch = content_hash(&bad.bytes, &bad.domain);
        let bad_key =
            build_object_key("pfx", "pipe", "users", &wmd(), &bad_ch, "jsonl")
                .unwrap();
        let good_ch = content_hash(&good.bytes, &good.domain);
        let good_key = build_object_key(
            "pfx",
            "pipe",
            "orders",
            &wmd(),
            &good_ch,
            "jsonl",
        )
        .unwrap();

        let inner: Arc<dyn object_store::ObjectStore> =
            Arc::new(InMemory::new());
        inner
            .put(
                &bad_key,
                object_store::PutPayload::from(Bytes::from_static(b"tampered")),
            )
            .await
            .unwrap();
        let s = ObjectStoreConditional::new(Arc::clone(&inner));

        let err = upload_batch(&s, "pfx", "pipe", &wmd(), vec![good, bad])
            .await
            .unwrap_err();
        assert!(matches!(err, DurableError::Integrity { .. }));
        // The first (good) object is still durable, not deleted.
        assert!(
            inner.get(&good_key).await.is_ok(),
            "earlier uploaded object must be preserved after partial failure"
        );
    }
}
