//! Independent equivalence proof for compaction replacements (P0.4, 9B.1).
//!
//! Proves that a compaction replacement represents EXACTLY the record's originals,
//! in canonical order, WITHOUT re-running the compactor's transformation and
//! comparing it to its own output. The validator decodes both sides from their
//! stored, hash-verified bytes and compares row by row, so a compactor bug cannot
//! mask a mismatch.
//!
//! 9B.1 supports JSONL only (Parquet compaction and its own validator land later);
//! any other format is rejected fail-closed. Row identity is the mandatory
//! top-level `event_id` string (a valid serialized `EventId`); a row lacking it is
//! an upstream contract violation and fails closed - never masked. Equivalence
//! preserves MULTIPLICITY and ORDER (at-least-once delivery may legitimately carry
//! repeated `event_id`s); it does not impose uniqueness.
//!
//! See `docs/specs/s3-durable-acks-9b1-design.md` section 6.

#![allow(dead_code)]

use deltaforge_core::EventId;
use object_store::path::Path;

use super::durable_encode::canonical_json_bytes;
use super::keys::content_hash;
use super::manifest::ManifestObject;
use super::store_cond::ConditionalStore;

#[derive(Debug, thiserror::Error)]
pub enum EquivalenceError {
    #[error("object store: {0}")]
    Store(String),
    #[error("unsupported format for equivalence: {0}")]
    Unsupported(String),
    #[error("integrity: {0}")]
    Integrity(String),
    #[error("not equivalent: {0}")]
    NotEquivalent(String),
}

/// One decoded, canonicalized JSONL row: its `event_id` identity and canonical
/// record bytes. Equality of the pair is row equivalence.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Row {
    event_id: String,
    canonical: Vec<u8>,
}

/// Prove `replacement` is row-for-row equivalent to `originals` concatenated in the
/// given (canonical ack) order. `originals` MUST already be ordered by acknowledgement
/// sequence and per-entry object order. Reads verified bytes from the store.
pub async fn verify_jsonl<S: ConditionalStore + ?Sized>(
    store: &S,
    originals: &[ManifestObject],
    replacement: &ManifestObject,
) -> Result<(), EquivalenceError> {
    // Format gate: JSONL only, and a single shared encoding domain.
    require_jsonl(replacement)?;
    let want_domain = replacement.encoding_domain();
    for o in originals {
        require_jsonl(o)?;
        if o.encoding_domain() != want_domain || o.table != replacement.table {
            return Err(EquivalenceError::Unsupported(format!(
                "original {} domain/table differs from the replacement",
                o.key
            )));
        }
    }

    // Decode expected (originals in order) and actual (replacement).
    let mut expected: Vec<Row> = Vec::new();
    for o in originals {
        let bytes = read_verified(store, o).await?;
        expected.extend(decode_rows(&o.key, &bytes)?);
    }
    let repl_bytes = read_verified(store, replacement).await?;
    let actual = decode_rows(&replacement.key, &repl_bytes)?;

    if actual.len() != expected.len() {
        return Err(EquivalenceError::NotEquivalent(format!(
            "row count differs: replacement {} vs originals {}",
            actual.len(),
            expected.len()
        )));
    }
    // Row-by-row, order-sensitive: a faithfully-preserved duplicate passes; a
    // dropped/added/reordered/mutated row fails at the first divergent position.
    for (i, (a, e)) in actual.iter().zip(expected.iter()).enumerate() {
        if a != e {
            return Err(EquivalenceError::NotEquivalent(format!(
                "row {i} differs (replacement event_id {}, expected event_id {})",
                a.event_id, e.event_id
            )));
        }
    }
    Ok(())
}

fn require_jsonl(o: &ManifestObject) -> Result<(), EquivalenceError> {
    if o.format != "jsonl" {
        return Err(EquivalenceError::Unsupported(format!(
            "object {} is {} (only jsonl is supported in 9B.1)",
            o.key, o.format
        )));
    }
    Ok(())
}

/// Read an object and verify its bytes hash to the recorded `content_hash` under
/// the object's encoding domain (the exact-verified-input-bytes requirement).
async fn read_verified<S: ConditionalStore + ?Sized>(
    store: &S,
    o: &ManifestObject,
) -> Result<bytes::Bytes, EquivalenceError> {
    let (raw, _) = store
        .get_with_etag(&Path::from(o.key.clone()))
        .await
        .map_err(|e| EquivalenceError::Store(e.to_string()))?
        .ok_or_else(|| {
            EquivalenceError::Integrity(format!("object {} missing", o.key))
        })?;
    if content_hash(&raw, &o.encoding_domain()) != o.content_hash {
        return Err(EquivalenceError::Integrity(format!(
            "object {} content hash does not match its record",
            o.key
        )));
    }
    Ok(raw)
}

/// Decode JSONL bytes into canonical rows, requiring a valid top-level `event_id`
/// on each. A trailing newline yields no empty row; a blank interior line is an
/// error (durable JSONL never writes one).
fn decode_rows(key: &str, bytes: &[u8]) -> Result<Vec<Row>, EquivalenceError> {
    let text = std::str::from_utf8(bytes).map_err(|_| {
        EquivalenceError::Integrity(format!("object {key} is not valid UTF-8"))
    })?;
    let mut rows = Vec::new();
    for (lineno, line) in text.split('\n').enumerate() {
        if line.is_empty() {
            // Only the final newline may produce an empty trailing segment.
            continue;
        }
        let value: serde_json::Value =
            serde_json::from_str(line).map_err(|e| {
                EquivalenceError::Integrity(format!(
                    "object {key} line {lineno} is not JSON: {e}"
                ))
            })?;
        let event_id = extract_event_id(key, lineno, &value)?;
        rows.push(Row {
            event_id,
            canonical: canonical_json_bytes(&value),
        });
    }
    Ok(rows)
}

/// The mandatory top-level `event_id`: present, non-null, a string, and a valid
/// serialized `EventId`. No fallback - absence/nullness/malformed fails closed.
fn extract_event_id(
    key: &str,
    lineno: usize,
    value: &serde_json::Value,
) -> Result<String, EquivalenceError> {
    let raw = value.get("event_id").ok_or_else(|| {
        EquivalenceError::Integrity(format!(
            "object {key} line {lineno} has no top-level event_id"
        ))
    })?;
    let s = raw.as_str().ok_or_else(|| {
        EquivalenceError::Integrity(format!(
            "object {key} line {lineno} event_id is not a string"
        ))
    })?;
    // Must parse as a real EventId, not merely be some string.
    s.parse::<EventId>().map_err(|e| {
        EquivalenceError::Integrity(format!(
            "object {key} line {lineno} event_id is not a valid EventId: {e}"
        ))
    })?;
    Ok(s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::keys::EncodingDomain;
    use crate::s3::store_cond::ObjectStoreConditional;
    use object_store::memory::InMemory;
    use std::sync::Arc;

    fn store() -> Arc<ObjectStoreConditional> {
        Arc::new(ObjectStoreConditional::new(Arc::new(InMemory::new())))
    }

    fn eid(n: u32) -> String {
        EventId::mysql_row_server(1, "orders", n as u64, 0).to_string()
    }

    fn domain() -> EncodingDomain {
        EncodingDomain::new("jsonl", 1, "s1", "none", "table", 1)
    }

    /// Build canonical JSONL bytes for `rows` (each an id + a value), writing one
    /// object per line as `{"event_id": id, "v": v}`.
    fn jsonl_bytes(rows: &[(String, i64)]) -> Vec<u8> {
        let mut buf = Vec::new();
        for (id, v) in rows {
            let val = serde_json::json!({"event_id": id, "v": v});
            buf.extend_from_slice(&canonical_json_bytes(&val));
            buf.push(b'\n');
        }
        buf
    }

    async fn put_obj(
        s: &ObjectStoreConditional,
        key: &str,
        bytes: Vec<u8>,
    ) -> ManifestObject {
        let d = domain();
        let ch = content_hash(&bytes, &d);
        let byte_len = bytes.len() as u64;
        s.put_if_absent(
            &Path::from(key.to_string()),
            bytes::Bytes::from(bytes),
        )
        .await
        .unwrap();
        ManifestObject {
            key: key.to_string(),
            table: "orders".into(),
            content_hash: ch,
            byte_len,
            format: "jsonl".into(),
            format_version: 1,
            schema_id: "s1".into(),
            compression: "none".into(),
            partition_spec: "table".into(),
            partition_version: 1,
        }
    }

    async fn orig(
        s: &ObjectStoreConditional,
        key: &str,
        rows: &[(String, i64)],
    ) -> ManifestObject {
        put_obj(s, key, jsonl_bytes(rows)).await
    }

    #[tokio::test]
    async fn faithful_replacement_is_equivalent() {
        let s = store();
        let o1 = orig(&s, "orders/wm-1/a.jsonl", &[(eid(1), 1)]).await;
        let o2 =
            orig(&s, "orders/wm-2/b.jsonl", &[(eid(2), 2), (eid(3), 3)]).await;
        let repl = orig(
            &s,
            "orders/compacted/r.jsonl",
            &[(eid(1), 1), (eid(2), 2), (eid(3), 3)],
        )
        .await;
        verify_jsonl(s.as_ref(), &[o1, o2], &repl)
            .await
            .expect("faithful replacement is equivalent");
    }

    #[tokio::test]
    async fn dropped_row_not_equivalent() {
        let s = store();
        let o1 =
            orig(&s, "orders/wm-1/a.jsonl", &[(eid(1), 1), (eid(2), 2)]).await;
        let repl = orig(&s, "orders/compacted/r.jsonl", &[(eid(1), 1)]).await;
        assert!(matches!(
            verify_jsonl(s.as_ref(), &[o1], &repl).await,
            Err(EquivalenceError::NotEquivalent(_))
        ));
    }

    #[tokio::test]
    async fn reordered_rows_not_equivalent() {
        let s = store();
        let o1 =
            orig(&s, "orders/wm-1/a.jsonl", &[(eid(1), 1), (eid(2), 2)]).await;
        let repl =
            orig(&s, "orders/compacted/r.jsonl", &[(eid(2), 2), (eid(1), 1)])
                .await;
        assert!(matches!(
            verify_jsonl(s.as_ref(), &[o1], &repl).await,
            Err(EquivalenceError::NotEquivalent(_))
        ));
    }

    #[tokio::test]
    async fn mutated_row_not_equivalent() {
        let s = store();
        let o1 = orig(&s, "orders/wm-1/a.jsonl", &[(eid(1), 1)]).await;
        let repl = orig(&s, "orders/compacted/r.jsonl", &[(eid(1), 999)]).await;
        assert!(matches!(
            verify_jsonl(s.as_ref(), &[o1], &repl).await,
            Err(EquivalenceError::NotEquivalent(_))
        ));
    }

    #[tokio::test]
    async fn duplicate_event_ids_preserved_is_equivalent() {
        // At-least-once may carry the same event_id twice; multiplicity + order
        // preserved at the same positions passes.
        let s = store();
        let o1 =
            orig(&s, "orders/wm-1/a.jsonl", &[(eid(1), 1), (eid(1), 1)]).await;
        let repl =
            orig(&s, "orders/compacted/r.jsonl", &[(eid(1), 1), (eid(1), 1)])
                .await;
        verify_jsonl(s.as_ref(), &[o1], &repl)
            .await
            .expect("preserved duplicate is equivalent");
    }

    #[tokio::test]
    async fn dropped_duplicate_not_equivalent() {
        let s = store();
        let o1 =
            orig(&s, "orders/wm-1/a.jsonl", &[(eid(1), 1), (eid(1), 1)]).await;
        let repl = orig(&s, "orders/compacted/r.jsonl", &[(eid(1), 1)]).await;
        assert!(matches!(
            verify_jsonl(s.as_ref(), &[o1], &repl).await,
            Err(EquivalenceError::NotEquivalent(_))
        ));
    }

    #[tokio::test]
    async fn missing_event_id_fails_closed() {
        let s = store();
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&canonical_json_bytes(
            &serde_json::json!({"v": 1}),
        ));
        bytes.push(b'\n');
        let o1 = put_obj(&s, "orders/wm-1/a.jsonl", bytes).await;
        let repl = orig(&s, "orders/compacted/r.jsonl", &[(eid(1), 1)]).await;
        assert!(matches!(
            verify_jsonl(s.as_ref(), &[o1], &repl).await,
            Err(EquivalenceError::Integrity(_))
        ));
    }

    #[tokio::test]
    async fn malformed_event_id_fails_closed() {
        let s = store();
        let o1 = orig(
            &s,
            "orders/wm-1/a.jsonl",
            &[("not-a-valid-event-id".into(), 1)],
        )
        .await;
        let repl = orig(&s, "orders/compacted/r.jsonl", &[(eid(1), 1)]).await;
        assert!(matches!(
            verify_jsonl(s.as_ref(), &[o1], &repl).await,
            Err(EquivalenceError::Integrity(_))
        ));
    }

    #[tokio::test]
    async fn non_jsonl_is_unsupported() {
        let s = store();
        let o1 = orig(&s, "orders/wm-1/a.jsonl", &[(eid(1), 1)]).await;
        let mut repl =
            orig(&s, "orders/compacted/r.jsonl", &[(eid(1), 1)]).await;
        repl.format = "parquet".into();
        assert!(matches!(
            verify_jsonl(s.as_ref(), &[o1], &repl).await,
            Err(EquivalenceError::Unsupported(_))
        ));
    }

    #[tokio::test]
    async fn domain_mismatch_is_unsupported() {
        let s = store();
        let mut o1 = orig(&s, "orders/wm-1/a.jsonl", &[(eid(1), 1)]).await;
        o1.compression = "zstd".into(); // differs from the replacement domain
        let repl = orig(&s, "orders/compacted/r.jsonl", &[(eid(1), 1)]).await;
        assert!(matches!(
            verify_jsonl(s.as_ref(), &[o1], &repl).await,
            Err(EquivalenceError::Unsupported(_))
        ));
    }

    #[tokio::test]
    async fn tampered_original_bytes_fail_closed() {
        let s = store();
        let mut o1 = orig(&s, "orders/wm-1/a.jsonl", &[(eid(1), 1)]).await;
        o1.content_hash = "deadbeef".into(); // no longer matches stored bytes
        let repl = orig(&s, "orders/compacted/r.jsonl", &[(eid(1), 1)]).await;
        assert!(matches!(
            verify_jsonl(s.as_ref(), &[o1], &repl).await,
            Err(EquivalenceError::Integrity(_))
        ));
    }

    #[tokio::test]
    async fn validator_is_independent_of_a_wrong_compactor_output() {
        // A "buggy compactor" produced a replacement that silently drops a row; an
        // independent decode+compare (not re-running the compactor) still catches it.
        let s = store();
        let o1 = orig(
            &s,
            "orders/wm-1/a.jsonl",
            &[(eid(1), 1), (eid(2), 2), (eid(3), 3)],
        )
        .await;
        let buggy =
            orig(&s, "orders/compacted/r.jsonl", &[(eid(1), 1), (eid(3), 3)])
                .await;
        assert!(matches!(
            verify_jsonl(s.as_ref(), &[o1], &buggy).await,
            Err(EquivalenceError::NotEquivalent(_))
        ));
    }
}
