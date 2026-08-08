//! Build the NDJSON `_bulk` request body and parse the per-item response.
//!
//! Each action uses `version_type=external` with the source-derived version, so
//! ES keeps the highest version per `_id`. A per-item `409 version_conflict`
//! means the doc was already superseded by a newer version — success, not a
//! failure.

use deltaforge_core::SinkError;
use serde_json::{Value, json};

/// One document operation. `doc: None` is a delete; `Some` is an index/upsert.
pub struct BulkAction {
    pub index: String,
    pub id: String,
    pub version: u64,
    pub doc: Option<Value>,
}

/// Outcome of a single bulk item, aligned by position with the input actions.
#[derive(Debug, PartialEq, Eq)]
pub enum ItemOutcome {
    /// Applied.
    Ok,
    /// Rejected as a stale version (`409 version_conflict`) — already superseded.
    Superseded,
    /// Non-retryable per-document error (e.g. mapping conflict).
    Failed(String),
}

/// Build the NDJSON `_bulk` body: an action line per item, plus a document line
/// for non-deletes. Ends with a trailing newline (required by ES).
pub fn build_bulk_body(actions: &[BulkAction]) -> Vec<u8> {
    let mut out = String::new();
    for a in actions {
        let meta = json!({
            "_index": a.index,
            "_id": a.id,
            "version": a.version,
            "version_type": "external",
        });
        if let Some(doc) = &a.doc {
            out.push_str(&json!({ "index": meta }).to_string());
            out.push('\n');
            out.push_str(&doc.to_string());
            out.push('\n');
        } else {
            out.push_str(&json!({ "delete": meta }).to_string());
            out.push('\n');
        }
    }
    out.into_bytes()
}

/// Parse a `_bulk` response body into one `ItemOutcome` per item (in order).
pub fn parse_bulk_response(body: &[u8]) -> Result<Vec<ItemOutcome>, SinkError> {
    let v: Value =
        serde_json::from_slice(body).map_err(|e| SinkError::Serialization {
            details: format!("elasticsearch bulk response parse: {e}").into(),
        })?;
    let items = v.get("items").and_then(|i| i.as_array()).ok_or_else(|| {
        SinkError::Serialization {
            details: "elasticsearch bulk response missing 'items'".into(),
        }
    })?;

    let mut outcomes = Vec::with_capacity(items.len());
    for item in items {
        // Each item is a single-key object: {"index": {...}} or {"delete": {...}}.
        let action = item
            .as_object()
            .and_then(|o| o.values().next())
            .ok_or_else(|| SinkError::Serialization {
                details: "elasticsearch bulk item malformed".into(),
            })?;
        let status = action.get("status").and_then(|s| s.as_u64()).unwrap_or(0);
        outcomes.push(match status {
            200..=299 => ItemOutcome::Ok,
            // Delete of a missing doc is fine (already absent).
            404 => ItemOutcome::Ok,
            409 => ItemOutcome::Superseded,
            _ => {
                let err = action.get("error");
                let kind = err
                    .and_then(|e| e.get("type"))
                    .and_then(|t| t.as_str())
                    .unwrap_or("unknown");
                let reason = err
                    .and_then(|e| e.get("reason"))
                    .and_then(|r| r.as_str())
                    .unwrap_or("");
                ItemOutcome::Failed(format!("{status} {kind}: {reason}"))
            }
        });
    }
    Ok(outcomes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn builds_index_and_delete_lines() {
        let acts = vec![
            BulkAction {
                index: "i".into(),
                id: "1".into(),
                version: 5,
                doc: Some(json!({"id": 1, "name": "a"})),
            },
            BulkAction {
                index: "i".into(),
                id: "2".into(),
                version: 6,
                doc: None,
            },
        ];
        let body = String::from_utf8(build_bulk_body(&acts)).unwrap();
        assert!(body.ends_with('\n'));
        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(lines.len(), 3); // index-action + doc + delete-action

        let a0: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(a0["index"]["_index"], "i");
        assert_eq!(a0["index"]["_id"], "1");
        assert_eq!(a0["index"]["version"], 5);
        assert_eq!(a0["index"]["version_type"], "external");

        let d0: Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(d0["name"], "a");

        let a1: Value = serde_json::from_str(lines[2]).unwrap();
        assert_eq!(a1["delete"]["_id"], "2");
        assert_eq!(a1["delete"]["version_type"], "external");
    }

    #[test]
    fn parses_success_conflict_and_failure() {
        let resp = json!({
            "errors": true,
            "items": [
                {"index": {"status": 200}},
                {"index": {"status": 409, "error": {"type": "version_conflict_engine_exception", "reason": "old"}}},
                {"index": {"status": 400, "error": {"type": "mapper_parsing_exception", "reason": "bad"}}},
                {"delete": {"status": 404}}
            ]
        })
        .to_string();
        let out = parse_bulk_response(resp.as_bytes()).unwrap();
        assert_eq!(out[0], ItemOutcome::Ok);
        assert_eq!(out[1], ItemOutcome::Superseded);
        match &out[2] {
            ItemOutcome::Failed(m) => {
                assert!(m.contains("mapper_parsing_exception"), "{m}");
                assert!(m.contains("bad"), "{m}");
            }
            other => panic!("expected Failed, got {other:?}"),
        }
        assert_eq!(out[3], ItemOutcome::Ok, "delete-missing is not a failure");
    }

    #[test]
    fn errors_on_missing_items() {
        assert!(parse_bulk_response(b"{}").is_err());
        assert!(parse_bulk_response(b"not json").is_err());
    }
}
