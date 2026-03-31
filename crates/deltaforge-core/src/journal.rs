//! Journal entry types for the DLQ and future replay/audit streams.
//!
//! These types define the on-disk and REST API representation of journal
//! entries. The `JournalEntry` is the shared envelope; stream-specific
//! metadata is stored in the `meta` field as typed JSON.

use serde::{Deserialize, Serialize};

/// A single journal entry. Shared across all stream types (DLQ, replay, audit).
///
/// Stored as serialized JSON in the StorageBackend queue/log primitives.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalEntry {
    /// Monotonic sequence number, per (namespace, key), assigned by storage backend.
    /// Increasing but not necessarily gapless. Stable across reads within the
    /// same backend; not portable across backends.
    pub seq: u64,

    /// When the entry was written (unix epoch seconds).
    pub timestamp: i64,

    /// Pipeline name.
    pub pipeline: String,

    /// Stream name (e.g., "dlq").
    pub stream: String,

    /// Event ID (UUID v7). Stable across replays, usable for dedup.
    /// If the source event did not include a stable ID, the coordinator
    /// assigns one before journal write.
    pub event_id: String,

    /// Source cursor — structured position in the source's change stream.
    /// Examples: `{"file":"mysql-bin.000005","pos":12345}`, `{"lsn":"0/16B3748"}`.
    pub source_cursor: Option<serde_json::Value>,

    /// The original event payload. May be truncated if it exceeds max_event_bytes.
    pub event: serde_json::Value,

    /// True if the event payload was truncated due to size limits.
    pub payload_truncated: bool,

    /// Stream-type-specific metadata (e.g., DlqMeta for DLQ entries).
    pub meta: serde_json::Value,
}

impl JournalEntry {
    /// Truncate `event.before` and `event.after` fields if the serialized
    /// event exceeds `max_bytes`. Preserves all other event metadata.
    /// Sets `payload_truncated = true` if truncation occurred.
    pub fn truncate_payload(&mut self, max_bytes: usize) {
        let current_size = serde_json::to_vec(&self.event)
            .map(|v| v.len())
            .unwrap_or(0);

        if current_size <= max_bytes {
            return;
        }

        // Truncate the large payload fields, keep metadata intact.
        if let Some(obj) = self.event.as_object_mut() {
            for field in &["before", "after"] {
                if let Some(val) = obj.get_mut(*field) {
                    let truncated_msg = format!(
                        "<truncated: original payload exceeded {} bytes>",
                        max_bytes
                    );
                    *val = serde_json::Value::String(truncated_msg);
                }
            }
        }
        self.payload_truncated = true;
    }
}

/// DLQ-specific metadata. Stored in the `meta` field of DLQ journal entries.
///
/// Use `to_json()` when writing and `from_json()` when reading to avoid
/// manipulating `meta` as raw JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqMeta {
    /// Which sink failed.
    pub sink_id: String,

    /// Error classification (e.g., "serialization", "routing", "message_too_large").
    pub error_kind: String,

    /// Human-readable error message.
    pub error_message: String,

    /// Number of delivery attempts before routing to DLQ.
    pub attempts: u32,
}

impl DlqMeta {
    /// Serialize to JSON Value for storage in `JournalEntry.meta`.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or_default()
    }

    /// Deserialize from `JournalEntry.meta`.
    pub fn from_json(value: &serde_json::Value) -> Option<Self> {
        serde_json::from_value(value.clone()).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn dlq_meta_roundtrip() {
        let meta = DlqMeta {
            sink_id: "kafka-primary".into(),
            error_kind: "serialization".into(),
            error_message: "invalid UTF-8".into(),
            attempts: 3,
        };
        let json = meta.to_json();
        let recovered = DlqMeta::from_json(&json).unwrap();
        assert_eq!(recovered.sink_id, "kafka-primary");
        assert_eq!(recovered.attempts, 3);
    }

    #[test]
    fn truncate_payload_preserves_metadata() {
        let mut entry = JournalEntry {
            seq: 0,
            timestamp: 0,
            pipeline: "test".into(),
            stream: "dlq".into(),
            event_id: "abc".into(),
            source_cursor: None,
            event: json!({
                "id": "abc",
                "op": "c",
                "source": {"db": "test", "table": "t"},
                "before": null,
                "after": {"id": 1, "data": "x".repeat(1000)}
            }),
            payload_truncated: false,
            meta: json!({}),
        };

        entry.truncate_payload(100);

        assert!(entry.payload_truncated);
        // Metadata fields preserved
        assert_eq!(entry.event["id"], "abc");
        assert_eq!(entry.event["op"], "c");
        // Payload fields truncated
        assert!(entry.event["after"].as_str().unwrap().contains("truncated"));
    }

    #[test]
    fn truncate_noop_when_under_limit() {
        let mut entry = JournalEntry {
            seq: 0,
            timestamp: 0,
            pipeline: "test".into(),
            stream: "dlq".into(),
            event_id: "abc".into(),
            source_cursor: None,
            event: json!({"id": 1, "after": {"x": "small"}}),
            payload_truncated: false,
            meta: json!({}),
        };

        entry.truncate_payload(10000);

        assert!(!entry.payload_truncated);
        assert_eq!(entry.event["after"]["x"], "small");
    }
}
