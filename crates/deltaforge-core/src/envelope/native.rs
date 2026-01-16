//! Native envelope - direct Event serialization.
//!
//! Since Event is already Debezium-compatible at the payload level,
//! this is the most efficient option for consumers that don't need
//! the outer `{"payload": ...}` wrapper.

use bytes::Bytes;

use super::{Envelope, EnvelopeError};
use crate::Event;

/// Native envelope - serializes Event directly.
///
/// Output matches Debezium's payload structure:
/// ```json
/// {
///   "before": null,
///   "after": {"id": 1, "name": "Alice"},
///   "source": { ... },
///   "op": "c",
///   "ts_ms": 1700000000000
/// }
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct Native;

impl Envelope for Native {
    fn name(&self) -> &'static str {
        "native"
    }

    #[inline]
    fn serialize(&self, event: &Event) -> Result<Bytes, EnvelopeError> {
        let bytes = serde_json::to_vec(event)?;
        Ok(Bytes::from(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Op, SourceInfo, SourcePosition};
    use serde_json::json;

    #[test]
    fn native_produces_debezium_payload() {
        let event = Event::new_row(
            SourceInfo {
                version: "test".into(),
                connector: "mysql".into(),
                name: "test-db".into(),
                ts_ms: 1000,
                db: "testdb".into(),
                schema: None,
                table: "users".into(),
                snapshot: None,
                position: SourcePosition::default(),
            },
            Op::Create,
            None,
            Some(json!({"id": 1})),
            1000,
            64,
        );

        let envelope = Native;
        let bytes = envelope.serialize(&event).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(json["op"], "c");
        assert_eq!(json["source"]["connector"], "mysql");
        assert!(json.get("payload").is_none()); // No wrapper
    }
}
