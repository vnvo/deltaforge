//! Debezium envelope - full `{"payload": ...}` wrapper.
//!
//! Some Debezium consumers expect the outer envelope structure.
//! This adds minimal overhead (~12 bytes) around the native Event.

use bytes::Bytes;
use serde::Serialize;

use super::{Envelope, EnvelopeError};
use crate::Event;

/// Debezium envelope - wraps Event in `{"payload": <event>}`.
///
/// Output:
/// ```json
/// {
///   "payload": {
///     "before": null,
///     "after": {"id": 1, "name": "Alice"},
///     "source": { ... },
///     "op": "c",
///     "ts_ms": 1700000000000
///   }
/// }
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct Debezium;

/// Wrapper struct for serialization.
#[derive(Serialize)]
struct DebeziumWrapper<'a> {
    payload: &'a Event,
}

impl Envelope for Debezium {
    fn name(&self) -> &'static str {
        "debezium"
    }

    #[inline]
    fn serialize(&self, event: &Event) -> Result<Bytes, EnvelopeError> {
        let wrapper = DebeziumWrapper { payload: event };
        let bytes = serde_json::to_vec(&wrapper)?;
        Ok(Bytes::from(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Op, SourceInfo, SourcePosition};
    use serde_json::json;

    #[test]
    fn debezium_wraps_in_payload() {
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

        let envelope = Debezium;
        let bytes = envelope.serialize(&event).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

        // Verify wrapper structure
        assert!(json["payload"].is_object());
        assert_eq!(json["payload"]["op"], "c");
        assert_eq!(json["payload"]["source"]["connector"], "mysql");
    }
}
