//! Debezium envelope - full `{"payload": ...}` wrapper.
//!
//! Some Debezium consumers expect the outer envelope structure.
//! This adds minimal overhead (~12 bytes) around the native Event.

use serde::Serialize;

use super::{Envelope, EnvelopeData, EnvelopeError};
use crate::{Event, NativeOpts, NativePayload};

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
pub struct DebeziumWrapper<'a> {
    schema: serde_json::Value,
    payload: NativePayload<'a>,
}

impl<'a> DebeziumWrapper<'a> {
    pub(crate) fn row_placeholders(&self) -> DebeziumWrapper<'a> {
        DebeziumWrapper {
            schema: self.schema.clone(),
            payload: self.payload.row_placeholders(),
        }
    }
}

impl Envelope for Debezium {
    fn name(&self) -> &'static str {
        "debezium"
    }

    #[inline]
    fn wrap<'a>(
        &'a self,
        event: &'a Event,
        opts: NativeOpts,
    ) -> Result<EnvelopeData<'a>, EnvelopeError> {
        let wrapper = DebeziumWrapper {
            schema: serde_json::Value::Null,
            payload: event.native(opts)?,
        };
        Ok(EnvelopeData::Debezium(wrapper))
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
            crate::EventId::mysql_row_server(1, "t", 1, 0),
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
            Some(crate::canonical::fixture::row(json!({"id": 1}))),
            crate::TypeMapping::V2,
            1000,
            64,
        );

        let envelope = Debezium;
        let data = envelope.wrap(&event, crate::NativeOpts::default()).unwrap();
        let json = serde_json::to_value(&data).unwrap();

        // Verify wrapper structure
        assert!(json["schema"].is_null());
        assert!(json["payload"].is_object());
        assert_eq!(json["payload"]["op"], "c");
        assert_eq!(json["payload"]["source"]["connector"], "mysql");
    }
}
