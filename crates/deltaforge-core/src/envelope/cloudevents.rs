//! CloudEvents envelope - restructures to CloudEvents 1.0 specification.
//!
//! CloudEvents is a common format for serverless/event-driven architectures.
//! This envelope restructures the Event to match the CloudEvents spec.

use serde::Serialize;

use super::{Envelope, EnvelopeData, EnvelopeError};
use crate::{Event, NativeOpts, Op, TypeMapping};

/// CloudEvents envelope - restructures Event to CloudEvents 1.0.
///
/// Output:
/// ```json
/// {
///   "specversion": "1.0",
///   "id": "event-uuid",
///   "source": "deltaforge/prod-db/inventory.customers",
///   "type": "com.example.cdc.created",
///   "time": "2024-01-15T10:30:00.000Z",
///   "datacontenttype": "application/json",
///   "data": {
///     "before": null,
///     "after": {"id": 1, "name": "Alice"}
///   }
/// }
/// ```
#[derive(Debug, Clone)]
pub struct CloudEvents {
    type_prefix: String,
}

impl CloudEvents {
    pub fn new(type_prefix: impl Into<String>) -> Self {
        Self {
            type_prefix: type_prefix.into(),
        }
    }
}

/// CloudEvents wire format.
#[derive(Serialize)]
pub struct CloudEventsWrapper<'a> {
    specversion: &'static str,
    id: String,
    source: String,
    #[serde(rename = "type")]
    event_type: String,
    time: String,
    datacontenttype: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    subject: Option<String>,
    data: CloudEventsData,
    #[serde(skip)]
    _event: std::marker::PhantomData<&'a Event>,
}

#[derive(Serialize)]
pub(crate) struct CloudEventsData {
    #[serde(skip_serializing_if = "Option::is_none")]
    before: Option<serde_json::Map<String, serde_json::Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    after: Option<serde_json::Map<String, serde_json::Value>>,
    op: &'static str,
    /// The mapping version the values are rendered with.
    type_mapping: TypeMapping,
    /// After-image columns PostgreSQL did not send (unchanged TOAST).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    unchanged_columns: Vec<String>,
}

impl Envelope for CloudEvents {
    fn name(&self) -> &'static str {
        "cloudevents"
    }

    fn wrap<'a>(
        &'a self,
        event: &'a Event,
        opts: NativeOpts,
    ) -> Result<EnvelopeData<'a>, EnvelopeError> {
        let rows = event.native(opts)?;
        let op_suffix = match event.op {
            Op::Create => "created",
            Op::Update => "updated",
            Op::Delete => "deleted",
            Op::Read => "snapshot",
            Op::Truncate => "truncated",
        };

        let wrapper = CloudEventsWrapper {
            specversion: "1.0",
            // No random fallback: a stable EventId is the only id source. An
            // id-less event must never reach an envelope (enforced upstream).
            id: event.event_id.map(|id| id.to_string()).unwrap_or_default(),
            source: format!(
                "deltaforge/{}/{}",
                event.source.name,
                event.source.full_table_name()
            ),
            event_type: format!("{}.{}", self.type_prefix, op_suffix),
            time: chrono::DateTime::from_timestamp_millis(event.ts_ms)
                .unwrap_or_else(chrono::Utc::now)
                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            datacontenttype: "application/json",
            subject: Some(event.source.full_table_name()),
            data: CloudEventsData {
                before: rows.before,
                after: rows.after,
                op: event.op.as_str(),
                type_mapping: event.type_mapping,
                unchanged_columns: rows.unchanged_columns,
            },
            _event: std::marker::PhantomData,
        };

        Ok(EnvelopeData::CloudEvents(wrapper))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SourceInfo, SourcePosition};
    use serde_json::json;

    #[test]
    fn cloudevents_structure() {
        let event = Event::new_row(
            crate::EventId::mysql_row_server(1, "t", 1, 0),
            SourceInfo {
                version: "test".into(),
                connector: "mysql".into(),
                name: "prod-db".into(),
                ts_ms: 1700000000000,
                db: "inventory".into(),
                schema: None,
                table: "customers".into(),
                snapshot: None,
                position: SourcePosition::default(),
            },
            Op::Create,
            None,
            Some(crate::canonical::fixture::row(
                json!({"id": 1, "name": "Alice"}),
            )),
            crate::TypeMapping::V2,
            1700000000000,
            64,
        );

        let envelope = CloudEvents::new("com.example.cdc");
        let data = envelope.wrap(&event, crate::NativeOpts::default()).unwrap();
        let json = serde_json::to_value(&data).unwrap();

        // Verify CloudEvents structure
        assert_eq!(json["specversion"], "1.0");
        assert!(json["id"].is_string());
        assert_eq!(json["source"], "deltaforge/prod-db/inventory.customers");
        assert_eq!(json["type"], "com.example.cdc.created");
        assert_eq!(json["datacontenttype"], "application/json");

        // Verify data payload
        assert_eq!(json["data"]["op"], "c");
        assert_eq!(json["data"]["after"]["name"], "Alice");
    }
}
