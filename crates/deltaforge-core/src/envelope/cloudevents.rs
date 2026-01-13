//! CloudEvents envelope - restructures to CloudEvents 1.0 specification.
//!
//! CloudEvents is a common format for serverless/event-driven architectures.
//! This envelope restructures the Event to match the CloudEvents spec.

use bytes::Bytes;
use serde::Serialize;
use uuid::Uuid;

use super::{Envelope, EnvelopeError};
use crate::{Event, Op};

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
struct CloudEventsWrapper<'a> {
    specversion: &'static str,
    id: String,
    source: String,
    #[serde(rename = "type")]
    event_type: String,
    time: String,
    datacontenttype: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    subject: Option<String>,
    data: CloudEventsData<'a>,
}

#[derive(Serialize)]
struct CloudEventsData<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    before: Option<&'a serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    after: Option<&'a serde_json::Value>,
    op: &'static str,
}

impl Envelope for CloudEvents {
    fn name(&self) -> &'static str {
        "cloudevents"
    }

    fn serialize(&self, event: &Event) -> Result<Bytes, EnvelopeError> {
        let op_suffix = match event.op {
            Op::Create => "created",
            Op::Update => "updated",
            Op::Delete => "deleted",
            Op::Read => "snapshot",
            Op::Truncate => "truncated",
        };

        let wrapper = CloudEventsWrapper {
            specversion: "1.0",
            id: event
                .event_id
                .map(|u| u.to_string())
                .unwrap_or_else(|| Uuid::new_v4().to_string()),
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
                before: event.before.as_ref(),
                after: event.after.as_ref(),
                op: event.op.as_str(),
            },
        };

        let bytes = serde_json::to_vec(&wrapper)?;
        Ok(Bytes::from(bytes))
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
            Some(json!({"id": 1, "name": "Alice"})),
            1700000000000,
            64,
        );

        let envelope = CloudEvents::new("com.example.cdc");
        let bytes = envelope.serialize(&event).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

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
