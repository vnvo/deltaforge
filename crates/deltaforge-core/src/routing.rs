//! Event routing metadata for dynamic sink destinations.
//!
//! Routing hints control where individual events land — which topic/stream/subject
//! and which message key — based on event content. Set by processors, outbox transforms,
//! or built-in routing rules. Sinks use these to override their static defaults.
//!
//! # Wire Visibility
//!
//! `EventRouting` is serializable so processors (including JS) can read/write it
//! naturally via `serde_json::to_value(&event)`. However, envelopes exclude routing
//! from wire output — consumers never see routing metadata.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Routing overrides for sink destinations.
///
/// When present on an [`Event`](crate::Event), sinks use these fields to override
/// their static config. Resolution order (first non-empty wins):
///
/// 1. `routing.topic` — explicit override (outbox, processor)
/// 2. Template in sink config — resolved per-event from event fields
/// 3. Static sink config — when no templates are present
///
/// # Empty String Handling
///
/// `topic: Some("")` is treated as "no override" — falls through to template or
/// static config. Only `Some(non_empty_string)` counts as an override. This prevents
/// JS processors from accidentally routing to an empty topic.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct EventRouting {
    /// Override sink destination (topic/stream/subject).
    /// Empty string is treated as "no override".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,

    /// Override message key (for partitioning / ordering).
    /// - Kafka: used as the record key
    /// - Redis: added as "df-key" field on stream entry
    /// - NATS: stored in header (no native key concept)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,

    /// Additional message headers (string key-value pairs).
    /// - Kafka: mapped to `OwnedHeaders` (native)
    /// - NATS: mapped to native headers
    /// - Redis: serialized as JSON in "df-headers" field
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<HashMap<String, String>>,

    /// When true, sinks serialize `event.after` directly as the wire payload,
    /// bypassing envelope wrapping. Set by the outbox processor when
    /// `raw_payload: true` is configured.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub raw_payload: bool,
}

impl EventRouting {
    /// Returns the topic override if it's a non-empty string.
    #[inline]
    pub fn effective_topic(&self) -> Option<&str> {
        self.topic.as_deref().filter(|t| !t.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_routing_is_empty() {
        let r = EventRouting::default();
        assert!(r.topic.is_none());
        assert!(r.key.is_none());
        assert!(r.headers.is_none());
        assert!(r.effective_topic().is_none());
    }

    #[test]
    fn empty_topic_is_no_override() {
        let r = EventRouting {
            topic: Some(String::new()),
            ..Default::default()
        };
        assert!(r.effective_topic().is_none());
    }

    #[test]
    fn non_empty_topic_is_override() {
        let r = EventRouting {
            topic: Some("orders.priority".into()),
            ..Default::default()
        };
        assert_eq!(r.effective_topic(), Some("orders.priority"));
    }

    #[test]
    fn routing_roundtrips_through_json() {
        let r = EventRouting {
            topic: Some("my-topic".into()),
            key: Some("customer-42".into()),
            headers: Some(HashMap::from([(
                "trace-id".into(),
                "abc-123".into(),
            )])),
            raw_payload: false,
        };
        let json = serde_json::to_value(&r).unwrap();
        let r2: EventRouting = serde_json::from_value(json).unwrap();
        assert_eq!(r, r2);
    }

    #[test]
    fn none_fields_omitted_from_json() {
        let r = EventRouting {
            topic: Some("t".into()),
            ..Default::default()
        };
        let json = serde_json::to_string(&r).unwrap();
        assert!(!json.contains("key"));
        assert!(!json.contains("headers"));
    }
}
