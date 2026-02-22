//! Outbox processor - transforms outbox-captured events and routes them.
//!
//! Explicit processor in the pipeline's `processors` list. Identifies events
//! tagged by the source with `source.schema = "__outbox"`, extracts outbox
//! fields, resolves destination topic via compiled template, and rewrites
//! the event for downstream sinks.
//!
//! # Multi-outbox support
//!
//! When a source captures multiple outbox channels, use the `tables` filter
//! to scope each processor instance:
//!
//! ```yaml
//! processors:
//!   - type: outbox
//!     config:
//!       tables: ["orders_outbox"]
//!       topic: "orders.${event_type}"
//!   - type: outbox
//!     config:
//!       tables: ["payments_outbox"]
//!       topic: "payments.${event_type}"
//! ```
//!
//! Without `tables`, the processor handles all `__outbox` events,
//! essentially relying on the source semantics for identifying the outbox messages.

use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use common::{AllowList, routing::CompiledTemplate};
use metrics::counter;
use serde_json::Value;
use tracing::{debug, warn};

use deltaforge_core::{Event, Op, Processor};

// Config types live in deltaforge-config to avoid circular deps
use deltaforge_config::{
    OUTBOX_SCHEMA_SENTINEL, OutboxColumns, OutboxProcessorCfg,
};

// =============================================================================
// Processor
// =============================================================================

pub struct OutboxProcessor {
    id: String,
    cfg: OutboxProcessorCfg,
    table_filter: AllowList,
    topic_template: Option<CompiledTemplate>,
    key_template: Option<CompiledTemplate>,
}

impl OutboxProcessor {
    pub fn new(cfg: OutboxProcessorCfg) -> Result<Self> {
        let topic_template = cfg
            .topic
            .as_ref()
            .map(|t| CompiledTemplate::parse(t))
            .transpose()
            .map_err(|e| {
                anyhow::anyhow!("invalid outbox topic template: {e}")
            })?;

        let key_template = cfg
            .key
            .as_ref()
            .map(|k| CompiledTemplate::parse(k))
            .transpose()
            .map_err(|e| anyhow::anyhow!("invalid outbox key template: {e}"))?;

        let table_filter = AllowList::new(&cfg.tables);
        let id = cfg.id.clone();

        Ok(Self {
            id,
            cfg,
            table_filter,
            topic_template,
            key_template,
        })
    }

    /// Check if an event is an outbox event that this processor should handle.
    fn should_process(&self, event: &Event) -> bool {
        if event.source.schema.as_deref() != Some(OUTBOX_SCHEMA_SENTINEL) {
            return false;
        }
        if self.table_filter.is_empty() {
            return true;
        }
        self.table_filter.matches_name(&event.source.table)
    }

    /// Extract a scalar value as a string. Handles string, number, bool.
    fn extract_scalar(obj: &Value, field: &str) -> Option<String> {
        match obj.get(field)? {
            Value::String(s) => Some(s.clone()),
            Value::Number(n) => Some(n.to_string()),
            Value::Bool(b) => Some(b.to_string()),
            _ => None,
        }
    }

    /// Transform an outbox event in place. Returns false to drop.
    fn transform(&self, event: &mut Event) -> bool {
        if !matches!(event.op, Op::Create) {
            warn!(
                table = %event.source.table,
                op = ?event.op,
                "outbox: dropping non-INSERT event"
            );
            counter!("deltaforge_outbox_dropped_total", "reason" => "non_insert").increment(1);
            return false;
        }

        let mut after = match event.after.take() {
            Some(v) if v.is_object() => v,
            Some(v) => {
                warn!(
                    table = %event.source.table,
                    value_type = %value_type_name(&v),
                    "outbox: dropping event with non-object payload"
                );
                counter!("deltaforge_outbox_dropped_total", "reason" => "non_object").increment(1);
                event.after = Some(v);
                return false;
            }
            None => {
                warn!(
                    table = %event.source.table,
                    "outbox: dropping event with null payload"
                );
                counter!("deltaforge_outbox_dropped_total", "reason" => "null_payload").increment(1);
                return false;
            }
        };

        let cols = &self.cfg.columns;

        // All immutable borrows of `after` in this block
        let topic = self.resolve_topic(&after, cols);
        let key = self.resolve_key(&after, cols);
        let event_id = Self::extract_scalar(&after, &cols.event_id);
        let aggregate_type = Self::extract_scalar(&after, &cols.aggregate_type);
        let aggregate_id = Self::extract_scalar(&after, &cols.aggregate_id);
        let event_type = Self::extract_scalar(&after, &cols.event_type);
        let additional: Vec<(String, String)> = self
            .cfg
            .additional_headers
            .iter()
            .filter_map(|(header_name, col_name)| {
                Self::extract_scalar(&after, col_name)
                    .map(|v| (header_name.clone(), v))
            })
            .collect();
        // — all immutable borrows released —

        // Mutable: move payload out instead of cloning
        let payload = after
            .as_object_mut()
            .and_then(|obj| obj.remove(cols.payload.as_str()));

        let mut routing = event.routing.take().unwrap_or_default();
        if let Some(t) = topic {
            routing.topic = Some(t);
        }
        if let Some(k) = key {
            routing.key = Some(k);
        }
        if self.cfg.raw_payload {
            routing.raw_payload = true;
        }

        let headers = routing.headers.get_or_insert_with(|| {
            HashMap::with_capacity(5 + additional.len())
        });

        // Debug first (borrows), then move into headers
        debug!(
            aggregate_type = ?aggregate_type,
            aggregate_id = ?aggregate_id,
            event_type = ?event_type,
            event_id = ?event_id,
            "outbox event transformed"
        );
        if let Some(v) = event_id {
            headers.insert("df-event-id".into(), v);
        }
        if let Some(v) = aggregate_type {
            headers.insert("df-aggregate-type".into(), v);
        }
        if let Some(v) = aggregate_id {
            headers.insert("df-aggregate-id".into(), v);
        }
        if let Some(v) = event_type {
            headers.insert("df-event-type".into(), v);
        }
        for (name, val) in additional {
            headers.insert(name, val);
        }
        headers.insert("df-source-kind".into(), "outbox".into());
        event.routing = Some(routing);

        event.after = Some(payload.unwrap_or(after));
        event.before = None;
        event.source.schema = None;

        counter!("deltaforge_outbox_transformed_total").increment(1);
        true
    }

    /// Key resolution: template → aggregate_id fallback.
    fn resolve_key(
        &self,
        after: &Value,
        cols: &OutboxColumns,
    ) -> Option<String> {
        if let Some(ref tpl) = self.key_template {
            let resolved = tpl.resolve_lenient(after);
            if !resolved.is_empty() {
                return Some(resolved);
            }
        }
        // Default: use aggregate_id as key when no template configured
        Self::extract_scalar(after, &cols.aggregate_id)
    }

    /// Topic resolution cascade: template → column → default.
    ///
    /// The template resolves against the raw `event.after` payload, so users
    /// can reference their actual column names (e.g. `${domain}.${action}`)
    /// without going through column mappings.
    fn resolve_topic(
        &self,
        after: &Value,
        cols: &OutboxColumns,
    ) -> Option<String> {
        if let Some(ref tpl) = self.topic_template {
            if tpl.is_static() {
                return Some(tpl.resolve_lenient(after));
            }
            // Resolve directly against the raw payload — no remapping.
            let resolved = tpl.resolve_lenient(after);
            if !resolved.is_empty() {
                return Some(resolved);
            }
        }

        if let Some(t) = Self::extract_scalar(after, &cols.topic) {
            if !t.is_empty() {
                return Some(t);
            }
        }

        self.cfg.default_topic.clone()
    }
}

/// Returns a human-readable JSON type name for diagnostics.
fn value_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

#[async_trait]
impl Processor for OutboxProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    async fn process(&self, events: Vec<Event>) -> Result<Vec<Event>> {
        let mut out = Vec::with_capacity(events.len());
        for mut event in events {
            if self.should_process(&event) {
                if self.transform(&mut event) {
                    out.push(event);
                }
                // else: dropped (non-insert outbox event)
            } else {
                out.push(event); // pass through
            }
        }
        Ok(out)
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use deltaforge_core::SourceInfo;
    use serde_json::json;

    fn outbox_event(table: &str, after: Value) -> Event {
        Event::new_row(
            SourceInfo {
                version: "test".into(),
                connector: "postgresql".into(),
                name: "pipe".into(),
                ts_ms: 1000,
                db: "db".into(),
                schema: Some(OUTBOX_SCHEMA_SENTINEL.into()),
                table: table.into(),
                snapshot: None,
                position: Default::default(),
            },
            Op::Create,
            None,
            Some(after),
            1000,
            100,
        )
    }

    fn table_event(table: &str) -> Event {
        Event::new_row(
            SourceInfo {
                version: "test".into(),
                connector: "mysql".into(),
                name: "pipe".into(),
                ts_ms: 1000,
                db: "db".into(),
                schema: None,
                table: table.into(),
                snapshot: None,
                position: Default::default(),
            },
            Op::Create,
            None,
            Some(json!({"id": 1})),
            1000,
            50,
        )
    }

    fn default_cfg() -> OutboxProcessorCfg {
        OutboxProcessorCfg {
            id: "outbox".into(),
            tables: vec![],
            columns: OutboxColumns::default(),
            topic: None,
            default_topic: Some("events.default".into()),
            key: None,
            additional_headers: HashMap::new(),
            raw_payload: false,
        }
    }

    fn outbox_payload() -> Value {
        json!({
            "aggregate_type": "Order",
            "aggregate_id": "42",
            "event_type": "OrderCreated",
            "payload": {"order_id": 42, "total": 99.99}
        })
    }

    #[tokio::test]
    async fn transforms_outbox_event() {
        let proc = OutboxProcessor::new(default_cfg()).unwrap();
        let result = proc
            .process(vec![outbox_event("outbox", outbox_payload())])
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        let ev = &result[0];
        assert_eq!(ev.after.as_ref().unwrap()["order_id"], 42);
        assert_eq!(
            ev.routing.as_ref().unwrap().topic.as_deref(),
            Some("events.default")
        );
        assert_eq!(
            ev.routing
                .as_ref()
                .unwrap()
                .headers
                .as_ref()
                .unwrap()
                .get("df-aggregate-type")
                .unwrap(),
            "Order"
        );
        assert!(ev.source.schema.is_none());
    }

    #[tokio::test]
    async fn template_topic() {
        let cfg = OutboxProcessorCfg {
            topic: Some("${aggregate_type}.${event_type}".into()),
            ..default_cfg()
        };
        let proc = OutboxProcessor::new(cfg).unwrap();
        let result = proc
            .process(vec![outbox_event("outbox", outbox_payload())])
            .await
            .unwrap();
        assert_eq!(
            result[0].routing.as_ref().unwrap().topic.as_deref(),
            Some("Order.OrderCreated")
        );
    }

    #[tokio::test]
    async fn custom_schema_topic_template() {
        // User has non-standard column names and references them directly in the template
        let cfg = OutboxProcessorCfg {
            topic: Some("${domain}.${action}".into()),
            columns: OutboxColumns {
                aggregate_type: "domain".into(),
                aggregate_id: "entity_id".into(),
                event_type: "action".into(),
                ..OutboxColumns::default()
            },
            ..default_cfg()
        };
        let proc = OutboxProcessor::new(cfg).unwrap();

        let payload = json!({
            "domain": "orders",
            "entity_id": "42",
            "action": "created",
            "payload": {"total": 99.99}
        });
        let result = proc
            .process(vec![outbox_event("outbox", payload)])
            .await
            .unwrap();

        // Template resolves against raw payload columns
        assert_eq!(
            result[0].routing.as_ref().unwrap().topic.as_deref(),
            Some("orders.created")
        );
        // Headers still use standard df- names, extracted via column mappings
        let headers = result[0]
            .routing
            .as_ref()
            .unwrap()
            .headers
            .as_ref()
            .unwrap();
        assert_eq!(headers.get("df-aggregate-type").unwrap(), "orders");
        assert_eq!(headers.get("df-aggregate-id").unwrap(), "42");
        assert_eq!(headers.get("df-event-type").unwrap(), "created");
    }

    #[tokio::test]
    async fn table_filter_scopes_processing() {
        let cfg = OutboxProcessorCfg {
            tables: vec!["orders_outbox".into()],
            ..default_cfg()
        };
        let proc = OutboxProcessor::new(cfg).unwrap();

        let result = proc
            .process(vec![
                outbox_event("orders_outbox", outbox_payload()),
                outbox_event("payments_outbox", outbox_payload()),
            ])
            .await
            .unwrap();

        assert_eq!(result.len(), 2);
        assert!(result[0].source.schema.is_none()); // transformed
        assert_eq!(
            result[1].source.schema.as_deref(),
            Some(OUTBOX_SCHEMA_SENTINEL)
        ); // untouched
    }

    #[tokio::test]
    async fn glob_table_filter() {
        let cfg = OutboxProcessorCfg {
            tables: vec!["order_%".into()],
            ..default_cfg()
        };
        let proc = OutboxProcessor::new(cfg).unwrap();

        let result = proc
            .process(vec![
                outbox_event("order_outbox", outbox_payload()),
                outbox_event("payment_outbox", outbox_payload()),
            ])
            .await
            .unwrap();

        assert!(result[0].source.schema.is_none());
        assert_eq!(
            result[1].source.schema.as_deref(),
            Some(OUTBOX_SCHEMA_SENTINEL)
        );
    }

    #[tokio::test]
    async fn non_outbox_pass_through() {
        let proc = OutboxProcessor::new(default_cfg()).unwrap();
        let result = proc
            .process(vec![
                table_event("orders"),
                outbox_event("outbox", outbox_payload()),
                table_event("customers"),
            ])
            .await
            .unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].source.table, "orders");
        assert_eq!(result[2].source.table, "customers");
    }

    #[tokio::test]
    async fn drops_non_insert_outbox() {
        let proc = OutboxProcessor::new(default_cfg()).unwrap();
        let mut ev = outbox_event("outbox", outbox_payload());
        ev.op = Op::Delete;

        let result = proc.process(vec![ev]).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn topic_cascade() {
        let cfg = OutboxProcessorCfg {
            topic: Some("${nonexistent}".into()),
            ..default_cfg()
        };
        let proc = OutboxProcessor::new(cfg).unwrap();

        // Has explicit topic column → uses it
        let result = proc.process(vec![outbox_event("outbox", json!({
            "aggregate_type": "X", "aggregate_id": "1", "event_type": "Y",
            "topic": "explicit", "payload": {}
        }))]).await.unwrap();
        assert_eq!(
            result[0].routing.as_ref().unwrap().topic.as_deref(),
            Some("explicit")
        );

        // No topic column → falls to default
        let result = proc.process(vec![outbox_event("outbox", json!({
            "aggregate_type": "X", "aggregate_id": "1", "event_type": "Y",
            "payload": {}
        }))]).await.unwrap();
        assert_eq!(
            result[0].routing.as_ref().unwrap().topic.as_deref(),
            Some("events.default")
        );
    }

    #[tokio::test]
    async fn additional_headers_forwarded() {
        let cfg = OutboxProcessorCfg {
            additional_headers: HashMap::from([
                ("x-trace-id".into(), "trace_id".into()),
                ("x-tenant".into(), "tenant".into()),
            ]),
            ..default_cfg()
        };
        let proc = OutboxProcessor::new(cfg).unwrap();

        let payload = json!({
            "aggregate_type": "Order",
            "aggregate_id": "42",
            "event_type": "OrderCreated",
            "trace_id": "abc-123",
            "tenant": "acme",
            "payload": {"total": 99.99}
        });
        let result = proc
            .process(vec![outbox_event("outbox", payload)])
            .await
            .unwrap();

        let headers = result[0]
            .routing
            .as_ref()
            .unwrap()
            .headers
            .as_ref()
            .unwrap();
        assert_eq!(headers.get("x-trace-id").unwrap(), "abc-123");
        assert_eq!(headers.get("x-tenant").unwrap(), "acme");
        assert_eq!(headers.get("df-aggregate-type").unwrap(), "Order");
    }

    #[tokio::test]
    async fn additional_headers_missing_column_skipped() {
        let cfg = OutboxProcessorCfg {
            additional_headers: HashMap::from([(
                "x-trace-id".into(),
                "trace_id".into(),
            )]),
            ..default_cfg()
        };
        let proc = OutboxProcessor::new(cfg).unwrap();

        // payload has no trace_id field
        let result = proc
            .process(vec![outbox_event("outbox", outbox_payload())])
            .await
            .unwrap();

        let headers = result[0]
            .routing
            .as_ref()
            .unwrap()
            .headers
            .as_ref()
            .unwrap();
        assert!(
            !headers.contains_key("x-trace-id"),
            "missing column should be skipped"
        );
        assert!(
            headers.contains_key("df-aggregate-type"),
            "standard headers still set"
        );
    }

    #[tokio::test]
    async fn raw_payload_sets_routing_flag() {
        let cfg = OutboxProcessorCfg {
            raw_payload: true,
            topic: Some("${aggregate_type}.${event_type}".into()),
            ..default_cfg()
        };
        let proc = OutboxProcessor::new(cfg).unwrap();
        let result = proc
            .process(vec![outbox_event("outbox", outbox_payload())])
            .await
            .unwrap();

        let routing = result[0].routing.as_ref().unwrap();
        assert!(routing.raw_payload, "raw_payload flag should be set");
        assert_eq!(routing.effective_topic(), Some("Order.OrderCreated"));
    }

    #[tokio::test]
    async fn raw_payload_false_by_default() {
        let proc = OutboxProcessor::new(default_cfg()).unwrap();
        let result = proc
            .process(vec![outbox_event("outbox", outbox_payload())])
            .await
            .unwrap();

        let routing = result[0].routing.as_ref().unwrap();
        assert!(!routing.raw_payload, "raw_payload should default to false");
    }

    // =========================================================================
    // P0: key routing, event-id, typed extraction, drop diagnostics
    // =========================================================================

    #[tokio::test]
    async fn key_template_sets_routing_key() {
        let cfg = OutboxProcessorCfg {
            key: Some("${aggregate_id}".into()),
            ..default_cfg()
        };
        let proc = OutboxProcessor::new(cfg).unwrap();

        let ev = outbox_event("outbox", outbox_payload());
        let out = proc.process(vec![ev]).await.unwrap();
        assert_eq!(out[0].routing.as_ref().unwrap().key.as_deref(), Some("42"));
    }

    #[tokio::test]
    async fn key_defaults_to_aggregate_id_when_no_template() {
        let proc = OutboxProcessor::new(default_cfg()).unwrap();

        let ev = outbox_event("outbox", outbox_payload());
        let out = proc.process(vec![ev]).await.unwrap();
        assert_eq!(out[0].routing.as_ref().unwrap().key.as_deref(), Some("42"));
    }

    #[tokio::test]
    async fn composite_key_template() {
        let cfg = OutboxProcessorCfg {
            key: Some("${aggregate_type}:${aggregate_id}".into()),
            ..default_cfg()
        };
        let proc = OutboxProcessor::new(cfg).unwrap();

        let ev = outbox_event("outbox", outbox_payload());
        let out = proc.process(vec![ev]).await.unwrap();
        assert_eq!(
            out[0].routing.as_ref().unwrap().key.as_deref(),
            Some("Order:42")
        );
    }

    #[tokio::test]
    async fn event_id_extracted_as_header() {
        let proc = OutboxProcessor::new(default_cfg()).unwrap();

        let ev = outbox_event(
            "outbox",
            json!({
                "id": "evt-abc-123",
                "aggregate_type": "Order",
                "aggregate_id": "1",
                "event_type": "Created",
                "payload": {}
            }),
        );
        let out = proc.process(vec![ev]).await.unwrap();
        let headers =
            out[0].routing.as_ref().unwrap().headers.as_ref().unwrap();
        assert_eq!(headers.get("df-event-id").unwrap(), "evt-abc-123");
    }

    #[tokio::test]
    async fn event_id_custom_column() {
        let cfg = OutboxProcessorCfg {
            columns: OutboxColumns {
                event_id: "uuid".into(),
                ..Default::default()
            },
            ..default_cfg()
        };
        let proc = OutboxProcessor::new(cfg).unwrap();

        let ev = outbox_event(
            "outbox",
            json!({
                "uuid": "550e8400-e29b-41d4",
                "aggregate_type": "Order",
                "aggregate_id": "1",
                "event_type": "Created",
                "payload": {}
            }),
        );
        let out = proc.process(vec![ev]).await.unwrap();
        let headers =
            out[0].routing.as_ref().unwrap().headers.as_ref().unwrap();
        assert_eq!(headers.get("df-event-id").unwrap(), "550e8400-e29b-41d4");
    }

    #[tokio::test]
    async fn numeric_ids_stringified_in_headers() {
        let proc = OutboxProcessor::new(default_cfg()).unwrap();

        let ev = outbox_event(
            "outbox",
            json!({
                "id": 12345,
                "aggregate_type": "Order",
                "aggregate_id": 42,
                "event_type": "Created",
                "payload": {}
            }),
        );
        let out = proc.process(vec![ev]).await.unwrap();
        let headers =
            out[0].routing.as_ref().unwrap().headers.as_ref().unwrap();
        assert_eq!(headers.get("df-event-id").unwrap(), "12345");
        assert_eq!(headers.get("df-aggregate-id").unwrap(), "42");
        // key also falls back to aggregate_id
        assert_eq!(out[0].routing.as_ref().unwrap().key.as_deref(), Some("42"));
    }

    #[tokio::test]
    async fn boolean_additional_header_stringified() {
        let cfg = OutboxProcessorCfg {
            additional_headers: HashMap::from([(
                "x-priority".into(),
                "is_priority".into(),
            )]),
            ..default_cfg()
        };
        let proc = OutboxProcessor::new(cfg).unwrap();

        let ev = outbox_event(
            "outbox",
            json!({
                "aggregate_type": "Order",
                "aggregate_id": "1",
                "event_type": "Created",
                "is_priority": true,
                "payload": {}
            }),
        );
        let out = proc.process(vec![ev]).await.unwrap();
        let headers =
            out[0].routing.as_ref().unwrap().headers.as_ref().unwrap();
        assert_eq!(headers.get("x-priority").unwrap(), "true");
    }

    #[tokio::test]
    async fn non_insert_outbox_event_dropped() {
        let proc = OutboxProcessor::new(default_cfg()).unwrap();

        let mut ev = outbox_event("outbox", outbox_payload());
        ev.op = Op::Update;

        let out = proc.process(vec![ev]).await.unwrap();
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn null_payload_event_dropped() {
        let proc = OutboxProcessor::new(default_cfg()).unwrap();

        let mut ev = outbox_event("outbox", outbox_payload());
        ev.after = None;

        let out = proc.process(vec![ev]).await.unwrap();
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn non_object_payload_dropped() {
        let proc = OutboxProcessor::new(default_cfg()).unwrap();

        let ev = outbox_event("outbox", json!("just a string"));
        let out = proc.process(vec![ev]).await.unwrap();
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn provenance_header_set_on_transformed_events() {
        let proc = OutboxProcessor::new(default_cfg()).unwrap();

        let ev = outbox_event("outbox", outbox_payload());
        let out = proc.process(vec![ev]).await.unwrap();
        let headers =
            out[0].routing.as_ref().unwrap().headers.as_ref().unwrap();
        assert_eq!(headers.get("df-source-kind").unwrap(), "outbox");
    }
}
