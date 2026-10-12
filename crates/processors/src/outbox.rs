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

use deltaforge_core::canonical::Document;
use deltaforge_core::canonical::view::row_view;
use deltaforge_core::{
    BatchContext, CanonicalRow, CanonicalValue, Event, Op, Processor,
};

// Config types live in deltaforge-config to avoid circular deps
use deltaforge_config::{
    OUTBOX_SCHEMA_SENTINEL, OutboxColumns, OutboxProcessorCfg,
};

// =============================================================================
// Processor
// =============================================================================

pub struct OutboxProcessor {
    id: String,
    pipeline: String,
    cfg: OutboxProcessorCfg,
    table_filter: AllowList,
    topic_template: Option<CompiledTemplate>,
    key_template: Option<CompiledTemplate>,
    digest: String,
}

/// Bump when a code change alters this processor's output for identical config.
const OUTBOX_IMPL_VERSION: u32 = 2;

impl OutboxProcessor {
    pub fn new(cfg: OutboxProcessorCfg, pipeline: String) -> Result<Self> {
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
        let digest =
            crate::digest::builtin_digest("outbox", OUTBOX_IMPL_VERSION, &cfg);

        Ok(Self {
            id,
            pipeline,
            cfg,
            table_filter,
            topic_template,
            key_template,
            digest,
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

    /// Transform an outbox event in place.
    /// Returns Ok(true) to keep, Ok(false) to drop, Err to fail the batch.
    fn transform(&self, event: &mut Event) -> Result<bool> {
        if !matches!(event.op, Op::Create) {
            warn!(
                table = %event.source.table,
                op = ?event.op,
                "outbox: dropping non-INSERT event"
            );
            counter!("deltaforge_outbox_dropped_total", "pipeline" => self.pipeline.clone(), "processor" => self.id.clone(), "reason" => "non_insert").increment(1);
            return Ok(false);
        }

        let mut row = match event.after.take() {
            Some(row) => row,
            None => {
                warn!(
                    table = %event.source.table,
                    "outbox: dropping event with null payload"
                );
                counter!("deltaforge_outbox_dropped_total", "pipeline" => self.pipeline.clone(), "processor" => self.id.clone(), "reason" => "null_payload").increment(1);
                return Ok(false);
            }
        };

        let cols = &self.cfg.columns;
        // Routing fields are read through the path view of the row (exact
        // integers, JSON columns as documents).
        let after = Value::Object(row_view(&row, event.type_mapping));

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

        // Strict mode: fail the batch so operators notice and fix the schema
        if self.cfg.strict {
            let has_payload = after.get(&cols.payload).is_some();
            let mut missing = Vec::new();
            if topic.is_none() {
                missing.push("topic");
            }
            if !has_payload {
                missing.push("payload");
            }
            if aggregate_type.is_none() {
                missing.push("aggregate_type");
            }
            if aggregate_id.is_none() {
                missing.push("aggregate_id");
            }
            if event_type.is_none() {
                missing.push("event_type");
            }
            if !missing.is_empty() {
                counter!("deltaforge_outbox_dropped_total", "pipeline" => self.pipeline.clone(), "processor" => self.id.clone(), "reason" => "strict_missing_fields").increment(1);
                anyhow::bail!(
                    "outbox strict: event from table '{}' missing required fields: {:?}",
                    event.source.table,
                    missing
                );
            }
        }

        let payload = row.remove(cols.payload.as_str());

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

        event.after = Some(match payload {
            Some(p) => payload_row(&cols.payload, p, event.type_mapping),
            // No payload column: the remaining columns, as documents.
            None => CanonicalRow::new(
                row_view(&row, event.type_mapping)
                    .into_iter()
                    .map(|(k, v)| {
                        (k, CanonicalValue::Document(Document::new(v)))
                    })
                    .collect(),
            )
            .expect("view keys are unique"),
        });
        event.before = None;
        event.source.schema = None;
        // The row is the payload now, not a row of the outbox table: it
        // names no schema version.
        event.schema_version = None;
        event.schema_sequence = None;

        counter!("deltaforge_outbox_transformed_total", "pipeline" => self.pipeline.clone(), "processor" => self.id.clone()).increment(1);
        Ok(true)
    }

    /// Key resolution: template -> aggregate_id fallback.
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

    /// Topic resolution cascade: template -> column -> default.
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
            // Resolve directly against the raw payload - no remapping.
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

/// The event's row after the transform, explicit schema-less data (the row
/// is no longer the outbox table's, so typed sinks never read it as its
/// columns): a payload that is a JSON object (a JSON column or a document)
/// becomes its fields, each an untyped document; any other payload is one
/// column, `name`, holding it as a document (its path view: exact integers,
/// a JSON column as its document), or as bytes for logical-message bytes.
fn payload_row(
    name: &str,
    payload: CanonicalValue,
    mapping: deltaforge_core::TypeMapping,
) -> CanonicalRow {
    let json = match &payload {
        CanonicalValue::Bytes(_) => None,
        CanonicalValue::Document(d) => Some(d.value().clone()),
        other => deltaforge_core::canonical::view::value_view(other, mapping),
    };
    match json {
        Some(Value::Object(fields)) => CanonicalRow::new(
            fields
                .into_iter()
                .map(|(k, v)| (k, CanonicalValue::Document(Document::new(v))))
                .collect(),
        )
        .expect("JSON object keys are unique"),
        Some(other) => CanonicalRow::new(vec![(
            name.to_string(),
            CanonicalValue::Document(Document::new(other)),
        )])
        .expect("one column"),
        None => CanonicalRow::new(vec![(name.to_string(), payload)])
            .expect("one column"),
    }
}

#[async_trait]
impl Processor for OutboxProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn identity_digest(&self) -> &str {
        &self.digest
    }

    async fn process(
        &self,
        events: Vec<Event>,
        _ctx: &BatchContext,
    ) -> Result<Vec<Event>> {
        let events_in = events.len() as u64;
        counter!(
            "deltaforge_processor_events_in_total",
            "pipeline" => self.pipeline.clone(),
            "processor" => self.id.clone(),
        )
        .increment(events_in);

        let mut out = Vec::with_capacity(events.len());
        for mut event in events {
            if self.should_process(&event) {
                if self.transform(&mut event)? {
                    out.push(event);
                }
                // else: dropped (non-insert / malformed outbox event)
            } else {
                out.push(event); // pass through
            }
        }

        counter!(
            "deltaforge_processor_events_out_total",
            "pipeline" => self.pipeline.clone(),
            "processor" => self.id.clone(),
        )
        .increment(out.len() as u64);

        Ok(out)
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use deltaforge_core::canonical::DbValue;
    use deltaforge_core::canonical::{JsonText, fixture};
    use deltaforge_core::{SourceInfo, TypeMapping};
    use serde_json::json;

    fn outbox_event(table: &str, after: Value) -> Event {
        outbox_row_event(table, fixture::row(after))
    }

    fn outbox_row_event(table: &str, after: CanonicalRow) -> Event {
        Event::new_row(
            deltaforge_core::EventId::mysql_row_server(1, "t", 1, 0),
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
            TypeMapping::V2,
            1000,
            100,
        )
    }

    fn table_event(table: &str) -> Event {
        Event::new_row(
            deltaforge_core::EventId::mysql_row_server(1, "t", 1, 0),
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
            Some(fixture::row(json!({"id": 1}))),
            TypeMapping::V2,
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
            strict: false,
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
        let proc = OutboxProcessor::new(default_cfg(), String::new()).unwrap();
        let events = vec![outbox_event("outbox", outbox_payload())];
        let ctx = BatchContext::from_batch(&events);
        let result = proc.process(events, &ctx).await.unwrap();

        assert_eq!(result.len(), 1);
        let ev = &result[0];
        // The document payload's fields become the row's columns.
        let after = ev.after.as_ref().unwrap();
        assert_eq!(after.len(), 2);
        assert_eq!(
            after.get("order_id"),
            Some(&CanonicalValue::Document(Document::new(json!(42))))
        );
        assert_eq!(
            after.get("total"),
            Some(&CanonicalValue::Document(Document::new(json!(99.99))))
        );
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
        let proc = OutboxProcessor::new(cfg, String::new()).unwrap();
        let events = vec![outbox_event("outbox", outbox_payload())];
        let ctx = BatchContext::from_batch(&events);
        let result = proc.process(events, &ctx).await.unwrap();
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
        let proc = OutboxProcessor::new(cfg, String::new()).unwrap();

        let payload = json!({
            "domain": "orders",
            "entity_id": "42",
            "action": "created",
            "payload": {"total": 99.99}
        });
        let events = vec![outbox_event("outbox", payload)];
        let ctx = BatchContext::from_batch(&events);
        let result = proc.process(events, &ctx).await.unwrap();

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
        let proc = OutboxProcessor::new(cfg, String::new()).unwrap();

        let events = vec![
            outbox_event("orders_outbox", outbox_payload()),
            outbox_event("payments_outbox", outbox_payload()),
        ];

        let ctx = BatchContext::from_batch(&events);

        let result = proc.process(events, &ctx).await.unwrap();

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
        let proc = OutboxProcessor::new(cfg, String::new()).unwrap();

        let events = vec![
            outbox_event("order_outbox", outbox_payload()),
            outbox_event("payment_outbox", outbox_payload()),
        ];

        let ctx = BatchContext::from_batch(&events);

        let result = proc.process(events, &ctx).await.unwrap();

        assert!(result[0].source.schema.is_none());
        assert_eq!(
            result[1].source.schema.as_deref(),
            Some(OUTBOX_SCHEMA_SENTINEL)
        );
    }

    #[tokio::test]
    async fn non_outbox_pass_through() {
        let proc = OutboxProcessor::new(default_cfg(), String::new()).unwrap();
        let events = vec![
            table_event("orders"),
            outbox_event("outbox", outbox_payload()),
            table_event("customers"),
        ];
        let ctx = BatchContext::from_batch(&events);
        let result = proc.process(events, &ctx).await.unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].source.table, "orders");
        assert_eq!(result[2].source.table, "customers");
    }

    #[tokio::test]
    async fn drops_non_insert_outbox() {
        let proc = OutboxProcessor::new(default_cfg(), String::new()).unwrap();
        let mut ev = outbox_event("outbox", outbox_payload());
        ev.op = Op::Delete;

        let events = vec![ev];

        let ctx = BatchContext::from_batch(&events);

        let result = proc.process(events, &ctx).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn topic_cascade() {
        let cfg = OutboxProcessorCfg {
            topic: Some("${nonexistent}".into()),
            ..default_cfg()
        };
        let proc = OutboxProcessor::new(cfg, String::new()).unwrap();

        // Has explicit topic column → uses it
        let events = vec![outbox_event(
            "outbox",
            json!({
                "aggregate_type": "X", "aggregate_id": "1", "event_type": "Y",
                "topic": "explicit", "payload": {}
            }),
        )];
        let ctx = BatchContext::from_batch(&events);
        let result = proc.process(events, &ctx).await.unwrap();
        assert_eq!(
            result[0].routing.as_ref().unwrap().topic.as_deref(),
            Some("explicit")
        );

        // No topic column → falls to default
        let events = vec![outbox_event(
            "outbox",
            json!({
                "aggregate_type": "X", "aggregate_id": "1", "event_type": "Y",
                "payload": {}
            }),
        )];
        let ctx = BatchContext::from_batch(&events);
        let result = proc.process(events, &ctx).await.unwrap();
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
        let proc = OutboxProcessor::new(cfg, String::new()).unwrap();

        let payload = json!({
            "aggregate_type": "Order",
            "aggregate_id": "42",
            "event_type": "OrderCreated",
            "trace_id": "abc-123",
            "tenant": "acme",
            "payload": {"total": 99.99}
        });
        let events = vec![outbox_event("outbox", payload)];
        let ctx = BatchContext::from_batch(&events);
        let result = proc.process(events, &ctx).await.unwrap();

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
        let proc = OutboxProcessor::new(cfg, String::new()).unwrap();

        // payload has no trace_id field
        let events = vec![outbox_event("outbox", outbox_payload())];
        let ctx = BatchContext::from_batch(&events);
        let result = proc.process(events, &ctx).await.unwrap();

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
        let proc = OutboxProcessor::new(cfg, String::new()).unwrap();
        let events = vec![outbox_event("outbox", outbox_payload())];
        let ctx = BatchContext::from_batch(&events);
        let result = proc.process(events, &ctx).await.unwrap();

        let routing = result[0].routing.as_ref().unwrap();
        assert!(routing.raw_payload, "raw_payload flag should be set");
        assert_eq!(routing.effective_topic(), Some("Order.OrderCreated"));
    }

    #[tokio::test]
    async fn raw_payload_false_by_default() {
        let proc = OutboxProcessor::new(default_cfg(), String::new()).unwrap();
        let events = vec![outbox_event("outbox", outbox_payload())];
        let ctx = BatchContext::from_batch(&events);
        let result = proc.process(events, &ctx).await.unwrap();

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
        let proc = OutboxProcessor::new(cfg, String::new()).unwrap();

        let ev = outbox_event("outbox", outbox_payload());
        let events = vec![ev];
        let ctx = BatchContext::from_batch(&events);
        let out = proc.process(events, &ctx).await.unwrap();
        assert_eq!(out[0].routing.as_ref().unwrap().key.as_deref(), Some("42"));
    }

    #[tokio::test]
    async fn key_defaults_to_aggregate_id_when_no_template() {
        let proc = OutboxProcessor::new(default_cfg(), String::new()).unwrap();

        let ev = outbox_event("outbox", outbox_payload());
        let events = vec![ev];
        let ctx = BatchContext::from_batch(&events);
        let out = proc.process(events, &ctx).await.unwrap();
        assert_eq!(out[0].routing.as_ref().unwrap().key.as_deref(), Some("42"));
    }

    #[tokio::test]
    async fn composite_key_template() {
        let cfg = OutboxProcessorCfg {
            key: Some("${aggregate_type}:${aggregate_id}".into()),
            ..default_cfg()
        };
        let proc = OutboxProcessor::new(cfg, String::new()).unwrap();

        let ev = outbox_event("outbox", outbox_payload());
        let events = vec![ev];
        let ctx = BatchContext::from_batch(&events);
        let out = proc.process(events, &ctx).await.unwrap();
        assert_eq!(
            out[0].routing.as_ref().unwrap().key.as_deref(),
            Some("Order:42")
        );
    }

    #[tokio::test]
    async fn event_id_extracted_as_header() {
        let proc = OutboxProcessor::new(default_cfg(), String::new()).unwrap();

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
        let events = vec![ev];
        let ctx = BatchContext::from_batch(&events);
        let out = proc.process(events, &ctx).await.unwrap();
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
        let proc = OutboxProcessor::new(cfg, String::new()).unwrap();

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
        let events = vec![ev];
        let ctx = BatchContext::from_batch(&events);
        let out = proc.process(events, &ctx).await.unwrap();
        let headers =
            out[0].routing.as_ref().unwrap().headers.as_ref().unwrap();
        assert_eq!(headers.get("df-event-id").unwrap(), "550e8400-e29b-41d4");
    }

    #[tokio::test]
    async fn numeric_ids_stringified_in_headers() {
        let proc = OutboxProcessor::new(default_cfg(), String::new()).unwrap();

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
        let events = vec![ev];
        let ctx = BatchContext::from_batch(&events);
        let out = proc.process(events, &ctx).await.unwrap();
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
        let proc = OutboxProcessor::new(cfg, String::new()).unwrap();

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
        let events = vec![ev];
        let ctx = BatchContext::from_batch(&events);
        let out = proc.process(events, &ctx).await.unwrap();
        let headers =
            out[0].routing.as_ref().unwrap().headers.as_ref().unwrap();
        assert_eq!(headers.get("x-priority").unwrap(), "true");
    }

    #[tokio::test]
    async fn non_insert_outbox_event_dropped() {
        let proc = OutboxProcessor::new(default_cfg(), String::new()).unwrap();

        let mut ev = outbox_event("outbox", outbox_payload());
        ev.op = Op::Update;

        let events = vec![ev];

        let ctx = BatchContext::from_batch(&events);

        let out = proc.process(events, &ctx).await.unwrap();
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn null_payload_event_dropped() {
        let proc = OutboxProcessor::new(default_cfg(), String::new()).unwrap();

        let mut ev = outbox_event("outbox", outbox_payload());
        ev.after = None;

        let events = vec![ev];

        let ctx = BatchContext::from_batch(&events);

        let out = proc.process(events, &ctx).await.unwrap();
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn non_object_payload_is_one_document_column() {
        let proc = OutboxProcessor::new(default_cfg(), String::new()).unwrap();

        let ev = outbox_event(
            "outbox",
            json!({
                "aggregate_type": "Order",
                "aggregate_id": "1",
                "event_type": "Created",
                "payload": "just a string"
            }),
        );
        let events = vec![ev];
        let ctx = BatchContext::from_batch(&events);
        let out = proc.process(events, &ctx).await.unwrap();
        assert_eq!(out.len(), 1);
        let after = out[0].after.as_ref().unwrap();
        assert_eq!(after.len(), 1);
        assert_eq!(
            after.get("payload"),
            Some(&CanonicalValue::Document(Document::new(json!(
                "just a string"
            ))))
        );
        // The row is no longer the outbox table's: no schema reference,
        // while the mapping version and identity stay.
        assert_eq!(out[0].schema_version, None);
        assert_eq!(out[0].schema_sequence, None);
        assert_eq!(out[0].type_mapping, deltaforge_core::TypeMapping::V2);
        assert!(out[0].event_id.is_some());
    }

    #[tokio::test]
    async fn json_column_payload_object_becomes_document_columns() {
        let proc = OutboxProcessor::new(default_cfg(), String::new()).unwrap();

        let mut row = fixture::row(json!({
            "aggregate_type": "Order",
            "aggregate_id": "7",
            "event_type": "Created",
        }));
        row.set(
            "payload",
            CanonicalValue::Db(DbValue::Json(
                JsonText::new(r#"{"total": 5, "lines": [1, 2]}"#.into())
                    .unwrap(),
            )),
        );
        let events = vec![outbox_row_event("outbox", row)];
        let ctx = BatchContext::from_batch(&events);
        let out = proc.process(events, &ctx).await.unwrap();
        let after = out[0].after.as_ref().unwrap();
        let mut names: Vec<_> = after.names().collect();
        names.sort_unstable();
        assert_eq!(names, vec!["lines", "total"]);
        assert_eq!(
            after.get("total"),
            Some(&CanonicalValue::Document(Document::new(json!(5))))
        );
        assert_eq!(
            after.get("lines"),
            Some(&CanonicalValue::Document(Document::new(json!([1, 2]))))
        );
    }

    #[tokio::test]
    async fn json_column_payload_scalar_stays_one_document_column() {
        let proc = OutboxProcessor::new(default_cfg(), String::new()).unwrap();

        let mut row = fixture::row(json!({
            "aggregate_type": "Order",
            "aggregate_id": "7",
            "event_type": "Created",
        }));
        row.set(
            "payload",
            CanonicalValue::Db(DbValue::Json(
                JsonText::new("[1, 2]".into()).unwrap(),
            )),
        );
        let events = vec![outbox_row_event("outbox", row)];
        let ctx = BatchContext::from_batch(&events);
        let out = proc.process(events, &ctx).await.unwrap();
        let after = out[0].after.as_ref().unwrap();
        assert_eq!(after.len(), 1);
        assert_eq!(
            after.get("payload"),
            Some(&CanonicalValue::Document(Document::new(json!([1, 2]))))
        );
    }

    #[tokio::test]
    async fn wide_integer_ids_exact_in_headers() {
        let proc = OutboxProcessor::new(default_cfg(), String::new()).unwrap();

        // 2^53 + 1: not representable as a double.
        let ev = outbox_event(
            "outbox",
            json!({
                "id": 9_007_199_254_740_993_i64,
                "aggregate_type": "Order",
                "aggregate_id": 9_007_199_254_740_993_i64,
                "event_type": "Created",
                "payload": {}
            }),
        );
        let events = vec![ev];
        let ctx = BatchContext::from_batch(&events);
        let out = proc.process(events, &ctx).await.unwrap();
        let routing = out[0].routing.as_ref().unwrap();
        let headers = routing.headers.as_ref().unwrap();
        assert_eq!(headers.get("df-event-id").unwrap(), "9007199254740993");
        assert_eq!(headers.get("df-aggregate-id").unwrap(), "9007199254740993");
        assert_eq!(routing.key.as_deref(), Some("9007199254740993"));
    }

    #[tokio::test]
    async fn provenance_header_set_on_transformed_events() {
        let proc = OutboxProcessor::new(default_cfg(), String::new()).unwrap();

        let ev = outbox_event("outbox", outbox_payload());
        let events = vec![ev];
        let ctx = BatchContext::from_batch(&events);
        let out = proc.process(events, &ctx).await.unwrap();
        let headers =
            out[0].routing.as_ref().unwrap().headers.as_ref().unwrap();
        assert_eq!(headers.get("df-source-kind").unwrap(), "outbox");
    }

    // =========================================================================
    // Strict mode
    // =========================================================================

    fn strict_cfg() -> OutboxProcessorCfg {
        OutboxProcessorCfg {
            strict: true,
            topic: Some("${aggregate_type}.${event_type}".into()),
            default_topic: None,
            ..default_cfg()
        }
    }

    #[tokio::test]
    async fn strict_passes_complete_event() {
        let proc = OutboxProcessor::new(strict_cfg(), String::new()).unwrap();

        let ev = outbox_event("outbox", outbox_payload());
        let events = vec![ev];
        let ctx = BatchContext::from_batch(&events);
        let out = proc.process(events, &ctx).await.unwrap();
        assert_eq!(out.len(), 1);
    }

    #[tokio::test]
    async fn strict_fails_batch_on_missing_aggregate_type() {
        let proc = OutboxProcessor::new(strict_cfg(), String::new()).unwrap();

        let ev = outbox_event(
            "outbox",
            json!({
                "aggregate_id": "42",
                "event_type": "OrderCreated",
                "payload": {"total": 99.99}
            }),
        );
        let events = vec![ev];
        let ctx = BatchContext::from_batch(&events);
        let err = proc.process(events, &ctx).await.unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("aggregate_type"),
            "error should name missing field: {msg}"
        );
    }

    #[tokio::test]
    async fn strict_fails_batch_on_missing_payload_column() {
        let proc = OutboxProcessor::new(strict_cfg(), String::new()).unwrap();

        let ev = outbox_event(
            "outbox",
            json!({
                "aggregate_type": "Order",
                "aggregate_id": "42",
                "event_type": "OrderCreated"
                // no "payload" key
            }),
        );
        let events = vec![ev];
        let ctx = BatchContext::from_batch(&events);
        let err = proc.process(events, &ctx).await.unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("payload"),
            "error should name missing field: {msg}"
        );
    }

    #[tokio::test]
    async fn strict_fails_batch_when_topic_unresolvable() {
        let cfg = OutboxProcessorCfg {
            topic: Some("${nonexistent}".into()),
            ..strict_cfg()
        };
        let proc = OutboxProcessor::new(cfg, String::new()).unwrap();

        let ev = outbox_event("outbox", outbox_payload());
        let events = vec![ev];
        let ctx = BatchContext::from_batch(&events);
        let err = proc.process(events, &ctx).await.unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("topic"),
            "error should name missing field: {msg}"
        );
    }

    #[tokio::test]
    async fn strict_reports_all_missing_fields() {
        let cfg = OutboxProcessorCfg {
            topic: Some("${nonexistent}".into()),
            ..strict_cfg()
        };
        let proc = OutboxProcessor::new(cfg, String::new()).unwrap();

        let ev = outbox_event("outbox", json!({"unrelated": true}));
        let events = vec![ev];
        let ctx = BatchContext::from_batch(&events);
        let err = proc.process(events, &ctx).await.unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("topic"), "{msg}");
        assert!(msg.contains("payload"), "{msg}");
        assert!(msg.contains("aggregate_type"), "{msg}");
    }

    #[tokio::test]
    async fn lenient_passes_missing_fields() {
        let proc = OutboxProcessor::new(default_cfg(), String::new()).unwrap();

        let ev = outbox_event(
            "outbox",
            json!({
                "aggregate_id": "42",
                "event_type": "OrderCreated",
                "payload": {"total": 99.99}
            }),
        );
        let events = vec![ev];
        let ctx = BatchContext::from_batch(&events);
        let out = proc.process(events, &ctx).await.unwrap();
        assert_eq!(out.len(), 1);
    }
}
