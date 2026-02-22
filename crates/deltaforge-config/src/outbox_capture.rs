//! Source-level outbox capture configuration.
//!
//! Each source type uses native terminology:
//! - PostgreSQL: `prefixes` - matched against pg_logical_emit_message prefix
//! - MySQL: `tables` - matched against binlog table name using AllowList globs
//!
//! Pattern syntax reuses the existing AllowList from `common::patterns`:
//! - `*.outbox` - outbox table in every database
//! - `shop.order_outbox_%` - prefix match within a database
//! - `outbox` - unqualified, matches any qualifier
//!
//! The source tags matching events with `source.schema = "__outbox"`.

use common::AllowList;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Sentinel schema value set by the source on outbox-captured events.
pub const OUTBOX_SCHEMA_SENTINEL: &str = "__outbox";

/// PostgreSQL outbox capture config.
///
/// ```yaml
/// source:
///   type: postgres
///   config:
///     outbox:
///       prefixes: [outbox, order_outbox_%]
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgOutboxCapture {
    /// Message prefixes to capture. Matched against
    /// `pg_logical_emit_message(true, '<prefix>', ...)`.
    ///
    /// Supports AllowList glob patterns (flat, no qualifier):
    /// - `outbox` — exact match
    /// - `outbox_%` — prefix match
    /// - `*` — capture all WAL messages as outbox events
    pub prefixes: Vec<String>,
}

impl PgOutboxCapture {
    /// Build an AllowList for prefix matching.
    pub fn allow_list(&self) -> AllowList {
        AllowList::new(&self.prefixes)
    }

    /// Check if a message prefix matches the capture config.
    pub fn matches(&self, prefix: &str) -> bool {
        self.allow_list().matches_name(prefix)
    }
}

/// MySQL outbox capture config.
///
/// ```yaml
/// source:
///   type: mysql
///   config:
///     outbox:
///       tables: ["*.outbox", "payments.payment_outbox"]
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MysqlOutboxCapture {
    /// Table patterns to capture. Uses AllowList glob syntax:
    /// - `*.outbox` - outbox table in every database
    /// - `shop.outbox` - exact match
    /// - `%.order_outbox_%` - prefix match across databases
    pub tables: Vec<String>,
}

impl MysqlOutboxCapture {
    /// Build an AllowList for table matching.
    pub fn allow_list(&self) -> AllowList {
        AllowList::new(&self.tables)
    }

    /// Check if a db.table combination matches the capture config.
    pub fn matches(&self, db: &str, table: &str) -> bool {
        self.allow_list().matches(db, table)
    }
}

// =============================================================================
// Processor Config (lives here so ProcessorCfg can reference it)
// =============================================================================

/// Outbox processor config - referenced by `ProcessorCfg::Outbox`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboxProcessorCfg {
    /// Processor identifier.
    #[serde(default = "default_id")]
    pub id: String,

    /// Optional filter: only process outbox events whose `source.table`
    /// matches these patterns. Uses AllowList glob syntax.
    /// If empty/omitted, processes all `__outbox` events.
    #[serde(default)]
    pub tables: Vec<String>,

    /// Column name mappings into the outbox JSON payload.
    #[serde(default)]
    pub columns: OutboxColumns,

    /// Topic template resolved against extracted outbox fields using `${...}`.
    #[serde(default)]
    pub topic: Option<String>,

    /// Fallback topic when template resolution fails or is not configured.
    #[serde(default)]
    pub default_topic: Option<String>,

    /// Key template resolved against the raw payload using `${...}`.
    /// Sets `routing.key` for sink partitioning.
    /// Example: `"${aggregate_id}"` routes by aggregate.
    /// If omitted, falls back to `columns.aggregate_id` when present.
    #[serde(default)]
    pub key: Option<String>,

    /// Forward additional payload fields as routing headers.
    /// Key = header name, value = column name in the outbox payload.
    /// Example: `{"x-trace-id": "trace_id"}` extracts `trace_id` from
    /// the payload and sets it as the `x-trace-id` header.
    #[serde(default)]
    pub additional_headers: HashMap<String, String>,

    /// When true, the outbox payload is delivered as-is to sinks,
    /// bypassing envelope wrapping (native/debezium/cloudevents).
    /// Metadata is still available via routing headers.
    #[serde(default)]
    pub raw_payload: bool,
}

fn default_id() -> String {
    "outbox".into()
}

/// Column name mappings for extracting outbox fields from `event.after`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboxColumns {
    #[serde(default = "default_payload")]
    pub payload: String,
    #[serde(default = "default_aggregate_type")]
    pub aggregate_type: String,
    #[serde(default = "default_aggregate_id")]
    pub aggregate_id: String,
    #[serde(default = "default_event_type")]
    pub event_type: String,
    #[serde(default = "default_topic_col")]
    pub topic: String,
    /// Column for event identity (extracted as `df-event-id` header).
    #[serde(default = "default_event_id")]
    pub event_id: String,
}

impl Default for OutboxColumns {
    fn default() -> Self {
        Self {
            payload: default_payload(),
            aggregate_type: default_aggregate_type(),
            aggregate_id: default_aggregate_id(),
            event_type: default_event_type(),
            topic: default_topic_col(),
            event_id: default_event_id(),
        }
    }
}

fn default_payload() -> String {
    "payload".into()
}
fn default_aggregate_type() -> String {
    "aggregate_type".into()
}
fn default_aggregate_id() -> String {
    "aggregate_id".into()
}
fn default_event_type() -> String {
    "event_type".into()
}
fn default_topic_col() -> String {
    "topic".into()
}
fn default_event_id() -> String {
    "id".into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_exact_prefix() {
        let cap = PgOutboxCapture {
            prefixes: vec!["outbox".into()],
        };
        assert!(cap.matches("outbox"));
        assert!(!cap.matches("audit"));
    }

    #[test]
    fn pg_glob_prefix() {
        let cap = PgOutboxCapture {
            prefixes: vec!["outbox_%".into()],
        };
        assert!(cap.matches("outbox_orders"));
        assert!(cap.matches("outbox_payments"));
        assert!(!cap.matches("outbox"));
        assert!(!cap.matches("audit"));
    }

    #[test]
    fn pg_multiple_prefixes() {
        let cap = PgOutboxCapture {
            prefixes: vec!["outbox".into(), "events_%".into()],
        };
        assert!(cap.matches("outbox"));
        assert!(cap.matches("events_orders"));
        assert!(!cap.matches("audit"));
    }

    #[test]
    fn mysql_wildcard_db() {
        let cap = MysqlOutboxCapture {
            tables: vec!["*.outbox".into()],
        };
        assert!(cap.matches("db1", "outbox"));
        assert!(cap.matches("db2", "outbox"));
        assert!(!cap.matches("db1", "orders"));
    }

    #[test]
    fn mysql_exact_table() {
        let cap = MysqlOutboxCapture {
            tables: vec!["shop.outbox".into()],
        };
        assert!(cap.matches("shop", "outbox"));
        assert!(!cap.matches("other", "outbox"));
    }

    #[test]
    fn mysql_mixed_patterns() {
        let cap = MysqlOutboxCapture {
            tables: vec!["*.outbox".into(), "payments.payment_outbox".into()],
        };
        assert!(cap.matches("db1", "outbox"));
        assert!(cap.matches("payments", "payment_outbox"));
        assert!(!cap.matches("payments", "orders"));
    }
}
