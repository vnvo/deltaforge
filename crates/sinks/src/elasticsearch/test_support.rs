//! Shared `#[cfg(test)]` helpers for the elasticsearch submodule tests.

use deltaforge_core::{Event, Op, SourceInfo, SourcePosition};
use serde_json::Value;

/// Build a minimal `Event`. `after`/`before` of `Value::Null` become `None`.
/// `schema` sets the PG schema namespace; `lsn` sets the source position.
#[allow(clippy::too_many_arguments)]
pub(crate) fn mk_event(
    op: Op,
    after: Value,
    before: Value,
    db: &str,
    schema: Option<&str>,
    table: &str,
    ts_ms: i64,
    lsn: Option<&str>,
) -> Event {
    Event {
        before: if before.is_null() { None } else { Some(before) },
        after: if after.is_null() { None } else { Some(after) },
        source: SourceInfo {
            version: "1".into(),
            connector: "mysql".into(),
            name: "t".into(),
            ts_ms,
            db: db.into(),
            schema: schema.map(String::from),
            table: table.into(),
            snapshot: None,
            position: SourcePosition {
                lsn: lsn.map(String::from),
                ..Default::default()
            },
        },
        op,
        ts_ms,
        transaction: None,
        event_id: None,
        tenant_id: None,
        schema_version: None,
        schema_sequence: None,
        ddl: None,
        trace_id: None,
        tags: None,
        synthetic: None,
        routing: None,
        tx_end: false,
        boundary: None,
        size_bytes: 0,

        received_at_ms: 0,
    }
}
