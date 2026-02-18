//! PostgreSQL logical decoding message → DeltaForge Event.
//!
//! Converts `ReplicationEvent::Message` fields into synthetic Events.
//! When the message prefix matches the source's outbox AllowList, the event
//! is tagged with `__outbox` sentinel for the OutboxProcessor.
//! Otherwise uses `__wal_message` for generic passthrough.

use bytes::Bytes;
use common::AllowList;
use serde_json::Value;
use tracing::{debug, warn};

use deltaforge_core::{Event, Op, SourceInfo, SourcePosition};
use pgwire_replication::Lsn;

use deltaforge_config::OUTBOX_SCHEMA_SENTINEL;

/// Schema sentinel for non-outbox WAL messages.
pub const WAL_MESSAGE_SCHEMA: &str = "__wal_message";

/// Build a DeltaForge Event from a logical decoding message.
///
/// If `outbox_prefixes` is non-empty and the prefix matches,
/// the event is tagged with `__outbox` sentinel.
///
/// Returns `None` if content is not valid JSON.
pub fn to_event(
    prefix: &str,
    content: &Bytes,
    lsn: Lsn,
    pipeline_name: &str,
    database: &str,
    tx_id: Option<u32>,
    commit_time_micros: Option<i64>,
    outbox_prefixes: &AllowList,
) -> Option<Event> {
    let after: Value = match serde_json::from_slice(content) {
        Ok(v) => v,
        Err(e) => {
            warn!(prefix = %prefix, error = %e, "WAL message content is not valid JSON, skipping");
            return None;
        }
    };

    let is_outbox =
        !outbox_prefixes.is_empty() && outbox_prefixes.matches_name(prefix);
    let schema = if is_outbox {
        OUTBOX_SCHEMA_SENTINEL
    } else {
        WAL_MESSAGE_SCHEMA
    };

    let ts_ms = commit_time_micros
        .map(|us| {
            const PG_EPOCH_OFFSET_US: i64 = 946_684_800_000_000;
            (us + PG_EPOCH_OFFSET_US) / 1000
        })
        .unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64
        });

    let source = SourceInfo {
        version: env!("CARGO_PKG_VERSION").into(),
        connector: "postgresql".into(),
        name: pipeline_name.into(),
        ts_ms,
        db: database.into(),
        schema: Some(schema.into()),
        table: prefix.into(),
        snapshot: None,
        position: SourcePosition::default(),
    };

    debug!(prefix = %prefix, lsn = %lsn, tx_id = ?tx_id, schema = schema, "emitted WAL message event");

    Some(Event::new_row(
        source,
        Op::Create,
        None,
        Some(after),
        ts_ms,
        content.len(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn allow(patterns: &[&str]) -> AllowList {
        AllowList::from_strs(patterns)
    }

    #[test]
    fn matching_prefix_tags_outbox() {
        let ev = to_event(
            "outbox",
            &Bytes::from(r#"{"id":"1"}"#),
            Lsn::from(1u64),
            "p",
            "db",
            None,
            None,
            &allow(&["outbox"]),
        )
        .unwrap();
        assert_eq!(ev.source.schema.as_deref(), Some(OUTBOX_SCHEMA_SENTINEL));
    }

    #[test]
    fn glob_prefix_match() {
        let ev = to_event(
            "outbox_orders",
            &Bytes::from(r#"{}"#),
            Lsn::from(1u64),
            "p",
            "db",
            None,
            None,
            &allow(&["outbox_%"]),
        )
        .unwrap();
        assert_eq!(ev.source.schema.as_deref(), Some(OUTBOX_SCHEMA_SENTINEL));
    }

    #[test]
    fn non_matching_prefix_tags_wal_message() {
        let ev = to_event(
            "audit",
            &Bytes::from(r#"{}"#),
            Lsn::from(1u64),
            "p",
            "db",
            None,
            None,
            &allow(&["outbox"]),
        )
        .unwrap();
        assert_eq!(ev.source.schema.as_deref(), Some(WAL_MESSAGE_SCHEMA));
    }

    #[test]
    fn empty_allow_list_tags_wal_message() {
        let ev = to_event(
            "outbox",
            &Bytes::from(r#"{}"#),
            Lsn::from(1u64),
            "p",
            "db",
            None,
            None,
            &AllowList::new(&[]),
        )
        .unwrap();
        assert_eq!(ev.source.schema.as_deref(), Some(WAL_MESSAGE_SCHEMA));
    }

    #[test]
    fn invalid_json_returns_none() {
        assert!(
            to_event(
                "x",
                &Bytes::from("nope"),
                Lsn::from(0u64),
                "p",
                "db",
                None,
                None,
                &AllowList::new(&[]),
            )
            .is_none()
        );
    }
}
