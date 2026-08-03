//! Project a CDC `Event` into one RowBinary row.
//!
//! Row layout: user columns (in declared order) then the meta columns
//! `_op` (String), `_version` (UInt64), `_deleted` (UInt8),
//! `_source_ts` (DateTime64(3) = `ts_ms`). The `Op::Delete` image is the
//! before-image (keys present); all other ops use the after-image.

use super::rowbinary::{encode_value, write_varuint, EncodeError};
use super::types::{ChType, ColDesc};
use super::version::derive_version;
use deltaforge_config::ChVersionSource;
use deltaforge_core::{Event, Op};
use serde_json::Value;

/// A resolved per-table projection: the user columns (with mapped types) plus
/// how to derive `_version`.
pub struct TableProjection {
    pub columns: Vec<(ColDesc, ChType)>,
    pub version_source: ChVersionSource,
}

/// The one-character `_op` value for an event.
fn op_str(op: Op) -> &'static str {
    match op {
        Op::Create => "c",
        Op::Update => "u",
        Op::Delete => "d",
        Op::Read => "r",
        Op::Truncate => "t",
    }
}

/// Project a single event into RowBinary bytes. Returns `EncodeError` if a value
/// can't be encoded for its column (caller isolates it into the DLQ).
pub fn project_row(p: &TableProjection, event: &Event) -> Result<Vec<u8>, EncodeError> {
    let image = match event.op {
        Op::Delete => event.before.as_ref(),
        _ => event.after.as_ref(),
    };
    let null = Value::Null;
    let mut buf = Vec::with_capacity(64);

    for (col, ty) in &p.columns {
        let v = image.and_then(|m| m.get(&col.name)).unwrap_or(&null);
        encode_value(&mut buf, ty, col.nullable, v)?;
    }

    // _op (String)
    let op = op_str(event.op);
    write_varuint(&mut buf, op.len() as u64);
    buf.extend_from_slice(op.as_bytes());

    // _version (UInt64)
    let version = derive_version(event, p.version_source.clone());
    buf.extend_from_slice(&version.to_le_bytes());

    // _deleted (UInt8)
    buf.push(u8::from(matches!(event.op, Op::Delete)));

    // _source_ts (DateTime64(3) = ts_ms as Int64)
    buf.extend_from_slice(&event.ts_ms.to_le_bytes());

    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltaforge_core::{SourceInfo, SourcePosition};
    use serde_json::json;

    fn proj() -> TableProjection {
        TableProjection {
            columns: vec![(
                ColDesc {
                    name: "id".into(),
                    data_type: "bigint".into(),
                    full_type: "bigint".into(),
                    nullable: false,
                    unsigned: false,
                    precision: None,
                    scale: None,
                },
                ChType::Int64,
            )],
            version_source: ChVersionSource::TsMs,
        }
    }

    fn ev(op: Op, after: Value, before: Value) -> Event {
        Event {
            before: if before.is_null() { None } else { Some(before) },
            after: if after.is_null() { None } else { Some(after) },
            source: SourceInfo {
                version: "1".into(),
                connector: "mysql".into(),
                name: "t".into(),
                ts_ms: 5,
                db: "d".into(),
                schema: None,
                table: "orders".into(),
                snapshot: None,
                position: SourcePosition::default(),
            },
            op,
            ts_ms: 5,
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
            checkpoint: None,
            size_bytes: 0,
            received_at_ms: 0,
        }
    }

    #[test]
    fn projects_insert_uses_after_and_appends_meta() {
        let bytes = project_row(&proj(), &ev(Op::Create, json!({"id": 7}), json!(null))).unwrap();
        let mut want = Vec::new();
        want.extend_from_slice(&7i64.to_le_bytes()); // id
        want.push(1);
        want.push(b'c'); // _op = "c"
        want.extend_from_slice(&5u64.to_le_bytes()); // _version = ts_ms
        want.push(0); // _deleted
        want.extend_from_slice(&5i64.to_le_bytes()); // _source_ts
        assert_eq!(bytes, want);
    }

    #[test]
    fn projects_delete_uses_before_and_sets_deleted() {
        let bytes = project_row(&proj(), &ev(Op::Delete, json!(null), json!({"id": 9}))).unwrap();
        assert_eq!(&bytes[..8], &9i64.to_le_bytes()); // key from before-image
        assert_eq!(bytes[8], 1);
        assert_eq!(bytes[9], b'd'); // _op = "d"
        // layout: id(8) + _op(2) + _version(8) → _deleted at byte 18
        assert_eq!(bytes[18], 1);
    }

    #[test]
    fn bad_value_returns_encode_error() {
        // id is Int64 but the event carries a string → EncodeError (caller DLQs).
        assert!(project_row(&proj(), &ev(Op::Create, json!({"id": "abc"}), json!(null))).is_err());
    }
}
