//! Project a CDC `Event` into one RowBinary row.
//!
//! Row layout: user columns (in declared order) then the meta columns
//! `_op` (String), `_version` (UInt64), `_deleted` (UInt8),
//! `_source_ts` (DateTime64(3) = `ts_ms`), then the extension column
//! (`extensions_column`, `Nullable(String)`: the image's processor-added
//! documents as one canonical JSON object). The `Op::Delete` image is the
//! before-image (keys present); all other ops use the after-image.

use super::rowbinary::{EncodeError, encode_value, write_varuint};
use super::types::{ChType, ColDesc};
use super::version::derive_version;
use deltaforge_config::ChVersionSource;
use deltaforge_core::{CanonicalValue, Event, Op};

/// A resolved per-table projection: the user columns (with mapped types) plus
/// how to derive `_version`.
pub struct TableProjection {
    pub columns: Vec<(ColDesc, ChType)>,
    pub version_source: ChVersionSource,
    /// The extension column (last in the row).
    pub extensions_column: String,
}

/// The ClickHouse meta columns every row carries.
pub const META_COLUMNS: [&str; 4] =
    ["_op", "_version", "_deleted", "_source_ts"];

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
pub fn project_row(
    p: &TableProjection,
    event: &Event,
) -> Result<Vec<u8>, EncodeError> {
    let image = match event.op {
        Op::Delete => event.before.as_ref(),
        _ => event.after.as_ref(),
    };
    let null = CanonicalValue::Null;
    let mut buf = Vec::with_capacity(64);

    // A column the table does not have is refused, never dropped.
    if let Some(extra) = image.and_then(|r| {
        r.names()
            .find(|n| !p.columns.iter().any(|(c, _)| c.name == *n))
    }) {
        return Err(EncodeError::Type {
            expected: format!("a column of the table (`{extra}` is not one)"),
            got: "an undefined column".into(),
        });
    }
    for (col, ty) in &p.columns {
        let v = image.and_then(|m| m.get(&col.name)).unwrap_or(&null);
        encode_value(&mut buf, ty, col.nullable, v).map_err(|e| match e {
            EncodeError::Type { expected, got } => EncodeError::Type {
                expected: format!("{expected} for column `{}`", col.name),
                got,
            },
        })?;
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

    // Extensions (Nullable(String)): the delivered image's document
    // extensions as a canonical JSON object, NULL when it has none.
    match image.filter(|r| !r.extensions().is_empty()) {
        None => buf.push(1),
        Some(r) => {
            buf.push(0);
            let obj = serde_json::Value::Object(
                r.extensions()
                    .iter()
                    .map(|(n, d)| (n.clone(), d.value().clone()))
                    .collect(),
            );
            let bytes = deltaforge_core::canonical_json_bytes(&obj);
            write_varuint(&mut buf, bytes.len() as u64);
            buf.extend_from_slice(&bytes);
        }
    }

    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltaforge_core::canonical::fixture;
    use deltaforge_core::{SourceInfo, SourcePosition, TypeMapping};
    use serde_json::{Value, json};

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
            extensions_column: "_extensions".into(),
        }
    }

    fn ev(op: Op, after: Value, before: Value) -> Event {
        Event {
            before: if before.is_null() {
                None
            } else {
                Some(fixture::row(before))
            },
            after: if after.is_null() {
                None
            } else {
                Some(fixture::row(after))
            },
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
            type_mapping: TypeMapping::V2,
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

    #[test]
    fn projects_insert_uses_after_and_appends_meta() {
        let bytes = project_row(
            &proj(),
            &ev(Op::Create, json!({"id": 7}), json!(null)),
        )
        .unwrap();
        let mut want = Vec::new();
        want.extend_from_slice(&7i64.to_le_bytes()); // id
        want.push(1);
        want.push(b'c'); // _op = "c"
        want.extend_from_slice(&5u64.to_le_bytes()); // _version = ts_ms
        want.push(0); // _deleted
        want.extend_from_slice(&5i64.to_le_bytes()); // _source_ts
        want.push(1); // _extensions: NULL
        assert_eq!(bytes, want);
    }

    #[test]
    fn extensions_are_one_canonical_json_column() {
        let mut e = ev(Op::Create, json!({"id": 7}), json!(null));
        e.after
            .as_mut()
            .unwrap()
            .set_extension(
                "z",
                deltaforge_core::canonical::Document::new(
                    json!({"b": 1, "a": 2}),
                ),
            )
            .unwrap();
        let bytes = project_row(&proj(), &e).unwrap();
        let tail = br#"{"z":{"a":2,"b":1}}"#;
        assert!(bytes.ends_with(tail));
        assert_eq!(bytes[bytes.len() - tail.len() - 2], 0, "not NULL");
        assert_eq!(bytes[bytes.len() - tail.len() - 1] as usize, tail.len());
    }

    #[test]
    fn projects_delete_uses_before_and_sets_deleted() {
        let bytes = project_row(
            &proj(),
            &ev(Op::Delete, json!(null), json!({"id": 9})),
        )
        .unwrap();
        assert_eq!(&bytes[..8], &9i64.to_le_bytes()); // key from before-image
        assert_eq!(bytes[8], 1);
        assert_eq!(bytes[9], b'd'); // _op = "d"
        // layout: id(8) + _op(2) + _version(8) → _deleted at byte 18
        assert_eq!(bytes[18], 1);
    }

    #[test]
    fn bad_value_returns_encode_error() {
        // id is Int64 but the event carries a string → EncodeError (caller DLQs).
        assert!(
            project_row(
                &proj(),
                &ev(Op::Create, json!({"id": "abc"}), json!(null))
            )
            .is_err()
        );
        // A column the table does not have is refused, never dropped.
        assert!(
            project_row(
                &proj(),
                &ev(
                    Op::Create,
                    json!({"id": 1, "extra": {"a": 1}}),
                    json!(null)
                )
            )
            .is_err()
        );
    }
}
