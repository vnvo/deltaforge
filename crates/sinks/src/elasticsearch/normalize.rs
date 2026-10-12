//! An Elasticsearch document from a row image's canonical values, by type
//! (never by the value's shape):
//!
//! | canonical value | document value |
//! |---|---|
//! | binary, untyped bytes, BIT(n>1) | base64 string (ES `binary`) |
//! | BIT(1) | boolean |
//! | integers (any width) | exact JSON number |
//! | temporal values | their exact text (the `date` mapping parses it) |
//! | JSON column | its JSON text |
//! | document in a column the table does not have | the embedded JSON |
//! | processor-added extension | merged in as embedded JSON (a name that is a column's is refused) |
//! | everything else | its native rendering for the event's version |
//!
//! An unchanged TOAST value (its value is not in the event: indexing the
//! document would drop it) or a document in a declared column is refused.

use crate::clickhouse::types::ColDesc;
use base64::Engine;
use deltaforge_core::canonical::render::{
    RenderOpts, WideIntegers, render_value,
};
use deltaforge_core::canonical::{CanonicalRow, CanonicalValue, DbValue};
use deltaforge_core::{SinkError, TypeMapping};
use serde_json::{Map, Value};

fn b64(b: &[u8]) -> Value {
    Value::String(base64::engine::general_purpose::STANDARD.encode(b))
}

fn refuse(column: &str, why: &str) -> SinkError {
    SinkError::Serialization {
        details: format!("elasticsearch: column `{column}`: {why}").into(),
    }
}

/// One column value as a document value.
fn doc_value(
    column: &str,
    declared: bool,
    v: &CanonicalValue,
) -> Result<Value, SinkError> {
    Ok(match v {
        CanonicalValue::Null => Value::Null,
        CanonicalValue::Unchanged => {
            return Err(refuse(
                column,
                "PostgreSQL did not send the value (unchanged TOAST)",
            ));
        }
        CanonicalValue::Document(_) if declared => {
            return Err(refuse(
                column,
                "an untyped document in a schema-typed column",
            ));
        }
        CanonicalValue::Document(d) => d.value().clone(),
        CanonicalValue::Bytes(b) => b64(b),
        CanonicalValue::Opaque(o) => Value::String(o.text.clone()),
        CanonicalValue::Db(DbValue::Binary(b)) => b64(b),
        CanonicalValue::Db(DbValue::Bits(b)) if b.len() > 1 => b64(b.bytes()),
        CanonicalValue::Db(DbValue::Int { value, .. }) => Value::from(*value),
        CanonicalValue::Db(DbValue::UInt { value, .. }) => Value::from(*value),
        CanonicalValue::Db(DbValue::Array(a)) => Value::Array(
            a.items()
                .iter()
                .map(|i| doc_value(column, declared, i))
                .collect::<Result<_, _>>()?,
        ),
        // Temporal text, JSON text, decimals, labels, UUIDs, floats, BIT(1):
        // the v2 rendering (v1's epoch-number TIMESTAMP is not a document
        // form: the date mapping reads text).
        other => render_value(
            other,
            RenderOpts {
                mapping: TypeMapping::V2,
                wide_integers: WideIntegers::String,
            },
        )
        .map_err(|e| refuse(column, &e.to_string()))?,
    })
}

/// The document of a row image, typed by the resolved column types. A
/// document always uses the v2 rendering, whatever the event's mapping
/// version (v1's epoch-number TIMESTAMP is not a document form).
pub fn normalize_doc(
    columns: &[ColDesc],
    row: &CanonicalRow,
) -> Result<Value, SinkError> {
    let mut out = Map::with_capacity(row.len());
    for (k, v) in row.iter() {
        let declared = columns.iter().any(|c| c.name == k);
        out.insert(k.to_string(), doc_value(k, declared, v)?);
    }
    // Processor-added extensions merge into the document; one that names a
    // column (of the row or of the table) is refused, never overwritten.
    for (k, d) in row.extensions() {
        if out.contains_key(k) || columns.iter().any(|c| &c.name == k) {
            return Err(refuse(k, "an extension has a column's name"));
        }
        out.insert(k.clone(), d.value().clone());
    }
    Ok(Value::Object(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltaforge_core::canonical::{
        Bits, Document, IntWidth, Precision, TimestampValue,
    };
    use serde_json::json;

    fn col(name: &str) -> ColDesc {
        ColDesc {
            name: name.into(),
            data_type: "x".into(),
            full_type: "x".into(),
            nullable: true,
            unsigned: false,
            precision: None,
            scale: None,
        }
    }

    fn doc(cols: Vec<(&str, CanonicalValue)>) -> Result<Value, SinkError> {
        let row = CanonicalRow::new(
            cols.into_iter().map(|(n, v)| (n.to_string(), v)).collect(),
        )
        .unwrap();
        normalize_doc(
            &[col("bin"), col("big"), col("ts"), col("b1"), col("j")],
            &row,
        )
    }

    #[test]
    fn values_follow_their_type_not_their_shape() {
        let d = doc(vec![
            ("bin", CanonicalValue::Db(DbValue::Binary(b"ok".to_vec()))),
            (
                "big",
                CanonicalValue::Db(
                    DbValue::uint(u64::MAX, IntWidth::W64).unwrap(),
                ),
            ),
            (
                "ts",
                CanonicalValue::Db(DbValue::Timestamp(
                    TimestampValue::instant(1_500, Precision::Fixed(6))
                        .unwrap(),
                )),
            ),
            (
                "b1",
                CanonicalValue::Db(DbValue::Bits(
                    Bits::new(1, vec![1]).unwrap(),
                )),
            ),
            (
                "j",
                CanonicalValue::Db(DbValue::Json(
                    deltaforge_core::canonical::JsonText::new(
                        "{\"a\": 1}".into(),
                    )
                    .unwrap(),
                )),
            ),
            (
                "added",
                CanonicalValue::Document(Document::new(json!({"k": 1}))),
            ),
        ])
        .unwrap();
        assert_eq!(d["bin"], json!("b2s="));
        assert_eq!(d["big"], json!(u64::MAX));
        assert_eq!(d["ts"], json!("1970-01-01T00:00:00.001500Z"));
        assert_eq!(d["b1"], json!(true));
        assert_eq!(d["j"], json!("{\"a\": 1}"));
        assert_eq!(d["added"], json!({"k": 1}));
    }

    #[test]
    fn extensions_merge_unless_they_name_a_column() {
        let row = CanonicalRow::with_extensions(
            vec![("big".to_string(), CanonicalValue::Null)],
            vec![("added".to_string(), Document::new(json!([1])))],
        )
        .unwrap();
        let d = normalize_doc(&[col("big")], &row).unwrap();
        assert_eq!(d["added"], json!([1]));
        // An extension that names a table column the row does not carry.
        let row = CanonicalRow::with_extensions(
            vec![],
            vec![("ts".to_string(), Document::new(json!(1)))],
        )
        .unwrap();
        assert!(normalize_doc(&[col("ts")], &row).is_err());
    }

    #[test]
    fn unchanged_and_documents_in_typed_columns_are_refused() {
        assert!(doc(vec![("ts", CanonicalValue::Unchanged)]).is_err());
        assert!(
            doc(vec![(
                "big",
                CanonicalValue::Document(Document::new(json!(1)))
            )])
            .is_err()
        );
    }
}
