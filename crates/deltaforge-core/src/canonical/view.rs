//! The path view of canonical values: JSON for evaluating dotted paths
//! (filter predicates, routing templates, schema sensing), never an output
//! format. Integers are exact JSON numbers (never strings: nothing here is
//! read by a JSON parser that rounds), a SQL JSON column is its parsed
//! document (so a path can descend into it), and an unchanged TOAST column
//! is absent (its value is not in the event). Every other value is its
//! native rendering for the event's mapping version.

use serde_json::{Map, Value};

use super::render::{RenderOpts, WideIntegers, render_value};
use super::{CanonicalRow, CanonicalValue, DbValue, TypeMapping};

/// A value's view; `None` for an unchanged TOAST value.
pub fn value_view(v: &CanonicalValue, mapping: TypeMapping) -> Option<Value> {
    Some(match v {
        CanonicalValue::Unchanged => return None,
        CanonicalValue::Db(DbValue::Int { value, .. }) => Value::from(*value),
        CanonicalValue::Db(DbValue::UInt { value, .. }) => Value::from(*value),
        CanonicalValue::Db(DbValue::Json(j)) => {
            serde_json::from_str(j.as_str()).expect("JSON text is valid JSON")
        }
        CanonicalValue::Db(DbValue::Array(a)) => Value::Array(
            a.items()
                .iter()
                .map(|i| value_view(i, mapping).unwrap_or(Value::Null))
                .collect(),
        ),
        other => render_value(
            other,
            RenderOpts {
                mapping,
                wide_integers: WideIntegers::String,
            },
        )
        .expect("only unchanged values and number-mode integers fail"),
    })
}

/// A row's view: its present columns and its extensions.
pub fn row_view(
    row: &CanonicalRow,
    mapping: TypeMapping,
) -> Map<String, Value> {
    let mut m: Map<String, Value> = row
        .iter()
        .filter_map(|(n, v)| value_view(v, mapping).map(|v| (n.to_string(), v)))
        .collect();
    for (n, d) in row.extensions() {
        m.entry(n.clone()).or_insert_with(|| d.value().clone());
    }
    m
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::canonical::{IntWidth, JsonText};
    use serde_json::json;

    #[test]
    fn integers_are_exact_numbers_and_json_columns_are_documents() {
        let row = CanonicalRow::new(vec![
            (
                "big".into(),
                CanonicalValue::Db(
                    DbValue::uint(u64::MAX, IntWidth::W64).unwrap(),
                ),
            ),
            (
                "doc".into(),
                CanonicalValue::Db(DbValue::Json(
                    JsonText::new(r#"{"a": [1]}"#.into()).unwrap(),
                )),
            ),
            (
                "jnull".into(),
                CanonicalValue::Db(DbValue::Json(
                    JsonText::new("null".into()).unwrap(),
                )),
            ),
            ("toast".into(), CanonicalValue::Unchanged),
            ("n".into(), CanonicalValue::Null),
        ])
        .unwrap();
        let v = Value::Object(row_view(&row, TypeMapping::V2));
        assert_eq!(v["big"], json!(u64::MAX));
        assert_eq!(v["doc"]["a"][0], json!(1));
        assert!(v.get("toast").is_none());
        assert_eq!(v["n"], Value::Null);
        assert_eq!(v["jnull"], Value::Null);
    }
}
