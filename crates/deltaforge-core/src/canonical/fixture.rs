//! Canonical rows from JSON literals, for test fixtures only (feature
//! `test-fixtures`). Production code never infers types from JSON: values
//! come from a source decoder or a typed parse.

use serde_json::Value;

use super::{CanonicalRow, CanonicalValue, DbValue, Document, IntWidth};

/// A fixture value: null is SQL NULL; booleans, integers (32-bit when they
/// fit, else 64-bit), floats and strings are the matching database values;
/// objects and arrays are documents.
pub fn value(v: &Value) -> CanonicalValue {
    match v {
        Value::Null => CanonicalValue::Null,
        Value::Bool(b) => CanonicalValue::Db(DbValue::Bool(*b)),
        Value::Number(n) => CanonicalValue::Db(if let Some(i) = n.as_i64() {
            if i32::try_from(i).is_ok() {
                DbValue::int(i, IntWidth::W32).expect("fits")
            } else {
                DbValue::int(i, IntWidth::W64).expect("fits")
            }
        } else if let Some(u) = n.as_u64() {
            DbValue::uint(u, IntWidth::W64).expect("fits")
        } else {
            DbValue::Float64(n.as_f64().expect("a JSON number"))
        }),
        Value::String(s) => CanonicalValue::Db(DbValue::Text(s.clone())),
        other => CanonicalValue::Document(Document::new(other.clone())),
    }
}

/// A fixture row from a JSON object (column order: the object's).
pub fn row(v: Value) -> CanonicalRow {
    let Value::Object(m) = v else {
        panic!("a fixture row is a JSON object");
    };
    CanonicalRow::new(m.iter().map(|(k, v)| (k.clone(), value(v))).collect())
        .expect("object keys are unique")
}
