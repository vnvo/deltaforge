//! Normalize a source `after` document so its values match the generated ES
//! mapping and what Elasticsearch will accept.
//!
//! The MySQL source encodes some column types in shapes ES rejects verbatim,
//! and the two source paths differ:
//!
//! | type            | binlog stream            | snapshot                     |
//! |-----------------|--------------------------|------------------------------|
//! | TEXT/BLOB       | `{"_base64": "<b64>"}`   | UTF-8 string (b64 if binary) |
//! | TIMESTAMP       | integer **microseconds** | ISO-8601 string (`…T…`)      |
//! | DATETIME/DATE   | string                   | ISO-8601 string              |
//! | DECIMAL         | string                   | string                       |
//!
//! So normalization is driven by the *value shape* (object/number/string), not
//! by assuming a single encoding:
//! - `{"_base64": s}` → for a binary column keep the base64 string (ES `binary`
//!   stores base64); otherwise decode to UTF-8 text (fall back to the base64
//!   string if not valid UTF-8).
//! - a numeric temporal (binlog TIMESTAMP, microseconds) → milliseconds, which
//!   the generated `date` mapping accepts as `epoch_millis`. String temporals
//!   pass through (the mapping's `format` list accepts them).

use crate::clickhouse::types::ColDesc;
use base64::Engine;
use serde_json::{Map, Value};

fn is_binary_type(dt: &str) -> bool {
    matches!(
        dt,
        "blob"
            | "tinyblob"
            | "mediumblob"
            | "longblob"
            | "binary"
            | "varbinary"
    )
}

/// Temporal types whose binlog form is an integer microsecond count.
fn is_numeric_temporal(dt: &str) -> bool {
    matches!(dt, "timestamp" | "timestamptz" | "datetime")
}

/// Normalize one column value given its source `data_type` (lowercased).
pub fn normalize_value(data_type: &str, v: &Value) -> Value {
    let dt = data_type.to_lowercase();
    match v {
        // Base64-wrapped binary/text (both `_base64` and the JSON fallback
        // `_base64_json` the source may emit).
        Value::Object(m)
            if m.contains_key("_base64") || m.contains_key("_base64_json") =>
        {
            let b64 = m
                .get("_base64")
                .or_else(|| m.get("_base64_json"))
                .and_then(Value::as_str)
                .unwrap_or("");
            if is_binary_type(&dt) {
                // ES `binary` wants the base64 string as-is.
                Value::String(b64.to_string())
            } else {
                // Character column: recover the text.
                match base64::engine::general_purpose::STANDARD.decode(b64) {
                    Ok(bytes) => match String::from_utf8(bytes) {
                        Ok(s) => Value::String(s),
                        Err(_) => Value::String(b64.to_string()),
                    },
                    Err(_) => Value::String(b64.to_string()),
                }
            }
        }
        // Binlog TIMESTAMP/DATETIME: integer microseconds → milliseconds.
        Value::Number(n) if is_numeric_temporal(&dt) => match n.as_i64() {
            Some(micros) => Value::Number((micros / 1_000).into()),
            None => v.clone(),
        },
        _ => v.clone(),
    }
}

/// Normalize every field of an `after`/`before` object using the resolved
/// column types. Fields with no matching column (or a non-object body) pass
/// through unchanged.
pub fn normalize_doc(columns: &[ColDesc], doc: &Value) -> Value {
    let Value::Object(m) = doc else {
        return doc.clone();
    };
    let mut out = Map::with_capacity(m.len());
    for (k, v) in m {
        let normalized = match columns.iter().find(|c| &c.name == k) {
            Some(c) => normalize_value(&c.data_type, v),
            None => v.clone(),
        };
        out.insert(k.clone(), normalized);
    }
    Value::Object(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn col(name: &str, dt: &str) -> ColDesc {
        ColDesc {
            name: name.into(),
            data_type: dt.into(),
            full_type: dt.into(),
            nullable: true,
            unsigned: false,
            precision: None,
            scale: None,
        }
    }

    fn b64(s: &str) -> String {
        base64::engine::general_purpose::STANDARD.encode(s.as_bytes())
    }

    #[test]
    fn text_column_base64_decodes_to_string() {
        // binlog delivers TEXT as a base64-wrapped blob.
        let v = json!({ "_base64": b64("backlog-123") });
        assert_eq!(normalize_value("text", &v), json!("backlog-123"));
    }

    #[test]
    fn blob_column_keeps_base64_string() {
        let raw = b64("\x00\x01binary");
        let v = json!({ "_base64": raw });
        // binary column → base64 string preserved (ES `binary`).
        assert_eq!(normalize_value("blob", &v), json!(raw));
    }

    #[test]
    fn non_utf8_text_falls_back_to_base64() {
        // 0xff 0xfe is not valid UTF-8.
        let raw =
            base64::engine::general_purpose::STANDARD.encode([0xff, 0xfe]);
        let v = json!({ "_base64": raw });
        assert_eq!(normalize_value("varchar", &v), json!(raw));
    }

    #[test]
    fn timestamp_micros_to_millis() {
        // binlog TIMESTAMP: 1789934706000000 µs → 1789934706000 ms.
        let v = json!(1789934706000000i64);
        assert_eq!(normalize_value("timestamp", &v), json!(1789934706000i64));
    }

    #[test]
    fn string_temporal_passes_through() {
        // snapshot ISO string is left for the mapping's date format to parse.
        let v = json!("2026-09-20T20:06:51.000000");
        assert_eq!(normalize_value("datetime", &v), v);
    }

    #[test]
    fn scalars_and_decimal_pass_through() {
        assert_eq!(normalize_value("bigint", &json!(42)), json!(42));
        assert_eq!(
            normalize_value("decimal", &json!("8150.7937")),
            json!("8150.7937")
        );
        assert_eq!(normalize_value("json", &json!({"a": 1})), json!({"a": 1}));
    }

    #[test]
    fn normalize_doc_uses_column_types_and_passes_unknowns() {
        let cols = vec![col("data", "text"), col("created_at", "timestamp")];
        let doc = json!({
            "id": 7,                                   // no column → passthrough
            "data": { "_base64": b64("hello") },
            "created_at": 1789934706000000i64,
        });
        let out = normalize_doc(&cols, &doc);
        assert_eq!(out["id"], json!(7));
        assert_eq!(out["data"], json!("hello"));
        assert_eq!(out["created_at"], json!(1789934706000i64));
    }
}
