use base64::prelude::*;
use mysql_binlog_connector_rust::column::column_value::ColumnValue;
use mysql_common::{binlog::jsonb, io::ParseBuf, proto::MyDeserialize};
use serde_json::{Value, json};
use std::sync::Arc;
use tracing::debug;

/// Build a JSON object using the included-columns bitmap and the compact values vector.
pub(super) fn build_object(
    cols: &Arc<Vec<String>>,
    included: &[bool],
    values: &[ColumnValue],
) -> Value {
    let mut obj = serde_json::Map::with_capacity(cols.len());
    let mut vi = 0usize;

    debug!(cols = ?cols, values = ?values);

    for (idx, inc) in included.iter().enumerate() {
        if !*inc {
            continue;
        }
        let key = cols.get(idx).map(|s| s.as_str()).unwrap_or("col");
        let v = values.get(vi).unwrap_or(&ColumnValue::None);
        vi += 1;
        let jv = match v {
            ColumnValue::None => Value::Null,
            ColumnValue::Tiny(x) => json!(*x),
            ColumnValue::Short(x) => json!(*x),
            ColumnValue::Long(x) => json!(*x),
            ColumnValue::LongLong(x) => json!(*x),
            ColumnValue::Float(x) => json!(*x),
            ColumnValue::Double(x) => json!(*x),
            ColumnValue::Decimal(s) => json!(s),
            ColumnValue::Time(s) => json!(s),
            ColumnValue::Date(s) => json!(s),
            ColumnValue::DateTime(s) => json!(s),
            ColumnValue::Timestamp(ts) => json!(ts),
            ColumnValue::Year(y) => json!(y),
            ColumnValue::Bit(b) => json!(b),
            ColumnValue::Set(v) => json!(v),
            ColumnValue::Enum(v) => json!(v),
            ColumnValue::String(bytes) => match std::str::from_utf8(bytes) {
                Ok(s) => json!(s),
                Err(_) => json!({ "_base64": to_b64(bytes) }),
            },
            ColumnValue::Blob(bytes) => json!({ "_base64": to_b64(bytes) }),
            ColumnValue::Json(bytes) => handle_json(bytes),
        };
        obj.insert(key.to_string(), jv);
    }
    Value::Object(obj)
}

fn parse_mysql_jsonb(bytes: &[u8]) -> Option<Value> {
    // Parse MySQL binary JSON (JSONB)
    let mut pb = ParseBuf(bytes);
    let v = jsonb::Value::deserialize((), &mut pb).ok()?;
    let dom = v.parse().ok()?;
    Some(dom.into())
}

fn handle_json(bytes: &[u8]) -> Value {
    if let Some(v) = parse_mysql_jsonb(bytes) {
        v
    } else if let Ok(s) = std::str::from_utf8(bytes) {
        // Fallback: sometimes sources hand textual JSON
        serde_json::from_str::<Value>(s).unwrap_or_else(|_| json!(s))
    } else {
        // Last resort: base64-wrap
        json!({ "_base64_json": to_b64(&bytes.to_vec()) })
    }
}

fn to_b64(bytes: &Vec<u8>) -> String {
    BASE64_STANDARD.encode(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mysql_binlog_connector_rust::column::column_value::ColumnValue;
    use serde_json::Value;
    use std::sync::Arc;

    #[test]
    fn build_object_maps_basic_scalar_types() {
        let cols = Arc::new(vec!["id".to_string(), "sku".to_string()]);
        let included = [true, true];
        let values = vec![
            ColumnValue::LongLong(42),
            ColumnValue::String(b"sku-1".to_vec()),
        ];

        let obj = build_object(&cols, &included, &values);
        assert_eq!(obj["id"], Value::from(42_i64));
        assert_eq!(obj["sku"], Value::from("sku-1"));
    }

    #[test]
    fn build_object_respects_included_bitmap() {
        let cols = Arc::new(vec![
            "id".to_string(),
            "ignored".to_string(),
            "sku".to_string(),
        ]);
        // Only 1st and 3rd columns included; values contain only those
        let included = [true, false, true];
        let values = vec![
            ColumnValue::LongLong(1),
            ColumnValue::String(b"sku-xx".to_vec()),
        ];

        let obj = build_object(&cols, &included, &values);
        assert_eq!(obj["id"], Value::from(1_i64));
        assert_eq!(obj["sku"], Value::from("sku-xx"));
        assert!(!obj.as_object().unwrap().contains_key("ignored"));
    }

    #[test]
    fn build_object_encodes_binary_string_as_base64_wrapper() {
        let cols = Arc::new(vec!["data".to_string()]);
        let included = [true];
        let bytes = vec![0xff, 0x00, 0xaa];
        let values = vec![ColumnValue::String(bytes.clone())];

        let obj = build_object(&cols, &included, &values);
        let data = &obj["data"];
        // Should be an object { "_base64": "..." }
        assert!(data.is_object());
        let base64 = &data["_base64"];
        assert!(base64.is_string());
        let expected = BASE64_STANDARD.encode(&bytes);
        assert_eq!(base64.as_str().unwrap(), expected);
    }

    #[test]
    fn build_object_encodes_blob_as_base64_wrapper() {
        let cols = Arc::new(vec!["blobz".to_string()]);
        let included = [true];
        let bytes = vec![0xde, 0xad, 0xbe, 0xef];
        let values = vec![ColumnValue::Blob(bytes.clone())];

        let obj = build_object(&cols, &included, &values);
        let data = &obj["blobz"];
        assert!(data.is_object());
        let base64 = &data["_base64"];
        assert!(base64.is_string());
        let expected = BASE64_STANDARD.encode(&bytes);
        assert_eq!(base64.as_str().unwrap(), expected);
    }

    #[test]
    fn handle_json_falls_back_to_textual_json() {
        // This is NOT MySQL JSONB, so parse_mysql_jsonb will fail,
        // but it's valid UTF-8 textual JSON.
        let bytes = br#"{"a":1,"b":"x"}"#;
        let v = super::handle_json(bytes);
        assert_eq!(v["a"], Value::from(1));
        assert_eq!(v["b"], Value::from("x"));
    }

    #[test]
    fn handle_json_wraps_non_utf8_as_base64_json() {
        // Neither JSONB nor UTF-8 → we expect the _base64_json wrapper
        let bytes = [0xff, 0x00, 0x01];
        let v = super::handle_json(&bytes);
        assert!(v.is_object());
        let inner = &v["_base64_json"];
        assert!(inner.is_string());
        let expected = BASE64_STANDARD.encode(&bytes);
        assert_eq!(inner.as_str().unwrap(), expected);
    }
}
