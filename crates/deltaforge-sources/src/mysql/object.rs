use base64::prelude::*;
use mysql_binlog_connector_rust::column::column_value::ColumnValue;
use mysql_common::{binlog::jsonb, io::ParseBuf, proto::MyDeserialize};
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::debug;

/// Build a JSON object using the included-columns bitmap and the compact values vector.
pub(super) fn build_object(
    cols: &Arc<Vec<String>>,
    included: &Vec<bool>,
    values: &Vec<ColumnValue>,
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
