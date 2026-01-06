//! PostgreSQL row value conversion to JSON objects.
//!
//! Handles pgoutput protocol tuple data conversion to JSON values,
//! supporting PostgreSQL's type system including arrays, JSON/JSONB,
//! binary data, and timestamps.

use base64::prelude::*;
use bytes::Bytes;
use serde_json::{Value, json};
use tracing::debug;

/// Column value from pgoutput tuple data.
#[derive(Debug, Clone)]
pub enum PgColumnValue {
    /// NULL value (n)
    Null,
    /// TOAST unchanged value (u) - placeholder
    Unchanged,
    /// Text value (t)
    Text(String),
    /// Binary value (b)
    Binary(Vec<u8>),
}

/// Relation column metadata from pgoutput.
#[derive(Debug, Clone)]
pub struct RelationColumn {
    pub name: String,
    pub type_oid: u32,
    #[allow(dead_code)]
    pub type_modifier: i32,
    /// Flags: 1 = part of key
    #[allow(dead_code)]
    pub flags: u8,
}

#[cfg(test)]
impl RelationColumn {
    pub fn is_key(&self) -> bool {
        self.flags & 1 != 0
    }
}

/// Build a JSON object from pgoutput tuple data.
///
/// The `columns` vector contains column metadata from the relation message.
/// The `values` vector contains the actual column values.
pub fn build_object(
    columns: &[RelationColumn],
    values: &[PgColumnValue],
) -> Value {
    let mut obj = serde_json::Map::with_capacity(columns.len());

    debug!(
        columns = columns.len(),
        values = values.len(),
        "building object"
    );

    for (idx, col) in columns.iter().enumerate() {
        let value = values.get(idx).unwrap_or(&PgColumnValue::Null);
        let json_value = convert_value(value, col.type_oid);
        obj.insert(col.name.clone(), json_value);
    }

    Value::Object(obj)
}

/// Build a JSON object with only key columns.
#[cfg(test)]
pub fn build_key_object(
    columns: &[RelationColumn],
    values: &[PgColumnValue],
) -> Value {
    let mut obj = serde_json::Map::new();

    for (idx, col) in columns.iter().enumerate() {
        if col.is_key() {
            let value = values.get(idx).unwrap_or(&PgColumnValue::Null);
            let json_value = convert_value(value, col.type_oid);
            obj.insert(col.name.clone(), json_value);
        }
    }

    Value::Object(obj)
}

/// Convert a pgoutput value to JSON based on type OID.
fn convert_value(value: &PgColumnValue, type_oid: u32) -> Value {
    match value {
        PgColumnValue::Null => Value::Null,
        PgColumnValue::Unchanged => json!({"_unchanged": true}),
        PgColumnValue::Text(s) => convert_text_value(s, type_oid),
        PgColumnValue::Binary(bytes) => convert_binary_value(bytes, type_oid),
    }
}

/// Convert text representation to JSON based on type.
fn convert_text_value(s: &str, type_oid: u32) -> Value {
    use super::postgres_table_schema::type_oids::*;

    match type_oid {
        BOOL => match s.to_lowercase().as_str() {
            "t" | "true" | "1" | "yes" | "on" => json!(true),
            "f" | "false" | "0" | "no" | "off" => json!(false),
            _ => json!(s),
        },
        INT2 | INT4 | INT8 | OID => {
            s.parse::<i64>().map(|v| json!(v)).unwrap_or(json!(s))
        }
        FLOAT4 | FLOAT8 => {
            s.parse::<f64>().map(|v| json!(v)).unwrap_or(json!(s))
        }
        NUMERIC => {
            // Keep as string to preserve precision
            json!(s)
        }
        JSON | JSONB => serde_json::from_str::<Value>(s).unwrap_or(json!(s)),
        BYTEA => {
            // bytea in text format is hex-escaped: \x...
            if let Some(hex) = s.strip_prefix("\\x") {
                if let Ok(bytes) = hex::decode(hex) {
                    json!({ "_base64": BASE64_STANDARD.encode(&bytes) })
                } else {
                    json!(s)
                }
            } else {
                json!(s)
            }
        }
        // Array types (PostgreSQL array OIDs are base type + offset)
        oid if is_array_type(oid) => parse_pg_array(s),
        // UUID
        UUID => json!(s),
        // Timestamps - keep as string for ISO 8601 format
        TIMESTAMP | TIMESTAMPTZ | DATE | TIME => json!(s),
        // Default: keep as string
        _ => json!(s),
    }
}

/// Convert binary representation to JSON.
fn convert_binary_value(bytes: &[u8], _type_oid: u32) -> Value {
    // Binary mode: encode as base64
    json!({ "_base64": BASE64_STANDARD.encode(bytes) })
}

/// Check if type OID is an array type.
fn is_array_type(oid: u32) -> bool {
    // PostgreSQL array type OIDs are typically in specific ranges
    // or have the high bit set for user-defined array types
    // Common array types:
    matches!(
        oid,
        1000 |  // bool[]
        1001 |  // bytea[]
        1005 |  // int2[]
        1007 |  // int4[]
        1009 |  // text[]
        1016 |  // int8[]
        1021 |  // float4[]
        1022 |  // float8[]
        1014 |  // char[]
        1015 |  // varchar[]
        2951 |  // uuid[]
        3802 |  // jsonb[]
        199 // json[]
    )
}

/// Parse PostgreSQL array literal to JSON array.
fn parse_pg_array(s: &str) -> Value {
    let s = s.trim();

    // Empty array
    if s == "{}" {
        return json!([]);
    }

    // Simple parser for PostgreSQL array format: {elem1,elem2,elem3}
    if !s.starts_with('{') || !s.ends_with('}') {
        return json!(s);
    }

    let inner = &s[1..s.len() - 1];
    let elements = parse_array_elements(inner);

    Value::Array(elements)
}

/// Parse comma-separated array elements, handling quotes and escapes.
fn parse_array_elements(s: &str) -> Vec<Value> {
    let mut elements = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut escape_next = false;
    let chars = s.chars().peekable();

    for c in chars {
        if escape_next {
            current.push(c);
            escape_next = false;
            continue;
        }

        match c {
            '\\' => {
                escape_next = true;
            }
            '"' => {
                in_quotes = !in_quotes;
            }
            ',' if !in_quotes => {
                elements.push(parse_array_element(&current));
                current.clear();
            }
            _ => {
                current.push(c);
            }
        }
    }

    // Last element
    if !current.is_empty() || !elements.is_empty() {
        elements.push(parse_array_element(&current));
    }

    elements
}

/// Parse a single array element.
fn parse_array_element(s: &str) -> Value {
    let s = s.trim();

    if s.eq_ignore_ascii_case("null") {
        return Value::Null;
    }

    // Handle booleans (PostgreSQL uses t/f in arrays)
    match s.to_lowercase().as_str() {
        "t" | "true" => return json!(true),
        "f" | "false" => return json!(false),
        _ => {}
    }

    // Try to parse as number
    if let Ok(i) = s.parse::<i64>() {
        return json!(i);
    }
    if let Ok(f) = s.parse::<f64>() {
        return json!(f);
    }

    // Keep as string
    json!(s)
}

/// Parse tuple data from pgoutput message.
/// Returns (values, bytes_consumed).
pub fn parse_tuple_data(
    data: &Bytes,
    column_count: usize,
) -> (Vec<PgColumnValue>, usize) {
    let mut values = Vec::with_capacity(column_count);
    let mut offset = 0;
    let data = data.as_ref();

    // First 2 bytes are number of columns
    if data.len() < 2 {
        return (values, 0);
    }
    let col_count = u16::from_be_bytes([data[0], data[1]]) as usize;
    offset += 2;

    for _ in 0..col_count {
        if offset >= data.len() {
            break;
        }

        let col_type = data[offset];
        offset += 1;

        let value = match col_type {
            b'n' => PgColumnValue::Null,
            b'u' => PgColumnValue::Unchanged,
            b't' => {
                if offset + 4 > data.len() {
                    break;
                }
                let len = u32::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]) as usize;
                offset += 4;

                if offset + len > data.len() {
                    break;
                }
                let text = String::from_utf8_lossy(&data[offset..offset + len])
                    .to_string();
                offset += len;
                PgColumnValue::Text(text)
            }
            b'b' => {
                if offset + 4 > data.len() {
                    break;
                }
                let len = u32::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]) as usize;
                offset += 4;

                if offset + len > data.len() {
                    break;
                }
                let bytes = data[offset..offset + len].to_vec();
                offset += len;
                PgColumnValue::Binary(bytes)
            }
            _ => PgColumnValue::Null,
        };

        values.push(value);
    }

    (values, offset)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_object() {
        let columns = vec![
            RelationColumn {
                name: "id".to_string(),
                type_oid: 23, // INT4
                type_modifier: -1,
                flags: 1,
            },
            RelationColumn {
                name: "name".to_string(),
                type_oid: 25, // TEXT
                type_modifier: -1,
                flags: 0,
            },
        ];

        let values = vec![
            PgColumnValue::Text("42".to_string()),
            PgColumnValue::Text("Alice".to_string()),
        ];

        let obj = build_object(&columns, &values);
        assert_eq!(obj["id"], json!(42));
        assert_eq!(obj["name"], json!("Alice"));
    }

    #[test]
    fn test_build_key_object() {
        let columns = vec![
            RelationColumn {
                name: "id".to_string(),
                type_oid: 23,
                type_modifier: -1,
                flags: 1, // Key
            },
            RelationColumn {
                name: "name".to_string(),
                type_oid: 25,
                type_modifier: -1,
                flags: 0, // Not key
            },
        ];

        let values = vec![
            PgColumnValue::Text("1".to_string()),
            PgColumnValue::Text("Bob".to_string()),
        ];

        let obj = build_key_object(&columns, &values);
        assert!(obj.get("id").is_some());
        assert!(obj.get("name").is_none());
    }

    #[test]
    fn test_convert_bool() {
        assert_eq!(convert_text_value("t", 16), json!(true));
        assert_eq!(convert_text_value("f", 16), json!(false));
        assert_eq!(convert_text_value("true", 16), json!(true));
        assert_eq!(convert_text_value("false", 16), json!(false));
    }

    #[test]
    fn test_convert_numbers() {
        assert_eq!(convert_text_value("42", 23), json!(42));
        assert_eq!(convert_text_value("-100", 20), json!(-100));
        assert_eq!(convert_text_value("3.15", 701), json!(3.15));
    }

    #[test]
    fn test_convert_json() {
        let json_str = r#"{"key": "value", "num": 42}"#;
        let result = convert_text_value(json_str, 114); // JSON
        assert_eq!(result["key"], json!("value"));
        assert_eq!(result["num"], json!(42));
    }

    #[test]
    fn test_convert_jsonb() {
        let json_str = r#"{"array": [1, 2, 3]}"#;
        let result = convert_text_value(json_str, 3802); // JSONB
        assert_eq!(result["array"], json!([1, 2, 3]));
    }

    #[test]
    fn test_convert_bytea() {
        let hex = "\\x48656c6c6f";
        let result = convert_text_value(hex, 17); // BYTEA
        assert!(result.get("_base64").is_some());
    }

    #[test]
    fn test_parse_array() {
        assert_eq!(parse_pg_array("{}"), json!([]));
        assert_eq!(parse_pg_array("{1,2,3}"), json!([1, 2, 3]));
        assert_eq!(
            parse_pg_array(r#"{"hello","world"}"#),
            json!(["hello", "world"])
        );
        assert_eq!(parse_pg_array("{1,NULL,3}"), json!([1, Value::Null, 3]));
    }

    #[test]
    fn test_null_and_unchanged() {
        let columns = vec![RelationColumn {
            name: "col".to_string(),
            type_oid: 25,
            type_modifier: -1,
            flags: 0,
        }];

        let null_obj = build_object(&columns, &[PgColumnValue::Null]);
        assert_eq!(null_obj["col"], Value::Null);

        let unchanged_obj = build_object(&columns, &[PgColumnValue::Unchanged]);
        assert!(unchanged_obj["col"].get("_unchanged").is_some());
    }
}
