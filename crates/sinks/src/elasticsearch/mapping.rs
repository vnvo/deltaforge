//! Generate an Elasticsearch index mapping from source column types.
//!
//! The sink already resolves the source schema (for the `_id` primary key), so
//! it can emit a *correct* explicit mapping — decimals as `scaled_float`, dates
//! as `date` — instead of relying on ES dynamic mapping, which infers decimals
//! as `float` and dates as `text`.

use crate::clickhouse::types::ColDesc;
use serde_json::{Value, json};

/// Map one source column to its Elasticsearch field-mapping JSON.
pub fn es_field_type(c: &ColDesc) -> Value {
    let dt = c.data_type.to_lowercase();
    match dt.as_str() {
        "boolean" | "bool" => json!({"type": "boolean"}),
        "tinyint" => json!({"type": "byte"}),
        "smallint" => json!({"type": "short"}),
        "int" | "integer" | "mediumint" => json!({"type": "integer"}),
        "bigint" => {
            if c.unsigned {
                json!({"type": "unsigned_long"})
            } else {
                json!({"type": "long"})
            }
        }
        "float" | "double" | "real" => json!({"type": "double"}),
        "decimal" | "numeric" => {
            // scaling_factor = 10^scale keeps `scaled_float` exact for the
            // declared scale (e.g. decimal(12,2) -> factor 100). Prefer the
            // numeric metadata; fall back to parsing the type string, since some
            // loaders (and Postgres) leave `scale` unset but carry it in
            // `full_type` — without this a decimal collapses to factor 1.
            let scale = c
                .scale
                .map(|s| s.max(0) as u32)
                .or_else(|| {
                    crate::clickhouse::types::parse_decimal_str(&c.full_type)
                        .map(|(_, s)| s)
                })
                .unwrap_or(0);
            let factor = 10f64.powi(scale as i32);
            json!({"type": "scaled_float", "scaling_factor": factor})
        }
        "date" | "datetime" | "timestamp" | "timestamptz" => {
            // Accept both encodings the source produces: converted epoch millis
            // (binlog TIMESTAMP, normalized) and ISO / MySQL datetime strings
            // (snapshot + binlog DATETIME).
            json!({
                "type": "date",
                "format": "epoch_millis||strict_date_optional_time||\
                           yyyy-MM-dd HH:mm:ss||yyyy-MM-dd HH:mm:ss.SSSSSS||\
                           yyyy-MM-dd"
            })
        }
        // TIME is a duration, not a point in time — keep it as an exact keyword.
        "time" => json!({"type": "keyword"}),
        // Binary/BLOB columns arrive base64-encoded; ES `binary` stores base64.
        "blob" | "tinyblob" | "mediumblob" | "longblob" | "binary"
        | "varbinary" => json!({"type": "binary"}),
        "json" | "jsonb" => json!({"type": "flattened"}),
        // varchar/text/uuid/enum/other -> text with a keyword sub-field so both
        // full-text search and exact-match/aggregations work.
        _ => json!({
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
        }),
    }
}

/// Build the `{ "properties": { ... } }` mapping body for a table's columns.
pub fn build_mapping(cols: &[ColDesc]) -> Value {
    let mut props = serde_json::Map::new();
    for c in cols {
        props.insert(c.name.clone(), es_field_type(c));
    }
    json!({ "properties": Value::Object(props) })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clickhouse::types::ColDesc;

    fn col(name: &str, dt: &str, full: &str) -> ColDesc {
        ColDesc {
            name: name.into(),
            data_type: dt.into(),
            full_type: full.into(),
            nullable: true,
            unsigned: false,
            precision: None,
            scale: None,
        }
    }

    #[test]
    fn decimal_maps_to_scaled_float_with_factor() {
        let c = ColDesc {
            precision: Some(12),
            scale: Some(2),
            ..col("amount", "decimal", "decimal(12,2)")
        };
        assert_eq!(
            es_field_type(&c),
            json!({"type": "scaled_float", "scaling_factor": 100.0})
        );
    }

    #[test]
    fn decimal_scale_parsed_from_type_when_metadata_missing() {
        // Mirrors the real loader path where scale is unset but the type string
        // carries it — must not collapse to scaling_factor 1.
        let c = col("amount", "decimal", "decimal(12,2)");
        assert_eq!(
            es_field_type(&c),
            json!({"type": "scaled_float", "scaling_factor": 100.0})
        );
    }

    #[test]
    fn core_type_mappings() {
        assert_eq!(
            es_field_type(&col("id", "bigint", "bigint"))["type"],
            "long"
        );
        assert_eq!(es_field_type(&col("n", "int", "int"))["type"], "integer");
        assert_eq!(
            es_field_type(&col("ok", "boolean", "tinyint(1)"))["type"],
            "boolean"
        );
        assert_eq!(
            es_field_type(&col("t", "datetime", "datetime"))["type"],
            "date"
        );
        assert_eq!(
            es_field_type(&col("j", "json", "json"))["type"],
            "flattened"
        );
        let v = es_field_type(&col("name", "varchar", "varchar(255)"));
        assert_eq!(v["type"], "text");
        assert_eq!(v["fields"]["keyword"]["type"], "keyword");
    }

    #[test]
    fn binary_time_and_date_format_mappings() {
        assert_eq!(es_field_type(&col("b", "blob", "blob"))["type"], "binary");
        assert_eq!(
            es_field_type(&col("bin", "varbinary", "varbinary(16)"))["type"],
            "binary"
        );
        assert_eq!(es_field_type(&col("t", "time", "time"))["type"], "keyword");
        // date/datetime/timestamp carry a permissive format that includes
        // epoch_millis (for the normalized numeric timestamp).
        let ts = es_field_type(&col("ts", "timestamp", "timestamp"));
        assert_eq!(ts["type"], "date");
        assert!(
            ts["format"].as_str().unwrap().contains("epoch_millis"),
            "date format must accept epoch_millis: {ts}"
        );
    }

    #[test]
    fn unsigned_bigint_maps_to_unsigned_long() {
        let c = ColDesc {
            unsigned: true,
            ..col("x", "bigint", "bigint unsigned")
        };
        assert_eq!(es_field_type(&c)["type"], "unsigned_long");
    }

    #[test]
    fn build_mapping_wraps_properties() {
        let cols = vec![
            col("id", "bigint", "bigint"),
            col("name", "varchar", "varchar(50)"),
        ];
        let m = build_mapping(&cols);
        assert_eq!(m["properties"]["id"]["type"], "long");
        assert_eq!(m["properties"]["name"]["type"], "text");
    }
}
