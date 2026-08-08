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
            // declared scale (e.g. decimal(12,2) -> factor 100).
            let scale = c.scale.unwrap_or(0).max(0) as u32;
            let factor = 10f64.powi(scale as i32);
            json!({"type": "scaled_float", "scaling_factor": factor})
        }
        "date" | "datetime" | "timestamp" | "timestamptz" => {
            json!({"type": "date"})
        }
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
    fn core_type_mappings() {
        assert_eq!(es_field_type(&col("id", "bigint", "bigint"))["type"], "long");
        assert_eq!(es_field_type(&col("n", "int", "int"))["type"], "integer");
        assert_eq!(
            es_field_type(&col("ok", "boolean", "tinyint(1)"))["type"],
            "boolean"
        );
        assert_eq!(
            es_field_type(&col("t", "datetime", "datetime"))["type"],
            "date"
        );
        assert_eq!(es_field_type(&col("j", "json", "json"))["type"], "flattened");
        let v = es_field_type(&col("name", "varchar", "varchar(255)"));
        assert_eq!(v["type"], "text");
        assert_eq!(v["fields"]["keyword"]["type"], "keyword");
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
