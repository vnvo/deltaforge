//! CDC envelope Avro schema construction.
//!
//! Defines the exact Avro record structure for DeltaForge CDC events:
//!
//! ```text
//! Envelope (top-level)
//! ├── before: ["null", Value]       ← row record from DDL
//! ├── after:  ["null", Value]       ← same record type, reused by name
//! ├── source: Source                ← common metadata
//! │   └── position: ["null", <ConnectorPosition>]
//! ├── op: string
//! ├── ts_ms: long
//! ├── event_id: ["null", string]
//! ├── schema_version: ["null", string]
//! └── transaction: ["null", Transaction]
//! ```
//!
//! The `Value` record is supplied externally (from DDL type conversion or
//! JSON inference). Everything else is a fixed DeltaForge-defined shape.

use apache_avro::Schema as AvroSchema;
use serde_json::json;

use super::EncodingError;

// =============================================================================
// Connector position schemas
// =============================================================================

/// MySQL binlog position schema JSON.
fn mysql_position_schema() -> serde_json::Value {
    json!({
        "type": "record",
        "name": "Position",
        "namespace": "deltaforge.cdc.mysql",
        "doc": "MySQL binlog position.",
        "fields": [
            {"name": "server_id", "type": ["null", "int"], "default": null},
            {"name": "file", "type": ["null", "string"], "default": null},
            {"name": "pos", "type": ["null", "long"], "default": null},
            {"name": "gtid", "type": ["null", "string"], "default": null},
            {"name": "row", "type": ["null", "int"], "default": null}
        ]
    })
}

/// PostgreSQL WAL position schema JSON.
fn postgres_position_schema() -> serde_json::Value {
    json!({
        "type": "record",
        "name": "Position",
        "namespace": "deltaforge.cdc.postgres",
        "doc": "PostgreSQL WAL position.",
        "fields": [
            {"name": "lsn", "type": ["null", "string"], "default": null},
            {"name": "tx_id", "type": ["null", "long"], "default": null},
            {"name": "xmin", "type": ["null", "long"], "default": null}
        ]
    })
}

/// Turso/libSQL position schema JSON.
fn turso_position_schema() -> serde_json::Value {
    json!({
        "type": "record",
        "name": "Position",
        "namespace": "deltaforge.cdc.turso",
        "doc": "Turso/libSQL change position.",
        "fields": [
            {"name": "change_id", "type": ["null", "long"], "default": null},
            {"name": "sequence", "type": ["null", "string"], "default": null}
        ]
    })
}

/// Get the position schema for a connector type.
fn position_schema_for(connector: &str) -> serde_json::Value {
    match connector {
        "mysql" => mysql_position_schema(),
        "postgresql" | "postgres" => postgres_position_schema(),
        "turso" | "libsql" => turso_position_schema(),
        _ => {
            // Generic fallback: just a sequence string
            json!({
                "type": "record",
                "name": "Position",
                "namespace": "deltaforge.cdc.generic",
                "doc": "Generic source position.",
                "fields": [
                    {"name": "sequence", "type": ["null", "string"], "default": null}
                ]
            })
        }
    }
}

// =============================================================================
// Source metadata schema
// =============================================================================

/// Build the Source metadata record schema for a given connector.
fn source_schema(connector: &str) -> serde_json::Value {
    json!({
        "type": "record",
        "name": "Source",
        "namespace": "deltaforge.cdc",
        "doc": "CDC source metadata.",
        "fields": [
            {"name": "version", "type": "string"},
            {"name": "connector", "type": "string"},
            {"name": "name", "type": "string"},
            {"name": "ts_ms", "type": "long"},
            {"name": "db", "type": "string"},
            {"name": "schema", "type": ["null", "string"], "default": null},
            {"name": "table", "type": "string"},
            {"name": "snapshot", "type": ["null", "string"], "default": null},
            {"name": "position", "type": ["null", position_schema_for(connector)], "default": null}
        ]
    })
}

// =============================================================================
// Transaction schema
// =============================================================================

fn transaction_schema() -> serde_json::Value {
    json!({
        "type": "record",
        "name": "Transaction",
        "namespace": "deltaforge.cdc",
        "doc": "Transaction metadata.",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "total_order", "type": ["null", "long"], "default": null},
            {"name": "data_collection_order", "type": ["null", "long"], "default": null}
        ]
    })
}

// =============================================================================
// Full CDC envelope schema
// =============================================================================

/// Build the complete CDC envelope Avro schema.
///
/// # Arguments
///
/// * `connector` - Source connector type ("mysql", "postgresql", etc.)
/// * `db` - Database name
/// * `table` - Table name
/// * `value_schema` - The row `Value` record as a JSON schema object.
///   This is either derived from source DDL (Path A) or inferred from
///   JSON (Path C fallback). It must be a record type with namespace
///   `deltaforge.{connector}.{db}.{table}` and name `Value`.
///
/// # Returns
///
/// A parsed `AvroSchema` for the full CDC envelope, or an error if the
/// schema JSON is invalid.
pub fn build_envelope_schema(
    connector: &str,
    db: &str,
    table: &str,
    value_schema: serde_json::Value,
) -> Result<(String, AvroSchema), EncodingError> {
    let envelope_ns =
        format!("deltaforge.cdc.{connector}.{db}.{table}");
    let value_fqn = format!(
        "{}.Value",
        value_schema["namespace"]
            .as_str()
            .unwrap_or(&format!("deltaforge.{connector}.{db}.{table}"))
    );

    let envelope_json = json!({
        "type": "record",
        "name": "Envelope",
        "namespace": envelope_ns,
        "doc": format!("CDC envelope for {connector}.{db}.{table}. Schema source: DDL (authoritative)."),
        "fields": [
            {
                "name": "before",
                "type": ["null", value_schema],
                "default": null
            },
            {
                "name": "after",
                "type": ["null", value_fqn],
                "default": null
            },
            {
                "name": "source",
                "type": source_schema(connector)
            },
            {
                "name": "op",
                "type": "string"
            },
            {
                "name": "ts_ms",
                "type": "long"
            },
            {
                "name": "event_id",
                "type": ["null", "string"],
                "default": null
            },
            {
                "name": "schema_version",
                "type": ["null", "string"],
                "default": null
            },
            {
                "name": "transaction",
                "type": ["null", transaction_schema()],
                "default": null
            }
        ]
    });

    let schema_str = serde_json::to_string(&envelope_json).map_err(|e| {
        EncodingError::Avro(format!(
            "failed to serialize envelope schema: {e}"
        ))
    })?;

    let schema = AvroSchema::parse_str(&schema_str).map_err(|e| {
        EncodingError::Avro(format!(
            "invalid envelope schema for {connector}.{db}.{table}: {e}"
        ))
    })?;

    Ok((schema_str, schema))
}

/// Build a simple `Value` record schema from field definitions.
///
/// This is the Path C fallback — used when no source DDL is available.
/// Path A (DDL-derived) builds the Value record directly from column types.
///
/// # Arguments
///
/// * `connector` - Source connector type
/// * `db` - Database name
/// * `table` - Table name
/// * `fields` - Avro field definitions as JSON array elements
pub fn build_value_schema(
    connector: &str,
    db: &str,
    table: &str,
    fields: Vec<serde_json::Value>,
) -> serde_json::Value {
    json!({
        "type": "record",
        "name": "Value",
        "namespace": format!("deltaforge.{connector}.{db}.{table}"),
        "fields": fields
    })
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_mysql_envelope_schema_parses() {
        let value = build_value_schema("mysql", "shop", "orders", vec![
            json!({"name": "id", "type": "long"}),
            json!({"name": "name", "type": ["null", "string"], "default": null}),
            json!({"name": "total", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}}),
        ]);

        let (schema_str, schema) =
            build_envelope_schema("mysql", "shop", "orders", value)
                .expect("should build valid schema");

        // Should parse as a Record
        assert!(
            matches!(schema, AvroSchema::Record(_)),
            "envelope should be a Record"
        );

        // Schema string should be valid JSON
        let _: serde_json::Value =
            serde_json::from_str(&schema_str).expect("should be valid JSON");
    }

    #[test]
    fn build_postgres_envelope_schema_parses() {
        let value = build_value_schema(
            "postgresql",
            "mydb",
            "users",
            vec![
                json!({"name": "id", "type": "int"}),
                json!({"name": "email", "type": ["null", "string"], "default": null}),
            ],
        );

        let (_schema_str, schema) =
            build_envelope_schema("postgresql", "mydb", "users", value)
                .expect("should build valid schema");

        assert!(matches!(schema, AvroSchema::Record(_)));
    }

    #[test]
    fn envelope_has_expected_fields() {
        let value = build_value_schema(
            "mysql",
            "test",
            "t1",
            vec![json!({"name": "id", "type": "long"})],
        );

        let (schema_str, _schema) =
            build_envelope_schema("mysql", "test", "t1", value).unwrap();

        let parsed: serde_json::Value =
            serde_json::from_str(&schema_str).unwrap();
        let fields = parsed["fields"].as_array().unwrap();
        let field_names: Vec<&str> = fields
            .iter()
            .map(|f| f["name"].as_str().unwrap())
            .collect();

        assert_eq!(
            field_names,
            vec![
                "before",
                "after",
                "source",
                "op",
                "ts_ms",
                "event_id",
                "schema_version",
                "transaction"
            ]
        );
    }

    #[test]
    fn source_has_nested_position() {
        let value = build_value_schema(
            "mysql",
            "test",
            "t1",
            vec![json!({"name": "id", "type": "long"})],
        );

        let (schema_str, _) =
            build_envelope_schema("mysql", "test", "t1", value).unwrap();

        let parsed: serde_json::Value =
            serde_json::from_str(&schema_str).unwrap();

        // Find the source field
        let source_field = parsed["fields"]
            .as_array()
            .unwrap()
            .iter()
            .find(|f| f["name"] == "source")
            .unwrap();

        // Source should be a record with a position field
        let source_fields = source_field["type"]["fields"]
            .as_array()
            .unwrap();
        let field_names: Vec<&str> = source_fields
            .iter()
            .map(|f| f["name"].as_str().unwrap())
            .collect();

        assert!(
            field_names.contains(&"position"),
            "source should have position field"
        );
        assert!(
            field_names.contains(&"connector"),
            "source should have connector field"
        );
        assert!(
            field_names.contains(&"db"),
            "source should have db field"
        );
    }

    #[test]
    fn mysql_position_has_binlog_fields() {
        let pos = mysql_position_schema();
        let fields = pos["fields"].as_array().unwrap();
        let names: Vec<&str> = fields
            .iter()
            .map(|f| f["name"].as_str().unwrap())
            .collect();

        assert!(names.contains(&"file"));
        assert!(names.contains(&"pos"));
        assert!(names.contains(&"gtid"));
        assert!(names.contains(&"server_id"));
    }

    #[test]
    fn postgres_position_has_wal_fields() {
        let pos = postgres_position_schema();
        let fields = pos["fields"].as_array().unwrap();
        let names: Vec<&str> = fields
            .iter()
            .map(|f| f["name"].as_str().unwrap())
            .collect();

        assert!(names.contains(&"lsn"));
        assert!(names.contains(&"tx_id"));
    }

    #[test]
    fn before_and_after_share_value_type() {
        let value = build_value_schema(
            "mysql",
            "shop",
            "items",
            vec![json!({"name": "id", "type": "long"})],
        );

        let (schema_str, _) =
            build_envelope_schema("mysql", "shop", "items", value).unwrap();
        let parsed: serde_json::Value =
            serde_json::from_str(&schema_str).unwrap();
        let fields = parsed["fields"].as_array().unwrap();

        // `before` should inline the Value record
        let before_type = &fields[0]["type"];
        assert_eq!(before_type[0], "null");
        // The second element should be the full record
        assert_eq!(before_type[1]["name"], "Value");

        // `after` should reference Value by fully-qualified name
        let after_type = &fields[1]["type"];
        assert_eq!(after_type[0], "null");
        assert!(
            after_type[1].as_str().unwrap().contains("Value"),
            "after should reference Value by name: {:?}",
            after_type[1]
        );
    }

    #[test]
    fn envelope_namespace_includes_connector_and_table() {
        let value = build_value_schema(
            "mysql",
            "shop",
            "orders",
            vec![json!({"name": "id", "type": "long"})],
        );

        let (schema_str, _) =
            build_envelope_schema("mysql", "shop", "orders", value).unwrap();
        let parsed: serde_json::Value =
            serde_json::from_str(&schema_str).unwrap();

        assert_eq!(
            parsed["namespace"].as_str().unwrap(),
            "deltaforge.cdc.mysql.shop.orders"
        );
        assert_eq!(parsed["name"].as_str().unwrap(), "Envelope");
    }

    #[test]
    fn unknown_connector_uses_generic_position() {
        let value = build_value_schema(
            "mongodb",
            "test",
            "col",
            vec![json!({"name": "id", "type": "string"})],
        );

        // Should not fail — uses generic position
        let result =
            build_envelope_schema("mongodb", "test", "col", value);
        assert!(result.is_ok());
    }
}
