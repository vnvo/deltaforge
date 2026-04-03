//! Source column type → Avro type conversion.
//!
//! Converts MySQL and PostgreSQL column types to Avro field definitions,
//! following the type policies defined in the Avro Schema Registry RFC:
//!
//! - **Exact:** lossless 1:1 mapping
//! - **Lossy:** semantics partially lost (e.g., UDT → string); warning emitted
//! - **Unsafe:** configurable behavior (e.g., BIGINT UNSIGNED → string or long)
//!
//! This module operates on type name strings (not source crate types) to
//! avoid circular dependencies between core and sources.

use serde_json::{Value, json};
use tracing::warn;

// =============================================================================
// Configuration
// =============================================================================

/// Type conversion options (from sink encoding config).
#[derive(Debug, Clone)]
pub struct TypeConversionOpts {
    /// How to map MySQL BIGINT UNSIGNED.
    /// `string` (default, safe) or `long` (risk of overflow for values >= 2^63).
    pub unsigned_bigint_mode: UnsignedBigintMode,

    /// How to map MySQL/PostgreSQL ENUM types.
    /// `string` (default, safe) or `enum` (strict typing, compatibility risk on symbol changes).
    pub enum_mode: EnumMode,

    /// How to map naive (timezone-unaware) timestamps.
    /// `string` (default, ISO-8601) or `timestamp` (Avro logical type, semantically misleading).
    pub naive_timestamp_mode: NaiveTimestampMode,
}

impl Default for TypeConversionOpts {
    fn default() -> Self {
        Self {
            unsigned_bigint_mode: UnsignedBigintMode::String,
            enum_mode: EnumMode::String,
            naive_timestamp_mode: NaiveTimestampMode::String,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum UnsignedBigintMode {
    #[default]
    String,
    Long,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum EnumMode {
    #[default]
    String,
    Enum,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum NaiveTimestampMode {
    #[default]
    String,
    Timestamp,
}

// =============================================================================
// Column description (connector-agnostic input)
// =============================================================================

/// Describes a source column for Avro type conversion.
///
/// Built from `MySqlColumn` or `PostgresColumn` at the call site —
/// this module does not depend on the sources crate.
#[derive(Debug, Clone)]
pub struct ColumnDesc {
    /// Column name.
    pub name: String,
    /// Base data type (lowercase): "bigint", "varchar", "timestamp", etc.
    pub data_type: String,
    /// Full column type string (e.g., "bigint(20) unsigned", "character varying(255)").
    pub column_type: String,
    /// Whether the column allows NULL.
    pub nullable: bool,
    /// Numeric precision (for DECIMAL/NUMERIC).
    pub precision: Option<i64>,
    /// Numeric scale (for DECIMAL/NUMERIC).
    pub scale: Option<i64>,
    /// Whether the type is unsigned (MySQL).
    pub unsigned: bool,
    /// Whether the type is an array (PostgreSQL).
    pub is_array: bool,
    /// Array element type (PostgreSQL).
    pub element_type: Option<String>,
}

// =============================================================================
// MySQL type conversion
// =============================================================================

/// Convert a MySQL column to an Avro field definition (JSON).
pub fn mysql_column_to_avro(
    col: &ColumnDesc,
    opts: &TypeConversionOpts,
) -> Value {
    let avro_type = mysql_type_to_avro(&col.data_type, col, opts);
    wrap_field(&col.name, avro_type, col.nullable)
}

fn mysql_type_to_avro(
    data_type: &str,
    col: &ColumnDesc,
    opts: &TypeConversionOpts,
) -> Value {
    match data_type {
        // Integer types
        "tinyint" | "smallint" | "mediumint" => json!("int"),
        "int" | "integer" => {
            if col.unsigned {
                // INT UNSIGNED fits in long
                json!("long")
            } else {
                json!("int")
            }
        }
        "bigint" => {
            if col.unsigned {
                match opts.unsigned_bigint_mode {
                    UnsignedBigintMode::String => {
                        warn!(
                            column = %col.name,
                            "BIGINT UNSIGNED mapped to string (safe default)"
                        );
                        json!("string")
                    }
                    UnsignedBigintMode::Long => {
                        warn!(
                            column = %col.name,
                            "BIGINT UNSIGNED mapped to long — values >= 2^63 will fail encoding"
                        );
                        json!("long")
                    }
                }
            } else {
                json!("long")
            }
        }

        // Floating point
        "float" => json!("float"),
        "double" | "real" => json!("double"),

        // Fixed-point decimal
        "decimal" | "numeric" => {
            if let (Some(p), Some(s)) = (col.precision, col.scale) {
                json!({
                    "type": "bytes",
                    "logicalType": "decimal",
                    "precision": p,
                    "scale": s
                })
            } else {
                warn!(
                    column = %col.name,
                    "DECIMAL without precision/scale — mapping to string"
                );
                json!("string")
            }
        }

        // String types
        "varchar" | "char" | "text" | "tinytext" | "mediumtext"
        | "longtext" => json!("string"),

        // Binary types
        "binary" | "varbinary" | "blob" | "tinyblob" | "mediumblob"
        | "longblob" => json!("bytes"),

        // Date/time types
        "date" => json!({"type": "int", "logicalType": "date"}),
        "datetime" => match opts.naive_timestamp_mode {
            NaiveTimestampMode::String => json!("string"),
            NaiveTimestampMode::Timestamp => {
                json!({"type": "long", "logicalType": "timestamp-millis"})
            }
        },
        "timestamp" => {
            json!({"type": "long", "logicalType": "timestamp-millis"})
        }
        "time" => json!({"type": "long", "logicalType": "time-millis"}),
        "year" => json!("int"),

        // Boolean
        "boolean" | "bool" | "bit" if col.precision == Some(1) => {
            json!("boolean")
        }

        // BIT(n > 1)
        "bit" => json!("bytes"),

        // JSON
        "json" => json!("string"),

        // ENUM
        "enum" => match opts.enum_mode {
            EnumMode::String => json!("string"),
            EnumMode::Enum => {
                // Parse enum values from column_type: "enum('a','b','c')"
                let symbols = parse_mysql_enum_values(&col.column_type);
                if symbols.is_empty() {
                    json!("string")
                } else {
                    json!({
                        "type": "enum",
                        "name": format!("{}_enum", col.name),
                        "symbols": symbols
                    })
                }
            }
        },

        // SET
        "set" => json!("string"),

        // Catch-all: map to string with warning
        other => {
            warn!(
                column = %col.name,
                data_type = other,
                "unknown MySQL type — mapping to string (lossy)"
            );
            json!("string")
        }
    }
}

/// Parse enum values from MySQL column_type like "enum('a','b','c')".
fn parse_mysql_enum_values(column_type: &str) -> Vec<String> {
    // Extract content between enum(...) or ENUM(...)
    let lower = column_type.to_lowercase();
    let start = match lower.find("enum(") {
        Some(i) => i + 5,
        None => return vec![],
    };
    let end = match lower[start..].find(')') {
        Some(i) => start + i,
        None => return vec![],
    };

    column_type[start..end]
        .split(',')
        .map(|s| s.trim().trim_matches('\'').to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

// =============================================================================
// PostgreSQL type conversion
// =============================================================================

/// Convert a PostgreSQL column to an Avro field definition (JSON).
pub fn postgres_column_to_avro(
    col: &ColumnDesc,
    opts: &TypeConversionOpts,
) -> Value {
    if col.is_array {
        let element_type = col.element_type.as_deref().unwrap_or("text");
        let item_avro = postgres_scalar_to_avro(element_type, col, opts);
        let array_type = json!({
            "type": "array",
            "items": item_avro
        });
        return wrap_field(&col.name, array_type, col.nullable);
    }

    let avro_type = postgres_scalar_to_avro(&col.data_type, col, opts);
    wrap_field(&col.name, avro_type, col.nullable)
}

fn postgres_scalar_to_avro(
    data_type: &str,
    col: &ColumnDesc,
    opts: &TypeConversionOpts,
) -> Value {
    // Normalize: strip "character varying" → "varchar", etc.
    let normalized = normalize_pg_type(data_type);

    match normalized.as_str() {
        // Integer types
        "smallint" | "int2" => json!("int"),
        "integer" | "int" | "int4" => json!("int"),
        "bigint" | "int8" => json!("long"),
        "serial" => json!("int"),
        "bigserial" => json!("long"),
        "smallserial" => json!("int"),

        // Floating point
        "real" | "float4" => json!("float"),
        "double precision" | "float8" => json!("double"),

        // Fixed-point decimal
        "numeric" | "decimal" => {
            if let (Some(p), Some(s)) = (col.precision, col.scale) {
                json!({
                    "type": "bytes",
                    "logicalType": "decimal",
                    "precision": p,
                    "scale": s
                })
            } else {
                warn!(
                    column = %col.name,
                    "NUMERIC without precision — mapping to string"
                );
                json!("string")
            }
        }

        // Boolean
        "boolean" | "bool" => json!("boolean"),

        // String types
        "text" | "varchar" | "character varying" | "char" | "character"
        | "name" | "citext" => json!("string"),

        // Binary
        "bytea" => json!("bytes"),

        // Date/time types
        "date" => json!({"type": "int", "logicalType": "date"}),
        "timestamp" | "timestamp without time zone" => {
            match opts.naive_timestamp_mode {
                NaiveTimestampMode::String => json!("string"),
                NaiveTimestampMode::Timestamp => {
                    json!({"type": "long", "logicalType": "timestamp-micros"})
                }
            }
        }
        "timestamptz" | "timestamp with time zone" => {
            json!({"type": "long", "logicalType": "timestamp-micros"})
        }
        "time" | "time without time zone" => {
            json!({"type": "long", "logicalType": "time-micros"})
        }
        "timetz" | "time with time zone" => {
            // Offset is lost if we use time-micros; use string to preserve it
            json!("string")
        }
        "interval" => json!("string"),

        // UUID
        "uuid" => json!({"type": "string", "logicalType": "uuid"}),

        // JSON
        "json" | "jsonb" => json!("string"),

        // Network types
        "inet" | "cidr" | "macaddr" | "macaddr8" => json!("string"),

        // Geometric types
        "point" | "line" | "lseg" | "box" | "path" | "polygon" | "circle" => {
            json!("string")
        }

        // hstore
        "hstore" => json!({"type": "map", "values": "string"}),

        // Range types
        "int4range" | "int8range" | "numrange" | "tsrange" | "tstzrange"
        | "daterange" => json!("string"),

        // Money
        "money" => json!("string"),

        // XML
        "xml" => json!("string"),

        // Catch-all
        other => {
            warn!(
                column = %col.name,
                data_type = other,
                "unknown PostgreSQL type — mapping to string (lossy)"
            );
            json!("string")
        }
    }
}

/// Normalize PostgreSQL type names to canonical short forms.
fn normalize_pg_type(data_type: &str) -> String {
    let lower = data_type.to_lowercase().trim().to_string();

    // Strip precision/length suffixes for matching
    if lower.starts_with("character varying") {
        return "varchar".to_string();
    }
    if lower.starts_with("character(") || lower == "character" {
        return "char".to_string();
    }
    if lower.starts_with("numeric(") {
        return "numeric".to_string();
    }
    if lower.starts_with("decimal(") {
        return "decimal".to_string();
    }
    if lower.starts_with("timestamp with time zone") {
        return "timestamptz".to_string();
    }
    if lower.starts_with("timestamp without time zone")
        || lower.starts_with("timestamp(")
        || lower == "timestamp"
    {
        // Distinguish: "timestamp with time zone" already matched above
        if lower.contains("with time zone") {
            return "timestamptz".to_string();
        }
        return "timestamp".to_string();
    }
    if lower.starts_with("time with time zone") {
        return "timetz".to_string();
    }
    if lower.starts_with("time without time zone")
        || lower.starts_with("time(")
        || lower == "time"
    {
        if lower.contains("with time zone") {
            return "timetz".to_string();
        }
        return "time".to_string();
    }
    if lower.starts_with("double precision") {
        return "double precision".to_string();
    }

    // Return the lowercased type name, trimming parenthesized suffixes
    if let Some(paren) = lower.find('(') {
        lower[..paren].trim_end().to_string()
    } else {
        lower
    }
}

// =============================================================================
// Shared helpers
// =============================================================================

/// Wrap an Avro type in a field definition with optional null union.
fn wrap_field(name: &str, avro_type: Value, nullable: bool) -> Value {
    if nullable {
        json!({
            "name": name,
            "type": ["null", avro_type],
            "default": null
        })
    } else {
        json!({
            "name": name,
            "type": avro_type
        })
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn col(
        name: &str,
        data_type: &str,
        column_type: &str,
        nullable: bool,
    ) -> ColumnDesc {
        ColumnDesc {
            name: name.to_string(),
            data_type: data_type.to_string(),
            column_type: column_type.to_string(),
            nullable,
            precision: None,
            scale: None,
            unsigned: false,
            is_array: false,
            element_type: None,
        }
    }

    fn col_with_precision(
        name: &str,
        data_type: &str,
        nullable: bool,
        precision: i64,
        scale: i64,
    ) -> ColumnDesc {
        ColumnDesc {
            precision: Some(precision),
            scale: Some(scale),
            ..col(name, data_type, data_type, nullable)
        }
    }

    fn unsigned_col(
        name: &str,
        data_type: &str,
        column_type: &str,
    ) -> ColumnDesc {
        ColumnDesc {
            unsigned: true,
            ..col(name, data_type, column_type, false)
        }
    }

    fn opts() -> TypeConversionOpts {
        TypeConversionOpts::default()
    }

    // --- MySQL integer types ---

    #[test]
    fn mysql_int_types() {
        let o = opts();
        let f =
            mysql_column_to_avro(&col("a", "tinyint", "tinyint", false), &o);
        assert_eq!(f["type"], "int");

        let f = mysql_column_to_avro(&col("a", "int", "int", false), &o);
        assert_eq!(f["type"], "int");

        let f = mysql_column_to_avro(&col("a", "bigint", "bigint", false), &o);
        assert_eq!(f["type"], "long");
    }

    #[test]
    fn mysql_int_unsigned() {
        let o = opts();
        let f =
            mysql_column_to_avro(&unsigned_col("a", "int", "int unsigned"), &o);
        assert_eq!(f["type"], "long"); // INT UNSIGNED fits in long

        // BIGINT UNSIGNED → string by default
        let f = mysql_column_to_avro(
            &unsigned_col("a", "bigint", "bigint unsigned"),
            &o,
        );
        assert_eq!(f["type"], "string");
    }

    #[test]
    fn mysql_bigint_unsigned_long_mode() {
        let o = TypeConversionOpts {
            unsigned_bigint_mode: UnsignedBigintMode::Long,
            ..opts()
        };
        let f = mysql_column_to_avro(
            &unsigned_col("a", "bigint", "bigint unsigned"),
            &o,
        );
        assert_eq!(f["type"], "long");
    }

    #[test]
    fn mysql_nullable_wraps_in_union() {
        let o = opts();
        let f = mysql_column_to_avro(
            &col("email", "varchar", "varchar(255)", true),
            &o,
        );
        // Should be ["null", "string"]
        let t = &f["type"];
        assert!(t.is_array());
        assert_eq!(t[0], "null");
        assert_eq!(t[1], "string");
        assert_eq!(f["default"], Value::Null);
    }

    #[test]
    fn mysql_not_null_no_union() {
        let o = opts();
        let f = mysql_column_to_avro(&col("id", "bigint", "bigint", false), &o);
        assert_eq!(f["type"], "long");
        assert!(f.get("default").is_none());
    }

    #[test]
    fn mysql_decimal() {
        let o = opts();
        let f = mysql_column_to_avro(
            &col_with_precision("total", "decimal", false, 10, 2),
            &o,
        );
        let t = &f["type"];
        assert_eq!(t["logicalType"], "decimal");
        assert_eq!(t["precision"], 10);
        assert_eq!(t["scale"], 2);
    }

    #[test]
    fn mysql_decimal_no_precision_fallback() {
        let o = opts();
        let f =
            mysql_column_to_avro(&col("x", "decimal", "decimal", false), &o);
        assert_eq!(f["type"], "string");
    }

    #[test]
    fn mysql_datetime_default_string() {
        let o = opts();
        let f = mysql_column_to_avro(
            &col("created", "datetime", "datetime", false),
            &o,
        );
        assert_eq!(f["type"], "string");
    }

    #[test]
    fn mysql_datetime_timestamp_mode() {
        let o = TypeConversionOpts {
            naive_timestamp_mode: NaiveTimestampMode::Timestamp,
            ..opts()
        };
        let f = mysql_column_to_avro(
            &col("created", "datetime", "datetime", false),
            &o,
        );
        assert_eq!(f["type"]["logicalType"], "timestamp-millis");
    }

    #[test]
    fn mysql_timestamp_always_logical() {
        let o = opts();
        let f = mysql_column_to_avro(
            &col("updated", "timestamp", "timestamp", false),
            &o,
        );
        assert_eq!(f["type"]["logicalType"], "timestamp-millis");
    }

    #[test]
    fn mysql_enum_default_string() {
        let o = opts();
        let f = mysql_column_to_avro(
            &col("status", "enum", "enum('a','b','c')", false),
            &o,
        );
        assert_eq!(f["type"], "string");
    }

    #[test]
    fn mysql_enum_enum_mode() {
        let o = TypeConversionOpts {
            enum_mode: EnumMode::Enum,
            ..opts()
        };
        let f = mysql_column_to_avro(
            &col("status", "enum", "enum('pending','shipped')", false),
            &o,
        );
        assert_eq!(f["type"]["type"], "enum");
        let symbols = f["type"]["symbols"].as_array().unwrap();
        assert_eq!(symbols.len(), 2);
    }

    #[test]
    fn mysql_json() {
        let o = opts();
        let f = mysql_column_to_avro(&col("data", "json", "json", true), &o);
        assert_eq!(f["type"][1], "string");
    }

    // --- Parse enum ---

    #[test]
    fn parse_mysql_enum_values_works() {
        let vals = parse_mysql_enum_values("enum('a','b','c')");
        assert_eq!(vals, vec!["a", "b", "c"]);
    }

    #[test]
    fn parse_mysql_enum_values_empty() {
        let vals = parse_mysql_enum_values("varchar(255)");
        assert!(vals.is_empty());
    }

    // --- PostgreSQL types ---

    #[test]
    fn pg_int_types() {
        let o = opts();
        let f =
            postgres_column_to_avro(&col("a", "integer", "integer", false), &o);
        assert_eq!(f["type"], "int");

        let f =
            postgres_column_to_avro(&col("a", "bigint", "bigint", false), &o);
        assert_eq!(f["type"], "long");
    }

    #[test]
    fn pg_numeric_with_precision() {
        let o = opts();
        let f = postgres_column_to_avro(
            &col_with_precision("total", "numeric", false, 12, 4),
            &o,
        );
        assert_eq!(f["type"]["logicalType"], "decimal");
        assert_eq!(f["type"]["precision"], 12);
    }

    #[test]
    fn pg_numeric_no_precision() {
        let o = opts();
        let f = postgres_column_to_avro(
            &col("amount", "numeric", "numeric", false),
            &o,
        );
        assert_eq!(f["type"], "string");
    }

    #[test]
    fn pg_timestamp_naive_default_string() {
        let o = opts();
        let f = postgres_column_to_avro(
            &col("created", "timestamp", "timestamp without time zone", false),
            &o,
        );
        assert_eq!(f["type"], "string");
    }

    #[test]
    fn pg_timestamptz_always_logical() {
        let o = opts();
        let f = postgres_column_to_avro(
            &col("updated", "timestamptz", "timestamp with time zone", false),
            &o,
        );
        assert_eq!(f["type"]["logicalType"], "timestamp-micros");
    }

    #[test]
    fn pg_timetz_string() {
        let o = opts();
        let f = postgres_column_to_avro(
            &col("t", "timetz", "time with time zone", false),
            &o,
        );
        assert_eq!(f["type"], "string");
    }

    #[test]
    fn pg_uuid() {
        let o = opts();
        let f = postgres_column_to_avro(&col("id", "uuid", "uuid", false), &o);
        assert_eq!(f["type"]["logicalType"], "uuid");
    }

    #[test]
    fn pg_jsonb() {
        let o = opts();
        let f =
            postgres_column_to_avro(&col("data", "jsonb", "jsonb", true), &o);
        assert_eq!(f["type"][1], "string");
    }

    #[test]
    fn pg_array() {
        let o = opts();
        let c = ColumnDesc {
            is_array: true,
            element_type: Some("integer".to_string()),
            ..col("tags", "ARRAY", "integer[]", true)
        };
        let f = postgres_column_to_avro(&c, &o);
        let t = &f["type"];
        assert_eq!(t[0], "null");
        assert_eq!(t[1]["type"], "array");
        assert_eq!(t[1]["items"], "int");
    }

    #[test]
    fn pg_hstore() {
        let o = opts();
        let f = postgres_column_to_avro(
            &col("meta", "hstore", "hstore", false),
            &o,
        );
        assert_eq!(f["type"]["type"], "map");
    }

    #[test]
    fn pg_character_varying_normalized() {
        let o = opts();
        let f = postgres_column_to_avro(
            &col("name", "character varying", "character varying(255)", false),
            &o,
        );
        assert_eq!(f["type"], "string");
    }

    #[test]
    fn pg_boolean() {
        let o = opts();
        let f = postgres_column_to_avro(
            &col("active", "boolean", "boolean", false),
            &o,
        );
        assert_eq!(f["type"], "boolean");
    }

    // --- Normalize PG type ---

    #[test]
    fn normalize_pg_types() {
        assert_eq!(normalize_pg_type("character varying(255)"), "varchar");
        assert_eq!(
            normalize_pg_type("timestamp with time zone"),
            "timestamptz"
        );
        assert_eq!(
            normalize_pg_type("timestamp without time zone"),
            "timestamp"
        );
        assert_eq!(normalize_pg_type("double precision"), "double precision");
        assert_eq!(normalize_pg_type("integer"), "integer");
        assert_eq!(normalize_pg_type("numeric(10,2)"), "numeric");
    }
}
