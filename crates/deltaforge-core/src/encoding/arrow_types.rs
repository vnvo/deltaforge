//! Source column type → Arrow `Field` conversion.
//!
//! Parallel to `avro_types.rs`, but produces Apache Arrow `Field` definitions
//! suitable for Parquet output. Mappings are 1:1 with the Avro path except
//! where Arrow/Parquet supports a richer native type — most notably:
//!
//! - **DECIMAL**: Avro maps to `string` (TD-003). Arrow uses `Decimal128(p, s)`
//!   natively, which is what Parquet expects. This fixes TD-003 for the
//!   Parquet sink without changing the Avro sink.
//! - **TIMESTAMP**: Avro stores as `long` with logical type. Arrow has
//!   `Timestamp(unit, tz)` as a first-class type, matching Parquet INT64
//!   with `isAdjustedToUTC` metadata.
//!
//! Reuses `ColumnDesc` and `TypeConversionOpts` from `avro_types` so the
//! same sink config drives both encodings.

use arrow_schema::{DataType, Field, TimeUnit};
use tracing::warn;

use super::avro_types::{
    ColumnDesc, EnumMode, NaiveTimestampMode, TypeConversionOpts,
    UnsignedBigintMode,
};

// =============================================================================
// MySQL type conversion
// =============================================================================

/// Convert a MySQL column to an Arrow `Field`.
pub fn mysql_column_to_arrow(
    col: &ColumnDesc,
    opts: &TypeConversionOpts,
) -> Field {
    let data_type = mysql_type_to_arrow(&col.data_type, col, opts);
    Field::new(&col.name, data_type, col.nullable)
}

fn mysql_type_to_arrow(
    data_type: &str,
    col: &ColumnDesc,
    opts: &TypeConversionOpts,
) -> DataType {
    match data_type {
        // Integer types
        "tinyint" | "smallint" | "mediumint" => DataType::Int32,
        "int" | "integer" => {
            if col.unsigned {
                // INT UNSIGNED max 2^32-1 — fits in Int64
                DataType::Int64
            } else {
                DataType::Int32
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
                        DataType::Utf8
                    }
                    UnsignedBigintMode::Long => {
                        warn!(
                            column = %col.name,
                            "BIGINT UNSIGNED mapped to int64 — values >= 2^63 will fail encoding"
                        );
                        DataType::Int64
                    }
                }
            } else {
                DataType::Int64
            }
        }

        // Floating point
        "float" => DataType::Float32,
        "double" | "real" => DataType::Float64,

        // Fixed-point decimal — FIXED in Arrow (TD-003 resolved for Parquet sink).
        // Arrow Decimal128 supports precision 1..=38, scale 0..=precision.
        "decimal" | "numeric" => decimal_data_type(col),

        // String types
        "varchar" | "char" | "text" | "tinytext" | "mediumtext"
        | "longtext" => DataType::Utf8,

        // Binary types
        "binary" | "varbinary" | "blob" | "tinyblob" | "mediumblob"
        | "longblob" => DataType::Binary,

        // Date/time types
        "date" => DataType::Date32,
        "datetime" => match opts.naive_timestamp_mode {
            NaiveTimestampMode::String => DataType::Utf8,
            NaiveTimestampMode::Timestamp => {
                DataType::Timestamp(TimeUnit::Millisecond, None)
            }
        },
        "timestamp" => {
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
        }
        "time" => DataType::Time32(TimeUnit::Millisecond),
        "year" => DataType::Int32,

        // Boolean (note: MySQL bool is tinyint(1) which arrives as data_type="tinyint")
        "boolean" | "bool" => DataType::Boolean,
        "bit" if col.precision == Some(1) => DataType::Boolean,

        // BIT(n > 1)
        "bit" => DataType::Binary,

        // JSON
        "json" => DataType::Utf8,

        // ENUM
        "enum" => match opts.enum_mode {
            // Arrow has Dictionary types for enums, but the round-trip story
            // through Parquet + downstream tools is patchier than plain Utf8.
            // Default to Utf8 (mirroring Avro `string` mode); the strict-Enum
            // mode hasn't shipped for Arrow yet.
            EnumMode::String | EnumMode::Enum => DataType::Utf8,
        },

        // SET
        "set" => DataType::Utf8,

        // Catch-all
        other => {
            warn!(
                column = %col.name,
                data_type = other,
                "unknown MySQL type — mapping to string (lossy)"
            );
            DataType::Utf8
        }
    }
}

// =============================================================================
// PostgreSQL type conversion
// =============================================================================

/// Convert a PostgreSQL column to an Arrow `Field`.
pub fn postgres_column_to_arrow(
    col: &ColumnDesc,
    opts: &TypeConversionOpts,
) -> Field {
    if col.is_array {
        let element_type = col.element_type.as_deref().unwrap_or("text");
        let item_dt = postgres_scalar_to_arrow(element_type, col, opts);
        // Arrow List takes a Field describing the element. Element nullability
        // mirrors PostgreSQL semantics (elements may be NULL inside an array).
        let item_field = Field::new("item", item_dt, true);
        return Field::new(
            &col.name,
            DataType::List(item_field.into()),
            col.nullable,
        );
    }

    let data_type = postgres_scalar_to_arrow(&col.data_type, col, opts);
    Field::new(&col.name, data_type, col.nullable)
}

fn postgres_scalar_to_arrow(
    data_type: &str,
    col: &ColumnDesc,
    opts: &TypeConversionOpts,
) -> DataType {
    let normalized = normalize_pg_type(data_type);

    match normalized.as_str() {
        // Integer types
        "smallint" | "int2" => DataType::Int32,
        "integer" | "int" | "int4" => DataType::Int32,
        "bigint" | "int8" => DataType::Int64,
        "serial" => DataType::Int32,
        "bigserial" => DataType::Int64,
        "smallserial" => DataType::Int32,

        // Floating point
        "real" | "float4" => DataType::Float32,
        "double precision" | "float8" => DataType::Float64,

        // Fixed-point decimal — FIXED for Parquet (TD-003).
        "numeric" | "decimal" => decimal_data_type(col),

        // Boolean
        "boolean" | "bool" => DataType::Boolean,

        // String types
        "text" | "varchar" | "character varying" | "char" | "character"
        | "name" | "citext" => DataType::Utf8,

        // Binary
        "bytea" => DataType::Binary,

        // Date/time types
        "date" => DataType::Date32,
        "timestamp" | "timestamp without time zone" => {
            match opts.naive_timestamp_mode {
                NaiveTimestampMode::String => DataType::Utf8,
                NaiveTimestampMode::Timestamp => {
                    DataType::Timestamp(TimeUnit::Microsecond, None)
                }
            }
        }
        "timestamptz" | "timestamp with time zone" => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        }
        "time" | "time without time zone" => {
            DataType::Time64(TimeUnit::Microsecond)
        }
        "timetz" | "time with time zone" => DataType::Utf8,
        "interval" => DataType::Utf8,

        // UUID
        "uuid" => DataType::Utf8,

        // JSON
        "json" | "jsonb" => DataType::Utf8,

        // Network / geometric / range / money / xml — all serialize as string
        "inet" | "cidr" | "macaddr" | "macaddr8" => DataType::Utf8,
        "point" | "line" | "lseg" | "box" | "path" | "polygon" | "circle" => {
            DataType::Utf8
        }
        "int4range" | "int8range" | "numrange" | "tsrange" | "tstzrange"
        | "daterange" => DataType::Utf8,
        "money" => DataType::Utf8,
        "xml" => DataType::Utf8,

        // hstore — Arrow Map: <Utf8, Utf8>
        "hstore" => DataType::Map(
            Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Utf8, true),
                    ]
                    .into(),
                ),
                false,
            )
            .into(),
            false,
        ),

        // Catch-all
        other => {
            warn!(
                column = %col.name,
                data_type = other,
                "unknown PostgreSQL type — mapping to string (lossy)"
            );
            DataType::Utf8
        }
    }
}

/// Normalize PostgreSQL type names to canonical short forms.
/// Identical to the Avro path so we get consistent type recognition.
fn normalize_pg_type(data_type: &str) -> String {
    let lower = data_type.to_lowercase().trim().to_string();

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

    if let Some(paren) = lower.find('(') {
        lower[..paren].trim_end().to_string()
    } else {
        lower
    }
}

// =============================================================================
// Decimal helper
// =============================================================================

/// Arrow Decimal128 supports precision 1..=38. Choose a sensible mapping for
/// columns that arrive without explicit precision/scale; fall back to string
/// when the source declares a precision that exceeds Decimal128 (rare).
fn decimal_data_type(col: &ColumnDesc) -> DataType {
    let p = col.precision.unwrap_or(38);
    let s = col.scale.unwrap_or(0);

    // Decimal128: 1 <= precision <= 38, 0 <= scale <= precision.
    if !(1..=38).contains(&p) {
        warn!(
            column = %col.name,
            precision = p,
            "DECIMAL precision out of Decimal128 range — falling back to string"
        );
        return DataType::Utf8;
    }
    let s = s.clamp(0, p);
    DataType::Decimal128(p as u8, s as i8)
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

    fn decimal_col(name: &str, precision: i64, scale: i64) -> ColumnDesc {
        ColumnDesc {
            precision: Some(precision),
            scale: Some(scale),
            ..col(
                name,
                "decimal",
                &format!("decimal({precision},{scale})"),
                true,
            )
        }
    }

    fn opts() -> TypeConversionOpts {
        TypeConversionOpts::default()
    }

    // -----------------------------------------------------------------------
    // MySQL tests
    // -----------------------------------------------------------------------

    #[test]
    fn mysql_int_types() {
        let o = opts();
        assert_eq!(
            mysql_column_to_arrow(&col("a", "tinyint", "tinyint", false), &o)
                .data_type(),
            &DataType::Int32
        );
        assert_eq!(
            mysql_column_to_arrow(&col("a", "smallint", "smallint", false), &o)
                .data_type(),
            &DataType::Int32
        );
        assert_eq!(
            mysql_column_to_arrow(&col("a", "int", "int", false), &o)
                .data_type(),
            &DataType::Int32
        );
        assert_eq!(
            mysql_column_to_arrow(&col("a", "bigint", "bigint", false), &o)
                .data_type(),
            &DataType::Int64
        );
    }

    #[test]
    fn mysql_unsigned_int_promotes_to_int64() {
        let o = opts();
        let f = mysql_column_to_arrow(
            &unsigned_col("a", "int", "int unsigned"),
            &o,
        );
        assert_eq!(f.data_type(), &DataType::Int64);
    }

    #[test]
    fn mysql_unsigned_bigint_safe_default_is_string() {
        let o = opts();
        let f = mysql_column_to_arrow(
            &unsigned_col("a", "bigint", "bigint unsigned"),
            &o,
        );
        assert_eq!(f.data_type(), &DataType::Utf8);
    }

    #[test]
    fn mysql_unsigned_bigint_opt_in_long() {
        let mut o = opts();
        o.unsigned_bigint_mode = UnsignedBigintMode::Long;
        let f = mysql_column_to_arrow(
            &unsigned_col("a", "bigint", "bigint unsigned"),
            &o,
        );
        assert_eq!(f.data_type(), &DataType::Int64);
    }

    #[test]
    fn mysql_decimal_fixes_td003() {
        let o = opts();
        let f = mysql_column_to_arrow(&decimal_col("price", 18, 2), &o);
        // The whole point of TD-003 for the Parquet sink — Decimal128 native,
        // not string.
        assert_eq!(f.data_type(), &DataType::Decimal128(18, 2));
        assert!(f.is_nullable());
    }

    #[test]
    fn mysql_decimal_precision_out_of_range_falls_back_to_string() {
        let o = opts();
        let f = mysql_column_to_arrow(&decimal_col("hyper", 100, 5), &o);
        assert_eq!(f.data_type(), &DataType::Utf8);
    }

    #[test]
    fn mysql_decimal_clamps_scale_to_precision() {
        let o = opts();
        let f = mysql_column_to_arrow(&decimal_col("x", 10, 20), &o);
        // scale clamped to precision so the Field is valid.
        assert_eq!(f.data_type(), &DataType::Decimal128(10, 10));
    }

    #[test]
    fn mysql_string_types() {
        let o = opts();
        for t in [
            "varchar",
            "char",
            "text",
            "tinytext",
            "mediumtext",
            "longtext",
        ] {
            let f = mysql_column_to_arrow(&col("c", t, t, false), &o);
            assert_eq!(f.data_type(), &DataType::Utf8, "type {t}");
        }
    }

    #[test]
    fn mysql_binary_types() {
        let o = opts();
        for t in [
            "binary",
            "varbinary",
            "blob",
            "tinyblob",
            "mediumblob",
            "longblob",
        ] {
            let f = mysql_column_to_arrow(&col("c", t, t, false), &o);
            assert_eq!(f.data_type(), &DataType::Binary, "type {t}");
        }
    }

    #[test]
    fn mysql_date_and_timestamp() {
        let o = opts();
        assert_eq!(
            mysql_column_to_arrow(&col("d", "date", "date", false), &o)
                .data_type(),
            &DataType::Date32
        );
        // TIMESTAMP always UTC-tagged.
        assert_eq!(
            mysql_column_to_arrow(
                &col("t", "timestamp", "timestamp", false),
                &o
            )
            .data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
        );
        // DATETIME defaults to string (naive timestamp).
        assert_eq!(
            mysql_column_to_arrow(&col("d", "datetime", "datetime", false), &o)
                .data_type(),
            &DataType::Utf8
        );
    }

    #[test]
    fn mysql_datetime_with_naive_timestamp_mode() {
        let mut o = opts();
        o.naive_timestamp_mode = NaiveTimestampMode::Timestamp;
        let f =
            mysql_column_to_arrow(&col("d", "datetime", "datetime", false), &o);
        assert_eq!(
            f.data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, None)
        );
    }

    #[test]
    fn mysql_nullable_marks_field() {
        let o = opts();
        let f = mysql_column_to_arrow(
            &col("a", "varchar", "varchar(255)", true),
            &o,
        );
        assert!(f.is_nullable());
    }

    #[test]
    fn mysql_unknown_type_falls_back_to_string() {
        let o = opts();
        let f =
            mysql_column_to_arrow(&col("g", "geometry", "geometry", false), &o);
        assert_eq!(f.data_type(), &DataType::Utf8);
    }

    #[test]
    fn mysql_bit_one_is_boolean() {
        let mut c = col("flag", "bit", "bit(1)", false);
        c.precision = Some(1);
        let f = mysql_column_to_arrow(&c, &opts());
        assert_eq!(f.data_type(), &DataType::Boolean);
    }

    #[test]
    fn mysql_bit_n_is_binary() {
        let mut c = col("mask", "bit", "bit(8)", false);
        c.precision = Some(8);
        let f = mysql_column_to_arrow(&c, &opts());
        assert_eq!(f.data_type(), &DataType::Binary);
    }

    #[test]
    fn mysql_json_is_utf8() {
        let f = mysql_column_to_arrow(&col("j", "json", "json", true), &opts());
        assert_eq!(f.data_type(), &DataType::Utf8);
    }

    // -----------------------------------------------------------------------
    // PostgreSQL tests
    // -----------------------------------------------------------------------

    #[test]
    fn pg_int_types() {
        let o = opts();
        assert_eq!(
            postgres_column_to_arrow(
                &col("a", "smallint", "smallint", false),
                &o
            )
            .data_type(),
            &DataType::Int32
        );
        assert_eq!(
            postgres_column_to_arrow(
                &col("a", "integer", "integer", false),
                &o
            )
            .data_type(),
            &DataType::Int32
        );
        assert_eq!(
            postgres_column_to_arrow(&col("a", "bigint", "bigint", false), &o)
                .data_type(),
            &DataType::Int64
        );
        assert_eq!(
            postgres_column_to_arrow(&col("a", "serial", "serial", false), &o)
                .data_type(),
            &DataType::Int32
        );
        assert_eq!(
            postgres_column_to_arrow(
                &col("a", "bigserial", "bigserial", false),
                &o
            )
            .data_type(),
            &DataType::Int64
        );
    }

    #[test]
    fn pg_decimal_fixes_td003() {
        let o = opts();
        let mut c = col("price", "numeric", "numeric(12,4)", true);
        c.precision = Some(12);
        c.scale = Some(4);
        let f = postgres_column_to_arrow(&c, &o);
        assert_eq!(f.data_type(), &DataType::Decimal128(12, 4));
    }

    #[test]
    fn pg_decimal_no_precision_falls_back_to_full_range() {
        let f = postgres_column_to_arrow(
            &col("amount", "numeric", "numeric", true),
            &opts(),
        );
        // 38, 0 — Decimal128 max
        assert_eq!(f.data_type(), &DataType::Decimal128(38, 0));
    }

    #[test]
    fn pg_string_types() {
        let o = opts();
        for t in [
            "text",
            "varchar",
            "character varying",
            "char",
            "name",
            "citext",
        ] {
            let f = postgres_column_to_arrow(&col("c", t, t, false), &o);
            assert_eq!(f.data_type(), &DataType::Utf8, "type {t}");
        }
    }

    #[test]
    fn pg_bytea() {
        let f = postgres_column_to_arrow(
            &col("b", "bytea", "bytea", false),
            &opts(),
        );
        assert_eq!(f.data_type(), &DataType::Binary);
    }

    #[test]
    fn pg_timestamp_variants() {
        let o = opts();
        assert_eq!(
            postgres_column_to_arrow(&col("d", "date", "date", false), &o)
                .data_type(),
            &DataType::Date32
        );
        assert_eq!(
            postgres_column_to_arrow(
                &col("t", "timestamptz", "timestamptz", false),
                &o
            )
            .data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        // Naive timestamp defaults to string
        assert_eq!(
            postgres_column_to_arrow(
                &col("t", "timestamp", "timestamp", false),
                &o
            )
            .data_type(),
            &DataType::Utf8
        );
    }

    #[test]
    fn pg_uuid_is_utf8() {
        let f = postgres_column_to_arrow(
            &col("id", "uuid", "uuid", false),
            &opts(),
        );
        assert_eq!(f.data_type(), &DataType::Utf8);
    }

    #[test]
    fn pg_json_jsonb_are_utf8() {
        let o = opts();
        for t in ["json", "jsonb"] {
            let f = postgres_column_to_arrow(&col("c", t, t, false), &o);
            assert_eq!(f.data_type(), &DataType::Utf8, "type {t}");
        }
    }

    #[test]
    fn pg_array_wraps_in_list() {
        let o = opts();
        let mut c = col("tags", "_text", "_text", true);
        c.is_array = true;
        c.element_type = Some("text".to_string());
        let f = postgres_column_to_arrow(&c, &o);
        match f.data_type() {
            DataType::List(item) => {
                assert_eq!(item.data_type(), &DataType::Utf8);
                assert!(item.is_nullable());
            }
            other => panic!("expected List, got {other:?}"),
        }
    }

    #[test]
    fn pg_array_of_integers() {
        let o = opts();
        let mut c = col("ids", "_int4", "_int4", false);
        c.is_array = true;
        c.element_type = Some("integer".to_string());
        let f = postgres_column_to_arrow(&c, &o);
        match f.data_type() {
            DataType::List(item) => {
                assert_eq!(item.data_type(), &DataType::Int32);
            }
            other => panic!("expected List, got {other:?}"),
        }
    }

    #[test]
    fn pg_normalizes_character_varying() {
        let f = postgres_column_to_arrow(
            &col("c", "character varying(50)", "character varying(50)", false),
            &opts(),
        );
        assert_eq!(f.data_type(), &DataType::Utf8);
    }

    #[test]
    fn pg_normalizes_numeric_with_args() {
        let mut c = col("c", "numeric(10,2)", "numeric(10,2)", true);
        c.precision = Some(10);
        c.scale = Some(2);
        let f = postgres_column_to_arrow(&c, &opts());
        assert_eq!(f.data_type(), &DataType::Decimal128(10, 2));
    }

    #[test]
    fn pg_unknown_type_falls_back_to_string() {
        let f = postgres_column_to_arrow(
            &col("x", "some_udt", "some_udt", false),
            &opts(),
        );
        assert_eq!(f.data_type(), &DataType::Utf8);
    }
}
