//! DDL column description → ClickHouse column type.
//!
//! Used both for RowBinary encoding (exact byte layout) and for generating
//! `CREATE TABLE` DDL (auto table creation).

/// A ClickHouse column type in the v1 supported set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChType {
    UInt8,
    Int16,
    Int32,
    Int64,
    UInt64,
    Float64,
    String,
    Decimal { p: u32, s: u32 },
    DateTime64_3,
    Bool,
}

/// A source column description the sink learns from the schema resolver.
#[derive(Debug, Clone)]
pub struct ColDesc {
    pub name: String,
    pub data_type: String,
    pub full_type: String,
    pub nullable: bool,
    pub unsigned: bool,
    pub precision: Option<i64>,
    pub scale: Option<i64>,
}

/// Map a source column to its ClickHouse type.
pub fn map_column(c: &ColDesc) -> ChType {
    let dt = c.data_type.to_lowercase();
    match dt.as_str() {
        "boolean" | "bool" => ChType::Bool,
        "tinyint" => ChType::UInt8,
        "smallint" => ChType::Int16,
        "int" | "integer" | "mediumint" => ChType::Int32,
        "bigint" => {
            if c.unsigned {
                ChType::UInt64
            } else {
                ChType::Int64
            }
        }
        "float" | "double" | "real" => ChType::Float64,
        "decimal" | "numeric" => ChType::Decimal {
            p: c.precision.unwrap_or(38) as u32,
            s: c.scale.unwrap_or(0) as u32,
        },
        "date" | "datetime" | "timestamp" | "timestamptz" => {
            ChType::DateTime64_3
        }
        // varchar/text/json/uuid/enum/arrays → String (JSON kept as text in v1)
        _ => ChType::String,
    }
}

impl ChType {
    /// ClickHouse type name for `CREATE TABLE` DDL and validation messages.
    pub fn ddl_name(&self) -> String {
        match self {
            ChType::UInt8 => "UInt8".into(),
            ChType::Int16 => "Int16".into(),
            ChType::Int32 => "Int32".into(),
            ChType::Int64 => "Int64".into(),
            ChType::UInt64 => "UInt64".into(),
            ChType::Float64 => "Float64".into(),
            ChType::String => "String".into(),
            ChType::Decimal { p, s } => format!("Decimal({p}, {s})"),
            ChType::DateTime64_3 => "DateTime64(3)".into(),
            ChType::Bool => "UInt8".into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(dt: &str, full: &str) -> ColDesc {
        ColDesc {
            name: "x".into(),
            data_type: dt.into(),
            full_type: full.into(),
            nullable: false,
            unsigned: false,
            precision: None,
            scale: None,
        }
    }

    #[test]
    fn maps_core_types() {
        assert_eq!(map_column(&col("bigint", "bigint")), ChType::Int64);
        assert_eq!(map_column(&col("int", "int")), ChType::Int32);
        assert_eq!(map_column(&col("varchar", "varchar(255)")), ChType::String);
        assert_eq!(map_column(&col("boolean", "boolean")), ChType::Bool);
    }

    #[test]
    fn maps_unsigned_bigint_to_uint64() {
        let c = ColDesc {
            unsigned: true,
            ..col("bigint", "bigint unsigned")
        };
        assert_eq!(map_column(&c), ChType::UInt64);
    }

    #[test]
    fn maps_decimal_with_precision_scale() {
        let c = ColDesc {
            precision: Some(12),
            scale: Some(2),
            ..col("decimal", "decimal(12,2)")
        };
        assert_eq!(map_column(&c), ChType::Decimal { p: 12, s: 2 });
    }

    #[test]
    fn ddl_names_render() {
        assert_eq!(ChType::Int64.ddl_name(), "Int64");
        assert_eq!(
            ChType::Decimal { p: 12, s: 2 }.ddl_name(),
            "Decimal(12, 2)"
        );
        assert_eq!(ChType::DateTime64_3.ddl_name(), "DateTime64(3)");
    }
}
