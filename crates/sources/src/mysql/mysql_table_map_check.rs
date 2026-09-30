//! Fail-closed compatibility check between a binlog `TableMap` and the schema
//! used to decode its rows.
//!
//! MySQL rows are decoded positionally: the i-th value of a row image is
//! labelled with the i-th column of the loaded schema. When the binlog being
//! read was written under a different table definition than the loaded one -
//! typically a restart that replays rows written before a DDL - positional
//! decoding would silently put values under the wrong columns. This check
//! compares the layout the binlog recorded with the loaded schema and reports
//! any difference so the caller can refuse to decode.
//!
//! Compared per column:
//! - the column count (and one type meta per column);
//! - the storage type family (the binlog type, with `CHAR`/`ENUM`/`SET`
//!   resolved from the column meta, against `INFORMATION_SCHEMA.DATA_TYPE`);
//! - the column name, when the server writes it (`binlog_row_metadata=FULL`).
//!
//! Compatibility must be proven, never assumed: a binlog type or schema type
//! this check cannot classify, or missing per-column type metadata, is a
//! mismatch.
//!
//! Residual limitation: with `binlog_row_metadata=MINIMAL` (the MySQL default)
//! names are not in the binlog, so a layout change that keeps the column count
//! and every type family (e.g. two same-typed columns swapped) is not
//! detectable here.

use mysql_binlog_connector_rust::column::column_type::ColumnType;
use mysql_binlog_connector_rust::event::table_map_event::TableMapEvent;

use super::mysql_table_schema::MySqlColumn;

/// Storage type families that share one binlog representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TypeFamily {
    Tiny,
    Short,
    Int24,
    Long,
    LongLong,
    Float,
    Double,
    Decimal,
    Date,
    Time,
    DateTime,
    Timestamp,
    Year,
    VarString,
    FixedString,
    Enum,
    Set,
    Bit,
    Json,
    Blob,
    Geometry,
}

/// Family of a binlog column type code. `None` for codes this check cannot
/// classify (treated as a mismatch by the caller).
fn binlog_family(code: u8, meta: u16) -> Option<TypeFamily> {
    use TypeFamily::*;
    let code = if code == ColumnType::String as u8 {
        // CHAR, BINARY, ENUM and SET all travel as MYSQL_TYPE_STRING; the real
        // type is carried in the column meta.
        ColumnType::parse_string_column_meta(meta, code)
            .map(|(real, _)| real)
            .ok()?
    } else {
        code
    };
    Some(match ColumnType::from_code(code) {
        ColumnType::Tiny => Tiny,
        ColumnType::Short => Short,
        ColumnType::Int24 => Int24,
        ColumnType::Long => Long,
        ColumnType::LongLong => LongLong,
        ColumnType::Float => Float,
        ColumnType::Double => Double,
        ColumnType::Decimal | ColumnType::NewDecimal => Decimal,
        ColumnType::Date | ColumnType::NewDate => Date,
        ColumnType::Time | ColumnType::Time2 => Time,
        ColumnType::DateTime | ColumnType::DateTime2 => DateTime,
        ColumnType::TimeStamp | ColumnType::TimeStamp2 => Timestamp,
        ColumnType::Year => Year,
        ColumnType::VarChar | ColumnType::VarString => VarString,
        ColumnType::String => FixedString,
        ColumnType::Enum => Enum,
        ColumnType::Set => Set,
        ColumnType::Bit => Bit,
        ColumnType::Json => Json,
        ColumnType::TinyBlob
        | ColumnType::MediumBlob
        | ColumnType::LongBlob
        | ColumnType::Blob => Blob,
        ColumnType::Geometry => Geometry,
        ColumnType::Null | ColumnType::Unknown => return None,
    })
}

/// Family of an `INFORMATION_SCHEMA.COLUMNS.DATA_TYPE` value. `None` for types
/// this check cannot classify (treated as a mismatch by the caller). Covers
/// every data type of the supported MySQL version (8.4).
fn schema_family(data_type: &str) -> Option<TypeFamily> {
    use TypeFamily::*;
    Some(match data_type.to_ascii_lowercase().as_str() {
        "tinyint" | "bool" | "boolean" => Tiny,
        "smallint" => Short,
        "mediumint" => Int24,
        "int" | "integer" => Long,
        "bigint" => LongLong,
        "float" => Float,
        "double" | "real" => Double,
        "decimal" | "numeric" => Decimal,
        "date" => Date,
        "time" => Time,
        "datetime" => DateTime,
        "timestamp" => Timestamp,
        "year" => Year,
        "varchar" | "varbinary" => VarString,
        "char" | "binary" => FixedString,
        "enum" => Enum,
        "set" => Set,
        "bit" => Bit,
        "json" => Json,
        "tinyblob" | "blob" | "mediumblob" | "longblob" | "tinytext"
        | "text" | "mediumtext" | "longtext" => Blob,
        "geometry" | "point" | "linestring" | "polygon" | "multipoint"
        | "multilinestring" | "multipolygon" | "geometrycollection"
        | "geomcollection" => Geometry,
        _ => return None,
    })
}

/// Describe why `tm` cannot be decoded positionally with `columns`, or `None`
/// when the layouts are compatible.
pub(crate) fn table_map_mismatch(
    tm: &TableMapEvent,
    columns: &[MySqlColumn],
) -> Option<String> {
    if tm.column_types.len() != columns.len() {
        return Some(format!(
            "the binlog row image has {} columns but the schema used for \
             decoding has {}",
            tm.column_types.len(),
            columns.len()
        ));
    }
    if tm.column_metas.len() != tm.column_types.len() {
        return Some(format!(
            "the binlog table map carries type metadata for {} of its {} \
             columns; the row layout cannot be verified",
            tm.column_metas.len(),
            tm.column_types.len()
        ));
    }
    let names = tm.table_metadata.as_ref().map(|m| &m.columns);
    for (i, col) in columns.iter().enumerate() {
        let code = tm.column_types[i];
        let Some(binlog) = binlog_family(code, tm.column_metas[i]) else {
            return Some(format!(
                "column {} (`{}`): unrecognized binlog column type {code}; \
                 compatibility with the schema cannot be proven",
                i + 1,
                col.name
            ));
        };
        let Some(schema) = schema_family(&col.data_type) else {
            return Some(format!(
                "column {} (`{}`): unrecognized schema data type `{}`; \
                 compatibility with the binlog cannot be proven",
                i + 1,
                col.name,
                col.data_type
            ));
        };
        if binlog != schema {
            return Some(format!(
                "column {} (`{}`): the binlog recorded a {binlog:?} value but \
                 the schema declares {}",
                i + 1,
                col.name,
                col.data_type
            ));
        }
        if let Some(recorded) = names
            .and_then(|cols| cols.get(i))
            .and_then(|m| m.column_name.as_deref())
            && !recorded.eq_ignore_ascii_case(&col.name)
        {
            return Some(format!(
                "column {}: the binlog recorded `{recorded}` but the schema \
                 has `{}`",
                i + 1,
                col.name
            ));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use mysql_binlog_connector_rust::event::table_map::table_metadata::{
        ColumnMetadata, TableMetadata,
    };

    fn col(name: &str, data_type: &str) -> MySqlColumn {
        MySqlColumn::new(name, data_type, data_type, true, 1)
    }

    fn tm(
        types: &[(ColumnType, u16)],
        names: Option<&[&str]>,
    ) -> TableMapEvent {
        TableMapEvent {
            table_id: 1,
            database_name: "db".into(),
            table_name: "t".into(),
            column_types: types.iter().map(|(t, _)| t.clone() as u8).collect(),
            column_metas: types.iter().map(|(_, m)| *m).collect(),
            null_bits: vec![true; types.len()],
            table_metadata: names.map(|ns| TableMetadata {
                default_charset: None,
                enum_and_set_default_charset: None,
                columns: ns
                    .iter()
                    .map(|n| ColumnMetadata {
                        column_name: Some(n.to_string()),
                        ..Default::default()
                    })
                    .collect(),
            }),
        }
    }

    /// Column meta of a CHAR/ENUM/SET column: real type in the high byte.
    fn string_meta(real: ColumnType, len: u16) -> u16 {
        ((real as u16) << 8) | len
    }

    #[test]
    fn matching_layout_is_compatible() {
        let t = tm(
            &[
                (ColumnType::Long, 0),
                (ColumnType::VarChar, 64),
                (ColumnType::NewDecimal, 0),
                (ColumnType::Json, 4),
                (ColumnType::Blob, 2),
            ],
            None,
        );
        let cols = [
            col("id", "int"),
            col("sku", "varchar"),
            col("amount", "decimal"),
            col("payload", "json"),
            col("body", "text"),
        ];
        assert_eq!(table_map_mismatch(&t, &cols), None);
    }

    #[test]
    fn dropped_column_is_a_count_mismatch() {
        // Row written as (id, a, b); schema now (id, b).
        let t = tm(
            &[
                (ColumnType::Long, 0),
                (ColumnType::VarChar, 16),
                (ColumnType::VarChar, 16),
            ],
            None,
        );
        let why =
            table_map_mismatch(&t, &[col("id", "int"), col("b", "varchar")])
                .unwrap();
        assert!(why.contains("3 columns") && why.contains("has 2"), "{why}");
    }

    #[test]
    fn same_count_different_type_is_a_mismatch() {
        // Row written as (id INT, v VARCHAR); schema now (id INT, w INT).
        let t = tm(&[(ColumnType::Long, 0), (ColumnType::VarChar, 16)], None);
        let why = table_map_mismatch(&t, &[col("id", "int"), col("w", "int")])
            .unwrap();
        assert!(why.contains("column 2"), "{why}");
    }

    #[test]
    fn char_enum_and_set_are_resolved_from_the_meta() {
        let t = tm(
            &[
                (ColumnType::String, string_meta(ColumnType::String, 10)),
                (ColumnType::String, string_meta(ColumnType::Enum, 1)),
                (ColumnType::String, string_meta(ColumnType::Set, 1)),
            ],
            None,
        );
        let ok = [col("c", "char"), col("e", "enum"), col("s", "set")];
        assert_eq!(table_map_mismatch(&t, &ok), None);
        let swapped = [col("c", "char"), col("e", "set"), col("s", "enum")];
        assert!(table_map_mismatch(&t, &swapped).is_some());
    }

    #[test]
    fn recorded_names_are_verified_when_present() {
        // Same count and families, but the binlog names the columns differently
        // (e.g. `a` dropped and `c` added with the same type).
        let t = tm(
            &[
                (ColumnType::Long, 0),
                (ColumnType::VarChar, 16),
                (ColumnType::VarChar, 16),
            ],
            Some(&["id", "a", "b"]),
        );
        let why = table_map_mismatch(
            &t,
            &[col("id", "int"), col("b", "varchar"), col("c", "varchar")],
        )
        .unwrap();
        assert!(why.contains("`a`") && why.contains("`b`"), "{why}");
        // Name comparison is case-insensitive, like MySQL column names.
        let ok = table_map_mismatch(
            &t,
            &[col("ID", "int"), col("A", "varchar"), col("B", "varchar")],
        );
        assert_eq!(ok, None);
    }

    #[test]
    fn unknown_binlog_type_is_a_mismatch() {
        let mut t = tm(&[(ColumnType::Long, 0), (ColumnType::Long, 0)], None);
        // 242 is not a type code this crate knows (e.g. a newer server type).
        t.column_types[1] = 242;
        let why = table_map_mismatch(&t, &[col("id", "int"), col("v", "int")])
            .expect("an unclassified binlog type cannot prove compatibility");
        assert!(why.contains("unrecognized binlog column type 242"), "{why}");
    }

    #[test]
    fn unknown_schema_type_is_a_mismatch() {
        let t = tm(&[(ColumnType::Long, 0), (ColumnType::Blob, 2)], None);
        let why =
            table_map_mismatch(&t, &[col("id", "int"), col("v", "vector")])
                .expect(
                    "an unclassified schema type cannot prove compatibility",
                );
        assert!(
            why.contains("unrecognized schema data type `vector`"),
            "{why}"
        );
    }

    #[test]
    fn missing_column_metadata_is_a_mismatch() {
        let mut t = tm(
            &[
                (ColumnType::Long, 0),
                (ColumnType::String, string_meta(ColumnType::Enum, 1)),
            ],
            None,
        );
        // The meta that resolves CHAR/ENUM/SET is absent.
        t.column_metas.pop();
        let why = table_map_mismatch(&t, &[col("id", "int"), col("e", "enum")])
            .expect("missing type metadata cannot prove compatibility");
        assert!(
            why.contains("type metadata for 1 of its 2 columns"),
            "{why}"
        );
    }

    #[test]
    fn every_integer_width_is_distinct() {
        let widths = [
            (ColumnType::Tiny, "tinyint"),
            (ColumnType::Short, "smallint"),
            (ColumnType::Int24, "mediumint"),
            (ColumnType::Long, "int"),
            (ColumnType::LongLong, "bigint"),
        ];
        for (i, (binlog, _)) in widths.iter().enumerate() {
            for (j, (_, declared)) in widths.iter().enumerate() {
                let t = tm(&[(binlog.clone(), 0)], None);
                let mismatch =
                    table_map_mismatch(&t, &[col("v", declared)]).is_some();
                assert_eq!(mismatch, i != j, "{binlog:?} vs {declared}");
            }
        }
    }
}
