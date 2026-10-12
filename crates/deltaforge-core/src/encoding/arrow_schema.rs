//! CDC envelope → Arrow `Schema` builder.
//!
//! Parallel to `avro_schema.rs`, but produces an Arrow schema suitable for
//! Parquet output. The key shape difference from the Avro envelope is that
//! Arrow columns are **flat** rather than nested:
//!
//! ```text
//! Avro envelope (nested records):
//!   { op, ts_ms, source: { db, table, position: {...} }, before: {...}, after: {...} }
//!
//! Arrow envelope (flat columns):
//!   op, op_ts, source_db, source_table, source_position,
//!   before_<col1>, before_<col2>, ..., after_<col1>, after_<col2>, ...
//! ```
//!
//! Flat columns trade nesting for usability — every analytics engine (Athena,
//! Trino, DuckDB, Spark, BigQuery external tables) handles flat columns out of
//! the box. Nested struct columns work in most but not all engines, and
//! complicate predicate pushdown. Phase 2 may add an opt-in nested variant.
//!
//! The user-data columns are derived from source DDL via `mysql_column_to_arrow`
//! or `postgres_column_to_arrow` in `arrow_types`.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, TimeUnit};

use super::arrow_types::{mysql_column_to_arrow, postgres_column_to_arrow};
use super::avro_types::{ColumnDesc, TypeConversionOpts};

/// Envelope columns shared by every CDC event regardless of connector.
fn envelope_meta_fields() -> Vec<Field> {
    vec![
        Field::new("op", DataType::Utf8, false),
        Field::new(
            "op_ts",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        Field::new("source_db", DataType::Utf8, true),
        Field::new("source_schema", DataType::Utf8, true),
        Field::new("source_table", DataType::Utf8, false),
        // Position is connector-specific and serialized as a stable string
        // (e.g. "binlog:file=mysql-bin.000123,pos=4567" or "lsn:0/16B41E0").
        // This keeps the column type stable across MySQL / PG sources in the
        // same lake table.
        Field::new("source_position", DataType::Utf8, true),
        Field::new("source_snapshot", DataType::Boolean, true),
        Field::new("event_id", DataType::Utf8, true),
        Field::new("schema_version", DataType::Utf8, true),
        Field::new("tx_id", DataType::Utf8, true),
    ]
}

/// Connector dispatch for column → Arrow field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Connector {
    Mysql,
    Postgres,
}

fn column_to_arrow_field(
    connector: Connector,
    col: &ColumnDesc,
    opts: &TypeConversionOpts,
) -> Field {
    match connector {
        Connector::Mysql => mysql_column_to_arrow(col, opts),
        Connector::Postgres => postgres_column_to_arrow(col, opts),
    }
}

/// Build a full Arrow envelope schema for a given table.
///
/// For each source column, two flat columns are emitted: `before_<name>` and
/// `after_<name>`. Both are always nullable in the envelope: DELETE events
/// have a null `after.*`, INSERT events have a null `before.*`, and even a
/// non-nullable source column can be `before_*` null on insert.
///
/// Two more columns carry each row image's processor-added document
/// extensions, `before_extensions` and `after_extensions` (canonical JSON
/// objects, null when a row has none): a stable part of every schema, so
/// added fields never change it. A table with a column named `extensions`
/// would collide with them and is refused.
pub fn build_envelope_arrow_schema(
    connector: Connector,
    columns: &[ColumnDesc],
    opts: &TypeConversionOpts,
) -> Result<Schema, String> {
    if columns.iter().any(|c| c.name == EXTENSIONS) {
        return Err(format!(
            "column `{EXTENSIONS}` collides with the extension columns \
             before_{EXTENSIONS}/after_{EXTENSIONS}"
        ));
    }
    let mut fields = envelope_meta_fields();
    for image in ["before", "after"] {
        fields.push(Field::new(
            format!("{image}_{EXTENSIONS}"),
            DataType::Utf8,
            true,
        ));
    }

    for col in columns {
        let f = column_to_arrow_field(connector, col, opts);
        // Force nullable=true on envelope projection regardless of source
        // nullability (see doc comment above).
        fields.push(
            Field::new(
                format!("before_{}", col.name),
                f.data_type().clone(),
                true,
            )
            .with_metadata(field_metadata(connector, col)),
        );
        fields.push(
            Field::new(
                format!("after_{}", col.name),
                f.data_type().clone(),
                true,
            )
            .with_metadata(field_metadata(connector, col)),
        );
    }

    Ok(Schema::new(fields))
}

/// The suffix of the extension columns (`before_extensions`,
/// `after_extensions`).
pub const EXTENSIONS: &str = "extensions";

/// Per-field Arrow metadata used to retain source-side type info for
/// downstream consumers (e.g. recovery, schema evolution, debugging).
fn field_metadata(
    connector: Connector,
    col: &ColumnDesc,
) -> std::collections::HashMap<String, String> {
    let mut m = std::collections::HashMap::new();
    m.insert(
        "deltaforge.connector".to_string(),
        match connector {
            Connector::Mysql => "mysql".to_string(),
            Connector::Postgres => "postgres".to_string(),
        },
    );
    m.insert(
        "deltaforge.source_type".to_string(),
        col.column_type.clone(),
    );
    if col.unsigned {
        m.insert("deltaforge.unsigned".to_string(), "true".to_string());
    }
    if col.is_array {
        m.insert("deltaforge.is_array".to_string(), "true".to_string());
    }
    m
}

/// Convenience: shared `Arc<Schema>` since multiple writers/readers will share
/// the same schema for the lifetime of a partition.
pub fn build_envelope_arrow_schema_arc(
    connector: Connector,
    columns: &[ColumnDesc],
    opts: &TypeConversionOpts,
) -> Result<Arc<Schema>, String> {
    build_envelope_arrow_schema(connector, columns, opts).map(Arc::new)
}

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

    #[test]
    fn envelope_meta_columns_present() {
        let schema = build_envelope_arrow_schema(
            Connector::Mysql,
            &[col("id", "bigint", "bigint", false)],
            &TypeConversionOpts::default(),
        )
        .unwrap();
        let names: Vec<&str> =
            schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(names.contains(&"op"));
        assert!(names.contains(&"op_ts"));
        assert!(names.contains(&"source_db"));
        assert!(names.contains(&"source_table"));
        assert!(names.contains(&"source_position"));
        assert!(names.contains(&"event_id"));
        assert!(names.contains(&"before_id"));
        assert!(names.contains(&"after_id"));
    }

    #[test]
    fn user_columns_emit_before_and_after() {
        let cols = vec![
            col("id", "bigint", "bigint", false),
            col("email", "varchar", "varchar(255)", true),
            col("created_at", "timestamp", "timestamp", false),
        ];
        let schema = build_envelope_arrow_schema(
            Connector::Mysql,
            &cols,
            &TypeConversionOpts::default(),
        )
        .unwrap();

        // 10 envelope meta + 2 * 3 user = 16
        assert_eq!(schema.fields().len(), 12 + 2 * cols.len());

        let before_id = schema.field_with_name("before_id").unwrap();
        let after_id = schema.field_with_name("after_id").unwrap();
        assert_eq!(before_id.data_type(), &DataType::Int64);
        assert_eq!(after_id.data_type(), &DataType::Int64);
        // before/after always nullable, even for non-nullable source columns.
        assert!(before_id.is_nullable());
        assert!(after_id.is_nullable());
    }

    #[test]
    fn decimal_columns_use_native_decimal128() {
        let mut c = col("price", "decimal", "decimal(12,2)", true);
        c.precision = Some(12);
        c.scale = Some(2);
        let schema = build_envelope_arrow_schema(
            Connector::Mysql,
            &[c],
            &TypeConversionOpts::default(),
        )
        .unwrap();
        let before = schema.field_with_name("before_price").unwrap();
        assert_eq!(before.data_type(), &DataType::Decimal128(12, 2));
        let after = schema.field_with_name("after_price").unwrap();
        assert_eq!(after.data_type(), &DataType::Decimal128(12, 2));
    }

    #[test]
    fn postgres_array_columns_become_list() {
        let mut c = col("tags", "_text", "_text", true);
        c.is_array = true;
        c.element_type = Some("text".to_string());
        let schema = build_envelope_arrow_schema(
            Connector::Postgres,
            &[c],
            &TypeConversionOpts::default(),
        )
        .unwrap();
        let f = schema.field_with_name("after_tags").unwrap();
        match f.data_type() {
            DataType::List(item) => {
                assert_eq!(item.data_type(), &DataType::Utf8);
            }
            other => panic!("expected List, got {other:?}"),
        }
    }

    #[test]
    fn metadata_records_source_type_and_connector() {
        let c = col("status", "enum", "enum('active','disabled')", false);
        let schema = build_envelope_arrow_schema(
            Connector::Mysql,
            &[c],
            &TypeConversionOpts::default(),
        )
        .unwrap();
        let before = schema.field_with_name("before_status").unwrap();
        let md = before.metadata();
        assert_eq!(
            md.get("deltaforge.connector").map(String::as_str),
            Some("mysql")
        );
        assert_eq!(
            md.get("deltaforge.source_type").map(String::as_str),
            Some("enum('active','disabled')")
        );
    }

    #[test]
    fn empty_table_only_emits_meta_and_extension_columns() {
        let schema = build_envelope_arrow_schema(
            Connector::Postgres,
            &[],
            &TypeConversionOpts::default(),
        )
        .unwrap();
        // 10 envelope meta columns + before/after extensions.
        assert_eq!(schema.fields().len(), 12);
    }

    #[test]
    fn extension_columns_are_stable_and_a_colliding_column_is_refused() {
        let schema = build_envelope_arrow_schema(
            Connector::Mysql,
            &[col("id", "bigint", "bigint", false)],
            &TypeConversionOpts::default(),
        )
        .unwrap();
        for f in ["before_extensions", "after_extensions"] {
            assert_eq!(
                schema.field_with_name(f).unwrap().data_type(),
                &DataType::Utf8
            );
        }
        assert!(
            build_envelope_arrow_schema(
                Connector::Mysql,
                &[col("extensions", "json", "json", true)],
                &TypeConversionOpts::default(),
            )
            .is_err()
        );
    }
}
