//! The declared column types of a registered schema version (see
//! [`deltaforge_core::canonical::schema`]), derived through the same type
//! mapping the source decoders use, so a processor types a value exactly as
//! the source would have decoded it.

use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use deltaforge_core::canonical::schema::{ColumnType, ColumnTypes};

use crate::mysql::mysql_decode::ColumnKind;
use crate::mysql::{MySqlColumn, MySqlTableSchema};
use crate::postgres::postgres_decode::PgKind;

fn mysql_type(col: &MySqlColumn) -> ColumnType {
    match ColumnKind::of(col) {
        Ok(ColumnKind::Int { width, unsigned }) => {
            ColumnType::Int { width, unsigned }
        }
        Ok(ColumnKind::Decimal) => ColumnType::Decimal,
        Ok(ColumnKind::Float) => ColumnType::Float32,
        Ok(ColumnKind::Double) => ColumnType::Float64,
        Ok(ColumnKind::Bit { len }) => ColumnType::Bits { len },
        Ok(ColumnKind::Text { .. }) => ColumnType::Text,
        Ok(ColumnKind::Binary { fixed }) => ColumnType::Binary { fixed },
        Ok(ColumnKind::Enum { labels }) => ColumnType::Enum { labels },
        Ok(ColumnKind::Set { labels }) => ColumnType::Set { labels },
        Ok(ColumnKind::Json) => ColumnType::Json,
        Ok(ColumnKind::Date) => ColumnType::Date,
        Ok(ColumnKind::Time { precision }) => ColumnType::Time { precision },
        Ok(ColumnKind::DateTime { precision }) => {
            ColumnType::DateTime { precision }
        }
        Ok(ColumnKind::Timestamp { precision }) => {
            ColumnType::Timestamp { precision }
        }
        Ok(ColumnKind::Year) => ColumnType::Year,
        Err(reason) => ColumnType::Unsupported(reason),
    }
}

/// The declared type of a PostgreSQL column decoded as `kind`.
pub fn postgres_type(kind: &PgKind) -> ColumnType {
    use deltaforge_core::canonical::IntWidth;
    match kind {
        PgKind::Bool => ColumnType::Bool,
        PgKind::Int(width) => ColumnType::Int {
            width: *width,
            unsigned: false,
        },
        PgKind::Oid => ColumnType::Int {
            width: IntWidth::W32,
            unsigned: true,
        },
        PgKind::Float4 => ColumnType::Float32,
        PgKind::Float8 => ColumnType::Float64,
        PgKind::Numeric => ColumnType::Decimal,
        PgKind::Text => ColumnType::Text,
        PgKind::Bytea => ColumnType::Binary { fixed: None },
        PgKind::Uuid => ColumnType::Uuid,
        PgKind::Json => ColumnType::Json,
        PgKind::Date => ColumnType::Date,
        PgKind::Time(precision) => ColumnType::Time {
            precision: *precision,
        },
        PgKind::Timestamp(precision) => ColumnType::DateTime {
            precision: *precision,
        },
        PgKind::Timestamptz(precision) => ColumnType::Timestamp {
            precision: *precision,
        },
        PgKind::Array {
            element,
            element_type,
        } => ColumnType::Array {
            element: Box::new(postgres_type(element)),
            element_type: element_type.clone(),
        },
        PgKind::Opaque(t) => ColumnType::Opaque(t.clone()),
    }
}

/// The declared types of a registered schema version (`schema_json`, as the
/// registry stores it) of a `connector` source.
pub fn column_types(
    connector: &str,
    schema_json: &serde_json::Value,
    fingerprint: Arc<str>,
    sequence: Option<u64>,
) -> Result<ColumnTypes> {
    let columns = match connector {
        "mysql" => {
            let schema: MySqlTableSchema =
                serde_json::from_value(schema_json.clone()).map_err(|e| {
                    anyhow!("registered MySQL schema unreadable: {e}")
                })?;
            schema
                .columns
                .iter()
                .map(|c| (c.name.clone(), mysql_type(c)))
                .collect()
        }
        "postgresql" | "postgres" => postgres_columns(schema_json)?,
        other => bail!("no declared types for connector {other:?}"),
    };
    Ok(ColumnTypes::new(fingerprint, sequence, columns))
}

/// PostgreSQL registers two shapes: the Relation-proven event schema (CDC;
/// builtin type names) and the catalog table schema (snapshot; type OIDs).
fn postgres_columns(
    schema_json: &serde_json::Value,
) -> Result<Vec<(String, ColumnType)>> {
    use crate::postgres::postgres_event_schema::PgEventSchema;
    use crate::postgres::postgres_table_schema::PostgresTableSchema;
    if schema_json.get("format").is_some() {
        let schema: PgEventSchema = serde_json::from_value(schema_json.clone())
            .map_err(|e| {
                anyhow!("registered PostgreSQL event schema unreadable: {e}")
            })?;
        Ok(schema
            .columns
            .iter()
            .map(|c| {
                let kind = crate::postgres::postgres_decode::builtin_type_oid(
                    &c.type_name,
                )
                .map(|oid| PgKind::of(oid, c.typmod, None))
                .unwrap_or_else(|| {
                    PgKind::Opaque(deltaforge_core::canonical::SourceType {
                        engine: deltaforge_core::canonical::Engine::Postgres,
                        name: c.type_name.clone(),
                        oid: None,
                        typmod: (c.typmod != -1).then_some(c.typmod),
                    })
                });
                (c.name.clone(), postgres_type(&kind))
            })
            .collect())
    } else {
        let schema: PostgresTableSchema =
            serde_json::from_value(schema_json.clone()).map_err(|e| {
                anyhow!("registered PostgreSQL schema unreadable: {e}")
            })?;
        schema
            .columns
            .iter()
            .map(|c| {
                let oid = c.type_oid.ok_or_else(|| {
                    anyhow!("column {:?} has no recorded type OID", c.name)
                })?;
                let kind = PgKind::of(
                    oid,
                    c.type_modifier.unwrap_or(-1),
                    c.udt_name.as_deref(),
                );
                Ok((c.name.clone(), postgres_type(&kind)))
            })
            .collect()
    }
}

/// A PostgreSQL column of a registered schema version as encoders describe
/// it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgEncoderColumn {
    pub name: String,
    /// The builtin type name (`int8`, `_text`, `timestamptz`), or
    /// `user_defined`.
    pub type_name: String,
    pub is_array: bool,
    /// The element's builtin type name of an array.
    pub element_type: Option<String>,
    /// A `numeric` column's declared precision and scale.
    pub precision: Option<i64>,
    pub scale: Option<i64>,
}

/// The encoder columns of a PostgreSQL schema version, or `None` when
/// `schema_json` is not a PostgreSQL shape. Both registered shapes of the
/// same DDL (the Relation-proven event schema of CDC and the catalog table
/// schema of the snapshot) describe every column identically: by its type
/// OID and modifier, and nullable (a Relation proves no NOT NULL), so a
/// snapshot row and a CDC row encode under the same schema.
pub fn postgres_encoder_columns(
    schema_json: &serde_json::Value,
) -> Option<Result<Vec<PgEncoderColumn>>> {
    use crate::postgres::postgres_event_schema::PgEventSchema;
    use crate::postgres::postgres_table_schema::PostgresTableSchema;
    let is_event = schema_json
        .get("format")
        .and_then(|f| f.as_str())
        .is_some_and(|f| f.starts_with("pg_event"));
    let is_catalog = schema_json
        .get("columns")
        .and_then(|c| c.as_array())
        .is_some_and(|c| c.iter().any(|c| c.get("type_oid").is_some()));
    let typed: Result<Vec<(String, String, i32)>> = if is_event {
        serde_json::from_value::<PgEventSchema>(schema_json.clone())
            .map_err(|e| {
                anyhow!("registered PostgreSQL event schema unreadable: {e}")
            })
            .map(|s| {
                s.columns
                    .into_iter()
                    .map(|c| (c.name, c.type_name, c.typmod))
                    .collect()
            })
    } else if is_catalog {
        serde_json::from_value::<PostgresTableSchema>(schema_json.clone())
            .map_err(|e| anyhow!("registered PostgreSQL schema unreadable: {e}"))
            .and_then(|s| {
                s.columns
                    .into_iter()
                    .map(|c| {
                        let oid = c.type_oid.ok_or_else(|| {
                            anyhow!("column {:?} has no recorded type OID", c.name)
                        })?;
                        Ok((
                            c.name,
                            crate::postgres::postgres_event_schema::type_name_of(oid)
                                .to_string(),
                            c.type_modifier.unwrap_or(-1),
                        ))
                    })
                    .collect()
            })
    } else {
        return None;
    };
    Some(typed.map(|cols| {
        cols.into_iter()
            .map(|(name, type_name, typmod)| {
                let element = type_name
                    .strip_prefix('_')
                    .filter(|e| builtin_type_oid_of(e))
                    .map(str::to_string);
                let numeric =
                    matches!(type_name.as_str(), "numeric" | "_numeric")
                        && typmod >= 4;
                PgEncoderColumn {
                    name,
                    is_array: element.is_some(),
                    element_type: element,
                    precision: numeric
                        .then(|| i64::from(((typmod - 4) >> 16) & 0xffff)),
                    scale: numeric.then(|| i64::from((typmod - 4) & 0xffff)),
                    type_name,
                }
            })
            .collect()
    }))
}

fn builtin_type_oid_of(name: &str) -> bool {
    crate::postgres::postgres_decode::builtin_type_oid(name).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltaforge_core::canonical::IntWidth;
    use serde_json::json;

    #[test]
    fn mysql_types_come_from_the_decoder_mapping() {
        let mut e = MySqlColumn::new("e", "enum('a','b')", "enum", true, 1);
        e.collation_id = Some(255);
        let schema = MySqlTableSchema::new(vec![
            MySqlColumn::new("id", "bigint unsigned", "bigint", false, 1),
            e,
            MySqlColumn::new("v", "vector(3)", "vector", true, 3),
        ]);
        let t = column_types(
            "mysql",
            &serde_json::to_value(&schema).unwrap(),
            "fp".into(),
            Some(1),
        )
        .unwrap();
        assert_eq!(
            t.get("id"),
            Some(&ColumnType::Int {
                width: IntWidth::W64,
                unsigned: true
            })
        );
        assert_eq!(
            t.get("e"),
            Some(&ColumnType::Enum {
                labels: vec!["a".into(), "b".into()]
            })
        );
        assert!(matches!(t.get("v"), Some(ColumnType::Unsupported(_))));
    }

    /// The snapshot (catalog) and CDC (event) schema of the same DDL give
    /// encoders identical columns, so both read paths encode alike.
    #[test]
    fn both_postgres_shapes_of_one_ddl_give_identical_encoder_columns() {
        use crate::postgres::postgres_table_schema::{
            PostgresColumn, PostgresTableSchema,
        };
        // id bigint NOT NULL, n numeric(10,2), tags text[], mood (an enum),
        // ts timestamptz(3).
        let typed = |name: &str,
                     ty: &str,
                     oid: u32,
                     typmod: Option<i32>,
                     nullable: bool| {
            let mut c = PostgresColumn::new(name, ty, nullable, 1);
            c.type_oid = Some(oid);
            c.type_modifier = typmod;
            c
        };
        let catalog = serde_json::to_value(PostgresTableSchema::new(vec![
            typed("id", "bigint", 20, None, false),
            typed("n", "numeric(10,2)", 1700, Some((10 << 16) + 2 + 4), true),
            typed("tags", "text[]", 1009, None, true),
            typed("mood", "mood", 16_385, None, true),
            typed("ts", "timestamp with time zone", 1184, Some(3), true),
        ]))
        .unwrap();
        let event = json!({
            "format": "pg_event_v2",
            "replica_identity": "d",
            "columns": [
                {"name": "id", "type": "int8", "typmod": -1, "key": true},
                {"name": "n", "type": "numeric", "typmod": (10 << 16) + 2 + 4, "key": false},
                {"name": "tags", "type": "_text", "typmod": -1, "key": false},
                {"name": "mood", "type": "user_defined", "typmod": -1, "key": false},
                {"name": "ts", "type": "timestamptz", "typmod": 3, "key": false}
            ]
        });
        let a = postgres_encoder_columns(&catalog).unwrap().unwrap();
        let b = postgres_encoder_columns(&event).unwrap().unwrap();
        assert_eq!(a, b);
        assert_eq!(a[1].precision, Some(10));
        assert_eq!(a[1].scale, Some(2));
        assert_eq!(a[2].element_type.as_deref(), Some("text"));
        assert_eq!(a[3].type_name, "user_defined");
        // A MySQL schema is not a PostgreSQL shape.
        assert!(
            postgres_encoder_columns(
                &json!({"columns": [{"name": "id", "data_type": "bigint"}]})
            )
            .is_none()
        );
    }

    #[test]
    fn postgres_event_schemas_map_builtin_names() {
        let schema = json!({
            "format": "pg_event_v2",
            "replica_identity": "d",
            "columns": [
                {"name": "id", "type": "int8", "typmod": -1, "key": true},
                {"name": "ts", "type": "timestamptz", "typmod": 3, "key": false},
                {"name": "tags", "type": "_text", "typmod": -1, "key": false},
                {"name": "mood", "type": "user_defined", "typmod": -1, "key": false}
            ]
        });
        let t = column_types("postgresql", &schema, "fp".into(), None).unwrap();
        assert_eq!(
            t.get("id"),
            Some(&ColumnType::Int {
                width: IntWidth::W64,
                unsigned: false
            })
        );
        assert_eq!(
            t.get("ts"),
            Some(&ColumnType::Timestamp {
                precision: deltaforge_core::canonical::Precision::Fixed(3)
            })
        );
        assert!(matches!(t.get("tags"), Some(ColumnType::Array { .. })));
        assert!(matches!(t.get("mood"), Some(ColumnType::Opaque(_))));
    }
}
