//! Row images converted to Avro from their canonical values (DDL-derived
//! schemas): each column's value maps to its field's Avro type exactly, or
//! fails before any record exists. An unchanged TOAST value, a document,
//! untyped bytes, or a column the schema version does not define is never
//! written and never dropped: it is a sink conversion failure.

use apache_avro::Schema as AvroSchema;
use apache_avro::schema::{RecordSchema, UnionSchema};
use apache_avro::types::Value as AvroValue;

use super::{Cx, EncodingError, SOURCE_TYPE_ATTR, kind_of};
use crate::canonical::render::{
    date_text, datetime_text, decimal_text, time_text, timestamp_text,
    uuid_text,
};
use crate::canonical::{
    CanonicalRow, CanonicalValue, DateTimeValue, DateValue, DbValue,
    MICROS_PER_DAY, TimestampValue,
};

impl Cx<'_> {
    fn refuse(
        &self,
        v: &CanonicalValue,
        target: &str,
        reason: &'static str,
    ) -> EncodingError {
        EncodingError::Conversion(super::super::ConversionFailure {
            column: self.path(),
            source_type: self
                .source
                .map(str::to_string)
                .unwrap_or_else(|| value_kind(v).to_string()),
            target_type: format!("Avro {target}"),
            reason,
        })
    }
}

/// The canonical kind of a value, for failures without a declared type.
fn value_kind(v: &CanonicalValue) -> &'static str {
    match v {
        CanonicalValue::Null => "SQL NULL",
        CanonicalValue::Unchanged => "unchanged TOAST",
        CanonicalValue::Opaque(_) => "opaque text",
        CanonicalValue::Document(_) => "document",
        CanonicalValue::Bytes(_) => "bytes",
        CanonicalValue::Db(_) => "database value",
    }
}

/// A row image as the record `r`.
pub(super) fn convert_row(
    row: &CanonicalRow,
    r: &RecordSchema,
    cx: &Cx<'_>,
) -> Result<AvroValue, EncodingError> {
    if let Some((name, v)) =
        row.iter().find(|(n, _)| !r.lookup.contains_key(*n))
    {
        return Err(cx.field(name, None).refuse(
            v,
            "record",
            "a column the event's schema version does not define",
        ));
    }
    let mut fields = Vec::with_capacity(r.fields.len());
    for field in &r.fields {
        let source = field
            .custom_attributes
            .get(SOURCE_TYPE_ATTR)
            .and_then(|v| v.as_str());
        let fcx = cx.field(&field.name, source);
        let value = row.get(&field.name).unwrap_or(&CanonicalValue::Null);
        fields.push((
            field.name.clone(),
            convert_value(value, &field.schema, &fcx)?,
        ));
    }
    Ok(AvroValue::Record(fields))
}

fn convert_union(
    v: &CanonicalValue,
    u: &UnionSchema,
    cx: &Cx<'_>,
) -> Result<AvroValue, EncodingError> {
    let variants = u.variants();
    if v.is_null() {
        return variants
            .iter()
            .position(|s| matches!(s, AvroSchema::Null))
            .map(|i| AvroValue::Union(i as u32, Box::new(AvroValue::Null)))
            .ok_or_else(|| {
                cx.refuse(v, "union", "null for a non-nullable field")
            });
    }
    let mut last = None;
    for (i, s) in variants.iter().enumerate() {
        if matches!(s, AvroSchema::Null) {
            continue;
        }
        match convert_value(v, s, cx) {
            Ok(x) => return Ok(AvroValue::Union(i as u32, Box::new(x))),
            Err(e) => last = Some(e),
        }
    }
    Err(last.unwrap_or_else(|| {
        cx.refuse(v, "union", "no branch accepts the value")
    }))
}

/// One column value as `schema`.
pub(super) fn convert_value(
    v: &CanonicalValue,
    schema: &AvroSchema,
    cx: &Cx<'_>,
) -> Result<AvroValue, EncodingError> {
    use AvroSchema as S;
    match (schema, v) {
        (_, CanonicalValue::Unchanged) => Err(cx.refuse(
            v,
            kind_of(schema),
            "PostgreSQL did not send the value (unchanged TOAST)",
        )),
        (_, CanonicalValue::Document(_)) => Err(cx.refuse(
            v,
            kind_of(schema),
            "an untyped document in a schema-typed column",
        )),
        (S::Union(u), _) => convert_union(v, u, cx),
        // Bytes encode as binary, exactly; never as another type.
        (S::Bytes, CanonicalValue::Bytes(b)) => Ok(AvroValue::Bytes(b.clone())),
        (S::Fixed(f), CanonicalValue::Bytes(b)) if b.len() == f.size => {
            Ok(AvroValue::Fixed(f.size, b.clone()))
        }
        (_, CanonicalValue::Bytes(_)) => Err(cx.refuse(
            v,
            kind_of(schema),
            "bytes for a non-binary column (or of another fixed width)",
        )),
        (S::Ref { name }, _) => {
            let target = cx
                .names
                .get(name)
                .or_else(|| {
                    cx.names
                        .iter()
                        .find(|(n, _)| n.name == name.name)
                        .map(|(_, s)| s)
                })
                .ok_or_else(|| {
                    cx.refuse(v, "named type", "unresolved named reference")
                })?;
            convert_value(v, target, cx)
        }
        (S::Null, CanonicalValue::Null) => Ok(AvroValue::Null),
        (_, CanonicalValue::Null) => {
            Err(cx.refuse(v, kind_of(schema), "null for a non-nullable field"))
        }
        (S::String, CanonicalValue::Opaque(o)) => {
            Ok(AvroValue::String(o.text.clone()))
        }
        (_, CanonicalValue::Opaque(_)) => Err(cx.refuse(
            v,
            kind_of(schema),
            "an opaque source value has only its text",
        )),
        (_, CanonicalValue::Db(db)) => convert_db(v, db, schema, cx),
    }
}

fn convert_db(
    v: &CanonicalValue,
    db: &DbValue,
    schema: &AvroSchema,
    cx: &Cx<'_>,
) -> Result<AvroValue, EncodingError> {
    use AvroSchema as S;
    let fail = |reason| cx.refuse(v, kind_of(schema), reason);
    match (schema, db) {
        (S::Boolean, DbValue::Bool(b)) => Ok(AvroValue::Boolean(*b)),
        (S::Boolean, DbValue::Bits(b)) if b.len() == 1 => {
            Ok(AvroValue::Boolean(b.to_u64() == 1))
        }
        (S::Int, DbValue::Int { value, .. }) => i32::try_from(*value)
            .map(AvroValue::Int)
            .map_err(|_| fail("not in the signed 32-bit range")),
        (S::Int, DbValue::UInt { value, .. }) => i32::try_from(*value)
            .map(AvroValue::Int)
            .map_err(|_| fail("not in the signed 32-bit range")),
        (S::Int, DbValue::Year(y)) => Ok(AvroValue::Int(*y as i32)),
        (S::Long, DbValue::Int { value, .. }) => Ok(AvroValue::Long(*value)),
        (S::Long, DbValue::UInt { value, .. }) => i64::try_from(*value)
            .map(AvroValue::Long)
            .map_err(|_| fail("an unsigned value above i64::MAX")),
        (S::Long, DbValue::Year(y)) => Ok(AvroValue::Long(*y as i64)),
        // MySQL TIME: a signed duration in microseconds.
        (S::Long, DbValue::Time(t)) => Ok(AvroValue::Long(t.micros())),
        (S::Float, DbValue::Float32(f)) => Ok(AvroValue::Float(*f)),
        (S::Double, DbValue::Float64(f)) => Ok(AvroValue::Double(*f)),
        (S::Double, DbValue::Float32(f)) => Ok(AvroValue::Double(*f as f64)),
        (S::String, db) => string_of(db)
            .map(AvroValue::String)
            .ok_or_else(|| fail("no exact text for the value's type")),
        (S::Uuid, DbValue::Uuid(u)) => {
            Ok(AvroValue::Uuid(uuid::Uuid::from_bytes(*u)))
        }
        (S::Bytes, DbValue::Binary(b)) => Ok(AvroValue::Bytes(b.clone())),
        (S::Bytes, DbValue::Bits(b)) => {
            Ok(AvroValue::Bytes(b.bytes().to_vec()))
        }
        (S::Fixed(f), DbValue::Bits(b)) if b.bytes().len() == f.size => {
            Ok(AvroValue::Fixed(f.size, b.bytes().to_vec()))
        }
        (S::Fixed(f), DbValue::Binary(b)) if b.len() == f.size => {
            Ok(AvroValue::Fixed(f.size, b.clone()))
        }
        (S::Fixed(f), DbValue::Uuid(u)) if f.size == 16 => {
            Ok(AvroValue::Fixed(16, u.to_vec()))
        }
        (S::Enum(e), DbValue::Enum(x)) => e
            .symbols
            .iter()
            .position(|s| *s == x.label)
            .map(|i| AvroValue::Enum(i as u32, x.label.clone()))
            .ok_or_else(|| fail("not a symbol of the enum")),
        (S::Date, DbValue::Date(DateValue::Civil(d))) => d
            .days_since_epoch()
            .and_then(|days| i32::try_from(days).ok())
            .map(AvroValue::Date)
            .ok_or_else(|| {
                fail("a MySQL zero date or one outside the 32-bit day range")
            }),
        (S::Date, DbValue::Date(_)) => Err(fail("an infinite date")),
        (S::TimeMicros, DbValue::Time(t))
            if (0..MICROS_PER_DAY).contains(&t.micros()) =>
        {
            Ok(AvroValue::TimeMicros(t.micros()))
        }
        (S::TimeMillis, DbValue::Time(t))
            if (0..MICROS_PER_DAY).contains(&t.micros())
                && t.micros() % 1000 == 0 =>
        {
            Ok(AvroValue::TimeMillis((t.micros() / 1000) as i32))
        }
        (
            S::TimestampMicros,
            DbValue::Timestamp(TimestampValue::Instant { micros, .. }),
        ) => Ok(AvroValue::TimestampMicros(*micros)),
        (
            S::TimestampMillis,
            DbValue::Timestamp(TimestampValue::Instant { micros, .. }),
        ) if micros % 1000 == 0 => {
            Ok(AvroValue::TimestampMillis(micros / 1000))
        }
        (S::TimestampMicros | S::TimestampMillis, DbValue::Timestamp(_)) => {
            Err(fail(
                "a MySQL zero timestamp, an infinity, or finer than the unit",
            ))
        }
        (
            S::LocalTimestampMicros,
            DbValue::DateTime(DateTimeValue::Civil {
                date,
                micros_of_day,
                ..
            }),
        ) => date
            .days_since_epoch()
            .and_then(|d| d.checked_mul(MICROS_PER_DAY))
            .and_then(|d| d.checked_add(*micros_of_day))
            .map(AvroValue::LocalTimestampMicros)
            .ok_or_else(|| fail("a MySQL zero date-time")),
        (
            S::LocalTimestampMicros | S::LocalTimestampMillis,
            DbValue::DateTime(_),
        ) => Err(fail(
            "a MySQL zero date-time, an infinity, or finer than the unit",
        )),
        (S::Array(a), DbValue::Array(arr)) => {
            let item = cx.item();
            arr.items()
                .iter()
                .map(|i| convert_value(i, &a.items, &item))
                .collect::<Result<Vec<_>, _>>()
                .map(AvroValue::Array)
        }
        _ => Err(fail("the value's type does not match the field's type")),
    }
}

/// The exact text of a value for a `string` field (DECIMAL, BIGINT UNSIGNED
/// in string mode, ENUM and SET labels, JSON text, temporal text in string
/// mode); `None` for values whose text form is not their typed content.
fn string_of(db: &DbValue) -> Option<String> {
    Some(match db {
        DbValue::Text(s) => s.clone(),
        DbValue::Decimal(d) => decimal_text(d).to_string(),
        DbValue::Int { value, .. } => value.to_string(),
        DbValue::UInt { value, .. } => value.to_string(),
        DbValue::Enum(e) => e.label.clone(),
        DbValue::Set(s) => s.members().join(","),
        DbValue::Json(j) => j.as_str().to_string(),
        DbValue::Uuid(u) => uuid_text(u),
        DbValue::Date(d) => date_text(d),
        DbValue::Time(t) => time_text(t),
        DbValue::DateTime(dt) => datetime_text(dt),
        DbValue::Timestamp(ts) => timestamp_text(ts),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::super::json_to_avro_rows;
    use crate::canonical::{
        Bits, CanonicalRow, CanonicalValue, CivilDate, DateValue, DbValue,
        Document, IntWidth, Precision, TimeValue, TimestampValue,
    };
    use apache_avro::Schema;
    use apache_avro::types::Value as A;
    use serde_json::json;

    fn schema() -> Schema {
        Schema::parse(&json!({
            "type": "record", "name": "Env",
            "fields": [
                {"name": "op", "type": "string"},
                {"name": "after", "type": ["null", {
                    "type": "record", "name": "Row",
                    "fields": [
                        {"name": "id", "type": "long", "deltaforge.source_type": "bigint unsigned"},
                        {"name": "b", "type": {"type": "fixed", "name": "b_bits", "size": 2}, "deltaforge.source_type": "bit(12)"},
                        {"name": "t", "type": {"type": "long", "deltaforge.logical": "mysql-time-micros"}, "deltaforge.source_type": "time(3)"},
                        {"name": "ts", "type": ["null", {"type": "long", "logicalType": "timestamp-micros"}], "deltaforge.source_type": "timestamp(6)"},
                        {"name": "d", "type": ["null", {"type": "int", "logicalType": "date"}], "deltaforge.source_type": "date"}
                    ]
                }]}
            ]
        }))
        .unwrap()
    }

    fn row(extra: Vec<(&str, CanonicalValue)>) -> CanonicalRow {
        let mut cols = vec![
            (
                "id".to_string(),
                CanonicalValue::Db(DbValue::uint(7, IntWidth::W64).unwrap()),
            ),
            (
                "b".to_string(),
                CanonicalValue::Db(DbValue::Bits(
                    Bits::new(12, vec![0x0a, 0xbc]).unwrap(),
                )),
            ),
            (
                "t".to_string(),
                CanonicalValue::Db(DbValue::Time(
                    TimeValue::mysql(-1_500_000, Precision::Fixed(3)).unwrap(),
                )),
            ),
            (
                "ts".to_string(),
                CanonicalValue::Db(DbValue::Timestamp(
                    TimestampValue::instant(5, Precision::Fixed(6)).unwrap(),
                )),
            ),
            ("d".to_string(), CanonicalValue::Null),
        ];
        for (n, v) in extra {
            if let Some(slot) = cols.iter_mut().find(|(c, _)| c == n) {
                slot.1 = v;
            } else {
                cols.push((n.to_string(), v));
            }
        }
        CanonicalRow::new(cols).unwrap()
    }

    fn convert(r: &CanonicalRow) -> Result<A, crate::encoding::EncodingError> {
        // The JSON row is ignored: the canonical row is converted.
        json_to_avro_rows(
            &json!({"op": "c", "after": {"id": "ignored"}}),
            &schema(),
            &[("after", r)],
        )
    }

    #[test]
    fn row_images_convert_from_canonical_values() {
        let v = convert(&row(vec![])).unwrap();
        let A::Record(fields) = v else { panic!() };
        let A::Union(1, after) = &fields[1].1 else {
            panic!("{:?}", fields[1])
        };
        let A::Record(cols) = after.as_ref() else {
            panic!()
        };
        assert_eq!(cols[0].1, A::Long(7));
        assert_eq!(cols[1].1, A::Fixed(2, vec![0x0a, 0xbc]));
        assert_eq!(cols[2].1, A::Long(-1_500_000));
        assert_eq!(cols[3].1, A::Union(1, Box::new(A::TimestampMicros(5))));
        assert_eq!(cols[4].1, A::Union(0, Box::new(A::Null)));
    }

    #[test]
    fn untyped_unchanged_undefined_and_inexact_values_are_refused() {
        for (col, v, reason) in [
            ("ts", CanonicalValue::Unchanged, "unchanged TOAST"),
            (
                "ts",
                CanonicalValue::Document(Document::new(json!(1))),
                "untyped document",
            ),
            ("id", CanonicalValue::Bytes(vec![1]), "non-binary column"),
            ("b", CanonicalValue::Bytes(vec![1]), "fixed width"),
            (
                "extra",
                CanonicalValue::Document(Document::new(json!(1))),
                "does not define",
            ),
            (
                "id",
                CanonicalValue::Db(
                    DbValue::uint(u64::MAX, IntWidth::W64).unwrap(),
                ),
                "above i64::MAX",
            ),
            (
                "ts",
                CanonicalValue::Db(DbValue::Timestamp(TimestampValue::Zero {
                    precision: Precision::Fixed(6),
                })),
                "zero timestamp",
            ),
            (
                "d",
                CanonicalValue::Db(DbValue::Date(DateValue::Civil(
                    CivilDate::mysql(0, 0, 0).unwrap(),
                ))),
                "zero date",
            ),
        ] {
            let err = convert(&row(vec![(col, v)])).unwrap_err().to_string();
            assert!(err.contains(reason), "{col}: {err}");
        }
    }
}

#[cfg(test)]
mod extension_tests {
    use super::super::{json_to_avro_rows, row_paths};
    use crate::canonical::{CanonicalRow, Document, fixture};
    use crate::encoding::avro_schema::{
        build_envelope_schema, build_value_schema,
    };
    use apache_avro::types::Value as A;
    use serde_json::json;

    /// Extensions go to the stable sibling slot; the row record holds only
    /// declared columns, and the schema is the same whatever extensions an
    /// event carries.
    #[test]
    fn extensions_fill_the_stable_sibling_slot() {
        let value = build_value_schema(
            "mysql",
            "d",
            "t",
            vec![
                json!({"name": "id", "type": "int", "deltaforge.source_type": "int"}),
            ],
        );
        let (_, schema) =
            build_envelope_schema("mysql", "d", "t", value).unwrap();
        let mut ev = crate::Event::new_row(
            crate::EventId::mysql_row_server(1, "t", 1, 0),
            crate::SourceInfo {
                version: "v".into(),
                connector: "mysql".into(),
                name: "p".into(),
                ts_ms: 1,
                db: "d".into(),
                schema: None,
                table: "t".into(),
                snapshot: None,
                position: Default::default(),
            },
            crate::Op::Create,
            None,
            Some(
                CanonicalRow::with_extensions(
                    fixture::row(json!({"id": 1})).into_columns(),
                    vec![(
                        "meta".to_string(),
                        Document::new(json!({"b": 1, "a": [2]})),
                    )],
                )
                .unwrap(),
            ),
            crate::TypeMapping::V2,
            1,
            1,
        );
        ev.event_id = None;
        let mut env = json!({"op": "c", "ts_ms": 1, "source": {
            "version": "v", "connector": "mysql", "name": "p", "ts_ms": 1, "db": "d", "table": "t"
        }});
        env["before_extensions"] = serde_json::Value::Null;
        env["after_extensions"] = json!({"meta": "{\"a\":[2],\"b\":1}"});
        let v = json_to_avro_rows(&env, &schema, &row_paths(&ev)).unwrap();
        let A::Record(fields) = v else { panic!() };
        let get = |n: &str| {
            fields
                .iter()
                .find(|(k, _)| k == n)
                .map(|(_, v)| v.clone())
                .unwrap()
        };
        let A::Union(1, after) = get("after") else {
            panic!("{:?}", get("after"))
        };
        let A::Record(cols) = *after else { panic!() };
        assert_eq!(cols.len(), 1, "declared columns only");
        let A::Union(1, ext) = get("after_extensions") else {
            panic!()
        };
        let A::Map(m) = *ext else { panic!() };
        assert_eq!(m["meta"], A::String("{\"a\":[2],\"b\":1}".into()));
        assert_eq!(get("before_extensions"), A::Union(0, Box::new(A::Null)));
    }
}
