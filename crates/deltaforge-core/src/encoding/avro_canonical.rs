//! Row images converted to Avro from their canonical values (DDL-derived
//! schemas): each column's value maps to its field's Avro type exactly, or
//! fails before any record exists. An unchanged TOAST value, a document,
//! untyped bytes, or a column the schema version does not define is never
//! written and never dropped: it is a sink conversion failure.

use std::borrow::Cow;

use super::super::avro_plan::{Out, Plan, RecordPlan};
use super::{Cx, EncodingError, back};
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
pub(super) fn convert_row<O: Out>(
    row: &CanonicalRow,
    r: &RecordPlan,
    cx: &Cx<'_>,
    out: &mut O,
) -> Result<(), EncodingError> {
    if let Some((name, v)) = row.iter().find(|(n, _)| !r.defines(n)) {
        return Err(cx.field(name, None).refuse(
            v,
            "record",
            "a column the event's schema version does not define",
        ));
    }
    out.record_begin(r.fields.len());
    for (i, field) in r.fields.iter().enumerate() {
        let fcx = cx.field(&field.name, field.source.as_deref());
        let value = row
            .get_hinted(i, &field.name)
            .unwrap_or(&CanonicalValue::Null);
        out.field(&field.name);
        convert_value(value, &field.plan, &fcx, out)?;
    }
    out.record_end();
    Ok(())
}

fn convert_union<O: Out>(
    v: &CanonicalValue,
    variants: &[Plan],
    cx: &Cx<'_>,
    out: &mut O,
) -> Result<(), EncodingError> {
    if v.is_null() {
        let i = variants
            .iter()
            .position(|s| matches!(s, Plan::Null))
            .ok_or_else(|| {
                cx.refuse(v, "union", "null for a non-nullable field")
            })?;
        out.union_branch(i as u32);
        out.null();
        return Ok(());
    }
    let mut last = None;
    for (i, s) in variants.iter().enumerate() {
        if matches!(s, Plan::Null) {
            continue;
        }
        let mark = out.mark();
        out.union_branch(i as u32);
        match convert_value(v, s, cx, out) {
            Ok(()) => return Ok(()),
            Err(e) => {
                out.reset(mark);
                last = Some(e);
            }
        }
    }
    Err(last.unwrap_or_else(|| {
        cx.refuse(v, "union", "no branch accepts the value")
    }))
}

/// One column value as `plan`.
pub(super) fn convert_value<O: Out>(
    v: &CanonicalValue,
    plan: &Plan,
    cx: &Cx<'_>,
    out: &mut O,
) -> Result<(), EncodingError> {
    match (plan, v) {
        (_, CanonicalValue::Unchanged) => Err(cx.refuse(
            v,
            plan.kind(),
            "PostgreSQL did not send the value (unchanged TOAST)",
        )),
        (_, CanonicalValue::Document(_)) => Err(cx.refuse(
            v,
            plan.kind(),
            "an untyped document in a schema-typed column",
        )),
        (Plan::Union(u), _) => convert_union(v, u, cx, out),
        // Bytes encode as binary, exactly; never as another type.
        (Plan::Bytes, CanonicalValue::Bytes(b)) => {
            out.bytes(b);
            Ok(())
        }
        (Plan::Fixed(size), CanonicalValue::Bytes(b)) if b.len() == *size => {
            out.fixed(b);
            Ok(())
        }
        (_, CanonicalValue::Bytes(_)) => Err(cx.refuse(
            v,
            plan.kind(),
            "bytes for a non-binary column (or of another fixed width)",
        )),
        (Plan::Ref(target), _) => convert_value(v, target, cx, out),
        (Plan::Back(r), _) => convert_value(v, &back(r), cx, out),
        (Plan::Unresolved, _) => {
            Err(cx.refuse(v, "named type", "unresolved named reference"))
        }
        (Plan::Null, CanonicalValue::Null) => {
            out.null();
            Ok(())
        }
        (_, CanonicalValue::Null) => {
            Err(cx.refuse(v, plan.kind(), "null for a non-nullable field"))
        }
        (Plan::String, CanonicalValue::Opaque(o)) => {
            out.string(&o.text);
            Ok(())
        }
        (_, CanonicalValue::Opaque(_)) => Err(cx.refuse(
            v,
            plan.kind(),
            "an opaque source value has only its text",
        )),
        (_, CanonicalValue::Db(db)) => convert_db(v, db, plan, cx, out),
    }
}

fn convert_db<O: Out>(
    v: &CanonicalValue,
    db: &DbValue,
    plan: &Plan,
    cx: &Cx<'_>,
    out: &mut O,
) -> Result<(), EncodingError> {
    let fail = |reason| cx.refuse(v, plan.kind(), reason);
    match (plan, db) {
        (Plan::Boolean, DbValue::Bool(b)) => out.boolean(*b),
        (Plan::Boolean, DbValue::Bits(b)) if b.len() == 1 => {
            out.boolean(b.to_u64() == 1)
        }
        (Plan::Int, DbValue::Int { value, .. }) => out.int(
            i32::try_from(*value)
                .map_err(|_| fail("not in the signed 32-bit range"))?,
        ),
        (Plan::Int, DbValue::UInt { value, .. }) => out.int(
            i32::try_from(*value)
                .map_err(|_| fail("not in the signed 32-bit range"))?,
        ),
        (Plan::Int, DbValue::Year(y)) => out.int(*y as i32),
        (Plan::Long, DbValue::Int { value, .. }) => out.long(*value),
        (Plan::Long, DbValue::UInt { value, .. }) => out.long(
            i64::try_from(*value)
                .map_err(|_| fail("an unsigned value above i64::MAX"))?,
        ),
        (Plan::Long, DbValue::Year(y)) => out.long(*y as i64),
        // MySQL TIME: a signed duration in microseconds.
        (Plan::Long, DbValue::Time(t)) => out.long(t.micros()),
        (Plan::Float, DbValue::Float32(f)) => out.float(*f),
        (Plan::Double, DbValue::Float64(f)) => out.double(*f),
        (Plan::Double, DbValue::Float32(f)) => out.double(*f as f64),
        (Plan::String, db) => out.string(
            &string_of(db)
                .ok_or_else(|| fail("no exact text for the value's type"))?,
        ),
        (Plan::Uuid, DbValue::Uuid(u)) => out.uuid(u),
        (Plan::Bytes, DbValue::Binary(b)) => out.bytes(b),
        (Plan::Bytes, DbValue::Bits(b)) => out.bytes(b.bytes()),
        (Plan::Fixed(size), DbValue::Bits(b)) if b.bytes().len() == *size => {
            out.fixed(b.bytes())
        }
        (Plan::Fixed(size), DbValue::Binary(b)) if b.len() == *size => {
            out.fixed(b)
        }
        (Plan::Fixed(16), DbValue::Uuid(u)) => out.fixed(u),
        (Plan::Enum(symbols), DbValue::Enum(x)) => {
            let i = symbols
                .iter()
                .position(|s| **s == *x.label)
                .ok_or_else(|| fail("not a symbol of the enum"))?;
            out.enum_symbol(i as u32, &x.label)
        }
        (Plan::Date, DbValue::Date(DateValue::Civil(d))) => out.date(
            d.days_since_epoch()
                .and_then(|days| i32::try_from(days).ok())
                .ok_or_else(|| {
                    fail(
                        "a MySQL zero date or one outside the 32-bit day range",
                    )
                })?,
        ),
        (Plan::Date, DbValue::Date(_)) => {
            return Err(fail("an infinite date"));
        }
        (Plan::TimeMicros, DbValue::Time(t))
            if (0..MICROS_PER_DAY).contains(&t.micros()) =>
        {
            out.time_micros(t.micros())
        }
        (Plan::TimeMillis, DbValue::Time(t))
            if (0..MICROS_PER_DAY).contains(&t.micros())
                && t.micros() % 1000 == 0 =>
        {
            out.time_millis((t.micros() / 1000) as i32)
        }
        (
            Plan::TimestampMicros,
            DbValue::Timestamp(TimestampValue::Instant { micros, .. }),
        ) => out.timestamp_micros(*micros),
        (
            Plan::TimestampMillis,
            DbValue::Timestamp(TimestampValue::Instant { micros, .. }),
        ) if micros % 1000 == 0 => out.timestamp_millis(micros / 1000),
        (
            Plan::TimestampMicros | Plan::TimestampMillis,
            DbValue::Timestamp(_),
        ) => {
            return Err(fail(
                "a MySQL zero timestamp, an infinity, or finer than the unit",
            ));
        }
        (
            Plan::LocalTimestampMicros,
            DbValue::DateTime(DateTimeValue::Civil {
                date,
                micros_of_day,
                ..
            }),
        ) => out.local_timestamp_micros(
            date.days_since_epoch()
                .and_then(|d| d.checked_mul(MICROS_PER_DAY))
                .and_then(|d| d.checked_add(*micros_of_day))
                .ok_or_else(|| fail("a MySQL zero date-time"))?,
        ),
        (
            Plan::LocalTimestampMicros | Plan::LocalTimestampMillis,
            DbValue::DateTime(_),
        ) => {
            return Err(fail(
                "a MySQL zero date-time, an infinity, or finer than the unit",
            ));
        }
        (Plan::Array(item), DbValue::Array(arr)) => {
            let item_cx = cx.item();
            out.array_begin(arr.items().len());
            for i in arr.items() {
                convert_value(i, item, &item_cx, out)?;
            }
            out.array_end()
        }
        _ => {
            return Err(fail(
                "the value's type does not match the field's type",
            ));
        }
    }
    Ok(())
}

/// The exact text of a value for a `string` field (DECIMAL, BIGINT UNSIGNED
/// in string mode, ENUM and SET labels, JSON text, temporal text in string
/// mode); `None` for values whose text form is not their typed content.
fn string_of(db: &DbValue) -> Option<Cow<'_, str>> {
    Some(match db {
        DbValue::Text(s) => Cow::Borrowed(s.as_str()),
        DbValue::Decimal(d) => Cow::Owned(decimal_text(d).to_string()),
        DbValue::Int { value, .. } => Cow::Owned(value.to_string()),
        DbValue::UInt { value, .. } => Cow::Owned(value.to_string()),
        DbValue::Enum(e) => Cow::Borrowed(e.label.as_str()),
        DbValue::Set(s) => Cow::Owned(s.members().join(",")),
        DbValue::Json(j) => Cow::Borrowed(j.as_str()),
        DbValue::Uuid(u) => Cow::Owned(uuid_text(u)),
        DbValue::Date(d) => Cow::Owned(date_text(d)),
        DbValue::Time(t) => Cow::Owned(time_text(t)),
        DbValue::DateTime(dt) => Cow::Owned(datetime_text(dt)),
        DbValue::Timestamp(ts) => Cow::Owned(timestamp_text(ts)),
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
