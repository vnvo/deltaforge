//! `Event` → Arrow `RecordBatch` encoder.
//!
//! Converts a batch of CDC events into a `RecordBatch` matching the envelope
//! schema produced by `deltaforge_core::encoding::arrow_schema`.
//!
//! Each user column converts its event's canonical value to the column's
//! Arrow type exactly (integers, floats, booleans, text, binary and
//! fixed-width binary, dates, times, durations, timestamps, Decimal128 and
//! lists), or fails: nothing is rounded, truncated or guessed from a value's
//! shape. A row column the schema does not declare is refused. The
//! `before_extensions` / `after_extensions` columns carry each image's
//! processor-added documents as one canonical JSON object.
//!
//! Every failure is a [`RowEncodeError`], which the pool's per-row DLQ
//! slow path recognizes (see `writer_pool::AppendOutcome`): a single bad row
//! does not fail the whole batch.

use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use arrow_array::builder::{
    BooleanBuilder, StringBuilder, TimestampMillisecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use deltaforge_core::Event;
use deltaforge_core::encoding::{ConversionFailure, EncodingError};

/// Encode a batch of events into an Arrow `RecordBatch` matching `schema`.
///
/// The encoder dispatches each schema field by name + DataType:
/// - `op` / `op_ts` / `source_*` / `event_id` / `schema_version` / `tx_id`:
///   filled from envelope fields.
/// - `before_<col>` / `after_<col>`: filled from `event.before[col]` /
///   `event.after[col]`, coerced to the column's Arrow type.
///
/// Every failure is a [`RowEncodeError`]: a property of the events, never of
/// the store.
pub fn events_to_record_batch(
    schema: &Arc<Schema>,
    events: &[Event],
) -> Result<RecordBatch> {
    record_batch(schema, events).context(RowEncodeError)
}

/// Context on an error caused by the events being encoded (a value its
/// column cannot hold exactly, an undeclared column, a render failure), as
/// opposed to a storage failure. The writer pool sends the rows that cause
/// it to the DLQ one by one instead of failing the batch.
#[derive(Debug, Clone, Copy)]
pub struct RowEncodeError;

impl std::fmt::Display for RowEncodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("encode rows")
    }
}

fn record_batch(schema: &Arc<Schema>, events: &[Event]) -> Result<RecordBatch> {
    check_rows_defined(schema, events)?;
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        let name = field.name();
        let dt = field.data_type();

        let array = if name == "before_extensions" {
            build_extensions_column(events, false)
        } else if name == "after_extensions" {
            build_extensions_column(events, true)
        } else if let Some(col) = name.strip_prefix("before_") {
            build_user_column(events, field, col, /* after */ false)?
        } else if let Some(col) = name.strip_prefix("after_") {
            build_user_column(events, field, col, /* after */ true)?
        } else {
            build_meta_column(events, name, dt)?
        };

        columns.push(array);
    }

    RecordBatch::try_new(schema.clone(), columns).context("build RecordBatch")
}

// =============================================================================
// Envelope meta columns
// =============================================================================

fn build_meta_column(
    events: &[Event],
    name: &str,
    dt: &DataType,
) -> Result<ArrayRef> {
    match (name, dt) {
        ("op", DataType::Utf8) => {
            let mut b = StringBuilder::new();
            for e in events {
                b.append_value(op_str(&e.op));
            }
            Ok(Arc::new(b.finish()))
        }
        ("op_ts", DataType::Timestamp(TimeUnit::Millisecond, _)) => {
            let mut b = TimestampMillisecondBuilder::new()
                .with_timezone_opt(Some("UTC"));
            for e in events {
                b.append_value(e.ts_ms);
            }
            Ok(Arc::new(b.finish()))
        }
        ("source_db", DataType::Utf8) => {
            let mut b = StringBuilder::new();
            for e in events {
                if e.source.db.is_empty() {
                    b.append_null();
                } else {
                    b.append_value(&e.source.db);
                }
            }
            Ok(Arc::new(b.finish()))
        }
        ("source_schema", DataType::Utf8) => {
            let mut b = StringBuilder::new();
            for e in events {
                match &e.source.schema {
                    Some(s) => b.append_value(s),
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        ("source_table", DataType::Utf8) => {
            let mut b = StringBuilder::new();
            for e in events {
                b.append_value(&e.source.table);
            }
            Ok(Arc::new(b.finish()))
        }
        ("source_position", DataType::Utf8) => {
            let mut b = StringBuilder::new();
            for e in events {
                let pos = format_source_position(e);
                if pos.is_empty() {
                    b.append_null();
                } else {
                    b.append_value(&pos);
                }
            }
            Ok(Arc::new(b.finish()))
        }
        ("source_snapshot", DataType::Boolean) => {
            let mut b = BooleanBuilder::new();
            for e in events {
                match &e.source.snapshot {
                    Some(s) => {
                        // Debezium uses "true"/"false"/"last"/"first"/"incremental".
                        // Anything that is not "false" or empty counts as snapshot.
                        let v = !matches!(s.as_str(), "false" | "");
                        b.append_value(v);
                    }
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        ("event_id", DataType::Utf8) => {
            let mut b = StringBuilder::new();
            for e in events {
                match &e.event_id {
                    Some(id) => b.append_value(id.to_string()),
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        ("schema_version", DataType::Utf8) => {
            let mut b = StringBuilder::new();
            for e in events {
                match &e.schema_version {
                    Some(v) => b.append_value(v),
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        ("tx_id", DataType::Utf8) => {
            let mut b = StringBuilder::new();
            for e in events {
                match &e.transaction {
                    Some(t) => b.append_value(&t.id),
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        (other, dt) => {
            bail!("unsupported meta column {other} with type {dt:?}")
        }
    }
}

fn op_str(op: &deltaforge_core::Op) -> &'static str {
    use deltaforge_core::Op::*;
    match op {
        Create => "c",
        Update => "u",
        Delete => "d",
        Read => "r",
        Truncate => "t",
    }
}

fn format_source_position(e: &Event) -> String {
    let p = &e.source.position;
    if let Some(lsn) = &p.lsn {
        return format!("lsn:{lsn}");
    }
    if let (Some(file), Some(pos)) = (&p.file, &p.pos) {
        return format!("binlog:file={file},pos={pos}");
    }
    if let Some(seq) = &p.sequence {
        return format!("seq:{seq}");
    }
    String::new()
}

// =============================================================================
// User-data columns (before_X / after_X), from canonical values
// =============================================================================

use arrow_array::builder::{ArrayBuilder, make_builder};
use deltaforge_core::canonical::render::{
    date_text, datetime_text, decimal_text, time_text, timestamp_text,
    uuid_text,
};
use deltaforge_core::canonical::{
    CanonicalRow, CanonicalValue, DateTimeValue, DateValue, DbValue,
    MICROS_PER_DAY, TimestampValue,
};

/// A column value as its Arrow type, exactly, or an error naming the
/// column, the target type and why (never the value).
fn build_user_column(
    events: &[Event],
    field: &Field,
    col_name: &str,
    after: bool,
) -> Result<ArrayRef> {
    let dt = field.data_type();
    let mut b = make_builder(dt, events.len());
    let image = if after { "after" } else { "before" };
    for e in events {
        let row = if after { &e.after } else { &e.before };
        let v = row
            .as_ref()
            .and_then(|r| r.get(col_name))
            .unwrap_or(&CanonicalValue::Null);
        append(b.as_mut(), dt, v).map_err(|e| {
            match e.downcast::<Refused>() {
                Ok(Refused(reason)) => conversion(
                    format!("{image}.{col_name}"),
                    source_type_of(field),
                    format!("Arrow {dt}"),
                    reason,
                ),
                Err(e) => {
                    e.context(format!("{image}.{col_name} as Arrow {dt}"))
                }
            }
        })?;
    }
    Ok(b.finish())
}

/// A row image's extensions as a canonical JSON object (null when it has
/// none).
fn build_extensions_column(events: &[Event], after: bool) -> ArrayRef {
    let mut b = StringBuilder::new();
    for e in events {
        let row = if after { &e.after } else { &e.before };
        match row.as_ref().filter(|r| !r.extensions().is_empty()) {
            None => b.append_null(),
            Some(r) => {
                let obj = serde_json::Value::Object(
                    r.extensions()
                        .iter()
                        .map(|(n, d)| (n.clone(), d.value().clone()))
                        .collect(),
                );
                let bytes = deltaforge_core::canonical_json_bytes(&obj);
                b.append_value(
                    String::from_utf8(bytes).expect("canonical JSON is UTF-8"),
                );
            }
        }
    }
    Arc::new(b.finish())
}

/// Every column of `events`' row images is a column of `schema`: a column
/// the schema version does not define is refused, never dropped.
pub(crate) fn check_rows_defined(
    schema: &Schema,
    events: &[Event],
) -> Result<()> {
    let defined = |image: &str, row: &CanonicalRow| -> Result<()> {
        for name in row.names() {
            if schema.field_with_name(&format!("{image}_{name}")).is_err() {
                return Err(conversion(
                    format!("{image}.{name}"),
                    "a column".to_string(),
                    "the Arrow schema of its version".to_string(),
                    "a column the event's schema version does not define \
                     (never dropped)",
                ));
            }
        }
        Ok(())
    };
    for e in events {
        if let Some(r) = &e.before {
            defined("before", r)?;
        }
        if let Some(r) = &e.after {
            defined("after", r)?;
        }
    }
    Ok(())
}

fn downcast<T: 'static>(b: &mut dyn ArrayBuilder) -> Result<&mut T> {
    b.as_any_mut()
        .downcast_mut::<T>()
        .ok_or_else(|| anyhow!("builder type mismatch"))
}

/// The exact text of a value for a `Utf8` column.
fn text_of(v: &CanonicalValue) -> Option<String> {
    Some(match v {
        CanonicalValue::Opaque(o) => o.text.clone(),
        CanonicalValue::Db(db) => match db {
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
        },
        _ => return None,
    })
}

/// Why a value cannot be appended to its column (never the value).
#[derive(Debug)]
struct Refused(&'static str);

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for Refused {}

/// The source type a column's field metadata records, if any.
fn source_type_of(field: &Field) -> String {
    field
        .metadata()
        .get("deltaforge.source_type")
        .cloned()
        .unwrap_or_else(|| "unknown".to_string())
}

/// A value its column cannot hold as a conversion failure (column, source
/// and target type, the reason; never the value), like every typed sink's.
fn conversion(
    column: String,
    source_type: String,
    target_type: String,
    reason: &'static str,
) -> anyhow::Error {
    anyhow::Error::new(EncodingError::Conversion(ConversionFailure {
        column,
        source_type,
        target_type,
        reason,
    }))
}

/// Append one value to `b`, a builder of `dt`.
fn append(
    b: &mut dyn ArrayBuilder,
    dt: &DataType,
    v: &CanonicalValue,
) -> Result<()> {
    use arrow_array::builder::*;
    let db = match v {
        CanonicalValue::Unchanged => {
            return Err(Refused(
                "PostgreSQL did not send the value (unchanged TOAST)",
            )
            .into());
        }
        CanonicalValue::Document(_) => {
            return Err(Refused(
                "an untyped document in a schema-typed column",
            )
            .into());
        }
        CanonicalValue::Bytes(bytes) => {
            // Bytes encode as binary, exactly; never as another type.
            match dt {
                DataType::Binary => {
                    downcast::<BinaryBuilder>(b)?.append_value(bytes)
                }
                DataType::FixedSizeBinary(n) if bytes.len() == *n as usize => {
                    downcast::<FixedSizeBinaryBuilder>(b)?
                        .append_value(bytes)?
                }
                _ => return Err(Refused(
                    "bytes for a non-binary column (or of another fixed width)",
                )
                .into()),
            }
            return Ok(());
        }
        _ => match v {
            CanonicalValue::Db(db) => Some(db),
            _ => None,
        },
    };
    let null = v.is_null();
    macro_rules! int {
        ($B:ty, $T:ty) => {{
            let b = downcast::<$B>(b)?;
            if null {
                b.append_null();
            } else {
                let n: i128 = match db {
                    Some(DbValue::Int { value, .. }) => *value as i128,
                    Some(DbValue::UInt { value, .. }) => *value as i128,
                    Some(DbValue::Year(y)) => *y as i128,
                    _ => return Err(Refused("not an integer").into()),
                };
                b.append_value(
                    <$T>::try_from(n).map_err(|_| Refused("out of range"))?,
                );
            }
        }};
    }
    match dt {
        DataType::Int8 => int!(Int8Builder, i8),
        DataType::Int16 => int!(Int16Builder, i16),
        DataType::Int32 => int!(Int32Builder, i32),
        DataType::Int64 => int!(Int64Builder, i64),
        DataType::UInt8 => int!(UInt8Builder, u8),
        DataType::UInt16 => int!(UInt16Builder, u16),
        DataType::UInt32 => int!(UInt32Builder, u32),
        DataType::UInt64 => int!(UInt64Builder, u64),
        DataType::Float32 => {
            let b = downcast::<Float32Builder>(b)?;
            match db {
                None => b.append_null(),
                Some(DbValue::Float32(f)) => b.append_value(*f),
                Some(_) => return Err(Refused("not a 32-bit float").into()),
            }
        }
        DataType::Float64 => {
            let b = downcast::<Float64Builder>(b)?;
            match db {
                None => b.append_null(),
                Some(DbValue::Float64(f)) => b.append_value(*f),
                Some(DbValue::Float32(f)) => b.append_value(*f as f64),
                Some(_) => return Err(Refused("not a float").into()),
            }
        }
        DataType::Boolean => {
            let b = downcast::<BooleanBuilder>(b)?;
            match db {
                None => b.append_null(),
                Some(DbValue::Bool(x)) => b.append_value(*x),
                Some(DbValue::Bits(x)) if x.len() == 1 => {
                    b.append_value(x.to_u64() == 1)
                }
                Some(_) => return Err(Refused("not a boolean").into()),
            }
        }
        DataType::Utf8 => {
            let b = downcast::<StringBuilder>(b)?;
            if null {
                b.append_null();
            } else {
                b.append_value(
                    text_of(v)
                        .ok_or(Refused("no exact text for the value's type"))?,
                );
            }
        }
        DataType::Binary => {
            let b = downcast::<BinaryBuilder>(b)?;
            match db {
                None => b.append_null(),
                Some(DbValue::Binary(x)) => b.append_value(x),
                Some(DbValue::Bits(x)) => b.append_value(x.bytes()),
                Some(_) => return Err(Refused("not bytes").into()),
            }
        }
        DataType::FixedSizeBinary(n) => {
            let b = downcast::<FixedSizeBinaryBuilder>(b)?;
            let bytes: Option<Vec<u8>> = match db {
                None => None,
                Some(DbValue::Bits(x)) => Some(x.bytes().to_vec()),
                Some(DbValue::Binary(x)) => Some(x.clone()),
                Some(DbValue::Uuid(u)) => Some(u.to_vec()),
                Some(_) => return Err(Refused("not fixed-width bytes").into()),
            };
            match bytes {
                None => b.append_null(),
                Some(x) if x.len() == *n as usize => b.append_value(&x)?,
                Some(_) => {
                    return Err(Refused(
                        "byte length differs from the fixed size",
                    )
                    .into());
                }
            }
        }
        DataType::Date32 => {
            let b = downcast::<Date32Builder>(b)?;
            match db {
                None => b.append_null(),
                Some(DbValue::Date(DateValue::Civil(d))) => b.append_value(
                    d.days_since_epoch()
                        .and_then(|x| i32::try_from(x).ok())
                        .ok_or(Refused("a MySQL zero date"))?,
                ),
                Some(_) => {
                    return Err(
                        Refused("not a calendar date (an infinity)").into()
                    );
                }
            }
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            let b = downcast::<DurationMicrosecondBuilder>(b)?;
            match db {
                None => b.append_null(),
                Some(DbValue::Time(t)) => b.append_value(t.micros()),
                Some(_) => return Err(Refused("not a duration").into()),
            }
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let b = downcast::<Time64MicrosecondBuilder>(b)?;
            match db {
                None => b.append_null(),
                Some(DbValue::Time(t))
                    if (0..MICROS_PER_DAY).contains(&t.micros()) =>
                {
                    b.append_value(t.micros())
                }
                Some(_) => {
                    return Err(
                        Refused("not a time of day below 24:00:00").into()
                    );
                }
            }
        }
        DataType::Timestamp(unit, tz) => {
            let micros = match db {
                None => None,
                Some(DbValue::Timestamp(TimestampValue::Instant {
                    micros,
                    ..
                })) if tz.is_some() => Some(*micros),
                Some(DbValue::DateTime(DateTimeValue::Civil {
                    date,
                    micros_of_day,
                    ..
                })) if tz.is_none() => Some(
                    date.days_since_epoch()
                        .and_then(|d| d.checked_mul(MICROS_PER_DAY))
                        .and_then(|d| d.checked_add(*micros_of_day))
                        .ok_or(Refused("a MySQL zero date-time"))?,
                ),
                Some(_) => return Err(Refused("not an instant (zone) or civil date-time (no zone) of this column, or a MySQL zero value or an infinity").into()),
            };
            match unit {
                TimeUnit::Microsecond => {
                    let b = downcast::<TimestampMicrosecondBuilder>(b)?;
                    match micros {
                        None => b.append_null(),
                        Some(us) => b.append_value(us),
                    }
                }
                TimeUnit::Millisecond => {
                    let b = downcast::<TimestampMillisecondBuilder>(b)?;
                    match micros {
                        None => b.append_null(),
                        Some(us) if us % 1000 == 0 => b.append_value(us / 1000),
                        Some(_) => {
                            return Err(
                                Refused("finer than milliseconds").into()
                            );
                        }
                    }
                }
                _ => {
                    return Err(Refused("an unsupported timestamp unit").into());
                }
            }
        }
        DataType::Decimal128(p, s) => {
            let b = downcast::<Decimal128Builder>(b)?;
            match db {
                None => b.append_null(),
                Some(DbValue::Decimal(
                    deltaforge_core::canonical::Decimal::Finite(t),
                )) => b.append_value(decimal_unscaled(t, *p, *s).ok_or(
                    Refused("more digits than the precision or scale keeps"),
                )?),
                Some(_) => return Err(Refused("not a finite decimal").into()),
            }
        }
        DataType::List(item) => {
            let b = downcast::<ListBuilder<Box<dyn ArrayBuilder>>>(b)?;
            match db {
                None => b.append_null(),
                Some(DbValue::Array(a)) => {
                    for i in a.items() {
                        append(b.values().as_mut(), item.data_type(), i)?;
                    }
                    b.append(true);
                }
                Some(_) => return Err(Refused("not an array").into()),
            }
        }
        _ => return Err(Refused("an unsupported Arrow type").into()),
    }
    Ok(())
}

/// Canonical decimal text as the unscaled integer at `scale`, or `None`
/// when it has more fraction digits than `scale` or more digits than
/// `precision` (never truncated or rounded).
fn decimal_unscaled(text: &str, precision: u8, scale: i8) -> Option<i128> {
    let scale = usize::try_from(scale).ok()?;
    let neg = text.starts_with('-');
    let t = text.trim_start_matches('-');
    let (int_part, frac) = t.split_once('.').unwrap_or((t, ""));
    let frac = frac.trim_end_matches('0');
    let int_part = int_part.trim_start_matches('0');
    if frac.len() > scale || int_part.len() + scale > precision as usize {
        return None;
    }
    let digits = format!("{int_part}{frac:0<scale$}");
    let n: i128 = if digits.is_empty() {
        0
    } else {
        digits.parse().ok()?
    };
    Some(if neg { -n } else { n })
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array,
        FixedSizeBinaryArray, Float64Array, Int32Array, Int64Array,
        StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    };
    use arrow_schema::{Field, Schema};
    use deltaforge_core::canonical::{
        ArrayValue, Bits, CivilDate, Decimal, Document, Engine, IntWidth,
        JsonText, OpaqueText, Precision, SourceType, fixture,
    };
    use deltaforge_core::encoding::arrow_schema::{
        Connector, build_envelope_arrow_schema,
    };
    use deltaforge_core::encoding::avro_types::{
        ColumnDesc, TypeConversionOpts,
    };
    use deltaforge_core::{
        Op, SourceInfo, SourcePosition, Transaction, TypeMapping,
    };
    use serde_json::{Value, json};

    fn r(v: Value) -> Option<CanonicalRow> {
        Some(fixture::row(v))
    }

    /// A one-column row holding `v`.
    fn r1(name: &str, v: CanonicalValue) -> Option<CanonicalRow> {
        Some(CanonicalRow::new(vec![(name.into(), v)]).unwrap())
    }

    fn db(v: DbValue) -> CanonicalValue {
        CanonicalValue::Db(v)
    }

    fn make_event(
        op: Op,
        table: &str,
        ts_ms: i64,
        before: Option<CanonicalRow>,
        after: Option<CanonicalRow>,
    ) -> Event {
        Event {
            before,
            after,
            source: SourceInfo {
                version: "1".into(),
                connector: "mysql".into(),
                name: "test".into(),
                ts_ms,
                db: "shop".into(),
                schema: None,
                table: table.into(),
                snapshot: Some("false".into()),
                position: SourcePosition {
                    file: Some("mysql-bin.000123".into()),
                    pos: Some(4567),
                    ..Default::default()
                },
            },
            op,
            ts_ms,
            transaction: Some(Transaction {
                id: "tx-42".into(),
                total_order: None,
                data_collection_order: None,
            }),
            type_mapping: TypeMapping::V2,
            event_id: Some(deltaforge_core::EventId::mysql_row_server(
                1, "t", 1, 0,
            )),
            tenant_id: None,
            schema_version: Some("v1".into()),
            schema_sequence: None,
            ddl: None,
            trace_id: None,
            tags: None,
            synthetic: None,
            routing: None,
            tx_end: false,
            boundary: None,
            size_bytes: 0,

            received_at_ms: ts_ms,
        }
    }

    fn create(after: Option<CanonicalRow>) -> Event {
        make_event(Op::Create, "t", 0, None, after)
    }

    fn col(name: &str, data_type: &str, column_type: &str) -> ColumnDesc {
        ColumnDesc {
            name: name.into(),
            data_type: data_type.into(),
            column_type: column_type.into(),
            nullable: true,
            precision: None,
            scale: None,
            unsigned: false,
            is_array: false,
            element_type: None,
        }
    }

    fn mysql_schema(cols: &[ColumnDesc]) -> Arc<Schema> {
        Arc::new(
            build_envelope_arrow_schema(
                Connector::Mysql,
                cols,
                &TypeConversionOpts::default(),
            )
            .unwrap(),
        )
    }

    fn pg_schema(cols: &[ColumnDesc]) -> Arc<Schema> {
        Arc::new(
            build_envelope_arrow_schema(
                Connector::Postgres,
                cols,
                &TypeConversionOpts::default(),
            )
            .unwrap(),
        )
    }

    fn column<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> &'a T {
        batch
            .column_by_name(name)
            .unwrap_or_else(|| panic!("no column {name}"))
            .as_any()
            .downcast_ref::<T>()
            .unwrap_or_else(|| panic!("column {name} has another type"))
    }

    fn err_of(schema: &Arc<Schema>, events: &[Event]) -> String {
        format!(
            "{:#}",
            events_to_record_batch(schema, events)
                .expect_err("encoding must fail")
        )
    }

    #[test]
    fn encodes_envelope_meta_columns() {
        let schema = mysql_schema(&[col("id", "bigint", "bigint")]);
        let events = vec![
            make_event(Op::Create, "orders", 1_000, None, r(json!({"id": 1}))),
            make_event(
                Op::Update,
                "orders",
                2_000,
                r(json!({"id": 1})),
                r(json!({"id": 2})),
            ),
        ];
        let batch = events_to_record_batch(&schema, &events).unwrap();
        assert_eq!(batch.num_rows(), 2);

        let op = column::<StringArray>(&batch, "op");
        assert_eq!(op.value(0), "c");
        assert_eq!(op.value(1), "u");

        let ts = column::<TimestampMillisecondArray>(&batch, "op_ts");
        assert_eq!(ts.value(0), 1_000);
        assert_eq!(ts.value(1), 2_000);

        assert_eq!(
            column::<StringArray>(&batch, "source_table").value(0),
            "orders"
        );
        assert_eq!(
            column::<StringArray>(&batch, "source_position").value(0),
            "binlog:file=mysql-bin.000123,pos=4567"
        );
        assert_eq!(column::<StringArray>(&batch, "tx_id").value(0), "tx-42");
    }

    #[test]
    fn encodes_int32_and_int64_before_after() {
        let schema = mysql_schema(&[
            col("age", "int", "int"),
            col("id", "bigint", "bigint"),
        ]);
        let events = vec![make_event(
            Op::Update,
            "users",
            0,
            r(json!({"age": 30, "id": 100})),
            r(json!({"age": 31, "id": i64::MAX})),
        )];
        let batch = events_to_record_batch(&schema, &events).unwrap();
        assert_eq!(column::<Int32Array>(&batch, "before_age").value(0), 30);
        assert_eq!(column::<Int32Array>(&batch, "after_age").value(0), 31);
        assert_eq!(column::<Int64Array>(&batch, "before_id").value(0), 100);
        assert_eq!(
            column::<Int64Array>(&batch, "after_id").value(0),
            i64::MAX,
            "64-bit integers are exact"
        );
    }

    #[test]
    fn integer_out_of_the_column_range_fails_instead_of_wrapping() {
        let schema = mysql_schema(&[col("age", "int", "int")]);
        let msg = err_of(
            &schema,
            &[create(r1(
                "age",
                db(DbValue::int(i64::from(i32::MAX) + 1, IntWidth::W64)
                    .unwrap()),
            ))],
        );
        assert!(msg.contains("after.age"), "got: {msg}");
        assert!(msg.contains("out of range"), "got: {msg}");
    }

    #[test]
    fn delete_event_has_null_after_columns() {
        let schema = mysql_schema(&[col("id", "bigint", "bigint")]);
        let events = vec![make_event(
            Op::Delete,
            "users",
            0,
            r(json!({"id": 42})),
            None,
        )];
        let batch = events_to_record_batch(&schema, &events).unwrap();
        assert!(batch.column_by_name("after_id").unwrap().is_null(0));
        assert_eq!(column::<Int64Array>(&batch, "before_id").value(0), 42);
    }

    #[test]
    fn boolean_accepts_bool_and_bit1_but_not_integers() {
        let schema = mysql_schema(&[col("active", "boolean", "boolean")]);
        let events = vec![
            create(r(json!({"active": true}))),
            create(r(json!({"active": false}))),
            create(r1(
                "active",
                db(DbValue::Bits(Bits::from_u64(1, 1).unwrap())),
            )),
        ];
        let batch = events_to_record_batch(&schema, &events).unwrap();
        let arr = column::<BooleanArray>(&batch, "after_active");
        assert!(arr.value(0));
        assert!(!arr.value(1));
        assert!(arr.value(2));

        // An integer is not guessed to be a boolean.
        let msg = err_of(&schema, &[create(r(json!({"active": 1})))]);
        assert!(msg.contains("not a boolean"), "got: {msg}");
    }

    #[test]
    fn float64_encodes() {
        let schema = mysql_schema(&[col("price", "double", "double")]);
        let events = vec![create(r(json!({"price": 3.15159})))];
        let batch = events_to_record_batch(&schema, &events).unwrap();
        let arr = column::<Float64Array>(&batch, "after_price");
        assert!((arr.value(0) - 3.15159).abs() < 1e-9);
    }

    #[test]
    fn binary_values_encode_exactly() {
        let schema = mysql_schema(&[col("payload", "blob", "blob")]);
        let events = vec![
            create(r1("payload", db(DbValue::Binary(b"hello".to_vec())))),
            // Untyped bytes (no column type) encode into a Binary column.
            create(r1("payload", CanonicalValue::Bytes(vec![0, 1, 0xff]))),
            create(r1("payload", CanonicalValue::Null)),
        ];
        let batch = events_to_record_batch(&schema, &events).unwrap();
        let arr = column::<BinaryArray>(&batch, "after_payload");
        assert_eq!(arr.value(0), b"hello");
        assert_eq!(arr.value(1), &[0, 1, 0xff]);
        assert!(arr.is_null(2));
    }

    #[test]
    fn base64_shaped_documents_are_not_guessed_to_be_bytes() {
        let schema = mysql_schema(&[col("payload", "blob", "blob")]);
        let msg = err_of(
            &schema,
            &[create(r(json!({"payload": {"_base64": "aGVsbG8="}})))],
        );
        assert!(msg.contains("untyped document"), "got: {msg}");
    }

    #[test]
    fn bytes_never_encode_as_another_type() {
        let schema = mysql_schema(&[col("name", "varchar", "varchar(10)")]);
        let msg = err_of(
            &schema,
            &[create(r1("name", CanonicalValue::Bytes(b"x".to_vec())))],
        );
        assert!(msg.contains("non-binary column"), "got: {msg}");
    }

    #[test]
    fn bit_n_encodes_as_fixed_size_binary() {
        let mut c = col("flags", "bit", "bit(12)");
        c.precision = Some(12);
        let schema = mysql_schema(&[c]);
        let events = vec![create(r1(
            "flags",
            db(DbValue::Bits(Bits::from_u64(12, 0xabc).unwrap())),
        ))];
        let batch = events_to_record_batch(&schema, &events).unwrap();
        let arr = column::<FixedSizeBinaryArray>(&batch, "after_flags");
        assert_eq!(arr.value(0), &[0x0a, 0xbc]);
    }

    #[test]
    fn date32_encodes_civil_dates() {
        let schema = mysql_schema(&[col("dob", "date", "date")]);
        let date = |y, m, d| {
            db(DbValue::Date(DateValue::Civil(
                CivilDate::new(y, m, d).unwrap(),
            )))
        };
        let events = vec![
            create(r1("dob", date(2000, 1, 15))),
            create(r1("dob", date(1969, 12, 31))),
        ];
        let batch = events_to_record_batch(&schema, &events).unwrap();
        let arr = column::<Date32Array>(&batch, "after_dob");
        // Days from 1970-01-01 to 2000-01-15 = 10_971
        assert_eq!(arr.value(0), 10_971);
        assert_eq!(arr.value(1), -1);

        // Text is not parsed into a date.
        let msg = err_of(&schema, &[create(r(json!({"dob": "2000-01-15"})))]);
        assert!(msg.contains("after.dob"), "got: {msg}");
    }

    #[test]
    fn mysql_zero_date_fails() {
        let schema = mysql_schema(&[col("dob", "date", "date")]);
        let zero = db(DbValue::Date(DateValue::Civil(
            CivilDate::mysql(0, 0, 0).unwrap(),
        )));
        let msg = err_of(&schema, &[create(r1("dob", zero))]);
        assert!(msg.contains("zero date"), "got: {msg}");
    }

    #[test]
    fn timestamp_encodes_instants_in_microseconds() {
        let schema =
            mysql_schema(&[col("created_at", "timestamp", "timestamp")]);
        let us = chrono::NaiveDate::from_ymd_opt(2026, 5, 19)
            .unwrap()
            .and_hms_micro_opt(12, 0, 0, 123_456)
            .unwrap()
            .and_utc()
            .timestamp_micros();
        let ts = db(DbValue::Timestamp(
            TimestampValue::instant(us, Precision::Fixed(6)).unwrap(),
        ));
        let batch =
            events_to_record_batch(&schema, &[create(r1("created_at", ts))])
                .unwrap();
        let arr =
            column::<TimestampMicrosecondArray>(&batch, "after_created_at");
        assert_eq!(arr.value(0), us);

        // Neither ISO text nor a bare number is parsed into an instant.
        for v in [json!("2026-05-19T12:00:00Z"), json!(1_779_192_000_000i64)] {
            let msg = err_of(&schema, &[create(r(json!({"created_at": v})))]);
            assert!(msg.contains("after.created_at"), "got: {msg}");
        }
    }

    #[test]
    fn millisecond_timestamp_column_refuses_sub_millisecond_values() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "after_ts",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true,
        )]));
        let ts = |us| {
            db(DbValue::Timestamp(
                TimestampValue::instant(us, Precision::Fixed(6)).unwrap(),
            ))
        };
        let batch =
            events_to_record_batch(&schema, &[create(r1("ts", ts(5_000)))])
                .unwrap();
        assert_eq!(
            column::<TimestampMillisecondArray>(&batch, "after_ts").value(0),
            5
        );
        let msg = err_of(&schema, &[create(r1("ts", ts(5_001)))]);
        assert!(msg.contains("finer than milliseconds"), "got: {msg}");
    }

    fn decimal(text: &str) -> CanonicalValue {
        db(DbValue::Decimal(Decimal::parse(text).unwrap()))
    }

    fn decimal_col(name: &str, p: i64, s: i64) -> ColumnDesc {
        let mut c = col(name, "decimal", &format!("decimal({p},{s})"));
        c.precision = Some(p);
        c.scale = Some(s);
        c
    }

    #[test]
    fn decimal128_encodes_exact_decimals() {
        let schema = mysql_schema(&[decimal_col("price", 18, 4)]);
        let events = vec![
            create(r1("price", decimal("1234.5678"))),
            create(r1("price", decimal("-0.1"))),
            create(r1("price", decimal("0"))),
            // Trailing fraction zeros beyond the scale are not digits lost.
            create(r1("price", decimal("1.500000"))),
        ];
        let batch = events_to_record_batch(&schema, &events).unwrap();
        let arr = column::<Decimal128Array>(&batch, "after_price");
        // 1234.5678 with scale=4 → 12_345_678
        assert_eq!(arr.value(0), 12_345_678);
        assert_eq!(arr.value(1), -1000);
        assert_eq!(arr.value(2), 0);
        assert_eq!(arr.value(3), 15_000);
    }

    #[test]
    fn decimal128_refuses_numbers_that_are_not_decimals() {
        // An integer or a float is not coerced into a decimal column.
        let schema = mysql_schema(&[decimal_col("amount", 18, 4)]);
        for v in [json!(100), json!(12.5), json!("12.5")] {
            let msg = err_of(&schema, &[create(r(json!({"amount": v})))]);
            assert!(msg.contains("not a finite decimal"), "got: {msg}");
        }
    }

    #[test]
    fn decimal128_with_more_fraction_digits_than_scale_fails() {
        // scale=2 column receiving 1.999: never truncated or rounded.
        let schema = mysql_schema(&[decimal_col("p", 5, 2)]);
        let msg = err_of(&schema, &[create(r1("p", decimal("1.999")))]);
        assert!(msg.contains("after.p"), "got: {msg}");
        assert!(msg.contains("precision or scale"), "got: {msg}");
    }

    #[test]
    fn decimal128_with_more_integer_digits_than_precision_fails() {
        let schema = mysql_schema(&[decimal_col("p", 5, 2)]);
        let msg = err_of(&schema, &[create(r1("p", decimal("1234.5")))]);
        assert!(msg.contains("precision or scale"), "got: {msg}");
    }

    #[test]
    fn utf8_columns_hold_exact_text() {
        let mut big = col("big", "bigint", "bigint unsigned");
        big.unsigned = true;
        let schema = mysql_schema(&[col("doc", "json", "json"), big]);
        let row = CanonicalRow::new(vec![
            (
                "doc".into(),
                db(DbValue::Json(
                    JsonText::new(r#"{"b": 1.10, "a": [1]}"#.into()).unwrap(),
                )),
            ),
            (
                "big".into(),
                db(DbValue::uint(u64::MAX, IntWidth::W64).unwrap()),
            ),
        ])
        .unwrap();
        let batch =
            events_to_record_batch(&schema, &[create(Some(row))]).unwrap();
        // JSON text is kept as the source sent it.
        assert_eq!(
            column::<StringArray>(&batch, "after_doc").value(0),
            r#"{"b": 1.10, "a": [1]}"#
        );
        assert_eq!(
            column::<StringArray>(&batch, "after_big").value(0),
            u64::MAX.to_string()
        );
    }

    #[test]
    fn untyped_document_in_a_typed_column_fails() {
        let schema = mysql_schema(&[col("doc", "json", "json")]);
        let msg = err_of(&schema, &[create(r(json!({"doc": {"a": 1}})))]);
        assert!(msg.contains("untyped document"), "got: {msg}");
    }

    #[test]
    fn missing_after_column_is_null() {
        let schema = mysql_schema(&[
            col("id", "bigint", "bigint"),
            col("missing", "bigint", "bigint"),
        ]);
        // The event's row has no `missing` column.
        let batch =
            events_to_record_batch(&schema, &[create(r(json!({"id": 1})))])
                .unwrap();
        assert!(batch.column_by_name("after_missing").unwrap().is_null(0));
    }

    #[test]
    fn row_column_not_in_the_schema_fails() {
        let schema = mysql_schema(&[col("id", "bigint", "bigint")]);
        let msg = err_of(&schema, &[create(r(json!({"id": 1, "other": 1})))]);
        assert!(msg.contains("after.other"), "got: {msg}");
        assert!(msg.contains("never dropped"), "got: {msg}");

        let before = make_event(
            Op::Delete,
            "t",
            0,
            r(json!({"id": 1, "gone": "x"})),
            None,
        );
        let msg = err_of(&schema, &[before]);
        assert!(msg.contains("before.gone"), "got: {msg}");
    }

    #[test]
    fn extension_columns_hold_canonical_json_or_null() {
        let schema = mysql_schema(&[col("id", "bigint", "bigint")]);
        let with_ext = CanonicalRow::with_extensions(
            vec![("id".into(), fixture::value(&json!(1)))],
            [(
                "enrich".to_string(),
                Document::new(json!({"z": 1, "a": {"y": true, "b": null}})),
            )],
        )
        .unwrap();
        let events = vec![
            make_event(Op::Update, "t", 0, r(json!({"id": 1})), Some(with_ext)),
            create(r(json!({"id": 2}))),
        ];
        let batch = events_to_record_batch(&schema, &events).unwrap();
        let after = column::<StringArray>(&batch, "after_extensions");
        assert_eq!(
            after.value(0),
            r#"{"enrich":{"a":{"b":null,"y":true},"z":1}}"#,
            "sorted keys, compact"
        );
        assert!(after.is_null(1), "no extensions: null");
        let before = column::<StringArray>(&batch, "before_extensions");
        assert!(before.is_null(0));
        assert!(before.is_null(1), "no before image: null");
        // Extensions are never a row column.
        assert!(batch.column_by_name("after_enrich").is_none());
    }

    #[test]
    fn empty_event_batch_yields_empty_record_batch() {
        let schema = mysql_schema(&[col("id", "bigint", "bigint")]);
        let batch = events_to_record_batch(&schema, &[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), schema.fields().len());
    }

    #[test]
    fn schema_alone_is_sufficient_no_dependency_on_column_desc() {
        // Sanity: encoder takes only Schema + events, not ColumnDesc.
        // This is the contract for Phase 1d.
        let raw_schema = Schema::new(vec![
            Field::new("op", DataType::Utf8, false),
            Field::new(
                "op_ts",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                false,
            ),
            Field::new("source_db", DataType::Utf8, true),
            Field::new("source_schema", DataType::Utf8, true),
            Field::new("source_table", DataType::Utf8, false),
            Field::new("source_position", DataType::Utf8, true),
            Field::new("source_snapshot", DataType::Boolean, true),
            Field::new("event_id", DataType::Utf8, true),
            Field::new("schema_version", DataType::Utf8, true),
            Field::new("tx_id", DataType::Utf8, true),
            Field::new("after_id", DataType::Int64, true),
        ]);
        let schema = Arc::new(raw_schema);
        let events = vec![create(r(json!({"id": 99})))];
        let _batch = events_to_record_batch(&schema, &events).unwrap();
    }

    // -----------------------------------------------------------------------
    // List<T> columns (PostgreSQL arrays) and opaque values
    // -----------------------------------------------------------------------

    fn pg_array_col(name: &str, element_type: &str) -> ColumnDesc {
        ColumnDesc {
            name: name.into(),
            data_type: format!("_{element_type}"),
            column_type: format!("_{element_type}"),
            nullable: true,
            precision: None,
            scale: None,
            unsigned: false,
            is_array: true,
            element_type: Some(element_type.into()),
        }
    }

    fn pg_type(name: &str) -> SourceType {
        SourceType {
            engine: Engine::Postgres,
            name: name.into(),
            oid: None,
            typmod: None,
        }
    }

    fn array(element: &str, items: Vec<CanonicalValue>) -> CanonicalValue {
        db(DbValue::Array(
            ArrayValue::new(pg_type(element), items).unwrap(),
        ))
    }

    fn int(v: i64, w: IntWidth) -> CanonicalValue {
        db(DbValue::int(v, w).unwrap())
    }

    #[test]
    fn list_of_int32_encodes() {
        use arrow_array::ListArray;
        let schema = pg_schema(&[pg_array_col("ids", "integer")]);
        let w = IntWidth::W32;
        let events = vec![
            create(r1(
                "ids",
                array("int4", vec![int(1, w), int(2, w), int(3, w)]),
            )),
            create(r1("ids", array("int4", vec![]))),
            create(r1("ids", CanonicalValue::Null)),
        ];
        let batch = events_to_record_batch(&schema, &events).unwrap();
        let arr = column::<ListArray>(&batch, "after_ids");
        assert_eq!(arr.value(0).len(), 3);
        assert_eq!(arr.value(1).len(), 0);
        assert!(arr.is_null(2));
    }

    #[test]
    fn list_of_strings_encodes() {
        use arrow_array::ListArray;
        let schema = pg_schema(&[pg_array_col("tags", "text")]);
        let text = |s: &str| db(DbValue::Text(s.into()));
        let events = vec![create(r1(
            "tags",
            array("text", vec![text("red"), text("green"), text("blue")]),
        ))];
        let batch = events_to_record_batch(&schema, &events).unwrap();
        let arr = column::<ListArray>(&batch, "after_tags");
        let strs = arr
            .value(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|s| s.unwrap().to_string())
            .collect::<Vec<_>>();
        assert_eq!(strs, vec!["red", "green", "blue"]);
    }

    #[test]
    fn list_with_null_items_preserved() {
        use arrow_array::ListArray;
        let schema = pg_schema(&[pg_array_col("nums", "bigint")]);
        let w = IntWidth::W64;
        let n = CanonicalValue::Null;
        let events = vec![create(r1(
            "nums",
            array("int8", vec![int(1, w), n.clone(), int(3, w), n, int(5, w)]),
        ))];
        let batch = events_to_record_batch(&schema, &events).unwrap();
        let arr = column::<ListArray>(&batch, "after_nums");
        let items = arr.value(0);
        assert_eq!(items.len(), 5);
        assert!(items.is_null(1));
        assert!(items.is_null(3));
        assert!(items.is_valid(0));
    }

    #[test]
    fn untyped_json_array_is_not_guessed_to_be_a_list() {
        let schema = pg_schema(&[pg_array_col("ids", "integer")]);
        let msg = err_of(&schema, &[create(r(json!({"ids": [1, 2]})))]);
        assert!(msg.contains("untyped document"), "got: {msg}");
    }

    fn pg_hstore_col(name: &str) -> ColumnDesc {
        ColumnDesc {
            name: name.into(),
            data_type: "hstore".into(),
            column_type: "hstore".into(),
            nullable: true,
            precision: None,
            scale: None,
            unsigned: false,
            is_array: false,
            element_type: None,
        }
    }

    #[test]
    fn hstore_is_its_opaque_text() {
        let schema = pg_schema(&[pg_hstore_col("attrs")]);
        let text = r#""color"=>"red", "size"=>"large""#;
        let events = vec![
            create(r1(
                "attrs",
                CanonicalValue::Opaque(OpaqueText {
                    source_type: pg_type("hstore"),
                    text: text.into(),
                }),
            )),
            create(r1("attrs", CanonicalValue::Null)),
        ];
        let batch = events_to_record_batch(&schema, &events).unwrap();
        let arr = column::<StringArray>(&batch, "after_attrs");
        assert_eq!(arr.value(0), text);
        assert!(arr.is_null(1));
    }

    #[test]
    fn list_of_int_overflow_fails() {
        let schema = pg_schema(&[pg_array_col("ids", "integer")]);
        // i64::MAX is too large for the Int32 element type.
        let w = IntWidth::W64;
        let events = vec![create(r1(
            "ids",
            array("int8", vec![int(1, w), int(i64::MAX, w), int(3, w)]),
        ))];
        let msg = err_of(&schema, &events);
        assert!(msg.contains("out of range"), "got: {msg}");
    }
}
