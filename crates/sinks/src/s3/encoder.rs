//! `Event` → Arrow `RecordBatch` encoder.
//!
//! Converts a batch of CDC events into a `RecordBatch` matching the envelope
//! schema produced by `deltaforge_core::encoding::arrow_schema`.
//!
//! Coverage:
//! - All envelope meta columns (`op`, `op_ts`, `source_*`, `event_id`, ...)
//! - User-data scalars: Int32/Int64, Float32/Float64, Boolean, Utf8, Binary
//!   (via `{"_base64": "..."}` unwrap), Date32, Timestamp(ms/us, _),
//!   Decimal128(p, s) via string-decimal parsing
//! - List<T> with scalar element types (Int32, Int64, Float32, Float64,
//!   Boolean, Utf8, Binary). Nested lists fall back to JSON-string with a warn.
//! - Map<Utf8, Utf8> (PostgreSQL hstore semantics). Other key/value types
//!   fall back to JSON-string.
//!
//! Encoding errors are caught by the pool's per-row DLQ slow-path retry
//! (see `writer_pool::AppendOutcome`) — a single bad row no longer fails
//! the whole batch.

use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    Float32Builder, Float64Builder, Int32Builder, Int64Builder, StringBuilder,
    TimestampMicrosecondBuilder, TimestampMillisecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Schema, TimeUnit};
use base64::Engine;
use chrono::{DateTime, NaiveDate, Utc};
use deltaforge_core::Event;
use serde_json::Value;

/// Encode a batch of events into an Arrow `RecordBatch` matching `schema`.
///
/// The encoder dispatches each schema field by name + DataType:
/// - `op` / `op_ts` / `source_*` / `event_id` / `schema_version` / `tx_id`:
///   filled from envelope fields.
/// - `before_<col>` / `after_<col>`: filled from `event.before[col]` /
///   `event.after[col]`, coerced to the column's Arrow type.
pub fn events_to_record_batch(
    schema: &Arc<Schema>,
    events: &[Event],
) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        let name = field.name();
        let dt = field.data_type();

        let array = if let Some(col) = name.strip_prefix("before_") {
            build_user_column(events, dt, col, /* after */ false)?
        } else if let Some(col) = name.strip_prefix("after_") {
            build_user_column(events, dt, col, /* after */ true)?
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
// User-data columns (before_X / after_X)
// =============================================================================

fn build_user_column(
    events: &[Event],
    dt: &DataType,
    col_name: &str,
    after: bool,
) -> Result<ArrayRef> {
    match dt {
        DataType::Int32 => build_int32(events, col_name, after),
        DataType::Int64 => build_int64(events, col_name, after),
        DataType::Float32 => build_float32(events, col_name, after),
        DataType::Float64 => build_float64(events, col_name, after),
        DataType::Boolean => build_bool(events, col_name, after),
        DataType::Utf8 => build_utf8(events, col_name, after),
        DataType::Binary => build_binary(events, col_name, after),
        DataType::Date32 => build_date32(events, col_name, after),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            build_ts(events, col_name, after, TimeUnit::Millisecond)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            build_ts(events, col_name, after, TimeUnit::Microsecond)
        }
        DataType::Decimal128(p, s) => {
            build_decimal128(events, col_name, after, *p, *s)
        }
        DataType::List(item_field) => {
            build_list(events, col_name, after, item_field)
        }
        DataType::Map(entries_field, _sorted) => {
            build_map(events, col_name, after, entries_field)
        }
        other => {
            bail!(
                "unsupported Arrow data type for column {col_name}: {other:?}"
            );
        }
    }
}

/// Look up a column value from an event's `before` or `after` JSON object.
fn lookup<'a>(e: &'a Event, col: &str, after: bool) -> Option<&'a Value> {
    let obj = if after { &e.after } else { &e.before };
    obj.as_ref()
        .and_then(|v| v.as_object())
        .and_then(|m| m.get(col))
}

fn build_int32(events: &[Event], col: &str, after: bool) -> Result<ArrayRef> {
    let mut b = Int32Builder::new();
    for e in events {
        match lookup(e, col, after) {
            None | Some(Value::Null) => b.append_null(),
            Some(Value::Number(n)) => {
                let v = n
                    .as_i64()
                    .ok_or_else(|| anyhow!("{col}: number not i64"))?;
                let v: i32 = v
                    .try_into()
                    .map_err(|_| anyhow!("{col}: value {v} overflows i32"))?;
                b.append_value(v);
            }
            Some(v) => bail!("{col}: expected number, got {v:?}"),
        }
    }
    Ok(Arc::new(b.finish()))
}

fn build_int64(events: &[Event], col: &str, after: bool) -> Result<ArrayRef> {
    let mut b = Int64Builder::new();
    for e in events {
        match lookup(e, col, after) {
            None | Some(Value::Null) => b.append_null(),
            Some(Value::Number(n)) => {
                let v = n
                    .as_i64()
                    .ok_or_else(|| anyhow!("{col}: number not i64"))?;
                b.append_value(v);
            }
            // Accept stringified integers (e.g. BIGINT UNSIGNED encoded as
            // string in JSON intermediary).
            Some(Value::String(s)) => {
                let v: i64 = s
                    .parse()
                    .with_context(|| format!("{col}: parse {s} as i64"))?;
                b.append_value(v);
            }
            Some(v) => bail!("{col}: expected number, got {v:?}"),
        }
    }
    Ok(Arc::new(b.finish()))
}

fn build_float32(events: &[Event], col: &str, after: bool) -> Result<ArrayRef> {
    let mut b = Float32Builder::new();
    for e in events {
        match lookup(e, col, after) {
            None | Some(Value::Null) => b.append_null(),
            Some(Value::Number(n)) => {
                let v = n
                    .as_f64()
                    .ok_or_else(|| anyhow!("{col}: number not f64"))?;
                b.append_value(v as f32);
            }
            Some(v) => bail!("{col}: expected number, got {v:?}"),
        }
    }
    Ok(Arc::new(b.finish()))
}

fn build_float64(events: &[Event], col: &str, after: bool) -> Result<ArrayRef> {
    let mut b = Float64Builder::new();
    for e in events {
        match lookup(e, col, after) {
            None | Some(Value::Null) => b.append_null(),
            Some(Value::Number(n)) => {
                let v = n
                    .as_f64()
                    .ok_or_else(|| anyhow!("{col}: number not f64"))?;
                b.append_value(v);
            }
            Some(v) => bail!("{col}: expected number, got {v:?}"),
        }
    }
    Ok(Arc::new(b.finish()))
}

fn build_bool(events: &[Event], col: &str, after: bool) -> Result<ArrayRef> {
    let mut b = BooleanBuilder::new();
    for e in events {
        match lookup(e, col, after) {
            None | Some(Value::Null) => b.append_null(),
            Some(Value::Bool(v)) => b.append_value(*v),
            // Some sources serialize tinyint(1) as 0/1.
            Some(Value::Number(n)) => match n.as_i64() {
                Some(0) => b.append_value(false),
                Some(1) => b.append_value(true),
                _ => bail!("{col}: numeric boolean must be 0 or 1, got {n}"),
            },
            Some(v) => bail!("{col}: expected bool, got {v:?}"),
        }
    }
    Ok(Arc::new(b.finish()))
}

fn build_utf8(events: &[Event], col: &str, after: bool) -> Result<ArrayRef> {
    let mut b = StringBuilder::new();
    for e in events {
        match lookup(e, col, after) {
            None | Some(Value::Null) => b.append_null(),
            Some(Value::String(s)) => b.append_value(s),
            // Coerce numbers/bools to their string form — matches what the
            // Avro `string` fallback produces today (e.g. BIGINT UNSIGNED).
            Some(Value::Number(n)) => b.append_value(n.to_string()),
            Some(Value::Bool(v)) => b.append_value(v.to_string()),
            Some(other) => b.append_value(other.to_string()),
        }
    }
    Ok(Arc::new(b.finish()))
}

fn build_binary(events: &[Event], col: &str, after: bool) -> Result<ArrayRef> {
    let mut b = BinaryBuilder::new();
    for e in events {
        match lookup(e, col, after) {
            None | Some(Value::Null) => b.append_null(),
            // MySQL TEXT/BLOB serialize through the binlog parser as
            // `{"_base64": "..."}`. Same convention as the Avro path.
            Some(Value::Object(m)) if m.contains_key("_base64") => {
                let s = m["_base64"]
                    .as_str()
                    .ok_or_else(|| anyhow!("{col}: _base64 not a string"))?;
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(s)
                    .with_context(|| {
                        format!("{col}: invalid base64 payload")
                    })?;
                b.append_value(&bytes);
            }
            Some(Value::String(s)) => b.append_value(s.as_bytes()),
            Some(v) => {
                bail!("{col}: expected base64-wrapped binary, got {v:?}")
            }
        }
    }
    Ok(Arc::new(b.finish()))
}

fn build_date32(events: &[Event], col: &str, after: bool) -> Result<ArrayRef> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let mut b = Date32Builder::new();
    for e in events {
        match lookup(e, col, after) {
            None | Some(Value::Null) => b.append_null(),
            Some(Value::String(s)) => {
                let d = NaiveDate::parse_from_str(s, "%Y-%m-%d").with_context(
                    || format!("{col}: parse {s} as YYYY-MM-DD"),
                )?;
                let days = d.signed_duration_since(epoch).num_days() as i32;
                b.append_value(days);
            }
            Some(v) => bail!("{col}: expected date string, got {v:?}"),
        }
    }
    Ok(Arc::new(b.finish()))
}

fn build_ts(
    events: &[Event],
    col: &str,
    after: bool,
    unit: TimeUnit,
) -> Result<ArrayRef> {
    // Common ISO formats we accept from upstream serializers:
    //   2026-05-19T12:34:56.789Z
    //   2026-05-19T12:34:56+00:00
    //   2026-05-19 12:34:56 (assumed UTC for naive timestamps)
    // Numbers are interpreted as milliseconds since epoch.
    let parse =
        |s: &str| -> Result<DateTime<Utc>> {
            if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
                return Ok(dt.with_timezone(&Utc));
            }
            // Fall back to naive parsing assuming UTC.
            let dt = chrono::NaiveDateTime::parse_from_str(
                s,
                "%Y-%m-%d %H:%M:%S%.f",
            )
            .or_else(|_| {
                chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
            })
            .or_else(|_| {
                chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
            })
            .with_context(|| format!("parse {s} as datetime"))?;
            Ok(dt.and_utc())
        };

    match unit {
        TimeUnit::Millisecond => {
            let mut b = TimestampMillisecondBuilder::new()
                .with_timezone_opt(Some("UTC"));
            for e in events {
                match lookup(e, col, after) {
                    None | Some(Value::Null) => b.append_null(),
                    Some(Value::Number(n)) => {
                        let v = n
                            .as_i64()
                            .ok_or_else(|| anyhow!("{col}: number not i64"))?;
                        b.append_value(v);
                    }
                    Some(Value::String(s)) => {
                        b.append_value(parse(s)?.timestamp_millis());
                    }
                    Some(v) => bail!("{col}: expected timestamp, got {v:?}"),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        TimeUnit::Microsecond => {
            let mut b = TimestampMicrosecondBuilder::new()
                .with_timezone_opt(Some("UTC"));
            for e in events {
                match lookup(e, col, after) {
                    None | Some(Value::Null) => b.append_null(),
                    Some(Value::Number(n)) => {
                        let v = n
                            .as_i64()
                            .ok_or_else(|| anyhow!("{col}: number not i64"))?;
                        b.append_value(v);
                    }
                    Some(Value::String(s)) => {
                        b.append_value(parse(s)?.timestamp_micros());
                    }
                    Some(v) => bail!("{col}: expected timestamp, got {v:?}"),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        other => bail!("unsupported TimeUnit {other:?}"),
    }
}

fn build_decimal128(
    events: &[Event],
    col: &str,
    after: bool,
    precision: u8,
    scale: i8,
) -> Result<ArrayRef> {
    let mut b =
        Decimal128Builder::new().with_precision_and_scale(precision, scale)?;
    for e in events {
        match lookup(e, col, after) {
            None | Some(Value::Null) => b.append_null(),
            // JSON serializer renders decimals as strings ("1234.5678") to
            // preserve precision through the JSON intermediary.
            Some(Value::String(s)) => {
                let v = parse_decimal_str(s, scale).with_context(|| {
                    format!("{col}: parse {s} as decimal({precision},{scale})")
                })?;
                b.append_value(v);
            }
            // Some integer columns may carry through as JSON numbers.
            Some(Value::Number(n)) => {
                if let Some(i) = n.as_i64() {
                    let v = i128::from(i) * 10_i128.pow(scale.max(0) as u32);
                    b.append_value(v);
                } else if let Some(f) = n.as_f64() {
                    let v = (f * 10_f64.powi(scale as i32)).round() as i128;
                    b.append_value(v);
                } else {
                    bail!("{col}: number out of range");
                }
            }
            Some(v) => bail!("{col}: expected decimal string, got {v:?}"),
        }
    }
    Ok(Arc::new(b.finish()))
}

/// Parse a decimal string ("1234.56") into the i128 representation that
/// Arrow Decimal128 stores: integer = round(value * 10^scale).
fn parse_decimal_str(s: &str, scale: i8) -> Result<i128> {
    let s = s.trim();
    let (sign, body) = if let Some(rest) = s.strip_prefix('-') {
        (-1i128, rest)
    } else if let Some(rest) = s.strip_prefix('+') {
        (1i128, rest)
    } else {
        (1i128, s)
    };

    let (whole, frac) = match body.split_once('.') {
        Some((w, f)) => (w, f),
        None => (body, ""),
    };

    let target_scale = scale.max(0) as usize;
    let mut frac = frac.to_string();
    if frac.len() > target_scale {
        // Truncate excess fractional digits (Phase 1d behavior — we may want
        // explicit rounding rules later).
        frac.truncate(target_scale);
    } else {
        while frac.len() < target_scale {
            frac.push('0');
        }
    }

    let combined: String = whole.chars().chain(frac.chars()).collect();
    let n: i128 = combined
        .parse()
        .with_context(|| format!("non-numeric decimal: {s}"))?;
    Ok(n * sign)
}

fn build_utf8_from_json(
    events: &[Event],
    col: &str,
    after: bool,
) -> Result<ArrayRef> {
    let mut b = StringBuilder::new();
    for e in events {
        match lookup(e, col, after) {
            None | Some(Value::Null) => b.append_null(),
            Some(v) => b.append_value(v.to_string()),
        }
    }
    Ok(Arc::new(b.finish()))
}

// -----------------------------------------------------------------------
// List<T> and Map<Utf8, Utf8> builders (Phase 2c)
// -----------------------------------------------------------------------

fn build_list(
    events: &[Event],
    col: &str,
    after: bool,
    item_field: &arrow_schema::FieldRef,
) -> Result<ArrayRef> {
    use arrow_array::builder::{
        BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder,
        Int32Builder, Int64Builder, ListBuilder, StringBuilder,
    };

    let item_dt = item_field.data_type();
    match item_dt {
        DataType::Int32 => {
            let mut b = ListBuilder::new(Int32Builder::new());
            for e in events {
                match lookup(e, col, after) {
                    None | Some(Value::Null) => b.append_null(),
                    Some(Value::Array(items)) => {
                        for item in items {
                            match item {
                                Value::Null => b.values().append_null(),
                                Value::Number(n) => {
                                    let v = n.as_i64().ok_or_else(|| {
                                        anyhow!("{col}: list item not i64")
                                    })?;
                                    let v: i32 =
                                        v.try_into().map_err(|_| {
                                            anyhow!(
                                                "{col}: list item {v} overflows i32"
                                            )
                                        })?;
                                    b.values().append_value(v);
                                }
                                other => bail!(
                                    "{col}: expected number list item, got {other:?}"
                                ),
                            }
                        }
                        b.append(true);
                    }
                    Some(other) => {
                        bail!("{col}: expected array, got {other:?}")
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int64 => {
            let mut b = ListBuilder::new(Int64Builder::new());
            for e in events {
                match lookup(e, col, after) {
                    None | Some(Value::Null) => b.append_null(),
                    Some(Value::Array(items)) => {
                        for item in items {
                            match item {
                                Value::Null => b.values().append_null(),
                                Value::Number(n) => {
                                    let v = n.as_i64().ok_or_else(|| {
                                        anyhow!("{col}: list item not i64")
                                    })?;
                                    b.values().append_value(v);
                                }
                                Value::String(s) => {
                                    let v: i64 =
                                        s.parse().with_context(|| {
                                            format!(
                                                "{col}: parse list item {s} as i64"
                                            )
                                        })?;
                                    b.values().append_value(v);
                                }
                                other => bail!(
                                    "{col}: expected number list item, got {other:?}"
                                ),
                            }
                        }
                        b.append(true);
                    }
                    Some(other) => {
                        bail!("{col}: expected array, got {other:?}")
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float32 => {
            let mut b = ListBuilder::new(Float32Builder::new());
            for e in events {
                match lookup(e, col, after) {
                    None | Some(Value::Null) => b.append_null(),
                    Some(Value::Array(items)) => {
                        for item in items {
                            match item {
                                Value::Null => b.values().append_null(),
                                Value::Number(n) => {
                                    let v = n.as_f64().ok_or_else(|| {
                                        anyhow!("{col}: list item not f64")
                                    })?;
                                    b.values().append_value(v as f32);
                                }
                                other => bail!(
                                    "{col}: expected number list item, got {other:?}"
                                ),
                            }
                        }
                        b.append(true);
                    }
                    Some(other) => {
                        bail!("{col}: expected array, got {other:?}")
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float64 => {
            let mut b = ListBuilder::new(Float64Builder::new());
            for e in events {
                match lookup(e, col, after) {
                    None | Some(Value::Null) => b.append_null(),
                    Some(Value::Array(items)) => {
                        for item in items {
                            match item {
                                Value::Null => b.values().append_null(),
                                Value::Number(n) => {
                                    let v = n.as_f64().ok_or_else(|| {
                                        anyhow!("{col}: list item not f64")
                                    })?;
                                    b.values().append_value(v);
                                }
                                other => bail!(
                                    "{col}: expected number list item, got {other:?}"
                                ),
                            }
                        }
                        b.append(true);
                    }
                    Some(other) => {
                        bail!("{col}: expected array, got {other:?}")
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Boolean => {
            let mut b = ListBuilder::new(BooleanBuilder::new());
            for e in events {
                match lookup(e, col, after) {
                    None | Some(Value::Null) => b.append_null(),
                    Some(Value::Array(items)) => {
                        for item in items {
                            match item {
                                Value::Null => b.values().append_null(),
                                Value::Bool(v) => b.values().append_value(*v),
                                other => bail!(
                                    "{col}: expected bool list item, got {other:?}"
                                ),
                            }
                        }
                        b.append(true);
                    }
                    Some(other) => {
                        bail!("{col}: expected array, got {other:?}")
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Utf8 => {
            let mut b = ListBuilder::new(StringBuilder::new());
            for e in events {
                match lookup(e, col, after) {
                    None | Some(Value::Null) => b.append_null(),
                    Some(Value::Array(items)) => {
                        for item in items {
                            match item {
                                Value::Null => b.values().append_null(),
                                Value::String(s) => b.values().append_value(s),
                                // Coerce other scalars to string (matches
                                // the scalar Utf8 builder behavior).
                                other => {
                                    b.values().append_value(other.to_string())
                                }
                            }
                        }
                        b.append(true);
                    }
                    Some(other) => {
                        bail!("{col}: expected array, got {other:?}")
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Binary => {
            let mut b = ListBuilder::new(BinaryBuilder::new());
            for e in events {
                match lookup(e, col, after) {
                    None | Some(Value::Null) => b.append_null(),
                    Some(Value::Array(items)) => {
                        for item in items {
                            match item {
                                Value::Null => b.values().append_null(),
                                Value::Object(m)
                                    if m.contains_key("_base64") =>
                                {
                                    let s = m["_base64"].as_str().ok_or_else(
                                        || {
                                            anyhow!(
                                            "{col}: list item _base64 not a string"
                                        )
                                        },
                                    )?;
                                    let bytes =
                                        base64::engine::general_purpose::STANDARD
                                            .decode(s)
                                            .with_context(|| {
                                                format!(
                                                    "{col}: invalid base64 in list item"
                                                )
                                            })?;
                                    b.values().append_value(&bytes);
                                }
                                Value::String(s) => {
                                    b.values().append_value(s.as_bytes())
                                }
                                other => bail!(
                                    "{col}: expected binary list item, got {other:?}"
                                ),
                            }
                        }
                        b.append(true);
                    }
                    Some(other) => {
                        bail!("{col}: expected array, got {other:?}")
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        // Nested lists or unsupported element types — fall back to
        // JSON-string column with a warn so the data survives even if
        // not in columnar form.
        other => {
            tracing::warn!(
                column = col,
                element_type = ?other,
                "list of unsupported element type — falling back to JSON-string column"
            );
            build_utf8_from_json(events, col, after)
        }
    }
}

fn build_map(
    events: &[Event],
    col: &str,
    after: bool,
    entries_field: &arrow_schema::FieldRef,
) -> Result<ArrayRef> {
    use arrow_array::builder::{MapBuilder, MapFieldNames, StringBuilder};

    // Validate the entries field shape: Struct<key: Utf8, value: Utf8?>.
    // Anything else (e.g. non-string keys) falls back to JSON-string.
    let is_string_utf8_map = match entries_field.data_type() {
        DataType::Struct(fields) if fields.len() == 2 => {
            matches!(fields[0].data_type(), DataType::Utf8)
                && matches!(fields[1].data_type(), DataType::Utf8)
        }
        _ => false,
    };
    if !is_string_utf8_map {
        tracing::warn!(
            column = col,
            entries = ?entries_field.data_type(),
            "map column not <Utf8, Utf8> — falling back to JSON-string column"
        );
        return build_utf8_from_json(events, col, after);
    }

    // The arrow_schema declaration (in encoding/arrow_types.rs) uses
    // `key`/`value` field names for hstore; MapBuilder's default is
    // `keys`/`values`. Override to match the schema, otherwise
    // RecordBatch::try_new errors with a schema-mismatch.
    let field_names = MapFieldNames {
        entry: "entries".to_string(),
        key: "key".to_string(),
        value: "value".to_string(),
    };
    let mut b = MapBuilder::new(
        Some(field_names),
        StringBuilder::new(),
        StringBuilder::new(),
    );
    for e in events {
        match lookup(e, col, after) {
            None | Some(Value::Null) => {
                b.append(false).context("map append null")?;
            }
            Some(Value::Object(m)) => {
                for (k, v) in m {
                    b.keys().append_value(k);
                    match v {
                        Value::Null => b.values().append_null(),
                        Value::String(s) => b.values().append_value(s),
                        other => b.values().append_value(other.to_string()),
                    }
                }
                b.append(true).context("map append")?;
            }
            Some(other) => bail!("{col}: expected object/map, got {other:?}"),
        }
    }
    Ok(Arc::new(b.finish()))
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array,
        Float64Array, Int32Array, Int64Array, StringArray,
        TimestampMillisecondArray,
    };
    use arrow_schema::{Field, Schema};
    use deltaforge_core::encoding::arrow_schema::{
        Connector, build_envelope_arrow_schema,
    };
    use deltaforge_core::encoding::avro_types::{
        ColumnDesc, TypeConversionOpts,
    };
    use deltaforge_core::{Op, SourceInfo, SourcePosition, Transaction};
    use serde_json::json;
    use uuid::Uuid;

    fn make_event(
        op: Op,
        table: &str,
        ts_ms: i64,
        before: Option<Value>,
        after: Option<Value>,
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
            event_id: Some(Uuid::nil()),
            tenant_id: None,
            schema_version: Some("v1".into()),
            schema_sequence: None,
            ddl: None,
            trace_id: None,
            tags: None,
            synthetic: None,
            routing: None,
            tx_end: false,
            checkpoint: None,
            size_bytes: 0,
            received_at_ms: ts_ms,
        }
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

    #[test]
    fn encodes_envelope_meta_columns() {
        let cols = [col("id", "bigint", "bigint")];
        let schema = Arc::new(build_envelope_arrow_schema(
            Connector::Mysql,
            &cols,
            &TypeConversionOpts::default(),
        ));
        let events = vec![
            make_event(
                Op::Create,
                "orders",
                1_000,
                None,
                Some(json!({"id": 1})),
            ),
            make_event(
                Op::Update,
                "orders",
                2_000,
                Some(json!({"id": 1})),
                Some(json!({"id": 2})),
            ),
        ];
        let batch = events_to_record_batch(&schema, &events).unwrap();
        assert_eq!(batch.num_rows(), 2);

        let op = batch
            .column_by_name("op")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(op.value(0), "c");
        assert_eq!(op.value(1), "u");

        let ts = batch
            .column_by_name("op_ts")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(ts.value(0), 1_000);
        assert_eq!(ts.value(1), 2_000);

        let tab = batch
            .column_by_name("source_table")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(tab.value(0), "orders");

        let pos = batch
            .column_by_name("source_position")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(pos.value(0), "binlog:file=mysql-bin.000123,pos=4567");

        let txid = batch
            .column_by_name("tx_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(txid.value(0), "tx-42");
    }

    #[test]
    fn encodes_int32_and_int64_before_after() {
        let cols = [col("age", "int", "int"), col("id", "bigint", "bigint")];
        let schema = Arc::new(build_envelope_arrow_schema(
            Connector::Mysql,
            &cols,
            &TypeConversionOpts::default(),
        ));
        let events = vec![make_event(
            Op::Update,
            "users",
            0,
            Some(json!({"age": 30, "id": 100})),
            Some(json!({"age": 31, "id": 100})),
        )];
        let batch = events_to_record_batch(&schema, &events).unwrap();

        let age_before = batch
            .column_by_name("before_age")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(age_before.value(0), 30);

        let age_after = batch
            .column_by_name("after_age")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(age_after.value(0), 31);

        let id_after = batch
            .column_by_name("after_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id_after.value(0), 100);
    }

    #[test]
    fn delete_event_has_null_after_columns() {
        let cols = [col("id", "bigint", "bigint")];
        let schema = Arc::new(build_envelope_arrow_schema(
            Connector::Mysql,
            &cols,
            &TypeConversionOpts::default(),
        ));
        let events = vec![make_event(
            Op::Delete,
            "users",
            0,
            Some(json!({"id": 42})),
            None,
        )];
        let batch = events_to_record_batch(&schema, &events).unwrap();
        let after = batch.column_by_name("after_id").unwrap();
        assert!(after.is_null(0));
        let before = batch
            .column_by_name("before_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(before.value(0), 42);
    }

    #[test]
    fn boolean_accepts_0_1_numeric() {
        let cols = [col("active", "boolean", "boolean")];
        let schema = Arc::new(build_envelope_arrow_schema(
            Connector::Mysql,
            &cols,
            &TypeConversionOpts::default(),
        ));
        let events = vec![
            make_event(Op::Create, "u", 0, None, Some(json!({"active": 1}))),
            make_event(Op::Create, "u", 0, None, Some(json!({"active": 0}))),
            make_event(Op::Create, "u", 0, None, Some(json!({"active": true}))),
        ];
        let batch = events_to_record_batch(&schema, &events).unwrap();
        let arr = batch
            .column_by_name("after_active")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(arr.value(0));
        assert!(!arr.value(1));
        assert!(arr.value(2));
    }

    #[test]
    fn float64_encodes() {
        let cols = [col("price", "double", "double")];
        let schema = Arc::new(build_envelope_arrow_schema(
            Connector::Mysql,
            &cols,
            &TypeConversionOpts::default(),
        ));
        let events = vec![make_event(
            Op::Create,
            "p",
            0,
            None,
            Some(json!({"price": 3.15159})),
        )];
        let batch = events_to_record_batch(&schema, &events).unwrap();
        let arr = batch
            .column_by_name("after_price")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((arr.value(0) - 3.15159).abs() < 1e-9);
    }

    #[test]
    fn binary_unwraps_base64_blobs() {
        let cols = [col("payload", "blob", "blob")];
        let schema = Arc::new(build_envelope_arrow_schema(
            Connector::Mysql,
            &cols,
            &TypeConversionOpts::default(),
        ));
        let b64 = base64::engine::general_purpose::STANDARD.encode(b"hello");
        let events = vec![make_event(
            Op::Create,
            "p",
            0,
            None,
            Some(json!({"payload": {"_base64": b64}})),
        )];
        let batch = events_to_record_batch(&schema, &events).unwrap();
        let arr = batch
            .column_by_name("after_payload")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(arr.value(0), b"hello");
    }

    #[test]
    fn date32_parses_iso_string() {
        let cols = [col("dob", "date", "date")];
        let schema = Arc::new(build_envelope_arrow_schema(
            Connector::Mysql,
            &cols,
            &TypeConversionOpts::default(),
        ));
        let events = vec![make_event(
            Op::Create,
            "u",
            0,
            None,
            Some(json!({"dob": "2000-01-15"})),
        )];
        let batch = events_to_record_batch(&schema, &events).unwrap();
        let arr = batch
            .column_by_name("after_dob")
            .unwrap()
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        // Days from 1970-01-01 to 2000-01-15 = 10_971
        assert_eq!(arr.value(0), 10_971);
    }

    #[test]
    fn timestamp_accepts_iso_and_numeric() {
        let cols = [col("created_at", "timestamp", "timestamp")];
        let schema = Arc::new(build_envelope_arrow_schema(
            Connector::Mysql,
            &cols,
            &TypeConversionOpts::default(),
        ));
        let iso_ms = chrono::NaiveDate::from_ymd_opt(2026, 5, 19)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_millis();
        let events = vec![
            make_event(
                Op::Create,
                "u",
                0,
                None,
                Some(json!({"created_at": "2026-05-19T12:00:00Z"})),
            ),
            make_event(
                Op::Create,
                "u",
                0,
                None,
                Some(json!({"created_at": iso_ms})),
            ),
        ];
        let batch = events_to_record_batch(&schema, &events).unwrap();
        let arr = batch
            .column_by_name("after_created_at")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(arr.value(0), arr.value(1));
        assert_eq!(arr.value(0), iso_ms);
    }

    #[test]
    fn decimal128_parses_strings() {
        let mut c = col("price", "decimal", "decimal(18,4)");
        c.precision = Some(18);
        c.scale = Some(4);
        let schema = Arc::new(build_envelope_arrow_schema(
            Connector::Mysql,
            &[c],
            &TypeConversionOpts::default(),
        ));
        let events = vec![
            make_event(
                Op::Create,
                "i",
                0,
                None,
                Some(json!({"price": "1234.5678"})),
            ),
            make_event(
                Op::Create,
                "i",
                0,
                None,
                Some(json!({"price": "-0.1"})),
            ),
            make_event(Op::Create, "i", 0, None, Some(json!({"price": "0"}))),
        ];
        let batch = events_to_record_batch(&schema, &events).unwrap();
        let arr = batch
            .column_by_name("after_price")
            .unwrap()
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        // 1234.5678 with scale=4 → 12_345_678
        assert_eq!(arr.value(0), 12_345_678);
        // -0.1 → -1000
        assert_eq!(arr.value(1), -1000);
        // 0 → 0
        assert_eq!(arr.value(2), 0);
    }

    #[test]
    fn decimal128_scales_json_numbers() {
        // Decimal columns may arrive as JSON numbers (not strings) when a
        // source carries integers/floats through. The value must be scaled
        // by 10^scale — exercises the Number arm that the string tests miss.
        let mut c = col("amount", "decimal", "decimal(18,4)");
        c.precision = Some(18);
        c.scale = Some(4);
        let schema = Arc::new(build_envelope_arrow_schema(
            Connector::Mysql,
            &[c],
            &TypeConversionOpts::default(),
        ));
        let events = vec![
            // integer 100 with scale 4 → 100 * 10^4 = 1_000_000
            make_event(Op::Create, "i", 0, None, Some(json!({"amount": 100}))),
            // float 12.5 with scale 4 → round(12.5 * 10^4) = 125_000
            make_event(Op::Create, "i", 0, None, Some(json!({"amount": 12.5}))),
        ];
        let batch = events_to_record_batch(&schema, &events).unwrap();
        let arr = batch
            .column_by_name("after_amount")
            .unwrap()
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(arr.value(0), 1_000_000);
        assert_eq!(arr.value(1), 125_000);
    }

    #[test]
    fn decimal128_truncates_excess_precision() {
        // scale=2 column receiving "1.999" → 199 (truncates last digit)
        let mut c = col("p", "decimal", "decimal(5,2)");
        c.precision = Some(5);
        c.scale = Some(2);
        let schema = Arc::new(build_envelope_arrow_schema(
            Connector::Mysql,
            &[c],
            &TypeConversionOpts::default(),
        ));
        let events = vec![make_event(
            Op::Create,
            "i",
            0,
            None,
            Some(json!({"p": "1.999"})),
        )];
        let batch = events_to_record_batch(&schema, &events).unwrap();
        let arr = batch
            .column_by_name("after_p")
            .unwrap()
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(arr.value(0), 199);
    }

    #[test]
    fn missing_after_column_is_null() {
        let cols = [col("missing", "bigint", "bigint")];
        let schema = Arc::new(build_envelope_arrow_schema(
            Connector::Mysql,
            &cols,
            &TypeConversionOpts::default(),
        ));
        // event has no `missing` field in after
        let events = vec![make_event(
            Op::Create,
            "x",
            0,
            None,
            Some(json!({"other": 1})),
        )];
        let batch = events_to_record_batch(&schema, &events).unwrap();
        let arr = batch.column_by_name("after_missing").unwrap();
        assert!(arr.is_null(0));
    }

    #[test]
    fn empty_event_batch_yields_empty_record_batch() {
        let cols = [col("id", "bigint", "bigint")];
        let schema = Arc::new(build_envelope_arrow_schema(
            Connector::Mysql,
            &cols,
            &TypeConversionOpts::default(),
        ));
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
        let events = vec![make_event(
            Op::Create,
            "t",
            0,
            None,
            Some(json!({"id": 99})),
        )];
        let _batch = events_to_record_batch(&schema, &events).unwrap();
    }

    // -----------------------------------------------------------------------
    // List<T> and Map<Utf8, Utf8> column tests (Phase 2c)
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

    #[test]
    fn list_of_int32_encodes() {
        use arrow_array::ListArray;
        let schema = Arc::new(build_envelope_arrow_schema(
            Connector::Postgres,
            &[pg_array_col("ids", "integer")],
            &TypeConversionOpts::default(),
        ));
        let events = vec![
            make_event(
                Op::Create,
                "t",
                0,
                None,
                Some(json!({"ids": [1, 2, 3]})),
            ),
            make_event(Op::Create, "t", 0, None, Some(json!({"ids": []}))),
            make_event(Op::Create, "t", 0, None, Some(json!({"ids": null}))),
        ];
        let batch = events_to_record_batch(&schema, &events).unwrap();
        let arr = batch
            .column_by_name("after_ids")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(arr.value(0).len(), 3);
        assert_eq!(arr.value(1).len(), 0);
        assert!(arr.is_null(2));
    }

    #[test]
    fn list_of_strings_encodes() {
        use arrow_array::ListArray;
        let schema = Arc::new(build_envelope_arrow_schema(
            Connector::Postgres,
            &[pg_array_col("tags", "text")],
            &TypeConversionOpts::default(),
        ));
        let events = vec![make_event(
            Op::Create,
            "t",
            0,
            None,
            Some(json!({"tags": ["red", "green", "blue"]})),
        )];
        let batch = events_to_record_batch(&schema, &events).unwrap();
        let arr = batch
            .column_by_name("after_tags")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
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
        let schema = Arc::new(build_envelope_arrow_schema(
            Connector::Postgres,
            &[pg_array_col("nums", "bigint")],
            &TypeConversionOpts::default(),
        ));
        let events = vec![make_event(
            Op::Create,
            "t",
            0,
            None,
            Some(json!({"nums": [1, null, 3, null, 5]})),
        )];
        let batch = events_to_record_batch(&schema, &events).unwrap();
        let arr = batch
            .column_by_name("after_nums")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let items = arr.value(0);
        assert_eq!(items.len(), 5);
        assert!(items.is_null(1));
        assert!(items.is_null(3));
        assert!(items.is_valid(0));
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
    fn map_utf8_to_utf8_encodes() {
        use arrow_array::MapArray;
        let schema = Arc::new(build_envelope_arrow_schema(
            Connector::Postgres,
            &[pg_hstore_col("attrs")],
            &TypeConversionOpts::default(),
        ));
        let events = vec![
            make_event(
                Op::Create,
                "t",
                0,
                None,
                Some(json!({"attrs": {"color": "red", "size": "large"}})),
            ),
            make_event(Op::Create, "t", 0, None, Some(json!({"attrs": {}}))),
            make_event(Op::Create, "t", 0, None, Some(json!({"attrs": null}))),
        ];
        let batch = events_to_record_batch(&schema, &events).unwrap();
        let arr = batch
            .column_by_name("after_attrs")
            .unwrap()
            .as_any()
            .downcast_ref::<MapArray>()
            .unwrap();
        // Row 0: 2 entries
        assert_eq!(arr.value(0).len(), 2);
        // Row 1: 0 entries
        assert_eq!(arr.value(1).len(), 0);
        // Row 2: null
        assert!(arr.is_null(2));
    }

    #[test]
    fn list_of_int_overflow_fails() {
        let schema = Arc::new(build_envelope_arrow_schema(
            Connector::Postgres,
            &[pg_array_col("ids", "integer")],
            &TypeConversionOpts::default(),
        ));
        // i64::MAX is too large for the Int32 element type.
        let events = vec![make_event(
            Op::Create,
            "t",
            0,
            None,
            Some(json!({"ids": [1, i64::MAX, 3]})),
        )];
        let res = events_to_record_batch(&schema, &events);
        assert!(res.is_err(), "expected overflow error");
        let err_msg = format!("{:#}", res.unwrap_err());
        assert!(err_msg.contains("overflows"), "got: {err_msg}");
    }
}
