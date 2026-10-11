//! Native JSON rendering of canonical values, per mapping version.
//!
//! Text forms used by more than one encoder (temporal text, decimal text,
//! UUID text) live here too, so every sink renders a value the same way.

use base64::prelude::*;
use serde_json::{Map, Value};

use super::{
    CanonicalRow, CanonicalValue, CivilDate, DateTimeValue, DateValue, DbValue,
    Decimal, IntWidth, MICROS_PER_DAY, Precision, TimeValue, TimestampValue,
    TypeMapping,
};

/// How native JSON renders 64-bit integers (v2 only; v1 always renders
/// numbers).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WideIntegers {
    /// Exact decimal strings (default): survive any JSON parser.
    #[default]
    String,
    /// JSON numbers. An unsigned value above `i64::MAX` is a sink
    /// conversion failure.
    Number,
}

/// Native JSON rendering settings.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RenderOpts {
    pub mapping: TypeMapping,
    pub wide_integers: WideIntegers,
}

impl RenderOpts {
    pub fn new(mapping: TypeMapping) -> Self {
        RenderOpts {
            mapping,
            wide_integers: WideIntegers::default(),
        }
    }
}

/// A canonical value the native renderer cannot write under its settings.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RenderError {
    #[error(
        "unsigned 64-bit value above i64::MAX cannot render as a JSON number (json_wide_integers: number)"
    )]
    WideUnsignedNumber,
    #[error("an unchanged TOAST value has no native rendering")]
    Unchanged,
}

/// A rendered row image: the present columns, and the columns PostgreSQL
/// did not send (unchanged TOAST values), which are never rendered as
/// values.
#[derive(Debug, Clone, PartialEq)]
pub struct RenderedRow {
    pub values: Map<String, Value>,
    pub unchanged: Vec<String>,
}

/// Render a row image.
pub fn render_row(
    row: &CanonicalRow,
    opts: RenderOpts,
) -> Result<RenderedRow, RenderError> {
    let mut values = Map::with_capacity(row.len());
    let mut unchanged = Vec::new();
    for (name, v) in row.iter() {
        if matches!(v, CanonicalValue::Unchanged) {
            unchanged.push(name.to_string());
        } else {
            values.insert(name.to_string(), render_value(v, opts)?);
        }
    }
    Ok(RenderedRow { values, unchanged })
}

/// Render one value.
pub fn render_value(
    v: &CanonicalValue,
    opts: RenderOpts,
) -> Result<Value, RenderError> {
    Ok(match v {
        CanonicalValue::Null => Value::Null,
        CanonicalValue::Unchanged => return Err(RenderError::Unchanged),
        CanonicalValue::Db(db) => render_db(db, opts)?,
        CanonicalValue::Opaque(o) => Value::String(o.text.clone()),
        CanonicalValue::Document(d) => d.value().clone(),
        CanonicalValue::Bytes(b) => base64_wrapper(b),
    })
}

/// The binary wrapper: exactly `{"_base64": "<standard base64>"}`.
pub fn base64_wrapper(bytes: &[u8]) -> Value {
    let mut m = Map::with_capacity(1);
    m.insert(
        "_base64".to_string(),
        Value::String(BASE64_STANDARD.encode(bytes)),
    );
    Value::Object(m)
}

/// The bytes of a binary wrapper, if `v` is exactly one.
pub fn parse_base64_wrapper(v: &Value) -> Option<Vec<u8>> {
    let m = v.as_object()?;
    if m.len() != 1 {
        return None;
    }
    BASE64_STANDARD.decode(m.get("_base64")?.as_str()?).ok()
}

fn render_db(v: &DbValue, opts: RenderOpts) -> Result<Value, RenderError> {
    let v1 = opts.mapping == TypeMapping::V1;
    Ok(match v {
        DbValue::Int { value, width } => {
            if *width == IntWidth::W64
                && !v1
                && opts.wide_integers == WideIntegers::String
            {
                Value::String(value.to_string())
            } else {
                Value::from(*value)
            }
        }
        DbValue::UInt { value, width } => {
            if *width != IntWidth::W64 || v1 {
                Value::from(*value)
            } else if opts.wide_integers == WideIntegers::String {
                Value::String(value.to_string())
            } else if *value > i64::MAX as u64 {
                return Err(RenderError::WideUnsignedNumber);
            } else {
                Value::from(*value)
            }
        }
        DbValue::Decimal(d) => Value::String(decimal_text(d).to_string()),
        DbValue::Float32(f) => {
            if v1 {
                float_value(*f as f64)
            } else {
                float32_value(*f)
            }
        }
        DbValue::Float64(f) => float_value(*f),
        DbValue::Bool(b) => Value::Bool(*b),
        DbValue::Text(s) => Value::String(s.clone()),
        DbValue::Binary(b) => base64_wrapper(b),
        DbValue::Bits(bits) => {
            if v1 {
                Value::from(bits.to_u64())
            } else if bits.len() == 1 {
                Value::Bool(bits.to_u64() == 1)
            } else {
                base64_wrapper(bits.bytes())
            }
        }
        DbValue::Enum(e) => Value::String(e.label.clone()),
        DbValue::Set(s) => Value::String(s.members().join(",")),
        DbValue::Json(j) => Value::String(j.as_str().to_string()),
        DbValue::Date(d) => Value::String(date_text(d)),
        DbValue::Time(t) => Value::String(if v1 {
            time_text_fixed(t.micros(), Precision::Fixed(6))
        } else {
            time_text(t)
        }),
        DbValue::DateTime(dt) => Value::String(if v1 {
            datetime_text_v1(dt)
        } else {
            datetime_text(dt)
        }),
        DbValue::Timestamp(ts) => match (v1, ts) {
            (true, TimestampValue::Instant { micros, .. }) => {
                Value::from(*micros)
            }
            _ => Value::String(timestamp_text(ts)),
        },
        DbValue::Year(y) => Value::from(*y),
        DbValue::Uuid(u) => Value::String(uuid_text(u)),
        DbValue::Array(a) => Value::Array(
            a.items()
                .iter()
                .map(|i| render_value(i, opts))
                .collect::<Result<_, _>>()?,
        ),
    })
}

/// A finite f64 as the shortest round-trip number; non-finite values as
/// `"NaN"`, `"Infinity"`, `"-Infinity"`.
fn float_value(f: f64) -> Value {
    match serde_json::Number::from_f64(f) {
        Some(n) => Value::Number(n),
        None => Value::String(non_finite_text(f).to_string()),
    }
}

/// A finite f32 as the shortest f32 round-trip number (`1.1`, not the
/// widened `1.100000023841858`).
fn float32_value(f: f32) -> Value {
    if !f.is_finite() {
        return Value::String(non_finite_text(f as f64).to_string());
    }
    // The shortest f32 text, read back as the f64 nearest to it: renders
    // with the same digits.
    let shortest: f64 =
        f.to_string().parse().expect("an f32's text parses as f64");
    float_value(shortest)
}

pub(crate) fn non_finite_text(f: f64) -> &'static str {
    if f.is_nan() {
        "NaN"
    } else if f > 0.0 {
        "Infinity"
    } else {
        "-Infinity"
    }
}

/// A decimal's text: the canonical digits, or `NaN`/`Infinity`/`-Infinity`.
pub fn decimal_text(d: &Decimal) -> &str {
    match d {
        Decimal::Finite(s) => s,
        Decimal::NaN => "NaN",
        Decimal::PosInfinity => "Infinity",
        Decimal::NegInfinity => "-Infinity",
    }
}

/// Lowercase hyphenated UUID text.
pub fn uuid_text(u: &[u8; 16]) -> String {
    uuid::Uuid::from_bytes(*u).hyphenated().to_string()
}

/// A year in ISO 8601 form: four digits for 0..=9999, otherwise a sign and
/// at least five digits.
fn year_text(y: i32) -> String {
    if (0..=9999).contains(&y) {
        format!("{y:04}")
    } else if y < 0 {
        format!("-{:05}", -(y as i64))
    } else {
        format!("+{y:05}")
    }
}

/// `YYYY-MM-DD` (zero parts as written).
pub fn civil_date_text(d: &CivilDate) -> String {
    format!("{}-{:02}-{:02}", year_text(d.year()), d.month(), d.day())
}

pub fn date_text(d: &DateValue) -> String {
    match d {
        DateValue::Civil(c) => civil_date_text(c),
        DateValue::PosInfinity => "infinity".to_string(),
        DateValue::NegInfinity => "-infinity".to_string(),
    }
}

/// The fractional part of `micros` (0..1_000_000) at `precision`, with its
/// leading dot, or empty.
fn fraction_text(frac_micros: i64, precision: Precision) -> String {
    match precision {
        Precision::Fixed(0) => String::new(),
        Precision::Fixed(d) => {
            let digits = format!("{frac_micros:06}");
            format!(".{}", &digits[..d as usize])
        }
        Precision::Variable => {
            if frac_micros == 0 {
                String::new()
            } else {
                let digits = format!("{frac_micros:06}");
                format!(".{}", digits.trim_end_matches('0'))
            }
        }
    }
}

/// `HH:MM:SS[.f]` of a time of day in microseconds (0..=24h).
fn clock_text(micros: i64, precision: Precision) -> String {
    let secs = micros / 1_000_000;
    format!(
        "{:02}:{:02}:{:02}{}",
        secs / 3600,
        secs / 60 % 60,
        secs % 60,
        fraction_text(micros % 1_000_000, precision)
    )
}

/// `[-]H+:MM:SS[.f]`: hours as at least two digits.
pub fn time_text(t: &TimeValue) -> String {
    time_text_fixed(t.micros(), t.precision())
}

fn time_text_fixed(micros: i64, precision: Precision) -> String {
    let sign = if micros < 0 { "-" } else { "" };
    format!("{sign}{}", clock_text(micros.abs(), precision))
}

/// `YYYY-MM-DDTHH:MM:SS[.f]`; MySQL's zero date-time as
/// `0000-00-00 00:00:00`; `infinity`/`-infinity`.
pub fn datetime_text(dt: &DateTimeValue) -> String {
    match dt {
        DateTimeValue::Civil {
            date,
            micros_of_day,
            precision,
        } => {
            if date.has_zero_part() {
                mysql_zero_datetime(date, *micros_of_day, *precision)
            } else {
                format!(
                    "{}T{}",
                    civil_date_text(date),
                    clock_text(*micros_of_day, *precision)
                )
            }
        }
        DateTimeValue::PosInfinity => "infinity".to_string(),
        DateTimeValue::NegInfinity => "-infinity".to_string(),
    }
}

/// The MySQL literal for a date-time with a zero part.
fn mysql_zero_datetime(
    date: &CivilDate,
    micros_of_day: i64,
    precision: Precision,
) -> String {
    format!(
        "{} {}",
        civil_date_text(date),
        clock_text(micros_of_day, precision)
    )
}

/// v1 DATETIME: the legacy `YYYY-MM-DD HH:MM:SS.ffffff`.
fn datetime_text_v1(dt: &DateTimeValue) -> String {
    match dt {
        DateTimeValue::Civil {
            date,
            micros_of_day,
            ..
        } => format!(
            "{} {}",
            civil_date_text(date),
            clock_text(*micros_of_day, Precision::Fixed(6))
        ),
        other => datetime_text(other),
    }
}

/// RFC 3339 UTC with `Z` at the value's precision; MySQL's zero TIMESTAMP
/// as `0000-00-00 00:00:00`; `infinity`/`-infinity`.
pub fn timestamp_text(ts: &TimestampValue) -> String {
    match ts {
        TimestampValue::Instant { micros, precision } => {
            let days = micros.div_euclid(MICROS_PER_DAY);
            let of_day = micros.rem_euclid(MICROS_PER_DAY);
            let (y, m, d) = super::civil_from_days(days);
            format!(
                "{}-{m:02}-{d:02}T{}Z",
                year_text(y as i32),
                clock_text(of_day, *precision)
            )
        }
        TimestampValue::Zero { precision } => {
            format!("0000-00-00 {}", clock_text(0, *precision))
        }
        TimestampValue::PosInfinity => "infinity".to_string(),
        TimestampValue::NegInfinity => "-infinity".to_string(),
    }
}
