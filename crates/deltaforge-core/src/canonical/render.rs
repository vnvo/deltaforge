//! Native JSON rendering of canonical values, per mapping version.
//!
//! Text forms used by more than one encoder (temporal text, decimal text,
//! UUID text) live here too, so every sink renders a value the same way.

use base64::display::Base64Display;
use base64::prelude::*;
use serde::ser::{Error as _, SerializeMap, SerializeSeq};
use serde::{Serialize, Serializer};
use serde_json::{Map, Value};

use super::{
    CanonicalRow, CanonicalValue, CivilDate, DateTimeValue, DateValue, DbValue,
    Decimal, IntWidth, MICROS_PER_DAY, Precision, TimeValue, TimestampValue,
    TypeMapping,
};

/// How native JSON renders 64-bit integers (v2 only; v1 always renders
/// numbers). Sink setting `json_wide_integers`.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(rename_all = "lowercase")]
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

/// Why a canonical value has no native rendering under the settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RenderFailure {
    #[error(
        "unsigned 64-bit value above i64::MAX cannot render as a JSON number (json_wide_integers: number)"
    )]
    WideUnsignedNumber,
    #[error("an unchanged TOAST value has no native rendering")]
    Unchanged,
    /// A processor-added extension and a column render under one name.
    #[error("an extension has the name of a rendered column")]
    ExtensionCollision,
}

/// A column the native renderer cannot write under its settings. Never
/// carries the value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("column {column}: {failure}")]
pub struct RenderError {
    /// The column (`after.id` once the image is known).
    pub column: String,
    pub failure: RenderFailure,
}

/// A rendered row image: the present columns, and the columns PostgreSQL
/// did not send (unchanged TOAST values), which are never rendered as
/// values.
#[derive(Debug, Clone, PartialEq)]
pub struct RenderedRow {
    pub values: Map<String, Value>,
    pub unchanged: Vec<String>,
}

/// Render a row image as a JSON map.
pub fn render_row(
    row: &CanonicalRow,
    opts: RenderOpts,
) -> Result<RenderedRow, RenderError> {
    let (image, unchanged) = row_image(row, opts)?;
    Ok(RenderedRow {
        values: image.to_map(),
        unchanged,
    })
}

/// A checked row image that renders as it serializes: its present columns
/// in row order, then its extensions, written straight to the serializer
/// (no map, no owned names, no intermediate values).
#[derive(Debug, Clone, Copy)]
pub struct RowImage<'a> {
    row: &'a CanonicalRow,
    opts: RenderOpts,
}

/// Check that every value of `row` renders under `opts`: the row's image,
/// and the columns PostgreSQL did not send (unchanged TOAST values), which
/// the image omits. A processor extension with a column's name is refused,
/// never written over the column.
pub fn row_image(
    row: &CanonicalRow,
    opts: RenderOpts,
) -> Result<(RowImage<'_>, Vec<String>), RenderError> {
    let mut unchanged = Vec::new();
    for (name, v) in row.iter() {
        if matches!(v, CanonicalValue::Unchanged) {
            unchanged.push(name.to_string());
        } else {
            check_value(v, opts).map_err(|failure| RenderError {
                column: name.to_string(),
                failure,
            })?;
        }
    }
    if let Some(name) = row.extensions().keys().find(|n| row.get(n).is_some()) {
        return Err(RenderError {
            column: name.clone(),
            failure: RenderFailure::ExtensionCollision,
        });
    }
    Ok((RowImage { row, opts }, unchanged))
}

impl RowImage<'_> {
    /// The image as a JSON map.
    pub fn to_map(&self) -> Map<String, Value> {
        match serde_json::to_value(self) {
            Ok(Value::Object(m)) => m,
            other => unreachable!("a row image is a JSON object: {other:?}"),
        }
    }
}

impl Serialize for RowImage<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(None)?;
        for (name, v) in self.row.iter() {
            if !matches!(v, CanonicalValue::Unchanged) {
                m.serialize_entry(name, &Rendered(v, self.opts))?;
            }
        }
        for (name, doc) in self.row.extensions() {
            m.serialize_entry(name, doc.value())?;
        }
        m.end()
    }
}

/// Whether `v` renders under `opts` (the failures [`Rendered`] would
/// meet).
fn check_value(
    v: &CanonicalValue,
    opts: RenderOpts,
) -> Result<(), RenderFailure> {
    match v {
        CanonicalValue::Unchanged => Err(RenderFailure::Unchanged),
        CanonicalValue::Db(DbValue::UInt {
            value,
            width: IntWidth::W64,
        }) if opts.mapping != TypeMapping::V1
            && opts.wide_integers == WideIntegers::Number
            && *value > i64::MAX as u64 =>
        {
            Err(RenderFailure::WideUnsignedNumber)
        }
        CanonicalValue::Db(DbValue::Array(a)) => {
            a.items().iter().try_for_each(|i| check_value(i, opts))
        }
        _ => Ok(()),
    }
}

/// Render one value.
pub fn render_value(
    v: &CanonicalValue,
    opts: RenderOpts,
) -> Result<Value, RenderFailure> {
    check_value(v, opts)?;
    Ok(serde_json::to_value(Rendered(v, opts))
        .expect("a checked value renders"))
}

/// A value's native rendering, written as it serializes. Only checked
/// values (see [`check_value`]) are serialized.
struct Rendered<'a>(&'a CanonicalValue, RenderOpts);

impl Serialize for Rendered<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self.0 {
            CanonicalValue::Null => s.serialize_unit(),
            CanonicalValue::Unchanged => {
                Err(S::Error::custom(RenderFailure::Unchanged.to_string()))
            }
            CanonicalValue::Db(db) => serialize_db(db, self.1, s),
            CanonicalValue::Opaque(o) => s.serialize_str(&o.text),
            CanonicalValue::Document(d) => d.value().serialize(s),
            CanonicalValue::Bytes(b) => Base64Wrapper(b).serialize(s),
        }
    }
}

/// The binary wrapper `{"_base64": "<standard base64>"}`, encoded as it
/// serializes.
struct Base64Wrapper<'a>(&'a [u8]);

impl Serialize for Base64Wrapper<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(Some(1))?;
        m.serialize_entry(
            "_base64",
            &Collect(Base64Display::new(self.0, &BASE64_STANDARD)),
        )?;
        m.end()
    }
}

/// A displayed value serialized as a JSON string.
struct Collect<T>(T);

impl<T: std::fmt::Display> Serialize for Collect<T> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(&self.0)
    }
}

/// A SET's members joined by `,`.
struct Members<'a>(&'a [String]);

impl std::fmt::Display for Members<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, m) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str(",")?;
            }
            f.write_str(m)?;
        }
        Ok(())
    }
}

fn serialize_db<S: Serializer>(
    v: &DbValue,
    opts: RenderOpts,
    s: S,
) -> Result<S::Ok, S::Error> {
    let v1 = opts.mapping == TypeMapping::V1;
    match v {
        DbValue::Int { value, width } => {
            if *width == IntWidth::W64
                && !v1
                && opts.wide_integers == WideIntegers::String
            {
                s.collect_str(value)
            } else {
                s.serialize_i64(*value)
            }
        }
        DbValue::UInt { value, width } => {
            if *width != IntWidth::W64 || v1 {
                s.serialize_u64(*value)
            } else if opts.wide_integers == WideIntegers::String {
                s.collect_str(value)
            } else if *value > i64::MAX as u64 {
                Err(S::Error::custom(
                    RenderFailure::WideUnsignedNumber.to_string(),
                ))
            } else {
                s.serialize_u64(*value)
            }
        }
        DbValue::Decimal(d) => s.serialize_str(decimal_text(d)),
        DbValue::Float32(f) => {
            if v1 {
                serialize_f64(*f as f64, s)
            } else {
                serialize_f32(*f, s)
            }
        }
        DbValue::Float64(f) => serialize_f64(*f, s),
        DbValue::Bool(b) => s.serialize_bool(*b),
        DbValue::Text(t) => s.serialize_str(t),
        DbValue::Binary(b) => Base64Wrapper(b).serialize(s),
        DbValue::Bits(bits) => {
            if v1 {
                s.serialize_u64(bits.to_u64())
            } else if bits.len() == 1 {
                s.serialize_bool(bits.to_u64() == 1)
            } else {
                Base64Wrapper(bits.bytes()).serialize(s)
            }
        }
        DbValue::Enum(e) => s.serialize_str(&e.label),
        DbValue::Set(set) => s.collect_str(&Members(set.members())),
        DbValue::Json(j) => s.serialize_str(j.as_str()),
        DbValue::Date(d) => s.serialize_str(&date_text(d)),
        DbValue::Time(t) => s.serialize_str(&if v1 {
            time_text_fixed(t.micros(), Precision::Fixed(6))
        } else {
            time_text(t)
        }),
        DbValue::DateTime(dt) => s.serialize_str(&if v1 {
            datetime_text_v1(dt)
        } else {
            datetime_text(dt)
        }),
        DbValue::Timestamp(ts) => match (v1, ts) {
            (true, TimestampValue::Instant { micros, .. }) => {
                s.serialize_i64(*micros)
            }
            _ => s.serialize_str(&timestamp_text(ts)),
        },
        DbValue::Year(y) => s.serialize_u16(*y),
        DbValue::Uuid(u) => {
            s.collect_str(&uuid::Uuid::from_bytes(*u).hyphenated())
        }
        DbValue::Array(a) => {
            let mut seq = s.serialize_seq(Some(a.items().len()))?;
            for i in a.items() {
                seq.serialize_element(&Rendered(i, opts))?;
            }
            seq.end()
        }
    }
}

/// A finite f64 as the shortest round-trip number; non-finite values as
/// `"NaN"`, `"Infinity"`, `"-Infinity"`.
fn serialize_f64<S: Serializer>(f: f64, s: S) -> Result<S::Ok, S::Error> {
    if f.is_finite() {
        s.serialize_f64(f)
    } else {
        s.serialize_str(non_finite_text(f))
    }
}

/// A finite f32 as the shortest f32 round-trip number (`1.1`, not the
/// widened `1.100000023841858`).
fn serialize_f32<S: Serializer>(f: f32, s: S) -> Result<S::Ok, S::Error> {
    if !f.is_finite() {
        return s.serialize_str(non_finite_text(f as f64));
    }
    // The shortest f32 text, read back as the f64 nearest to it: renders
    // with the same digits.
    let mut buf = [0u8; 64];
    let mut w = &mut buf[..];
    std::io::Write::write_fmt(&mut w, format_args!("{f}"))
        .expect("an f32's text fits 64 bytes");
    let len = 64 - w.len();
    let text = std::str::from_utf8(&buf[..len]).expect("ASCII digits");
    s.serialize_f64(text.parse().expect("an f32's text parses as f64"))
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
