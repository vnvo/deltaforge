//! Canonical values: the single representation of row images.
//!
//! A row image is a [`CanonicalRow`] of [`CanonicalValue`]s. Sources decode
//! into it (MySQL and PostgreSQL, snapshot and CDC, through one decoder per
//! engine); every consumer renders from it according to the event's
//! [`TypeMapping`] (see [`render`]); the replay journal, the DLQ and durable
//! S3 objects keep its lossless tagged form (see [`durable`]).
//!
//! Every value is built through a validating constructor, so an invariant
//! that a constructor checks holds for every value in the system. No two
//! variants mean the same thing: see `docs/design/canonical-event-model.md`.

pub mod durable;
pub mod render;

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Mapping version
// ---------------------------------------------------------------------------

/// The type-mapping version an event is stamped with at its source. Every
/// encoder renders the event's own version.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize,
)]
pub enum TypeMapping {
    /// Legacy rendering, kept only where the old output was lossless.
    #[serde(rename = "v1")]
    V1,
    /// The canonical rendering (default).
    #[default]
    #[serde(rename = "v2")]
    V2,
}

impl TypeMapping {
    pub fn as_str(self) -> &'static str {
        match self {
            TypeMapping::V1 => "v1",
            TypeMapping::V2 => "v2",
        }
    }

    /// Every version this build can render.
    pub const ALL: [TypeMapping; 2] = [TypeMapping::V1, TypeMapping::V2];

    pub fn parse(s: &str) -> Option<TypeMapping> {
        TypeMapping::ALL.into_iter().find(|v| v.as_str() == s)
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// A value that violates a canonical invariant. Raised by constructors,
/// never by rendering a valid value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid canonical value: {0}")]
pub struct CanonicalError(pub String);

fn invalid<T>(msg: impl Into<String>) -> Result<T, CanonicalError> {
    Err(CanonicalError(msg.into()))
}

// ---------------------------------------------------------------------------
// Source types
// ---------------------------------------------------------------------------

/// The database engine a source type belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Engine {
    Mysql,
    Postgres,
}

/// A source type's identity: enough to interpret its values (PostgreSQL:
/// the type OID and modifier with its name; MySQL: the column type).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SourceType {
    pub engine: Engine,
    /// The type name as the source reports it (`int4range`, `inet`,
    /// `geometry`, a user-defined type's name).
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oid: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub typmod: Option<i32>,
}

// ---------------------------------------------------------------------------
// Values
// ---------------------------------------------------------------------------

/// One value of a row image.
#[derive(Clone, Debug, PartialEq)]
pub enum CanonicalValue {
    /// SQL NULL.
    Null,
    /// A PostgreSQL unchanged TOAST value: the column was not sent. Legal
    /// only in a PostgreSQL update's after image (see
    /// [`CanonicalRow::check_unchanged_allowed`]).
    Unchanged,
    /// A schema-typed database value.
    Db(DbValue),
    /// A value of a source type without a canonical mapping, as the source's
    /// deterministic text.
    Opaque(OpaqueText),
    /// An untyped JSON document produced without a column type.
    Document(Document),
    /// Arbitrary bytes without a column type.
    Bytes(Vec<u8>),
}

impl CanonicalValue {
    pub fn is_null(&self) -> bool {
        matches!(self, CanonicalValue::Null)
    }
}

/// A source-typed value without a canonical mapping.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpaqueText {
    pub source_type: SourceType,
    pub text: String,
}

/// An untyped JSON document. Holds valid JSON only (a `serde_json::Value`
/// cannot hold a non-finite number); its canonical bytes sort object keys at
/// every level.
#[derive(Clone, Debug, PartialEq)]
pub struct Document(serde_json::Value);

impl Document {
    pub fn new(value: serde_json::Value) -> Self {
        Document(value)
    }

    pub fn value(&self) -> &serde_json::Value {
        &self.0
    }

    pub fn into_value(self) -> serde_json::Value {
        self.0
    }

    /// Canonical JSON bytes: object keys sorted at every level, compact,
    /// numbers as `serde_json` writes them.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        crate::canonical_json::canonical_json_bytes(&self.0)
    }

    /// The canonical JSON text.
    pub fn canonical_text(&self) -> String {
        String::from_utf8(self.canonical_bytes())
            .expect("canonical JSON is UTF-8")
    }
}

/// The declared width of an integer column.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum IntWidth {
    W8,
    W16,
    W24,
    W32,
    W64,
}

impl IntWidth {
    pub fn bits(self) -> u32 {
        match self {
            IntWidth::W8 => 8,
            IntWidth::W16 => 16,
            IntWidth::W24 => 24,
            IntWidth::W32 => 32,
            IntWidth::W64 => 64,
        }
    }

    pub fn from_bits(bits: u32) -> Option<IntWidth> {
        [
            IntWidth::W8,
            IntWidth::W16,
            IntWidth::W24,
            IntWidth::W32,
            IntWidth::W64,
        ]
        .into_iter()
        .find(|w| w.bits() == bits)
    }

    fn signed_range(self) -> (i64, i64) {
        match self {
            IntWidth::W64 => (i64::MIN, i64::MAX),
            w => {
                let half = 1i64 << (w.bits() - 1);
                (-half, half - 1)
            }
        }
    }

    fn unsigned_max(self) -> u64 {
        match self {
            IntWidth::W64 => u64::MAX,
            w => (1u64 << w.bits()) - 1,
        }
    }
}

/// Fractional-second precision of a temporal column.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Precision {
    /// The declared precision, 0..=6 digits.
    Fixed(u8),
    /// No declared precision (PostgreSQL without a typmod): rendered with
    /// trailing fractional zeros removed.
    Variable,
}

impl Precision {
    pub fn fixed(digits: u8) -> Result<Precision, CanonicalError> {
        if digits <= 6 {
            Ok(Precision::Fixed(digits))
        } else {
            invalid(format!("fractional precision {digits} above 6"))
        }
    }

    /// Whether `micros` (a fractional part, or any microsecond count) is
    /// representable at this precision.
    fn admits(self, micros: i64) -> bool {
        match self {
            Precision::Variable => true,
            Precision::Fixed(d) => micros % 10i64.pow(6 - d as u32) == 0,
        }
    }
}

/// A schema-typed database value.
#[derive(Clone, Debug, PartialEq)]
pub enum DbValue {
    Int { value: i64, width: IntWidth },
    UInt { value: u64, width: IntWidth },
    Decimal(Decimal),
    Float32(f32),
    Float64(f64),
    Bool(bool),
    Text(String),
    Binary(Vec<u8>),
    Bits(Bits),
    Enum(EnumValue),
    Set(SetValue),
    Json(JsonText),
    Date(DateValue),
    Time(TimeValue),
    DateTime(DateTimeValue),
    Timestamp(TimestampValue),
    Year(u16),
    Uuid([u8; 16]),
    Array(ArrayValue),
}

impl DbValue {
    pub fn int(value: i64, width: IntWidth) -> Result<DbValue, CanonicalError> {
        let (lo, hi) = width.signed_range();
        if value < lo || value > hi {
            return invalid(format!(
                "{value} outside a {}-bit signed integer",
                width.bits()
            ));
        }
        Ok(DbValue::Int { value, width })
    }

    pub fn uint(
        value: u64,
        width: IntWidth,
    ) -> Result<DbValue, CanonicalError> {
        if value > width.unsigned_max() {
            return invalid(format!(
                "{value} outside a {}-bit unsigned integer",
                width.bits()
            ));
        }
        Ok(DbValue::UInt { value, width })
    }

    pub fn year(value: u16) -> Result<DbValue, CanonicalError> {
        if value == 0 || (1901..=2155).contains(&value) {
            Ok(DbValue::Year(value))
        } else {
            invalid(format!("year {value} outside 0 and 1901..2155"))
        }
    }
}

/// An exact decimal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Decimal {
    /// Canonical text: optional `-`, an integer part without leading zeros
    /// (`0` when zero), optionally `.` and fraction digits. Never `-0`.
    Finite(String),
    NaN,
    PosInfinity,
    NegInfinity,
}

impl Decimal {
    /// Parse exact decimal text (`-12.50`, `0.001`, `.5`, `+3`), keeping the
    /// fraction digits as given (the scale is part of the value). Exponents
    /// are refused: no source renders them for an exact decimal.
    pub fn parse(text: &str) -> Result<Decimal, CanonicalError> {
        let (neg, body) = match text.as_bytes().first() {
            Some(b'-') => (true, &text[1..]),
            Some(b'+') => (false, &text[1..]),
            _ => (false, text),
        };
        let (int, frac) = match body.split_once('.') {
            Some((i, f)) => (i, Some(f)),
            None => (body, None),
        };
        let digits = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
        if !digits(int)
            || frac.is_some_and(|f| f.is_empty() || !digits(f))
            || (int.is_empty() && frac.is_none())
        {
            return invalid("not an exact decimal");
        }
        let int = int.trim_start_matches('0');
        let int = if int.is_empty() { "0" } else { int };
        let zero =
            int == "0" && frac.is_none_or(|f| f.bytes().all(|b| b == b'0'));
        let mut out = String::with_capacity(text.len());
        if neg && !zero {
            out.push('-');
        }
        out.push_str(int);
        if let Some(f) = frac {
            out.push('.');
            out.push_str(f);
        }
        Ok(Decimal::Finite(out))
    }
}

/// MySQL BIT(len): ceil(len/8) big-endian bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Bits {
    len: u8,
    bytes: Vec<u8>,
}

impl Bits {
    pub fn new(len: u8, bytes: Vec<u8>) -> Result<Bits, CanonicalError> {
        if !(1..=64).contains(&len) {
            return invalid(format!("BIT length {len} outside 1..=64"));
        }
        let want = (len as usize).div_ceil(8);
        if bytes.len() != want {
            return invalid(format!(
                "BIT({len}) needs {want} bytes, got {}",
                bytes.len()
            ));
        }
        let spare = (want * 8) as u32 - len as u32;
        if spare > 0 && bytes[0] >> (8 - spare) != 0 {
            return invalid(format!("a bit above BIT({len}) is set"));
        }
        Ok(Bits { len, bytes })
    }

    /// From the bit pattern as an integer.
    pub fn from_u64(len: u8, value: u64) -> Result<Bits, CanonicalError> {
        let want = (len as usize).div_ceil(8);
        let all = value.to_be_bytes();
        if want > 8 || all[..8 - want].iter().any(|b| *b != 0) {
            return invalid(format!("value does not fit BIT({len})"));
        }
        Bits::new(len, all[8 - want..].to_vec())
    }

    pub fn len(&self) -> u8 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        false
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn to_u64(&self) -> u64 {
        self.bytes
            .iter()
            .fold(0u64, |acc, b| (acc << 8) | *b as u64)
    }
}

/// An ENUM value: its label and, for MySQL, the stored 1-based index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnumValue {
    pub label: String,
    pub index: u16,
}

impl EnumValue {
    /// A MySQL ENUM value; index 0 (the error sentinel) is refused.
    pub fn mysql(
        index: u16,
        label: String,
    ) -> Result<EnumValue, CanonicalError> {
        if index == 0 {
            return invalid("ENUM index 0 (MySQL's error sentinel)");
        }
        Ok(EnumValue { label, index })
    }
}

/// A MySQL SET value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetValue {
    bitmask: u64,
    members: Vec<String>,
}

impl SetValue {
    /// From the stored bitmask and the column's definition (labels in
    /// definition order); a bit outside the definition is refused.
    pub fn from_bitmask(
        bitmask: u64,
        definition: &[String],
    ) -> Result<SetValue, CanonicalError> {
        if definition.len() < 64 && bitmask >> definition.len() != 0 {
            return invalid("a SET bit outside the definition");
        }
        let members = definition
            .iter()
            .enumerate()
            .filter(|(i, _)| bitmask & (1u64 << i) != 0)
            .map(|(_, l)| l.clone())
            .collect();
        Ok(SetValue { bitmask, members })
    }

    /// From a bitmask and its member labels (the durable form): one label
    /// per set bit.
    pub fn from_parts(
        bitmask: u64,
        members: Vec<String>,
    ) -> Result<SetValue, CanonicalError> {
        if bitmask.count_ones() as usize != members.len() {
            return invalid("SET members do not match the bitmask");
        }
        Ok(SetValue { bitmask, members })
    }

    pub fn bitmask(&self) -> u64 {
        self.bitmask
    }

    pub fn members(&self) -> &[String] {
        &self.members
    }
}

/// A SQL JSON column's value: the source's canonical JSON text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JsonText(String);

impl JsonText {
    pub fn new(text: String) -> Result<JsonText, CanonicalError> {
        if serde_json::from_str::<serde::de::IgnoredAny>(&text).is_err() {
            return invalid("not valid JSON text");
        }
        Ok(JsonText(text))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A civil date. MySQL zero dates and dates with zero parts (`0000-00-00`,
/// `2024-00-15`) are representable; any other invalid date is refused. The
/// year uses astronomical numbering (1 BC is 0).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CivilDate {
    year: i32,
    month: u8,
    day: u8,
}

impl CivilDate {
    /// A real calendar date.
    pub fn new(
        year: i32,
        month: u8,
        day: u8,
    ) -> Result<CivilDate, CanonicalError> {
        if !(1..=12).contains(&month)
            || day < 1
            || day > days_in_month(year, month)
        {
            return invalid(format!(
                "{year}-{month:02}-{day:02} is not a calendar date"
            ));
        }
        Ok(CivilDate { year, month, day })
    }

    /// A MySQL date: a real date, or one with a zero month and/or day.
    pub fn mysql(
        year: i32,
        month: u8,
        day: u8,
    ) -> Result<CivilDate, CanonicalError> {
        if month == 0 || day == 0 {
            if !(0..=9999).contains(&year) || month > 12 || day > 31 {
                return invalid("invalid MySQL zero-part date");
            }
            return Ok(CivilDate { year, month, day });
        }
        CivilDate::new(year, month, day)
    }

    pub fn year(&self) -> i32 {
        self.year
    }

    pub fn month(&self) -> u8 {
        self.month
    }

    pub fn day(&self) -> u8 {
        self.day
    }

    /// A MySQL zero or partial-zero date.
    pub fn has_zero_part(&self) -> bool {
        self.month == 0 || self.day == 0
    }

    /// Days since 1970-01-01; `None` for a zero-part date.
    pub fn days_since_epoch(&self) -> Option<i64> {
        (!self.has_zero_part()).then(|| {
            days_from_civil(
                self.year as i64,
                self.month as i64,
                self.day as i64,
            )
        })
    }

    pub fn from_days_since_epoch(
        days: i64,
    ) -> Result<CivilDate, CanonicalError> {
        let (y, m, d) = civil_from_days(days);
        let year = i32::try_from(y)
            .map_err(|_| CanonicalError("year out of range".into()))?;
        CivilDate::new(year, m as u8, d as u8)
    }
}

fn is_leap(y: i32) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

fn days_in_month(y: i32, m: u8) -> u8 {
    match m {
        2 if is_leap(y) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// Days since 1970-01-01 of a proleptic Gregorian date (H. Hinnant).
pub(crate) fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The proleptic Gregorian date of a day count since 1970-01-01.
pub(crate) fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

pub(crate) const MICROS_PER_DAY: i64 = 86_400_000_000;

/// A DATE value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DateValue {
    Civil(CivilDate),
    PosInfinity,
    NegInfinity,
}

/// A TIME value: MySQL's signed duration (-838:59:59 to 838:59:59) or
/// PostgreSQL's time of day (00:00:00 to 24:00:00).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimeValue {
    micros: i64,
    precision: Precision,
}

impl TimeValue {
    const MYSQL_MAX: i64 = (838 * 3600 + 59 * 60 + 59) * 1_000_000;

    pub fn mysql(
        micros: i64,
        precision: Precision,
    ) -> Result<TimeValue, CanonicalError> {
        if micros.abs() > Self::MYSQL_MAX + 999_999 || !precision.admits(micros)
        {
            return invalid("not a MySQL TIME at its precision");
        }
        Ok(TimeValue { micros, precision })
    }

    pub fn time_of_day(
        micros: i64,
        precision: Precision,
    ) -> Result<TimeValue, CanonicalError> {
        if !(0..=MICROS_PER_DAY).contains(&micros) || !precision.admits(micros)
        {
            return invalid("not a time of day at its precision");
        }
        Ok(TimeValue { micros, precision })
    }

    pub fn micros(&self) -> i64 {
        self.micros
    }

    pub fn precision(&self) -> Precision {
        self.precision
    }
}

/// A civil date-time without a zone.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DateTimeValue {
    Civil {
        date: CivilDate,
        micros_of_day: i64,
        precision: Precision,
    },
    PosInfinity,
    NegInfinity,
}

impl DateTimeValue {
    pub fn civil(
        date: CivilDate,
        micros_of_day: i64,
        precision: Precision,
    ) -> Result<DateTimeValue, CanonicalError> {
        if !(0..MICROS_PER_DAY).contains(&micros_of_day)
            || !precision.admits(micros_of_day)
        {
            return invalid("not a time of day at its precision");
        }
        Ok(DateTimeValue::Civil {
            date,
            micros_of_day,
            precision,
        })
    }
}

/// A UTC instant, MySQL's zero TIMESTAMP, or an infinity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimestampValue {
    Instant {
        micros: i64,
        precision: Precision,
    },
    /// MySQL's `0000-00-00 00:00:00` TIMESTAMP.
    Zero {
        precision: Precision,
    },
    PosInfinity,
    NegInfinity,
}

impl TimestampValue {
    pub fn instant(
        micros: i64,
        precision: Precision,
    ) -> Result<TimestampValue, CanonicalError> {
        if !precision.admits(micros.rem_euclid(1_000_000)) {
            return invalid("instant not representable at its precision");
        }
        Ok(TimestampValue::Instant { micros, precision })
    }
}

/// A PostgreSQL array: one nesting level per dimension (lower bound 1),
/// items typed by the declared element type.
#[derive(Clone, Debug, PartialEq)]
pub struct ArrayValue {
    element_type: SourceType,
    items: Vec<CanonicalValue>,
}

impl ArrayValue {
    /// Items are `Null`, `Db` values (an inner dimension is a `Db(Array)`)
    /// or `Opaque` values of the element type.
    pub fn new(
        element_type: SourceType,
        items: Vec<CanonicalValue>,
    ) -> Result<ArrayValue, CanonicalError> {
        for item in &items {
            match item {
                CanonicalValue::Null
                | CanonicalValue::Db(_)
                | CanonicalValue::Opaque(_) => {}
                other => return invalid(format!("array item {other:?}")),
            }
        }
        Ok(ArrayValue {
            element_type,
            items,
        })
    }

    pub fn element_type(&self) -> &SourceType {
        &self.element_type
    }

    pub fn items(&self) -> &[CanonicalValue] {
        &self.items
    }
}

// ---------------------------------------------------------------------------
// Rows
// ---------------------------------------------------------------------------

/// A row image: column values in source order, names unique.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct CanonicalRow {
    columns: Vec<(String, CanonicalValue)>,
}

impl CanonicalRow {
    pub fn new(
        columns: Vec<(String, CanonicalValue)>,
    ) -> Result<CanonicalRow, CanonicalError> {
        let mut seen = std::collections::HashSet::new();
        for (name, _) in &columns {
            if !seen.insert(name.as_str()) {
                return invalid(format!("duplicate column {name}"));
            }
        }
        Ok(CanonicalRow { columns })
    }

    pub fn get(&self, name: &str) -> Option<&CanonicalValue> {
        self.columns.iter().find(|(n, _)| n == name).map(|(_, v)| v)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &CanonicalValue)> {
        self.columns.iter().map(|(n, v)| (n.as_str(), v))
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.columns.iter().map(|(n, _)| n.as_str())
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Set `name` (replacing its value in place, or appending it).
    pub fn set(&mut self, name: impl Into<String>, value: CanonicalValue) {
        let name = name.into();
        match self.columns.iter_mut().find(|(n, _)| *n == name) {
            Some((_, v)) => *v = value,
            None => self.columns.push((name, value)),
        }
    }

    /// Remove `name`, returning its value.
    pub fn remove(&mut self, name: &str) -> Option<CanonicalValue> {
        let i = self.columns.iter().position(|(n, _)| n == name)?;
        Some(self.columns.remove(i).1)
    }

    pub fn into_columns(self) -> Vec<(String, CanonicalValue)> {
        self.columns
    }

    /// Whether the row holds an [`CanonicalValue::Unchanged`] value.
    pub fn has_unchanged(&self) -> bool {
        self.columns
            .iter()
            .any(|(_, v)| matches!(v, CanonicalValue::Unchanged))
    }
}

#[cfg(test)]
mod tests;
