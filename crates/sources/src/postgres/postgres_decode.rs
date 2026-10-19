//! The schema-driven PostgreSQL value decoder, shared by the snapshot and
//! CDC paths.
//!
//! Both paths read every value as its type's output-function text under one
//! pinned session ([`PINNED_SESSION`]): CDC through pgoutput's text mode on a
//! walsender session pinned before START_REPLICATION, the snapshot through
//! the simple-query protocol on a pinned session. The column's declared type
//! (OID and modifier, never the value) decides the canonical value. A value
//! that does not parse exactly is a [`PgDecodeError`], a source decoding
//! failure; errors name the column and type, never the value.
//!
//! Types without a canonical mapping are [`CanonicalValue::Opaque`]: their
//! output text under the pinned session, with the type identity.

use deltaforge_core::canonical::{
    ArrayValue, CanonicalRow, CanonicalValue, CivilDate, DateTimeValue,
    DateValue, DbValue, Decimal, Engine, IntWidth, JsonText, MICROS_PER_DAY,
    OpaqueText, Precision, SourceType, TimeValue, TimestampValue,
};

use super::postgres_builtin_types::{BUILTIN_TYPES, builtin_type_name};

/// The session settings every value-reading PostgreSQL session runs with,
/// so an output function renders a value the same way on every path.
pub const PINNED_SESSION: &[(&str, &str)] = &[
    ("DateStyle", "ISO, YMD"),
    ("IntervalStyle", "postgres"),
    ("TimeZone", "UTC"),
    ("extra_float_digits", "3"),
    ("bytea_output", "hex"),
    ("lc_monetary", "C"),
    // regclass, regtype, ... print schema-qualified names.
    ("search_path", "pg_catalog"),
];

/// The `SET` statements of [`PINNED_SESSION`], `;`-separated.
pub fn pinned_session_sql() -> String {
    PINNED_SESSION
        .iter()
        .map(|(k, v)| format!("SET {k} = '{v}'"))
        .collect::<Vec<_>>()
        .join("; ")
}

/// A value PostgreSQL sent that its declared type cannot decode exactly.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("cannot decode column `{column}` ({type_name}): {reason}")]
pub struct PgDecodeError {
    pub column: String,
    pub type_name: String,
    pub reason: String,
}

/// How a type decodes.
#[derive(Debug, Clone, PartialEq)]
pub enum PgKind {
    Bool,
    Int(IntWidth),
    Oid,
    Float4,
    Float8,
    Numeric,
    Text,
    Bytea,
    Uuid,
    Json,
    Date,
    Time(Precision),
    Timestamp(Precision),
    Timestamptz(Precision),
    /// An array of the declared element type.
    Array {
        element: Box<PgKind>,
        element_type: SourceType,
    },
    /// A type without a canonical mapping, kept as its output text.
    Opaque(SourceType),
}

/// The fractional precision of a time type's modifier (-1: none declared).
fn time_precision(typmod: i32) -> Precision {
    match u8::try_from(typmod) {
        Ok(d) if d <= 6 => Precision::Fixed(d),
        _ => Precision::Variable,
    }
}

/// The OID of the builtin type named `name`.
pub fn builtin_type_oid(name: &str) -> Option<u32> {
    builtin_oid(name)
}

fn builtin_oid(name: &str) -> Option<u32> {
    BUILTIN_TYPES
        .iter()
        .find(|(_, n)| *n == name)
        .map(|(o, _)| *o)
}

impl PgKind {
    /// The kind of a column of type `oid` with modifier `typmod`; `name`
    /// names a type that is not builtin (from the decoding schema).
    pub fn of(oid: u32, typmod: i32, name: Option<&str>) -> PgKind {
        let source_type = |oid: u32| SourceType {
            engine: Engine::Postgres,
            name: builtin_type_name(oid)
                .map(str::to_string)
                .or_else(|| name.map(str::to_string))
                .unwrap_or_else(|| format!("oid {oid}")),
            oid: Some(oid),
            typmod: (typmod != -1).then_some(typmod),
        };
        match oid {
            16 => PgKind::Bool,
            21 => PgKind::Int(IntWidth::W16),
            23 => PgKind::Int(IntWidth::W32),
            20 => PgKind::Int(IntWidth::W64),
            26 => PgKind::Oid,
            700 => PgKind::Float4,
            701 => PgKind::Float8,
            1700 => PgKind::Numeric,
            // text, varchar, bpchar, name, "char"
            25 | 1043 | 1042 | 19 | 18 => PgKind::Text,
            17 => PgKind::Bytea,
            2950 => PgKind::Uuid,
            114 | 3802 => PgKind::Json,
            1082 => PgKind::Date,
            1083 => PgKind::Time(time_precision(typmod)),
            1114 => PgKind::Timestamp(time_precision(typmod)),
            1184 => PgKind::Timestamptz(time_precision(typmod)),
            _ => {
                // A builtin array (`_elem`) of a decodable element type.
                let element = builtin_type_name(oid)
                    .and_then(|n| n.strip_prefix('_'))
                    .and_then(builtin_oid)
                    .map(|e| (e, PgKind::of(e, typmod, None)));
                match element {
                    Some((e, kind))
                        if !matches!(
                            kind,
                            PgKind::Opaque(_) | PgKind::Array { .. }
                        ) =>
                    {
                        PgKind::Array {
                            element: Box::new(kind),
                            element_type: source_type(e),
                        }
                    }
                    _ => PgKind::Opaque(source_type(oid)),
                }
            }
        }
    }
}

fn parse_err<T>(what: &str) -> Result<T, String> {
    Err(format!("not a valid {what}"))
}

/// Decode one value's output text by kind.
pub fn decode_text(
    kind: &PgKind,
    text: &str,
) -> Result<CanonicalValue, String> {
    let e = |err: deltaforge_core::canonical::CanonicalError| err.0;
    let db = |v: DbValue| Ok(CanonicalValue::Db(v));
    match kind {
        PgKind::Bool => match text {
            "t" => db(DbValue::Bool(true)),
            "f" => db(DbValue::Bool(false)),
            _ => parse_err("boolean"),
        },
        PgKind::Int(w) => match text.parse::<i64>() {
            Ok(v) => db(DbValue::int(v, *w).map_err(e)?),
            Err(_) => parse_err("integer"),
        },
        PgKind::Oid => match text.parse::<u32>() {
            Ok(v) => db(DbValue::uint(v as u64, IntWidth::W32).map_err(e)?),
            Err(_) => parse_err("oid"),
        },
        PgKind::Float4 => db(DbValue::Float32(match text {
            "NaN" => f32::NAN,
            "Infinity" => f32::INFINITY,
            "-Infinity" => f32::NEG_INFINITY,
            t => finite_text(t).parse().or_else(|_| parse_err("real"))?,
        })),
        PgKind::Float8 => db(DbValue::Float64(match text {
            "NaN" => f64::NAN,
            "Infinity" => f64::INFINITY,
            "-Infinity" => f64::NEG_INFINITY,
            t => finite_text(t)
                .parse()
                .or_else(|_| parse_err("double precision"))?,
        })),
        PgKind::Numeric => db(DbValue::Decimal(match text {
            "NaN" => Decimal::NaN,
            "Infinity" => Decimal::PosInfinity,
            "-Infinity" => Decimal::NegInfinity,
            t => Decimal::parse(t).map_err(e)?,
        })),
        PgKind::Text => db(DbValue::Text(text.to_string())),
        PgKind::Bytea => match text.strip_prefix("\\x").map(hex::decode) {
            Some(Ok(bytes)) => db(DbValue::Binary(bytes)),
            _ => parse_err("hex bytea"),
        },
        PgKind::Uuid => match uuid::Uuid::try_parse(text) {
            Ok(u) if u.hyphenated().to_string() == text => {
                db(DbValue::Uuid(*u.as_bytes()))
            }
            _ => parse_err("uuid"),
        },
        PgKind::Json => {
            db(DbValue::Json(JsonText::new(text.to_string()).map_err(e)?))
        }
        PgKind::Date => db(DbValue::Date(match text {
            "infinity" => DateValue::PosInfinity,
            "-infinity" => DateValue::NegInfinity,
            t => {
                let (date, bc) = strip_bc(t);
                DateValue::Civil(parse_date(date, bc)?)
            }
        })),
        PgKind::Time(p) => {
            let micros = parse_clock(text, true)?;
            db(DbValue::Time(
                TimeValue::time_of_day(micros, *p).map_err(e)?,
            ))
        }
        PgKind::Timestamp(p) => db(DbValue::DateTime(match text {
            "infinity" => DateTimeValue::PosInfinity,
            "-infinity" => DateTimeValue::NegInfinity,
            t => {
                let (t, bc) = strip_bc(t);
                let (date, clock) =
                    t.split_once(' ').ok_or("not a valid timestamp")?;
                let date = parse_date(date, bc)?;
                DateTimeValue::civil(date, parse_clock(clock, false)?, *p)
                    .map_err(e)?
            }
        })),
        PgKind::Timestamptz(p) => db(DbValue::Timestamp(match text {
            "infinity" => TimestampValue::PosInfinity,
            "-infinity" => TimestampValue::NegInfinity,
            t => {
                let (t, bc) = strip_bc(t);
                let (date, clock) =
                    t.split_once(' ').ok_or("not a valid timestamptz")?;
                // TimeZone is pinned to UTC: the offset is always +00.
                let clock = clock.strip_suffix("+00").ok_or(
                    "timestamptz not in UTC (the session zone is not pinned)",
                )?;
                let date = parse_date(date, bc)?;
                let days = date.days_since_epoch().expect("a calendar date");
                let of_day = parse_clock(clock, false)?;
                let micros = days
                    .checked_mul(MICROS_PER_DAY)
                    .and_then(|d| d.checked_add(of_day))
                    .ok_or("timestamptz out of range")?;
                TimestampValue::instant(micros, *p).map_err(e)?
            }
        })),
        PgKind::Array {
            element,
            element_type,
        } => {
            if !text.starts_with('{') {
                // Explicit bounds (`[0:1]={...}`) have no canonical form, and
                // the column's other values are canonical arrays: refuse
                // rather than change representation by value.
                return Err(
                    "an array with lower bounds other than 1".to_string()
                );
            }
            let mut p = ArrayParser {
                s: text.as_bytes(),
                i: 0,
            };
            let v = p.array(element, element_type)?;
            if p.i != p.s.len() {
                return parse_err("array");
            }
            Ok(v)
        }
        PgKind::Opaque(t) => Ok(CanonicalValue::Opaque(OpaqueText {
            source_type: t.clone(),
            text: text.to_string(),
        })),
    }
}

/// `t` if it is finite float text, else an unparsable string (Rust also
/// parses `nan`, `inf` and `infinity`, which PostgreSQL never outputs).
fn finite_text(t: &str) -> &str {
    if t.bytes()
        .all(|b| b.is_ascii_digit() || matches!(b, b'.' | b'e' | b'+' | b'-'))
    {
        t
    } else {
        "!"
    }
}

/// `(text without " BC", is BC)`.
fn strip_bc(t: &str) -> (&str, bool) {
    match t.strip_suffix(" BC") {
        Some(rest) => (rest, true),
        None => (t, false),
    }
}

/// `Y+-MM-DD` (ISO DateStyle), astronomical year for BC dates.
fn parse_date(t: &str, bc: bool) -> Result<CivilDate, String> {
    let mut parts = t.splitn(3, '-');
    let (Some(y), Some(m), Some(d)) =
        (parts.next(), parts.next(), parts.next())
    else {
        return parse_err("date");
    };
    if y.len() < 4 || m.len() != 2 || d.len() != 2 {
        return parse_err("date");
    }
    let (Ok(y), Ok(m), Ok(d)) =
        (y.parse::<i32>(), m.parse::<u8>(), d.parse::<u8>())
    else {
        return parse_err("date");
    };
    // 1 BC is year 0.
    let year = if bc { 1 - y } else { y };
    CivilDate::new(year, m, d).map_err(|e| e.0)
}

/// `HH:MM:SS[.f]` in microseconds; `24:00:00` only for a time of day.
fn parse_clock(t: &str, allow_24: bool) -> Result<i64, String> {
    let (hms, frac) = match t.split_once('.') {
        Some((hms, f)) => (hms, Some(f)),
        None => (t, None),
    };
    let mut parts = hms.split(':');
    let (Some(h), Some(m), Some(s), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return parse_err("time");
    };
    if h.len() != 2 || m.len() != 2 || s.len() != 2 {
        return parse_err("time");
    }
    let (Ok(h), Ok(m), Ok(s)) =
        (h.parse::<i64>(), m.parse::<i64>(), s.parse::<i64>())
    else {
        return parse_err("time");
    };
    let micros = match frac {
        None => 0,
        Some(f)
            if !f.is_empty()
                && f.len() <= 6
                && f.bytes().all(|b| b.is_ascii_digit()) =>
        {
            format!("{f:0<6}").parse::<i64>().expect("six digits")
        }
        Some(_) => return parse_err("time"),
    };
    if m > 59
        || s > 59
        || h > 24
        || (h == 24 && (!allow_24 || m + s + micros > 0))
    {
        return parse_err("time");
    }
    Ok(((h * 60 + m) * 60 + s) * 1_000_000 + micros)
}

/// PostgreSQL's array output: `{a,"b c",NULL,{...}}`, `,` delimited.
struct ArrayParser<'a> {
    s: &'a [u8],
    i: usize,
}

impl ArrayParser<'_> {
    fn peek(&self) -> Option<u8> {
        self.s.get(self.i).copied()
    }

    fn expect(&mut self, b: u8) -> Result<(), String> {
        if self.peek() == Some(b) {
            self.i += 1;
            Ok(())
        } else {
            parse_err("array")
        }
    }

    fn array(
        &mut self,
        element: &PgKind,
        element_type: &SourceType,
    ) -> Result<CanonicalValue, String> {
        self.expect(b'{')?;
        let mut items = Vec::new();
        if self.peek() == Some(b'}') {
            self.i += 1;
        } else {
            loop {
                items.push(match self.peek() {
                    Some(b'{') => self.array(element, element_type)?,
                    Some(b'"') => decode_text(element, &self.quoted()?)?,
                    _ => {
                        let raw = self.unquoted();
                        if raw == "NULL" {
                            CanonicalValue::Null
                        } else {
                            decode_text(element, &raw)?
                        }
                    }
                });
                match self.peek() {
                    Some(b',') => self.i += 1,
                    Some(b'}') => {
                        self.i += 1;
                        break;
                    }
                    _ => return parse_err("array"),
                }
            }
        }
        Ok(CanonicalValue::Db(DbValue::Array(
            ArrayValue::new(element_type.clone(), items).map_err(|e| e.0)?,
        )))
    }

    fn quoted(&mut self) -> Result<String, String> {
        self.expect(b'"')?;
        let mut out = Vec::new();
        loop {
            match self.peek() {
                None => return parse_err("array"),
                Some(b'"') => {
                    self.i += 1;
                    break;
                }
                Some(b'\\') => {
                    out.push(
                        *self.s.get(self.i + 1).ok_or("not a valid array")?,
                    );
                    self.i += 2;
                }
                Some(b) => {
                    out.push(b);
                    self.i += 1;
                }
            }
        }
        String::from_utf8(out).or_else(|_| parse_err("array"))
    }

    fn unquoted(&mut self) -> String {
        let start = self.i;
        while let Some(b) = self.peek() {
            if b == b',' || b == b'}' {
                break;
            }
            self.i += 1;
        }
        String::from_utf8_lossy(&self.s[start..self.i]).into_owned()
    }
}

/// One column's decoder.
#[derive(Debug, Clone, PartialEq)]
pub struct PgColumnDecoder {
    pub name: String,
    pub kind: PgKind,
    /// Part of the relation's replica identity key.
    pub key: bool,
}

impl PgColumnDecoder {
    pub fn new(
        name: &str,
        oid: u32,
        typmod: i32,
        type_name: Option<&str>,
    ) -> Self {
        PgColumnDecoder {
            name: name.to_string(),
            kind: PgKind::of(oid, typmod, type_name),
            key: false,
        }
    }

    pub fn with_key(mut self, key: bool) -> Self {
        self.key = key;
        self
    }

    pub fn type_name(&self) -> String {
        match &self.kind {
            PgKind::Opaque(t) => t.name.clone(),
            PgKind::Array { element_type, .. } => {
                format!("{}[]", element_type.name)
            }
            k => format!("{k:?}").to_ascii_lowercase(),
        }
    }

    fn fail(&self, reason: impl Into<String>) -> PgDecodeError {
        PgDecodeError {
            column: self.name.clone(),
            type_name: self.type_name(),
            reason: reason.into(),
        }
    }

    pub fn decode(
        &self,
        text: Option<&str>,
    ) -> Result<CanonicalValue, PgDecodeError> {
        match text {
            None => Ok(CanonicalValue::Null),
            Some(t) => decode_text(&self.kind, t).map_err(|r| self.fail(r)),
        }
    }
}

/// A pgoutput tuple column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TupleValue<'a> {
    Null,
    Unchanged,
    Text(&'a [u8]),
    Binary,
}

/// What [`PgRowDecoder::reconstruct_unchanged`] did to one row.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Reconstruction {
    /// Unchanged values filled from the full old row.
    pub reconstructed: u64,
    /// Unchanged values left unavailable (no full old row, or no value in it).
    pub unavailable: u64,
}

/// Which row image a tuple is: only an update's new image may hold
/// unchanged TOAST values, and a key-only old image (`K`) holds only the
/// key columns (PostgreSQL sends no value for the others, so they are not
/// in the image rather than NULL).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Image {
    UpdateAfter,
    KeyOnly,
    Other,
}

/// Decoders for one relation, in column order.
#[derive(Debug, Clone, PartialEq)]
pub struct PgRowDecoder {
    pub columns: Vec<PgColumnDecoder>,
}

impl PgRowDecoder {
    /// Decode a pgoutput tuple.
    pub fn decode_tuple(
        &self,
        values: &[TupleValue<'_>],
        image: Image,
    ) -> Result<CanonicalRow, PgDecodeError> {
        if values.len() != self.columns.len() {
            return Err(PgDecodeError {
                column: String::new(),
                type_name: String::new(),
                reason: format!(
                    "the tuple has {} columns, the relation {}",
                    values.len(),
                    self.columns.len()
                ),
            });
        }
        let mut out = Vec::with_capacity(values.len());
        for (col, v) in self.columns.iter().zip(values) {
            if image == Image::KeyOnly && !col.key {
                if *v != TupleValue::Null {
                    return Err(
                        col.fail("a value outside the key in a key-only image")
                    );
                }
                continue;
            }
            let value = match v {
                TupleValue::Null => CanonicalValue::Null,
                TupleValue::Unchanged if image == Image::UpdateAfter => {
                    CanonicalValue::Unchanged
                }
                TupleValue::Unchanged => {
                    return Err(col.fail(
                        "an unchanged TOAST value outside an update's new row",
                    ));
                }
                TupleValue::Binary => {
                    return Err(
                        col.fail("a binary-format value (text mode expected)")
                    );
                }
                TupleValue::Text(b) => {
                    let t = std::str::from_utf8(b)
                        .map_err(|_| col.fail("text is not UTF-8"))?;
                    col.decode(Some(t))?
                }
            };
            out.push((col.name.clone(), value));
        }
        CanonicalRow::new(out).map_err(|e| PgDecodeError {
            column: String::new(),
            type_name: String::new(),
            reason: e.0,
        })
    }

    /// Fill an update's unchanged TOAST values from its old row. PostgreSQL
    /// leaves an unchanged out-of-line value out of the new row; under
    /// REPLICA IDENTITY FULL it sends the complete old row, whose value of
    /// that column is the unchanged value, exactly (proven per major by
    /// `postgres_toast_old_image_e2e`). `full_before`: the old row, only when
    /// it is that full image. Both rows were decoded by this decoder, so a
    /// column's declared type is the same in both. A value whose old copy is
    /// missing or itself unavailable stays unchanged (unavailable).
    pub fn reconstruct_unchanged(
        after: &mut CanonicalRow,
        full_before: Option<&CanonicalRow>,
    ) -> Reconstruction {
        let mut outcome = Reconstruction::default();
        let unchanged: Vec<String> = after
            .iter()
            .filter(|(_, v)| matches!(v, CanonicalValue::Unchanged))
            .map(|(name, _)| name.to_string())
            .collect();
        for name in unchanged {
            match full_before.and_then(|before| before.get(&name)) {
                Some(old) if !matches!(old, CanonicalValue::Unchanged) => {
                    after.set(name, old.clone());
                    outcome.reconstructed += 1;
                }
                _ => outcome.unavailable += 1,
            }
        }
        outcome
    }

    /// Decode a snapshot row read with the simple-query protocol (each
    /// value its output text).
    pub fn decode_texts(
        &self,
        values: &[Option<&str>],
    ) -> Result<CanonicalRow, PgDecodeError> {
        if values.len() != self.columns.len() {
            return Err(PgDecodeError {
                column: String::new(),
                type_name: String::new(),
                reason: format!(
                    "the row has {} columns, the decoding schema {}",
                    values.len(),
                    self.columns.len()
                ),
            });
        }
        let mut out = Vec::with_capacity(values.len());
        for (col, v) in self.columns.iter().zip(values) {
            out.push((col.name.clone(), col.decode(*v)?));
        }
        CanonicalRow::new(out).map_err(|e| PgDecodeError {
            column: String::new(),
            type_name: String::new(),
            reason: e.0,
        })
    }
}

#[cfg(test)]
mod tests;
