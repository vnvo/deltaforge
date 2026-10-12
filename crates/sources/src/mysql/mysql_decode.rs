//! The schema-driven MySQL value decoder: one per table schema version,
//! shared by the snapshot and CDC paths.
//!
//! Each path turns its wire value into a [`Raw`] (the binlog's
//! `ColumnValue`, or the binary protocol's `mysql_async::Value` read with
//! `character_set_results = binary` and `time_zone = '+00:00'`), and the
//! column's [`ColumnKind`] (from the decoding schema, never from the value)
//! turns the raw value into a [`CanonicalValue`]. A value the kind cannot
//! decode exactly is a [`DecodeError`]: a source decoding failure that fails
//! the source closed. Errors name the column, its type and the reason, never
//! the value.

use deltaforge_core::canonical::{
    Bits, CanonicalRow, CanonicalValue, CivilDate, DateTimeValue, DateValue,
    DbValue, Decimal, EnumValue, IntWidth, JsonText, Precision, SetValue,
    TimeValue, TimestampValue,
};
use mysql_binlog_connector_rust::column::column_value::ColumnValue;

use super::MySqlColumn;
use super::mysql_table_schema::MySqlTableSchema;

/// A value MySQL sent that the decoding schema cannot decode exactly.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("cannot decode column `{column}` ({column_type}): {reason}")]
pub struct DecodeError {
    pub column: String,
    pub column_type: String,
    pub reason: String,
}

/// A column's character set, from its collation id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Charset {
    Utf8mb4,
    Utf8mb3,
    Ascii,
    /// MySQL's latin1 (cp1252, with its five undefined bytes mapped to the
    /// C1 controls).
    Latin1,
}

impl Charset {
    /// The supported character sets by collation id (MySQL 8.4's
    /// `information_schema.COLLATIONS`). `None` for every other id.
    pub fn from_collation_id(id: i64) -> Option<Charset> {
        match id {
            45 | 46 | 224..=247 | 255..=323 => Some(Charset::Utf8mb4),
            33 | 76 | 83 | 192..=215 | 223 => Some(Charset::Utf8mb3),
            11 | 65 => Some(Charset::Ascii),
            5 | 8 | 15 | 31 | 47 | 48 | 49 | 94 => Some(Charset::Latin1),
            _ => None,
        }
    }

    fn decode(self, bytes: &[u8]) -> Result<String, &'static str> {
        match self {
            Charset::Utf8mb4 => std::str::from_utf8(bytes)
                .map(str::to_string)
                .map_err(|_| "bytes are not valid utf8mb4"),
            Charset::Utf8mb3 => match std::str::from_utf8(bytes) {
                Ok(s) if s.chars().all(|c| (c as u32) <= 0xFFFF) => {
                    Ok(s.to_string())
                }
                _ => Err("bytes are not valid utf8mb3"),
            },
            Charset::Ascii => match bytes.is_ascii() {
                true => Ok(bytes.iter().map(|b| *b as char).collect()),
                false => Err("bytes are not valid ascii"),
            },
            Charset::Latin1 => {
                Ok(bytes.iter().map(|b| latin1_char(*b)).collect())
            }
        }
    }
}

/// MySQL latin1: cp1252, whose undefined bytes (0x81, 0x8D, 0x8F, 0x90,
/// 0x9D) map to the C1 controls of the same value.
fn latin1_char(b: u8) -> char {
    const HIGH: [u16; 32] = [
        0x20AC, 0x0081, 0x201A, 0x0192, 0x201E, 0x2026, 0x2020, 0x2021, 0x02C6,
        0x2030, 0x0160, 0x2039, 0x0152, 0x008D, 0x017D, 0x008F, 0x0090, 0x2018,
        0x2019, 0x201C, 0x201D, 0x2022, 0x2013, 0x2014, 0x02DC, 0x2122, 0x0161,
        0x203A, 0x0153, 0x009D, 0x017E, 0x0178,
    ];
    match b {
        0x80..=0x9F => char::from_u32(HIGH[(b - 0x80) as usize] as u32)
            .expect("table holds chars"),
        b => b as char,
    }
}

/// How a column decodes, from its definition.
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnKind {
    Int {
        width: IntWidth,
        unsigned: bool,
    },
    Decimal,
    Float,
    Double,
    Bit {
        len: u8,
    },
    /// CHAR (`trim`: trailing spaces removed) and VARCHAR/TEXT.
    Text {
        charset: Charset,
        trim: bool,
    },
    /// BINARY(n) (`fixed`: zero-padded to n bytes), VARBINARY, BLOB,
    /// GEOMETRY (stored bytes).
    Binary {
        fixed: Option<usize>,
    },
    Enum {
        labels: Vec<String>,
    },
    Set {
        labels: Vec<String>,
    },
    Json,
    Date,
    Time {
        precision: Precision,
    },
    DateTime {
        precision: Precision,
    },
    Timestamp {
        precision: Precision,
    },
    Year,
}

impl ColumnKind {
    /// The decoding kind of `col`, or why it has none.
    pub fn of(col: &MySqlColumn) -> Result<ColumnKind, String> {
        let dt = col.data_type.trim().to_ascii_lowercase();
        let ct = col.column_type.to_ascii_lowercase();
        let unsigned = ct.contains("unsigned");
        let int = |width| Ok(ColumnKind::Int { width, unsigned });
        let precision = || -> Result<Precision, String> {
            Precision::fixed(fsp(col) as u8).map_err(|e| e.0)
        };
        let charset = || -> Result<Charset, String> {
            let id = col
                .collation_id
                .ok_or("the schema does not record the column's collation")?;
            Charset::from_collation_id(id).ok_or_else(|| {
                format!("collation id {id} is not a supported character set")
            })
        };
        match dt.as_str() {
            "tinyint" => int(IntWidth::W8),
            "smallint" => int(IntWidth::W16),
            "mediumint" => int(IntWidth::W24),
            "int" | "integer" => int(IntWidth::W32),
            "bigint" => int(IntWidth::W64),
            "decimal" | "numeric" => Ok(ColumnKind::Decimal),
            "float" => Ok(ColumnKind::Float),
            "double" | "real" | "double precision" => Ok(ColumnKind::Double),
            "bit" => {
                let len = paren_number(&ct).unwrap_or(1);
                match u8::try_from(len) {
                    Ok(len @ 1..=64) => Ok(ColumnKind::Bit { len }),
                    _ => Err(format!("BIT length {len}")),
                }
            }
            "char" => Ok(ColumnKind::Text {
                charset: charset()?,
                trim: true,
            }),
            "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" => {
                Ok(ColumnKind::Text {
                    charset: charset()?,
                    trim: false,
                })
            }
            "binary" => {
                let n = col
                    .char_octet_length
                    .or(col.char_max_length)
                    .or_else(|| paren_number(&ct).map(|n| n as i64))
                    .ok_or("BINARY without a length")?;
                Ok(ColumnKind::Binary {
                    fixed: Some(n as usize),
                })
            }
            "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob"
            | "geometry" | "point" | "linestring" | "polygon"
            | "multipoint" | "multilinestring" | "multipolygon"
            | "geometrycollection" | "geomcollection" => {
                Ok(ColumnKind::Binary { fixed: None })
            }
            "enum" => Ok(ColumnKind::Enum {
                labels: super::mysql_signature::quoted_values(&col.column_type)
                    .ok_or("unreadable ENUM definition")?,
            }),
            "set" => Ok(ColumnKind::Set {
                labels: super::mysql_signature::quoted_values(&col.column_type)
                    .ok_or("unreadable SET definition")?,
            }),
            "json" => Ok(ColumnKind::Json),
            "date" => Ok(ColumnKind::Date),
            "time" => Ok(ColumnKind::Time {
                precision: precision()?,
            }),
            "datetime" => Ok(ColumnKind::DateTime {
                precision: precision()?,
            }),
            "timestamp" => Ok(ColumnKind::Timestamp {
                precision: precision()?,
            }),
            "year" => Ok(ColumnKind::Year),
            other => {
                Err(format!("MySQL type `{other}` has no canonical mapping"))
            }
        }
    }

    /// Whether the snapshot selects the column as `CAST(col AS UNSIGNED)`
    /// (its stored ENUM index or SET bitmask) rather than its text.
    pub fn snapshot_selects_number(&self) -> bool {
        matches!(self, ColumnKind::Enum { .. } | ColumnKind::Set { .. })
    }
}

/// The fractional-seconds precision of a temporal column: recorded, or the
/// `(n)` suffix of its type, or 0.
fn fsp(c: &MySqlColumn) -> i64 {
    c.datetime_precision
        .or_else(|| {
            paren_number(&c.column_type.to_ascii_lowercase()).map(|n| n as i64)
        })
        .unwrap_or(0)
}

fn paren_number(column_type: &str) -> Option<u64> {
    let open = column_type.find('(')?;
    column_type[open + 1..]
        .split(')')
        .next()?
        .trim()
        .parse()
        .ok()
}

/// A wire value, normalized from either path.
#[derive(Debug, Clone, PartialEq)]
pub enum Raw<'a> {
    Null,
    /// An integer value (snapshot).
    Int(i64),
    /// An unsigned integer value (snapshot).
    UInt(u64),
    /// A binlog integer: the column's stored bits, sign-extended. An
    /// unsigned column reinterprets them at its width.
    BinlogInt(i64),
    Float(f32),
    Double(f64),
    Bytes(&'a [u8]),
    /// A binlog BIT value.
    BitValue(u64),
    /// An ENUM index or SET bitmask.
    Index(u64),
    Date {
        year: u16,
        month: u8,
        day: u8,
    },
    DateTime {
        year: u16,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        micros: u32,
    },
    Time {
        negative: bool,
        hours: u64,
        minutes: u8,
        seconds: u8,
        micros: u32,
    },
    /// A binlog TIMESTAMP: microseconds since the epoch (0 is MySQL's zero
    /// TIMESTAMP, which no other instant can be).
    EpochMicros(i64),
    /// Binlog binary JSON.
    Jsonb(&'a [u8]),
}

impl<'a> Raw<'a> {
    /// A binlog value.
    pub fn from_binlog(v: &'a ColumnValue) -> Raw<'a> {
        match v {
            ColumnValue::None => Raw::Null,
            ColumnValue::Tiny(x) => Raw::BinlogInt(*x as i64),
            ColumnValue::Short(x) => Raw::BinlogInt(*x as i64),
            ColumnValue::Long(x) => Raw::BinlogInt(*x as i64),
            ColumnValue::LongLong(x) => Raw::BinlogInt(*x),
            ColumnValue::Float(x) => Raw::Float(*x),
            ColumnValue::Double(x) => Raw::Double(*x),
            ColumnValue::Decimal(s) => Raw::Bytes(s.as_bytes()),
            ColumnValue::Time(t) => Raw::Time {
                negative: t.negative,
                hours: t.hours as u64,
                minutes: t.minutes,
                seconds: t.seconds,
                micros: t.micros,
            },
            ColumnValue::Date(d) => Raw::Date {
                year: d.year,
                month: d.month,
                day: d.day,
            },
            ColumnValue::DateTime(dt) => Raw::DateTime {
                year: dt.date.year,
                month: dt.date.month,
                day: dt.date.day,
                hour: dt.hour,
                minute: dt.minute,
                second: dt.second,
                micros: dt.micros,
            },
            ColumnValue::Timestamp(us) => Raw::EpochMicros(*us),
            ColumnValue::Year(y) => Raw::UInt(*y as u64),
            ColumnValue::String(b) | ColumnValue::Blob(b) => Raw::Bytes(b),
            ColumnValue::Bit(b) => Raw::BitValue(*b),
            ColumnValue::Set(b) => Raw::Index(*b),
            ColumnValue::Enum(i) => Raw::Index(*i as u64),
            ColumnValue::Json(b) => Raw::Jsonb(b),
        }
    }

    /// A binary-protocol snapshot value of a column of `kind` (ENUM and SET
    /// columns are selected as `CAST(col AS UNSIGNED)`).
    pub fn from_snapshot(
        v: &'a mysql_async::Value,
        kind: &ColumnKind,
    ) -> Raw<'a> {
        use mysql_async::Value as V;
        match v {
            V::NULL => Raw::Null,
            V::Int(n) if kind.snapshot_selects_number() && *n >= 0 => {
                Raw::Index(*n as u64)
            }
            V::UInt(n) if kind.snapshot_selects_number() => Raw::Index(*n),
            V::Int(n) => Raw::Int(*n),
            V::UInt(n) => Raw::UInt(*n),
            V::Float(f) => Raw::Float(*f),
            V::Double(f) => Raw::Double(*f),
            V::Bytes(b) => Raw::Bytes(b),
            V::Date(y, mo, d, h, mi, s, us) => {
                if matches!(kind, ColumnKind::Date) {
                    Raw::Date {
                        year: *y,
                        month: *mo,
                        day: *d,
                    }
                } else {
                    Raw::DateTime {
                        year: *y,
                        month: *mo,
                        day: *d,
                        hour: *h,
                        minute: *mi,
                        second: *s,
                        micros: *us,
                    }
                }
            }
            V::Time(neg, days, h, mi, s, us) => Raw::Time {
                negative: *neg,
                hours: *days as u64 * 24 + *h as u64,
                minutes: *mi,
                seconds: *s,
                micros: *us,
            },
        }
    }
}

/// One column's decoder.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDecoder {
    pub name: String,
    pub column_type: String,
    /// The decoding kind, or why the column cannot be decoded (raised for
    /// the first non-NULL value, so a table whose unsupported column is
    /// always NULL still flows).
    pub kind: Result<ColumnKind, String>,
}

impl ColumnDecoder {
    pub fn new(col: &MySqlColumn) -> ColumnDecoder {
        ColumnDecoder {
            name: col.name.clone(),
            column_type: col.column_type.clone(),
            kind: ColumnKind::of(col),
        }
    }

    fn fail(&self, reason: impl Into<String>) -> DecodeError {
        DecodeError {
            column: self.name.clone(),
            column_type: self.column_type.clone(),
            reason: reason.into(),
        }
    }

    /// Decode one value.
    pub fn decode(&self, raw: Raw<'_>) -> Result<CanonicalValue, DecodeError> {
        if raw == Raw::Null {
            return Ok(CanonicalValue::Null);
        }
        let kind = self.kind.as_ref().map_err(|e| self.fail(e.clone()))?;
        decode_kind(kind, raw)
            .map(CanonicalValue::Db)
            .map_err(|reason| self.fail(reason))
    }
}

fn mismatch(kind: &ColumnKind, raw: &Raw<'_>) -> String {
    // The variant name only: never the value.
    let variant = format!("{raw:?}");
    let variant = variant.split(['(', ' ', '{']).next().unwrap_or("");
    format!(
        "a {variant} wire value for a {kind_name} column",
        kind_name = kind_name(kind)
    )
}

fn kind_name(kind: &ColumnKind) -> &'static str {
    match kind {
        ColumnKind::Int { .. } => "integer",
        ColumnKind::Decimal => "DECIMAL",
        ColumnKind::Float => "FLOAT",
        ColumnKind::Double => "DOUBLE",
        ColumnKind::Bit { .. } => "BIT",
        ColumnKind::Text { .. } => "text",
        ColumnKind::Binary { .. } => "binary",
        ColumnKind::Enum { .. } => "ENUM",
        ColumnKind::Set { .. } => "SET",
        ColumnKind::Json => "JSON",
        ColumnKind::Date => "DATE",
        ColumnKind::Time { .. } => "TIME",
        ColumnKind::DateTime { .. } => "DATETIME",
        ColumnKind::Timestamp { .. } => "TIMESTAMP",
        ColumnKind::Year => "YEAR",
    }
}

fn text_of<'a>(bytes: &'a [u8], what: &str) -> Result<&'a str, String> {
    std::str::from_utf8(bytes).map_err(|_| format!("{what} is not ASCII text"))
}

fn decode_kind(kind: &ColumnKind, raw: Raw<'_>) -> Result<DbValue, String> {
    let e = |err: deltaforge_core::canonical::CanonicalError| err.0;
    match (kind, raw) {
        (
            ColumnKind::Int {
                width,
                unsigned: false,
            },
            Raw::Int(v) | Raw::BinlogInt(v),
        ) => DbValue::int(v, *width).map_err(e),
        (
            ColumnKind::Int {
                width,
                unsigned: false,
            },
            Raw::UInt(v),
        ) => {
            let v = i64::try_from(v)
                .map_err(|_| "unsigned value in a signed column".to_string())?;
            DbValue::int(v, *width).map_err(e)
        }
        (
            ColumnKind::Int {
                width,
                unsigned: true,
            },
            Raw::BinlogInt(v),
        ) => {
            let mask = match width {
                IntWidth::W64 => u64::MAX,
                w => (1u64 << w.bits()) - 1,
            };
            DbValue::uint(v as u64 & mask, *width).map_err(e)
        }
        (
            ColumnKind::Int {
                width,
                unsigned: true,
            },
            Raw::UInt(v),
        ) => DbValue::uint(v, *width).map_err(e),
        (
            ColumnKind::Int {
                width,
                unsigned: true,
            },
            Raw::Int(v),
        ) => {
            let v = u64::try_from(v).map_err(|_| {
                "negative value in an unsigned column".to_string()
            })?;
            DbValue::uint(v, *width).map_err(e)
        }
        (ColumnKind::Decimal, Raw::Bytes(b)) => Ok(DbValue::Decimal(
            Decimal::parse(text_of(b, "DECIMAL")?).map_err(e)?,
        )),
        (ColumnKind::Float, Raw::Float(f)) => Ok(DbValue::Float32(f)),
        (ColumnKind::Double, Raw::Double(f)) => Ok(DbValue::Float64(f)),
        (ColumnKind::Bit { len }, Raw::BitValue(v)) => {
            Ok(DbValue::Bits(Bits::from_u64(*len, v).map_err(e)?))
        }
        (ColumnKind::Bit { len }, Raw::Bytes(b)) => {
            Ok(DbValue::Bits(Bits::new(*len, b.to_vec()).map_err(e)?))
        }
        (ColumnKind::Text { charset, trim }, Raw::Bytes(b)) => {
            let mut s = charset.decode(b)?;
            if *trim {
                s.truncate(s.trim_end_matches(' ').len());
            }
            Ok(DbValue::Text(s))
        }
        (ColumnKind::Binary { fixed }, Raw::Bytes(b)) => {
            let mut bytes = b.to_vec();
            if let Some(n) = fixed {
                // The binlog drops BINARY(n)'s trailing zero padding.
                if bytes.len() > *n {
                    return Err(format!(
                        "{} bytes in a BINARY({n})",
                        bytes.len()
                    ));
                }
                bytes.resize(*n, 0);
            }
            Ok(DbValue::Binary(bytes))
        }
        (ColumnKind::Enum { labels }, Raw::Index(i)) => {
            if i == 0 {
                return Err(
                    "ENUM index 0 (MySQL's error value for an invalid ENUM)"
                        .into(),
                );
            }
            let label = usize::try_from(i - 1)
                .ok()
                .and_then(|i| labels.get(i))
                .ok_or_else(|| {
                    format!(
                        "ENUM index {i} beyond the definition's {}",
                        labels.len()
                    )
                })?;
            Ok(DbValue::Enum(
                EnumValue::mysql(i as u16, label.clone()).map_err(e)?,
            ))
        }
        (ColumnKind::Set { labels }, Raw::Index(bits)) => Ok(DbValue::Set(
            SetValue::from_bitmask(bits, labels).map_err(e)?,
        )),
        (ColumnKind::Json, Raw::Jsonb(b)) => {
            let text = super::mysql_jsonb::jsonb_to_mysql_text(b)
                .map_err(|err| err.0)?;
            Ok(DbValue::Json(JsonText::new(text).map_err(e)?))
        }
        (ColumnKind::Json, Raw::Bytes(b)) => {
            let text = std::str::from_utf8(b)
                .map_err(|_| "JSON text is not UTF-8".to_string())?;
            Ok(DbValue::Json(JsonText::new(text.to_string()).map_err(e)?))
        }
        (ColumnKind::Date, Raw::Date { year, month, day }) => {
            Ok(DbValue::Date(DateValue::Civil(
                CivilDate::mysql(year as i32, month, day).map_err(e)?,
            )))
        }
        (
            ColumnKind::Time { precision },
            Raw::Time {
                negative,
                hours,
                minutes,
                seconds,
                micros,
            },
        ) => {
            let abs = ((hours as i64 * 60 + minutes as i64) * 60
                + seconds as i64)
                * 1_000_000
                + micros as i64;
            let micros = if negative { -abs } else { abs };
            Ok(DbValue::Time(
                TimeValue::mysql(micros, *precision).map_err(e)?,
            ))
        }
        (
            ColumnKind::DateTime { precision },
            Raw::DateTime {
                year,
                month,
                day,
                hour,
                minute,
                second,
                micros,
            },
        ) => {
            let date = CivilDate::mysql(year as i32, month, day).map_err(e)?;
            let of_day = clock_micros(hour, minute, second, micros)?;
            Ok(DbValue::DateTime(
                DateTimeValue::civil(date, of_day, *precision).map_err(e)?,
            ))
        }
        (ColumnKind::Timestamp { precision }, Raw::EpochMicros(us)) => {
            Ok(DbValue::Timestamp(if us == 0 {
                TimestampValue::Zero {
                    precision: *precision,
                }
            } else {
                TimestampValue::instant(us, *precision).map_err(e)?
            }))
        }
        (
            ColumnKind::Timestamp { precision },
            Raw::DateTime {
                year,
                month,
                day,
                hour,
                minute,
                second,
                micros,
            },
        ) => {
            // The snapshot session's zone is UTC: the civil value is the
            // instant.
            if (year, month, day, hour, minute, second, micros)
                == (0, 0, 0, 0, 0, 0, 0)
            {
                return Ok(DbValue::Timestamp(TimestampValue::Zero {
                    precision: *precision,
                }));
            }
            let date = CivilDate::new(year as i32, month, day).map_err(e)?;
            let days = date.days_since_epoch().expect("a calendar date");
            let us = days * 86_400_000_000
                + clock_micros(hour, minute, second, micros)?;
            Ok(DbValue::Timestamp(
                TimestampValue::instant(us, *precision).map_err(e)?,
            ))
        }
        (ColumnKind::Year, Raw::UInt(y)) => year(y),
        (ColumnKind::Year, Raw::Int(y)) => {
            year(u64::try_from(y).map_err(|_| "negative YEAR".to_string())?)
        }
        (kind, raw) => Err(mismatch(kind, &raw)),
    }
}

fn year(y: u64) -> Result<DbValue, String> {
    let y = u16::try_from(y).map_err(|_| "YEAR out of range".to_string())?;
    DbValue::year(y).map_err(|e| e.0)
}

fn clock_micros(
    hour: u8,
    minute: u8,
    second: u8,
    micros: u32,
) -> Result<i64, String> {
    if hour > 23 || minute > 59 || second > 59 || micros > 999_999 {
        return Err("time of day out of range".into());
    }
    Ok(
        ((hour as i64 * 60 + minute as i64) * 60 + second as i64) * 1_000_000
            + micros as i64,
    )
}

/// The decoders of one table schema version, in column order.
#[derive(Debug, Clone, PartialEq)]
pub struct TableDecoder {
    pub columns: Vec<ColumnDecoder>,
}

impl TableDecoder {
    pub fn new(schema: &MySqlTableSchema) -> TableDecoder {
        TableDecoder {
            columns: schema.columns.iter().map(ColumnDecoder::new).collect(),
        }
    }

    /// Decode a binlog row image: `included` is the image's column bitmap
    /// (by ordinal), `values` the included columns' values in order.
    pub fn decode_binlog_row(
        &self,
        included: &[bool],
        values: &[ColumnValue],
    ) -> Result<CanonicalRow, DecodeError> {
        let mut out = Vec::with_capacity(values.len());
        let mut vi = values.iter();
        for (idx, inc) in included.iter().enumerate() {
            if !*inc {
                continue;
            }
            let col = self.columns.get(idx).ok_or_else(|| DecodeError {
                column: format!("#{}", idx + 1),
                column_type: String::new(),
                reason: format!(
                    "the row image has more columns than the decoding schema's {}",
                    self.columns.len()
                ),
            })?;
            let v = vi.next().ok_or_else(|| {
                col.fail("the row image is missing the column's value")
            })?;
            out.push((col.name.clone(), col.decode(Raw::from_binlog(v))?));
        }
        CanonicalRow::new(out).map_err(|err| DecodeError {
            column: String::new(),
            column_type: String::new(),
            reason: err.0,
        })
    }

    /// The snapshot's select list: every column in order, ENUM and SET as
    /// their stored number.
    pub fn snapshot_select_list(&self) -> String {
        self.columns
            .iter()
            .map(|c| {
                let q = format!("`{}`", c.name.replace('`', "``"));
                match &c.kind {
                    // `CAST(.. AS UNSIGNED)`, never `col+0`: MySQL returns
                    // `col+0` as a DOUBLE, which cannot hold a 64-member
                    // SET's bitmask (it came back negative and rounded).
                    Ok(k) if k.snapshot_selects_number() => {
                        format!("CAST({q} AS UNSIGNED) AS {q}")
                    }
                    _ => q,
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Decode a snapshot row selected with [`Self::snapshot_select_list`].
    pub fn decode_snapshot_row(
        &self,
        values: &[mysql_async::Value],
    ) -> Result<CanonicalRow, DecodeError> {
        if values.len() != self.columns.len() {
            return Err(DecodeError {
                column: String::new(),
                column_type: String::new(),
                reason: format!(
                    "the snapshot row has {} columns, the decoding schema {}",
                    values.len(),
                    self.columns.len()
                ),
            });
        }
        let mut out = Vec::with_capacity(values.len());
        for (col, v) in self.columns.iter().zip(values) {
            let value = match &col.kind {
                Ok(kind) => col.decode(Raw::from_snapshot(v, kind))?,
                Err(_) if matches!(v, mysql_async::Value::NULL) => {
                    CanonicalValue::Null
                }
                Err(reason) => return Err(col.fail(reason.clone())),
            };
            out.push((col.name.clone(), value));
        }
        CanonicalRow::new(out).map_err(|err| DecodeError {
            column: String::new(),
            column_type: String::new(),
            reason: err.0,
        })
    }
}

#[cfg(test)]
mod tests;
