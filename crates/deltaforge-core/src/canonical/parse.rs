//! The exact inverse of the native rendering: a rendered value parsed back
//! as a value of a known column type. Used where a value comes back from a
//! consumer that only sees renderings (the JavaScript processor): the
//! column's original canonical value is the type authority, and a rendered
//! value either parses exactly into that type or is refused. Nothing is
//! coerced, rounded or guessed.

use serde_json::Value;

use super::render::parse_base64_wrapper;
use super::{
    ArrayValue, Bits, CanonicalValue, CivilDate, DateTimeValue, DateValue,
    DbValue, Decimal, Document, IntWidth, JsonText, MICROS_PER_DAY, OpaqueText,
    Precision, TimeValue, TimestampValue, TypeMapping,
};

/// Why a rendered value does not parse into its column's type.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ParseError {
    /// The column's original value is SQL NULL or unchanged TOAST: its type
    /// is not in the event.
    #[error("the column's type is not known (its original value carries none)")]
    UnknownType,
    /// ENUM and SET values are validated by their definition, which the
    /// event does not carry: a changed label is refused.
    #[error("a changed {0} value cannot be validated without its definition")]
    NeedsDefinition(&'static str),
    #[error("not a valid {0}")]
    Invalid(&'static str),
    /// The column's declared type cannot be decoded by its source.
    #[error("the column's type {0} has no canonical mapping")]
    Unsupported(String),
}

fn invalid<T>(what: &'static str) -> Result<T, ParseError> {
    Err(ParseError::Invalid(what))
}

/// Parse `v`, a rendering for `mapping`, as a value of the type of
/// `template` (the column's original value).
pub fn parse_like(
    template: &CanonicalValue,
    v: &Value,
    mapping: TypeMapping,
) -> Result<CanonicalValue, ParseError> {
    if v.is_null() {
        return Ok(CanonicalValue::Null);
    }
    match template {
        CanonicalValue::Null | CanonicalValue::Unchanged => {
            Err(ParseError::UnknownType)
        }
        CanonicalValue::Opaque(o) => match v {
            Value::String(s) => Ok(CanonicalValue::Opaque(OpaqueText {
                source_type: o.source_type.clone(),
                text: s.clone(),
            })),
            _ => invalid("text value"),
        },
        CanonicalValue::Document(_) => {
            Ok(CanonicalValue::Document(Document::new(v.clone())))
        }
        CanonicalValue::Bytes(_) => match parse_base64_wrapper(v) {
            Some(b) => Ok(CanonicalValue::Bytes(b)),
            None => invalid("binary wrapper"),
        },
        CanonicalValue::Db(db) => {
            parse_db(db, v, mapping).map(CanonicalValue::Db)
        }
    }
}

/// An exact integer from a JSON number (an integral float only within the
/// range where every integer is exact) or decimal text.
fn int_of(v: &Value) -> Result<i128, ParseError> {
    match v {
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i as i128)
            } else if let Some(u) = n.as_u64() {
                Ok(u as i128)
            } else {
                let f = n.as_f64().unwrap_or(f64::NAN);
                if f.fract() == 0.0 && f.abs() <= 9_007_199_254_740_992.0 {
                    Ok(f as i128)
                } else {
                    invalid("integer")
                }
            }
        }
        Value::String(s) => {
            let digits = s.strip_prefix('-').unwrap_or(s);
            if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit())
            {
                return invalid("integer");
            }
            s.parse::<i128>().or_else(|_| invalid("integer"))
        }
        _ => invalid("integer"),
    }
}

fn float_text(v: &Value) -> Result<String, ParseError> {
    match v {
        Value::Number(n) => Ok(n.to_string()),
        _ => invalid("number"),
    }
}

fn non_finite(v: &Value) -> Option<f64> {
    match v.as_str()? {
        "NaN" => Some(f64::NAN),
        "Infinity" => Some(f64::INFINITY),
        "-Infinity" => Some(f64::NEG_INFINITY),
        _ => None,
    }
}

fn parse_db(
    t: &DbValue,
    v: &Value,
    mapping: TypeMapping,
) -> Result<DbValue, ParseError> {
    let e = |_: super::CanonicalError| {
        ParseError::Invalid("value of the column's type")
    };
    let v1 = mapping == TypeMapping::V1;
    match t {
        DbValue::Int { width, .. } => {
            let i =
                i64::try_from(int_of(v)?).or_else(|_| invalid("integer"))?;
            DbValue::int(i, *width).map_err(e)
        }
        DbValue::UInt { width, .. } => {
            let u = u64::try_from(int_of(v)?)
                .or_else(|_| invalid("unsigned integer"))?;
            DbValue::uint(u, *width).map_err(e)
        }
        DbValue::Decimal(_) => {
            let text = match v {
                Value::String(s) => s.clone(),
                Value::Number(n) => n.to_string(),
                _ => return invalid("decimal"),
            };
            Ok(DbValue::Decimal(match text.as_str() {
                "NaN" => Decimal::NaN,
                "Infinity" => Decimal::PosInfinity,
                "-Infinity" => Decimal::NegInfinity,
                t => Decimal::parse(t).map_err(e)?,
            }))
        }
        DbValue::Float32(_) => match non_finite(v) {
            Some(f) => Ok(DbValue::Float32(f as f32)),
            None => {
                let f: f32 =
                    float_text(v)?.parse().or_else(|_| invalid("real"))?;
                if f.is_finite() {
                    Ok(DbValue::Float32(f))
                } else {
                    invalid("real")
                }
            }
        },
        DbValue::Float64(_) => match non_finite(v) {
            Some(f) => Ok(DbValue::Float64(f)),
            None => {
                let f: f64 =
                    float_text(v)?.parse().or_else(|_| invalid("double"))?;
                if f.is_finite() {
                    Ok(DbValue::Float64(f))
                } else {
                    invalid("double")
                }
            }
        },
        DbValue::Bool(_) => match v {
            Value::Bool(b) => Ok(DbValue::Bool(*b)),
            _ => invalid("boolean"),
        },
        DbValue::Text(_) => match v {
            Value::String(s) => Ok(DbValue::Text(s.clone())),
            _ => invalid("text"),
        },
        DbValue::Binary(_) => match parse_base64_wrapper(v) {
            Some(b) => Ok(DbValue::Binary(b)),
            None => invalid("binary wrapper"),
        },
        DbValue::Bits(b) => {
            let len = b.len();
            let bits = if v1 {
                let n = u64::try_from(int_of(v)?)
                    .or_else(|_| invalid("BIT value"))?;
                Bits::from_u64(len, n)
            } else if len == 1 {
                match v {
                    Value::Bool(x) => Bits::new(1, vec![*x as u8]),
                    _ => return invalid("BIT(1) boolean"),
                }
            } else {
                match parse_base64_wrapper(v) {
                    Some(bytes) => Bits::new(len, bytes),
                    None => return invalid("BIT binary wrapper"),
                }
            };
            Ok(DbValue::Bits(bits.map_err(e)?))
        }
        DbValue::Enum(en) => match v {
            Value::String(s) if *s == en.label => Ok(t.clone()),
            _ => Err(ParseError::NeedsDefinition("ENUM")),
        },
        DbValue::Set(set) => match v {
            Value::String(s) if *s == set.members().join(",") => Ok(t.clone()),
            _ => Err(ParseError::NeedsDefinition("SET")),
        },
        DbValue::Json(_) => match v {
            Value::String(s) => {
                Ok(DbValue::Json(JsonText::new(s.clone()).map_err(e)?))
            }
            _ => invalid("JSON text"),
        },
        DbValue::Date(_) => match v.as_str() {
            Some("infinity") => Ok(DbValue::Date(DateValue::PosInfinity)),
            Some("-infinity") => Ok(DbValue::Date(DateValue::NegInfinity)),
            Some(s) => Ok(DbValue::Date(DateValue::Civil(parse_date(s)?))),
            None => invalid("date"),
        },
        DbValue::Time(tv) => {
            let s = v.as_str().ok_or(ParseError::Invalid("time"))?;
            let micros = parse_duration(s)?;
            let p = tv.precision();
            let t = TimeValue::mysql(micros, p)
                .or_else(|_| TimeValue::time_of_day(micros, p));
            Ok(DbValue::Time(t.map_err(e)?))
        }
        DbValue::DateTime(dt) => {
            let precision = match dt {
                DateTimeValue::Civil { precision, .. } => *precision,
                _ => Precision::Variable,
            };
            match v.as_str() {
                Some("infinity") => {
                    Ok(DbValue::DateTime(DateTimeValue::PosInfinity))
                }
                Some("-infinity") => {
                    Ok(DbValue::DateTime(DateTimeValue::NegInfinity))
                }
                Some(s) => {
                    let (date, clock) = s
                        .split_once(['T', ' '])
                        .ok_or(ParseError::Invalid("date-time"))?;
                    let date = parse_date(date)?;
                    let micros = parse_clock(clock)?;
                    Ok(DbValue::DateTime(
                        DateTimeValue::civil(date, micros, precision)
                            .map_err(e)?,
                    ))
                }
                None => invalid("date-time"),
            }
        }
        DbValue::Timestamp(ts) => {
            let precision = match ts {
                TimestampValue::Instant { precision, .. }
                | TimestampValue::Zero { precision } => *precision,
                _ => Precision::Variable,
            };
            match v {
                Value::String(s) if s == "infinity" => {
                    Ok(DbValue::Timestamp(TimestampValue::PosInfinity))
                }
                Value::String(s) if s == "-infinity" => {
                    Ok(DbValue::Timestamp(TimestampValue::NegInfinity))
                }
                Value::String(s) if s.starts_with("0000-00-00 ") => {
                    if parse_clock(&s["0000-00-00 ".len()..])? != 0 {
                        return invalid("zero timestamp");
                    }
                    Ok(DbValue::Timestamp(TimestampValue::Zero { precision }))
                }
                Value::String(s) if !v1 => Ok(DbValue::Timestamp(
                    TimestampValue::instant(parse_rfc3339(s)?, precision)
                        .map_err(e)?,
                )),
                other if v1 => {
                    let us = i64::try_from(int_of(other)?)
                        .or_else(|_| invalid("epoch micros"))?;
                    Ok(DbValue::Timestamp(
                        TimestampValue::instant(us, precision).map_err(e)?,
                    ))
                }
                _ => invalid("timestamp"),
            }
        }
        DbValue::Year(_) => {
            let y = u16::try_from(int_of(v)?).or_else(|_| invalid("year"))?;
            DbValue::year(y).map_err(e)
        }
        DbValue::Uuid(_) => {
            let s = v.as_str().ok_or(ParseError::Invalid("uuid"))?;
            match uuid::Uuid::try_parse(s) {
                Ok(u) if u.hyphenated().to_string() == s => {
                    Ok(DbValue::Uuid(*u.as_bytes()))
                }
                _ => invalid("uuid"),
            }
        }
        DbValue::Array(a) => {
            let Value::Array(items) = v else {
                return invalid("array");
            };
            let template = a.items().iter().find(|i| !i.is_null());
            let items = items
                .iter()
                .map(|item| match (item, template) {
                    (Value::Null, _) => Ok(CanonicalValue::Null),
                    (_, Some(t)) => parse_like(t, item, mapping),
                    (_, None) => Err(ParseError::UnknownType),
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(DbValue::Array(
                ArrayValue::new(a.element_type().clone(), items).map_err(e)?,
            ))
        }
    }
}

/// `Y-MM-DD` with ISO expanded years (`+12345`, `-00044`); MySQL zero parts
/// allowed.
fn parse_date(s: &str) -> Result<CivilDate, ParseError> {
    let (sign, body) = match s.as_bytes().first() {
        Some(b'+') => (1, &s[1..]),
        Some(b'-') => (-1, &s[1..]),
        _ => (1, s),
    };
    let mut parts = body.splitn(3, '-');
    let (Some(y), Some(m), Some(d)) =
        (parts.next(), parts.next(), parts.next())
    else {
        return invalid("date");
    };
    let all_digits =
        |p: &str| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit());
    if !all_digits(y)
        || y.len() < 4
        || m.len() != 2
        || d.len() != 2
        || !all_digits(m)
        || !all_digits(d)
    {
        return invalid("date");
    }
    let year = sign * y.parse::<i32>().or_else(|_| invalid("date"))?;
    CivilDate::mysql(
        year,
        m.parse().expect("digits"),
        d.parse().expect("digits"),
    )
    .or_else(|_| invalid("date"))
}

/// `HH:MM:SS[.f]` (a time of day, up to 24:00:00) in microseconds.
fn parse_clock(s: &str) -> Result<i64, ParseError> {
    let us = parse_duration(s)?;
    if !(0..=MICROS_PER_DAY).contains(&us) {
        return invalid("time of day");
    }
    Ok(us)
}

/// `[-]H+:MM:SS[.f]` in microseconds.
fn parse_duration(s: &str) -> Result<i64, ParseError> {
    let (neg, body) = match s.strip_prefix('-') {
        Some(b) => (true, b),
        None => (false, s),
    };
    let (hms, frac) = match body.split_once('.') {
        Some((a, f)) => (a, Some(f)),
        None => (body, None),
    };
    let mut parts = hms.split(':');
    let (Some(h), Some(m), Some(sec), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return invalid("time");
    };
    let num =
        |p: &str, min_len: usize, max_len: usize| -> Result<i64, ParseError> {
            if p.len() < min_len
                || p.len() > max_len
                || !p.bytes().all(|b| b.is_ascii_digit())
            {
                return invalid("time");
            }
            p.parse().or_else(|_| invalid("time"))
        };
    let (h, m, sec) = (num(h, 2, 4)?, num(m, 2, 2)?, num(sec, 2, 2)?);
    if m > 59 || sec > 59 {
        return invalid("time");
    }
    let micros = match frac {
        None => 0,
        Some(f)
            if !f.is_empty()
                && f.len() <= 6
                && f.bytes().all(|b| b.is_ascii_digit()) =>
        {
            format!("{f:0<6}").parse::<i64>().expect("six digits")
        }
        Some(_) => return invalid("time"),
    };
    let us = ((h * 60 + m) * 60 + sec) * 1_000_000 + micros;
    Ok(if neg { -us } else { us })
}

/// RFC 3339 (`Z` or a `±HH:MM` offset) as microseconds since the epoch.
fn parse_rfc3339(s: &str) -> Result<i64, ParseError> {
    let (dt, offset_us) = if let Some(dt) = s.strip_suffix('Z') {
        (dt, 0)
    } else {
        let at = s
            .len()
            .checked_sub(6)
            .ok_or(ParseError::Invalid("timestamp"))?;
        let (dt, off) = s.split_at(at);
        let sign = match off.as_bytes()[0] {
            b'+' => 1,
            b'-' => -1,
            _ => return invalid("timestamp"),
        };
        let mins = parse_duration(&format!("{}:00", &off[1..]))? / 60_000_000;
        (dt, sign * mins * 60_000_000)
    };
    let (date, clock) =
        dt.split_once('T').ok_or(ParseError::Invalid("timestamp"))?;
    let date = parse_date(date)?;
    let days = date
        .days_since_epoch()
        .ok_or(ParseError::Invalid("timestamp"))?;
    let clock = parse_clock(clock)?;
    if clock == MICROS_PER_DAY {
        return invalid("timestamp");
    }
    days.checked_mul(MICROS_PER_DAY)
        .and_then(|d| d.checked_add(clock))
        .and_then(|us| us.checked_sub(offset_us))
        .ok_or(ParseError::Invalid("timestamp"))
}

/// Whether `width` is 64 bits (the integers a JavaScript number cannot hold).
pub fn is_wide(width: IntWidth) -> bool {
    width == IntWidth::W64
}

#[cfg(test)]
mod tests {
    use super::super::render::{RenderOpts, render_value};
    use super::super::{EnumValue, SetValue};
    use super::*;
    use serde_json::json;

    fn db(v: DbValue) -> CanonicalValue {
        CanonicalValue::Db(v)
    }

    /// Every value parses back from its own rendering, for both versions.
    #[test]
    fn a_rendering_parses_back_to_its_value() {
        let def: Vec<String> = ["a", "b"].map(String::from).to_vec();
        let values = vec![
            db(DbValue::int(i64::MIN, IntWidth::W64).unwrap()),
            db(DbValue::int(-7, IntWidth::W16).unwrap()),
            db(DbValue::uint(u64::MAX, IntWidth::W64).unwrap()),
            db(DbValue::Decimal(Decimal::parse("-0.50").unwrap())),
            db(DbValue::Decimal(Decimal::NaN)),
            db(DbValue::Float32(1.1)),
            db(DbValue::Float64(f64::NEG_INFINITY)),
            db(DbValue::Bool(true)),
            db(DbValue::Text("x".into())),
            db(DbValue::Binary(vec![0, 1])),
            db(DbValue::Bits(Bits::new(1, vec![1]).unwrap())),
            db(DbValue::Bits(Bits::new(12, vec![0x0f, 0xff]).unwrap())),
            db(DbValue::Enum(EnumValue::mysql(2, "b".into()).unwrap())),
            db(DbValue::Set(SetValue::from_bitmask(3, &def).unwrap())),
            db(DbValue::Json(JsonText::new("{\"a\": 1}".into()).unwrap())),
            db(DbValue::Date(DateValue::Civil(
                CivilDate::mysql(0, 0, 0).unwrap(),
            ))),
            db(DbValue::Date(DateValue::Civil(
                CivilDate::new(-44, 3, 15).unwrap(),
            ))),
            db(DbValue::Time(
                TimeValue::mysql(-3_000_500_000, Precision::Fixed(1)).unwrap(),
            )),
            db(DbValue::DateTime(
                DateTimeValue::civil(
                    CivilDate::new(2024, 1, 2).unwrap(),
                    5_000_000,
                    Precision::Fixed(3),
                )
                .unwrap(),
            )),
            db(DbValue::Timestamp(
                TimestampValue::instant(-1, Precision::Fixed(6)).unwrap(),
            )),
            db(DbValue::Timestamp(TimestampValue::Zero {
                precision: Precision::Fixed(0),
            })),
            db(DbValue::year(0).unwrap()),
            db(DbValue::Uuid([9; 16])),
            CanonicalValue::Bytes(vec![1, 2]),
            CanonicalValue::Document(Document::new(json!({"k": [1]}))),
        ];
        for mapping in TypeMapping::ALL {
            for v in &values {
                let r = render_value(v, RenderOpts::new(mapping)).unwrap();
                let back = parse_like(v, &r, mapping)
                    .unwrap_or_else(|e| panic!("{v:?} {mapping:?}: {e}"));
                assert_eq!(
                    format!("{back:?}"),
                    format!("{v:?}"),
                    "{mapping:?}"
                );
            }
        }
    }

    #[test]
    fn changed_values_parse_exactly_or_are_refused() {
        let int = db(DbValue::int(1, IntWidth::W32).unwrap());
        assert_eq!(
            parse_like(&int, &json!(5.0), TypeMapping::V2).unwrap(),
            db(DbValue::int(5, IntWidth::W32).unwrap())
        );
        assert!(parse_like(&int, &json!(5.5), TypeMapping::V2).is_err());
        assert!(
            parse_like(&int, &json!(4_294_967_296i64), TypeMapping::V2)
                .is_err()
        );
        assert!(parse_like(&int, &json!("7"), TypeMapping::V2).is_ok());
        assert!(parse_like(&int, &json!(true), TypeMapping::V2).is_err());
        let big = db(DbValue::int(1, IntWidth::W64).unwrap());
        // A wide value as a JS number above 2^53 is refused (not exact).
        assert!(
            parse_like(&big, &json!(9.007199254740994e15), TypeMapping::V2)
                .is_err()
        );
        assert_eq!(
            parse_like(&big, &json!("9007199254740993"), TypeMapping::V2)
                .unwrap(),
            db(DbValue::int(9_007_199_254_740_993, IntWidth::W64).unwrap())
        );
        let e = db(DbValue::Enum(EnumValue::mysql(1, "a".into()).unwrap()));
        assert_eq!(
            parse_like(&e, &json!("z"), TypeMapping::V2),
            Err(ParseError::NeedsDefinition("ENUM"))
        );
        assert_eq!(
            parse_like(&CanonicalValue::Null, &json!(1), TypeMapping::V2),
            Err(ParseError::UnknownType)
        );
        assert_eq!(
            parse_like(&int, &Value::Null, TypeMapping::V2).unwrap(),
            CanonicalValue::Null
        );
        let ts = db(DbValue::Timestamp(
            TimestampValue::instant(0, Precision::Fixed(0)).unwrap(),
        ));
        assert_eq!(
            parse_like(
                &ts,
                &json!("2024-01-01T02:00:00+02:00"),
                TypeMapping::V2
            )
            .unwrap(),
            db(DbValue::Timestamp(
                TimestampValue::instant(
                    1_704_067_200_000_000,
                    Precision::Fixed(0)
                )
                .unwrap()
            ))
        );
        assert!(
            parse_like(&ts, &json!("2024-01-01T00:00:00.5Z"), TypeMapping::V2)
                .is_err(),
            "finer than the precision"
        );
        let j = db(DbValue::Json(JsonText::new("1".into()).unwrap()));
        assert!(parse_like(&j, &json!("{bad"), TypeMapping::V2).is_err());
    }
}
