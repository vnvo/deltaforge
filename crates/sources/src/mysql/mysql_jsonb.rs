//! MySQL binary JSON (JSONB, from the binlog) rendered as the text MySQL
//! itself returns for the document (`CAST(j AS CHAR)`), so a CDC value and a
//! snapshot value of the same document are the same text byte for byte.
//!
//! MySQL's rules (measured on 8.4):
//! - objects `{"k": v, "k2": v2}` in stored key order (MySQL sorts keys by
//!   length, then bytes), arrays `[a, b]`;
//! - strings escape `"`, `\`, `\b`, `\f`, `\n`, `\r`, `\t`, and other control
//!   characters as `\u00xx` (lowercase hex); everything else is raw UTF-8;
//! - integers exact; doubles as their shortest round-trip digits, in fixed
//!   notation when the decimal exponent is -15 < e < 16 (or the digits have
//!   a fractional part beyond that), otherwise as `<d>[.<ddd>]e<exp>`; a
//!   fixed integral double gets `.0`;
//! - opaque values: DECIMAL as its digits with its scale; DATE
//!   `YYYY-MM-DD`; TIME `[-]HH:MM:SS.ffffff`; DATETIME and TIMESTAMP
//!   `YYYY-MM-DD HH:MM:SS.ffffff` (quoted); any other type
//!   `"base64:type<N>:<base64>"`.

use std::fmt::Write;

use base64::prelude::*;
use mysql_common::binlog::jsonb;
use mysql_common::constants::ColumnType;
use mysql_common::io::ParseBuf;
use mysql_common::proto::MyDeserialize;

/// A JSONB value that does not parse.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("malformed binary JSON: {0}")]
pub struct JsonbError(pub String);

/// Render binary JSON as MySQL's text. A zero-length value is MySQL's JSON
/// `null` (an empty JSON column value).
pub fn jsonb_to_mysql_text(bytes: &[u8]) -> Result<String, JsonbError> {
    if bytes.is_empty() {
        return Ok("null".to_string());
    }
    let mut pb = ParseBuf(bytes);
    let v = jsonb::Value::deserialize((), &mut pb)
        .map_err(|e| JsonbError(e.to_string()))?;
    let mut out = String::with_capacity(bytes.len());
    write_value(&mut out, &v)?;
    Ok(out)
}

fn io_err(e: std::io::Error) -> JsonbError {
    JsonbError(e.to_string())
}

fn write_value(
    out: &mut String,
    v: &jsonb::Value<'_>,
) -> Result<(), JsonbError> {
    use jsonb::Value as V;
    match v {
        V::Null => out.push_str("null"),
        V::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        V::I16(x) => write!(out, "{x}").unwrap(),
        V::U16(x) => write!(out, "{x}").unwrap(),
        V::I32(x) => write!(out, "{x}").unwrap(),
        V::U32(x) => write!(out, "{x}").unwrap(),
        V::I64(x) => write!(out, "{x}").unwrap(),
        V::U64(x) => write!(out, "{x}").unwrap(),
        V::F64(x) => out.push_str(&mysql_double_text(*x)),
        V::String(s) => write_string(
            out,
            std::str::from_utf8(s.str_raw())
                .map_err(|_| JsonbError("string is not UTF-8".into()))?,
        ),
        V::SmallArray(a) => {
            out.push('[');
            for (i, item) in a.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                write_value(out, &item.map_err(io_err)?)?;
            }
            out.push(']');
        }
        V::LargeArray(a) => {
            out.push('[');
            for (i, item) in a.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                write_value(out, &item.map_err(io_err)?)?;
            }
            out.push(']');
        }
        V::SmallObject(o) => {
            out.push('{');
            for (i, entry) in o.iter().enumerate() {
                let (k, v) = entry.map_err(io_err)?;
                write_entry(out, i, k.value_raw(), &v)?;
            }
            out.push('}');
        }
        V::LargeObject(o) => {
            out.push('{');
            for (i, entry) in o.iter().enumerate() {
                let (k, v) = entry.map_err(io_err)?;
                write_entry(out, i, k.value_raw(), &v)?;
            }
            out.push('}');
        }
        V::Opaque(o) => write_opaque(out, o.value_type(), o.data_raw())?,
    }
    Ok(())
}

fn write_entry(
    out: &mut String,
    i: usize,
    key: &[u8],
    v: &jsonb::Value<'_>,
) -> Result<(), JsonbError> {
    if i > 0 {
        out.push_str(", ");
    }
    write_string(
        out,
        std::str::from_utf8(key)
            .map_err(|_| JsonbError("key is not UTF-8".into()))?,
    );
    out.push_str(": ");
    write_value(out, v)
}

fn write_string(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                write!(out, "\\u{:04x}", c as u32).unwrap();
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// MySQL's text for a JSON double.
pub fn mysql_double_text(x: f64) -> String {
    if x == 0.0 {
        return if x.is_sign_negative() { "-0.0" } else { "0.0" }.to_string();
    }
    // Shortest round-trip digits and the decimal exponent: `d.ddde<exp>`.
    let sci = format!("{:e}", x.abs());
    let (mantissa, exp) = sci.split_once('e').expect("{:e} has an exponent");
    let exp: i32 = exp.parse().expect("{:e} exponent is an integer");
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let len = digits.len() as i32;
    // The decimal point sits after `decpt` digits.
    let decpt = exp + 1;
    let sign = if x < 0.0 { "-" } else { "" };
    let fixed = (-14..=15).contains(&decpt) || (decpt > 15 && decpt < len);
    if !fixed {
        let (first, rest) = digits.split_at(1);
        return if rest.is_empty() {
            format!("{sign}{first}e{exp}")
        } else {
            format!("{sign}{first}.{rest}e{exp}")
        };
    }
    if decpt <= 0 {
        format!("{sign}0.{}{digits}", "0".repeat((-decpt) as usize))
    } else if decpt >= len {
        format!("{sign}{digits}{}.0", "0".repeat((decpt - len) as usize))
    } else {
        let (int, frac) = digits.split_at(decpt as usize);
        format!("{sign}{int}.{frac}")
    }
}

fn write_opaque(
    out: &mut String,
    ty: ColumnType,
    data: &[u8],
) -> Result<(), JsonbError> {
    let packed = || -> Result<i64, JsonbError> {
        data.first_chunk::<8>()
            .map(|b| i64::from_le_bytes(*b))
            .ok_or_else(|| JsonbError("short temporal value".into()))
    };
    match ty {
        ColumnType::MYSQL_TYPE_NEWDECIMAL => {
            if data.len() < 2 {
                return Err(JsonbError("short decimal value".into()));
            }
            let (precision, scale) = (data[0] as usize, data[1] as usize);
            let bin = data[2..].to_vec();
            let text = mysql_binlog_connector_rust::column::column_value::ColumnValue::parse_decimal(
                &mut std::io::Cursor::new(&bin),
                precision,
                scale,
            )
            .map_err(|e| JsonbError(format!("decimal: {e:?}")))?;
            let canonical = deltaforge_core::canonical::Decimal::parse(&text)
                .map_err(|e| JsonbError(e.to_string()))?;
            out.push_str(deltaforge_core::canonical::render::decimal_text(
                &canonical,
            ));
        }
        ColumnType::MYSQL_TYPE_DATE => {
            let t = Unpacked::datetime(packed()?);
            write!(out, "\"{:04}-{:02}-{:02}\"", t.year, t.month, t.day)
                .unwrap();
        }
        ColumnType::MYSQL_TYPE_TIME => {
            // MySQL renders a JSON TIME through its date-time unpacking
            // (hours modulo 32), so do the same.
            let t = Unpacked::datetime(packed()?);
            write!(
                out,
                "\"{}{:02}:{:02}:{:02}.{:06}\"",
                if t.negative { "-" } else { "" },
                t.hour,
                t.minute,
                t.second,
                t.micros
            )
            .unwrap();
        }
        ColumnType::MYSQL_TYPE_DATETIME | ColumnType::MYSQL_TYPE_TIMESTAMP => {
            let t = Unpacked::datetime(packed()?);
            write!(
                out,
                "\"{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}\"",
                t.year, t.month, t.day, t.hour, t.minute, t.second, t.micros
            )
            .unwrap();
        }
        other => {
            write!(
                out,
                "\"base64:type{}:{}\"",
                other as u8,
                BASE64_STANDARD.encode(data)
            )
            .unwrap();
        }
    }
    Ok(())
}

/// MySQL's packed temporal value (`TIME_from_longlong_datetime_packed`).
struct Unpacked {
    negative: bool,
    year: i64,
    month: i64,
    day: i64,
    hour: i64,
    minute: i64,
    second: i64,
    micros: i64,
}

impl Unpacked {
    fn datetime(packed: i64) -> Unpacked {
        let negative = packed < 0;
        let tmp = packed.unsigned_abs() as i64;
        let micros = tmp % (1 << 24);
        let ymdhms = tmp >> 24;
        let ymd = ymdhms >> 17;
        let ym = ymd >> 5;
        let hms = ymdhms % (1 << 17);
        Unpacked {
            negative,
            year: ym / 13,
            month: ym % 13,
            day: ymd % (1 << 5),
            hour: hms >> 12,
            minute: (hms >> 6) % (1 << 6),
            second: hms % (1 << 6),
            micros,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// MySQL 8.4's text for these doubles (measured with `JSON_ARRAY`).
    #[test]
    fn doubles_match_mysql_text() {
        for (x, want) in [
            (0.1, "0.1"),
            (1.0, "1.0"),
            (100.0, "100.0"),
            (1e14, "100000000000000.0"),
            (1e15, "1e15"),
            (1.5e15, "1.5e15"),
            (123456789012345.0, "123456789012345.0"),
            (1234567890123456.0, "1.234567890123456e15"),
            (1234567890123456.8, "1234567890123456.8"),
            (12345678901234567.0, "1.2345678901234568e16"),
            (999999999999999.9, "999999999999999.9"),
            (0.1 + 0.2, "0.30000000000000004"),
            (123456789012345678901.0, "1.2345678901234568e20"),
            (1e300, "1e300"),
            (1.23e22, "1.23e22"),
            (1.7976931348623157e308, "1.7976931348623157e308"),
            (5e-324, "5e-324"),
            (2.2250738585072014e-308, "2.2250738585072014e-308"),
            (1e-7, "0.0000001"),
            (1.5e-7, "0.00000015"),
            (1e-15, "0.000000000000001"),
            (1.5e-15, "0.0000000000000015"),
            (1e-16, "1e-16"),
            (1.25e-16, "1.25e-16"),
            (1.2345678901234568e-15, "0.0000000000000012345678901234568"),
            (-1.5e-300, "-1.5e-300"),
            (-1234.5, "-1234.5"),
            (0.0, "0.0"),
            (-0.0, "-0.0"),
        ] {
            assert_eq!(mysql_double_text(x), want, "{x:e}");
        }
    }

    #[test]
    fn empty_value_is_json_null() {
        assert_eq!(jsonb_to_mysql_text(&[]).unwrap(), "null");
    }

    #[test]
    fn strings_escape_like_mysql() {
        let mut s = String::new();
        write_string(
            &mut s,
            "ü\"\\\n\t\u{8}\u{c}\r/\u{1}\u{1f}\u{7f}\u{80}\u{2028}",
        );
        assert_eq!(
            s,
            "\"ü\\\"\\\\\\n\\t\\b\\f\\r/\\u0001\\u001f\u{7f}\u{80}\u{2028}\""
        );
    }

    #[test]
    fn packed_time_unpacks_like_mysql() {
        // -838:59:59 packs past the 5 hour bits; MySQL shows hour 6.
        let hms: i64 = (838 << 12) | (59 << 6) | 59;
        let t = Unpacked::datetime(-(hms << 24));
        assert!(t.negative);
        assert_eq!((t.hour, t.minute, t.second), (6, 59, 59));
    }
}
