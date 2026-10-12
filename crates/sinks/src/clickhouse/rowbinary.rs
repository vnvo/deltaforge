//! Minimal ClickHouse RowBinary encoder for the v1 type set, from canonical
//! values. Every conversion is exact: a value its column type cannot hold
//! (out of range, a finer fraction or timestamp than the type keeps, a
//! MySQL zero date, an infinity, an untyped document or bytes, an unchanged
//! TOAST value) is an error naming the column and types, never the value.
//!
//! RowBinary rules used here:
//! - fixed integers: little-endian
//! - `String`: var-uint (LEB128) length prefix + bytes
//! - `Nullable(T)`: 1 byte (`1` = null, `0` = present) then, if present, the value
//! - `Decimal(P, S)`: the unscaled integer, little-endian, sized by P
//!   (P ≤ 9 → Int32, ≤ 18 → Int64, else Int128)
//! - `DateTime64(3)`: `Int64` milliseconds since epoch
//! - `Bool` / `UInt8`: 1 byte

use super::types::ChType;
use deltaforge_core::canonical::render::{
    date_text, datetime_text, decimal_text, time_text, timestamp_text,
    uuid_text,
};
use deltaforge_core::canonical::{
    CanonicalValue, DateTimeValue, DateValue, DbValue, Decimal, MICROS_PER_DAY,
    TimestampValue,
};

#[derive(Debug)]
pub enum EncodeError {
    /// `got` names the value's kind, never the value.
    Type { expected: String, got: String },
}

impl std::fmt::Display for EncodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EncodeError::Type { expected, got } => {
                write!(f, "cannot encode {got} as ClickHouse {expected}")
            }
        }
    }
}
impl std::error::Error for EncodeError {}

/// A value's kind for errors (never its content).
fn kind(v: &CanonicalValue) -> String {
    match v {
        CanonicalValue::Null => "SQL NULL".into(),
        CanonicalValue::Unchanged => {
            "an unchanged TOAST value (not sent by PostgreSQL)".into()
        }
        CanonicalValue::Opaque(o) => {
            format!("an opaque {} value", o.source_type.name)
        }
        CanonicalValue::Document(_) => "an untyped document".into(),
        CanonicalValue::Bytes(_) => "untyped bytes".into(),
        CanonicalValue::Db(db) => {
            let k = format!("{db:?}");
            let name = k
                .split(['(', ' ', '{'])
                .next()
                .unwrap_or("value")
                .to_string();
            format!("a {name} value")
        }
    }
}

fn type_err(ty: &ChType, v: &CanonicalValue, why: &str) -> EncodeError {
    EncodeError::Type {
        expected: format!("{} ({why})", ty.ddl_name()),
        got: kind(v),
    }
}

/// LEB128 var-uint (ClickHouse string-length prefix).
pub fn write_varuint(buf: &mut Vec<u8>, mut n: u64) {
    loop {
        let mut byte = (n & 0x7f) as u8;
        n >>= 7;
        if n != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if n == 0 {
            break;
        }
    }
}

pub fn encode_value(
    buf: &mut Vec<u8>,
    ty: &ChType,
    nullable: bool,
    v: &CanonicalValue,
) -> Result<(), EncodeError> {
    if nullable {
        if v.is_null() {
            buf.push(1);
            return Ok(());
        }
        buf.push(0);
    } else if v.is_null() {
        // non-null column with a null value → let the caller DLQ it
        return Err(EncodeError::Type {
            expected: format!("non-null {}", ty.ddl_name()),
            got: "SQL NULL".into(),
        });
    }
    let db = match v {
        CanonicalValue::Db(db) => db,
        CanonicalValue::Opaque(o) if *ty == ChType::String => {
            write_bytes(buf, o.text.as_bytes());
            return Ok(());
        }
        // A ClickHouse String holds bytes exactly.
        CanonicalValue::Bytes(b) if *ty == ChType::String => {
            write_bytes(buf, b);
            return Ok(());
        }
        _ => {
            return Err(type_err(
                ty,
                v,
                "only schema-typed values are encoded",
            ));
        }
    };
    let int = |lo: i128, hi: i128| -> Result<i128, EncodeError> {
        let n = match db {
            DbValue::Int { value, .. } => *value as i128,
            DbValue::UInt { value, .. } => *value as i128,
            DbValue::Year(y) => *y as i128,
            DbValue::Bool(b) => *b as i128,
            DbValue::Bits(b) if b.len() == 1 => b.to_u64() as i128,
            _ => return Err(type_err(ty, v, "not an integer")),
        };
        if (lo..=hi).contains(&n) {
            Ok(n)
        } else {
            Err(type_err(ty, v, "out of range"))
        }
    };
    match ty {
        ChType::Bool | ChType::UInt8 => {
            buf.push(int(0, u8::MAX as i128)? as u8)
        }
        ChType::Int8 => {
            buf.push(int(i8::MIN as i128, i8::MAX as i128)? as i8 as u8)
        }
        ChType::Int16 => buf.extend_from_slice(
            &(int(i16::MIN as i128, i16::MAX as i128)? as i16).to_le_bytes(),
        ),
        ChType::Int32 => buf.extend_from_slice(
            &(int(i32::MIN as i128, i32::MAX as i128)? as i32).to_le_bytes(),
        ),
        ChType::Int64 => buf.extend_from_slice(
            &(int(i64::MIN as i128, i64::MAX as i128)? as i64).to_le_bytes(),
        ),
        ChType::UInt64 => buf.extend_from_slice(
            &(int(0, u64::MAX as i128)? as u64).to_le_bytes(),
        ),
        ChType::Float64 => {
            let f = match db {
                DbValue::Float64(f) => *f,
                DbValue::Float32(f) => *f as f64,
                _ => return Err(type_err(ty, v, "not a float")),
            };
            buf.extend_from_slice(&f.to_le_bytes());
        }
        ChType::DateTime64_3 => {
            let micros = match db {
                DbValue::Timestamp(TimestampValue::Instant {
                    micros, ..
                }) => Some(*micros),
                DbValue::DateTime(DateTimeValue::Civil {
                    date,
                    micros_of_day,
                    ..
                }) => date
                    .days_since_epoch()
                    .and_then(|d| d.checked_mul(MICROS_PER_DAY))
                    .and_then(|d| d.checked_add(*micros_of_day)),
                DbValue::Date(DateValue::Civil(d)) => d
                    .days_since_epoch()
                    .and_then(|d| d.checked_mul(MICROS_PER_DAY)),
                _ => None,
            }
            .ok_or_else(|| {
                type_err(
                    ty,
                    v,
                    "not a date or instant (a MySQL zero value or an infinity)",
                )
            })?;
            if micros % 1000 != 0 {
                return Err(type_err(ty, v, "finer than milliseconds"));
            }
            buf.extend_from_slice(&(micros / 1000).to_le_bytes());
        }
        ChType::Decimal { p, s } => {
            let DbValue::Decimal(Decimal::Finite(text)) = db else {
                return Err(type_err(ty, v, "not a finite decimal"));
            };
            let unscaled =
                decimal_str_to_unscaled(text, *s, *p).ok_or_else(|| {
                    type_err(
                        ty,
                        v,
                        "more digits than the precision or scale keeps",
                    )
                })?;
            if *p <= 9 {
                buf.extend_from_slice(&(unscaled as i32).to_le_bytes());
            } else if *p <= 18 {
                buf.extend_from_slice(&(unscaled as i64).to_le_bytes());
            } else {
                buf.extend_from_slice(&unscaled.to_le_bytes()); // Int128, 16 bytes LE
            }
        }
        ChType::String => {
            let text: Vec<u8> = match db {
                DbValue::Text(s) => s.clone().into_bytes(),
                DbValue::Binary(b) => b.clone(),
                DbValue::Bits(b) => b.bytes().to_vec(),
                DbValue::Decimal(d) => decimal_text(d).as_bytes().to_vec(),
                DbValue::Int { value, .. } => value.to_string().into_bytes(),
                DbValue::UInt { value, .. } => value.to_string().into_bytes(),
                DbValue::Enum(e) => e.label.clone().into_bytes(),
                DbValue::Set(set) => set.members().join(",").into_bytes(),
                DbValue::Json(j) => j.as_str().as_bytes().to_vec(),
                DbValue::Uuid(u) => uuid_text(u).into_bytes(),
                DbValue::Date(d) => date_text(d).into_bytes(),
                DbValue::Time(t) => time_text(t).into_bytes(),
                DbValue::DateTime(dt) => datetime_text(dt).into_bytes(),
                DbValue::Timestamp(ts) => timestamp_text(ts).into_bytes(),
                _ => {
                    return Err(type_err(
                        ty,
                        v,
                        "no exact text for the value's type",
                    ));
                }
            };
            write_bytes(buf, &text);
        }
    }
    Ok(())
}

fn write_bytes(buf: &mut Vec<u8>, b: &[u8]) {
    write_varuint(buf, b.len() as u64);
    buf.extend_from_slice(b);
}

/// Canonical decimal text as the unscaled integer at scale `scale`, or
/// `None` when it has more fraction digits than `scale` or more digits than
/// `precision` (never truncated).
fn decimal_str_to_unscaled(
    text: &str,
    scale: u32,
    precision: u32,
) -> Option<i128> {
    let neg = text.starts_with('-');
    let t = text.trim_start_matches('-');
    let (int_part, frac_part) = match t.split_once('.') {
        Some((i, f)) => (i, f),
        None => (t, ""),
    };
    let frac_part = frac_part.trim_end_matches('0');
    if frac_part.len() > scale as usize {
        return None;
    }
    let int_part = int_part.trim_start_matches('0');
    if int_part.len() + scale as usize > precision as usize {
        return None;
    }
    let mut frac = frac_part.to_string();
    while (frac.len() as u32) < scale {
        frac.push('0');
    }
    let combined = format!("{int_part}{frac}");
    let mut n: i128 = if combined.is_empty() {
        0
    } else {
        combined.parse().ok()?
    };
    if neg {
        n = -n;
    }
    Some(n)
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltaforge_core::canonical::{Document, IntWidth, Precision};

    fn int(v: i64, w: IntWidth) -> CanonicalValue {
        CanonicalValue::Db(DbValue::int(v, w).unwrap())
    }

    #[test]
    fn encodes_int64_le() {
        let mut b = Vec::new();
        encode_value(&mut b, &ChType::Int64, false, &int(1, IntWidth::W64))
            .unwrap();
        assert_eq!(b, 1i64.to_le_bytes().to_vec());
    }

    #[test]
    fn encodes_string_with_varuint_len() {
        let mut b = Vec::new();
        encode_value(
            &mut b,
            &ChType::String,
            false,
            &CanonicalValue::Db(DbValue::Text("hi".into())),
        )
        .unwrap();
        assert_eq!(b, vec![2u8, b'h', b'i']);
    }

    #[test]
    fn nullable_null_is_single_one_byte() {
        let mut b = Vec::new();
        encode_value(&mut b, &ChType::Int64, true, &CanonicalValue::Null)
            .unwrap();
        assert_eq!(b, vec![1u8]);
    }

    #[test]
    fn nullable_present_prefixes_zero() {
        let mut b = Vec::new();
        encode_value(&mut b, &ChType::Int64, true, &int(5, IntWidth::W64))
            .unwrap();
        let mut want = vec![0u8];
        want.extend_from_slice(&5i64.to_le_bytes());
        assert_eq!(b, want);
    }

    #[test]
    fn non_null_column_with_null_errors() {
        let mut b = Vec::new();
        assert!(
            encode_value(&mut b, &ChType::Int64, false, &CanonicalValue::Null)
                .is_err()
        );
    }

    #[test]
    fn decimal_encodes_unscaled_int64() {
        let mut b = Vec::new();
        let d = CanonicalValue::Db(DbValue::Decimal(
            Decimal::parse("12.34").unwrap(),
        ));
        encode_value(&mut b, &ChType::Decimal { p: 12, s: 2 }, false, &d)
            .unwrap();
        assert_eq!(b, 1234i64.to_le_bytes().to_vec());
    }

    #[test]
    fn decimal_negative_and_short_frac() {
        assert_eq!(decimal_str_to_unscaled("-1.2", 3, 10), Some(-1200));
        assert_eq!(decimal_str_to_unscaled("5", 2, 10), Some(500));
        assert_eq!(
            decimal_str_to_unscaled("1.50", 1, 10),
            Some(15),
            "trailing zeros are exact"
        );
    }

    #[test]
    fn inexact_values_fail_instead_of_wrapping_or_truncating() {
        let mut b = Vec::new();
        assert!(
            decimal_str_to_unscaled("1.234", 2, 10).is_none(),
            "never truncated"
        );
        assert!(
            decimal_str_to_unscaled("123", 0, 2).is_none(),
            "beyond the precision"
        );
        assert!(
            encode_value(&mut b, &ChType::UInt8, false, &int(-1, IntWidth::W8))
                .is_err(),
            "never wrapped"
        );
        assert!(
            encode_value(&mut b, &ChType::Int8, false, &int(-1, IntWidth::W8))
                .is_ok()
        );
        let ts = CanonicalValue::Db(DbValue::Timestamp(
            TimestampValue::instant(1_500, Precision::Fixed(6)).unwrap(),
        ));
        assert!(
            encode_value(&mut b, &ChType::DateTime64_3, false, &ts).is_err(),
            "finer than ms"
        );
        let zero =
            CanonicalValue::Db(DbValue::Timestamp(TimestampValue::Zero {
                precision: Precision::Fixed(0),
            }));
        assert!(
            encode_value(&mut b, &ChType::DateTime64_3, false, &zero).is_err()
        );
        for untyped in [
            CanonicalValue::Unchanged,
            CanonicalValue::Document(Document::new(serde_json::json!(1))),
        ] {
            assert!(
                encode_value(&mut b, &ChType::String, true, &untyped).is_err()
            );
        }
        assert!(
            encode_value(
                &mut b,
                &ChType::String,
                true,
                &CanonicalValue::Bytes(vec![0, 255])
            )
            .is_ok()
        );
        assert!(
            encode_value(
                &mut b,
                &ChType::Int64,
                true,
                &CanonicalValue::Bytes(vec![1])
            )
            .is_err()
        );
        let err = encode_value(
            &mut b,
            &ChType::Int16,
            false,
            &CanonicalValue::Db(DbValue::Text("secret".into())),
        )
        .unwrap_err()
        .to_string();
        assert!(!err.contains("secret"), "{err}");
    }

    #[test]
    fn varuint_multibyte() {
        let mut b = Vec::new();
        write_varuint(&mut b, 300);
        assert_eq!(b, vec![0xAC, 0x02]);
    }
}
