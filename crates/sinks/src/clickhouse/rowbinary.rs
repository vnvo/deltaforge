//! Minimal ClickHouse RowBinary encoder for the v1 type set.
//!
//! RowBinary rules used here:
//! - fixed integers: little-endian
//! - `String`: var-uint (LEB128) length prefix + UTF-8 bytes
//! - `Nullable(T)`: 1 byte (`1` = null, `0` = present) then, if present, the value
//! - `Decimal(P, S)`: the unscaled integer, little-endian, sized by P
//!   (P ≤ 9 → Int32, ≤ 18 → Int64, else Int128)
//! - `DateTime64(3)`: `Int64` milliseconds since epoch
//! - `Bool` / `UInt8`: 1 byte

use super::types::ChType;
use serde_json::Value;

#[derive(Debug)]
pub enum EncodeError {
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

fn type_err(ty: &ChType, v: &Value) -> EncodeError {
    EncodeError::Type {
        expected: ty.ddl_name(),
        got: v.to_string(),
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
    v: &Value,
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
            got: "null".into(),
        });
    }

    match ty {
        ChType::Bool | ChType::UInt8 => buf.push(as_i128(v, ty)? as u8),
        ChType::Int16 => buf.extend_from_slice(&(as_i128(v, ty)? as i16).to_le_bytes()),
        ChType::Int32 => buf.extend_from_slice(&(as_i128(v, ty)? as i32).to_le_bytes()),
        ChType::Int64 => buf.extend_from_slice(&(as_i128(v, ty)? as i64).to_le_bytes()),
        ChType::UInt64 => buf.extend_from_slice(&(as_i128(v, ty)? as u64).to_le_bytes()),
        ChType::Float64 => {
            let f = v.as_f64().ok_or_else(|| type_err(ty, v))?;
            buf.extend_from_slice(&f.to_le_bytes());
        }
        ChType::DateTime64_3 => {
            buf.extend_from_slice(&datetime_millis(v)?.to_le_bytes());
        }
        ChType::Decimal { p, s } => encode_decimal(buf, *p, *s, v)?,
        ChType::String => {
            let s = match v {
                Value::String(s) => s.clone(),
                other => other.to_string(), // numbers/bools/objects → JSON text
            };
            write_varuint(buf, s.len() as u64);
            buf.extend_from_slice(s.as_bytes());
        }
    }
    Ok(())
}

fn as_i128(v: &Value, ty: &ChType) -> Result<i128, EncodeError> {
    v.as_i64()
        .map(|n| n as i128)
        .or_else(|| v.as_u64().map(|n| n as i128))
        .or_else(|| v.as_bool().map(|b| b as i128))
        .ok_or_else(|| type_err(ty, v))
}

fn datetime_millis(v: &Value) -> Result<i64, EncodeError> {
    if let Some(n) = v.as_i64() {
        return Ok(n);
    }
    if let Some(s) = v.as_str() {
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
            return Ok(dt.timestamp_millis());
        }
    }
    Err(EncodeError::Type {
        expected: "DateTime64(3)".into(),
        got: v.to_string(),
    })
}

fn encode_decimal(buf: &mut Vec<u8>, p: u32, s: u32, v: &Value) -> Result<(), EncodeError> {
    let text = match v {
        Value::String(t) => t.clone(),
        Value::Number(n) => n.to_string(),
        _ => {
            return Err(EncodeError::Type {
                expected: format!("Decimal({p}, {s})"),
                got: v.to_string(),
            })
        }
    };
    let unscaled = decimal_str_to_unscaled(&text, s).ok_or_else(|| EncodeError::Type {
        expected: format!("Decimal({p}, {s})"),
        got: text.clone(),
    })?;
    if p <= 9 {
        buf.extend_from_slice(&(unscaled as i32).to_le_bytes());
    } else if p <= 18 {
        buf.extend_from_slice(&(unscaled as i64).to_le_bytes());
    } else {
        buf.extend_from_slice(&unscaled.to_le_bytes()); // Int128, 16 bytes LE
    }
    Ok(())
}

/// `"12.34"`, scale 2 → 1234 ; `"-1.2"`, scale 3 → -1200.
fn decimal_str_to_unscaled(text: &str, scale: u32) -> Option<i128> {
    let neg = text.starts_with('-');
    let t = text.trim_start_matches(['-', '+']);
    let (int_part, frac_part) = match t.split_once('.') {
        Some((i, f)) => (i, f),
        None => (t, ""),
    };
    let mut frac = frac_part.to_string();
    if frac.len() > scale as usize {
        frac.truncate(scale as usize);
    }
    while (frac.len() as u32) < scale {
        frac.push('0');
    }
    let combined = format!("{int_part}{frac}");
    let mut n: i128 = combined.parse().ok()?;
    if neg {
        n = -n;
    }
    Some(n)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn encodes_int64_le() {
        let mut b = Vec::new();
        encode_value(&mut b, &ChType::Int64, false, &json!(1)).unwrap();
        assert_eq!(b, 1i64.to_le_bytes().to_vec());
    }

    #[test]
    fn encodes_string_with_varuint_len() {
        let mut b = Vec::new();
        encode_value(&mut b, &ChType::String, false, &json!("hi")).unwrap();
        assert_eq!(b, vec![2u8, b'h', b'i']);
    }

    #[test]
    fn nullable_null_is_single_one_byte() {
        let mut b = Vec::new();
        encode_value(&mut b, &ChType::Int64, true, &json!(null)).unwrap();
        assert_eq!(b, vec![1u8]);
    }

    #[test]
    fn nullable_present_prefixes_zero() {
        let mut b = Vec::new();
        encode_value(&mut b, &ChType::Int64, true, &json!(5)).unwrap();
        let mut want = vec![0u8];
        want.extend_from_slice(&5i64.to_le_bytes());
        assert_eq!(b, want);
    }

    #[test]
    fn non_null_column_with_null_errors() {
        let mut b = Vec::new();
        assert!(encode_value(&mut b, &ChType::Int64, false, &json!(null)).is_err());
    }

    #[test]
    fn decimal_encodes_unscaled_int64() {
        let mut b = Vec::new();
        encode_value(&mut b, &ChType::Decimal { p: 12, s: 2 }, false, &json!("12.34")).unwrap();
        assert_eq!(b, 1234i64.to_le_bytes().to_vec());
    }

    #[test]
    fn decimal_negative_and_short_frac() {
        assert_eq!(decimal_str_to_unscaled("-1.2", 3), Some(-1200));
        assert_eq!(decimal_str_to_unscaled("5", 2), Some(500));
    }

    #[test]
    fn varuint_multibyte() {
        let mut b = Vec::new();
        write_varuint(&mut b, 300);
        assert_eq!(b, vec![0xAC, 0x02]);
    }
}
