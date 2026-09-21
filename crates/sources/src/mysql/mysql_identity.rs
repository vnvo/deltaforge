//! Schema-directed identity extraction for MySQL.
//!
//! Interpretation is chosen from the **declared column type**, never from the
//! runtime value variant or a JSON/display rendering. A `BINARY` column stays
//! `Bytes` even when its bytes are valid UTF-8; signedness comes from the
//! column metadata; decimals become sign + unscaled integer + scale (never
//! display text); dates/times become fixed source-semantic integers.
//!
//! MySQL has no native UUID or boolean type: UUIDs live in `CHAR(36)` (→ Text)
//! or `BINARY(16)` (→ Bytes), and booleans are `TINYINT` (→ Int/UInt).
//! Unsupported identity types (float/double/set/json/bit/geometry/…) are
//! rejected before allocation.

use deltaforge_core::{IdentityKind, TemporalKind};
use mysql_async::Value;

use super::MySqlColumn;
use crate::snapshot_event_id::OwnedIdentityCell;

/// Errors mapping a MySQL column/value to a canonical identity cell.
#[derive(Debug, thiserror::Error)]
pub enum MysqlIdentityError {
    /// The column's declared type is not supported as a stable identity column.
    #[error(
        "column {column:?}: MySQL type {data_type:?} is not supported as a \
         stable identity column"
    )]
    UnsupportedType {
        /// Column name.
        column: String,
        /// Declared data type.
        data_type: String,
    },
    /// A runtime value did not match its declared identity type.
    #[error(
        "column {column:?}: value incompatible with identity type ({detail})"
    )]
    ValueMismatch {
        /// Column name.
        column: String,
        /// Detail.
        detail: String,
    },
}

/// Map a MySQL column's declared type to its canonical [`IdentityKind`], or
/// reject it as unsupported for identity.
pub fn mysql_identity_kind(
    col: &MySqlColumn,
) -> Result<IdentityKind, MysqlIdentityError> {
    let dt = col.data_type.to_lowercase();
    let kind = match dt.as_str() {
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint"
        | "year" => {
            if col.is_unsigned() {
                IdentityKind::UInt
            } else {
                IdentityKind::Int
            }
        }
        "decimal" | "numeric" | "dec" | "fixed" => IdentityKind::Decimal,
        "char" | "varchar" | "tinytext" | "text" | "mediumtext"
        | "longtext" => IdentityKind::Text,
        "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob"
        | "longblob" => IdentityKind::Bytes,
        "date" | "datetime" | "timestamp" | "time" => IdentityKind::DateTime,
        "enum" => IdentityKind::Enum,
        _ => {
            return Err(MysqlIdentityError::UnsupportedType {
                column: col.name.clone(),
                data_type: col.data_type.clone(),
            });
        }
    };
    Ok(kind)
}

fn temporal_kind(data_type: &str) -> TemporalKind {
    match data_type {
        "date" => TemporalKind::Date,
        "time" => TemporalKind::Time,
        // MySQL `timestamp` is stored/retrieved in UTC; `datetime` is wall-clock.
        "timestamp" => TemporalKind::TimestampTz,
        _ => TemporalKind::Timestamp, // datetime
    }
}

fn mismatch(col: &MySqlColumn, detail: &str) -> MysqlIdentityError {
    MysqlIdentityError::ValueMismatch {
        column: col.name.clone(),
        detail: detail.to_string(),
    }
}

/// Extract the canonical identity cell for one column value, directed by the
/// column's declared type. `NULL` yields `Null(kind)` carrying the declared
/// type tag.
pub fn mysql_identity_cell(
    col: &MySqlColumn,
    val: &Value,
) -> Result<OwnedIdentityCell, MysqlIdentityError> {
    let kind = mysql_identity_kind(col)?;
    if matches!(val, Value::NULL) {
        return Ok(OwnedIdentityCell::Null(kind));
    }
    let cell = match kind {
        IdentityKind::Int => OwnedIdentityCell::Int(as_i64(col, val)?),
        IdentityKind::UInt => OwnedIdentityCell::UInt(as_u64(col, val)?),
        IdentityKind::Text => OwnedIdentityCell::Text(as_text(col, val)?),
        IdentityKind::Bytes => OwnedIdentityCell::Bytes(as_bytes(col, val)?),
        IdentityKind::Decimal => {
            let (negative, unscaled, scale) = as_decimal(col, val)?;
            OwnedIdentityCell::Decimal {
                negative,
                unscaled,
                scale,
            }
        }
        IdentityKind::DateTime => OwnedIdentityCell::DateTime {
            kind: temporal_kind(&col.data_type.to_lowercase()),
            value: as_temporal_int(col, val)?,
        },
        IdentityKind::Enum => OwnedIdentityCell::Enum {
            // MySQL enums are anonymous; the full column type (e.g.
            // "enum('a','b')") is the stable discriminator.
            enum_type: col.column_type.clone(),
            label: as_text(col, val)?,
        },
        // MySQL never maps a column to these kinds.
        IdentityKind::Uuid | IdentityKind::Bool => {
            return Err(mismatch(col, "unexpected kind for MySQL"));
        }
    };
    Ok(cell)
}

fn as_i64(col: &MySqlColumn, val: &Value) -> Result<i64, MysqlIdentityError> {
    match val {
        Value::Int(n) => Ok(*n),
        Value::UInt(n) => i64::try_from(*n)
            .map_err(|_| mismatch(col, "unsigned value exceeds i64")),
        Value::Bytes(b) => std::str::from_utf8(b)
            .ok()
            .and_then(|s| s.trim().parse::<i64>().ok())
            .ok_or_else(|| mismatch(col, "not an integer")),
        _ => Err(mismatch(col, "not an integer")),
    }
}

fn as_u64(col: &MySqlColumn, val: &Value) -> Result<u64, MysqlIdentityError> {
    match val {
        Value::UInt(n) => Ok(*n),
        Value::Int(n) => u64::try_from(*n)
            .map_err(|_| mismatch(col, "negative value in unsigned column")),
        Value::Bytes(b) => std::str::from_utf8(b)
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .ok_or_else(|| mismatch(col, "not an unsigned integer")),
        _ => Err(mismatch(col, "not an unsigned integer")),
    }
}

fn as_text(
    col: &MySqlColumn,
    val: &Value,
) -> Result<String, MysqlIdentityError> {
    match val {
        Value::Bytes(b) => String::from_utf8(b.clone())
            .map_err(|_| mismatch(col, "text column value is not valid UTF-8")),
        _ => Err(mismatch(col, "not a text value")),
    }
}

fn as_bytes(
    col: &MySqlColumn,
    val: &Value,
) -> Result<Vec<u8>, MysqlIdentityError> {
    match val {
        Value::Bytes(b) => Ok(b.clone()),
        _ => Err(mismatch(col, "not a byte value")),
    }
}

/// Parse a MySQL decimal string (e.g. `"-123.45"`) into
/// `(negative, big-endian unscaled magnitude, scale)`.
fn as_decimal(
    col: &MySqlColumn,
    val: &Value,
) -> Result<(bool, Vec<u8>, i32), MysqlIdentityError> {
    let s = match val {
        Value::Bytes(b) => std::str::from_utf8(b)
            .map_err(|_| mismatch(col, "decimal not UTF-8"))?
            .trim()
            .to_string(),
        Value::Int(n) => n.to_string(),
        Value::UInt(n) => n.to_string(),
        _ => return Err(mismatch(col, "not a decimal")),
    };
    let (negative, rest) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s.strip_prefix('+').unwrap_or(&s)),
    };
    let (int_part, frac_part) = match rest.split_once('.') {
        Some((i, f)) => (i, f),
        None => (rest, ""),
    };
    if !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(mismatch(col, "malformed decimal"));
    }
    let mut digits: Vec<u8> =
        Vec::with_capacity(int_part.len() + frac_part.len());
    digits.extend(int_part.bytes().map(|b| b - b'0'));
    digits.extend(frac_part.bytes().map(|b| b - b'0'));
    let unscaled = decimal_digits_to_be_bytes(&digits);
    Ok((negative, unscaled, frac_part.len() as i32))
}

/// Convert base-10 digits (most significant first) to a big-endian base-256
/// magnitude, with leading zero bytes stripped.
fn decimal_digits_to_be_bytes(digits: &[u8]) -> Vec<u8> {
    let mut bytes: Vec<u8> = Vec::new();
    for &d in digits {
        let mut carry = d as u16;
        for b in bytes.iter_mut().rev() {
            let v = (*b as u16) * 10 + carry;
            *b = (v & 0xff) as u8;
            carry = v >> 8;
        }
        while carry > 0 {
            bytes.insert(0, (carry & 0xff) as u8);
            carry >>= 8;
        }
    }
    let first = bytes.iter().position(|&b| b != 0).unwrap_or(bytes.len());
    bytes[first..].to_vec()
}

/// Days from the civil calendar epoch (1970-01-01), Howard Hinnant's algorithm.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn as_temporal_int(
    col: &MySqlColumn,
    val: &Value,
) -> Result<i64, MysqlIdentityError> {
    match val {
        Value::Date(y, mo, d, h, mi, s, us) => {
            let days = days_from_civil(*y as i64, *mo as i64, *d as i64);
            if col.data_type.eq_ignore_ascii_case("date") {
                Ok(days)
            } else {
                // datetime / timestamp → microseconds from the epoch.
                let secs = days * 86_400
                    + *h as i64 * 3600
                    + *mi as i64 * 60
                    + *s as i64;
                Ok(secs * 1_000_000 + *us as i64)
            }
        }
        Value::Time(neg, days, h, mi, s, us) => {
            let micros = ((*days as i64 * 24 + *h as i64) * 3600
                + *mi as i64 * 60
                + *s as i64)
                * 1_000_000
                + *us as i64;
            Ok(if *neg { -micros } else { micros })
        }
        _ => Err(mismatch(col, "not a date/time value")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, column_type: &str, data_type: &str) -> MySqlColumn {
        MySqlColumn::new(name, column_type, data_type, false, 1)
    }

    #[test]
    fn kind_signedness_from_metadata() {
        let signed = col("a", "int(11)", "int");
        let unsigned = col("b", "int(11) unsigned", "int");
        assert_eq!(mysql_identity_kind(&signed).unwrap(), IdentityKind::Int);
        assert_eq!(mysql_identity_kind(&unsigned).unwrap(), IdentityKind::UInt);
    }

    #[test]
    fn binary_column_stays_bytes_even_if_utf8() {
        // "42" is valid UTF-8, but a BINARY column must remain Bytes.
        let c = col("id", "binary(2)", "binary");
        let cell =
            mysql_identity_cell(&c, &Value::Bytes(b"42".to_vec())).unwrap();
        assert_eq!(cell, OwnedIdentityCell::Bytes(b"42".to_vec()));
    }

    #[test]
    fn text_column_is_text() {
        let c = col("id", "varchar(8)", "varchar");
        let cell =
            mysql_identity_cell(&c, &Value::Bytes(b"42".to_vec())).unwrap();
        assert_eq!(cell, OwnedIdentityCell::Text("42".into()));
    }

    #[test]
    fn unsupported_type_is_rejected() {
        let c = col("g", "geometry", "geometry");
        assert!(matches!(
            mysql_identity_kind(&c),
            Err(MysqlIdentityError::UnsupportedType { .. })
        ));
        let f = col("f", "double", "double");
        assert!(mysql_identity_kind(&f).is_err());
    }

    #[test]
    fn null_carries_declared_kind() {
        let c = col("id", "varchar(8)", "varchar");
        assert_eq!(
            mysql_identity_cell(&c, &Value::NULL).unwrap(),
            OwnedIdentityCell::Null(IdentityKind::Text)
        );
    }

    #[test]
    fn decimal_parses_sign_unscaled_scale() {
        let c = col("amt", "decimal(6,2)", "decimal");
        let cell = mysql_identity_cell(&c, &Value::Bytes(b"-123.45".to_vec()))
            .unwrap();
        // 12345 in base-256 is [0x30, 0x39].
        assert_eq!(
            cell,
            OwnedIdentityCell::Decimal {
                negative: true,
                unscaled: vec![0x30, 0x39],
                scale: 2,
            }
        );
    }

    #[test]
    fn decimal_digits_conversion_matches_be() {
        assert_eq!(
            decimal_digits_to_be_bytes(&[1, 2, 3, 4, 5]),
            vec![0x30, 0x39]
        );
        assert_eq!(decimal_digits_to_be_bytes(&[0, 0]), Vec::<u8>::new());
        assert_eq!(decimal_digits_to_be_bytes(&[2, 5, 5]), vec![0xff]);
        assert_eq!(decimal_digits_to_be_bytes(&[2, 5, 6]), vec![0x01, 0x00]);
    }

    #[test]
    fn date_is_days_from_epoch() {
        let c = col("d", "date", "date");
        // 1970-01-02 is day 1.
        let cell =
            mysql_identity_cell(&c, &Value::Date(1970, 1, 2, 0, 0, 0, 0))
                .unwrap();
        assert_eq!(
            cell,
            OwnedIdentityCell::DateTime {
                kind: TemporalKind::Date,
                value: 1,
            }
        );
    }

    #[test]
    fn datetime_is_micros_from_epoch() {
        let c = col("ts", "datetime", "datetime");
        let cell =
            mysql_identity_cell(&c, &Value::Date(1970, 1, 1, 0, 0, 1, 500_000))
                .unwrap();
        assert_eq!(
            cell,
            OwnedIdentityCell::DateTime {
                kind: TemporalKind::Timestamp,
                value: 1_500_000,
            }
        );
    }

    #[test]
    fn enum_carries_type_and_label() {
        let c = col("mood", "enum('happy','sad')", "enum");
        let cell =
            mysql_identity_cell(&c, &Value::Bytes(b"happy".to_vec())).unwrap();
        assert_eq!(
            cell,
            OwnedIdentityCell::Enum {
                enum_type: "enum('happy','sad')".into(),
                label: "happy".into(),
            }
        );
    }
}
