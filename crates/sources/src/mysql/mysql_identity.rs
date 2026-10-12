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

use deltaforge_core::canonical::{
    CanonicalValue, CivilDate, DateTimeValue, DateValue, DbValue, Decimal,
    TimestampValue,
};
use deltaforge_core::{IdentityKind, TemporalKind};

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

/// Extract the identity cell for one decoded column value, directed by the
/// column's declared type. `NULL` yields `Null(kind)` carrying the declared
/// type tag. The value comes from the shared decoder, so a snapshot row and a
/// CDC row of the same stored value give the same cell.
pub fn mysql_identity_cell(
    col: &MySqlColumn,
    val: &CanonicalValue,
) -> Result<OwnedIdentityCell, MysqlIdentityError> {
    let kind = mysql_identity_kind(col)?;
    let db = match val {
        CanonicalValue::Null => return Ok(OwnedIdentityCell::Null(kind)),
        CanonicalValue::Db(db) => db,
        _ => return Err(mismatch(col, "not a database value")),
    };
    let cell = match (kind, db) {
        (IdentityKind::Int, DbValue::Int { value, .. }) => {
            OwnedIdentityCell::Int(*value)
        }
        (IdentityKind::Int, DbValue::Year(y)) => {
            OwnedIdentityCell::Int(*y as i64)
        }
        (IdentityKind::UInt, DbValue::UInt { value, .. }) => {
            OwnedIdentityCell::UInt(*value)
        }
        (IdentityKind::UInt, DbValue::Year(y)) => {
            OwnedIdentityCell::UInt(*y as u64)
        }
        (IdentityKind::Text, DbValue::Text(t)) => {
            OwnedIdentityCell::Text(t.clone())
        }
        (IdentityKind::Bytes, DbValue::Binary(b)) => {
            OwnedIdentityCell::Bytes(b.clone())
        }
        (IdentityKind::Decimal, DbValue::Decimal(Decimal::Finite(text))) => {
            let (negative, unscaled, scale) = decimal_parts(col, text)?;
            OwnedIdentityCell::Decimal {
                negative,
                unscaled,
                scale,
            }
        }
        (IdentityKind::DateTime, v) => OwnedIdentityCell::DateTime {
            kind: temporal_kind(&col.data_type.to_lowercase()),
            value: temporal_int(col, v)?,
        },
        (IdentityKind::Enum, DbValue::Enum(e)) => OwnedIdentityCell::Enum {
            // MySQL enums are anonymous; the full column type (e.g.
            // "enum('a','b')") is the stable discriminator.
            enum_type: col.column_type.clone(),
            label: e.label.clone(),
        },
        // MySQL never maps a column to these kinds.
        (IdentityKind::Uuid | IdentityKind::Bool, _) => {
            return Err(mismatch(col, "unexpected kind for MySQL"));
        }
        _ => return Err(mismatch(col, "value does not match the column type")),
    };
    Ok(cell)
}

/// Split canonical decimal text (`-123.45`) into
/// `(negative, big-endian unscaled magnitude, scale)`.
fn decimal_parts(
    col: &MySqlColumn,
    s: &str,
) -> Result<(bool, Vec<u8>, i32), MysqlIdentityError> {
    let (negative, rest) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s),
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

/// Days from the civil calendar epoch (1970-01-01), Howard Hinnant's algorithm
/// (MySQL zero parts go through the same arithmetic).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn civil_days(d: &CivilDate) -> i64 {
    days_from_civil(d.year() as i64, d.month() as i64, d.day() as i64)
}

/// DATE: days from the epoch; DATETIME and TIMESTAMP: microseconds from the
/// epoch (TIMESTAMP: the UTC instant); TIME: signed microseconds.
fn temporal_int(
    col: &MySqlColumn,
    v: &DbValue,
) -> Result<i64, MysqlIdentityError> {
    match v {
        DbValue::Date(DateValue::Civil(d)) => Ok(civil_days(d)),
        DbValue::DateTime(DateTimeValue::Civil {
            date,
            micros_of_day,
            ..
        }) => Ok(civil_days(date) * 86_400_000_000 + micros_of_day),
        DbValue::Timestamp(TimestampValue::Instant { micros, .. }) => {
            Ok(*micros)
        }
        DbValue::Timestamp(TimestampValue::Zero { .. }) => {
            Ok(days_from_civil(0, 0, 0) * 86_400_000_000)
        }
        DbValue::Time(t) => Ok(t.micros()),
        _ => Err(mismatch(col, "not a date/time value")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mysql::mysql_decode::{ColumnDecoder, Raw};
    use mysql_async::Value;

    fn col(name: &str, column_type: &str, data_type: &str) -> MySqlColumn {
        let mut c = MySqlColumn::new(name, column_type, data_type, false, 1);
        c.collation_id = Some(255);
        c
    }

    /// The identity cell of a snapshot value, through the shared decoder.
    fn cell(
        c: &MySqlColumn,
        v: Value,
    ) -> Result<OwnedIdentityCell, MysqlIdentityError> {
        let d = ColumnDecoder::new(c);
        let kind = d.kind.clone().unwrap();
        let decoded = d.decode(Raw::from_snapshot(&v, &kind)).unwrap();
        mysql_identity_cell(c, &decoded)
    }

    #[test]
    fn kind_signedness_from_metadata() {
        let signed = col("a", "int(11)", "int");
        let unsigned = col("b", "int(11) unsigned", "int");
        assert_eq!(mysql_identity_kind(&signed).unwrap(), IdentityKind::Int);
        assert_eq!(mysql_identity_kind(&unsigned).unwrap(), IdentityKind::UInt);
        assert_eq!(
            cell(&unsigned, Value::UInt(7)).unwrap(),
            OwnedIdentityCell::UInt(7)
        );
        assert_eq!(
            cell(&signed, Value::Int(-7)).unwrap(),
            OwnedIdentityCell::Int(-7)
        );
    }

    #[test]
    fn binary_column_stays_bytes_even_if_utf8() {
        // "42" is valid UTF-8, but a BINARY column must remain Bytes.
        let mut c = col("id", "binary(2)", "binary");
        c.char_octet_length = Some(2);
        assert_eq!(
            cell(&c, Value::Bytes(b"42".to_vec())).unwrap(),
            OwnedIdentityCell::Bytes(b"42".to_vec())
        );
    }

    #[test]
    fn text_column_is_text() {
        let c = col("id", "varchar(8)", "varchar");
        assert_eq!(
            cell(&c, Value::Bytes(b"42".to_vec())).unwrap(),
            OwnedIdentityCell::Text("42".into())
        );
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
            cell(&c, Value::NULL).unwrap(),
            OwnedIdentityCell::Null(IdentityKind::Text)
        );
    }

    #[test]
    fn decimal_parses_sign_unscaled_scale() {
        let c = col("amt", "decimal(6,2)", "decimal");
        // 12345 in base-256 is [0x30, 0x39].
        assert_eq!(
            cell(&c, Value::Bytes(b"-123.45".to_vec())).unwrap(),
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
        assert_eq!(
            cell(&c, Value::Date(1970, 1, 2, 0, 0, 0, 0)).unwrap(),
            OwnedIdentityCell::DateTime {
                kind: TemporalKind::Date,
                value: 1,
            }
        );
    }

    #[test]
    fn datetime_and_timestamp_are_micros_from_epoch() {
        let mut c = col("ts", "datetime(6)", "datetime");
        c.datetime_precision = Some(6);
        assert_eq!(
            cell(&c, Value::Date(1970, 1, 1, 0, 0, 1, 500_000)).unwrap(),
            OwnedIdentityCell::DateTime {
                kind: TemporalKind::Timestamp,
                value: 1_500_000,
            }
        );
        let mut t = col("ts", "timestamp(6)", "timestamp");
        t.datetime_precision = Some(6);
        assert_eq!(
            cell(&t, Value::Date(1970, 1, 1, 0, 0, 1, 500_000)).unwrap(),
            OwnedIdentityCell::DateTime {
                kind: TemporalKind::TimestampTz,
                value: 1_500_000,
            }
        );
    }

    #[test]
    fn enum_carries_type_and_label() {
        let c = col("mood", "enum('happy','sad')", "enum");
        // The snapshot reads the stored index (`mood+0`).
        assert_eq!(
            cell(&c, Value::Int(1)).unwrap(),
            OwnedIdentityCell::Enum {
                enum_type: "enum('happy','sad')".into(),
                label: "happy".into(),
            }
        );
    }
}
