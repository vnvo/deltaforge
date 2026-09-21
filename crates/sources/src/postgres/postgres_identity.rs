//! Schema-directed identity extraction for PostgreSQL, reading the **native
//! binary wire representation** of each identity column.
//!
//! The snapshot query emits the row payload via `row_to_json`, but identity is
//! derived from the exact binary field bytes captured through [`PgIdentityRaw`]
//! (a tiny `FromSql` wrapper) — never from the lossy JSON, never via display
//! text, and never through a decimal type with a precision ceiling. This gives
//! exact PostgreSQL semantics: arbitrary-precision numeric, raw UUID bytes,
//! binary microsecond timestamps, and bytea-vs-text distinction.

use std::error::Error;

use anyhow::{Context, Result, anyhow};
use deltaforge_core::{IdentityKind, TemporalKind};
use tokio_postgres::types::{FromSql, Kind, Type};

use crate::snapshot_event_id::OwnedIdentityCell;

/// Quote a PostgreSQL identifier safely (doubling embedded quotes).
pub fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Resolve each identity column's canonical [`IdentityKind`] from the catalog in
/// **one batched query** (never per row), resolving domains recursively to their
/// base type. Returns kinds in the same order as `cols`, or an error if any
/// column's type is unsupported for stable identity (composite, range,
/// multirange, array, or unknown custom type).
pub async fn resolve_identity_kinds(
    client: &tokio_postgres::Client,
    schema: &str,
    table: &str,
    cols: &[String],
) -> Result<Vec<(String, IdentityKind)>> {
    use std::collections::HashMap;

    // Recursively peel domains (`typtype = 'd'`) down to the base type, then
    // return the terminal (non-domain) type per identity column.
    let sql = r#"
        WITH RECURSIVE resolved AS (
            SELECT a.attname::text AS name,
                   a.atttypid       AS oid,
                   t.typtype::text  AS typtype,
                   t.typbasetype    AS base
            FROM pg_attribute a
            JOIN pg_type t ON t.oid = a.atttypid
            WHERE a.attrelid = format('%I.%I', $1::text, $2::text)::regclass
              AND a.attname::text = ANY($3::text[])
              AND a.attnum > 0
              AND NOT a.attisdropped
            UNION ALL
            SELECT r.name, t.oid, t.typtype::text, t.typbasetype
            FROM resolved r
            JOIN pg_type t ON t.oid = r.base
            WHERE r.typtype = 'd'
        )
        SELECT name, oid, typtype FROM resolved WHERE typtype <> 'd'
    "#;

    let cols_vec: Vec<String> = cols.to_vec();
    let rows = client
        .query(sql, &[&schema, &table, &cols_vec])
        .await
        .context("resolve identity column types from catalog")?;

    let mut found: HashMap<String, (u32, String)> = HashMap::new();
    for row in rows {
        let name: String = row.get(0);
        let oid: u32 = row.get(1);
        let typtype: String = row.get(2);
        found.insert(name, (oid, typtype));
    }

    let mut out = Vec::with_capacity(cols.len());
    for c in cols {
        let (oid, typtype) = found.get(c).ok_or_else(|| {
            anyhow!("identity column {c:?} not found in catalog")
        })?;
        let kind = classify_catalog_type(*oid, typtype).ok_or_else(|| {
            anyhow!(
                "identity column {c:?} has a type unsupported for stable \
                 identity (composite, range, array, or unknown custom type)"
            )
        })?;
        out.push((c.clone(), kind));
    }
    Ok(out)
}

/// Classify a terminal (domain-resolved) catalog type into an [`IdentityKind`].
fn classify_catalog_type(oid: u32, typtype: &str) -> Option<IdentityKind> {
    match typtype {
        "e" => Some(IdentityKind::Enum),
        // Base type: defer to the wire-type mapping (rejects arrays/float/etc.).
        "b" => Type::from_oid(oid).and_then(|t| pg_identity_kind(&t)),
        // Composite ('c'), range ('r'), multirange ('m'), pseudo ('p'): reject.
        _ => None,
    }
}

/// Raw native field bytes plus the (domain-resolved) column type, captured via
/// `FromSql` so we see PostgreSQL's exact binary wire form.
#[derive(Debug, Clone)]
pub struct PgIdentityRaw {
    /// Domain-resolved base type.
    pub ty: Type,
    /// Raw binary field bytes (the wire representation).
    pub bytes: Vec<u8>,
}

impl<'a> FromSql<'a> for PgIdentityRaw {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(Self {
            ty: resolve_base(ty),
            bytes: raw.to_vec(),
        })
    }

    fn accepts(ty: &Type) -> bool {
        is_supported_identity_type(ty)
    }
}

/// Resolve a domain to its base type (recursively), so a domain over `uuid`
/// canonicalizes exactly like `uuid`. Non-domains are returned as-is.
fn resolve_base(ty: &Type) -> Type {
    match ty.kind() {
        Kind::Domain(base) => resolve_base(base),
        _ => ty.clone(),
    }
}

/// Whether a column type is supported as a stable identity column. Arrays,
/// composites, ranges, and unknown custom types are rejected; enums and domains
/// over supported bases are accepted.
pub fn is_supported_identity_type(ty: &Type) -> bool {
    pg_identity_kind(ty).is_some()
}

/// Map a (possibly domain-wrapped) PostgreSQL type to its canonical
/// [`IdentityKind`], or `None` if unsupported for identity.
pub fn pg_identity_kind(ty: &Type) -> Option<IdentityKind> {
    let base = resolve_base(ty);
    if matches!(base.kind(), Kind::Enum(_)) {
        return Some(IdentityKind::Enum);
    }
    Some(match base {
        Type::INT2 | Type::INT4 | Type::INT8 => IdentityKind::Int,
        Type::OID => IdentityKind::UInt,
        Type::UUID => IdentityKind::Uuid,
        Type::NUMERIC => IdentityKind::Decimal,
        Type::BOOL => IdentityKind::Bool,
        Type::BYTEA => IdentityKind::Bytes,
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => {
            IdentityKind::Text
        }
        Type::DATE | Type::TIME | Type::TIMESTAMP | Type::TIMESTAMPTZ => {
            IdentityKind::DateTime
        }
        _ => return None,
    })
}

/// Error decoding a native PostgreSQL identity value.
#[derive(Debug, thiserror::Error)]
pub enum PgIdentityError {
    /// The column type is not a supported identity type.
    #[error("PostgreSQL type {0} is not supported as a stable identity column")]
    UnsupportedType(String),
    /// The binary field bytes did not match the expected width/format.
    #[error("malformed binary value for {ty}: {detail}")]
    Malformed {
        /// Type name.
        ty: String,
        /// Detail.
        detail: String,
    },
}

fn malformed(ty: &Type, detail: &str) -> PgIdentityError {
    PgIdentityError::Malformed {
        ty: ty.name().to_string(),
        detail: detail.to_string(),
    }
}

fn temporal_kind(ty: &Type) -> TemporalKind {
    match *ty {
        Type::DATE => TemporalKind::Date,
        Type::TIME => TemporalKind::Time,
        Type::TIMESTAMPTZ => TemporalKind::TimestampTz,
        _ => TemporalKind::Timestamp,
    }
}

/// Decode a non-null native identity value into its canonical cell.
pub fn pg_identity_cell(
    raw: &PgIdentityRaw,
) -> Result<OwnedIdentityCell, PgIdentityError> {
    let ty = &raw.ty;
    let b = &raw.bytes;

    if matches!(ty.kind(), Kind::Enum(_)) {
        let label = std::str::from_utf8(b)
            .map_err(|_| malformed(ty, "enum label not UTF-8"))?;
        return Ok(OwnedIdentityCell::Enum {
            enum_type: ty.name().to_string(),
            label: label.to_string(),
        });
    }

    let cell = match *ty {
        Type::INT2 => OwnedIdentityCell::Int(be_int(b, 2, ty)?),
        Type::INT4 => OwnedIdentityCell::Int(be_int(b, 4, ty)?),
        Type::INT8 => OwnedIdentityCell::Int(be_int(b, 8, ty)?),
        Type::OID => OwnedIdentityCell::UInt(be_int(b, 4, ty)? as u64),
        Type::UUID => {
            if b.len() != 16 {
                return Err(malformed(ty, "uuid not 16 bytes"));
            }
            let mut u = [0u8; 16];
            u.copy_from_slice(b);
            OwnedIdentityCell::Uuid(u)
        }
        Type::NUMERIC => {
            let (negative, unscaled, scale) = decode_numeric(b, ty)?;
            OwnedIdentityCell::Decimal {
                negative,
                unscaled,
                scale,
            }
        }
        Type::BOOL => {
            if b.len() != 1 {
                return Err(malformed(ty, "bool not 1 byte"));
            }
            OwnedIdentityCell::Bool(b[0] != 0)
        }
        Type::BYTEA => OwnedIdentityCell::Bytes(b.clone()),
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => {
            let s = std::str::from_utf8(b)
                .map_err(|_| malformed(ty, "text not UTF-8"))?;
            OwnedIdentityCell::Text(s.to_string())
        }
        Type::DATE => OwnedIdentityCell::DateTime {
            kind: TemporalKind::Date,
            value: be_int(b, 4, ty)?,
        },
        Type::TIME | Type::TIMESTAMP | Type::TIMESTAMPTZ => {
            OwnedIdentityCell::DateTime {
                kind: temporal_kind(ty),
                value: be_int(b, 8, ty)?,
            }
        }
        _ => {
            return Err(PgIdentityError::UnsupportedType(
                ty.name().to_string(),
            ));
        }
    };
    Ok(cell)
}

/// Decode a big-endian signed integer of `width` bytes.
fn be_int(b: &[u8], width: usize, ty: &Type) -> Result<i64, PgIdentityError> {
    if b.len() != width {
        return Err(malformed(ty, "unexpected integer width"));
    }
    // Sign-extend from `width` bytes.
    let mut acc: i64 = if b[0] & 0x80 != 0 { -1 } else { 0 };
    for &byte in b {
        acc = (acc << 8) | byte as i64;
    }
    Ok(acc)
}

// ---------------------------------------------------------------------------
// PostgreSQL binary NUMERIC → (negative, big-endian unscaled magnitude, scale)
// Wire format: ndigits(u16), weight(i16), sign(u16), dscale(u16), then ndigits
// base-10000 digits (u16 each), most significant first. No precision ceiling.
// ---------------------------------------------------------------------------

const NUMERIC_POS: u16 = 0x0000;
const NUMERIC_NEG: u16 = 0x4000;

fn decode_numeric(
    b: &[u8],
    ty: &Type,
) -> Result<(bool, Vec<u8>, i32), PgIdentityError> {
    if b.len() < 8 {
        return Err(malformed(ty, "numeric header too short"));
    }
    let ndigits = u16::from_be_bytes([b[0], b[1]]) as usize;
    let weight = i16::from_be_bytes([b[2], b[3]]) as i32;
    let sign = u16::from_be_bytes([b[4], b[5]]);
    let dscale = u16::from_be_bytes([b[6], b[7]]) as i32;
    let negative = match sign {
        NUMERIC_POS => false,
        NUMERIC_NEG => true,
        _ => return Err(malformed(ty, "numeric NaN/Inf is not an identity")),
    };
    if b.len() < 8 + ndigits * 2 {
        return Err(malformed(ty, "numeric digits truncated"));
    }
    // Build U_raw = Σ digit[i] * 10000^(ndigits-1-i) as a big-endian bigint.
    let mut be: Vec<u8> = Vec::new();
    for i in 0..ndigits {
        let off = 8 + i * 2;
        let digit = u16::from_be_bytes([b[off], b[off + 1]]);
        if digit > 9999 {
            return Err(malformed(ty, "numeric digit out of range"));
        }
        mul_small(&mut be, 10000);
        add_small(&mut be, digit as u32);
    }
    // value = U_raw * 10000^(weight-ndigits+1); unscaled = value * 10^dscale.
    let shift = 4 * (weight - ndigits as i32 + 1) + dscale;
    if shift > 0 {
        for _ in 0..shift {
            mul_small(&mut be, 10);
        }
    } else if shift < 0 {
        for _ in 0..(-shift) {
            if divmod10(&mut be) != 0 {
                return Err(malformed(
                    ty,
                    "numeric has precision beyond its declared scale",
                ));
            }
        }
    }
    Ok((negative, be, dscale))
}

/// `be = be * m` (big-endian base-256), `m` small.
fn mul_small(be: &mut Vec<u8>, m: u32) {
    let mut carry: u64 = 0;
    for byte in be.iter_mut().rev() {
        let v = (*byte as u64) * m as u64 + carry;
        *byte = (v & 0xff) as u8;
        carry = v >> 8;
    }
    let mut pre = Vec::new();
    while carry > 0 {
        pre.push((carry & 0xff) as u8);
        carry >>= 8;
    }
    if !pre.is_empty() {
        pre.reverse();
        pre.extend_from_slice(be);
        *be = pre;
    }
}

/// `be = be + a` (big-endian base-256), `a` small.
fn add_small(be: &mut Vec<u8>, a: u32) {
    let mut carry = a as u64;
    let mut idx = be.len();
    while carry > 0 {
        if idx == 0 {
            let mut pre = Vec::new();
            while carry > 0 {
                pre.push((carry & 0xff) as u8);
                carry >>= 8;
            }
            pre.reverse();
            pre.extend_from_slice(be);
            *be = pre;
            return;
        }
        idx -= 1;
        let v = be[idx] as u64 + (carry & 0xff);
        be[idx] = (v & 0xff) as u8;
        carry = (carry >> 8) + (v >> 8);
    }
}

/// Divide `be` by 10 in place (big-endian base-256); returns the remainder.
fn divmod10(be: &mut Vec<u8>) -> u8 {
    let mut rem: u32 = 0;
    for byte in be.iter_mut() {
        let cur = (rem << 8) | (*byte as u32);
        *byte = (cur / 10) as u8;
        rem = cur % 10;
    }
    let first = be.iter().position(|&x| x != 0).unwrap_or(be.len());
    be.drain(0..first);
    rem as u8
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(ty: Type, bytes: Vec<u8>) -> PgIdentityRaw {
        PgIdentityRaw { ty, bytes }
    }

    /// Build a PostgreSQL binary numeric field.
    fn numeric_bytes(
        weight: i16,
        sign: u16,
        dscale: u16,
        digits: &[u16],
    ) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&(digits.len() as u16).to_be_bytes());
        b.extend_from_slice(&weight.to_be_bytes());
        b.extend_from_slice(&sign.to_be_bytes());
        b.extend_from_slice(&dscale.to_be_bytes());
        for d in digits {
            b.extend_from_slice(&d.to_be_bytes());
        }
        b
    }

    #[test]
    fn kinds_and_support() {
        assert_eq!(pg_identity_kind(&Type::INT8), Some(IdentityKind::Int));
        assert_eq!(pg_identity_kind(&Type::UUID), Some(IdentityKind::Uuid));
        assert_eq!(
            pg_identity_kind(&Type::NUMERIC),
            Some(IdentityKind::Decimal)
        );
        assert_eq!(
            pg_identity_kind(&Type::TIMESTAMPTZ),
            Some(IdentityKind::DateTime)
        );
        assert!(pg_identity_kind(&Type::FLOAT8).is_none());
        assert!(!is_supported_identity_type(&Type::INT4_ARRAY));
    }

    #[test]
    fn integers_sign_extend() {
        assert_eq!(
            pg_identity_cell(&raw(Type::INT2, (-5i16).to_be_bytes().to_vec()))
                .unwrap(),
            OwnedIdentityCell::Int(-5)
        );
        assert_eq!(
            pg_identity_cell(&raw(Type::INT8, i64::MAX.to_be_bytes().to_vec()))
                .unwrap(),
            OwnedIdentityCell::Int(i64::MAX)
        );
        assert_eq!(
            pg_identity_cell(&raw(Type::INT8, i64::MIN.to_be_bytes().to_vec()))
                .unwrap(),
            OwnedIdentityCell::Int(i64::MIN)
        );
    }

    #[test]
    fn uuid_is_raw_16_bytes() {
        let u = [0xABu8; 16];
        assert_eq!(
            pg_identity_cell(&raw(Type::UUID, u.to_vec())).unwrap(),
            OwnedIdentityCell::Uuid(u)
        );
    }

    #[test]
    fn bytea_stays_bytes_even_if_utf8() {
        let cell = pg_identity_cell(&raw(Type::BYTEA, b"42".to_vec())).unwrap();
        assert_eq!(cell, OwnedIdentityCell::Bytes(b"42".to_vec()));
        // Distinct from the text "42".
        let text = pg_identity_cell(&raw(Type::TEXT, b"42".to_vec())).unwrap();
        assert_ne!(cell, text);
    }

    #[test]
    fn bool_and_timestamp() {
        assert_eq!(
            pg_identity_cell(&raw(Type::BOOL, vec![1])).unwrap(),
            OwnedIdentityCell::Bool(true)
        );
        // TIMESTAMP micros are kept as the raw PostgreSQL binary integer.
        let micros: i64 = 123_456_789;
        assert_eq!(
            pg_identity_cell(&raw(
                Type::TIMESTAMP,
                micros.to_be_bytes().to_vec()
            ))
            .unwrap(),
            OwnedIdentityCell::DateTime {
                kind: TemporalKind::Timestamp,
                value: micros,
            }
        );
    }

    #[test]
    fn numeric_small() {
        // 1.23 → digits [1, 2300], weight 0, dscale 2.
        let (neg, unscaled, scale) = decode_numeric(
            &numeric_bytes(0, NUMERIC_POS, 2, &[1, 2300]),
            &Type::NUMERIC,
        )
        .unwrap();
        assert!(!neg);
        assert_eq!(scale, 2);
        // 123 in base-256 == [0x7b].
        assert_eq!(unscaled, vec![0x7b]);
    }

    #[test]
    fn numeric_multi_digit_and_negative() {
        // -12345.678 → digits [1, 2345, 6780], weight 1, dscale 3.
        let (neg, unscaled, scale) = decode_numeric(
            &numeric_bytes(1, NUMERIC_NEG, 3, &[1, 2345, 6780]),
            &Type::NUMERIC,
        )
        .unwrap();
        assert!(neg);
        assert_eq!(scale, 3);
        // unscaled = 12345678.
        assert_eq!(unscaled, 12_345_678u32.to_be_bytes()[1..].to_vec());
    }

    #[test]
    fn numeric_high_precision_beyond_rust_decimal() {
        // A 40-digit integer — far beyond rust_decimal's 96-bit ceiling.
        // 10 base-10000 digits of 9999 each = (10^40 - 1), weight 9, scale 0.
        let digits = [9999u16; 10];
        let (neg, unscaled, scale) = decode_numeric(
            &numeric_bytes(9, NUMERIC_POS, 0, &digits),
            &Type::NUMERIC,
        )
        .unwrap();
        assert!(!neg);
        assert_eq!(scale, 0);
        // Reconstruct the decimal value from the base-256 magnitude and check
        // it equals 10^40 - 1.
        let mut s = String::new();
        // Convert big-endian base-256 to decimal via repeated divmod10.
        let mut mag = unscaled.clone();
        if mag.is_empty() {
            s.push('0');
        }
        while !mag.is_empty() {
            let r = divmod10(&mut mag);
            s.push((b'0' + r) as char);
        }
        let decimal: String = s.chars().rev().collect();
        assert_eq!(decimal, "9".repeat(40));
    }

    #[test]
    fn numeric_zero() {
        let (neg, unscaled, scale) = decode_numeric(
            &numeric_bytes(0, NUMERIC_POS, 0, &[]),
            &Type::NUMERIC,
        )
        .unwrap();
        assert!(!neg);
        assert_eq!(scale, 0);
        assert!(unscaled.is_empty());
    }

    #[test]
    fn numeric_nan_rejected() {
        assert!(
            decode_numeric(&numeric_bytes(0, 0xC000, 0, &[]), &Type::NUMERIC)
                .is_err()
        );
    }
}
