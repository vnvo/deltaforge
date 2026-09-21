//! Stable event identity — `dfid:v1:<class>:<32 hex>`.
//!
//! Identity is derived from **immutable source coordinates** (never the payload)
//! so it is identical across retry, restart, and legitimate source failover, and
//! changes only on a deliberate re-snapshot. See
//! `docs/specs/stable-event-id.md` for the full contract and test vectors.
//!
//! Wire form: `dfid:v1:<class>:<32 lowercase hex>` where the hex is the first
//! 128 bits of `SHA-256(DOMAIN || class_byte || <binary coordinates>)`. The
//! binary encoding is length-prefixed and big-endian to avoid delimiter
//! ambiguity; `FromStr` is strict and only accepts the one canonical spelling.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};

/// Domain-separation prefix (22 bytes, including the trailing NUL).
const DOMAIN: &[u8] = b"DeltaForge.EventId.v1\0";

/// The class of source change an [`EventId`] identifies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventClass {
    /// MySQL row change (insert/update/delete).
    MyRow,
    /// PostgreSQL row change.
    PgRow,
    /// Snapshot (initial load) row.
    Snap,
    /// DDL / schema change.
    Ddl,
    /// Logical message.
    Msg,
    /// Synthetic (processor-generated) event.
    Syn,
}

impl EventClass {
    /// Class byte used in the hashed binary encoding.
    const fn byte(self) -> u8 {
        match self {
            EventClass::MyRow => 0x01,
            EventClass::PgRow => 0x02,
            EventClass::Snap => 0x03,
            EventClass::Ddl => 0x04,
            EventClass::Msg => 0x05,
            EventClass::Syn => 0x06,
        }
    }

    /// Wire name used in the textual form.
    const fn wire(self) -> &'static str {
        match self {
            EventClass::MyRow => "myrow",
            EventClass::PgRow => "pgrow",
            EventClass::Snap => "snap",
            EventClass::Ddl => "ddl",
            EventClass::Msg => "msg",
            EventClass::Syn => "syn",
        }
    }

    fn from_wire(s: &str) -> Option<Self> {
        Some(match s {
            "myrow" => EventClass::MyRow,
            "pgrow" => EventClass::PgRow,
            "snap" => EventClass::Snap,
            "ddl" => EventClass::Ddl,
            "msg" => EventClass::Msg,
            "syn" => EventClass::Syn,
            _ => return None,
        })
    }
}

/// A stable, fixed-size event identity. Cheap to hash/store (16-byte digest +
/// a class tag), so `HashSet<EventId>` stays efficient.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EventId {
    class: EventClass,
    hash: [u8; 16],
}

/// Builds the length-prefixed, big-endian coordinate buffer that is hashed.
/// Keeping this explicit (rather than a formatted string) removes delimiter and
/// escaping ambiguity, so the encoding is reproducible byte-for-byte.
#[derive(Default)]
struct Coordinates {
    buf: Vec<u8>,
}

impl Coordinates {
    fn u8(&mut self, v: u8) -> &mut Self {
        self.buf.push(v);
        self
    }
    fn u32(&mut self, v: u32) -> &mut Self {
        self.buf.extend_from_slice(&v.to_be_bytes());
        self
    }
    fn u64(&mut self, v: u64) -> &mut Self {
        self.buf.extend_from_slice(&v.to_be_bytes());
        self
    }
    fn bytes16(&mut self, v: &[u8; 16]) -> &mut Self {
        self.buf.extend_from_slice(v);
        self
    }
    /// A UTF-8 string, `u16` big-endian byte-length prefixed.
    fn str(&mut self, s: &str) -> &mut Self {
        let b = s.as_bytes();
        // Lengths are bounded by real identifiers/keys; truncation is not a
        // concern for identity coordinates in practice.
        self.buf.extend_from_slice(&(b.len() as u16).to_be_bytes());
        self.buf.extend_from_slice(b);
        self
    }
    /// Raw bytes, `u32` big-endian byte-length prefixed. Used for binary
    /// identity values, which can exceed a `u16` length.
    fn bytes_lp(&mut self, b: &[u8]) -> &mut Self {
        self.buf.extend_from_slice(&(b.len() as u32).to_be_bytes());
        self.buf.extend_from_slice(b);
        self
    }
    /// A component count for a following list.
    fn count(&mut self, n: u16) -> &mut Self {
        self.buf.extend_from_slice(&n.to_be_bytes());
        self
    }
}

/// Immutable source lineage, mixed into snapshot, DDL, and logical-message
/// identity so the same coordinates in two independent clusters/lineages cannot
/// collide. (MySQL/PostgreSQL row ids already embed lineage via the GTID SID /
/// `system_identifier`.)
pub enum SourceLineage<'a> {
    /// PostgreSQL cluster identity.
    Postgres { system_identifier: u64 },
    /// MySQL GTID lineage — the transaction's source UUID.
    MysqlGtid { source_uuid: [u8; 16] },
    /// MySQL non-GTID fallback — server id + binlog filename context.
    MysqlServer { server_id: u32, file: &'a str },
}

impl SourceLineage<'_> {
    fn encode(&self, c: &mut Coordinates) {
        match self {
            SourceLineage::Postgres { system_identifier } => {
                c.u8(0x01).u64(*system_identifier);
            }
            SourceLineage::MysqlGtid { source_uuid } => {
                c.u8(0x02).bytes16(source_uuid);
            }
            SourceLineage::MysqlServer { server_id, file } => {
                c.u8(0x03).u32(*server_id).str(file);
            }
        }
    }
}

/// Canonical type category of an identity (primary-key / `identity_columns`)
/// value. Hashed before the value bytes so the integer `42` and the text `"42"`
/// — and a null in a text column versus a null in an integer column — can never
/// collide in a snapshot id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityKind {
    /// Signed integer.
    Int,
    /// Unsigned integer.
    UInt,
    /// UTF-8 text.
    Text,
    /// Raw bytes.
    Bytes,
    /// UUID (encoded as its raw 16 bytes).
    Uuid,
    /// Exact numeric / decimal (sign + unscaled integer + declared scale).
    Decimal,
    /// Boolean.
    Bool,
    /// Date / time (a source-semantic integer plus a temporal sub-kind).
    DateTime,
    /// Enumerated label (declared enum type + label).
    Enum,
}

impl IdentityKind {
    const fn tag(self) -> u8 {
        match self {
            IdentityKind::Int => 0x01,
            IdentityKind::UInt => 0x02,
            IdentityKind::Text => 0x03,
            IdentityKind::Bytes => 0x04,
            IdentityKind::Uuid => 0x05,
            IdentityKind::Decimal => 0x06,
            IdentityKind::Bool => 0x07,
            IdentityKind::DateTime => 0x08,
            IdentityKind::Enum => 0x09,
        }
    }
}

/// Temporal sub-kind for a [`IdentityCell::DateTime`]. Distinguishes, e.g., a
/// null (or value) in a `date` column from one in a `timestamp` column, and
/// fixes the source-semantic meaning of the accompanying integer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemporalKind {
    /// Calendar date (integer = days from the source epoch).
    Date,
    /// Time of day (integer = sub-day units, e.g. microseconds).
    Time,
    /// Timestamp without time zone (integer = units from the source epoch).
    Timestamp,
    /// Timestamp with time zone (integer = units from the UTC epoch).
    TimestampTz,
}

impl TemporalKind {
    const fn tag(self) -> u8 {
        match self {
            TemporalKind::Date => 0x01,
            TemporalKind::Time => 0x02,
            TemporalKind::Timestamp => 0x03,
            TemporalKind::TimestampTz => 0x04,
        }
    }
}

/// A typed identity cell — the canonical value of one identity column. Each
/// variant carries an explicit type tag (including for nulls), so distinct
/// source types with equal textual/byte forms hash to distinct ids.
pub enum IdentityCell<'a> {
    /// Signed integer value.
    Int(i64),
    /// Unsigned integer value.
    UInt(u64),
    /// UTF-8 text value.
    Text(&'a str),
    /// Raw byte-string value.
    Bytes(&'a [u8]),
    /// UUID as its raw 16 bytes (not display text).
    Uuid([u8; 16]),
    /// Exact numeric: `negative` sign, big-endian `unscaled` magnitude (the
    /// integer value with the decimal point removed), and the declared `scale`
    /// (digits after the point). Encoding normalizes the magnitude (leading
    /// zero bytes stripped) and forbids negative zero, but keeps the declared
    /// scale — so `1.0` (scale 1) and `1.00` (scale 2) differ, as their column
    /// types do.
    Decimal {
        /// Sign of the value.
        negative: bool,
        /// Big-endian unscaled magnitude bytes.
        unscaled: &'a [u8],
        /// Declared scale (digits after the decimal point).
        scale: i32,
    },
    /// Boolean value.
    Bool(bool),
    /// Date/time: a source-semantic integer plus its temporal sub-kind.
    DateTime {
        /// Temporal sub-kind (fixes the integer's meaning).
        kind: TemporalKind,
        /// Source-semantic integer (e.g. days, or micros from the epoch).
        value: i64,
    },
    /// Enumerated value: the declared enum type name and the label.
    Enum {
        /// Declared enum/source type name (so equal labels of different enum
        /// types do not collide).
        enum_type: &'a str,
        /// The enum label.
        label: &'a str,
    },
    /// A null value in a column of the given type category.
    Null(IdentityKind),
}

/// One identity column's contribution to a snapshot id: its name plus a typed,
/// canonical value. Ordering within the identity list is significant.
pub struct IdentityValue<'a> {
    /// Column name. Included so a renamed/reordered composite key cannot
    /// silently alias to a different key with the same values.
    pub name: &'a str,
    /// The typed cell value.
    pub cell: IdentityCell<'a>,
}

impl IdentityValue<'_> {
    /// Encode `name || type_tag || presence_marker || canonical_value_bytes`.
    fn encode(&self, c: &mut Coordinates) {
        c.str(self.name);
        match &self.cell {
            IdentityCell::Int(v) => {
                c.u8(IdentityKind::Int.tag()).u8(1).u64(*v as u64);
            }
            IdentityCell::UInt(v) => {
                c.u8(IdentityKind::UInt.tag()).u8(1).u64(*v);
            }
            IdentityCell::Text(s) => {
                c.u8(IdentityKind::Text.tag()).u8(1).str(s);
            }
            IdentityCell::Bytes(b) => {
                c.u8(IdentityKind::Bytes.tag()).u8(1).bytes_lp(b);
            }
            IdentityCell::Uuid(u) => {
                c.u8(IdentityKind::Uuid.tag()).u8(1).bytes16(u);
            }
            IdentityCell::Decimal {
                negative,
                unscaled,
                scale,
            } => {
                // Normalize: strip leading zero bytes; zero is never negative.
                let mag = {
                    let first = unscaled
                        .iter()
                        .position(|&b| b != 0)
                        .unwrap_or(unscaled.len());
                    &unscaled[first..]
                };
                let neg = if mag.is_empty() { false } else { *negative };
                c.u8(IdentityKind::Decimal.tag())
                    .u8(1)
                    .u8(neg as u8)
                    .u32(*scale as u32)
                    .bytes_lp(mag);
            }
            IdentityCell::Bool(b) => {
                c.u8(IdentityKind::Bool.tag()).u8(1).u8(*b as u8);
            }
            IdentityCell::DateTime { kind, value } => {
                c.u8(IdentityKind::DateTime.tag())
                    .u8(1)
                    .u8(kind.tag())
                    .u64(*value as u64);
            }
            IdentityCell::Enum { enum_type, label } => {
                c.u8(IdentityKind::Enum.tag())
                    .u8(1)
                    .str(enum_type)
                    .str(label);
            }
            // Presence marker 0 = null; the column's type tag still
            // participates, so a null distinguishes by column type.
            IdentityCell::Null(kind) => {
                c.u8(kind.tag()).u8(0);
            }
        }
    }
}

impl EventId {
    /// The class this id belongs to.
    pub fn class(&self) -> EventClass {
        self.class
    }

    /// The raw 128-bit digest.
    pub fn digest(&self) -> &[u8; 16] {
        &self.hash
    }

    /// Hash `DOMAIN || class_byte || coordinates` and keep the first 128 bits.
    fn from_coordinates(class: EventClass, coords: &Coordinates) -> Self {
        let mut h = Sha256::new();
        h.update(DOMAIN);
        h.update([class.byte()]);
        h.update(&coords.buf);
        let digest = h.finalize();
        let mut hash = [0u8; 16];
        hash.copy_from_slice(&digest[..16]);
        Self { class, hash }
    }

    /// MySQL row, GTID form: `SID:GNO` + rows-event position + row ordinal.
    /// Failover-stable (the source UUID travels with the GTID).
    pub fn mysql_row_gtid(
        source_uuid: &[u8; 16],
        gno: u64,
        event_position: u64,
        row_ordinal: u32,
    ) -> Self {
        let mut c = Coordinates::default();
        c.u8(0x01) // form: gtid
            .bytes16(source_uuid)
            .u64(gno)
            .u64(event_position)
            .u32(row_ordinal);
        Self::from_coordinates(EventClass::MyRow, &c)
    }

    /// MySQL row, non-GTID fallback: `server_id` + binlog file + event position
    /// + row ordinal. Not failover-stable (recommend `gtid_mode=ON`).
    pub fn mysql_row_server(
        server_id: u32,
        file: &str,
        event_position: u64,
        row_ordinal: u32,
    ) -> Self {
        let mut c = Coordinates::default();
        c.u8(0x00) // form: server_id
            .u32(server_id)
            .str(file)
            .u64(event_position)
            .u32(row_ordinal);
        Self::from_coordinates(EventClass::MyRow, &c)
    }

    /// PostgreSQL row: cluster `system_identifier` + transaction final LSN +
    /// relation OID + per-transaction change ordinal.
    pub fn pg_row(
        system_identifier: u64,
        final_lsn: u64,
        relation_oid: u32,
        change_ordinal: u32,
    ) -> Self {
        let mut c = Coordinates::default();
        c.u64(system_identifier)
            .u64(final_lsn)
            .u32(relation_oid)
            .u32(change_ordinal);
        Self::from_coordinates(EventClass::PgRow, &c)
    }

    /// Snapshot row: source lineage + snapshot generation + table identity +
    /// typed identity-column values (in declared order).
    ///
    /// Lineage prevents two independent source instances with identical database
    /// / table names and identical rows from colliding. Identity values are
    /// typed and length-prefixed, so the integer `42` and the text `"42"` (and
    /// nulls of different column types) produce different ids. The generation is
    /// durably allocated per (re)snapshot, so a deliberate resnapshot yields a
    /// fresh id space while a resumed snapshot reproduces the same ids.
    pub fn snapshot(
        lineage: &SourceLineage<'_>,
        generation: u64,
        db: &str,
        table: &str,
        identity: &[IdentityValue<'_>],
    ) -> Self {
        let mut c = Coordinates::default();
        lineage.encode(&mut c);
        c.u64(generation)
            .str(db)
            .str(table)
            .count(identity.len() as u16);
        for col in identity {
            col.encode(&mut c);
        }
        Self::from_coordinates(EventClass::Snap, &c)
    }

    /// DDL / schema change: source lineage + source position (LSN/binlog string)
    /// + a per-position message ordinal.
    pub fn ddl(
        lineage: &SourceLineage<'_>,
        source_position: &str,
        message_ordinal: u32,
    ) -> Self {
        let mut c = Coordinates::default();
        lineage.encode(&mut c);
        c.str(source_position).u32(message_ordinal);
        Self::from_coordinates(EventClass::Ddl, &c)
    }

    /// Logical message: source lineage + source position + per-position ordinal.
    pub fn logical_message(
        lineage: &SourceLineage<'_>,
        source_position: &str,
        message_ordinal: u32,
    ) -> Self {
        let mut c = Coordinates::default();
        lineage.encode(&mut c);
        c.str(source_position).u32(message_ordinal);
        Self::from_coordinates(EventClass::Msg, &c)
    }

    /// Synthetic (processor-generated) event: derived from the parent event's
    /// identity, a stable processor digest (e.g. a normalized code+config hash
    /// for inline JS, or `"<name>:<version>"` for built-ins), and the output's
    /// ordinal among the parent's newly-emitted events.
    ///
    /// A 1:1 transformation should **retain the parent's `EventId`** rather than
    /// mint a synthetic one; this constructor is only for *newly emitted*
    /// outputs. A non-deterministic processor cannot promise replay-stable
    /// synthetic identity.
    pub fn synthetic(
        parent: &EventId,
        processor_digest: &str,
        output_ordinal: u32,
    ) -> Self {
        let mut c = Coordinates::default();
        c.u8(parent.class.byte())
            .bytes16(&parent.hash)
            .str(processor_digest)
            .u32(output_ordinal);
        Self::from_coordinates(EventClass::Syn, &c)
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "dfid:v1:{}:{}",
            self.class.wire(),
            hex::encode(self.hash)
        )
    }
}

/// Error parsing an [`EventId`] from its textual form.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid EventId: {0}")]
pub struct EventIdParseError(String);

impl FromStr for EventId {
    type Err = EventIdParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let rest = s.strip_prefix("dfid:v1:").ok_or_else(|| {
            EventIdParseError(format!("expected 'dfid:v1:' prefix in {s:?}"))
        })?;
        let (class_str, hex_str) = rest.split_once(':').ok_or_else(|| {
            EventIdParseError("missing class/hash separator".into())
        })?;
        let class = EventClass::from_wire(class_str).ok_or_else(|| {
            EventIdParseError(format!("unknown class {class_str:?}"))
        })?;
        if hex_str.len() != 32 {
            return Err(EventIdParseError(format!(
                "hash must be 32 hex chars, got {}",
                hex_str.len()
            )));
        }
        // Canonical spelling only: lowercase hex, no other characters. `hex`
        // itself would accept uppercase, so reject it explicitly to keep one
        // textual identity per event.
        if !hex_str
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(EventIdParseError("hash must be lowercase hex".into()));
        }
        let decoded = hex::decode(hex_str)
            .map_err(|e| EventIdParseError(format!("bad hex: {e}")))?;
        let mut hash = [0u8; 16];
        hash.copy_from_slice(&decoded);
        Ok(Self { class, hash })
    }
}

impl Serialize for EventId {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for EventId {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sid() -> [u8; 16] {
        // 3e11fa47-71ca-11e1-9e33-c80aa9429562
        let mut b = [0u8; 16];
        b.copy_from_slice(
            &hex::decode("3e11fa4771ca11e19e33c80aa9429562").unwrap(),
        );
        b
    }

    /// Canonical PostgreSQL lineage used across the snapshot vectors/tests.
    fn pg_lineage() -> SourceLineage<'static> {
        SourceLineage::Postgres {
            system_identifier: 0x1234_5678_90AB_CDEF,
        }
    }

    /// Terse identity-column constructor for tests.
    fn iv<'a>(name: &'a str, cell: IdentityCell<'a>) -> IdentityValue<'a> {
        IdentityValue { name, cell }
    }

    #[test]
    fn pinned_vector_mysql_gtid() {
        // Reproduced independently from the RFC's binary encoding.
        let id = EventId::mysql_row_gtid(&sid(), 23, 1547, 0);
        assert_eq!(
            id.to_string(),
            "dfid:v1:myrow:b8107ad4ab475b60e3a0c28b6c336688"
        );
        assert_eq!(id.class(), EventClass::MyRow);
    }

    #[test]
    fn pinned_vector_snapshot() {
        // PG lineage (sysid 0x1234567890ABCDEF), generation 1,
        // orders.customers, single Int identity column id=42.
        let id = EventId::snapshot(
            &pg_lineage(),
            1,
            "orders",
            "customers",
            &[iv("id", IdentityCell::Int(42))],
        );
        assert_eq!(
            id.to_string(),
            "dfid:v1:snap:a50e51a36b9842393e2a4283c0c7d341"
        );
    }

    #[test]
    fn pinned_vector_ddl_msg_synthetic() {
        let pg = SourceLineage::Postgres {
            system_identifier: 0x1234_5678_90AB_CDEF,
        };
        assert_eq!(
            EventId::ddl(&pg, "0/16B374D8", 0).to_string(),
            "dfid:v1:ddl:bfa60f51fbf665a7129e6566f0f2cc39"
        );
        let my = SourceLineage::MysqlGtid { source_uuid: sid() };
        assert_eq!(
            EventId::logical_message(&my, "mysql-bin.000008:15248355", 0)
                .to_string(),
            "dfid:v1:msg:d5f3b61c68d8e522c998c54ae149b459"
        );
        // Synthetic derives from the parent (the myrow pinned vector), a
        // processor digest, and the output ordinal.
        let parent = EventId::mysql_row_gtid(&sid(), 23, 1547, 0);
        assert_eq!(
            EventId::synthetic(&parent, "js:abc123", 0).to_string(),
            "dfid:v1:syn:e6e4b2a25cdb888cf8ff42bc36c64934"
        );
    }

    #[test]
    fn ddl_identity_includes_source_lineage() {
        // Same position + ordinal, different lineage → different id (no
        // cross-cluster collision).
        let a = EventId::ddl(
            &SourceLineage::Postgres {
                system_identifier: 1,
            },
            "0/16B374D8",
            0,
        );
        let b = EventId::ddl(
            &SourceLineage::Postgres {
                system_identifier: 2,
            },
            "0/16B374D8",
            0,
        );
        let c = EventId::ddl(
            &SourceLineage::MysqlGtid { source_uuid: sid() },
            "0/16B374D8",
            0,
        );
        assert_ne!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn synthetic_is_sensitive_to_parent_digest_and_ordinal() {
        let p1 = EventId::mysql_row_gtid(&sid(), 23, 1547, 0);
        let p2 = EventId::mysql_row_gtid(&sid(), 23, 1547, 1);
        let base = EventId::synthetic(&p1, "js:abc123", 0);
        assert_ne!(base, EventId::synthetic(&p2, "js:abc123", 0)); // parent
        assert_ne!(base, EventId::synthetic(&p1, "js:def456", 0)); // digest
        assert_ne!(base, EventId::synthetic(&p1, "js:abc123", 1)); // ordinal
        assert_eq!(base, EventId::synthetic(&p1, "js:abc123", 0)); // stable
    }

    #[test]
    fn roundtrip_display_fromstr() {
        for id in [
            EventId::mysql_row_gtid(&sid(), 7, 100, 3),
            EventId::mysql_row_server(42, "mysql-bin.000008", 15248355, 2),
            EventId::pg_row(0xABCD_1234_5678_9ABC, 0x16_B374_D848, 16384, 5),
            EventId::snapshot(
                &pg_lineage(),
                2,
                "shop",
                "orders",
                &[
                    iv("a", IdentityCell::Text("x")),
                    iv("b", IdentityCell::Bytes(b"\x00\x01")),
                ],
            ),
            EventId::ddl(
                &SourceLineage::Postgres {
                    system_identifier: 9,
                },
                "0/16B374D8",
                1,
            ),
            EventId::logical_message(
                &SourceLineage::MysqlServer {
                    server_id: 7,
                    file: "mysql-bin.000008",
                },
                "0/16B374D8",
                2,
            ),
            EventId::synthetic(
                &EventId::snapshot(
                    &pg_lineage(),
                    1,
                    "d",
                    "t",
                    &[iv("id", IdentityCell::UInt(1))],
                ),
                "outbox:v1",
                0,
            ),
        ] {
            let s = id.to_string();
            assert_eq!(s.parse::<EventId>().unwrap(), id, "roundtrip {s}");
        }
    }

    #[test]
    fn serde_json_roundtrip() {
        let id = EventId::snapshot(
            &pg_lineage(),
            1,
            "orders",
            "customers",
            &[iv("id", IdentityCell::Int(42))],
        );
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "\"dfid:v1:snap:a50e51a36b9842393e2a4283c0c7d341\"");
        let back: EventId = serde_json::from_str(&json).unwrap();
        assert_eq!(back, id);
    }

    #[test]
    fn fromstr_rejects_malformed() {
        let bad = [
            "",
            "nope",
            "dfid:v2:myrow:b8107ad4ab475b60e3a0c28b6c336688", // wrong version
            "dfid:v1:bogus:b8107ad4ab475b60e3a0c28b6c336688", // unknown class
            "dfid:v1:myrow:b8107ad4ab475b60e3a0c28b6c3366",   // too short
            "dfid:v1:myrow:b8107ad4ab475b60e3a0c28b6c33668899", // too long
            "dfid:v1:myrow:B8107AD4AB475B60E3A0C28B6C336688", // uppercase hex
            "dfid:v1:myrow:zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", // non-hex
            "dfid:v1:myrow",                                  // no hash
        ];
        for s in bad {
            assert!(s.parse::<EventId>().is_err(), "should reject {s:?}");
        }
    }

    #[test]
    fn distinct_coordinates_distinct_ids() {
        let a = EventId::mysql_row_gtid(&sid(), 23, 1547, 0);
        let b = EventId::mysql_row_gtid(&sid(), 23, 1547, 1); // row ordinal differs
        let c = EventId::mysql_row_gtid(&sid(), 24, 1547, 0); // gno differs
        assert_ne!(a, b);
        assert_ne!(a, c);
        // stable: same coordinates → same id.
        assert_eq!(a, EventId::mysql_row_gtid(&sid(), 23, 1547, 0));
    }

    #[test]
    fn usable_in_hashset() {
        use std::collections::HashSet;
        let one = || {
            EventId::snapshot(
                &pg_lineage(),
                1,
                "d",
                "t",
                &[iv("id", IdentityCell::Int(1))],
            )
        };
        let two = EventId::snapshot(
            &pg_lineage(),
            1,
            "d",
            "t",
            &[iv("id", IdentityCell::Int(2))],
        );
        let mut set = HashSet::new();
        set.insert(one());
        assert!(set.contains(&one()));
        assert!(!set.contains(&two));
    }

    #[test]
    fn snapshot_distinguishes_type_from_text_and_bytes() {
        // The reviewer's core requirement: int 42, uint 42, text "42" and the
        // bytes b"42" must never conflate.
        let d =
            |c| EventId::snapshot(&pg_lineage(), 1, "d", "t", &[iv("id", c)]);
        let as_int = d(IdentityCell::Int(42));
        let as_uint = d(IdentityCell::UInt(42));
        let as_text = d(IdentityCell::Text("42"));
        let as_bytes = d(IdentityCell::Bytes(b"42"));
        assert_ne!(as_int, as_uint);
        assert_ne!(as_int, as_text);
        assert_ne!(as_int, as_bytes);
        assert_ne!(as_text, as_bytes);
    }

    #[test]
    fn snapshot_identity_includes_lineage() {
        // Two independent sources with identical db/table/rows must not collide.
        let d = |lin: &SourceLineage<'_>| {
            EventId::snapshot(
                lin,
                1,
                "d",
                "t",
                &[iv("id", IdentityCell::Int(1))],
            )
        };
        let a = d(&SourceLineage::Postgres {
            system_identifier: 1,
        });
        let b = d(&SourceLineage::Postgres {
            system_identifier: 2,
        });
        let c = d(&SourceLineage::MysqlGtid { source_uuid: sid() });
        assert_ne!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn snapshot_null_distinct_by_column_type_and_from_present() {
        let d =
            |c| EventId::snapshot(&pg_lineage(), 1, "d", "t", &[iv("id", c)]);
        let null_int = d(IdentityCell::Null(IdentityKind::Int));
        let null_text = d(IdentityCell::Null(IdentityKind::Text));
        let present_zero = d(IdentityCell::Int(0));
        assert_ne!(null_int, null_text); // null differs by column type
        assert_ne!(null_int, present_zero); // null differs from a present value
    }

    #[test]
    fn snapshot_composite_key_order_and_names_matter() {
        let base = EventId::snapshot(
            &pg_lineage(),
            1,
            "d",
            "t",
            &[iv("a", IdentityCell::Int(1)), iv("b", IdentityCell::Int(2))],
        );
        // Swapped column order → different id.
        let swapped = EventId::snapshot(
            &pg_lineage(),
            1,
            "d",
            "t",
            &[iv("b", IdentityCell::Int(2)), iv("a", IdentityCell::Int(1))],
        );
        // Same order/values, different column name → different id.
        let renamed = EventId::snapshot(
            &pg_lineage(),
            1,
            "d",
            "t",
            &[iv("a", IdentityCell::Int(1)), iv("x", IdentityCell::Int(2))],
        );
        assert_ne!(base, swapped);
        assert_ne!(base, renamed);
    }

    #[test]
    fn snapshot_extended_identity_types_are_distinct_and_stable() {
        let d =
            |c| EventId::snapshot(&pg_lineage(), 1, "d", "t", &[iv("k", c)]);
        let uuid = [0xABu8; 16];
        let variants = [
            d(IdentityCell::Uuid(uuid)),
            d(IdentityCell::Decimal {
                negative: false,
                unscaled: &[1, 0],
                scale: 2,
            }),
            d(IdentityCell::Bool(true)),
            d(IdentityCell::Bool(false)),
            d(IdentityCell::DateTime {
                kind: TemporalKind::Date,
                value: 19_000,
            }),
            d(IdentityCell::DateTime {
                kind: TemporalKind::Timestamp,
                value: 19_000,
            }),
            d(IdentityCell::Enum {
                enum_type: "mood",
                label: "happy",
            }),
            // Same textual bytes as the UUID, but as raw Bytes → distinct kind.
            d(IdentityCell::Bytes(&uuid)),
        ];
        // All variants must be pairwise distinct.
        for (i, a) in variants.iter().enumerate() {
            for b in &variants[i + 1..] {
                assert_ne!(a, b);
            }
        }
        // Stable: same coordinates reproduce the same id.
        assert_eq!(
            d(IdentityCell::Uuid(uuid)),
            EventId::snapshot(
                &pg_lineage(),
                1,
                "d",
                "t",
                &[iv("k", IdentityCell::Uuid(uuid))]
            )
        );
    }

    #[test]
    fn decimal_scale_and_normalization() {
        let d = |neg, unscaled: &[u8], scale| {
            EventId::snapshot(
                &pg_lineage(),
                1,
                "d",
                "t",
                &[iv(
                    "k",
                    IdentityCell::Decimal {
                        negative: neg,
                        unscaled,
                        scale,
                    },
                )],
            )
        };
        // Leading zero bytes are normalized away (same value).
        assert_eq!(d(false, &[0, 0, 1, 0], 2), d(false, &[1, 0], 2));
        // Declared scale is part of identity: 1.0 (scale 1) != 1.00 (scale 2).
        assert_ne!(d(false, &[10], 1), d(false, &[100], 2));
        // Negative zero normalizes to positive zero.
        assert_eq!(d(true, &[0], 0), d(false, &[], 0));
        // Sign matters for non-zero.
        assert_ne!(d(true, &[5], 0), d(false, &[5], 0));
    }

    #[test]
    fn null_distinguishes_all_extended_kinds() {
        let kinds = [
            IdentityKind::Int,
            IdentityKind::UInt,
            IdentityKind::Text,
            IdentityKind::Bytes,
            IdentityKind::Uuid,
            IdentityKind::Decimal,
            IdentityKind::Bool,
            IdentityKind::DateTime,
            IdentityKind::Enum,
        ];
        let ids: Vec<_> = kinds
            .iter()
            .map(|k| {
                EventId::snapshot(
                    &pg_lineage(),
                    1,
                    "d",
                    "t",
                    &[iv("k", IdentityCell::Null(*k))],
                )
            })
            .collect();
        for (i, a) in ids.iter().enumerate() {
            for b in &ids[i + 1..] {
                assert_ne!(a, b, "null of distinct kinds must differ");
            }
        }
    }

    #[test]
    fn snapshot_generation_changes_id_but_is_stable_within_generation() {
        let at = |g| {
            EventId::snapshot(
                &pg_lineage(),
                g,
                "d",
                "t",
                &[iv("id", IdentityCell::Int(1))],
            )
        };
        assert_ne!(at(1), at(2)); // a resnapshot (new generation) mints new ids
        assert_eq!(at(1), at(1)); // resume reproduces the same ids
    }
}
