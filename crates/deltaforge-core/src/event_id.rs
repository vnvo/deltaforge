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
    /// A component count for a following list.
    fn count(&mut self, n: u16) -> &mut Self {
        self.buf.extend_from_slice(&n.to_be_bytes());
        self
    }
}

/// Immutable source lineage, mixed into DDL / logical-message identity so the
/// same textual position in two independent clusters/lineages cannot collide.
/// (Row and snapshot ids already embed lineage via the GTID SID /
/// `system_identifier` / snapshot generation.)
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

    /// Snapshot row: snapshot generation + table identity + primary-key values
    /// (in declared order).
    pub fn snapshot(
        generation: u64,
        db: &str,
        table: &str,
        primary_key: &[String],
    ) -> Self {
        let mut c = Coordinates::default();
        c.u64(generation)
            .str(db)
            .str(table)
            .count(primary_key.len() as u16);
        for v in primary_key {
            c.str(v);
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
        let id = EventId::snapshot(1, "orders", "customers", &["42".into()]);
        assert_eq!(
            id.to_string(),
            "dfid:v1:snap:67994950b9ad4240be0a4e73135c5257"
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
            EventId::snapshot(2, "shop", "orders", &["a".into(), "b".into()]),
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
                &EventId::snapshot(1, "d", "t", &["1".into()]),
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
        let id = EventId::snapshot(1, "orders", "customers", &["42".into()]);
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "\"dfid:v1:snap:67994950b9ad4240be0a4e73135c5257\"");
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
        let mut set = HashSet::new();
        set.insert(EventId::snapshot(1, "d", "t", &["1".into()]));
        assert!(set.contains(&EventId::snapshot(1, "d", "t", &["1".into()])));
        assert!(!set.contains(&EventId::snapshot(1, "d", "t", &["2".into()])));
    }
}
