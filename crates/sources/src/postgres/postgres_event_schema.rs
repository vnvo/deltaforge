//! The PostgreSQL event schema (`pg_event_v2`) and the physical Relation
//! binding (design `docs/design/postgres-catalog-capture.md`, sections 1-4 of
//! revisions 3-7).
//!
//! The event schema holds only what the pgoutput Relation message proves for
//! the rows that follow it: the replica identity and, per published column in
//! Relation order, the name, a stable type classification, the type modifier
//! and the key flag. It is the only schema that is fingerprinted, stamped on
//! events and given to encoders. Physical identity (the table OID and type
//! OIDs) lives in the separate Relation projection, whose digest keys the
//! durable binding from a Relation to its event schema version.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::postgres_builtin_types::builtin_type_name;
use super::postgres_object::RelationColumn;
use schema_registry::{SourceSchema, compute_fingerprint};

/// Format tag of the event schema; part of its content and fingerprint.
pub(crate) const PG_EVENT_V2: &str = "pg_event_v2";

/// Type OIDs below this are builtin (`FirstGenbkiObjectId`): pinned rows.
const FIRST_GENBKI_OBJECT_ID: u32 = 10_000;

/// Type classification for a non-builtin type: user-defined type names can
/// change without a new Relation, so no name is part of the content.
pub(crate) const USER_DEFINED: &str = "user_defined";

/// Bit 0 of a pgoutput Relation column's flags: part of the key.
const KEY_FLAG: u8 = 1;

/// One published column of the event schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgEventColumn {
    pub name: String,
    /// The builtin type name, or [`USER_DEFINED`].
    #[serde(rename = "type")]
    pub type_name: String,
    pub typmod: i32,
    pub key: bool,
}

/// The Relation-proven event schema (`pg_event_v2`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgEventSchema {
    pub format: String,
    /// pgoutput replica identity char: `d`, `f`, `i` or `n`.
    pub replica_identity: String,
    pub columns: Vec<PgEventColumn>,
}

/// Why a Relation cannot yield an event schema (fails closed).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EventSchemaError {
    #[error(
        "column {column:?} has builtin type OID {oid}, which is not in the \
         pinned builtin type table (unsupported PostgreSQL version?)"
    )]
    UnknownBuiltinType { column: String, oid: u32 },
    #[error("unknown replica identity {0:?}")]
    UnknownReplicaIdentity(char),
}

impl PgEventSchema {
    /// The event schema proven by `columns` and `replica_identity` of one
    /// Relation message.
    pub(crate) fn from_relation(
        replica_identity: char,
        columns: &[RelationColumn],
    ) -> Result<Self, EventSchemaError> {
        if !matches!(replica_identity, 'd' | 'f' | 'i' | 'n') {
            return Err(EventSchemaError::UnknownReplicaIdentity(
                replica_identity,
            ));
        }
        let columns = columns
            .iter()
            .map(|c| {
                let type_name = if c.type_oid < FIRST_GENBKI_OBJECT_ID {
                    builtin_type_name(c.type_oid)
                        .ok_or_else(|| EventSchemaError::UnknownBuiltinType {
                            column: c.name.clone(),
                            oid: c.type_oid,
                        })?
                        .to_string()
                } else {
                    USER_DEFINED.to_string()
                };
                Ok(PgEventColumn {
                    name: c.name.clone(),
                    type_name,
                    typmod: c.type_modifier,
                    key: c.flags & KEY_FLAG != 0,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            format: PG_EVENT_V2.to_string(),
            replica_identity: replica_identity.to_string(),
            columns,
        })
    }

    /// Whether column `i` is proven non-null: a key column under replica
    /// identity DEFAULT (the primary key) or INDEX (the replica identity
    /// index, whose columns PostgreSQL requires to be NOT NULL).
    pub fn proven_non_null(&self, i: usize) -> bool {
        self.columns.get(i).is_some_and(|c| c.key)
            && matches!(self.replica_identity.as_str(), "d" | "i")
    }
}

impl SourceSchema for PgEventSchema {
    fn source_kind(&self) -> &'static str {
        "postgres"
    }

    fn fingerprint(&self) -> String {
        compute_fingerprint(self)
    }

    fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }

    fn primary_key(&self) -> Vec<&str> {
        // Only a proven key: the key columns under DEFAULT or INDEX identity.
        if matches!(self.replica_identity.as_str(), "d" | "i") {
            self.columns
                .iter()
                .filter(|c| c.key)
                .map(|c| c.name.as_str())
                .collect()
        } else {
            vec![]
        }
    }
}

/// Character length declared by a `varchar`/`bpchar` type modifier.
pub fn char_max_length(type_name: &str, typmod: i32) -> Option<i32> {
    matches!(type_name, "varchar" | "bpchar")
        .then_some(typmod)
        .filter(|t| *t >= 4)
        .map(|t| t - 4)
}

/// `(precision, scale)` declared by a `numeric` type modifier. The scale is
/// an 11-bit signed field (negative scales exist since PostgreSQL 15).
pub fn numeric_precision_scale(
    type_name: &str,
    typmod: i32,
) -> Option<(i32, i32)> {
    if type_name != "numeric" || typmod < 4 {
        return None;
    }
    let t = typmod - 4;
    let precision = (t >> 16) & 0xffff;
    let scale = ((t & 0x7ff) ^ 1024) - 1024;
    Some((precision, scale))
}

/// The physical Relation projection: what binds WAL rows to an event schema
/// version. Never part of the public fingerprint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RelationProjection {
    pub oid: u32,
    pub replica_identity: char,
    /// `(name, type OID, typmod, key)` per column, in Relation order.
    pub columns: Vec<(String, u32, i32, bool)>,
}

impl RelationProjection {
    pub(crate) fn of(
        oid: u32,
        replica_identity: char,
        columns: &[RelationColumn],
    ) -> Self {
        Self {
            oid,
            replica_identity,
            columns: columns
                .iter()
                .map(|c| {
                    (
                        c.name.clone(),
                        c.type_oid,
                        c.type_modifier,
                        c.flags & KEY_FLAG != 0,
                    )
                })
                .collect(),
        }
    }

    /// SHA-256 under the domain `pg-relation-v1`, hex. Length-prefixed
    /// fields, so no two projections share an encoding.
    pub(crate) fn digest(&self) -> String {
        let mut h = Sha256::new();
        h.update(b"pg-relation-v1\0");
        h.update(self.oid.to_be_bytes());
        h.update([self.replica_identity as u8]);
        h.update((self.columns.len() as u32).to_be_bytes());
        for (name, type_oid, typmod, key) in &self.columns {
            h.update((name.len() as u32).to_be_bytes());
            h.update(name.as_bytes());
            h.update(type_oid.to_be_bytes());
            h.update(typmod.to_be_bytes());
            h.update([*key as u8]);
        }
        hex::encode(h.finalize())
    }

    /// Whether `other` has the same content (everything but the OID):
    /// a different OID with the same content is a replaced relation, not
    /// schema drift.
    pub(crate) fn same_content(&self, other: &Self) -> bool {
        self.replica_identity == other.replica_identity
            && self.columns == other.columns
    }
}

/// Durable binding of one Relation projection to its event schema version.
/// Deterministic bytes (no timestamps or positions), so a retried append is
/// byte-identical.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RelationBinding {
    pub format_version: u32,
    pub digest: String,
    pub oid: u32,
    pub replica_identity: String,
    /// `[name, type OID, typmod, key]` per column.
    pub columns: Vec<(String, u32, i32, bool)>,
    /// The registry version of the event schema.
    pub schema_version: i32,
    /// The event schema's public fingerprint.
    pub fingerprint: String,
}

impl RelationBinding {
    pub(crate) const FORMAT_VERSION: u32 = 1;

    pub(crate) fn new(
        projection: &RelationProjection,
        schema_version: i32,
        fingerprint: &str,
    ) -> Self {
        Self {
            format_version: Self::FORMAT_VERSION,
            digest: projection.digest(),
            oid: projection.oid,
            replica_identity: projection.replica_identity.to_string(),
            columns: projection.columns.clone(),
            schema_version,
            fingerprint: fingerprint.to_string(),
        }
    }

    pub(crate) fn projection(&self) -> Option<RelationProjection> {
        let mut chars = self.replica_identity.chars();
        let (Some(c), None) = (chars.next(), chars.next()) else {
            return None;
        };
        Some(RelationProjection {
            oid: self.oid,
            replica_identity: c,
            columns: self.columns.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, oid: u32, typmod: i32, flags: u8) -> RelationColumn {
        RelationColumn {
            name: name.into(),
            type_oid: oid,
            type_modifier: typmod,
            flags,
        }
    }

    fn orders() -> Vec<RelationColumn> {
        vec![
            col("id", 23, -1, 1),
            col("sku", 1043, 14, 0),
            col("price", 1700, 524_294, 0),
            col("mood", 16_386, -1, 0),
        ]
    }

    #[test]
    fn event_schema_is_relation_content_only() {
        let s = PgEventSchema::from_relation('d', &orders()).unwrap();
        assert_eq!(s.format, PG_EVENT_V2);
        let t: Vec<_> =
            s.columns.iter().map(|c| c.type_name.as_str()).collect();
        assert_eq!(t, ["int4", "varchar", "numeric", USER_DEFINED]);
        assert!(s.columns[0].key && !s.columns[1].key);
        assert!(s.proven_non_null(0));
        assert!(!s.proven_non_null(1));
        assert_eq!(s.primary_key(), ["id"]);
        assert_eq!(char_max_length("varchar", 14), Some(10));
        assert_eq!(numeric_precision_scale("numeric", 524_294), Some((8, 2)));
        // Negative scale (PostgreSQL 15+): numeric(4, -2).
        let typmod = ((4 << 16) | (-2i32 & 0x7ff)) + 4;
        assert_eq!(numeric_precision_scale("numeric", typmod), Some((4, -2)));
    }

    #[test]
    fn full_identity_proves_no_key_or_non_null() {
        let mut cols = orders();
        for c in &mut cols {
            c.flags = 1;
        }
        let s = PgEventSchema::from_relation('f', &cols).unwrap();
        assert!(s.primary_key().is_empty());
        assert!((0..cols.len()).all(|i| !s.proven_non_null(i)));
    }

    #[test]
    fn identical_content_under_two_oids_has_one_fingerprint_two_bindings() {
        let a = PgEventSchema::from_relation('d', &orders()).unwrap();
        let b = PgEventSchema::from_relation('d', &orders()).unwrap();
        assert_eq!(a.fingerprint(), b.fingerprint());
        let pa = RelationProjection::of(16_400, 'd', &orders());
        let pb = RelationProjection::of(16_500, 'd', &orders());
        assert_ne!(pa.digest(), pb.digest());
        assert!(pa.same_content(&pb));
    }

    #[test]
    fn a_user_type_rename_or_oid_never_changes_content() {
        let mut other = orders();
        other[3].type_oid = 17_000;
        let a = PgEventSchema::from_relation('d', &orders()).unwrap();
        let b = PgEventSchema::from_relation('d', &other).unwrap();
        assert_eq!(a.fingerprint(), b.fingerprint());
        assert_ne!(
            RelationProjection::of(1, 'd', &orders()).digest(),
            RelationProjection::of(1, 'd', &other).digest()
        );
    }

    #[test]
    fn every_relation_field_changes_the_content_or_digest() {
        let base = PgEventSchema::from_relation('d', &orders()).unwrap();
        let fp = base.fingerprint();
        let changed = |f: &dyn Fn(&mut Vec<RelationColumn>)| {
            let mut c = orders();
            f(&mut c);
            PgEventSchema::from_relation('d', &c).unwrap().fingerprint()
        };
        assert_ne!(changed(&|c| c[1].type_modifier = 24), fp);
        assert_ne!(changed(&|c| c[1].flags = 1), fp);
        assert_ne!(changed(&|c| c[1].name = "sku2".into()), fp);
        assert_ne!(changed(&|c| c[1].type_oid = 25), fp);
        assert_ne!(changed(&|c| c.swap(1, 2)), fp);
        assert_ne!(
            PgEventSchema::from_relation('f', &orders())
                .unwrap()
                .fingerprint(),
            fp
        );
    }

    #[test]
    fn an_unknown_builtin_oid_fails_closed() {
        let r = PgEventSchema::from_relation('d', &[col("x", 9_999, -1, 0)]);
        assert!(matches!(
            r,
            Err(EventSchemaError::UnknownBuiltinType { .. })
        ));
        assert!(PgEventSchema::from_relation('x', &orders()).is_err());
    }

    #[test]
    fn binding_bytes_are_deterministic_and_round_trip() {
        let p = RelationProjection::of(16_400, 'i', &orders());
        let b = RelationBinding::new(&p, 3, "sha256:ab");
        let bytes = serde_json::to_vec(&b).unwrap();
        assert_eq!(bytes, serde_json::to_vec(&b.clone()).unwrap());
        let back: RelationBinding = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back.projection().unwrap(), p);
    }
}
