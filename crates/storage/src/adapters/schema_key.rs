//! Qualified schema-registry keys bound to verified physical source lineage.
//!
//! The durable schema-registry key is
//! `v1/<enc tenant>/<enc source_id>/<lineage-hash>/<enc db>/<enc table>`.
//!
//! The `lineage-hash` binds every schema stream to the *verified physical*
//! source, not just the operator-controlled `source_id`: a replaced database
//! that reuses a `source_id` produces a different hash and therefore a fresh,
//! empty namespace instead of silently inheriting another server's schemas.

use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Durable key format version for the schema registry.
pub const FORMAT_VERSION: &str = "v1";

/// Domain separator for the lineage hash. Distinct from the event-id domain so
/// the two hash spaces can never be confused.
const LINEAGE_DOMAIN: &[u8] = b"DeltaForge.SchemaRegistry.Lineage.v1\0";

/// Characters escaped in a key segment so no segment value can forge a `/`
/// boundary or an escape introducer. `%` must be escaped for reversibility.
const SEGMENT_ESCAPE: &AsciiSet = &CONTROLS.add(b'%').add(b'/');

fn enc(segment: &str) -> String {
    utf8_percent_encode(segment, SEGMENT_ESCAPE).to_string()
}

/// Percent-encode a key segment so it cannot forge a `/` boundary. Shared with
/// sibling durable records that address by `(tenant, source_id)`.
pub(crate) fn encode_segment(segment: &str) -> String {
    enc(segment)
}

/// Verified physical source lineage used to derive the registry key's lineage
/// segment.
///
/// This is intentionally distinct from the event-id `SourceLineage`: for
/// PostgreSQL it also carries `database_oid`, so two databases in the same
/// cluster (same `system_identifier`) never share a schema namespace.
///
/// MySQL is identified by `server_uuid` in both GTID and non-GTID modes
/// (`@@server_uuid` does not require GTID). There is deliberately no
/// `server_id`-based variant: `server_id` is operator-assigned and routinely
/// reused after replacement or restore, so it cannot prove physical identity.
/// A MySQL source without a stable, non-zero `server_uuid` has no durable
/// lineage and must fail closed for qualified-registry access. The
/// `(server_id, binlog file)` pair remains a position/anchor descriptor only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LineageDescriptor {
    Postgres {
        system_identifier: u64,
        database_oid: u64,
    },
    Mysql {
        server_uuid: String,
    },
}

impl LineageDescriptor {
    /// Verified PostgreSQL lineage. Fails closed on a zero `system_identifier`
    /// or `database_oid`, which would indicate an unverified identity.
    pub fn postgres(
        system_identifier: u64,
        database_oid: u64,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            system_identifier != 0 && database_oid != 0,
            "refusing schema-registry access: unverified PostgreSQL lineage \
             (system_identifier={system_identifier}, database_oid={database_oid})"
        );
        Ok(Self::Postgres {
            system_identifier,
            database_oid,
        })
    }

    /// Verified MySQL lineage from `@@server_uuid`. Fails closed on an empty or
    /// all-zero UUID: without a stable UUID there is no durable physical
    /// identity, and `server_id` is not an acceptable substitute.
    pub fn mysql(server_uuid: &str) -> anyhow::Result<Self> {
        let u = server_uuid.trim();
        let all_zero = u.chars().all(|c| c == '0' || c == '-');
        anyhow::ensure!(
            !u.is_empty() && !all_zero,
            "refusing schema-registry access: MySQL server has no stable \
             server_uuid; server_id cannot prove physical identity"
        );
        Ok(Self::Mysql {
            server_uuid: u.to_ascii_lowercase(),
        })
    }

    /// Re-check a descriptor read back from durable storage with the same rules
    /// the constructors enforce (non-zero PostgreSQL identifiers; a non-empty,
    /// non-zero, normalized MySQL UUID). A stored descriptor that would not
    /// have been constructible is refused.
    pub fn validate(&self) -> anyhow::Result<()> {
        let rebuilt = match self {
            Self::Postgres {
                system_identifier,
                database_oid,
            } => Self::postgres(*system_identifier, *database_oid)?,
            Self::Mysql { server_uuid } => Self::mysql(server_uuid)?,
        };
        anyhow::ensure!(
            &rebuilt == self,
            "lineage descriptor is not in canonical form: {self:?}"
        );
        Ok(())
    }

    /// Stable, domain-separated hex hash of the verified physical lineage.
    ///
    /// A one-byte variant tag ensures PostgreSQL and MySQL identities occupy
    /// disjoint hash spaces; fields are length-prefixed where variable-length so
    /// no two distinct descriptors can produce the same pre-image.
    pub fn lineage_hash(&self) -> String {
        let mut h = Sha256::new();
        h.update(LINEAGE_DOMAIN);
        match self {
            LineageDescriptor::Postgres {
                system_identifier,
                database_oid,
            } => {
                h.update([0x01]);
                h.update(system_identifier.to_be_bytes());
                h.update(database_oid.to_be_bytes());
            }
            LineageDescriptor::Mysql { server_uuid } => {
                h.update([0x02]);
                let b = server_uuid.as_bytes();
                h.update((b.len() as u64).to_be_bytes());
                h.update(b);
            }
        }
        hex::encode(&h.finalize()[..16])
    }
}

/// A fully-qualified schema-registry key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaKey {
    pub tenant: String,
    pub source_id: String,
    pub lineage_hash: String,
    /// PostgreSQL: the schema name. MySQL: the database name.
    pub db: String,
    pub table: String,
}

impl SchemaKey {
    pub fn new(
        tenant: impl Into<String>,
        source_id: impl Into<String>,
        lineage_hash: impl Into<String>,
        db: impl Into<String>,
        table: impl Into<String>,
    ) -> Self {
        Self {
            tenant: tenant.into(),
            source_id: source_id.into(),
            lineage_hash: lineage_hash.into(),
            db: db.into(),
            table: table.into(),
        }
    }

    /// The durable backend key string. Every non-hash segment is percent-encoded
    /// so a value containing `/` or `%` cannot forge a segment boundary.
    pub fn backend_key(&self) -> String {
        format!(
            "{}/{}/{}/{}/{}/{}",
            FORMAT_VERSION,
            enc(&self.tenant),
            enc(&self.source_id),
            self.lineage_hash,
            enc(&self.db),
            enc(&self.table)
        )
    }

    /// Prefix that scopes enumeration to one `(tenant, source_id, lineage)`.
    pub fn source_prefix(
        tenant: &str,
        source_id: &str,
        lineage_hash: &str,
    ) -> String {
        format!(
            "{}/{}/{}/{}/",
            FORMAT_VERSION,
            enc(tenant),
            enc(source_id),
            lineage_hash
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_and_mysql_lineage_never_collide() {
        let pg = LineageDescriptor::Postgres {
            system_identifier: 7,
            database_oid: 7,
        };
        let my = LineageDescriptor::Mysql {
            server_uuid: "7".into(),
        };
        assert_ne!(pg.lineage_hash(), my.lineage_hash());
    }

    #[test]
    fn pg_hash_depends_on_both_system_identifier_and_database_oid() {
        let base = LineageDescriptor::Postgres {
            system_identifier: 100,
            database_oid: 200,
        };
        let diff_sysid = LineageDescriptor::Postgres {
            system_identifier: 101,
            database_oid: 200,
        };
        let diff_dboid = LineageDescriptor::Postgres {
            system_identifier: 100,
            database_oid: 201,
        };
        assert_ne!(base.lineage_hash(), diff_sysid.lineage_hash());
        assert_ne!(
            base.lineage_hash(),
            diff_dboid.lineage_hash(),
            "database_oid must be mixed in so two DBs in one cluster differ"
        );
    }

    #[test]
    fn mysql_hash_depends_on_server_uuid() {
        let a =
            LineageDescriptor::mysql("3E11FA47-71CA-11E1-9E33-C80AA9429562")
                .unwrap();
        let b =
            LineageDescriptor::mysql("3e11fa47-71ca-11e1-9e33-c80aa9429563")
                .unwrap();
        assert_ne!(a.lineage_hash(), b.lineage_hash());
        // Case-normalized: the same UUID in either case is the same lineage.
        let a_lower =
            LineageDescriptor::mysql("3e11fa47-71ca-11e1-9e33-c80aa9429562")
                .unwrap();
        assert_eq!(a.lineage_hash(), a_lower.lineage_hash());
    }

    #[test]
    fn mysql_without_stable_uuid_fails_closed() {
        assert!(LineageDescriptor::mysql("").is_err());
        assert!(LineageDescriptor::mysql("   ").is_err());
        assert!(
            LineageDescriptor::mysql("00000000-0000-0000-0000-000000000000")
                .is_err()
        );
    }

    #[test]
    fn postgres_unverified_identity_fails_closed() {
        assert!(LineageDescriptor::postgres(0, 5).is_err());
        assert!(LineageDescriptor::postgres(5, 0).is_err());
        assert!(LineageDescriptor::postgres(5, 6).is_ok());
    }

    #[test]
    fn hash_is_stable_and_hex() {
        let d = LineageDescriptor::Postgres {
            system_identifier: 42,
            database_oid: 99,
        };
        let h = d.lineage_hash();
        assert_eq!(h, d.lineage_hash());
        assert!(h.chars().all(|c| c.is_ascii_hexdigit()));
        assert!(!h.is_empty());
    }

    #[test]
    fn segment_separators_cannot_be_forged() {
        // A table literally named "a/b" must not collide with db="a", table="b".
        let k1 = SchemaKey::new("t", "s", "abcd", "db", "a/b");
        let k2 = SchemaKey::new("t", "s", "abcd", "db/a", "b");
        assert_ne!(
            k1.backend_key(),
            k2.backend_key(),
            "'/' inside a segment must be escaped so boundaries are unambiguous"
        );
    }

    #[test]
    fn percent_cannot_be_forged() {
        // A raw "%2F" in a name must not be confused with an encoded '/'.
        let raw = SchemaKey::new("t", "s", "abcd", "db", "a%2Fb");
        let slash = SchemaKey::new("t", "s", "abcd", "db", "a/b");
        assert_ne!(raw.backend_key(), slash.backend_key());
    }

    #[test]
    fn backend_key_has_format_prefix_and_six_segments() {
        let k = SchemaKey::new("acme", "src1", "deadbeef", "public", "orders");
        let key = k.backend_key();
        assert!(key.starts_with("v1/"));
        assert_eq!(key.split('/').count(), 6);
    }

    #[test]
    fn source_prefix_is_a_prefix_of_matching_keys() {
        let k = SchemaKey::new("acme", "src1", "deadbeef", "public", "orders");
        let prefix = SchemaKey::source_prefix("acme", "src1", "deadbeef");
        assert!(
            k.backend_key().starts_with(&prefix),
            "{} should start with {}",
            k.backend_key(),
            prefix
        );
    }

    #[test]
    fn source_prefix_excludes_other_sources() {
        let k = SchemaKey::new("acme", "src2", "deadbeef", "public", "orders");
        let prefix = SchemaKey::source_prefix("acme", "src1", "deadbeef");
        assert!(!k.backend_key().starts_with(&prefix));
    }
}
