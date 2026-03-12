//! Server identity persistence and change detection.
//!
//! `IdentityStore` is the single place responsible for answering:
//! "is this the same server we were connected to before?"
//!
//! It owns nothing except a `StorageBackend` reference and a namespace key.
//! All source-specific query logic lives in `mysql_health` / `postgres_health`;
//! all reconciliation logic lives above this module.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use storage::StorageBackend;

use crate::mysql::mysql_health::MySqlServerIdentity;
use crate::postgres::postgres_health::PostgresServerIdentity;

const NS: &str = "failover";

// ============================================================================
// ServerIdentity — unified enum over source-specific identity types
// ============================================================================

/// The stable identity of a source server, regardless of database type.
///
/// Serialized to JSON and stored in the KV backend under the `failover`
/// namespace. The variant tag ensures MySQL and PostgreSQL identities never
/// collide even if stored under the same source_id (which can't happen in
/// practice, but defensive is cheap here).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerIdentity {
    MySql(MySqlServerIdentity),
    Postgres(PostgresServerIdentity),
}

impl From<MySqlServerIdentity> for ServerIdentity {
    fn from(id: MySqlServerIdentity) -> Self {
        ServerIdentity::MySql(id)
    }
}

impl From<PostgresServerIdentity> for ServerIdentity {
    fn from(id: PostgresServerIdentity) -> Self {
        ServerIdentity::Postgres(id)
    }
}

// ============================================================================
// IdentityComparison — result of comparing stored vs live identity
// ============================================================================

#[derive(Debug, PartialEq)]
pub enum IdentityComparison {
    /// No stored identity — first connection after a clean start or wipe.
    /// Caller stores the live identity and proceeds normally.
    FirstSeen,
    /// Live identity matches stored identity — normal reconnect.
    Same,
    /// Live identity differs from stored — failover detected.
    Changed {
        previous: ServerIdentity,
        current: ServerIdentity,
    },
}

// ============================================================================
// IdentityStore
// ============================================================================

pub struct IdentityStore {
    backend: Arc<dyn StorageBackend>,
}

impl IdentityStore {
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        Self { backend }
    }

    fn key(source_id: &str) -> String {
        format!("identity:{source_id}")
    }

    /// Compare `live` against the stored identity for `source_id`.
    ///
    /// Returns the comparison result. Does **not** update storage — the caller
    /// decides when to persist (after successful reconciliation, not before).
    pub async fn compare(
        &self,
        source_id: &str,
        live: &ServerIdentity,
    ) -> Result<IdentityComparison> {
        let key = Self::key(source_id);
        let stored = self
            .backend
            .kv_get(NS, &key)
            .await
            .context("IdentityStore: kv_get failed")?;

        match stored {
            None => Ok(IdentityComparison::FirstSeen),
            Some(bytes) => {
                let previous: ServerIdentity = serde_json::from_slice(&bytes)
                    .context(
                    "IdentityStore: deserialize stored identity",
                )?;
                if &previous == live {
                    Ok(IdentityComparison::Same)
                } else {
                    Ok(IdentityComparison::Changed {
                        previous,
                        current: live.clone(),
                    })
                }
            }
        }
    }

    /// Persist `identity` as the current known-good identity for `source_id`.
    ///
    /// Called after successful reconciliation, not at detection time.
    /// Calling this before reconciliation completes would lose the "previous"
    /// identity needed to correlate failover events.
    pub async fn store(
        &self,
        source_id: &str,
        identity: &ServerIdentity,
    ) -> Result<()> {
        let key = Self::key(source_id);
        let bytes =
            serde_json::to_vec(identity).context("IdentityStore: serialize")?;
        self.backend
            .kv_put(NS, &key, &bytes)
            .await
            .context("IdentityStore: kv_put failed")
    }

    /// Clear stored identity for `source_id`.
    ///
    /// Used when a pipeline is fully reset (e.g. re-snapshot from scratch).
    /// After clearing, the next `compare` call returns `FirstSeen`.
    pub async fn clear(&self, source_id: &str) -> Result<bool> {
        self.backend
            .kv_delete(NS, &Self::key(source_id))
            .await
            .context("IdentityStore: kv_delete failed")
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use storage::MemoryStorageBackend;

    fn store() -> IdentityStore {
        IdentityStore::new(Arc::new(MemoryStorageBackend::new()))
    }

    fn mysql_id(uuid: &str) -> ServerIdentity {
        ServerIdentity::MySql(MySqlServerIdentity {
            server_uuid: uuid.into(),
        })
    }

    fn pg_id(n: i64) -> ServerIdentity {
        ServerIdentity::Postgres(PostgresServerIdentity {
            system_identifier: n,
        })
    }

    #[tokio::test]
    async fn first_seen_when_empty() {
        let s = store();
        let result = s.compare("src1", &mysql_id("abc")).await.unwrap();
        assert_eq!(result, IdentityComparison::FirstSeen);
    }

    #[tokio::test]
    async fn same_after_store() {
        let s = store();
        let id = mysql_id("abc-123");
        s.store("src1", &id).await.unwrap();
        assert_eq!(
            s.compare("src1", &id).await.unwrap(),
            IdentityComparison::Same
        );
    }

    #[tokio::test]
    async fn changed_on_different_identity() {
        let s = store();
        let old = mysql_id("server-a");
        let new = mysql_id("server-b");
        s.store("src1", &old).await.unwrap();

        match s.compare("src1", &new).await.unwrap() {
            IdentityComparison::Changed { previous, current } => {
                assert_eq!(previous, old);
                assert_eq!(current, new);
            }
            other => panic!("expected Changed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn store_is_not_called_on_compare() {
        // compare() must be read-only — storing happens only after reconciliation
        let s = store();
        let id = mysql_id("xyz");
        s.compare("src1", &id).await.unwrap(); // FirstSeen, no write
        // still FirstSeen — nothing was written
        assert_eq!(
            s.compare("src1", &id).await.unwrap(),
            IdentityComparison::FirstSeen
        );
    }

    #[tokio::test]
    async fn clear_resets_to_first_seen() {
        let s = store();
        let id = pg_id(7432167506173212672);
        s.store("src1", &id).await.unwrap();
        s.clear("src1").await.unwrap();
        assert_eq!(
            s.compare("src1", &id).await.unwrap(),
            IdentityComparison::FirstSeen
        );
    }

    #[tokio::test]
    async fn source_ids_are_isolated() {
        let s = store();
        s.store("src1", &mysql_id("a")).await.unwrap();
        // src2 has no stored identity
        assert_eq!(
            s.compare("src2", &mysql_id("a")).await.unwrap(),
            IdentityComparison::FirstSeen
        );
    }

    #[tokio::test]
    async fn mysql_and_postgres_identities_dont_collide() {
        // Serialized forms differ by type tag — same backing value won't match across types
        let s = store();
        s.store("src1", &mysql_id("same-value")).await.unwrap();
        let pg = ServerIdentity::Postgres(PostgresServerIdentity {
            system_identifier: 0,
        });
        // Different type → Changed, not Same
        match s.compare("src1", &pg).await.unwrap() {
            IdentityComparison::Changed { .. } => {}
            other => panic!("expected Changed, got {other:?}"),
        }
    }

    #[test]
    fn identity_serialization_round_trip() {
        let mysql = mysql_id("6ccd780c-baba-1026-9564-5b8c656024db");
        let pg = pg_id(7432167506173212672);

        for id in [mysql, pg] {
            let json = serde_json::to_string(&id).unwrap();
            let back: ServerIdentity = serde_json::from_str(&json).unwrap();
            assert_eq!(id, back);
        }
    }
}
