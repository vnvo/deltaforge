//! SQLite checkpoint store with versioning support.

use super::{CheckpointError, CheckpointResult, CheckpointStore, VersionInfo};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;
use std::sync::Mutex;

/// SQLite checkpoint store with version history.
///
/// Supports rollback to previous checkpoint versions.
pub struct SqliteCheckpointStore {
    conn: Mutex<Connection>,
}

impl SqliteCheckpointStore {
    /// Create store at file path.
    pub fn new(path: impl AsRef<Path>) -> CheckpointResult<Self> {
        let conn =
            Connection::open(path.as_ref()).map_err(|e| CheckpointError::Database(e.to_string()))?;
        Self::init(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Create in-memory store (for testing).
    pub fn in_memory() -> CheckpointResult<Self> {
        let conn =
            Connection::open_in_memory().map_err(|e| CheckpointError::Database(e.to_string()))?;
        Self::init(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    fn init(conn: &Connection) -> CheckpointResult<()> {
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS checkpoints (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key TEXT NOT NULL,
                version INTEGER NOT NULL,
                payload BLOB NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(key, version)
            );
            CREATE INDEX IF NOT EXISTS idx_ckpt_key_ver ON checkpoints(key, version DESC);
            "#,
        )
        .map_err(|e| CheckpointError::Database(e.to_string()))?;
        Ok(())
    }
}

#[async_trait]
impl CheckpointStore for SqliteCheckpointStore {
    async fn get_raw(&self, source_id: &str) -> CheckpointResult<Option<Vec<u8>>> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT payload FROM checkpoints WHERE key = ?1 ORDER BY version DESC LIMIT 1",
            params![source_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| CheckpointError::Database(e.to_string()))
    }

    async fn put_raw(&self, source_id: &str, bytes: &[u8]) -> CheckpointResult<()> {
        self.put_raw_versioned(source_id, bytes).await?;
        Ok(())
    }

    async fn delete(&self, source_id: &str) -> CheckpointResult<bool> {
        let conn = self.conn.lock().unwrap();
        let n = conn
            .execute("DELETE FROM checkpoints WHERE key = ?1", params![source_id])
            .map_err(|e| CheckpointError::Database(e.to_string()))?;
        Ok(n > 0)
    }

    async fn list(&self) -> CheckpointResult<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT DISTINCT key FROM checkpoints ORDER BY key")
            .map_err(|e| CheckpointError::Database(e.to_string()))?;
        let rows = stmt
            .query_map([], |row| row.get(0))
            .map_err(|e| CheckpointError::Database(e.to_string()))?;

        let mut keys = Vec::new();
        for row in rows {
            keys.push(row.map_err(|e| CheckpointError::Database(e.to_string()))?);
        }
        Ok(keys)
    }

    fn supports_versioning(&self) -> bool {
        true
    }

    async fn put_raw_versioned(
        &self,
        source_id: &str,
        bytes: &[u8],
    ) -> CheckpointResult<Option<u64>> {
        let conn = self.conn.lock().unwrap();

        let next_version: u64 = conn
            .query_row(
                "SELECT COALESCE(MAX(version), 0) + 1 FROM checkpoints WHERE key = ?1",
                params![source_id],
                |row| row.get(0),
            )
            .map_err(|e| CheckpointError::Database(e.to_string()))?;

        conn.execute(
            "INSERT INTO checkpoints (key, version, payload, created_at) VALUES (?1, ?2, ?3, ?4)",
            params![source_id, next_version, bytes, Utc::now().to_rfc3339()],
        )
        .map_err(|e| CheckpointError::Database(e.to_string()))?;

        Ok(Some(next_version))
    }

    async fn get_version_raw(
        &self,
        source_id: &str,
        version: u64,
    ) -> CheckpointResult<Option<Vec<u8>>> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT payload FROM checkpoints WHERE key = ?1 AND version = ?2",
            params![source_id, version],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| CheckpointError::Database(e.to_string()))
    }

    async fn list_versions(&self, source_id: &str) -> CheckpointResult<Vec<VersionInfo>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT version, created_at, LENGTH(payload) FROM checkpoints 
                 WHERE key = ?1 ORDER BY version DESC",
            )
            .map_err(|e| CheckpointError::Database(e.to_string()))?;

        let rows = stmt
            .query_map(params![source_id], |row| {
                let version: u64 = row.get(0)?;
                let ts_str: String = row.get(1)?;
                let size: usize = row.get(2)?;
                let created_at = DateTime::parse_from_rfc3339(&ts_str)
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or_else(|_| Utc::now());
                Ok(VersionInfo {
                    version,
                    created_at,
                    size_bytes: size,
                })
            })
            .map_err(|e| CheckpointError::Database(e.to_string()))?;

        let mut versions = Vec::new();
        for row in rows {
            versions.push(row.map_err(|e| CheckpointError::Database(e.to_string()))?);
        }
        Ok(versions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CheckpointStoreExt;

    #[tokio::test]
    async fn test_basic_ops() {
        let store = SqliteCheckpointStore::in_memory().unwrap();

        store.put_raw("k1", b"v1").await.unwrap();
        let val = store.get_raw("k1").await.unwrap().unwrap();
        assert_eq!(val, b"v1");

        assert!(store.delete("k1").await.unwrap());
        assert!(store.get_raw("k1").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_versioning() {
        let store = SqliteCheckpointStore::in_memory().unwrap();

        let v1 = store.put_raw_versioned("k1", b"v1").await.unwrap();
        let v2 = store.put_raw_versioned("k1", b"v2").await.unwrap();
        let v3 = store.put_raw_versioned("k1", b"v3").await.unwrap();

        assert_eq!(v1, Some(1));
        assert_eq!(v2, Some(2));
        assert_eq!(v3, Some(3));

        // Latest is v3
        assert_eq!(store.get_raw("k1").await.unwrap().unwrap(), b"v3");

        // Can get old version
        assert_eq!(
            store.get_version_raw("k1", 1).await.unwrap().unwrap(),
            b"v1"
        );

        // List versions
        let versions = store.list_versions("k1").await.unwrap();
        assert_eq!(versions.len(), 3);
        assert_eq!(versions[0].version, 3);
    }

    #[tokio::test]
    async fn test_rollback() {
        let store = SqliteCheckpointStore::in_memory().unwrap();

        store.put_raw("k1", b"v1").await.unwrap();
        store.put_raw("k1", b"v2").await.unwrap();
        store.put_raw("k1", b"v3").await.unwrap();

        // Rollback to v1
        let new_ver = store.rollback("k1", 1).await.unwrap();
        assert_eq!(new_ver, Some(4));

        // Latest is now v1's data
        assert_eq!(store.get_raw("k1").await.unwrap().unwrap(), b"v1");
    }

    #[tokio::test]
    async fn test_typed_access() {
        let store = SqliteCheckpointStore::in_memory().unwrap();

        #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
        struct Ckpt {
            file: String,
            pos: u64,
        }

        let ckpt = Ckpt {
            file: "bin.001".into(),
            pos: 12345,
        };
        store.put("src1", ckpt).await.unwrap();

        let loaded: Ckpt = store.get("src1").await.unwrap().unwrap();
        assert_eq!(loaded.file, "bin.001");
        assert_eq!(loaded.pos, 12345);
    }
}