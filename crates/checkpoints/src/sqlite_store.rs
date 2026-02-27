//! SQLite checkpoint store with versioning support.
//!
//! All DB operations are dispatched via `tokio::task::spawn_blocking` so the
//! Tokio worker thread is never stalled by synchronous SQLite I/O.

use super::{CheckpointError, CheckpointResult, CheckpointStore, VersionInfo};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use std::path::Path;
use std::sync::{Arc, Mutex};

// ---------------------------------------------------------------------------
// Helper macro — reduces spawn_blocking boilerplate.
// ---------------------------------------------------------------------------

/// Spawn a blocking closure that receives a locked `&Connection`.
/// Returns `CheckpointResult<T>` where `T: Send + 'static`.
macro_rules! db {
    ($conn:expr, $body:expr) => {{
        let conn = Arc::clone(&$conn);
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().unwrap();
            ($body)(&*guard)
        })
        .await
        .map_err(|e| {
            CheckpointError::Database(format!("spawn_blocking panic: {e}"))
        })?
    }};
}

// ---------------------------------------------------------------------------
// Store
// ---------------------------------------------------------------------------

/// SQLite checkpoint store with version history.
///
/// Uses `spawn_blocking` for every DB call so async callers are never blocked.
/// The underlying connection is kept behind an `Arc<Mutex<Connection>>` so it
/// can be sent into blocking tasks without lifetime issues.
pub struct SqliteCheckpointStore {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteCheckpointStore {
    /// Create store at file path.
    pub fn new(path: impl AsRef<Path>) -> CheckpointResult<Self> {
        let conn = Connection::open(path.as_ref())
            .map_err(|e| CheckpointError::Database(e.to_string()))?;
        Self::init(&conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Create in-memory store (for testing).
    pub fn in_memory() -> CheckpointResult<Self> {
        let conn = Connection::open_in_memory()
            .map_err(|e| CheckpointError::Database(e.to_string()))?;
        Self::init(&conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    fn init(conn: &Connection) -> CheckpointResult<()> {
        conn.execute_batch(
            r#"
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;
            PRAGMA busy_timeout=5000;
            PRAGMA foreign_keys=ON;

            CREATE TABLE IF NOT EXISTS checkpoints (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                key         TEXT    NOT NULL,
                version     INTEGER NOT NULL,
                payload     BLOB    NOT NULL,
                created_at  TEXT    NOT NULL,
                UNIQUE(key, version)
            );
            CREATE INDEX IF NOT EXISTS idx_ckpt_key_ver
                ON checkpoints(key, version DESC);
            "#,
        )
        .map_err(|e| CheckpointError::Database(e.to_string()))?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// CheckpointStore impl
// ---------------------------------------------------------------------------

#[async_trait]
impl CheckpointStore for SqliteCheckpointStore {
    async fn get_raw(
        &self,
        source_id: &str,
    ) -> CheckpointResult<Option<Vec<u8>>> {
        let key = source_id.to_owned();
        db!(self.conn, move |conn: &Connection| {
            conn.query_row(
                "SELECT payload FROM checkpoints \
                 WHERE key = ?1 ORDER BY version DESC LIMIT 1",
                params![key],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| CheckpointError::Database(e.to_string()))
        })
    }

    async fn put_raw(
        &self,
        source_id: &str,
        bytes: &[u8],
    ) -> CheckpointResult<()> {
        self.put_raw_versioned(source_id, bytes).await?;
        Ok(())
    }

    async fn delete(&self, source_id: &str) -> CheckpointResult<bool> {
        let key = source_id.to_owned();
        db!(self.conn, move |conn: &Connection| {
            conn.execute("DELETE FROM checkpoints WHERE key = ?1", params![key])
                .map(|n| n > 0)
                .map_err(|e| CheckpointError::Database(e.to_string()))
        })
    }

    async fn list(&self) -> CheckpointResult<Vec<String>> {
        db!(self.conn, move |conn: &Connection| {
            let mut stmt = conn
                .prepare("SELECT DISTINCT key FROM checkpoints ORDER BY key")
                .map_err(|e| CheckpointError::Database(e.to_string()))?;
            let rows = stmt
                .query_map([], |row| row.get(0))
                .map_err(|e| CheckpointError::Database(e.to_string()))?;
            rows.map(|r| {
                r.map_err(|e| CheckpointError::Database(e.to_string()))
            })
            .collect()
        })
    }

    fn supports_versioning(&self) -> bool {
        true
    }

    async fn put_raw_versioned(
        &self,
        source_id: &str,
        bytes: &[u8],
    ) -> CheckpointResult<Option<u64>> {
        let key = source_id.to_owned();
        let payload = bytes.to_owned();

        db!(self.conn, move |conn: &Connection| {
            // Compute next version and insert in one transaction.
            let next_version: i64 = conn
                .query_row(
                    "SELECT COALESCE(MAX(version), 0) + 1 \
                     FROM checkpoints WHERE key = ?1",
                    params![key],
                    |row| row.get(0),
                )
                .map_err(|e| CheckpointError::Database(e.to_string()))?;

            conn.execute(
                "INSERT INTO checkpoints (key, version, payload, created_at) \
                 VALUES (?1, ?2, ?3, ?4)",
                params![key, next_version, payload, Utc::now().to_rfc3339()],
            )
            .map_err(|e| CheckpointError::Database(e.to_string()))?;

            Ok(Some(next_version as u64))
        })
    }

    async fn get_version_raw(
        &self,
        source_id: &str,
        version: u64,
    ) -> CheckpointResult<Option<Vec<u8>>> {
        let key = source_id.to_owned();
        let version_i64 = version as i64;
        db!(self.conn, move |conn: &Connection| {
            conn.query_row(
                "SELECT payload FROM checkpoints \
                 WHERE key = ?1 AND version = ?2",
                params![key, version_i64],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| CheckpointError::Database(e.to_string()))
        })
    }

    async fn list_versions(
        &self,
        source_id: &str,
    ) -> CheckpointResult<Vec<VersionInfo>> {
        let key = source_id.to_owned();
        db!(self.conn, move |conn: &Connection| {
            let mut stmt = conn
                .prepare(
                    "SELECT version, created_at, LENGTH(payload) \
                     FROM checkpoints WHERE key = ?1 ORDER BY version DESC",
                )
                .map_err(|e| CheckpointError::Database(e.to_string()))?;

            let rows = stmt
                .query_map(params![key], |row| {
                    let version_i64: i64 = row.get(0)?;
                    let ts_str: String = row.get(1)?;
                    let size_i64: i64 = row.get(2)?;
                    let created_at = DateTime::parse_from_rfc3339(&ts_str)
                        .map(|dt| dt.with_timezone(&Utc))
                        .unwrap_or_else(|_| Utc::now());
                    Ok(VersionInfo {
                        version: version_i64 as u64,
                        created_at,
                        size_bytes: size_i64 as usize,
                    })
                })
                .map_err(|e| CheckpointError::Database(e.to_string()))?;

            rows.map(|r| {
                r.map_err(|e| CheckpointError::Database(e.to_string()))
            })
            .collect()
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CheckpointStoreExt;

    #[tokio::test]
    async fn test_basic_crud() {
        let store = SqliteCheckpointStore::in_memory().unwrap();

        store.put_raw("k1", b"v1").await.unwrap();
        assert_eq!(store.get_raw("k1").await.unwrap().unwrap(), b"v1");

        assert!(store.delete("k1").await.unwrap());
        assert!(store.get_raw("k1").await.unwrap().is_none());

        // deleting non-existent key returns false
        assert!(!store.delete("k1").await.unwrap());
    }

    #[tokio::test]
    async fn test_versioning_and_rollback() {
        let store = SqliteCheckpointStore::in_memory().unwrap();

        let v1 = store.put_raw_versioned("k1", b"v1").await.unwrap();
        let v2 = store.put_raw_versioned("k1", b"v2").await.unwrap();
        let v3 = store.put_raw_versioned("k1", b"v3").await.unwrap();
        assert_eq!((v1, v2, v3), (Some(1), Some(2), Some(3)));

        // latest always wins
        assert_eq!(store.get_raw("k1").await.unwrap().unwrap(), b"v3");
        // point-in-time access
        assert_eq!(
            store.get_version_raw("k1", 1).await.unwrap().unwrap(),
            b"v1"
        );

        // rollback: copies v1 as new latest (v4)
        let new_ver = store.rollback("k1", 1).await.unwrap();
        assert_eq!(new_ver, Some(4));
        assert_eq!(store.get_raw("k1").await.unwrap().unwrap(), b"v1");

        let versions = store.list_versions("k1").await.unwrap();
        assert_eq!(versions.len(), 4);
        assert_eq!(versions[0].version, 4); // newest first
    }

    #[tokio::test]
    async fn test_multiple_keys_isolated() {
        let store = SqliteCheckpointStore::in_memory().unwrap();

        store.put_raw("pipeline-a", b"a1").await.unwrap();
        store.put_raw("pipeline-b", b"b1").await.unwrap();
        store.put_raw("pipeline-a", b"a2").await.unwrap();

        // each key has independent versioning
        assert_eq!(store.get_raw("pipeline-a").await.unwrap().unwrap(), b"a2");
        assert_eq!(store.get_raw("pipeline-b").await.unwrap().unwrap(), b"b1");

        let versions_a = store.list_versions("pipeline-a").await.unwrap();
        assert_eq!(versions_a.len(), 2);

        let keys = store.list().await.unwrap();
        assert_eq!(keys, vec!["pipeline-a", "pipeline-b"]);
    }

    #[tokio::test]
    async fn test_concurrent_writes_dont_deadlock() {
        // spawn_blocking means concurrent callers queue up on the Mutex
        // rather than deadlocking — verify this works under light concurrency.
        let store = Arc::new(SqliteCheckpointStore::in_memory().unwrap());
        let mut handles = vec![];

        for i in 0u8..8 {
            let s = Arc::clone(&store);
            handles.push(tokio::spawn(async move {
                s.put_raw("shared-key", &[i]).await.unwrap();
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        // 8 versions written, latest is whichever won the race
        let versions = store.list_versions("shared-key").await.unwrap();
        assert_eq!(versions.len(), 8);
    }
}
