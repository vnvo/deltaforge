//! SQLite `StorageBackend` for single-node production deployments.
//!
//! Uses WAL mode with a single `Arc<Mutex<Connection>>` — all reads and writes
//! are serialized through it via `spawn_blocking`, matching the pattern used by
//! `checkpoints/src/sqlite_store.rs`. This avoids a second `rusqlite`/
//! `libsqlite3-sys` dependency version in the workspace.
//!
//! A background task sweeps expired KV entries every 60 seconds.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use rusqlite::{Connection, OptionalExtension, params};
use tokio::task::JoinHandle;
use tracing::{error, info};

use crate::StorageBackend;

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

// ── Backend ───────────────────────────────────────────────────────────────────

pub struct SqliteStorageBackend {
    conn: Arc<Mutex<Connection>>,
    _sweep_handle: JoinHandle<()>,
}

const SCHEMA: &str = r#"
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA busy_timeout=5000;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS df_kv (
    ns          TEXT    NOT NULL,
    key         TEXT    NOT NULL,
    val         BLOB    NOT NULL,
    updated_at  INTEGER NOT NULL DEFAULT (unixepoch()),
    expires_at  INTEGER,
    PRIMARY KEY (ns, key)
);
CREATE INDEX IF NOT EXISTS df_kv_expires ON df_kv(expires_at) WHERE expires_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS df_log (
    seq  INTEGER PRIMARY KEY AUTOINCREMENT,
    ns   TEXT    NOT NULL,
    key  TEXT    NOT NULL,
    val  BLOB    NOT NULL,
    ts   INTEGER NOT NULL DEFAULT (unixepoch())
);
CREATE INDEX IF NOT EXISTS df_log_ns_key_seq ON df_log(ns, key, seq);

CREATE TABLE IF NOT EXISTS df_slot (
    ns          TEXT    NOT NULL,
    key         TEXT    NOT NULL,
    version     INTEGER NOT NULL DEFAULT 1,
    state       BLOB    NOT NULL,
    updated_at  INTEGER NOT NULL DEFAULT (unixepoch()),
    PRIMARY KEY (ns, key)
);

CREATE TABLE IF NOT EXISTS df_queue (
    id   INTEGER PRIMARY KEY AUTOINCREMENT,
    ns   TEXT    NOT NULL,
    key  TEXT    NOT NULL,
    val  BLOB    NOT NULL,
    ts   INTEGER NOT NULL DEFAULT (unixepoch())
);
CREATE INDEX IF NOT EXISTS df_queue_ns_key_id ON df_queue(ns, key, id);
"#;

impl SqliteStorageBackend {
    /// Open (or create) a SQLite database at the given path.
    pub fn open(path: impl AsRef<Path>) -> Result<Arc<Self>> {
        let conn = Connection::open(path)?;
        Self::init(conn)
    }

    /// In-memory database — for testing only.
    pub fn in_memory() -> Result<Arc<Self>> {
        let conn = Connection::open_in_memory()?;
        Self::init(conn)
    }

    fn init(conn: Connection) -> Result<Arc<Self>> {
        conn.execute_batch(SCHEMA)?;

        let conn = Arc::new(Mutex::new(conn));

        // TTL sweep every 60s
        let sweep_conn = Arc::clone(&conn);
        let sweep_handle = tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                interval.tick().await;
                let c = Arc::clone(&sweep_conn);
                let res = tokio::task::spawn_blocking(move || {
                    let conn = c.lock().unwrap();
                    conn.execute(
                        "DELETE FROM df_kv WHERE expires_at IS NOT NULL AND expires_at < ?1",
                        params![now_secs()],
                    )
                })
                .await;
                if let Err(e) = res {
                    error!("TTL sweep error: {e}");
                }
            }
        });

        info!("SQLite storage backend initialized");
        Ok(Arc::new(Self {
            conn,
            _sweep_handle: sweep_handle,
        }))
    }
}

/// Dispatch a closure to the blocking thread pool with the locked connection.
macro_rules! db {
    ($self:expr, $body:expr) => {{
        let conn = Arc::clone(&$self.conn);
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().unwrap();
            ($body)(&*guard)
        })
        .await
        .map_err(|e| anyhow!("spawn_blocking panic: {e}"))?
    }};
}

#[async_trait]
impl StorageBackend for SqliteStorageBackend {
    // ── KV ──────────────────────────────────────────────────────────────────

    async fn kv_get(&self, ns: &str, key: &str) -> Result<Option<Vec<u8>>> {
        let ns = ns.to_string();
        let key = key.to_string();
        let now = now_secs();
        db!(self, move |conn: &Connection| {
            let row: Option<(Vec<u8>, Option<i64>)> = conn
                .query_row(
                    "SELECT val, expires_at FROM df_kv WHERE ns=?1 AND key=?2",
                    params![ns, key],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            Ok(row.and_then(|(val, exp)| {
                if exp.is_some_and(|e| e <= now) {
                    None
                } else {
                    Some(val)
                }
            }))
        })
    }

    async fn kv_put(&self, ns: &str, key: &str, value: &[u8]) -> Result<()> {
        let ns = ns.to_string();
        let key = key.to_string();
        let value = value.to_vec();
        db!(self, move |conn: &Connection| {
            conn.execute(
                "INSERT INTO df_kv(ns, key, val, updated_at, expires_at)
                 VALUES(?1, ?2, ?3, ?4, NULL)
                 ON CONFLICT(ns, key) DO UPDATE SET
                   val=excluded.val, updated_at=excluded.updated_at, expires_at=NULL",
                params![ns, key, value, now_secs()],
            )?;
            Ok(())
        })
    }

    async fn kv_put_with_ttl(
        &self,
        ns: &str,
        key: &str,
        value: &[u8],
        ttl_secs: u64,
    ) -> Result<()> {
        let ns = ns.to_string();
        let key = key.to_string();
        let value = value.to_vec();
        let expires_at = now_secs() + ttl_secs as i64;
        db!(self, move |conn: &Connection| {
            conn.execute(
                "INSERT INTO df_kv(ns, key, val, updated_at, expires_at)
                 VALUES(?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(ns, key) DO UPDATE SET
                   val=excluded.val, updated_at=excluded.updated_at, expires_at=excluded.expires_at",
                params![ns, key, value, now_secs(), expires_at],
            )?;
            Ok(())
        })
    }

    async fn kv_delete(&self, ns: &str, key: &str) -> Result<bool> {
        let ns = ns.to_string();
        let key = key.to_string();
        db!(self, move |conn: &Connection| {
            Ok(conn.execute(
                "DELETE FROM df_kv WHERE ns=?1 AND key=?2",
                params![ns, key],
            )? > 0)
        })
    }

    async fn kv_list(
        &self,
        ns: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<String>> {
        let ns = ns.to_string();
        let prefix_pat = prefix.map(|p| format!("{p}%"));
        let now = now_secs();
        db!(self, move |conn: &Connection| {
            let mut keys = Vec::new();
            if let Some(pat) = &prefix_pat {
                let mut stmt = conn.prepare(
                    "SELECT key FROM df_kv WHERE ns=?1 AND key LIKE ?2
                     AND (expires_at IS NULL OR expires_at > ?3) ORDER BY key",
                )?;
                let rows =
                    stmt.query_map(params![ns, pat, now], |r| r.get(0))?;
                for r in rows {
                    keys.push(r?);
                }
            } else {
                let mut stmt = conn.prepare(
                    "SELECT key FROM df_kv WHERE ns=?1
                     AND (expires_at IS NULL OR expires_at > ?2) ORDER BY key",
                )?;
                let rows = stmt.query_map(params![ns, now], |r| r.get(0))?;
                for r in rows {
                    keys.push(r?);
                }
            }
            Ok(keys)
        })
    }

    // ── Log ─────────────────────────────────────────────────────────────────

    async fn log_append(
        &self,
        ns: &str,
        key: &str,
        value: &[u8],
    ) -> Result<u64> {
        let ns = ns.to_string();
        let key = key.to_string();
        let value = value.to_vec();
        db!(self, move |conn: &Connection| {
            conn.execute(
                "INSERT INTO df_log(ns, key, val, ts) VALUES(?1, ?2, ?3, ?4)",
                params![ns, key, value, now_secs()],
            )?;
            Ok(conn.last_insert_rowid() as u64)
        })
    }

    async fn log_list(
        &self,
        ns: &str,
        key: &str,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        let ns = ns.to_string();
        let key = key.to_string();
        db!(self, move |conn: &Connection| {
            query_log(conn, &ns, &key, None)
        })
    }

    async fn log_since(
        &self,
        ns: &str,
        key: &str,
        since_seq: u64,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        let ns = ns.to_string();
        let key = key.to_string();
        db!(self, move |conn: &Connection| {
            query_log(conn, &ns, &key, Some(since_seq))
        })
    }

    async fn log_latest(
        &self,
        ns: &str,
        key: &str,
    ) -> Result<Option<(u64, Vec<u8>)>> {
        let ns = ns.to_string();
        let key = key.to_string();
        db!(self, move |conn: &Connection| {
            conn.query_row(
                "SELECT seq, val FROM df_log WHERE ns=?1 AND key=?2 ORDER BY seq DESC LIMIT 1",
                params![ns, key],
                |r| Ok((r.get::<_, i64>(0)? as u64, r.get(1)?)),
            )
            .optional()
            .map_err(Into::into)
        })
    }

    // ── Slot ────────────────────────────────────────────────────────────────

    async fn slot_upsert(
        &self,
        ns: &str,
        key: &str,
        state: &[u8],
    ) -> Result<u64> {
        let ns = ns.to_string();
        let key = key.to_string();
        let state = state.to_vec();
        db!(self, move |conn: &Connection| {
            conn.execute(
                "INSERT INTO df_slot(ns, key, version, state, updated_at)
                 VALUES(?1, ?2, 1, ?3, ?4)
                 ON CONFLICT(ns, key) DO UPDATE SET
                   version=df_slot.version+1, state=excluded.state, updated_at=excluded.updated_at",
                params![ns, key, state, now_secs()],
            )?;
            let v: i64 = conn.query_row(
                "SELECT version FROM df_slot WHERE ns=?1 AND key=?2",
                params![ns, key],
                |r| r.get(0),
            )?;
            Ok(v as u64)
        })
    }

    async fn slot_get(
        &self,
        ns: &str,
        key: &str,
    ) -> Result<Option<(u64, Vec<u8>)>> {
        let ns = ns.to_string();
        let key = key.to_string();
        db!(self, move |conn: &Connection| {
            conn.query_row(
                "SELECT version, state FROM df_slot WHERE ns=?1 AND key=?2",
                params![ns, key],
                |r| Ok((r.get::<_, i64>(0)? as u64, r.get(1)?)),
            )
            .optional()
            .map_err(Into::into)
        })
    }

    async fn slot_cas(
        &self,
        ns: &str,
        key: &str,
        expected_version: u64,
        state: &[u8],
    ) -> Result<bool> {
        let ns = ns.to_string();
        let key = key.to_string();
        let state = state.to_vec();
        db!(self, move |conn: &Connection| {
            let n = conn.execute(
                "UPDATE df_slot SET state=?1, version=version+1, updated_at=?2
                 WHERE ns=?3 AND key=?4 AND version=?5",
                params![state, now_secs(), ns, key, expected_version as i64],
            )?;
            Ok(n == 1)
        })
    }

    async fn slot_delete(&self, ns: &str, key: &str) -> Result<bool> {
        let ns = ns.to_string();
        let key = key.to_string();
        db!(self, move |conn: &Connection| {
            Ok(conn.execute(
                "DELETE FROM df_slot WHERE ns=?1 AND key=?2",
                params![ns, key],
            )? > 0)
        })
    }

    // ── Queue ────────────────────────────────────────────────────────────────

    async fn queue_push(
        &self,
        ns: &str,
        key: &str,
        value: &[u8],
    ) -> Result<u64> {
        let ns = ns.to_string();
        let key = key.to_string();
        let value = value.to_vec();
        db!(self, move |conn: &Connection| {
            conn.execute(
                "INSERT INTO df_queue(ns, key, val, ts) VALUES(?1, ?2, ?3, ?4)",
                params![ns, key, value, now_secs()],
            )?;
            Ok(conn.last_insert_rowid() as u64)
        })
    }

    async fn queue_peek(
        &self,
        ns: &str,
        key: &str,
        limit: usize,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        let ns = ns.to_string();
        let key = key.to_string();
        db!(self, move |conn: &Connection| {
            let mut stmt = conn.prepare(
                "SELECT id, val FROM df_queue WHERE ns=?1 AND key=?2 ORDER BY id ASC LIMIT ?3",
            )?;
            let rows = stmt.query_map(params![ns, key, limit as i64], |r| {
                Ok((r.get::<_, i64>(0)? as u64, r.get(1)?))
            })?;
            rows.map(|r| r.map_err(Into::into)).collect()
        })
    }

    async fn queue_ack(
        &self,
        ns: &str,
        key: &str,
        up_to_id: u64,
    ) -> Result<usize> {
        let ns = ns.to_string();
        let key = key.to_string();
        db!(self, move |conn: &Connection| {
            Ok(conn.execute(
                "DELETE FROM df_queue WHERE ns=?1 AND key=?2 AND id<=?3",
                params![ns, key, up_to_id as i64],
            )?)
        })
    }

    async fn queue_len(&self, ns: &str, key: &str) -> Result<u64> {
        let ns = ns.to_string();
        let key = key.to_string();
        db!(self, move |conn: &Connection| {
            let n: i64 = conn.query_row(
                "SELECT COUNT(*) FROM df_queue WHERE ns=?1 AND key=?2",
                params![ns, key],
                |r| r.get(0),
            )?;
            Ok(n as u64)
        })
    }

    async fn queue_drop_oldest(
        &self,
        ns: &str,
        key: &str,
        count: usize,
    ) -> Result<usize> {
        let ns = ns.to_string();
        let key = key.to_string();
        db!(self, move |conn: &Connection| {
            Ok(conn.execute(
                "DELETE FROM df_queue WHERE rowid IN (
                   SELECT rowid FROM df_queue WHERE ns=?1 AND key=?2
                   ORDER BY id ASC LIMIT ?3
                 )",
                params![ns, key, count as i64],
            )?)
        })
    }
}

fn query_log(
    conn: &Connection,
    ns: &str,
    key: &str,
    since_seq: Option<u64>,
) -> Result<Vec<(u64, Vec<u8>)>> {
    let entries = if let Some(since) = since_seq {
        let mut stmt = conn.prepare(
            "SELECT seq, val FROM df_log WHERE ns=?1 AND key=?2 AND seq>?3 ORDER BY seq ASC",
        )?;
        stmt.query_map(params![ns, key, since as i64], |r| {
            Ok((r.get::<_, i64>(0)? as u64, r.get(1)?))
        })?
        .map(|r| r.map_err(Into::into))
        .collect::<Result<Vec<_>>>()?
    } else {
        let mut stmt = conn
            .prepare("SELECT seq, val FROM df_log WHERE ns=?1 AND key=?2 ORDER BY seq ASC")?;
        stmt.query_map(params![ns, key], |r| {
            Ok((r.get::<_, i64>(0)? as u64, r.get(1)?))
        })?
        .map(|r| r.map_err(Into::into))
        .collect::<Result<Vec<_>>>()?
    };
    Ok(entries)
}
