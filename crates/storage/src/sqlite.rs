//! SQLite `StorageBackend` for single-node production deployments.
//!
//! Uses WAL mode with a single `Arc<Mutex<Connection>>` - all reads and writes
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

use crate::{
    AppendStatus, LogAppendOutcome, LogEntryMeta, LogError, LogStreamMeta,
    LogTruncateOutcome, LogTruncateRequest, StorageBackend, content_digest,
};

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Additively add the replay columns to a pre-existing `df_log` table. Idempotent:
/// a column that already exists is skipped. No data is rewritten.
fn migrate_df_log_columns(conn: &Connection) -> Result<()> {
    let existing: std::collections::HashSet<String> = {
        let mut stmt = conn.prepare("PRAGMA table_info(df_log)")?;
        let cols = stmt.query_map([], |r| r.get::<_, String>(1))?;
        cols.collect::<rusqlite::Result<_>>()?
    };
    for (col, decl) in [
        ("ts_ms", "INTEGER"),
        ("capture_id", "TEXT"),
        ("content_hash", "TEXT"),
    ] {
        if !existing.contains(col) {
            conn.execute(
                &format!("ALTER TABLE df_log ADD COLUMN {col} {decl}"),
                [],
            )?;
        }
    }
    Ok(())
}

/// Lazily create the metadata row for a stream, initializing `head_seq` from the
/// current maximum sequence (non-destructive). No-op if it already exists.
fn ensure_log_meta(
    conn: &Connection,
    ns: &str,
    key: &str,
) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO df_log_meta(ns, key, min_valid_from_seq, head_seq)
         VALUES(?1, ?2, 0,
                COALESCE((SELECT MAX(seq) FROM df_log WHERE ns=?1 AND key=?2), 0))",
        params![ns, key],
    )?;
    Ok(())
}

pub struct SqliteStorageBackend {
    conn: Arc<Mutex<Connection>>,
    _sweep_handle: JoinHandle<()>,
    /// Test hook: when set, `log_truncate` returns an error AFTER performing the
    /// delete + horizon update but BEFORE committing, to prove neither is durable
    /// unless the transaction commits. Always false in production (one atomic load).
    fail_truncate_after_delete: Arc<std::sync::atomic::AtomicBool>,
}

impl std::fmt::Debug for SqliteStorageBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteStorageBackend")
            .finish_non_exhaustive()
    }
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
    seq          INTEGER PRIMARY KEY AUTOINCREMENT,
    ns           TEXT    NOT NULL,
    key          TEXT    NOT NULL,
    val          BLOB    NOT NULL,
    ts           INTEGER NOT NULL DEFAULT (unixepoch()),
    ts_ms        INTEGER,
    capture_id   TEXT,
    content_hash TEXT
);
CREATE INDEX IF NOT EXISTS df_log_ns_key_seq ON df_log(ns, key, seq);
-- NOTE: the unique capture index is created in init() AFTER the additive column
-- migration, because an upgraded database does not yet have capture_id here.

-- Durable per-stream metadata: the retention horizon and head sequence, owned by
-- log_truncate / the append paths (never an independently updated slot).
CREATE TABLE IF NOT EXISTS df_log_meta (
    ns                 TEXT    NOT NULL,
    key                TEXT    NOT NULL,
    min_valid_from_seq INTEGER NOT NULL DEFAULT 0,
    head_seq           INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (ns, key)
);

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

    /// In-memory database - for testing only.
    pub fn in_memory() -> Result<Arc<Self>> {
        let conn = Connection::open_in_memory()?;
        Self::init(conn)
    }

    fn init(conn: Connection) -> Result<Arc<Self>> {
        conn.execute_batch(SCHEMA)?;
        // Additive migration for pre-existing df_log tables (no destructive backfill).
        migrate_df_log_columns(&conn)?;
        // Only now that capture_id is guaranteed to exist (fresh CREATE TABLE or the
        // migration above) can the partial unique index be built - on an upgraded
        // database the column did not exist when SCHEMA ran.
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS df_log_capture
             ON df_log(ns, key, capture_id) WHERE capture_id IS NOT NULL",
            [],
        )?;

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
            fail_truncate_after_delete: Arc::new(
                std::sync::atomic::AtomicBool::new(false),
            ),
        }))
    }

    /// Test hook: force `log_truncate` to fail after the delete + horizon update but
    /// before commit, so a test can prove the transaction rolls back both together.
    #[cfg(test)]
    pub(crate) fn set_fail_truncate_after_delete(&self, v: bool) {
        self.fail_truncate_after_delete
            .store(v, std::sync::atomic::Ordering::SeqCst);
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
            let tx = conn.unchecked_transaction()?;
            tx.execute(
                "INSERT INTO df_log(ns, key, val, ts, ts_ms) VALUES(?1, ?2, ?3, ?4, ?5)",
                params![ns, key, value, now_secs(), now_ms()],
            )?;
            let seq = tx.last_insert_rowid() as u64;
            // Maintain head_seq atomically where stream metadata exists.
            tx.execute(
                "UPDATE df_log_meta SET head_seq=MAX(head_seq, ?3) WHERE ns=?1 AND key=?2",
                params![ns, key, seq as i64],
            )?;
            tx.commit()?;
            Ok(seq)
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

    async fn log_append_if_absent(
        &self,
        ns: &str,
        key: &str,
        capture_id: &str,
        value: &[u8],
    ) -> Result<LogAppendOutcome> {
        let ns = ns.to_string();
        let key = key.to_string();
        let capture_id = capture_id.to_string();
        let value = value.to_vec();
        db!(self, move |conn: &Connection| {
            let digest = content_digest(&value);
            let tx = conn.unchecked_transaction()?;
            ensure_log_meta(&tx, &ns, &key)?;
            let existing: Option<(i64, Vec<u8>, Option<String>)> = tx
                .query_row(
                    "SELECT seq, val, content_hash FROM df_log
                     WHERE ns=?1 AND key=?2 AND capture_id=?3",
                    params![ns, key, capture_id],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()?;
            if let Some((seq, val, ch)) = existing {
                // Cheap digest check, then authoritative exact-bytes comparison.
                if ch.as_deref() == Some(digest.as_str()) && val == value {
                    tx.commit()?;
                    return Ok(LogAppendOutcome {
                        seq: seq as u64,
                        status: AppendStatus::AlreadyPresent,
                    });
                }
                // Different bytes under an existing identity: never overwrite.
                return Err(LogError::CaptureIdentityConflict {
                    capture_id: capture_id.clone(),
                }
                .into());
            }
            tx.execute(
                "INSERT INTO df_log(ns, key, val, ts, ts_ms, capture_id, content_hash)
                 VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![ns, key, value, now_secs(), now_ms(), capture_id, digest],
            )?;
            let seq = tx.last_insert_rowid() as u64;
            tx.execute(
                "UPDATE df_log_meta SET head_seq=MAX(head_seq, ?3) WHERE ns=?1 AND key=?2",
                params![ns, key, seq as i64],
            )?;
            tx.commit()?;
            Ok(LogAppendOutcome {
                seq,
                status: AppendStatus::Inserted,
            })
        })
    }

    async fn log_truncate(
        &self,
        ns: &str,
        key: &str,
        req: LogTruncateRequest,
    ) -> Result<LogTruncateOutcome> {
        let ns = ns.to_string();
        let key = key.to_string();
        let fail_flag = Arc::clone(&self.fail_truncate_after_delete);
        db!(self, move |conn: &Connection| {
            let tx = conn.unchecked_transaction()?;
            ensure_log_meta(&tx, &ns, &key)?;

            // (seq, byte_len, ts_ms) ascending by seq. ts_ms falls back to ts*1000
            // for rows predating the ms column.
            let rows: Vec<(i64, i64, i64)> = {
                let mut stmt = tx.prepare(
                    "SELECT seq, LENGTH(val), COALESCE(ts_ms, ts*1000)
                     FROM df_log WHERE ns=?1 AND key=?2 ORDER BY seq ASC",
                )?;
                let mapped = stmt.query_map(params![ns, key], |r| {
                    Ok((r.get(0)?, r.get(1)?, r.get(2)?))
                })?;
                mapped.collect::<rusqlite::Result<_>>()?
            };
            let n = rows.len();
            let below_pin = rows
                .iter()
                .take_while(|(seq, _, _)| (*seq as u64) < req.pin_seq)
                .count();
            let age_remove = match req.older_than_ms {
                Some(cutoff) => {
                    rows.iter().take_while(|(_, _, tms)| *tms < cutoff).count()
                }
                None => 0,
            };
            let entries_cap_remove = match req.max_entries {
                Some(max) => n.saturating_sub(max as usize),
                None => 0,
            };
            let bytes_cap_remove = match req.max_bytes {
                Some(max) => {
                    let total: i64 = rows.iter().map(|(_, len, _)| *len).sum();
                    let mut over = (total as u64).saturating_sub(max);
                    let mut cnt = 0usize;
                    for (_, len, _) in &rows {
                        if over == 0 {
                            break;
                        }
                        over = over.saturating_sub(*len as u64);
                        cnt += 1;
                    }
                    cnt
                }
                None => 0,
            };
            let desired =
                age_remove.max(entries_cap_remove).max(bytes_cap_remove);
            let actual = desired.min(below_pin);
            let capacity_pinned =
                entries_cap_remove.max(bytes_cap_remove) > below_pin;

            let (removed_bytes, highest_removed_seq) = if actual > 0 {
                let rb: i64 =
                    rows.iter().take(actual).map(|(_, len, _)| *len).sum();
                (rb as u64, Some(rows[actual - 1].0 as u64))
            } else {
                (0, None)
            };
            if let Some(h) = highest_removed_seq {
                tx.execute(
                    "DELETE FROM df_log WHERE ns=?1 AND key=?2 AND seq<=?3",
                    params![ns, key, h as i64],
                )?;
                tx.execute(
                    "UPDATE df_log_meta SET min_valid_from_seq=MAX(min_valid_from_seq, ?3)
                     WHERE ns=?1 AND key=?2",
                    params![ns, key, h as i64],
                )?;
            }
            let (mvfs, head): (i64, i64) = tx.query_row(
                "SELECT min_valid_from_seq, head_seq FROM df_log_meta WHERE ns=?1 AND key=?2",
                params![ns, key],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            let oldest: Option<i64> = tx.query_row(
                "SELECT MIN(seq) FROM df_log WHERE ns=?1 AND key=?2",
                params![ns, key],
                |r| r.get::<_, Option<i64>>(0),
            )?;
            // Test hook: fail after the delete + horizon update, before commit.
            // Dropping `tx` here rolls back BOTH.
            if fail_flag.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(anyhow!(
                    "injected truncate failure after delete, before commit"
                ));
            }
            tx.commit()?;
            Ok(LogTruncateOutcome {
                removed: actual,
                removed_bytes,
                oldest_seq: oldest.map(|s| s as u64),
                highest_removed_seq,
                min_valid_from_seq: mvfs as u64,
                head_seq: head as u64,
                capacity_pinned,
            })
        })
    }

    async fn log_stream_meta(
        &self,
        ns: &str,
        key: &str,
    ) -> Result<LogStreamMeta> {
        let ns = ns.to_string();
        let key = key.to_string();
        db!(self, move |conn: &Connection| {
            let tx = conn.unchecked_transaction()?;
            ensure_log_meta(&tx, &ns, &key)?;
            let (mvfs, head): (i64, i64) = tx.query_row(
                "SELECT min_valid_from_seq, head_seq FROM df_log_meta WHERE ns=?1 AND key=?2",
                params![ns, key],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            let oldest: Option<i64> = tx.query_row(
                "SELECT MIN(seq) FROM df_log WHERE ns=?1 AND key=?2",
                params![ns, key],
                |r| r.get::<_, Option<i64>>(0),
            )?;
            let len: i64 = tx.query_row(
                "SELECT COUNT(*) FROM df_log WHERE ns=?1 AND key=?2",
                params![ns, key],
                |r| r.get(0),
            )?;
            tx.commit()?;
            Ok(LogStreamMeta {
                min_valid_from_seq: mvfs as u64,
                oldest_seq: oldest.map(|s| s as u64),
                head_seq: head as u64,
                len: len as u64,
            })
        })
    }

    async fn log_read_meta_since(
        &self,
        ns: &str,
        key: &str,
        since_seq: u64,
        limit: usize,
    ) -> Result<Vec<LogEntryMeta>> {
        let ns = ns.to_string();
        let key = key.to_string();
        db!(self, move |conn: &Connection| {
            let mut stmt = conn.prepare(
                "SELECT seq, COALESCE(ts_ms, ts*1000), capture_id, content_hash, val
                 FROM df_log WHERE ns=?1 AND key=?2 AND seq>?3
                 ORDER BY seq ASC LIMIT ?4",
            )?;
            let rows = stmt.query_map(
                params![ns, key, since_seq as i64, limit as i64],
                |r| {
                    Ok(LogEntryMeta {
                        seq: r.get::<_, i64>(0)? as u64,
                        stored_at_ms: r.get(1)?,
                        capture_id: r.get(2)?,
                        content_hash: r.get(3)?,
                        value: r.get(4)?,
                    })
                },
            )?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(Into::into)
        })
    }

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

    async fn slot_create(
        &self,
        ns: &str,
        key: &str,
        state: &[u8],
    ) -> Result<Option<u64>> {
        let ns = ns.to_string();
        let key = key.to_string();
        let state = state.to_vec();
        db!(self, move |conn: &Connection| {
            let n = conn.execute(
                "INSERT INTO df_slot(ns, key, version, state, updated_at)
                 VALUES(?1, ?2, 1, ?3, ?4)
                 ON CONFLICT(ns, key) DO NOTHING",
                params![ns, key, state, now_secs()],
            )?;
            Ok(if n == 1 { Some(1u64) } else { None })
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LogTruncateRequest;

    fn be() -> Arc<dyn StorageBackend> {
        SqliteStorageBackend::in_memory().unwrap()
    }

    #[tokio::test]
    async fn idempotency() {
        crate::log_contract_suite::idempotency(be(), "journal").await;
    }
    #[tokio::test]
    async fn conflict_rejected() {
        crate::log_contract_suite::conflict_rejected(be(), "journal").await;
    }
    #[tokio::test]
    async fn horizon_with_global_gaps() {
        crate::log_contract_suite::horizon_with_global_gaps(be(), "journal")
            .await;
    }
    #[tokio::test]
    async fn pin_invariant() {
        crate::log_contract_suite::pin_invariant(be(), "journal").await;
    }
    #[tokio::test]
    async fn empty_vs_truncated() {
        crate::log_contract_suite::empty_vs_truncated(be(), "journal").await;
    }
    #[tokio::test]
    async fn concurrent_appends() {
        crate::log_contract_suite::concurrent_appends(be(), "journal").await;
    }

    /// A failure after the delete + horizon update but before commit rolls BOTH
    /// back: neither the deletion nor the horizon advancement is durable alone.
    #[tokio::test]
    async fn truncate_rollback_is_atomic() {
        let be = SqliteStorageBackend::in_memory().unwrap();
        let s1 = be
            .log_append_if_absent("journal", "s", "c1", b"a")
            .await
            .unwrap()
            .seq;
        be.log_append_if_absent("journal", "s", "c2", b"b")
            .await
            .unwrap();
        be.log_append_if_absent("journal", "s", "c3", b"c")
            .await
            .unwrap();

        be.set_fail_truncate_after_delete(true);
        let err = be
            .log_truncate(
                "journal",
                "s",
                LogTruncateRequest {
                    older_than_ms: Some(i64::MAX),
                    pin_seq: u64::MAX,
                    max_entries: Some(0),
                    max_bytes: None,
                },
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("injected"), "got: {err}");
        be.set_fail_truncate_after_delete(false);

        // Deletion rolled back.
        assert_eq!(be.log_since("journal", "s", 0).await.unwrap().len(), 3);
        // Horizon advancement rolled back.
        let meta = be.log_stream_meta("journal", "s").await.unwrap();
        assert_eq!(meta.min_valid_from_seq, 0);
        assert_eq!(meta.oldest_seq, Some(s1));

        // A subsequent real truncate commits normally.
        let out = be
            .log_truncate(
                "journal",
                "s",
                LogTruncateRequest {
                    older_than_ms: Some(i64::MAX),
                    pin_seq: u64::MAX,
                    max_entries: Some(0),
                    max_bytes: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(out.removed, 3);
    }

    /// An old-format df_log (no replay columns, no metadata, no capture index) with
    /// existing data opens through the new backend: startup succeeds, data is
    /// preserved, and the new APIs work.
    #[tokio::test]
    async fn migrates_old_schema_and_preserves_data() {
        let path = std::env::temp_dir().join(format!(
            "df_migrate_{}_{}.db",
            now_ms(),
            std::process::id()
        ));
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE df_log (
                     seq INTEGER PRIMARY KEY AUTOINCREMENT,
                     ns  TEXT NOT NULL, key TEXT NOT NULL, val BLOB NOT NULL,
                     ts  INTEGER NOT NULL DEFAULT (unixepoch()));
                 CREATE INDEX df_log_ns_key_seq ON df_log(ns, key, seq);",
            )
            .unwrap();
            conn.execute(
                "INSERT INTO df_log(ns, key, val) VALUES('journal', 's', ?1)",
                params![b"old-1".to_vec()],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO df_log(ns, key, val) VALUES('journal', 's', ?1)",
                params![b"old-2".to_vec()],
            )
            .unwrap();
        }

        // Opening runs the additive migration; startup must not fail.
        let be = SqliteStorageBackend::open(&path).unwrap();

        // Old data preserved.
        let entries = be.log_since("journal", "s", 0).await.unwrap();
        assert_eq!(
            entries.iter().map(|(_, v)| v.clone()).collect::<Vec<_>>(),
            vec![b"old-1".to_vec(), b"old-2".to_vec()],
        );
        // Metadata lazily initialized from the existing max sequence.
        let meta = be.log_stream_meta("journal", "s").await.unwrap();
        assert_eq!(meta.head_seq, entries.last().unwrap().0);
        assert_eq!(meta.min_valid_from_seq, 0);
        assert_eq!(meta.len, 2);
        // New APIs work post-migration.
        let ins = be
            .log_append_if_absent("journal", "s", "cap-new", b"new")
            .await
            .unwrap();
        assert_eq!(ins.status, AppendStatus::Inserted);
        let again = be
            .log_append_if_absent("journal", "s", "cap-new", b"new")
            .await
            .unwrap();
        assert_eq!(again.status, AppendStatus::AlreadyPresent);
        assert_eq!(ins.seq, again.seq);

        drop(be);
        let _ = std::fs::remove_file(&path);
    }
}
