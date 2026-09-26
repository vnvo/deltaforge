//! PostgreSQL `StorageBackend`.
//!
//! Uses a `deadpool_postgres` connection pool - each primitive operation
//! acquires a connection from the pool, executes natively async, and releases
//! it immediately. No `spawn_blocking`, no serializing mutex.
//!
//! The log sequence is driven by a PostgreSQL `BIGSERIAL` column which is
//! backed by a shared sequence, giving the same global-monotonic guarantee
//! as the SQLite `AUTOINCREMENT` implementation.
//!
//! TTL expiry is handled by a background task that runs a DELETE sweep
//! every 60 seconds.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use deadpool_postgres::{Config, Pool, Runtime, tokio_postgres::NoTls};
use tokio::task::JoinHandle;
use tracing::{error, info};

use crate::{
    AppendStatus, LogAppendOutcome, LogError, LogStreamMeta,
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

const ENSURE_LOG_META_SQL: &str = "\
INSERT INTO df_log_meta(ns, key, min_valid_from_seq, head_seq) \
VALUES($1, $2, 0, COALESCE((SELECT MAX(seq) FROM df_log WHERE ns=$1 AND key=$2), 0)) \
ON CONFLICT (ns, key) DO NOTHING";

// ── Schema ────────────────────────────────────────────────────────────────────

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS df_kv (
    ns          TEXT    NOT NULL,
    key         TEXT    NOT NULL,
    val         BYTEA   NOT NULL,
    updated_at  BIGINT  NOT NULL,
    expires_at  BIGINT,
    PRIMARY KEY (ns, key)
);
CREATE INDEX IF NOT EXISTS df_kv_expires ON df_kv(expires_at)
    WHERE expires_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS df_log (
    seq  BIGSERIAL PRIMARY KEY,
    ns   TEXT   NOT NULL,
    key  TEXT   NOT NULL,
    val  BYTEA  NOT NULL,
    ts   BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS df_log_ns_key_seq ON df_log(ns, key, seq);
-- Additive replay columns (idempotent; safe on pre-existing tables).
ALTER TABLE df_log ADD COLUMN IF NOT EXISTS ts_ms BIGINT;
ALTER TABLE df_log ADD COLUMN IF NOT EXISTS capture_id TEXT;
ALTER TABLE df_log ADD COLUMN IF NOT EXISTS content_hash TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS df_log_capture
    ON df_log(ns, key, capture_id) WHERE capture_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS df_log_meta (
    ns                 TEXT   NOT NULL,
    key                TEXT   NOT NULL,
    min_valid_from_seq BIGINT NOT NULL DEFAULT 0,
    head_seq           BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (ns, key)
);

CREATE TABLE IF NOT EXISTS df_slot (
    ns          TEXT   NOT NULL,
    key         TEXT   NOT NULL,
    version     BIGINT NOT NULL DEFAULT 1,
    state       BYTEA  NOT NULL,
    updated_at  BIGINT NOT NULL,
    PRIMARY KEY (ns, key)
);

CREATE TABLE IF NOT EXISTS df_queue (
    id   BIGSERIAL PRIMARY KEY,
    ns   TEXT   NOT NULL,
    key  TEXT   NOT NULL,
    val  BYTEA  NOT NULL,
    ts   BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS df_queue_ns_key_id ON df_queue(ns, key, id);
"#;

// ── Backend ───────────────────────────────────────────────────────────────────

pub struct PostgresStorageBackend {
    pool: Pool,
    _sweep_handle: JoinHandle<()>,
}

impl std::fmt::Debug for PostgresStorageBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresStorageBackend")
            .finish_non_exhaustive()
    }
}

impl PostgresStorageBackend {
    /// Connect to a PostgreSQL database and initialize schema.
    ///
    /// `dsn` is a libpq connection string, e.g.
    /// `"host=localhost dbname=deltaforge user=df password=secret"`
    pub async fn connect(dsn: &str) -> Result<Arc<Self>> {
        let mut cfg = Config::new();
        cfg.url = Some(dsn.to_string());
        let pool = cfg
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .map_err(|e| anyhow!("pool create: {e}"))?;

        // Run schema migrations
        let client = pool.get().await.map_err(|e| anyhow!("pool get: {e}"))?;
        for stmt in SCHEMA.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            client
                .execute(stmt, &[])
                .await
                .map_err(|e| anyhow!("schema migration: {e}\nSQL: {stmt}"))?;
        }

        // TTL sweep every 60s
        let sweep_pool = pool.clone();
        let sweep_handle = tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                interval.tick().await;
                match sweep_pool.get().await {
                    Ok(c) => {
                        let _ = c
                            .execute(
                                "DELETE FROM df_kv WHERE expires_at IS NOT NULL AND expires_at < $1",
                                &[&now_secs()],
                            )
                            .await
                            .map_err(|e| error!("TTL sweep error: {e}"));
                    }
                    Err(e) => error!("TTL sweep pool error: {e}"),
                }
            }
        });

        info!("PostgreSQL storage backend initialized");
        Ok(Arc::new(Self {
            pool,
            _sweep_handle: sweep_handle,
        }))
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

macro_rules! client {
    ($self:expr) => {
        $self
            .pool
            .get()
            .await
            .map_err(|e| anyhow!("pool get: {e}"))?
    };
}

// ── StorageBackend ────────────────────────────────────────────────────────────

#[async_trait]
impl StorageBackend for PostgresStorageBackend {
    // ── KV ──────────────────────────────────────────────────────────────────

    async fn kv_get(&self, ns: &str, key: &str) -> Result<Option<Vec<u8>>> {
        let c = client!(self);
        let now = now_secs();
        let row = c
            .query_opt(
                "SELECT val, expires_at FROM df_kv WHERE ns=$1 AND key=$2",
                &[&ns, &key],
            )
            .await?;
        Ok(row.and_then(|r| {
            let val: Vec<u8> = r.get(0);
            let exp: Option<i64> = r.get(1);
            if exp.is_some_and(|e| e <= now) {
                None
            } else {
                Some(val)
            }
        }))
    }

    async fn kv_put(&self, ns: &str, key: &str, value: &[u8]) -> Result<()> {
        let c = client!(self);
        c.execute(
            "INSERT INTO df_kv(ns, key, val, updated_at, expires_at)
             VALUES($1, $2, $3, $4, NULL)
             ON CONFLICT(ns, key) DO UPDATE SET
               val=EXCLUDED.val, updated_at=EXCLUDED.updated_at, expires_at=NULL",
            &[&ns, &key, &value, &now_secs()],
        )
        .await?;
        Ok(())
    }

    async fn kv_put_with_ttl(
        &self,
        ns: &str,
        key: &str,
        value: &[u8],
        ttl_secs: u64,
    ) -> Result<()> {
        let c = client!(self);
        let expires_at = now_secs() + ttl_secs as i64;
        c.execute(
            "INSERT INTO df_kv(ns, key, val, updated_at, expires_at)
             VALUES($1, $2, $3, $4, $5)
             ON CONFLICT(ns, key) DO UPDATE SET
               val=EXCLUDED.val, updated_at=EXCLUDED.updated_at, expires_at=EXCLUDED.expires_at",
            &[&ns, &key, &value, &now_secs(), &expires_at],
        )
        .await?;
        Ok(())
    }

    async fn kv_delete(&self, ns: &str, key: &str) -> Result<bool> {
        let c = client!(self);
        let n = c
            .execute("DELETE FROM df_kv WHERE ns=$1 AND key=$2", &[&ns, &key])
            .await?;
        Ok(n > 0)
    }

    async fn kv_list(
        &self,
        ns: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<String>> {
        let c = client!(self);
        let now = now_secs();
        let rows = if let Some(p) = prefix {
            let pat = format!("{p}%");
            c.query(
                "SELECT key FROM df_kv WHERE ns=$1 AND key LIKE $2
                 AND (expires_at IS NULL OR expires_at > $3) ORDER BY key",
                &[&ns, &pat, &now],
            )
            .await?
        } else {
            c.query(
                "SELECT key FROM df_kv WHERE ns=$1
                 AND (expires_at IS NULL OR expires_at > $2) ORDER BY key",
                &[&ns, &now],
            )
            .await?
        };
        Ok(rows.into_iter().map(|r| r.get::<_, String>(0)).collect())
    }

    // ── Log ─────────────────────────────────────────────────────────────────

    async fn log_append(
        &self,
        ns: &str,
        key: &str,
        value: &[u8],
    ) -> Result<u64> {
        let mut c = client!(self);
        let tx = c.transaction().await?;
        let row = tx
            .query_one(
                "INSERT INTO df_log(ns, key, val, ts, ts_ms) VALUES($1, $2, $3, $4, $5) RETURNING seq",
                &[&ns, &key, &value, &now_secs(), &now_ms()],
            )
            .await?;
        let seq = row.get::<_, i64>(0);
        // Maintain head_seq atomically where stream metadata exists.
        tx.execute(
            "UPDATE df_log_meta SET head_seq=GREATEST(head_seq, $3) WHERE ns=$1 AND key=$2",
            &[&ns, &key, &seq],
        )
        .await?;
        tx.commit().await?;
        Ok(seq as u64)
    }

    async fn log_list(
        &self,
        ns: &str,
        key: &str,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        let c = client!(self);
        let rows = c
            .query(
                "SELECT seq, val FROM df_log WHERE ns=$1 AND key=$2 ORDER BY seq ASC",
                &[&ns, &key],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| (r.get::<_, i64>(0) as u64, r.get(1)))
            .collect())
    }

    async fn log_since(
        &self,
        ns: &str,
        key: &str,
        since_seq: u64,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        let c = client!(self);
        let rows = c
            .query(
                "SELECT seq, val FROM df_log WHERE ns=$1 AND key=$2 AND seq>$3 ORDER BY seq ASC",
                &[&ns, &key, &(since_seq as i64)],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| (r.get::<_, i64>(0) as u64, r.get(1)))
            .collect())
    }

    async fn log_latest(
        &self,
        ns: &str,
        key: &str,
    ) -> Result<Option<(u64, Vec<u8>)>> {
        let c = client!(self);
        let row = c
            .query_opt(
                "SELECT seq, val FROM df_log WHERE ns=$1 AND key=$2 ORDER BY seq DESC LIMIT 1",
                &[&ns, &key],
            )
            .await?;
        Ok(row.map(|r| (r.get::<_, i64>(0) as u64, r.get(1))))
    }

    async fn log_append_if_absent(
        &self,
        ns: &str,
        key: &str,
        capture_id: &str,
        value: &[u8],
    ) -> Result<LogAppendOutcome> {
        let digest = content_digest(value);
        let mut c = client!(self);
        let tx = c.transaction().await?;
        tx.execute(ENSURE_LOG_META_SQL, &[&ns, &key]).await?;
        // Race-safe insert: attempt it directly and let the partial unique index
        // arbitrate. A concurrent inserter with the same capture_id makes this a
        // no-op - the second transaction blocks on the conflicting tuple, then (once
        // the first commits) DO NOTHING returns no row and we re-read below. This
        // avoids the check-then-insert race that would surface a raw unique-constraint
        // error to the loser.
        let inserted = tx
            .query_opt(
                "INSERT INTO df_log(ns, key, val, ts, ts_ms, capture_id, content_hash)
                 VALUES($1, $2, $3, $4, $5, $6, $7)
                 ON CONFLICT (ns, key, capture_id) WHERE capture_id IS NOT NULL
                 DO NOTHING
                 RETURNING seq",
                &[&ns, &key, &value, &now_secs(), &now_ms(), &capture_id, &digest],
            )
            .await?;
        if let Some(row) = inserted {
            let seq: i64 = row.get(0);
            tx.execute(
                "UPDATE df_log_meta SET head_seq=GREATEST(head_seq, $3) WHERE ns=$1 AND key=$2",
                &[&ns, &key, &seq],
            )
            .await?;
            tx.commit().await?;
            return Ok(LogAppendOutcome {
                seq: seq as u64,
                status: AppendStatus::Inserted,
            });
        }
        // The identity already exists (committed by a concurrent or prior writer).
        // Re-read and confirm by exact bytes.
        let row = tx
            .query_one(
                "SELECT seq, val, content_hash FROM df_log
                 WHERE ns=$1 AND key=$2 AND capture_id=$3",
                &[&ns, &key, &capture_id],
            )
            .await?;
        let seq: i64 = row.get(0);
        let val: Vec<u8> = row.get(1);
        let ch: Option<String> = row.get(2);
        if ch.as_deref() == Some(digest.as_str()) && val.as_slice() == value {
            tx.commit().await?;
            return Ok(LogAppendOutcome {
                seq: seq as u64,
                status: AppendStatus::AlreadyPresent,
            });
        }
        // Different bytes under an existing identity: never overwrite (rollback).
        Err(LogError::CaptureIdentityConflict {
            capture_id: capture_id.to_string(),
        }
        .into())
    }

    async fn log_truncate(
        &self,
        ns: &str,
        key: &str,
        req: LogTruncateRequest,
    ) -> Result<LogTruncateOutcome> {
        let mut c = client!(self);
        let tx = c.transaction().await?;
        tx.execute(ENSURE_LOG_META_SQL, &[&ns, &key]).await?;
        let rows = tx
            .query(
                "SELECT seq, LENGTH(val), COALESCE(ts_ms, ts*1000)
                 FROM df_log WHERE ns=$1 AND key=$2 ORDER BY seq ASC",
                &[&ns, &key],
            )
            .await?;
        let rows: Vec<(i64, i64, i64)> = rows
            .into_iter()
            .map(|r| {
                (
                    r.get::<_, i64>(0),
                    r.get::<_, i32>(1) as i64,
                    r.get::<_, i64>(2),
                )
            })
            .collect();
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
        let desired = age_remove.max(entries_cap_remove).max(bytes_cap_remove);
        let actual = desired.min(below_pin);
        let capacity_pinned =
            entries_cap_remove.max(bytes_cap_remove) > below_pin;
        let (removed_bytes, highest_removed_seq) = if actual > 0 {
            let rb: i64 =
                rows.iter().take(actual).map(|(_, len, _)| *len).sum();
            (rb as u64, Some(rows[actual - 1].0))
        } else {
            (0, None)
        };
        if let Some(h) = highest_removed_seq {
            tx.execute(
                "DELETE FROM df_log WHERE ns=$1 AND key=$2 AND seq<=$3",
                &[&ns, &key, &h],
            )
            .await?;
            tx.execute(
                "UPDATE df_log_meta SET min_valid_from_seq=GREATEST(min_valid_from_seq, $3)
                 WHERE ns=$1 AND key=$2",
                &[&ns, &key, &h],
            )
            .await?;
        }
        let meta_row = tx
            .query_one(
                "SELECT min_valid_from_seq, head_seq FROM df_log_meta WHERE ns=$1 AND key=$2",
                &[&ns, &key],
            )
            .await?;
        let mvfs: i64 = meta_row.get(0);
        let head: i64 = meta_row.get(1);
        let oldest_row = tx
            .query_one(
                "SELECT MIN(seq) FROM df_log WHERE ns=$1 AND key=$2",
                &[&ns, &key],
            )
            .await?;
        let oldest: Option<i64> = oldest_row.get(0);
        tx.commit().await?;
        Ok(LogTruncateOutcome {
            removed: actual,
            removed_bytes,
            oldest_seq: oldest.map(|s| s as u64),
            highest_removed_seq: highest_removed_seq.map(|s| s as u64),
            min_valid_from_seq: mvfs as u64,
            head_seq: head as u64,
            capacity_pinned,
        })
    }

    async fn log_stream_meta(
        &self,
        ns: &str,
        key: &str,
    ) -> Result<LogStreamMeta> {
        let mut c = client!(self);
        let tx = c.transaction().await?;
        tx.execute(ENSURE_LOG_META_SQL, &[&ns, &key]).await?;
        let meta_row = tx
            .query_one(
                "SELECT min_valid_from_seq, head_seq FROM df_log_meta WHERE ns=$1 AND key=$2",
                &[&ns, &key],
            )
            .await?;
        let mvfs: i64 = meta_row.get(0);
        let head: i64 = meta_row.get(1);
        let stat_row = tx
            .query_one(
                "SELECT MIN(seq), COUNT(*) FROM df_log WHERE ns=$1 AND key=$2",
                &[&ns, &key],
            )
            .await?;
        let oldest: Option<i64> = stat_row.get(0);
        let len: i64 = stat_row.get(1);
        tx.commit().await?;
        Ok(LogStreamMeta {
            min_valid_from_seq: mvfs as u64,
            oldest_seq: oldest.map(|s| s as u64),
            head_seq: head as u64,
            len: len as u64,
        })
    }

    // ── Slot ────────────────────────────────────────────────────────────────

    async fn slot_upsert(
        &self,
        ns: &str,
        key: &str,
        state: &[u8],
    ) -> Result<u64> {
        let c = client!(self);
        let row = c
            .query_one(
                "INSERT INTO df_slot(ns, key, version, state, updated_at)
                 VALUES($1, $2, 1, $3, $4)
                 ON CONFLICT(ns, key) DO UPDATE SET
                   version = df_slot.version + 1,
                   state   = EXCLUDED.state,
                   updated_at = EXCLUDED.updated_at
                 RETURNING version",
                &[&ns, &key, &state, &now_secs()],
            )
            .await?;
        Ok(row.get::<_, i64>(0) as u64)
    }

    async fn slot_get(
        &self,
        ns: &str,
        key: &str,
    ) -> Result<Option<(u64, Vec<u8>)>> {
        let c = client!(self);
        let row = c
            .query_opt(
                "SELECT version, state FROM df_slot WHERE ns=$1 AND key=$2",
                &[&ns, &key],
            )
            .await?;
        Ok(row.map(|r| (r.get::<_, i64>(0) as u64, r.get(1))))
    }

    async fn slot_cas(
        &self,
        ns: &str,
        key: &str,
        expected_version: u64,
        state: &[u8],
    ) -> Result<bool> {
        let c = client!(self);
        let n = c
            .execute(
                "UPDATE df_slot SET state=$1, version=version+1, updated_at=$2
                 WHERE ns=$3 AND key=$4 AND version=$5",
                &[&state, &now_secs(), &ns, &key, &(expected_version as i64)],
            )
            .await?;
        Ok(n == 1)
    }

    async fn slot_create(
        &self,
        ns: &str,
        key: &str,
        state: &[u8],
    ) -> Result<Option<u64>> {
        let c = client!(self);
        let row = c
            .query_opt(
                "INSERT INTO df_slot(ns, key, version, state, updated_at)
                 VALUES($1, $2, 1, $3, $4)
                 ON CONFLICT(ns, key) DO NOTHING
                 RETURNING version",
                &[&ns, &key, &state, &now_secs()],
            )
            .await?;
        Ok(row.map(|r| r.get::<_, i64>(0) as u64))
    }

    async fn slot_delete(&self, ns: &str, key: &str) -> Result<bool> {
        let c = client!(self);
        let n = c
            .execute("DELETE FROM df_slot WHERE ns=$1 AND key=$2", &[&ns, &key])
            .await?;
        Ok(n > 0)
    }

    // ── Queue ────────────────────────────────────────────────────────────────

    async fn queue_push(
        &self,
        ns: &str,
        key: &str,
        value: &[u8],
    ) -> Result<u64> {
        let c = client!(self);
        let row = c
            .query_one(
                "INSERT INTO df_queue(ns, key, val, ts) VALUES($1, $2, $3, $4) RETURNING id",
                &[&ns, &key, &value, &now_secs()],
            )
            .await?;
        Ok(row.get::<_, i64>(0) as u64)
    }

    async fn queue_peek(
        &self,
        ns: &str,
        key: &str,
        limit: usize,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        let c = client!(self);
        let rows = c
            .query(
                "SELECT id, val FROM df_queue WHERE ns=$1 AND key=$2 ORDER BY id ASC LIMIT $3",
                &[&ns, &key, &(limit as i64)],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| (r.get::<_, i64>(0) as u64, r.get(1)))
            .collect())
    }

    async fn queue_ack(
        &self,
        ns: &str,
        key: &str,
        up_to_id: u64,
    ) -> Result<usize> {
        let c = client!(self);
        let n = c
            .execute(
                "DELETE FROM df_queue WHERE ns=$1 AND key=$2 AND id<=$3",
                &[&ns, &key, &(up_to_id as i64)],
            )
            .await?;
        Ok(n as usize)
    }

    async fn queue_len(&self, ns: &str, key: &str) -> Result<u64> {
        let c = client!(self);
        let row = c
            .query_one(
                "SELECT COUNT(*) FROM df_queue WHERE ns=$1 AND key=$2",
                &[&ns, &key],
            )
            .await?;
        Ok(row.get::<_, i64>(0) as u64)
    }

    async fn queue_drop_oldest(
        &self,
        ns: &str,
        key: &str,
        count: usize,
    ) -> Result<usize> {
        let c = client!(self);
        let n = c
            .execute(
                "DELETE FROM df_queue WHERE id IN (
                   SELECT id FROM df_queue WHERE ns=$1 AND key=$2
                   ORDER BY id ASC LIMIT $3
                 )",
                &[&ns, &key, &(count as i64)],
            )
            .await?;
        Ok(n as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The full shared contract suite against a live PostgreSQL, including the
    /// concurrent cases (which exercise the ON CONFLICT / re-read race). #[ignore] +
    /// env-gated on DELTAFORGE_IT_PG_DSN; each case runs under a unique namespace so
    /// it is safe against a shared database and repeated runs.
    #[tokio::test]
    #[ignore = "requires a live PostgreSQL (DELTAFORGE_IT_PG_DSN)"]
    async fn pg_log_contracts() {
        let dsn = std::env::var("DELTAFORGE_IT_PG_DSN")
            .expect("DELTAFORGE_IT_PG_DSN must be set to run this test");
        use crate::log_contract_suite as suite;
        let be: Arc<dyn StorageBackend> =
            PostgresStorageBackend::connect(&dsn).await.unwrap();
        let base = now_ms();
        suite::idempotency(be.clone(), &format!("it{base}_idem")).await;
        suite::conflict_rejected(be.clone(), &format!("it{base}_conf")).await;
        suite::horizon_with_global_gaps(
            be.clone(),
            &format!("it{base}_horizon"),
        )
        .await;
        suite::pin_invariant(be.clone(), &format!("it{base}_pin")).await;
        suite::empty_vs_truncated(be.clone(), &format!("it{base}_empty")).await;
        suite::concurrent_appends(be.clone(), &format!("it{base}_conc")).await;
    }
}
