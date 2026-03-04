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

use crate::StorageBackend;

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

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
        f.debug_struct("PostgresStorageBackend").finish_non_exhaustive()
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
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
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
        Ok(Arc::new(Self { pool, _sweep_handle: sweep_handle }))
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
            if exp.is_some_and(|e| e <= now) { None } else { Some(val) }
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

    async fn kv_list(&self, ns: &str, prefix: Option<&str>) -> Result<Vec<String>> {
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

    async fn log_append(&self, ns: &str, key: &str, value: &[u8]) -> Result<u64> {
        let c = client!(self);
        let row = c
            .query_one(
                "INSERT INTO df_log(ns, key, val, ts) VALUES($1, $2, $3, $4) RETURNING seq",
                &[&ns, &key, &value, &now_secs()],
            )
            .await?;
        Ok(row.get::<_, i64>(0) as u64)
    }

    async fn log_list(&self, ns: &str, key: &str) -> Result<Vec<(u64, Vec<u8>)>> {
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

    async fn log_since(&self, ns: &str, key: &str, since_seq: u64) -> Result<Vec<(u64, Vec<u8>)>> {
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

    async fn log_latest(&self, ns: &str, key: &str) -> Result<Option<(u64, Vec<u8>)>> {
        let c = client!(self);
        let row = c
            .query_opt(
                "SELECT seq, val FROM df_log WHERE ns=$1 AND key=$2 ORDER BY seq DESC LIMIT 1",
                &[&ns, &key],
            )
            .await?;
        Ok(row.map(|r| (r.get::<_, i64>(0) as u64, r.get(1))))
    }

    // ── Slot ────────────────────────────────────────────────────────────────

    async fn slot_upsert(&self, ns: &str, key: &str, state: &[u8]) -> Result<u64> {
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

    async fn slot_get(&self, ns: &str, key: &str) -> Result<Option<(u64, Vec<u8>)>> {
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

    async fn slot_delete(&self, ns: &str, key: &str) -> Result<bool> {
        let c = client!(self);
        let n = c
            .execute("DELETE FROM df_slot WHERE ns=$1 AND key=$2", &[&ns, &key])
            .await?;
        Ok(n > 0)
    }

    // ── Queue ────────────────────────────────────────────────────────────────

    async fn queue_push(&self, ns: &str, key: &str, value: &[u8]) -> Result<u64> {
        let c = client!(self);
        let row = c
            .query_one(
                "INSERT INTO df_queue(ns, key, val, ts) VALUES($1, $2, $3, $4) RETURNING id",
                &[&ns, &key, &value, &now_secs()],
            )
            .await?;
        Ok(row.get::<_, i64>(0) as u64)
    }

    async fn queue_peek(&self, ns: &str, key: &str, limit: usize) -> Result<Vec<(u64, Vec<u8>)>> {
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

    async fn queue_ack(&self, ns: &str, key: &str, up_to_id: u64) -> Result<usize> {
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

    async fn queue_drop_oldest(&self, ns: &str, key: &str, count: usize) -> Result<usize> {
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