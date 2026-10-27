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
    AppendStatus, AtomicBatch, BatchFault, BatchFaults, BatchOp, BatchOutcome,
    BatchRead, BatchReadResult, BatchValue, LogAppendOutcome, LogEntryMeta,
    LogError, LogStreamMeta, LogTruncateOutcome, LogTruncateRequest,
    Precondition, StorageBackend, content_digest, max_seq_value, render_seq,
};

/// The transaction-scoped advisory lock every log writer takes before it
/// allocates a sequence: allocation, and so commit, order is serialized, and
/// one transaction's entries take a contiguous range. A reader never sees a
/// sequence before a lower one that commits later.
const LOG_ALLOCATION_LOCK: i64 = 0x6466_5f6c_6f67; // "df_log"

const LOCK_LOG_SQL: &str = "SELECT pg_advisory_xact_lock($1)";

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

// Schema + additive migrations as a structured list, each element EXACTLY ONE
// statement with no trailing ';' and no embedded SQL comments. They are executed one
// at a time (never by splitting a blob on ';'), so a comment or literal containing a
// semicolon can never break schema execution. Order matters: df_log's replay columns
// are added before the index that references them, so an upgraded database is safe.
const MIGRATIONS: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS df_kv (
        ns TEXT NOT NULL, key TEXT NOT NULL, val BYTEA NOT NULL,
        updated_at BIGINT NOT NULL, expires_at BIGINT,
        PRIMARY KEY (ns, key))",
    "CREATE INDEX IF NOT EXISTS df_kv_expires ON df_kv(expires_at)
        WHERE expires_at IS NOT NULL",
    "CREATE TABLE IF NOT EXISTS df_log (
        seq BIGSERIAL PRIMARY KEY, ns TEXT NOT NULL, key TEXT NOT NULL,
        val BYTEA NOT NULL, ts BIGINT NOT NULL)",
    "CREATE INDEX IF NOT EXISTS df_log_ns_key_seq ON df_log(ns, key, seq)",
    "ALTER TABLE df_log ADD COLUMN IF NOT EXISTS ts_ms BIGINT",
    "ALTER TABLE df_log ADD COLUMN IF NOT EXISTS capture_id TEXT",
    "ALTER TABLE df_log ADD COLUMN IF NOT EXISTS content_hash TEXT",
    "CREATE UNIQUE INDEX IF NOT EXISTS df_log_capture
        ON df_log(ns, key, capture_id) WHERE capture_id IS NOT NULL",
    "CREATE TABLE IF NOT EXISTS df_log_meta (
        ns TEXT NOT NULL, key TEXT NOT NULL,
        min_valid_from_seq BIGINT NOT NULL DEFAULT 0,
        head_seq BIGINT NOT NULL DEFAULT 0,
        PRIMARY KEY (ns, key))",
    "CREATE TABLE IF NOT EXISTS df_slot (
        ns TEXT NOT NULL, key TEXT NOT NULL, version BIGINT NOT NULL DEFAULT 1,
        state BYTEA NOT NULL, updated_at BIGINT NOT NULL,
        PRIMARY KEY (ns, key))",
    "CREATE TABLE IF NOT EXISTS df_queue (
        id BIGSERIAL PRIMARY KEY, ns TEXT NOT NULL, key TEXT NOT NULL,
        val BYTEA NOT NULL, ts BIGINT NOT NULL)",
    "CREATE INDEX IF NOT EXISTS df_queue_ns_key_id ON df_queue(ns, key, id)",
    "CREATE INDEX IF NOT EXISTS df_kv_checkpoints_key_c ON df_kv ((key COLLATE \"C\"))
        WHERE ns = 'checkpoints'",
    "CREATE INDEX IF NOT EXISTS df_slot_ns_key_c ON df_slot (ns, (key COLLATE \"C\"))",
    "CREATE TABLE IF NOT EXISTS df_batch_commit (
        batch_id TEXT PRIMARY KEY, digest BYTEA NOT NULL,
        result BYTEA NOT NULL, committed_at BIGINT NOT NULL)",
    "CREATE INDEX IF NOT EXISTS df_batch_commit_at ON df_batch_commit(committed_at)",
];

// Prefix listings compare keys byte-wise (`COLLATE "C"`), whatever the database
// collation. The checkpoint namespace (listed by prefix on the resume / feedback
// hot path) has a partial "C"-collated index, `df_kv_checkpoints_key_c`, and its
// own query with the literal namespace predicate that index requires, so the
// index is provably usable even by a generic plan. Other namespaces have no
// production prefix caller and no catalog-wide index (df_kv also holds the dense
// schema-registry indexes). Each query comes with and without an upper bound, so
// no bound is hidden behind `$n IS NULL OR`.
const KV_LIST_CHECKPOINTS_PREFIX_SQL: &str = "SELECT key FROM df_kv
     WHERE ns = 'checkpoints'
       AND key COLLATE \"C\" >= $1 AND key COLLATE \"C\" < $2
       AND (expires_at IS NULL OR expires_at > $3)
     ORDER BY key COLLATE \"C\"";
const KV_LIST_CHECKPOINTS_PREFIX_OPEN_SQL: &str = "SELECT key FROM df_kv
     WHERE ns = 'checkpoints'
       AND key COLLATE \"C\" >= $1
       AND (expires_at IS NULL OR expires_at > $2)
     ORDER BY key COLLATE \"C\"";
const KV_LIST_PREFIX_SQL: &str = "SELECT key FROM df_kv
     WHERE ns = $1 AND key COLLATE \"C\" >= $2 AND key COLLATE \"C\" < $3
       AND (expires_at IS NULL OR expires_at > $4)
     ORDER BY key COLLATE \"C\"";
const KV_LIST_PREFIX_OPEN_SQL: &str = "SELECT key FROM df_kv
     WHERE ns = $1 AND key COLLATE \"C\" >= $2
       AND (expires_at IS NULL OR expires_at > $3)
     ORDER BY key COLLATE \"C\"";

const SLOT_LIST_SQL: &str = "SELECT key, version, state FROM df_slot
     WHERE ns = $1
       AND ($2::text IS NULL OR key COLLATE \"C\" >= $2)
       AND ($3::text IS NULL OR key COLLATE \"C\" <  $3)
       AND ($4::text IS NULL OR key COLLATE \"C\" >  $4)
     ORDER BY key COLLATE \"C\" ASC
     LIMIT $5";

// ── Backend ───────────────────────────────────────────────────────────────────

pub struct PostgresStorageBackend {
    pool: Pool,
    _sweep_handle: JoinHandle<()>,
    /// Test-only fault point of `apply_batch` (one shot).
    batch_fault: std::sync::Mutex<Option<BatchFault>>,
}

impl PostgresStorageBackend {
    /// A backend over `pool` for the background commit-marker sweep (no
    /// TTL task of its own).
    fn marker_sweeper(pool: Pool) -> Self {
        Self {
            pool,
            _sweep_handle: tokio::spawn(async {}),
            batch_fault: std::sync::Mutex::new(None),
        }
    }
}

impl BatchFaults for PostgresStorageBackend {
    fn set_batch_fault(&self, fault: Option<BatchFault>) {
        *self.batch_fault.lock().expect("batch fault") = fault;
    }
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

        // Run schema migrations: one complete statement at a time (never split a
        // multi-statement blob on ';', which a comment/literal semicolon would break).
        let client = pool.get().await.map_err(|e| anyhow!("pool get: {e}"))?;
        for stmt in MIGRATIONS {
            client
                .execute(*stmt, &[])
                .await
                .map_err(|e| anyhow!("schema migration: {e}\nSQL: {stmt}"))?;
        }

        // TTL sweep every 60s; commit-marker sweep every hour.
        let sweep_pool = pool.clone();
        let marker_pool = pool.clone();
        tokio::spawn(async move {
            let be = PostgresStorageBackend::marker_sweeper(marker_pool);
            let mut interval =
                tokio::time::interval(std::time::Duration::from_secs(3600));
            loop {
                interval.tick().await;
                if let Err(e) = crate::sweep_batch_markers(&be, now_ms()).await
                {
                    error!("commit-marker sweep error: {e:#}");
                }
            }
        });
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
            batch_fault: std::sync::Mutex::new(None),
        }))
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// A key's live value inside `tx` (an expired one reads as absent).
async fn pg_kv_live(
    tx: &deadpool_postgres::tokio_postgres::Transaction<'_>,
    ns: &str,
    key: &str,
    now: i64,
) -> Result<Option<Vec<u8>>> {
    let row = tx
        .query_opt(
            "SELECT val, expires_at FROM df_kv WHERE ns=$1 AND key=$2",
            &[&ns, &key],
        )
        .await?;
    Ok(row.and_then(|r| {
        let exp: Option<i64> = r.get(1);
        (!exp.is_some_and(|e| e <= now)).then(|| r.get(0))
    }))
}

/// A stream's last sequence inside `tx` (0 when empty).
async fn pg_log_head(
    tx: &deadpool_postgres::tokio_postgres::Transaction<'_>,
    ns: &str,
    key: &str,
) -> Result<u64> {
    Ok(tx
        .query_opt(
            "SELECT seq FROM df_log WHERE ns=$1 AND key=$2 ORDER BY seq DESC LIMIT 1",
            &[&ns, &key],
        )
        .await?
        .map_or(0, |r| r.get::<_, i64>(0) as u64))
}

const KV_PUT_SQL: &str =
    "INSERT INTO df_kv(ns, key, val, updated_at, expires_at)
     VALUES($1, $2, $3, $4, NULL)
     ON CONFLICT(ns, key) DO UPDATE SET
       val=EXCLUDED.val, updated_at=EXCLUDED.updated_at, expires_at=NULL";

async fn pg_kv_put(
    tx: &deadpool_postgres::tokio_postgres::Transaction<'_>,
    ns: &str,
    key: &str,
    value: &[u8],
) -> Result<()> {
    tx.execute(KV_PUT_SQL, &[&ns, &key, &value, &now_secs()])
        .await?;
    Ok(())
}

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
    async fn read_batch(
        &self,
        reads: &[BatchRead],
    ) -> Result<Vec<BatchReadResult>> {
        let mut c = client!(self);
        // One snapshot.
        let tx = c
            .build_transaction()
            .isolation_level(deadpool_postgres::tokio_postgres::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await?;
        let now = now_secs();
        let mut out = Vec::with_capacity(reads.len());
        for r in reads {
            out.push(match r {
                BatchRead::KvGet { ns, key } => BatchReadResult::Kv(pg_kv_live(&tx, ns, key, now).await?),
                BatchRead::LogLatest { ns, key } => BatchReadResult::Latest(
                    tx.query_opt(
                        "SELECT seq, val FROM df_log WHERE ns=$1 AND key=$2 ORDER BY seq DESC LIMIT 1",
                        &[ns, key],
                    )
                    .await?
                    .map(|r| (r.get::<_, i64>(0) as u64, r.get(1))),
                ),
                BatchRead::LogHead { ns, key } => BatchReadResult::Head(pg_log_head(&tx, ns, key).await?),
                BatchRead::LogLen { ns, key } => BatchReadResult::Len(
                    tx.query_one(
                        "SELECT COUNT(*) FROM df_log WHERE ns=$1 AND key=$2",
                        &[ns, key],
                    )
                    .await?
                    .get::<_, i64>(0) as u64,
                ),
            });
        }
        tx.commit().await?;
        Ok(out)
    }

    async fn log_read_streams(
        &self,
        streams: &[(String, String)],
    ) -> Result<Vec<Vec<LogEntryMeta>>> {
        let mut out: Vec<Vec<LogEntryMeta>> = vec![Vec::new(); streams.len()];
        if streams.is_empty() {
            return Ok(out);
        }
        let (nss, keys): (Vec<&str>, Vec<&str>) = streams
            .iter()
            .map(|(n, k)| (n.as_str(), k.as_str()))
            .unzip();
        let c = client!(self);
        // One statement, so one snapshot and one round trip for every
        // stream; a repeated request is answered under each ordinal.
        let rows = c
            .query(
                "SELECT r.i, l.seq, COALESCE(l.ts_ms, l.ts*1000), l.capture_id,
                        l.content_hash, l.val
                 FROM unnest($1::text[], $2::text[]) WITH ORDINALITY AS r(ns, key, i)
                 JOIN df_log l ON l.ns = r.ns AND l.key = r.key
                 ORDER BY r.i, l.seq",
                &[&nss, &keys],
            )
            .await?;
        for r in rows {
            let i = r.get::<_, i64>(0) as usize - 1;
            out[i].push(LogEntryMeta {
                seq: r.get::<_, i64>(1) as u64,
                stored_at_ms: r.get(2),
                capture_id: r.get(3),
                content_hash: r.get(4),
                value: r.get(5),
            });
        }
        Ok(out)
    }

    async fn apply_batch(&self, batch: &AtomicBatch) -> Result<BatchOutcome> {
        batch.validate()?;
        let fault = self.batch_fault.lock().expect("batch fault").take();
        if fault == Some(BatchFault::BeforeTransaction) {
            anyhow::bail!("injected failure before the transaction");
        }
        let mut c = client!(self);
        // The store lock first, then every read. Read committed: each
        // statement after the lock sees every transaction committed before
        // the lock was granted, and every writer of guarded data holds the
        // same lock until it commits, so a precondition that holds when read
        // holds until this commit. (A repeatable-read or serializable
        // snapshot would be taken by the lock statement itself, before the
        // wait, and miss writers that committed meanwhile.) Dropped without
        // a commit, the transaction rolls back.
        let tx = c.transaction().await?;
        tx.execute(LOCK_LOG_SQL, &[&LOG_ALLOCATION_LOCK]).await?;
        let now = now_secs();
        for (i, p) in batch.preconditions.iter().enumerate() {
            let holds = match p {
                Precondition::LogHead { ns, key, seq } => {
                    pg_log_head(&tx, ns, key).await? == *seq
                }
                Precondition::KvIs { ns, key, value } => {
                    pg_kv_live(&tx, ns, key, now).await? == *value
                }
            };
            if !holds {
                return Ok(BatchOutcome::Conflict {
                    why: format!("precondition {i} no longer holds ({p:?})"),
                });
            }
        }
        let mut seqs: Vec<Option<u64>> = vec![None; batch.ops.len()];
        for (i, op) in batch.ops.iter().enumerate() {
            if fault == Some(BatchFault::AfterWrites(i)) {
                anyhow::bail!("injected failure after {i} staged writes");
            }
            match op {
                BatchOp::LogAppendIfAbsent {
                    ns,
                    key,
                    capture_id,
                    value,
                } => {
                    tx.execute(ENSURE_LOG_META_SQL, &[ns, key]).await?;
                    let inserted = tx
                        .query_opt(
                            "INSERT INTO df_log(ns, key, val, ts, ts_ms, capture_id, content_hash)
                             VALUES($1, $2, $3, $4, $5, $6, $7)
                             ON CONFLICT (ns, key, capture_id) WHERE capture_id IS NOT NULL
                             DO NOTHING
                             RETURNING seq",
                            &[ns, key, value, &now_secs(), &now_ms(), capture_id, &content_digest(value)],
                        )
                        .await?;
                    let Some(row) = inserted else {
                        return Ok(BatchOutcome::Conflict {
                            why: format!(
                                "{capture_id} already exists in {key}"
                            ),
                        });
                    };
                    let seq: i64 = row.get(0);
                    tx.execute(
                        "UPDATE df_log_meta SET head_seq=GREATEST(head_seq, $3) WHERE ns=$1 AND key=$2",
                        &[ns, key, &seq],
                    )
                    .await?;
                    seqs[i] = Some(seq as u64);
                }
                BatchOp::KvPut { ns, key, value } => {
                    let bytes = match value {
                        BatchValue::Bytes(b) => b.clone(),
                        BatchValue::SeqTemplate { template, of } => {
                            render_seq(template, seqs[*of].expect("validated"))
                        }
                    };
                    pg_kv_put(&tx, ns, key, &bytes).await?;
                }
                BatchOp::KvMaxSeq { ns, key, of } => {
                    let current = pg_kv_live(&tx, ns, key, now).await?;
                    let bytes = max_seq_value(
                        current.as_deref(),
                        seqs[*of].expect("validated"),
                    )?;
                    pg_kv_put(&tx, ns, key, &bytes).await?;
                }
                BatchOp::KvMax { ns, key, value } => {
                    let current = pg_kv_live(&tx, ns, key, now).await?;
                    let bytes = max_seq_value(current.as_deref(), *value)?;
                    pg_kv_put(&tx, ns, key, &bytes).await?;
                }
                BatchOp::CommitMarker { batch_id, digest } => {
                    let result = crate::encode_batch_result(
                        &batch.ops[..i],
                        &seqs[..i],
                    )?;
                    tx.execute(
                        "INSERT INTO df_batch_commit(batch_id, digest, result, committed_at)
                         VALUES($1, $2, $3, $4)",
                        &[batch_id, digest, &result, &now_ms()],
                    )
                    .await?;
                }
            }
        }
        if fault == Some(BatchFault::AfterWrites(batch.ops.len())) {
            anyhow::bail!("injected failure after every write was staged");
        }
        tx.commit().await?;
        if fault == Some(BatchFault::AfterCommit) {
            anyhow::bail!("injected failure after the commit");
        }
        Ok(BatchOutcome::Committed { seqs })
    }

    async fn batch_commit_get(
        &self,
        batch_id: &str,
    ) -> Result<Option<crate::BatchCommitRecord>> {
        // Under the store lock: a batch that wrote this marker holds the lock
        // until its commit or rollback has ended, so the answer is final.
        let mut c = client!(self);
        let tx = c.transaction().await?;
        tx.execute(LOCK_LOG_SQL, &[&LOG_ALLOCATION_LOCK]).await?;
        let row = tx
            .query_opt(
                "SELECT digest, result, committed_at FROM df_batch_commit WHERE batch_id=$1",
                &[&batch_id],
            )
            .await?;
        tx.commit().await?;
        Ok(row.map(|r| crate::BatchCommitRecord {
            digest: r.get(0),
            result: r.get(1),
            committed_at_ms: r.get(2),
        }))
    }

    async fn batch_commit_sweep(
        &self,
        before_ms: i64,
        limit: usize,
    ) -> Result<usize> {
        // One chunk, one transaction: the store lock is released between
        // chunks.
        let mut c = client!(self);
        let tx = c.transaction().await?;
        tx.execute(LOCK_LOG_SQL, &[&LOG_ALLOCATION_LOCK]).await?;
        let n = tx
            .execute(
                "DELETE FROM df_batch_commit WHERE batch_id IN (
                   SELECT batch_id FROM df_batch_commit WHERE committed_at < $1
                   ORDER BY committed_at LIMIT $2)",
                &[&before_ms, &(limit as i64)],
            )
            .await?;
        tx.commit().await?;
        Ok(n as usize)
    }

    async fn log_find_capture(
        &self,
        ns: &str,
        key: &str,
        capture_id: &str,
    ) -> Result<Option<(u64, Vec<u8>)>> {
        let c = client!(self);
        Ok(c
            .query_opt(
                "SELECT seq, val FROM df_log WHERE ns=$1 AND key=$2 AND capture_id=$3",
                &[&ns, &key, &capture_id],
            )
            .await?
            .map(|r| (r.get::<_, i64>(0) as u64, r.get(1))))
    }

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
        let mut c = client!(self);
        if crate::is_guarded_namespace(ns) {
            // A guarded key is written under the store lock (see
            // `is_guarded_namespace`).
            let tx = c.transaction().await?;
            tx.execute(LOCK_LOG_SQL, &[&LOG_ALLOCATION_LOCK]).await?;
            pg_kv_put(&tx, ns, key, value).await?;
            tx.commit().await?;
        } else {
            // Any other key: one auto-committed statement.
            c.execute(KV_PUT_SQL, &[&ns, &key, &value, &now_secs()])
                .await?;
        }
        Ok(())
    }

    async fn kv_put_with_ttl(
        &self,
        ns: &str,
        key: &str,
        value: &[u8],
        ttl_secs: u64,
    ) -> Result<()> {
        anyhow::ensure!(
            !crate::is_guarded_namespace(ns),
            "a key in guarded namespace {ns} cannot carry a TTL"
        );
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
        const DELETE: &str = "DELETE FROM df_kv WHERE ns=$1 AND key=$2";
        let mut c = client!(self);
        let n = if crate::is_guarded_namespace(ns) {
            // Under the store lock, like every guarded write.
            let tx = c.transaction().await?;
            tx.execute(LOCK_LOG_SQL, &[&LOG_ALLOCATION_LOCK]).await?;
            let n = tx.execute(DELETE, &[&ns, &key]).await?;
            tx.commit().await?;
            n
        } else {
            // Any other key: one auto-committed statement.
            c.execute(DELETE, &[&ns, &key]).await?
        };
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
            // Literal, byte-wise "starts with" (never LIKE, whose `%`/`_`
            // wildcards would match other keys); `COLLATE "C"` keeps the range
            // byte-wise whatever the database collation.
            let upper = crate::prefix_successor(p);
            match (ns == crate::CHECKPOINTS_NS, &upper) {
                (true, Some(u)) => {
                    c.query(KV_LIST_CHECKPOINTS_PREFIX_SQL, &[&p, u, &now])
                        .await?
                }
                (true, None) => {
                    c.query(KV_LIST_CHECKPOINTS_PREFIX_OPEN_SQL, &[&p, &now])
                        .await?
                }
                (false, Some(u)) => {
                    c.query(KV_LIST_PREFIX_SQL, &[&ns, &p, u, &now]).await?
                }
                (false, None) => {
                    c.query(KV_LIST_PREFIX_OPEN_SQL, &[&ns, &p, &now]).await?
                }
            }
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
        tx.execute(LOCK_LOG_SQL, &[&LOG_ALLOCATION_LOCK]).await?;
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

    async fn log_ns_max_seq(&self, ns: &str) -> Result<u64> {
        let c = client!(self);
        let row = c
            .query_one(
                "SELECT COALESCE(MAX(seq), 0) FROM df_log WHERE ns=$1",
                &[&ns],
            )
            .await?;
        Ok(row.get::<_, i64>(0) as u64)
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
        tx.execute(LOCK_LOG_SQL, &[&LOG_ALLOCATION_LOCK]).await?;
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
        // A writer of df_log and df_log_meta: under the store lock.
        tx.execute(LOCK_LOG_SQL, &[&LOG_ALLOCATION_LOCK]).await?;
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
        const META: &str = "SELECT min_valid_from_seq, head_seq FROM df_log_meta WHERE ns=$1 AND key=$2";
        // The meta row is created (a write) only under the store lock.
        let meta_row = match tx.query_opt(META, &[&ns, &key]).await? {
            Some(row) => row,
            None => {
                tx.execute(LOCK_LOG_SQL, &[&LOG_ALLOCATION_LOCK]).await?;
                tx.execute(ENSURE_LOG_META_SQL, &[&ns, &key]).await?;
                tx.query_one(META, &[&ns, &key]).await?
            }
        };
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

    async fn log_read_meta_since(
        &self,
        ns: &str,
        key: &str,
        since_seq: u64,
        limit: usize,
    ) -> Result<Vec<LogEntryMeta>> {
        let c = client!(self);
        let rows = c
            .query(
                "SELECT seq, COALESCE(ts_ms, ts*1000), capture_id, content_hash, val
                 FROM df_log WHERE ns=$1 AND key=$2 AND seq>$3
                 ORDER BY seq ASC LIMIT $4",
                &[&ns, &key, &(since_seq as i64), &(limit as i64)],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| LogEntryMeta {
                seq: r.get::<_, i64>(0) as u64,
                stored_at_ms: r.get(1),
                capture_id: r.get(2),
                content_hash: r.get(3),
                value: r.get(4),
            })
            .collect())
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

    async fn slot_list(
        &self,
        ns: &str,
        prefix: Option<&str>,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<crate::SlotPage> {
        let prefix_norm = prefix.unwrap_or("");
        let after = match cursor {
            Some(c) => Some(crate::decode_slot_cursor(c, ns, prefix_norm)?),
            None => None,
        };
        let eff = limit.min(crate::SLOT_LIST_MAX_LIMIT);
        if eff == 0 {
            return Ok(crate::SlotPage {
                records: Vec::new(),
                next_cursor: None,
            });
        }
        let (lower, upper) = if prefix_norm.is_empty() {
            (None, None)
        } else {
            (
                Some(prefix_norm.to_string()),
                crate::prefix_successor(prefix_norm),
            )
        };
        let fetch = (eff as i64) + 1;
        let c = client!(self);
        // `COLLATE "C"` forces byte-wise comparison/order regardless of the
        // database's default collation, so ordering agrees with Rust `str` order
        // (and the SQLite/Memory backends) and keyset pagination never skips or
        // duplicates a stable key.
        let rows = c
            .query(SLOT_LIST_SQL, &[&ns, &lower, &upper, &after, &fetch])
            .await?;
        let mut all: Vec<(String, u64, Vec<u8>)> = rows
            .into_iter()
            .map(|r| {
                (
                    r.get::<_, String>(0),
                    r.get::<_, i64>(1) as u64,
                    r.get::<_, Vec<u8>>(2),
                )
            })
            .collect();
        let has_more = all.len() > eff;
        all.truncate(eff);
        let next_cursor = if has_more {
            all.last()
                .map(|(k, _, _)| crate::encode_slot_cursor(ns, prefix_norm, k))
        } else {
            None
        };
        let records = all
            .into_iter()
            .map(|(key, version, value)| crate::SlotRecord {
                key,
                version,
                value,
            })
            .collect();
        Ok(crate::SlotPage {
            records,
            next_cursor,
        })
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

    /// Regression: every migration must be exactly one statement with no ';'. This
    /// prevents reintroducing a multi-statement blob parsed by splitting on ';', which
    /// a comment or literal containing a semicolon would break. Runs without a DB.
    #[test]
    fn migrations_are_single_statements() {
        assert!(!MIGRATIONS.is_empty());
        for stmt in MIGRATIONS {
            assert!(
                !stmt.contains(';'),
                "migration must be a single statement with no ';': {stmt}"
            );
        }
    }

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
        suite::ns_max_seq_scoped(be.clone(), &format!("it{base}_nsmax")).await;
    }

    /// Waits until `n` sessions wait for the log-allocation lock.
    async fn lock_waiters(
        admin: &deadpool_postgres::tokio_postgres::Client,
        n: i64,
    ) {
        for _ in 0..500 {
            let row = admin
                .query_one(
                    "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted",
                    &[],
                )
                .await
                .unwrap();
            if row.get::<_, i64>(0) >= n {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("{n} lock waiters never appeared");
    }

    /// A batch's preconditions are read after it holds the store lock: a
    /// writer that committed while the batch waited for the lock is seen,
    /// and the batch reports a conflict instead of committing on a stale
    /// read.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires a live PostgreSQL (DELTAFORGE_IT_PG_DSN)"]
    async fn pg_batch_preconditions_see_a_writer_that_committed_while_the_batch_waited()
     {
        use deadpool_postgres::tokio_postgres;
        let dsn = std::env::var("DELTAFORGE_IT_PG_DSN")
            .expect("DELTAFORGE_IT_PG_DSN must be set to run this test");
        let be = PostgresStorageBackend::connect(&dsn).await.unwrap();
        let ns = format!("schemas.v1.it{}_stale", now_ms());
        let (admin, conn) = tokio_postgres::connect(&dsn, NoTls).await.unwrap();
        tokio::spawn(conn);
        admin.batch_execute("BEGIN").await.unwrap();
        admin
            .execute(LOCK_LOG_SQL, &[&LOG_ALLOCATION_LOCK])
            .await
            .unwrap();

        // A writer queues for the lock first, then a batch that requires the
        // writer's stream to be still empty.
        let writer = {
            let (be, ns) = (be.clone(), ns.clone());
            tokio::spawn(async move {
                be.log_append_if_absent(&ns, "s", "w1", b"w").await
            })
        };
        lock_waiters(&admin, 1).await;
        let batch = AtomicBatch {
            preconditions: vec![Precondition::LogHead {
                ns: ns.clone(),
                key: "s".into(),
                seq: 0,
            }],
            ops: vec![BatchOp::LogAppendIfAbsent {
                ns: ns.clone(),
                key: "other".into(),
                capture_id: "a1".into(),
                value: b"a".to_vec(),
            }],
        };
        let applied = {
            let be = be.clone();
            tokio::spawn(async move { be.apply_batch(&batch).await })
        };
        lock_waiters(&admin, 2).await;
        admin.batch_execute("COMMIT").await.unwrap();

        writer.await.unwrap().unwrap();
        let outcome = applied.await.unwrap().unwrap();
        assert!(
            matches!(outcome, BatchOutcome::Conflict { .. }),
            "the stream was written before the batch held the lock: {outcome:?}"
        );
    }

    /// A write to a guarded key cannot commit between a batch's precondition
    /// read and its commit: it waits for the store lock the batch holds, so a
    /// batch that commits on `KvIs` committed before the key changed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires a live PostgreSQL (DELTAFORGE_IT_PG_DSN)"]
    async fn pg_guarded_kv_writes_serialize_with_batches() {
        use deadpool_postgres::tokio_postgres;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let dsn = std::env::var("DELTAFORGE_IT_PG_DSN")
            .expect("DELTAFORGE_IT_PG_DSN must be set to run this test");
        let be = PostgresStorageBackend::connect(&dsn).await.unwrap();
        let key = format!("it{}_guarded", now_ms());
        let ns = "schemas.v1.index";
        be.kv_put(ns, &key, b"v1").await.unwrap();
        let (admin, conn) = tokio_postgres::connect(&dsn, NoTls).await.unwrap();
        tokio::spawn(conn);
        admin.batch_execute("BEGIN").await.unwrap();
        admin
            .execute(LOCK_LOG_SQL, &[&LOG_ALLOCATION_LOCK])
            .await
            .unwrap();

        // The batch waits for the lock first, then the key is rewritten.
        let done = Arc::new(AtomicUsize::new(0));
        let batch = AtomicBatch {
            preconditions: vec![Precondition::KvIs {
                ns: ns.into(),
                key: key.clone(),
                value: Some(b"v1".to_vec()),
            }],
            ops: vec![BatchOp::LogAppendIfAbsent {
                ns: format!("schemas.v1.{key}"),
                key: "s".into(),
                capture_id: "a1".into(),
                value: b"a".to_vec(),
            }],
        };
        let applied = {
            let (be, done) = (be.clone(), done.clone());
            tokio::spawn(async move {
                let r = be.apply_batch(&batch).await;
                (r, done.fetch_add(1, Ordering::SeqCst))
            })
        };
        lock_waiters(&admin, 1).await;
        let writer = {
            let (be, done, key) = (be.clone(), done.clone(), key.clone());
            tokio::spawn(async move {
                let r = be.kv_put(ns, &key, b"v2").await;
                (r, done.fetch_add(1, Ordering::SeqCst))
            })
        };
        // Long enough for a writer that ignores the lock to commit.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        admin.batch_execute("COMMIT").await.unwrap();

        let (written, writer_rank) = writer.await.unwrap();
        written.unwrap();
        let (outcome, batch_rank) = applied.await.unwrap();
        let outcome = outcome.unwrap();
        if matches!(outcome, BatchOutcome::Committed { .. }) {
            assert!(
                batch_rank < writer_rank,
                "the batch committed on v1 after the key became v2"
            );
        }
        assert_eq!(
            be.kv_get(ns, &key).await.unwrap().as_deref(),
            Some(&b"v2"[..])
        );
    }

    /// Which writers take the store lock: while another session holds it,
    /// writes of unguarded keys finish (one auto-committed statement, no
    /// lock), and guarded kv writes, deletes and log truncation wait until it
    /// is released, then complete correctly.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires a live PostgreSQL (DELTAFORGE_IT_PG_DSN)"]
    async fn pg_guarded_writers_wait_for_the_store_lock_and_others_do_not() {
        use deadpool_postgres::tokio_postgres;
        use std::time::Duration;
        let dsn = std::env::var("DELTAFORGE_IT_PG_DSN")
            .expect("DELTAFORGE_IT_PG_DSN must be set to run this test");
        let be = PostgresStorageBackend::connect(&dsn).await.unwrap();
        let run = now_ms();
        let (free_ns, guarded_ns) =
            (format!("it{run}_free"), "schemas.v1.index");
        let (gkey, stream_ns) =
            (format!("it{run}_g"), format!("schemas.v1.it{run}_trunc"));
        be.kv_put(&free_ns, "gone", b"x").await.unwrap();
        be.kv_put(guarded_ns, &gkey, b"x").await.unwrap();
        for i in 0..3 {
            be.log_append(&stream_ns, "s", format!("e{i}").as_bytes())
                .await
                .unwrap();
        }
        let (admin, conn) = tokio_postgres::connect(&dsn, NoTls).await.unwrap();
        tokio::spawn(conn);
        admin.batch_execute("BEGIN").await.unwrap();
        admin
            .execute(LOCK_LOG_SQL, &[&LOG_ALLOCATION_LOCK])
            .await
            .unwrap();

        // Unguarded keys never wait for the lock.
        tokio::time::timeout(Duration::from_secs(5), async {
            be.kv_put(&free_ns, "k", b"v").await.unwrap();
            assert!(be.kv_delete(&free_ns, "gone").await.unwrap());
        })
        .await
        .expect("an unguarded write waited for the store lock");

        // Guarded writers wait while the lock is held.
        let put = {
            let (be, gkey) = (be.clone(), gkey.clone());
            tokio::spawn(
                async move { be.kv_put(guarded_ns, &gkey, b"y").await },
            )
        };
        let delete = {
            let (be, gkey) = (be.clone(), gkey.clone());
            tokio::spawn(async move {
                be.kv_delete(guarded_ns, &format!("{gkey}_other")).await
            })
        };
        let truncate = {
            let (be, stream_ns) = (be.clone(), stream_ns.clone());
            tokio::spawn(async move {
                be.log_truncate(
                    &stream_ns,
                    "s",
                    LogTruncateRequest {
                        older_than_ms: None,
                        pin_seq: u64::MAX,
                        max_entries: Some(1),
                        max_bytes: None,
                    },
                )
                .await
            })
        };
        lock_waiters(&admin, 3).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            !put.is_finished(),
            "a guarded kv_put ran without the store lock"
        );
        assert!(
            !delete.is_finished(),
            "a guarded kv_delete ran without the store lock"
        );
        assert!(
            !truncate.is_finished(),
            "log_truncate ran without the store lock"
        );

        admin.batch_execute("COMMIT").await.unwrap();
        put.await.unwrap().unwrap();
        assert!(!delete.await.unwrap().unwrap());
        let out = truncate.await.unwrap().unwrap();
        assert_eq!(out.removed, 2, "{out:?}");
        assert_eq!(
            be.kv_get(guarded_ns, &gkey).await.unwrap().as_deref(),
            Some(&b"y"[..])
        );
        assert_eq!(
            be.kv_get(&free_ns, "k").await.unwrap().as_deref(),
            Some(&b"v"[..])
        );
    }

    /// The store lock serializes guarded writes of every source sharing the
    /// store. Qualification at 1, 8 and 32 concurrent sources, each
    /// committing batches on its own stream plus the shared sequence
    /// high-water: no source starves, unrelated sources never conflict, and
    /// sequences stay unique and increase per source; the high-water ends at
    /// the largest sequence. The measured latency and throughput are printed
    /// (`--nocapture`).
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[ignore = "requires a live PostgreSQL (DELTAFORGE_IT_PG_DSN)"]
    async fn pg_store_lock_qualification_across_sources() {
        use deadpool_postgres::tokio_postgres;
        use std::time::{Duration, Instant};
        let dsn = std::env::var("DELTAFORGE_IT_PG_DSN")
            .expect("DELTAFORGE_IT_PG_DSN must be set to run this test");
        let be = PostgresStorageBackend::connect(&dsn).await.unwrap();
        let (admin, conn) = tokio_postgres::connect(&dsn, NoTls).await.unwrap();
        tokio::spawn(conn);
        let run = now_ms();
        for sources in [1usize, 8, 32] {
            let ns = format!("schemas.v1.it{run}_q{sources}");
            let hw_ns = "schemas.v1.index";
            let hw_key = format!("it{run}_q{sources}_hw");
            let until = Instant::now() + Duration::from_secs(3);
            let mut tasks = Vec::new();
            for src in 0..sources {
                let (be, ns, hw_key) = (be.clone(), ns.clone(), hw_key.clone());
                tasks.push(tokio::spawn(async move {
                    let (mut head, mut seqs, mut lat) =
                        (0u64, Vec::new(), Vec::new());
                    let mut conflicts = 0usize;
                    while Instant::now() < until {
                        let n = seqs.len();
                        let batch = AtomicBatch {
                            preconditions: vec![Precondition::LogHead {
                                ns: ns.clone(),
                                key: format!("s{src}"),
                                seq: head,
                            }],
                            ops: vec![
                                BatchOp::LogAppendIfAbsent {
                                    ns: ns.clone(),
                                    key: format!("s{src}"),
                                    capture_id: format!("c{n}"),
                                    value: vec![b'x'; 256],
                                },
                                BatchOp::KvMaxSeq {
                                    ns: hw_ns.into(),
                                    key: hw_key.clone(),
                                    of: 0,
                                },
                            ],
                        };
                        let t = Instant::now();
                        match be.apply_batch(&batch).await.unwrap() {
                            BatchOutcome::Committed { seqs: s } => {
                                head = s[0].unwrap();
                                seqs.push(head);
                            }
                            BatchOutcome::Conflict { .. } => conflicts += 1,
                        }
                        lat.push(t.elapsed());
                    }
                    (seqs, lat, conflicts)
                }));
            }
            // Sample the sessions waiting for the lock while the sources run.
            let mut waiters = Vec::new();
            while Instant::now() < until {
                let row = admin
                    .query_one(
                        "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted",
                        &[],
                    )
                    .await
                    .unwrap();
                waiters.push(row.get::<_, i64>(0));
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            let (mut all_seqs, mut lat, mut conflicts) =
                (Vec::new(), Vec::new(), 0);
            for t in tasks {
                let (seqs, l, c) = t.await.unwrap();
                assert!(
                    !seqs.is_empty(),
                    "a source starved at {sources} sources"
                );
                assert!(
                    seqs.windows(2).all(|w| w[0] < w[1]),
                    "one source's sequences went back"
                );
                all_seqs.extend(seqs);
                lat.extend(l);
                conflicts += c;
            }
            assert_eq!(conflicts, 0, "unrelated sources conflicted");
            let total = all_seqs.len();
            all_seqs.sort_unstable();
            all_seqs.dedup();
            assert_eq!(all_seqs.len(), total, "a sequence was committed twice");
            let hw: u64 = serde_json::from_slice(
                &be.kv_get(hw_ns, &hw_key).await.unwrap().unwrap(),
            )
            .unwrap();
            assert_eq!(
                hw,
                *all_seqs.last().unwrap(),
                "the high-water is not the largest sequence"
            );
            lat.sort_unstable();
            let q = |p: f64| lat[((lat.len() - 1) as f64 * p) as usize];
            let max = *lat.last().unwrap();
            assert!(
                max < Duration::from_secs(2),
                "a batch waited {max:?} at {sources} sources"
            );
            waiters.sort_unstable();
            eprintln!(
                "store lock, {sources} sources: {total} batches in 3 s ({:.0}/s), latency p50 {:?} p99 {:?} max {:?}, lock waiters p50 {} max {}",
                total as f64 / 3.0,
                q(0.5),
                q(0.99),
                max,
                waiters[waiters.len() / 2],
                waiters.last().unwrap()
            );
        }
    }

    /// The shared atomic-batch conformance suite against a live PostgreSQL
    /// (env-gated like the log contracts).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires a live PostgreSQL (DELTAFORGE_IT_PG_DSN)"]
    async fn pg_atomic_batch_conformance() {
        let dsn = std::env::var("DELTAFORGE_IT_PG_DSN")
            .expect("DELTAFORGE_IT_PG_DSN must be set to run this test");
        let be = PostgresStorageBackend::connect(&dsn).await.unwrap();
        let dynbe: Arc<dyn StorageBackend> = be.clone();
        crate::atomic_batch_suite::all(
            dynbe,
            be.as_ref(),
            &format!("it{}_batch", now_ms()),
        )
        .await;
    }

    /// A fresh database whose default collation is NOT "C", with the schema
    /// applied by `connect`.
    async fn non_c_database(
        dsn: &str,
    ) -> (String, deadpool_postgres::tokio_postgres::Client, String) {
        use deadpool_postgres::tokio_postgres;
        let db = format!("collate_it_{}", now_ms());
        let (admin, conn) = tokio_postgres::connect(dsn, NoTls).await.unwrap();
        tokio::spawn(conn);
        admin
            .batch_execute(&format!(
                "CREATE DATABASE {db} TEMPLATE template0 \
                 LC_COLLATE 'en_US.utf8' LC_CTYPE 'en_US.utf8'"
            ))
            .await
            .unwrap();
        let db_dsn = format!("{dsn} dbname={db}");
        let (c, conn) = tokio_postgres::connect(&db_dsn, NoTls).await.unwrap();
        tokio::spawn(conn);
        let collation: String = c
            .query_one(
                "SELECT datcollate FROM pg_database WHERE datname = current_database()",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert!(
            collation != "C" && collation != "POSIX",
            "the test database must not use the C collation ({collation})"
        );
        (db, c, db_dsn)
    }

    async fn drop_database(dsn: &str, db: &str) {
        use deadpool_postgres::tokio_postgres;
        let (admin, conn) = tokio_postgres::connect(dsn, NoTls).await.unwrap();
        tokio::spawn(conn);
        let _ = admin
            .batch_execute(&format!("DROP DATABASE {db} WITH (FORCE)"))
            .await;
    }

    fn plan_text(rows: Vec<deadpool_postgres::tokio_postgres::Row>) -> String {
        rows.iter()
            .map(|r| r.get::<_, String>(0))
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn assert_bounded_by(what: &str, plan: &str, index: &str) {
        assert!(
            !plan.contains("Seq Scan"),
            "{what} scans the table:\n{plan}"
        );
        assert!(plan.contains(index), "{what} does not use {index}:\n{plan}");
        let cond = plan
            .lines()
            .find(|l| l.contains("Index Cond"))
            .unwrap_or_else(|| panic!("{what}: no index condition:\n{plan}"));
        assert!(
            cond.contains(">=") && cond.contains("<") && cond.contains("key"),
            "{what}: the key range is not the index condition:\n{plan}"
        );
    }

    /// On a non-"C" database, the checkpoint prefix query (exact production SQL)
    /// is a bounded range on the PARTIAL checkpoint index - also as a generic
    /// plan - and `slot_list` a bounded range on `df_slot_ns_key_c`. #[ignore] +
    /// env-gated; creates and drops its own database.
    #[tokio::test]
    #[ignore = "requires a live PostgreSQL (DELTAFORGE_IT_PG_DSN)"]
    async fn pg_prefix_listings_use_the_c_collated_indexes() {
        let dsn = std::env::var("DELTAFORGE_IT_PG_DSN")
            .expect("DELTAFORGE_IT_PG_DSN must be set to run this test");
        let (db, c, db_dsn) = non_c_database(&dsn).await;
        let be = PostgresStorageBackend::connect(&db_dsn).await.unwrap();

        // Checkpoints alongside many registry-index rows in the same table.
        for i in 0..2000 {
            let key = format!("src-{i:04}::sink::kafka");
            be.kv_put(crate::CHECKPOINTS_NS, &key, b"cp").await.unwrap();
            be.kv_put("schemas.v1.index", &format!("v/{key}"), b"i")
                .await
                .unwrap();
            be.slot_upsert("snapshots", &key, b"s").await.unwrap();
        }
        c.batch_execute("ANALYZE df_kv; ANALYZE df_slot")
            .await
            .unwrap();

        let prefix = "src-0042::sink::";
        let upper = crate::prefix_successor(prefix).unwrap();
        let now: i64 = 0;
        let custom = plan_text(
            c.query(
                &format!("EXPLAIN {KV_LIST_CHECKPOINTS_PREFIX_SQL}"),
                &[&prefix, &upper, &now],
            )
            .await
            .unwrap(),
        );
        // The same statement as a generic plan (as a cached prepared
        // statement could be): the literal namespace still proves the partial
        // index's predicate.
        c.batch_execute(&format!(
            "SET plan_cache_mode = force_generic_plan;
             PREPARE cp(text, text, bigint) AS {KV_LIST_CHECKPOINTS_PREFIX_SQL}"
        ))
        .await
        .unwrap();
        let generic = plan_text(
            c.query(
                &format!("EXPLAIN EXECUTE cp('{prefix}', '{upper}', 0)"),
                &[],
            )
            .await
            .unwrap(),
        );
        c.batch_execute("DEALLOCATE cp; RESET plan_cache_mode")
            .await
            .unwrap();

        let fetch: i64 = 101;
        let after: Option<String> = None;
        let slot = plan_text(
            c.query(
                &format!("EXPLAIN {SLOT_LIST_SQL}"),
                &[&"snapshots", &Some(prefix), &Some(&upper), &after, &fetch],
            )
            .await
            .unwrap(),
        );
        println!(
            "checkpoint prefix (custom):\n{custom}\n\ncheckpoint prefix (generic):\n\
             {generic}\n\nslot_list:\n{slot}"
        );
        assert_bounded_by(
            "checkpoint prefix",
            &custom,
            "df_kv_checkpoints_key_c",
        );
        assert_bounded_by(
            "checkpoint prefix (generic plan)",
            &generic,
            "df_kv_checkpoints_key_c",
        );
        assert_bounded_by("slot_list", &slot, "df_slot_ns_key_c");

        // The checkpoint index is partial: it covers checkpoint rows only.
        let def: String = c
            .query_one(
                "SELECT indexdef FROM pg_indexes WHERE indexname = 'df_kv_checkpoints_key_c'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert!(def.contains("WHERE (ns = 'checkpoints'::text)"), "{def}");

        drop(be);
        drop(c);
        drop_database(&dsn, &db).await;
    }

    /// Upgrading a populated store: connecting the new version creates the
    /// prefix indexes and changes no row. #[ignore] + env-gated.
    #[tokio::test]
    #[ignore = "requires a live PostgreSQL (DELTAFORGE_IT_PG_DSN)"]
    async fn pg_upgrade_adds_the_prefix_indexes_without_touching_data() {
        let dsn = std::env::var("DELTAFORGE_IT_PG_DSN")
            .expect("DELTAFORGE_IT_PG_DSN must be set to run this test");
        let (db, c, db_dsn) = non_c_database(&dsn).await;
        {
            let be = PostgresStorageBackend::connect(&db_dsn).await.unwrap();
            for i in 0..500 {
                let key = format!("src-{i:03}::sink::kafka");
                be.kv_put(
                    crate::CHECKPOINTS_NS,
                    &key,
                    format!("cp{i}").as_bytes(),
                )
                .await
                .unwrap();
                be.kv_put("schemas.v1.index", &format!("v/{key}"), b"i")
                    .await
                    .unwrap();
                be.slot_upsert("snapshots", &key, b"s").await.unwrap();
            }
        }
        // A store as the previous version left it: no prefix indexes.
        c.batch_execute(
            "DROP INDEX df_kv_checkpoints_key_c; DROP INDEX df_slot_ns_key_c",
        )
        .await
        .unwrap();
        let fingerprint = || async {
            let kv: String = c
                .query_one(
                    "SELECT md5(string_agg(ns || '/' || key || '/' || encode(val, 'hex') \
                     || '/' || updated_at || '/' || coalesce(expires_at::text, '-'), \
                     ',' ORDER BY ns, key)) FROM df_kv",
                    &[],
                )
                .await
                .unwrap()
                .get(0);
            let slot: String = c
                .query_one(
                    "SELECT md5(string_agg(ns || '/' || key || '/' || version || '/' \
                     || encode(state, 'hex') || '/' || updated_at, ',' ORDER BY ns, key)) \
                     FROM df_slot",
                    &[],
                )
                .await
                .unwrap()
                .get(0);
            (kv, slot)
        };
        let indexes = || async {
            c.query(
                "SELECT indexname FROM pg_indexes WHERE indexname IN \
                 ('df_kv_checkpoints_key_c', 'df_slot_ns_key_c') ORDER BY 1",
                &[],
            )
            .await
            .unwrap()
            .len()
        };
        let before = fingerprint().await;
        assert_eq!(indexes().await, 0);

        let be = PostgresStorageBackend::connect(&db_dsn).await.unwrap();
        assert_eq!(indexes().await, 2, "connect created the prefix indexes");
        assert_eq!(fingerprint().await, before, "no row changed");
        let mut keys = be
            .kv_list(crate::CHECKPOINTS_NS, Some("src-042::"))
            .await
            .unwrap();
        keys.sort();
        assert_eq!(keys, ["src-042::sink::kafka"]);

        drop(be);
        drop(c);
        drop_database(&dsn, &db).await;
    }

    /// `kv_list` literal-prefix contract against a live PostgreSQL (LIKE
    /// wildcards and escapes must not apply). #[ignore] + env-gated.
    #[tokio::test]
    #[ignore = "requires a live PostgreSQL (DELTAFORGE_IT_PG_DSN)"]
    async fn pg_kv_list_prefix_is_literal() {
        let dsn = std::env::var("DELTAFORGE_IT_PG_DSN")
            .expect("DELTAFORGE_IT_PG_DSN must be set to run this test");
        let be: Arc<dyn StorageBackend> =
            PostgresStorageBackend::connect(&dsn).await.unwrap();
        crate::kv_list_contract_suite::literal_prefix(
            be,
            &format!("it{}_kvp", now_ms()),
        )
        .await;
    }

    /// The shared `slot_list` contract suite against a live PostgreSQL, exercising
    /// the `COLLATE "C"` byte-wise ordering path. #[ignore] + env-gated; each case
    /// runs under a unique namespace so it is safe against a shared database and
    /// repeated runs.
    #[tokio::test]
    #[ignore = "requires a live PostgreSQL (DELTAFORGE_IT_PG_DSN)"]
    async fn pg_slot_list_contracts() {
        let dsn = std::env::var("DELTAFORGE_IT_PG_DSN")
            .expect("DELTAFORGE_IT_PG_DSN must be set to run this test");
        use crate::slot_list_contract_suite as suite;
        let be: Arc<dyn StorageBackend> =
            PostgresStorageBackend::connect(&dsn).await.unwrap();
        let base = now_ms();
        suite::namespace_isolation(be.clone(), &format!("it{base}_iso")).await;
        suite::prefix_correctness(be.clone(), &format!("it{base}_pfx")).await;
        suite::pagination_without_duplicates(
            be.clone(),
            &format!("it{base}_page"),
        )
        .await;
        suite::concurrent_mutation_tolerance(
            be.clone(),
            &format!("it{base}_conc"),
        )
        .await;
        suite::malformed_cursor_rejected(be.clone(), &format!("it{base}_cur"))
            .await;
        suite::zero_limit_is_empty(be.clone(), &format!("it{base}_zero")).await;
        suite::unicode_ordering_and_pagination(
            be.clone(),
            &format!("it{base}_uni"),
        )
        .await;
        suite::prefix_across_utf8_length_boundary(
            be.clone(),
            &format!("it{base}_utf8"),
        )
        .await;
        suite::cursor_validation(be.clone(), &format!("it{base}_curval")).await;
    }
}
