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

/// Items per array statement (`docs/design/mysql-mass-invalidation.md`,
/// "Chunking and size limits").
pub(crate) const SET_CHUNK_ITEMS: usize = 4096;
/// Payload bytes at which an array statement's chunk closes.
pub(crate) const SET_CHUNK_BYTES: usize = 4 * 1024 * 1024;

/// A group's chunks, in request order, from each item's payload: a chunk
/// closes once it holds `SET_CHUNK_ITEMS` items or `SET_CHUNK_BYTES` of
/// payload (an item is added first, then the check).
pub(crate) fn set_chunks(
    sizes: impl Iterator<Item = usize>,
) -> Vec<std::ops::Range<usize>> {
    let mut out = Vec::new();
    let (mut start, mut n, mut bytes) = (0, 0, 0);
    for (i, size) in sizes.enumerate() {
        n += 1;
        bytes += size;
        if n == SET_CHUNK_ITEMS || bytes >= SET_CHUNK_BYTES {
            out.push(start..i + 1);
            (start, n, bytes) = (i + 1, 0, 0);
        }
    }
    if n > 0 {
        out.push(start..start + n);
    }
    out
}

type Tx<'a> = deadpool_postgres::tokio_postgres::Transaction<'a>;

/// Each `(index, ns, key)`'s live value (an expired one reads as absent).
async fn pg_kv_live_many(
    tx: &Tx<'_>,
    items: &[(usize, &str, &str)],
    now: i64,
) -> Result<Vec<(usize, Option<Vec<u8>>)>> {
    let mut out = Vec::with_capacity(items.len());
    for chunk in set_chunks(items.iter().map(|i| i.1.len() + i.2.len())) {
        let rows = &items[chunk];
        let found = tx
            .query(
                "SELECT r.i, k.val, k.expires_at
                 FROM unnest($1::text[], $2::text[]) WITH ORDINALITY AS r(ns, key, i)
                 LEFT JOIN df_kv k ON k.ns = r.ns AND k.key = r.key
                 ORDER BY r.i",
                &[
                    &rows.iter().map(|i| i.1).collect::<Vec<_>>(),
                    &rows.iter().map(|i| i.2).collect::<Vec<_>>(),
                ],
            )
            .await?;
        for r in found {
            let i = r.get::<_, i64>(0) as usize - 1;
            let exp: Option<i64> = r.get(2);
            let val: Option<Vec<u8>> = r.get(1);
            out.push((
                rows[i].0,
                val.filter(|_| !exp.is_some_and(|e| e <= now)),
            ));
        }
    }
    Ok(out)
}

/// Each `(index, ns, key)` stream's last entry.
async fn pg_log_latest_many(
    tx: &Tx<'_>,
    items: &[(usize, &str, &str)],
) -> Result<Vec<(usize, Option<(u64, Vec<u8>)>)>> {
    let mut out = Vec::with_capacity(items.len());
    for chunk in set_chunks(items.iter().map(|i| i.1.len() + i.2.len())) {
        let rows = &items[chunk];
        let found = tx
            .query(
                "SELECT r.i, l.seq, l.val
                 FROM unnest($1::text[], $2::text[]) WITH ORDINALITY AS r(ns, key, i)
                 LEFT JOIN LATERAL (
                   SELECT seq, val FROM df_log
                   WHERE ns = r.ns AND key = r.key ORDER BY seq DESC LIMIT 1
                 ) l ON true
                 ORDER BY r.i",
                &[
                    &rows.iter().map(|i| i.1).collect::<Vec<_>>(),
                    &rows.iter().map(|i| i.2).collect::<Vec<_>>(),
                ],
            )
            .await?;
        for r in found {
            let i = r.get::<_, i64>(0) as usize - 1;
            let seq: Option<i64> = r.get(1);
            out.push((rows[i].0, seq.map(|s| (s as u64, r.get(2)))));
        }
    }
    Ok(out)
}

/// Each `(index, ns, key)` stream's last sequence (0 when empty).
async fn pg_log_head_many(
    tx: &Tx<'_>,
    items: &[(usize, &str, &str)],
) -> Result<Vec<(usize, u64)>> {
    let mut out = Vec::with_capacity(items.len());
    for chunk in set_chunks(items.iter().map(|i| i.1.len() + i.2.len())) {
        let rows = &items[chunk];
        let found = tx
            .query(
                "SELECT r.i, COALESCE((SELECT l.seq FROM df_log l
                                       WHERE l.ns = r.ns AND l.key = r.key
                                       ORDER BY l.seq DESC LIMIT 1), 0)
                 FROM unnest($1::text[], $2::text[]) WITH ORDINALITY AS r(ns, key, i)
                 ORDER BY r.i",
                &[
                    &rows.iter().map(|i| i.1).collect::<Vec<_>>(),
                    &rows.iter().map(|i| i.2).collect::<Vec<_>>(),
                ],
            )
            .await?;
        for r in found {
            let i = r.get::<_, i64>(0) as usize - 1;
            out.push((rows[i].0, r.get::<_, i64>(1) as u64));
        }
    }
    Ok(out)
}

/// Each `(index, ns, key)` stream's entry count.
async fn pg_log_len_many(
    tx: &Tx<'_>,
    items: &[(usize, &str, &str)],
) -> Result<Vec<(usize, u64)>> {
    let mut out = Vec::with_capacity(items.len());
    for chunk in set_chunks(items.iter().map(|i| i.1.len() + i.2.len())) {
        let rows = &items[chunk];
        let found = tx
            .query(
                "SELECT r.i, (SELECT COUNT(*) FROM df_log l
                              WHERE l.ns = r.ns AND l.key = r.key)
                 FROM unnest($1::text[], $2::text[]) WITH ORDINALITY AS r(ns, key, i)
                 ORDER BY r.i",
                &[
                    &rows.iter().map(|i| i.1).collect::<Vec<_>>(),
                    &rows.iter().map(|i| i.2).collect::<Vec<_>>(),
                ],
            )
            .await?;
        for r in found {
            let i = r.get::<_, i64>(0) as usize - 1;
            out.push((rows[i].0, r.get::<_, i64>(1) as u64));
        }
    }
    Ok(out)
}

/// The first precondition (in batch order) that no longer holds, with why;
/// each stream and key is read once. Two different expectations of one
/// stream or key cannot both hold: the later one is the conflict.
async fn pg_preconditions_conflict(
    tx: &Tx<'_>,
    pre: &[Precondition],
    now: i64,
) -> Result<Option<String>> {
    let mut heads: Vec<(usize, &str, &str)> = Vec::new();
    let mut values: Vec<(usize, &str, &str)> = Vec::new();
    let mut head_at = std::collections::HashMap::new();
    let mut value_at = std::collections::HashMap::new();
    for (i, p) in pre.iter().enumerate() {
        match p {
            Precondition::LogHead { ns, key, .. } => {
                let k = (ns.as_str(), key.as_str());
                if let std::collections::hash_map::Entry::Vacant(e) =
                    head_at.entry(k)
                {
                    e.insert(heads.len());
                    heads.push((i, k.0, k.1));
                }
            }
            Precondition::KvIs { ns, key, .. } => {
                let k = (ns.as_str(), key.as_str());
                if let std::collections::hash_map::Entry::Vacant(e) =
                    value_at.entry(k)
                {
                    e.insert(values.len());
                    values.push((i, k.0, k.1));
                }
            }
        }
    }
    let head_now = pg_log_head_many(tx, &heads).await?;
    let value_now = pg_kv_live_many(tx, &values, now).await?;
    for (i, p) in pre.iter().enumerate() {
        let holds = match p {
            Precondition::LogHead { ns, key, seq } => {
                head_now[head_at[&(ns.as_str(), key.as_str())]].1 == *seq
            }
            Precondition::KvIs { ns, key, value } => {
                value_now[value_at[&(ns.as_str(), key.as_str())]].1 == *value
            }
        };
        if !holds {
            return Ok(Some(format!(
                "precondition {i} no longer holds ({p:?})"
            )));
        }
    }
    Ok(None)
}

/// A log write as the batch checks it: its position, stream, capture
/// identity, bytes and whether a stored identical entry answers it.
type LogWrite<'a> = (usize, &'a str, &'a str, &'a str, &'a [u8], bool);

/// The stored `(seq, value)` of each `unique` log write's capture identity
/// that its stream holds already, by index into `unique`.
async fn pg_stored_captures(
    tx: &Tx<'_>,
    unique: &[LogWrite<'_>],
) -> Result<Vec<(usize, (u64, Vec<u8>))>> {
    let mut out = Vec::new();
    let mut offset = 0;
    for chunk in
        set_chunks(unique.iter().map(|u| u.1.len() + u.2.len() + u.3.len()))
    {
        let rows = &unique[chunk];
        let found = tx
            .query(
                "SELECT r.i, l.seq, l.val
                 FROM unnest($1::text[], $2::text[], $3::text[])
                      WITH ORDINALITY AS r(ns, key, capture_id, i)
                 JOIN df_log l ON l.ns = r.ns AND l.key = r.key
                                AND l.capture_id = r.capture_id
                 ORDER BY r.i",
                &[
                    &rows.iter().map(|u| u.1).collect::<Vec<_>>(),
                    &rows.iter().map(|u| u.2).collect::<Vec<_>>(),
                    &rows.iter().map(|u| u.3).collect::<Vec<_>>(),
                ],
            )
            .await?;
        for r in found {
            let i = r.get::<_, i64>(0) as usize - 1;
            out.push((offset + i, (r.get::<_, i64>(1) as u64, r.get(2))));
        }
        offset += rows.len();
    }
    Ok(out)
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
        if reads.is_empty() {
            return Ok(Vec::new());
        }
        let mut c = client!(self);
        // One snapshot; one array statement per kind and chunk.
        let tx = c
            .build_transaction()
            .isolation_level(deadpool_postgres::tokio_postgres::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await?;
        let now = now_secs();
        let mut out: Vec<Option<BatchReadResult>> = vec![None; reads.len()];
        let (mut kv, mut latest, mut head, mut len) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for (i, r) in reads.iter().enumerate() {
            match r {
                BatchRead::KvGet { ns, key } => {
                    kv.push((i, ns.as_str(), key.as_str()))
                }
                BatchRead::LogLatest { ns, key } => {
                    latest.push((i, ns.as_str(), key.as_str()))
                }
                BatchRead::LogHead { ns, key } => {
                    head.push((i, ns.as_str(), key.as_str()))
                }
                BatchRead::LogLen { ns, key } => {
                    len.push((i, ns.as_str(), key.as_str()))
                }
            }
        }
        for (k, v) in pg_kv_live_many(&tx, &kv, now).await? {
            out[k] = Some(BatchReadResult::Kv(v));
        }
        for (k, v) in pg_log_latest_many(&tx, &latest).await? {
            out[k] = Some(BatchReadResult::Latest(v));
        }
        for (k, v) in pg_log_head_many(&tx, &head).await? {
            out[k] = Some(BatchReadResult::Head(v));
        }
        for (k, v) in pg_log_len_many(&tx, &len).await? {
            out[k] = Some(BatchReadResult::Len(v));
        }
        tx.commit().await?;
        let out: Vec<BatchReadResult> = out
            .into_iter()
            .map(|r| r.expect("every read answered"))
            .collect();
        crate::note_batch_reads(reads, &out);
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
        crate::note_stream_reads(streams, &out);
        Ok(out)
    }

    async fn log_read_streams_within(
        &self,
        streams: &[(String, String)],
        budget: u64,
    ) -> Result<Vec<Vec<LogEntryMeta>>> {
        use futures::TryStreamExt as _;
        let mut out: Vec<Vec<LogEntryMeta>> = Vec::new();
        if streams.is_empty() {
            return Ok(out);
        }
        let (nss, keys): (Vec<&str>, Vec<&str>) = streams
            .iter()
            .map(|(n, k)| (n.as_str(), k.as_str()))
            .unzip();
        let c = client!(self);
        // One statement, consumed as a row stream in request order: the
        // rows past the budget are discarded as they arrive, never kept.
        let params: [&(
             dyn deadpool_postgres::tokio_postgres::types::ToSql + Sync
         ); 2] = [&nss, &keys];
        let rows = c
            .query_raw(
                "SELECT r.i, l.seq, COALESCE(l.ts_ms, l.ts*1000), l.capture_id,
                        l.content_hash, l.val
                 FROM unnest($1::text[], $2::text[]) WITH ORDINALITY AS r(ns, key, i)
                 JOIN df_log l ON l.ns = r.ns AND l.key = r.key
                 ORDER BY r.i, l.seq",
                params,
            )
            .await?;
        futures::pin_mut!(rows);
        let mut within = crate::StreamBudget::new(budget);
        let mut stopped = false;
        while let Some(r) = rows.try_next().await? {
            let i = r.get::<_, i64>(0) as usize - 1;
            let m = LogEntryMeta {
                seq: r.get::<_, i64>(1) as u64,
                stored_at_ms: r.get(2),
                capture_id: r.get(3),
                content_hash: r.get(4),
                value: r.get(5),
            };
            if !within.take(i, &m) {
                // Every stream before `i` is whole (an empty one has no
                // rows); `i` and the rest are not answered.
                out.truncate(i);
                out.resize_with(i, Vec::new);
                stopped = true;
                break;
            }
            while out.len() <= i {
                out.push(Vec::new());
            }
            out[i].push(m);
        }
        if !stopped {
            out.resize_with(streams.len(), Vec::new);
        }
        crate::note_stream_reads(&streams[..out.len()], &out);
        Ok(out)
    }

    async fn apply_batch(&self, batch: &AtomicBatch) -> Result<BatchOutcome> {
        batch.validate()?;
        let fault = self.batch_fault.lock().expect("batch fault").take();
        if fault == Some(BatchFault::BeforeTransaction) {
            anyhow::bail!("injected failure before the transaction");
        }
        // `AfterWrites(k)`: a failure before the k-th write statement.
        let mut writes = 0usize;
        let mut write = |what: &str| -> Result<()> {
            if fault == Some(BatchFault::AfterWrites(writes)) {
                anyhow::bail!(
                    "injected failure after {writes} write statements, before {what}"
                );
            }
            writes += 1;
            Ok(())
        };
        let mut c = client!(self);
        // The store lock first, then every read. Read committed: each
        // statement after the lock sees every transaction committed before
        // the lock was granted, and every writer of guarded data holds the
        // same lock until it commits, so a precondition that holds when read
        // holds until this commit. (A repeatable-read or serializable
        // snapshot would be taken by the lock statement itself, before the
        // wait, and miss writers that committed meanwhile.) Dropped without
        // a commit, the transaction rolls back. Every group of items runs as
        // array statements, chunked (`set_chunks`), all in this transaction.
        let tx = c.transaction().await?;
        tx.execute(LOCK_LOG_SQL, &[&LOG_ALLOCATION_LOCK]).await?;
        let now = now_secs();

        // Preconditions: each stream's head and each key's value read once.
        if let Some(why) =
            pg_preconditions_conflict(&tx, &batch.preconditions, now).await?
        {
            return Ok(BatchOutcome::Conflict { why });
        }

        // Log entries. A repeated identity collapsed onto its first position
        // (`canonical_log_ops`); each remaining identity is checked against
        // the store at once: a stored one is a conflict (`Absent`), or, with
        // identical bytes, answered with its original sequence
        // (`AbsentOrIdentical`; other bytes fail closed). Only new
        // identities are inserted, numbered in first-input order.
        let canonical = crate::canonical_log_ops(&batch.ops)?;
        let unique: Vec<LogWrite<'_>> = batch
            .ops
            .iter()
            .enumerate()
            .filter(|(i, _)| canonical[*i] == *i)
            .filter_map(|(i, op)| {
                op.log_write().map(|(ns, key, cid, value, idem)| {
                    (i, ns, key, cid, value, idem)
                })
            })
            .collect();
        let mut seqs: Vec<Option<u64>> = vec![None; batch.ops.len()];
        for (j, (seq, stored)) in pg_stored_captures(&tx, &unique).await? {
            let (i, _, key, capture_id, value, idempotent) = unique[j];
            if !idempotent {
                return Ok(BatchOutcome::Conflict {
                    why: format!("{capture_id} already exists in {key}"),
                });
            }
            if stored != value {
                return Err(crate::LogError::CaptureIdentityConflict {
                    capture_id: capture_id.to_string(),
                }
                .into());
            }
            seqs[i] = Some(seq);
        }
        let appends: Vec<(usize, &str, &str, &str, &[u8])> = unique
            .iter()
            .filter(|u| seqs[u.0].is_none())
            .map(|u| (u.0, u.1, u.2, u.3, u.4))
            .collect();
        if !appends.is_empty() {
            // One contiguous block, in op order: every df_log insert holds
            // the store lock, so nothing else allocates meanwhile.
            write("the sequence reservation")?;
            let base: i64 = tx
                .query_one(
                    "WITH b AS (SELECT nextval(pg_get_serial_sequence('df_log', 'seq')) AS base)
                     SELECT base, setval(pg_get_serial_sequence('df_log', 'seq'), base + $1 - 1)
                     FROM b",
                    &[&(appends.len() as i64)],
                )
                .await?
                .get(0);
            for (rank, (i, ..)) in appends.iter().enumerate() {
                seqs[*i] = Some((base + rank as i64) as u64);
            }
            let ms = now_ms();
            for chunk in set_chunks(
                appends
                    .iter()
                    .map(|a| a.1.len() + a.2.len() + a.3.len() + a.4.len()),
            ) {
                let rows = &appends[chunk];
                write("a log insert")?;
                let inserted = tx
                    .execute(
                        "INSERT INTO df_log(seq, ns, key, val, ts, ts_ms, capture_id, content_hash)
                         SELECT r.seq, r.ns, r.key, r.val, $5, $6, r.capture_id, r.content_hash
                         FROM unnest($1::bigint[], $2::text[], $3::text[], $4::bytea[],
                                     $7::text[], $8::text[])
                              AS r(seq, ns, key, val, capture_id, content_hash)
                         ON CONFLICT (ns, key, capture_id) WHERE capture_id IS NOT NULL
                         DO NOTHING",
                        &[
                            &rows.iter().map(|a| seqs[a.0].expect("reserved") as i64).collect::<Vec<_>>(),
                            &rows.iter().map(|a| a.1).collect::<Vec<_>>(),
                            &rows.iter().map(|a| a.2).collect::<Vec<_>>(),
                            &rows.iter().map(|a| a.4).collect::<Vec<_>>(),
                            &now,
                            &ms,
                            &rows.iter().map(|a| a.3).collect::<Vec<_>>(),
                            &rows.iter().map(|a| content_digest(a.4)).collect::<Vec<_>>(),
                        ],
                    )
                    .await?;
                if inserted as usize != rows.len() {
                    return Ok(BatchOutcome::Conflict {
                        why: format!(
                            "{} of {} capture ids already exist or repeat",
                            rows.len() - inserted as usize,
                            rows.len()
                        ),
                    });
                }
            }
            // Each touched stream's meta: created at its head, or advanced.
            let mut streams: Vec<(&str, &str)> = Vec::new();
            let mut known = std::collections::HashSet::new();
            for a in &appends {
                if known.insert((a.1, a.2)) {
                    streams.push((a.1, a.2));
                }
            }
            for chunk in
                set_chunks(streams.iter().map(|s| s.0.len() + s.1.len()))
            {
                let rows = &streams[chunk];
                write("a log meta update")?;
                tx.execute(
                    "INSERT INTO df_log_meta(ns, key, min_valid_from_seq, head_seq)
                     SELECT r.ns, r.key, 0,
                            COALESCE((SELECT MAX(l.seq) FROM df_log l
                                      WHERE l.ns = r.ns AND l.key = r.key), 0)
                     FROM unnest($1::text[], $2::text[]) AS r(ns, key)
                     ON CONFLICT (ns, key) DO UPDATE
                       SET head_seq = GREATEST(df_log_meta.head_seq, EXCLUDED.head_seq)",
                    &[
                        &rows.iter().map(|s| s.0).collect::<Vec<_>>(),
                        &rows.iter().map(|s| s.1).collect::<Vec<_>>(),
                    ],
                )
                .await?;
            }
        }

        // A collapsed repeat: its first position's sequence.
        for (i, c) in canonical.iter().enumerate() {
            if *c != i {
                seqs[i] = seqs[*c];
            }
        }

        // Key-value writes, folded in op order: the value each key ends
        // with (a max sees the batch's own earlier writes, as one op at a
        // time did); one upsert per key.
        let mut state: std::collections::HashMap<
            (&str, &str),
            Option<Vec<u8>>,
        > = std::collections::HashMap::new();
        let mut unread: Vec<(usize, &str, &str)> = Vec::new();
        let mut asked = std::collections::HashSet::new();
        let mut written = std::collections::HashSet::new();
        for (i, op) in batch.ops.iter().enumerate() {
            match op {
                BatchOp::KvPut { ns, key, .. } => {
                    written.insert((ns.as_str(), key.as_str()));
                }
                BatchOp::KvMaxSeq { ns, key, .. }
                | BatchOp::KvMax { ns, key, .. } => {
                    let k = (ns.as_str(), key.as_str());
                    if !written.contains(&k) && asked.insert(k) {
                        unread.push((i, k.0, k.1));
                    }
                    written.insert(k);
                }
                _ => {}
            }
        }
        let read = pg_kv_live_many(&tx, &unread, now).await?;
        for ((_, ns, key), (_, v)) in unread.iter().zip(read) {
            state.insert((ns, key), v);
        }
        let mut order: Vec<(&str, &str)> = Vec::new();
        for op in &batch.ops {
            let (k, v) = match op {
                BatchOp::KvPut { ns, key, value } => (
                    (ns.as_str(), key.as_str()),
                    match value {
                        BatchValue::Bytes(b) => b.clone(),
                        BatchValue::SeqTemplate { template, of } => {
                            render_seq(template, seqs[*of].expect("validated"))
                        }
                    },
                ),
                BatchOp::KvMaxSeq { ns, key, of } => {
                    let k = (ns.as_str(), key.as_str());
                    let current = state.get(&k).cloned().flatten();
                    (
                        k,
                        max_seq_value(
                            current.as_deref(),
                            seqs[*of].expect("validated"),
                        )?,
                    )
                }
                BatchOp::KvMax { ns, key, value } => {
                    let k = (ns.as_str(), key.as_str());
                    let current = state.get(&k).cloned().flatten();
                    (k, max_seq_value(current.as_deref(), *value)?)
                }
                _ => continue,
            };
            if !order.contains(&k) {
                order.push(k);
            }
            state.insert(k, Some(v));
        }
        let finals: Vec<(&str, &str, Vec<u8>)> = order
            .iter()
            .map(|k| (k.0, k.1, state[k].clone().expect("written")))
            .collect();
        for chunk in
            set_chunks(finals.iter().map(|f| f.0.len() + f.1.len() + f.2.len()))
        {
            let rows = &finals[chunk];
            write("a key-value upsert")?;
            tx.execute(
                "INSERT INTO df_kv(ns, key, val, updated_at, expires_at)
                 SELECT r.ns, r.key, r.val, $4, NULL
                 FROM unnest($1::text[], $2::text[], $3::bytea[]) AS r(ns, key, val)
                 ON CONFLICT(ns, key) DO UPDATE SET
                   val=EXCLUDED.val, updated_at=EXCLUDED.updated_at, expires_at=NULL",
                &[
                    &rows.iter().map(|f| f.0).collect::<Vec<_>>(),
                    &rows.iter().map(|f| f.1).collect::<Vec<_>>(),
                    &rows.iter().map(|f| f.2.as_slice()).collect::<Vec<_>>(),
                    &now_secs(),
                ],
            )
            .await?;
        }

        // The commit marker, the batch's last write (validated last).
        if let Some(BatchOp::CommitMarker { batch_id, digest }) =
            batch.ops.last()
        {
            let i = batch.ops.len() - 1;
            let result =
                crate::encode_batch_result(&batch.ops[..i], &seqs[..i])?;
            write("the commit marker")?;
            tx.execute(
                "INSERT INTO df_batch_commit(batch_id, digest, result, committed_at)
                 VALUES($1, $2, $3, $4)",
                &[batch_id, digest, &result, &now_ms()],
            )
            .await?;
        }
        if fault == Some(BatchFault::AfterWrites(writes)) {
            anyhow::bail!("injected failure after every write statement");
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
        crate::check_guarded_write(ns, key, value.len())?;
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
        crate::check_guarded_write(ns, key, value.len())?;
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
        crate::check_guarded_write(ns, key, value.len())?;
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
        crate::check_guarded_write(ns, key, value.len())?;
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

    /// The commit-marker lookup waits for the store lock (an in-flight
    /// batch holding it cannot still be committing when the answer comes),
    /// and the sweep releases the lock between chunks: a guarded writer
    /// queued behind the sweep runs before the sweep's next chunk.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires a live PostgreSQL (DELTAFORGE_IT_PG_DSN)"]
    async fn pg_marker_lookup_and_sweep_follow_the_store_lock() {
        use deadpool_postgres::tokio_postgres;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;
        let dsn = std::env::var("DELTAFORGE_IT_PG_DSN")
            .expect("DELTAFORGE_IT_PG_DSN must be set to run this test");
        let be = PostgresStorageBackend::connect(&dsn).await.unwrap();
        let run = now_ms();
        // Clear older markers, then 3 chunks' worth of expired ones.
        tokio::time::sleep(Duration::from_millis(5)).await;
        crate::sweep_batch_markers_within(
            be.as_ref(),
            now_ms(),
            usize::MAX,
            Duration::from_secs(600),
        )
        .await
        .unwrap();
        let (admin, conn) = tokio_postgres::connect(&dsn, NoTls).await.unwrap();
        tokio::spawn(conn);
        admin
            .execute(
                "INSERT INTO df_batch_commit(batch_id, digest, result, committed_at)
                 SELECT 'it' || $1::text || '-' || g, decode('00', 'hex'), decode('01', 'hex'), $2
                 FROM generate_series(1, 3000) g",
                &[&run.to_string(), &(run - 1)],
            )
            .await
            .unwrap();

        admin.batch_execute("BEGIN").await.unwrap();
        admin
            .execute(LOCK_LOG_SQL, &[&LOG_ALLOCATION_LOCK])
            .await
            .unwrap();
        let lookup = {
            let be = be.clone();
            let id = format!("it{run}-1");
            tokio::spawn(async move { be.batch_commit_get(&id).await })
        };
        lock_waiters(&admin, 1).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            !lookup.is_finished(),
            "the marker lookup ran without the store lock"
        );

        // The sweep queues next, then a guarded writer behind it.
        let order = Arc::new(AtomicUsize::new(0));
        let sweep = {
            let (be, order) = (be.clone(), order.clone());
            tokio::spawn(async move {
                let r = crate::sweep_batch_markers_within(
                    be.as_ref(),
                    run,
                    64,
                    Duration::from_secs(60),
                )
                .await;
                (r, order.fetch_add(1, Ordering::SeqCst))
            })
        };
        lock_waiters(&admin, 2).await;
        let writer = {
            let (be, order) = (be.clone(), order.clone());
            tokio::spawn(async move {
                let r = be
                    .kv_put(
                        "schemas.v1.index",
                        &format!("it{run}_sweepw"),
                        b"v",
                    )
                    .await;
                (r, order.fetch_add(1, Ordering::SeqCst))
            })
        };
        lock_waiters(&admin, 3).await;
        admin.batch_execute("COMMIT").await.unwrap();

        assert!(lookup.await.unwrap().unwrap().is_some());
        let (written, writer_rank) = writer.await.unwrap();
        written.unwrap();
        let (swept, sweep_rank) = sweep.await.unwrap();
        let swept = swept.unwrap();
        assert_eq!(swept.deleted, 3000);
        assert_eq!(swept.chunks, 3);
        assert!(
            writer_rank < sweep_rank,
            "the sweep held the store lock across chunks"
        );
    }

    /// The later-writer regression against PostgreSQL: a lost acknowledgement
    /// resolves as committed from the marker although a valid writer changed
    /// one of the batch's keys before the resolution.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires a live PostgreSQL (DELTAFORGE_IT_PG_DSN)"]
    async fn pg_a_lost_acknowledgement_resolves_from_the_marker() {
        let dsn = std::env::var("DELTAFORGE_IT_PG_DSN")
            .expect("DELTAFORGE_IT_PG_DSN must be set to run this test");
        let pg = PostgresStorageBackend::connect(&dsn).await.unwrap();
        let fb = crate::adapters::test_util::FaultBackend::wrap(pg.clone());
        let ns = format!("schemas.v1.it{}_ack", now_ms());
        let batch = AtomicBatch {
            preconditions: Vec::new(),
            ops: vec![
                BatchOp::LogAppendIfAbsent {
                    ns: ns.clone(),
                    key: "s".into(),
                    capture_id: "c1".into(),
                    value: b"entry".to_vec(),
                },
                BatchOp::KvPut {
                    ns: ns.clone(),
                    key: "k".into(),
                    value: crate::BatchValue::Bytes(b"batch".to_vec()),
                },
            ],
        };
        pg.set_batch_fault(Some(BatchFault::AfterCommit));
        *fb.write_after_batch_error.lock().unwrap() =
            Some((ns.clone(), "k".into(), b"later".to_vec()));
        let out = crate::commit_atomic(&fb, &batch, 3).await.unwrap();
        let seq = pg
            .log_find_capture(&ns, "s", "c1")
            .await
            .unwrap()
            .unwrap()
            .0;
        assert_eq!(out, crate::CommitResult::Committed(vec![Some(seq), None]));
        assert_eq!(
            pg.kv_get(&ns, "k").await.unwrap().as_deref(),
            Some(&b"later"[..])
        );
    }

    /// A batch of several chunks per group, failed before each of its write
    /// statements in turn: nothing of it is visible from another
    /// connection and its commit marker is absent; then it commits whole.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires a live PostgreSQL (DELTAFORGE_IT_PG_DSN)"]
    async fn pg_a_failure_after_any_chunked_statement_leaves_nothing() {
        let dsn = std::env::var("DELTAFORGE_IT_PG_DSN")
            .expect("DELTAFORGE_IT_PG_DSN must be set to run this test");
        let pg = PostgresStorageBackend::connect(&dsn).await.unwrap();
        let ns = format!("schemas.v1.it{}_chunks", now_ms());
        let n = SET_CHUNK_ITEMS + 100;
        let batch = |id: &str| {
            let mut ops: Vec<BatchOp> = (0..n)
                .map(|i| BatchOp::LogAppendIfAbsent {
                    ns: ns.clone(),
                    key: format!("s{}", i % 3),
                    capture_id: format!("c{i}"),
                    value: format!("v{i}").into_bytes(),
                })
                .collect();
            ops.extend((0..n).map(|i| BatchOp::KvPut {
                ns: ns.clone(),
                key: format!("k{i}"),
                value: crate::BatchValue::Bytes(b"x".to_vec()),
            }));
            ops.push(BatchOp::CommitMarker {
                batch_id: id.into(),
                digest: b"d".to_vec(),
            });
            AtomicBatch {
                preconditions: Vec::new(),
                ops,
            }
        };
        // The reservation, two insert chunks, the meta upsert, two
        // key-value chunks and the marker.
        let statements = 7;
        let other = PostgresStorageBackend::connect(&dsn).await.unwrap();
        for k in 0..=statements {
            let id = format!("fail-{k}");
            pg.set_batch_fault(Some(BatchFault::AfterWrites(k)));
            assert!(pg.apply_batch(&batch(&id)).await.is_err(), "{k}");
            for key in ["s0", "s1", "s2"] {
                assert!(
                    other.log_list(&ns, key).await.unwrap().is_empty(),
                    "{k}"
                );
            }
            assert_eq!(other.kv_get(&ns, "k0").await.unwrap(), None, "{k}");
            assert_eq!(
                other.kv_get(&ns, &format!("k{}", n - 1)).await.unwrap(),
                None
            );
            assert!(
                other.batch_commit_get(&id).await.unwrap().is_none(),
                "{k}"
            );
        }
        pg.set_batch_fault(Some(BatchFault::AfterWrites(statements + 1)));
        match pg.apply_batch(&batch("whole")).await.unwrap() {
            BatchOutcome::Committed { .. } => {}
            other => panic!("{other:?}"),
        }
        assert!(other.batch_commit_get("whole").await.unwrap().is_some());
        let total: usize = entries(&other, &ns).await;
        assert_eq!(total, n);
    }

    async fn entries(be: &PostgresStorageBackend, ns: &str) -> usize {
        let mut total = 0;
        for key in ["s0", "s1", "s2"] {
            total += be.log_list(ns, key).await.unwrap().len();
        }
        total
    }

    /// A batch that rolls back after reserving its sequences leaves a gap
    /// that is never reused: a batch committed meanwhile takes later
    /// sequences, and the retry later ones still, so committed sequences
    /// are unique and increase in commit order.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires a live PostgreSQL (DELTAFORGE_IT_PG_DSN)"]
    async fn pg_a_rolled_back_reservation_is_never_reused() {
        let dsn = std::env::var("DELTAFORGE_IT_PG_DSN")
            .expect("DELTAFORGE_IT_PG_DSN must be set to run this test");
        let pg = PostgresStorageBackend::connect(&dsn).await.unwrap();
        let ns = format!("schemas.v1.it{}_gap", now_ms());
        let batch = |tag: &str| AtomicBatch {
            preconditions: Vec::new(),
            ops: (0..5)
                .map(|i| BatchOp::LogAppendIfAbsent {
                    ns: ns.clone(),
                    key: tag.into(),
                    capture_id: format!("c{i}"),
                    value: b"v".to_vec(),
                })
                .collect(),
        };
        let seqs = |o: BatchOutcome| match o {
            BatchOutcome::Committed { seqs } => {
                seqs.into_iter().map(Option::unwrap).collect::<Vec<u64>>()
            }
            other => panic!("{other:?}"),
        };
        let before = seqs(pg.apply_batch(&batch("before")).await.unwrap());
        // A reserves its block, then fails before its first insert.
        pg.set_batch_fault(Some(BatchFault::AfterWrites(1)));
        assert!(pg.apply_batch(&batch("a")).await.is_err());
        let b = seqs(pg.apply_batch(&batch("b")).await.unwrap());
        let a = seqs(pg.apply_batch(&batch("a")).await.unwrap());
        let last = *before.last().unwrap();
        assert!(b[0] > last + 5, "B reused A's gap: {before:?} {b:?}");
        assert!(a[0] > *b.last().unwrap(), "{b:?} {a:?}");
        for s in [&before, &b, &a] {
            assert!(s.windows(2).all(|w| w[1] == w[0] + 1), "{s:?}");
        }
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
    /// A chunk closes at `SET_CHUNK_ITEMS` items or once its payload reaches
    /// `SET_CHUNK_BYTES` (the item is added first); request order is kept
    /// and every item is in exactly one chunk.
    #[test]
    fn set_chunks_follow_the_packing_rule() {
        assert!(set_chunks(std::iter::empty()).is_empty());
        assert_eq!(set_chunks([1, 1, 1].into_iter()), vec![0..3]);
        let n = 2 * SET_CHUNK_ITEMS + 1;
        assert_eq!(
            set_chunks(std::iter::repeat_n(1, n)),
            vec![
                0..SET_CHUNK_ITEMS,
                SET_CHUNK_ITEMS..2 * SET_CHUNK_ITEMS,
                2 * SET_CHUNK_ITEMS..n
            ]
        );
        let half = SET_CHUNK_BYTES / 2;
        assert_eq!(
            set_chunks([half, half, 1, SET_CHUNK_BYTES * 3, 1].into_iter()),
            vec![0..2, 2..4, 4..5]
        );
    }

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
