//! MySQL consistent snapshot engine.
//!
//! Initial load with a single consistent anchor established under a brief global
//! read lock (snapshot-anchor hardening):
//!
//! 1. Hold `FLUSH TABLES WITH READ LOCK` on a dedicated non-pooled connection
//!    while opening a bounded pool of `min(max_parallel_tables, pending)` worker
//!    connections, each `START TRANSACTION WITH CONSISTENT SNAPSHOT` under
//!    `REPEATABLE READ`, and capturing the binlog position + GTID set. Because no
//!    transaction can commit while the lock is held, every worker's read view
//!    equals the captured position - one shared anchor, no seam loss. The whole
//!    setup is bounded by `snapshot.lock_timeout_secs`; the lock is guaranteed to
//!    release by dropping the lock connection on any error/panic/timeout
//!    (`UNLOCK TABLES` is only the success path).
//! 2. Each worker reads its bucket of tables sequentially under its single
//!    consistent snapshot and commits once when the bucket is done, so the lock
//!    window and connection count are bounded by the worker count, not the table
//!    count. Single-integer-PK tables use PK-range chunking; others full-scan.
//! 3. Completed tables are recorded in the checkpoint store so a crash resumes
//!    at the table level rather than restarting from scratch.
//! 4. Returns the `MySqlCheckpoint` captured under the lock - pass this to
//!    `prepare_client` as the replication start position so streaming picks up
//!    exactly where the snapshot left off.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use checkpoints::CheckpointStore;
use deltaforge_config::SnapshotCfg;
use deltaforge_core::{
    CheckpointMeta, Event, EventId, Op, SourceError, SourceInfo, SourceItem,
    SourcePosition,
};
use metrics::counter;
use mysql_async::{Conn, Opts, Pool, Row, Value, prelude::Queryable};
use std::collections::HashMap;
use tokio::time::timeout;

use super::mysql_identity::mysql_identity_cell;
use crate::durable_checkpoint::{CursorKind, SnapshotCursor};
use crate::snapshot_event_id::{OwnedIdentityValue, snapshot_row_event_id};
use crate::snapshot_frontier::{
    SnapshotAggregator, SnapshotPublisher, TableResume,
};
use crate::snapshot_generation::PersistedLineage;
use scopeguard;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use super::mysql_health as health;
use super::{MySqlCheckpoint, MySqlSchemaLoader};

const POSITION_GUARD_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(30);

// ============================================================================
// Snapshot progress (persisted for crash resume)
// ============================================================================
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MysqlSnapshotProgress {
    /// Serialized `MySqlCheckpoint` captured before any rows were read.
    pub start_position: String,
    /// Tables that have been fully snapshotted ("db.table").
    pub done_tables: Vec<String>,
    /// True once every table is complete.
    pub finished: bool,
}

impl MysqlSnapshotProgress {
    fn table_done(&self, db: &str, table: &str) -> bool {
        self.done_tables.contains(&fqn(db, table))
    }

    fn mark_done(&mut self, db: &str, table: &str) {
        let key = fqn(db, table);
        if !self.done_tables.contains(&key) {
            self.done_tables.push(key);
        }
    }
}

fn fqn(db: &str, table: &str) -> String {
    format!("{db}.{table}")
}

pub fn progress_key(source_id: &str) -> String {
    format!("mysql_snapshot_progress:{source_id}")
}

/// Load persisted snapshot progress, failing closed on an unreadable or corrupt
/// record. A genuinely absent record is a fresh start (`default`). Silently
/// treating a store error or corrupt bytes as "no progress" would restart the
/// whole snapshot - re-exporting rows and, for a finished snapshot, discarding
/// the saved CDC start position.
pub(crate) async fn load_snapshot_progress(
    store: &dyn CheckpointStore,
    source_id: &str,
) -> Result<MysqlSnapshotProgress> {
    match store.get_raw(&progress_key(source_id)).await.context(
        "reading mysql snapshot progress; refusing to restart the snapshot on \
         an unreadable progress record",
    )? {
        Some(bytes) => serde_json::from_slice(&bytes).context(
            "parsing mysql snapshot progress; refusing to restart the snapshot \
             on a corrupt progress record",
        ),
        None => Ok(MysqlSnapshotProgress::default()),
    }
}

// ============================================================================
// Entry point
// ============================================================================

pub struct SnapshotCtx<'a> {
    pub dsn: &'a str,
    pub source_id: &'a str,
    pub pipeline: &'a str,
    pub tenant: &'a str,
    pub cfg: &'a SnapshotCfg,
    pub schema_loader: &'a MySqlSchemaLoader,
    pub chkpt_store: Arc<dyn CheckpointStore>,
    pub tx: mpsc::Sender<SourceItem>,
    pub cancel: CancellationToken,
    /// Durable snapshot generation (allocated before any row).
    pub generation: u64,
    /// Frozen source lineage for snapshot identity.
    pub lineage: PersistedLineage,
    /// `db.table` → resolved identity column names (identity order).
    pub identity_map: HashMap<String, Vec<String>>,
}

/// Spawns a background task that polls SHOW BINARY LOGS every POSITION_GUARD_INTERVAL seconds.
/// On confirmed purge: sets abort_reason and fires the CancellationToken.
/// Transient errors (connect failures, empty results) are retried - never abort.
fn spawn_binlog_position_guard(
    dsn: crate::credentials::ProtectedDsn,
    captured_file: String,
    cancel: CancellationToken,
    abort_reason: Arc<Mutex<Option<String>>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(POSITION_GUARD_INTERVAL);
        interval.tick().await;

        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = interval.tick() => {}
            }

            let mut conn = match Pool::new(dsn.expose()).get_conn().await {
                Ok(c) => c,
                Err(e) => {
                    warn!(error = %e, "binlog guard: connect error, retrying");
                    continue;
                }
            };

            let rows: Vec<Row> = match conn.query("SHOW BINARY LOGS").await {
                Ok(r) => r,
                Err(e) => {
                    warn!(error = %e, "binlog guard: SHOW BINARY LOGS failed, retrying");
                    continue;
                }
            };

            let available: Vec<String> = rows
                .into_iter()
                .filter_map(|mut r: Row| r.take::<String, usize>(0))
                .collect();

            if !health::binlog_file_still_present(&available, &captured_file) {
                let msg = format!(
                    "binlog file '{}' purged during snapshot \
                     (available: [{}]). \
                     Increase binlog_expire_logs_seconds or reduce \
                     max_parallel_tables and restart.",
                    captured_file,
                    available.join(", ")
                );
                warn!("{}", msg);
                *abort_reason.lock().unwrap() = Some(msg);
                cancel.cancel();
                return;
            }

            debug!(file = %captured_file, "binlog guard: ok");
        }
    })
}

/// Run a consistent snapshot of `tables`.
///
/// Returns a `MySqlCheckpoint` captured after all worker transactions open -
/// InnoDB guarantees every visible row was committed at or before this position.
/// The caller must persist this as the binlog checkpoint so streaming resumes
/// with no gaps.
pub async fn run_snapshot(
    ctx: &SnapshotCtx<'_>,
    tables: &[(String, String)],
) -> Result<MySqlCheckpoint> {
    let t0 = Instant::now();

    // Load previous progress for crash resume. Fail closed on an unreadable or
    // corrupt progress record; only a genuinely absent record is a fresh start.
    let mut progress =
        load_snapshot_progress(ctx.chkpt_store.as_ref(), ctx.source_id).await?;

    if progress.finished {
        info!(
            ctx.source_id,
            "mysql snapshot already complete, returning saved position"
        );
        return serde_json::from_str(&progress.start_position)
            .context("parse saved snapshot position");
    }

    // preflight validation and risk estimation/guessing. Hard errors fail
    // closed with a typed error: a missing RELOAD privilege (managed MySQL that
    // cannot FLUSH TABLES WITH READ LOCK) surfaces as Permission; a bad server
    // config (non-GTID, non-InnoDB, non-ROW binlog) as Incompatible. There is no
    // silent fallback to an unsafe per-worker-snapshot anchor.
    let preflight =
        health::run_preflight(ctx.dsn, tables, ctx.cfg.max_parallel_tables)
            .await
            .context("snapshot preflight")?;
    preflight.emit(ctx.source_id, tables.len());
    if !preflight.hard_errors.is_empty() {
        let details = preflight.hard_errors.join("; ");
        let se = if preflight.permission_error {
            SourceError::Permission {
                details: details.into(),
            }
        } else {
            SourceError::Incompatible {
                details: details.into(),
            }
        };
        return Err(anyhow::Error::new(se));
    }

    // step 1: establish the consistent anchor under a brief global read lock.
    // Only pending (not-yet-done) tables need workers; skip completed ones on
    // resume so the lock window and connection count stay bounded.
    let pending: Vec<(String, String)> = tables
        .iter()
        .filter(|(db, t)| !progress.table_done(db, t))
        .cloned()
        .collect();
    let num_workers =
        ctx.cfg.max_parallel_tables.min(pending.len().max(1)).max(1);

    let (worker_conns, position) = acquire_locked_anchor(
        ctx.dsn,
        num_workers,
        Duration::from_secs(ctx.cfg.lock_timeout_secs.max(1)),
    )
    .await
    .context("acquire locked snapshot anchor")?;

    progress.start_position = serde_json::to_string(&position)
        .context("serialize binlog position")?;
    save_progress(&ctx.chkpt_store, ctx.source_id, &progress).await;

    // spawn background position guard
    let abort_reason: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let guard_cancel = ctx.cancel.child_token();
    let _guard_stop = scopeguard::guard((), |_| guard_cancel.cancel());
    let _position_guard = spawn_binlog_position_guard(
        crate::credentials::ProtectedDsn::from(ctx.dsn),
        position.file.clone(),
        guard_cancel.clone(),
        abort_reason.clone(),
    );

    info!(
        source_id = %ctx.source_id,
        file = %position.file,
        pos = position.pos,
        tables = tables.len(),
        "mysql snapshot started"
    );

    // Build the ordered-aggregation owner. Its source vector is restored from
    // the source's own progress (done_tables + finished) - NEVER from any sink's
    // HEAD, so the source is not fast-forwarded by how far one sink is durable.
    // Resolve every table's cursor kind up front (including already-done tables,
    // which enter the vector complete at their kind's max) so the vector's key
    // set and cursor kinds are fixed from the first batch.
    let mut kinds: HashMap<String, CursorKind> = HashMap::new();
    for (db, table) in tables {
        let loaded = ctx
            .schema_loader
            .load_schema(db, table)
            .await
            .with_context(|| {
                format!("load schema (cursor kind) for {}", fqn(db, table))
            })?;
        kinds.insert(fqn(db, table), mysql_cursor_kind(&loaded.schema));
    }
    let all_tables: Vec<(String, CursorKind)> = tables
        .iter()
        .map(|(db, table)| {
            let k = fqn(db, table);
            let kind = kinds.get(&k).copied().unwrap_or(CursorKind::Unsigned);
            (k, kind)
        })
        .collect();
    let resume: Vec<(String, TableResume)> = all_tables
        .iter()
        .map(|(t, kind)| {
            let done = progress.finished || progress.done_tables.contains(t);
            (t.clone(), TableResume { kind: *kind, done })
        })
        .collect();
    let snapshot_checkpoint = CheckpointMeta::from_vec(
        serde_json::to_vec(&position)
            .context("serialize snapshot checkpoint")?,
    );
    let publisher = Arc::new(SnapshotPublisher::new(
        SnapshotAggregator::from_source_progress(
            ctx.generation,
            ctx.lineage.clone(),
            snapshot_checkpoint,
            &resume,
        ),
        ctx.tx.clone(),
    ));

    // step 2: bounded worker pool. Each worker owns one consistent-snapshot
    // connection and reads a bucket of pending tables sequentially under that
    // single snapshot, committing once when the bucket is done. This bounds the
    // connection count (and the earlier lock window) by num_workers, not by the
    // table count. Table completion (durable progress + publisher boundary) is
    // recorded as each table finishes, preserving table-level crash resume.
    let mut buckets: Vec<Vec<(String, String)>> =
        (0..num_workers).map(|_| Vec::new()).collect();
    for (i, tbl) in pending.iter().enumerate() {
        buckets[i % num_workers].push(tbl.clone());
    }

    let progress_shared = Arc::new(tokio::sync::Mutex::new(progress));
    let mut handles = Vec::new();

    for (bucket, worker_conn) in buckets.into_iter().zip(worker_conns) {
        let publisher = Arc::clone(&publisher);
        let progress_shared = Arc::clone(&progress_shared);
        let chkpt_store = ctx.chkpt_store.clone();
        let schema_loader = ctx.schema_loader.clone();
        let cancel = ctx.cancel.clone();
        let identity_map = ctx.identity_map.clone();
        let kinds = kinds.clone();
        let source_id = ctx.source_id.to_string();
        let pipeline = ctx.pipeline.to_string();
        let tenant = ctx.tenant.to_string();
        let cfg = ctx.cfg.clone();
        let generation = ctx.generation;
        let lineage = ctx.lineage.clone();

        let handle = tokio::spawn(async move {
            let mut conn = worker_conn;
            let mut failed: Vec<String> = Vec::new();
            for (db, table) in bucket {
                if cancel.is_cancelled() {
                    break;
                }
                let table_key = fqn(&db, &table);
                let identity =
                    identity_map.get(&table_key).cloned().unwrap_or_default();
                let cursor_kind = kinds
                    .get(&table_key)
                    .copied()
                    .unwrap_or(CursorKind::Unsigned);
                let worker = TableWorker {
                    db: db.clone(),
                    table: table.clone(),
                    conn,
                    source_id: source_id.clone(),
                    pipeline: pipeline.clone(),
                    tenant: tenant.clone(),
                    cfg: cfg.clone(),
                    table_key: table_key.clone(),
                    cursor_kind,
                    publisher: Arc::clone(&publisher),
                    schema_loader: schema_loader.clone(),
                    cancel: cancel.clone(),
                    generation,
                    lineage: lineage.clone(),
                    identity,
                    schema: None,
                };
                match worker.run().await {
                    Ok((rows, conn_back)) => {
                        conn = conn_back;
                        {
                            let mut p = progress_shared.lock().await;
                            p.mark_done(&db, &table);
                            save_progress(&chkpt_store, &source_id, &p).await;
                        }
                        // Table-complete boundary (and the completed boundary on
                        // the final table). Delivered and durably acked by the
                        // coordinator even with no trailing data rows.
                        if publisher.complete_table(&table_key).await.is_err() {
                            failed
                                .push(format!("{table_key} (channel closed)"));
                            return failed;
                        }
                        info!(table = %table_key, rows, "table snapshot complete");
                    }
                    Err(e) => {
                        error!(table = %table_key, error = %e, "table snapshot failed");
                        failed.push(table_key);
                        // conn consumed by the failed run; dropping it rolls back
                        // this bucket's transaction.
                        return failed;
                    }
                }
            }
            // commit the bucket's single consistent-snapshot transaction once.
            let _ = conn.query_drop("COMMIT").await;
            failed
        });
        handles.push(handle);
    }

    // step 3: collect worker-pool results.
    let mut failed: Vec<String> = Vec::new();
    for handle in handles {
        match handle.await {
            Ok(mut f) => failed.append(&mut f),
            Err(e) => {
                error!(error = %e, "snapshot worker pool task panicked");
                failed.push("worker pool task panicked".into());
            }
        }
    }

    // guard error takes priority over generic worker failures
    if let Some(reason) = abort_reason.lock().unwrap().take() {
        anyhow::bail!("snapshot aborted: {}", reason);
    }

    if !failed.is_empty() {
        anyhow::bail!(
            "mysql snapshot failed for tables: {}",
            failed.join(", ")
        );
    }

    // final synchronous position check before marking complete
    // this closes the 30s polling race window.
    health::verify_binlog_position(ctx.dsn, &position.file)
        .await
        .context("post-snapshot binlog position verification")?;

    // only write finished=true after the position is confirmed still valid.
    // "finished" means "safe to hand off to CDC", not just "rows emitted".
    {
        let mut p = progress_shared.lock().await;
        p.finished = true;
        save_progress(&ctx.chkpt_store, ctx.source_id, &p).await;
    }

    info!(
        source_id = %ctx.source_id,
        elapsed_secs = t0.elapsed().as_secs(),
        file = %position.file,
        pos = position.pos,
        "mysql snapshot finished"
    );

    guard_cancel.cancel();
    Ok(position)
}

// ============================================================================
// Helpers
// ============================================================================

/// Establish the consistent snapshot anchor under a brief global read lock.
///
/// Holds `FLUSH TABLES WITH READ LOCK` on a dedicated **non-pooled** connection
/// while it opens `num_workers` `REPEATABLE READ` consistent-snapshot worker
/// connections and captures the binlog position + GTID set. Because no
/// transaction can commit while the lock is held, every worker's read view
/// equals the captured position - one shared anchor with no snapshot->CDC seam.
///
/// The whole lock-held setup is bounded by `timeout_dur`. Lock release is
/// guaranteed: the lock connection is a non-pooled `Conn`, so on any error,
/// panic, or timeout it is dropped, closing its session and releasing the lock
/// server-side. `UNLOCK TABLES` is only the normal success path.
async fn acquire_locked_anchor(
    dsn: &str,
    num_workers: usize,
    timeout_dur: Duration,
) -> Result<(Vec<Conn>, MySqlCheckpoint)> {
    let opts = Opts::from_url(dsn).context("parse mysql dsn")?;

    // Dedicated, non-pooled lock connection: dropping it releases FTWRL.
    let mut lock_conn = Conn::new(opts.clone())
        .await
        .context("connect lock connection")?;

    // Bound how long FTWRL may wait on in-flight statements/metadata locks.
    let lock_wait = timeout_dur.as_secs().max(1);
    lock_conn
        .query_drop(format!("SET SESSION lock_wait_timeout = {lock_wait}"))
        .await
        .ok();

    // All lock-held setup runs under one deadline. On timeout the future is
    // dropped, which drops lock_conn and releases the lock.
    let setup = async {
        lock_conn
            .query_drop("FLUSH TABLES WITH READ LOCK")
            .await
            .context("FLUSH TABLES WITH READ LOCK")?;

        let mut workers = Vec::with_capacity(num_workers);
        for _ in 0..num_workers {
            let mut c = Conn::new(opts.clone())
                .await
                .context("connect snapshot worker")?;
            c.query_drop(
                "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
            )
            .await
            .context("set repeatable read")?;
            c.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
                .await
                .context("start consistent snapshot")?;
            workers.push(c);
        }

        // Position captured while the lock is still held -> matches every
        // worker's read view exactly.
        let position = capture_binlog_position(&mut lock_conn).await?;
        Result::<(Vec<Conn>, MySqlCheckpoint)>::Ok((workers, position))
    };

    let (workers, position) = match timeout(timeout_dur, setup).await {
        Ok(inner) => inner?, // inner Err drops lock_conn -> lock released
        Err(_) => {
            // timeout: `setup` future dropped -> lock_conn dropped -> released.
            return Err(anyhow!(
                "timed out establishing snapshot read lock within {timeout_dur:?}; \
                 the source may be under long-running statements. Increase \
                 snapshot.lock_timeout_secs or retry when the source is quieter."
            ));
        }
    };

    // Success: explicit unlock, then drop the lock connection.
    lock_conn.query_drop("UNLOCK TABLES").await.ok();
    drop(lock_conn);

    Ok((workers, position))
}

/// Capture the current binlog position. Supports MySQL 8.4+ (`BINARY LOG STATUS`)
/// and older (`MASTER STATUS`).
async fn capture_binlog_position(
    conn: &mut mysql_async::Conn,
) -> Result<MySqlCheckpoint> {
    let row: Option<Row> =
        match conn.query_first("SHOW BINARY LOG STATUS").await {
            Ok(r) => r,
            Err(_) => conn
                .query_first("SHOW MASTER STATUS")
                .await
                .context("SHOW MASTER STATUS - is binary logging enabled?")?,
        };

    let mut row = row
        .context("no binlog position returned - is binary logging enabled?")?;

    let file: String = row.take(0).context("binlog file")?;
    let pos: u32 = row.take(1).context("binlog pos")?;

    let gtid_row: Option<Row> = conn
        .query_first("SELECT @@GLOBAL.gtid_executed")
        .await
        .ok()
        .flatten();
    let gtid_set = gtid_row
        .and_then(|mut r| r.take::<Option<String>, _>(0).flatten())
        .filter(|g| !g.is_empty());

    Ok(MySqlCheckpoint {
        file,
        pos: pos as u64,
        gtid_set,
    })
}

async fn save_progress(
    store: &Arc<dyn CheckpointStore>,
    source_id: &str,
    progress: &MysqlSnapshotProgress,
) {
    if let Ok(bytes) = serde_json::to_vec(progress) {
        let _ = store.put_raw(&progress_key(source_id), &bytes).await;
    }
}

// ============================================================================
// Table worker
// ============================================================================

struct TableWorker {
    db: String,
    table: String,
    /// Already-started consistent-snapshot transaction connection.
    conn: mysql_async::Conn,
    source_id: String,
    pipeline: String,
    tenant: String,
    cfg: SnapshotCfg,
    /// Fully-qualified `db.table`, the aggregator's key for this table.
    table_key: String,
    /// Cursor kind for this table (matches the aggregator's frontier kind).
    cursor_kind: CursorKind,
    /// Shared aggregation owner: serializes boundary-advance + channel send.
    publisher: Arc<SnapshotPublisher>,
    schema_loader: MySqlSchemaLoader,
    cancel: CancellationToken,
    /// Durable snapshot generation for stable-id derivation.
    generation: u64,
    /// Frozen source lineage.
    lineage: PersistedLineage,
    /// Resolved identity column names (identity order).
    identity: Vec<String>,
    /// Loaded schema, populated in `run` before any scan.
    schema: Option<super::MySqlTableSchema>,
}

impl TableWorker {
    /// Read one table under this worker's consistent snapshot and return the row
    /// count plus the connection, so the pool can reuse the same un-committed
    /// snapshot transaction for the next table in its bucket. The caller commits
    /// once when the bucket is done.
    async fn run(mut self) -> Result<(u64, mysql_async::Conn)> {
        info!(pipeline=%self.pipeline, source_id=%self.source_id, db=%self.db, table=%self.table, "snapshot worker starting");
        let table_fqn = fqn(&self.db, &self.table);
        let t0 = Instant::now();

        let loaded = self
            .schema_loader
            .load_schema(&self.db, &self.table)
            .await
            .with_context(|| format!("load schema for {table_fqn}"))?;
        // Retain the schema for schema-directed native identity extraction.
        self.schema = Some(loaded.schema.clone());

        let pk = loaded.schema.primary_key.clone();
        let rows_sent = if pk.len() == 1
            && is_integer_pk(loaded.schema.column(pk[0].as_str()))
        {
            self.by_pk(&pk[0]).await?
        } else {
            debug!(
                table = %table_fqn,
                pk_len = pk.len(),
                "using full scan"
            );
            self.full_scan().await?
        };

        // NB: no COMMIT here - the pool worker commits the bucket's single
        // consistent-snapshot transaction once, after its last table.
        counter!(
            "deltaforge_snapshot_rows_total",
            "pipeline" => self.pipeline.clone(),
            "table" => table_fqn.clone()
        )
        .increment(rows_sent);

        info!(
            table = %table_fqn,
            rows_sent,
            elapsed_ms = t0.elapsed().as_millis(),
            "table done"
        );

        Ok((rows_sent, self.conn))
    }

    // ── PK-range chunking ─────────────────────────────────────────────────────

    async fn by_pk(&mut self, pk_col: &str) -> Result<u64> {
        // Signed and unsigned integer PKs use disjoint cursor domains and must
        // never be mixed: an unsigned PK above i64::MAX would corrupt a signed
        // (i64) scan. The kind was resolved from the column type up front.
        match self.cursor_kind {
            CursorKind::Unsigned => self.by_pk_unsigned(pk_col).await,
            _ => self.by_pk_signed(pk_col).await,
        }
    }

    /// Build snapshot events for a scanned chunk's rows.
    fn rows_to_events(&self, rows: Vec<Row>) -> Result<Vec<Event>> {
        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            // Derive identity from NATIVE values before the lossy JSON
            // conversion, then build the event.
            let id = self.provisional_id(&row)?;
            let json = row_to_json(row)?;
            events.push(self.make_event(json, id));
        }
        Ok(events)
    }

    async fn by_pk_signed(&mut self, pk_col: &str) -> Result<u64> {
        let table_fqn = fqn(&self.db, &self.table);

        let bounds_row: Option<Row> = self
            .conn
            .query_first(format!(
                "SELECT MIN(`{pk_col}`), MAX(`{pk_col}`) FROM `{}`.`{}`",
                self.db, self.table
            ))
            .await
            .with_context(|| format!("PK bounds for {table_fqn}"))?;

        let (min_pk, max_pk) = match bounds_row {
            None => return Ok(0),
            Some(mut r) => {
                match (r.take::<Option<i64>, _>(0), r.take::<Option<i64>, _>(1))
                {
                    (Some(Some(a)), Some(Some(b))) => (a, b),
                    _ => {
                        debug!(table = %table_fqn, "empty table");
                        return Ok(0);
                    }
                }
            }
        };

        let chunk = self.cfg.chunk_size as i64;
        let mut cursor = min_pk;
        let mut total_sent = 0u64;
        // Half-open frontier cursor, starting at the kind's minimum so the first
        // chunk covers everything below min_pk (vacuously durable). Signed PKs
        // keep their true (possibly negative) order via the typed cursor.
        let mut published_end: SnapshotCursor = self.cursor_kind.min();

        while cursor <= max_pk {
            if self.cancel.is_cancelled() {
                anyhow::bail!("snapshot cancelled");
            }

            let end = cursor + chunk;
            let rows: Vec<Row> = self
                .conn
                .query(format!(
                    "SELECT * FROM `{}`.`{}` WHERE `{pk_col}` >= {cursor} AND `{pk_col}` < {end}",
                    self.db, self.table
                ))
                .await
                .with_context(|| {
                    format!("PK range [{cursor},{end}) for {table_fqn}")
                })?;

            total_sent += rows.len() as u64;
            let events = self.rows_to_events(rows)?;
            let chunk_end = mysql_signed_cursor(end);
            self.publisher
                .publish_chunk(
                    &self.table_key,
                    published_end,
                    chunk_end,
                    events,
                )
                .await
                .map_err(|_| anyhow!("event channel closed"))?;
            published_end = chunk_end;
            cursor = end;
        }

        Ok(total_sent)
    }

    /// Unsigned integer PK scan. Bounds are read and interpolated as full-range
    /// `u64` (never cast through `i64`), and chunk arithmetic is overflow-safe so
    /// a range crossing `i64::MAX` and ending at `u64::MAX` scans correctly.
    async fn by_pk_unsigned(&mut self, pk_col: &str) -> Result<u64> {
        let table_fqn = fqn(&self.db, &self.table);

        let bounds_row: Option<Row> = self
            .conn
            .query_first(format!(
                "SELECT MIN(`{pk_col}`), MAX(`{pk_col}`) FROM `{}`.`{}`",
                self.db, self.table
            ))
            .await
            .with_context(|| format!("PK bounds for {table_fqn}"))?;

        let (min_pk, max_pk) = match bounds_row {
            None => return Ok(0),
            Some(mut r) => {
                match (r.take::<Option<u64>, _>(0), r.take::<Option<u64>, _>(1))
                {
                    (Some(Some(a)), Some(Some(b))) => (a, b),
                    _ => {
                        debug!(table = %table_fqn, "empty table");
                        return Ok(0);
                    }
                }
            }
        };

        let chunk = self.cfg.chunk_size as u64;
        let mut cursor = min_pk;
        let mut total_sent = 0u64;
        let mut published_end: SnapshotCursor = self.cursor_kind.min();

        loop {
            if self.cancel.is_cancelled() {
                anyhow::bail!("snapshot cancelled");
            }

            let cb = unsigned_chunk_bounds(cursor, chunk, max_pk);
            let op = if cb.inclusive { "<=" } else { "<" };
            let rows: Vec<Row> = self
                .conn
                .query(format!(
                    "SELECT * FROM `{}`.`{}` WHERE `{pk_col}` >= {cursor} AND `{pk_col}` {op} {}",
                    self.db, self.table, cb.upper
                ))
                .await
                .with_context(|| {
                    format!("PK range [{cursor},{}] for {table_fqn}", cb.upper)
                })?;

            total_sent += rows.len() as u64;
            let events = self.rows_to_events(rows)?;
            let chunk_end = SnapshotCursor::Unsigned(cb.frontier_end);
            self.publisher
                .publish_chunk(
                    &self.table_key,
                    published_end,
                    chunk_end,
                    events,
                )
                .await
                .map_err(|_| anyhow!("event channel closed"))?;
            published_end = chunk_end;

            if cb.inclusive {
                break;
            }
            cursor = cb.next;
        }

        Ok(total_sent)
    }

    // ── Full scan (composite/non-integer/no PK) ───────────────────────────────

    async fn full_scan(&mut self) -> Result<u64> {
        let table_fqn = fqn(&self.db, &self.table);

        if self.cancel.is_cancelled() {
            anyhow::bail!("snapshot cancelled");
        }

        let rows: Vec<Row> = self
            .conn
            .query(format!("SELECT * FROM `{}`.`{}`", self.db, self.table))
            .await
            .with_context(|| format!("full scan of {table_fqn}"))?;

        let n = rows.len() as u64;
        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let id = self.provisional_id(&row)?;
            let json = row_to_json(row)?;
            events.push(self.make_event(json, id));
        }

        // No PK cursor: the whole table is one chunk [min, n) over an unsigned
        // row-count cursor. The frontier advances to n and the boundary lands on
        // the last row; resume for a full-scan table is table-level (rescan),
        // matching the durable progress model.
        let start = self.cursor_kind.min();
        let end = SnapshotCursor::Unsigned(n.max(1));
        self.publisher
            .publish_chunk(&self.table_key, start, end, events)
            .await
            .map_err(|_| anyhow!("event channel closed"))?;

        Ok(n)
    }

    /// Compute the provisional snapshot [`EventId`] from the row's **native**
    /// identity values (schema-directed) and the allocated generation.
    fn provisional_id(&self, row: &Row) -> Result<EventId> {
        let schema = self
            .schema
            .as_ref()
            .ok_or_else(|| anyhow!("schema not loaded before scan"))?;
        let mut values: Vec<OwnedIdentityValue> =
            Vec::with_capacity(self.identity.len());
        for name in &self.identity {
            let col = schema
                .column(name)
                .ok_or_else(|| anyhow!("identity column {name:?} missing"))?;
            let idx = row
                .columns_ref()
                .iter()
                .position(|c| c.name_str() == name.as_str())
                .ok_or_else(|| {
                    anyhow!("identity column {name:?} not present in row")
                })?;
            let val = row.as_ref(idx).cloned().unwrap_or(Value::NULL);
            let cell = mysql_identity_cell(col, &val)
                .map_err(|e| anyhow!("{name}: {e}"))?;
            values.push(OwnedIdentityValue {
                name: name.clone(),
                cell,
            });
        }
        Ok(snapshot_row_event_id(
            &self.lineage.as_source_lineage(),
            self.generation,
            &self.db,
            &self.table,
            &values,
        ))
    }

    fn make_event(&self, after: serde_json::Value, event_id: EventId) -> Event {
        let size = after.to_string().len();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let mut ev = Event::new_row(
            event_id,
            SourceInfo {
                version: concat!("deltaforge-", env!("CARGO_PKG_VERSION"))
                    .to_string(),
                connector: "mysql".to_string(),
                name: self.pipeline.clone(),
                ts_ms: now,
                db: self.db.clone(),
                schema: None,
                table: self.table.clone(),
                snapshot: Some("true".to_string()),
                position: SourcePosition {
                    snapshot_generation: Some(self.generation),
                    ..Default::default()
                },
            },
            Op::Read,
            None,
            Some(after),
            now,
            size,
        );
        ev.tenant_id = Some(self.tenant.clone());
        ev
    }
}

// ============================================================================
// Type helpers
// ============================================================================

fn is_integer_pk(col: Option<&super::MySqlColumn>) -> bool {
    match col {
        Some(c) => matches!(
            c.data_type.to_lowercase().as_str(),
            "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint"
        ),
        None => false,
    }
}

/// The snapshot cursor kind for a table: signed vs unsigned integer PK-range
/// scan, or an unsigned row-count cursor for the full-scan fallback. Must match
/// the worker's scan decision so the aggregator's kind agrees with the cursors it
/// receives.
fn mysql_cursor_kind(schema: &super::MySqlTableSchema) -> CursorKind {
    let pk = &schema.primary_key;
    if pk.len() == 1 {
        if let Some(col) = schema.column(&pk[0]) {
            if is_integer_pk(Some(col)) {
                return if col.is_unsigned() {
                    CursorKind::Unsigned
                } else {
                    CursorKind::Signed
                };
            }
        }
    }
    // Full scan: a monotone row-count cursor.
    CursorKind::Unsigned
}

/// Build a signed cursor from a scan value (MySQL signed PK path).
fn mysql_signed_cursor(v: i64) -> SnapshotCursor {
    SnapshotCursor::Signed(v)
}

/// One unsigned PK chunk's bounds, overflow-safe near `u64::MAX`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct UnsignedChunk {
    /// SQL predicate upper bound for this chunk.
    upper: u64,
    /// Whether the upper bound is inclusive (the final chunk, so the row at
    /// `max` - possibly `u64::MAX` - is not dropped by an exclusive `<`).
    inclusive: bool,
    /// Half-open frontier end reported to the aggregator. `max + 1`, saturating
    /// at `u64::MAX` (where completion, not the cursor, marks the max row done).
    frontier_end: u64,
    /// The next cursor to scan from (only meaningful when not the final chunk).
    next: u64,
}

/// Compute the next unsigned PK chunk `[cursor, ..)` given `chunk` size and the
/// table `max`. Uses checked arithmetic so a cursor near `u64::MAX` never
/// overflows; the final chunk is inclusive of `max`.
fn unsigned_chunk_bounds(cursor: u64, chunk: u64, max: u64) -> UnsignedChunk {
    match cursor.checked_add(chunk) {
        Some(end) if end <= max => UnsignedChunk {
            upper: end,
            inclusive: false,
            frontier_end: end,
            next: end,
        },
        // Final chunk: covers [cursor, max] inclusively.
        _ => UnsignedChunk {
            upper: max,
            inclusive: true,
            frontier_end: max.saturating_add(1),
            next: max,
        },
    }
}

fn row_to_json(mut row: Row) -> Result<serde_json::Value> {
    let columns = row.columns_ref().to_vec();
    let mut map = serde_json::Map::with_capacity(columns.len());
    for (i, col) in columns.iter().enumerate() {
        let name = col.name_str().into_owned();
        let val: Value = row.take(i).unwrap_or(Value::NULL);
        map.insert(name, value_to_json(val));
    }
    Ok(serde_json::Value::Object(map))
}

fn value_to_json(val: Value) -> serde_json::Value {
    match val {
        Value::NULL => serde_json::Value::Null,
        Value::Int(n) => serde_json::json!(n),
        Value::UInt(n) => serde_json::json!(n),
        Value::Float(f) => serde_json::json!(f),
        Value::Double(d) => serde_json::json!(d),
        Value::Bytes(b) => match String::from_utf8(b.clone()) {
            Ok(s) => serde_json::Value::String(s),
            Err(_) => serde_json::json!({ "_base64": base64_encode(&b) }),
        },
        Value::Date(y, mo, d, h, min, s, us) => serde_json::Value::String(
            format!("{y:04}-{mo:02}-{d:02}T{h:02}:{min:02}:{s:02}.{us:06}"),
        ),
        Value::Time(neg, days, h, min, s, us) => {
            let sign = if neg { "-" } else { "" };
            let total_h = days as u64 * 24 + h as u64;
            serde_json::Value::String(format!(
                "{sign}{total_h:02}:{min:02}:{s:02}.{us:06}"
            ))
        }
    }
}

fn base64_encode(bytes: &[u8]) -> String {
    const ALPHA: &[u8] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(ALPHA[((n >> 18) & 0x3f) as usize] as char);
        out.push(ALPHA[((n >> 12) & 0x3f) as usize] as char);
        out.push(if chunk.len() > 1 {
            ALPHA[((n >> 6) & 0x3f) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHA[(n & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    out
}

#[cfg(test)]
mod progress_load_tests {
    //! R3-C7: snapshot-progress load fails closed on an unreadable/corrupt
    //! record; only a genuinely absent record is a fresh start.
    use super::*;
    use checkpoints::{CheckpointError, CheckpointResult, MemCheckpointStore};

    #[tokio::test]
    async fn absent_progress_is_fresh_default() {
        let store = MemCheckpointStore::new().unwrap();
        let p = load_snapshot_progress(&store, "s1").await.unwrap();
        assert!(
            !p.finished,
            "absent record must be a fresh (unfinished) start"
        );
    }

    #[tokio::test]
    async fn valid_progress_round_trips() {
        let store = MemCheckpointStore::new().unwrap();
        let saved = MysqlSnapshotProgress {
            start_position: "pos".into(),
            done_tables: vec!["db.t".into()],
            finished: true,
        };
        store
            .put_raw(&progress_key("s1"), &serde_json::to_vec(&saved).unwrap())
            .await
            .unwrap();
        let got = load_snapshot_progress(&store, "s1").await.unwrap();
        assert!(got.finished);
        assert_eq!(got.done_tables, vec!["db.t".to_string()]);
    }

    #[tokio::test]
    async fn corrupt_progress_fails_closed() {
        let store = MemCheckpointStore::new().unwrap();
        store
            .put_raw(&progress_key("s1"), b"not json at all")
            .await
            .unwrap();
        assert!(
            load_snapshot_progress(&store, "s1").await.is_err(),
            "a corrupt progress record must fail closed, not restart the snapshot"
        );
    }

    #[tokio::test]
    async fn store_error_fails_closed() {
        #[derive(Debug)]
        struct ErrStore;
        #[async_trait::async_trait]
        impl CheckpointStore for ErrStore {
            async fn get_raw(
                &self,
                _k: &str,
            ) -> CheckpointResult<Option<Vec<u8>>> {
                Err(CheckpointError::Data("injected read failure".into()))
            }
            async fn put_raw(
                &self,
                _k: &str,
                _b: &[u8],
            ) -> CheckpointResult<()> {
                Ok(())
            }
            async fn delete(&self, _k: &str) -> CheckpointResult<bool> {
                Ok(false)
            }
            async fn list(&self) -> CheckpointResult<Vec<String>> {
                Ok(vec![])
            }
        }
        assert!(
            load_snapshot_progress(&ErrStore, "s1").await.is_err(),
            "a store read error must fail closed, not restart the snapshot"
        );
    }
}

#[cfg(test)]
mod cursor_tests {
    use super::*;

    #[test]
    fn unsigned_chunk_bounds_normal_range() {
        let c = unsigned_chunk_bounds(0, 100, 1000);
        assert_eq!(
            c,
            UnsignedChunk {
                upper: 100,
                inclusive: false,
                frontier_end: 100,
                next: 100,
            }
        );
    }

    #[test]
    fn unsigned_chunk_bounds_final_chunk_is_inclusive() {
        // cursor + chunk overshoots max: final inclusive chunk covers [900, 950].
        let c = unsigned_chunk_bounds(900, 100, 950);
        assert!(c.inclusive);
        assert_eq!(c.upper, 950);
        assert_eq!(c.frontier_end, 951);
    }

    #[test]
    fn unsigned_chunk_bounds_crossing_i64_max() {
        // A cursor just below i64::MAX with a chunk that crosses it stays correct
        // (no i64 overflow / sign flip).
        let below = i64::MAX as u64 - 10;
        let c = unsigned_chunk_bounds(below, 100, u64::MAX);
        // below + 100 > u64... no: below+100 < u64::MAX, and <= max -> normal.
        assert!(!c.inclusive);
        assert_eq!(c.upper, below + 100);
        assert_eq!(c.frontier_end, below + 100);
        // The crossed value is above i64::MAX.
        assert!(c.upper > i64::MAX as u64);
    }

    #[test]
    fn unsigned_chunk_bounds_ending_at_u64_max_no_overflow() {
        // cursor near u64::MAX: checked_add overflows -> final inclusive chunk to
        // u64::MAX, frontier_end saturates (no panic, no wraparound).
        let cursor = u64::MAX - 5;
        let c = unsigned_chunk_bounds(cursor, 1000, u64::MAX);
        assert!(c.inclusive);
        assert_eq!(c.upper, u64::MAX);
        assert_eq!(c.frontier_end, u64::MAX, "saturates, never wraps to 0");
    }

    #[test]
    fn unsigned_chunk_bounds_exact_max_boundary() {
        // cursor + chunk == max exactly: normal exclusive chunk to max, then a
        // later call from max produces the final inclusive [max, max].
        let c = unsigned_chunk_bounds(0, 500, 500);
        assert!(!c.inclusive);
        assert_eq!(c.upper, 500);
        let last = unsigned_chunk_bounds(500, 500, 500);
        assert!(last.inclusive);
        assert_eq!(last.upper, 500);
        assert_eq!(last.frontier_end, 501);
    }
}

#[cfg(test)]
mod live_anchor_tests {
    //! Live seam regression test for the FTWRL-bracketed anchor.
    //!
    //! Proves the fix: under concurrent writes, every worker connection opened by
    //! `acquire_locked_anchor` shares ONE consistent view (identical row counts)
    //! and the captured position is a clean cut (post-anchor commits are absent
    //! from the snapshot, GTID captured). The pre-fix per-worker-independent
    //! snapshot could not guarantee this - see the anchor verification report.
    //!
    //! Gated on `MYSQL_IT_DSN` (a live MySQL 8 with GTID + a user holding RELOAD).
    use super::*;
    use mysql_async::prelude::Queryable;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[tokio::test]
    #[ignore = "requires a live MySQL (set MYSQL_IT_DSN)"]
    async fn ftwrl_anchor_consistent_under_concurrent_writes() {
        let Ok(dsn) = std::env::var("MYSQL_IT_DSN") else {
            eprintln!("skip: MYSQL_IT_DSN unset");
            return;
        };
        let opts = Opts::from_url(&dsn).unwrap();
        let mut admin = Conn::new(opts.clone()).await.unwrap();
        admin
            .query_drop("DROP TABLE IF EXISTS anchor_seam")
            .await
            .unwrap();
        admin
            .query_drop(
                "CREATE TABLE anchor_seam(\
                 id BIGINT PRIMARY KEY AUTO_INCREMENT, v INT) ENGINE=InnoDB",
            )
            .await
            .unwrap();
        admin
            .query_drop("INSERT INTO anchor_seam(v) VALUES (0),(0),(0),(0),(0)")
            .await
            .unwrap();

        // Concurrent writer hammering commits across the anchor setup window.
        let writer_dsn = dsn.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = stop.clone();
        let writer = tokio::spawn(async move {
            let mut c = Conn::new(Opts::from_url(&writer_dsn).unwrap())
                .await
                .unwrap();
            let mut n: u64 = 0;
            while !stop2.load(Ordering::Relaxed) {
                let _ =
                    c.query_drop("INSERT INTO anchor_seam(v) VALUES (1)").await;
                n += 1;
            }
            n
        });
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Establish the anchor while writes are in flight.
        let (mut workers, position) =
            acquire_locked_anchor(&dsn, 4, Duration::from_secs(10))
                .await
                .expect("acquire anchor");

        // All worker snapshots must agree - one shared consistent view.
        let mut counts = Vec::new();
        for w in workers.iter_mut() {
            let c: Option<u64> = w
                .query_first("SELECT COUNT(*) FROM anchor_seam")
                .await
                .unwrap();
            counts.push(c.unwrap_or(0));
        }
        assert!(
            counts.windows(2).all(|w| w[0] == w[1]),
            "worker snapshots diverged under concurrent writes: {counts:?} - \
             anchor is not a single consistent view"
        );
        let anchor_count = counts[0];
        assert!(anchor_count >= 5, "baseline rows missing from anchor");

        // A commit AFTER the anchor must NOT appear in the snapshot: clean cut.
        admin
            .query_drop("INSERT INTO anchor_seam(v) VALUES (99)")
            .await
            .unwrap();
        let after: Option<u64> = workers[0]
            .query_first("SELECT COUNT(*) FROM anchor_seam")
            .await
            .unwrap();
        assert_eq!(
            after.unwrap_or(0),
            anchor_count,
            "post-anchor commit leaked into the snapshot view"
        );

        // GTID must be captured at the anchor (mandatory for resume).
        assert!(
            !position.gtid_set.as_deref().unwrap_or("").is_empty(),
            "GTID set not captured at the anchor"
        );

        stop.store(true, Ordering::Relaxed);
        let _ = writer.await;
        for mut w in workers {
            w.query_drop("ROLLBACK").await.ok();
        }
        admin
            .query_drop("DROP TABLE IF EXISTS anchor_seam")
            .await
            .ok();
    }

    /// Success path releases the global lock: after `acquire_locked_anchor`
    /// returns, an external write is not blocked.
    #[tokio::test]
    #[ignore = "requires a live MySQL (set MYSQL_IT_DSN)"]
    async fn acquire_releases_lock_on_success() {
        let Ok(dsn) = std::env::var("MYSQL_IT_DSN") else {
            eprintln!("skip: MYSQL_IT_DSN unset");
            return;
        };
        let opts = Opts::from_url(&dsn).unwrap();
        let mut admin = Conn::new(opts.clone()).await.unwrap();
        admin
            .query_drop("DROP TABLE IF EXISTS lock_rel")
            .await
            .unwrap();
        admin
            .query_drop(
                "CREATE TABLE lock_rel(id INT PRIMARY KEY) ENGINE=InnoDB",
            )
            .await
            .unwrap();

        let (workers, _pos) =
            acquire_locked_anchor(&dsn, 2, Duration::from_secs(10))
                .await
                .expect("acquire");
        drop(workers);

        // An external write must complete promptly - the lock is gone.
        let ins = tokio::time::timeout(
            Duration::from_secs(3),
            admin.query_drop("INSERT INTO lock_rel(id) VALUES (1)"),
        )
        .await;
        assert!(
            ins.is_ok(),
            "write blocked after anchor success - lock leaked"
        );
        admin.query_drop("DROP TABLE IF EXISTS lock_rel").await.ok();
    }

    /// Error/panic safety: the lock is held on a non-pooled connection, so simply
    /// dropping that connection releases FTWRL server-side (this is the mechanism
    /// that guarantees release on any early return, `?`, or panic).
    #[tokio::test]
    #[ignore = "requires a live MySQL (set MYSQL_IT_DSN)"]
    async fn dropped_lock_conn_releases_ftwrl() {
        let Ok(dsn) = std::env::var("MYSQL_IT_DSN") else {
            eprintln!("skip: MYSQL_IT_DSN unset");
            return;
        };
        let opts = Opts::from_url(&dsn).unwrap();
        let mut lock_conn = Conn::new(opts.clone()).await.unwrap();
        lock_conn
            .query_drop("FLUSH TABLES WITH READ LOCK")
            .await
            .unwrap();

        // While held, an external write blocks.
        let mut writer = Conn::new(opts.clone()).await.unwrap();
        let blocked = tokio::time::timeout(
            Duration::from_secs(2),
            writer.query_drop("CREATE TABLE lock_drop_probe(id INT)"),
        )
        .await;
        assert!(blocked.is_err(), "write was not blocked while FTWRL held");

        // Dropping the lock connection (no UNLOCK) must release the lock.
        drop(lock_conn);

        let unblocked = tokio::time::timeout(
            Duration::from_secs(5),
            writer.query_drop("CREATE TABLE lock_drop_probe(id INT)"),
        )
        .await;
        assert!(
            unblocked.is_ok(),
            "lock not released after dropping the lock connection"
        );
        writer
            .query_drop("DROP TABLE IF EXISTS lock_drop_probe")
            .await
            .ok();
    }

    /// Timeout path releases: when FTWRL cannot be acquired within the budget
    /// (a conflicting table lock is held), `acquire_locked_anchor` fails and
    /// leaves no lingering lock.
    #[tokio::test]
    #[ignore = "requires a live MySQL (set MYSQL_IT_DSN)"]
    async fn acquire_times_out_and_releases_when_blocked() {
        let Ok(dsn) = std::env::var("MYSQL_IT_DSN") else {
            eprintln!("skip: MYSQL_IT_DSN unset");
            return;
        };
        let opts = Opts::from_url(&dsn).unwrap();
        let mut admin = Conn::new(opts.clone()).await.unwrap();
        admin
            .query_drop("DROP TABLE IF EXISTS lock_to")
            .await
            .unwrap();
        admin
            .query_drop(
                "CREATE TABLE lock_to(id INT PRIMARY KEY) ENGINE=InnoDB",
            )
            .await
            .unwrap();

        // Blocker: hold a WRITE table lock so FTWRL must wait.
        let mut blocker = Conn::new(opts.clone()).await.unwrap();
        blocker
            .query_drop("LOCK TABLES lock_to WRITE")
            .await
            .unwrap();

        let started = Instant::now();
        let res = acquire_locked_anchor(&dsn, 2, Duration::from_secs(2)).await;
        assert!(res.is_err(), "expected timeout while FTWRL was blocked");
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "acquire did not honor the timeout budget"
        );

        // Release the blocker; the anchor must now succeed (no lingering lock).
        blocker.query_drop("UNLOCK TABLES").await.unwrap();
        drop(blocker);
        let (workers, _pos) =
            acquire_locked_anchor(&dsn, 2, Duration::from_secs(10))
                .await
                .expect("acquire after blocker released");
        drop(workers);
        admin.query_drop("DROP TABLE IF EXISTS lock_to").await.ok();
    }

    /// Managed-MySQL refusal: a user without the global RELOAD privilege cannot
    /// FLUSH TABLES WITH READ LOCK, so preflight fails closed with a permission
    /// error (no silent fallback to an unsafe anchor).
    #[tokio::test]
    #[ignore = "requires a live MySQL (set MYSQL_IT_DSN)"]
    async fn managed_mysql_without_reload_fails_closed() {
        let Ok(dsn) = std::env::var("MYSQL_IT_DSN") else {
            eprintln!("skip: MYSQL_IT_DSN unset");
            return;
        };
        let opts = Opts::from_url(&dsn).unwrap();
        let mut admin = Conn::new(opts.clone()).await.unwrap();
        // Fresh table + a limited user that intentionally lacks RELOAD.
        admin
            .query_drop("DROP TABLE IF EXISTS ltd_t")
            .await
            .unwrap();
        admin
            .query_drop("CREATE TABLE ltd_t(id INT PRIMARY KEY) ENGINE=InnoDB")
            .await
            .unwrap();
        admin.query_drop("DROP USER IF EXISTS 'ltd'@'%'").await.ok();
        admin
            .query_drop("CREATE USER 'ltd'@'%' IDENTIFIED BY 'ltd'")
            .await
            .unwrap();
        admin
            .query_drop(
                "GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* \
                 TO 'ltd'@'%'",
            )
            .await
            .unwrap();
        admin.query_drop("FLUSH PRIVILEGES").await.ok();

        // Build the limited-user DSN by swapping credentials.
        let base = dsn.split('@').nth(1).unwrap();
        let ltd_dsn = format!("mysql://ltd:ltd@{base}");

        let report = health::run_preflight(
            &ltd_dsn,
            &[("seam".to_string(), "ltd_t".to_string())],
            4,
        )
        .await
        .expect("preflight runs");
        assert!(
            report.permission_error,
            "expected a permission (RELOAD) hard error for a no-RELOAD user"
        );
        assert!(
            report.hard_errors.iter().any(|e| e.contains("RELOAD")),
            "hard errors should name RELOAD: {:?}",
            report.hard_errors
        );

        // A full-privilege user (root) passes the RELOAD gate.
        let root_report = health::run_preflight(
            &dsn,
            &[("seam".to_string(), "ltd_t".to_string())],
            4,
        )
        .await
        .expect("preflight runs");
        assert!(
            !root_report.permission_error,
            "root should hold RELOAD: {:?}",
            root_report.hard_errors
        );

        admin.query_drop("DROP USER IF EXISTS 'ltd'@'%'").await.ok();
        admin.query_drop("DROP TABLE IF EXISTS ltd_t").await.ok();
    }
}
