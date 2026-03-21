//! MySQL consistent snapshot engine.
//!
//! Lock-free initial load using InnoDB's consistent read mechanism:
//!
//! 1. Open all table worker connections and start each with
//!    `START TRANSACTION WITH CONSISTENT SNAPSHOT`. All workers see the
//!    same consistent DB state without any global lock.
//! 2. Capture the current binlog position *after* all workers have started -
//!    InnoDB guarantees every visible row was committed at or before this
//!    position, so CDC streaming from here has no gaps.
//! 3. Tables with a single integer PK use PK-range chunking. All others fall
//!    back to a full scan.
//! 4. Completed tables are recorded in the checkpoint store so a crash resumes
//!    at the table level rather than restarting from scratch.
//! 5. Returns a `MySqlCheckpoint` captured in step 2 - pass this to
//!    `prepare_client` as the replication start position so streaming picks up
//!    exactly where the snapshot left off.

use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{Context, Result};
use checkpoints::CheckpointStore;
use deltaforge_config::SnapshotCfg;
use deltaforge_core::{Event, Op, SourceInfo, SourcePosition};
use mysql_async::{Pool, Row, Value, prelude::Queryable};
use scopeguard;
use serde::{Deserialize, Serialize};
use tokio::sync::{Semaphore, mpsc};
use tokio_util::sync::CancellationToken;
use metrics::counter;
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
    pub tx: mpsc::Sender<Event>,
    pub cancel: CancellationToken,
}

/// Spawns a background task that polls SHOW BINARY LOGS every POSITION_GUARD_INTERVAL seconds.
/// On confirmed purge: sets abort_reason and fires the CancellationToken.
/// Transient errors (connect failures, empty results) are retried - never abort.
fn spawn_binlog_position_guard(
    dsn: String,
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

            let mut conn = match Pool::new(dsn.as_str()).get_conn().await {
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

    // load previous progress for crash resume
    let mut progress: MysqlSnapshotProgress = ctx
        .chkpt_store
        .get_raw(&progress_key(ctx.source_id))
        .await
        .ok()
        .flatten()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or_default();

    if progress.finished {
        info!(
            ctx.source_id,
            "mysql snapshot already complete, returning saved position"
        );
        return serde_json::from_str(&progress.start_position)
            .context("parse saved snapshot position");
    }

    // preflight validation and risk estimation/guessing
    let preflight =
        health::run_preflight(ctx.dsn, tables, ctx.cfg.max_parallel_tables)
            .await
            .context("snapshot preflight")?;
    preflight.emit_and_check(ctx.source_id, tables.len())?;

    // step 1: start worker transactions
    let mut worker_conns: Vec<mysql_async::Conn> =
        Vec::with_capacity(tables.len());
    for _ in 0..tables.len() {
        let mut conn = Pool::new(ctx.dsn)
            .get_conn()
            .await
            .context("snapshot worker connect")?;
        conn.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
            .await
            .context("start consistent snapshot")?;
        worker_conns.push(conn);
    }

    let mut pos_conn = Pool::new(ctx.dsn)
        .get_conn()
        .await
        .context("snapshot position connect")?;
    let position = capture_binlog_position(&mut pos_conn).await?;
    drop(pos_conn);

    progress.start_position = serde_json::to_string(&position)
        .context("serialize binlog position")?;
    save_progress(&ctx.chkpt_store, ctx.source_id, &progress).await;

    // spawn background position guard
    let abort_reason: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let guard_cancel = ctx.cancel.child_token();
    let _guard_stop = scopeguard::guard((), |_| guard_cancel.cancel());
    let _position_guard = spawn_binlog_position_guard(
        ctx.dsn.to_string(),
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

    // step 2: fan out parallel table workers (unchanged)
    let max_parallel = ctx.cfg.max_parallel_tables.min(tables.len()).max(1);
    let semaphore = Arc::new(Semaphore::new(max_parallel));
    let mut handles = Vec::new();

    for ((db, table), conn) in tables.iter().zip(worker_conns) {
        if progress.table_done(db, table) {
            info!(table = %fqn(db, table), "already complete, skipping");
            let mut c = conn;
            c.query_drop("COMMIT").await.ok();
            continue;
        }

        let permit = semaphore.clone().acquire_owned().await?;
        let worker = TableWorker {
            db: db.clone(),
            table: table.clone(),
            conn,
            source_id: ctx.source_id.to_string(),
            pipeline: ctx.pipeline.to_string(),
            tenant: ctx.tenant.to_string(),
            cfg: ctx.cfg.clone(),
            tx: ctx.tx.clone(),
            schema_loader: ctx.schema_loader.clone(),
            cancel: ctx.cancel.clone(),
        };
        let handle = tokio::spawn(async move {
            let result = worker.run().await;
            drop(permit);
            result
        });
        handles.push((fqn(db, table), handle));
    }

    // step 3: collect results
    let mut failed = Vec::new();

    for (name, handle) in handles {
        match handle.await {
            Ok(Ok(rows)) => {
                let parts: Vec<&str> = name.splitn(2, '.').collect();
                if parts.len() == 2 {
                    progress.mark_done(parts[0], parts[1]);
                    save_progress(&ctx.chkpt_store, ctx.source_id, &progress)
                        .await;
                }
                info!(table = %name, rows, "table snapshot complete");
            }
            Ok(Err(e)) => {
                error!(table = %name, error = %e, "table snapshot failed");
                failed.push(name);
            }
            Err(e) => {
                error!(table = %name, error = %e, "snapshot worker panicked");
                failed.push(name);
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
    progress.finished = true;
    save_progress(&ctx.chkpt_store, ctx.source_id, &progress).await;

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
                .context("SHOW MASTER STATUS — is binary logging enabled?")?,
        };

    let mut row = row
        .context("no binlog position returned — is binary logging enabled?")?;

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
    tx: mpsc::Sender<Event>,
    schema_loader: MySqlSchemaLoader,
    cancel: CancellationToken,
}

impl TableWorker {
    async fn run(mut self) -> Result<u64> {
        info!(pipeline=%self.pipeline, source_id=%self.source_id, db=%self.db, table=%self.table, "snapshot worker starting");
        let table_fqn = fqn(&self.db, &self.table);
        let t0 = Instant::now();

        let loaded = self
            .schema_loader
            .load_schema(&self.db, &self.table)
            .await
            .with_context(|| format!("load schema for {table_fqn}"))?;

        let pk = &loaded.schema.primary_key;
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

        self.conn.query_drop("COMMIT").await.ok();

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

        Ok(rows_sent)
    }

    // ── PK-range chunking ─────────────────────────────────────────────────────

    async fn by_pk(&mut self, pk_col: &str) -> Result<u64> {
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

            let n = rows.len() as u64;
            for row in rows {
                if self
                    .tx
                    .send(self.make_event(row_to_json(row)?))
                    .await
                    .is_err()
                {
                    anyhow::bail!("event channel closed");
                }
            }

            total_sent += n;
            cursor = end;
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
        for row in rows {
            if self
                .tx
                .send(self.make_event(row_to_json(row)?))
                .await
                .is_err()
            {
                anyhow::bail!("event channel closed");
            }
        }

        Ok(n)
    }

    fn make_event(&self, after: serde_json::Value) -> Event {
        let size = after.to_string().len();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let mut ev = Event::new_row(
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
                position: SourcePosition::default(),
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
