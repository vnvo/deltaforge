//! MySQL consistent snapshot engine.
//!
//! Low-impact initial load using InnoDB's consistent read mechanism:
//!
//! 1. Open a coordinator connection. Issue `FLUSH TABLES WITH READ LOCK` to
//!    freeze writes just long enough to capture the current binlog position
//!    and open all table worker connections with
//!    `START TRANSACTION WITH CONSISTENT SNAPSHOT`.
//! 2. Release the lock immediately — writes resume. Workers continue reading
//!    their consistent snapshots in parallel; all see the same DB state.
//! 3. Tables with a single integer PK use PK-range chunking. All others fall
//!    back to a streaming full scan.
//! 4. Completed tables are recorded in the checkpoint store so a crash resumes
//!    at the table level rather than restarting from scratch.
//! 5. Returns a `MySqlCheckpoint` captured at the lock point — pass this to
//!    `prepare_client` as the replication start position so streaming picks up
//!    exactly where the snapshot left off.

use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use checkpoints::CheckpointStore;
use deltaforge_config::SnapshotCfg;
use deltaforge_core::{Event, Op, SourceInfo, SourcePosition};
use mysql_async::{Pool, Row, Value, prelude::Queryable};
use serde::{Deserialize, Serialize};
use tokio::sync::{Semaphore, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use super::{MySqlCheckpoint, MySqlSchemaLoader};

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

/// Run a consistent snapshot of `tables`.
///
/// Returns a `MySqlCheckpoint` captured at the `FLUSH TABLES WITH READ LOCK`
/// point. The caller must persist this as the binlog checkpoint so streaming
/// resumes with no gaps.
pub async fn run_snapshot(
    dsn: &str,
    source_id: &str,
    pipeline: &str,
    tenant: &str,
    tables: &[(String, String)], // (db, table)
    cfg: &SnapshotCfg,
    schema_loader: &MySqlSchemaLoader,
    chkpt_store: Arc<dyn CheckpointStore>,
    tx: mpsc::Sender<Event>,
    cancel: CancellationToken,
) -> Result<MySqlCheckpoint> {
    let t0 = Instant::now();

    // load previous progress for crash resume.
    let mut progress: MysqlSnapshotProgress = chkpt_store
        .get_raw(&progress_key(source_id))
        .await
        .ok()
        .flatten()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or_default();

    if progress.finished {
        info!(
            source_id,
            "mysql snapshot already complete, returning saved position"
        );
        return serde_json::from_str(&progress.start_position)
            .context("parse saved snapshot position");
    }

    // step 1: lock, capture position, start worker transactions
    let mut worker_conns: Vec<mysql_async::Conn> =
        Vec::with_capacity(tables.len());
    for _ in 0..tables.len() {
        let mut conn = Pool::new(dsn)
            .get_conn()
            .await
            .context("snapshot worker connect")?;
        conn.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
            .await
            .context("start consistent snapshot")?;
        worker_conns.push(conn);
    }

    // Capture position AFTER all workers have started their consistent snapshots.
    // InnoDB guarantees every row visible to a worker was committed at a binlog
    // position ≤ this captured position, so CDC streaming from here has no gaps.
    let mut pos_conn = Pool::new(dsn)
        .get_conn()
        .await
        .context("snapshot position connect")?;
    let position = capture_binlog_position(&mut pos_conn).await?;
    drop(pos_conn);

    // persist position immediately for crash safety.
    progress.start_position = serde_json::to_string(&position)
        .context("serialize binlog position")?;
    save_progress(&chkpt_store, source_id, &progress).await;

    info!(
        source_id,
        file = %position.file,
        pos = position.pos,
        gtid = ?position.gtid_set,
        tables = tables.len(),
        "mysql snapshot started"
    );

    // step 2: fan out parallel table workers
    let max_parallel = cfg.max_parallel_tables.min(tables.len()).max(1);
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
            source_id: source_id.to_string(),
            pipeline: pipeline.to_string(),
            tenant: tenant.to_string(),
            cfg: cfg.clone(),
            tx: tx.clone(),
            schema_loader: schema_loader.clone(),
            cancel: cancel.clone(),
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
                    save_progress(&chkpt_store, source_id, &progress).await;
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

    if !failed.is_empty() {
        anyhow::bail!("mysql snapshot failed for: {}", failed.join(", "));
    }

    progress.finished = true;
    save_progress(&chkpt_store, source_id, &progress).await;

    info!(
        source_id,
        elapsed_secs = t0.elapsed().as_secs(),
        file = %position.file,
        pos = position.pos,
        "mysql snapshot finished"
    );

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
    let mut out = String::with_capacity((bytes.len() + 2) / 3 * 4);
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
