//! PostgreSQL consistent snapshot engine.
//!
//! Performs a lock-free initial load using PostgreSQL's exported snapshot mechanism:
//!
//! 1. Open a coordinator connection, begin REPEATABLE READ, export a snapshot ID
//!    and capture the current WAL LSN - all in one round trip.
//! 2. Fan out parallel table workers; each imports the shared snapshot into its own
//!    transaction so all workers see the same consistent DB state.
//! 3. Tables with a single integer PK use PK-range chunking (O(log n) seeks).
//!    All other tables fall back to ctid page-range chunking.
//! 4. Completed tables are recorded in the checkpoint store so a crashed snapshot
//!    resumes at the table level rather than restarting from scratch.
//! 5. When all tables finish the coordinator commits, releasing the exported snapshot,
//!    and the captured WAL LSN is returned to the caller as the replication start point.

use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{Context, Result};
use checkpoints::CheckpointStore;
use common::redact_url_password;
use deltaforge_config::SnapshotCfg;
use deltaforge_core::{Event, Op, SourceInfo, SourcePosition};
use metrics::counter;
use pgwire_replication::Lsn;
use scopeguard;
use serde::{Deserialize, Serialize};
use tokio::sync::{Semaphore, mpsc};
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use super::postgres_health as health;
use super::postgres_schema_loader::PostgresSchemaLoader;

// ============================================================================
// Snapshot progress (persisted for resume on crash)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SnapshotProgress {
    /// WAL LSN captured before any rows were read.
    pub start_lsn: String,
    /// Tables that have been fully snapshotted ("schema.table").
    pub done_tables: Vec<String>,
    /// True once every table is complete.
    pub finished: bool,
}

impl SnapshotProgress {
    pub fn table_done(&self, schema: &str, table: &str) -> bool {
        self.done_tables.contains(&fqn(schema, table))
    }

    pub fn mark_done(&mut self, schema: &str, table: &str) {
        let key = fqn(schema, table);
        if !self.done_tables.contains(&key) {
            self.done_tables.push(key);
        }
    }
}

fn fqn(schema: &str, table: &str) -> String {
    format!("{schema}.{table}")
}

pub fn progress_key(source_id: &str) -> String {
    format!("snapshot_progress:{source_id}")
}

// ============================================================================
// Entry point
// ============================================================================

pub struct PgSnapshotCtx<'a> {
    pub dsn: &'a str,
    pub source_id: &'a str,
    pub pipeline: &'a str,
    pub tenant: &'a str,
    pub cfg: &'a SnapshotCfg,
    pub schema_loader: &'a PostgresSchemaLoader,
    pub chkpt_store: Arc<dyn CheckpointStore>,
    pub tx: mpsc::Sender<Event>,
    pub cancel: CancellationToken,
    pub slot_name: Option<&'a str>,
}

/// Run a consistent snapshot of `tables`.
///
/// Returns the WAL LSN captured before any rows were read — pass this to the
/// replication client as `start_lsn` so streaming picks up exactly where the
/// snapshot left off with no gaps and no duplicate events.
pub async fn run_snapshot(
    ctx: &PgSnapshotCtx<'_>,
    tables: &[(String, String)],
) -> Result<Lsn> {
    let t0 = Instant::now();

    // load any previous progress so we can skip already-completed tables.
    let mut progress: SnapshotProgress = ctx
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
            "snapshot already complete, returning saved LSN"
        );
        return Lsn::parse(&progress.start_lsn)
            .context("parse saved snapshot LSN");
    }

    // preflight
    let preflight = health::run_preflight(
        ctx.dsn,
        ctx.slot_name,
        // publication name not on ctx — pass empty string; publication check
        // is already done in ensure_slot_and_publication before we get here.
        // Pass slot_name here only for slot health checks.
        "",
        tables,
        ctx.cfg.max_parallel_tables,
    )
    .await
    .context("postgres snapshot preflight")?;
    preflight.emit_and_check(ctx.source_id, tables.len())?;

    // step 1: coordinator connection - export snapshot + capture LSN
    let (coord, coord_conn) = tokio_postgres::connect(ctx.dsn, NoTls)
        .await
        .context("snapshot coordinator connect")?;

    tokio::spawn(async move {
        if let Err(e) = coord_conn.await {
            error!(error = %e, "snapshot coordinator connection dropped");
        }
    });

    coord
        .batch_execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .await
        .context("begin coordinator transaction")?;

    let row = coord
        .query_one(
            "SELECT pg_export_snapshot(), pg_current_wal_lsn()::text",
            &[],
        )
        .await
        .context("export snapshot + capture LSN")?;

    let snapshot_id: String = row.get(0);
    let lsn_str: String = row.get(1);
    let start_lsn = Lsn::parse(&lsn_str).context("parse snapshot LSN")?;

    // Save start_lsn immediately - if we crash before finishing, we know
    // where to resume streaming from.
    progress.start_lsn = lsn_str.clone();
    save_progress(&ctx.chkpt_store, ctx.source_id, &progress).await;

    let abort_reason: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let guard_cancel = ctx.cancel.child_token();
    let _guard_stop = scopeguard::guard((), |_| guard_cancel.cancel());
    let _slot_guard = ctx.slot_name.map(|slot| {
        health::spawn_wal_slot_guard(
            ctx.dsn.to_string(),
            slot.to_string(),
            guard_cancel.clone(),
            abort_reason.clone(),
        )
    });

    info!(
        source_id = %ctx.source_id,
        snapshot_id = %snapshot_id,
        lsn = %start_lsn,
        tables = tables.len(),
        "snapshot started"
    );

    // step 2: fan out parallel table workers
    let max_parallel = ctx.cfg.max_parallel_tables.min(tables.len()).max(1);
    let semaphore = Arc::new(Semaphore::new(max_parallel));
    let mut handles = Vec::new();

    for (schema, table) in tables {
        if progress.table_done(schema, table) {
            info!(table = %fqn(schema, table), "already complete, skipping");
            continue;
        }

        let permit = semaphore.clone().acquire_owned().await?;

        let worker = TableWorker {
            dsn: ctx.dsn.to_string(),
            schema: schema.clone(),
            table: table.clone(),
            snapshot_id: snapshot_id.clone(),
            source_id: ctx.source_id.to_string(),
            pipeline: ctx.pipeline.to_string(),
            tenant: ctx.tenant.to_string(),
            cfg: ctx.cfg.clone(),
            tx: ctx.tx.clone(),
            schema_loader: ctx.schema_loader.clone(),
            chkpt_store: ctx.chkpt_store.clone(),
            cancel: ctx.cancel.clone(),
        };

        let handle = tokio::spawn(async move {
            let result = worker.run().await;
            drop(permit);
            result
        });

        handles.push((fqn(schema, table), handle));
    }

    // step 3: collect results
    let mut failed = Vec::new();

    for (name, handle) in handles {
        match handle.await {
            Ok(Ok(_rows)) => {
                let parts: Vec<&str> = name.splitn(2, '.').collect();
                if parts.len() == 2 {
                    progress.mark_done(parts[0], parts[1]);
                    save_progress(&ctx.chkpt_store, ctx.source_id, &progress)
                        .await;
                }
                info!(table = %name, "snapshot complete");
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

    // guard check takes priority
    if let Some(reason) = abort_reason.lock().unwrap().take() {
        anyhow::bail!("snapshot aborted: {}", reason);
    }

    if !failed.is_empty() {
        anyhow::bail!("snapshot failed for: {}", failed.join(", "));
    }

    // final slot health check before marking complete
    if let Some(slot) = ctx.slot_name {
        health::verify_slot_still_healthy(ctx.dsn, slot)
            .await
            .context("post-snapshot slot verification")?;
    }

    // release the exported snapshot.
    coord.batch_execute("COMMIT").await.ok();

    // mark fully done.
    progress.finished = true;
    save_progress(&ctx.chkpt_store, ctx.source_id, &progress).await;

    info!(
        source_id = %ctx.source_id,
        elapsed_secs = t0.elapsed().as_secs(),
        start_lsn = %start_lsn,
        "snapshot finished — streaming will start from this LSN"
    );

    guard_cancel.cancel();
    Ok(start_lsn)
}

async fn save_progress(
    store: &Arc<dyn CheckpointStore>,
    source_id: &str,
    progress: &SnapshotProgress,
) {
    if let Ok(bytes) = serde_json::to_vec(progress) {
        let _ = store.put_raw(&progress_key(source_id), &bytes).await;
    }
}

// ============================================================================
// Table worker
// ============================================================================

struct TableWorker {
    dsn: String,
    schema: String,
    table: String,
    snapshot_id: String,
    source_id: String,
    pipeline: String,
    tenant: String,
    cfg: SnapshotCfg,
    tx: mpsc::Sender<Event>,
    schema_loader: PostgresSchemaLoader,
    #[allow(unused)]
    chkpt_store: Arc<dyn CheckpointStore>,
    cancel: CancellationToken,
}

impl TableWorker {
    async fn run(self) -> Result<u64> {
        info!(pipeline=%self.pipeline, source_id=%self.source_id, schema=%self.schema, table=%self.table, "snapshot worker starting");
        let fqn = fqn(&self.schema, &self.table);
        let t0 = Instant::now();

        let (client, conn) = tokio_postgres::connect(&self.dsn, NoTls)
            .await
            .with_context(|| format!("connect for {fqn}"))?;

        tokio::spawn(async move {
            if let Err(e) = conn.await {
                warn!(error = %e, "snapshot worker connection dropped");
            }
        });

        // Import the shared snapshot — all workers see the same DB state.
        client
            .batch_execute(&format!(
                "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ; \
                 SET TRANSACTION SNAPSHOT '{}'",
                self.snapshot_id
            ))
            .await
            .with_context(|| format!("import snapshot for {fqn}"))?;

        // Determine chunking strategy from the schema.
        let loaded = self
            .schema_loader
            .load_schema(&self.schema, &self.table)
            .await
            .with_context(|| format!("load schema for {fqn}"))?;

        let pk = &loaded.schema.primary_key;
        let total = estimate_rows(&client, &self.schema, &self.table).await;

        info!(table = %fqn, total_rows = total, pk = ?pk, "snapshotting");

        let rows_sent = if pk.len() == 1
            && is_integer_type(loaded.schema.column(pk[0].as_str()))
        {
            self.by_pk(&client, &pk[0], total).await?
        } else {
            if pk.len() != 1 {
                debug!(
                    table = %fqn,
                    "composite or missing PK — using ctid chunking"
                );
            }
            self.by_ctid(&client).await?
        };

        client.batch_execute("COMMIT").await.ok();

        counter!(
            "deltaforge_snapshot_rows_total",
            "pipeline" => self.pipeline.clone(),
            "table" => fqn.clone()
        )
        .increment(rows_sent);

        info!(
            table = %fqn,
            rows_sent,
            elapsed_ms = t0.elapsed().as_millis(),
            "table done"
        );

        Ok(rows_sent)
    }

    //PK-range chunking

    async fn by_pk(
        &self,
        client: &tokio_postgres::Client,
        pk_col: &str,
        total: u64,
    ) -> Result<u64> {
        let fqn = fqn(&self.schema, &self.table);

        let bounds = client
            .query_one(
                &format!(
                    r#"SELECT MIN("{pk_col}")::bigint, MAX("{pk_col}")::bigint
                       FROM "{}"."{}" "#,
                    self.schema, self.table
                ),
                &[],
            )
            .await
            .context("fetch PK bounds")?;

        let min_pk: Option<i64> = bounds.get(0);
        let max_pk: Option<i64> = bounds.get(1);
        let (min_pk, max_pk) = match (min_pk, max_pk) {
            (Some(a), Some(b)) => (a, b),
            _ => {
                debug!(table = %fqn, "empty table");
                return Ok(0);
            }
        };

        // Decide parallelism: multiple concurrent chunks for large tables
        // when intra_table_parallel is enabled.
        let use_parallel = self.cfg.intra_table_parallel
            && total > self.cfg.chunk_size as u64 * 4;

        if use_parallel {
            self.by_pk_parallel(client, pk_col, min_pk, max_pk).await
        } else {
            self.by_pk_sequential(client, pk_col, min_pk, max_pk).await
        }
    }

    async fn by_pk_sequential(
        &self,
        client: &tokio_postgres::Client,
        pk_col: &str,
        min_pk: i64,
        max_pk: i64,
    ) -> Result<u64> {
        let fqn = fqn(&self.schema, &self.table);
        let chunk = self.cfg.chunk_size as i64;
        let mut cursor = min_pk;
        let mut total_sent = 0u64;

        while cursor <= max_pk {
            if self.cancel.is_cancelled() {
                anyhow::bail!("snapshot cancelled");
            }
            let next = cursor + chunk;
            let n = self
                .read_pk_range(client, pk_col, cursor, next, &fqn)
                .await?;
            total_sent += n;
            cursor = next;
        }
        Ok(total_sent)
    }

    async fn by_pk_parallel(
        &self,
        client: &tokio_postgres::Client,
        pk_col: &str,
        min_pk: i64,
        max_pk: i64,
    ) -> Result<u64> {
        // Divide the PK space into N equal sub-ranges; each sub-range is read
        // sequentially by a dedicated connection that imports the same snapshot.
        let n_chunks = self.cfg.max_parallel_chunks;
        let range = max_pk - min_pk + 1;
        let per_chunk = (range / n_chunks as i64).max(1);

        let semaphore = Arc::new(Semaphore::new(n_chunks));
        let mut handles = Vec::new();

        let mut chunk_start = min_pk;
        while chunk_start <= max_pk {
            let chunk_end = (chunk_start + per_chunk).min(max_pk + 1);
            let permit = semaphore.clone().acquire_owned().await?;

            // Each intra-table chunk needs its own connection with the
            // snapshot imported.
            let (sub_client, sub_conn) =
                tokio_postgres::connect(&self.dsn, NoTls)
                    .await
                    .context("intra-table chunk connect")?;

            let snapshot_id = self.snapshot_id.clone();
            tokio::spawn(async move {
                if let Err(e) = sub_conn.await {
                    warn!(error = %e, "intra-table chunk connection dropped");
                }
            });

            sub_client
                .batch_execute(&format!(
                    "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ; \
                     SET TRANSACTION SNAPSHOT '{snapshot_id}'"
                ))
                .await
                .context("import snapshot for intra-table chunk")?;

            let worker_self = self.clone_for_chunk();
            let pk = pk_col.to_string();
            let fqn = fqn(&self.schema, &self.table);
            let step = self.cfg.chunk_size as i64;
            let tx = self.tx.clone();
            let cancel = self.cancel.clone();

            let handle = tokio::spawn(async move {
                let mut cursor = chunk_start;
                let mut sent = 0u64;
                while cursor < chunk_end {
                    if cancel.is_cancelled() {
                        return Err(anyhow::anyhow!("cancelled"));
                    }
                    let next = (cursor + step).min(chunk_end);
                    sent += worker_self
                        .read_pk_range_with_tx(
                            &sub_client,
                            &tx,
                            &pk,
                            cursor,
                            next,
                            &fqn,
                        )
                        .await?;
                    cursor = next;
                }
                sub_client.batch_execute("COMMIT").await.ok();
                drop(permit);
                Ok(sent)
            });
            handles.push(handle);
            chunk_start = chunk_end;
        }

        // read_pk_range also uses the main client for the sub-ranges not spawned
        // (shouldn't reach here if chunking is correct, but be safe)
        let _ = client;

        let mut total = 0u64;
        for h in handles {
            total += h.await.context("chunk task panicked")??;
        }
        Ok(total)
    }

    async fn read_pk_range(
        &self,
        client: &tokio_postgres::Client,
        pk_col: &str,
        from: i64,
        to: i64,
        fqn: &str,
    ) -> Result<u64> {
        self.read_pk_range_with_tx(client, &self.tx, pk_col, from, to, fqn)
            .await
    }

    async fn read_pk_range_with_tx(
        &self,
        client: &tokio_postgres::Client,
        tx: &mpsc::Sender<Event>,
        pk_col: &str,
        from: i64,
        to: i64,
        fqn: &str,
    ) -> Result<u64> {
        // row_to_json does the Rust type-mapping for free — every row becomes
        // a JSON object with correct types using PostgreSQL's own serialiser.
        let sql = format!(
            r#"SELECT row_to_json(t)::text
               FROM (SELECT * FROM "{}"."{}"
                     WHERE "{pk_col}" >= $1 AND "{pk_col}" < $2
                     ORDER BY "{pk_col}") t"#,
            self.schema, self.table
        );

        let rows = client
            .query(&sql, &[&from, &to])
            .await
            .with_context(|| format!("read chunk [{from},{to}) from {fqn}"))?;

        let n = rows.len() as u64;
        for row in rows {
            let json_str: &str = row.get(0);
            let after: serde_json::Value = serde_json::from_str(json_str)
                .with_context(|| {
                    format!("parse row_to_json output for {fqn}")
                })?;
            let size = json_str.len();
            let event = self.make_event(after, size);
            if tx.send(event).await.is_err() {
                anyhow::bail!("event channel closed");
            }
        }

        Ok(n)
    }

    // ctid-range chunking (fallback for non-integer-PK tables)
    async fn by_ctid(&self, client: &tokio_postgres::Client) -> Result<u64> {
        let fqn = fqn(&self.schema, &self.table);

        // relpages is 0 on tables that have never been ANALYZEd (common in
        // tests and freshly loaded tables). ANALYZE first so stats are current.
        client
            .execute(
                &format!(r#"ANALYZE "{}"."{}" "#, self.schema, self.table),
                &[],
            )
            .await
            .ok();

        let page_row = client
            .query_one(
                "SELECT relpages FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = $1 AND c.relname = $2",
                &[&self.schema, &self.table],
            )
            .await
            .context("fetch page count")?;

        let total_pages: i32 = page_row.get(0);
        if total_pages == 0 {
            debug!(table = %fqn, "empty table");
            return Ok(0);
        }

        // Aim for ~chunk_size rows per batch (assume ~100 rows/page).
        let pages_per_chunk = ((self.cfg.chunk_size / 100) as i32).max(1);
        let mut page = 0i32;
        let mut total_sent = 0u64;

        while page < total_pages {
            if self.cancel.is_cancelled() {
                anyhow::bail!("snapshot cancelled");
            }
            let end_page = (page + pages_per_chunk).min(total_pages);
            let sql = format!(
                r#"SELECT row_to_json(t)::text
                   FROM (SELECT * FROM "{}"."{}"
                         WHERE ctid >= '({page},1)'::tid
                           AND ctid < '({end_page},1)'::tid) t"#,
                self.schema, self.table
            );

            let rows = client.query(&sql, &[]).await.with_context(|| {
                format!("read ctid [{page},{end_page}) from {fqn}")
            })?;

            let n = rows.len() as u64;
            for row in rows {
                let json_str: &str = row.get(0);
                let after: serde_json::Value =
                    serde_json::from_str(json_str).context("parse ctid row")?;
                let size = json_str.len();
                let event = self.make_event(after, size);
                if self.tx.send(event).await.is_err() {
                    anyhow::bail!("event channel closed");
                }
            }

            total_sent += n;
            page = end_page;
        }

        Ok(total_sent)
    }

    //Helpers
    fn make_event(&self, after: serde_json::Value, size_bytes: usize) -> Event {
        let ts_ms = chrono::Utc::now().timestamp_millis();
        let source = SourceInfo {
            version: concat!("deltaforge-", env!("CARGO_PKG_VERSION"))
                .to_string(),
            connector: "postgresql".into(),
            name: self.pipeline.clone(),
            ts_ms,
            db: self.schema.clone(),
            schema: Some(self.schema.clone()),
            table: self.table.clone(),
            snapshot: Some("true".into()),
            position: SourcePosition::default(),
        };

        Event::new_row(source, Op::Read, None, Some(after), ts_ms, size_bytes)
            .with_tenant(self.tenant.clone())
    }

    /// Shallow clone for intra-table parallel workers.
    fn clone_for_chunk(&self) -> ChunkWorkerCtx {
        ChunkWorkerCtx {
            schema: self.schema.clone(),
            table: self.table.clone(),
            pipeline: self.pipeline.clone(),
            tenant: self.tenant.clone(),
            dsn: self.dsn.clone(),
            snapshot_id: self.snapshot_id.clone(),
            chunk_size: self.cfg.chunk_size,
        }
    }
}

/// Minimal context passed into spawned intra-table chunk tasks.
struct ChunkWorkerCtx {
    schema: String,
    table: String,
    pipeline: String,
    tenant: String,
    dsn: String,
    snapshot_id: String,
    chunk_size: usize,
}

impl ChunkWorkerCtx {
    async fn read_pk_range_with_tx(
        &self,
        client: &tokio_postgres::Client,
        tx: &mpsc::Sender<Event>,
        pk_col: &str,
        from: i64,
        to: i64,
        fqn: &str,
    ) -> Result<u64> {
        debug!(
            pipeline=%self.pipeline,
            dsn=%redact_url_password(&self.dsn),
            snapshot_id=%self.snapshot_id,
            chunk_size=%self.chunk_size,
            "reading PK range"
        );

        let sql = format!(
            r#"SELECT row_to_json(t)::text
               FROM (SELECT * FROM "{}"."{}"
                     WHERE "{pk_col}" >= $1 AND "{pk_col}" < $2
                     ORDER BY "{pk_col}") t"#,
            self.schema, self.table
        );

        let rows = client
            .query(&sql, &[&from, &to])
            .await
            .with_context(|| format!("read chunk [{from},{to}) from {fqn}"))?;

        let n = rows.len() as u64;
        for row in rows {
            let json_str: &str = row.get(0);
            let after: serde_json::Value =
                serde_json::from_str(json_str).context("parse row")?;
            let size = json_str.len();
            let ts_ms = chrono::Utc::now().timestamp_millis();
            let source = SourceInfo {
                version: concat!("deltaforge-", env!("CARGO_PKG_VERSION"))
                    .to_string(),
                connector: "postgresql".into(),
                name: self.pipeline.clone(),
                ts_ms,
                db: self.schema.clone(),
                schema: Some(self.schema.clone()),
                table: self.table.clone(),
                snapshot: Some("true".into()),
                position: SourcePosition::default(),
            };
            let event = Event::new_row(
                source,
                Op::Read,
                None,
                Some(after),
                ts_ms,
                size,
            )
            .with_tenant(self.tenant.clone());
            if tx.send(event).await.is_err() {
                anyhow::bail!("event channel closed");
            }
        }

        Ok(n)
    }
}

// Utility functions

fn is_integer_type(
    col: Option<&crate::postgres::postgres_table_schema::PostgresColumn>,
) -> bool {
    let Some(col) = col else { return false };
    matches!(
        col.base_type().to_lowercase().as_str(),
        "integer"
            | "int"
            | "int4"
            | "int8"
            | "bigint"
            | "smallint"
            | "int2"
            | "serial"
            | "bigserial"
            | "smallserial"
    )
}

async fn estimate_rows(
    client: &tokio_postgres::Client,
    schema: &str,
    table: &str,
) -> u64 {
    // Use pg_class statistics for a fast estimate (no full scan).
    let row = client
        .query_opt(
            "SELECT reltuples::bigint FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND c.relname = $2",
            &[&schema, &table],
        )
        .await;

    match row {
        Ok(Some(r)) => r.get::<_, i64>(0).max(0) as u64,
        _ => 0,
    }
}
