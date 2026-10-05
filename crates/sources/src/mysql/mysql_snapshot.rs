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

use anyhow::{Context, Result, anyhow, bail};
use checkpoints::CheckpointStore;
use deltaforge_config::SnapshotCfg;
use deltaforge_core::{
    Event, EventId, Op, SourceInfo, SourceItem, SourcePosition,
};
use metrics::counter;
use mysql_async::{Conn, Opts, Row, Value, prelude::Queryable};
use tokio::time::timeout;

use super::mysql_identity::mysql_identity_cell;
use crate::durable_checkpoint::CursorKind;
use crate::snapshot_event_id::{OwnedIdentityValue, snapshot_row_event_id};
use crate::snapshot_generation::PersistedLineage;
use crate::snapshot_publish::GenerationPublisher;
use crate::snapshot_queue::{PlanItem, QueueStore};
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
    /// Tables that have been fully snapshotted ("db.table"); a JSON array of
    /// names, as always.
    pub done_tables: std::collections::BTreeSet<String>,
    /// True once every table is complete.
    pub finished: bool,
    /// The generation whose anchor `start_position` is; absent (0) in
    /// records written before it was recorded.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub generation: u64,
}

fn is_zero(n: &u64) -> bool {
    *n == 0
}

impl MysqlSnapshotProgress {
    pub(crate) fn table_done(&self, db: &str, table: &str) -> bool {
        self.done_tables.contains(&fqn(db, table))
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
// The legacy classification reads pre-queue progress with it (design
// section 10).
#[cfg_attr(not(test), allow(dead_code))]
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
    /// Verified registry lineage hash stamped on the snapshot checkpoint.
    pub checkpoint_lineage: Option<String>,
    pub dsn: &'a str,
    /// The verified `server_uuid`: every snapshot connection (lock, workers,
    /// position checks) proves it is this server before it is used.
    pub expected_uuid: &'a str,
    pub source_id: &'a str,
    pub pipeline: &'a str,
    pub tenant: &'a str,
    pub cfg: &'a SnapshotCfg,
    /// The configured table patterns (the catalog is re-read under the
    /// anchor's read lock to verify the plan).
    pub table_patterns: &'a [String],
    pub schema_loader: &'a MySqlSchemaLoader,
    pub chkpt_store: Arc<dyn CheckpointStore>,
    pub tx: mpsc::Sender<SourceItem>,
    pub cancel: CancellationToken,
    /// Durable snapshot generation (allocated before any row).
    pub generation: u64,
    /// Frozen source lineage for snapshot identity.
    pub lineage: PersistedLineage,
}

/// The bounds a running generation is held to (design section 9): the
/// anchor's binlog file still on the server, the anchor age, and the binlog
/// retention against it. Approaching a limit raises a non-blocking warning;
/// reaching it blocks the generation (`snapshot_anchor_unavailable`, only
/// the run's own generation at the version it holds) and stops the copy.
/// A connection to another server stops the copy (the generation is lost).
pub(crate) struct GenerationGuard {
    pub dsn: crate::credentials::ProtectedDsn,
    pub expected_uuid: String,
    pub captured_file: String,
    pub source_id: String,
    pub generation: u64,
    pub run: String,
    pub version: Arc<std::sync::atomic::AtomicU64>,
    pub anchored_at_ms: i64,
    pub max_anchor_age: Duration,
    /// The server's binlog retention, when known.
    pub retention: Option<Duration>,
    pub queue: QueueStore,
    pub incidents: storage::adapters::incidents::IncidentStore,
    /// Cancelled when the generation blocks or is lost.
    pub cancel: CancellationToken,
    /// Why it stopped.
    pub stopped: Arc<Mutex<Option<GuardStop>>>,
}

/// Why the guard stopped the copy.
#[derive(Debug, Clone)]
pub(crate) enum GuardStop {
    /// A bound blocked the generation.
    Blocked(String),
    /// A guard connection reached another server than the generation's.
    WrongServer(String),
}

/// Run the guard until the copy ends (the task is aborted) or a bound
/// stops it.
pub(crate) fn spawn_generation_guard(
    g: GenerationGuard,
) -> tokio::task::JoinHandle<()> {
    use crate::snapshot_driver::{
        GuardFinding, anchor_age_finding, incidents as drafts,
        retention_finding,
    };
    use deltaforge_core::incident::ReasonCode;
    let every = (g.max_anchor_age / 10)
        .clamp(Duration::from_millis(200), POSITION_GUARD_INTERVAL);
    tokio::spawn(async move {
        let mut warned: std::collections::BTreeSet<&'static str> =
            Default::default();
        loop {
            tokio::select! {
                _ = g.cancel.cancelled() => return,
                _ = tokio::time::sleep(every) => {}
            }
            let age = Duration::from_millis(
                u64::try_from(
                    chrono::Utc::now().timestamp_millis() - g.anchored_at_ms,
                )
                .unwrap_or(0),
            );
            let binlog = match binlog_finding(&g).await {
                Ok(f) => f,
                Err(lost) => {
                    warn!(source_id = %g.source_id, "{lost}");
                    *g.stopped.lock().expect("not poisoned") =
                        Some(GuardStop::WrongServer(lost));
                    g.cancel.cancel();
                    return;
                }
            };
            for finding in [
                anchor_age_finding(age, g.max_anchor_age),
                retention_finding(age, g.retention),
                binlog,
            ] {
                match finding {
                    GuardFinding::Ok => {}
                    GuardFinding::Warn(class) => {
                        if warned.insert(class) {
                            warn!(
                                source_id = %g.source_id, class,
                                "snapshot generation approaching a bound"
                            );
                            let _ = g
                                .incidents
                                .raise(
                                    &drafts::bound_warning(
                                        &g.source_id,
                                        g.generation,
                                        class,
                                    ),
                                    1,
                                )
                                .await;
                        }
                    }
                    GuardFinding::Block(class) => {
                        error!(
                            source_id = %g.source_id, class,
                            "snapshot generation reached a bound: blocked"
                        );
                        let draft = drafts::blocked(
                            ReasonCode::SnapshotAnchorUnavailable,
                            &g.source_id,
                            g.generation,
                            class,
                        );
                        if let Err(e) =
                            crate::snapshot_driver::block_generation(
                                &g.queue,
                                &g.incidents,
                                g.generation,
                                Some(&g.run),
                                g.version
                                    .load(std::sync::atomic::Ordering::SeqCst),
                                &draft,
                            )
                            .await
                        {
                            warn!(
                                source_id = %g.source_id,
                                error = %e,
                                "could not record the block; the next start \
                                 detects it again"
                            );
                        }
                        *g.stopped.lock().expect("not poisoned") =
                            Some(GuardStop::Blocked(format!(
                                "snapshot_anchor_unavailable ({class})"
                            )));
                        g.cancel.cancel();
                        return;
                    }
                }
            }
        }
    })
}

/// One check of the anchor's binlog file on the verified server: purged
/// blocks; a transient failure is `Ok` (retried); a connection to another
/// server is `Err` (the generation's server is gone).
async fn binlog_finding(
    g: &GenerationGuard,
) -> std::result::Result<crate::snapshot_driver::GuardFinding, String> {
    use crate::snapshot_driver::GuardFinding;
    let mut conn = match super::mysql_session::open_control_connection(
        g.dsn.expose(),
        &g.expected_uuid,
        Duration::from_secs(10),
    )
    .await
    {
        Ok(c) => c,
        Err(super::mysql_session::SessionError::Connect(e)) => {
            warn!(error = %e, "snapshot guard: connect error, retrying");
            return Ok(GuardFinding::Ok);
        }
        Err(e) => {
            return Err(format!(
                "snapshot guard: the connection is not server {}: {e:?}",
                g.expected_uuid
            ));
        }
    };
    let rows: Vec<Row> = match conn.query("SHOW BINARY LOGS").await {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, "snapshot guard: SHOW BINARY LOGS failed, retrying");
            return Ok(GuardFinding::Ok);
        }
    };
    conn.disconnect().await.ok();
    let available: Vec<String> = rows
        .into_iter()
        .filter_map(|mut r: Row| r.take::<String, usize>(0))
        .collect();
    Ok(
        if health::binlog_file_still_present(&available, &g.captured_file) {
            GuardFinding::Ok
        } else {
            GuardFinding::Block("binlog_purged")
        },
    )
}

/// The sealed plan's tables in key order (the discovery order), read page by
/// page from the store: the plan is never resident in full.
pub(crate) struct PlanPages {
    queue: Option<QueueStore>,
    generation: u64,
    page: usize,
    buf: std::collections::VecDeque<super::MyPlannedTable>,
    after: Option<String>,
    done: bool,
}

impl PlanPages {
    pub(crate) fn new(queue: QueueStore, generation: u64, page: usize) -> Self {
        Self {
            queue: Some(queue),
            generation,
            page: page.max(1),
            buf: Default::default(),
            after: None,
            done: false,
        }
    }

    /// A plan of no table.
    #[cfg(test)]
    fn empty() -> Self {
        Self {
            queue: None,
            generation: 0,
            page: 1,
            buf: Default::default(),
            after: None,
            done: true,
        }
    }

    /// The next planned table.
    pub(crate) async fn next(
        &mut self,
    ) -> Result<Option<super::MyPlannedTable>> {
        if self.buf.is_empty() && !self.done {
            let queue = self.queue.as_ref().expect("a stored plan");
            let (items, next) = queue
                .items_page(self.generation, self.after.as_deref(), self.page)
                .await?;
            for (_, item) in &items {
                self.buf.push_back(planned(item)?);
            }
            self.done = next.is_none();
            self.after = next;
        }
        Ok(self.buf.pop_front())
    }
}

/// A plan item as a table to copy.
fn planned(item: &PlanItem) -> Result<super::MyPlannedTable> {
    let identity: Vec<String> = serde_json::from_value(item.identity.clone())
        .with_context(|| {
        format!(
            "plan item {}.{}: identity columns",
            item.qualifier, item.table
        )
    })?;
    Ok(super::MyPlannedTable {
        qualifier: item.qualifier.clone(),
        table: item.table.clone(),
        identity,
        cursor_kind: item.cursor_kind,
        signature: item.signature.clone(),
    })
}

/// What one generation's copy needs (`docs/design/snapshot-durable-queue.md`,
/// sections 2 and 9).
pub(crate) struct MyCopyCtx<'a> {
    pub source_id: &'a str,
    pub pipeline: &'a str,
    pub tenant: &'a str,
    pub cfg: &'a SnapshotCfg,
    pub schema_loader: &'a MySqlSchemaLoader,
    pub cancel: CancellationToken,
    pub plan: PlanPages,
    /// The workers' consistent-snapshot connections, all opened under the
    /// anchor's read lock: each one's read view is the generation's.
    pub workers: Vec<Conn>,
    pub publisher: Arc<GenerationPublisher>,
    pub generation: u64,
    pub lineage: PersistedLineage,
}

/// Copy every table of the generation's sealed plan. Each worker reads on
/// its own consistent-snapshot connection and takes the plan's next table
/// until none is left, then commits once. Losing any worker - its
/// connection or any failure of its table - loses the generation's read
/// view: the copy stops and fails, and the next start replaces the
/// generation (design section 2: no later-view reads).
pub(crate) async fn copy_generation(ctx: MyCopyCtx<'_>) -> Result<()> {
    let plan = Arc::new(tokio::sync::Mutex::new(ctx.plan));
    let stop = ctx.cancel.child_token();
    let _stop = scopeguard::guard(stop.clone(), |s| s.cancel());
    let fetches_before = ctx.schema_loader.live_fetch_count();
    let mut running = tokio::task::JoinSet::new();
    for conn in ctx.workers {
        let (plan, stop) = (Arc::clone(&plan), stop.clone());
        let shape = RowShape {
            db: String::new(),
            table: String::new(),
            pipeline: ctx.pipeline.to_string(),
            tenant: ctx.tenant.to_string(),
            generation: ctx.generation,
            lineage: ctx.lineage.clone(),
            identity: Vec::new(),
            schema: None,
        };
        let (cfg, schema_loader, publisher, source_id) = (
            ctx.cfg.clone(),
            ctx.schema_loader.clone(),
            Arc::clone(&ctx.publisher),
            ctx.source_id.to_string(),
        );
        running.spawn(async move {
            let mut conn = conn;
            loop {
                if stop.is_cancelled() {
                    bail!("snapshot cancelled");
                }
                let next = plan.lock().await.next().await?;
                let Some(planned) = next else {
                    break;
                };
                let key = planned.key();
                let worker = TableWorker {
                    conn,
                    shape: RowShape {
                        db: planned.qualifier.clone(),
                        table: planned.table.clone(),
                        identity: planned.identity.clone(),
                        ..shape.clone()
                    },
                    source_id: source_id.clone(),
                    cfg: cfg.clone(),
                    cursor_kind: planned.cursor_kind,
                    publisher: Arc::clone(&publisher),
                    schema_loader: schema_loader.clone(),
                    cancel: stop.clone(),
                };
                match worker.run().await {
                    Ok((rows, back)) => {
                        conn = back;
                        info!(table = %key, rows, "snapshot table copied");
                    }
                    Err(e) => {
                        stop.cancel();
                        return Err(e.context(format!(
                            "snapshot of {key}: the worker is lost, and with \
                             it the generation's read view"
                        )));
                    }
                }
            }
            conn.query_drop("COMMIT").await.ok();
            Ok(())
        });
    }
    crate::snapshot_probe::record_table_tasks(running.len());
    let mut first: Option<anyhow::Error> = None;
    while let Some(done) = running.join_next().await {
        let r = match done {
            Ok(r) => r,
            Err(e) => Err(anyhow!("snapshot worker panicked: {e}")),
        };
        if let Err(e) = r {
            stop.cancel();
            error!(error = %format!("{e:#}"), "snapshot worker failed");
            first.get_or_insert(e);
        }
    }
    crate::snapshot_probe::record_worker_live_fetches(
        ctx.schema_loader.live_fetch_count() - fetches_before,
    );
    match first {
        Some(e) => Err(e),
        None => Ok(()),
    }
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
/// The plan the anchor verifies under its read lock.
pub(crate) struct PlanCheck<'a> {
    pub patterns: &'a [String],
    pub page_size: usize,
    /// The sealed plan, page by page.
    pub expected: PlanPages,
}

#[cfg(test)]
impl PlanCheck<'static> {
    /// A plan of no table, for patterns that match none.
    fn nothing() -> Self {
        static NONE: std::sync::LazyLock<Vec<String>> =
            std::sync::LazyLock::new(|| {
                vec!["df_no_such_database.df_no_such_table".to_string()]
            });
        Self {
            patterns: &NONE,
            page_size: 1_000,
            expected: PlanPages::empty(),
        }
    }
}

/// Under the read lock (no DDL can run), re-read the catalog with the same
/// paged discovery and require exactly the planned tables, in order, page by
/// page against the stored plan: a table created, dropped or renamed since
/// discovery (between its pages, or before the anchor) fails the snapshot
/// before any row is read. MySQL's INFORMATION_SCHEMA is not read in one
/// consistent snapshot, so this is what makes the paged discovery a
/// consistent view, as of the anchor.
async fn verify_plan_under_lock(
    conn: &mut Conn,
    check: &mut PlanCheck<'_>,
) -> Result<()> {
    let mut discovery = crate::snapshot_discovery::Discovery::for_verification(
        check.patterns,
        check.page_size,
    );
    while !discovery.is_done() {
        let rows = super::mysql_schema_loader::discovery_page(
            conn,
            check.patterns,
            discovery.after(),
            discovery.page_size(),
        )
        .await?;
        let mut page = Vec::new();
        for (db, table) in discovery.accept(rows)? {
            match check.expected.next().await? {
                Some(p) if p.qualifier == db && p.table == table => {
                    page.push(p)
                }
                Some(p) => anyhow::bail!(catalog_moved(format!(
                    "{db}.{table} is where the plan has {}",
                    p.key()
                ))),
                None => {
                    anyhow::bail!(catalog_moved(format!("{db}.{table} is new")))
                }
            }
        }
        verify_shapes(conn, &page).await?;
    }
    if let Some(p) = check.expected.next().await? {
        anyhow::bail!(catalog_moved(format!("{} is gone", p.key())));
    }
    crate::snapshot_probe::record_fixed(
        crate::snapshot_probe::FixedOp::CatalogVerification,
    );
    Ok(())
}

/// Under the read lock, rebuild the registered schema model of each of
/// `planned` from INFORMATION_SCHEMA (the loader's own fetch) and require
/// the plan's signature: a table altered since preparation stops the
/// snapshot before any row is read.
async fn verify_shapes(
    conn: &mut Conn,
    planned: &[super::MyPlannedTable],
) -> Result<()> {
    let keys: Vec<(&str, &str)> = planned
        .iter()
        .map(|p| (p.qualifier.as_str(), p.table.as_str()))
        .collect();
    let schemas =
        super::mysql_schema_loader::fetch_table_schemas_on(conn, &keys)
            .await
            .context("verify the plan: read the planned schemas")?;
    for p in planned {
        let same = schemas
            .get(&(p.qualifier.clone(), p.table.clone()))
            .is_some_and(|s| mysql_schema_signature(s) == p.signature);
        if !same {
            anyhow::bail!(catalog_moved(format!(
                "the schema of {} changed",
                p.key()
            )));
        }
    }
    Ok(())
}

fn catalog_moved(detail: String) -> String {
    format!(
        "the tables the snapshot's patterns match changed between discovery \
         and the snapshot anchor ({detail}); no row was read - restart to plan \
         the snapshot again"
    )
}

pub(crate) async fn acquire_locked_anchor(
    dsn: &str,
    expected_uuid: &str,
    num_workers: usize,
    timeout_dur: Duration,
    mut check: PlanCheck<'_>,
) -> Result<(Vec<Conn>, MySqlCheckpoint)> {
    let opts = Opts::from_url(dsn).context("parse mysql dsn")?;
    // Each connection proves it is the verified server before it is used:
    // the anchor position and every worker's rows must come from one server.
    let verify = |e: super::mysql_session::SessionError| {
        anyhow::Error::new(e.into_source_error(expected_uuid))
    };

    // Dedicated, non-pooled lock connection: dropping it releases FTWRL.
    let mut lock_conn = Conn::new(opts.clone())
        .await
        .context("connect lock connection")?;
    super::mysql_session::verify_connection(&mut lock_conn, expected_uuid)
        .await
        .map_err(verify)?;

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
            super::mysql_session::verify_connection(&mut c, expected_uuid)
                .await
                .map_err(verify)?;
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
        verify_plan_under_lock(&mut lock_conn, &mut check).await?;
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

    // In GTID mode the position is the executed GTID set - also when it is
    // EMPTY (a server that has executed no GTID transaction yet), so it stays
    // comparable with later GTID checkpoints. Outside GTID mode, no set.
    let gtid_row: Option<Row> = conn
        .query_first("SELECT @@GLOBAL.gtid_mode, @@GLOBAL.gtid_executed")
        .await
        .ok()
        .flatten();
    let gtid_set = gtid_row.and_then(|mut r| {
        let mode: Option<String> = r.take::<Option<String>, _>(0).flatten();
        let set: Option<String> = r.take::<Option<String>, _>(1).flatten();
        mode.filter(|m| m.eq_ignore_ascii_case("ON"))
            .map(|_| set.unwrap_or_default())
    });

    Ok(MySqlCheckpoint {
        file,
        pos: pos as u64,
        gtid_set,
        lineage: None,
        snapshot_completed: None,
        snapshot_chain: None,
    })
}

// ============================================================================
// Table worker
// ============================================================================

struct TableWorker {
    /// Already-started consistent-snapshot transaction connection.
    conn: mysql_async::Conn,
    /// What every row of the table becomes an event with.
    shape: RowShape,
    source_id: String,
    cfg: SnapshotCfg,
    /// Cursor kind for this table (signed or unsigned PK, or a full scan).
    cursor_kind: CursorKind,
    publisher: Arc<GenerationPublisher>,
    schema_loader: MySqlSchemaLoader,
    cancel: CancellationToken,
}

/// The table a worker reads and what its rows become events with (apart
/// from the connection, so rows stream while events are built).
#[derive(Clone)]
struct RowShape {
    db: String,
    table: String,
    pipeline: String,
    tenant: String,
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
        info!(pipeline=%self.shape.pipeline, source_id=%self.source_id, db=%self.shape.db, table=%self.shape.table, "snapshot worker starting");
        let table_fqn = fqn(&self.shape.db, &self.shape.table);
        let t0 = Instant::now();

        let loaded = self
            .schema_loader
            .load_schema(&self.shape.db, &self.shape.table)
            .await
            .with_context(|| format!("load schema for {table_fqn}"))?;
        // Retain the schema for schema-directed native identity extraction.
        self.shape.schema = Some(loaded.schema.clone());

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
            "pipeline" => self.shape.pipeline.clone(),
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

    async fn by_pk_signed(&mut self, pk_col: &str) -> Result<u64> {
        let table_fqn = fqn(&self.shape.db, &self.shape.table);

        let bounds_row: Option<Row> = self
            .conn
            .query_first(format!(
                "SELECT MIN(`{pk_col}`), MAX(`{pk_col}`) FROM `{}`.`{}`",
                self.shape.db, self.shape.table
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
                    self.shape.db, self.shape.table
                ))
                .await
                .with_context(|| {
                    format!("PK range [{cursor},{end}) for {table_fqn}")
                })?;

            total_sent += rows.len() as u64;
            let events = self.shape.rows_to_events(rows)?;
            self.publisher
                .publish_chunk(events)
                .await
                .map_err(|e| anyhow!(e))?;
            cursor = end;
        }

        Ok(total_sent)
    }

    /// Unsigned integer PK scan. Bounds are read and interpolated as full-range
    /// `u64` (never cast through `i64`), and chunk arithmetic is overflow-safe so
    /// a range crossing `i64::MAX` and ending at `u64::MAX` scans correctly.
    async fn by_pk_unsigned(&mut self, pk_col: &str) -> Result<u64> {
        let table_fqn = fqn(&self.shape.db, &self.shape.table);

        let bounds_row: Option<Row> = self
            .conn
            .query_first(format!(
                "SELECT MIN(`{pk_col}`), MAX(`{pk_col}`) FROM `{}`.`{}`",
                self.shape.db, self.shape.table
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
                    self.shape.db, self.shape.table, cb.upper
                ))
                .await
                .with_context(|| {
                    format!("PK range [{cursor},{}] for {table_fqn}", cb.upper)
                })?;

            total_sent += rows.len() as u64;
            let events = self.shape.rows_to_events(rows)?;
            self.publisher
                .publish_chunk(events)
                .await
                .map_err(|e| anyhow!(e))?;

            if cb.inclusive {
                break;
            }
            cursor = cb.next;
        }

        Ok(total_sent)
    }

    // ── Full scan (composite/non-integer/no PK) ───────────────────────────────

    /// A table without an integer key is read by one streaming scan, in
    /// chunks of `chunk_size` rows: at most one chunk is held at a time.
    async fn full_scan(&mut self) -> Result<u64> {
        let table_fqn = fqn(&self.shape.db, &self.shape.table);
        let chunk = self.cfg.chunk_size.max(1);
        let mut result = self
            .conn
            .query_iter(format!(
                "SELECT * FROM `{}`.`{}`",
                self.shape.db, self.shape.table
            ))
            .await
            .with_context(|| format!("full scan of {table_fqn}"))?;
        let mut n = 0u64;
        let mut rows = Vec::with_capacity(chunk);
        loop {
            if self.cancel.is_cancelled() {
                anyhow::bail!("snapshot cancelled");
            }
            let row = result
                .next()
                .await
                .with_context(|| format!("full scan of {table_fqn}"))?;
            let end = row.is_none();
            if let Some(row) = row {
                rows.push(row);
            }
            if rows.len() >= chunk || (end && !rows.is_empty()) {
                n += rows.len() as u64;
                let events =
                    self.shape.rows_to_events(std::mem::take(&mut rows))?;
                self.publisher
                    .publish_chunk(events)
                    .await
                    .map_err(|e| anyhow!(e))?;
            }
            if end {
                break;
            }
        }
        drop(result);
        Ok(n)
    }
}

impl RowShape {
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

/// The plan signature of a MySQL table: its whole registered schema model.
pub(crate) fn mysql_schema_signature(
    schema: &super::MySqlTableSchema,
) -> String {
    crate::snapshot_plan::schema_signature(schema)
}

/// The snapshot cursor kind for a table: signed vs unsigned integer PK-range
/// scan, or an unsigned row-count cursor for the full-scan fallback. Must match
/// the worker's scan decision so the aggregator's kind agrees with the cursors it
/// receives.
pub(crate) fn mysql_cursor_kind(
    schema: &super::MySqlTableSchema,
) -> CursorKind {
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
            done_tables: ["db.t".to_string()].into(),
            finished: true,
            generation: 4,
        };
        store
            .put_raw(&progress_key("s1"), &serde_json::to_vec(&saved).unwrap())
            .await
            .unwrap();
        let got = load_snapshot_progress(&store, "s1").await.unwrap();
        assert!(got.finished);
        assert!(got.table_done("db", "t"));
        // The durable form is the JSON array it always was.
        let legacy = br#"{"start_position":"p","done_tables":["b.t","a.t"],"finished":false}"#;
        let p: MysqlSnapshotProgress = serde_json::from_slice(legacy).unwrap();
        assert!(p.table_done("a", "t") && p.table_done("b", "t"));
        let v: serde_json::Value = serde_json::to_value(&p).unwrap();
        assert_eq!(v["done_tables"], serde_json::json!(["a.t", "b.t"]));
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

    async fn live_uuid(dsn: &str) -> String {
        let mut c = Conn::new(Opts::from_url(dsn).unwrap()).await.unwrap();
        c.query_first("SELECT @@GLOBAL.server_uuid")
            .await
            .unwrap()
            .unwrap()
    }

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
        let (mut workers, position) = acquire_locked_anchor(
            &dsn,
            &live_uuid(&dsn).await,
            4,
            Duration::from_secs(10),
            PlanCheck::nothing(),
        )
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

        let (workers, _pos) = acquire_locked_anchor(
            &dsn,
            &live_uuid(&dsn).await,
            2,
            Duration::from_secs(10),
            PlanCheck::nothing(),
        )
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
        let res = acquire_locked_anchor(
            &dsn,
            &live_uuid(&dsn).await,
            2,
            Duration::from_secs(2),
            PlanCheck::nothing(),
        )
        .await;
        assert!(res.is_err(), "expected timeout while FTWRL was blocked");
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "acquire did not honor the timeout budget"
        );

        // Release the blocker; the anchor must now succeed (no lingering lock).
        blocker.query_drop("UNLOCK TABLES").await.unwrap();
        drop(blocker);
        let (workers, _pos) = acquire_locked_anchor(
            &dsn,
            &live_uuid(&dsn).await,
            2,
            Duration::from_secs(10),
            PlanCheck::nothing(),
        )
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
