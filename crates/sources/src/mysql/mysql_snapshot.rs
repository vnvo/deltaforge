//! MySQL consistent snapshot engine.
//!
//! Initial load from one anchor, taken without any lock on the source
//! ([`acquire_anchor`]):
//!
//! 1. The plan is verified live against the catalog between two status
//!    reads D0 and E0, and a binlog scan of (D0, E0] proves nothing that the
//!    plan depends on changed meanwhile (else the anchor is taken again).
//!    E0 is the anchor. Then a bounded pool of `min(max_parallel_tables,
//!    pending)` worker connections opens at once, each `START TRANSACTION
//!    WITH CONSISTENT SNAPSHOT` under `REPEATABLE READ`, bounded by
//!    `snapshot.anchor_timeout_secs`. Every view contains everything in E0;
//!    a transaction committed after E0 may be in a view and is streamed
//!    again (a bounded overlap, at least once), never lost.
//! 2. Each worker reads its bucket of tables sequentially under its single
//!    consistent snapshot and commits once when the bucket is done, so the
//!    connection count is bounded by the worker count, not the table count.
//!    Single-integer-PK tables use PK-range chunking; others full-scan.
//! 3. A generation lives and dies with its views: losing one (a worker
//!    connection, a DDL of a table its worker has yet to read, the process)
//!    loses the generation, which the next start copies again in full.
//! 4. The stream starts at E0's GTID set, so streaming picks up everything
//!    no view was guaranteed to hold.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use checkpoints::CheckpointStore;
use deltaforge_config::SnapshotCfg;
use deltaforge_core::{
    Event, EventId, Op, SourceInfo, SourceItem, SourcePosition,
};
use metrics::counter;
use mysql_async::{Conn, Opts, Row, prelude::Queryable};
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
    /// The configured table patterns (the anchor re-reads the catalog to
    /// verify the plan).
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
    /// The source's control pool: each check is a health connection of it.
    pub control: super::mysql_control::ControlPool,
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
    use super::mysql_control::Class;
    use crate::snapshot_driver::GuardFinding;
    let mut conn = match g
        .control
        .connect(
            Class::Health,
            g.dsn.expose(),
            &g.expected_uuid,
            Class::Health.wait(),
        )
        .await
        .map_err(super::mysql_control::ControlError::into_session)
    {
        Ok(c) => c,
        Err(super::mysql_session::SessionError::Connect(_, e)) => {
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
    conn.release().await;
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
    /// The workers' consistent-snapshot connections, all opened after the
    /// anchor's position: each one's read view contains everything up to it
    /// (and may hold transactions just after it: the bounded overlap).
    pub workers: Vec<CopyConn>,
    pub publisher: Arc<GenerationPublisher>,
    pub generation: u64,
    pub lineage: PersistedLineage,
    /// The type-mapping version stamped on every snapshot event.
    pub type_mapping: deltaforge_core::TypeMapping,
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
            version: None,
            source_id: ctx.source_id.to_string(),
            db: String::new(),
            table: String::new(),
            pipeline: ctx.pipeline.to_string(),
            tenant: ctx.tenant.to_string(),
            generation: ctx.generation,
            lineage: ctx.lineage.clone(),
            identity: Vec::new(),
            loaded: None,
            type_mapping: ctx.type_mapping,
        };
        let (cfg, schema_loader, publisher, source_id) = (
            ctx.cfg.clone(),
            ctx.schema_loader.clone(),
            Arc::clone(&ctx.publisher),
            ctx.source_id.to_string(),
        );
        running.spawn(async move {
            // Counted for as long as this worker holds its connection.
            let (mut conn, _counted) = conn.into_parts();
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
                        version: None,
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

/// The plan the anchor verifies live, between its two status reads.
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

/// Re-read the catalog with the same paged discovery and require exactly
/// the planned tables, in order, page by page against the stored plan, and
/// every planned table's schema as planned: a table created, dropped,
/// renamed or altered since discovery fails the snapshot before any row is
/// read. MySQL's INFORMATION_SCHEMA is not read in one consistent snapshot;
/// the anchor runs this between its reads D0 and E0 and proves from the
/// binlog that nothing it could have missed changed in (D0, E0]
/// ([`acquire_anchor`]).
async fn verify_plan(conn: &mut Conn, check: &mut PlanCheck<'_>) -> Result<()> {
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

/// Rebuild the registered schema model of each of `planned` from
/// INFORMATION_SCHEMA (the loader's own fetch) and require
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

/// The snapshot workers' value-reading session settings.
pub(crate) const SNAPSHOT_SESSION: &str =
    "SET SESSION time_zone = '+00:00', character_set_results = binary";

/// A snapshot connection, counted as one of its source's snapshot-copy
/// connections until dropped (`mysql_connections`).
pub(crate) struct CopyConn {
    conn: Conn,
    _counted: super::mysql_connections::Counted,
}

impl CopyConn {
    fn new(conn: Conn, source: &str) -> Self {
        Self {
            conn,
            _counted: super::mysql_connections::Counted::new(
                source,
                super::mysql_connections::Kind::SnapshotCopy,
            ),
        }
    }
}

impl CopyConn {
    /// The connection, and its count (dropped when the connection is).
    fn into_parts(self) -> (Conn, super::mysql_connections::Counted) {
        (self.conn, self._counted)
    }
}

impl std::ops::Deref for CopyConn {
    type Target = Conn;
    fn deref(&self) -> &Conn {
        &self.conn
    }
}

impl std::ops::DerefMut for CopyConn {
    fn deref_mut(&mut self) -> &mut Conn {
        &mut self.conn
    }
}

/// Anchor attempts refused by a plan-moving statement are retried at least
/// this many times, and for at least [`ANCHOR_REFUSED_FOR`].
const ANCHOR_ATTEMPTS: u32 = 3;
const ANCHOR_REFUSED_FOR: Duration = Duration::from_secs(30);

/// Test-only: the attempts the last anchor took.
#[cfg(test)]
pub(crate) static LAST_ANCHOR_ATTEMPTS: std::sync::atomic::AtomicU32 =
    std::sync::atomic::AtomicU32::new(0);

/// Establish a generation's anchor without any lock on the source
/// (`docs/src/sources/mysql.md`, "Snapshot (initial load)"):
///
/// 1. D0: the server status (executed GTID set), on a verified connection.
/// 2. The plan verified live against the catalog ([`verify_plan`]).
/// 3. E0: the status again. Every GTID in E0 is committed in the engine (a
///    GTID enters the executed set only after its engine commit), so every
///    read view opened after this read contains it.
/// 4. `num_workers` worker connections opened at once, each verified,
///    `REPEATABLE READ`, its session pinned and its consistent snapshot
///    started, within `timeout_dur`.
/// 5. One binlog scan of (D0, E0]: a statement that can change the planned
///    set or a planned table's shape ([`moves_plan`]) refuses the attempt.
///    The verification in step 2 then may have missed it; the workers are
///    closed and the anchor is taken again, at least [`ANCHOR_ATTEMPTS`]
///    times and for [`ANCHOR_REFUSED_FOR`], then the source stops with a
///    `mysql_snapshot_anchor_refused` incident.
///
/// The anchor is E0, where the stream starts. A transaction committed after
/// E0 and visible to a worker's snapshot is in that worker's rows and again
/// in the stream (a bounded overlap, at least once); one not visible is
/// streamed only. None is lost. A DDL of a planned table after E0 makes its
/// worker's read fail (the generation is lost), and the anchor baselines
/// prove every table over (E0, S].
#[allow(clippy::too_many_arguments)]
pub(crate) async fn acquire_anchor<'a>(
    source: &str,
    dsn: &str,
    expected_uuid: &str,
    lineage_hash: &str,
    patterns: &[String],
    num_workers: usize,
    timeout_dur: Duration,
    mut check: impl FnMut() -> PlanCheck<'a>,
) -> Result<(Vec<CopyConn>, MySqlCheckpoint)> {
    use super::mysql_binlog_scan::{
        ProofKind, ScanLimits, ScanTag, read_position, scan_interval,
    };
    let opts = Opts::from_url(dsn).context("parse mysql dsn")?;
    let verify = |e: super::mysql_session::SessionError| {
        anyhow::Error::new(e.into_source_error(expected_uuid))
    };
    let proof = |e: super::mysql_binlog_scan::ProofError| {
        anyhow!("snapshot anchor: {e}")
    };
    let mut conn = CopyConn::new(
        Conn::new(opts.clone())
            .await
            .context("connect anchor connection")?,
        source,
    );
    super::mysql_session::verify_connection(&mut conn, expected_uuid)
        .await
        .map_err(verify)?;
    let lctn: u8 = conn
        .query_first("SELECT @@lower_case_table_names")
        .await
        .context("read lower_case_table_names")?
        .context("no lower_case_table_names")?;
    let allow = common::AllowList::new(patterns);
    let server_id = super::mysql_helpers::derive_server_id(&format!(
        "{source}/snapshot-anchor"
    ));
    let tag = ScanTag {
        source_id: source,
        kind: ProofKind::SnapshotAnchor,
    };
    let first = Instant::now();
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        let d0 = read_position(&mut conn, true, lineage_hash)
            .await
            .map_err(proof)?;
        verify_plan(&mut conn, &mut check()).await?;
        crate::snapshot_probe::after_plan_verified().await;
        let e0 = read_position(&mut conn, true, lineage_hash)
            .await
            .map_err(proof)?;
        crate::snapshot_probe::after_anchor_position().await;
        let workers = match timeout(
            timeout_dur,
            open_workers(&opts, source, expected_uuid, num_workers),
        )
        .await
        {
            Ok(w) => w?,
            Err(_) => {
                return Err(anyhow!(
                    "the snapshot workers' connections did not open within \
                     {timeout_dur:?}; increase snapshot.anchor_timeout_secs \
                     or retry when the source accepts connections faster"
                ));
            }
        };
        let report = scan_interval(
            dsn,
            server_id,
            expected_uuid,
            lineage_hash,
            &d0,
            &e0,
            &ScanLimits::default(),
            &tag,
        )
        .await
        .map_err(proof)?;
        let Some(moved) = report
            .statements
            .iter()
            .find_map(|st| moves_plan(st, &allow, lctn))
        else {
            drop(conn);
            #[cfg(test)]
            LAST_ANCHOR_ATTEMPTS
                .store(attempt, std::sync::atomic::Ordering::SeqCst);
            return Ok((workers, e0));
        };
        drop(workers);
        warn!(source_id = %source, attempt, %moved, "a statement that can change the snapshot plan ran while the anchor was taken; taking it again");
        if attempt >= ANCHOR_ATTEMPTS && first.elapsed() >= ANCHOR_REFUSED_FOR {
            return Err(anyhow::Error::new(
                crate::incident_drafts::mysql_snapshot_anchor_refused(
                    source, attempt, &moved, &e0,
                ),
            ));
        }
        tokio::time::sleep(
            Duration::from_millis(100 * u64::from(attempt))
                .min(Duration::from_secs(2)),
        )
        .await;
    }
}

/// Open `n` snapshot workers at once: each verified, `REPEATABLE READ`, its
/// session pinned, its consistent snapshot started.
async fn open_workers(
    opts: &Opts,
    source: &str,
    expected_uuid: &str,
    n: usize,
) -> Result<Vec<CopyConn>> {
    let mut opening = tokio::task::JoinSet::new();
    for _ in 0..n {
        let (opts, source, uuid) =
            (opts.clone(), source.to_string(), expected_uuid.to_string());
        opening.spawn(async move {
            let mut c = CopyConn::new(
                Conn::new(opts).await.context("connect snapshot worker")?,
                &source,
            );
            super::mysql_session::verify_connection(&mut c, &uuid)
                .await
                .map_err(|e| anyhow::Error::new(e.into_source_error(&uuid)))?;
            c.query_drop(
                "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
            )
            .await
            .context("set repeatable read")?;
            // Values are read as stored: TIMESTAMP in UTC (no session zone
            // can move an instant), text as the column's own bytes (decoded
            // by its character set, like the binlog's).
            c.query_drop(SNAPSHOT_SESSION)
                .await
                .context("pin the snapshot session")?;
            c.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
                .await
                .context("start consistent snapshot")?;
            Ok::<_, anyhow::Error>(c)
        });
    }
    let mut workers = Vec::with_capacity(n);
    while let Some(joined) = opening.join_next().await {
        workers.push(joined.context("snapshot worker task")??);
    }
    Ok(workers)
}

/// What a statement in (D0, E0] changed that the plan depends on, if
/// anything: a table the patterns match (created, dropped, renamed or
/// altered), a database they can match, or anything the classifier cannot
/// attribute. With `lower_case_table_names = 2` names cannot be compared
/// with certainty, so any shape change counts.
pub(crate) fn moves_plan(
    st: &super::mysql_binlog_scan::Statement,
    allow: &common::AllowList,
    lctn: u8,
) -> Option<String> {
    use super::mysql_ddl_attribution::{BarrierScopeOf, DdlEffect};
    let matches = |db: &str, table: &str| {
        allow.matches(db, table)
            || (lctn != 0
                && allow.matches(&db.to_lowercase(), &table.to_lowercase()))
    };
    match &st.effect {
        DdlEffect::None | DdlEffect::SameShape(_) => None,
        DdlEffect::Barrier(BarrierScopeOf::Lineage) => {
            Some("a statement no table can be attributed to".into())
        }
        _ if lctn == 2 => Some(format!("{:?}", st.effect)),
        DdlEffect::Barrier(BarrierScopeOf::Database(d)) => (allow
            .may_match_qualifier(d)
            || (lctn != 0 && allow.may_match_qualifier(&d.to_lowercase())))
        .then(|| format!("a statement on database {d}")),
        DdlEffect::Tables(tables) => tables
            .iter()
            .find(|t| matches(&t.db, &t.table))
            .map(|t| format!("a DDL of {}.{}", t.db, t.table)),
    }
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
    source_id: String,
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
    /// Loaded schema version with its decoder, populated in `run` before
    /// any scan.
    loaded: Option<Arc<super::mysql_schema_loader::LoadedSchema>>,
    /// The type-mapping version stamped on every event.
    type_mapping: deltaforge_core::TypeMapping,
    /// Its registered version (fingerprint, registry sequence), stamped on
    /// every snapshot event.
    version: Option<(String, u64)>,
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
        // Retain the schema version: its decoder reads the rows, and its
        // columns direct identity extraction.
        self.shape.loaded = Some(Arc::clone(&loaded));
        self.shape.version =
            Some((loaded.fingerprint.to_string(), loaded.sequence));

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
            deltaforge_core::table_metrics::with_table(
                vec![metrics::Label::new(
                    "pipeline",
                    self.shape.pipeline.clone()
                )],
                deltaforge_core::table_metrics::for_pipeline(
                    &self.shape.pipeline
                )
                .label(&table_fqn)
                .as_ref(),
            )
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
                .exec(
                    format!(
                        "SELECT {} FROM `{}`.`{}` WHERE `{pk_col}` >= ? AND `{pk_col}` < ?",
                        self.shape.select_list()?,
                        self.shape.db,
                        self.shape.table
                    ),
                    (cursor, end),
                )
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
                .exec(
                    format!(
                        "SELECT {} FROM `{}`.`{}` WHERE `{pk_col}` >= ? AND `{pk_col}` {op} ?",
                        self.shape.select_list()?,
                        self.shape.db,
                        self.shape.table
                    ),
                    (cursor, cb.upper),
                )
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
            .exec_iter(
                format!(
                    "SELECT {} FROM `{}`.`{}`",
                    self.shape.select_list()?,
                    self.shape.db,
                    self.shape.table
                ),
                (),
            )
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
    fn loaded(&self) -> Result<&super::mysql_schema_loader::LoadedSchema> {
        self.loaded
            .as_deref()
            .ok_or_else(|| anyhow!("schema not loaded before scan"))
    }

    /// The scan's select list: every column of the decoding schema in
    /// order (ENUM and SET as their stored number).
    fn select_list(&self) -> Result<String> {
        Ok(self.loaded()?.decoder.snapshot_select_list())
    }

    /// Build snapshot events for a scanned chunk's rows, each decoded by the
    /// schema version's decoder (the one CDC uses).
    fn rows_to_events(&self, rows: Vec<Row>) -> Result<Vec<Event>> {
        let loaded = self.loaded()?;
        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let after = loaded
                .decoder
                .decode_snapshot_row(&row.unwrap())
                .map_err(|e| {
                    anyhow::Error::new(
                        crate::incident_drafts::source_decode_failed(
                            &self.source_id,
                            &fqn(&self.db, &self.table),
                            &e.column,
                            &e.column_type,
                            &e.reason,
                        ),
                    )
                })?;
            let id = self.provisional_id(&loaded.schema, &after)?;
            events.push(self.make_event(after, id));
        }
        Ok(events)
    }

    /// Compute the provisional snapshot [`EventId`] from the row's decoded
    /// identity values (schema-directed) and the allocated generation.
    fn provisional_id(
        &self,
        schema: &super::MySqlTableSchema,
        row: &deltaforge_core::CanonicalRow,
    ) -> Result<EventId> {
        let mut values: Vec<OwnedIdentityValue> =
            Vec::with_capacity(self.identity.len());
        for name in &self.identity {
            let col = schema
                .column(name)
                .ok_or_else(|| anyhow!("identity column {name:?} missing"))?;
            let val = row.get(name).ok_or_else(|| {
                anyhow!("identity column {name:?} not present in row")
            })?;
            let cell = mysql_identity_cell(col, val)
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

    fn make_event(
        &self,
        after: deltaforge_core::CanonicalRow,
        event_id: EventId,
    ) -> Event {
        let size = after.approx_size();
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
            self.type_mapping,
            now,
            size,
        );
        ev.tenant_id = Some(self.tenant.clone());
        if let Some((fingerprint, sequence)) = &self.version {
            ev.schema_version = Some(fingerprint.clone());
            ev.schema_sequence = Some(*sequence);
        }
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
mod plan_moving_tests {
    //! Which statements in (D0, E0] refuse an anchor ([`moves_plan`]).
    use super::*;
    use crate::mysql::mysql_activation::EventIdentity;
    use crate::mysql::mysql_binlog_scan::Statement;
    use crate::mysql::mysql_ddl_attribution::{
        BarrierScopeOf, DdlEffect, TableName,
    };

    fn st(effect: DdlEffect) -> Statement {
        Statement {
            event: EventIdentity::Gtid {
                gtid: "3e11fa47-71ca-11e1-9e33-c80aa9429562:7".into(),
                ordinal: 0,
            },
            effect,
        }
    }

    fn tables(names: &[(&str, &str)]) -> DdlEffect {
        DdlEffect::Tables(
            names
                .iter()
                .map(|(db, table)| TableName {
                    db: db.to_string(),
                    table: table.to_string(),
                })
                .collect(),
        )
    }

    fn allow(patterns: &[&str]) -> common::AllowList {
        common::AllowList::from_strs(patterns)
    }

    #[test]
    fn a_ddl_of_a_matched_table_moves_the_plan_and_others_do_not() {
        let a = allow(&["shop.ord%"]);
        assert!(
            moves_plan(&st(tables(&[("shop", "orders")])), &a, 0).is_some()
        );
        assert!(moves_plan(&st(tables(&[("shop", "users")])), &a, 0).is_none());
        // A rename names both sides: into the patterns moves the plan.
        assert!(
            moves_plan(
                &st(tables(&[("shop", "tmp"), ("shop", "orders_v2")])),
                &a,
                0
            )
            .is_some()
        );
        assert!(moves_plan(&st(DdlEffect::None), &a, 0).is_none());
        assert!(
            moves_plan(
                &st(DdlEffect::SameShape(vec![TableName {
                    db: "shop".into(),
                    table: "orders".into(),
                }])),
                &a,
                0
            )
            .is_none()
        );
    }

    #[test]
    fn a_statement_on_a_database_moves_the_plan_when_patterns_can_match_it() {
        let db = |d: &str| {
            st(DdlEffect::Barrier(BarrierScopeOf::Database(d.into())))
        };
        assert!(moves_plan(&db("shop"), &allow(&["shop.orders"]), 0).is_some());
        assert!(
            moves_plan(&db("audit"), &allow(&["shop.orders"]), 0).is_none()
        );
        // A pattern without a database matches tables of any database.
        assert!(moves_plan(&db("audit"), &allow(&["orders"]), 0).is_some());
        assert!(
            moves_plan(
                &st(DdlEffect::Barrier(BarrierScopeOf::Lineage)),
                &allow(&["shop.orders"]),
                0
            )
            .is_some()
        );
    }

    #[test]
    fn names_are_compared_as_the_server_stores_them() {
        let a = allow(&["shop.orders"]);
        let mixed = st(tables(&[("Shop", "Orders")]));
        // 0: case-sensitive, as written.
        assert!(moves_plan(&mixed, &a, 0).is_none());
        // 1: stored lower case.
        assert!(moves_plan(&mixed, &a, 1).is_some());
        let db =
            st(DdlEffect::Barrier(BarrierScopeOf::Database("SHOP".into())));
        assert!(moves_plan(&db, &a, 0).is_none());
        assert!(moves_plan(&db, &a, 1).is_some());
        // 2: not comparable with certainty, so any shape change counts.
        let other = st(tables(&[("audit", "log")]));
        assert!(moves_plan(&other, &a, 2).is_some());
        assert!(moves_plan(&st(DdlEffect::None), &a, 2).is_none());
    }
}

#[cfg(test)]
mod live_anchor_tests {
    //! The lock-free anchor on a throwaway MySQL 8.4, through a user without
    //! `RELOAD`: nothing it does waits for, or holds back, other sessions;
    //! its position precedes every worker's view; a statement that moves the
    //! plan in (D0, E0] refuses the attempt.
    use super::*;
    use deltaforge_core::incident::ReasonCode;
    use gate_ownership::GateOwned;
    use mysql_async::prelude::Queryable;
    use std::sync::atomic::Ordering;
    use testcontainers::core::WaitFor;
    use testcontainers::runners::AsyncRunner;
    use testcontainers::{ContainerAsync, GenericImage, ImageExt};
    use tokio::sync::OnceCell;

    static SERVER: OnceCell<(ContainerAsync<GenericImage>, u16)> =
        OnceCell::const_new();
    const LINEAGE: &str = "anchor-lineage";

    async fn port() -> u16 {
        SERVER
            .get_or_init(|| async {
                let c = GenericImage::new("mysql", "8.4")
                    .with_wait_for(WaitFor::message_on_stderr(
                        "ready for connections. Version: '8.4",
                    ))
                    .with_env_var("MYSQL_ROOT_PASSWORD", "pw")
                    .with_cmd([
                        "--server-id=61",
                        "--log-bin=mysql-bin",
                        "--binlog-format=ROW",
                        "--gtid-mode=ON",
                        "--enforce-gtid-consistency=ON",
                    ])
                    .gate_owned()
                    .start()
                    .await
                    .expect("start mysql");
                let port = c.get_host_port_ipv4(3306).await.unwrap();
                let mut admin = None;
                for _ in 0..60 {
                    if let Ok(a) = Conn::from_url(root(port)).await {
                        admin = Some(a);
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                let mut admin = admin.expect("mysql up");
                for sql in [
                    "CREATE USER 'anc'@'%' IDENTIFIED BY 'pw'",
                    "GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* \
                     TO 'anc'@'%'",
                ] {
                    admin.query_drop(sql).await.unwrap();
                }
                (c, port)
            })
            .await
            .1
    }

    fn root(port: u16) -> String {
        format!("mysql://root:pw@127.0.0.1:{port}/")
    }

    fn user(port: u16) -> String {
        format!("mysql://anc:pw@127.0.0.1:{port}/")
    }

    async fn sql(port: u16, stmts: &[&str]) {
        let mut c = Conn::from_url(root(port)).await.unwrap();
        for s in stmts {
            c.query_drop(*s)
                .await
                .unwrap_or_else(|e| panic!("{s}: {e}"));
        }
        c.disconnect().await.ok();
    }

    async fn uuid(port: u16) -> String {
        let mut c = Conn::from_url(root(port)).await.unwrap();
        c.query_first("SELECT @@GLOBAL.server_uuid")
            .await
            .unwrap()
            .unwrap()
    }

    /// A database `db` with table `t`, and the anchor over `db.*`.
    async fn prepare(port: u16, db: &str) {
        sql(
            port,
            &[
                &format!("DROP DATABASE IF EXISTS {db}"),
                &format!("CREATE DATABASE {db}"),
                &format!("CREATE TABLE {db}.t (id INT PRIMARY KEY)"),
                &format!("INSERT INTO {db}.t VALUES (1)"),
            ],
        )
        .await;
    }

    async fn anchor(
        port: u16,
        db: &str,
        workers: usize,
    ) -> Result<(Vec<CopyConn>, MySqlCheckpoint)> {
        acquire_anchor(
            "anchor-test",
            &user(port),
            &uuid(port).await,
            LINEAGE,
            &[format!("{db}.*")],
            workers,
            Duration::from_secs(10),
            PlanCheck::nothing,
        )
        .await
    }

    /// A long read of a planned table neither delays the anchor nor is
    /// waited for, and a write inside the anchor (between its plan
    /// verification and its position) commits at once: no lock is held.
    #[tokio::test]
    #[ignore = "requires docker"]
    async fn the_anchor_neither_waits_for_nor_blocks_other_sessions() {
        let port = port().await;
        let db = "anc_nolock";
        prepare(port, db).await;
        let mut long = Conn::from_url(root(port)).await.unwrap();
        let read = format!("SELECT SLEEP(10) FROM {db}.t");
        let long = tokio::spawn(async move { long.query_drop(read).await });
        tokio::time::sleep(Duration::from_millis(300)).await;
        crate::snapshot_probe::run_after_plan_verified(
            &root(port),
            vec![format!("INSERT INTO {db}.t VALUES (2)")],
            1,
        );
        let t = Instant::now();
        let (workers, _) = anchor(port, db, 4).await.expect("anchor");
        assert!(
            t.elapsed() < Duration::from_secs(5),
            "the anchor waited {:?} (for the long read?)",
            t.elapsed()
        );
        assert_eq!(workers.len(), 4);
        assert!(!long.is_finished(), "the long read was not held back");
        long.abort();
    }

    /// The position precedes every worker's view: a commit before it is in
    /// every view and in it; one after it (before the workers open) is in
    /// the views but not in the position, so the stream sends it again (the
    /// bounded overlap), never neither.
    #[tokio::test]
    #[ignore = "requires docker"]
    async fn the_position_precedes_every_workers_view() {
        let port = port().await;
        let db = "anc_overlap";
        prepare(port, db).await;
        crate::snapshot_probe::run_after_anchor_position(
            &root(port),
            vec![format!("INSERT INTO {db}.t VALUES (3)")],
            1,
        );
        let (mut workers, e0) = anchor(port, db, 3).await.expect("anchor");
        let mut c = Conn::from_url(root(port)).await.unwrap();
        let after: String = c
            .query_first("SELECT @@GLOBAL.gtid_executed")
            .await
            .unwrap()
            .unwrap();
        let e0 =
            crate::gtid_set::GtidSet::parse(e0.gtid_set.as_deref().unwrap())
                .unwrap();
        let after = crate::gtid_set::GtidSet::parse(&after).unwrap();
        assert!(!after.is_subset(&e0), "the overlap commit is after E0");
        for w in workers.iter_mut() {
            let ids: Vec<i32> = w
                .query(format!("SELECT id FROM {db}.t ORDER BY id"))
                .await
                .unwrap();
            assert_eq!(ids, vec![1, 3], "the view holds E0 and the overlap");
        }
    }

    /// A table created and dropped under the patterns in (D0, E0], unseen
    /// by the verification, refuses the attempt; the next one succeeds.
    #[tokio::test]
    #[ignore = "requires docker"]
    async fn a_plan_moving_statement_refuses_the_attempt() {
        let port = port().await;
        let db = "anc_moved";
        prepare(port, db).await;
        crate::snapshot_probe::run_after_plan_verified(
            &root(port),
            vec![
                format!("CREATE TABLE {db}.brief (id INT PRIMARY KEY)"),
                format!("DROP TABLE {db}.brief"),
            ],
            1,
        );
        anchor(port, db, 1).await.expect("the retry anchors");
        assert_eq!(LAST_ANCHOR_ATTEMPTS.load(Ordering::SeqCst), 2);
    }

    /// The same statements outside the patterns refuse nothing.
    #[tokio::test]
    #[ignore = "requires docker"]
    async fn a_statement_outside_the_patterns_refuses_nothing() {
        let port = port().await;
        let db = "anc_outside";
        prepare(port, db).await;
        sql(port, &["CREATE DATABASE IF NOT EXISTS anc_other"]).await;
        crate::snapshot_probe::run_after_plan_verified(
            &root(port),
            vec![
                "CREATE TABLE anc_other.brief (id INT PRIMARY KEY)".into(),
                "DROP TABLE anc_other.brief".into(),
            ],
            1,
        );
        anchor(port, db, 1).await.expect("anchors");
        assert_eq!(LAST_ANCHOR_ATTEMPTS.load(Ordering::SeqCst), 1);
    }

    /// Under concurrent writers (each row carries its own GTID number) and
    /// a plan-moving statement now and then, 100 anchors: every row whose
    /// GTID is in E0 is in every worker's view (none lost), the rest of a
    /// view is the bounded overlap, and no write waits on an anchor.
    #[tokio::test]
    #[ignore = "requires docker"]
    async fn anchors_under_load_never_lose_a_committed_row() {
        use std::sync::atomic::{AtomicBool, AtomicU64};
        let port = port().await;
        let db = "anc_load";
        sql(
            port,
            &[
                &format!("DROP DATABASE IF EXISTS {db}"),
                &format!("CREATE DATABASE {db}"),
                &format!("CREATE TABLE {db}.t (id BIGINT PRIMARY KEY)"),
            ],
        )
        .await;
        let server = uuid(port).await;
        // Explicit GTIDs from a fresh UUID: row id = GTID number.
        let tag = "aaaaaaaa-0000-4000-8000-000000000001";
        let next = Arc::new(AtomicU64::new(1));
        let stop = Arc::new(AtomicBool::new(false));
        let mut writers = Vec::new();
        for _ in 0..4 {
            let (next, stop) = (Arc::clone(&next), Arc::clone(&stop));
            let mut c = Conn::from_url(root(port)).await.unwrap();
            writers.push(tokio::spawn(async move {
                let mut worst = Duration::ZERO;
                while !stop.load(Ordering::Relaxed) {
                    let n = next.fetch_add(1, Ordering::SeqCst);
                    let t = Instant::now();
                    c.query_drop(format!("SET GTID_NEXT = '{tag}:{n}'"))
                        .await
                        .unwrap();
                    c.query_drop(format!("INSERT INTO {db}.t VALUES ({n})"))
                        .await
                        .unwrap();
                    c.query_drop("SET GTID_NEXT = 'AUTOMATIC'").await.unwrap();
                    worst = worst.max(t.elapsed());
                }
                worst
            }));
        }
        let mover = {
            let stop = Arc::clone(&stop);
            let mut c = Conn::from_url(root(port)).await.unwrap();
            tokio::spawn(async move {
                let mut k = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    k += 1;
                    c.query_drop(format!("CREATE TABLE {db}.m{k} (id INT)"))
                        .await
                        .unwrap();
                    c.query_drop(format!("DROP TABLE {db}.m{k}"))
                        .await
                        .unwrap();
                    tokio::time::sleep(Duration::from_millis(300)).await;
                }
                k
            })
        };
        tokio::time::sleep(Duration::from_millis(500)).await;
        let (mut overlaps, mut refused_attempts, mut anchor_time) =
            (Vec::new(), 0u32, Duration::ZERO);
        for _ in 0..100 {
            let t = Instant::now();
            let (mut workers, e0) = acquire_anchor(
                "anchor-test",
                &user(port),
                &server,
                LINEAGE,
                &[format!("{db}.*")],
                2,
                Duration::from_secs(10),
                PlanCheck::nothing,
            )
            .await
            .expect("anchor");
            anchor_time = anchor_time.max(t.elapsed());
            refused_attempts += LAST_ANCHOR_ATTEMPTS.load(Ordering::SeqCst) - 1;
            let e0 = crate::gtid_set::GtidSet::parse(
                e0.gtid_set.as_deref().unwrap(),
            )
            .unwrap();
            for w in workers.iter_mut() {
                let ids: Vec<u64> =
                    w.query(format!("SELECT id FROM {db}.t")).await.unwrap();
                let seen: std::collections::HashSet<u64> =
                    ids.iter().copied().collect();
                let hi = next.load(Ordering::SeqCst);
                let in_e0: Vec<u64> = (1..hi)
                    .filter(|n| e0.contains(&format!("{tag}:{n}")))
                    .collect();
                let lost: Vec<&u64> =
                    in_e0.iter().filter(|n| !seen.contains(n)).collect();
                assert!(
                    lost.is_empty(),
                    "rows in E0 missing from a view: {lost:?}"
                );
                let overlap = ids
                    .iter()
                    .filter(|n| !e0.contains(&format!("{tag}:{n}")))
                    .count();
                overlaps.push(overlap);
            }
        }
        stop.store(true, Ordering::Relaxed);
        let mut worst = Duration::ZERO;
        for w in writers {
            worst = worst.max(w.await.unwrap());
        }
        let moved = mover.await.unwrap();
        overlaps.sort_unstable();
        println!(
            "anchors=100 rows={} moves={moved} refused_attempts={refused_attempts} \
             overlap_rows p50={} p99={} max={} anchor_max_ms={} \
             writer_worst_ms={}",
            next.load(Ordering::SeqCst) - 1,
            overlaps[overlaps.len() / 2],
            overlaps[overlaps.len() * 99 / 100],
            overlaps.last().unwrap(),
            anchor_time.as_millis(),
            worst.as_millis(),
        );
        assert!(
            worst < Duration::from_millis(500),
            "a write waited {worst:?} while anchors ran"
        );
    }

    /// A plan kept moving refuses every attempt: at least three, for at
    /// least 30 s, then the `mysql_snapshot_anchor_refused` incident.
    #[tokio::test]
    #[ignore = "requires docker"]
    async fn a_plan_kept_moving_raises_the_incident() {
        let port = port().await;
        let db = "anc_storm";
        prepare(port, db).await;
        crate::snapshot_probe::run_after_plan_verified(
            &root(port),
            vec![format!("ALTER TABLE {db}.t COMMENT 'moved'")],
            usize::MAX,
        );
        let t = Instant::now();
        let err = anchor(port, db, 1).await.err().expect("refused");
        crate::snapshot_probe::run_after_plan_verified(&root(port), vec![], 0);
        assert!(t.elapsed() >= ANCHOR_REFUSED_FOR, "{:?}", t.elapsed());
        let err = err.downcast::<deltaforge_core::SourceError>().unwrap();
        let draft = err.draft().expect("an incident");
        assert_eq!(draft.reason_code, ReasonCode::MysqlSnapshotAnchorRefused);
        assert!(format!("{err:#}").contains("snapshot_plan_ddl"), "{err:#}");
    }
}
