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

use anyhow::{Context, Result, anyhow, bail};
use deltaforge_config::SnapshotCfg;
use deltaforge_core::{
    Event, EventId, IdentityKind, Op, SourceInfo, SourcePosition,
};
use std::collections::HashMap;

use super::postgres_identity::{PgIdentityRaw, pg_identity_cell, quote_ident};
use crate::durable_checkpoint::CursorKind;
use crate::snapshot_driver::{GuardFinding, anchor_age_finding};
use crate::snapshot_event_id::{OwnedIdentityValue, snapshot_row_event_id};
use crate::snapshot_generation::PersistedLineage;
use crate::snapshot_permits::{ExtraPermit, SnapshotPermits};
use crate::snapshot_publish::GenerationPublisher;
use crate::snapshot_queue::{PlanItem, QueueStore};
use metrics::counter;
use scopeguard;
use serde::{Deserialize, Serialize};
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use super::postgres_schema_loader::PostgresSchemaLoader;

// ============================================================================
// Snapshot progress (persisted for resume on crash)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SnapshotProgress {
    /// The anchor LSN: the replication slot's consistent point (PG-A-lite).
    pub start_lsn: String,
    /// Tables that have been fully snapshotted ("schema.table"); a JSON
    /// array of names, as always.
    pub done_tables: std::collections::BTreeSet<String>,
    /// True once every table is complete.
    pub finished: bool,
    /// Anchor protocol version. 0 (default, for records written before this
    /// milestone) = legacy `pg_current_wal_lsn` anchor that could lose rows
    /// committed in the snapshot->CDC seam. `SNAPSHOT_ANCHOR_VERSION` = the
    /// slot-consistent-point anchor. Used to flag completed legacy snapshots.
    #[serde(default)]
    pub anchor_version: u32,
}

/// Current snapshot-anchor protocol version (slot-consistent-point anchor).
pub const SNAPSHOT_ANCHOR_VERSION: u32 = 1;

impl SnapshotProgress {
    pub fn table_done(&self, schema: &str, table: &str) -> bool {
        self.done_tables.contains(&fqn(schema, table))
    }

    pub fn mark_done(&mut self, schema: &str, table: &str) {
        self.done_tables.insert(fqn(schema, table));
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

/// One identity column: its name and the canonical kind (for null-tagging).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdentitySpec {
    pub name: String,
    pub kind: IdentityKind,
}

/// The `, t."col"...` suffix that adds native identity columns to a snapshot
/// SELECT (column 0 is always the `row_to_json` payload).
fn identity_select_suffix(specs: &[IdentitySpec]) -> String {
    specs
        .iter()
        .map(|s| format!(", t.{}", quote_ident(&s.name)))
        .collect()
}

/// The single, shared identity-extraction path used by **all** scan strategies
/// (PK sequential, PK parallel, ctid). Reads the native binary identity columns
/// (indices `1..=specs.len()`) and mints the provisional snapshot id.
fn provisional_snapshot_id(
    row: &tokio_postgres::Row,
    specs: &[IdentitySpec],
    lineage: &PersistedLineage,
    generation: u64,
    schema: &str,
    table: &str,
) -> Result<EventId> {
    let expected = 1 + specs.len();
    if row.len() != expected {
        bail!(
            "snapshot row has {} columns, expected {expected} (payload + {} \
             identity columns)",
            row.len(),
            specs.len()
        );
    }
    let mut values = Vec::with_capacity(specs.len());
    for (i, spec) in specs.iter().enumerate() {
        // ctid is never an identity input; identity columns are explicit.
        let raw: Option<PgIdentityRaw> = row
            .try_get(1 + i)
            .with_context(|| format!("read identity column {:?}", spec.name))?;
        let cell = match raw {
            Some(r) => pg_identity_cell(&r)
                .map_err(|e| anyhow!("identity column {}: {e}", spec.name))?,
            None => {
                crate::snapshot_event_id::OwnedIdentityCell::Null(spec.kind)
            }
        };
        values.push(OwnedIdentityValue {
            name: spec.name.clone(),
            cell,
        });
    }
    Ok(snapshot_row_event_id(
        &lineage.as_source_lineage(),
        generation,
        schema,
        table,
        &values,
    ))
}

/// What one generation's copy needs (`docs/design/snapshot-durable-queue.md`,
/// sections 2 and 9). The copy runs inside the read view the coordinator
/// exported (`snapshot_id`): every worker imports it, and a failed table is
/// copied again only while that view lives.
pub struct PgCopyCtx<'a> {
    pub dsn: &'a str,
    pub source_id: &'a str,
    pub pipeline: &'a str,
    pub tenant: &'a str,
    pub cfg: &'a SnapshotCfg,
    pub schema_loader: &'a PostgresSchemaLoader,
    pub cancel: CancellationToken,
    pub queue: &'a QueueStore,
    pub publisher: Arc<GenerationPublisher>,
    pub permits: Arc<SnapshotPermits>,
    /// The coordinator session holding the exported view open.
    pub coord: &'a tokio_postgres::Client,
    pub snapshot_id: &'a str,
    pub generation: u64,
    pub lineage: PersistedLineage,
}

/// How often a table whose worker failed is copied again while the
/// exported view lives.
const TABLE_RETRIES: u32 = 2;

/// A plan item as a table to copy.
fn planned(item: &PlanItem) -> Result<super::PgPlannedTable> {
    let identity: Vec<IdentitySpec> =
        serde_json::from_value(item.identity.clone()).with_context(|| {
            format!(
                "plan item {}.{}: identity columns",
                item.qualifier, item.table
            )
        })?;
    Ok(super::PgPlannedTable {
        qualifier: item.qualifier.clone(),
        table: item.table.clone(),
        identity,
        cursor_kind: item.cursor_kind,
        signature: item.signature.clone(),
    })
}

/// Copy every table of the generation's sealed plan, read page by page from
/// the store. At most `max_parallel_tables` tables run at once: the first
/// on the snapshot's base permit, every other one only on an extra permit,
/// taken while the running tables keep going (never waited for before work
/// starts). A table whose worker failed is copied again (its rows may be
/// published twice: at-least-once) while the exported view lives, at most
/// [`TABLE_RETRIES`] times; otherwise the copy fails and the generation is
/// lost.
pub async fn copy_generation(ctx: &PgCopyCtx<'_>) -> Result<()> {
    let max_parallel = ctx.cfg.max_parallel_tables.max(1);
    let page = ctx.cfg.discovery_page_size.max(1);
    let workers_cancel = ctx.cancel.child_token();
    let _stop_workers =
        scopeguard::guard(workers_cancel.clone(), |c| c.cancel());
    let fetches_before = ctx.schema_loader.live_fetch_count();

    let mut running: tokio::task::JoinSet<Result<u64>> =
        tokio::task::JoinSet::new();
    // Per running task: its table, attempt and extra permit (`None`: the
    // base permit).
    let mut tasks: HashMap<
        tokio::task::Id,
        (super::PgPlannedTable, u32, Option<ExtraPermit>),
    > = HashMap::new();
    let mut base_free = true;
    let mut queue: std::collections::VecDeque<(super::PgPlannedTable, u32)> =
        std::collections::VecDeque::new();
    let mut after: Option<String> = None;
    let mut pages_done = false;

    let spawn = |running: &mut tokio::task::JoinSet<Result<u64>>,
                 tasks: &mut HashMap<_, _>,
                 table: super::PgPlannedTable,
                 attempt: u32,
                 permit: Option<ExtraPermit>| {
        let worker = TableWorker {
            dsn: crate::credentials::ProtectedDsn::from(ctx.dsn),
            schema: table.qualifier.clone(),
            table: table.table.clone(),
            snapshot_id: ctx.snapshot_id.to_string(),
            source_id: ctx.source_id.to_string(),
            pipeline: ctx.pipeline.to_string(),
            tenant: ctx.tenant.to_string(),
            cfg: ctx.cfg.clone(),
            schema_loader: ctx.schema_loader.clone(),
            cancel: workers_cancel.clone(),
            generation: ctx.generation,
            lineage: ctx.lineage.clone(),
            identity: table.identity.clone(),
            publisher: Arc::clone(&ctx.publisher),
            permits: Arc::clone(&ctx.permits),
        };
        let task = running.spawn(worker.run());
        tasks.insert(task.id(), (table, attempt, permit));
        crate::snapshot_probe::record_table_tasks(running.len());
    };

    loop {
        if queue.is_empty() && !pages_done {
            let (items, next) = ctx
                .queue
                .items_page(ctx.generation, after.as_deref(), page)
                .await?;
            for (_, item) in &items {
                queue.push_back((planned(item)?, 0));
            }
            pages_done = next.is_none();
            after = next;
        }
        let Some((table, attempt)) = queue.pop_front() else {
            if running.is_empty() {
                break;
            }
            let done = running.join_next_with_id().await.expect("running");
            finish_table(ctx, done, &mut tasks, &mut base_free, &mut queue)
                .await?;
            continue;
        };
        if base_free {
            base_free = false;
            spawn(&mut running, &mut tasks, table, attempt, None);
            continue;
        }
        if running.len() >= max_parallel {
            queue.push_front((table, attempt));
            let done = running.join_next_with_id().await.expect("running");
            finish_table(ctx, done, &mut tasks, &mut base_free, &mut queue)
                .await?;
            continue;
        }
        // Another table only on an extra permit, while the running ones
        // continue; a table finishing first frees its permit instead.
        tokio::select! {
            biased;
            done = running.join_next_with_id() => {
                queue.push_front((table, attempt));
                finish_table(
                    ctx,
                    done.expect("running"),
                    &mut tasks,
                    &mut base_free,
                    &mut queue,
                )
                .await?;
            }
            permit = ctx.permits.extra(&ctx.cancel) => {
                let permit = permit.map_err(|e| anyhow!(e))?;
                spawn(&mut running, &mut tasks, table, attempt, Some(permit));
            }
        }
    }
    crate::snapshot_probe::record_worker_live_fetches(
        ctx.schema_loader.live_fetch_count() - fetches_before,
    );
    Ok(())
}

/// Record one finished table task: a failed table is queued again while
/// the exported view lives and it has attempts left; otherwise the copy
/// fails.
async fn finish_table(
    ctx: &PgCopyCtx<'_>,
    done: std::result::Result<
        (tokio::task::Id, Result<u64>),
        tokio::task::JoinError,
    >,
    tasks: &mut HashMap<
        tokio::task::Id,
        (super::PgPlannedTable, u32, Option<ExtraPermit>),
    >,
    base_free: &mut bool,
    queue: &mut std::collections::VecDeque<(super::PgPlannedTable, u32)>,
) -> Result<()> {
    let id = match &done {
        Ok((id, _)) => *id,
        Err(e) => e.id(),
    };
    let (table, attempt, permit) =
        tasks.remove(&id).expect("every task is recorded");
    if permit.is_none() {
        *base_free = true;
    }
    drop(permit);
    let failure = match done {
        Ok((_, Ok(rows))) => {
            info!(table = %table.key(), rows, "snapshot table copied");
            return Ok(());
        }
        Ok((_, Err(e))) => format!("{e:#}"),
        Err(e) => format!("worker panicked: {e}"),
    };
    if ctx.cancel.is_cancelled() {
        bail!("snapshot cancelled");
    }
    if attempt < TABLE_RETRIES && view_alive(ctx.coord).await {
        warn!(
            table = %table.key(), attempt, error = %failure,
            "snapshot table failed; copying it again in the same read view"
        );
        queue.push_front((table, attempt + 1));
        return Ok(());
    }
    error!(table = %table.key(), error = %failure, "snapshot table failed");
    bail!("snapshot failed for {}: {failure}", table.key())
}

/// Whether the coordinator transaction that exported the read view is
/// still open (a worker may import the view again only then).
async fn view_alive(coord: &tokio_postgres::Client) -> bool {
    coord.simple_query("SELECT 1").await.is_ok()
}

/// In the exported snapshot the rows are read from, rebuild every planned
/// table's registered schema model (the loader's own fetch), one plan page
/// at a time, and require the plan's signature: a table altered, dropped or
/// replaced since the plan was made stops the generation before any row is
/// read.
pub async fn verify_plan_in_snapshot(
    coord: &tokio_postgres::Client,
    queue: &QueueStore,
    generation: u64,
    page: usize,
) -> Result<()> {
    let mut after: Option<String> = None;
    loop {
        let (items, next) = queue
            .items_page(generation, after.as_deref(), page.max(1))
            .await?;
        let keys: Vec<(&str, &str)> = items
            .iter()
            .map(|(_, t)| (t.qualifier.as_str(), t.table.as_str()))
            .collect();
        let schemas =
            super::postgres_schema_loader::fetch_tables_on(coord, &keys)
                .await
                .context("verify the plan: read the planned schemas")?;
        for (_, item) in &items {
            let name = format!("{}.{}", item.qualifier, item.table);
            let key = (item.qualifier.clone(), item.table.clone());
            let Some(schema) = schemas.get(&key) else {
                bail!(plan_moved(&name, "it no longer exists"));
            };
            if pg_schema_signature(schema) != item.signature {
                bail!(plan_moved(&name, "its schema changed"));
            }
        }
        crate::snapshot_probe::record_verification_page();
        match next {
            Some(n) => after = Some(n),
            None => break,
        }
    }
    crate::snapshot_probe::record_fixed(
        crate::snapshot_probe::FixedOp::CatalogVerification,
    );
    Ok(())
}

fn plan_moved(table: &str, what: &str) -> String {
    format!(
        "{table} changed between planning and the snapshot anchor ({what}); \
         no row was read - the next start plans a new generation"
    )
}

/// The plan signature of a PostgreSQL table: its whole registered schema
/// model, including the table OID and replica identity.
pub(crate) fn pg_schema_signature(
    schema: &crate::postgres::postgres_table_schema::PostgresTableSchema,
) -> String {
    crate::snapshot_plan::schema_signature(schema)
}

/// The bounds a running generation is held to (design section 9): its
/// slot's WAL retention and the anchor age. Approaching a limit raises a
/// non-blocking warning; reaching it blocks the generation
/// (`snapshot_anchor_unavailable`) and stops the copy, before the retained
/// log is lost.
pub struct GenerationGuard {
    pub dsn: crate::credentials::ProtectedDsn,
    pub slot: String,
    pub source_id: String,
    pub generation: u64,
    pub anchored_at_ms: i64,
    pub max_anchor_age: std::time::Duration,
    /// The run that owns the generation: only its own generation is
    /// blocked.
    pub run: String,
    /// The control version the run holds: the only one blocked at.
    pub version: Arc<std::sync::atomic::AtomicU64>,
    pub queue: QueueStore,
    pub incidents: storage::adapters::incidents::IncidentStore,
    /// Cancelled when the generation blocks.
    pub cancel: CancellationToken,
    /// Why it blocked.
    pub blocked: Arc<Mutex<Option<String>>>,
}

/// The slot's WAL retention: the slot gone or invalidated, its WAL lost or
/// no safe WAL left blocks; `unreserved`, or less than 20% of
/// `max_slot_wal_keep_size` left, warns.
fn wal_finding(
    exists: bool,
    invalidation: Option<&str>,
    wal_status: Option<&str>,
    safe_wal_size: Option<i64>,
    keep_mb: Option<i64>,
) -> GuardFinding {
    if !exists {
        return GuardFinding::Block("slot_missing");
    }
    if invalidation.is_some() {
        return GuardFinding::Block("slot_invalidated");
    }
    if wal_status == Some("lost") || safe_wal_size.is_some_and(|n| n <= 0) {
        return GuardFinding::Block("wal_lost");
    }
    let low = match (safe_wal_size, keep_mb) {
        (Some(safe), Some(mb)) if mb > 0 => safe * 5 < mb * 1024 * 1024,
        _ => false,
    };
    if wal_status == Some("unreserved") || low {
        return GuardFinding::Warn("wal_retention");
    }
    GuardFinding::Ok
}

/// One read of the slot's WAL retention.
async fn check_slot(
    dsn: &crate::credentials::ProtectedDsn,
    slot: &str,
) -> Result<GuardFinding> {
    let (client, conn) = tokio_postgres::connect(dsn.expose(), NoTls).await?;
    let task = tokio::spawn(async move {
        let _ = conn.await;
    });
    let _task = scopeguard::guard(task, |t| t.abort());
    let row = client
        .query_opt(
            &format!(
                "SELECT wal_status, safe_wal_size, {} \
                 FROM pg_replication_slots s WHERE slot_name = $1",
                super::postgres_health::INVALIDATION
            ),
            &[&slot],
        )
        .await?;
    let keep: Option<i64> = client
        .query_opt(
            "SELECT setting::bigint FROM pg_settings \
             WHERE name = 'max_slot_wal_keep_size'",
            &[],
        )
        .await?
        .map(|r| r.get(0));
    Ok(match row {
        None => wal_finding(false, None, None, None, keep),
        Some(r) => {
            let status: Option<String> = r.get(0);
            let safe: Option<i64> = r.get(1);
            let invalidation: Option<String> = r.get(2);
            wal_finding(
                true,
                invalidation.as_deref(),
                status.as_deref(),
                safe,
                keep,
            )
        }
    })
}

/// Run the guard until the generation's copy ends (the task is aborted) or
/// a bound blocks it.
pub fn spawn_generation_guard(
    g: GenerationGuard,
) -> tokio::task::JoinHandle<()> {
    use crate::snapshot_driver::incidents as drafts;
    use deltaforge_core::incident::ReasonCode;
    let every = (g.max_anchor_age / 10).clamp(
        std::time::Duration::from_millis(200),
        std::time::Duration::from_secs(30),
    );
    tokio::spawn(async move {
        let mut warned: std::collections::BTreeSet<&'static str> =
            Default::default();
        loop {
            tokio::select! {
                _ = g.cancel.cancelled() => return,
                _ = tokio::time::sleep(every) => {}
            }
            let age = std::time::Duration::from_millis(
                u64::try_from(
                    chrono::Utc::now().timestamp_millis() - g.anchored_at_ms,
                )
                .unwrap_or(0),
            );
            let slot = match check_slot(&g.dsn, &g.slot).await {
                Ok(f) => f,
                Err(e) => {
                    warn!(
                        source_id = %g.source_id, slot = %g.slot,
                        error = %format!("{e:#}"),
                        "snapshot guard: slot check failed; retrying"
                    );
                    GuardFinding::Ok
                }
            };
            for finding in [anchor_age_finding(age, g.max_anchor_age), slot] {
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
                        *g.blocked.lock().expect("not poisoned") = Some(
                            format!("snapshot_anchor_unavailable ({class})"),
                        );
                        g.cancel.cancel();
                        return;
                    }
                }
            }
        }
    })
}

// ============================================================================
// Table worker
// ============================================================================

struct TableWorker {
    dsn: crate::credentials::ProtectedDsn,
    schema: String,
    table: String,
    snapshot_id: String,
    source_id: String,
    pipeline: String,
    tenant: String,
    cfg: SnapshotCfg,
    schema_loader: PostgresSchemaLoader,
    cancel: CancellationToken,
    /// Durable snapshot generation for stable-id derivation.
    generation: u64,
    /// Frozen source lineage.
    lineage: PersistedLineage,
    /// Resolved identity columns (name + kind, identity order).
    identity: Vec<IdentitySpec>,
    publisher: Arc<GenerationPublisher>,
    /// For intra-table readers, each on an extra permit.
    permits: Arc<SnapshotPermits>,
}

/// Open a connection that reads in the exported view `snapshot_id`.
async fn import_view(
    dsn: &crate::credentials::ProtectedDsn,
    snapshot_id: &str,
    what: &str,
) -> Result<tokio_postgres::Client> {
    let (client, conn) = tokio_postgres::connect(dsn.expose(), NoTls)
        .await
        .with_context(|| format!("connect for {what}"))?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            warn!(error = %e, "snapshot reader connection dropped");
        }
    });
    client
        .batch_execute(&format!(
            "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ; \
             SET TRANSACTION SNAPSHOT '{snapshot_id}'"
        ))
        .await
        .with_context(|| format!("import the snapshot for {what}"))?;
    Ok(client)
}

impl TableWorker {
    async fn run(self) -> Result<u64> {
        info!(pipeline=%self.pipeline, source_id=%self.source_id, schema=%self.schema, table=%self.table, "snapshot worker starting");
        let fqn = fqn(&self.schema, &self.table);
        let t0 = Instant::now();

        // Every worker reads in the shared exported view.
        let client = import_view(&self.dsn, &self.snapshot_id, &fqn).await?;

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
                    "composite or missing PK - using ctid chunking"
                );
            }
            self.by_ctid(&client).await?
        };

        client.batch_execute("COMMIT").await.ok();

        counter!(
            "deltaforge_snapshot_rows_total",
            deltaforge_core::table_metrics::with_table(
                vec![metrics::Label::new("pipeline", self.pipeline.clone())],
                deltaforge_core::table_metrics::for_pipeline(&self.pipeline)
                    .label(&fqn)
                    .as_ref(),
            )
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

        // Decide parallelism: multiple concurrent readers for large tables
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
        let chunk = self.cfg.chunk_size as i64;
        let mut cursor = min_pk;
        let mut total_sent = 0u64;
        while cursor <= max_pk {
            if self.cancel.is_cancelled() {
                anyhow::bail!("snapshot cancelled");
            }
            let next = cursor.saturating_add(chunk);
            total_sent += self
                .reader()
                .read_and_publish_pk_range(client, pk_col, cursor, next)
                .await?;
            cursor = next;
        }
        Ok(total_sent)
    }

    /// Divide the PK space into `max_parallel_chunks` sub-ranges, read in
    /// chunks from one shared queue: by this worker's own connection, and
    /// by up to `max_parallel_chunks - 1` more readers, each on an extra
    /// permit free right now (never waited for: this worker reads on).
    async fn by_pk_parallel(
        &self,
        client: &tokio_postgres::Client,
        pk_col: &str,
        min_pk: i64,
        max_pk: i64,
    ) -> Result<u64> {
        let step = self.cfg.chunk_size.max(1) as i64;
        let mut ranges = std::collections::VecDeque::new();
        let mut cursor = min_pk;
        while cursor <= max_pk {
            let next =
                cursor.saturating_add(step).min(max_pk.saturating_add(1));
            ranges.push_back((cursor, next));
            if next <= cursor {
                break;
            }
            cursor = next;
        }
        let ranges = Arc::new(Mutex::new(ranges));

        let mut helpers = Vec::new();
        for _ in 1..self.cfg.max_parallel_chunks.max(1) {
            let Some(permit) = self.permits.try_extra() else {
                break;
            };
            let (reader, ranges, pk) =
                (self.reader(), Arc::clone(&ranges), pk_col.to_string());
            let (dsn, snapshot_id) =
                (self.dsn.clone(), self.snapshot_id.clone());
            let what = format!(
                "{} (intra-table reader)",
                fqn(&self.schema, &self.table)
            );
            helpers.push(tokio::spawn(async move {
                let _permit = permit;
                let client = import_view(&dsn, &snapshot_id, &what).await?;
                let sent = reader.drain(&client, &pk, &ranges).await;
                client.batch_execute("COMMIT").await.ok();
                sent
            }));
        }
        let mut total = self.reader().drain(client, pk_col, &ranges).await?;
        for h in helpers {
            total += h.await.context("intra-table reader panicked")??;
        }
        Ok(total)
    }

    fn reader(&self) -> RangeReader {
        RangeReader {
            schema: self.schema.clone(),
            table: self.table.clone(),
            pipeline: self.pipeline.clone(),
            tenant: self.tenant.clone(),
            generation: self.generation,
            lineage: self.lineage.clone(),
            identity: self.identity.clone(),
            publisher: Arc::clone(&self.publisher),
            cancel: self.cancel.clone(),
        }
    }

    // ctid-range chunking (fallback for non-integer-PK tables)
    async fn by_ctid(&self, client: &tokio_postgres::Client) -> Result<u64> {
        let fqn = fqn(&self.schema, &self.table);

        // Page count from the REAL on-disk size (`pg_relation_size`), never from
        // `pg_class.relpages` - relpages is a planner statistic that is 0/stale
        // until an ANALYZE the CDC role may not be permitted to run, and a stale
        // 0 must never be mistaken for an empty table. `pg_relation_size` reads
        // the actual file size, so 0 here means genuinely empty.
        let page_row = client
            .query_one(
                "SELECT (pg_relation_size(\
                     format('%I.%I', $1::text, $2::text)::regclass) \
                 / current_setting('block_size')::bigint)::bigint",
                &[&self.schema, &self.table],
            )
            .await
            .context("fetch relation size")?;

        let total_pages: i64 = page_row.get(0);
        if total_pages == 0 {
            debug!(table = %fqn, "empty relation (0 data pages)");
            return Ok(0);
        }
        let total_pages: i32 = total_pages.min(i32::MAX as i64) as i32;

        // Aim for ~chunk_size rows per batch (assume ~100 rows/page).
        let pages_per_chunk = ((self.cfg.chunk_size / 100) as i32).max(1);
        let mut page = 0i32;
        let mut total_sent = 0u64;

        while page < total_pages {
            if self.cancel.is_cancelled() {
                anyhow::bail!("snapshot cancelled");
            }
            let end_page = (page + pages_per_chunk).min(total_pages);
            // ctid drives pagination ONLY; identity comes from the explicit
            // native identity columns, never ctid.
            let sql = format!(
                r#"SELECT row_to_json(t)::text{}
                   FROM (SELECT * FROM "{}"."{}"
                         WHERE ctid >= '({page},1)'::tid
                           AND ctid < '({end_page},1)'::tid) t"#,
                identity_select_suffix(&self.identity),
                self.schema,
                self.table
            );

            let rows = client.query(&sql, &[]).await.with_context(|| {
                format!("read ctid [{page},{end_page}) from {fqn}")
            })?;

            let mut events = Vec::with_capacity(rows.len());
            for row in rows {
                events.push(build_pg_snapshot_event(
                    &row,
                    &self.identity,
                    &self.lineage,
                    self.generation,
                    &self.schema,
                    &self.table,
                    &self.pipeline,
                    &self.tenant,
                )?);
            }

            total_sent += events.len() as u64;
            self.publisher
                .publish_chunk(events)
                .await
                .map_err(|e| anyhow!(e))?;
            page = end_page;
        }

        Ok(total_sent)
    }
}

/// Reads PK ranges of one table and publishes each as a chunk.
struct RangeReader {
    schema: String,
    table: String,
    pipeline: String,
    tenant: String,
    generation: u64,
    lineage: PersistedLineage,
    identity: Vec<IdentitySpec>,
    publisher: Arc<GenerationPublisher>,
    cancel: CancellationToken,
}

impl RangeReader {
    /// Read ranges from the shared queue until it is empty.
    async fn drain(
        &self,
        client: &tokio_postgres::Client,
        pk_col: &str,
        ranges: &Mutex<std::collections::VecDeque<(i64, i64)>>,
    ) -> Result<u64> {
        let mut sent = 0;
        loop {
            if self.cancel.is_cancelled() {
                bail!("snapshot cancelled");
            }
            let next = ranges.lock().expect("not poisoned").pop_front();
            let Some((from, to)) = next else {
                return Ok(sent);
            };
            sent += self
                .read_and_publish_pk_range(client, pk_col, from, to)
                .await?;
        }
    }

    /// Read one PK range `[from, to)` and publish it as one chunk.
    async fn read_and_publish_pk_range(
        &self,
        client: &tokio_postgres::Client,
        pk_col: &str,
        from: i64,
        to: i64,
    ) -> Result<u64> {
        let fqn = fqn(&self.schema, &self.table);
        // row_to_json (column 0) is the payload; native identity columns follow
        // and are the ONLY identity input (schema-directed, never the JSON).
        let sql = format!(
            r#"SELECT row_to_json(t)::text{}
               FROM (SELECT * FROM "{}"."{}"
                     WHERE "{pk_col}" >= $1::bigint AND "{pk_col}" < $2::bigint
                     ORDER BY "{pk_col}") t"#,
            identity_select_suffix(&self.identity),
            self.schema,
            self.table
        );

        let rows = client
            .query(&sql, &[&from, &to])
            .await
            .with_context(|| format!("read chunk [{from},{to}) from {fqn}"))?;

        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            events.push(build_pg_snapshot_event(
                &row,
                &self.identity,
                &self.lineage,
                self.generation,
                &self.schema,
                &self.table,
                &self.pipeline,
                &self.tenant,
            )?);
        }
        let n = events.len() as u64;
        self.publisher
            .publish_chunk(events)
            .await
            .map_err(|e| anyhow!(e))?;
        Ok(n)
    }
}

// Utility functions

/// Build one snapshot event from a scanned row. Identity comes only from the
/// explicit native identity columns (never ctid); column 0 is the row_to_json
/// payload. Shared by the table worker and its intra-table chunk tasks.
#[allow(clippy::too_many_arguments)]
fn build_pg_snapshot_event(
    row: &tokio_postgres::Row,
    identity: &[IdentitySpec],
    lineage: &PersistedLineage,
    generation: u64,
    schema: &str,
    table: &str,
    pipeline: &str,
    tenant: &str,
) -> Result<Event> {
    let id = provisional_snapshot_id(
        row, identity, lineage, generation, schema, table,
    )?;
    let json_str: &str = row.get(0);
    let after: serde_json::Value =
        serde_json::from_str(json_str).context("parse row_to_json output")?;
    let size = json_str.len();
    let ts_ms = chrono::Utc::now().timestamp_millis();
    let source = SourceInfo {
        version: concat!("deltaforge-", env!("CARGO_PKG_VERSION")).to_string(),
        connector: "postgresql".into(),
        name: pipeline.to_string(),
        ts_ms,
        db: schema.to_string(),
        schema: Some(schema.to_string()),
        table: table.to_string(),
        snapshot: Some("true".into()),
        position: SourcePosition {
            snapshot_generation: Some(generation),
            ..Default::default()
        },
    };
    Ok(
        Event::new_row(id, source, Op::Read, None, Some(after), ts_ms, size)
            .with_tenant(tenant.to_string()),
    )
}

/// The snapshot cursor kind for a table: a signed integer-PK range scan (PG
/// integer types are all signed) or a ctid page-block frontier for
/// composite/non-integer/no-PK tables. ctid is a SCAN frontier only, never row
/// identity (identity comes from the explicit native identity columns).
pub(crate) fn pg_cursor_kind(
    schema: &crate::postgres::postgres_table_schema::PostgresTableSchema,
) -> CursorKind {
    let pk = &schema.primary_key;
    if pk.len() == 1 && is_integer_type(schema.column(pk[0].as_str())) {
        CursorKind::Signed
    } else {
        CursorKind::CtidBlock
    }
}

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

#[cfg(test)]
mod guard_tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn the_anchor_age_warns_at_80_percent_and_blocks_at_the_limit() {
        let max = Duration::from_secs(100);
        let at = |s| anchor_age_finding(Duration::from_secs(s), max);
        assert_eq!(at(79), GuardFinding::Ok);
        assert_eq!(at(80), GuardFinding::Warn("anchor_age"));
        assert_eq!(at(100), GuardFinding::Block("anchor_age"));
    }

    #[test]
    fn wal_retention_blocks_before_the_log_is_lost() {
        const MB: i64 = 1024 * 1024;
        assert_eq!(
            wal_finding(false, None, None, None, None),
            GuardFinding::Block("slot_missing")
        );
        assert_eq!(
            wal_finding(true, Some("wal_removed"), Some("lost"), None, None),
            GuardFinding::Block("slot_invalidated")
        );
        assert_eq!(
            wal_finding(true, None, Some("lost"), None, None),
            GuardFinding::Block("wal_lost")
        );
        assert_eq!(
            wal_finding(true, None, Some("unreserved"), Some(0), Some(100)),
            GuardFinding::Block("wal_lost")
        );
        assert_eq!(
            wal_finding(
                true,
                None,
                Some("unreserved"),
                Some(50 * MB),
                Some(100)
            ),
            GuardFinding::Warn("wal_retention")
        );
        assert_eq!(
            wal_finding(true, None, Some("reserved"), Some(19 * MB), Some(100)),
            GuardFinding::Warn("wal_retention")
        );
        assert_eq!(
            wal_finding(true, None, Some("reserved"), Some(21 * MB), Some(100)),
            GuardFinding::Ok
        );
        assert_eq!(
            wal_finding(true, None, Some("reserved"), None, Some(-1)),
            GuardFinding::Ok,
            "unlimited retention"
        );
    }
}
