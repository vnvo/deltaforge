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
use checkpoints::CheckpointStore;
use common::redact_url_password;
use deltaforge_config::SnapshotCfg;
use deltaforge_core::{
    CheckpointMeta, Event, EventId, IdentityKind, Op, SourceInfo, SourceItem,
    SourcePosition,
};
use std::collections::HashMap;

use super::postgres_identity::{PgIdentityRaw, pg_identity_cell, quote_ident};
use crate::durable_checkpoint::{CursorKind, SnapshotCursor};
use crate::snapshot_event_id::{OwnedIdentityValue, snapshot_row_event_id};
use crate::snapshot_frontier::{
    SnapshotAggregator, SnapshotPublisher, TableResume,
};
use crate::snapshot_generation::PersistedLineage;
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
#[derive(Debug, Clone)]
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

pub struct PgSnapshotCtx<'a> {
    pub dsn: &'a str,
    pub source_id: &'a str,
    pub pipeline: &'a str,
    pub tenant: &'a str,
    pub cfg: &'a SnapshotCfg,
    pub schema_loader: &'a PostgresSchemaLoader,
    pub chkpt_store: Arc<dyn CheckpointStore>,
    pub tx: mpsc::Sender<SourceItem>,
    pub cancel: CancellationToken,
    pub slot_name: Option<&'a str>,
    /// Durable snapshot generation (allocated before any row).
    pub generation: u64,
    /// Frozen source lineage for snapshot identity.
    pub lineage: PersistedLineage,
}

/// Run a consistent snapshot of `tables`.
///
/// Returns the anchor LSN (the slot's consistent point) - pass this to the
/// replication client as `start_lsn` so streaming resumes from the anchor with
/// **no gaps** (no committed row is lost). Note this is at-least-once at the
/// boundary: rows committed in `(anchor, snapshot-export]` are delivered by both
/// the snapshot and CDC (a bounded overlap), so **duplicate events are possible**
/// and expected - it is not exactly-once (PG-A-lite).
pub async fn run_snapshot(
    ctx: &PgSnapshotCtx<'_>,
    tables: &[super::PgPlannedTable],
    // The anchor LSN: the replication slot's consistent point, established by
    // prepare_snapshot_slot_anchor. CDC resumes from here; rows committed in
    // (anchor, snapshot-export] are re-delivered by CDC (bounded at-least-once
    // overlap), never lost. NOT pg_current_wal_lsn.
    anchor: Lsn,
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
    let names: Vec<(&str, &str)> = tables
        .iter()
        .map(|t| (t.qualifier.as_str(), t.table.as_str()))
        .collect();
    let preflight = health::run_preflight(
        ctx.dsn,
        ctx.slot_name,
        // DeltaForge creates and owns the slot as part of the snapshot anchor, so
        // an absent slot here is expected, not a hard error.
        health::SlotPresence::CreatedByDeltaforge,
        // publication name not on ctx - pass empty string; publication check
        // is already done in ensure_slot_and_publication before we get here.
        // Pass slot_name here only for slot health checks.
        "",
        &names,
        ctx.cfg.max_parallel_tables,
        ctx.cfg.discovery_page_size,
    )
    .await
    .context("postgres snapshot preflight")?;
    crate::snapshot_probe::record_fixed(
        crate::snapshot_probe::FixedOp::Preflight,
    );
    drop(names);
    preflight.emit_and_check(ctx.source_id, tables.len())?;

    crate::snapshot_probe::before_anchor().await;
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

    // Export the MVCC snapshot for worker mutual consistency only. The CDC
    // start position is the slot's consistent point (`anchor`), NOT
    // pg_current_wal_lsn - that decoupling is exactly the pre-hardening seam
    // bug (PG-A-lite).
    let row = coord
        .query_one("SELECT pg_export_snapshot()", &[])
        .await
        .context("export snapshot")?;

    let snapshot_id: String = row.get(0);
    crate::snapshot_probe::record_fixed(crate::snapshot_probe::FixedOp::Anchor);
    // The rows are read in this exported snapshot: every planned table must
    // have, in it, exactly the shape the plan was prepared from.
    verify_plan_in_snapshot(&coord, tables, ctx.cfg.discovery_page_size)
        .await?;
    let start_lsn = anchor;

    // Save start_lsn immediately - if we crash before finishing, we know
    // where to resume streaming from.
    progress.start_lsn = anchor.to_string();
    save_progress(&ctx.chkpt_store, ctx.source_id, &progress).await;
    crate::snapshot_probe::record_phase(
        crate::snapshot_probe::Phase::PreflightAnchor,
        t0.elapsed(),
    );

    let abort_reason: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let guard_cancel = ctx.cancel.child_token();
    let _guard_stop = scopeguard::guard((), |_| guard_cancel.cancel());
    let _slot_guard = ctx.slot_name.map(|slot| {
        health::spawn_wal_slot_guard(
            crate::credentials::ProtectedDsn::from(ctx.dsn),
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

    // Build the aggregation owner. Its source vector is restored from the
    // source's own progress (done_tables + finished) - NEVER from any sink's
    // HEAD. Every table's cursor kind (integer-PK signed range or ctid
    // page-block) comes from the plan; already-done tables enter the vector
    // complete at their kind's max, so the key set and cursor kinds are fixed
    // from the first batch.
    let resume: Vec<(String, TableResume)> = tables
        .iter()
        .map(|t| {
            let done = progress.finished
                || progress.table_done(&t.qualifier, &t.table);
            (
                t.key(),
                TableResume {
                    kind: t.cursor_kind,
                    done,
                },
            )
        })
        .collect();
    crate::snapshot_probe::record_frontier_tables(resume.len());
    let snapshot_checkpoint =
        CheckpointMeta::from_vec(anchor.to_string().into_bytes());
    let publisher = Arc::new(SnapshotPublisher::new(
        SnapshotAggregator::from_source_progress(
            ctx.generation,
            ctx.lineage.clone(),
            snapshot_checkpoint,
            &resume,
        ),
        ctx.tx.clone(),
    ));

    let copy_started = Instant::now();
    // step 2: table workers, at most `max_parallel_tables` alive at once;
    // each result is recorded as it completes.
    let max_parallel = ctx.cfg.max_parallel_tables.max(1);
    let fetches_before = ctx.schema_loader.live_fetch_count();
    let mut running: tokio::task::JoinSet<Result<u64>> =
        tokio::task::JoinSet::new();
    // The table of each running task (at most `max_parallel` entries).
    let mut names: HashMap<tokio::task::Id, String> = HashMap::new();
    let mut failed = Vec::new();

    for planned in tables {
        let (schema, table) = (&planned.qualifier, &planned.table);
        if progress.table_done(schema, table) {
            info!(table = %planned.key(), "already complete, skipping");
            continue;
        }
        while running.len() >= max_parallel {
            let done =
                running.join_next_with_id().await.expect("a running table");
            record_table_result(
                ctx,
                &publisher,
                &mut progress,
                &mut failed,
                &mut names,
                done,
            )
            .await?;
        }

        let worker = TableWorker {
            dsn: crate::credentials::ProtectedDsn::from(ctx.dsn),
            schema: schema.clone(),
            table: table.clone(),
            snapshot_id: snapshot_id.clone(),
            source_id: ctx.source_id.to_string(),
            pipeline: ctx.pipeline.to_string(),
            tenant: ctx.tenant.to_string(),
            cfg: ctx.cfg.clone(),
            schema_loader: ctx.schema_loader.clone(),
            chkpt_store: ctx.chkpt_store.clone(),
            cancel: ctx.cancel.clone(),
            generation: ctx.generation,
            lineage: ctx.lineage.clone(),
            identity: planned.identity.clone(),
            table_key: planned.key(),
            cursor_kind: planned.cursor_kind,
            publisher: Arc::clone(&publisher),
        };
        let task = running.spawn(worker.run());
        names.insert(task.id(), planned.key());
        crate::snapshot_probe::record_table_tasks(running.len());
    }

    // step 3: the remaining results
    while let Some(done) = running.join_next_with_id().await {
        record_table_result(
            ctx,
            &publisher,
            &mut progress,
            &mut failed,
            &mut names,
            done,
        )
        .await?;
    }
    crate::snapshot_probe::record_worker_live_fetches(
        ctx.schema_loader.live_fetch_count() - fetches_before,
    );
    crate::snapshot_probe::record_phase(
        crate::snapshot_probe::Phase::RowCopy,
        copy_started.elapsed(),
    );
    let final_started = Instant::now();

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

    // mark fully done, stamped with the current (safe) anchor protocol version.
    progress.finished = true;
    progress.anchor_version = SNAPSHOT_ANCHOR_VERSION;
    save_progress(&ctx.chkpt_store, ctx.source_id, &progress).await;

    info!(
        source_id = %ctx.source_id,
        elapsed_secs = t0.elapsed().as_secs(),
        start_lsn = %start_lsn,
        "snapshot finished - streaming will start from this LSN"
    );

    guard_cancel.cancel();
    crate::snapshot_probe::record_phase(
        crate::snapshot_probe::Phase::Finalization,
        final_started.elapsed(),
    );
    Ok(start_lsn)
}

/// In the exported snapshot the rows are read from, recompute every planned
/// table's shape signature from the catalog (batched by `batch`) and require
/// the plan's: a table altered, dropped or replaced since preparation stops
/// the snapshot before any row is read.
async fn verify_plan_in_snapshot(
    coord: &tokio_postgres::Client,
    tables: &[super::PgPlannedTable],
    batch: usize,
) -> Result<()> {
    for chunk in tables.chunks(batch.max(1)) {
        let schemas: Vec<&str> =
            chunk.iter().map(|t| t.qualifier.as_str()).collect();
        let names: Vec<&str> = chunk.iter().map(|t| t.table.as_str()).collect();
        let col_rows = coord
            .query(
                "SELECT x.s, x.t, c.oid::int8, a.attnum::int4, a.attname::text, \
                        a.atttypid::int8, \
                        NOT (a.attnotnull OR (ty.typtype = 'd' AND ty.typnotnull)) \
                 FROM unnest($1::text[], $2::text[]) AS x(s, t) \
                 JOIN pg_namespace n ON n.nspname = x.s \
                 JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = x.t \
                 JOIN pg_attribute a ON a.attrelid = c.oid \
                      AND a.attnum > 0 AND NOT a.attisdropped \
                 JOIN pg_type ty ON ty.oid = a.atttypid",
                &[&schemas, &names],
            )
            .await
            .context("verify the plan: read column shapes")?;
        let pk_rows = coord
            .query(
                "SELECT x.s, x.t, a.attname::text \
                 FROM unnest($1::text[], $2::text[]) AS x(s, t) \
                 JOIN pg_namespace n ON n.nspname = x.s \
                 JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = x.t \
                 JOIN pg_index i ON i.indrelid = c.oid AND i.indisprimary \
                 JOIN pg_attribute a ON a.attrelid = c.oid \
                      AND a.attnum = ANY(i.indkey) \
                 ORDER BY x.s, x.t, array_position(i.indkey, a.attnum)",
                &[&schemas, &names],
            )
            .await
            .context("verify the plan: read primary keys")?;
        let mut shapes: HashMap<
            (String, String),
            (Option<u32>, Vec<PgShapeColumn>),
        > = HashMap::new();
        for r in col_rows {
            let e = shapes.entry((r.get(0), r.get(1))).or_insert_with(|| {
                (Some(r.get::<_, i64>(2) as u32), Vec::new())
            });
            e.1.push((
                r.get(3),
                r.get(4),
                Some(r.get::<_, i64>(5) as u32),
                r.get(6),
            ));
        }
        let mut keys: HashMap<(String, String), Vec<String>> = HashMap::new();
        for r in pk_rows {
            keys.entry((r.get(0), r.get(1))).or_default().push(r.get(2));
        }
        for planned in chunk {
            let key = (planned.qualifier.clone(), planned.table.clone());
            let Some((oid, cols)) = shapes.get(&key) else {
                bail!(plan_moved(&planned.key(), "it no longer exists"));
            };
            let pk = keys.get(&key).map(Vec::as_slice).unwrap_or(&[]);
            if pg_shape_signature(*oid, cols, pk) != planned.signature {
                bail!(plan_moved(&planned.key(), "its schema changed"));
            }
        }
        crate::snapshot_probe::record_verification_page();
    }
    crate::snapshot_probe::record_fixed(
        crate::snapshot_probe::FixedOp::CatalogVerification,
    );
    Ok(())
}

fn plan_moved(table: &str, what: &str) -> String {
    format!(
        "{table} changed between snapshot preparation and the snapshot \
         anchor ({what}); no row was read - restart to plan the snapshot again"
    )
}

/// One column of a PostgreSQL shape signature: ordinal, name, type OID,
/// nullable (as `information_schema.columns.is_nullable` reports it).
pub(crate) type PgShapeColumn = (i32, String, Option<u32>, bool);

/// The PostgreSQL shape signature: table OID, columns by ordinal, primary
/// key in index order.
pub(crate) fn pg_shape_signature(
    oid: Option<u32>,
    columns: &[PgShapeColumn],
    primary_key: &[String],
) -> String {
    let mut cols: Vec<&PgShapeColumn> = columns.iter().collect();
    cols.sort_by_key(|c| c.0);
    let mut s = crate::snapshot_plan::ShapeSignature::new("postgres");
    s.num(oid.map_or(-1, i64::from)).num(cols.len() as i64);
    for (ordinal, name, type_oid, nullable) in cols {
        s.num(i64::from(*ordinal))
            .text(name)
            .num(type_oid.map_or(-1, i64::from))
            .num(i64::from(*nullable));
    }
    s.num(primary_key.len() as i64);
    for k in primary_key {
        s.text(k);
    }
    s.finish()
}

/// [`pg_shape_signature`] of a loaded schema.
pub(crate) fn pg_schema_signature(
    schema: &crate::postgres::postgres_table_schema::PostgresTableSchema,
) -> String {
    let cols: Vec<PgShapeColumn> = schema
        .columns
        .iter()
        .map(|c| (c.ordinal_position, c.name.clone(), c.type_oid, c.nullable))
        .collect();
    pg_shape_signature(schema.oid, &cols, &schema.primary_key)
}

/// The plan entry of `schema.table` with `identity`, its cursor kind from
/// the schema `loader` resolves (as the preparation pass derives it). For
/// driving [`run_snapshot`] directly.
pub async fn plan_table(
    loader: &PostgresSchemaLoader,
    schema: &str,
    table: &str,
    identity: Vec<IdentitySpec>,
) -> Result<super::PgPlannedTable> {
    let loaded = loader.load_schema(schema, table).await?;
    Ok(super::PgPlannedTable {
        qualifier: schema.to_string(),
        table: table.to_string(),
        identity,
        cursor_kind: pg_cursor_kind(&loaded.schema),
        signature: pg_schema_signature(&loaded.schema),
    })
}

/// Record one finished table task: a completed table is marked done,
/// persisted and completed in the aggregator; a failure is collected.
async fn record_table_result(
    ctx: &PgSnapshotCtx<'_>,
    publisher: &SnapshotPublisher,
    progress: &mut SnapshotProgress,
    failed: &mut Vec<String>,
    names: &mut HashMap<tokio::task::Id, String>,
    done: std::result::Result<
        (tokio::task::Id, Result<u64>),
        tokio::task::JoinError,
    >,
) -> Result<()> {
    let id = match &done {
        Ok((id, _)) => *id,
        Err(e) => e.id(),
    };
    let name = names.remove(&id).unwrap_or_default();
    match done {
        Ok((_, Ok(_rows))) => {
            if let Some((schema, table)) = name.split_once('.') {
                progress.mark_done(schema, table);
                save_progress(&ctx.chkpt_store, ctx.source_id, progress).await;
            }
            // Explicit table completion: emits a table-complete boundary, and
            // the `completed = true` snapshot boundary once every scanned
            // table is done (delivered and durably acked with no trailing
            // data rows).
            if publisher.complete_table(&name).await.is_err() {
                bail!("event channel closed at table completion");
            }
            info!(table = %name, "snapshot complete");
        }
        Ok((_, Err(e))) => {
            // Preserve the full anyhow cause chain, not just the outer message.
            let chain = format!("{e:#}");
            error!(table = %name, error = %chain, "table snapshot failed");
            failed.push(name);
        }
        Err(e) => {
            error!(table = %name, error = %e, "snapshot worker panicked");
            failed.push(name);
        }
    }
    Ok(())
}

async fn save_progress(
    store: &Arc<dyn CheckpointStore>,
    source_id: &str,
    progress: &SnapshotProgress,
) {
    if let Ok(bytes) = serde_json::to_vec(progress) {
        let started = std::time::Instant::now();
        let _ = store.put_raw(&progress_key(source_id), &bytes).await;
        crate::snapshot_probe::record_progress_write(
            bytes.len(),
            started.elapsed(),
        );
    }
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
    #[allow(unused)]
    chkpt_store: Arc<dyn CheckpointStore>,
    cancel: CancellationToken,
    /// Durable snapshot generation for stable-id derivation.
    generation: u64,
    /// Frozen source lineage.
    lineage: PersistedLineage,
    /// Resolved identity columns (name + kind, identity order).
    identity: Vec<IdentitySpec>,
    /// Fully-qualified `schema.table`, the aggregator's key for this table.
    table_key: String,
    /// Cursor kind for this table (matches the aggregator's frontier kind).
    cursor_kind: CursorKind,
    /// Shared aggregation owner: serializes boundary-advance + channel send.
    publisher: Arc<SnapshotPublisher>,
}

impl TableWorker {
    async fn run(self) -> Result<u64> {
        info!(pipeline=%self.pipeline, source_id=%self.source_id, schema=%self.schema, table=%self.table, "snapshot worker starting");
        let fqn = fqn(&self.schema, &self.table);
        let t0 = Instant::now();

        let (client, conn) = tokio_postgres::connect(self.dsn.expose(), NoTls)
            .await
            .with_context(|| format!("connect for {fqn}"))?;

        tokio::spawn(async move {
            if let Err(e) = conn.await {
                warn!(error = %e, "snapshot worker connection dropped");
            }
        });

        // Import the shared snapshot - all workers see the same DB state.
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
                    "composite or missing PK - using ctid chunking"
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
        // Half-open frontier cursor reported to the aggregator, starting at the
        // signed minimum so the first chunk abuts the frontier (rows below min_pk
        // do not exist and are vacuously durable). PG integer PKs are signed.
        let mut published: SnapshotCursor = self.cursor_kind.min();

        while cursor <= max_pk {
            if self.cancel.is_cancelled() {
                anyhow::bail!("snapshot cancelled");
            }
            let next = cursor + chunk;
            let events = self
                .read_pk_events(client, pk_col, cursor, next, &fqn)
                .await?;
            total_sent += events.len() as u64;
            let end = SnapshotCursor::Signed(next);
            self.publisher
                .publish_chunk(&self.table_key, published, end, events)
                .await
                .map_err(|_| anyhow!("event channel closed"))?;
            published = end;
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

        // Cover the vacuous prefix below min_pk once, so every real chunk (from
        // any concurrent sub-range) abuts the contiguous frontier at min_pk.
        // Empty events: this only advances the frontier, emitting no boundary.
        self.publisher
            .publish_chunk(
                &self.table_key,
                self.cursor_kind.min(),
                SnapshotCursor::Signed(min_pk),
                Vec::new(),
            )
            .await
            .map_err(|_| anyhow!("event channel closed"))?;

        let semaphore = Arc::new(Semaphore::new(n_chunks));
        let mut handles = Vec::new();

        let mut chunk_start = min_pk;
        while chunk_start <= max_pk {
            let chunk_end = (chunk_start + per_chunk).min(max_pk + 1);
            let permit = semaphore.clone().acquire_owned().await?;

            // Each intra-table chunk needs its own connection with the
            // snapshot imported.
            let (sub_client, sub_conn) =
                tokio_postgres::connect(self.dsn.expose(), NoTls)
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
            let cancel = self.cancel.clone();

            let handle = tokio::spawn(async move {
                let mut cursor = chunk_start;
                let mut sent = 0u64;
                while cursor < chunk_end {
                    if cancel.is_cancelled() {
                        return Err(anyhow::anyhow!("cancelled"));
                    }
                    let next = (cursor + step).min(chunk_end);
                    // Concurrent sub-ranges publish their real chunks
                    // [Signed(cursor), Signed(next)); the aggregator buffers
                    // out-of-order arrivals and advances the frontier only through
                    // contiguous ranges.
                    sent += worker_self
                        .read_and_publish_pk_range(
                            &sub_client,
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

    /// Read one PK range `[from, to)` and build its snapshot events (no send);
    /// the caller publishes the chunk through the aggregation owner.
    async fn read_pk_events(
        &self,
        client: &tokio_postgres::Client,
        pk_col: &str,
        from: i64,
        to: i64,
        fqn: &str,
    ) -> Result<Vec<Event>> {
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
        Ok(events)
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
        // ctid page-block frontier, starting at block 0 (the kind's minimum).
        let mut published: SnapshotCursor = self.cursor_kind.min();

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
            let end = SnapshotCursor::CtidBlock(end_page as u64);
            self.publisher
                .publish_chunk(&self.table_key, published, end, events)
                .await
                .map_err(|_| anyhow!("event channel closed"))?;
            published = end;
            page = end_page;
        }

        Ok(total_sent)
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
            generation: self.generation,
            lineage: self.lineage.clone(),
            identity: self.identity.clone(),
            table_key: self.table_key.clone(),
            publisher: Arc::clone(&self.publisher),
        }
    }
}

/// Minimal context passed into spawned intra-table chunk tasks.
struct ChunkWorkerCtx {
    schema: String,
    table: String,
    pipeline: String,
    tenant: String,
    dsn: crate::credentials::ProtectedDsn,
    snapshot_id: String,
    chunk_size: usize,
    generation: u64,
    lineage: PersistedLineage,
    identity: Vec<IdentitySpec>,
    table_key: String,
    publisher: Arc<SnapshotPublisher>,
}

impl ChunkWorkerCtx {
    /// Read one PK sub-range `[from, to)` and publish it as the half-open chunk
    /// `[Signed(from), Signed(to))` through the shared aggregation owner.
    /// Concurrent sub-ranges publish out of order; the aggregator buffers them and
    /// advances the contiguous frontier only through abutting ranges.
    async fn read_and_publish_pk_range(
        &self,
        client: &tokio_postgres::Client,
        pk_col: &str,
        from: i64,
        to: i64,
        fqn: &str,
    ) -> Result<u64> {
        debug!(
            pipeline=%self.pipeline,
            dsn=%redact_url_password(self.dsn.expose()),
            snapshot_id=%self.snapshot_id,
            chunk_size=%self.chunk_size,
            "reading PK range"
        );

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
            .publish_chunk(
                &self.table_key,
                SnapshotCursor::Signed(from),
                SnapshotCursor::Signed(to),
                events,
            )
            .await
            .map_err(|_| anyhow!("event channel closed"))?;
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
