use std::{
    collections::HashMap,
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

use async_trait::async_trait;
use deltaforge_config::SnapshotMode;
use mysql_binlog_connector_rust::{
    binlog_client::BinlogClient, binlog_stream::BinlogStream,
    event::table_map_event::TableMapEvent,
};
use serde::{Deserialize, Serialize};
use storage::{ArcStorageBackend, DurableSchemaRegistry};
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use checkpoints::{CheckpointStore, CheckpointStoreExt};
use common::{AllowList, RetryPolicy, pause_until_resumed};

use crate::snapshot_generation::PersistedLineage;
use deltaforge_core::incident::{CauseCode, IncidentDraft, Retryability};
use deltaforge_core::{
    CheckpointOrder, Source, SourceError, SourceHandle, SourceItem,
    SourceResult,
};
mod mysql_errors;
pub use mysql_errors::{LoopControl, MySqlSourceError, MySqlSourceResult};

mod mysql_activation;
mod mysql_baseline;
mod mysql_binlog_scan;
mod mysql_checkpoint_lineage;
mod mysql_ddl_attribution;
mod mysql_failover_drift;
mod mysql_forward_proof;
mod mysql_helpers;
mod mysql_selection;
mod mysql_session;
mod mysql_signature;
use mysql_helpers::{checkpoint_lineage, prepare_client};

pub mod mysql_object;

mod mysql_schema_loader;
pub use mysql_schema_loader::{LoadedSchema, MySqlSchemaLoader};

pub mod mysql_rotation;

mod mysql_event;

pub mod mysql_event_id;
use mysql_event::*;
pub use mysql_event_id::mysql_row_event_id;

pub mod mysql_identity;
pub use mysql_identity::{
    MysqlIdentityError, mysql_identity_cell, mysql_identity_kind,
};

mod mysql_table_schema;
use crate::mysql::mysql_helpers::{
    Opened, connect_binlog_with_retries, resolve_binlog_tail,
};
use crate::mysql::mysql_session::{SessionError, open_control_connection};
pub use mysql_table_schema::{MySqlColumn, MySqlTableSchema};

pub mod mysql_snapshot;
pub use mysql_snapshot::{MysqlSnapshotProgress, progress_key};

pub mod mysql_health;

use crate::failover::identity::{
    IdentityComparison, IdentityStore, ServerIdentity,
};
use crate::failover::reconciler::{ReconcileInput, SchemaReconciler};
use crate::mysql::mysql_health::{
    PositionReachability, check_position_reachability_on, fetch_server_identity,
};
use crate::registry_scope::{
    ScopeChange, SharedRegistryScope, establish_scope, previous_scope,
};
use storage::adapters::LineageDescriptor;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MySqlCheckpoint {
    pub file: String,
    pub pos: u64,
    pub gtid_set: Option<String>,
    /// Verified registry lineage hash of the server this position belongs to
    /// (MySQL `server_uuid`). `None` only in checkpoints written before
    /// lineage was recorded; see [`compare_mysql_checkpoints`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lineage: Option<String>,
    /// Set (to the generation) only on the checkpoint of the boundary that
    /// completes a snapshot, at its anchor: proof that the sinks committed the
    /// whole snapshot. A checkpoint at the anchor without it was written
    /// before the proof existed and proves nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_completed: Option<u64>,
    /// With `snapshot_completed`: the snapshot chain of the completed
    /// generation (absent on completions written before chains).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_chain: Option<String>,
}

/// A resume position read from the checkpoint store.
#[derive(Debug, Clone)]
pub(crate) enum MyResumePosition {
    /// A snapshot no sink acknowledged as complete, or a sink's start of a
    /// snapshot generation: not a stream position.
    Snapshot,
    /// A stream position (CDC, or the completing snapshot boundary).
    Stream(MySqlCheckpoint),
}

/// Classify stored checkpoint bytes; anything else fails closed.
pub(crate) fn classify_mysql_checkpoint(
    raw: &[u8],
) -> Result<MyResumePosition, String> {
    match crate::snapshot_position::classify::<MySqlCheckpoint>(raw)? {
        crate::snapshot_position::Classified::Incomplete(_)
        | crate::snapshot_position::Classified::Adopted(_) => {
            return Ok(MyResumePosition::Snapshot);
        }
        crate::snapshot_position::Classified::Stream => {}
    }
    serde_json::from_slice::<MySqlCheckpoint>(raw)
        .map(MyResumePosition::Stream)
        .map_err(|e| format!("unrecognised checkpoint: {e}"))
}

/// Whether `resume` proves the sinks committed the snapshot `progress`
/// records: either its completing checkpoint (marked with that snapshot's
/// generation, exactly at its anchor), or an unmarked stream position the
/// comparator orders strictly after the anchor. A missing or unreadable
/// anchor, another generation, or a position at, before or incomparable with
/// the anchor proves nothing.

#[derive(Debug, Clone)]
pub struct MySqlSource {
    pub id: String,
    /// Protected connection DSN (redacted `Debug`, no serialization). Built at
    /// startup from inline config or resolved secret references.
    pub dsn: crate::credentials::ProtectedDsn,
    pub tables: Vec<String>,
    pub tenant: String,
    pub pipeline: String,
    pub registry: Arc<DurableSchemaRegistry>,
    /// Verified-lineage scope shared with the pipeline's schema loaders. The
    /// source establishes it before any registry access.
    pub registry_scope: crate::registry_scope::SharedRegistryScope,
    pub backend: ArcStorageBackend,
    pub outbox_tables: AllowList,
    pub snapshot_cfg: deltaforge_config::SnapshotCfg,
    pub on_schema_drift: deltaforge_config::OnSchemaDrift,
    /// Per-table options (identity_columns, assume_unique), keyed by
    /// fully-qualified `db.table`.
    pub table_options:
        std::collections::BTreeMap<String, deltaforge_config::TableOptions>,
    /// Controlled credential-rotation spec, when configured and file-backed.
    /// `None` disables rotation for this source. Requires GTID mode.
    pub rotation: Option<Arc<crate::rotation_manager::RotationSpec>>,
    /// The commit policy and sink cohort a snapshot generation freezes, set
    /// by the runner before the source runs.
    pub snapshot_cohort: crate::SnapshotCohortSlot,
}

pub(crate) const HEARTBEAT_INTERVAL_SECS: u64 = 15;
pub(crate) const READ_TIMEOUT: u64 = 90;

pub(crate) struct RunCtx {
    source_id: String,
    pipeline: String,
    /// The pipeline's per-table label policy.
    table_metrics: Arc<deltaforge_core::table_metrics::TableMetrics>,
    tenant: String,
    dsn: crate::credentials::ProtectedDsn,
    #[allow(dead_code)]
    host: String,
    default_db: String,
    server_id: u64,
    tx: mpsc::Sender<SourceItem>,
    chkpt: Arc<dyn CheckpointStore>,
    cancel: CancellationToken,
    paused: Arc<AtomicBool>,
    pause_notify: Arc<Notify>,
    schema: MySqlSchemaLoader,
    allow: AllowList,
    retry: RetryPolicy,
    inactivity: Duration,
    table_map: HashMap<u64, TableMapEvent>,
    last_file: String,
    last_pos: u64,
    last_gtid: Option<String>,
    /// The current transaction's exact GTID (`uuid:gno`), captured from the GTID
    /// event before it is merged into `last_gtid`'s accumulated executed set.
    /// Used as the immutable per-event identity coordinate.
    current_gtid: Option<String>,
    /// Whether the current GTID transaction has entered an explicit `BEGIN` block
    /// (multi-event transaction ending in `Xid`/`COMMIT`/`ROLLBACK`). A GTID-backed
    /// autocommit statement (DDL, `FLUSH`, ...) never sets this, so it is its own
    /// single-statement transaction closed by its `QueryEvent`. Used to tell a
    /// control statement inside a transaction (e.g. `SAVEPOINT`) from a standalone
    /// autocommit statement.
    in_explicit_txn: bool,
    /// DDL message ordinal: reset per transaction (GTID / BEGIN), incremented
    /// **before filtering** for each DDL so a skipped DDL never renumbers a
    /// retained one.
    message_ordinal: u32,
    /// QueryEvent ordinal within the current GTID transaction (reset at each
    /// GTID, incremented for every QueryEvent): the event identity of the
    /// activation records a statement establishes, as the binlog scanner
    /// numbers them.
    query_ordinal: u32,
    /// Ordinal of the current rows event among its transaction's rows events
    /// (every table, before filtering; nested order in a compressed
    /// transaction). 0 before the first.
    rows_ordinal: u32,
    /// The server's `lower_case_table_names` (read at startup on a verified
    /// connection): how DDL table names map to registry keys.
    lower_case_table_names: u8,
    /// Evaluation position of rows (spec 7.3): the executed state immediately
    /// before the current transaction - the stream position after the last
    /// commit boundary (or the position the stream (re)started from).
    txn_eval: Option<crate::durable_checkpoint::WmPos>,
    /// The same position as binlog coordinates (lineage left unset): where a
    /// lazy baseline's scan starts.
    txn_eval_cp: Option<MySqlCheckpoint>,
    /// Validated activation timelines and row-time selections.
    selection: mysql_selection::Caches,
    /// The current lineage's failover anchor (`Some(None)`: not entered by
    /// a failover; `None`: not loaded yet).
    failover: Option<Option<Arc<mysql_failover_drift::FailoverAnchor>>>,
    /// Tables whose failover drift check completed in this run.
    drift_checked: std::collections::HashSet<String>,
    /// Resolves this source's schema-drift incidents as tables are accepted.
    drift_resolver: crate::incident_drafts::DriftResolver,
    /// The pipeline's incidents: a retried resume-position check is reported
    /// here while it retries.
    incidents: storage::adapters::incidents::IncidentStore,
    /// Original checkpoint position, preserved even after a pre-connect failover
    /// adjustment clears last_gtid/last_file. Used by check_position_reachability
    /// to verify whether A's position actually exists on B.
    checkpoint_gtid: Option<String>,
    checkpoint_file: String,
    outbox_tables: AllowList,
    identity_store: IdentityStore,
    reconciler: SchemaReconciler,
    /// Verified-lineage registry scope (shared with the pipeline's loaders).
    registry_scope: SharedRegistryScope,
    /// Backend holding the durable source-lineage record.
    registry_backend: ArcStorageBackend,
    on_schema_drift: deltaforge_config::OnSchemaDrift,
    /// Frozen source lineage captured once at startup, for durable CDC
    /// watermarks. `None` if lineage could not be resolved (durable mode then
    /// fails closed at the sink; non-durable pipelines are unaffected).
    durable_lineage: Option<PersistedLineage>,
}

/// A table of the snapshot plan: its identity column names (in identity
/// order).
pub(crate) type MyPlannedTable =
    crate::snapshot_plan::PlannedTable<Vec<String>>;

/// What the preparation pass needs to apply the failover drift policy.
struct DriftCheck<'a> {
    anchor: &'a mysql_failover_drift::FailoverAnchor,
    env: mysql_failover_drift::DriftEnv<'a>,
}

/// The snapshot connections a MySQL generation cannot start without: the
/// read-lock connection (catalog, anchor), the guard's session and one
/// worker.
const MY_BASE_SNAPSHOT_CONNECTIONS: u32 = 3;

/// A failure as the source's error: a typed one (another server, a missing
/// privilege, an incompatible setting) as it is, anything else with
/// `context`.
fn typed(e: anyhow::Error, context: &'static str) -> SourceError {
    e.downcast::<SourceError>()
        .unwrap_or_else(|e| SourceError::Other(e.context(context)))
}

/// A control record's anchor as a MySQL anchor.
fn my_anchor_of(
    a: &crate::snapshot_queue::EngineAnchor,
) -> Option<MySqlCheckpoint> {
    match a {
        crate::snapshot_queue::EngineAnchor::Mysql {
            file,
            pos,
            gtid_set,
            lineage,
        } => Some(MySqlCheckpoint {
            file: file.clone(),
            pos: *pos,
            gtid_set: gtid_set.clone(),
            lineage: lineage.clone(),
            snapshot_completed: None,
            snapshot_chain: None,
        }),
        crate::snapshot_queue::EngineAnchor::Postgres { .. } => None,
    }
}

/// What a MySQL generation start or completion check reads.
type MyGenerationInputs = crate::snapshot_driver::GenerationInputs<MyOrder>;

/// What a start decided for the snapshot (design section 4).
enum MyStart {
    /// Stream; `completed_anchor` is the anchor of the completed generation
    /// (the stream starts there when no sink holds a stream position yet).
    Stream {
        completed_anchor: Option<MySqlCheckpoint>,
    },
    /// Run this generation (allocated, its start barrier pending).
    Generation {
        version: u64,
        control: Box<crate::snapshot_queue::GenerationControl>,
    },
}

/// A generation this start ran: where the stream starts, and the plan
/// whose tables are proven at the anchor before completion is watched.
struct MyGenerationRun {
    anchor: MySqlCheckpoint,
    generation: u64,
    /// The run that owns the generation, and its publisher: the rows are
    /// recorded produced and the terminal barrier sent only once every
    /// final check passed, the anchor baselines included
    /// ([`MySqlSource::finish_generation`]).
    run: String,
    publisher: Arc<crate::snapshot_publish::GenerationPublisher>,
}

impl MySqlSource {
    fn incidents(&self) -> storage::adapters::incidents::IncidentStore {
        storage::adapters::incidents::IncidentStore::new(
            Arc::clone(&self.backend),
            &self.pipeline,
        )
    }

    fn queue(&self) -> crate::snapshot_queue::QueueStore {
        crate::snapshot_queue::QueueStore::new(
            Arc::clone(&self.backend),
            &self.id,
        )
    }

    /// The configuration part of a generation's fingerprint (format 3: the
    /// table patterns; each table's schema is bound by its plan item).
    fn config_fingerprint(&self) -> String {
        crate::snapshot_generation::SnapshotFingerprintBuilder::new(
            "mysql",
            &self.tables,
        )
        .finish()
        .as_str()
        .to_string()
    }

    /// Decide what this start does about the snapshot (design section 4).
    async fn decide_snapshot(
        &self,
        chkpt_store: &Arc<dyn CheckpointStore>,
        lineage: PersistedLineage,
    ) -> SourceResult<(MyStart, Option<MyGenerationInputs>)> {
        let queue = self.queue();
        let cohort = self
            .snapshot_cohort
            .lock()
            .expect("not poisoned")
            .cohort
            .clone();
        let Some(cohort) = cohort else {
            // A source run outside a pipeline: only mode `never` without
            // any generation streams; a snapshot needs the frozen cohort.
            let stored = queue.read().await.map_err(|e| {
                SourceError::Other(anyhow::anyhow!("snapshot state: {e}"))
            })?;
            if self.snapshot_cfg.mode == SnapshotMode::Never && stored.is_none()
            {
                return Ok((
                    MyStart::Stream {
                        completed_anchor: None,
                    },
                    None,
                ));
            }
            return Err(SourceError::Other(anyhow::anyhow!(
                "source {}: a snapshot generation needs the pipeline's sink \
                 cohort, which was not set",
                self.id
            )));
        };
        let inputs = MyGenerationInputs {
            queue,
            checkpoints: Arc::clone(chkpt_store),
            source_id: self.id.clone(),
            lineage,
            fingerprint: self.config_fingerprint(),
            policy: crate::snapshot_queue::PolicySnapshot::from(&cohort),
            mode: self.snapshot_cfg.mode.clone(),
            engine: MyOrder,
            anchor_of: my_anchor_of,
            legacy: self.legacy_proof(chkpt_store).await?,
        };
        let decided = crate::snapshot_driver::decide(
            &inputs,
            &self.incidents(),
            &self.snapshot_cohort,
        )
        .await
        .map_err(|e| crate::snapshot_driver::source_error(&self.id, e))?;
        self.retire_legacy_progress(chkpt_store).await?;
        let start = match decided {
            crate::snapshot_driver::Decided::Stream { completed } => {
                MyStart::Stream {
                    completed_anchor: completed
                        .as_ref()
                        .and_then(|c| c.anchor.as_ref())
                        .and_then(my_anchor_of),
                }
            }
            crate::snapshot_driver::Decided::Generation {
                version,
                control,
            } => {
                // The lineage was captured at startup, not by the snapshot.
                crate::snapshot_probe::record_fixed(
                    crate::snapshot_probe::FixedOp::GenerationAllocation,
                );
                MyStart::Generation {
                    version,
                    control: Box::new(control),
                }
            }
        };
        Ok((start, Some(inputs)))
    }

    /// Discover the tables to copy (keyset pages of the catalog, filtered by
    /// the CDC matcher) and prepare each exactly once - after a failover its
    /// drift policy first (before its schema is loaded or registered), then
    /// one schema resolution, identity resolution and type validation, cursor
    /// kind - and store it as an immutable plan item of `control`'s
    /// generation, page by page: the plan is never resident in full. Keyless
    /// tables or unsupported identity types fail here, before any row. The
    /// plan bounds are checked as it grows (design section 9).
    async fn plan_generation(
        &self,
        loader: &MySqlSchemaLoader,
        drift: Option<&DriftCheck<'_>>,
        version: u64,
        control: &crate::snapshot_queue::GenerationControl,
    ) -> SourceResult<crate::snapshot_queue::PlanSummary> {
        use crate::identity_resolution::{
            IdentitySchemaView, resolve_identity,
        };
        use crate::snapshot_driver::incidents as drafts;

        let queue = self.queue();
        let started = std::time::Instant::now();
        let fetches_before = loader.live_fetch_count();
        let mut conn = loader.discovery_conn().await?;
        crate::snapshot_probe::record_fixed(
            crate::snapshot_probe::FixedOp::CatalogSession,
        );
        let mut discovery = crate::snapshot_discovery::Discovery::new(
            &self.tables,
            self.snapshot_cfg.discovery_page_size,
        );
        let mut digest = crate::snapshot_queue::PlanDigest::default();
        let (mut items, mut bytes, mut warned) = (0u64, 0u64, false);
        let (max_items, max_bytes) = (
            self.snapshot_cfg.max_plan_items,
            self.snapshot_cfg.max_plan_bytes,
        );
        while !discovery.is_done() {
            let t = std::time::Instant::now();
            let rows = mysql_schema_loader::discovery_page(
                &mut conn,
                &self.tables,
                discovery.after(),
                discovery.page_size(),
            )
            .await?;
            crate::snapshot_probe::record_discovery_time(t.elapsed());
            let page = discovery.accept(rows)?;
            crate::snapshot_probe::after_discovery_page().await;
            if let Some(d) = drift {
                for (db, table) in &page {
                    mysql_failover_drift::check(
                        &d.env,
                        d.anchor,
                        db,
                        table,
                        mysql_failover_drift::FirstEvent::Snapshot,
                    )
                    .await?;
                }
            }
            loader.warm_from_registry(&page).await?;
            for (db, table) in page {
                let loaded = loader.load_schema(&db, &table).await?;
                let schema = &loaded.schema;
                let col_names: Vec<String> =
                    schema.columns.iter().map(|c| c.name.clone()).collect();
                let fqn = format!("{db}.{table}");
                let opts = self.table_options.get(&fqn);
                let view = IdentitySchemaView {
                    columns: &col_names,
                    primary_key: &schema.primary_key,
                    unique_constraints: &[],
                };
                let resolved = resolve_identity(
                    &db,
                    &table,
                    &view,
                    opts.and_then(|o| o.identity_columns.as_deref()),
                    opts.map(|o| o.assume_unique).unwrap_or(false),
                )
                .map_err(|e| SourceError::Other(anyhow::anyhow!(e)))?;
                for name in &resolved.columns {
                    let col = schema.column(name).ok_or_else(|| {
                        SourceError::Other(anyhow::anyhow!(
                            "identity column {name:?} vanished from schema"
                        ))
                    })?;
                    mysql_identity_kind(col)
                        .map_err(|e| SourceError::Other(anyhow::anyhow!(e)))?;
                }
                let item = crate::snapshot_queue::PlanItem {
                    item_format: crate::snapshot_queue::ITEM_FORMAT,
                    qualifier: db,
                    table,
                    identity: serde_json::to_value(&resolved.columns)
                        .map_err(|e| SourceError::Other(e.into()))?,
                    cursor_kind: mysql_snapshot::mysql_cursor_kind(schema),
                    schema_version: loaded.registry_version as u64,
                    signature: mysql_snapshot::mysql_schema_signature(schema),
                };
                let (key, item_bytes) =
                    queue.put_item(control, &item).await.map_err(|e| {
                        SourceError::Other(anyhow::anyhow!("plan item: {e}"))
                    })?;
                digest.add(&key, &item_bytes);
                items += 1;
                bytes += item_bytes.len() as u64;
                crate::snapshot_probe::record_prepared_table();
                let over = if items > max_items {
                    Some("plan_items")
                } else if bytes > max_bytes {
                    Some("plan_bytes")
                } else {
                    None
                };
                if let Some(class) = over {
                    let draft = drafts::blocked(
                        deltaforge_core::incident::ReasonCode::SnapshotBoundExceeded,
                        &self.id,
                        control.generation,
                        class,
                    );
                    crate::snapshot_driver::block_generation(
                        &queue,
                        &self.incidents(),
                        control.generation,
                        None,
                        version,
                        &draft,
                    )
                    .await
                    .map_err(|e| SourceError::Other(e.into()))?;
                    return Err(SourceError::Incompatible {
                        details: format!(
                            "source {}: the snapshot plan exceeds its bound \
                             ({class}: {items} items, {bytes} bytes; limits \
                             {max_items} items, {max_bytes} bytes); the \
                             generation is blocked until an explicit resnapshot",
                            self.id
                        )
                        .into(),
                    });
                }
                if !warned
                    && (items * 5 >= max_items * 4
                        || bytes * 5 >= max_bytes * 4)
                {
                    warned = true;
                    let _ = self
                        .incidents()
                        .raise(
                            &drafts::bound_warning(
                                &self.id,
                                control.generation,
                                "plan_storage",
                            ),
                            1,
                        )
                        .await;
                }
            }
        }
        drop(conn);
        loader.check_binlog_row_image().await?;
        crate::snapshot_probe::record_fixed(
            crate::snapshot_probe::FixedOp::RowImageCheck,
        );
        crate::snapshot_probe::record_preparation(
            started.elapsed(),
            bytes as usize,
            loader.live_fetch_count() - fetches_before,
        );
        Ok(digest.seal())
    }

    /// The preflight over the plan (design section 9): server settings, and
    /// the size and storage engine of every planned table, page by page.
    /// Returns the server's binlog retention, when it expires binlogs.
    async fn preflight_plan(
        &self,
        server_uuid: &str,
        generation: u64,
        tables: u64,
    ) -> SourceResult<Option<u64>> {
        let mut report = mysql_health::run_preflight_verified(
            self.dsn.expose(),
            server_uuid,
            &[] as &[(&str, &str)],
            self.snapshot_cfg.max_parallel_tables,
        )
        .await
        .map_err(|e| typed(e, "snapshot preflight"))?;
        let mut conn = open_control_connection(
            self.dsn.expose(),
            server_uuid,
            CONTROL_CONNECT_TIMEOUT,
        )
        .await
        .map_err(|e| e.into_source_error(server_uuid))?;
        let mut plan = mysql_snapshot::PlanPages::new(
            self.queue(),
            generation,
            self.snapshot_cfg.discovery_page_size,
        );
        let (mut total, mut page) = (0u64, Vec::new());
        loop {
            let next = plan.next().await.map_err(SourceError::Other)?;
            if let Some(t) = &next {
                page.push((t.qualifier.clone(), t.table.clone()));
            }
            if page.len() >= 1_000 || (next.is_none() && !page.is_empty()) {
                total += mysql_health::size_and_engines(
                    &mut conn,
                    &page,
                    &mut report,
                )
                .await;
                page.clear();
            }
            if next.is_none() {
                break;
            }
        }
        conn.disconnect().await.ok();
        report.apply_size_estimate(
            total,
            tables as usize,
            self.snapshot_cfg.max_parallel_tables,
        );
        crate::snapshot_probe::record_fixed(
            crate::snapshot_probe::FixedOp::Preflight,
        );
        report.emit(&self.id, tables as usize);
        let retention = report.retention_secs;
        if !report.hard_errors.is_empty() {
            let details = report.hard_errors.join("; ");
            return Err(if report.permission_error {
                SourceError::Permission {
                    details: details.into(),
                }
            } else {
                SourceError::Incompatible {
                    details: details.into(),
                }
            });
        }
        Ok(retention)
    }

    /// After a failure of a running generation: when the control record no
    /// longer shows this run's generation, another process took over, and
    /// this one stops (a non-blocking `concurrent_owner` incident; the
    /// current owner's control record is never written).
    async fn concurrent_owner(
        &self,
        generation: u64,
        run: &str,
    ) -> Option<SourceError> {
        crate::snapshot_driver::owner_lost(
            &self.queue(),
            &self.incidents(),
            &self.id,
            generation,
            run,
        )
        .await
        .map(|why| SourceError::Incompatible {
            details: format!("source {}: {why}", self.id).into(),
        })
    }

    /// What this source's legacy snapshot progress proves (design section
    /// 10, the #131 rules), read only when the control record is a legacy
    /// one: its anchor (in the verified lineage, which the startup
    /// reconciliation already adopted pre-lineage checkpoints into) and the
    /// generation its completion mark names. A malformed anchor proves
    /// nothing; a corrupt record is refused, untouched.
    async fn legacy_proof(
        &self,
        chkpt_store: &Arc<dyn CheckpointStore>,
    ) -> SourceResult<
        Option<crate::snapshot_driver::LegacyProof<MySqlCheckpoint>>,
    > {
        use crate::snapshot_driver::LegacyProof;
        let legacy = matches!(
            self.queue().read().await,
            Ok(Some(crate::snapshot_queue::Stored::Legacy { .. }))
        );
        if !legacy {
            return Ok(None);
        }
        let progress = match mysql_snapshot::load_snapshot_progress(
            chkpt_store.as_ref(),
            &self.id,
        )
        .await
        {
            Ok(p) => p,
            Err(e) => {
                return Ok(Some(LegacyProof {
                    refused: Some(format!("{e:#}")),
                    ..Default::default()
                }));
            }
        };
        if progress.start_position.is_empty() {
            return Ok(Some(LegacyProof {
                unproven: Some("its progress records no anchor".into()),
                ..Default::default()
            }));
        }
        let mut anchor: MySqlCheckpoint =
            match serde_json::from_str(&progress.start_position) {
                Ok(a) => a,
                Err(e) => {
                    return Ok(Some(LegacyProof {
                        unproven: Some(format!(
                            "its recorded anchor is malformed: {e}"
                        )),
                        ..Default::default()
                    }));
                }
            };
        if anchor.lineage.is_none() {
            anchor.lineage = checkpoint_lineage(&self.registry_scope);
        }
        Ok(Some(LegacyProof {
            anchor: Some((
                anchor.clone(),
                crate::snapshot_queue::EngineAnchor::Mysql {
                    file: anchor.file,
                    pos: anchor.pos,
                    gtid_set: anchor.gtid_set,
                    lineage: anchor.lineage,
                },
            )),
            mark_generation: (progress.generation != 0)
                .then_some(progress.generation),
            ..Default::default()
        }))
    }

    /// Once the control record is this release's, the legacy progress record
    /// is no longer read: delete it (idempotent; design section 3.3).
    async fn retire_legacy_progress(
        &self,
        chkpt_store: &Arc<dyn CheckpointStore>,
    ) -> SourceResult<()> {
        let current = matches!(
            self.queue().read().await,
            Ok(Some(crate::snapshot_queue::Stored::Current { .. }))
        );
        if current {
            chkpt_store
                .delete(&mysql_snapshot::progress_key(&self.id))
                .await
                .map_err(|e| SourceError::Checkpoint {
                    details: format!(
                        "retire the legacy snapshot progress: {e}"
                    )
                    .into(),
                })?;
        }
        Ok(())
    }

    /// Run generation `control` (design sections 2 to 6): the snapshot
    /// connections, the plan, the preflight, the anchor under the read lock
    /// (every worker's consistent snapshot opened under it, the plan
    /// verified under it), the start barrier, the copy, the final check,
    /// `rows_produced` and the terminal barrier. Returns the anchor, where
    /// the stream starts.
    #[allow(clippy::too_many_arguments)]
    async fn run_generation(
        &self,
        version: u64,
        control: crate::snapshot_queue::GenerationControl,
        inputs: &MyGenerationInputs,
        loader: &MySqlSchemaLoader,
        drift: Option<&DriftCheck<'_>>,
        server_uuid: &str,
        tx: &mpsc::Sender<SourceItem>,
        cancel: &CancellationToken,
    ) -> SourceResult<MyGenerationRun> {
        use crate::snapshot_permits::{SnapshotPermits, global_cap, validate};
        let other = |e: anyhow::Error| SourceError::Other(e);
        let queue = &inputs.queue;
        let run = format!("{:032x}", rand::random::<u128>());
        let t0 = std::time::Instant::now();

        // 1. The snapshot's connections, before any catalog session, lock or
        // anchor; cancellable while queued. Extra workers only when free now.
        let cfg = &self.snapshot_cfg;
        let source_cap = cfg.snapshot_connection_cap();
        let most =
            u32::try_from(cfg.max_parallel_tables.max(1).saturating_add(2))
                .unwrap_or(u32::MAX);
        validate(global_cap(), source_cap, MY_BASE_SNAPSHOT_CONNECTIONS, most)
            .map_err(|e| SourceError::Incompatible {
                details: format!("source {}: {e}", self.id).into(),
            })?;
        let permits = SnapshotPermits::acquire(
            &self.pipeline,
            source_cap,
            MY_BASE_SNAPSHOT_CONNECTIONS,
            cancel,
        )
        .await
        .map_err(|_| SourceError::Cancelled)?;

        // 2. The plan, page by page, and the preflight over it.
        let plan = self
            .plan_generation(loader, drift, version, &control)
            .await?;
        let retention = self
            .preflight_plan(server_uuid, control.generation, plan.items)
            .await?
            .map(Duration::from_secs);
        let mut extras = Vec::new();
        while extras.len() + 1 < cfg.max_parallel_tables.max(1)
            && (extras.len() as u64 + 1) < plan.items.max(1)
        {
            match permits.try_extra() {
                Some(p) => extras.push(p),
                None => break,
            }
        }
        let workers = 1 + extras.len();

        // 3. The anchor: every worker's consistent snapshot opened under the
        // read lock, the position captured and the plan verified under it.
        crate::snapshot_probe::before_anchor().await;
        let (worker_conns, mut position) =
            mysql_snapshot::acquire_locked_anchor(
                self.dsn.expose(),
                server_uuid,
                workers,
                Duration::from_secs(cfg.lock_timeout_secs.max(1)),
                mysql_snapshot::PlanCheck {
                    patterns: &self.tables,
                    page_size: cfg.discovery_page_size,
                    expected: mysql_snapshot::PlanPages::new(
                        queue.clone(),
                        control.generation,
                        cfg.discovery_page_size,
                    ),
                },
            )
            .await
            .map_err(|e| {
                e.downcast::<SourceError>().unwrap_or_else(|e| {
                    other(e.context("acquire locked snapshot anchor"))
                })
            })?;
        crate::snapshot_probe::record_fixed(
            crate::snapshot_probe::FixedOp::Anchor,
        );
        position.lineage = checkpoint_lineage(&self.registry_scope);
        crate::snapshot_probe::record_phase(
            crate::snapshot_probe::Phase::PreflightAnchor,
            t0.elapsed(),
        );

        // 4. Seal the plan, record the anchor and this run as the owner.
        let (version, control) = queue
            .seal_and_run(
                version,
                &control,
                plan,
                crate::snapshot_queue::EngineAnchor::Mysql {
                    file: position.file.clone(),
                    pos: position.pos,
                    gtid_set: position.gtid_set.clone(),
                    lineage: position.lineage.clone(),
                },
                &run,
                chrono::Utc::now().timestamp_millis(),
            )
            .await
            .map_err(|e| {
                other(anyhow::anyhow!("seal the snapshot plan: {e}"))
            })?;
        info!(
            source_id = %self.id,
            generation = control.generation,
            file = %position.file,
            pos = position.pos,
            tables = control.plan.items,
            workers,
            "snapshot generation running"
        );

        // The guard: the anchor's binlog, the anchor age and the binlog
        // retention block the generation and stop the copy.
        let guard_version =
            Arc::new(std::sync::atomic::AtomicU64::new(version));
        let gen_cancel = cancel.child_token();
        let stopped: Arc<std::sync::Mutex<Option<mysql_snapshot::GuardStop>>> =
            Default::default();
        let guard = mysql_snapshot::spawn_generation_guard(
            mysql_snapshot::GenerationGuard {
                dsn: self.dsn.clone(),
                expected_uuid: server_uuid.to_string(),
                captured_file: position.file.clone(),
                source_id: self.id.clone(),
                generation: control.generation,
                run: run.clone(),
                version: Arc::clone(&guard_version),
                anchored_at_ms: control.anchored_at_ms.unwrap_or_default(),
                max_anchor_age: Duration::from_secs(cfg.max_anchor_age_secs),
                retention,
                queue: queue.clone(),
                incidents: self.incidents(),
                cancel: gen_cancel.clone(),
                stopped: Arc::clone(&stopped),
            },
        );
        let _guard = scopeguard::guard(guard, |g| g.abort());
        let stopped_err = |stopped: &std::sync::Mutex<
            Option<mysql_snapshot::GuardStop>,
        >| {
            stopped.lock().expect("not poisoned").clone().map(|why| match why {
                    mysql_snapshot::GuardStop::WrongServer(why) => {
                        SourceError::Lineage {
                            details: format!("source {}: {why}", self.id).into(),
                        }
                    }
                    mysql_snapshot::GuardStop::Blocked(why) => {
                        SourceError::Incompatible {
                            details: format!(
                                "source {}: snapshot generation {} stopped: {why}",
                                self.id, control.generation
                            )
                            .into(),
                        }
                    }
                })
        };

        // 5. The start barrier: no row before the whole cohort entered the
        // generation.
        let publisher =
            Arc::new(crate::snapshot_publish::GenerationPublisher::new(
                queue.clone(),
                tx.clone(),
                control.clone(),
                &run,
                crate::snapshot_position::encode_chained(
                    &control.snapshot_chain,
                    control.generation,
                    &position,
                ),
            ));
        if let Err(e) = publisher.start().await {
            return Err(self
                .concurrent_owner(control.generation, &run)
                .await
                .unwrap_or_else(|| other(e.into())));
        }
        let adopted = crate::snapshot_driver::await_adoption(
            &inputs.input(),
            &gen_cancel,
        )
        .await;
        match adopted {
            Ok((v, _)) => {
                guard_version.store(v, std::sync::atomic::Ordering::SeqCst)
            }
            Err(e) => {
                let e = match stopped_err(&stopped) {
                    Some(b) => b,
                    None if cancel.is_cancelled() => SourceError::Cancelled,
                    None => {
                        other(anyhow::anyhow!("snapshot start barrier: {e}"))
                    }
                };
                return Err(self
                    .concurrent_owner(control.generation, &run)
                    .await
                    .unwrap_or(e));
            }
        }

        // 6. The copy.
        let copy_started = std::time::Instant::now();
        let copied =
            mysql_snapshot::copy_generation(mysql_snapshot::MyCopyCtx {
                source_id: &self.id,
                pipeline: &self.pipeline,
                tenant: &self.tenant,
                cfg,
                schema_loader: loader,
                cancel: gen_cancel.clone(),
                plan: mysql_snapshot::PlanPages::new(
                    queue.clone(),
                    control.generation,
                    cfg.discovery_page_size,
                ),
                workers: worker_conns,
                publisher: Arc::clone(&publisher),
                generation: control.generation,
                lineage: control.lineage.clone(),
            })
            .await;
        crate::snapshot_probe::record_phase(
            crate::snapshot_probe::Phase::RowCopy,
            copy_started.elapsed(),
        );
        if let Some(b) = stopped_err(&stopped) {
            return Err(b);
        }
        // The source stopping mid-copy is a stop, not a failure: the
        // generation is replaced at the next start.
        if copied.is_err() && cancel.is_cancelled() {
            return Err(SourceError::Cancelled);
        }
        if let Err(e) = copied {
            return Err(self
                .concurrent_owner(control.generation, &run)
                .await
                .unwrap_or_else(|| other(e)));
        }
        drop(extras);
        drop(permits);

        // 7. The anchor's binlog still there. The rows are recorded produced
        // only after the anchor baselines too ([`Self::finish_generation`]).
        let final_started = std::time::Instant::now();
        mysql_health::verify_binlog_position(
            self.dsn.expose(),
            server_uuid,
            &position.file,
        )
        .await
        .map_err(|e| typed(e, "post-snapshot binlog position verification"))?;
        crate::snapshot_probe::record_phase(
            crate::snapshot_probe::Phase::Finalization,
            final_started.elapsed(),
        );
        Ok(MyGenerationRun {
            anchor: position,
            generation: control.generation,
            run,
            publisher,
        })
    }

    /// The last step of a generation, once every final check passed - the
    /// anchor baselines of its tables established included: `rows_produced`,
    /// then the terminal barrier with the completing position. Only from
    /// here can the generation complete (design section 4); a crash before
    /// it leaves the generation `running`, replaced at the next start.
    async fn finish_generation(
        &self,
        ran: &MyGenerationRun,
    ) -> SourceResult<()> {
        let other = |e: anyhow::Error| SourceError::Other(e);
        let queue = self.queue();
        let current = match queue.read().await {
            Ok(Some(crate::snapshot_queue::Stored::Current {
                version,
                control,
            })) => (version, *control),
            other_state => {
                return Err(self
                    .concurrent_owner(ran.generation, &ran.run)
                    .await
                    .unwrap_or_else(|| {
                        other(anyhow::anyhow!(
                            "the snapshot control record changed before the \
                             rows were produced: {other_state:?}"
                        ))
                    }));
            }
        };
        let produced = match queue
            .rows_produced(current.0, &current.1, &ran.run)
            .await
        {
            Ok((_, produced)) => produced,
            Err(e) => {
                return Err(self
                    .concurrent_owner(ran.generation, &ran.run)
                    .await
                    .unwrap_or_else(|| {
                        other(anyhow::anyhow!("record the rows produced: {e}"))
                    }));
            }
        };
        let completing = serde_json::to_vec(&MySqlCheckpoint {
            snapshot_completed: Some(produced.generation),
            snapshot_chain: Some(produced.snapshot_chain.clone()),
            ..ran.anchor.clone()
        })
        .map_err(|e| other(e.into()))?;
        if let Err(e) = ran.publisher.terminal(completing).await {
            return Err(self
                .concurrent_owner(ran.generation, &ran.run)
                .await
                .unwrap_or_else(|| other(e.into())));
        }
        info!(
            source_id = %self.id,
            generation = produced.generation,
            file = %ran.anchor.file,
            pos = ran.anchor.pos,
            "snapshot rows produced; the stream continues from the anchor"
        );
        crate::snapshot_probe::after_terminal().await;
        Ok(())
    }
}

impl MySqlSource {
    /// Whether unfinished legacy snapshot progress is ambiguous: among the
    /// tables the configured patterns expand to now (paged discovery, the
    /// same canonical `db.table` identities the snapshot records), some are
    /// done and some pending.
    async fn legacy_progress_is_ambiguous(
        &self,
        progress: &mysql_snapshot::MysqlSnapshotProgress,
    ) -> SourceResult<bool> {
        let mut conn = mysql_async::Conn::from_url(self.dsn.expose())
            .await
            .map_err(|e| SourceError::Connect {
                details: format!(
                    "discover tables to check snapshot progress: {e}"
                )
                .into(),
            })?;
        let mut discovery = crate::snapshot_discovery::Discovery::new(
            &self.tables,
            self.snapshot_cfg.discovery_page_size,
        );
        let mut scan = crate::snapshot_frontier::LegacyProgressScan::default();
        while !discovery.is_done() && !scan.settled() {
            let rows = mysql_schema_loader::discovery_page(
                &mut conn,
                &self.tables,
                discovery.after(),
                discovery.page_size(),
            )
            .await?;
            for (db, table) in discovery.accept(rows)? {
                scan.observe(progress.table_done(&db, &table));
            }
        }
        conn.disconnect().await.ok();
        Ok(scan.ambiguous(progress.finished))
    }

    /// The resume position in the checkpoint store, classified (fails closed
    /// on bytes that are neither a snapshot position nor a stream position).
    async fn resume_position(
        &self,
        store: &Arc<dyn CheckpointStore>,
    ) -> SourceResult<Option<MyResumePosition>> {
        store
            .get_raw(&self.id)
            .await
            .map_err(|e| SourceError::Checkpoint {
                details: e.to_string().into(),
            })?
            .map(|raw| classify_mysql_checkpoint(&raw))
            .transpose()
            .map_err(|e| SourceError::Checkpoint {
                details: format!("source {}: {e}", self.id).into(),
            })
    }

    /// Freeze the source lineage for snapshot identity: prefer the stable
    /// `@@server_uuid` (file-free, rotation-immune); fall back to
    /// `server_id` + the current binlog file.
    async fn capture_snapshot_lineage(&self) -> SourceResult<PersistedLineage> {
        use mysql_async::{Pool, Row, prelude::Queryable};
        let pool = Pool::new(self.dsn.expose());
        let mut conn = pool
            .get_conn()
            .await
            .map_err(|e| SourceError::Other(e.into()))?;
        let uuid: Option<String> = conn
            .query_first("SELECT @@global.server_uuid")
            .await
            .map_err(|e| SourceError::Other(e.into()))?;
        if let Some(u) = uuid {
            if let Some(bytes) = mysql_event_id::parse_uuid16(&u) {
                return Ok(PersistedLineage::MysqlGtid { source_uuid: bytes });
            }
        }
        // Non-GTID fallback lineage = (server_id, current binlog file). Both must
        // be real: a zero server_id or an empty/absent binlog file is an unverified
        // lineage. The build step returns an error rather than a bogus anchor, and
        // the caller propagates it with `?` at startup, so the source fails closed
        // before opening a stream instead of binding to a meaningless identity.
        let server_id: u32 = conn
            .query_first("SELECT @@server_id")
            .await
            .map_err(|e| SourceError::Other(e.into()))?
            .unwrap_or(0);
        let row: Option<Row> =
            match conn.query_first("SHOW BINARY LOG STATUS").await {
                Ok(r) => r,
                Err(_) => conn
                    .query_first("SHOW MASTER STATUS")
                    .await
                    .map_err(|e| SourceError::Other(e.into()))?,
            };
        let file: String = row
            .and_then(|mut r| r.take::<String, _>(0))
            .unwrap_or_default();
        mysql_server_lineage(server_id, file)
    }

    async fn run_inner(
        &self,
        tx: mpsc::Sender<SourceItem>,
        chkpt_store: Arc<dyn CheckpointStore>,
        cancel: CancellationToken,
        paused: Arc<AtomicBool>,
        pause_notify: Arc<Notify>,
        ready: deltaforge_core::SourceReady,
    ) -> SourceResult<()> {
        // Verify the source lineage ONCE, before any snapshot, stream, or RunCtx.
        // Fail closed if it cannot be established: a swallowed error here would
        // proceed to snapshot/stream with no durable-watermark identity, leaving
        // correctness to downstream consumers instead of stopping the source. The
        // same verified value anchors both the snapshot generation and the CDC
        // durable watermarks.
        let durable_lineage = self.capture_snapshot_lineage().await?;

        // Verify the server_uuid lineage and establish the schema-registry scope
        // BEFORE any registry access (the snapshot preload below included), in
        // every binlog mode: persist it durably (fail closed), then publish it
        // to this pipeline's loaders. The snapshot lineage above was read on
        // another connection: both must name one server before anything is
        // recorded. From here on every connection proves it is this server
        // before it is trusted.
        let root = fetch_identity_verified(self.dsn.expose()).await?;
        let same_server = match (&durable_lineage, &root) {
            (
                PersistedLineage::MysqlGtid { source_uuid },
                ServerIdentity::MySql(id),
            ) => {
                mysql_event_id::parse_uuid16(&id.server_uuid)
                    == Some(*source_uuid)
            }
            _ => false,
        };
        if !same_server {
            return Err(SourceError::Lineage {
                details: format!(
                    "startup connections disagree on the server: snapshot \
                     lineage {durable_lineage:?}, identity {root:?}"
                )
                .into(),
            });
        }
        // A binlog that may miss applied transactions proves nothing:
        // refused before anything durable, any snapshot or any stream.
        let ServerIdentity::MySql(root_id) = &root else {
            return Err(SourceError::Other(anyhow::anyhow!(
                "MySQL source verified a non-MySQL identity"
            )));
        };
        require_complete_binlog(self.dsn.expose(), &root_id.server_uuid)
            .await?;
        establish_scope(
            &self.backend,
            &self.registry_scope,
            &self.tenant,
            &self.id,
            mysql_registry_lineage(&root)?,
        )
        .await?;

        // Checkpoints carry the verified lineage. Before anything reads
        // snapshot progress or the resume position, reconcile the stored
        // checkpoints with it: adopt pre-lineage ones (no lineage change ever
        // recorded), carry a failover predecessor's GTID checkpoints over after
        // verifying them on the current server, and otherwise STOP here.
        let scope = self.registry_scope.current()?;
        let lineage_hash = scope.lineage().lineage_hash.clone();
        let storage::adapters::LineageDescriptor::Mysql { server_uuid } =
            scope.lineage().descriptor.clone()
        else {
            return Err(SourceError::Other(anyhow::anyhow!(
                "MySQL source established a non-MySQL lineage"
            )));
        };
        mysql_checkpoint_lineage::reconcile_checkpoint_lineage(
            chkpt_store.as_ref(),
            &self.backend,
            &self.tenant,
            &self.id,
            &lineage_hash,
            &mysql_checkpoint_lineage::LiveGtidAvailability {
                dsn: self.dsn.expose(),
                server_uuid: server_uuid.clone(),
            },
        )
        .await?;

        // A failover found at startup is anchored before anything is
        // snapshotted or streamed: F is the committed (carried-over) GTID
        // position, if this server has executed it (else unknown: every
        // table's drift check is then unprovable). Each table's drift policy
        // applies lazily at its first event (`mysql_failover_drift`).
        if let IdentityComparison::Changed { previous, .. } =
            IdentityStore::new(Arc::clone(&self.backend))
                .compare(&self.id, &root)
                .await
                .map_err(SourceError::Other)?
        {
            let committed = match self.resume_position(&chkpt_store).await? {
                Some(MyResumePosition::Stream(cp)) => Some(cp),
                _ => None,
            };
            let position = match committed.and_then(|c| c.gtid_set) {
                Some(set) => {
                    let mut conn = open_control_connection(
                        self.dsn.expose(),
                        &server_uuid,
                        CONTROL_CONNECT_TIMEOUT,
                    )
                    .await
                    .map_err(|e| e.into_source_error(&server_uuid))?;
                    let proven =
                        mysql_health::require_gtid_executed(&mut conn, &set)
                            .await
                            .is_ok();
                    conn.disconnect().await.ok();
                    proven.then(|| MySqlCheckpoint {
                        file: String::new(),
                        pos: 0,
                        gtid_set: Some(set),
                        lineage: Some(lineage_hash.clone()),
                        snapshot_completed: None,
                        snapshot_chain: None,
                    })
                }
                None => None,
            };
            mysql_failover_drift::record_anchor(
                &self.backend,
                &self.tenant,
                &self.id,
                &mysql_registry_lineage(&previous)?.lineage_hash(),
                position,
            )
            .await
            .map_err(SourceError::Other)?;
        }

        // The snapshot decision is the generation driver's (design section
        // 4): an incomplete generation is replaced, never resumed as a
        // stream; a completed one streams.
        let resume = self.resume_position(&chkpt_store).await?;
        // The stream position a snapshot-free start resumes from.
        let stream_resume = match &resume {
            Some(MyResumePosition::Stream(cp)) => Some(cp.clone()),
            _ => None,
        };
        let (start, generation_inputs) = self
            .decide_snapshot(&chkpt_store, durable_lineage.clone())
            .await?;
        let needs_snapshot = matches!(start, MyStart::Generation { .. });
        // Whether this start continues from a committed resume position.
        let committed_resume = stream_resume.is_some() && !needs_snapshot;

        // The generation this start ran, if any.
        let mut generation_run: Option<MyGenerationRun> = None;
        // The anchor a snapshot of this start, or the completed generation,
        // leaves the stream at.
        let mut snapshot_start: Option<MySqlCheckpoint> = None;
        match start {
            MyStart::Stream { completed_anchor } => {
                if stream_resume.is_none() {
                    snapshot_start = completed_anchor.map(|mut a| {
                        a.lineage = checkpoint_lineage(&self.registry_scope);
                        a
                    });
                }
            }
            MyStart::Generation { version, control } => {
                info!(source_id = %self.id, "starting the snapshot generation");
                let snap_schema_loader = MySqlSchemaLoader::new(
                    self.dsn.clone(),
                    self.registry.clone(),
                    &self.tenant,
                    self.registry_scope.clone(),
                );
                // After a failover, each snapshotted table's drift policy
                // applies before its schema is loaded or registered and
                // before any snapshot row (Round 38): in the planning pass,
                // page by page, from the one discovery.
                let drift_anchor = mysql_failover_drift::load_anchor(
                    &self.backend,
                    &self.tenant,
                    &self.id,
                )
                .await
                .map_err(SourceError::Other)?;
                let drift_scope = self.registry_scope.current()?;
                let drift = match &drift_anchor {
                    Some(anchor) => Some(DriftCheck {
                        anchor,
                        env: mysql_failover_drift::DriftEnv {
                            backend: &self.backend,
                            loader: &snap_schema_loader,
                            scope: &drift_scope,
                            dsn: self.dsn.expose(),
                            server_uuid: &server_uuid,
                            source_id: &self.id,
                            halt: self.on_schema_drift
                                == deltaforge_config::OnSchemaDrift::Halt,
                            lower_case_table_names:
                                fetch_lower_case_table_names(
                                    self.dsn.expose(),
                                    &server_uuid,
                                )
                                .await?,
                        },
                    }),
                    None => None,
                };
                let ran = self
                    .run_generation(
                        version,
                        *control,
                        generation_inputs
                            .as_ref()
                            .expect("a generation has its inputs"),
                        &snap_schema_loader,
                        drift.as_ref(),
                        &server_uuid,
                        &tx,
                        &cancel,
                    )
                    .await?;
                // The stream starts at the anchor. No checkpoint is written
                // here: only the sinks' commits record what was delivered.
                snapshot_start = Some(ran.anchor.clone());
                generation_run = Some(ran);
                info!(source_id = %self.id, "snapshot rows produced, starting binlog streaming");
            }
        }

        // binlog streaming - retry prepare_client on transient connection errors
        // (MySQL or toxiproxy may not be ready yet during startup).
        let (host, default_db, server_id, client) = {
            let mut retry = common::retry::RetryPolicy::default();
            loop {
                match prepare_client(
                    self.dsn.expose(),
                    &server_uuid,
                    &self.id,
                    snapshot_start.clone().or_else(|| stream_resume.clone()),
                )
                .await
                {
                    Ok(result) => break result,
                    Err(e) => {
                        let source_err: SourceError = e.into();
                        let retryable = matches!(
                            &source_err,
                            SourceError::Connect { .. }
                                | SourceError::Io(_)
                                | SourceError::Timeout { .. }
                        );
                        if retryable {
                            let delay = retry
                                .next_backoff()
                                .min(std::time::Duration::from_secs(60));
                            warn!(
                                source_id = %self.id,
                                error = %source_err,
                                delay_secs = delay.as_secs(),
                                "prepare_client failed, retrying after backoff"
                            );
                            tokio::select! {
                                _ = tokio::time::sleep(delay) => {}
                                _ = cancel.cancelled() => {
                                    return Err(SourceError::Cancelled);
                                }
                            }
                        } else {
                            return Err(source_err);
                        }
                    }
                }
            }
        };

        // Capture the original checkpoint position BEFORE any adjustment.
        // This is used later by check_position_reachability to determine whether
        // A's position actually exists on B - even if we've already switched the
        // connection to B's tail.
        let checkpoint_gtid =
            client.gtid_enabled.then(|| client.gtid_set.clone());
        let checkpoint_file = client.binlog_filename.clone();

        // Fetch and verify the live server identity ONCE, in every mode, before
        // opening the binlog stream. Reused below for the pre-connect position
        // decision and for the durable identity resolution. Fails closed: the
        // stream must not open on an unverified server.
        let live =
            fetch_identity_verified_as(self.dsn.expose(), &server_uuid).await?;
        let lower_case_table_names =
            fetch_lower_case_table_names(self.dsn.expose(), &server_uuid)
                .await?;

        // A failover (stored identity != live) is reconciled below, before
        // the stream opens, against the EXACT position the stream will start
        // from: the (carried-over) checkpoint GTID set. A file/position start
        // across servers cannot be proven and stops there.
        let init_gtid = client.gtid_enabled.then(|| client.gtid_set.clone());
        let init_file = client.binlog_filename.clone();
        let init_pos = client.binlog_position as u64;

        info!(source_id=%self.id, "prepare_client finished, loading schemas");
        let schema_loader = MySqlSchemaLoader::new(
            self.dsn.clone(),
            self.registry.clone(),
            &self.tenant,
            self.registry_scope.clone(),
        );

        // NOTE: preload deferred until after check_identity_post_reconnect so the
        // registry still holds A's schema when the reconciler computes the drift diff.

        info!(
            source_id=%self.id,
            host=%host,
            db=%default_db,
            "mysql source starting ...");

        let backend = Arc::clone(&self.backend);

        let mut ctx = RunCtx {
            source_id: self.id.clone(),
            pipeline: self.pipeline.clone(),
            table_metrics: deltaforge_core::table_metrics::for_pipeline(
                &self.pipeline,
            ),
            tenant: self.tenant.clone(),
            dsn: self.dsn.clone(),
            host,
            default_db,
            server_id,
            tx,
            chkpt: chkpt_store,
            cancel,
            paused,
            pause_notify,
            schema: schema_loader,
            allow: AllowList::new(&self.tables),
            retry: RetryPolicy::default(),
            identity_store: IdentityStore::new(Arc::clone(&backend)),
            reconciler: SchemaReconciler::new(
                Arc::clone(&self.registry),
                Arc::clone(&backend),
            ),
            registry_scope: self.registry_scope.clone(),
            registry_backend: Arc::clone(&backend),
            inactivity: Duration::from_secs(60),
            table_map: HashMap::new(),
            last_file: init_file,
            last_pos: init_pos,
            last_gtid: init_gtid,
            current_gtid: None,
            in_explicit_txn: false,
            message_ordinal: 0,
            query_ordinal: 0,
            rows_ordinal: 0,
            lower_case_table_names,
            txn_eval: None,
            txn_eval_cp: None,
            selection: mysql_selection::Caches::new(&self.pipeline, &self.id),
            failover: None,
            drift_checked: Default::default(),
            drift_resolver: crate::incident_drafts::DriftResolver::new(
                Arc::clone(&self.backend),
                &self.pipeline,
                &self.id,
            ),
            incidents: storage::adapters::incidents::IncidentStore::new(
                Arc::clone(&self.backend),
                &self.pipeline,
            ),
            checkpoint_gtid,
            checkpoint_file,
            outbox_tables: self.outbox_tables.clone(),
            on_schema_drift: self.on_schema_drift.clone(),
            // Verified once above, before the stream opens.
            durable_lineage: Some(durable_lineage),
        };

        // Resolve the identity BEFORE opening the stream: persist FirstSeen,
        // reconcile a detected change, or verify a Same position is still
        // reachable. Reuse the identity already verified above. A failure here
        // means no binlog stream is opened. The registry still holds A's schema
        // for the reconciler's drift diff; preload stays deferred until after.
        let resume = ResumeAt::of(&client);
        check_identity_post_reconnect(&mut ctx, Some(live), &resume).await?;

        // A start that does not continue from the committed resume position
        // invalidates positional proof for every table from here: a
        // lineage-wide barrier at the start position, durable before the
        // stream opens. An ordinary restart at the committed position writes
        // none.
        if needs_snapshot || !committed_resume {
            record_stream_start(&ctx).await?;
        }

        info!(source_id=%self.id, "connecting for binlog stream ..");
        let mut stream = connect_first_stream(&ctx, client).await?;
        ctx.mark_transaction_boundary();

        // No schema is enumerated or loaded here (except the tables a
        // snapshot just copied): each table's version is resolved, and if
        // needed proven by a lazy baseline, at its first rows (design spec
        // 7.21). CDC startup work is independent of the catalog.
        ctx.schema.check_binlog_row_image().await?;
        // A snapshot just copied its plan's tables: prove each at the anchor
        // now (no gap until its first CDC rows), page by page, before the
        // rows are recorded produced and the terminal barrier is sent; then
        // record completion once the frozen policy's frontier covers the
        // terminal (completion reclaims the plan). Nothing has been read
        // from the stream yet.
        if let Some(ran) = &generation_run {
            let mut plan = mysql_snapshot::PlanPages::new(
                self.queue(),
                ran.generation,
                self.snapshot_cfg.discovery_page_size,
            );
            let mut page = Vec::new();
            loop {
                let next = plan.next().await.map_err(SourceError::Other)?;
                if let Some(t) = next.as_ref() {
                    page.push((t.qualifier.clone(), t.table.clone()));
                }
                if page.len() >= self.snapshot_cfg.discovery_page_size.max(1)
                    || (next.is_none() && !page.is_empty())
                {
                    mysql_baseline::establish(&ctx, &page).await?;
                    page.clear();
                }
                if next.is_none() {
                    break;
                }
            }
            // Every final check passed, the baselines included: only now
            // the rows are produced and the terminal barrier is sent, so a
            // completed generation always has its baselines.
            self.finish_generation(ran).await?;
            crate::snapshot_driver::spawn_completion_watch(
                generation_inputs
                    .clone()
                    .expect("a generation has its inputs"),
                self.incidents(),
                Arc::clone(&self.snapshot_cohort),
                ctx.cancel.clone(),
            );
        }

        // Controlled credential rotation (opt-in, file-backed credentials only).
        // GTID mode is mandatory for live rotation - fail startup otherwise. The
        // runtime owns the watcher/manager task; it is cancelled and joined after
        // the loop on every exit path, with `Drop` as the abort backstop.
        let mut rotation = match &self.rotation {
            Some(spec) => {
                mysql_rotation::require_gtid_mode(
                    self.dsn.expose(),
                    &ctx.expected_uuid()?,
                )
                .await?;
                Some(mysql_rotation::MySqlRotationRuntime::spawn(
                    spec,
                    ctx.dsn.clone(),
                    self.id.clone(),
                    self.pipeline.clone(),
                    Arc::clone(&self.backend),
                    ctx.server_id,
                    ctx.default_db.clone(),
                    &ctx.cancel,
                )?)
            }
            None => None,
        };

        // Startup checks passed and the stream is open on the verified
        // server: the source half of the verified-running barrier.
        ready.mark();
        info!("entering binlog read loop");
        // The loop runs inside an async block so its result can be captured and the
        // rotation runtime cancelled+joined before teardown, on both the normal and
        // fatal exit paths. A fatal result (including gate-6 rotation failures) is
        // re-propagated after join and before the teardown checkpoint put.
        let loop_result: SourceResult<()> = async {
            // The stream ended and no reconnect has succeeded yet.
            let mut lost = false;
            loop {
                if !pause_until_resumed(
                    &ctx.cancel,
                    &ctx.paused,
                    &ctx.pause_notify,
                )
                .await
                {
                    info!(source_id=%ctx.source_id, "resuming ..");
                    break;
                }

                // Rotation: schedule Stage-A preflight concurrently with the stream,
                // and apply the Stage-B swap only at a whole-transaction GTID boundary
                // (between transactions, with an established GTID position).
                if let Some(rt) = rotation.as_mut() {
                    rt.drive_preflight(&ctx);
                    if ctx.current_gtid.is_none() && ctx.last_gtid.is_some() {
                        // A CloseUncertain/FailedClosed outcome returns an error from
                        // this block (gate 6); `loop_result?` re-propagates it before the
                        // teardown checkpoint put, so the checkpoint never advances.
                        let (s, replaced) = rt
                            .apply_at_boundary(&mut ctx, stream, lost)
                            .await?;
                        stream = s;
                        lost &= !replaced;
                    }
                }

                // A stream given up for a reconnect is never read again: what
                // it still holds is past the position the reconnect resumes
                // from (an abandoned transaction's remainder). One attempt per
                // pass, after the backoff; rotation activity cuts the wait
                // short so new credentials apply (at the top of the loop)
                // instead of retrying ones the server may have revoked.
                if lost {
                    let delay = ctx.retry.next_backoff();
                    warn!(
                        source_id = %ctx.source_id,
                        delay_ms = delay.as_millis(),
                        "scheduling reconnect after backoff"
                    );
                    let woken = match rotation.as_mut() {
                        Some(rt) => tokio::select! {
                            _ = tokio::time::sleep(delay) => false,
                            _ = rt.wait_activity() => true,
                            _ = ctx.cancel.cancelled() => break,
                        },
                        None => tokio::select! {
                            _ = tokio::time::sleep(delay) => false,
                            _ = ctx.cancel.cancelled() => break,
                        },
                    };
                    if woken {
                        continue;
                    }
                    match reconnect_stream(&mut ctx).await {
                        Ok(s) => {
                            stream = s;
                            lost = false;
                            ctx.mark_transaction_boundary();
                        }
                        Err(SourceError::Connect { .. })
                        | Err(SourceError::Io(_))
                        | Err(SourceError::Timeout { .. }) => {}
                        Err(SourceError::Cancelled) => break,
                        Err(e) => return Err(e),
                    }
                    continue;
                }

                debug!(source_id=%ctx.source_id, "reading the next event ..");
                // Idle-source wakeup: race the read against rotation activity so a
                // rotation applies even when no binlog events are flowing.
                let control: Result<(), LoopControl> = match rotation.as_mut() {
                    Some(rt) => tokio::select! {
                        r = read_next_event(&mut stream, &ctx) => match r {
                            Ok((header, data)) => {
                                advance_position(&mut ctx, &header);
                                dispatch_event(&mut ctx, &header, data).await?;
                                Ok(())
                            }
                            Err(ctrl) => Err(ctrl),
                        },
                        _ = rt.wait_activity() => continue,
                    },
                    None => match read_next_event(&mut stream, &ctx).await {
                        Ok((header, data)) => {
                            advance_position(&mut ctx, &header);
                            dispatch_event(&mut ctx, &header, data).await?;
                            Ok(())
                        }
                        Err(ctrl) => Err(ctrl),
                    },
                };

                match control {
                    Ok(()) => {}
                    Err(LoopControl::ReloadSchema { db, table }) => {
                        apply_reload_request(&mut ctx, db, table).await?;
                        ctx.abandon_open_transaction().await?;
                        lost = true;
                    }
                    Err(LoopControl::Reconnect) => {
                        ctx.abandon_open_transaction().await?;
                        lost = true;
                    }
                    Err(LoopControl::Stop) => break,
                    Err(LoopControl::Fail(e)) => return Err(e),
                }
            }
            Ok(())
        }
        .await;

        // Cancel and join the rotation tasks on every exit path (normal or fatal).
        if let Some(rt) = rotation.take() {
            rt.shutdown().await;
        }
        loop_result?;

        // Best-effort final checkpoint update. Persist the read position as the
        // aggregate checkpoint ONLY when this store does not derive the resume position
        // from per-sink checkpoints. In production the coordinator writes per-sink
        // checkpoints (only after sink acknowledgement) and the resume position is their
        // minimum; writing the read position here would resume ahead of un-acknowledged
        // deliveries and lose them on restart (a clean stop during a sink outage). MySQL
        // has no consumer-driven binlog purge, so there is no server-side WAL feedback to
        // fix - only this resume checkpoint.
        if !ctx.chkpt.manages_per_sink_checkpoints() {
            let _ = ctx
                .chkpt
                .put(&ctx.source_id, {
                    // Never inside a transaction: a stop mid-transaction
                    // restarts before it.
                    let (file, pos, gtid_set) = ctx.resume_point();
                    MySqlCheckpoint {
                        lineage: checkpoint_lineage(&ctx.registry_scope),
                        file,
                        pos,
                        gtid_set,
                        snapshot_completed: None,
                        snapshot_chain: None,
                    }
                })
                .await;
        }

        Ok(())
    }
}

/// Order two MySQL checkpoints by binlog `(file, pos)`, failing closed.
///
/// `MySqlCheckpoint` is `{ file: String, pos: u64, gtid_set: Option<String> }`.
/// An unparseable checkpoint is [`CheckpointOrder::Incomparable`] rather than
/// silently treated as an orderable position (which could select a resume point
/// ahead of a sink and drop its events).
///
/// An incomplete-snapshot position equals only the same position of the same
/// generation and anchor, and orders before any stream position at or after
/// its anchor (the completing boundary's, or CDC after it). Against any other
/// position - another generation or anchor, or a stream position not after
/// the anchor - it is incomparable.
pub fn compare_mysql_checkpoints(a: &[u8], b: &[u8]) -> CheckpointOrder {
    crate::snapshot_position::order(&MyOrder, a, b)
}

/// MySQL's part of the snapshot position order.
#[derive(Clone, Copy)]
pub(crate) struct MyOrder;

impl crate::snapshot_position::EngineOrder for MyOrder {
    type Anchor = MySqlCheckpoint;

    fn stream_order(&self, a: &[u8], b: &[u8]) -> CheckpointOrder {
        compare_mysql_stream_checkpoints(a, b)
    }

    fn anchor_vs_stream(
        &self,
        anchor: &MySqlCheckpoint,
        stream: &[u8],
    ) -> CheckpointOrder {
        match serde_json::to_vec(anchor) {
            Ok(anchor) => compare_mysql_stream_checkpoints(&anchor, stream),
            Err(_) => CheckpointOrder::Incomparable,
        }
    }

    fn completion_mark(&self, stream: &[u8]) -> Option<(Option<String>, u64)> {
        let cp = serde_json::from_slice::<MySqlCheckpoint>(stream).ok()?;
        cp.snapshot_completed.map(|g| (cp.snapshot_chain, g))
    }

    fn stream_lineage(&self, stream: &[u8]) -> Option<String> {
        serde_json::from_slice::<MySqlCheckpoint>(stream)
            .ok()?
            .lineage
    }
}

/// [`compare_mysql_checkpoints`] of two stream positions.
fn compare_mysql_stream_checkpoints(a: &[u8], b: &[u8]) -> CheckpointOrder {
    // Positions of different servers are never ordered: two checkpoints that
    // both carry a lineage must carry the same one; a lineage-bearing and a
    // legacy (pre-lineage) checkpoint are Incomparable; two legacy checkpoints
    // keep the position-only comparison for upgrade compatibility. Positions
    // then go to the shared comparator: GTID sets by inclusion, binlog
    // coordinates by (base, numeric index, pos) - never lexically - and a GTID
    // checkpoint against a file/pos one is Incomparable.
    let parse = |raw: &[u8], which: &str| -> Option<MySqlCheckpoint> {
        match serde_json::from_slice(raw) {
            Ok(v) => Some(v),
            Err(e) => {
                tracing::warn!(error = %e, "incomparable checkpoint {which}: parse failed");
                None
            }
        }
    };
    let position = |cp: &MySqlCheckpoint, which: &str| {
        let p = crate::durable_checkpoint::mysql_checkpoint_position(
            &cp.file,
            cp.pos,
            cp.gtid_set.as_deref(),
        );
        if p.is_none() {
            tracing::warn!(file = %cp.file, "incomparable checkpoint {which}: unrecognised binlog file name");
        }
        p
    };
    let (Some(ca), Some(cb)) = (parse(a, "a"), parse(b, "b")) else {
        return CheckpointOrder::Incomparable;
    };
    // A lineage must be a canonical hash; two identical malformed strings
    // are not evidence of the same server.
    let malformed = |cp: &MySqlCheckpoint| {
        cp.lineage
            .as_deref()
            .is_some_and(|l| !mysql_checkpoint_lineage::is_canonical_lineage(l))
    };
    if malformed(&ca) || malformed(&cb) {
        tracing::warn!("incomparable checkpoints: malformed lineage");
        return CheckpointOrder::Incomparable;
    }
    if ca.lineage != cb.lineage {
        tracing::warn!(
            a = ?ca.lineage,
            b = ?cb.lineage,
            "incomparable checkpoints: different (or missing) server lineage"
        );
        return CheckpointOrder::Incomparable;
    }
    match (position(&ca, "a"), position(&cb, "b")) {
        (Some(a), Some(b)) => {
            crate::durable_checkpoint::order_positions(&a, &b)
        }
        _ => CheckpointOrder::Incomparable,
    }
}

#[async_trait]
impl Source for MySqlSource {
    async fn run(
        &self,
        tx: mpsc::Sender<SourceItem>,
        chkpt_store: Arc<dyn CheckpointStore>,
    ) -> SourceHandle {
        let cancel = CancellationToken::new();
        let paused = Arc::new(AtomicBool::new(false));
        let pause_notify = Arc::new(Notify::new());

        let this = self.clone();
        let cancel_for_task = cancel.clone();
        let paused_for_task = paused.clone();
        let pause_notify_for_task = pause_notify.clone();
        let ready = deltaforge_core::SourceReady::new();
        let ready_for_task = ready.clone();

        let join = tokio::spawn(async move {
            let res = this
                .run_inner(
                    tx,
                    chkpt_store,
                    cancel_for_task,
                    paused_for_task,
                    pause_notify_for_task,
                    ready_for_task,
                )
                .await;
            if let Err(e) = &res {
                error!(error=?e, "run task ended with error");
            }
            res
        });

        SourceHandle {
            cancel,
            paused,
            pause_notify,
            join,
            ready,
        }
    }

    fn compare_checkpoints(&self, a: &[u8], b: &[u8]) -> CheckpointOrder {
        compare_mysql_checkpoints(a, b)
    }

    fn checkpoint_generation_start(
        &self,
        prev: Option<&[u8]>,
        start: &deltaforge_core::GenerationStart,
    ) -> deltaforge_core::CheckpointStart {
        let lineage = checkpoint_lineage(&self.registry_scope);
        crate::snapshot_position::checkpoint_start(
            &MyOrder,
            prev,
            lineage.as_deref(),
            start,
        )
    }

    fn checkpoint_is_snapshot(&self, raw: &[u8]) -> bool {
        matches!(
            classify_mysql_checkpoint(raw),
            Ok(MyResumePosition::Snapshot)
        )
    }

    fn set_snapshot_cohort(&self, cohort: deltaforge_core::SnapshotCohort) {
        self.snapshot_cohort.lock().expect("not poisoned").cohort =
            Some(cohort);
    }

    fn resume_exclusions(&self) -> Vec<String> {
        self.snapshot_cohort
            .lock()
            .expect("not poisoned")
            .resume_exclusions
            .clone()
    }

    async fn check_durable_snapshot_startup(
        &self,
        checkpoint_store: &dyn CheckpointStore,
    ) -> Result<(), SourceError> {
        let progress: mysql_snapshot::MysqlSnapshotProgress = checkpoint_store
            .get_raw(&mysql_snapshot::progress_key(&self.id))
            .await
            .ok()
            .flatten()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default();
        if !progress.finished
            && !progress.done_tables.is_empty()
            && self.legacy_progress_is_ambiguous(&progress).await?
        {
            return Err(SourceError::Other(anyhow::anyhow!(
                "durable_v2: interrupted legacy snapshot progress for source \
                 {} cannot be adopted (some tables done, some pending); finish \
                 it under legacy mode or start a new snapshot generation",
                self.id
            )));
        }
        Ok(())
    }
}

// ============================================================================
// Stream helpers
// ============================================================================

async fn connect_first_stream(
    ctx: &RunCtx,
    client: BinlogClient,
) -> SourceResult<BinlogStream> {
    let init_gtid = client.gtid_enabled.then(|| client.gtid_set.clone());
    let init_file = if client.gtid_enabled {
        None
    } else {
        Some(client.binlog_filename.clone())
    };
    let init_pos = if client.gtid_enabled {
        None
    } else {
        Some(client.binlog_position)
    };

    let dsn = ctx.dsn.clone();
    let sid = ctx.server_id;
    let make_client = move || {
        let mut c = BinlogClient {
            url: dsn.expose().to_string(),
            server_id: sid,
            heartbeat_interval_secs: HEARTBEAT_INTERVAL_SECS,
            timeout_secs: READ_TIMEOUT,
            ..Default::default()
        };

        if let Some(gtid) = init_gtid.clone() {
            c.gtid_enabled = true;
            c.gtid_set = gtid
        } else if let (Some(fname), Some(pos)) = (init_file.clone(), init_pos) {
            c.gtid_enabled = false;
            c.binlog_filename = fname;
            c.binlog_position = pos;
        }
        c
    };

    let expected = ctx.expected_uuid()?;
    match connect_binlog_with_retries(
        &ctx.source_id,
        &expected,
        make_client,
        &ctx.cancel,
        &ctx.default_db,
        ctx.retry.clone(),
    )
    .await?
    {
        // Checked again once the stream is open, before any event is read:
        // the setting changes only with a restart, which ends this stream.
        Opened::Stream(stream) => {
            require_complete_binlog(ctx.dsn.expose(), &expected).await?;
            Ok(stream)
        }
        // Startup verified this server on every earlier connection: a
        // replication session elsewhere is split routing, never a failover.
        Opened::OtherServer(found) => Err(SourceError::Lineage {
            details: format!(
                "startup replication session reached server {found}, \
                 expected {expected}"
            )
            .into(),
        }),
    }
}

/// Reconnect using the best available resume position. Every replication
/// session is verified as the expected server before its dump command. A
/// session that reached another server U was closed before the dump: U is a
/// failover candidate, accepted only after reconciliation proves it on
/// connections verified as U and records it durably; a fresh session verified
/// as U is then opened. Callers see a ready stream regardless.
async fn reconnect_stream(ctx: &mut RunCtx) -> SourceResult<BinlogStream> {
    // Every reopen (same server, rotation recovery, failover candidate)
    // resumes from a proven boundary only.
    ctx.abandon_open_transaction().await?;
    let expected = ctx.expected_uuid()?;
    let (gtid_to_use, file_to_use, pos_to_use) = if let Some(g) = &ctx.last_gtid
    {
        (Some(g.clone()), None, None)
    } else if !ctx.last_file.is_empty() && ctx.last_pos > 0 {
        (None, Some(ctx.last_file.clone()), Some(ctx.last_pos as u32))
    } else {
        match resolve_binlog_tail(ctx.dsn.expose(), &expected).await {
            Ok((f, p)) => (None, Some(f), Some(p as u32)),
            Err(MySqlSourceError::Lineage(details)) => {
                return Err(SourceError::Lineage {
                    details: details.into(),
                });
            }
            Err(_) => {
                return Err(SourceError::Connect {
                    details: "could not resolve binlog tail during reconnect"
                        .into(),
                });
            }
        }
    };

    // The exact position the replacement stream starts from: what any
    // failover below must prove on the new server.
    let resume = match &gtid_to_use {
        Some(g) => ResumeAt::Gtid(g.clone()),
        None => ResumeAt::FilePos {
            file: file_to_use.clone().unwrap_or_default(),
            pos: pos_to_use.unwrap_or_default().into(),
        },
    };
    let dsn = ctx.dsn.clone();
    let sid = ctx.server_id;
    let make_client = move || {
        let mut c = BinlogClient {
            url: dsn.expose().to_string(),
            server_id: sid,
            heartbeat_interval_secs: HEARTBEAT_INTERVAL_SECS,
            timeout_secs: READ_TIMEOUT,
            ..Default::default()
        };

        if let Some(g) = gtid_to_use.clone() {
            c.gtid_enabled = true;
            c.gtid_set = g;
        } else if let (Some(f), Some(p)) = (file_to_use.clone(), pos_to_use) {
            c.gtid_enabled = false;
            c.binlog_filename = f;
            c.binlog_position = p;
        }
        c
    };

    let opened = connect_binlog_with_retries(
        &ctx.source_id,
        &expected,
        make_client.clone(),
        &ctx.cancel,
        &ctx.default_db,
        reconnect_attempt(&ctx.retry),
    )
    .await?;
    let found = match opened {
        Opened::Stream(stream) => {
            ctx.retry.reset();
            check_identity_post_reconnect(ctx, None, &resume).await?;
            return Ok(stream);
        }
        Opened::OtherServer(found) => found,
    };

    // Failover candidate. Nothing has been read from it and nothing durable
    // has changed; reconciliation proves it (on connections verified as
    // `found`) and only then records it.
    warn!(
        source_id = %ctx.source_id,
        %expected,
        candidate = %found,
        "reconnect reached another server; reconciling a possible failover"
    );
    // A candidate whose binlog may miss applied transactions is refused
    // before reconciliation records anything.
    require_complete_binlog(ctx.dsn.expose(), &found).await?;
    let previous = mysql_identity(&expected);
    let current = mysql_identity(&found);
    run_failover_reconciliation(ctx, previous, current, &resume).await?;

    // The candidate is now the verified lineage: the fresh session must prove
    // it is that server before its dump command.
    let expected = ctx.expected_uuid()?;
    match connect_binlog_with_retries(
        &ctx.source_id,
        &expected,
        make_client,
        &ctx.cancel,
        &ctx.default_db,
        reconnect_attempt(&ctx.retry),
    )
    .await?
    {
        Opened::Stream(stream) => {
            ctx.retry.reset();
            require_complete_binlog(ctx.dsn.expose(), &expected).await?;
            Ok(stream)
        }
        Opened::OtherServer(other) => Err(SourceError::Lineage {
            details: format!(
                "after failover to {expected}, the replication session \
                 reached server {other}"
            )
            .into(),
        }),
    }
}

/// One connect attempt per reconnect: the run loop owns the retry (with its
/// backoff), so between attempts a credential rotation can apply. Retrying
/// inside would keep presenting credentials the server may have revoked.
fn reconnect_attempt(retry: &RetryPolicy) -> RetryPolicy {
    let mut once = retry.clone();
    once.max_retries = Some(1);
    once
}

// ============================================================================
// Failover detection + reconciliation
// ============================================================================

/// Build a non-GTID MySQL lineage from `(server_id, binlog file)`, failing closed
/// on an unverified anchor. A zero `server_id` (server exposed neither a GTID
/// `server_uuid` nor a real `server_id`) or an empty binlog file (binary logging
/// off, or `SHOW BINARY LOG STATUS` returned nothing) must not be recorded: the
/// error is propagated by the caller (with `?`) at startup, so the source fails
/// closed before opening a stream rather than binding to a meaningless identity.
fn mysql_server_lineage(
    server_id: u32,
    file: String,
) -> SourceResult<PersistedLineage> {
    if server_id == 0 {
        return Err(SourceError::Other(anyhow::anyhow!(
            "cannot capture MySQL lineage: server exposes neither a server_uuid \
             (GTID) nor a nonzero server_id; refusing to record an unverified \
             lineage"
        )));
    }
    if file.is_empty() {
        return Err(SourceError::Other(anyhow::anyhow!(
            "cannot capture MySQL lineage: no current binlog file (is binary \
             logging enabled?); refusing to record an empty-file lineage"
        )));
    }
    Ok(PersistedLineage::MysqlServer { server_id, file })
}

impl RunCtx {
    /// The committed set plus the open transaction's GTID: the executed state
    /// through the current statement (the position a completed statement -
    /// a commit, an autocommit DDL - records). Not a resume position while a
    /// transaction is open.
    pub(crate) fn executed_through_current(&self) -> Option<String> {
        match (&self.last_gtid, &self.current_gtid) {
            (_, None) => self.last_gtid.clone(),
            (None, Some(g)) => Some(g.clone()),
            (Some(set), Some(g)) => Some(mysql_event::merge_gtid(set, g)),
        }
    }

    /// Whether a transaction has started and not reached a proven boundary.
    pub(crate) fn transaction_open(&self) -> bool {
        self.current_gtid.is_some() || self.in_explicit_txn
    }

    /// Where a resume may start: the last proven transaction boundary while
    /// a transaction is open (its events are read again), else the current
    /// position. A resume position contains only transactions proven
    /// complete.
    pub(crate) fn resume_point(&self) -> (String, u64, Option<String>) {
        match (&self.txn_eval_cp, self.transaction_open()) {
            (Some(cp), true) => (cp.file.clone(), cp.pos, cp.gtid_set.clone()),
            _ => (
                self.last_file.clone(),
                self.last_pos,
                self.last_gtid.clone(),
            ),
        }
    }

    /// Before a stream is reopened: a transaction cut short by the
    /// disconnect is abandoned - the coordinator drops its buffered prefix
    /// (`TxAbort`), the decoder state is discarded and the cursor returns to
    /// the last proven boundary, so the server sends the whole transaction
    /// again. Duplicates of its rows are possible; skipping any is not.
    pub(crate) async fn abandon_open_transaction(
        &mut self,
    ) -> SourceResult<()> {
        if !self.transaction_open() {
            return Ok(());
        }
        let (file, pos, gtid) = self.resume_point();
        warn!(
            source_id = %self.source_id,
            open = ?self.current_gtid,
            resume_file = %file, resume_pos = pos, resume_gtid = ?gtid,
            "stream ended inside a transaction: resuming before it"
        );
        metrics::counter!(
            "deltaforge_source_abandoned_transactions_total",
            "pipeline" => self.pipeline.clone(),
            "source" => self.source_id.clone(),
        )
        .increment(1);
        if let Some(tx_id) = self.current_gtid.take() {
            self.tx
                .send(SourceItem::TxAbort { tx_id })
                .await
                .map_err(|e| SourceError::Other(e.into()))?;
        }
        self.in_explicit_txn = false;
        self.message_ordinal = 0;
        self.query_ordinal = 0;
        self.rows_ordinal = 0;
        self.last_file = file;
        self.last_pos = pos;
        self.last_gtid = gtid;
        Ok(())
    }

    /// The stream is at a transaction boundary: rows of the next transaction
    /// are evaluated at the current position.
    pub(crate) fn mark_transaction_boundary(&mut self) {
        self.rows_ordinal = 0;
        self.txn_eval = crate::durable_checkpoint::mysql_checkpoint_position(
            &self.last_file,
            self.last_pos,
            self.last_gtid.as_deref(),
        );
        self.txn_eval_cp = Some(MySqlCheckpoint {
            file: self.last_file.clone(),
            pos: self.last_pos,
            gtid_set: self.last_gtid.clone(),
            lineage: None,
            snapshot_completed: None,
            snapshot_chain: None,
        });
    }

    /// The verified `server_uuid` (the published registry lineage) that every
    /// connection of this run must prove it is before it is trusted.
    pub(crate) fn expected_uuid(&self) -> SourceResult<String> {
        match &self.registry_scope.current()?.lineage().descriptor {
            LineageDescriptor::Mysql { server_uuid } => Ok(server_uuid.clone()),
            other => Err(SourceError::Lineage {
                details: format!(
                    "MySQL source scoped to a non-MySQL lineage {other:?}"
                )
                .into(),
            }),
        }
    }
}

/// Refuse a server whose binlog does not hold every transaction it applies
/// (`mysql_health::require_complete_binlog`).
pub(crate) async fn require_complete_binlog(
    dsn: &str,
    expected_uuid: &str,
) -> SourceResult<()> {
    let mut conn =
        open_control_connection(dsn, expected_uuid, CONTROL_CONNECT_TIMEOUT)
            .await
            .map_err(|e| e.into_source_error(expected_uuid))?;
    let r = mysql_health::require_complete_binlog(&mut conn).await;
    conn.disconnect().await.ok();
    r.map_err(|details| SourceError::Incompatible {
        details: details.into(),
    })
}

/// `@@lower_case_table_names` of the verified server: how DDL table names map
/// to registry keys (0: as written; 1: stored lower case; 2: compared case
/// insensitively but stored as written).
async fn fetch_lower_case_table_names(
    dsn: &str,
    expected_uuid: &str,
) -> SourceResult<u8> {
    use mysql_async::prelude::Queryable;
    let mut conn =
        open_control_connection(dsn, expected_uuid, CONTROL_CONNECT_TIMEOUT)
            .await
            .map_err(|e| e.into_source_error(expected_uuid))?;
    let value: Result<Option<u8>, _> =
        conn.query_first("SELECT @@lower_case_table_names").await;
    conn.disconnect().await.ok();
    match value {
        Ok(Some(v @ 0..=2)) => Ok(v),
        Ok(other) => Err(SourceError::Incompatible {
            details: format!("unexpected lower_case_table_names {other:?}")
                .into(),
        }),
        Err(e) => Err(SourceError::Connect {
            details: format!("read lower_case_table_names: {e}").into(),
        }),
    }
}

/// A schema reload requested by the stream. Never enumerates: a named table
/// is reloaded, any other request forgets every cached schema; the
/// activation timelines and selections cached for them go with them.
pub(crate) async fn apply_reload_request(
    ctx: &mut RunCtx,
    db: Option<String>,
    table: Option<String>,
) -> SourceResult<()> {
    if let (Some(d), Some(t)) = (db, table) {
        let _ = ctx.schema.reload_schema(&d, &t).await?;
        let key = ctx.registry_scope.current()?.key(&d, &t);
        ctx.selection.invalidate(&key);
    } else {
        ctx.schema.clear_cache().await;
        ctx.selection.invalidate_all();
    }
    Ok(())
}

/// The lineage-wide barrier of a stream discontinuity at the position the
/// stream starts from (its identity is that position, so a repeated start at
/// the same position re-derives it).
async fn record_stream_start(ctx: &RunCtx) -> SourceResult<()> {
    use mysql_activation::{BarrierScope, EventIdentity, record_barrier};
    let position = crate::durable_checkpoint::mysql_checkpoint_position(
        &ctx.last_file,
        ctx.last_pos,
        ctx.last_gtid.as_deref(),
    )
    .ok_or_else(|| SourceError::Checkpoint {
        details: format!(
            "unparseable start position {}:{} {:?}",
            ctx.last_file, ctx.last_pos, ctx.last_gtid
        )
        .into(),
    })?;
    let key = ctx.registry_scope.current()?.key("", "");
    record_barrier(
        &ctx.registry_backend,
        ctx.schema.registry(),
        &key,
        BarrierScope::Lineage,
        &EventIdentity::StreamStart {
            position: position.clone(),
        },
        position,
    )
    .await
    .map_err(|e| {
        SourceError::Other(e.context("persist the stream-start barrier"))
    })?;
    info!(source_id = %ctx.source_id, "stream discontinuity: activation barrier at the start position");
    Ok(())
}

/// Advance the file/position cursor to the end of `header`'s event. The
/// server's artificial events at the start of a dump (the format description
/// after the fake rotate) carry no position (`0`): they never move it.
fn advance_position(
    ctx: &mut RunCtx,
    header: &mysql_binlog_connector_rust::event::event_header::EventHeader,
) {
    if header.next_event_position != 0 {
        ctx.last_pos = u64::from(header.next_event_position);
    }
}

/// Connect timeout for identity-verified control connections.
const CONTROL_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

fn mysql_identity(server_uuid: &str) -> ServerIdentity {
    ServerIdentity::MySql(mysql_health::MySqlServerIdentity {
        server_uuid: server_uuid.to_string(),
    })
}

/// The live identity, proven on a control connection to be `expected_uuid`
/// (the verified registry lineage). Transient connect failures are retried
/// (bounded); another server or an unverifiable identity is a lineage error.
async fn fetch_identity_verified_as(
    dsn: &str,
    expected_uuid: &str,
) -> SourceResult<ServerIdentity> {
    let mut attempt = 0u32;
    loop {
        match open_control_connection(
            dsn,
            expected_uuid,
            CONTROL_CONNECT_TIMEOUT,
        )
        .await
        {
            Ok(conn) => {
                conn.disconnect().await.ok();
                return Ok(mysql_identity(expected_uuid));
            }
            Err(SessionError::Connect(e)) => {
                attempt += 1;
                if attempt >= IDENTITY_FETCH_ATTEMPTS {
                    return Err(SourceError::Connect {
                        details: format!(
                            "failed to verify server identity after {attempt} \
                             attempts: {e}; refusing to stream on an unverified \
                             server"
                        )
                        .into(),
                    });
                }
                warn!(
                    attempt, error = %e,
                    "identity verification connect failed; retrying"
                );
                tokio::time::sleep(Duration::from_millis(
                    200 * 2u64.pow(attempt.min(5)),
                ))
                .await;
            }
            Err(e) => return Err(e.into_source_error(expected_uuid)),
        }
    }
}

/// Bounded attempts to fetch the live server identity before failing closed.
const IDENTITY_FETCH_ATTEMPTS: u32 = 5;

/// Fetch the live MySQL server identity with bounded retries, failing closed if
/// it cannot be obtained or is absent. Identity is a correctness authority: the
/// source must never continue against an unverified server, so a persistent
/// fetch failure (or a server that exposes no identity) stops the source rather
/// than being silently skipped.
async fn fetch_identity_verified(dsn: &str) -> SourceResult<ServerIdentity> {
    let mut attempt = 0u32;
    loop {
        match fetch_server_identity(dsn).await {
            Ok(Some(id)) => return Ok(ServerIdentity::from(id)),
            Ok(None) => {
                return Err(SourceError::Other(anyhow::anyhow!(
                    "server identity unavailable (no server_uuid/server_id); \
                     cannot verify server lineage - refusing to stream"
                )));
            }
            Err(e) => {
                attempt += 1;
                if attempt >= IDENTITY_FETCH_ATTEMPTS {
                    return Err(SourceError::Other(anyhow::anyhow!(
                        "failed to fetch server identity after {attempt} \
                         attempts: {e}; refusing to stream on unverified identity"
                    )));
                }
                warn!(
                    source_id = "?", attempt, error = %e,
                    "identity fetch failed; retrying before failing closed"
                );
                tokio::time::sleep(Duration::from_millis(
                    200 * 2u64.pow(attempt.min(5)),
                ))
                .await;
            }
        }
    }
}

/// The schema-registry lineage of a verified MySQL identity: its `server_uuid`,
/// in every binlog mode (GTID is not required to read `@@server_uuid`). An
/// empty or all-zero UUID fails closed - `server_id` is never a substitute,
/// since it is operator-assigned and reused after replacement.
fn mysql_registry_lineage(
    live: &ServerIdentity,
) -> SourceResult<LineageDescriptor> {
    match live {
        ServerIdentity::MySql(id) => LineageDescriptor::mysql(&id.server_uuid)
            .map_err(|e| SourceError::Incompatible {
                details: format!("{e:#}").into(),
            }),
        other => Err(SourceError::Other(anyhow::anyhow!(
            "expected a MySQL server identity, got {other:?}"
        ))),
    }
}

/// Verify the live MySQL `server_uuid` lineage and establish it as the registry
/// scope of `(tenant, source_id)`: durably recorded (fail closed), then
/// published into `shared`. This is the startup step every MySQL source runs
/// before any schema-registry access, in every binlog mode.
pub async fn establish_registry_scope(
    dsn: &str,
    backend: &ArcStorageBackend,
    shared: &SharedRegistryScope,
    tenant: &str,
    source_id: &str,
) -> SourceResult<ScopeChange> {
    let live = fetch_identity_verified(dsn).await?;
    Ok(establish_scope(
        backend,
        shared,
        tenant,
        source_id,
        mysql_registry_lineage(&live)?,
    )
    .await?)
}

/// Bring the published registry scope in line with a lineage just verified
/// against the live server, before any further registry access. A changed
/// `server_uuid` is a failover to another server: the new lineage is durably
/// recorded (fail closed) and published, giving it a fresh schema namespace and
/// invalidating caches from the old lineage.
async fn sync_registry_lineage(
    ctx: &RunCtx,
    live: &ServerIdentity,
) -> SourceResult<()> {
    let descriptor = mysql_registry_lineage(live)?;
    if ctx.registry_scope.current()?.lineage().descriptor == descriptor {
        return Ok(());
    }
    let change = establish_scope(
        &ctx.registry_backend,
        &ctx.registry_scope,
        &ctx.tenant,
        &ctx.source_id,
        descriptor,
    )
    .await?;
    warn!(
        source_id = %ctx.source_id,
        lineage = %change.scope.lineage().lineage_hash,
        "source lineage changed; schema registry re-scoped to the new server"
    );
    Ok(())
}

/// How long a plausibly transient failure to verify the resume position is
/// retried before the source stops.
const REACHABILITY_RETRY_WINDOW: Duration = Duration::from_secs(120);

/// Verify, on a connection proven to be `expected`, that the server has
/// executed the checkpoint's GTID set before reading on. Fails closed: a
/// confirmed loss or an error answer stops at once; an unknown answer is
/// retried with backoff while it is plausibly transient. While it retries,
/// the condition is an open auto-retry `mysql_gtid_position_unavailable`
/// incident; when the window ends the source stops on the same incident,
/// which then needs an operator. Every stop is operator action; cancellation
/// stops without one.
async fn verify_gtid_position(
    ctx: &RunCtx,
    expected: &str,
    window: Duration,
) -> SourceResult<()> {
    let deadline = std::time::Instant::now() + window;
    let mut delay = Duration::from_secs(1);
    let gtid = ctx.checkpoint_gtid.as_deref();
    let mut retrying = None;
    loop {
        let reach = match open_control_connection(
            ctx.dsn.expose(),
            expected,
            CONTROL_CONNECT_TIMEOUT,
        )
        .await
        {
            Ok(mut conn) => {
                let r = check_position_reachability_on(
                    &mut conn,
                    &ctx.checkpoint_file,
                    gtid,
                )
                .await;
                conn.disconnect().await.ok();
                r.unwrap_or_else(|e| PositionReachability::Unknown {
                    transient: false,
                    reason: format!("{e:#}"),
                })
            }
            Err(SessionError::Connect(e)) => PositionReachability::Unknown {
                transient: true,
                reason: e,
            },
            Err(e) => return Err(e.into_source_error(expected)),
        };
        match reach {
            PositionReachability::Reachable => return Ok(()),
            PositionReachability::Lost { class, reason } => {
                error!(
                    source_id = %ctx.source_id, %reason,
                    "checkpoint position not available on this server; halting"
                );
                return Err(gtid_position_unavailable(
                    &ctx.source_id,
                    Some(expected),
                    gtid,
                    class,
                    SourceError::Checkpoint {
                        details: format!(
                            "checkpoint position not available on this \
                             server (binlog purge?): {reason}. Re-snapshot \
                             required."
                        )
                        .into(),
                    },
                ));
            }
            PositionReachability::Unknown { transient, reason } => {
                if transient && std::time::Instant::now() + delay < deadline {
                    if retrying.is_none() {
                        retrying = crate::incident_drafts::record_retrying(
                            &ctx.incidents,
                            &gtid_position_unavailable_draft(
                                &ctx.source_id,
                                Some(expected),
                                gtid,
                                "unknown_unreachable",
                                Retryability::AutoRetry,
                                CauseCode::SourceConnect,
                            ),
                        )
                        .await;
                    }
                    warn!(
                        source_id = %ctx.source_id, %reason,
                        retry_in_secs = delay.as_secs(),
                        "cannot verify the resume position yet; retrying \
                         (reading stays stopped)"
                    );
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {}
                        _ = ctx.cancel.cancelled() => {
                            crate::incident_drafts::cancel_retrying(
                                &ctx.incidents,
                                retrying,
                            )
                            .await;
                            return Err(SourceError::Cancelled);
                        }
                    }
                    delay = (delay * 2).min(Duration::from_secs(15));
                    continue;
                }
                let class = if transient {
                    "unknown_unreachable"
                } else {
                    "unknown_query_failed"
                };
                let details = format!(
                    "cannot verify that the server holds the resume position: \
                     {reason}"
                );
                let cause = if transient {
                    SourceError::Connect {
                        details: details.into(),
                    }
                } else {
                    SourceError::Checkpoint {
                        details: details.into(),
                    }
                };
                return Err(gtid_position_unavailable(
                    &ctx.source_id,
                    Some(expected),
                    gtid,
                    class,
                    cause,
                ));
            }
        }
    }
}

/// The `mysql_gtid_position_unavailable` incident around `cause`: the source
/// stops, so it needs an operator.
pub(crate) fn gtid_position_unavailable(
    source_id: &str,
    server_uuid: Option<&str>,
    gtid_set: Option<&str>,
    class: &str,
    cause: SourceError,
) -> SourceError {
    let draft = gtid_position_unavailable_draft(
        source_id,
        server_uuid,
        gtid_set,
        class,
        Retryability::OperatorAction,
        cause.cause_code(),
    );
    SourceError::incident(draft, cause)
}

/// The `mysql_gtid_position_unavailable` draft. The GTID set is exposed only
/// as a digest with its interval count. Its identity is the server, GTID set
/// and class, never the retryability, so an automatic retry that exhausts
/// its window stays one incident.
fn gtid_position_unavailable_draft(
    source_id: &str,
    server_uuid: Option<&str>,
    gtid_set: Option<&str>,
    class: &str,
    retryability: Retryability,
    cause_code: CauseCode,
) -> IncidentDraft {
    use deltaforge_core::incident::{
        ActionCode, Component, EvidenceKey as K, ReasonCode, SafetyState,
    };
    let unknown = class.starts_with("unknown_");
    let actions: &[ActionCode] = if unknown {
        &[ActionCode::VerifyEndpoint, ActionCode::InspectLogs]
    } else {
        &[
            ActionCode::Resnapshot,
            ActionCode::RestoreMissingTransactions,
        ]
    };
    let set = gtid_set.unwrap_or("");
    let intervals =
        set.split(',').filter(|p| !p.trim().is_empty()).count() as u64;
    IncidentDraft::new(
        ReasonCode::MysqlGtidPositionUnavailable,
        Component::Source {
            id: source_id.to_string(),
        },
        retryability,
        SafetyState::HaltedSafe,
        cause_code,
    )
    .discriminate("server_uuid", server_uuid.unwrap_or("-"))
    .discriminate("gtid_set", set)
    .discriminate("class", class)
    .with_evidence(|e| {
        e.text(K::SourceId, source_id).text(K::ReasonClass, class);
        if let Some(uuid) = server_uuid {
            e.text(K::ServerUuid, uuid);
        }
        if gtid_set.is_some() {
            e.digest(K::GtidSet, set.as_bytes(), intervals);
        }
    })
    .with_actions(actions)
}

/// Compare the live server identity against the stored one.
///
/// - `FirstSeen`: store and continue (clean start or wiped state).
/// - `Same`: normal reconnect, nothing to do.
/// - `Changed`: run full failover reconciliation before returning.
///
/// Identity fetch, comparison, and persistence are correctness authority: any
/// failure fails closed (propagated), never mapped to `Same`. The live
/// identity is the run's verified lineage, proven on a control connection.
///
/// `prefetched` reuses an identity already verified by the caller (startup path)
/// so the live server is queried once; `None` fetches and verifies here (the
/// reconnect path).
async fn check_identity_post_reconnect(
    ctx: &mut RunCtx,
    prefetched: Option<ServerIdentity>,
    resume: &ResumeAt,
) -> SourceResult<()> {
    let expected = ctx.expected_uuid()?;
    let live = match prefetched {
        Some(live) => live,
        None => {
            // A reconnect's stream is open, nothing read yet: the server
            // may have restarted with another configuration.
            require_complete_binlog(ctx.dsn.expose(), &expected).await?;
            fetch_identity_verified_as(ctx.dsn.expose(), &expected).await?
        }
    };

    match ctx
        .identity_store
        .compare(&ctx.source_id, &live)
        .await
        .map_err(SourceError::Other)?
    {
        IdentityComparison::FirstSeen => {
            ctx.identity_store
                .store(&ctx.source_id, &live)
                .await
                .map_err(SourceError::Other)?;
        }
        IdentityComparison::Same => {
            // A RESET BINARY LOGS AND GTIDS wipes the GTID history without
            // changing the server UUID.  Run the position check here too so
            // we catch that case on the first reconnect after a purge.
            // Skip when there is no GTID checkpoint (file/pos mode or fresh
            // start) to avoid false positives from the file-presence fallback.
            if ctx.checkpoint_gtid.is_some() {
                verify_gtid_position(ctx, &expected, REACHABILITY_RETRY_WINDOW)
                    .await?;
            }
        }
        IdentityComparison::Changed { previous, current } => {
            warn!(
                source_id = %ctx.source_id,
                prev = ?previous,
                new = ?current,
                "server identity changed - failover detected, reconciling"
            );
            run_failover_reconciliation(ctx, previous, current, resume).await?;
        }
    }

    Ok(())
}

/// The exact position a stream (re)opens from, which a failover must prove on
/// the new server.
#[derive(Debug, Clone)]
pub(crate) enum ResumeAt {
    Gtid(String),
    FilePos { file: String, pos: u64 },
}

impl ResumeAt {
    fn of(client: &BinlogClient) -> Self {
        if client.gtid_enabled {
            Self::Gtid(client.gtid_set.clone())
        } else {
            Self::FilePos {
                file: client.binlog_filename.clone(),
                pos: client.binlog_position.into(),
            }
        }
    }
}

/// Reconcile a failover from `previous` to `current` and only then make
/// `current` the verified lineage.
///
/// Cross-server failover is supported only in GTID mode: the exact GTID set
/// the stream resumes from must be proven executed by `current`
/// (`GTID_SUBSET` exactly 1); a file/position resume is never comparable
/// across servers and stops. Every live-server fact is read on ONE control
/// connection that first proves it is `current`.
///
/// The reconciliation record (loaded when a previous attempt persisted it,
/// else created) is the single source of the drift decision: drift is
/// derived from it and `on_schema_drift=halt` applied on every attempt.
/// Order: proof, record, halt, durable lineage edge (the expected server of
/// every later connection), schema reloads (failures stop: no stream opens),
/// and only then the identity record, so a restart after any failure
/// re-enters this path from the stored record. A crash before the lineage
/// edge keeps the previous lineage; after it, startup carries eligible GTID
/// checkpoints over.
async fn run_failover_reconciliation(
    ctx: &mut RunCtx,
    previous: ServerIdentity,
    current: ServerIdentity,
    resume: &ResumeAt,
) -> SourceResult<()> {
    let ServerIdentity::MySql(current_id) = &current else {
        return Err(SourceError::Lineage {
            details: format!("failover to a non-MySQL identity {current:?}")
                .into(),
        });
    };
    let current_uuid = current_id.server_uuid.clone();
    let resume_set = match resume {
        ResumeAt::Gtid(set) => set,
        ResumeAt::FilePos { file, pos } => {
            return Err(SourceError::Lineage {
                details: format!(
                    "failover from {previous:?} to {current_uuid} at \
                     {file}:{pos}: binlog positions are not comparable across \
                     servers; cross-server failover requires GTID mode"
                )
                .into(),
            });
        }
    };

    let mut conn = open_control_connection(
        ctx.dsn.expose(),
        &current_uuid,
        CONTROL_CONNECT_TIMEOUT,
    )
    .await
    .map_err(|e| e.into_source_error(&current_uuid))?;
    let record =
        failover_record(ctx, &mut conn, &previous, &current, resume_set).await;
    conn.disconnect().await.ok();
    let record = record?;

    // Schema drift is not decided here: each table is checked lazily at its
    // first event under the new lineage (`mysql_failover_drift`), wildcard
    // tables included, against the shape proven at the failover position.
    let _ = record;

    // Reconciled: record the lineage edge (fail closed) and publish it - from
    // here on every connection must prove it is `current`.
    sync_registry_lineage(ctx, &current).await?;

    // The failover position F for every table's lazy drift check, durable
    // before the identity record (an earlier anchor for this lineage transition,
    // e.g. recorded at startup before a snapshot replaced the checkpoint,
    // is kept). Tables are re-checked under the new lineage.
    let previous_lineage = mysql_registry_lineage(&previous)?.lineage_hash();
    let current_lineage =
        ctx.registry_scope.current()?.lineage().lineage_hash.clone();
    mysql_failover_drift::record_anchor(
        &ctx.registry_backend,
        &ctx.tenant,
        &ctx.source_id,
        &previous_lineage,
        Some(MySqlCheckpoint {
            file: String::new(),
            pos: 0,
            gtid_set: Some(resume_set.clone()),
            lineage: Some(current_lineage),
            snapshot_completed: None,
            snapshot_chain: None,
        }),
    )
    .await
    .map_err(SourceError::Other)?;
    ctx.failover = None;
    ctx.drift_checked.clear();

    // Persist new identity only after everything above completed. Fail closed
    // if the durable write does not commit.
    ctx.identity_store
        .store(&ctx.source_id, &current)
        .await
        .map_err(SourceError::Other)?;

    // The stream resumes from the proven set: keep it (a later reconnect
    // must not lose the previous server's part of it). File/position
    // coordinates of the previous server mean nothing here.
    ctx.last_gtid = Some(resume_set.clone());
    ctx.current_gtid = None;
    ctx.message_ordinal = 0;
    ctx.query_ordinal = 0;
    ctx.rows_ordinal = 0;
    ctx.last_file = String::new();
    ctx.last_pos = 0;

    info!(source_id = %ctx.source_id, "failover reconciliation complete");
    Ok(())
}

/// Prove the exact resume set on `conn` (verified as the failover candidate
/// by the caller), then return the reconciliation record: the one a previous
/// attempt persisted, else a new one built from the live columns of every
/// configured table read on `conn`.
async fn failover_record(
    ctx: &RunCtx,
    conn: &mut mysql_async::Conn,
    previous: &ServerIdentity,
    current: &ServerIdentity,
    resume_set: &str,
) -> SourceResult<crate::failover::reconciler::ReconciliationRecord> {
    mysql_health::require_gtid_executed(conn, resume_set)
        .await
        .map_err(|why| {
            let uuid = match current {
                ServerIdentity::MySql(id) => Some(id.server_uuid.as_str()),
                _ => None,
            };
            gtid_position_unavailable(
                &ctx.source_id,
                uuid,
                Some(resume_set),
                why.class,
                SourceError::Checkpoint {
                    details: format!(
                        "failover to {current:?}: the resume position \
                         {resume_set:?} is not proven on the new server \
                         ({why}). Re-snapshot required."
                    )
                    .into(),
                },
            )
        })?;

    if let Some(record) = ctx
        .reconciler
        .already_completed(&ctx.source_id, previous, current)
        .await
        .map_err(SourceError::Other)?
    {
        return Ok(record);
    }

    // No eager per-table schema diff (it skipped wildcard tables): drift
    // is checked lazily per table (`mysql_failover_drift`).
    let inputs: Vec<ReconcileInput> = Vec::new();

    // Diff against the last-known schemas of the lineage the source ran
    // under before this failover: the published scope while it still is
    // that lineage (reconnect), else its recorded predecessor (startup,
    // where the scope was established on the new server already).
    let scope_now = ctx.registry_scope.current()?;
    let prior_recorded;
    let prior = if scope_now.lineage().descriptor
        == mysql_registry_lineage(current)?
    {
        prior_recorded =
            previous_scope(&ctx.registry_backend, &ctx.tenant, &ctx.source_id)
                .await?;
        prior_recorded.as_ref()
    } else {
        Some(scope_now.as_ref())
    };
    ctx.reconciler
        .run(&ctx.source_id, previous, current, prior, &inputs)
        .await
        .map_err(SourceError::Other)
}

#[cfg(test)]
mod server_lineage_tests {
    //! R3-C8: the non-GTID fallback lineage fails closed on a zero server_id or
    //! an empty binlog file, rather than persisting an unverified anchor.
    use super::{PersistedLineage, mysql_server_lineage};

    #[test]
    fn rejects_zero_server_id() {
        assert!(
            mysql_server_lineage(0, "binlog.000001".into()).is_err(),
            "a zero server_id is an unverified lineage"
        );
    }

    #[test]
    fn rejects_empty_binlog_file() {
        assert!(
            mysql_server_lineage(5, String::new()).is_err(),
            "an empty binlog file must not be recorded as lineage"
        );
    }

    #[test]
    fn accepts_valid_server_and_file() {
        match mysql_server_lineage(5, "binlog.000007".into()) {
            Ok(PersistedLineage::MysqlServer { server_id, file }) => {
                assert_eq!(server_id, 5);
                assert_eq!(file, "binlog.000007");
            }
            other => panic!("expected a MysqlServer lineage, got {other:?}"),
        }
    }
}

#[cfg(test)]
mod compare_checkpoints_tests {
    use super::compare_mysql_checkpoints;
    use deltaforge_core::CheckpointOrder;

    fn cp(file: &str, pos: u64) -> Vec<u8> {
        format!(r#"{{"file":"{file}","pos":{pos},"gtid_set":null}}"#)
            .into_bytes()
    }

    // Regression: the (file, pos) ordering must be preserved exactly through the
    // CheckpointOrder conversion.
    #[test]
    fn orders_by_pos_within_same_file() {
        assert_eq!(
            compare_mysql_checkpoints(
                &cp("bin.000001", 100),
                &cp("bin.000001", 200)
            ),
            CheckpointOrder::Before
        );
        assert_eq!(
            compare_mysql_checkpoints(
                &cp("bin.000001", 200),
                &cp("bin.000001", 100)
            ),
            CheckpointOrder::After
        );
        assert_eq!(
            compare_mysql_checkpoints(
                &cp("bin.000001", 100),
                &cp("bin.000001", 100)
            ),
            CheckpointOrder::Equal
        );
    }

    #[test]
    fn file_dominates_pos() {
        // A later file with a smaller pos is still After.
        assert_eq!(
            compare_mysql_checkpoints(
                &cp("bin.000002", 1),
                &cp("bin.000001", 999)
            ),
            CheckpointOrder::After
        );
    }

    #[test]
    fn malformed_is_incomparable_never_equal() {
        for bad in [&b"not json"[..], &b"{}"[..], br#"{"file":"x"}"#] {
            assert_eq!(
                compare_mysql_checkpoints(bad, &cp("bin.000001", 1)),
                CheckpointOrder::Incomparable
            );
            assert_eq!(
                compare_mysql_checkpoints(&cp("bin.000001", 1), bad),
                CheckpointOrder::Incomparable
            );
            assert_ne!(
                compare_mysql_checkpoints(bad, &cp("bin.000001", 1)),
                CheckpointOrder::Equal
            );
        }
    }

    fn gcp(file: &str, pos: u64, gtid: &str) -> Vec<u8> {
        format!(r#"{{"file":"{file}","pos":{pos},"gtid_set":"{gtid}"}}"#)
            .into_bytes()
    }

    /// Lexical file order is wrong once the index outgrows its zero padding;
    /// the numeric index decides.
    #[test]
    fn binlog_index_is_numeric_not_lexical() {
        assert_eq!(
            compare_mysql_checkpoints(
                &cp("bin.999999", 500),
                &cp("bin.1000000", 4)
            ),
            CheckpointOrder::Before
        );
        assert_eq!(
            compare_mysql_checkpoints(&cp("bin.000010", 4), &cp("bin.9", 900)),
            CheckpointOrder::After
        );
    }

    #[test]
    fn different_binlog_bases_are_incomparable() {
        assert_eq!(
            compare_mysql_checkpoints(
                &cp("bin.000001", 1),
                &cp("other.000001", 1)
            ),
            CheckpointOrder::Incomparable
        );
    }

    /// GTID checkpoints are ordered by set inclusion; the file/pos they also
    /// carry does not override it.
    #[test]
    fn gtid_sets_order_by_inclusion() {
        let u = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
        let small = gcp("bin.000009", 900, &format!("{u}:1-5"));
        let large = gcp("bin.000001", 4, &format!("{u}:1-9"));
        assert_eq!(
            compare_mysql_checkpoints(&small, &large),
            CheckpointOrder::Before
        );
        assert_eq!(
            compare_mysql_checkpoints(&large, &small),
            CheckpointOrder::After
        );
        assert_eq!(
            compare_mysql_checkpoints(&small, &small),
            CheckpointOrder::Equal
        );
        // Multi-line Executed_Gtid_Set formatting is accepted.
        let v = "4f2a0b1c-71ca-11e1-9e33-c80aa9429562";
        let a = gcp("bin.000001", 4, &format!("{u}:1-5,\\n{v}:1-2"));
        let b = gcp("bin.000001", 4, &format!("{u}:1-6,{v}:1-2"));
        assert_eq!(compare_mysql_checkpoints(&a, &b), CheckpointOrder::Before);
    }

    #[test]
    fn disjoint_gtid_sets_are_incomparable() {
        let u = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
        let v = "4f2a0b1c-71ca-11e1-9e33-c80aa9429562";
        assert_eq!(
            compare_mysql_checkpoints(
                &gcp("bin.000001", 1, &format!("{u}:1-5")),
                &gcp("bin.000001", 2, &format!("{v}:1-5"))
            ),
            CheckpointOrder::Incomparable
        );
    }

    #[test]
    fn gtid_against_file_pos_is_incomparable() {
        let u = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
        assert_eq!(
            compare_mysql_checkpoints(
                &gcp("bin.000001", 1, &format!("{u}:1-5")),
                &cp("bin.000002", 1)
            ),
            CheckpointOrder::Incomparable
        );
    }

    #[test]
    fn malformed_gtid_or_file_is_incomparable() {
        assert_eq!(
            compare_mysql_checkpoints(
                &gcp("bin.000001", 1, "not-a-gtid-set"),
                &gcp("bin.000001", 1, "not-a-gtid-set")
            ),
            CheckpointOrder::Incomparable
        );
        assert_eq!(
            compare_mysql_checkpoints(&cp("binlog", 1), &cp("binlog", 1)),
            CheckpointOrder::Incomparable
        );
    }

    const LA: &str = "0123456789abcdef0123456789abcdef";
    const LB: &str = "fedcba9876543210fedcba9876543210";

    /// Identical but malformed lineage strings never make two checkpoints
    /// comparable.
    #[test]
    fn malformed_lineages_are_incomparable_even_when_identical() {
        for bad in ["lineage-a", "0123456789ABCDEF0123456789ABCDEF", ""] {
            let a = lcp("binlog.000010", 400, None, bad);
            assert_eq!(
                compare_mysql_checkpoints(&a, &a),
                CheckpointOrder::Incomparable,
                "{bad:?}"
            );
        }
    }

    fn lcp(file: &str, pos: u64, gtid: Option<&str>, lineage: &str) -> Vec<u8> {
        serde_json::to_vec(&super::MySqlCheckpoint {
            file: file.into(),
            pos,
            gtid_set: gtid.map(str::to_string),
            lineage: Some(lineage.into()),
            snapshot_completed: None,
            snapshot_chain: None,
        })
        .unwrap()
    }

    /// The same coordinates on two different servers are never ordered - in
    /// file/pos mode (identical binlog names are common) and in GTID mode.
    #[test]
    fn same_coordinates_on_different_lineages_are_incomparable() {
        let u = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
        let g = format!("{u}:1-5");
        for gtid in [None, Some(g.as_str())] {
            let a = lcp("binlog.000010", 400, gtid, LA);
            let b = lcp("binlog.000010", 400, gtid, LB);
            assert_eq!(
                compare_mysql_checkpoints(&a, &b),
                CheckpointOrder::Incomparable,
                "gtid {gtid:?}"
            );
            let later = lcp("binlog.000011", 4, gtid, LB);
            assert_eq!(
                compare_mysql_checkpoints(&a, &later),
                CheckpointOrder::Incomparable,
                "gtid {gtid:?}"
            );
        }
    }

    #[test]
    fn same_lineage_orders_by_position() {
        let u = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
        assert_eq!(
            compare_mysql_checkpoints(
                &lcp("binlog.000010", 400, None, LA),
                &lcp("binlog.000011", 4, None, LA)
            ),
            CheckpointOrder::Before
        );
        assert_eq!(
            compare_mysql_checkpoints(
                &lcp("binlog.000010", 400, Some(&format!("{u}:1-9")), LA),
                &lcp("binlog.000010", 400, Some(&format!("{u}:1-5")), LA)
            ),
            CheckpointOrder::After
        );
    }

    /// A lineage-bearing checkpoint and a legacy one are never ordered; two
    /// legacy checkpoints keep the position comparison.
    #[test]
    fn legacy_and_lineage_checkpoints_do_not_mix() {
        let u = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
        let g = format!("{u}:1-5");
        assert_eq!(
            compare_mysql_checkpoints(
                &lcp("binlog.000010", 400, None, LA),
                &cp("binlog.000011", 4)
            ),
            CheckpointOrder::Incomparable
        );
        assert_eq!(
            compare_mysql_checkpoints(
                &gcp("binlog.000010", 400, &g),
                &lcp("binlog.000010", 400, Some(&g), LA)
            ),
            CheckpointOrder::Incomparable
        );
        assert_eq!(
            compare_mysql_checkpoints(
                &cp("binlog.000010", 400),
                &cp("binlog.000011", 4)
            ),
            CheckpointOrder::Before
        );
    }

    /// Checkpoints written before lineage existed still parse.
    #[test]
    fn pre_lineage_checkpoint_bytes_parse_as_legacy() {
        let cp: super::MySqlCheckpoint = serde_json::from_slice(
            br#"{"file":"binlog.000001","pos":4,"gtid_set":null}"#,
        )
        .unwrap();
        assert_eq!(cp.lineage, None);
    }

    #[test]
    fn well_formed_is_reflexively_equal() {
        assert_eq!(
            compare_mysql_checkpoints(
                &cp("bin.000001", 42),
                &cp("bin.000001", 42)
            ),
            CheckpointOrder::Equal
        );
    }
}

#[cfg(test)]
mod identity_fail_closed_tests {
    //! R3-C2: MySQL live identity fetch fails closed.
    //!
    //! Container-backed failover behaviour (identity change detection, GTID tail
    //! switch, reconciliation) is covered by `tests/failover_e2e.rs`. Here we
    //! prove the shared `fetch_identity_verified` helper - used by both the
    //! pre-connect and post-reconnect paths - refuses to return when the live
    //! identity query cannot reach the server, instead of yielding an absent
    //! identity that would let the stream open on an unverified lineage.
    use super::fetch_identity_verified;

    // Port 1 is not bound; the connect is refused fast (ECONNREFUSED).
    const UNREACHABLE_DSN: &str = "mysql://root:none@127.0.0.1:1/none";

    #[tokio::test]
    async fn fetch_identity_verified_fails_closed_when_query_unavailable() {
        let err = fetch_identity_verified(UNREACHABLE_DSN)
            .await
            .expect_err("unreachable server must fail closed");
        let msg = err.to_string();
        assert!(
            msg.contains("refusing to stream"),
            "error should refuse to stream on unverified identity, got: {msg}"
        );
    }
}

#[cfg(test)]
mod snapshot_completion_tests {
    //! The #131 rules a legacy snapshot is proven complete by (design
    //! section 10), as the driver applies them with MySQL's order.
    use super::*;
    use crate::snapshot_driver::{SinkState, legacy_state};

    const UUID: &str = "3E11FA47-71CA-11E1-9E33-C80AA9429562";
    const LINEAGE: &str = "0123456789abcdef0123456789abcdef";

    fn at(set: &str) -> MySqlCheckpoint {
        MySqlCheckpoint {
            file: "mysql-bin.000003".into(),
            pos: 100,
            gtid_set: Some(format!("{UUID}:{set}")),
            lineage: Some(LINEAGE.into()),
            snapshot_completed: None,
            snapshot_chain: None,
        }
    }

    fn marked(set: &str, generation: u64) -> MySqlCheckpoint {
        MySqlCheckpoint {
            snapshot_completed: Some(generation),
            ..at(set)
        }
    }

    /// Whether `cp` proves the legacy snapshot of generation `generation`
    /// (`0`: its progress recorded none) anchored at `1-5` complete.
    fn proven(generation: u64, cp: &MySqlCheckpoint) -> bool {
        legacy_state(
            &MyOrder,
            Some(&serde_json::to_vec(cp).unwrap()),
            &at("1-5"),
            (generation != 0).then_some(generation),
        ) == SinkState::AtOrPast
    }

    #[test]
    fn completion_is_proven_only_at_the_anchor_or_strictly_after_it() {
        // The completing checkpoint of this generation, at the anchor.
        assert!(proven(3, &marked("1-5", 3)));
        // Unmarked and strictly after the anchor (CDC past it).
        assert!(proven(3, &at("1-9")));

        // Marked for another generation, or not at the anchor.
        assert!(!proven(3, &marked("1-5", 2)));
        assert!(!proven(3, &marked("1-9", 3)));
        // Unmarked at the anchor (what earlier releases committed), or before.
        assert!(!proven(3, &at("1-5")));
        assert!(!proven(3, &at("1-2")));
        // Another server (lineage), or incomparable (file/pos vs GTID).
        let foreign = MySqlCheckpoint {
            lineage: Some("fedcba9876543210fedcba9876543210".into()),
            ..at("1-9")
        };
        assert!(!proven(3, &foreign));
        let file_pos = MySqlCheckpoint {
            gtid_set: None,
            pos: 900,
            ..at("1-9")
        };
        assert!(!proven(3, &file_pos));
        // No checkpoint at all.
        assert_eq!(
            legacy_state(&MyOrder, None, &at("1-5"), Some(3)),
            SinkState::Behind
        );
    }

    #[test]
    fn a_record_without_a_generation_proves_only_past_the_anchor() {
        // A record without a generation proves completion only past the
        // anchor, never by a mark.
        assert!(!proven(0, &marked("1-5", 0)));
        assert!(proven(0, &at("1-9")));
    }
}

#[cfg(test)]
mod my_completion_tests {
    use super::{MyOrder, MySqlCheckpoint};
    use crate::snapshot_driver::{SinkState, sink_state};

    fn at(gtid: &str) -> MySqlCheckpoint {
        MySqlCheckpoint {
            file: "bin.000003".into(),
            pos: 100,
            gtid_set: Some(format!(
                "3E11FA47-71CA-11E1-9E33-C80AA9429562:{gtid}"
            )),
            lineage: Some("0123456789abcdef0123456789abcdef".into()),
            snapshot_completed: None,
            snapshot_chain: None,
        }
    }

    fn marked(gtid: &str, chain: &str, g: u64) -> Vec<u8> {
        serde_json::to_vec(&MySqlCheckpoint {
            snapshot_completed: Some(g),
            snapshot_chain: Some(chain.into()),
            ..at(gtid)
        })
        .unwrap()
    }

    /// The completing position counts only at exactly the anchor, for the
    /// generation and chain it names.
    #[test]
    fn a_completion_is_bound_to_its_chain_and_exact_anchor() {
        let anchor = at("1-5");
        let state = |raw: &[u8]| {
            sink_state(&MyOrder, Some(raw), "ch", 4, Some(&anchor))
        };
        assert_eq!(state(&marked("1-5", "ch", 4)), SinkState::AtOrPast);
        assert_eq!(state(&marked("1-5", "ch", 3)), SinkState::Behind);
        assert_eq!(state(&marked("1-5", "other", 4)), SinkState::Foreign);
        assert_eq!(state(&marked("1-7", "ch", 4)), SinkState::Foreign);
        let after = serde_json::to_vec(&at("1-9")).unwrap();
        assert_eq!(state(&after), SinkState::AtOrPast);
        let at_anchor = serde_json::to_vec(&at("1-5")).unwrap();
        assert_eq!(state(&at_anchor), SinkState::Behind, "unmarked");
        // A #131 mark (no chain) completes only the generation it names (a
        // legacy generation upgraded in place keeps its number).
        let legacy = |g| {
            serde_json::to_vec(&MySqlCheckpoint {
                snapshot_completed: Some(g),
                ..at("1-5")
            })
            .unwrap()
        };
        assert_eq!(state(&legacy(4)), SinkState::AtOrPast);
        assert_eq!(state(&legacy(3)), SinkState::Behind);
    }
}
