//! Activation baselines (design spec 7.7, binding rulings Rounds 25 and 29).
//!
//! When the stream starts at R0 and a tracked table's timeline does not
//! already prove its version there (a first start, a snapshot anchor, a
//! deployment upgraded from a version without timelines), its version is
//! established read-only:
//!
//! 1. a stable capture of the table's shape (position, shape, position equal
//!    on one verified connection) at S_t;
//! 2. one complete scan of (R0, S], S being the last capture's position (at
//!    or after every S_t), with the approved classifier;
//! 3. only if the scan proves no applicable table DDL and no barrier-class
//!    statement for the table in (R0, S]: the captured shape is registered
//!    through the normal registry path and a `baseline` record is appended at
//!    R0. The shape captured at S_t is the shape at S (nothing changed it in
//!    (S_t, S]) and at R0 (nothing changed it in (R0, S_t]).
//!
//! A baseline at the same position as the start's discontinuity barrier
//! carries that barrier's exact capture identity (`binds`); selection lets it
//! supersede that barrier only. Anything uncertain writes no activation
//! proof (a captured shape may already be registered): the table stays
//! unproven at R0.

use deltaforge_core::{CheckpointOrder, SourceError, SourceResult};
use tracing::{info, warn};

use super::mysql_activation::{
    self, Kind, Record, Selection, Stored, baseline_capture_id, select,
    table_stream,
};
use super::mysql_binlog_scan::{
    CLASSIFIER_VERSION, ProofError, ProofKind, ScanLimits, ScanReport, ScanTag,
    Statement, capture, covers, observe_capture, scan_interval, trace_proof,
};
use super::mysql_ddl_attribution::{BarrierScopeOf, DdlEffect, same_name};
use super::mysql_selection::Timeline;
use super::{MySqlCheckpoint, RunCtx};
use crate::durable_checkpoint::{
    WmPos, mysql_checkpoint_position, order_positions,
};
use storage::adapters::SchemaKey;

fn other_server(expected: &str, found: &str) -> SourceError {
    SourceError::Lineage {
        details: format!(
            "a baseline connection reached another server ({found}), \
             expected {expected}"
        )
        .into(),
    }
}

/// Whether a statement in (R0, S] may have changed `db.table` or invalidated
/// positional proof for it.
pub(crate) fn affects(
    statement: &Statement,
    db: &str,
    table: &str,
    lower_case_table_names: u8,
) -> bool {
    let lctn = lower_case_table_names;
    match &statement.effect {
        DdlEffect::None | DdlEffect::SameShape(_) => false,
        DdlEffect::Barrier(BarrierScopeOf::Lineage) => true,
        // Names compared case-insensitively but stored as written: not
        // mappable with certainty.
        _ if lctn == 2 => true,
        DdlEffect::Barrier(BarrierScopeOf::Database(d)) => {
            same_name(d, db, lctn)
        }
        DdlEffect::Tables(tables) => tables.iter().any(|t| {
            same_name(&t.db, db, lctn) && same_name(&t.table, table, lctn)
        }),
    }
}

/// The capture identity of the one barrier exactly at `r0` (the start's
/// discontinuity barrier) for the baseline to bind. `Err` when the position
/// is shared with anything a baseline cannot supersede: several barriers, or
/// a table record.
fn barrier_to_bind(
    table: &[Stored],
    barriers: &[Stored],
    r0: &WmPos,
) -> Result<Option<String>, &'static str> {
    let at = |s: &&Stored| {
        order_positions(&s.record.position, r0) == CheckpointOrder::Equal
    };
    if table.iter().any(|s| at(&s)) {
        return Err("a table record at the start position");
    }
    let mut here = barriers.iter().filter(at);
    match (here.next(), here.next()) {
        (None, _) => Ok(None),
        (Some(b), None) => Ok(Some(b.capture_id.clone())),
        _ => Err("several barriers at the start position"),
    }
}

struct Candidate {
    db: String,
    table: String,
    binds: Option<String>,
}

/// Establish baselines at the stream's start position (the snapshot anchor:
/// called only after a snapshot, for the tables it copied) for every table
/// not already proven there. Storage failures are fatal; an uncertain
/// or failed proof only leaves the table unproven (logged).
pub(crate) async fn establish(
    ctx: &RunCtx,
    tracked: &[(String, String)],
) -> SourceResult<usize> {
    let scope = ctx.registry_scope.current()?;
    let lineage = scope.lineage().lineage_hash.clone();
    let server_uuid = ctx.expected_uuid()?;
    let tag = ScanTag {
        source_id: &ctx.source_id,
        kind: ProofKind::Snapshot,
    };
    let lctn = ctx.lower_case_table_names;
    let r0_cp = MySqlCheckpoint {
        file: ctx.last_file.clone(),
        pos: ctx.last_pos,
        gtid_set: ctx.last_gtid.clone(),
        lineage: Some(lineage.clone()),
        snapshot_completed: None,
        snapshot_chain: None,
    };
    let Some(r0) = mysql_checkpoint_position(
        &r0_cp.file,
        r0_cp.pos,
        r0_cp.gtid_set.as_deref(),
    ) else {
        warn!(source_id = %ctx.source_id, "unparseable start position: no baselines");
        return Ok(0);
    };
    let storage = |e: anyhow::Error| {
        SourceError::Other(e.context("read the activation timeline"))
    };

    // Tables whose version is not proven at R0, and the barrier each binds.
    let mut candidates = Vec::new();
    for (db, table) in tracked {
        let key = scope.key(db, table);
        let records = mysql_activation::read_table(
            &ctx.registry_backend,
            ctx.schema.registry(),
            &key,
        )
        .await
        .map_err(storage)?;
        let barriers =
            mysql_activation::read_barriers(&ctx.registry_backend, &key)
                .await
                .map_err(storage)?;
        let all: Vec<Stored> =
            records.iter().chain(barriers.iter()).cloned().collect();
        if matches!(select(&all, &r0), Selection::Proven { .. }) {
            continue;
        }
        match barrier_to_bind(&records, &barriers, &r0) {
            Ok(binds) => candidates.push(Candidate {
                db: db.clone(),
                table: table.clone(),
                binds,
            }),
            Err(why) => {
                warn!(source_id = %ctx.source_id, %db, %table, why, "no baseline");
            }
        }
    }
    if candidates.is_empty() {
        return Ok(0);
    }

    // 1. A capture of each table's shape, bound to its interval by step 2.
    let mut captured = Vec::new();
    for c in candidates {
        match capture(
            ctx.dsn.expose(),
            &server_uuid,
            &lineage,
            &c.db,
            &c.table,
            None,
        )
        .await
        {
            Ok(cap) => {
                observe_capture(&tag, &cap);
                captured.push((c, cap))
            }
            // The connection reached another server: never a "no baseline".
            Err(ProofError::OtherServer(found)) => {
                return Err(other_server(&server_uuid, &found));
            }
            Err(e) => {
                warn!(source_id = %ctx.source_id, db = %c.db, table = %c.table, error = ?e, "no baseline: no capture");
                trace_proof(
                    &tag,
                    &c.db,
                    &c.table,
                    None,
                    None,
                    lctn,
                    "no_capture",
                );
            }
        }
    }
    let Some(s_cp) = captured.last().map(|(_, cap)| cap.position.clone())
    else {
        return Ok(0);
    };
    let Some(s) = mysql_checkpoint_position(
        &s_cp.file,
        s_cp.pos,
        s_cp.gtid_set.as_deref(),
    ) else {
        return Ok(0);
    };

    // 2. One complete scan of (R0, S].
    let report: ScanReport = match scan_interval(
        ctx.dsn.expose(),
        super::mysql_helpers::derive_server_id(&format!(
            "{}/baseline",
            ctx.source_id
        )),
        &server_uuid,
        &lineage,
        &r0_cp,
        &s_cp,
        &ScanLimits::default(),
        &tag,
    )
    .await
    {
        Ok(report) => report,
        Err(ProofError::OtherServer(found)) => {
            return Err(other_server(&server_uuid, &found));
        }
        Err(e) => {
            warn!(source_id = %ctx.source_id, error = ?e, "no baselines: the interval scan failed");
            for (c, cap) in &captured {
                trace_proof(
                    &tag,
                    &c.db,
                    &c.table,
                    Some(cap),
                    None,
                    lctn,
                    "scan_failed",
                );
            }
            return Ok(0);
        }
    };

    // 3. A baseline for every table the scan proves untouched.
    let checkpoint =
        serde_json::to_vec(&r0_cp).map_err(|e| SourceError::Other(e.into()))?;
    let mut written = 0;
    for (c, cap) in captured {
        if !covers(&cap, &r0, &s) {
            warn!(source_id = %ctx.source_id, db = %c.db, table = %c.table, "no baseline: the scan does not cover the capture interval");
            trace_proof(
                &tag,
                &c.db,
                &c.table,
                Some(&cap),
                Some(&report),
                lctn,
                "not_covered",
            );
            continue;
        }
        if report
            .statements
            .iter()
            .any(|st| affects(st, &c.db, &c.table, ctx.lower_case_table_names))
        {
            info!(source_id = %ctx.source_id, db = %c.db, table = %c.table, "no baseline: a DDL or barrier in the scanned interval");
            trace_proof(
                &tag,
                &c.db,
                &c.table,
                Some(&cap),
                Some(&report),
                lctn,
                "relevant_ddl",
            );
            continue;
        }
        let (version, schema_hash) = ctx
            .schema
            .register_captured(&c.db, &c.table, &cap.schema, &checkpoint)
            .await?;
        let key = scope.key(&c.db, &c.table);
        let stream = table_stream(&key);
        let record = Record::new(
            r0.clone(),
            Kind::Baseline {
                version,
                schema_hash,
                to: s.clone(),
                scan_digest: report.digest.clone(),
                binds: c.binds,
                classifier_version: CLASSIFIER_VERSION.to_string(),
            },
        );
        let id = baseline_capture_id(&lineage, &stream, &record)
            .map_err(SourceError::Other)?;
        mysql_activation::append(
            &ctx.registry_backend,
            ctx.schema.registry(),
            &key,
            mysql_activation::ACTIVATION_NS,
            &stream,
            &id,
            &record,
        )
        .await
        .map_err(|e| SourceError::Other(e.context("persist a baseline")))?;
        trace_proof(
            &tag,
            &c.db,
            &c.table,
            Some(&cap),
            Some(&report),
            lctn,
            "proven",
        );
        written += 1;
    }
    info!(
        source_id = %ctx.source_id,
        baselines = written,
        scanned_events = report.events,
        "activation baselines established"
    );
    Ok(written)
}

/// A lazy baseline (binding ruling Round 36, Q1) for one table whose rows
/// at the evaluation position E are not positionally proven: a stable
/// capture of its complete shape at S, E <= S in the verified lineage, and
/// one complete scan of (E, S] in which nothing applies to the table. The
/// captured shape is registered and a `baseline` at E (binding the one
/// barrier exactly at E, if any) is appended - before the rows are decoded -
/// only if it then decides the rows (see `admit_decisive`). Anything
/// uncertain writes no activation proof and returns `false` (a candidate
/// schema already registered from the capture may remain registered, as in
/// the FULL fallback); a connection on another server or a storage failure
/// is an error.
pub(crate) async fn establish_at(
    ctx: &mut RunCtx,
    db: &str,
    table: &str,
    key: &SchemaKey,
    current: &Timeline,
) -> SourceResult<bool> {
    let (Some(e), Some(mut e_cp)) =
        (ctx.txn_eval.clone(), ctx.txn_eval_cp.clone())
    else {
        return Ok(false);
    };
    let lineage = key.lineage_hash.clone();
    e_cp.lineage = Some(lineage.clone());
    let server_uuid = ctx.expected_uuid()?;
    let source_id = ctx.source_id.clone();
    let tag = ScanTag {
        source_id: &source_id,
        kind: ProofKind::Lazy,
    };
    let lctn = ctx.lower_case_table_names;
    let binds = match barrier_to_bind(&current.records, &current.barriers, &e) {
        Ok(binds) => binds,
        Err(why) => {
            info!(source_id = %ctx.source_id, %db, %table, why, "no lazy baseline");
            return Ok(false);
        }
    };
    let cap = match capture(
        ctx.dsn.expose(),
        &server_uuid,
        &lineage,
        db,
        table,
        None,
    )
    .await
    {
        Ok(cap) => {
            observe_capture(&tag, &cap);
            cap
        }
        Err(ProofError::OtherServer(found)) => {
            return Err(other_server(&server_uuid, &found));
        }
        Err(err) => {
            warn!(source_id = %ctx.source_id, %db, %table, error = ?err, "no lazy baseline: no capture");
            trace_proof(&tag, db, table, None, None, lctn, "no_capture");
            return Ok(false);
        }
    };
    let s_cp = cap.position.clone();
    let Some(s) = mysql_checkpoint_position(
        &s_cp.file,
        s_cp.pos,
        s_cp.gtid_set.as_deref(),
    ) else {
        return Ok(false);
    };
    if !covers(&cap, &e, &s) {
        warn!(source_id = %ctx.source_id, %db, %table, "no lazy baseline: the capture did not start at or after the rows");
        trace_proof(&tag, db, table, Some(&cap), None, lctn, "not_covered");
        return Ok(false);
    }
    let report = match scan_interval(
        ctx.dsn.expose(),
        super::mysql_helpers::derive_server_id(&format!(
            "{}/baseline",
            ctx.source_id
        )),
        &server_uuid,
        &lineage,
        &e_cp,
        &s_cp,
        &ScanLimits::default(),
        &tag,
    )
    .await
    {
        Ok(report) => report,
        Err(ProofError::OtherServer(found)) => {
            return Err(other_server(&server_uuid, &found));
        }
        Err(err) => {
            warn!(source_id = %ctx.source_id, %db, %table, error = ?err, "no lazy baseline: the interval scan failed");
            trace_proof(&tag, db, table, Some(&cap), None, lctn, "scan_failed");
            return Ok(false);
        }
    };
    if report
        .statements
        .iter()
        .any(|st| affects(st, db, table, ctx.lower_case_table_names))
    {
        info!(source_id = %ctx.source_id, %db, %table, "no lazy baseline: a DDL or barrier after the rows");
        trace_proof(
            &tag,
            db,
            table,
            Some(&cap),
            Some(&report),
            lctn,
            "relevant_ddl",
        );
        return Ok(false);
    }
    let checkpoint = serde_json::to_vec(&e_cp)
        .map_err(|err| SourceError::Other(err.into()))?;
    let (version, schema_hash) = ctx
        .schema
        .register_captured(db, table, &cap.schema, &checkpoint)
        .await?;
    let stream = table_stream(key);
    let record = Record::new(
        e.clone(),
        Kind::Baseline {
            version,
            schema_hash,
            to: s,
            scan_digest: report.digest.clone(),
            binds,
            classifier_version: CLASSIFIER_VERSION.to_string(),
        },
    );
    let id = baseline_capture_id(&lineage, &stream, &record)
        .map_err(SourceError::Other)?;
    let proposed = Stored {
        capture_id: id.clone(),
        record: record.clone(),
    };
    if let Err(why) = super::mysql_selection::admit_decisive(
        ctx.schema.registry(),
        key,
        current,
        proposed,
        &e,
    )
    .await
    {
        info!(source_id = %ctx.source_id, %db, %table, why, "no lazy baseline");
        trace_proof(
            &tag,
            db,
            table,
            Some(&cap),
            Some(&report),
            lctn,
            "not_decisive",
        );
        return Ok(false);
    }
    mysql_activation::append(
        &ctx.registry_backend,
        ctx.schema.registry(),
        key,
        mysql_activation::ACTIVATION_NS,
        &stream,
        &id,
        &record,
    )
    .await
    .map_err(|err| SourceError::Other(err.context("persist a baseline")))?;
    ctx.selection.invalidate(key);
    info!(source_id = %ctx.source_id, %db, %table, version, scanned_events = report.events, "lazy baseline established");
    trace_proof(&tag, db, table, Some(&cap), Some(&report), lctn, "proven");
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mysql::mysql_activation::{BarrierScope, EventIdentity};
    use crate::mysql::mysql_ddl_attribution::TableName;

    fn stmt(effect: DdlEffect) -> Statement {
        Statement {
            event: EventIdentity::FilePos {
                file: "b.000001".into(),
                end_pos: 9,
                ordinal: 0,
            },
            effect,
        }
    }

    fn t(db: &str, table: &str) -> TableName {
        TableName {
            db: db.into(),
            table: table.into(),
        }
    }

    #[test]
    fn only_applicable_ddl_and_barriers_prevent_a_baseline() {
        let tables = |v: Vec<TableName>| stmt(DdlEffect::Tables(v));
        assert!(affects(&tables(vec![t("d", "a")]), "d", "a", 0));
        assert!(affects(
            &tables(vec![t("x", "y"), t("d", "a")]),
            "d",
            "a",
            0
        ));
        assert!(!affects(&tables(vec![t("d", "b")]), "d", "a", 0));
        assert!(!affects(&tables(vec![t("e", "a")]), "d", "a", 0));
        assert!(!affects(
            &stmt(DdlEffect::SameShape(vec![t("d", "a")])),
            "d",
            "a",
            0
        ));
        assert!(affects(
            &stmt(DdlEffect::Barrier(BarrierScopeOf::Lineage)),
            "d",
            "a",
            0
        ));
        assert!(affects(
            &stmt(DdlEffect::Barrier(BarrierScopeOf::Database("d".into()))),
            "d",
            "a",
            0
        ));
        assert!(!affects(
            &stmt(DdlEffect::Barrier(BarrierScopeOf::Database("e".into()))),
            "d",
            "a",
            0
        ));
        // Case per lower_case_table_names.
        assert!(!affects(&tables(vec![t("D", "A")]), "d", "a", 0));
        assert!(affects(&tables(vec![t("D", "A")]), "d", "a", 1));
        assert!(affects(&tables(vec![t("e", "z")]), "d", "a", 2));
        let db = |d: &str| {
            stmt(DdlEffect::Barrier(BarrierScopeOf::Database(d.into())))
        };
        assert!(!affects(&db("D"), "d", "a", 0));
        assert!(affects(&db("D"), "d", "a", 1));
        assert!(!affects(&db("e"), "d", "a", 1));
        assert!(affects(&db("e"), "d", "a", 2));
    }

    fn st(id: &str, pos: u64, kind: Kind) -> Stored {
        Stored {
            capture_id: id.into(),
            record: Record::new(
                WmPos::MysqlBinlog {
                    file_base: "b".into(),
                    file_index: 1,
                    pos,
                },
                kind,
            ),
        }
    }

    #[test]
    fn a_baseline_binds_only_the_one_barrier_at_its_position() {
        let r0 = WmPos::MysqlBinlog {
            file_base: "b".into(),
            file_index: 1,
            pos: 10,
        };
        let lineage = || Kind::Barrier {
            scope: BarrierScope::Lineage,
        };
        assert_eq!(barrier_to_bind(&[], &[], &r0), Ok(None));
        assert_eq!(
            barrier_to_bind(&[], &[st("old", 5, lineage())], &r0),
            Ok(None)
        );
        assert_eq!(
            barrier_to_bind(
                &[],
                &[st("old", 5, lineage()), st("start", 10, lineage())],
                &r0
            ),
            Ok(Some("start".into()))
        );
        assert!(
            barrier_to_bind(
                &[],
                &[st("a", 10, lineage()), st("b", 10, lineage())],
                &r0
            )
            .is_err()
        );
        assert!(
            barrier_to_bind(&[st("ddl", 10, Kind::Ddl)], &[], &r0).is_err()
        );
    }

    /// Live MySQL 8.4 (Docker): startup establishes nothing; a table's first
    /// unproven rows get a lazy baseline at their evaluation position, which
    /// selection over the stored records then proves. They share one server
    /// and pace phases by time: run them serially
    /// (`-- --include-ignored --test-threads=1 mysql_baseline`).
    mod live {
        use super::*;
        use crate::mysql::MySqlSource;
        use crate::mysql::mysql_activation::{read_barriers, read_table};
        use checkpoints::{
            CheckpointStore, CheckpointStoreExt, MemCheckpointStore,
        };
        use deltaforge_config::{OnSchemaDrift, SnapshotCfg, SnapshotMode};
        use deltaforge_core::Source;
        use gate_ownership::GateOwned;
        use mysql_async::prelude::Queryable;
        use std::sync::Arc;
        use std::time::Duration;
        use storage::{
            ArcStorageBackend, DurableSchemaRegistry, MemoryStorageBackend,
        };
        use testcontainers::core::WaitFor;
        use testcontainers::runners::AsyncRunner;
        use testcontainers::{ContainerAsync, GenericImage, ImageExt};
        use tokio::sync::OnceCell;

        static GTID: OnceCell<(ContainerAsync<GenericImage>, u16)> =
            OnceCell::const_new();
        static FILEPOS: OnceCell<(ContainerAsync<GenericImage>, u16)> =
            OnceCell::const_new();

        async fn start(gtid: bool) -> (ContainerAsync<GenericImage>, u16) {
            let mut cmd = vec![
                "--server-id=51",
                "--log-bin=mysql-bin",
                "--binlog-format=ROW",
                "--binlog-checksum=NONE",
            ];
            if gtid {
                cmd.extend(["--gtid-mode=ON", "--enforce-gtid-consistency=ON"]);
            }
            let c = GenericImage::new("mysql", "8.4")
                .with_wait_for(WaitFor::message_on_stderr(
                    "ready for connections. Version: '8.4",
                ))
                .with_env_var("MYSQL_ROOT_PASSWORD", "pw")
                .with_cmd(cmd)
                .gate_owned()
                .start()
                .await
                .expect("start mysql");
            let port = c.get_host_port_ipv4(3306).await.unwrap();
            for _ in 0..60 {
                if mysql_async::Conn::from_url(dsn(port, "")).await.is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            (c, port)
        }

        async fn server(gtid: bool) -> u16 {
            let cell = if gtid { &GTID } else { &FILEPOS };
            cell.get_or_init(|| start(gtid)).await.1
        }

        fn dsn(port: u16, db: &str) -> String {
            format!("mysql://root:pw@127.0.0.1:{port}/{db}")
        }

        async fn sql(port: u16, stmts: &[String]) {
            let mut c =
                mysql_async::Conn::from_url(dsn(port, "")).await.unwrap();
            for s in stmts {
                c.query_drop(s).await.unwrap_or_else(|e| panic!("{s}: {e}"));
            }
            c.disconnect().await.ok();
        }

        struct State {
            backend: ArcStorageBackend,
            registry: Arc<DurableSchemaRegistry>,
            ckpt: Arc<dyn CheckpointStore>,
            scope: crate::registry_scope::SharedRegistryScope,
        }

        async fn state() -> State {
            let backend: ArcStorageBackend =
                Arc::new(MemoryStorageBackend::new());
            State {
                registry: DurableSchemaRegistry::new(backend.clone())
                    .await
                    .unwrap(),
                ckpt: Arc::new(MemCheckpointStore::new().unwrap()),
                scope: crate::registry_scope::SharedRegistryScope::default(),
                backend,
            }
        }

        /// Run the source on `tables` (no snapshot unless `mode` says so),
        /// execute `during` once it is streaming (phases separated by an
        /// empty statement), then stop it. Returns the
        /// rows it emitted and how it ended.
        async fn run(
            st: &State,
            port: u16,
            id: &str,
            tables: &[&str],
            mode: SnapshotMode,
            during: &[String],
        ) -> (Vec<deltaforge_core::Event>, SourceResult<()>) {
            let src = MySqlSource {
                id: id.into(),
                dsn: dsn(port, "").as_str().into(),
                tables: tables.iter().map(|t| t.to_string()).collect(),
                tenant: "acme".into(),
                pipeline: "test".into(),
                registry: st.registry.clone(),
                registry_scope: st.scope.clone(),
                backend: st.backend.clone(),
                outbox_tables: common::AllowList::default(),
                snapshot_cfg: SnapshotCfg {
                    mode,
                    ..Default::default()
                },
                on_schema_drift: OnSchemaDrift::Adapt,
                table_options: Default::default(),
                rotation: None,
                snapshot_cohort: Default::default(),
            };
            // One required sink; its barriers commit through the runner's
            // own barrier commit, so a generation can start and complete.
            src.set_snapshot_cohort(deltaforge_core::SnapshotCohort {
                policy: deltaforge_core::CohortPolicy::Required,
                sinks: vec![deltaforge_core::CohortSink {
                    id: "sink".into(),
                    required: true,
                }],
            });
            let commit = runner::coordinator::build_barrier_fn(
                st.ckpt.clone(),
                format!("{id}::sink::sink"),
                Arc::new(src.clone()),
            );
            let (tx, mut rx) = tokio::sync::mpsc::channel(1024);
            let handle = src.run(tx, st.ckpt.clone()).await;
            let rows = tokio::spawn(async move {
                use deltaforge_core::{BarrierKind, SourceItem};
                use runner::coordinator::BarrierCommit;
                let mut rows = Vec::new();
                while let Some(item) = rx.recv().await {
                    match item {
                        SourceItem::Barrier { barrier } => {
                            let c = match barrier.kind {
                                BarrierKind::GenerationStart(s) => {
                                    BarrierCommit::Start(s)
                                }
                                BarrierKind::Terminal => {
                                    BarrierCommit::Checkpoint(
                                        barrier.boundary.checkpoint,
                                    )
                                }
                            };
                            if commit(c).await.is_err() {
                                break;
                            }
                        }
                        SourceItem::Event(e) if e.ddl.is_none() => rows.push(e),
                        _ => {}
                    }
                }
                rows
            });
            tokio::time::sleep(Duration::from_secs(4)).await;
            // An empty statement separates phases: the source catches up
            // before the next one runs.
            for phase in during.split(|s| s.is_empty()) {
                sql(port, phase).await;
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
            handle.stop();
            let res =
                tokio::time::timeout(Duration::from_secs(30), handle.join)
                    .await
                    .expect("stops")
                    .expect("task");
            let rows = rows.await.unwrap();
            (rows, res)
        }

        /// The table's records and its barriers.
        async fn timeline(
            st: &State,
            db: &str,
            table: &str,
        ) -> (Vec<Stored>, Vec<Stored>) {
            let key = st.scope.current().unwrap().key(db, table);
            (
                read_table(&st.backend, &st.registry, &key).await.unwrap(),
                read_barriers(&st.backend, &key).await.unwrap(),
            )
        }

        async fn stored_position(st: &State, id: &str) -> WmPos {
            let cp: crate::MySqlCheckpoint =
                st.ckpt.get(id).await.unwrap().unwrap();
            mysql_checkpoint_position(&cp.file, cp.pos, cp.gtid_set.as_deref())
                .unwrap()
        }

        fn baselines(records: &[Stored]) -> Vec<&Stored> {
            records
                .iter()
                .filter(|s| matches!(s.record.kind, Kind::Baseline { .. }))
                .collect()
        }

        fn proven(
            table: &[Stored],
            barriers: &[Stored],
            at: &WmPos,
        ) -> Selection {
            let all: Vec<Stored> =
                table.iter().chain(barriers.iter()).cloned().collect();
            select(&all, at)
        }

        async fn prepare(port: u16, db: &str, tables: &[&str]) {
            let mut s = vec![
                format!("DROP DATABASE IF EXISTS {db}"),
                format!("CREATE DATABASE {db}"),
            ];
            for t in tables {
                s.push(format!(
                    "CREATE TABLE {db}.{t} (id INT PRIMARY KEY, v INT)"
                ));
            }
            sql(port, &s).await;
        }

        async fn fresh_start(gtid: bool) {
            let port = server(gtid).await;
            let db = if gtid { "base_g" } else { "base_f" };
            prepare(port, db, &["t"]).await;
            let st = state().await;
            let t = format!("{db}.t");

            // Startup alone establishes nothing.
            let (rows, res) =
                run(&st, port, db, &[&t], SnapshotMode::Never, &[]).await;
            res.unwrap();
            assert!(rows.is_empty());
            let r0 = stored_position(&st, db).await;
            let (table, barriers) = timeline(&st, db, "t").await;
            assert!(table.is_empty(), "{table:?}");
            let start: Vec<&Stored> = barriers
                .iter()
                .filter(|b| {
                    order_positions(&b.record.position, &r0)
                        == CheckpointOrder::Equal
                })
                .collect();
            assert_eq!(start.len(), 1, "{barriers:?}");

            // The first rows (evaluated at the start position) get a lazy
            // baseline there, binding the start barrier.
            let st2 = state().await;
            let (rows, res) = run(
                &st2,
                port,
                db,
                &[&t],
                SnapshotMode::Never,
                &[format!("INSERT INTO {t} VALUES (1, 1)")],
            )
            .await;
            res.unwrap();
            assert_eq!(rows.len(), 1);
            let (table, barriers) = timeline(&st2, db, "t").await;
            assert_eq!(barriers.len(), 1, "{barriers:?}");
            let r0 = barriers[0].record.position.clone();
            let base = baselines(&table);
            assert_eq!(base.len(), 1, "{table:?}");
            assert_eq!(
                order_positions(&base[0].record.position, &r0),
                CheckpointOrder::Equal
            );
            let start: Vec<&Stored> = barriers
                .iter()
                .filter(|b| {
                    order_positions(&b.record.position, &r0)
                        == CheckpointOrder::Equal
                })
                .collect();
            let Kind::Baseline { version, binds, .. } = &base[0].record.kind
            else {
                unreachable!()
            };
            assert_eq!(binds.as_deref(), Some(start[0].capture_id.as_str()));
            assert_eq!(
                proven(&table, &barriers, &r0),
                Selection::Proven { version: *version }
            );

            // A restart at the committed (already proven) position adds no
            // record, and its next rows reuse the proof.
            let (rows, res) = run(
                &st2,
                port,
                db,
                &[&t],
                SnapshotMode::Never,
                &[format!("INSERT INTO {t} VALUES (2, 2)")],
            )
            .await;
            res.unwrap();
            assert_eq!(rows.len(), 1);
            let (again, barriers_again) = timeline(&st2, db, "t").await;
            assert_eq!(again, table);
            assert_eq!(barriers_again, barriers);
        }

        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_first_rows_baseline_binds_the_start_barrier_gtid() {
            fresh_start(true).await;
        }

        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_first_rows_baseline_binds_the_start_barrier_file_position() {
            fresh_start(false).await;
        }

        /// A DDL of the table after its rows (in (E, S]) prevents its lazy
        /// baseline and the source stops before those rows; another table's
        /// earlier rows still get theirs.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_later_ddl_of_the_table_prevents_its_lazy_baseline() {
            let port = server(true).await;
            let db = "base_scan";
            prepare(port, db, &["t", "u"]).await;
            let st = state().await;
            let (t, u) = (format!("{db}.t"), format!("{db}.u"));
            run(&st, port, db, &[&t, &u], SnapshotMode::Never, &[])
                .await
                .1
                .unwrap();
            let committed = st.ckpt.get_raw(db).await.unwrap().unwrap();
            // While stopped: u's row, t's row, then a DDL of t.
            sql(
                port,
                &[
                    format!("INSERT INTO {u} VALUES (1, 1)"),
                    format!("INSERT INTO {t} VALUES (1, 1)"),
                    format!("ALTER TABLE {t} ADD COLUMN w INT"),
                ],
            )
            .await;
            let (rows, res) =
                run(&st, port, db, &[&t, &u], SnapshotMode::Never, &[]).await;
            let err = format!("{:#}", res.expect_err("t's rows are refused"));
            assert!(err.contains("no positional proof"), "{err}");
            assert_eq!(rows.len(), 1, "only u's row");
            assert_eq!(rows[0].source.table, "u");
            assert_eq!(baselines(&timeline(&st, db, "u").await.0).len(), 1);
            assert!(baselines(&timeline(&st, db, "t").await.0).is_empty());
            assert_eq!(st.ckpt.get_raw(db).await.unwrap().unwrap(), committed);
        }

        /// After a barrier (here a versioned-comment DDL, unattributable), a
        /// table's next rows restore proof by a lazy baseline: later
        /// authoritative evidence for that table supersedes the barrier.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_lazy_baseline_restores_proof_after_a_barrier() {
            let port = server(true).await;
            let db = "base_barrier";
            prepare(port, db, &["t"]).await;
            let st = state().await;
            let t = format!("{db}.t");
            let (rows, res) = run(
                &st,
                port,
                db,
                &[&t],
                SnapshotMode::Never,
                &[
                    format!("INSERT INTO {t} VALUES (1, 1)"),
                    String::new(),
                    format!("/*!50100 ALTER TABLE {t} ADD COLUMN w INT */"),
                    format!("INSERT INTO {t} VALUES (2, 2, 2)"),
                ],
            )
            .await;
            res.unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[1].after.as_ref().unwrap()["w"], 2);
            let (table, barriers) = timeline(&st, db, "t").await;
            let base = baselines(&table);
            assert_eq!(base.len(), 2, "{table:?}");
            // The second baseline follows the in-stream barrier.
            let barrier = barriers.last().unwrap();
            assert_eq!(
                order_positions(
                    &barrier.record.position,
                    &base[1].record.position
                ),
                CheckpointOrder::Equal
            );
            assert!(matches!(
                &base[1].record.kind,
                Kind::Baseline { binds: Some(b), .. } if *b == barrier.capture_id
            ));
        }

        /// Stored schema history that cannot be read is corrupt: a load of it
        /// (here by a snapshot) fails closed, never replaced by the live
        /// catalog, and nothing is emitted.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_corrupt_stored_schema_fails_closed() {
            let port = server(true).await;
            let db = "base_corrupt";
            prepare(port, db, &["t"]).await;
            sql(port, &[format!("INSERT INTO {db}.t VALUES (1, 1)")]).await;
            let st = state().await;
            let t = format!("{db}.t");
            // Establish the lineage, then store an unreadable latest version.
            run(&st, port, db, &[&t], SnapshotMode::Never, &[])
                .await
                .1
                .unwrap();
            let key = st.scope.current().unwrap().key(db, "t");
            st.registry
                .register_with_checkpoint(
                    &key,
                    "corrupt",
                    &serde_json::json!({ "columns": 5 }),
                    None,
                )
                .await
                .unwrap();
            let (rows, res) =
                run(&st, port, db, &[&t], SnapshotMode::Always, &[]).await;
            let err = format!("{:#}", res.expect_err("fails closed"));
            assert!(err.contains("is unreadable"), "{err}");
            assert!(rows.is_empty(), "{rows:?}");
        }

        /// A snapshot proves every table it copied at its anchor (they are
        /// already enumerated and loaded): one baseline there, binding the
        /// anchor's barrier, before any CDC rows.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_snapshot_proves_its_tables_at_the_anchor() {
            let port = server(true).await;
            let db = "base_snap";
            prepare(port, db, &["t"]).await;
            sql(port, &[format!("INSERT INTO {db}.t VALUES (1, 1)")]).await;
            let st = state().await;
            let t = format!("{db}.t");
            // No CDC rows: the proof exists before any first use.
            let (rows, res) =
                run(&st, port, db, &[&t], SnapshotMode::Initial, &[]).await;
            res.unwrap();
            assert_eq!(rows.len(), 1, "the snapshot row only");
            let (table, barriers) = timeline(&st, db, "t").await;
            assert_eq!(barriers.len(), 1, "{barriers:?}");
            let anchor = &barriers[0];
            let base = baselines(&table);
            assert_eq!(base.len(), 1, "{table:?}");
            assert_eq!(
                order_positions(
                    &anchor.record.position,
                    &base[0].record.position
                ),
                CheckpointOrder::Equal
            );
            assert!(matches!(
                &base[0].record.kind,
                Kind::Baseline { binds: Some(b), .. } if *b == anchor.capture_id
            ));
        }
    }
}
