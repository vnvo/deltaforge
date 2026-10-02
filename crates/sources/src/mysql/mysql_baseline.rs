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
//! supersede that barrier only. Anything uncertain writes nothing: the table
//! stays unproven at R0.

use deltaforge_core::{CheckpointOrder, SourceError, SourceResult};
use tracing::{info, warn};

use super::mysql_activation::{
    self, Kind, Record, Selection, Stored, baseline_capture_id, select,
    table_stream,
};
use super::mysql_binlog_scan::{
    CLASSIFIER_VERSION, ProofError, ScanLimits, ScanReport, Statement,
    scan_interval, stable_capture,
};
use super::mysql_ddl_attribution::{BarrierScopeOf, DdlEffect, same_name};
use super::{MySqlCheckpoint, RunCtx};
use crate::durable_checkpoint::{
    WmPos, mysql_checkpoint_position, order_positions,
};

fn other_server(expected: &str, found: &str) -> SourceError {
    SourceError::Lineage {
        details: format!(
            "a baseline connection reached another server ({found}), \
             expected {expected}"
        )
        .into(),
    }
}

/// Stable-capture attempts per table before giving up on its baseline.
const CAPTURE_ATTEMPTS: u32 = 5;

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

/// Establish baselines at the stream's start position for every tracked
/// table not already proven there. Storage failures are fatal; an uncertain
/// or failed proof only leaves the table unproven (logged).
pub(crate) async fn establish(
    ctx: &RunCtx,
    tracked: &[(String, String)],
) -> SourceResult<usize> {
    let scope = ctx.registry_scope.current()?;
    let lineage = scope.lineage().lineage_hash.clone();
    let server_uuid = ctx.expected_uuid()?;
    let r0_cp = MySqlCheckpoint {
        file: ctx.last_file.clone(),
        pos: ctx.last_pos,
        gtid_set: ctx.last_gtid.clone(),
        lineage: Some(lineage.clone()),
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

    // 1. A stable capture of each table's shape.
    let mut captured = Vec::new();
    for c in candidates {
        match stable_capture(
            ctx.dsn.expose(),
            &server_uuid,
            &lineage,
            &c.db,
            &c.table,
            CAPTURE_ATTEMPTS,
            None,
        )
        .await
        {
            Ok(cap) => captured.push((c, cap)),
            // The connection reached another server: never a "no baseline".
            Err(ProofError::OtherServer(found)) => {
                return Err(other_server(&server_uuid, &found));
            }
            Err(e) => {
                warn!(source_id = %ctx.source_id, db = %c.db, table = %c.table, error = ?e, "no baseline: no stable capture");
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
    )
    .await
    {
        Ok(report) => report,
        Err(ProofError::OtherServer(found)) => {
            return Err(other_server(&server_uuid, &found));
        }
        Err(e) => {
            warn!(source_id = %ctx.source_id, error = ?e, "no baselines: the interval scan failed");
            return Ok(0);
        }
    };

    // 3. A baseline for every table the scan proves untouched.
    let checkpoint =
        serde_json::to_vec(&r0_cp).map_err(|e| SourceError::Other(e.into()))?;
    let mut written = 0;
    for (c, cap) in captured {
        let at_or_before_s = matches!(
            mysql_checkpoint_position(
                &cap.position.file,
                cap.position.pos,
                cap.position.gtid_set.as_deref()
            )
            .map(|p| order_positions(&p, &s)),
            Some(CheckpointOrder::Before | CheckpointOrder::Equal)
        );
        if !at_or_before_s {
            warn!(source_id = %ctx.source_id, db = %c.db, table = %c.table, "no baseline: capture not before the scan end");
            continue;
        }
        if report
            .statements
            .iter()
            .any(|st| affects(st, &c.db, &c.table, ctx.lower_case_table_names))
        {
            info!(source_id = %ctx.source_id, db = %c.db, table = %c.table, "no baseline: a DDL or barrier in the scanned interval");
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

    /// Live MySQL 8.4 (Docker): the source establishes baselines at its
    /// start position; selection over the stored records proves them.
    mod live {
        use super::*;
        use crate::mysql::MySqlSource;
        use crate::mysql::mysql_activation::{read_barriers, read_table};
        use checkpoints::{
            CheckpointStore, CheckpointStoreExt, MemCheckpointStore,
        };
        use deltaforge_config::{OnSchemaDrift, SnapshotCfg, SnapshotMode};
        use deltaforge_core::Source;
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

        /// Run the source on `tables` until startup is done, then stop it.
        async fn run_once(
            st: &State,
            port: u16,
            id: &str,
            tables: &[&str],
            mode: SnapshotMode,
        ) {
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
            };
            let (tx, mut rx) = tokio::sync::mpsc::channel(1024);
            let handle = src.run(tx, st.ckpt.clone()).await;
            let drain =
                tokio::spawn(async move { while rx.recv().await.is_some() {} });
            tokio::time::sleep(Duration::from_secs(4)).await;
            handle.stop();
            tokio::time::timeout(Duration::from_secs(30), handle.join)
                .await
                .expect("stops")
                .expect("task")
                .expect("source ran cleanly");
            drain.abort();
        }

        /// The table's records and its barriers, and the stored position.
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
            let cp: MySqlCheckpoint = st.ckpt.get(id).await.unwrap().unwrap();
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

        async fn fresh_start(gtid: bool) {
            let port = server(gtid).await;
            let db = if gtid { "base_g" } else { "base_f" };
            sql(
                port,
                &[
                    format!("DROP DATABASE IF EXISTS {db}"),
                    format!("CREATE DATABASE {db}"),
                    format!("CREATE TABLE {db}.t (id INT PRIMARY KEY, v INT)"),
                ],
            )
            .await;
            let st = state().await;
            run_once(&st, port, db, &[&format!("{db}.t")], SnapshotMode::Never)
                .await;
            let r0 = stored_position(&st, db).await;
            let (table, barriers) = timeline(&st, db, "t").await;
            // The start barrier and the baseline that binds it, at R0.
            let start: Vec<&Stored> = barriers
                .iter()
                .filter(|b| {
                    order_positions(&b.record.position, &r0)
                        == CheckpointOrder::Equal
                })
                .collect();
            assert_eq!(start.len(), 1, "{barriers:?}");
            let base = baselines(&table);
            assert_eq!(base.len(), 1, "{table:?}");
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
            // record.
            run_once(&st, port, db, &[&format!("{db}.t")], SnapshotMode::Never)
                .await;
            let (again, barriers_again) = timeline(&st, db, "t").await;
            assert_eq!(again, table);
            assert_eq!(barriers_again, barriers);
        }

        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_first_start_baseline_binds_the_start_barrier_gtid() {
            fresh_start(true).await;
        }

        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_first_start_baseline_binds_the_start_barrier_file_position()
        {
            fresh_start(false).await;
        }

        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_ddl_or_barrier_in_the_interval_prevents_the_baseline() {
            let port = server(true).await;
            let db = "base_scan";
            sql(
                port,
                &[
                    format!("DROP DATABASE IF EXISTS {db}"),
                    format!("CREATE DATABASE {db}"),
                    format!("CREATE TABLE {db}.seed (id INT PRIMARY KEY)"),
                    format!("CREATE TABLE {db}.t (id INT PRIMARY KEY, v INT)"),
                    format!("CREATE TABLE {db}.u (id INT PRIMARY KEY, v INT)"),
                ],
            )
            .await;
            let st = state().await;
            // A committed position whose timeline knows nothing of t and u
            // (an upgraded deployment).
            run_once(
                &st,
                port,
                db,
                &[&format!("{db}.seed")],
                SnapshotMode::Never,
            )
            .await;
            // Move the committed position past the first start's barrier.
            sql(port, &["FLUSH PRIVILEGES".to_string()]).await;
            run_once(
                &st,
                port,
                db,
                &[&format!("{db}.seed")],
                SnapshotMode::Never,
            )
            .await;
            let r0 = stored_position(&st, db).await;
            // While stopped: a DDL on t, and an unrelated database dropped.
            sql(
                port,
                &[
                    format!("ALTER TABLE {db}.t ADD COLUMN w INT"),
                    format!("DROP DATABASE IF EXISTS {db}_other"),
                ],
            )
            .await;
            run_once(
                &st,
                port,
                db,
                &[&format!("{db}.t"), &format!("{db}.u")],
                SnapshotMode::Never,
            )
            .await;
            let (t, tb) = timeline(&st, db, "t").await;
            assert!(baselines(&t).is_empty(), "t changed in (R0, S]: {t:?}");
            assert_ne!(proven(&t, &tb, &r0), Selection::Proven { version: 1 });
            let (u, ub) = timeline(&st, db, "u").await;
            let base = baselines(&u);
            assert_eq!(base.len(), 1, "{u:?}");
            // A committed restart has no barrier at R0: nothing to bind.
            assert!(matches!(
                base[0].record.kind,
                Kind::Baseline { binds: None, .. }
            ));
            assert!(matches!(proven(&u, &ub, &r0), Selection::Proven { .. }));

            // A lineage barrier while stopped: no baseline for anything new.
            let r1 = stored_position(&st, db).await;
            sql(port, &[
                format!("CREATE TABLE {db}.v (id INT PRIMARY KEY)"),
                format!(
                    "CREATE TABLE {db}.w (id INT PRIMARY KEY) /*!50100 ENGINE=InnoDB */"
                ),
            ])
            .await;
            run_once(&st, port, db, &[&format!("{db}.v")], SnapshotMode::Never)
                .await;
            let (v, vb) = timeline(&st, db, "v").await;
            assert!(baselines(&v).is_empty(), "{v:?}");
            assert!(!matches!(proven(&v, &vb, &r1), Selection::Proven { .. }));
        }

        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_snapshot_baseline_binds_the_anchor_barrier() {
            let port = server(true).await;
            let db = "base_snap";
            sql(
                port,
                &[
                    format!("DROP DATABASE IF EXISTS {db}"),
                    format!("CREATE DATABASE {db}"),
                    format!("CREATE TABLE {db}.t (id INT PRIMARY KEY, v INT)"),
                    format!("INSERT INTO {db}.t VALUES (1, 1)"),
                ],
            )
            .await;
            let st = state().await;
            run_once(
                &st,
                port,
                db,
                &[&format!("{db}.t")],
                SnapshotMode::Initial,
            )
            .await;
            let anchor = stored_position(&st, db).await;
            let (table, barriers) = timeline(&st, db, "t").await;
            let base = baselines(&table);
            assert_eq!(base.len(), 1, "{table:?}");
            assert_eq!(
                order_positions(&base[0].record.position, &anchor),
                CheckpointOrder::Equal
            );
            assert!(matches!(
                proven(&table, &barriers, &anchor),
                Selection::Proven { .. }
            ));
        }
    }
}
