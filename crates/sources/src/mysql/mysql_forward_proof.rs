//! Forward proof after a live DDL (design spec 7.15, binding rulings Rounds
//! 24, 25 and 30).
//!
//! When the stream reads a DDL that may change a tracked table, the table's
//! `ddl` record (3b-1) makes the shape after it pending. Before the DDL event
//! is emitted, and so before any later event is read, the source proves the
//! post-DDL shape:
//!
//! 1. a stable capture of the table's shape (position, shape, position equal
//!    on one verified connection) at S_t;
//! 2. D (the DDL's position) and S (the last capture's position, at or after
//!    every S_t) comparable in the verified lineage with D <= S;
//! 3. one complete scan of (D, S] in which nothing affects the table: no DDL
//!    attributed to it, no database barrier for its database, no lineage
//!    barrier;
//! 4. only then: the captured shape is registered through the normal
//!    registry path, and `Observed { binds: ddl_id, proof }` is appended at D
//!    with the exact capture identity of the pending `ddl` record.
//!
//! Any failure leaves the DDL pending (rows after it stay unproven); a
//! connection that reached another server stops the source. A DROP gets no
//! forward proof. Replay: a binding already present for the `ddl` record is
//! the durable evidence, so nothing is captured or appended again
//! (byte-identical). A crash after registration but before the binding
//! repeats the proof; registration deduplicates the shape.

use deltaforge_core::{CheckpointOrder, SourceError, SourceResult};
use tracing::{info, warn};

use super::mysql_activation::{self, ForwardProof, resolve_binding};
use super::mysql_baseline::affects;
use super::mysql_binlog_scan::{
    CLASSIFIER_VERSION, ProofError, ScanLimits, scan_interval, stable_capture,
};
use super::{MySqlCheckpoint, RunCtx};
use crate::durable_checkpoint::{mysql_checkpoint_position, order_positions};

/// Stable-capture attempts per table before the DDL stays pending.
const CAPTURE_ATTEMPTS: u32 = 5;

/// A tracked table whose shape a DDL left pending.
pub(crate) struct Pending {
    pub db: String,
    pub table: String,
    /// The exact capture identity of the table's pending `ddl` record.
    pub ddl_id: String,
}

fn other_server(expected: &str, found: &str) -> SourceError {
    SourceError::Lineage {
        details: format!(
            "a forward-proof connection reached another server ({found}), \
             expected {expected}"
        )
        .into(),
    }
}

/// Prove the post-DDL shape of every `pending` table at the DDL's position
/// (the stream's current position). Returns how many bindings were written.
pub(crate) async fn prove(
    ctx: &RunCtx,
    pending: Vec<Pending>,
) -> SourceResult<usize> {
    if pending.is_empty() {
        return Ok(0);
    }
    let scope = ctx.registry_scope.current()?;
    let lineage = scope.lineage().lineage_hash.clone();
    let server_uuid = ctx.expected_uuid()?;
    let d_cp = MySqlCheckpoint {
        file: ctx.last_file.clone(),
        pos: ctx.last_pos,
        gtid_set: ctx.last_gtid.clone(),
        lineage: Some(lineage.clone()),
    };
    let Some(d) = mysql_checkpoint_position(
        &d_cp.file,
        d_cp.pos,
        d_cp.gtid_set.as_deref(),
    ) else {
        warn!(source_id = %ctx.source_id, "unparseable DDL position: the DDL stays pending");
        return Ok(0);
    };
    let storage = |e: anyhow::Error| {
        SourceError::Other(e.context("forward proof: activation timeline"))
    };

    // Replay: a binding already recorded for the pending DDL is the proof.
    let mut open = Vec::new();
    for p in pending {
        let key = scope.key(&p.db, &p.table);
        let records = mysql_activation::read_table(
            &ctx.registry_backend,
            ctx.schema.registry(),
            &key,
        )
        .await
        .map_err(storage)?;
        // A binding is the proof only once validated as complete,
        // consistent evidence; an invalid one stops here (never a fresh
        // proof over later server state).
        match resolve_binding(
            ctx.schema.registry(),
            &key,
            &records,
            &p.ddl_id,
            CLASSIFIER_VERSION,
        )
        .await
        {
            Ok(Some(_)) => {}
            Ok(None) => open.push(p),
            Err(e) => {
                return Err(SourceError::Schema {
                    details: format!("{e:#}").into(),
                });
            }
        }
    }
    if open.is_empty() {
        return Ok(0);
    }

    // 1. Stable captures.
    let mut captured = Vec::new();
    for p in open {
        match stable_capture(
            ctx.dsn.expose(),
            &server_uuid,
            &lineage,
            &p.db,
            &p.table,
            CAPTURE_ATTEMPTS,
            None,
        )
        .await
        {
            Ok(cap) => captured.push((p, cap)),
            Err(ProofError::OtherServer(found)) => {
                return Err(other_server(&server_uuid, &found));
            }
            Err(e) => {
                warn!(source_id = %ctx.source_id, db = %p.db, table = %p.table, error = ?e, "forward proof: no stable capture; the DDL stays pending");
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

    // 2. D <= S in the verified lineage.
    if !matches!(
        order_positions(&d, &s),
        CheckpointOrder::Before | CheckpointOrder::Equal
    ) {
        warn!(source_id = %ctx.source_id, "forward proof: the capture is not at or after the DDL; the DDL stays pending");
        return Ok(0);
    }

    // 3. One complete scan of (D, S].
    let report = match scan_interval(
        ctx.dsn.expose(),
        super::mysql_helpers::derive_server_id(&format!(
            "{}/forward",
            ctx.source_id
        )),
        &server_uuid,
        &lineage,
        &d_cp,
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
            warn!(source_id = %ctx.source_id, error = ?e, "forward proof: the interval scan failed; the DDL stays pending");
            return Ok(0);
        }
    };

    // 4. Register, then bind.
    let checkpoint =
        serde_json::to_vec(&d_cp).map_err(|e| SourceError::Other(e.into()))?;
    let mut written = 0;
    for (p, cap) in captured {
        let at_or_before_s = matches!(
            mysql_checkpoint_position(
                &cap.position.file,
                cap.position.pos,
                cap.position.gtid_set.as_deref()
            )
            .map(|c| order_positions(&c, &s)),
            Some(CheckpointOrder::Before | CheckpointOrder::Equal)
        );
        if !at_or_before_s {
            warn!(source_id = %ctx.source_id, db = %p.db, table = %p.table, "forward proof: capture not before the scan end; the DDL stays pending");
            continue;
        }
        if report
            .statements
            .iter()
            .any(|st| affects(st, &p.db, &p.table, ctx.lower_case_table_names))
        {
            info!(source_id = %ctx.source_id, db = %p.db, table = %p.table, "forward proof: a DDL or barrier in (D, S]; the DDL stays pending");
            continue;
        }
        let (version, schema_hash) = ctx
            .schema
            .register_captured(&p.db, &p.table, &cap.schema, &checkpoint)
            .await?;
        let key = scope.key(&p.db, &p.table);
        mysql_activation::record_forward_proof(
            &ctx.registry_backend,
            ctx.schema.registry(),
            &key,
            &p.ddl_id,
            d.clone(),
            version,
            ForwardProof {
                classifier_version: CLASSIFIER_VERSION.to_string(),
                to: s.clone(),
                scan_digest: report.digest.clone(),
                schema_hash,
            },
        )
        .await
        .map_err(|e| {
            SourceError::Other(e.context("persist a forward-proof binding"))
        })?;
        written += 1;
    }
    info!(source_id = %ctx.source_id, bindings = written, "forward proof after DDL");
    Ok(written)
}

#[cfg(test)]
mod tests {
    /// Live MySQL 8.4 (Docker).
    mod live {
        use super::super::*;
        use crate::mysql::MySqlSource;
        use crate::mysql::mysql_activation::{
            Kind, Selection, Stored, read_barriers, read_table, select,
        };
        use checkpoints::{CheckpointStore, MemCheckpointStore};
        use deltaforge_config::{OnSchemaDrift, SnapshotCfg, SnapshotMode};
        use deltaforge_core::{Source, SourceHandle, SourceItem};
        use gate_ownership::GateOwned;
        use mysql_async::prelude::Queryable;
        use std::sync::Arc;
        use std::time::{Duration, Instant};
        use storage::adapters::test_util::FaultBackend;
        use storage::{
            ArcStorageBackend, DurableSchemaRegistry, MemoryStorageBackend,
        };
        use testcontainers::core::WaitFor;
        use testcontainers::runners::AsyncRunner;
        use testcontainers::{ContainerAsync, GenericImage, ImageExt};
        use tokio::sync::{OnceCell, mpsc};

        static GTID: OnceCell<(ContainerAsync<GenericImage>, u16)> =
            OnceCell::const_new();
        static FILEPOS: OnceCell<(ContainerAsync<GenericImage>, u16)> =
            OnceCell::const_new();

        async fn start(gtid: bool) -> (ContainerAsync<GenericImage>, u16) {
            let mut cmd = vec![
                "--server-id=61",
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
                if mysql_async::Conn::from_url(dsn(port)).await.is_ok() {
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

        fn dsn(port: u16) -> String {
            format!("mysql://root:pw@127.0.0.1:{port}/")
        }

        async fn sql(port: u16, stmts: &[&str]) {
            let mut c = mysql_async::Conn::from_url(dsn(port)).await.unwrap();
            for s in stmts {
                c.query_drop(*s)
                    .await
                    .unwrap_or_else(|e| panic!("{s}: {e}"));
            }
            c.disconnect().await.ok();
        }

        struct State {
            backend: ArcStorageBackend,
            registry: Arc<DurableSchemaRegistry>,
            ckpt: Arc<dyn CheckpointStore>,
            scope: crate::registry_scope::SharedRegistryScope,
        }

        async fn state(backend: ArcStorageBackend) -> State {
            State {
                registry: DurableSchemaRegistry::new(backend.clone())
                    .await
                    .unwrap(),
                ckpt: Arc::new(MemCheckpointStore::new().unwrap()),
                scope: crate::registry_scope::SharedRegistryScope::default(),
                backend,
            }
        }

        struct Run {
            handle: SourceHandle,
            rx: mpsc::Receiver<SourceItem>,
        }

        async fn run(st: &State, port: u16, id: &str, tables: &[&str]) -> Run {
            let src = MySqlSource {
                id: id.into(),
                dsn: dsn(port).as_str().into(),
                tables: tables.iter().map(|t| t.to_string()).collect(),
                tenant: "acme".into(),
                pipeline: "test".into(),
                registry: st.registry.clone(),
                registry_scope: st.scope.clone(),
                backend: st.backend.clone(),
                outbox_tables: common::AllowList::default(),
                snapshot_cfg: SnapshotCfg {
                    mode: SnapshotMode::Never,
                    ..Default::default()
                },
                on_schema_drift: OnSchemaDrift::Adapt,
                table_options: Default::default(),
                rotation: None,
            };
            let (tx, rx) = mpsc::channel(4096);
            let handle = src.run(tx, st.ckpt.clone()).await;
            tokio::time::sleep(Duration::from_secs(3)).await;
            Run { handle, rx }
        }

        async fn stop(handle: SourceHandle) {
            handle.stop();
            tokio::time::timeout(Duration::from_secs(30), handle.join)
                .await
                .ok();
        }

        /// Wait for the DDL event containing `needle`.
        async fn ddl_event(rx: &mut mpsc::Receiver<SourceItem>, needle: &str) {
            let deadline = Instant::now() + Duration::from_secs(60);
            while let Some(left) =
                deadline.checked_duration_since(Instant::now())
            {
                match tokio::time::timeout(left, rx.recv()).await {
                    Ok(Some(SourceItem::Event(e)))
                        if e.ddl.as_ref().is_some_and(|d| {
                            d.to_string().contains(needle)
                        }) =>
                    {
                        return;
                    }
                    Ok(Some(_)) => {}
                    _ => break,
                }
            }
            panic!("no DDL event containing {needle:?}");
        }

        async fn records(st: &State, db: &str, table: &str) -> Vec<Stored> {
            let key = st.scope.current().unwrap().key(db, table);
            read_table(&st.backend, &st.registry, &key).await.unwrap()
        }

        async fn barriers(st: &State, db: &str, table: &str) -> Vec<Stored> {
            let key = st.scope.current().unwrap().key(db, table);
            read_barriers(&st.backend, &key).await.unwrap()
        }

        fn ddls(recs: &[Stored]) -> Vec<&Stored> {
            recs.iter().filter(|s| s.record.kind == Kind::Ddl).collect()
        }

        /// The validated forward binding of `ddl` (panics on invalid
        /// evidence).
        async fn bound(
            st: &State,
            db: &str,
            table: &str,
            recs: &[Stored],
            ddl: &Stored,
        ) -> Option<i32> {
            let key = st.scope.current().unwrap().key(db, table);
            resolve_binding(
                &st.registry,
                &key,
                recs,
                &ddl.capture_id,
                CLASSIFIER_VERSION,
            )
            .await
            .expect("valid binding evidence")
        }

        async fn prepare(port: u16, db: &str) {
            sql(
                port,
                &[
                    &format!("DROP DATABASE IF EXISTS {db}"),
                    &format!("CREATE DATABASE {db}"),
                    &format!("CREATE TABLE {db}.t (id INT PRIMARY KEY, v INT)"),
                    &format!("CREATE TABLE {db}.other (id INT PRIMARY KEY)"),
                ],
            )
            .await;
        }

        async fn a_live_ddl_is_bound(gtid: bool) {
            let port = server(gtid).await;
            let db = if gtid { "fwd_g" } else { "fwd_f" };
            prepare(port, db).await;
            let st = state(Arc::new(MemoryStorageBackend::new())).await;
            // A committed position before the DDL (for the replay below).
            let r = run(&st, port, db, &[&format!("{db}.t")]).await;
            stop(r.handle).await;
            let mut r = run(&st, port, db, &[&format!("{db}.t")]).await;
            sql(port, &[&format!("ALTER TABLE {db}.t ADD COLUMN w INT")]).await;
            ddl_event(&mut r.rx, "ADD COLUMN w").await;
            // Durable before the DDL event: the pending DDL and its binding.
            let recs = records(&st, db, "t").await;
            let d = ddls(&recs);
            assert_eq!(d.len(), 1, "{recs:?}");
            let version = bound(&st, db, "t", &recs, d[0])
                .await
                .expect("the DDL is bound");
            let key = st.scope.current().unwrap().key(db, "t");
            let shape = st.registry.version_hash(&key, version).await.unwrap();
            assert!(shape.is_some());
            let all: Vec<Stored> = recs
                .iter()
                .cloned()
                .chain(barriers(&st, db, "t").await)
                .collect();
            assert_eq!(
                select(&all, &d[0].record.position),
                Selection::Proven { version }
            );
            let history =
                crate::mysql::mysql_activation::tests_support::history(
                    &st.registry,
                    &key,
                )
                .await;
            assert!(
                history.last().unwrap().contains("\"w\""),
                "the bound version has the new column"
            );

            // A crash after the binding (no checkpoint past the DDL): the
            // replay re-derives nothing new.
            let before = records(&st, db, "t").await;
            r.handle.join.abort();
            // The server moves on before the restart: a re-proof would capture
            // a different S (another record); the existing binding is reused.
            sql(port, &[&format!("INSERT INTO {db}.other VALUES (1)")]).await;
            let mut r = run(&st, port, db, &[&format!("{db}.t")]).await;
            ddl_event(&mut r.rx, "ADD COLUMN w").await;
            assert_eq!(records(&st, db, "t").await, before);
            stop(r.handle).await;
        }

        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_live_ddl_is_bound_by_its_forward_proof_gtid() {
            a_live_ddl_is_bound(true).await;
        }

        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_live_ddl_is_bound_by_its_forward_proof_file_position() {
            a_live_ddl_is_bound(false).await;
        }

        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_crash_between_registration_and_binding_finishes_the_binding()
        {
            let port = server(true).await;
            let db = "fwd_crash";
            prepare(port, db).await;
            let fault = Arc::new(FaultBackend::new());
            let st = state(fault.clone()).await;
            let r = run(&st, port, db, &[&format!("{db}.t")]).await;
            stop(r.handle).await;
            let key_versions = |st: &State| {
                let key = st.scope.current().unwrap().key(db, "t");
                let reg = st.registry.clone();
                async move {
                    crate::mysql::mysql_activation::tests_support::history(
                        &reg, &key,
                    )
                    .await
                    .len()
                }
            };
            let r = run(&st, port, db, &[&format!("{db}.t")]).await;
            // The DDL's activation writes: its `ddl` record, then the binding.
            *fault.fail_after_writes_to.lock().unwrap() =
                Some((crate::mysql::mysql_activation::ACTIVATION_NS.into(), 1));
            sql(port, &[&format!("ALTER TABLE {db}.t ADD COLUMN z INT")]).await;
            let res =
                tokio::time::timeout(Duration::from_secs(60), r.handle.join)
                    .await
                    .expect("stops")
                    .expect("task");
            assert!(res.is_err(), "the failed binding stops the source");
            let recs = records(&st, db, "t").await;
            let d = ddls(&recs);
            assert_eq!(d.len(), 1);
            assert_eq!(
                bound(&st, db, "t", &recs, d[0]).await,
                None,
                "registered but not bound"
            );
            let registered = key_versions(&st).await;

            // Restart: the DDL replays, the proof repeats, registration
            // deduplicates, the same DDL gets its binding.
            let mut r = run(&st, port, db, &[&format!("{db}.t")]).await;
            ddl_event(&mut r.rx, "ADD COLUMN z").await;
            let recs = records(&st, db, "t").await;
            let d = ddls(&recs);
            assert_eq!(d.len(), 1);
            assert!(bound(&st, db, "t", &recs, d[0]).await.is_some());
            assert_eq!(key_versions(&st).await, registered, "no new version");
            stop(r.handle).await;
        }

        #[tokio::test]
        #[ignore = "requires docker"]
        async fn only_a_clean_interval_proves_the_post_ddl_shape() {
            let port = server(true).await;
            let db = "fwd_scan";
            prepare(port, db).await;
            let st = state(Arc::new(MemoryStorageBackend::new())).await;
            let tables =
                [format!("{db}.t"), format!("{db}.x"), format!("{db}.u")];
            let tables: Vec<&str> = tables.iter().map(String::as_str).collect();
            let r = run(&st, port, db, &tables).await;
            stop(r.handle).await;

            // While stopped (the restart replays them, so each proof's
            // interval holds the later statements):
            sql(
                port,
                &[
                    // a second DDL on t inside the first one's (D, S];
                    &format!("ALTER TABLE {db}.t ADD COLUMN a INT"),
                    &format!("ALTER TABLE {db}.t ADD COLUMN b INT"),
                    // an unrelated table's DDL does not block other's proof;
                    &format!("ALTER TABLE {db}.other ADD COLUMN c INT"),
                    &format!("CREATE TABLE {db}.u (id INT PRIMARY KEY)"),
                    // a DROP gets no proof; the recreate is its own DDL;
                    &format!("DROP TABLE {db}.other"),
                    &format!(
                        "CREATE TABLE {db}.other (id INT PRIMARY KEY, n INT)"
                    ),
                    // a rename binds its destination.
                    &format!("CREATE TABLE {db}.y (id INT PRIMARY KEY)"),
                    &format!("RENAME TABLE {db}.y TO {db}.x"),
                ],
            )
            .await;
            let tracked = [
                format!("{db}.t"),
                format!("{db}.x"),
                format!("{db}.u"),
                format!("{db}.other"),
            ];
            let tracked: Vec<&str> =
                tracked.iter().map(String::as_str).collect();
            let mut r = run(&st, port, db, &tracked).await;
            ddl_event(&mut r.rx, &format!("RENAME TABLE {db}.y TO {db}.x"))
                .await;

            let t = records(&st, db, "t").await;
            let td = ddls(&t);
            assert_eq!(td.len(), 2, "{t:?}");
            assert_eq!(
                bound(&st, db, "t", &t, td[0]).await,
                None,
                "a second DDL in (D, S]"
            );
            assert!(bound(&st, db, "t", &t, td[1]).await.is_some());

            let u = records(&st, db, "u").await;
            assert!(
                bound(&st, db, "u", &u, ddls(&u)[0]).await.is_some(),
                "unrelated DDLs do not block"
            );

            let o = records(&st, db, "other").await;
            let od = ddls(&o);
            assert_eq!(od.len(), 3, "{o:?}"); // ALTER, DROP, CREATE
            assert_eq!(
                bound(&st, db, "other", &o, od[1]).await,
                None,
                "a DROP gets no proof"
            );
            assert!(
                bound(&st, db, "other", &o, od[2]).await.is_some(),
                "the recreate is proven"
            );

            let x = records(&st, db, "x").await;
            assert!(
                bound(&st, db, "x", &x, ddls(&x)[0]).await.is_some(),
                "the rename destination"
            );
            stop(r.handle).await;

            // A lineage barrier in (D, S] leaves the DDL pending.
            sql(
                port,
                &[
                    &format!("ALTER TABLE {db}.u ADD COLUMN q INT"),
                    &format!(
                        "CREATE TABLE {db}.w (id INT) /*!50100 ENGINE=InnoDB */"
                    ),
                ],
            )
            .await;
            let mut r = run(&st, port, db, &tracked).await;
            ddl_event(&mut r.rx, "CREATE TABLE").await;
            let u = records(&st, db, "u").await;
            let ud = ddls(&u);
            assert_eq!(
                bound(&st, db, "u", &u, ud[ud.len() - 1]).await,
                None,
                "barrier in (D, S]"
            );
            stop(r.handle).await;
        }

        #[tokio::test]
        #[ignore = "requires docker"]
        async fn an_invalid_binding_stops_the_replay_without_a_new_proof() {
            let port = server(true).await;
            let db = "fwd_invalid";
            prepare(port, db).await;
            let st = state(Arc::new(MemoryStorageBackend::new())).await;
            let r = run(&st, port, db, &[&format!("{db}.t")]).await;
            stop(r.handle).await;
            let mut r = run(&st, port, db, &[&format!("{db}.t")]).await;
            sql(port, &[&format!("ALTER TABLE {db}.t ADD COLUMN k INT")]).await;
            ddl_event(&mut r.rx, "ADD COLUMN k").await;
            r.handle.join.abort();

            // A second record binding the same DDL (duplicate evidence).
            let key = st.scope.current().unwrap().key(db, "t");
            let stream = crate::mysql::mysql_activation::table_stream(&key);
            let recs = records(&st, db, "t").await;
            let binding = recs
                .iter()
                .find(|s| matches!(s.record.kind, Kind::Observed { .. }))
                .unwrap();
            st.backend
                .log_append_if_absent(
                    crate::mysql::mysql_activation::ACTIVATION_NS,
                    &stream,
                    "forged",
                    &serde_json::to_vec(&binding.record).unwrap(),
                )
                .await
                .unwrap();
            let tampered = records(&st, db, "t").await;

            // The replay stops on the invalid evidence: no new proof, no DDL
            // event.
            let mut r = run(&st, port, db, &[&format!("{db}.t")]).await;
            let res =
                tokio::time::timeout(Duration::from_secs(60), r.handle.join)
                    .await
                    .expect("stops")
                    .expect("task");
            assert!(
                matches!(&res, Err(SourceError::Schema { details }) if details.contains("2 bindings")),
                "{res:?}"
            );
            while let Ok(item) = r.rx.try_recv() {
                if let SourceItem::Event(e) = item {
                    assert!(e.ddl.is_none(), "the DDL event was emitted");
                }
            }
            assert_eq!(records(&st, db, "t").await, tampered);
        }
    }
}
