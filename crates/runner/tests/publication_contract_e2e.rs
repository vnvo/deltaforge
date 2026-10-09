//! The immutable-publication contract through real pipelines
//! (PipelineManager, PostgreSQL 17 in a container, an HTTP sink recording
//! the delivered rows, the recovery service):
//!
//! - offline changes are refused by the database and the restart continues;
//! - drops of published tables are refused while running, transactionally;
//! - tampering is detected at startup and on a reconnect;
//! - a snapshot start needs a registration;
//! - retained WAL from before the registration is refused;
//! - the maintenance procedure, abandoning or re-snapshotting.
//!
//! Run with:
//! ```bash
//! cargo test -p runner --test publication_contract_e2e -- --include-ignored --test-threads=1
//! ```

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use gate_ownership::GateOwned;
use rest_api::PipelineController;
use runner::pipeline_manager::PipelineManager;
use serde_json::Value;
use sources::postgres::postgres_publication::{self as contract, fixtures};
use storage::{ArcStorageBackend, MemoryStorageBackend};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt, core::WaitFor, runners::AsyncRunner,
};
use tokio::time::sleep;
use tokio_postgres::NoTls;

const PASS: &str = "pg";

/// An HTTP sink recording the `id` of every row it accepted.
struct Hook {
    url: String,
    accepted: Arc<Mutex<BTreeSet<i64>>>,
}

impl Hook {
    async fn start() -> Self {
        let accepted = Arc::new(Mutex::new(BTreeSet::new()));
        let acc = accepted.clone();
        let app = axum::Router::new().route(
            "/",
            axum::routing::post(move |body: axum::body::Bytes| {
                let acc = acc.clone();
                async move {
                    let v = serde_json::from_slice::<Value>(&body)
                        .unwrap_or_default();
                    if let Some(id) = v["after"]["id"].as_i64() {
                        acc.lock().unwrap().insert(id);
                    }
                    axum::http::StatusCode::OK
                }
            }),
        );
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", l.local_addr().unwrap());
        tokio::spawn(async move {
            axum::serve(l, app).await.ok();
        });
        Self { url, accepted }
    }

    fn accepted(&self) -> BTreeSet<i64> {
        self.accepted.lock().unwrap().clone()
    }

    async fn until(&self, id: i64) {
        let deadline = Instant::now() + Duration::from_secs(60);
        while !self.accepted().contains(&id) {
            assert!(Instant::now() < deadline, "row {id} never delivered");
            sleep(Duration::from_millis(200)).await;
        }
    }
}

/// Log to the test output (`RUST_LOG`, default info).
fn trace() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .with_test_writer()
        .try_init();
}

async fn start_postgres() -> (ContainerAsync<GenericImage>, u16) {
    trace();
    let c = GenericImage::new("postgres", "17")
        .with_wait_for(WaitFor::message_on_stderr("database system is ready"))
        .with_env_var("POSTGRES_PASSWORD", PASS)
        .with_cmd(["postgres", "-c", "wal_level=logical"])
        .gate_owned()
        .start()
        .await
        .expect("start postgres");
    let port = c.get_host_port_ipv4(5432).await.expect("pg port");
    sleep(Duration::from_secs(3)).await;
    let pg = admin(port).await;
    pg.batch_execute(
        "CREATE TABLE orders (id INT PRIMARY KEY, sku TEXT);
         CREATE SCHEMA s; CREATE TABLE s.items (id INT PRIMARY KEY);
         CREATE TABLE other (id INT PRIMARY KEY);
         INSERT INTO orders VALUES (1, 'snap');
         CREATE ROLE app_owner;
         ALTER TABLE orders OWNER TO app_owner;
         ALTER TABLE s.items OWNER TO app_owner;
         CREATE PUBLICATION pc_pub FOR TABLE orders, s.items;",
    )
    .await
    .unwrap();
    (c, port)
}

async fn admin(port: u16) -> tokio_postgres::Client {
    let (pg, conn) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={port} user=postgres password={PASS} dbname=postgres"
        ),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        conn.await.ok();
    });
    pg
}

async fn register(port: u16) {
    contract::register(&admin(port).await, &["pc_pub".to_string()])
        .await
        .unwrap();
}

fn spec(
    name: &str,
    port: u16,
    url: &str,
    snapshot: &str,
) -> deltaforge_config::PipelineSpec {
    let yaml = format!(
        r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata:
  name: {name}
  tenant: acme
spec:
  source:
    type: postgres
    config:
      id: pc-src
      dsn: "host=127.0.0.1 port={port} user=postgres password={PASS} dbname=postgres"
      slot: pc_slot
      publication: pc_pub
      tables: [public.orders]
      snapshot:
        mode: {snapshot}
  processors: []
  sinks:
    - type: http
      config:
        id: hook
        url: "{url}"
        required: true
        send_timeout_secs: 2
        batch_timeout_secs: 2
        connect_timeout_secs: 2
  batch:
    max_events: 10
    max_ms: 50
    respect_source_tx: false
"#
    );
    serde_yaml::from_str(&yaml).expect("pipeline spec")
}

async fn manager() -> (Arc<PipelineManager>, ArcStorageBackend) {
    let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
    let mgr = PipelineManager::with_backend(Arc::clone(&backend))
        .await
        .unwrap();
    (Arc::new(mgr), backend)
}

async fn status(mgr: &PipelineManager, name: &str) -> String {
    PipelineController::get(mgr, name).await.unwrap().status
}

async fn until_status(mgr: &PipelineManager, name: &str, want: &str) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while status(mgr, name).await != want {
        assert!(
            Instant::now() < deadline,
            "{name}: never {want} (is {})",
            status(mgr, name).await
        );
        sleep(Duration::from_millis(200)).await;
    }
}

/// Until the source streams from its slot (a pipeline reports running
/// before its source has opened the stream).
async fn until_streaming(port: u16) {
    let pg = admin(port).await;
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let active: bool = pg
            .query_one(
                "SELECT coalesce(bool_or(active), false) FROM pg_replication_slots \
                 WHERE slot_name = 'pc_slot'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        if active {
            return;
        }
        assert!(Instant::now() < deadline, "the source never streamed");
        sleep(Duration::from_millis(200)).await;
    }
}

async fn stop(mgr: &PipelineManager, name: &str) {
    PipelineController::stop(mgr, name).await.unwrap();
    until_status(mgr, name, "stopped").await;
    // The stopped tasks finish releasing the slot.
    sleep(Duration::from_secs(1)).await;
}

/// The incidents' `(reason, class)`.
async fn incidents(
    backend: &ArcStorageBackend,
    pipeline: &str,
) -> Vec<(String, String)> {
    storage::adapters::incidents::IncidentStore::new(backend.clone(), pipeline)
        .list()
        .await
        .unwrap()
        .into_iter()
        .map(|r| {
            let v = serde_json::to_value(&r).unwrap();
            (
                v["reason_code"].as_str().unwrap_or_default().to_string(),
                v["evidence"]["reason_class"].to_string(),
            )
        })
        .collect()
}

/// The pipeline failed with `reason` (and, if given, the class).
async fn failed_with(
    mgr: &PipelineManager,
    backend: &ArcStorageBackend,
    name: &str,
    reason: &str,
    class: Option<&str>,
) -> String {
    until_status(mgr, name, "failed").await;
    let all = incidents(backend, name).await;
    let hit = all
        .iter()
        .find(|(r, c)| r == reason && class.is_none_or(|k| c.contains(k)));
    assert!(hit.is_some(), "{name}: no {reason} {class:?} in {all:?}");
    let svc = recovery(mgr);
    use rest_api::recovery::RecoveryController;
    let diag = svc.diagnose(name).await.unwrap();
    diag["incidents"]
        .as_array()
        .unwrap()
        .iter()
        .find(|i| i["reason_code"] == reason)
        .map(|i| i["incident_id"].as_str().unwrap().to_string())
        .unwrap_or_default()
}

fn refused(r: Result<(), tokio_postgres::Error>, what: &str) {
    let e = r.expect_err(what);
    assert!(
        e.as_db_error()
            .is_some_and(|d| d.message().starts_with("deltaforge:")),
        "{what}: {e:?}"
    );
}

fn recovery(mgr: &PipelineManager) -> runner::recovery::RecoveryService {
    // The service needs an owned manager handle.
    runner::recovery::RecoveryService::new(Arc::new(mgr.clone()))
        .with_operation(Arc::new(runner::recovery_resnapshot::Resnapshot))
        .with_operation(Arc::new(
            runner::recovery_publication::PublicationMaintenance,
        ))
}

async fn apply(
    mgr: &PipelineManager,
    name: &str,
    operation: &str,
    incident: Option<String>,
    args: &[(&str, &str)],
) -> Value {
    use rest_api::recovery::{
        ApplyRequest, Caller, PlanRequest, RecoveryController,
    };
    let svc = recovery(mgr);
    let req = || PlanRequest {
        operation: operation.into(),
        incident: incident.clone(),
        args: args
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    };
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let planned = match svc.plan(name, req()).await {
            Ok(p) => p,
            Err(e)
                if e.code == "pipeline_not_quiescent"
                    && Instant::now() < deadline =>
            {
                sleep(Duration::from_millis(200)).await;
                continue;
            }
            Err(e) => panic!("plan {operation}: {e:?}"),
        };
        match svc
            .apply(
                name,
                ApplyRequest {
                    plan: req(),
                    expect_proof: planned["proof"].as_str().unwrap().into(),
                    actor: "alice".into(),
                    reason: "publication maintenance".into(),
                },
                Caller {
                    origin: "127.0.0.1:1".into(),
                    credential: "token",
                },
            )
            .await
        {
            Ok(v) => return v,
            Err(e)
                if e.code == "pipeline_not_quiescent"
                    && Instant::now() < deadline =>
            {
                sleep(Duration::from_millis(200)).await;
            }
            Err(e) => panic!("apply {operation}: {e:?}"),
        }
    }
}

/// With the pipeline stopped, the database refuses every publication change
/// and every drop of a published table; the restart continues and delivers
/// what was written meanwhile.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn offline_changes_are_refused_and_the_restart_continues() -> Result<()> {
    let (_pg, port) = start_postgres().await;
    register(port).await;
    let hook = Hook::start().await;
    let (mgr, _backend) = manager().await;
    mgr.start_pipeline(spec("pcoff", port, &hook.url, "never"))
        .await?;
    until_status(&mgr, "pcoff", "running").await;
    until_streaming(port).await;
    let pg = admin(port).await;
    pg.execute("INSERT INTO orders VALUES (2, 'a')", &[])
        .await?;
    hook.until(2).await;
    stop(&mgr, "pcoff").await;
    for sql in [
        "ALTER PUBLICATION pc_pub ADD TABLE other",
        "ALTER PUBLICATION pc_pub DROP TABLE s.items",
        "ALTER PUBLICATION pc_pub SET (publish = 'insert')",
        "ALTER PUBLICATION pc_pub RENAME TO renamed",
        "ALTER PUBLICATION pc_pub OWNER TO postgres",
        "DROP PUBLICATION pc_pub",
        "DROP TABLE orders",
        "DROP SCHEMA s CASCADE",
        "DROP OWNED BY app_owner",
    ] {
        refused(pg.batch_execute(sql).await, sql);
    }
    pg.execute("INSERT INTO orders VALUES (3, 'b')", &[])
        .await?;
    mgr.resume("pcoff").await?;
    until_streaming(port).await;
    hook.until(3).await;
    assert_eq!(status(&mgr, "pcoff").await, "running");
    Ok(())
}

/// While running: drops are refused inside the transaction; a refused drop
/// in a savepoint is rolled back with it and the rest of the transaction is
/// delivered.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn drops_are_refused_while_running() -> Result<()> {
    let (_pg, port) = start_postgres().await;
    register(port).await;
    let hook = Hook::start().await;
    let (mgr, _backend) = manager().await;
    mgr.start_pipeline(spec("pcrun", port, &hook.url, "never"))
        .await?;
    until_status(&mgr, "pcrun", "running").await;
    until_streaming(port).await;
    let pg = admin(port).await;
    for sql in [
        "DROP TABLE orders",
        "DROP TABLE s.items, other",
        "DROP SCHEMA s CASCADE",
        "DROP OWNED BY app_owner",
    ] {
        refused(pg.batch_execute(sql).await, sql);
    }
    pg.batch_execute("BEGIN; INSERT INTO orders VALUES (10, 'x'); SAVEPOINT p")
        .await?;
    refused(pg.batch_execute("DROP TABLE orders").await, "in savepoint");
    pg.batch_execute(
        "ROLLBACK TO SAVEPOINT p; INSERT INTO orders VALUES (11, 'y'); COMMIT",
    )
    .await?;
    hook.until(11).await;
    assert!(hook.accepted().contains(&10));
    // An unrelated table can still be dropped.
    pg.batch_execute("DROP TABLE other").await?;
    pg.execute("INSERT INTO orders VALUES (12, 'z')", &[])
        .await?;
    hook.until(12).await;
    assert_eq!(status(&mgr, "pcrun").await, "running");
    Ok(())
}

/// Tampering is outside the guarantee but detected: a disabled guard at
/// startup, and a superuser change made with the guard off while running,
/// at the next reconnect.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn tampering_is_detected_at_startup_and_reconnect() -> Result<()> {
    let (_pg, port) = start_postgres().await;
    register(port).await;
    let hook = Hook::start().await;
    let (mgr, backend) = manager().await;
    let pg = admin(port).await;
    // Startup: the drop guard disabled.
    pg.batch_execute(&format!(
        "ALTER EVENT TRIGGER {} DISABLE",
        contract::DROP_GUARD
    ))
    .await?;
    mgr.start_pipeline(spec("pctamper", port, &hook.url, "never"))
        .await?;
    failed_with(
        &mgr,
        &backend,
        "pctamper",
        "pg_publication_enforcement",
        None,
    )
    .await;
    assert!(hook.accepted().is_empty());
    pg.batch_execute(&format!(
        "ALTER EVENT TRIGGER {} ENABLE ALWAYS",
        contract::DROP_GUARD
    ))
    .await?;
    mgr.resume("pctamper").await?;
    until_status(&mgr, "pctamper", "running").await;
    until_streaming(port).await;
    pg.execute("INSERT INTO orders VALUES (20, 'a')", &[])
        .await?;
    hook.until(20).await;
    // Running: a superuser changes the publication with the guard off,
    // then the stream reconnects.
    pg.batch_execute(&format!(
        "BEGIN; ALTER EVENT TRIGGER {g} DISABLE; \
         ALTER PUBLICATION pc_pub ADD TABLE other; \
         ALTER EVENT TRIGGER {g} ENABLE ALWAYS; COMMIT",
        g = contract::DDL_GUARD
    ))
    .await?;
    pg.execute(
        "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots \
         WHERE slot_name = 'pc_slot' AND active_pid IS NOT NULL",
        &[],
    )
    .await?;
    failed_with(
        &mgr,
        &backend,
        "pctamper",
        "pg_publication_changed",
        Some("changed"),
    )
    .await;
    Ok(())
}

/// A snapshot start (and any start) needs a registration: refused before
/// any row is read, and the registered start snapshots and streams.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn a_snapshot_start_needs_a_registration() -> Result<()> {
    let (_pg, port) = start_postgres().await;
    let hook = Hook::start().await;
    let (mgr, backend) = manager().await;
    mgr.start_pipeline(spec("pcsnap", port, &hook.url, "initial"))
        .await?;
    failed_with(
        &mgr,
        &backend,
        "pcsnap",
        "pg_publication_changed",
        Some("unregistered"),
    )
    .await;
    assert!(hook.accepted().is_empty(), "nothing read before the check");
    register(port).await;
    mgr.resume("pcsnap").await?;
    hook.until(1).await;
    admin(port)
        .await
        .execute("INSERT INTO orders VALUES (30, 'a')", &[])
        .await?;
    hook.until(30).await;
    Ok(())
}

/// A slot positioned before the registration holds WAL whose rows may have
/// been published under another publication state: refused.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn retained_wal_before_the_registration_is_refused() -> Result<()> {
    let (_pg, port) = start_postgres().await;
    let pg = admin(port).await;
    pg.batch_execute(
        "SELECT pg_create_logical_replication_slot('pc_slot', 'pgoutput');
         INSERT INTO orders VALUES (40, 'before');",
    )
    .await?;
    register(port).await;
    let hook = Hook::start().await;
    let (mgr, backend) = manager().await;
    mgr.start_pipeline(spec("pcwal", port, &hook.url, "never"))
        .await?;
    failed_with(
        &mgr,
        &backend,
        "pcwal",
        "pg_publication_changed",
        Some("retained_wal"),
    )
    .await;
    assert!(hook.accepted().is_empty());
    Ok(())
}

/// Maintenance, abandon: without a decision the re-registered publication
/// is refused; with one, the source starts at the new registration and the
/// backlog before it is never delivered.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn maintenance_with_abandonment() -> Result<()> {
    let (_pg, port) = start_postgres().await;
    register(port).await;
    let hook = Hook::start().await;
    let (mgr, backend) = manager().await;
    mgr.start_pipeline(spec("pcab", port, &hook.url, "never"))
        .await?;
    until_status(&mgr, "pcab", "running").await;
    until_streaming(port).await;
    let pg = admin(port).await;
    pg.execute("INSERT INTO orders VALUES (50, 'a')", &[])
        .await?;
    hook.until(50).await;
    stop(&mgr, "pcab").await;
    // The backlog, then the database-wide procedure.
    pg.execute("INSERT INTO orders VALUES (51, 'backlog')", &[])
        .await?;
    let e = fixtures::recreate_registered(&pg, "pc_pub", &["orders"]).await;
    assert!(e.is_ok(), "{e:?}");
    mgr.resume("pcab").await?;
    let incident = failed_with(
        &mgr,
        &backend,
        "pcab",
        "pg_publication_changed",
        Some("reregistered"),
    )
    .await;
    let applied = apply(
        &mgr,
        "pcab",
        "pg-publication-maintenance",
        Some(incident),
        &[("mode", "abandon")],
    )
    .await;
    assert_eq!(applied["state"], "completed", "{applied}");
    mgr.resume("pcab").await?;
    until_status(&mgr, "pcab", "running").await;
    until_streaming(port).await;
    pg.execute("INSERT INTO orders VALUES (52, 'after')", &[])
        .await?;
    hook.until(52).await;
    assert!(!hook.accepted().contains(&51), "the abandoned backlog");
    Ok(())
}

/// Maintenance, re-snapshot: the decision alone is not enough (refused until
/// a new generation starts); after the resnapshot operation every row,
/// the backlog included, is delivered.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn maintenance_with_resnapshot() -> Result<()> {
    let (_pg, port) = start_postgres().await;
    register(port).await;
    let hook = Hook::start().await;
    let (mgr, backend) = manager().await;
    mgr.start_pipeline(spec("pcre", port, &hook.url, "initial"))
        .await?;
    hook.until(1).await;
    let pg = admin(port).await;
    pg.execute("INSERT INTO orders VALUES (60, 'a')", &[])
        .await?;
    hook.until(60).await;
    stop(&mgr, "pcre").await;
    pg.execute("INSERT INTO orders VALUES (61, 'backlog')", &[])
        .await?;
    fixtures::recreate_registered(&pg, "pc_pub", &["orders"]).await?;
    mgr.resume("pcre").await?;
    let incident = failed_with(
        &mgr,
        &backend,
        "pcre",
        "pg_publication_changed",
        Some("reregistered"),
    )
    .await;
    apply(
        &mgr,
        "pcre",
        "pg-publication-maintenance",
        Some(incident),
        &[("mode", "resnapshot")],
    )
    .await;
    mgr.resume("pcre").await?;
    let pending = failed_with(
        &mgr,
        &backend,
        "pcre",
        "pg_publication_changed",
        Some("resnapshot_pending"),
    )
    .await;
    let _ = pending;
    let applied = apply(&mgr, "pcre", "resnapshot", None, &[]).await;
    assert_eq!(applied["state"], "completed", "{applied}");
    mgr.resume("pcre").await?;
    hook.until(61).await;
    pg.execute("INSERT INTO orders VALUES (62, 'after')", &[])
        .await?;
    hook.until(62).await;
    Ok(())
}
