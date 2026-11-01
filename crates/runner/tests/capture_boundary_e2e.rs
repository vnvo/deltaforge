//! The start-up capture boundary (`docs/design/startup-capture-boundary.md`)
//! through real pipelines (PipelineManager, the source in a container, an
//! HTTP sink served by the test that records the rows it accepted):
//!
//! - a write after the pipeline reports `running` is delivered; until the
//!   boundary is fixed the pipeline reports `starting`, and an idle source
//!   becomes `running` without any data (MySQL, PostgreSQL);
//! - start-up retries keep `starting` (PostgreSQL, publication missing);
//! - a stop while starting stays `stopped`, even when the stopped run goes
//!   on to fix its boundary (a stale signal);
//! - a failure between the durable boundary and the stream opening resumes
//!   at that boundary, never a newer one (MySQL, PostgreSQL);
//! - a snapshot pipeline is `starting` until its generation completes;
//! - MySQL with GTID off; a MySQL user without the `log_status` privileges.
//!
//! The boundary seams (`sources::stream_probe`) are process-wide: one test
//! at a time. Run with:
//! ```bash
//! cargo test -p runner --test capture_boundary_e2e -- --include-ignored --test-threads=1
//! ```

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use deltaforge_core::{CaptureOrigin, CaptureStart};
use gate_ownership::GateOwned;
use mysql_async::prelude::Queryable;
use rest_api::PipelineController;
use runner::pipeline_manager::PipelineManager;
use serde_json::Value;
use storage::{ArcStorageBackend, MemoryStorageBackend};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt, core::WaitFor, runners::AsyncRunner,
};
use tokio::sync::Notify;
use tokio::time::sleep;
use tokio_postgres::NoTls;

const SNAPSHOT_ROWS: i64 = 20;
const PASS: &str = "pg";

// ---- sink -------------------------------------------------------------------

/// The test's HTTP sink: records the `id` of every row it accepted; while
/// closed it holds requests until opened.
struct Hook {
    url: String,
    open: Arc<AtomicBool>,
    opened: Arc<Notify>,
    accepted: Arc<Mutex<BTreeSet<i64>>>,
}

impl Hook {
    async fn start() -> Self {
        let open = Arc::new(AtomicBool::new(true));
        let opened = Arc::new(Notify::new());
        let accepted = Arc::new(Mutex::new(BTreeSet::new()));
        let (o, n, acc) = (open.clone(), opened.clone(), accepted.clone());
        let app = axum::Router::new().route(
            "/",
            axum::routing::post(move |body: axum::body::Bytes| {
                let (o, n, acc) = (o.clone(), n.clone(), acc.clone());
                async move {
                    while !o.load(Ordering::SeqCst) {
                        let wake = n.notified();
                        if o.load(Ordering::SeqCst) {
                            break;
                        }
                        wake.await;
                    }
                    let v = serde_json::from_slice::<Value>(&body)
                        .unwrap_or_default();
                    // A 64-bit id renders as an exact decimal string (v2).
                    let id = &v["after"]["id"];
                    if let Some(id) =
                        id.as_i64().or_else(|| id.as_str()?.parse().ok())
                    {
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
        Self {
            url,
            open,
            opened,
            accepted,
        }
    }

    fn set_open(&self, open: bool) {
        self.open.store(open, Ordering::SeqCst);
        self.opened.notify_waiters();
    }

    fn accepted(&self) -> BTreeSet<i64> {
        self.accepted.lock().unwrap().clone()
    }

    async fn until_accepted(&self, id: i64) {
        let deadline = Instant::now() + Duration::from_secs(120);
        while !self.accepted().contains(&id) {
            assert!(Instant::now() < deadline, "row {id} never delivered");
            sleep(Duration::from_millis(100)).await;
        }
    }
}

fn sink_yaml(url: &str) -> String {
    format!(
        r#"
  sinks:
    - type: http
      config:
        id: hook
        url: "{url}"
        required: true
        send_timeout_secs: 60
        batch_timeout_secs: 60
        connect_timeout_secs: 2
  batch:
    max_events: 10
    max_ms: 50
    respect_source_tx: false
"#
    )
}

// ---- pipeline -----------------------------------------------------------------

async fn manager() -> (PipelineManager, ArcStorageBackend) {
    let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
    let mgr = PipelineManager::with_backend(Arc::clone(&backend))
        .await
        .unwrap();
    (mgr, backend)
}

async fn info(mgr: &PipelineManager, name: &str) -> rest_api::PipeInfo {
    PipelineController::get(mgr, name).await.unwrap()
}

async fn until_status(mgr: &PipelineManager, name: &str, want: &str) {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let status = info(mgr, name).await.status;
        if status == want {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{name}: never {want} (last {status})"
        );
        sleep(Duration::from_millis(50)).await;
    }
}

/// The pipeline reports `want` throughout `d`.
async fn stays(mgr: &PipelineManager, name: &str, want: &str, d: Duration) {
    let until = Instant::now() + d;
    while Instant::now() < until {
        assert_eq!(info(mgr, name).await.status, want, "{name}");
        sleep(Duration::from_millis(50)).await;
    }
}

/// The published boundary of a running pipeline.
async fn capture_start(mgr: &PipelineManager, name: &str) -> CaptureStart {
    let i = info(mgr, name).await;
    assert_eq!(i.status, "running", "{name}");
    i.capture_start.expect("a running pipeline has a boundary")
}

/// The observable-readiness invariant, across a start held just before the
/// boundary: whenever the API reports `running`, a row written then is
/// delivered. `insert(id)` writes a row. While held, a pipeline that
/// (wrongly) reports `running` gets a row that must be delivered; one that
/// reports `starting` gets none. Returns the id written after `running`.
async fn a_write_after_running_is_delivered<F, Fut>(
    mgr: &PipelineManager,
    name: &str,
    hook: &Hook,
    reached: Arc<Notify>,
    release: Arc<Notify>,
    insert: F,
) -> i64
where
    F: Fn(i64) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    tokio::time::timeout(Duration::from_secs(120), reached.notified())
        .await
        .expect("the start reaches its boundary");
    let mut while_held = None;
    let until = Instant::now() + Duration::from_secs(2);
    while Instant::now() < until {
        if while_held.is_none() && info(mgr, name).await.status == "running" {
            insert(1_000).await;
            while_held = Some(1_000);
        }
        sleep(Duration::from_millis(50)).await;
    }
    release.notify_one();
    until_status(mgr, name, "running").await;
    insert(2_000).await;
    hook.until_accepted(2_000).await;
    if let Some(id) = while_held {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !hook.accepted().contains(&id) {
            assert!(
                Instant::now() < deadline,
                "{name}: row {id}, written while the pipeline reported \
                 running, was never delivered"
            );
            sleep(Duration::from_millis(100)).await;
        }
    }
    assert_eq!(
        while_held, None,
        "{name}: reported running before its boundary was fixed"
    );
    2_000
}

// ---- MySQL --------------------------------------------------------------------

async fn start_mysql(gtid: bool) -> (ContainerAsync<GenericImage>, u16) {
    let mut cmd = vec![
        "--server-id=999",
        "--log-bin=/var/lib/mysql/mysql-bin.log",
        "--binlog-format=ROW",
        "--binlog-row-image=FULL",
        "--binlog-checksum=NONE",
    ];
    if gtid {
        cmd.extend(["--gtid-mode=ON", "--enforce-gtid-consistency=ON"]);
    } else {
        cmd.push("--gtid-mode=OFF");
    }
    let c = GenericImage::new("mysql", "8.4")
        .with_wait_for(WaitFor::message_on_stderr(
            "ready for connections. Version: '8.4",
        ))
        .with_env_var("MYSQL_ROOT_PASSWORD", "rootpw")
        .with_cmd(cmd)
        .gate_owned()
        .start()
        .await
        .expect("start mysql");
    let port = c.get_host_port_ipv4(3306).await.expect("mysql port");
    sleep(Duration::from_secs(3)).await;
    mysql_sql(
        port,
        &[
            "CREATE DATABASE shop".to_string(),
            "CREATE TABLE shop.orders (id INT PRIMARY KEY, sku VARCHAR(64))"
                .to_string(),
            format!(
                "INSERT INTO shop.orders \
                 WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g \
                 WHERE n < {SNAPSHOT_ROWS}) SELECT n, CONCAT('sku-', n) FROM g"
            ),
        ],
    )
    .await;
    (c, port)
}

/// A CDC user, with or without the grants a first start reads its capture
/// boundary with.
async fn mysql_user(port: u16, user: &str, log_status: bool) {
    let mut sql = vec![
        format!("CREATE USER '{user}'@'%' IDENTIFIED BY 'pw'"),
        format!(
            "GRANT REPLICATION SLAVE, REPLICATION CLIENT, RELOAD ON *.* TO '{user}'@'%'"
        ),
        format!("GRANT SELECT ON shop.* TO '{user}'@'%'"),
    ];
    if log_status {
        sql.push(format!("GRANT BACKUP_ADMIN ON *.* TO '{user}'@'%'"));
        sql.push(format!(
            "GRANT SELECT ON performance_schema.log_status TO '{user}'@'%'"
        ));
    }
    mysql_sql(port, &sql).await;
    // Prime the user's caching_sha2_password cache (the binlog client does
    // not do the first, uncached authentication over plaintext).
    let mut c = mysql_conn(&format!("mysql://{user}:pw@127.0.0.1:{port}/shop"))
        .await
        .unwrap();
    c.query_drop("SELECT 1").await.unwrap();
}

async fn mysql_conn(dsn: &str) -> Result<mysql_async::Conn> {
    let opts = mysql_async::Opts::from_url(dsn)?;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match mysql_async::Conn::new(opts.clone()).await {
            Ok(c) => return Ok(c),
            Err(_) if Instant::now() < deadline => {
                sleep(Duration::from_millis(500)).await
            }
            Err(e) => return Err(e.into()),
        }
    }
}

async fn mysql_sql(port: u16, sql: &[String]) {
    let mut root =
        mysql_conn(&format!("mysql://root:rootpw@127.0.0.1:{port}/"))
            .await
            .unwrap();
    for s in sql {
        root.query_drop(s).await.unwrap();
    }
}

async fn mysql_insert(port: u16, id: i64) {
    mysql_sql(
        port,
        &[format!("INSERT INTO shop.orders VALUES ({id}, 'new')")],
    )
    .await;
}

fn mysql_spec(
    name: &str,
    port: u16,
    user: &str,
    snapshot: &str,
    url: &str,
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
    type: mysql
    config:
      id: my-src
      dsn: "mysql://{user}:pw@127.0.0.1:{port}/shop"
      tables: [shop.orders]
      snapshot:
        mode: {snapshot}
        chunk_size: 10
        max_parallel_tables: 1
  processors: []
{}"#,
        sink_yaml(url)
    );
    serde_yaml::from_str(&yaml).expect("pipeline spec")
}

/// A first start held just before its boundary reports `starting`; once
/// `running` (with no data yet: an idle source), a write is delivered. A
/// row committed before the boundary was read is not.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn mysql_a_write_after_running_is_delivered() -> Result<()> {
    let (_my, port) = start_mysql(true).await;
    mysql_user(port, "df", true).await;
    let hook = Hook::start().await;
    let (mgr, _) = manager().await;
    let (reached, release) =
        sources::stream_probe::hold_before_capture_boundary();
    mgr.start_pipeline(mysql_spec("mywrite", port, "df", "never", &hook.url))
        .await?;
    // Committed before the boundary is read: not captured.
    let early = {
        let reached = reached.clone();
        tokio::time::timeout(Duration::from_secs(120), reached.notified())
            .await
            .expect("held");
        reached.notify_one();
        mysql_insert(port, 500).await;
        500
    };
    let after = a_write_after_running_is_delivered(
        &mgr,
        "mywrite",
        &hook,
        reached,
        release,
        |id| mysql_insert(port, id),
    )
    .await;
    match capture_start(&mgr, "mywrite").await {
        CaptureStart::Mysql {
            origin, gtid_set, ..
        } => {
            assert_eq!(origin, CaptureOrigin::FirstStart);
            assert!(gtid_set.is_some(), "GTID mode: the set is the boundary");
        }
        other => panic!("{other:?}"),
    }
    hook.until_accepted(after).await;
    assert!(
        !hook.accepted().contains(&early),
        "a row committed before the boundary is not a change after it"
    );
    Ok(())
}

/// GTID off: the boundary is the position alone, read in the same one
/// statement; a write after running is delivered.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn mysql_gtid_off_captures_from_the_position() -> Result<()> {
    let (_my, port) = start_mysql(false).await;
    mysql_user(port, "df", true).await;
    let hook = Hook::start().await;
    let (mgr, _) = manager().await;
    mgr.start_pipeline(mysql_spec("mynogtid", port, "df", "never", &hook.url))
        .await?;
    until_status(&mgr, "mynogtid", "running").await;
    match capture_start(&mgr, "mynogtid").await {
        CaptureStart::Mysql {
            origin,
            gtid_set,
            file,
            pos,
            ..
        } => {
            assert_eq!(origin, CaptureOrigin::FirstStart);
            assert_eq!(gtid_set, None);
            assert!(!file.is_empty() && pos > 0, "{file}:{pos}");
        }
        other => panic!("{other:?}"),
    }
    mysql_insert(port, 2_000).await;
    hook.until_accepted(2_000).await;
    Ok(())
}

/// Without BACKUP_ADMIN and SELECT on performance_schema.log_status a first
/// start fails closed with a permission cause; no fallback position.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn mysql_a_first_start_without_the_log_status_grants_fails_closed()
-> Result<()> {
    let (_my, port) = start_mysql(true).await;
    mysql_user(port, "nolog", false).await;
    let hook = Hook::start().await;
    let (mgr, backend) = manager().await;
    mgr.start_pipeline(mysql_spec(
        "mynolog", port, "nolog", "never", &hook.url,
    ))
    .await?;
    until_status(&mgr, "mynolog", "failed").await;
    assert_eq!(info(&mgr, "mynolog").await.capture_start, None);
    let incidents =
        storage::adapters::incidents::IncidentStore::new(backend, "mynolog")
            .list()
            .await?;
    assert!(
        incidents.iter().any(|r| r.cause_code
            == deltaforge_core::incident::CauseCode::SourcePermission),
        "{incidents:?}"
    );
    Ok(())
}

/// A failure after the boundary is persisted and before the stream opens:
/// the restart resumes from the persisted boundary, and a row written while
/// the pipeline was down is delivered.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn mysql_a_failure_before_the_stream_opens_keeps_the_boundary()
-> Result<()> {
    let (_my, port) = start_mysql(true).await;
    mysql_user(port, "df", true).await;
    let hook = Hook::start().await;
    let (mgr, backend) = manager().await;
    sources::stream_probe::fail_after_capture_boundary();
    mgr.start_pipeline(mysql_spec("mycrash", port, "df", "never", &hook.url))
        .await?;
    until_status(&mgr, "mycrash", "failed").await;
    let persisted: Value = serde_json::from_slice(
        &backend
            .kv_get("checkpoints", "my-src::sink::hook")
            .await?
            .expect("the boundary was persisted for the sink"),
    )?;
    mysql_insert(port, 3_000).await;

    mgr.resume("mycrash").await.unwrap();
    until_status(&mgr, "mycrash", "running").await;
    match capture_start(&mgr, "mycrash").await {
        CaptureStart::Mysql {
            origin, gtid_set, ..
        } => {
            assert_eq!(origin, CaptureOrigin::Resume);
            assert_eq!(
                gtid_set.as_deref(),
                persisted["gtid_set"].as_str(),
                "the persisted boundary, not a newer tail"
            );
        }
        other => panic!("{other:?}"),
    }
    hook.until_accepted(3_000).await;
    Ok(())
}

/// A stop while starting gives `stopped`; when the stopped run goes on to
/// fix and publish its boundary, the pipeline stays `stopped`. A resume
/// starts a new run, which becomes `running`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn mysql_a_stop_while_starting_is_never_marked_running() -> Result<()> {
    let (_my, port) = start_mysql(true).await;
    mysql_user(port, "df", true).await;
    let hook = Hook::start().await;
    let (mgr, _) = manager().await;
    let (reached, release) =
        sources::stream_probe::hold_before_capture_boundary();
    mgr.start_pipeline(mysql_spec("mystop", port, "df", "never", &hook.url))
        .await?;
    tokio::time::timeout(Duration::from_secs(120), reached.notified())
        .await
        .expect("held");
    assert_eq!(info(&mgr, "mystop").await.status, "starting");
    PipelineController::stop(&mgr, "mystop").await.unwrap();
    until_status(&mgr, "mystop", "stopped").await;
    // The stopped run proceeds past its hold (a late signal).
    release.notify_one();
    stays(&mgr, "mystop", "stopped", Duration::from_secs(5)).await;

    mgr.resume("mystop").await.unwrap();
    until_status(&mgr, "mystop", "running").await;
    mysql_insert(port, 4_000).await;
    hook.until_accepted(4_000).await;
    Ok(())
}

/// A snapshot pipeline is `starting` while its generation is not durably
/// completed (the sink holds the rows), then `running` at the anchor.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn mysql_a_snapshot_is_starting_until_its_generation_completes()
-> Result<()> {
    let (_my, port) = start_mysql(true).await;
    mysql_user(port, "df", true).await;
    let hook = Hook::start().await;
    hook.set_open(false);
    let (mgr, _) = manager().await;
    mgr.start_pipeline(mysql_spec("mysnap", port, "df", "initial", &hook.url))
        .await?;
    stays(&mgr, "mysnap", "starting", Duration::from_secs(5)).await;
    hook.set_open(true);
    until_status(&mgr, "mysnap", "running").await;
    assert_eq!(
        capture_start(&mgr, "mysnap").await.origin(),
        CaptureOrigin::Snapshot
    );
    let all: BTreeSet<i64> = (1..=SNAPSHOT_ROWS).collect();
    assert!(hook.accepted().is_superset(&all), "every snapshot row");
    mysql_insert(port, 5_000).await;
    hook.until_accepted(5_000).await;
    Ok(())
}

// ---- PostgreSQL -----------------------------------------------------------------

async fn start_postgres() -> (ContainerAsync<GenericImage>, u16) {
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
    pg(port)
        .await
        .batch_execute("CREATE TABLE orders (id INT PRIMARY KEY, sku TEXT)")
        .await
        .unwrap();
    (c, port)
}

async fn pg(port: u16) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(
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
    client
}

async fn publish(port: u16) {
    let c = pg(port).await;
    c.batch_execute("CREATE PUBLICATION cb_pub FOR TABLE orders")
        .await
        .unwrap();
    sources::postgres::postgres_publication::register(
        &c,
        &["cb_pub".to_string()],
    )
    .await
    .unwrap();
}

async fn pg_insert(port: u16, id: i64) {
    pg(port)
        .await
        .execute(
            "INSERT INTO orders VALUES ($1, 'new')",
            &[&i32::try_from(id).unwrap()],
        )
        .await
        .unwrap();
}

fn pg_spec(
    name: &str,
    port: u16,
    url: &str,
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
      id: pg-src
      dsn: "host=127.0.0.1 port={port} user=postgres password={PASS} dbname=postgres"
      slot: cb_slot
      publication: cb_pub
      tables: [public.orders]
  processors: []
{}"#,
        sink_yaml(url)
    );
    serde_yaml::from_str(&yaml).expect("pipeline spec")
}

/// PostgreSQL's equivalent of the MySQL gap: a first start held just before
/// it creates its slot reports `starting`; once `running` (idle), a write
/// is delivered; a row committed before the slot existed is not.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn postgres_a_write_after_running_is_delivered() -> Result<()> {
    let (_pg, port) = start_postgres().await;
    publish(port).await;
    let hook = Hook::start().await;
    let (mgr, _) = manager().await;
    let (reached, release) =
        sources::stream_probe::hold_before_capture_boundary();
    mgr.start_pipeline(pg_spec("pgwrite", port, &hook.url))
        .await?;
    let early = {
        let reached = reached.clone();
        tokio::time::timeout(Duration::from_secs(120), reached.notified())
            .await
            .expect("held");
        reached.notify_one();
        pg_insert(port, 500).await;
        500
    };
    let after = a_write_after_running_is_delivered(
        &mgr,
        "pgwrite",
        &hook,
        reached,
        release,
        |id| pg_insert(port, id),
    )
    .await;
    match capture_start(&mgr, "pgwrite").await {
        CaptureStart::Postgres { origin, slot, .. } => {
            assert_eq!(origin, CaptureOrigin::FirstStart);
            assert_eq!(slot, "cb_slot");
        }
        other => panic!("{other:?}"),
    }
    hook.until_accepted(after).await;
    assert!(!hook.accepted().contains(&early));
    Ok(())
}

/// Start-up retries (the publication does not exist yet) keep `starting`;
/// once it exists the pipeline becomes `running` and captures.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn postgres_startup_retries_keep_starting() -> Result<()> {
    let (_pg, port) = start_postgres().await;
    let hook = Hook::start().await;
    let (mgr, _) = manager().await;
    mgr.start_pipeline(pg_spec("pgretry", port, &hook.url))
        .await?;
    stays(&mgr, "pgretry", "starting", Duration::from_secs(5)).await;
    publish(port).await;
    until_status(&mgr, "pgretry", "running").await;
    pg_insert(port, 2_000).await;
    hook.until_accepted(2_000).await;
    Ok(())
}

/// A failure after the slot exists and before the stream opens: the restart
/// starts at the slot's position (never a newer one), and a row written
/// while the pipeline was down is delivered.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn postgres_a_failure_before_the_stream_opens_keeps_the_boundary()
-> Result<()> {
    let (_pg, port) = start_postgres().await;
    publish(port).await;
    let hook = Hook::start().await;
    let (mgr, _) = manager().await;
    sources::stream_probe::fail_after_capture_boundary();
    mgr.start_pipeline(pg_spec("pgcrash", port, &hook.url))
        .await?;
    until_status(&mgr, "pgcrash", "failed").await;
    let slot_lsn: String = pg(port)
        .await
        .query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots \
             WHERE slot_name = 'cb_slot'",
            &[],
        )
        .await?
        .get(0);
    pg_insert(port, 3_000).await;

    mgr.resume("pgcrash").await.unwrap();
    until_status(&mgr, "pgcrash", "running").await;
    match capture_start(&mgr, "pgcrash").await {
        CaptureStart::Postgres { lsn, .. } => {
            assert_eq!(lsn, slot_lsn, "the slot's position, not a newer one")
        }
        other => panic!("{other:?}"),
    }
    hook.until_accepted(3_000).await;
    Ok(())
}
