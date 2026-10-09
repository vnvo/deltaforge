//! A pipeline restarted during or right after its initial snapshot delivers
//! every row (PipelineManager, the source in a container, an HTTP sink served
//! by the test that records the rows it accepted).
//!
//! - A sink commits part of a PostgreSQL snapshot, then fails: the restart
//!   completes the snapshot.
//! - The sink fails before committing anything, after the source has read
//!   every table (PostgreSQL and MySQL): the restart still delivers every
//!   row.
//!
//! Run with:
//! ```bash
//! cargo test -p runner --test snapshot_restart_e2e -- --include-ignored --test-threads=1
//! ```

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
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

const ROWS: i64 = 60;
const PASS: &str = "pg";

// ---- sink -------------------------------------------------------------------

/// How the test's HTTP endpoint answers.
#[derive(Clone, Copy)]
enum Answer {
    /// 200 to everything.
    Accept,
    /// 200 to the next `n` requests, then 500.
    AcceptThenFail(usize),
    /// Hold every request until released, then 500.
    HoldThenFail,
}

struct Hook {
    url: String,
    answer: Arc<Mutex<Answer>>,
    release: Arc<Notify>,
    /// The `id` of every row a 200 answer accepted.
    accepted: Arc<Mutex<BTreeSet<i64>>>,
    /// How many snapshot rows (`op` "r") a 200 answer accepted.
    snapshot_reads: Arc<std::sync::atomic::AtomicUsize>,
}

impl Hook {
    async fn start() -> Self {
        let answer = Arc::new(Mutex::new(Answer::Accept));
        let release = Arc::new(Notify::new());
        let accepted = Arc::new(Mutex::new(BTreeSet::new()));
        let snapshot_reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (a, r, acc, reads) = (
            answer.clone(),
            release.clone(),
            accepted.clone(),
            snapshot_reads.clone(),
        );
        let app = axum::Router::new().route(
            "/",
            axum::routing::post(move |body: axum::body::Bytes| {
                let (a, r, acc, reads) =
                    (a.clone(), r.clone(), acc.clone(), reads.clone());
                async move {
                    let ok = {
                        let mut a = a.lock().unwrap();
                        match *a {
                            Answer::Accept => Some(true),
                            Answer::AcceptThenFail(0) => Some(false),
                            Answer::AcceptThenFail(n) => {
                                *a = Answer::AcceptThenFail(n - 1);
                                Some(true)
                            }
                            Answer::HoldThenFail => None,
                        }
                    };
                    let ok = match ok {
                        Some(ok) => ok,
                        None => {
                            r.notified().await;
                            false
                        }
                    };
                    if !ok {
                        return axum::http::StatusCode::INTERNAL_SERVER_ERROR;
                    }
                    let v = serde_json::from_slice::<Value>(&body)
                        .unwrap_or_default();
                    if v["op"] == "r" {
                        reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                    // MySQL snapshot rows carry integers as strings.
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
            answer,
            release,
            accepted,
            snapshot_reads,
        }
    }

    fn answer(&self, a: Answer) {
        *self.answer.lock().unwrap() = a;
        self.release.notify_waiters();
    }

    fn accepted(&self) -> BTreeSet<i64> {
        self.accepted.lock().unwrap().clone()
    }

    fn snapshot_reads(&self) -> usize {
        self.snapshot_reads
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    async fn until_accepted(&self, id: i64) {
        let deadline = Instant::now() + Duration::from_secs(120);
        while !self.accepted().contains(&id) {
            assert!(Instant::now() < deadline, "row {id} never delivered");
            sleep(Duration::from_millis(200)).await;
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
        send_timeout_secs: 2
        batch_timeout_secs: 2
        connect_timeout_secs: 2
  batch:
    max_events: 10
    max_ms: 50
    respect_source_tx: false
"#
    )
}

// ---- pipeline -----------------------------------------------------------------

async fn manager() -> PipelineManager {
    manager_with_backend().await.0
}

async fn manager_with_backend() -> (PipelineManager, ArcStorageBackend) {
    let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
    let mgr = PipelineManager::with_backend(Arc::clone(&backend))
        .await
        .unwrap();
    (mgr, backend)
}

/// The sink's committed checkpoint, once `done` holds on it.
async fn until_checkpoint(
    backend: &ArcStorageBackend,
    key: &str,
    done: impl Fn(&Value) -> bool,
) -> Value {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        if let Some(v) = backend
            .kv_get("checkpoints", key)
            .await
            .unwrap()
            .and_then(|b| serde_json::from_slice::<Value>(&b).ok())
            && done(&v)
        {
            return v;
        }
        assert!(Instant::now() < deadline, "{key} never reached");
        sleep(Duration::from_millis(200)).await;
    }
}

/// Stop the pipeline, then start it again.
async fn restart(mgr: &PipelineManager, name: &str) {
    PipelineController::stop(mgr, name).await.unwrap();
    until_status(mgr, name, "stopped").await;
    mgr.resume(name).await.unwrap();
}

async fn status(mgr: &PipelineManager, name: &str) -> String {
    PipelineController::get(mgr, name).await.unwrap().status
}

async fn until_status(mgr: &PipelineManager, name: &str, want: &str) {
    let deadline = Instant::now() + Duration::from_secs(120);
    while status(mgr, name).await != want {
        assert!(Instant::now() < deadline, "{name}: never {want}");
        sleep(Duration::from_millis(200)).await;
    }
}

/// After a restart: the pipeline keeps running and every row arrives.
async fn every_row_arrives(mgr: &PipelineManager, name: &str, hook: &Hook) {
    let all: BTreeSet<i64> = (1..=ROWS).collect();
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let info = PipelineController::get(mgr, name).await.unwrap();
        assert_ne!(
            info.status,
            "failed",
            "{name}: the restarted pipeline failed: {}",
            serde_json::json!({"ops": info.ops, "incidents": info.incidents})
        );
        if hook.accepted() == all {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{name}: rows never delivered: {:?}",
            all.difference(&hook.accepted()).collect::<Vec<_>>()
        );
        sleep(Duration::from_millis(200)).await;
    }
}

// ---- PostgreSQL ---------------------------------------------------------------

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
    pg.batch_execute(&format!(
        "CREATE TABLE orders (id INT PRIMARY KEY, sku TEXT);
         INSERT INTO orders SELECT g, 'sku-' || g FROM generate_series(1, {ROWS}) g;
         CREATE PUBLICATION snap_pub FOR TABLE orders;"
    ))
    .await
    .unwrap();
    sources::postgres::postgres_publication::register(
        &pg,
        &["snap_pub".to_string()],
    )
    .await
    .unwrap();
    (c, port)
}

async fn pg_insert(port: u16, id: i64) {
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
    pg.execute(
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
      slot: snap_slot
      publication: snap_pub
      tables: [public.orders]
      snapshot:
        mode: initial
        chunk_size: 10
        max_parallel_tables: 1
  processors: []
{}"#,
        sink_yaml(url)
    );
    serde_yaml::from_str(&yaml).expect("pipeline spec")
}

/// A sink that committed part of the snapshot, then failed: the restart
/// completes the snapshot.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn postgres_restart_after_a_committed_snapshot_batch() -> Result<()> {
    let (_pg, port) = start_postgres().await;
    let hook = Hook::start().await;
    let mgr = manager().await;
    hook.answer(Answer::AcceptThenFail(15));
    mgr.start_pipeline(pg_spec("pgpart", port, &hook.url))
        .await?;
    until_status(&mgr, "pgpart", "failed").await;
    assert!(!hook.accepted().is_empty(), "a batch was committed");

    hook.answer(Answer::Accept);
    mgr.resume("pgpart").await.unwrap();
    every_row_arrives(&mgr, "pgpart", &hook).await;
    Ok(())
}

/// The sink failed before committing anything, after the source had read
/// every table: the restart delivers every row.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn postgres_restart_after_the_snapshot_was_read_but_not_committed()
-> Result<()> {
    let (_pg, port) = start_postgres().await;
    let hook = Hook::start().await;
    let mgr = manager().await;
    hook.answer(Answer::HoldThenFail);
    mgr.start_pipeline(pg_spec("pgheld", port, &hook.url))
        .await?;
    // The source reads every table while the sink holds the first batch.
    sleep(Duration::from_secs(5)).await;
    // Released into failure (retries fail too), until the pipeline fails.
    hook.answer(Answer::AcceptThenFail(0));
    until_status(&mgr, "pgheld", "failed").await;
    hook.answer(Answer::Accept);
    assert!(hook.accepted().is_empty(), "nothing was committed");
    mgr.resume("pgheld").await.unwrap();
    every_row_arrives(&mgr, "pgheld", &hook).await;
    Ok(())
}

/// A delivered snapshot's completing checkpoint is committed (it rides the
/// last event): a restart streams on without copying the snapshot again.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn postgres_a_delivered_snapshot_is_not_copied_again() -> Result<()> {
    let (_pg, port) = start_postgres().await;
    let hook = Hook::start().await;
    let (mgr, backend) = manager_with_backend().await;
    mgr.start_pipeline(pg_spec("pgdone", port, &hook.url))
        .await?;
    every_row_arrives(&mgr, "pgdone", &hook).await;
    // The completing checkpoint: a stream position, not a snapshot one.
    until_checkpoint(&backend, "pg-src::sink::hook", |v| {
        v.get("lsn").is_some()
    })
    .await;

    restart(&mgr, "pgdone").await;
    let reads = hook.snapshot_reads();
    pg_insert(port, ROWS + 1).await;
    hook.until_accepted(ROWS + 1).await;
    assert_eq!(hook.snapshot_reads(), reads, "no snapshot row again");
    Ok(())
}

// ---- MySQL --------------------------------------------------------------------

async fn start_mysql() -> (ContainerAsync<GenericImage>, u16) {
    let c = GenericImage::new("mysql", "8.4")
        .with_wait_for(WaitFor::message_on_stderr(
            "ready for connections. Version: '8.4",
        ))
        .with_env_var("MYSQL_ROOT_PASSWORD", "rootpw")
        .with_cmd(vec![
            "--server-id=999",
            "--log-bin=/var/lib/mysql/mysql-bin.log",
            "--binlog-format=ROW",
            "--binlog-row-image=FULL",
            "--gtid-mode=ON",
            "--enforce-gtid-consistency=ON",
            "--binlog-checksum=NONE",
        ])
        .gate_owned()
        .start()
        .await
        .expect("start mysql");
    let port = c.get_host_port_ipv4(3306).await.expect("mysql port");
    sleep(Duration::from_secs(3)).await;
    let mut root =
        mysql_conn(&format!("mysql://root:rootpw@127.0.0.1:{port}/"))
            .await
            .unwrap();
    for sql in [
        "CREATE DATABASE shop".to_string(),
        "CREATE USER 'df'@'%' IDENTIFIED BY 'dfpw'".to_string(),
        "GRANT REPLICATION SLAVE, REPLICATION CLIENT, RELOAD ON *.* TO 'df'@'%'"
            .to_string(),
        "GRANT SELECT ON shop.* TO 'df'@'%'".to_string(),
        "CREATE TABLE shop.orders (id INT PRIMARY KEY, sku VARCHAR(64))"
            .to_string(),
        format!(
            "INSERT INTO shop.orders \
             WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g WHERE n < {ROWS}) \
             SELECT n, CONCAT('sku-', n) FROM g"
        ),
    ] {
        root.query_drop(sql).await.unwrap();
    }
    // Prime the CDC user's caching_sha2_password cache (the binlog client
    // does not do the first, uncached authentication over plaintext).
    let mut df = mysql_conn(&format!("mysql://df:dfpw@127.0.0.1:{port}/shop"))
        .await
        .unwrap();
    df.query_drop("SELECT 1").await.unwrap();
    (c, port)
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

fn mysql_spec(
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
    type: mysql
    config:
      id: my-src
      dsn: "mysql://df:dfpw@127.0.0.1:{port}/shop"
      tables: [shop.orders]
      snapshot:
        mode: initial
        chunk_size: 10
        max_parallel_tables: 1
  processors: []
{}"#,
        sink_yaml(url)
    );
    serde_yaml::from_str(&yaml).expect("pipeline spec")
}

/// The sink failed before committing anything, after the source had read
/// every table and recorded its snapshot finished: the restart delivers every
/// row.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn mysql_restart_after_the_snapshot_was_read_but_not_committed()
-> Result<()> {
    let (_my, port) = start_mysql().await;
    let hook = Hook::start().await;
    let mgr = manager().await;
    hook.answer(Answer::HoldThenFail);
    mgr.start_pipeline(mysql_spec("myheld", port, &hook.url))
        .await?;
    // The source reads every table while the sink holds the first batch.
    sleep(Duration::from_secs(5)).await;
    // Released into failure (retries fail too), until the pipeline fails.
    hook.answer(Answer::AcceptThenFail(0));
    until_status(&mgr, "myheld", "failed").await;
    hook.answer(Answer::Accept);
    assert!(hook.accepted().is_empty(), "nothing was committed");
    mgr.resume("myheld").await.unwrap();
    every_row_arrives(&mgr, "myheld", &hook).await;
    Ok(())
}

async fn mysql_insert(port: u16, id: i64) {
    let mut root =
        mysql_conn(&format!("mysql://root:rootpw@127.0.0.1:{port}/"))
            .await
            .unwrap();
    root.query_drop(format!("INSERT INTO shop.orders VALUES ({id}, 'new')"))
        .await
        .unwrap();
}

/// A delivered snapshot's completing checkpoint is committed (it rides the
/// last event): a restart streams on without copying the snapshot again.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn mysql_a_delivered_snapshot_is_not_copied_again() -> Result<()> {
    let (_my, port) = start_mysql().await;
    let hook = Hook::start().await;
    let (mgr, backend) = manager_with_backend().await;
    mgr.start_pipeline(mysql_spec("mydone", port, &hook.url))
        .await?;
    every_row_arrives(&mgr, "mydone", &hook).await;
    until_checkpoint(&backend, "my-src::sink::hook", |v| {
        v.get("snapshot_completed").is_some()
    })
    .await;

    restart(&mgr, "mydone").await;
    let reads = hook.snapshot_reads();
    mysql_insert(port, ROWS + 1).await;
    hook.until_accepted(ROWS + 1).await;
    assert_eq!(hook.snapshot_reads(), reads, "no snapshot row again");
    Ok(())
}

/// A durably completed generation is never reinterpreted: a sink whose
/// checkpoint falls behind its terminal afterwards (here the completing
/// checkpoint without its mark: at the anchor, proving nothing) is reported
/// as lagging (`sink_snapshot_incomplete`), and the snapshot is not copied
/// again; the stream continues.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn mysql_a_sink_behind_a_completed_generation_is_reported_not_recopied()
-> Result<()> {
    let (_my, port) = start_mysql().await;
    let hook = Hook::start().await;
    let (mgr, backend) = manager_with_backend().await;
    mgr.start_pipeline(mysql_spec("myold", port, &hook.url))
        .await?;
    every_row_arrives(&mgr, "myold", &hook).await;
    let mut completing =
        until_checkpoint(&backend, "my-src::sink::hook", |v| {
            v.get("snapshot_completed").is_some()
        })
        .await;
    // Completed once the terminal's acknowledgement is seen.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let completed = matches!(
            sources::snapshot_queue::QueueStore::new(backend.clone(), "my-src")
                .read()
                .await?,
            Some(sources::snapshot_queue::Stored::Current { control, .. })
                if control.state == sources::snapshot_queue::State::Completed
        );
        if completed {
            break;
        }
        assert!(Instant::now() < deadline, "never completed");
        sleep(Duration::from_millis(200)).await;
    }
    PipelineController::stop(&mgr, "myold").await.unwrap();
    until_status(&mgr, "myold", "stopped").await;

    // The sink's checkpoint falls behind the terminal: the anchor, unmarked.
    for mark in ["snapshot_completed", "snapshot_chain"] {
        completing.as_object_mut().unwrap().remove(mark);
    }
    backend
        .kv_put(
            "checkpoints",
            "my-src::sink::hook",
            &serde_json::to_vec(&completing)?,
        )
        .await?;

    let reads = hook.snapshot_reads();
    mgr.resume("myold").await.unwrap();
    mysql_insert(port, ROWS + 1).await;
    hook.until_accepted(ROWS + 1).await;
    assert_eq!(hook.snapshot_reads(), reads, "no snapshot row again");
    let incidents =
        storage::adapters::incidents::IncidentStore::new(backend, "myold")
            .list()
            .await?;
    assert!(
        incidents.iter().any(|r| r.reason_code
            == deltaforge_core::incident::ReasonCode::SinkSnapshotIncomplete),
        "{incidents:?}"
    );
    Ok(())
}

// ---- Durable snapshot queue evidence -----------------------------------------
//
// `docs/design/snapshot-durable-queue.md`, sections 2, 6, 9 and 14: crash and
// restart on both durable backends and both engines, the policy outcomes,
// blocking across restarts, and the anchor-age and source-log bounds.

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

/// A pipeline manager over `backend`.
async fn manager_over(backend: ArcStorageBackend) -> PipelineManager {
    PipelineManager::with_backend(backend).await.unwrap()
}

/// A SQLite state store in a fresh directory.
fn sqlite() -> (tempfile::TempDir, ArcStorageBackend) {
    let dir = tempfile::tempdir().unwrap();
    let backend =
        storage::SqliteStorageBackend::open(dir.path().join("state.db"))
            .unwrap();
    (dir, backend)
}

/// A PostgreSQL state store in its own container.
async fn postgres_store() -> (ContainerAsync<GenericImage>, ArcStorageBackend) {
    let c = GenericImage::new("postgres", "17")
        .with_wait_for(WaitFor::message_on_stderr("database system is ready"))
        .with_env_var("POSTGRES_PASSWORD", PASS)
        .gate_owned()
        .start()
        .await
        .expect("start the state store");
    let port = c.get_host_port_ipv4(5432).await.expect("store port");
    sleep(Duration::from_secs(3)).await;
    let backend = storage::PostgresStorageBackend::connect(&format!(
        "host=127.0.0.1 port={port} user=postgres password={PASS} dbname=postgres"
    ))
    .await
    .unwrap();
    (c, backend)
}

/// The source's snapshot control record.
async fn control(
    backend: &ArcStorageBackend,
    source: &str,
) -> Option<sources::snapshot_queue::GenerationControl> {
    match sources::snapshot_queue::QueueStore::new(backend.clone(), source)
        .read()
        .await
        .unwrap()
    {
        Some(sources::snapshot_queue::Stored::Current { control, .. }) => {
            Some(*control)
        }
        _ => None,
    }
}

async fn until_control(
    backend: &ArcStorageBackend,
    source: &str,
    done: impl Fn(&sources::snapshot_queue::GenerationControl) -> bool,
) -> sources::snapshot_queue::GenerationControl {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        if let Some(c) = control(backend, source).await
            && done(&c)
        {
            return c;
        }
        assert!(Instant::now() < deadline, "{source}: control never reached");
        sleep(Duration::from_millis(200)).await;
    }
}

fn completed(c: &sources::snapshot_queue::GenerationControl) -> bool {
    c.state == sources::snapshot_queue::State::Completed
}

/// The incidents' `(reason, class)` (the class evidence as its JSON).
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

/// HTTP sinks `(id, url, required)`.
fn hooks_yaml(hooks: &[(&str, &str, bool)]) -> String {
    let mut y = String::from("  sinks:\n");
    for (id, url, required) in hooks {
        y.push_str(&format!(
            "    - type: http\n      config:\n        id: {id}\n        url: \"{url}\"\n        required: {required}\n        send_timeout_secs: 2\n        batch_timeout_secs: 2\n        connect_timeout_secs: 2\n"
        ));
    }
    y.push_str(
        "  batch:\n    max_events: 10\n    max_ms: 50\n    respect_source_tx: false\n",
    );
    y
}

/// A PostgreSQL pipeline with `snapshot` settings appended to the defaults,
/// `sinks` and an optional commit policy.
fn pg_spec_with(
    name: &str,
    port: u16,
    snapshot: &str,
    sinks: &str,
    commit: &str,
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
      slot: snap_slot
      publication: snap_pub
      tables: [public.orders]
      snapshot:
        mode: initial
        chunk_size: 10
        max_parallel_tables: 1
{snapshot}
  processors: []
{commit}{sinks}"#
    );
    serde_yaml::from_str(&yaml).expect("pipeline spec")
}

fn mysql_spec_with(
    name: &str,
    port: u16,
    snapshot: &str,
    sinks: &str,
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
      dsn: "mysql://df:dfpw@127.0.0.1:{port}/shop"
      tables: [shop.orders]
      snapshot:
        mode: initial
        chunk_size: 10
        max_parallel_tables: 1
{snapshot}
  processors: []
{sinks}"#
    );
    serde_yaml::from_str(&yaml).expect("pipeline spec")
}

/// PostgreSQL source over a SQLite store: an interrupted generation is
/// replaced by the next one of its chain and copied in full, completes only
/// through its terminal barrier, and a later restart streams without copying.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn postgres_on_sqlite_an_interrupted_generation_is_replaced() -> Result<()>
{
    let (_pg, port) = start_postgres().await;
    let (_dir, backend) = sqlite();
    let hook = Hook::start().await;
    let mgr = manager_over(backend.clone()).await;
    hook.answer(Answer::AcceptThenFail(3));
    mgr.start_pipeline(pg_spec_with(
        "pgsqlite",
        port,
        "",
        &hooks_yaml(&[("hook", &hook.url, true)]),
        "",
    ))
    .await?;
    until_status(&mgr, "pgsqlite", "failed").await;
    let first = control(&backend, "pg-src").await.expect("a generation");
    assert!(!completed(&first), "interrupted");

    hook.answer(Answer::Accept);
    mgr.resume("pgsqlite").await.unwrap();
    every_row_arrives(&mgr, "pgsqlite", &hook).await;
    let done = until_control(&backend, "pg-src", completed).await;
    assert_eq!(done.snapshot_chain, first.snapshot_chain, "one chain");
    assert_eq!(done.generation, first.generation + 1, "replaced");
    assert_eq!(done.replaced, Some(first.generation));

    restart(&mgr, "pgsqlite").await;
    let reads = hook.snapshot_reads();
    pg_insert(port, ROWS + 1).await;
    hook.until_accepted(ROWS + 1).await;
    assert_eq!(hook.snapshot_reads(), reads, "no snapshot row again");
    assert_eq!(control(&backend, "pg-src").await, Some(done));
    Ok(())
}

/// MySQL source over a PostgreSQL store: the same.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn mysql_on_postgres_store_an_interrupted_generation_is_replaced()
-> Result<()> {
    let (_my, port) = start_mysql().await;
    let (_store, backend) = postgres_store().await;
    let hook = Hook::start().await;
    let mgr = manager_over(backend.clone()).await;
    hook.answer(Answer::AcceptThenFail(3));
    mgr.start_pipeline(mysql_spec_with(
        "mypg",
        port,
        "",
        &hooks_yaml(&[("hook", &hook.url, true)]),
    ))
    .await?;
    until_status(&mgr, "mypg", "failed").await;
    let first = control(&backend, "my-src").await.expect("a generation");
    assert!(!completed(&first), "interrupted");

    hook.answer(Answer::Accept);
    mgr.resume("mypg").await.unwrap();
    every_row_arrives(&mgr, "mypg", &hook).await;
    let done = until_control(&backend, "my-src", completed).await;
    assert_eq!(done.snapshot_chain, first.snapshot_chain, "one chain");
    assert_eq!(done.generation, first.generation + 1, "replaced");

    restart(&mgr, "mypg").await;
    let reads = hook.snapshot_reads();
    mysql_insert(port, ROWS + 1).await;
    hook.until_accepted(ROWS + 1).await;
    assert_eq!(hook.snapshot_reads(), reads, "no snapshot row again");
    Ok(())
}

/// The frozen policy decides completion (design section 6.2): `Required`
/// and `Quorum(1)` complete without a failing optional sink, which is then
/// reported lagging (`sink_snapshot_incomplete`); `All` does not complete
/// while one sink fails, and the generation is replaced and completed by
/// both once it recovers.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn completion_follows_the_frozen_policy() -> Result<()> {
    trace();
    for (name, commit, b_required) in [
        ("polreq", "", false),
        (
            "polquorum",
            "  commit_policy:\n    mode: quorum\n    quorum: 1\n",
            false,
        ),
    ] {
        let (_pg, port) = start_postgres().await;
        let (a, b) = (Hook::start().await, Hook::start().await);
        b.answer(Answer::AcceptThenFail(0));
        let (mgr, backend) = manager_with_backend().await;
        mgr.start_pipeline(pg_spec_with(
            name,
            port,
            "",
            &hooks_yaml(&[("a", &a.url, true), ("b", &b.url, b_required)]),
            commit,
        ))
        .await?;
        every_row_arrives(&mgr, name, &a).await;
        let done = until_control(&backend, "pg-src", completed).await;
        assert_eq!(done.completion.unwrap().acks, ["a"], "{name}");
        let deadline = Instant::now() + Duration::from_secs(60);
        while !incidents(&backend, name)
            .await
            .iter()
            .any(|(r, _)| r == "sink_snapshot_incomplete")
        {
            assert!(Instant::now() < deadline, "{name}: no lagging incident");
            sleep(Duration::from_millis(200)).await;
        }
        pg_insert(port, ROWS + 1).await;
        a.until_accepted(ROWS + 1).await;
    }

    // All: one sink failing holds completion - the pipeline fails on it.
    let (_pg, port) = start_postgres().await;
    let (a, b) = (Hook::start().await, Hook::start().await);
    b.answer(Answer::AcceptThenFail(0));
    let (mgr, backend) = manager_with_backend().await;
    mgr.start_pipeline(pg_spec_with(
        "polall",
        port,
        "",
        &hooks_yaml(&[("a", &a.url, true), ("b", &b.url, true)]),
        "  commit_policy:\n    mode: all\n",
    ))
    .await?;
    until_status(&mgr, "polall", "failed").await;
    let held = control(&backend, "pg-src").await.expect("a generation");
    assert!(!completed(&held), "All: not completed while b fails");
    b.answer(Answer::Accept);
    mgr.resume("polall").await.unwrap();
    every_row_arrives(&mgr, "polall", &b).await;
    let done = until_control(&backend, "pg-src", completed).await;
    assert!(done.generation > held.generation, "replaced");
    let mut acks = done.completion.unwrap().acks;
    acks.sort();
    assert_eq!(acks, ["a", "b"]);
    Ok(())
}

/// A blocked generation stays halted across restarts (design section 9.2):
/// a plan above its bound blocks with `snapshot_bound_exceeded`; two
/// restarts copy nothing and keep it blocked; only the proof-bound
/// replacement (the recovery CLI's `resnapshot`, here its queue operation)
/// clears it.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn a_blocked_generation_survives_restarts() -> Result<()> {
    trace();
    let (_pg, port) = start_postgres().await;
    let (_dir, backend) = sqlite();
    let hook = Hook::start().await;
    let sinks = hooks_yaml(&[("hook", &hook.url, true)]);
    let mgr = manager_over(backend.clone()).await;
    mgr.start_pipeline(pg_spec_with(
        "pgblocked",
        port,
        "        max_plan_items: 0",
        &sinks,
        "",
    ))
    .await?;
    until_status(&mgr, "pgblocked", "failed").await;
    let blocked = control(&backend, "pg-src").await.expect("a generation");
    assert_eq!(
        blocked.blocked.as_ref().map(|b| b.reason.as_str()),
        Some("snapshot_bound_exceeded")
    );
    for _ in 0..2 {
        mgr.resume("pgblocked").await.unwrap();
        until_status(&mgr, "pgblocked", "failed").await;
        assert_eq!(control(&backend, "pg-src").await, Some(blocked.clone()));
        assert!(hook.accepted().is_empty(), "no row");
    }
    assert!(incidents(&backend, "pgblocked").await.iter().any(|(r, c)| r
        == "snapshot_bound_exceeded"
        && c.contains("plan_items")));

    // The recovery: the bound raised, a proof-bound replacement.
    PipelineController::delete(&mgr, "pgblocked").await.ok();
    let q = sources::snapshot_queue::QueueStore::new(backend.clone(), "pg-src");
    let stored = q.read().await?.unwrap();
    q.replace(
        &stored,
        &blocked.lineage,
        &blocked.config_fingerprint,
        blocked.policy.clone(),
        true,
    )
    .await?;
    mgr.start_pipeline(pg_spec_with("pgblocked", port, "", &sinks, ""))
        .await?;
    every_row_arrives(&mgr, "pgblocked", &hook).await;
    // The recovery allocated the next generation; the start replaced that
    // allocated one in turn (a start never continues an allocated plan).
    let done = until_control(&backend, "pg-src", completed).await;
    assert!(done.generation > blocked.generation);
    assert_eq!(done.snapshot_chain, blocked.snapshot_chain);
    Ok(())
}

/// The anchor-age bound (design section 9): a generation held longer than
/// `max_anchor_age_secs` blocks with `snapshot_anchor_unavailable` (class
/// `anchor_age`), and stays blocked at the next start.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn the_anchor_age_bound_blocks_the_generation() -> Result<()> {
    trace();
    let (_pg, port) = start_postgres().await;
    let hook = Hook::start().await;
    let (mgr, backend) = manager_with_backend().await;
    let (_reached, release) = sources::snapshot_probe::hold_during_copy();
    mgr.start_pipeline(pg_spec_with(
        "pgage",
        port,
        "        max_anchor_age_secs: 2",
        &hooks_yaml(&[("hook", &hook.url, true)]),
        "",
    ))
    .await?;
    let blocked =
        until_control(&backend, "pg-src", |c| c.blocked.is_some()).await;
    assert_eq!(
        blocked.blocked.as_ref().map(|b| b.reason.as_str()),
        Some("snapshot_anchor_unavailable")
    );
    assert!(
        incidents(&backend, "pgage")
            .await
            .iter()
            .any(|(r, c)| r == "snapshot_anchor_unavailable"
                && c.contains("anchor_age"))
    );
    release.notify_one();
    until_status(&mgr, "pgage", "failed").await;
    mgr.resume("pgage").await.unwrap();
    until_status(&mgr, "pgage", "failed").await;
    assert_eq!(control(&backend, "pg-src").await, Some(blocked));
    Ok(())
}

/// PostgreSQL WAL retention (design section 9): the generation's slot lost
/// while it runs blocks it with `snapshot_anchor_unavailable` (class
/// `slot_missing`) before any later row.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn a_lost_slot_blocks_the_generation() -> Result<()> {
    trace();
    let (_pg, port) = start_postgres().await;
    let hook = Hook::start().await;
    let (mgr, backend) = manager_with_backend().await;
    let (reached, release) = sources::snapshot_probe::hold_during_copy();
    mgr.start_pipeline(pg_spec_with(
        "pgslot",
        port,
        "        max_anchor_age_secs: 300",
        &hooks_yaml(&[("hook", &hook.url, true)]),
        "",
    ))
    .await?;
    reached.notified().await;
    let (pg, conn) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={port} user=postgres password={PASS} dbname=postgres"
        ),
        NoTls,
    )
    .await?;
    tokio::spawn(async move {
        conn.await.ok();
    });
    pg.execute("SELECT pg_drop_replication_slot('snap_slot')", &[])
        .await?;
    let blocked =
        until_control(&backend, "pg-src", |c| c.blocked.is_some()).await;
    assert_eq!(
        blocked.blocked.as_ref().map(|b| b.reason.as_str()),
        Some("snapshot_anchor_unavailable")
    );
    assert!(
        incidents(&backend, "pgslot")
            .await
            .iter()
            .any(|(r, c)| r == "snapshot_anchor_unavailable"
                && c.contains("slot_missing"))
    );
    release.notify_one();
    Ok(())
}

/// MySQL binlog retention (design section 9): the anchor's binlog file
/// purged while the generation runs blocks it with
/// `snapshot_anchor_unavailable` (class `binlog_purged`).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn a_purged_anchor_binlog_blocks_the_generation() -> Result<()> {
    trace();
    let (_my, port) = start_mysql().await;
    let hook = Hook::start().await;
    let (mgr, backend) = manager_with_backend().await;
    let (reached, release) = sources::snapshot_probe::hold_during_copy();
    mgr.start_pipeline(mysql_spec_with(
        "mypurge",
        port,
        "        max_anchor_age_secs: 300",
        &hooks_yaml(&[("hook", &hook.url, true)]),
    ))
    .await?;
    reached.notified().await;
    let mut root =
        mysql_conn(&format!("mysql://root:rootpw@127.0.0.1:{port}/")).await?;
    root.query_drop("FLUSH BINARY LOGS").await?;
    let current: String = root
        .query_first::<mysql_async::Row, _>("SHOW BINARY LOG STATUS")
        .await?
        .and_then(|mut r| r.take(0))
        .expect("the current binlog");
    root.query_drop(format!("PURGE BINARY LOGS TO '{current}'"))
        .await?;
    let blocked =
        until_control(&backend, "my-src", |c| c.blocked.is_some()).await;
    assert_eq!(
        blocked.blocked.as_ref().map(|b| b.reason.as_str()),
        Some("snapshot_anchor_unavailable")
    );
    assert!(
        incidents(&backend, "mypurge")
            .await
            .iter()
            .any(|(r, c)| r == "snapshot_anchor_unavailable"
                && c.contains("binlog_purged"))
    );
    release.notify_one();
    Ok(())
}

// ---- Recovery: resnapshot (docs/design/recovery-cli.md, section 5.1) ------

async fn pg_admin(port: u16) -> tokio_postgres::Client {
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

fn recovery(mgr: &Arc<PipelineManager>) -> runner::recovery::RecoveryService {
    runner::recovery::RecoveryService::new(Arc::clone(mgr))
        .with_operation(Arc::new(runner::recovery_resnapshot::Resnapshot))
        .with_operation(Arc::new(
            runner::recovery_adopt_timeline::AdoptTimeline::default(),
        ))
}

fn plan_req(incident: Option<String>) -> rest_api::recovery::PlanRequest {
    rest_api::recovery::PlanRequest {
        operation: "resnapshot".into(),
        incident,
        args: Default::default(),
    }
}

/// The open incident diagnose offers `resnapshot` for.
async fn resnapshot_incident(
    svc: &runner::recovery::RecoveryService,
    name: &str,
) -> String {
    use rest_api::recovery::RecoveryController;
    let diag = svc.diagnose(name).await.unwrap();
    diag["incidents"]
        .as_array()
        .unwrap()
        .iter()
        .find(|i| {
            i["operations"]
                .as_array()
                .is_some_and(|o| o.iter().any(|x| x == "resnapshot"))
        })
        .unwrap_or_else(|| panic!("no resnapshot incident: {diag}"))["incident_id"]
        .as_str()
        .unwrap()
        .to_string()
}

async fn plan_and_apply(
    svc: &runner::recovery::RecoveryService,
    name: &str,
    incident: Option<String>,
) -> (Value, Value) {
    use rest_api::recovery::{ApplyRequest, Caller, RecoveryController};
    let planned = svc.plan(name, plan_req(incident.clone())).await.unwrap();
    let applied = svc
        .apply(
            name,
            ApplyRequest {
                plan: plan_req(incident),
                expect_proof: planned["proof"].as_str().unwrap().into(),
                actor: "alice".into(),
                reason: "test".into(),
            },
            Caller {
                origin: "127.0.0.1:1".into(),
                credential: "token",
            },
        )
        .await
        .unwrap();
    (planned, applied)
}

async fn sink_checkpoints(
    backend: &ArcStorageBackend,
    source: &str,
) -> Vec<(String, Vec<u8>)> {
    let mut out = Vec::new();
    use checkpoints::CheckpointStore;
    for key in storage::BackendCheckpointStore::new(backend.clone())
        .list_with_prefix(&format!("{source}::sink::"))
        .await
        .unwrap()
    {
        out.push((
            key.clone(),
            backend.kv_get("checkpoints", &key).await.unwrap().unwrap(),
        ));
    }
    out
}

fn step_names(planned: &Value) -> Vec<String> {
    planned["plan"]["steps"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["name"].as_str().unwrap().to_string())
        .collect()
}

/// A completed snapshot, then generation 2 blocked by its plan bound; the
/// bound raised; still blocked.
async fn blocked_after_a_completed_snapshot(
    mgr: &Arc<PipelineManager>,
    backend: &ArcStorageBackend,
    name: &str,
    source: &str,
    hook: &Hook,
) -> sources::snapshot_queue::GenerationControl {
    every_row_arrives(mgr, name, hook).await;
    let first = until_control(backend, source, completed).await;
    let snapshot = |extra: Value| serde_json::json!({ "spec": { "source": { "config": { "snapshot": extra } } } });
    PipelineController::patch(
        mgr.as_ref(),
        name,
        snapshot(serde_json::json!({ "mode": "always", "max_plan_items": 0 })),
    )
    .await
    .unwrap();
    until_status(mgr, name, "failed").await;
    let blocked = until_control(backend, source, |c| c.blocked.is_some()).await;
    assert_eq!(blocked.generation, first.generation + 1);
    PipelineController::patch(
        mgr.as_ref(),
        name,
        snapshot(serde_json::json!({ "mode": "initial", "max_plan_items": 1_000_000 })),
    )
    .await
    .unwrap();
    until_status(mgr, name, "failed").await;
    assert_eq!(
        control(backend, source).await.unwrap().generation,
        blocked.generation
    );
    blocked
}

/// The recovery allocation runs at the next explicit resume, in place,
/// copying the whole snapshot again; nothing before that resume moved.
async fn recovers_by_resume(
    mgr: &Arc<PipelineManager>,
    backend: &ArcStorageBackend,
    name: &str,
    source: &str,
    hook: &Hook,
    blocked: &sources::snapshot_queue::GenerationControl,
    before: &[(String, Vec<u8>)],
) {
    let next = control(backend, source).await.unwrap();
    assert_eq!(next.generation, blocked.generation + 1);
    assert_eq!(next.snapshot_chain, blocked.snapshot_chain);
    assert_eq!(
        next.allocation,
        Some(sources::snapshot_queue::AllocationMark::Recovery)
    );
    assert_eq!(next.blocked, None);
    assert_eq!(next.adoption, sources::snapshot_queue::Adoption::Pending);
    // Not started; no checkpoint moved.
    sleep(Duration::from_secs(1)).await;
    assert_ne!(status(mgr, name).await, "running");
    assert_eq!(sink_checkpoints(backend, source).await, before);
    let reads = hook.snapshot_reads();
    mgr.resume(name).await.unwrap();
    let done = until_control(backend, source, completed).await;
    assert_eq!(
        done.generation, next.generation,
        "run in place, no further generation"
    );
    assert_eq!(done.adoption, sources::snapshot_queue::Adoption::Done);
    let deadline = Instant::now() + Duration::from_secs(60);
    while hook.snapshot_reads() < reads + ROWS as usize {
        assert!(
            Instant::now() < deadline,
            "the snapshot was not copied again"
        );
        sleep(Duration::from_millis(200)).await;
    }
    assert_ne!(
        sink_checkpoints(backend, source).await,
        before,
        "moved by the generation"
    );
}

/// PostgreSQL: a blocked generation and a lost (dropped) slot recover
/// through `resnapshot` without advancing any checkpoint; the explicit
/// resume runs the recovery allocation and the next start creates the slot.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn postgres_resnapshot_recovers_a_blocked_generation_and_a_lost_slot()
-> Result<()> {
    trace();
    let (_pg, port) = start_postgres().await;
    let (_dir, backend) = sqlite();
    let hook = Hook::start().await;
    let mgr = Arc::new(manager_over(backend.clone()).await);
    let sinks = hooks_yaml(&[("hook", &hook.url, true)]);
    mgr.start_pipeline(pg_spec_with("pgrec", port, "", &sinks, ""))
        .await?;
    let blocked = blocked_after_a_completed_snapshot(
        &mgr, &backend, "pgrec", "pg-src", &hook,
    )
    .await;
    pg_admin(port)
        .await
        .execute("SELECT pg_drop_replication_slot('snap_slot')", &[])
        .await?;
    let before = sink_checkpoints(&backend, "pg-src").await;
    assert!(!before.is_empty());
    let svc = recovery(&mgr);
    let incident = resnapshot_incident(&svc, "pgrec").await;
    let (planned, applied) =
        plan_and_apply(&svc, "pgrec", Some(incident.clone())).await;
    assert_eq!(
        step_names(&planned),
        [
            "replace_generation",
            "reclaim_plan",
            "authorize_slot_recreation"
        ]
    );
    assert_eq!(applied["state"], "completed");
    recovers_by_resume(
        &mgr, &backend, "pgrec", "pg-src", &hook, &blocked, &before,
    )
    .await;
    slot_recreation_consumed(
        &backend,
        "pgrec",
        "pg-src",
        blocked.generation + 1,
    )
    .await;
    // A completed same-proof re-apply (on the stopped pipeline) stays a
    // no-op: nothing recomputed or written.
    use rest_api::recovery::{ApplyRequest, Caller, RecoveryController};
    PipelineController::stop(mgr.as_ref(), "pgrec").await?;
    let after = control(&backend, "pg-src").await;
    let deadline = Instant::now() + Duration::from_secs(60);
    let again = loop {
        match svc
            .apply(
                "pgrec",
                ApplyRequest {
                    plan: plan_req(Some(incident.clone())),
                    expect_proof: planned["proof"].as_str().unwrap().into(),
                    actor: "alice".into(),
                    reason: "again".into(),
                },
                Caller {
                    origin: "127.0.0.1:1".into(),
                    credential: "token",
                },
            )
            .await
        {
            Ok(v) => break v,
            // The stopped tasks are still finishing.
            Err(e)
                if e.code == "pipeline_not_quiescent"
                    && Instant::now() < deadline =>
            {
                sleep(Duration::from_millis(200)).await;
            }
            Err(e) => panic!("{e:?}"),
        }
    };
    assert_eq!(again["already_completed"], true);
    assert_eq!(control(&backend, "pg-src").await, after);
    Ok(())
}

/// MySQL: a blocked generation whose anchor binlog was purged recovers the
/// same way.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn mysql_resnapshot_recovers_a_blocked_generation_and_a_purged_binlog()
-> Result<()> {
    trace();
    let (_my, port) = start_mysql().await;
    let (_dir, backend) = sqlite();
    let hook = Hook::start().await;
    let mgr = Arc::new(manager_over(backend.clone()).await);
    let sinks = hooks_yaml(&[("hook", &hook.url, true)]);
    mgr.start_pipeline(mysql_spec_with("myrec", port, "", &sinks))
        .await?;
    let blocked = blocked_after_a_completed_snapshot(
        &mgr, &backend, "myrec", "my-src", &hook,
    )
    .await;
    let mut root =
        mysql_conn(&format!("mysql://root:rootpw@127.0.0.1:{port}/")).await?;
    root.query_drop("FLUSH BINARY LOGS").await?;
    let current: String = root
        .query_first::<mysql_async::Row, _>("SHOW BINARY LOG STATUS")
        .await?
        .and_then(|r| r.get(0))
        .unwrap();
    root.query_drop(format!("PURGE BINARY LOGS TO '{current}'"))
        .await?;
    let before = sink_checkpoints(&backend, "my-src").await;
    assert!(!before.is_empty());
    let svc = recovery(&mgr);
    let incident = resnapshot_incident(&svc, "myrec").await;
    let (planned, applied) =
        plan_and_apply(&svc, "myrec", Some(incident)).await;
    assert_eq!(step_names(&planned), ["replace_generation", "reclaim_plan"]);
    assert_eq!(applied["state"], "completed");
    recovers_by_resume(
        &mgr, &backend, "myrec", "my-src", &hook, &blocked, &before,
    )
    .await;
    Ok(())
}

/// The authorization was used and completed by exactly the recovery generation, and
/// the recovery audit shows both the authorization and the recreation.
async fn slot_recreation_consumed(
    backend: &ArcStorageBackend,
    pipeline: &str,
    source: &str,
    generation: u64,
) {
    let (_, auth) =
        sources::snapshot_recovery::read_slot_recreation(backend, source)
            .await
            .unwrap()
            .expect("an authorization");
    assert_eq!(
        auth.state,
        sources::snapshot_recovery::SlotRecreationState::Created
    );
    assert_eq!(
        Some(auth.created.as_ref().unwrap().consistent_lsn.clone()),
        owner_consistent(backend).await,
        "the recorded slot is the owned one"
    );
    assert_eq!(auth.generation, generation);
    let trail = storage::adapters::recovery::RecoveryStore::new(
        backend.clone(),
        pipeline,
    )
    .audit_trail(50)
    .await
    .unwrap();
    let applied = trail
        .iter()
        .find(|e| e.event == "applied" && e.applied.proof == auth.proof)
        .unwrap();
    assert!(
        applied.outcomes["slot_recreation"].contains("authorized"),
        "{applied:?}"
    );
    let recreated = trail
        .iter()
        .find(|e| e.event == "slot_recreated" && e.applied.proof == auth.proof)
        .expect("the recreation is audited");
    assert_eq!(recreated.outcomes["generation"], generation.to_string());
    assert_eq!(recreated.applied.asserted_actor, "alice");
}

async fn slot_exists(a: &tokio_postgres::Client) -> bool {
    a.query_opt(
        "SELECT 1 FROM pg_replication_slots WHERE slot_name = 'snap_slot'",
        &[],
    )
    .await
    .unwrap()
    .is_some()
}

/// PostgreSQL: an owned slot whose retention was lost is dropped only as
/// the plan's explicit step; a slot whose ownership cannot be proven
/// refuses the plan and stays untouched.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn postgres_resnapshot_drops_only_a_provably_owned_lost_slot()
-> Result<()> {
    trace();
    let (_pg, port) = start_postgres().await;
    let (_dir, backend) = sqlite();
    let hook = Hook::start().await;
    let mgr = Arc::new(manager_over(backend.clone()).await);
    let sinks = hooks_yaml(&[("hook", &hook.url, true)]);
    mgr.start_pipeline(pg_spec_with("pgslot", port, "", &sinks, ""))
        .await?;
    let blocked = blocked_after_a_completed_snapshot(
        &mgr, &backend, "pgslot", "pg-src", &hook,
    )
    .await;
    let admin = pg_admin(port).await;
    // Lose the owned, inactive slot's retention.
    for sql in [
        "ALTER SYSTEM SET max_slot_wal_keep_size = '1MB'",
        "SELECT pg_reload_conf()",
        "CREATE TABLE IF NOT EXISTS filler (x text)",
    ] {
        admin.batch_execute(sql).await?;
    }
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        admin
            .batch_execute(
                "INSERT INTO filler SELECT repeat('x', 1000) FROM generate_series(1, 5000); \
                 SELECT pg_switch_wal(); CHECKPOINT;",
            )
            .await?;
        let status: Option<String> = admin
            .query_one(
                "SELECT wal_status::text FROM pg_replication_slots WHERE slot_name = 'snap_slot'",
                &[],
            )
            .await?
            .get(0);
        if status.as_deref() == Some("lost") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the slot never lost its retention"
        );
    }
    admin
        .batch_execute("ALTER SYSTEM RESET max_slot_wal_keep_size")
        .await?;
    admin.batch_execute("SELECT pg_reload_conf()").await?;
    let svc = recovery(&mgr);
    // Without the ownership record the slot is not provably ours: refused,
    // untouched.
    let owner = backend
        .kv_get("checkpoints", "slot_owner:pg-src")
        .await?
        .unwrap();
    backend
        .kv_delete("checkpoints", "slot_owner:pg-src")
        .await?;
    {
        use rest_api::recovery::RecoveryController;
        let e = svc.plan("pgslot", plan_req(None)).await.unwrap_err();
        assert_eq!(e.code, "manual_repair", "{e:?}");
    }
    assert!(slot_exists(&admin).await, "an unprovable slot stays");
    backend
        .kv_put("checkpoints", "slot_owner:pg-src", &owner)
        .await?;
    // Proven: dropped as the plan's explicit step.
    let before = sink_checkpoints(&backend, "pg-src").await;
    let (planned, applied) = plan_and_apply(&svc, "pgslot", None).await;
    assert_eq!(
        step_names(&planned),
        [
            "replace_generation",
            "reclaim_plan",
            "drop_lost_slot",
            "authorize_slot_recreation"
        ]
    );
    assert_eq!(applied["outcomes"]["slot"], "dropped_owned_lost_slot");
    assert!(!slot_exists(&admin).await);
    recovers_by_resume(
        &mgr, &backend, "pgslot", "pg-src", &hook, &blocked, &before,
    )
    .await;
    slot_recreation_consumed(
        &backend,
        "pgslot",
        "pg-src",
        blocked.generation + 1,
    )
    .await;
    Ok(())
}

/// A slot this source created and lost is never recreated implicitly: a
/// start that needs it fails closed without an authorization, and an absent
/// slot whose ownership record is missing is a manual repair.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn postgres_a_lost_slot_is_never_recreated_implicitly() -> Result<()> {
    trace();
    let (_pg, port) = start_postgres().await;
    let (_dir, backend) = sqlite();
    let hook = Hook::start().await;
    let mgr = Arc::new(manager_over(backend.clone()).await);
    let sinks = hooks_yaml(&[("hook", &hook.url, true)]);
    mgr.start_pipeline(pg_spec_with("pglost", port, "", &sinks, ""))
        .await?;
    every_row_arrives(&mgr, "pglost", &hook).await;
    until_control(&backend, "pg-src", completed).await;
    PipelineController::stop(mgr.as_ref(), "pglost").await?;
    until_status(&mgr, "pglost", "stopped").await;
    let admin = pg_admin(port).await;
    // The stopped source's walsender may still hold the slot briefly.
    let deadline = Instant::now() + Duration::from_secs(30);
    while admin
        .execute("SELECT pg_drop_replication_slot('snap_slot')", &[])
        .await
        .is_err()
    {
        assert!(Instant::now() < deadline, "the slot was never dropped");
        sleep(Duration::from_millis(200)).await;
    }
    // A re-snapshot start needs the slot: refused, nothing created.
    PipelineController::patch(
        mgr.as_ref(),
        "pglost",
        serde_json::json!({ "spec": { "source": { "config": {
            "snapshot": { "mode": "always" } } } } }),
    )
    .await?;
    until_status(&mgr, "pglost", "failed").await;
    assert!(!slot_exists(&admin).await, "never recreated implicitly");
    assert!(
        sources::snapshot_recovery::read_slot_recreation(&backend, "pg-src")
            .await?
            .is_none()
    );
    // Without the ownership record nothing can be authorized.
    let owner = backend
        .kv_get("checkpoints", "slot_owner:pg-src")
        .await?
        .unwrap();
    backend
        .kv_delete("checkpoints", "slot_owner:pg-src")
        .await?;
    {
        use rest_api::recovery::RecoveryController;
        let e = recovery(&mgr)
            .plan("pglost", plan_req(None))
            .await
            .unwrap_err();
        assert_eq!(e.code, "manual_repair", "{e:?}");
    }
    backend
        .kv_put("checkpoints", "slot_owner:pg-src", &owner)
        .await?;
    Ok(())
}

/// `pg-adopt-timeline` through the recovery service: for a
/// `timeline_unrecorded` incident over checkpoints without a continuity
/// stamp, apply creates the record at transition 0 at F with the plan's
/// seeded chain, moves no checkpoint and resolves the incident; the same
/// proof again is a no-op; the explicit resume stamps the checkpoints with
/// that chain and streams.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn postgres_adopt_timeline_through_the_recovery_service() -> Result<()> {
    use deltaforge_core::incident::{
        ActionCode, CauseCode, Component, EvidenceKey, IncidentDraft,
        ReasonCode, Retryability, SafetyState,
    };
    use rest_api::recovery::{
        ApplyRequest, Caller, PlanRequest, RecoveryController,
    };
    trace();
    let (_pg, port) = start_postgres().await;
    let (_dir, backend) = sqlite();
    let hook = Hook::start().await;
    let mgr = Arc::new(manager_over(backend.clone()).await);
    let sinks = hooks_yaml(&[("hook", &hook.url, true)]);
    mgr.start_pipeline(pg_spec_with("pgadopt", port, "", &sinks, ""))
        .await?;
    every_row_arrives(&mgr, "pgadopt", &hook).await;
    until_control(&backend, "pg-src", completed).await;
    PipelineController::stop(mgr.as_ref(), "pgadopt").await?;
    until_status(&mgr, "pgadopt", "stopped").await;
    // Its tasks joined: nothing commits a checkpoint any more.
    {
        use rest_api::recovery::RecoveryController;
        let svc = recovery(&mgr);
        let deadline = Instant::now() + Duration::from_secs(60);
        while svc.diagnose("pgadopt").await.unwrap()["quiescent"] != true {
            assert!(Instant::now() < deadline, "the pipeline never stopped");
            sleep(Duration::from_millis(200)).await;
        }
    }

    // As an earlier release left it: no continuity record, unstamped
    // checkpoints, a timeline_unrecorded incident.
    backend
        .kv_delete("failover", "pg_continuity:pg-src")
        .await?;
    let mut lsns = Vec::new();
    for (key, bytes) in sink_checkpoints(&backend, "pg-src").await {
        let mut cp: Value = serde_json::from_slice(&bytes)?;
        for k in ["timeline", "chain", "transition"] {
            cp.as_object_mut().unwrap().remove(k);
        }
        lsns.push(cp["lsn"].as_str().unwrap().to_string());
        backend
            .kv_put("checkpoints", &key, &serde_json::to_vec(&cp)?)
            .await?;
    }
    let before = sink_checkpoints(&backend, "pg-src").await;
    let incidents = storage::adapters::incidents::IncidentStore::new(
        backend.clone(),
        "pgadopt",
    );
    let id = incidents
        .raise(
            &IncidentDraft::new(
                ReasonCode::PgContinuityUnproven,
                Component::Source {
                    id: "pg-src".into(),
                },
                Retryability::OperatorAction,
                SafetyState::HaltedSafe,
                CauseCode::SourceLineage,
            )
            .discriminate("class", "timeline_unrecorded")
            .with_evidence(|e| {
                e.text(EvidenceKey::ReasonClass, "timeline_unrecorded");
            })
            .with_actions(&[ActionCode::AdoptTimeline]),
            1,
        )
        .await?
        .record()
        .incident_id
        .clone();

    let svc = recovery(&mgr);
    let req = || PlanRequest {
        operation: "pg-adopt-timeline".into(),
        incident: Some(id.0.clone()),
        args: Default::default(),
    };
    let diag = svc.diagnose("pgadopt").await.unwrap();
    assert!(diag.to_string().contains("pg-adopt-timeline"), "{diag}");
    // Settle until the stopped source released its slot.
    let deadline = Instant::now() + Duration::from_secs(60);
    let planned = loop {
        match svc.plan("pgadopt", req()).await {
            Ok(p) => break p,
            Err(e)
                if e.code == "precondition_failed"
                    && Instant::now() < deadline =>
            {
                sleep(Duration::from_millis(500)).await;
            }
            Err(e) => panic!("{e:?}"),
        }
    };
    let step = &planned["plan"]["steps"][0];
    assert_eq!(step["name"], "create_continuity_record");
    let chain = step["detail"]["chain_id"].as_str().unwrap().to_string();
    assert_eq!(chain.len(), 32);
    let f = lsns.iter().min_by_key(|l| sources_lsn(l)).unwrap().clone();
    assert_eq!(planned["plan"]["bindings"]["f"], f.as_str());
    // Recomputing yields the same chain and proof.
    assert_eq!(
        svc.plan("pgadopt", req()).await.unwrap()["proof"],
        planned["proof"]
    );
    let apply = || ApplyRequest {
        plan: req(),
        expect_proof: planned["proof"].as_str().unwrap().into(),
        actor: "alice".into(),
        reason: "upgrade adoption".into(),
    };
    let caller = || Caller {
        origin: "127.0.0.1:1".into(),
        credential: "token",
    };
    let done = svc.apply("pgadopt", apply(), caller()).await.unwrap();
    assert_eq!(done["state"], "completed");
    let rec: Value = serde_json::from_slice(
        &backend
            .kv_get("failover", "pg_continuity:pg-src")
            .await?
            .unwrap(),
    )?;
    assert_eq!(rec["chain_id"], chain.as_str());
    assert_eq!(rec["transition_id"], 0);
    assert_eq!(rec["proven_at"], f.as_str());
    assert_eq!(
        sink_checkpoints(&backend, "pg-src").await,
        before,
        "nothing moved"
    );
    assert!(incidents.get(&id).await?.unwrap().status.is_resolved());
    let again = svc.apply("pgadopt", apply(), caller()).await.unwrap();
    assert_eq!(again["already_completed"], true);

    // The explicit resume: proven against the record, stamped, streaming.
    mgr.resume("pgadopt").await.unwrap();
    pg_insert(port, ROWS + 1).await;
    hook.until_accepted(ROWS + 1).await;
    for (key, bytes) in sink_checkpoints(&backend, "pg-src").await {
        let cp: Value = serde_json::from_slice(&bytes)?;
        assert_eq!(cp["chain"], chain.as_str(), "{key}");
        assert_eq!(cp["transition"], 0, "{key}");
    }
    Ok(())
}

/// An LSN's numeric order.
fn sources_lsn(l: &str) -> u64 {
    let (hi, lo) = l.split_once('/').unwrap();
    (u64::from_str_radix(hi, 16).unwrap() << 32)
        | u64::from_str_radix(lo, 16).unwrap()
}

/// A stopped PostgreSQL pipeline as an earlier release left it for
/// `pg-adopt-timeline`: no continuity record, unstamped checkpoints, a
/// `timeline_unrecorded` incident (whose id is returned).
async fn adoption_ready(
    mgr: &Arc<PipelineManager>,
    backend: &ArcStorageBackend,
    name: &str,
) -> deltaforge_core::incident::IncidentId {
    use deltaforge_core::incident::{
        ActionCode, CauseCode, Component, EvidenceKey, IncidentDraft,
        ReasonCode, Retryability, SafetyState,
    };
    use rest_api::recovery::RecoveryController;
    PipelineController::stop(mgr.as_ref(), name).await.unwrap();
    until_status(mgr, name, "stopped").await;
    let svc = recovery(mgr);
    let deadline = Instant::now() + Duration::from_secs(60);
    while svc.diagnose(name).await.unwrap()["quiescent"] != true {
        assert!(Instant::now() < deadline, "the pipeline never stopped");
        sleep(Duration::from_millis(200)).await;
    }
    backend
        .kv_delete("failover", "pg_continuity:pg-src")
        .await
        .unwrap();
    for (key, bytes) in sink_checkpoints(backend, "pg-src").await {
        let mut cp: Value = serde_json::from_slice(&bytes).unwrap();
        for k in ["timeline", "chain", "transition"] {
            cp.as_object_mut().unwrap().remove(k);
        }
        backend
            .kv_put("checkpoints", &key, &serde_json::to_vec(&cp).unwrap())
            .await
            .unwrap();
    }
    storage::adapters::incidents::IncidentStore::new(backend.clone(), name)
        .raise(
            &IncidentDraft::new(
                ReasonCode::PgContinuityUnproven,
                Component::Source {
                    id: "pg-src".into(),
                },
                Retryability::OperatorAction,
                SafetyState::HaltedSafe,
                CauseCode::SourceLineage,
            )
            .discriminate("class", "timeline_unrecorded")
            .with_evidence(|e| {
                e.text(EvidenceKey::ReasonClass, "timeline_unrecorded");
            })
            .with_actions(&[ActionCode::AdoptTimeline]),
            1,
        )
        .await
        .unwrap()
        .record()
        .incident_id
        .clone()
}

/// A proof-bound server fact (the timeline) that changes after the plan
/// is never adopted: changed before the apply's recomputation, the proof
/// no longer matches; changed only for the executor's own observation
/// (after the proof matched), the apply stops. Either way no continuity
/// record, no checkpoint or incident change, and replanning shows another
/// proof; the stopped operation then finishes with its own proof once the
/// facts are honest again.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn postgres_adopt_timeline_refuses_a_fact_changed_after_the_plan()
-> Result<()> {
    use rest_api::recovery::{
        ApplyRequest, Caller, PlanRequest, RecoveryController,
    };
    use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
    trace();
    let (_pg, port) = start_postgres().await;
    let (_dir, backend) = sqlite();
    let hook = Hook::start().await;
    let mgr = Arc::new(manager_over(backend.clone()).await);
    let sinks = hooks_yaml(&[("hook", &hook.url, true)]);
    mgr.start_pipeline(pg_spec_with("pgfacts", port, "", &sinks, ""))
        .await?;
    every_row_arrives(&mgr, "pgfacts", &hook).await;
    until_control(&backend, "pg-src", completed).await;
    let id = adoption_ready(&mgr, &backend, "pgfacts").await;

    // 0: honest; 1: every observation shows another timeline; 2: only
    // observations after the first one since the switch.
    let mode = Arc::new(AtomicU8::new(0));
    let seen = Arc::new(AtomicUsize::new(0));
    let facts_hook: sources::postgres::postgres_adoption::FactsHook = {
        let (mode, seen) = (Arc::clone(&mode), Arc::clone(&seen));
        Arc::new(move |f| {
            let n = seen.fetch_add(1, Ordering::SeqCst);
            match mode.load(Ordering::SeqCst) {
                1 => f.timeline += 1,
                2 if n >= 1 => f.timeline += 1,
                _ => {}
            }
        })
    };
    let svc = runner::recovery::RecoveryService::new(Arc::clone(&mgr))
        .with_operation(Arc::new(
        runner::recovery_adopt_timeline::AdoptTimeline::with_session_facts_hook(
            facts_hook,
        ),
    ));
    let req = || PlanRequest {
        operation: "pg-adopt-timeline".into(),
        incident: Some(id.0.clone()),
        args: Default::default(),
    };
    let apply = |proof: &str| ApplyRequest {
        plan: req(),
        expect_proof: proof.into(),
        actor: "alice".into(),
        reason: "adopt".into(),
    };
    let caller = || Caller {
        origin: "127.0.0.1:1".into(),
        credential: "token",
    };
    let deadline = Instant::now() + Duration::from_secs(60);
    let planned = loop {
        match svc.plan("pgfacts", req()).await {
            Ok(p) => break p,
            Err(e)
                if e.code == "precondition_failed"
                    && Instant::now() < deadline =>
            {
                sleep(Duration::from_millis(500)).await;
            }
            Err(e) => panic!("{e:?}"),
        }
    };
    let proof = planned["proof"].as_str().unwrap().to_string();
    let incidents = storage::adapters::incidents::IncidentStore::new(
        backend.clone(),
        "pgfacts",
    );
    let incident_before = incidents.get(&id).await?.unwrap();
    let checkpoints_before = sink_checkpoints(&backend, "pg-src").await;
    let unchanged = |what: &'static str| {
        let (backend, incidents, id) = (&backend, &incidents, &id);
        let (inc, cps) = (&incident_before, &checkpoints_before);
        async move {
            assert!(
                backend
                    .kv_get("failover", "pg_continuity:pg-src")
                    .await
                    .unwrap()
                    .is_none(),
                "{what}: no continuity record"
            );
            assert_eq!(
                &sink_checkpoints(backend, "pg-src").await,
                cps,
                "{what}"
            );
            assert_eq!(
                &incidents.get(id).await.unwrap().unwrap(),
                inc,
                "{what}"
            );
        }
    };

    // Changed before the recomputation: the proof no longer matches.
    mode.store(1, Ordering::SeqCst);
    let e = svc
        .apply("pgfacts", apply(&proof), caller())
        .await
        .unwrap_err();
    assert_eq!(e.code, "proof_mismatch", "{e:?}");
    unchanged("proof mismatch").await;
    let replanned = svc.plan("pgfacts", req()).await.unwrap();
    assert_ne!(replanned["proof"], planned["proof"]);
    assert!(
        storage::adapters::recovery::RecoveryStore::new(
            backend.clone(),
            "pgfacts"
        )
        .read()
        .await?
        .is_none(),
        "nothing claimed"
    );

    // Changed only for the executor's observation: the apply stops.
    seen.store(0, Ordering::SeqCst);
    mode.store(2, Ordering::SeqCst);
    let e = svc
        .apply("pgfacts", apply(&proof), caller())
        .await
        .unwrap_err();
    assert_eq!(e.code, "apply_stopped", "{e:?}");
    unchanged("divergence at apply").await;
    mode.store(1, Ordering::SeqCst);
    assert_ne!(
        svc.plan("pgfacts", req()).await.unwrap()["proof"],
        planned["proof"]
    );

    // Honest again: the pending operation finishes with its own proof.
    mode.store(0, Ordering::SeqCst);
    let done = svc.apply("pgfacts", apply(&proof), caller()).await.unwrap();
    assert_eq!(done["state"], "completed");
    let rec: Value = serde_json::from_slice(
        &backend
            .kv_get("failover", "pg_continuity:pg-src")
            .await?
            .unwrap(),
    )?;
    assert_eq!(rec["transition_id"], 0);
    assert_eq!(
        sink_checkpoints(&backend, "pg-src").await,
        checkpoints_before
    );
    Ok(())
}

/// The consistent point recorded in the slot ownership record.
async fn owner_consistent(backend: &ArcStorageBackend) -> Option<String> {
    backend
        .kv_get("checkpoints", "slot_owner:pg-src")
        .await
        .unwrap()
        .and_then(|b| serde_json::from_slice::<Value>(&b).ok())
        .and_then(|v| v["consistent_lsn"].as_str().map(String::from))
}

/// An authorized slot recreation survives a stop at either crash point of
/// its start: after the authorization was taken and before the slot was
/// created, the same generation finishes the creation; after the slot was
/// created and before the completion was recorded, it keeps exactly that
/// slot (not created again) and repairs the audit. The authorization stays
/// bound to the generation throughout.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn postgres_an_authorized_slot_recreation_survives_a_crash_at_either_point()
-> Result<()> {
    use sources::snapshot_probe::{SlotRecreationPoint, hold_slot_recreation};
    use sources::snapshot_recovery::{
        SlotRecreationState, read_slot_recreation,
    };
    trace();
    for point in [
        SlotRecreationPoint::BeforeCreate,
        SlotRecreationPoint::AfterCreate,
    ] {
        let (_pg, port) = start_postgres().await;
        let (_dir, backend) = sqlite();
        let hook = Hook::start().await;
        let mgr = Arc::new(manager_over(backend.clone()).await);
        let sinks = hooks_yaml(&[("hook", &hook.url, true)]);
        mgr.start_pipeline(pg_spec_with("pgcrash", port, "", &sinks, ""))
            .await?;
        let blocked = blocked_after_a_completed_snapshot(
            &mgr, &backend, "pgcrash", "pg-src", &hook,
        )
        .await;
        let admin = pg_admin(port).await;
        let deadline = Instant::now() + Duration::from_secs(30);
        while admin
            .execute("SELECT pg_drop_replication_slot('snap_slot')", &[])
            .await
            .is_err()
        {
            assert!(Instant::now() < deadline, "the slot was never dropped");
            sleep(Duration::from_millis(200)).await;
        }
        let svc = recovery(&mgr);
        let incident = resnapshot_incident(&svc, "pgcrash").await;
        let (planned, _) =
            plan_and_apply(&svc, "pgcrash", Some(incident)).await;
        assert!(
            step_names(&planned)
                .contains(&"authorize_slot_recreation".to_string())
        );
        let generation = blocked.generation + 1;
        let (_, authorized) =
            read_slot_recreation(&backend, "pg-src").await?.unwrap();
        assert_eq!(authorized.state, SlotRecreationState::Authorized);
        assert_eq!(authorized.generation, generation);
        let bound = |r: &sources::snapshot_recovery::SlotRecreation| {
            (
                r.source.clone(),
                r.pipeline.clone(),
                r.slot.clone(),
                r.generation,
                r.proof.clone(),
                r.owner_record.clone(),
            )
        };

        // The start stops at the crash point.
        let (reached, release, crash) = hold_slot_recreation(point);
        mgr.resume("pgcrash").await.unwrap();
        tokio::time::timeout(Duration::from_secs(120), reached.notified())
            .await
            .unwrap_or_else(|_| panic!("{point:?} never reached"));
        crash.store(true, std::sync::atomic::Ordering::SeqCst);
        release.notify_one();
        until_status(&mgr, "pgcrash", "failed").await;
        let (_, auth) =
            read_slot_recreation(&backend, "pg-src").await?.unwrap();
        assert_eq!(auth.state, SlotRecreationState::Consumed, "{point:?}");
        assert_eq!(
            bound(&auth),
            bound(&authorized),
            "{point:?}: the same binding"
        );
        assert_eq!(auth.generation, generation, "{point:?}: still bound");
        assert_eq!(auth.proof, planned["proof"].as_str().unwrap(), "{point:?}");
        let consistent_at_crash = owner_consistent(&backend).await;
        assert_eq!(
            slot_exists(&admin).await,
            point == SlotRecreationPoint::AfterCreate,
            "{point:?}"
        );
        let trail = storage::adapters::recovery::RecoveryStore::new(
            backend.clone(),
            "pgcrash",
        )
        .audit_trail(50)
        .await?;
        assert!(
            !trail.iter().any(|e| e.event == "slot_recreated"),
            "{point:?}: not audited yet"
        );

        // The restart finishes it, in the same generation.
        let reads = hook.snapshot_reads();
        mgr.resume("pgcrash").await.unwrap();
        let done = until_control(&backend, "pg-src", completed).await;
        assert_eq!(
            done.generation, generation,
            "{point:?}: no further generation"
        );
        let deadline = Instant::now() + Duration::from_secs(60);
        while hook.snapshot_reads() < reads + ROWS as usize {
            assert!(Instant::now() < deadline, "{point:?}: not copied again");
            sleep(Duration::from_millis(200)).await;
        }
        let (_, auth) =
            read_slot_recreation(&backend, "pg-src").await?.unwrap();
        assert_eq!(auth.state, SlotRecreationState::Created, "{point:?}");
        assert_eq!(auth.generation, generation);
        assert_eq!(
            bound(&auth),
            bound(&authorized),
            "{point:?}: the same binding"
        );
        let created = auth.created.clone().unwrap();
        let (created_lsn, created_at) =
            (created.consistent_lsn.clone(), created.at_ms);
        let anchor = match done.anchor.as_ref() {
            Some(sources::snapshot_queue::EngineAnchor::Postgres {
                lsn,
                ..
            }) => lsn.clone(),
            other => panic!("{other:?}"),
        };
        assert_eq!(
            anchor, created.consistent_lsn,
            "{point:?}: the anchor is the slot's point"
        );
        if point == SlotRecreationPoint::AfterCreate {
            assert_eq!(
                Some(created.consistent_lsn.clone()),
                consistent_at_crash,
                "the slot created before the stop is kept, not recreated"
            );
        }
        assert_eq!(
            owner_consistent(&backend).await,
            Some(created.consistent_lsn)
        );
        let trail = storage::adapters::recovery::RecoveryStore::new(
            backend.clone(),
            "pgcrash",
        )
        .audit_trail(50)
        .await?;
        assert_eq!(
            trail.iter().filter(|e| e.event == "slot_recreated").count(),
            1,
            "{point:?}: audited once"
        );
        // Byte-stable: the same event appended again (as a repair would)
        // is accepted as identical and adds nothing.
        let store = storage::adapters::recovery::RecoveryStore::new(
            backend.clone(),
            "pgcrash",
        );
        store
            .append_event(
                &auth.proof,
                "slot_recreated",
                std::collections::BTreeMap::from([
                    ("slot".to_string(), "snap_slot".to_string()),
                    ("generation".to_string(), generation.to_string()),
                    ("consistent_lsn".to_string(), created_lsn.clone()),
                ]),
                created_at,
            )
            .await?;
        assert_eq!(
            store
                .audit_trail(50)
                .await?
                .iter()
                .filter(|e| e.event == "slot_recreated")
                .count(),
            1
        );
        PipelineController::stop(mgr.as_ref(), "pgcrash").await?;
    }
    Ok(())
}
