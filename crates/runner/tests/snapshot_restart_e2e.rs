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
