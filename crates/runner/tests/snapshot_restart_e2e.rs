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
