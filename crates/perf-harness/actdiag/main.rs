//! `actdiag`: the forward-proof diagnosis comparisons
//! (`docs/design/mysql-ddl-proof-transactions.md`, sections 6 to 8). The
//! README beside this file describes the comparisons, what each run retains
//! and how to rerun them.
//!
//! Every repetition starts fresh: its own MySQL server and state store
//! (data on tmpfs, so nothing is left behind), its own runner process and
//! Kafka topic. Builds alternate in rotated order (ABC, BCA, CAB, ...).
//! Every row the harness commits is in its ledger; a run ends only when
//! every sink's durable checkpoint covers MySQL's final GTID set and the
//! delivered rows equal the ledger, or fails at its drain timeout.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, Result, anyhow, bail};
use clap::Parser;
use gate_ownership::GateOwned;
use mysql_async::prelude::Queryable;
use rdkafka::ClientConfig;
use rdkafka::Message;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::consumer::{Consumer, StreamConsumer};
use serde_json::{Value, json};
use testcontainers::core::{IntoContainerPort, Mount, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

const KAFKA_NAME: &str = "df-actdiag-kafka";
const KAFKA_HOST_PORT: u16 = 9497;
const API: &str = "127.0.0.1:18180";
const METRICS: &str = "127.0.0.1:19180";
const PW: &str = "pw";
const DB: &str = "actdiag";
const LOADGEN: &str = "loadgen";
/// Load writers; table `i` belongs to writer `i % WRITERS`.
const WRITERS: usize = 64;
/// A run's ids: `seq * RUN_IDS` plus a counter.
const RUN_IDS: i64 = 1_000_000_000;
/// Marker rows: offsets from here, in the control table.
const MARKER: i64 = 900_000_000;

/// The images (as `canonical-perf` pins them).
const MYSQL_IMAGE: (&str, &str) = (
    "mysql",
    "8.4@sha256:c36050afdca850f23cef85703f84c7531a5ae155a11b5ee1c60acb09937c4084",
);
const POSTGRES_IMAGE: (&str, &str) = (
    "postgres",
    "17@sha256:a426e44bac0b759c95894d68e1a0ac03ecc20b619f498a91aae373bf06d8508d",
);
const KAFKA_IMAGE: (&str, &str) = (
    "confluentinc/cp-kafka",
    "7.5.0@sha256:fbbb6fa11b258a88b83f54d4f0bddfcffbf2279f99d66a843486e3da7bdfbf41",
);

#[derive(Parser, Debug)]
#[command(about = "Forward-proof diagnosis comparisons")]
struct Args {
    /// `label=dir`: a build directory holding `runner` and `identity.json`
    /// (`build.sh`). The same directory may appear under two labels (a
    /// build against itself).
    #[arg(long = "build", required = true)]
    builds: Vec<String>,
    /// The comparisons: ddl-unloaded, ddl-loaded, ddl-burst, act-fresh,
    /// act-burst, act-restart.
    #[arg(long, value_delimiter = ',', required = true)]
    comparisons: Vec<String>,
    /// DeltaForge's state store: `postgres` or `sqlite`.
    #[arg(long, default_value = "postgres")]
    store: String,
    /// Repetitions per build of every comparison.
    #[arg(long, default_value_t = 3)]
    reps: usize,
    /// Output directory (created; must not hold a previous series).
    #[arg(long)]
    out: PathBuf,
    /// Scale the DDL, table and row counts (a smoke run uses a fraction).
    #[arg(long, default_value_t = 1.0)]
    scale: f64,
    /// The seed of every schedule.
    #[arg(long, default_value_t = 1)]
    seed: u64,
    /// The drain timeout, seconds (default: the comparison's own).
    #[arg(long)]
    drain_timeout_s: Option<u64>,
}

// ---------------------------------------------------------------------------
// Comparisons
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DdlKind {
    /// `CREATE TABLE` of a new table, then a row into it.
    Create,
    /// Alternately a `CREATE TABLE` and an `ALTER TABLE .. ADD COLUMN` of a
    /// cached idle table, each altered once (its row through its writer):
    /// a table altered again before its proof runs keeps that DDL pending,
    /// which is a different path (the forward-proof failure follow-up).
    Alternate,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Activation {
    /// One first row per table at `rate`, then ordinary rows.
    Fresh,
    /// Every table's first row at once.
    Burst,
    /// Activated, drained, the runner restarted, then a row per table.
    Restart,
}

#[derive(Clone, Debug)]
struct Comparison {
    name: &'static str,
    tables: usize,
    busy: usize,
    /// Rows/s over the busy tables while DDLs run.
    rate: f64,
    ddls: usize,
    ddl: DdlKind,
    activation: Option<Activation>,
    /// Rows/s of an activation's first and ordinary rows.
    act_rate: f64,
    /// Seconds of ordinary rows after an activation.
    ordinary_s: f64,
    drain_timeout: Duration,
}

fn comparison(name: &str, scale: f64) -> Result<Comparison> {
    let n = |v: usize| ((v as f64 * scale).ceil() as usize).max(1);
    let ddl = |name, tables, busy, rate, ddls, ddl| Comparison {
        name,
        tables: n(tables),
        busy: n(busy).min(n(tables)),
        rate,
        ddls: n(ddls),
        ddl,
        activation: None,
        act_rate: 0.0,
        ordinary_s: 0.0,
        drain_timeout: Duration::from_secs(600),
    };
    let act = |name, a| Comparison {
        name,
        tables: n(200),
        busy: 0,
        rate: 0.0,
        ddls: 0,
        ddl: DdlKind::Create,
        activation: Some(a),
        act_rate: 400.0,
        ordinary_s: 10.0,
        drain_timeout: Duration::from_secs(300),
    };
    Ok(match name {
        "ddl-unloaded" => {
            ddl("ddl-unloaded", 100, 0, 0.0, 300, DdlKind::Create)
        }
        "ddl-loaded" => {
            ddl("ddl-loaded", 1000, 150, 2000.0, 1000, DdlKind::Alternate)
        }
        "ddl-burst" => {
            ddl("ddl-burst", 150, 150, 2000.0, 4200, DdlKind::Create)
        }
        "act-fresh" => act("act-fresh", Activation::Fresh),
        "act-burst" => act("act-burst", Activation::Burst),
        "act-restart" => act("act-restart", Activation::Restart),
        other => bail!("unknown comparison {other}"),
    })
}

// ---------------------------------------------------------------------------
// Infrastructure
// ---------------------------------------------------------------------------

struct Build {
    label: String,
    dir: PathBuf,
    identity: Value,
}

async fn start_kafka() -> Result<ContainerAsync<GenericImage>> {
    std::process::Command::new("docker")
        .args(["rm", "-f", KAFKA_NAME])
        .output()
        .ok();
    let c = GenericImage::new(KAFKA_IMAGE.0, KAFKA_IMAGE.1)
        .with_wait_for(WaitFor::Duration {
            length: Duration::from_secs(15),
        })
        .with_container_name(KAFKA_NAME)
        .with_env_var("KAFKA_NODE_ID", "1")
        .with_env_var("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env_var("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:29093")
        .with_env_var(
            "KAFKA_LISTENERS",
            format!(
                "PLAINTEXT://0.0.0.0:29092,CONTROLLER://0.0.0.0:29093,EXTERNAL://0.0.0.0:{KAFKA_HOST_PORT}"
            ),
        )
        .with_env_var(
            "KAFKA_ADVERTISED_LISTENERS",
            format!(
                "PLAINTEXT://localhost:29092,EXTERNAL://localhost:{KAFKA_HOST_PORT}"
            ),
        )
        .with_env_var(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT",
        )
        .with_env_var("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env_var("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
        .with_env_var("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .with_env_var("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .with_env_var("CLUSTER_ID", "QWN0RGlhZ0thZmthMDAwMQ")
        // The image runs as appuser: a tmpfs it can write.
        .with_mount(Mount::tmpfs_mount("/var/lib/kafka/data").with_mode(0o1777))
        .with_mount(Mount::tmpfs_mount("/etc/kafka/secrets").with_mode(0o1777))
        .with_mapped_port(KAFKA_HOST_PORT, KAFKA_HOST_PORT.tcp())
        .gate_owned()
        .start()
        .await
        .context("start kafka")?;
    Ok(c)
}

fn brokers() -> String {
    format!("127.0.0.1:{KAFKA_HOST_PORT}")
}

async fn create_topic(topic: &str) -> Result<()> {
    let admin: AdminClient<rdkafka::client::DefaultClientContext> =
        ClientConfig::new()
            .set("bootstrap.servers", brokers())
            .create()?;
    let res = admin
        .create_topics(
            &[NewTopic::new(topic, 1, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await?;
    for r in res {
        r.map_err(|(t, e)| anyhow!("create topic {t}: {e}"))?;
    }
    Ok(())
}

/// The containers this invocation created, by exact ID.
static CREATED: Mutex<Vec<String>> = Mutex::new(Vec::new());

fn created(id: &str) {
    CREATED.lock().unwrap().push(id.to_string());
}

/// Remove every container this invocation created that still exists, with
/// any anonymous volume (there is none: data is on tmpfs).
fn remove_created() {
    let ids: Vec<String> = CREATED.lock().unwrap().drain(..).collect();
    for id in ids {
        std::process::Command::new("docker")
            .args(["rm", "-f", "-v", &id])
            .output()
            .ok();
    }
}

/// One repetition's MySQL server and state store.
struct Fresh {
    mysql: ContainerAsync<GenericImage>,
    state: Option<ContainerAsync<GenericImage>>,
}

impl Fresh {
    async fn start(postgres: bool) -> Result<Fresh> {
        let mysql = GenericImage::new(MYSQL_IMAGE.0, MYSQL_IMAGE.1)
            .with_wait_for(WaitFor::message_on_stderr(
                "ready for connections. Version: '8.4",
            ))
            .with_env_var("MYSQL_ROOT_PASSWORD", PW)
            .with_cmd(vec![
                "--server-id=70",
                "--log-bin=mysql-bin",
                "--binlog-format=ROW",
                "--binlog-row-image=FULL",
                "--gtid-mode=ON",
                "--enforce-gtid-consistency=ON",
                "--max-allowed-packet=256M",
                "--max-connections=500",
            ])
            .with_mount(Mount::tmpfs_mount("/var/lib/mysql"))
            .gate_owned()
            .start()
            .await
            .context("start mysql")?;
        let state = if postgres {
            Some(
                GenericImage::new(POSTGRES_IMAGE.0, POSTGRES_IMAGE.1)
                    .with_wait_for(WaitFor::message_on_stderr(
                        "database system is ready to accept connections",
                    ))
                    .with_env_var("POSTGRES_PASSWORD", PW)
                    .with_cmd([
                        "postgres",
                        "-c",
                        "shared_preload_libraries=pg_stat_statements",
                        "-c",
                        "pg_stat_statements.max=10000",
                        "-c",
                        "max_connections=300",
                    ])
                    .with_mount(Mount::tmpfs_mount("/var/lib/postgresql/data"))
                    .gate_owned()
                    .start()
                    .await
                    .context("start state postgres")?,
            )
        } else {
            None
        };
        created(mysql.id());
        if let Some(c) = &state {
            created(c.id());
        }
        let fresh = Fresh { mysql, state };
        // The images restart once after initializing: wait for a session.
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            if let Ok(mut c) =
                mysql_async::Conn::from_url(fresh.mysql_dsn().await?).await
                && c.query_drop("SELECT 1").await.is_ok()
            {
                c.disconnect().await.ok();
                break;
            }
            if Instant::now() > deadline {
                bail!("mysql never accepted a session");
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        if fresh.state.is_some() {
            let deadline = Instant::now() + Duration::from_secs(120);
            loop {
                if let Ok(c) = fresh.state_client().await
                    && c.batch_execute(
                        "CREATE EXTENSION IF NOT EXISTS pg_stat_statements",
                    )
                    .await
                    .is_ok()
                {
                    break;
                }
                if Instant::now() > deadline {
                    bail!("the state store never accepted a session");
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
        Ok(fresh)
    }

    async fn mysql_dsn(&self) -> Result<String> {
        let port = self.mysql.get_host_port_ipv4(3306).await?;
        Ok(format!("mysql://root:{PW}@127.0.0.1:{port}/"))
    }

    async fn loadgen_dsn(&self) -> Result<String> {
        let port = self.mysql.get_host_port_ipv4(3306).await?;
        Ok(format!("mysql://{LOADGEN}:{PW}@127.0.0.1:{port}/"))
    }

    async fn state_dsn(&self) -> Result<Option<String>> {
        Ok(match &self.state {
            Some(c) => Some(format!(
                "host=127.0.0.1 port={} user=postgres password={PW} dbname=postgres",
                c.get_host_port_ipv4(5432).await?
            )),
            None => None,
        })
    }

    async fn state_client(&self) -> Result<tokio_postgres::Client> {
        let dsn = self
            .state_dsn()
            .await?
            .ok_or_else(|| anyhow!("no state store"))?;
        let (client, conn) =
            tokio_postgres::connect(&dsn, tokio_postgres::NoTls).await?;
        tokio::spawn(conn);
        Ok(client)
    }

    fn ids(&self) -> Vec<String> {
        std::iter::once(self.mysql.id().to_string())
            .chain(self.state.iter().map(|c| c.id().to_string()))
            .collect()
    }
}

/// A runner process.
struct Runner {
    child: tokio::process::Child,
    pid: u32,
}

fn port_in_use(addr: &str) -> bool {
    std::net::TcpListener::bind(addr).is_err()
}

async fn launch(
    build: &Build,
    dir: &Path,
    state_dsn: Option<&str>,
    n: usize,
) -> Result<Runner> {
    std::fs::create_dir_all(dir)?;
    for addr in [API, METRICS] {
        let deadline = Instant::now() + Duration::from_secs(30);
        while port_in_use(addr) {
            if Instant::now() > deadline {
                bail!("{addr} is still in use");
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
    let mut cmd = tokio::process::Command::new(build.dir.join("runner"));
    cmd.args(["--api-addr", API, "--metrics-addr", METRICS]);
    match state_dsn {
        Some(dsn) => {
            cmd.args(["--storage-backend", "postgres", "--storage-dsn", dsn])
        }
        None => cmd.arg("--storage-path").arg(dir.join("state.db")),
    };
    let child = cmd
        .env("RUST_LOG", "warn")
        .stdout(std::fs::File::create(dir.join(format!("runner-{n}.out")))?)
        .stderr(std::fs::File::create(dir.join(format!("runner-{n}.err")))?)
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("start {}", build.dir.display()))?;
    let pid = child.id().ok_or_else(|| anyhow!("no pid"))?;
    let mut runner = Runner { child, pid };
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(status) = runner.child.try_wait()? {
            bail!("the runner exited before becoming healthy: {status}");
        }
        if reqwest::get(format!("http://{API}/health"))
            .await
            .is_ok_and(|r| r.status().is_success())
        {
            return Ok(runner);
        }
        if Instant::now() > deadline {
            bail!("the runner never became healthy");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

impl Runner {
    /// SIGTERM, then SIGKILL after 30 s.
    async fn down(mut self) -> Result<()> {
        std::process::Command::new("kill")
            .args(["-TERM", &self.pid.to_string()])
            .output()
            .ok();
        if tokio::time::timeout(Duration::from_secs(30), self.child.wait())
            .await
            .is_err()
        {
            self.child.kill().await.ok();
        }
        Ok(())
    }
}

async fn api(
    method: reqwest::Method,
    path: &str,
    body: Option<&Value>,
) -> Result<Value> {
    let client = reqwest::Client::new();
    let mut req = client.request(method, format!("http://{API}{path}"));
    if let Some(b) = body {
        req = req.json(b);
    }
    let resp = req.send().await?;
    let status = resp.status();
    let text = resp.text().await?;
    if !status.is_success() {
        bail!("{path}: {status}: {text}");
    }
    Ok(serde_json::from_str(&text).unwrap_or(Value::Null))
}

/// The blocking incidents of a pipeline that failed, or `None` while it
/// has not.
async fn failure(name: &str) -> Option<String> {
    let v = api(reqwest::Method::GET, &format!("/pipelines/{name}"), None)
        .await
        .ok()?;
    (v["status"] == "failed").then(|| v["incidents"]["blocking"].to_string())
}

async fn wait_running(name: &str) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(v) =
            api(reqwest::Method::GET, &format!("/pipelines/{name}"), None).await
            && v.to_string().to_lowercase().contains("\"running\"")
        {
            return Ok(());
        }
        if Instant::now() > deadline {
            bail!("pipeline {name} never ran");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn metrics_text() -> String {
    match reqwest::get(format!("http://{METRICS}/metrics")).await {
        Ok(r) => r.text().await.unwrap_or_default(),
        Err(_) => String::new(),
    }
}

/// The phase histograms of a scrape: per phase, its bucket counts by upper
/// bound, its sum and its count.
fn phases(text: &str) -> BTreeMap<String, Value> {
    let mut out: BTreeMap<String, Value> = BTreeMap::new();
    let base = "deltaforge_perf_phase_latency_seconds";
    for line in text.lines() {
        let Some(rest) = line.strip_prefix(base) else {
            continue;
        };
        let (kind, rest) = if let Some(r) = rest.strip_prefix("_bucket") {
            ("bucket", r)
        } else if let Some(r) = rest.strip_prefix("_sum") {
            ("sum", r)
        } else if let Some(r) = rest.strip_prefix("_count") {
            ("count", r)
        } else {
            continue;
        };
        let Some((labels, value)) = rest.rsplit_once(' ') else {
            continue;
        };
        let Ok(value) = value.parse::<f64>() else {
            continue;
        };
        let label = |k: &str| {
            labels
                .split(&[',', '{', '}'][..])
                .find_map(|kv| kv.strip_prefix(&format!("{k}=\"")))
                .map(|v| v.trim_end_matches('"').to_string())
        };
        let Some(phase) = label("phase") else {
            continue;
        };
        let e = out.entry(phase).or_insert_with(|| json!({"buckets": {}}));
        match kind {
            "bucket" => {
                if let Some(le) = label("le") {
                    e["buckets"][le] = json!(value);
                }
            }
            k => e[k] = json!(value),
        }
    }
    out
}

/// Counters of interest, summed over labels.
fn counters(text: &str) -> BTreeMap<&'static str, f64> {
    const NAMES: [&str; 6] = [
        "deltaforge_mysql_selection_cache_evictions_total",
        "deltaforge_mysql_proof_requests_total",
        "deltaforge_mysql_proof_scan_seconds_sum",
        "deltaforge_mysql_proof_scan_seconds_count",
        "deltaforge_checkpoints_total",
        "deltaforge_source_events_total",
    ];
    NAMES
        .iter()
        .map(|name| {
            let v = text
                .lines()
                .filter(|l| {
                    l.strip_prefix(name).is_some_and(|r| {
                        r.starts_with('{') || r.starts_with(' ')
                    })
                })
                .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
                .sum();
            (*name, v)
        })
        .collect()
}

/// The state store's counters (PostgreSQL): committed transactions and the
/// store lock's calls.
async fn store_counters(fresh: &Fresh) -> Result<Option<Value>> {
    if fresh.state.is_none() {
        return Ok(None);
    }
    let c = fresh.state_client().await?;
    let commits: i64 = c
        .query_one(
            "SELECT xact_commit FROM pg_stat_database WHERE datname = 'postgres'",
            &[],
        )
        .await?
        .get(0);
    let rows = c
        .query(
            "SELECT query, calls FROM pg_stat_statements ORDER BY calls DESC",
            &[],
        )
        .await?;
    let mut locks = 0i64;
    let mut statements = 0i64;
    let mut commit_statements = 0i64;
    for r in rows {
        let q: String = r.get(0);
        let calls: i64 = r.get(1);
        statements += calls;
        if q.contains("pg_advisory_xact_lock") {
            locks += calls;
        }
        if q.trim().eq_ignore_ascii_case("commit") {
            commit_statements += calls;
        }
    }
    // `xact_commit` is published with a delay; the COMMIT statements are
    // exact (explicit transactions only).
    Ok(Some(json!({
        "xact_commit": commits,
        "commit_statements": commit_statements,
        "statements": statements,
        "advisory_xact_locks": locks,
    })))
}

async fn reset_store_counters(fresh: &Fresh) -> Result<()> {
    if fresh.state.is_some() {
        let c = fresh.state_client().await?;
        c.batch_execute("SELECT pg_stat_statements_reset()").await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Ledger and consumer
// ---------------------------------------------------------------------------

/// A committed row.
#[derive(Clone, Copy)]
struct Commit {
    id: i64,
    table: u32,
    at: Instant,
    /// A DDL's row (the first after the DDL in its table).
    ddl: bool,
}

#[derive(Default)]
struct Seen {
    first: HashMap<i64, Instant>,
    /// Per table, ids in arrival order.
    order: HashMap<u32, Vec<i64>>,
    duplicates: u64,
    unknown: u64,
}

fn table_name(i: usize) -> String {
    format!("{DB}.t{i:05}")
}

fn new_table_name(i: usize) -> String {
    format!("{DB}.n{i:05}")
}

/// Table numbers: fixture tables `0..`, new tables from `NEW_BASE`, the
/// control table `CONTROL`.
const NEW_BASE: u32 = 1_000_000;
const CONTROL: u32 = u32::MAX;

fn table_number(db: &str, table: &str) -> Option<u32> {
    if db != DB {
        return None;
    }
    if table == "ctl" {
        return Some(CONTROL);
    }
    if let Some(n) = table.strip_prefix('t') {
        return n.parse().ok();
    }
    table
        .strip_prefix('n')
        .and_then(|n| n.parse::<u32>().ok())
        .map(|n| NEW_BASE + n)
}

fn consume(
    topic: &str,
    seen: Arc<Mutex<Seen>>,
    stop: Arc<AtomicBool>,
) -> Result<tokio::task::JoinHandle<()>> {
    let c: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .set("group.id", format!("actdiag-{topic}"))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("fetch.wait.max.ms", "10")
        .create()?;
    c.subscribe(&[topic])?;
    Ok(tokio::spawn(async move {
        while !stop.load(Ordering::Relaxed) {
            let m = match tokio::time::timeout(
                Duration::from_millis(200),
                c.recv(),
            )
            .await
            {
                Ok(Ok(m)) => m,
                Ok(Err(_)) => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
                Err(_) => continue,
            };
            let now = Instant::now();
            let Some(payload) = m.payload() else { continue };
            let Ok(v) = serde_json::from_slice::<Value>(payload) else {
                continue;
            };
            let id = match &v["after"]["id"] {
                Value::Number(n) => n.as_i64(),
                Value::String(t) => t.parse().ok(),
                _ => None,
            };
            let Some(id) = id else { continue };
            let mut s = seen.lock().unwrap();
            let Some(table) = table_number(
                v["source"]["db"].as_str().unwrap_or_default(),
                v["source"]["table"].as_str().unwrap_or_default(),
            ) else {
                s.unknown += 1;
                continue;
            };
            if s.first.contains_key(&id) {
                s.duplicates += 1;
                continue;
            }
            s.first.insert(id, now);
            s.order.entry(table).or_default().push(id);
        }
    }))
}

/// The ledger against what arrived: missing rows, duplicates, rows of
/// unknown tables, and tables whose rows arrived out of commit order.
fn reconcile(commits: &[Commit], seen: &Seen) -> Value {
    let mut by_table: HashMap<u32, Vec<i64>> = HashMap::new();
    for c in commits {
        by_table.entry(c.table).or_default().push(c.id);
    }
    let missing = commits
        .iter()
        .filter(|c| !seen.first.contains_key(&c.id))
        .count();
    let ledger: std::collections::HashSet<i64> =
        commits.iter().map(|c| c.id).collect();
    let extra = seen.first.keys().filter(|id| !ledger.contains(id)).count();
    let mut reordered_tables = 0;
    for (t, ids) in &by_table {
        let arrived = seen.order.get(t).map(Vec::as_slice).unwrap_or(&[]);
        let delivered: Vec<i64> = ids
            .iter()
            .copied()
            .filter(|id| seen.first.contains_key(id))
            .collect();
        let arrived: Vec<i64> = arrived
            .iter()
            .copied()
            .filter(|id| ledger.contains(id))
            .collect();
        if arrived != delivered {
            reordered_tables += 1;
        }
    }
    json!({
        "committed": commits.len(),
        "delivered": seen.first.len(),
        "missing": missing,
        "extra": extra,
        "duplicates": seen.duplicates,
        "unknown_table": seen.unknown,
        "reordered_tables": reordered_tables,
        "equal": missing == 0 && extra == 0 && seen.duplicates == 0
            && reordered_tables == 0,
    })
}

fn quantiles(mut v: Vec<f64>) -> Value {
    if v.is_empty() {
        return Value::Null;
    }
    v.sort_by(|a, b| a.total_cmp(b));
    let q = |p: f64| v[((v.len() - 1) as f64 * p).round() as usize];
    json!({
        "n": v.len(),
        "p50": q(0.50),
        "p99": q(0.99),
        "max": v[v.len() - 1],
        "mean": v.iter().sum::<f64>() / v.len() as f64,
    })
}

// ---------------------------------------------------------------------------
// A run
// ---------------------------------------------------------------------------

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

async fn fixture(fresh: &Fresh, tables: usize) -> Result<()> {
    let mut c = mysql_async::Conn::from_url(fresh.mysql_dsn().await?).await?;
    c.query_drop(format!("CREATE USER '{LOADGEN}'@'%' IDENTIFIED BY '{PW}'"))
        .await?;
    c.query_drop(format!("GRANT INSERT ON *.* TO '{LOADGEN}'@'%'"))
        .await?;
    c.query_drop("SET GLOBAL sync_binlog = 0").await?;
    c.query_drop("SET GLOBAL innodb_flush_log_at_trx_commit = 2")
        .await?;
    c.query_drop(format!("CREATE DATABASE {DB}")).await?;
    c.query_drop(format!(
        "CREATE TABLE {DB}.ctl (id BIGINT PRIMARY KEY, a INT, b VARCHAR(32), c DOUBLE)"
    ))
    .await?;
    c.disconnect().await?;
    let mut tasks = Vec::new();
    for w in 0..8 {
        let dsn = fresh.mysql_dsn().await?;
        tasks.push(tokio::spawn(async move {
            let mut c = mysql_async::Conn::from_url(dsn).await?;
            for i in (w..tables).step_by(8) {
                c.query_drop(format!(
                    "CREATE TABLE {} (id BIGINT PRIMARY KEY, a INT, b VARCHAR(32), c DOUBLE)",
                    table_name(i)
                ))
                .await?;
            }
            c.disconnect().await?;
            Ok::<_, anyhow::Error>(())
        }));
    }
    for t in tasks {
        t.await??;
    }
    Ok(())
}

fn spec(name: &str, dsn: &str, topic: &str) -> Value {
    json!({
        "apiVersion": "deltaforge/v1",
        "kind": "Pipeline",
        "metadata": {"name": name, "tenant": "perf"},
        "spec": {
            "source": {"type": "mysql", "config": {
                "id": name, "dsn": dsn,
                "tables": [format!("{DB}.*")], "snapshot": {"mode": "never"}}},
            "processors": [],
            "sinks": [{"type": "kafka", "config": {
                "id": "kafka", "brokers": brokers(), "topic": topic,
                "envelope": {"type": "native"}, "encoding": "json",
                "required": true}}],
        },
    })
}

/// The run's shared state: the ledger, what arrived, the id counter.
struct Live {
    commits: Arc<Mutex<Vec<Commit>>>,
    seen: Arc<Mutex<Seen>>,
    next: Arc<std::sync::atomic::AtomicI64>,
    base: i64,
}

impl Live {
    fn id(&self) -> i64 {
        self.base + self.next.fetch_add(1, Ordering::SeqCst)
    }

    fn arrived(&self, id: i64) -> Option<Instant> {
        self.seen.lock().unwrap().first.get(&id).copied()
    }
}

/// Insert a marker into the control table until one arrives; the time from
/// the first marker.
async fn marker(
    fresh: &Fresh,
    name: &str,
    live: &Live,
    timeout: Duration,
) -> Result<f64> {
    let mut c = mysql_async::Conn::from_url(fresh.loadgen_dsn().await?).await?;
    let t0 = Instant::now();
    loop {
        let id = live.base + MARKER + live.next.fetch_add(1, Ordering::SeqCst);
        c.query_drop(format!(
            "INSERT INTO {DB}.ctl (id, a, b, c) VALUES ({id}, 0, 'marker', 0)"
        ))
        .await?;
        live.commits.lock().unwrap().push(Commit {
            id,
            table: CONTROL,
            at: Instant::now(),
            ddl: false,
        });
        let wait = Instant::now() + Duration::from_secs(5);
        while Instant::now() < wait {
            if live.arrived(id).is_some() {
                c.disconnect().await.ok();
                return Ok(t0.elapsed().as_secs_f64());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        if let Some(f) = failure(name).await {
            bail!("the pipeline failed: {f}");
        }
        if t0.elapsed() > timeout {
            bail!("no marker arrived within {timeout:?}");
        }
    }
}

/// Load writers: each owns the tables `i % WRITERS`; `(id, table, ddl)`
/// sent to a table's writer is committed in order.
struct Writers {
    queues: Vec<tokio::sync::mpsc::UnboundedSender<(i64, u32, bool)>>,
    tasks: Vec<tokio::task::JoinHandle<Result<()>>>,
}

impl Writers {
    async fn start(fresh: &Fresh, live: &Live, n: usize) -> Result<Writers> {
        let mut queues = Vec::new();
        let mut tasks = Vec::new();
        for _ in 0..n.max(1) {
            let mut c =
                mysql_async::Conn::from_url(fresh.loadgen_dsn().await?).await?;
            let (tx, mut rx) =
                tokio::sync::mpsc::unbounded_channel::<(i64, u32, bool)>();
            queues.push(tx);
            let commits = Arc::clone(&live.commits);
            tasks.push(tokio::spawn(async move {
                while let Some((id, table, ddl)) = rx.recv().await {
                    let name = if table >= NEW_BASE {
                        new_table_name((table - NEW_BASE) as usize)
                    } else {
                        table_name(table as usize)
                    };
                    c.query_drop(format!(
                        "INSERT INTO {name} (id, a, b, c) VALUES ({id}, 1, 'r', 1.5)"
                    ))
                    .await?;
                    commits.lock().unwrap().push(Commit {
                        id,
                        table,
                        at: Instant::now(),
                        ddl,
                    });
                }
                c.disconnect().await.ok();
                Ok(())
            }));
        }
        Ok(Writers { queues, tasks })
    }

    fn send(&self, id: i64, table: u32, ddl: bool) -> Result<()> {
        let w = table as usize % self.queues.len();
        self.queues[w]
            .send((id, table, ddl))
            .map_err(|_| anyhow!("writer {w} stopped"))
    }

    async fn finish(self) -> Result<()> {
        drop(self.queues);
        for t in self.tasks {
            t.await??;
        }
        Ok(())
    }
}

/// Offer `rate` rows/s over `tables` (by number) until `halt`.
fn load(
    writers: Arc<Writers>,
    live_next: Arc<std::sync::atomic::AtomicI64>,
    base: i64,
    tables: Vec<u32>,
    rate: f64,
    seed: u64,
    halt: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let mut rng = Rng(seed.max(1));
        let start = Instant::now();
        let mut due = Duration::ZERO;
        while !halt.load(Ordering::Relaxed) {
            due += Duration::from_secs_f64(1.0 / rate);
            if Instant::now() < start + due {
                tokio::time::sleep_until((start + due).into()).await;
            }
            let t = tables[rng.below(tables.len())];
            let id = base + live_next.fetch_add(1, Ordering::SeqCst);
            writers.send(id, t, false)?;
        }
        Ok(())
    })
}

/// Wait until every sink's durable checkpoint covers `final_gtid` and the
/// delivered rows equal the ledger; the seconds it took, or an error at
/// `timeout`.
async fn drain(
    fresh: &Fresh,
    name: &str,
    live: &Live,
    timeout: Duration,
) -> Result<(f64, Value)> {
    let mut c = mysql_async::Conn::from_url(fresh.mysql_dsn().await?).await?;
    let final_gtid: String = c
        .query_first("SELECT @@GLOBAL.gtid_executed")
        .await?
        .unwrap_or_default();
    let t0 = Instant::now();
    loop {
        let cps = api(
            reqwest::Method::GET,
            &format!("/pipelines/{name}/checkpoints"),
            None,
        )
        .await
        .unwrap_or(Value::Null);
        let mut covered = false;
        if let Some(list) = cps.as_array()
            && list.iter().any(|cp| cp["sink_id"] == "kafka")
        {
            covered = true;
            for cp in list {
                let Some(set) = cp["position"]["gtid_set"].as_str() else {
                    covered = false;
                    break;
                };
                let sub: Option<i64> = c
                    .exec_first("SELECT GTID_SUBSET(?, ?)", (&final_gtid, set))
                    .await?;
                if sub != Some(1) {
                    covered = false;
                    break;
                }
            }
        }
        let ledger = {
            let commits = live.commits.lock().unwrap();
            reconcile(&commits, &live.seen.lock().unwrap())
        };
        if covered && ledger["equal"] == true {
            c.disconnect().await.ok();
            return Ok((
                t0.elapsed().as_secs_f64(),
                json!({"final_gtid": final_gtid, "checkpoints": cps, "ledger": ledger}),
            ));
        }
        if let Some(f) = failure(name).await {
            c.disconnect().await.ok();
            bail!("the pipeline failed while draining: {f}");
        }
        if t0.elapsed() > timeout {
            c.disconnect().await.ok();
            bail!(
                "drain timed out after {timeout:?}: frontier covered = {covered}, ledger = {ledger}"
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn run(
    cmp: &Comparison,
    build: &Build,
    postgres: bool,
    seq: u64,
    seed: u64,
    dir: &Path,
    drain_timeout: Duration,
) -> Result<Value> {
    std::fs::create_dir_all(dir)?;
    let fresh = Fresh::start(postgres).await?;
    let containers = fresh.ids();
    let started = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let out = run_on(cmp, build, &fresh, seq, seed, dir, drain_timeout).await;
    // The run's containers are removed with their tmpfs data.
    drop(fresh);
    let mut v = match out {
        Ok(v) => v,
        Err(e) => json!({"failed": format!("{e:#}")}),
    };
    v["containers"] = json!(containers);
    v["started_ms"] = json!(started);
    Ok(v)
}

#[allow(clippy::too_many_arguments)]
async fn run_on(
    cmp: &Comparison,
    build: &Build,
    fresh: &Fresh,
    seq: u64,
    seed: u64,
    dir: &Path,
    drain_timeout: Duration,
) -> Result<Value> {
    let name = format!("a{seq}");
    let topic = format!("actdiag-{seq}");
    fixture(fresh, cmp.tables).await?;
    create_topic(&topic).await?;
    let live = Live {
        commits: Arc::new(Mutex::new(Vec::new())),
        seen: Arc::new(Mutex::new(Seen::default())),
        next: Arc::new(std::sync::atomic::AtomicI64::new(1)),
        base: seq as i64 * RUN_IDS,
    };
    let stop = Arc::new(AtomicBool::new(false));
    let _stop_guard = RaiseOnDrop(Arc::clone(&stop));
    let consumer = consume(&topic, Arc::clone(&live.seen), Arc::clone(&stop))?;
    let state_dsn = fresh.state_dsn().await?;
    let mut runner = launch(build, dir, state_dsn.as_deref(), 0).await?;
    let pid = runner.pid;
    let exe_sha = sha256(&build.dir.join("runner"));
    let spec = spec(&name, &fresh.mysql_dsn().await?, &topic);
    std::fs::write(
        dir.join("pipeline.json"),
        serde_json::to_vec_pretty(&spec)?,
    )?;
    api(reqwest::Method::POST, "/pipelines", Some(&spec)).await?;
    wait_running(&name).await?;
    let first_marker_s =
        marker(fresh, &name, &live, Duration::from_secs(120)).await?;

    let mut result = json!({
        "comparison": cmp.name,
        "build": build.label,
        "seq": seq,
        "seed": seed,
        "runner_pid": pid,
        "runner_sha256": exe_sha,
        "tables": cmp.tables,
        "busy": cmp.busy,
        "rate": cmp.rate,
        "ddls": cmp.ddls,
        "first_marker_s": first_marker_s,
    });

    let writers = Arc::new(
        Writers::start(fresh, &live, cmp.busy.max(cmp.tables).min(WRITERS))
            .await?,
    );
    let halt = Arc::new(AtomicBool::new(false));
    let _halt_guard = RaiseOnDrop(Arc::clone(&halt));

    if let Some(act) = cmp.activation {
        let mut restarts = 0;
        if act == Activation::Restart {
            // Activate every table, drain, restart on the same state.
            for t in 0..cmp.tables {
                writers.send(live.id(), t as u32, false)?;
            }
            drain(fresh, &name, &live, drain_timeout).await?;
            runner.down().await?;
            restarts += 1;
            runner = launch(build, dir, state_dsn.as_deref(), restarts).await?;
            // API-created pipelines are not kept across a restart: the same
            // spec (same source id) resumes from its durable state.
            api(reqwest::Method::POST, "/pipelines", Some(&spec)).await?;
            wait_running(&name).await?;
            marker(fresh, &name, &live, Duration::from_secs(120)).await?;
        }
        let before = metrics_text().await;
        // First rows: one per table, at the rate or all at once.
        let t_first = Instant::now();
        let mut firsts = Vec::with_capacity(cmp.tables);
        for t in 0..cmp.tables {
            if act != Activation::Burst {
                let due =
                    t_first + Duration::from_secs_f64(t as f64 / cmp.act_rate);
                if Instant::now() < due {
                    tokio::time::sleep_until(due.into()).await;
                }
            }
            let id = live.id();
            firsts.push(id);
            writers.send(id, t as u32, false)?;
        }
        // Ordinary rows for a while.
        let tables: Vec<u32> = (0..cmp.tables as u32).collect();
        let loader = load(
            Arc::clone(&writers),
            Arc::clone(&live.next),
            live.base,
            tables,
            cmp.act_rate,
            seed,
            Arc::clone(&halt),
        );
        tokio::time::sleep(Duration::from_secs_f64(cmp.ordinary_s)).await;
        halt.store(true, Ordering::Relaxed);
        loader.await??;
        let t_stop = Instant::now();
        let (drain_s, drained) =
            match drain(fresh, &name, &live, drain_timeout).await {
                Ok(d) => d,
                Err(e) => {
                    stop.store(true, Ordering::Relaxed);
                    return Err(e);
                }
            };
        let after = metrics_text().await;
        let commits = live.commits.lock().unwrap().clone();
        let seen = live.seen.lock().unwrap();
        let first_set: std::collections::HashSet<i64> =
            firsts.iter().copied().collect();
        let mut act_ms = Vec::new();
        let mut ordinary_ms = Vec::new();
        let mut last_first = t_first;
        for c in commits
            .iter()
            .filter(|c| c.at >= t_first && c.table != CONTROL)
        {
            let Some(at) = seen.first.get(&c.id) else {
                continue;
            };
            let ms = at.saturating_duration_since(c.at).as_secs_f64() * 1e3;
            if first_set.contains(&c.id) {
                act_ms.push(ms);
                last_first = last_first.max(*at);
            } else {
                ordinary_ms.push(ms);
            }
        }
        result["activation"] = json!({
            "kind": format!("{act:?}"),
            "activation_ms": quantiles(act_ms),
            "ordinary_ms": quantiles(ordinary_ms),
            "phase_completion_s": last_first.duration_since(t_first).as_secs_f64(),
            "restarts": restarts,
        });
        result["drain_s"] = json!(drain_s);
        result["drain_after_stop_s"] =
            json!(Instant::now().duration_since(t_stop).as_secs_f64());
        result["drain"] = drained;
        result["phases"] = phase_delta(&before, &after);
        result["counters_after"] = json!(counters(&after));
    } else {
        // Busy tables, and the idle tables the ALTERs change: cached by a
        // first row each. Then the busy tables are loaded.
        let busy: Vec<u32> = (0..cmp.busy as u32).collect();
        let n_alters = match cmp.ddl {
            DdlKind::Alternate => cmp.ddls / 2,
            DdlKind::Create => 0,
        };
        if cmp.busy + n_alters > cmp.tables {
            bail!(
                "{} busy and {n_alters} altered tables need {} tables",
                cmp.busy,
                cmp.busy + n_alters
            );
        }
        let altered: Vec<u32> =
            (cmp.busy as u32..(cmp.busy + n_alters) as u32).collect();
        for &t in busy.iter().chain(&altered) {
            writers.send(live.id(), t, false)?;
        }
        if !busy.is_empty() || !altered.is_empty() {
            drain(fresh, &name, &live, drain_timeout).await?;
        }
        let loader = (!busy.is_empty() && cmp.rate > 0.0).then(|| {
            load(
                Arc::clone(&writers),
                Arc::clone(&live.next),
                live.base,
                busy.clone(),
                cmp.rate,
                seed,
                Arc::clone(&halt),
            )
        });
        if loader.is_some() {
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
        reset_store_counters(fresh).await?;
        let store0 = store_counters(fresh).await?;
        let before = metrics_text().await;
        // The DDLs, on one connection, as fast as the server takes them;
        // each followed by a row in the table it changed.
        let mut ddl_conn =
            mysql_async::Conn::from_url(fresh.mysql_dsn().await?).await?;
        let mut ddl_rows = Vec::with_capacity(cmp.ddls);
        let mut alters = 0usize;
        let t_ddl = Instant::now();
        for i in 0..cmp.ddls {
            let create = cmp.ddl == DdlKind::Create
                || i % 2 == 0
                || alters >= altered.len();
            let id = live.id();
            if create {
                ddl_conn
                    .query_drop(format!(
                        "CREATE TABLE {} (id BIGINT PRIMARY KEY, a INT, b VARCHAR(32), c DOUBLE)",
                        new_table_name(i)
                    ))
                    .await?;
                ddl_conn
                    .query_drop(format!(
                        "INSERT INTO {} (id, a, b, c) VALUES ({id}, 1, 'ddl', 1.5)",
                        new_table_name(i)
                    ))
                    .await?;
                live.commits.lock().unwrap().push(Commit {
                    id,
                    table: NEW_BASE + i as u32,
                    at: Instant::now(),
                    ddl: true,
                });
            } else {
                let t = altered[alters];
                alters += 1;
                ddl_conn
                    .query_drop(format!(
                        "ALTER TABLE {} ADD COLUMN x{i:05} INT",
                        table_name(t as usize)
                    ))
                    .await?;
                writers.send(id, t, true)?;
            }
            ddl_rows.push(id);
        }
        let issued_s = t_ddl.elapsed().as_secs_f64();
        ddl_conn.disconnect().await.ok();
        // Phase completion: every DDL's row delivered.
        let deadline = Instant::now() + drain_timeout;
        let mut polls = 0u64;
        let t_done = loop {
            let done = {
                let seen = live.seen.lock().unwrap();
                ddl_rows
                    .iter()
                    .map(|id| seen.first.get(id).copied())
                    .collect::<Option<Vec<Instant>>>()
            };
            if let Some(ats) = done {
                break ats.into_iter().max().unwrap_or(t_ddl);
            }
            if Instant::now() > deadline {
                bail!(
                    "the DDL phase did not complete within {drain_timeout:?}"
                );
            }
            if polls.is_multiple_of(10)
                && let Some(f) = failure(&name).await
            {
                bail!("the pipeline failed in the DDL phase: {f}");
            }
            polls += 1;
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        let after_phase = metrics_text().await;
        let store1 = store_counters(fresh).await?;
        halt.store(true, Ordering::Relaxed);
        if let Some(l) = loader {
            l.await??;
        }
        let t_stop = Instant::now();
        let (drain_s, drained) =
            drain(fresh, &name, &live, drain_timeout).await?;
        let completion = t_done.duration_since(t_ddl).as_secs_f64();
        let commits = live.commits.lock().unwrap().clone();
        let seen = live.seen.lock().unwrap();
        let mut lag_ms = Vec::new();
        for c in commits.iter().filter(|c| {
            !c.ddl && c.table < NEW_BASE && c.at >= t_ddl && c.at <= t_done
        }) {
            if let Some(at) = seen.first.get(&c.id) {
                lag_ms.push(
                    at.saturating_duration_since(c.at).as_secs_f64() * 1e3,
                );
            }
        }
        result["ddl"] = json!({
            "kind": format!("{:?}", cmp.ddl),
            "issued_s": issued_s,
            "phase_completion_s": completion,
            "per_ddl_ms": completion * 1e3 / cmp.ddls as f64,
            "busy_lag_ms": quantiles(lag_ms),
            "alters": alters,
        });
        result["store_before"] = store0.unwrap_or(Value::Null);
        result["store_after_phase"] = store1.unwrap_or(Value::Null);
        result["drain_s"] = json!(drain_s);
        result["drain_after_stop_s"] =
            json!(Instant::now().duration_since(t_stop).as_secs_f64());
        result["drain"] = drained;
        result["phases"] = phase_delta(&before, &after_phase);
        result["counters_after"] = json!(counters(&after_phase));
    }

    // Raw records: every commit and its arrival.
    {
        let commits = live.commits.lock().unwrap().clone();
        let seen = live.seen.lock().unwrap();
        let origin = commits.first().map(|c| c.at).unwrap_or_else(Instant::now);
        let mut csv = String::from("id,table,ddl,commit_us,arrival_us\n");
        for c in &commits {
            let arr = seen
                .first
                .get(&c.id)
                .map(|a| {
                    a.saturating_duration_since(origin).as_micros().to_string()
                })
                .unwrap_or_default();
            csv.push_str(&format!(
                "{},{},{},{},{}\n",
                c.id,
                c.table,
                c.ddl as u8,
                c.at.saturating_duration_since(origin).as_micros(),
                arr
            ));
        }
        std::fs::write(dir.join("rows.csv"), csv)?;
    }
    std::fs::write(dir.join("metrics-final.txt"), metrics_text().await)?;
    stop.store(true, Ordering::Relaxed);
    consumer.await.ok();
    match Arc::try_unwrap(writers) {
        Ok(w) => w.finish().await?,
        Err(_) => bail!("a writer is still shared"),
    }
    runner.down().await?;
    Ok(result)
}

/// Raises its flag when dropped: a run's consumer and load stop however the
/// run ends.
struct RaiseOnDrop(Arc<AtomicBool>);

impl Drop for RaiseOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

/// The phase histograms' growth between two scrapes.
fn phase_delta(before: &str, after: &str) -> Value {
    let (b, a) = (phases(before), phases(after));
    let mut out = serde_json::Map::new();
    for (phase, av) in &a {
        let bv = b.get(phase);
        let num = |v: Option<&Value>, k: &str| {
            v.and_then(|v| v[k].as_f64()).unwrap_or(0.0)
        };
        let mut buckets = serde_json::Map::new();
        if let Some(ab) = av["buckets"].as_object() {
            for (le, n) in ab {
                let before =
                    bv.and_then(|v| v["buckets"][le].as_f64()).unwrap_or(0.0);
                buckets.insert(
                    le.clone(),
                    json!(n.as_f64().unwrap_or(0.0) - before),
                );
            }
        }
        out.insert(
            phase.clone(),
            json!({
                "count": num(Some(av), "count") - num(bv, "count"),
                "sum_s": num(Some(av), "sum") - num(bv, "sum"),
                "buckets": buckets,
            }),
        );
    }
    Value::Object(out)
}

fn sha256(path: &Path) -> Option<String> {
    let out = std::process::Command::new("sha256sum")
        .arg(path)
        .output()
        .ok()?;
    String::from_utf8(out.stdout)
        .ok()?
        .split_whitespace()
        .next()
        .map(str::to_string)
}

fn sh(cmd: &str) -> String {
    std::process::Command::new("sh")
        .args(["-c", cmd])
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_default()
}

async fn environment(args: &Args, builds: &[Build]) -> Value {
    let image = |r: (&str, &str)| format!("{}:{}", r.0, r.1);
    json!({
        "args": format!("{args:?}"),
        "command": std::env::args().collect::<Vec<_>>(),
        "harness_sha256": std::env::current_exe().ok().and_then(|p| sha256(&p)),
        "harness_revision": sh("git rev-parse HEAD"),
        "harness_dirty": !sh("git status --porcelain").is_empty(),
        "builds": builds.iter().map(|b| json!({
            "label": b.label, "dir": b.dir, "identity": b.identity,
        })).collect::<Vec<_>>(),
        "images": [image(MYSQL_IMAGE), image(POSTGRES_IMAGE), image(KAFKA_IMAGE)],
        "host": {
            "kernel": sh("uname -a"),
            "cpu": sh("grep -m1 'model name' /proc/cpuinfo"),
            "cpus": sh("nproc"),
            "governor": sh("cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"),
            "memory": sh("grep MemTotal /proc/meminfo"),
            "disk": sh("df -h --output=source,size,avail,target ."),
            "docker": sh("docker version --format '{{.Server.Version}}'"),
        },
        "mysql_settings": {
            "sync_binlog": 0,
            "innodb_flush_log_at_trx_commit": 2,
            "data": "tmpfs",
        },
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let lock =
        std::fs::File::create(std::env::temp_dir().join("actdiag.lock"))?;
    if lock.try_lock().is_err() {
        bail!("another actdiag invocation is running on this machine");
    }
    if args.store != "postgres" && args.store != "sqlite" {
        bail!("--store postgres or sqlite");
    }
    if args.out.join("runs.jsonl").exists() {
        bail!("{} already holds a series", args.out.display());
    }
    let comparisons: Vec<Comparison> = args
        .comparisons
        .iter()
        .map(|c| comparison(c, args.scale))
        .collect::<Result<_>>()?;
    let builds: Vec<Build> = args
        .builds
        .iter()
        .map(|b| {
            let (label, dir) = b
                .split_once('=')
                .ok_or_else(|| anyhow!("--build label=dir"))?;
            let dir = PathBuf::from(dir).canonicalize()?;
            let identity: Value = serde_json::from_slice(
                &std::fs::read(dir.join("identity.json")).with_context(
                    || format!("{}/identity.json", dir.display()),
                )?,
            )?;
            Ok(Build {
                label: label.to_string(),
                dir,
                identity,
            })
        })
        .collect::<Result<_>>()?;
    std::fs::create_dir_all(&args.out)?;
    std::fs::write(
        args.out.join("invocation.json"),
        serde_json::to_vec_pretty(&environment(&args, &builds).await)?,
    )?;
    let kafka = start_kafka().await?;
    created(kafka.id());
    let series = async {
        let mut runs = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(args.out.join("runs.jsonl"))?;
        let mut seq: u64 = 1;
        for cmp in &comparisons {
            let drain_timeout = args
                .drain_timeout_s
                .map(Duration::from_secs)
                .unwrap_or(cmp.drain_timeout);
            for rep in 0..args.reps {
                // Rotated order: rep r starts at build r.
                for k in 0..builds.len() {
                    let build = &builds[(rep + k) % builds.len()];
                    let dir = args.out.join("runs").join(format!(
                        "{}-{}-r{}",
                        cmp.name,
                        build.label,
                        rep + 1
                    ));
                    eprintln!(
                        "[{}] {} {} rep {} (seq {seq})",
                        sh("date -Is"),
                        cmp.name,
                        build.label,
                        rep + 1
                    );
                    let mut v = run(
                        cmp,
                        build,
                        args.store == "postgres",
                        seq,
                        args.seed + rep as u64,
                        &dir,
                        drain_timeout,
                    )
                    .await?;
                    v["rep"] = json!(rep + 1);
                    v["order"] = json!(k);
                    v["comparison"] = json!(cmp.name);
                    v["build"] = json!(build.label);
                    v["store"] = json!(args.store);
                    use std::io::Write as _;
                    writeln!(runs, "{}", serde_json::to_string(&v)?)?;
                    runs.flush()?;
                    if let Some(f) = v.get("failed") {
                        eprintln!("  FAILED: {f}");
                    }
                    seq += 1;
                }
            }
        }
        Ok::<_, anyhow::Error>(())
    };
    // An interrupt ends the series like a failure: the current run's
    // runner is killed with it, and every container this invocation
    // created is removed.
    let mut term = tokio::signal::unix::signal(
        tokio::signal::unix::SignalKind::terminate(),
    )?;
    let result = tokio::select! {
        r = series => r,
        _ = tokio::signal::ctrl_c() => Err(anyhow!("interrupted")),
        _ = term.recv() => Err(anyhow!("terminated")),
    };
    drop(kafka);
    remove_created();
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn commit(id: i64, table: u32) -> Commit {
        Commit {
            id,
            table,
            at: Instant::now(),
            ddl: false,
        }
    }

    fn seen(order: &[(u32, &[i64])], duplicates: u64) -> Seen {
        let mut s = Seen {
            duplicates,
            ..Seen::default()
        };
        for (t, ids) in order {
            for id in *ids {
                s.first.insert(*id, Instant::now());
                s.order.entry(*t).or_default().push(*id);
            }
        }
        s
    }

    /// The ledger is equal only when every committed row arrived once, no
    /// other row arrived, and each table's rows arrived in commit order.
    #[test]
    fn reconcile_finds_missing_extra_duplicate_and_reordered_rows() {
        let commits = [commit(1, 0), commit(2, 0), commit(3, 1)];
        let all = reconcile(&commits, &seen(&[(0, &[1, 2]), (1, &[3])], 0));
        assert_eq!(all["equal"], true, "{all}");
        let missing = reconcile(&commits, &seen(&[(0, &[1]), (1, &[3])], 0));
        assert_eq!(
            (missing["missing"].as_u64(), missing["equal"].as_bool()),
            (Some(1), Some(false))
        );
        let extra =
            reconcile(&commits, &seen(&[(0, &[1, 2, 9]), (1, &[3])], 0));
        assert_eq!(
            (extra["extra"].as_u64(), extra["equal"].as_bool()),
            (Some(1), Some(false))
        );
        let dup = reconcile(&commits, &seen(&[(0, &[1, 2]), (1, &[3])], 1));
        assert_eq!(dup["equal"], false);
        let reordered =
            reconcile(&commits, &seen(&[(0, &[2, 1]), (1, &[3])], 0));
        assert_eq!(
            (
                reordered["reordered_tables"].as_u64(),
                reordered["equal"].as_bool()
            ),
            (Some(1), Some(false))
        );
    }

    /// Phase histograms are read from the exporter's text, and their growth
    /// between two scrapes is what a phase did in between.
    #[test]
    fn phase_histograms_are_parsed_and_differenced() {
        let before = "\
deltaforge_perf_phase_latency_seconds_bucket{phase=\"capture\",le=\"0.01\"} 1
deltaforge_perf_phase_latency_seconds_sum{phase=\"capture\"} 0.004
deltaforge_perf_phase_latency_seconds_count{phase=\"capture\"} 1
";
        let after = "\
deltaforge_perf_phase_latency_seconds_bucket{phase=\"capture\",le=\"0.01\"} 4
deltaforge_perf_phase_latency_seconds_sum{phase=\"capture\"} 0.025
deltaforge_perf_phase_latency_seconds_count{phase=\"capture\"} 4
deltaforge_perf_phase_latency_seconds_count{phase=\"commit\"} 2
deltaforge_perf_phase_latency_seconds_sum{phase=\"commit\"} 0.002
other_metric 7
";
        let d = phase_delta(before, after);
        assert_eq!(d["capture"]["count"], 3.0);
        assert!((d["capture"]["sum_s"].as_f64().unwrap() - 0.021).abs() < 1e-9);
        assert_eq!(d["capture"]["buckets"]["0.01"], 3.0);
        assert_eq!(d["commit"]["count"], 2.0);
    }

    #[test]
    fn quantiles_of_a_sample() {
        let q = quantiles((1..=100).map(f64::from).collect());
        assert_eq!(
            (q["p50"].as_f64(), q["max"].as_f64()),
            (Some(51.0), Some(100.0))
        );
        assert_eq!(quantiles(Vec::new()), Value::Null);
    }

    #[test]
    fn table_numbers_name_fixture_new_and_control_tables() {
        assert_eq!(table_number(DB, "t00042"), Some(42));
        assert_eq!(table_number(DB, "n00007"), Some(NEW_BASE + 7));
        assert_eq!(table_number(DB, "ctl"), Some(CONTROL));
        assert_eq!(table_number("other", "t00001"), None);
    }
}
