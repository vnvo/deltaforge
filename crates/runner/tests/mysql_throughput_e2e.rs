//! Reliable end-to-end throughput / backlog-drain baseline for MySQL.
//!
//! Self-provisioning (testcontainers) companion to `throughput_e2e` (PostgreSQL).
//! It wires the real production pipeline - `MySqlSource` -> `Coordinator` ->
//! `KafkaSink` through the production `PerSinkCheckpointProxy` - against real
//! MySQL 8.4 and Kafka containers, with a dynamic mapped MySQL port and no
//! manual pipeline apply or proxy. Kafka uses a dedicated host port (fixed at
//! start for the broker's advertised listener), overridable via
//! `THROUGHPUT_KAFKA_PORT` for parallel/CI runs.
//!
//! Unlike PostgreSQL (whose slot retains WAL from creation), MySQL has no slot,
//! so a source started "from end" sits at the current binlog tail and would skip
//! a pre-written backlog. To measure a true cold drain the test:
//!   1. runs the pipeline and writes a small WARMUP so the source establishes a
//!      binlog checkpoint after delivery,
//!   2. stops the pipeline (checkpoint persisted),
//!   3. writes the backlog (binlog advances past the checkpoint),
//!   4. restarts the pipeline against the SAME checkpoint store so the source
//!      resumes from the checkpoint and drains the whole backlog.
//!
//! It reports backlog write rate, drain throughput (events/s, wall-clock and
//! steady-state) and peak process RSS, and asserts full delivery plus a floor.
//!
//! ALWAYS run in release for a representative number (a debug build throttles
//! CPU-bound work several-fold):
//!   THROUGHPUT_ROWS=1000000 cargo test --release -p runner \
//!     --test mysql_throughput_e2e -- --include-ignored --nocapture

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use checkpoints::{CheckpointStore, MemCheckpointStore};
use common::AllowList;
use deltaforge_config::{
    BatchConfig, EncodingCfg, EnvelopeCfg, KafkaSinkCfg, OnSchemaDrift,
    SnapshotCfg, SnapshotMode,
};
use deltaforge_core::{
    ArcDynProcessor, ArcDynSink, Source, SourceHandle, SourceItem,
};
use gate_ownership::GateOwned;
use mysql_async::prelude::Queryable;
use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, StreamConsumer};
use runner::coordinator::{
    Coordinator, PauseState, build_batch_processor, build_commit_fn,
};
use runner::pipeline_manager::PerSinkCheckpointProxy;
use sinks::kafka::KafkaSink;
use sources::mysql::MySqlSource;
use storage::{ArcStorageBackend, DurableSchemaRegistry, MemoryStorageBackend};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

const KAFKA_INTERNAL_PORT: u16 = 29092;

/// Host port for the Kafka container (env-overridable for parallel/CI runs).
fn kafka_port() -> u16 {
    std::env::var("THROUGHPUT_KAFKA_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(9392)
}
const SID: &str = "mysqlthroughput";
const TOPIC: &str = "orders.events";
const WARMUP: usize = 20;

fn row_count() -> usize {
    std::env::var("THROUGHPUT_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50_000)
}

// ── Kafka container ──────────────────────────────────────────────────────────

async fn start_kafka() -> (ContainerAsync<GenericImage>, String) {
    let kp = kafka_port();
    let image = GenericImage::new("confluentinc/cp-kafka", "7.5.0")
        .with_wait_for(WaitFor::Duration { length: Duration::from_secs(15) })
        .with_env_var("KAFKA_NODE_ID", "1")
        .with_env_var("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env_var("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:29093")
        .with_env_var(
            "KAFKA_LISTENERS",
            format!(
                "PLAINTEXT://0.0.0.0:{KAFKA_INTERNAL_PORT},CONTROLLER://0.0.0.0:29093,EXTERNAL://0.0.0.0:{kp}"
            ),
        )
        .with_env_var(
            "KAFKA_ADVERTISED_LISTENERS",
            format!(
                "PLAINTEXT://localhost:{KAFKA_INTERNAL_PORT},EXTERNAL://localhost:{kp}"
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
        .with_env_var("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
        .with_env_var("CLUSTER_ID", "MkU3OEVBNTcwNTJENDM2Qg")
        .with_mapped_port(kp, kp.tcp());
    let container = image.gate_owned().start().await.expect("start kafka");
    let brokers = format!("localhost:{kp}");
    wait_for_kafka(&brokers, Duration::from_secs(60)).await;
    (container, brokers)
}

async fn wait_for_kafka(brokers: &str, dur: Duration) {
    use rdkafka::admin::AdminClient;
    use rdkafka::client::DefaultClientContext;
    let deadline = Instant::now() + dur;
    while Instant::now() < deadline {
        if let Ok(admin) = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("socket.timeout.ms", "5000")
            .create::<AdminClient<DefaultClientContext>>()
        {
            let ok = tokio::task::spawn_blocking(move || {
                admin.inner().fetch_metadata(None, Duration::from_secs(5))
            })
            .await;
            if matches!(ok, Ok(Ok(_))) {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("kafka not ready after {dur:?}");
}

/// Consume the topic from the beginning and return the set of unique `after.id`
/// values present. Proves every distinct row arrived (a high-watermark count can
/// be satisfied by duplicates while rows are missing).
async fn consume_unique_ids(
    brokers: &str,
    expected: usize,
    secs: u64,
) -> std::collections::HashSet<i64> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "mysql-throughput-verify")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("verify consumer");
    consumer.subscribe(&[TOPIC]).expect("subscribe");
    let mut ids = std::collections::HashSet::new();
    let deadline = Instant::now() + Duration::from_secs(secs);
    while ids.len() < expected && Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(5), consumer.recv())
            .await
        {
            Ok(Ok(m)) => {
                if let Some(p) = m.payload() {
                    if let Ok(v) =
                        serde_json::from_slice::<serde_json::Value>(p)
                    {
                        if let Some(id) = v
                            .get("after")
                            .and_then(|a| a.get("id"))
                            .and_then(|i| i.as_i64())
                        {
                            ids.insert(id);
                        }
                    }
                }
            }
            _ => break,
        }
    }
    ids
}

/// High-watermark offset of the topic (total messages produced). 0 if absent.
async fn kafka_high_watermark(brokers: &str) -> i64 {
    let brokers = brokers.to_string();
    tokio::task::spawn_blocking(move || {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set("group.id", "mysql-throughput-watermark")
            .create()
            .expect("consumer");
        match consumer.fetch_watermarks(TOPIC, 0, Duration::from_secs(5)) {
            Ok((_, high)) => high,
            Err(_) => 0,
        }
    })
    .await
    .unwrap_or(0)
}

// ── MySQL container ──────────────────────────────────────────────────────────

async fn start_mysql() -> (ContainerAsync<GenericImage>, u16) {
    let image = GenericImage::new("mysql", "8.4")
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
        ]);
    let c = image.gate_owned().start().await.expect("start mysql");
    let port = c.get_host_port_ipv4(3306).await.expect("mysql port");
    // Give MySQL a moment past the readiness banner to accept connections.
    tokio::time::sleep(Duration::from_secs(3)).await;
    (c, port)
}

fn root_dsn(port: u16) -> String {
    format!("mysql://root:rootpw@127.0.0.1:{port}/")
}

fn cdc_dsn(port: u16) -> String {
    format!("mysql://df:dfpw@127.0.0.1:{port}/orders")
}

async fn mysql_conn(dsn: &str) -> Result<mysql_async::Conn> {
    // Retry briefly: the server can refuse connections right after the banner.
    let opts = mysql_async::Opts::from_url(dsn)?;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match mysql_async::Conn::new(opts.clone()).await {
            Ok(c) => return Ok(c),
            Err(e) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(500)).await;
                let _ = e;
            }
            Err(e) => return Err(e.into()),
        }
    }
}

/// Resident set size of this process, in MiB (Linux /proc/self/statm).
fn process_rss_mib() -> f64 {
    let page = 4096.0;
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| s.split_whitespace().nth(1).map(|v| v.to_string()))
        .and_then(|v| v.parse::<f64>().ok())
        .map(|pages| pages * page / (1024.0 * 1024.0))
        .unwrap_or(0.0)
}

// ── Pipeline wiring (production components) ─────────────────────────────────

struct Running {
    src_handle: SourceHandle,
    coord_task: tokio::task::JoinHandle<Result<()>>,
    cancel: CancellationToken,
    // Keep the pause-channel sender alive for the pipeline's lifetime: if it is
    // dropped the coordinator sees the watch close and shuts down, tearing down
    // the event channel.
    _pause_tx: watch::Sender<PauseState>,
}

impl Running {
    async fn stop(self) {
        self.cancel.cancel();
        self.src_handle.stop();
        let _ = self.src_handle.join().await;
        let _ = self.coord_task.await;
    }
}

async fn spawn_pipeline(
    dsn: &str,
    brokers: &str,
    store: Arc<dyn CheckpointStore>,
    backend: ArcStorageBackend,
    registry: Arc<DurableSchemaRegistry>,
) -> Result<Running> {
    let src = MySqlSource {
        id: SID.into(),
        dsn: dsn.into(),
        tables: vec!["orders.orders".into()],
        tenant: "acme".into(),
        pipeline: "test".into(),
        registry,
        registry_scope: sources::registry_scope::SharedRegistryScope::default(),
        backend,
        outbox_tables: AllowList::default(),
        snapshot_cfg: SnapshotCfg {
            mode: SnapshotMode::Never,
            ..Default::default()
        },
        on_schema_drift: OnSchemaDrift::Adapt,
        table_options: Default::default(),
        rotation: None,
        snapshot_cohort: Default::default(),
    };
    let src: Arc<dyn Source> = Arc::new(src);
    let commit_signal = Arc::new(tokio::sync::Notify::new());
    let proxy: Arc<dyn CheckpointStore> = Arc::new(
        PerSinkCheckpointProxy::for_source(
            store.clone(),
            SID.to_string(),
            &src,
        )
        .with_commit_signal(commit_signal.clone()),
    );
    let (event_tx, event_rx) = mpsc::channel::<SourceItem>(8192);
    let src_handle = src.run(event_tx, proxy).await;

    let cancel = CancellationToken::new();
    let kafka = Arc::new(KafkaSink::new(
        &KafkaSinkCfg {
            id: "kafka".into(),
            brokers: brokers.into(),
            topic: TOPIC.into(),
            key: None,
            envelope: EnvelopeCfg::Native,
            encoding: EncodingCfg::Json,
            required: Some(true),
            exactly_once: None,
            send_timeout_secs: Some(30),
            client_conf: HashMap::from([
                ("linger.ms".to_string(), "0".to_string()),
                (
                    "queue.buffering.max.messages".to_string(),
                    "2000000".to_string(),
                ),
                (
                    "queue.buffering.max.kbytes".to_string(),
                    "2097152".to_string(),
                ),
                ("compression.type".to_string(), "lz4".to_string()),
            ]),
            secret_refs: Default::default(),
            filter: None,
        },
        cancel.clone(),
        "test",
        None,
        &sinks::ResolvedSinkCreds::default(),
    )?) as ArcDynSink;

    let cp_fn = build_commit_fn(store.clone(), format!("{SID}::sink::kafka"));
    let processors: Arc<[ArcDynProcessor]> = Arc::from(vec![]);
    let batch_processor = build_batch_processor(processors, "test".to_string());
    let coord = Coordinator::builder(SID)
        .sinks(vec![kafka])
        .batch_config(Some(BatchConfig {
            max_events: Some(16000),
            max_bytes: Some(16 * 1024 * 1024),
            max_ms: Some(100),
            respect_source_tx: Some(false),
            max_inflight: Some(8),
            ..BatchConfig::default()
        }))
        .commit_fn("kafka", cp_fn)
        .commit_notify(commit_signal.clone())
        .process_fn(batch_processor)
        .build();

    let coord_cancel = cancel.clone();
    let (pause_tx, pause_rx) = watch::channel(PauseState::default());
    let coord_task = tokio::spawn(async move {
        coord.run(event_rx, coord_cancel, pause_rx).await
    });

    Ok(Running {
        src_handle,
        coord_task,
        cancel,
        _pause_tx: pause_tx,
    })
}

/// Wait until the Kafka high-watermark reaches `target` (or time out).
async fn wait_for_offset(brokers: &str, target: i64, secs: u64) -> i64 {
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut hw = 0;
    while hw < target && Instant::now() < deadline {
        hw = kafka_high_watermark(brokers).await;
        if hw < target {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    hw
}

// ── Test ─────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn mysql_to_kafka_backlog_drain_throughput() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new("warn"))
        .try_init();
    let rows = row_count();
    let (_my, port) = start_mysql().await;

    // Root: create the CDC user, grants, database and table.
    {
        let mut root = mysql_conn(&root_dsn(port)).await?;
        root.query_drop("CREATE DATABASE IF NOT EXISTS orders")
            .await?;
        root.query_drop(
            "CREATE USER IF NOT EXISTS 'df'@'%' IDENTIFIED BY 'dfpw'",
        )
        .await?;
        root.query_drop(
            "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'df'@'%'",
        )
        .await?;
        root.query_drop("GRANT SELECT ON orders.* TO 'df'@'%'")
            .await?;
        root.query_drop("FLUSH PRIVILEGES").await?;
        root.query_drop(
            "CREATE TABLE orders.orders \
             (id INT PRIMARY KEY, sku VARCHAR(64), amount DECIMAL(10,2))",
        )
        .await?;
    }

    let (_kafka, brokers) = start_kafka().await;

    // Prime the CDC user's caching_sha2_password auth cache with a client
    // connection: MySQL 8.4's default plugin needs an RSA/TLS handshake on the
    // first (uncached) auth, which the binlog connector does not perform over a
    // plaintext connection. A prior `df` client connect populates the server
    // cache so the binlog connector can then use the fast path.
    {
        let mut c = mysql_conn(&cdc_dsn(port)).await?;
        c.query_drop("SELECT 1").await?;
    }

    let store: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
    let registry = DurableSchemaRegistry::new(backend.clone()).await?;

    // ── Phase 1: warm up so the source establishes a binlog checkpoint ──
    let run1 = spawn_pipeline(
        &cdc_dsn(port),
        &brokers,
        store.clone(),
        backend.clone(),
        registry.clone(),
    )
    .await?;
    // The MySQL source starts "from end": it must connect and position at the
    // current binlog tail before the warmup rows are written, otherwise those
    // rows fall before its start position and are never seen. Give it time to
    // establish the binlog stream.
    tokio::time::sleep(Duration::from_secs(8)).await;
    {
        // Writes go through root; the CDC user `df` is read-only (SELECT + REPLICATION).
        let mut c = mysql_conn(&root_dsn(port)).await?;
        for id in 1..=WARMUP as i32 {
            c.exec_drop(
                "INSERT INTO orders.orders (id, sku, amount) VALUES (?, ?, ?)",
                (id, format!("warm-{id}"), 1.0f64),
            )
            .await?;
        }
    }
    let warm = wait_for_offset(&brokers, WARMUP as i64, 60).await;
    assert!(
        warm >= WARMUP as i64,
        "warmup did not deliver: {warm}/{WARMUP} (source never established a checkpoint)"
    );
    run1.stop().await;

    // ── Phase 2: write the backlog while the pipeline is stopped ──
    let write_start = Instant::now();
    {
        let mut c = mysql_conn(&root_dsn(port)).await?;
        c.query_drop("SET SESSION cte_max_recursion_depth = 100000000")
            .await?;
        let base = WARMUP as i64;
        c.query_drop(format!(
            "INSERT INTO orders.orders (id, sku, amount) \
             WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n < {rows}) \
             SELECT n + {base}, CONCAT('sku-', n), (n MOD 1000) FROM seq"
        ))
        .await?;
    }
    let write_secs = write_start.elapsed().as_secs_f64();
    let write_rps = rows as f64 / write_secs;
    println!(
        "mysql throughput: wrote {rows} backlog rows in {write_secs:.2}s ({write_rps:.0} rows/s)"
    );

    // ── Phase 3: restart -> resume from checkpoint -> drain the backlog ──
    let stop_sampler = Arc::new(AtomicBool::new(false));
    let peak_rss = Arc::new(AtomicU64::new(0));
    let sampler = {
        let stop = stop_sampler.clone();
        let peak = peak_rss.clone();
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                peak.fetch_max(process_rss_mib() as u64, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
    };

    let target = (WARMUP + rows) as i64;
    let drain_start = Instant::now();
    let run2 = spawn_pipeline(
        &cdc_dsn(port),
        &brokers,
        store.clone(),
        backend.clone(),
        registry.clone(),
    )
    .await?;

    let mut first_delivery: Option<Instant> = None;
    let deadline = drain_start + Duration::from_secs(300);
    let mut delivered = warm;
    while delivered < target && Instant::now() < deadline {
        delivered = kafka_high_watermark(&brokers).await;
        if first_delivery.is_none() && delivered > warm {
            first_delivery = Some(Instant::now());
        }
        if delivered < target {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    let drain_end = Instant::now();
    let total_secs = drain_end.duration_since(drain_start).as_secs_f64();

    stop_sampler.store(true, Ordering::Relaxed);
    let _ = sampler.await;
    run2.stop().await;

    let wall_eps = rows as f64 / total_secs;
    let steady_eps = match first_delivery {
        Some(t0) => {
            rows as f64 / drain_end.duration_since(t0).as_secs_f64().max(0.001)
        }
        None => wall_eps,
    };
    let peak = peak_rss.load(Ordering::Relaxed);

    println!("═══════════════════════════════════════════════");
    println!("  Backlog-drain throughput (MySQL → Kafka)");
    println!("═══════════════════════════════════════════════");
    println!("  backlog rows        : {rows}");
    println!(
        "  backlog write       : {write_secs:.2}s ({write_rps:.0} rows/s)"
    );
    println!("  delivered to Kafka  : {delivered} (incl. {WARMUP} warmup)");
    println!("  drain wall-clock    : {total_secs:.2}s");
    println!("  drain throughput    : {wall_eps:.0} events/s (wall)");
    println!("  drain throughput    : {steady_eps:.0} events/s (steady-state)");
    println!(
        "  peak process RSS    : {peak} MiB (in-process; includes test harness)"
    );
    println!("═══════════════════════════════════════════════");

    assert_eq!(
        delivered, target,
        "backlog not fully drained: {delivered}/{target}"
    );

    // Correctness: every DISTINCT row (warmup + backlog) must be present, not
    // just a matching count. Backlog ids are (WARMUP+1)..=(WARMUP+rows).
    let ids = consume_unique_ids(&brokers, target as usize, 180).await;
    assert_eq!(
        ids.len(),
        target as usize,
        "expected {target} unique rows in Kafka, found {} (missing or duplicated)",
        ids.len()
    );
    assert!(
        ids.contains(&((WARMUP + 1) as i64))
            && ids.contains(&((WARMUP + rows) as i64)),
        "backlog boundary rows missing from Kafka"
    );

    assert!(
        wall_eps > 2000.0,
        "drain throughput {wall_eps:.0} ev/s below the 2000 ev/s floor"
    );

    Ok(())
}
