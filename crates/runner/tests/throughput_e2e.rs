//! Reliable end-to-end throughput / backlog-drain baseline.
//!
//! Self-provisioning (testcontainers) replacement for the chaos `backlog-drain`
//! scenario. It wires the real production pipeline - `PostgresSource` ->
//! `Coordinator` -> `KafkaSink` through the production `PerSinkCheckpointProxy` -
//! against real PostgreSQL and Kafka containers, with no manual pipeline apply
//! and no toxiproxy. The PostgreSQL container uses a dynamic mapped host port;
//! Kafka uses a dedicated host port (the repo-wide pattern, required because the
//! broker's advertised listener must be fixed at start) that is distinct per
//! test and overridable via `THROUGHPUT_KAFKA_PORT` for parallel/CI runs.
//! Because the replication slot is
//! created BEFORE the backlog is written, the slot retains the WAL and the source
//! cold-drains the entire backlog from the slot's consistent point - so no
//! checkpoint-priming dance is needed.
//!
//! It measures: backlog write rate (rows/s), end-to-end drain throughput
//! (events/s, both wall-clock and steady-state), and peak process RSS during the
//! drain. It asserts that every row is delivered and that throughput clears a
//! conservative regression floor.
//!
//! Row count is `THROUGHPUT_ROWS` (default 50_000; override with the env var of
//! the same name). ALWAYS run in release - a debug build throttles CPU-bound
//! work (JSON encoding) several-fold:
//!   THROUGHPUT_ROWS=1000000 cargo test --release -p runner --test throughput_e2e \
//!     -- --include-ignored --nocapture
//!
//! The reported throughput is for local capacity work only and is
//! environment-dependent: on a shared developer machine it varies substantially
//! (2x+) with background CPU/IO load, so a single run is not a headline figure -
//! measure on a quiet/dedicated host, or take the median of several runs. What
//! this test ASSERTS and guards in CI-style runs is correctness (every distinct
//! row delivered) and a conservative regression floor, not an absolute rate.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use checkpoints::{CheckpointStore, MemCheckpointStore};
use deltaforge_config::{
    BatchConfig, EncodingCfg, EnvelopeCfg, KafkaSinkCfg, OnSchemaDrift,
    SnapshotCfg, SnapshotMode,
};
use deltaforge_core::{ArcDynProcessor, ArcDynSink, Source, SourceItem};
use gate_ownership::GateOwned;
use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, StreamConsumer};
use runner::coordinator::{
    Coordinator, PauseState, build_batch_processor, build_commit_fn,
};
use runner::pipeline_manager::PerSinkCheckpointProxy;
use sinks::kafka::KafkaSink;
use sources::postgres::PostgresSource;
use storage::{ArcStorageBackend, DurableSchemaRegistry, MemoryStorageBackend};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::sync::{mpsc, watch};
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;

const KAFKA_INTERNAL_PORT: u16 = 29092;

/// Host port for the Kafka container (env-overridable for parallel/CI runs).
fn kafka_port() -> u16 {
    std::env::var("THROUGHPUT_KAFKA_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(9391)
}
const SID: &str = "throughput";
const TOPIC: &str = "orders.events";
const SLOT: &str = "slot_throughput";
const PUBLICATION: &str = "pub_throughput";

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
/// values actually present. A high-watermark count can be satisfied by
/// duplicates while distinct rows are missing; this proves every distinct row
/// arrived.
async fn consume_unique_ids(
    brokers: &str,
    expected: usize,
    secs: u64,
) -> std::collections::HashSet<i64> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "throughput-verify")
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
            .set("group.id", "throughput-watermark")
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

// ── PostgreSQL container ─────────────────────────────────────────────────────

async fn start_pg() -> (ContainerAsync<GenericImage>, u16) {
    let image = GenericImage::new("postgres", "17")
        .with_wait_for(WaitFor::message_on_stderr("database system is ready"))
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "password")
        .with_cmd(vec![
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=10",
            "-c",
            "max_wal_senders=10",
        ]);
    let c = image.gate_owned().start().await.expect("start postgres");
    let port = c.get_host_port_ipv4(5432).await.expect("pg port");
    tokio::time::sleep(Duration::from_secs(5)).await;
    (c, port)
}

fn pg_dsn(port: u16) -> String {
    format!(
        "host=127.0.0.1 port={port} user=postgres password=password dbname=postgres"
    )
}

async fn pg_connect(port: u16) -> Result<tokio_postgres::Client> {
    let (client, conn) = tokio_postgres::connect(&pg_dsn(port), NoTls).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    Ok(client)
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

// ── Test ─────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn pg_to_kafka_backlog_drain_throughput() -> Result<()> {
    let rows = row_count();
    let (_pg, port) = start_pg().await;
    let client = pg_connect(port).await?;

    // Schema + publication + slot. The slot is created BEFORE the backlog is
    // written, so it retains the WAL and the source drains the whole backlog.
    client
        .execute(
            "CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(64), amount NUMERIC(10,2))",
            &[],
        )
        .await?;
    client
        .execute("ALTER TABLE orders REPLICA IDENTITY FULL", &[])
        .await?;
    client
        .execute(
            &format!("CREATE PUBLICATION {PUBLICATION} FOR TABLE orders"),
            &[],
        )
        .await?;
    sources::postgres::postgres_publication::register(
        &client,
        &[PUBLICATION.to_string()],
    )
    .await?;
    client
        .batch_execute(&format!(
            "SELECT pg_create_logical_replication_slot('{SLOT}', 'pgoutput')"
        ))
        .await?;

    let (_kafka, brokers) = start_kafka().await;

    // ── Write the backlog (single fast server-side generate_series insert) ──
    let write_start = Instant::now();
    client
        .batch_execute(&format!(
            "INSERT INTO orders (id, sku, amount) \
             SELECT g, 'sku-' || g, (g % 1000)::numeric \
             FROM generate_series(1, {rows}) g"
        ))
        .await?;
    let write_secs = write_start.elapsed().as_secs_f64();
    let write_rps = rows as f64 / write_secs;
    println!(
        "throughput: wrote {rows} rows in {write_secs:.2}s ({write_rps:.0} rows/s)"
    );

    // ── Wire the real production pipeline ──
    let store: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
    let registry = DurableSchemaRegistry::new(backend.clone()).await?;

    let src = PostgresSource {
        id: SID.into(),
        dsn: pg_dsn(port).into(),
        slot: SLOT.into(),
        publication: PUBLICATION.into(),
        tables: vec!["public.orders".into()],
        tenant: "acme".into(),
        pipeline: "test".into(),
        registry,
        registry_scope: sources::registry_scope::SharedRegistryScope::default(),
        outbox_prefixes: Default::default(),
        snapshot_cfg: SnapshotCfg {
            mode: SnapshotMode::Never,
            ..Default::default()
        },
        backend,
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
            brokers: brokers.clone(),
            topic: TOPIC.into(),
            key: None,
            envelope: EnvelopeCfg::Native,
            encoding: EncodingCfg::Json,
            required: Some(true),
            exactly_once: None,
            send_timeout_secs: Some(30),
            // Drain/catch-up tuning: linger.ms=0 for maximum throughput, larger
            // producer queue so batches never stall on a full librdkafka queue.
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
        // Tuned drain/catch-up batching (matches the documented high-throughput
        // profile): large batches, deep pipelining, transaction grouping off.
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
    let (_pause_tx, pause_rx) = watch::channel(PauseState::default());
    let coord_task = tokio::spawn(async move {
        coord.run(event_rx, coord_cancel, pause_rx).await
    });

    // ── Peak-RSS sampler ──
    let stop_sampler = Arc::new(AtomicBool::new(false));
    let peak_rss = Arc::new(AtomicU64::new(0));
    let sampler = {
        let stop = stop_sampler.clone();
        let peak = peak_rss.clone();
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let mib = process_rss_mib() as u64;
                peak.fetch_max(mib, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
    };

    // ── Measure the drain: poll the Kafka high-watermark until all rows land ──
    let drain_start = Instant::now();
    let mut first_delivery: Option<Instant> = None;
    let deadline = drain_start + Duration::from_secs(300);
    let mut delivered: i64 = 0;
    while delivered < rows as i64 && Instant::now() < deadline {
        delivered = kafka_high_watermark(&brokers).await;
        if first_delivery.is_none() && delivered > 0 {
            first_delivery = Some(Instant::now());
        }
        if delivered < rows as i64 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    let drain_end = Instant::now();
    let total_secs = drain_end.duration_since(drain_start).as_secs_f64();

    stop_sampler.store(true, Ordering::Relaxed);
    let _ = sampler.await;
    cancel.cancel();
    src_handle.stop();
    let _ = src_handle.join().await;
    let _ = coord_task.await;

    // ── Results ──
    let wall_eps = rows as f64 / total_secs;
    // Steady-state excludes the source connect/first-delivery startup latency.
    let steady_eps = match first_delivery {
        Some(t0) => {
            let steady_secs =
                drain_end.duration_since(t0).as_secs_f64().max(0.001);
            rows as f64 / steady_secs
        }
        None => wall_eps,
    };
    let peak = peak_rss.load(Ordering::Relaxed);

    println!("═══════════════════════════════════════════════");
    println!("  Backlog-drain throughput (PostgreSQL → Kafka)");
    println!("═══════════════════════════════════════════════");
    println!("  rows                : {rows}");
    println!(
        "  backlog write       : {write_secs:.2}s ({write_rps:.0} rows/s)"
    );
    println!("  delivered to Kafka  : {delivered}");
    println!("  drain wall-clock    : {total_secs:.2}s");
    println!("  drain throughput    : {wall_eps:.0} events/s (wall)");
    println!("  drain throughput    : {steady_eps:.0} events/s (steady-state)");
    println!(
        "  peak process RSS    : {peak} MiB (in-process; includes test harness)"
    );
    println!("═══════════════════════════════════════════════");

    assert_eq!(
        delivered, rows as i64,
        "not all rows drained to Kafka: {delivered}/{rows}"
    );

    // Correctness: every DISTINCT row must be present, not merely a matching
    // message count (duplicates could inflate the high-watermark while rows are
    // missing). The source ids are 1..=rows.
    let ids = consume_unique_ids(&brokers, rows, 180).await;
    assert_eq!(
        ids.len(),
        rows,
        "expected {rows} unique rows in Kafka, found {} (missing or duplicated)",
        ids.len()
    );
    assert!(
        ids.contains(&1) && ids.contains(&(rows as i64)),
        "boundary rows missing from Kafka"
    );

    // Conservative regression floor (dev-machine, JSON encoding). The observed
    // rate is far higher; this guards against a throughput collapse.
    assert!(
        wall_eps > 2000.0,
        "drain throughput {wall_eps:.0} ev/s below the 2000 ev/s regression floor"
    );

    Ok(())
}
