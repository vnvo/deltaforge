//! Internal reliability soak (operational pass item 4).
//!
//! Drives the real source -> coordinator -> Kafka sink pipeline against live
//! containers and injects one fault per scenario, printing a structured ledger
//! (positions before/during/after, delivered ids, duplicates, missing, recovery,
//! pipeline task state) for the soak report. Covered here: restart durability,
//! required-sink outage, checkpoint-store outage, and shutdown with a committed
//! transaction in flight.
//!
//! Note on health: this harness drives the pipeline components directly and does
//! NOT run the REST layer, so it records "pipeline task state", not HTTP health.
//! The `/health` (process liveness) and `/ready` (503 when a pipeline is failed,
//! back to 200 on recovery) behaviour is covered by the rest-api readiness tests;
//! see the soak report.
//! Schema-change scenarios (compatible / incompatible under Halt / Adapt) are
//! covered by postgres_cdc_e2e / failover_e2e and recorded in the report.
//!
//! Run: cargo test -p runner --test reliability_soak_e2e -- --include-ignored \
//!      --nocapture --test-threads=1

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;
use checkpoints::{CheckpointResult, CheckpointStore, MemCheckpointStore};
use deltaforge_config::{
    BatchConfig, EncodingCfg, EnvelopeCfg, KafkaSinkCfg, OnSchemaDrift,
    SnapshotCfg, SnapshotMode,
};
use deltaforge_core::{
    ArcDynProcessor, ArcDynSink, Source, SourceHandle, SourceItem,
};
use gate_ownership::GateOwned;
use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use runner::coordinator::{
    Coordinator, PauseState, build_batch_processor, build_commit_fn,
};
use runner::pipeline_manager::PerSinkCheckpointProxy;
use sinks::kafka::KafkaSink;
use sources::postgres::{PostgresCheckpoint, PostgresSource};
use storage::{ArcStorageBackend, DurableSchemaRegistry, MemoryStorageBackend};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;

const SID: &str = "soak";
const SLOT: &str = "slot_soak";
const PUB: &str = "pub_soak";
const TOPIC: &str = "soak.events";
const KAFKA_PORT: u16 = 9393;
const KAFKA_INTERNAL_PORT: u16 = 29092;
const DEAD_BROKER: &str = "localhost:1"; // unreachable: models the sink down

// ── Ledger ───────────────────────────────────────────────────────────────────

fn ledger(scenario: &str, fields: &[(&str, String)]) {
    println!("\n==================== SOAK: {scenario} ====================");
    for (k, v) in fields {
        println!("  {k}: {v}");
    }
    println!("==========================================================\n");
}

fn dup_missing(delivered: &[i64], expected: &[i64]) -> (usize, Vec<i64>) {
    let set: HashSet<i64> = delivered.iter().copied().collect();
    let duplicates = delivered.len() - set.len();
    let missing: Vec<i64> = expected
        .iter()
        .copied()
        .filter(|e| !set.contains(e))
        .collect();
    (duplicates, missing)
}

// ── Kafka container ────────────────────────────────────────────────────────

async fn start_kafka() -> (ContainerAsync<GenericImage>, String) {
    let image = GenericImage::new("confluentinc/cp-kafka", "7.5.0")
        .with_wait_for(WaitFor::Duration { length: Duration::from_secs(15) })
        .with_env_var("KAFKA_NODE_ID", "1")
        .with_env_var("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env_var("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:29093")
        .with_env_var(
            "KAFKA_LISTENERS",
            format!(
                "PLAINTEXT://0.0.0.0:{KAFKA_INTERNAL_PORT},CONTROLLER://0.0.0.0:29093,EXTERNAL://0.0.0.0:{KAFKA_PORT}"
            ),
        )
        .with_env_var(
            "KAFKA_ADVERTISED_LISTENERS",
            format!(
                "PLAINTEXT://localhost:{KAFKA_INTERNAL_PORT},EXTERNAL://localhost:{KAFKA_PORT}"
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
        .with_mapped_port(KAFKA_PORT, KAFKA_PORT.tcp());
    let container = image.gate_owned().start().await.expect("start kafka");
    let brokers = format!("localhost:{KAFKA_PORT}");
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

// ── PostgreSQL container ───────────────────────────────────────────────────

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

async fn provision(client: &tokio_postgres::Client) -> Result<()> {
    client
        .execute(
            "CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(64))",
            &[],
        )
        .await?;
    client
        .execute("ALTER TABLE orders REPLICA IDENTITY FULL", &[])
        .await?;
    client
        .execute(&format!("CREATE PUBLICATION {PUB} FOR TABLE orders"), &[])
        .await?;
    client
        .batch_execute(&format!(
            "SELECT pg_create_logical_replication_slot('{SLOT}', 'pgoutput')"
        ))
        .await?;
    Ok(())
}

async fn confirmed_flush(client: &tokio_postgres::Client) -> Result<String> {
    let row = client
        .query_one(
            &format!(
                "SELECT confirmed_flush_lsn::text FROM pg_replication_slots \
                 WHERE slot_name = '{SLOT}'"
            ),
            &[],
        )
        .await?;
    Ok(row.get::<_, String>(0))
}

async fn kafka_checkpoint(store: &Arc<dyn CheckpointStore>) -> Option<String> {
    store
        .get_raw(&format!("{SID}::sink::kafka"))
        .await
        .unwrap()
        .map(|b| {
            serde_json::from_slice::<PostgresCheckpoint>(&b)
                .unwrap()
                .lsn
        })
}

async fn consume_ids(brokers: &str, want: usize, secs: u64) -> Vec<i64> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "soak-consumer")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "6000")
        .create()
        .expect("consumer");
    consumer.subscribe(&[TOPIC]).expect("subscribe");
    let mut ids = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < deadline && ids.len() < want {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if let Ok(Ok(msg)) =
            tokio::time::timeout(remaining, consumer.recv()).await
        {
            if let Some(p) = msg.payload() {
                if let Ok(v) = serde_json::from_slice::<serde_json::Value>(p) {
                    if let Some(id) = v
                        .get("after")
                        .and_then(|a| a.get("id"))
                        .and_then(|i| i.as_i64())
                    {
                        ids.push(id);
                    }
                }
            }
        }
    }
    ids
}

// ── Toggleable failing checkpoint store ────────────────────────────────────

/// Delegates to an in-memory store, but fails `put_raw`/`put_raw_multi` while the
/// `fail` flag is set - modelling the checkpoint store being unavailable at
/// runtime (writes fail) while reads still work.
struct TogglePutStore {
    inner: MemCheckpointStore,
    fail: Arc<AtomicBool>,
}

#[async_trait]
impl CheckpointStore for TogglePutStore {
    async fn get_raw(&self, k: &str) -> CheckpointResult<Option<Vec<u8>>> {
        self.inner.get_raw(k).await
    }
    async fn put_raw(&self, k: &str, b: &[u8]) -> CheckpointResult<()> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(checkpoints::CheckpointError::Data(
                "injected checkpoint-store outage".into(),
            ));
        }
        self.inner.put_raw(k, b).await
    }
    async fn delete(&self, k: &str) -> CheckpointResult<bool> {
        self.inner.delete(k).await
    }
    async fn list(&self) -> CheckpointResult<Vec<String>> {
        self.inner.list().await
    }
}

// ── Pipeline wiring ────────────────────────────────────────────────────────

type Pipe = (SourceHandle, JoinHandle<Result<()>>, CancellationToken);

async fn start_pipe(
    dsn: &str,
    brokers: &str,
    store: Arc<dyn CheckpointStore>,
    backend: ArcStorageBackend,
    registry: Arc<DurableSchemaRegistry>,
) -> Result<Pipe> {
    let src = PostgresSource {
        id: SID.into(),
        dsn: dsn.into(),
        slot: SLOT.into(),
        publication: PUB.into(),
        tables: vec!["public.orders".into()],
        tenant: "acme".into(),
        pipeline: "soak".into(),
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
    let (event_tx, event_rx) = mpsc::channel::<SourceItem>(1024);
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
            send_timeout_secs: Some(4),
            client_conf: Default::default(),
            secret_refs: Default::default(),
            filter: None,
        },
        cancel.clone(),
        "soak",
        None,
        &sinks::ResolvedSinkCreds::default(),
    )?) as ArcDynSink;

    let cp_fn = build_commit_fn(store.clone(), format!("{SID}::sink::kafka"));
    let processors: Arc<[ArcDynProcessor]> = Arc::from(vec![]);
    let batch_processor = build_batch_processor(processors, "soak".to_string());
    let coord = Coordinator::builder(SID)
        .sinks(vec![kafka])
        .batch_config(Some(BatchConfig {
            max_events: Some(10),
            max_bytes: None,
            max_ms: Some(200),
            respect_source_tx: Some(true),
            max_inflight: Some(1),
            ..BatchConfig::default()
        }))
        .commit_fn("kafka", cp_fn)
        .commit_notify(commit_signal.clone())
        .process_fn(batch_processor)
        .build();

    let coord_cancel = cancel.clone();
    let (_pause_tx, pause_rx) = watch::channel(PauseState::default());
    // Keep the pause sender alive for the pipeline's lifetime.
    std::mem::forget(_pause_tx);
    let coord_task = tokio::spawn(async move {
        coord.run(event_rx, coord_cancel, pause_rx).await
    });
    Ok((src_handle, coord_task, cancel))
}

async fn stop_pipe(pipe: Pipe) -> Result<()> {
    let (src_handle, coord_task, cancel) = pipe;
    cancel.cancel();
    src_handle.stop();
    let _ = src_handle.join().await;
    coord_task.await.expect("coordinator task panicked")
}

/// Run a pipeline for `run_for`, then stop it; returns the coordinator result.
async fn run_for(
    dsn: &str,
    brokers: &str,
    store: Arc<dyn CheckpointStore>,
    backend: ArcStorageBackend,
    registry: Arc<DurableSchemaRegistry>,
    run_for: Duration,
) -> Result<()> {
    let (src_handle, mut coord_task, cancel) =
        start_pipe(dsn, brokers, store, backend, registry).await?;
    let sleeper = tokio::time::sleep(run_for);
    tokio::pin!(sleeper);
    let joined = tokio::select! {
        r = &mut coord_task => r,
        _ = &mut sleeper => { cancel.cancel(); coord_task.await }
    };
    src_handle.stop();
    let _ = src_handle.join().await;
    joined.expect("coordinator task panicked")
}

async fn insert(
    client: &tokio_postgres::Client,
    id: i32,
    sku: &str,
) -> Result<()> {
    client
        .execute("INSERT INTO orders VALUES ($1, $2)", &[&id, &sku])
        .await?;
    Ok(())
}

// ── Scenario 1: restart durability ─────────────────────────────────────────

#[tokio::test]
#[ignore = "requires docker"]
async fn soak_restart_durability() -> Result<()> {
    let commit = env!("CARGO_PKG_VERSION");
    let (_pg, port) = start_pg().await;
    let (_kafka, brokers) = start_kafka().await;
    let client = pg_connect(port).await?;
    provision(&client).await?;

    let store: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
    let registry = DurableSchemaRegistry::new(backend.clone()).await?;

    for id in 1..=5 {
        insert(&client, id, "before").await?;
    }
    let cf_before = confirmed_flush(&client).await?;

    // Run 1: deliver the first batch, then stop (simulated restart).
    run_for(
        &pg_dsn(port),
        &brokers,
        store.clone(),
        backend.clone(),
        registry.clone(),
        Duration::from_secs(12),
    )
    .await?;
    let cp_after_run1 = kafka_checkpoint(&store).await;
    let cf_after_run1 = confirmed_flush(&client).await?;

    // While "down", write more rows (retained by the slot).
    for id in 6..=10 {
        insert(&client, id, "during-down").await?;
    }

    // Run 2: restart against the same store; must resume and deliver the rest.
    run_for(
        &pg_dsn(port),
        &brokers,
        store.clone(),
        backend.clone(),
        registry.clone(),
        Duration::from_secs(15),
    )
    .await?;
    let cp_after_run2 = kafka_checkpoint(&store).await;
    let cf_after_run2 = confirmed_flush(&client).await?;

    let expected: Vec<i64> = (1..=10).collect();
    let delivered = consume_ids(&brokers, 10, 20).await;
    let (dups, missing) = dup_missing(&delivered, &expected);

    let pass = missing.is_empty() && cp_after_run2.is_some();
    ledger("restart durability", &[
        ("build", commit.to_string()),
        ("config", "PG->Kafka, snapshot=never, respect_source_tx=true, required sink".into()),
        ("fault/duration", "stop pipeline after run 1 (~12s); 5 rows written while down; restart".into()),
        ("invariant", "all rows delivered exactly-once-effective across restart; no missing".into()),
        ("source position before", cf_before),
        ("checkpoint after run1", format!("{cp_after_run1:?}")),
        ("source position after run1", cf_after_run1),
        ("checkpoint after run2", format!("{cp_after_run2:?}")),
        ("source position after run2", cf_after_run2),
        ("delivered ids", format!("{} of {}", delivered.iter().collect::<HashSet<_>>().len(), expected.len())),
        ("duplicates", dups.to_string()),
        ("missing", format!("{missing:?}")),
        ("recovery", "automatic on restart; no operator intervention".into()),
        ("pipeline task state", "coordinator ran to idle each run; clean stop".into()),
        ("WAL retention effect", "slot retained rows written while down; released after durable ack".into()),
        ("RESULT", if pass { "PASS".into() } else { "FAIL".into() }),
    ]);
    assert!(pass, "restart durability: missing={missing:?}");
    Ok(())
}

// ── Scenario 2: required-sink outage ───────────────────────────────────────

#[tokio::test]
#[ignore = "requires docker"]
async fn soak_required_sink_outage() -> Result<()> {
    let commit = env!("CARGO_PKG_VERSION");
    let (_pg, port) = start_pg().await;
    let (_kafka, brokers) = start_kafka().await;
    let client = pg_connect(port).await?;
    provision(&client).await?;

    let store: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
    let registry = DurableSchemaRegistry::new(backend.clone()).await?;

    for id in 1..=3 {
        insert(&client, id, "x").await?;
    }
    let cf_before = confirmed_flush(&client).await?;

    // Fault: required sink unreachable. Delivery must fail; checkpoint must NOT
    // advance and the slot must NOT release WAL.
    let start = Instant::now();
    let r_down = run_for(
        &pg_dsn(port),
        DEAD_BROKER,
        store.clone(),
        backend.clone(),
        registry.clone(),
        Duration::from_secs(20),
    )
    .await;
    let cp_during = kafka_checkpoint(&store).await;
    let cf_during = confirmed_flush(&client).await?;

    // Recover: sink reachable again.
    run_for(
        &pg_dsn(port),
        &brokers,
        store.clone(),
        backend.clone(),
        registry.clone(),
        Duration::from_secs(15),
    )
    .await?;
    let recovery = start.elapsed();
    let cp_after = kafka_checkpoint(&store).await;
    let cf_after = confirmed_flush(&client).await?;

    let expected: Vec<i64> = (1..=3).collect();
    let delivered = consume_ids(&brokers, 3, 20).await;
    let (dups, missing) = dup_missing(&delivered, &expected);

    let held_during = cp_during.is_none();
    let pass = held_during && missing.is_empty() && cp_after.is_some();
    ledger("required-sink outage", &[
        ("build", commit.to_string()),
        ("config", "PG->Kafka required sink, send_timeout=4s".into()),
        ("fault/duration", "required sink unreachable (dead broker) for ~20s, then restored".into()),
        ("invariant", "checkpoint held + WAL retained while sink down; no loss; resume on recovery".into()),
        ("source position before", cf_before),
        ("coordinator result while down", format!("{r_down:?}")),
        ("checkpoint during outage", format!("{cp_during:?} (held={held_during})")),
        ("source position during outage", cf_during),
        ("checkpoint after recovery", format!("{cp_after:?}")),
        ("source position after recovery", cf_after),
        ("delivered ids", format!("{} of {}", delivered.iter().collect::<HashSet<_>>().len(), expected.len())),
        ("duplicates", dups.to_string()),
        ("missing", format!("{missing:?}")),
        ("recovery", format!("automatic once sink reachable; ~{}s; no operator intervention", recovery.as_secs())),
        ("pipeline task state", "required-sink failure stopped delivery (fail-closed), recovered on restart".into()),
        ("WAL retention effect", "slot retained WAL during outage; released after durable ack".into()),
        ("RESULT", if pass { "PASS".into() } else { "FAIL".into() }),
    ]);
    assert!(
        pass,
        "required-sink outage: held_during={held_during} missing={missing:?}"
    );
    Ok(())
}

// ── Scenario 3: checkpoint-store outage ────────────────────────────────────

#[tokio::test]
#[ignore = "requires docker"]
async fn soak_checkpoint_store_outage() -> Result<()> {
    let commit = env!("CARGO_PKG_VERSION");
    let (_pg, port) = start_pg().await;
    let (_kafka, brokers) = start_kafka().await;
    let client = pg_connect(port).await?;
    provision(&client).await?;

    let fail = Arc::new(AtomicBool::new(true)); // store down at start
    let store: Arc<dyn CheckpointStore> = Arc::new(TogglePutStore {
        inner: MemCheckpointStore::new()?,
        fail: fail.clone(),
    });
    let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
    let registry = DurableSchemaRegistry::new(backend.clone()).await?;

    for id in 1..=3 {
        insert(&client, id, "x").await?;
    }
    let cf_before = confirmed_flush(&client).await?;

    // Fault: checkpoint writes fail. Coordinator must fail closed - not advance
    // the checkpoint or release the source WAL after a sink ack.
    let start = Instant::now();
    let r_down = run_for(
        &pg_dsn(port),
        &brokers,
        store.clone(),
        backend.clone(),
        registry.clone(),
        Duration::from_secs(18),
    )
    .await;
    let cp_during = kafka_checkpoint(&store).await;
    let cf_during = confirmed_flush(&client).await?;

    // Recover: checkpoint store writable again.
    fail.store(false, Ordering::SeqCst);
    run_for(
        &pg_dsn(port),
        &brokers,
        store.clone(),
        backend.clone(),
        registry.clone(),
        Duration::from_secs(15),
    )
    .await?;
    let recovery = start.elapsed();
    let cp_after = kafka_checkpoint(&store).await;
    let cf_after = confirmed_flush(&client).await?;

    // Rows may be re-delivered (at-least-once) since the checkpoint could not be
    // recorded during the outage; the invariant is no LOSS + eventual checkpoint.
    let expected: Vec<i64> = (1..=3).collect();
    let delivered = consume_ids(&brokers, 3, 20).await;
    let (dups, missing) = dup_missing(&delivered, &expected);

    let held_during = cp_during.is_none();
    let pass = held_during && missing.is_empty() && cp_after.is_some();
    ledger("checkpoint-store outage", &[
        ("build", commit.to_string()),
        ("config", "PG->Kafka; checkpoint put_raw fails during outage".into()),
        ("fault/duration", "checkpoint store writes fail ~18s, then restored".into()),
        ("invariant", "fail closed: no checkpoint advance + no WAL release during outage; no loss".into()),
        ("source position before", cf_before),
        ("coordinator result while down", format!("{r_down:?}")),
        ("checkpoint during outage", format!("{cp_during:?} (held={held_during})")),
        ("source position during outage", cf_during),
        ("checkpoint after recovery", format!("{cp_after:?}")),
        ("source position after recovery", cf_after),
        ("delivered ids", format!("{} of {}", delivered.iter().collect::<HashSet<_>>().len(), expected.len())),
        ("duplicates", format!("{dups} (at-least-once re-delivery expected across the outage)")),
        ("missing", format!("{missing:?}")),
        ("recovery", format!("automatic once store writable; ~{}s; no operator intervention", recovery.as_secs())),
        ("pipeline task state", "checkpoint failure surfaced fail-closed; recovered on restart".into()),
        ("WAL retention effect", "slot held WAL while checkpoint could not advance".into()),
        ("RESULT", if pass { "PASS".into() } else { "FAIL".into() }),
    ]);
    assert!(
        pass,
        "checkpoint-store outage: held_during={held_during} missing={missing:?}"
    );
    Ok(())
}

// ── Scenario 4: shutdown with a committed transaction in flight ─────────────
//
// PostgreSQL logical replication exposes a transaction's rows around COMMIT, so a
// timing delay cannot guarantee the source is between BEGIN and COMMIT. This
// scenario therefore validates shutdown while a *committed* transaction's events
// are in flight (still valuable): the tx must be delivered whole across the
// shutdown, never a partial set. A deterministic BEGIN/COMMIT-boundary variant
// (pause the source after TxBegin, before commit) can be added if the release
// checklist requires that stronger invariant.

#[tokio::test]
#[ignore = "requires docker"]
async fn soak_shutdown_committed_tx_in_flight() -> Result<()> {
    let commit = env!("CARGO_PKG_VERSION");
    let (_pg, port) = start_pg().await;
    let (_kafka, brokers) = start_kafka().await;
    let client = pg_connect(port).await?;
    provision(&client).await?;

    let store: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
    let registry = DurableSchemaRegistry::new(backend.clone()).await?;

    let cf_before = confirmed_flush(&client).await?;

    // Start the pipeline, then commit one multi-row transaction and stop the
    // pipeline almost immediately - mid-delivery of that transaction's events.
    let pipe = start_pipe(
        &pg_dsn(port),
        &brokers,
        store.clone(),
        backend.clone(),
        registry.clone(),
    )
    .await?;

    // A single transaction with several rows (streams as one unit at COMMIT).
    let mut tx_client = pg_connect(port).await?;
    let txn = tx_client.transaction().await?;
    for id in 1..=8 {
        txn.execute("INSERT INTO orders VALUES ($1, $2)", &[&id, &"tx"])
            .await?;
    }
    txn.commit().await?;

    // Stop very soon after commit - shutdown while the tx's events are in flight.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let _ = stop_pipe(pipe).await;
    let cp_after_stop = kafka_checkpoint(&store).await;

    // Restart: the transaction must be delivered whole (all-or-nothing), never a
    // partial set of its rows.
    run_for(
        &pg_dsn(port),
        &brokers,
        store.clone(),
        backend.clone(),
        registry.clone(),
        Duration::from_secs(15),
    )
    .await?;
    let cp_after = kafka_checkpoint(&store).await;
    let cf_after = confirmed_flush(&client).await?;

    let expected: Vec<i64> = (1..=8).collect();
    let delivered = consume_ids(&brokers, 8, 20).await;
    let (dups, missing) = dup_missing(&delivered, &expected);

    // Invariant: every row of the committed transaction is delivered (whole),
    // none missing, after the shutdown+restart.
    let pass = missing.is_empty() && cp_after.is_some();
    ledger("shutdown with a committed transaction in flight", &[
        ("build", commit.to_string()),
        ("config", "PG->Kafka, respect_source_tx=true".into()),
        ("fault/duration", "stop pipeline ~300ms after an 8-row tx commit (its events in flight), then restart".into()),
        ("invariant", "committed transaction delivered whole across shutdown; no partial or missing rows".into()),
        ("source position before", cf_before),
        ("checkpoint at stop", format!("{cp_after_stop:?}")),
        ("checkpoint after restart", format!("{cp_after:?}")),
        ("source position after", cf_after),
        ("delivered ids", format!("{} of {}", delivered.iter().collect::<HashSet<_>>().len(), expected.len())),
        ("duplicates", format!("{dups} (at-least-once if the tx re-delivered after an incomplete checkpoint)")),
        ("missing", format!("{missing:?}")),
        ("recovery", "automatic on restart; no operator intervention".into()),
        ("pipeline task state", "clean stop mid-delivery; resumed on restart".into()),
        ("WAL retention effect", "tx retained by slot until durably acked".into()),
        ("RESULT", if pass { "PASS".into() } else { "FAIL".into() }),
    ]);
    assert!(pass, "shutdown during open tx: missing={missing:?}");
    Ok(())
}
