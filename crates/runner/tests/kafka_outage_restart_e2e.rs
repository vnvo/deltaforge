//! Gate 1 durability regression: a Kafka outage that spans a DeltaForge restart must
//! not lose events, and Kafka's per-sink checkpoint must never advance before Kafka
//! acknowledges delivery.
//!
//! Sequence (real PostgreSQL source, real Kafka sink, durable checkpoint store, and the
//! production `PerSinkCheckpointProxy`): Kafka is made unavailable and three PostgreSQL
//! rows are committed; the pipeline runs while Kafka is down (delivery fails, so Kafka's
//! checkpoint does not advance); the pipeline restarts while Kafka is still down (still no
//! advance); then Kafka is restored, the retained rows are delivered, and the checkpoint
//! advances. Then assert every committed row arrives at Kafka (measuring any duplicates)
//! and that the stored Kafka checkpoint moved only after delivery.
//!
//! The outage is modelled as an unreachable broker address (a Kafka-unavailable
//! condition) rather than stopping the container, so the run is deterministic; "restore"
//! points the sink at the live broker. Requires docker.
//!
//! This originally reproduced a durability bug: the PostgreSQL source reported `wal_end`
//! as the flushed LSN to PostgreSQL as soon as each message was handed to the coordinator
//! (before any sink acknowledged it), and it persisted its read position as the resume
//! checkpoint on a clean stop. PostgreSQL then released the WAL and a restart resumed
//! ahead of un-acknowledged deliveries, losing them. The fix bounds both the WAL feedback
//! and the resume checkpoint by the durable per-sink minimum (only what every required
//! sink has acknowledged); this test now passes and guards that invariant. It also
//! records the observed `confirmed_flush_lsn` at each stage as evidence.
#![cfg(test)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use checkpoints::{CheckpointStore, MemCheckpointStore};
use deltaforge_config::{
    BatchConfig, EncodingCfg, EnvelopeCfg, KafkaSinkCfg, OnSchemaDrift,
    SnapshotCfg, SnapshotMode,
};
use deltaforge_core::{ArcDynProcessor, ArcDynSink, Source, SourceItem};
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
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;

const KAFKA_PORT: u16 = 9291;
const KAFKA_INTERNAL_PORT: u16 = 29092;
const DEAD_BROKER: &str = "localhost:1"; // unreachable: models Kafka down
const SID: &str = "kafkaoutage";
const TOPIC: &str = "orders.events";

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
    let container = image.start().await.expect("start kafka");
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
    let c = image.start().await.expect("start postgres");
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

/// The slot's `confirmed_flush_lsn` - how far PostgreSQL believes the consumer has
/// durably flushed, and therefore how much WAL it may release.
async fn confirmed_flush(client: &tokio_postgres::Client) -> Result<String> {
    let row = client
        .query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots \
             WHERE slot_name = 'slot_outage'",
            &[],
        )
        .await?;
    Ok(row.get::<_, String>(0))
}

/// Whether `a` is strictly greater than `b` as PostgreSQL LSNs.
async fn lsn_gt(
    client: &tokio_postgres::Client,
    a: &str,
    b: &str,
) -> Result<bool> {
    let row = client
        .query_one("SELECT $1::text::pg_lsn > $2::text::pg_lsn", &[&a, &b])
        .await?;
    Ok(row.get::<_, bool>(0))
}

// ── Pipeline wiring (production components) ─────────────────────────────────

/// Run the real source -> coordinator -> Kafka sink pipeline for `run_for`, sharing the
/// durable `store` through the production `PerSinkCheckpointProxy`. Returns the
/// coordinator's result (an `Err` when a required-sink outage stops it).
#[allow(clippy::too_many_arguments)]
async fn run_pipeline(
    src_dsn: &str,
    brokers: &str,
    store: Arc<dyn CheckpointStore>,
    backend: ArcStorageBackend,
    registry: Arc<DurableSchemaRegistry>,
    run_for: Duration,
) -> Result<()> {
    let src = PostgresSource {
        id: SID.into(),
        dsn: src_dsn.into(),
        slot: "slot_outage".into(),
        publication: "pub_outage".into(),
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
    };
    let src: Arc<dyn Source> = Arc::new(src);
    // Change-driven feedback: the coordinator signals this on each per-sink commit.
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
            // Short timeout so an outage surfaces as a failed delivery quickly.
            send_timeout_secs: Some(4),
            client_conf: HashMap::new(),
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
            max_events: Some(10),
            max_bytes: None,
            max_ms: Some(200),
            respect_source_tx: None,
            max_inflight: Some(1),
            ..BatchConfig::default()
        }))
        .commit_fn("kafka", cp_fn)
        .commit_notify(commit_signal.clone())
        .process_fn(batch_processor)
        .build();

    let coord_cancel = cancel.clone();
    let (_pause_tx, pause_rx) = watch::channel(PauseState::default());
    let mut coord_task = tokio::spawn(async move {
        coord.run(event_rx, coord_cancel, pause_rx).await
    });

    let sleeper = tokio::time::sleep(run_for);
    tokio::pin!(sleeper);
    let joined = tokio::select! {
        r = &mut coord_task => r,
        _ = &mut sleeper => {
            // Still running (delivery succeeded and the source went idle): stop cleanly.
            cancel.cancel();
            coord_task.await
        }
    };
    src_handle.stop();
    let _ = src_handle.join().await;
    joined.expect("coordinator task panicked")
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

/// Consume the topic and return the `after.id` of every message (Native envelope).
async fn consume_ids(brokers: &str, want: usize, secs: u64) -> Vec<i64> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "gate1-consumer")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "6000")
        .create()
        .expect("consumer");
    consumer.subscribe(&[TOPIC]).expect("subscribe");

    let mut ids = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < deadline && ids.len() < want {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining, consumer.recv()).await {
            Ok(Ok(msg)) => {
                if let Some(p) = msg.payload() {
                    if let Ok(v) =
                        serde_json::from_slice::<serde_json::Value>(p)
                    {
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
            _ => continue,
        }
    }
    ids
}

#[tokio::test]
#[ignore = "requires docker"]
async fn kafka_outage_across_restart_loses_no_events_and_holds_checkpoint()
-> Result<()> {
    let (_pg, port) = start_pg().await;
    let client = pg_connect(port).await?;

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
        .batch_execute(
            "SELECT pg_drop_replication_slot('slot_outage') \
             WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='slot_outage')",
        )
        .await
        .ok();
    client
        .execute("CREATE PUBLICATION pub_outage FOR TABLE orders", &[])
        .await?;
    client
        .batch_execute(
            "SELECT pg_create_logical_replication_slot('slot_outage', 'pgoutput')",
        )
        .await?;

    // Durable checkpoint store + schema registry shared across every restart.
    let store: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
    let registry = DurableSchemaRegistry::new(backend.clone()).await?;

    // (1) Kafka unavailable -> commit three rows (retained by the slot).
    for (id, sku) in [(1i32, "a"), (2, "b"), (3, "c")] {
        client
            .execute("INSERT INTO orders VALUES ($1, $2)", &[&id, &sku])
            .await?;
    }
    let cf_initial = confirmed_flush(&client).await?;
    println!("gate1: confirmed_flush after inserts = {cf_initial}");

    // (2) Run while Kafka is down: delivery fails, checkpoint must not advance.
    let r1 = run_pipeline(
        &pg_dsn(port),
        DEAD_BROKER,
        store.clone(),
        backend.clone(),
        registry.clone(),
        Duration::from_secs(25),
    )
    .await;
    let cf_run1 = confirmed_flush(&client).await?;
    println!(
        "gate1: run 1 (kafka down) result = {r1:?}, confirmed_flush = {cf_run1}"
    );
    assert!(
        kafka_checkpoint(&store).await.is_none(),
        "Kafka checkpoint must not advance while Kafka is unavailable"
    );
    assert!(
        !lsn_gt(&client, &cf_run1, &cf_initial).await?,
        "confirmed_flush_lsn must not advance past the last acked checkpoint while \
         Kafka is unavailable (was {cf_initial}, now {cf_run1})"
    );

    // (3) Restart while Kafka is still down: still no advance, still no loss.
    let r2 = run_pipeline(
        &pg_dsn(port),
        DEAD_BROKER,
        store.clone(),
        backend.clone(),
        registry.clone(),
        Duration::from_secs(25),
    )
    .await;
    let cf_run2 = confirmed_flush(&client).await?;
    println!(
        "gate1: run 2 (kafka down, restart) result = {r2:?}, confirmed_flush = {cf_run2}"
    );
    assert!(
        kafka_checkpoint(&store).await.is_none(),
        "Kafka checkpoint must still not advance across the restart"
    );
    assert!(
        !lsn_gt(&client, &cf_run2, &cf_initial).await?,
        "confirmed_flush_lsn must still not advance across the restart while Kafka is \
         unavailable (was {cf_initial}, now {cf_run2})"
    );

    // (4) Restore Kafka: the retained rows are delivered and the checkpoint advances.
    let (_kafka, brokers) = start_kafka().await;
    run_pipeline(
        &pg_dsn(port),
        &brokers,
        store.clone(),
        backend.clone(),
        registry.clone(),
        Duration::from_secs(20),
    )
    .await?;

    let cp_after = kafka_checkpoint(&store).await;
    assert!(
        cp_after.is_some(),
        "Kafka checkpoint must advance after Kafka acknowledges delivery"
    );

    // Every committed row must have arrived at Kafka. Duplicates are acceptable under
    // at-least-once, but here delivery happened exactly once (nothing reached Kafka
    // while it was down), so we measure and report the count.
    let ids = consume_ids(&brokers, 3, 30).await;
    for want in [1i64, 2, 3] {
        assert!(
            ids.contains(&want),
            "committed row {want} must arrive at Kafka; got {ids:?}"
        );
    }
    let mut sorted = ids.clone();
    sorted.sort_unstable();
    sorted.dedup();
    let duplicates = ids.len() - sorted.len();
    println!(
        "gate1: delivered ids={ids:?} unique={} duplicates={duplicates} checkpoint={cp_after:?}",
        sorted.len()
    );

    // (5) Restart once more (Kafka still up, no new data). On resume the source
    // initializes BOTH its read position and the WAL feedback from the same durable
    // checkpoint, so PostgreSQL's confirmed_flush_lsn advances to the acknowledged
    // position and the now-durable WAL is released.
    run_pipeline(
        &pg_dsn(port),
        &brokers,
        store.clone(),
        backend.clone(),
        registry.clone(),
        Duration::from_secs(12),
    )
    .await?;
    let cf_after = confirmed_flush(&client).await?;
    println!(
        "gate1: after feedback restart, confirmed_flush = {cf_after} (was {cf_initial}, checkpoint {cp_after:?})"
    );
    assert!(
        lsn_gt(&client, &cf_after, &cf_initial).await?,
        "confirmed_flush_lsn must advance to the durable checkpoint after Kafka \
         acknowledges delivery (was {cf_initial}, now {cf_after})"
    );

    Ok(())
}
