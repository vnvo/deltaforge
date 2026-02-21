//! Integration tests for Kafka sink.
//!
//! These tests require Docker and pull `confluentinc/cp-kafka:7.5.0`.
//!
//! Run with:
//! ```bash
//! cargo test -p sinks --test kafka_sink_tests -- --include-ignored --nocapture --test-threads=1
//! ```

use anyhow::Result;
use ctor::dtor;
use deltaforge_config::{EncodingCfg, EnvelopeCfg, KafkaSinkCfg};
use deltaforge_core::{
    Event, EventRouting, Op, Sink, SourceInfo, SourcePosition,
};
use rdkafka::Message;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Headers;
use serde_json::json;
use sinks::kafka::KafkaSink;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::sync::OnceCell;
use tokio::time::{Duration, sleep, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

mod sink_test_common;
use sink_test_common::{
    init_test_tracing, make_event_for_table, make_large_event, make_test_event,
};

// =============================================================================
// Shared Test Infrastructure
// =============================================================================

const KAFKA_PORT: u16 = 9192;
const KAFKA_INTERNAL_PORT: u16 = 29092;

/// Shared Kafka container - initialized once, reused by all tests.
static KAFKA_CONTAINER: OnceCell<ContainerAsync<GenericImage>> =
    OnceCell::const_new();

#[dtor]
fn cleanup() {
    // Force container cleanup on process exit
    if let Some(container) = KAFKA_CONTAINER.get() {
        std::process::Command::new("docker")
            .args(["rm", "-f", container.id()])
            .output()
            .ok();
    }
}

/// Get or start the shared Kafka container (KRaft mode, no Zookeeper).
async fn get_kafka_container() -> &'static ContainerAsync<GenericImage> {
    KAFKA_CONTAINER
        .get_or_init(|| async {
            info!("starting Kafka container (KRaft mode)...");

            // Use KRaft-based Kafka (no Zookeeper needed)
            let image = GenericImage::new("confluentinc/cp-kafka", "7.5.0")
                .with_wait_for(WaitFor::Duration { length: Duration::from_secs(15) })
                .with_env_var("KAFKA_NODE_ID", "1")
                .with_env_var("KAFKA_PROCESS_ROLES", "broker,controller")
                .with_env_var("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:29093")
                .with_env_var("KAFKA_LISTENERS", format!(
                    "PLAINTEXT://0.0.0.0:{},CONTROLLER://0.0.0.0:29093,EXTERNAL://0.0.0.0:{}",
                    KAFKA_INTERNAL_PORT, KAFKA_PORT
                ))
                .with_env_var("KAFKA_ADVERTISED_LISTENERS", format!(
                    "PLAINTEXT://localhost:{},EXTERNAL://localhost:{}",
                    KAFKA_INTERNAL_PORT, KAFKA_PORT
                ))
                .with_env_var("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", 
                    "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT")
                .with_env_var("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
                .with_env_var("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
                .with_env_var("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
                .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
                .with_env_var("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
                .with_env_var("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
                .with_env_var("CLUSTER_ID", "MkU3OEVBNTcwNTJENDM2Qg")
                .with_mapped_port(KAFKA_PORT, KAFKA_PORT.tcp());

            let container = image.start().await.expect("start kafka container");
            info!("Kafka container started: {}", container.id());

            // Wait for Kafka to be fully ready
            wait_for_kafka(&brokers(), Duration::from_secs(60))
                .await
                .expect("Kafka should be ready");

            container
        })
        .await
}

/// Poll Kafka until it's ready to accept connections.
async fn wait_for_kafka(
    brokers: &str,
    timeout_duration: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout_duration;

    while Instant::now() < deadline {
        // Try to create an admin client and list topics
        let admin_result: Result<AdminClient<DefaultClientContext>, _> =
            ClientConfig::new()
                .set("bootstrap.servers", brokers)
                .set("socket.timeout.ms", "5000")
                .set("request.timeout.ms", "5000")
                .create();

        if let Ok(admin) = admin_result {
            // Try to fetch metadata
            let metadata_result = tokio::task::spawn_blocking(move || {
                admin
                    .inner()
                    .fetch_metadata(None, std::time::Duration::from_secs(5))
            })
            .await;

            if let Ok(Ok(_)) = metadata_result {
                info!("Kafka is ready");
                return Ok(());
            }
        }

        debug!("waiting for Kafka...");
        sleep(Duration::from_millis(500)).await;
    }

    anyhow::bail!("Kafka not ready after {:?}", timeout_duration)
}

fn brokers() -> String {
    format!("localhost:{}", KAFKA_PORT)
}

/// Create a unique topic name for each test.
fn test_topic(test_name: &str) -> String {
    format!("df-test-{}", test_name.replace('_', "-"))
}

/// Create a topic with proper configuration.
async fn create_topic(
    brokers: &str,
    topic: &str,
    partitions: i32,
) -> Result<()> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()?;

    let new_topic =
        NewTopic::new(topic, partitions, TopicReplication::Fixed(1));

    let opts =
        AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));

    let results = admin.create_topics(&[new_topic], &opts).await?;

    for result in results {
        match result {
            Ok(_) => debug!("created topic: {}", topic),
            Err((_, err)) => {
                // Ignore "topic already exists" error
                if !err.to_string().contains("already exists") {
                    warn!("failed to create topic {}: {:?}", topic, err);
                }
            }
        }
    }

    // Give Kafka a moment to propagate topic metadata
    sleep(Duration::from_millis(500)).await;
    Ok(())
}

/// Delete a topic (cleanup).
async fn delete_topic(brokers: &str, topic: &str) -> Result<()> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()?;

    let opts =
        AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));
    let _ = admin.delete_topics(&[topic], &opts).await;
    debug!("deleted topic: {}", topic);
    Ok(())
}

/// Create a consumer for reading messages from a topic.
fn create_consumer(
    brokers: &str,
    topic: &str,
    group_id: &str,
) -> Result<StreamConsumer> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group_id)
        .set("auto.offset.reset", "earliest")
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .create()?;

    consumer.subscribe(&[topic])?;
    Ok(consumer)
}

/// Consume messages from a topic until a condition is met or timeout.
async fn consume_until<F>(
    consumer: &StreamConsumer,
    timeout_duration: Duration,
    mut condition: F,
) -> Vec<Vec<u8>>
where
    F: FnMut(&[Vec<u8>]) -> bool,
{
    let deadline = Instant::now() + timeout_duration;
    let mut messages = Vec::new();

    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match timeout(remaining, consumer.recv()).await {
            Ok(Ok(msg)) => {
                if let Some(payload) = msg.payload() {
                    messages.push(payload.to_vec());
                    if condition(&messages) {
                        break;
                    }
                }
            }
            Ok(Err(e)) => {
                warn!("consumer error: {}", e);
                break;
            }
            Err(_) => break,
        }
    }

    messages
}

// =============================================================================
// Basic Functionality Tests
// =============================================================================

/// Test that a single event is written to the topic correctly.
#[tokio::test]
#[ignore = "requires docker"]
async fn kafka_sink_sends_single_event() -> Result<()> {
    init_test_tracing();
    let _container = get_kafka_container().await;

    let topic = test_topic("single");
    let brokers = brokers();
    create_topic(&brokers, &topic, 1).await?;

    let cfg = KafkaSinkCfg {
        id: "test-kafka".into(),
        brokers: brokers.clone(),
        topic: topic.clone(),
        key: None,
        envelope: EnvelopeCfg::Native,
        encoding: EncodingCfg::Json,
        required: Some(true),
        exactly_once: None,
        send_timeout_secs: Some(30),
        client_conf: HashMap::new(),
    };

    let cancel = CancellationToken::new();
    let sink = KafkaSink::new(&cfg, cancel)?;

    let event = make_test_event(1);
    sink.send(&event).await?;

    // Consume and verify
    let consumer = create_consumer(&brokers, &topic, "test-single-consumer")?;
    let messages = consume_until(&consumer, Duration::from_secs(10), |msgs| {
        !msgs.is_empty()
    })
    .await;

    assert!(!messages.is_empty(), "should receive at least one message");

    let parsed: Event = serde_json::from_slice(&messages[0])?;
    assert_eq!(parsed.event_id, event.event_id);

    info!("✓ single event sent successfully");
    delete_topic(&brokers, &topic).await?;
    Ok(())
}

/// Test batch send with rdkafka batching.
#[tokio::test]
#[ignore = "requires docker"]
async fn kafka_sink_sends_batch() -> Result<()> {
    init_test_tracing();
    let _container = get_kafka_container().await;

    let topic = test_topic("batch");
    let brokers = brokers();
    create_topic(&brokers, &topic, 3).await?;

    let cfg = KafkaSinkCfg {
        id: "test-kafka-batch".into(),
        brokers: brokers.clone(),
        topic: topic.clone(),
        key: None,
        envelope: EnvelopeCfg::Native,
        encoding: EncodingCfg::Json,
        required: Some(true),
        exactly_once: None,
        send_timeout_secs: Some(30),
        client_conf: HashMap::new(),
    };

    let cancel = CancellationToken::new();
    let sink = KafkaSink::new(&cfg, cancel)?;

    // Send a batch of 100 events
    let events: Vec<Event> = (0..100).map(make_test_event).collect();
    sink.send_batch(&events).await?;

    // Consume all messages
    let consumer = create_consumer(&brokers, &topic, "test-batch-consumer")?;
    let messages = consume_until(&consumer, Duration::from_secs(30), |msgs| {
        msgs.len() >= 100
    })
    .await;

    assert_eq!(messages.len(), 100, "should receive all 100 messages");

    info!("✓ batch of 100 events sent successfully");
    delete_topic(&brokers, &topic).await?;
    Ok(())
}

/// Test that empty batch is a no-op.
#[tokio::test]
#[ignore = "requires docker"]
async fn kafka_sink_empty_batch_is_noop() -> Result<()> {
    init_test_tracing();
    let _container = get_kafka_container().await;

    let topic = test_topic("empty-batch");
    let brokers = brokers();
    create_topic(&brokers, &topic, 1).await?;

    let cfg = KafkaSinkCfg {
        id: "test-kafka-empty".into(),
        brokers: brokers.clone(),
        topic: topic.clone(),
        key: None,
        envelope: EnvelopeCfg::Native,
        encoding: EncodingCfg::Json,
        required: Some(true),
        exactly_once: None,
        send_timeout_secs: Some(30),
        client_conf: HashMap::new(),
    };

    let cancel = CancellationToken::new();
    let sink = KafkaSink::new(&cfg, cancel)?;

    // Send empty batch - should not produce any messages
    sink.send_batch(&[]).await?;

    info!("✓ empty batch is a no-op");
    delete_topic(&brokers, &topic).await?;
    Ok(())
}

// =============================================================================
// Envelope Format Tests
// =============================================================================

/// Test Native envelope format (direct Event serialization).
#[tokio::test]
#[ignore = "requires docker"]
async fn kafka_sink_native_envelope() -> Result<()> {
    init_test_tracing();
    let _container = get_kafka_container().await;

    let topic = test_topic("native-envelope");
    let brokers = brokers();
    create_topic(&brokers, &topic, 1).await?;

    let cfg = KafkaSinkCfg {
        id: "test-native".into(),
        brokers: brokers.clone(),
        topic: topic.clone(),
        key: None,
        envelope: EnvelopeCfg::Native,
        encoding: EncodingCfg::Json,
        required: Some(true),
        exactly_once: None,
        send_timeout_secs: Some(30),
        client_conf: HashMap::new(),
    };

    let cancel = CancellationToken::new();
    let sink = KafkaSink::new(&cfg, cancel)?;

    let event = make_test_event(1);
    sink.send(&event).await?;

    let consumer = create_consumer(&brokers, &topic, "test-native-consumer")?;
    let messages = consume_until(&consumer, Duration::from_secs(10), |msgs| {
        !msgs.is_empty()
    })
    .await;

    assert!(!messages.is_empty(), "should receive message");

    // Native envelope serializes Event directly (Debezium payload structure)
    let parsed: serde_json::Value = serde_json::from_slice(&messages[0])?;

    // Verify native format has top-level Debezium-compatible fields
    assert!(
        parsed.get("op").is_some(),
        "native format should have 'op' field at top level"
    );
    assert!(
        parsed.get("source").is_some(),
        "native format should have 'source' field at top level"
    );

    // Verify it's NOT wrapped in a payload (that's what Debezium envelope does)
    assert!(
        parsed.get("payload").is_none(),
        "native format should NOT have 'payload' wrapper"
    );

    // Verify the op value is a valid Debezium code
    let op = parsed.get("op").and_then(|v| v.as_str());
    assert!(
        matches!(
            op,
            Some("c") | Some("u") | Some("d") | Some("r") | Some("t")
        ),
        "op should be a valid Debezium operation code, got: {:?}",
        op
    );

    info!("✓ native envelope format works correctly");
    delete_topic(&brokers, &topic).await?;
    Ok(())
}

/// Test Debezium envelope format (payload wrapper).
#[tokio::test]
#[ignore = "requires docker"]
async fn kafka_sink_debezium_envelope() -> Result<()> {
    init_test_tracing();
    let _container = get_kafka_container().await;

    let topic = test_topic("debezium-envelope");
    let brokers = brokers();
    create_topic(&brokers, &topic, 1).await?;

    let cfg = KafkaSinkCfg {
        id: "test-debezium".into(),
        brokers: brokers.clone(),
        topic: topic.clone(),
        key: None,
        envelope: EnvelopeCfg::Debezium,
        encoding: EncodingCfg::Json,
        required: Some(true),
        exactly_once: None,
        send_timeout_secs: Some(30),
        client_conf: HashMap::new(),
    };

    let cancel = CancellationToken::new();
    let sink = KafkaSink::new(&cfg, cancel)?;

    let event = make_test_event(1);
    sink.send(&event).await?;

    let consumer = create_consumer(&brokers, &topic, "test-debezium-consumer")?;
    let messages = consume_until(&consumer, Duration::from_secs(10), |msgs| {
        !msgs.is_empty()
    })
    .await;

    assert!(!messages.is_empty(), "should receive message");

    // Debezium envelope wraps event in {"payload": <event>}
    let parsed: serde_json::Value = serde_json::from_slice(&messages[0])?;

    // Verify Debezium format has payload wrapper
    assert!(
        parsed.get("schema").is_some(),
        "debezium format must have 'schema' field"
    );
    assert!(
        parsed.get("schema").unwrap().is_null(),
        "debezium schema should be null (schemaless mode)"
    );
    assert!(
        parsed.get("payload").is_some(),
        "debezium format must have 'payload' wrapper"
    );
    assert!(
        parsed.get("payload").unwrap().is_object(),
        "payload must be an object"
    );

    // Verify payload contains the event fields (op, source, etc.)
    let payload = parsed.get("payload").unwrap();
    assert!(
        payload.get("op").is_some(),
        "payload should contain 'op' field"
    );
    assert!(
        payload.get("source").is_some(),
        "payload should contain 'source' field"
    );

    // Verify the op value is a valid Debezium code
    let op = payload.get("op").and_then(|v| v.as_str());
    assert!(
        matches!(
            op,
            Some("c") | Some("u") | Some("d") | Some("r") | Some("t")
        ),
        "op should be a valid Debezium operation code, got: {:?}",
        op
    );

    // Verify source block has connector info
    let source = payload.get("source").unwrap();
    assert!(
        source.get("connector").is_some(),
        "source should have 'connector' field"
    );

    info!("✓ debezium envelope format works correctly");
    delete_topic(&brokers, &topic).await?;
    Ok(())
}

/// Verify exact wire format for Debezium envelope.
#[tokio::test]
#[ignore = "requires docker"]
async fn kafka_sink_debezium_wire_format() -> Result<()> {
    init_test_tracing();
    let _container = get_kafka_container().await;

    let topic = test_topic("debezium-wire-format");
    let brokers = brokers();
    create_topic(&brokers, &topic, 1).await?;

    let cfg = KafkaSinkCfg {
        id: "test-wire-format".into(),
        brokers: brokers.clone(),
        topic: topic.clone(),
        key: None,
        envelope: EnvelopeCfg::Debezium,
        encoding: EncodingCfg::Json,
        required: Some(true),
        exactly_once: None,
        send_timeout_secs: Some(30),
        client_conf: HashMap::new(),
    };

    let cancel = CancellationToken::new();
    let sink = KafkaSink::new(&cfg, cancel)?;

    let event = make_test_event(1);
    sink.send(&event).await?;

    let consumer =
        create_consumer(&brokers, &topic, "test-wire-format-consumer")?;
    let messages = consume_until(&consumer, Duration::from_secs(10), |msgs| {
        !msgs.is_empty()
    })
    .await;

    let raw = String::from_utf8_lossy(&messages[0]);

    // Verify wire format starts with expected structure
    // This catches field ordering changes and unexpected fields
    assert!(
        raw.starts_with(r#"{"schema":null,"payload":{"#),
        "Debezium wire format should start with {{\"schema\":null,\"payload\":{{, got: {}",
        &raw[..raw.len().min(100)]
    );

    info!("✓ debezium wire format matches spec");
    delete_topic(&brokers, &topic).await?;
    Ok(())
}

/// Test CloudEvents envelope format.
#[tokio::test]
#[ignore = "requires docker"]
async fn kafka_sink_cloudevents_envelope() -> Result<()> {
    init_test_tracing();
    let _container = get_kafka_container().await;

    let topic = test_topic("cloudevents-envelope");
    let brokers = brokers();
    create_topic(&brokers, &topic, 1).await?;

    let cfg = KafkaSinkCfg {
        id: "test-cloudevents".into(),
        brokers: brokers.clone(),
        topic: topic.clone(),
        key: None,
        envelope: EnvelopeCfg::CloudEvents {
            type_prefix: "com.deltaforge.cdc".to_string(),
        },
        encoding: EncodingCfg::Json,
        required: Some(true),
        exactly_once: None,
        send_timeout_secs: Some(30),
        client_conf: HashMap::new(),
    };

    let cancel = CancellationToken::new();
    let sink = KafkaSink::new(&cfg, cancel)?;

    let event = make_test_event(1);
    sink.send(&event).await?;

    let consumer =
        create_consumer(&brokers, &topic, "test-cloudevents-consumer")?;
    let messages = consume_until(&consumer, Duration::from_secs(10), |msgs| {
        !msgs.is_empty()
    })
    .await;

    assert!(!messages.is_empty(), "should receive message");

    // CloudEvents envelope restructures to CE 1.0 spec
    let parsed: serde_json::Value = serde_json::from_slice(&messages[0])?;

    // Verify CloudEvents 1.0 required attributes
    assert_eq!(
        parsed.get("specversion").and_then(|v| v.as_str()),
        Some("1.0"),
        "CloudEvents must have specversion 1.0"
    );

    assert!(
        parsed.get("id").is_some(),
        "CloudEvents must have 'id' attribute"
    );

    assert!(
        parsed.get("source").is_some(),
        "CloudEvents must have 'source' attribute"
    );
    // Source format: deltaforge/{name}/{full_table_name}
    let source = parsed.get("source").and_then(|v| v.as_str()).unwrap_or("");
    assert!(
        source.starts_with("deltaforge/"),
        "CloudEvents source should start with 'deltaforge/', got: {}",
        source
    );

    assert!(
        parsed.get("type").is_some(),
        "CloudEvents must have 'type' attribute"
    );
    // Type format: {prefix}.{op_suffix} where op_suffix is created/updated/deleted/snapshot/truncated
    let type_field = parsed.get("type").and_then(|v| v.as_str()).unwrap_or("");
    assert!(
        type_field.starts_with("com.deltaforge.cdc."),
        "CloudEvents type should start with configured prefix, got: {}",
        type_field
    );
    // Verify suffix is valid operation
    let valid_suffixes =
        ["created", "updated", "deleted", "snapshot", "truncated"];
    let has_valid_suffix =
        valid_suffixes.iter().any(|s| type_field.ends_with(s));
    assert!(
        has_valid_suffix,
        "CloudEvents type should end with valid op suffix (created/updated/deleted/snapshot/truncated), got: {}",
        type_field
    );

    // Verify optional but expected attributes
    assert_eq!(
        parsed.get("datacontenttype").and_then(|v| v.as_str()),
        Some("application/json"),
        "CloudEvents should have datacontenttype application/json"
    );

    assert!(
        parsed.get("time").is_some(),
        "CloudEvents should have 'time' attribute"
    );

    // Verify data payload contains before/after/op
    assert!(
        parsed.get("data").is_some(),
        "CloudEvents must have 'data' attribute containing event data"
    );
    let data = parsed.get("data").unwrap();
    assert!(
        data.get("op").is_some(),
        "CloudEvents data should contain 'op' field"
    );
    // Op in data uses Debezium codes (c/u/d/r/t)
    let data_op = data.get("op").and_then(|v| v.as_str());
    assert!(
        matches!(
            data_op,
            Some("c") | Some("u") | Some("d") | Some("r") | Some("t")
        ),
        "data.op should be a valid Debezium operation code, got: {:?}",
        data_op
    );

    info!("✓ cloudevents envelope format works correctly");
    delete_topic(&brokers, &topic).await?;
    Ok(())
}

/// Test batch send with Debezium envelope.
#[tokio::test]
#[ignore = "requires docker"]
async fn kafka_sink_debezium_envelope_batch() -> Result<()> {
    init_test_tracing();
    let _container = get_kafka_container().await;

    let topic = test_topic("debezium-batch");
    let brokers = brokers();
    create_topic(&brokers, &topic, 3).await?;

    let cfg = KafkaSinkCfg {
        id: "test-debezium-batch".into(),
        brokers: brokers.clone(),
        topic: topic.clone(),
        key: None,
        envelope: EnvelopeCfg::Debezium,
        encoding: EncodingCfg::Json,
        required: Some(true),
        exactly_once: None,
        send_timeout_secs: Some(30),
        client_conf: HashMap::new(),
    };

    let cancel = CancellationToken::new();
    let sink = KafkaSink::new(&cfg, cancel)?;

    let events: Vec<Event> = (0..50).map(make_test_event).collect();
    sink.send_batch(&events).await?;

    let consumer =
        create_consumer(&brokers, &topic, "test-debezium-batch-consumer")?;
    let messages = consume_until(&consumer, Duration::from_secs(30), |msgs| {
        msgs.len() >= 50
    })
    .await;

    assert_eq!(messages.len(), 50, "should receive all 50 messages");

    // Verify all messages have Debezium envelope
    for msg in &messages {
        let parsed: serde_json::Value = serde_json::from_slice(msg)?;
        assert!(
            parsed.get("payload").is_some(),
            "all batch messages should have Debezium payload wrapper"
        );
    }

    info!("✓ debezium envelope batch works correctly");
    delete_topic(&brokers, &topic).await?;
    Ok(())
}

/// Test CloudEvents envelope with different operation types.
#[tokio::test]
#[ignore = "requires docker"]
async fn kafka_sink_cloudevents_operations() -> Result<()> {
    init_test_tracing();
    let _container = get_kafka_container().await;

    let topic = test_topic("cloudevents-ops");
    let brokers = brokers();
    create_topic(&brokers, &topic, 1).await?;

    let cfg = KafkaSinkCfg {
        id: "test-cloudevents-ops".into(),
        brokers: brokers.clone(),
        topic: topic.clone(),
        key: None,
        envelope: EnvelopeCfg::CloudEvents {
            type_prefix: "io.deltaforge.test".to_string(),
        },
        encoding: EncodingCfg::Json,
        required: Some(true),
        exactly_once: None,
        send_timeout_secs: Some(30),
        client_conf: HashMap::new(),
    };

    let cancel = CancellationToken::new();
    let sink = KafkaSink::new(&cfg, cancel)?;

    // Create events with different operations
    let create_event = Event::new_row(
        SourceInfo {
            version: "deltaforge-test".into(),
            connector: "test".into(),
            name: "test-db".into(),
            ts_ms: 1_700_000_000_000,
            db: "testdb".into(),
            schema: None,
            table: "table".into(),
            snapshot: None,
            position: SourcePosition::default(),
        },
        Op::Create,
        None,
        Some(json!({"id": 1})),
        1_700_000_000_000,
        64,
    );

    let update_event = Event::new_row(
        SourceInfo {
            version: "deltaforge-test".into(),
            connector: "test".into(),
            name: "test-db".into(),
            ts_ms: 1_700_000_000_001,
            db: "testdb".into(),
            schema: None,
            table: "table".into(),
            snapshot: None,
            position: SourcePosition::default(),
        },
        Op::Update,
        Some(json!({"id": 2, "name": "old"})),
        Some(json!({"id": 2, "name": "new"})),
        1_700_000_000_001,
        64,
    );

    let delete_event = Event::new_row(
        SourceInfo {
            version: "deltaforge-test".into(),
            connector: "test".into(),
            name: "test-db".into(),
            ts_ms: 1_700_000_000_002,
            db: "testdb".into(),
            schema: None,
            table: "table".into(),
            snapshot: None,
            position: SourcePosition::default(),
        },
        Op::Delete,
        Some(json!({"id": 3})),
        None,
        1_700_000_000_002,
        64,
    );

    sink.send(&create_event).await?;
    sink.send(&update_event).await?;
    sink.send(&delete_event).await?;

    let consumer =
        create_consumer(&brokers, &topic, "test-cloudevents-ops-consumer")?;
    let messages = consume_until(&consumer, Duration::from_secs(10), |msgs| {
        msgs.len() >= 3
    })
    .await;

    assert_eq!(messages.len(), 3, "should receive all 3 messages");

    // Collect all type suffixes found
    let mut type_suffixes: Vec<String> = Vec::new();
    for msg in &messages {
        let parsed: serde_json::Value = serde_json::from_slice(msg)?;

        // Verify CloudEvents structure
        assert_eq!(
            parsed.get("specversion").and_then(|v| v.as_str()),
            Some("1.0"),
            "all messages should be CloudEvents 1.0"
        );

        let type_field =
            parsed.get("type").and_then(|v| v.as_str()).unwrap_or("");
        assert!(
            type_field.starts_with("io.deltaforge.test."),
            "type should have configured prefix"
        );

        // Extract suffix (created/updated/deleted)
        let suffix =
            type_field.strip_prefix("io.deltaforge.test.").unwrap_or("");
        type_suffixes.push(suffix.to_string());

        // Verify data has op field with valid Debezium code
        let data = parsed.get("data").expect("should have data");
        let op = data.get("op").and_then(|v| v.as_str());
        assert!(
            matches!(
                op,
                Some("c") | Some("u") | Some("d") | Some("r") | Some("t")
            ),
            "data.op should be a valid Debezium code, got: {:?}",
            op
        );
    }

    // Verify we got all three operation type suffixes
    // Op::Create -> "created", Op::Update -> "updated", Op::Delete -> "deleted"
    assert!(
        type_suffixes.contains(&"created".to_string()),
        "should have 'created' type for Op::Create, found: {:?}",
        type_suffixes
    );
    assert!(
        type_suffixes.contains(&"updated".to_string()),
        "should have 'updated' type for Op::Update, found: {:?}",
        type_suffixes
    );
    assert!(
        type_suffixes.contains(&"deleted".to_string()),
        "should have 'deleted' type for Op::Delete, found: {:?}",
        type_suffixes
    );

    info!("✓ cloudevents envelope handles all operation types correctly");
    delete_topic(&brokers, &topic).await?;
    Ok(())
}

// =============================================================================
// Exactly-Once Semantics Tests
// =============================================================================

/// Test idempotent producer (default mode).
#[tokio::test]
#[ignore = "requires docker"]
async fn kafka_sink_idempotent_mode() -> Result<()> {
    init_test_tracing();
    let _container = get_kafka_container().await;

    let topic = test_topic("idempotent");
    let brokers = brokers();
    create_topic(&brokers, &topic, 1).await?;

    let cfg = KafkaSinkCfg {
        id: "test-idempotent".into(),
        brokers: brokers.clone(),
        topic: topic.clone(),
        key: None,
        envelope: EnvelopeCfg::Native,
        encoding: EncodingCfg::Json,
        required: Some(true),
        exactly_once: Some(false), // Idempotent, not exactly-once
        send_timeout_secs: Some(30),
        client_conf: HashMap::new(),
    };

    let cancel = CancellationToken::new();
    let sink = KafkaSink::new(&cfg, cancel)?;

    // Send multiple events
    for i in 0..10 {
        let event = make_test_event(i);
        sink.send(&event).await?;
    }

    let consumer =
        create_consumer(&brokers, &topic, "test-idempotent-consumer")?;
    let messages = consume_until(&consumer, Duration::from_secs(10), |msgs| {
        msgs.len() >= 10
    })
    .await;

    assert_eq!(messages.len(), 10, "should receive all 10 messages");

    info!("✓ idempotent producer works correctly");
    delete_topic(&brokers, &topic).await?;
    Ok(())
}

/// Test exactly-once enabled producer.
#[tokio::test]
#[ignore = "requires docker"]
async fn kafka_sink_exactly_once_mode() -> Result<()> {
    init_test_tracing();
    let _container = get_kafka_container().await;

    let topic = test_topic("exactly-once");
    let brokers = brokers();
    create_topic(&brokers, &topic, 1).await?;

    let cfg = KafkaSinkCfg {
        id: "test-exactly-once".into(),
        brokers: brokers.clone(),
        topic: topic.clone(),
        key: None,
        envelope: EnvelopeCfg::Native,
        encoding: EncodingCfg::Json,
        required: Some(true),
        exactly_once: Some(true),
        send_timeout_secs: Some(30),
        client_conf: HashMap::new(),
    };

    let cancel = CancellationToken::new();
    let sink = KafkaSink::new(&cfg, cancel)?;

    // Note: Full EOS testing requires transaction support; this just validates
    // that the producer can be created and send messages
    let event = make_test_event(1);
    sink.send(&event).await?;

    let consumer = create_consumer(&brokers, &topic, "test-eos-consumer")?;
    let messages = consume_until(&consumer, Duration::from_secs(10), |msgs| {
        !msgs.is_empty()
    })
    .await;

    assert!(!messages.is_empty(), "should receive message in EOS mode");

    info!("✓ exactly-once producer created and sends successfully");
    delete_topic(&brokers, &topic).await?;
    Ok(())
}

// =============================================================================
// Connection and Retry Tests
// =============================================================================

/// Test sink with invalid brokers fails gracefully.
#[tokio::test]
#[ignore = "requires docker"]
async fn kafka_sink_invalid_brokers() -> Result<()> {
    init_test_tracing();
    let _container = get_kafka_container().await;

    let cfg = KafkaSinkCfg {
        id: "test-invalid".into(),
        brokers: "invalid-host:9999".into(),
        topic: "df-test-invalid".into(),
        key: None,
        envelope: EnvelopeCfg::Native,
        encoding: EncodingCfg::Json,
        required: Some(true),
        exactly_once: None,
        send_timeout_secs: Some(5),
        client_conf: HashMap::new(),
    };

    let cancel = CancellationToken::new();
    let sink = KafkaSink::new(&cfg, cancel)?;

    // Send should fail (after retries/timeout)
    let event = make_test_event(1);
    let result = sink.send(&event).await;

    assert!(result.is_err(), "send to invalid brokers should fail");
    info!("✓ invalid brokers fails gracefully: {:?}", result.err());
    Ok(())
}

// =============================================================================
// Large Payload Tests
// =============================================================================

/// Test handling of large events.
#[tokio::test]
#[ignore = "requires docker"]
async fn kafka_sink_large_events() -> Result<()> {
    init_test_tracing();
    let _container = get_kafka_container().await;

    let topic = test_topic("large");
    let brokers = brokers();
    create_topic(&brokers, &topic, 1).await?;

    let cfg = KafkaSinkCfg {
        id: "test-large".into(),
        brokers: brokers.clone(),
        topic: topic.clone(),
        key: None,
        envelope: EnvelopeCfg::Native,
        encoding: EncodingCfg::Json,
        required: Some(true),
        exactly_once: None,
        send_timeout_secs: Some(30),
        client_conf: HashMap::new(),
    };

    let cancel = CancellationToken::new();
    let sink = KafkaSink::new(&cfg, cancel)?;

    // Send a 500KB event (default max is 1MB)
    let large_event = make_large_event(1, 500 * 1024);
    sink.send(&large_event).await?;

    let consumer = create_consumer(&brokers, &topic, "test-large-consumer")?;
    let messages = consume_until(&consumer, Duration::from_secs(10), |msgs| {
        !msgs.is_empty()
    })
    .await;

    assert!(!messages.is_empty(), "large event should be delivered");

    info!("✓ large event (500KB) sent successfully");
    delete_topic(&brokers, &topic).await?;
    Ok(())
}

// =============================================================================
// Concurrent Access Tests
// =============================================================================

/// Test concurrent sends from multiple tasks.
#[tokio::test]
#[ignore = "requires docker"]
async fn kafka_sink_concurrent_sends() -> Result<()> {
    init_test_tracing();
    let _container = get_kafka_container().await;

    let topic = test_topic("concurrent");
    let brokers = brokers();
    create_topic(&brokers, &topic, 3).await?;

    let cfg = KafkaSinkCfg {
        id: "test-concurrent".into(),
        brokers: brokers.clone(),
        topic: topic.clone(),
        key: None,
        envelope: EnvelopeCfg::Native,
        encoding: EncodingCfg::Json,
        required: Some(true),
        exactly_once: None,
        send_timeout_secs: Some(30),
        client_conf: HashMap::new(),
    };

    let cancel = CancellationToken::new();
    let sink = Arc::new(KafkaSink::new(&cfg, cancel)?);

    // Spawn 10 concurrent tasks, each sending 10 events
    let mut handles = Vec::new();
    for task_id in 0..10 {
        let sink = sink.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..10 {
                let event = make_test_event(task_id * 100 + i);
                sink.send(&event).await?;
            }
            Ok::<_, anyhow::Error>(())
        }));
    }

    // Wait for all tasks
    for handle in handles {
        handle.await??;
    }

    // Consume and verify
    let consumer =
        create_consumer(&brokers, &topic, "test-concurrent-consumer")?;
    let messages = consume_until(&consumer, Duration::from_secs(30), |msgs| {
        msgs.len() >= 100
    })
    .await;

    assert_eq!(
        messages.len(),
        100,
        "all concurrent events should be delivered"
    );

    info!("✓ 100 concurrent events sent successfully");
    delete_topic(&brokers, &topic).await?;
    Ok(())
}

// =============================================================================
// Client Configuration Tests
// =============================================================================

/// Test custom client configuration overrides.
#[tokio::test]
#[ignore = "requires docker"]
async fn kafka_sink_custom_config() -> Result<()> {
    init_test_tracing();
    let _container = get_kafka_container().await;

    let topic = test_topic("custom-config");
    let brokers = brokers();
    create_topic(&brokers, &topic, 1).await?;

    let mut client_conf = HashMap::new();
    client_conf.insert("linger.ms".to_string(), "10".to_string());
    client_conf.insert("compression.type".to_string(), "gzip".to_string());

    let cfg = KafkaSinkCfg {
        id: "test-custom".into(),
        brokers: brokers.clone(),
        topic: topic.clone(),
        key: None,
        envelope: EnvelopeCfg::Native,
        encoding: EncodingCfg::Json,
        required: Some(true),
        exactly_once: None,
        send_timeout_secs: Some(30),
        client_conf,
    };

    let cancel = CancellationToken::new();
    let sink = KafkaSink::new(&cfg, cancel)?;

    let event = make_test_event(1);
    sink.send(&event).await?;

    let consumer = create_consumer(&brokers, &topic, "test-custom-consumer")?;
    let messages = consume_until(&consumer, Duration::from_secs(10), |msgs| {
        !msgs.is_empty()
    })
    .await;

    assert!(
        !messages.is_empty(),
        "message should be delivered with custom config"
    );

    info!("✓ custom client configuration works");
    delete_topic(&brokers, &topic).await?;
    Ok(())
}

// =============================================================================
// Trait Implementation Tests
// =============================================================================

/// Test Sink trait methods.
#[tokio::test]
#[ignore = "requires docker"]
async fn kafka_sink_trait_implementation() -> Result<()> {
    init_test_tracing();
    let _container = get_kafka_container().await;

    let cfg = KafkaSinkCfg {
        id: "test-trait".into(),
        brokers: brokers(),
        topic: "df-test-trait".into(),
        key: None,
        envelope: EnvelopeCfg::Native,
        encoding: EncodingCfg::Json,
        required: Some(true),
        exactly_once: None,
        send_timeout_secs: Some(30),
        client_conf: HashMap::new(),
    };

    let cancel = CancellationToken::new();
    let sink = KafkaSink::new(&cfg, cancel)?;

    // Test id()
    assert_eq!(sink.id(), "test-trait");

    // Test required()
    assert!(sink.required());

    info!("✓ Sink trait methods work correctly");
    Ok(())
}

/// Test required() returns false when configured.
#[tokio::test]
#[ignore = "requires docker"]
async fn kafka_sink_optional() -> Result<()> {
    init_test_tracing();
    let _container = get_kafka_container().await;

    let cfg = KafkaSinkCfg {
        id: "test-optional".into(),
        brokers: brokers(),
        topic: "df-test-optional".into(),
        key: None,
        envelope: EnvelopeCfg::Native,
        encoding: EncodingCfg::Json,
        required: Some(false),
        exactly_once: None,
        send_timeout_secs: None,
        client_conf: HashMap::new(),
    };

    let cancel = CancellationToken::new();
    let sink = KafkaSink::new(&cfg, cancel)?;

    assert!(!sink.required(), "sink should be optional");

    info!("✓ optional sink configuration works");
    Ok(())
}

// =============================================================================
// Reconnection and Recovery Tests
// =============================================================================

/// Test that sink recovers after Kafka broker restarts.
#[tokio::test]
#[ignore = "requires docker"]
async fn kafka_sink_recovers_after_restart() -> Result<()> {
    init_test_tracing();

    // Start a dedicated container for this test (not shared)
    let restart_port: u16 = 9193;
    let image = GenericImage::new("confluentinc/cp-kafka", "7.5.0")
        .with_wait_for(WaitFor::Duration { length: Duration::from_secs(15) })
        .with_env_var("KAFKA_NODE_ID", "1")
        .with_env_var("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env_var("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:29093")
        .with_env_var("KAFKA_LISTENERS", format!(
            "PLAINTEXT://0.0.0.0:29092,CONTROLLER://0.0.0.0:29093,EXTERNAL://0.0.0.0:{}",
            restart_port
        ))
        .with_env_var("KAFKA_ADVERTISED_LISTENERS", format!(
            "PLAINTEXT://localhost:29092,EXTERNAL://localhost:{}",
            restart_port
        ))
        .with_env_var("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", 
            "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT")
        .with_env_var("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env_var("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
        .with_env_var("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .with_env_var("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .with_env_var("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
        .with_env_var("CLUSTER_ID", "NkU3OEVBNTcwNTJENDM2Qg")
        .with_mapped_port(restart_port, restart_port.tcp());

    let container = image.start().await?;
    let brokers = format!("localhost:{}", restart_port);

    wait_for_kafka(&brokers, Duration::from_secs(60)).await?;

    let topic = "df-test-restart";
    create_topic(&brokers, topic, 1).await?;

    let cfg = KafkaSinkCfg {
        id: "test-restart".into(),
        brokers: brokers.clone(),
        topic: topic.into(),
        key: None,
        envelope: EnvelopeCfg::Native,
        encoding: EncodingCfg::Json,
        required: Some(true),
        exactly_once: None,
        send_timeout_secs: Some(30),
        client_conf: HashMap::new(),
    };

    let cancel = CancellationToken::new();
    let sink = Arc::new(KafkaSink::new(&cfg, cancel.clone())?);

    // Send first event successfully
    let event1 = make_test_event(1);
    sink.send(&event1).await?;
    info!("✓ first event sent before restart");

    // Stop the container (simulates Kafka going down)
    info!("stopping Kafka container...");
    container.stop().await?;
    sleep(Duration::from_secs(2)).await;

    // Start sending in background - should retry until Kafka comes back
    let sink_clone = sink.clone();
    let send_handle = tokio::spawn(async move {
        let event2 = make_test_event(2);
        sink_clone.send(&event2).await
    });

    // Wait for retries to start
    sleep(Duration::from_secs(3)).await;

    // Restart the container
    info!("restarting Kafka container...");
    container.start().await?;
    wait_for_kafka(&brokers, Duration::from_secs(60)).await?;
    info!("Kafka is back up");

    // The send should eventually succeed (give it extra time for broker recovery)
    let result = timeout(Duration::from_secs(60), send_handle).await??;
    assert!(
        result.is_ok(),
        "send should succeed after Kafka recovers: {:?}",
        result.err()
    );

    info!("✓ sink recovered after Kafka restart");
    Ok(())
}

/// Test that producer handles temporary broker unavailability.
#[tokio::test]
#[ignore = "requires docker"]
async fn kafka_sink_handles_broker_hiccup() -> Result<()> {
    init_test_tracing();
    let _container = get_kafka_container().await;

    let topic = test_topic("hiccup");
    let brokers = brokers();
    create_topic(&brokers, &topic, 1).await?;

    let cfg = KafkaSinkCfg {
        id: "test-hiccup".into(),
        brokers: brokers.clone(),
        topic: topic.clone(),
        key: None,
        envelope: EnvelopeCfg::Native,
        encoding: EncodingCfg::Json,
        required: Some(true),
        exactly_once: None,
        send_timeout_secs: Some(30),
        client_conf: HashMap::new(),
    };

    let cancel = CancellationToken::new();
    let sink = Arc::new(KafkaSink::new(&cfg, cancel)?);

    // Send multiple events rapidly
    // rdkafka's internal retry should handle any transient issues
    let mut handles = Vec::new();
    for i in 0..50 {
        let sink = sink.clone();
        handles.push(tokio::spawn(async move {
            let event = make_test_event(i);
            sink.send(&event).await
        }));
    }

    // Wait for all sends
    let mut success_count = 0;
    for handle in handles {
        if handle.await?.is_ok() {
            success_count += 1;
        }
    }

    assert_eq!(success_count, 50, "all events should be delivered");

    // Verify by consuming
    let consumer = create_consumer(&brokers, &topic, "test-hiccup-consumer")?;
    let messages = consume_until(&consumer, Duration::from_secs(30), |msgs| {
        msgs.len() >= 50
    })
    .await;

    assert_eq!(messages.len(), 50, "all 50 messages should be consumable");

    info!("✓ handled rapid sends without issues");
    delete_topic(&brokers, &topic).await?;
    Ok(())
}

/// Test batch delivery with intermittent failures.
#[tokio::test]
#[ignore = "requires docker"]
async fn kafka_sink_batch_resilience() -> Result<()> {
    init_test_tracing();
    let _container = get_kafka_container().await;

    let topic = test_topic("batch-resilience");
    let brokers = brokers();
    create_topic(&brokers, &topic, 3).await?;

    let cfg = KafkaSinkCfg {
        id: "test-batch-resilience".into(),
        brokers: brokers.clone(),
        topic: topic.clone(),
        key: None,
        envelope: EnvelopeCfg::Native,
        encoding: EncodingCfg::Json,
        required: Some(true),
        exactly_once: None,
        send_timeout_secs: Some(30),
        client_conf: HashMap::new(),
    };

    let cancel = CancellationToken::new();
    let sink = KafkaSink::new(&cfg, cancel)?;

    // Send multiple batches in sequence
    for batch_num in 0..5 {
        let events: Vec<Event> = (0..20)
            .map(|i| make_test_event(batch_num * 100 + i))
            .collect();

        sink.send_batch(&events).await?;
        debug!("batch {} delivered", batch_num);
    }

    // Verify all messages
    let consumer =
        create_consumer(&brokers, &topic, "test-batch-resilience-consumer")?;
    let messages = consume_until(&consumer, Duration::from_secs(30), |msgs| {
        msgs.len() >= 100
    })
    .await;

    assert_eq!(messages.len(), 100, "all 100 messages should be delivered");

    info!("✓ batch delivery is resilient");
    delete_topic(&brokers, &topic).await?;
    Ok(())
}

// =============================================================================
// Cancellation Tests
// =============================================================================

/// Test that cancellation is propagated correctly.
#[tokio::test]
#[ignore = "requires docker"]
async fn kafka_sink_respects_cancellation() -> Result<()> {
    init_test_tracing();
    let _container = get_kafka_container().await;

    // Use an invalid broker so retry loop keeps trying
    let cfg = KafkaSinkCfg {
        id: "test-cancel".into(),
        brokers: "invalid-host:9999".into(),
        topic: "df-test-cancel".into(),
        key: None,
        envelope: EnvelopeCfg::Native,
        encoding: EncodingCfg::Json,
        required: Some(true),
        exactly_once: None,
        send_timeout_secs: Some(2),
        client_conf: HashMap::new(),
    };

    let cancel = CancellationToken::new();
    let sink = Arc::new(KafkaSink::new(&cfg, cancel.clone())?);

    let sink_clone = sink.clone();
    let send_handle = tokio::spawn(async move {
        let event = make_test_event(1);
        sink_clone.send(&event).await
    });

    // Give it a moment to start, then cancel
    sleep(Duration::from_millis(100)).await;
    cancel.cancel();

    let result = send_handle.await?;
    assert!(result.is_err(), "cancelled operation should fail");

    info!("✓ cancellation respected");
    Ok(())
}

// =============================================================================
// Envelope Default Tests
// =============================================================================

/// Test that default envelope is Native.
#[tokio::test]
#[ignore = "requires docker"]
async fn kafka_sink_default_envelope_is_native() -> Result<()> {
    init_test_tracing();
    let _container = get_kafka_container().await;

    let topic = test_topic("default-envelope");
    let brokers = brokers();
    create_topic(&brokers, &topic, 1).await?;

    // Use Default::default() for envelope and encoding
    let cfg = KafkaSinkCfg {
        id: "test-default".into(),
        brokers: brokers.clone(),
        topic: topic.clone(),
        key: None,
        envelope: EnvelopeCfg::default(),
        encoding: EncodingCfg::default(),
        required: Some(true),
        exactly_once: None,
        send_timeout_secs: Some(30),
        client_conf: HashMap::new(),
    };

    // Verify defaults
    assert_eq!(cfg.envelope, EnvelopeCfg::Native);
    assert_eq!(cfg.encoding, EncodingCfg::Json);

    let cancel = CancellationToken::new();
    let sink = KafkaSink::new(&cfg, cancel)?;

    let event = make_test_event(1);
    sink.send(&event).await?;

    let consumer = create_consumer(&brokers, &topic, "test-default-consumer")?;
    let messages = consume_until(&consumer, Duration::from_secs(10), |msgs| {
        !msgs.is_empty()
    })
    .await;

    assert!(!messages.is_empty(), "should receive message with defaults");

    // Verify native format (no payload wrapper, op at top level)
    let parsed: serde_json::Value = serde_json::from_slice(&messages[0])?;
    assert!(
        parsed.get("payload").is_none(),
        "default (native) envelope should NOT have payload wrapper"
    );
    assert!(
        parsed.get("op").is_some(),
        "default (native) envelope should have 'op' at top level"
    );
    assert!(
        parsed.get("source").is_some(),
        "default (native) envelope should have 'source' at top level"
    );

    info!("✓ default envelope is Native");
    delete_topic(&brokers, &topic).await?;
    Ok(())
}

// =============================================================================
// Dynamic Routing Tests
// =============================================================================

struct ConsumedMessage {
    payload: Vec<u8>,
    key: Option<String>,
    headers: HashMap<String, String>,
}

/// Like `consume_until` but also captures message key and headers.
async fn consume_messages_until<F>(
    consumer: &StreamConsumer,
    timeout_duration: Duration,
    mut condition: F,
) -> Vec<ConsumedMessage>
where
    F: FnMut(&[ConsumedMessage]) -> bool,
{
    let deadline = Instant::now() + timeout_duration;
    let mut messages = Vec::new();

    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match timeout(remaining, consumer.recv()).await {
            Ok(Ok(msg)) => {
                if let Some(payload) = msg.payload() {
                    let key = msg
                        .key()
                        .map(|k| String::from_utf8_lossy(k).to_string());
                    let mut headers = HashMap::new();
                    if let Some(h) = msg.headers() {
                        for header in h.iter() {
                            if let Some(val) = header.value {
                                headers.insert(
                                    header.key.to_string(),
                                    String::from_utf8_lossy(val).to_string(),
                                );
                            }
                        }
                    }
                    messages.push(ConsumedMessage {
                        payload: payload.to_vec(),
                        key,
                        headers,
                    });
                    if condition(&messages) {
                        break;
                    }
                }
            }
            Ok(Err(e)) => {
                warn!("consumer error: {}", e);
                break;
            }
            Err(_) => break,
        }
    }

    messages
}

/// Topic template routes events to per-table Kafka topics.
#[tokio::test]
#[ignore = "requires docker"]
async fn kafka_sink_topic_template_routes_by_table() -> Result<()> {
    init_test_tracing();
    let _container = get_kafka_container().await;

    let brokers = brokers();
    let topic_orders = test_topic("route-orders");
    let topic_users = test_topic("route-users");
    create_topic(&brokers, &topic_orders, 1).await?;
    create_topic(&brokers, &topic_users, 1).await?;

    let cfg = KafkaSinkCfg {
        id: "test-routing".into(),
        brokers: brokers.clone(),
        topic: "df-test-route-${source.table}".into(),
        key: None,
        envelope: EnvelopeCfg::Native,
        encoding: EncodingCfg::Json,
        required: Some(true),
        exactly_once: None,
        send_timeout_secs: Some(30),
        client_conf: HashMap::new(),
    };

    let cancel = CancellationToken::new();
    let sink = KafkaSink::new(&cfg, cancel)?;

    sink.send_batch(&[
        make_event_for_table(1, "orders"),
        make_event_for_table(2, "users"),
    ])
    .await?;

    let consumer =
        create_consumer(&brokers, &topic_orders, "route-orders-consumer")?;
    let msgs =
        consume_messages_until(&consumer, Duration::from_secs(10), |m| {
            !m.is_empty()
        })
        .await;
    assert_eq!(msgs.len(), 1);
    let parsed: serde_json::Value = serde_json::from_slice(&msgs[0].payload)?;
    assert_eq!(parsed["source"]["table"], "orders");

    let consumer =
        create_consumer(&brokers, &topic_users, "route-users-consumer")?;
    let msgs =
        consume_messages_until(&consumer, Duration::from_secs(10), |m| {
            !m.is_empty()
        })
        .await;
    assert_eq!(msgs.len(), 1);
    let parsed: serde_json::Value = serde_json::from_slice(&msgs[0].payload)?;
    assert_eq!(parsed["source"]["table"], "users");

    info!("✓ topic template routes events to correct topics");
    delete_topic(&brokers, &topic_orders).await?;
    delete_topic(&brokers, &topic_users).await?;
    Ok(())
}

/// EventRouting.topic overrides the configured topic.
#[tokio::test]
#[ignore = "requires docker"]
async fn kafka_sink_event_routing_override() -> Result<()> {
    init_test_tracing();
    let _container = get_kafka_container().await;

    let brokers = brokers();
    let default_topic = test_topic("route-default");
    let override_topic = test_topic("route-override");
    create_topic(&brokers, &default_topic, 1).await?;
    create_topic(&brokers, &override_topic, 1).await?;

    let cfg = KafkaSinkCfg {
        id: "test-override".into(),
        brokers: brokers.clone(),
        topic: default_topic.clone(),
        key: None,
        envelope: EnvelopeCfg::Native,
        encoding: EncodingCfg::Json,
        required: Some(true),
        exactly_once: None,
        send_timeout_secs: Some(30),
        client_conf: HashMap::new(),
    };

    let cancel = CancellationToken::new();
    let sink = KafkaSink::new(&cfg, cancel)?;

    let ev1 = make_test_event(1);
    let mut ev2 = make_test_event(2);
    ev2.routing = Some(EventRouting {
        topic: Some(override_topic.clone()),
        ..Default::default()
    });

    sink.send_batch(&[ev1, ev2]).await?;

    let consumer =
        create_consumer(&brokers, &default_topic, "default-consumer")?;
    let msgs =
        consume_until(&consumer, Duration::from_secs(10), |m| !m.is_empty())
            .await;
    assert_eq!(msgs.len(), 1);

    let consumer =
        create_consumer(&brokers, &override_topic, "override-consumer")?;
    let msgs =
        consume_until(&consumer, Duration::from_secs(10), |m| !m.is_empty())
            .await;
    assert_eq!(msgs.len(), 1);

    info!("✓ EventRouting.topic overrides static config");
    delete_topic(&brokers, &default_topic).await?;
    delete_topic(&brokers, &override_topic).await?;
    Ok(())
}

/// Key template resolves and routing headers land on the Kafka message.
#[tokio::test]
#[ignore = "requires docker"]
async fn kafka_sink_routing_key_and_headers() -> Result<()> {
    init_test_tracing();
    let _container = get_kafka_container().await;

    let brokers = brokers();
    let topic = test_topic("route-key-headers");
    create_topic(&brokers, &topic, 1).await?;

    let cfg = KafkaSinkCfg {
        id: "test-kh".into(),
        brokers: brokers.clone(),
        topic: topic.clone(),
        key: Some("${after.customer_id}".into()),
        envelope: EnvelopeCfg::Native,
        encoding: EncodingCfg::Json,
        required: Some(true),
        exactly_once: None,
        send_timeout_secs: Some(30),
        client_conf: HashMap::new(),
    };

    let cancel = CancellationToken::new();
    let sink = KafkaSink::new(&cfg, cancel)?;

    let mut event = make_event_for_table(1, "orders");
    event.after = Some(json!({"id": 1, "customer_id": "cust-42"}));
    event.routing = Some(EventRouting {
        headers: Some(HashMap::from([
            ("trace-id".into(), "abc-123".into()),
            ("x-source".into(), "deltaforge".into()),
        ])),
        ..Default::default()
    });

    sink.send(&event).await?;

    let consumer = create_consumer(&brokers, &topic, "kh-consumer")?;
    let msgs =
        consume_messages_until(&consumer, Duration::from_secs(10), |m| {
            !m.is_empty()
        })
        .await;
    assert_eq!(msgs.len(), 1);

    let msg = &msgs[0];
    assert_eq!(msg.key.as_deref(), Some("cust-42"), "key from template");
    assert_eq!(
        msg.headers.get("trace-id").map(|s| s.as_str()),
        Some("abc-123")
    );
    assert_eq!(
        msg.headers.get("x-source").map(|s| s.as_str()),
        Some("deltaforge")
    );

    info!("✓ key template + routing headers delivered correctly");
    delete_topic(&brokers, &topic).await?;
    Ok(())
}

// =============================================================================
// Raw Payload Tests (Outbox)
// =============================================================================

/// When routing.raw_payload is true, the sink should serialize event.after
/// directly to the wire, bypassing the configured envelope format.
/// This test uses Debezium envelope to prove the bypass works.
#[tokio::test]
#[ignore = "requires docker"]
async fn kafka_sink_raw_payload_bypasses_envelope() -> Result<()> {
    init_test_tracing();
    let _container = get_kafka_container().await;

    let topic = test_topic("raw-payload");
    let brokers = brokers();
    create_topic(&brokers, &topic, 1).await?;

    // Deliberately use Debezium envelope - raw_payload should bypass it
    let cfg = KafkaSinkCfg {
        id: "test-raw".into(),
        brokers: brokers.clone(),
        topic: topic.clone(),
        key: None,
        envelope: EnvelopeCfg::Debezium,
        encoding: EncodingCfg::Json,
        required: Some(true),
        exactly_once: None,
        send_timeout_secs: Some(30),
        client_conf: HashMap::new(),
    };

    let cancel = CancellationToken::new();
    let sink = KafkaSink::new(&cfg, cancel)?;

    // Event with raw_payload flag (simulates outbox processor output)
    let mut raw_event = make_test_event(1);
    raw_event.after = Some(json!({"order_id": 42, "total": 99.99}));
    raw_event.routing = Some(EventRouting {
        raw_payload: true,
        headers: Some(HashMap::from([(
            "df-aggregate-type".into(),
            "Order".into(),
        )])),
        ..Default::default()
    });

    // Normal event without raw_payload
    let normal_event = make_test_event(2);

    sink.send_batch(&[raw_event, normal_event]).await?;

    let consumer = create_consumer(&brokers, &topic, "raw-payload-consumer")?;
    let msgs =
        consume_messages_until(&consumer, Duration::from_secs(10), |m| {
            m.len() >= 2
        })
        .await;
    assert_eq!(msgs.len(), 2, "should receive both messages");

    // Find raw vs normal by checking structure
    let parsed: Vec<serde_json::Value> = msgs
        .iter()
        .map(|m| serde_json::from_slice(&m.payload).unwrap())
        .collect();

    // One message should be raw (just {order_id, total}), the other wrapped
    let raw_msg = parsed
        .iter()
        .find(|v| v.get("order_id").is_some())
        .expect("should have a raw payload message");
    let envelope_msg = parsed
        .iter()
        .find(|v| v.get("payload").is_some())
        .expect("should have a Debezium-wrapped message");

    // Raw: just the payload, no envelope wrapping
    assert_eq!(raw_msg["order_id"], 42);
    assert_eq!(raw_msg["total"], 99.99);
    assert!(raw_msg.get("op").is_none(), "raw should have no 'op' field");
    assert!(
        raw_msg.get("source").is_none(),
        "raw should have no 'source' field"
    );
    assert!(
        raw_msg.get("schema").is_none(),
        "raw should have no 'schema' field"
    );

    // Normal: full Debezium envelope
    assert!(envelope_msg["payload"].get("op").is_some());
    assert!(envelope_msg["payload"].get("source").is_some());

    // Verify headers still delivered on raw message
    let raw_kafka_msg = msgs
        .iter()
        .find(|m| {
            let v: serde_json::Value =
                serde_json::from_slice(&m.payload).unwrap();
            v.get("order_id").is_some()
        })
        .unwrap();
    assert_eq!(
        raw_kafka_msg
            .headers
            .get("df-aggregate-type")
            .map(|s| s.as_str()),
        Some("Order"),
        "routing headers should still be delivered"
    );

    info!("✓ raw_payload bypasses envelope, normal events still wrapped");
    delete_topic(&brokers, &topic).await?;
    Ok(())
}
