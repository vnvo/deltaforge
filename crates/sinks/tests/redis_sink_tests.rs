//! Integration tests for Redis Streams sink.
//!
//! These tests require Docker and pull `redis:7-alpine`.
//!
//! Run with:
//! ```bash
//! cargo test -p sinks --test redis_sink_tests -- --include-ignored --nocapture --test-threads=1
//! ```

use anyhow::Result;
use ctor::dtor;
use deltaforge_config::{EncodingCfg, EnvelopeCfg, RedisSinkCfg};
use deltaforge_core::{
    Event, EventRouting, Op, Sink, SourceInfo, SourcePosition,
};
use redis::AsyncCommands;
use serde_json::json;
use sinks::redis::RedisSink;
use std::time::Instant;
use std::{collections::HashMap, sync::Arc};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::time::{Duration, sleep};
use tokio::{sync::OnceCell, time::timeout};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

mod sink_test_common;
use sink_test_common::{
    init_test_tracing, make_event_for_table, make_large_event, make_test_event,
};

// =============================================================================
// Shared Test Infrastructure
// =============================================================================

const REDIS_PORT: u16 = 6399;

/// Shared container - initialized once, reused by all tests.
static REDIS_CONTAINER: OnceCell<ContainerAsync<GenericImage>> =
    OnceCell::const_new();

#[dtor]
fn cleanup() {
    // Force container cleanup on process exit
    if let Some(container) = REDIS_CONTAINER.get() {
        std::process::Command::new("docker")
            .args(["rm", "-f", container.id()])
            .output()
            .ok();
    }
}

/// Get or start the shared Redis container.
async fn get_redis_container() -> &'static ContainerAsync<GenericImage> {
    REDIS_CONTAINER
        .get_or_init(|| async {
            info!("starting Redis container...");

            let image = GenericImage::new("redis", "7-alpine")
                .with_wait_for(WaitFor::message_on_stdout(
                    "Ready to accept connections",
                ))
                .with_mapped_port(REDIS_PORT, 6379.tcp());

            let container = image.start().await.expect("start redis container");
            info!("Redis container started: {}", container.id());

            // Wait for Redis to be fully ready
            wait_for_redis(&redis_uri(), Duration::from_secs(30))
                .await
                .expect("Redis should be ready");

            container
        })
        .await
}

/// Poll Redis until it's ready to accept connections.
async fn wait_for_redis(uri: &str, timeout_duration: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout_duration;
    let client = redis::Client::open(uri)?;

    while Instant::now() < deadline {
        if let Ok(mut conn) = client.get_multiplexed_async_connection().await {
            let result: Result<String, _> =
                redis::cmd("PING").query_async(&mut conn).await;
            if result.is_ok() {
                info!("Redis is ready");
                return Ok(());
            }
        }
        sleep(Duration::from_millis(100)).await;
    }

    anyhow::bail!("Redis not ready after {:?}", timeout_duration)
}

fn redis_uri() -> String {
    format!("redis://127.0.0.1:{}/0", REDIS_PORT)
}

/// Create a unique stream name for each test.
fn test_stream(test_name: &str) -> String {
    format!("df.test.{}", test_name.replace('-', "_"))
}

/// Clean up a stream before/after test.
async fn cleanup_stream(uri: &str, stream: &str) -> Result<()> {
    let client = redis::Client::open(uri)?;
    let mut conn = client.get_multiplexed_async_connection().await?;
    let _: () = conn.del(stream).await.unwrap_or(());
    debug!("cleaned up stream: {}", stream);
    Ok(())
}

/// Read all entries from a Redis stream.
async fn read_stream_entries(
    uri: &str,
    stream: &str,
) -> Result<Vec<(String, Vec<(String, String)>)>> {
    let client = redis::Client::open(uri)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    let entries: Vec<(String, Vec<(String, String)>)> = redis::cmd("XRANGE")
        .arg(stream)
        .arg("-")
        .arg("+")
        .query_async(&mut conn)
        .await?;

    Ok(entries)
}

/// Get stream length.
async fn stream_length(uri: &str, stream: &str) -> Result<i64> {
    let client = redis::Client::open(uri)?;
    let mut conn = client.get_multiplexed_async_connection().await?;
    let len: i64 = redis::cmd("XLEN")
        .arg(stream)
        .query_async(&mut conn)
        .await?;
    Ok(len)
}

// =============================================================================
// Basic Functionality Tests
// =============================================================================

/// Test that a single event is written to the stream correctly.
#[tokio::test]
#[ignore = "requires docker"]
async fn redis_sink_sends_single_event() -> Result<()> {
    init_test_tracing();
    let _container = get_redis_container().await;

    let stream = test_stream("single");
    let uri = redis_uri();
    cleanup_stream(&uri, &stream).await?;

    let cfg = RedisSinkCfg {
        id: "test-redis".into(),
        uri: uri.clone(),
        stream: stream.clone(),
        key: None,
        envelope: EnvelopeCfg::Native,
        encoding: EncodingCfg::Json,
        required: Some(true),
        send_timeout_secs: Some(5),
        batch_timeout_secs: Some(30),
        connect_timeout_secs: Some(10),
    };

    let cancel = CancellationToken::new();
    let sink = RedisSink::new(&cfg, cancel)?;

    let event = make_test_event(1);
    sink.send(&event).await?;

    // Verify the event is in the stream
    let entries = read_stream_entries(&uri, &stream).await?;
    assert_eq!(entries.len(), 1, "expected exactly one entry in stream");

    let (_id, fields) = &entries[0];
    assert!(!fields.is_empty(), "expected fields in entry");

    // Find the df-event field
    let df_event = fields.iter().find(|(k, _)| k == "df-event");
    assert!(df_event.is_some(), "expected df-event field");

    let payload = &df_event.unwrap().1;
    let parsed: serde_json::Value = serde_json::from_str(payload)?;

    // Verify native envelope format (no payload wrapper)
    assert!(
        parsed.get("op").is_some(),
        "native format should have 'op' at top level"
    );
    assert!(
        parsed.get("source").is_some(),
        "native format should have 'source' at top level"
    );
    assert!(
        parsed.get("payload").is_none(),
        "native format should NOT have 'payload' wrapper"
    );

    info!("✓ single event sent successfully");
    cleanup_stream(&uri, &stream).await?;
    Ok(())
}

/// Test batch send with pipeline optimization.
#[tokio::test]
#[ignore = "requires docker"]
async fn redis_sink_sends_batch() -> Result<()> {
    init_test_tracing();
    let _container = get_redis_container().await;

    let stream = test_stream("batch");
    let uri = redis_uri();
    cleanup_stream(&uri, &stream).await?;

    let cfg = RedisSinkCfg {
        id: "test-redis-batch".into(),
        uri: uri.clone(),
        stream: stream.clone(),
        key: None,
        envelope: EnvelopeCfg::Native,
        encoding: EncodingCfg::Json,
        required: Some(true),
        send_timeout_secs: Some(5),
        batch_timeout_secs: Some(30),
        connect_timeout_secs: Some(10),
    };

    let cancel = CancellationToken::new();
    let sink = RedisSink::new(&cfg, cancel)?;

    // Send a batch of 100 events
    let events: Vec<Event> = (0..100).map(make_test_event).collect();
    sink.send_batch(&events).await?;

    // Verify all events are in the stream
    let len = stream_length(&uri, &stream).await?;
    assert_eq!(len, 100, "expected 100 entries in stream");

    info!("✓ batch of 100 events sent successfully");
    cleanup_stream(&uri, &stream).await?;
    Ok(())
}

/// Test that empty batch is a no-op.
#[tokio::test]
#[ignore = "requires docker"]
async fn redis_sink_empty_batch_is_noop() -> Result<()> {
    init_test_tracing();
    let _container = get_redis_container().await;

    let stream = test_stream("empty_batch");
    let uri = redis_uri();
    cleanup_stream(&uri, &stream).await?;

    let cfg = RedisSinkCfg {
        id: "test-redis-empty".into(),
        uri: uri.clone(),
        stream: stream.clone(),
        key: None,
        envelope: EnvelopeCfg::Native,
        encoding: EncodingCfg::Json,
        required: Some(true),
        send_timeout_secs: Some(5),
        batch_timeout_secs: Some(30),
        connect_timeout_secs: Some(10),
    };

    let cancel = CancellationToken::new();
    let sink = RedisSink::new(&cfg, cancel)?;

    // Send empty batch
    sink.send_batch(&[]).await?;

    // Stream should not exist or be empty
    let len = stream_length(&uri, &stream).await.unwrap_or(0);
    assert_eq!(len, 0, "stream should be empty after empty batch");

    info!("✓ empty batch is a no-op");
    Ok(())
}

// =============================================================================
// Envelope Format Tests
// =============================================================================

/// Test Native envelope format (direct Event serialization).
#[tokio::test]
#[ignore = "requires docker"]
async fn redis_sink_native_envelope() -> Result<()> {
    init_test_tracing();
    let _container = get_redis_container().await;

    let stream = test_stream("native_envelope");
    let uri = redis_uri();
    cleanup_stream(&uri, &stream).await?;

    let cfg = RedisSinkCfg {
        id: "test-native".into(),
        uri: uri.clone(),
        stream: stream.clone(),
        key: None,
        envelope: EnvelopeCfg::Native,
        encoding: EncodingCfg::Json,
        required: Some(true),
        send_timeout_secs: Some(5),
        batch_timeout_secs: Some(30),
        connect_timeout_secs: Some(10),
    };

    let cancel = CancellationToken::new();
    let sink = RedisSink::new(&cfg, cancel)?;

    let event = make_test_event(1);
    sink.send(&event).await?;

    let entries = read_stream_entries(&uri, &stream).await?;
    assert_eq!(entries.len(), 1, "expected one entry");

    let (_id, fields) = &entries[0];
    let df_event = fields.iter().find(|(k, _)| k == "df-event").unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&df_event.1)?;

    // Verify native format
    assert!(
        parsed.get("op").is_some(),
        "native format should have 'op' at top level"
    );
    assert!(
        parsed.get("source").is_some(),
        "native format should have 'source' at top level"
    );
    assert!(
        parsed.get("payload").is_none(),
        "native format should NOT have 'payload' wrapper"
    );

    // Verify op is valid Debezium code
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
    cleanup_stream(&uri, &stream).await?;
    Ok(())
}

/// Test Debezium envelope format (payload wrapper).
#[tokio::test]
#[ignore = "requires docker"]
async fn redis_sink_debezium_envelope() -> Result<()> {
    init_test_tracing();
    let _container = get_redis_container().await;

    let stream = test_stream("debezium_envelope");
    let uri = redis_uri();
    cleanup_stream(&uri, &stream).await?;

    let cfg = RedisSinkCfg {
        id: "test-debezium".into(),
        uri: uri.clone(),
        stream: stream.clone(),
        key: None,
        envelope: EnvelopeCfg::Debezium,
        encoding: EncodingCfg::Json,
        required: Some(true),
        send_timeout_secs: Some(5),
        batch_timeout_secs: Some(30),
        connect_timeout_secs: Some(10),
    };

    let cancel = CancellationToken::new();
    let sink = RedisSink::new(&cfg, cancel)?;

    let event = make_test_event(1);
    sink.send(&event).await?;

    let entries = read_stream_entries(&uri, &stream).await?;
    assert_eq!(entries.len(), 1, "expected one entry");

    let (_id, fields) = &entries[0];
    let df_event = fields.iter().find(|(k, _)| k == "df-event").unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&df_event.1)?;

    // Verify Debezium format has payload wrapper
    assert!(
        parsed.get("payload").is_some(),
        "debezium format must have 'payload' wrapper"
    );
    assert!(
        parsed.get("payload").unwrap().is_object(),
        "payload must be an object"
    );

    // Verify payload contains event fields
    let payload = parsed.get("payload").unwrap();
    assert!(
        payload.get("op").is_some(),
        "payload should contain 'op' field"
    );
    assert!(
        payload.get("source").is_some(),
        "payload should contain 'source' field"
    );

    // Verify op is valid Debezium code
    let op = payload.get("op").and_then(|v| v.as_str());
    assert!(
        matches!(
            op,
            Some("c") | Some("u") | Some("d") | Some("r") | Some("t")
        ),
        "op should be a valid Debezium operation code, got: {:?}",
        op
    );

    info!("✓ debezium envelope format works correctly");
    cleanup_stream(&uri, &stream).await?;
    Ok(())
}

/// Test CloudEvents envelope format.
#[tokio::test]
#[ignore = "requires docker"]
async fn redis_sink_cloudevents_envelope() -> Result<()> {
    init_test_tracing();
    let _container = get_redis_container().await;

    let stream = test_stream("cloudevents_envelope");
    let uri = redis_uri();
    cleanup_stream(&uri, &stream).await?;

    let cfg = RedisSinkCfg {
        id: "test-cloudevents".into(),
        uri: uri.clone(),
        stream: stream.clone(),
        key: None,
        envelope: EnvelopeCfg::CloudEvents {
            type_prefix: "com.deltaforge.cdc".to_string(),
        },
        encoding: EncodingCfg::Json,
        required: Some(true),
        send_timeout_secs: Some(5),
        batch_timeout_secs: Some(30),
        connect_timeout_secs: Some(10),
    };

    let cancel = CancellationToken::new();
    let sink = RedisSink::new(&cfg, cancel)?;

    let event = make_test_event(1);
    sink.send(&event).await?;

    let entries = read_stream_entries(&uri, &stream).await?;
    assert_eq!(entries.len(), 1, "expected one entry");

    let (_id, fields) = &entries[0];
    let df_event = fields.iter().find(|(k, _)| k == "df-event").unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&df_event.1)?;

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
    assert!(
        parsed.get("type").is_some(),
        "CloudEvents must have 'type' attribute"
    );

    // Verify source format: deltaforge/{name}/{full_table_name}
    let source = parsed.get("source").and_then(|v| v.as_str()).unwrap_or("");
    assert!(
        source.starts_with("deltaforge/"),
        "CloudEvents source should start with 'deltaforge/', got: {}",
        source
    );

    // Verify type format: {prefix}.{op_suffix}
    let type_field = parsed.get("type").and_then(|v| v.as_str()).unwrap_or("");
    assert!(
        type_field.starts_with("com.deltaforge.cdc."),
        "CloudEvents type should start with configured prefix, got: {}",
        type_field
    );
    let valid_suffixes =
        ["created", "updated", "deleted", "snapshot", "truncated"];
    let has_valid_suffix =
        valid_suffixes.iter().any(|s| type_field.ends_with(s));
    assert!(
        has_valid_suffix,
        "CloudEvents type should end with valid op suffix, got: {}",
        type_field
    );

    // Verify optional attributes
    assert_eq!(
        parsed.get("datacontenttype").and_then(|v| v.as_str()),
        Some("application/json"),
        "CloudEvents should have datacontenttype application/json"
    );
    assert!(
        parsed.get("time").is_some(),
        "CloudEvents should have 'time' attribute"
    );

    // Verify data payload
    assert!(
        parsed.get("data").is_some(),
        "CloudEvents must have 'data' attribute"
    );
    let data = parsed.get("data").unwrap();
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
    cleanup_stream(&uri, &stream).await?;
    Ok(())
}

/// Test batch send with Debezium envelope.
#[tokio::test]
#[ignore = "requires docker"]
async fn redis_sink_debezium_envelope_batch() -> Result<()> {
    init_test_tracing();
    let _container = get_redis_container().await;

    let stream = test_stream("debezium_batch");
    let uri = redis_uri();
    cleanup_stream(&uri, &stream).await?;

    let cfg = RedisSinkCfg {
        id: "test-debezium-batch".into(),
        uri: uri.clone(),
        stream: stream.clone(),
        key: None,
        envelope: EnvelopeCfg::Debezium,
        encoding: EncodingCfg::Json,
        required: Some(true),
        send_timeout_secs: Some(5),
        batch_timeout_secs: Some(30),
        connect_timeout_secs: Some(10),
    };

    let cancel = CancellationToken::new();
    let sink = RedisSink::new(&cfg, cancel)?;

    let events: Vec<Event> = (0..50).map(make_test_event).collect();
    sink.send_batch(&events).await?;

    let entries = read_stream_entries(&uri, &stream).await?;
    assert_eq!(entries.len(), 50, "should have 50 entries");

    // Verify all entries have Debezium envelope
    for (_id, fields) in &entries {
        let df_event = fields.iter().find(|(k, _)| k == "df-event").unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&df_event.1)?;
        assert!(
            parsed.get("payload").is_some(),
            "all batch messages should have Debezium payload wrapper"
        );
    }

    info!("✓ debezium envelope batch works correctly");
    cleanup_stream(&uri, &stream).await?;
    Ok(())
}

/// Test CloudEvents envelope with different operation types.
#[tokio::test]
#[ignore = "requires docker"]
async fn redis_sink_cloudevents_operations() -> Result<()> {
    init_test_tracing();
    let _container = get_redis_container().await;

    let stream = test_stream("cloudevents_ops");
    let uri = redis_uri();
    cleanup_stream(&uri, &stream).await?;

    let cfg = RedisSinkCfg {
        id: "test-cloudevents-ops".into(),
        uri: uri.clone(),
        stream: stream.clone(),
        key: None,
        envelope: EnvelopeCfg::CloudEvents {
            type_prefix: "io.deltaforge.test".to_string(),
        },
        encoding: EncodingCfg::Json,
        required: Some(true),
        send_timeout_secs: Some(5),
        batch_timeout_secs: Some(30),
        connect_timeout_secs: Some(10),
    };

    let cancel = CancellationToken::new();
    let sink = RedisSink::new(&cfg, cancel)?;

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

    let entries = read_stream_entries(&uri, &stream).await?;
    assert_eq!(entries.len(), 3, "should have 3 entries");

    // Collect all type suffixes
    let mut type_suffixes: Vec<String> = Vec::new();
    for (_id, fields) in &entries {
        let df_event = fields.iter().find(|(k, _)| k == "df-event").unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&df_event.1)?;

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

        let suffix =
            type_field.strip_prefix("io.deltaforge.test.").unwrap_or("");
        type_suffixes.push(suffix.to_string());

        // Verify data has valid op
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
    cleanup_stream(&uri, &stream).await?;
    Ok(())
}

/// Test that default envelope is Native.
#[tokio::test]
#[ignore = "requires docker"]
async fn redis_sink_default_envelope_is_native() -> Result<()> {
    init_test_tracing();
    let _container = get_redis_container().await;

    let stream = test_stream("default_envelope");
    let uri = redis_uri();
    cleanup_stream(&uri, &stream).await?;

    let cfg = RedisSinkCfg {
        id: "test-default".into(),
        uri: uri.clone(),
        stream: stream.clone(),
        key: None,
        envelope: EnvelopeCfg::default(),
        encoding: EncodingCfg::default(),
        required: Some(true),
        send_timeout_secs: Some(5),
        batch_timeout_secs: Some(30),
        connect_timeout_secs: Some(10),
    };

    // Verify defaults
    assert_eq!(cfg.envelope, EnvelopeCfg::Native);
    assert_eq!(cfg.encoding, EncodingCfg::Json);

    let cancel = CancellationToken::new();
    let sink = RedisSink::new(&cfg, cancel)?;

    let event = make_test_event(1);
    sink.send(&event).await?;

    let entries = read_stream_entries(&uri, &stream).await?;
    let (_id, fields) = &entries[0];
    let df_event = fields.iter().find(|(k, _)| k == "df-event").unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&df_event.1)?;

    // Verify native format (no payload wrapper)
    assert!(
        parsed.get("payload").is_none(),
        "default (native) envelope should NOT have payload wrapper"
    );
    assert!(
        parsed.get("op").is_some(),
        "default (native) envelope should have 'op' at top level"
    );

    info!("✓ default envelope is Native");
    cleanup_stream(&uri, &stream).await?;
    Ok(())
}

// =============================================================================
// Connection and Retry Tests
// =============================================================================

/// Test that connection is established lazily and reused.
#[tokio::test]
#[ignore = "requires docker"]
async fn redis_sink_connection_reuse() -> Result<()> {
    init_test_tracing();
    let _container = get_redis_container().await;

    let stream = test_stream("conn_reuse");
    let uri = redis_uri();
    cleanup_stream(&uri, &stream).await?;

    let cfg = RedisSinkCfg {
        id: "test-conn-reuse".into(),
        uri: uri.clone(),
        stream: stream.clone(),
        key: None,
        envelope: EnvelopeCfg::Native,
        encoding: EncodingCfg::Json,
        required: Some(true),
        send_timeout_secs: Some(5),
        batch_timeout_secs: Some(30),
        connect_timeout_secs: Some(10),
    };

    let cancel = CancellationToken::new();
    let sink = RedisSink::new(&cfg, cancel)?;

    // Send multiple events - should reuse connection
    for i in 0..10 {
        let event = make_test_event(i);
        sink.send(&event).await?;
    }

    let len = stream_length(&uri, &stream).await?;
    assert_eq!(len, 10, "expected 10 entries");

    info!("✓ connection reused across multiple sends");
    cleanup_stream(&uri, &stream).await?;
    Ok(())
}

/// Test sink with invalid URI fails gracefully.
#[tokio::test]
#[ignore = "requires docker"]
async fn redis_sink_invalid_uri() -> Result<()> {
    init_test_tracing();
    let _container = get_redis_container().await;

    let cfg = RedisSinkCfg {
        id: "test-invalid".into(),
        uri: "redis://invalid-host:9999/0".into(),
        stream: "df.test.invalid".into(),
        key: None,
        envelope: EnvelopeCfg::Native,
        encoding: EncodingCfg::Json,
        required: Some(true),
        send_timeout_secs: Some(2),
        batch_timeout_secs: Some(5),
        connect_timeout_secs: Some(2),
    };

    let cancel = CancellationToken::new();
    let sink = RedisSink::new(&cfg, cancel)?;

    // Send should fail after retries
    let event = make_test_event(1);
    let result = sink.send(&event).await;

    assert!(result.is_err(), "send to invalid host should fail");
    info!("✓ invalid URI fails gracefully: {:?}", result.err());
    Ok(())
}

// =============================================================================
// Cancellation Tests
// =============================================================================

/// Test that cancellation stops in-flight operations.
#[tokio::test]
#[ignore = "requires docker"]
async fn redis_sink_respects_cancellation() -> Result<()> {
    init_test_tracing();
    let _container = get_redis_container().await;

    // Use an invalid URI so retry loop keeps trying
    let cfg = RedisSinkCfg {
        id: "test-cancel".into(),
        uri: "redis://invalid-host:9999/0".into(),
        stream: "df.test.cancel".into(),
        key: None,
        envelope: EnvelopeCfg::Native,
        encoding: EncodingCfg::Json,
        required: Some(true),
        send_timeout_secs: Some(1),
        batch_timeout_secs: Some(5),
        connect_timeout_secs: Some(1),
    };

    let cancel = CancellationToken::new();
    let sink = Arc::new(RedisSink::new(&cfg, cancel.clone())?);

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
// Large Payload Tests
// =============================================================================

/// Test handling of large events.
#[tokio::test]
#[ignore = "requires docker"]
async fn redis_sink_large_events() -> Result<()> {
    init_test_tracing();
    let _container = get_redis_container().await;

    let stream = test_stream("large");
    let uri = redis_uri();
    cleanup_stream(&uri, &stream).await?;

    let cfg = RedisSinkCfg {
        id: "test-large".into(),
        uri: uri.clone(),
        stream: stream.clone(),
        key: None,
        envelope: EnvelopeCfg::Native,
        encoding: EncodingCfg::Json,
        required: Some(true),
        send_timeout_secs: Some(10),
        batch_timeout_secs: Some(60),
        connect_timeout_secs: Some(10),
    };

    let cancel = CancellationToken::new();
    let sink = RedisSink::new(&cfg, cancel)?;

    // Send a 1MB event
    let large_event = make_large_event(1, 1024 * 1024);
    sink.send(&large_event).await?;

    let len = stream_length(&uri, &stream).await?;
    assert_eq!(len, 1, "large event should be in stream");

    info!("✓ large event (1MB) sent successfully");
    cleanup_stream(&uri, &stream).await?;
    Ok(())
}

/// Test batch of large events.
#[tokio::test]
#[ignore = "requires docker"]
async fn redis_sink_large_batch() -> Result<()> {
    init_test_tracing();
    let _container = get_redis_container().await;

    let stream = test_stream("large_batch");
    let uri = redis_uri();
    cleanup_stream(&uri, &stream).await?;

    let cfg = RedisSinkCfg {
        id: "test-large-batch".into(),
        uri: uri.clone(),
        stream: stream.clone(),
        key: None,
        envelope: EnvelopeCfg::Native,
        encoding: EncodingCfg::Json,
        required: Some(true),
        send_timeout_secs: Some(10),
        batch_timeout_secs: Some(60),
        connect_timeout_secs: Some(10),
    };

    let cancel = CancellationToken::new();
    let sink = RedisSink::new(&cfg, cancel)?;

    // Send 10 events of 100KB each
    let events: Vec<Event> =
        (0..10).map(|i| make_large_event(i, 100 * 1024)).collect();
    sink.send_batch(&events).await?;

    let len = stream_length(&uri, &stream).await?;
    assert_eq!(len, 10, "all large events should be in stream");

    info!("✓ batch of large events sent successfully");
    cleanup_stream(&uri, &stream).await?;
    Ok(())
}

// =============================================================================
// Concurrent Access Tests
// =============================================================================

/// Test concurrent sends from multiple tasks.
#[tokio::test]
#[ignore = "requires docker"]
async fn redis_sink_concurrent_sends() -> Result<()> {
    init_test_tracing();
    let _container = get_redis_container().await;

    let stream = test_stream("concurrent");
    let uri = redis_uri();
    cleanup_stream(&uri, &stream).await?;

    let cfg = RedisSinkCfg {
        id: "test-concurrent".into(),
        uri: uri.clone(),
        stream: stream.clone(),
        key: None,
        envelope: EnvelopeCfg::Native,
        encoding: EncodingCfg::Json,
        required: Some(true),
        send_timeout_secs: Some(10),
        batch_timeout_secs: Some(30),
        connect_timeout_secs: Some(10),
    };

    let cancel = CancellationToken::new();
    let sink = Arc::new(RedisSink::new(&cfg, cancel)?);

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

    let len = stream_length(&uri, &stream).await?;
    assert_eq!(len, 100, "all concurrent events should be in stream");

    info!("✓ 100 concurrent events sent successfully");
    cleanup_stream(&uri, &stream).await?;
    Ok(())
}

// =============================================================================
// Reconnection and Recovery Tests
// =============================================================================

/// Test that sink recovers after Redis restarts.
#[tokio::test]
#[ignore = "requires docker"]
async fn redis_sink_recovers_after_restart() -> Result<()> {
    init_test_tracing();

    // Start a dedicated container for this test (not shared)
    let restart_port: u16 = 6398;
    let image = GenericImage::new("redis", "7-alpine")
        .with_wait_for(WaitFor::message_on_stdout(
            "Ready to accept connections",
        ))
        .with_mapped_port(restart_port, 6379.tcp());

    let container = image.start().await?;
    let uri = format!("redis://127.0.0.1:{}/0", restart_port);

    wait_for_redis(&uri, Duration::from_secs(30)).await?;

    let stream = "df.test.restart";
    let cfg = RedisSinkCfg {
        id: "test-restart".into(),
        uri: uri.clone(),
        stream: stream.into(),
        key: None,
        envelope: EnvelopeCfg::Native,
        encoding: EncodingCfg::Json,
        required: Some(true),
        send_timeout_secs: Some(5),
        batch_timeout_secs: Some(30),
        connect_timeout_secs: Some(10),
    };

    let cancel = CancellationToken::new();
    let sink = Arc::new(RedisSink::new(&cfg, cancel.clone())?);

    // Send first event successfully
    let event1 = make_test_event(1);
    sink.send(&event1).await?;
    info!("✓ first event sent before restart");

    // Stop the container (simulates Redis going down)
    info!("stopping Redis container...");
    container.stop().await?;
    sleep(Duration::from_secs(1)).await;

    // Start sending in background - should retry until Redis comes back
    let sink_clone = sink.clone();
    let send_handle = tokio::spawn(async move {
        let event2 = make_test_event(2);
        sink_clone.send(&event2).await
    });

    // Wait a bit for retries to start
    sleep(Duration::from_secs(2)).await;

    // Restart the container
    info!("restarting Redis container...");
    container.start().await?;
    wait_for_redis(&uri, Duration::from_secs(30)).await?;
    info!("Redis is back up");

    // The send should eventually succeed
    let result = timeout(Duration::from_secs(30), send_handle).await??;
    assert!(
        result.is_ok(),
        "send should succeed after Redis recovers: {:?}",
        result.err()
    );

    info!("✓ sink recovered after Redis restart");
    Ok(())
}

/// Test that sink handles connection drop mid-operation.
#[tokio::test]
#[ignore = "requires docker"]
async fn redis_sink_handles_connection_drop() -> Result<()> {
    init_test_tracing();
    let _container = get_redis_container().await;

    let stream = test_stream("conn_drop");
    let uri = redis_uri();
    cleanup_stream(&uri, &stream).await?;

    let cfg = RedisSinkCfg {
        id: "test-conn-drop".into(),
        uri: uri.clone(),
        stream: stream.clone(),
        key: None,
        envelope: EnvelopeCfg::Native,
        encoding: EncodingCfg::Json,
        required: Some(true),
        send_timeout_secs: Some(5),
        batch_timeout_secs: Some(30),
        connect_timeout_secs: Some(10),
    };

    let cancel = CancellationToken::new();
    let sink = RedisSink::new(&cfg, cancel)?;

    // Send first event to establish connection
    let event1 = make_test_event(1);
    sink.send(&event1).await?;
    info!("✓ first event sent, connection established");

    // Kill all client connections using CLIENT KILL
    let client = redis::Client::open(uri.clone())?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    // Get our client info and kill other connections
    let clients: String = redis::cmd("CLIENT")
        .arg("LIST")
        .query_async(&mut conn)
        .await?;
    debug!("connected clients before kill: {}", clients.lines().count());

    // CLIENT KILL TYPE normal kills all normal client connections
    let _: Result<(), _> = redis::cmd("CLIENT")
        .arg("KILL")
        .arg("TYPE")
        .arg("normal")
        .query_async::<()>(&mut conn)
        .await;

    info!("killed client connections");
    sleep(Duration::from_millis(100)).await;

    // Next send should reconnect and succeed
    let event2 = make_test_event(2);
    sink.send(&event2).await?;

    // Verify both events are in the stream
    let len = stream_length(&uri, &stream).await?;
    assert_eq!(len, 2, "both events should be in stream after reconnection");

    info!("✓ sink recovered after connection drop");
    cleanup_stream(&uri, &stream).await?;
    Ok(())
}

/// Test batch retry after transient failure.
#[tokio::test]
#[ignore = "requires docker"]
async fn redis_sink_batch_retries_on_failure() -> Result<()> {
    init_test_tracing();
    let _container = get_redis_container().await;

    let stream = test_stream("batch_retry");
    let uri = redis_uri();
    cleanup_stream(&uri, &stream).await?;

    let cfg = RedisSinkCfg {
        id: "test-batch-retry".into(),
        uri: uri.clone(),
        stream: stream.clone(),
        key: None,
        envelope: EnvelopeCfg::Native,
        encoding: EncodingCfg::Json,
        required: Some(true),
        send_timeout_secs: Some(5),
        batch_timeout_secs: Some(30),
        connect_timeout_secs: Some(10),
    };

    let cancel = CancellationToken::new();
    let sink = RedisSink::new(&cfg, cancel)?;

    // Send first batch to warm up connection
    let events1: Vec<Event> = (0..10).map(make_test_event).collect();
    sink.send_batch(&events1).await?;
    info!("✓ first batch sent");

    // Kill connections
    let client = redis::Client::open(uri.clone())?;
    let mut conn = client.get_multiplexed_async_connection().await?;
    let _: Result<(), _> = redis::cmd("CLIENT")
        .arg("KILL")
        .arg("TYPE")
        .arg("normal")
        .query_async::<()>(&mut conn)
        .await;

    sleep(Duration::from_millis(100)).await;

    // Send second batch - should retry and succeed
    let events2: Vec<Event> = (100..110).map(make_test_event).collect();
    sink.send_batch(&events2).await?;

    let len = stream_length(&uri, &stream).await?;
    assert_eq!(len, 20, "all 20 events should be in stream");

    info!("✓ batch recovered after connection drop");
    cleanup_stream(&uri, &stream).await?;
    Ok(())
}

// =============================================================================
// Trait Implementation Tests
// =============================================================================

/// Test Sink trait methods.
#[tokio::test]
#[ignore = "requires docker"]
async fn redis_sink_trait_implementation() -> Result<()> {
    init_test_tracing();
    let _container = get_redis_container().await;

    let cfg = RedisSinkCfg {
        id: "test-trait".into(),
        uri: redis_uri(),
        stream: "df.test.trait".into(),
        key: None,
        envelope: EnvelopeCfg::Native,
        encoding: EncodingCfg::Json,
        required: Some(true),
        send_timeout_secs: Some(5),
        batch_timeout_secs: Some(30),
        connect_timeout_secs: Some(10),
    };

    let cancel = CancellationToken::new();
    let sink = RedisSink::new(&cfg, cancel)?;

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
async fn redis_sink_optional() -> Result<()> {
    init_test_tracing();
    let _container = get_redis_container().await;

    let cfg = RedisSinkCfg {
        id: "test-optional".into(),
        uri: redis_uri(),
        stream: "df.test.optional".into(),
        key: None,
        envelope: EnvelopeCfg::Native,
        encoding: EncodingCfg::Json,
        required: Some(false),
        send_timeout_secs: None,
        batch_timeout_secs: None,
        connect_timeout_secs: None,
    };

    let cancel = CancellationToken::new();
    let sink = RedisSink::new(&cfg, cancel)?;

    assert!(!sink.required(), "sink should be optional");

    info!("✓ optional sink configuration works");
    Ok(())
}

// =============================================================================
// Dynamic Routing Tests
// =============================================================================

/// Find a field value in a Redis stream entry's field list.
fn find_field<'a>(
    fields: &'a [(String, String)],
    name: &str,
) -> Option<&'a str> {
    fields
        .iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.as_str())
}

/// Stream template routes events to per-table Redis streams.
#[tokio::test]
#[ignore = "requires docker"]
async fn redis_sink_stream_template_routes_by_table() -> Result<()> {
    init_test_tracing();
    let _container = get_redis_container().await;

    let uri = redis_uri();
    let stream_orders = test_stream("route-orders");
    let stream_users = test_stream("route-users");
    cleanup_stream(&uri, &stream_orders).await?;
    cleanup_stream(&uri, &stream_users).await?;

    let cfg = RedisSinkCfg {
        id: "test-routing".into(),
        uri: uri.clone(),
        stream: "df.test.route_${source.table}".into(),
        key: None,
        envelope: EnvelopeCfg::Native,
        encoding: EncodingCfg::Json,
        required: Some(true),
        send_timeout_secs: Some(5),
        batch_timeout_secs: Some(30),
        connect_timeout_secs: Some(10),
    };

    let cancel = CancellationToken::new();
    let sink = RedisSink::new(&cfg, cancel)?;

    sink.send_batch(&[
        make_event_for_table(1, "orders"),
        make_event_for_table(2, "users"),
    ])
    .await?;

    let entries = read_stream_entries(&uri, &stream_orders).await?;
    assert_eq!(entries.len(), 1, "orders stream should have 1 entry");
    let payload =
        find_field(&entries[0].1, "df-event").expect("df-event missing");
    let parsed: serde_json::Value = serde_json::from_str(payload)?;
    assert_eq!(parsed["source"]["table"], "orders");

    let entries = read_stream_entries(&uri, &stream_users).await?;
    assert_eq!(entries.len(), 1, "users stream should have 1 entry");
    let payload =
        find_field(&entries[0].1, "df-event").expect("df-event missing");
    let parsed: serde_json::Value = serde_json::from_str(payload)?;
    assert_eq!(parsed["source"]["table"], "users");

    info!("✓ stream template routes events to correct streams");
    cleanup_stream(&uri, &stream_orders).await?;
    cleanup_stream(&uri, &stream_users).await?;
    Ok(())
}

/// EventRouting overrides stream + key appears as df-key field.
#[tokio::test]
#[ignore = "requires docker"]
async fn redis_sink_routing_override_with_key() -> Result<()> {
    init_test_tracing();
    let _container = get_redis_container().await;

    let uri = redis_uri();
    let default_stream = test_stream("route-default");
    let override_stream = test_stream("route-override");
    cleanup_stream(&uri, &default_stream).await?;
    cleanup_stream(&uri, &override_stream).await?;

    let cfg = RedisSinkCfg {
        id: "test-override".into(),
        uri: uri.clone(),
        stream: default_stream.clone(),
        key: None,
        envelope: EnvelopeCfg::Native,
        encoding: EncodingCfg::Json,
        required: Some(true),
        send_timeout_secs: Some(5),
        batch_timeout_secs: Some(30),
        connect_timeout_secs: Some(10),
    };

    let cancel = CancellationToken::new();
    let sink = RedisSink::new(&cfg, cancel)?;

    let mut event = make_test_event(1);
    event.routing = Some(EventRouting {
        topic: Some(override_stream.clone()),
        key: Some("customer-42".into()),
        ..Default::default()
    });

    sink.send(&event).await?;

    let len = stream_length(&uri, &default_stream).await.unwrap_or(0);
    assert_eq!(len, 0, "default stream should be empty");

    let entries = read_stream_entries(&uri, &override_stream).await?;
    assert_eq!(entries.len(), 1);

    let df_key = find_field(&entries[0].1, "df-key");
    assert_eq!(df_key, Some("customer-42"), "df-key field should match");

    info!("✓ EventRouting overrides stream + key delivered as df-key");
    cleanup_stream(&uri, &default_stream).await?;
    cleanup_stream(&uri, &override_stream).await?;
    Ok(())
}

// =============================================================================
// Raw Payload Tests (Outbox)
// =============================================================================

/// When routing.raw_payload is true, the sink should serialize event.after
/// directly, bypassing the configured envelope format.
#[tokio::test]
#[ignore = "requires docker"]
async fn redis_sink_raw_payload_bypasses_envelope() -> Result<()> {
    init_test_tracing();
    let _container = get_redis_container().await;

    let stream = test_stream("raw_payload");
    let uri = redis_uri();
    cleanup_stream(&uri, &stream).await?;

    // Deliberately use Debezium envelope — raw_payload should bypass it
    let cfg = RedisSinkCfg {
        id: "test-raw".into(),
        uri: uri.clone(),
        stream: stream.clone(),
        key: None,
        envelope: EnvelopeCfg::Debezium,
        encoding: EncodingCfg::Json,
        required: Some(true),
        send_timeout_secs: Some(5),
        batch_timeout_secs: Some(30),
        connect_timeout_secs: Some(10),
    };

    let cancel = CancellationToken::new();
    let sink = RedisSink::new(&cfg, cancel)?;

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

    sink.send(&raw_event).await?;
    sink.send(&normal_event).await?;

    let entries = read_stream_entries(&uri, &stream).await?;
    assert_eq!(entries.len(), 2, "should have 2 entries in stream");

    // Redis stream entries are (id, [(field, value), ...])
    // The sink writes the serialized payload in the "df-event" field
    let payloads: Vec<serde_json::Value> = entries
        .iter()
        .filter_map(|(_id, fields)| {
            fields
                .iter()
                .find(|(k, _)| k == "df-event")
                .map(|(_, v)| serde_json::from_str(v).unwrap())
        })
        .collect();

    assert_eq!(payloads.len(), 2, "both entries should have df-event field");

    // One should be raw payload, other should be Debezium envelope
    let raw_payload = payloads
        .iter()
        .find(|v| v.get("order_id").is_some())
        .expect("should have raw payload entry");
    let envelope_payload = payloads
        .iter()
        .find(|v| v.get("payload").is_some())
        .expect("should have Debezium-wrapped entry");

    // Raw: just the payload
    assert_eq!(raw_payload["order_id"], 42);
    assert_eq!(raw_payload["total"], 99.99);
    assert!(raw_payload.get("op").is_none(), "raw should have no 'op'");
    assert!(
        raw_payload.get("schema").is_none(),
        "raw should have no 'schema'"
    );

    // Normal: full Debezium envelope
    assert!(envelope_payload["payload"].get("op").is_some());

    info!("✓ raw_payload bypasses envelope on Redis stream");
    cleanup_stream(&uri, &stream).await?;
    Ok(())
}
