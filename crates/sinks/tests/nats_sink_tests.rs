//! Integration tests for NATS JetStream sink.
//!
//! These tests require Docker and pull `nats:2.10-alpine`.
//!
//! Run with:
//! ```bash
//! cargo test -p sinks --test nats_sink_tests -- --include-ignored --nocapture --test-threads=1
//! ```

use anyhow::Result;
use async_nats::jetstream::{self, stream::Config as StreamConfig};
use ctor::dtor;
use deltaforge_config::{EncodingCfg, EnvelopeCfg, NatsSinkCfg};
use deltaforge_core::{Event, EventRouting, Sink};
use serde_json::json;
use sinks::nats::NatsSink;
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
use tracing::{debug, info};

mod sink_test_common;
use sink_test_common::{
    init_test_tracing, make_event_for_table, make_large_event, make_test_event,
};

// =============================================================================
// Shared Test Infrastructure
// =============================================================================

const NATS_PORT: u16 = 4322;

/// Shared NATS container - initialized once, reused by all tests.
static NATS_CONTAINER: OnceCell<ContainerAsync<GenericImage>> =
    OnceCell::const_new();

#[dtor]
fn cleanup() {
    // Force container cleanup on process exit
    if let Some(container) = NATS_CONTAINER.get() {
        std::process::Command::new("docker")
            .args(["rm", "-f", container.id()])
            .output()
            .ok();
    }
}

/// Get or start the shared NATS container with JetStream enabled.
async fn get_nats_container() -> &'static ContainerAsync<GenericImage> {
    NATS_CONTAINER
        .get_or_init(|| async {
            info!("starting NATS container with JetStream...");

            // Use duration wait, then verify with our own polling
            // NATS with JetStream typically starts within 5 seconds
            let image = GenericImage::new("nats", "2.10-alpine")
                .with_wait_for(WaitFor::Duration {
                    length: Duration::from_secs(5),
                })
                .with_cmd(vec!["-js"])
                .with_mapped_port(NATS_PORT, 4222.tcp());

            let container = image.start().await.expect("start nats container");
            info!("NATS container started: {}", container.id());

            // Wait for NATS JetStream to be fully ready
            wait_for_nats(&nats_url(), Duration::from_secs(30))
                .await
                .expect("NATS should be ready");

            container
        })
        .await
}

/// Poll NATS until JetStream is ready to accept connections.
async fn wait_for_nats(url: &str, timeout_duration: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout_duration;

    while Instant::now() < deadline {
        match async_nats::connect(url).await {
            Ok(client) => {
                // Verify JetStream is available by querying account info
                let js = jetstream::new(client);
                if js.query_account().await.is_ok() {
                    info!("NATS JetStream is ready");
                    return Ok(());
                }
            }
            Err(e) => {
                debug!("waiting for NATS: {}", e);
            }
        }
        sleep(Duration::from_millis(100)).await;
    }

    anyhow::bail!("NATS not ready after {:?}", timeout_duration)
}

fn nats_url() -> String {
    format!("nats://127.0.0.1:{}", NATS_PORT)
}

/// Create a unique subject name for each test.
fn test_subject(test_name: &str) -> String {
    format!("df.test.{}", test_name.replace('_', "-"))
}

/// Create a unique stream name for each test.
fn test_stream(test_name: &str) -> String {
    format!("DF_TEST_{}", test_name.to_uppercase().replace('-', "_"))
}

/// Setup a JetStream stream for testing.
async fn setup_test_stream(
    url: &str,
    stream: &str,
    subject: &str,
) -> Result<()> {
    let client = async_nats::connect(url).await?;
    let js = jetstream::new(client);

    // Delete stream if exists
    let _ = js.delete_stream(stream).await;

    // Create stream
    js.create_stream(StreamConfig {
        name: stream.to_string(),
        subjects: vec![subject.to_string()],
        ..Default::default()
    })
    .await?;

    debug!("created test stream: {} with subject: {}", stream, subject);
    Ok(())
}

/// Cleanup a test stream.
async fn cleanup_test_stream(url: &str, stream: &str) -> Result<()> {
    let client = async_nats::connect(url).await?;
    let js = jetstream::new(client);
    let _ = js.delete_stream(stream).await;
    debug!("cleaned up stream: {}", stream);
    Ok(())
}

/// Get the message count in a stream.
async fn stream_message_count(url: &str, stream: &str) -> Result<u64> {
    let client = async_nats::connect(url).await?;
    let js = jetstream::new(client);
    let mut stream = js.get_stream(stream).await?;
    let info = stream.info().await?;
    Ok(info.state.messages)
}

/// Read messages from a stream.
async fn read_stream_messages(
    url: &str,
    stream_name: &str,
    count: usize,
) -> Result<Vec<Vec<u8>>> {
    let client = async_nats::connect(url).await?;
    let js = jetstream::new(client);
    let stream = js.get_stream(stream_name).await?;

    let mut messages = Vec::new();
    let consumer = stream
        .create_consumer(async_nats::jetstream::consumer::pull::Config {
            durable_name: Some(format!("test-consumer-{}", stream_name)),
            ..Default::default()
        })
        .await?;

    let batch = consumer.fetch().max_messages(count).messages().await?;
    use futures::StreamExt;
    let mut batch = std::pin::pin!(batch);

    while let Some(msg) = batch.next().await {
        if let Ok(msg) = msg {
            messages.push(msg.payload.to_vec());
            if messages.len() >= count {
                break;
            }
        }
    }

    Ok(messages)
}

// =============================================================================
// Test Helpers
// =============================================================================

/// Standard per-test setup: tracing, shared container, url/subject/stream creation.
/// Returns (url, subject, stream).
async fn setup(name: &str) -> Result<(String, String, String)> {
    init_test_tracing();
    get_nats_container().await;
    let url = nats_url();
    let subject = test_subject(name);
    let stream = test_stream(name);
    setup_test_stream(&url, &stream, &subject).await?;
    Ok((url, subject, stream))
}

/// Build a NatsSink with standard defaults (5s send, 30s batch, 10s connect).
fn make_sink(
    id: &str,
    url: &str,
    subject: &str,
    stream: Option<&str>,
    envelope: EnvelopeCfg,
) -> Result<NatsSink> {
    NatsSink::new(
        &NatsSinkCfg {
            id: id.into(),
            url: url.into(),
            subject: subject.into(),
            key: None,
            envelope,
            encoding: EncodingCfg::Json,
            stream: stream.map(Into::into),
            required: Some(true),
            send_timeout_secs: Some(5),
            batch_timeout_secs: Some(30),
            connect_timeout_secs: Some(10),
            credentials_file: None,
            username: None,
            password: None,
            token: None,
            filter: None,
        },
        CancellationToken::new(),
    )
}

// =============================================================================
// Basic Functionality Tests
// =============================================================================

/// Test that a single event is written to the stream correctly.
#[tokio::test]
#[ignore = "requires docker"]
async fn nats_sink_sends_single_event() -> Result<()> {
    let (url, subject, stream) = setup("single").await?;

    let sink = make_sink(
        "test-nats",
        &url,
        &subject,
        Some(&stream),
        EnvelopeCfg::Native,
    )?;

    let event = make_test_event(1);
    sink.send(&event).await?;

    // Verify the event is in the stream
    let count = stream_message_count(&url, &stream).await?;
    assert_eq!(count, 1, "expected exactly one message in stream");

    // Read and verify the message content
    let messages = read_stream_messages(&url, &stream, 1).await?;
    assert!(!messages.is_empty(), "should receive message");

    let parsed: Event = serde_json::from_slice(&messages[0])?;
    assert_eq!(parsed.event_id, event.event_id);

    info!("✓ single event sent successfully");
    cleanup_test_stream(&url, &stream).await?;
    Ok(())
}

/// Test batch send with concurrent publish optimization.
#[tokio::test]
#[ignore = "requires docker"]
async fn nats_sink_sends_batch() -> Result<()> {
    let (url, subject, stream) = setup("batch").await?;

    let sink = make_sink(
        "test-nats-batch",
        &url,
        &subject,
        Some(&stream),
        EnvelopeCfg::Native,
    )?;

    // Send a batch of 100 events
    let events: Vec<Event> = (0..100).map(make_test_event).collect();
    sink.send_batch(&events).await?;

    // Verify all events are in the stream
    let count = stream_message_count(&url, &stream).await?;
    assert_eq!(count, 100, "expected 100 messages in stream");

    info!("✓ batch of 100 events sent successfully");
    cleanup_test_stream(&url, &stream).await?;
    Ok(())
}

/// Test that empty batch is a no-op.
#[tokio::test]
#[ignore = "requires docker"]
async fn nats_sink_empty_batch_is_noop() -> Result<()> {
    let (url, subject, stream) = setup("empty-batch").await?;

    let sink = make_sink(
        "test-nats-empty",
        &url,
        &subject,
        Some(&stream),
        EnvelopeCfg::Native,
    )?;

    // Send empty batch - should not produce any messages
    sink.send_batch(&[]).await?;

    // Stream should be empty
    let count = stream_message_count(&url, &stream).await?;
    assert_eq!(count, 0, "stream should be empty after empty batch");

    info!("✓ empty batch is a no-op");
    cleanup_test_stream(&url, &stream).await?;
    Ok(())
}

// =============================================================================
// Envelope Format Tests
// =============================================================================

/// Test Native envelope format (direct Event serialization).
#[tokio::test]
#[ignore = "requires docker"]
async fn nats_sink_native_envelope() -> Result<()> {
    let (url, subject, stream) = setup("native-envelope").await?;

    let sink = make_sink(
        "test-native",
        &url,
        &subject,
        Some(&stream),
        EnvelopeCfg::Native,
    )?;

    let event = make_test_event(1);
    sink.send(&event).await?;

    let messages = read_stream_messages(&url, &stream, 1).await?;
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
    cleanup_test_stream(&url, &stream).await?;
    Ok(())
}

/// Test Debezium envelope format (payload wrapper).
#[tokio::test]
#[ignore = "requires docker"]
async fn nats_sink_debezium_envelope() -> Result<()> {
    let (url, subject, stream) = setup("debezium-envelope").await?;

    let sink = make_sink(
        "test-debezium",
        &url,
        &subject,
        Some(&stream),
        EnvelopeCfg::Debezium,
    )?;

    let event = make_test_event(1);
    sink.send(&event).await?;

    let messages = read_stream_messages(&url, &stream, 1).await?;
    assert!(!messages.is_empty(), "should receive message");

    let parsed: serde_json::Value = serde_json::from_slice(&messages[0])?;

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
    cleanup_test_stream(&url, &stream).await?;
    Ok(())
}

/// Test CloudEvents envelope format.
#[tokio::test]
#[ignore = "requires docker"]
async fn nats_sink_cloudevents_envelope() -> Result<()> {
    let (url, subject, stream) = setup("cloudevents-envelope").await?;

    let sink = make_sink(
        "test-cloudevents",
        &url,
        &subject,
        Some(&stream),
        EnvelopeCfg::CloudEvents {
            type_prefix: "com.deltaforge.cdc".into(),
        },
    )?;

    let event = make_test_event(1);
    sink.send(&event).await?;

    let messages = read_stream_messages(&url, &stream, 1).await?;
    assert!(!messages.is_empty(), "should receive message");

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
    cleanup_test_stream(&url, &stream).await?;
    Ok(())
}

/// Test batch send with Debezium envelope.
#[tokio::test]
#[ignore = "requires docker"]
async fn nats_sink_debezium_envelope_batch() -> Result<()> {
    let (url, subject, stream) = setup("debezium-batch").await?;

    let sink = make_sink(
        "test-debezium-batch",
        &url,
        &subject,
        Some(&stream),
        EnvelopeCfg::Debezium,
    )?;

    let events: Vec<Event> = (0..50).map(make_test_event).collect();
    sink.send_batch(&events).await?;

    let count = stream_message_count(&url, &stream).await?;
    assert_eq!(count, 50, "expected 50 messages in stream");

    // Verify format of a few messages
    let messages = read_stream_messages(&url, &stream, 5).await?;
    for msg in &messages {
        let parsed: serde_json::Value = serde_json::from_slice(msg)?;
        assert!(
            parsed.get("payload").is_some(),
            "debezium batch messages should have payload wrapper"
        );
    }

    info!("✓ debezium envelope batch works correctly");
    cleanup_test_stream(&url, &stream).await?;
    Ok(())
}

/// Test batch send with CloudEvents envelope.
#[tokio::test]
#[ignore = "requires docker"]
async fn nats_sink_cloudevents_envelope_batch() -> Result<()> {
    let (url, subject, stream) = setup("cloudevents-batch").await?;

    let sink = make_sink(
        "test-cloudevents-batch",
        &url,
        &subject,
        Some(&stream),
        EnvelopeCfg::CloudEvents {
            type_prefix: "io.deltaforge.test".into(),
        },
    )?;

    let events: Vec<Event> = (0..50).map(make_test_event).collect();
    sink.send_batch(&events).await?;

    let count = stream_message_count(&url, &stream).await?;
    assert_eq!(count, 50, "expected 50 messages in stream");

    // Verify format of a few messages
    let messages = read_stream_messages(&url, &stream, 5).await?;
    for msg in &messages {
        let parsed: serde_json::Value = serde_json::from_slice(msg)?;
        assert_eq!(
            parsed.get("specversion").and_then(|v| v.as_str()),
            Some("1.0"),
            "cloudevents batch messages should have specversion"
        );
        assert!(
            parsed.get("data").is_some(),
            "cloudevents batch messages should have data"
        );
    }

    info!("✓ cloudevents envelope batch works correctly");
    cleanup_test_stream(&url, &stream).await?;
    Ok(())
}

// =============================================================================
// Large Payload Tests
// =============================================================================

/// Test sending large events.
#[tokio::test]
#[ignore = "requires docker"]
async fn nats_sink_large_payload() -> Result<()> {
    let (url, subject, stream) = setup("large-payload").await?;

    let sink = NatsSink::new(
        &NatsSinkCfg {
            id: "test-large".into(),
            url: url.clone(),
            subject: subject.clone(),
            key: None,
            envelope: EnvelopeCfg::Native,
            encoding: EncodingCfg::Json,
            stream: Some(stream.clone()),
            required: Some(true),
            send_timeout_secs: Some(30),
            batch_timeout_secs: Some(60),
            connect_timeout_secs: Some(10),
            credentials_file: None,
            username: None,
            password: None,
            token: None,
            filter: None,
        },
        CancellationToken::new(),
    )?;

    // Test various payload sizes
    let sizes = [1_000, 10_000, 100_000, 500_000];
    for (i, size) in sizes.iter().enumerate() {
        let event = make_large_event(i as i64, *size);
        sink.send(&event).await?;
        debug!("sent event with {} byte payload", size);
    }

    let count = stream_message_count(&url, &stream).await?;
    assert_eq!(
        count,
        sizes.len() as u64,
        "all large events should be delivered"
    );

    info!("✓ large payloads sent successfully");
    cleanup_test_stream(&url, &stream).await?;
    Ok(())
}

/// Test batch of large events.
#[tokio::test]
#[ignore = "requires docker"]
async fn nats_sink_batch_large_payload() -> Result<()> {
    let (url, subject, stream) = setup("batch-large").await?;

    let sink = NatsSink::new(
        &NatsSinkCfg {
            id: "test-batch-large".into(),
            url: url.clone(),
            subject: subject.clone(),
            key: None,
            envelope: EnvelopeCfg::Native,
            encoding: EncodingCfg::Json,
            stream: Some(stream.clone()),
            required: Some(true),
            send_timeout_secs: Some(30),
            batch_timeout_secs: Some(120),
            connect_timeout_secs: Some(10),
            credentials_file: None,
            username: None,
            password: None,
            token: None,
            filter: None,
        },
        CancellationToken::new(),
    )?;

    // 20 events at 50KB each = ~1MB total
    let events: Vec<Event> =
        (0..20).map(|i| make_large_event(i, 50_000)).collect();

    sink.send_batch(&events).await?;

    let count = stream_message_count(&url, &stream).await?;
    assert_eq!(count, 20, "all large batch events should be delivered");

    info!("✓ large batch sent successfully");
    cleanup_test_stream(&url, &stream).await?;
    Ok(())
}

// =============================================================================
// Concurrency Tests
// =============================================================================

/// Test concurrent sends from multiple tasks.
#[tokio::test]
#[ignore = "requires docker"]
async fn nats_sink_concurrent_sends() -> Result<()> {
    let (url, subject, stream) = setup("concurrent").await?;

    let sink = Arc::new(NatsSink::new(
        &NatsSinkCfg {
            id: "test-concurrent".into(),
            url: url.clone(),
            subject: subject.clone(),
            key: None,
            envelope: EnvelopeCfg::Native,
            encoding: EncodingCfg::Json,
            stream: Some(stream.clone()),
            required: Some(true),
            send_timeout_secs: Some(10),
            batch_timeout_secs: Some(30),
            connect_timeout_secs: Some(10),
            credentials_file: None,
            username: None,
            password: None,
            token: None,
            filter: None,
        },
        CancellationToken::new(),
    )?);

    // Spawn multiple concurrent send tasks
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

    assert_eq!(
        success_count, 50,
        "all concurrent events should be delivered"
    );

    // Verify by checking stream
    let count = stream_message_count(&url, &stream).await?;
    assert_eq!(count, 50, "all 50 messages should be in stream");

    info!("✓ concurrent sends handled correctly");
    cleanup_test_stream(&url, &stream).await?;
    Ok(())
}

/// Test multiple batches sent concurrently.
#[tokio::test]
#[ignore = "requires docker"]
async fn nats_sink_concurrent_batches() -> Result<()> {
    let (url, subject, stream) = setup("concurrent-batch").await?;

    let sink = Arc::new(NatsSink::new(
        &NatsSinkCfg {
            id: "test-concurrent-batch".into(),
            url: url.clone(),
            subject: subject.clone(),
            key: None,
            envelope: EnvelopeCfg::Native,
            encoding: EncodingCfg::Json,
            stream: Some(stream.clone()),
            required: Some(true),
            send_timeout_secs: Some(10),
            batch_timeout_secs: Some(60),
            connect_timeout_secs: Some(10),
            credentials_file: None,
            username: None,
            password: None,
            token: None,
            filter: None,
        },
        CancellationToken::new(),
    )?);

    // Spawn multiple concurrent batch send tasks
    let mut handles = Vec::new();
    for batch_num in 0..5 {
        let sink = sink.clone();
        handles.push(tokio::spawn(async move {
            let events: Vec<Event> = (0..20)
                .map(|i| make_test_event(batch_num * 100 + i))
                .collect();
            sink.send_batch(&events).await
        }));
    }

    // Wait for all batches to complete
    for handle in handles {
        let result = handle.await?;
        assert!(result.is_ok(), "concurrent batch send should succeed");
    }

    // Verify all events arrived (5 batches x 20 events = 100)
    let count = stream_message_count(&url, &stream).await?;
    assert_eq!(
        count, 100,
        "all 100 events from concurrent batches should be in stream"
    );

    info!("✓ concurrent batches handled correctly");
    cleanup_test_stream(&url, &stream).await?;
    Ok(())
}

// =============================================================================
// Connection and Resilience Tests
// =============================================================================

/// Test that sink recovers after NATS restarts.
#[tokio::test]
#[ignore = "requires docker"]
async fn nats_sink_recovers_after_restart() -> Result<()> {
    init_test_tracing();

    // Start a dedicated container for this test (not shared)
    let restart_port: u16 = 4323;
    let image = GenericImage::new("nats", "2.10-alpine")
        .with_wait_for(WaitFor::Duration {
            length: Duration::from_secs(5),
        })
        .with_cmd(vec!["-js"])
        .with_mapped_port(restart_port, 4222.tcp());

    let container = image.start().await?;
    let url = format!("nats://127.0.0.1:{}", restart_port);

    wait_for_nats(&url, Duration::from_secs(30)).await?;

    let subject = "df.test.restart";
    let stream = "DF_TEST_RESTART";
    setup_test_stream(&url, stream, subject).await?;

    let sink = Arc::new(make_sink(
        "test-restart",
        &url,
        subject,
        Some(stream),
        EnvelopeCfg::Native,
    )?);

    // Send first event successfully
    let event1 = make_test_event(1);
    sink.send(&event1).await?;
    info!("✓ first event sent before restart");

    // Stop the container (simulates NATS going down)
    info!("stopping NATS container...");
    container.stop().await?;
    sleep(Duration::from_secs(1)).await;

    // Start sending in background - should retry until NATS comes back
    let sink_clone = sink.clone();
    let send_handle = tokio::spawn(async move {
        let event2 = make_test_event(2);
        sink_clone.send(&event2).await
    });

    // Wait a bit for retries to start
    sleep(Duration::from_secs(2)).await;

    // Restart the container
    info!("restarting NATS container...");
    container.start().await?;
    wait_for_nats(&url, Duration::from_secs(30)).await?;
    info!("NATS is back up");

    // The send should eventually succeed
    let result = timeout(Duration::from_secs(30), send_handle).await??;
    assert!(
        result.is_ok(),
        "send should succeed after NATS recovers: {:?}",
        result.err()
    );

    info!("✓ sink recovered after NATS restart");
    Ok(())
}

/// Test connection reuse across multiple sends.
#[tokio::test]
#[ignore = "requires docker"]
async fn nats_sink_connection_reuse() -> Result<()> {
    let (url, subject, stream) = setup("conn-reuse").await?;

    let sink = make_sink(
        "test-conn-reuse",
        &url,
        &subject,
        Some(&stream),
        EnvelopeCfg::Native,
    )?;

    // Send multiple events - should reuse connection
    for i in 0..10 {
        let event = make_test_event(i);
        sink.send(&event).await?;
    }

    let count = stream_message_count(&url, &stream).await?;
    assert_eq!(count, 10, "all 10 events should be in stream");

    info!("✓ connection reused across sends");
    cleanup_test_stream(&url, &stream).await?;
    Ok(())
}

/// Test connection failure with invalid URL fails appropriately.
#[tokio::test]
#[ignore = "requires docker"]
async fn nats_sink_connection_failure_invalid_url() -> Result<()> {
    init_test_tracing();

    let sink = make_sink(
        "test-invalid",
        "nats://127.0.0.1:59999",
        "df.test.invalid",
        None,
        EnvelopeCfg::Native,
    )?;

    let event = make_test_event(1);
    let result = sink.send(&event).await;

    assert!(result.is_err(), "send should fail with invalid URL");
    info!("✓ connection failure handled correctly: {:?}", result.err());
    Ok(())
}

/// Test that sink handles rapid sends without issues.
#[tokio::test]
#[ignore = "requires docker"]
async fn nats_sink_handles_rapid_sends() -> Result<()> {
    let (url, subject, stream) = setup("rapid").await?;

    let sink = Arc::new(NatsSink::new(
        &NatsSinkCfg {
            id: "test-rapid".into(),
            url: url.clone(),
            subject: subject.clone(),
            key: None,
            envelope: EnvelopeCfg::Native,
            encoding: EncodingCfg::Json,
            stream: Some(stream.clone()),
            required: Some(true),
            send_timeout_secs: Some(30),
            batch_timeout_secs: Some(60),
            connect_timeout_secs: Some(10),
            credentials_file: None,
            username: None,
            password: None,
            token: None,
            filter: None,
        },
        CancellationToken::new(),
    )?);

    // Send multiple events rapidly
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

    // Verify by checking stream
    let count = stream_message_count(&url, &stream).await?;
    assert_eq!(count, 50, "all 50 messages should be in stream");

    info!("✓ handled rapid sends without issues");
    cleanup_test_stream(&url, &stream).await?;
    Ok(())
}

/// Test batch delivery with multiple sequential batches.
#[tokio::test]
#[ignore = "requires docker"]
async fn nats_sink_batch_resilience() -> Result<()> {
    let (url, subject, stream) = setup("batch-resilience").await?;

    let sink = NatsSink::new(
        &NatsSinkCfg {
            id: "test-batch-resilience".into(),
            url: url.clone(),
            subject: subject.clone(),
            key: None,
            envelope: EnvelopeCfg::Native,
            encoding: EncodingCfg::Json,
            stream: Some(stream.clone()),
            required: Some(true),
            send_timeout_secs: Some(30),
            batch_timeout_secs: Some(60),
            connect_timeout_secs: Some(10),
            credentials_file: None,
            username: None,
            password: None,
            token: None,
            filter: None,
        },
        CancellationToken::new(),
    )?;

    // Send multiple batches in sequence
    for batch_num in 0..5 {
        let events: Vec<Event> = (0..20)
            .map(|i| make_test_event(batch_num * 100 + i))
            .collect();

        sink.send_batch(&events).await?;
        debug!("batch {} delivered", batch_num);
    }

    // Verify all messages
    let count = stream_message_count(&url, &stream).await?;
    assert_eq!(count, 100, "all 100 messages should be delivered");

    info!("✓ batch delivery is resilient");
    cleanup_test_stream(&url, &stream).await?;
    Ok(())
}

// =============================================================================
// Cancellation Tests
// =============================================================================

/// Test that cancellation is propagated correctly.
#[tokio::test]
#[ignore = "requires docker"]
async fn nats_sink_respects_cancellation() -> Result<()> {
    init_test_tracing();
    get_nats_container().await;

    // Use an invalid URL so retry loop keeps trying
    let cancel = CancellationToken::new();
    let sink = Arc::new(NatsSink::new(
        &NatsSinkCfg {
            id: "test-cancel".into(),
            url: "nats://invalid-host:9999".into(),
            subject: "df.test.cancel".into(),
            key: None,
            envelope: EnvelopeCfg::Native,
            encoding: EncodingCfg::Json,
            stream: None,
            required: Some(true),
            send_timeout_secs: Some(2),
            batch_timeout_secs: Some(5),
            connect_timeout_secs: Some(2),
            credentials_file: None,
            username: None,
            password: None,
            token: None,
            filter: None,
        },
        cancel.clone(),
    )?);

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
async fn nats_sink_default_envelope_is_native() -> Result<()> {
    let (url, subject, stream) = setup("default-envelope").await?;

    // Verify defaults
    assert_eq!(EnvelopeCfg::default(), EnvelopeCfg::Native);
    assert_eq!(EncodingCfg::default(), EncodingCfg::Json);

    let sink = make_sink(
        "test-default",
        &url,
        &subject,
        Some(&stream),
        EnvelopeCfg::Native,
    )?;

    let event = make_test_event(1);
    sink.send(&event).await?;

    let messages = read_stream_messages(&url, &stream, 1).await?;
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
    cleanup_test_stream(&url, &stream).await?;
    Ok(())
}

// =============================================================================
// Trait Implementation Tests
// =============================================================================

/// Test Sink trait methods.
#[tokio::test]
#[ignore = "requires docker"]
async fn nats_sink_trait_implementation() -> Result<()> {
    init_test_tracing();
    get_nats_container().await;

    let url = nats_url();
    let sink = make_sink(
        "test-trait",
        &url,
        "df.test.trait",
        None,
        EnvelopeCfg::Native,
    )?;

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
async fn nats_sink_optional() -> Result<()> {
    init_test_tracing();
    get_nats_container().await;

    let sink = NatsSink::new(
        &NatsSinkCfg {
            id: "test-optional".into(),
            url: nats_url(),
            subject: "df.test.optional".into(),
            key: None,
            envelope: EnvelopeCfg::Native,
            encoding: EncodingCfg::Json,
            stream: None,
            required: Some(false),
            send_timeout_secs: None,
            batch_timeout_secs: None,
            connect_timeout_secs: None,
            credentials_file: None,
            username: None,
            password: None,
            token: None,
            filter: None,
        },
        CancellationToken::new(),
    )?;

    assert!(!sink.required(), "sink should be optional");

    info!("✓ optional sink configuration works");
    Ok(())
}

// =============================================================================
// JetStream Specific Tests
// =============================================================================

/// Test that sink works without explicit stream configuration.
#[tokio::test]
#[ignore = "requires docker"]
async fn nats_sink_without_stream_config() -> Result<()> {
    let (url, subject, stream) = setup("no-stream-cfg").await?;
    // Stream created by setup(); sink intentionally unaware of stream name

    let sink =
        make_sink("test-no-stream", &url, &subject, None, EnvelopeCfg::Native)?;

    let event = make_test_event(1);
    sink.send(&event).await?;

    // Message should still be in stream (matched by subject)
    let count = stream_message_count(&url, &stream).await?;
    assert_eq!(count, 1, "event should be in stream");

    info!("✓ sink works without explicit stream configuration");
    cleanup_test_stream(&url, &stream).await?;
    Ok(())
}

/// Test streaming to a subject with wildcard pattern.
#[tokio::test]
#[ignore = "requires docker"]
async fn nats_sink_wildcard_subject_stream() -> Result<()> {
    init_test_tracing();
    get_nats_container().await;

    let url = nats_url();
    let stream = test_stream("wildcard");

    // Create stream that captures all df.test.* subjects
    let client = async_nats::connect(&url).await?;
    let js = jetstream::new(client);
    let _ = js.delete_stream(&stream).await;
    js.create_stream(StreamConfig {
        name: stream.clone(),
        subjects: vec!["df.test.wildcard.*".to_string()],
        ..Default::default()
    })
    .await?;

    // Send to specific subject under wildcard
    let specific_subject = "df.test.wildcard.events";
    let sink = make_sink(
        "test-wildcard",
        &url,
        specific_subject,
        Some(&stream),
        EnvelopeCfg::Native,
    )?;

    let event = make_test_event(1);
    sink.send(&event).await?;

    let count = stream_message_count(&url, &stream).await?;
    assert_eq!(count, 1, "event should match wildcard stream");

    info!("✓ wildcard subject stream works correctly");
    cleanup_test_stream(&url, &stream).await?;
    Ok(())
}

// =============================================================================
// Dynamic Routing Tests
// =============================================================================

struct ConsumedNatsMessage {
    subject: String,
    #[allow(dead_code)]
    payload: Vec<u8>,
    headers: HashMap<String, String>,
}

/// Read messages from a NATS stream with subject and header metadata.
async fn read_stream_messages_with_metadata(
    url: &str,
    stream_name: &str,
    count: usize,
) -> Result<Vec<ConsumedNatsMessage>> {
    let client = async_nats::connect(url).await?;
    let js = jetstream::new(client);
    let stream = js.get_stream(stream_name).await?;

    let consumer = stream
        .create_consumer(async_nats::jetstream::consumer::pull::Config {
            durable_name: Some(format!("test-meta-consumer-{}", stream_name)),
            ..Default::default()
        })
        .await?;

    let batch = consumer.fetch().max_messages(count).messages().await?;
    use futures::StreamExt;
    let mut batch = std::pin::pin!(batch);

    let mut messages = Vec::new();
    while let Some(msg) = batch.next().await {
        if let Ok(msg) = msg {
            let mut headers = HashMap::new();
            if let Some(ref hdrs) = msg.headers {
                for (key, values) in hdrs.iter() {
                    if let Some(val) = values.first() {
                        headers.insert(key.to_string(), val.to_string());
                    }
                }
            }
            messages.push(ConsumedNatsMessage {
                subject: msg.subject.to_string(),
                payload: msg.payload.to_vec(),
                headers,
            });
            if messages.len() >= count {
                break;
            }
        }
    }

    Ok(messages)
}

/// Subject template routes events to per-table NATS subjects.
#[tokio::test]
#[ignore = "requires docker"]
async fn nats_sink_subject_template_routes_by_table() -> Result<()> {
    init_test_tracing();
    get_nats_container().await;

    let url = nats_url();
    let stream = test_stream("routing");

    let client = async_nats::connect(&url).await?;
    let js = jetstream::new(client);
    let _ = js.delete_stream(&stream).await;
    js.create_stream(StreamConfig {
        name: stream.clone(),
        subjects: vec!["df.test.routing.>".to_string()],
        ..Default::default()
    })
    .await?;

    let sink = make_sink(
        "test-routing",
        &url,
        "df.test.routing.${source.table}",
        Some(&stream),
        EnvelopeCfg::Native,
    )?;

    sink.send_batch(&[
        make_event_for_table(1, "orders"),
        make_event_for_table(2, "users"),
    ])
    .await?;

    let msgs = read_stream_messages_with_metadata(&url, &stream, 2).await?;
    assert_eq!(msgs.len(), 2);

    let subjects: Vec<&str> = msgs.iter().map(|m| m.subject.as_str()).collect();
    assert!(
        subjects.contains(&"df.test.routing.orders"),
        "should have orders subject, got: {:?}",
        subjects
    );
    assert!(
        subjects.contains(&"df.test.routing.users"),
        "should have users subject, got: {:?}",
        subjects
    );

    info!("✓ subject template routes events to correct subjects");
    cleanup_test_stream(&url, &stream).await?;
    Ok(())
}

/// EventRouting overrides subject + headers delivered as NATS headers.
#[tokio::test]
#[ignore = "requires docker"]
async fn nats_sink_routing_override_with_headers() -> Result<()> {
    init_test_tracing();
    get_nats_container().await;

    let url = nats_url();
    let stream = test_stream("route-override");

    let client = async_nats::connect(&url).await?;
    let js = jetstream::new(client);
    let _ = js.delete_stream(&stream).await;
    js.create_stream(StreamConfig {
        name: stream.clone(),
        subjects: vec!["df.test.route-override.>".to_string()],
        ..Default::default()
    })
    .await?;

    let sink = make_sink(
        "test-override",
        &url,
        "df.test.route-override.default",
        Some(&stream),
        EnvelopeCfg::Native,
    )?;

    let mut event = make_test_event(1);
    event.routing = Some(EventRouting {
        topic: Some("df.test.route-override.priority".into()),
        key: Some("order-99".into()),
        headers: Some(HashMap::from([("trace-id".into(), "xyz-789".into())])),
        raw_payload: false,
    });

    sink.send(&event).await?;

    let msgs = read_stream_messages_with_metadata(&url, &stream, 1).await?;
    assert_eq!(msgs.len(), 1);

    let msg = &msgs[0];
    assert_eq!(msg.subject, "df.test.route-override.priority");
    assert_eq!(
        msg.headers.get("df-key").map(|s| s.as_str()),
        Some("order-99"),
    );
    assert_eq!(
        msg.headers.get("trace-id").map(|s| s.as_str()),
        Some("xyz-789"),
    );

    info!("✓ EventRouting overrides subject + headers delivered");
    cleanup_test_stream(&url, &stream).await?;
    Ok(())
}

// =============================================================================
// Raw Payload Tests (Outbox)
// =============================================================================

/// When routing.raw_payload is true, the sink should serialize event.after
/// directly, bypassing the configured envelope format.
#[tokio::test]
#[ignore = "requires docker"]
async fn nats_sink_raw_payload_bypasses_envelope() -> Result<()> {
    let (url, subject, stream) = setup("raw-payload").await?;

    // Deliberately use Debezium envelope - raw_payload should bypass it
    let sink = make_sink(
        "test-raw",
        &url,
        &subject,
        Some(&stream),
        EnvelopeCfg::Debezium,
    )?;

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

    let messages = read_stream_messages(&url, &stream, 2).await?;
    assert_eq!(messages.len(), 2, "should have 2 messages");

    let payloads: Vec<serde_json::Value> = messages
        .iter()
        .map(|m| serde_json::from_slice(m).unwrap())
        .collect();

    // One should be raw payload, other should be Debezium envelope
    let raw_payload = payloads
        .iter()
        .find(|v| v.get("order_id").is_some())
        .expect("should have raw payload message");
    let envelope_payload = payloads
        .iter()
        .find(|v| v.get("payload").is_some())
        .expect("should have Debezium-wrapped message");

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

    // Verify headers still delivered on raw message via NATS headers
    let msgs_meta =
        read_stream_messages_with_metadata(&url, &stream, 2).await?;
    let raw_meta = msgs_meta
        .iter()
        .find(|m| {
            let v: serde_json::Value =
                serde_json::from_slice(&m.payload).unwrap();
            v.get("order_id").is_some()
        })
        .expect("should find raw message metadata");
    assert_eq!(
        raw_meta
            .headers
            .get("df-aggregate-type")
            .map(|s| s.as_str()),
        Some("Order"),
        "routing headers should still be delivered as NATS headers"
    );

    info!("✓ raw_payload bypasses envelope on NATS");
    cleanup_test_stream(&url, &stream).await?;
    Ok(())
}
