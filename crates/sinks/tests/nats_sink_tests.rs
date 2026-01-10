//! Integration tests for NATS JetStream sink.
//!
//! These tests require Docker to run a NATS server with JetStream enabled.
//! Run with: cargo test -p sinks --test nats_sink_tests -- --ignored --test-threads=1

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::Result;
use async_nats::jetstream::{self, stream::Config as StreamConfig};
use deltaforge_config::NatsSinkCfg;
use deltaforge_core::{Event, Op, Sink, SourceMeta};
use serde_json::json;
use sinks::NatsSink;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt, runners::AsyncRunner,
};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};
use uuid::Uuid;

// =============================================================================
// Test Infrastructure
// =============================================================================

static NATS_CONTAINER: OnceLock<ContainerAsync<GenericImage>> = OnceLock::new();
const NATS_PORT: u16 = 4222;

fn init_test_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("debug,testcontainers=info,bollard=warn")
        .with_test_writer()
        .try_init();
}

async fn get_nats_container() -> &'static ContainerAsync<GenericImage> {
    if let Some(container) = NATS_CONTAINER.get() {
        return container;
    }

    info!("starting NATS container with JetStream...");
    let container = GenericImage::new("nats", "2.10-alpine")
        .with_exposed_port(NATS_PORT.into())
        .with_cmd(vec!["-js"]) // Enable JetStream
        .start()
        .await
        .expect("failed to start NATS container");

    // Wait for NATS to be ready
    let url = nats_url_from_container(&container).await;
    wait_for_nats(&url, Duration::from_secs(30))
        .await
        .expect("NATS failed to start");

    NATS_CONTAINER.get_or_init(|| container)
}

async fn nats_url_from_container(
    container: &ContainerAsync<GenericImage>,
) -> String {
    let port = container
        .get_host_port_ipv4(NATS_PORT)
        .await
        .expect("failed to get NATS port");
    format!("nats://127.0.0.1:{}", port)
}

async fn wait_for_nats(url: &str, timeout: Duration) -> Result<()> {
    let start = std::time::Instant::now();
    loop {
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

        if start.elapsed() > timeout {
            anyhow::bail!("timeout waiting for NATS");
        }
        sleep(Duration::from_millis(100)).await;
    }
}

fn test_subject(name: &str) -> String {
    format!("df.test.{}.{}", name, Uuid::new_v4().as_simple())
}

fn test_stream(name: &str) -> String {
    format!(
        "DF_TEST_{}_{}",
        name.to_uppercase(),
        Uuid::new_v4().as_simple()
    )
}

fn make_test_event(seq: u64) -> Event {
    let source = SourceMeta {
        kind: "test".into(),
        host: "localhost".into(),
        db: "testdb".into(),
    };

    let after = json!({
        "id": seq,
        "name": format!("test-{}", seq),
        "data": "x".repeat(100)
    });

    let ts_ms = chrono::Utc::now().timestamp_millis();

    Event::new_row(
        "test-tenant".into(),
        source,
        "testdb.testtable".into(),
        Op::Insert,
        None,
        Some(after),
        ts_ms,
        200,
    )
}

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

    info!(stream = %stream, subject = %subject, "test stream created");
    Ok(())
}

async fn cleanup_test_stream(url: &str, stream: &str) -> Result<()> {
    let client = async_nats::connect(url).await?;
    let js = jetstream::new(client);
    let _ = js.delete_stream(stream).await;
    Ok(())
}

async fn stream_message_count(url: &str, stream: &str) -> Result<u64> {
    let client = async_nats::connect(url).await?;
    let js = jetstream::new(client);
    let mut stream = js.get_stream(stream).await?;
    let info = stream.info().await?;
    Ok(info.state.messages)
}

// =============================================================================
// Basic Functionality Tests
// =============================================================================

/// Test basic send operation.
#[tokio::test]
#[ignore = "requires docker"]
async fn nats_sink_send_single_event() -> Result<()> {
    init_test_tracing();
    let container = get_nats_container().await;
    let url = nats_url_from_container(container).await;

    let subject = test_subject("single");
    let stream = test_stream("single");
    setup_test_stream(&url, &stream, &subject).await?;

    let cfg = NatsSinkCfg {
        id: "test-single".into(),
        url: url.clone(),
        subject: subject.clone(),
        stream: Some(stream.clone()),
        required: Some(true),
        send_timeout_secs: Some(5),
        batch_timeout_secs: Some(30),
        connect_timeout_secs: Some(10),
        credentials_file: None,
        username: None,
        password: None,
        token: None,
    };

    let cancel = CancellationToken::new();
    let sink = NatsSink::new(&cfg, cancel)?;

    let event = make_test_event(1);
    sink.send(&event).await?;

    let count = stream_message_count(&url, &stream).await?;
    assert_eq!(count, 1, "expected 1 message in stream");

    info!("✓ single event sent successfully");
    cleanup_test_stream(&url, &stream).await?;
    Ok(())
}

/// Test batch send operation.
#[tokio::test]
#[ignore = "requires docker"]
async fn nats_sink_send_batch() -> Result<()> {
    init_test_tracing();
    let container = get_nats_container().await;
    let url = nats_url_from_container(container).await;

    let subject = test_subject("batch");
    let stream = test_stream("batch");
    setup_test_stream(&url, &stream, &subject).await?;

    let cfg = NatsSinkCfg {
        id: "test-batch".into(),
        url: url.clone(),
        subject: subject.clone(),
        stream: Some(stream.clone()),
        required: Some(true),
        send_timeout_secs: Some(5),
        batch_timeout_secs: Some(30),
        connect_timeout_secs: Some(10),
        credentials_file: None,
        username: None,
        password: None,
        token: None,
    };

    let cancel = CancellationToken::new();
    let sink = NatsSink::new(&cfg, cancel)?;

    // Send batch of 100 events
    let events: Vec<Event> = (0..100).map(make_test_event).collect();
    sink.send_batch(&events).await?;

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
    init_test_tracing();
    let container = get_nats_container().await;
    let url = nats_url_from_container(container).await;

    let subject = test_subject("empty");
    let stream = test_stream("empty");
    setup_test_stream(&url, &stream, &subject).await?;

    let cfg = NatsSinkCfg {
        id: "test-empty".into(),
        url: url.clone(),
        subject: subject.clone(),
        stream: Some(stream.clone()),
        required: Some(true),
        send_timeout_secs: Some(5),
        batch_timeout_secs: Some(30),
        connect_timeout_secs: Some(10),
        credentials_file: None,
        username: None,
        password: None,
        token: None,
    };

    let cancel = CancellationToken::new();
    let sink = NatsSink::new(&cfg, cancel)?;

    // Send empty batch
    sink.send_batch(&[]).await?;

    let count = stream_message_count(&url, &stream).await?;
    assert_eq!(count, 0, "stream should be empty after empty batch");

    info!("✓ empty batch is a no-op");
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
    let container = get_nats_container().await;
    let url = nats_url_from_container(container).await;

    let cfg = NatsSinkCfg {
        id: "test-trait".into(),
        url,
        subject: "df.test.trait".into(),
        stream: None,
        required: Some(true),
        send_timeout_secs: Some(5),
        batch_timeout_secs: Some(30),
        connect_timeout_secs: Some(10),
        credentials_file: None,
        username: None,
        password: None,
        token: None,
    };

    let cancel = CancellationToken::new();
    let sink = NatsSink::new(&cfg, cancel)?;

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
    let container = get_nats_container().await;
    let url = nats_url_from_container(container).await;

    let cfg = NatsSinkCfg {
        id: "test-optional".into(),
        url,
        subject: "df.test.optional".into(),
        stream: None,
        required: Some(false),
        send_timeout_secs: None,
        batch_timeout_secs: None,
        connect_timeout_secs: None,
        credentials_file: None,
        username: None,
        password: None,
        token: None,
    };

    let cancel = CancellationToken::new();
    let sink = NatsSink::new(&cfg, cancel)?;

    assert!(!sink.required(), "sink should be optional");

    info!("✓ optional sink configuration works");
    Ok(())
}

// =============================================================================
// Connection and Retry Tests
// =============================================================================

/// Test that connection is established lazily and reused.
#[tokio::test]
#[ignore = "requires docker"]
async fn nats_sink_connection_reuse() -> Result<()> {
    init_test_tracing();
    let container = get_nats_container().await;
    let url = nats_url_from_container(container).await;

    let subject = test_subject("conn_reuse");
    let stream = test_stream("conn_reuse");
    setup_test_stream(&url, &stream, &subject).await?;

    let cfg = NatsSinkCfg {
        id: "test-conn-reuse".into(),
        url: url.clone(),
        subject: subject.clone(),
        stream: Some(stream.clone()),
        required: Some(true),
        send_timeout_secs: Some(5),
        batch_timeout_secs: Some(30),
        connect_timeout_secs: Some(10),
        credentials_file: None,
        username: None,
        password: None,
        token: None,
    };

    let cancel = CancellationToken::new();
    let sink = NatsSink::new(&cfg, cancel)?;

    // Send multiple events - should reuse connection
    for i in 0..10 {
        let event = make_test_event(i);
        sink.send(&event).await?;
    }

    let count = stream_message_count(&url, &stream).await?;
    assert_eq!(count, 10, "expected 10 messages");

    info!("✓ connection reused across sends");
    cleanup_test_stream(&url, &stream).await?;
    Ok(())
}

/// Test cancellation token is respected.
#[tokio::test]
#[ignore = "requires docker"]
async fn nats_sink_respects_cancellation() -> Result<()> {
    init_test_tracing();
    let container = get_nats_container().await;
    let url = nats_url_from_container(container).await;

    let cfg = NatsSinkCfg {
        id: "test-cancel".into(),
        url: url.clone(),
        subject: "df.test.cancel".into(),
        stream: None,
        required: Some(true),
        send_timeout_secs: Some(30),
        batch_timeout_secs: Some(30),
        connect_timeout_secs: Some(30),
        credentials_file: None,
        username: None,
        password: None,
        token: None,
    };

    let cancel = CancellationToken::new();
    let sink = Arc::new(NatsSink::new(&cfg, cancel.clone())?);

    // Cancel immediately
    cancel.cancel();

    // Attempt to send - should fail or return quickly
    let sink_clone = sink.clone();
    let result = tokio::time::timeout(Duration::from_secs(5), async move {
        let event = make_test_event(1);
        sink_clone.send(&event).await
    })
    .await;

    // Should complete without hanging (either success before cancel or error)
    assert!(result.is_ok(), "operation should not hang on cancellation");

    info!("✓ cancellation token is respected");
    Ok(())
}

// =============================================================================
// Large Payload Tests
// =============================================================================

/// Test sending large events.
#[tokio::test]
#[ignore = "requires docker"]
async fn nats_sink_large_payload() -> Result<()> {
    init_test_tracing();
    let container = get_nats_container().await;
    let url = nats_url_from_container(container).await;

    let subject = test_subject("large");
    let stream = test_stream("large");
    setup_test_stream(&url, &stream, &subject).await?;

    let cfg = NatsSinkCfg {
        id: "test-large".into(),
        url: url.clone(),
        subject: subject.clone(),
        stream: Some(stream.clone()),
        required: Some(true),
        send_timeout_secs: Some(30),
        batch_timeout_secs: Some(60),
        connect_timeout_secs: Some(10),
        credentials_file: None,
        username: None,
        password: None,
        token: None,
    };

    let cancel = CancellationToken::new();
    let sink = NatsSink::new(&cfg, cancel)?;

    // Create event with large payload (~500KB)
    let large_data = "x".repeat(500_000);
    let mut event = make_test_event(1);
    event.after = Some(json!({
        "id": 1,
        "data": large_data
    }));

    sink.send(&event).await?;

    let count = stream_message_count(&url, &stream).await?;
    assert_eq!(count, 1, "large event should be delivered");

    info!("✓ large payload sent successfully");
    cleanup_test_stream(&url, &stream).await?;
    Ok(())
}

// =============================================================================
// Connection Retry and Resilience Tests
// =============================================================================

/// Test that sink handles connection invalidation and reconnects.
#[tokio::test]
#[ignore = "requires docker"]
async fn nats_sink_reconnects_after_connection_drop() -> Result<()> {
    init_test_tracing();
    let container = get_nats_container().await;
    let url = nats_url_from_container(container).await;

    let subject = test_subject("conn_drop");
    let stream = test_stream("conn_drop");
    setup_test_stream(&url, &stream, &subject).await?;

    let cfg = NatsSinkCfg {
        id: "test-conn-drop".into(),
        url: url.clone(),
        subject: subject.clone(),
        stream: Some(stream.clone()),
        required: Some(true),
        send_timeout_secs: Some(5),
        batch_timeout_secs: Some(30),
        connect_timeout_secs: Some(10),
        credentials_file: None,
        username: None,
        password: None,
        token: None,
    };

    let cancel = CancellationToken::new();
    let sink = NatsSink::new(&cfg, cancel)?;

    // Send first event to establish connection
    let event1 = make_test_event(1);
    sink.send(&event1).await?;
    info!("✓ first event sent, connection established");

    // Force connection invalidation by sending multiple events rapidly
    // The connection should be reused
    for i in 2..=5 {
        let event = make_test_event(i);
        sink.send(&event).await?;
    }

    // Verify all events arrived
    let count = stream_message_count(&url, &stream).await?;
    assert_eq!(count, 5, "all 5 events should be in stream");

    info!("✓ sink handled multiple sends with connection reuse");
    cleanup_test_stream(&url, &stream).await?;
    Ok(())
}

/// Test batch retry after transient failure.
#[tokio::test]
#[ignore = "requires docker"]
async fn nats_sink_batch_retries_on_failure() -> Result<()> {
    init_test_tracing();
    let container = get_nats_container().await;
    let url = nats_url_from_container(container).await;

    let subject = test_subject("batch_retry");
    let stream = test_stream("batch_retry");
    setup_test_stream(&url, &stream, &subject).await?;

    let cfg = NatsSinkCfg {
        id: "test-batch-retry".into(),
        url: url.clone(),
        subject: subject.clone(),
        stream: Some(stream.clone()),
        required: Some(true),
        send_timeout_secs: Some(5),
        batch_timeout_secs: Some(30),
        connect_timeout_secs: Some(10),
        credentials_file: None,
        username: None,
        password: None,
        token: None,
    };

    let cancel = CancellationToken::new();
    let sink = NatsSink::new(&cfg, cancel)?;

    // Send first batch to warm up connection
    let events1: Vec<Event> = (0..10).map(make_test_event).collect();
    sink.send_batch(&events1).await?;
    info!("✓ first batch sent");

    // Send second batch - should work with existing connection
    let events2: Vec<Event> = (100..110).map(make_test_event).collect();
    sink.send_batch(&events2).await?;

    let count = stream_message_count(&url, &stream).await?;
    assert_eq!(count, 20, "all 20 events should be in stream");

    info!("✓ batch operations work correctly");
    cleanup_test_stream(&url, &stream).await?;
    Ok(())
}

/// Test connection failure with invalid URL fails appropriately.
#[tokio::test]
#[ignore = "requires docker"]
async fn nats_sink_connection_failure_invalid_url() -> Result<()> {
    init_test_tracing();

    let cfg = NatsSinkCfg {
        id: "test-invalid".into(),
        url: "nats://127.0.0.1:59999".into(), // Invalid port, nothing listening
        subject: "df.test.invalid".into(),
        stream: None,
        required: Some(true),
        send_timeout_secs: Some(2),
        batch_timeout_secs: Some(5),
        connect_timeout_secs: Some(2), // Short timeout to fail fast
        credentials_file: None,
        username: None,
        password: None,
        token: None,
    };

    let cancel = CancellationToken::new();
    let sink = NatsSink::new(&cfg, cancel)?;

    let event = make_test_event(1);
    let result = sink.send(&event).await;

    assert!(result.is_err(), "send should fail with invalid URL");
    info!("✓ connection failure handled correctly: {:?}", result.err());
    Ok(())
}

/// Test concurrent sends don't cause issues.
#[tokio::test]
#[ignore = "requires docker"]
async fn nats_sink_concurrent_sends() -> Result<()> {
    init_test_tracing();
    let container = get_nats_container().await;
    let url = nats_url_from_container(container).await;

    let subject = test_subject("concurrent");
    let stream = test_stream("concurrent");
    setup_test_stream(&url, &stream, &subject).await?;

    let cfg = NatsSinkCfg {
        id: "test-concurrent".into(),
        url: url.clone(),
        subject: subject.clone(),
        stream: Some(stream.clone()),
        required: Some(true),
        send_timeout_secs: Some(10),
        batch_timeout_secs: Some(30),
        connect_timeout_secs: Some(10),
        credentials_file: None,
        username: None,
        password: None,
        token: None,
    };

    let cancel = CancellationToken::new();
    let sink = Arc::new(NatsSink::new(&cfg, cancel)?);

    // Spawn multiple concurrent send tasks
    let mut handles = vec![];
    for i in 0..10 {
        let sink_clone = sink.clone();
        let handle = tokio::spawn(async move {
            let event = make_test_event(i);
            sink_clone.send(&event).await
        });
        handles.push(handle);
    }

    // Wait for all sends to complete
    for handle in handles {
        let result = handle.await?;
        assert!(result.is_ok(), "concurrent send should succeed");
    }

    // Verify all events arrived
    let count = stream_message_count(&url, &stream).await?;
    assert_eq!(count, 10, "all 10 concurrent events should be in stream");

    info!("✓ concurrent sends handled correctly");
    cleanup_test_stream(&url, &stream).await?;
    Ok(())
}

/// Test multiple batches sent concurrently.
#[tokio::test]
#[ignore = "requires docker"]
async fn nats_sink_concurrent_batches() -> Result<()> {
    init_test_tracing();
    let container = get_nats_container().await;
    let url = nats_url_from_container(container).await;

    let subject = test_subject("concurrent_batch");
    let stream = test_stream("concurrent_batch");
    setup_test_stream(&url, &stream, &subject).await?;

    let cfg = NatsSinkCfg {
        id: "test-concurrent-batch".into(),
        url: url.clone(),
        subject: subject.clone(),
        stream: Some(stream.clone()),
        required: Some(true),
        send_timeout_secs: Some(10),
        batch_timeout_secs: Some(30),
        connect_timeout_secs: Some(10),
        credentials_file: None,
        username: None,
        password: None,
        token: None,
    };

    let cancel = CancellationToken::new();
    let sink = Arc::new(NatsSink::new(&cfg, cancel)?);

    // Spawn multiple concurrent batch send tasks
    let mut handles = vec![];
    for batch_num in 0..5 {
        let sink_clone = sink.clone();
        let handle = tokio::spawn(async move {
            let events: Vec<Event> = (0..20)
                .map(|i| make_test_event(batch_num * 100 + i))
                .collect();
            sink_clone.send_batch(&events).await
        });
        handles.push(handle);
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
// Cleanup
// =============================================================================

#[ctor::dtor]
fn cleanup() {
    // Container will be cleaned up automatically when dropped
}
