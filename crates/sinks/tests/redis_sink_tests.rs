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
use deltaforge_config::RedisSinkCfg;
use deltaforge_core::{Event, Op, Sink, SourceMeta};
use redis::AsyncCommands;
use serde_json::json;
use sinks::redis::RedisSink;
use std::sync::Arc;
use std::time::Instant;
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
use sink_test_common::init_test_tracing;

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

/// Create a test event with a specific ID.
fn make_test_event(id: i64) -> Event {
    Event::new_row(
        "tenant".into(),
        SourceMeta {
            kind: "test".into(),
            host: "localhost".into(),
            db: "testdb".into(),
        },
        "test.table".into(),
        Op::Insert,
        None,
        Some(json!({"id": id, "name": format!("item-{}", id)})),
        1_700_000_000_000 + id,
        64,
    )
}

/// Create a test event with specific data size.
fn make_large_event(id: i64, size_bytes: usize) -> Event {
    let padding = "x".repeat(size_bytes);
    Event::new_row(
        "tenant".into(),
        SourceMeta {
            kind: "test".into(),
            host: "localhost".into(),
            db: "testdb".into(),
        },
        "test.table".into(),
        Op::Insert,
        None,
        Some(json!({"id": id, "payload": padding})),
        1_700_000_000_000 + id,
        64,
    )
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
    let parsed: Event = serde_json::from_str(payload)?;
    assert_eq!(parsed.event_id, event.event_id);

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
