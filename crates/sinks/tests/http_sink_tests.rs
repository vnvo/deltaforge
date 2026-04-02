//! Integration tests for HTTP sink.
//!
//! These tests spin up a local HTTP server (axum) and verify that the HTTP
//! sink delivers events correctly. No external dependencies required.
//!
//! Run with:
//! ```bash
//! cargo test -p sinks --test http_sink_tests -- --nocapture
//! ```

use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use anyhow::Result;
use axum::{Json, Router, extract::State, routing::post};
use deltaforge_config::{EncodingCfg, EnvelopeCfg, HttpSinkCfg};
use deltaforge_core::{Event, Sink};
use sinks::http::HttpSink;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

mod sink_test_common;
use sink_test_common::{
    init_test_tracing, make_event_for_table, make_test_event,
};

// =============================================================================
// Test HTTP Server
// =============================================================================

#[derive(Clone)]
struct TestServer {
    received: Arc<Mutex<Vec<serde_json::Value>>>,
    request_count: Arc<AtomicUsize>,
}

impl TestServer {
    fn new() -> Self {
        Self {
            received: Arc::new(Mutex::new(Vec::new())),
            request_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    async fn received_events(&self) -> Vec<serde_json::Value> {
        self.received.lock().await.clone()
    }

    fn request_count(&self) -> usize {
        self.request_count.load(Ordering::Relaxed)
    }
}

async fn handle_event(
    State(state): State<TestServer>,
    Json(body): Json<serde_json::Value>,
) -> axum::http::StatusCode {
    state.request_count.fetch_add(1, Ordering::Relaxed);
    let mut received = state.received.lock().await;
    if body.is_array() {
        // Batch mode: each element is an event
        if let Some(arr) = body.as_array() {
            for item in arr {
                received.push(item.clone());
            }
        }
    } else {
        received.push(body);
    }
    axum::http::StatusCode::OK
}

async fn handle_401(State(state): State<TestServer>) -> axum::http::StatusCode {
    state.request_count.fetch_add(1, Ordering::Relaxed);
    axum::http::StatusCode::UNAUTHORIZED
}

async fn handle_500(State(state): State<TestServer>) -> axum::http::StatusCode {
    state.request_count.fetch_add(1, Ordering::Relaxed);
    axum::http::StatusCode::INTERNAL_SERVER_ERROR
}

/// Start a test HTTP server on a random port. Returns (port, server_state).
async fn start_test_server(
    handler: Router,
) -> (u16, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = tokio::spawn(async move {
        axum::serve(listener, handler).await.unwrap();
    });
    // Give the server a moment to bind.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    (port, handle)
}

fn make_http_cfg(id: &str, url: &str) -> HttpSinkCfg {
    HttpSinkCfg {
        id: id.into(),
        url: url.into(),
        method: "POST".into(),
        headers: HashMap::new(),
        batch_mode: false,
        envelope: EnvelopeCfg::Native,
        encoding: EncodingCfg::Json,
        required: Some(true),
        send_timeout_secs: Some(5),
        batch_timeout_secs: Some(10),
        connect_timeout_secs: Some(2),
        filter: None,
    }
}

// =============================================================================
// Tests
// =============================================================================

#[tokio::test]
async fn http_sink_sends_single_event() -> Result<()> {
    init_test_tracing();

    let server = TestServer::new();
    let app = Router::new()
        .route("/events", post(handle_event))
        .with_state(server.clone());
    let (port, _handle) = start_test_server(app).await;

    let cfg =
        make_http_cfg("test-http", &format!("http://127.0.0.1:{port}/events"));
    let sink = HttpSink::new(&cfg, CancellationToken::new(), "test")?;

    let event = make_test_event(1);
    sink.send(&event).await?;

    let received = server.received_events().await;
    assert_eq!(received.len(), 1);
    assert_eq!(received[0]["after"]["id"], 1);
    assert_eq!(server.request_count(), 1);

    Ok(())
}

#[tokio::test]
async fn http_sink_sends_batch_per_event() -> Result<()> {
    init_test_tracing();

    let server = TestServer::new();
    let app = Router::new()
        .route("/events", post(handle_event))
        .with_state(server.clone());
    let (port, _handle) = start_test_server(app).await;

    let cfg =
        make_http_cfg("test-batch", &format!("http://127.0.0.1:{port}/events"));
    let sink = HttpSink::new(&cfg, CancellationToken::new(), "test")?;

    let events: Vec<Event> = (0..5).map(make_test_event).collect();
    let result = sink.send_batch(&events).await?;

    assert!(result.dlq_failures.is_empty());
    let received = server.received_events().await;
    assert_eq!(received.len(), 5);
    // 5 separate requests (batch_mode = false)
    assert_eq!(server.request_count(), 5);

    Ok(())
}

#[tokio::test]
async fn http_sink_batch_mode_sends_array() -> Result<()> {
    init_test_tracing();

    let server = TestServer::new();
    let app = Router::new()
        .route("/events", post(handle_event))
        .with_state(server.clone());
    let (port, _handle) = start_test_server(app).await;

    let mut cfg = make_http_cfg(
        "test-batch-mode",
        &format!("http://127.0.0.1:{port}/events"),
    );
    cfg.batch_mode = true;

    let sink = HttpSink::new(&cfg, CancellationToken::new(), "test")?;

    let events: Vec<Event> = (0..5).map(make_test_event).collect();
    let result = sink.send_batch(&events).await?;

    assert!(result.dlq_failures.is_empty());
    let received = server.received_events().await;
    assert_eq!(received.len(), 5);
    // 1 request with JSON array (batch_mode = true)
    assert_eq!(server.request_count(), 1);

    Ok(())
}

#[tokio::test]
async fn http_sink_auth_error_fails_immediately() -> Result<()> {
    init_test_tracing();

    let server = TestServer::new();
    let app = Router::new()
        .route("/events", post(handle_401))
        .with_state(server.clone());
    let (port, _handle) = start_test_server(app).await;

    let cfg =
        make_http_cfg("test-auth", &format!("http://127.0.0.1:{port}/events"));
    let sink = HttpSink::new(&cfg, CancellationToken::new(), "test")?;

    let event = make_test_event(1);
    let result = sink.send(&event).await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, deltaforge_core::SinkError::Auth { .. }),
        "expected Auth error, got: {:?}",
        err
    );
    // Should NOT retry — only 1 request
    assert_eq!(server.request_count(), 1);

    Ok(())
}

#[tokio::test]
async fn http_sink_5xx_retries() -> Result<()> {
    init_test_tracing();

    let server = TestServer::new();
    let app = Router::new()
        .route("/events", post(handle_500))
        .with_state(server.clone());
    let (port, _handle) = start_test_server(app).await;

    let cfg =
        make_http_cfg("test-retry", &format!("http://127.0.0.1:{port}/events"));
    let sink = HttpSink::new(&cfg, CancellationToken::new(), "test")?;

    let event = make_test_event(1);
    let result = sink.send(&event).await;

    assert!(result.is_err());
    // Should have retried multiple times (3 attempts configured)
    assert!(
        server.request_count() > 1,
        "expected retries, got {} requests",
        server.request_count()
    );

    Ok(())
}

#[tokio::test]
async fn http_sink_url_template_routing() -> Result<()> {
    init_test_tracing();

    let server = TestServer::new();
    let app = Router::new()
        .route("/events/{table}", post(handle_event))
        .with_state(server.clone());
    let (port, _handle) = start_test_server(app).await;

    let cfg = make_http_cfg(
        "test-routing",
        &format!("http://127.0.0.1:{port}/events/${{source.table}}"),
    );
    let sink = HttpSink::new(&cfg, CancellationToken::new(), "test")?;

    let event = make_event_for_table(1, "orders");
    sink.send(&event).await?;

    let received = server.received_events().await;
    assert_eq!(received.len(), 1);
    assert_eq!(received[0]["source"]["table"], "orders");

    Ok(())
}

#[tokio::test]
async fn http_sink_custom_headers() -> Result<()> {
    init_test_tracing();

    let server = TestServer::new();

    // Server that checks for custom header
    async fn check_header(
        headers: axum::http::HeaderMap,
        State(state): State<TestServer>,
        Json(body): Json<serde_json::Value>,
    ) -> axum::http::StatusCode {
        state.request_count.fetch_add(1, Ordering::Relaxed);
        let mut received = state.received.lock().await;
        // Store the custom header value alongside the event
        let header_val = headers
            .get("x-custom")
            .map(|v| v.to_str().unwrap_or(""))
            .unwrap_or("missing");
        let mut enriched = body;
        enriched["_test_header"] = serde_json::json!(header_val);
        received.push(enriched);
        axum::http::StatusCode::OK
    }

    let app = Router::new()
        .route("/events", post(check_header))
        .with_state(server.clone());
    let (port, _handle) = start_test_server(app).await;

    let mut cfg = make_http_cfg(
        "test-headers",
        &format!("http://127.0.0.1:{port}/events"),
    );
    cfg.headers
        .insert("X-Custom".into(), "deltaforge-test".into());

    let sink = HttpSink::new(&cfg, CancellationToken::new(), "test")?;

    let event = make_test_event(1);
    sink.send(&event).await?;

    let received = server.received_events().await;
    assert_eq!(received.len(), 1);
    assert_eq!(received[0]["_test_header"], "deltaforge-test");

    Ok(())
}

#[tokio::test]
async fn http_sink_connection_refused_retries() -> Result<()> {
    init_test_tracing();

    // No server running on this port
    let cfg = make_http_cfg("test-refused", "http://127.0.0.1:19999/events");
    let sink = HttpSink::new(&cfg, CancellationToken::new(), "test")?;

    let event = make_test_event(1);
    let result = sink.send(&event).await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    // Should be a connection error after retries
    assert!(
        matches!(
            err,
            deltaforge_core::SinkError::Connect { .. }
                | deltaforge_core::SinkError::Backpressure { .. }
        ),
        "expected Connect or Backpressure error, got: {:?}",
        err
    );

    Ok(())
}
