//! Shared test utilities for sink integration tests.

use std::sync::Once;

use deltaforge_core::{Event, Op, SourceInfo, SourcePosition};
use serde_json::json;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

static TRACING_INIT: Once = Once::new();

/// Initialize tracing for tests (once per process).
pub fn init_test_tracing() {
    TRACING_INIT.call_once(|| {
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            EnvFilter::new("info,rdkafka=warn,testcontainers=warn")
        });

        tracing_subscriber::registry()
            .with(fmt::layer().with_test_writer().compact())
            .with(filter)
            .init();
    });
}

/// Create a test event with a specific ID.
pub fn make_test_event(id: i64) -> Event {
    Event::new_row(
        SourceInfo {
            version: "deltaforge-test".into(),
            connector: "test".into(),
            name: "test-db".into(),
            ts_ms: 1_700_000_000_000 + id,
            db: "testdb".into(),
            schema: None,
            table: "table".into(),
            snapshot: None,
            position: SourcePosition::default(),
        },
        Op::Create,
        None,
        Some(json!({"id": id, "name": format!("item-{}", id)})),
        1_700_000_000_000 + id,
        64,
    )
}

/// Create a test event with specific data size.
pub fn make_large_event(id: i64, size_bytes: usize) -> Event {
    let padding = "x".repeat(size_bytes);
    Event::new_row(
        SourceInfo {
            version: "deltaforge-test".into(),
            connector: "test".into(),
            name: "test-db".into(),
            ts_ms: 1_700_000_000_000 + id,
            db: "testdb".into(),
            schema: None,
            table: "table".into(),
            snapshot: None,
            position: SourcePosition::default(),
        },
        Op::Create,
        None,
        Some(json!({"id": id, "payload": padding})),
        1_700_000_000_000 + id,
        64,
    )
}

/// Create a test event for a specific table (for routing tests).
pub fn make_event_for_table(id: i64, table: &str) -> Event {
    Event::new_row(
        SourceInfo {
            version: "deltaforge-test".into(),
            connector: "test".into(),
            name: "test-db".into(),
            ts_ms: 1_700_000_000_000 + id,
            db: "testdb".into(),
            schema: None,
            table: table.into(),
            snapshot: None,
            position: SourcePosition::default(),
        },
        Op::Create,
        None,
        Some(json!({"id": id, "name": format!("item-{}", id)})),
        1_700_000_000_000 + id,
        64,
    )
}
