//! Shared test utilities for sink integration tests.

use std::sync::Once;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

static TRACING_INIT: Once = Once::new();

/// Initialize tracing for tests (once per process).
pub fn init_test_tracing() {
    TRACING_INIT.call_once(|| {
        let filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("info,rdkafka=warn,testcontainers=warn"));

        tracing_subscriber::registry()
            .with(fmt::layer().with_test_writer().compact())
            .with(filter)
            .init();
    });
}