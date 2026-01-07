//! Shared utilities for DeltaForge sources and sinks.
//!
//! This crate provides common functionality used across the DeltaForge
//! ecosystem, including:
//!
//! - **DSN Utilities**: Connection string parsing and credential redaction
//! - **Retry Logic**: Exponential backoff with jitter for resilient connections
//! - **Pattern Matching**: Flexible allow-lists for tables, topics, and streams
//! - **Async Patterns**: Pause gates, watchdogs, and timeout helpers
//!
//! # Design Philosophy
//!
//! These utilities are designed to be:
//! - **Database/sink agnostic**: Work with MySQL, PostgreSQL, Kafka, Redis, etc.
//! - **Async-first**: Built for tokio-based async code
//! - **Zero-copy where possible**: Use references and Arc where appropriate
//! - **Well-tested**: Comprehensive unit tests for all functionality
//!
//! # Example
//!
//! ```ignore
//! use deltaforge_common::{RetryPolicy, retry_async, redact_dsn};
//!
//! // Redact credentials for logging
//! let dsn = "postgres://user:secret@localhost/db";
//! println!("Connecting to {}", redact_dsn(dsn));
//!
//! // Retry with exponential backoff
//! let result = retry_async(
//!     |attempt| async move { connect_to_database().await },
//!     |e| classify_error(e),
//!     |e| is_retryable(e),
//!     Duration::from_secs(30),
//!     RetryPolicy::default(),
//!     &cancel_token,
//!     "db_connect",
//! ).await;
//! ```

pub mod dsn;
pub mod patterns;
pub mod pause;
pub mod retry;
pub mod time;

// Re-export commonly used types at crate root
pub use dsn::{
    DsnComponents, extract_host_from_url, redact_auth_token, redact_dsn,
    redact_keyvalue_password, redact_url_password,
};
pub use patterns::AllowList;
pub use pause::pause_until_resumed;
pub use retry::{
    RetryPolicy, retry_async, retryable_connect, retryable_io, watchdog,
};
pub use time::{PG_EPOCH_OFFSET_MICROS, pg_timestamp_to_unix_ms, ts_sec_to_ms};
