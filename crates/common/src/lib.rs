//! Shared utilities for DeltaForge sources and sinks.
//!
//! This crate provides common functionality used across the DeltaForge
//! ecosystem, including:
//!
//! - **DSN Utilities**: Connection string parsing and credential redaction
//! - **Retry Logic**: Exponential backoff with jitter for resilient connections
//! - **Pattern Matching**: Flexible allow-lists for tables, topics, and streams
//! - **Async Patterns**: Pause gates, watchdogs, and timeout helpers
//! - **Time Utilities**: Timestamp conversions between database formats
//!
//! # Design Philosophy
//!
//! These utilities are designed to be:
//! - **Database/sink agnostic**: Work with MySQL, PostgreSQL, Kafka, Redis, etc.
//! - **Async-first**: Built for tokio-based async code
//! - **Zero-copy where possible**: Use references and Arc where appropriate
//! - **Well-tested**: Comprehensive unit tests for all functionality
//!
//! # Quick Start
//!
//! ```ignore
//! use deltaforge_common::{
//!     // DSN utilities
//!     redact_dsn, DsnComponents,
//!     // Retry logic
//!     RetryPolicy, retry_async, RetryOutcome, is_retryable_message,
//!     // Pattern matching
//!     AllowList,
//!     // Pause gates
//!     pause_until_resumed,
//! };
//!
//! // Redact credentials for logging
//! let dsn = "postgres://user:secret@localhost/db";
//! println!("Connecting to {}", redact_dsn(dsn));
//!
//! // Retry with exponential backoff
//! let result = retry_async(
//!     |attempt| async move { connect_to_database().await },
//!     |e| is_retryable_message(&e.to_string()),
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

// =============================================================================
// DSN Utilities
// =============================================================================

pub use dsn::{
    DsnComponents, extract_host_from_url, redact_auth_token, redact_dsn,
    redact_keyvalue_password, redact_url_password,
};

// =============================================================================
// Pattern Matching
// =============================================================================

pub use patterns::AllowList;

// =============================================================================
// Pause Gates
// =============================================================================

pub use pause::{PauseResult, pause_until_resumed, pause_with_timeout};

// =============================================================================
// Retry Logic
// =============================================================================

pub use retry::{
    // Outcome types
    RetryOutcome,
    // Policy configuration
    RetryPolicy,
    // Trait for custom error types
    Retryable,
    // Helper functions for error classification
    is_permanent_failure,
    is_retryable_message,
    // Core retry function and watchdog
    retry_async,
    watchdog,
};

// =============================================================================
// Time Utilities
// =============================================================================

pub use time::{
    // PostgreSQL timestamp conversion
    PG_EPOCH_OFFSET_MICROS,
    now_ms,
    pg_timestamp_to_unix_ms,
    // General timestamp utilities
    ts_sec_to_ms,
    ts_sec_to_ms_i64,
    unix_ms_to_pg_timestamp,
};
