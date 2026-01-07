//! Retry utilities with exponential backoff and jitter.
//!
//! This module provides resilient connection and operation handling for
//! database sources and sinks. It includes:
//!
//! - **RetryPolicy**: Configurable exponential backoff with jitter
//! - **retry_async**: Generic async retry helper with timeouts
//! - **watchdog**: Operation timeout wrapper with cancellation support
//! - **Retryability helpers**: Classify errors as retryable or fatal
//!
//! # Design
//!
//! The retry logic is designed to:
//! - Prevent thundering herd by using jitter
//! - Cap backoff to avoid excessive waits
//! - Support cancellation for graceful shutdown
//! - Provide detailed logging for debugging
//!
//! # Examples
//!
//! ```ignore
//! use common::{RetryPolicy, retry_async, retryable_connect};
//! use std::time::Duration;
//!
//! let result = retry_async(
//!     |attempt| async move {
//!         println!("Attempt {}", attempt);
//!         connect_to_database().await
//!     },
//!     |e| classify_as_source_error(e),
//!     retryable_connect,
//!     Duration::from_secs(30),
//!     RetryPolicy::default(),
//!     &cancel_token,
//!     "db_connect",
//! ).await;
//! ```

use std::borrow::Cow;
use std::future::Future;
use std::time::Duration;

use rand::Rng;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

// =============================================================================
// Error Types
// =============================================================================

/// Common error type for retry operations.
///
/// This mirrors common error patterns in sources and sinks without
/// requiring a dependency on deltaforge-core.
#[derive(Debug, thiserror::Error)]
pub enum RetryError {
    /// Operation was cancelled
    #[error("operation cancelled")]
    Cancelled,

    /// Operation timed out
    #[error("timeout: {action}")]
    Timeout { action: Cow<'static, str> },

    /// Connection failed
    #[error("connection failed: {details}")]
    Connect { details: Cow<'static, str> },

    /// I/O error
    #[error("I/O error: {0}")]
    Io(String),

    /// Other error
    #[error("{0}")]
    Other(#[from] anyhow::Error),
}

impl RetryError {
    /// Create a timeout error with a static action label.
    pub fn timeout(action: &'static str) -> Self {
        Self::Timeout {
            action: Cow::Borrowed(action),
        }
    }

    /// Create a connection error.
    pub fn connect(details: impl Into<String>) -> Self {
        Self::Connect {
            details: Cow::Owned(details.into()),
        }
    }
}

// =============================================================================
// Retry Policy
// =============================================================================

/// Exponential backoff policy with jitter.
///
/// The backoff doubles after each attempt (up to `max`), with random jitter
/// applied to prevent thundering herd problems when multiple clients retry
/// simultaneously.
///
/// # Example
///
/// ```
/// use common::RetryPolicy;
/// use std::time::Duration;
///
/// // Default: 1s initial, 60s max, 20% jitter, unlimited retries
/// let mut policy = RetryPolicy::default();
///
/// // Custom policy
/// let mut custom = RetryPolicy {
///     initial: Duration::from_millis(500),
///     max: Duration::from_secs(30),
///     jitter: 0.1, // 10% jitter
///     max_retries: Some(5),
///     ..Default::default()
/// };
///
/// // Get next backoff duration
/// let delay = policy.next_backoff();
/// ```
#[derive(Clone, Debug)]
pub struct RetryPolicy {
    /// Initial backoff interval
    pub initial: Duration,

    /// Maximum backoff interval (caps exponential growth)
    pub max: Duration,

    /// Jitter factor (0.0 to 1.0). Applied as ±jitter to the backoff.
    pub jitter: f64,

    /// Maximum retry attempts. `None` = retry forever until cancelled.
    pub max_retries: Option<u32>,

    /// Current backoff duration (internal state)
    pub current_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            initial: Duration::from_millis(1000),
            max: Duration::from_secs(60),
            jitter: 0.2,
            max_retries: None,
            current_backoff: Duration::from_millis(1000),
        }
    }
}

impl RetryPolicy {
    /// Create a new retry policy with custom parameters.
    pub fn new(initial: Duration, max: Duration, jitter: f64, max_retries: Option<u32>) -> Self {
        Self {
            initial,
            max,
            jitter: jitter.clamp(0.0, 1.0),
            max_retries,
            current_backoff: initial,
        }
    }

    /// Create a policy for aggressive retries (short backoff).
    pub fn aggressive() -> Self {
        Self {
            initial: Duration::from_millis(100),
            max: Duration::from_secs(5),
            jitter: 0.1,
            max_retries: Some(10),
            current_backoff: Duration::from_millis(100),
        }
    }

    /// Create a policy for patient retries (long backoff).
    pub fn patient() -> Self {
        Self {
            initial: Duration::from_secs(5),
            max: Duration::from_secs(300), // 5 minutes
            jitter: 0.25,
            max_retries: None,
            current_backoff: Duration::from_secs(5),
        }
    }

    /// Get next backoff duration and advance internal state.
    ///
    /// The returned duration has jitter applied. After calling this,
    /// the internal state advances to the next backoff level.
    pub fn next_backoff(&mut self) -> Duration {
        let current = self.current_backoff;

        // Exponential increase, capped at max
        self.current_backoff = current.saturating_mul(2).min(self.max);

        // Apply jitter to returned value (skip if jitter is zero)
        if self.jitter > 0.0 {
            let jitter_range = -self.jitter..self.jitter;
            let jitter_factor = 1.0 + rand::rng().random_range(jitter_range);
            current
                .mul_f64(jitter_factor)
                .max(Duration::from_nanos(1))
        } else {
            current
        }
    }

    /// Reset backoff to initial value.
    ///
    /// Call this after a successful operation to reset the backoff
    /// for future retries.
    pub fn reset(&mut self) {
        self.current_backoff = self.initial;
    }

    /// Check if we should continue retrying.
    pub fn should_retry(&self, attempt: u32) -> bool {
        self.max_retries.map_or(true, |max| attempt <= max)
    }
}

// =============================================================================
// Retry Helpers
// =============================================================================

/// Generic async retry helper with attempt timeout and cancellation.
///
/// This function handles the retry loop, including:
/// - Per-attempt timeouts
/// - Exponential backoff between attempts
/// - Cancellation support
/// - Error classification (retryable vs fatal)
///
/// # Type Parameters
///
/// - `T`: Success type
/// - `E`: Raw error type from the operation
/// - `Fut`: Future returned by the operation
/// - `Op`: Operation function (takes attempt number, returns future)
/// - `Classify`: Maps raw error to `RetryError`
/// - `Retryable`: Determines if a `RetryError` should be retried
///
/// # Arguments
///
/// - `op`: Function that performs the operation (receives attempt number)
/// - `classify`: Function to convert the operation's error to `RetryError`
/// - `should_retry`: Function to determine if an error is retryable
/// - `attempt_timeout`: Maximum duration for each attempt
/// - `policy`: Retry policy (backoff configuration)
/// - `cancel`: Cancellation token for graceful shutdown
/// - `label`: Label for logging
///
/// # Returns
///
/// - `Ok(T)` on success
/// - `Err(RetryError)` if all retries exhausted or non-retryable error
pub async fn retry_async<T, E, Fut, Op, Classify, Retryable>(
    mut op: Op,
    classify: Classify,
    should_retry: Retryable,
    attempt_timeout: Duration,
    mut policy: RetryPolicy,
    cancel: &CancellationToken,
    label: &'static str,
) -> Result<T, RetryError>
where
    Op: FnMut(u32) -> Fut,
    Fut: Future<Output = Result<T, E>>,
    Classify: Fn(E) -> RetryError,
    Retryable: Fn(&RetryError) -> bool,
{
    let mut attempt = 0u32;

    loop {
        if cancel.is_cancelled() {
            return Err(RetryError::Cancelled);
        }

        attempt += 1;

        // Check if we've exceeded max retries
        if !policy.should_retry(attempt) {
            return Err(RetryError::Other(anyhow::anyhow!(
                "{}: max retries ({}) exceeded",
                label,
                policy.max_retries.unwrap_or(0)
            )));
        }

        // Run one attempt with timeout
        let result = timeout(attempt_timeout, op(attempt));
        match result.await {
            Ok(Ok(value)) => {
                debug!(
                    label = label,
                    attempt = attempt,
                    "operation succeeded"
                );
                return Ok(value);
            }
            Ok(Err(e)) => {
                let retry_err = classify(e);

                if should_retry(&retry_err) {
                    let backoff = policy.next_backoff();
                    warn!(
                        label = label,
                        attempt = attempt,
                        error = %retry_err,
                        backoff_ms = backoff.as_millis(),
                        "retryable error, backing off"
                    );

                    // Wait with cancellation support
                    tokio::select! {
                        _ = cancel.cancelled() => {
                            return Err(RetryError::Cancelled);
                        }
                        _ = sleep(backoff) => {}
                    }
                } else {
                    // Non-retryable error
                    warn!(
                        label = label,
                        attempt = attempt,
                        error = %retry_err,
                        "non-retryable error"
                    );
                    return Err(retry_err);
                }
            }
            Err(_elapsed) => {
                // Timeout
                let backoff = policy.next_backoff();
                warn!(
                    label = label,
                    attempt = attempt,
                    timeout_ms = attempt_timeout.as_millis(),
                    backoff_ms = backoff.as_millis(),
                    "attempt timed out, backing off"
                );

                tokio::select! {
                    _ = cancel.cancelled() => {
                        return Err(RetryError::Cancelled);
                    }
                    _ = sleep(backoff) => {}
                }
            }
        }
    }
}

/// Watchdog wrapper for async operations.
///
/// Wraps an operation with a timeout and cancellation support.
/// Unlike `retry_async`, this doesn't retry - it just enforces a timeout.
///
/// # Arguments
///
/// - `op`: The future to execute
/// - `timeout_duration`: Maximum duration to wait
/// - `cancel`: Cancellation token
/// - `label`: Label for the timeout error
///
/// # Returns
///
/// - `Ok(T)` if the operation completes in time
/// - `Err(RetryError::Timeout)` if it times out
/// - `Err(RetryError::Cancelled)` if cancelled
pub async fn watchdog<T, E, Fut>(
    op: Fut,
    timeout_duration: Duration,
    cancel: &CancellationToken,
    label: &'static str,
) -> Result<T, RetryError>
where
    Fut: Future<Output = Result<T, E>>,
    E: Into<anyhow::Error>,
{
    tokio::select! {
        _ = cancel.cancelled() => Err(RetryError::Cancelled),
        res = timeout(timeout_duration, op) => {
            match res {
                Ok(Ok(val)) => Ok(val),
                Ok(Err(e)) => Err(RetryError::Other(e.into())),
                Err(_) => Err(RetryError::Timeout {
                    action: Cow::Borrowed(label),
                }),
            }
        }
    }
}

// =============================================================================
// Retryability Predicates
// =============================================================================

/// Default retryability check for connection-related errors.
///
/// Returns `true` for errors that are typically transient and worth retrying:
/// - Timeouts
/// - Connection refused/reset
/// - I/O errors
pub fn retryable_connect(err: &RetryError) -> bool {
    matches!(
        err,
        RetryError::Timeout { .. } | RetryError::Connect { .. } | RetryError::Io(_)
    )
}

/// Default retryability check for I/O and streaming errors.
///
/// Returns `true` for errors that suggest a broken connection that
/// might succeed on reconnect:
/// - Timeouts
/// - Connection errors
/// - I/O errors
/// - Specific error messages (EOF, connection reset, etc.)
pub fn retryable_io(err: &RetryError) -> bool {
    match err {
        RetryError::Timeout { .. } => true,
        RetryError::Connect { .. } => true,
        RetryError::Io(_) => true,
        RetryError::Other(inner) => {
            let s = inner.to_string().to_lowercase();
            s.contains("io error")
                || s.contains("eof")
                || s.contains("connection reset")
                || s.contains("broken pipe")
                || s.contains("timeout")
                || s.contains("connection refused")
        }
        RetryError::Cancelled => false,
    }
}

/// Check if an error is definitely not retryable.
///
/// These errors indicate bugs or permanent failures:
/// - Authentication failures
/// - Permission denied
/// - Invalid configuration
pub fn definitely_not_retryable(err: &RetryError) -> bool {
    match err {
        RetryError::Other(inner) => {
            let s = inner.to_string().to_lowercase();
            s.contains("authentication")
                || s.contains("permission denied")
                || s.contains("access denied")
                || s.contains("invalid")
                || s.contains("not found")
        }
        _ => false,
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    #[test]
    fn backoff_increases_exponentially() {
        let mut policy = RetryPolicy::new(
            Duration::from_secs(1),
            Duration::from_secs(60),
            0.0, // No jitter for predictable test
            None,
        );

        let d1 = policy.next_backoff();
        let d2 = policy.next_backoff();
        let d3 = policy.next_backoff();

        assert_eq!(d1, Duration::from_secs(1));
        assert_eq!(d2, Duration::from_secs(2));
        assert_eq!(d3, Duration::from_secs(4));
    }

    #[test]
    fn backoff_caps_at_max() {
        let mut policy = RetryPolicy::new(
            Duration::from_secs(30),
            Duration::from_secs(60),
            0.0,
            None,
        );

        let _ = policy.next_backoff(); // 30s
        let _ = policy.next_backoff(); // 60s (capped)
        let d3 = policy.next_backoff(); // still 60s

        assert_eq!(d3, Duration::from_secs(60));
    }

    #[test]
    fn backoff_with_jitter_stays_within_bounds() {
        let mut policy = RetryPolicy::new(
            Duration::from_secs(1),
            Duration::from_secs(60),
            0.5, // 50% jitter: values should be 0.5x to 1.5x
            None,
        );

        // Run many iterations to verify bounds
        for _ in 0..100 {
            policy.reset();
            let d = policy.next_backoff();
            // With 50% jitter on 1s: should be between 500ms and 1500ms
            assert!(
                d >= Duration::from_millis(500) && d <= Duration::from_millis(1500),
                "backoff {:?} out of expected range [500ms, 1500ms]",
                d
            );
        }
    }

    #[test]
    fn reset_returns_to_initial() {
        let mut policy = RetryPolicy::default();

        // Advance a few times
        let _ = policy.next_backoff();
        let _ = policy.next_backoff();
        let _ = policy.next_backoff();

        policy.reset();

        assert_eq!(policy.current_backoff, policy.initial);
    }

    #[test]
    fn should_retry_respects_max_retries() {
        let policy = RetryPolicy {
            max_retries: Some(3),
            ..Default::default()
        };

        assert!(policy.should_retry(1));
        assert!(policy.should_retry(3));
        assert!(!policy.should_retry(4));
    }

    #[test]
    fn retryable_connect_classifications() {
        assert!(retryable_connect(&RetryError::Timeout {
            action: "test".into()
        }));
        assert!(retryable_connect(&RetryError::Connect {
            details: "refused".into()
        }));
        assert!(retryable_connect(&RetryError::Io("broken".into())));
        assert!(!retryable_connect(&RetryError::Cancelled));
    }

    #[test]
    fn retryable_io_checks_error_message() {
        // Retryable messages
        assert!(retryable_io(&RetryError::Other(anyhow::anyhow!(
            "connection reset by peer"
        ))));
        assert!(retryable_io(&RetryError::Other(anyhow::anyhow!(
            "broken pipe"
        ))));
        assert!(retryable_io(&RetryError::Other(anyhow::anyhow!("EOF"))));

        // Not retryable
        assert!(!retryable_io(&RetryError::Other(anyhow::anyhow!(
            "authentication failed"
        ))));
        assert!(!retryable_io(&RetryError::Cancelled));
    }

    #[test]
    fn definitely_not_retryable_checks_error_message() {
        assert!(definitely_not_retryable(&RetryError::Other(anyhow::anyhow!(
            "authentication failed"
        ))));
        assert!(definitely_not_retryable(&RetryError::Other(anyhow::anyhow!(
            "permission denied"
        ))));
        assert!(definitely_not_retryable(&RetryError::Other(anyhow::anyhow!(
            "invalid configuration"
        ))));

        // These are retryable, not definitely-not
        assert!(!definitely_not_retryable(&RetryError::Timeout {
            action: "test".into()
        }));
        assert!(!definitely_not_retryable(&RetryError::Other(
            anyhow::anyhow!("connection reset")
        )));
    }

    #[tokio::test]
    async fn retry_succeeds_on_third_attempt() {
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = attempts.clone();

        let cancel = CancellationToken::new();
        let policy = RetryPolicy::new(
            Duration::from_millis(1), // Fast backoff for test
            Duration::from_millis(10),
            0.0,
            Some(5),
        );

        let result: Result<&str, RetryError> = retry_async(
            |_| {
                let attempts = attempts_clone.clone();
                async move {
                    let count = attempts.fetch_add(1, Ordering::SeqCst);
                    if count < 2 {
                        Err(RetryError::Connect {
                            details: "not yet".into(),
                        })
                    } else {
                        Ok("success")
                    }
                }
            },
            |e| e,
            retryable_connect,
            Duration::from_secs(1),
            policy,
            &cancel,
            "test",
        )
        .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "success");
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn retry_stops_on_cancel() {
        let cancel = CancellationToken::new();
        cancel.cancel(); // Cancel immediately

        let policy = RetryPolicy::new(
            Duration::from_millis(100),
            Duration::from_secs(1),
            0.0,
            None,
        );

        let result: Result<(), RetryError> = retry_async(
            |_| async { Err(RetryError::Connect { details: "fail".into() }) },
            |e| e,
            retryable_connect,
            Duration::from_secs(1),
            policy,
            &cancel,
            "test",
        )
        .await;

        assert!(matches!(result, Err(RetryError::Cancelled)));
    }

    #[tokio::test]
    async fn retry_stops_on_non_retryable_error() {
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = attempts.clone();
        let cancel = CancellationToken::new();

        let result: Result<(), RetryError> = retry_async(
            |_| {
                let attempts = attempts_clone.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    // Return a non-retryable error
                    Err(RetryError::Other(anyhow::anyhow!("authentication failed")))
                }
            },
            |e| e,
            retryable_io, // Uses message-based classification
            Duration::from_secs(1),
            RetryPolicy::default(),
            &cancel,
            "test",
        )
        .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 1, "should stop after first non-retryable error");
    }

    #[tokio::test]
    async fn retry_stops_after_max_retries() {
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = attempts.clone();
        let cancel = CancellationToken::new();

        let policy = RetryPolicy::new(
            Duration::from_millis(1),
            Duration::from_millis(10),
            0.0,
            Some(3), // Max 3 attempts
        );

        let result: Result<(), RetryError> = retry_async(
            |_| {
                let attempts = attempts_clone.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err(RetryError::Connect { details: "always fail".into() })
                }
            },
            |e| e,
            retryable_connect,
            Duration::from_secs(1),
            policy,
            &cancel,
            "test",
        )
        .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 3, "should stop after max retries");
    }

    #[tokio::test]
    async fn watchdog_returns_timeout_error() {
        let cancel = CancellationToken::new();

        let result: Result<(), RetryError> = watchdog(
            async {
                tokio::time::sleep(Duration::from_secs(10)).await;
                Ok::<_, anyhow::Error>(())
            },
            Duration::from_millis(10),
            &cancel,
            "slow_op",
        )
        .await;

        assert!(matches!(result, Err(RetryError::Timeout { .. })));
    }
}