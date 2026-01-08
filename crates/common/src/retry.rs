//! Retry utilities with exponential backoff and jitter.
//!
//! This module provides resilient connection and operation handling for
//! database sources and sinks. The design preserves caller error types
//! without requiring intermediate conversions.
//!
//! # Key Components
//!
//! - [`RetryPolicy`]: Configurable exponential backoff with jitter
//! - [`retry_async`]: Generic async retry loop with timeout and cancellation
//! - [`RetryOutcome`]: Result type that preserves the caller's error type
//!
//! # Design Principles
//!
//! - **Zero-cost error handling**: No error type conversions required
//! - **Thundering herd prevention**: Random jitter on backoff
//! - **Graceful shutdown**: Cancellation token support throughout
//! - **Observability**: Structured logging at each retry stage
//!
//! # Example
//!
//! ```ignore
//! use common::retry::{retry_async, RetryPolicy, RetryOutcome};
//! use std::time::Duration;
//!
//! // Define what errors are worth retrying
//! fn is_transient(e: &MyError) -> bool {
//!     matches!(e, MyError::Timeout | MyError::ConnectionReset)
//! }
//!
//! let result = retry_async(
//!     |attempt| async move {
//!         println!("Attempt {}", attempt);
//!         connect_to_database().await
//!     },
//!     is_transient,
//!     Duration::from_secs(30),
//!     RetryPolicy::default(),
//!     &cancel_token,
//!     "db_connect",
//! ).await;
//!
//! match result {
//!     Ok(conn) => println!("Connected!"),
//!     Err(RetryOutcome::Exhausted { attempts, last_error }) => {
//!         println!("Failed after {} attempts: {}", attempts, last_error);
//!     }
//!     Err(RetryOutcome::Cancelled) => println!("Cancelled"),
//!     // ... handle other cases
//! }
//! ```

use std::borrow::Cow;
use std::fmt::Display;
use std::future::Future;
use std::time::Duration;

use rand::Rng;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

// =============================================================================
// Retry Outcome
// =============================================================================

/// Outcome of a failed retry operation.
///
/// This enum is generic over `E`, allowing callers to preserve their native
/// error types without conversion overhead.
///
/// # Type Parameter
///
/// - `E`: The error type returned by the operation being retried.
///
/// # Variants
///
/// - `Cancelled`: The operation was cancelled via the cancellation token.
///   This takes priority over other failure modes.
///
/// - `Timeout`: An individual attempt timed out. The `action` field contains
///   the label passed to `retry_async` for identification in logs.
///
/// - `Exhausted`: All retry attempts were used. Contains the attempt count
///   and the last error encountered, allowing callers to inspect or propagate it.
///
/// - `Failed`: A non-retryable error occurred. The operation will not be
///   retried, and the original error is returned immediately.
#[derive(Debug)]
pub enum RetryOutcome<E> {
    /// Operation was cancelled via the cancellation token.
    Cancelled,

    /// An individual attempt exceeded the timeout duration.
    ///
    /// This is considered retryable by default - the retry loop will
    /// back off and try again unless max retries is exceeded.
    Timeout {
        /// Label identifying which operation timed out.
        action: Cow<'static, str>,
    },

    /// Maximum retry attempts exhausted.
    ///
    /// Contains information about the retry history for logging/debugging.
    Exhausted {
        /// Total number of attempts made.
        attempts: u32,
        /// The error from the final attempt.
        last_error: E,
    },

    /// A non-retryable error occurred.
    ///
    /// The `is_retryable` predicate returned `false` for this error,
    /// so no further attempts will be made.
    Failed(E),
}

impl<E: Display> Display for RetryOutcome<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cancelled => write!(f, "operation cancelled"),
            Self::Timeout { action } => write!(f, "timeout: {}", action),
            Self::Exhausted {
                attempts,
                last_error,
            } => {
                write!(
                    f,
                    "exhausted after {} attempts: {}",
                    attempts, last_error
                )
            }
            Self::Failed(e) => write!(f, "non-retryable error: {}", e),
        }
    }
}

impl<E: Display + std::fmt::Debug> std::error::Error for RetryOutcome<E> {}

impl<E> RetryOutcome<E> {
    /// Returns `true` if the outcome was cancellation.
    pub fn is_cancelled(&self) -> bool {
        matches!(self, Self::Cancelled)
    }

    /// Returns `true` if the outcome was a timeout.
    pub fn is_timeout(&self) -> bool {
        matches!(self, Self::Timeout { .. })
    }

    /// Returns the inner error if this is `Exhausted` or `Failed`.
    ///
    /// Returns `None` for `Cancelled` and `Timeout` variants.
    pub fn into_inner(self) -> Option<E> {
        match self {
            Self::Exhausted { last_error, .. } => Some(last_error),
            Self::Failed(e) => Some(e),
            _ => None,
        }
    }

    /// Maps the error type using the provided function.
    ///
    /// Useful for converting the error when propagating up the call stack.
    pub fn map_err<F, U>(self, f: F) -> RetryOutcome<U>
    where
        F: FnOnce(E) -> U,
    {
        match self {
            Self::Cancelled => RetryOutcome::Cancelled,
            Self::Timeout { action } => RetryOutcome::Timeout { action },
            Self::Exhausted {
                attempts,
                last_error,
            } => RetryOutcome::Exhausted {
                attempts,
                last_error: f(last_error),
            },
            Self::Failed(e) => RetryOutcome::Failed(f(e)),
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
/// # Fields
///
/// - `initial`: Starting backoff duration (default: 1 second)
/// - `max`: Maximum backoff duration, caps exponential growth (default: 60 seconds)
/// - `jitter`: Random variation factor, 0.0 to 1.0 (default: 0.2 = ±20%)
/// - `max_retries`: Maximum attempts before giving up. `None` = unlimited (default: None)
///
/// # Backoff Calculation
///
/// Each call to `next_backoff()`:
/// 1. Returns the current backoff with jitter applied
/// 2. Doubles the internal backoff (capped at `max`)
///
/// With `jitter = 0.2` and `initial = 1s`, the first backoff will be
/// randomly between 0.8s and 1.2s.
///
/// # Example
///
/// ```
/// use common::retry::RetryPolicy;
/// use std::time::Duration;
///
/// // Default policy: 1s initial, 60s max, 20% jitter, unlimited retries
/// let mut policy = RetryPolicy::default();
///
/// // Custom policy for aggressive retries
/// let mut fast = RetryPolicy::new(
///     Duration::from_millis(100),  // Start at 100ms
///     Duration::from_secs(5),       // Cap at 5s
///     0.1,                          // 10% jitter
///     Some(10),                     // Give up after 10 attempts
/// );
///
/// // Get backoff durations
/// let d1 = policy.next_backoff(); // ~1s (with jitter)
/// let d2 = policy.next_backoff(); // ~2s (with jitter)
/// let d3 = policy.next_backoff(); // ~4s (with jitter)
/// ```
#[derive(Clone, Debug)]
pub struct RetryPolicy {
    /// Initial backoff interval.
    pub initial: Duration,

    /// Maximum backoff interval (caps exponential growth).
    pub max: Duration,

    /// Jitter factor (0.0 to 1.0). Applied as ±jitter to the backoff.
    /// For example, 0.2 means the actual delay will be 80%-120% of the
    /// calculated backoff.
    pub jitter: f64,

    /// Maximum retry attempts. `None` = retry forever until cancelled.
    pub max_retries: Option<u32>,

    /// Current backoff duration (internal state, advances with each call).
    current_backoff: Duration,
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
    ///
    /// # Arguments
    ///
    /// - `initial`: Starting backoff duration
    /// - `max`: Maximum backoff (caps exponential growth)
    /// - `jitter`: Random factor (0.0-1.0), clamped to valid range
    /// - `max_retries`: Maximum attempts, or `None` for unlimited
    pub fn new(
        initial: Duration,
        max: Duration,
        jitter: f64,
        max_retries: Option<u32>,
    ) -> Self {
        Self {
            initial,
            max,
            jitter: jitter.clamp(0.0, 1.0),
            max_retries,
            current_backoff: initial,
        }
    }

    /// Create a policy for aggressive retries (short backoff, limited attempts).
    ///
    /// Good for operations that should fail fast:
    /// - 100ms initial, 5s max
    /// - 10% jitter
    /// - Max 10 attempts
    pub fn aggressive() -> Self {
        Self {
            initial: Duration::from_millis(100),
            max: Duration::from_secs(5),
            jitter: 0.1,
            max_retries: Some(10),
            current_backoff: Duration::from_millis(100),
        }
    }

    /// Create a policy for patient retries (long backoff, unlimited attempts).
    ///
    /// Good for critical operations that must eventually succeed:
    /// - 5s initial, 5min max
    /// - 25% jitter
    /// - Unlimited retries
    pub fn patient() -> Self {
        Self {
            initial: Duration::from_secs(5),
            max: Duration::from_secs(300),
            jitter: 0.25,
            max_retries: None,
            current_backoff: Duration::from_secs(5),
        }
    }

    /// Get next backoff duration and advance internal state.
    ///
    /// Returns the current backoff with jitter applied, then doubles
    /// the internal backoff (capped at `max`) for the next call.
    ///
    /// # Jitter Behavior
    ///
    /// If `jitter > 0`, the returned duration is multiplied by a random
    /// factor in the range `[1-jitter, 1+jitter]`. This prevents
    /// synchronized retries across multiple clients.
    pub fn next_backoff(&mut self) -> Duration {
        let current = self.current_backoff;

        // Exponential increase, capped at max
        self.current_backoff = current.saturating_mul(2).min(self.max);

        // Apply jitter to returned value (skip if jitter is zero)
        if self.jitter > 0.0 {
            let jitter_range = -self.jitter..self.jitter;
            let jitter_factor = 1.0 + rand::rng().random_range(jitter_range);
            current.mul_f64(jitter_factor).max(Duration::from_nanos(1))
        } else {
            current
        }
    }

    /// Reset backoff to initial value.
    ///
    /// Call this after a successful operation to reset the progression
    /// for future retry sequences.
    pub fn reset(&mut self) {
        self.current_backoff = self.initial;
    }

    /// Check if we should continue retrying given the attempt count.
    ///
    /// Returns `true` if:
    /// - `max_retries` is `None` (unlimited), or
    /// - `attempt <= max_retries`
    pub fn should_retry(&self, attempt: u32) -> bool {
        self.max_retries.is_none_or(|max| attempt <= max)
    }
}

// =============================================================================
// Retry Function
// =============================================================================

/// Generic async retry loop with timeout and cancellation support.
///
/// Executes an async operation repeatedly until it succeeds, is cancelled,
/// times out too many times, or returns a non-retryable error.
///
/// # Type Parameters
///
/// - `T`: Success type returned by the operation
/// - `E`: Error type returned by the operation (preserved in `RetryOutcome`)
/// - `Fut`: Future type returned by the operation
/// - `Op`: Operation factory function
/// - `IsRetryable`: Predicate to classify errors
///
/// # Arguments
///
/// - `op`: Function that creates the operation future. Receives the current
///   attempt number (1-indexed). Called fresh for each retry attempt.
///
/// - `is_retryable`: Predicate that examines an error and returns `true` if
///   the operation should be retried. Common patterns:
///   - Connection errors: retry
///   - Timeouts: retry
///   - Authentication failures: don't retry
///   - Invalid input: don't retry
///
/// - `attempt_timeout`: Maximum duration for each individual attempt.
///   If exceeded, the attempt is considered a timeout (retryable by default).
///
/// - `policy`: Backoff policy controlling delay between attempts and
///   maximum retry count.
///
/// - `cancel`: Cancellation token. When triggered, the retry loop exits
///   immediately with `RetryOutcome::Cancelled`.
///
/// - `label`: Static string identifying this operation in logs.
///
/// # Returns
///
/// - `Ok(T)`: Operation succeeded
/// - `Err(RetryOutcome::Cancelled)`: Cancelled via token
/// - `Err(RetryOutcome::Timeout { .. })`: Final attempt timed out and retries exhausted
/// - `Err(RetryOutcome::Exhausted { .. })`: Max retries reached
/// - `Err(RetryOutcome::Failed(E))`: Non-retryable error encountered
///
/// # Cancellation Behavior
///
/// Cancellation is checked:
/// 1. Before each attempt starts
/// 2. During backoff waits between attempts
///
/// This ensures prompt shutdown even during long backoff periods.
///
/// # Example
///
/// ```ignore
/// use common::retry::{retry_async, RetryPolicy, RetryOutcome};
///
/// #[derive(Debug)]
/// enum DbError {
///     ConnectionRefused,
///     AuthFailed,
///     Timeout,
/// }
///
/// async fn connect() -> Result<Connection, DbError> {
///     // ... connection logic
/// }
///
/// let result = retry_async(
///     |attempt| {
///         println!("Attempt {}", attempt);
///         connect()
///     },
///     |e| matches!(e, DbError::ConnectionRefused | DbError::Timeout),
///     Duration::from_secs(10),
///     RetryPolicy::default(),
///     &cancel,
///     "db_connect",
/// ).await;
/// ```
pub async fn retry_async<T, E, Fut, Op, IsRetryable>(
    mut op: Op,
    is_retryable: IsRetryable,
    attempt_timeout: Duration,
    mut policy: RetryPolicy,
    cancel: &CancellationToken,
    label: &'static str,
) -> Result<T, RetryOutcome<E>>
where
    E: Display,
    Fut: Future<Output = Result<T, E>>,
    Op: FnMut(u32) -> Fut,
    IsRetryable: Fn(&E) -> bool,
{
    let mut attempt = 0u32;
    let mut last_error: Option<E> = None;

    loop {
        // Check cancellation before starting attempt
        if cancel.is_cancelled() {
            return Err(RetryOutcome::Cancelled);
        }

        attempt += 1;

        // Check if we've exceeded max retries
        if !policy.should_retry(attempt) {
            return Err(match last_error {
                Some(e) => RetryOutcome::Exhausted {
                    attempts: attempt - 1,
                    last_error: e,
                },
                None => RetryOutcome::Timeout {
                    action: Cow::Borrowed(label),
                },
            });
        }

        debug!(label = label, attempt = attempt, "starting attempt");

        // Run one attempt with timeout
        match timeout(attempt_timeout, op(attempt)).await {
            // Success!
            Ok(Ok(value)) => {
                debug!(label = label, attempt = attempt, "operation succeeded");
                return Ok(value);
            }

            // Operation returned an error
            Ok(Err(e)) => {
                if is_retryable(&e) {
                    let backoff = policy.next_backoff();
                    warn!(
                        label = label,
                        attempt = attempt,
                        error = %e,
                        backoff_ms = backoff.as_millis(),
                        "retryable error, backing off"
                    );

                    last_error = Some(e);

                    // Wait with cancellation support
                    tokio::select! {
                        _ = cancel.cancelled() => {
                            return Err(RetryOutcome::Cancelled);
                        }
                        _ = sleep(backoff) => {}
                    }
                } else {
                    // Non-retryable error - fail immediately
                    warn!(
                        label = label,
                        attempt = attempt,
                        error = %e,
                        "non-retryable error, giving up"
                    );
                    return Err(RetryOutcome::Failed(e));
                }
            }

            // Attempt timed out
            Err(_elapsed) => {
                let backoff = policy.next_backoff();
                warn!(
                    label = label,
                    attempt = attempt,
                    timeout_ms = attempt_timeout.as_millis(),
                    backoff_ms = backoff.as_millis(),
                    "attempt timed out, backing off"
                );

                // Timeouts are retryable by default
                tokio::select! {
                    _ = cancel.cancelled() => {
                        return Err(RetryOutcome::Cancelled);
                    }
                    _ = sleep(backoff) => {}
                }
            }
        }
    }
}

// =============================================================================
// Watchdog (single attempt with timeout)
// =============================================================================

/// Run an async operation with timeout and cancellation support.
///
/// Unlike `retry_async`, this executes the operation only once. Use this for
/// operations that shouldn't be retried but need timeout protection.
///
/// # Arguments
///
/// - `op`: The future to execute
/// - `timeout_duration`: Maximum time to wait for completion
/// - `cancel`: Cancellation token for early termination
/// - `label`: Identifier for timeout error messages
///
/// # Returns
///
/// - `Ok(T)`: Operation completed successfully
/// - `Err(RetryOutcome::Cancelled)`: Cancelled via token
/// - `Err(RetryOutcome::Timeout { .. })`: Operation timed out
/// - `Err(RetryOutcome::Failed(E))`: Operation returned an error
///
/// # Example
///
/// ```ignore
/// use common::retry::{watchdog, RetryOutcome};
/// use std::time::Duration;
///
/// let result = watchdog(
///     fetch_metadata(),
///     Duration::from_secs(5),
///     &cancel,
///     "fetch_metadata",
/// ).await;
///
/// match result {
///     Ok(meta) => println!("Got metadata: {:?}", meta),
///     Err(RetryOutcome::Timeout { .. }) => println!("Fetch timed out"),
///     Err(RetryOutcome::Cancelled) => println!("Cancelled"),
///     Err(RetryOutcome::Failed(e)) => println!("Error: {}", e),
/// }
/// ```
pub async fn watchdog<T, E, Fut>(
    op: Fut,
    timeout_duration: Duration,
    cancel: &CancellationToken,
    label: &'static str,
) -> Result<T, RetryOutcome<E>>
where
    E: Display,
    Fut: Future<Output = Result<T, E>>,
{
    tokio::select! {
        _ = cancel.cancelled() => Err(RetryOutcome::Cancelled),
        result = timeout(timeout_duration, op) => {
            match result {
                Ok(Ok(value)) => Ok(value),
                Ok(Err(e)) => Err(RetryOutcome::Failed(e)),
                Err(_) => Err(RetryOutcome::Timeout {
                    action: Cow::Borrowed(label),
                }),
            }
        }
    }
}

// =============================================================================
// Retryability Helpers
// =============================================================================

/// Helper trait for common error classification patterns.
///
/// Implement this on your error type to get automatic retryability
/// classification without writing custom predicates.
///
/// # Example
///
/// ```ignore
/// use common::retry::Retryable;
///
/// #[derive(Debug)]
/// enum MyError {
///     Timeout,
///     ConnectionReset,
///     AuthFailed,
/// }
///
/// impl Retryable for MyError {
///     fn is_retryable(&self) -> bool {
///         matches!(self, Self::Timeout | Self::ConnectionReset)
///     }
/// }
///
/// // Then use with retry_async:
/// retry_async(op, MyError::is_retryable, ...)
/// ```
pub trait Retryable {
    /// Returns `true` if this error is transient and the operation
    /// should be retried.
    fn is_retryable(&self) -> bool;
}

/// Check if an error message indicates a retryable I/O condition.
///
/// Useful as a fallback when you have an opaque error type (like `anyhow::Error`)
/// and need to classify by message content.
///
/// # Recognized Patterns
///
/// Returns `true` for messages containing (case-insensitive):
/// - "connection reset"
/// - "broken pipe"
/// - "eof" / "end of file"
/// - "timeout"
/// - "connection refused"
/// - "temporarily unavailable"
///
/// # Example
///
/// ```
/// use common::retry::is_retryable_message;
///
/// assert!(is_retryable_message("connection reset by peer"));
/// assert!(is_retryable_message("operation timed out"));
/// assert!(!is_retryable_message("authentication failed"));
/// ```
pub fn is_retryable_message(msg: &str) -> bool {
    let lower = msg.to_lowercase();
    lower.contains("connection reset")
        || lower.contains("broken pipe")
        || lower.contains("eof")
        || lower.contains("end of file")
        || lower.contains("timeout")
        || lower.contains("timed out")
        || lower.contains("connection refused")
        || lower.contains("temporarily unavailable")
        || lower.contains("try again")
}

/// Check if an error message indicates a permanent failure.
///
/// These errors should NOT be retried as they indicate configuration
/// or permission problems that won't resolve on their own.
///
/// # Recognized Patterns
///
/// Returns `true` for messages containing (case-insensitive):
/// - "authentication"
/// - "permission denied"
/// - "access denied"
/// - "unauthorized"
/// - "invalid" (credentials, configuration, etc.)
/// - "not found" (resource doesn't exist)
///
/// # Example
///
/// ```
/// use common::retry::is_permanent_failure;
///
/// assert!(is_permanent_failure("authentication failed"));
/// assert!(is_permanent_failure("permission denied"));
/// assert!(!is_permanent_failure("connection reset"));
/// ```
pub fn is_permanent_failure(msg: &str) -> bool {
    let lower = msg.to_lowercase();
    lower.contains("authentication")
        || lower.contains("permission denied")
        || lower.contains("access denied")
        || lower.contains("unauthorized")
        || lower.contains("invalid")
        || lower.contains("not found")
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    // Test error type that doesn't require any conversion
    #[derive(Debug, Clone)]
    enum TestError {
        Transient(String),
        Permanent(String),
    }

    impl Display for TestError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Transient(msg) => write!(f, "transient: {}", msg),
                Self::Permanent(msg) => write!(f, "permanent: {}", msg),
            }
        }
    }

    fn is_transient(e: &TestError) -> bool {
        matches!(e, TestError::Transient(_))
    }

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

        for _ in 0..100 {
            policy.reset();
            let d = policy.next_backoff();
            assert!(
                d >= Duration::from_millis(500)
                    && d <= Duration::from_millis(1500),
                "backoff {:?} out of expected range [500ms, 1500ms]",
                d
            );
        }
    }

    #[test]
    fn reset_returns_to_initial() {
        let mut policy = RetryPolicy::default();

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
    fn retry_outcome_map_err() {
        let outcome: RetryOutcome<i32> = RetryOutcome::Failed(42);
        let mapped: RetryOutcome<String> = outcome.map_err(|n| n.to_string());

        match mapped {
            RetryOutcome::Failed(s) => assert_eq!(s, "42"),
            _ => panic!("expected Failed variant"),
        }
    }

    #[test]
    fn retry_outcome_into_inner() {
        let exhausted: RetryOutcome<&str> = RetryOutcome::Exhausted {
            attempts: 3,
            last_error: "oops",
        };
        assert_eq!(exhausted.into_inner(), Some("oops"));

        let cancelled: RetryOutcome<&str> = RetryOutcome::Cancelled;
        assert_eq!(cancelled.into_inner(), None);
    }

    #[test]
    fn is_retryable_message_patterns() {
        assert!(is_retryable_message("connection reset by peer"));
        assert!(is_retryable_message("broken pipe"));
        assert!(is_retryable_message("unexpected EOF"));
        assert!(is_retryable_message("operation timed out"));
        assert!(is_retryable_message("Connection Refused")); // case insensitive

        assert!(!is_retryable_message("authentication failed"));
        assert!(!is_retryable_message("permission denied"));
        assert!(!is_retryable_message("success"));
    }

    #[test]
    fn is_permanent_failure_patterns() {
        assert!(is_permanent_failure("authentication failed"));
        assert!(is_permanent_failure("Permission Denied")); // case insensitive
        assert!(is_permanent_failure("invalid credentials"));
        assert!(is_permanent_failure("resource not found"));

        assert!(!is_permanent_failure("connection reset"));
        assert!(!is_permanent_failure("timeout"));
    }

    #[tokio::test]
    async fn retry_succeeds_on_third_attempt() {
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = attempts.clone();

        let cancel = CancellationToken::new();
        let policy = RetryPolicy::new(
            Duration::from_millis(1),
            Duration::from_millis(10),
            0.0,
            Some(5),
        );

        let result: Result<&str, RetryOutcome<TestError>> = retry_async(
            |_| {
                let attempts = attempts_clone.clone();
                async move {
                    let count = attempts.fetch_add(1, Ordering::SeqCst);
                    if count < 2 {
                        Err(TestError::Transient("not yet".into()))
                    } else {
                        Ok("success")
                    }
                }
            },
            is_transient,
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
        cancel.cancel();

        let policy = RetryPolicy::new(
            Duration::from_millis(100),
            Duration::from_secs(1),
            0.0,
            None,
        );

        let result: Result<(), RetryOutcome<TestError>> = retry_async(
            |_| async { Err(TestError::Transient("fail".into())) },
            is_transient,
            Duration::from_secs(1),
            policy,
            &cancel,
            "test",
        )
        .await;

        assert!(matches!(result, Err(RetryOutcome::Cancelled)));
    }

    #[tokio::test]
    async fn retry_stops_on_non_retryable_error() {
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = attempts.clone();
        let cancel = CancellationToken::new();

        let result: Result<(), RetryOutcome<TestError>> = retry_async(
            |_| {
                let attempts = attempts_clone.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err(TestError::Permanent("auth failed".into()))
                }
            },
            is_transient,
            Duration::from_secs(1),
            RetryPolicy::default(),
            &cancel,
            "test",
        )
        .await;

        assert!(matches!(result, Err(RetryOutcome::Failed(_))));
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "should stop after first non-retryable error"
        );
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
            Some(3),
        );

        let result: Result<(), RetryOutcome<TestError>> = retry_async(
            |_| {
                let attempts = attempts_clone.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err(TestError::Transient("always fail".into()))
                }
            },
            is_transient,
            Duration::from_secs(1),
            policy,
            &cancel,
            "test",
        )
        .await;

        match result {
            Err(RetryOutcome::Exhausted { attempts: n, .. }) => {
                assert_eq!(n, 3, "should report 3 attempts");
            }
            other => panic!("expected Exhausted, got {:?}", other),
        }
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            3,
            "should stop after max retries"
        );
    }

    #[tokio::test]
    async fn retry_preserves_last_error_on_exhaustion() {
        let cancel = CancellationToken::new();
        let policy = RetryPolicy::new(
            Duration::from_millis(1),
            Duration::from_millis(10),
            0.0,
            Some(2),
        );

        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        let result: Result<(), RetryOutcome<TestError>> = retry_async(
            |_| {
                let counter = counter_clone.clone();
                async move {
                    let n = counter.fetch_add(1, Ordering::SeqCst);
                    Err(TestError::Transient(format!("error {}", n)))
                }
            },
            is_transient,
            Duration::from_secs(1),
            policy,
            &cancel,
            "test",
        )
        .await;

        match result {
            Err(RetryOutcome::Exhausted { last_error, .. }) => {
                // Should have the error from the last (2nd) attempt
                match last_error {
                    TestError::Transient(msg) => assert_eq!(msg, "error 1"),
                    _ => panic!("wrong error type"),
                }
            }
            other => panic!("expected Exhausted, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn watchdog_returns_success() {
        let cancel = CancellationToken::new();

        let result: Result<&str, RetryOutcome<TestError>> = watchdog(
            async { Ok("done") },
            Duration::from_secs(1),
            &cancel,
            "test",
        )
        .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "done");
    }

    #[tokio::test]
    async fn watchdog_returns_timeout() {
        let cancel = CancellationToken::new();

        let result: Result<(), RetryOutcome<TestError>> = watchdog(
            async {
                tokio::time::sleep(Duration::from_secs(10)).await;
                Ok(())
            },
            Duration::from_millis(10),
            &cancel,
            "slow_op",
        )
        .await;

        assert!(matches!(result, Err(RetryOutcome::Timeout { .. })));
    }

    #[tokio::test]
    async fn watchdog_returns_cancelled() {
        let cancel = CancellationToken::new();
        cancel.cancel();

        let result: Result<(), RetryOutcome<TestError>> = watchdog(
            async {
                tokio::time::sleep(Duration::from_secs(10)).await;
                Ok(())
            },
            Duration::from_secs(1),
            &cancel,
            "test",
        )
        .await;

        assert!(matches!(result, Err(RetryOutcome::Cancelled)));
    }

    #[tokio::test]
    async fn watchdog_returns_error() {
        let cancel = CancellationToken::new();

        let result: Result<(), RetryOutcome<TestError>> = watchdog(
            async { Err(TestError::Permanent("auth failed".into())) },
            Duration::from_secs(1),
            &cancel,
            "test",
        )
        .await;

        assert!(matches!(result, Err(RetryOutcome::Failed(_))));
    }
}
