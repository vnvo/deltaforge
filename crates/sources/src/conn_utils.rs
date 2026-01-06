use std::future::Future;
use std::time::Instant;
use std::{borrow::Cow, time::Duration};

use deltaforge_core::{SourceError, SourceResult};
use rand::{Rng, rng};
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

/// standard exponential backoff with jitter.
#[derive(Clone, Debug)]
pub struct RetryPolicy {
    /// first backoffice interval
    pub initial: Duration,

    /// maximum backoff interval
    pub max: Duration,

    /// jitter factor in percentage
    pub jitter: f64,

    /// stop after N attempts. `None` = forever until cancelled
    pub max_retries: Option<u32>,

    /// Current backoff (for stateful reties)
    current_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            initial: Duration::from_millis(1000),
            max: Duration::from_secs(60),
            jitter: 0.2,
            max_retries: None, // retry forever by default
            current_backoff: Duration::from_millis(1000),
        }
    }
}

impl RetryPolicy {
    /// Get next backoff duration and advance internal state.
    pub fn next_backoff(&mut self) -> Duration {
        let current = self.current_backoff;
        // Exponential increase, capped at max
        self.current_backoff = current.saturating_mul(2).min(self.max);

        // Apply jitter to returned value
        let j = 1.0 + rand::rng().random_range(-self.jitter..self.jitter);
        current.mul_f64(j).max(Duration::from_nanos(1))
    }

    /// Reset backoff to initial value (call after successful operation).
    pub fn reset(&mut self) {
        self.current_backoff = self.initial;
    }
}

/// Generic async retry helper with attempt timeout and cancellation.
///
/// - `op(attempt)` runs the operation and returns `Result<T, E>`.
/// - `classify(&E)` maps the error into a `SourceError`.
/// - `should_retry(&SourceError)` decides if the classified error is retryable.
/// - `attempt_timeout` is a per-attempt timeout.
/// - Backoff between attempts is managed by `policy`.
pub async fn retry_async<T, E, Fut, Op, Classify, Retryable>(
    mut op: Op,
    classify: Classify,
    should_retry: Retryable,
    attempt_timeout: Duration,
    mut policy: RetryPolicy,
    cancel: &CancellationToken,
    label: &'static str,
) -> SourceResult<T>
where
    Op: FnMut(u32) -> Fut,
    Fut: Future<Output = Result<T, E>>,
    Classify: Fn(E) -> SourceError,
    Retryable: Fn(&SourceError) -> bool,
{
    let mut attempt = 0u32;
    let mut backoff = policy.initial;

    loop {
        if cancel.is_cancelled() {
            return Err(SourceError::Cancelled);
        }

        attempt += 1;

        // run one attempt with a hrad limit
        let res = timeout(attempt_timeout, op(attempt));
        match res.await {
            Ok(Ok(ok)) => {
                debug!(label, attempt, "operation succeeded");
                return Ok(ok);
            }
            Ok(Err(err)) => {
                let se = classify(err);
                if !should_retry(&se)
                    || policy.max_retries.is_some_and(|m| attempt > m)
                {
                    return Err(se);
                }

                warn!(label, attempt, ?backoff, error=%se, "attempt failed; will retry");
            }
            Err(_) => {
                let se = SourceError::Timeout {
                    action: Cow::Borrowed(label),
                };
                if !should_retry(&se)
                    || policy.max_retries.is_some_and(|m| attempt > m)
                {
                    return Err(se);
                }

                warn!(label, attempt, ?backoff, "attempt timedout; will retry");
            }
        }

        // sleep with cancellation before the next attempt
        let until = Instant::now() + backoff;
        tokio::select! {
            _ = cancel.cancelled() => return Err(SourceError::Cancelled),
            _ = sleep(backoff) => {}
        }

        if Instant::now() > until {
            debug!(label, attempt, "backoff sleep complete");
        }
        backoff = policy.next_backoff();
    }
}

pub async fn watchdog<T, E, Fut>(
    op: Fut,
    inactivity: Duration,
    cancel: &CancellationToken,
    label: &'static str,
) -> SourceResult<T>
where
    Fut: Future<Output = Result<T, E>>,
    E: Into<anyhow::Error>,
{
    tokio::select! {
        _ = cancel.cancelled() => Err(SourceError::Cancelled),
        res = timeout(inactivity, op) => {
            match res {
                Ok(Ok(val)) => Ok(val),
                Ok(Err(e)) => Err(SourceError::Other(e.into())),
                Err(_) => Err(SourceError::Timeout { action: Cow::Borrowed(label) }),
            }
        }
    }
}

/// default retryability check for connectivity related errors
pub fn retryable_connect(err: &SourceError) -> bool {
    matches!(
        err,
        SourceError::Timeout { .. }
            | SourceError::Connect { .. }
            | SourceError::Io(_)
    )
}

/// default retryability check for streaming/read errors
pub fn retryable_stream(err: &SourceError) -> bool {
    match err {
        SourceError::Timeout { .. } => true,
        SourceError::Connect { .. } => true,
        SourceError::Io(_) => true,
        SourceError::Other(inner) => {
            let s = inner.to_string();
            s.contains("io error")
                || s.contains("EOF")
                || s.contains("Connection reset")
                || s.contains("Broken pipe")
                || s.contains("timeout")
        }
        _ => false,
    }
}
