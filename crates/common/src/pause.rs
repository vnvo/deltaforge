//! Pause gate utilities for sources and sinks.
//!
//! This module provides standardized pause/resume functionality that allows
//! sources and sinks to be temporarily suspended without losing their position.
//!
//! # Use Cases
//!
//! - **Backpressure**: Pause source when sink is overwhelmed
//! - **Maintenance**: Pause for configuration changes
//! - **Resource management**: Temporarily release connections
//! - **Debugging**: Pause to inspect state
//!
//! # Example
//!
//! ```ignore
//! use common::pause_until_resumed;
//! use std::sync::atomic::{AtomicBool, Ordering};
//! use tokio::sync::Notify;
//! use tokio_util::sync::CancellationToken;
//!
//! async fn process_loop(
//!     cancel: &CancellationToken,
//!     paused: &AtomicBool,
//!     notify: &Notify,
//! ) {
//!     loop {
//!         // Check pause gate at the start of each iteration
//!         if !pause_until_resumed(cancel, paused, notify).await {
//!             break; // Cancelled
//!         }
//!
//!         // Do work...
//!     }
//! }
//! ```

use std::sync::atomic::{AtomicBool, Ordering};

use tokio::select;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::debug;

/// Wait until the source/sink is resumed or cancelled.
///
/// This is the standard pause gate pattern used by all sources and sinks.
/// When paused, it blocks until either:
/// - The component is resumed (returns `true`)
/// - The component is cancelled (returns `false`)
///
/// If not currently paused, returns immediately with `true`.
///
/// # Arguments
///
/// - `cancel`: Cancellation token for shutdown
/// - `paused`: Atomic flag indicating pause state
/// - `notify`: Notification channel for resume signal
///
/// # Returns
///
/// - `true` if resumed and should continue processing
/// - `false` if cancelled and should stop
///
/// # Example
///
/// ```ignore
/// loop {
///     if !pause_until_resumed(&cancel, &paused, &notify).await {
///         break; // Cancelled
///     }
///     
///     // Process next item...
/// }
/// ```
pub async fn pause_until_resumed(
    cancel: &CancellationToken,
    paused: &AtomicBool,
    notify: &Notify,
) -> bool {
    // Fast path: not paused
    if !paused.load(Ordering::SeqCst) {
        return true;
    }

    debug!("paused, waiting for resume or cancel");

    select! {
        _ = cancel.cancelled() => {
            debug!("cancelled while paused");
            false
        }
        _ = notify.notified() => {
            debug!("resumed");
            true
        }
    }
}

/// Extended pause gate with timeout support.
///
/// Similar to `pause_until_resumed`, but also wakes up periodically
/// for housekeeping (e.g., sending heartbeats while paused).
///
/// # Arguments
///
/// - `cancel`: Cancellation token for shutdown
/// - `paused`: Atomic flag indicating pause state
/// - `notify`: Notification channel for resume signal
/// - `timeout`: Maximum time to wait before returning for housekeeping
///
/// # Returns
///
/// - `PauseResult::Resumed` if resumed
/// - `PauseResult::Cancelled` if cancelled
/// - `PauseResult::Timeout` if timeout elapsed (still paused)
pub async fn pause_with_timeout(
    cancel: &CancellationToken,
    paused: &AtomicBool,
    notify: &Notify,
    timeout: std::time::Duration,
) -> PauseResult {
    // Fast path: not paused
    if !paused.load(Ordering::SeqCst) {
        return PauseResult::Resumed;
    }

    debug!("paused, waiting for resume, cancel, or timeout");

    select! {
        _ = cancel.cancelled() => {
            debug!("cancelled while paused");
            PauseResult::Cancelled
        }
        _ = notify.notified() => {
            debug!("resumed");
            PauseResult::Resumed
        }
        _ = tokio::time::sleep(timeout) => {
            debug!("pause timeout elapsed");
            PauseResult::Timeout
        }
    }
}

/// Result of a pause gate with timeout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PauseResult {
    /// Resumed - continue processing
    Resumed,
    /// Cancelled - should stop
    Cancelled,
    /// Timeout elapsed while still paused - do housekeeping
    Timeout,
}

impl PauseResult {
    /// Check if processing should continue.
    pub fn should_continue(&self) -> bool {
        matches!(self, Self::Resumed | Self::Timeout)
    }

    /// Check if the component was cancelled.
    pub fn is_cancelled(&self) -> bool {
        matches!(self, Self::Cancelled)
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn not_paused_returns_immediately() {
        let cancel = CancellationToken::new();
        let paused = AtomicBool::new(false);
        let notify = Notify::new();

        let result = pause_until_resumed(&cancel, &paused, &notify).await;
        assert!(result);
    }

    #[tokio::test]
    async fn cancelled_while_paused_returns_false() {
        let cancel = CancellationToken::new();
        let paused = AtomicBool::new(true);
        let notify = Notify::new();

        cancel.cancel();

        let result = pause_until_resumed(&cancel, &paused, &notify).await;
        assert!(!result);
    }

    #[tokio::test]
    async fn notify_wakes_paused_gate() {
        let cancel = CancellationToken::new();
        let paused = AtomicBool::new(true);
        let notify = Arc::new(Notify::new());
        let notify_clone = notify.clone();

        // Wake via notify after a short delay
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            notify_clone.notify_waiters();
        });

        let result = pause_until_resumed(&cancel, &paused, &notify).await;
        assert!(result);
    }

    #[tokio::test]
    async fn pause_with_timeout_returns_timeout_when_not_notified() {
        let cancel = CancellationToken::new();
        let paused = AtomicBool::new(true);
        let notify = Notify::new();

        let result = pause_with_timeout(
            &cancel,
            &paused,
            &notify,
            Duration::from_millis(10),
        )
        .await;
        assert_eq!(result, PauseResult::Timeout);
        assert!(result.should_continue()); // Timeout still means continue
    }

    #[tokio::test]
    async fn pause_with_timeout_cancelled() {
        let cancel = CancellationToken::new();
        let paused = AtomicBool::new(true);
        let notify = Notify::new();

        cancel.cancel();

        let result = pause_with_timeout(
            &cancel,
            &paused,
            &notify,
            Duration::from_secs(10),
        )
        .await;
        assert_eq!(result, PauseResult::Cancelled);
        assert!(!result.should_continue());
    }
}
