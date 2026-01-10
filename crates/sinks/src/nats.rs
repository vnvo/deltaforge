//! NATS JetStream sink implementation.
//!
//! This sink writes CDC events to NATS JetStream for durable, distributed
//! event streaming with at-least-once delivery guarantees.
//!
//! # Features
//!
//! - **JetStream**: Uses NATS JetStream for durable message delivery
//! - **Automatic reconnection**: Exponential backoff with jitter on failures
//! - **Batch optimization**: Publishes batches concurrently for throughput
//! - **Graceful shutdown**: Respects cancellation tokens during operations
//!
//! # Configuration
//!
//! ```yaml
//! sinks:
//!   - nats:
//!       id: nats-events
//!       url: nats://localhost:4222
//!       subject: deltaforge.events
//!       stream: DELTAFORGE          # Optional: JetStream stream name
//!       required: true
//!       send_timeout_secs: 5        # Per-message timeout
//!       batch_timeout_secs: 30      # Batch publish timeout
//!       connect_timeout_secs: 10    # Connection establishment timeout
//!       credentials_file: /path/to/creds.json  # Optional
//! ```

use std::time::Duration;

use anyhow::Context;
use async_nats::Client;
use async_nats::jetstream::{self, Context as JetStreamContext};
use async_trait::async_trait;
use common::{RetryOutcome, RetryPolicy, retry_async};
use deltaforge_config::NatsSinkCfg;
use deltaforge_core::{Event, Sink, SinkError, SinkResult};
use futures::future::try_join_all;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument, warn};

// =============================================================================
// Constants
// =============================================================================

/// Default timeout for individual publish operations.
const DEFAULT_SEND_TIMEOUT: Duration = Duration::from_secs(5);

/// Default timeout for batch publish operations.
const DEFAULT_BATCH_TIMEOUT: Duration = Duration::from_secs(30);

/// Default timeout for connection establishment.
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

// =============================================================================
// NATS Sink
// =============================================================================

/// NATS JetStream sink with connection management and retry logic.
pub struct NatsSink {
    id: String,
    cfg: NatsSinkCfg,
    subject: String,
    /// Cached NATS client and JetStream context with RwLock for interior mutability.
    /// Using Option to allow lazy initialization and reconnection.
    client: RwLock<Option<ClientState>>,
    /// Cancellation token for graceful shutdown.
    cancel: CancellationToken,
    /// Timeouts (extracted from config or defaults).
    send_timeout: Duration,
    batch_timeout: Duration,
    connect_timeout: Duration,
}

/// Internal state holding the NATS client and JetStream context.
struct ClientState {
    _client: Client,
    jetstream: JetStreamContext,
}

impl NatsSink {
    /// Create a new NATS sink.
    ///
    /// # Arguments
    ///
    /// * `cfg` - NATS sink configuration
    /// * `cancel` - Cancellation token for graceful shutdown
    ///
    /// # Example
    ///
    /// ```ignore
    /// let sink = NatsSink::new(&cfg, cancel.clone())?;
    /// ```
    #[instrument(skip_all, fields(sink_id = %cfg.id, subject = %cfg.subject))]
    pub fn new(
        cfg: &NatsSinkCfg,
        cancel: CancellationToken,
    ) -> anyhow::Result<Self> {
        // Extract timeouts from config or use defaults
        let send_timeout = cfg
            .send_timeout_secs
            .map(|s| Duration::from_secs(s as u64))
            .unwrap_or(DEFAULT_SEND_TIMEOUT);

        let batch_timeout = cfg
            .batch_timeout_secs
            .map(|s| Duration::from_secs(s as u64))
            .unwrap_or(DEFAULT_BATCH_TIMEOUT);

        let connect_timeout = cfg
            .connect_timeout_secs
            .map(|s| Duration::from_secs(s as u64))
            .unwrap_or(DEFAULT_CONNECT_TIMEOUT);

        info!(
            url = %redact_nats_url(&cfg.url),
            subject = %cfg.subject,
            stream = ?cfg.stream,
            send_timeout_ms = send_timeout.as_millis(),
            batch_timeout_ms = batch_timeout.as_millis(),
            "nats sink created"
        );

        Ok(Self {
            id: cfg.id.clone(),
            cfg: cfg.clone(),
            subject: cfg.subject.clone(),
            client: RwLock::new(None),
            cancel,
            send_timeout,
            batch_timeout,
            connect_timeout,
        })
    }

    /// Get or establish a NATS connection with retry logic.
    ///
    /// This method implements connection caching by storing the client
    /// and only reconnecting when necessary.
    async fn get_client(&self) -> SinkResult<JetStreamContext> {
        // Fast path: check if we have a valid cached connection
        {
            let guard = self.client.read().await;
            if let Some(ref state) = *guard {
                return Ok(state.jetstream.clone());
            }
        }

        // Slow path: need to establish connection
        let mut guard = self.client.write().await;

        // Double-check after acquiring write lock (another task may have connected)
        if let Some(ref state) = *guard {
            return Ok(state.jetstream.clone());
        }

        // Establish new connection with retry
        let state = self.connect_with_retry().await?;
        let jetstream = state.jetstream.clone();
        *guard = Some(state);

        Ok(jetstream)
    }

    /// Establish connection with exponential backoff retry.
    async fn connect_with_retry(&self) -> SinkResult<ClientState> {
        let policy = RetryPolicy::new(
            Duration::from_millis(100),
            Duration::from_secs(10),
            0.2,     // 20% jitter
            Some(5), // Max 5 attempts for connection
        );

        let cfg = self.cfg.clone();
        let url_redacted = redact_nats_url(&cfg.url);

        let result = retry_async(
            |attempt| {
                let cfg = cfg.clone();
                async move {
                    debug!(attempt, "attempting nats connection");
                    connect_nats(&cfg)
                        .await
                        .map_err(|e| NatsRetryError::Connect(e.to_string()))
                }
            },
            |e| e.is_retryable(),
            self.connect_timeout,
            policy,
            &self.cancel,
            "nats_connect",
        )
        .await;

        match result {
            Ok(state) => {
                info!(url = %url_redacted, "nats connection established");
                Ok(state)
            }
            Err(outcome) => {
                let err = match outcome {
                    RetryOutcome::Cancelled => SinkError::Connect {
                        details: "cancelled during connection".into(),
                    },
                    RetryOutcome::Exhausted {
                        attempts,
                        last_error,
                    } => SinkError::Connect {
                        details: format!(
                            "failed after {} attempts: {}",
                            attempts, last_error
                        )
                        .into(),
                    },
                    RetryOutcome::Timeout { action } => SinkError::Connect {
                        details: format!("connection timeout: {}", action)
                            .into(),
                    },
                    RetryOutcome::Failed(e) => SinkError::Connect {
                        details: format!("permanent failure: {}", e).into(),
                    },
                };
                Err(err)
            }
        }
    }

    /// Invalidate the cached connection (e.g., after an error).
    async fn invalidate_connection(&self) {
        let mut guard = self.client.write().await;
        *guard = None;
        debug!("nats connection invalidated");
    }

    /// Publish a single message with timeout.
    async fn publish_single(
        &self,
        jetstream: &JetStreamContext,
        payload: &[u8],
    ) -> Result<(), NatsRetryError> {
        let result = tokio::time::timeout(self.send_timeout, async {
            jetstream
                .publish(self.subject.clone(), payload.to_vec().into())
                .await
                .map_err(|e| NatsRetryError::Publish(e.to_string()))?
                .await
                .map_err(|e| NatsRetryError::Publish(e.to_string()))
        })
        .await;

        match result {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(NatsRetryError::Timeout),
        }
    }
}

/// Establish a NATS connection and JetStream context.
async fn connect_nats(cfg: &NatsSinkCfg) -> anyhow::Result<ClientState> {
    let mut connect_opts = async_nats::ConnectOptions::new();

    // Apply credentials if provided
    if let Some(ref creds_file) = cfg.credentials_file {
        connect_opts = connect_opts
            .credentials_file(creds_file)
            .await
            .with_context(|| {
                format!("failed to load credentials from {}", creds_file)
            })?;
    }

    // Apply username/password if provided
    if let (Some(user), Some(pass)) = (&cfg.username, &cfg.password) {
        connect_opts =
            connect_opts.user_and_password(user.clone(), pass.clone());
    }

    // Apply token if provided
    if let Some(ref token) = cfg.token {
        connect_opts = connect_opts.token(token.clone());
    }

    let client = connect_opts.connect(&cfg.url).await.with_context(|| {
        format!("failed to connect to NATS at {}", redact_nats_url(&cfg.url))
    })?;

    let jetstream = jetstream::new(client.clone());

    // Optionally verify stream exists if stream name is provided
    if let Some(ref stream_name) = cfg.stream {
        // Try to get stream info to verify it exists
        match jetstream.get_stream(stream_name).await {
            Ok(_) => {
                debug!(stream = %stream_name, "verified jetstream stream exists");
            }
            Err(e) => {
                warn!(
                    stream = %stream_name,
                    error = %e,
                    "jetstream stream not found - messages may not be persisted"
                );
            }
        }
    }

    Ok(ClientState {
        _client: client,
        jetstream,
    })
}

#[async_trait]
impl Sink for NatsSink {
    fn id(&self) -> &str {
        &self.id
    }

    fn required(&self) -> bool {
        self.cfg.required.unwrap_or(true)
    }

    #[instrument(skip_all, fields(sink_id = %self.id, event_id = %event.event_id))]
    async fn send(&self, event: &Event) -> SinkResult<()> {
        // Pre-serialize to separate serialization errors from network errors
        let payload = serde_json::to_vec(event)?;

        // Retry loop for transient failures
        let policy = RetryPolicy::new(
            Duration::from_millis(50),
            Duration::from_secs(5),
            0.2,
            Some(3),
        );

        let result = retry_async(
            |attempt| {
                let payload = payload.clone();
                async move {
                    debug!(attempt, "sending event to nats");

                    let jetstream = self
                        .get_client()
                        .await
                        .map_err(|e| NatsRetryError::Connect(e.to_string()))?;

                    match self.publish_single(&jetstream, &payload).await {
                        Ok(_) => Ok(()),
                        Err(e) => {
                            // Invalidate connection on error to force reconnect
                            self.invalidate_connection().await;
                            Err(e)
                        }
                    }
                }
            },
            |e| e.is_retryable(),
            self.send_timeout,
            policy,
            &self.cancel,
            "nats_send",
        )
        .await;

        match result {
            Ok(_) => {
                debug!(subject = %self.subject, "event sent to nats");
                Ok(())
            }
            Err(outcome) => Err(outcome_to_sink_error(outcome)),
        }
    }

    #[instrument(skip_all, fields(sink_id = %self.id, count = events.len()))]
    async fn send_batch(&self, events: &[Event]) -> SinkResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        // Pre-serialize all payloads to separate serialization from network errors
        let serialized: Vec<Vec<u8>> = events
            .iter()
            .map(serde_json::to_vec)
            .collect::<Result<Vec<_>, _>>()?;

        // Retry loop for transient failures
        let policy = RetryPolicy::new(
            Duration::from_millis(100),
            Duration::from_secs(10),
            0.2,
            Some(3),
        );

        let result = retry_async(
            |attempt| {
                let serialized = serialized.clone();
                async move {
                    debug!(
                        attempt,
                        count = serialized.len(),
                        "sending batch to nats"
                    );

                    let jetstream = self
                        .get_client()
                        .await
                        .map_err(|e| NatsRetryError::Connect(e.to_string()))?;

                    // Publish all messages concurrently
                    let futures: Vec<_> = serialized
                        .iter()
                        .map(|payload| {
                            let subject = self.subject.clone();
                            let jetstream = jetstream.clone();
                            let payload = payload.clone();
                            async move {
                                jetstream
                                    .publish(subject, payload.into())
                                    .await
                                    .map_err(|e| {
                                        NatsRetryError::Publish(e.to_string())
                                    })?
                                    .await
                                    .map_err(|e| {
                                        NatsRetryError::Publish(e.to_string())
                                    })
                            }
                        })
                        .collect();

                    let result = tokio::time::timeout(
                        self.batch_timeout,
                        try_join_all(futures),
                    )
                    .await;

                    match result {
                        Ok(Ok(_)) => Ok(()),
                        Ok(Err(e)) => {
                            self.invalidate_connection().await;
                            Err(e)
                        }
                        Err(_) => {
                            self.invalidate_connection().await;
                            Err(NatsRetryError::Timeout)
                        }
                    }
                }
            },
            |e| e.is_retryable(),
            self.batch_timeout,
            policy,
            &self.cancel,
            "nats_batch",
        )
        .await;

        match result {
            Ok(_) => {
                debug!(subject = %self.subject, count = events.len(), "batch sent to nats");
                Ok(())
            }
            Err(outcome) => Err(outcome_to_sink_error(outcome)),
        }
    }
}

// =============================================================================
// Error Handling
// =============================================================================

/// Internal error type for retry classification.
#[derive(Debug, Clone)]
enum NatsRetryError {
    /// Connection establishment failed.
    Connect(String),
    /// Publish operation failed.
    Publish(String),
    /// Operation timed out.
    Timeout,
}

impl std::fmt::Display for NatsRetryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connect(msg) => write!(f, "connection error: {}", msg),
            Self::Publish(msg) => write!(f, "publish error: {}", msg),
            Self::Timeout => write!(f, "operation timed out"),
        }
    }
}

impl NatsRetryError {
    /// Determine if this error is retryable.
    fn is_retryable(&self) -> bool {
        match self {
            // Connection errors are generally retryable unless auth-related
            Self::Connect(msg) => !is_permanent_failure(msg),
            // Publish errors depend on the message
            Self::Publish(msg) => !is_permanent_failure(msg),
            // Timeouts are always retryable
            Self::Timeout => true,
        }
    }
}

/// Check if an error message indicates a permanent (non-retryable) failure.
fn is_permanent_failure(msg: &str) -> bool {
    let lower = msg.to_lowercase();
    lower.contains("authentication")
        || lower.contains("authorization")
        || lower.contains("permission denied")
        || lower.contains("invalid credentials")
        || lower.contains("not authorized")
        || lower.contains("no responders") // Subject doesn't exist
}

/// Redact sensitive information from NATS URL for logging.
fn redact_nats_url(url: &str) -> String {
    // Handle nats://user:pass@host:port format
    if let Some(at_idx) = url.find('@') {
        if let Some(colon_idx) = url[..at_idx].rfind(':') {
            // Find the protocol separator
            if let Some(proto_end) = url.find("://") {
                let proto_len = proto_end + 3;
                if colon_idx > proto_len {
                    return format!(
                        "{}***{}",
                        &url[..colon_idx + 1],
                        &url[at_idx..]
                    );
                }
            }
        }
    }
    url.to_string()
}

/// Convert a RetryOutcome to SinkError.
fn outcome_to_sink_error(outcome: RetryOutcome<NatsRetryError>) -> SinkError {
    match outcome {
        RetryOutcome::Cancelled => {
            SinkError::Other(anyhow::anyhow!("operation cancelled"))
        }
        RetryOutcome::Timeout { action } => SinkError::Backpressure {
            details: format!("timeout: {}", action).into(),
        },
        RetryOutcome::Exhausted {
            attempts,
            last_error,
        } => SinkError::Connect {
            details: format!(
                "exhausted after {} attempts: {}",
                attempts, last_error
            )
            .into(),
        },
        RetryOutcome::Failed(e) => SinkError::Connect {
            details: e.to_string().into(),
        },
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permanent_failures_are_not_retryable() {
        assert!(is_permanent_failure("Authentication failed"));
        assert!(is_permanent_failure("Authorization error"));
        assert!(is_permanent_failure("not authorized to publish"));
        assert!(is_permanent_failure("Permission denied"));

        assert!(!is_permanent_failure("connection reset"));
        assert!(!is_permanent_failure("broken pipe"));
        assert!(!is_permanent_failure("timeout"));
    }

    #[test]
    fn retry_error_classification() {
        let timeout = NatsRetryError::Timeout;
        assert!(timeout.is_retryable());

        let connection_reset =
            NatsRetryError::Connect("connection reset by peer".into());
        assert!(connection_reset.is_retryable());

        let auth_failed =
            NatsRetryError::Connect("Authentication failed".into());
        assert!(!auth_failed.is_retryable());
    }

    #[test]
    fn url_redaction() {
        // No credentials
        assert_eq!(
            redact_nats_url("nats://localhost:4222"),
            "nats://localhost:4222"
        );

        // With credentials
        assert_eq!(
            redact_nats_url("nats://user:secret@localhost:4222"),
            "nats://user:***@localhost:4222"
        );
    }
}
