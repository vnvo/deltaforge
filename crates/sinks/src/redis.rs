//! Redis Streams sink implementation.
//!
//! This sink writes CDC events to Redis Streams using pipelining for
//! efficient batch operations.
//!
//! # Features
//!
//! - **Connection pooling**: Maintains a persistent multiplexed connection
//! - **Automatic reconnection**: Exponential backoff with jitter on failures
//! - **Pipelining**: Batch operations use Redis pipelines for single round-trip
//! - **Graceful shutdown**: Respects cancellation tokens during operations
//! - **Configurable envelope**: Native, Debezium, or CloudEvents
//! - **Configurable encoding**: JSON (Avro/Protobuf planned)
//!
//! # Configuration
//!
//! ```yaml
//! sinks:
//!   - redis:
//!       id: redis-events
//!       uri: redis://localhost:6379
//!       stream: deltaforge-events
//!       envelope: native            # native | debezium | cloudevents
//!       encoding: json              # json (avro, protobuf planned)
//!       required: true
//!       send_timeout_secs: 5        # Per-message timeout
//!       batch_timeout_secs: 30      # Batch pipeline timeout
//!       connect_timeout_secs: 10    # Connection establishment timeout
//! ```

use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use common::{RetryOutcome, RetryPolicy, redact_url_password, retry_async};
use deltaforge_config::RedisSinkCfg;
use deltaforge_core::encoding::EncodingType;
use deltaforge_core::envelope::Envelope;
use deltaforge_core::{Event, Sink, SinkError, SinkResult};
use redis::aio::MultiplexedConnection;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument, warn};

// =============================================================================
// Constants
// =============================================================================

/// Default timeout for individual send operations.
const DEFAULT_SEND_TIMEOUT: Duration = Duration::from_secs(5);

/// Default timeout for batch pipeline operations.
const DEFAULT_BATCH_TIMEOUT: Duration = Duration::from_secs(30);

/// Default timeout for connection establishment.
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

// =============================================================================
// Redis Sink
// =============================================================================

/// Redis Streams sink with connection pooling and retry logic.
pub struct RedisSink {
    id: String,
    cfg: RedisSinkCfg,
    client: redis::Client,
    stream: String,
    /// Envelope for wrapping events (Native, Debezium, CloudEvents)
    envelope: Box<dyn Envelope>,
    /// Encoding type (currently just JSON)
    #[allow(dead_code)]
    encoding: EncodingType,
    /// Cached multiplexed connection with RwLock for interior mutability.
    /// Using Option to allow lazy initialization and reconnection.
    conn: RwLock<Option<MultiplexedConnection>>,
    /// Cancellation token for graceful shutdown.
    cancel: CancellationToken,
    /// Timeouts (extracted from config or defaults).
    send_timeout: Duration,
    batch_timeout: Duration,
    connect_timeout: Duration,
}

impl RedisSink {
    /// Create a new Redis sink.
    ///
    /// # Arguments
    ///
    /// * `cfg` - Redis sink configuration
    /// * `cancel` - Cancellation token for graceful shutdown
    ///
    /// # Example
    ///
    /// ```ignore
    /// let sink = RedisSink::new(&cfg, cancel.clone())?;
    /// ```
    #[instrument(skip_all, fields(sink_id = %cfg.id, stream = %cfg.stream))]
    pub fn new(
        cfg: &RedisSinkCfg,
        cancel: CancellationToken,
    ) -> anyhow::Result<Self> {
        let client =
            redis::Client::open(cfg.uri.clone()).with_context(|| {
                format!("invalid redis URI: {}", redact_url_password(&cfg.uri))
            })?;

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

        // Build envelope and encoding from config
        let envelope_type = cfg.envelope.to_envelope_type();
        let encoding_type = cfg.encoding.to_encoding_type();

        info!(
            uri = %redact_url_password(&cfg.uri),
            envelope = %envelope_type.name(),
            encoding = encoding_type.name(),
            send_timeout_ms = send_timeout.as_millis(),
            batch_timeout_ms = batch_timeout.as_millis(),
            "redis sink created"
        );

        Ok(Self {
            id: cfg.id.clone(),
            cfg: cfg.clone(),
            client,
            stream: cfg.stream.clone(),
            envelope: envelope_type.build(),
            encoding: encoding_type,
            conn: RwLock::new(None),
            cancel,
            send_timeout,
            batch_timeout,
            connect_timeout,
        })
    }

    /// Serialize event using configured envelope.
    fn serialize_event(&self, event: &Event) -> SinkResult<Vec<u8>> {
        let envelope = self.envelope.wrap(event).map_err(|e| {
            SinkError::Serialization {
                details: e.to_string().into(),
            }
        })?;

        self.encoding
            .encode(&envelope)
            .map(|b| b.to_vec())
            .map_err(|e| SinkError::Serialization {
                details: e.to_string().into(),
            })
    }

    /// Get or establish a multiplexed connection with retry logic.
    ///
    /// This method implements connection pooling by caching the connection
    /// and only reconnecting when necessary.
    async fn get_connection(&self) -> SinkResult<MultiplexedConnection> {
        // Fast path: check if we have a valid cached connection
        {
            let guard = self.conn.read().await;
            if let Some(ref conn) = *guard {
                return Ok(conn.clone());
            }
        }

        // Slow path: need to establish connection
        let mut guard = self.conn.write().await;

        // Double-check after acquiring write lock (another task may have connected)
        if let Some(ref conn) = *guard {
            return Ok(conn.clone());
        }

        // Establish new connection with retry
        let conn = self.connect_with_retry().await?;
        *guard = Some(conn.clone());

        Ok(conn)
    }

    /// Establish connection with exponential backoff retry.
    async fn connect_with_retry(&self) -> SinkResult<MultiplexedConnection> {
        let policy = RetryPolicy::new(
            Duration::from_millis(100),
            Duration::from_secs(10),
            0.2,     // 20% jitter
            Some(5), // Max 5 attempts for connection
        );

        let client = self.client.clone();
        let uri_redacted = redact_url_password(&self.cfg.uri);

        let result = retry_async(
            |attempt| {
                let client = client.clone();
                async move {
                    debug!(attempt, "attempting redis connection");
                    client
                        .get_multiplexed_async_connection()
                        .await
                        .map_err(|e| RedisRetryError::Connect(e.to_string()))
                }
            },
            |e| e.is_retryable(),
            self.connect_timeout,
            policy,
            &self.cancel,
            "redis_connect",
        )
        .await;

        match result {
            Ok(conn) => {
                info!(uri = %uri_redacted, "redis connection established");
                Ok(conn)
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
        let mut guard = self.conn.write().await;
        *guard = None;
        debug!("redis connection invalidated");
    }

    /// Execute a single XADD command with timeout.
    async fn xadd_single(
        &self,
        conn: &mut MultiplexedConnection,
        event_id: &str,
        payload: &[u8],
    ) -> Result<(), RedisRetryError> {
        let result = tokio::time::timeout(self.send_timeout, async {
            redis::cmd("XADD")
                .arg(&self.stream)
                .arg("*")
                .arg("event_id")
                .arg(event_id)
                .arg("df-event")
                .arg(payload)
                .query_async::<()>(conn)
                .await
        })
        .await;

        match result {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(RedisRetryError::Command(e.to_string())),
            Err(_) => Err(RedisRetryError::Timeout),
        }
    }

    /// Execute a batch of XADD commands using a pipeline.
    async fn execute_pipeline(
        &self,
        conn: &mut MultiplexedConnection,
        items: &[(String, Vec<u8>)],
    ) -> Result<(), RedisRetryError> {
        let result = tokio::time::timeout(self.batch_timeout, async {
            let mut pipe = redis::pipe();

            for (event_id, payload) in items {
                pipe.cmd("XADD")
                    .arg(&self.stream)
                    .arg("*")
                    .arg("event_id")
                    .arg(event_id)
                    .arg("df-event")
                    .arg(payload);
            }

            pipe.query_async::<()>(conn).await
        })
        .await;

        match result {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(RedisRetryError::Command(e.to_string())),
            Err(_) => Err(RedisRetryError::Timeout),
        }
    }
}

#[async_trait]
impl Sink for RedisSink {
    fn id(&self) -> &str {
        &self.id
    }

    fn required(&self) -> bool {
        self.cfg.required.unwrap_or(true)
    }

    #[instrument(skip_all, fields(sink_id = %self.id))]
    async fn send(&self, event: &Event) -> SinkResult<()> {
        // Serialize using configured envelope
        let payload = self.serialize_event(event)?;
        let event_id =
            event.event_id.map(|id| id.to_string()).unwrap_or_default();

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
                let event_id = event_id.clone();
                async move {
                    debug!(attempt, "sending event to redis");

                    let mut conn = self
                        .get_connection()
                        .await
                        .map_err(|e| RedisRetryError::Connect(e.to_string()))?;

                    match self.xadd_single(&mut conn, &event_id, &payload).await
                    {
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
            "redis_send",
        )
        .await;

        match result {
            Ok(_) => {
                debug!(stream = %self.stream, "event sent to redis");
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

        // Pre-serialize all payloads using configured envelope
        let serialized: Vec<(String, Vec<u8>)> = events
            .iter()
            .map(|e| {
                Ok((
                    e.event_id.map(|id| id.to_string()).unwrap_or_default(),
                    self.serialize_event(e)?,
                ))
            })
            .collect::<Result<Vec<_>, SinkError>>()?;

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
                        "sending batch to redis"
                    );

                    let mut conn = self
                        .get_connection()
                        .await
                        .map_err(|e| RedisRetryError::Connect(e.to_string()))?;

                    match self.execute_pipeline(&mut conn, &serialized).await {
                        Ok(_) => Ok(()),
                        Err(e) => {
                            self.invalidate_connection().await;
                            Err(e)
                        }
                    }
                }
            },
            |e| e.is_retryable(),
            self.batch_timeout,
            policy,
            &self.cancel,
            "redis_batch",
        )
        .await;

        match result {
            Ok(_) => {
                debug!(stream = %self.stream, count = events.len(), "batch sent to redis");
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
enum RedisRetryError {
    /// Connection establishment failed.
    Connect(String),
    /// Redis command failed.
    Command(String),
    /// Operation timed out.
    Timeout,
}

impl std::fmt::Display for RedisRetryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connect(msg) => write!(f, "connection error: {}", msg),
            Self::Command(msg) => write!(f, "command error: {}", msg),
            Self::Timeout => write!(f, "operation timed out"),
        }
    }
}

impl RedisRetryError {
    /// Determine if this error is retryable.
    fn is_retryable(&self) -> bool {
        match self {
            // Connection errors are generally retryable
            Self::Connect(msg) => !is_permanent_failure(msg),
            // Command errors depend on the message
            Self::Command(msg) => !is_permanent_failure(msg),
            // Timeouts are always retryable
            Self::Timeout => true,
        }
    }
}

/// Check if an error message indicates a permanent (non-retryable) failure.
fn is_permanent_failure(msg: &str) -> bool {
    let lower = msg.to_lowercase();
    lower.contains("authentication")
        || lower.contains("noauth")
        || lower.contains("wrongpass")
        || lower.contains("permission denied")
        || lower.contains("invalid")
}

/// Convert a RetryOutcome to SinkError.
fn outcome_to_sink_error(outcome: RetryOutcome<RedisRetryError>) -> SinkError {
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
        assert!(is_permanent_failure("NOAUTH Authentication required"));
        assert!(is_permanent_failure("WRONGPASS invalid password"));
        assert!(is_permanent_failure("Permission denied"));

        assert!(!is_permanent_failure("connection reset"));
        assert!(!is_permanent_failure("broken pipe"));
    }

    #[test]
    fn retry_error_classification() {
        let timeout = RedisRetryError::Timeout;
        assert!(timeout.is_retryable());

        let connection_reset =
            RedisRetryError::Connect("connection reset by peer".into());
        assert!(connection_reset.is_retryable());

        let auth_failed =
            RedisRetryError::Connect("NOAUTH Authentication required".into());
        assert!(!auth_failed.is_retryable());
    }
}
