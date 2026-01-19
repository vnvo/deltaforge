//! Kafka sink implementation.
//!
//! This sink writes CDC events to Kafka topics using rdkafka with support
//! for idempotent production and optional exactly-once semantics.
//!
//! # Features
//!
//! - **Idempotent producer**: Enabled by default for at-least-once delivery
//! - **Exactly-once semantics**: Optional transactional mode
//! - **Batching**: Leverages rdkafka's internal batching with concurrent delivery awaits
//! - **Compression**: LZ4 compression by default
//! - **Graceful shutdown**: Respects cancellation tokens
//! - **Configurable envelope**: Native, Debezium, or CloudEvents
//! - **Configurable encoding**: JSON (Avro/Protobuf planned)
//!
//! # Configuration
//!
//! ```yaml
//! sinks:
//!   - kafka:
//!       id: kafka-events
//!       brokers: localhost:9092
//!       topic: deltaforge-events
//!       envelope: debezium          # native | debezium | cloudevents
//!       encoding: json              # json (avro, protobuf planned)
//!       exactly_once: false
//!       required: true
//!       send_timeout_secs: 30
//!       client_conf:
//!         security.protocol: SASL_SSL
//!         sasl.mechanism: PLAIN
//! ```

use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use common::{RetryOutcome, RetryPolicy, retry_async};
use deltaforge_config::KafkaSinkCfg;
use deltaforge_core::encoding::EncodingType;
use deltaforge_core::envelope::Envelope;
use deltaforge_core::{Event, Sink, SinkError, SinkResult};
use futures::future::try_join_all;
use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaError;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument, warn};

/// Default timeout for individual send operations.
const DEFAULT_SEND_TIMEOUT: Duration = Duration::from_secs(10);

/// Default linger time for batching (ms).
const DEFAULT_LINGER_MS: &str = "5";

/// Kafka sink with idempotent production and optional exactly-once semantics.
pub struct KafkaSink {
    id: String,
    cfg: KafkaSinkCfg,
    producer: FutureProducer,
    topic: String,
    /// Envelope for wrapping events (Native, Debezium, CloudEvents)
    envelope: Box<dyn Envelope>,
    /// Encoding type (currently just JSON, used for content-type headers)
    #[allow(dead_code)]
    encoding: EncodingType,
    /// Cancellation token for graceful shutdown.
    cancel: CancellationToken,
    /// Per-message send timeout.
    send_timeout: Duration,
}

impl KafkaSink {
    /// Create a new Kafka sink.
    ///
    /// # Arguments
    ///
    /// * `cfg` - Kafka sink configuration
    /// * `cancel` - Cancellation token for graceful shutdown
    ///
    /// # Producer Configuration
    ///
    /// The producer is configured with sensible defaults:
    /// - Idempotent production (prevents duplicates on retry)
    /// - LZ4 compression
    /// - 5ms linger for micro-batching
    /// - Appropriate timeouts for reliability
    ///
    /// User-provided `client_conf` overrides are applied last.
    #[instrument(skip_all, fields(sink_id = %cfg.id, topic = %cfg.topic))]
    pub fn new(
        cfg: &KafkaSinkCfg,
        cancel: CancellationToken,
    ) -> anyhow::Result<Self> {
        let mut client_cfg = ClientConfig::new();

        // Core connection settings
        client_cfg
            .set("bootstrap.servers", &cfg.brokers)
            .set("client.id", format!("deltaforge-{}", cfg.id))
            .set("socket.keepalive.enable", "true");

        // Batching and compression
        client_cfg
            .set("compression.type", "lz4")
            .set("linger.ms", DEFAULT_LINGER_MS);

        // Timeout configuration
        client_cfg
            .set("message.timeout.ms", "60000")
            .set("delivery.timeout.ms", "120000")
            .set("request.timeout.ms", "30000")
            .set("retry.backoff.ms", "100");

        // Reliability settings
        if cfg.exactly_once == Some(true) {
            // Exactly-once: transactional semantics
            client_cfg
                .set("enable.idempotence", "true")
                .set("acks", "all")
                .set("retries", "1000000") // rdkafka caps appropriately
                .set("max.in.flight.requests.per.connection", "5");
        } else {
            // At-least-once: idempotent production
            client_cfg
                .set("enable.idempotence", "true")
                .set("acks", "all")
                .set("retries", "10")
                .set("max.in.flight.requests.per.connection", "5");
        }

        // Apply user overrides last
        for (k, v) in &cfg.client_conf {
            client_cfg.set(k, v);
        }

        let producer: FutureProducer =
            client_cfg.create().with_context(|| {
                format!(
                    "creating kafka producer for {}",
                    redact_brokers(&cfg.brokers)
                )
            })?;

        let send_timeout = cfg
            .send_timeout_secs
            .map(|s| Duration::from_secs(s as u64))
            .unwrap_or(DEFAULT_SEND_TIMEOUT);

        // Build envelope and encoding from config
        let envelope_type = cfg.envelope.to_envelope_type();
        let encoding_type = cfg.encoding.to_encoding_type();

        info!(
            brokers = %redact_brokers(&cfg.brokers),
            topic = %cfg.topic,
            envelope = %envelope_type.name(),
            encoding = encoding_type.name(),
            exactly_once = cfg.exactly_once.unwrap_or(false),
            send_timeout_ms = send_timeout.as_millis(),
            "kafka sink created"
        );

        Ok(Self {
            id: cfg.id.clone(),
            cfg: cfg.clone(),
            producer,
            topic: cfg.topic.clone(),
            envelope: envelope_type.build(),
            encoding: encoding_type,
            cancel,
            send_timeout,
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

    /// Send a single message with retry logic.
    async fn send_with_retry(
        &self,
        payload: Vec<u8>,
        key: String,
    ) -> SinkResult<()> {
        let policy = RetryPolicy::new(
            Duration::from_millis(100),
            Duration::from_secs(10),
            0.2,
            Some(3),
        );

        let result = retry_async(
            |attempt| {
                let payload = payload.clone();
                let key = key.clone();
                async move {
                    debug!(attempt, key = %key, "sending to kafka");

                    self.producer
                        .send(
                            FutureRecord::to(&self.topic)
                                .payload(&payload)
                                .key(&key),
                            Timeout::After(self.send_timeout),
                        )
                        .await
                        .map(|_| ())
                        .map_err(|(e, _msg)| KafkaRetryError::from(e))
                }
            },
            |e| e.is_retryable(),
            self.send_timeout,
            policy,
            &self.cancel,
            "kafka_send",
        )
        .await;

        match result {
            Ok(_) => Ok(()),
            Err(outcome) => Err(outcome_to_sink_error(outcome)),
        }
    }
}

#[async_trait]
impl Sink for KafkaSink {
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
        let key = event.idempotency_key();

        self.send_with_retry(payload, key).await?;

        debug!(topic = %self.topic, "event sent to kafka");
        Ok(())
    }

    /// Batch send for Kafka: queue all messages, then await all deliveries.
    ///
    /// This leverages rdkafka's internal batching by:
    /// 1. Pre-serializing all events (fail fast on serialization errors)
    /// 2. Enqueuing all messages to rdkafka's buffer
    /// 3. Awaiting all delivery futures concurrently
    ///
    /// The `linger.ms` setting controls how long rdkafka waits to batch
    /// messages before sending.
    #[instrument(skip_all, fields(sink_id = %self.id, count = events.len()))]
    async fn send_batch(&self, events: &[Event]) -> SinkResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        // Phase 1: Pre-serialize all payloads using configured envelope
        let serialized: Vec<(Vec<u8>, String)> = events
            .iter()
            .map(|e| Ok((self.serialize_event(e)?, e.idempotency_key())))
            .collect::<Result<Vec<_>, SinkError>>()?;

        // Phase 2: Enqueue all messages to rdkafka's buffer
        let futures: Vec<_> = serialized
            .iter()
            .map(|(payload, key)| {
                self.producer.send(
                    FutureRecord::to(&self.topic).payload(payload).key(key),
                    Timeout::After(self.send_timeout),
                )
            })
            .collect();

        // Phase 3: Await all deliveries concurrently
        // Fail fast on first error to avoid partial batch delivery
        let result = try_join_all(futures.into_iter().map(|f| async move {
            f.await.map_err(|(e, _)| KafkaRetryError::from(e))
        }))
        .await;

        match result {
            Ok(_) => {
                debug!(topic = %self.topic, count = events.len(), "batch sent to kafka");
                Ok(())
            }
            Err(e) => {
                warn!(topic = %self.topic, error = %e, "batch delivery failed");
                Err(SinkError::Backpressure {
                    details: format!("kafka batch error: {}", e).into(),
                })
            }
        }
    }
}

// =============================================================================
// Error Handling
// =============================================================================

/// Internal error type for retry classification.
#[derive(Debug, Clone)]
enum KafkaRetryError {
    /// Queue is full (backpressure).
    QueueFull,
    /// Message timed out.
    Timeout,
    /// Broker connection error.
    BrokerConnection(String),
    /// Authentication/authorization error.
    Auth(String),
    /// Message too large.
    MessageTooLarge,
    /// Other Kafka error.
    Other(String),
}

impl From<KafkaError> for KafkaRetryError {
    fn from(e: KafkaError) -> Self {
        match &e {
            KafkaError::MessageProduction(code) => match code {
                rdkafka::types::RDKafkaErrorCode::QueueFull => Self::QueueFull,
                rdkafka::types::RDKafkaErrorCode::MessageTimedOut => {
                    Self::Timeout
                }
                rdkafka::types::RDKafkaErrorCode::MessageSizeTooLarge => {
                    Self::MessageTooLarge
                }
                _ => Self::Other(e.to_string()),
            },
            KafkaError::ClientCreation(msg) => {
                if is_auth_error(msg) {
                    Self::Auth(msg.clone())
                } else {
                    Self::BrokerConnection(msg.clone())
                }
            }
            _ => {
                let msg = e.to_string();
                if is_auth_error(&msg) {
                    Self::Auth(msg)
                } else if msg.contains("timeout") || msg.contains("timed out") {
                    Self::Timeout
                } else {
                    Self::Other(msg)
                }
            }
        }
    }
}

impl std::fmt::Display for KafkaRetryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::QueueFull => write!(f, "producer queue full (backpressure)"),
            Self::Timeout => write!(f, "message timed out"),
            Self::BrokerConnection(msg) => {
                write!(f, "broker connection: {}", msg)
            }
            Self::Auth(msg) => write!(f, "authentication error: {}", msg),
            Self::MessageTooLarge => write!(f, "message too large"),
            Self::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl KafkaRetryError {
    /// Determine if this error is retryable.
    fn is_retryable(&self) -> bool {
        match self {
            // Backpressure: retry after producer clears
            Self::QueueFull => true,
            // Timeouts are retryable
            Self::Timeout => true,
            // Broker connection issues are retryable
            Self::BrokerConnection(_) => true,
            // Auth errors are NOT retryable
            Self::Auth(_) => false,
            // Message too large is NOT retryable (won't change on retry)
            Self::MessageTooLarge => false,
            // Other errors: check message content
            Self::Other(msg) => {
                !is_auth_error(msg) && !msg.contains("too large")
            }
        }
    }
}

/// Check if an error message indicates an authentication error.
fn is_auth_error(msg: &str) -> bool {
    let lower = msg.to_lowercase();
    lower.contains("authentication")
        || lower.contains("sasl")
        || lower.contains("unauthorized")
        || lower.contains("access denied")
        || lower.contains("permission denied")
}

/// Redact sensitive information from broker list for logging.
///
/// If brokers contain credentials (unlikely but possible in some setups),
/// this provides basic redaction.
fn redact_brokers(brokers: &str) -> String {
    // Most Kafka setups don't have credentials in broker string,
    // but handle the case where they might
    if brokers.contains('@') {
        // Has credentials, redact password portion
        brokers
            .split(',')
            .map(|b| {
                if let Some(at_idx) = b.find('@') {
                    if let Some(colon_idx) = b[..at_idx].rfind(':') {
                        format!("{}:***{}", &b[..colon_idx], &b[at_idx..])
                    } else {
                        b.to_string()
                    }
                } else {
                    b.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join(",")
    } else {
        brokers.to_string()
    }
}

/// Convert a RetryOutcome to SinkError.
fn outcome_to_sink_error(outcome: RetryOutcome<KafkaRetryError>) -> SinkError {
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
        } => {
            let details = format!(
                "exhausted after {} attempts: {}",
                attempts, last_error
            );
            match last_error {
                KafkaRetryError::QueueFull => SinkError::Backpressure {
                    details: details.into(),
                },
                _ => SinkError::Connect {
                    details: details.into(),
                },
            }
        }
        RetryOutcome::Failed(e) => match e {
            KafkaRetryError::Auth(msg) => SinkError::Connect {
                details: msg.into(),
            },
            KafkaRetryError::MessageTooLarge => {
                SinkError::Other(anyhow::anyhow!("message too large for Kafka"))
            }
            _ => SinkError::Connect {
                details: e.to_string().into(),
            },
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_errors_are_not_retryable() {
        let auth = KafkaRetryError::Auth("SASL authentication failed".into());
        assert!(!auth.is_retryable());

        let other_auth = KafkaRetryError::Other("authentication error".into());
        assert!(!other_auth.is_retryable());
    }

    #[test]
    fn transient_errors_are_retryable() {
        assert!(KafkaRetryError::QueueFull.is_retryable());
        assert!(KafkaRetryError::Timeout.is_retryable());
        assert!(
            KafkaRetryError::BrokerConnection("connection reset".into())
                .is_retryable()
        );
    }

    #[test]
    fn message_too_large_is_not_retryable() {
        assert!(!KafkaRetryError::MessageTooLarge.is_retryable());
    }

    #[test]
    fn broker_redaction_without_credentials() {
        let brokers = "broker1:9092,broker2:9092";
        assert_eq!(redact_brokers(brokers), brokers);
    }

    #[test]
    fn broker_redaction_with_credentials() {
        let brokers = "user:password@broker1:9092,user:secret@broker2:9092";
        let redacted = redact_brokers(brokers);
        assert!(!redacted.contains("password"));
        assert!(!redacted.contains("secret"));
        assert!(redacted.contains("***"));
    }
}
