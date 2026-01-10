use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "config", rename_all = "lowercase")]
pub enum SinkCfg {
    Kafka(KafkaSinkCfg),
    Redis(RedisSinkCfg),
    Nats(NatsSinkCfg),
}

/// Kafka sink configuration.
///
/// # Example
///
/// ```yaml
/// sinks:
///   - type: kafka
///     config:
///       id: kafka-events
///       brokers: localhost:9092
///       topic: deltaforge-events
///       required: true
///       exactly_once: false
///       send_timeout_secs: 30
///       client_conf:
///         security.protocol: SASL_SSL
///         sasl.mechanism: PLAIN
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaSinkCfg {
    /// Unique identifier for this sink instance.
    pub id: String,

    /// Comma-separated list of Kafka broker addresses.
    /// Example: "broker1:9092,broker2:9092"
    pub brokers: String,

    /// Target Kafka topic for CDC events.
    pub topic: String,

    /// Whether this sink must succeed for checkpoint to proceed.
    /// Default: true
    #[serde(default)]
    pub required: Option<bool>,

    /// Enable exactly-once semantics (transactional producer).
    /// Default: false (idempotent at-least-once)
    #[serde(default)]
    pub exactly_once: Option<bool>,

    /// Timeout for individual message sends (seconds).
    /// Default: 30
    #[serde(default)]
    pub send_timeout_secs: Option<u32>,

    /// Raw librdkafka client configuration overrides.
    ///
    /// Keys and values are passed directly to `rdkafka::ClientConfig::set`.
    /// These are applied *after* DeltaForge's own defaults, so user values win.
    ///
    /// Common overrides:
    /// - `security.protocol`: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
    /// - `sasl.mechanism`: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
    /// - `sasl.username`, `sasl.password`: SASL credentials
    /// - `linger.ms`: Batching delay (default: 5)
    /// - `compression.type`: none, gzip, snappy, lz4, zstd (default: lz4)
    #[serde(default)]
    pub client_conf: HashMap<String, String>,
}

/// Redis Streams sink configuration.
///
/// # Example
///
/// ```yaml
/// sinks:
///   - type: redis
///     config:
///       id: redis-events
///       uri: redis://localhost:6379
///       stream: deltaforge-events
///       required: true
///       send_timeout_secs: 5
///       batch_timeout_secs: 30
///       connect_timeout_secs: 10
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisSinkCfg {
    /// Unique identifier for this sink instance.
    pub id: String,

    /// Redis connection URI.
    /// Supports: redis://, rediss:// (TLS), redis+sentinel://
    /// Example: "redis://:password@localhost:6379/0"
    pub uri: String,

    /// Target Redis Stream name for CDC events.
    pub stream: String,

    /// Whether this sink must succeed for checkpoint to proceed.
    /// Default: true
    #[serde(default)]
    pub required: Option<bool>,

    /// Timeout for individual XADD operations (seconds).
    /// Default: 5
    #[serde(default)]
    pub send_timeout_secs: Option<u32>,

    /// Timeout for batch pipeline operations (seconds).
    /// Default: 30
    #[serde(default)]
    pub batch_timeout_secs: Option<u32>,

    /// Timeout for connection establishment (seconds).
    /// Default: 10
    #[serde(default)]
    pub connect_timeout_secs: Option<u32>,
}

/// NATS JetStream sink configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NatsSinkCfg {
    /// Unique identifier for this sink.
    pub id: String,

    /// NATS server URL (e.g., "nats://localhost:4222").
    /// Supports comma-separated list for cluster connections.
    pub url: String,

    /// Subject to publish events to.
    pub subject: String,

    /// Optional JetStream stream name to verify exists.
    /// If provided, the sink will log a warning if the stream doesn't exist.
    #[serde(default)]
    pub stream: Option<String>,

    /// Whether this sink must succeed for checkpoint to proceed.
    /// Default: true
    #[serde(default)]
    pub required: Option<bool>,

    /// Timeout for individual publish operations (seconds).
    /// Default: 5
    #[serde(default)]
    pub send_timeout_secs: Option<u32>,

    /// Timeout for batch publish operations (seconds).
    /// Default: 30
    #[serde(default)]
    pub batch_timeout_secs: Option<u32>,

    /// Timeout for connection establishment (seconds).
    /// Default: 10
    #[serde(default)]
    pub connect_timeout_secs: Option<u32>,

    /// Path to NATS credentials file (.creds).
    #[serde(default)]
    pub credentials_file: Option<String>,

    /// Username for basic authentication.
    #[serde(default)]
    pub username: Option<String>,

    /// Password for basic authentication.
    #[serde(default)]
    pub password: Option<String>,

    /// Token for token-based authentication.
    #[serde(default)]
    pub token: Option<String>,
}
