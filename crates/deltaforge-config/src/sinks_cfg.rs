use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ============================================================================
// Envelope Configuration
// ============================================================================

/// Envelope format for sink serialization.
///
/// Controls the outer structure of serialized events.
/// Since Event is Debezium-compatible at the payload level, envelopes
/// are thin wrappers that add minimal overhead.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum EnvelopeCfg {
    /// Direct Event serialization (Debezium payload structure).
    /// Output: `{"before":..., "after":..., "source":..., "op":"c", ...}`
    #[default]
    Native,

    /// Full Debezium envelope with payload wrapper.
    /// Output: `{"payload": {"before":..., "after":..., ...}}`
    Debezium,

    /// CloudEvents 1.0 specification.
    /// Output: `{"specversion":"1.0", "type":"prefix.created", "data":{...}}`
    CloudEvents {
        /// Type prefix for the CloudEvents `type` field.
        /// Example: "com.example.cdc" produces "com.example.cdc.created"
        type_prefix: String,
    },
}

// ============================================================================
// Encoding Configuration
// ============================================================================

/// Wire encoding format for sink serialization.
///
/// Controls how the envelope structure is serialized to bytes.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum EncodingCfg {
    /// JSON encoding (UTF-8).
    #[default]
    Json,
    // Future:
    // /// Avro encoding with Schema Registry.
    // Avro {
    //     schema_registry: String,
    //     #[serde(default)]
    //     subject_strategy: SubjectStrategy,
    // },
}

// ============================================================================
// Sink Configurations
// ============================================================================

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
///       envelope: debezium
///       encoding: json
///       required: true
///       exactly_once: false
///       send_timeout_secs: 30
///       client_conf:
///         security.protocol: SASL_SSL
///         sasl.mechanism: PLAIN
/// ```
///
/// # CloudEvents Example
///
/// ```yaml
/// sinks:
///   - type: kafka
///     config:
///       id: kafka-cloudevents
///       brokers: localhost:9092
///       topic: events
///       envelope:
///         type: cloudevents
///         type_prefix: "com.example.cdc"
///       encoding: json
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaSinkCfg {
    /// Unique identifier for this sink instance.
    pub id: String,

    /// Comma-separated list of Kafka broker addresses.
    /// Example: "broker1:9092,broker2:9092"
    pub brokers: String,

    /// Target Kafka topic. Supports `${path}` templates for per-event routing.
    /// Examples: `"cdc.${source.table}"`, `"static-topic"`
    pub topic: String,

    /// Message key template for partition affinity.
    /// Supports `${path}` templates. When unset, uses `event.idempotency_key()`.
    /// Examples: `"${after.customer_id}"`, `"${source.table}"`
    #[serde(default)]
    pub key: Option<String>,

    /// Envelope format for event serialization.
    /// Default: native (Debezium payload structure)
    #[serde(default)]
    pub envelope: EnvelopeCfg,

    /// Wire encoding format.
    /// Default: json
    #[serde(default)]
    pub encoding: EncodingCfg,

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
///       envelope: native
///       encoding: json
///       required: true
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisSinkCfg {
    /// Unique identifier for this sink instance.
    pub id: String,

    /// Redis connection URI.
    /// Supports: redis://, rediss:// (TLS), redis+sentinel://
    /// Example: "redis://:password@localhost:6379/0"
    pub uri: String,

    /// Target Redis Stream name for CDC events. Supports `${path}` templates.
    pub stream: String,

    /// Message key template for the "df-key" stream field.
    /// When unset, uses event_id. Supports `${path}` templates.
    #[serde(default)]
    pub key: Option<String>,

    /// Envelope format for event serialization.
    /// Default: native (Debezium payload structure)
    #[serde(default)]
    pub envelope: EnvelopeCfg,

    /// Wire encoding format.
    /// Default: json
    #[serde(default)]
    pub encoding: EncodingCfg,

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
///
/// # Example
///
/// ```yaml
/// sinks:
///   - type: nats
///     config:
///       id: nats-events
///       url: nats://localhost:4222
///       subject: cdc.events
///       envelope: native
///       encoding: json
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NatsSinkCfg {
    /// Unique identifier for this sink.
    pub id: String,

    /// NATS server URL (e.g., "nats://localhost:4222").
    /// Supports comma-separated list for cluster connections.
    pub url: String,

    /// Subject to publish events to. Supports `${path}` templates.
    pub subject: String,

    /// Message key stored in NATS header "df-key".
    /// Supports `${path}` templates. Optional, no header if unset.
    #[serde(default)]
    pub key: Option<String>,

    /// Envelope format for event serialization.
    /// Default: native (Debezium payload structure)
    #[serde(default)]
    pub envelope: EnvelopeCfg,

    /// Wire encoding format.
    /// Default: json
    #[serde(default)]
    pub encoding: EncodingCfg,

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

// ============================================================================
// Conversion helpers (config → core types)
// ============================================================================

impl EnvelopeCfg {
    /// Convert to core envelope type.
    pub fn to_envelope_type(&self) -> deltaforge_core::envelope::EnvelopeType {
        match self {
            EnvelopeCfg::Native => {
                deltaforge_core::envelope::EnvelopeType::Native
            }
            EnvelopeCfg::Debezium => {
                deltaforge_core::envelope::EnvelopeType::Debezium
            }
            EnvelopeCfg::CloudEvents { type_prefix } => {
                deltaforge_core::envelope::EnvelopeType::CloudEvents {
                    type_prefix: type_prefix.clone(),
                }
            }
        }
    }
}

impl EncodingCfg {
    /// Convert to core encoding type.
    pub fn to_encoding_type(&self) -> deltaforge_core::encoding::EncodingType {
        match self {
            EncodingCfg::Json => deltaforge_core::encoding::EncodingType::Json,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_native_envelope() {
        let yaml = r#"
            type: native
        "#;
        let cfg: EnvelopeCfg = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg, EnvelopeCfg::Native);
    }

    #[test]
    fn parse_debezium_envelope() {
        let yaml = r#"
            type: debezium
        "#;
        let cfg: EnvelopeCfg = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg, EnvelopeCfg::Debezium);
    }

    #[test]
    fn parse_cloudevents_envelope() {
        let yaml = r#"
            type: cloudevents
            type_prefix: "com.example.cdc"
        "#;
        let cfg: EnvelopeCfg = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            cfg,
            EnvelopeCfg::CloudEvents {
                type_prefix: "com.example.cdc".to_string()
            }
        );
    }

    #[test]
    fn parse_kafka_sink_with_envelope() {
        let yaml = r#"
            id: test-kafka
            brokers: localhost:9092
            topic: events
            envelope:
              type: debezium
            encoding: json
        "#;
        let cfg: KafkaSinkCfg = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.envelope, EnvelopeCfg::Debezium);
        assert_eq!(cfg.encoding, EncodingCfg::Json);
    }

    #[test]
    fn kafka_sink_defaults() {
        let yaml = r#"
            id: test-kafka
            brokers: localhost:9092
            topic: events
        "#;
        let cfg: KafkaSinkCfg = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.envelope, EnvelopeCfg::Native);
        assert_eq!(cfg.encoding, EncodingCfg::Json);
    }
}
