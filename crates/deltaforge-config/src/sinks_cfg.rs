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
///
/// Supports two YAML forms:
/// - Simple: `encoding: json`
/// - Structured: `encoding: { type: avro, schema_registry_url: "..." }`
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum EncodingCfg {
    /// JSON encoding (UTF-8).
    #[default]
    Json,

    /// Avro encoding with Confluent Schema Registry.
    ///
    /// Produces Confluent wire format: `[0x00][4-byte schema ID][avro payload]`.
    /// Schemas are auto-registered and cached.
    ///
    /// ```yaml
    /// encoding:
    ///   type: avro
    ///   schema_registry_url: "http://localhost:8081"
    ///   subject_strategy: topic_name   # default
    /// ```
    Avro {
        /// Schema Registry URL (Confluent-compatible API).
        schema_registry_url: String,

        /// Subject naming strategy for schema registration.
        subject_strategy: SubjectStrategy,

        /// Basic auth username for Schema Registry.
        username: Option<String>,

        /// Basic auth password for Schema Registry.
        password: Option<String>,

        /// How to map MySQL BIGINT UNSIGNED. Default: "string" (safe).
        /// "long" risks overflow for values >= 2^63.
        unsigned_bigint_mode: Option<String>,

        /// How to map MySQL/PostgreSQL ENUM types. Default: "string" (safe).
        /// "enum" uses Avro enum (compatibility risk on symbol changes).
        enum_mode: Option<String>,

        /// How to map naive (timezone-unaware) timestamps. Default: "string" (ISO-8601).
        /// "timestamp" uses Avro timestamp logical types (semantically misleading for non-UTC).
        naive_timestamp_mode: Option<String>,
    },
}

impl Serialize for EncodingCfg {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            EncodingCfg::Json => serializer.serialize_str("json"),
            EncodingCfg::Avro {
                schema_registry_url,
                subject_strategy,
                username,
                password,
                unsigned_bigint_mode,
                enum_mode,
                naive_timestamp_mode,
            } => {
                use serde::ser::SerializeMap;
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "avro")?;
                map.serialize_entry(
                    "schema_registry_url",
                    schema_registry_url,
                )?;
                map.serialize_entry("subject_strategy", subject_strategy)?;
                if let Some(u) = username {
                    map.serialize_entry("username", u)?;
                }
                if let Some(p) = password {
                    map.serialize_entry("password", p)?;
                }
                if let Some(v) = unsigned_bigint_mode {
                    map.serialize_entry("unsigned_bigint_mode", v)?;
                }
                if let Some(v) = enum_mode {
                    map.serialize_entry("enum_mode", v)?;
                }
                if let Some(v) = naive_timestamp_mode {
                    map.serialize_entry("naive_timestamp_mode", v)?;
                }
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for EncodingCfg {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de;

        // Try as string first ("json"), then as map ({ type: avro, ... })
        struct EncodingVisitor;

        impl<'de> de::Visitor<'de> for EncodingVisitor {
            type Value = EncodingCfg;

            fn expecting(
                &self,
                formatter: &mut std::fmt::Formatter<'_>,
            ) -> std::fmt::Result {
                formatter.write_str(
                    r#""json" or { type: "avro", schema_registry_url: "..." }"#,
                )
            }

            fn visit_str<E: de::Error>(
                self,
                v: &str,
            ) -> Result<Self::Value, E> {
                match v {
                    "json" => Ok(EncodingCfg::Json),
                    "avro" => Err(de::Error::custom(
                        "avro encoding requires schema_registry_url; use { type: avro, schema_registry_url: \"...\" }",
                    )),
                    other => Err(de::Error::unknown_variant(
                        other,
                        &["json", "avro"],
                    )),
                }
            }

            fn visit_map<A: de::MapAccess<'de>>(
                self,
                map: A,
            ) -> Result<Self::Value, A::Error> {
                #[derive(Deserialize)]
                #[serde(tag = "type", rename_all = "lowercase")]
                enum Tagged {
                    Json,
                    Avro {
                        schema_registry_url: String,
                        #[serde(default)]
                        subject_strategy: SubjectStrategy,
                        #[serde(default)]
                        username: Option<String>,
                        #[serde(default)]
                        password: Option<String>,
                        #[serde(default)]
                        unsigned_bigint_mode: Option<String>,
                        #[serde(default)]
                        enum_mode: Option<String>,
                        #[serde(default)]
                        naive_timestamp_mode: Option<String>,
                    },
                }
                let tagged = Tagged::deserialize(
                    de::value::MapAccessDeserializer::new(map),
                )?;
                Ok(match tagged {
                    Tagged::Json => EncodingCfg::Json,
                    Tagged::Avro {
                        schema_registry_url,
                        subject_strategy,
                        username,
                        password,
                        unsigned_bigint_mode,
                        enum_mode,
                        naive_timestamp_mode,
                    } => EncodingCfg::Avro {
                        schema_registry_url,
                        subject_strategy,
                        username,
                        password,
                        unsigned_bigint_mode,
                        enum_mode,
                        naive_timestamp_mode,
                    },
                })
            }
        }

        deserializer.deserialize_any(EncodingVisitor)
    }
}

/// Subject naming strategy for Schema Registry.
///
/// Determines how the Schema Registry subject name is derived.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SubjectStrategy {
    /// `{topic}-value` — one schema per Kafka topic (default, most common).
    #[default]
    TopicName,

    /// `{record_name}` — one schema per record type (requires `record_name` in event).
    RecordName,

    /// `{topic}-{record_name}` — per-topic, per-record schema.
    TopicRecordName,
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
    Http(HttpSinkCfg),
    S3(S3SinkCfg),
}

impl SinkCfg {
    /// Return the sink's unique identifier.
    pub fn sink_id(&self) -> &str {
        match self {
            Self::Kafka(c) => &c.id,
            Self::Redis(c) => &c.id,
            Self::Nats(c) => &c.id,
            Self::Http(c) => &c.id,
            Self::S3(c) => &c.id,
        }
    }
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

    /// Optional filter applied before delivery. Events not matching the filter
    /// are silently ignored by this sink.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<SinkFilter>,
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

    /// Optional filter applied before delivery. Events not matching the filter
    /// are silently ignored by this sink.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<SinkFilter>,
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

    /// Optional filter applied before delivery. Events not matching the filter
    /// are silently ignored by this sink.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<SinkFilter>,
}

/// HTTP/Webhook sink configuration.
///
/// Delivers events via HTTP POST (or PUT) to any URL. Supports dynamic URL
/// templates, custom headers with env var expansion, and optional batch mode.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpSinkCfg {
    /// Unique identifier for this sink.
    pub id: String,

    /// Target URL. Supports `${path}` templates for per-event routing.
    /// Example: `https://api.example.com/cdc/${source.table}`
    pub url: String,

    /// HTTP method. Default: POST.
    #[serde(default = "default_http_method")]
    pub method: String,

    /// Static headers added to every request. Values support `${ENV_VAR}` expansion.
    /// Example: `{"Authorization": "Bearer ${API_TOKEN}", "X-Source": "deltaforge"}`
    #[serde(default)]
    pub headers: std::collections::HashMap<String, String>,

    /// Batch mode: if true, send a JSON array of events in one request.
    /// If false (default), send one request per event.
    #[serde(default)]
    pub batch_mode: bool,

    /// Envelope format for event serialization.
    #[serde(default)]
    pub envelope: EnvelopeCfg,

    /// Wire encoding format.
    #[serde(default)]
    pub encoding: EncodingCfg,

    /// Whether this sink must succeed for checkpoint to proceed.
    #[serde(default)]
    pub required: Option<bool>,

    /// Timeout for individual HTTP requests (seconds). Default: 10.
    #[serde(default)]
    pub send_timeout_secs: Option<u32>,

    /// Timeout for batch operations (seconds). Default: 30.
    #[serde(default)]
    pub batch_timeout_secs: Option<u32>,

    /// Timeout for TCP connection establishment (seconds). Default: 5.
    #[serde(default)]
    pub connect_timeout_secs: Option<u32>,

    /// Optional filter applied before delivery.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<SinkFilter>,
}

fn default_http_method() -> String {
    "POST".to_string()
}

// ============================================================================
// S3 / Parquet / JSON Lines file sink
// ============================================================================

/// S3 + Parquet (or JSON Lines) sink configuration.
///
/// Writes CDC events to S3-compatible object storage (S3, MinIO, GCS, Azure,
/// local FS) as Parquet or JSON Lines files. Files are partitioned by table +
/// UTC date in Hive style and roll on configurable size / event-count / age
/// thresholds.
///
/// # Example
///
/// ```yaml
/// sinks:
///   - type: s3
///     config:
///       id: orders-lake
///       bucket: my-data-lake
///       prefix: "deltaforge/orders"
///       region: us-east-1
///       endpoint: null              # null for AWS S3; set for MinIO/etc
///       access_key_id: "${S3_ACCESS_KEY}"
///       secret_access_key: "${S3_SECRET_KEY}"
///       format: parquet              # parquet | jsonl
///       compression: snappy          # snappy | gzip | zstd | none
///       file_roll:
///         max_bytes: 268435456       # 256 MiB
///         max_events: 1000000        # 1M
///         max_age_secs: 300          # 5 min
///         idle_age_secs: 600         # 10 min
///       required: true
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3SinkCfg {
    /// Unique identifier for this sink instance.
    pub id: String,

    /// Target bucket name (S3) or root directory (for `local: true`).
    pub bucket: String,

    /// Prefix prepended to the Hive partition path. Trailing slashes are stripped.
    /// Final layout: `{prefix}/table={t}/year={y}/month={m:02}/day={d:02}/<ulid>.{ext}`
    #[serde(default)]
    pub prefix: String,

    /// AWS region. Required for AWS S3, ignored by MinIO/local.
    #[serde(default)]
    pub region: Option<String>,

    /// Override endpoint for non-AWS S3 services (MinIO, Ceph, R2, etc.).
    /// Leave `None` for AWS S3.
    #[serde(default)]
    pub endpoint: Option<String>,

    /// Inline access key (supports `${ENV_VAR}` expansion). Prefer IAM
    /// instance roles in production.
    #[serde(default)]
    pub access_key_id: Option<String>,

    /// Inline secret key (supports `${ENV_VAR}` expansion).
    #[serde(default)]
    pub secret_access_key: Option<String>,

    /// Use virtual-hosted-style addressing. Defaults to `false` (path-style),
    /// which works for MinIO. Set `true` for AWS S3 with custom domains.
    #[serde(default)]
    pub virtual_hosted_style: bool,

    /// Use local filesystem (`object_store::local::LocalFileSystem`) rooted
    /// at `bucket`. For tests and single-node deployments.
    #[serde(default)]
    pub local: bool,

    /// Output file format.
    #[serde(default)]
    pub format: S3FileFormat,

    /// Compression. Per-format: Parquet supports `snappy` (default), `gzip`,
    /// `zstd`, `none`. JSON Lines supports `gzip` (default) and `none`.
    #[serde(default)]
    pub compression: S3Compression,

    /// File rolling thresholds. A writer is rolled when any threshold fires.
    #[serde(default)]
    pub file_roll: S3FileRoll,

    /// Whether this sink must succeed for the checkpoint to advance.
    #[serde(default)]
    pub required: Option<bool>,

    /// Optional filter applied before delivery. Events not matching the filter
    /// are silently ignored by this sink.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<SinkFilter>,
}

#[derive(
    Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq,
)]
#[serde(rename_all = "lowercase")]
pub enum S3FileFormat {
    #[default]
    Parquet,
    Jsonl,
}

#[derive(
    Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq,
)]
#[serde(rename_all = "lowercase")]
pub enum S3Compression {
    None,
    #[default]
    Snappy,
    Gzip,
    Zstd,
}

/// File rolling thresholds. Writer rolls when any threshold is reached.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct S3FileRoll {
    /// Maximum buffered bytes (Parquet estimate / JSONL exact) before rolling.
    #[serde(default = "default_s3_max_bytes")]
    pub max_bytes: u64,

    /// Maximum events written to a single file before rolling.
    #[serde(default = "default_s3_max_events")]
    pub max_events: u64,

    /// Maximum age (seconds) of a writer measured from its first event.
    #[serde(default = "default_s3_max_age_secs")]
    pub max_age_secs: u64,

    /// Idle age (seconds): if no new events arrive within this window, the
    /// writer is rolled. Lets low-volume partitions land data eventually.
    #[serde(default = "default_s3_idle_age_secs")]
    pub idle_age_secs: u64,
}

impl Default for S3FileRoll {
    fn default() -> Self {
        Self {
            max_bytes: default_s3_max_bytes(),
            max_events: default_s3_max_events(),
            max_age_secs: default_s3_max_age_secs(),
            idle_age_secs: default_s3_idle_age_secs(),
        }
    }
}

fn default_s3_max_bytes() -> u64 {
    256 * 1024 * 1024
}
fn default_s3_max_events() -> u64 {
    1_000_000
}
fn default_s3_max_age_secs() -> u64 {
    300
}
fn default_s3_idle_age_secs() -> u64 {
    600
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
            EncodingCfg::Avro { .. } => {
                // Avro encoding is handled at the sink level (needs async Schema Registry).
                // The EncodingType returned here is only used for content_type/name.
                deltaforge_core::encoding::EncodingType::Avro
            }
        }
    }
}

/// Filter applied before a sink receives events.
///
/// All configured conditions are AND-ed together. An event must satisfy
/// every condition to be delivered. Omitting a field means "no restriction".
///
/// # Example
///
/// ```yaml
/// sinks:
///   - type: kafka
///     config:
///       id: kafka-business
///       topic: "cdc.${source.table}"
///       filter:
///         exclude_synthetic: true    # drop processor-created events
///
///   - type: kafka
///     config:
///       id: kafka-metrics
///       topic: "_deltaforge.metrics"
///       filter:
///         synthetic_only: true       # only processor-created events
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct SinkFilter {
    /// Drop all synthetic events (those created by processors, not sourced from a DB).
    /// Mutually exclusive with `synthetic_only`.
    #[serde(default)]
    pub exclude_synthetic: bool,

    /// Drop all non-synthetic events (only deliver processor-created events).
    /// Mutually exclusive with `exclude_synthetic`.
    #[serde(default)]
    pub synthetic_only: bool,

    /// Only deliver events produced by these processor IDs.
    /// Empty = no restriction. Works alongside `synthetic_only`.
    #[serde(default)]
    pub producers: Vec<String>,
}

impl SinkFilter {
    /// Returns true if any filter condition is configured.
    /// Used to skip wrapping sinks when no filtering is needed.
    pub fn is_active(&self) -> bool {
        self.exclude_synthetic
            || self.synthetic_only
            || !self.producers.is_empty()
    }

    /// Returns true if the given event should be delivered to the sink.
    pub fn allows(&self, event: &deltaforge_core::Event) -> bool {
        if self.exclude_synthetic && event.is_synthetic() {
            return false;
        }
        if self.synthetic_only && !event.is_synthetic() {
            return false;
        }
        if !self.producers.is_empty() {
            let producer = event.synthetic.as_deref().unwrap_or("");
            if !self.producers.iter().any(|p| p == producer) {
                return false;
            }
        }
        true
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

    #[test]
    fn parse_avro_encoding() {
        let yaml = r#"
            id: test-kafka
            brokers: localhost:9092
            topic: events
            encoding:
              type: avro
              schema_registry_url: "http://localhost:8081"
        "#;
        let cfg: KafkaSinkCfg = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(cfg.encoding, EncodingCfg::Avro { .. }));
        if let EncodingCfg::Avro {
            schema_registry_url,
            subject_strategy,
            ..
        } = &cfg.encoding
        {
            assert_eq!(schema_registry_url, "http://localhost:8081");
            assert_eq!(*subject_strategy, SubjectStrategy::TopicName);
        }
    }

    #[test]
    fn parse_avro_encoding_with_strategy() {
        let yaml = r#"
            id: test-kafka
            brokers: localhost:9092
            topic: events
            encoding:
              type: avro
              schema_registry_url: "http://sr:8081"
              subject_strategy: record_name
              username: user
              password: pass
        "#;
        let cfg: KafkaSinkCfg = serde_yaml::from_str(yaml).unwrap();
        if let EncodingCfg::Avro {
            subject_strategy,
            username,
            password,
            ..
        } = &cfg.encoding
        {
            assert_eq!(*subject_strategy, SubjectStrategy::RecordName);
            assert_eq!(username.as_deref(), Some("user"));
            assert_eq!(password.as_deref(), Some("pass"));
        } else {
            panic!("expected Avro encoding");
        }
    }

    #[test]
    fn parse_json_string_encoding() {
        // Backward compat: `encoding: json` as bare string
        let yaml = r#"
            id: test-kafka
            brokers: localhost:9092
            topic: events
            encoding: json
        "#;
        let cfg: KafkaSinkCfg = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.encoding, EncodingCfg::Json);
    }

    #[test]
    fn parse_json_map_encoding() {
        // encoding: { type: json }
        let yaml = r#"
            id: test-kafka
            brokers: localhost:9092
            topic: events
            encoding:
              type: json
        "#;
        let cfg: KafkaSinkCfg = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.encoding, EncodingCfg::Json);
    }

    #[test]
    fn parse_s3_sink_parquet_default() {
        let yaml = r#"
            id: test-s3
            bucket: my-data-lake
            prefix: deltaforge/orders
            region: us-east-1
            access_key_id: ${S3_KEY}
            secret_access_key: ${S3_SECRET}
            format: parquet
            compression: snappy
            file_roll:
              max_bytes: 268435456
              max_events: 1000000
              max_age_secs: 300
              idle_age_secs: 600
            required: true
        "#;
        let cfg: S3SinkCfg = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.id, "test-s3");
        assert_eq!(cfg.bucket, "my-data-lake");
        assert_eq!(cfg.prefix, "deltaforge/orders");
        assert_eq!(cfg.region.as_deref(), Some("us-east-1"));
        assert_eq!(cfg.format, S3FileFormat::Parquet);
        assert_eq!(cfg.compression, S3Compression::Snappy);
        assert_eq!(cfg.file_roll.max_bytes, 268_435_456);
        assert_eq!(cfg.file_roll.max_events, 1_000_000);
        assert_eq!(cfg.required, Some(true));
    }

    #[test]
    fn parse_s3_sink_jsonl_gzip_minio() {
        let yaml = r#"
            id: chaos-s3-jsonl
            bucket: deltaforge-test
            prefix: audit
            endpoint: "http://minio:9000"
            region: us-east-1
            access_key_id: minioadmin
            secret_access_key: minioadmin
            format: jsonl
            compression: gzip
        "#;
        let cfg: S3SinkCfg = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.endpoint.as_deref(), Some("http://minio:9000"));
        assert_eq!(cfg.format, S3FileFormat::Jsonl);
        assert_eq!(cfg.compression, S3Compression::Gzip);
        // Defaults applied where unset.
        assert_eq!(cfg.file_roll.max_bytes, 256 * 1024 * 1024);
        assert!(!cfg.local);
        assert!(!cfg.virtual_hosted_style);
    }

    #[test]
    fn parse_s3_sink_local_filesystem() {
        let yaml = r#"
            id: local-sink
            bucket: /tmp/deltaforge-out
            local: true
        "#;
        let cfg: S3SinkCfg = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.local);
        assert_eq!(cfg.bucket, "/tmp/deltaforge-out");
        // Format defaults to parquet
        assert_eq!(cfg.format, S3FileFormat::Parquet);
    }

    #[test]
    fn parse_full_pipeline_with_s3_sink() {
        let yaml = r#"
            sinks:
              - type: s3
                config:
                  id: orders-lake
                  bucket: my-lake
                  prefix: cdc/orders
                  format: parquet
                  compression: snappy
        "#;
        #[derive(serde::Deserialize)]
        struct Holder {
            sinks: Vec<SinkCfg>,
        }
        let h: Holder = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(h.sinks.len(), 1);
        match &h.sinks[0] {
            SinkCfg::S3(c) => {
                assert_eq!(c.id, "orders-lake");
                assert_eq!(c.format, S3FileFormat::Parquet);
            }
            other => panic!("expected S3, got {other:?}"),
        }
    }
}
