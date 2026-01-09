use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs};
use thiserror::Error;
use tracing::error;
use walkdir::WalkDir;

#[cfg(feature = "turso")]
mod turso_cfg;
#[cfg(feature = "turso")]
pub use turso_cfg::{NativeCdcLevel, TursoSrcCfg};

mod schema_sensing;
pub use schema_sensing::{
    ColumnFilter, DeepInspectConfig, SamplingConfig, SchemaSensingConfig,
    SensingOutputConfig, TableFilter, TrackingConfig,
};

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config file {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to parse config file {path}: {source}")]
    Parse {
        path: String,
        #[source]
        source: serde_yaml::Error,
    },

    #[error("failed to expand environment variables in {path}: {source}")]
    Env {
        path: String,
        #[source]
        source: shellexpand::LookupError<std::env::VarError>,
    },

    #[error("no config files found at {0}")]
    NotFound(String),
}

pub type ConfigResult<T> = std::result::Result<T, ConfigError>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineSpec {
    /// General metadata for pipeline
    pub metadata: Metadata,

    /// Actual pipeline config/spec
    pub spec: Spec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    /// Pipeline name (unique)
    pub name: String,

    /// Business oriented tenant identifier
    pub tenant: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Spec {
    /// Sharding config for the pipeline
    pub sharding: Option<Sharding>,

    /// Source config
    pub source: SourceCfg,

    /// Multi (sequential) processors
    pub processors: Vec<ProcessorCfg>,

    /// Multi sink config
    pub sinks: Vec<SinkCfg>,

    /// Source connection policy
    pub connection_policy: Option<ConnectionPolicy>,

    /// Pipeline-level batch configuration (commit unit).
    pub batch: Option<BatchConfig>,

    /// How sink acknowledgements gate checkpoint commits.
    pub commit_policy: Option<CommitPolicy>,

    /// Schema sensing configuration.
    /// When enabled, automatically infers and tracks schema from event payloads.
    #[serde(default)]
    pub schema_sensing: SchemaSensingConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresSrcCfg {
    pub id: String,
    pub dsn: String,
    pub publication: String,
    pub slot: String,
    pub tables: Vec<String>,
    /// Starting position when no checkpoint exists.
    /// - "earliest" (default) starts from the beginning (LSN 0 / publication snapshot)
    /// - "latest" starts from pg_current_wal_lsn()
    /// - {"lsn": "..."} starts from a specific LSN string (e.g., "0/16B6C50")
    #[serde(default)]
    pub start_position: PostgresStartPosition,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum PostgresStartPosition {
    #[default]
    Earliest,
    Latest,
    #[serde(rename = "lsn")]
    Lsn(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MysqlSrcCfg {
    pub id: String,
    pub dsn: String,
    pub tables: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "config", rename_all = "lowercase")]
pub enum SourceCfg {
    Postgres(PostgresSrcCfg),
    Mysql(MysqlSrcCfg),
    #[cfg(feature = "turso")]
    Turso(turso_cfg::TursoSrcCfg),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ProcessorCfg {
    #[serde(rename = "javascript")]
    Javascript {
        id: String,
        inline: String,
        limits: Option<Limits>,
    },
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "config", rename_all = "lowercase")]
pub enum SinkCfg {
    Kafka(KafkaSinkCfg),
    Redis(RedisSinkCfg),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sharding {
    pub mode: String,
    pub count: Option<u32>,
    pub key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Limits {
    pub cpu_ms: Option<u64>,
    pub mem_mb: Option<u64>,
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionPolicy {
    pub default_mode: Option<String>,
    pub preferred_replica: Option<String>,
    pub limits: Option<ConnectionLimits>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionLimits {
    pub max_dedicated_per_source: Option<u32>,
}

/// The pipeline-level batch (the **commit unit**). Coordinator will build batches
/// using these thresholds and checkpoint after a batch is accepted by sinks.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BatchConfig {
    /// Flush when this many events have been accumulated.
    pub max_events: Option<usize>,
    /// Flush when total serialized size reaches this threshold (bytes).
    pub max_bytes: Option<usize>,
    /// Flush when this much time has elapsed since batch start (ms).
    pub max_ms: Option<u64>,
    /// If true, never split a single source transaction (e.g., MySQL XID) across batches.
    pub respect_source_tx: Option<bool>,
    /// How many batches may be in-flight concurrently (keep 1 until we add WAL).
    pub max_inflight: Option<usize>,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_events: Some(1000),
            max_bytes: Some(3 * 1014 * 1024),
            max_ms: Some(100),
            respect_source_tx: Some(true),
            max_inflight: Some(1),
        }
    }
}

/// Per-component performance knobs that do **not** change the commit unit.
/// Pipeline Components can optionally process the coordinator's batch in smaller chunks.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct MicrobatchConfig {
    pub max_events: Option<usize>,
    pub max_bytes: Option<usize>,
    pub max_ms: Option<u64>,
}

/// How sink acks gate checkpointing when multiple sinks are configured.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum CommitPolicy {
    /// All sinks (or all marked as required) must ack the batch.
    All,
    /// Only sinks marked as `required: true` must ack; optional sinks are best-effort.
    #[default]
    Required,
    /// Checkpoint after at least `quorum` sinks ack (availability over consistency).
    Quorum { quorum: usize },
}

pub fn load_from_path(file_path: &str) -> ConfigResult<PipelineSpec> {
    let raw = fs::read_to_string(file_path).map_err(|e| ConfigError::Io {
        path: file_path.to_owned(),
        source: e,
    })?;

    let with_env = shellexpand::env(&raw)
        .map_err(|e| ConfigError::Env {
            path: file_path.to_owned(),
            source: e,
        })?
        .to_string();
    let spec: PipelineSpec =
        serde_yaml::from_str(&with_env).map_err(|e| ConfigError::Parse {
            path: file_path.to_owned(),
            source: e,
        })?;

    Ok(spec)
}

pub fn load_from_dir(dir_path: &str) -> ConfigResult<Vec<PipelineSpec>> {
    let mut specs = Vec::<PipelineSpec>::new();
    for entry in WalkDir::new(dir_path)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.metadata().map(|m| m.is_file()).unwrap_or(false))
    {
        if let Some(path_str) = entry.path().to_str() {
            let spec = load_from_path(path_str)?;
            specs.push(spec);
        } else {
            error!(file=%entry.path().display(), "skipping file in config dir")
        }
    }

    Ok(specs)
}

pub fn load_cfg(path: &str) -> ConfigResult<Vec<PipelineSpec>> {
    let cfg_path = std::path::Path::new(path);

    match cfg_path.is_dir() {
        true => load_from_dir(path),
        false => {
            let spec = load_from_path(path)?;
            Ok(vec![spec])
        }
    }
}
