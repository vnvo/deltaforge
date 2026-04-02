use serde::{Deserialize, Serialize};
use std::fs;
use thiserror::Error;
use tracing::{error, warn};
use walkdir::WalkDir;

#[cfg(feature = "turso")]
mod turso_cfg;
#[cfg(feature = "turso")]
pub use turso_cfg::{NativeCdcLevel, TursoSrcCfg};

mod storage;
pub use storage::*;

mod snapshot_cfg;
pub use snapshot_cfg::*;

mod sinks_cfg;
pub use sinks_cfg::*;

mod flatten_cfg;
pub use flatten_cfg::*;

mod schema_sensing;
pub use schema_sensing::{
    ColumnFilter, DeepInspectConfig, SamplingConfig, SchemaSensingConfig,
    SensingOutputConfig, TableFilter, TrackingConfig,
};

mod outbox_capture;
pub use outbox_capture::{
    MysqlOutboxCapture, OUTBOX_SCHEMA_SENTINEL, OutboxColumns,
    OutboxProcessorCfg, PgOutboxCapture,
};

mod filter_cfg;
pub use filter_cfg::{
    FieldOp, FieldPredicate, FilterProcessorCfg, MatchMode, OpFilter,
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

    /// Key-value labels for filtering, grouping, and Grafana variables.
    /// Example: `{"env": "prod", "team": "platform", "tier": "critical"}`
    #[serde(default)]
    pub labels: std::collections::HashMap<String, String>,

    /// Free-form annotations for non-filtering metadata (docs, links, ownership).
    /// Example: `{"owner": "team-platform@company.com", "runbook": "https://..."}`
    #[serde(default)]
    pub annotations: std::collections::HashMap<String, String>,
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

    /// Event journal configuration (DLQ, future replay).
    /// When enabled, failed events are routed to a dead letter queue
    /// instead of blocking the pipeline.
    #[serde(default)]
    pub journal: Option<JournalConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum OnSchemaDrift {
    /// Detect drift, record it, reload the schema cache, and continue streaming.
    /// Safe for additive drift (new columns on B). Risky if B is missing columns
    /// that A had - row events may decode incorrectly against the wrong schema.
    #[default]
    Adapt,
    /// Stop the source when any schema drift is detected after failover.
    /// Requires operator intervention: verify B's schema, apply missing migrations,
    /// then restart. Use this when DDL sync to replicas is not guaranteed.
    Halt,
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
    #[serde(default)]
    pub outbox: Option<PgOutboxCapture>,
    #[serde(default)]
    pub snapshot: SnapshotCfg,
    /// What to do when schema drift is detected after failover.
    #[serde(default)]
    pub on_schema_drift: OnSchemaDrift,
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
    #[serde(default)]
    pub outbox: Option<MysqlOutboxCapture>,
    #[serde(default)]
    pub snapshot: SnapshotCfg,
    /// What to do when schema drift is detected after failover.
    #[serde(default)]
    pub on_schema_drift: OnSchemaDrift,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "config", rename_all = "lowercase")]
pub enum SourceCfg {
    Postgres(PostgresSrcCfg),
    Mysql(MysqlSrcCfg),
    #[cfg(feature = "turso")]
    Turso(turso_cfg::TursoSrcCfg),
}

impl SourceCfg {
    pub fn source_id(&self) -> &str {
        match self {
            SourceCfg::Postgres(c) => &c.id,
            SourceCfg::Mysql(c) => &c.id,
            #[cfg(feature = "turso")]
            SourceCfg::Turso(c) => &c.id,
        }
    }
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
    #[serde(rename = "outbox")]
    Outbox {
        #[serde(flatten)]
        config: Box<OutboxProcessorCfg>,
    },
    #[serde(rename = "flatten")]
    Flatten {
        #[serde(flatten)]
        config: Box<FlattenProcessorCfg>,
    },
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
            max_events: Some(2000),
            max_bytes: Some(16 * 1024 * 1024),
            max_ms: Some(50),
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
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
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

// ── Event journal (DLQ, replay) ──────────────────────────────────────────────

/// Top-level journal configuration. Opt-in: `enabled: false` by default.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct JournalConfig {
    /// Master switch. When false, no journal streams are active.
    pub enabled: bool,
    /// Maximum event payload size (bytes) stored in journal entries.
    /// Payloads exceeding this are truncated with `payload_truncated: true`.
    pub max_event_bytes: usize,
    /// DLQ stream configuration.
    pub dlq: DlqStreamConfig,
}

impl Default for JournalConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_event_bytes: 256 * 1024, // 256KB
            dlq: DlqStreamConfig::default(),
        }
    }
}

/// Dead letter queue stream configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DlqStreamConfig {
    /// Maximum number of entries in the DLQ queue.
    pub max_entries: u64,
    /// Auto-purge entries older than this (seconds). 0 = no age limit.
    pub max_age_secs: u64,
    /// What happens when the DLQ reaches max_entries.
    pub overflow_policy: OverflowPolicy,
}

impl Default for DlqStreamConfig {
    fn default() -> Self {
        Self {
            max_entries: 10_000,
            max_age_secs: 7 * 24 * 3600, // 7 days
            overflow_policy: OverflowPolicy::default(),
        }
    }
}

/// Behavior when the DLQ queue is full.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum OverflowPolicy {
    /// Remove oldest entry, admit newest. Most recent failures are usually
    /// most valuable for investigation.
    #[default]
    DropOldest,
    /// Reject the write. The failed event is not persisted and is dropped
    /// after an error log.
    Reject,
    /// Block the pipeline until space is available (operator acks entries).
    /// Visible via health endpoint as `degraded`.
    Block,
}

pub fn load_from_path(file_path: &str) -> ConfigResult<PipelineSpec> {
    let raw = fs::read_to_string(file_path).map_err(|e| ConfigError::Io {
        path: file_path.to_owned(),
        source: e,
    })?;

    let with_env = shellexpand::env_with_context(&raw, |var_name: &str| {
        match std::env::var(var_name) {
            Ok(val) => Ok(Some(val)),
            Err(_) => {
                // Unknown variable — pass through as-is.
                // This allows routing templates like ${source.table}
                // to coexist with env vars like ${KAFKA_BROKERS}.
                warn!(
                    var = var_name,
                    "unresolved variable, passing through as template"
                );
                Ok::<Option<String>, std::env::VarError>(Some(format!(
                    "${{{}}}",
                    var_name
                )))
            }
        }
    })
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
