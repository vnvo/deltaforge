use std::fs;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineSpec {
    pub metadata: Metadata,
    pub spec: Spec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    /// Pipeline name (unique)
    pub name: String,

    /// Business oriented tenant identifier
    pub tenant: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Spec {

    /// Sharding config for the pipeline
    pub sharding: Option<Sharding>,

    /// Multi source config
    pub sources: Vec<SourceCfg>,

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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SourceCfg {
    #[serde(rename = "postgres")]
    Postgres {
        id: String,
        dsn: String,
        publication: Option<String>,
        slot: Option<String>,
        tables: Vec<String>,
    },
    #[serde(rename = "mysql")]
    Mysql {
        id: String,
        dsn: String,
        tables: Vec<String>,
    },
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SinkCfg {
    #[serde(rename = "kafka")]
    Kafka {
        id: String,
        brokers: String,
        topic: String,
        exactly_once: Option<bool>,
    },

    #[serde(rename = "redis")]
    Redis {
        id: String,
        uri: String,
        stream: String,
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
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
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

/// Per-component performance knobs that do **not** change the commit unit.
/// Pipeline Components can optionally process the coordinator’s batch in smaller chunks.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct MicrobatchConfig {
    pub max_events: Option<usize>,
    pub max_bytes: Option<usize>,
    pub max_ms: Option<u64>,
}

/// How sink acks gate checkpointing when multiple sinks are configured.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum CommitPolicy {
    /// All sinks (or all marked as required) must ack the batch.
    All,
    /// Only sinks marked as `required: true` must ack; optional sinks are best-effort.
    Required,
    /// Checkpoint after at least `quorum` sinks ack (availability over consistency).
    Quorum { quorum: usize },
}

impl Default for CommitPolicy {
    fn default() -> Self { CommitPolicy::Required }
}

pub fn load_from_path(path: &str) -> Result<PipelineSpec> {
    let raw = fs::read_to_string(path).with_context(|| format!("reading config {path}"))?;
    let with_env = shellexpand::env(&raw).unwrap().to_string();
    let spec: PipelineSpec = serde_yaml::from_str(&with_env).with_context(|| "parsing yaml")?;

    Ok(spec)
}
