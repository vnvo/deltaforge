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
    pub name: String,
    pub tenant: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Spec {
    pub sharding: Option<Sharding>,
    pub sources: Vec<SourceCfg>,
    pub processors: Vec<ProcessorCfg>,
    pub sinks: Vec<SinkCfg>,
    pub connection_policy: Option<ConnectionPolicy>,
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

pub fn load_from_path(path: &str) -> Result<PipelineSpec> {
    let raw = fs::read_to_string(path).with_context(|| format!("reading config {path}"))?;
    let with_env = shellexpand::env(&raw).unwrap().to_string();
    let spec: PipelineSpec = serde_yaml::from_str(&with_env).with_context(|| "parsing yaml")?;

    Ok(spec)
}
