use serde::{Deserialize, Serialize};

/// When to run the initial snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotMode {
    /// Only on first run (no checkpoint). Default.
    #[default]
    Initial,
    /// Always snapshot on pipeline start, even if a checkpoint exists.
    Always,
    /// Skip snapshot entirely - stream from current WAL position.
    Never,
}

/// Initial snapshot configuration for PostgreSQL sources.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotCfg {
    /// When to run the snapshot.
    #[serde(default)]
    pub mode: SnapshotMode,

    /// Max tables snapshotted concurrently.
    /// Default: 8 (or table count if smaller).
    #[serde(default = "default_parallel_tables")]
    pub max_parallel_tables: usize,

    /// Rows per read batch.
    #[serde(default = "default_chunk_size")]
    pub chunk_size: usize,

    /// Parallelise reads within a single large table.
    /// Enable for database/DW sinks. Disable for Kafka (partition bottleneck).
    #[serde(default)]
    pub intra_table_parallel: bool,

    /// Max parallel chunks per table (only used when intra_table_parallel = true).
    #[serde(default = "default_parallel_chunks")]
    pub max_parallel_chunks: usize,
}

impl Default for SnapshotCfg {
    fn default() -> Self {
        Self {
            mode: SnapshotMode::Initial,
            max_parallel_tables: default_parallel_tables(),
            chunk_size: default_chunk_size(),
            intra_table_parallel: false,
            max_parallel_chunks: default_parallel_chunks(),
        }
    }
}

fn default_parallel_tables() -> usize {
    8
}
fn default_chunk_size() -> usize {
    10_000
}
fn default_parallel_chunks() -> usize {
    4
}
