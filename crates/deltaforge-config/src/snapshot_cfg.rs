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

    /// Bounded timeout (seconds) for acquiring the snapshot consistency anchor -
    /// for MySQL, the window holding `FLUSH TABLES WITH READ LOCK` while worker
    /// snapshots open and the binlog position is captured. If the lock/setup
    /// cannot complete within this budget the snapshot fails closed rather than
    /// stalling writes on the source. The worker count under the lock is bounded
    /// by `max_parallel_tables` (reused; not a separate knob).
    #[serde(default = "default_lock_timeout_secs")]
    pub lock_timeout_secs: u64,
}

impl Default for SnapshotCfg {
    fn default() -> Self {
        Self {
            mode: SnapshotMode::Never,
            max_parallel_tables: default_parallel_tables(),
            chunk_size: default_chunk_size(),
            intra_table_parallel: false,
            max_parallel_chunks: default_parallel_chunks(),
            lock_timeout_secs: default_lock_timeout_secs(),
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
fn default_lock_timeout_secs() -> u64 {
    10
}
