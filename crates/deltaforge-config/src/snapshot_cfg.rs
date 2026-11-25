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

/// How a snapshot meets the change stream.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotStrategy {
    /// One anchor: the tables are copied in read views opened at it, and
    /// the stream continues from it. The only strategy.
    #[default]
    Anchor,
}

/// Initial snapshot configuration for PostgreSQL and MySQL sources.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotCfg {
    /// When to run the snapshot.
    #[serde(default)]
    pub mode: SnapshotMode,

    /// How the snapshot meets the change stream (`anchor`, the default and
    /// only strategy).
    #[serde(default)]
    pub strategy: SnapshotStrategy,

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

    /// Bound (seconds) on opening the snapshot's read views at its anchor
    /// (MySQL: every worker connection opened and its consistent snapshot
    /// started; no lock is taken on the source). Past it the snapshot fails
    /// closed. `lock_timeout_secs` is the earlier name, still accepted.
    #[serde(
        default = "default_anchor_timeout_secs",
        alias = "lock_timeout_secs"
    )]
    pub anchor_timeout_secs: u64,

    /// Tables read per catalog query while discovering the tables a snapshot
    /// copies (keyset pages in byte order of `(schema, table)`). Bounded:
    /// `1..=10000`; anything else is rejected when the configuration loads.
    #[serde(
        default = "default_discovery_page_size",
        deserialize_with = "deserialize_discovery_page_size"
    )]
    pub discovery_page_size: usize,

    /// This source's share of the process-wide snapshot connection cap
    /// (`--max-snapshot-connections`): every connection its snapshot opens
    /// counts. Default: `max_parallel_tables x max_parallel_chunks + 2`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_snapshot_connections: Option<u32>,

    /// How long a snapshot generation may hold its anchor (the source's
    /// read view, and the log retained for the stream after it). A warning
    /// incident at 80%; at the limit the generation stops and blocks
    /// (`snapshot_anchor_unavailable`) until an explicit resnapshot.
    #[serde(default = "default_max_anchor_age_secs")]
    pub max_anchor_age_secs: u64,

    /// Bounds of a generation's durable plan (its stored table entries),
    /// checked during discovery: a warning incident at 80%; at either limit
    /// discovery stops before the plan is sealed and the generation blocks
    /// (`snapshot_bound_exceeded`).
    #[serde(default = "default_max_plan_bytes")]
    pub max_plan_bytes: u64,
    #[serde(default = "default_max_plan_items")]
    pub max_plan_items: u64,
}

/// Bounds of [`SnapshotCfg::discovery_page_size`].
pub const DISCOVERY_PAGE_SIZE_MIN: usize = 1;
pub const DISCOVERY_PAGE_SIZE_MAX: usize = 10_000;

impl Default for SnapshotCfg {
    fn default() -> Self {
        Self {
            mode: SnapshotMode::Never,
            max_parallel_tables: default_parallel_tables(),
            chunk_size: default_chunk_size(),
            intra_table_parallel: false,
            max_parallel_chunks: default_parallel_chunks(),
            strategy: SnapshotStrategy::default(),
            anchor_timeout_secs: default_anchor_timeout_secs(),
            discovery_page_size: default_discovery_page_size(),
            max_snapshot_connections: None,
            max_anchor_age_secs: default_max_anchor_age_secs(),
            max_plan_bytes: default_max_plan_bytes(),
            max_plan_items: default_max_plan_items(),
        }
    }
}

impl SnapshotCfg {
    /// This source's snapshot connection share (see
    /// [`SnapshotCfg::max_snapshot_connections`]).
    pub fn snapshot_connection_cap(&self) -> u32 {
        self.max_snapshot_connections.unwrap_or_else(|| {
            let n = self
                .max_parallel_tables
                .max(1)
                .saturating_mul(self.max_parallel_chunks.max(1))
                .saturating_add(2);
            u32::try_from(n).unwrap_or(u32::MAX)
        })
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
fn default_anchor_timeout_secs() -> u64 {
    10
}
fn default_discovery_page_size() -> usize {
    1_000
}
fn default_max_anchor_age_secs() -> u64 {
    24 * 60 * 60
}
fn default_max_plan_bytes() -> u64 {
    256 * 1024 * 1024
}
fn default_max_plan_items() -> u64 {
    1_000_000
}

fn deserialize_discovery_page_size<'de, D>(d: D) -> Result<usize, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let n = usize::deserialize(d)?;
    if !(DISCOVERY_PAGE_SIZE_MIN..=DISCOVERY_PAGE_SIZE_MAX).contains(&n) {
        return Err(serde::de::Error::custom(format!(
            "snapshot.discovery_page_size must be between \
             {DISCOVERY_PAGE_SIZE_MIN} and {DISCOVERY_PAGE_SIZE_MAX}, got {n}"
        )));
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discovery_page_size_defaults_and_is_bounded() {
        let cfg: SnapshotCfg = serde_json::from_str("{}").unwrap();
        assert_eq!(cfg.discovery_page_size, 1_000);
        let cfg: SnapshotCfg =
            serde_json::from_str(r#"{"discovery_page_size": 10000}"#).unwrap();
        assert_eq!(cfg.discovery_page_size, 10_000);
        for bad in ["0", "10001"] {
            let err = serde_json::from_str::<SnapshotCfg>(&format!(
                r#"{{"discovery_page_size": {bad}}}"#
            ))
            .unwrap_err();
            assert!(err.to_string().contains("discovery_page_size"), "{err}");
        }
    }

    #[test]
    fn the_anchor_timeout_keeps_its_earlier_name() {
        let cfg: SnapshotCfg =
            serde_json::from_str(r#"{"lock_timeout_secs": 3}"#).unwrap();
        assert_eq!(cfg.anchor_timeout_secs, 3);
        let cfg: SnapshotCfg =
            serde_json::from_str(r#"{"anchor_timeout_secs": 4}"#).unwrap();
        assert_eq!(cfg.anchor_timeout_secs, 4);
    }

    #[test]
    fn the_strategy_is_anchor_and_nothing_else() {
        let cfg: SnapshotCfg = serde_json::from_str("{}").unwrap();
        assert_eq!(cfg.strategy, SnapshotStrategy::Anchor);
        let cfg: SnapshotCfg =
            serde_json::from_str(r#"{"strategy": "anchor"}"#).unwrap();
        assert_eq!(cfg.strategy, SnapshotStrategy::Anchor);
        assert!(
            serde_json::from_str::<SnapshotCfg>(r#"{"strategy": "chunked"}"#)
                .is_err()
        );
    }
}
