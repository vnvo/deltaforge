//! Schema sensing configuration.
//!
//! Controls automatic schema inference from event payloads.
//! This is a pipeline-level feature that works across all source types.

use serde::{Deserialize, Serialize};

/// Schema sensing configuration.
///
/// When enabled, DeltaForge automatically infers and tracks schema
/// from event payloads, including deep inspection of JSON columns.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SchemaSensingConfig {
    /// Master switch - disabled by default to avoid overhead
    #[serde(default)]
    pub enabled: bool,

    /// Table filtering - which tables to sense
    #[serde(default)]
    pub tables: TableFilter,

    /// Deep inspection of JSON/JSONB/TEXT columns
    #[serde(default)]
    pub deep_inspect: DeepInspectConfig,

    /// What statistics to track
    #[serde(default)]
    pub tracking: TrackingConfig,

    /// Output options
    #[serde(default)]
    pub output: SensingOutputConfig,

    /// Sampling configuration for performance optimization
    #[serde(default)]
    pub sampling: SamplingConfig,

    /// High-cardinality field detection.
    #[serde(default)]
    pub high_cardinality: HighCardinalityConfig,
}

/// Filter which tables to apply schema sensing to.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TableFilter {
    /// Tables to include (if empty, all tables are included).
    /// Supports patterns: `orders`, `audit_%`, `*`
    #[serde(default)]
    pub include: Vec<String>,

    /// Tables to exclude (evaluated after include).
    /// Supports patterns: `_df_%`, `temp_*`
    #[serde(default)]
    pub exclude: Vec<String>,
}

/// Configuration for deep inspection of JSON columns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeepInspectConfig {
    /// Whether to inspect inside JSON/JSONB/TEXT columns.
    /// When false, JSON columns are treated as opaque blobs.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Maximum nesting depth for object/array inspection.
    /// Deeper structures are treated as opaque JSON.
    #[serde(default = "default_max_depth")]
    pub max_depth: usize,

    /// Stop detailed tracking after this many samples per table.
    /// Reduces overhead once schema has stabilized.
    /// Set to 0 for unlimited sampling.
    #[serde(default = "default_sample_size")]
    pub max_sample_size: usize,

    /// Column filtering for deep inspection.
    /// Only matching columns get deep-inspected.
    #[serde(default)]
    pub columns: ColumnFilter,
}

impl Default for DeepInspectConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_depth: default_max_depth(),
            max_sample_size: default_sample_size(),
            columns: ColumnFilter::default(),
        }
    }
}

/// Filter which columns to deep-inspect.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ColumnFilter {
    /// Columns to include for deep inspection.
    /// If empty, all JSON-like columns are inspected.
    #[serde(default)]
    pub include: Vec<String>,

    /// Columns to exclude from deep inspection.
    #[serde(default)]
    pub exclude: Vec<String>,
}

/// Configuration for what statistics to track.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackingConfig {
    /// Track min/max/cardinality for numeric fields.
    /// Adds memory overhead but useful for data profiling.
    #[serde(default)]
    pub field_statistics: bool,

    /// Track null rates per field.
    #[serde(default = "default_true")]
    pub null_rates: bool,

    /// Detect enum-like string fields (low cardinality).
    #[serde(default = "default_true")]
    pub enum_detection: bool,

    /// Maximum unique values to consider a string field an enum.
    /// Fields with more unique values are treated as free-form strings.
    #[serde(default = "default_enum_cardinality")]
    pub enum_max_cardinality: usize,
}

impl Default for TrackingConfig {
    fn default() -> Self {
        Self {
            field_statistics: false,
            null_rates: true,
            enum_detection: true,
            enum_max_cardinality: default_enum_cardinality(),
        }
    }
}

/// Output options for sensed schemas.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SensingOutputConfig {
    /// Expose inferred schemas via REST API.
    #[serde(default = "default_true")]
    pub expose_api: bool,

    /// Emit schema change events to sinks.
    /// Creates `_schema_change` events when schema evolves.
    #[serde(default)]
    pub emit_events: bool,

    /// Enable JSON Schema export endpoint.
    #[serde(default = "default_true")]
    pub export_json_schema: bool,
}

impl Default for SensingOutputConfig {
    fn default() -> Self {
        Self {
            expose_api: true,
            emit_events: false,
            export_json_schema: true,
        }
    }
}

/// Sampling configuration for performance optimization.
///
/// After warmup, sensing switches to sampling mode to reduce overhead
/// while still detecting schema changes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SamplingConfig {
    /// Number of events to fully sense before switching to sampling mode.
    /// During warmup, every event is analyzed to build initial schema.
    #[serde(default = "default_warmup_events")]
    pub warmup_events: usize,

    /// Sample rate after warmup (1 = 100%, 10 = 10%, 100 = 1%).
    /// Higher values = less CPU overhead but slower schema evolution detection.
    #[serde(default = "default_sample_rate")]
    pub sample_rate: usize,

    /// Use structure fingerprinting to skip identical payloads.
    /// When enabled, payloads with the same key structure as previously
    /// seen are skipped entirely, providing significant speedup for
    /// homogeneous data.
    #[serde(default = "default_true")]
    pub structure_cache: bool,

    /// Maximum number of unique structures to cache per table.
    /// Prevents unbounded memory growth for highly variable schemas.
    #[serde(default = "default_structure_cache_size")]
    pub structure_cache_size: usize,
}

impl Default for SamplingConfig {
    fn default() -> Self {
        Self {
            warmup_events: default_warmup_events(),
            sample_rate: default_sample_rate(),
            structure_cache: true,
            structure_cache_size: default_structure_cache_size(),
        }
    }
}

// Default value functions
fn default_true() -> bool {
    true
}

fn default_max_depth() -> usize {
    10
}

fn default_sample_size() -> usize {
    1000
}

fn default_enum_cardinality() -> usize {
    50
}

fn default_warmup_events() -> usize {
    1000
}

fn default_sample_rate() -> usize {
    10 // 10% sampling after warmup
}

fn default_structure_cache_size() -> usize {
    100
}

impl SchemaSensingConfig {
    /// Check if sensing is enabled for a given table.
    pub fn should_sense_table(&self, table: &str) -> bool {
        if !self.enabled {
            return false;
        }
        self.tables.matches(table)
    }

    /// Check if a column should be deep-inspected.
    pub fn should_deep_inspect(&self, column: &str) -> bool {
        if !self.enabled || !self.deep_inspect.enabled {
            return false;
        }
        self.deep_inspect.columns.matches(column)
    }

    /// Get the max depth for inspection.
    pub fn max_depth(&self) -> usize {
        if self.deep_inspect.enabled {
            self.deep_inspect.max_depth
        } else {
            1
        }
    }

    /// Check if we're in warmup phase for a given event count.
    pub fn is_warmup(&self, event_count: u64) -> bool {
        event_count < self.sampling.warmup_events as u64
    }

    /// Check if an event should be sampled (after warmup).
    pub fn should_sample(&self, event_count: u64) -> bool {
        if self.is_warmup(event_count) {
            return true; // Always sample during warmup
        }
        // After warmup, sample at configured rate
        let rate = self.sampling.sample_rate.max(1);
        event_count.is_multiple_of(rate as u64)
    }
}

impl TableFilter {
    /// Check if a table matches the filter.
    pub fn matches(&self, table: &str) -> bool {
        // If include list is empty, include all
        let included = self.include.is_empty()
            || self.include.iter().any(|p| matches_pattern(p, table));

        // Check exclusions
        let excluded = self.exclude.iter().any(|p| matches_pattern(p, table));

        included && !excluded
    }
}

impl ColumnFilter {
    /// Check if a column matches the filter.
    pub fn matches(&self, column: &str) -> bool {
        // If include list is empty, include all
        let included = self.include.is_empty()
            || self.include.iter().any(|p| matches_pattern(p, column));

        // Check exclusions
        let excluded = self.exclude.iter().any(|p| matches_pattern(p, column));

        included && !excluded
    }
}

/// Simple pattern matching supporting `*` (any) and `%` (prefix).
fn matches_pattern(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix('%') {
        return value.starts_with(prefix);
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        return value.starts_with(prefix);
    }
    pattern == value
}

/// Configuration for high-cardinality field detection.
///
/// When enabled, schema sensing distinguishes between stable fields
/// (schema properties) and dynamic fields (map keys like UUIDs).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HighCardinalityConfig {
    /// Enable field classification for high-cardinality key detection.
    #[serde(default = "hc_default_true")]
    pub enabled: bool,

    /// Minimum events before making classification decisions.
    #[serde(default = "hc_default_min_events")]
    pub min_events: u64,

    /// Frequency threshold for stable fields (0.0 - 1.0).
    /// Fields appearing in >= this fraction of events are stable.
    #[serde(default = "hc_default_stable_threshold")]
    pub stable_threshold: f64,

    /// Minimum dynamic fields to classify object as having map component.
    #[serde(default = "hc_default_min_dynamic_fields")]
    pub min_dynamic_fields: usize,

    /// Confidence threshold for classification decisions.
    #[serde(default = "hc_default_confidence_threshold")]
    pub confidence_threshold: f64,

    /// Re-evaluate classification every N events (0 = never).
    #[serde(default = "hc_default_reevaluate_interval")]
    pub reevaluate_interval: u64,

    /// HyperLogLog precision (10-16).
    #[serde(default = "hc_default_hll_precision")]
    pub hll_precision: u8,

    /// Heavy hitter tracking capacity.
    #[serde(default = "hc_default_heavy_hitter_capacity")]
    pub heavy_hitter_capacity: usize,

    /// Reservoir sample size for pattern extraction.
    #[serde(default = "hc_default_sample_size")]
    pub sample_size: usize,
}

fn hc_default_true() -> bool {
    true
}
fn hc_default_min_events() -> u64 {
    100
}
fn hc_default_stable_threshold() -> f64 {
    0.5
}
fn hc_default_min_dynamic_fields() -> usize {
    5
}
fn hc_default_confidence_threshold() -> f64 {
    0.7
}
fn hc_default_reevaluate_interval() -> u64 {
    10_000
}
fn hc_default_hll_precision() -> u8 {
    12
}
fn hc_default_heavy_hitter_capacity() -> usize {
    50
}
fn hc_default_sample_size() -> usize {
    50
}

impl Default for HighCardinalityConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_events: 100,
            stable_threshold: 0.5,
            min_dynamic_fields: 5,
            confidence_threshold: 0.7,
            reevaluate_interval: 10_000,
            hll_precision: 12,
            heavy_hitter_capacity: 50,
            sample_size: 50,
        }
    }
}

impl HighCardinalityConfig {
    /// Estimated memory usage per tracked path.
    pub fn memory_per_path(&self) -> usize {
        let hll = 1 << self.hll_precision;
        let ss = self.heavy_hitter_capacity * 80;
        let rs = self.sample_size * 40;
        hll + ss + rs + 256
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_disabled() {
        let config = SchemaSensingConfig::default();
        assert!(!config.enabled);
        assert!(!config.should_sense_table("orders"));
    }

    #[test]
    fn test_enabled_senses_all_tables() {
        let config = SchemaSensingConfig {
            enabled: true,
            ..Default::default()
        };
        assert!(config.should_sense_table("orders"));
        assert!(config.should_sense_table("customers"));
    }

    #[test]
    fn test_table_include_filter() {
        let config = SchemaSensingConfig {
            enabled: true,
            tables: TableFilter {
                include: vec!["orders".into(), "audit_%".into()],
                exclude: vec![],
            },
            ..Default::default()
        };
        assert!(config.should_sense_table("orders"));
        assert!(config.should_sense_table("audit_log"));
        assert!(config.should_sense_table("audit_events"));
        assert!(!config.should_sense_table("customers"));
    }

    #[test]
    fn test_table_exclude_filter() {
        let config = SchemaSensingConfig {
            enabled: true,
            tables: TableFilter {
                include: vec![],
                exclude: vec!["_df_%".into(), "temp_*".into()],
            },
            ..Default::default()
        };
        assert!(config.should_sense_table("orders"));
        assert!(!config.should_sense_table("_df_cdc_changes"));
        assert!(!config.should_sense_table("temp_data"));
    }

    #[test]
    fn test_column_deep_inspect() {
        let config = SchemaSensingConfig {
            enabled: true,
            deep_inspect: DeepInspectConfig {
                enabled: true,
                columns: ColumnFilter {
                    include: vec!["metadata".into(), "payload".into()],
                    exclude: vec![],
                },
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(config.should_deep_inspect("metadata"));
        assert!(config.should_deep_inspect("payload"));
        assert!(!config.should_deep_inspect("name"));
    }

    #[test]
    fn test_pattern_matching() {
        assert!(matches_pattern("*", "anything"));
        assert!(matches_pattern("orders", "orders"));
        assert!(!matches_pattern("orders", "customers"));
        assert!(matches_pattern("audit_%", "audit_log"));
        assert!(matches_pattern("audit_*", "audit_log"));
        assert!(!matches_pattern("audit_%", "orders"));
    }

    #[test]
    fn test_warmup_phase() {
        let config = SchemaSensingConfig {
            enabled: true,
            sampling: SamplingConfig {
                warmup_events: 100,
                sample_rate: 10,
                ..Default::default()
            },
            ..Default::default()
        };

        // During warmup
        assert!(config.is_warmup(0));
        assert!(config.is_warmup(99));
        assert!(!config.is_warmup(100));
        assert!(!config.is_warmup(1000));
    }

    #[test]
    fn test_sampling_rate() {
        let config = SchemaSensingConfig {
            enabled: true,
            sampling: SamplingConfig {
                warmup_events: 10,
                sample_rate: 5, // 20% sampling
                ..Default::default()
            },
            ..Default::default()
        };

        // During warmup - always sample
        assert!(config.should_sample(0));
        assert!(config.should_sample(5));
        assert!(config.should_sample(9));

        // After warmup - sample every 5th
        assert!(config.should_sample(10)); // 10 % 5 == 0
        assert!(!config.should_sample(11));
        assert!(!config.should_sample(12));
        assert!(!config.should_sample(13));
        assert!(!config.should_sample(14));
        assert!(config.should_sample(15)); // 15 % 5 == 0
    }
}
