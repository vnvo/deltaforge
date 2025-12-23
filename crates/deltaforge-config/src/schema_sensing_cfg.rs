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
}
