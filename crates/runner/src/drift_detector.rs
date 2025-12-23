//! Schema drift detection.
//!
//! Compares expected database schema against observed data patterns
//! to detect mismatches, unexpected nulls, and type drift.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::schema_provider::{ColumnSchemaInfo, TableSchemaInfo};

/// Type of schema drift detected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DriftType {
    /// Column expected to be non-null has null values
    UnexpectedNull,
    /// Observed type doesn't match declared type
    TypeMismatch,
    /// Column exists in data but not in schema
    UndeclaredColumn,
    /// Column exists in schema but never seen in data
    MissingColumn,
    /// JSON structure changed (for JSON columns)
    JsonStructureChange,
    /// Numeric value outside expected range
    ValueOutOfRange,
}

/// A single drift observation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriftEvent {
    /// Table where drift was detected
    pub table: String,
    /// Column where drift was detected
    pub column: String,
    /// Type of drift
    pub drift_type: DriftType,
    /// Expected value/type from schema
    pub expected: String,
    /// Actual observed value/type
    pub observed: String,
    /// When drift was first detected
    pub first_seen: DateTime<Utc>,
    /// Number of occurrences
    pub count: u64,
    /// Sample values that caused drift (limited)
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub samples: Vec<String>,
}

/// Statistics for a single column.
#[derive(Debug, Clone, Default)]
pub struct ColumnStats {
    /// Total observations
    pub total_count: u64,
    /// Null count
    pub null_count: u64,
    /// Observed JSON types (for type tracking)
    pub observed_types: HashMap<String, u64>,
    /// Whether column was ever seen
    pub seen: bool,
}

impl ColumnStats {
    /// Calculate null rate.
    pub fn null_rate(&self) -> f64 {
        if self.total_count == 0 {
            0.0
        } else {
            self.null_count as f64 / self.total_count as f64
        }
    }

    /// Get the dominant observed type.
    pub fn dominant_type(&self) -> Option<&str> {
        self.observed_types
            .iter()
            .max_by_key(|(_, count)| *count)
            .map(|(t, _)| t.as_str())
    }
}

/// Tracks schema drift for a single table.
#[derive(Debug)]
pub struct TableDriftTracker {
    /// Expected schema from database
    pub expected_schema: TableSchemaInfo,
    /// Per-column statistics
    pub column_stats: HashMap<String, ColumnStats>,
    /// Detected drift events
    pub drift_events: Vec<DriftEvent>,
    /// Columns seen in data but not in schema
    pub undeclared_columns: HashMap<String, u64>,
    /// Configuration
    config: DriftConfig,
}

/// Configuration for drift detection.
#[derive(Debug, Clone)]
pub struct DriftConfig {
    /// Null rate threshold to trigger warning (e.g., 0.01 = 1%)
    pub null_rate_threshold: f64,
    /// Maximum samples to keep per drift event
    pub max_samples: usize,
    /// Whether to track undeclared columns
    pub track_undeclared: bool,
}

impl Default for DriftConfig {
    fn default() -> Self {
        Self {
            null_rate_threshold: 0.001, // 0.1%
            max_samples: 5,
            track_undeclared: true,
        }
    }
}

impl TableDriftTracker {
    /// Create a new tracker for a table.
    pub fn new(schema: TableSchemaInfo) -> Self {
        Self::with_config(schema, DriftConfig::default())
    }

    /// Create with custom configuration.
    pub fn with_config(schema: TableSchemaInfo, config: DriftConfig) -> Self {
        let mut column_stats = HashMap::new();
        for col in &schema.columns {
            column_stats.insert(col.name.clone(), ColumnStats::default());
        }

        Self {
            expected_schema: schema,
            column_stats,
            drift_events: Vec::new(),
            undeclared_columns: HashMap::new(),
            config,
        }
    }

    /// Observe a row and check for drift.
    pub fn observe_row(&mut self, row: &serde_json::Value) {
        let Some(obj) = row.as_object() else {
            return;
        };

        // Collect column names we expect (owned to avoid borrow conflict)
        let expected_columns: Vec<String> = self
            .expected_schema
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect();

        // Track which declared columns we see
        let mut seen_columns: HashMap<String, bool> = expected_columns
            .iter()
            .map(|name| (name.clone(), false))
            .collect();

        for (col_name, value) in obj {
            if let Some(seen) = seen_columns.get_mut(col_name) {
                *seen = true;
                self.observe_declared_column(col_name, value);
            } else if self.config.track_undeclared {
                // Column not in schema
                *self
                    .undeclared_columns
                    .entry(col_name.clone())
                    .or_default() += 1;
            }
        }

        // Check for missing columns (in schema but not in row)
        for (col_name, seen) in seen_columns {
            if !seen {
                if let Some(stats) = self.column_stats.get_mut(&col_name) {
                    stats.total_count += 1;
                    // Missing is treated as null for nullable columns
                    if let Some(col_info) =
                        self.expected_schema.column(&col_name)
                    {
                        if !col_info.nullable {
                            stats.null_count += 1;
                        }
                    }
                }
            }
        }
    }

    fn observe_declared_column(
        &mut self,
        col_name: &str,
        value: &serde_json::Value,
    ) {
        let Some(col_info) = self.expected_schema.column(col_name) else {
            return;
        };

        let stats = self.column_stats.entry(col_name.to_string()).or_default();

        stats.total_count += 1;
        stats.seen = true;

        // Check for null
        if value.is_null() {
            stats.null_count += 1;

            // Drift: unexpected null
            if !col_info.nullable && stats.null_count == 1 {
                self.record_drift(DriftEvent {
                    table: self.expected_schema.table.clone(),
                    column: col_name.to_string(),
                    drift_type: DriftType::UnexpectedNull,
                    expected: "NOT NULL".to_string(),
                    observed: "NULL".to_string(),
                    first_seen: Utc::now(),
                    count: 1,
                    samples: vec![],
                });
            }
            return;
        }

        // Track observed type
        let observed_type = json_type_name(value);
        *stats
            .observed_types
            .entry(observed_type.clone())
            .or_default() += 1;

        // Check for type mismatch
        if let Some(drift) = check_type_mismatch(col_info, value) {
            self.record_drift(DriftEvent {
                table: self.expected_schema.table.clone(),
                column: col_name.to_string(),
                drift_type: DriftType::TypeMismatch,
                expected: col_info.data_type.clone(),
                observed: drift,
                first_seen: Utc::now(),
                count: 1,
                samples: vec![value.to_string().chars().take(100).collect()],
            });
        }
    }

    fn record_drift(&mut self, event: DriftEvent) {
        // Check if we already have this drift
        if let Some(existing) = self.drift_events.iter_mut().find(|e| {
            e.table == event.table
                && e.column == event.column
                && e.drift_type == event.drift_type
        }) {
            existing.count += 1;
            if existing.samples.len() < self.config.max_samples {
                existing.samples.extend(event.samples);
            }
        } else {
            warn!(
                table = %event.table,
                column = %event.column,
                drift_type = ?event.drift_type,
                expected = %event.expected,
                observed = %event.observed,
                "schema drift detected"
            );
            self.drift_events.push(event);
        }
    }

    /// Get columns with high null rates (above threshold).
    pub fn high_null_columns(&self) -> Vec<(&str, f64)> {
        self.column_stats
            .iter()
            .filter(|(name, stats)| {
                let col = self.expected_schema.column(name);
                let expected_nullable = col.map(|c| c.nullable).unwrap_or(true);
                !expected_nullable
                    && stats.null_rate() > self.config.null_rate_threshold
            })
            .map(|(name, stats)| (name.as_str(), stats.null_rate()))
            .collect()
    }

    /// Get undeclared columns (in data but not in schema).
    pub fn undeclared(&self) -> &HashMap<String, u64> {
        &self.undeclared_columns
    }

    /// Get all drift events.
    pub fn drift_events(&self) -> &[DriftEvent] {
        &self.drift_events
    }

    /// Generate a drift summary.
    pub fn summary(&self) -> DriftSummary {
        DriftSummary {
            table: self.expected_schema.table.clone(),
            total_drift_events: self.drift_events.len(),
            unexpected_nulls: self
                .drift_events
                .iter()
                .filter(|e| e.drift_type == DriftType::UnexpectedNull)
                .count(),
            type_mismatches: self
                .drift_events
                .iter()
                .filter(|e| e.drift_type == DriftType::TypeMismatch)
                .count(),
            undeclared_columns: self.undeclared_columns.len(),
            high_null_columns: self.high_null_columns().len(),
        }
    }
}

/// Summary of drift for a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriftSummary {
    pub table: String,
    pub total_drift_events: usize,
    pub unexpected_nulls: usize,
    pub type_mismatches: usize,
    pub undeclared_columns: usize,
    pub high_null_columns: usize,
}

/// Get JSON type name.
fn json_type_name(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::Bool(_) => "boolean".to_string(),
        serde_json::Value::Number(n) => {
            if n.is_i64() {
                "integer".to_string()
            } else {
                "number".to_string()
            }
        }
        serde_json::Value::String(_) => "string".to_string(),
        serde_json::Value::Array(_) => "array".to_string(),
        serde_json::Value::Object(_) => "object".to_string(),
    }
}

/// Check if observed JSON value mismatches expected SQL type.
fn check_type_mismatch(
    col: &ColumnSchemaInfo,
    value: &serde_json::Value,
) -> Option<String> {
    let sql_type = col.data_type.to_lowercase();

    match value {
        serde_json::Value::Bool(_) => {
            if !matches!(
                sql_type.as_str(),
                "bool" | "boolean" | "tinyint" | "bit"
            ) {
                Some("boolean".to_string())
            } else {
                None
            }
        }
        serde_json::Value::Number(n) => {
            let is_int = n.is_i64() || n.is_u64();
            let expects_int = matches!(
                sql_type.as_str(),
                "int"
                    | "integer"
                    | "bigint"
                    | "smallint"
                    | "tinyint"
                    | "mediumint"
                    | "serial"
                    | "bigserial"
            );
            let expects_float = matches!(
                sql_type.as_str(),
                "float" | "double" | "real" | "decimal" | "numeric"
            );

            if is_int && !expects_int && !expects_float {
                Some("integer".to_string())
            } else if !is_int && expects_int {
                Some("float".to_string())
            } else {
                None
            }
        }
        serde_json::Value::String(_) => {
            // Strings are compatible with most types (due to serialization)
            // Only flag if column is strictly numeric
            let strictly_numeric = matches!(
                sql_type.as_str(),
                "int" | "integer" | "bigint" | "float" | "double"
            );
            if strictly_numeric {
                Some("string".to_string())
            } else {
                None
            }
        }
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            // Complex types should only go into JSON columns
            if !col.is_json_like {
                Some(json_type_name(value))
            } else {
                None
            }
        }
        serde_json::Value::Null => None, // Handled separately
    }
}

/// Drift detector that manages multiple tables.
#[derive(Debug, Default)]
pub struct DriftDetector {
    /// Per-table trackers
    trackers: HashMap<String, TableDriftTracker>,
    /// Global config
    config: DriftConfig,
}

impl DriftDetector {
    /// Create a new drift detector.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create with custom configuration.
    pub fn with_config(config: DriftConfig) -> Self {
        Self {
            trackers: HashMap::new(),
            config,
        }
    }

    /// Register a table schema for drift tracking.
    pub fn register_table(&mut self, schema: TableSchemaInfo) {
        let table_key = format!("{}.{}", schema.database, schema.table);
        self.trackers.insert(
            table_key,
            TableDriftTracker::with_config(schema, self.config.clone()),
        );
    }

    /// Observe a row for a table.
    pub fn observe(&mut self, table: &str, row: &serde_json::Value) {
        if let Some(tracker) = self.trackers.get_mut(table) {
            tracker.observe_row(row);
        }
    }

    /// Get tracker for a table.
    pub fn tracker(&self, table: &str) -> Option<&TableDriftTracker> {
        self.trackers.get(table)
    }

    /// Get all drift summaries.
    pub fn all_summaries(&self) -> Vec<DriftSummary> {
        self.trackers.values().map(|t| t.summary()).collect()
    }

    /// Check if any drift has been detected.
    pub fn has_drift(&self) -> bool {
        self.trackers.values().any(|t| !t.drift_events.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn test_schema() -> TableSchemaInfo {
        TableSchemaInfo {
            database: "shop".into(),
            table: "orders".into(),
            columns: vec![
                ColumnSchemaInfo {
                    name: "id".into(),
                    data_type: "int".into(),
                    full_type: "int".into(),
                    nullable: false,
                    is_json_like: false,
                },
                ColumnSchemaInfo {
                    name: "customer_id".into(),
                    data_type: "int".into(),
                    full_type: "int".into(),
                    nullable: false,
                    is_json_like: false,
                },
                ColumnSchemaInfo {
                    name: "notes".into(),
                    data_type: "text".into(),
                    full_type: "text".into(),
                    nullable: true,
                    is_json_like: false,
                },
                ColumnSchemaInfo {
                    name: "metadata".into(),
                    data_type: "json".into(),
                    full_type: "json".into(),
                    nullable: true,
                    is_json_like: true,
                },
            ],
            primary_key: vec!["id".into()],
        }
    }

    #[test]
    fn test_no_drift_on_valid_data() {
        let mut tracker = TableDriftTracker::new(test_schema());

        tracker.observe_row(&json!({
            "id": 1,
            "customer_id": 100,
            "notes": "test",
            "metadata": {"key": "value"}
        }));

        assert!(tracker.drift_events.is_empty());
    }

    #[test]
    fn test_detects_unexpected_null() {
        let mut tracker = TableDriftTracker::new(test_schema());

        tracker.observe_row(&json!({
            "id": 1,
            "customer_id": null,  // NOT NULL column!
            "notes": null,        // nullable, ok
            "metadata": null      // nullable, ok
        }));

        assert_eq!(tracker.drift_events.len(), 1);
        assert_eq!(tracker.drift_events[0].column, "customer_id");
        assert_eq!(
            tracker.drift_events[0].drift_type,
            DriftType::UnexpectedNull
        );
    }

    #[test]
    fn test_detects_type_mismatch() {
        let mut tracker = TableDriftTracker::new(test_schema());

        tracker.observe_row(&json!({
            "id": "not_an_int",  // string in int column
            "customer_id": 100,
            "notes": "ok",
            "metadata": {}
        }));

        assert_eq!(tracker.drift_events.len(), 1);
        assert_eq!(tracker.drift_events[0].column, "id");
        assert_eq!(tracker.drift_events[0].drift_type, DriftType::TypeMismatch);
    }

    #[test]
    fn test_tracks_undeclared_columns() {
        let mut tracker = TableDriftTracker::new(test_schema());

        tracker.observe_row(&json!({
            "id": 1,
            "customer_id": 100,
            "notes": "test",
            "metadata": {},
            "unknown_col": "surprise!"  // not in schema
        }));

        assert_eq!(tracker.undeclared_columns.get("unknown_col"), Some(&1));
    }

    #[test]
    fn test_drift_summary() {
        let mut tracker = TableDriftTracker::new(test_schema());

        tracker.observe_row(&json!({
            "id": 1,
            "customer_id": null,
            "notes": null,
            "metadata": {},
            "extra": "undeclared"
        }));

        let summary = tracker.summary();
        assert_eq!(summary.unexpected_nulls, 1);
        assert_eq!(summary.undeclared_columns, 1);
    }
}
