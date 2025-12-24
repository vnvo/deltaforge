use std::collections::HashMap;

use schema_analysis::InferredSchema;
use serde::de::DeserializeSeed;
use tracing::{debug, info, trace, warn};

use deltaforge_config::SchemaSensingConfig;

use crate::errors::{SensorError, SensorResult};
use crate::json_schema::JsonSchema;
use crate::schema_state::{SchemaSnapshot, SensedSchemaVersion, TableSchemaState};

/// Result of observing an event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObserveResult {
    /// Schema sensing is disabled for this table
    Disabled,

    /// First event for this table - schema initialized
    NewSchema { fingerprint: String, sequence: u64 },

    /// Schema changed from observing this event
    Evolved {
        old_fingerprint: String,
        new_fingerprint: String,
        old_sequence: u64,
        new_sequence: u64,
    },

    /// Schema unchanged
    Unchanged { fingerprint: String, sequence: u64 },

    /// Sampling limit reached - schema stabilized
    Stabilized { fingerprint: String, sequence: u64 },
}

/// Universal schema sensor.
///
/// Infers and tracks schema from JSON payloads across all tables.
/// Uses `schema_analysis` for robust type inference.
pub struct SchemaSensor {
    /// Configuration
    config: SchemaSensingConfig,

    /// Per-table schema state
    schemas: HashMap<String, TableSchemaState>,
}

impl SchemaSensor {
    pub fn new(config: SchemaSensingConfig) -> Self {
        Self {
            config,
            schemas: HashMap::new(),
        }
    }

    /// Create a sensor with sensing enabled (for testing).
    pub fn enabled() -> Self {
        Self::new(SchemaSensingConfig {
            enabled: true,
            ..Default::default()
        })
    }

    /// Check if sensing is enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Observe a JSON payload and update schema tracking.
    ///
    /// # Arguments
    /// * `table` - The table/entity name
    /// * `json` - Raw JSON bytes of the event payload
    ///
    /// # Returns
    /// Result indicating what happened (new schema, evolved, unchanged, etc.)
    pub fn observe(
        &mut self,
        table: &str,
        json: &[u8],
    ) -> SensorResult<ObserveResult> {
        // Check if sensing is enabled for this table
        if !self.config.should_sense_table(table) {
            return Ok(ObserveResult::Disabled);
        }

        // Check if we already have state for this table
        if let Some(state) = self.schemas.get_mut(table) {
            // Check if we've hit the sampling limit
            if state.stabilized {
                return Ok(ObserveResult::Stabilized {
                    fingerprint: state.fingerprint.clone(),
                    sequence: state.sequence,
                });
            }

            // Check sampling limit
            let max_samples = self.config.deep_inspect.max_sample_size;
            if max_samples > 0 && state.event_count >= max_samples as u64 {
                state.mark_stabilized();
                info!(
                    table = %table,
                    events = state.event_count,
                    "schema stabilized after sampling limit"
                );
                return Ok(ObserveResult::Stabilized {
                    fingerprint: state.fingerprint.clone(),
                    sequence: state.sequence,
                });
            }

            // Expand schema with new observation
            let old_fingerprint = state.fingerprint.clone();
            let old_sequence = state.sequence;

            // Use DeserializeSeed to expand the existing schema
            let mut de = serde_json::Deserializer::from_slice(json);
            if let Err(e) = state.inferred.deserialize(&mut de) {
                warn!(table = %table, error = %e, "failed to expand schema");
                // Still count as observation even if expansion fails
                state.record_observation();
                return Ok(ObserveResult::Unchanged {
                    fingerprint: state.fingerprint.clone(),
                    sequence: state.sequence,
                });
            }

            state.record_observation();
            state.update_fingerprint();

            if state.fingerprint != old_fingerprint {
                debug!(
                    table = %table,
                    old_fp = %old_fingerprint,
                    new_fp = %state.fingerprint,
                    sequence = state.sequence,
                    "schema evolved"
                );
                Ok(ObserveResult::Evolved {
                    old_fingerprint,
                    new_fingerprint: state.fingerprint.clone(),
                    old_sequence,
                    new_sequence: state.sequence,
                })
            } else {
                trace!(table = %table, events = state.event_count, "schema unchanged");
                Ok(ObserveResult::Unchanged {
                    fingerprint: state.fingerprint.clone(),
                    sequence: state.sequence,
                })
            }
        } else {
            // First event for this table - create initial schema
            let inferred: InferredSchema = serde_json::from_slice(json)?;
            let state = TableSchemaState::new(table.to_string(), inferred);

            let result = ObserveResult::NewSchema {
                fingerprint: state.fingerprint.clone(),
                sequence: state.sequence,
            };

            info!(
                table = %table,
                fingerprint = %state.fingerprint,
                "new schema discovered"
            );

            self.schemas.insert(table.to_string(), state);
            Ok(result)
        }
    }

    /// Observe a pre-parsed JSON value.
    pub fn observe_value(
        &mut self,
        table: &str,
        value: &serde_json::Value,
    ) -> SensorResult<ObserveResult> {
        // Re-serialize to bytes for schema_analysis
        let json = serde_json::to_vec(value)
            .map_err(|e| SensorError::Serialization(e.to_string()))?;
        self.observe(table, &json)
    }

    /// Get schema version info for a table.
    pub fn get_version(&self, table: &str) -> Option<SensedSchemaVersion> {
        self.schemas.get(table).map(|s| s.version())
    }

    /// Get full schema snapshot for a table.
    pub fn get_snapshot(&self, table: &str) -> Option<SchemaSnapshot> {
        self.schemas.get(table).map(SchemaSnapshot::from)
    }

    /// Get all table names being tracked.
    pub fn tables(&self) -> Vec<&str> {
        self.schemas.keys().map(|s| s.as_str()).collect()
    }

    /// Get snapshots for all tables.
    pub fn all_snapshots(&self) -> Vec<SchemaSnapshot> {
        self.schemas.values().map(SchemaSnapshot::from).collect()
    }

    /// Get the raw inferred schema for a table.
    pub fn get_schema(&self, table: &str) -> Option<&schema_analysis::Schema> {
        self.schemas.get(table).map(|s| s.schema())
    }

    /// Get event count for a table.
    pub fn event_count(&self, table: &str) -> u64 {
        self.schemas.get(table).map(|s| s.event_count).unwrap_or(0)
    }

    /// Check if a table's schema has stabilized.
    pub fn is_stabilized(&self, table: &str) -> bool {
        self.schemas
            .get(table)
            .map(|s| s.stabilized)
            .unwrap_or(false)
    }

    /// Reset tracking for a table.
    pub fn reset_table(&mut self, table: &str) {
        self.schemas.remove(table);
    }

    /// Reset all tracking.
    pub fn reset_all(&mut self) {
        self.schemas.clear();
    }

    /// Get configuration.
    pub fn config(&self) -> &SchemaSensingConfig {
        &self.config
    }

    /// Export a table's schema as JSON Schema.
    pub fn to_json_schema(&self, table: &str) -> Option<JsonSchema> {
        self.schemas
            .get(table)
            .map(|s| crate::json_schema::to_json_schema(s.schema()))
    }

    /// Export all schemas as JSON Schema.
    pub fn all_json_schemas(&self) -> HashMap<String, JsonSchema> {
        self.schemas
            .iter()
            .map(|(table, state)| {
                (
                    table.clone(),
                    crate::json_schema::to_json_schema(state.schema()),
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_disabled_sensor() {
        let mut sensor = SchemaSensor::new(SchemaSensingConfig::default());
        let result = sensor.observe("users", b"{}").unwrap();
        assert_eq!(result, ObserveResult::Disabled);
    }

    #[test]
    fn test_new_schema() {
        let mut sensor = SchemaSensor::enabled();
        let json = br#"{"id": 1, "name": "Alice"}"#;

        let result = sensor.observe("users", json).unwrap();
        assert!(matches!(result, ObserveResult::NewSchema { .. }));

        let version = sensor.get_version("users").unwrap();
        assert_eq!(version.sequence, 1);
    }

    #[test]
    fn test_schema_unchanged() {
        let mut sensor = SchemaSensor::enabled();
        let json1 = br#"{"id": 1, "name": "Alice"}"#;
        let json2 = br#"{"id": 2, "name": "Bob"}"#;

        sensor.observe("users", json1).unwrap();
        let result = sensor.observe("users", json2).unwrap();

        assert!(matches!(result, ObserveResult::Unchanged { .. }));
        assert_eq!(sensor.event_count("users"), 2);
    }

    #[test]
    fn test_schema_evolution() {
        let mut sensor = SchemaSensor::enabled();

        // Initial schema
        let json1 = br#"{"id": 1, "name": "Alice"}"#;
        sensor.observe("users", json1).unwrap();

        // Add new field - schema should evolve
        let json2 = br#"{"id": 2, "name": "Bob", "email": "bob@example.com"}"#;
        let result = sensor.observe("users", json2).unwrap();

        match result {
            ObserveResult::Evolved {
                old_sequence,
                new_sequence,
                ..
            } => {
                assert_eq!(old_sequence, 1);
                assert_eq!(new_sequence, 2);
            }
            _ => panic!("expected schema evolution, got {:?}", result),
        }
    }

    #[test]
    fn test_table_filter() {
        let config = SchemaSensingConfig {
            enabled: true,
            tables: deltaforge_config::TableFilter {
                include: vec!["users".into()],
                exclude: vec![],
            },
            ..Default::default()
        };

        let mut sensor = SchemaSensor::new(config);

        let result1 = sensor.observe("users", br#"{"id": 1}"#).unwrap();
        let result2 = sensor.observe("orders", br#"{"id": 1}"#).unwrap();

        assert!(matches!(result1, ObserveResult::NewSchema { .. }));
        assert_eq!(result2, ObserveResult::Disabled);
    }

    #[test]
    fn test_stabilization() {
        let config = SchemaSensingConfig {
            enabled: true,
            deep_inspect: deltaforge_config::DeepInspectConfig {
                max_sample_size: 3,
                ..Default::default()
            },
            ..Default::default()
        };

        let mut sensor = SchemaSensor::new(config);

        sensor.observe("users", br#"{"id": 1}"#).unwrap();
        sensor.observe("users", br#"{"id": 2}"#).unwrap();
        sensor.observe("users", br#"{"id": 3}"#).unwrap();

        // Should be stabilized now
        let result = sensor.observe("users", br#"{"id": 4}"#).unwrap();
        assert!(matches!(result, ObserveResult::Stabilized { .. }));
        assert!(sensor.is_stabilized("users"));
    }

    #[test]
    fn test_nested_json() {
        let mut sensor = SchemaSensor::enabled();

        let json = br#"{
            "id": 1,
            "metadata": {
                "created_by": "system",
                "tags": ["important", "urgent"],
                "settings": {
                    "notify": true,
                    "priority": 5
                }
            }
        }"#;

        let result = sensor.observe("tasks", json).unwrap();
        assert!(matches!(result, ObserveResult::NewSchema { .. }));

        // Verify we have a schema
        assert!(sensor.get_schema("tasks").is_some());
    }

    #[test]
    fn test_multiple_tables() {
        let mut sensor = SchemaSensor::enabled();

        sensor
            .observe("users", br#"{"id": 1, "name": "Alice"}"#)
            .unwrap();
        sensor
            .observe("orders", br#"{"order_id": 100, "total": 99.99}"#)
            .unwrap();

        assert_eq!(sensor.tables().len(), 2);
        assert!(sensor.get_version("users").is_some());
        assert!(sensor.get_version("orders").is_some());
    }

    #[test]
    fn test_observe_value() {
        let mut sensor = SchemaSensor::enabled();

        let value = serde_json::json!({
            "id": 1,
            "name": "Test"
        });

        let result = sensor.observe_value("items", &value).unwrap();
        assert!(matches!(result, ObserveResult::NewSchema { .. }));
    }

    #[test]
    fn test_reset_table() {
        let mut sensor = SchemaSensor::enabled();

        sensor.observe("users", br#"{"id": 1}"#).unwrap();
        sensor.observe("orders", br#"{"id": 1}"#).unwrap();

        assert!(sensor.get_version("users").is_some());
        sensor.reset_table("users");
        assert!(sensor.get_version("users").is_none());
        assert!(sensor.get_version("orders").is_some());
    }

    #[test]
    fn test_reset_all() {
        let mut sensor = SchemaSensor::enabled();

        sensor.observe("users", br#"{"id": 1}"#).unwrap();
        sensor.observe("orders", br#"{"id": 1}"#).unwrap();

        assert_eq!(sensor.tables().len(), 2);
        sensor.reset_all();
        assert_eq!(sensor.tables().len(), 0);
    }

    #[test]
    fn test_to_json_schema() {
        let mut sensor = SchemaSensor::enabled();

        sensor
            .observe("users", br#"{"id": 1, "name": "Test"}"#)
            .unwrap();

        let js = sensor.to_json_schema("users");
        assert!(js.is_some());

        let js = js.unwrap();
        assert!(js.properties.is_some());
    }

    #[test]
    fn test_all_snapshots() {
        let mut sensor = SchemaSensor::enabled();

        sensor.observe("users", br#"{"id": 1}"#).unwrap();
        sensor.observe("orders", br#"{"id": 1}"#).unwrap();

        let snapshots = sensor.all_snapshots();
        assert_eq!(snapshots.len(), 2);
    }
}
