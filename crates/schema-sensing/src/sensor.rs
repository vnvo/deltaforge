//! Schema sensor with high-cardinality key handling.
//!
//! This is the modified SchemaSensor that integrates field classification
//! to handle dynamic map keys properly.

use std::collections::{HashMap, HashSet};

use schema_analysis::InferredSchema;
use tracing::debug;

use deltaforge_config::SchemaSensingConfig;

use crate::adaptive_hash::{compute_adaptive_hash, compute_structure_hash};
use crate::errors::{SensorError, SensorResult};
use crate::field_classifier::FieldClassifier;
use crate::high_cardinality::{HighCardinalityConfig, PathClassification};
use crate::json_schema::JsonSchema;
use crate::schema_state::{
    SchemaSnapshot, SensedSchemaVersion, TableSchemaState,
};

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

    /// Skipped due to structure cache hit (fast path)
    CacheHit { fingerprint: String, sequence: u64 },

    /// Skipped due to sampling (not selected for this event)
    Sampled { fingerprint: String, sequence: u64 },
}

/// Cache statistics for a single table.
#[derive(Debug, Clone)]
pub struct CacheStatsEntry {
    pub table: String,
    pub cached_structures: usize,
    pub max_cache_size: usize,
    pub cache_hits: u64,
    pub cache_misses: u64,
}

/// Per-table structure cache for fast-path skipping.
struct StructureCache {
    seen: HashSet<u64>,
    max_size: usize,
    hits: u64,
    misses: u64,
}

impl StructureCache {
    fn new(max_size: usize) -> Self {
        Self {
            seen: HashSet::new(),
            max_size,
            hits: 0,
            misses: 0,
        }
    }

    fn check_and_insert(&mut self, hash: u64) -> bool {
        if self.seen.contains(&hash) {
            self.hits += 1;
            return true;
        }
        self.misses += 1;
        if self.seen.len() < self.max_size {
            self.seen.insert(hash);
        }
        false
    }

    fn clear(&mut self) {
        self.seen.clear();
        self.hits = 0;
        self.misses = 0;
    }

    fn stats(&self) -> (usize, usize, u64, u64) {
        (self.seen.len(), self.max_size, self.hits, self.misses)
    }
}

/// Universal schema sensor with high-cardinality key handling.
///
/// Infers and tracks schema from JSON payloads across all tables.
/// Uses `schema_analysis` for robust type inference and `high_cardinality`
/// for distinguishing stable fields from dynamic map keys.
pub struct SchemaSensor {
    /// Configuration
    config: SchemaSensingConfig,

    /// High-cardinality configuration
    hc_config: HighCardinalityConfig,

    /// Per-table schema state
    schemas: HashMap<String, TableSchemaState>,

    /// Per-table structure cache for fast-path skipping
    structure_caches: HashMap<String, StructureCache>,

    /// Per-table field classifier for high-cardinality handling
    field_classifiers: HashMap<String, FieldClassifier>,
}

impl SchemaSensor {
    pub fn new(config: SchemaSensingConfig) -> Self {
        Self::with_hc_config(config, HighCardinalityConfig::default())
    }

    pub fn with_hc_config(
        config: SchemaSensingConfig,
        hc_config: HighCardinalityConfig,
    ) -> Self {
        Self {
            config,
            hc_config,
            schemas: HashMap::new(),
            structure_caches: HashMap::new(),
            field_classifiers: HashMap::new(),
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

    /// Observe a pre-parsed JSON value (optimized path).
    ///
    /// This is the preferred method when you already have a parsed Value,
    /// as it can use structure caching without re-parsing.
    pub fn observe_value(
        &mut self,
        table: &str,
        value: &serde_json::Value,
    ) -> SensorResult<ObserveResult> {
        // Check if sensing is enabled for this table
        if !self.config.should_sense_table(table) {
            return Ok(ObserveResult::Disabled);
        }

        // Get current event count for sampling decision
        let event_count =
            self.schemas.get(table).map(|s| s.event_count).unwrap_or(0);

        // Check if we've hit the stabilization limit
        if let Some(state) = self.schemas.get(table) {
            if state.stabilized {
                return Ok(ObserveResult::Stabilized {
                    fingerprint: state.fingerprint.clone(),
                    sequence: state.sequence,
                });
            }
        }

        // Update field classifier (always, even before cache check)
        let classifications = self.update_field_classifier(table, value);

        // Structure cache check using ADAPTIVE hash
        if self.config.sampling.structure_cache {
            let structure_hash =
                if self.hc_config.enabled && !classifications.is_empty() {
                    compute_adaptive_hash(value, &classifications)
                } else {
                    compute_structure_hash(value)
                };

            let cache = self
                .structure_caches
                .entry(table.to_string())
                .or_insert_with(|| {
                    StructureCache::new(
                        self.config.sampling.structure_cache_size,
                    )
                });

            if cache.check_and_insert(structure_hash) {
                // Cache hit - skip full sensing
                if let Some(state) = self.schemas.get_mut(table) {
                    state.record_observation();

                    // Check stabilization even on cache hit
                    let max_samples = self.config.deep_inspect.max_sample_size;
                    if max_samples > 0
                        && state.event_count >= max_samples as u64
                        && !state.stabilized
                    {
                        state.mark_stabilized();
                        return Ok(ObserveResult::Stabilized {
                            fingerprint: state.fingerprint.clone(),
                            sequence: state.sequence,
                        });
                    }

                    return Ok(ObserveResult::CacheHit {
                        fingerprint: state.fingerprint.clone(),
                        sequence: state.sequence,
                    });
                }
                // No schema yet - fall through to full sensing
            }
        }

        // Sampling check (after warmup)
        if !self.config.should_sample(event_count) {
            if let Some(state) = self.schemas.get_mut(table) {
                state.record_observation();
                return Ok(ObserveResult::Sampled {
                    fingerprint: state.fingerprint.clone(),
                    sequence: state.sequence,
                });
            }
        }

        // Full sensing path - serialize and process
        let json = serde_json::to_vec(value)
            .map_err(|e| SensorError::Serialization(e.to_string()))?;
        self.observe_bytes(table, &json)
    }

    /// Observe raw JSON bytes.
    pub fn observe(
        &mut self,
        table: &str,
        json: &[u8],
    ) -> SensorResult<ObserveResult> {
        // Check if sensing is enabled for this table
        if !self.config.should_sense_table(table) {
            return Ok(ObserveResult::Disabled);
        }

        let event_count =
            self.schemas.get(table).map(|s| s.event_count).unwrap_or(0);

        // Check stabilization
        if let Some(state) = self.schemas.get(table) {
            if state.stabilized {
                return Ok(ObserveResult::Stabilized {
                    fingerprint: state.fingerprint.clone(),
                    sequence: state.sequence,
                });
            }
        }

        // Parse for field classification and cache check
        if let Ok(value) = serde_json::from_slice::<serde_json::Value>(json) {
            let classifications = self.update_field_classifier(table, &value);

            if self.config.sampling.structure_cache {
                let structure_hash =
                    if self.hc_config.enabled && !classifications.is_empty() {
                        compute_adaptive_hash(&value, &classifications)
                    } else {
                        compute_structure_hash(&value)
                    };

                let cache = self
                    .structure_caches
                    .entry(table.to_string())
                    .or_insert_with(|| {
                        StructureCache::new(
                            self.config.sampling.structure_cache_size,
                        )
                    });

                if cache.check_and_insert(structure_hash) {
                    // Cache hit
                    if let Some(state) = self.schemas.get_mut(table) {
                        state.record_observation();

                        // Check stabilization even on cache hit
                        let max_samples =
                            self.config.deep_inspect.max_sample_size;
                        if max_samples > 0
                            && state.event_count >= max_samples as u64
                            && !state.stabilized
                        {
                            state.mark_stabilized();
                            return Ok(ObserveResult::Stabilized {
                                fingerprint: state.fingerprint.clone(),
                                sequence: state.sequence,
                            });
                        }

                        return Ok(ObserveResult::CacheHit {
                            fingerprint: state.fingerprint.clone(),
                            sequence: state.sequence,
                        });
                    }
                }
            }
        }

        // Sampling check (after warmup)
        if !self.config.should_sample(event_count) {
            if let Some(state) = self.schemas.get_mut(table) {
                state.record_observation();
                return Ok(ObserveResult::Sampled {
                    fingerprint: state.fingerprint.clone(),
                    sequence: state.sequence,
                });
            }
        }

        self.observe_bytes(table, json)
    }

    /// Internal: process JSON bytes for schema inference.
    fn observe_bytes(
        &mut self,
        table: &str,
        json: &[u8],
    ) -> SensorResult<ObserveResult> {
        // Use schema_analysis to infer types (via serde deserialization)
        let inferred: InferredSchema = serde_json::from_slice(json)?;
        let fingerprint =
            crate::fingerprint::compute_fingerprint(&inferred.schema);

        // Check if table exists and if schema evolved
        let evolution_info = if let Some(state) = self.schemas.get(table) {
            if state.fingerprint != fingerprint {
                Some((state.fingerprint.clone(), state.sequence))
            } else {
                None
            }
        } else {
            None
        };

        // Handle schema evolution or creation
        if let Some((old_fingerprint, old_sequence)) = evolution_info {
            // Schema evolved - update state
            let state = self.schemas.get_mut(table).unwrap();
            state.inferred = inferred;
            state.fingerprint = fingerprint.clone();
            state.sequence += 1;
            state.record_observation();

            let new_sequence = state.sequence;

            debug!(
                table = table,
                old_fingerprint = old_fingerprint,
                new_fingerprint = fingerprint,
                "Schema evolved"
            );

            // Clear structure cache on evolution
            if let Some(cache) = self.structure_caches.get_mut(table) {
                cache.clear();
            }

            return Ok(ObserveResult::Evolved {
                old_fingerprint,
                new_fingerprint: fingerprint,
                old_sequence,
                new_sequence,
            });
        }

        // Get or create table state
        let is_new = !self.schemas.contains_key(table);
        let state =
            self.schemas.entry(table.to_string()).or_insert_with(|| {
                TableSchemaState::new(table.to_string(), inferred)
            });

        state.record_observation();

        // Check stabilization
        let max_samples = self.config.deep_inspect.max_sample_size;
        if max_samples > 0
            && state.event_count >= max_samples as u64
            && !state.stabilized
        {
            state.mark_stabilized();
            return Ok(ObserveResult::Stabilized {
                fingerprint: state.fingerprint.clone(),
                sequence: state.sequence,
            });
        }

        if is_new {
            Ok(ObserveResult::NewSchema {
                fingerprint: state.fingerprint.clone(),
                sequence: state.sequence,
            })
        } else {
            Ok(ObserveResult::Unchanged {
                fingerprint: state.fingerprint.clone(),
                sequence: state.sequence,
            })
        }
    }

    /// Update field classifier and return current classifications.
    fn update_field_classifier(
        &mut self,
        table: &str,
        value: &serde_json::Value,
    ) -> HashMap<String, PathClassification> {
        if !self.hc_config.enabled {
            return HashMap::new();
        }

        // Clone config before borrowing field_classifiers to avoid borrow conflict
        let hc_config = self.hc_config.clone();
        let classifier = self
            .field_classifiers
            .entry(table.to_string())
            .or_insert_with(|| FieldClassifier::new(hc_config));

        classifier.observe(value).clone()
    }

    // ========================================================================
    // Query methods
    // ========================================================================

    /// Get the current schema version for a table.
    pub fn get_version(&self, table: &str) -> Option<SensedSchemaVersion> {
        self.schemas.get(table).map(|s| s.version())
    }

    /// Get a snapshot of a table's schema.
    pub fn get_snapshot(&self, table: &str) -> Option<SchemaSnapshot> {
        self.schemas.get(table).map(SchemaSnapshot::from)
    }

    /// List all tracked tables.
    pub fn tables(&self) -> Vec<&str> {
        self.schemas.keys().map(|s| s.as_str()).collect()
    }

    /// Get all snapshots for all tables.
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

    /// Get cache statistics for a table.
    pub fn cache_stats(&self, table: &str) -> Option<CacheStatsEntry> {
        self.structure_caches.get(table).map(|c| {
            let (cached, max_size, hits, misses) = c.stats();
            CacheStatsEntry {
                table: table.to_string(),
                cached_structures: cached,
                max_cache_size: max_size,
                cache_hits: hits,
                cache_misses: misses,
            }
        })
    }

    /// Get cache statistics for all tables.
    pub fn all_cache_stats(&self) -> Vec<CacheStatsEntry> {
        self.structure_caches
            .iter()
            .map(|(table, cache)| {
                let (cached, max_size, hits, misses) = cache.stats();
                CacheStatsEntry {
                    table: table.clone(),
                    cached_structures: cached,
                    max_cache_size: max_size,
                    cache_hits: hits,
                    cache_misses: misses,
                }
            })
            .collect()
    }

    // ========================================================================
    // High-cardinality query methods
    // ========================================================================

    /// Get field classification for a path within a table.
    pub fn get_classification(
        &self,
        table: &str,
        path: &str,
    ) -> Option<&PathClassification> {
        self.field_classifiers
            .get(table)
            .and_then(|c| c.get_classification(path))
    }

    /// Get all classifications for a table.
    pub fn get_all_classifications(
        &self,
        table: &str,
    ) -> Option<&HashMap<String, PathClassification>> {
        self.field_classifiers
            .get(table)
            .map(|c| c.classifications())
    }

    /// Get paths detected as maps (having dynamic keys) for a table.
    pub fn detected_maps(&self, table: &str) -> Vec<&str> {
        self.field_classifiers
            .get(table)
            .map(|c| c.map_paths())
            .unwrap_or_default()
    }

    // ========================================================================
    // Mutation methods
    // ========================================================================

    /// Reset tracking for a table.
    pub fn reset_table(&mut self, table: &str) {
        self.schemas.remove(table);
        self.structure_caches.remove(table);
        self.field_classifiers.remove(table);
    }

    /// Reset all tracking.
    pub fn reset_all(&mut self) {
        self.schemas.clear();
        self.structure_caches.clear();
        self.field_classifiers.clear();
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
    use deltaforge_config::SamplingConfig;
    use serde_json::json;

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
    fn test_cache_hit_with_dynamic_keys() {
        let config = SchemaSensingConfig {
            enabled: true,
            sampling: SamplingConfig {
                structure_cache: true,
                structure_cache_size: 100,
                warmup_events: 1000,
                sample_rate: 1,
            },
            ..Default::default()
        };
        let hc_config = HighCardinalityConfig {
            enabled: true,
            min_events: 5,
            confidence_threshold: 0.3,
            min_dynamic_fields: 2,
            ..Default::default()
        };
        let mut sensor = SchemaSensor::with_hc_config(config, hc_config);

        // Warmup phase 1: Build classifications with varied keys
        for i in 0..10 {
            let val = json!({
                "id": i,
                "sessions": {
                    format!("sess_{}", i): {"user": "test"}
                }
            });
            sensor.observe_value("events", &val).unwrap();
        }

        // Warmup phase 2: Use consistent structure to stabilize schema
        // This lets the cache fill with the adaptive hash
        for i in 10..20 {
            let val = json!({
                "id": i,
                "sessions": {
                    "sess_stable": {"user": "test"}
                }
            });
            sensor.observe_value("events", &val).unwrap();
        }

        // Verify classifications detected the map
        let maps = sensor.detected_maps("events");
        assert!(
            maps.contains(&"sessions"),
            "sessions should be detected as map"
        );

        // Now test: different session keys should have same adaptive hash
        let val1 = json!({
            "id": 100,
            "sessions": {
                "sess_NEW_ABC": {"user": "alice"}
            }
        });
        let result1 = sensor.observe_value("events", &val1).unwrap();

        let val2 = json!({
            "id": 101,
            "sessions": {
                "sess_DIFFERENT_XYZ": {"user": "bob"}
            }
        });
        let result2 = sensor.observe_value("events", &val2).unwrap();

        // Both should cache hit because:
        // 1. Classifications exist (sessions is dynamic)
        // 2. Adaptive hash ignores dynamic key names
        // 3. Structure matches warmup phase 2
        assert!(
            matches!(
                result1,
                ObserveResult::CacheHit { .. }
                    | ObserveResult::Unchanged { .. }
            ),
            "Expected cache hit or unchanged, got {:?}",
            result1
        );
        assert!(
            matches!(result2, ObserveResult::CacheHit { .. }),
            "Expected cache hit, got {:?}",
            result2
        );
    }

    #[test]
    fn test_detected_maps() {
        let config = SchemaSensingConfig {
            enabled: true,
            ..Default::default()
        };
        let hc_config = HighCardinalityConfig {
            enabled: true,
            min_events: 5,
            confidence_threshold: 0.3,
            min_dynamic_fields: 2,
            ..Default::default()
        };
        let mut sensor = SchemaSensor::with_hc_config(config, hc_config);

        for i in 0..50 {
            let val = json!({
                "data": {
                    format!("key_{}", i): i
                }
            });
            sensor.observe_value("test", &val).unwrap();
        }

        let maps = sensor.detected_maps("test");
        assert!(maps.contains(&"data"));
    }

    #[test]
    fn test_get_classification() {
        let config = SchemaSensingConfig {
            enabled: true,
            ..Default::default()
        };
        let hc_config = HighCardinalityConfig {
            enabled: true,
            min_events: 5,
            confidence_threshold: 0.3,
            ..Default::default()
        };
        let mut sensor = SchemaSensor::with_hc_config(config, hc_config);

        for _ in 0..50 {
            let val = json!({"id": 1, "name": "test", "email": "x@y.com"});
            sensor.observe_value("users", &val).unwrap();
        }

        let class = sensor.get_classification("users", "").unwrap();
        assert_eq!(class.stable_fields.len(), 3);
        assert!(!class.has_dynamic_fields);
    }
}
