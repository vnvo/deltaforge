use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use schema_analysis::InferredSchema;
use serde::de::DeserializeSeed;
use tracing::{debug, info, trace, warn};

use deltaforge_config::SchemaSensingConfig;

use crate::errors::{SensorError, SensorResult};
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

/// Compute a lightweight structure fingerprint from top-level JSON keys only.
///
/// This is MUCH faster than traversing nested structures. We only hash:
/// - The set of top-level key names (for objects)
/// - The type tag (for primitives/arrays)
///
/// This is sufficient to detect most schema changes (new/removed columns)
/// while being O(keys) instead of O(total_nodes).
fn compute_structure_hash(value: &serde_json::Value) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    let mut hasher = DefaultHasher::new();
    hash_top_level_structure(value, &mut hasher);
    hasher.finish()
}

/// Hash only top-level structure - no recursion into nested objects.
fn hash_top_level_structure<H: Hasher>(
    value: &serde_json::Value,
    hasher: &mut H,
) {
    match value {
        serde_json::Value::Null => 0u8.hash(hasher),
        serde_json::Value::Bool(_) => 1u8.hash(hasher),
        serde_json::Value::Number(_) => 2u8.hash(hasher),
        serde_json::Value::String(_) => 3u8.hash(hasher),
        serde_json::Value::Array(_) => {
            // Just mark as array - don't inspect elements
            4u8.hash(hasher);
        }
        serde_json::Value::Object(obj) => {
            5u8.hash(hasher);
            // Hash number of keys (fast structural check)
            obj.len().hash(hasher);
            // Hash sorted key names only - O(k log k) where k = number of keys
            let mut keys: Vec<_> = obj.keys().collect();
            keys.sort_unstable();
            for key in keys {
                key.hash(hasher);
            }
        }
    }
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
    }

    fn stats(&self) -> (usize, usize, u64, u64) {
        (self.seen.len(), self.max_size, self.hits, self.misses)
    }
}

/// Universal schema sensor with performance optimizations.
///
/// Infers and tracks schema from JSON payloads across all tables.
/// Uses `schema_analysis` for robust type inference.
///
/// Performance features:
/// - Structure caching: Skip payloads with identical key structure
/// - Sampling: After warmup, only analyze a fraction of events
/// - Stabilization: Stop after max_sample_size events per table
pub struct SchemaSensor {
    /// Configuration
    config: SchemaSensingConfig,

    /// Per-table schema state
    schemas: HashMap<String, TableSchemaState>,

    /// Per-table structure cache for fast-path skipping
    structure_caches: HashMap<String, StructureCache>,
}

impl SchemaSensor {
    pub fn new(config: SchemaSensingConfig) -> Self {
        Self {
            config,
            schemas: HashMap::new(),
            structure_caches: HashMap::new(),
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

        // Structure cache check (fast path) - O(keys) for top-level only
        if self.config.sampling.structure_cache {
            let structure_hash = compute_structure_hash(value);
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
        self.observe_bytes(table, &json, event_count)
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

        // For bytes path with structure caching, we need to parse once
        // and reuse for both cache check and sensing
        if self.config.sampling.structure_cache {
            if let Ok(value) = serde_json::from_slice::<serde_json::Value>(json)
            {
                let structure_hash = compute_structure_hash(&value);
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

                // Cache miss - check sampling
                if !self.config.should_sample(event_count) {
                    if let Some(state) = self.schemas.get_mut(table) {
                        state.record_observation();
                        return Ok(ObserveResult::Sampled {
                            fingerprint: state.fingerprint.clone(),
                            sequence: state.sequence,
                        });
                    }
                }

                // Need full sensing - use already-parsed JSON bytes
                return self.observe_bytes(table, json, event_count);
            }
        }

        // No cache path - just check sampling
        if !self.config.should_sample(event_count) {
            if let Some(state) = self.schemas.get_mut(table) {
                state.record_observation();
                return Ok(ObserveResult::Sampled {
                    fingerprint: state.fingerprint.clone(),
                    sequence: state.sequence,
                });
            }
        }

        self.observe_bytes(table, json, event_count)
    }

    /// Internal: perform full schema sensing on bytes.
    fn observe_bytes(
        &mut self,
        table: &str,
        json: &[u8],
        event_count: u64,
    ) -> SensorResult<ObserveResult> {
        // Check if we already have state for this table
        if let Some(state) = self.schemas.get_mut(table) {
            // Check sampling limit for stabilization
            let max_samples = self.config.deep_inspect.max_sample_size;
            if max_samples > 0 && event_count >= max_samples as u64 {
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

                // Clear structure cache on evolution
                if let Some(cache) = self.structure_caches.get_mut(table) {
                    cache.clear();
                }

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

    /// Reset tracking for a table.
    pub fn reset_table(&mut self, table: &str) {
        self.schemas.remove(table);
        self.structure_caches.remove(table);
    }

    /// Reset all tracking.
    pub fn reset_all(&mut self) {
        self.schemas.clear();
        self.structure_caches.clear();
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
    fn test_structure_cache_hit() {
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
        let mut sensor = SchemaSensor::new(config);

        // First event - new schema
        let json1 = serde_json::json!({"id": 1, "name": "Alice"});
        let result1 = sensor.observe_value("users", &json1).unwrap();
        assert!(matches!(result1, ObserveResult::NewSchema { .. }));

        // Second event with same structure - cache hit
        let json2 = serde_json::json!({"id": 2, "name": "Bob"});
        let result2 = sensor.observe_value("users", &json2).unwrap();
        assert!(matches!(result2, ObserveResult::CacheHit { .. }));

        // Event count should still increase
        assert_eq!(sensor.event_count("users"), 2);
    }

    #[test]
    fn test_structure_cache_miss_different_keys() {
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
        let mut sensor = SchemaSensor::new(config);

        let json1 = serde_json::json!({"id": 1, "name": "Alice"});
        sensor.observe_value("users", &json1).unwrap();

        // Different top-level keys - cache miss, triggers evolution
        let json2 = serde_json::json!({"id": 2, "name": "Bob", "email": "bob@example.com"});
        let result2 = sensor.observe_value("users", &json2).unwrap();
        assert!(matches!(result2, ObserveResult::Evolved { .. }));
    }

    #[test]
    fn test_structure_cache_hit_nested_changes() {
        // With top-level-only hashing, nested structure changes don't cause cache miss
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
        let mut sensor = SchemaSensor::new(config);

        let json1 = serde_json::json!({"id": 1, "data": {"a": 1}});
        sensor.observe_value("users", &json1).unwrap();

        // Same top-level keys, different nested structure - still cache HIT
        // (This is intentional for performance - we trade some accuracy for speed)
        let json2 = serde_json::json!({"id": 2, "data": {"a": 1, "b": 2}});
        let result2 = sensor.observe_value("users", &json2).unwrap();
        assert!(matches!(result2, ObserveResult::CacheHit { .. }));
    }

    #[test]
    fn test_sampling_after_warmup() {
        let config = SchemaSensingConfig {
            enabled: true,
            sampling: SamplingConfig {
                warmup_events: 5,
                sample_rate: 2,         // 50% sampling
                structure_cache: false, // Disable for this test
                ..Default::default()
            },
            ..Default::default()
        };
        let mut sensor = SchemaSensor::new(config);

        // Warmup phase - all events processed
        for i in 0..5 {
            let json = serde_json::json!({"id": i});
            let result = sensor.observe_value("users", &json).unwrap();
            assert!(!matches!(result, ObserveResult::Sampled { .. }));
        }

        // After warmup - sampling kicks in
        let mut sampled_count = 0;
        let mut processed_count = 0;
        for i in 5..15 {
            let json = serde_json::json!({"id": i});
            let result = sensor.observe_value("users", &json).unwrap();
            match result {
                ObserveResult::Sampled { .. } => sampled_count += 1,
                ObserveResult::Unchanged { .. }
                | ObserveResult::CacheHit { .. } => processed_count += 1,
                _ => {}
            }
        }

        // With 50% sampling, roughly half should be sampled
        assert!(sampled_count > 0, "some events should be sampled");
        assert!(processed_count > 0, "some events should be processed");
    }

    #[test]
    fn test_cache_cleared_on_evolution() {
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
        let mut sensor = SchemaSensor::new(config);

        // Build up cache
        let json1 = serde_json::json!({"id": 1});
        sensor.observe_value("users", &json1).unwrap();
        let json2 = serde_json::json!({"id": 2});
        sensor.observe_value("users", &json2).unwrap();

        let stats = sensor.cache_stats("users").unwrap();
        assert_eq!(stats.cached_structures, 1); // Only one unique structure
        assert_eq!(stats.cache_hits, 1); // Second json2 was a cache hit
        assert_eq!(stats.cache_misses, 1); // First json1 was a miss        

        // Evolution should clear cache
        let json3 = serde_json::json!({"id": 3, "extra": "field"});
        let result = sensor.observe_value("users", &json3).unwrap();
        assert!(matches!(result, ObserveResult::Evolved { .. }));

        let stats = sensor.cache_stats("users").unwrap();
        assert_eq!(stats.cached_structures, 0); // Cache cleared
    }

    #[test]
    fn test_schema_evolution() {
        let mut sensor = SchemaSensor::enabled();

        let json1 = br#"{"id": 1, "name": "Alice"}"#;
        sensor.observe("users", json1).unwrap();

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

        let result = sensor.observe("users", br#"{"id": 4}"#).unwrap();
        assert!(matches!(result, ObserveResult::Stabilized { .. }));
        assert!(sensor.is_stabilized("users"));
    }
}
