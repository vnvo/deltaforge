//! Schema sensor with high-cardinality key handling.
//!
//! It tries to be fast:
//! 1. Fast path for pure structs (no dynamic fields) - uses original hash
//! 2. Lazy classifier updates - only update when needed, not every event
//! 3. Reference-based classifications - no HashMap clone
//! 4. Skip normalization when not needed

use std::collections::{HashMap, HashSet};

use metrics::{counter, gauge, histogram};
use schema_analysis::InferredSchema;
use tracing::debug;

use deltaforge_config::SchemaSensingConfig;

use crate::adaptive_hash::{compute_adaptive_hash, compute_structure_hash};
use crate::errors::SensorResult;
use crate::field_classifier::FieldClassifier;
use crate::high_cardinality::{HighCardinalityConfig, PathClassification};
use crate::json_schema::JsonSchema;
use crate::schema_state::{
    SchemaSnapshot, SensedSchemaVersion, TableSchemaState,
};

/// Result of observing an event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObserveResult {
    Disabled,
    NewSchema {
        fingerprint: String,
        sequence: u64,
    },
    Evolved {
        old_fingerprint: String,
        new_fingerprint: String,
        old_sequence: u64,
        new_sequence: u64,
    },
    Unchanged {
        fingerprint: String,
        sequence: u64,
    },
    Stabilized {
        fingerprint: String,
        sequence: u64,
    },
    CacheHit {
        fingerprint: String,
        sequence: u64,
    },
    Sampled {
        fingerprint: String,
        sequence: u64,
    },
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
            seen: HashSet::with_capacity(max_size),
            max_size,
            hits: 0,
            misses: 0,
        }
    }

    fn check_and_insert(&mut self, hash: u64) -> bool {
        if self.seen.len() >= self.max_size {
            // At capacity: only check, don't insert (1 hash op)
            if self.seen.contains(&hash) {
                self.hits += 1;
                true
            } else {
                self.misses += 1;
                false
            }
        } else {
            // Under capacity: insert returns false if was present (1 hash op)
            if self.seen.insert(hash) {
                // Was not present (now inserted)
                self.misses += 1;
                false
            } else {
                // Was present
                self.hits += 1;
                true
            }
        }
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

/// Per-table HC state tracking
struct TableHcState {
    classifier: FieldClassifier,
    /// Cached: does any path have dynamic fields?
    has_any_dynamic: bool,
    /// Event count when has_any_dynamic was last computed
    dynamic_check_at: u64,
    /// Classification is considered stable (high confidence, enough events)
    classification_stable: bool,
    /// Fast path: confirmed pure struct (no dynamic fields after sufficient events)
    confirmed_pure_struct: bool,
}

impl TableHcState {
    fn new(config: HighCardinalityConfig) -> Self {
        Self {
            classifier: FieldClassifier::new(config),
            has_any_dynamic: false,
            dynamic_check_at: 0,
            classification_stable: false,
            confirmed_pure_struct: false,
        }
    }
}

pub struct SchemaSensor {
    config: SchemaSensingConfig,
    hc_config: HighCardinalityConfig,
    schemas: HashMap<String, TableSchemaState>,
    structure_caches: HashMap<String, StructureCache>,
    hc_states: HashMap<String, TableHcState>,
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
            hc_states: HashMap::new(),
        }
    }

    pub fn enabled() -> Self {
        Self::new(SchemaSensingConfig {
            enabled: true,
            ..Default::default()
        })
    }

    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Observe a pre-parsed JSON value (optimized path).
    pub fn observe_value(
        &mut self,
        table: &str,
        value: &serde_json::Value,
    ) -> SensorResult<ObserveResult> {
        let start = std::time::Instant::now();
        let result = self.observe_value_inner(table, value);

        if let Ok(ref res) = result {
            self.record_metrics(table, res, start.elapsed());
        }

        result
    }

    /// Record metrics for an observation result
    fn record_metrics(
        &self,
        table: &str,
        result: &ObserveResult,
        elapsed: std::time::Duration,
    ) {
        counter!("deltaforge_schema_events_total", "table" => table.to_string()).increment(1);
        histogram!("deltaforge_schema_sensing_seconds", "table" => table.to_string())
            .record(elapsed.as_secs_f64());

        match result {
            ObserveResult::CacheHit { .. } => {
                counter!("deltaforge_schema_cache_hits_total", "table" => table.to_string()).increment(1);
            }
            ObserveResult::Evolved { .. } => {
                counter!("deltaforge_schema_cache_misses_total", "table" => table.to_string()).increment(1);
                counter!("deltaforge_schema_evolutions_total", "table" => table.to_string()).increment(1);
            }
            ObserveResult::NewSchema { .. } => {
                counter!("deltaforge_schema_cache_misses_total", "table" => table.to_string()).increment(1);
            }
            ObserveResult::Unchanged { .. } | ObserveResult::Sampled { .. } => {
                // Not a cache hit but also not a miss - full sensing path
            }
            _ => {}
        }

        // Update gauges periodically (every 1000 events to avoid overhead)
        if let Some(state) = self.schemas.get(table) {
            if state.event_count % 1000 == 0 {
                gauge!("deltaforge_schema_tables_total")
                    .set(self.schemas.len() as f64);
                let dynamic_maps: usize = self
                    .hc_states
                    .values()
                    .map(|s| s.classifier.map_paths().len())
                    .sum();
                gauge!("deltaforge_schema_dynamic_maps_total")
                    .set(dynamic_maps as f64);
            }
        }
    }

    /// Inner observe logic (no metrics)
    fn observe_value_inner(
        &mut self,
        table: &str,
        value: &serde_json::Value,
    ) -> SensorResult<ObserveResult> {
        if !self.config.should_sense_table(table) {
            return Ok(ObserveResult::Disabled);
        }

        let event_count =
            self.schemas.get(table).map(|s| s.event_count).unwrap_or(0);

        // Early exit: schema stabilized
        if let Some(state) = self.schemas.get(table) {
            if state.stabilized {
                return Ok(ObserveResult::Stabilized {
                    fingerprint: state.fingerprint.clone(),
                    sequence: state.sequence,
                });
            }
        }

        // HC processing - only when enabled and beneficial
        let (use_adaptive_hash, has_dynamic) = if self.hc_config.enabled {
            self.update_hc_state(table, value, event_count)
        } else {
            (false, false)
        };

        // Structure cache check
        if self.config.sampling.structure_cache {
            // OPTIMIZATION: Use simple hash when no dynamic fields
            let structure_hash = if use_adaptive_hash && has_dynamic {
                let hc_state = self.hc_states.get(table).unwrap();
                compute_adaptive_hash(
                    value,
                    hc_state.classifier.classifications(),
                )
            } else {
                compute_structure_hash(value)
            };

            // OPTIMIZATION: Use get() first to avoid String allocation on hot path
            let cache_hit =
                if let Some(cache) = self.structure_caches.get_mut(table) {
                    cache.check_and_insert(structure_hash)
                } else {
                    // First time for this table - need to create cache
                    let cache = self
                        .structure_caches
                        .entry(table.to_string())
                        .or_insert_with(|| {
                            StructureCache::new(
                                self.config.sampling.structure_cache_size,
                            )
                        });
                    cache.check_and_insert(structure_hash)
                };

            if cache_hit {
                if let Some(state) = self.schemas.get_mut(table) {
                    state.record_observation();

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
            }
        }

        // Sampling check
        if !self.config.should_sample(event_count) {
            if let Some(state) = self.schemas.get_mut(table) {
                state.record_observation();
                return Ok(ObserveResult::Sampled {
                    fingerprint: state.fingerprint.clone(),
                    sequence: state.sequence,
                });
            }
        }

        // Full sensing - normalize only if needed
        self.observe_value_full(table, value, has_dynamic)
    }

    /// Update HC state and return (should_use_adaptive_hash, has_any_dynamic)
    fn update_hc_state(
        &mut self,
        table: &str,
        value: &serde_json::Value,
        event_count: u64,
    ) -> (bool, bool) {
        // FAST PATH: Check if we already have state and it's a confirmed pure struct
        // Use .get() first to avoid String allocation for table key
        if let Some(hc_state) = self.hc_states.get(table) {
            if hc_state.confirmed_pure_struct {
                return (false, false);
            }
            if hc_state.classification_stable && !hc_state.has_any_dynamic {
                return (false, false);
            }
        }

        // Slower path - need to potentially create/update state
        let min_events = self.hc_config.min_events;
        let confidence_threshold = self.hc_config.confidence_threshold;
        let reevaluate_interval = self.hc_config.reevaluate_interval;

        // Now we need mutable access - only allocate table string if needed
        let hc_state = self
            .hc_states
            .entry(table.to_string())
            .or_insert_with(|| TableHcState::new(self.hc_config.clone()));

        // Double-check after getting mutable reference
        if hc_state.confirmed_pure_struct {
            return (false, false);
        }

        // Check if classification is stable with no dynamic fields
        if hc_state.classification_stable && !hc_state.has_any_dynamic {
            // After 2x min_events with no dynamic fields, confirm pure struct
            if event_count >= min_events * 2 {
                hc_state.confirmed_pure_struct = true;
            }
            return (false, false);
        }

        // Determine if we need to update classifier
        let should_update = if hc_state.classification_stable {
            // Only periodic re-evaluation after stable
            reevaluate_interval > 0
                && event_count.saturating_sub(hc_state.dynamic_check_at)
                    >= reevaluate_interval
        } else {
            // During warmup: update less frequently as we get more events
            if event_count < 20 {
                true
            } else if event_count < min_events {
                event_count % 5 == 0
            } else {
                event_count % 20 == 0
                    || hc_state.classifier.classifications().is_empty()
            }
        };

        if should_update {
            hc_state.classifier.observe(value);
            hc_state.dynamic_check_at = event_count;

            // Update has_any_dynamic cache
            let classifications = hc_state.classifier.classifications();
            hc_state.has_any_dynamic =
                classifications.values().any(|c| c.has_dynamic_fields);

            // Check if classification is now stable
            if !hc_state.classification_stable && !classifications.is_empty() {
                let all_confident = classifications
                    .values()
                    .all(|c| c.confidence >= confidence_threshold);
                let enough_events = event_count >= min_events;
                hc_state.classification_stable = all_confident && enough_events;
            }
        }

        let has_classifications =
            !hc_state.classifier.classifications().is_empty();
        (
            has_classifications && hc_state.has_any_dynamic,
            hc_state.has_any_dynamic,
        )
    }

    /// Full sensing path with optional normalization
    fn observe_value_full(
        &mut self,
        table: &str,
        value: &serde_json::Value,
        has_dynamic: bool,
    ) -> SensorResult<ObserveResult> {
        // OPTIMIZATION: Only normalize when there are dynamic fields
        let empty_map = HashMap::new();
        let inferred: InferredSchema = if has_dynamic {
            let hc_state = self.hc_states.get(table);
            let classifications = hc_state
                .map(|s| s.classifier.classifications())
                .unwrap_or(&empty_map);

            if has_any_dynamic_in_tree(classifications) {
                let normalized = normalize_value(value, "", classifications);
                let json_bytes = serde_json::to_vec(&normalized)?;
                serde_json::from_slice(&json_bytes)?
            } else {
                // No actual dynamic fields in tree, use value directly
                let json_bytes = serde_json::to_vec(value)?;
                serde_json::from_slice(&json_bytes)?
            }
        } else {
            // Fast path: no normalization needed
            let json_bytes = serde_json::to_vec(value)?;
            serde_json::from_slice(&json_bytes)?
        };

        let fingerprint =
            crate::fingerprint::compute_fingerprint(&inferred.schema);

        // Check for evolution
        let evolution_info = if let Some(state) = self.schemas.get(table) {
            if state.fingerprint != fingerprint {
                Some((state.fingerprint.clone(), state.sequence))
            } else {
                None
            }
        } else {
            None
        };

        if let Some((old_fingerprint, old_sequence)) = evolution_info {
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

        let is_new = !self.schemas.contains_key(table);
        let state =
            self.schemas.entry(table.to_string()).or_insert_with(|| {
                TableSchemaState::new(table.to_string(), inferred)
            });

        state.record_observation();

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

    /// Observe raw JSON bytes.
    pub fn observe(
        &mut self,
        table: &str,
        json: &[u8],
    ) -> SensorResult<ObserveResult> {
        let value: serde_json::Value = serde_json::from_slice(json)?;
        self.observe_value(table, &value)
    }

    // Query methods
    pub fn get_version(&self, table: &str) -> Option<SensedSchemaVersion> {
        self.schemas.get(table).map(|s| s.version())
    }

    pub fn get_snapshot(&self, table: &str) -> Option<SchemaSnapshot> {
        self.schemas.get(table).map(SchemaSnapshot::from)
    }

    pub fn tables(&self) -> Vec<&str> {
        self.schemas.keys().map(|s| s.as_str()).collect()
    }

    pub fn all_snapshots(&self) -> Vec<SchemaSnapshot> {
        self.schemas.values().map(SchemaSnapshot::from).collect()
    }

    pub fn get_schema(&self, table: &str) -> Option<&schema_analysis::Schema> {
        self.schemas.get(table).map(|s| s.schema())
    }

    pub fn event_count(&self, table: &str) -> u64 {
        self.schemas.get(table).map(|s| s.event_count).unwrap_or(0)
    }

    pub fn is_stabilized(&self, table: &str) -> bool {
        self.schemas
            .get(table)
            .map(|s| s.stabilized)
            .unwrap_or(false)
    }

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

    pub fn reset_table(&mut self, table: &str) {
        self.schemas.remove(table);
        self.structure_caches.remove(table);
        self.hc_states.remove(table);
    }

    pub fn reset_all(&mut self) {
        self.schemas.clear();
        self.structure_caches.clear();
        self.hc_states.clear();
    }

    pub fn config(&self) -> &SchemaSensingConfig {
        &self.config
    }

    pub fn to_json_schema(&self, table: &str) -> Option<JsonSchema> {
        self.schemas
            .get(table)
            .map(|s| crate::json_schema::to_json_schema(s.schema()))
    }

    pub fn all_json_schemas(&self) -> HashMap<String, JsonSchema> {
        self.schemas
            .iter()
            .map(|(t, s)| {
                (t.clone(), crate::json_schema::to_json_schema(s.schema()))
            })
            .collect()
    }

    // HC-specific methods
    pub fn get_classification(
        &self,
        table: &str,
        path: &str,
    ) -> Option<&PathClassification> {
        self.hc_states
            .get(table)
            .and_then(|s| s.classifier.classifications().get(path))
    }

    pub fn get_all_classifications(
        &self,
        table: &str,
    ) -> Option<&HashMap<String, PathClassification>> {
        self.hc_states
            .get(table)
            .map(|s| s.classifier.classifications())
    }

    pub fn detected_maps(&self, table: &str) -> Vec<&str> {
        self.hc_states
            .get(table)
            .map(|s| s.classifier.map_paths())
            .unwrap_or_default()
    }

    pub fn has_dynamic_fields(&self, table: &str) -> bool {
        self.hc_states
            .get(table)
            .map(|s| s.has_any_dynamic)
            .unwrap_or(false)
    }
}

/// Check if any path in classifications has dynamic fields
fn has_any_dynamic_in_tree(
    classifications: &HashMap<String, PathClassification>,
) -> bool {
    classifications.values().any(|c| c.has_dynamic_fields)
}

/// Normalize value - optimized to avoid cloning when possible
fn normalize_value(
    value: &serde_json::Value,
    path: &str,
    classifications: &HashMap<String, PathClassification>,
) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let class = classifications.get(path);
            let has_dynamic =
                class.map(|c| c.has_dynamic_fields).unwrap_or(false);

            if !has_dynamic {
                // No dynamic fields at this path - still need to check children
                let mut needs_normalization = false;
                for (key, _) in map {
                    let child_path = if path.is_empty() {
                        key.clone()
                    } else {
                        format!("{}.{}", path, key)
                    };
                    if classifications
                        .get(&child_path)
                        .map(|c| c.has_dynamic_fields)
                        .unwrap_or(false)
                    {
                        needs_normalization = true;
                        break;
                    }
                }

                if !needs_normalization {
                    // Fast path: no normalization needed in subtree
                    return value.clone();
                }
            }

            let stable_set: HashSet<&str> = class
                .map(|c| {
                    c.stable_fields.iter().map(|f| f.name.as_str()).collect()
                })
                .unwrap_or_default();

            let mut new_map = serde_json::Map::new();
            let mut dynamic_sample: Option<serde_json::Value> = None;

            for (key, child) in map {
                let is_dynamic =
                    has_dynamic && !stable_set.contains(key.as_str());

                if is_dynamic {
                    if dynamic_sample.is_none() {
                        let placeholder_path = if path.is_empty() {
                            "<*>".to_string()
                        } else {
                            format!("{}.<*>", path)
                        };
                        dynamic_sample = Some(normalize_value(
                            child,
                            &placeholder_path,
                            classifications,
                        ));
                    }
                } else {
                    let child_path = if path.is_empty() {
                        key.clone()
                    } else {
                        format!("{}.{}", path, key)
                    };
                    new_map.insert(
                        key.clone(),
                        normalize_value(child, &child_path, classifications),
                    );
                }
            }

            if let Some(sample) = dynamic_sample {
                new_map.insert("<dynamic>".to_string(), sample);
            }

            serde_json::Value::Object(new_map)
        }
        serde_json::Value::Array(arr) => {
            if let Some(first) = arr.first() {
                let elem_path = if path.is_empty() {
                    "[]".to_string()
                } else {
                    format!("{}[]", path)
                };
                serde_json::Value::Array(vec![normalize_value(
                    first,
                    &elem_path,
                    classifications,
                )])
            } else {
                value.clone()
            }
        }
        _ => value.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltaforge_config::SamplingConfig;
    use serde_json::json;

    #[test]
    fn test_disabled_sensor() {
        let config = SchemaSensingConfig {
            enabled: false,
            ..Default::default()
        };
        let mut sensor = SchemaSensor::new(config);
        let result = sensor.observe("test", b"{}").unwrap();
        assert!(matches!(result, ObserveResult::Disabled));
    }

    #[test]
    fn test_new_schema() {
        let mut sensor = SchemaSensor::enabled();
        let result = sensor.observe("users", br#"{"id": 1}"#).unwrap();
        assert!(matches!(result, ObserveResult::NewSchema { .. }));
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

        let make_event = |i: u64| {
            json!({
                "id": i,
                "type": "event",
                "sessions": {
                    format!("sess_{}", i): {"user_id": i % 100, "ts": 123}
                }
            })
        };

        // Warmup
        for i in 0..20 {
            sensor.observe_value("events", &make_event(i)).unwrap();
        }

        let maps = sensor.detected_maps("events");
        assert!(
            maps.contains(&"sessions"),
            "sessions should be detected as map, got: {:?}",
            maps
        );

        let val1 = make_event(100);
        let result1 = sensor.observe_value("events", &val1).unwrap();

        let val2 = make_event(101);
        let result2 = sensor.observe_value("events", &val2).unwrap();

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

    #[test]
    fn test_fingerprint_stability_with_dynamic_keys() {
        let config = SchemaSensingConfig {
            enabled: true,
            sampling: SamplingConfig {
                structure_cache: false,
                structure_cache_size: 0,
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

        let make_event = |i: u64| {
            json!({
                "id": i,
                "sessions": {
                    format!("sess_{}", i): {"ts": 123}
                }
            })
        };

        // Warmup
        for i in 0..20 {
            sensor.observe_value("events", &make_event(i)).unwrap();
        }

        let mut unchanged_count = 0;
        let mut evolved_count = 0;

        for i in 100..150 {
            let result =
                sensor.observe_value("events", &make_event(i)).unwrap();
            match result {
                ObserveResult::Unchanged { .. } => unchanged_count += 1,
                ObserveResult::Evolved { .. } => evolved_count += 1,
                _ => {}
            }
        }

        assert!(
            unchanged_count > evolved_count,
            "Expected more Unchanged than Evolved with HC on: unchanged={}, evolved={}",
            unchanged_count,
            evolved_count
        );
    }
}
