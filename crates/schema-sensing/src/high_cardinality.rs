//! High-cardinality key handling for schema sensing.
//!
//! This module provides field classification to distinguish between
//! stable fields (schema properties) and dynamic fields (map keys like UUIDs).
//!
//! Uses `flowstats` for probabilistic data structures.

use flowstats::cardinality::HyperLogLog;
use flowstats::frequency::SpaceSaving;
use flowstats::sampling::ReservoirSampler;
use flowstats::traits::{CardinalitySketch, HeavyHitters, Sketch};
use serde::{Deserialize, Serialize};

/// Configuration for high-cardinality field detection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HighCardinalityConfig {
    /// Enable field classification.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Minimum events before classification.
    #[serde(default = "default_min_events")]
    pub min_events: u64,

    /// Frequency threshold for stable fields (0.0-1.0).
    /// Fields in >= this fraction of events are stable.
    #[serde(default = "default_stable_threshold")]
    pub stable_threshold: f64,

    /// Minimum unique fields to consider object a map.
    #[serde(default = "default_min_dynamic_fields")]
    pub min_dynamic_fields: usize,

    /// Confidence threshold for classification.
    #[serde(default = "default_confidence_threshold")]
    pub confidence_threshold: f64,

    /// Re-evaluate every N events (0 = never).
    #[serde(default = "default_reevaluate_interval")]
    pub reevaluate_interval: u64,

    /// HyperLogLog precision (4-18).
    #[serde(default = "default_hll_precision")]
    pub hll_precision: u8,

    /// Heavy hitter capacity.
    #[serde(default = "default_heavy_hitter_capacity")]
    pub heavy_hitter_capacity: usize,

    /// Reservoir sample size.
    #[serde(default = "default_sample_size")]
    pub sample_size: usize,
}

fn default_true() -> bool {
    true
}
fn default_min_events() -> u64 {
    100
}
fn default_stable_threshold() -> f64 {
    0.5
}
fn default_min_dynamic_fields() -> usize {
    5
}
fn default_confidence_threshold() -> f64 {
    0.7
}
fn default_reevaluate_interval() -> u64 {
    10_000
}
fn default_hll_precision() -> u8 {
    12
}
fn default_heavy_hitter_capacity() -> usize {
    50
}
fn default_sample_size() -> usize {
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
    /// Memory budget per path in bytes.
    pub fn memory_per_path(&self) -> usize {
        let hll = 1 << self.hll_precision;
        let ss = self.heavy_hitter_capacity * 80;
        let rs = self.sample_size * 40;
        hll + ss + rs + 256
    }
}

/// Per-path field statistics.
///
/// Tracks field frequency and cardinality for a single JSON path.
#[derive(Debug, Clone)]
pub struct PathFieldStats {
    pub total_events: u64,
    total_field_observations: u64,
    cardinality: HyperLogLog,
    heavy_hitters: SpaceSaving<String>,
    samples: ReservoirSampler<String>,
    classification: Option<PathClassification>,
    classified_at_event: u64,
}

impl PathFieldStats {
    /// Create new stats tracker.
    pub fn new(config: &HighCardinalityConfig) -> Self {
        Self {
            total_events: 0,
            total_field_observations: 0,
            cardinality: HyperLogLog::new(config.hll_precision),
            heavy_hitters: SpaceSaving::new(config.heavy_hitter_capacity),
            samples: ReservoirSampler::new(config.sample_size),
            classification: None,
            classified_at_event: 0,
        }
    }

    /// Observe field names from an event.
    pub fn observe(&mut self, fields: &[&str]) {
        self.total_events += 1;
        for field in fields {
            self.total_field_observations += 1;
            self.cardinality.insert(field);
            // Allocate once, clone once (not two to_string() calls)
            let s = field.to_string();
            self.heavy_hitters.add(s.clone());
            self.samples.add(s);
        }
    }

    /// Observe from owned strings.
    pub fn observe_owned(&mut self, fields: &[String]) {
        self.total_events += 1;
        for field in fields {
            self.total_field_observations += 1;
            self.cardinality.insert(field);
            // Allocate once, clone once (not two to_string() calls)
            self.heavy_hitters.add(field.clone());
            self.samples.add(field.clone());
        }
    }

    /// Estimated unique field count.
    pub fn unique_field_count(&self) -> u64 {
        self.cardinality.estimate() as u64
    }

    /// Get top-k heavy hitters.
    pub fn get_heavy_hitters(&self, k: usize) -> Vec<(String, u64)> {
        self.heavy_hitters.top_k(k)
    }

    /// Get field samples (for pattern extraction).
    pub fn get_samples(&self) -> &[String] {
        self.samples.sample()
    }

    /// Get samples excluding stable fields.
    pub fn get_dynamic_samples(&self, stable: &[String]) -> Vec<&str> {
        self.samples
            .sample()
            .iter()
            .map(|s| s.as_str())
            .filter(|s| !stable.iter().any(|st| st == *s))
            .collect()
    }

    /// Get cached classification.
    pub fn classification(&self) -> Option<&PathClassification> {
        self.classification.as_ref()
    }

    /// Check if classification needs refresh.
    pub fn needs_classification(&self, config: &HighCardinalityConfig) -> bool {
        if self.total_events < config.min_events {
            return false;
        }
        if self.classification.is_none() {
            return true;
        }
        if config.reevaluate_interval > 0 {
            let since = self.total_events - self.classified_at_event;
            return since >= config.reevaluate_interval;
        }
        false
    }

    /// Classify fields as stable vs dynamic.
    pub fn classify(
        &mut self,
        config: &HighCardinalityConfig,
    ) -> Option<&PathClassification> {
        if self.total_events < config.min_events {
            return None;
        }

        // Get heavy hitters and convert to event frequency
        let avg_fields_per_event =
            self.total_field_observations as f64 / self.total_events as f64;
        let top_fields = self.heavy_hitters.top_k(config.heavy_hitter_capacity);

        let mut stable_fields: Vec<StableField> = top_fields
            .into_iter()
            .map(|(name, count)| {
                // Convert observation count to event frequency estimate
                let obs_frequency =
                    count as f64 / self.total_field_observations as f64;
                let event_frequency =
                    (obs_frequency * avg_fields_per_event).min(1.0);
                StableField {
                    name,
                    frequency: event_frequency,
                    count,
                }
            })
            .filter(|f| f.frequency >= config.stable_threshold)
            .collect();

        // Pre-sort by name for efficient binary_search lookup in adaptive_hash
        stable_fields.sort_unstable_by(|a, b| a.name.cmp(&b.name));

        let unique_count = self.cardinality.estimate() as usize;
        let stable_count = stable_fields.len();
        let dynamic_count = unique_count.saturating_sub(stable_count);
        let has_dynamic = dynamic_count >= config.min_dynamic_fields;

        // Confidence based on sample size and separation
        // Scale by min_events (not hardcoded 1000) so classification succeeds near min_events
        let denom = config.min_events.max(1) as f64;
        let sample_factor = (self.total_events as f64 / denom).min(1.0).sqrt();
        let separation = if stable_fields.is_empty() {
            0.5
        } else {
            let min_freq = stable_fields
                .iter()
                .map(|f| f.frequency)
                .fold(f64::MAX, f64::min);
            ((min_freq - config.stable_threshold) / min_freq).clamp(0.0, 1.0)
        };
        let confidence = sample_factor * 0.5 + separation * 0.5;

        if confidence < config.confidence_threshold {
            return None;
        }

        let classification = PathClassification {
            stable_fields,
            has_dynamic_fields: has_dynamic,
            dynamic_count,
            confidence,
            based_on_events: self.total_events,
        };

        self.classification = Some(classification);
        self.classified_at_event = self.total_events;
        self.classification.as_ref()
    }

    /// Clear cached classification.
    pub fn invalidate_classification(&mut self) {
        self.classification = None;
    }

    /// Reset all state.
    pub fn clear(&mut self) {
        self.total_events = 0;
        self.total_field_observations = 0;
        self.cardinality.clear();
        self.heavy_hitters.clear();
        self.samples.clear();
        self.classification = None;
        self.classified_at_event = 0;
    }
}

/// Classification result for a JSON path.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PathClassification {
    /// Fields that appear consistently.
    pub stable_fields: Vec<StableField>,
    /// Whether dynamic (map-like) fields exist.
    pub has_dynamic_fields: bool,
    /// Estimated count of dynamic fields.
    pub dynamic_count: usize,
    /// Confidence in classification (0.0-1.0).
    pub confidence: f64,
    /// Events observed when classified.
    pub based_on_events: u64,
}

/// A stable (frequently occurring) field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StableField {
    /// Field name.
    pub name: String,
    /// Estimated event frequency (0.0-1.0).
    pub frequency: f64,
    /// Observation count.
    pub count: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> HighCardinalityConfig {
        HighCardinalityConfig {
            min_events: 10,
            confidence_threshold: 0.3,
            ..Default::default()
        }
    }

    #[test]
    fn test_pure_struct() {
        let config = test_config();
        let mut stats = PathFieldStats::new(&config);

        for _ in 0..50 {
            stats.observe(&["id", "name", "email"]);
        }

        let class = stats.classify(&config).unwrap();
        assert_eq!(class.stable_fields.len(), 3);
        assert!(!class.has_dynamic_fields);
    }

    #[test]
    fn test_pure_map() {
        let config = HighCardinalityConfig {
            min_dynamic_fields: 3,
            ..test_config()
        };
        let mut stats = PathFieldStats::new(&config);

        for i in 0..100 {
            stats.observe(&[&format!("uuid_{i}")]);
        }

        let class = stats.classify(&config).unwrap();
        assert!(class.stable_fields.is_empty());
        assert!(class.has_dynamic_fields);
    }

    #[test]
    fn test_mixed() {
        let config = HighCardinalityConfig {
            min_dynamic_fields: 3,
            stable_threshold: 0.5,
            ..test_config()
        };
        let mut stats = PathFieldStats::new(&config);

        for i in 0..100 {
            stats.observe(&["id", "type", &format!("sess_{i}")]);
        }

        let class = stats.classify(&config).unwrap();

        let stable_names: Vec<_> =
            class.stable_fields.iter().map(|f| &f.name).collect();
        assert!(stable_names.contains(&&"id".to_string()));
        assert!(stable_names.contains(&&"type".to_string()));
        assert!(class.has_dynamic_fields);
    }

    #[test]
    fn test_insufficient_events() {
        let config = HighCardinalityConfig {
            min_events: 100,
            ..Default::default()
        };
        let mut stats = PathFieldStats::new(&config);

        for _ in 0..50 {
            stats.observe(&["id", "name"]);
        }

        assert!(stats.classify(&config).is_none());
    }

    #[test]
    fn test_reevaluation() {
        let config = HighCardinalityConfig {
            reevaluate_interval: 50,
            ..test_config()
        };
        let mut stats = PathFieldStats::new(&config);

        for _ in 0..20 {
            stats.observe(&["a", "b"]);
        }
        stats.classify(&config);

        assert!(!stats.needs_classification(&config));

        for _ in 0..50 {
            stats.observe(&["a", "b"]);
        }

        assert!(stats.needs_classification(&config));
    }

    #[test]
    fn test_dynamic_samples() {
        let config = HighCardinalityConfig::default();
        let mut stats = PathFieldStats::new(&config);

        stats.observe(&["stable", "dyn_1"]);
        stats.observe(&["stable", "dyn_2"]);
        stats.observe(&["stable", "dyn_3"]);

        let stable = vec!["stable".to_string()];
        let dynamic = stats.get_dynamic_samples(&stable);

        assert!(!dynamic.contains(&"stable"));
        assert!(dynamic.iter().any(|s| s.starts_with("dyn_")));
    }

    #[test]
    fn test_unique_count() {
        let config = HighCardinalityConfig::default();
        let mut stats = PathFieldStats::new(&config);

        for i in 0..1000 {
            stats.observe(&[&format!("field_{i}")]);
        }

        let estimate = stats.unique_field_count();
        let error = (estimate as f64 - 1000.0).abs() / 1000.0;
        assert!(error < 0.05, "HLL error {error:.2} too high");
    }
}
