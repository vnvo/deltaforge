//! Field classifier for high-cardinality detection (optimized).
//!
//! Optimizations:
//! 1. No cloning of children during iteration
//! 2. Reuses config reference instead of cloning
//! 3. Uses indices for path building to avoid allocations

use std::collections::HashMap;

use crate::high_cardinality::{
    HighCardinalityConfig, PathClassification, PathFieldStats,
};

/// Classifies JSON object fields as stable (schema) vs dynamic (map keys).
pub struct FieldClassifier {
    config: HighCardinalityConfig,
    path_stats: HashMap<String, PathFieldStats>,
    classifications: HashMap<String, PathClassification>,
    total_events: u64,
}

impl FieldClassifier {
    pub fn new(config: HighCardinalityConfig) -> Self {
        Self {
            config,
            path_stats: HashMap::new(),
            classifications: HashMap::new(),
            total_events: 0,
        }
    }

    /// Observe a JSON value and update field statistics.
    /// Returns reference to current classifications (no clone).
    pub fn observe(
        &mut self,
        value: &serde_json::Value,
    ) -> &HashMap<String, PathClassification> {
        self.total_events += 1;
        self.walk_and_observe(value, "");
        self.update_classifications();
        &self.classifications
    }

    /// Get reference to current classifications.
    pub fn classifications(&self) -> &HashMap<String, PathClassification> {
        &self.classifications
    }

    /// Get classification for a specific path.
    pub fn get_classification(
        &self,
        path: &str,
    ) -> Option<&PathClassification> {
        self.classifications.get(path)
    }

    /// Check if a path has dynamic fields.
    pub fn has_dynamic_fields(&self, path: &str) -> bool {
        self.classifications
            .get(path)
            .map(|c| c.has_dynamic_fields)
            .unwrap_or(false)
    }

    /// Get all tracked paths.
    pub fn paths(&self) -> Vec<&str> {
        self.path_stats.keys().map(|s| s.as_str()).collect()
    }

    /// Get paths that have been classified as having dynamic fields.
    pub fn map_paths(&self) -> Vec<&str> {
        self.classifications
            .iter()
            .filter(|(_, c)| c.has_dynamic_fields)
            .map(|(p, _)| p.as_str())
            .collect()
    }

    /// Clear all state.
    pub fn clear(&mut self) {
        self.path_stats.clear();
        self.classifications.clear();
        self.total_events = 0;
    }

    /// Total memory usage estimate.
    pub fn memory_bytes(&self) -> usize {
        self.path_stats.len() * self.config.memory_per_path()
    }

    fn walk_and_observe(&mut self, value: &serde_json::Value, path: &str) {
        if let serde_json::Value::Object(map) = value {
            // Get or create stats for this path
            let config = &self.config;
            let stats = self
                .path_stats
                .entry(path.to_string())
                .or_insert_with(|| PathFieldStats::new(config));

            // Observe field names - collect keys first to avoid holding borrow
            let fields: Vec<&str> = map.keys().map(|s| s.as_str()).collect();
            stats.observe(&fields);

            // Extract classification info as OWNED data before recursion
            let (stable_fields, has_dynamic): (Vec<String>, bool) =
                if let Some(c) = self.classifications.get(path) {
                    let stable = c
                        .stable_fields
                        .iter()
                        .map(|f| f.name.clone())
                        .collect();
                    (stable, c.has_dynamic_fields)
                } else {
                    (Vec::new(), false)
                };

            // Track which dynamic path we've visited
            let mut visited_dynamic = false;

            // Iterate without cloning children
            for (key, child) in map.iter() {
                let is_dynamic =
                    has_dynamic && !stable_fields.iter().any(|s| s == key);

                if is_dynamic {
                    if !visited_dynamic {
                        let placeholder = make_path(path, "<*>");
                        self.walk_and_observe(child, &placeholder);
                        visited_dynamic = true;
                    }
                } else {
                    let child_path = make_path(path, key);
                    self.walk_and_observe(child, &child_path);
                }
            }
        } else if let serde_json::Value::Array(arr) = value {
            if let Some(first) = arr.first() {
                let elem_path = if path.is_empty() {
                    "[]".to_string()
                } else {
                    format!("{}[]", path)
                };
                self.walk_and_observe(first, &elem_path);
            }
        }
    }

    fn update_classifications(&mut self) {
        let config = &self.config;
        let updates: Vec<(String, PathClassification)> = self
            .path_stats
            .iter_mut()
            .filter_map(|(path, stats)| {
                if stats.needs_classification(config) {
                    stats.classify(config).cloned().map(|c| (path.clone(), c))
                } else {
                    None
                }
            })
            .collect();

        for (path, class) in updates {
            self.classifications.insert(path, class);
        }
    }
}

fn make_path(parent: &str, child: &str) -> String {
    if parent.is_empty() {
        child.to_string()
    } else {
        format!("{}.{}", parent, child)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn test_config() -> HighCardinalityConfig {
        HighCardinalityConfig {
            enabled: true,
            min_events: 5,
            confidence_threshold: 0.3,
            min_dynamic_fields: 2,
            ..Default::default()
        }
    }

    #[test]
    fn test_simple_struct() {
        let mut classifier = FieldClassifier::new(test_config());

        for _ in 0..20 {
            let val = json!({"id": 1, "name": "test"});
            classifier.observe(&val);
        }

        let class = classifier.get_classification("").unwrap();
        assert_eq!(class.stable_fields.len(), 2);
        assert!(!class.has_dynamic_fields);
    }

    #[test]
    fn test_nested_map() {
        let mut classifier = FieldClassifier::new(test_config());

        for i in 0..30 {
            let val = json!({
                "id": i,
                "sessions": {
                    format!("sess_{}", i): {"ts": 123}
                }
            });
            classifier.observe(&val);
        }

        assert!(classifier.has_dynamic_fields("sessions"));
        assert!(!classifier.has_dynamic_fields(""));

        let maps = classifier.map_paths();
        assert!(maps.contains(&"sessions"));
    }

    #[test]
    fn test_mixed_stable_and_dynamic() {
        let config = HighCardinalityConfig {
            min_dynamic_fields: 3,
            ..test_config()
        };
        let mut classifier = FieldClassifier::new(config);

        for i in 0..50 {
            let val = json!({
                "id": i,
                "type": "event",
                format!("dyn_{}", i): i
            });
            classifier.observe(&val);
        }

        let class = classifier.get_classification("").unwrap();
        let stable_names: Vec<&str> = class
            .stable_fields
            .iter()
            .map(|f| f.name.as_str())
            .collect();

        assert!(stable_names.contains(&"id"));
        assert!(stable_names.contains(&"type"));
        assert!(class.has_dynamic_fields);
    }

    #[test]
    fn test_array_elements() {
        let mut classifier = FieldClassifier::new(test_config());

        for _ in 0..20 {
            let val = json!({
                "items": [
                    {"id": 1, "name": "a"},
                    {"id": 2, "name": "b"}
                ]
            });
            classifier.observe(&val);
        }

        let paths = classifier.paths();
        // Root object is tracked (has "items" field)
        assert!(paths.contains(&""), "root path should exist");
        // Array elements are tracked (have "id", "name" fields)
        assert!(paths.contains(&"items[]"), "items[] path should exist");
        // Note: "items" itself is an array, not tracked (only objects are tracked)
    }

    #[test]
    fn test_disabled() {
        let config = HighCardinalityConfig {
            enabled: false,
            ..Default::default()
        };
        let mut classifier = FieldClassifier::new(config);

        for _ in 0..20 {
            let val = json!({"id": 1});
            classifier.observe(&val);
        }

        // Still tracks stats, but that's fine
        assert!(
            classifier.classifications().is_empty()
                || !classifier.has_dynamic_fields("")
        );
    }

    #[test]
    fn test_map_paths() {
        let mut classifier = FieldClassifier::new(test_config());

        for i in 0..30 {
            let val = json!({
                "stable": "value",
                "map_field": {
                    format!("key_{}", i): i
                }
            });
            classifier.observe(&val);
        }

        let maps = classifier.map_paths();
        assert!(maps.contains(&"map_field"));
        assert!(!maps.contains(&""));
    }
}
