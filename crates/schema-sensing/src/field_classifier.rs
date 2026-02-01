//! Field classifier for JSON values.
//!
//! Manages per-path field statistics and walks JSON values to update them.

use std::collections::HashMap;

use crate::high_cardinality::{
    HighCardinalityConfig, PathClassification, PathFieldStats,
};

/// Manages field classification across all paths for a table.
#[derive(Debug)]
pub struct FieldClassifier {
    config: HighCardinalityConfig,
    /// Stats per JSON path (e.g., "", "metadata", "metadata.tags")
    path_stats: HashMap<String, PathFieldStats>,
    /// Cached classifications
    classifications: HashMap<String, PathClassification>,
}

impl FieldClassifier {
    pub fn new(config: HighCardinalityConfig) -> Self {
        Self {
            config,
            path_stats: HashMap::new(),
            classifications: HashMap::new(),
        }
    }

    /// Check if classification is enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Observe a JSON value and update all path statistics.
    ///
    /// Returns current classifications (may be stale if not enough events yet).
    pub fn observe(
        &mut self,
        value: &serde_json::Value,
    ) -> &HashMap<String, PathClassification> {
        if !self.config.enabled {
            return &self.classifications;
        }

        self.walk_and_observe(value, "");
        self.update_classifications();
        &self.classifications
    }

    /// Get current classifications without observing.
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

    /// Get stats for a specific path.
    pub fn get_stats(&self, path: &str) -> Option<&PathFieldStats> {
        self.path_stats.get(path)
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
    }

    /// Total memory usage estimate.
    pub fn memory_bytes(&self) -> usize {
        self.path_stats.len() * self.config.memory_per_path()
    }

    fn walk_and_observe(&mut self, value: &serde_json::Value, path: &str) {
        if let serde_json::Value::Object(map) = value {
            // Clone config before borrowing path_stats to avoid borrow conflict
            let config = self.config.clone();

            // Get or create stats for this path
            let stats = self
                .path_stats
                .entry(path.to_string())
                .or_insert_with(|| PathFieldStats::new(&config));

            // Observe field names at this path
            let fields: Vec<&str> = map.keys().map(|s| s.as_str()).collect();
            stats.observe(&fields);

            // Extract classification data BEFORE recursing (to avoid borrow conflict)
            let (stable_set, has_dynamic): (
                std::collections::HashSet<String>,
                bool,
            ) = if let Some(class) = self.classifications.get(path) {
                let set = class
                    .stable_fields
                    .iter()
                    .map(|f| f.name.clone())
                    .collect();
                (set, class.has_dynamic_fields)
            } else {
                (std::collections::HashSet::new(), false)
            };

            // Collect children to process (to avoid borrowing map during recursion)
            let children: Vec<(String, serde_json::Value)> =
                map.iter().map(|(k, v)| (k.clone(), v.clone())).collect();

            let mut visited_dynamic = false;

            for (key, child) in children {
                let is_dynamic = has_dynamic && !stable_set.contains(&key);

                if is_dynamic {
                    // For dynamic keys, use placeholder path and only visit once
                    if !visited_dynamic {
                        let placeholder = make_path(path, "<*>");
                        self.walk_and_observe(&child, &placeholder);
                        visited_dynamic = true;
                    }
                } else {
                    // Stable field - recurse normally
                    let child_path = make_path(path, &key);
                    self.walk_and_observe(&child, &child_path);
                }
            }
        } else if let serde_json::Value::Array(arr) = value {
            // For arrays, observe first element with [] suffix
            if let Some(first) = arr.first() {
                let elem_path = if path.is_empty() {
                    "[]".to_string()
                } else {
                    format!("{}[]", path)
                };
                self.walk_and_observe(first, &elem_path);
            }
        }
        // Primitives don't need path tracking
    }

    fn update_classifications(&mut self) {
        // Clone config to avoid borrow conflict
        let config = self.config.clone();

        // Collect updates first to avoid borrow conflict with classifications
        let updates: Vec<(String, PathClassification)> = self
            .path_stats
            .iter_mut()
            .filter_map(|(path, stats)| {
                if stats.needs_classification(&config) {
                    stats.classify(&config).map(|c| (path.clone(), c.clone()))
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
            min_events: 10,
            confidence_threshold: 0.3,
            min_dynamic_fields: 3,
            ..Default::default()
        }
    }

    #[test]
    fn test_simple_struct() {
        let mut classifier = FieldClassifier::new(test_config());

        for i in 0..50 {
            let val = json!({"id": i, "name": format!("user_{i}")});
            classifier.observe(&val);
        }

        let class = classifier.get_classification("").unwrap();
        assert_eq!(class.stable_fields.len(), 2);
        assert!(!class.has_dynamic_fields);
    }

    #[test]
    fn test_nested_map() {
        let mut classifier = FieldClassifier::new(test_config());

        for i in 0..100 {
            let val = json!({
                "id": i,
                "sessions": {
                    format!("sess_{i}"): {"ts": 123}
                }
            });
            classifier.observe(&val);
        }

        // Root should have stable fields
        let root = classifier.get_classification("").unwrap();
        assert!(root.stable_fields.iter().any(|f| f.name == "id"));
        assert!(root.stable_fields.iter().any(|f| f.name == "sessions"));

        // sessions path should be classified as map
        let sessions = classifier.get_classification("sessions").unwrap();
        assert!(sessions.has_dynamic_fields);
        assert!(sessions.stable_fields.is_empty());
    }

    #[test]
    fn test_mixed_stable_and_dynamic() {
        let mut classifier = FieldClassifier::new(test_config());

        for i in 0..100 {
            let val = json!({
                "version": "1.0",
                "data": {
                    "type": "event",
                    format!("trace_{i}"): {"value": i}
                }
            });
            classifier.observe(&val);
        }

        let data = classifier.get_classification("data").unwrap();
        // "type" should be stable
        assert!(data.stable_fields.iter().any(|f| f.name == "type"));
        // Should also have dynamic fields
        assert!(data.has_dynamic_fields);
    }

    #[test]
    fn test_array_elements() {
        let mut classifier = FieldClassifier::new(test_config());

        for i in 0..50 {
            let val = json!({
                "items": [
                    {"id": i, "name": "item"}
                ]
            });
            classifier.observe(&val);
        }

        // Should track items[] path
        assert!(classifier.get_stats("items[]").is_some());
    }

    #[test]
    fn test_map_paths() {
        let mut classifier = FieldClassifier::new(test_config());

        for i in 0..100 {
            let val = json!({
                "users": {format!("user_{i}"): {}},
                "config": {"debug": true}
            });
            classifier.observe(&val);
        }

        let maps = classifier.map_paths();
        assert!(maps.contains(&"users"));
        assert!(!maps.contains(&"config"));
    }

    #[test]
    fn test_disabled() {
        let config = HighCardinalityConfig {
            enabled: false,
            ..Default::default()
        };
        let mut classifier = FieldClassifier::new(config);

        let val = json!({"id": 1});
        classifier.observe(&val);

        assert!(classifier.classifications().is_empty());
    }
}
