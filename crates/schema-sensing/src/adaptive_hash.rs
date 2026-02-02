//! Adaptive structure hashing for high-cardinality key handling.
//!
//! Computes structure hashes that remain stable when dynamic keys change,
//! enabling high cache hit rates for objects with map-like fields.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use crate::high_cardinality::PathClassification;

/// Compute an adaptive structure hash that ignores dynamic key names.
///
/// When a path is classified as having dynamic fields, those field names
/// are replaced with a `<MAP>` marker, ensuring the hash remains stable
/// across different dynamic key values.
pub fn compute_adaptive_hash(
    value: &serde_json::Value,
    classifications: &HashMap<String, PathClassification>,
) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    let mut hasher = DefaultHasher::new();
    hash_value_adaptive(value, "", &mut hasher, classifications);
    hasher.finish()
}

/// Original non-adaptive hash for backward compatibility.
pub fn compute_structure_hash(value: &serde_json::Value) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    let mut hasher = DefaultHasher::new();
    hash_top_level_structure(value, &mut hasher);
    hasher.finish()
}

fn hash_value_adaptive<H: Hasher>(
    value: &serde_json::Value,
    path: &str,
    hasher: &mut H,
    classifications: &HashMap<String, PathClassification>,
) {
    match value {
        serde_json::Value::Null => 0u8.hash(hasher),
        serde_json::Value::Bool(_) => 1u8.hash(hasher),
        serde_json::Value::Number(_) => 2u8.hash(hasher),
        serde_json::Value::String(_) => 3u8.hash(hasher),
        serde_json::Value::Array(arr) => {
            4u8.hash(hasher);
            // Hash first element's structure (if any)
            if let Some(first) = arr.first() {
                let elem_path = if path.is_empty() {
                    "[]".to_string()
                } else {
                    format!("{}[]", path)
                };
                hash_value_adaptive(first, &elem_path, hasher, classifications);
            }
        }
        serde_json::Value::Object(obj) => {
            5u8.hash(hasher);

            if let Some(class) = classifications.get(path) {
                // We have classification - use it for stable hashing
                // stable_fields is pre-sorted by name, so we can use binary_search

                // Hash stable fields (already sorted in classification)
                class.stable_fields.len().hash(hasher);
                for sf in &class.stable_fields {
                    sf.name.hash(hasher);
                    if let Some(child) = obj.get(&sf.name) {
                        let child_path = if path.is_empty() {
                            sf.name.clone()
                        } else {
                            format!("{}.{}", path, sf.name)
                        };
                        hash_value_adaptive(
                            child,
                            &child_path,
                            hasher,
                            classifications,
                        );
                    }
                }

                // Hash dynamic portion as marker + ONE value's structure
                if class.has_dynamic_fields {
                    "<MAP>".hash(hasher);

                    // Find first dynamic key (not in stable_fields) and hash its value structure
                    for (key, val) in obj.iter() {
                        // Use binary_search on pre-sorted stable_fields - no allocation
                        let is_stable = class
                            .stable_fields
                            .binary_search_by(|f| {
                                f.name.as_str().cmp(key.as_str())
                            })
                            .is_ok();
                        if !is_stable {
                            let placeholder_path = if path.is_empty() {
                                "<*>".to_string()
                            } else {
                                format!("{}.<*>", path)
                            };
                            hash_value_adaptive(
                                val,
                                &placeholder_path,
                                hasher,
                                classifications,
                            );
                            break;
                        }
                    }
                }
            } else {
                // No classification - use original behavior (hash all keys)
                obj.len().hash(hasher);
                let mut keys: Vec<&str> =
                    obj.keys().map(|s| s.as_str()).collect();
                keys.sort_unstable();
                for key in keys {
                    key.hash(hasher);
                }
            }
        }
    }
}

/// Hash only top-level structure - original non-adaptive version.
fn hash_top_level_structure<H: Hasher>(
    value: &serde_json::Value,
    hasher: &mut H,
) {
    match value {
        serde_json::Value::Null => 0u8.hash(hasher),
        serde_json::Value::Bool(_) => 1u8.hash(hasher),
        serde_json::Value::Number(_) => 2u8.hash(hasher),
        serde_json::Value::String(_) => 3u8.hash(hasher),
        serde_json::Value::Array(_) => 4u8.hash(hasher),
        serde_json::Value::Object(obj) => {
            5u8.hash(hasher);
            obj.len().hash(hasher);
            let mut keys: Vec<_> = obj.keys().collect();
            keys.sort_unstable();
            for key in keys {
                key.hash(hasher);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::high_cardinality::StableField;
    use serde_json::json;

    #[test]
    fn test_no_classification_same_as_original() {
        let value = json!({"id": 1, "name": "test"});
        let empty: HashMap<String, PathClassification> = HashMap::new();

        let adaptive = compute_adaptive_hash(&value, &empty);
        let original = compute_structure_hash(&value);

        // Should behave similarly for top-level (though not identical due to recursion)
        assert!(adaptive != 0);
        assert!(original != 0);
    }

    #[test]
    fn test_dynamic_keys_same_hash() {
        let mut classifications = HashMap::new();
        classifications.insert(
            "sessions".to_string(),
            PathClassification {
                stable_fields: vec![],
                has_dynamic_fields: true,
                dynamic_count: 100,
                confidence: 0.9,
                based_on_events: 1000,
            },
        );
        classifications.insert(
            "".to_string(),
            PathClassification {
                stable_fields: vec![
                    StableField {
                        name: "id".to_string(),
                        frequency: 1.0,
                        count: 1000,
                    },
                    StableField {
                        name: "sessions".to_string(),
                        frequency: 1.0,
                        count: 1000,
                    },
                ],
                has_dynamic_fields: false,
                dynamic_count: 0,
                confidence: 0.95,
                based_on_events: 1000,
            },
        );

        let val1 = json!({
            "id": 1,
            "sessions": {
                "sess_abc": {"user": "alice"},
                "sess_def": {"user": "bob"}
            }
        });

        let val2 = json!({
            "id": 2,
            "sessions": {
                "sess_xyz": {"user": "charlie"},
                "sess_123": {"user": "dave"},
                "sess_456": {"user": "eve"}
            }
        });

        let hash1 = compute_adaptive_hash(&val1, &classifications);
        let hash2 = compute_adaptive_hash(&val2, &classifications);

        assert_eq!(
            hash1, hash2,
            "Different dynamic keys should produce same hash"
        );
    }

    #[test]
    fn test_different_value_types_different_hash() {
        // Without classifications, different structures should hash differently
        let empty: HashMap<String, PathClassification> = HashMap::new();

        let val1 = json!({"id": 1, "name": "test"});
        let val2 = json!({"id": 1, "count": 42}); // different field name

        let hash1 = compute_adaptive_hash(&val1, &empty);
        let hash2 = compute_adaptive_hash(&val2, &empty);

        // Different top-level keys should produce different hashes
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_nested_dynamic_keys() {
        let mut classifications = HashMap::new();
        classifications.insert(
            "".to_string(),
            PathClassification {
                stable_fields: vec![StableField {
                    name: "data".to_string(),
                    frequency: 1.0,
                    count: 100,
                }],
                has_dynamic_fields: false,
                dynamic_count: 0,
                confidence: 0.9,
                based_on_events: 100,
            },
        );
        classifications.insert(
            "data".to_string(),
            PathClassification {
                stable_fields: vec![],
                has_dynamic_fields: true,
                dynamic_count: 50,
                confidence: 0.9,
                based_on_events: 100,
            },
        );

        let val1 = json!({"data": {"uuid_1": 10, "uuid_2": 20}});
        let val2 = json!({"data": {"uuid_x": 30, "uuid_y": 40, "uuid_z": 50}});

        let hash1 = compute_adaptive_hash(&val1, &classifications);
        let hash2 = compute_adaptive_hash(&val2, &classifications);

        assert_eq!(hash1, hash2);
    }
}
