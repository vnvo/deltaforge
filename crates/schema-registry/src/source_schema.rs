//! Source-owned schema trait.
//!
//! Each CDC source defines its own schema type implementing this trait.
//! This allows sources to capture their native semantics while providing
//! a common interface for fingerprinting and column access.

use serde::{Serialize, de::DeserializeOwned};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Trait for source-specific schema types.
///
/// Sources implement this for their schema representation.
/// The schema registry can store schemas as JSON, using this trait
/// for common operations like fingerprinting and column extraction.
pub trait SourceSchema: Serialize + DeserializeOwned + Send + Sync {
    /// Source type identifier (e.g., "mysql", "postgres", "mongodb").
    fn source_kind(&self) -> &'static str;

    /// Content-addressable fingerprint for change detection.
    ///
    /// Two schemas with the same fingerprint are considered identical.
    fn fingerprint(&self) -> String;

    /// Column/field names in ordinal order.
    fn column_names(&self) -> Vec<&str>;

    /// Primary key column names.
    fn primary_key(&self) -> Vec<&str> {
        vec![]
    }

    /// Human-readable description.
    fn describe(&self) -> String {
        format!(
            "{}({} columns)",
            self.source_kind(),
            self.column_names().len()
        )
    }
}

/// Compute fingerprint from serializable data.
///
/// Uses JSON serialization + hashing for deterministic fingerprint.
pub fn compute_fingerprint<T: Serialize>(value: &T) -> String {
    let json = serde_json::to_vec(value).unwrap_or_default();
    let mut hasher = DefaultHasher::new();
    json.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Serialize, Deserialize)]
    struct TestSchema {
        columns: Vec<String>,
    }

    impl SourceSchema for TestSchema {
        fn source_kind(&self) -> &'static str {
            "test"
        }
        fn fingerprint(&self) -> String {
            compute_fingerprint(&self.columns)
        }
        fn column_names(&self) -> Vec<&str> {
            self.columns.iter().map(|s| s.as_str()).collect()
        }
    }

    #[test]
    fn test_fingerprint_stable() {
        let s = TestSchema {
            columns: vec!["a".into(), "b".into()],
        };
        assert_eq!(s.fingerprint(), s.fingerprint());
    }

    #[test]
    fn test_fingerprint_changes() {
        let s1 = TestSchema {
            columns: vec!["a".into()],
        };
        let s2 = TestSchema {
            columns: vec!["a".into(), "b".into()],
        };
        assert_ne!(s1.fingerprint(), s2.fingerprint());
    }
}
