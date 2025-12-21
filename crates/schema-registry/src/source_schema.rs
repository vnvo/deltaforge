//! Source-owned schema trait.
//!
//! Each CDC source defines its own schema type implementing this trait.
//! This allows sources to capture their native semantics while providing
//! a common interface for fingerprinting and column access.

use serde::{Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};

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
    let hash = Sha256::digest(&json);
    format!("sha256:{}", hex::encode(hash))
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
    fn fingerprint_is_stable() {
        let s = TestSchema {
            columns: vec!["a".into(), "b".into()],
        };
        let fp1 = s.fingerprint();
        let fp2 = s.fingerprint();
        assert_eq!(fp1, fp2);
    }

    #[test]
    fn fingerprint_changes_with_content() {
        let s1 = TestSchema {
            columns: vec!["a".into()],
        };
        let s2 = TestSchema {
            columns: vec!["a".into(), "b".into()],
        };
        assert_ne!(s1.fingerprint(), s2.fingerprint());
    }

    #[test]
    fn fingerprint_has_sha256_prefix() {
        let s = TestSchema {
            columns: vec!["id".into()],
        };
        let fp = s.fingerprint();
        assert!(
            fp.starts_with("sha256:"),
            "fingerprint should start with 'sha256:', got: {}",
            fp
        );
    }

    #[test]
    fn fingerprint_has_correct_length() {
        let s = TestSchema {
            columns: vec!["id".into()],
        };
        let fp = s.fingerprint();
        // "sha256:" (7 chars) + 64 hex chars = 71 total
        assert_eq!(
            fp.len(),
            71,
            "fingerprint should be 71 chars, got: {}",
            fp.len()
        );
    }

    #[test]
    fn fingerprint_is_valid_hex() {
        let s = TestSchema {
            columns: vec!["id".into(), "name".into()],
        };
        let fp = s.fingerprint();
        let hash_part = fp.strip_prefix("sha256:").expect("should have prefix");

        // Verify it's valid hex
        assert!(
            hex::decode(hash_part).is_ok(),
            "hash part should be valid hex: {}",
            hash_part
        );
    }

    #[test]
    fn fingerprint_is_deterministic_for_known_input() {
        // This test ensures the hash doesn't change across versions
        // JSON for ["test"]: ["test"]
        let s = TestSchema {
            columns: vec!["test".into()],
        };
        let fp = s.fingerprint();

        // Pre-computed expected hash for JSON: ["test"]
        // echo -n '["test"]' | sha256sum
        let expected = "sha256:ecfd160805b1b0481fd0793c745be3b45d2054582de1c4df5d9b8fa4d78e7fbc";
        assert_eq!(fp, expected, "fingerprint should be deterministic");
    }

    #[test]
    fn fingerprint_empty_input() {
        let s = TestSchema { columns: vec![] };
        let fp = s.fingerprint();

        // JSON for []: []
        // echo -n '[]' | sha256sum
        let expected = "sha256:4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945";
        assert_eq!(fp, expected);
    }

    #[test]
    fn fingerprint_order_matters() {
        let s1 = TestSchema {
            columns: vec!["a".into(), "b".into()],
        };
        let s2 = TestSchema {
            columns: vec!["b".into(), "a".into()],
        };
        // Different order = different fingerprint
        assert_ne!(s1.fingerprint(), s2.fingerprint());
    }

    #[test]
    fn source_schema_trait_methods() {
        let s = TestSchema {
            columns: vec!["id".into(), "name".into(), "email".into()],
        };

        assert_eq!(s.source_kind(), "test");
        assert_eq!(s.column_names(), vec!["id", "name", "email"]);
        assert_eq!(s.primary_key(), Vec::<&str>::new()); // default impl
        assert_eq!(s.describe(), "test(3 columns)");
    }
}
