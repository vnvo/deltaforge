//! Schema fingerprint generation.
//!
//! Generates stable SHA-256 fingerprints from inferred schemas
//! for version tracking and cache invalidation.
//!
//! The fingerprint only considers the *structure* of the schema
//! (types and field names), not the statistical aggregations
//! (min/max, samples, etc.) that change with every observation.

use schema_analysis::Schema;
use sha2::{Digest, Sha256};

/// Compute a stable fingerprint for a schema's structure.
///
/// The fingerprint is a hex-encoded SHA-256 hash of the schema's
/// structural representation. This provides:
/// - Stability: Same structure always produces same fingerprint
/// - Uniqueness: Different structures produce different fingerprints
/// - Compactness: 16-char hex string (first 8 bytes of hash)
///
/// Note: This only considers the structural shape (types, field names),
/// not the statistical context (min/max, samples, etc.).
pub fn compute_fingerprint(schema: &Schema) -> String {
    let mut hasher = Sha256::new();
    hash_structure(schema, &mut hasher);
    let result = hasher.finalize();

    // Return hex-encoded hash (first 8 bytes = 16 hex chars)
    hex::encode(&result[..8])
}

/// Recursively hash the structural parts of a schema.
fn hash_structure(schema: &Schema, hasher: &mut Sha256) {
    match schema {
        Schema::Null(_) => {
            hasher.update(b"null");
        }
        Schema::Boolean(_) => {
            hasher.update(b"bool");
        }
        Schema::Integer(_) => {
            hasher.update(b"int");
        }
        Schema::Float(_) => {
            hasher.update(b"float");
        }
        Schema::String(_) => {
            hasher.update(b"string");
        }
        Schema::Bytes(_) => {
            hasher.update(b"bytes");
        }
        Schema::Sequence { field, .. } => {
            hasher.update(b"seq[");
            // Hash the field's status
            hash_field_status(&field.status, hasher);
            // Hash the inner schema if present
            if let Some(ref inner) = field.schema {
                hash_structure(inner, hasher);
            } else {
                hasher.update(b"?");
            }
            hasher.update(b"]");
        }
        Schema::Struct { fields, .. } => {
            hasher.update(b"struct{");
            // Fields are in a BTreeMap, so iteration order is deterministic
            for (name, field) in fields {
                hasher.update(name.as_bytes());
                hasher.update(b":");
                hash_field_status(&field.status, hasher);
                if let Some(ref inner) = field.schema {
                    hash_structure(inner, hasher);
                } else {
                    hasher.update(b"?");
                }
                hasher.update(b",");
            }
            hasher.update(b"}");
        }
        Schema::Union { variants } => {
            hasher.update(b"union(");
            // Sort variants by type for deterministic ordering
            let mut sorted: Vec<_> = variants.iter().collect();
            sorted.sort_by_key(|s| variant_order(s));
            for variant in sorted {
                hash_structure(variant, hasher);
                hasher.update(b"|");
            }
            hasher.update(b")");
        }
    }
}

/// Hash the field status flags that affect structure.
fn hash_field_status(status: &schema_analysis::FieldStatus, hasher: &mut Sha256) {
    let mut flags = 0u8;
    if status.may_be_null {
        flags |= 1;
    }
    if status.may_be_missing {
        flags |= 2;
    }
    hasher.update(&[flags]);
}

/// Get a sort order for schema variants.
fn variant_order(schema: &Schema) -> u8 {
    match schema {
        Schema::Null(_) => 0,
        Schema::Boolean(_) => 1,
        Schema::Integer(_) => 2,
        Schema::Float(_) => 3,
        Schema::String(_) => 4,
        Schema::Bytes(_) => 5,
        Schema::Sequence { .. } => 6,
        Schema::Struct { .. } => 7,
        Schema::Union { .. } => 8,
    }
}

/// Compute a short fingerprint (8 chars) for display.
pub fn compute_short_fingerprint(schema: &Schema) -> String {
    let full = compute_fingerprint(schema);
    full.chars().take(8).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use schema_analysis::InferredSchema;

    #[test]
    fn test_fingerprint_stability() {
        let json = br#"{"id": 1, "name": "test"}"#;
        let inferred: InferredSchema = serde_json::from_slice(json).unwrap();

        let fp1 = compute_fingerprint(&inferred.schema);
        let fp2 = compute_fingerprint(&inferred.schema);
        assert_eq!(fp1, fp2);
    }

    #[test]
    fn test_fingerprint_same_structure_different_values() {
        // Same structure, different values should have same fingerprint
        let json1 = br#"{"id": 1, "name": "Alice"}"#;
        let json2 = br#"{"id": 999, "name": "Bob"}"#;

        let inferred1: InferredSchema = serde_json::from_slice(json1).unwrap();
        let inferred2: InferredSchema = serde_json::from_slice(json2).unwrap();

        let fp1 = compute_fingerprint(&inferred1.schema);
        let fp2 = compute_fingerprint(&inferred2.schema);
        assert_eq!(fp1, fp2, "Same structure should have same fingerprint");
    }

    #[test]
    fn test_fingerprint_uniqueness() {
        let json1 = br#"{"id": 1}"#;
        let json2 = br#"{"name": "test"}"#;

        let inferred1: InferredSchema = serde_json::from_slice(json1).unwrap();
        let inferred2: InferredSchema = serde_json::from_slice(json2).unwrap();

        let fp1 = compute_fingerprint(&inferred1.schema);
        let fp2 = compute_fingerprint(&inferred2.schema);
        assert_ne!(fp1, fp2);
    }

    #[test]
    fn test_fingerprint_different_types() {
        let json1 = br#"{"value": 123}"#;
        let json2 = br#"{"value": "text"}"#;

        let inferred1: InferredSchema = serde_json::from_slice(json1).unwrap();
        let inferred2: InferredSchema = serde_json::from_slice(json2).unwrap();

        let fp1 = compute_fingerprint(&inferred1.schema);
        let fp2 = compute_fingerprint(&inferred2.schema);
        assert_ne!(fp1, fp2, "Different types should have different fingerprint");
    }

    #[test]
    fn test_fingerprint_length() {
        let json = br#"{"test": true}"#;
        let inferred: InferredSchema = serde_json::from_slice(json).unwrap();

        let fp = compute_fingerprint(&inferred.schema);
        assert_eq!(fp.len(), 16); // 8 bytes = 16 hex chars
    }

    #[test]
    fn test_short_fingerprint() {
        let json = br#"{"id": 1}"#;
        let inferred: InferredSchema = serde_json::from_slice(json).unwrap();

        let short = compute_short_fingerprint(&inferred.schema);
        assert_eq!(short.len(), 8);
    }
}