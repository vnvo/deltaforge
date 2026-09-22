//! Deterministic content hashing for durable S3 objects (P0.4).
//!
//! A durable data object's identity is a hash of **its exact durable bytes**
//! together with an **encoding/version domain** (format + format version + schema
//! id). Folding the domain into the hash guarantees that a change in how bytes
//! are produced (a new format, a bumped encoder version, a different schema)
//! can never collide with previously written content, even if the logical rows
//! are identical. The deterministic encoders in `durable_encode` guarantee the
//! *bytes* half; this module guarantees the *identity* half.
//!
//! Staged scaffolding: consumed by the object-key builder in a later P0.4 commit.
#![allow(dead_code)]

use sha2::{Digest, Sha256};

/// The encoding/version domain that qualifies a content hash. Two objects with
/// identical bytes but different domains hash differently and never collide.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodingDomain {
    /// Format label, e.g. `"parquet"` or `"jsonl"`.
    pub format: &'static str,
    /// Encoder version. Bump when the byte layout of an unchanged input changes.
    pub format_version: u16,
    /// Schema identity for the encoded rows (e.g. the Arrow schema fingerprint).
    pub schema_id: String,
}

impl EncodingDomain {
    pub fn new(
        format: &'static str,
        format_version: u16,
        schema_id: impl Into<String>,
    ) -> Self {
        Self {
            format,
            format_version,
            schema_id: schema_id.into(),
        }
    }

    /// Stable, length-prefixed domain bytes, so distinct field values can never
    /// produce the same concatenation (e.g. format "a"+"bc" vs "ab"+"c").
    fn domain_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for part in [
            self.format.as_bytes(),
            &self.format_version.to_be_bytes()[..],
            self.schema_id.as_bytes(),
        ] {
            out.extend_from_slice(&(part.len() as u64).to_be_bytes());
            out.extend_from_slice(part);
        }
        out
    }
}

/// Content hash of `bytes` qualified by `domain`, as lowercase hex. Uses SHA-256
/// (the same family as the P0.3 stable `EventId`) and is stable across processes
/// and runs given identical inputs.
pub fn content_hash(bytes: &[u8], domain: &EncodingDomain) -> String {
    let mut h = Sha256::new();
    // Domain first (length-prefixed), then the exact object bytes.
    h.update(domain.domain_bytes());
    h.update(bytes);
    let digest = h.finalize();
    let mut s = String::with_capacity(digest.len() * 2);
    for b in digest {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dom() -> EncodingDomain {
        EncodingDomain::new("parquet", 1, "schema-abc")
    }

    #[test]
    fn hash_is_stable_for_identical_inputs() {
        let a = content_hash(b"same-bytes", &dom());
        let b = content_hash(b"same-bytes", &dom());
        assert_eq!(a, b);
        assert_eq!(a.len(), 64); // 32-byte SHA-256 as hex
    }

    #[test]
    fn different_bytes_differ() {
        assert_ne!(content_hash(b"one", &dom()), content_hash(b"two", &dom()));
    }

    #[test]
    fn different_domain_differs_for_identical_bytes() {
        let bytes = b"identical-logical-rows";
        let parquet =
            content_hash(bytes, &EncodingDomain::new("parquet", 1, "s"));
        let jsonl = content_hash(bytes, &EncodingDomain::new("jsonl", 1, "s"));
        let bumped =
            content_hash(bytes, &EncodingDomain::new("parquet", 2, "s"));
        let other_schema =
            content_hash(bytes, &EncodingDomain::new("parquet", 1, "s2"));
        assert_ne!(parquet, jsonl);
        assert_ne!(parquet, bumped);
        assert_ne!(parquet, other_schema);
    }

    #[test]
    fn domain_field_boundaries_are_unambiguous() {
        // Length-prefixing means these two do not collide even though the naive
        // concatenation of their fields would.
        let a = content_hash(b"x", &EncodingDomain::new("ab", 1, "c"));
        let b = content_hash(b"x", &EncodingDomain::new("a", 1, "bc"));
        assert_ne!(a, b);
    }
}
