//! Deterministic content hashing for durable S3 objects (P0.4).
//!
//! A durable data object's identity is a hash of **its exact durable bytes**
//! together with an **encoding/version domain** (format + format version + schema
//! id + compression codec + partition spec/version). Folding the domain into the
//! hash guarantees that a change in how bytes are produced (a new format, a
//! bumped encoder version, a different schema, a different compression codec, or
//! different partition semantics) can never collide with previously written
//! content, even if the logical rows are identical. The deterministic encoders in
//! `durable_encode` guarantee the *bytes* half; this module guarantees the
//! *identity* half - and it is the exact identity compaction compatibility keys
//! on, so two objects are compaction-compatible only if their full domains match.
//!
//! Staged scaffolding: consumed by the object-key builder in a later P0.4 commit.
#![allow(dead_code)]

use sha2::{Digest, Sha256};

/// The encoding/version domain that qualifies a content hash. Two objects with
/// identical bytes but different domains hash differently and never collide.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodingDomain {
    /// Format label, e.g. `"parquet"` or `"jsonl"`. Owned so it can be
    /// reconstructed from a manifest record during recovery.
    pub format: String,
    /// Encoder version. Bump when the byte layout of an unchanged input changes.
    pub format_version: u16,
    /// Schema identity for the encoded rows (e.g. the Arrow schema fingerprint).
    pub schema_id: String,
    /// Compression codec label, e.g. `"none"`, `"snappy"`, `"zstd"`. Part of the
    /// identity: Snappy and Zstd objects with the same schema are NOT compatible.
    pub compression: String,
    /// Partition-spec identity, e.g. `"table"`. Even though durable v2 currently
    /// partitions only by table, this is explicit so a future partition scheme
    /// cannot silently be mixed with an older one.
    pub partition_spec: String,
    /// Version of the partition spec. Bump when partition semantics change.
    pub partition_version: u16,
}

impl EncodingDomain {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        format: impl Into<String>,
        format_version: u16,
        schema_id: impl Into<String>,
        compression: impl Into<String>,
        partition_spec: impl Into<String>,
        partition_version: u16,
    ) -> Self {
        Self {
            format: format.into(),
            format_version,
            schema_id: schema_id.into(),
            compression: compression.into(),
            partition_spec: partition_spec.into(),
            partition_version,
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
            self.compression.as_bytes(),
            self.partition_spec.as_bytes(),
            &self.partition_version.to_be_bytes()[..],
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

/// Version of the durable data-object key layout.
///
/// - v1: `{prefix}/{pipeline}/{table}/wm-{hex(watermark)}/{content}.{ext}`.
///   The watermark segment grows with the position (a MySQL GTID set), past
///   backend name limits. Still read: manifests record every key verbatim.
/// - v2: `.../wm2-{digest}/{content}.{ext}`, the digest a bounded identity of
///   the canonical watermark ([`watermark_key_digest`]). A v2 key can never
///   equal a v1 key (`wm2-` vs `wm-`).
pub const KEY_LAYOUT_VERSION: u16 = 2;

/// The v2 watermark segment tag.
pub const WATERMARK_SEGMENT_V2: &str = "wm2-";

/// Domain-separation tag of the watermark key digest.
const WATERMARK_KEY_DOMAIN: &[u8] = b"deltaforge:s3:watermark-key";

/// The longest path segment (as stored, percent-encoded) a key may have:
/// below the 255-byte file-name limit of file-backed stores (RustFS, local
/// file systems) with margin.
pub const MAX_KEY_SEGMENT_BYTES: usize = 200;

/// The longest object key: below the 1024-byte S3 key limit with margin.
pub const MAX_OBJECT_KEY_BYTES: usize = 900;

/// The bounded key identity of a durable watermark: lowercase hex SHA-256
/// over the length-prefixed domain tag, key layout version, position kind and
/// canonical position bytes. Same logical position, same digest (restart,
/// retry, replay); the kind separates position types.
pub fn watermark_key_digest(id: &deltaforge_core::WatermarkIdentity) -> String {
    let mut h = Sha256::new();
    for part in [
        WATERMARK_KEY_DOMAIN,
        &KEY_LAYOUT_VERSION.to_be_bytes()[..],
        id.kind.as_bytes(),
        id.canonical.as_slice(),
    ] {
        h.update((part.len() as u64).to_be_bytes());
        h.update(part);
    }
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

/// The watermark digest a v2 data-object key carries (its second-to-last
/// segment), or `None` for any other key (v1 `wm-`, compacted, manifest).
pub fn key_watermark_digest(key: &str) -> Option<&str> {
    let mut segs = key.rsplit('/');
    segs.next()?;
    segs.next()?.strip_prefix(WATERMARK_SEGMENT_V2)
}

/// Whether `key` is an uncompacted data object (either layout).
pub fn is_batch_object_key(key: &str) -> bool {
    let mut segs = key.rsplit('/');
    segs.next();
    segs.next().is_some_and(|s| {
        s.starts_with("wm-") || s.starts_with(WATERMARK_SEGMENT_V2)
    })
}

/// Domain-separation tag of the relation key digest.
const RELATION_KEY_DOMAIN: &[u8] = b"deltaforge:s3:relation-key";

/// The tag of a digest relation segment. A readable segment never contains
/// `-` (its escaping turns `-` into `_2d`), so the two forms never collide.
pub const RELATION_DIGEST_PREFIX: &str = "rel-";

/// The object-key segment of a relation: its readable
/// [`key_segment`](deltaforge_core::SourceRelationIdentity::key_segment)
/// when that fits [`MAX_KEY_SEGMENT_BYTES`], otherwise `rel-` and the
/// lowercase hex SHA-256 of the length-prefixed domain tag and the
/// relation's canonical identity (68 bytes). Never truncated; the manifest
/// keeps the readable identity either way.
pub fn relation_key_segment(
    relation: &deltaforge_core::SourceRelationIdentity,
) -> String {
    let readable = relation.key_segment();
    if readable.len() <= MAX_KEY_SEGMENT_BYTES {
        return readable;
    }
    let mut h = Sha256::new();
    for part in [RELATION_KEY_DOMAIN, relation.canonical().as_bytes()] {
        h.update((part.len() as u64).to_be_bytes());
        h.update(part);
    }
    let hex: String = h.finalize().iter().map(|b| format!("{b:02x}")).collect();
    format!("{RELATION_DIGEST_PREFIX}{hex}")
}

/// The object-key segment of a manifest table: the relation segment of a
/// canonical relation identity; a table recorded before relation
/// identities (`namespace.table`) is its own segment, as it was keyed.
pub fn table_key_segment(table: &str) -> String {
    match deltaforge_core::SourceRelationIdentity::from_canonical(table) {
        Ok(relation) => relation_key_segment(&relation),
        Err(_) => table.to_string(),
    }
}

/// Check a stored key against [`MAX_KEY_SEGMENT_BYTES`] and
/// [`MAX_OBJECT_KEY_BYTES`]: the offending part when it exceeds them.
pub fn check_key_bounds(key: &str) -> Result<(), String> {
    if key.len() > MAX_OBJECT_KEY_BYTES {
        return Err(format!(
            "key is {} bytes (limit {MAX_OBJECT_KEY_BYTES})",
            key.len()
        ));
    }
    if let Some(seg) = key.split('/').find(|s| s.len() > MAX_KEY_SEGMENT_BYTES)
    {
        return Err(format!(
            "segment of {} bytes (limit {MAX_KEY_SEGMENT_BYTES}): {}...",
            seg.len(),
            &seg[..seg.floor_char_boundary(40)]
        ));
    }
    Ok(())
}

/// Whether every data-object key of `pipeline` under `prefix` stays within
/// [`check_key_bounds`] for any table whose encoded name fits a segment: the
/// longest such key, checked once when the sink is built.
pub fn check_prefix_capacity(
    prefix: &str,
    pipeline: &str,
) -> Result<(), String> {
    let digest = "0".repeat(64);
    let worst = object_store::path::Path::from_iter(
        prefix
            .split('/')
            .filter(|p| !p.is_empty())
            .map(str::to_string)
            .chain([
                pipeline.to_string(),
                "t".repeat(MAX_KEY_SEGMENT_BYTES),
                format!("{WATERMARK_SEGMENT_V2}{digest}"),
                format!("{digest}.parquet"),
            ]),
    );
    check_key_bounds(worst.as_ref())
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltaforge_core::SourceRelationIdentity;

    fn relation(
        source: &str,
        db: &str,
        ns: &str,
        table: &str,
    ) -> SourceRelationIdentity {
        SourceRelationIdentity {
            connector: "postgresql".into(),
            source: source.into(),
            database: db.into(),
            namespace: ns.into(),
            table: table.into(),
        }
    }

    /// The longest identifiers sources allow (MySQL: 64 characters of up to
    /// 3 bytes; PostgreSQL: 63 bytes), escaped, never exceed a segment:
    /// they take the digest form. A complete key with the longest pipeline
    /// a prefix admits stays within both key limits.
    #[test]
    fn the_longest_relations_keep_bounded_keys() {
        let mysql_max = "€".repeat(64); // 192 bytes, 576 escaped
        let pg_max = "é".repeat(31); // 62 bytes
        let source = "s".repeat(255);
        for rel in [
            relation(&source, &mysql_max, &mysql_max, &mysql_max),
            relation(&source, &pg_max, &pg_max, &pg_max),
            relation(&source, "d", "n", &"t".repeat(64)),
        ] {
            let seg = relation_key_segment(&rel);
            assert!(seg.len() <= MAX_KEY_SEGMENT_BYTES, "{seg}");
            let pipeline = "p".repeat(MAX_KEY_SEGMENT_BYTES);
            check_prefix_capacity("a/b", &pipeline).unwrap();
            let key = object_store::path::Path::from_iter([
                "a".to_string(),
                "b".to_string(),
                pipeline,
                seg,
                format!("{WATERMARK_SEGMENT_V2}{}", "0".repeat(64)),
                format!("{}.parquet", "0".repeat(64)),
            ]);
            check_key_bounds(key.as_ref()).unwrap();
        }
    }

    #[test]
    fn a_relation_segment_is_readable_until_the_limit_then_a_digest() {
        // `d.n.` + table: the readable form is 4 + table bytes.
        let fits =
            relation("s", "d", "n", &"t".repeat(MAX_KEY_SEGMENT_BYTES - 4));
        assert_eq!(relation_key_segment(&fits), fits.key_segment());
        let over =
            relation("s", "d", "n", &"t".repeat(MAX_KEY_SEGMENT_BYTES - 3));
        let seg = relation_key_segment(&over);
        assert!(
            seg.starts_with(RELATION_DIGEST_PREFIX) && seg.len() == 68,
            "{seg}"
        );
        // Deterministic, and a function of the whole identity.
        assert_eq!(relation_key_segment(&over), seg);
        let other_source =
            relation("s2", "d", "n", &"t".repeat(MAX_KEY_SEGMENT_BYTES - 3));
        assert_ne!(relation_key_segment(&other_source), seg);
        // A table literally named like a digest stays readable and distinct.
        let named = relation("s", "d", "n", &seg);
        assert!(
            !relation_key_segment(&named).starts_with(RELATION_DIGEST_PREFIX)
        );
        // The manifest's canonical identity maps to the same segment.
        assert_eq!(table_key_segment(&over.canonical()), seg);
        assert_eq!(table_key_segment("shop.orders"), "shop.orders");
    }

    /// Pinned (computed independently: SHA-256 over the u64-BE
    /// length-prefixed domain tag and canonical identity).
    #[test]
    fn the_relation_digest_is_pinned() {
        let rel = relation("p", "d", "n", &"t".repeat(300));
        assert_eq!(
            relation_key_segment(&rel),
            "rel-c8c7b2c20188e915d4f89ea2ea582b4ff0f84aa1b1c13c5f52b29b0c61c2ec65"
        );
    }

    fn wm(
        kind: &'static str,
        canonical: &[u8],
    ) -> deltaforge_core::WatermarkIdentity {
        deltaforge_core::WatermarkIdentity {
            kind,
            canonical: canonical.to_vec(),
            summary: String::new(),
        }
    }

    #[test]
    fn watermark_digests_are_bounded_stable_and_domain_separated() {
        let a = watermark_key_digest(&wm("mysql_gtid", b"x"));
        assert_eq!(a, watermark_key_digest(&wm("mysql_gtid", b"x")));
        assert_eq!(a.len(), 64);
        // A position type and its bytes are separate inputs.
        assert_ne!(a, watermark_key_digest(&wm("pg_lsn", b"x")));
        assert_ne!(
            watermark_key_digest(&wm("mysql_gtidx", b"")),
            watermark_key_digest(&wm("mysql_gtid", b"x"))
        );
        // Bounded however large the position.
        let huge = vec![b'u'; 1 << 20];
        assert_eq!(watermark_key_digest(&wm("mysql_gtid", &huge)).len(), 64);
    }

    /// The digest is part of every stored key, so it is pinned: a key a
    /// release wrote must be recomputed identically by the next release
    /// (restart and replay across an upgrade). The vector was computed
    /// independently: SHA-256 over each part as 8-byte big-endian length then
    /// bytes, for the domain tag, the layout version (u16 BE 2), the kind and
    /// the canonical bytes.
    #[test]
    fn the_watermark_digest_is_pinned() {
        assert_eq!(
            watermark_key_digest(&wm("mysql_gtid", br#"{"pos":1}"#)),
            "4a5b1609339d431ef9eeb80a784035e34580c6d9b7591eea5219dc7697c52902"
        );
    }

    #[test]
    fn key_layouts_are_told_apart() {
        let d = "ab".repeat(32);
        let v2 = format!("p/pipe/orders/wm2-{d}/c.parquet");
        assert_eq!(key_watermark_digest(&v2), Some(d.as_str()));
        assert!(is_batch_object_key(&v2));
        let v1 = "p/pipe/orders/wm-0a0b/c.parquet";
        assert_eq!(key_watermark_digest(v1), None);
        assert!(is_batch_object_key(v1));
        // A table named like the tag is not the watermark segment.
        let t = "p/pipe/wm2-orders/wm-0a/c.jsonl";
        assert_eq!(key_watermark_digest(t), None);
        assert!(!is_batch_object_key("p/pipe/orders/compacted/h.parquet"));
        assert!(!is_batch_object_key("p/pipe/_manifest/entries/1-h.json"));
    }

    #[test]
    fn a_prefix_leaves_room_for_the_longest_key_or_is_refused() {
        assert!(check_prefix_capacity("lake/cdc", "orders-pipeline").is_ok());
        // A prefix segment, or a pipeline, longer than a segment.
        let seg = "s".repeat(MAX_KEY_SEGMENT_BYTES + 1);
        assert!(check_prefix_capacity(&seg, "p").is_err());
        assert!(check_prefix_capacity("lake", &seg).is_err());
        // Short segments that together leave no room for a longest key.
        let many = vec!["s".repeat(MAX_KEY_SEGMENT_BYTES); 3].join("/");
        assert!(check_prefix_capacity(&many, "p").is_err());
        // The largest prefix that still fits is accepted.
        let fits = "s".repeat(MAX_KEY_SEGMENT_BYTES);
        assert!(check_prefix_capacity(&fits, "p").is_ok());
    }

    #[test]
    fn key_bounds_are_enforced_per_segment_and_overall() {
        assert!(check_key_bounds("a/b/c").is_ok());
        let long_seg = format!("a/{}/c", "s".repeat(MAX_KEY_SEGMENT_BYTES + 1));
        assert!(check_key_bounds(&long_seg).is_err());
        let ok_seg = format!("a/{}/c", "s".repeat(MAX_KEY_SEGMENT_BYTES));
        assert!(check_key_bounds(&ok_seg).is_ok());
        let long_key = vec!["s".repeat(150); 7].join("/");
        assert!(long_key.len() > MAX_OBJECT_KEY_BYTES);
        assert!(check_key_bounds(&long_key).is_err());
    }

    fn dom() -> EncodingDomain {
        EncodingDomain::new("parquet", 1, "schema-abc", "none", "table", 1)
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
        let parquet = content_hash(
            bytes,
            &EncodingDomain::new("parquet", 1, "s", "none", "table", 1),
        );
        let jsonl = content_hash(
            bytes,
            &EncodingDomain::new("jsonl", 1, "s", "none", "table", 1),
        );
        let bumped = content_hash(
            bytes,
            &EncodingDomain::new("parquet", 2, "s", "none", "table", 1),
        );
        let other_schema = content_hash(
            bytes,
            &EncodingDomain::new("parquet", 1, "s2", "none", "table", 1),
        );
        assert_ne!(parquet, jsonl);
        assert_ne!(parquet, bumped);
        assert_ne!(parquet, other_schema);
    }

    #[test]
    fn domain_field_boundaries_are_unambiguous() {
        // Length-prefixing means these two do not collide even though the naive
        // concatenation of their fields would.
        let a = content_hash(
            b"x",
            &EncodingDomain::new("ab", 1, "c", "none", "table", 1),
        );
        let b = content_hash(
            b"x",
            &EncodingDomain::new("a", 1, "bc", "none", "table", 1),
        );
        assert_ne!(a, b);
    }
}
