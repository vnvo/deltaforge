//! Deterministic, byte-reproducible in-memory batch encoders for the durable S3
//! path (P0.4).
//!
//! The durable path must upload a batch as a single immutable object and address
//! it by a content hash, so encoding the *same* events must yield the *same*
//! bytes on every retry. That rules out any encoder entropy: wall-clock
//! metadata, random file/metadata ids, or ordering that varies run to run.
//!
//! Reproducibility must hold across *differently built binaries*, not just
//! across retries in one process: Cargo feature unification is additive, so a
//! dependency elsewhere in the final binary can flip a shared library's
//! behavior. This module never relies on such build-dependent behavior.
//!
//! - **Parquet**: written with a fixed `created_by` so bytes do not depend on the
//!   parquet crate version (the version is carried in the `EncodingDomain`
//!   instead). parquet-rs embeds no write timestamps and statistics are
//!   data-derived, so output is reproducible - *except* Gzip, whose `flate2`
//!   backend is chosen by feature unification; `map_compression` therefore
//!   rejects Gzip and permits only None, Snappy and Zstd.
//! - **JSON Lines**: keys are written in recursively sorted order by
//!   `write_canonical_json`, so the bytes never depend on `serde_json`'s map
//!   implementation or the `preserve_order` feature (which unification can
//!   enable). Array order is preserved.
//!
//! Staged scaffolding: consumed by the durable sink in a later P0.4 commit.
#![allow(dead_code)]

use std::sync::Arc;

use anyhow::Result;
use arrow_schema::Schema;
use bytes::Bytes;
use deltaforge_core::Event;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression as PqCompression;
use parquet::file::properties::WriterProperties;

use super::encoder::events_to_record_batch;
use super::file_format::Compression;

/// Fixed Parquet `created_by`. Pinning it means the bytes of an unchanged input
/// do not shift when the parquet crate is upgraded; that version dimension lives
/// in the `EncodingDomain` (see `keys`) instead.
const PARQUET_CREATED_BY: &str = "deltaforge-s3-durable";

/// Encoder version for Parquet output. Bump if the byte layout of an unchanged
/// input intentionally changes; it feeds the `EncodingDomain`.
pub const PARQUET_ENCODER_VERSION: u16 = 1;
/// Encoder version for JSON Lines output.
pub const JSONL_ENCODER_VERSION: u16 = 1;

/// Map a durable-path compression codec to parquet's, rejecting any codec whose
/// compressed bytes are not stable across build configurations. `Gzip` is
/// rejected: parquet-rs compresses it with `flate2`, whose backend (pure-Rust
/// miniz_oxide vs C zlib) is selected by Cargo feature unification and is
/// additive - a single dependency anywhere in the final binary's graph enabling
/// `any_zlib` silently changes the gzip byte stream. That would content-address
/// identical events to different keys across differently built binaries. `None`,
/// `Snappy` and `Zstd` produce byte-identical output regardless of unification.
fn map_compression(c: Compression) -> Result<PqCompression> {
    match c {
        Compression::None => Ok(PqCompression::UNCOMPRESSED),
        Compression::Snappy => Ok(PqCompression::SNAPPY),
        Compression::Zstd => Ok(PqCompression::ZSTD(Default::default())),
        Compression::Gzip => Err(anyhow::anyhow!(
            "Gzip is not permitted on the durable S3 path: its flate2 backend \
             is chosen by Cargo feature unification, so its bytes are not \
             reproducible across binaries; use None, Snappy or Zstd"
        )),
    }
}

/// Encode `events` to a self-contained, byte-reproducible Parquet object.
pub fn encode_parquet(
    schema: &Arc<Schema>,
    events: &[Event],
    compression: Compression,
) -> Result<Bytes> {
    let batch = events_to_record_batch(schema, events)?;
    let props = WriterProperties::builder()
        .set_created_by(PARQUET_CREATED_BY.to_string())
        .set_compression(map_compression(compression)?)
        .build();
    let mut buf: Vec<u8> = Vec::new();
    let mut writer =
        ArrowWriter::try_new(&mut buf, schema.clone(), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(Bytes::from(buf))
}

/// Encode `events` to a byte-reproducible JSON Lines object (one JSON object per
/// line). Every object is emitted with its keys in sorted order, recursively, so
/// the bytes never depend on `serde_json`'s map implementation or the
/// `preserve_order` feature (which Cargo feature unification can silently enable
/// in some builds but not others). Array order is preserved. Without this, the
/// same encoder version would content-address identical events to different keys
/// across differently built binaries.
pub fn encode_jsonl(events: &[Event]) -> Result<Bytes> {
    let mut buf: Vec<u8> = Vec::new();
    for e in events {
        let v = serde_json::to_value(e)?;
        write_canonical_json(&mut buf, &v);
        buf.push(b'\n');
    }
    Ok(Bytes::from(buf))
}

/// Write `v` as compact JSON with object keys sorted lexicographically at every
/// level (arrays keep their order). Matches `serde_json`'s compact spacing, so
/// the only difference from `to_writer` is the guaranteed key order.
fn write_canonical_json(buf: &mut Vec<u8>, v: &serde_json::Value) {
    use serde_json::Value;
    match v {
        Value::Object(map) => {
            buf.push(b'{');
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort_unstable();
            for (i, k) in keys.iter().enumerate() {
                if i > 0 {
                    buf.push(b',');
                }
                // Encode the key string with correct quoting/escaping.
                serde_json::to_writer(&mut *buf, k)
                    .expect("string key serializes");
                buf.push(b':');
                write_canonical_json(buf, &map[*k]);
            }
            buf.push(b'}');
        }
        Value::Array(arr) => {
            buf.push(b'[');
            for (i, item) in arr.iter().enumerate() {
                if i > 0 {
                    buf.push(b',');
                }
                write_canonical_json(buf, item);
            }
            buf.push(b']');
        }
        // Scalars (null/bool/number/string) have a single canonical compact form.
        other => {
            serde_json::to_writer(&mut *buf, other).expect("scalar serializes")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field};
    use deltaforge_core::{Op, SourceInfo, SourcePosition, Transaction};
    use serde_json::{Value, json};

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("after_id", DataType::Int64, true),
            Field::new("after_name", DataType::Utf8, true),
        ]))
    }

    // ── Golden fixture: a fixed, representative batch exercising nulls, binary,
    // nested lists, booleans and multiple rows. Its encoded bytes are pinned
    // below; see the golden test for the change protocol.

    fn golden_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("op", DataType::Utf8, false),
            Field::new("source_table", DataType::Utf8, false),
            Field::new("after_id", DataType::Int64, true),
            Field::new("after_name", DataType::Utf8, true),
            Field::new("after_blob", DataType::Binary, true),
            Field::new(
                "after_tags",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Utf8,
                    true,
                ))),
                true,
            ),
            Field::new("after_active", DataType::Boolean, true),
        ]))
    }

    fn golden_event(op: Op, after: Value) -> Event {
        Event {
            before: None,
            after: Some(after),
            source: SourceInfo {
                version: "1".into(),
                connector: "mysql".into(),
                name: "golden".into(),
                ts_ms: 1_700_000_000_000,
                db: "shop".into(),
                schema: None,
                table: "orders".into(),
                snapshot: Some("false".into()),
                position: SourcePosition {
                    file: Some("mysql-bin.000007".into()),
                    pos: Some(42),
                    ..Default::default()
                },
            },
            op,
            ts_ms: 1_700_000_000_000,
            transaction: Some(Transaction {
                id: "tx-golden".into(),
                total_order: None,
                data_collection_order: None,
            }),
            event_id: Some(deltaforge_core::EventId::mysql_row_server(
                1, "orders", 1, 0,
            )),
            tenant_id: None,
            schema_version: Some("v1".into()),
            schema_sequence: None,
            ddl: None,
            trace_id: None,
            tags: None,
            synthetic: None,
            routing: None,
            tx_end: false,
            boundary: None,
            size_bytes: 0,

            received_at_ms: 1_700_000_000_000,
        }
    }

    fn golden_events() -> Vec<Event> {
        vec![
            golden_event(
                Op::Create,
                json!({
                    "id": 1,
                    "name": "alpha",
                    // binary via the {"_base64": ...} convention: bytes "hi"
                    "blob": {"_base64": "aGk="},
                    "tags": ["x", "y"],
                    "active": true
                }),
            ),
            golden_event(
                Op::Update,
                json!({
                    "id": 2,
                    // name/blob absent -> nulls
                    "tags": [],
                    "active": false
                }),
            ),
            golden_event(
                Op::Delete,
                json!({
                    "id": 3,
                    "name": "gamma",
                    // bytes 0x00 0x01 0xff
                    "blob": {"_base64": "AAH/"},
                    "tags": ["z"],
                    "active": null
                }),
            ),
        ]
    }

    fn sha256_hex(bytes: &[u8]) -> String {
        use sha2::{Digest, Sha256};
        let d = Sha256::new().chain_update(bytes).finalize();
        d.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Pinned golden vectors. A "fixed created_by" does not by itself guarantee
    /// byte stability: a parquet/compression library upgrade, a metadata-ordering
    /// or dictionary-encoding change, or an encoder tweak can all shift the bytes
    /// silently, and the `EncodingDomain` version only prevents *collisions* if
    /// someone remembers to bump it. These vectors make any such drift a loud
    /// test failure.
    ///
    /// If this test fails after an intentional encoder or dependency change:
    /// 1. bump the relevant `*_ENCODER_VERSION` (so new bytes cannot collide with
    ///    previously written content at the same content-addressed key), then
    /// 2. re-run `print_golden_vectors` (`--ignored --nocapture`) and paste the
    ///    new length + SHA-256 below.
    ///
    /// If it fails WITHOUT an intentional change, a dependency silently altered
    /// the output: investigate before touching these constants.
    #[test]
    fn golden_vectors_are_pinned() {
        let s = golden_schema();
        let evs = golden_events();
        let expected_parquet = [
            (
                Compression::None,
                2199,
                "de8fd743863a472c3f51ed3f3091a2654c240978c5ac2fd3d385fc7d7d9ec637",
            ),
            (
                Compression::Snappy,
                2223,
                "ce9023b7584c4f25ec929b5d574b68ca83b205fea95737b7ed2ef3f21db0c7ec",
            ),
            // Gzip is intentionally absent: it is rejected on the durable path
            // (see map_compression) because its flate2 backend, and thus its
            // bytes, depend on Cargo feature unification.
            (
                Compression::Zstd,
                2317,
                "01d8ec4a9a2a40029ebc35db1e69a540270bd043a687a058cff9c5d08822e541",
            ),
        ];
        for (c, len, sha) in expected_parquet {
            let bytes = encode_parquet(&s, &evs, c).unwrap();
            assert_eq!(
                bytes.len(),
                len,
                "Parquet {c:?} byte length drifted; see the change protocol"
            );
            assert_eq!(
                sha256_hex(&bytes),
                sha,
                "Parquet {c:?} bytes drifted; see the change protocol"
            );
        }
        // Canonical (recursively sorted keys): identical under isolated sinks
        // tests and the unified workspace graph (serde_json/preserve_order on).
        let j = encode_jsonl(&evs).unwrap();
        assert_eq!(j.len(), 1200, "JSONL byte length drifted");
        assert_eq!(
            sha256_hex(&j),
            "803f679b0fa3061165a83a28374a40b794a1d6713a4917b2bb77f231377e48de",
            "JSONL bytes drifted; see the change protocol"
        );
    }

    // Capture helper: prints the golden hashes so they can be re-pinned.
    #[test]
    #[ignore = "capture-only: run to (re)generate golden vectors"]
    fn print_golden_vectors() {
        let s = golden_schema();
        let evs = golden_events();
        for c in [Compression::None, Compression::Snappy, Compression::Zstd] {
            let bytes = encode_parquet(&s, &evs, c).unwrap();
            println!("PARQUET {c:?} {} {}", bytes.len(), sha256_hex(&bytes));
        }
        let j = encode_jsonl(&evs).unwrap();
        println!("JSONL {} {}", j.len(), sha256_hex(&j));
    }

    fn event(id: i64, after: Option<Value>) -> Event {
        Event {
            before: None,
            after,
            source: SourceInfo {
                version: "1".into(),
                connector: "mysql".into(),
                name: "test".into(),
                ts_ms: 0,
                db: "shop".into(),
                schema: None,
                table: "orders".into(),
                snapshot: Some("false".into()),
                position: SourcePosition {
                    file: Some("mysql-bin.000001".into()),
                    pos: Some(id as u64),
                    ..Default::default()
                },
            },
            op: Op::Create,
            ts_ms: 0,
            transaction: Some(Transaction {
                id: "tx-1".into(),
                total_order: None,
                data_collection_order: None,
            }),
            event_id: Some(deltaforge_core::EventId::mysql_row_server(
                1, "orders", 1, id as u32,
            )),
            tenant_id: None,
            schema_version: Some("v1".into()),
            schema_sequence: None,
            ddl: None,
            trace_id: None,
            tags: None,
            synthetic: None,
            routing: None,
            tx_end: false,
            boundary: None,
            size_bytes: 0,

            received_at_ms: 0,
        }
    }

    fn events() -> Vec<Event> {
        vec![
            event(1, Some(json!({"id": 1, "name": "a"}))),
            event(2, Some(json!({"id": 2, "name": "b"}))),
            event(3, Some(json!({"id": 3, "name": "c"}))),
        ]
    }

    #[test]
    fn parquet_encoding_is_byte_reproducible() {
        let s = schema();
        let evs = events();
        let a = encode_parquet(&s, &evs, Compression::Snappy).unwrap();
        let b = encode_parquet(&s, &evs, Compression::Snappy).unwrap();
        assert_eq!(a, b, "identical events must encode to identical Parquet");
        assert!(!a.is_empty());
    }

    #[test]
    fn parquet_reproducible_across_permitted_compressions() {
        let s = schema();
        let evs = events();
        for c in [Compression::None, Compression::Snappy, Compression::Zstd] {
            let a = encode_parquet(&s, &evs, c).unwrap();
            let b = encode_parquet(&s, &evs, c).unwrap();
            assert_eq!(a, b, "Parquet not reproducible for {c:?}");
        }
    }

    #[test]
    fn durable_parquet_rejects_gzip() {
        // Gzip's bytes are not stable across feature unification, so the durable
        // path must refuse it rather than write a non-reproducible object.
        let s = schema();
        let evs = events();
        let err = encode_parquet(&s, &evs, Compression::Gzip)
            .expect_err("Gzip must be rejected on the durable path");
        assert!(err.to_string().contains("Gzip is not permitted"));
    }

    #[test]
    fn jsonl_encoding_is_byte_reproducible_and_key_order_stable() {
        // The two events carry the same logical fields in different JSON key
        // orders; sorted-key serialization must make their lines identical.
        let e1 = event(1, Some(json!({"id": 1, "name": "a"})));
        let e2 = event(1, Some(json!({"name": "a", "id": 1})));
        let a = encode_jsonl(std::slice::from_ref(&e1)).unwrap();
        let b = encode_jsonl(std::slice::from_ref(&e2)).unwrap();
        assert_eq!(a, b, "JSON key order must not affect the bytes");

        let evs = events();
        assert_eq!(
            encode_jsonl(&evs).unwrap(),
            encode_jsonl(&evs).unwrap(),
            "identical events must encode to identical JSONL"
        );
    }

    #[test]
    fn canonical_json_sorts_nested_keys_regardless_of_insertion_order() {
        // Build the SAME nested object with keys inserted in two different
        // orders, using serde_json::Map directly so the test exercises whatever
        // map backing is active (BTreeMap or, under preserve_order, IndexMap).
        // Canonicalization must collapse both to identical, sorted bytes.
        use serde_json::{Map, Value};
        let mut a = Map::new();
        a.insert("zebra".into(), Value::from(1));
        a.insert("alpha".into(), Value::from(2));
        let mut a_inner = Map::new();
        a_inner.insert("y".into(), Value::from("yy"));
        a_inner.insert("x".into(), Value::from("xx"));
        a.insert("nested".into(), Value::Object(a_inner));
        a.insert("list".into(), Value::from(vec![3, 2, 1]));

        let mut b = Map::new();
        b.insert("list".into(), Value::from(vec![3, 2, 1]));
        let mut b_inner = Map::new();
        b_inner.insert("x".into(), Value::from("xx"));
        b_inner.insert("y".into(), Value::from("yy"));
        b.insert("nested".into(), Value::Object(b_inner));
        b.insert("alpha".into(), Value::from(2));
        b.insert("zebra".into(), Value::from(1));

        let mut ba = Vec::new();
        write_canonical_json(&mut ba, &Value::Object(a));
        let mut bb = Vec::new();
        write_canonical_json(&mut bb, &Value::Object(b));

        assert_eq!(ba, bb, "insertion order must not affect canonical bytes");
        // Keys sorted at every level; array order preserved (3,2,1 not sorted).
        assert_eq!(
            String::from_utf8(ba).unwrap(),
            r#"{"alpha":2,"list":[3,2,1],"nested":{"x":"xx","y":"yy"},"zebra":1}"#
        );
    }
}
