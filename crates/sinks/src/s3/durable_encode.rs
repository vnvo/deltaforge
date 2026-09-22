//! Deterministic, byte-reproducible in-memory batch encoders for the durable S3
//! path (P0.4).
//!
//! The durable path must upload a batch as a single immutable object and address
//! it by a content hash, so encoding the *same* events must yield the *same*
//! bytes on every retry. That rules out any encoder entropy: wall-clock
//! metadata, random file/metadata ids, or ordering that varies run to run.
//!
//! - **Parquet**: written with a fixed `created_by` so bytes do not depend on the
//!   parquet crate version (the version is carried in the `EncodingDomain`
//!   instead), and a pinned compression codec. parquet-rs embeds no write
//!   timestamps, and statistics are data-derived, so output is reproducible.
//! - **JSON Lines**: `serde_json` serializes object keys in sorted (BTreeMap)
//!   order here (no `preserve_order` feature in the tree), so identical events
//!   serialize identically.
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

fn map_compression(c: Compression) -> PqCompression {
    match c {
        Compression::None => PqCompression::UNCOMPRESSED,
        Compression::Snappy => PqCompression::SNAPPY,
        Compression::Gzip => PqCompression::GZIP(Default::default()),
        Compression::Zstd => PqCompression::ZSTD(Default::default()),
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
        .set_compression(map_compression(compression))
        .build();
    let mut buf: Vec<u8> = Vec::new();
    let mut writer =
        ArrowWriter::try_new(&mut buf, schema.clone(), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(Bytes::from(buf))
}

/// Encode `events` to a byte-reproducible JSON Lines object (one JSON object per
/// line). Compression, when added, must itself be deterministic; callers pass
/// pre-agreed settings.
pub fn encode_jsonl(events: &[Event]) -> Result<Bytes> {
    let mut buf: Vec<u8> = Vec::new();
    for e in events {
        serde_json::to_writer(&mut buf, e)?;
        buf.push(b'\n');
    }
    Ok(Bytes::from(buf))
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
            checkpoint: None,
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
    fn parquet_reproducible_across_all_compressions() {
        let s = schema();
        let evs = events();
        for c in [
            Compression::None,
            Compression::Snappy,
            Compression::Gzip,
            Compression::Zstd,
        ] {
            let a = encode_parquet(&s, &evs, c).unwrap();
            let b = encode_parquet(&s, &evs, c).unwrap();
            assert_eq!(a, b, "Parquet not reproducible for {c:?}");
        }
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
}
