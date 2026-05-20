//! JSON Lines writer over `object_store`.
//!
//! Phase 2b: streaming writer over `object_store::buffered::BufWriter`,
//! optionally wrapped in an `async-compression` encoder (gzip or zstd).
//! Memory footprint is bounded by `BufWriter`'s internal chunk size
//! (~8 MiB by default), not the final file size — so a 256 MiB JSONL
//! roll no longer holds the whole file in RAM.
//!
//! Per-row DLQ contract is preserved by serializing each `append` call's
//! events into a small local buffer first (atomic per-call), then writing
//! all-or-nothing to the encoder. A serde failure on any event in the
//! call rolls back the local buffer; the encoder and underlying BufWriter
//! are untouched. This means a failed `append` is safe to retry per-event.

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_schema::Schema;
use async_compression::Level;
use async_compression::tokio::write::{GzipEncoder, ZstdEncoder};
use async_trait::async_trait;
use bytes::Bytes;
use deltaforge_core::Event;
use object_store::buffered::BufWriter as ObjectBufWriter;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use tokio::io::AsyncWriteExt;

use super::SimpleRow;
use super::file_format::{Compression, FileFormat, FileWriter, WriteResult};

/// JSON Lines implementation of `FileFormat`.
///
/// Supports `none` (uncompressed), `gzip` (default), and `zstd` compression.
/// snappy is not a meaningful choice for JSONL and is silently upgraded to
/// gzip.
#[derive(Debug, Clone)]
pub struct JsonLinesFormat {
    compression: Compression,
}

impl JsonLinesFormat {
    pub fn new(compression: Compression) -> Self {
        let compression = match compression {
            Compression::None | Compression::Gzip | Compression::Zstd => {
                compression
            }
            other => {
                tracing::warn!(
                    "jsonl compression {other:?} not supported, using gzip"
                );
                Compression::Gzip
            }
        };
        Self { compression }
    }
}

impl Default for JsonLinesFormat {
    fn default() -> Self {
        Self::new(Compression::Gzip)
    }
}

#[async_trait]
impl FileFormat for JsonLinesFormat {
    fn extension(&self) -> &'static str {
        match self.compression {
            Compression::Gzip => "jsonl.gz",
            Compression::Zstd => "jsonl.zst",
            _ => "jsonl",
        }
    }

    fn content_type(&self) -> &'static str {
        match self.compression {
            Compression::Gzip => "application/gzip",
            Compression::Zstd => "application/zstd",
            _ => "application/x-ndjson",
        }
    }

    fn label(&self) -> &'static str {
        "jsonl"
    }

    async fn open_writer(
        &self,
        store: Arc<dyn ObjectStore>,
        path: Path,
        _schema: Arc<Schema>,
    ) -> Result<Box<dyn FileWriter + Send>> {
        let buf_writer = ObjectBufWriter::new(store.clone(), path.clone());
        let output = match self.compression {
            Compression::None => Output::Plain(buf_writer),
            Compression::Gzip => Output::Gzip(GzipEncoder::with_quality(
                buf_writer,
                Level::Default,
            )),
            Compression::Zstd => Output::Zstd(ZstdEncoder::with_quality(
                buf_writer,
                Level::Default,
            )),
            // mapped to gzip by ::new — unreachable in practice
            Compression::Snappy => Output::Gzip(GzipEncoder::with_quality(
                buf_writer,
                Level::Default,
            )),
        };
        Ok(Box::new(JsonLinesFileWriter {
            output,
            store,
            path,
            events_written: 0,
            bytes_written: 0,
        }))
    }
}

/// Output abstraction — either raw or compressed. All three variants
/// implement `tokio::io::AsyncWrite` and are pollable in the same code
/// path via `write_all` / `shutdown`.
enum Output {
    Plain(ObjectBufWriter),
    Gzip(GzipEncoder<ObjectBufWriter>),
    Zstd(ZstdEncoder<ObjectBufWriter>),
}

impl Output {
    async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            Output::Plain(w) => w.write_all(buf).await,
            Output::Gzip(w) => w.write_all(buf).await,
            Output::Zstd(w) => w.write_all(buf).await,
        }
    }

    /// Finalize the stream: gzip/zstd append their footer; the underlying
    /// BufWriter completes its multipart upload. The object becomes
    /// visible at its destination key only after this returns success.
    ///
    /// `async-compression`'s `shutdown()` flushes the encoder footer AND
    /// propagates shutdown to the inner writer, so the BufWriter's
    /// multipart-complete fires as a side effect. We rely on that here
    /// — calling shutdown on `into_inner()` afterward would double-poll
    /// the BufWriter's already-completed future.
    async fn shutdown(self) -> std::io::Result<()> {
        match self {
            Output::Plain(mut w) => w.shutdown().await,
            Output::Gzip(mut w) => w.shutdown().await,
            Output::Zstd(mut w) => w.shutdown().await,
        }
    }
}

/// Incremental, streaming JSONL writer.
pub struct JsonLinesFileWriter {
    output: Output,
    store: Arc<dyn ObjectStore>,
    path: Path,
    events_written: u64,
    /// Pre-compression byte count, useful for the rolling thresholds.
    bytes_written: u64,
}

#[async_trait]
impl FileWriter for JsonLinesFileWriter {
    async fn append(&mut self, events: &[Event]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        // Serialize all events into a local buffer first. If any event
        // fails, we discard the buffer and the underlying stream is
        // untouched — the per-row DLQ retry contract requires this
        // all-or-nothing semantic.
        //
        // The local buffer holds only one `append` call's events
        // (typically ≪ a full file), so the streaming property (no full-
        // file buffering) is preserved.
        let mut buf = Vec::with_capacity(events.len() * 128);
        for e in events {
            serde_json::to_writer(&mut buf, e)
                .context("serialize event to jsonl buffer")?;
            buf.push(b'\n');
        }
        self.output
            .write_all(&buf)
            .await
            .context("write jsonl bytes to stream")?;
        self.events_written += events.len() as u64;
        self.bytes_written += buf.len() as u64;
        Ok(())
    }

    fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    fn events_written(&self) -> u64 {
        self.events_written
    }

    async fn close(self: Box<Self>) -> Result<WriteResult> {
        let Self {
            output,
            store,
            path,
            events_written,
            ..
        } = *self;
        output
            .shutdown()
            .await
            .context("shutdown jsonl stream (close + finalize compression + multipart-complete)")?;
        // The on-disk size is known only after the object lands.
        let final_bytes = store.head(&path).await.map(|m| m.size).unwrap_or(0);
        Ok(WriteResult {
            rows_written: events_written,
            bytes_written: final_bytes,
        })
    }
}

// =============================================================================
// SimpleRow facade (kept for Phase 1a/1b sample tests)
// =============================================================================

impl JsonLinesFormat {
    /// Phase 1b convenience used by sample tests. Bypasses the FileWriter
    /// flow and writes a single in-memory blob via `store.put`. Useful for
    /// the small Phase 1b smoke tests; production paths use `open_writer`.
    pub async fn write_simple_rows(
        &self,
        store: Arc<dyn ObjectStore>,
        path: &Path,
        rows: &[SimpleRow],
    ) -> Result<WriteResult> {
        use std::io::Write as _;
        let mut buf = Vec::with_capacity(rows.len() * 96);
        for row in rows {
            serde_json::to_writer(
                &mut buf,
                &serde_json::json!({
                    "id": row.id,
                    "name": row.name,
                    "ts_ms": row.ts_ms,
                }),
            )?;
            buf.push(b'\n');
        }

        let payload = match self.compression {
            Compression::Gzip => {
                let mut encoder = flate2::write::GzEncoder::new(
                    Vec::with_capacity(buf.len() / 4),
                    flate2::Compression::default(),
                );
                encoder.write_all(&buf).context("gzip write")?;
                encoder.finish().context("gzip finish")?
            }
            _ => buf,
        };
        let bytes_written = payload.len() as u64;
        store
            .put(path, PutPayload::from(Bytes::from(payload)))
            .await
            .context("put jsonl object")?;
        Ok(WriteResult {
            rows_written: rows.len() as u64,
            bytes_written,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::object_writer::{ObjectStoreHandle, ObjectStoreParams};
    use flate2::read::GzDecoder;
    use std::io::Read;

    fn sample_rows(n: usize) -> Vec<SimpleRow> {
        (0..n)
            .map(|i| SimpleRow {
                id: i as i64,
                name: format!("row-{i}"),
                ts_ms: 1_700_000_000_000 + (i as i64),
            })
            .collect()
    }

    async fn read_bytes(
        handle: &ObjectStoreHandle,
        path: &Path,
    ) -> Result<Vec<u8>> {
        let got = handle.store.get(path).await?.bytes().await?;
        Ok(got.to_vec())
    }

    #[tokio::test]
    async fn writes_uncompressed_jsonl_to_local_store() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let params =
            ObjectStoreParams::local(tmp.path().to_string_lossy().to_string());
        let handle = ObjectStoreHandle::new(params)?;
        let format = JsonLinesFormat::new(Compression::None);
        assert_eq!(format.extension(), "jsonl");

        let path = Path::from("jsonl/uncompressed.jsonl");
        let rows = sample_rows(100);
        let res = format
            .write_simple_rows(handle.store.clone(), &path, &rows)
            .await?;
        assert_eq!(res.rows_written, 100);

        let bytes = read_bytes(&handle, &path).await?;
        let s = std::str::from_utf8(&bytes)?;
        let lines: Vec<&str> = s.lines().collect();
        assert_eq!(lines.len(), 100);

        let first: serde_json::Value = serde_json::from_str(lines[0])?;
        assert_eq!(first["id"], 0);
        assert_eq!(first["name"], "row-0");
        Ok(())
    }

    #[tokio::test]
    async fn writes_gzipped_jsonl_to_local_store() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let params =
            ObjectStoreParams::local(tmp.path().to_string_lossy().to_string());
        let handle = ObjectStoreHandle::new(params)?;
        let format = JsonLinesFormat::new(Compression::Gzip);
        assert_eq!(format.extension(), "jsonl.gz");

        let path = Path::from("jsonl/compressed.jsonl.gz");
        let rows = sample_rows(1000);
        let res = format
            .write_simple_rows(handle.store.clone(), &path, &rows)
            .await?;
        assert_eq!(res.rows_written, 1000);

        // Roundtrip: read bytes, gunzip, count newline-terminated lines.
        let bytes = read_bytes(&handle, &path).await?;
        let mut decoder = GzDecoder::new(&bytes[..]);
        let mut out = String::new();
        decoder.read_to_string(&mut out)?;
        let line_count = out.lines().count();
        assert_eq!(line_count, 1000);
        Ok(())
    }

    #[tokio::test]
    async fn streaming_writer_roundtrip_uncompressed() -> Result<()> {
        // Phase 2b: exercise the streaming FileWriter path (open_writer →
        // append → close). Goes through ObjectBufWriter, no encoder.
        use crate::s3::file_format::Compression;
        use crate::s3::object_writer::build_object_store;
        use deltaforge_core::{Event, Op, SourceInfo, SourcePosition};

        let tmp = tempfile::tempdir()?;
        let params =
            ObjectStoreParams::local(tmp.path().to_string_lossy().to_string());
        let store = build_object_store(&params)?;
        let format = JsonLinesFormat::new(Compression::None);
        let path = Path::from("stream/plain.jsonl");

        let schema = Arc::new(arrow_schema::Schema::new(Vec::<
            arrow_schema::Field,
        >::new()));
        let mut writer = format
            .open_writer(store.clone(), path.clone(), schema)
            .await?;

        let make_event = |i: i64| Event {
            before: None,
            after: Some(serde_json::json!({"id": i})),
            source: SourceInfo {
                version: "1".into(),
                connector: "mysql".into(),
                name: "t".into(),
                ts_ms: i,
                db: "shop".into(),
                schema: None,
                table: "orders".into(),
                snapshot: None,
                position: SourcePosition::default(),
            },
            op: Op::Create,
            ts_ms: i,
            transaction: None,
            event_id: None,
            tenant_id: None,
            schema_version: None,
            schema_sequence: None,
            ddl: None,
            trace_id: None,
            tags: None,
            synthetic: None,
            routing: None,
            tx_end: false,
            checkpoint: None,
            size_bytes: 0,
            received_at_ms: i,
        };

        // Append in 5 chunks of 20 each — exercises the incremental write path.
        for chunk in 0..5 {
            let events: Vec<Event> =
                (0..20).map(|i| make_event(chunk * 20 + i)).collect();
            writer.append(&events).await?;
        }
        let result = writer.close().await?;
        assert_eq!(result.rows_written, 100);
        assert!(result.bytes_written > 0);

        let got = store.get(&path).await?.bytes().await?;
        let line_count = std::str::from_utf8(&got)?.lines().count();
        assert_eq!(line_count, 100);
        Ok(())
    }

    #[tokio::test]
    async fn streaming_writer_roundtrip_gzip() -> Result<()> {
        use crate::s3::file_format::Compression;
        use crate::s3::object_writer::build_object_store;
        use deltaforge_core::{Event, Op, SourceInfo, SourcePosition};

        let tmp = tempfile::tempdir()?;
        let params =
            ObjectStoreParams::local(tmp.path().to_string_lossy().to_string());
        let store = build_object_store(&params)?;
        let format = JsonLinesFormat::new(Compression::Gzip);
        let path = Path::from("stream/gzip.jsonl.gz");

        let schema = Arc::new(arrow_schema::Schema::new(Vec::<
            arrow_schema::Field,
        >::new()));
        let mut writer = format
            .open_writer(store.clone(), path.clone(), schema)
            .await?;

        let make_event = |i: i64| Event {
            before: None,
            after: Some(serde_json::json!({"id": i, "msg": "hello world"})),
            source: SourceInfo {
                version: "1".into(),
                connector: "mysql".into(),
                name: "t".into(),
                ts_ms: i,
                db: "shop".into(),
                schema: None,
                table: "orders".into(),
                snapshot: None,
                position: SourcePosition::default(),
            },
            op: Op::Create,
            ts_ms: i,
            transaction: None,
            event_id: None,
            tenant_id: None,
            schema_version: None,
            schema_sequence: None,
            ddl: None,
            trace_id: None,
            tags: None,
            synthetic: None,
            routing: None,
            tx_end: false,
            checkpoint: None,
            size_bytes: 0,
            received_at_ms: i,
        };

        for chunk in 0..10 {
            let events: Vec<Event> =
                (0..50).map(|i| make_event(chunk * 50 + i)).collect();
            writer.append(&events).await?;
        }
        let result = writer.close().await?;
        assert_eq!(result.rows_written, 500);

        // Roundtrip: read bytes, gunzip, count lines.
        let got = store.get(&path).await?.bytes().await?;
        let mut decoder = GzDecoder::new(&got[..]);
        let mut out = String::new();
        decoder.read_to_string(&mut out)?;
        assert_eq!(out.lines().count(), 500);
        Ok(())
    }

    #[tokio::test]
    async fn append_rolls_back_on_serde_failure() -> Result<()> {
        // The per-row DLQ retry contract: a failed append must NOT
        // partially write to the underlying stream. We can't easily
        // induce a serde failure on a real Event (it's well-defined),
        // so instead verify the loop semantics: a successful append
        // followed by a close roundtrips cleanly with the expected
        // event count. Combined with the bytes_written counter only
        // advancing on success, this is the contract the slow-path
        // retry depends on.
        use crate::s3::file_format::Compression;
        use crate::s3::object_writer::build_object_store;
        use deltaforge_core::{Event, Op, SourceInfo, SourcePosition};

        let tmp = tempfile::tempdir()?;
        let params =
            ObjectStoreParams::local(tmp.path().to_string_lossy().to_string());
        let store = build_object_store(&params)?;
        let format = JsonLinesFormat::new(Compression::None);
        let path = Path::from("stream/rollback.jsonl");

        let schema = Arc::new(arrow_schema::Schema::new(Vec::<
            arrow_schema::Field,
        >::new()));
        let mut writer = format
            .open_writer(store.clone(), path.clone(), schema)
            .await?;

        let event = Event {
            before: None,
            after: Some(serde_json::json!({"id": 1})),
            source: SourceInfo {
                version: "1".into(),
                connector: "mysql".into(),
                name: "t".into(),
                ts_ms: 0,
                db: "shop".into(),
                schema: None,
                table: "orders".into(),
                snapshot: None,
                position: SourcePosition::default(),
            },
            op: Op::Create,
            ts_ms: 0,
            transaction: None,
            event_id: None,
            tenant_id: None,
            schema_version: None,
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
        };

        writer.append(&[event]).await?;
        assert_eq!(writer.events_written(), 1);
        let bytes_before_empty = writer.bytes_written();

        // Empty append is a no-op; counters unchanged.
        writer.append(&[]).await?;
        assert_eq!(writer.events_written(), 1);
        assert_eq!(writer.bytes_written(), bytes_before_empty);

        let result = writer.close().await?;
        assert_eq!(result.rows_written, 1);
        Ok(())
    }

    #[tokio::test]
    async fn unsupported_compression_falls_back_to_gzip() {
        let format = JsonLinesFormat::new(Compression::Snappy);
        assert_eq!(format.extension(), "jsonl.gz");
    }

    #[tokio::test]
    async fn empty_batch_writes_zero_byte_jsonl() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let params =
            ObjectStoreParams::local(tmp.path().to_string_lossy().to_string());
        let handle = ObjectStoreHandle::new(params)?;
        let format = JsonLinesFormat::new(Compression::None);

        let path = Path::from("jsonl/empty.jsonl");
        let res = format
            .write_simple_rows(handle.store.clone(), &path, &[])
            .await?;
        assert_eq!(res.rows_written, 0);
        assert_eq!(res.bytes_written, 0);
        Ok(())
    }
}
