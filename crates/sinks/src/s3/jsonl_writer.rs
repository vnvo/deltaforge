//! JSON Lines writer over `object_store`.
//!
//! Each row is serialized to a single JSON object followed by `\n`. Optional
//! gzip compression wraps the whole stream. The format is schema-less and
//! self-describing, making it the format of choice for forensic / audit /
//! log-pipeline use cases.
//!
//! Phase 1b: single-shot in-memory serialization. Phase 1d switches to a
//! streaming writer for large files.

use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use flate2::Compression as GzLevel;
use flate2::write::GzEncoder;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use std::io::Write;

use super::SimpleRow;
use super::file_format::{Compression, FileFormat, WriteResult};

/// JSON Lines implementation of `FileFormat`. Supports gzip compression.
#[derive(Debug, Clone)]
pub struct JsonLinesFormat {
    compression: Compression,
}

impl JsonLinesFormat {
    pub fn new(compression: Compression) -> Self {
        // Phase 1b: jsonl supports gzip / none. zstd will land with the
        // streaming writer in Phase 1d.
        let compression = match compression {
            Compression::None | Compression::Gzip => compression,
            other => {
                tracing::warn!(
                    "jsonl compression {other:?} not yet supported, using gzip"
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
            _ => "jsonl",
        }
    }

    fn content_type(&self) -> &'static str {
        match self.compression {
            Compression::Gzip => "application/gzip",
            _ => "application/x-ndjson",
        }
    }

    fn label(&self) -> &'static str {
        "jsonl"
    }

    async fn write_rows(
        &self,
        store: Arc<dyn ObjectStore>,
        path: &Path,
        rows: &[SimpleRow],
    ) -> Result<WriteResult> {
        // Serialize all rows to a contiguous buffer (`{...}\n` per row).
        // Phase 1d will move this to a streaming writer over BufWriter so we
        // don't hold the whole file in memory for large rolls.
        let mut buf = Vec::with_capacity(rows.len() * 96);
        for row in rows {
            serde_json::to_writer(&mut buf, &row_as_json(row))
                .context("serialize row to jsonl buffer")?;
            buf.push(b'\n');
        }

        let payload = match self.compression {
            Compression::Gzip => {
                let mut encoder = GzEncoder::new(
                    Vec::with_capacity(buf.len() / 4),
                    GzLevel::default(),
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

/// Phase 1b placeholder — Phase 1c replaces this with the CDC envelope shape.
fn row_as_json(row: &SimpleRow) -> serde_json::Value {
    serde_json::json!({
        "id": row.id,
        "name": row.name,
        "ts_ms": row.ts_ms,
    })
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
            .write_rows(handle.store.clone(), &path, &rows)
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
            .write_rows(handle.store.clone(), &path, &rows)
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
        let res = format.write_rows(handle.store.clone(), &path, &[]).await?;
        assert_eq!(res.rows_written, 0);
        assert_eq!(res.bytes_written, 0);
        Ok(())
    }
}
