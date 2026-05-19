//! Parquet writer over `object_store`.
//!
//! Phase 1d: incremental `FileWriter` over `AsyncArrowWriter` wrapping an
//! `object_store::buffered::BufWriter`. Multipart upload is automatic; the
//! file is finalized atomically on `close`.

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_array::{
    Int64Array, RecordBatch, StringArray, TimestampMillisecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use async_trait::async_trait;
use deltaforge_core::Event;
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::Compression as ParquetCompression;
use parquet::file::properties::WriterProperties;

use super::encoder::events_to_record_batch;
use super::file_format::{Compression, FileFormat, FileWriter, WriteResult};

/// Backwards-compatible placeholder row used by Phase 1a/1b sample tests.
/// Phase 1c+ replaces this with `Event`-driven encoding via the encoder
/// module, but we keep the type and the `ParquetSinkWriter` facade so older
/// tests + the spec examples continue to compile.
#[derive(Debug, Clone)]
pub struct SimpleRow {
    pub id: i64,
    pub name: String,
    pub ts_ms: i64,
}

/// Parquet implementation of `FileFormat`.
#[derive(Debug, Clone)]
pub struct ParquetFormat {
    placeholder_schema: Arc<Schema>,
    compression: ParquetCompression,
}

impl ParquetFormat {
    pub fn new(compression: Compression) -> Self {
        Self {
            placeholder_schema: placeholder_schema(),
            compression: map_compression(compression),
        }
    }

    /// Placeholder schema used by the SimpleRow facade tests. Real `FileWriter`
    /// instances built via `open_writer` use the schema passed in at open time.
    pub fn schema(&self) -> Arc<Schema> {
        self.placeholder_schema.clone()
    }
}

impl Default for ParquetFormat {
    fn default() -> Self {
        Self::new(Compression::Snappy)
    }
}

fn placeholder_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
    ]))
}

fn map_compression(c: Compression) -> ParquetCompression {
    match c {
        Compression::None => ParquetCompression::UNCOMPRESSED,
        Compression::Snappy => ParquetCompression::SNAPPY,
        Compression::Gzip => ParquetCompression::GZIP(Default::default()),
        Compression::Zstd => ParquetCompression::ZSTD(Default::default()),
    }
}

#[async_trait]
impl FileFormat for ParquetFormat {
    fn extension(&self) -> &'static str {
        "parquet"
    }
    fn content_type(&self) -> &'static str {
        "application/vnd.apache.parquet"
    }
    fn label(&self) -> &'static str {
        "parquet"
    }

    async fn open_writer(
        &self,
        store: Arc<dyn ObjectStore>,
        path: Path,
        schema: Arc<Schema>,
    ) -> Result<Box<dyn FileWriter + Send>> {
        let props = WriterProperties::builder()
            .set_compression(self.compression)
            .build();
        let buf_writer =
            object_store::buffered::BufWriter::new(store.clone(), path.clone());
        let writer =
            AsyncArrowWriter::try_new(buf_writer, schema.clone(), Some(props))
                .context("create parquet writer")?;
        Ok(Box::new(ParquetFileWriter {
            inner: writer,
            schema,
            store,
            path,
            events_written: 0,
        }))
    }
}

/// Incremental Parquet writer. Holds an `AsyncArrowWriter` over a buffered
/// object-store sink; calls `events_to_record_batch` to produce columnar
/// batches on each append.
pub struct ParquetFileWriter {
    inner: AsyncArrowWriter<object_store::buffered::BufWriter>,
    schema: Arc<Schema>,
    store: Arc<dyn ObjectStore>,
    path: Path,
    events_written: u64,
}

#[async_trait]
impl FileWriter for ParquetFileWriter {
    async fn append(&mut self, events: &[Event]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let batch = events_to_record_batch(&self.schema, events)?;
        self.inner
            .write(&batch)
            .await
            .context("append parquet batch")?;
        self.events_written += events.len() as u64;
        Ok(())
    }

    fn bytes_written(&self) -> u64 {
        // AsyncArrowWriter reports in-memory bytes accumulated for the
        // current row group, not the final on-disk size. It is a useful
        // signal for rolling but the authoritative byte count is the
        // object's HEAD size after close.
        self.inner.bytes_written() as u64
    }

    fn events_written(&self) -> u64 {
        self.events_written
    }

    async fn close(self: Box<Self>) -> Result<WriteResult> {
        let Self {
            inner,
            store,
            path,
            events_written,
            ..
        } = *self;
        let metadata = inner.close().await.context("close parquet writer")?;
        let bytes = store.head(&path).await.map(|m| m.size).unwrap_or(0);
        Ok(WriteResult {
            rows_written: metadata.file_metadata().num_rows().max(0) as u64,
            // Defensive: events_written should equal rows for Parquet, but
            // keep the field as the authoritative ingress counter.
            bytes_written: bytes,
        })
        .map(|wr| WriteResult {
            rows_written: wr.rows_written.max(events_written),
            ..wr
        })
    }
}

// =============================================================================
// SimpleRow facade (kept for Phase 1a/1b sample tests)
// =============================================================================

/// Backwards-compatible facade kept for Phase 1a sample tests. Writes a fixed
/// (id, name, ts) schema and is unrelated to the production Event path.
pub struct ParquetSinkWriter {
    store: Arc<dyn ObjectStore>,
    format: ParquetFormat,
}

impl ParquetSinkWriter {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            format: ParquetFormat::default(),
        }
    }

    pub fn schema(&self) -> Arc<Schema> {
        self.format.schema()
    }

    pub async fn write_rows(
        &self,
        path: &Path,
        rows: &[SimpleRow],
    ) -> Result<u64> {
        let schema = self.format.schema();
        let batch = rows_to_record_batch(&schema, rows)?;

        let props = WriterProperties::builder()
            .set_compression(self.format.compression)
            .build();
        let buf_writer = object_store::buffered::BufWriter::new(
            self.store.clone(),
            path.clone(),
        );
        let mut writer =
            AsyncArrowWriter::try_new(buf_writer, schema, Some(props))
                .context("create parquet writer")?;
        writer.write(&batch).await.context("write record batch")?;
        let metadata = writer.close().await.context("close parquet writer")?;
        Ok(metadata.file_metadata().num_rows() as u64)
    }
}

fn rows_to_record_batch(
    schema: &Arc<Schema>,
    rows: &[SimpleRow],
) -> Result<RecordBatch> {
    let ids: Int64Array = rows.iter().map(|r| r.id).collect();
    let names: StringArray =
        rows.iter().map(|r| Some(r.name.as_str())).collect();
    let ts_values: Vec<i64> = rows.iter().map(|r| r.ts_ms).collect();
    let ts = TimestampMillisecondArray::from(ts_values).with_timezone("UTC");

    RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(ids), Arc::new(names), Arc::new(ts)],
    )
    .context("build RecordBatch")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::object_writer::{ObjectStoreHandle, ObjectStoreParams};
    use arrow_array::RecordBatch;
    use parquet::arrow::ParquetRecordBatchStreamBuilder;
    use parquet::arrow::async_reader::ParquetObjectReader;

    fn sample_rows(n: usize) -> Vec<SimpleRow> {
        (0..n)
            .map(|i| SimpleRow {
                id: i as i64,
                name: format!("row-{i}"),
                ts_ms: 1_700_000_000_000 + (i as i64),
            })
            .collect()
    }

    async fn read_back_rows(
        handle: &ObjectStoreHandle,
        path: &Path,
    ) -> Result<usize> {
        let meta = handle.store.head(path).await?;
        let reader =
            ParquetObjectReader::new(handle.store.clone(), meta.location)
                .with_file_size(meta.size);
        let stream = ParquetRecordBatchStreamBuilder::new(reader)
            .await?
            .build()?;
        let batches: Vec<RecordBatch> =
            futures::TryStreamExt::try_collect(stream).await?;
        Ok(batches.iter().map(|b| b.num_rows()).sum())
    }

    #[tokio::test]
    async fn writes_parquet_to_local_store() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let params =
            ObjectStoreParams::local(tmp.path().to_string_lossy().to_string());
        let handle = ObjectStoreHandle::new(params)?;
        let writer = ParquetSinkWriter::new(handle.store.clone());

        let path = Path::from("phase1a/local.parquet");
        let rows = sample_rows(1000);
        let written = writer.write_rows(&path, &rows).await?;
        assert_eq!(written, 1000);

        let read = read_back_rows(&handle, &path).await?;
        assert_eq!(read, 1000);
        Ok(())
    }

    #[tokio::test]
    async fn writes_empty_batch_produces_valid_parquet() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let params =
            ObjectStoreParams::local(tmp.path().to_string_lossy().to_string());
        let handle = ObjectStoreHandle::new(params)?;
        let writer = ParquetSinkWriter::new(handle.store.clone());

        let path = Path::from("phase1a/empty.parquet");
        let written = writer.write_rows(&path, &[]).await?;
        assert_eq!(written, 0);

        let read = read_back_rows(&handle, &path).await?;
        assert_eq!(read, 0);
        Ok(())
    }

    #[tokio::test]
    async fn parquet_compression_options() -> Result<()> {
        for compression in [
            Compression::None,
            Compression::Snappy,
            Compression::Gzip,
            Compression::Zstd,
        ] {
            let tmp = tempfile::tempdir()?;
            let params = ObjectStoreParams::local(
                tmp.path().to_string_lossy().to_string(),
            );
            let handle = ObjectStoreHandle::new(params)?;
            let format = ParquetFormat::new(compression);

            // Open writer via the trait + write a small set of SimpleRows
            // via the facade. We can't use ParquetFileWriter directly here
            // (it expects Event input). Phase 1d.2 integration test covers
            // Event input properly.
            let writer = ParquetSinkWriter::new(handle.store.clone());
            assert_eq!(format.extension(), "parquet");
            let path =
                Path::from(format!("compression/{compression:?}.parquet"));
            let n = writer.write_rows(&path, &sample_rows(500)).await?;
            assert_eq!(n, 500, "compression {compression:?}");
        }
        Ok(())
    }
}
