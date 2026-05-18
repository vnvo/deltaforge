//! Parquet writer over `object_store`.
//!
//! Phase 1a/1b scope: write a fixed-schema batch of rows to a single Parquet
//! file at a given object-store path. No partitioning, no DLQ — those are
//! Phase 1d-1f.
//!
//! The fixed schema below (`id`, `name`, `ts`) is a placeholder to validate
//! the end-to-end plumbing. Phase 1c replaces it with a DDL-derived schema.

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_array::{
    Int64Array, RecordBatch, StringArray, TimestampMillisecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use async_trait::async_trait;
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::Compression as ParquetCompression;
use parquet::file::properties::WriterProperties;

use super::file_format::{Compression, FileFormat, WriteResult};

/// Placeholder row type — replaced in Phase 1c by a generic `Event` mapper.
#[derive(Debug, Clone)]
pub struct SimpleRow {
    pub id: i64,
    pub name: String,
    pub ts_ms: i64,
}

/// Parquet implementation of `FileFormat`.
#[derive(Debug, Clone)]
pub struct ParquetFormat {
    schema: Arc<Schema>,
    compression: ParquetCompression,
}

impl ParquetFormat {
    pub fn new(compression: Compression) -> Self {
        Self {
            schema: default_schema(),
            compression: map_compression(compression),
        }
    }

    pub fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}

impl Default for ParquetFormat {
    fn default() -> Self {
        Self::new(Compression::Snappy)
    }
}

fn default_schema() -> Arc<Schema> {
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

    async fn write_rows(
        &self,
        store: Arc<dyn ObjectStore>,
        path: &Path,
        rows: &[SimpleRow],
    ) -> Result<WriteResult> {
        let batch = rows_to_record_batch(&self.schema, rows)
            .context("convert rows to record batch")?;

        let props = WriterProperties::builder()
            .set_compression(self.compression)
            .build();

        let buf_writer =
            object_store::buffered::BufWriter::new(store.clone(), path.clone());

        let mut writer = AsyncArrowWriter::try_new(
            buf_writer,
            self.schema.clone(),
            Some(props),
        )
        .context("create parquet writer")?;

        writer.write(&batch).await.context("write record batch")?;
        let metadata = writer.close().await.context("close parquet writer")?;

        // BufWriter::shutdown has happened inside writer.close(); the file is
        // visible at `path` now. Query its size to report bytes_written.
        let bytes = store.head(path).await.map(|m| m.size).unwrap_or(0);

        Ok(WriteResult {
            rows_written: metadata.file_metadata().num_rows() as u64,
            bytes_written: bytes,
        })
    }
}

/// Backward-compatible facade kept for Phase 1a tests.
/// Will be retired when Phase 1c switches to the generic `Event` flow.
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
        let res = self
            .format
            .write_rows(self.store.clone(), path, rows)
            .await?;
        Ok(res.rows_written)
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
    async fn parquet_format_trait_works() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let params =
            ObjectStoreParams::local(tmp.path().to_string_lossy().to_string());
        let handle = ObjectStoreHandle::new(params)?;
        let format = ParquetFormat::default();

        let path = Path::from("phase1b/via_trait.parquet");
        let rows = sample_rows(500);
        let res = format
            .write_rows(handle.store.clone(), &path, &rows)
            .await?;
        assert_eq!(res.rows_written, 500);
        assert!(res.bytes_written > 0);
        assert_eq!(format.extension(), "parquet");
        assert_eq!(format.label(), "parquet");
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
            let path =
                Path::from(format!("compression/{compression:?}.parquet"));
            let res = format
                .write_rows(handle.store.clone(), &path, &sample_rows(500))
                .await?;
            assert_eq!(res.rows_written, 500, "compression {compression:?}");
        }
        Ok(())
    }
}
