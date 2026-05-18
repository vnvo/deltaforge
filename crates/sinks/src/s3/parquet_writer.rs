//! Parquet writer over `object_store`.
//!
//! Phase 1a scope: write a fixed-schema batch of rows to a single Parquet file
//! at a given object-store path. No schema mapping, no rolling, no partitioning,
//! no DLQ — those are Phase 1b-1f.
//!
//! The fixed schema below (`id`, `name`, `ts`) is a placeholder to validate the
//! end-to-end plumbing. Phase 1c replaces it with a DDL-derived schema.

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_array::{
    Int64Array, RecordBatch, StringArray, TimestampMillisecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use object_store::{ObjectStore, path::Path};
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

/// Placeholder row type — replaced in Phase 1c by a generic `Event` mapper.
#[derive(Debug, Clone)]
pub struct SimpleRow {
    pub id: i64,
    pub name: String,
    pub ts_ms: i64,
}

/// Writes Parquet files to an `object_store::ObjectStore`.
///
/// Phase 1a is single-shot: build → write_rows → close.
pub struct ParquetSinkWriter {
    store: Arc<dyn ObjectStore>,
    schema: Arc<Schema>,
    compression: Compression,
}

impl ParquetSinkWriter {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                false,
            ),
        ]));
        Self {
            store,
            schema,
            compression: Compression::SNAPPY,
        }
    }

    pub fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    /// Write `rows` to a Parquet file at the given object-store `path`.
    ///
    /// Uses the parquet AsyncArrowWriter over a `BufWriter` against
    /// `object_store`. The writer issues multipart uploads automatically
    /// once buffered bytes exceed the threshold; on `close()` it finalizes
    /// the upload atomically.
    pub async fn write_rows(
        &self,
        path: &Path,
        rows: &[SimpleRow],
    ) -> Result<u64> {
        let batch = rows_to_record_batch(&self.schema, rows)
            .context("convert rows to record batch")?;

        let props = WriterProperties::builder()
            .set_compression(self.compression)
            .build();

        // object_store BufWriter handles multipart upload semantics.
        let buf_writer = object_store::buffered::BufWriter::new(
            self.store.clone(),
            path.clone(),
        );

        let mut writer = AsyncArrowWriter::try_new(
            buf_writer,
            self.schema.clone(),
            Some(props),
        )
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
    use object_store::ObjectStoreExt;
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
}
