//! Phase 1a smoke test for S3 + Parquet against a MinIO testcontainer.
//!
//! Run with:
//!   cargo test -p sinks --test s3_minio_test -- --ignored
//!
//! These tests require Docker. They are gated behind `#[ignore]` so the
//! default `cargo test` run stays fast and dependency-free.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use ctor::dtor;
use object_store::ObjectStoreExt;
use object_store::path::Path;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::async_reader::ParquetObjectReader;
use sinks::s3::{
    Compression, FileFormat, JsonLinesFormat, ObjectStoreParams, ParquetFormat,
    ParquetSinkWriter, SimpleRow, build_object_store,
};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::sync::OnceCell;

const MINIO_PORT: u16 = 9000;
const MINIO_ACCESS_KEY: &str = "minioadmin";
const MINIO_SECRET_KEY: &str = "minioadmin";
const TEST_BUCKET: &str = "deltaforge-test";

struct MinioInfra {
    #[allow(dead_code)]
    container: ContainerAsync<GenericImage>,
    endpoint: String,
}

static MINIO: OnceCell<MinioInfra> = OnceCell::const_new();

#[dtor]
fn cleanup_minio() {
    if let Some(infra) = MINIO.get() {
        std::process::Command::new("docker")
            .args(["rm", "-f", infra.container.id()])
            .output()
            .ok();
    }
}

async fn minio() -> &'static MinioInfra {
    MINIO
        .get_or_init(|| async {
            // MinIO in filesystem mode treats top-level dirs under /data as
            // buckets, so we pre-create `/data/{TEST_BUCKET}` and skip the
            // need for SigV4-signed CreateBucket requests in this test.
            let container = GenericImage::new("minio/minio", "latest")
                .with_wait_for(WaitFor::seconds(2))
                .with_exposed_port(MINIO_PORT.tcp())
                .with_entrypoint("/bin/sh")
                .with_env_var("MINIO_ROOT_USER", MINIO_ACCESS_KEY)
                .with_env_var("MINIO_ROOT_PASSWORD", MINIO_SECRET_KEY)
                .with_cmd(vec![
                    "-c".to_string(),
                    format!(
                        "mkdir -p /data/{TEST_BUCKET} && \
                         minio server /data"
                    ),
                ])
                .start()
                .await
                .expect("start MinIO container");
            let host = container.get_host().await.expect("minio host");
            let port = container
                .get_host_port_ipv4(MINIO_PORT)
                .await
                .expect("minio host port");
            let endpoint = format!("http://{host}:{port}");

            // Wait for MinIO's HTTP listener to be reachable.
            wait_for_http(&endpoint, Duration::from_secs(30))
                .await
                .expect("MinIO HTTP ready");

            MinioInfra {
                container,
                endpoint,
            }
        })
        .await
}

async fn wait_for_http(endpoint: &str, timeout: Duration) -> Result<()> {
    let client = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if let Ok(resp) = client
            .get(format!("{endpoint}/minio/health/live"))
            .timeout(Duration::from_secs(2))
            .send()
            .await
            && resp.status().is_success()
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    anyhow::bail!("MinIO never became ready at {endpoint}")
}

fn params_for(endpoint: &str) -> ObjectStoreParams {
    ObjectStoreParams::s3_minio(
        TEST_BUCKET,
        endpoint,
        MINIO_ACCESS_KEY,
        MINIO_SECRET_KEY,
    )
}

async fn read_back_rows(
    store: Arc<dyn object_store::ObjectStore>,
    path: &Path,
) -> Result<usize> {
    let meta = store.head(path).await.context("head")?;
    let reader = ParquetObjectReader::new(store.clone(), meta.location)
        .with_file_size(meta.size);
    let stream = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .context("builder")?
        .build()
        .context("build stream")?;
    let batches: Vec<RecordBatch> =
        futures::TryStreamExt::try_collect(stream).await?;
    Ok(batches.iter().map(|b| b.num_rows()).sum())
}

fn sample_rows(n: usize) -> Vec<SimpleRow> {
    (0..n)
        .map(|i| SimpleRow {
            id: i as i64,
            name: format!("row-{i}"),
            ts_ms: 1_700_000_000_000 + (i as i64 * 1000),
        })
        .collect()
}

#[tokio::test]
#[ignore]
async fn phase1a_writes_parquet_to_minio() -> Result<()> {
    let infra = minio().await;
    let store = build_object_store(&params_for(&infra.endpoint))?;
    let writer = ParquetSinkWriter::new(store.clone());

    let path = Path::from("phase1a/minio_smoke.parquet");
    let rows = sample_rows(5000);
    let written = writer.write_rows(&path, &rows).await?;
    assert_eq!(written, 5000);

    let read = read_back_rows(store, &path).await?;
    assert_eq!(read, 5000);
    Ok(())
}

#[tokio::test]
#[ignore]
async fn phase1a_writes_large_file_via_multipart() -> Result<()> {
    let infra = minio().await;
    let store = build_object_store(&params_for(&infra.endpoint))?;
    let writer = ParquetSinkWriter::new(store.clone());

    let path = Path::from("phase1a/minio_large.parquet");
    // 200K rows ≈ ~10 MiB before compression; ends up well into multipart
    // territory after Parquet+snappy, validating object_store's multipart path.
    let rows = sample_rows(200_000);
    let written = writer.write_rows(&path, &rows).await?;
    assert_eq!(written, 200_000);

    let read = read_back_rows(store, &path).await?;
    assert_eq!(read, 200_000);
    Ok(())
}

#[tokio::test]
#[ignore]
async fn phase1b_writes_jsonl_gzip_to_minio() -> Result<()> {
    let infra = minio().await;
    let store = build_object_store(&params_for(&infra.endpoint))?;
    let format = JsonLinesFormat::new(Compression::Gzip);

    let path = Path::from("phase1b/jsonl_gzip.jsonl.gz");
    let rows = sample_rows(10_000);
    let res = format.write_rows(store.clone(), &path, &rows).await?;
    assert_eq!(res.rows_written, 10_000);
    assert!(res.bytes_written > 0);

    // Sanity: file exists and is non-empty.
    let meta = store.head(&path).await?;
    assert!(meta.size > 0);
    assert_eq!(meta.size, res.bytes_written);
    Ok(())
}

#[tokio::test]
#[ignore]
async fn phase1b_writes_jsonl_plain_to_minio() -> Result<()> {
    let infra = minio().await;
    let store = build_object_store(&params_for(&infra.endpoint))?;
    let format = JsonLinesFormat::new(Compression::None);

    let path = Path::from("phase1b/jsonl_plain.jsonl");
    let rows = sample_rows(1000);
    let res = format.write_rows(store.clone(), &path, &rows).await?;
    assert_eq!(res.rows_written, 1000);

    // Plain JSONL must be larger than gzipped equivalent.
    // 1000 rows of ~45-byte objects + newlines → ~45KiB.
    let meta = store.head(&path).await?;
    assert!(
        meta.size > 30_000,
        "1000 rows of plain jsonl should be >30KiB, got {}",
        meta.size
    );
    Ok(())
}

#[tokio::test]
#[ignore]
async fn phase1b_parquet_via_format_trait() -> Result<()> {
    let infra = minio().await;
    let store = build_object_store(&params_for(&infra.endpoint))?;
    let format = ParquetFormat::default();

    let path = Path::from("phase1b/via_trait.parquet");
    let rows = sample_rows(5000);
    let res = format.write_rows(store.clone(), &path, &rows).await?;
    assert_eq!(res.rows_written, 5000);
    assert!(res.bytes_written > 0);

    let read = read_back_rows(store, &path).await?;
    assert_eq!(read, 5000);
    Ok(())
}
