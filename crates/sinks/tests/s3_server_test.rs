//! S3 sink writers (Parquet, JSON Lines) against the S3-compatible test
//! server (`s3-test-server`: RustFS, pinned).
//!
//! Run with:
//!   cargo test -p sinks --test s3_server_test -- --ignored
//!
//! These tests require Docker. They are gated behind `#[ignore]` so the
//! default `cargo test` run stays fast and dependency-free.

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use ctor::dtor;
use object_store::ObjectStoreExt;
use object_store::path::Path;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::async_reader::ParquetObjectReader;
use sinks::s3::{
    Compression, JsonLinesFormat, ObjectStoreParams, ParquetFormat,
    ParquetSinkWriter, SimpleRow, build_object_store,
};
use tokio::sync::OnceCell;

const TEST_BUCKET: &str = "deltaforge-test";

#[dtor]
fn cleanup_server() {
    s3_test_server::remove_shared();
}

/// The binary's shared S3 test server (`s3-test-server`), with the test
/// bucket created.
async fn server() -> &'static s3_test_server::S3Server {
    static BUCKET: OnceCell<()> = OnceCell::const_new();
    let server = s3_test_server::shared().await;
    BUCKET
        .get_or_init(|| async {
            server
                .create_bucket(TEST_BUCKET)
                .await
                .expect("create the test bucket")
        })
        .await;
    server
}

fn params_for(endpoint: &str) -> ObjectStoreParams {
    ObjectStoreParams::s3_compatible(
        TEST_BUCKET,
        endpoint,
        s3_test_server::ACCESS_KEY,
        s3_test_server::SECRET_KEY,
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
async fn phase1a_writes_parquet_to_s3_server() -> Result<()> {
    let infra = server().await;
    let store = build_object_store(&params_for(&infra.endpoint))?;
    let writer = ParquetSinkWriter::new(store.clone());

    let path = Path::from("phase1a/server_smoke.parquet");
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
    let infra = server().await;
    let store = build_object_store(&params_for(&infra.endpoint))?;
    let writer = ParquetSinkWriter::new(store.clone());

    let path = Path::from("phase1a/server_large.parquet");
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
async fn phase1b_writes_jsonl_gzip_to_s3_server() -> Result<()> {
    let infra = server().await;
    let store = build_object_store(&params_for(&infra.endpoint))?;
    let format = JsonLinesFormat::new(Compression::Gzip);

    let path = Path::from("phase1b/jsonl_gzip.jsonl.gz");
    let rows = sample_rows(10_000);
    let res = format
        .write_simple_rows(store.clone(), &path, &rows)
        .await?;
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
async fn phase1b_writes_jsonl_plain_to_s3_server() -> Result<()> {
    let infra = server().await;
    let store = build_object_store(&params_for(&infra.endpoint))?;
    let format = JsonLinesFormat::new(Compression::None);

    let path = Path::from("phase1b/jsonl_plain.jsonl");
    let rows = sample_rows(1000);
    let res = format
        .write_simple_rows(store.clone(), &path, &rows)
        .await?;
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
async fn phase1e_abandoned_writer_produces_no_visible_object() -> Result<()> {
    use deltaforge_core::encoding::arrow_schema::{
        Connector, build_envelope_arrow_schema,
    };
    use deltaforge_core::encoding::avro_types::{
        ColumnDesc, TypeConversionOpts,
    };
    use deltaforge_core::{Event, Op, SourceInfo, SourcePosition};
    use serde_json::json;
    use sinks::s3::{
        ParquetFormat, RollingConfig, WriterPool, WriterPoolConfig,
    };
    use std::sync::Arc;

    let infra = server().await;
    let store = build_object_store(&params_for(&infra.endpoint))?;

    let cols = vec![ColumnDesc {
        name: "id".into(),
        data_type: "bigint".into(),
        column_type: "bigint".into(),
        nullable: true,
        precision: None,
        scale: None,
        unsigned: false,
        is_array: false,
        element_type: None,
    }];
    let schema = Arc::new(build_envelope_arrow_schema(
        Connector::Mysql,
        &cols,
        &TypeConversionOpts::default(),
    ));

    let format: Arc<dyn sinks::s3::FileFormat> =
        Arc::new(ParquetFormat::default());
    // High thresholds so the writer doesn't roll on its own.
    let mut pool = WriterPool::with_fixed_schema(
        store.clone(),
        format,
        schema,
        WriterPoolConfig {
            prefix: "phase1e".into(),
            rolling: RollingConfig::default(),
        },
    );

    let ts_ms = chrono::NaiveDate::from_ymd_opt(2026, 6, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_millis();
    let events: Vec<Event> = (0..50_000)
        .map(|i| Event {
            before: None,
            after: Some(json!({"id": i as i64})),
            source: SourceInfo {
                version: "1".into(),
                connector: "mysql".into(),
                name: "t".into(),
                ts_ms,
                db: "shop".into(),
                schema: None,
                table: "orders".into(),
                snapshot: None,
                position: SourcePosition::default(),
            },
            op: Op::Create,
            ts_ms,
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
            boundary: None,
            size_bytes: 0,

            received_at_ms: ts_ms,
        })
        .collect();

    pool.append_batch(&events).await?;
    assert_eq!(pool.open_writer_count(), 1);

    // Force-abandon — never call close.
    let abandoned = pool.abandon_all();
    assert_eq!(abandoned, 1);

    // No completed file under the prefix should be visible.
    let prefix = Path::from("phase1e");
    let listed: Vec<_> =
        futures::TryStreamExt::try_collect::<Vec<_>>(store.list(Some(&prefix)))
            .await?;
    assert!(
        listed.is_empty(),
        "abandoned writer leaked a visible object: {} entries",
        listed.len()
    );
    // The orphan multipart still exists on the server until lifecycle expires
    // it — that's the operational requirement, not a test failure.
    Ok(())
}

#[tokio::test]
#[ignore]
async fn phase1b_parquet_via_format_trait() -> Result<()> {
    let infra = server().await;
    let store = build_object_store(&params_for(&infra.endpoint))?;
    let _format = ParquetFormat::default();

    // The incremental ParquetFormat::open_writer flow is exercised by the
    // Phase 1d e2e tests against local FS. Here we just smoke-check that
    // a ParquetSinkWriter (the facade) writes successfully to the server.
    let writer = ParquetSinkWriter::new(store.clone());
    let path = Path::from("phase1b/via_trait.parquet");
    let rows = sample_rows(5000);
    let n = writer.write_rows(&path, &rows).await?;
    assert_eq!(n, 5000);

    let read = read_back_rows(store, &path).await?;
    assert_eq!(read, 5000);
    Ok(())
}
