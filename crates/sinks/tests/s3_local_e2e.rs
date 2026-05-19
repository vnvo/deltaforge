//! End-to-end smoke test for the S3 / Parquet sink against local FS.
//!
//! Exercises the full Phase 1d path:
//!   Event → partition_for → WriterPool → ParquetFileWriter → object_store::local
//!
//! Run with: `cargo test -p sinks --test s3_local_e2e`
//! (No Docker required — runs in default `cargo test`.)

use std::sync::Arc;

use anyhow::Result;
use arrow_array::RecordBatch;
use deltaforge_core::encoding::arrow_schema::{
    Connector, build_envelope_arrow_schema,
};
use deltaforge_core::encoding::avro_types::{ColumnDesc, TypeConversionOpts};
use deltaforge_core::{Event, Op, SourceInfo, SourcePosition};
use object_store::ObjectStoreExt;
use object_store::path::Path;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::async_reader::ParquetObjectReader;
use serde_json::json;
use sinks::s3::{
    Compression, FileFormat, ObjectStoreParams, ParquetFormat, RollingConfig,
    WriterPool, WriterPoolConfig, build_object_store,
};

fn col(name: &str, data_type: &str) -> ColumnDesc {
    ColumnDesc {
        name: name.into(),
        data_type: data_type.into(),
        column_type: data_type.into(),
        nullable: true,
        precision: None,
        scale: None,
        unsigned: false,
        is_array: false,
        element_type: None,
    }
}

fn event_on(day: u32, table: &str, id: i64, email: &str) -> Event {
    let ts_ms = chrono::NaiveDate::from_ymd_opt(2026, 5, day)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_millis();
    Event {
        before: None,
        after: Some(json!({"id": id, "email": email})),
        source: SourceInfo {
            version: "1".into(),
            connector: "mysql".into(),
            name: "test".into(),
            ts_ms,
            db: "shop".into(),
            schema: None,
            table: table.into(),
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
        checkpoint: None,
        size_bytes: 0,
        received_at_ms: ts_ms,
    }
}

async fn read_rows(
    store: Arc<dyn object_store::ObjectStore>,
    path: &Path,
) -> Result<usize> {
    let meta = store.head(path).await?;
    let reader = ParquetObjectReader::new(store.clone(), meta.location)
        .with_file_size(meta.size);
    let stream = ParquetRecordBatchStreamBuilder::new(reader)
        .await?
        .build()?;
    let batches: Vec<RecordBatch> =
        futures::TryStreamExt::try_collect(stream).await?;
    Ok(batches.iter().map(|b| b.num_rows()).sum())
}

#[tokio::test]
async fn e2e_writes_partitioned_parquet_to_local_fs() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let params =
        ObjectStoreParams::local(tmp.path().to_string_lossy().to_string());
    let store = build_object_store(&params)?;

    let cols = [col("id", "bigint"), col("email", "varchar")];
    let schema = Arc::new(build_envelope_arrow_schema(
        Connector::Mysql,
        &cols,
        &TypeConversionOpts::default(),
    ));

    let format: Arc<dyn FileFormat> =
        Arc::new(ParquetFormat::new(Compression::Snappy));
    let cfg = WriterPoolConfig {
        prefix: "deltaforge".into(),
        rolling: RollingConfig {
            max_events: 10,
            ..Default::default()
        },
    };
    let mut pool =
        WriterPool::with_fixed_schema(store.clone(), format, schema, cfg);

    // 30 events across 3 partitions (3 tables on same day) → 3 files of 10 each.
    let mut events = Vec::new();
    for table in ["orders", "customers", "events"] {
        for i in 0..10 {
            events.push(event_on(19, table, i, &format!("u{i}@x")));
        }
    }
    let committed = pool.append_batch(&events).await?;
    assert_eq!(committed.len(), 3, "one file per table partition");

    // Each file contains 10 rows.
    for c in &committed {
        assert_eq!(c.result.rows_written, 10);
        let p = Path::from(c.path.clone());
        let n = read_rows(store.clone(), &p).await?;
        assert_eq!(n, 10);
    }

    // No open writers left.
    assert_eq!(pool.open_writer_count(), 0);
    Ok(())
}

#[tokio::test]
async fn e2e_close_all_flushes_in_progress_writers() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let params =
        ObjectStoreParams::local(tmp.path().to_string_lossy().to_string());
    let store = build_object_store(&params)?;
    let cols = [col("id", "bigint")];
    let schema = Arc::new(build_envelope_arrow_schema(
        Connector::Mysql,
        &cols,
        &TypeConversionOpts::default(),
    ));
    let format: Arc<dyn FileFormat> = Arc::new(ParquetFormat::default());
    let cfg = WriterPoolConfig {
        prefix: String::new(),
        rolling: RollingConfig::default(), // very lenient
    };
    let mut pool =
        WriterPool::with_fixed_schema(store.clone(), format, schema, cfg);

    let events: Vec<_> = (0..50)
        .map(|i| event_on(20, "orders", i, &format!("u{i}@x")))
        .collect();
    let committed = pool.append_batch(&events).await?;
    assert!(
        committed.is_empty(),
        "50 events well under rolling thresholds"
    );

    let final_committed = pool.close_all().await;
    assert_eq!(final_committed.len(), 1);
    assert_eq!(final_committed[0].result.rows_written, 50);

    let p = Path::from(final_committed[0].path.clone());
    assert_eq!(read_rows(store, &p).await?, 50);
    Ok(())
}

#[tokio::test]
async fn e2e_separate_days_produce_separate_files() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let params =
        ObjectStoreParams::local(tmp.path().to_string_lossy().to_string());
    let store = build_object_store(&params)?;
    let cols = [col("id", "bigint")];
    let schema = Arc::new(build_envelope_arrow_schema(
        Connector::Mysql,
        &cols,
        &TypeConversionOpts::default(),
    ));
    let format: Arc<dyn FileFormat> = Arc::new(ParquetFormat::default());
    let mut pool = WriterPool::with_fixed_schema(
        store.clone(),
        format,
        schema,
        WriterPoolConfig::default(),
    );

    let events = vec![
        event_on(10, "orders", 1, "a@x"),
        event_on(11, "orders", 2, "b@x"),
        event_on(12, "orders", 3, "c@x"),
    ];
    pool.append_batch(&events).await?;
    assert_eq!(pool.open_writer_count(), 3, "one writer per day partition");

    let committed = pool.close_all().await;
    assert_eq!(committed.len(), 3);
    // Each partition path contains a different day component.
    let mut days: Vec<String> = committed
        .iter()
        .map(|c| {
            c.path
                .split('/')
                .find(|p| p.starts_with("day="))
                .unwrap()
                .to_string()
        })
        .collect();
    days.sort();
    assert_eq!(days, vec!["day=10", "day=11", "day=12"]);
    Ok(())
}
