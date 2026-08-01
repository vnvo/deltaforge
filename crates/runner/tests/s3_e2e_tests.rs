//! End-to-end integration tests for the S3/Parquet sink.
//!
//! These tests exercise the full runner-side wiring of:
//!   `Event` → `S3Sink` (built by `build_s3_sink`) → `WriterPool`
//!   → `ParquetFileWriter` → MinIO → Parquet read-back.
//!
//! The schema resolver is built via `runner::schema_provider::build_arrow_schema_resolver`
//! with a fake `SchemaProvider` so we don't need a live source. This isolates
//! the sink integration from MySQL/PG specifics while still exercising the
//! production code paths.
//!
//! Run with: `cargo test -p runner --test s3_e2e_tests -- --ignored --test-threads=1`

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use arrow_array::{
    Array, BooleanArray, Decimal128Array, Int64Array, RecordBatch, StringArray,
};
use async_trait::async_trait;
use ctor::dtor;
use deltaforge_config::{
    S3Compression, S3FileFormat, S3FileRoll, S3SinkCfg, SinkCfg,
};
use deltaforge_core::encoding::avro_types::TypeConversionOpts;
use deltaforge_core::{
    BatchResult, Event, Op, Sink, SourceInfo, SourcePosition, Transaction,
};
use object_store::path::Path;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::async_reader::ParquetObjectReader;
use runner::{
    ArcSchemaProvider, ColumnSchemaInfo, SchemaProvider, TableSchemaInfo,
    build_arrow_schema_resolver,
};
use serde_json::json;
use sinks::s3::{ObjectStoreParams, build_object_store, build_s3_sink};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::sync::OnceCell;
use tokio_util::sync::CancellationToken;

// =============================================================================
// MinIO testcontainer (shared across tests in this file)
// =============================================================================

const MINIO_PORT: u16 = 9000;
const MINIO_KEY: &str = "minioadmin";
const MINIO_SECRET: &str = "minioadmin";
const BUCKET: &str = "deltaforge-e2e";

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
            let container = GenericImage::new("minio/minio", "latest")
                .with_wait_for(WaitFor::seconds(2))
                .with_exposed_port(MINIO_PORT.tcp())
                .with_entrypoint("/bin/sh")
                .with_env_var("MINIO_ROOT_USER", MINIO_KEY)
                .with_env_var("MINIO_ROOT_PASSWORD", MINIO_SECRET)
                .with_cmd(vec![
                    "-c".to_string(),
                    format!("mkdir -p /data/{BUCKET} && minio server /data"),
                ])
                .start()
                .await
                .expect("start MinIO");
            let host = container.get_host().await.expect("minio host");
            let port = container
                .get_host_port_ipv4(MINIO_PORT)
                .await
                .expect("minio port");
            let endpoint = format!("http://{host}:{port}");
            wait_for_minio_ready(&endpoint, Duration::from_secs(30))
                .await
                .expect("MinIO ready");
            MinioInfra {
                container,
                endpoint,
            }
        })
        .await
}

async fn wait_for_minio_ready(endpoint: &str, timeout: Duration) -> Result<()> {
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

// =============================================================================
// Fake SchemaProvider — declares the columns our test events use
// =============================================================================

struct FakeSchemaProvider {
    by_table: std::collections::HashMap<String, TableSchemaInfo>,
}

#[async_trait]
impl SchemaProvider for FakeSchemaProvider {
    async fn get_table_schema(&self, table: &str) -> Option<TableSchemaInfo> {
        self.by_table.get(table).cloned()
    }
    async fn list_schemas(&self) -> Vec<TableSchemaInfo> {
        self.by_table.values().cloned().collect()
    }
}

fn col(
    name: &str,
    data_type: &str,
    full_type: &str,
    nullable: bool,
    precision: Option<i64>,
    scale: Option<i64>,
) -> ColumnSchemaInfo {
    ColumnSchemaInfo {
        name: name.into(),
        data_type: data_type.into(),
        full_type: full_type.into(),
        nullable,
        is_json_like: false,
        unsigned: false,
        is_array: false,
        numeric_precision: precision,
        numeric_scale: scale,
        element_type: None,
    }
}

fn orders_schema() -> TableSchemaInfo {
    TableSchemaInfo {
        database: "shop".into(),
        table: "orders".into(),
        columns: vec![
            col("id", "bigint", "bigint", false, None, None),
            col(
                "customer_email",
                "varchar",
                "varchar(255)",
                true,
                None,
                None,
            ),
            col(
                "amount",
                "decimal",
                "decimal(12,2)",
                true,
                Some(12),
                Some(2),
            ),
            col("paid", "boolean", "boolean", false, None, None),
        ],
        primary_key: vec!["id".into()],
    }
}

fn fake_provider() -> ArcSchemaProvider {
    let mut by_table = std::collections::HashMap::new();
    // The real schema registry is keyed by the db-qualified name ("db.table"),
    // and the S3 sink's resolver looks up that qualified key. Register the same
    // way so this test exercises the production lookup path (the events carry
    // db="shop", table="orders").
    by_table.insert("shop.orders".to_string(), orders_schema());
    Arc::new(FakeSchemaProvider { by_table })
}

// =============================================================================
// Test event factory
// =============================================================================

fn make_order(
    op: Op,
    day: u32,
    id: i64,
    email: Option<&str>,
    amount: &str,
    paid: bool,
) -> Event {
    let ts_ms = chrono::NaiveDate::from_ymd_opt(2026, 5, day)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_millis();

    let after = json!({
        "id": id,
        "customer_email": email,
        "amount": amount,
        "paid": paid,
    });

    Event {
        before: None,
        after: Some(after),
        source: SourceInfo {
            version: "1".into(),
            connector: "mysql".into(),
            name: "e2e".into(),
            ts_ms,
            db: "shop".into(),
            schema: None,
            table: "orders".into(),
            snapshot: None,
            position: SourcePosition {
                file: Some("mysql-bin.000001".into()),
                pos: Some(1000 + id as u64),
                ..Default::default()
            },
        },
        op,
        ts_ms,
        transaction: Some(Transaction {
            id: format!("tx-{id}"),
            total_order: None,
            data_collection_order: None,
        }),
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

// =============================================================================
// Helpers
// =============================================================================

fn s3_cfg(format: S3FileFormat, prefix: &str, max_events: u64) -> S3SinkCfg {
    S3SinkCfg {
        id: "e2e-s3".into(),
        bucket: BUCKET.into(),
        prefix: prefix.into(),
        region: Some("us-east-1".into()),
        endpoint: None, // overridden below
        access_key_id: Some(MINIO_KEY.into()),
        secret_access_key: Some(MINIO_SECRET.into()),
        virtual_hosted_style: false,
        local: false,
        format,
        compression: match format {
            S3FileFormat::Parquet => S3Compression::Snappy,
            S3FileFormat::Jsonl => S3Compression::Gzip,
        },
        file_roll: S3FileRoll {
            max_bytes: 256 * 1024 * 1024,
            max_events,
            max_age_secs: 300,
            idle_age_secs: 600,
        },
        send_timeout_secs: 60,
        required: Some(true),
        filter: None,
    }
}

fn cfg_for_endpoint(mut cfg: S3SinkCfg, endpoint: &str) -> SinkCfg {
    cfg.endpoint = Some(endpoint.to_string());
    SinkCfg::S3(cfg)
}

async fn list_under(
    store: Arc<dyn object_store::ObjectStore>,
    prefix: &str,
) -> Result<Vec<object_store::ObjectMeta>> {
    let p = Path::from(prefix);
    Ok(
        futures::TryStreamExt::try_collect::<Vec<_>>(store.list(Some(&p)))
            .await?,
    )
}

async fn read_parquet(
    store: Arc<dyn object_store::ObjectStore>,
    obj: &object_store::ObjectMeta,
) -> Result<Vec<RecordBatch>> {
    let reader = ParquetObjectReader::new(store.clone(), obj.location.clone())
        .with_file_size(obj.size);
    let stream = ParquetRecordBatchStreamBuilder::new(reader)
        .await?
        .build()?;
    Ok(futures::TryStreamExt::try_collect(stream).await?)
}

fn schema_resolver_for_test() -> sinks::s3::SchemaResolver {
    build_arrow_schema_resolver(
        fake_provider(),
        "mysql",
        TypeConversionOpts::default(),
    )
}

fn store_for(endpoint: &str) -> Arc<dyn object_store::ObjectStore> {
    build_object_store(&ObjectStoreParams::s3_minio(
        BUCKET,
        endpoint,
        MINIO_KEY,
        MINIO_SECRET,
    ))
    .unwrap()
}

// =============================================================================
// Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn e2e_parquet_with_ddl_schema_minio() -> Result<()> {
    let infra = minio().await;
    let SinkCfg::S3(cfg) = cfg_for_endpoint(
        s3_cfg(S3FileFormat::Parquet, "e2e/parquet", 5),
        &infra.endpoint,
    ) else {
        unreachable!()
    };

    let sink = build_s3_sink(
        &cfg,
        CancellationToken::new(),
        "pipeline-e2e",
        Some(schema_resolver_for_test()),
    )?;

    // 5 events → exactly one rolled file (max_events: 5).
    let events = vec![
        make_order(Op::Create, 20, 1, Some("a@x"), "10.00", true),
        make_order(Op::Create, 20, 2, Some("b@x"), "20.50", false),
        make_order(Op::Update, 20, 1, Some("a@x"), "12.34", true),
        make_order(Op::Create, 20, 3, None, "0.01", true),
        make_order(Op::Delete, 20, 2, Some("b@x"), "20.50", false),
    ];
    let result: BatchResult = sink.send_batch(&events).await?;
    assert!(
        result.dlq_failures.is_empty(),
        "no per-row DLQ failures in Phase 1: {:?}",
        result.dlq_failures
    );

    // File should be visible.
    let store = store_for(&infra.endpoint);
    let listed = list_under(store.clone(), "e2e/parquet").await?;
    assert_eq!(listed.len(), 1, "exactly one rolled file");
    let obj = &listed[0];
    assert!(
        obj.location
            .as_ref()
            .contains("table=orders/year=2026/month=05/day=20/"),
        "Hive partition path, got {}",
        obj.location
    );
    assert!(obj.location.as_ref().ends_with(".parquet"));

    // Read it back and assert content correctness for the typed columns.
    let batches = read_parquet(store, obj).await?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5);

    // First batch contains all rows since it's a single RecordBatch from one send.
    let b = &batches[0];

    // op column: c, c, u, c, d
    let ops = b
        .column_by_name("op")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        (0..5).map(|i| ops.value(i)).collect::<Vec<_>>(),
        vec!["c", "c", "u", "c", "d"]
    );

    // after_id (typed Int64, not stringified)
    let after_id = b
        .column_by_name("after_id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(after_id.value(0), 1);
    assert_eq!(after_id.value(4), 2);

    // after_amount as native Decimal128(12, 2) — TD-003 fix in action.
    let amount = b
        .column_by_name("after_amount")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(amount.value(0), 1000); // 10.00 → 1000 with scale=2
    assert_eq!(amount.value(1), 2050); // 20.50 → 2050
    assert_eq!(amount.value(2), 1234); // 12.34 → 1234

    // after_paid as native Boolean
    let paid = b
        .column_by_name("after_paid")
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(paid.value(0));
    assert!(!paid.value(1));

    // Nullable column: row 3 has email=None
    let email = b
        .column_by_name("after_customer_email")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(email.value(0), "a@x");
    assert!(email.is_null(3));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn e2e_jsonl_gzip_with_ddl_schema_minio() -> Result<()> {
    let infra = minio().await;
    let SinkCfg::S3(cfg) = cfg_for_endpoint(
        s3_cfg(S3FileFormat::Jsonl, "e2e/jsonl", 3),
        &infra.endpoint,
    ) else {
        unreachable!()
    };

    let sink = build_s3_sink(
        &cfg,
        CancellationToken::new(),
        "pipeline-e2e",
        Some(schema_resolver_for_test()),
    )?;

    let events = vec![
        make_order(Op::Create, 21, 10, Some("x@y"), "100.00", true),
        make_order(Op::Create, 21, 11, Some("a@b"), "0.01", false),
        make_order(Op::Update, 21, 10, Some("x@y"), "150.00", true),
    ];
    sink.send_batch(&events).await?;

    let store = store_for(&infra.endpoint);
    let listed = list_under(store.clone(), "e2e/jsonl").await?;
    assert_eq!(listed.len(), 1, "single rolled jsonl file");
    let obj = &listed[0];
    assert!(obj.location.as_ref().ends_with(".jsonl.gz"));

    // Roundtrip: gunzip and parse line by line.
    use object_store::ObjectStoreExt;
    let bytes = store.get(&obj.location).await?.bytes().await?;
    use flate2::read::GzDecoder;
    use std::io::Read;
    let mut s = String::new();
    GzDecoder::new(&bytes[..]).read_to_string(&mut s)?;
    let lines: Vec<&str> = s.lines().collect();
    assert_eq!(lines.len(), 3);

    // Each line is a serialized Event; just verify the JSON is well-formed
    // and carries our `op` + `after` payload.
    for (i, line) in lines.iter().enumerate() {
        let v: serde_json::Value = serde_json::from_str(line)?;
        assert!(v.get("op").is_some(), "line {i}: missing op");
        assert!(v.get("after").is_some(), "line {i}: missing after");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn e2e_multi_day_partitions_produce_separate_files() -> Result<()> {
    let infra = minio().await;
    let SinkCfg::S3(cfg) = cfg_for_endpoint(
        s3_cfg(S3FileFormat::Parquet, "e2e/multiday", 1_000_000),
        &infra.endpoint,
    ) else {
        unreachable!()
    };

    let sink = build_s3_sink(
        &cfg,
        CancellationToken::new(),
        "pipeline-e2e",
        Some(schema_resolver_for_test()),
    )?;

    let events = vec![
        make_order(Op::Create, 22, 1, Some("a@x"), "1.00", true),
        make_order(Op::Create, 23, 2, Some("b@x"), "2.00", true),
        make_order(Op::Create, 24, 3, Some("c@x"), "3.00", true),
    ];
    sink.send_batch(&events).await?;

    // No threshold hit (max_events very high); files only land after flush.
    let committed = sink.flush_on_shutdown().await;
    assert_eq!(committed.len(), 3, "one file per day partition");

    let store = store_for(&infra.endpoint);
    let listed = list_under(store, "e2e/multiday").await?;
    let mut days: Vec<String> = listed
        .iter()
        .map(|m| {
            m.location
                .as_ref()
                .split('/')
                .find(|p| p.starts_with("day="))
                .unwrap()
                .to_string()
        })
        .collect();
    days.sort();
    assert_eq!(days, vec!["day=22", "day=23", "day=24"]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn e2e_envelope_only_fallback_when_no_resolver() -> Result<()> {
    let infra = minio().await;
    let SinkCfg::S3(cfg) = cfg_for_endpoint(
        s3_cfg(S3FileFormat::Parquet, "e2e/envonly", 2),
        &infra.endpoint,
    ) else {
        unreachable!()
    };

    // No schema resolver — should fall back to envelope-only Parquet.
    let sink = build_s3_sink(&cfg, CancellationToken::new(), "pipe", None)?;
    sink.send_batch(&[
        make_order(Op::Create, 25, 1, Some("a@x"), "1.00", true),
        make_order(Op::Create, 25, 2, Some("b@x"), "2.00", true),
    ])
    .await?;

    let store = store_for(&infra.endpoint);
    let listed = list_under(store.clone(), "e2e/envonly").await?;
    assert_eq!(listed.len(), 1);

    let batches = read_parquet(store, &listed[0]).await?;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    // Envelope-only: no before_*/after_* columns, only meta.
    let schema = batches[0].schema();
    let names: Vec<&str> =
        schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(names.contains(&"op"));
    assert!(!names.iter().any(|n| n.starts_with("after_")));
    Ok(())
}
