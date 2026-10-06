//! MiniStack canary test for the S3 sink.
//!
//! Runs the same end-to-end S3 sink flow as `s3_e2e_tests.rs` but against
//! MiniStack (a LocalStack-alternative AWS emulator). Catches regressions
//! that MinIO might mask — region detection, AWS-specific error semantics,
//! IAM-flavoured auth flows that real AWS exercises.
//!
//! Scope is intentionally narrow (one test): MinIO is the workhorse for
//! integration / chaos testing; MiniStack is the independent verification
//! gate. If this test diverges from the MinIO equivalent, we have a real
//! AWS-fidelity problem to investigate.
//!
//! Run with: `cargo test -p runner --test s3_ministack_canary -- --ignored`

use std::sync::Arc;

use anyhow::Result;
use arrow_array::{
    Array, BooleanArray, Decimal128Array, Int64Array, StringArray,
};
use async_trait::async_trait;
use deltaforge_config::{
    S3Compression, S3Durability, S3FileFormat, S3FileRoll, S3SinkCfg, SinkCfg,
};
use deltaforge_core::encoding::avro_types::TypeConversionOpts;
use deltaforge_core::{Event, Op, Sink, SourceInfo, SourcePosition};
use object_store::path::Path;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::async_reader::ParquetObjectReader;
use runner::{
    ArcSchemaProvider, ColumnSchemaInfo, SchemaLookupError, SchemaProvider,
    TableSchemaInfo, build_arrow_schema_resolver,
};
use serde_json::json;
use sinks::s3::{ObjectStoreParams, build_object_store, build_s3_sink};
use tokio_util::sync::CancellationToken;

mod ministack;
use ministack::{BUCKET, MS_KEY, MS_SECRET, ministack};

// =============================================================================
// Test fixtures (parallel to s3_e2e_tests.rs — kept inline for canary isolation)
// =============================================================================

struct FakeSchemaProvider {
    by_table: std::collections::HashMap<String, TableSchemaInfo>,
}

#[async_trait]
impl SchemaProvider for FakeSchemaProvider {
    async fn get_table_schema(
        &self,
        table: &str,
    ) -> Result<Option<TableSchemaInfo>, SchemaLookupError> {
        Ok(self.by_table.get(table).cloned())
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

fn fake_provider() -> ArcSchemaProvider {
    let mut by_table = std::collections::HashMap::new();
    // Keyed by the db-qualified name ("db.table") to match the real schema
    // registry and the S3 sink resolver's lookup (events carry db="shop").
    by_table.insert(
        "shop.orders".to_string(),
        TableSchemaInfo {
            database: "shop".into(),
            table: "orders".into(),
            columns: vec![
                col("id", "bigint", "bigint", false, None, None),
                col("name", "varchar", "varchar(50)", true, None, None),
                col(
                    "amount",
                    "decimal",
                    "decimal(10,2)",
                    true,
                    Some(10),
                    Some(2),
                ),
                col("paid", "boolean", "boolean", false, None, None),
            ],
            primary_key: vec!["id".into()],
        },
    );
    Arc::new(FakeSchemaProvider { by_table })
}

fn make_event(id: i64, name: &str, amount: &str, paid: bool) -> Event {
    let ts_ms = chrono::NaiveDate::from_ymd_opt(2026, 5, 19)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_millis();
    Event {
        before: None,
        after: Some(json!({
            "id": id,
            "name": name,
            "amount": amount,
            "paid": paid,
        })),
        source: SourceInfo {
            version: "1".into(),
            connector: "mysql".into(),
            name: "canary".into(),
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
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn ministack_canary_parquet_roundtrip() -> Result<()> {
    let infra = ministack().await;

    let cfg = SinkCfg::S3(S3SinkCfg {
        id: "canary-s3".into(),
        bucket: BUCKET.into(),
        prefix: "canary".into(),
        region: Some("us-east-1".into()),
        endpoint: Some(infra.endpoint.clone()),
        access_key_id: Some(MS_KEY.into()),
        secret_access_key: Some(MS_SECRET.into()),
        session_token: None,
        access_key_id_ref: None,
        secret_access_key_ref: None,
        session_token_ref: None,
        virtual_hosted_style: false,
        local: false,
        format: S3FileFormat::Parquet,
        compression: S3Compression::Snappy,
        file_roll: S3FileRoll {
            max_bytes: 256 * 1024 * 1024,
            max_events: 3,
            max_age_secs: 300,
            idle_age_secs: 600,
        },
        send_timeout_secs: 60,
        required: Some(true),
        // Mirrors s3_e2e_tests: the legacy rolling sink (`build_s3_sink`);
        // the default (`durable_v2`) is built by the durable path instead.
        durability: S3Durability::LegacyRolling,
        filter: None,
    });

    let SinkCfg::S3(ref s3_cfg) = cfg else {
        unreachable!()
    };
    let resolver = build_arrow_schema_resolver(
        fake_provider(),
        "mysql",
        TypeConversionOpts::default(),
    );
    let sink = build_s3_sink(
        s3_cfg,
        CancellationToken::new(),
        "canary",
        Some(resolver),
        &sinks::ResolvedSinkCreds::default(),
    )?;

    // 3 events → exactly one rolled file via max_events.
    let events = vec![
        make_event(1, "alpha", "10.00", true),
        make_event(2, "beta", "20.50", false),
        make_event(3, "gamma", "0.01", true),
    ];
    sink.send_batch(&events).await?;

    // Verify via the same object_store path the sink used.
    let store = build_object_store(&ObjectStoreParams::s3_compatible(
        BUCKET,
        &infra.endpoint,
        MS_KEY,
        MS_SECRET,
    ))?;
    let listed: Vec<_> = futures::TryStreamExt::try_collect::<Vec<_>>(
        store.list(Some(&Path::from("canary"))),
    )
    .await?;
    assert_eq!(listed.len(), 1, "single rolled Parquet file on MiniStack");
    let obj = &listed[0];
    assert!(
        obj.location
            .as_ref()
            .contains("table=orders/year=2026/month=05/day=19/")
    );
    assert!(obj.location.as_ref().ends_with(".parquet"));

    let reader = ParquetObjectReader::new(store.clone(), obj.location.clone())
        .with_file_size(obj.size);
    let stream = ParquetRecordBatchStreamBuilder::new(reader)
        .await?
        .build()?;
    let batches: Vec<_> =
        futures::TryStreamExt::try_collect::<Vec<_>>(stream).await?;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);

    let b = &batches[0];
    // Same correctness assertions as the MinIO test — proves the AWS-shaped
    // emulator behaves identically.
    let after_id = b
        .column_by_name("after_id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(after_id.value(0), 1);
    assert_eq!(after_id.value(2), 3);

    let amount = b
        .column_by_name("after_amount")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(amount.value(0), 1000);
    assert_eq!(amount.value(1), 2050);
    assert_eq!(amount.value(2), 1);

    let paid = b
        .column_by_name("after_paid")
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(paid.value(0));
    assert!(!paid.value(1));

    let name = b
        .column_by_name("after_name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(name.value(0), "alpha");

    Ok(())
}
