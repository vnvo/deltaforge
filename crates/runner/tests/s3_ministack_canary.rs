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
use std::time::Duration;

use anyhow::Result;
use arrow_array::{
    Array, BooleanArray, Decimal128Array, Int64Array, StringArray,
};
use async_trait::async_trait;
use ctor::dtor;
use deltaforge_config::{
    S3Compression, S3FileFormat, S3FileRoll, S3SinkCfg, SinkCfg,
};
use deltaforge_core::encoding::avro_types::TypeConversionOpts;
use deltaforge_core::{Event, Op, Sink, SourceInfo, SourcePosition};
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
use testcontainers::{ContainerAsync, GenericImage};
use tokio::sync::OnceCell;
use tokio_util::sync::CancellationToken;

// =============================================================================
// MiniStack testcontainer
// =============================================================================

const MS_PORT: u16 = 4566;
const MS_KEY: &str = "test";
const MS_SECRET: &str = "test";
const BUCKET: &str = "deltaforge-canary";

struct MinistackInfra {
    #[allow(dead_code)]
    container: ContainerAsync<GenericImage>,
    endpoint: String,
}

static MINISTACK: OnceCell<MinistackInfra> = OnceCell::const_new();

#[dtor]
fn cleanup_ministack() {
    if let Some(infra) = MINISTACK.get() {
        std::process::Command::new("docker")
            .args(["rm", "-f", infra.container.id()])
            .output()
            .ok();
    }
}

async fn ministack() -> &'static MinistackInfra {
    MINISTACK
        .get_or_init(|| async {
            let container =
                GenericImage::new("ministackorg/ministack", "latest")
                    .with_wait_for(WaitFor::seconds(3))
                    .with_exposed_port(MS_PORT.tcp())
                    .start()
                    .await
                    .expect("start MiniStack");
            let host = container.get_host().await.expect("ministack host");
            let port = container
                .get_host_port_ipv4(MS_PORT)
                .await
                .expect("ministack port");
            let endpoint = format!("http://{host}:{port}");
            wait_for_http(&endpoint, Duration::from_secs(60))
                .await
                .expect("MiniStack ready");
            ensure_bucket(&endpoint)
                .await
                .expect("create canary bucket");
            MinistackInfra {
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
        // MiniStack accepts any path on 4566 and replies with S3 XML if
        // it's up. A simple HEAD on /healthz or root with an OK-ish status
        // is enough.
        if let Ok(resp) = client
            .get(endpoint)
            .timeout(Duration::from_secs(2))
            .send()
            .await
            && resp.status().as_u16() < 500
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    anyhow::bail!("MiniStack never became ready at {endpoint}")
}

/// Create the test bucket via a SigV4-signed PUT through object_store's
/// own client. Easiest path: use object_store::aws::AmazonS3 to issue an
/// `put_opts` on the bucket root (object_store doesn't expose CreateBucket
/// directly, but PUTs to `s3://bucket/` are idempotent on most emulators
/// — both MiniStack and MinIO accept this). If the emulator returns
/// `BucketAlreadyOwnedByYou` (409) or similar, we treat it as success.
async fn ensure_bucket(endpoint: &str) -> Result<()> {
    // Drop down to a raw SigV4 client via reqwest. Implementing SigV4 by
    // hand is the simplest dependency-free path for the canary. We use the
    // path-style URL.
    use chrono::Utc;
    use hmac::{Hmac, Mac};
    use sha2::{Digest, Sha256};

    type HmacSha256 = Hmac<Sha256>;

    let now = Utc::now();
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let date_stamp = now.format("%Y%m%d").to_string();
    let region = "us-east-1";
    let service = "s3";

    let url = url::Url::parse(endpoint)?;
    let host =
        format!("{}:{}", url.host_str().unwrap(), url.port().unwrap_or(80));
    let canonical_uri = format!("/{BUCKET}");
    let canonical_querystring = "";
    let payload_hash = hex::encode(Sha256::digest(b""));
    let canonical_headers = format!(
        "host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n"
    );
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let canonical_request = format!(
        "PUT\n{canonical_uri}\n{canonical_querystring}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
    );
    let credential_scope =
        format!("{date_stamp}/{region}/{service}/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{}",
        hex::encode(Sha256::digest(canonical_request.as_bytes()))
    );

    let k_date =
        sign(format!("AWS4{MS_SECRET}").as_bytes(), date_stamp.as_bytes());
    let k_region = sign(&k_date, region.as_bytes());
    let k_service = sign(&k_region, service.as_bytes());
    let k_signing = sign(&k_service, b"aws4_request");
    let signature = hex::encode(sign(&k_signing, string_to_sign.as_bytes()));

    let auth = format!(
        "AWS4-HMAC-SHA256 Credential={MS_KEY}/{credential_scope}, \
         SignedHeaders={signed_headers}, Signature={signature}"
    );

    let put_url = format!("{endpoint}{canonical_uri}");
    let resp = reqwest::Client::new()
        .put(&put_url)
        .header("x-amz-date", &amz_date)
        .header("x-amz-content-sha256", &payload_hash)
        .header("Authorization", &auth)
        .send()
        .await?;
    let status = resp.status().as_u16();
    if status == 200 || status == 409 {
        return Ok(());
    }
    let body = resp.text().await.unwrap_or_default();
    anyhow::bail!("create bucket failed: status={status}, body={body}");

    fn sign(key: &[u8], data: &[u8]) -> Vec<u8> {
        let mut mac = HmacSha256::new_from_slice(key).unwrap();
        mac.update(data);
        mac.finalize().into_bytes().to_vec()
    }
}

// =============================================================================
// Test fixtures (parallel to s3_e2e_tests.rs — kept inline for canary isolation)
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

fn fake_provider() -> ArcSchemaProvider {
    let mut by_table = std::collections::HashMap::new();
    by_table.insert(
        "orders".to_string(),
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
        checkpoint: None,
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
    )?;

    // 3 events → exactly one rolled file via max_events.
    let events = vec![
        make_event(1, "alpha", "10.00", true),
        make_event(2, "beta", "20.50", false),
        make_event(3, "gamma", "0.01", true),
    ];
    sink.send_batch(&events).await?;

    // Verify via the same object_store path the sink used.
    let store = build_object_store(&ObjectStoreParams::s3_minio(
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
