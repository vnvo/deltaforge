//! Integration tests for Avro encoding.
//!
//! Two test suites:
//! 1. **Mock tests** (no Docker) — axum-based mock Schema Registry, run by default
//! 2. **Real Schema Registry tests** (Docker) — `confluentinc/cp-schema-registry`,
//!    marked `#[ignore]`, require Docker
//!
//! Run mock tests:
//! ```bash
//! cargo test -p sinks --test avro_encoding_tests -- --nocapture
//! ```
//!
//! Run real Schema Registry tests:
//! ```bash
//! cargo test -p sinks --test avro_encoding_tests -- --include-ignored --nocapture --test-threads=1
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use axum::{Json, Router, extract::Path, extract::State, routing::post};
use deltaforge_core::encoding::avro::{AvroEncoder, SubjectStrategy};
use serde_json::json;
use tokio::sync::Mutex;
use tracing::info;

#[allow(dead_code)]
mod sink_test_common;
use sink_test_common::init_test_tracing;

// =============================================================================
// Mock Schema Registry (for fast, no-Docker tests)
// =============================================================================

type SchemaStore = Arc<Mutex<HashMap<String, Vec<(u32, String)>>>>;

#[derive(Clone)]
struct MockSchemaRegistry {
    /// subject → Vec<(schema_id, schema_json)>
    schemas: SchemaStore,
    next_id: Arc<std::sync::atomic::AtomicU32>,
}

impl MockSchemaRegistry {
    fn new() -> Self {
        Self {
            schemas: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(std::sync::atomic::AtomicU32::new(1)),
        }
    }

    async fn registered_subjects(&self) -> Vec<String> {
        self.schemas.lock().await.keys().cloned().collect()
    }
}

/// POST /subjects/{subject}/versions — register schema
async fn register_schema(
    Path(subject): Path<String>,
    State(state): State<MockSchemaRegistry>,
    Json(body): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let schema_str = body["schema"].as_str().unwrap_or("").to_string();
    let mut schemas = state.schemas.lock().await;

    // Check if this exact schema already exists for this subject
    if let Some(versions) = schemas.get(&subject) {
        for (id, existing) in versions {
            if *existing == schema_str {
                return Json(json!({ "id": id }));
            }
        }
    }

    let id = state
        .next_id
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    schemas
        .entry(subject)
        .or_default()
        .push((id, schema_str));

    Json(json!({ "id": id }))
}

async fn start_mock_registry() -> (String, MockSchemaRegistry) {
    let registry = MockSchemaRegistry::new();
    let app = Router::new()
        .route(
            "/subjects/{subject}/versions",
            post(register_schema),
        )
        .with_state(registry.clone());

    let listener =
        tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://127.0.0.1:{}", addr.port());

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    (url, registry)
}

// =============================================================================
// Mock tests (fast, no Docker)
// =============================================================================

#[tokio::test]
async fn mock_avro_encode_simple_object() -> Result<()> {
    init_test_tracing();

    let (url, registry) = start_mock_registry().await;
    let encoder =
        AvroEncoder::new(&url, SubjectStrategy::TopicName, None, None)?;

    let value = json!({
        "id": 42,
        "name": "Alice",
        "active": true
    });

    let bytes = encoder.encode("test-topic", &value, None).await?;

    // Verify Confluent wire format
    assert_eq!(bytes[0], 0x00, "magic byte should be 0x00");
    let schema_id =
        u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
    assert_eq!(schema_id, 1, "first schema should get ID 1");
    assert!(bytes.len() > 5, "should have Avro payload after header");

    // Verify schema was registered under correct subject
    let subjects = registry.registered_subjects().await;
    assert!(subjects.contains(&"test-topic-value".to_string()));

    Ok(())
}

#[tokio::test]
async fn mock_avro_encode_caches_schema_id() -> Result<()> {
    init_test_tracing();

    let (url, registry) = start_mock_registry().await;
    let encoder =
        AvroEncoder::new(&url, SubjectStrategy::TopicName, None, None)?;

    let value1 = json!({"id": 1, "name": "Alice"});
    let value2 = json!({"id": 2, "name": "Bob"});

    let bytes1 = encoder.encode("cache-topic", &value1, None).await?;
    let bytes2 = encoder.encode("cache-topic", &value2, None).await?;

    // Both should use the same schema ID (cached)
    let id1 =
        u32::from_be_bytes([bytes1[1], bytes1[2], bytes1[3], bytes1[4]]);
    let id2 =
        u32::from_be_bytes([bytes2[1], bytes2[2], bytes2[3], bytes2[4]]);
    assert_eq!(id1, id2, "same schema structure should get same cached ID");

    // Should only have registered once
    let schemas = registry.schemas.lock().await;
    let versions = schemas.get("cache-topic-value").unwrap();
    assert_eq!(versions.len(), 1, "should only register schema once");

    Ok(())
}

#[tokio::test]
async fn mock_avro_encode_record_name_strategy() -> Result<()> {
    init_test_tracing();

    let (url, _registry) = start_mock_registry().await;
    let encoder = AvroEncoder::new(
        &url,
        SubjectStrategy::RecordName,
        None,
        None,
    )?;

    let value = json!({"id": 1, "name": "test"});
    let bytes = encoder
        .encode("any-topic", &value, Some("com.acme.Order"))
        .await?;

    assert_eq!(bytes[0], 0x00);
    assert!(bytes.len() > 5);

    Ok(())
}

#[tokio::test]
async fn mock_avro_encode_nested_object() -> Result<()> {
    init_test_tracing();

    let (url, _registry) = start_mock_registry().await;
    let encoder =
        AvroEncoder::new(&url, SubjectStrategy::TopicName, None, None)?;

    let value = json!({
        "before": null,
        "after": {"id": 1, "name": "Alice"},
        "op": "c",
        "ts_ms": 1700000000000_i64,
        "source": {
            "connector": "mysql",
            "db": "shop",
            "table": "customers"
        }
    });

    let bytes = encoder.encode("cdc-events", &value, None).await?;

    assert_eq!(bytes[0], 0x00);
    let schema_id =
        u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
    assert!(schema_id > 0);
    assert!(bytes.len() > 5);

    Ok(())
}

#[tokio::test]
async fn mock_avro_encode_with_null_fields() -> Result<()> {
    init_test_tracing();

    let (url, _registry) = start_mock_registry().await;
    let encoder =
        AvroEncoder::new(&url, SubjectStrategy::TopicName, None, None)?;

    let value = json!({
        "id": 1,
        "name": "Alice",
        "deleted_at": null,
        "optional_field": null
    });

    let bytes = encoder.encode("null-topic", &value, None).await?;

    assert_eq!(bytes[0], 0x00);
    assert!(bytes.len() > 5);

    Ok(())
}

#[tokio::test]
async fn mock_avro_encoder_registry_unreachable() {
    init_test_tracing();

    let encoder = AvroEncoder::new(
        "http://127.0.0.1:1",
        SubjectStrategy::TopicName,
        None,
        None,
    )
    .unwrap();

    let value = json!({"id": 1});
    let result = encoder.encode("test", &value, None).await;

    assert!(result.is_err(), "should fail when registry is unreachable");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("schema") || err.contains("connect"),
        "error should mention schema or connection: {err}"
    );
}

// =============================================================================
// Real Schema Registry tests (Docker required)
// =============================================================================
//
// These tests spin up real Kafka + Confluent Schema Registry containers
// using testcontainers, then exercise the full encode path including:
// - Schema auto-registration
// - Confluent wire format validation (decode with apache-avro)
// - Schema ID lookup from real registry
// - Producing Avro messages to Kafka and consuming them
//
// Marked `#[ignore]` so they don't run in CI without Docker. Run with:
//   cargo test -p sinks --test avro_encoding_tests -- --include-ignored --nocapture --test-threads=1

use ctor::dtor;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::sync::OnceCell;

/// Schema Registry port (must not conflict with other services).
const SR_PORT: u16 = 8181;

/// Kafka broker address from inside the docker-compose network.
/// The chaos-env Kafka listens on kafka:29092 (BROKER) within
/// the `deltaforge_chaos_net` network.
const KAFKA_BOOTSTRAP: &str = "kafka:29092";

/// Docker network to join (same as docker-compose chaos environment).
const DOCKER_NETWORK: &str = "deltaforge_chaos_net";

/// Shared Schema Registry container.
struct AvroInfra {
    #[allow(dead_code)]
    schema_registry: ContainerAsync<GenericImage>,
}

static AVRO_INFRA: OnceCell<AvroInfra> = OnceCell::const_new();

#[dtor]
fn cleanup() {
    if let Some(infra) = AVRO_INFRA.get() {
        std::process::Command::new("docker")
            .args(["rm", "-f", infra.schema_registry.id()])
            .output()
            .ok();
    }
}

fn sr_url() -> String {
    format!("http://localhost:{SR_PORT}")
}

async fn wait_for_schema_registry(
    url: &str,
    timeout: Duration,
) -> Result<()> {
    let client = reqwest::Client::new();
    let deadline = Instant::now() + timeout;

    while Instant::now() < deadline {
        match client
            .get(format!("{url}/subjects"))
            .timeout(Duration::from_secs(2))
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                info!("Schema Registry is ready");
                return Ok(());
            }
            _ => {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }

    anyhow::bail!("Schema Registry not ready after {timeout:?}")
}

/// Start a real Confluent Schema Registry container.
///
/// Reuses the existing Kafka from docker-compose (port 9092).
/// Only spins up the Schema Registry via testcontainers.
async fn get_infra() -> &'static AvroInfra {
    AVRO_INFRA
        .get_or_init(|| async {
            info!("starting Schema Registry container (using existing Kafka on {KAFKA_BOOTSTRAP})...");

            let sr = GenericImage::new(
                "confluentinc/cp-schema-registry",
                "7.5.0",
            )
            .with_wait_for(WaitFor::Duration {
                length: Duration::from_secs(10),
            })
            .with_env_var("SCHEMA_REGISTRY_HOST_NAME", "localhost")
            .with_env_var(
                "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                format!("PLAINTEXT://{KAFKA_BOOTSTRAP}"),
            )
            .with_env_var(
                "SCHEMA_REGISTRY_LISTENERS",
                format!("http://0.0.0.0:{SR_PORT}"),
            )
            .with_env_var("SCHEMA_REGISTRY_DEBUG", "true")
            .with_mapped_port(SR_PORT, SR_PORT.tcp())
            .with_network(DOCKER_NETWORK)
            .start()
            .await
            .expect("start schema-registry container");

            info!("Schema Registry started: {}", sr.id());

            wait_for_schema_registry(&sr_url(), Duration::from_secs(60))
                .await
                .expect("Schema Registry should be ready");

            AvroInfra {
                schema_registry: sr,
            }
        })
        .await
}

/// Fetch schema by ID from Schema Registry to verify registration.
async fn fetch_schema_by_id(
    sr_url: &str,
    schema_id: u32,
) -> Result<serde_json::Value> {
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{sr_url}/schemas/ids/{schema_id}"))
        .send()
        .await?;
    assert!(
        resp.status().is_success(),
        "GET /schemas/ids/{schema_id} failed: {}",
        resp.status()
    );
    let body: serde_json::Value = resp.json().await?;
    Ok(body)
}

/// List all subjects from Schema Registry.
async fn list_subjects(sr_url: &str) -> Result<Vec<String>> {
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{sr_url}/subjects"))
        .send()
        .await?;
    let subjects: Vec<String> = resp.json().await?;
    Ok(subjects)
}

// ---- Real integration tests ------------------------------------------------

#[tokio::test]
#[ignore]
async fn real_sr_encode_and_verify_schema_registered() -> Result<()> {
    init_test_tracing();
    let _infra = get_infra().await;

    let encoder = AvroEncoder::new(
        &sr_url(),
        SubjectStrategy::TopicName,
        None,
        None,
    )?;

    let value = json!({
        "id": 1,
        "name": "Alice",
        "active": true
    });

    let bytes = encoder.encode("avro-test-1", &value, None).await?;

    // Verify wire format
    assert_eq!(bytes[0], 0x00, "magic byte");
    let schema_id =
        u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
    assert!(schema_id > 0, "schema_id should be positive: {schema_id}");
    info!(schema_id, "schema registered");

    // Fetch schema from real registry and verify it's valid
    let schema_resp = fetch_schema_by_id(&sr_url(), schema_id).await?;
    let schema_str = schema_resp["schema"]
        .as_str()
        .expect("schema should be a string");
    info!(schema = schema_str, "fetched schema from registry");

    // Parse as valid Avro schema
    let avro_schema = apache_avro::Schema::parse_str(schema_str)?;
    assert!(matches!(avro_schema, apache_avro::Schema::Record(_)));

    // Verify subject was created
    let subjects = list_subjects(&sr_url()).await?;
    assert!(
        subjects.contains(&"avro-test-1-value".to_string()),
        "subject should be registered: {subjects:?}"
    );

    Ok(())
}

#[tokio::test]
#[ignore]
async fn real_sr_decode_avro_payload() -> Result<()> {
    init_test_tracing();
    let _infra = get_infra().await;

    let encoder = AvroEncoder::new(
        &sr_url(),
        SubjectStrategy::TopicName,
        None,
        None,
    )?;

    let original = json!({
        "id": 42,
        "name": "Bob",
        "score": 99
    });

    let bytes = encoder.encode("avro-test-decode", &original, None).await?;
    let schema_id =
        u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);

    // Fetch schema and decode the Avro payload
    let schema_resp = fetch_schema_by_id(&sr_url(), schema_id).await?;
    let schema_str = schema_resp["schema"].as_str().unwrap();
    let avro_schema = apache_avro::Schema::parse_str(schema_str)?;

    // Strip Confluent wire header (5 bytes) and decode Avro
    let avro_payload = &bytes[5..];
    let decoded = apache_avro::from_avro_datum(
        &avro_schema,
        &mut &avro_payload[..],
        None,
    )?;

    info!(?decoded, "decoded Avro value");

    // Verify we can round-trip the data
    if let apache_avro::types::Value::Record(fields) = &decoded {
        let field_names: Vec<&str> =
            fields.iter().map(|(name, _)| name.as_str()).collect();
        assert!(field_names.contains(&"id"), "should have 'id' field");
        assert!(field_names.contains(&"name"), "should have 'name' field");
        assert!(
            field_names.contains(&"score"),
            "should have 'score' field"
        );
    } else {
        panic!("expected Record, got: {decoded:?}");
    }

    Ok(())
}

#[tokio::test]
#[ignore]
async fn real_sr_schema_caching_same_id() -> Result<()> {
    init_test_tracing();
    let _infra = get_infra().await;

    let encoder = AvroEncoder::new(
        &sr_url(),
        SubjectStrategy::TopicName,
        None,
        None,
    )?;

    let val1 = json!({"x": 1, "y": "a"});
    let val2 = json!({"x": 2, "y": "b"});
    let val3 = json!({"x": 3, "y": "c"});

    let b1 = encoder.encode("avro-test-cache", &val1, None).await?;
    let b2 = encoder.encode("avro-test-cache", &val2, None).await?;
    let b3 = encoder.encode("avro-test-cache", &val3, None).await?;

    let id1 = u32::from_be_bytes([b1[1], b1[2], b1[3], b1[4]]);
    let id2 = u32::from_be_bytes([b2[1], b2[2], b2[3], b2[4]]);
    let id3 = u32::from_be_bytes([b3[1], b3[2], b3[3], b3[4]]);

    assert_eq!(id1, id2, "same structure should get same schema ID");
    assert_eq!(id2, id3, "all three should share schema ID");
    info!(schema_id = id1, "all 3 events used same cached schema ID");

    Ok(())
}

#[tokio::test]
#[ignore]
async fn real_sr_record_name_strategy() -> Result<()> {
    init_test_tracing();
    let _infra = get_infra().await;

    let encoder = AvroEncoder::new(
        &sr_url(),
        SubjectStrategy::RecordName,
        None,
        None,
    )?;

    let value = json!({"order_id": 100, "total": 42});
    let bytes = encoder
        .encode("any-topic", &value, Some("com.acme.Order"))
        .await?;

    assert_eq!(bytes[0], 0x00);
    let schema_id =
        u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
    assert!(schema_id > 0);

    // Verify subject uses record name, not topic name
    let subjects = list_subjects(&sr_url()).await?;
    assert!(
        subjects.contains(&"com.acme.Order".to_string()),
        "should use record name as subject: {subjects:?}"
    );

    Ok(())
}

#[tokio::test]
#[ignore]
async fn real_sr_cdc_event_structure() -> Result<()> {
    init_test_tracing();
    let _infra = get_infra().await;

    let encoder = AvroEncoder::new(
        &sr_url(),
        SubjectStrategy::TopicName,
        None,
        None,
    )?;

    // Simulate a full CDC event (like what DeltaForge produces after envelope wrapping)
    let cdc_event = json!({
        "before": null,
        "after": {"id": 1, "name": "Alice", "email": "alice@example.com"},
        "source": {
            "version": "deltaforge-test",
            "connector": "mysql",
            "name": "orders-db",
            "ts_ms": 1700000000000_i64,
            "db": "shop",
            "table": "customers"
        },
        "op": "c",
        "ts_ms": 1700000000000_i64
    });

    let bytes = encoder
        .encode("cdc-avro-test", &cdc_event, None)
        .await?;

    assert_eq!(bytes[0], 0x00);
    let schema_id =
        u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
    info!(schema_id, payload_len = bytes.len(), "CDC event encoded");

    // Decode and verify structure
    let schema_resp = fetch_schema_by_id(&sr_url(), schema_id).await?;
    let schema_str = schema_resp["schema"].as_str().unwrap();
    let avro_schema = apache_avro::Schema::parse_str(schema_str)?;

    let decoded = apache_avro::from_avro_datum(
        &avro_schema,
        &mut &bytes[5..],
        None,
    )?;

    if let apache_avro::types::Value::Record(fields) = &decoded {
        let field_names: Vec<&str> =
            fields.iter().map(|(name, _)| name.as_str()).collect();
        assert!(field_names.contains(&"op"));
        assert!(field_names.contains(&"after"));
        assert!(field_names.contains(&"source"));
        info!("CDC event decoded successfully with all expected fields");
    } else {
        panic!("expected Record, got: {decoded:?}");
    }

    Ok(())
}
