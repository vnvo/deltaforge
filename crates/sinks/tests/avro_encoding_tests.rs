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
    /// A registration whose schema contains the marker is answered only
    /// after the delay (orders concurrent registrations).
    delays: Arc<std::sync::Mutex<Vec<(String, Duration)>>>,
    /// When set, every registration is answered with this HTTP status.
    fail_status: Arc<std::sync::Mutex<Option<u16>>>,
    /// When set, registrations under this subject are rejected with 409.
    reject_subject: Arc<std::sync::Mutex<Option<String>>>,
}

impl MockSchemaRegistry {
    fn new() -> Self {
        Self {
            schemas: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(std::sync::atomic::AtomicU32::new(1)),
            delays: Arc::default(),
            fail_status: Arc::default(),
            reject_subject: Arc::default(),
        }
    }

    /// The schema registered with `id`.
    async fn schema_by_id(&self, id: u32) -> String {
        self.schemas
            .lock()
            .await
            .values()
            .flatten()
            .find(|(i, _)| *i == id)
            .map(|(_, s)| s.clone())
            .unwrap_or_else(|| panic!("no schema with id {id}"))
    }

    async fn versions(&self, subject: &str) -> usize {
        self.schemas.lock().await.get(subject).map_or(0, Vec::len)
    }

    #[allow(dead_code)]
    async fn registered_subjects(&self) -> Vec<String> {
        self.schemas.lock().await.keys().cloned().collect()
    }
}

/// POST /subjects/{subject}/versions — register schema
async fn register_schema(
    Path(subject): Path<String>,
    State(state): State<MockSchemaRegistry>,
    Json(body): Json<serde_json::Value>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let rejected =
        state.reject_subject.lock().unwrap().as_deref() == Some(&subject);
    let fail = if rejected {
        Some(409)
    } else {
        *state.fail_status.lock().unwrap()
    };
    if let Some(status) = fail {
        let status = axum::http::StatusCode::from_u16(status).unwrap();
        let code = u32::from(status.as_u16()) * 100;
        return (
            status,
            Json(json!({"error_code": code, "message": "mock failure"})),
        )
            .into_response();
    }
    register_ok(subject, state, body).await.into_response()
}

async fn register_ok(
    subject: String,
    state: MockSchemaRegistry,
    body: serde_json::Value,
) -> Json<serde_json::Value> {
    let schema_str = body["schema"].as_str().unwrap_or("").to_string();
    let delay = state
        .delays
        .lock()
        .unwrap()
        .iter()
        .find(|(marker, _)| schema_str.contains(marker.as_str()))
        .map(|(_, d)| *d);
    if let Some(d) = delay {
        tokio::time::sleep(d).await;
    }
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

    schemas.entry(subject).or_default().push((id, schema_str));

    Json(json!({ "id": id }))
}

/// GET /subjects/{subject}/versions/latest
async fn latest_schema(
    Path(subject): Path<String>,
    State(state): State<MockSchemaRegistry>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    match state
        .schemas
        .lock()
        .await
        .get(&subject)
        .and_then(|v| v.last())
    {
        Some((id, schema)) => {
            Json(json!({"subject": subject, "id": id, "schema": schema}))
                .into_response()
        }
        None => (
            axum::http::StatusCode::NOT_FOUND,
            Json(json!({"error_code": 40401, "message": "Subject not found"})),
        )
            .into_response(),
    }
}

async fn start_mock_registry() -> (String, MockSchemaRegistry) {
    let registry = MockSchemaRegistry::new();
    let app = Router::new()
        .route("/subjects/{subject}/versions", post(register_schema))
        .route(
            "/subjects/{subject}/versions/latest",
            axum::routing::get(latest_schema),
        )
        .with_state(registry.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
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
    let id1 = u32::from_be_bytes([bytes1[1], bytes1[2], bytes1[3], bytes1[4]]);
    let id2 = u32::from_be_bytes([bytes2[1], bytes2[2], bytes2[3], bytes2[4]]);
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
    let encoder =
        AvroEncoder::new(&url, SubjectStrategy::RecordName, None, None)?;

    let value = json!({"id": 1, "name": "test"});
    let bytes = encoder
        .encode("any-topic", &value, Some("com.acme.Order"))
        .await?;

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
// DDL-derived envelope schema tests (mock SR, realistic MySQL schemas)
// =============================================================================
//
// These tests model the exact table schemas from the soak test suite and
// exercise the full DDL → Avro type conversion → envelope schema → encode →
// register → decode path. They catch type mismatches (Int vs Long, Decimal
// encoding, TimestampMillis, nullable unions, Ref nodes) that simple unit
// tests miss.

use deltaforge_core::encoding::avro_schema::{
    build_envelope_schema, build_value_schema,
};
use deltaforge_core::encoding::avro_types::{
    ColumnDesc, TypeConversionOpts, mysql_column_to_avro,
};

/// Build envelope schema from MySQL column definitions, encode a CDC event,
/// register with mock SR, and decode back. Panics on any failure.
async fn ddl_roundtrip(
    table: &str,
    columns: Vec<ColumnDesc>,
    after_json: serde_json::Value,
) {
    let opts = TypeConversionOpts::default();
    let fields: Vec<serde_json::Value> = columns
        .iter()
        .map(|c| mysql_column_to_avro(c, &opts))
        .collect();

    let value_schema = build_value_schema("mysql", "orders", table, fields);
    let (schema_json, _schema) =
        build_envelope_schema("mysql", "orders", table, value_schema)
            .unwrap_or_else(|e| {
                panic!("envelope schema build failed for {table}: {e}")
            });

    let (url, _registry) = start_mock_registry().await;
    let encoder =
        AvroEncoder::new(&url, SubjectStrategy::TopicName, None, None).unwrap();

    // Build a CDC INSERT event
    let event = json!({
        "before": null,
        "after": after_json,
        "source": {
            "version": "deltaforge-test",
            "connector": "mysql",
            "name": "test-pipeline",
            "ts_ms": 1700000000000_i64,
            "db": "orders",
            "schema": null,
            "table": table,
            "snapshot": null,
            "position": null
        },
        "op": "c",
        "ts_ms": 1700000000000_i64,
        "event_id": null,
        "schema_version": null,
        "transaction": null
    });

    let bytes = encoder
        .encode(&format!("cdc.{table}"), &event, None)
        .await
        .unwrap_or_else(|e| {
            panic!("Avro encode failed for {table}: {e}\nSchema: {schema_json}")
        });

    // Verify Confluent wire format
    assert_eq!(bytes[0], 0x00, "{table}: magic byte");
    assert!(bytes.len() > 5, "{table}: payload should exist");
}

fn col(
    name: &str,
    data_type: &str,
    column_type: &str,
    nullable: bool,
) -> ColumnDesc {
    ColumnDesc {
        name: name.to_string(),
        data_type: data_type.to_string(),
        column_type: column_type.to_string(),
        nullable,
        precision: None,
        scale: None,
        unsigned: false,
        is_array: false,
        element_type: None,
    }
}

fn decimal_col(
    name: &str,
    precision: i64,
    scale: i64,
    nullable: bool,
) -> ColumnDesc {
    ColumnDesc {
        precision: Some(precision),
        scale: Some(scale),
        ..col(
            name,
            "decimal",
            &format!("decimal({precision},{scale})"),
            nullable,
        )
    }
}

// =============================================================================
// Realistic binlog output tests
// =============================================================================
//
// These tests use the EXACT JSON that mysql_object::build_object() produces
// for each soak table domain. Values taken directly from production logs:
//   - Long(9205) for INT AUTO_INCREMENT → json!(9205) → Number
//   - String([bytes]) for VARCHAR → "string" (UTF-8) or {"_base64":"..."} (binary)
//   - Blob([bytes]) for TEXT → {"_base64": "..."}
//   - Decimal("9785.6240") → "9785.6240" (string)
//   - Tiny(0) for TINYINT → 0 (small number)
//   - Short(0) for SMALLINT → 0 (small number)
//   - Float(0.0) → 0.0
//   - None → null
//   - Timestamp(1775260449000000) → 1775260449000000 (microseconds!)

#[tokio::test]
async fn ddl_binlog_payment_domain() {
    // Payment domain: INT, VARCHAR, TEXT(blob), DECIMAL, TINYINT, CHAR,
    // VARCHAR, DECIMAL×3, BOOLEAN(tinyint), DATETIME, JSON, TIMESTAMP×2
    // Exact data from production log of soak_payment_01
    ddl_roundtrip(
        "soak_payment_01",
        vec![
            col("id", "int", "int", false),
            col("tag", "varchar", "varchar(100)", false),
            col("data", "text", "text", true),
            decimal_col("value", 10, 4, true),
            col("status", "tinyint", "tinyint", true),
            col("currency", "char", "char(3)", true),
            col("method", "varchar", "varchar(32)", true),
            col("reference", "varchar", "varchar(128)", true),
            decimal_col("fee", 8, 4, true),
            decimal_col("net_amount", 12, 4, true),
            col("refunded", "boolean", "tinyint(1)", true),
            col("processed_at", "datetime", "datetime", true),
            col("metadata", "json", "json", true),
            col("created_at", "timestamp", "timestamp", true),
            col("updated_at", "timestamp", "timestamp", true),
        ],
        // Exact JSON from build_object() output in production:
        json!({
            "id": 9205,                                          // Long → json number
            "tag": "drain-26-1775260448196-52",                  // String (UTF-8)
            "data": {"_base64": "YmFja2xvZy0xNzc1MjYwNDQ4MTk2"}, // Blob → _base64
            "value": "9785.6240",                                // Decimal → string
            "status": 2,                                         // Tiny → small number
            "currency": "USD",                                   // String (UTF-8)
            "method": "",                                        // String (empty)
            "reference": "",                                     // String (empty)
            "fee": "0.0000",                                     // Decimal → string
            "net_amount": "0.0000",                              // Decimal → string
            "refunded": 0,                                       // Tiny(0) for BOOLEAN
            "processed_at": null,                                // None → null
            "metadata": null,                                    // None → null
            "created_at": 1775260448000000_i64,                  // Timestamp (microseconds)
            "updated_at": 1775260448000000_i64                   // Timestamp (microseconds)
        }),
    )
    .await;
}

#[tokio::test]
async fn ddl_binlog_customer_domain() {
    // Customer domain: INT, VARCHAR, TEXT(blob), DECIMAL, TINYINT,
    // VARCHAR×3, BOOLEAN(tinyint), INT, FLOAT, JSON, DATE, DATETIME, TIMESTAMP×2
    ddl_roundtrip(
        "soak_customer_01",
        vec![
            col("id", "int", "int", false),
            col("tag", "varchar", "varchar(100)", false),
            col("data", "text", "text", true),
            decimal_col("value", 10, 4, true),
            col("status", "tinyint", "tinyint", true),
            col("name", "varchar", "varchar(255)", true),
            col("email", "varchar", "varchar(255)", true),
            col("phone", "varchar", "varchar(32)", true),
            col("active", "boolean", "tinyint(1)", true),
            col("loyalty_points", "int", "int", true),
            col("credit_score", "float", "float", true),
            col("preferences", "json", "json", true),
            col("dob", "date", "date", true),
            col("last_login", "datetime", "datetime", true),
            col("created_at", "timestamp", "timestamp", true),
            col("updated_at", "timestamp", "timestamp", true),
        ],
        // Exact patterns from binlog output:
        json!({
            "id": 10241,                                         // Long
            "tag": "drain-19-1775260449024-0",                   // String (UTF-8)
            "data": {"_base64": "YmFja2xvZy0xNzc1MjYwNDQ5MDI0"}, // Blob
            "value": "1932.7713",                                // Decimal
            "status": 2,                                         // Tiny
            "name": "",                                          // String (empty)
            "email": "",                                         // String (empty)
            "phone": "",                                         // String (empty)
            "active": 1,                                         // Tiny(1) for BOOLEAN
            "loyalty_points": 0,                                 // Long
            "credit_score": 0.0,                                 // Float
            "preferences": null,                                 // None (JSON null)
            "dob": null,                                         // None
            "last_login": null,                                  // None
            "created_at": 1775260449000000_i64,                  // Timestamp
            "updated_at": 1775260449000000_i64                   // Timestamp
        }),
    )
    .await;
}

#[tokio::test]
async fn ddl_binlog_order_domain() {
    // Order domain: INT, VARCHAR, TEXT(blob), DECIMAL, TINYINT,
    // CHAR(3), SMALLINT, DECIMAL×2, BOOLEAN, DATETIME×2, TEXT, TIMESTAMP×2
    ddl_roundtrip(
        "soak_order_01",
        vec![
            col("id", "int", "int", false),
            col("tag", "varchar", "varchar(100)", false),
            col("data", "text", "text", true),
            decimal_col("value", 10, 4, true),
            col("status", "tinyint", "tinyint", true),
            col("currency", "char", "char(3)", true),
            col("items_count", "smallint", "smallint", true),
            decimal_col("shipping_cost", 8, 4, true),
            decimal_col("gross_amount", 12, 4, true),
            col("is_gift", "boolean", "tinyint(1)", true),
            col("placed_at", "datetime", "datetime", true),
            col("dispatched_at", "datetime", "datetime", true),
            col("notes", "text", "text", true),
            col("created_at", "timestamp", "timestamp", true),
            col("updated_at", "timestamp", "timestamp", true),
        ],
        json!({
            "id": 5001,
            "tag": "drain-10-1775260449000-0",
            "data": {"_base64": "YmFja2xvZy0xNzc1MjYwNDQ5MDAw"},
            "value": "4500.9900",
            "status": 1,
            "currency": "USD",
            "items_count": 3,                                    // Short → number
            "shipping_cost": "5.9900",
            "gross_amount": "4506.9800",
            "is_gift": 0,                                        // Tiny(0) for BOOLEAN
            "placed_at": null,
            "dispatched_at": null,
            "notes": {"_base64": ""},                             // TEXT blob, empty
            "created_at": 1775260449000000_i64,
            "updated_at": 1775260449000000_i64
        }),
    )
    .await;
}

#[tokio::test]
async fn ddl_binlog_event_domain() {
    // Event domain: INT, VARCHAR, TEXT(blob), DECIMAL, TINYINT,
    // VARCHAR×3, SMALLINT, BIGINT, DATETIME, TEXT, TIMESTAMP×2
    ddl_roundtrip(
        "soak_event_01",
        vec![
            col("id", "int", "int", false),
            col("tag", "varchar", "varchar(100)", false),
            col("data", "text", "text", true),
            decimal_col("value", 10, 4, true),
            col("status", "tinyint", "tinyint", true),
            col("event_type", "varchar", "varchar(32)", true),
            col("source_ip", "varchar", "varchar(45)", true),
            col("session_id", "varchar", "varchar(64)", true),
            col("severity", "smallint", "smallint", true),
            col("duration_ms", "bigint", "bigint", true),
            col("occurred_at", "datetime", "datetime", true),
            col("payload", "text", "text", true),
            col("created_at", "timestamp", "timestamp", true),
            col("updated_at", "timestamp", "timestamp", true),
        ],
        // From binlog: Short(0), Long(0) for severity/duration_ms
        json!({
            "id": 9281,
            "tag": "drain-29-1775260449022-0",
            "data": {"_base64": "YmFja2xvZy0xNzc1MjYwNDQ5MDIy"},
            "value": "352.6975",
            "status": 1,
            "event_type": "",
            "source_ip": "",
            "session_id": "",
            "severity": 0,                                       // Short → number
            "duration_ms": 0,                                    // Long → number
            "occurred_at": null,
            "payload": null,                                     // None (TEXT null)
            "created_at": 1775260449000000_i64,
            "updated_at": 1775260449000000_i64
        }),
    )
    .await;
}

// ── Original payment test kept for backwards compat

#[tokio::test]
async fn ddl_mysql_payment_table() {
    ddl_roundtrip(
        "soak_payment_01",
        vec![
            col("id", "int", "int", false),
            col("tag", "varchar", "varchar(100)", false),
            col("data", "text", "text", true),
            decimal_col("value", 10, 4, true),
            col("status", "tinyint", "tinyint", true),
            col("currency", "char", "char(3)", true),
            col("method", "varchar", "varchar(32)", true),
            col("reference", "varchar", "varchar(128)", true),
            decimal_col("fee", 8, 4, true),
            decimal_col("net_amount", 12, 4, true),
            col("refunded", "boolean", "tinyint(1)", true),
            col("processed_at", "datetime", "datetime", true),
            col("metadata", "json", "json", true),
            col("created_at", "timestamp", "timestamp", true),
            col("updated_at", "timestamp", "timestamp", true),
        ],
        json!({
            "id": 9205,
            "tag": "drain-26-1775260448196-52",
            "data": {"_base64": "YmFja2xvZy0xNzc1MjYwNDQ4MTk2"},
            "value": "9785.6240",
            "status": 2,
            "currency": "USD",
            "method": "",
            "reference": "",
            "fee": "0.0000",
            "net_amount": "0.0000",
            "refunded": false,
            "processed_at": null,
            "metadata": null,
            "created_at": 1775260448000_i64,
            "updated_at": 1775260448000_i64
        }),
    )
    .await;
}

// ── DELETE event: before has data, after is null

#[tokio::test]
async fn ddl_mysql_delete_event() {
    let opts = TypeConversionOpts::default();
    let columns = [
        col("id", "int", "int", false),
        col("name", "varchar", "varchar(255)", true),
    ];
    let fields: Vec<serde_json::Value> = columns
        .iter()
        .map(|c| mysql_column_to_avro(c, &opts))
        .collect();

    let value_schema = build_value_schema("mysql", "orders", "users", fields);
    let (schema_json, _) =
        build_envelope_schema("mysql", "orders", "users", value_schema)
            .unwrap();

    let (url, _) = start_mock_registry().await;
    let encoder =
        AvroEncoder::new(&url, SubjectStrategy::TopicName, None, None).unwrap();

    let event = json!({
        "before": {"id": 1, "name": "Alice"},
        "after": null,
        "source": {
            "version": "deltaforge-test",
            "connector": "mysql",
            "name": "test",
            "ts_ms": 1700000000000_i64,
            "db": "orders",
            "schema": null,
            "table": "users",
            "snapshot": null,
            "position": null
        },
        "op": "d",
        "ts_ms": 1700000000000_i64,
        "event_id": null,
        "schema_version": null,
        "transaction": null
    });

    let bytes = encoder.encode("cdc.users", &event, None).await;
    assert!(
        bytes.is_ok(),
        "DELETE event should encode: schema={schema_json}, error={:?}",
        bytes.err()
    );
}

// ── UPDATE event: both before and after have data

#[tokio::test]
async fn ddl_mysql_update_event() {
    let opts = TypeConversionOpts::default();
    let columns = [
        col("id", "int", "int", false),
        col("name", "varchar", "varchar(255)", true),
        col("balance", "float", "float", true),
    ];
    let fields: Vec<serde_json::Value> = columns
        .iter()
        .map(|c| mysql_column_to_avro(c, &opts))
        .collect();

    let value_schema =
        build_value_schema("mysql", "orders", "accounts", fields);
    let (schema_json, _) =
        build_envelope_schema("mysql", "orders", "accounts", value_schema)
            .unwrap();

    let (url, _) = start_mock_registry().await;
    let encoder =
        AvroEncoder::new(&url, SubjectStrategy::TopicName, None, None).unwrap();

    let event = json!({
        "before": {"id": 1, "name": "Alice", "balance": 100.50},
        "after": {"id": 1, "name": "Alicia", "balance": 200.75},
        "source": {
            "version": "deltaforge-test",
            "connector": "mysql",
            "name": "test",
            "ts_ms": 1700000000000_i64,
            "db": "orders",
            "schema": null,
            "table": "accounts",
            "snapshot": null,
            "position": null
        },
        "op": "u",
        "ts_ms": 1700000000000_i64,
        "event_id": "019d5926-0322-7534-8000-000000008534",
        "schema_version": "abc123",
        "transaction": {
            "id": "gtid:1-185",
            "total_order": 1000,
            "data_collection_order": 1
        }
    });

    let bytes = encoder.encode("cdc.accounts", &event, None).await;
    assert!(
        bytes.is_ok(),
        "UPDATE event should encode: schema={schema_json}, error={:?}",
        bytes.err()
    );
}

// ── Event with all null optional fields

#[tokio::test]
async fn ddl_mysql_all_nulls() {
    ddl_roundtrip(
        "soak_payment_nulls",
        vec![
            col("id", "int", "int", false),
            col("tag", "varchar", "varchar(100)", false),
            col("data", "text", "text", true),
            decimal_col("value", 10, 4, true),
            col("status", "tinyint", "tinyint", true),
            col("notes", "text", "text", true),
            col("created_at", "timestamp", "timestamp", true),
        ],
        json!({
            "id": 1,
            "tag": "test",
            "data": null,
            "value": null,
            "status": null,
            "notes": null,
            "created_at": null
        }),
    )
    .await;
}

// =============================================================================
// Exhaustive type coverage: every MySQL type
// =============================================================================

use deltaforge_core::encoding::avro_types::{
    EnumMode, NaiveTimestampMode, UnsignedBigintMode, postgres_column_to_avro,
};

fn unsigned_col(name: &str, data_type: &str, column_type: &str) -> ColumnDesc {
    ColumnDesc {
        unsigned: true,
        ..col(name, data_type, column_type, false)
    }
}

fn bit_col(name: &str, precision: i64) -> ColumnDesc {
    ColumnDesc {
        precision: Some(precision),
        ..col(name, "bit", &format!("bit({precision})"), true)
    }
}

fn pg_col(name: &str, data_type: &str, nullable: bool) -> ColumnDesc {
    col(name, data_type, data_type, nullable)
}

fn pg_array_col(name: &str, element_type: &str) -> ColumnDesc {
    ColumnDesc {
        is_array: true,
        element_type: Some(element_type.to_string()),
        ..col(name, "ARRAY", &format!("{element_type}[]"), true)
    }
}

fn pg_decimal_col(name: &str, precision: i64, scale: i64) -> ColumnDesc {
    ColumnDesc {
        precision: Some(precision),
        scale: Some(scale),
        ..col(
            name,
            "numeric",
            &format!("numeric({precision},{scale})"),
            true,
        )
    }
}

/// Build DDL-derived envelope, encode CDC event, verify it produces valid wire format.
async fn ddl_encode_roundtrip(
    connector: &str,
    table: &str,
    columns: Vec<ColumnDesc>,
    after_json: serde_json::Value,
    opts: &TypeConversionOpts,
) {
    let fields: Vec<serde_json::Value> = columns
        .iter()
        .map(|c| {
            if connector == "mysql" {
                mysql_column_to_avro(c, opts)
            } else {
                postgres_column_to_avro(c, opts)
            }
        })
        .collect();

    let value_schema = build_value_schema(connector, "testdb", table, fields);
    let (schema_json, _) =
        build_envelope_schema(connector, "testdb", table, value_schema)
            .unwrap_or_else(|e| {
                panic!("{connector}.{table} schema build failed: {e}")
            });

    let (url, _) = start_mock_registry().await;
    let encoder =
        AvroEncoder::new(&url, SubjectStrategy::TopicName, None, None).unwrap();

    let event = json!({
        "before": null,
        "after": after_json,
        "source": {
            "version": "deltaforge-test",
            "connector": connector,
            "name": "test",
            "ts_ms": 1700000000000_i64,
            "db": "testdb",
            "schema": null,
            "table": table,
            "snapshot": null,
            "position": null
        },
        "op": "c",
        "ts_ms": 1700000000000_i64,
        "event_id": null,
        "schema_version": null,
        "transaction": null
    });

    let result = encoder.encode(&format!("test.{table}"), &event, None).await;
    assert!(
        result.is_ok(),
        "{connector}.{table} encode failed: {:?}\nSchema: {schema_json}",
        result.err()
    );
    let bytes = result.unwrap();
    assert_eq!(bytes[0], 0x00, "{connector}.{table}: magic byte");
    assert!(bytes.len() > 5, "{connector}.{table}: payload exists");
}

#[tokio::test]
async fn ddl_mysql_all_integer_types() {
    ddl_encode_roundtrip(
        "mysql",
        "int_types",
        vec![
            col("c_tinyint", "tinyint", "tinyint", false),
            col("c_smallint", "smallint", "smallint", true),
            col("c_mediumint", "mediumint", "mediumint", true),
            col("c_int", "int", "int", false),
            col("c_bigint", "bigint", "bigint", true),
            unsigned_col("c_int_unsigned", "int", "int unsigned"),
            unsigned_col("c_bigint_unsigned", "bigint", "bigint unsigned"),
        ],
        json!({
            "c_tinyint": 127,
            "c_smallint": 32000,
            "c_mediumint": 8000000,
            "c_int": 2147483647,
            "c_bigint": 9223372036854775000_i64,
            "c_int_unsigned": 4294967295_i64,
            "c_bigint_unsigned": "18446744073709551615"
        }),
        &TypeConversionOpts::default(),
    )
    .await;
}

#[tokio::test]
async fn ddl_mysql_float_double() {
    ddl_encode_roundtrip(
        "mysql",
        "float_types",
        vec![
            col("c_float", "float", "float", true),
            col("c_double", "double", "double", true),
            col("c_real", "real", "real", true),
        ],
        json!({
            "c_float": 3.15,
            "c_double": 2.72,
            "c_real": 1.0
        }),
        &TypeConversionOpts::default(),
    )
    .await;
}

#[tokio::test]
async fn ddl_mysql_decimal_types() {
    ddl_encode_roundtrip(
        "mysql",
        "decimal_types",
        vec![
            decimal_col("c_decimal_10_2", 10, 2, true),
            decimal_col("c_decimal_8_4", 8, 4, true),
            decimal_col("c_decimal_12_4", 12, 4, false),
            col("c_decimal_no_prec", "decimal", "decimal", true), // no precision → string
        ],
        json!({
            "c_decimal_10_2": "12345678.99",
            "c_decimal_8_4": "1234.5678",
            "c_decimal_12_4": "12345678.9012",
            "c_decimal_no_prec": "99999.99"
        }),
        &TypeConversionOpts::default(),
    )
    .await;
}

#[tokio::test]
async fn ddl_mysql_string_types() {
    ddl_encode_roundtrip(
        "mysql",
        "string_types",
        vec![
            col("c_varchar", "varchar", "varchar(255)", true),
            col("c_char", "char", "char(3)", true),
            col("c_text", "text", "text", true),
            col("c_tinytext", "tinytext", "tinytext", true),
            col("c_mediumtext", "mediumtext", "mediumtext", true),
            col("c_longtext", "longtext", "longtext", true),
        ],
        json!({
            "c_varchar": "hello world",
            "c_char": "USD",
            "c_text": "some long text",
            "c_tinytext": "tiny",
            "c_mediumtext": "medium",
            "c_longtext": "very long text here"
        }),
        &TypeConversionOpts::default(),
    )
    .await;
}

#[tokio::test]
async fn ddl_mysql_binary_types() {
    ddl_encode_roundtrip(
        "mysql",
        "binary_types",
        vec![
            col("c_binary", "binary", "binary(16)", true),
            col("c_varbinary", "varbinary", "varbinary(255)", true),
            col("c_blob", "blob", "blob", true),
            col("c_tinyblob", "tinyblob", "tinyblob", true),
            col("c_mediumblob", "mediumblob", "mediumblob", true),
            col("c_longblob", "longblob", "longblob", true),
        ],
        json!({
            "c_binary": "AAAAAAAAAAAAAAAA",
            "c_varbinary": "deadbeef",
            "c_blob": "blob data",
            "c_tinyblob": "tiny",
            "c_mediumblob": "medium",
            "c_longblob": "long"
        }),
        &TypeConversionOpts::default(),
    )
    .await;
}

#[tokio::test]
async fn ddl_mysql_datetime_types() {
    ddl_encode_roundtrip(
        "mysql",
        "datetime_types",
        vec![
            col("c_date", "date", "date", true),
            col("c_datetime", "datetime", "datetime", true), // → string (default)
            col("c_timestamp", "timestamp", "timestamp", true), // → timestamp-millis
            col("c_time", "time", "time", true),
            col("c_year", "year", "year", true),
        ],
        json!({
            "c_date": 19000,
            "c_datetime": "2026-04-04T10:30:00",
            "c_timestamp": 1700000000000_i64,
            "c_time": 38400000_i64,
            "c_year": 2026
        }),
        &TypeConversionOpts::default(),
    )
    .await;
}

#[tokio::test]
async fn ddl_mysql_datetime_timestamp_mode() {
    // Override: naive timestamps use Avro timestamp-millis
    let opts = TypeConversionOpts {
        naive_timestamp_mode: NaiveTimestampMode::Timestamp,
        ..Default::default()
    };
    ddl_encode_roundtrip(
        "mysql",
        "datetime_ts_mode",
        vec![
            col("c_datetime", "datetime", "datetime", true),
            col("c_timestamp", "timestamp", "timestamp", true),
        ],
        json!({
            "c_datetime": 1700000000000_i64,
            "c_timestamp": 1700000000000_i64
        }),
        &opts,
    )
    .await;
}

#[tokio::test]
async fn ddl_mysql_special_types() {
    ddl_encode_roundtrip(
        "mysql",
        "special_types",
        vec![
            col("c_bool", "boolean", "tinyint(1)", true),
            bit_col("c_bit1", 1), // BIT(1) → boolean
            bit_col("c_bit8", 8), // BIT(8) → bytes
            col("c_json", "json", "json", true),
            col("c_enum", "enum", "enum('a','b','c')", true), // → string (default)
            col("c_set", "set", "set('x','y','z')", true),
        ],
        json!({
            "c_bool": true,
            "c_bit1": true,
            "c_bit8": "AA",
            "c_json": "{\"key\":\"value\"}",
            "c_enum": "b",
            "c_set": "x,y"
        }),
        &TypeConversionOpts::default(),
    )
    .await;
}

#[tokio::test]
async fn ddl_mysql_enum_mode_enum() {
    let opts = TypeConversionOpts {
        enum_mode: EnumMode::Enum,
        ..Default::default()
    };
    ddl_encode_roundtrip(
        "mysql",
        "enum_strict",
        vec![col(
            "c_enum",
            "enum",
            "enum('pending','shipped','delivered')",
            false,
        )],
        json!({"c_enum": "shipped"}),
        &opts,
    )
    .await;
}

#[tokio::test]
async fn ddl_mysql_unsigned_bigint_long_mode() {
    let opts = TypeConversionOpts {
        unsigned_bigint_mode: UnsignedBigintMode::Long,
        ..Default::default()
    };
    ddl_encode_roundtrip(
        "mysql",
        "unsigned_long",
        vec![unsigned_col(
            "c_bigint_unsigned",
            "bigint",
            "bigint unsigned",
        )],
        json!({"c_bigint_unsigned": 9223372036854775000_i64}),
        &opts,
    )
    .await;
}

// =============================================================================
// Exhaustive type coverage: every PostgreSQL type
// =============================================================================

#[tokio::test]
async fn ddl_pg_all_integer_types() {
    ddl_encode_roundtrip(
        "postgresql",
        "pg_int_types",
        vec![
            pg_col("c_smallint", "smallint", false),
            pg_col("c_integer", "integer", false),
            pg_col("c_bigint", "bigint", true),
            pg_col("c_serial", "serial", false),
            pg_col("c_bigserial", "bigserial", false),
            pg_col("c_smallserial", "smallserial", false),
        ],
        json!({
            "c_smallint": 32000,
            "c_integer": 2147483647,
            "c_bigint": 9223372036854775000_i64,
            "c_serial": 1,
            "c_bigserial": 1,
            "c_smallserial": 1
        }),
        &TypeConversionOpts::default(),
    )
    .await;
}

#[tokio::test]
async fn ddl_pg_float_types() {
    ddl_encode_roundtrip(
        "postgresql",
        "pg_float_types",
        vec![
            pg_col("c_real", "real", true),
            pg_col("c_double", "double precision", true),
        ],
        json!({
            "c_real": 3.15,
            "c_double": 2.72
        }),
        &TypeConversionOpts::default(),
    )
    .await;
}

#[tokio::test]
async fn ddl_pg_numeric_types() {
    ddl_encode_roundtrip(
        "postgresql",
        "pg_numeric_types",
        vec![
            pg_decimal_col("c_numeric_10_2", 10, 2),
            pg_decimal_col("c_numeric_38_18", 38, 18),
            pg_col("c_numeric_unbound", "numeric", true), // no precision → string
        ],
        json!({
            "c_numeric_10_2": "12345678.99",
            "c_numeric_38_18": "12345678901234567890.123456789012345678",
            "c_numeric_unbound": "99999.12345"
        }),
        &TypeConversionOpts::default(),
    )
    .await;
}

#[tokio::test]
async fn ddl_pg_string_types() {
    ddl_encode_roundtrip(
        "postgresql",
        "pg_string_types",
        vec![
            pg_col("c_text", "text", true),
            pg_col("c_varchar", "character varying", true),
            pg_col("c_char", "character", true),
            pg_col("c_name", "name", true),
            pg_col("c_citext", "citext", true),
        ],
        json!({
            "c_text": "hello",
            "c_varchar": "world",
            "c_char": "X",
            "c_name": "my_column",
            "c_citext": "Case Insensitive"
        }),
        &TypeConversionOpts::default(),
    )
    .await;
}

#[tokio::test]
async fn ddl_pg_temporal_types() {
    ddl_encode_roundtrip(
        "postgresql",
        "pg_temporal_types",
        vec![
            pg_col("c_date", "date", true),
            pg_col("c_timestamp", "timestamp", true), // → string (default)
            pg_col("c_timestamptz", "timestamptz", true), // → timestamp-micros
            pg_col("c_time", "time", true),
            pg_col("c_timetz", "timetz", true), // → string
            pg_col("c_interval", "interval", true),
        ],
        json!({
            "c_date": 19000,
            "c_timestamp": "2026-04-04T10:30:00.000000",
            "c_timestamptz": 1700000000000000_i64,
            "c_time": 38400000000_i64,
            "c_timetz": "14:30:00.000000+02:00",
            "c_interval": "P1Y2M3DT4H5M6S"
        }),
        &TypeConversionOpts::default(),
    )
    .await;
}

#[tokio::test]
async fn ddl_pg_json_and_binary() {
    ddl_encode_roundtrip(
        "postgresql",
        "pg_json_bin",
        vec![
            pg_col("c_json", "json", true),
            pg_col("c_jsonb", "jsonb", true),
            pg_col("c_bytea", "bytea", true),
            pg_col("c_uuid", "uuid", true),
            pg_col("c_boolean", "boolean", false),
        ],
        json!({
            "c_json": "{\"key\":\"value\"}",
            "c_jsonb": "{\"nested\":{\"a\":1}}",
            "c_bytea": "deadbeef",
            "c_uuid": "550e8400-e29b-41d4-a716-446655440000",
            "c_boolean": true
        }),
        &TypeConversionOpts::default(),
    )
    .await;
}

#[tokio::test]
async fn ddl_pg_network_and_geo_types() {
    ddl_encode_roundtrip(
        "postgresql",
        "pg_net_geo",
        vec![
            pg_col("c_inet", "inet", true),
            pg_col("c_cidr", "cidr", true),
            pg_col("c_macaddr", "macaddr", true),
            pg_col("c_macaddr8", "macaddr8", true),
            pg_col("c_point", "point", true),
            pg_col("c_line", "line", true),
            pg_col("c_polygon", "polygon", true),
            pg_col("c_circle", "circle", true),
        ],
        json!({
            "c_inet": "192.168.1.1/24",
            "c_cidr": "10.0.0.0/8",
            "c_macaddr": "08:00:2b:01:02:03",
            "c_macaddr8": "08:00:2b:01:02:03:04:05",
            "c_point": "(1.5,2.5)",
            "c_line": "{1,-1,0}",
            "c_polygon": "((0,0),(1,0),(1,1),(0,1))",
            "c_circle": "<(0,0),5>"
        }),
        &TypeConversionOpts::default(),
    )
    .await;
}

#[tokio::test]
async fn ddl_pg_special_types() {
    ddl_encode_roundtrip(
        "postgresql",
        "pg_special",
        vec![
            pg_col("c_money", "money", true),
            pg_col("c_xml", "xml", true),
            pg_col("c_hstore", "hstore", true),
            pg_col("c_int4range", "int4range", true),
            pg_col("c_tstzrange", "tstzrange", true),
        ],
        json!({
            "c_money": "$1,234.56",
            "c_xml": "<root><item>1</item></root>",
            "c_hstore": "key1=>val1,key2=>val2",
            "c_int4range": "[1,10)",
            "c_tstzrange": "[2026-01-01T00:00:00Z,2026-12-31T23:59:59Z)"
        }),
        &TypeConversionOpts::default(),
    )
    .await;
}

#[tokio::test]
async fn ddl_pg_array_types() {
    ddl_encode_roundtrip(
        "postgresql",
        "pg_arrays",
        vec![
            pg_array_col("c_int_arr", "integer"),
            pg_array_col("c_text_arr", "text"),
            pg_array_col("c_bool_arr", "boolean"),
        ],
        json!({
            "c_int_arr": [1, 2, 3],
            "c_text_arr": ["a", "b", "c"],
            "c_bool_arr": [true, false, true]
        }),
        &TypeConversionOpts::default(),
    )
    .await;
}

#[tokio::test]
async fn ddl_pg_timestamp_mode_override() {
    let opts = TypeConversionOpts {
        naive_timestamp_mode: NaiveTimestampMode::Timestamp,
        ..Default::default()
    };
    ddl_encode_roundtrip(
        "postgresql",
        "pg_ts_mode",
        vec![
            pg_col("c_timestamp", "timestamp", true),
            pg_col("c_timestamptz", "timestamptz", true),
        ],
        json!({
            "c_timestamp": 1700000000000000_i64,
            "c_timestamptz": 1700000000000000_i64
        }),
        &opts,
    )
    .await;
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
use gate_ownership::GateOwned;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::sync::OnceCell;

/// Schema Registry port (must not conflict with other services).
const SR_PORT: u16 = 8181;

/// Private, auto-created testcontainers network shared by this file's Kafka +
/// Schema Registry. Self-provisioned so the suite is hermetic — it no longer
/// depends on an externally-running docker-compose Kafka.
const PRIVATE_NETWORK: &str = "df-avro-encoding-net";
/// Stable container name for Kafka so Schema Registry can resolve it by DNS on
/// the shared network. Must be unique across this file's containers.
const KAFKA_HOST: &str = "df-avro-encoding-kafka";
/// Kafka broker (PLAINTEXT) port, reachable only inside PRIVATE_NETWORK.
const KAFKA_INTERNAL_PORT: u16 = 29092;

/// Shared Kafka + Schema Registry containers.
struct AvroInfra {
    // Kept alive for the whole test process; dropped/cleaned up in the dtor.
    #[allow(dead_code)]
    kafka: ContainerAsync<GenericImage>,
    #[allow(dead_code)]
    schema_registry: ContainerAsync<GenericImage>,
}

static AVRO_INFRA: OnceCell<AvroInfra> = OnceCell::const_new();

#[dtor]
fn cleanup() {
    if let Some(infra) = AVRO_INFRA.get() {
        for id in [infra.schema_registry.id(), infra.kafka.id()] {
            std::process::Command::new("docker")
                .args(["rm", "-f", id])
                .output()
                .ok();
        }
    }
}

fn sr_url() -> String {
    format!("http://localhost:{SR_PORT}")
}

async fn wait_for_schema_registry(url: &str, timeout: Duration) -> Result<()> {
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

/// Start a single-node KRaft Kafka on `PRIVATE_NETWORK` under a stable name so
/// Schema Registry can reach it by DNS. The broker is only advertised on the
/// internal network — the host never talks to Kafka directly (tests use the SR
/// HTTP API only), so no host port is mapped.
async fn start_kafka() -> ContainerAsync<GenericImage> {
    // Remove any leftover container from a hard-killed prior run so the fixed
    // name doesn't collide.
    std::process::Command::new("docker")
        .args(["rm", "-f", KAFKA_HOST])
        .output()
        .ok();

    GenericImage::new("confluentinc/cp-kafka", "7.5.0")
        .with_wait_for(WaitFor::Duration {
            length: Duration::from_secs(15),
        })
        .with_container_name(KAFKA_HOST)
        .with_network(PRIVATE_NETWORK)
        .with_env_var("KAFKA_NODE_ID", "1")
        .with_env_var("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env_var("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:29093")
        .with_env_var(
            "KAFKA_LISTENERS",
            format!("PLAINTEXT://0.0.0.0:{KAFKA_INTERNAL_PORT},CONTROLLER://0.0.0.0:29093"),
        )
        .with_env_var(
            "KAFKA_ADVERTISED_LISTENERS",
            format!("PLAINTEXT://{KAFKA_HOST}:{KAFKA_INTERNAL_PORT}"),
        )
        .with_env_var(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
        )
        .with_env_var("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env_var("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
        .with_env_var("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .with_env_var("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .with_env_var("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
        .with_env_var("CLUSTER_ID", "MkU3OEVBNTcwNTJENDM2Qg")
        .gate_owned()
        .start()
        .await
        .expect("start kafka container")
}

/// Start a self-provisioned Kafka (KRaft) + Confluent Schema Registry.
///
/// Both run on a private testcontainers network; SR reaches Kafka by its
/// container name. Fully hermetic — no external docker-compose dependency.
async fn get_infra() -> &'static AvroInfra {
    AVRO_INFRA
        .get_or_init(|| async {
            info!("starting private Kafka (KRaft) for Schema Registry...");
            let kafka = start_kafka().await;

            let sr =
                GenericImage::new("confluentinc/cp-schema-registry", "7.5.0")
                    .with_wait_for(WaitFor::Duration {
                        length: Duration::from_secs(10),
                    })
                    .with_container_name("df-avro-encoding-sr")
                    .with_network(PRIVATE_NETWORK)
                    .with_env_var("SCHEMA_REGISTRY_HOST_NAME", "localhost")
                    .with_env_var(
                        "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                        format!(
                            "PLAINTEXT://{KAFKA_HOST}:{KAFKA_INTERNAL_PORT}"
                        ),
                    )
                    .with_env_var(
                        "SCHEMA_REGISTRY_LISTENERS",
                        format!("http://0.0.0.0:{SR_PORT}"),
                    )
                    .with_env_var("SCHEMA_REGISTRY_DEBUG", "true")
                    .with_mapped_port(SR_PORT, SR_PORT.tcp())
                    .gate_owned()
                    .start()
                    .await
                    .expect("start schema-registry container");

            info!("Schema Registry started: {}", sr.id());

            wait_for_schema_registry(&sr_url(), Duration::from_secs(60))
                .await
                .expect("Schema Registry should be ready");

            AvroInfra {
                kafka,
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
    let resp = client.get(format!("{sr_url}/subjects")).send().await?;
    let subjects: Vec<String> = resp.json().await?;
    Ok(subjects)
}

// ---- Real integration tests ------------------------------------------------

#[tokio::test]
#[ignore]
async fn real_sr_encode_and_verify_schema_registered() -> Result<()> {
    init_test_tracing();
    let _infra = get_infra().await;

    let encoder =
        AvroEncoder::new(&sr_url(), SubjectStrategy::TopicName, None, None)?;

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

    let encoder =
        AvroEncoder::new(&sr_url(), SubjectStrategy::TopicName, None, None)?;

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
        assert!(field_names.contains(&"score"), "should have 'score' field");
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

    let encoder =
        AvroEncoder::new(&sr_url(), SubjectStrategy::TopicName, None, None)?;

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

    let encoder =
        AvroEncoder::new(&sr_url(), SubjectStrategy::RecordName, None, None)?;

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

    let encoder =
        AvroEncoder::new(&sr_url(), SubjectStrategy::TopicName, None, None)?;

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

    let bytes = encoder.encode("cdc-avro-test", &cdc_event, None).await?;

    assert_eq!(bytes[0], 0x00);
    let schema_id =
        u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
    info!(schema_id, payload_len = bytes.len(), "CDC event encoded");

    // Decode and verify structure
    let schema_resp = fetch_schema_by_id(&sr_url(), schema_id).await?;
    let schema_str = schema_resp["schema"].as_str().unwrap();
    let avro_schema = apache_avro::Schema::parse_str(schema_str)?;

    let decoded =
        apache_avro::from_avro_datum(&avro_schema, &mut &bytes[5..], None)?;

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

// ---- Real SR: DDL-derived envelope schema tests ----------------------------
//
// Full production path with a real Schema Registry:
// DDL column types → avro_types → envelope schema → encode →
// register with real SR → decode and verify roundtrip.

async fn real_sr_ddl_roundtrip(
    connector: &str,
    table: &str,
    columns: Vec<ColumnDesc>,
    after_json: serde_json::Value,
    opts: &TypeConversionOpts,
) -> Result<()> {
    let _infra = get_infra().await;
    let fields: Vec<serde_json::Value> = columns
        .iter()
        .map(|c| {
            if connector == "mysql" {
                mysql_column_to_avro(c, opts)
            } else {
                postgres_column_to_avro(c, opts)
            }
        })
        .collect();

    let value_schema = build_value_schema(connector, "testdb", table, fields);
    let _ = build_envelope_schema(connector, "testdb", table, value_schema)?;

    let encoder =
        AvroEncoder::new(&sr_url(), SubjectStrategy::TopicName, None, None)?;

    let event = json!({
        "before": null,
        "after": after_json,
        "source": {
            "version": "test", "connector": connector, "name": "test",
            "ts_ms": 1700000000000_i64, "db": "testdb", "schema": null,
            "table": table, "snapshot": null, "position": null
        },
        "op": "c", "ts_ms": 1700000000000_i64,
        "event_id": null, "schema_version": null, "transaction": null
    });

    let topic = format!("real-sr-ddl.{table}");
    let bytes = encoder.encode(&topic, &event, None).await?;
    assert_eq!(bytes[0], 0x00);
    let schema_id =
        u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
    assert!(schema_id > 0);

    // Fetch from SR, decode, verify
    let sr = fetch_schema_by_id(&sr_url(), schema_id).await?;
    let schema =
        apache_avro::Schema::parse_str(sr["schema"].as_str().unwrap())?;
    let decoded =
        apache_avro::from_avro_datum(&schema, &mut &bytes[5..], None)?;
    if let apache_avro::types::Value::Record(f) = &decoded {
        let names: Vec<&str> = f.iter().map(|(n, _)| n.as_str()).collect();
        assert!(names.contains(&"after"));
        assert!(names.contains(&"op"));
    } else {
        panic!("{table}: expected Record");
    }
    info!(table, schema_id, "DDL roundtrip OK with real SR");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn real_sr_ddl_mysql_payment() -> Result<()> {
    init_test_tracing();
    real_sr_ddl_roundtrip(
        "mysql", "soak_payment_01",
        vec![
            col("id", "int", "int", false),
            col("tag", "varchar", "varchar(100)", false),
            col("data", "text", "text", true),
            decimal_col("value", 10, 4, true),
            col("status", "tinyint", "tinyint", true),
            col("currency", "char", "char(3)", true),
            col("method", "varchar", "varchar(32)", true),
            col("reference", "varchar", "varchar(128)", true),
            decimal_col("fee", 8, 4, true),
            decimal_col("net_amount", 12, 4, true),
            col("refunded", "boolean", "tinyint(1)", true),
            col("processed_at", "datetime", "datetime", true),
            col("metadata", "json", "json", true),
            col("created_at", "timestamp", "timestamp", true),
            col("updated_at", "timestamp", "timestamp", true),
        ],
        json!({
            "id": 9205, "tag": "test", "data": {"_base64": "YmxvYg=="}, "value": "9785.62",
            "status": 2, "currency": "USD", "method": "card", "reference": "r1",
            "fee": "1.50", "net_amount": "9784.12", "refunded": false,
            "processed_at": "2026-04-04T10:00:00", "metadata": null,
            "created_at": 1700000000000_i64, "updated_at": 1700000000000_i64
        }),
        &TypeConversionOpts::default(),
    ).await
}

#[tokio::test]
#[ignore]
async fn real_sr_ddl_mysql_all_types() -> Result<()> {
    init_test_tracing();
    real_sr_ddl_roundtrip(
        "mysql", "all_mysql_types",
        vec![
            col("c_tinyint", "tinyint", "tinyint", false),
            col("c_smallint", "smallint", "smallint", true),
            col("c_mediumint", "mediumint", "mediumint", true),
            col("c_int", "int", "int", false),
            col("c_bigint", "bigint", "bigint", true),
            unsigned_col("c_int_unsigned", "int", "int unsigned"),
            unsigned_col("c_bigint_unsigned", "bigint", "bigint unsigned"),
            col("c_float", "float", "float", true),
            col("c_double", "double", "double", true),
            decimal_col("c_decimal", 10, 2, true),
            col("c_varchar", "varchar", "varchar(255)", true),
            col("c_char", "char", "char(3)", true),
            col("c_text", "text", "text", true),
            col("c_blob", "blob", "blob", true),
            col("c_date", "date", "date", true),
            col("c_datetime", "datetime", "datetime", true),
            col("c_timestamp", "timestamp", "timestamp", true),
            col("c_time", "time", "time", true),
            col("c_year", "year", "year", true),
            col("c_bool", "boolean", "tinyint(1)", true),
            col("c_json", "json", "json", true),
            col("c_enum", "enum", "enum('a','b','c')", true),
            col("c_set", "set", "set('x','y')", true),
        ],
        json!({
            "c_tinyint": 127, "c_smallint": 32000, "c_mediumint": 8000000,
            "c_int": 2147483647, "c_bigint": 9223372036854775000_i64,
            "c_int_unsigned": 4294967295_i64, "c_bigint_unsigned": "18446744073709551615",
            "c_float": 3.15, "c_double": 2.72, "c_decimal": "12345678.99",
            "c_varchar": "hello", "c_char": "USD", "c_text": "text", "c_blob": "bin",
            "c_date": 19000, "c_datetime": "2026-04-04T10:30:00",
            "c_timestamp": 1700000000000_i64, "c_time": 38400000_i64, "c_year": 2026,
            "c_bool": true, "c_json": "{}", "c_enum": "b", "c_set": "x,y"
        }),
        &TypeConversionOpts::default(),
    ).await
}

#[tokio::test]
#[ignore]
async fn real_sr_ddl_postgres_all_types() -> Result<()> {
    init_test_tracing();
    real_sr_ddl_roundtrip(
        "postgresql",
        "all_pg_types",
        vec![
            pg_col("c_smallint", "smallint", false),
            pg_col("c_integer", "integer", false),
            pg_col("c_bigint", "bigint", true),
            pg_col("c_real", "real", true),
            pg_col("c_double", "double precision", true),
            pg_decimal_col("c_numeric", 10, 2),
            pg_col("c_boolean", "boolean", false),
            pg_col("c_text", "text", true),
            pg_col("c_varchar", "character varying", true),
            pg_col("c_bytea", "bytea", true),
            pg_col("c_date", "date", true),
            pg_col("c_timestamp", "timestamp", true),
            pg_col("c_timestamptz", "timestamptz", true),
            pg_col("c_time", "time", true),
            pg_col("c_timetz", "timetz", true),
            pg_col("c_uuid", "uuid", true),
            pg_col("c_json", "json", true),
            pg_col("c_jsonb", "jsonb", true),
            pg_col("c_inet", "inet", true),
            pg_col("c_point", "point", true),
            pg_array_col("c_int_arr", "integer"),
            pg_array_col("c_text_arr", "text"),
        ],
        json!({
            "c_smallint": 32000, "c_integer": 2147483647,
            "c_bigint": 9223372036854775000_i64,
            "c_real": 3.15, "c_double": 2.72,
            "c_numeric": "12345678.99", "c_boolean": true,
            "c_text": "hello", "c_varchar": "world", "c_bytea": "deadbeef",
            "c_date": 19000, "c_timestamp": "2026-04-04T10:30:00",
            "c_timestamptz": 1700000000000000_i64,
            "c_time": 38400000000_i64, "c_timetz": "14:30:00+02:00",
            "c_uuid": "550e8400-e29b-41d4-a716-446655440000",
            "c_json": "{}", "c_jsonb": "{}",
            "c_inet": "192.168.1.1/24", "c_point": "(1.5,2.5)",
            "c_int_arr": [1, 2, 3], "c_text_arr": ["a", "b"]
        }),
        &TypeConversionOpts::default(),
    )
    .await
}

#[tokio::test]
#[ignore]
async fn real_sr_ddl_crud_operations() -> Result<()> {
    init_test_tracing();
    let _infra = get_infra().await;
    let opts = TypeConversionOpts::default();
    let cols = [
        col("id", "int", "int", false),
        col("name", "varchar", "varchar(255)", true),
        decimal_col("balance", 10, 2, true),
    ];
    let fields: Vec<serde_json::Value> = cols
        .iter()
        .map(|c| mysql_column_to_avro(c, &opts))
        .collect();
    let vs = build_value_schema("mysql", "testdb", "accts", fields);
    let _ = build_envelope_schema("mysql", "testdb", "accts", vs)?;

    let encoder =
        AvroEncoder::new(&sr_url(), SubjectStrategy::TopicName, None, None)?;
    let topic = "real-sr-ddl.accts";
    let src = json!({"version":"t","connector":"mysql","name":"t","ts_ms":1700000000000_i64,"db":"testdb","schema":null,"table":"accts","snapshot":null,"position":null});

    let mk = |op, before, after, ts_ms: i64| {
        json!({
            "before": before, "after": after, "source": src, "op": op,
            "ts_ms": ts_ms, "event_id": null, "schema_version": null, "transaction": null
        })
    };

    let ib = encoder
        .encode(
            topic,
            &mk(
                "c",
                json!(null),
                json!({"id":1,"name":"Alice","balance":"100.50"}),
                1700000000000,
            ),
            None,
        )
        .await?;
    let ub = encoder
        .encode(
            topic,
            &mk(
                "u",
                json!({"id":1,"name":"Alice","balance":"100.50"}),
                json!({"id":1,"name":"Alicia","balance":"200.75"}),
                1700000000001,
            ),
            None,
        )
        .await?;
    let db = encoder
        .encode(
            topic,
            &mk(
                "d",
                json!({"id":1,"name":"Alicia","balance":"200.75"}),
                json!(null),
                1700000000002,
            ),
            None,
        )
        .await?;

    let id = |b: &[u8]| u32::from_be_bytes([b[1], b[2], b[3], b[4]]);
    assert_eq!(id(&ib), id(&ub));
    assert_eq!(id(&ub), id(&db));

    // Decode all
    let sr = fetch_schema_by_id(&sr_url(), id(&ib)).await?;
    let schema =
        apache_avro::Schema::parse_str(sr["schema"].as_str().unwrap())?;
    for (label, b) in [("INSERT", &ib), ("UPDATE", &ub), ("DELETE", &db)] {
        let d = apache_avro::from_avro_datum(&schema, &mut &b[5..], None)?;
        assert!(matches!(d, apache_avro::types::Value::Record(_)), "{label}");
    }
    info!("CRUD roundtrip verified with real SR");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn real_sr_ddl_per_table_subjects() -> Result<()> {
    init_test_tracing();
    let _infra = get_infra().await;
    let opts = TypeConversionOpts::default();
    let encoder =
        AvroEncoder::new(&sr_url(), SubjectStrategy::TopicName, None, None)?;

    let mk_fields = |cols: &[ColumnDesc]| -> Vec<serde_json::Value> {
        cols.iter()
            .map(|c| mysql_column_to_avro(c, &opts))
            .collect()
    };

    let c1 = [
        col("id", "int", "int", false),
        col("name", "varchar", "varchar(100)", true),
    ];
    let f1 = mk_fields(&c1);
    let vs1 = build_value_schema("mysql", "testdb", "tbl_a", f1);
    let _ = build_envelope_schema("mysql", "testdb", "tbl_a", vs1)?;

    let c2 = [
        col("id", "int", "int", false),
        decimal_col("price", 8, 2, true),
    ];
    let f2 = mk_fields(&c2);
    let vs2 = build_value_schema("mysql", "testdb", "tbl_b", f2);
    let _ = build_envelope_schema("mysql", "testdb", "tbl_b", vs2)?;

    let src = json!({"version":"t","connector":"mysql","name":"t","ts_ms":1700000000000_i64,"db":"testdb","schema":null,"table":"x","snapshot":null,"position":null});
    let mk = |after| json!({"before":null,"after":after,"source":src,"op":"c","ts_ms":1700000000000_i64,"event_id":null,"schema_version":null,"transaction":null});

    let b1 = encoder
        .encode(
            "real-sr-ddl-subj.tbl_a",
            &mk(json!({"id":1,"name":"A"})),
            None,
        )
        .await?;
    let b2 = encoder
        .encode(
            "real-sr-ddl-subj.tbl_b",
            &mk(json!({"id":1,"price":"9.99"})),
            None,
        )
        .await?;

    let id1 = u32::from_be_bytes([b1[1], b1[2], b1[3], b1[4]]);
    let id2 = u32::from_be_bytes([b2[1], b2[2], b2[3], b2[4]]);
    assert_ne!(id1, id2, "different tables → different IDs");

    let subjects = list_subjects(&sr_url()).await?;
    assert!(subjects.contains(&"real-sr-ddl-subj.tbl_a-value".to_string()));
    assert!(subjects.contains(&"real-sr-ddl-subj.tbl_b-value".to_string()));
    info!("per-table subjects verified");
    Ok(())
}

// =============================================================================
// Schema selection: every event is encoded with the schema it requires
// =============================================================================

use apache_avro::types::Value as AvroValue;
use deltaforge_core::encoding::avro::{
    EnvelopeSchemaLookup, SchemaIdentity, SourceSchemaProvider,
};

fn schema_id(bytes: &[u8]) -> u32 {
    u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]])
}

/// Decode `bytes` with the schema the registry holds for its ID.
async fn decode(registry: &MockSchemaRegistry, bytes: &[u8]) -> AvroValue {
    let schema = apache_avro::Schema::parse_str(
        &registry.schema_by_id(schema_id(bytes)).await,
    )
    .unwrap();
    apache_avro::from_avro_datum(&schema, &mut &bytes[5..], None).unwrap()
}

/// A record field, through any union.
fn field<'a>(v: &'a AvroValue, name: &str) -> &'a AvroValue {
    let mut v = v;
    while let AvroValue::Union(_, inner) = v {
        v = inner;
    }
    let AvroValue::Record(fields) = v else {
        panic!("{name}: not a record: {v:?}");
    };
    let mut f = &fields
        .iter()
        .find(|(n, _)| n == name)
        .unwrap_or_else(|| panic!("no field {name} in {fields:?}"))
        .1;
    while let AvroValue::Union(_, inner) = f {
        f = inner;
    }
    f
}

fn field_names(v: &AvroValue) -> Vec<String> {
    let mut v = v;
    while let AvroValue::Union(_, inner) = v {
        v = inner;
    }
    let AvroValue::Record(fields) = v else {
        panic!("not a record: {v:?}");
    };
    fields.iter().map(|(n, _)| n.clone()).collect()
}

fn envelope(
    op: &str,
    before: serde_json::Value,
    after: serde_json::Value,
) -> serde_json::Value {
    json!({
        "before": before,
        "after": after,
        "source": {
            "version": "t", "connector": "mysql", "name": "t",
            "ts_ms": 1700000000000_i64, "db": "shop", "schema": null,
            "table": "orders", "snapshot": null, "position": null
        },
        "op": op,
        "ts_ms": 1700000000000_i64,
        "event_id": null,
        "schema_version": null,
        "transaction": null
    })
}

#[tokio::test]
async fn an_inferred_schema_keeps_the_before_image_of_later_updates_and_deletes()
 {
    let (url, registry) = start_mock_registry().await;
    let encoder =
        AvroEncoder::new(&url, SubjectStrategy::TopicName, None, None).unwrap();
    let row = |name: &str| json!({"id": 1, "name": name});

    let ins = encoder
        .encode("t", &envelope("c", json!(null), row("a")), None)
        .await
        .unwrap();
    let upd = encoder
        .encode("t", &envelope("u", row("a"), row("b")), None)
        .await
        .unwrap();
    let del = encoder
        .encode("t", &envelope("d", row("b"), json!(null)), None)
        .await
        .unwrap();

    for (label, bytes, name) in [("update", &upd, "a"), ("delete", &del, "b")] {
        let d = decode(&registry, bytes).await;
        assert_eq!(
            field(field(&d, "before"), "name"),
            &AvroValue::String(name.into()),
            "{label}: the before image is encoded"
        );
        assert_eq!(field(field(&d, "before"), "id"), &AvroValue::Long(1));
    }
    assert_eq!(
        field(field(&decode(&registry, &ins).await, "after"), "name"),
        &AvroValue::String("a".into())
    ); // The insert's after image already shaped the before image: one
    // schema version serves all three operations.
    assert_eq!(schema_id(&ins), schema_id(&upd));
    assert_eq!(schema_id(&upd), schema_id(&del));
    assert_eq!(registry.versions("t-value").await, 1);
}

#[tokio::test]
async fn a_value_after_a_first_row_null_is_encoded() {
    let (url, registry) = start_mock_registry().await;
    let encoder =
        AvroEncoder::new(&url, SubjectStrategy::TopicName, None, None).unwrap();

    let first = encoder
        .encode(
            "t",
            &envelope("c", json!(null), json!({"id": 1, "note": null})),
            None,
        )
        .await
        .unwrap();
    let second = encoder
        .encode(
            "t",
            &envelope("c", json!(null), json!({"id": 2, "note": "kept"})),
            None,
        )
        .await
        .unwrap();

    assert_eq!(
        field(field(&decode(&registry, &first).await, "after"), "note"),
        &AvroValue::Null
    );
    assert_eq!(
        field(field(&decode(&registry, &second).await, "after"), "note"),
        &AvroValue::String("kept".into()),
        "the value is never dropped for the null-first schema"
    );
}

#[tokio::test]
async fn a_restarted_encoder_widens_the_registered_inferred_schema() {
    let (url, registry) = start_mock_registry().await;
    let first =
        AvroEncoder::new(&url, SubjectStrategy::TopicName, None, None).unwrap();
    first
        .encode(
            "t",
            &envelope("c", json!(null), json!({"id": 1, "note": "n"})),
            None,
        )
        .await
        .unwrap();

    // A new process sees only ids: its schema still carries `note`.
    let restarted =
        AvroEncoder::new(&url, SubjectStrategy::TopicName, None, None).unwrap();
    let bytes = restarted
        .encode("t", &envelope("c", json!(null), json!({"id": 2})), None)
        .await
        .unwrap();
    let after = field(&decode(&registry, &bytes).await, "after").clone();
    assert!(field_names(&after).contains(&"note".to_string()));
    assert_eq!(registry.versions("t-value").await, 1, "no narrower version");
}

/// Envelope schemas of `shop.orders` by fingerprint, with their registry
/// sequence, built from the DDL as the runner's provider does.
struct VersionedOrders {
    versions: HashMap<String, (u64, String)>,
}

impl VersionedOrders {
    fn new() -> Self {
        let opts = TypeConversionOpts::default();
        let build = |cols: &[ColumnDesc]| {
            let fields = cols
                .iter()
                .map(|c| mysql_column_to_avro(c, &opts))
                .collect();
            let vs = build_value_schema("mysql", "shop", "orders", fields);
            build_envelope_schema("mysql", "shop", "orders", vs)
                .unwrap()
                .0
        };
        let v1 = [
            col("id", "int", "int", false),
            col("name", "varchar", "varchar(20)", true),
        ];
        let mut v2 = v1.to_vec();
        v2.push(col("extra", "int", "int", true));
        Self {
            versions: HashMap::from([
                ("h1".to_string(), (1, build(&v1))),
                ("h2".to_string(), (2, build(&v2))),
            ]),
        }
    }
}

impl SourceSchemaProvider for VersionedOrders {
    fn get_envelope_schema(
        &self,
        _connector: &str,
        _db: &str,
        _table: &str,
        identity: &SchemaIdentity,
    ) -> EnvelopeSchemaLookup {
        match self.versions.get(&identity.fingerprint) {
            Some((seq, json))
                if identity.sequence.is_none_or(|s| s == *seq) =>
            {
                let schema = apache_avro::Schema::parse_str(json).unwrap();
                EnvelopeSchemaLookup::Found(json.clone(), Arc::new(schema))
            }
            Some((seq, _)) => EnvelopeSchemaLookup::Conflict(format!(
                "registered with sequence {seq}"
            )),
            None => EnvelopeSchemaLookup::Conflict("not registered".into()),
        }
    }

    fn mapping_id(&self) -> &str {
        "test-mapping"
    }
}

fn v1() -> Option<SchemaIdentity> {
    SchemaIdentity::of(Some("h1"), Some(1))
}

fn v2() -> Option<SchemaIdentity> {
    SchemaIdentity::of(Some("h2"), Some(2))
}

fn ddl_encoder(url: &str) -> AvroEncoder {
    AvroEncoder::with_source_schemas(
        url,
        SubjectStrategy::TopicName,
        None,
        None,
        Some(Arc::new(VersionedOrders::new())),
    )
    .unwrap()
}

fn v1_row(id: i64) -> serde_json::Value {
    envelope("c", json!(null), json!({"id": id, "name": "n"}))
}

fn v2_row(id: i64) -> serde_json::Value {
    envelope(
        "c",
        json!(null),
        json!({"id": id, "name": "n", "extra": 42}),
    )
}

async fn encode_ddl(
    encoder: &AvroEncoder,
    event: &serde_json::Value,
    identity: Option<SchemaIdentity>,
) -> Result<bytes::Bytes, deltaforge_core::encoding::EncodingError> {
    encoder
        .encode_event(
            "orders",
            event,
            "mysql",
            "shop",
            "orders",
            identity.as_ref(),
        )
        .await
}

#[tokio::test]
async fn old_new_old_events_each_use_their_own_schema_id() {
    let (url, registry) = start_mock_registry().await;
    let encoder = ddl_encoder(&url);

    let a = encode_ddl(&encoder, &v1_row(1), v1()).await.unwrap();
    let b = encode_ddl(&encoder, &v2_row(2), v2()).await.unwrap();
    // Replaying an older event after the newer schema is registered.
    let c = encode_ddl(&encoder, &v1_row(3), v1()).await.unwrap();

    assert_ne!(schema_id(&a), schema_id(&b));
    assert_eq!(schema_id(&a), schema_id(&c));
    let after_b = field(&decode(&registry, &b).await, "after").clone();
    assert_eq!(field(&after_b, "extra"), &AvroValue::Int(42));
    let after_c = field(&decode(&registry, &c).await, "after").clone();
    assert_eq!(field_names(&after_c), ["id", "name"]);
}

#[tokio::test]
async fn a_mixed_version_batch_selects_each_events_schema() {
    let (url, registry) = start_mock_registry().await;
    let encoder = ddl_encoder(&url);
    let batch = [
        (v2_row(1), v2()),
        (v1_row(2), v1()),
        (v2_row(3), v2()),
        (v1_row(4), v1()),
    ];
    let mut ids = HashMap::new();
    for (event, identity) in &batch {
        let bytes =
            encode_ddl(&encoder, event, identity.clone()).await.unwrap();
        let fp = identity.as_ref().unwrap().fingerprint.clone();
        assert_eq!(
            *ids.entry(fp.clone()).or_insert(schema_id(&bytes)),
            schema_id(&bytes),
            "{fp}: one ID per version"
        );
        let after = field(&decode(&registry, &bytes).await, "after").clone();
        let want: &[&str] = if fp == "h2" {
            &["id", "name", "extra"]
        } else {
            &["id", "name"]
        };
        assert_eq!(field_names(&after), want);
    }
    assert_ne!(ids["h1"], ids["h2"]);
}

#[tokio::test]
async fn an_unknown_input_column_fails_before_any_record() {
    let (url, _registry) = start_mock_registry().await;
    let encoder = ddl_encoder(&url);
    // A v2 row stamped as v1: `extra` is not in the v1 schema.
    let err = encode_ddl(&encoder, &v2_row(1), v1()).await.unwrap_err();
    let deltaforge_core::encoding::EncodingError::Conversion(f) = &err else {
        panic!("expected a conversion failure, got {err:?}");
    };
    assert_eq!(f.column, "after.extra");
    assert!(
        err.into_sink_error_for("k").is_dlq_eligible(),
        "a per-event conversion failure keeps the DLQ rules"
    );
}

#[tokio::test]
async fn a_conflicting_schema_sequence_stops_the_sink() {
    let (url, registry) = start_mock_registry().await;
    let encoder = ddl_encoder(&url);
    encode_ddl(&encoder, &v1_row(1), v1()).await.unwrap();
    let stale = SchemaIdentity::of(Some("h1"), Some(7));
    let err = encode_ddl(&encoder, &v1_row(2), stale).await.unwrap_err();
    assert!(
        matches!(
            err,
            deltaforge_core::encoding::EncodingError::SchemaConflict(_)
        ),
        "{err:?}"
    );
    let sink_err = err.into_sink_error_for("k");
    assert!(!sink_err.is_dlq_eligible(), "never dead-lettered");
    assert!(matches!(sink_err, deltaforge_core::SinkError::Fatal { .. }));
    assert_eq!(registry.versions("orders-value").await, 1);
}

#[tokio::test]
async fn cache_eviction_and_reload_never_select_another_version() {
    let (url, registry) = start_mock_registry().await;
    let encoder = ddl_encoder(&url);
    let a = encode_ddl(&encoder, &v1_row(1), v1()).await.unwrap();
    let b = encode_ddl(&encoder, &v2_row(2), v2()).await.unwrap();

    encoder.clear_caches();
    // Reloaded in the opposite order: still each version's own schema.
    let b2 = encode_ddl(&encoder, &v2_row(3), v2()).await.unwrap();
    let a2 = encode_ddl(&encoder, &v1_row(4), v1()).await.unwrap();
    assert_eq!(schema_id(&a), schema_id(&a2));
    assert_eq!(schema_id(&b), schema_id(&b2));
    let after = field(&decode(&registry, &a2).await, "after").clone();
    assert_eq!(field_names(&after), ["id", "name"]);
}

#[tokio::test]
async fn concurrent_registration_of_two_versions_keeps_their_ids_apart() {
    let (url, registry) = start_mock_registry().await;
    // v1's registration is answered last, so v2 is assigned the first ID.
    let v1_marker = {
        let p = VersionedOrders::new();
        p.versions["h1"].1.clone()
    };
    registry
        .delays
        .lock()
        .unwrap()
        .push((v1_marker.clone(), Duration::from_millis(300)));
    let encoder = Arc::new(ddl_encoder(&url));

    let (e1, e2) = (encoder.clone(), encoder.clone());
    let t1 = tokio::spawn(async move {
        encode_ddl(&e1, &v1_row(1), v1()).await.unwrap()
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let t2 = tokio::spawn(async move {
        encode_ddl(&e2, &v2_row(2), v2()).await.unwrap()
    });
    let (a, b) = (t1.await.unwrap(), t2.await.unwrap());

    assert_eq!(schema_id(&b), 1, "v2 registered first");
    assert_eq!(schema_id(&a), 2, "v1 registered second");
    assert_eq!(registry.schema_by_id(schema_id(&a)).await, v1_marker);
    // Later events of each version keep their own ID.
    let a2 = encode_ddl(&encoder, &v1_row(3), v1()).await.unwrap();
    let b2 = encode_ddl(&encoder, &v2_row(4), v2()).await.unwrap();
    assert_eq!(schema_id(&a2), schema_id(&a));
    assert_eq!(schema_id(&b2), schema_id(&b));
}

/// A Schema Registry outage (unreachable, 5xx, 408, 429) is retryable:
/// never dead-lettered. A 409 rejection is a schema failure: the sink stops
/// with `sink_schema_rejected`, never dead-lettered.
#[tokio::test]
async fn registry_failures_are_never_event_failures() {
    use deltaforge_core::SinkError;
    use deltaforge_core::encoding::EncodingError as E;

    let unreachable = ddl_encoder("http://127.0.0.1:1");
    let err = encode_ddl(&unreachable, &v1_row(1), v1())
        .await
        .unwrap_err();
    assert!(matches!(err, E::SchemaRegistryUnavailable(_)), "{err:?}");
    assert!(matches!(
        err.into_sink_error_for("k"),
        SinkError::Backpressure { .. }
    ));

    let (url, registry) = start_mock_registry().await;
    let encoder = ddl_encoder(&url);
    for status in [500, 503, 408, 429] {
        *registry.fail_status.lock().unwrap() = Some(status);
        let err = encode_ddl(&encoder, &v1_row(1), v1()).await.unwrap_err();
        assert!(
            matches!(err, E::SchemaRegistryUnavailable(_)),
            "{status}: {err:?}"
        );
        let sink_err = err.into_sink_error_for("k");
        assert!(!sink_err.is_dlq_eligible(), "{status}");
        assert!(
            matches!(sink_err, SinkError::Backpressure { .. }),
            "{status}"
        );
    }

    *registry.fail_status.lock().unwrap() = Some(409);
    let err = encode_ddl(&encoder, &v1_row(1), v1()).await.unwrap_err();
    assert!(
        matches!(&err, E::SchemaRejected { subject, .. } if subject == "orders-value"),
        "{err:?}"
    );
    let sink_err = err.into_sink_error_for("k");
    assert!(!sink_err.is_dlq_eligible());
    assert!(matches!(sink_err.root(), SinkError::Fatal { .. }));
    let SinkError::Incident { draft, .. } = &sink_err else {
        panic!("expected an incident: {sink_err:?}");
    };
    assert_eq!(
        draft.reason_code,
        deltaforge_core::incident::ReasonCode::SinkSchemaRejected
    );

    // Any other registry error is not the event's fault either.
    *registry.fail_status.lock().unwrap() = Some(401);
    let err = encode_ddl(&encoder, &v1_row(1), v1()).await.unwrap_err();
    assert!(!err.into_sink_error_for("k").is_dlq_eligible());

    // Recovered: the same event encodes.
    *registry.fail_status.lock().unwrap() = None;
    encode_ddl(&encoder, &v1_row(1), v1()).await.unwrap();
}

/// An Avro value as JSON (unions unwrapped, records as objects).
/// Covers the kinds of inferred schemas and of DDL records.
fn avro_json(v: &AvroValue) -> serde_json::Value {
    match v {
        AvroValue::Null => json!(null),
        AvroValue::Boolean(b) => json!(b),
        AvroValue::Int(n) => json!(n),
        AvroValue::Long(n) => json!(n),
        AvroValue::Double(d) => json!(d),
        AvroValue::String(s) => json!(s),
        AvroValue::Union(_, inner) => avro_json(inner),
        AvroValue::Array(items) => {
            serde_json::Value::Array(items.iter().map(avro_json).collect())
        }
        AvroValue::Record(fields) => serde_json::Value::Object(
            fields
                .iter()
                .map(|(k, v)| (k.clone(), avro_json(v)))
                .collect(),
        ),
        other => panic!("not an inferred kind: {other:?}"),
    }
}

/// Whether `decoded` holds `original` exactly: every field present with
/// its value; fields the original lacks are null; an integer beyond the
/// signed 64-bit range is its exact decimal text; a nested array or object
/// inside an array is its JSON text.
fn holds_exactly(
    original: &serde_json::Value,
    decoded: &serde_json::Value,
) -> bool {
    use serde_json::Value as J;
    match (original, decoded) {
        (J::Object(o), J::Object(d)) => {
            o.iter()
                .all(|(k, v)| d.get(k).is_some_and(|dv| holds_exactly(v, dv)))
                && d.iter().all(|(k, dv)| o.contains_key(k) || dv.is_null())
        }
        (J::Array(o), J::Array(d)) => {
            o.len() == d.len()
                && o.iter().zip(d).all(|(ov, dv)| match (ov, dv) {
                    (J::Array(_) | J::Object(_), J::String(text)) => {
                        serde_json::from_str::<J>(text).ok().as_ref()
                            == Some(ov)
                    }
                    _ => holds_exactly(ov, dv),
                })
        }
        (J::Number(n), J::String(text)) => {
            !n.is_i64() && n.to_string() == *text
        }
        _ => original == decoded,
    }
}

/// Inferred schemas widen losslessly and deterministically: after the
/// schema has widened, replaying an earlier event encodes it exactly (it
/// may use a newer, compatible schema ID, never a narrower one), and two
/// encoders seeing the same events register the same schemas.
#[tokio::test]
async fn inferred_widening_is_lossless_and_deterministic() {
    let events = [
        envelope("c", json!(null), json!({"id": 1, "note": null, "tags": []})),
        envelope(
            "c",
            json!(null),
            json!({"id": 2, "note": "n", "tags": [1, null]}),
        ),
        envelope(
            "u",
            json!({"id": 2, "note": "n"}),
            json!({"id": 2, "note": 2.5}),
        ),
        envelope(
            "c",
            json!(null),
            json!({"id": 18446744073709551615u64, "j": {"k": [1, {"x": true}]}}),
        ),
        envelope("d", json!({"id": 3, "flag": false}), json!(null)),
    ];
    let (url_a, registry_a) = start_mock_registry().await;
    let (url_b, registry_b) = start_mock_registry().await;
    let a = AvroEncoder::new(&url_a, SubjectStrategy::TopicName, None, None)
        .unwrap();
    let b = AvroEncoder::new(&url_b, SubjectStrategy::TopicName, None, None)
        .unwrap();

    let mut first_ids = Vec::new();
    for e in &events {
        let bytes = a.encode("t", e, None).await.unwrap();
        b.encode("t", e, None).await.unwrap();
        first_ids.push(schema_id(&bytes));
        let decoded = avro_json(&decode(&registry_a, &bytes).await);
        assert!(holds_exactly(e, &decoded), "{e}\n{decoded}");
    }
    // Replay every event after the schema has widened.
    for (e, first) in events.iter().zip(&first_ids) {
        let bytes = a.encode("t", e, None).await.unwrap();
        assert!(schema_id(&bytes) >= *first, "never a narrower schema");
        let decoded = avro_json(&decode(&registry_a, &bytes).await);
        assert!(holds_exactly(e, &decoded), "replay: {e}\n{decoded}");
    }
    let schemas = |r: MockSchemaRegistry| async move {
        r.schemas.lock().await.get("t-value").cloned().unwrap()
    };
    assert_eq!(schemas(registry_a).await, schemas(registry_b).await);
}

// =============================================================================
// DDL events: their own explicit record under the topic's DDL subject
// =============================================================================

const DDL_SUBJECT: &str = "orders-deltaforge.DdlEvent-v2";

fn ddl_event(db: &str, payload: serde_json::Value) -> deltaforge_core::Event {
    let source = deltaforge_core::SourceInfo {
        version: "t".into(),
        connector: "mysql".into(),
        name: "p".into(),
        ts_ms: 1_700_000_000_000,
        db: db.into(),
        schema: None,
        table: "_ddl".into(),
        snapshot: None,
        position: deltaforge_core::SourcePosition::default(),
    };
    deltaforge_core::Event::new_ddl(
        deltaforge_core::EventId::mysql_row_server(1, "t", 1, 0),
        source,
        payload,
        1_700_000_000_000,
        0,
    )
}

/// MySQL format-2 payloads as the source emits them (see
/// `mysql_event::ddl_identity`), a redacted account statement and a
/// PostgreSQL TRUNCATE.
fn ddl_payloads() -> Vec<(&'static str, serde_json::Value)> {
    let obj = |db: &str, name: &str| json!({"kind": "table", "database": db, "name": name});
    vec![
        (
            "shop",
            json!({"ddl_format": 2, "scope": "tables",
            "objects": [obj("shop", "orders")], "database": "shop",
            "session_database": null,
            "sql": "ALTER TABLE shop.orders ADD COLUMN extra INT"}),
        ),
        (
            "shop",
            json!({"ddl_format": 2, "scope": "tables",
            "objects": [obj("shop", "a"), obj("shop", "a2")],
            "database": "shop", "session_database": "shop",
            "sql": "RENAME TABLE a TO a2"}),
        ),
        (
            "",
            json!({"ddl_format": 2, "scope": "tables",
            "objects": [obj("shop", "b"), obj("arch", "b2")],
            "database": null, "session_database": "",
            "sql": "RENAME TABLE shop.b TO arch.b2"}),
        ),
        (
            "shop",
            json!({"ddl_format": 2, "scope": "tables",
            "objects": [obj("shop", "c"), obj("shop", "a2"), obj("shop", "d")],
            "database": "shop", "session_database": null,
            "sql": "DROP TABLE shop.c, shop.a2, shop.d"}),
        ),
        (
            "arch",
            json!({"ddl_format": 2, "scope": "database",
            "objects": [{"kind": "database", "database": "arch", "name": "arch"}],
            "database": "arch", "session_database": null,
            "sql": "DROP DATABASE arch"}),
        ),
        (
            "",
            json!({"ddl_format": 2, "scope": "lineage", "objects": [],
            "database": null, "session_database": "shop",
            "sql": "ALTER TABLE a ADD x INT COMMENT \"x\""}),
        ),
        (
            "",
            json!({"ddl_format": 2, "scope": "none", "objects": [],
            "database": null, "session_database": null,
            "operation": "CREATE USER", "redacted": true,
            "note": "credential-bearing account statement; raw SQL omitted"}),
        ),
        (
            "public",
            json!({"sql": "TRUNCATE", "cascade": false,
            "restart_identity": true}),
        ),
    ]
}

/// The decoded DDL payload holds `payload` exactly: every key with its
/// value (null and "" distinct, objects in order); keys it lacks are null.
fn assert_payload_exact(
    payload: &serde_json::Value,
    decoded: &serde_json::Value,
) {
    let (p, d) = (payload.as_object().unwrap(), decoded.as_object().unwrap());
    for (k, v) in p {
        assert_eq!(d.get(k), Some(v), "{k}: {decoded}");
    }
    for (k, v) in d {
        assert!(p.contains_key(k) || v.is_null(), "{k}: {decoded}");
    }
}

#[tokio::test]
async fn ddl_events_are_lossless_records_under_their_own_subject() {
    let (url, registry) = start_mock_registry().await;
    let encoder = ddl_encoder(&url);
    for (db, payload) in ddl_payloads() {
        let ev = ddl_event(db, payload.clone());
        let bytes = encoder.encode_ddl("orders", &ev).await.unwrap();
        let d = avro_json(&decode(&registry, &bytes).await);
        assert_payload_exact(&payload, &d["ddl"]);
        assert_eq!(d["mapping_version"], json!(2));
        assert_eq!(d["source"]["db"], json!(db), "source.db kept as is");
        assert_eq!(d["op"], json!("r"));
        assert_eq!(d["ts_ms"], json!(1_700_000_000_000_i64));
        assert_eq!(d["event_id"], json!(ev.event_id.unwrap().to_string()));
    }
    assert_eq!(
        registry.versions(DDL_SUBJECT).await,
        1,
        "one explicit schema"
    );
    assert_eq!(registry.versions("orders-value").await, 0, "no row subject");
}

#[tokio::test]
async fn ddl_between_row_versions_selects_every_records_own_schema() {
    let (url, registry) = start_mock_registry().await;
    let encoder = ddl_encoder(&url);
    let alter = ddl_event("shop", ddl_payloads()[0].1.clone());

    let a = encode_ddl(&encoder, &v1_row(1), v1()).await.unwrap();
    let ddl = encoder.encode_ddl("orders", &alter).await.unwrap();
    let b = encode_ddl(&encoder, &v2_row(2), v2()).await.unwrap();
    let c = encode_ddl(&encoder, &v1_row(3), v1()).await.unwrap();
    let ddl2 = encoder.encode_ddl("orders", &alter).await.unwrap();

    let ids = [a.clone(), ddl.clone(), b.clone(), c.clone(), ddl2.clone()]
        .map(|x| schema_id(&x));
    assert_eq!(ids[0], ids[3], "old row schema");
    assert_eq!(ids[1], ids[4], "DDL schema");
    assert_eq!(
        std::collections::HashSet::from(ids).len(),
        3,
        "row v1, row v2 and DDL each have their own ID: {ids:?}"
    );
    // A consumer that knows only schema IDs decodes the mixed topic.
    for bytes in [&a, &ddl, &b, &c, &ddl2] {
        let d = avro_json(&decode(&registry, bytes).await);
        assert!(d.get("ddl").is_some() || d.get("after").is_some(), "{d}");
    }
    let after_b = field(&decode(&registry, &b).await, "after").clone();
    assert_eq!(field(&after_b, "extra"), &AvroValue::Int(42));
    assert_eq!(registry.versions("orders-value").await, 2);
    assert_eq!(registry.versions(DDL_SUBJECT).await, 1);
}

#[tokio::test]
async fn an_unknown_ddl_payload_key_fails_before_any_record() {
    let (url, _registry) = start_mock_registry().await;
    let encoder = ddl_encoder(&url);
    let mut payload = ddl_payloads()[0].1.clone();
    payload["algorithm"] = json!("INSTANT");
    let err = encoder
        .encode_ddl("orders", &ddl_event("shop", payload))
        .await
        .unwrap_err();
    let deltaforge_core::encoding::EncodingError::Conversion(f) = &err else {
        panic!("expected a conversion failure, got {err:?}");
    };
    assert_eq!(f.column, "ddl.algorithm");
}

#[tokio::test]
async fn a_row_schema_rejection_is_never_a_ddl_schema_rejection() {
    use deltaforge_core::SinkError;
    let subject_of = |e: deltaforge_core::encoding::EncodingError| {
        let sink_err = e.into_sink_error_for("k");
        let SinkError::Incident { draft, .. } = &sink_err else {
            panic!("expected an incident: {sink_err:?}");
        };
        assert_eq!(
            draft.reason_code,
            deltaforge_core::incident::ReasonCode::SinkSchemaRejected
        );
        serde_json::to_value(draft).unwrap()["evidence"].to_string()
    };
    let (url, registry) = start_mock_registry().await;
    let encoder = ddl_encoder(&url);
    let alter = ddl_event("shop", ddl_payloads()[0].1.clone());

    *registry.reject_subject.lock().unwrap() = Some("orders-value".into());
    let row = encode_ddl(&encoder, &v1_row(1), v1()).await.unwrap_err();
    let row_evidence = subject_of(row);
    assert!(row_evidence.contains("orders-value"), "{row_evidence}");
    encoder
        .encode_ddl("orders", &alter)
        .await
        .expect("DDL unaffected");

    encoder.clear_caches();
    *registry.reject_subject.lock().unwrap() = Some(DDL_SUBJECT.into());
    let ddl = encoder.encode_ddl("orders", &alter).await.unwrap_err();
    let ddl_evidence = subject_of(ddl);
    assert!(ddl_evidence.contains(DDL_SUBJECT), "{ddl_evidence}");
    encode_ddl(&encoder, &v1_row(1), v1())
        .await
        .expect("rows unaffected");

    // A registry outage on the DDL subject is retryable backpressure.
    encoder.clear_caches();
    *registry.reject_subject.lock().unwrap() = None;
    *registry.fail_status.lock().unwrap() = Some(503);
    let err = encoder.encode_ddl("orders", &alter).await.unwrap_err();
    assert!(matches!(
        err.into_sink_error_for("k"),
        SinkError::Backpressure { .. }
    ));
}

#[tokio::test]
async fn a_restarted_encoder_emits_an_identical_ddl_record() {
    let (url, _registry) = start_mock_registry().await;
    let alter = ddl_event("shop", ddl_payloads()[0].1.clone());
    let first = ddl_encoder(&url)
        .encode_ddl("orders", &alter)
        .await
        .unwrap();
    let replayed = ddl_encoder(&url)
        .encode_ddl("orders", &alter)
        .await
        .unwrap();
    assert_eq!(first, replayed, "same schema ID, identical record");
}

/// One table version whose type mapping can change (BIGINT UNSIGNED as a
/// decimal string or a long): the same source schema maps to another Avro
/// schema under the other mapping.
struct SwitchableMapping {
    long: std::sync::atomic::AtomicBool,
}

impl SourceSchemaProvider for SwitchableMapping {
    fn get_envelope_schema(
        &self,
        _connector: &str,
        _db: &str,
        _table: &str,
        _identity: &SchemaIdentity,
    ) -> EnvelopeSchemaLookup {
        let opts = TypeConversionOpts {
            unsigned_bigint_mode: if self
                .long
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                UnsignedBigintMode::Long
            } else {
                UnsignedBigintMode::String
            },
            ..TypeConversionOpts::default()
        };
        let fields = [
            col("id", "int", "int", false),
            unsigned_col("b", "bigint", "bigint unsigned"),
        ]
        .iter()
        .map(|c| mysql_column_to_avro(c, &opts))
        .collect();
        let vs = build_value_schema("mysql", "shop", "orders", fields);
        let (json, schema) =
            build_envelope_schema("mysql", "shop", "orders", vs).unwrap();
        EnvelopeSchemaLookup::Found(json, Arc::new(schema))
    }

    fn mapping_id(&self) -> &str {
        if self.long.load(std::sync::atomic::Ordering::SeqCst) {
            "unsigned=long"
        } else {
            "unsigned=string"
        }
    }
}

/// A schema resolved under one type mapping is never reused under another,
/// even for the same table version.
#[tokio::test]
async fn a_changed_type_mapping_never_reuses_the_cached_schema() {
    let (url, registry) = start_mock_registry().await;
    let provider = Arc::new(SwitchableMapping {
        long: Default::default(),
    });
    let encoder = AvroEncoder::with_source_schemas(
        &url,
        SubjectStrategy::TopicName,
        None,
        None,
        Some(provider.clone()),
    )
    .unwrap();
    let row = envelope("c", json!(null), json!({"id": 1, "b": 5}));

    let as_string = encode_ddl(&encoder, &row, v1()).await.unwrap();
    provider
        .long
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let as_long = encode_ddl(&encoder, &row, v1()).await.unwrap();

    assert_ne!(schema_id(&as_string), schema_id(&as_long));
    let b = |v: &AvroValue| field(field(v, "after"), "b").clone();
    assert_eq!(
        b(&decode(&registry, &as_string).await),
        AvroValue::String("5".into())
    );
    assert_eq!(b(&decode(&registry, &as_long).await), AvroValue::Long(5));
}
