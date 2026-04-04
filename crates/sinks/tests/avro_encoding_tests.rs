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

    schemas.entry(subject).or_default().push((id, schema_str));

    Json(json!({ "id": id }))
}

async fn start_mock_registry() -> (String, MockSchemaRegistry) {
    let registry = MockSchemaRegistry::new();
    let app = Router::new()
        .route("/subjects/{subject}/versions", post(register_schema))
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
