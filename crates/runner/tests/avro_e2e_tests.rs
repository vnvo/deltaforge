//! End-to-end Avro encoding tests using REAL source serializers.
//!
//! These tests exercise the full production path:
//!   MySQL ColumnValue → mysql_object::build_object() → JSON → Avro encoder → Schema Registry
//!   PostgreSQL PgColumnValue → postgres_object::build_object() → JSON → Avro encoder → SR
//!
//! NO hand-crafted JSON. The JSON is produced by the actual source serializers
//! with the exact same ColumnValue types that the binlog/WAL parsers produce.
//!
//! Requires Docker for Schema Registry (testcontainers). Run with:
//!   cargo test -p runner --test avro_e2e_tests -- --include-ignored --nocapture --test-threads=1

use std::time::{Duration, Instant};

use anyhow::Result;
use bytes::Bytes;
use ctor::dtor;
use mysql_binlog_connector_rust::column::column_value::ColumnValue;
use serde_json::json;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::sync::OnceCell;
use tracing::info;

use deltaforge_core::encoding::avro::{AvroEncoder, SubjectStrategy};
use deltaforge_core::encoding::avro_schema::{
    build_envelope_schema, build_value_schema,
};
use deltaforge_core::encoding::avro_types::{
    ColumnDesc, TypeConversionOpts, mysql_column_to_avro,
    postgres_column_to_avro,
};
use sources::mysql::mysql_object;
use sources::postgres::postgres_object::{PgColumnValue, RelationColumn};
use sources::postgres::postgres_table_schema::type_oids;

// =============================================================================
// Schema Registry infrastructure (real, Docker)
// =============================================================================

/// Must not conflict with the sinks tests' SR (port 8181).
const SR_PORT: u16 = 8282;
/// The Kafka BROKER listener inside the docker-compose chaos network.
const KAFKA_BOOTSTRAP: &str = "kafka:29092";
/// The docker-compose network that Kafka is on.
const DOCKER_NETWORK: &str = "deltaforge_chaos_net";

struct SrInfra {
    #[allow(dead_code)]
    schema_registry: ContainerAsync<GenericImage>,
}

static SR_INFRA: OnceCell<SrInfra> = OnceCell::const_new();

fn sr_url() -> String {
    format!("http://localhost:{SR_PORT}")
}

async fn wait_for_sr(url: &str, timeout: Duration) -> Result<()> {
    let client = reqwest::Client::new();
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Ok(resp) = client
            .get(format!("{url}/subjects"))
            .timeout(Duration::from_secs(2))
            .send()
            .await
        {
            if resp.status().is_success() {
                return Ok(());
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    anyhow::bail!("SR not ready after {timeout:?}")
}

/// Force-remove the SR container on process exit (including panics).
#[dtor]
fn cleanup_sr() {
    if let Some(infra) = SR_INFRA.get() {
        std::process::Command::new("docker")
            .args(["rm", "-f", infra.schema_registry.id()])
            .output()
            .ok();
    }
}

async fn get_sr() -> &'static SrInfra {
    SR_INFRA
        .get_or_init(|| async {
            let sr =
                GenericImage::new("confluentinc/cp-schema-registry", "7.5.0")
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
                    .with_mapped_port(SR_PORT, SR_PORT.tcp())
                    .with_network(DOCKER_NETWORK)
                    .start()
                    .await
                    .expect("start schema-registry container");

            wait_for_sr(&sr_url(), Duration::from_secs(60))
                .await
                .expect("SR should be ready");

            SrInfra {
                schema_registry: sr,
            }
        })
        .await
}

// =============================================================================
// Helper: build DDL columns, convert to Avro schema, encode, verify
// =============================================================================

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

fn decimal_col(name: &str, precision: i64, scale: i64) -> ColumnDesc {
    ColumnDesc {
        precision: Some(precision),
        scale: Some(scale),
        ..col(
            name,
            "decimal",
            &format!("decimal({precision},{scale})"),
            true,
        )
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

/// Full encode + SR register + decode roundtrip.
async fn avro_roundtrip(
    connector: &str,
    table: &str,
    columns: &[ColumnDesc],
    after_json: serde_json::Value,
) -> Result<()> {
    let _ = get_sr().await;
    let opts = TypeConversionOpts::default();

    let fields: Vec<serde_json::Value> = columns
        .iter()
        .map(|c| {
            if connector == "mysql" {
                mysql_column_to_avro(c, &opts)
            } else {
                postgres_column_to_avro(c, &opts)
            }
        })
        .collect();

    let value_schema = build_value_schema(connector, "e2e_test", table, fields);
    let _ = build_envelope_schema(connector, "e2e_test", table, value_schema)?;

    let encoder =
        AvroEncoder::new(&sr_url(), SubjectStrategy::TopicName, None, None)?;

    let event = json!({
        "before": null,
        "after": after_json,
        "source": {
            "version": "e2e-test",
            "connector": connector,
            "name": "e2e",
            "ts_ms": 1700000000000_i64,
            "db": "e2e_test",
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

    let topic = format!("e2e.{connector}.{table}");
    let bytes = encoder.encode(&topic, &event, None).await?;

    assert_eq!(bytes[0], 0x00, "{table}: magic byte");
    let schema_id =
        u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
    assert!(schema_id > 0, "{table}: schema_id positive");

    // Fetch schema from SR and decode
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{}/schemas/ids/{schema_id}", sr_url()))
        .send()
        .await?;
    let sr_body: serde_json::Value = resp.json().await?;
    let schema_str = sr_body["schema"].as_str().unwrap();
    let schema = apache_avro::Schema::parse_str(schema_str)?;

    let decoded =
        apache_avro::from_avro_datum(&schema, &mut &bytes[5..], None)?;
    assert!(
        matches!(decoded, apache_avro::types::Value::Record(_)),
        "{table}: expected Record"
    );

    info!(connector, table, schema_id, "E2E roundtrip OK");
    Ok(())
}

// =============================================================================
// MySQL: build_object with real ColumnValues
// =============================================================================

/// Simulate what mysql_object::build_object produces for a given set of columns.
fn mysql_build_row(
    col_names: &[&str],
    values: Vec<ColumnValue>,
) -> serde_json::Value {
    let cols: Vec<String> = col_names.iter().map(|s| s.to_string()).collect();
    let included: Vec<bool> = vec![true; cols.len()];
    mysql_object::build_object(&cols, &included, &values)
}

// ── MySQL payment domain (the table that kept failing) ──────────────────────

#[tokio::test]
#[ignore]
async fn e2e_mysql_payment_domain() -> Result<()> {
    tracing_subscriber::fmt::try_init().ok();

    let after = mysql_build_row(
        &[
            "id",
            "tag",
            "data",
            "value",
            "status",
            "currency",
            "method",
            "reference",
            "fee",
            "net_amount",
            "refunded",
            "processed_at",
            "metadata",
            "created_at",
            "updated_at",
        ],
        vec![
            ColumnValue::Long(9205),
            ColumnValue::String(b"drain-26-1775260448196-52".to_vec()),
            ColumnValue::Blob(b"backlog-1775260448196".to_vec()),
            ColumnValue::Decimal("9785.6240".to_string()),
            ColumnValue::Tiny(2),
            ColumnValue::String(b"USD".to_vec()),
            ColumnValue::String(b"".to_vec()),
            ColumnValue::String(b"".to_vec()),
            ColumnValue::Decimal("0.0000".to_string()),
            ColumnValue::Decimal("0.0000".to_string()),
            ColumnValue::Tiny(0), // BOOLEAN as TINYINT
            ColumnValue::None,    // processed_at NULL
            ColumnValue::None,    // metadata NULL
            ColumnValue::Timestamp(1775260448000000),
            ColumnValue::Timestamp(1775260448000000),
        ],
    );

    avro_roundtrip(
        "mysql",
        "e2e_payment",
        &[
            col("id", "int", "int", false),
            col("tag", "varchar", "varchar(100)", false),
            col("data", "text", "text", true),
            decimal_col("value", 10, 4),
            col("status", "tinyint", "tinyint", true),
            col("currency", "char", "char(3)", true),
            col("method", "varchar", "varchar(32)", true),
            col("reference", "varchar", "varchar(128)", true),
            decimal_col("fee", 8, 4),
            decimal_col("net_amount", 12, 4),
            col("refunded", "boolean", "tinyint(1)", true),
            col("processed_at", "datetime", "datetime", true),
            col("metadata", "json", "json", true),
            col("created_at", "timestamp", "timestamp", true),
            col("updated_at", "timestamp", "timestamp", true),
        ],
        after,
    )
    .await
}

// ── MySQL customer domain ───────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn e2e_mysql_customer_domain() -> Result<()> {
    tracing_subscriber::fmt::try_init().ok();

    let after = mysql_build_row(
        &[
            "id",
            "tag",
            "data",
            "value",
            "status",
            "name",
            "email",
            "phone",
            "active",
            "loyalty_points",
            "credit_score",
            "preferences",
            "dob",
            "last_login",
            "created_at",
            "updated_at",
        ],
        vec![
            ColumnValue::Long(10241),
            ColumnValue::String(b"drain-19-1775260449024-0".to_vec()),
            ColumnValue::Blob(b"backlog-1775260449024".to_vec()),
            ColumnValue::Decimal("1932.7713".to_string()),
            ColumnValue::Tiny(2),
            ColumnValue::String(b"".to_vec()),
            ColumnValue::String(b"".to_vec()),
            ColumnValue::String(b"".to_vec()),
            ColumnValue::Tiny(1), // BOOLEAN true
            ColumnValue::Long(0),
            ColumnValue::Float(0.0),
            ColumnValue::None, // JSON null
            ColumnValue::None, // DATE null
            ColumnValue::None, // DATETIME null
            ColumnValue::Timestamp(1775260449000000),
            ColumnValue::Timestamp(1775260449000000),
        ],
    );

    avro_roundtrip(
        "mysql",
        "e2e_customer",
        &[
            col("id", "int", "int", false),
            col("tag", "varchar", "varchar(100)", false),
            col("data", "text", "text", true),
            decimal_col("value", 10, 4),
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
        after,
    )
    .await
}

// ── MySQL order domain ──────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn e2e_mysql_order_domain() -> Result<()> {
    tracing_subscriber::fmt::try_init().ok();

    let after = mysql_build_row(
        &[
            "id",
            "tag",
            "data",
            "value",
            "status",
            "currency",
            "items_count",
            "shipping_cost",
            "gross_amount",
            "is_gift",
            "placed_at",
            "dispatched_at",
            "notes",
            "created_at",
            "updated_at",
        ],
        vec![
            ColumnValue::Long(5001),
            ColumnValue::String(b"drain-10-1775260449000-0".to_vec()),
            ColumnValue::Blob(b"backlog-1775260449000".to_vec()),
            ColumnValue::Decimal("4500.9900".to_string()),
            ColumnValue::Tiny(1),
            ColumnValue::String(b"USD".to_vec()),
            ColumnValue::Short(3), // SMALLINT
            ColumnValue::Decimal("5.9900".to_string()),
            ColumnValue::Decimal("4506.9800".to_string()),
            ColumnValue::Tiny(0), // BOOLEAN false
            ColumnValue::None,    // DATETIME null
            ColumnValue::None,    // DATETIME null
            ColumnValue::Blob(b"handle with care".to_vec()), // TEXT as blob
            ColumnValue::Timestamp(1775260449000000),
            ColumnValue::Timestamp(1775260449000000),
        ],
    );

    avro_roundtrip(
        "mysql",
        "e2e_order",
        &[
            col("id", "int", "int", false),
            col("tag", "varchar", "varchar(100)", false),
            col("data", "text", "text", true),
            decimal_col("value", 10, 4),
            col("status", "tinyint", "tinyint", true),
            col("currency", "char", "char(3)", true),
            col("items_count", "smallint", "smallint", true),
            decimal_col("shipping_cost", 8, 4),
            decimal_col("gross_amount", 12, 4),
            col("is_gift", "boolean", "tinyint(1)", true),
            col("placed_at", "datetime", "datetime", true),
            col("dispatched_at", "datetime", "datetime", true),
            col("notes", "text", "text", true),
            col("created_at", "timestamp", "timestamp", true),
            col("updated_at", "timestamp", "timestamp", true),
        ],
        after,
    )
    .await
}

// ── MySQL event domain ──────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn e2e_mysql_event_domain() -> Result<()> {
    tracing_subscriber::fmt::try_init().ok();

    let after = mysql_build_row(
        &[
            "id",
            "tag",
            "data",
            "value",
            "status",
            "event_type",
            "source_ip",
            "session_id",
            "severity",
            "duration_ms",
            "occurred_at",
            "payload",
            "created_at",
            "updated_at",
        ],
        vec![
            ColumnValue::Long(9281),
            ColumnValue::String(b"drain-29-1775260449022-0".to_vec()),
            ColumnValue::Blob(b"backlog-1775260449022".to_vec()),
            ColumnValue::Decimal("352.6975".to_string()),
            ColumnValue::Tiny(1),
            ColumnValue::String(b"".to_vec()),
            ColumnValue::String(b"".to_vec()),
            ColumnValue::String(b"".to_vec()),
            ColumnValue::Short(0),    // SMALLINT
            ColumnValue::LongLong(0), // BIGINT
            ColumnValue::None,        // DATETIME null
            ColumnValue::None,        // TEXT null
            ColumnValue::Timestamp(1775260449000000),
            ColumnValue::Timestamp(1775260449000000),
        ],
    );

    avro_roundtrip(
        "mysql",
        "e2e_event",
        &[
            col("id", "int", "int", false),
            col("tag", "varchar", "varchar(100)", false),
            col("data", "text", "text", true),
            decimal_col("value", 10, 4),
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
        after,
    )
    .await
}

// ── MySQL: every column type in one table ───────────────────────────────────

#[tokio::test]
#[ignore]
async fn e2e_mysql_all_types() -> Result<()> {
    tracing_subscriber::fmt::try_init().ok();

    let after = mysql_build_row(
        &[
            "c_tinyint",
            "c_smallint",
            "c_mediumint",
            "c_int",
            "c_bigint",
            "c_float",
            "c_double",
            "c_decimal",
            "c_varchar",
            "c_char",
            "c_text",
            "c_blob",
            "c_date",
            "c_datetime",
            "c_timestamp",
            "c_time",
            "c_year",
            "c_bool",
            "c_json",
            "c_enum",
            "c_set",
            "c_null_text",
            "c_null_int",
        ],
        vec![
            ColumnValue::Tiny(127),                          // TINYINT
            ColumnValue::Short(32000),                       // SMALLINT
            ColumnValue::Long(8000000), // MEDIUMINT (sent as Long)
            ColumnValue::Long(2147483647), // INT
            ColumnValue::LongLong(9223372036854775000), // BIGINT
            ColumnValue::Float(3.15),   // FLOAT
            ColumnValue::Double(2.72),  // DOUBLE
            ColumnValue::Decimal("12345678.99".to_string()), // DECIMAL
            ColumnValue::String(b"hello world".to_vec()), // VARCHAR
            ColumnValue::String(b"USD".to_vec()), // CHAR
            ColumnValue::Blob(b"long text content".to_vec()), // TEXT (as Blob!)
            ColumnValue::Blob(b"\x00\x01\x02\x03".to_vec()), // BLOB (binary)
            ColumnValue::Date("2026-04-04".to_string()), // DATE
            ColumnValue::DateTime("2026-04-04 10:30:00".to_string()), // DATETIME
            ColumnValue::Timestamp(1700000000000000), // TIMESTAMP (micros)
            ColumnValue::Time("10:30:00".to_string()), // TIME
            ColumnValue::Year(2026),                  // YEAR
            ColumnValue::Tiny(1),                     // BOOLEAN (as TINYINT)
            ColumnValue::Json(br#"{"key":"value"}"#.to_vec()), // JSON
            ColumnValue::Enum(2),                     // ENUM index
            ColumnValue::Set(3),                      // SET bitmask
            ColumnValue::None,                        // NULL TEXT
            ColumnValue::None,                        // NULL INT
        ],
    );

    avro_roundtrip(
        "mysql",
        "e2e_all_mysql",
        &[
            col("c_tinyint", "tinyint", "tinyint", false),
            col("c_smallint", "smallint", "smallint", true),
            col("c_mediumint", "mediumint", "mediumint", true),
            col("c_int", "int", "int", false),
            col("c_bigint", "bigint", "bigint", true),
            col("c_float", "float", "float", true),
            col("c_double", "double", "double", true),
            decimal_col("c_decimal", 10, 2),
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
            col("c_set", "set", "set('x','y','z')", true),
            col("c_null_text", "text", "text", true),
            col("c_null_int", "int", "int", true),
        ],
        after,
    )
    .await
}

// =============================================================================
// PostgreSQL: build_object with real PgColumnValues
// =============================================================================

/// Build a PG row using the real postgres_object::build_object.
fn pg_build_row(
    named_values: Vec<(&str, PgColumnValue, u32)>,
) -> serde_json::Value {
    let columns: Vec<RelationColumn> = named_values
        .iter()
        .map(|(name, _, oid)| RelationColumn {
            name: name.to_string(),
            type_oid: *oid,
            type_modifier: -1,
            flags: 0,
        })
        .collect();
    let values: Vec<PgColumnValue> =
        named_values.into_iter().map(|(_, v, _)| v).collect();
    sources::postgres::postgres_object::build_object(&columns, &values)
}

fn pg_text_val(s: &str) -> PgColumnValue {
    PgColumnValue::Text(Bytes::from(s.to_string()))
}

// ── PostgreSQL: every type ──────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn e2e_pg_all_types() -> Result<()> {
    tracing_subscriber::fmt::try_init().ok();

    let after = pg_build_row(vec![
        ("c_bool", pg_text_val("t"), type_oids::BOOL),
        ("c_int2", pg_text_val("32000"), type_oids::INT2),
        ("c_int4", pg_text_val("2147483647"), type_oids::INT4),
        (
            "c_int8",
            pg_text_val("9223372036854775000"),
            type_oids::INT8,
        ),
        ("c_float4", pg_text_val("3.15"), type_oids::FLOAT4),
        ("c_float8", pg_text_val("2.72"), type_oids::FLOAT8),
        ("c_numeric", pg_text_val("12345678.99"), type_oids::NUMERIC),
        ("c_text", pg_text_val("hello world"), 25), // TEXT oid=25
        ("c_varchar", pg_text_val("varchar value"), 1043), // VARCHAR
        ("c_bytea", pg_text_val("\\x0001020304"), type_oids::BYTEA),
        ("c_date", pg_text_val("2026-04-04"), type_oids::DATE),
        (
            "c_timestamp",
            pg_text_val("2026-04-04 10:30:00"),
            type_oids::TIMESTAMP,
        ),
        (
            "c_timestamptz",
            pg_text_val("2026-04-04 10:30:00+00"),
            type_oids::TIMESTAMPTZ,
        ),
        ("c_time", pg_text_val("10:30:00"), type_oids::TIME),
        (
            "c_uuid",
            pg_text_val("550e8400-e29b-41d4-a716-446655440000"),
            type_oids::UUID,
        ),
        (
            "c_json",
            pg_text_val("{\"key\":\"value\"}"),
            type_oids::JSON,
        ),
        (
            "c_jsonb",
            pg_text_val("{\"nested\":{\"a\":1}}"),
            type_oids::JSONB,
        ),
        ("c_null", PgColumnValue::Null, type_oids::INT4),
        ("c_int_arr", pg_text_val("{1,2,3}"), 1007), // int4[]
        ("c_text_arr", pg_text_val("{hello,world}"), 1009), // text[]
    ]);

    avro_roundtrip(
        "postgresql",
        "e2e_all_pg",
        &[
            pg_col("c_bool", "boolean", false),
            pg_col("c_int2", "smallint", false),
            pg_col("c_int4", "integer", false),
            pg_col("c_int8", "bigint", true),
            pg_col("c_float4", "real", true),
            pg_col("c_float8", "double precision", true),
            pg_col("c_numeric", "numeric", true), // unbounded → string
            pg_col("c_text", "text", true),
            pg_col("c_varchar", "character varying", true),
            pg_col("c_bytea", "bytea", true),
            pg_col("c_date", "date", true),
            pg_col("c_timestamp", "timestamp", true),
            pg_col("c_timestamptz", "timestamptz", true),
            pg_col("c_time", "time", true),
            pg_col("c_uuid", "uuid", true),
            pg_col("c_json", "json", true),
            pg_col("c_jsonb", "jsonb", true),
            pg_col("c_null", "integer", true),
            pg_array_col("c_int_arr", "integer"),
            pg_array_col("c_text_arr", "text"),
        ],
        after,
    )
    .await
}

// ── PostgreSQL: realistic table ─────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn e2e_pg_realistic_table() -> Result<()> {
    tracing_subscriber::fmt::try_init().ok();

    let after = pg_build_row(vec![
        ("id", pg_text_val("42"), type_oids::INT8),
        ("name", pg_text_val("Alice"), 1043), // varchar
        ("email", pg_text_val("alice@test.com"), 1043),
        ("balance", pg_text_val("1234.56"), type_oids::NUMERIC),
        ("active", pg_text_val("t"), type_oids::BOOL),
        (
            "metadata",
            pg_text_val("{\"pref\":\"dark\"}"),
            type_oids::JSONB,
        ),
        (
            "created_at",
            pg_text_val("2026-04-04 10:30:00+00"),
            type_oids::TIMESTAMPTZ,
        ),
        ("tags", pg_text_val("{admin,user}"), 1009), // text[]
    ]);

    avro_roundtrip(
        "postgresql",
        "e2e_pg_users",
        &[
            pg_col("id", "bigint", false),
            pg_col("name", "character varying", true),
            pg_col("email", "character varying", true),
            pg_col("balance", "numeric", true),
            pg_col("active", "boolean", false),
            pg_col("metadata", "jsonb", true),
            pg_col("timestamptz", "timestamptz", true),
            pg_array_col("tags", "text"),
        ],
        after,
    )
    .await
}
