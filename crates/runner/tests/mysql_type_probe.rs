//! Canonical MySQL type probe (measurement only, not a correctness test).
//!
//! For every probed value, records what each production path emits:
//! snapshot rows and CDC insert / update (both images) / delete (before
//! image), under `binlog_row_metadata` MINIMAL and FULL, through three real
//! pipelines per table (built by the pipeline manager from a live MySQL
//! source): Kafka JSON (native envelope), Kafka Avro (Schema Registry,
//! DDL-derived schemas) and S3 Parquet (RustFS, durable sink). One pipeline
//! per (sink, table) so an encoding that halts its pipeline hides nothing
//! else.
//!
//! Raw output goes to `$TYPE_PROBE_OUT` (default `target/type-probe/<ts>`):
//! `env.json` (MySQL version, SQL modes, character sets, collations,
//! session/system/global time zones, binlog settings, process time zone),
//! `schema-<meta>.json` (information_schema columns and the registered
//! source schema), `records-<meta>.jsonl` (one line per sink record image),
//! `sql-<meta>.jsonl` (every statement with its session SQL mode and
//! outcome) and `pipelines-<meta>.json` (final status and incidents).
//!
//! The S3 sink runs `legacy_rolling` by default (`TYPE_PROBE_S3_DURABILITY`
//! overrides): the durable sink's object keys embed the hex-encoded MySQL
//! GTID watermark, a path segment over 255 bytes that file-backed object
//! stores (RustFS) reject. Both modes share the Parquet encoder.
//! `scripts/type-probe-report.py` builds the comparison from them.
//!
//! ```bash
//! cargo test -p runner --test mysql_type_probe -- --include-ignored --nocapture
//! ```

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use apache_avro::types::Value as Avro;
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, TimeUnit};
use ctor::dtor;
use deltaforge_config::PipelineSpec;
use gate_ownership::GateOwned;
use mysql_async::prelude::Queryable;
use object_store::path::Path as ObjPath;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::async_reader::ParquetObjectReader;
use rdkafka::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::message::Message;
use rest_api::PipelineController;
use runner::pipeline_manager::PipelineManager;
use serde_json::{Value, json};
use sinks::s3::{ObjectStoreParams, build_object_store};
use storage::adapters::{LineageDescriptor, SchemaKey};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::sync::OnceCell;

const NETWORK: &str = "df-type-probe-net";
const KAFKA_NAME: &str = "df-type-probe-kafka";
const KAFKA_INTERNAL_PORT: u16 = 29092;
const KAFKA_HOST_PORT: u16 = 9396;
const ROOT_PW: &str = "pw";
const BUCKET: &str = "type-probe";
const TENANT: &str = "t";
/// The MySQL server's own time zone: neither UTC nor the host's, so any
/// conversion between them is visible.
const SERVER_TZ: &str = "+02:00";
const MARKER: i64 = 9_999;

/// Images pinned by digest (the reproducible inputs; the tags are for
/// reading only). RustFS is pinned by `s3_test_server`.
const MYSQL_IMAGE: (&str, &str) = (
    "mysql",
    "8.4@sha256:c36050afdca850f23cef85703f84c7531a5ae155a11b5ee1c60acb09937c4084",
);
const KAFKA_IMAGE: (&str, &str) = (
    "confluentinc/cp-kafka",
    "7.5.0@sha256:fbbb6fa11b258a88b83f54d4f0bddfcffbf2279f99d66a843486e3da7bdfbf41",
);
const SCHEMA_REGISTRY_IMAGE: (&str, &str) = (
    "confluentinc/cp-schema-registry",
    "7.5.0@sha256:e51684b472a2481f065f44616d3d8ad2182029a7011f949a612d35b54566a1f6",
);

struct Infra {
    kafka: ContainerAsync<GenericImage>,
    sr: ContainerAsync<GenericImage>,
    mysql: ContainerAsync<GenericImage>,
    s3: &'static s3_test_server::S3Server,
}

static INFRA: OnceCell<Infra> = OnceCell::const_new();

#[dtor]
fn cleanup() {
    if let Some(i) = INFRA.get() {
        for id in [i.sr.id(), i.kafka.id(), i.mysql.id()] {
            std::process::Command::new("docker")
                .args(["rm", "-f", "-v", id])
                .output()
                .ok();
        }
    }
    s3_test_server::remove_shared();
}

fn brokers() -> String {
    format!("localhost:{KAFKA_HOST_PORT}")
}

async fn start() -> Infra {
    std::process::Command::new("docker")
        .args(["rm", "-f", KAFKA_NAME])
        .output()
        .ok();
    let kafka = GenericImage::new(KAFKA_IMAGE.0, KAFKA_IMAGE.1)
        .with_wait_for(WaitFor::Duration {
            length: Duration::from_secs(15),
        })
        .with_container_name(KAFKA_NAME)
        .with_network(NETWORK)
        .with_env_var("KAFKA_NODE_ID", "1")
        .with_env_var("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env_var("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:29093")
        .with_env_var(
            "KAFKA_LISTENERS",
            format!(
                "PLAINTEXT://0.0.0.0:{KAFKA_INTERNAL_PORT},CONTROLLER://0.0.0.0:29093,EXTERNAL://0.0.0.0:{KAFKA_HOST_PORT}"
            ),
        )
        .with_env_var(
            "KAFKA_ADVERTISED_LISTENERS",
            format!(
                "PLAINTEXT://{KAFKA_NAME}:{KAFKA_INTERNAL_PORT},EXTERNAL://localhost:{KAFKA_HOST_PORT}"
            ),
        )
        .with_env_var(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT",
        )
        .with_env_var("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env_var("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
        .with_env_var("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .with_env_var("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .with_env_var("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
        .with_env_var("CLUSTER_ID", "MkU3OEVBNTcwNTJENDM2Qg")
        .with_mapped_port(KAFKA_HOST_PORT, KAFKA_HOST_PORT.tcp())
        .gate_owned()
        .start()
        .await
        .expect("start kafka");
    let sr =
        GenericImage::new(SCHEMA_REGISTRY_IMAGE.0, SCHEMA_REGISTRY_IMAGE.1)
            .with_exposed_port(8081.tcp())
            .with_wait_for(WaitFor::Duration {
                length: Duration::from_secs(5),
            })
            .with_network(NETWORK)
            .with_env_var("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .with_env_var(
                "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                format!("PLAINTEXT://{KAFKA_NAME}:{KAFKA_INTERNAL_PORT}"),
            )
            .with_env_var("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .gate_owned()
            .start()
            .await
            .expect("start schema registry");
    let mysql = GenericImage::new(MYSQL_IMAGE.0, MYSQL_IMAGE.1)
        .with_wait_for(WaitFor::message_on_stderr(
            "ready for connections. Version: '8.4",
        ))
        .with_env_var("MYSQL_ROOT_PASSWORD", ROOT_PW)
        .with_cmd(vec![
            "--server-id=50".to_string(),
            "--log-bin=mysql-bin".into(),
            "--binlog-format=ROW".into(),
            "--binlog-row-image=FULL".into(),
            "--gtid-mode=ON".into(),
            "--enforce-gtid-consistency=ON".into(),
            format!("--default-time-zone={SERVER_TZ}"),
        ])
        .gate_owned()
        .start()
        .await
        .expect("start mysql");
    let s3 = s3_test_server::shared().await;
    s3.create_bucket(BUCKET).await.expect("bucket");
    let infra = Infra {
        kafka,
        sr,
        mysql,
        s3,
    };
    let deadline = Instant::now() + Duration::from_secs(180);
    let http = reqwest::Client::new();
    loop {
        let sr_ok = http
            .get(format!("{}/subjects", sr_url(&infra).await))
            .send()
            .await
            .is_ok_and(|r| r.status().is_success());
        let mysql_ok =
            mysql_async::Conn::from_url(dsn(&infra).await).await.is_ok();
        if sr_ok && mysql_ok {
            break;
        }
        assert!(Instant::now() < deadline, "infrastructure never ready");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let _ = &infra.kafka;
    infra
}

async fn sr_url(i: &Infra) -> String {
    format!(
        "http://127.0.0.1:{}",
        i.sr.get_host_port_ipv4(8081).await.unwrap()
    )
}

async fn dsn(i: &Infra) -> String {
    let port = i.mysql.get_host_port_ipv4(3306).await.unwrap();
    format!("mysql://root:{ROOT_PW}@127.0.0.1:{port}/")
}

// ---------------------------------------------------------------------------
// The probed values
// ---------------------------------------------------------------------------

/// One probed row: SQL literals per column, and whether the session must be
/// permissive (`sql_mode=''`) to store it (zero dates, invalid enum).
struct Row {
    label: &'static str,
    values: Vec<&'static str>,
    permissive: bool,
}

struct Table {
    name: &'static str,
    /// `(name, type)`; every column is nullable.
    columns: Vec<(&'static str, &'static str)>,
    rows: Vec<Row>,
    /// Statements expected to be refused (recorded with their error).
    refused: Vec<String>,
}

fn row(label: &'static str, values: &[&'static str]) -> Row {
    Row {
        label,
        values: values.to_vec(),
        permissive: false,
    }
}

fn permissive(label: &'static str, values: &[&'static str]) -> Row {
    Row {
        label,
        values: values.to_vec(),
        permissive: true,
    }
}

fn nulls(n: usize) -> Row {
    Row {
        label: "null",
        values: vec!["NULL"; n],
        permissive: false,
    }
}

fn tables() -> Vec<Table> {
    let int_cols = vec![
        ("ti", "TINYINT"),
        ("tiu", "TINYINT UNSIGNED"),
        ("si", "SMALLINT"),
        ("siu", "SMALLINT UNSIGNED"),
        ("mi", "MEDIUMINT"),
        ("miu", "MEDIUMINT UNSIGNED"),
        ("i", "INT"),
        ("iu", "INT UNSIGNED"),
        ("bi", "BIGINT"),
        ("biu", "BIGINT UNSIGNED"),
        ("t1", "TINYINT(1)"),
        ("bo", "BOOLEAN"),
    ];
    let n_int = int_cols.len();
    vec![
        Table {
            name: "t_int",
            columns: int_cols,
            rows: vec![
                row(
                    "min",
                    &[
                        "-128",
                        "0",
                        "-32768",
                        "0",
                        "-8388608",
                        "0",
                        "-2147483648",
                        "0",
                        "-9223372036854775808",
                        "0",
                        "-128",
                        "FALSE",
                    ],
                ),
                row(
                    "max",
                    &[
                        "127",
                        "255",
                        "32767",
                        "65535",
                        "8388607",
                        "16777215",
                        "2147483647",
                        "4294967295",
                        "9223372036854775807",
                        "18446744073709551615",
                        "127",
                        "TRUE",
                    ],
                ),
                row(
                    "mid",
                    &[
                        "-1",
                        "128",
                        "-1",
                        "32768",
                        "-1",
                        "8388608",
                        "-1",
                        "2147483648",
                        "-1",
                        "9223372036854775808",
                        "5",
                        "2",
                    ],
                ),
                nulls(n_int),
            ],
            refused: vec![
                "INSERT INTO {db}.t_int (id, tiu) VALUES (900, -1)".into(),
                "INSERT INTO {db}.t_int (id, biu) VALUES (901, 18446744073709551616)".into(),
            ],
        },
        Table {
            name: "t_num",
            columns: vec![
                ("d65", "DECIMAL(65,30)"),
                ("d10", "DECIMAL(10,0)"),
                ("d52", "DECIMAL(5,2)"),
                ("f", "FLOAT"),
                ("db", "DOUBLE"),
            ],
            rows: vec![
                row(
                    "max",
                    &[
                        "99999999999999999999999999999999999.999999999999999999999999999999",
                        "9999999999",
                        "999.99",
                        "3.40282e38",
                        "1.7976931348623157e308",
                    ],
                ),
                row(
                    "min",
                    &[
                        "-99999999999999999999999999999999999.999999999999999999999999999999",
                        "-9999999999",
                        "-999.99",
                        "-3.40282e38",
                        "-1.7976931348623157e308",
                    ],
                ),
                row(
                    "small",
                    &[
                        "0.000000000000000000000000000001",
                        "1",
                        "-0.01",
                        "1.1",
                        "0.1",
                    ],
                ),
                row("zero", &["0", "0", "0.00", "-0.0", "-0.0"]),
                row(
                    "tiny",
                    &["12345.678900000000000000000000000000", "0", "1.50", "1.17549e-38", "4.9e-324"],
                ),
                nulls(5),
            ],
            refused: vec![
                "INSERT INTO {db}.t_num (id, db) VALUES (900, 'NaN')".into(),
                "INSERT INTO {db}.t_num (id, db) VALUES (901, 1e309)".into(),
                "INSERT INTO {db}.t_num (id, d52) VALUES (902, 1000.00)".into(),
            ],
        },
        Table {
            name: "t_bit",
            columns: vec![("b1", "BIT(1)"), ("b8", "BIT(8)"), ("b64", "BIT(64)")],
            rows: vec![
                row("zero", &["b'0'", "b'0'", "b'0'"]),
                row("one", &["b'1'", "b'1'", "b'1'"]),
                row(
                    "max",
                    &[
                        "b'1'",
                        "b'11111111'",
                        "b'1111111111111111111111111111111111111111111111111111111111111111'",
                    ],
                ),
                row("pattern", &["b'1'", "b'10100101'", "0x8000000000000001"]),
                nulls(3),
            ],
            refused: vec![],
        },
        Table {
            name: "t_str",
            columns: vec![
                ("vu", "VARCHAR(32) CHARACTER SET utf8mb4"),
                ("vl", "VARCHAR(32) CHARACTER SET latin1"),
                ("cu", "CHAR(8) CHARACTER SET utf8mb4"),
                ("tu", "TEXT CHARACTER SET utf8mb4"),
                ("tl", "TEXT CHARACTER SET latin1"),
                ("va", "VARCHAR(16) CHARACTER SET ascii"),
            ],
            rows: vec![
                row(
                    "text",
                    &[
                        "'héllo 😀'",
                        "'héllo ÿ'",
                        "'ab  '",
                        "'multi\\nline 😀'",
                        "'déjà vu ÿ'",
                        "'plain'",
                    ],
                ),
                row("empty", &["''", "''", "''", "''", "''", "''"]),
                row(
                    "spaces",
                    &["' x '", "' x '", "'  '", "' x '", "' x '", "' '"],
                ),
                nulls(6),
            ],
            refused: vec![],
        },
        Table {
            name: "t_bin",
            columns: vec![
                ("bn", "BINARY(4)"),
                ("vb", "VARBINARY(16)"),
                ("bl", "BLOB"),
            ],
            rows: vec![
                row("printable", &["'ab'", "'hello'", "'blob text'"]),
                row("invalid_utf8", &["0xFF00C3FE", "0xFF", "0xC328FFFE"]),
                row("zero_bytes", &["0x00000000", "0x00", "0x0000"]),
                row("trailing", &["0x61200000", "0x612000", "0x6120"]),
                row("empty", &["''", "''", "''"]),
                nulls(3),
            ],
            refused: vec![],
        },
        Table {
            name: "t_enum",
            columns: vec![
                ("e", "ENUM('a','b','c')"),
                ("s", "SET('x','y','z')"),
            ],
            rows: vec![
                row("first", &["'a'", "''"]),
                row("last", &["'c'", "'x,y,z'"]),
                row("mid", &["'b'", "'z,x'"]),
                permissive("invalid", &["'zzz'", "'x,bogus'"]),
                nulls(2),
            ],
            refused: vec!["INSERT INTO {db}.t_enum (id, e) VALUES (900, 'zzz')".into()],
        },
        Table {
            name: "t_json",
            columns: vec![("j", "JSON")],
            rows: vec![
                row("null_literal", &["'null'"]),
                row("int", &["'1'"]),
                row("u64_max", &["'18446744073709551615'"]),
                row("i64_min", &["'-9223372036854775808'"]),
                row("float", &["'1.5'"]),
                row("float_int", &["'1.0'"]),
                row("big_float", &["'1e300'"]),
                row("string", &["'\"str ü 😀\"'"]),
                row("bool", &["'true'"]),
                row("array", &["'[1, \"a\", null, [2.5]]'"]),
                row("object", &["'{\"k\": {\"n\": 1.0, \"u\": \"😀\"}, \"a\": 1}'"]),
                row("empty_object", &["'{}'"]),
                nulls(1),
            ],
            refused: vec![],
        },
        Table {
            name: "t_time",
            columns: vec![
                ("d", "DATE"),
                ("tm", "TIME"),
                ("tm6", "TIME(6)"),
                ("dt", "DATETIME"),
                ("dt6", "DATETIME(6)"),
                ("ts", "TIMESTAMP NULL"),
                ("ts6", "TIMESTAMP(6) NULL"),
                ("y", "YEAR"),
            ],
            rows: vec![
                row(
                    "min",
                    &[
                        "'1000-01-01'",
                        "'-838:59:59'",
                        "'-838:59:59.000000'",
                        "'1000-01-01 00:00:00'",
                        "'1000-01-01 00:00:00.000000'",
                        "'1970-01-01 02:00:01'",
                        "'1970-01-01 02:00:01.000000'",
                        "1901",
                    ],
                ),
                row(
                    "max",
                    &[
                        "'9999-12-31'",
                        "'838:59:59'",
                        "'838:59:59.000000'",
                        "'9999-12-31 23:59:59'",
                        "'9999-12-31 23:59:59.999999'",
                        "'2038-01-19 05:14:07'",
                        "'2038-01-19 05:14:07.999999'",
                        "2155",
                    ],
                ),
                row(
                    "mid",
                    &[
                        "'2024-02-29'",
                        "'12:34:56'",
                        "'-00:00:00.500000'",
                        "'2024-02-29 12:34:56'",
                        "'2024-02-29 12:34:56.123456'",
                        "'2024-03-31 01:30:00'",
                        "'2024-10-27 02:30:00.654321'",
                        "2024",
                    ],
                ),
                permissive(
                    "zero",
                    &[
                        "'0000-00-00'",
                        "'00:00:00'",
                        "'00:00:00.000000'",
                        "'0000-00-00 00:00:00'",
                        "'0000-00-00 00:00:00.000000'",
                        "'0000-00-00 00:00:00'",
                        "'0000-00-00 00:00:00.000000'",
                        "0",
                    ],
                ),
                nulls(8),
            ],
            refused: vec![
                "INSERT INTO {db}.t_time (id, d) VALUES (900, '0000-00-00')".into(),
                "INSERT INTO {db}.t_time (id, d) VALUES (901, '2023-02-29')".into(),
            ],
        },
    ]
}

// ---------------------------------------------------------------------------
// SQL with a log
// ---------------------------------------------------------------------------

struct Sql {
    conn: mysql_async::Conn,
    log: Vec<Value>,
    mode: String,
}

impl Sql {
    async fn new(i: &Infra) -> Self {
        Sql {
            conn: mysql_async::Conn::from_url(dsn(i).await).await.unwrap(),
            log: Vec::new(),
            mode: "default".into(),
        }
    }

    async fn mode(&mut self, permissive: bool) {
        let want = if permissive { "permissive" } else { "default" };
        if self.mode != want {
            let stmt = if permissive {
                "SET SESSION sql_mode = ''"
            } else {
                "SET SESSION sql_mode = DEFAULT"
            };
            self.conn.query_drop(stmt).await.unwrap();
            self.mode = want.into();
        }
    }

    /// Run `stmt`; record its outcome and warnings. Panics on error unless
    /// `refusal_expected`.
    async fn run(&mut self, stmt: &str, refusal_expected: bool) {
        let mode: Option<String> = self
            .conn
            .query_first("SELECT @@SESSION.sql_mode")
            .await
            .unwrap();
        let r = self.conn.query_drop(stmt).await;
        let warnings: Vec<(String, u32, String)> = if r.is_ok() {
            self.conn
                .query_map("SHOW WARNINGS", |(l, c, m)| (l, c, m))
                .await
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        self.log.push(json!({
            "sql": stmt,
            "sql_mode": mode,
            "ok": r.is_ok(),
            "error": r.as_ref().err().map(|e| e.to_string()),
            "warnings": warnings,
            "refusal_expected": refusal_expected,
        }));
        if let Err(e) = r
            && !refusal_expected
        {
            panic!("{stmt}: {e}");
        }
    }
}

fn insert(db: &str, t: &Table, id: i64, r: &Row) -> String {
    let cols: Vec<&str> = t.columns.iter().map(|(c, _)| *c).collect();
    format!(
        "INSERT INTO {db}.{} (id, {}) VALUES ({id}, {})",
        t.name,
        cols.join(", "),
        r.values.join(", ")
    )
}

fn update(db: &str, t: &Table, id: i64, r: &Row) -> String {
    let set: Vec<String> = t
        .columns
        .iter()
        .zip(&r.values)
        .map(|((c, _), v)| format!("{c} = {v}"))
        .collect();
    format!(
        "UPDATE {db}.{} SET {} WHERE id = {id}",
        t.name,
        set.join(", ")
    )
}

// ---------------------------------------------------------------------------
// Pipelines
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Sink {
    Json,
    Avro,
    Parquet,
}

impl Sink {
    fn name(self) -> &'static str {
        match self {
            Sink::Json => "json",
            Sink::Avro => "avro",
            Sink::Parquet => "parquet",
        }
    }
}

fn pipeline_name(meta: &str, sink: Sink, table: &str) -> String {
    format!(
        "p-{}-{}-{}",
        meta.to_lowercase().replace('_', "-"),
        sink.name(),
        table.replace('_', "-")
    )
}

fn topic(meta: &str, sink: Sink, table: &str) -> String {
    format!("probe-{}-{}-{table}", meta.to_lowercase(), sink.name())
}

fn s3_prefix(meta: &str, table: &str) -> String {
    format!("probe/{}/{table}", meta.to_lowercase())
}

async fn spec(
    i: &Infra,
    meta: &str,
    sink: Sink,
    db: &str,
    table: &str,
    snapshot: &str,
) -> PipelineSpec {
    let name = pipeline_name(meta, sink, table);
    let sink_yaml = match sink {
        Sink::Json => format!(
            r#"
    - type: kafka
      config:
        id: kafka
        brokers: "{brokers}"
        topic: {topic}
        envelope: {{type: native}}
        encoding: json
        required: true"#,
            brokers = brokers(),
            topic = topic(meta, sink, table),
        ),
        Sink::Avro => format!(
            r#"
    - type: kafka
      config:
        id: kafka
        brokers: "{brokers}"
        topic: {topic}
        envelope: {{type: native}}
        required: true
        encoding:
          type: avro
          schema_registry_url: "{sr}""#,
            brokers = brokers(),
            topic = topic(meta, sink, table),
            sr = sr_url(i).await,
        ),
        Sink::Parquet => format!(
            r#"
    - type: s3
      config:
        id: s3
        bucket: {BUCKET}
        prefix: {prefix}
        region: us-east-1
        endpoint: "{endpoint}"
        access_key_id: {ak}
        secret_access_key: {sk}
        format: parquet
        file_roll: {{max_events: 1, max_age_secs: 1, idle_age_secs: 1}}
        durability: {durability}
        required: true"#,
            durability = std::env::var("TYPE_PROBE_S3_DURABILITY")
                .unwrap_or_else(|_| "legacy_rolling".into()),
            prefix = s3_prefix(meta, table),
            endpoint = i.s3.endpoint,
            ak = s3_test_server::ACCESS_KEY,
            sk = s3_test_server::SECRET_KEY,
        ),
    };
    let yaml = format!(
        r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata: {{name: {name}, tenant: {TENANT}}}
spec:
  source:
    type: mysql
    config:
      id: {name}
      dsn: "{dsn}"
      tables: ["{db}.{table}"]
      snapshot: {{mode: {snapshot}}}
  processors: []
  sinks:{sink_yaml}
"#,
        dsn = dsn(i).await,
    );
    serde_yaml::from_str(&yaml).unwrap_or_else(|e| panic!("{e}: {yaml}"))
}

// ---------------------------------------------------------------------------
// Readers: every record image as {sink, op, image, id, columns: {name: tagged}}
// ---------------------------------------------------------------------------

fn id_of(v: &Value) -> Option<i64> {
    v.as_i64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

/// JSON values tagged with their JSON type.
fn tag_json(v: &Value) -> Value {
    let t = match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(n) if n.is_u64() => "u64",
        Value::Number(n) if n.is_i64() => "i64",
        Value::Number(_) => "f64",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    };
    json!({"type": t, "value": v})
}

fn images_json(env: &Value) -> Vec<(String, Value)> {
    ["before", "after"]
        .iter()
        .filter(|k| env[**k].is_object())
        .map(|k| {
            let cols: serde_json::Map<String, Value> = env[*k]
                .as_object()
                .unwrap()
                .iter()
                .map(|(c, v)| (c.clone(), tag_json(v)))
                .collect();
            (k.to_string(), Value::Object(cols))
        })
        .collect()
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn tag_avro(v: &Avro) -> Value {
    match v {
        Avro::Null => json!({"type": "null", "value": null}),
        Avro::Boolean(b) => json!({"type": "boolean", "value": b}),
        Avro::Int(x) => json!({"type": "int", "value": x}),
        Avro::Long(x) => json!({"type": "long", "value": x}),
        Avro::Float(x) => json!({"type": "float", "value": x}),
        Avro::Double(x) => json!({"type": "double", "value": x}),
        Avro::Bytes(b) => json!({"type": "bytes", "hex": hex(b)}),
        Avro::String(s) => json!({"type": "string", "value": s}),
        Avro::Fixed(n, b) => {
            json!({"type": format!("fixed({n})"), "hex": hex(b)})
        }
        Avro::Enum(i, s) => json!({"type": "enum", "index": i, "value": s}),
        Avro::Union(i, inner) => {
            let mut t = tag_avro(inner);
            t["union_branch"] = json!(i);
            t
        }
        Avro::Date(d) => json!({"type": "date", "value": d}),
        Avro::TimeMillis(x) => json!({"type": "time-millis", "value": x}),
        Avro::TimeMicros(x) => json!({"type": "time-micros", "value": x}),
        Avro::TimestampMillis(x) => {
            json!({"type": "timestamp-millis", "value": x})
        }
        Avro::TimestampMicros(x) => {
            json!({"type": "timestamp-micros", "value": x})
        }
        Avro::LocalTimestampMillis(x) => {
            json!({"type": "local-timestamp-millis", "value": x})
        }
        Avro::LocalTimestampMicros(x) => {
            json!({"type": "local-timestamp-micros", "value": x})
        }
        other => json!({"type": "other", "debug": format!("{other:?}")}),
    }
}

fn unwrap_union(v: &Avro) -> &Avro {
    match v {
        Avro::Union(_, inner) => inner,
        other => other,
    }
}

async fn decode_avro(i: &Infra, bytes: &[u8]) -> Value {
    let id = u32::from_be_bytes(bytes[1..5].try_into().unwrap());
    let body: Value =
        reqwest::get(format!("{}/schemas/ids/{id}", sr_url(i).await))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    let schema_text = body["schema"].as_str().unwrap().to_string();
    let schema = apache_avro::Schema::parse_str(&schema_text).unwrap();
    let value =
        apache_avro::from_avro_datum(&schema, &mut &bytes[5..], None).unwrap();
    let Avro::Record(fields) = value else {
        panic!("not a record")
    };
    let fields: BTreeMap<String, Avro> = fields.into_iter().collect();
    let op = match fields.get("op").map(unwrap_union) {
        Some(Avro::String(s)) => s.clone(),
        other => format!("{other:?}"),
    };
    let mut images = Vec::new();
    for k in ["before", "after"] {
        if let Some(Avro::Record(cols)) = fields.get(k).map(unwrap_union) {
            let tagged: serde_json::Map<String, Value> =
                cols.iter().map(|(c, v)| (c.clone(), tag_avro(v))).collect();
            images.push((k.to_string(), Value::Object(tagged)));
        }
    }
    json!({"op": op, "images": images, "schema_id": id, "schema": serde_json::from_str::<Value>(&schema_text).unwrap()})
}

async fn kafka_records(i: &Infra, topic: &str, sink: Sink) -> Vec<Value> {
    let c: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .set(
            "group.id",
            format!(
                "probe-{topic}-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ),
        )
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .unwrap();
    c.subscribe(&[topic]).unwrap();
    let mut out = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(4);
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(
            left.min(Duration::from_millis(1500)),
            c.recv(),
        )
        .await
        {
            Ok(Ok(m)) => {
                let payload = m.payload().unwrap_or_default();
                let rec = match sink {
                    Sink::Json => {
                        let env: Value =
                            serde_json::from_slice(payload).unwrap();
                        json!({"op": env["op"], "images": images_json(&env), "raw": env})
                    }
                    _ => decode_avro(i, payload).await,
                };
                out.push(rec);
            }
            Ok(Err(e))
                if e.rdkafka_error_code()
                    == Some(RDKafkaErrorCode::UnknownTopicOrPartition) =>
            {
                if Instant::now() >= deadline {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            Ok(Err(e)) => panic!("consume {topic}: {e}"),
            Err(_) => break,
        }
    }
    out
}

fn tag_arrow(a: &dyn Array, row: usize) -> Value {
    use arrow_array::cast::AsArray;
    use arrow_array::types::*;
    let dt = a.data_type().to_string();
    if a.is_null(row) {
        return json!({"type": dt, "value": null});
    }
    let v = match a.data_type() {
        DataType::Boolean => json!(a.as_boolean().value(row)),
        DataType::Int8 => json!(a.as_primitive::<Int8Type>().value(row)),
        DataType::Int16 => json!(a.as_primitive::<Int16Type>().value(row)),
        DataType::Int32 => json!(a.as_primitive::<Int32Type>().value(row)),
        DataType::Int64 => json!(a.as_primitive::<Int64Type>().value(row)),
        DataType::UInt8 => json!(a.as_primitive::<UInt8Type>().value(row)),
        DataType::UInt16 => json!(a.as_primitive::<UInt16Type>().value(row)),
        DataType::UInt32 => json!(a.as_primitive::<UInt32Type>().value(row)),
        DataType::UInt64 => json!(a.as_primitive::<UInt64Type>().value(row)),
        DataType::Float32 => json!(a.as_primitive::<Float32Type>().value(row)),
        DataType::Float64 => json!(a.as_primitive::<Float64Type>().value(row)),
        DataType::Utf8 => json!(a.as_string::<i32>().value(row)),
        DataType::LargeUtf8 => json!(a.as_string::<i64>().value(row)),
        DataType::Binary => {
            json!({"hex": hex(a.as_binary::<i32>().value(row))})
        }
        DataType::Date32 => json!(a.as_primitive::<Date32Type>().value(row)),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            json!(a.as_primitive::<TimestampMillisecondType>().value(row))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            json!(a.as_primitive::<TimestampMicrosecondType>().value(row))
        }
        DataType::Decimal128(_, _) => {
            json!(a.as_primitive::<Decimal128Type>().value_as_string(row))
        }
        _ => json!({"unsupported": format!("{a:?}")}),
    };
    json!({"type": dt, "value": v})
}

async fn parquet_records(i: &Infra, prefix: &str) -> (Vec<Value>, Vec<String>) {
    let store = build_object_store(&ObjectStoreParams::s3_compatible(
        BUCKET,
        &i.s3.endpoint,
        s3_test_server::ACCESS_KEY,
        s3_test_server::SECRET_KEY,
    ))
    .unwrap();
    let listed: Vec<object_store::ObjectMeta> =
        futures::TryStreamExt::try_collect(
            store.list(Some(&ObjPath::from(prefix))),
        )
        .await
        .unwrap_or_default();
    let mut objects: Vec<String> = Vec::new();
    let mut out = Vec::new();
    for obj in listed {
        objects.push(obj.location.to_string());
        if !obj.location.as_ref().ends_with(".parquet") {
            continue;
        }
        let reader =
            ParquetObjectReader::new(store.clone(), obj.location.clone())
                .with_file_size(obj.size);
        let Ok(builder) = ParquetRecordBatchStreamBuilder::new(reader).await
        else {
            continue;
        };
        let batches: Vec<RecordBatch> =
            futures::TryStreamExt::try_collect(builder.build().unwrap())
                .await
                .unwrap();
        for b in batches {
            let schema = b.schema();
            for r in 0..b.num_rows() {
                let mut op = Value::Null;
                let mut before = serde_json::Map::new();
                let mut after = serde_json::Map::new();
                for (f, col) in schema.fields().iter().zip(b.columns()) {
                    let t = tag_arrow(col.as_ref(), r);
                    if f.name() == "op" {
                        op = t["value"].clone();
                    } else if let Some(c) = f.name().strip_prefix("before_") {
                        before.insert(c.to_string(), t);
                    } else if let Some(c) = f.name().strip_prefix("after_") {
                        after.insert(c.to_string(), t);
                    }
                }
                let mut images = Vec::new();
                if before.values().any(|v| !v["value"].is_null()) {
                    images.push(("before".to_string(), Value::Object(before)));
                }
                if after.values().any(|v| !v["value"].is_null()) {
                    images.push(("after".to_string(), Value::Object(after)));
                }
                out.push(json!({"op": op, "images": images, "object": obj.location.to_string()}));
            }
        }
    }
    (out, objects)
}

/// The record ids an image set carries.
fn rec_id(rec: &Value) -> Option<i64> {
    rec["images"].as_array()?.iter().find_map(|img| {
        let cols = &img[1];
        id_of(&cols["id"]["value"])
    })
}

async fn records(i: &Infra, meta: &str, sink: Sink, table: &str) -> Vec<Value> {
    match sink {
        Sink::Parquet => parquet_records(i, &s3_prefix(meta, table)).await.0,
        _ => kafka_records(i, &topic(meta, sink, table), sink).await,
    }
}

// ---------------------------------------------------------------------------
// The probe
// ---------------------------------------------------------------------------

async fn env(i: &Infra, out: &Path) {
    let mut c = mysql_async::Conn::from_url(dsn(i).await).await.unwrap();
    let vars = [
        "version",
        "version_comment",
        "GLOBAL.sql_mode",
        "SESSION.sql_mode",
        "character_set_server",
        "collation_server",
        "character_set_client",
        "character_set_connection",
        "character_set_results",
        "character_set_database",
        "collation_connection",
        "default_collation_for_utf8mb4",
        "GLOBAL.time_zone",
        "SESSION.time_zone",
        "system_time_zone",
        "binlog_format",
        "binlog_row_image",
        "binlog_row_metadata",
        "binlog_row_value_options",
        "explicit_defaults_for_timestamp",
        "lower_case_table_names",
        "gtid_mode",
        "server_uuid",
    ];
    let mut m = serde_json::Map::new();
    for v in vars {
        let x: Option<String> = c
            .query_first(format!("SELECT CAST(@@{v} AS CHAR)"))
            .await
            .unwrap_or(None);
        m.insert(v.to_string(), json!(x));
    }
    let now: Option<(String, String)> = c
        .query_first(
            "SELECT CAST(NOW(6) AS CHAR), CAST(UTC_TIMESTAMP(6) AS CHAR)",
        )
        .await
        .unwrap();
    m.insert("server_now_and_utc".into(), json!(now));
    m.insert(
        "images".into(),
        json!({
            "mysql": format!("{}:{}", MYSQL_IMAGE.0, MYSQL_IMAGE.1),
            "kafka": format!("{}:{}", KAFKA_IMAGE.0, KAFKA_IMAGE.1),
            "schema_registry": format!("{}:{}", SCHEMA_REGISTRY_IMAGE.0, SCHEMA_REGISTRY_IMAGE.1),
            "rustfs": format!("{}:{}", s3_test_server::IMAGE, s3_test_server::TAG),
        }),
    );
    m.insert("server_args_default_time_zone".into(), json!(SERVER_TZ));
    m.insert("process_tz_env".into(), json!(std::env::var("TZ").ok()));
    m.insert(
        "process_local_offset".into(),
        json!(chrono::Local::now().format("%:z").to_string()),
    );
    m.insert(
        "deltaforge_revision".into(),
        json!(
            std::process::Command::new("git")
                .args(["rev-parse", "HEAD"])
                .output()
                .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
                .ok()
        ),
    );
    std::fs::write(
        out.join("env.json"),
        serde_json::to_vec_pretty(&m).unwrap(),
    )
    .unwrap();
}

/// One run: `row_metadata` is the binlog setting; `snapshot` the source's
/// snapshot mode (`initial`: snapshot rows then CDC; `never`: CDC only, so a
/// typed sink that cannot encode snapshot rows still shows its CDC output).
async fn probe(row_metadata: &str, snapshot: &str, out: &Path) {
    let meta_owned = format!("{row_metadata}_{snapshot}");
    let meta = meta_owned.as_str();
    let i = INFRA.get_or_init(start).await;
    let db = format!("probe_{}", meta.to_lowercase());
    let tables = tables();
    let mut sql = Sql::new(i).await;
    sql.run(
        &format!("SET GLOBAL binlog_row_metadata = {row_metadata}"),
        false,
    )
    .await;
    sql.run(&format!("DROP DATABASE IF EXISTS {db}"), false)
        .await;
    sql.run(&format!("CREATE DATABASE {db}"), false).await;
    for t in &tables {
        let cols: Vec<String> = t
            .columns
            .iter()
            .map(|(c, ty)| format!("{c} {ty}"))
            .collect();
        sql.run(
            &format!(
                "CREATE TABLE {db}.{} (id BIGINT PRIMARY KEY, {})",
                t.name,
                cols.join(", ")
            ),
            false,
        )
        .await;
        for refused in &t.refused {
            sql.mode(false).await;
            sql.run(&refused.replace("{db}", &db), true).await;
        }
        for (k, r) in t.rows.iter().enumerate() {
            sql.mode(r.permissive).await;
            sql.run(&insert(&db, t, k as i64 + 1, r), false).await;
        }
    }
    sql.mode(false).await;

    // Schema metadata as MySQL reports it.
    let mut c = mysql_async::Conn::from_url(dsn(i).await).await.unwrap();
    let columns: Vec<Value> = c
        .exec_map(
            "SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, DATA_TYPE, \
             CHARACTER_SET_NAME, COLLATION_NAME, NUMERIC_PRECISION, NUMERIC_SCALE, \
             DATETIME_PRECISION, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH \
             FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? \
             ORDER BY TABLE_NAME, ORDINAL_POSITION",
            (db.clone(),),
            |row: mysql_async::Row| {
                let mut row = row;
                json!({
                    "table": row.take::<String, _>(0),
                    "column": row.take::<String, _>(1),
                    "column_type": row.take::<String, _>(2),
                    "data_type": row.take::<String, _>(3),
                    "charset": row.take::<Option<String>, _>(4),
                    "collation": row.take::<Option<String>, _>(5),
                    "numeric_precision": row.take::<Option<u64>, _>(6),
                    "numeric_scale": row.take::<Option<u64>, _>(7),
                    "datetime_precision": row.take::<Option<u64>, _>(8),
                    "char_max_length": row.take::<Option<u64>, _>(9),
                    "char_octet_length": row.take::<Option<u64>, _>(10),
                })
            },
        )
        .await
        .unwrap();
    let uuid: String = c
        .query_first("SELECT @@server_uuid")
        .await
        .unwrap()
        .unwrap();

    // Pipelines.
    let backend: storage::ArcStorageBackend =
        Arc::new(storage::MemoryStorageBackend::new());
    let mgr = Arc::new(
        PipelineManager::with_backend(backend.clone())
            .await
            .unwrap(),
    );
    let sinks = [Sink::Json, Sink::Avro, Sink::Parquet];
    let mut names = Vec::new();
    for t in &tables {
        for s in sinks {
            mgr.create(spec(i, meta, s, &db, t.name, snapshot).await)
                .await
                .expect("create");
            names.push((s, t.name, pipeline_name(meta, s, t.name)));
        }
    }
    let deadline = Instant::now() + Duration::from_secs(180);
    for (_, _, name) in &names {
        loop {
            let st = mgr.get(name).await.map(|p| p.status).unwrap_or_default();
            if st == "running" || st == "failed" || Instant::now() > deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    // Streaming: a marker row reaches every pipeline that still runs.
    for t in &tables {
        sql.run(
            &format!("INSERT INTO {db}.{} (id) VALUES ({MARKER})", t.name),
            false,
        )
        .await;
    }
    let deadline = Instant::now() + Duration::from_secs(180);
    for (s, table, name) in &names {
        loop {
            let st = mgr.get(name).await.map(|p| p.status).unwrap_or_default();
            if st == "failed" {
                break;
            }
            let recs = records(i, meta, *s, table).await;
            if recs.iter().any(|r| rec_id(r) == Some(MARKER))
                || Instant::now() > deadline
            {
                break;
            }
        }
    }

    // CDC: insert, update to the next row's values, delete.
    for t in &tables {
        let n = t.rows.len();
        for (k, r) in t.rows.iter().enumerate() {
            sql.mode(r.permissive).await;
            sql.run(&insert(&db, t, 101 + k as i64, r), false).await;
        }
        sql.mode(true).await;
        for k in 0..n {
            sql.run(
                &update(&db, t, 101 + k as i64, &t.rows[(k + 1) % n]),
                false,
            )
            .await;
        }
        for k in 0..n {
            sql.run(
                &format!("DELETE FROM {db}.{} WHERE id = {}", t.name, 101 + k),
                false,
            )
            .await;
        }
        sql.mode(false).await;
    }

    // Collect: every expected image or the pipeline's end.
    let mut lines = Vec::new();
    let mut status = BTreeMap::new();
    for (s, table, name) in &names {
        let t = tables.iter().find(|t| t.name == *table).unwrap();
        // Snapshot rows (if any), the marker, inserts, updates, deletes.
        let per_row = if snapshot == "never" { 3 } else { 4 };
        let want = t.rows.len() * per_row + 1;
        let deadline = Instant::now() + Duration::from_secs(120);
        let recs = loop {
            let recs = records(i, meta, *s, table).await;
            let st = mgr.get(name).await.map(|p| p.status).unwrap_or_default();
            if recs.len() >= want || st == "failed" || Instant::now() > deadline
            {
                break recs;
            }
        };
        let info = mgr.get(name).await.ok();
        let incidents = mgr.incidents(name).await.ok();
        status.insert(
            name.clone(),
            json!({
                "sink": s.name(), "table": table,
                "status": info.map(|p| p.status),
                "records": recs.len(), "expected": want,
                "incidents": incidents,
            }),
        );
        for r in recs {
            let id = rec_id(&r);
            let label = id.and_then(|id| {
                let k = if id >= 101 { id - 101 } else { id - 1 };
                t.rows.get(k as usize).map(|r| r.label)
            });
            lines.push(json!({
                "meta": row_metadata, "snapshot": snapshot, "sink": s.name(), "table": table, "id": id,
                "row_label": label, "record": r,
            }));
        }
    }
    let mut buf = Vec::new();
    for l in &lines {
        buf.extend(serde_json::to_vec(l).unwrap());
        buf.push(b'\n');
    }
    std::fs::write(
        out.join(format!("records-{}.jsonl", meta.to_lowercase())),
        buf,
    )
    .unwrap();
    std::fs::write(
        out.join(format!("pipelines-{}.json", meta.to_lowercase())),
        serde_json::to_vec_pretty(&status).unwrap(),
    )
    .unwrap();

    // The registered source schema (the JSON pipeline's).
    let registry = storage::DurableSchemaRegistry::new(backend.clone())
        .await
        .unwrap();
    let hash = LineageDescriptor::mysql(&uuid).unwrap().lineage_hash();
    let mut registered = serde_json::Map::new();
    for t in &tables {
        let key = SchemaKey::new(
            TENANT,
            pipeline_name(meta, Sink::Json, t.name),
            &hash,
            &db,
            t.name,
        );
        let v = registry.get_latest(&key).await.ok().flatten();
        registered.insert(t.name.to_string(), json!(v.map(|v| v.schema_json)));
    }
    std::fs::write(
        out.join(format!("schema-{}.json", meta.to_lowercase())),
        serde_json::to_vec_pretty(&json!({
            "information_schema": columns,
            "registered": registered,
            "rows": tables.iter().map(|t| json!({
                "table": t.name,
                "columns": t.columns.iter().map(|(c, ty)| json!([c, ty])).collect::<Vec<_>>(),
                "rows": t.rows.iter().map(|r| json!({"label": r.label, "sql": r.values, "permissive": r.permissive})).collect::<Vec<_>>(),
            })).collect::<Vec<_>>(),
        }))
        .unwrap(),
    )
    .unwrap();
    let mut buf = Vec::new();
    for l in &sql.log {
        buf.extend(serde_json::to_vec(l).unwrap());
        buf.push(b'\n');
    }
    std::fs::write(out.join(format!("sql-{}.jsonl", meta.to_lowercase())), buf)
        .unwrap();
    for (_, _, name) in &names {
        mgr.delete(name).await.ok();
    }
}

fn out_dir() -> PathBuf {
    let dir = std::env::var_os("TYPE_PROBE_OUT")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../target/type-probe")
                .join(chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string())
        });
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "measurement probe: MySQL, Kafka, Schema Registry and RustFS"]
async fn probe_mysql_types_through_every_path() {
    let out = out_dir();
    eprintln!("type probe output: {}", out.display());
    let log =
        std::fs::File::create(out.join("deltaforge-warnings.log")).unwrap();
    let _ = tracing_subscriber::fmt()
        .with_env_filter("warn")
        .with_ansi(false)
        .with_writer(std::sync::Mutex::new(log))
        .try_init();
    let i = INFRA.get_or_init(start).await;
    env(i, &out).await;
    for row_metadata in ["MINIMAL", "FULL"] {
        for snapshot in ["initial", "never"] {
            probe(row_metadata, snapshot, &out).await;
        }
    }
}
