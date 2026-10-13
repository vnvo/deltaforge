//! Type parity: every value a source table holds reaches each sink
//! identically whether the snapshot or CDC read it.
//!
//! Each probed table runs through three real pipelines per run, built by the
//! pipeline manager from a live source: Kafka JSON (native envelope), Kafka
//! Avro (Schema Registry, DDL-derived schemas) and S3 Parquet (RustFS,
//! durable sink). One pipeline per (sink, table), so a refusal hides nothing
//! else. MySQL runs under `binlog_row_metadata` MINIMAL and FULL, both
//! sources under snapshot modes `initial` and `never`. Rows are inserted
//! (ids 1.., read by the snapshot), inserted again by CDC (ids 101..),
//! updated to the next row's values and deleted. Per sink and table:
//!
//! - a snapshot row's after image equals the CDC insert of the same values;
//! - an update's before image equals the row it changed and its after image
//!   the insert of the values it set; a delete's before image equals the
//!   update's after image;
//! - CDC output is identical across row-metadata settings and snapshot
//!   modes;
//! - no event is missing, duplicated or unexpected, and nothing is refused.
//!
//! Tables of values that must be refused assert the refusal instead:
//!
//! - a MySQL ENUM index 0 and a PostgreSQL array whose lower bound is not 1
//!   stop the source (`source_decode_failed`) with no event for the row;
//! - MySQL zero dates, PostgreSQL infinities and `24:00:00` keep parity in
//!   native JSON and are refused by Avro and Parquet
//!   (`sink_conversion_failed`);
//! - a PostgreSQL update that leaves a TOASTed value unchanged omits it from
//!   native JSON (listed in `unchanged_columns`) and is refused by Avro and
//!   Parquet.
//!
//! Raw output goes to `$TYPE_PROBE_OUT` (default `target/type-probe/<ts>`)
//! for `scripts/type-probe-report.py`: `env.json`, `schema-<run>.json`,
//! `records-<run>.jsonl` (one line per sink record), `sql-<run>.jsonl` and
//! `pipelines-<run>.json` (final status and incidents).
//!
//! ```bash
//! cargo test -p runner --test type_parity_e2e -- --include-ignored --nocapture --test-threads=1
//! ```

use std::collections::{BTreeMap, BTreeSet};
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
/// The PostgreSQL server's time zone, for the same reason.
const PG_SERVER_TZ: &str = "America/St_Johns";
const MARKER: i64 = 9_999;

/// Images pinned by digest (the reproducible inputs; the tags are for
/// reading only). RustFS is pinned by `s3_test_server`.
const MYSQL_IMAGE: (&str, &str) = (
    "mysql",
    "8.4@sha256:c36050afdca850f23cef85703f84c7531a5ae155a11b5ee1c60acb09937c4084",
);
const POSTGRES_IMAGE: (&str, &str) = (
    "postgres",
    "17@sha256:a426e44bac0b759c95894d68e1a0ac03ecc20b619f498a91aae373bf06d8508d",
);
const KAFKA_IMAGE: (&str, &str) = (
    "confluentinc/cp-kafka",
    "7.5.0@sha256:fbbb6fa11b258a88b83f54d4f0bddfcffbf2279f99d66a843486e3da7bdfbf41",
);
const SCHEMA_REGISTRY_IMAGE: (&str, &str) = (
    "confluentinc/cp-schema-registry",
    "7.5.0@sha256:e51684b472a2481f065f44616d3d8ad2182029a7011f949a612d35b54566a1f6",
);

/// Kafka, Schema Registry and RustFS, shared by every run.
struct Infra {
    kafka: ContainerAsync<GenericImage>,
    sr: ContainerAsync<GenericImage>,
    s3: &'static s3_test_server::S3Server,
}

static INFRA: OnceCell<Infra> = OnceCell::const_new();
static MYSQL: OnceCell<ContainerAsync<GenericImage>> = OnceCell::const_new();
static POSTGRES: OnceCell<ContainerAsync<GenericImage>> = OnceCell::const_new();

#[dtor]
fn cleanup() {
    let mut ids: Vec<&str> = Vec::new();
    if let Some(i) = INFRA.get() {
        ids.extend([i.sr.id(), i.kafka.id()]);
    }
    ids.extend(MYSQL.get().map(|c| c.id()));
    ids.extend(POSTGRES.get().map(|c| c.id()));
    for id in ids {
        std::process::Command::new("docker")
            .args(["rm", "-f", "-v", id])
            .output()
            .ok();
    }
    s3_test_server::remove_shared();
}

fn brokers() -> String {
    format!("localhost:{KAFKA_HOST_PORT}")
}

async fn infra() -> &'static Infra {
    INFRA.get_or_init(start).await
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
    let s3 = s3_test_server::shared().await;
    s3.create_bucket(BUCKET).await.expect("bucket");
    let infra = Infra { kafka, sr, s3 };
    let deadline = Instant::now() + Duration::from_secs(180);
    let http = reqwest::Client::new();
    loop {
        let sr_ok = http
            .get(format!("{}/subjects", sr_url(&infra).await))
            .send()
            .await
            .is_ok_and(|r| r.status().is_success());
        if sr_ok {
            break;
        }
        assert!(Instant::now() < deadline, "schema registry never ready");
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

async fn mysql() -> &'static ContainerAsync<GenericImage> {
    MYSQL
        .get_or_init(|| async {
            let c = GenericImage::new(MYSQL_IMAGE.0, MYSQL_IMAGE.1)
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
            let port = c.get_host_port_ipv4(3306).await.unwrap();
            let dsn = format!("mysql://root:{ROOT_PW}@127.0.0.1:{port}/");
            let deadline = Instant::now() + Duration::from_secs(180);
            while mysql_async::Conn::from_url(dsn.as_str()).await.is_err() {
                assert!(Instant::now() < deadline, "mysql never ready");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            c
        })
        .await
}

async fn mysql_dsn() -> String {
    let port = mysql().await.get_host_port_ipv4(3306).await.unwrap();
    format!("mysql://root:{ROOT_PW}@127.0.0.1:{port}/")
}

async fn postgres() -> &'static ContainerAsync<GenericImage> {
    POSTGRES
        .get_or_init(|| async {
            // One replication slot and walsender per pipeline.
            let c = GenericImage::new(POSTGRES_IMAGE.0, POSTGRES_IMAGE.1)
                .with_wait_for(WaitFor::message_on_stderr(
                    "database system is ready to accept connections",
                ))
                .with_env_var("POSTGRES_PASSWORD", ROOT_PW)
                .with_cmd([
                    "postgres",
                    "-c",
                    "wal_level=logical",
                    "-c",
                    "max_replication_slots=256",
                    "-c",
                    "max_wal_senders=256",
                    "-c",
                    "max_connections=600",
                    "-c",
                    &format!("timezone={PG_SERVER_TZ}"),
                ])
                .gate_owned()
                .start()
                .await
                .expect("start postgres");
            let port = c.get_host_port_ipv4(5432).await.unwrap();
            let dsn = pg_dsn_on(port);
            let deadline = Instant::now() + Duration::from_secs(180);
            while tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                .await
                .is_err()
            {
                assert!(Instant::now() < deadline, "postgres never ready");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            c
        })
        .await
}

fn pg_dsn_on(port: u16) -> String {
    format!(
        "host=127.0.0.1 port={port} user=postgres password={ROOT_PW} dbname=postgres"
    )
}

async fn pg_dsn() -> String {
    pg_dsn_on(postgres().await.get_host_port_ipv4(5432).await.unwrap())
}

async fn pg_client() -> tokio_postgres::Client {
    let (client, conn) =
        tokio_postgres::connect(&pg_dsn().await, tokio_postgres::NoTls)
            .await
            .unwrap();
    tokio::spawn(conn);
    client
}

// ---------------------------------------------------------------------------
// The probed values
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Engine {
    Mysql,
    Postgres,
}

/// What a table's values must do.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
enum Expect {
    /// Every sink keeps snapshot/CDC parity and refuses nothing.
    #[default]
    Parity,
    /// Native JSON keeps parity; Avro and Parquet refuse the first non-null
    /// row they meet (`sink_conversion_failed`) and emit no event for any.
    TypedRefused,
    /// The source refuses every non-null row (`source_decode_failed`): no
    /// sink emits an event for one.
    DecodeRefused,
    /// PostgreSQL: an update leaves a TOASTed value unchanged. Native JSON
    /// omits it (`unchanged_columns`); Avro and Parquet refuse the update.
    Toast,
}

/// One probed row: SQL literals per column, and whether the session must be
/// permissive (`sql_mode=''`) to store it (zero dates, invalid enum).
struct Row {
    label: &'static str,
    values: Vec<&'static str>,
    permissive: bool,
}

#[derive(Default)]
struct Table {
    name: &'static str,
    /// `(name, type)`; every column is nullable.
    columns: Vec<(&'static str, &'static str)>,
    rows: Vec<Row>,
    /// Statements expected to be refused (recorded with their error).
    refused: Vec<String>,
    /// Statements run after `CREATE TABLE` (`{db}` is the database or
    /// schema).
    setup: Vec<String>,
    expect: Expect,
    /// Exact native JSON values: `(row label, column, value)` of the row's
    /// inserted after image.
    json: Vec<(&'static str, &'static str, Value)>,
}

impl Table {
    /// The rows a refusing sink or source must never emit.
    fn refused_row(&self, k: usize) -> bool {
        self.expect != Expect::Parity
            && self.expect != Expect::Toast
            && self.rows[k].label != "null"
    }
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

/// `SET('m1', ..., 'm64')`.
fn wide_set() -> &'static str {
    let members: Vec<String> = (1..=64).map(|i| format!("'m{i}'")).collect();
    Box::leak(format!("SET({})", members.join(",")).into_boxed_str())
}

fn mysql_tables() -> Vec<Table> {
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
            ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
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
                nulls(2),
            ],
            refused: vec!["INSERT INTO {db}.t_enum (id, e) VALUES (900, 'zzz')".into()],
            ..Default::default()
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
            ..Default::default()
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
                nulls(8),
            ],
            refused: vec![
                "INSERT INTO {db}.t_time (id, d) VALUES (900, '0000-00-00')".into(),
                "INSERT INTO {db}.t_time (id, d) VALUES (901, '2023-02-29')".into(),
            ],
            ..Default::default()
        },
        Table {
            name: "t_set_wide",
            // 64 members: the bitmask needs all 64 bits (a DOUBLE cannot
            // hold it).
            columns: vec![("s", wide_set())],
            rows: vec![
                row("edges", &["'m1,m64'"]),
                row("top", &["'m64'"]),
                row("empty", &["''"]),
                nulls(1),
            ],
            json: vec![
                ("edges", "s", json!("m1,m64")),
                ("top", "s", json!("m64")),
                ("empty", "s", json!("")),
            ],
            ..Default::default()
        },
        Table {
            name: "t_enum_invalid",
            columns: vec![
                ("e", "ENUM('a','b','c')"),
                ("s", "SET('x','y','z')"),
            ],
            rows: vec![permissive("index_0", &["'zzz'", "'x,bogus'"]), nulls(2)],
            expect: Expect::DecodeRefused,
            ..Default::default()
        },
        Table {
            name: "t_time_zero",
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
            expect: Expect::TypedRefused,
            ..Default::default()
        },
    ]
}

// ---------------------------------------------------------------------------
// SQL with a log
// ---------------------------------------------------------------------------

/// A text value stored out of line: incompressible storage, about 6 KiB.
const PG_TOASTED: &str =
    "(SELECT string_agg(md5(g::text), '') FROM generate_series(1, 200) g)";

fn pg_tables() -> Vec<Table> {
    // Updates and deletes carry the full old row.
    let full =
        |t: &str| vec![format!("ALTER TABLE {{db}}.{t} REPLICA IDENTITY FULL")];
    vec![
        Table {
            name: "t_int",
            columns: vec![
                ("si", "smallint"),
                ("i", "integer"),
                ("bi", "bigint"),
                ("b", "boolean"),
            ],
            rows: vec![
                row("min", &["-32768", "-2147483648", "-9223372036854775808", "false"]),
                row("max", &["32767", "2147483647", "9223372036854775807", "true"]),
                row("zero", &["0", "0", "0", "false"]),
                nulls(4),
            ],
            setup: full("t_int"),
            ..Default::default()
        },
        Table {
            name: "t_num",
            columns: vec![
                ("n102", "numeric(10,2)"),
                ("n380", "numeric(38,0)"),
                ("n", "numeric"),
                ("r", "real"),
                ("d", "double precision"),
            ],
            rows: vec![
                row(
                    "max",
                    &[
                        "99999999.99",
                        "99999999999999999999999999999999999999",
                        "1e300",
                        "3.4028235e38",
                        "1.7976931348623157e308",
                    ],
                ),
                row(
                    "min",
                    &[
                        "-99999999.99",
                        "-99999999999999999999999999999999999999",
                        "-1e-300",
                        "-1.1754944e-38",
                        "5e-324",
                    ],
                ),
                row("nan", &["NULL", "NULL", "'NaN'", "'NaN'", "'NaN'"]),
                row(
                    "infinity",
                    &["NULL", "NULL", "'Infinity'", "'-Infinity'", "'Infinity'"],
                ),
                row("zero", &["0.00", "0", "-0.000", "'-0'", "'-0'"]),
                row("fraction", &["0.01", "1", "0.1", "0.1", "0.1"]),
                nulls(5),
            ],
            setup: full("t_num"),
            ..Default::default()
        },
        Table {
            name: "t_str",
            columns: vec![
                ("t", "text"),
                ("vc", "varchar(10)"),
                ("c", "char(5)"),
                ("u", "uuid"),
                ("js", "json"),
                ("jb", "jsonb"),
            ],
            rows: vec![
                row(
                    "unicode",
                    &[
                        r#"'ü 😀 \ "'"#,
                        "'abc'",
                        "'ab'",
                        "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'",
                        r#"'{"b": 1, "a": [1.0, null]}'"#,
                        r#"'{"b": 1, "a": [1.0, null]}'"#,
                    ],
                ),
                row(
                    "empty",
                    &[
                        "''",
                        "''",
                        "''",
                        "'00000000-0000-0000-0000-000000000000'",
                        r#"'"x"'"#,
                        "'null'",
                    ],
                ),
                row(
                    "escapes",
                    &[
                        r"E'tab\there\nnl'",
                        "' x '",
                        "'abcde'",
                        "'FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF'",
                        "'[]'",
                        r#"'{"k": 1e300, "u": "ü"}'"#,
                    ],
                ),
                nulls(6),
            ],
            setup: full("t_str"),
            ..Default::default()
        },
        Table {
            name: "t_bin",
            columns: vec![("by", "bytea")],
            rows: vec![
                row("bytes", &[r"'\x00ff10'"]),
                row("empty", &[r"'\x'"]),
                row("text", &[r"'\xdeadbeef'"]),
                nulls(1),
            ],
            setup: full("t_bin"),
            ..Default::default()
        },
        Table {
            name: "t_time",
            columns: vec![
                ("d", "date"),
                ("t", "time"),
                ("t3", "time(3)"),
                ("ts", "timestamp"),
                ("ts0", "timestamp(0)"),
                ("tz", "timestamptz"),
                ("iv", "interval"),
            ],
            rows: vec![
                row(
                    "min",
                    &[
                        "'4713-01-01 BC'",
                        "'00:00:00'",
                        "'00:00:00.001'",
                        "'4713-01-01 00:00:00 BC'",
                        "'4713-01-01 00:00:00 BC'",
                        "'4713-01-01 00:00:00+00 BC'",
                        "'-178000000 years'",
                    ],
                ),
                row(
                    "max",
                    &[
                        "'5874897-12-31'",
                        "'23:59:59.999999'",
                        "'23:59:59.999'",
                        "'200000-12-31 23:59:59.999999'",
                        "'200000-12-31 23:59:59'",
                        "'200000-12-31 23:59:59.999999+00'",
                        "'178000000 years'",
                    ],
                ),
                row(
                    "mid",
                    &[
                        "'2024-02-29'",
                        "'12:34:56.5'",
                        "'12:34:56.789'",
                        "'2024-03-31 02:30:00.123456'",
                        "'2024-03-31 02:30:00'",
                        "'2024-10-27 02:30:00.654321+03:30'",
                        "'1 year 2 mons 3 days 04:05:06.789'",
                    ],
                ),
                nulls(7),
            ],
            setup: full("t_time"),
            ..Default::default()
        },
        Table {
            name: "t_array",
            columns: vec![
                ("ia", "integer[]"),
                ("ta", "text[]"),
                ("ba", "boolean[]"),
                ("fa", "double precision[]"),
            ],
            rows: vec![
                row(
                    "values",
                    &[
                        "'{1,NULL,-2147483648}'",
                        r#"'{"a b","",NULL,"q\"uote","NULL"}'"#,
                        "'{t,f,NULL}'",
                        "'{1.5,NaN,-Infinity}'",
                    ],
                ),
                row("empty", &["'{}'", "'{}'", "'{}'", "'{}'"]),
                row(
                    "null_elements",
                    &["'{NULL}'", "'{NULL,NULL}'", "'{NULL}'", "'{NULL}'"],
                ),
                nulls(4),
            ],
            setup: full("t_array"),
            // A NULL array is null; an array of NULL elements is an array.
            json: vec![
                ("null", "ia", Value::Null),
                ("null_elements", "ia", json!([null])),
                ("null_elements", "ta", json!([null, null])),
                ("empty", "ia", json!([])),
                ("values", "ia", json!([1, null, -2147483648])),
                ("values", "ta", json!(["a b", "", null, "q\"uote", "NULL"])),
            ],
            ..Default::default()
        },
        Table {
            name: "t_array_nested",
            columns: vec![("ia", "integer[]"), ("ta", "text[]")],
            rows: vec![
                row(
                    "nested",
                    &["'{{1,NULL},{NULL,4}}'", r#"'{{"a",NULL},{"b","c"}}'"#],
                ),
                row("nested_null", &["'{{NULL}}'", "'{{NULL},{NULL}}'"]),
                nulls(2),
            ],
            setup: full("t_array_nested"),
            // Native JSON nests; a typed sink's one-dimensional slot refuses.
            expect: Expect::TypedRefused,
            json: vec![
                ("nested", "ia", json!([[1, null], [null, 4]])),
                ("nested", "ta", json!([["a", null], ["b", "c"]])),
                ("nested_null", "ia", json!([[null]])),
                ("nested_null", "ta", json!([[null], [null]])),
                ("null", "ia", Value::Null),
            ],
            ..Default::default()
        },
        Table {
            name: "t_time_edge",
            columns: vec![
                ("d", "date"),
                ("t", "time"),
                ("ts", "timestamp"),
                ("tz", "timestamptz"),
            ],
            rows: vec![
                row(
                    "infinity",
                    &["'infinity'", "'24:00:00'", "'infinity'", "'infinity'"],
                ),
                row(
                    "neg_infinity",
                    &["'-infinity'", "'24:00:00'", "'-infinity'", "'-infinity'"],
                ),
                nulls(4),
            ],
            setup: full("t_time_edge"),
            expect: Expect::TypedRefused,
            ..Default::default()
        },
        Table {
            name: "t_array_bounds",
            columns: vec![("ia", "integer[]")],
            rows: vec![row("lower_bound_0", &["'[0:1]={1,2}'"]), nulls(1)],
            setup: full("t_array_bounds"),
            expect: Expect::DecodeRefused,
            ..Default::default()
        },
        Table {
            name: "t_toast",
            columns: vec![("big", "text"), ("n", "integer")],
            rows: vec![row("toasted", &[PG_TOASTED, "1"])],
            setup: vec!["ALTER TABLE {db}.t_toast ALTER COLUMN big SET STORAGE EXTERNAL".into()],
            expect: Expect::Toast,
            ..Default::default()
        },
    ]
}

// ---------------------------------------------------------------------------
// SQL with a log
// ---------------------------------------------------------------------------

enum Conn {
    Mysql(mysql_async::Conn),
    Postgres(tokio_postgres::Client),
}

struct Sql {
    conn: Conn,
    log: Vec<Value>,
    mode: String,
}

impl Sql {
    async fn new(engine: Engine) -> Self {
        let conn = match engine {
            Engine::Mysql => Conn::Mysql(
                mysql_async::Conn::from_url(mysql_dsn().await)
                    .await
                    .unwrap(),
            ),
            Engine::Postgres => Conn::Postgres(pg_client().await),
        };
        Sql {
            conn,
            log: Vec::new(),
            mode: "default".into(),
        }
    }

    /// MySQL only: a permissive session stores zero dates and invalid
    /// enum values.
    async fn mode(&mut self, permissive: bool) {
        let Conn::Mysql(conn) = &mut self.conn else {
            return;
        };
        let want = if permissive { "permissive" } else { "default" };
        if self.mode != want {
            let stmt = if permissive {
                "SET SESSION sql_mode = ''"
            } else {
                "SET SESSION sql_mode = DEFAULT"
            };
            conn.query_drop(stmt).await.unwrap();
            self.mode = want.into();
        }
    }

    /// Run `stmt`; record its outcome and warnings. Panics on error unless
    /// `refusal_expected`.
    async fn run(&mut self, stmt: &str, refusal_expected: bool) {
        let (mode, r, warnings) = match &mut self.conn {
            Conn::Mysql(conn) => {
                let mode: Option<String> = conn
                    .query_first("SELECT @@SESSION.sql_mode")
                    .await
                    .unwrap();
                let r = conn.query_drop(stmt).await.map_err(|e| e.to_string());
                let warnings: Vec<(String, u32, String)> = if r.is_ok() {
                    conn.query_map("SHOW WARNINGS", |(l, c, m)| (l, c, m))
                        .await
                        .unwrap_or_default()
                } else {
                    Vec::new()
                };
                (mode, r, json!(warnings))
            }
            Conn::Postgres(client) => {
                let r = client.batch_execute(stmt).await.map_err(|e| {
                    e.as_db_error()
                        .map(|d| d.message().to_string())
                        .unwrap_or_else(|| e.to_string())
                });
                (None, r, json!([]))
            }
        };
        self.log.push(json!({
            "sql": stmt,
            "sql_mode": mode,
            "ok": r.is_ok(),
            "error": r.as_ref().err(),
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

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum Sink {
    Json,
    Avro,
    /// The durable S3 sink (`durable_v2`).
    Parquet,
    /// The legacy rolling S3 sink: its own per-row failure path.
    ParquetLegacy,
}

impl Sink {
    fn name(self) -> &'static str {
        match self {
            Sink::Json => "json",
            Sink::Avro => "avro",
            Sink::Parquet => "parquet",
            Sink::ParquetLegacy => "pqlegacy",
        }
    }

    /// The incident a sink raises for a table's values, if any.
    fn refusal(self, expect: Expect) -> Option<&'static str> {
        match (expect, self) {
            (Expect::Parity, _)
            | (Expect::TypedRefused | Expect::Toast, Sink::Json) => None,
            (Expect::TypedRefused | Expect::Toast, _) => {
                Some("sink_conversion_failed")
            }
            (Expect::DecodeRefused, _) => Some("source_decode_failed"),
        }
    }
}

const SINKS: [Sink; 4] =
    [Sink::Json, Sink::Avro, Sink::Parquet, Sink::ParquetLegacy];

/// One run: a source configuration and snapshot mode.
struct Run {
    engine: Engine,
    /// `MINIMAL_initial`, `pg_never`, ...: names the run's database,
    /// pipelines, topics and output files.
    name: String,
    snapshot: &'static str,
}

impl Run {
    fn db(&self) -> String {
        format!("probe_{}", self.name.to_lowercase())
    }

    fn initial(&self) -> bool {
        self.snapshot == "initial"
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

fn s3_prefix(meta: &str, sink: Sink, table: &str) -> String {
    format!("probe/{}/{}/{table}", meta.to_lowercase(), sink.name())
}

fn publication(db: &str, table: &str) -> String {
    format!("pub_{db}_{table}")
}

/// One sink's YAML: Kafka to `topic` (a template), or S3 under `prefix`.
async fn sink_yaml(i: &Infra, sink: Sink, topic: &str, prefix: &str) -> String {
    match sink {
        Sink::Json => format!(
            r#"
    - type: kafka
      config:
        id: kafka
        brokers: "{brokers}"
        topic: "{topic}"
        envelope: {{type: native}}
        encoding: json
        required: true"#,
            brokers = brokers(),
        ),
        Sink::Avro => format!(
            r#"
    - type: kafka
      config:
        id: kafka
        brokers: "{brokers}"
        topic: "{topic}"
        envelope: {{type: native}}
        required: true
        encoding:
          type: avro
          schema_registry_url: "{sr}""#,
            brokers = brokers(),
            sr = sr_url(i).await,
        ),
        Sink::Parquet | Sink::ParquetLegacy => format!(
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
            durability = if sink == Sink::Parquet {
                "durable_v2"
            } else {
                "legacy_rolling"
            },
            endpoint = i.s3.endpoint,
            ak = s3_test_server::ACCESS_KEY,
            sk = s3_test_server::SECRET_KEY,
        ),
    }
}

async fn spec(i: &Infra, run: &Run, sink: Sink, table: &str) -> PipelineSpec {
    let meta = run.name.as_str();
    let db = run.db();
    let name = pipeline_name(meta, sink, table);
    let sink_yaml = sink_yaml(
        i,
        sink,
        &topic(meta, sink, table),
        &s3_prefix(meta, sink, table),
    )
    .await;
    let source_yaml = match run.engine {
        Engine::Mysql => format!(
            r#"
    type: mysql
    config:
      id: {name}
      dsn: "{dsn}"
      tables: ["{db}.{table}"]
      snapshot: {{mode: {snapshot}}}"#,
            dsn = mysql_dsn().await,
            snapshot = run.snapshot,
        ),
        Engine::Postgres => format!(
            r#"
    type: postgres
    config:
      id: {name}
      dsn: "{dsn}"
      slot: {slot}
      publication: {publication}
      tables: ["{db}.{table}"]
      snapshot: {{mode: {snapshot}}}"#,
            dsn = pg_dsn().await,
            slot = name.replace('-', "_"),
            publication = publication(&db, table),
            snapshot = run.snapshot,
        ),
    };
    let yaml = format!(
        r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata: {{name: {name}, tenant: {TENANT}}}
spec:
  source:{source_yaml}
  processors: []
  sinks:{sink_yaml}
"#,
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
        Avro::Array(items) => {
            json!({"type": "array", "items": items.iter().map(tag_avro).collect::<Vec<_>>()})
        }
        Avro::Map(entries) => {
            let tagged: BTreeMap<&String, Value> =
                entries.iter().map(|(k, v)| (k, tag_avro(v))).collect();
            json!({"type": "map", "entries": tagged})
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
        DataType::FixedSizeBinary(_) => {
            json!({"hex": hex(a.as_fixed_size_binary().value(row))})
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            json!(a.as_primitive::<Time64MicrosecondType>().value(row))
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            json!(a.as_primitive::<DurationMicrosecondType>().value(row))
        }
        DataType::List(_) => {
            let items = a.as_list::<i32>().value(row);
            json!(
                (0..items.len())
                    .map(|k| tag_arrow(items.as_ref(), k))
                    .collect::<Vec<_>>()
            )
        }
        other => panic!("no reader for Arrow type {other}"),
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
                let mut source = serde_json::Map::new();
                let mut before = serde_json::Map::new();
                let mut after = serde_json::Map::new();
                for (f, col) in schema.fields().iter().zip(b.columns()) {
                    let t = tag_arrow(col.as_ref(), r);
                    if f.name() == "op" {
                        op = t["value"].clone();
                    } else if let Some(k) = f.name().strip_prefix("source_") {
                        source.insert(k.to_string(), t["value"].clone());
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
                out.push(json!({"op": op, "images": images, "source": source, "object": obj.location.to_string()}));
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
        Sink::Parquet | Sink::ParquetLegacy => {
            parquet_records(i, &s3_prefix(meta, sink, table)).await.0
        }
        _ => kafka_records(i, &topic(meta, sink, table), sink).await,
    }
}

// ---------------------------------------------------------------------------
// The environment
// ---------------------------------------------------------------------------

async fn mysql_env(out: &Path) {
    let mut c = mysql_async::Conn::from_url(mysql_dsn().await)
        .await
        .unwrap();
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
            "postgres": format!("{}:{}", POSTGRES_IMAGE.0, POSTGRES_IMAGE.1),
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

async fn pg_env(out: &Path) {
    let c = pg_client().await;
    let mut m = serde_json::Map::new();
    for v in [
        "server_version",
        "TimeZone",
        "DateStyle",
        "IntervalStyle",
        "extra_float_digits",
        "server_encoding",
        "wal_level",
    ] {
        let row = c.query_one(&format!("SHOW {v}"), &[]).await.unwrap();
        m.insert(v.to_string(), json!(row.get::<_, String>(0)));
    }
    m.insert("server_args_timezone".into(), json!(PG_SERVER_TZ));
    std::fs::write(
        out.join("env-postgres.json"),
        serde_json::to_vec_pretty(&m).unwrap(),
    )
    .unwrap();
}

// ---------------------------------------------------------------------------
// A run
// ---------------------------------------------------------------------------

/// Per `(op, id)`: the record's images (`{"before": {...}, "after":
/// {...}}`), without the `id` column.
type Images = BTreeMap<(String, i64), Value>;

/// Per `(sink, table)`: what the pipeline emitted.
type RunImages = BTreeMap<(Sink, &'static str), Images>;

fn tables_of(engine: Engine) -> Vec<Table> {
    match engine {
        Engine::Mysql => mysql_tables(),
        Engine::Postgres => pg_tables(),
    }
}

/// The reason codes of a pipeline's incidents.
async fn reasons(mgr: &PipelineManager, name: &str) -> Vec<String> {
    let v = mgr.incidents(name).await.unwrap_or(Value::Null);
    v["incidents"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|i| i["reason_code"].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

async fn create_tables(run: &Run, tables: &[Table], sql: &mut Sql) {
    let db = run.db();
    match run.engine {
        Engine::Mysql => {
            let row_metadata = run.name.split('_').next().unwrap();
            sql.run(
                &format!("SET GLOBAL binlog_row_metadata = {row_metadata}"),
                false,
            )
            .await;
            sql.run(&format!("DROP DATABASE IF EXISTS {db}"), false)
                .await;
            sql.run(&format!("CREATE DATABASE {db}"), false).await;
        }
        Engine::Postgres => {
            sql.run(&format!("DROP SCHEMA IF EXISTS {db} CASCADE"), false)
                .await;
            sql.run(&format!("CREATE SCHEMA {db}"), false).await;
        }
    }
    for t in tables {
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
        for s in &t.setup {
            sql.run(&s.replace("{db}", &db), false).await;
        }
        if run.engine == Engine::Postgres {
            sql.run(
                &format!(
                    "CREATE PUBLICATION {} FOR TABLE {db}.{}",
                    publication(&db, t.name),
                    t.name
                ),
                false,
            )
            .await;
        }
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
    if run.engine == Engine::Postgres {
        // The immutable-publication contract: a source streams only from a
        // registered publication.
        let pubs: Vec<String> =
            tables.iter().map(|t| publication(&db, t.name)).collect();
        sources::postgres::postgres_publication::register(
            &pg_client().await,
            &pubs,
        )
        .await
        .expect("register publications");
    }
}

/// CDC: insert every row again (ids 101..), update each to the next row's
/// values, delete them. A TOAST table updates only its other column.
async fn change_rows(run: &Run, tables: &[Table], sql: &mut Sql) {
    let db = run.db();
    for t in tables {
        let n = t.rows.len();
        for (k, r) in t.rows.iter().enumerate() {
            sql.mode(r.permissive).await;
            sql.run(&insert(&db, t, 101 + k as i64, r), false).await;
        }
        sql.mode(true).await;
        for k in 0..n {
            let stmt = if t.expect == Expect::Toast {
                format!(
                    "UPDATE {db}.{} SET n = n + 1 WHERE id = {}",
                    t.name,
                    101 + k
                )
            } else {
                update(&db, t, 101 + k as i64, &t.rows[(k + 1) % n])
            };
            sql.run(&stmt, false).await;
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
}

/// The `(op, id)` of every event a pipeline that refuses nothing emits.
fn expected_keys(run: &Run, t: &Table) -> BTreeSet<(String, i64)> {
    let mut keys = BTreeSet::new();
    for k in 0..t.rows.len() as i64 {
        if run.initial() {
            keys.insert(("r".to_string(), k + 1));
        }
        for op in ["c", "u", "d"] {
            keys.insert((op.to_string(), 101 + k));
        }
    }
    keys
}

/// The rows whose values an event's images carry: a snapshot read or an
/// insert its own; an update its own (before) and the next (after); a
/// delete the next (the values its row was updated to).
fn carried_rows(op: &str, id: i64, n: usize) -> Vec<usize> {
    let Some(k) = row_of(id) else { return vec![] };
    match op {
        "r" | "c" => vec![k],
        "u" => vec![k, (k + 1) % n],
        _ => vec![(k + 1) % n],
    }
}

/// The row a record id belongs to (snapshot ids 1.., CDC ids 101..).
fn row_of(id: i64) -> Option<usize> {
    match id {
        1..=100 => Some(id as usize - 1),
        101..=200 => Some(id as usize - 101),
        _ => None,
    }
}

/// A record's images keyed by `(op, id)`, without the `id` column. Parquet's
/// extension columns must be null (no processor ran) and are dropped.
fn normalize(ctx: &str, recs: &[Value], failures: &mut Vec<String>) -> Images {
    let mut out = Images::new();
    for r in recs {
        let op = r["op"].as_str().unwrap_or_default().to_string();
        let Some(id) = rec_id(r) else {
            failures.push(format!("{ctx}: a record without an id: {r}"));
            continue;
        };
        // The streaming barrier, not a probed value.
        if id == MARKER {
            continue;
        }
        let mut images = serde_json::Map::new();
        for img in r["images"].as_array().into_iter().flatten() {
            let mut cols = img[1].as_object().cloned().unwrap_or_default();
            cols.remove("id");
            if let Some(x) = cols.remove("extensions")
                && !x["value"].is_null()
            {
                failures.push(format!(
                    "{ctx}: ({op}, {id}) carries extensions {x}"
                ));
            }
            images.insert(
                img[0].as_str().unwrap().to_string(),
                Value::Object(cols),
            );
        }
        if out
            .insert((op.clone(), id), Value::Object(images))
            .is_some()
        {
            failures.push(format!("{ctx}: ({op}, {id}) emitted twice"));
        }
    }
    out
}

fn image<'a>(
    m: &'a Images,
    op: &str,
    id: i64,
    which: &str,
) -> Option<&'a Value> {
    m.get(&(op.to_string(), id)).and_then(|v| v.get(which))
}

fn expect_eq(
    failures: &mut Vec<String>,
    ctx: &str,
    what: &str,
    got: Option<&Value>,
    want: Option<&Value>,
) {
    if got != want || got.is_none() {
        let show = |v: Option<&Value>| {
            let s = v.map(Value::to_string).unwrap_or("absent".into());
            if s.len() > 600 {
                format!("{}...", &s[..600])
            } else {
                s
            }
        };
        failures.push(format!(
            "{ctx}: {what}\n    got  {}\n    want {}",
            show(got),
            show(want)
        ));
    }
}

/// The assertions for one pipeline; returns its normalized images.
fn check(
    run: &Run,
    t: &Table,
    sink: Sink,
    recs: &[Value],
    reasons: &[String],
    failures: &mut Vec<String>,
) -> Images {
    let ctx = format!("{} {} {}", run.name, sink.name(), t.name);
    let m = normalize(&ctx, recs, failures);
    if sink == Sink::Json {
        for (label, column, want) in &t.json {
            let k =
                t.rows.iter().position(|r| r.label == *label).unwrap() as i64;
            let got =
                image(&m, "c", 101 + k, "after").map(|a| &a[*column]["value"]);
            expect_eq(
                failures,
                &ctx,
                &format!("row {label}: native JSON of {column}"),
                got,
                Some(want),
            );
        }
    }
    let refusal = sink.refusal(t.expect);
    match refusal {
        Some(r) if !reasons.iter().any(|x| x == r) => failures
            .push(format!("{ctx}: expected incident {r}, got {reasons:?}")),
        None if !reasons.is_empty() => {
            failures.push(format!("{ctx}: unexpected incidents {reasons:?}"))
        }
        _ => {}
    }
    let keys: BTreeSet<_> = m.keys().cloned().collect();
    let want = expected_keys(run, t);
    let n = t.rows.len() as i64;

    if t.expect == Expect::Toast {
        // One row: snapshot 1, insert/update/delete 101.
        let typed = refusal.is_some();
        if typed {
            // Never the refused update; the insert before it and the
            // delete after it may share its batch.
            let allowed: BTreeSet<(String, i64)> =
                [("r".into(), 1), ("c".into(), 101), ("d".into(), 101)].into();
            let snap = ("r".to_string(), 1);
            if !keys.is_subset(&allowed)
                || (run.initial() != keys.contains(&snap))
            {
                failures.push(format!("{ctx}: events {keys:?}"));
            }
        } else if keys != want {
            failures.push(format!("{ctx}: events {keys:?}, expected {want:?}"));
        }
        let ins = image(&m, "c", 101, "after");
        if run.initial() && ins.is_some() {
            expect_eq(
                failures,
                &ctx,
                "snapshot = insert",
                image(&m, "r", 1, "after"),
                ins,
            );
        }
        if !typed {
            let mut changed = ins.cloned().unwrap_or_default();
            if let Some(o) = changed.as_object_mut() {
                o.remove("big");
                let ty = o.get("n").map(|n| n["type"].clone());
                o.insert("n".into(), json!({"type": ty, "value": 2}));
            }
            expect_eq(
                failures,
                &ctx,
                "update after omits the unchanged TOAST value",
                image(&m, "u", 101, "after"),
                Some(&changed),
            );
            let raw = recs
                .iter()
                .find(|r| r["op"] == "u")
                .map(|r| r["raw"]["unchanged_columns"].clone());
            expect_eq(
                failures,
                &ctx,
                "unchanged_columns",
                raw.as_ref(),
                Some(&json!(["big"])),
            );
            expect_eq(
                failures,
                &ctx,
                "update before (default identity: none)",
                image(&m, "u", 101, "before").or(Some(&Value::Null)),
                Some(&Value::Null),
            );
            expect_eq(
                failures,
                &ctx,
                "delete before is the key only",
                image(&m, "d", 101, "before"),
                Some(&json!({})),
            );
        }
        return m;
    }

    if refusal.is_some() {
        // No event carries a refused row's values; nothing outside the
        // expected events. Events after the refused one in its batch may be
        // delivered: the checkpoint stays before the batch, so a restart
        // delivers them again (see `assert_refusals_hold`).
        for (op, id) in &keys {
            if !want.contains(&(op.clone(), *id)) {
                failures.push(format!("{ctx}: unexpected event ({op}, {id})"));
            } else if carried_rows(op, *id, t.rows.len())
                .into_iter()
                .any(|k| t.refused_row(k))
            {
                failures.push(format!(
                    "{ctx}: an event carrying a refused row was emitted ({op}, {id})"
                ));
            }
        }
        return m;
    }

    if keys != want {
        let missing: Vec<_> = want.difference(&keys).collect();
        let extra: Vec<_> = keys.difference(&want).collect();
        failures
            .push(format!("{ctx}: missing {missing:?}, unexpected {extra:?}"));
    }
    for k in 0..n {
        let label = t.rows[k as usize].label;
        let ins = image(&m, "c", 101 + k, "after");
        if run.initial() {
            expect_eq(
                failures,
                &ctx,
                &format!("row {label}: snapshot = insert"),
                image(&m, "r", k + 1, "after"),
                ins,
            );
        }
        expect_eq(
            failures,
            &ctx,
            &format!("row {label}: update before = insert"),
            image(&m, "u", 101 + k, "before"),
            ins,
        );
        expect_eq(
            failures,
            &ctx,
            &format!("row {label}: update after = insert of the values it set"),
            image(&m, "u", 101 + k, "after"),
            image(&m, "c", 101 + (k + 1) % n, "after"),
        );
        expect_eq(
            failures,
            &ctx,
            &format!("row {label}: delete before = update after"),
            image(&m, "d", 101 + k, "before"),
            image(&m, "u", 101 + k, "after"),
        );
    }
    m
}

/// Whether a pipeline is done: halted by an incident, or every expected
/// event arrived.
async fn settled(
    mgr: &PipelineManager,
    name: &str,
    recs: &[Value],
    want: usize,
) -> bool {
    recs.len() >= want || !reasons(mgr, name).await.is_empty()
}

async fn run(run: &Run, out: &Path, failures: &mut Vec<String>) -> RunImages {
    let i = infra().await;
    let meta = run.name.as_str();
    let tables = tables_of(run.engine);
    let mut sql = Sql::new(run.engine).await;
    create_tables(run, &tables, &mut sql).await;

    let backend: storage::ArcStorageBackend =
        Arc::new(storage::MemoryStorageBackend::new());
    let mgr = Arc::new(
        PipelineManager::with_backend(backend.clone())
            .await
            .unwrap(),
    );
    let mut names = Vec::new();
    for t in &tables {
        for s in SINKS {
            mgr.create(spec(i, run, s, t.name).await)
                .await
                .expect("create");
            names.push((s, t, pipeline_name(meta, s, t.name)));
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

    // Streaming barrier: a "running" pipeline may still be starting, so a
    // marker row is written again until every pipeline of its table has
    // emitted it (as a snapshot or a CDC row) or halted.
    let db = run.db();
    for t in &tables {
        let deadline = Instant::now() + Duration::from_secs(180);
        let mut pending: Vec<_> =
            names.iter().filter(|(_, x, _)| x.name == t.name).collect();
        while !pending.is_empty() && Instant::now() < deadline {
            sql.run(
                &format!("DELETE FROM {db}.{} WHERE id = {MARKER}", t.name),
                false,
            )
            .await;
            sql.run(
                &format!("INSERT INTO {db}.{} (id) VALUES ({MARKER})", t.name),
                false,
            )
            .await;
            let wait = Instant::now() + Duration::from_secs(10);
            while !pending.is_empty() && Instant::now() < wait {
                let mut still = Vec::new();
                for p in pending {
                    let (s, _, name) = p;
                    let seen = records(i, meta, *s, t.name)
                        .await
                        .iter()
                        .any(|r| rec_id(r) == Some(MARKER));
                    if !seen && reasons(&mgr, name).await.is_empty() {
                        still.push(p);
                    }
                }
                pending = still;
            }
        }
        assert!(
            pending.is_empty(),
            "{}: within 180 s these pipelines neither emitted the streaming \
             marker nor halted: {:?}",
            t.name,
            pending.iter().map(|(_, _, n)| n).collect::<Vec<_>>()
        );
    }

    change_rows(run, &tables, &mut sql).await;

    // Collect every expected event, or the pipeline's halt.
    let mut lines = Vec::new();
    let mut status = BTreeMap::new();
    let mut images = RunImages::new();
    for (s, t, name) in &names {
        let want = expected_keys(run, t).len();
        let deadline = Instant::now() + Duration::from_secs(120);
        let mut recs = loop {
            let recs = records(i, meta, *s, t.name).await;
            if settled(&mgr, name, &recs, want).await
                || Instant::now() > deadline
            {
                break recs;
            }
        };
        if s.refusal(t.expect).is_some() {
            // A halted pipeline must stay halted: nothing may follow.
            tokio::time::sleep(Duration::from_secs(3)).await;
            recs = records(i, meta, *s, t.name).await;
        }
        let reasons = reasons(&mgr, name).await;
        status.insert(
            name.clone(),
            json!({
                "sink": s.name(), "table": t.name,
                "status": mgr.get(name).await.ok().map(|p| p.status),
                "records": recs.len(), "expected": want,
                "incidents": mgr.incidents(name).await.ok(),
            }),
        );
        images
            .insert((*s, t.name), check(run, t, *s, &recs, &reasons, failures));
        for r in recs {
            let id = rec_id(&r);
            let label = id
                .and_then(row_of)
                .and_then(|k| t.rows.get(k))
                .map(|r| r.label);
            lines.push(json!({
                "meta": meta, "snapshot": run.snapshot, "sink": s.name(), "table": t.name,
                "id": id, "row_label": label, "record": r,
            }));
        }
    }

    assert_refusals_hold(&mgr, i, run, &names, failures).await;

    let file = |kind: &str, ext: &str| {
        out.join(format!("{kind}-{}.{ext}", meta.to_lowercase()))
    };
    let jsonl = |v: &[Value]| {
        let mut buf = Vec::new();
        for l in v {
            buf.extend(serde_json::to_vec(l).unwrap());
            buf.push(b'\n');
        }
        buf
    };
    std::fs::write(file("records", "jsonl"), jsonl(&lines)).unwrap();
    std::fs::write(file("sql", "jsonl"), jsonl(&sql.log)).unwrap();
    std::fs::write(
        file("pipelines", "json"),
        serde_json::to_vec_pretty(&status).unwrap(),
    )
    .unwrap();
    std::fs::write(
        file("schema", "json"),
        serde_json::to_vec_pretty(&json!({
            "rows": tables.iter().map(|t| json!({
                "table": t.name,
                "expect": format!("{:?}", t.expect),
                "columns": t.columns.iter().map(|(c, ty)| json!([c, ty])).collect::<Vec<_>>(),
                "rows": t.rows.iter().map(|r| json!({"label": r.label, "sql": r.values, "permissive": r.permissive})).collect::<Vec<_>>(),
            })).collect::<Vec<_>>(),
        }))
        .unwrap(),
    )
    .unwrap();
    for (_, _, name) in &names {
        mgr.delete(name).await.ok();
    }
    if run.engine == Engine::Postgres {
        // The run's slots outlive its deleted pipelines: drop them once
        // their walsenders have gone.
        // Slots are the pipeline names with `_` for `-`: `p_<run>_...`.
        let prefix = format!("p_{}_", meta.to_lowercase());
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            sql.run(
                &format!(
                    "SELECT pg_drop_replication_slot(slot_name) FROM \
                     pg_replication_slots WHERE NOT active AND slot_name LIKE '{prefix}%'"
                ),
                false,
            )
            .await;
            let left = pg_client()
                .await
                .query_one(
                    "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE $1",
                    &[&format!("{prefix}%")],
                )
                .await
                .unwrap()
                .get::<_, i64>(0);
            if left == 0 || Instant::now() > deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
    images
}

/// How often a pipeline met `reason`, over all its incidents of it (an
/// incident a verified start settles is per recovery epoch: each restart's
/// occurrence is its own incident).
async fn occurrences(mgr: &PipelineManager, name: &str, reason: &str) -> u64 {
    let Ok(v) = mgr.incidents(name).await else {
        return 0;
    };
    v["incidents"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|i| i["reason_code"] == reason)
        .filter_map(|i| i["occurrences"].as_u64())
        .sum()
}

/// A pipeline's sink checkpoint positions.
async fn positions(mgr: &PipelineManager, name: &str) -> Vec<(String, Value)> {
    mgr.checkpoints(name)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|c| (c.sink_id, c.position))
        .collect()
}

/// Every refusal holds: the pipeline raised its incident once (the source
/// or sink did not retry it), its checkpoints do not move while it is
/// halted, and a restart meets the same refusal again (the refused event
/// was neither skipped nor acknowledged) and still emits no event carrying
/// a refused row.
async fn assert_refusals_hold(
    mgr: &PipelineManager,
    i: &Infra,
    run: &Run,
    names: &[(Sink, &Table, String)],
    failures: &mut Vec<String>,
) {
    let refusing: Vec<_> = names
        .iter()
        .filter_map(|(s, t, n)| s.refusal(t.expect).map(|r| (*s, *t, n, r)))
        .collect();
    let mut held = Vec::new();
    for (_, _, name, reason) in &refusing {
        let occurrences = occurrences(mgr, name, reason).await;
        if occurrences != 1 {
            failures.push(format!(
                "{name}: {reason} occurred {occurrences} times before any \
                 restart, expected once (no retry)"
            ));
        }
        held.push(positions(mgr, name).await);
    }
    tokio::time::sleep(Duration::from_secs(5)).await;
    for ((_, _, name, _), before) in refusing.iter().zip(&held) {
        if positions(mgr, name).await != *before {
            failures.push(format!("{name}: a checkpoint moved while halted"));
        }
        mgr.resume(name).await.ok();
    }
    let deadline = Instant::now() + Duration::from_secs(300);
    for (sink, t, name, reason) in &refusing {
        loop {
            if occurrences(mgr, name, reason).await >= 2 {
                break;
            }
            if Instant::now() > deadline {
                failures.push(format!(
                    "{name}: after a restart the pipeline did not meet its \
                     {reason} refusal again"
                ));
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        for r in records(i, &run.name, *sink, t.name).await {
            let op = r["op"].as_str().unwrap_or_default();
            if let Some(id) = rec_id(&r)
                && carried_rows(op, id, t.rows.len())
                    .into_iter()
                    .any(|k| t.refused_row(k))
            {
                failures.push(format!(
                    "{name}: after a restart an event carrying a refused row \
                     was emitted ({op}, {id})"
                ));
            }
        }
    }
}

/// CDC output (ids 101.. and the marker) is the same in every run; snapshot
/// output is the same in every run that snapshots.
fn compare_runs(runs: &[(Run, RunImages)], failures: &mut Vec<String>) {
    let cdc = |m: &Images| -> Images {
        m.iter()
            .filter(|((op, _), _)| op != "r")
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    };
    let snap = |m: &Images| -> Images {
        m.iter()
            .filter(|((op, _), _)| op == "r")
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    };
    let (first, base) = &runs[0];
    let tables = tables_of(first.engine);
    // A refusing pipeline halts at a point that depends on the run.
    let comparable = |(sink, table): &(Sink, &str)| {
        tables
            .iter()
            .any(|t| t.name == *table && sink.refusal(t.expect).is_none())
    };
    for (other, imgs) in &runs[1..] {
        for (key, m) in imgs {
            let Some(b) = base.get(key).filter(|_| comparable(key)) else {
                continue;
            };
            let ctx = format!(
                "{} vs {} {} {}",
                first.name,
                other.name,
                key.0.name(),
                key.1
            );
            if cdc(m) != cdc(b) {
                failures.push(format!("{ctx}: CDC output differs"));
            }
            if first.initial() && other.initial() && snap(m) != snap(b) {
                failures.push(format!("{ctx}: snapshot output differs"));
            }
        }
    }
}

fn out_dir() -> PathBuf {
    static DIR: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    DIR.get_or_init(|| {
        let dir = std::env::var_os("TYPE_PROBE_OUT")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../../target/type-probe")
                    .join(
                        chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string(),
                    )
            });
        std::fs::create_dir_all(&dir).unwrap();
        let log =
            std::fs::File::create(dir.join("deltaforge-warnings.log")).unwrap();
        let _ = tracing_subscriber::fmt()
            .with_env_filter("warn")
            .with_ansi(false)
            .with_writer(std::sync::Mutex::new(log))
            .try_init();
        eprintln!("type parity output: {}", dir.display());
        dir
    })
    .clone()
}

async fn assert_parity(engine: Engine, names: &[&str]) {
    let out = out_dir();
    let mut failures = Vec::new();
    let mut runs = Vec::new();
    for name in names {
        let r = Run {
            engine,
            name: name.to_string(),
            snapshot: if name.ends_with("initial") {
                "initial"
            } else {
                "never"
            },
        };
        let images = run(&r, &out, &mut failures).await;
        runs.push((r, images));
    }
    compare_runs(&runs, &mut failures);
    assert!(
        failures.is_empty(),
        "{} parity failures (raw output in {}):\n{}",
        failures.len(),
        out.display(),
        failures.join("\n")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "live: MySQL, Kafka, Schema Registry and RustFS"]
async fn mysql_values_keep_parity_across_snapshot_cdc_and_sinks() {
    let out = out_dir();
    mysql_env(&out).await;
    assert_parity(
        Engine::Mysql,
        &[
            "MINIMAL_initial",
            "FULL_initial",
            "MINIMAL_never",
            "FULL_never",
        ],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "live: PostgreSQL, Kafka, Schema Registry and RustFS"]
async fn postgres_values_keep_parity_across_snapshot_cdc_and_sinks() {
    let out = out_dir();
    pg_env(&out).await;
    assert_parity(Engine::Postgres, &["pg_initial", "pg_never"]).await;
}

/// Two schemas of one database hold identically named tables with the same
/// DDL. Snapshot and CDC events keep them apart everywhere: `source.db` is
/// the database and `source.schema` the schema; each table has its own
/// schema versions; routing (Kafka topics templated by schema), Avro record
/// names and subjects, and S3 partitions and objects never mix them.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "live: PostgreSQL, Kafka, Schema Registry and RustFS"]
async fn postgres_same_named_tables_in_two_schemas_keep_separate_identities() {
    let _ = out_dir();
    let i = infra().await;
    let schemas = ["ident_a", "ident_b"];
    let mut sql = Sql::new(Engine::Postgres).await;
    for s in schemas {
        sql.run(&format!("CREATE SCHEMA {s}"), false).await;
        sql.run(
            &format!("CREATE TABLE {s}.t (id BIGINT PRIMARY KEY, v TEXT)"),
            false,
        )
        .await;
        sql.run(&format!("INSERT INTO {s}.t VALUES (1, '{s}')"), false)
            .await;
    }
    sql.run(
        "CREATE PUBLICATION pub_ident FOR TABLE ident_a.t, ident_b.t",
        false,
    )
    .await;
    sources::postgres::postgres_publication::register(
        &pg_client().await,
        &["pub_ident".to_string()],
    )
    .await
    .expect("register");

    let backend: storage::ArcStorageBackend =
        Arc::new(storage::MemoryStorageBackend::new());
    let mgr = Arc::new(PipelineManager::with_backend(backend).await.unwrap());
    let topic_of = |sink: Sink, schema: &str| {
        format!("probe-ident-{}-{schema}-t", sink.name())
    };
    let prefix_of = |sink: Sink| format!("probe/ident/{}", sink.name());
    for sink in SINKS {
        let name = format!("p-ident-{}", sink.name());
        let sinks = sink_yaml(
            i,
            sink,
            &format!(
                "probe-ident-{}-${{source.schema}}-${{source.table}}",
                sink.name()
            ),
            &prefix_of(sink),
        )
        .await;
        let yaml = format!(
            r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata: {{name: {name}, tenant: {TENANT}}}
spec:
  source:
    type: postgres
    config:
      id: {name}
      dsn: "{dsn}"
      slot: {slot}
      publication: pub_ident
      tables: ["ident_a.t", "ident_b.t"]
      snapshot: {{mode: initial}}
  processors: []
  sinks:{sinks}
"#,
            dsn = pg_dsn().await,
            slot = name.replace('-', "_"),
        );
        mgr.create(serde_yaml::from_str(&yaml).unwrap())
            .await
            .expect("create");
    }

    // Snapshot rows (id 1) first, then CDC rows (id 2).
    let records_of = async |sink: Sink, schema: &str| -> Vec<Value> {
        match sink {
            Sink::Json | Sink::Avro => {
                kafka_records(i, &topic_of(sink, schema), sink).await
            }
            _ => parquet_records(i, &prefix_of(sink))
                .await
                .0
                .into_iter()
                .filter(|r| r["source"]["schema"] == schema)
                .collect(),
        }
    };
    let deadline = Instant::now() + Duration::from_secs(240);
    let mut inserted = false;
    let mut failures = Vec::new();
    'wait: loop {
        let mut complete = true;
        for sink in SINKS {
            for schema in schemas {
                let ids: BTreeSet<i64> = records_of(sink, schema)
                    .await
                    .iter()
                    .filter_map(rec_id)
                    .collect();
                complete &= ids.contains(&1) && (!inserted || ids.contains(&2));
            }
        }
        if complete && inserted {
            break 'wait;
        }
        if complete {
            for s in schemas {
                sql.run(&format!("INSERT INTO {s}.t VALUES (2, '{s}')"), false)
                    .await;
            }
            inserted = true;
        }
        if Instant::now() > deadline {
            failures.push(format!(
                "not every sink delivered both schemas' rows (inserted: {inserted})"
            ));
            break 'wait;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let mut refs: BTreeMap<&str, BTreeSet<(String, u64)>> = BTreeMap::new();
    for sink in SINKS {
        for schema in schemas {
            let ctx = format!("{} {schema}", sink.name());
            for r in records_of(sink, schema).await {
                // Every row is its own schema's.
                let v = r["images"]
                    .as_array()
                    .and_then(|imgs| imgs.iter().find(|img| img[0] == "after"))
                    .map(|img| img[1]["v"]["value"].clone());
                if v != Some(json!(schema)) {
                    failures
                        .push(format!("{ctx}: a row of another table: {v:?}"));
                }
                match sink {
                    Sink::Json => {
                        let src = &r["raw"]["source"];
                        if src["db"] != "postgres" || src["schema"] != schema {
                            failures.push(format!(
                                "{ctx}: source.db {} source.schema {}, expected \
                                 postgres and {schema}",
                                src["db"], src["schema"]
                            ));
                        }
                        let fp =
                            r["raw"]["schema_version"].as_str().unwrap_or("");
                        let seq =
                            r["raw"]["schema_sequence"].as_u64().unwrap_or(0);
                        refs.entry(schema)
                            .or_default()
                            .insert((fp.into(), seq));
                    }
                    Sink::Avro => {
                        // The relation's escaped database, schema and table.
                        let (path, table) =
                            deltaforge_core::relation::avro_path(
                                "postgres", schema, "t",
                            );
                        let want = format!(".{path}.{table}");
                        let ns =
                            r["schema"]["namespace"].as_str().unwrap_or("");
                        if !ns.ends_with(&want) {
                            failures.push(format!(
                                "{ctx}: Avro namespace {ns} does not end with {want}"
                            ));
                        }
                    }
                    Sink::Parquet | Sink::ParquetLegacy => {
                        if r["source"]["db"] != "postgres" {
                            failures.push(format!(
                                "{ctx}: source_db {}",
                                r["source"]["db"]
                            ));
                        }
                        let object = r["object"].as_str().unwrap_or("");
                        if sink == Sink::ParquetLegacy
                            && !object.contains(&format!(
                                "database=postgres/namespace={schema}/table=t/"
                            ))
                        {
                            failures.push(format!(
                                "{ctx}: object {object} outside its namespace partition"
                            ));
                        }
                    }
                }
            }
        }
    }
    // Same DDL, separate tables: never one schema version.
    if let (Some(a), Some(b)) = (refs.get("ident_a"), refs.get("ident_b"))
        && !a.is_disjoint(b)
    {
        failures.push(format!(
            "the two tables share a schema version: {a:?} {b:?}"
        ));
    }
    if refs.len() != 2 {
        failures.push(format!("schema references of {} tables", refs.len()));
    }
    // A durable object holds one table: its rows share one schema.
    let mut by_object: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for r in parquet_records(i, &prefix_of(Sink::Parquet)).await.0 {
        by_object
            .entry(r["object"].as_str().unwrap_or("").to_string())
            .or_default()
            .insert(r["source"]["schema"].to_string());
    }
    for (object, schemas) in by_object {
        if schemas.len() != 1 {
            failures.push(format!("object {object} mixes tables: {schemas:?}"));
        }
    }
    for sink in SINKS {
        mgr.delete(&format!("p-ident-{}", sink.name())).await.ok();
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// A snapshot larger than one batch (5,000 rows; default batching cuts at
/// 2,000 events, snapshot chunks hold 10,000 rows) reaches the durable S3
/// sink whole: a batch limit never cuts a chunk, so every batch ends at the
/// chunk's boundary and carries its watermark.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "live: PostgreSQL and RustFS"]
async fn a_snapshot_larger_than_a_batch_reaches_durable_s3() {
    let _ = out_dir();
    let i = infra().await;
    let rows = 5_000;
    let mut sql = Sql::new(Engine::Postgres).await;
    sql.run("CREATE SCHEMA big_snap", false).await;
    sql.run(
        "CREATE TABLE big_snap.t (id BIGINT PRIMARY KEY, v TEXT)",
        false,
    )
    .await;
    sql.run(
        &format!(
            "INSERT INTO big_snap.t SELECT g, 'v' || g FROM generate_series(1, {rows}) g"
        ),
        false,
    )
    .await;
    sql.run(
        "CREATE PUBLICATION pub_big_snap FOR TABLE big_snap.t",
        false,
    )
    .await;
    sources::postgres::postgres_publication::register(
        &pg_client().await,
        &["pub_big_snap".to_string()],
    )
    .await
    .expect("register");
    let backend: storage::ArcStorageBackend =
        Arc::new(storage::MemoryStorageBackend::new());
    let mgr = Arc::new(PipelineManager::with_backend(backend).await.unwrap());
    let prefix = "probe/big-snap";
    let sinks = sink_yaml(i, Sink::Parquet, "", prefix).await;
    let yaml = format!(
        r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata: {{name: p-big-snap, tenant: {TENANT}}}
spec:
  source:
    type: postgres
    config:
      id: p-big-snap
      dsn: "{dsn}"
      slot: p_big_snap
      publication: pub_big_snap
      tables: ["big_snap.t"]
      snapshot: {{mode: initial}}
  processors: []
  sinks:{sinks}
"#,
        dsn = pg_dsn().await,
    );
    mgr.create(serde_yaml::from_str(&yaml).unwrap())
        .await
        .expect("create");
    let deadline = Instant::now() + Duration::from_secs(180);
    let delivered = loop {
        let n = parquet_records(i, prefix).await.0.len();
        let failed = !reasons(&mgr, "p-big-snap").await.is_empty();
        if n >= rows || failed || Instant::now() > deadline {
            break n;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    let incidents = reasons(&mgr, "p-big-snap").await;
    mgr.delete("p-big-snap").await.ok();
    assert!(incidents.is_empty(), "incidents: {incidents:?}");
    assert_eq!(delivered, rows);
}
