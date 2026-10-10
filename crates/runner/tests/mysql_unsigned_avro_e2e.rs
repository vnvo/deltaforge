//! Unsigned MySQL integers through Avro, end to end on the production path:
//! a live MySQL source, a pipeline built by the pipeline manager, a Kafka
//! sink with Avro encoding (DDL-derived schemas from the source's schema
//! loader) and a real Schema Registry. Every Kafka record is decoded with
//! the schema it was registered under.
//!
//! - `INT UNSIGNED` is an Avro `long` and keeps 2^32 - 1 exactly.
//! - `BIGINT UNSIGNED`, `unsigned_bigint_mode: string` (the default): the
//!   exact decimal value through 2^64 - 1.
//! - `BIGINT UNSIGNED`, `unsigned_bigint_mode: long`: exact through
//!   `i64::MAX`; a value above it fails the pipeline explicitly and no
//!   record of it reaches the topic (never wrapped, zeroed or a double).
//!
//! Inserts, updates (both images) and deletes (before image).
//!
//! ```bash
//! cargo test -p runner --test mysql_unsigned_avro_e2e -- --include-ignored --test-threads=1
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use apache_avro::types::Value as Avro;
use ctor::dtor;
use deltaforge_config::PipelineSpec;
use gate_ownership::GateOwned;
use mysql_async::prelude::Queryable;
use rdkafka::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::message::Message;
use rest_api::PipelineController;
use runner::pipeline_manager::PipelineManager;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::sync::OnceCell;

const NETWORK: &str = "df-unsigned-avro-net";
const KAFKA_NAME: &str = "df-unsigned-avro-kafka";
const KAFKA_INTERNAL_PORT: u16 = 29092;
/// The host listener (unique among the gate's fixed ports).
const KAFKA_HOST_PORT: u16 = 9395;
const ROOT_PW: &str = "pw";

const I64_MAX: u64 = i64::MAX as u64;
const U32_MAX: u64 = u32::MAX as u64;

struct Infra {
    _kafka: ContainerAsync<GenericImage>,
    sr: ContainerAsync<GenericImage>,
    mysql: ContainerAsync<GenericImage>,
}

static INFRA: OnceCell<Infra> = OnceCell::const_new();

#[dtor]
fn cleanup() {
    if let Some(i) = INFRA.get() {
        for id in [i.sr.id(), i._kafka.id(), i.mysql.id()] {
            std::process::Command::new("docker")
                .args(["rm", "-f", "-v", id])
                .output()
                .ok();
        }
    }
}

fn brokers() -> String {
    format!("localhost:{KAFKA_HOST_PORT}")
}

async fn start() -> Infra {
    std::process::Command::new("docker")
        .args(["rm", "-f", KAFKA_NAME])
        .output()
        .ok();
    let kafka = GenericImage::new("confluentinc/cp-kafka", "7.5.0")
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
    let sr = GenericImage::new("confluentinc/cp-schema-registry", "7.5.0")
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
    let mysql = GenericImage::new("mysql", "8.4")
        .with_wait_for(WaitFor::message_on_stderr(
            "ready for connections. Version: '8.4",
        ))
        .with_env_var("MYSQL_ROOT_PASSWORD", ROOT_PW)
        .with_cmd(vec![
            "--server-id=49",
            "--log-bin=mysql-bin",
            "--binlog-format=ROW",
            "--binlog-row-image=FULL",
            "--gtid-mode=ON",
            "--enforce-gtid-consistency=ON",
        ])
        .gate_owned()
        .start()
        .await
        .expect("start mysql");
    let infra = Infra {
        _kafka: kafka,
        sr,
        mysql,
    };
    let deadline = Instant::now() + Duration::from_secs(120);
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

async fn sql(i: &Infra, stmts: &[String]) {
    let mut c = mysql_async::Conn::from_url(dsn(i).await).await.unwrap();
    for s in stmts {
        c.query_drop(s.as_str())
            .await
            .unwrap_or_else(|e| panic!("{s}: {e}"));
    }
    c.disconnect().await.ok();
}

/// Warnings and errors this process logged.
static LOGS: std::sync::Mutex<Vec<u8>> = std::sync::Mutex::new(Vec::new());

struct Logs;

impl std::io::Write for Logs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        LOGS.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn capture_logs() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("warn")
        .with_ansi(false)
        .with_writer(|| Logs)
        .try_init();
}

fn logged(needle: &str) -> bool {
    String::from_utf8_lossy(&LOGS.lock().unwrap()).contains(needle)
}

/// One pipeline from a live source table to an Avro-encoded topic.
async fn pipeline(
    i: &Infra,
    mgr: &PipelineManager,
    db: &str,
    mode: &str,
) -> String {
    let name = format!("ua-{mode}");
    let yaml = format!(
        r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata: {{name: {name}, tenant: t}}
spec:
  source:
    type: mysql
    config: {{id: src-{mode}, dsn: "{dsn}", tables: ["{db}.t"]}}
  processors: []
  sinks:
    - type: kafka
      config:
        id: kafka
        brokers: "{brokers}"
        topic: {db}
        envelope: {{type: native}}
        required: true
        encoding:
          type: avro
          schema_registry_url: "{sr}"
          unsigned_bigint_mode: {mode}
"#,
        dsn = dsn(i).await,
        brokers = brokers(),
        sr = sr_url(i).await,
    );
    let spec: PipelineSpec = serde_yaml::from_str(&yaml).expect("spec");
    mgr.create(spec).await.expect("create pipeline");
    until_status(mgr, &name, "running").await;
    name
}

async fn until_status(mgr: &PipelineManager, name: &str, want: &str) {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let status = mgr.get(name).await.map(|p| p.status).ok();
        if status.as_deref() == Some(want) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{name} never reached {want} (status {status:?})"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// One decoded record: the operation and the row images' `i` and `b`.
#[derive(Debug, Clone, PartialEq)]
struct Rec {
    op: String,
    before: Option<BTreeMap<String, Avro>>,
    after: Option<BTreeMap<String, Avro>>,
}

fn unwrap_union(v: Avro) -> Avro {
    match v {
        Avro::Union(_, inner) => *inner,
        other => other,
    }
}

fn image(v: Option<Avro>) -> Option<BTreeMap<String, Avro>> {
    match v.map(unwrap_union) {
        Some(Avro::Record(fields)) => Some(
            fields
                .into_iter()
                .map(|(k, v)| (k, unwrap_union(v)))
                .collect(),
        ),
        _ => None,
    }
}

/// Decode a Confluent-framed Avro record with its registered schema.
async fn decode(i: &Infra, bytes: &[u8]) -> Rec {
    assert_eq!(bytes[0], 0, "magic byte");
    let id = u32::from_be_bytes(bytes[1..5].try_into().unwrap());
    let body: serde_json::Value =
        reqwest::get(format!("{}/schemas/ids/{id}", sr_url(i).await))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    let schema =
        apache_avro::Schema::parse_str(body["schema"].as_str().unwrap())
            .unwrap();
    let value =
        apache_avro::from_avro_datum(&schema, &mut &bytes[5..], None).unwrap();
    let Avro::Record(fields) = value else {
        panic!("not a record: {value:?}")
    };
    let mut fields: BTreeMap<String, Avro> = fields.into_iter().collect();
    let op = match fields.remove("op").map(unwrap_union) {
        Some(Avro::String(s)) => s,
        other => panic!("op: {other:?}"),
    };
    Rec {
        op,
        before: image(fields.remove("before")),
        after: image(fields.remove("after")),
    }
}

/// Every record on `topic` until `want` arrived and the topic stayed quiet
/// for `quiet` (or `max` passed).
async fn consume(
    i: &Infra,
    topic: &str,
    want: usize,
    quiet: Duration,
    max: Duration,
) -> Vec<Rec> {
    let c: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .set("group.id", format!("ua-{topic}-{}", rand_suffix()))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .unwrap();
    c.subscribe(&[topic]).unwrap();
    let deadline = Instant::now() + max;
    let mut out = Vec::new();
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let wait = if out.len() >= want {
            quiet.min(left)
        } else {
            left
        };
        match tokio::time::timeout(wait, c.recv()).await {
            Ok(Ok(m)) => out.push(decode(i, m.payload().unwrap()).await),
            // The sink creates the topic with its first record.
            Ok(Err(e))
                if e.rdkafka_error_code()
                    == Some(RDKafkaErrorCode::UnknownTopicOrPartition) =>
            {
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            Ok(Err(e)) => panic!("consume {topic}: {e}"),
            Err(_) => break,
        }
    }
    out
}

fn rand_suffix() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

/// Marker rows have ids from here up.
const MARKER: i32 = 1_000;

/// A fresh pipeline without a snapshot captures changes from the binlog
/// position its source starts at, which can be shortly after the pipeline
/// reports `running`: write marker rows until one reaches the topic.
async fn until_streaming(i: &Infra, db: &str) {
    for k in 0..120 {
        sql(
            i,
            &[format!("INSERT INTO {db}.t VALUES ({}, 0, 0)", MARKER + k)],
        )
        .await;
        let got = consume(
            i,
            db,
            1,
            Duration::from_millis(100),
            Duration::from_secs(2),
        )
        .await;
        if !got.is_empty() {
            return;
        }
    }
    panic!("{db}: the pipeline never streamed a marker row");
}

/// The records of the test's own rows (markers dropped).
fn rows(recs: Vec<Rec>) -> Vec<Rec> {
    let id = |img: &Option<BTreeMap<String, Avro>>| match img
        .as_ref()
        .and_then(|m| m.get("id"))
    {
        Some(Avro::Int(id)) => Some(*id),
        _ => None,
    };
    recs.into_iter()
        .filter(|r| {
            id(&r.after).or(id(&r.before)).is_some_and(|id| id < MARKER)
        })
        .collect()
}

fn long(v: u64) -> Avro {
    Avro::Long(i64::try_from(v).unwrap())
}

fn row(id: i32, i: Avro, b: Avro) -> BTreeMap<String, Avro> {
    BTreeMap::from([
        ("id".to_string(), Avro::Int(id)),
        ("i".to_string(), i),
        ("b".to_string(), b),
    ])
}

async fn manager() -> Arc<PipelineManager> {
    let backend: storage::ArcStorageBackend =
        Arc::new(storage::MemoryStorageBackend::new());
    Arc::new(PipelineManager::with_backend(backend).await.unwrap())
}

const TABLE: &str = "id INT PRIMARY KEY, i INT UNSIGNED, b BIGINT UNSIGNED";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "docker: MySQL, Kafka and Schema Registry"]
async fn bigint_unsigned_in_string_mode_keeps_exact_decimal_values() {
    let i = INFRA.get_or_init(start).await;
    let db = "ua_string";
    sql(
        i,
        &[
            format!("DROP DATABASE IF EXISTS {db}"),
            format!("CREATE DATABASE {db}"),
            format!("CREATE TABLE {db}.t ({TABLE})"),
        ],
    )
    .await;
    let mgr = manager().await;
    let name = pipeline(i, &mgr, db, "string").await;
    until_streaming(i, db).await;
    sql(
        i,
        &[
            format!("INSERT INTO {db}.t VALUES (1, {U32_MAX}, {I64_MAX})"),
            format!(
                "INSERT INTO {db}.t VALUES (2, {U32_MAX}, {})",
                I64_MAX + 1
            ),
            format!("INSERT INTO {db}.t VALUES (3, {U32_MAX}, {})", u64::MAX),
            format!("UPDATE {db}.t SET b = {} WHERE id = 1", u64::MAX),
            format!("DELETE FROM {db}.t WHERE id = 3"),
        ],
    )
    .await;
    let s = |v: u64| Avro::String(v.to_string());
    let got = rows(
        consume(i, db, 6, Duration::from_secs(5), Duration::from_secs(120))
            .await,
    );
    assert_eq!(
        got,
        vec![
            Rec {
                op: "c".into(),
                before: None,
                after: Some(row(1, long(U32_MAX), s(I64_MAX))),
            },
            Rec {
                op: "c".into(),
                before: None,
                after: Some(row(2, long(U32_MAX), s(I64_MAX + 1))),
            },
            Rec {
                op: "c".into(),
                before: None,
                after: Some(row(3, long(U32_MAX), s(u64::MAX))),
            },
            Rec {
                op: "u".into(),
                before: Some(row(1, long(U32_MAX), s(I64_MAX))),
                after: Some(row(1, long(U32_MAX), s(u64::MAX))),
            },
            Rec {
                op: "d".into(),
                before: Some(row(3, long(U32_MAX), s(u64::MAX))),
                after: None,
            },
        ]
    );
    mgr.delete(&name).await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "docker: MySQL, Kafka and Schema Registry"]
async fn bigint_unsigned_in_long_mode_is_exact_to_i64_max_and_fails_above() {
    capture_logs();
    let i = INFRA.get_or_init(start).await;
    let db = "ua_long";
    sql(
        i,
        &[
            format!("DROP DATABASE IF EXISTS {db}"),
            format!("CREATE DATABASE {db}"),
            format!("CREATE TABLE {db}.t ({TABLE})"),
        ],
    )
    .await;
    let mgr = manager().await;
    let name = pipeline(i, &mgr, db, "long").await;
    until_streaming(i, db).await;
    sql(
        i,
        &[
            format!("INSERT INTO {db}.t VALUES (1, {U32_MAX}, {I64_MAX})"),
            format!("UPDATE {db}.t SET i = 0 WHERE id = 1"),
            format!("DELETE FROM {db}.t WHERE id = 1"),
        ],
    )
    .await;
    let got = rows(
        consume(i, db, 4, Duration::from_secs(5), Duration::from_secs(120))
            .await,
    );
    assert_eq!(
        got,
        vec![
            Rec {
                op: "c".into(),
                before: None,
                after: Some(row(1, long(U32_MAX), long(I64_MAX))),
            },
            Rec {
                op: "u".into(),
                before: Some(row(1, long(U32_MAX), long(I64_MAX))),
                after: Some(row(1, Avro::Long(0), long(I64_MAX))),
            },
            Rec {
                op: "d".into(),
                before: Some(row(1, Avro::Long(0), long(I64_MAX))),
                after: None,
            },
        ]
    );

    // Above i64::MAX: the pipeline fails explicitly, and nothing of the
    // row (wrapped, zeroed or as a double) reaches the topic.
    sql(
        i,
        &[format!(
            "INSERT INTO {db}.t VALUES (2, {U32_MAX}, {})",
            I64_MAX + 1
        )],
    )
    .await;
    until_status(&mgr, &name, "failed").await;
    let incidents = format!("{:?}", mgr.incidents(&name).await);
    assert!(incidents.contains("halted"), "{incidents}");
    let cause = format!("{} does not fit an Avro long", I64_MAX + 1);
    assert!(logged(&cause), "the failure is the explicit encoding error");
    let got = rows(
        consume(i, db, 4, Duration::from_secs(10), Duration::from_secs(120))
            .await,
    );
    assert_eq!(got.len(), 3, "no record after the failure: {got:?}");
    mgr.delete(&name).await.ok();
}
