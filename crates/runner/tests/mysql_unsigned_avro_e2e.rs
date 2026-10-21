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
    pipeline_named(i, mgr, &format!("ua-{mode}"), db, mode, false).await
}

/// A pipeline `name` from `db.t` to topic `db`, with the event journal's
/// DLQ when `dlq`.
async fn pipeline_named(
    i: &Infra,
    mgr: &PipelineManager,
    name: &str,
    db: &str,
    mode: &str,
    dlq: bool,
) -> String {
    pipeline_with(i, mgr, name, db, mode, dlq, false, 1).await
}

/// [`pipeline_named`], plus a JSON (native envelope) sink to topic
/// `{db}_json` when `mirror`, the Avro sink encoding on `workers` threads.
#[allow(clippy::too_many_arguments)]
async fn pipeline_with(
    i: &Infra,
    mgr: &PipelineManager,
    name: &str,
    db: &str,
    mode: &str,
    dlq: bool,
    mirror: bool,
    workers: usize,
) -> String {
    let mirror = if mirror {
        format!(
            "\n    - type: kafka\n      config: {{id: json, brokers: \"{}\", \
             topic: {db}_json, envelope: {{type: native}}, required: true}}",
            brokers()
        )
    } else {
        String::new()
    };
    let journal = if dlq {
        "\n  journal: {enabled: true}"
    } else {
        ""
    };
    let yaml = format!(
        r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata: {{name: {name}, tenant: t}}
spec:
  source:
    type: mysql
    config: {{id: src-{name}, dsn: "{dsn}", tables: ["{db}.t"]}}
  processors: []
  sinks:
    - type: kafka
      config:
        id: kafka
        brokers: "{brokers}"
        topic: {db}
        envelope: {{type: native}}
        required: true
        encode_workers: {workers}
        encoding:
          type: avro
          schema_registry_url: "{sr}"
          unsigned_bigint_mode: {mode}{mirror}{journal}
"#,
        dsn = dsn(i).await,
        brokers = brokers(),
        sr = sr_url(i).await,
    );
    let spec: PipelineSpec = serde_yaml::from_str(&yaml).expect("spec");
    mgr.create(spec).await.expect("create pipeline");
    until_status(mgr, name, "running").await;
    name.to_string()
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

/// Decode a Confluent-framed Avro record knowing only its schema ID, as
/// a generic consumer of the topic does.
async fn decode_any(i: &Infra, bytes: &[u8]) -> Avro {
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
    apache_avro::from_avro_datum(&schema, &mut &bytes[5..], None).unwrap()
}

/// Decode a row record (see [`decode_any`]).
async fn decode(i: &Infra, bytes: &[u8]) -> Rec {
    let value = decode_any(i, bytes).await;
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
    let mut out = Vec::new();
    for bytes in consume_raw(topic, want, quiet, max).await {
        out.push(decode(i, &bytes).await);
    }
    out
}

/// [`consume`]'s raw record values.
async fn consume_raw(
    topic: &str,
    want: usize,
    quiet: Duration,
    max: Duration,
) -> Vec<Vec<u8>> {
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
            Ok(Ok(m)) => out.push(m.payload().unwrap().to_vec()),
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
            &[format!("INSERT INTO {db}.t (id) VALUES ({})", MARKER + k)],
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
    // Its own incident: the column and types, never the value.
    let incidents = mgr.incidents(&name).await.unwrap().to_string();
    for want in [
        "sink_conversion_failed",
        "after.b",
        "bigint_unsigned",
        "avro_long",
    ] {
        assert!(incidents.contains(want), "{want}: {incidents}");
    }
    let value = (I64_MAX + 1).to_string();
    assert!(!incidents.contains(&value), "{incidents}");
    assert!(
        logged(
            "column after.b (bigint unsigned) cannot be represented exactly as Avro long"
        ),
        "the failure is the explicit conversion error"
    );
    assert!(!logged(&value), "the value never reaches the log");
    let got = rows(
        consume(i, db, 4, Duration::from_secs(10), Duration::from_secs(120))
            .await,
    );
    assert_eq!(got.len(), 3, "no record after the failure: {got:?}");
    mgr.delete(&name).await.ok();
}

/// With the event journal's DLQ, a value long mode cannot hold goes to the
/// DLQ with its conversion failure (column and types, not the value), the
/// rows around it are delivered exactly, and the pipeline keeps running.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "docker: MySQL, Kafka and Schema Registry"]
async fn a_conversion_failure_goes_to_the_dlq_when_one_is_configured() {
    capture_logs();
    let i = INFRA.get_or_init(start).await;
    let db = "ua_dlq";
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
    let name = pipeline_named(i, &mgr, "ua-dlq", db, "long", true).await;
    until_streaming(i, db).await;
    sql(
        i,
        &[
            format!("INSERT INTO {db}.t VALUES (1, {U32_MAX}, {I64_MAX})"),
            format!(
                "INSERT INTO {db}.t VALUES (2, {U32_MAX}, {})",
                I64_MAX + 1
            ),
            format!("INSERT INTO {db}.t VALUES (3, 1, 1)"),
        ],
    )
    .await;
    let got = rows(
        consume(i, db, 3, Duration::from_secs(5), Duration::from_secs(120))
            .await,
    );
    let ids: Vec<Avro> = got
        .iter()
        .map(|r| r.after.as_ref().unwrap()["id"].clone())
        .collect();
    assert_eq!(ids, vec![Avro::Int(1), Avro::Int(3)], "{got:?}");
    assert_eq!(got[0].after.as_ref().unwrap()["b"], long(I64_MAX));
    assert_eq!(mgr.get(&name).await.unwrap().status, "running");
    assert_eq!(mgr.dlq_count(&name).await.unwrap(), 1);
    let entries = format!("{:?}", mgr.dlq_peek(&name, 10).await.unwrap());
    for want in ["after.b", "bigint_unsigned", "avro_long"] {
        assert!(entries.contains(want), "{want}: {entries}");
    }
    mgr.delete(&name).await.ok();
}

/// DATE, TIMESTAMP(6), TEXT and JSON values reach Avro exactly (DATE was
/// written as the epoch and TIMESTAMP declared in the wrong unit before).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "docker: MySQL, Kafka and Schema Registry"]
async fn temporal_text_and_json_values_encode_exactly() {
    let i = INFRA.get_or_init(start).await;
    let db = "ua_fam";
    sql(
        i,
        &[
            format!("DROP DATABASE IF EXISTS {db}"),
            format!("CREATE DATABASE {db}"),
            format!(
                "CREATE TABLE {db}.t (id INT PRIMARY KEY, d DATE, \
                 ts TIMESTAMP(6) NULL, txt TEXT, j JSON)"
            ),
        ],
    )
    .await;
    let mgr = manager().await;
    let name = pipeline_named(i, &mgr, "ua-fam", db, "string", false).await;
    until_streaming(i, db).await;
    sql(
        i,
        &[
            "SET time_zone = '+00:00'".into(),
            format!(
                "INSERT INTO {db}.t VALUES (1, '2024-02-29', \
                 '2024-03-30 23:30:00.654321', 'multi\\nline 😀', \
                 '{{\"k\": [1, 2.5]}}')"
            ),
        ],
    )
    .await;
    let got = rows(
        consume(i, db, 2, Duration::from_secs(5), Duration::from_secs(120))
            .await,
    );
    let after = got[0].after.as_ref().expect("after");
    assert_eq!(after["d"], Avro::Date(19_782));
    assert_eq!(after["ts"], Avro::TimestampMicros(1_711_841_400_654_321));
    assert_eq!(after["txt"], Avro::String("multi\nline 😀".into()));
    // MySQL's own JSON text (its separators), never a re-serialization.
    assert_eq!(after["j"], Avro::String(r#"{"k": [1, 2.5]}"#.into()));
    mgr.delete(&name).await.ok();
}

/// CDC delivers an ENUM decoded by its declared labels: the label reaches
/// Avro, never the stored index.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "docker: MySQL, Kafka and Schema Registry"]
async fn an_enum_reaches_avro_as_its_label() {
    let i = INFRA.get_or_init(start).await;
    let db = "ua_enum";
    sql(
        i,
        &[
            format!("DROP DATABASE IF EXISTS {db}"),
            format!("CREATE DATABASE {db}"),
            format!(
                "CREATE TABLE {db}.t (id INT PRIMARY KEY, e ENUM('a', 'b'))"
            ),
        ],
    )
    .await;
    let mgr = manager().await;
    let name = pipeline_named(i, &mgr, "ua-enum", db, "string", false).await;
    until_streaming(i, db).await;
    sql(i, &[format!("INSERT INTO {db}.t VALUES (1, 'b')")]).await;
    let got = rows(
        consume(i, db, 1, Duration::from_secs(5), Duration::from_secs(120))
            .await,
    );
    let after = got[0].after.as_ref().expect("after");
    assert_eq!(after["e"], Avro::String("b".into()));
    assert!(
        mgr.incidents(&name).await.unwrap()["incidents"]
            .as_array()
            .is_none_or(|a| a.is_empty())
    );
    mgr.delete(&name).await.ok();
}

/// A column added by DDL reaches Avro: the encoder never keeps converting
/// rows with the subject's earlier schema (which has no such column).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "docker: MySQL, Kafka and Schema Registry"]
async fn a_column_added_by_ddl_reaches_avro() {
    let i = INFRA.get_or_init(start).await;
    let db = "ua_ddl";
    sql(
        i,
        &[
            format!("DROP DATABASE IF EXISTS {db}"),
            format!("CREATE DATABASE {db}"),
            format!("CREATE TABLE {db}.t (id INT PRIMARY KEY, i INT UNSIGNED, b BIGINT UNSIGNED)"),
        ],
    )
    .await;
    let mgr = manager().await;
    let name = pipeline_named(i, &mgr, "ua-ddl", db, "string", false).await;
    until_streaming(i, db).await;
    sql(
        i,
        &[
            format!("INSERT INTO {db}.t VALUES (1, 1, 1)"),
            format!("ALTER TABLE {db}.t ADD COLUMN extra INT"),
            format!("INSERT INTO {db}.t VALUES (2, 2, 2, 42)"),
        ],
    )
    .await;
    let got = rows(
        consume(i, db, 3, Duration::from_secs(5), Duration::from_secs(120))
            .await,
    );
    let status = mgr.get(&name).await.unwrap().status;
    let second = got.iter().find(|r| {
        r.after
            .as_ref()
            .is_some_and(|a| a.get("id") == Some(&Avro::Int(2)))
    });
    assert!(
        second
            .is_some_and(|r| r.after.as_ref().unwrap().get("extra")
                == Some(&Avro::Int(42))),
        "the added column's value is missing (status {status}): {got:?}"
    );

    // One lossless DDL record for the ALTER, before the row that has the
    // new column; every record decodes by its schema ID alone.
    let mut records = Vec::new();
    for bytes in
        consume_raw(db, 4, Duration::from_secs(2), Duration::from_secs(60))
            .await
    {
        records.push(avro_json(&decode_any(i, &bytes).await));
    }
    let ddl: Vec<usize> = (0..records.len())
        .filter(|&k| records[k].get("ddl").is_some())
        .collect();
    assert_eq!(ddl.len(), 1, "{records:#?}");
    assert_eq!(
        records[ddl[0]]["ddl"],
        serde_json::json!({
            "ddl_format": 2,
            "scope": "tables",
            "objects": [{"kind": "table", "database": db, "name": "t"}],
            "database": db,
            "session_database": null,
            "sql": format!("ALTER TABLE {db}.t ADD COLUMN extra INT"),
            "operation": null, "redacted": null, "note": null,
            "cascade": null, "restart_identity": null
        })
    );
    let row2 = records
        .iter()
        .position(|r| r["after"]["id"] == 2)
        .expect("row 2");
    assert!(ddl[0] < row2, "the DDL record precedes the row");
    assert_eq!(records[row2]["after"]["extra"], 42);
    mgr.delete(&name).await.ok();
}

/// An Avro value as JSON (unions unwrapped, records as objects); other
/// kinds as their debug text.
fn avro_json(v: &Avro) -> serde_json::Value {
    use serde_json::json;
    match v {
        Avro::Null => json!(null),
        Avro::Boolean(b) => json!(b),
        Avro::Int(n) => json!(n),
        Avro::Long(n) => json!(n),
        Avro::String(s) => json!(s),
        Avro::Union(_, inner) => avro_json(inner),
        Avro::Array(items) => {
            serde_json::Value::Array(items.iter().map(avro_json).collect())
        }
        Avro::Record(fields) => serde_json::Value::Object(
            fields
                .iter()
                .map(|(k, v)| (k.clone(), avro_json(v)))
                .collect(),
        ),
        other => json!(format!("{other:?}")),
    }
}

/// DDL events reach Avro as lossless DDL records under the topic's DDL
/// subject, equal to the source's own events (a JSON sink of the same
/// pipeline): renames keep their ordered source and target objects, a
/// multi-table DROP every object, database and lineage barriers their
/// scopes, and account statements no secret. Rows keep flowing with the
/// new column.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "docker: MySQL, Kafka and Schema Registry"]
async fn ddl_events_reach_avro_as_lossless_ddl_records() {
    const SECRETS: [&str; 2] = ["Ua-Secret-7781", "Ua-Secret-9902"];
    let i = INFRA.get_or_init(start).await;
    let db = "ua_ddlrec";
    let x = "ua_ddlrec_x";
    sql(
        i,
        &[
            format!("DROP DATABASE IF EXISTS {db}"),
            format!("DROP DATABASE IF EXISTS {x}"),
            "DROP USER IF EXISTS 'ua_ddl_user'@'%'".into(),
            format!("CREATE DATABASE {db}"),
            format!("CREATE TABLE {db}.t (id INT PRIMARY KEY)"),
            format!("CREATE TABLE {db}.a (id INT PRIMARY KEY)"),
            format!("CREATE TABLE {db}.b (id INT PRIMARY KEY)"),
            format!("CREATE TABLE {db}.c (id INT PRIMARY KEY)"),
            format!("CREATE TABLE {db}.d (id INT PRIMARY KEY)"),
        ],
    )
    .await;
    let mgr = manager().await;
    let name =
        pipeline_with(i, &mgr, "ua-ddlrec", db, "string", false, true, 1).await;
    until_streaming(i, db).await;
    sql(
        i,
        &[
            format!("CREATE DATABASE IF NOT EXISTS `{x}`"),
            format!("ALTER TABLE {db}.t ADD COLUMN extra INT"),
            format!("/*!50100 ALTER TABLE {db}.t ADD COLUMN c1 INT */"),
            format!("USE {db}"),
            "RENAME TABLE a TO a2".into(),
            format!("RENAME TABLE {db}.b TO {x}.b2"),
            format!("DROP TABLE {db}.c, {db}.a2"),
            // A double-quoted token may be an ANSI_QUOTES identifier: the
            // statement is a lineage barrier.
            format!("ALTER TABLE {db}.d ADD COLUMN y VARCHAR(5) DEFAULT \"x\""),
            format!(
                "CREATE USER 'ua_ddl_user'@'%' IDENTIFIED BY '{}'",
                SECRETS[0]
            ),
            format!(
                "ALTER USER 'ua_ddl_user'@'%' IDENTIFIED BY '{}'",
                SECRETS[1]
            ),
            "DROP USER 'ua_ddl_user'@'%'".into(),
        ],
    )
    .await;

    // While the pipeline is stopped, then caught up after resume.
    mgr.stop(&name).await.expect("stop");
    until_status(&mgr, &name, "stopped").await;
    sql(
        i,
        &[
            format!("/*!40101 DROP DATABASE IF EXISTS `{x}` */"),
            format!("/*! ALTER TABLE {db}.t ADD COLUMN c2 INT */"),
            format!("ALTER DATABASE `{db}` CHARACTER SET utf8mb4"),
        ],
    )
    .await;
    mgr.resume(&name).await.expect("resume");
    until_status(&mgr, &name, "running").await;
    sql(
        i,
        &[format!(
            "INSERT INTO {db}.t (id, extra, c1, c2) VALUES (1, 5, 6, 7)"
        )],
    )
    .await;

    // The source's own DDL events (JSON sink), in order.
    let quiet = Duration::from_secs(5);
    let max = Duration::from_secs(120);
    let json_raw = consume_raw(&format!("{db}_json"), 15, quiet, max).await;
    let source: Vec<serde_json::Value> = json_raw
        .iter()
        .map(|b| serde_json::from_slice(b).unwrap())
        .filter(|e: &serde_json::Value| e.get("ddl").is_some())
        .collect();
    // The Avro topic, decoded by schema ID alone.
    let avro_raw = consume_raw(db, 15, quiet, max).await;
    let mut avro = Vec::new();
    for bytes in &avro_raw {
        avro.push(avro_json(&decode_any(i, bytes).await));
    }
    let status = mgr.get(&name).await.unwrap().status;
    let ddl: Vec<&serde_json::Value> =
        avro.iter().filter(|r| r.get("ddl").is_some()).collect();
    assert_eq!(ddl.len(), source.len(), "status {status}: {avro:#?}");
    assert_eq!(ddl.len(), 13, "every statement: {source:#?}");

    // Each Avro DDL record equals the source's event, field by field.
    for (a, s) in ddl.iter().zip(&source) {
        assert_eq!(a["event_id"], s["event_id"]);
        assert_eq!(a["op"], s["op"]);
        assert_eq!(a["ts_ms"], s["ts_ms"]);
        assert_eq!(a["mapping_version"], 2);
        for k in ["db", "table", "connector", "name", "ts_ms"] {
            assert_eq!(a["source"][k], s["source"][k], "source.{k}");
        }
        let (ap, sp) =
            (a["ddl"].as_object().unwrap(), s["ddl"].as_object().unwrap());
        for (k, v) in sp {
            assert_eq!(ap.get(k), Some(v), "ddl.{k}: {s}");
        }
        for (k, v) in ap {
            assert!(sp.contains_key(k) || v.is_null(), "ddl.{k}: {a}");
        }
    }

    let by_sql = |sql: &str| {
        ddl.iter()
            .find(|r| r["ddl"]["sql"] == sql)
            .unwrap_or_else(|| panic!("no record for {sql}: {ddl:#?}"))
    };
    let obj = |d: &str, n: &str| serde_json::json!({"kind": "table", "database": d, "name": n});
    let rename = by_sql("RENAME TABLE a TO a2");
    assert_eq!(
        rename["ddl"]["objects"],
        serde_json::json!([obj(db, "a"), obj(db, "a2")])
    );
    assert_eq!(rename["ddl"]["session_database"], db);
    let cross = by_sql(&format!("RENAME TABLE {db}.b TO {x}.b2"));
    assert_eq!(
        cross["ddl"]["objects"],
        serde_json::json!([obj(db, "b"), obj(x, "b2")])
    );
    assert_eq!(cross["ddl"]["database"], serde_json::Value::Null);
    assert_eq!(cross["source"]["db"], "");
    let scopes: Vec<&str> = ddl
        .iter()
        .filter_map(|r| r["ddl"]["scope"].as_str())
        .collect();
    for want in ["database", "lineage", "tables"] {
        assert!(scopes.contains(&want), "{want}: {scopes:?}");
    }
    let drop = ddl
        .iter()
        .find(|r| {
            r["ddl"]["sql"]
                .as_str()
                .is_some_and(|s| s.starts_with("DROP TABLE"))
        })
        .expect("the DROP TABLE record");
    assert_eq!(
        drop["ddl"]["objects"],
        serde_json::json!([obj(db, "c"), obj(db, "a2")])
    );
    // Executable comments, as executed and with their original SQL; live
    // and on catch-up.
    for (sql_text, objects) in [
        (
            format!("/*!50100 ALTER TABLE {db}.t ADD COLUMN c1 INT */"),
            serde_json::json!([obj(db, "t")]),
        ),
        (
            format!("/*! ALTER TABLE {db}.t ADD COLUMN c2 INT */"),
            serde_json::json!([obj(db, "t")]),
        ),
    ] {
        let r = by_sql(&sql_text);
        assert_eq!(r["ddl"]["scope"], "tables", "{sql_text}");
        assert_eq!(r["ddl"]["objects"], objects, "{sql_text}");
    }
    // CREATE, ALTER and DROP DATABASE: one scope, the database object.
    let database = |d: &str| serde_json::json!([{"kind": "database", "database": d, "name": d}]);
    for (sql_text, d) in [
        (format!("CREATE DATABASE IF NOT EXISTS `{x}`"), x),
        (format!("/*!40101 DROP DATABASE IF EXISTS `{x}` */"), x),
        (format!("ALTER DATABASE `{db}` CHARACTER SET utf8mb4"), db),
    ] {
        let r = by_sql(&sql_text);
        assert_eq!(r["ddl"]["scope"], "database", "{sql_text}");
        assert_eq!(r["ddl"]["objects"], database(d), "{sql_text}");
        assert_eq!(r["source"]["db"], d, "{sql_text}");
    }
    let redacted: Vec<_> = ddl
        .iter()
        .filter(|r| r["ddl"]["redacted"] == true)
        .collect();
    assert_eq!(redacted.len(), 2, "CREATE USER and ALTER USER");
    for r in &redacted {
        assert_eq!(r["ddl"]["sql"], serde_json::Value::Null);
    }
    for bytes in avro_raw.iter().chain(&json_raw) {
        let text = String::from_utf8_lossy(bytes);
        for secret in SECRETS {
            assert!(!text.contains(secret), "secret in a record");
        }
    }
    // Rows keep flowing with the new column.
    let row = avro
        .iter()
        .find(|r| r["after"]["id"] == 1)
        .expect("the row after the DDL");
    assert_eq!(row["after"]["extra"], 5);
    assert_eq!(row["after"]["c1"], 6);
    assert_eq!(row["after"]["c2"], 7);
    mgr.delete(&name).await.ok();
}

/// Every decoded record carries exactly the columns of its table's schema
/// version, with every value: an insert with a NULL, a later value for the
/// same column, and the before and after images of an update and a delete.
/// (Snapshot rows are excluded: they carry every value as text until the
/// snapshot decoder is schema-driven.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "docker: MySQL, Kafka and Schema Registry"]
async fn decoded_records_carry_every_column_of_their_schema_version() {
    let i = INFRA.get_or_init(start).await;
    let db = "ua_all";
    sql(
        i,
        &[
            format!("DROP DATABASE IF EXISTS {db}"),
            format!("CREATE DATABASE {db}"),
            format!(
                "CREATE TABLE {db}.t (id INT PRIMARY KEY, n INT NULL, \
                 s VARCHAR(10), b BIGINT UNSIGNED)"
            ),
        ],
    )
    .await;
    let mgr = manager().await;
    let name = pipeline_named(i, &mgr, "ua-all", db, "string", false).await;
    until_streaming(i, db).await;
    sql(
        i,
        &[
            format!("INSERT INTO {db}.t VALUES (2, NULL, 'x', 7)"),
            format!(
                "INSERT INTO {db}.t VALUES (3, 9, 'y', 18446744073709551615)"
            ),
            format!("UPDATE {db}.t SET n = 10, s = 'z' WHERE id = 2"),
            format!("DELETE FROM {db}.t WHERE id = 3"),
        ],
    )
    .await;
    let got = rows(
        consume(i, db, 5, Duration::from_secs(5), Duration::from_secs(120))
            .await,
    );
    let status = mgr.get(&name).await.unwrap().status;

    let img = |id: i32, n: Avro, s: &str, b: &str| {
        Some(BTreeMap::from([
            ("id".to_string(), Avro::Int(id)),
            ("n".to_string(), n),
            ("s".to_string(), Avro::String(s.into())),
            ("b".to_string(), Avro::String(b.into())),
        ]))
    };
    let want = [
        Rec {
            op: "c".into(),
            before: None,
            after: img(2, Avro::Null, "x", "7"),
        },
        Rec {
            op: "c".into(),
            before: None,
            after: img(3, Avro::Int(9), "y", "18446744073709551615"),
        },
        Rec {
            op: "u".into(),
            before: img(2, Avro::Null, "x", "7"),
            after: img(2, Avro::Int(10), "z", "7"),
        },
        Rec {
            op: "d".into(),
            before: img(3, Avro::Int(9), "y", "18446744073709551615"),
            after: None,
        },
    ];
    assert_eq!(got, want, "status {status}");
    mgr.delete(&name).await.ok();
}

/// The ids of a topic's records past the marker range (`id >= 10000`), in
/// topic order.
async fn ids(i: &Infra, topic: &str, want: usize) -> Vec<i64> {
    let mut out = Vec::new();
    for bytes in consume_raw(
        topic,
        want,
        Duration::from_secs(5),
        Duration::from_secs(180),
    )
    .await
    {
        let rec = decode(i, &bytes).await;
        let image = rec.after.or(rec.before).expect("a row image");
        let id = match image.get("id") {
            Some(Avro::Int(v)) => i64::from(*v),
            Some(Avro::Long(v)) => *v,
            other => panic!("id {other:?}"),
        };
        if id >= 10_000 {
            out.push(id);
        }
    }
    out
}

/// `count` rows from id `first` in transactions of `per_tx` rows.
fn inserts(db: &str, first: i64, count: i64, per_tx: i64) -> Vec<String> {
    (0..count)
        .step_by(per_tx as usize)
        .map(|start| {
            let values: Vec<String> = (start..(start + per_tx).min(count))
                .map(|k| format!("({}, {k}, {k})", first + k))
                .collect();
            format!("INSERT INTO {db}.t VALUES {}", values.join(", "))
        })
        .collect()
}

/// With four encoding threads every row arrives exactly once and in source
/// order, and the checkpoint advances with what was delivered: after a
/// stop and resume the pipeline continues from it (the rows written while
/// stopped arrive; none delivered before is delivered again).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "docker: MySQL, Kafka and Schema Registry"]
async fn four_encoding_threads_deliver_in_order_and_the_checkpoint_advances() {
    let i = INFRA.get_or_init(start).await;
    let db = "ua_workers";
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
    let name =
        pipeline_with(i, &mgr, "ua-workers", db, "string", false, false, 4)
            .await;
    until_streaming(i, db).await;

    sql(i, &inserts(db, 10_000, 2_000, 200)).await;
    let got = ids(i, db, 2_000).await;
    assert_eq!(
        got,
        (10_000..12_000).collect::<Vec<_>>(),
        "every row once, in order"
    );

    mgr.stop(&name).await.expect("stop");
    until_status(&mgr, &name, "stopped").await;
    sql(i, &inserts(db, 12_000, 500, 100)).await;
    mgr.resume(&name).await.expect("resume");
    until_status(&mgr, &name, "running").await;
    let got = ids(i, db, 2_500).await;
    assert_eq!(
        got,
        (10_000..12_500).collect::<Vec<_>>(),
        "resumed from the checkpoint: the new rows once, nothing again"
    );
    mgr.delete(&name).await.ok();
}

/// The ids a pipeline encoding on `workers` threads publishes when one
/// transaction's row cannot be encoded: 100 rows, then a 300-row
/// transaction whose 151st row holds a value long mode cannot, then 50 more
/// rows after the pipeline failed.
async fn failing_row_scenario(i: &Infra, workers: usize) -> Vec<i64> {
    let db = format!("ua_workers_fail{workers}");
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
    let name = pipeline_with(
        i,
        &mgr,
        &format!("ua-workers-fail{workers}"),
        &db,
        "long",
        false,
        false,
        workers,
    )
    .await;
    until_streaming(i, &db).await;
    sql(i, &inserts(&db, 10_000, 100, 100)).await;
    assert_eq!(ids(i, &db, 100).await, (10_000..10_100).collect::<Vec<_>>());

    let values: Vec<String> = (0..300_i64)
        .map(|k| {
            let b = if k == 150 { I64_MAX + 1 } else { k as u64 };
            format!("({}, {k}, {b})", 20_000 + k)
        })
        .collect();
    sql(
        i,
        &[format!("INSERT INTO {db}.t VALUES {}", values.join(", "))],
    )
    .await;
    until_status(&mgr, &name, "failed").await;
    let incidents = mgr.incidents(&name).await.unwrap().to_string();
    assert!(incidents.contains("sink_conversion_failed"), "{incidents}");
    sql(i, &inserts(&db, 30_000, 50, 50)).await;
    let got = ids(i, &db, 399).await;
    mgr.delete(&name).await.ok();
    got
}

/// With four encoding threads a row that cannot be encoded is handled
/// exactly as with one: it is never published, the rows of its batch are
/// (a conversion failure is a row's, which a DLQ would take; without one
/// the pipeline fails after the batch, its checkpoint not advanced), and no
/// later batch is published.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "docker: MySQL, Kafka and Schema Registry"]
async fn with_four_encoding_threads_a_failing_row_is_handled_as_with_one() {
    capture_logs();
    let i = INFRA.get_or_init(start).await;
    let one = failing_row_scenario(i, 1).await;
    let four = failing_row_scenario(i, 4).await;
    let want: Vec<i64> = (10_000..10_100)
        .chain((20_000..20_300).filter(|id| *id != 20_150))
        .collect();
    assert_eq!(one, want, "one thread");
    assert_eq!(four, one, "four threads publish what one does");
}
