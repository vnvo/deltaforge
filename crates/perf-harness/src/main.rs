//! Canonical-value performance comparison: an A/B of two DeltaForge release
//! binaries (`runner`) on identical, deterministic fixtures
//! (`docs/design/canonical-performance.md`).
//!
//! Every run starts a fresh process of one binary with its own state store
//! and measures it from outside, so the two versions need nothing in common
//! but the pipeline configuration format:
//!
//! - throughput: rows over the time from the start of delivery (resume of a
//!   paused pipeline with a backlog, for CDC; pipeline creation, for a
//!   snapshot) to the last row reaching the sink;
//! - latency (Kafka): commit-to-arrival of single-row transactions at a
//!   steady rate;
//! - process CPU (`/proc/<pid>/stat`), peak resident memory (sampled
//!   `VmRSS`), allocation requests (jemalloc's exit statistics of a build
//!   with `tikv-jemallocator/stats`, minus an idle run's),
//!   output bytes (Kafka payloads and keys, S3 data objects) and database CPU
//!   (its container's cgroup) over the delivery window.
//!
//! Runs alternate the versions within every repetition, so drift on the
//! shared machine affects both alike. Results go to `<out>/runs.jsonl`
//! (one line per run) and `<out>/summary.md` (medians per cell).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use gate_ownership::GateOwned;
use mysql_async::prelude::Queryable;
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use serde_json::{Value, json};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

mod debezium;
mod extras;
mod threads;

const NETWORK: &str = "df-perf-net";
/// MySQL's name on the harness network (Kafka Connect reaches it there).
const MYSQL_NAME: &str = "df-perf-mysql";
const KAFKA_NAME: &str = "df-perf-kafka";
const KAFKA_INTERNAL_PORT: u16 = 29092;
const KAFKA_HOST_PORT: u16 = 9397;
const PW: &str = "pw";
const BUCKET: &str = "perf";
const API: &str = "127.0.0.1:18080";
const METRICS: &str = "127.0.0.1:19000";
/// Rows per INSERT statement (and so per source transaction).
const ROWS_PER_STATEMENT: usize = 500;

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
const SCHEMA_REGISTRY_NAME: &str = "df-perf-sr";

#[derive(Parser, Debug)]
struct Args {
    /// `label=path` of a DeltaForge release binary; exactly two.
    #[arg(long = "binary", required = true)]
    binaries: Vec<String>,
    /// Output directory.
    #[arg(long)]
    out: PathBuf,
    /// Repetitions of every cell (versions alternate within each).
    #[arg(long, default_value_t = 3)]
    reps: usize,
    /// Only cells whose id contains one of these.
    #[arg(long)]
    filter: Vec<String>,
    /// Scale every cell's row count (a smoke run uses e.g. 0.05).
    #[arg(long, default_value_t = 1.0)]
    scale: f64,
    /// List the cells and exit.
    #[arg(long)]
    list: bool,
    /// DeltaForge's state store: `sqlite` (default) or `postgres` (a
    /// separate container, with its reads counted by pg_stat_statements).
    #[arg(long, default_value = "sqlite")]
    state_store: String,
    /// Batch settings (a JSON object) added to every DeltaForge pipeline,
    /// e.g. `{"min_flush_interval_ms": 10}`.
    #[arg(long)]
    df_batch: Option<String>,
    /// Sink settings (a JSON object) added to the DeltaForge sink of the
    /// system labelled LABEL, as `LABEL=JSON`, e.g.
    /// `two='{"encode_workers": 2}'` (repeatable; the same binary may run
    /// under two labels).
    #[arg(long = "df-sink")]
    df_sink: Vec<String>,
    /// Read DeltaForge's batch, checkpoint and Kafka request counters over
    /// each run's window (adds 5.5 s per run: librdkafka's statistics
    /// interval).
    #[arg(long)]
    delivery_stats: bool,
    /// Run every cell once for every system before the measured
    /// repetitions, unrecorded (a JIT-compiled system is measured warm).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    warmup: bool,
    /// The Debezium profile (`defaults` or `tuned`) of a `debezium` system
    /// (`--binary label=debezium`).
    #[arg(long, default_value = "defaults")]
    debezium_profile: String,
    /// After the last row arrives, watch the PostgreSQL slot's
    /// confirmed_flush_lsn to measure WAL-feedback staleness.
    #[arg(long)]
    feedback_probe: bool,
    /// Where the Debezium worker's added plugins (the Avro converter,
    /// GraalJS) are downloaded and verified.
    #[arg(long, default_value = "target/perf-cache")]
    cache: PathBuf,
}

// ---------------------------------------------------------------------------
// The matrix
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Engine {
    Mysql,
    Postgres,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    /// Backlog drain: N rows written while the pipeline is paused.
    Cdc,
    /// Initial snapshot of N preloaded rows.
    Snapshot,
    /// Single-row transactions at a steady rate (Kafka sinks).
    Latency,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Sink {
    Json,
    Avro,
    Parquet,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    Narrow,
    Wide,
    Binary,
    Temporal,
    NullHeavy,
    /// `id` and n INT columns (row-width sweep).
    Ints(usize),
}

#[derive(Clone, Debug)]
struct Cell {
    engine: Engine,
    mode: Mode,
    sink: Sink,
    shape: Shape,
    batch: Option<usize>,
    js: bool,
    rows: usize,
    /// Latency cells: how the transactions are written.
    load: Load,
}

/// A latency cell's write pattern: `rate` transactions per second of
/// `tx_rows` rows each, on one connection; with `burst`, groups of
/// `burst.0` transactions separated by `burst.1` ms of idle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Load {
    rate: u32,
    tx_rows: usize,
    burst: Option<(usize, u64)>,
}

impl Load {
    const DEFAULT: Load = Load {
        rate: 200,
        tx_rows: 1,
        burst: None,
    };
}

impl Cell {
    fn id(&self) -> String {
        let shape = match self.shape {
            Shape::Narrow => "narrow".to_string(),
            Shape::Wide => "wide".into(),
            Shape::Binary => "binary".into(),
            Shape::Temporal => "temporal".into(),
            Shape::NullHeavy => "nullheavy".into(),
            Shape::Ints(n) => format!("ints{n}"),
        };
        let mut id = format!(
            "{}-{}-{}-{shape}",
            match self.engine {
                Engine::Mysql => "my",
                Engine::Postgres => "pg",
            },
            match self.mode {
                Mode::Cdc => "cdc",
                Mode::Snapshot => "snap",
                Mode::Latency => "lat",
            },
            match self.sink {
                Sink::Json => "json",
                Sink::Avro => "avro",
                Sink::Parquet => "parquet",
            },
        );
        if let Some(b) = self.batch {
            id.push_str(&format!("-b{b}"));
        }
        if self.js {
            id.push_str("-js");
        }
        if self.load != Load::DEFAULT {
            let l = self.load;
            if l.rate != Load::DEFAULT.rate {
                id.push_str(&format!("-r{}", l.rate));
            }
            if l.tx_rows != 1 {
                id.push_str(&format!("-tx{}", l.tx_rows));
            }
            if let Some((n, idle)) = l.burst {
                id.push_str(&format!("-burst{n}x{idle}ms"));
            }
        }
        id
    }
}

fn rows_for(shape: Shape) -> usize {
    match shape {
        Shape::Narrow => 200_000,
        Shape::Temporal => 100_000,
        Shape::Wide | Shape::Binary | Shape::NullHeavy | Shape::Ints(_) => {
            50_000
        }
    }
}

fn matrix() -> Vec<Cell> {
    let mut cells = Vec::new();
    let shapes = [
        Shape::Narrow,
        Shape::Wide,
        Shape::Binary,
        Shape::Temporal,
        Shape::NullHeavy,
    ];
    for engine in [Engine::Mysql, Engine::Postgres] {
        for mode in [Mode::Cdc, Mode::Snapshot] {
            for sink in [Sink::Json, Sink::Avro, Sink::Parquet] {
                for shape in shapes {
                    cells.push(Cell {
                        engine,
                        mode,
                        sink,
                        shape,
                        batch: None,
                        js: false,
                        rows: rows_for(shape),
                        load: Load::DEFAULT,
                    });
                }
            }
        }
        // Batch sizes (CDC, JSON).
        for shape in [Shape::Narrow, Shape::Wide] {
            for batch in [256, 4096] {
                cells.push(Cell {
                    engine,
                    mode: Mode::Cdc,
                    sink: Sink::Json,
                    shape,
                    batch: Some(batch),
                    js: false,
                    rows: rows_for(shape),
                    load: Load::DEFAULT,
                });
            }
        }
        // Row-width sweep (CDC, JSON and Avro).
        for sink in [Sink::Json, Sink::Avro] {
            for n in [10, 50, 200] {
                cells.push(Cell {
                    engine,
                    mode: Mode::Cdc,
                    sink,
                    shape: Shape::Ints(n),
                    batch: None,
                    js: false,
                    rows: rows_for(Shape::Ints(n)),
                    load: Load::DEFAULT,
                });
            }
        }
        // JavaScript (CDC, JSON).
        for shape in [Shape::Narrow, Shape::Wide] {
            cells.push(Cell {
                engine,
                mode: Mode::Cdc,
                sink: Sink::Json,
                shape,
                batch: None,
                js: true,
                rows: 50_000,
                load: Load::DEFAULT,
            });
        }
        // Latency (Kafka sinks).
        for sink in [Sink::Json, Sink::Avro] {
            cells.push(Cell {
                engine,
                mode: Mode::Latency,
                sink,
                shape: Shape::Narrow,
                batch: None,
                js: false,
                rows: 2_000,
                load: Load::DEFAULT,
            });
        }
        // Latency at 50 transactions/s (Avro), and with JavaScript (JSON).
        for (sink, js, rate, rows) in [
            (Sink::Avro, false, 50, 1_000),
            (Sink::Json, true, 200, 2_000),
        ] {
            cells.push(Cell {
                engine,
                mode: Mode::Latency,
                sink,
                shape: Shape::Narrow,
                batch: None,
                js,
                rows,
                load: Load {
                    rate,
                    ..Load::DEFAULT
                },
            });
        }
        // Latency across rates, transaction sizes and bursts (JSON).
        for (rows, load) in [
            (
                30,
                Load {
                    rate: 1,
                    ..Load::DEFAULT
                },
            ),
            (
                1_000,
                Load {
                    rate: 50,
                    ..Load::DEFAULT
                },
            ),
            (
                5_000,
                Load {
                    rate: 1_000,
                    ..Load::DEFAULT
                },
            ),
            (
                20_000,
                Load {
                    tx_rows: 10,
                    ..Load::DEFAULT
                },
            ),
            (
                1_000,
                Load {
                    rate: 1_000,
                    burst: Some((200, 2_000)),
                    ..Load::DEFAULT
                },
            ),
        ] {
            cells.push(Cell {
                engine,
                mode: Mode::Latency,
                sink: Sink::Json,
                shape: Shape::Narrow,
                batch: None,
                js: false,
                rows,
                load,
            });
        }
    }
    cells
}

// ---------------------------------------------------------------------------
// Fixtures: deterministic rows
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum Col {
    Int,
    BigInt,
    Varchar,
    Decimal,
    Double,
    Date,
    Time,
    DateTime,
    Timestamp,
    Bytes,
}

fn columns(shape: Shape) -> Vec<Col> {
    use Col::*;
    let cycle = |kinds: &[Col], n: usize| -> Vec<Col> {
        (0..n).map(|i| kinds[i % kinds.len()]).collect()
    };
    match shape {
        Shape::Narrow => vec![Int, Varchar, Decimal, Timestamp],
        Shape::Wide | Shape::NullHeavy => {
            cycle(&[Int, BigInt, Varchar, Decimal, Double, Date, DateTime], 60)
        }
        Shape::Binary => vec![Bytes; 4],
        Shape::Temporal => cycle(&[Date, Time, DateTime, Timestamp], 20),
        Shape::Ints(n) => vec![Int; n],
    }
}

fn sql_type(engine: Engine, c: Col) -> &'static str {
    match (engine, c) {
        (_, Col::Int) => "INT",
        (_, Col::BigInt) => "BIGINT",
        (_, Col::Varchar) => "VARCHAR(64)",
        (_, Col::Decimal) => "DECIMAL(12,3)",
        (Engine::Mysql, Col::Double) => "DOUBLE",
        (Engine::Postgres, Col::Double) => "DOUBLE PRECISION",
        (_, Col::Date) => "DATE",
        (Engine::Mysql, Col::Time) => "TIME(6)",
        (Engine::Postgres, Col::Time) => "TIME(6)",
        (Engine::Mysql, Col::DateTime) => "DATETIME(6)",
        (Engine::Postgres, Col::DateTime) => "TIMESTAMP(6)",
        (Engine::Mysql, Col::Timestamp) => "TIMESTAMP(6) NULL",
        (Engine::Postgres, Col::Timestamp) => "TIMESTAMPTZ(6)",
        (Engine::Mysql, Col::Bytes) => "BLOB",
        (Engine::Postgres, Col::Bytes) => "BYTEA",
    }
}

/// A deterministic 64-bit value for (row, column).
fn mix(row: usize, col: usize) -> u64 {
    let mut x = (row as u64)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add((col as u64 + 1).wrapping_mul(0xC2B2_AE3D_27D4_EB4F));
    x ^= x >> 33;
    x = x.wrapping_mul(0xFF51_AFD7_ED55_8CCD);
    x ^= x >> 33;
    x
}

fn literal(
    engine: Engine,
    shape: Shape,
    c: Col,
    row: usize,
    col: usize,
) -> String {
    let r = mix(row, col);
    if shape == Shape::NullHeavy && !r.is_multiple_of(10) {
        return "NULL".into();
    }
    let day = r % 20_000; // days after 1970-01-01
    let (y, m, d) = civil(day as i64);
    let micros = r % 86_400_000_000;
    let clock = format!(
        "{:02}:{:02}:{:02}.{:06}",
        micros / 3_600_000_000,
        micros / 60_000_000 % 60,
        micros / 1_000_000 % 60,
        micros % 1_000_000
    );
    match c {
        Col::Int => format!("{}", (r % 2_000_000_000) as i64 - 1_000_000_000),
        Col::BigInt => format!("{}", r as i64),
        Col::Varchar => format!("'v{:x}'", r % 0xFFFF_FFFF_FFFF),
        Col::Decimal => format!("{}.{:03}", r % 1_000_000_000, r % 1000),
        Col::Double => format!("{:.6}", (r % 1_000_000) as f64 / 7.0),
        Col::Date => format!("'{y:04}-{m:02}-{d:02}'"),
        Col::Time => format!("'{clock}'"),
        Col::DateTime => format!("'{y:04}-{m:02}-{d:02} {clock}'"),
        Col::Timestamp => match engine {
            Engine::Mysql => format!("'{y:04}-{m:02}-{d:02} {clock}'"),
            Engine::Postgres => format!("'{y:04}-{m:02}-{d:02} {clock}+00'"),
        },
        Col::Bytes => {
            let hex: String = (0..32)
                .map(|i| format!("{:016x}", mix(row, col * 100 + i)))
                .collect::<String>()[..512]
                .to_string(); // 256 bytes
            match engine {
                Engine::Mysql => format!("X'{hex}'"),
                Engine::Postgres => format!("'\\x{hex}'::bytea"),
            }
        }
    }
}

/// Days since 1970-01-01 as (year, month, day), keeping years >= 1970.
fn civil(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

fn create_table(engine: Engine, ns: &str, shape: Shape) -> String {
    let cols: Vec<String> = columns(shape)
        .iter()
        .enumerate()
        .map(|(i, c)| format!("c{i} {}", sql_type(engine, *c)))
        .collect();
    format!(
        "CREATE TABLE {ns}.t (id BIGINT PRIMARY KEY, {})",
        cols.join(", ")
    )
}

/// INSERT statements of rows `from..to`, `ROWS_PER_STATEMENT` per statement.
fn inserts(
    engine: Engine,
    ns: &str,
    shape: Shape,
    from: usize,
    to: usize,
) -> Vec<String> {
    let cols = columns(shape);
    let names: Vec<String> = (0..cols.len()).map(|i| format!("c{i}")).collect();
    let mut out = Vec::new();
    let mut start = from;
    while start < to {
        let end = (start + ROWS_PER_STATEMENT).min(to);
        let rows: Vec<String> = (start..end)
            .map(|row| {
                let vals: Vec<String> = cols
                    .iter()
                    .enumerate()
                    .map(|(i, c)| literal(engine, shape, *c, row, i))
                    .collect();
                format!("({}, {})", row + 1, vals.join(", "))
            })
            .collect();
        out.push(format!(
            "INSERT INTO {ns}.t (id, {}) VALUES {}",
            names.join(", "),
            rows.join(", ")
        ));
        start = end;
    }
    out
}

// ---------------------------------------------------------------------------
// Infrastructure
// ---------------------------------------------------------------------------

struct Infra {
    _kafka: ContainerAsync<GenericImage>,
    sr: ContainerAsync<GenericImage>,
    mysql: ContainerAsync<GenericImage>,
    pg: ContainerAsync<GenericImage>,
    /// DeltaForge's PostgreSQL state store, when used.
    state: Option<ContainerAsync<GenericImage>>,
    s3: &'static s3_test_server::S3Server,
}

impl Infra {
    async fn start(state_pg: bool) -> Result<Infra> {
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
            .context("start kafka")?;
        std::process::Command::new("docker")
            .args(["rm", "-f", SCHEMA_REGISTRY_NAME])
            .output()
            .ok();
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
                .with_env_var(
                    "SCHEMA_REGISTRY_LISTENERS",
                    "http://0.0.0.0:8081",
                )
                .with_container_name(SCHEMA_REGISTRY_NAME)
                .gate_owned()
                .start()
                .await
                .context("start schema registry")?;
        std::process::Command::new("docker")
            .args(["rm", "-f", MYSQL_NAME])
            .output()
            .ok();
        let mysql = GenericImage::new(MYSQL_IMAGE.0, MYSQL_IMAGE.1)
            .with_wait_for(WaitFor::message_on_stderr(
                "ready for connections. Version: '8.4",
            ))
            .with_env_var("MYSQL_ROOT_PASSWORD", PW)
            .with_container_name(MYSQL_NAME)
            .with_network(NETWORK)
            .with_cmd(vec![
                "--server-id=60",
                "--log-bin=mysql-bin",
                "--binlog-format=ROW",
                "--binlog-row-image=FULL",
                "--gtid-mode=ON",
                "--enforce-gtid-consistency=ON",
                "--max-allowed-packet=256M",
            ])
            .gate_owned()
            .start()
            .await
            .context("start mysql")?;
        let pg = GenericImage::new(POSTGRES_IMAGE.0, POSTGRES_IMAGE.1)
            .with_wait_for(WaitFor::message_on_stderr(
                "database system is ready to accept connections",
            ))
            .with_env_var("POSTGRES_PASSWORD", PW)
            .with_cmd([
                "postgres",
                "-c",
                "wal_level=logical",
                "-c",
                "max_replication_slots=64",
                "-c",
                "max_wal_senders=64",
            ])
            .gate_owned()
            .start()
            .await
            .context("start postgres")?;
        let state = if state_pg {
            let c = GenericImage::new(POSTGRES_IMAGE.0, POSTGRES_IMAGE.1)
                .with_wait_for(WaitFor::message_on_stderr(
                    "database system is ready to accept connections",
                ))
                .with_env_var("POSTGRES_PASSWORD", PW)
                .with_cmd([
                    "postgres",
                    "-c",
                    "shared_preload_libraries=pg_stat_statements",
                    "-c",
                    "pg_stat_statements.max=10000",
                    "-c",
                    "max_connections=300",
                ])
                .gate_owned()
                .start()
                .await
                .context("start state postgres")?;
            Some(c)
        } else {
            None
        };
        let s3 = s3_test_server::shared().await;
        s3.create_bucket(BUCKET).await.context("bucket")?;
        let infra = Infra {
            _kafka: kafka,
            sr,
            mysql,
            pg,
            state,
            s3,
        };
        if infra.state.is_some() {
            infra
                .state_exec(
                    "postgres",
                    "CREATE EXTENSION IF NOT EXISTS pg_stat_statements",
                )
                .await?;
        }
        let deadline = Instant::now() + Duration::from_secs(180);
        loop {
            let sr_ok =
                reqwest::get(format!("{}/subjects", infra.sr_url().await))
                    .await
                    .is_ok_and(|r| r.status().is_success());
            let my_ok = mysql_async::Conn::from_url(infra.mysql_dsn().await)
                .await
                .is_ok();
            let pg_ok = tokio_postgres::connect(
                &infra.pg_dsn().await,
                tokio_postgres::NoTls,
            )
            .await
            .is_ok();
            if sr_ok && my_ok && pg_ok {
                break;
            }
            if Instant::now() > deadline {
                bail!("infrastructure never ready");
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Ok(infra)
    }

    async fn sr_url(&self) -> String {
        format!(
            "http://127.0.0.1:{}",
            self.sr.get_host_port_ipv4(8081).await.unwrap()
        )
    }

    async fn mysql_dsn(&self) -> String {
        let port = self.mysql.get_host_port_ipv4(3306).await.unwrap();
        format!("mysql://root:{PW}@127.0.0.1:{port}/")
    }

    async fn pg_dsn(&self) -> String {
        let port = self.pg.get_host_port_ipv4(5432).await.unwrap();
        format!(
            "host=127.0.0.1 port={port} user=postgres password={PW} dbname=postgres"
        )
    }

    async fn state_dsn(&self, db: &str) -> Option<String> {
        let c = self.state.as_ref()?;
        let port = c.get_host_port_ipv4(5432).await.ok()?;
        Some(format!(
            "host=127.0.0.1 port={port} user=postgres password={PW} dbname={db}"
        ))
    }

    async fn state_client(&self, db: &str) -> Result<tokio_postgres::Client> {
        let dsn = self
            .state_dsn(db)
            .await
            .ok_or_else(|| anyhow!("no state store"))?;
        let (c, conn) =
            tokio_postgres::connect(&dsn, tokio_postgres::NoTls).await?;
        tokio::spawn(conn);
        Ok(c)
    }

    async fn state_exec(&self, db: &str, stmt: &str) -> Result<()> {
        self.state_client(db).await?.batch_execute(stmt).await?;
        Ok(())
    }

    /// Reads of the key-value table in `db` so far (pg_stat_statements).
    async fn state_reads(&self, db: &str) -> Option<u64> {
        let c = self.state_client("postgres").await.ok()?;
        let row = c
            .query_one(
                "SELECT coalesce(sum(s.calls), 0)::bigint FROM pg_stat_statements s \
                 JOIN pg_database d ON d.oid = s.dbid \
                 WHERE d.datname = $1 AND s.query ILIKE 'select%df_kv%'",
                &[&db],
            )
            .await
            .ok()?;
        Some(row.get::<_, i64>(0) as u64)
    }

    /// The source's current WAL insert position, as a number.
    async fn wal_insert_lsn(&self) -> Option<u64> {
        let (c, conn) = tokio_postgres::connect(
            &self.pg_dsn().await,
            tokio_postgres::NoTls,
        )
        .await
        .ok()?;
        tokio::spawn(conn);
        let row = c
            .query_one("SELECT pg_current_wal_insert_lsn()::text", &[])
            .await
            .ok()?;
        parse_lsn(&row.get::<_, String>(0))
    }

    /// The source slot's confirmed_flush_lsn, as a number.
    async fn confirmed_flush(&self, slot: &str) -> Option<u64> {
        let (c, conn) = tokio_postgres::connect(
            &self.pg_dsn().await,
            tokio_postgres::NoTls,
        )
        .await
        .ok()?;
        tokio::spawn(conn);
        let row = c
            .query_opt(
                "SELECT confirmed_flush_lsn::text FROM pg_replication_slots \
                 WHERE slot_name = $1",
                &[&slot],
            )
            .await
            .ok()??;
        let text: Option<String> = row.get(0);
        parse_lsn(&text?)
    }

    /// Remove this run's containers and their anonymous volumes.
    fn cleanup(&self) {
        let state = self.state.as_ref().map(|c| c.id());
        for id in [
            Some(self._kafka.id()),
            Some(self.sr.id()),
            Some(self.mysql.id()),
            Some(self.pg.id()),
            state,
        ]
        .into_iter()
        .flatten()
        {
            std::process::Command::new("docker")
                .args(["rm", "-f", "-v", id])
                .output()
                .ok();
        }
        s3_test_server::remove_shared();
    }

    fn container_id(&self, engine: Engine) -> String {
        match engine {
            Engine::Mysql => self.mysql.id().to_string(),
            Engine::Postgres => self.pg.id().to_string(),
        }
    }

    /// The database container's cumulative CPU (cgroup v2), microseconds.
    fn db_cpu_usec(&self, engine: Engine) -> Option<u64> {
        let id = self.container_id(engine);
        let stat = std::fs::read_to_string(format!(
            "/sys/fs/cgroup/system.slice/docker-{id}.scope/cpu.stat"
        ))
        .ok()?;
        stat.lines()
            .find_map(|l| l.strip_prefix("usage_usec "))
            .and_then(|v| v.trim().parse().ok())
    }

    async fn exec(&self, engine: Engine, stmts: &[String]) -> Result<()> {
        match engine {
            Engine::Mysql => {
                let mut c =
                    mysql_async::Conn::from_url(self.mysql_dsn().await).await?;
                c.query_drop("SET SESSION time_zone = '+00:00'").await?;
                for s in stmts {
                    c.query_drop(s).await.with_context(|| truncate(s))?;
                }
                c.disconnect().await?;
            }
            Engine::Postgres => {
                let (c, conn) = tokio_postgres::connect(
                    &self.pg_dsn().await,
                    tokio_postgres::NoTls,
                )
                .await?;
                tokio::spawn(conn);
                for s in stmts {
                    c.batch_execute(s).await.with_context(|| truncate(s))?;
                }
            }
        }
        Ok(())
    }
}

/// One open connection.
enum Sql {
    Mysql(mysql_async::Conn),
    Postgres(tokio_postgres::Client),
}

impl Sql {
    async fn open(infra: &Infra, engine: Engine) -> Result<Sql> {
        Ok(match engine {
            Engine::Mysql => {
                let mut c =
                    mysql_async::Conn::from_url(infra.mysql_dsn().await)
                        .await?;
                c.query_drop("SET SESSION time_zone = '+00:00'").await?;
                Sql::Mysql(c)
            }
            Engine::Postgres => {
                let (c, conn) = tokio_postgres::connect(
                    &infra.pg_dsn().await,
                    tokio_postgres::NoTls,
                )
                .await?;
                tokio::spawn(conn);
                Sql::Postgres(c)
            }
        })
    }

    async fn exec(&mut self, stmt: &str) -> Result<()> {
        match self {
            Sql::Mysql(c) => c.query_drop(stmt).await?,
            Sql::Postgres(c) => c.batch_execute(stmt).await?,
        }
        Ok(())
    }
}

fn truncate(s: &str) -> String {
    s.chars().take(200).collect()
}

// ---------------------------------------------------------------------------
// The process under test
// ---------------------------------------------------------------------------

/// A system under test: a DeltaForge release binary, or Debezium on Kafka
/// Connect (`--binary label=debezium`).
#[derive(Clone, Debug)]
enum System {
    DeltaForge(PathBuf),
    Debezium(debezium::Profile),
}

/// The running system of one run: a DeltaForge process of its own, or
/// the invocation's shared (warmed-up) Kafka Connect worker.
enum Sut<'a> {
    Df(Box<Process>),
    Dbz(&'a debezium::Connect),
}

impl Sut<'_> {
    fn cpu_secs(&self) -> f64 {
        match self {
            Sut::Df(p) => p.cpu_secs(),
            Sut::Dbz(c) => {
                let (user, sys) = c.cpu_split();
                user + sys
            }
        }
    }

    fn sys_secs(&self) -> f64 {
        match self {
            Sut::Df(p) => p.sys_secs(),
            Sut::Dbz(c) => c.cpu_split().1,
        }
    }

    /// Process kernel counters (DeltaForge only; zeros for a container).
    fn kernel_counts(&self) -> [u64; 5] {
        match self {
            Sut::Df(p) => p.kernel_counts(),
            Sut::Dbz(_) => [0; 5],
        }
    }

    fn rss_kib(&self) -> u64 {
        match self {
            Sut::Df(p) => p.rss_kib(),
            Sut::Dbz(c) => c.rss_kib(),
        }
    }

    async fn stop(self) -> Option<u64> {
        match self {
            Sut::Df(p) => p.stop().await,
            // The shared worker outlives the run.
            Sut::Dbz(_) => None,
        }
    }
}

struct Process {
    child: tokio::process::Child,
    /// The runner's own process: the one listening on the API address (the
    /// started command itself, or a process it runs, e.g. under a wrapper).
    pid: u32,
    /// The executable that process runs.
    exe: PathBuf,
    stderr: PathBuf,
}

/// Refuse to start a runner while anything answers on the API address or
/// holds the API or metrics port: a leftover runner would serve the run.
async fn ports_free() -> Result<()> {
    if reqwest::get(format!("http://{API}/health")).await.is_ok() {
        bail!("something answers on {API} before the run (a leftover runner?)");
    }
    for addr in [API, METRICS] {
        std::net::TcpListener::bind(addr).with_context(|| {
            format!("{addr} is in use before the run (a leftover runner?)")
        })?;
    }
    Ok(())
}

/// The process listening on TCP `port` (IPv4 or IPv6), from `/proc`.
fn listener_pid(port: u16) -> Option<u32> {
    let inodes: Vec<String> = ["/proc/net/tcp", "/proc/net/tcp6"]
        .iter()
        .filter_map(|f| std::fs::read_to_string(f).ok())
        .flat_map(|t| {
            t.lines()
                .skip(1)
                .filter_map(|l| {
                    let f: Vec<&str> = l.split_whitespace().collect();
                    let local_port = f.get(1)?.rsplit_once(':')?.1;
                    let inode = f.get(9)?;
                    (u16::from_str_radix(local_port, 16).ok()? == port
                        && *f.get(3)? == "0A")
                        .then(|| format!("socket:[{inode}]"))
                })
                .collect::<Vec<_>>()
        })
        .collect();
    std::fs::read_dir("/proc").ok()?.flatten().find_map(|e| {
        let pid: u32 = e.file_name().to_str()?.parse().ok()?;
        std::fs::read_dir(e.path().join("fd"))
            .ok()?
            .flatten()
            .find_map(|fd| {
                let target = std::fs::read_link(fd.path()).ok()?;
                inodes
                    .iter()
                    .any(|i| target.as_os_str() == i.as_str())
                    .then_some(pid)
            })
    })
}

/// Whether `pid` is `ancestor` or runs under it.
fn descends_from(mut pid: u32, ancestor: u32) -> bool {
    while pid > 1 {
        if pid == ancestor {
            return true;
        }
        let Some(ppid) = std::fs::read_to_string(format!("/proc/{pid}/stat"))
            .ok()
            .and_then(|s| {
                s.rsplit_once(')')?
                    .1
                    .split_whitespace()
                    .nth(1)?
                    .parse()
                    .ok()
            })
        else {
            return false;
        };
        pid = ppid;
    }
    false
}

/// The SHA-256 of a file, as `sha256sum` prints it.
fn sha256(path: &Path) -> Option<String> {
    let out = std::process::Command::new("sha256sum")
        .arg(path)
        .output()
        .ok()?;
    String::from_utf8(out.stdout)
        .ok()?
        .split_whitespace()
        .next()
        .map(str::to_string)
}

impl Process {
    async fn start(
        binary: &Path,
        dir: &Path,
        state_dsn: Option<&str>,
    ) -> Result<Process> {
        std::fs::create_dir_all(dir)?;
        ports_free().await?;
        let stderr = dir.join("stderr.log");
        let mut cmd = tokio::process::Command::new(binary);
        cmd.args(["--api-addr", API, "--metrics-addr", METRICS]);
        match state_dsn {
            Some(dsn) => cmd.args([
                "--storage-backend",
                "postgres",
                "--storage-dsn",
                dsn,
            ]),
            None => cmd.arg("--storage-path").arg(dir.join("state.db")),
        };
        let mut child = cmd
            .env("RUST_LOG", "warn")
            .env("_RJEM_MALLOC_CONF", "stats_print:true")
            .stdout(std::fs::File::create(dir.join("stdout.log"))?)
            .stderr(std::fs::File::create(&stderr)?)
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("start {}", binary.display()))?;
        let child_pid = child.id().ok_or_else(|| anyhow!("no pid"))?;
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(status) = child.try_wait()? {
                bail!(
                    "{} exited before becoming healthy: {status}",
                    binary.display()
                );
            }
            if reqwest::get(format!("http://{API}/health"))
                .await
                .is_ok_and(|r| r.status().is_success())
            {
                break;
            }
            if Instant::now() > deadline {
                bail!("{} never became healthy", binary.display());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        // The API must be served by the process just started (or one it
        // runs), never by another.
        let api_port: u16 = API
            .rsplit_once(':')
            .and_then(|(_, p)| p.parse().ok())
            .unwrap_or(0);
        let pid = listener_pid(api_port)
            .ok_or_else(|| anyhow!("no process found listening on {API}"))?;
        if !descends_from(pid, child_pid) {
            bail!(
                "{API} is served by pid {pid}, not by the started runner ({child_pid})"
            );
        }
        let exe = std::fs::read_link(format!("/proc/{pid}/exe"))?;
        Ok(Process {
            child,
            pid,
            exe,
            stderr,
        })
    }

    /// utime + stime, seconds.
    fn cpu_secs(&self) -> f64 {
        let (user, sys) = self.cpu_split();
        user + sys
    }

    /// Kernel counters: read and write system calls (`/proc/<pid>/io`
    /// `syscr`, `syscw`), the main thread's voluntary and involuntary
    /// context switches, and the process's minor page faults.
    fn kernel_counts(&self) -> [u64; 5] {
        let read = |file: &str| {
            std::fs::read_to_string(format!("/proc/{}/{file}", self.pid))
                .unwrap_or_default()
        };
        let field = |text: &str, name: &str| {
            text.lines()
                .find_map(|l| l.strip_prefix(name))
                .and_then(|v| v.trim().parse().ok())
                .unwrap_or(0)
        };
        let (io, status, stat) = (read("io"), read("status"), read("stat"));
        // minflt = field 10 (1-based); after the command name, field 3 is
        // the first.
        let minflt = stat
            .rsplit_once(')')
            .and_then(|(_, r)| r.split_whitespace().nth(7))
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        [
            field(&io, "syscr:"),
            field(&io, "syscw:"),
            field(&status, "voluntary_ctxt_switches:"),
            field(&status, "nonvoluntary_ctxt_switches:"),
            minflt,
        ]
    }

    /// stime (system time), seconds.
    fn sys_secs(&self) -> f64 {
        self.cpu_split().1
    }

    /// (utime, stime), seconds.
    fn cpu_split(&self) -> (f64, f64) {
        let Ok(stat) =
            std::fs::read_to_string(format!("/proc/{}/stat", self.pid))
        else {
            return (0.0, 0.0);
        };
        // Fields after the parenthesized command name.
        let rest = stat.rsplit_once(')').map(|(_, r)| r).unwrap_or("");
        let f: Vec<&str> = rest.split_whitespace().collect();
        let ticks = |i: usize| {
            f.get(i).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0)
        };
        // utime = field 14, stime = 15 (1-based); `rest` starts at field 3.
        (ticks(11) / 100.0, ticks(12) / 100.0)
    }

    fn rss_kib(&self) -> u64 {
        std::fs::read_to_string(format!("/proc/{}/status", self.pid))
            .ok()
            .and_then(|s| {
                s.lines()
                    .find_map(|l| l.strip_prefix("VmRSS:"))
                    .and_then(|v| {
                        v.trim().trim_end_matches("kB").trim().parse().ok()
                    })
            })
            .unwrap_or(0)
    }

    /// Stop with SIGTERM; jemalloc's total allocation count from its exit
    /// statistics.
    async fn stop(mut self) -> Option<u64> {
        std::process::Command::new("kill")
            .args(["-TERM", &self.pid.to_string()])
            .output()
            .ok();
        if tokio::time::timeout(Duration::from_secs(30), self.child.wait())
            .await
            .is_err()
        {
            // The runner itself too, when it runs under a wrapper.
            std::process::Command::new("kill")
                .args(["-KILL", &self.pid.to_string()])
                .output()
                .ok();
            self.child.kill().await.ok();
            return None;
        }
        alloc_requests(&std::fs::read_to_string(&self.stderr).ok()?)
    }
}

/// The merged arenas' total allocation requests (`nrequests`) from
/// jemalloc's `stats_print`. `nmalloc` counts only the allocations that
/// reached an arena (thread-cache fills), not every request.
fn alloc_requests(stats: &str) -> Option<u64> {
    let merged = &stats[stats.find("Merged arenas stats")?..];
    let header = merged.lines().find(|l| l.contains("nrequests"))?;
    // The header's columns, in the order of the `total:` line's values.
    let column = header.split_whitespace().position(|c| c == "nrequests")?;
    merged
        .lines()
        .find(|l| l.trim_start().starts_with("total:"))
        .and_then(|l| l.split_whitespace().nth(column + 1))
        .and_then(|v| v.parse().ok())
}

async fn api(
    method: reqwest::Method,
    path: &str,
    body: Option<&Value>,
) -> Result<Value> {
    let c = reqwest::Client::new();
    let mut req = c.request(method, format!("http://{API}{path}"));
    if let Some(b) = body {
        req = req.json(b);
    }
    let resp = req.send().await?;
    let status = resp.status();
    let text = resp.text().await?;
    if !status.is_success() {
        bail!("{path}: {status}: {text}");
    }
    Ok(serde_json::from_str(&text).unwrap_or(Value::Null))
}

/// DeltaForge's delivery counters: batches delivered, the events in them,
/// checkpoints committed and Kafka requests sent (librdkafka's broker
/// statistics, refreshed every 5 s).
async fn delivery_counters() -> Option<[f64; 4]> {
    let text = reqwest::get(format!("http://{METRICS}/metrics"))
        .await
        .ok()?
        .text()
        .await
        .ok()?;
    let sum = |name: &str| -> f64 {
        text.lines()
            .filter(|l| {
                l.strip_prefix(name).is_some_and(|rest| {
                    rest.starts_with('{') || rest.starts_with(' ')
                })
            })
            .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
            .sum()
    };
    Some([
        sum("deltaforge_batch_events_count"),
        sum("deltaforge_batch_events_sum"),
        sum("deltaforge_checkpoints_total"),
        sum("deltaforge_kafka_requests_total"),
    ])
}

async fn wait_status(name: &str, want: &str) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let v = api(reqwest::Method::GET, &format!("/pipelines/{name}"), None)
            .await?;
        let st = v["status"].as_str().unwrap_or_default().to_string();
        if st == want {
            return Ok(());
        }
        if st == "failed" {
            bail!("{name} failed: {v}");
        }
        if Instant::now() > deadline {
            bail!("{name} never reached {want} (status {st})");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

// ---------------------------------------------------------------------------
// Sinks: counting what arrives
// ---------------------------------------------------------------------------

/// Arrivals at a Kafka topic: count, bytes and each arrival instant.
#[derive(Default)]
struct Arrivals {
    count: AtomicU64,
    bytes: AtomicU64,
    times: std::sync::Mutex<Vec<Instant>>,
    /// The row ids seen (`after.id`, or `before.id` for a delete), and how
    /// many messages repeated one: delivery is counted per message, so
    /// distinct ids show a missing or duplicated row.
    ids: std::sync::Mutex<std::collections::HashSet<i64>>,
    duplicates: AtomicU64,
}

/// The row id of a JSON change event (a number, or a decimal string for a
/// 64-bit integer rendered exactly).
fn row_id(payload: &[u8]) -> Option<i64> {
    let v: Value = serde_json::from_slice(payload).ok()?;
    let image = if v["after"].is_object() {
        &v["after"]
    } else {
        &v["before"]
    };
    match &image["id"] {
        Value::Number(n) => n.as_i64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

/// Kafka's bootstrap address on the harness network.
fn bootstrap_internal() -> String {
    format!("{KAFKA_NAME}:{KAFKA_INTERNAL_PORT}")
}

fn brokers() -> String {
    format!("localhost:{KAFKA_HOST_PORT}")
}

async fn create_topic(topic: &str) -> Result<()> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .create()?;
    admin
        .create_topics(
            &[NewTopic::new(topic, 1, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await?;
    Ok(())
}

fn consume(
    topic: &str,
    arrivals: Arc<Arrivals>,
) -> Result<tokio::task::JoinHandle<()>> {
    let c: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .set("group.id", format!("perf-{topic}"))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("fetch.wait.max.ms", "10")
        .create()?;
    c.subscribe(&[topic])?;
    Ok(tokio::spawn(async move {
        loop {
            match c.recv().await {
                Ok(m) => {
                    let now = Instant::now();
                    let n = m.payload().map_or(0, <[u8]>::len)
                        + m.key().map_or(0, <[u8]>::len);
                    arrivals.bytes.fetch_add(n as u64, Ordering::Relaxed);
                    if let Some(id) = m.payload().and_then(row_id)
                        && !arrivals.ids.lock().unwrap().insert(id)
                    {
                        arrivals.duplicates.fetch_add(1, Ordering::Relaxed);
                    }
                    arrivals.times.lock().unwrap().push(now);
                    arrivals.count.fetch_add(1, Ordering::Release);
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
    }))
}

/// Rows and bytes of the Parquet data objects under `prefix`.
async fn parquet_rows(
    store: &Arc<dyn object_store::ObjectStore>,
    prefix: &str,
    seen: &mut BTreeMap<String, (u64, u64)>,
) -> Result<(u64, u64)> {
    use futures::TryStreamExt;
    let listed: Vec<object_store::ObjectMeta> = store
        .list(Some(&object_store::path::Path::from(prefix)))
        .try_collect()
        .await
        .unwrap_or_default();
    for o in listed {
        let key = o.location.to_string();
        if !key.ends_with(".parquet") || seen.contains_key(&key) {
            continue;
        }
        let reader = parquet::arrow::async_reader::ParquetObjectReader::new(
            Arc::clone(store),
            o.location.clone(),
        )
        .with_file_size(o.size);
        let Ok(builder) =
            parquet::arrow::ParquetRecordBatchStreamBuilder::new(reader).await
        else {
            continue;
        };
        let rows = builder.metadata().file_metadata().num_rows() as u64;
        seen.insert(key, (rows, o.size));
    }
    Ok(seen
        .values()
        .fold((0, 0), |(r, b), (r2, b2)| (r + r2, b + b2)))
}

// ---------------------------------------------------------------------------
// A run
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn pipeline_spec(
    infra_dsn: &str,
    sr: &str,
    s3_endpoint: &str,
    cell: &Cell,
    name: &str,
    ns: &str,
    topic: &str,
    prefix: &str,
    sink_extra: Option<&Value>,
) -> Value {
    // A snapshot chunk that a batch cuts mid-chunk reaches the durable S3
    // sink without a boundary, which it refuses (a defect of both
    // versions): Parquet snapshots use chunks that divide the batch limit
    // (2000) and a 1 s timer (far longer than reading a 1000-row chunk, and
    // short enough not to hold barriers), so every batch ends at a chunk
    // boundary.
    let snapshot = if cell.mode != Mode::Snapshot {
        json!({"mode": "never"})
    } else if cell.sink == Sink::Parquet {
        json!({"mode": "initial", "chunk_size": 1000})
    } else {
        json!({"mode": "initial"})
    };
    let source = match cell.engine {
        Engine::Mysql => json!({
            "type": "mysql",
            "config": {
                "id": name,
                "dsn": format!("{infra_dsn}"),
                "tables": [format!("{ns}.t")],
                "snapshot": snapshot,
            }
        }),
        Engine::Postgres => json!({
            "type": "postgres",
            "config": {
                "id": name,
                "dsn": infra_dsn,
                "slot": name.replace('-', "_"),
                "publication": format!("pub_{ns}"),
                "tables": [format!("{ns}.t")],
                "snapshot": snapshot,
            }
        }),
    };
    let sink = match cell.sink {
        Sink::Json => json!({"type": "kafka", "config": {
            "id": "kafka", "brokers": brokers(), "topic": topic,
            "envelope": {"type": "native"}, "encoding": "json", "required": true}}),
        Sink::Avro => json!({"type": "kafka", "config": {
            "id": "kafka", "brokers": brokers(), "topic": topic,
            "envelope": {"type": "native"}, "required": true,
            "encoding": {"type": "avro", "schema_registry_url": sr}}}),
        Sink::Parquet => json!({"type": "s3", "config": {
            "id": "s3", "bucket": BUCKET, "prefix": prefix, "region": "us-east-1",
            "endpoint": s3_endpoint, "access_key_id": s3_test_server::ACCESS_KEY,
            "secret_access_key": s3_test_server::SECRET_KEY, "format": "parquet",
            "required": true}}),
    };
    let processors = if cell.js {
        json!([{"type": "javascript", "id": "js", "inline":
            "function processBatch(events) { return events.map(e => { if (e.after) { e.after.tagged = true; } return e; }); }"}])
    } else {
        json!([])
    };
    let mut sink = sink;
    if let Some(Value::Object(extra)) = sink_extra {
        for (k, v) in extra {
            sink["config"][k] = v.clone();
        }
    }
    let mut spec = json!({
        "source": source,
        "processors": processors,
        "sinks": [sink],
    });
    if let Some(b) = cell.batch {
        spec["batch"] = json!({"max_events": b});
    }
    if cell.mode == Mode::Snapshot && cell.sink == Sink::Parquet {
        spec["batch"] = json!({"max_ms": 1000});
    }
    if let Some(Value::Object(extra)) = DF_BATCH.get() {
        if !spec["batch"].is_object() {
            spec["batch"] = json!({});
        }
        for (k, v) in extra {
            spec["batch"][k] = v.clone();
        }
    }
    json!({
        "apiVersion": "deltaforge/v1",
        "kind": "Pipeline",
        "metadata": {"name": name, "tenant": "perf"},
        "spec": spec,
    })
}

/// Batch settings added to every DeltaForge pipeline (`--df-batch`).
static DF_BATCH: std::sync::OnceLock<Value> = std::sync::OnceLock::new();
/// `--df-sink` settings by system label.
static DF_SINK: std::sync::OnceLock<BTreeMap<String, Value>> =
    std::sync::OnceLock::new();

#[derive(Debug, Default, Clone)]
struct Measured {
    ok: bool,
    error: Option<String>,
    rows: u64,
    secs: f64,
    rows_per_sec: f64,
    cpu_secs: f64,
    /// The system (kernel) part of `cpu_secs`.
    sys_secs: f64,
    /// DeltaForge's batches, events in them, checkpoint commits and Kafka
    /// requests in the window (`--delivery-stats`).
    delivery: Option<[f64; 4]>,
    /// Distinct row ids that arrived (JSON sinks), and duplicated messages.
    distinct_rows: Option<u64>,
    duplicates: Option<u64>,
    /// Read and write system calls, the main thread's voluntary and
    /// involuntary context switches, and minor page faults in the window.
    kernel: [u64; 5],
    rss_peak_mib: f64,
    db_cpu_secs: Option<f64>,
    out_bytes: u64,
    allocs: Option<u64>,
    lat_p50_ms: Option<f64>,
    lat_p95_ms: Option<f64>,
    lat_p99_ms: Option<f64>,
    lat_max_ms: Option<f64>,
    /// State-store key-value reads in the measured window (PostgreSQL state
    /// store only).
    kv_reads: Option<u64>,
    /// `deltaforge_wal_feedback_reads_total` in the window (when exported).
    feedback_reads: Option<u64>,
    feedback_staleness_s: Option<f64>,
    /// The process that ran the pipeline (DeltaForge), and the executable
    /// it ran with its SHA-256 (DeltaForge), or the worker image (Debezium).
    pid: Option<u32>,
    binary: Option<String>,
    binary_sha256: Option<String>,
    /// Per thread group over the window (see `threads`).
    threads: Option<Value>,
    /// An instrumented runner's own attribution (its `ATTR {json}` line).
    attribution: Option<Value>,
}

impl Measured {
    fn json(&self) -> Value {
        json!({
            "ok": self.ok, "error": self.error, "rows": self.rows, "secs": self.secs,
            "rows_per_sec": self.rows_per_sec, "cpu_secs": self.cpu_secs,
            "sys_secs": self.sys_secs,
            "distinct_rows": self.distinct_rows, "duplicates": self.duplicates,
            "batches": self.delivery.map(|d| d[0]),
            "batch_events": self.delivery.map(|d| d[1]),
            "checkpoints": self.delivery.map(|d| d[2]),
            "kafka_requests": self.delivery.map(|d| d[3]), "syscr": self.kernel[0],
            "syscw": self.kernel[1], "vcsw": self.kernel[2],
            "ivcsw": self.kernel[3], "minflt": self.kernel[4],
            "rss_peak_mib": self.rss_peak_mib, "db_cpu_secs": self.db_cpu_secs,
            "out_bytes": self.out_bytes, "allocs": self.allocs,
            "lat_p50_ms": self.lat_p50_ms, "lat_p95_ms": self.lat_p95_ms,
            "lat_p99_ms": self.lat_p99_ms, "lat_max_ms": self.lat_max_ms,
            "kv_reads": self.kv_reads, "feedback_reads": self.feedback_reads,
            "feedback_staleness_s": self.feedback_staleness_s,
            "pid": self.pid, "binary": self.binary,
            "binary_sha256": self.binary_sha256,
            "threads": self.threads, "attribution": self.attribution,
            // Rates over the window (the delivery counters' window adds
            // only an idle tail).
            "cpu_cores": (self.secs > 0.0).then(|| self.cpu_secs / self.secs),
            "batches_per_sec": self.delivery.map(|d| d[0] / self.secs),
            "checkpoints_per_sec": self.delivery.map(|d| d[2] / self.secs),
            "kafka_requests_per_sec": self.delivery.map(|d| d[3] / self.secs),
        })
    }
}

fn parse_lsn(text: &str) -> Option<u64> {
    let (hi, lo) = text.split_once('/')?;
    Some(
        (u64::from_str_radix(hi, 16).ok()? << 32)
            | u64::from_str_radix(lo, 16).ok()?,
    )
}

/// `deltaforge_wal_feedback_reads_total` summed over its series; `None` when
/// the binary does not export it.
async fn feedback_reads() -> Option<u64> {
    let text = reqwest::get(format!("http://{METRICS}/metrics"))
        .await
        .ok()?
        .text()
        .await
        .ok()?;
    let mut found = None;
    for line in text.lines() {
        if line.starts_with("deltaforge_wal_feedback_reads_total")
            && let Some(v) = line.rsplit(' ').next()
            && let Ok(v) = v.parse::<f64>()
        {
            *found.get_or_insert(0u64) += v as u64;
        }
    }
    found
}

#[allow(clippy::too_many_arguments)]
async fn run_once(
    infra: &Infra,
    store: &Arc<dyn object_store::ObjectStore>,
    system: &System,
    connect: Option<&debezium::Connect>,
    idle_allocs: Option<u64>,
    cell: &Cell,
    rows: usize,
    tag: &str,
    dir: &Path,
    args_probe: bool,
    args_delivery: bool,
    sink_extra: Option<&Value>,
) -> Measured {
    match run_inner(
        infra,
        store,
        system,
        connect,
        idle_allocs,
        cell,
        rows,
        tag,
        dir,
        args_probe,
        args_delivery,
        sink_extra,
    )
    .await
    {
        Ok(m) => m,
        Err(e) => Measured {
            ok: false,
            error: Some(format!("{e:#}")),
            ..Default::default()
        },
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_inner(
    infra: &Infra,
    store: &Arc<dyn object_store::ObjectStore>,
    system: &System,
    connect: Option<&debezium::Connect>,
    idle_allocs: Option<u64>,
    cell: &Cell,
    rows: usize,
    tag: &str,
    dir: &Path,
    args_probe: bool,
    args_delivery: bool,
    sink_extra: Option<&Value>,
) -> Result<Measured> {
    let engine = cell.engine;
    let ns = format!("p_{}", tag.replace('-', "_"));
    let name = format!("p-{tag}");
    let dbz = matches!(system, System::Debezium(_));
    let profile = match system {
        System::Debezium(p) => *p,
        System::DeltaForge(_) => debezium::Profile::Defaults,
    };
    if dbz && (engine != Engine::Mysql || cell.sink == Sink::Parquet) {
        bail!("not measured for Debezium: only MySQL to Kafka cells");
    }
    let topic = if dbz {
        debezium::topic(&format!("perf-{tag}"), &ns)
    } else {
        format!("perf-{tag}")
    };
    let prefix = format!("perf/{tag}");

    // Fixture.
    let mut setup = match engine {
        Engine::Mysql => vec![
            format!("DROP DATABASE IF EXISTS {ns}"),
            format!("CREATE DATABASE {ns}"),
        ],
        Engine::Postgres => vec![format!("CREATE SCHEMA {ns}")],
    };
    setup.push(create_table(engine, &ns, cell.shape));
    if engine == Engine::Postgres {
        setup.push(format!("ALTER TABLE {ns}.t REPLICA IDENTITY FULL"));
        setup.push(format!("CREATE PUBLICATION pub_{ns} FOR TABLE {ns}.t"));
    }
    infra.exec(engine, &setup).await?;
    if engine == Engine::Postgres {
        let (c, conn) = tokio_postgres::connect(
            &infra.pg_dsn().await,
            tokio_postgres::NoTls,
        )
        .await?;
        tokio::spawn(conn);
        sources::postgres::postgres_publication::register(
            &c,
            &[format!("pub_{ns}")],
        )
        .await
        .context("register publication")?;
    }
    if cell.mode == Mode::Snapshot {
        infra
            .exec(engine, &inserts(engine, &ns, cell.shape, 0, rows))
            .await?;
    }

    // Sink observation.
    let arrivals = Arc::new(Arrivals::default());
    let consumer = if cell.sink == Sink::Parquet {
        None
    } else {
        create_topic(&topic).await?;
        Some(consume(&topic, Arc::clone(&arrivals))?)
    };

    let dsn = match engine {
        Engine::Mysql => infra.mysql_dsn().await,
        Engine::Postgres => infra.pg_dsn().await,
    };
    let spec = pipeline_spec(
        &dsn,
        &infra.sr_url().await,
        &infra.s3.endpoint,
        cell,
        &name,
        &ns,
        &topic,
        &prefix,
        sink_extra,
    );

    let state_db = format!("st_{ns}");
    let state_dsn = if infra.state.is_some() {
        infra
            .state_exec("postgres", &format!("CREATE DATABASE {state_db}"))
            .await?;
        infra.state_dsn(&state_db).await
    } else {
        None
    };
    let slot = name.replace('-', "_");
    let process = match system {
        System::DeltaForge(binary) => Sut::Df(Box::new(
            Process::start(binary, dir, state_dsn.as_deref()).await?,
        )),
        System::Debezium(_) => {
            Sut::Dbz(connect.ok_or_else(|| anyhow!("no Kafka Connect worker"))?)
        }
    };
    // A MySQL server id of its own per Debezium run (the fixture is 60).
    static SERVER_IDS: std::sync::atomic::AtomicU32 =
        std::sync::atomic::AtomicU32::new(10_000);
    let connector = dbz.then(|| {
        debezium::mysql_connector(
            &name,
            tag,
            MYSQL_NAME,
            PW,
            &bootstrap_internal(),
            SERVER_IDS.fetch_add(1, Ordering::Relaxed),
            &ns,
            &format!("perf-{tag}"),
            cell.mode == Mode::Snapshot,
            match cell.sink {
                Sink::Avro => debezium::Format::Avro(format!(
                    "http://{SCHEMA_REGISTRY_NAME}:8081"
                )),
                _ => debezium::Format::Json,
            },
            cell.js,
            profile,
        )
    });
    if let Some(c) = &connector {
        std::fs::create_dir_all(dir).ok();
        std::fs::write(
            dir.join("connector.json"),
            serde_json::to_string_pretty(c).unwrap_or_default(),
        )
        .ok();
    }
    // Create, pause, resume, check and delete the pipeline (a DeltaForge
    // pipeline, or the Debezium connector).
    let create = || async {
        match &process {
            Sut::Df(_) => api(reqwest::Method::POST, "/pipelines", Some(&spec))
                .await
                .map(|_| ()),
            Sut::Dbz(c) => {
                c.create(connector.as_ref().expect("a Debezium run")).await
            }
        }
    };
    let control = |action: &'static str| {
        let name = name.clone();
        let process = &process;
        async move {
            match process {
                Sut::Df(_) => api(
                    reqwest::Method::POST,
                    &format!("/pipelines/{name}/{action}"),
                    None,
                )
                .await
                .map(|_| ()),
                Sut::Dbz(c) if action == "pause" => c.pause(&name).await,
                Sut::Dbz(c) => c.resume(&name).await,
            }
        }
    };
    let mut parquet_seen = BTreeMap::new();
    let delivered = |seen: &mut BTreeMap<String, (u64, u64)>| {
        let arrivals = Arc::clone(&arrivals);
        let prefix = prefix.clone();
        let store = Arc::clone(store);
        let sink = cell.sink;
        let mut local = std::mem::take(seen);
        async move {
            let r = if sink == Sink::Parquet {
                parquet_rows(&store, &prefix, &mut local).await
            } else {
                Ok((
                    arrivals.count.load(Ordering::Acquire),
                    arrivals.bytes.load(Ordering::Relaxed),
                ))
            };
            (r, local)
        }
    };

    let result = async {
        // Marker rows (CDC and latency) bring streaming up before the
        // measurement; ids are above the fixture's.
        let mut markers = 0u64;
        // Rows already delivered before the measured ones.
        let mut base = 0u64;
        let t0;
        let mut commits: Vec<Instant> = Vec::new();
        // The WAL position just before the last measured transaction: the
        // feedback covers that transaction once confirmed_flush passes it.
        let mut last_tx_lsn: Option<u64> = None;
        let window_start = || async {
            // Debezium's counters first: their reader runs in the worker's
            // cgroup, before its CPU is read.
            let dbz = match (&process, args_delivery) {
                (Sut::Dbz(c), true) => c
                    .counters()
                    .map(|[req, commits]| [f64::NAN, f64::NAN, commits, req]),
                _ => None,
            };
            (
                (
                    process.cpu_secs(),
                    process.sys_secs(),
                    process.kernel_counts(),
                ),
                infra.db_cpu_usec(engine),
                infra.state_reads(&state_db).await,
                feedback_reads().await,
                match (&process, args_delivery) {
                    (Sut::Df(_), true) => delivery_counters().await,
                    _ => dbz,
                },
            )
        };
        let (cpu0, db0, reads0, fb0, dc0);
        // The process tree whose threads are attributed.
        let root_pid = match &process {
            Sut::Df(p) => Some(p.pid),
            Sut::Dbz(c) => c.pid(),
        };
        let threads0;
        if cell.mode == Mode::Snapshot {
            (cpu0, db0, reads0, fb0, dc0) = window_start().await;
            threads0 = root_pid.map(threads::Snapshot::take);
            t0 = Instant::now();
            create().await?;
        } else {
            create().await?;
            match &process {
                Sut::Df(_) => wait_status(&name, "running").await?,
                Sut::Dbz(c) => c.wait(&name, "RUNNING").await?,
            }
            let deadline = Instant::now() + Duration::from_secs(120);
            loop {
                markers += 1;
                let marker = rows as u64 + markers;
                let one = inserts(
                    engine,
                    &ns,
                    cell.shape,
                    marker as usize - 1,
                    marker as usize,
                );
                infra.exec(engine, &one).await?;
                let wait = Instant::now() + Duration::from_secs(5);
                let mut seen = false;
                while Instant::now() < wait {
                    let (r, local) = delivered(&mut parquet_seen).await;
                    parquet_seen = local;
                    if r?.0 >= 1 {
                        seen = true;
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                if seen {
                    break;
                }
                if Instant::now() > deadline {
                    bail!("streaming never started");
                }
            }
            // A marker written before the source started streaming is never
            // delivered: the baseline is what has arrived once the source is
            // quiet, not the number of markers written.
            if cell.mode == Mode::Cdc {
                control("pause").await?;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
            let (r, local) = delivered(&mut parquet_seen).await;
            parquet_seen = local;
            base = r?.0;
            if cell.mode == Mode::Cdc {
                let mut stmts = inserts(engine, &ns, cell.shape, 0, rows);
                let last = stmts.pop().expect("at least one insert");
                infra.exec(engine, &stmts).await?;
                if engine == Engine::Postgres {
                    last_tx_lsn = infra.wal_insert_lsn().await;
                }
                infra.exec(engine, &[last]).await?;
                (cpu0, db0, reads0, fb0, dc0) = window_start().await;
                threads0 = root_pid.map(threads::Snapshot::take);
                t0 = Instant::now();
                control("resume").await?;
            } else {
                // Latency: transactions of `tx_rows` rows at `rate` per
                // second, on one connection (in bursts with idle between
                // them, for a bursty load).
                let load = cell.load;
                let mut sql = Sql::open(infra, engine).await?;
                (cpu0, db0, reads0, fb0, dc0) = window_start().await;
                threads0 = root_pid.map(threads::Snapshot::take);
                t0 = Instant::now();
                let mut tick = tokio::time::interval(Duration::from_micros(
                    1_000_000 / u64::from(load.rate.max(1)),
                ));
                tick.set_missed_tick_behavior(
                    tokio::time::MissedTickBehavior::Delay,
                );
                let mut row = 0;
                let mut txs = 0;
                while row < rows {
                    if let Some((n, idle)) = load.burst
                        && txs > 0
                        && txs % n == 0
                    {
                        tokio::time::sleep(Duration::from_millis(idle)).await;
                        tick.reset();
                    }
                    tick.tick().await;
                    let end = (row + load.tx_rows).min(rows);
                    if end == rows && engine == Engine::Postgres {
                        last_tx_lsn = infra.wal_insert_lsn().await;
                    }
                    let one = inserts(engine, &ns, cell.shape, row, end);
                    sql.exec(&one[0]).await?;
                    let at = Instant::now();
                    commits.extend(std::iter::repeat_n(at, end - row));
                    row = end;
                    txs += 1;
                }
            }
        }
        let want = rows as u64 + base;
        let mut rss_peak = 0u64;
        let deadline = Instant::now() + Duration::from_secs(1800);
        // A run that stops delivering fails rather than waiting out the
        // deadline (e.g. a worker out of memory).
        let (mut last_got, mut last_progress) = (0u64, Instant::now());
        let (got, bytes) = loop {
            rss_peak = rss_peak.max(process.rss_kib());
            let (r, local) = delivered(&mut parquet_seen).await;
            parquet_seen = local;
            let (got, bytes) = r?;
            if got >= want {
                break (got, bytes);
            }
            if got > last_got {
                (last_got, last_progress) = (got, Instant::now());
            } else if last_progress.elapsed() > Duration::from_secs(120) {
                bail!("stalled: no row for 120 s ({got} of {want} arrived)");
            }
            match &process {
                Sut::Df(_) => {
                    if let Ok(v) = api(
                        reqwest::Method::GET,
                        &format!("/pipelines/{name}"),
                        None,
                    )
                    .await
                        && v["status"] == "failed"
                    {
                        bail!("pipeline failed: {}", v);
                    }
                }
                Sut::Dbz(c) => {
                    if let Some(why) = c.failure(&name).await {
                        bail!("connector failed: {why}");
                    }
                }
            }
            if Instant::now() > deadline {
                bail!("only {got} of {want} rows arrived");
            }
            tokio::time::sleep(Duration::from_millis(
                if cell.sink == Sink::Parquet { 250 } else { 20 },
            ))
            .await;
        };
        // The window ends at the last row's arrival (Kafka) or its listing.
        let t_end = if cell.sink == Sink::Parquet {
            Instant::now()
        } else {
            let times = arrivals.times.lock().unwrap();
            times
                .get(want as usize - 1)
                .copied()
                .unwrap_or_else(Instant::now)
        };
        let secs = t_end.duration_since(t0).as_secs_f64();
        let cpu = process.cpu_secs() - cpu0.0;
        let sys = process.sys_secs() - cpu0.1;
        let threads = match (root_pid, &threads0) {
            (Some(pid), Some(t0)) => {
                Some(threads::Snapshot::take(pid).since(t0))
            }
            _ => None,
        };
        let k1 = process.kernel_counts();
        let kernel: [u64; 5] =
            std::array::from_fn(|i| k1[i].saturating_sub(cpu0.2[i]));
        let kv_reads = match (reads0, infra.state_reads(&state_db).await) {
            (Some(a), Some(b)) => Some(b.saturating_sub(a)),
            _ => None,
        };
        let fb_reads = match (fb0, feedback_reads().await) {
            (Some(a), Some(b)) => Some(b.saturating_sub(a)),
            _ => None,
        };
        // WAL-feedback staleness: from the last row's arrival until the
        // slot's confirmed_flush_lsn passes the WAL position just before the
        // last transaction (so the feedback covers it). 40 s is reported
        // when it never does within that bound.
        let staleness = match (args_probe, last_tx_lsn) {
            (true, Some(target)) => {
                let stop = Instant::now() + Duration::from_secs(40);
                let mut at = None;
                while Instant::now() < stop {
                    if infra
                        .confirmed_flush(&slot)
                        .await
                        .is_some_and(|l| l > target)
                    {
                        at = Some(
                            Instant::now()
                                .saturating_duration_since(t_end)
                                .as_secs_f64(),
                        );
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Some(at.unwrap_or(40.0))
            }
            _ => None,
        };
        // librdkafka refreshes its statistics every 5 s. Debezium's are
        // read after its CPU (above). Debezium has no batch counters (NaN).
        let delivery = match (dc0, &process) {
            (Some(a), Sut::Df(_)) => {
                tokio::time::sleep(Duration::from_millis(5_500)).await;
                delivery_counters()
                    .await
                    .map(|b| std::array::from_fn(|i| b[i] - a[i]))
            }
            (Some(a), Sut::Dbz(c)) => c.counters().map(|[req, commits]| {
                [f64::NAN, f64::NAN, commits - a[2], req - a[3]]
            }),
            _ => None,
        };
        let db = match (db0, infra.db_cpu_usec(engine)) {
            (Some(a), Some(b)) => Some((b - a) as f64 / 1e6),
            _ => None,
        };
        let (p50, p95, p99, pmax) = if cell.mode == Mode::Latency {
            let times = arrivals.times.lock().unwrap();
            let mut lat: Vec<f64> = commits
                .iter()
                .enumerate()
                .filter_map(|(i, c)| {
                    times.get(base as usize + i).map(|a| {
                        a.saturating_duration_since(*c).as_secs_f64() * 1e3
                    })
                })
                .collect();
            lat.sort_by(f64::total_cmp);
            let q = |p: f64| {
                lat.get(((lat.len() as f64 - 1.0) * p) as usize).copied()
            };
            (q(0.5), q(0.95), q(0.99), lat.last().copied())
        } else {
            (None, None, None, None)
        };
        Ok::<_, anyhow::Error>(Measured {
            ok: true,
            error: None,
            rows: got - base,
            secs,
            rows_per_sec: rows as f64 / secs,
            cpu_secs: cpu,
            sys_secs: sys,
            delivery,
            distinct_rows: (cell.sink == Sink::Json)
                .then(|| arrivals.ids.lock().unwrap().len() as u64),
            duplicates: (cell.sink == Sink::Json)
                .then(|| arrivals.duplicates.load(Ordering::Relaxed)),
            kernel,
            rss_peak_mib: rss_peak as f64 / 1024.0,
            db_cpu_secs: db,
            out_bytes: bytes,
            allocs: None,
            lat_p50_ms: p50,
            lat_p95_ms: p95,
            lat_p99_ms: p99,
            lat_max_ms: pmax,
            kv_reads,
            feedback_reads: fb_reads,
            feedback_staleness_s: staleness,
            pid: None,
            binary: None,
            binary_sha256: None,
            threads,
            attribution: None,
        })
    }
    .await;
    let identity = match &process {
        Sut::Df(p) => (
            Some(p.pid),
            Some(p.exe.display().to_string()),
            sha256(&p.exe),
        ),
        Sut::Dbz(_) => (
            None,
            Some(format!(
                "{}:{}",
                debezium::CONNECT_IMAGE.0,
                debezium::CONNECT_IMAGE.1
            )),
            None,
        ),
    };

    match &process {
        Sut::Df(_) => {
            api(reqwest::Method::DELETE, &format!("/pipelines/{name}"), None)
                .await
                .ok();
        }
        Sut::Dbz(c) => c.delete(&name).await,
    }
    let attr_log = match &process {
        Sut::Df(p) => Some(p.stderr.clone()),
        Sut::Dbz(_) => None,
    };
    let allocs = process.stop().await;
    let attribution = attr_log
        .and_then(|f| std::fs::read_to_string(f).ok())
        .and_then(|t| {
            t.lines()
                .rev()
                .find_map(|l| l.strip_prefix("ATTR "))
                .and_then(|j| serde_json::from_str::<Value>(j).ok())
        });
    if let Some(c) = consumer {
        c.abort();
    }
    if engine == Engine::Postgres {
        let slot = name.replace('-', "_");
        infra
            .exec(
                engine,
                &[format!(
                    "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots \
                     WHERE slot_name = '{slot}' AND NOT active"
                )],
            )
            .await
            .ok();
    }
    teardown(infra, store, engine, &ns, &topic, &prefix).await;
    if infra.state.is_some() {
        infra
            .state_exec(
                "postgres",
                &format!("DROP DATABASE IF EXISTS {state_db} WITH (FORCE)"),
            )
            .await
            .ok();
    }
    let mut m = result?;
    (m.pid, m.binary, m.binary_sha256) = identity;
    m.attribution = attribution;
    m.allocs = match (allocs, idle_allocs) {
        (Some(a), Some(i)) => Some(a.saturating_sub(i)),
        (a, _) => a,
    };
    Ok(m)
}

/// Free a run's data: its topic, its S3 objects, its database or schema
/// (and its publication), and MySQL's binary logs. Best effort.
async fn teardown(
    infra: &Infra,
    store: &Arc<dyn object_store::ObjectStore>,
    engine: Engine,
    ns: &str,
    topic: &str,
    prefix: &str,
) {
    use futures::TryStreamExt;
    if let Ok(admin) = ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .create::<AdminClient<DefaultClientContext>>()
    {
        admin
            .delete_topics(&[topic], &AdminOptions::new())
            .await
            .ok();
    }
    let keys: Vec<object_store::path::Path> = store
        .list(Some(&object_store::path::Path::from(prefix)))
        .map_ok(|o| o.location)
        .try_collect()
        .await
        .unwrap_or_default();
    for k in keys {
        object_store::ObjectStoreExt::delete(store.as_ref(), &k)
            .await
            .ok();
    }
    match engine {
        Engine::Mysql => {
            infra
                .exec(
                    engine,
                    &[
                        format!("DROP DATABASE IF EXISTS {ns}"),
                        "PURGE BINARY LOGS BEFORE NOW()".to_string(),
                    ],
                )
                .await
                .ok();
        }
        Engine::Postgres => {
            if let Ok((c, conn)) = tokio_postgres::connect(
                &infra.pg_dsn().await,
                tokio_postgres::NoTls,
            )
            .await
            {
                tokio::spawn(conn);
                let publication = format!("pub_{ns}");
                sources::postgres::postgres_publication::unregister(
                    &c,
                    std::slice::from_ref(&publication),
                )
                .await
                .ok();
                c.batch_execute(&format!(
                    "DROP PUBLICATION IF EXISTS {publication}; DROP SCHEMA IF EXISTS {ns} CASCADE"
                ))
                .await
                .ok();
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn median(mut v: Vec<f64>) -> Option<f64> {
    if v.is_empty() {
        return None;
    }
    v.sort_by(f64::total_cmp);
    Some(v[v.len() / 2])
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let cells: Vec<Cell> = matrix()
        .into_iter()
        .filter(|c| {
            args.filter.is_empty()
                || args.filter.iter().any(|f| match f.strip_prefix('=') {
                    Some(exact) => c.id() == exact,
                    None => c.id().contains(f.as_str()),
                })
        })
        .collect();
    if args.list {
        for c in &cells {
            println!("{} rows={}", c.id(), c.rows);
        }
        return Ok(());
    }
    // One invocation at a time on this machine: two would share the named
    // containers and the runner's ports. Held until exit.
    let lock = std::fs::File::create(
        std::env::temp_dir().join("canonical-perf.lock"),
    )?;
    if lock.try_lock().is_err() {
        bail!("another canonical-perf invocation is running on this machine");
    }
    let profile = debezium::Profile::parse(&args.debezium_profile)?;
    if let Some(b) = &args.df_batch {
        let v: Value = serde_json::from_str(b).context("--df-batch")?;
        if !v.is_object() {
            bail!("--df-batch must be a JSON object");
        }
        DF_BATCH.set(v).ok();
    }
    let mut df_sink = BTreeMap::new();
    for item in &args.df_sink {
        let (label, json) = item
            .split_once('=')
            .ok_or_else(|| anyhow!("--df-sink must be LABEL=JSON"))?;
        let v: Value = serde_json::from_str(json).context("--df-sink")?;
        if !v.is_object() {
            bail!("--df-sink must be LABEL=<a JSON object>");
        }
        df_sink.insert(label.to_string(), v);
    }
    DF_SINK.set(df_sink).ok();
    let binaries: Vec<(String, System)> = args
        .binaries
        .iter()
        .map(|b| {
            let (l, p) = b
                .split_once('=')
                .ok_or_else(|| anyhow!("--binary label=path"))?;
            let system = if p == "debezium" {
                System::Debezium(profile)
            } else {
                System::DeltaForge(PathBuf::from(p))
            };
            Ok((l.to_string(), system))
        })
        .collect::<Result<_>>()?;
    if binaries.len() != 2 {
        bail!("exactly two binaries");
    }
    std::fs::create_dir_all(&args.out)?;
    let infra = Infra::start(args.state_store == "postgres").await?;
    let outcome = measure(&args, &binaries, &infra).await;
    infra.cleanup();
    outcome
}

async fn measure(
    args: &Args,
    binaries: &[(String, System)],
    infra: &Infra,
) -> Result<()> {
    let cells: Vec<Cell> = matrix()
        .into_iter()
        .filter(|c| {
            args.filter.is_empty()
                || args.filter.iter().any(|f| match f.strip_prefix('=') {
                    Some(exact) => c.id() == exact,
                    None => c.id().contains(f.as_str()),
                })
        })
        .collect();
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(
        object_store::aws::AmazonS3Builder::new()
            .with_bucket_name(BUCKET)
            .with_region("us-east-1")
            .with_endpoint(&infra.s3.endpoint)
            .with_access_key_id(s3_test_server::ACCESS_KEY)
            .with_secret_access_key(s3_test_server::SECRET_KEY)
            .with_allow_http(true)
            .build()?,
    );

    // Idle allocation baselines.
    let mut idle = BTreeMap::new();
    for (label, system) in binaries {
        let System::DeltaForge(bin) = system else {
            idle.insert(label.clone(), None);
            continue;
        };
        let dir = args.out.join(format!("idle-{label}"));
        let p = Process::start(bin, &dir, None).await?;
        tokio::time::sleep(Duration::from_secs(3)).await;
        idle.insert(label.clone(), p.stop().await);
    }

    // One Kafka Connect worker for the invocation, warmed up: each run
    // registers a connector of its own on it.
    let connect = match binaries.iter().find_map(|(_, s)| match s {
        System::Debezium(p) => Some(*p),
        _ => None,
    }) {
        Some(profile) => Some(
            debezium::Connect::start(
                NETWORK,
                &bootstrap_internal(),
                "shared",
                profile,
                &extras::prepare(&args.cache)?,
            )
            .await?,
        ),
        None => None,
    };
    // Warm-up: one unrecorded run of every cell for every system, so a JIT
    // compiled system is measured warm.
    if args.warmup {
        for cell in &cells {
            for (label, system) in binaries {
                let rows = ((cell.rows as f64 * args.scale) as usize).max(100);
                let tag = format!("{}-{}-warmup", cell.id(), label);
                let m = run_once(
                    infra,
                    &store,
                    system,
                    connect.as_ref(),
                    idle[label],
                    cell,
                    rows,
                    &tag,
                    &args.out.join("warmup").join(&tag),
                    false,
                    false,
                    DF_SINK.get().and_then(|m| m.get(label.as_str())),
                )
                .await;
                eprintln!(
                    "{} {label} warm-up: {}",
                    cell.id(),
                    if m.ok {
                        format!("{:.0} rows/s", m.rows_per_sec)
                    } else {
                        format!("FAILED {}", m.error.unwrap_or_default())
                    }
                );
            }
        }
    }

    let mut log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(args.out.join("runs.jsonl"))?;
    let mut results: BTreeMap<(String, String), Vec<Measured>> =
        BTreeMap::new();
    let mut seq = 0usize;
    for rep in 0..args.reps {
        for cell in &cells {
            let order: Vec<&(String, System)> = if rep % 2 == 0 {
                binaries.iter().collect()
            } else {
                binaries.iter().rev().collect()
            };
            for (label, bin) in order {
                seq += 1;
                let rows = ((cell.rows as f64 * args.scale) as usize).max(100);
                let tag = format!("{}-{}-r{rep}-{seq}", cell.id(), label);
                let dir = args.out.join("runs").join(&tag);
                let m = run_once(
                    infra,
                    &store,
                    bin,
                    connect.as_ref(),
                    idle[label],
                    cell,
                    rows,
                    &tag,
                    &dir,
                    args.feedback_probe,
                    args.delivery_stats,
                    DF_SINK.get().and_then(|m| m.get(label.as_str())),
                )
                .await;
                if let (false, System::Debezium(_), Some(c)) =
                    (m.ok, bin, &connect)
                    && !c.alive().await
                {
                    eprintln!(
                        "the Kafka Connect worker no longer answers: killed"
                    );
                    c.kill();
                }
                let line = json!({"cell": cell.id(), "version": label, "rep": rep, "rows_target": rows, "result": m.json()});
                use std::io::Write;
                writeln!(log, "{line}")?;
                eprintln!(
                    "{} {label} rep {rep}: {}",
                    cell.id(),
                    if m.ok {
                        format!(
                            "{:.0} rows/s, cpu {:.1}s, rss {:.0} MiB",
                            m.rows_per_sec, m.cpu_secs, m.rss_peak_mib
                        )
                    } else {
                        format!(
                            "FAILED {}",
                            m.error.clone().unwrap_or_default()
                        )
                    }
                );
                results
                    .entry((cell.id(), label.clone()))
                    .or_default()
                    .push(m);
                // Free the run's directory but its logs.
                std::fs::remove_file(dir.join("state.db")).ok();
            }
        }
    }

    // Summary: medians per cell and version, and B/A ratios.
    let (a, b) = (&binaries[0].0, &binaries[1].0);
    let mut md = format!(
        "| cell | {a} rows/s | {b} rows/s | {b}/{a} | cpu s/100k {a} | {b} | rss MiB {a} | {b} | allocs/row {a} | {b} | bytes/row {a} | {b} | db cpu s {a} | {b} | p50/p99 ms {a} | {b} | failures | sys s/100k {a} | {b} |\n|{}\n",
        "---|".repeat(20)
    );
    for cell in &cells {
        let get = |l: &str| {
            results
                .get(&(cell.id(), l.to_string()))
                .cloned()
                .unwrap_or_default()
        };
        let (ra, rb) = (get(a), get(b));
        let ok = |v: &Vec<Measured>| {
            v.iter().filter(|m| m.ok).cloned().collect::<Vec<_>>()
        };
        let (oa, ob) = (ok(&ra), ok(&rb));
        let med = |v: &Vec<Measured>, f: &dyn Fn(&Measured) -> Option<f64>| {
            median(v.iter().filter_map(f).collect())
        };
        let thr = |v: &Vec<Measured>| med(v, &|m| Some(m.rows_per_sec));
        let cpu = |v: &Vec<Measured>| {
            med(v, &|m| Some(m.cpu_secs / m.rows as f64 * 1e5))
        };
        let sys = |v: &Vec<Measured>| {
            med(v, &|m| Some(m.sys_secs / m.rows as f64 * 1e5))
        };
        let rss = |v: &Vec<Measured>| med(v, &|m| Some(m.rss_peak_mib));
        let alloc = |v: &Vec<Measured>| {
            med(v, &|m| m.allocs.map(|x| x as f64 / m.rows as f64))
        };
        let bytes = |v: &Vec<Measured>| {
            med(v, &|m| Some(m.out_bytes as f64 / m.rows as f64))
        };
        let db = |v: &Vec<Measured>| med(v, &|m| m.db_cpu_secs);
        let lat = |v: &Vec<Measured>| match (
            med(v, &|m| m.lat_p50_ms),
            med(v, &|m| m.lat_p99_ms),
        ) {
            (Some(p), Some(q)) => format!("{p:.0}/{q:.0}"),
            _ => "-".into(),
        };
        let f = |x: Option<f64>, d: usize| {
            x.map(|x| format!("{x:.d$}")).unwrap_or("-".into())
        };
        let ratio = match (thr(&oa), thr(&ob)) {
            (Some(x), Some(y)) => format!("{:.2}", y / x),
            _ => "-".into(),
        };
        let failures = ra.len() - oa.len() + rb.len() - ob.len();
        md.push_str(&format!(
            "| {} | {} | {} | {ratio} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {failures} | {} | {} |\n",
            cell.id(),
            f(thr(&oa), 0), f(thr(&ob), 0),
            f(cpu(&oa), 2), f(cpu(&ob), 2),
            f(rss(&oa), 0), f(rss(&ob), 0),
            f(alloc(&oa), 1), f(alloc(&ob), 1),
            f(bytes(&oa), 0), f(bytes(&ob), 0),
            f(db(&oa), 1), f(db(&ob), 1),
            lat(&oa), lat(&ob),
            f(sys(&oa), 2), f(sys(&ob), 2),
        ));
    }
    std::fs::write(args.out.join("summary.md"), md)?;
    if let Some(c) = connect {
        c.stop(&args.out.join("connect.log")).await;
    }

    // WAL feedback: state-store reads per million rows and staleness.
    let mut fb = format!(
        "| cell | version | runs | rows/s | kv reads per 1M rows | feedback reads per 1M rows | staleness s (median, max) |\n|{}\n",
        "---|".repeat(7)
    );
    for cell in &cells {
        for (label, _) in binaries {
            let v: Vec<Measured> = results
                .get(&(cell.id(), label.clone()))
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter(|m| m.ok)
                .collect();
            let per_m = |x: Option<u64>, m: &Measured| {
                x.map(|x| x as f64 * 1e6 / m.rows as f64)
            };
            let f = |x: Option<f64>, d: usize| {
                x.map(|x| format!("{x:.d$}")).unwrap_or("-".into())
            };
            let stale: Vec<f64> =
                v.iter().filter_map(|m| m.feedback_staleness_s).collect();
            let max = stale.iter().copied().fold(None, |a: Option<f64>, x| {
                Some(a.map_or(x, |a| a.max(x)))
            });
            fb.push_str(&format!(
                "| {} | {label} | {} | {} | {} | {} | {}, {} |\n",
                cell.id(),
                v.len(),
                f(median(v.iter().map(|m| m.rows_per_sec).collect()), 0),
                f(
                    median(
                        v.iter().filter_map(|m| per_m(m.kv_reads, m)).collect()
                    ),
                    0
                ),
                f(
                    median(
                        v.iter()
                            .filter_map(|m| per_m(m.feedback_reads, m))
                            .collect()
                    ),
                    0
                ),
                f(median(stale.clone()), 2),
                f(max, 2),
            ));
        }
    }
    std::fs::write(args.out.join("feedback.md"), fb)?;
    eprintln!("summary: {}", args.out.join("summary.md").display());
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_row_id_is_read_from_either_image_as_a_number_or_exact_text() {
        assert_eq!(super::row_id(br#"{"after":{"id":7}}"#), Some(7));
        assert_eq!(
            super::row_id(br#"{"after":{"id":"9007199254740993"}}"#),
            Some(9007199254740993)
        );
        assert_eq!(
            super::row_id(br#"{"before":{"id":3},"after":null}"#),
            Some(3)
        );
        assert_eq!(super::row_id(b"not json"), None);
    }

    #[test]
    fn allocations_are_the_merged_request_count() {
        let stats = "Merged arenas stats:\n\
            assigned threads: 2\n\
            \x20                allocated  nmalloc (#/sec) ndalloc (#/sec) nrequests (#/sec)\n\
            small:              1191776  5110527  730075  5107728  729675  9767705  1395386\n\
            total:              1757024  5112312  730330  5109503  729929  9772547  1396078\n";
        assert_eq!(super::alloc_requests(stats), Some(9_772_547));
        assert_eq!(super::alloc_requests("no stats"), None);
    }
}
