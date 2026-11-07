//! PostgreSQL source qualification (release checklist G4,
//! `docs/ops/release-checklist.md`): one PostgreSQL source, one pipeline into
//! Kafka (native JSON), an initial snapshot of many tables, then paced CDC
//! with one stop and resume and one `ADD COLUMN`, tables with out-of-line
//! (TOAST) values under `REPLICA IDENTITY FULL` and `DEFAULT`, and the
//! pipeline's health watched throughout. The checks are in
//! [`crate::pgqual_check`]. Two deliberately unsupported cases run in pipelines
//! of their own and are reported separately: a sink that needs complete rows
//! with a table that is not `FULL` must be refused at start, and a table
//! changed away from `FULL` while running must stop the source before its next
//! row.
//!
//! The instance under test, its state store (SQLite for G4) and the
//! infrastructure are started outside the harness; the run records what it
//! was told.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use chrono::Utc;
use parking_lot::Mutex;
use rand::distr::{Alphanumeric, SampleString};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rdkafka::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::watch;
use tokio_postgres::{Client, NoTls};

use crate::deltaforge::{Api, merge};
use crate::evidence;
use crate::measure;
use crate::pgqual_check::{
    Big, Committed, Delivered, Expect, FinalRow, Fingerprint, Kind, Op, Report,
    evaluate,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PgQualConfig {
    /// Results go to `<output_dir>/g4/<run id>/`.
    pub output_dir: String,
    /// The instance's REST API, e.g. `http://127.0.0.1:8080`.
    pub api_url: String,
    /// The instance's Prometheus endpoint.
    pub metrics_url: String,
    /// The state store the instance was started with (recorded).
    pub state_store: String,
    /// Kafka as the instance reaches it (in the pipeline specs).
    pub kafka_brokers: String,
    /// Kafka as the harness reaches it.
    pub verifier_kafka_brokers: String,
    /// A superuser connection from the harness (fixture, registration,
    /// final reads).
    pub admin_dsn: String,
    /// The source DSN as the instance reaches the database (in the specs).
    pub cdc_dsn: String,
    #[serde(default = "default_prefix")]
    pub prefix: String,
    #[serde(default = "default_tenant")]
    pub tenant: String,
    pub tables: TablesCfg,
    pub cdc: CdcCfg,
    pub stop: StopCfg,
    pub add_column: AddColumnCfg,
    /// The deliberately unsupported cases; without them the run is
    /// incomplete (it fails).
    #[serde(default)]
    pub unsupported: Option<UnsupportedCfg>,
    #[serde(default = "default_poll_ms")]
    pub status_poll_ms: u64,
    #[serde(default = "default_snapshot_timeout")]
    pub snapshot_timeout_secs: u64,
    #[serde(default = "default_drain_idle")]
    pub drain_idle_secs: u64,
    #[serde(default = "default_drain_max")]
    pub drain_max_secs: u64,
    /// Merged into the main pipeline's `spec`.
    #[serde(default)]
    pub spec_overrides: Option<serde_yaml::Value>,
    #[serde(default)]
    pub seed: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TablesCfg {
    pub count: u32,
    /// The first `toast_full` tables hold out-of-line values under
    /// `REPLICA IDENTITY FULL`.
    pub toast_full: u32,
    /// The next `toast_default` tables hold them under `DEFAULT`.
    pub toast_default: u32,
    pub initial_rows: u32,
    /// Size of each out-of-line value (stored uncompressed, out of line).
    #[serde(default = "default_big_bytes")]
    pub big_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CdcCfg {
    pub duration_secs: u64,
    pub ops_per_sec: f64,
    pub writers: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StopCfg {
    /// Seconds into the CDC phase.
    pub at_secs: u64,
    pub for_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AddColumnCfg {
    /// Seconds into the CDC phase.
    pub at_secs: u64,
    /// The table (index) that receives `ADD COLUMN extra bigint`.
    pub table: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnsupportedCfg {
    /// The Avro sink (a sink that needs complete rows) needs a registry.
    pub schema_registry_url: String,
    #[serde(default = "default_refusal_timeout")]
    pub timeout_secs: u64,
}

fn default_prefix() -> String {
    "g4".into()
}
fn default_tenant() -> String {
    "g4".into()
}
fn default_poll_ms() -> u64 {
    500
}
fn default_snapshot_timeout() -> u64 {
    1800
}
fn default_drain_idle() -> u64 {
    30
}
fn default_drain_max() -> u64 {
    1800
}
fn default_big_bytes() -> usize {
    8192
}
fn default_refusal_timeout() -> u64 {
    120
}

impl PgQualConfig {
    pub fn validate(&self) -> Result<()> {
        let t = &self.tables;
        ensure!(t.count > 0, "tables.count must be positive");
        ensure!(
            t.toast_full + t.toast_default <= t.count,
            "tables.toast_full + tables.toast_default exceed tables.count"
        );
        ensure!(
            t.big_bytes >= 4096,
            "tables.big_bytes must be at least 4096 (stored out of line)"
        );
        ensure!(self.cdc.writers > 0, "cdc.writers must be positive");
        ensure!(
            self.cdc.ops_per_sec > 0.0,
            "cdc.ops_per_sec must be positive"
        );
        ensure!(
            self.stop.at_secs + self.stop.for_secs < self.cdc.duration_secs,
            "the stop must end before the CDC phase does"
        );
        ensure!(
            self.add_column.at_secs < self.cdc.duration_secs,
            "add_column.at_secs must fall inside the CDC phase"
        );
        ensure!(
            self.add_column.table < t.count,
            "add_column.table is not a table of the fixture"
        );
        Ok(())
    }

    pub fn kind(&self, table: u32) -> Kind {
        if table < self.tables.toast_full {
            Kind::ToastFull
        } else if table < self.tables.toast_full + self.tables.toast_default {
            Kind::ToastDefault
        } else {
            Kind::Plain
        }
    }
}

/// The run's names, all derived from its run tag.
#[derive(Debug, Clone, Serialize)]
pub struct Names {
    pub schema: String,
    pub publication: String,
    pub slot: String,
    pub pipeline: String,
    pub topic: String,
}

impl Names {
    pub fn new(prefix: &str, tag: u64) -> Names {
        Names {
            schema: format!("{prefix}_{tag}"),
            publication: format!("{prefix}pub_{tag}"),
            slot: format!("{prefix}slot_{tag}"),
            pipeline: format!("{prefix}-{tag}"),
            topic: format!("{prefix}.{tag}"),
        }
    }

    fn unsupported(&self, case: &str) -> Names {
        Names {
            schema: format!("{}_{case}", self.schema),
            publication: format!("{}_{case}", self.publication),
            slot: format!("{}_{case}", self.slot),
            pipeline: format!("{}-{case}", self.pipeline),
            topic: format!("{}.{case}", self.topic),
        }
    }
}

pub fn table_name(t: u32) -> String {
    format!("t{t:04}")
}

/// A pipeline spec: a PostgreSQL source over `names`' publication and
/// schema, and one Kafka sink (native JSON, or Avro through `registry`).
pub fn render_spec(
    cfg: &PgQualConfig,
    names: &Names,
    registry: Option<&str>,
    snapshot_mode: &str,
) -> Result<Value> {
    let mut sink = json!({
        "id": "kafka",
        "brokers": cfg.kafka_brokers,
        "topic": names.topic,
        "envelope": {"type": "native"},
        "required": true,
    });
    if let Some(url) = registry {
        sink["encoding"] = json!({"type": "avro", "schema_registry_url": url});
    }
    let mut spec = json!({
        "apiVersion": "deltaforge/v1",
        "kind": "Pipeline",
        "metadata": {"name": names.pipeline, "tenant": cfg.tenant},
        "spec": {
            "source": {"type": "postgres", "config": {
                "id": format!("src-{}", names.pipeline),
                "dsn": cfg.cdc_dsn,
                "publication": names.publication,
                "slot": names.slot,
                "tables": [format!("{}.*", names.schema)],
                "snapshot": {"mode": snapshot_mode},
            }},
            "processors": [],
            "sinks": [{"type": "kafka", "config": sink}],
        }
    });
    if registry.is_none()
        && let Some(o) = &cfg.spec_overrides
    {
        let o: Value = serde_json::to_value(o).context("spec_overrides")?;
        ensure!(o.is_object(), "spec_overrides must be a mapping");
        merge(&mut spec["spec"], &o);
    }
    Ok(spec)
}

async fn connect(dsn: &str) -> Result<Client> {
    let (client, conn) = tokio_postgres::connect(dsn, NoTls)
        .await
        .context("connect to PostgreSQL")?;
    tokio::spawn(conn);
    Ok(client)
}

fn big_value(rng: &mut StdRng, n: usize) -> String {
    Alphanumeric.sample_string(rng, n)
}

/// One table's rows as the writers know them.
#[derive(Debug, Default, Clone)]
struct TableState {
    live: Vec<i64>,
    next_id: i64,
    v: HashMap<i64, i64>,
    big: HashMap<i64, Option<String>>,
}

/// Create the schema, tables, rows and publication, and register it.
/// Returns the committed fixture rows and each table's state.
async fn fixture(
    admin: &Client,
    cfg: &PgQualConfig,
    names: &Names,
    rng: &mut StdRng,
) -> Result<(Vec<Committed>, BTreeMap<u32, TableState>)> {
    let s = &names.schema;
    admin
        .batch_execute(&format!("CREATE SCHEMA \"{s}\""))
        .await?;
    let mut committed = Vec::new();
    let mut states = BTreeMap::new();
    for t in 0..cfg.tables.count {
        let tn = table_name(t);
        let kind = cfg.kind(t);
        let mut ddl = format!(
            "CREATE TABLE \"{s}\".\"{tn}\" (id bigint PRIMARY KEY, v bigint NOT NULL, note text NOT NULL, big text);"
        );
        if kind != Kind::Plain {
            ddl += &format!(
                "ALTER TABLE \"{s}\".\"{tn}\" ALTER COLUMN big SET STORAGE EXTERNAL;"
            );
        }
        if kind == Kind::ToastFull {
            ddl +=
                &format!("ALTER TABLE \"{s}\".\"{tn}\" REPLICA IDENTITY FULL;");
        }
        admin.batch_execute(&ddl).await?;
        let mut st = TableState {
            next_id: cfg.tables.initial_rows as i64 + 1,
            ..TableState::default()
        };
        for id in 1..=cfg.tables.initial_rows as i64 {
            let big = (kind != Kind::Plain)
                .then(|| big_value(rng, cfg.tables.big_bytes));
            admin
                .execute(
                    &format!("INSERT INTO \"{s}\".\"{tn}\" (id, v, note, big) VALUES ($1, 0, $2, $3)"),
                    &[&id, &format!("n{id}-0"), &big],
                )
                .await?;
            committed.push(Committed {
                table: t,
                id,
                op: Op::Insert,
                v: 0,
                big: big.as_deref().map(Fingerprint::of),
                big_unchanged: false,
                extra: false,
            });
            st.live.push(id);
            st.v.insert(id, 0);
            st.big.insert(id, big);
        }
        states.insert(t, st);
    }
    let list: Vec<String> = (0..cfg.tables.count)
        .map(|t| format!("\"{s}\".\"{}\"", table_name(t)))
        .collect();
    admin
        .batch_execute(&format!(
            "CREATE PUBLICATION \"{}\" FOR TABLE {}",
            names.publication,
            list.join(", ")
        ))
        .await?;
    grant(admin, cfg, s).await?;
    sources::postgres::postgres_publication::register(
        admin,
        std::slice::from_ref(&names.publication),
    )
    .await
    .context("register the publication")?;
    Ok((committed, states))
}

/// Let the source's role read `schema` (snapshot reads and schema
/// introspection), when it is not the admin role.
async fn grant(admin: &Client, cfg: &PgQualConfig, schema: &str) -> Result<()> {
    let cdc: tokio_postgres::Config = cfg.cdc_dsn.parse().context("cdc_dsn")?;
    let admin_cfg: tokio_postgres::Config =
        cfg.admin_dsn.parse().context("admin_dsn")?;
    if let Some(user) = cdc.get_user()
        && Some(user) != admin_cfg.get_user()
    {
        admin
            .batch_execute(&format!(
                "GRANT USAGE ON SCHEMA \"{schema}\" TO \"{user}\"; GRANT SELECT ON ALL TABLES IN SCHEMA \"{schema}\" TO \"{user}\";"
            ))
            .await
            .context("grant the source's role")?;
    }
    Ok(())
}

/// A writer's share of the CDC phase: the tables it owns (every change of
/// a row comes from one writer, in order).
struct Writer {
    dsn: String,
    schema: String,
    kinds: Arc<dyn Fn(u32) -> Kind + Send + Sync>,
    tables: BTreeMap<u32, TableState>,
    start: Instant,
    cdc_start: tokio::time::Instant,
    duration: Duration,
    interval: Duration,
    ddl: Option<(u32, Duration)>,
    big_bytes: usize,
    seed: u64,
}

/// What a writer did.
#[derive(Debug, Default)]
struct Written {
    committed: Vec<Committed>,
    ddl_at_ms: Option<f64>,
    ops: u64,
}

impl Writer {
    async fn run(mut self) -> Result<Written> {
        let client = connect(&self.dsn).await?;
        let mut rng = StdRng::seed_from_u64(self.seed);
        let mut out = Written::default();
        let mut extra_on = false;
        let owned: Vec<u32> = self.tables.keys().copied().collect();
        let s = self.schema.clone();
        let mut next = self.cdc_start;
        let end = self.cdc_start + self.duration;
        loop {
            next += self.interval;
            if next >= end {
                break;
            }
            tokio::time::sleep_until(next).await;
            if let Some((t, at)) = self.ddl
                && !extra_on
                && tokio::time::Instant::now() >= self.cdc_start + at
            {
                client
                    .batch_execute(&format!(
                        "ALTER TABLE \"{s}\".\"{}\" ADD COLUMN extra bigint",
                        table_name(t)
                    ))
                    .await
                    .context("ADD COLUMN")?;
                extra_on = true;
                out.ddl_at_ms = Some(self.start.elapsed().as_secs_f64() * 1e3);
            }
            let t = owned[rng.random_range(0..owned.len())];
            let kind = (self.kinds)(t);
            let tn = table_name(t);
            let with_extra = extra_on && self.ddl.is_some_and(|(d, _)| d == t);
            let st = self.tables.get_mut(&t).expect("owned table");
            let roll: f64 = rng.random();
            let note = format!("n{}", rng.random::<u32>());
            if st.live.is_empty() || roll < 0.3 {
                let id = st.next_id;
                st.next_id += 1;
                let big = (kind != Kind::Plain)
                    .then(|| big_value(&mut rng, self.big_bytes));
                let sql = if with_extra {
                    format!(
                        "INSERT INTO \"{s}\".\"{tn}\" (id, v, note, big, extra) VALUES ($1, 0, $2, $3, 0)"
                    )
                } else {
                    format!(
                        "INSERT INTO \"{s}\".\"{tn}\" (id, v, note, big) VALUES ($1, 0, $2, $3)"
                    )
                };
                client
                    .execute(&sql, &[&id, &note, &big])
                    .await
                    .context("insert")?;
                out.committed.push(Committed {
                    table: t,
                    id,
                    op: Op::Insert,
                    v: 0,
                    big: big.as_deref().map(Fingerprint::of),
                    big_unchanged: false,
                    extra: with_extra,
                });
                st.live.push(id);
                st.v.insert(id, 0);
                st.big.insert(id, big);
            } else if roll < 0.8 {
                let id = st.live[rng.random_range(0..st.live.len())];
                let change_big = kind != Kind::Plain && rng.random_bool(0.5);
                let set_extra = if with_extra { ", extra = v + 1" } else { "" };
                let v: i64 = if change_big {
                    let big = big_value(&mut rng, self.big_bytes);
                    let v = client
                        .query_one(
                            &format!("UPDATE \"{s}\".\"{tn}\" SET v = v + 1, note = $1, big = $2{set_extra} WHERE id = $3 RETURNING v"),
                            &[&note, &big, &id],
                        )
                        .await
                        .context("update")?
                        .get(0);
                    st.big.insert(id, Some(big));
                    v
                } else {
                    client
                        .query_one(
                            &format!("UPDATE \"{s}\".\"{tn}\" SET v = v + 1, note = $1{set_extra} WHERE id = $2 RETURNING v"),
                            &[&note, &id],
                        )
                        .await
                        .context("update")?
                        .get(0)
                };
                st.v.insert(id, v);
                out.committed.push(Committed {
                    table: t,
                    id,
                    op: Op::Update,
                    v,
                    big: st.big[&id].as_deref().map(Fingerprint::of),
                    big_unchanged: kind != Kind::Plain && !change_big,
                    extra: with_extra,
                });
            } else {
                let i = rng.random_range(0..st.live.len());
                let id = st.live.swap_remove(i);
                client
                    .execute(
                        &format!("DELETE FROM \"{s}\".\"{tn}\" WHERE id = $1"),
                        &[&id],
                    )
                    .await
                    .context("delete")?;
                out.committed.push(Committed {
                    table: t,
                    id,
                    op: Op::Delete,
                    v: st.v[&id],
                    big: None,
                    big_unchanged: false,
                    extra: with_extra,
                });
            }
            out.ops += 1;
        }
        Ok(out)
    }
}

/// Every row of every table, as the source holds it at the end.
async fn finals(
    admin: &Client,
    cfg: &PgQualConfig,
    names: &Names,
    ddl_added: bool,
) -> Result<BTreeMap<(u32, i64), FinalRow>> {
    let mut out = BTreeMap::new();
    for t in 0..cfg.tables.count {
        let has_extra = ddl_added && t == cfg.add_column.table;
        let cols = if has_extra {
            "id, v, note, big, extra"
        } else {
            "id, v, note, big"
        };
        for row in admin
            .query(
                &format!(
                    "SELECT {cols} FROM \"{}\".\"{}\"",
                    names.schema,
                    table_name(t)
                ),
                &[],
            )
            .await?
        {
            let big: Option<String> = row.get(3);
            out.insert(
                (t, row.get::<_, i64>(0)),
                FinalRow {
                    v: row.get(1),
                    note: row.get(2),
                    big: big.as_deref().map(Fingerprint::of),
                    extra: has_extra.then(|| row.get::<_, Option<i64>>(4)),
                },
            );
        }
    }
    Ok(out)
}

fn as_i64(v: &Value) -> Option<i64> {
    v.as_i64().or_else(|| v.as_str()?.parse().ok())
}

/// One native-envelope event of the run's schema, or `None` for anything
/// else.
pub fn decode(
    schema: &str,
    payload: &[u8],
    offset: i64,
    at_ms: f64,
) -> Result<Option<Delivered>> {
    let v: Value =
        serde_json::from_slice(payload).context("event is not JSON")?;
    let (op, snapshot) = match v.get("op").and_then(Value::as_str) {
        Some("r") => (Op::Insert, true),
        Some("c") => (Op::Insert, false),
        Some("u") => (Op::Update, false),
        Some("d") => (Op::Delete, false),
        _ => return Ok(None),
    };
    let source = &v["source"];
    let in_schema = source.get("schema").and_then(Value::as_str);
    let table = source.get("table").and_then(Value::as_str).unwrap_or("");
    let (sch, tn) = match table.split_once('.') {
        Some((a, b)) => (Some(a), b),
        None => (in_schema, table),
    };
    if sch != Some(schema) {
        return Ok(None);
    }
    let Some(t) = tn.strip_prefix('t').and_then(|n| n.parse::<u32>().ok())
    else {
        return Ok(None);
    };
    let image = if op == Op::Delete {
        &v["before"]
    } else {
        &v["after"]
    };
    let id = as_i64(&image["id"]).context("row without id")?;
    let after = v.get("after").and_then(Value::as_object);
    let big = match after.and_then(|a| a.get("big")) {
        None => Big::Absent,
        Some(Value::Null) => Big::Null,
        Some(Value::String(s)) => Big::Value(Fingerprint::of(s)),
        Some(other) => bail!("big is not text: {other}"),
    };
    let unchanged = v
        .get("unchanged_columns")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|c| c.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    Ok(Some(Delivered {
        offset,
        at_ms,
        table: t,
        id,
        op,
        snapshot,
        v: as_i64(&image["v"]),
        note: image.get("note").and_then(Value::as_str).map(String::from),
        big,
        unchanged,
        extra: after.and_then(|a| a.get("extra")).map(as_i64),
    }))
}

/// Messages as consumed: (offset, arrival ms since the run started,
/// payload).
type Seen = Arc<Mutex<Vec<(i64, f64, Vec<u8>)>>>;

/// Consume `topic` from its start until stopped, then until idle for
/// `idle` (never longer than `max` after the stop); every message is kept
/// with its offset and arrival.
async fn consume(
    brokers: String,
    topic: String,
    start: Instant,
    seen: Seen,
    mut stop: watch::Receiver<bool>,
    idle: Duration,
    max: Duration,
) -> Result<()> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set(
            "group.id",
            format!("pgqual-{topic}-{}", Utc::now().timestamp_micros()),
        )
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("fetch.max.bytes", "52428800")
        .create()
        .context("create the consumer")?;
    consumer.subscribe(&[&topic])?;
    let mut stopped_at: Option<tokio::time::Instant> = None;
    loop {
        if stopped_at.is_none() && *stop.borrow() {
            stopped_at = Some(tokio::time::Instant::now());
        }
        let wait = if stopped_at.is_some() {
            idle
        } else {
            Duration::from_secs(1)
        };
        tokio::select! {
            m = tokio::time::timeout(wait, consumer.recv()) => match m {
                Ok(Ok(m)) => seen.lock().push((
                    m.offset(),
                    start.elapsed().as_secs_f64() * 1e3,
                    m.payload().unwrap_or_default().to_vec(),
                )),
                Ok(Err(e)) => eprintln!("  consumer: kafka error: {e}"),
                Err(_) if stopped_at.is_some() => break,
                Err(_) => {}
            },
            _ = stop.changed(), if stopped_at.is_none() => {}
        }
        if stopped_at.is_some_and(|at| at.elapsed() >= max) {
            break;
        }
    }
    Ok(())
}

/// How many messages `topic` holds (read until idle for `idle`).
async fn count_messages(
    brokers: &str,
    topic: &str,
    idle: Duration,
) -> Result<u64> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (tx, rx) = watch::channel(true);
    consume(
        brokers.into(),
        topic.into(),
        Instant::now(),
        seen.clone(),
        rx,
        idle,
        idle * 4,
    )
    .await?;
    drop(tx);
    Ok(seen.lock().len() as u64)
}

/// Status observations: (ms since start, status).
type Statuses = Arc<Mutex<Vec<(f64, String)>>>;

async fn watch_status(
    api: Arc<Api>,
    name: String,
    start: Instant,
    every: Duration,
    out: Statuses,
    mut stop: watch::Receiver<bool>,
) {
    loop {
        let s = api
            .status(&name)
            .await
            .unwrap_or_else(|e| format!("unreadable: {e}"));
        out.lock().push((start.elapsed().as_secs_f64() * 1e3, s));
        tokio::select! {
            _ = tokio::time::sleep(every) => {}
            _ = stop.changed() => break,
        }
    }
}

/// Health of the main pipeline: no `failed` status at any time, and no
/// status other than `running` after it first ran, except inside the
/// planned stop (`window`, ms).
pub fn health(
    statuses: &[(f64, String)],
    window: Option<(f64, f64)>,
) -> (bool, Vec<String>) {
    let mut ran = false;
    let mut bad = Vec::new();
    for (at, s) in statuses {
        let planned = window.is_some_and(|(a, b)| *at >= a && *at <= b);
        if s == "failed" {
            bad.push(format!("{at:.0} ms: failed"));
        } else if s == "running" {
            ran = true;
        } else if ran && !planned {
            bad.push(format!("{at:.0} ms: {s}"));
        }
    }
    if !ran {
        bad.push("never running".into());
    }
    (bad.is_empty(), bad)
}

/// A metrics scrape: the exposition text, or why it could not be read.
type Scrape = std::result::Result<String, String>;

/// Scrape `url`. A refused request, an HTTP error status or an unreadable
/// body is an error, never an empty exposition.
async fn scrape(url: &str) -> Scrape {
    let resp = reqwest::get(url)
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(|e| format!("{url}: {e}"))?;
    resp.text().await.map_err(|e| format!("{url}: body: {e}"))
}

/// Counter `name` of `pipeline` (with the extra label, if any), summed over
/// its series. `None` when no series is exported: counters are exported
/// lazily, on their first increment. A value that is not a finite,
/// non-negative number is an error.
fn counter(
    text: &str,
    name: &str,
    pipeline: &str,
    label: Option<(&str, &str)>,
) -> std::result::Result<Option<f64>, String> {
    let mut sum = None;
    for line in text.lines() {
        let Some((n, labels, value)) = measure::parse_series(line) else {
            continue;
        };
        let wanted = n == name
            && labels.get("pipeline").map(String::as_str) == Some(pipeline)
            && label.is_none_or(|(k, v)| {
                labels.get(k).map(String::as_str) == Some(v)
            });
        if !wanted {
            continue;
        }
        let v = value
            .parse::<f64>()
            .ok()
            .filter(|v| v.is_finite() && *v >= 0.0)
            .ok_or_else(|| format!("{name}: invalid value {value:?}"))?;
        sum = Some(sum.unwrap_or(0.0) + v);
    }
    Ok(sum)
}

/// The increase of a counter between two scrapes. Not yet exported at the
/// start is zero; a failed scrape, an invalid value, a series that
/// disappears or a decrease is an error: missing evidence, never zero.
fn counter_delta(
    before: &Scrape,
    after: &Scrape,
    name: &str,
    pipeline: &str,
    label: Option<(&str, &str)>,
) -> std::result::Result<f64, String> {
    let read = |s: &Scrape, when: &str| {
        let text = s
            .as_ref()
            .map_err(|e| format!("{when} scrape failed: {e}"))?;
        counter(text, name, pipeline, label)
            .map_err(|e| format!("{when} scrape: {e}"))
    };
    match (read(before, "start")?, read(after, "end")?) {
        (Some(b), None) => Err(format!("{name}: disappeared (was {b})")),
        (b, a) => {
            let (b, a) = (b.unwrap_or(0.0), a.unwrap_or(0.0));
            if a < b {
                Err(format!("{name}: decreased from {b} to {a}"))
            } else {
                Ok(a - b)
            }
        }
    }
}

/// The number of incidents in a `/pipelines/{name}/incidents` response
/// (its list, or one if only a primary is named). A response that could not
/// be read or lacks that shape is an error, never "no incidents".
fn open_incidents(resp: &Result<Value>) -> std::result::Result<usize, String> {
    let v = resp.as_ref().map_err(|e| format!("{e:#}"))?;
    let list = v["incidents"]
        .as_array()
        .ok_or_else(|| format!("unexpected incidents response: {v}"))?;
    Ok(list.len().max(usize::from(!v["primary"].is_null())))
}

/// The reasons the incident evidence fails the run, or leaves it
/// incomplete.
fn incident_reasons(
    raised: &std::result::Result<f64, String>,
    open: &std::result::Result<usize, String>,
) -> Vec<String> {
    let mut out = Vec::new();
    match raised {
        Ok(n) if *n > 0.0 => out.push(format!("incidents raised: {n}")),
        Ok(_) => {}
        Err(e) => {
            out.push(format!("incidents counter unreadable (incomplete): {e}"))
        }
    }
    match open {
        Ok(0) => {}
        Ok(n) => out.push(format!("{n} incident(s) recorded")),
        Err(e) => out.push(format!("incidents unreadable (incomplete): {e}")),
    }
    out
}

/// The reason an unchanged-TOAST counter fails the run, or leaves it
/// incomplete. Its delta must be at least the delivered count: an event
/// emitted before the stop and not delivered is emitted (and counted) again
/// after it.
fn toast_counter_reason(
    outcome: &str,
    delta: &std::result::Result<f64, String>,
    delivered: u64,
) -> Option<String> {
    match delta {
        Ok(d) if *d < delivered as f64 => Some(format!(
            "unchanged-TOAST {outcome} counter delta {d} is below the {delivered} delivered"
        )),
        Ok(_) => None,
        Err(e) => Some(format!(
            "unchanged-TOAST {outcome} counter unreadable (incomplete): {e}"
        )),
    }
}

/// A read kept as evidence: its value, or the error.
fn kept<T: Serialize>(r: &std::result::Result<T, String>) -> Value {
    match r {
        Ok(v) => json!(v),
        Err(e) => json!({"error": e}),
    }
}

/// An API read kept as evidence: its value, or the error.
fn kept_api(r: &Result<Value>) -> Value {
    match r {
        Ok(v) => v.clone(),
        Err(e) => json!({"error": format!("{e:#}")}),
    }
}

async fn until_status(
    api: &Api,
    name: &str,
    want: &str,
    timeout: Duration,
) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if api.status(name).await.ok().as_deref() == Some(want) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    false
}

const REFUSAL_CLASS: &str = "toast_without_full_identity";

/// The deliberately unsupported cases, each in its own pipeline with an
/// Avro sink. Each is `ok` only when it fails visibly as specified.
async fn unsupported(
    cfg: &PgQualConfig,
    api: &Api,
    admin: &Client,
    names: &Names,
    u: &UnsupportedCfg,
) -> Result<Value> {
    let timeout = Duration::from_secs(u.timeout_secs);
    let idle = Duration::from_secs(5);
    let mut out = serde_json::Map::new();

    // U1: a table that is not FULL, refused at start.
    let n1 = names.unsupported("u1");
    let n2 = names.unsupported("u2");
    for (n, identity) in [(&n1, "DEFAULT"), (&n2, "FULL")] {
        admin
            .batch_execute(&format!(
                "CREATE SCHEMA \"{s}\"; CREATE TABLE \"{s}\".\"t0000\" (id bigint PRIMARY KEY, v bigint NOT NULL, note text NOT NULL, big text); \
                 ALTER TABLE \"{s}\".\"t0000\" ALTER COLUMN big SET STORAGE EXTERNAL; \
                 ALTER TABLE \"{s}\".\"t0000\" REPLICA IDENTITY {identity}; \
                 INSERT INTO \"{s}\".\"t0000\" VALUES (1, 0, 'n', repeat('x', 8192)); \
                 CREATE PUBLICATION \"{p}\" FOR TABLE \"{s}\".\"t0000\";",
                s = n.schema,
                p = n.publication
            ))
            .await?;
        grant(admin, cfg, &n.schema).await?;
        sources::postgres::postgres_publication::register(
            admin,
            std::slice::from_ref(&n.publication),
        )
        .await?;
        crate::consumer::ensure_topics(
            &cfg.verifier_kafka_brokers,
            std::slice::from_ref(&n.topic),
            Some(1),
            Duration::from_secs(60),
        )
        .await?;
    }
    let created = api
        .create(&render_spec(
            cfg,
            &n1,
            Some(&u.schema_registry_url),
            "initial",
        )?)
        .await;
    let failed = match &created {
        Ok(_) => until_status(api, &n1.pipeline, "failed", timeout).await,
        Err(_) => true,
    };
    let incidents = api.incidents(&n1.pipeline).await;
    let class_seen = incidents
        .as_ref()
        .is_ok_and(|v| v.to_string().contains(REFUSAL_CLASS))
        || created
            .as_ref()
            .err()
            .is_some_and(|e| e.to_string().contains(REFUSAL_CLASS));
    let messages =
        count_messages(&cfg.verifier_kafka_brokers, &n1.topic, idle).await?;
    // An unreadable checkpoint list is no evidence of "no checkpoint".
    let checkpoints = api.checkpoints(&n1.pipeline).await;
    let no_checkpoint = checkpoints
        .as_ref()
        .is_ok_and(|v| v.as_array().is_some_and(|a| a.is_empty()));
    out.insert(
        "u1_refused_at_start".into(),
        json!({
            "ok": failed && class_seen && messages == 0 && no_checkpoint,
            "failed_status": failed, "incident_class_seen": class_seen,
            "messages": messages, "checkpoints": kept_api(&checkpoints),
            "incidents": kept_api(&incidents),
            "create_error": created.as_ref().err().map(|e| format!("{e:#}")),
        }),
    );
    api.delete(&n1.pipeline).await.ok();

    // U2: a FULL table changed away from FULL while running.
    api.create(&render_spec(
        cfg,
        &n2,
        Some(&u.schema_registry_url),
        "never",
    )?)
    .await?;
    let running = until_status(api, &n2.pipeline, "running", timeout).await;
    admin
        .execute(&format!("INSERT INTO \"{}\".\"t0000\" VALUES (2, 0, 'a', repeat('y', 8192))", n2.schema), &[])
        .await?;
    let mut before = 0;
    let deadline = tokio::time::Instant::now() + timeout;
    while before == 0 && tokio::time::Instant::now() < deadline {
        before = count_messages(&cfg.verifier_kafka_brokers, &n2.topic, idle)
            .await?;
    }
    admin
        .batch_execute(&format!(
            "ALTER TABLE \"{s}\".\"t0000\" REPLICA IDENTITY DEFAULT; INSERT INTO \"{s}\".\"t0000\" VALUES (3, 0, 'b', repeat('z', 8192));",
            s = n2.schema
        ))
        .await?;
    let failed = until_status(api, &n2.pipeline, "failed", timeout).await;
    let incidents = api.incidents(&n2.pipeline).await;
    let class_seen = incidents
        .as_ref()
        .is_ok_and(|v| v.to_string().contains(REFUSAL_CLASS));
    let after =
        count_messages(&cfg.verifier_kafka_brokers, &n2.topic, idle).await?;
    out.insert("u2_stopped_when_changed".into(), json!({
        "ok": running && before == 1 && failed && class_seen && after == before,
        "ran": running, "messages_before_change": before, "messages_after": after,
        "failed_status": failed, "incident_class_seen": class_seen,
        "incidents": kept_api(&incidents),
    }));
    api.delete(&n2.pipeline).await.ok();
    Ok(Value::Object(out))
}

/// Run the qualification once. Returns the run's result directory; its
/// `result.json` holds the verdict.
pub async fn run(cfg: &PgQualConfig) -> Result<PathBuf> {
    cfg.validate()?;
    let start = Instant::now();
    let at = Utc::now();
    let tag = evidence::claim_run_tag(&cfg.output_dir, at)?;
    let run_id = format!("{}-g4", evidence::unique_stem(at));
    let dir = PathBuf::from(&cfg.output_dir).join("g4").join(&run_id);
    evidence::create_new_dir(&dir)?;
    let names = Names::new(&cfg.prefix, tag);
    let api = Arc::new(Api::new(&cfg.api_url)?);
    let admin = connect(&cfg.admin_dsn).await?;
    let mut rng = StdRng::seed_from_u64(cfg.seed ^ tag);
    let mut reasons: Vec<String> = Vec::new();
    eprintln!(
        "g4 {run_id}: fixture {} tables in {}",
        cfg.tables.count, names.schema
    );

    let (mut committed, states) =
        fixture(&admin, cfg, &names, &mut rng).await?;
    let snapshot_rows = committed.len();
    crate::consumer::ensure_topics(
        &cfg.verifier_kafka_brokers,
        std::slice::from_ref(&names.topic),
        Some(1),
        Duration::from_secs(60),
    )
    .await?;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (stop_tx, stop_rx) = watch::channel(false);
    let consumer = tokio::spawn(consume(
        cfg.verifier_kafka_brokers.clone(),
        names.topic.clone(),
        start,
        seen.clone(),
        stop_rx,
        Duration::from_secs(cfg.drain_idle_secs),
        Duration::from_secs(cfg.drain_max_secs),
    ));
    let m0 = scrape(&cfg.metrics_url).await;

    let spec = render_spec(cfg, &names, None, "initial")?;
    evidence::write_new(
        &dir.join("pipeline.json"),
        &serde_json::to_vec_pretty(&spec)?,
    )?;
    api.create(&spec).await?;
    let statuses: Statuses = Arc::default();
    let (health_tx, health_rx) = watch::channel(false);
    let watcher = tokio::spawn(watch_status(
        api.clone(),
        names.pipeline.clone(),
        start,
        Duration::from_millis(cfg.status_poll_ms),
        statuses.clone(),
        health_rx,
    ));

    // The snapshot: every fixture row read, before any CDC write.
    let deadline =
        Instant::now() + Duration::from_secs(cfg.snapshot_timeout_secs);
    let (mut scanned, mut reads) = (0usize, 0usize);
    let snapshot_complete = loop {
        {
            let seen = seen.lock();
            for (o, a, p) in &seen[scanned..] {
                if matches!(decode(&names.schema, p, *o, *a), Ok(Some(d)) if d.snapshot)
                {
                    reads += 1;
                }
            }
            scanned = seen.len();
        }
        if reads >= snapshot_rows {
            break true;
        }
        if Instant::now() >= deadline
            || statuses.lock().last().is_some_and(|(_, s)| s == "failed")
        {
            break false;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    };
    let snapshot_ms = start.elapsed().as_secs_f64() * 1e3;
    if !snapshot_complete {
        reasons.push("the snapshot did not deliver every fixture row".into());
    }

    // The CDC phase.
    let writers = cfg.cdc.writers;
    let interval =
        Duration::from_secs_f64(writers as f64 / cfg.cdc.ops_per_sec);
    let cdc_start = tokio::time::Instant::now();
    let cdc_start_ms = start.elapsed().as_secs_f64() * 1e3;
    let kinds: Arc<dyn Fn(u32) -> Kind + Send + Sync> = {
        let c = cfg.clone();
        Arc::new(move |t| c.kind(t))
    };
    let mut handles = Vec::new();
    let mut split: Vec<BTreeMap<u32, TableState>> =
        (0..writers).map(|_| BTreeMap::new()).collect();
    for (t, st) in states {
        split[(t % writers) as usize].insert(t, st);
    }
    for (w, tables) in split.into_iter().enumerate() {
        if tables.is_empty() {
            continue;
        }
        let ddl = tables.contains_key(&cfg.add_column.table).then(|| {
            (
                cfg.add_column.table,
                Duration::from_secs(cfg.add_column.at_secs),
            )
        });
        handles.push(tokio::spawn(
            Writer {
                dsn: cfg.admin_dsn.clone(),
                schema: names.schema.clone(),
                kinds: kinds.clone(),
                tables,
                start,
                cdc_start,
                duration: Duration::from_secs(cfg.cdc.duration_secs),
                interval,
                ddl,
                big_bytes: cfg.tables.big_bytes,
                seed: cfg.seed ^ tag ^ (w as u64 + 1),
            }
            .run(),
        ));
    }
    tokio::time::sleep_until(cdc_start + Duration::from_secs(cfg.stop.at_secs))
        .await;
    let stop_ms = start.elapsed().as_secs_f64() * 1e3;
    let stopped = api.stop(&names.pipeline).await;
    tokio::time::sleep(Duration::from_secs(cfg.stop.for_secs)).await;
    let resume_sent_ms = start.elapsed().as_secs_f64() * 1e3;
    let resumed = api.resume(&names.pipeline).await;
    let running_again = api
        .until_running(&names.pipeline, Duration::from_secs(300))
        .await;
    let resumed_ms = start.elapsed().as_secs_f64() * 1e3;
    for (what, r) in [
        ("stop", stopped.map(|_| ())),
        ("resume", resumed.map(|_| ())),
        ("running again", running_again),
    ] {
        if let Err(e) = r {
            reasons.push(format!("the {what} did not complete: {e:#}"));
        }
    }
    let mut ddl_at_ms = None;
    let mut ops = 0;
    let mut writer_errors = Vec::new();
    for h in handles {
        match h.await? {
            Ok(w) => {
                committed.extend(w.committed);
                ddl_at_ms = ddl_at_ms.or(w.ddl_at_ms);
                ops += w.ops;
            }
            Err(e) => writer_errors.push(format!("{e:#}")),
        }
    }
    if !writer_errors.is_empty() {
        reasons.push(format!("{} writer(s) failed", writer_errors.len()));
    }
    let cdc_end_ms = start.elapsed().as_secs_f64() * 1e3;
    if ddl_at_ms.is_none() {
        reasons.push("ADD COLUMN did not run".into());
    }

    // Drain, then the final state.
    stop_tx.send(true).ok();
    consumer.await??;
    let finals = finals(&admin, cfg, &names, ddl_at_ms.is_some()).await?;
    health_tx.send(true).ok();
    watcher.await.ok();
    let m1 = scrape(&cfg.metrics_url).await;
    let incidents = api.incidents(&names.pipeline).await;

    let raw = std::mem::take(&mut *seen.lock());
    let mut delivered = Vec::with_capacity(raw.len());
    let mut undecodable = 0u64;
    for (offset, at_ms, payload) in &raw {
        match decode(&names.schema, payload, *offset, *at_ms) {
            Ok(Some(d)) => delivered.push(d),
            Ok(None) => {}
            Err(_) => undecodable += 1,
        }
    }
    if undecodable > 0 {
        reasons.push(format!("{undecodable} undecodable event(s)"));
    }
    let kind = |t: u32| cfg.kind(t);
    let report: Report = evaluate(
        &committed,
        &delivered,
        &finals,
        &Expect {
            kind: &kind,
            ddl_table: cfg.add_column.table,
            resumed_ms: Some(resume_sent_ms),
        },
    );
    if !report.ok() {
        reasons.push("the checks failed (see report)".into());
    }
    let statuses = statuses.lock().clone();
    let (healthy, health_issues) =
        health(&statuses, Some((stop_ms, resumed_ms)));
    if !healthy {
        reasons.push(format!("pipeline health: {}", health_issues.join("; ")));
    }
    let pl = &names.pipeline;
    let incidents_raised =
        counter_delta(&m0, &m1, "deltaforge_incidents_raised_total", pl, None);
    reasons.extend(incident_reasons(
        &incidents_raised,
        &open_incidents(&incidents),
    ));
    let toast_delta = |outcome| {
        counter_delta(
            &m0,
            &m1,
            "deltaforge_pg_unchanged_toast_total",
            pl,
            Some(("outcome", outcome)),
        )
    };
    let (reconstructed, unavailable) =
        (toast_delta("reconstructed"), toast_delta("unavailable"));
    reasons.extend(
        [
            ("reconstructed", &reconstructed, report.toast_reconstructed),
            ("unavailable", &unavailable, report.toast_unavailable),
        ]
        .into_iter()
        .filter_map(|(o, d, n)| toast_counter_reason(o, d, n)),
    );
    let unsupported_result = match &cfg.unsupported {
        Some(u) => match unsupported(cfg, &api, &admin, &names, u).await {
            Ok(v) => {
                if v.as_object()
                    .is_some_and(|m| m.values().any(|c| c["ok"] != json!(true)))
                {
                    reasons.push("a deliberately unsupported case did not fail as specified".into());
                }
                v
            }
            Err(e) => {
                reasons
                    .push(format!("the unsupported cases did not run: {e:#}"));
                Value::Null
            }
        },
        None => {
            reasons.push("the deliberately unsupported cases are not configured (incomplete)".into());
            Value::Null
        }
    };

    write_jsonl(&dir.join("ledger.jsonl"), &committed)?;
    write_jsonl(&dir.join("delivered.jsonl"), &delivered)?;
    evidence::write_new(
        &dir.join("statuses.json"),
        &serde_json::to_vec(&statuses)?,
    )?;
    let result = json!({
        "run_id": run_id, "run_tag": tag, "names": names,
        "state_store": cfg.state_store,
        "config": cfg,
        "ok": reasons.is_empty(),
        "reasons": reasons,
        "timings_ms": {"snapshot_done": snapshot_ms, "cdc_start": cdc_start_ms, "stop": stop_ms,
                        "resume_sent": resume_sent_ms, "running_again": resumed_ms,
                        "add_column": ddl_at_ms, "cdc_end": cdc_end_ms,
                        "end": start.elapsed().as_secs_f64() * 1e3},
        "snapshot_rows": snapshot_rows, "snapshot_complete": snapshot_complete,
        "cdc_ops": ops, "writer_errors": writer_errors,
        "messages": raw.len(), "undecodable": undecodable,
        "report": report,
        "health": {"ok": healthy, "issues": health_issues, "observations": statuses.len()},
        "incidents": {"raised": kept(&incidents_raised), "open": kept_api(&incidents)},
        "toast_counters": {"reconstructed": kept(&reconstructed),
                           "unavailable": kept(&unavailable),
                           "delivered_reconstructed": report.toast_reconstructed,
                           "delivered_unavailable": report.toast_unavailable},
        "unsupported_cases": unsupported_result,
        "pg_state_store_qualified": false,
    });
    evidence::write_new(
        &dir.join("result.json"),
        &serde_json::to_vec_pretty(&result)?,
    )?;
    Ok(dir)
}

fn write_jsonl<T: Serialize>(path: &std::path::Path, rows: &[T]) -> Result<()> {
    let mut out = Vec::new();
    for r in rows {
        serde_json::to_writer(&mut out, r)?;
        out.push(b'\n');
    }
    evidence::write_new(path, &out)
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXAMPLE: &str = include_str!("../qualification/g4-postgres.yaml");

    #[test]
    fn the_g4_configuration_parses_and_validates() {
        let cfg: PgQualConfig = serde_yaml::from_str(EXAMPLE).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.state_store, "sqlite");
        assert_eq!(cfg.tables.count, 1000);
        assert!(cfg.unsupported.is_some(), "G4 needs its unsupported cases");
        let mut bad = cfg.clone();
        bad.stop.at_secs = bad.cdc.duration_secs;
        assert!(bad.validate().is_err());
        let mut bad = cfg;
        bad.tables.toast_full = bad.tables.count + 1;
        assert!(bad.validate().is_err());
    }

    #[test]
    fn specs_name_the_run_and_the_avro_cases_use_the_registry() {
        let cfg: PgQualConfig = serde_yaml::from_str(EXAMPLE).unwrap();
        let names = Names::new("g4", 42);
        let spec = render_spec(&cfg, &names, None, "initial").unwrap();
        let src = &spec["spec"]["source"]["config"];
        assert_eq!(src["publication"], "g4pub_42");
        assert_eq!(src["slot"], "g4slot_42");
        assert_eq!(src["tables"][0], "g4_42.*");
        assert_eq!(spec["spec"]["sinks"][0]["config"]["topic"], "g4.42");
        assert!(spec["spec"]["sinks"][0]["config"]["encoding"].is_null());
        let u = names.unsupported("u1");
        let spec =
            render_spec(&cfg, &u, Some("http://sr:8081"), "never").unwrap();
        assert_eq!(
            spec["spec"]["sinks"][0]["config"]["encoding"]["type"],
            "avro"
        );
        assert_eq!(
            spec["spec"]["source"]["config"]["snapshot"]["mode"],
            "never"
        );
        assert_eq!(spec["metadata"]["name"], "g4-42-u1");
    }

    #[test]
    fn events_are_decoded_with_their_toast_and_added_column() {
        let full = br#"{"op":"u","source":{"schema":"g4_1","table":"t0003"},"before":null,
            "after":{"id":7,"v":2,"note":"n","big":"abc","extra":2},"unchanged_columns":[]}"#;
        let d = decode("g4_1", full, 5, 1.0).unwrap().unwrap();
        assert_eq!((d.table, d.id, d.v, d.op), (3, 7, Some(2), Op::Update));
        assert_eq!(d.big, Big::Value(Fingerprint::of("abc")));
        assert_eq!(d.extra, Some(Some(2)));
        let partial = br#"{"op":"u","source":{"schema":"g4_1","table":"t0004"},
            "after":{"id":"8","v":"3","note":"n"},"unchanged_columns":["big"]}"#;
        let d = decode("g4_1", partial, 6, 1.0).unwrap().unwrap();
        assert_eq!(
            (d.id, d.v, d.big, d.extra),
            (8, Some(3), Big::Absent, None)
        );
        assert_eq!(d.unchanged, vec!["big".to_string()]);
        let snap = br#"{"op":"r","source":{"schema":"g4_1","table":"t0000"},"after":{"id":1,"v":0,"note":"n","big":null}}"#;
        let d = decode("g4_1", snap, 1, 0.0).unwrap().unwrap();
        assert!(d.snapshot && d.big == Big::Null);
        let del = br#"{"op":"d","source":{"schema":"g4_1","table":"t0000"},"before":{"id":1},"after":null}"#;
        let d = decode("g4_1", del, 2, 0.0).unwrap().unwrap();
        assert_eq!((d.op, d.id, d.v), (Op::Delete, 1, None));
        let other = br#"{"op":"c","source":{"schema":"elsewhere","table":"t0000"},"after":{"id":1}}"#;
        assert!(decode("g4_1", other, 3, 0.0).unwrap().is_none());
    }

    #[test]
    fn health_allows_only_the_planned_stop() {
        let s = |v: &[(f64, &str)]| {
            v.iter()
                .map(|(a, b)| (*a, b.to_string()))
                .collect::<Vec<_>>()
        };
        let ok = s(&[
            (0.0, "starting"),
            (1.0, "running"),
            (5.0, "stopped"),
            (6.0, "starting"),
            (7.0, "running"),
        ]);
        assert!(health(&ok, Some((4.0, 7.5))).0);
        assert!(!health(&ok, None).0, "a stop outside the plan");
        let failed = s(&[(1.0, "running"), (2.0, "failed"), (3.0, "running")]);
        let (good, issues) = health(&failed, Some((0.0, 10.0)));
        assert!(!good, "a failure inside the stop window still fails");
        assert_eq!(issues, vec!["2 ms: failed".to_string()]);
        assert!(!health(&s(&[(0.0, "starting")]), None).0, "never running");
    }

    #[test]
    fn counters_are_summed_per_pipeline_and_label() {
        let text = "deltaforge_pg_unchanged_toast_total{pipeline=\"g4-1\",source=\"s\",outcome=\"reconstructed\"} 3\n\
                    deltaforge_pg_unchanged_toast_total{pipeline=\"g4-1\",source=\"s\",outcome=\"unavailable\"} 2\n\
                    deltaforge_pg_unchanged_toast_total{pipeline=\"other\",source=\"s\",outcome=\"reconstructed\"} 9\n";
        let c = |pl, label| {
            counter(text, "deltaforge_pg_unchanged_toast_total", pl, label)
        };
        assert_eq!(
            c("g4-1", Some(("outcome", "reconstructed"))),
            Ok(Some(3.0))
        );
        assert_eq!(c("g4-1", None), Ok(Some(5.0)));
        assert_eq!(c("absent", None), Ok(None), "not exported yet");
    }

    const C: &str = "deltaforge_pg_unchanged_toast_total";

    fn at(v: &str) -> Scrape {
        Ok(format!(
            "# TYPE {C} counter\n{C}{{pipeline=\"p\",outcome=\"reconstructed\"}} {v}\n"
        ))
    }

    fn delta(
        before: Scrape,
        after: Scrape,
    ) -> std::result::Result<f64, String> {
        counter_delta(
            &before,
            &after,
            C,
            "p",
            Some(("outcome", "reconstructed")),
        )
    }

    /// Lazy export is zero; a failed scrape, an invalid value, a vanished
    /// series or a decrease is missing evidence, never zero.
    #[test]
    fn counter_deltas_never_turn_missing_evidence_into_zero() {
        let none = || Ok(String::new());
        assert_eq!(delta(none(), none()), Ok(0.0), "never exported");
        assert_eq!(delta(none(), at("5")), Ok(5.0), "exported during the run");
        assert_eq!(delta(at("2"), at("5")), Ok(3.0));
        let e = delta(Err("connection refused".into()), at("5")).unwrap_err();
        assert!(e.contains("start scrape failed"), "{e}");
        let e = delta(at("5"), Err("500 Internal Server Error".into()))
            .unwrap_err();
        assert!(e.contains("end scrape failed"), "{e}");
        for bad in ["NaN", "-1", "+Inf", "abc", ""] {
            assert!(delta(at("1"), at(bad)).is_err(), "end value {bad:?}");
            assert!(delta(at(bad), at("1")).is_err(), "start value {bad:?}");
        }
        assert!(delta(at("5"), at("4")).unwrap_err().contains("decreased"));
        assert!(delta(at("5"), none()).unwrap_err().contains("disappeared"));
    }

    /// Only a readable response of the API's shape counts incidents.
    #[test]
    fn incidents_are_counted_only_from_a_readable_response() {
        let ok = |v: Value| -> Result<Value> { Ok(v) };
        let empty = json!({"pipeline": "p", "primary": null, "incidents": []});
        assert_eq!(open_incidents(&ok(empty)), Ok(0));
        let two = json!({"primary": "a", "incidents": [{"incident_id": "a"}, {"incident_id": "b"}]});
        assert_eq!(open_incidents(&ok(two)), Ok(2));
        assert_eq!(
            open_incidents(&ok(json!({"primary": "a", "incidents": []}))),
            Ok(1)
        );
        assert!(open_incidents(&Err(anyhow::anyhow!("500"))).is_err());
        assert!(open_incidents(&ok(Value::Null)).is_err(), "an empty body");
        assert!(
            open_incidents(&ok(json!([]))).is_err(),
            "not the API's shape"
        );

        assert!(incident_reasons(&Ok(0.0), &Ok(0)).is_empty());
        assert_eq!(incident_reasons(&Ok(1.0), &Ok(1)).len(), 2);
        for r in [
            incident_reasons(&Err("end scrape failed".into()), &Ok(0)),
            incident_reasons(&Ok(0.0), &Err("unreadable".into())),
        ] {
            assert_eq!(r.len(), 1);
            assert!(r[0].contains("(incomplete)"), "{r:?}");
        }
    }

    /// The counter delta must reach the delivered count; more is a replayed
    /// emission, an unreadable delta is incomplete.
    #[test]
    fn toast_counters_are_at_least_the_delivered_count() {
        assert_eq!(toast_counter_reason("reconstructed", &Ok(5.0), 5), None);
        assert_eq!(
            toast_counter_reason("reconstructed", &Ok(6.0), 5),
            None,
            "a replayed emission"
        );
        assert!(toast_counter_reason("reconstructed", &Ok(4.0), 5).is_some());
        let r =
            toast_counter_reason("unavailable", &Err("x".into()), 0).unwrap();
        assert!(r.contains("(incomplete)"), "{r}");
    }

    /// A refused request, an HTTP error or an unreadable body is a failed
    /// scrape, never an empty exposition.
    #[tokio::test]
    async fn failed_scrapes_are_errors() {
        use crate::deltaforge::tests::serve_raw;
        let status = serve_raw(
            "HTTP/1.1 500 Internal Server Error\r\ncontent-length: 0\r\n\r\n",
        )
        .await;
        assert!(scrape(&status).await.is_err());
        let truncated =
            serve_raw("HTTP/1.1 200 OK\r\ncontent-length: 100\r\n\r\nx 1\n")
                .await;
        assert!(scrape(&truncated).await.is_err());
        let good =
            serve_raw("HTTP/1.1 200 OK\r\ncontent-length: 4\r\n\r\nx 1\n")
                .await;
        assert_eq!(scrape(&good).await.unwrap(), "x 1\n");
        let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = closed.local_addr().unwrap();
        drop(closed);
        assert!(scrape(&format!("http://{addr}")).await.is_err());
    }
}
