//! The per-instance scaling study (`results/scale-study-design.md`): one
//! DeltaForge MySQL source and one Debezium connector over fixtures of many
//! tables, with a share of them changing.
//!
//! A fixture of `tables` narrow tables (`scale_<d>.t<k>`, 100 per database)
//! is built once per table count and shared by every run. Load writers (a
//! MySQL user of their own, `loadgen`, so that the root connections are the
//! system's) insert one-row transactions into the active tables. Each table
//! belongs to one writer, so its ids are committed in order. A run records
//! every commit and every arrival, and reports correctness, startup,
//! latency, catch-up and drain, and the resource and catalog counters of
//! both systems and of MySQL.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, anyhow, bail};
use mysql_async::prelude::Queryable;
use rdkafka::ClientConfig;
use rdkafka::Message;
use rdkafka::consumer::{Consumer, StreamConsumer};
use serde_json::{Value, json};

use crate::{
    Infra, KAFKA_NAME, METRICS, MYSQL_NAME, PW, Process, api,
    bootstrap_internal, brokers, create_topic, debezium, wait_status,
};

const TABLES_PER_DATABASE: usize = 100;
/// Load writers (MySQL connections); table `i` belongs to writer `i % WRITERS`.
const WRITERS: usize = 64;
const LOADGEN: &str = "loadgen";
/// A run's ids: `seq * RUN_IDS` plus its offset; markers are offsets from
/// `MARKER`, above every load row's.
const RUN_IDS: i64 = 1_000_000_000_000;
const MARKER: i64 = 900_000_000_000;
/// The rotation interval and the share of the active set it replaces.
const ROTATE_EVERY: Duration = Duration::from_secs(10);
const ROTATE_SHARE: f64 = 0.10;
/// Catch-up: when the pipeline stops, and for how long.
const CATCHUP_AT: Duration = Duration::from_secs(30);
const CATCHUP_FOR: Duration = Duration::from_secs(15);
/// Capacity sweep steps and the declared limits.
const STEP: Duration = Duration::from_secs(30);
const MAX_STEPS: u32 = 7;
const LIMIT_LATENCY: Duration = Duration::from_secs(5);
const LIMIT_SHARE: f64 = 0.95;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Workload {
    /// 2,000 changes/s.
    Steady,
    /// 1 s at 10,000 changes/s, then 4 s idle (2,000/s on average).
    Burst,
    /// From 2,000 changes/s, doubling every 30 s until a declared limit.
    Capacity,
}

impl Workload {
    pub fn parse(s: &str) -> Result<Workload> {
        Ok(match s {
            "steady" => Workload::Steady,
            "burst" => Workload::Burst,
            "capacity" => Workload::Capacity,
            other => {
                bail!("unknown workload {other} (steady, burst, capacity)")
            }
        })
    }

    fn name(self) -> &'static str {
        match self {
            Workload::Steady => "steady",
            Workload::Burst => "burst",
            Workload::Capacity => "capacity",
        }
    }

    /// The offered aggregate rate (changes/s) at `t` into the load, or None
    /// while idle.
    fn rate(self, t: Duration) -> Option<f64> {
        match self {
            Workload::Steady => Some(2_000.0),
            Workload::Burst => {
                (t.as_secs_f64() % 5.0 < 1.0).then_some(10_000.0)
            }
            Workload::Capacity => {
                let step = (t.as_secs_f64() / STEP.as_secs_f64()) as u32;
                Some(2_000.0 * f64::from(1u32 << step.min(MAX_STEPS - 1)))
            }
        }
    }
}

/// One point of the study.
#[derive(Clone, Copy, Debug)]
pub struct Point {
    pub tables: usize,
    /// Percent of the tables active.
    pub active: u32,
    pub workload: Workload,
    pub rotate: bool,
    /// The binary log's byte offset (MiB) in a fresh file when the run
    /// starts: 0 is a fresh-file best case for DeltaForge's proofs.
    pub binlog_mib: u64,
    /// What the file is padded with up to that offset.
    pub pad: Pad,
}

/// The padding of a run's binary log up to its starting offset.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Pad {
    /// Rows of 8 MiB: few, large events.
    Large,
    /// Single-row transactions like the load's: many small events, as an
    /// OLTP server's binary log holds.
    Small,
}

impl Pad {
    pub fn parse(s: &str) -> Result<Pad> {
        Ok(match s {
            "large" => Pad::Large,
            "small" => Pad::Small,
            other => bail!("unknown binary-log padding {other} (large, small)"),
        })
    }
}

impl Point {
    pub fn id(&self) -> String {
        format!(
            "t{}-a{}-{}{}-o{}{}",
            self.tables,
            self.active,
            self.workload.name(),
            if self.rotate { "-rotate" } else { "" },
            self.binlog_mib,
            if self.pad == Pad::Small { "s" } else { "" }
        )
    }

    fn active_count(&self) -> usize {
        ((self.tables as f64 * f64::from(self.active) / 100.0).round() as usize)
            .clamp(1, self.tables)
    }

    /// How long the load runs: the base duration, or for a rotating set
    /// long enough to activate half the initially inactive tables (at least
    /// 6 intervals); for a capacity sweep, every step.
    fn duration(&self, base: Duration) -> Duration {
        match self.workload {
            Workload::Capacity => STEP * MAX_STEPS,
            _ if self.rotate => {
                let active = self.active_count() as f64;
                let inactive = self.tables as f64 - active;
                let per = (active * ROTATE_SHARE).max(1.0);
                let intervals = ((0.5 * inactive / per).ceil() as u32).max(6);
                base.max(ROTATE_EVERY * intervals)
            }
            _ => base,
        }
    }
}

/// A deterministic generator (SplitMix64).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    fn shuffle<T>(&mut self, v: &mut [T]) {
        for i in (1..v.len()).rev() {
            v.swap(i, self.below(i + 1));
        }
    }
}

fn table_name(i: usize) -> String {
    format!(
        "scale_{:04}.t{:03}",
        i / TABLES_PER_DATABASE,
        i % TABLES_PER_DATABASE
    )
}

async fn root(infra: &Infra) -> Result<mysql_async::Conn> {
    Ok(mysql_async::Conn::from_url(infra.mysql_dsn().await).await?)
}

async fn loadgen(infra: &Infra) -> Result<mysql_async::Conn> {
    let port = infra.mysql.get_host_port_ipv4(3306).await?;
    Ok(mysql_async::Conn::from_url(format!(
        "mysql://{LOADGEN}:{PW}@127.0.0.1:{port}/"
    ))
    .await?)
}

/// Build the fixture of `tables` tables (replacing any other), and the load
/// writers' user. Returns the seconds it took.
pub async fn fixture(infra: &Infra, tables: usize) -> Result<f64> {
    let t0 = Instant::now();
    let mut c = root(infra).await?;
    let old: Vec<String> = c
        .query(
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE 'scale\\_%'",
        )
        .await?;
    for db in old {
        c.query_drop(format!("DROP DATABASE `{db}`")).await?;
    }
    c.query_drop(format!(
        "CREATE USER IF NOT EXISTS '{LOADGEN}'@'%' IDENTIFIED BY '{PW}'"
    ))
    .await?;
    c.query_drop(format!("GRANT INSERT ON *.* TO '{LOADGEN}'@'%'"))
        .await?;
    // The source commits without syncing its logs, so the load writers can
    // offer the study's rates; what either system reads is unchanged.
    c.query_drop("SET GLOBAL sync_binlog = 0").await?;
    c.query_drop("SET GLOBAL innodb_flush_log_at_trx_commit = 2")
        .await?;
    // Padding for the binary log's starting offset, in a database neither
    // system captures (outside `scale_*`).
    c.query_drop("CREATE DATABASE IF NOT EXISTS perfpad")
        .await?;
    c.query_drop("CREATE TABLE IF NOT EXISTS perfpad.p (id BIGINT AUTO_INCREMENT PRIMARY KEY, b LONGBLOB)").await?;
    c.query_drop("CREATE TABLE IF NOT EXISTS perfpad.s (id BIGINT AUTO_INCREMENT PRIMARY KEY, a INT, b VARCHAR(32), c DOUBLE)").await?;
    for d in 0..tables.div_ceil(TABLES_PER_DATABASE) {
        c.query_drop(format!("CREATE DATABASE scale_{d:04}"))
            .await?;
    }
    c.disconnect().await?;
    // Tables on 8 connections (aborted if the build is cancelled).
    let mut guard = Tasks::default();
    let mut tasks = Vec::new();
    for w in 0..8 {
        let mut c = root(infra).await?;
        tasks.push(tokio::spawn(async move {
            for i in (w..tables).step_by(8) {
                c.query_drop(format!(
                    "CREATE TABLE {} (id BIGINT PRIMARY KEY, a INT, b VARCHAR(32), c DOUBLE)",
                    table_name(i)
                ))
                .await?;
            }
            c.disconnect().await?;
            Ok::<_, anyhow::Error>(())
        }));
        guard.add(tasks.last().unwrap());
    }
    for t in tasks {
        t.await??;
    }
    Ok(t0.elapsed().as_secs_f64())
}

/// A row's commit.
#[derive(Clone, Copy)]
struct Commit {
    id: i64,
    table: u32,
    at: Instant,
}

/// What arrived: per id the first arrival, duplicates, order violations
/// (a table's id below one already seen), rows from outside the run's ids,
/// bytes, and the next offset to read (the topic has one partition).
#[derive(Default)]
struct Seen {
    first: HashMap<i64, Instant>,
    last_id: HashMap<u32, i64>,
    duplicates: u64,
    out_of_order: u64,
    unknown_table: u64,
    foreign: u64,
    bytes: u64,
    first_marker: Option<Instant>,
    markers: std::collections::HashSet<i64>,
    next_offset: i64,
}

fn consume(
    topic: &str,
    id_base: i64,
    tables: Arc<HashMap<String, u32>>,
    seen: Arc<Mutex<Seen>>,
    stop: Arc<AtomicBool>,
) -> Result<tokio::task::JoinHandle<()>> {
    let c: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .set("group.id", format!("scale-{topic}"))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("fetch.wait.max.ms", "10")
        .create()?;
    c.subscribe(&[topic])?;
    Ok(tokio::spawn(async move {
        while !stop.load(Ordering::Relaxed) {
            let m = match tokio::time::timeout(
                Duration::from_millis(200),
                c.recv(),
            )
            .await
            {
                Ok(Ok(m)) => m,
                Ok(Err(_)) => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
                Err(_) => continue,
            };
            let now = Instant::now();
            let mut s = seen.lock().unwrap();
            s.next_offset = s.next_offset.max(m.offset() + 1);
            let Some(payload) = m.payload() else { continue };
            s.bytes += (payload.len() + m.key().map_or(0, <[u8]>::len)) as u64;
            let Ok(v) = serde_json::from_slice::<Value>(payload) else {
                continue;
            };
            // A 64-bit integer may be rendered exactly, as a decimal string.
            let id = match &v["after"]["id"] {
                Value::Number(n) => n.as_i64(),
                Value::String(t) => t.parse().ok(),
                _ => None,
            };
            let Some(id) = id else { continue };
            // A row of another run (an earlier run's events, if any were
            // still readable) fails the run.
            if id < id_base || id >= id_base + RUN_IDS {
                s.foreign += 1;
                continue;
            }
            if id % RUN_IDS >= MARKER {
                s.first_marker.get_or_insert(now);
                s.markers.insert(id);
                continue;
            }
            let name = format!(
                "{}.{}",
                v["source"]["db"].as_str().unwrap_or_default(),
                v["source"]["table"].as_str().unwrap_or_default()
            );
            let Some(&table) = tables.get(&name) else {
                s.unknown_table += 1;
                continue;
            };
            if s.first.contains_key(&id) {
                s.duplicates += 1;
                continue;
            }
            s.first.insert(id, now);
            let last = s.last_id.entry(table).or_insert(i64::MIN);
            if id < *last {
                s.out_of_order += 1;
            } else {
                *last = id;
            }
        }
    }))
}

/// DeltaForge's counters of the study, summed over labels. Its histograms
/// are exported as summaries: their `_sum` and `_count` series are read
/// (the bare name carries quantiles).
const DF_COUNTERS: [&str; 25] = [
    "deltaforge_checkpoints_total",
    "deltaforge_kafka_requests_total",
    "deltaforge_sink_bytes_total",
    "deltaforge_source_events_total",
    "deltaforge_source_schema_cache_misses_total",
    "deltaforge_schema_sensing_seconds_sum",
    // Proof requests (one per first-touched table, among others) and their
    // cost: the bounded capture (lock wait, the connection's wait for its
    // lock, and the schema query) and the binary-log scan.
    "deltaforge_mysql_proof_requests_total",
    "deltaforge_mysql_proof_capture_lock_wait_seconds_sum",
    "deltaforge_mysql_proof_capture_lock_wait_seconds_count",
    "deltaforge_mysql_proof_capture_schema_seconds_sum",
    "deltaforge_mysql_proof_capture_retries_total",
    "deltaforge_mysql_proof_scan_bytes_sum",
    "deltaforge_mysql_proof_scan_events_sum",
    "deltaforge_mysql_proof_scan_seconds_sum",
    "deltaforge_mysql_proof_scan_seconds_count",
    "deltaforge_mysql_proof_scan_seek_seconds_sum",
    "deltaforge_mysql_proof_scan_setup_seconds_sum",
    "deltaforge_mysql_activation_windows_total",
    "deltaforge_mysql_selection_cache_evictions_total",
    // Measured failures (`DF_FAILURES`).
    "deltaforge_mysql_control_permit_timeouts_total",
    "deltaforge_mysql_control_saturation_deferrals_total",
    "deltaforge_mysql_activation_window_deferred_total",
    "deltaforge_mysql_activation_window_overflow_total",
    "deltaforge_mysql_proof_rounds_failed_total",
    "deltaforge_mysql_capture_kill_failures_total",
];

async fn df_counters() -> Option<HashMap<&'static str, f64>> {
    let text = reqwest::get(format!("http://{METRICS}/metrics"))
        .await
        .ok()?
        .text()
        .await
        .ok()?;
    Some(
        DF_COUNTERS
            .iter()
            .map(|name| {
                let v = text
                    .lines()
                    .filter(|l| {
                        l.strip_prefix(name).is_some_and(|rest| {
                            rest.starts_with('{') || rest.starts_with(' ')
                        })
                    })
                    .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
                    .sum();
                (*name, v)
            })
            .collect(),
    )
}

/// MySQL's global counters of the study.
const MYSQL_STATUS: [&str; 6] = [
    "Innodb_rows_read",
    "Bytes_sent",
    "Com_select",
    "Com_show_fields",
    "Opened_tables",
    "Opened_table_definitions",
];

async fn mysql_status(
    c: &mut mysql_async::Conn,
) -> Result<HashMap<String, f64>> {
    let rows: Vec<(String, String)> = c
        .query(format!(
            "SHOW GLOBAL STATUS WHERE Variable_name IN ({})",
            MYSQL_STATUS
                .iter()
                .map(|n| format!("'{n}'"))
                .collect::<Vec<_>>()
                .join(",")
        ))
        .await?;
    Ok(rows
        .into_iter()
        .filter_map(|(k, v)| Some((k, v.parse().ok()?)))
        .collect())
}

/// The binary logs' total size, bytes.
async fn binlog_bytes(c: &mut mysql_async::Conn) -> Result<u64> {
    let rows: Vec<mysql_async::Row> = c.query("SHOW BINARY LOGS").await?;
    Ok(rows
        .iter()
        .filter_map(|r| r.get::<u64, _>("File_size"))
        .sum())
}

/// The root connections other than this one: the system's, since the load
/// writers connect as `loadgen`.
async fn system_connections(c: &mut mysql_async::Conn) -> Result<u64> {
    Ok(c
        .query_first(
            "SELECT COUNT(*) FROM information_schema.processlist WHERE user = 'root' AND id <> CONNECTION_ID()",
        )
        .await?
        .unwrap_or(0))
}

/// The binary log's current file and byte offset.
async fn binlog_position(c: &mut mysql_async::Conn) -> Result<(String, u64)> {
    let row: mysql_async::Row = c
        .query_first("SHOW BINARY LOG STATUS")
        .await?
        .ok_or_else(|| anyhow::anyhow!("no binary log status"))?;
    Ok((
        row.get::<String, _>("File").unwrap_or_default(),
        row.get::<u64, _>("Position").unwrap_or_default(),
    ))
}

/// Start a fresh binary log file (purging the older ones: no system runs
/// between runs) and pad it to `mib` MiB with rows of a database neither
/// system captures. Returns the starting file and offset.
async fn position_binlog(
    infra: &Infra,
    c: &mut mysql_async::Conn,
    mib: u64,
    pad: Pad,
) -> Result<(String, u64)> {
    c.query_drop("FLUSH BINARY LOGS").await?;
    let (file, _) = binlog_position(c).await?;
    c.query_drop(format!("PURGE BINARY LOGS TO '{file}'"))
        .await?;
    let target = mib << 20;
    if pad == Pad::Small && target > 0 {
        // Single-row transactions on the writers' connections until the
        // offset is reached.
        let done = Arc::new(AtomicBool::new(false));
        let mut guard = Tasks::default();
        let mut tasks = Vec::new();
        for _ in 0..WRITERS {
            let mut w = loadgen(infra).await?;
            let done = Arc::clone(&done);
            tasks.push(tokio::spawn(async move {
                let mut k = 0u64;
                while !done.load(Ordering::Relaxed) {
                    k += 1;
                    w.query_drop(format!(
                        "INSERT INTO perfpad.s (a, b, c) VALUES ({k}, 'r{k}', {k}.5)"
                    ))
                    .await?;
                }
                w.disconnect().await.ok();
                Ok::<_, anyhow::Error>(())
            }));
            guard.add(tasks.last().unwrap());
        }
        let outcome = loop {
            let (f, pos) = binlog_position(c).await?;
            if f != file {
                break Err(anyhow::anyhow!(
                    "the binary log rotated while padding to {mib} MiB"
                ));
            }
            if pos >= target {
                break Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        done.store(true, Ordering::Relaxed);
        for t in tasks {
            t.await??;
        }
        outcome?;
        c.query_drop("TRUNCATE TABLE perfpad.s").await?;
        return binlog_position(c).await;
    }
    const CHUNK: u64 = 8 << 20;
    loop {
        let (f, pos) = binlog_position(c).await?;
        if f != file {
            bail!("the binary log rotated while padding to {mib} MiB");
        }
        // A row event is the blob plus about 100 bytes of headers.
        let left = target.saturating_sub(pos);
        if left < 4096 {
            break;
        }
        let n = left.min(CHUNK).saturating_sub(1024).max(1);
        c.query_drop(format!(
            "INSERT INTO perfpad.p (b) VALUES (REPEAT('x', {n}))"
        ))
        .await?;
    }
    c.query_drop("TRUNCATE TABLE perfpad.p").await?;
    binlog_position(c).await
}

/// DeltaForge's proof trace (`deltaforge::proof_trace`, on its standard
/// output): proofs, scans, total seek, and the seek by decile of the order
/// in which the proofs ran.
fn proof_trace(stdout: &Path) -> Value {
    let Ok(text) = std::fs::read_to_string(stdout) else {
        return Value::Null;
    };
    let (mut proofs, mut seeks) = (0u64, Vec::new());
    let mut outcomes: HashMap<String, u64> = HashMap::new();
    // Tables handed on after their window committed (activated).
    let mut released = std::collections::HashSet::new();
    // Each activation window: its size beside its phases and state-store
    // commit, so the commit reads per table.
    let mut windows = Vec::new();
    for line in text.lines() {
        let Some(i) = line.find("{\"record\"") else {
            continue;
        };
        let Ok(r) = serde_json::from_str::<Value>(&line[i..]) else {
            continue;
        };
        match r["record"].as_str() {
            Some("proof") => {
                proofs += 1;
                *outcomes
                    .entry(r["outcome"].as_str().unwrap_or("?").to_string())
                    .or_default() += 1;
            }
            Some("scan") => seeks.push(r["seek_ms"].as_f64().unwrap_or(0.0)),
            Some("release") => {
                released.insert(format!(
                    "{}.{}",
                    r["db"].as_str().unwrap_or_default(),
                    r["table"].as_str().unwrap_or_default()
                ));
            }
            Some("window") => windows.push(json!({
                "tables": r["tables"], "events": r["events"],
                "bytes": r["bytes"], "cut": r["cut"],
                "closed_by": r["closed_by"], "prefetched": r["prefetched"],
                "read_ms": r["read_ms"], "capture_ms": r["capture_ms"],
                "capture_lock_ms": r["capture_lock_ms"],
                "capture_schema_ms": r["capture_schema_ms"],
                "scan_ms": r["scan_ms"], "commit_ms": r["commit_ms"],
                "batch_conflicts": r["batch_conflicts"],
                "batch_applies": r["batch_applies"],
                "capture_attempts": r["capture_attempts"],
                "stream_openings": r["stream_openings"],
                "extensions_committed": r["extensions_committed"],
            })),
            _ => {}
        }
    }
    let deciles: Vec<Value> = (0..10)
        .map(|d| {
            let (a, b) = (seeks.len() * d / 10, seeks.len() * (d + 1) / 10);
            json!({"decile": d, "seek_ms": quantiles(seeks[a..b].to_vec())})
        })
        .collect();
    json!({
        "released_tables": released.len(),
        "window_summary": window_summary(&windows),
        "proofs": proofs, "outcomes": outcomes, "scans": seeks.len(),
        "seek_total_ms": seeks.iter().sum::<f64>(),
        "seek_ms": quantiles(seeks.clone()),
        "seek_by_decile": deciles,
        "windows": windows,
    })
}

/// The windows of a run: their number, tables per window, atomic plans
/// (applies plus conflicts), cuts, the largest buffered events, and commit
/// time.
fn window_summary(windows: &[Value]) -> Value {
    let num = |w: &Value, k: &str| w[k].as_f64().unwrap_or(0.0);
    let plans: Vec<f64> = windows
        .iter()
        .map(|w| num(w, "batch_applies") + num(w, "batch_conflicts"))
        .collect();
    json!({
        "windows": windows.len(),
        "tables": windows.iter().map(|w| num(w, "tables")).sum::<f64>(),
        "tables_per_window": quantiles(windows.iter().map(|w| num(w, "tables")).collect()),
        "plans": plans.iter().sum::<f64>(),
        "plans_per_window": quantiles(plans),
        "cuts": windows.iter().filter(|w| w["cut"] == json!(true)).count(),
        "bytes_max": windows.iter().map(|w| num(w, "bytes")).fold(0.0, f64::max),
        "commit_ms": quantiles(windows.iter().map(|w| num(w, "commit_ms")).collect()),
    })
}

fn quantiles(mut v: Vec<f64>) -> Value {
    if v.is_empty() {
        return Value::Null;
    }
    v.sort_by(f64::total_cmp);
    let q = |p: f64| v[((v.len() as f64 - 1.0) * p) as usize];
    json!({"n": v.len(), "p50": q(0.5), "p95": q(0.95), "p99": q(0.99), "max": v[v.len() - 1]})
}

/// The system of a run.
pub enum Sys<'a> {
    Df(&'a Path),
    Dbz(&'a debezium::Connect, debezium::Profile),
}

/// One run of `point` for `sys` on the built fixture. `seq` makes its ids,
/// names and topic its own.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    infra: &Infra,
    sys: &Sys<'_>,
    point: &Point,
    seq: u64,
    tag: &str,
    dir: &Path,
    base: Duration,
    catchup_phase: bool,
) -> Result<Value> {
    std::fs::create_dir_all(dir)?;
    let name = format!("s-{tag}");
    let topic = format!("scale-{tag}");
    let id_base = seq as i64 * RUN_IDS;
    create_topic(&topic).await?;
    let names: Arc<HashMap<String, u32>> = Arc::new(
        (0..point.tables)
            .map(|i| (table_name(i), i as u32))
            .collect(),
    );
    let seen = Arc::new(Mutex::new(Seen::default()));
    let stop = Arc::new(AtomicBool::new(false));
    let mut tasks = Tasks::default();
    let consumer = consume(
        &topic,
        id_base,
        Arc::clone(&names),
        Arc::clone(&seen),
        Arc::clone(&stop),
    )?;
    tasks.add(&consumer);

    // The active set: the same for every run of the point.
    let mut order: Vec<usize> = (0..point.tables).collect();
    Rng(point.tables as u64 * 1_000 + u64::from(point.active))
        .shuffle(&mut order);
    let n_active = point.active_count();
    let mut active: Vec<usize> = order[..n_active].to_vec();
    let mut inactive: Vec<usize> = order[n_active..].to_vec();

    let mut mon = root(infra).await?;
    let datadir_before = datadir_bytes(infra).await;
    let binlog_start =
        position_binlog(infra, &mut mon, point.binlog_mib, point.pad).await?;
    let gtid_start: String = mon
        .query_first("SELECT @@GLOBAL.gtid_executed")
        .await?
        .unwrap_or_default();
    let status0 = mysql_status(&mut mon).await?;
    let binlog0 = binlog_bytes(&mut mon).await?;
    let db_cpu0 = infra.db_cpu_usec(crate::Engine::Mysql);

    // Start.
    let t_create = Instant::now();
    // On the PostgreSQL store, a database of the run's own.
    let state_db = match sys {
        Sys::Df(_) if infra.state.is_some() => {
            let db = format!("st_scale_{seq}");
            infra
                .state_exec("postgres", &format!("CREATE DATABASE {db}"))
                .await?;
            Some(db)
        }
        _ => None,
    };
    let state_dsn = match &state_db {
        Some(db) => infra.state_dsn(db).await,
        None => None,
    };
    let process = match sys {
        Sys::Df(binary) => {
            let p = Process::start(
                binary,
                &dir.join("state"),
                state_dsn.as_deref(),
            )
            .await?;
            let spec = json!({
                "apiVersion": "deltaforge/v1",
                "kind": "Pipeline",
                "metadata": {"name": name, "tenant": "perf"},
                "spec": {
                    "source": {"type": "mysql", "config": {
                        "id": name, "dsn": infra.mysql_dsn().await,
                        "tables": ["scale_*.*"], "snapshot": {"mode": "never"}}},
                    "processors": [],
                    "sinks": [{"type": "kafka", "config": {
                        "id": "kafka", "brokers": brokers(), "topic": topic,
                        "envelope": {"type": "native"}, "encoding": "json",
                        "required": true}}],
                },
            });
            std::fs::write(
                dir.join("pipeline.json"),
                serde_json::to_vec_pretty(&spec)?,
            )?;
            api(reqwest::Method::POST, "/pipelines", Some(&spec)).await?;
            wait_status(&name, "running").await?;
            Some(p)
        }
        Sys::Dbz(c, profile) => {
            static SERVER_IDS: std::sync::atomic::AtomicU32 =
                std::sync::atomic::AtomicU32::new(20_000);
            let conn = debezium::scale_connector(
                &name,
                tag,
                MYSQL_NAME,
                PW,
                &bootstrap_internal(),
                SERVER_IDS.fetch_add(1, Ordering::Relaxed),
                "scale_.*",
                &topic,
                *profile,
            );
            std::fs::write(
                dir.join("connector.json"),
                serde_json::to_vec_pretty(&conn)?,
            )?;
            c.create(&conn).await?;
            c.wait(&name, "RUNNING").await?;
            None
        }
    };
    let ready_s = t_create.elapsed().as_secs_f64();
    let cpu0 = sys_cpu(sys, process.as_ref());
    let dbz0 = match sys {
        Sys::Dbz(c, _) => c.scale_counters(),
        Sys::Df(_) => None,
    };
    let df0 = df_counters().await;

    // First event: a marker row (in the first active table) until one
    // arrives. Markers use the load writers' user.
    let mut lg = loadgen(infra).await?;
    let mut markers = 0i64;
    let first_event_s = loop {
        markers += 1;
        lg.query_drop(format!(
            "INSERT INTO {} (id, a, b, c) VALUES ({}, 0, 'marker', 0)",
            table_name(active[0]),
            id_base + MARKER + markers
        ))
        .await?;
        let wait = Instant::now() + Duration::from_secs(5);
        while Instant::now() < wait
            && seen.lock().unwrap().first_marker.is_none()
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        if let Some(at) = seen.lock().unwrap().first_marker {
            break at.duration_since(t_create).as_secs_f64();
        }
        if t_create.elapsed() > Duration::from_secs(180) {
            bail!("no event within 180 s of the start");
        }
    };

    // The load: a scheduler at the offered rate picks an active table for
    // each change and hands it to the table's writer (table `i` to writer
    // `i % WRITERS`), so each table's ids are committed in order.
    let sets = Arc::new(RwLock::new(active.clone()));
    let commits = Arc::new(Mutex::new(Vec::<Commit>::new()));
    let offered = Arc::new(AtomicU64::new(0));
    let duration = point.duration(base);
    let t_load = Instant::now();
    let load_end = t_load + duration;
    let counters = match (sys, process.as_ref()) {
        (Sys::Df(_), Some(p))
            if PERF_COUNTERS.get().copied().unwrap_or(false) =>
        {
            let pid = p.pid();
            let t = tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(15)).await;
                perf_counters(pid, 30).await
            });
            tasks.add(&t);
            Some(t)
        }
        _ => None,
    };
    let halt = Arc::new(AtomicBool::new(false));
    let mut writers = Vec::new();
    let mut queues = Vec::new();
    for _ in 0..WRITERS {
        let mut c = loadgen(infra).await?;
        let (tx, mut rx) =
            tokio::sync::mpsc::unbounded_channel::<(i64, usize)>();
        queues.push(tx);
        let commits = Arc::clone(&commits);
        let halt = Arc::clone(&halt);
        writers.push(tokio::spawn(async move {
            while let Some((id, table)) = rx.recv().await {
                if halt.load(Ordering::Relaxed) {
                    break;
                }
                let k = id - id_base;
                c.query_drop(format!(
                    "INSERT INTO {} (id, a, b, c) VALUES ({id}, {k}, 'r{k}', {k}.5)",
                    table_name(table)
                ))
                .await?;
                commits.lock().unwrap().push(Commit {
                    id,
                    table: table as u32,
                    at: Instant::now(),
                });
            }
            c.disconnect().await.ok();
            Ok::<_, anyhow::Error>(())
        }));
        tasks.add(writers.last().unwrap());
    }
    let scheduler = {
        let sets = Arc::clone(&sets);
        let halt = Arc::clone(&halt);
        let workload = point.workload;
        tokio::spawn(async move {
            let mut rng = Rng(seq * 1_000 + 1);
            let mut k: i64 = 0;
            let mut due = Duration::ZERO;
            while !halt.load(Ordering::Relaxed) {
                let Some(rate) = workload.rate(due) else {
                    // Idle: to the next burst.
                    due = Duration::from_secs_f64(
                        (due.as_secs_f64() / 5.0).floor() * 5.0 + 5.0,
                    );
                    continue;
                };
                due += Duration::from_secs_f64(1.0 / rate);
                if t_load + due >= load_end {
                    break;
                }
                if Instant::now() < t_load + due {
                    tokio::time::sleep_until((t_load + due).into()).await;
                }
                let table = {
                    let s = sets.read().unwrap();
                    s[rng.below(s.len())]
                };
                k += 1;
                if queues[table % WRITERS].send((id_base + k, table)).is_err() {
                    break;
                }
            }
        })
    };
    tasks.add(&scheduler);
    // Offered changes: the schedule's count, for the generator check.
    {
        let mut t = Duration::ZERO;
        let mut n = 0u64;
        while t < duration {
            if let Some(r) = point.workload.rate(t) {
                n += (r / 10.0) as u64;
            }
            t += Duration::from_millis(100);
        }
        offered.store(n, Ordering::Relaxed);
    }

    // Rotation, catch-up and sampling while the load runs.
    let mut activated: HashMap<u32, Instant> =
        active.iter().map(|&t| (t as u32, t_load)).collect();
    let mut rot_rng = Rng(seq * 7_919 + 1);
    let mut next_rotation = t_load + ROTATE_EVERY;
    let mut rotations = 0u32;
    let catchup = catchup_phase && point.workload != Workload::Capacity;
    let (mut stopped_at, mut resumed_at) = (None, None);
    let mut conns_max = 0u64;
    let mut rss_max = 0u64;
    // DeltaForge's sampled gauges, each at its largest sample.
    let mut gauge_peaks: HashMap<String, f64> = HashMap::new();
    let is_df = matches!(sys, Sys::Df(_));
    let mut binlog_peak = binlog0;
    let mut step_results = Vec::new();
    let mut capacity_stop = None;
    while Instant::now() < load_end && !halt.load(Ordering::Relaxed) {
        let now = Instant::now();
        if point.rotate && now >= next_rotation {
            let n = ((active.len() as f64) * ROTATE_SHARE).round().max(1.0)
                as usize;
            for _ in 0..n.min(inactive.len()) {
                let out = active.swap_remove(rot_rng.below(active.len()));
                let into = inactive.swap_remove(rot_rng.below(inactive.len()));
                active.push(into);
                inactive.push(out);
                activated.insert(into as u32, now);
            }
            *sets.write().unwrap() = active.clone();
            rotations += 1;
            next_rotation += ROTATE_EVERY;
        }
        if catchup && stopped_at.is_none() && now >= t_load + CATCHUP_AT {
            control(sys, &name, "stop").await?;
            stopped_at = Some(Instant::now());
        }
        if let Some(s) = stopped_at
            && resumed_at.is_none()
            && now >= s + CATCHUP_FOR
        {
            control(sys, &name, "resume").await?;
            resumed_at = Some(Instant::now());
        }
        // A capacity step is judged 5 s after it ends.
        if point.workload == Workload::Capacity {
            let judged = step_results.len() as u32;
            let step_end = t_load + STEP * (judged + 1);
            if now >= step_end + LIMIT_LATENCY && judged < MAX_STEPS {
                let r =
                    judge_step(&commits, &seen, point.workload, t_load, judged);
                let ok = r["sustained"].as_bool().unwrap_or(false);
                step_results.push(r);
                if !ok {
                    capacity_stop = Some(judged);
                    halt.store(true, Ordering::Relaxed);
                }
            }
        }
        conns_max =
            conns_max.max(system_connections(&mut mon).await.unwrap_or(0));
        rss_max = rss_max.max(sys_rss(sys, process.as_ref()));
        if is_df && let Some(g) = df_gauges().await {
            keep_peaks(&mut gauge_peaks, g);
        }
        binlog_peak =
            binlog_peak.max(binlog_bytes(&mut mon).await.unwrap_or(0));
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    halt.store(true, Ordering::Relaxed);
    let t_load_end = Instant::now();
    // A load that ends during the catch-up stop still resumes the system,
    // so that the drain can complete.
    if stopped_at.is_some() && resumed_at.is_none() {
        control(sys, &name, "resume").await?;
        resumed_at = Some(Instant::now());
    }
    scheduler.await.ok();
    let mut writer_error = None;
    for w in writers {
        if let Err(e) = w.await? {
            writer_error.get_or_insert(e.to_string());
        }
    }
    let commits = std::mem::take(&mut *commits.lock().unwrap());

    // Drain (`results/scale-stage2-plan.md`): every committed row arrived
    // and the system's durable frontier covers MySQL's final GTID set. A
    // timeout fails the run.
    let final_gtid: String = mon
        .query_first("SELECT @@GLOBAL.gtid_executed")
        .await?
        .unwrap_or_default();
    // One marker row after it, in both systems' runs: Debezium's committed
    // offset stops before the last transaction it processed (a restart
    // repeats it with skip counts), so its frontier covers every load
    // transaction once a later transaction has been processed and its
    // offset committed. Markers are outside the ledger.
    markers += 1;
    lg.query_drop(format!(
        "INSERT INTO {} (id, a, b, c) VALUES ({}, 0, 'marker', 0)",
        table_name(active[0]),
        id_base + MARKER + markers
    ))
    .await?;
    let drain_marker = id_base + MARKER + markers;
    let t_drain = Instant::now();
    let mut last_frontier = Value::Null;
    let mut drain_error: Option<String> = None;
    let mut durable_frontier = Value::Null;
    let mut connector_stopped = false;
    let mut delivered_s = None;
    loop {
        let (missing, marker) = {
            let s = seen.lock().unwrap();
            let missing = commits
                .iter()
                .filter(|c| !s.first.contains_key(&c.id))
                .count();
            (missing, s.markers.contains(&drain_marker))
        };
        if missing == 0 {
            delivered_s.get_or_insert(t_drain.elapsed().as_secs_f64());
        }
        if missing == 0 && marker {
            match frontier(
                sys,
                &name,
                &final_gtid,
                &mut mon,
                &mut connector_stopped,
            )
            .await
            {
                Ok((true, f)) => {
                    durable_frontier = f;
                    break;
                }
                Ok((false, seen)) => last_frontier = seen,
                Err(e) => {
                    drain_error = Some(format!("drain: {e:#}"));
                    break;
                }
            }
        }
        if t_drain.elapsed() > DRAIN_TIMEOUT {
            drain_error = Some(format!(
                "drain timed out after {DRAIN_TIMEOUT:?}: {missing} rows missing, durable frontier covered: false (last seen: {last_frontier})"
            ));
            break;
        }
        conns_max =
            conns_max.max(system_connections(&mut mon).await.unwrap_or(0));
        rss_max = rss_max.max(sys_rss(sys, process.as_ref()));
        if is_df && let Some(g) = df_gauges().await {
            keep_peaks(&mut gauge_peaks, g);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let frontier_s = t_drain.elapsed().as_secs_f64();

    let cpu = sys_cpu(sys, process.as_ref()) - cpu0;
    let perf = match counters {
        Some(task) => match task.await {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => json!({"error": format!("{e:#}")}),
            Err(e) => json!({"error": e.to_string()}),
        },
        None => Value::Null,
    };
    let df1 = df_counters().await;
    let (dbz1, history) = match sys {
        Sys::Dbz(c, _) => (
            c.scale_counters(),
            Some(json!({
                "bytes": debezium::Connect::topic_bytes(KAFKA_NAME, &format!("dbz-history-{tag}")),
            })),
        ),
        Sys::Df(_) => (None, None),
    };
    let status1 = mysql_status(&mut mon).await?;
    let binlog_end = binlog_position(&mut mon).await?;
    let binlog1 = binlog_bytes(&mut mon).await?;
    let db_cpu = match (db_cpu0, infra.db_cpu_usec(crate::Engine::Mysql)) {
        (Some(a), Some(b)) => Some((b - a) as f64 / 1e6),
        _ => None,
    };

    // Shut the system down, then read the topic to its end: the ledger is
    // checked over everything delivered, through the shutdown.
    let allocs = match (sys, process) {
        (Sys::Df(_), Some(p)) => {
            api(reqwest::Method::DELETE, &format!("/pipelines/{name}"), None)
                .await
                .ok();
            p.stop().await
        }
        (Sys::Dbz(c, _), _) => {
            c.delete(&name).await;
            None
        }
        _ => None,
    };
    let end_offset = topic_end(&topic).await;
    let t_consumer = Instant::now();
    match end_offset {
        Ok(end) => {
            while seen.lock().unwrap().next_offset < end {
                if t_consumer.elapsed() > CONSUMER_DRAIN_TIMEOUT {
                    drain_error.get_or_insert(format!(
                        "the consumer did not reach the topic's end ({end}) within {CONSUMER_DRAIN_TIMEOUT:?}"
                    ));
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        Err(e) => {
            drain_error.get_or_insert(format!("the topic's end: {e:#}"));
        }
    }
    stop.store(true, Ordering::Relaxed);
    consumer.await.ok();
    // DeltaForge's proofs, from its trace (its standard output).
    let proofs = match sys {
        Sys::Df(_) => proof_trace(&dir.join("state").join("stderr.out")),
        Sys::Dbz(..) => Value::Null,
    };
    let store_statements = match &state_db {
        Some(db) => {
            let v = state_statements(infra, db).await;
            infra
                .state_exec(
                    "postgres",
                    &format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)"),
                )
                .await
                .ok();
            v
        }
        None => Value::Null,
    };
    let datadir_after = datadir_bytes(infra).await;
    mon.disconnect().await.ok();

    // Raw rows, for any later analysis: each committed row's id, table,
    // commit and arrival (ms since the load started; empty if it never
    // arrived).
    {
        use std::io::Write;
        let mut w = std::io::BufWriter::new(std::fs::File::create(
            dir.join("rows.csv"),
        )?);
        writeln!(w, "id,table,commit_ms,arrival_ms")?;
        let ms = |t: Instant| {
            t.saturating_duration_since(t_load).as_secs_f64() * 1e3
        };
        let s = seen.lock().unwrap();
        for c in &commits {
            match s.first.get(&c.id) {
                Some(a) => writeln!(
                    w,
                    "{},{},{:.3},{:.3}",
                    c.id,
                    c.table,
                    ms(c.at),
                    ms(*a)
                )?,
                None => writeln!(w, "{},{},{:.3},", c.id, c.table, ms(c.at))?,
            }
        }
        let markers = |t: Option<Instant>| {
            t.map_or(String::new(), |t| format!("{:.3}", ms(t)))
        };
        std::fs::write(
            dir.join("phases.json"),
            serde_json::to_vec_pretty(&json!({
                "load_s": t_load_end.duration_since(t_load).as_secs_f64(),
                "stopped_ms": markers(stopped_at), "resumed_ms": markers(resumed_at),
                "activated_ms": activated.iter().map(|(t, at)| (t.to_string(), json!(ms(*at)))).collect::<serde_json::Map<_, _>>(),
            }))?,
        )?;
    }

    // Analysis.
    let s = seen.lock().unwrap();
    let missing = commits
        .iter()
        .filter(|c| !s.first.contains_key(&c.id))
        .count();
    let mut first_of_table: HashMap<u32, i64> = HashMap::new();
    let mut by_activation: Vec<(&Commit, Instant)> = Vec::new();
    for c in &commits {
        // The first row of a table after its (latest) activation.
        if let Some(&act) = activated.get(&c.table)
            && c.at >= act
        {
            by_activation.push((c, act));
        }
    }
    by_activation.sort_by_key(|(c, _)| c.at);
    for (c, _) in &by_activation {
        first_of_table.entry(c.table).or_insert(c.id);
    }
    let catchup_s = resumed_at.and_then(|r| {
        commits
            .iter()
            .filter(|c| c.at < r)
            .filter_map(|c| s.first.get(&c.id))
            .max()
            .map(|a| a.saturating_duration_since(r).as_secs_f64())
    });
    // The catch-up phase: from the stop until every row committed before
    // the resume has arrived; rows committed in it queue behind the backlog.
    let caught_up = resumed_at
        .zip(catchup_s)
        .map(|(r, s)| r + Duration::from_secs_f64(s));
    let in_catchup = |c: &Commit| match (stopped_at, caught_up.or(resumed_at)) {
        // Committed, or arrived, in the phase.
        (Some(a), Some(b)) => {
            (c.at >= a && c.at < b)
                || s.first.get(&c.id).is_some_and(|t| *t >= a && *t < b)
        }
        _ => false,
    };
    let lat = |c: &Commit| {
        s.first
            .get(&c.id)
            .map(|a| a.saturating_duration_since(c.at).as_secs_f64() * 1e3)
    };
    let activation: Vec<f64> = commits
        .iter()
        .filter(|c| {
            first_of_table.get(&c.table) == Some(&c.id) && !in_catchup(c)
        })
        .filter_map(lat)
        .collect();
    let steady: Vec<f64> = commits
        .iter()
        .filter(|c| {
            first_of_table.get(&c.table) != Some(&c.id) && !in_catchup(c)
        })
        .filter_map(lat)
        .collect();
    // Activation latency in order of activation, by decile: whether the
    // first-touch cost grows as more tables become active within a run.
    let mut firsts: Vec<&Commit> = commits
        .iter()
        .filter(|c| {
            first_of_table.get(&c.table) == Some(&c.id) && !in_catchup(c)
        })
        .collect();
    firsts.sort_by_key(|c| c.at);
    let deciles: Vec<Value> = (0..10)
        .map(|d| {
            let (a, b) = (firsts.len() * d / 10, firsts.len() * (d + 1) / 10);
            json!({"decile": d, "latency_ms": quantiles(firsts[a..b].iter().filter_map(|c| lat(c)).collect())})
        })
        .collect();
    let catchup_lat: Vec<f64> = commits
        .iter()
        .filter(|c| in_catchup(c))
        .filter_map(lat)
        .collect();
    let last_arrival = commits
        .iter()
        .filter_map(|c| s.first.get(&c.id))
        .max()
        .copied();
    let drain_s = last_arrival
        .map(|a| a.saturating_duration_since(t_load_end).as_secs_f64());
    let load_secs = t_load_end.duration_since(t_load).as_secs_f64();
    let offered_n = offered.load(Ordering::Relaxed) as f64;
    let generator_share = if point.workload == Workload::Capacity {
        None
    } else {
        Some(commits.len() as f64 / offered_n.max(1.0))
    };
    let delta = |a: &Option<HashMap<&'static str, f64>>,
                 b: &Option<HashMap<&'static str, f64>>|
     -> Value {
        match (a, b) {
            (Some(a), Some(b)) => DF_COUNTERS
                .iter()
                .map(|k| (k.to_string(), json!(b[k] - a[k])))
                .collect::<serde_json::Map<_, _>>()
                .into(),
            _ => Value::Null,
        }
    };
    let mysql_delta: serde_json::Map<String, Value> = MYSQL_STATUS
        .iter()
        .map(|k| {
            (
                k.to_string(),
                json!(
                    status1.get(*k).unwrap_or(&0.0)
                        - status0.get(*k).unwrap_or(&0.0)
                ),
            )
        })
        .collect();
    let binlog_written = binlog1.saturating_sub(binlog0);
    let df_delta = delta(&df0, &df1);
    let proof_amplification = df_delta["deltaforge_mysql_proof_scan_bytes_sum"]
        .as_f64()
        .map(|b| b / (binlog_written.max(1) as f64));
    let dbz_delta = match (dbz0, dbz1) {
        (Some(a), Some(b)) => json!({
            "kafka_requests": b[0] - a[0], "offset_records": b[1] - a[1],
            "bytes_produced": b[2] - a[2], "gc_count": b[3] - a[3], "gc_ms": b[4] - a[4],
        }),
        _ => Value::Null,
    };
    // Activation counts (`results/scale-stage2-plan.md`): the fixture's
    // tables are all selected; touched tables had a committed row; a
    // touched table whose first row arrived was activated (for DeltaForge
    // also counted from its trace's `release` records).
    let touched: std::collections::HashSet<u32> =
        commits.iter().map(|c| c.table).collect();
    let first_arrivals: Vec<Instant> = first_of_table
        .values()
        .filter_map(|id| s.first.get(id).copied())
        .collect();
    let time_to_last_activation_s = first_arrivals
        .iter()
        .max()
        .map(|a| a.saturating_duration_since(t_load).as_secs_f64());
    // After the catch-up resume: from the resume to the first row arriving,
    // and the latency of rows committed after it in tables activated before
    // the stop (the restart path, not first touches).
    let resume_to_first_arrival_s = resumed_at.and_then(|r| {
        s.first
            .values()
            .filter(|a| **a >= r)
            .min()
            .map(|a| a.saturating_duration_since(r).as_secs_f64())
    });
    let known_before_stop: std::collections::HashSet<u32> = match stopped_at {
        Some(st) => by_activation
            .iter()
            .filter(|(c, _)| {
                first_of_table.get(&c.table) == Some(&c.id) && c.at < st
            })
            .map(|(c, _)| c.table)
            .collect(),
        None => Default::default(),
    };
    let after_resume_known: Vec<f64> = match resumed_at {
        Some(r) => commits
            .iter()
            .filter(|c| c.at >= r && known_before_stop.contains(&c.table))
            .filter_map(lat)
            .collect(),
        None => Vec::new(),
    };
    let failures = if is_df {
        measured_failures(&df_delta)
    } else {
        Vec::new()
    };
    let mut failures = failures;
    if is_df
        && !touched.is_empty()
        && proofs["windows"].as_array().is_none_or(|w| w.is_empty())
    {
        failures.push("no proof trace (PERF_RUST_LOG must enable deltaforge::proof_trace)".into());
    }
    let ok = writer_error.is_none()
        && drain_error.is_none()
        && failures.is_empty()
        && missing == 0
        && s.duplicates == 0
        && s.out_of_order == 0
        && s.foreign == 0
        && s.unknown_table == 0
        && generator_share.is_none_or(|g| g >= LIMIT_SHARE);
    Ok(json!({
        "ok": ok,
        "error": writer_error.or(drain_error.clone()),
        "drain_error": drain_error,
        "measured_failures": failures,
        "generator_bound": generator_share.is_some_and(|g| g < LIMIT_SHARE),
        "tables": point.tables, "active": point.active, "active_tables": n_active,
        "workload": point.workload.name(), "rotate": point.rotate,
        "rotations": rotations, "activation_set": activated.len(),
        "catchup_phase": catchup,
        "ready_s": ready_s, "first_event_s": first_event_s,
        "load_s": load_secs, "offered": offered_n, "committed": commits.len(),
        "generator_share": generator_share,
        "achieved_per_s": commits.len() as f64 / load_secs,
        "missing": missing, "duplicates": s.duplicates, "out_of_order": s.out_of_order,
        "unknown_table": s.unknown_table, "foreign": s.foreign,
        "selected_tables": point.tables, "touched_tables": touched.len(),
        "activated_tables": first_arrivals.len(),
        "released_tables": proofs["released_tables"],
        "time_to_last_activation_s": time_to_last_activation_s,
        "resume_to_first_arrival_s": resume_to_first_arrival_s,
        "latency_after_resume_known_ms": quantiles(after_resume_known),
        "drain": {"delivered_s": delivered_s, "frontier_s": frontier_s,
                  "final_gtid": final_gtid, "durable_frontier": durable_frontier,
                  "consumer_end_s": t_consumer.elapsed().as_secs_f64()},
        "gtid_start": gtid_start,
        "datadir_bytes_before": datadir_before, "datadir_bytes_after": datadir_after,
        "gauge_peaks": gauge_peaks,
        "store_statements": store_statements,
        "latency_steady_ms": quantiles(steady),
        "latency_activation_ms": quantiles(activation),
        "latency_activation_by_decile": deciles,
        "latency_catchup_ms": quantiles(catchup_lat),
        "catchup_s": catchup_s, "drain_s": drain_s,
        "cpu_s": cpu, "cpu_cores": cpu / (load_secs + drain_s.unwrap_or(0.0)),
        "perf_counters": perf,
        "rss_peak_mib": rss_max as f64 / 1024.0,
        "allocations": allocs,
        "mysql_connections_max": conns_max,
        "state_store_connections": match sys { Sys::Df(_) if infra.state.is_some() => json!("PostgreSQL"), Sys::Df(_) => json!("none (SQLite)"), Sys::Dbz(..) => json!("Kafka (offsets topic)") },
        "mysql_cpu_s": db_cpu, "mysql": mysql_delta,
        "binlog_start": {"file": binlog_start.0, "offset": binlog_start.1},
        "binlog_end": {"file": binlog_end.0, "offset": binlog_end.1},
        "binlog_rotated": binlog_start.0 != binlog_end.0,
        "proof_trace": proofs,
        "binlog_written_bytes": binlog_written, "binlog_retained_bytes": binlog1, "binlog_peak_bytes": binlog_peak,
        "kafka_bytes_consumed": s.bytes,
        "deltaforge": df_delta, "proof_amplification": proof_amplification,
        "debezium": dbz_delta, "schema_history": history,
        "capacity_steps": step_results, "capacity_failed_step": capacity_stop,
    }))
}

/// Judge capacity step `step`, 5 s after it ends, by the declared limits:
/// rows delivered within the step at least 95% of the offered rate, p99
/// commit-to-arrival latency of its rows at most 5 s (a row not yet arrived
/// counts as later), and its last row arrived within 5 s of the step's end.
/// A step whose writers committed less than 95% of the offered rate is
/// generator-bound: the sweep ends there without a failure of the system.
fn judge_step(
    commits: &Arc<Mutex<Vec<Commit>>>,
    seen: &Arc<Mutex<Seen>>,
    workload: Workload,
    t_load: Instant,
    step: u32,
) -> Value {
    let start = t_load + STEP * step;
    let end = start + STEP;
    let rate = workload.rate(STEP * step).unwrap_or(0.0);
    let want = rate * STEP.as_secs_f64();
    let commits = commits.lock().unwrap();
    let s = seen.lock().unwrap();
    let in_step: Vec<&Commit> = commits
        .iter()
        .filter(|c| c.at >= start && c.at < end)
        .collect();
    let generator_share = in_step.len() as f64 / want;
    let delivered = s
        .first
        .values()
        .filter(|a| **a >= start && **a < end)
        .count() as f64
        / want;
    let mut lat: Vec<f64> = in_step
        .iter()
        .map(|c| {
            s.first.get(&c.id).map_or(f64::INFINITY, |a| {
                a.saturating_duration_since(c.at).as_secs_f64()
            })
        })
        .collect();
    lat.sort_by(f64::total_cmp);
    let p99 = lat.get(((lat.len() as f64 - 1.0) * 0.99) as usize).copied();
    let last_ok = in_step
        .iter()
        .max_by_key(|c| c.at)
        .and_then(|c| s.first.get(&c.id))
        .is_some_and(|a| *a <= end + LIMIT_LATENCY);
    let generator_bound = generator_share < LIMIT_SHARE;
    let sustained = !generator_bound
        && delivered >= LIMIT_SHARE
        && p99.is_some_and(|p| p <= LIMIT_LATENCY.as_secs_f64())
        && last_ok;
    json!({"step": step, "offered_per_s": rate, "generator_share": generator_share,
           "delivered_share": delivered, "p99_s": p99.filter(|p| p.is_finite()),
           "last_row_in_time": last_ok, "generator_bound": generator_bound,
           "sustained": sustained})
}

fn sys_cpu(sys: &Sys<'_>, p: Option<&Process>) -> f64 {
    match (sys, p) {
        (Sys::Df(_), Some(p)) => p.cpu_secs(),
        (Sys::Dbz(c, _), _) => {
            let (u, s) = c.cpu_split();
            u + s
        }
        _ => 0.0,
    }
}

fn sys_rss(sys: &Sys<'_>, p: Option<&Process>) -> u64 {
    match (sys, p) {
        (Sys::Df(_), Some(p)) => p.rss_kib(),
        (Sys::Dbz(c, _), _) => c.rss_kib(),
        _ => 0,
    }
}

async fn control(sys: &Sys<'_>, name: &str, action: &str) -> Result<()> {
    match sys {
        Sys::Df(_) => {
            api(
                reqwest::Method::POST,
                &format!("/pipelines/{name}/{action}"),
                None,
            )
            .await?;
            Ok(())
        }
        Sys::Dbz(c, _) if action == "stop" => c.stop_connector(name).await,
        Sys::Dbz(c, _) => c.resume(name).await,
    }
}

/// Free disk (GiB: the lowest of the harness output's disk and Docker's
/// data disk) and available memory (GiB).
fn floors() -> (f64, f64) {
    static DOCKER_ROOT: std::sync::OnceLock<Option<String>> =
        std::sync::OnceLock::new();
    let docker = DOCKER_ROOT.get_or_init(|| {
        let o = std::process::Command::new("docker")
            .args(["info", "--format", "{{.DockerRootDir}}"])
            .output()
            .ok()?;
        let root = String::from_utf8(o.stdout).ok()?.trim().to_string();
        (!root.is_empty()).then_some(root)
    });
    let mut args = vec!["-B1", "--output=avail", "."];
    if let Some(root) = docker {
        args.push(root);
    }
    let disk = std::process::Command::new("df")
        .args(&args)
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|t| {
            t.lines()
                .skip(1)
                .filter_map(|l| l.trim().parse::<f64>().ok())
                .reduce(f64::min)
        })
        .unwrap_or(0.0)
        / f64::from(1u32 << 30);
    let mem = std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|t| {
            t.lines()
                .find_map(|l| l.strip_prefix("MemAvailable:"))?
                .split_whitespace()
                .next()?
                .parse::<f64>()
                .ok()
        })
        .unwrap_or(0.0)
        / f64::from(1u32 << 20);
    (disk, mem)
}

/// The study's resource floors (`results/scale-stage2-plan.md`): free disk
/// and available memory, in GiB, watched every second.
const FLOOR_DISK_GIB: f64 = 25.0;
const FLOOR_MEM_GIB: f64 = 4.0;
const MONITOR_EVERY: Duration = Duration::from_secs(1);

/// The breach of a floor by `disk` and `mem` (GiB), if any.
fn floor_breach(disk: f64, mem: f64) -> Option<String> {
    (disk < FLOOR_DISK_GIB || mem < FLOOR_MEM_GIB).then(|| {
        format!(
            "resource floor: {disk:.1} GiB disk free, {mem:.1} GiB memory available (floors {FLOOR_DISK_GIB} and {FLOOR_MEM_GIB})"
        )
    })
}

/// An invocation's continuous resource monitor: it samples the floors every
/// second (into `monitor.jsonl`) from its start to its drop, keeps the
/// lowest values since the last [`Monitor::take_low`], and holds the first
/// breach, which cancels the step in progress ([`guarded`]).
struct Monitor {
    breach: tokio::sync::watch::Receiver<Option<String>>,
    low: Arc<Mutex<(f64, f64)>>,
    task: tokio::task::JoinHandle<()>,
}

impl Monitor {
    async fn start(out: &Path) -> Result<Monitor> {
        use std::io::Write;
        let mut log = std::io::BufWriter::new(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(out.join("monitor.jsonl"))?,
        );
        let (tx, breach) = tokio::sync::watch::channel(None);
        let low = Arc::new(Mutex::new((f64::INFINITY, f64::INFINITY)));
        let (first_tx, first) = tokio::sync::oneshot::channel();
        let task = {
            let low = Arc::clone(&low);
            tokio::spawn(async move {
                let mut first_tx = Some(first_tx);
                loop {
                    let (disk, mem) = tokio::task::spawn_blocking(floors)
                        .await
                        .unwrap_or((0.0, 0.0));
                    {
                        let mut l = low.lock().unwrap();
                        l.0 = l.0.min(disk);
                        l.1 = l.1.min(mem);
                    }
                    let unix_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_or(0, |d| d.as_millis());
                    writeln!(
                        log,
                        "{}",
                        json!({"unix_ms": unix_ms, "disk_gib": disk, "mem_gib": mem})
                    )
                    .ok();
                    log.flush().ok();
                    if let Some(b) = floor_breach(disk, mem) {
                        tx.send_if_modified(|v| {
                            v.is_none().then(|| *v = Some(b)).is_some()
                        });
                    }
                    if let Some(f) = first_tx.take() {
                        f.send(()).ok();
                    }
                    tokio::time::sleep(MONITOR_EVERY).await;
                }
            })
        };
        first.await.ok();
        Ok(Monitor { breach, low, task })
    }

    /// The first breach, if one happened.
    fn breached(&self) -> Option<String> {
        self.breach.borrow().clone()
    }

    /// Resolves at the first breach.
    async fn breach(&self) -> String {
        let mut rx = self.breach.clone();
        loop {
            if let Some(b) = rx.borrow_and_update().clone() {
                return b;
            }
            if rx.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }

    /// The lowest free disk and available memory (GiB) since the last call.
    fn take_low(&self) -> (f64, f64) {
        std::mem::replace(
            &mut *self.low.lock().unwrap(),
            (f64::INFINITY, f64::INFINITY),
        )
    }
}

impl Drop for Monitor {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Run `step` unless a floor is breached first: a breach drops it (its
/// process and its spawned tasks with it, [`Tasks`]) and fails it.
async fn guarded<T>(
    monitor: &Monitor,
    step: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    if let Some(b) = monitor.breached() {
        bail!(b);
    }
    tokio::select! {
        r = step => r,
        b = monitor.breach() => Err(anyhow!(b)),
    }
}

/// Spawned tasks, aborted when dropped: a cancelled or failed step leaves
/// none running.
#[derive(Default)]
struct Tasks(Vec<tokio::task::AbortHandle>);

impl Tasks {
    fn add<T>(&mut self, task: &tokio::task::JoinHandle<T>) {
        self.0.push(task.abort_handle());
    }
}

impl Drop for Tasks {
    fn drop(&mut self) {
        for t in &self.0 {
            t.abort();
        }
    }
}

/// Stop a run's system after a failed or cancelled run (best effort): its
/// pipeline or connector is deleted. A DeltaForge process dropped with the
/// run was already killed.
async fn stop_after_failure(sys: &Sys<'_>, name: &str) {
    match sys {
        Sys::Df(_) => {
            api(reqwest::Method::DELETE, &format!("/pipelines/{name}"), None)
                .await
                .ok();
        }
        Sys::Dbz(c, _) => c.delete(name).await,
    }
}

/// How long the drain waits for delivery and the durable frontier, and the
/// consumer for the topic's end after the shutdown.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(300);
const CONSUMER_DRAIN_TIMEOUT: Duration = Duration::from_secs(60);

/// Whether GTID set `set` contains `final_gtid` (MySQL decides).
async fn gtid_covers(
    c: &mut mysql_async::Conn,
    final_gtid: &str,
    set: &str,
) -> Result<bool> {
    let sub: Option<i64> = c
        .exec_first("SELECT GTID_SUBSET(?, ?)", (final_gtid, set))
        .await?;
    Ok(sub == Some(1))
}

/// The system's durable frontier, and whether it covers `final_gtid`:
/// DeltaForge's sink checkpoints, or Debezium's committed offset (the
/// connector is stopped first, so that its tasks commit it; `stopped`
/// records that).
async fn frontier(
    sys: &Sys<'_>,
    name: &str,
    final_gtid: &str,
    c: &mut mysql_async::Conn,
    stopped: &mut bool,
) -> Result<(bool, Value)> {
    match sys {
        Sys::Df(_) => {
            let cps = api(
                reqwest::Method::GET,
                &format!("/pipelines/{name}/checkpoints"),
                None,
            )
            .await?;
            let Some(list) = cps.as_array() else {
                return Ok((false, cps));
            };
            if !list.iter().any(|cp| cp["sink_id"] == "kafka") {
                return Ok((false, cps));
            }
            for cp in list {
                let Some(set) = cp["position"]["gtid_set"].as_str() else {
                    return Ok((false, cps));
                };
                if !gtid_covers(c, final_gtid, set).await? {
                    return Ok((false, cps));
                }
            }
            Ok((true, cps))
        }
        Sys::Dbz(connect, _) => {
            if !*stopped {
                connect.stop_connector(name).await?;
                *stopped = true;
            }
            let v = connect.offsets(name).await?;
            let Some(list) = v["offsets"].as_array().filter(|l| !l.is_empty())
            else {
                return Ok((false, v));
            };
            for o in list {
                let Some(set) = o["offset"]["gtids"].as_str() else {
                    bail!(
                        "the connector's committed offset has no GTID set: {o}"
                    );
                };
                if !gtid_covers(c, final_gtid, set).await? {
                    return Ok((false, v));
                }
            }
            Ok((true, v))
        }
    }
}

/// The topic's end offset (its one partition's high watermark).
async fn topic_end(topic: &str) -> Result<i64> {
    let topic = topic.to_string();
    tokio::task::spawn_blocking(move || {
        let c: rdkafka::consumer::BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers())
            .create()?;
        let (_, high) =
            c.fetch_watermarks(&topic, 0, Duration::from_secs(10))?;
        Ok(high)
    })
    .await?
}

/// MySQL's data directory size (bytes), in its container.
async fn datadir_bytes(infra: &Infra) -> Option<u64> {
    let o = tokio::process::Command::new("docker")
        .args(["exec", infra.mysql.id(), "du", "-sb", "/var/lib/mysql"])
        .output()
        .await
        .ok()?;
    String::from_utf8(o.stdout)
        .ok()?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

/// A run's store statements on the PostgreSQL store (`pg_stat_statements`
/// of its own database): every statement, and its commits and begins.
async fn state_statements(infra: &Infra, db: &str) -> Value {
    let Ok(c) = infra.state_client("postgres").await else {
        return Value::Null;
    };
    let row = c
        .query_one(
            "SELECT coalesce(sum(s.calls), 0)::bigint, \
                    coalesce(sum(s.calls) FILTER (WHERE s.query ILIKE 'commit%'), 0)::bigint, \
                    coalesce(sum(s.calls) FILTER (WHERE s.query ILIKE 'begin%' OR s.query ILIKE 'start transaction%'), 0)::bigint \
             FROM pg_stat_statements s JOIN pg_database d ON d.oid = s.dbid \
             WHERE d.datname = $1",
            &[&db],
        )
        .await;
    match row {
        Ok(r) => json!({
            "statements": r.get::<_, i64>(0),
            "commits": r.get::<_, i64>(1),
            "begins": r.get::<_, i64>(2),
        }),
        Err(e) => json!({"error": e.to_string()}),
    }
}

/// DeltaForge's sampled gauges: MySQL connections by kind, and the
/// selection cache's estimated bytes and timelines (summed over sources).
const DF_GAUGES: [&str; 3] = [
    "deltaforge_mysql_connections",
    "deltaforge_mysql_selection_cache_bytes",
    "deltaforge_mysql_selection_cache_timelines",
];

/// The gauges in a metrics text, keyed `name` or `name{kind}` for the
/// connections.
fn parse_gauges(text: &str) -> HashMap<String, f64> {
    let mut out: HashMap<String, f64> = HashMap::new();
    for l in text.lines() {
        for name in DF_GAUGES {
            let Some(rest) = l.strip_prefix(name) else {
                continue;
            };
            if !(rest.starts_with('{') || rest.starts_with(' ')) {
                continue;
            }
            let Some(v) =
                l.rsplit(' ').next().and_then(|v| v.parse::<f64>().ok())
            else {
                continue;
            };
            let key = match rest
                .split_once("kind=\"")
                .and_then(|(_, k)| k.split_once('"'))
            {
                Some((kind, _)) => format!("{name}{{{kind}}}"),
                None => name.to_string(),
            };
            *out.entry(key).or_default() += v;
        }
    }
    out
}

async fn df_gauges() -> Option<HashMap<String, f64>> {
    let text = reqwest::get(format!("http://{METRICS}/metrics"))
        .await
        .ok()?
        .text()
        .await
        .ok()?;
    Some(parse_gauges(&text))
}

/// Keep each gauge's largest sample.
fn keep_peaks(peaks: &mut HashMap<String, f64>, sample: HashMap<String, f64>) {
    for (k, v) in sample {
        let p = peaks.entry(k).or_insert(v);
        *p = p.max(v);
    }
}

/// DeltaForge's counters whose increase fails a run (a measured failure,
/// kept and investigated: `results/scale-stage2-plan.md`).
const DF_FAILURES: [&str; 6] = [
    "deltaforge_mysql_control_permit_timeouts_total",
    "deltaforge_mysql_control_saturation_deferrals_total",
    "deltaforge_mysql_activation_window_deferred_total",
    "deltaforge_mysql_activation_window_overflow_total",
    "deltaforge_mysql_proof_rounds_failed_total",
    "deltaforge_mysql_capture_kill_failures_total",
];

/// The measured failures in a run's counter increases.
fn measured_failures(delta: &Value) -> Vec<String> {
    DF_FAILURES
        .iter()
        .filter_map(|k| {
            let v = delta[*k].as_f64().unwrap_or(0.0);
            (v > 0.0).then(|| format!("{k} +{v}"))
        })
        .collect()
}

/// The study's plan, from the command line.
#[derive(Clone)]
pub struct Plan {
    pub tables: Vec<usize>,
    pub active: Vec<u32>,
    pub workloads: Vec<Workload>,
    pub rotate: bool,
    pub binlog_mib: Vec<u64>,
    pub pad: Pad,
    pub reps: usize,
    pub seconds: u64,
    pub warmup: bool,
    /// The catch-up phase (a stop and resume during the load).
    pub catchup: bool,
    /// When not empty, only the grid's points with these ids run.
    pub only: Vec<String>,
}

impl Plan {
    /// Refuse a point list that names a point outside the grid, or one
    /// point twice. Selected points always run in the grid's order.
    pub fn check_points(&self) -> Result<()> {
        let grid = Plan {
            only: Vec::new(),
            ..self.clone()
        };
        let all: Vec<String> = grid
            .tables
            .iter()
            .flat_map(|&t| grid.points(t))
            .map(|p| p.id())
            .collect();
        for (i, id) in self.only.iter().enumerate() {
            if !all.contains(id) {
                bail!("--scale-points: {id} is not a point of the grid");
            }
            if self.only[..i].contains(id) {
                bail!("--scale-points: {id} is listed twice");
            }
        }
        Ok(())
    }

    fn points(&self, tables: usize) -> Vec<Point> {
        let mut v = Vec::new();
        for &workload in &self.workloads {
            for &active in &self.active {
                // Rotation has no meaning with every table active.
                if self.rotate && active >= 100 {
                    continue;
                }
                for &binlog_mib in &self.binlog_mib {
                    let p = Point {
                        tables,
                        active,
                        workload,
                        rotate: self.rotate,
                        binlog_mib,
                        pad: self.pad,
                    };
                    if self.only.is_empty() || self.only.contains(&p.id()) {
                        v.push(p);
                    }
                }
            }
        }
        v
    }
}

/// Run the plan for both systems, interleaved, into `out`: `runs.jsonl`
/// (one line per run), `fixtures.jsonl` (each fixture's build time) and
/// `scale-summary.md`.
pub async fn study(
    plan: &Plan,
    binaries: &[(String, crate::System)],
    infra: &Infra,
    out: &Path,
    extras_cache: &Path,
    offset_flush_ms: Option<u64>,
) -> Result<()> {
    use std::io::Write;
    let profile = binaries.iter().find_map(|(_, s)| match s {
        crate::System::Debezium(p) => Some(*p),
        _ => None,
    });
    let extras = match profile {
        Some(_) => Some(crate::extras::prepare(extras_cache)?),
        None => None,
    };
    let start_worker = |generation: u32| {
        let extras = extras.clone();
        async move {
            match (profile, extras) {
                (Some(p), Some(e)) => Ok::<_, anyhow::Error>(Some(
                    debezium::Connect::start(
                        crate::NETWORK,
                        &bootstrap_internal(),
                        &format!("scale-{generation}"),
                        p,
                        offset_flush_ms,
                        &e,
                    )
                    .await?,
                )),
                _ => Ok(None),
            }
        }
    };
    // DeltaForge's activation counts come from its proof trace.
    if binaries
        .iter()
        .any(|(_, s)| matches!(s, crate::System::DeltaForge(_)))
        && !std::env::var("PERF_RUST_LOG")
            .is_ok_and(|v| v.contains("deltaforge::proof_trace=info"))
    {
        bail!(
            "the scaling study needs DeltaForge's proof trace: set PERF_RUST_LOG to include deltaforge::proof_trace=info"
        );
    }
    let monitor = Monitor::start(out).await?;
    let mut generation = 0u32;
    let mut connect = start_worker(generation).await?;
    let mut runs = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(out.join("runs.jsonl"))?;
    let mut fixtures = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(out.join("fixtures.jsonl"))?;
    let base = Duration::from_secs(plan.seconds);
    let unix_s = || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0.0, |d| d.as_secs_f64())
    };
    let mut seq = 0u64;
    // The runs' place in the invocation (warm-ups included).
    let mut index = 0u64;
    let mut outcome = Ok(());
    plan.check_points()?;
    'tables: for &tables in &plan.tables {
        if plan.points(tables).is_empty() {
            continue;
        }
        monitor.take_low();
        let secs = match guarded(&monitor, fixture(infra, tables)).await {
            Ok(secs) => secs,
            Err(e) => {
                outcome = Err(e.context(format!("the {tables}-table fixture")));
                break 'tables;
            }
        };
        let (disk_low, mem_low) = monitor.take_low();
        writeln!(
            fixtures,
            "{}",
            json!({"tables": tables, "build_s": secs,
                   "datadir_bytes": datadir_bytes(infra).await,
                   "disk_low_gib": disk_low, "mem_low_gib": mem_low})
        )?;
        eprintln!("fixture of {tables} tables built in {secs:.0} s");
        let points = plan.points(tables);
        let mut todo: Vec<(Point, Option<usize>)> = Vec::new();
        if plan.warmup
            && let Some(p) = points.first()
        {
            todo.push((*p, None));
        }
        for p in &points {
            for rep in 0..plan.reps {
                todo.push((*p, Some(rep)));
            }
        }
        for (point, rep) in todo {
            let order: Vec<&(String, crate::System)> =
                if rep.unwrap_or(0) % 2 == 0 {
                    binaries.iter().collect()
                } else {
                    binaries.iter().rev().collect()
                };
            for (label, system) in order {
                if let Some(b) = monitor.breached() {
                    outcome = Err(anyhow!(b));
                    break 'tables;
                }
                seq += 1;
                index += 1;
                let rep_name =
                    rep.map_or("warmup".to_string(), |r| format!("r{r}"));
                let tag = format!("{}-{label}-{rep_name}-{seq}", point.id());
                let dir = out.join("runs").join(&tag);
                let sys = match system {
                    crate::System::DeltaForge(b) => Sys::Df(b),
                    crate::System::Debezium(p) => match &connect {
                        Some(c) => Sys::Dbz(c, *p),
                        None => bail!("no Kafka Connect worker"),
                    },
                };
                monitor.take_low();
                let started_unix_s = unix_s();
                let r = guarded(
                    &monitor,
                    run(
                        infra,
                        &sys,
                        &point,
                        seq,
                        &tag,
                        &dir,
                        base,
                        plan.catchup,
                    ),
                )
                .await;
                if r.is_err() {
                    // A failed or cancelled run's system is stopped before
                    // anything else runs.
                    stop_after_failure(&sys, &format!("s-{tag}")).await;
                }
                let (disk_low, mem_low) = monitor.take_low();
                let failed = !matches!(&r, Ok(v) if v["ok"] == json!(true));
                let result = r.unwrap_or_else(
                    |e| json!({"ok": false, "error": format!("{e:#}")}),
                );
                let order_record = json!({
                    "seq": seq, "index": index,
                    "started_unix_s": started_unix_s, "ended_unix_s": unix_s(),
                    "disk_low_gib": disk_low, "mem_low_gib": mem_low,
                    "connect_worker_idle": matches!(system, crate::System::DeltaForge(_)) && connect.is_some(),
                });
                eprintln!(
                    "{} {label} {rep_name}: {}",
                    point.id(),
                    if failed {
                        format!("FAILED {}", result["error"])
                    } else {
                        format!(
                            "ready {:.2} s, first event {:.2} s, steady p99 {} ms, cpu {:.1} s",
                            result["ready_s"].as_f64().unwrap_or(0.0),
                            result["first_event_s"].as_f64().unwrap_or(0.0),
                            result["latency_steady_ms"]["p99"]
                                .as_f64()
                                .map_or("-".into(), |v| format!("{v:.0}")),
                            result["cpu_s"].as_f64().unwrap_or(0.0),
                        )
                    }
                );
                let worker = matches!(system, crate::System::Debezium(_))
                    .then_some(generation);
                if rep.is_some() {
                    writeln!(
                        runs,
                        "{}",
                        json!({"point": point.id(), "version": label, "rep": rep, "worker": worker, "order": order_record, "result": result})
                    )?;
                }
                if failed
                    && matches!(system, crate::System::Debezium(_))
                    && let Some(c) = connect.take()
                {
                    eprintln!(
                        "a Debezium run failed: replacing the Kafka Connect worker"
                    );
                    c.stop(&out.join(format!("connect-{generation}.log")))
                        .await;
                    generation += 1;
                    connect = start_worker(generation).await?;
                }
            }
        }
    }
    if let Some(c) = connect {
        c.stop(&out.join("connect.log")).await;
    }
    summary(out)?;
    outcome
}

/// `scale-summary.md`: per point and system, the medians of the main
/// measures over the repetitions, and every failed run.
fn summary(out: &Path) -> Result<()> {
    let text =
        std::fs::read_to_string(out.join("runs.jsonl")).unwrap_or_default();
    let runs: Vec<Value> = text
        .lines()
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect();
    let mut keys: Vec<(String, String)> = runs
        .iter()
        .map(|r| {
            (
                r["point"].as_str().unwrap_or_default().to_string(),
                r["version"].as_str().unwrap_or_default().to_string(),
            )
        })
        .collect();
    keys.sort();
    keys.dedup();
    let med = |v: Vec<f64>| -> Option<f64> {
        let mut v: Vec<f64> = v.into_iter().filter(|x| x.is_finite()).collect();
        if v.is_empty() {
            return None;
        }
        v.sort_by(f64::total_cmp);
        Some(v[v.len() / 2])
    };
    let f = |v: Option<f64>| v.map_or("-".to_string(), |x| format!("{x:.2}"));
    let mut md = String::from(
        "Single-instance scaling curves on one host, not a fleet qualification. Medians over the repetitions.\n\n\
         | point | system | ok | binlog start MiB | ready s | first event s | steady p50/p99 ms | activation p50/p95/p99 ms | proofs | seek total s | seek p50 ms | catch-up s | drain s | achieved/s | cpu cores | rss MiB | mysql conns | binlog MiB |\n|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n",
    );
    for (point, label) in &keys {
        let rs: Vec<&Value> = runs
            .iter()
            .filter(|r| {
                r["point"] == json!(point) && r["version"] == json!(label)
            })
            .map(|r| &r["result"])
            .collect();
        let ok: Vec<&&Value> =
            rs.iter().filter(|r| r["ok"] == json!(true)).collect();
        let m = |path: &[&str]| {
            med(ok
                .iter()
                .filter_map(|r| {
                    let mut v = **r;
                    for p in path {
                        v = &v[*p];
                    }
                    v.as_f64()
                })
                .collect())
        };
        md.push_str(&format!(
            "| {point} | {label} | {}/{} | {} | {} | {} | {}/{} | {}/{}/{} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            ok.len(), rs.len(),
            f(m(&["binlog_start", "offset"]).map(|b| b / 1048576.0)),
            f(m(&["ready_s"])), f(m(&["first_event_s"])),
            f(m(&["latency_steady_ms", "p50"])), f(m(&["latency_steady_ms", "p99"])),
            f(m(&["latency_activation_ms", "p50"])), f(m(&["latency_activation_ms", "p95"])), f(m(&["latency_activation_ms", "p99"])),
            f(m(&["proof_trace", "proofs"])),
            f(m(&["proof_trace", "seek_total_ms"]).map(|v| v / 1e3)),
            f(m(&["proof_trace", "seek_ms", "p50"])),
            f(m(&["catchup_s"])), f(m(&["drain_s"])), f(m(&["achieved_per_s"])),
            f(m(&["cpu_cores"])), f(m(&["rss_peak_mib"])), f(m(&["mysql_connections_max"])),
            f(m(&["binlog_written_bytes"]).map(|b| b / 1048576.0)),
        ));
    }
    md.push_str("\nFailed runs:\n\n");
    let mut any = false;
    for r in &runs {
        if r["result"]["ok"] != json!(true) {
            any = true;
            md.push_str(&format!(
                "- {} {} rep {}: {}\n",
                r["point"], r["version"], r["rep"], r["result"]["error"]
            ));
        }
    }
    if !any {
        md.push_str("- none\n");
    }
    md.push_str("\nRaw results: `runs.jsonl` (every measure of every run), `fixtures.jsonl`, and each run's configuration under `runs/`.\n");
    std::fs::write(out.join("scale-summary.md"), md)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_point_list_selects_exactly_its_points() {
        let plan = |only: &[&str]| Plan {
            tables: vec![100, 1000],
            active: vec![1, 20, 50, 100],
            workloads: vec![Workload::Steady, Workload::Burst],
            rotate: false,
            binlog_mib: vec![0],
            pad: Pad::Large,
            reps: 5,
            seconds: 120,
            warmup: true,
            catchup: true,
            only: only.iter().map(|s| s.to_string()).collect(),
        };
        let ids = |p: &Plan, t| -> Vec<String> {
            p.points(t).iter().map(|p| p.id()).collect()
        };
        assert_eq!(ids(&plan(&[]), 1000).len(), 8);
        let p = plan(&["t100-a20-burst-o0", "t1000-a1-burst-o0"]);
        assert_eq!(ids(&p, 100), ["t100-a20-burst-o0"]);
        assert_eq!(ids(&p, 1000), ["t1000-a1-burst-o0"]);
        assert!(p.check_points().is_ok());

        // The grid's order, whatever the list's order.
        let listed = [
            "t100-a50-burst-o0",
            "t100-a1-steady-o0",
            "t100-a20-burst-o0",
        ];
        let mut reversed = listed;
        reversed.reverse();
        assert_eq!(ids(&plan(&listed), 100), ids(&plan(&reversed), 100));
        assert_eq!(
            ids(&plan(&listed), 100),
            [
                "t100-a1-steady-o0",
                "t100-a20-burst-o0",
                "t100-a50-burst-o0"
            ]
        );

        let unknown = plan(&["t100-a20-burst-o0", "t100-a30-burst-o0"]);
        let e = unknown.check_points().unwrap_err().to_string();
        assert!(e.contains("t100-a30-burst-o0 is not a point"), "{e}");
        let twice = plan(&["t100-a20-burst-o0", "t100-a20-burst-o0"]);
        let e = twice.check_points().unwrap_err().to_string();
        assert!(e.contains("listed twice"), "{e}");
    }

    #[test]
    fn a_rotating_run_activates_half_the_inactive_tables() {
        let p = |active| Point {
            tables: 40_000,
            active,
            workload: Workload::Steady,
            rotate: true,
            binlog_mib: 0,
            pad: Pad::Large,
        };
        // 20%: 32,000 inactive, 800 replaced per interval: 20 intervals.
        assert_eq!(
            p(20).duration(Duration::from_secs(120)),
            Duration::from_secs(200)
        );
        // 50%: 5 intervals, at least 6, at least the base duration.
        assert_eq!(
            p(50).duration(Duration::from_secs(120)),
            Duration::from_secs(120)
        );
    }

    #[test]
    fn the_burst_averages_the_steady_rate() {
        let mut n = 0.0;
        let mut t = Duration::ZERO;
        while t < Duration::from_secs(10) {
            n += Workload::Burst.rate(t).unwrap_or(0.0) / 1_000.0;
            t += Duration::from_millis(1);
        }
        assert!((n / 10.0 - 2_000.0).abs() < 1.0, "{n}");
    }

    /// Connections are kept by kind, summed over sources; the cache gauges
    /// are summed; a longer name sharing a gauge's prefix is not read.
    #[test]
    fn gauges_are_read_by_kind() {
        let text = "\
# TYPE deltaforge_mysql_connections gauge
deltaforge_mysql_connections{kind=\"control\",source=\"a\"} 3
deltaforge_mysql_connections{kind=\"control\",source=\"b\"} 2
deltaforge_mysql_connections{kind=\"replication\",source=\"a\"} 1
deltaforge_mysql_selection_cache_bytes{pipeline=\"p\",source=\"a\"} 1048576
deltaforge_mysql_selection_cache_timelines{pipeline=\"p\",source=\"a\"} 40
deltaforge_mysql_connections_other{kind=\"control\"} 99
";
        let g = parse_gauges(text);
        assert_eq!(g["deltaforge_mysql_connections{control}"], 5.0);
        assert_eq!(g["deltaforge_mysql_connections{replication}"], 1.0);
        assert_eq!(g["deltaforge_mysql_selection_cache_bytes"], 1_048_576.0);
        assert_eq!(g["deltaforge_mysql_selection_cache_timelines"], 40.0);
        assert_eq!(g.len(), 4, "{g:?}");
        let mut peaks = HashMap::new();
        keep_peaks(&mut peaks, g);
        keep_peaks(
            &mut peaks,
            parse_gauges("deltaforge_mysql_connections{kind=\"control\"} 1\n"),
        );
        assert_eq!(peaks["deltaforge_mysql_connections{control}"], 5.0);
    }

    /// A window's plans are its applies and its conflicts.
    #[test]
    fn windows_count_plans_and_cuts() {
        let w = |tables: u64, applies: u64, conflicts: u64, cut: bool| {
            json!({"tables": tables, "batch_applies": applies,
                   "batch_conflicts": conflicts, "cut": cut,
                   "bytes": tables * 10, "commit_ms": 2.0})
        };
        let s = window_summary(&[
            w(910, 1, 0, true),
            w(300, 2, 1, false),
            w(5, 1, 0, false),
        ]);
        assert_eq!(s["windows"], 3);
        assert_eq!(s["tables"], 1215.0);
        assert_eq!(s["plans"], 5.0);
        assert_eq!(s["plans_per_window"]["max"], 3.0);
        assert_eq!(s["cuts"], 1);
        assert_eq!(s["bytes_max"], 9100.0);
        assert_eq!(window_summary(&[])["windows"], 0);
    }

    /// Any increase of a failure counter fails the run, named with it.
    #[test]
    fn failure_counters_fail_the_run() {
        let mut d = serde_json::Map::new();
        for k in DF_COUNTERS {
            d.insert(k.to_string(), json!(0.0));
        }
        assert!(measured_failures(&Value::Object(d.clone())).is_empty());
        d.insert(
            "deltaforge_mysql_control_saturation_deferrals_total".into(),
            json!(2.0),
        );
        d.insert(
            "deltaforge_mysql_activation_windows_total".into(),
            json!(9.0),
        );
        assert_eq!(
            measured_failures(&Value::Object(d)),
            ["deltaforge_mysql_control_saturation_deferrals_total +2"]
        );
        for k in DF_FAILURES {
            assert!(DF_COUNTERS.contains(&k), "{k} is read");
        }
    }

    #[test]
    fn the_floors_are_25_gib_disk_and_4_gib_memory() {
        assert!(floor_breach(25.0, 4.0).is_none());
        assert!(floor_breach(24.9, 64.0).is_some());
        assert!(floor_breach(400.0, 3.9).is_some());
    }

    /// A breach cancels the step in progress at once: the step is dropped,
    /// and the tasks it spawned are aborted with it.
    #[tokio::test]
    async fn a_breach_cancels_the_step_and_its_tasks() {
        let (tx, breach) = tokio::sync::watch::channel(None);
        let monitor = Monitor {
            breach,
            low: Arc::new(Mutex::new((f64::INFINITY, f64::INFINITY))),
            task: tokio::spawn(std::future::pending::<()>()),
        };
        let ticks = Arc::new(AtomicU64::new(0));
        let step = {
            let ticks = Arc::clone(&ticks);
            async move {
                let mut tasks = Tasks::default();
                let t = tokio::spawn(async move {
                    loop {
                        ticks.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                });
                tasks.add(&t);
                tokio::time::sleep(Duration::from_secs(3_600)).await;
                Ok::<_, anyhow::Error>(())
            }
        };
        let breached = async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            tx.send_replace(Some("resource floor: test".into()));
        };
        let started = Instant::now();
        let (r, ()) = tokio::join!(guarded(&monitor, step), breached);
        assert!(started.elapsed() < Duration::from_secs(5));
        assert!(r.unwrap_err().to_string().contains("resource floor"));
        let after = ticks.load(Ordering::SeqCst);
        assert!(after > 0, "the task ran before the breach");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(ticks.load(Ordering::SeqCst), after, "the task was aborted");
        // A later step does not start.
        let late = guarded(&monitor, async { Ok::<_, anyhow::Error>(()) });
        assert!(late.await.is_err());
    }
}

/// Whether each DeltaForge scale run is counted with `perf stat`.
pub static PERF_COUNTERS: std::sync::OnceLock<bool> =
    std::sync::OnceLock::new();

/// The process's user-space instructions and cycles over `secs`, from
/// `perf stat -x,`. On a hybrid CPU perf reports each core type apart
/// (`cpu_atom/instructions/u`, `cpu_core/instructions/u`), each already
/// scaled for multiplexing: the process's total is their sum. A counter
/// perf could not count fails the measurement instead of reading as zero.
async fn perf_counters(pid: u32, secs: u64) -> Result<Value> {
    let out = tokio::process::Command::new("perf")
        .args(["stat", "-x", ",", "-e", "instructions:u,cycles:u", "-p"])
        .arg(pid.to_string())
        .args(["--", "sleep"])
        .arg(secs.to_string())
        .output()
        .await
        .context("run perf stat")?;
    let text = String::from_utf8_lossy(&out.stderr).to_string();
    parse_perf_csv(&text)
}

/// [`perf_counters`]' parse, apart for its tests.
fn parse_perf_csv(text: &str) -> Result<Value> {
    let (mut instructions, mut cycles) = (0f64, 0f64);
    let mut parts = Vec::new();
    for line in text.lines() {
        let f: Vec<&str> = line.split(',').collect();
        if f.len() < 3 || line.starts_with('#') {
            continue;
        }
        let event = f[2];
        let base = event
            .rsplit('/')
            .find(|s| !s.is_empty() && *s != "u")
            .unwrap_or(event)
            .trim_end_matches(":u");
        if base != "instructions" && base != "cycles" {
            continue;
        }
        let value: f64 = f[0]
            .parse()
            .map_err(|_| anyhow!("perf could not count {event}: {:?}", f[0]))?;
        let pct = f.get(4).and_then(|p| p.parse::<f64>().ok());
        parts.push(json!({"event": event, "value": value, "running_pct": pct}));
        if base == "instructions" {
            instructions += value;
        } else {
            cycles += value;
        }
    }
    if parts.is_empty() {
        bail!("perf stat reported no counters: {text}");
    }
    Ok(json!({"instructions": instructions, "cycles": cycles, "parts": parts}))
}

#[cfg(test)]
mod perf_tests {
    use super::*;

    /// A hybrid CPU's two scaled counts are summed; a plain CPU's one count
    /// is taken as is; an uncounted event fails.
    #[test]
    fn hybrid_counters_are_summed() {
        let hybrid = "9761736008,,cpu_atom/instructions/u,23078934514,76.87,,\n\
                      13393446607,,cpu_core/instructions/u,6942384110,23.13,,\n\
                      9115411318,,cpu_atom/cycles/u,23078934514,76.87,,\n\
                      10763386464,,cpu_core/cycles/u,6942384110,23.13,,\n";
        let v = parse_perf_csv(hybrid).unwrap();
        assert_eq!(v["instructions"], 23155182615.0);
        assert_eq!(v["cycles"], 19878797782.0);
        let plain =
            "1200,,instructions:u,100,100.00,,\n800,,cycles:u,100,100.00,,\n";
        let v = parse_perf_csv(plain).unwrap();
        assert_eq!(
            (v["instructions"].as_f64(), v["cycles"].as_f64()),
            (Some(1200.0), Some(800.0))
        );
        assert!(
            parse_perf_csv("<not counted>,,cpu_core/instructions/u,0,0.00,,\n")
                .is_err()
        );
    }
}
