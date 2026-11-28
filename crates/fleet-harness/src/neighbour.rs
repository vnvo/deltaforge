//! The neighbour workload of a loaded-source run: an open-loop OLTP load on
//! tables DeltaForge does not capture, on the same server as the fixture,
//! so a run shows what DeltaForge costs the source's own work and how it
//! behaves next to it.
//!
//! Each connection runs its share of the offered rate on a fixed schedule;
//! an operation's latency counts from its scheduled start, so a stall shows
//! as latency, never as a lower offered rate. Updates touch only the rows
//! the connection owns and inserts use ids unique to the connection and the
//! run, so the neighbour alone never waits on a row lock: any lock wait in
//! its records comes from someone else. Every `interval_secs` one JSON line
//! goes to `neighbour-<server>.jsonl`: operations started and done, errors,
//! latency percentiles, and the server's lock-wait counters (null with the
//! error when unreadable, never zero).

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use mysql_async::Pool;
use mysql_async::prelude::Queryable;
use parking_lot::Mutex;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::config::{Neighbour, NeighbourMix, RunConfig, Server};

/// Inserts a connection may make in one run (the id space per run).
const INSERTS_PER_RUN: u64 = 1 << 24;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Select = 0,
    Update = 1,
    Insert = 2,
}

/// The operation for a uniform draw in [0, 1).
pub fn pick(u: f64, mix: &NeighbourMix) -> Op {
    if u < mix.point_select {
        Op::Select
    } else if u < mix.point_select + mix.update {
        Op::Update
    } else {
        Op::Insert
    }
}

/// When a connection's `k`-th operation is due, in seconds after the start:
/// the connections interleave, so together they offer `ops_per_sec`.
pub fn scheduled_secs(
    k: u64,
    conn: u32,
    connections: u32,
    ops_per_sec: f64,
) -> f64 {
    (k as f64 * f64::from(connections) + f64::from(conn)) / ops_per_sec
}

/// One of the ids in `1..=rows` a connection owns (`(id - 1) % connections
/// == conn`), or `None` when it owns none.
pub fn owned_id(
    draw: u64,
    conn: u32,
    connections: u32,
    rows: u64,
) -> Option<u64> {
    let (c, conn) = (u64::from(connections), u64::from(conn));
    if conn >= rows {
        return None;
    }
    let owned = (rows - conn - 1) / c + 1;
    Some(conn + 1 + (draw % owned) * c)
}

/// The first insert id of a run started at `start_unix_secs`: above the
/// filled rows and above every id of a run started at an earlier second.
pub fn run_base(start_unix_secs: u64, rows: u64) -> u64 {
    (start_unix_secs * INSERTS_PER_RUN).max(rows + 1)
}

/// A connection's `k`-th insert id, or `None` once the run's id space is
/// used up.
pub fn insert_id(
    base: u64,
    k: u64,
    conn: u32,
    connections: u32,
) -> Option<u64> {
    let n = k * u64::from(connections) + u64::from(conn);
    (n < INSERTS_PER_RUN).then_some(base + n)
}

/// Nearest-rank percentile of sorted values.
pub fn pct(sorted: &[u32], p: f64) -> Option<u32> {
    if sorted.is_empty() {
        return None;
    }
    let rank =
        ((p * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len());
    Some(sorted[rank - 1])
}

#[derive(Debug, Default)]
struct Tally {
    started: [u64; 3],
    done: [u64; 3],
    errors: u64,
    first_error: Option<String>,
    lat_us: Vec<u32>,
}

impl Tally {
    fn add(&mut self, other: &Tally) {
        for i in 0..3 {
            self.started[i] += other.started[i];
            self.done[i] += other.done[i];
        }
        self.errors += other.errors;
    }
}

fn by_op(v: &[u64; 3]) -> Value {
    json!({"select": v[0], "update": v[1], "insert": v[2], "total": v.iter().sum::<u64>()})
}

/// What the neighbour did over the whole run.
#[derive(Debug, Clone, Serialize)]
pub struct Summary {
    pub server: String,
    pub secs: f64,
    pub offered_per_sec: f64,
    pub started: Value,
    pub done: Value,
    pub errors: u64,
    pub first_error: Option<String>,
    pub records: u64,
}

/// A started neighbour; dropping it stops it too.
pub struct Running {
    server: String,
    stop: watch::Sender<bool>,
    task: JoinHandle<Result<Summary>>,
}

impl Running {
    pub fn server(&self) -> &str {
        &self.server
    }

    /// Stop it and wait for its last record.
    pub async fn finish(self) -> Result<Summary> {
        self.stop.send(true).ok();
        self.task.await.context("neighbour task")?
    }
}

fn pool(cfg: &RunConfig, server: &Server, max: usize) -> Result<Pool> {
    crate::fixture::admin_pool(cfg, server, max)
}

/// Check the neighbour's tables on `server` and start its load; records go
/// to `dir/neighbour-<server>.jsonl`.
pub async fn start(
    cfg: &RunConfig,
    server: &Server,
    dir: &Path,
) -> Result<Running> {
    let n = cfg
        .neighbour
        .clone()
        .context("no neighbour section in the configuration")?;
    let pool = pool(cfg, server, n.connections as usize + 1)?;
    {
        let mut conn = pool.get_conn().await.with_context(|| {
            format!("neighbour: connect to {}", server.name)
        })?;
        for t in 0..n.tables {
            let rows: Option<u64> = conn
                .query_first(format!(
                    "SELECT COUNT(*) FROM (SELECT id FROM {} WHERE id <= {} LIMIT {}) x",
                    table(&n, t),
                    n.rows_per_table,
                    n.rows_per_table
                ))
                .await
                .with_context(|| {
                    format!("neighbour: read {} on {}", table(&n, t), server.name)
                })?;
            if rows != Some(n.rows_per_table) {
                bail!(
                    "neighbour: {} on {} has {} of the {} filled rows; build it first",
                    table(&n, t),
                    server.name,
                    rows.unwrap_or(0),
                    n.rows_per_table
                );
            }
        }
    }
    let (stop_tx, stop) = watch::channel(false);
    let path = dir.join(format!("neighbour-{}.jsonl", server.name));
    let name = server.name.clone();
    let task = tokio::spawn(run(Arc::new(n), pool, name.clone(), path, stop));
    Ok(Running {
        server: name,
        stop: stop_tx,
        task,
    })
}

pub fn table(n: &Neighbour, t: u32) -> String {
    format!("`{}`.`t{t}`", n.database)
}

async fn run(
    n: Arc<Neighbour>,
    pool: Pool,
    server: String,
    path: PathBuf,
    stop: watch::Receiver<bool>,
) -> Result<Summary> {
    let mut out = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("open {}", path.display()))?;
    let tally = Arc::new(Mutex::new(Tally::default()));
    let start = tokio::time::Instant::now();
    let unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let base = run_base(unix, n.rows_per_table);
    let mut workers: Vec<JoinHandle<()>> = (0..n.connections)
        .map(|c| {
            tokio::spawn(worker(
                n.clone(),
                pool.clone(),
                c,
                start,
                base,
                tally.clone(),
                stop.clone(),
            ))
        })
        .collect();
    let mut total = Tally::default();
    let mut records = 0u64;
    let mut stop_r = stop.clone();
    let every = Duration::from_secs(n.interval_secs.max(1));
    let mut next = start + every;
    let mut last = start;
    loop {
        let stopping = tokio::select! {
            _ = tokio::time::sleep_until(next) => false,
            _ = stop_r.wait_for(|s| *s) => true,
        };
        if stopping {
            for w in workers.drain(..) {
                w.await.ok();
            }
        }
        let now = tokio::time::Instant::now();
        let mut t = std::mem::take(&mut *tally.lock());
        t.lat_us.sort_unstable();
        let locks = locks(&pool).await;
        let line = json!({
            "server": server,
            "at": chrono::Utc::now().to_rfc3339(),
            "secs": now.duration_since(start).as_secs_f64(),
            "interval_secs": now.duration_since(last).as_secs_f64(),
            "offered_per_sec": n.ops_per_sec,
            "started": by_op(&t.started),
            "done": by_op(&t.done),
            "errors": t.errors,
            "first_error": t.first_error,
            "lat_us": {
                "n": t.lat_us.len(),
                "p50": pct(&t.lat_us, 0.50),
                "p99": pct(&t.lat_us, 0.99),
                "p999": pct(&t.lat_us, 0.999),
                "max": t.lat_us.last(),
            },
            "locks": locks,
        });
        writeln!(out, "{line}").context("write a neighbour record")?;
        out.flush().ok();
        records += 1;
        if total.first_error.is_none() {
            total.first_error = t.first_error.clone();
        }
        total.add(&t);
        last = now;
        next += every;
        if stopping {
            break;
        }
    }
    pool.disconnect().await.ok();
    Ok(Summary {
        server,
        secs: last.duration_since(start).as_secs_f64(),
        offered_per_sec: n.ops_per_sec,
        started: by_op(&total.started),
        done: by_op(&total.done),
        errors: total.errors,
        first_error: total.first_error,
        records,
    })
}

async fn worker(
    n: Arc<Neighbour>,
    pool: Pool,
    conn: u32,
    start: tokio::time::Instant,
    base: u64,
    tally: Arc<Mutex<Tally>>,
    mut stop: watch::Receiver<bool>,
) {
    let mut rng = StdRng::seed_from_u64(0x6e65_6967 ^ u64::from(conn));
    let mut c = None;
    let mut inserts = 0u64;
    let pad = "x".repeat(60);
    for k in 0u64.. {
        let at = start
            + Duration::from_secs_f64(scheduled_secs(
                k,
                conn,
                n.connections,
                n.ops_per_sec,
            ));
        tokio::select! {
            _ = tokio::time::sleep_until(at) => {}
            _ = stop.wait_for(|s| *s) => return,
        }
        let op = pick(rng.random::<f64>(), &n.mix);
        let t = rng.random_range(0..n.tables);
        let draw = rng.random::<u64>();
        tally.lock().started[op as usize] += 1;
        let r: Result<()> = async {
            if c.is_none() {
                c = Some(pool.get_conn().await?);
            }
            let conn_ = c.as_mut().expect("connected");
            match op {
                Op::Select => {
                    let id = 1 + draw % n.rows_per_table;
                    conn_
                        .exec_drop(format!("SELECT c FROM {} WHERE id = ?", table(&n, t)), (id,))
                        .await?;
                }
                Op::Update => {
                    let id = owned_id(draw, conn, n.connections, n.rows_per_table)
                        .context("the connection owns no rows")?;
                    conn_
                        .exec_drop(format!("UPDATE {} SET k = k + 1 WHERE id = ?", table(&n, t)), (id,))
                        .await?;
                }
                Op::Insert => {
                    let id = insert_id(base, inserts, conn, n.connections)
                        .context("the run's insert id space is used up")?;
                    inserts += 1;
                    conn_
                        .exec_drop(
                            format!("INSERT INTO {} (id, k, c, pad) VALUES (?, ?, ?, ?)", table(&n, t)),
                            (id, (draw % 1_000_000) as i64, format!("{draw:0>120}"), &pad),
                        )
                        .await?;
                }
            }
            Ok(())
        }
        .await;
        let lat = tokio::time::Instant::now().duration_since(at).as_micros();
        let mut g = tally.lock();
        match r {
            Ok(()) => {
                g.done[op as usize] += 1;
                g.lat_us.push(u32::try_from(lat).unwrap_or(u32::MAX));
            }
            Err(e) => {
                g.errors += 1;
                if g.first_error.is_none() {
                    g.first_error =
                        Some(format!("{e:#}").chars().take(300).collect());
                }
                drop(g);
                c = None;
            }
        }
    }
}

/// The server's lock-wait counters at this instant.
async fn locks(pool: &Pool) -> Value {
    let r: Result<Value> = async {
        let mut c = pool.get_conn().await?;
        let rows: Vec<(String, String)> = c
            .query(
                "SHOW GLOBAL STATUS WHERE Variable_name IN \
                 ('Innodb_row_lock_waits', 'Innodb_row_lock_time', 'Innodb_row_lock_current_waits')",
            )
            .await?;
        let mut v = serde_json::Map::new();
        for (k, val) in rows {
            v.insert(k.to_lowercase(), json!(val.parse::<u64>().ok()));
        }
        let mdl: Option<(u64, u64)> = c
            .query_first(
                "SELECT COUNT_STAR, SUM_TIMER_WAIT FROM performance_schema.events_waits_summary_global_by_event_name \
                 WHERE EVENT_NAME = 'wait/lock/metadata/sql/mdl'",
            )
            .await?;
        let (count, ps) = mdl.context("the metadata lock instrument is absent")?;
        v.insert("mdl_waits".into(), json!(count));
        v.insert("mdl_wait_ps".into(), json!(ps));
        let pending: Option<u64> = c
            .query_first("SELECT COUNT(*) FROM performance_schema.metadata_locks WHERE LOCK_STATUS = 'PENDING'")
            .await?;
        v.insert("mdl_pending".into(), json!(pending));
        Ok(Value::Object(v))
    }
    .await;
    r.unwrap_or_else(|e| json!({"error": format!("{e:#}")}))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mix() -> NeighbourMix {
        NeighbourMix {
            point_select: 0.7,
            update: 0.2,
            insert: 0.1,
        }
    }

    /// Together the connections offer the configured rate: the operations
    /// due in any second are `ops_per_sec`, spread over every connection.
    #[test]
    fn the_connections_interleave_to_the_offered_rate() {
        let (c, rate) = (4, 100.0);
        let mut due: Vec<f64> = (0..c)
            .flat_map(|conn| {
                (0..50).map(move |k| scheduled_secs(k, conn, c, rate))
            })
            .collect();
        due.sort_by(f64::total_cmp);
        assert_eq!(due.iter().filter(|t| **t < 1.0).count(), 100);
        for w in due.windows(2) {
            assert!((w[1] - w[0] - 0.01).abs() < 1e-9, "{w:?}");
        }
    }

    /// No two connections update the same row, and every owned id is a
    /// filled row.
    #[test]
    fn update_ids_are_owned_by_one_connection() {
        let (c, rows) = (3, 10);
        let mut seen = std::collections::BTreeMap::new();
        for conn in 0..c {
            for draw in 0..100 {
                let id = owned_id(draw, conn, c, rows).unwrap();
                assert!((1..=rows).contains(&id));
                assert_eq!(*seen.entry(id).or_insert(conn), conn, "id {id}");
            }
        }
        assert_eq!(seen.len(), 10, "every row is owned");
        assert_eq!(
            owned_id(7, 5, 8, 3),
            None,
            "a connection past the rows owns none"
        );
    }

    /// Insert ids never repeat across connections, never hit a filled row,
    /// and a later run's start never reuses an earlier run's ids.
    #[test]
    fn insert_ids_are_unique_within_and_across_runs() {
        let rows = 250_000;
        let (b1, b2) =
            (run_base(1_795_000_000, rows), run_base(1_795_000_001, rows));
        let mut ids = std::collections::BTreeSet::new();
        for conn in 0..4 {
            for k in 0..1000 {
                assert!(ids.insert(insert_id(b1, k, conn, 4).unwrap()));
            }
        }
        assert!(*ids.first().unwrap() > rows);
        let last_of_run1 =
            insert_id(b1, INSERTS_PER_RUN / 4 - 1, 3, 4).unwrap();
        assert!(insert_id(b2, 0, 0, 4).unwrap() > last_of_run1);
        assert_eq!(insert_id(b1, INSERTS_PER_RUN / 4, 0, 4), None);
        assert_eq!(run_base(0, rows), rows + 1);
    }

    #[test]
    fn the_mix_splits_draws_by_share() {
        let m = mix();
        assert_eq!(pick(0.0, &m), Op::Select);
        assert_eq!(pick(0.69, &m), Op::Select);
        assert_eq!(pick(0.7, &m), Op::Update);
        assert_eq!(pick(0.89, &m), Op::Update);
        assert_eq!(pick(0.9, &m), Op::Insert);
        assert_eq!(pick(0.999, &m), Op::Insert);
    }

    #[test]
    fn percentiles_are_nearest_rank() {
        let v: Vec<u32> = (1..=1000).collect();
        assert_eq!(pct(&v, 0.5), Some(500));
        assert_eq!(pct(&v, 0.99), Some(990));
        assert_eq!(pct(&v, 0.999), Some(999));
        assert_eq!(pct(&[7], 0.999), Some(7));
        assert_eq!(pct(&[], 0.5), None);
    }
}
