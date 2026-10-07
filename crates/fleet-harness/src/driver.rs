//! The workload driver (design section 4.3).
//!
//! Per server, `writers_per_server` connections write transactions of
//! `rows_per_txn` operations at the configured rate, on tables chosen from
//! the active set. Every committed operation is appended to a ledger.
//!
//! Correctness of the expectations, by construction:
//! - **One writer per table**: a table belongs to the writer `hash(table) %
//!   writers`, so a row's operations commit in version order and the
//!   stream's order can be checked against versions.
//! - **Versions** come from one increasing sequence per server.
//! - **Deletes are stamped**: the row is first updated to the delete's
//!   version in the same transaction, so the delete's before image carries a
//!   version the ledger knows without tracking every row (the extra update is
//!   counted as a stamp, not as part of the operation mix).
//! - **Row ids are namespaced per run** (`run_tag << 40`), so a reused fixture
//!   never collides with rows of earlier runs.
//!
//! DDL migrations and onboarding write probe rows whose new column must
//! arrive with the expected value ([`crate::verify::Probes`]).

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use chrono::Utc;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::Serialize;
use tokio::sync::watch;

use crate::activeset::ActiveSet;
use crate::config::{OpMix, RunConfig, Server};
use crate::ledger::{Op, Record, Writer};
use crate::topology::{self, Naming, RowKey, TableRef, Template};
use crate::verify::Probes;

/// Live rate control for one server (hot-cluster scenarios change it).
#[derive(Debug)]
pub struct Rate {
    bits: AtomicU64,
}

impl Rate {
    pub fn new(per_sec: f64) -> Arc<Rate> {
        Arc::new(Rate {
            bits: AtomicU64::new(per_sec.to_bits()),
        })
    }

    pub fn set(&self, per_sec: f64) {
        self.bits.store(per_sec.to_bits(), Ordering::Relaxed);
    }

    pub fn get(&self) -> f64 {
        f64::from_bits(self.bits.load(Ordering::Relaxed))
    }
}

/// The rate at `elapsed`: the average, with deterministic bursts at peak
/// for `peak_fraction` of every minute.
pub fn scheduled_rate(
    avg: f64,
    peak: f64,
    peak_fraction: f64,
    elapsed: Duration,
) -> f64 {
    let in_minute = elapsed.as_secs_f64() % 60.0;
    if in_minute < peak_fraction * 60.0 {
        peak
    } else {
        avg
    }
}

/// Counters for one server.
#[derive(Debug, Default)]
pub struct Counters {
    pub ops: AtomicU64,
    pub inserts: AtomicU64,
    pub updates: AtomicU64,
    pub deletes: AtomicU64,
    pub stamps: AtomicU64,
    pub txns: AtomicU64,
    pub errors: AtomicU64,
    pub ddl_statements: AtomicU64,
    pub probes: AtomicU64,
    /// Failed transactions whose outcome was read back from MySQL.
    pub reconciled_committed: AtomicU64,
    pub reconciled_rolled_back: AtomicU64,
    /// Failed transactions whose outcome could not be determined.
    pub uncertain_txns: AtomicU64,
    pub uncertain_ops: AtomicU64,
    /// Tables no longer written because of an uncertain transaction.
    pub retired_tables: AtomicU64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct CountersSnapshot {
    pub ops: u64,
    pub inserts: u64,
    pub updates: u64,
    pub deletes: u64,
    pub delete_stamps: u64,
    pub txns: u64,
    pub errors: u64,
    pub ddl_statements: u64,
    pub probes: u64,
    pub reconciled_committed: u64,
    pub reconciled_rolled_back: u64,
    pub uncertain_txns: u64,
    pub uncertain_ops: u64,
    pub retired_tables: u64,
}

impl Counters {
    pub fn snapshot(&self) -> CountersSnapshot {
        let g = |a: &AtomicU64| a.load(Ordering::Relaxed);
        CountersSnapshot {
            ops: g(&self.ops),
            inserts: g(&self.inserts),
            updates: g(&self.updates),
            deletes: g(&self.deletes),
            delete_stamps: g(&self.stamps),
            txns: g(&self.txns),
            errors: g(&self.errors),
            ddl_statements: g(&self.ddl_statements),
            probes: g(&self.probes),
            reconciled_committed: g(&self.reconciled_committed),
            reconciled_rolled_back: g(&self.reconciled_rolled_back),
            uncertain_txns: g(&self.uncertain_txns),
            uncertain_ops: g(&self.uncertain_ops),
            retired_tables: g(&self.retired_tables),
        }
    }
}

/// Shared by the writers and DDL tasks of one server.
pub struct ServerCtx {
    pub index: u16,
    pub server: Server,
    pub opts: Opts,
    pub naming: Naming,
    pub templates: Arc<Vec<Template>>,
    pub active: Arc<ActiveSet>,
    pub rate: Arc<Rate>,
    pub seq: Arc<AtomicU64>,
    pub counters: Arc<Counters>,
    pub probes: Probes,
    pub run_tag: u64,
    /// The configured workload so far (operations x 1000), integrated over
    /// the peak schedule and any rate change ([`target_integrator`]).
    pub target_milli_ops: Arc<AtomicU64>,
}

impl ServerCtx {
    pub fn new(
        cfg: &RunConfig,
        index: u16,
        server: &Server,
        probes: Probes,
        run_tag: u64,
    ) -> ServerCtx {
        let t = &cfg.topology;
        let opts = Opts::from(
            OptsBuilder::default()
                .ip_or_hostname(server.host.clone())
                .tcp_port(server.port)
                .user(Some(t.admin_user.clone()))
                .pass(Some(t.admin_password.clone())),
        );
        let templates: Arc<Vec<Template>> = Arc::new(
            (0..t.tables_per_database.value)
                .map(|i| Template::generate(t.seed, i as u16))
                .collect(),
        );
        ServerCtx {
            index,
            server: server.clone(),
            opts,
            naming: Naming {
                database_prefix: t.database_prefix.clone(),
            },
            templates,
            active: Arc::new(ActiveSet::new(
                index,
                t.databases_per_server.value,
                t.tables_per_database.value,
                &cfg.traffic.active_set.value,
                cfg.traffic.zipf_exponent.value,
                t.seed,
            )),
            rate: Rate::new(cfg.traffic.changes_per_sec_avg.value),
            seq: Arc::new(AtomicU64::new(1)),
            counters: Arc::new(Counters::default()),
            probes,
            run_tag,
            target_milli_ops: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Operations the configuration asked for so far.
    pub fn target_ops(&self) -> f64 {
        self.target_milli_ops.load(Ordering::Relaxed) as f64 / 1_000.0
    }

    fn next_version(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed)
    }
}

fn owner(table: &TableRef, writers: u32) -> u32 {
    let mut h = DefaultHasher::new();
    table.hash(&mut h);
    (h.finish() % u64::from(writers.max(1))) as u32
}

fn now_micros() -> i64 {
    Utc::now().timestamp_micros()
}

fn sql_time(micros: i64) -> String {
    chrono::DateTime::from_timestamp_micros(micros)
        .expect("valid time")
        .format("%Y-%m-%d %H:%M:%S%.6f")
        .to_string()
}

/// The values of one written row.
#[derive(Clone, Copy)]
struct RowWrite<'a> {
    id: u64,
    version: u64,
    at: i64,
    payload: &'a str,
    /// A migrated column and its probe value.
    extra: Option<(&'a str, i64)>,
}

/// The INSERT of a full row of template `t`.
fn insert_sql(
    db: &str,
    table: &str,
    t: &Template,
    row: &RowWrite<'_>,
) -> String {
    let RowWrite {
        id,
        version,
        at,
        payload,
        extra,
    } = *row;
    let mut cols = vec!["`id`", "`version`", "`committed_at`", "`payload`"]
        .into_iter()
        .map(String::from)
        .collect::<Vec<_>>();
    let mut vals = vec![
        id.to_string(),
        version.to_string(),
        format!("'{}'", sql_time(at)),
        format!("'{payload}'"),
    ];
    for (c, k) in &t.columns {
        cols.push(format!("`{c}`"));
        vals.push(k.literal(version));
    }
    if let Some((c, v)) = extra {
        cols.push(format!("`{c}`"));
        vals.push(v.to_string());
    }
    format!(
        "INSERT INTO `{db}`.`{table}` ({}) VALUES ({})",
        cols.join(", "),
        vals.join(", ")
    )
}

/// Rows of this run in one table: ids `[lo, hi)` below the run's id base.
#[derive(Debug, Clone, Copy, Default)]
struct RowRange {
    lo: u64,
    hi: u64,
}

/// One writer of one server.
pub async fn writer(
    ctx: Arc<ServerCtx>,
    cfg: Arc<RunConfig>,
    writer_index: u32,
    ledger: PathBuf,
    mut stop: watch::Receiver<bool>,
) -> Result<u64> {
    let writers = cfg.traffic.writers_per_server.max(1);
    let mix: OpMix = cfg.traffic.op_mix.value;
    let rows_per_txn = cfg.traffic.rows_per_txn.value.max(1);
    let payload = "x".repeat(cfg.traffic.row_bytes.value.min(4096) as usize);
    let (avg, peak, peak_fraction) = (
        cfg.traffic.changes_per_sec_avg.value,
        cfg.traffic.changes_per_sec_peak.value,
        cfg.traffic.peak_fraction.value,
    );
    let base = ctx.run_tag << 40;
    let mut rng = StdRng::seed_from_u64(
        cfg.topology.seed
            ^ ((u64::from(ctx.index)) << 20)
            ^ u64::from(writer_index),
    );
    let mut rows: HashMap<TableRef, RowRange> = HashMap::new();
    // Tables of a transaction whose outcome is unknown: their row ranges
    // may be off, so they are not written again in this run.
    let mut poisoned: HashSet<TableRef> = HashSet::new();
    let mut out = Writer::create(&ledger)?;
    let mut uncertain = Writer::create(&ledger.with_extension("uncertain"))?;
    let mut conn = Conn::new(ctx.opts.clone())
        .await
        .with_context(|| format!("writer connection to {}", ctx.server.name))?;
    let started = Instant::now();
    let mut next_at = Instant::now();
    loop {
        if *stop.borrow() {
            break;
        }
        // Pace: this writer's share of the server's scheduled rate.
        let scale = ctx.rate.get() / avg.max(f64::MIN_POSITIVE);
        let rate = scheduled_rate(avg, peak, peak_fraction, started.elapsed())
            * scale
            / f64::from(writers);
        let interval =
            Duration::from_secs_f64(f64::from(rows_per_txn) / rate.max(0.001));
        next_at += interval;
        let now = Instant::now();
        if next_at > now {
            tokio::select! {
                _ = tokio::time::sleep(next_at - now) => {}
                _ = stop.changed() => break,
            }
        } else if now - next_at > Duration::from_secs(1) {
            next_at = now; // do not accumulate unbounded catch-up
        }

        let mut sql = Vec::new();
        let mut records = Vec::new();
        let mut kinds = Vec::new();
        let mut planned: Vec<Planned> = Vec::new();
        for _ in 0..rows_per_txn {
            let table = loop {
                let t = ctx.active.pick(started.elapsed().as_secs(), &mut rng);
                if owner(&t, writers) == writer_index && !poisoned.contains(&t)
                {
                    break t;
                }
            };
            let range = rows.entry(table).or_default();
            let db = ctx.naming.database(table.db);
            let tname = ctx.naming.table(table.table);
            let template = &ctx.templates[table.table as usize];
            let roll: f64 = rng.random();
            let op = if range.lo == range.hi || roll < mix.insert {
                Op::Insert
            } else if roll < mix.insert + mix.update {
                Op::Update
            } else {
                Op::Delete
            };
            let version = ctx.next_version();
            let at = now_micros();
            let (id, statements) = match op {
                Op::Insert => {
                    let id = base + range.hi;
                    range.hi += 1;
                    (
                        id,
                        vec![insert_sql(
                            &db,
                            &tname,
                            template,
                            &RowWrite {
                                id,
                                version,
                                at,
                                payload: &payload,
                                extra: None,
                            },
                        )],
                    )
                }
                Op::Update => {
                    let id = base + rng.random_range(range.lo..range.hi);
                    (
                        id,
                        vec![format!(
                            "UPDATE `{db}`.`{tname}` SET `version` = {version}, `committed_at` = '{}' WHERE `id` = {id}",
                            sql_time(at)
                        )],
                    )
                }
                Op::Delete => {
                    let id = base + range.lo;
                    range.lo += 1;
                    (
                        id,
                        vec![
                            format!(
                                "UPDATE `{db}`.`{tname}` SET `version` = {version}, `committed_at` = '{}' WHERE `id` = {id}",
                                sql_time(at)
                            ),
                            format!(
                                "DELETE FROM `{db}`.`{tname}` WHERE `id` = {id}"
                            ),
                        ],
                    )
                }
            };
            sql.extend(statements);
            planned.push(Planned {
                db: db.clone(),
                table: tname.clone(),
                row: RowKey { table, id },
                version,
                op,
            });
            let row = RowKey { table, id };
            if op == Op::Delete {
                // The stamp (an update) and the delete share the version.
                records.push(Record {
                    row,
                    version,
                    op: Op::Update,
                    at_micros: at,
                    partition: -1,
                    offset: -1,
                });
            }
            records.push(Record {
                row,
                version,
                op,
                at_micros: at,
                partition: -1,
                offset: -1,
            });
            kinds.push(op);
        }
        let txn = format!("START TRANSACTION; {}; COMMIT", sql.join("; "));
        match conn.query_drop(txn).await {
            Ok(()) => {
                for r in &records {
                    out.append(r)?;
                }
                let c = &ctx.counters;
                c.txns.fetch_add(1, Ordering::Relaxed);
                for op in kinds {
                    c.ops.fetch_add(1, Ordering::Relaxed);
                    match op {
                        Op::Insert => c.inserts.fetch_add(1, Ordering::Relaxed),
                        Op::Update => c.updates.fetch_add(1, Ordering::Relaxed),
                        Op::Delete => {
                            c.stamps.fetch_add(1, Ordering::Relaxed);
                            c.deletes.fetch_add(1, Ordering::Relaxed)
                        }
                    };
                }
            }
            Err(e) => {
                // A lost reply leaves the commit unknown: read the rows
                // back (this writer is their only writer) to learn it.
                let c = &ctx.counters;
                c.errors.fetch_add(1, Ordering::Relaxed);
                tracing_like_warn(&ctx.server.name, &e.to_string());
                match reconcile(&ctx, &mut conn, &planned, &mut stop).await {
                    Some(true) => {
                        for r in &records {
                            out.append(r)?;
                        }
                        c.reconciled_committed.fetch_add(1, Ordering::Relaxed);
                        c.txns.fetch_add(1, Ordering::Relaxed);
                        for op in kinds {
                            c.ops.fetch_add(1, Ordering::Relaxed);
                            match op {
                                Op::Insert => {
                                    c.inserts.fetch_add(1, Ordering::Relaxed)
                                }
                                Op::Update => {
                                    c.updates.fetch_add(1, Ordering::Relaxed)
                                }
                                Op::Delete => {
                                    c.stamps.fetch_add(1, Ordering::Relaxed);
                                    c.deletes.fetch_add(1, Ordering::Relaxed)
                                }
                            };
                        }
                    }
                    Some(false) => {
                        // Not committed: undo the row-range changes.
                        for p in planned.iter().rev() {
                            if let Some(range) = rows.get_mut(&p.row.table) {
                                match p.op {
                                    Op::Insert => range.hi -= 1,
                                    Op::Delete => range.lo -= 1,
                                    Op::Update => {}
                                }
                            }
                        }
                        c.reconciled_rolled_back
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    None => {
                        // Undeterminable (the run stopped first, or the
                        // rows contradict each other): expected neither way,
                        // and the tables are retired.
                        c.uncertain_txns.fetch_add(1, Ordering::Relaxed);
                        c.uncertain_ops
                            .fetch_add(planned.len() as u64, Ordering::Relaxed);
                        for r in &records {
                            uncertain.append(r)?;
                            if poisoned.insert(r.row.table) {
                                c.retired_tables
                                    .fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
            }
        }
    }
    conn.disconnect().await.ok();
    uncertain.finish()?;
    out.finish()
}

/// One operation of a transaction, for reconciliation.
#[derive(Debug, Clone)]
struct Planned {
    db: String,
    table: String,
    row: RowKey,
    version: u64,
    op: Op,
}

/// What a row should look like if the transaction committed: present at
/// the version of its last operation, or absent after a delete. A row the
/// transaction both created and deleted looks the same either way and is
/// left out (`None`).
fn committed_states(
    planned: &[Planned],
) -> Vec<(&Planned, Option<Option<u64>>)> {
    let mut last: HashMap<RowKey, (usize, bool)> = HashMap::new();
    for (i, p) in planned.iter().enumerate() {
        let created = last.get(&p.row).map_or(p.op == Op::Insert, |(_, c)| *c);
        last.insert(p.row, (i, created));
    }
    let mut out: Vec<_> = last
        .into_values()
        .map(|(i, created)| {
            let p = &planned[i];
            let expect = match p.op {
                Op::Delete if created => None, // ambiguous
                Op::Delete => Some(None),
                _ => Some(Some(p.version)),
            };
            (p, expect)
        })
        .collect();
    out.sort_by_key(|(p, _)| (p.row, p.version));
    out
}

/// Decide from the rows read back: `Some(true)` committed, `Some(false)`
/// not committed, `None` when nothing decides or the rows disagree.
fn decide(states: &[(Option<Option<u64>>, Option<u64>)]) -> Option<bool> {
    let mut verdicts = states
        .iter()
        .filter_map(|(expect, actual)| expect.map(|e| e == *actual));
    let first = verdicts.next()?;
    verdicts.all(|v| v == first).then_some(first)
}

/// Learn a failed transaction's outcome by reading its rows back, retrying
/// the connection until it succeeds or the run stops.
async fn reconcile(
    ctx: &ServerCtx,
    conn: &mut Conn,
    planned: &[Planned],
    stop: &mut watch::Receiver<bool>,
) -> Option<bool> {
    conn.query_drop("ROLLBACK").await.ok();
    let expectations = committed_states(planned);
    loop {
        let mut states = Vec::with_capacity(expectations.len());
        let mut failed = false;
        for (p, expect) in &expectations {
            let q = format!(
                "SELECT `version` FROM `{}`.`{}` WHERE `id` = {}",
                p.db, p.table, p.row.id
            );
            match conn.query_first::<u64, _>(q).await {
                Ok(actual) => states.push((*expect, actual)),
                Err(_) => {
                    failed = true;
                    break;
                }
            }
        }
        if !failed {
            return decide(&states);
        }
        if *stop.borrow() {
            return None;
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(500)) => {}
            _ = stop.changed() => return None,
        }
        if let Ok(c) = Conn::new(ctx.opts.clone()).await {
            *conn = c;
        }
    }
}

/// Integrate the configured workload of one server (the peak schedule
/// times any rate change) into `ctx.target_milli_ops` until stopped.
pub async fn target_integrator(
    ctx: Arc<ServerCtx>,
    cfg: Arc<RunConfig>,
    mut stop: watch::Receiver<bool>,
) {
    let (avg, peak, pf) = (
        cfg.traffic.changes_per_sec_avg.value,
        cfg.traffic.changes_per_sec_peak.value,
        cfg.traffic.peak_fraction.value,
    );
    let started = Instant::now();
    let mut last = started;
    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
            _ = stop.changed() => break,
        }
        let now = Instant::now();
        let scale = ctx.rate.get() / avg.max(f64::MIN_POSITIVE);
        let rate = scheduled_rate(avg, peak, pf, now - started) * scale;
        let add = rate * (now - last).as_secs_f64() * 1_000.0;
        ctx.target_milli_ops
            .fetch_add(add as u64, Ordering::Relaxed);
        last = now;
    }
}

fn tracing_like_warn(server: &str, error: &str) {
    eprintln!("  driver {server}: transaction failed: {error}");
}

/// Apply one migration statement to every customer database of the server
/// (in rollout order), writing a probe row after each `ALTER TABLE`.
pub async fn migration(
    ctx: Arc<ServerCtx>,
    cfg: Arc<RunConfig>,
    generation: u32,
    ledger: &Path,
    mut stop: watch::Receiver<bool>,
) -> Result<u64> {
    let ddl = &cfg.ddl;
    let statements = ddl.migration_statements.as_ref().map_or(0, |p| p.value);
    let pace = ddl
        .migration_pace
        .as_ref()
        .map_or(1.0, |p| p.value)
        .max(0.001);
    let (group, gap) = match ddl.migration_rollout.as_ref().map(|p| &p.value) {
        Some(crate::config::Rollout::Staged {
            group_size,
            gap_secs,
        }) => (*group_size, Duration::from_secs(*gap_secs)),
        _ => (u32::MAX, Duration::ZERO),
    };
    let databases = cfg.topology.databases_per_server.value;
    let tables = cfg.topology.tables_per_database.value as u16;
    let mut out = Writer::create(ledger)?;
    let mut conn = Conn::new(ctx.opts.clone()).await?;
    // Probe ids live in their own range of the run's id space.
    let mut probe_id = (ctx.run_tag << 40) | (1 << 39);
    let mut done = 0u64;
    for s in 0..statements {
        let column = topology::migration_column(generation, s);
        let t = topology::migration_table(s, tables);
        for db in 0..databases {
            if *stop.borrow() {
                conn.disconnect().await.ok();
                out.finish()?;
                return Ok(done);
            }
            if db > 0 && db % group == 0 && !gap.is_zero() {
                tokio::select! {
                    _ = tokio::time::sleep(gap) => {}
                    _ = stop.changed() => {}
                }
            }
            let dbn = ctx.naming.database(db);
            let tname = ctx.naming.table(t);
            conn.query_drop(topology::migration_sql(&dbn, &tname, &column))
                .await
                .with_context(|| format!("migrate {dbn}.{tname}"))?;
            ctx.counters.ddl_statements.fetch_add(1, Ordering::Relaxed);
            let version = ctx.next_version();
            let at = now_micros();
            let value = (version % 1_000_000_000) as i64;
            probe_id += 1;
            let row = RowKey {
                table: TableRef {
                    server: ctx.index,
                    db,
                    table: t,
                },
                id: probe_id,
            };
            ctx.probes.expect(row, version, column.clone(), value);
            conn.query_drop(insert_sql(
                &dbn,
                &tname,
                &ctx.templates[t as usize],
                &RowWrite {
                    id: probe_id,
                    version,
                    at,
                    payload: "probe",
                    extra: Some((&column, value)),
                },
            ))
            .await?;
            out.append(&Record {
                row,
                version,
                op: Op::Insert,
                at_micros: at,
                partition: -1,
                offset: -1,
            })?;
            ctx.counters.probes.fetch_add(1, Ordering::Relaxed);
            done += 1;
            tokio::time::sleep(Duration::from_secs_f64(1.0 / pace)).await;
        }
    }
    conn.disconnect().await.ok();
    out.finish()?;
    Ok(done)
}

/// First database index of onboarded customers: beyond any fixture, so
/// offboarding (which drops only databases onboarded in this run) never
/// touches the reusable fixture.
pub const ONBOARD_BASE: u32 = 90_000;

#[derive(Debug, Clone, Default, Serialize)]
pub struct LifecycleReport {
    pub onboarded: u32,
    pub offboarded: u32,
    pub rows_written: u64,
}

/// Onboard and offboard customers at the configured rates until stopped:
/// onboarding creates a database with every template table and writes one
/// row per table; offboarding drops the oldest database onboarded in this
/// run.
pub async fn lifecycle(
    ctx: Arc<ServerCtx>,
    cfg: Arc<RunConfig>,
    ledger: &Path,
    mut stop: watch::Receiver<bool>,
) -> Result<LifecycleReport> {
    let rate = |p: &Option<crate::config::Param<f64>>| {
        p.as_ref().map_or(0.0, |p| p.value)
    };
    let (on, off) = (
        rate(&cfg.lifecycle.onboard_per_hour),
        rate(&cfg.lifecycle.offboard_per_hour),
    );
    let mut report = LifecycleReport::default();
    if on <= 0.0 && off <= 0.0 {
        return Ok(report);
    }
    let mut out = Writer::create(ledger)?;
    let mut conn = Conn::new(ctx.opts.clone()).await?;
    let every = |per_hour: f64| {
        (per_hour > 0.0).then(|| Duration::from_secs_f64(3_600.0 / per_hour))
    };
    let (on_every, off_every) = (every(on), every(off));
    let started = Instant::now();
    let (mut next_on, mut next_off) = (on_every, off_every);
    let mut live: std::collections::VecDeque<u32> = Default::default();
    let mut next_index = ONBOARD_BASE + (ctx.run_tag as u32 % 1_000) * 9;
    let id_base = (ctx.run_tag << 40) | (1 << 38);
    loop {
        let due = [next_on, next_off].into_iter().flatten().min();
        let Some(due) = due else { break };
        let wait = due.saturating_sub(started.elapsed());
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            _ = stop.changed() => break,
        }
        if *stop.borrow() {
            break;
        }
        if next_on == Some(due) {
            let db = next_index;
            next_index += 1;
            let name = ctx.naming.database(db);
            conn.query_drop(format!("CREATE DATABASE IF NOT EXISTS `{name}`"))
                .await?;
            for t in ctx.templates.iter() {
                let tname = ctx.naming.table(t.index);
                conn.query_drop(t.create_sql(&name, &tname)).await?;
                let version = ctx.next_version();
                let at = now_micros();
                let id = id_base + u64::from(t.index);
                conn.query_drop(insert_sql(
                    &name,
                    &tname,
                    t,
                    &RowWrite {
                        id,
                        version,
                        at,
                        payload: "onboard",
                        extra: None,
                    },
                ))
                .await?;
                out.append(&Record {
                    row: RowKey {
                        table: TableRef {
                            server: ctx.index,
                            db,
                            table: t.index,
                        },
                        id,
                    },
                    version,
                    op: Op::Insert,
                    at_micros: at,
                    partition: -1,
                    offset: -1,
                })?;
                report.rows_written += 1;
            }
            live.push_back(db);
            report.onboarded += 1;
            next_on = on_every.map(|e| due + e);
        }
        if next_off == Some(due) {
            if let Some(db) = live.pop_front() {
                conn.query_drop(format!(
                    "DROP DATABASE IF EXISTS `{}`",
                    ctx.naming.database(db)
                ))
                .await?;
                report.offboarded += 1;
            }
            next_off = off_every.map(|e| due + e);
        }
    }
    conn.disconnect().await.ok();
    out.finish()?;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_schedule_bursts_to_peak_for_its_fraction_of_each_minute() {
        let r = |s| scheduled_rate(100.0, 1_000.0, 0.1, Duration::from_secs(s));
        assert_eq!(r(0), 1_000.0);
        assert_eq!(r(5), 1_000.0);
        assert_eq!(r(6), 100.0);
        assert_eq!(r(65), 1_000.0);
        assert_eq!(scheduled_rate(100.0, 1_000.0, 0.0, Duration::ZERO), 100.0);
    }

    #[test]
    fn every_table_has_exactly_one_writer() {
        let mut counts = [0u32; 4];
        for db in 0..100 {
            for table in 0..50 {
                let t = TableRef {
                    server: 0,
                    db,
                    table,
                };
                let w = owner(&t, 4);
                assert_eq!(w, owner(&t, 4), "stable");
                counts[w as usize] += 1;
            }
        }
        assert!(counts.iter().all(|&c| c > 1_000), "spread: {counts:?}");
    }

    #[test]
    fn inserts_carry_every_template_column() {
        let t = Template::generate(1, 3);
        let sql = insert_sql(
            "cust_00001",
            "t003",
            &t,
            &RowWrite {
                id: 5,
                version: 9,
                at: 0,
                payload: "p",
                extra: Some(("m0001_00", 42)),
            },
        );
        assert!(sql.starts_with("INSERT INTO `cust_00001`.`t003`"));
        assert_eq!(sql.matches('`').count() / 2, 2 + 4 + t.columns.len() + 1);
        assert!(sql.contains("`m0001_00`") && sql.ends_with("42)"));
        assert!(sql.contains("'1970-01-01 00:00:00.000000'"));
    }

    #[test]
    fn rate_control_scales_the_schedule() {
        let r = Rate::new(100.0);
        r.set(1_000.0);
        assert_eq!(r.get(), 1_000.0);
    }

    fn planned(id: u64, version: u64, op: Op) -> Planned {
        Planned {
            db: "d".into(),
            table: "t".into(),
            row: RowKey {
                table: TableRef {
                    server: 0,
                    db: 0,
                    table: 0,
                },
                id,
            },
            version,
            op,
        }
    }

    #[test]
    fn reconciliation_follows_each_rows_last_operation() {
        // Insert row 1 (v5), then update it (v6) in the same transaction.
        let p = [
            planned(1, 5, Op::Insert),
            planned(1, 6, Op::Update),
            planned(2, 7, Op::Delete),
        ];
        let states = committed_states(&p);
        assert_eq!(states.len(), 2);
        let expect: Vec<_> = states.iter().map(|(_, e)| *e).collect();
        assert_eq!(expect, vec![Some(Some(6)), Some(None)]);
        // Committed: row 1 at v6, row 2 gone.
        assert_eq!(
            decide(&[(expect[0], Some(6)), (expect[1], None)]),
            Some(true)
        );
        // Rolled back: row 1 absent, row 2 still present at its old version.
        assert_eq!(
            decide(&[(expect[0], None), (expect[1], Some(3))]),
            Some(false)
        );
        // Contradictory rows decide nothing.
        assert_eq!(decide(&[(expect[0], Some(6)), (expect[1], Some(3))]), None);
    }

    #[test]
    fn a_row_created_and_deleted_in_the_transaction_decides_nothing() {
        let p = [planned(1, 5, Op::Insert), planned(1, 5, Op::Delete)];
        let states = committed_states(&p);
        assert_eq!(states[0].1, None);
        assert_eq!(
            decide(&[(None, None)]),
            None,
            "only ambiguous rows: undeterminable"
        );
    }
}
