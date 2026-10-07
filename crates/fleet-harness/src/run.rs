//! Running a scenario and sweeping source counts.
//!
//! One run: prepare the servers, create one pipeline per server through the
//! REST API, start the verifying consumer, the sampler and the writers, warm
//! up, execute the plan's steps (measuring recovery after each disruption),
//! stop, drain, check completeness offline, delete the pipelines and write
//! the result. A sweep repeats this for each source count and repetition,
//! and never assumes a count succeeds.

use std::collections::{BTreeMap, HashMap};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use chrono::Utc;
use parking_lot::Mutex;
use serde_json::json;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::config::{Param, RunConfig, Server};
use crate::consumer::{self, Drain};
use crate::deltaforge::{self, Api};
use crate::driver::{self, CountersSnapshot, ServerCtx};
use crate::fixture;
use crate::ledger;
use crate::measure::{self, Aggregate};
use crate::results::{
    Environment, RunResult, StepOutcome, Sweep, SweepPoint, Verdict,
};
use crate::scenario::{self, Action, Plan};
use crate::stats;
use crate::topology::Naming;
use crate::verify::{Probes, RecoveryMarks, Verifier};

/// Mechanics of a run that are not workload inputs.
#[derive(Debug, Clone)]
pub struct RunOptions {
    pub drain_idle: Duration,
    pub drain_max: Duration,
    pub recovery_timeout: Duration,
    pub start_timeout: Duration,
    pub keep_pipelines: bool,
    /// Records per in-memory chunk of the external sort.
    pub sort_chunk_records: usize,
    /// Run files merged at once by the external sort (bounds descriptors).
    pub sort_fan_in: usize,
}

impl Default for RunOptions {
    fn default() -> Self {
        RunOptions {
            drain_idle: Duration::from_secs(30),
            drain_max: Duration::from_secs(1_800),
            recovery_timeout: Duration::from_secs(900),
            start_timeout: Duration::from_secs(900),
            keep_pipelines: false,
            sort_chunk_records: 8_000_000,
            sort_fan_in: ledger::DEFAULT_FAN_IN,
        }
    }
}

/// Run every source count and repetition of the configuration.
pub async fn sweep(cfg: RunConfig, opts: RunOptions) -> Result<Sweep> {
    cfg.validate()?;
    cfg.check_class()?;
    let cfg = Arc::new(cfg);
    let mut points = Vec::new();
    for &n in &cfg.run.sources {
        let mut runs: Vec<RunResult> = Vec::new();
        let mut failed = false;
        for rep in 1..=cfg.run.repetitions {
            match run_once(cfg.clone(), n, rep, &opts).await {
                Ok(r) => {
                    failed |= !r.verdict.repetition_ok;
                    runs.push(r);
                }
                Err(e) => {
                    eprintln!(
                        "run with {n} sources (repetition {rep}) failed: {e:#}"
                    );
                    failed = true;
                    break;
                }
            }
        }
        points.push(SweepPoint {
            sources: n,
            runs: runs.iter().map(|r| r.run_id.clone()).collect(),
            correctness_ok: !failed && !runs.is_empty(),
            budgets_ok: if runs.iter().any(|r| r.verdict.budgets_ok.is_none()) {
                None
            } else {
                Some(runs.iter().all(|r| r.verdict.budgets_ok == Some(true)))
            },
            achieved_ops_per_sec_total: runs
                .iter()
                .map(|r| r.achieved_ops_per_sec_total)
                .collect(),
            lag_p99_ms_max: runs
                .iter()
                .map(|r| r.lag_ms.values().filter_map(|s| s.p99).max())
                .collect(),
            memory_peak_bytes: runs
                .iter()
                .map(|r| r.resources.memory_peak_bytes)
                .collect(),
            cpu_cores_mean: runs
                .iter()
                .map(|r| r.resources.cpu_cores_mean)
                .collect(),
            connections_max: runs
                .iter()
                .map(|r| r.resources.connections_max.values().copied().max())
                .collect(),
        });
        if failed && cfg.run.stop_sweep_on_failure {
            eprintln!("stopping the sweep at {n} sources (failure)");
            break;
        }
    }
    let sweep = Sweep::summarize(cfg.class, &cfg.run.scenario, points);
    let dir = Path::new(&cfg.output_dir).join(cfg.class.dir());
    std::fs::create_dir_all(&dir)?;
    let name = format!(
        "sweep-{}-{}.json",
        cfg.run.scenario,
        Utc::now().format("%Y%m%dT%H%M%S")
    );
    std::fs::write(dir.join(name), serde_json::to_vec_pretty(&sweep)?)?;
    Ok(sweep)
}

struct Shared {
    cfg: Arc<RunConfig>,
    servers: Vec<(u16, Server)>,
    names: Vec<String>,
    api: Api,
    marks: RecoveryMarks,
    agg: Arc<Mutex<Aggregate>>,
    ctxs: Vec<Arc<ServerCtx>>,
    stop: watch::Receiver<bool>,
    work: PathBuf,
    run_tag: u64,
}

/// One run with the first `n` servers.
pub async fn run_once(
    cfg: Arc<RunConfig>,
    n: u32,
    rep: u32,
    opts: &RunOptions,
) -> Result<RunResult> {
    let started_at = Utc::now();
    let run_tag = (started_at.timestamp() as u64) & ((1 << 23) - 1);
    let run_id = format!(
        "{}-{}-n{n}-r{rep}",
        started_at.format("%Y%m%dT%H%M%S"),
        cfg.run.scenario
    );
    let dir = RunResult::dir(&cfg.output_dir, cfg.class, &run_id);
    let work = Path::new(&cfg.verifier.work_dir).join(&run_id);
    std::fs::create_dir_all(&dir)?;
    std::fs::create_dir_all(&work)?;
    let plan: Plan = scenario::plan(&cfg)?;
    let mut run_cfg = (*cfg).clone();
    if let Some(p) = plan.active_set.clone() {
        run_cfg.traffic.active_set = Param {
            value: p,
            provenance: cfg.traffic.active_set.provenance,
        };
    }
    let cfg = Arc::new(run_cfg);
    let naming = Naming {
        database_prefix: cfg.topology.database_prefix.clone(),
    };
    let servers: Vec<(u16, Server)> = cfg.topology.servers[..n as usize]
        .iter()
        .enumerate()
        .map(|(i, s)| (i as u16, s.clone()))
        .collect();

    // Servers: CDC user, fixture check, settings.
    let mut environment = Environment::local();
    let mut fixture_tables = BTreeMap::new();
    let expected = u64::from(cfg.topology.databases_per_server.value)
        * u64::from(cfg.topology.tables_per_database.value);
    for (_, s) in &servers {
        fixture::ensure_cdc_user(&cfg, s).await?;
        let pool = fixture::admin_pool(&cfg, s, 1)?;
        let mut conn = pool.get_conn().await?;
        environment
            .mysql
            .insert(s.name.clone(), fixture::settings(&mut conn).await?);
        let like =
            format!("{}%", cfg.topology.database_prefix.replace('_', "\\_"));
        use mysql_async::prelude::Queryable;
        let tables: Option<u64> = conn
            .exec_first("SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA LIKE ?", (like,))
            .await?;
        let tables = tables.unwrap_or(0);
        fixture_tables.insert(s.name.clone(), tables);
        drop(conn);
        pool.disconnect().await.ok();
        if tables < expected {
            let msg = format!(
                "server {} has {tables} fixture tables, expected {expected}",
                s.name
            );
            if cfg.class == crate::config::RunClass::Qualification {
                bail!("{msg}: build the fixture first");
            }
            eprintln!("warning: {msg}");
        }
    }

    // Pipelines.
    let api = Api::new(&cfg.deltaforge.api_url)?;
    let names: Vec<String> = servers
        .iter()
        .map(|(_, s)| deltaforge::pipeline_name(&cfg, s))
        .collect();
    let specs: Vec<serde_json::Value> = servers
        .iter()
        .map(|(_, s)| deltaforge::render_spec(&cfg, s, &naming))
        .collect::<Result<_>>()?;
    for (name, spec) in names.iter().zip(&specs) {
        api.delete(name).await.ok();
        api.create(spec)
            .await
            .with_context(|| format!("create {name}"))?;
    }
    let creating = Instant::now();
    for name in &names {
        api.until_running(name, opts.start_timeout).await?;
    }
    let pipelines_running_secs = Some(creating.elapsed().as_secs_f64());

    // Verifier.
    let probes = Probes::default();
    let marks = RecoveryMarks::default();
    let verifier = Verifier::new(
        naming.clone(),
        cfg.verifier.expected_key,
        probes.clone(),
        marks.clone(),
        &work.join("consumed.bin"),
    )?
    .for_run(run_tag);
    let (consumer_stop_tx, consumer_stop) = watch::channel(false);
    let topic_to_server: HashMap<String, u16> = servers
        .iter()
        .map(|(i, s)| (deltaforge::topic(&cfg, s), *i))
        .collect();
    let pattern = consumer::topic_pattern(
        &cfg.deltaforge.topic_prefix,
        &servers
            .iter()
            .map(|(_, s)| s.name.clone())
            .collect::<Vec<_>>(),
    );
    let group = format!("fleet-harness-{run_id}");
    let consumer_task = {
        let brokers = cfg.verifier.kafka_brokers.clone();
        let drain = Drain {
            idle: opts.drain_idle,
            max: opts.drain_max,
        };
        tokio::spawn(async move {
            consumer::run(
                &brokers,
                &group,
                &pattern,
                topic_to_server,
                verifier,
                consumer_stop,
                drain,
            )
            .await
        })
    };

    // Sampler.
    let agg = Arc::new(Mutex::new(Aggregate::default()));
    let (sampler_stop_tx, sampler_stop) = watch::channel(false);
    let sampler_task = tokio::spawn(sampler(
        cfg.clone(),
        servers.clone(),
        agg.clone(),
        dir.join("samples.jsonl"),
        sampler_stop,
    ));

    // Writers.
    let ctxs: Vec<Arc<ServerCtx>> = servers
        .iter()
        .map(|(i, s)| {
            Arc::new(ServerCtx::new(&cfg, *i, s, probes.clone(), run_tag))
        })
        .collect();
    let (driver_stop_tx, driver_stop) = watch::channel(false);
    let mut writers: Vec<JoinHandle<Result<u64>>> = Vec::new();
    let integrators: Vec<JoinHandle<()>> = ctxs
        .iter()
        .map(|ctx| {
            tokio::spawn(driver::target_integrator(
                ctx.clone(),
                cfg.clone(),
                driver_stop.clone(),
            ))
        })
        .collect();
    for ctx in &ctxs {
        for w in 0..cfg.traffic.writers_per_server.max(1) {
            let path = work.join(format!("w-{}-{w}.bin", ctx.server.name));
            writers.push(tokio::spawn(driver::writer(
                ctx.clone(),
                cfg.clone(),
                w,
                path,
                driver_stop.clone(),
            )));
        }
    }

    // Warm-up, then the measured window.
    tokio::time::sleep(Duration::from_secs(cfg.run.warmup_secs)).await;
    *agg.lock() = Aggregate::default();
    let window_start = Instant::now();
    let ops_at_start: Vec<u64> =
        ctxs.iter().map(|c| c.counters.snapshot().ops).collect();
    let target_at_start: Vec<f64> =
        ctxs.iter().map(|c| c.target_ops()).collect();
    let shared = Shared {
        cfg: cfg.clone(),
        servers: servers.clone(),
        names: names.clone(),
        api: api.clone(),
        marks: marks.clone(),
        agg: agg.clone(),
        ctxs: ctxs.clone(),
        stop: driver_stop.clone(),
        work: work.clone(),
        run_tag,
    };
    let mut steps = Vec::new();
    let mut background: Vec<(usize, JoinHandle<Result<String>>)> = Vec::new();
    for step in &plan.steps {
        if step.at_secs >= cfg.run.duration_secs {
            continue;
        }
        let due = window_start + Duration::from_secs(step.at_secs);
        tokio::time::sleep_until(due.into()).await;
        let (outcome, task) =
            execute(&shared, &specs, step.at_secs, &step.action, opts).await;
        if let Some(t) = task {
            background.push((steps.len(), t));
        }
        steps.push(outcome);
    }
    tokio::time::sleep_until(
        (window_start + Duration::from_secs(cfg.run.duration_secs)).into(),
    )
    .await;
    let measured_secs = window_start.elapsed().as_secs_f64();
    let counters: Vec<CountersSnapshot> =
        ctxs.iter().map(|c| c.counters.snapshot()).collect();
    let target_at_end: Vec<f64> = ctxs.iter().map(|c| c.target_ops()).collect();

    // Stop and drain.
    driver_stop_tx.send(true).ok();
    let mut writer_errors = Vec::new();
    for w in writers {
        match w.await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => writer_errors.push(format!("{e:#}")),
            Err(e) => writer_errors.push(format!("writer task: {e}")),
        }
    }
    for i in integrators {
        i.await.ok();
    }
    let final_counters: Vec<CountersSnapshot> =
        ctxs.iter().map(|c| c.counters.snapshot()).collect();
    for (i, t) in background {
        match t.await? {
            Ok(note) => {
                steps[i].error =
                    (!note.is_empty()).then_some(note).or(steps[i].error.take())
            }
            Err(e) => steps[i].error = Some(format!("{e:#}")),
        }
    }
    consumer_stop_tx.send(true).ok();
    let verifier = consumer_task.await??;
    let (stream, lag, _spilled) = verifier.finish()?;
    sampler_stop_tx.send(true).ok();
    sampler_task.await?.ok();

    // Completeness, offline.
    let files = |suffix: &str| -> Result<Vec<PathBuf>> {
        Ok(std::fs::read_dir(&work)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().and_then(|e| e.to_str()) == Some(suffix))
            .filter(|p| {
                p.file_name()
                    .is_some_and(|f| f.to_string_lossy() != "consumed.bin")
            })
            .collect())
    };
    let (written, written_stats) = ledger::sort_files_with(
        &files("bin")?,
        &work.join("sorted-written"),
        opts.sort_chunk_records,
        opts.sort_fan_in,
    )?;
    let (uncertain, uncertain_stats) = ledger::sort_files_with(
        &files("uncertain")?,
        &work.join("sorted-uncertain"),
        opts.sort_chunk_records,
        opts.sort_fan_in,
    )?;
    let (consumed, consumed_stats) = ledger::sort_files_with(
        &[work.join("consumed.bin")],
        &work.join("sorted-consumed"),
        opts.sort_chunk_records,
        opts.sort_fan_in,
    )?;
    let completeness = ledger::compare(&written, &consumed, Some(&uncertain))?;

    if !opts.keep_pipelines {
        for name in &names {
            api.delete(name).await.ok();
        }
    }

    let by_name = |i: u16| {
        servers
            .iter()
            .find(|(j, _)| *j == i)
            .map(|(_, s)| s.name.clone())
            .unwrap_or_default()
    };
    let mut workload = BTreeMap::new();
    for (((_, s), (c, start)), (t0, t1)) in servers
        .iter()
        .zip(counters.iter().zip(&ops_at_start))
        .zip(target_at_start.iter().zip(&target_at_end))
    {
        let target_ops = t1 - t0;
        let committed_ops = c.ops - start;
        workload.insert(
            s.name.clone(),
            crate::results::Workload {
                target_ops,
                committed_ops,
                ratio: (target_ops > 0.0)
                    .then(|| committed_ops as f64 / target_ops),
            },
        );
    }
    let mut achieved = BTreeMap::new();
    for ((i, s), (c, start)) in
        servers.iter().zip(counters.iter().zip(&ops_at_start))
    {
        let _ = i;
        achieved.insert(
            s.name.clone(),
            (c.ops - start) as f64 / measured_secs.max(f64::MIN_POSITIVE),
        );
    }
    let mut result = RunResult {
        run_id: run_id.clone(),
        class: cfg.class,
        claims_allowed: cfg.class == crate::config::RunClass::Qualification
            && cfg.qualification_errors().is_empty(),
        scenario: cfg.run.scenario.clone(),
        sources: n,
        repetition: rep,
        started_at: started_at.to_rfc3339(),
        ended_at: Utc::now().to_rfc3339(),
        harness_revision: crate::git_revision().to_string(),
        placeholders: cfg.placeholders(),
        config: (*cfg).clone(),
        environment,
        fixture_tables,
        pipelines_running_secs,
        measured_secs,
        driver: servers
            .iter()
            .map(|(_, s)| s.name.clone())
            .zip(final_counters)
            .collect(),
        achieved_ops_per_sec_total: achieved.values().sum(),
        achieved_ops_per_sec: achieved,
        workload,
        writer_errors,
        sort: BTreeMap::from([
            ("written".to_string(), written_stats),
            ("uncertain".to_string(), uncertain_stats),
            ("consumed".to_string(), consumed_stats),
        ]),
        stream,
        lag_ms: lag
            .into_iter()
            .map(|(i, h)| (by_name(i), h.summary()))
            .collect(),
        resources: agg.lock().clone(),
        steps,
        completeness,
        budgets: Vec::new(),
        verdict: Verdict {
            correctness_ok: false,
            plan_executed: false,
            actions_not_run: 0,
            action_errors: 0,
            uncertainty_ok: false,
            workload_ok: None,
            budgets_ok: None,
            repetition_ok: false,
        },
    };
    result.evaluate();
    result.write(&dir)?;
    if result.verdict.correctness_ok {
        std::fs::remove_dir_all(&work).ok();
    }
    Ok(result)
}

/// Wait until every marked server delivered a later change (or time out),
/// then summarize.
async fn recovery(
    shared: &Shared,
    servers: &[u16],
    timeout: Duration,
) -> stats::Recovery {
    let deadline = Instant::now() + timeout;
    while shared.marks.pending() > 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let taken = shared.marks.take();
    let times: Vec<(String, Option<f64>)> = servers
        .iter()
        .map(|i| {
            let name = shared
                .servers
                .iter()
                .find(|(j, _)| j == i)
                .map(|(_, s)| s.name.clone())
                .unwrap_or_default();
            (name, taken.get(i).copied().flatten())
        })
        .collect();
    stats::recovery(&times)
}

fn now_micros() -> i64 {
    Utc::now().timestamp_micros()
}

/// Execute one step; long-running actions return a background task.
async fn execute(
    shared: &Shared,
    specs: &[serde_json::Value],
    at_secs: u64,
    action: &Action,
    opts: &RunOptions,
) -> (StepOutcome, Option<JoinHandle<Result<String>>>) {
    let mut out = StepOutcome {
        at_secs,
        action: action.clone(),
        not_run: scenario::unavailable(&shared.cfg, action),
        error: None,
        duration_secs: None,
        shutdown_secs: None,
        recovery: None,
    };
    if out.not_run.is_some() {
        return (out, None);
    }
    let started = Instant::now();
    let all: Vec<u16> = shared.servers.iter().map(|(i, _)| *i).collect();
    let hooks = &shared.cfg.actions;
    let mut task = None;
    let result: Result<Option<Vec<u16>>> = async {
        match action {
            Action::SetRate {
                servers,
                multiplier,
            } => {
                for ctx in shared.ctxs.iter().take(*servers) {
                    ctx.rate.set(
                        shared.cfg.traffic.changes_per_sec_avg.value
                            * multiplier,
                    );
                }
                Ok(None)
            }
            Action::Migration => {
                let handles: Vec<_> = shared
                    .ctxs
                    .iter()
                    .map(|ctx| {
                        let path = shared
                            .work
                            .join(format!("m-{}.bin", ctx.server.name));
                        let (ctx, cfg, stop) = (
                            ctx.clone(),
                            shared.cfg.clone(),
                            shared.stop.clone(),
                        );
                        let generation = (shared.run_tag % 10_000) as u32;
                        tokio::spawn(async move {
                            driver::migration(ctx, cfg, generation, &path, stop)
                                .await
                        })
                    })
                    .collect();
                task = Some(tokio::spawn(async move {
                    let mut total = 0;
                    for h in handles {
                        total += h.await??;
                    }
                    Ok(format!("migration statements with probes: {total}"))
                }));
                Ok(None)
            }
            Action::Lifecycle => {
                let handles: Vec<_> = shared
                    .ctxs
                    .iter()
                    .map(|ctx| {
                        let path = shared
                            .work
                            .join(format!("l-{}.bin", ctx.server.name));
                        let (ctx, cfg, stop) = (
                            ctx.clone(),
                            shared.cfg.clone(),
                            shared.stop.clone(),
                        );
                        tokio::spawn(async move {
                            driver::lifecycle(ctx, cfg, &path, stop).await
                        })
                    })
                    .collect();
                task = Some(tokio::spawn(async move {
                    let mut notes = Vec::new();
                    for h in handles {
                        let r = h.await??;
                        notes.push(format!(
                            "onboarded {} offboarded {} rows {}",
                            r.onboarded, r.offboarded, r.rows_written
                        ));
                    }
                    Ok(notes.join("; "))
                }));
                Ok(None)
            }
            Action::EndpointOutage { secs } => {
                if let Some(url) = &shared.cfg.disruptions.toxiproxy_url {
                    let proxies: Vec<String> = shared
                        .servers
                        .iter()
                        .map(|(_, s)| {
                            s.proxy.clone().ok_or_else(|| {
                                anyhow::anyhow!(
                                    "server {} has no proxy",
                                    s.name
                                )
                            })
                        })
                        .collect::<Result<_>>()?;
                    crate::actions::toxiproxy(url, &proxies, false).await?;
                    tokio::time::sleep(Duration::from_secs(*secs)).await;
                    crate::actions::toxiproxy(url, &proxies, true).await?;
                } else {
                    for (_, s) in &shared.servers {
                        crate::actions::hook(
                            hooks,
                            "endpoint_down",
                            &[("server", &s.name)],
                        )
                        .await?;
                    }
                    tokio::time::sleep(Duration::from_secs(*secs)).await;
                    for (_, s) in &shared.servers {
                        crate::actions::hook(
                            hooks,
                            "endpoint_up",
                            &[("server", &s.name)],
                        )
                        .await?;
                    }
                }
                Ok(Some(all.clone()))
            }
            Action::SinkOutage { secs } => {
                crate::actions::hook(hooks, "sink_outage_start", &[]).await?;
                tokio::time::sleep(Duration::from_secs(*secs)).await;
                crate::actions::hook(hooks, "sink_outage_stop", &[]).await?;
                Ok(Some(all.clone()))
            }
            Action::StoreRestart => {
                crate::actions::hook(hooks, "store_restart", &[]).await?;
                Ok(Some(all.clone()))
            }
            Action::Failover => {
                let (i, s) = &shared.servers[0];
                crate::actions::hook(
                    hooks,
                    "failover",
                    &[("server", &s.name), ("pipeline", &shared.names[0])],
                )
                .await?;
                Ok(Some(vec![*i]))
            }
            Action::RotateCredentials => {
                for ((_, s), p) in shared.servers.iter().zip(&shared.names) {
                    crate::actions::hook(
                        hooks,
                        "rotate_credentials",
                        &[("server", &s.name), ("pipeline", p)],
                    )
                    .await?;
                }
                Ok(Some(all.clone()))
            }
            Action::LineageBarrier => {
                let (i, s) = &shared.servers[0];
                crate::actions::hook(
                    hooks,
                    "lineage_barrier",
                    &[("server", &s.name), ("pipeline", &shared.names[0])],
                )
                .await?;
                Ok(Some(vec![*i]))
            }
            Action::RestartPipelines => {
                for name in &shared.names {
                    shared.api.stop(name).await?;
                    shared.api.resume(name).await?;
                }
                for name in &shared.names {
                    shared.api.until_running(name, opts.start_timeout).await?;
                }
                Ok(Some(all.clone()))
            }
            Action::ProcessRestart => {
                let pid = measure::resolve_pid(&shared.cfg.deltaforge.process)?;
                crate::actions::hook(
                    hooks,
                    "process_stop",
                    &[("pid", &pid.map(|p| p.to_string()).unwrap_or_default())],
                )
                .await?;
                if let Some(pid) = pid {
                    out.shutdown_secs = Some(
                        crate::actions::wait_exit(
                            pid,
                            Duration::from_secs(600),
                        )
                        .await?
                        .as_secs_f64(),
                    );
                }
                crate::actions::hook(hooks, "process_start", &[]).await?;
                let deadline = Instant::now() + opts.start_timeout;
                for (name, spec) in shared.names.iter().zip(specs) {
                    loop {
                        match shared.api.get(name).await {
                            Ok(_) => break,
                            Err(e) if e.to_string().contains("404") => {
                                shared.api.create(spec).await?;
                                break;
                            }
                            Err(_) if Instant::now() < deadline => {
                                tokio::time::sleep(Duration::from_millis(500))
                                    .await
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    shared.api.until_running(name, opts.start_timeout).await?;
                }
                Ok(Some(all.clone()))
            }
            Action::LifecycleCycles { cycles } => {
                let (api, names, cycles, stop) = (
                    shared.api.clone(),
                    shared.names.clone(),
                    *cycles,
                    shared.stop.clone(),
                );
                let timeout = opts.start_timeout;
                task = Some(tokio::spawn(async move {
                    let mut slowest = 0f64;
                    for c in 0..cycles {
                        if *stop.borrow() {
                            break;
                        }
                        let name = &names[c as usize % names.len()];
                        let t = Instant::now();
                        api.stop(name).await?;
                        api.resume(name).await?;
                        let max_ms = if c % 2 == 0 { 60 } else { 50 };
                        api.patch(
                            name,
                            &json!({"spec": {"batch": {"max_ms": max_ms}}}),
                        )
                        .await?;
                        api.until_running(name, timeout).await?;
                        slowest = slowest.max(t.elapsed().as_secs_f64());
                    }
                    Ok(format!("slowest stop/resume/patch cycle {slowest:.2}s"))
                }));
                Ok(None)
            }
            Action::ScrapeLoad { interval_secs } => {
                let (url, agg, mut stop, every) = (
                    shared.cfg.deltaforge.metrics_url.clone(),
                    shared.agg.clone(),
                    shared.stop.clone(),
                    Duration::from_secs(*interval_secs),
                );
                task = Some(tokio::spawn(async move {
                    let http = reqwest::Client::new();
                    loop {
                        let t = Instant::now();
                        if let Ok(r) = http.get(&url).send().await
                            && let Ok(body) = r.bytes().await
                        {
                            agg.lock().scrape(
                                t.elapsed().as_secs_f64() * 1e3,
                                body.len() as u64,
                            );
                        }
                        tokio::select! {
                            _ = tokio::time::sleep(every) => {}
                            _ = stop.changed() => break,
                        }
                    }
                    Ok(String::new())
                }));
                Ok(None)
            }
        }
    }
    .await;
    out.duration_secs = Some(started.elapsed().as_secs_f64());
    match result {
        Ok(Some(affected)) => {
            shared.marks.mark(&affected, now_micros());
            out.recovery =
                Some(recovery(shared, &affected, opts.recovery_timeout).await);
        }
        Ok(None) => {}
        Err(e) => out.error = Some(format!("{e:#}")),
    }
    (out, task)
}

/// Sample the instance every `measure.interval_secs` until stopped.
async fn sampler(
    cfg: Arc<RunConfig>,
    servers: Vec<(u16, Server)>,
    agg: Arc<Mutex<Aggregate>>,
    path: PathBuf,
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
    let mut out = std::io::BufWriter::new(std::fs::File::create(&path)?);
    let http = reqwest::Client::new();
    let mut conns: HashMap<String, mysql_async::Conn> = HashMap::new();
    let store = match &cfg.measure.state_store_dsn {
        Some(dsn) => {
            let (client, conn) =
                tokio_postgres::connect(dsn, tokio_postgres::NoTls).await?;
            tokio::spawn(conn);
            Some(client)
        }
        None => None,
    };
    let every = Duration::from_secs(cfg.measure.interval_secs.max(1));
    loop {
        let now = Instant::now();
        let pid = measure::resolve_pid(&cfg.deltaforge.process).ok().flatten();
        let process = pid.map(|pid| {
            let cg = measure::cgroup_dir(
                Path::new("/proc"),
                Path::new("/sys/fs/cgroup"),
                pid,
            )
            .ok();
            measure::read_process(Path::new("/proc"), cg.as_deref(), pid)
        });
        let mut connections = HashMap::new();
        for (_, s) in &servers {
            if !conns.contains_key(&s.name)
                && let Ok(pool) = fixture::admin_pool(&cfg, s, 1)
                && let Ok(c) = pool.get_conn().await
            {
                conns.insert(s.name.clone(), c);
            }
            if let Some(c) = conns.get_mut(&s.name) {
                match measure::mysql_connections(c, &cfg.measure.cdc_users)
                    .await
                {
                    Ok(n) => {
                        connections.insert(s.name.clone(), n);
                    }
                    Err(_) => {
                        conns.remove(&s.name);
                    }
                }
            }
        }
        let store_size = match &store {
            Some(c) => measure::store_size(c).await.ok(),
            None => None,
        };
        let scrape_started = Instant::now();
        let metrics = match http.get(&cfg.deltaforge.metrics_url).send().await {
            Ok(r) => r.text().await.ok(),
            Err(_) => None,
        };
        let scrape_ms = scrape_started.elapsed().as_secs_f64() * 1e3;
        let samples = metrics
            .as_deref()
            .map(measure::parse_prometheus)
            .unwrap_or_default();
        {
            let mut a = agg.lock();
            if let Some(p) = &process {
                a.process(now, p);
            }
            a.connections(&connections);
            if let Some(s) = &store_size {
                a.store(s.database_bytes);
            }
            if let Some(m) = &metrics {
                a.scrape(scrape_ms, m.len() as u64);
            }
        }
        let line = json!({
            "at": Utc::now().to_rfc3339(),
            "process": process,
            "connections": connections,
            "store": store_size,
            "lag_seconds": measure::sum_by(&samples, "deltaforge_source_lag_seconds", "pipeline"),
            "sink_events": measure::sum_by(&samples, "deltaforge_sink_events_total", "pipeline"),
            "scrape_ms": scrape_ms,
        });
        writeln!(out, "{line}")?;
        out.flush()?;
        tokio::select! {
            _ = tokio::time::sleep(every) => {}
            _ = stop.changed() => break,
        }
    }
    Ok(())
}
