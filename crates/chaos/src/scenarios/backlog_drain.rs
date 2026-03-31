//! Scenario: backlog drain benchmark.
//!
//! Measures how quickly DeltaForge can replay a pre-built backlog —
//! the catch-up path that runs on cold starts, long outages, or migrations.
//!
//! ## Method
//!
//! 1. Stop the pipeline via the REST API so DeltaForge disconnects and saves
//!    its checkpoint (binlog position for MySQL, LSN for Postgres).
//! 2. Record the current Kafka high-watermark offset as baseline.
//! 3. Hammer the source DB with `TARGET_EVENTS` pure inserts using
//!    `WRITER_TASKS` concurrent writers (no artificial delay).
//! 4. Record write throughput and duration.
//! 5. Resume the pipeline — it reconnects and starts replaying from the
//!    saved checkpoint, processing the entire backlog.
//! 6. Poll the Kafka offset every 500 ms until the delta reaches
//!    `TARGET_EVENTS`, then record drain throughput.
//!
//! ## Usage
//!
//! ```bash
//! # MySQL — requires the soak compose profile
//! docker compose -f docker-compose.chaos.yml --profile soak up -d
//! cargo run -p chaos -- --scenario backlog-drain --source mysql
//!
//! # Postgres — requires the pg-soak compose profile
//! docker compose -f docker-compose.chaos.yml --profile pg-soak up -d
//! cargo run -p chaos -- --scenario backlog-drain --source postgres
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use tokio::time::sleep;
use tracing::info;

use crate::harness::{self, Harness, ScenarioResult};
use crate::scenarios::soak::SoakSource;

// ── Constants ─────────────────────────────────────────────────────────────────

/// How often to sample the Kafka offset during drain.
const POLL_INTERVAL: Duration = Duration::from_millis(500);

// ── Drain config ──────────────────────────────────────────────────────────────

/// Per-run throughput settings applied via PATCH before the drain starts.
#[derive(Debug, Clone)]
pub struct DrainConfig {
    /// Number of rows to insert before starting DeltaForge.
    pub target_events: u64,
    /// Concurrent writer tasks during the population phase.
    pub writer_tasks: usize,
    /// Maximum time (seconds) to wait for the drain to complete after resuming.
    /// 0 = auto-scale based on target_events (10s per 1M events, minimum 120s).
    pub drain_timeout_secs: u64,
    /// Max events per batch (pipeline spec `batch.max_events`).
    pub max_events: u64,
    /// Max batch age in ms (pipeline spec `batch.max_ms`).
    pub max_ms: u64,
    /// How many batches may be in-flight concurrently.
    /// Higher values overlap accumulation with delivery for better throughput.
    pub max_inflight: u64,
    /// Commit mode: "required" (safe) or "periodic" (faster, may lose events on crash).
    pub commit_mode: String,
    /// Checkpoint interval in ms — only used when `commit_mode` is "periodic".
    pub commit_interval_ms: u64,
    /// Whether schema sensing is enabled during the drain.
    /// Disabling it improves throughput significantly.
    pub schema_sensing: bool,
    /// Max batch size in bytes. 0 = leave unchanged (default 3MB).
    pub max_bytes: u64,
    /// rdkafka producer overrides applied to the sink's `client_conf`.
    /// Common keys: `linger.ms`, `batch.size`, `batch.num.messages`,
    /// `compression.type`, `queue.buffering.max.messages`.
    pub kafka_client_conf: std::collections::HashMap<String, String>,
    /// Enable or disable exactly-once (transactional) delivery on the Kafka sink.
    /// `None` leaves the pipeline's current setting unchanged.
    pub exactly_once: Option<bool>,
}

impl DrainConfig {
    /// Effective drain timeout — auto-scales if `drain_timeout_secs` is 0.
    pub fn drain_timeout(&self) -> Duration {
        if self.drain_timeout_secs > 0 {
            Duration::from_secs(self.drain_timeout_secs)
        } else {
            // 30 seconds per 1M events, minimum 120s.
            // Conservative to accommodate exactly-once transactional overhead.
            let secs = ((self.target_events as f64 / 1_000_000.0) * 30.0).ceil()
                as u64;
            Duration::from_secs(secs.max(120))
        }
    }
}

impl Default for DrainConfig {
    fn default() -> Self {
        let mut kafka = std::collections::HashMap::new();
        // Zero linger for drain benchmarks: the coordinator enqueues thousands
        // of messages per batch in a tight loop, so rdkafka batches naturally.
        // Any linger > 0 adds idle wait per produce and directly caps throughput.
        kafka.insert("linger.ms".into(), "0".into());

        Self {
            target_events: 1_000_000,
            writer_tasks: 32,
            drain_timeout_secs: 0, // auto-scale
            max_events: 4000,
            max_ms: 100,
            max_inflight: 4,
            commit_mode: "required".to_string(),
            commit_interval_ms: 500,
            schema_sensing: false,
            max_bytes: 0,
            kafka_client_conf: kafka,
            exactly_once: None,
        }
    }
}

// ── Entry points ──────────────────────────────────────────────────────────────

pub async fn run(
    harness: &Harness,
    cfg: DrainConfig,
) -> Result<ScenarioResult> {
    run_with_source(harness, &super::soak::MYSQL_SOAK, cfg).await
}

pub async fn run_pg(
    harness: &Harness,
    cfg: DrainConfig,
) -> Result<ScenarioResult> {
    run_with_source(harness, &super::soak::PG_SOAK, cfg).await
}

async fn run_with_source(
    harness: &Harness,
    src: &SoakSource,
    cfg: DrainConfig,
) -> Result<ScenarioResult> {
    let name = format!("backlog-drain-{}", src.name);
    let is_pg = src.name == "postgres";

    crate::harness::print_scenario_banner(
        &name,
        &format!(
            "Writes {} rows to {}, then measures catch-up throughput from saved checkpoint.",
            cfg.target_events, src.name
        ),
        "All events delivered to Kafka. Reports avg/p50/peak events/s.",
    );
    info!(
        source = src.name,
        max_events = cfg.max_events,
        max_ms = cfg.max_ms,
        max_inflight = cfg.max_inflight,
        commit_mode = %cfg.commit_mode,
        commit_interval_ms = cfg.commit_interval_ms,
        schema_sensing = cfg.schema_sensing,
        exactly_once = ?cfg.exactly_once,
        kafka_overrides = cfg.kafka_client_conf.len(),
        "drain config"
    );
    harness.setup().await?;

    // Step 1: verify DeltaForge is healthy.
    info!("step 1/6: waiting for DeltaForge at {} ...", src.health_url);
    harness::wait_for_url(src.health_url, Duration::from_secs(60)).await?;
    info!("DeltaForge is healthy");

    // Step 2: stop the pipeline so DeltaForge disconnects and saves its checkpoint.
    let pipeline = src.pipeline;
    info!("step 2/6: stopping pipeline '{pipeline}' ...");
    df_post(src.df_base, &format!("/pipelines/{pipeline}/stop")).await?;
    info!("pipeline stopped — checkpoint saved");

    // Step 3: baseline Kafka offset.
    let kafka_baseline = harness::kafka_offset_for_topic(src.topic)
        .await
        .unwrap_or(0);
    info!(kafka_baseline, "kafka baseline offset recorded");

    // Step 4: populate the backlog.
    let target_events = cfg.target_events;
    let writer_tasks = cfg.writer_tasks;
    let drain_timeout = cfg.drain_timeout();

    info!(
        target_events,
        writer_tasks,
        source = src.name,
        "step 3/6: populating {} backlog ...",
        src.name,
    );
    let counter = Arc::new(AtomicU64::new(0));
    let write_start = Instant::now();

    // Progress monitor — logs every 10 s.
    let progress_ctr = Arc::clone(&counter);
    let progress_handle = tokio::spawn(async move {
        let mut last = 0u64;
        let mut last_t = Instant::now();
        loop {
            sleep(Duration::from_secs(10)).await;
            let current = progress_ctr.load(Ordering::Relaxed);
            if current >= target_events {
                break;
            }
            let delta = current.saturating_sub(last);
            let tps = delta as f64 / last_t.elapsed().as_secs_f64();
            info!(
                written = current,
                remaining = target_events.saturating_sub(current),
                tps = format!("{tps:.0}"),
                "write progress"
            );
            last = current;
            last_t = Instant::now();
        }
    });

    let handles: Vec<_> = (0..writer_tasks)
        .map(|id| {
            let ctr = Arc::clone(&counter);
            if is_pg {
                tokio::spawn(
                    async move { pg_writer(id, ctr, target_events).await },
                )
            } else {
                tokio::spawn(async move {
                    mysql_writer(id, ctr, target_events).await
                })
            }
        })
        .collect();

    for h in handles {
        let _ = h.await;
    }
    progress_handle.abort();

    let rows_written = counter.load(Ordering::Relaxed);
    let write_secs = write_start.elapsed().as_secs_f64();
    let write_tps = rows_written as f64 / write_secs;
    info!(
        rows_written,
        write_secs = format!("{write_secs:.1}"),
        write_tps = format!("{write_tps:.0}"),
        "backlog population complete"
    );

    // Step 5: patch throughput settings and restart from saved checkpoint.
    info!(
        max_events = cfg.max_events,
        max_ms = cfg.max_ms,
        max_inflight = cfg.max_inflight,
        commit_mode = %cfg.commit_mode,
        schema_sensing = cfg.schema_sensing,
        "step 4/6: patching pipeline '{pipeline}' and restarting ..."
    );
    let mut batch_json = serde_json::json!({
        "max_events": cfg.max_events,
        "max_ms": cfg.max_ms,
        "max_inflight": cfg.max_inflight,
    });
    if cfg.max_bytes > 0 {
        batch_json["max_bytes"] = serde_json::json!(cfg.max_bytes);
    }
    let mut patch_spec = serde_json::json!({
        "schema_sensing": {"enabled": cfg.schema_sensing},
        "batch": batch_json,
        "commit_policy": {"mode": cfg.commit_mode},
    });

    // Patch the first sink if client_conf or exactly_once is specified.
    if !cfg.kafka_client_conf.is_empty() || cfg.exactly_once.is_some() {
        let mut sink_patch = serde_json::json!({"config": {}});

        if !cfg.kafka_client_conf.is_empty() {
            let cc: serde_json::Value = cfg
                .kafka_client_conf
                .iter()
                .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
                .collect::<serde_json::Map<String, serde_json::Value>>()
                .into();
            info!(?cc, "applying kafka client_conf overrides");
            sink_patch["config"]["client_conf"] = cc;
        }

        if let Some(eo) = cfg.exactly_once {
            info!(exactly_once = eo, "applying exactly_once override");
            sink_patch["config"]["exactly_once"] = serde_json::json!(eo);
        }

        patch_spec["sinks"] = serde_json::json!([sink_patch]);
    }

    let patch_body = serde_json::json!({ "spec": patch_spec });
    let patch_ok = df_patch(src.df_base, &format!("/pipelines/{pipeline}"), patch_body)
        .await
        .map(|_| true)
        .unwrap_or_else(|e| {
            tracing::warn!(error = %e, "could not patch pipeline — will resume with current config");
            false
        });

    // If PATCH succeeded, the pipeline restarts automatically from the saved
    // checkpoint. If it failed, issue an explicit resume so the drain still runs.
    if !patch_ok {
        info!("issuing explicit resume after failed patch ...");
        df_post(src.df_base, &format!("/pipelines/{pipeline}/resume")).await?;
    }

    // Dump the effective pipeline config so there's no ambiguity about what's running.
    match df_get(src.df_base, &format!("/pipelines/{pipeline}")).await {
        Ok(body) => {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&body) {
                if let Some(spec) = v.get("spec") {
                    let flat = flatten_json("", spec);
                    info!("── effective pipeline config ──");
                    for (k, v) in &flat {
                        info!("  {k} = {v}");
                    }
                }
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "could not fetch pipeline config for dump")
        }
    }

    let drain_start = Instant::now();
    let conn_mode =
        harness::connection_mode_summary(src.df_base, pipeline).await;
    info!(connection = %conn_mode, "pipeline restarted — backlog drain started");

    // Step 6: poll Kafka until the offset advances by rows_written.
    info!(
        target = rows_written,
        "step 5/6: waiting for Kafka to reflect all events ..."
    );
    let mut last_offset = kafka_baseline;
    let mut last_sample_time = Instant::now();
    let mut peak_drain_tps: f64 = 0.0;
    let mut throughput_samples: Vec<f64> = Vec::new();

    loop {
        sleep(POLL_INTERVAL).await;

        let current_offset = harness::kafka_offset_for_topic(src.topic)
            .await
            .unwrap_or(kafka_baseline);
        let delivered = current_offset.saturating_sub(kafka_baseline);
        let elapsed = drain_start.elapsed();

        // Sample throughput every 5 s.
        let sample_secs = last_sample_time.elapsed().as_secs_f64();
        if sample_secs >= 5.0 {
            let delta = current_offset.saturating_sub(last_offset);
            let tps = delta as f64 / sample_secs;
            throughput_samples.push(tps);
            if tps > peak_drain_tps {
                peak_drain_tps = tps;
            }
            info!(
                delivered,
                remaining = rows_written.saturating_sub(delivered),
                drain_tps = format!("{tps:.0}"),
                elapsed_secs = format!("{:.1}", elapsed.as_secs_f64()),
                "drain progress"
            );
            last_offset = current_offset;
            last_sample_time = Instant::now();
        }

        if delivered >= rows_written {
            info!(delivered, "all events delivered — drain complete");
            break;
        }

        if elapsed >= drain_timeout {
            let shortfall = rows_written.saturating_sub(delivered);
            return Ok(ScenarioResult::fail(
                &name,
                format!("drain timed out after {drain_timeout:?}: {shortfall} events not yet delivered"),
            )
            .note(format!("source: {}", src.name))
            .note(format!("rows written: {rows_written}"))
            .note(format!("events delivered to Kafka: {delivered}"))
            .note(format!("write throughput: {write_tps:.0} rows/s"))
            .note(format!("write duration: {write_secs:.1}s")));
        }
    }

    let drain_secs = drain_start.elapsed().as_secs_f64();
    let avg_drain_tps = rows_written as f64 / drain_secs;

    // Compute p50 drain throughput from samples.
    let p50_drain_tps = if throughput_samples.is_empty() {
        avg_drain_tps
    } else {
        let mut sorted = throughput_samples.clone();
        sorted.sort_by(f64::total_cmp);
        sorted[sorted.len() / 2]
    };

    info!(
        rows_written,
        drain_secs = format!("{drain_secs:.1}"),
        avg_drain_tps = format!("{avg_drain_tps:.0}"),
        peak_drain_tps = format!("{peak_drain_tps:.0}"),
        "step 6/6: backlog drain benchmark complete"
    );

    let mut result = ScenarioResult::pass(&name)
        .note(format!("source: {}", src.name))
        .note(format!("connection: {conn_mode}"))
        .note(format!("target events: {target_events}"))
        .note(format!("rows written: {rows_written}"))
        .note(format!("batch max_events: {}", cfg.max_events))
        .note(format!("batch max_inflight: {}", cfg.max_inflight))
        .note(format!("commit mode: {}", cfg.commit_mode))
        .note(format!("schema sensing: {}", cfg.schema_sensing))
        .note(format!(
            "exactly_once: {}",
            cfg.exactly_once
                .map(|v| v.to_string())
                .unwrap_or_else(|| "unchanged".into())
        ));

    if !cfg.kafka_client_conf.is_empty() {
        let pairs: Vec<String> = cfg
            .kafka_client_conf
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect();
        result = result.note(format!("kafka overrides: {}", pairs.join(", ")));
    }

    Ok(result
        .note(format!(
            "write duration: {write_secs:.1}s ({write_tps:.0} rows/s)"
        ))
        .note(format!("drain duration: {drain_secs:.1}s"))
        .note(format!("drain avg throughput: {avg_drain_tps:.0} events/s"))
        .note(format!("drain p50 throughput: {p50_drain_tps:.0} events/s"))
        .note(format!(
            "drain peak throughput: {peak_drain_tps:.0} events/s"
        )))
}

// ── MySQL writer ──────────────────────────────────────────────────────────────

/// Rows per multi-row INSERT statement in the MySQL writer.
const MYSQL_BATCH_SIZE: usize = 64;

async fn mysql_writer(id: usize, counter: Arc<AtomicU64>, target: u64) {
    use mysql_async::prelude::Queryable;

    let tables = soak_tables();
    let pool = mysql_async::Pool::new(crate::backend::MYSQL_DSN);
    let mut rng =
        <rand::rngs::SmallRng as rand::SeedableRng>::seed_from_u64(id as u64);

    // Outer loop: acquire a connection and reuse it for many inserts.
    loop {
        if counter.load(Ordering::Relaxed) >= target {
            return;
        }

        let mut conn = match pool.get_conn().await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(writer = id, error = %e, "mysql writer connect failed, retrying");
                sleep(Duration::from_millis(200)).await;
                continue;
            }
        };

        // Inner loop: batch inserts on a live connection.
        loop {
            // Reserve a batch of rows.
            let batch_start =
                counter.fetch_add(MYSQL_BATCH_SIZE as u64, Ordering::Relaxed);
            if batch_start >= target {
                counter.fetch_sub(MYSQL_BATCH_SIZE as u64, Ordering::Relaxed);
                return; // pool + conn dropped automatically
            }
            let actual_batch =
                ((target - batch_start) as usize).min(MYSQL_BATCH_SIZE);
            if actual_batch < MYSQL_BATCH_SIZE {
                counter.fetch_sub(
                    (MYSQL_BATCH_SIZE - actual_batch) as u64,
                    Ordering::Relaxed,
                );
            }

            let table =
                &tables[rand::Rng::gen_range(&mut rng, 0..tables.len())];
            let ts = now_ms();

            // Build a multi-row INSERT: INSERT INTO t (cols) VALUES (...), (...), ...
            let mut sql = format!(
                "INSERT INTO {table} (tag, data, value, status) VALUES "
            );
            let mut params: Vec<mysql_async::Value> =
                Vec::with_capacity(actual_batch * 4);
            for i in 0..actual_batch {
                if i > 0 {
                    sql.push_str(", ");
                }
                sql.push_str("(?, ?, ?, ?)");
                params.push(format!("drain-{id}-{ts}-{i}").into());
                params.push(format!("backlog-{ts}").into());
                params.push(
                    rand::Rng::gen_range(&mut rng, 0.0..10000.0_f64).into(),
                );
                params.push(rand::Rng::gen_range(&mut rng, 0u8..4).into());
            }

            match conn
                .exec_drop(&sql, mysql_async::Params::Positional(params))
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(writer = id, error = %e, "mysql writer query failed, reconnecting");
                    counter.fetch_sub(actual_batch as u64, Ordering::Relaxed);
                    break; // Break to outer loop → reconnect.
                }
            }
        }

        sleep(Duration::from_millis(100)).await;
    }
}

// ── Postgres writer ───────────────────────────────────────────────────────────

async fn pg_writer(id: usize, counter: Arc<AtomicU64>, target: u64) {
    let tables = soak_tables();
    let mut rng =
        <rand::rngs::SmallRng as rand::SeedableRng>::seed_from_u64(id as u64);

    // Outer loop: reconnect on connection failure.
    loop {
        // Check if target reached before connecting.
        if counter.load(Ordering::Relaxed) >= target {
            break;
        }

        let (client, conn) = match tokio_postgres::connect(
            crate::backend::PG_DSN,
            tokio_postgres::NoTls,
        )
        .await
        {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(writer = id, error = %e, "pg writer connect failed, retrying");
                sleep(Duration::from_millis(200)).await;
                continue;
            }
        };
        let conn_handle = tokio::spawn(async move {
            let _ = conn.await;
        });

        // Inner loop: execute queries on active connection.
        loop {
            let prev = counter.fetch_add(1, Ordering::Relaxed);
            if prev >= target {
                counter.fetch_sub(1, Ordering::Relaxed);
                conn_handle.abort();
                return;
            }

            let table =
                &tables[rand::Rng::gen_range(&mut rng, 0..tables.len())];
            let ts = now_ms();
            let tag = format!("drain-{id}-{ts}");
            let data = format!("backlog-{ts}");
            let value = format!(
                "{:.4}",
                rand::Rng::gen_range(&mut rng, 0.0..10000.0_f64)
            );
            let status: i16 = rand::Rng::gen_range(&mut rng, 0i16..4);

            match client
                .execute(
                    &format!(
                        "INSERT INTO {table} (tag, data, value, status) \
                         VALUES ($1, $2, {value}, $3)"
                    ),
                    &[&tag, &data, &status],
                )
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(writer = id, error = %e, "pg writer query failed, reconnecting");
                    counter.fetch_sub(1, Ordering::Relaxed);
                    break; // Break to outer loop → reconnect.
                }
            }
        }

        conn_handle.abort();
        sleep(Duration::from_millis(100)).await;
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn soak_tables() -> Vec<String> {
    const DOMAINS: &[&str] = &[
        "customer",
        "order",
        "product",
        "inventory",
        "payment",
        "event",
    ];
    let mut tables = Vec::with_capacity(DOMAINS.len() * 20);
    for domain in DOMAINS {
        for i in 1..=20usize {
            tables.push(format!("soak_{domain}_{i:02}"));
        }
    }
    tables
}

async fn df_patch(
    base: &str,
    path: &str,
    body: serde_json::Value,
) -> Result<()> {
    let url = format!("{base}{path}");
    info!(%url, "df_patch");
    let resp = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()?
        .patch(&url)
        .json(&body)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("PATCH {url} connection error: {e}"))?;
    let status = resp.status();
    if status.is_success() {
        info!(%url, %status, "df_patch ok");
        Ok(())
    } else {
        let body = resp.text().await.unwrap_or_default();
        bail!("PATCH {url} returned {status}: {body}")
    }
}

async fn df_get(base: &str, path: &str) -> Result<String> {
    let url = format!("{base}{path}");
    let resp = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?
        .get(&url)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("GET {url} connection error: {e}"))?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if status.is_success() {
        Ok(body)
    } else {
        bail!("GET {url} returned {status}: {body}")
    }
}

/// Flatten a JSON value into dot-separated key=value pairs for readable logging.
fn flatten_json(
    prefix: &str,
    value: &serde_json::Value,
) -> Vec<(String, String)> {
    let mut out = Vec::new();
    match value {
        serde_json::Value::Object(map) => {
            for (k, v) in map {
                let key = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{prefix}.{k}")
                };
                out.extend(flatten_json(&key, v));
            }
        }
        serde_json::Value::Array(arr) => {
            for (i, v) in arr.iter().enumerate() {
                out.extend(flatten_json(&format!("{prefix}[{i}]"), v));
            }
        }
        _ => {
            out.push((prefix.to_string(), value.to_string()));
        }
    }
    out
}

async fn df_post(base: &str, path: &str) -> Result<()> {
    let url = format!("{base}{path}");
    info!(%url, "df_post");
    let resp = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()?
        .post(&url)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("POST {url} connection error: {e}"))?;
    let status = resp.status();
    if status.is_success() {
        info!(%url, %status, "df_post ok");
        Ok(())
    } else {
        let body = resp.text().await.unwrap_or_default();
        bail!("POST {url} returned {status}: {body}")
    }
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
}
