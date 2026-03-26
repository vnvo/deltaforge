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

/// Number of rows to insert before starting DeltaForge.
const TARGET_EVENTS: u64 = 1_000_000;

/// Concurrent writer tasks during the population phase.
/// No artificial delay — we want maximum write throughput.
const WRITER_TASKS: usize = 32;

/// Maximum time to wait for the drain to complete after resuming.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(600); // 10 min

/// How often to sample the Kafka offset during drain.
const POLL_INTERVAL: Duration = Duration::from_millis(500);

// ── Drain config ──────────────────────────────────────────────────────────────

/// Per-run throughput settings applied via PATCH before the drain starts.
#[derive(Debug, Clone)]
pub struct DrainConfig {
    /// Max events per batch (pipeline spec `batch.max_events`).
    pub max_events: u64,
    /// Max batch age in ms (pipeline spec `batch.max_ms`).
    pub max_ms: u64,
    /// Commit mode: "required" (safe) or "periodic" (faster, may lose events on crash).
    pub commit_mode: String,
    /// Checkpoint interval in ms — only used when `commit_mode` is "periodic".
    pub commit_interval_ms: u64,
    /// Whether schema sensing is enabled during the drain.
    /// Disabling it improves throughput significantly.
    pub schema_sensing: bool,
    /// rdkafka producer overrides applied to the sink's `client_conf`.
    /// Common keys: `linger.ms`, `batch.size`, `batch.num.messages`,
    /// `compression.type`, `queue.buffering.max.messages`.
    pub kafka_client_conf: std::collections::HashMap<String, String>,
}

impl Default for DrainConfig {
    fn default() -> Self {
        Self {
            max_events: 200,
            max_ms: 100,
            commit_mode: "required".to_string(),
            commit_interval_ms: 500,
            schema_sensing: false,
            kafka_client_conf: std::collections::HashMap::new(),
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

    info!(
        source = src.name,
        max_events = cfg.max_events,
        max_ms = cfg.max_ms,
        commit_mode = %cfg.commit_mode,
        commit_interval_ms = cfg.commit_interval_ms,
        schema_sensing = cfg.schema_sensing,
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
    info!(
        TARGET_EVENTS,
        WRITER_TASKS,
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
            if current >= TARGET_EVENTS {
                break;
            }
            let delta = current.saturating_sub(last);
            let tps = delta as f64 / last_t.elapsed().as_secs_f64();
            info!(
                written = current,
                remaining = TARGET_EVENTS.saturating_sub(current),
                tps = format!("{tps:.0}"),
                "write progress"
            );
            last = current;
            last_t = Instant::now();
        }
    });

    let handles: Vec<_> = (0..WRITER_TASKS)
        .map(|id| {
            let ctr = Arc::clone(&counter);
            if is_pg {
                tokio::spawn(async move { pg_writer(id, ctr).await })
            } else {
                tokio::spawn(async move { mysql_writer(id, ctr).await })
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
        commit_mode = %cfg.commit_mode,
        schema_sensing = cfg.schema_sensing,
        "step 4/6: patching pipeline '{pipeline}' and restarting ..."
    );
    let mut patch_spec = serde_json::json!({
        "schema_sensing": {"enabled": cfg.schema_sensing},
        "batch": {"max_events": cfg.max_events, "max_ms": cfg.max_ms},
        "commit_policy": {"mode": cfg.commit_mode},
    });

    // If any rdkafka client_conf overrides were provided, patch the first sink.
    if !cfg.kafka_client_conf.is_empty() {
        let cc: serde_json::Value = cfg
            .kafka_client_conf
            .iter()
            .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
            .collect::<serde_json::Map<String, serde_json::Value>>()
            .into();
        info!(?cc, "applying kafka client_conf overrides");
        patch_spec["sinks"] = serde_json::json!([
            {"config": {"client_conf": cc}}
        ]);
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

    let drain_start = Instant::now();
    info!("pipeline restarted — backlog drain started");

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

        if elapsed >= DRAIN_TIMEOUT {
            let shortfall = rows_written.saturating_sub(delivered);
            return Ok(ScenarioResult::fail(
                &name,
                format!("drain timed out after {DRAIN_TIMEOUT:?}: {shortfall} events not yet delivered"),
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
        .note(format!("target events: {TARGET_EVENTS}"))
        .note(format!("rows written: {rows_written}"))
        .note(format!("batch max_events: {}", cfg.max_events))
        .note(format!("commit mode: {}", cfg.commit_mode))
        .note(format!("schema sensing: {}", cfg.schema_sensing));

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

async fn mysql_writer(id: usize, counter: Arc<AtomicU64>) {
    use mysql_async::prelude::Queryable;

    let tables = soak_tables();
    let pool = mysql_async::Pool::new(crate::backend::MYSQL_DSN);
    let mut rng =
        <rand::rngs::SmallRng as rand::SeedableRng>::seed_from_u64(id as u64);

    loop {
        let prev = counter.fetch_add(1, Ordering::Relaxed);
        if prev >= TARGET_EVENTS {
            counter.fetch_sub(1, Ordering::Relaxed);
            break;
        }

        let table = &tables[rand::Rng::gen_range(&mut rng, 0..tables.len())];
        let ts = now_ms();

        match pool.get_conn().await {
            Ok(mut conn) => {
                let _ = conn
                    .exec_drop(
                        format!(
                            "INSERT INTO {table} (tag, data, value, status) \
                             VALUES (?, ?, ?, ?)"
                        ),
                        (
                            format!("drain-{id}-{ts}"),
                            format!("backlog-{ts}"),
                            rand::Rng::gen_range(&mut rng, 0.0..10000.0_f64),
                            rand::Rng::gen_range(&mut rng, 0u8..4),
                        ),
                    )
                    .await;
            }
            Err(e) => {
                tracing::warn!(writer = id, error = %e, "mysql writer connection error, retrying");
                counter.fetch_sub(1, Ordering::Relaxed);
                sleep(Duration::from_millis(50)).await;
            }
        }
    }

    let _ = pool.disconnect().await;
}

// ── Postgres writer ───────────────────────────────────────────────────────────

async fn pg_writer(id: usize, counter: Arc<AtomicU64>) {
    let tables = soak_tables();
    let mut rng =
        <rand::rngs::SmallRng as rand::SeedableRng>::seed_from_u64(id as u64);

    // Outer loop: reconnect on connection failure.
    loop {
        // Check if target reached before connecting.
        if counter.load(Ordering::Relaxed) >= TARGET_EVENTS {
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
            if prev >= TARGET_EVENTS {
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
