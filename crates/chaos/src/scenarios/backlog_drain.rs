//! Scenario: binlog backlog drain benchmark.
//!
//! Measures how quickly DeltaForge can replay a pre-built binlog backlog —
//! the catch-up path that runs on cold starts, long outages, or migrations.
//!
//! ## Method
//!
//! 1. Stop the `chaos-soak` pipeline via the REST API so DeltaForge
//!    disconnects from MySQL and saves its binlog checkpoint.
//! 2. Record the current Kafka high-watermark offset as baseline.
//! 3. Hammer MySQL with `TARGET_EVENTS` pure inserts using `WRITER_TASKS`
//!    concurrent writers (no artificial delay).
//! 4. Record write throughput and duration.
//! 5. Resume the pipeline — it reconnects and starts replaying from the
//!    saved checkpoint, processing the entire backlog.
//! 6. Poll the Kafka offset every 500 ms until the delta reaches
//!    `TARGET_EVENTS`, then record drain throughput.
//!
//! ## Usage
//!
//! ```bash
//! # Requires the soak compose profile
//! docker compose -f docker-compose.chaos.yml --profile soak up -d
//!
//! cargo run -p chaos -- --scenario backlog-drain --source mysql
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use mysql_async::prelude::Queryable;
use tokio::time::sleep;
use tracing::info;

use crate::backend::MYSQL_DSN;
use crate::harness::{self, Harness, ScenarioResult};
use crate::scenarios::soak::{SOAK_HEALTH_URL, SOAK_TOPIC};

// ── Constants ─────────────────────────────────────────────────────────────────

/// Number of rows to insert into MySQL before starting DeltaForge.
const TARGET_EVENTS: u64 = 1_000_000;

/// Concurrent writer tasks during the population phase.
/// No artificial delay — we want maximum write throughput.
const WRITER_TASKS: usize = 32;

/// Maximum time to wait for the drain to complete after resuming.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(600); // 10 min

/// How often to sample the Kafka offset during drain.
const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// DeltaForge soak instance base URL.
const DF_BASE: &str = "http://localhost:8081";

/// Name of the pipeline managed by the soak DeltaForge instance.
const PIPELINE_NAME: &str = "chaos-soak";

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
}

impl Default for DrainConfig {
    fn default() -> Self {
        Self {
            max_events: 200,
            max_ms: 100,
            commit_mode: "required".to_string(),
            commit_interval_ms: 500,
            schema_sensing: false,
        }
    }
}

// ── Entry point ───────────────────────────────────────────────────────────────

pub async fn run(harness: &Harness, cfg: DrainConfig) -> Result<ScenarioResult> {
    let name = "backlog-drain";
    info!(
        max_events = cfg.max_events,
        max_ms = cfg.max_ms,
        commit_mode = %cfg.commit_mode,
        commit_interval_ms = cfg.commit_interval_ms,
        schema_sensing = cfg.schema_sensing,
        "drain config"
    );
    harness.setup().await?;

    // Step 1: verify soak DeltaForge is healthy.
    info!("step 1/6: waiting for soak DeltaForge at {SOAK_HEALTH_URL} ...");
    harness::wait_for_url(SOAK_HEALTH_URL, Duration::from_secs(60)).await?;
    info!("soak DeltaForge is healthy");

    // Step 2: stop the pipeline so DeltaForge disconnects and saves its
    // binlog checkpoint. All writes from this point forward will queue up
    // in MySQL's binlog.
    info!("step 2/6: stopping pipeline '{PIPELINE_NAME}' ...");
    df_post(&format!("/pipelines/{PIPELINE_NAME}/stop")).await?;
    info!("pipeline stopped — binlog checkpoint saved");

    // Step 3: baseline Kafka offset. We snapshot here (after the stop) so
    // any in-flight events that flushed just before the stop are excluded.
    let kafka_baseline = harness::kafka_offset_for_topic(SOAK_TOPIC)
        .await
        .unwrap_or(0);
    info!(kafka_baseline, "kafka baseline offset recorded");

    // Step 4: populate the backlog — write TARGET_EVENTS rows as fast as
    // possible. Writers stop themselves once the shared counter hits the target.
    info!(TARGET_EVENTS, WRITER_TASKS, "step 3/6: populating MySQL backlog ...");
    let counter = Arc::new(AtomicU64::new(0));
    let write_start = Instant::now();

    // Progress monitor — logs every 10 s so the UI terminal shows activity.
    let progress_ctr = Arc::clone(&counter);
    let progress_handle = tokio::spawn(async move {
        let mut last = 0u64;
        let mut last_t = Instant::now();
        loop {
            sleep(Duration::from_secs(10)).await;
            let current = progress_ctr.load(Ordering::Relaxed);
            if current >= TARGET_EVENTS { break; }
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
            tokio::spawn(async move { writer(id, ctr).await })
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
        "step 4/6: patching pipeline '{PIPELINE_NAME}' and restarting ..."
    );
    // Build the PATCH body from DrainConfig.
    let patch_body = serde_json::json!({
        "spec": {
            "schema_sensing": {"enabled": cfg.schema_sensing},
            "batch": {"max_events": cfg.max_events, "max_ms": cfg.max_ms},
            "commit_policy": {"mode": cfg.commit_mode},
        }
    });
    let patch_ok = df_patch(&format!("/pipelines/{PIPELINE_NAME}"), patch_body)
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
        df_post(&format!("/pipelines/{PIPELINE_NAME}/resume")).await?;
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

        let current_offset = harness::kafka_offset_for_topic(SOAK_TOPIC)
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
                name,
                format!("drain timed out after {DRAIN_TIMEOUT:?}: {shortfall} events not yet delivered"),
            )
            .note(format!("rows written to MySQL: {rows_written}"))
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

    Ok(ScenarioResult::pass(name)
        .note(format!("target events: {TARGET_EVENTS}"))
        .note(format!("rows written to MySQL: {rows_written}"))
        .note(format!("batch max_events: {}", cfg.max_events))
        .note(format!("commit mode: {}", cfg.commit_mode))
        .note(format!("schema sensing: {}", cfg.schema_sensing))
        .note(format!(
            "write duration: {write_secs:.1}s ({write_tps:.0} rows/s)"
        ))
        .note(format!(
            "drain duration: {drain_secs:.1}s"
        ))
        .note(format!("drain avg throughput: {avg_drain_tps:.0} events/s"))
        .note(format!("drain p50 throughput: {p50_drain_tps:.0} events/s"))
        .note(format!("drain peak throughput: {peak_drain_tps:.0} events/s")))
}

// ── Writer ────────────────────────────────────────────────────────────────────

async fn writer(id: usize, counter: Arc<AtomicU64>) {
    let tables = soak_tables();
    let pool = mysql_async::Pool::new(MYSQL_DSN);
    let mut rng = <rand::rngs::SmallRng as rand::SeedableRng>::seed_from_u64(id as u64);

    loop {
        // Atomically claim a slot; stop if target reached.
        let prev = counter.fetch_add(1, Ordering::Relaxed);
        if prev >= TARGET_EVENTS {
            // Undo the over-increment so the final count stays accurate.
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
                tracing::debug!(writer = id, error = %e, "connection error, retrying");
                // Undo the claim so this slot can be retried.
                counter.fetch_sub(1, Ordering::Relaxed);
                sleep(Duration::from_millis(50)).await;
            }
        }
    }

    let _ = pool.disconnect().await;
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn soak_tables() -> Vec<String> {
    const DOMAINS: &[&str] =
        &["customer", "order", "product", "inventory", "payment", "event"];
    let mut tables = Vec::with_capacity(DOMAINS.len() * 20);
    for domain in DOMAINS {
        for i in 1..=20usize {
            tables.push(format!("soak_{domain}_{i:02}"));
        }
    }
    tables
}

async fn df_patch(path: &str, body: serde_json::Value) -> Result<()> {
    let url = format!("{DF_BASE}{path}");
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

async fn df_post(path: &str) -> Result<()> {
    let url = format!("{DF_BASE}{path}");
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

