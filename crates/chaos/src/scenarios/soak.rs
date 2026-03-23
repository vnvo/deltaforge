//! Scenario: long-running endurance soak test.
//!
//! Hammers 120 tables (6 domains × 20) with 16 concurrent writer tasks doing
//! inserts, updates, and deletes while randomly injecting network partitions,
//! sink outages, and process crashes every 2–5 minutes. Designed to run for
//! hours to surface memory leaks, checkpoint drift, and recovery regressions.
//!
//! # Usage
//!
//! ```bash
//! # Start the soak stack (includes deltaforge-soak on ports 8081/9001)
//! docker compose -f docker-compose.chaos.yml --profile soak up -d
//!
//! # Run the soak scenario for 2 hours
//! cargo run -p chaos -- --scenario soak --source mysql --duration-mins 120
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use mysql_async::prelude::Queryable;
use rand::Rng;
use rand::SeedableRng as _;
use rand::rngs::SmallRng;
use tokio::time::sleep;
use tracing::info;

use crate::backend::MYSQL_DSN;
use crate::docker;
use crate::harness::{self, Harness, ScenarioResult};

// ── Constants ─────────────────────────────────────────────────────────────────

/// 6 domains × 20 tables = 120 total.
const DOMAINS: &[&str] = &[
    "customer",
    "order",
    "product",
    "inventory",
    "payment",
    "event",
];
const TABLES_PER_DOMAIN: usize = 20;

/// Default number of concurrent writer goroutines driving DML.
const DEFAULT_WRITER_TASKS: usize = 16;

/// Minimum and maximum seconds between injected faults.
const FAULT_MIN_SECS: u64 = 300;
const FAULT_MAX_SECS: u64 = 600;

/// How long to hold each fault type before restoring.
const PARTITION_HOLD_SECS: u64 = 15;
const OUTAGE_HOLD_SECS: u64 = 20;

/// How long to wait for DeltaForge to recover after a fault.
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(90);

/// Interval between mid-stream ALTER TABLE operations (schema drift injection).
const ALTER_MIN_SECS: u64 = 600; // 10 min
const ALTER_MAX_SECS: u64 = 1800; // 30 min

/// Compose profile and service for the soak DeltaForge instance.
pub const SOAK_PROFILE: &str = "soak";
pub const SOAK_SERVICE: &str = "deltaforge-soak";

/// Health endpoint for the soak DeltaForge (different host port than the regular one).
pub const SOAK_HEALTH_URL: &str = "http://localhost:8081/health";

/// Kafka topic written by the soak DeltaForge pipeline.
pub const SOAK_TOPIC: &str = "chaos.soak";

// ── Public entry point ────────────────────────────────────────────────────────

pub async fn run(
    harness: &Harness,
    duration_mins: u64,
    writer_tasks: usize,
    write_delay_ms: u64,
) -> Result<ScenarioResult> {
    let name = "soak";
    harness.setup().await?;

    // Step 1: ensure soak DeltaForge is healthy.
    info!("step 1/4: waiting for soak DeltaForge at {SOAK_HEALTH_URL} ...");
    harness::wait_for_url(SOAK_HEALTH_URL, Duration::from_secs(60)).await?;
    info!("soak DeltaForge is healthy");

    // Step 2: seed tables if needed (idempotent — MySQL init SQL already created them).
    info!(
        tables = DOMAINS.len() * TABLES_PER_DOMAIN,
        "step 2/4: verifying soak tables exist ..."
    );
    seed_tables().await?;

    // Step 3: launch writer tasks + schema alter task.
    let total_written = Arc::new(AtomicU64::new(0));
    // `stop_flag` is the reliable termination signal — checked at the top of
    // every loop iteration so no task can miss it regardless of timing.
    // `wake` is a best-effort nudge that interrupts the alter loop's long sleep
    // early; it is NOT used as the authoritative stop check.
    let stop_flag = Arc::new(AtomicBool::new(false));
    let wake = Arc::new(tokio::sync::Notify::new());

    let writer_tasks = if writer_tasks == 0 {
        DEFAULT_WRITER_TASKS
    } else {
        writer_tasks
    };
    info!(
        writer_tasks,
        write_delay_ms, "step 3/4: starting writer tasks ..."
    );
    let writer_handles: Vec<_> = (0..writer_tasks)
        .map(|id| {
            let written = Arc::clone(&total_written);
            let flag = Arc::clone(&stop_flag);
            tokio::spawn(async move {
                writer_loop(id, written, flag, write_delay_ms).await
            })
        })
        .collect();

    let alter_handle = {
        let flag = Arc::clone(&stop_flag);
        let w = Arc::clone(&wake);
        tokio::spawn(async move { alter_loop(flag, w).await })
    };

    // Step 4: endurance loop with random faults.
    info!(duration_mins, "step 4/4: running endurance loop ...");
    let deadline = Instant::now() + Duration::from_secs(duration_mins * 60);
    let mut rng = rand::thread_rng();
    let mut faults: Vec<FaultRecord> = Vec::new();
    let mut stats_samples: Vec<ResourceSample> = Vec::new();

    // Kafka baseline so we can count delivered events over the run.
    let kafka_start = harness::kafka_offset_for_topic(SOAK_TOPIC)
        .await
        .unwrap_or(0);

    while Instant::now() < deadline {
        let remaining = deadline.duration_since(Instant::now());
        let wait_secs = rng.gen_range(FAULT_MIN_SECS..=FAULT_MAX_SECS);
        let actual_wait = Duration::from_secs(wait_secs).min(remaining);

        // Less than 30 s left — not enough time for a meaningful fault cycle.
        if actual_wait < Duration::from_secs(30) {
            break;
        }

        // Sample resource usage before sleeping.
        sample_and_log(&mut stats_samples, &total_written).await;

        sleep(actual_wait).await;

        // Re-check deadline after sleeping — the sleep may have consumed all
        // remaining time, in which case skip the fault + recovery to exit cleanly.
        if Instant::now() >= deadline {
            break;
        }

        // Pick and inject a random fault.
        let fault_idx = rng.gen_range(0usize..3);
        let fault_name =
            ["network_partition", "sink_outage", "crash"][fault_idx];
        info!(%fault_name, "injecting fault");

        let fault_start = Instant::now();
        let inject_result = match fault_idx {
            0 => inject_network_partition(harness).await,
            1 => inject_sink_outage(harness).await,
            _ => inject_crash().await,
        };

        match inject_result {
            Ok(recovery) => {
                info!(
                    %fault_name,
                    recovery_secs = recovery.as_secs_f64(),
                    "fault recovery complete"
                );
                faults.push(FaultRecord {
                    kind: fault_name,
                    recovery_secs: recovery.as_secs_f64(),
                    recovered: true,
                });
            }
            Err(e) => {
                info!(%fault_name, error = %e, "fault recovery timed out or errored");
                faults.push(FaultRecord {
                    kind: fault_name,
                    recovery_secs: fault_start.elapsed().as_secs_f64(),
                    recovered: false,
                });
            }
        }
    }

    // Final resource sample.
    sample_and_log(&mut stats_samples, &total_written).await;

    // Stop writers and alter task.
    stop_flag.store(true, Ordering::Relaxed);
    wake.notify_waiters(); // wake alter loop from its long inter-fault sleep
    for h in writer_handles {
        let _ = h.await;
    }
    let alters = alter_handle.await.unwrap_or_default();

    // Gather totals.
    let kafka_end = harness::kafka_offset_for_topic(SOAK_TOPIC)
        .await
        .unwrap_or(0);
    let delivered = kafka_end.saturating_sub(kafka_start);
    let written = total_written.load(Ordering::Relaxed);
    let failed = faults.iter().filter(|f| !f.recovered).count();
    let alters_ok = alters.iter().filter(|a| a.ok).count();
    let alters_failed = alters.iter().filter(|a| !a.ok).count();
    let avg_recovery = if faults.is_empty() {
        0.0
    } else {
        faults.iter().map(|f| f.recovery_secs).sum::<f64>()
            / faults.len() as f64
    };
    let max_recovery = faults
        .iter()
        .map(|f| f.recovery_secs)
        .fold(0.0_f64, f64::max);
    let peak_mem = stats_samples.iter().map(|s| s.mem_bytes).max().unwrap_or(0);
    let max_cpu = stats_samples
        .iter()
        .map(|s| (s.cpu_percent * 10.0) as u64)
        .max()
        .unwrap_or(0);

    let mut result = if failed == 0 {
        ScenarioResult::pass(name)
    } else {
        ScenarioResult::fail(
            name,
            format!("{failed} fault(s) did not recover within timeout"),
        )
    };

    result = result
        .note(format!("duration: {duration_mins} min"))
        .note(format!(
            "tables: {} ({} domains × {} each)",
            DOMAINS.len() * TABLES_PER_DOMAIN,
            DOMAINS.len(),
            TABLES_PER_DOMAIN
        ))
        .note(format!("writer tasks: {writer_tasks}"))
        .note(format!("write delay max: {write_delay_ms} ms"))
        .note(format!("rows written to DB: {written}"))
        .note(format!("events delivered to Kafka: {delivered}"))
        .note(format!("faults injected: {}", faults.len()))
        .note(format!("recovery failures: {failed}"))
        .note(format!("avg recovery: {avg_recovery:.1}s"))
        .note(format!("max recovery: {max_recovery:.1}s"))
        .note(format!("peak memory: {} MiB", peak_mem / 1024 / 1024))
        .note(format!("max CPU: {:.1}%", max_cpu as f64 / 10.0))
        .note(format!("schema alters applied: {alters_ok}"))
        .note(format!("schema alters failed: {alters_failed}"));

    for fault in &faults {
        result = result.note(format!(
            "  {} → {:.1}s ({})",
            fault.kind,
            fault.recovery_secs,
            if fault.recovered { "OK" } else { "TIMEOUT" }
        ));
    }

    Ok(result)
}

// ── Stable (no-fault) variant ─────────────────────────────────────────────────

/// Same as [`run`] but with **no fault injection** — only writers and ALTER TABLE
/// run for the full duration. Use this to establish a steady-state baseline
/// for metrics like cache hit ratio, E2E latency, and resource usage.
pub async fn run_stable(
    harness: &Harness,
    duration_mins: u64,
    writer_tasks: usize,
    write_delay_ms: u64,
) -> Result<ScenarioResult> {
    let name = "soak-stable";
    harness.setup().await?;

    info!("step 1/3: waiting for soak DeltaForge at {SOAK_HEALTH_URL} ...");
    harness::wait_for_url(SOAK_HEALTH_URL, Duration::from_secs(60)).await?;
    info!("soak DeltaForge is healthy");

    info!(
        tables = DOMAINS.len() * TABLES_PER_DOMAIN,
        "step 2/3: verifying soak tables exist ..."
    );
    seed_tables().await?;

    let total_written = Arc::new(AtomicU64::new(0));
    let stop_flag = Arc::new(AtomicBool::new(false));
    let wake = Arc::new(tokio::sync::Notify::new());

    let writer_tasks = if writer_tasks == 0 {
        DEFAULT_WRITER_TASKS
    } else {
        writer_tasks
    };
    info!(
        writer_tasks,
        write_delay_ms, "step 3/3: starting writer tasks (no faults) ..."
    );
    let writer_handles: Vec<_> = (0..writer_tasks)
        .map(|id| {
            let written = Arc::clone(&total_written);
            let flag = Arc::clone(&stop_flag);
            tokio::spawn(async move {
                writer_loop(id, written, flag, write_delay_ms).await
            })
        })
        .collect();

    let alter_handle = {
        let flag = Arc::clone(&stop_flag);
        let w = Arc::clone(&wake);
        tokio::spawn(async move { alter_loop(flag, w).await })
    };

    let kafka_start = harness::kafka_offset_for_topic(SOAK_TOPIC)
        .await
        .unwrap_or(0);
    let deadline = Instant::now() + Duration::from_secs(duration_mins * 60);
    let mut stats_samples: Vec<ResourceSample> = Vec::new();

    // Just wait — sample resource usage every 60 s.
    while Instant::now() < deadline {
        sample_and_log(&mut stats_samples, &total_written).await;
        let remaining = deadline.duration_since(Instant::now());
        sleep(remaining.min(Duration::from_secs(60))).await;
    }

    sample_and_log(&mut stats_samples, &total_written).await;

    stop_flag.store(true, Ordering::Relaxed);
    wake.notify_waiters();
    for h in writer_handles {
        let _ = h.await;
    }
    let alters = alter_handle.await.unwrap_or_default();

    let kafka_end = harness::kafka_offset_for_topic(SOAK_TOPIC)
        .await
        .unwrap_or(0);
    let delivered = kafka_end.saturating_sub(kafka_start);
    let written = total_written.load(Ordering::Relaxed);
    let alters_ok = alters.iter().filter(|a| a.ok).count();
    let alters_failed = alters.iter().filter(|a| !a.ok).count();
    let peak_mem = stats_samples.iter().map(|s| s.mem_bytes).max().unwrap_or(0);
    let max_cpu = stats_samples
        .iter()
        .map(|s| (s.cpu_percent * 10.0) as u64)
        .max()
        .unwrap_or(0);

    let result = ScenarioResult::pass(name)
        .note(format!("duration: {duration_mins} min"))
        .note(format!(
            "tables: {} ({} domains × {} each)",
            DOMAINS.len() * TABLES_PER_DOMAIN,
            DOMAINS.len(),
            TABLES_PER_DOMAIN
        ))
        .note(format!("writer tasks: {writer_tasks}"))
        .note(format!("write delay max: {write_delay_ms} ms"))
        .note(format!("rows written to DB: {written}"))
        .note(format!("events delivered to Kafka: {delivered}"))
        .note("faults injected: 0 (stable baseline run)")
        .note(format!("peak memory: {} MiB", peak_mem / 1024 / 1024))
        .note(format!("max CPU: {:.1}%", max_cpu as f64 / 10.0))
        .note(format!("schema alters applied: {alters_ok}"))
        .note(format!("schema alters failed: {alters_failed}"));

    Ok(result)
}

// ── Writer loop ───────────────────────────────────────────────────────────────

/// All table names built from the domain × index product.
fn all_tables() -> Vec<String> {
    let mut tables = Vec::with_capacity(DOMAINS.len() * TABLES_PER_DOMAIN);
    for domain in DOMAINS {
        for i in 1..=TABLES_PER_DOMAIN {
            tables.push(format!("soak_{domain}_{i:02}"));
        }
    }
    tables
}

async fn seed_tables() -> Result<()> {
    // The init SQL already creates the tables; just verify we can connect.
    let pool = mysql_async::Pool::new(MYSQL_DSN);
    let mut conn = pool.get_conn().await?;
    let count: u64 = conn
        .query_first("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'orders' AND table_name LIKE 'soak_%'")
        .await?
        .unwrap_or(0);
    conn.disconnect().await?;
    let _ = pool.disconnect().await;
    info!(soak_tables = count, "soak tables confirmed in MySQL");
    Ok(())
}

async fn writer_loop(
    id: usize,
    written: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    write_delay_ms: u64,
) {
    let tables = all_tables();
    let pool = match mysql_async::Pool::new(MYSQL_DSN).get_conn().await {
        Ok(_) => mysql_async::Pool::new(MYSQL_DSN),
        Err(e) => {
            tracing::warn!(writer = id, error = %e, "writer failed to connect, exiting");
            return;
        }
    };

    let mut rng = SmallRng::from_entropy();

    loop {
        // Compute the sleep duration before the select so rng is not held
        // across the await point (required for Send).
        if stop.load(Ordering::Relaxed) {
            break;
        }

        let delay_ms = if write_delay_ms == 0 {
            0
        } else {
            rng.gen_range(1u64..=write_delay_ms)
        };
        if delay_ms > 0 {
            sleep(Duration::from_millis(delay_ms)).await;
        }

        let table = &tables[rng.gen_range(0..tables.len())];
        let op = rng.gen_range(0u8..10); // 0-6 insert, 7-8 update, 9 delete

        let mut conn = match pool.get_conn().await {
            Ok(c) => c,
            Err(e) => {
                tracing::debug!(writer = id, error = %e, "writer connection error, retrying");
                sleep(Duration::from_millis(200)).await;
                continue;
            }
        };

        let ts = now_ms();
        let result = if op < 7 {
            conn.exec_drop(
                format!("INSERT INTO {table} (tag, data, value, status) VALUES (?, ?, ?, ?)"),
                (
                    format!("w{id}-{ts}"),
                    format!("payload-{ts}"),
                    rng.gen_range(0.0..10000.0_f64),
                    rng.gen_range(0u8..4),
                ),
            )
            .await
        } else if op < 9 {
            conn.exec_drop(
                format!("UPDATE {table} SET value = ?, status = ?, updated_at = NOW() WHERE id = ? LIMIT 1"),
                (
                    rng.gen_range(0.0..10000.0_f64),
                    rng.gen_range(0u8..4),
                    rng.gen_range(1u64..5000),
                ),
            )
            .await
        } else {
            conn.exec_drop(
                format!("DELETE FROM {table} WHERE id = ? LIMIT 1"),
                (rng.gen_range(1u64..5000),),
            )
            .await
        };

        if result.is_ok() {
            written.fetch_add(1, Ordering::Relaxed);
        }

        let _ = conn.disconnect().await;
    }

    let _ = pool.disconnect().await;
}

// ── Fault injectors ───────────────────────────────────────────────────────────

/// Cut the MySQL Toxiproxy for PARTITION_HOLD_SECS, restore, then wait for
/// DeltaForge to recover. Returns the total elapsed time (hold + recovery).
async fn inject_network_partition(harness: &Harness) -> Result<Duration> {
    let start = Instant::now();
    harness.toxi.disable("mysql").await?;
    sleep(Duration::from_secs(PARTITION_HOLD_SECS)).await;
    harness.toxi.enable("mysql").await?;
    harness::wait_for_url(SOAK_HEALTH_URL, RECOVERY_TIMEOUT).await?;
    Ok(start.elapsed())
}

/// Cut the Kafka Toxiproxy for OUTAGE_HOLD_SECS, restore, then wait for
/// DeltaForge to recover and flush pending events.
async fn inject_sink_outage(harness: &Harness) -> Result<Duration> {
    let start = Instant::now();
    harness.toxi.disable("kafka").await?;
    sleep(Duration::from_secs(OUTAGE_HOLD_SECS)).await;
    harness.toxi.enable("kafka").await?;
    harness::wait_for_url(SOAK_HEALTH_URL, RECOVERY_TIMEOUT).await?;
    Ok(start.elapsed())
}

/// SIGKILL the soak DeltaForge container, wait for the restart policy to bring
/// it back, then wait for the health endpoint.
async fn inject_crash() -> Result<Duration> {
    let start = Instant::now();
    docker::kill_service(SOAK_PROFILE, SOAK_SERVICE).await?;
    docker::start_service(SOAK_PROFILE, SOAK_SERVICE).await?;
    harness::wait_for_url(SOAK_HEALTH_URL, RECOVERY_TIMEOUT).await?;
    Ok(start.elapsed())
}

// ── Resource sampling ─────────────────────────────────────────────────────────

struct ResourceSample {
    mem_bytes: u64,
    cpu_percent: f64,
}

async fn sample_and_log(
    samples: &mut Vec<ResourceSample>,
    written: &Arc<AtomicU64>,
) {
    match docker::sample_stats(SOAK_PROFILE, SOAK_SERVICE).await {
        Ok(s) if s.mem_bytes > 0 => {
            info!(
                cpu = s.cpu_percent,
                mem_mib = s.mem_bytes / 1024 / 1024,
                rows_written = written.load(Ordering::Relaxed),
                "resource snapshot"
            );
            samples.push(ResourceSample {
                mem_bytes: s.mem_bytes,
                cpu_percent: s.cpu_percent,
            });
        }
        _ => {}
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

struct FaultRecord {
    kind: &'static str,
    recovery_secs: f64,
    recovered: bool,
}

// ── Schema alter loop ─────────────────────────────────────────────────────────

struct AlterRecord {
    ok: bool,
}

/// Periodically fires an ALTER TABLE ADD COLUMN against a random soak table
/// while the endurance loop is running. Uses unique random column names so
/// multiple runs against the same volumes don't collide. Errors are logged
/// and swallowed — a failed alter is interesting but not a test failure.
async fn alter_loop(
    stop: Arc<AtomicBool>,
    wake: Arc<tokio::sync::Notify>,
) -> Vec<AlterRecord> {
    let tables = all_tables();
    let mut rng = SmallRng::from_entropy();
    let mut records: Vec<AlterRecord> = Vec::new();
    let pool = mysql_async::Pool::new(MYSQL_DSN);

    loop {
        if stop.load(Ordering::Relaxed) {
            break;
        }

        // Wait for the inter-alter interval, but wake early if stop fires.
        let wait_secs = rng.gen_range(ALTER_MIN_SECS..=ALTER_MAX_SECS);
        tokio::select! {
            _ = wake.notified() => {}
            _ = sleep(Duration::from_secs(wait_secs)) => {}
        }

        if stop.load(Ordering::Relaxed) {
            break;
        }

        let table = tables[rng.gen_range(0..tables.len())].clone();
        // Random suffix makes the column name unique across restarts.
        let col_suffix: u32 = rng.r#gen();
        let col_name = format!("ext_{col_suffix:08x}");

        let sql = match rng.gen_range(0u8..5) {
            0 => format!(
                "ALTER TABLE {table} ADD COLUMN {col_name} BIGINT DEFAULT NULL"
            ),
            1 => format!(
                "ALTER TABLE {table} ADD COLUMN {col_name} VARCHAR(128) DEFAULT NULL"
            ),
            2 => format!(
                "ALTER TABLE {table} ADD COLUMN {col_name} BOOLEAN DEFAULT FALSE"
            ),
            3 => format!(
                "ALTER TABLE {table} ADD COLUMN {col_name} JSON DEFAULT NULL"
            ),
            _ => format!(
                "ALTER TABLE {table} ADD COLUMN {col_name} FLOAT DEFAULT NULL"
            ),
        };

        match pool.get_conn().await {
            Ok(mut conn) => match conn.exec_drop(&sql, ()).await {
                Ok(_) => {
                    info!(%table, %col_name, "mid-stream schema alter applied");
                    records.push(AlterRecord { ok: true });
                }
                Err(e) => {
                    info!(%table, %col_name, error = %e, "mid-stream schema alter failed (ignored)");
                    records.push(AlterRecord { ok: false });
                }
            },
            Err(e) => {
                tracing::warn!(%table, error = %e, "alter loop failed to get connection");
                records.push(AlterRecord { ok: false });
            }
        }
    }

    let _ = pool.disconnect().await;
    records
}
