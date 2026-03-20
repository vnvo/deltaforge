//! Scenario: MySQL binlog is purged while DeltaForge is offline.
//!
//! What it proves: the snapshot health guard (F12) detects that the saved
//! GTID/binlog position has been purged and raises an error rather than
//! silently skipping events or reconnecting to an invalid position.
//!
//! Steps:
//!   1. Warmup: confirm DeltaForge is streaming and has a valid checkpoint.
//!   2. Stop DeltaForge cleanly so the checkpoint is saved.
//!   3. Insert rows then purge all binary logs — checkpoint is now invalid.
//!   4. Restart DeltaForge.
//!   5. Verify it halts with an error rather than silently resuming.
//!
//! NOTE: RESET BINARY LOGS AND GTIDS wipes GTID state — MySQL will have a
//! fresh GTID sequence after this. This scenario must run last in `--scenario all`.

use anyhow::Result;
use mysql_async::prelude::Queryable;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::info;

use crate::harness::{Harness, MYSQL_DSN, ScenarioResult};

const WARMUP_TIMEOUT: Duration = Duration::from_secs(15);
const UNHEALTHY_TIMEOUT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_secs(2);

pub async fn run(harness: &Harness) -> Result<ScenarioResult> {
    const NAME: &str = "binlog_purge";
    harness.setup().await?;

    // ── Warmup ────────────────────────────────────────────────────────────────
    // Ensure DeltaForge has a valid checkpoint before we stop it.
    info!("step 1/4: warming up - confirming DeltaForge has a checkpoint ...");
    let warm_offset = harness.kafka_offset().await?;
    insert_rows(1).await?;
    let deadline = Instant::now() + WARMUP_TIMEOUT;
    loop {
        if harness.kafka_offset().await? > warm_offset {
            info!("checkpoint confirmed - DeltaForge is streaming");
            break;
        }
        if Instant::now() > deadline {
            return Ok(ScenarioResult::fail(
                NAME,
                "DeltaForge not streaming before stop (warmup timed out)",
            ));
        }
        sleep(Duration::from_secs(1)).await;
    }
    // Give DeltaForge a moment to write the checkpoint to SQLite.
    // The checkpoint is written after sink ack — wait for a few more events
    // to ensure at least one full checkpoint cycle has completed.
    sleep(Duration::from_secs(3)).await;

    // ── Stop and purge ────────────────────────────────────────────────────────
    info!("step 2/4: stopping DeltaForge cleanly to preserve checkpoint ...");
    stop_deltaforge().await?;
    // Clean stop (SIGTERM) flushes the checkpoint to SQLite before exit.
    // SQLite WAL is on the Docker volume and replays automatically on restart —
    // no manual WAL checkpoint needed.
    info!("DeltaForge stopped");

    insert_rows(3).await?;
    purge_binlogs().await?;
    info!("binlogs purged - checkpoint GTID position is now invalid");

    // ── Restart ───────────────────────────────────────────────────────────────
    info!(
        "step 3/4: restarting DeltaForge - expecting it to detect purge and halt ..."
    );
    start_deltaforge().await?;

    // ── Verify unhealthy ──────────────────────────────────────────────────────
    // Poll the health endpoint — DeltaForge should go down, not stay up.
    // Allow a brief window for it to start up and then detect the purge.
    info!(
        "step 4/4: polling health endpoint for up to {}s ...",
        UNHEALTHY_TIMEOUT.as_secs()
    );
    let deadline = Instant::now() + UNHEALTHY_TIMEOUT;
    let mut went_unhealthy = false;
    loop {
        let healthy = reqwest::get("http://localhost:8080/healthz")
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false);

        if !healthy {
            info!(
                "DeltaForge is unhealthy - binlog purge guard fired correctly"
            );
            went_unhealthy = true;
            break;
        }

        if Instant::now() > deadline {
            break;
        }
        sleep(POLL_INTERVAL).await;
    }

    // Always stop deltaforge after this scenario — binlog state is corrupted
    let _ = stop_deltaforge().await;

    if !went_unhealthy {
        return Ok(ScenarioResult::fail(
            NAME,
            format!(
                "DeltaForge remained healthy for {}s after binlog purge — guard may not be firing",
                UNHEALTHY_TIMEOUT.as_secs()
            ),
        ));
    }

    Ok(ScenarioResult::pass(NAME).note(
        "DeltaForge correctly halted after detecting purged binlog position",
    ))
}

async fn stop_deltaforge() -> Result<()> {
    let status = tokio::process::Command::new("docker")
        .args([
            "compose",
            "-f",
            "docker-compose.chaos.yml",
            "--profile",
            "app",
            "stop",
            "deltaforge",
        ])
        .status()
        .await?;
    if !status.success() {
        anyhow::bail!("docker stop failed");
    }
    Ok(())
}

async fn start_deltaforge() -> Result<()> {
    let status = tokio::process::Command::new("docker")
        .args([
            "compose",
            "-f",
            "docker-compose.chaos.yml",
            "--profile",
            "app",
            "start",
            "deltaforge",
        ])
        .status()
        .await?;
    if !status.success() {
        anyhow::bail!("docker start failed");
    }
    Ok(())
}

async fn purge_binlogs() -> Result<()> {
    let pool = mysql_async::Pool::new(MYSQL_DSN);
    let mut conn = pool.get_conn().await?;
    // MySQL 8.0+: RESET BINARY LOGS AND GTIDS purges all binlogs and GTID state
    // Falls back to RESET MASTER for older versions
    if let Err(_) = conn.query_drop("RESET BINARY LOGS AND GTIDS").await {
        conn.query_drop("RESET MASTER").await?;
    }
    conn.disconnect().await?;
    let _ = pool.disconnect().await;
    Ok(())
}

async fn insert_rows(n: i64) -> Result<i64> {
    let pool = mysql_async::Pool::new(MYSQL_DSN);
    let mut conn = pool.get_conn().await?;
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    for i in 0..n {
        conn.exec_drop(
            "INSERT INTO customers (name, email, balance) VALUES (?, ?, ?)",
            (
                format!("purge-{ts}-{i}"),
                format!("purge-{ts}-{i}@test.com"),
                0.0_f64,
            ),
        )
        .await?;
    }
    conn.disconnect().await?;
    let _ = pool.disconnect().await;
    Ok(n)
}
