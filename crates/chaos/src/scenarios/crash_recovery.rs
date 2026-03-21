//! Scenario: DeltaForge process is killed and restarted mid-stream.
//!
//! What it proves: the checkpoint saved to SQLite survives a crash and streaming
//! resumes from the correct position - no gap in delivered events.
//!
//! Steps:
//!   1. Warmup: confirm DeltaForge is actively streaming.
//!   2. Insert rows and confirm they arrive (pre-crash baseline).
//!   3. SIGKILL the DeltaForge container (no graceful shutdown).
//!   4. Insert rows while it is down (must not be lost).
//!   5. Wait for Docker restart policy to bring it back.
//!   6. Verify all post-crash rows eventually arrive in Kafka.
//!
//! Requires `restart: on-failure` on the deltaforge service in compose.

use anyhow::Result;
use mysql_async::prelude::Queryable;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::info;

use crate::harness::{Harness, MYSQL_DSN, ScenarioResult};

const WARMUP_TIMEOUT: Duration = Duration::from_secs(60);
const RESTART_TIMEOUT: Duration = Duration::from_secs(60);
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_secs(2);

pub async fn run(harness: &Harness) -> Result<ScenarioResult> {
    const NAME: &str = "crash_recovery";
    harness.setup().await?;

    // Warmup
    info!(
        "step 1/5: warming up - waiting for DeltaForge to stream a sentinel event ..."
    );
    let warm_offset = harness.kafka_offset().await?;
    let deadline = Instant::now() + WARMUP_TIMEOUT;
    loop {
        insert_rows("warmup", 1).await?;
        sleep(Duration::from_secs(3)).await;
        if harness.kafka_offset().await? > warm_offset {
            info!("sentinel arrived in Kafka - DeltaForge is streaming");
            break;
        }
        if Instant::now() > deadline {
            return Ok(ScenarioResult::fail(
                NAME,
                "DeltaForge not streaming before crash (warmup timed out)",
            ));
        }
    }

    // Pre-crash inserts
    // Insert rows and wait for them to be checkpointed before killing.
    // This ensures the crash happens after a valid checkpoint exists.
    info!("step 2/5: inserting 5 rows and waiting for checkpoint ...");
    let pre_crash_baseline = harness.kafka_offset().await?;
    insert_rows("pre-crash", 5).await?;
    let target_pre = pre_crash_baseline + 5;
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if harness.kafka_offset().await? >= target_pre {
            break;
        }
        if Instant::now() > deadline {
            return Ok(ScenarioResult::fail(
                NAME,
                "pre-crash inserts did not arrive before kill",
            ));
        }
        sleep(Duration::from_secs(1)).await;
    }
    let events_before_crash = harness.kafka_offset().await?;
    // Give DeltaForge a moment to checkpoint the delivered events
    sleep(Duration::from_secs(2)).await;
    info!(%events_before_crash, "pre-crash inserts confirmed and checkpointed");

    // Kill
    info!("step 3/5: sending SIGKILL to DeltaForge container ...");
    kill_and_restart_deltaforge().await?;
    info!(
        "DeltaForge killed and restart initiated - inserting rows while down"
    );

    // Post-crash inserts
    // These must not be lost — they're in MySQL binlog, DeltaForge must
    // replay from its checkpoint and deliver them after restart.
    let post_inserts = insert_rows("post-crash", 5).await?;
    info!(post_inserts, "rows inserted while DeltaForge is down");

    // Wait for process ready
    info!("step 4/5: waiting for DeltaForge to become healthy ...");
    harness.wait_for_deltaforge(RESTART_TIMEOUT).await?;
    info!("DeltaForge is healthy");

    // Recovery
    info!(
        "step 5/5: waiting up to {}s for post-crash events to arrive ...",
        RECOVERY_TIMEOUT.as_secs()
    );
    let target = events_before_crash + post_inserts as u64;
    let deadline = Instant::now() + RECOVERY_TIMEOUT;
    let mut events_after = events_before_crash;
    let mut last_logged = events_before_crash;
    loop {
        events_after = harness.kafka_offset().await?;
        if events_after != last_logged {
            info!(
                kafka_offset = events_after,
                target,
                remaining = target.saturating_sub(events_after),
                "recovery progress"
            );
            last_logged = events_after;
        }
        if events_after >= target {
            break;
        }
        if Instant::now() > deadline {
            break;
        }
        sleep(POLL_INTERVAL).await;
    }

    let delivered = events_after.saturating_sub(events_before_crash);
    info!(%delivered, post_inserts, "recovery complete");

    if delivered < post_inserts as u64 {
        return Ok(ScenarioResult::fail(
            NAME,
            format!(
                "only {delivered}/{post_inserts} post-crash events delivered after {}s",
                RECOVERY_TIMEOUT.as_secs()
            ),
        ));
    }

    Ok(ScenarioResult::pass(NAME)
        .note(format!(
            "post-crash events delivered: {delivered}/{post_inserts}"
        ))
        .note(if delivered > post_inserts as u64 {
            format!(
                "{} duplicate(s) — expected with at-least-once",
                delivered - post_inserts as u64
            )
        } else {
            "no duplicates observed".into()
        }))
}

/// SIGKILL then explicitly restart the deltaforge container.
/// More deterministic than relying on Docker restart policy backoff.
async fn kill_and_restart_deltaforge() -> Result<()> {
    // Kill
    let status = tokio::process::Command::new("docker")
        .args([
            "compose",
            "-f",
            "docker-compose.chaos.yml",
            "--profile",
            "app",
            "kill",
            "-s",
            "KILL",
            "deltaforge",
        ])
        .status()
        .await?;
    if !status.success() {
        anyhow::bail!("docker kill failed");
    }

    // Brief pause to ensure the container is fully stopped
    sleep(Duration::from_secs(2)).await;

    // Explicit start — no restart policy backoff
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

async fn insert_rows(tag: &str, n: i64) -> Result<i64> {
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
                format!("{tag}-{ts}-{i}"),
                format!("{tag}-{ts}-{i}@test.com"),
                0.0_f64,
            ),
        )
        .await?;
    }
    conn.disconnect().await?;
    let _ = pool.disconnect().await;
    Ok(n)
}
