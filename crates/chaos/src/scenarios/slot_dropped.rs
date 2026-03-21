//! Scenario: PostgreSQL replication slot is dropped while DeltaForge is offline.
//!
//! What it proves: the position guard detects that the replication slot is gone
//! and halts rather than silently connecting to a different slot or starting
//! from the WAL head — mirroring the MySQL binlog-purge guard.
//!
//! Steps:
//!   1. Warmup: confirm DeltaForge is streaming and has a valid checkpoint.
//!   2. Stop DeltaForge cleanly so the checkpoint is saved.
//!   3. Drop the replication slot on Postgres — checkpoint position is now lost.
//!   4. Restart DeltaForge.
//!   5. Verify it halts rather than silently resuming.
//!
//! Cleanup: clear the checkpoint DB and restart DeltaForge so subsequent runs
//! start fresh (DeltaForge will recreate the slot on a clean start).

use anyhow::Result;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::info;

use crate::backend::PG_DSN;
use crate::docker;
use crate::harness::{Harness, ScenarioResult};

// Slot name must match `slot_name` in chaos/config/pg-to-kafka.yaml.
const SLOT_NAME: &str = "chaos_slot";

const WARMUP_TIMEOUT: Duration = Duration::from_secs(60);
const UNHEALTHY_TIMEOUT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_secs(2);

pub async fn run(harness: &Harness) -> Result<ScenarioResult> {
    const NAME: &str = "postgres/slot_dropped";
    harness.setup().await?;

    // ── Warmup ────────────────────────────────────────────────────────────────
    info!("step 1/4: warming up - confirming DeltaForge has a checkpoint ...");
    let warm_offset = harness.kafka_offset().await?;
    let deadline = Instant::now() + WARMUP_TIMEOUT;
    loop {
        insert_row().await?;
        sleep(Duration::from_secs(3)).await;
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
    }
    // Give DeltaForge time to write the checkpoint after sink ack.
    sleep(Duration::from_secs(3)).await;

    // ── Stop and drop slot ────────────────────────────────────────────────────
    info!("step 2/4: stopping DeltaForge cleanly to preserve checkpoint ...");
    docker::stop_service("pg-app", "deltaforge-pg").await?;
    info!("DeltaForge stopped");

    insert_row().await?;
    drop_replication_slot().await?;
    info!(
        "replication slot '{SLOT_NAME}' dropped — checkpoint position is now lost"
    );

    // ── Restart ───────────────────────────────────────────────────────────────
    info!(
        "step 3/4: restarting DeltaForge — expecting it to detect lost slot and halt ..."
    );
    docker::start_service("pg-app", "deltaforge-pg").await?;

    // ── Verify unhealthy ──────────────────────────────────────────────────────
    info!(
        timeout_secs = UNHEALTHY_TIMEOUT.as_secs(),
        "step 4/4: polling health endpoint ..."
    );
    let deadline = Instant::now() + UNHEALTHY_TIMEOUT;
    let mut went_unhealthy = false;
    loop {
        let healthy = reqwest::get("http://localhost:8080/health")
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false);

        if !healthy {
            info!(
                "DeltaForge is unhealthy — slot-dropped guard fired correctly"
            );
            went_unhealthy = true;
            break;
        }
        if Instant::now() > deadline {
            break;
        }
        sleep(POLL_INTERVAL).await;
    }

    // ── Cleanup ───────────────────────────────────────────────────────────────
    // Clear the stale checkpoint so the next run starts fresh. DeltaForge will
    // recreate the replication slot on a clean start.
    let _ = docker::stop_service("pg-app", "deltaforge-pg").await;
    let _ = docker::clear_checkpoint("pg-app", "deltaforge-pg").await;
    let _ = docker::start_service("pg-app", "deltaforge-pg").await;

    if !went_unhealthy {
        return Ok(ScenarioResult::fail(
            NAME,
            format!(
                "DeltaForge remained healthy for {}s after slot was dropped — \
                 guard may not be firing",
                UNHEALTHY_TIMEOUT.as_secs()
            ),
        ));
    }

    Ok(ScenarioResult::pass(NAME).note(
        "DeltaForge correctly halted after detecting that the replication slot was dropped",
    ))
}

async fn drop_replication_slot() -> Result<()> {
    let (client, conn) =
        tokio_postgres::connect(PG_DSN, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    // SELECT (not DO) so errors surface immediately rather than being swallowed.
    client
        .execute(
            "SELECT pg_drop_replication_slot(slot_name) \
             FROM pg_replication_slots \
             WHERE slot_name = $1",
            &[&SLOT_NAME],
        )
        .await?;
    Ok(())
}

async fn insert_row() -> Result<()> {
    let (client, conn) =
        tokio_postgres::connect(PG_DSN, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let name = format!("slot-drop-{ts}");
    let email = format!("slot-drop-{ts}@test.com");
    let balance = 0.0_f64;
    client
        .execute(
            "INSERT INTO customers (name, email, balance) VALUES ($1, $2, $3)",
            &[&name, &email, &balance],
        )
        .await?;
    Ok(())
}
