//! Scenario: PostgreSQL primary is replaced mid-stream (failover / promotion).
//!
//! What it proves: DeltaForge detects the server identity change via
//! `system_identifier` comparison, discovers the checkpoint position (LSN /
//! replication slot) is unreachable on the new server, and halts cleanly —
//! mirroring the MySQL UUID-based failover guard.
//!
//! Steps:
//!   1. Warmup: confirm DeltaForge is actively streaming on postgres-a.
//!   2. Cut the postgres proxy so the active connection is torn down.
//!   3. Switch the proxy upstream to postgres-b (fresh server, different
//!      system_identifier, no replication slot).
//!   4. Re-enable the proxy — DeltaForge reconnects to postgres-b.
//!   5. check_identity_post_reconnect detects system_identifier change.
//!   6. check_position_reachability returns Lost (slot missing on postgres-b).
//!   7. DeltaForge halts — health endpoint goes unhealthy.
//!   8. Restore proxy to postgres-a, clear checkpoint, restart for next scenario.
//!
//! Requires postgres-b service in docker-compose.chaos.yml.

use anyhow::Result;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::info;

use crate::backend::PG_DSN;
use crate::docker;
use crate::harness::{Harness, ScenarioResult};

const WARMUP_TIMEOUT: Duration = Duration::from_secs(60);
const UNHEALTHY_TIMEOUT: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_secs(2);

pub async fn run(harness: &Harness) -> Result<ScenarioResult> {
    const NAME: &str = "postgres/pg_failover";
    harness.setup().await?;

    // ── Warmup ────────────────────────────────────────────────────────────────
    info!(
        "step 1/5: warming up — confirming DeltaForge is streaming on postgres-a ..."
    );
    let warm_offset = harness.kafka_offset().await?;
    let deadline = Instant::now() + WARMUP_TIMEOUT;
    loop {
        insert_row().await?;
        sleep(Duration::from_secs(3)).await;
        if harness.kafka_offset().await? > warm_offset {
            info!("sentinel arrived in Kafka — DeltaForge is streaming");
            break;
        }
        if Instant::now() > deadline {
            return Ok(ScenarioResult::fail(
                NAME,
                "DeltaForge not streaming before failover (warmup timed out)",
            ));
        }
    }

    // ── Switch ────────────────────────────────────────────────────────────────
    info!(
        "step 2/5: cutting postgres proxy to tear down active connection ..."
    );
    harness.toxi.disable("postgres").await?;
    sleep(Duration::from_secs(2)).await;

    info!(
        "step 3/5: switching proxy upstream from postgres-a to postgres-b ..."
    );
    harness
        .toxi
        .update_upstream("postgres", "postgres-b:5432")
        .await?;

    info!(
        "step 4/5: re-enabling proxy — DeltaForge will reconnect to postgres-b ..."
    );
    harness.toxi.enable("postgres").await?;

    // ── Verify ────────────────────────────────────────────────────────────────
    // DeltaForge should reconnect, run check_identity_post_reconnect, detect
    // the system_identifier change, call check_position_reachability (which
    // returns Lost because postgres-b has no replication slot), and halt.
    info!(
        timeout_secs = UNHEALTHY_TIMEOUT.as_secs(),
        "step 5/5: polling health endpoint — expecting unhealthy ..."
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
                "DeltaForge is unhealthy — pg failover guard fired correctly"
            );
            went_unhealthy = true;
            break;
        }
        if Instant::now() > deadline {
            break;
        }
        sleep(POLL_INTERVAL).await;
    }

    // ── Restore ───────────────────────────────────────────────────────────────
    // Restore the proxy to postgres-a, clear the stale checkpoint (it references
    // a slot on postgres-a that DeltaForge may have already invalidated), then
    // restart so subsequent scenarios start fresh.
    info!(
        "restoring postgres proxy upstream to postgres-a and restarting DeltaForge ..."
    );
    let _ = harness
        .toxi
        .update_upstream("postgres", "postgres:5432")
        .await;
    let _ = harness.toxi.enable("postgres").await;
    let _ = docker::stop_service("pg-app", "deltaforge-pg").await;
    let _ = docker::clear_checkpoint("pg-app", "deltaforge-pg").await;
    let _ = docker::start_service("pg-app", "deltaforge-pg").await;
    let _ = harness.wait_for_deltaforge(Duration::from_secs(60)).await;
    info!("DeltaForge restored");

    if !went_unhealthy {
        return Ok(ScenarioResult::fail(
            NAME,
            format!(
                "DeltaForge remained healthy for {}s after switching to a \
                 different server — identity guard may not be firing",
                UNHEALTHY_TIMEOUT.as_secs()
            ),
        ));
    }

    Ok(ScenarioResult::pass(NAME).note(
        "DeltaForge correctly halted after detecting server identity change \
         (slot missing on new server)",
    ))
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
    let name = format!("pg-failover-{ts}");
    let email = format!("pg-failover-{ts}@test.com");
    let balance = 0.0_f64;
    client
        .execute(
            "INSERT INTO customers (name, email, balance) VALUES ($1, $2, $3)",
            &[&name, &email, &balance],
        )
        .await?;
    Ok(())
}
