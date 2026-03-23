//! Scenario: MySQL server is replaced mid-stream (primary failover / promotion).
//!
//! What it proves: DeltaForge detects the server identity change via UUID
//! comparison, runs reconciliation, discovers the checkpoint position is lost
//! on the new server, and halts cleanly — rather than silently streaming from
//! an inconsistent position.
//!
//! Steps:
//!   1. Warmup: confirm DeltaForge is actively streaming on mysql-a.
//!   2. Cut the MySQL proxy so the current connection is torn down.
//!   3. Switch the proxy upstream to mysql-b (fresh server, different UUID).
//!   4. Re-enable the proxy — DeltaForge reconnects to mysql-b.
//!   5. check_identity_post_reconnect detects UUID change → reconciliation.
//!   6. check_position_reachability returns Lost (mysql-b has no binlog history).
//!   7. DeltaForge halts — health endpoint goes unhealthy.
//!   8. Restore proxy to mysql-a and restart DeltaForge for subsequent scenarios.
//!
//! Requires mysql-b service in docker-compose.chaos.yml.

use anyhow::Result;
use mysql_async::prelude::Queryable;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::info;

use crate::backend::MYSQL_DSN;
use crate::docker;
use crate::harness::{Harness, ScenarioResult};

const WARMUP_TIMEOUT: Duration = Duration::from_secs(60);
// DeltaForge must lose the connection, hit backoff, reconnect to mysql-b,
// run identity check, discover position lost — then /health returns 503.
const UNHEALTHY_TIMEOUT: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_secs(2);

pub async fn run(harness: &Harness) -> Result<ScenarioResult> {
    const NAME: &str = "failover";
    harness.setup().await?;

    // ── Warmup ────────────────────────────────────────────────────────────────
    info!(
        "step 1/5: warming up — confirming DeltaForge is streaming on mysql-a ..."
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
    info!("step 2/5: cutting MySQL proxy to tear down active connection ...");
    harness.toxi.disable("mysql").await?;
    // Brief pause so the existing TCP connection is closed before we swap the
    // upstream — prevents DeltaForge from racing into a half-switched proxy.
    sleep(Duration::from_secs(2)).await;

    info!("step 3/5: switching proxy upstream from mysql-a to mysql-b ...");
    harness
        .toxi
        .update_upstream("mysql", "mysql-b:3306")
        .await?;

    info!(
        "step 4/5: re-enabling proxy — DeltaForge will reconnect to mysql-b ..."
    );
    harness.toxi.enable("mysql").await?;

    // ── Verify ────────────────────────────────────────────────────────────────
    // DeltaForge should reconnect, run check_identity_post_reconnect, detect
    // the UUID change, call check_position_reachability (which returns Lost
    // because mysql-b has an empty gtid_executed), and halt.
    info!(
        "step 5/5: polling health endpoint for up to {}s — expecting unhealthy ...",
        UNHEALTHY_TIMEOUT.as_secs()
    );
    let deadline = Instant::now() + UNHEALTHY_TIMEOUT;
    let mut went_unhealthy = false;
    loop {
        let healthy = reqwest::get("http://localhost:8080/health")
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false);

        if !healthy {
            info!("DeltaForge is unhealthy — failover guard fired correctly");
            went_unhealthy = true;
            break;
        }
        if Instant::now() > deadline {
            break;
        }
        sleep(POLL_INTERVAL).await;
    }

    // ── Restore ───────────────────────────────────────────────────────────────
    // Always restore mysql proxy and restart DeltaForge so subsequent scenarios
    // start from a clean state.
    info!(
        "restoring mysql proxy upstream to mysql-a and restarting DeltaForge ..."
    );
    let _ = harness.toxi.update_upstream("mysql", "mysql:3306").await;
    let _ = harness.toxi.enable("mysql").await;
    let _ = docker::restart_service("app", "deltaforge").await;
    let _ = harness.wait_for_deltaforge(Duration::from_secs(30)).await;
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
         (position lost on new server)",
    ))
}

async fn insert_row() -> Result<()> {
    let pool = mysql_async::Pool::new(MYSQL_DSN);
    let mut conn = pool.get_conn().await?;
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    conn.exec_drop(
        "INSERT INTO customers (name, email, balance) VALUES (?, ?, ?)",
        (
            format!("failover-{ts}"),
            format!("failover-{ts}@test.com"),
            0.0_f64,
        ),
    )
    .await?;
    conn.disconnect().await?;
    let _ = pool.disconnect().await;
    Ok(())
}
