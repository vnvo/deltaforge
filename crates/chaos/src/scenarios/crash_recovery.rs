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
//! Requires `restart: on-failure` on the DeltaForge service in compose.

use anyhow::Result;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::info;

use crate::backend::SourceBackend;
use crate::docker;
use crate::harness::{Harness, ScenarioResult};

const WARMUP_TIMEOUT: Duration = Duration::from_secs(60);
const RESTART_TIMEOUT: Duration = Duration::from_secs(60);
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_secs(2);

pub async fn run<B: SourceBackend>(harness: &Harness, backend: &B) -> Result<ScenarioResult> {
    let name = format!("{}/crash_recovery", backend.name());
    harness.setup().await?;

    info!("step 1/5: warming up - waiting for DeltaForge to stream a sentinel event ...");
    let warm_offset = harness.kafka_offset().await?;
    let deadline = Instant::now() + WARMUP_TIMEOUT;
    loop {
        backend.insert_rows("warmup", 1).await?;
        sleep(Duration::from_secs(3)).await;
        if harness.kafka_offset().await? > warm_offset {
            info!("sentinel arrived in Kafka - DeltaForge is streaming");
            break;
        }
        if Instant::now() > deadline {
            return Ok(ScenarioResult::fail(
                &name,
                "DeltaForge not streaming before crash (warmup timed out)",
            ));
        }
    }

    info!("step 2/5: inserting 5 rows and waiting for checkpoint ...");
    let pre_crash_baseline = harness.kafka_offset().await?;
    backend.insert_rows("pre-crash", 5).await?;
    let target_pre = pre_crash_baseline + 5;
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if harness.kafka_offset().await? >= target_pre {
            break;
        }
        if Instant::now() > deadline {
            return Ok(ScenarioResult::fail(
                &name,
                "pre-crash inserts did not arrive before kill",
            ));
        }
        sleep(Duration::from_secs(1)).await;
    }
    let events_before_crash = harness.kafka_offset().await?;
    // Give DeltaForge a moment to checkpoint the delivered events.
    sleep(Duration::from_secs(2)).await;
    info!(%events_before_crash, "pre-crash inserts confirmed and checkpointed");

    info!("step 3/5: sending SIGKILL to DeltaForge container ...");
    docker::kill_service(backend.compose_profile(), backend.compose_service()).await?;
    docker::start_service(backend.compose_profile(), backend.compose_service()).await?;
    info!("DeltaForge killed and restart initiated - inserting rows while down");

    let post_inserts = backend.insert_rows("post-crash", 5).await?;
    info!(%post_inserts, "rows inserted while DeltaForge is down");

    info!("step 4/5: waiting for DeltaForge to become healthy ...");
    harness.wait_for_deltaforge(RESTART_TIMEOUT).await?;
    info!("DeltaForge is healthy");

    info!(
        recovery_secs = RECOVERY_TIMEOUT.as_secs(),
        "step 5/5: waiting for post-crash events to arrive ..."
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
    info!(%delivered, %post_inserts, "recovery complete");

    if delivered < post_inserts as u64 {
        return Ok(ScenarioResult::fail(
            &name,
            format!(
                "only {delivered}/{post_inserts} post-crash events delivered after {}s",
                RECOVERY_TIMEOUT.as_secs()
            ),
        ));
    }

    Ok(ScenarioResult::pass(&name)
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
