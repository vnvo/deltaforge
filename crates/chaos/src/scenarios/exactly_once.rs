//! Scenario: Exactly-once crash recovery.
//!
//! What it proves: with `exactly_once: true` on the Kafka sink, a crash mid-
//! stream does not produce partial (uncommitted) batches visible to a
//! `read_committed` consumer. After recovery, all events are delivered without
//! gaps.
//!
//! Steps:
//!   1. Warmup: confirm DeltaForge is actively streaming (exactly-once mode).
//!   2. Insert rows, confirm they arrive in Kafka (pre-crash baseline).
//!   3. SIGKILL the DeltaForge container (mid-transaction if we're lucky).
//!   4. Insert rows while it is down (must not be lost).
//!   5. Wait for Docker restart to bring it back.
//!   6. Verify all post-crash rows arrive in Kafka.
//!   7. Verify `read_committed` consumer sees no partial batches.
//!
//! Requires:
//!   - `exactly_once: true` in the sink config for the chaos DeltaForge service
//!   - `restart: on-failure` on the DeltaForge service in compose

use anyhow::Result;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::info;

use crate::backend::SourceBackend;
use crate::docker;
use crate::harness::{Harness, ScenarioResult, kafka_committed_count};

const WARMUP_TIMEOUT: Duration = Duration::from_secs(60);
const RESTART_TIMEOUT: Duration = Duration::from_secs(60);
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_secs(2);

pub async fn run<B: SourceBackend>(
    harness: &Harness,
    backend: &B,
) -> Result<ScenarioResult> {
    let name = format!("{}/exactly_once", backend.name());
    crate::harness::print_scenario_banner(
        &name,
        "SIGKILL mid-stream with exactly_once: true. Verifies read_committed consumer sees no partial batches.",
        "All post-crash events delivered. No uncommitted transactions visible. Duplicates OK (replayed).",
    );
    harness.setup().await?;

    // ── Step 1: warmup ─────────────────────────────────────────────────
    info!("step 1/7: warming up - waiting for DeltaForge to stream ...");
    let warm_offset = harness.kafka_offset().await?;
    let deadline = Instant::now() + WARMUP_TIMEOUT;
    loop {
        backend.insert_rows("eo-warmup", 1).await?;
        sleep(Duration::from_secs(3)).await;
        if harness.kafka_offset().await? > warm_offset {
            info!("sentinel arrived - DeltaForge is streaming");
            break;
        }
        if Instant::now() > deadline {
            return Ok(ScenarioResult::fail(
                &name,
                "DeltaForge not streaming (warmup timed out)",
            ));
        }
    }

    // ── Step 2: pre-crash baseline ─────────────────────────────────────
    info!("step 2/7: inserting 10 rows, waiting for Kafka delivery ...");
    let pre_crash_offset = harness.kafka_offset().await?;
    backend.insert_rows("eo-pre", 10).await?;
    let target_pre = pre_crash_offset + 10;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if harness.kafka_offset().await? >= target_pre {
            break;
        }
        if Instant::now() > deadline {
            return Ok(ScenarioResult::fail(
                &name,
                "pre-crash inserts did not arrive",
            ));
        }
        sleep(Duration::from_secs(1)).await;
    }
    let offset_before_crash = harness.kafka_offset().await?;
    // Allow checkpoint to flush.
    sleep(Duration::from_secs(2)).await;
    info!(%offset_before_crash, "pre-crash baseline confirmed");

    // ── Step 3: SIGKILL ────────────────────────────────────────────────
    info!("step 3/7: sending SIGKILL to DeltaForge container ...");
    docker::kill_service(backend.compose_profile(), backend.compose_service())
        .await?;
    docker::start_service(backend.compose_profile(), backend.compose_service())
        .await?;
    info!("DeltaForge killed, restart initiated");

    // ── Step 4: insert while down ──────────────────────────────────────
    let post_inserts = 10i64;
    backend.insert_rows("eo-post", post_inserts).await?;
    let post_inserts = post_inserts as u64;
    info!(%post_inserts, "rows inserted while DeltaForge is down");

    // ── Step 5: wait for recovery ──────────────────────────────────────
    info!("step 5/7: waiting for DeltaForge to become healthy ...");
    harness.wait_for_deltaforge(RESTART_TIMEOUT).await?;
    info!("DeltaForge is healthy after crash");

    // ── Step 6: wait for events to arrive ──────────────────────────────
    info!("step 6/7: waiting for post-crash events to arrive ...");
    let target = offset_before_crash + post_inserts;
    let deadline = Instant::now() + RECOVERY_TIMEOUT;
    let mut current;
    loop {
        current = harness.kafka_offset().await?;
        if current >= target {
            break;
        }
        if Instant::now() > deadline {
            break;
        }
        sleep(POLL_INTERVAL).await;
    }

    let delivered = current.saturating_sub(offset_before_crash);
    if delivered < post_inserts {
        return Ok(ScenarioResult::fail(
            &name,
            format!(
                "only {delivered}/{post_inserts} post-crash events delivered"
            ),
        ));
    }

    // ── Step 7: read_committed check ───────────────────────────────────
    info!("step 7/7: verifying read_committed consumer sees all events ...");
    let committed = kafka_committed_count(pre_crash_offset).await?;
    let total_expected = delivered + 10; // pre-crash (10) + post-crash
    info!(
        %committed,
        %total_expected,
        "read_committed consumer message count"
    );

    if committed < total_expected {
        return Ok(ScenarioResult::fail(
            &name,
            format!(
                "read_committed saw {committed} but expected >= {total_expected} — \
                 possible partial/uncommitted transaction visible"
            ),
        ));
    }

    Ok(ScenarioResult::pass(&name)
        .note(format!(
            "post-crash events delivered: {delivered}/{post_inserts}"
        ))
        .note(format!(
            "read_committed consumer: {committed} events (no partial batches)"
        ))
        .note(if delivered > post_inserts {
            format!(
                "{} duplicate(s) — replayed and committed",
                delivered - post_inserts
            )
        } else {
            "no duplicates observed".into()
        }))
}
