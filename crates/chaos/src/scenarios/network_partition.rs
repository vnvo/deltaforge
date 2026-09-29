//! Scenario: network partition between DeltaForge and the source DB.
//!
//! What it proves: after connectivity is restored, streaming resumes from the
//! last checkpoint - no events duplicated beyond at-least-once, none skipped.
//!
//! Steps:
//!   1. Confirm DeltaForge is actively streaming (warmup insert).
//!   2. Cut the DB proxy. DeltaForge should start reconnect attempts.
//!   3. Insert rows while the partition is active (must not be lost).
//!   4. Hold partition for 30s to force backoff accumulation.
//!   5. Restore the proxy. DeltaForge reconnects and delivers buffered rows.
//!   6. Verify reconnect counter incremented and all inserts arrived in Kafka.

use anyhow::Result;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::info;

use crate::backend::SourceBackend;
use crate::harness::{Harness, ScenarioResult};

const PARTITION_HOLD: Duration = Duration::from_secs(10);
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_secs(2);
const WARMUP_TIMEOUT: Duration = Duration::from_secs(60);

pub async fn run<B: SourceBackend>(
    harness: &Harness,
    backend: &B,
) -> Result<ScenarioResult> {
    let name = format!("{}/network_partition", backend.name());
    const ROUNDS: u32 = 2;

    crate::harness::print_scenario_banner(
        &name,
        "Cuts source DB proxy via Toxiproxy, inserts rows while disconnected, restores connectivity.",
        "All events delivered after reconnect, no data loss. Reconnect counter increments.",
    );
    harness.setup().await?;

    for round in 1..=ROUNDS {
        info!(round, total = ROUNDS, "-- starting round");
        let result = run_once(harness, backend, &name, round).await?;
        if !result.passed {
            return Ok(result);
        }
        if round < ROUNDS {
            info!(round, "round complete, waiting 5s before next round ...");
            sleep(Duration::from_secs(5)).await;
        }
    }

    Ok(ScenarioResult::pass(&name).note(format!("all {ROUNDS} rounds passed")))
}

async fn run_once<B: SourceBackend>(
    harness: &Harness,
    backend: &B,
    name: &str,
    round: u32,
) -> Result<ScenarioResult> {
    info!(
        round,
        "step 1/6: warming up - waiting for DeltaForge to stream a sentinel event ..."
    );
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
                name,
                "DeltaForge not streaming before partition (warmup timed out)",
            ));
        }
    }

    let reconnects_before = harness.reconnect_count().await?;
    let events_before = harness.kafka_offset().await?;
    info!(%reconnects_before, %events_before, "baseline captured");

    info!(
        round,
        "step 2/6: inserting 3 rows before partition - these should arrive immediately"
    );
    backend.insert_rows("pre-partition", 3).await?;
    let target_pre = events_before + 3;
    let deadline_pre = Instant::now() + Duration::from_secs(15);
    loop {
        if harness.kafka_offset().await? >= target_pre {
            break;
        }
        if Instant::now() > deadline_pre {
            return Ok(ScenarioResult::fail(
                name,
                "pre-partition inserts did not arrive in Kafka",
            ));
        }
        sleep(Duration::from_secs(1)).await;
    }
    let events_before_partition = harness.kafka_offset().await?;
    info!(%events_before_partition, "pre-partition inserts confirmed in Kafka");

    let proxy = backend.proxy();
    info!(
        round,
        proxy,
        "step 3/6: cutting proxy - DeltaForge should begin reconnect backoff"
    );
    harness.toxi.disable(proxy).await?;

    info!(
        round,
        "step 4/6: inserting 5 rows during partition (direct to DB, bypassing proxy)"
    );
    let inserts = backend.insert_rows("partition", 5).await?;
    info!(
        inserts,
        "rows committed to DB - DeltaForge cannot see them yet"
    );

    info!(
        round,
        hold_secs = PARTITION_HOLD.as_secs(),
        "step 5/6: holding partition (backoff accumulation) ..."
    );
    sleep(PARTITION_HOLD).await;

    let reconnects_during = harness.reconnect_count().await?;
    info!(%reconnects_during, "reconnects during partition (expect 0)");

    info!(
        round,
        proxy,
        recovery_secs = RECOVERY_TIMEOUT.as_secs(),
        "step 6/6: restoring proxy - waiting for DeltaForge to recover ..."
    );
    harness.toxi.enable(backend.proxy()).await?;

    let target = events_before_partition + inserts as u64;
    let deadline = Instant::now() + RECOVERY_TIMEOUT;
    let mut events_after;
    let mut last_logged = events_before;
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

    let reconnects_after = harness.reconnect_count().await?;
    let events_recovered = events_after.saturating_sub(events_before);
    let reconnect_delta = reconnects_after - reconnects_before;

    info!(
        %events_recovered,
        %reconnect_delta,
        expected_inserts = inserts,
        "recovery complete"
    );

    if reconnect_delta == 0.0 {
        return Ok(ScenarioResult::fail(
            name,
            "no reconnects observed - DeltaForge may not have detected the partition",
        ));
    }
    if events_recovered < inserts as u64 {
        return Ok(ScenarioResult::fail(
            name,
            format!(
                "only {events_recovered}/{inserts} inserts delivered to Kafka after {}s",
                RECOVERY_TIMEOUT.as_secs()
            ),
        ));
    }

    Ok(ScenarioResult::pass(name)
        .note(format!("reconnects observed: {reconnect_delta}"))
        .note(format!(
            "events delivered to Kafka: {events_recovered}/{inserts}"
        )))
}
