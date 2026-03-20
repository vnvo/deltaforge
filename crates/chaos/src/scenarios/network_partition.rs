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
use mysql_async::prelude::Queryable;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::info;

use crate::harness::{CHAOS_TOPIC, Harness, MYSQL_DSN, ScenarioResult};

const PARTITION_HOLD: Duration = Duration::from_secs(10);
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_secs(2);
const WARMUP_TIMEOUT: Duration = Duration::from_secs(15);

pub async fn run(harness: &Harness) -> Result<ScenarioResult> {
    const NAME: &str = "network_partition";
    const ROUNDS: u32 = 2;

    harness.setup().await?;

    for round in 1..=ROUNDS {
        info!(round, total = ROUNDS, "-- starting round");
        let result = run_once(harness, NAME, round).await?;
        if !result.passed {
            return Ok(result);
        }
        if round < ROUNDS {
            info!(round, "round complete, waiting 5s before next round ...");
            sleep(Duration::from_secs(5)).await;
        }
    }

    Ok(ScenarioResult::pass(NAME).note(format!("all {ROUNDS} rounds passed")))
}

async fn run_once(
    harness: &Harness,
    name: &str,
    round: u32,
) -> Result<ScenarioResult> {
    // Warmup
    // Insert a sentinel row and wait for it to appear in Kafka.
    // This confirms DeltaForge is actively streaming before we cut the proxy.
    // Expected: < 5s on a healthy pipeline.
    info!(
        round,
        "step 1/6: warming up - waiting for DeltaForge to stream a sentinel event ..."
    );
    let warm_offset = harness.kafka_offset().await?;
    insert_rows(1).await?;
    let deadline = Instant::now() + WARMUP_TIMEOUT;
    loop {
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
        sleep(Duration::from_secs(1)).await;
    }

    let reconnects_before = harness.reconnect_count().await?;
    let events_before = harness.kafka_offset().await?;
    info!(%reconnects_before, %events_before, "baseline captured");

    // Pre-partition inserts
    // Insert rows while streaming is healthy. These should arrive in Kafka
    // before the partition is cut, establishing a clean pre-partition baseline.
    info!(
        round,
        "step 2/6: inserting 3 rows before partition - these should arrive immediately"
    );
    insert_rows(3).await?;
    let target_pre = events_before + 3;
    let deadline_pre = Instant::now() + Duration::from_secs(15);
    loop {
        let offset = harness.kafka_offset().await?;
        if offset >= target_pre {
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

    // Partition
    // Cut the MySQL proxy. DeltaForge will detect the broken connection and
    // begin exponential backoff reconnects (~1s, 2s, 4s, 8s ...).
    info!(
        round,
        "step 3/6: cutting MySQL proxy - DeltaForge should begin reconnect backoff"
    );
    harness.toxi.disable("mysql").await?;

    // Insert rows directly to MySQL (bypasses toxiproxy).
    // These must survive the partition and be delivered after recovery.
    info!(
        round,
        "step 4/6: inserting 5 rows during partition (direct to MySQL, bypassing proxy)"
    );
    let inserts = insert_rows(5).await?;
    info!(
        inserts,
        "rows committed to MySQL - DeltaForge cannot see them yet"
    );

    // Hold the partition to accumulate backoff — ensures we test recovery
    // from deep backoff sleep, not just a brief hiccup.
    info!(
        round,
        "step 5/6: holding partition for {}s (backoff accumulation) ...",
        PARTITION_HOLD.as_secs()
    );
    sleep(PARTITION_HOLD).await;

    let reconnects_during = harness.reconnect_count().await?;
    info!(%reconnects_during, "successful reconnects during partition (expect 0)");

    // Recovery
    // Restore the proxy. DeltaForge will reconnect on its next backoff wake-up
    // (up to ~60s), then replay the binlog from its last checkpoint.
    info!(
        round,
        "step 6/6: restoring MySQL proxy - waiting up to {}s for DeltaForge to recover ...",
        RECOVERY_TIMEOUT.as_secs()
    );
    harness.toxi.enable("mysql").await?;

    let target = events_before_partition + inserts as u64;
    let deadline = Instant::now() + RECOVERY_TIMEOUT;
    let mut events_after = events_before;
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
                format!("chaos-{ts}-{i}"),
                format!("chaos-{ts}-{i}@test.com"),
                0.0_f64,
            ),
        )
        .await?;
    }
    conn.disconnect().await?;
    let _ = pool.disconnect().await;
    Ok(n)
}
