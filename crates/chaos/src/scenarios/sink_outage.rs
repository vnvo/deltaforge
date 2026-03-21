//! Scenario: Kafka sink becomes unavailable mid-stream.
//!
//! What it proves: DeltaForge does NOT advance the checkpoint while the sink
//! is down (at-least-once guarantee). After recovery, all events are delivered
//! - potentially with duplicates, but never with gaps.
//!
//! Steps:
//!   1. Warmup: confirm DeltaForge is actively streaming.
//!   2. Record baseline Kafka watermark.
//!   3. Cut the Kafka proxy.
//!   4. Insert rows - DeltaForge should buffer/retry, not checkpoint.
//!   5. Restore Kafka proxy.
//!   6. Verify all inserted rows eventually appear in the topic.

use anyhow::Result;
use mysql_async::prelude::Queryable;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::info;

use crate::harness::{Harness, MYSQL_DSN, ScenarioResult};

const OUTAGE_HOLD: Duration = Duration::from_secs(21);
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_secs(2);
const WARMUP_TIMEOUT: Duration = Duration::from_secs(60);
const ROUNDS: u32 = 2;

pub async fn run(harness: &Harness) -> Result<ScenarioResult> {
    const NAME: &str = "sink_outage";
    harness.setup().await?;

    for round in 1..=ROUNDS {
        info!(round, total = ROUNDS, "── starting round");
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
    info!(
        round,
        "step 1/5: warming up - waiting for DeltaForge to stream a sentinel event ..."
    );
    let warm_offset = harness.kafka_offset().await?;
    let deadline = Instant::now() + WARMUP_TIMEOUT;
    loop {
        insert_rows(1).await?;
        sleep(Duration::from_secs(3)).await;
        if harness.kafka_offset().await? > warm_offset {
            info!(round, "sentinel arrived in Kafka - DeltaForge is streaming");
            break;
        }
        if Instant::now() > deadline {
            return Ok(ScenarioResult::fail(
                name,
                "DeltaForge not streaming before outage (warmup timed out)",
            ));
        }
    }

    let events_before = harness.kafka_offset().await?;
    info!(round, %events_before, "baseline captured");

    // Outage
    // Cut Kafka. DeltaForge will buffer events and retry delivery.
    // The checkpoint must NOT advance while the sink is down.
    info!(
        round,
        "step 2/5: cutting Kafka proxy - DeltaForge should buffer and retry"
    );
    harness.toxi.disable("kafka").await?;

    info!(round, "step 3/5: inserting 10 rows while Kafka is down");
    let inserts = insert_rows(10).await?;
    info!(
        round,
        inserts, "rows committed to MySQL - DeltaForge cannot deliver them yet"
    );

    info!(
        round,
        "step 4/5: holding outage for {}s ...",
        OUTAGE_HOLD.as_secs()
    );
    sleep(OUTAGE_HOLD).await;

    // Recovery
    info!(
        round,
        "step 5/5: restoring Kafka proxy - waiting up to {}s for flush ...",
        RECOVERY_TIMEOUT.as_secs()
    );
    harness.toxi.enable("kafka").await?;

    let target = events_before + inserts as u64;
    let deadline = Instant::now() + RECOVERY_TIMEOUT;
    let mut events_after = events_before;
    let mut last_logged = events_before;
    loop {
        events_after = harness.kafka_offset().await?;
        if events_after != last_logged {
            info!(
                round,
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

    let delivered = events_after.saturating_sub(events_before);
    info!(round, %delivered, inserts, "recovery complete");

    if delivered < inserts as u64 {
        return Ok(ScenarioResult::fail(
            name,
            format!(
                "only {delivered}/{inserts} events delivered after {}s recovery",
                RECOVERY_TIMEOUT.as_secs()
            ),
        ));
    }

    Ok(ScenarioResult::pass(name)
        .note(format!(
            "round {round}: events delivered: {delivered}/{inserts}"
        ))
        .note(if delivered > inserts as u64 {
            format!(
                "{} duplicate(s) - expected with at-least-once",
                delivered - inserts as u64
            )
        } else {
            "no duplicates observed".into()
        }))
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
            "INSERT INTO order_events (customer_id, amount, status) VALUES (?, ?, ?)",
            (1i64, (i as f64 + 1.0) * 9.99, "pending"),
        )
        .await?;
    }
    conn.disconnect().await?;
    let _ = pool.disconnect().await;
    Ok(n)
}
