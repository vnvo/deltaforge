//! Scenario: table schema changes while DeltaForge is actively streaming.
//!
//! What it proves: DeltaForge receives the DDL event, reloads the column
//! definitions, and continues delivering row events without data loss or
//! pipeline failure (on_schema_drift=adapt).
//!
//! Steps:
//!   1. Warmup: confirm DeltaForge is streaming.
//!   2. ALTER TABLE customers ADD COLUMN notes TEXT — DDL mid-stream.
//!   3. Insert rows after the DDL (exercises the reloaded schema).
//!   4. Verify all post-DDL events arrive in Kafka within the deadline.
//!   5. Verify DeltaForge is still healthy (adapt, not halt).
//!   6. Restore: DROP COLUMN notes so the table is clean for subsequent runs.

use anyhow::Result;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::info;

use crate::backend::SourceBackend;
use crate::harness::{Harness, ScenarioResult};

const WARMUP_TIMEOUT: Duration = Duration::from_secs(60);
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_secs(2);
const POST_DDL_INSERTS: u64 = 5;

pub async fn run<B: SourceBackend>(harness: &Harness, backend: &B) -> Result<ScenarioResult> {
    let name = format!("{}/schema_drift", backend.name());
    harness.setup().await?;

    // ── Warmup ────────────────────────────────────────────────────────────────
    info!("step 1/5: warming up — confirming DeltaForge is streaming ...");
    let warm_offset = harness.kafka_offset().await?;
    let deadline = Instant::now() + WARMUP_TIMEOUT;
    loop {
        backend.schema_drift_insert(None).await?;
        sleep(Duration::from_secs(3)).await;
        if harness.kafka_offset().await? > warm_offset {
            info!("sentinel arrived in Kafka — DeltaForge is streaming");
            break;
        }
        if Instant::now() > deadline {
            return Ok(ScenarioResult::fail(
                &name,
                "DeltaForge not streaming before schema change (warmup timed out)",
            ));
        }
    }

    let pre_ddl_offset = harness.kafka_offset().await?;

    // ── DDL ───────────────────────────────────────────────────────────────────
    info!("step 2/5: ALTER TABLE customers ADD COLUMN notes TEXT — DDL mid-stream ...");
    backend.schema_drift_add().await?;

    // ── Post-DDL inserts ──────────────────────────────────────────────────────
    info!("step 3/5: inserting rows using the new schema ...");
    for i in 0..POST_DDL_INSERTS {
        backend
            .schema_drift_insert(Some(format!("note-{i}")))
            .await?;
    }
    let target_offset = pre_ddl_offset + POST_DDL_INSERTS;

    // ── Verify delivery ───────────────────────────────────────────────────────
    info!(
        delivery_secs = DELIVERY_TIMEOUT.as_secs(),
        "step 4/5: waiting for post-DDL events to arrive in Kafka ..."
    );
    let deadline = Instant::now() + DELIVERY_TIMEOUT;
    let mut delivered = false;
    loop {
        let offset = harness.kafka_offset().await?;
        if offset >= target_offset {
            info!(
                kafka_offset = offset,
                target = target_offset,
                "all post-DDL events delivered"
            );
            delivered = true;
            break;
        }
        if Instant::now() > deadline {
            let offset = harness.kafka_offset().await?;
            info!(
                kafka_offset = offset,
                target = target_offset,
                remaining = target_offset.saturating_sub(offset),
                "delivery timed out"
            );
            break;
        }
        sleep(POLL_INTERVAL).await;
    }

    // ── Health check ──────────────────────────────────────────────────────────
    info!("step 5/5: verifying DeltaForge is still healthy ...");
    let healthy = reqwest::get("http://localhost:8080/health")
        .await
        .map(|r| r.status().is_success())
        .unwrap_or(false);

    // ── Restore ───────────────────────────────────────────────────────────────
    info!("restoring schema: DROP COLUMN notes ...");
    let _ = backend.schema_drift_drop().await;

    if !delivered {
        return Ok(ScenarioResult::fail(
            &name,
            format!(
                "post-DDL events did not arrive within {}s — \
                 schema reload may not have fired",
                DELIVERY_TIMEOUT.as_secs()
            ),
        ));
    }

    if !healthy {
        return Ok(ScenarioResult::fail(
            &name,
            "DeltaForge became unhealthy after schema drift — \
             expected adapt, not halt",
        ));
    }

    Ok(ScenarioResult::pass(&name).note(
        "DeltaForge adapted to schema change without data loss or pipeline failure",
    ))
}
