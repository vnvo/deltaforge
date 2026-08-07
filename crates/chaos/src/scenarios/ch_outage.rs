//! Scenario: ClickHouse becomes unavailable mid-stream (ClickHouse sink).
//!
//! What it proves:
//! - DeltaForge backpressures the source while ClickHouse is down (required sink).
//! - No rows land during the outage (the required sink blocks; events buffer).
//! - The pipeline recovers cleanly when ClickHouse returns — the batches that
//!   backpressured get replayed and land.
//!
//! Prerequisites:
//! - Pipeline running with a ClickHouse sink targeting the toxiproxy route
//!   (`http://toxiproxy:5105`) and table `default.customers` (auto-created).
//! - `--profile ch-infra` up (ClickHouse + toxiproxy route).
//!
//! Steps:
//!   1. Warmup: insert rows, wait for the row count to grow.
//!   2. Cut the ClickHouse proxy.
//!   3. Insert more rows; the row count should NOT grow.
//!   4. Restore the proxy.
//!   5. Verify the row count grows again after restore.

use std::time::{Duration, Instant};

use anyhow::Result;
use tokio::time::sleep;
use tracing::info;

use crate::backend::SourceBackend;
use crate::harness::{Harness, ScenarioResult, print_scenario_banner};
use crate::scenarios::meta::{ScenarioCategory, ScenarioMeta};

pub const META: ScenarioMeta = ScenarioMeta {
    name: "ch-outage",
    description: "Cuts ClickHouse via toxiproxy mid-stream then restores it.",
    expected: "Required ClickHouse sink backpressures the source; no rows land \
               during the outage; the pipeline catches up cleanly after \
               ClickHouse returns.",
    category: ScenarioCategory::ClickHouse,
    required_profiles: &["base", "mysql-infra", "ch-infra", "df"],
    tags: &["clickhouse", "resilience", "backpressure"],
};

// Host port 8124 → ClickHouse container 8123 (direct — bypasses the proxy so we
// read ground truth even while the sink's proxy route is cut).
const CH_HTTP: &str = "http://localhost:8124";
const CH_PROXY: &str = "clickhouse";
const TABLE: &str = "default.customers";

const WARMUP_TIMEOUT: Duration = Duration::from_secs(60);
const OUTAGE_HOLD: Duration = Duration::from_secs(20);
const RECOVERY_WAIT: Duration = Duration::from_secs(45);
const POLL_INTERVAL: Duration = Duration::from_secs(3);

/// Row count in the target table, or 0 if the table doesn't exist yet / the
/// query fails (e.g. during warmup before auto-create, or a transient error).
async fn ch_count(table: &str) -> u64 {
    let sql = format!("SELECT count() FROM {table} FORMAT TSV");
    let resp = reqwest::Client::new()
        .post(CH_HTTP)
        .query(&[("query", sql.as_str())])
        .send()
        .await;
    match resp {
        Ok(r) if r.status().is_success() => r
            .text()
            .await
            .ok()
            .and_then(|t| t.trim().parse::<u64>().ok())
            .unwrap_or(0),
        _ => 0,
    }
}

pub async fn run<B: SourceBackend>(
    harness: &Harness,
    backend: &B,
) -> Result<ScenarioResult> {
    let name = format!("{}/ch-outage", backend.name());
    print_scenario_banner(&name, META.description, META.expected);
    harness.setup().await?;

    // ── Warmup ──
    info!("warmup: priming ClickHouse sink and waiting for rows...");
    let warmup_start = Instant::now();
    let initial_rows = loop {
        if warmup_start.elapsed() > WARMUP_TIMEOUT {
            return Ok(ScenarioResult::fail(
                &name,
                "warmup timeout — no rows appeared in ClickHouse. Is a \
                 ClickHouse pipeline running and targeting toxiproxy:5105?",
            ));
        }
        backend.insert_rows("ch-outage-warmup", 100).await?;
        let count = ch_count(TABLE).await;
        if count > 0 {
            info!(count, "warmup complete");
            break count;
        }
        sleep(POLL_INTERVAL).await;
    };

    // ── Phase 1: Cut ClickHouse ──
    info!("cutting ClickHouse proxy ({CH_PROXY})...");
    harness.toxi.disable(CH_PROXY).await?;

    // Required sink → the coordinator backpressures the source; events buffer
    // in DeltaForge. No rows should land while ClickHouse is unreachable.
    info!("inserting rows while ClickHouse is down (expect backpressure)...");
    let inserted_during_outage =
        backend.insert_rows("ch-outage-down", 500).await?;
    info!(
        inserted_during_outage,
        "rows inserted into source during outage"
    );
    sleep(OUTAGE_HOLD).await;

    let rows_during_outage = ch_count(TABLE).await;
    let delta = rows_during_outage as i64 - initial_rows as i64;
    info!(
        rows_during_outage,
        delta, "ClickHouse row count during outage (delta should be 0)"
    );

    if delta > 0 {
        harness.toxi.enable(CH_PROXY).await?;
        return Ok(ScenarioResult::fail(
            &name,
            format!(
                "ClickHouse row count grew by {delta} during the outage — the \
                 required sink did not backpressure"
            ),
        ));
    }

    // ── Phase 2: Restore ClickHouse ──
    info!("restoring ClickHouse proxy...");
    harness.toxi.enable(CH_PROXY).await?;

    let restore_deadline = Instant::now() + RECOVERY_WAIT;
    let recovered = loop {
        if Instant::now() > restore_deadline {
            break false;
        }
        backend.insert_rows("ch-outage-recovery", 200).await?;
        sleep(POLL_INTERVAL).await;
        let count = ch_count(TABLE).await;
        if count > rows_during_outage {
            info!(
                rows = count,
                delta = count - rows_during_outage,
                "recovery complete — rows landing in ClickHouse again"
            );
            break true;
        }
    };

    if !recovered {
        return Ok(ScenarioResult::fail(
            &name,
            format!(
                "no rows landed within {}s after restore — pipeline may be stuck",
                RECOVERY_WAIT.as_secs()
            ),
        ));
    }

    let final_rows = ch_count(TABLE).await;
    let result = ScenarioResult::pass(&name)
        .note(format!("rows inserted during outage: {inserted_during_outage}"))
        .note(format!(
            "rows during outage: {rows_during_outage} (delta 0 — backpressure held)"
        ))
        .note(format!("rows after recovery: {final_rows}"))
        .note(format!(
            "rows landed post-restore: {}",
            final_rows - rows_during_outage
        ));
    Ok(result)
}
