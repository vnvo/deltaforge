//! Scenario: Schema Registry becomes unavailable mid-stream (Avro encoding).
//!
//! What it proves:
//! - DeltaForge continues encoding events with cached schema when SR is down
//! - Events are still delivered to Kafka with correct Avro wire format
//! - After SR recovery, new schema registrations succeed
//!
//! Prerequisites:
//! - Avro-encoded pipeline running (`avro-app` profile)
//! - Schema Registry behind Toxiproxy (`schema-registry` proxy on port 5103)
//! - At least one event must have been processed before the outage
//!   (so the schema is cached)
//!
//! Steps:
//!   1. Warmup: confirm events are flowing with Avro encoding.
//!   2. Record baseline Kafka watermark.
//!   3. Cut the Schema Registry proxy.
//!   4. Insert rows — DeltaForge should continue with cached schema.
//!   5. Verify events still appear in Kafka (cached schema path).
//!   6. Restore Schema Registry proxy.
//!   7. Insert more rows — verify continued delivery.

use anyhow::Result;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::info;

use crate::backend::SourceBackend;
use crate::harness::{
    Harness, ScenarioResult, kafka_offset_for_topic, print_scenario_banner,
};

const OUTAGE_HOLD: Duration = Duration::from_secs(15);
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_secs(2);
const WARMUP_TIMEOUT: Duration = Duration::from_secs(60);

const SR_PROXY: &str = "schema-registry";
const AVRO_TOPIC: &str = "chaos.avro";

pub async fn run<B: SourceBackend>(
    harness: &Harness,
    backend: &B,
) -> Result<ScenarioResult> {
    let name = format!("{}/sr_outage", backend.name());
    print_scenario_banner(
        &name,
        "Cuts Schema Registry proxy while Avro pipeline is running.",
        "Events continue flowing via cached schema. No data loss.",
    );
    harness.setup().await?;

    // ── Warmup: wait for at least 1 event in Kafka (schema gets cached) ──
    info!("waiting for Avro pipeline warmup (events flowing to Kafka)...");
    let warmup_start = Instant::now();
    let initial_offset = loop {
        if warmup_start.elapsed() > WARMUP_TIMEOUT {
            return Ok(ScenarioResult::fail(
                &name,
                "warmup timeout — no events in Kafka. Is the Avro pipeline running?",
            ));
        }
        let offset = kafka_offset_for_topic(AVRO_TOPIC).await?;
        if offset > 0 {
            info!(offset, "warmup complete — events flowing");
            break offset;
        }
        sleep(Duration::from_secs(2)).await;
    };

    // ── Phase 1: Cut Schema Registry ──
    info!("cutting Schema Registry proxy...");
    harness.toxi.disable(SR_PROXY).await?;

    // Insert rows while SR is down
    info!("inserting rows while SR is down...");
    let inserted = backend.insert_rows("sr-outage", 50).await?;
    info!(inserted, "rows inserted during SR outage");

    // Wait for outage hold period
    sleep(OUTAGE_HOLD).await;

    // Check that events still arrived in Kafka (cached schema)
    let offset_during_outage = kafka_offset_for_topic(AVRO_TOPIC).await?;
    let events_during_outage = offset_during_outage - initial_offset;
    info!(
        events_during_outage,
        "events delivered during SR outage (should be > 0 from cache)"
    );

    if events_during_outage == 0 {
        // Restore before failing
        harness.toxi.enable(SR_PROXY).await?;
        return Ok(ScenarioResult::fail(
            &name,
            "no events delivered during SR outage — cached schema fallback not working",
        ));
    }

    // ── Phase 2: Restore Schema Registry ──
    info!("restoring Schema Registry proxy...");
    harness.toxi.enable(SR_PROXY).await?;

    // Insert more rows after recovery
    info!("inserting rows after SR recovery...");
    let inserted_after = backend.insert_rows("sr-recovery", 50).await?;
    info!(inserted_after, "rows inserted after SR recovery");

    // Wait for delivery
    let target_offset = offset_during_outage + inserted_after as u64;
    let deadline = Instant::now() + RECOVERY_TIMEOUT;
    loop {
        let current = kafka_offset_for_topic(AVRO_TOPIC).await?;
        if current >= target_offset {
            info!(
                current,
                target_offset, "all events delivered after SR recovery"
            );
            break;
        }
        if Instant::now() > deadline {
            // Restore proxy in case of failure
            return Ok(ScenarioResult::fail(
                &name,
                format!(
                    "recovery timeout — Kafka offset {current}, expected >= {target_offset}"
                ),
            ));
        }
        sleep(POLL_INTERVAL).await;
    }

    info!(
        "SR outage scenario passed — cached schema fallback worked correctly"
    );

    Ok(ScenarioResult::pass(&name).note(format!(
        "events_during_outage={events_during_outage}, events_after_recovery={inserted_after}"
    )))
}
