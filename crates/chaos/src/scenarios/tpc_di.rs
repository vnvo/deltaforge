//! Scenario stub: TPC-DI (Data Integration benchmark).
//!
//! TPC-DI is the only TPC benchmark designed explicitly for data integration
//! pipelines. Unlike TPC-C/H/E it tests correctness of the pipeline itself,
//! not just raw throughput — making it the most direct proof of DeltaForge's
//! data integrity guarantees.
//!
//! This module prints requirements and exits. Implement when ready.

use crate::harness::ScenarioResult;
use anyhow::Result;

pub fn print_requirements() {
    println!();
    println!("═══════════════════════════════════════════════════════════════");
    println!("  TPC-DI — Data Integration Benchmark  [NOT YET IMPLEMENTED]");
    println!("═══════════════════════════════════════════════════════════════");
    println!();
    println!("  What this test proves for DeltaForge:");
    println!();
    println!("    Idempotent delivery");
    println!("      Source events are replayed after every fault. The sink");
    println!("      state must match the TPC-DI audit result set exactly —");
    println!("      no duplicates, no gaps, even with at-least-once delivery.");
    println!();
    println!("    Late-arriving and out-of-order events");
    println!("      TPC-DI injects batched file updates alongside the live");
    println!("      CDC stream. DeltaForge must reconcile both sources into");
    println!("      a consistent sink without dropping either.");
    println!();
    println!("    Schema evolution across pipeline versions");
    println!("      Two schema migration cycles are applied mid-run");
    println!("      (ADD COLUMN, MODIFY COLUMN type widening). The audit");
    println!("      queries must still pass after each migration.");
    println!();
    println!("    Incremental vs full-load consistency");
    println!("      After a forced re-seed of a dimension table, the");
    println!("      incremental CDC stream and a full snapshot must converge");
    println!("      to the same result.");
    println!();
    println!("  What is required to implement this test:");
    println!();
    println!("    • A staging database receiving batched file-based updates");
    println!("      alongside the live CDC stream (two DeltaForge pipelines).");
    println!();
    println!("    • The TPC-DI audit query set ported to Kafka consumers");
    println!("      or a downstream OLAP store (e.g. DuckDB) so correctness");
    println!("      can be asserted, not just event counts.");
    println!();
    println!(
        "    • A secondary pipeline sourcing from both MySQL and Postgres"
    );
    println!("      simultaneously to test cross-source fan-in ordering.");
    println!();
    println!("    • At least two full schema migration cycles injected while");
    println!("      the pipeline is running under load.");
    println!();
    println!("    • Estimated implementation effort: ~3 days");
    println!("      Estimated run time: 4-8 hours (audit queries are slow)");
    println!();
    println!("═══════════════════════════════════════════════════════════════");
    println!();
}

pub async fn run() -> Result<ScenarioResult> {
    print_requirements();
    Ok(ScenarioResult::fail(
        "tpc-di",
        "not yet implemented — see requirements above".to_string(),
    ))
}
