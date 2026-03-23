//! Scenario stub: TPC-E (Online Transaction Processing — brokerage workload).
//!
//! TPC-E models a securities brokerage firm. Its 33-table schema and
//! financial transaction lifecycle stress DeltaForge's schema cache,
//! UPDATE throughput, and precision handling in ways TPC-C cannot.
//!
//! This module prints requirements and exits. Implement when ready.

use crate::harness::ScenarioResult;
use anyhow::Result;

pub fn print_requirements() {
    println!();
    println!("═══════════════════════════════════════════════════════════════");
    println!("  TPC-E — Brokerage OLTP Benchmark  [NOT YET IMPLEMENTED]");
    println!("═══════════════════════════════════════════════════════════════");
    println!();
    println!("  What this test proves for DeltaForge:");
    println!();
    println!("    Schema cache at scale");
    println!("      33 tables across customer, broker, market, and dimension");
    println!("      domains. With short cache TTLs and high table count the");
    println!("      schema cache eviction policy is exercised continuously.");
    println!();
    println!("    High-frequency UPDATE streams");
    println!("      Market data rows (LAST_TRADE, SECURITY) are updated at");
    println!("      rates that can exceed 10k events/s on a single table.");
    println!("      Tests sustained UPDATE throughput and back-pressure.");
    println!();
    println!("    Multi-step trade lifecycle");
    println!("      A single Trade-Order spans TRADE → TRADE_HISTORY →");
    println!("      HOLDING_SUMMARY → BROKER_ACCOUNT → CUSTOMER_ACCOUNT");
    println!("      across 5+ tables per commit. CDC ordering must preserve");
    println!("      the full causal chain end-to-end.");
    println!();
    println!("    Financial decimal precision");
    println!("      DECIMAL(12,2) balances and DATETIME(6) microsecond");
    println!("      timestamps must pass through the CDC pipeline without");
    println!("      any rounding, truncation, or timezone shifts.");
    println!();
    println!("  What is required to implement this test:");
    println!();
    println!("    • Full 33-table TPC-E schema with FK constraints and");
    println!("      appropriate indexes (schema DDL is ~600 lines).");
    println!();
    println!("    • A market data feed simulator that generates continuous");
    println!("      LAST_TRADE and SECURITY price updates (ticker loop).");
    println!();
    println!("    • Seed data generator for broker, customer, account, and");
    println!("      security reference data (~500 MB at scale factor 1).");
    println!();
    println!("    • Trade lifecycle state machine across 5 statuses");
    println!("      (submitted → pending → sent → completed → settled)");
    println!("      with proper rollback handling for rejected trades.");
    println!();
    println!("    • Estimated implementation effort: ~5 days");
    println!("      Estimated run time: 2-4 hours per scale factor");
    println!();
    println!("═══════════════════════════════════════════════════════════════");
    println!();
}

pub async fn run() -> Result<ScenarioResult> {
    print_requirements();
    Ok(ScenarioResult::fail(
        "tpc-e",
        "not yet implemented — see requirements above".to_string(),
    ))
}
