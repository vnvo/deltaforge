mod backend;
mod docker;
mod harness;
mod scenarios;
mod toxiproxy;

use anyhow::Result;
use backend::{MysqlBackend, PgBackend};
use clap::{Parser, ValueEnum};
use harness::Harness;
use tracing::info;

#[derive(Parser)]
#[command(name = "chaos", about = "DeltaForge chaos scenario runner")]
struct Cli {
    /// Scenario to run, or 'all' to run every scenario for the selected source.
    #[arg(short, long, default_value = "all")]
    scenario: Scenario,

    /// CDC source to test. Determines which DeltaForge instance and scenario
    /// set to use. `mysql` requires the `app` profile; `postgres` requires
    /// the `pg-app` profile.
    #[arg(long, default_value = "mysql")]
    source: Source,

    /// Seconds to wait for DeltaForge to become healthy before starting.
    #[arg(long, default_value_t = 30)]
    wait_secs: u64,

    /// Duration for the soak scenario in minutes (ignored for other scenarios).
    #[arg(long, default_value_t = 120)]
    duration_mins: u64,
}

#[derive(Clone, ValueEnum)]
enum Source {
    Mysql,
    Postgres,
}

#[derive(Clone, ValueEnum)]
enum Scenario {
    /// Run all scenarios for the selected source in sequence.
    All,
    // Generic (run for any source via --source)
    NetworkPartition,
    SinkOutage,
    CrashRecovery,
    SchemaDrift,
    // MySQL-specific
    Failover,
    BinlogPurge,
    // PostgreSQL-specific
    PgFailover,
    SlotDropped,
    /// Long-running endurance test with random fault injection (MySQL only).
    /// Requires the `soak` compose profile. Use --duration-mins to control length.
    Soak,
    // Heavy benchmark scenarios — not included in `all`.
    // Each prints a preamble explaining what it proves before running.
    /// TPC-C inspired endurance test: New-Order / Payment / Delivery transaction
    /// mix against a 9-table wholesale-supplier schema. Requires the `tpcc`
    /// compose profile. Use --duration-mins to control length.
    Tpcc,
    /// TPC-DI data integration benchmark — prints requirements and exits.
    /// Not yet implemented.
    TpcDi,
    /// TPC-E brokerage OLTP benchmark — prints requirements and exits.
    /// Not yet implemented.
    TpcE,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "chaos=info".parse().unwrap()),
        )
        .init();

    let cli = Cli::parse();
    let harness = Harness::new();

    // Soak/Tpcc/TpcDi/TpcE target separate instances or print-and-exit;
    // skip the default port-8080 health check for all of them.
    if !matches!(cli.scenario, Scenario::Soak | Scenario::Tpcc | Scenario::TpcDi | Scenario::TpcE) {
        info!("waiting for DeltaForge to be healthy...");
        harness
            .wait_for_deltaforge(std::time::Duration::from_secs(cli.wait_secs))
            .await?;
        info!("DeltaForge is healthy — starting chaos");
    }

    let results = match cli.source {
        Source::Mysql => run_mysql(&harness, &cli.scenario, cli.duration_mins).await?,
        Source::Postgres => run_postgres(&harness, &cli.scenario, cli.duration_mins).await?,
    };

    println!("\n═══════════════════════════════════");
    println!("  Chaos Run Results");
    println!("═══════════════════════════════════");
    for r in &results {
        r.print();
    }

    let failures = results.iter().filter(|r| !r.passed).count();
    println!(
        "\n{}/{} scenarios passed",
        results.len() - failures,
        results.len()
    );

    if failures > 0 {
        std::process::exit(1);
    }
    Ok(())
}

async fn run_mysql(
    harness: &Harness,
    scenario: &Scenario,
    duration_mins: u64,
) -> Result<Vec<harness::ScenarioResult>> {
    let backend = MysqlBackend;
    let mut results = vec![];

    match scenario {
        Scenario::NetworkPartition => {
            results.push(
                scenarios::network_partition::run(harness, &backend).await?,
            );
        }
        Scenario::SinkOutage => {
            results.push(scenarios::sink_outage::run(harness, &backend).await?);
        }
        Scenario::CrashRecovery => {
            results
                .push(scenarios::crash_recovery::run(harness, &backend).await?);
        }
        Scenario::SchemaDrift => {
            results
                .push(scenarios::schema_drift::run(harness, &backend).await?);
        }
        Scenario::Failover => {
            results.push(scenarios::failover::run(harness).await?);
        }
        Scenario::BinlogPurge => {
            results.push(scenarios::binlog_purge::run(harness).await?);
        }
        Scenario::Soak => {
            results.push(scenarios::soak::run(harness, duration_mins).await?);
        }
        Scenario::Tpcc => {
            results.push(scenarios::tpcc::run(harness, duration_mins).await?);
        }
        Scenario::TpcDi => {
            results.push(scenarios::tpc_di::run().await?);
        }
        Scenario::TpcE => {
            results.push(scenarios::tpc_e::run().await?);
        }
        Scenario::PgFailover | Scenario::SlotDropped => {
            eprintln!(
                "error: {:?} is a PostgreSQL-specific scenario — use --source postgres",
                scenario.to_possible_value().unwrap().get_name()
            );
            std::process::exit(2);
        }
        Scenario::All => {
            // Run in order of increasing destructiveness.
            // binlog_purge wipes MySQL GTID state and must always run last.
            results.push(
                scenarios::network_partition::run(harness, &backend).await?,
            );
            results.push(scenarios::sink_outage::run(harness, &backend).await?);
            results
                .push(scenarios::crash_recovery::run(harness, &backend).await?);
            results.push(scenarios::failover::run(harness).await?);
            results
                .push(scenarios::schema_drift::run(harness, &backend).await?);
            results.push(scenarios::binlog_purge::run(harness).await?);
        }
    }

    Ok(results)
}

async fn run_postgres(
    harness: &Harness,
    scenario: &Scenario,
    _duration_mins: u64,
) -> Result<Vec<harness::ScenarioResult>> {
    let backend = PgBackend;
    let mut results = vec![];

    match scenario {
        Scenario::NetworkPartition => {
            results.push(
                scenarios::network_partition::run(harness, &backend).await?,
            );
        }
        Scenario::SinkOutage => {
            results.push(scenarios::sink_outage::run(harness, &backend).await?);
        }
        Scenario::CrashRecovery => {
            results
                .push(scenarios::crash_recovery::run(harness, &backend).await?);
        }
        Scenario::SchemaDrift => {
            results
                .push(scenarios::schema_drift::run(harness, &backend).await?);
        }
        Scenario::PgFailover => {
            results.push(scenarios::pg_failover::run(harness).await?);
        }
        Scenario::SlotDropped => {
            results.push(scenarios::slot_dropped::run(harness).await?);
        }
        Scenario::Soak | Scenario::Tpcc | Scenario::Failover | Scenario::BinlogPurge => {
            eprintln!(
                "error: {:?} is a MySQL-specific scenario — use --source mysql",
                scenario.to_possible_value().unwrap().get_name()
            );
            std::process::exit(2);
        }
        Scenario::TpcDi => {
            results.push(scenarios::tpc_di::run().await?);
        }
        Scenario::TpcE => {
            results.push(scenarios::tpc_e::run().await?);
        }
        Scenario::All => {
            // slot_dropped must run last — it clears the checkpoint DB.
            results.push(
                scenarios::network_partition::run(harness, &backend).await?,
            );
            results.push(scenarios::sink_outage::run(harness, &backend).await?);
            results
                .push(scenarios::crash_recovery::run(harness, &backend).await?);
            results.push(scenarios::pg_failover::run(harness).await?);
            results
                .push(scenarios::schema_drift::run(harness, &backend).await?);
            results.push(scenarios::slot_dropped::run(harness).await?);
        }
    }

    Ok(results)
}
