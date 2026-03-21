mod harness;
mod scenarios;
mod toxiproxy;

use anyhow::Result;
use clap::{Parser, ValueEnum};
use harness::Harness;
use tracing::info;

#[derive(Parser)]
#[command(name = "chaos", about = "DeltaForge chaos scenario runner")]
struct Cli {
    /// Scenario to run, or 'all' to run every scenario in sequence.
    #[arg(short, long, default_value = "all")]
    scenario: Scenario,

    /// Seconds to wait for DeltaForge to become healthy before starting.
    #[arg(long, default_value_t = 30)]
    wait_secs: u64,
}

#[derive(Clone, ValueEnum)]
enum Scenario {
    All,
    NetworkPartition,
    SinkOutage,
    CrashRecovery,
    Failover,
    BinlogPurge,
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

    info!("waiting for DeltaForge to be healthy...");
    harness
        .wait_for_deltaforge(std::time::Duration::from_secs(cli.wait_secs))
        .await?;
    info!("DeltaForge is healthy — starting chaos");

    let results = match cli.scenario {
        Scenario::NetworkPartition => {
            vec![scenarios::network_partition::run(&harness).await?]
        }
        Scenario::SinkOutage => {
            vec![scenarios::sink_outage::run(&harness).await?]
        }
        Scenario::CrashRecovery => {
            vec![scenarios::crash_recovery::run(&harness).await?]
        }
        Scenario::BinlogPurge => {
            vec![scenarios::binlog_purge::run(&harness).await?]
        }
        Scenario::Failover => {
            vec![scenarios::failover::run(&harness).await?]
        }
        Scenario::All => {
            let mut results = vec![];
            // Run in order of increasing destructiveness.
            // Failover restarts DeltaForge mid-run; binlog_purge wipes MySQL
            // GTID state and must always run last.
            results.push(scenarios::network_partition::run(&harness).await?);
            results.push(scenarios::sink_outage::run(&harness).await?);
            results.push(scenarios::crash_recovery::run(&harness).await?);
            results.push(scenarios::failover::run(&harness).await?);
            results.push(scenarios::binlog_purge::run(&harness).await?);
            results
        }
    };

    println!("\n═══════════════════════════════════");
    println!("  Chaos Run Results");
    println!("═══════════════════════════════════");
    for r in &results {
        r.print();
    }

    let failures = results.iter().filter(|r| !r.passed).count();
    println!("\n{}/{} scenarios passed", results.len() - failures, results.len());

    if failures > 0 {
        std::process::exit(1);
    }
    Ok(())
}