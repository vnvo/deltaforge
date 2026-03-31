mod backend;
mod docker;
mod harness;
mod scenarios;
mod toxiproxy;
mod ui;

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

    /// Number of concurrent writer tasks for the soak scenario (ignored for other scenarios).
    /// Higher values increase write throughput. Default: 16.
    #[arg(long, default_value_t = 16)]
    writer_tasks: usize,

    /// Maximum per-task sleep between writes in milliseconds for the soak scenario.
    /// Lower values increase write throughput. Default: 30.
    #[arg(long, default_value_t = 30)]
    write_delay_ms: u64,

    // ── Backlog-drain throughput knobs ────────────────────────────────────────
    /// Total rows to insert for the backlog-drain benchmark. Default: 1M.
    #[arg(long, default_value_t = 1_000_000)]
    drain_target: u64,

    /// Concurrent writer tasks during the backlog population phase.
    #[arg(long, default_value_t = 32)]
    drain_writers: usize,

    /// Max seconds to wait for the drain to complete. 0 = auto-scale based on target.
    #[arg(long, default_value_t = 0)]
    drain_timeout: u64,

    /// Max events per Kafka batch during the backlog-drain (pipeline spec batch.max_events).
    /// Higher values reduce produce calls per second.
    #[arg(long, default_value_t = 4000)]
    drain_max_events: u64,

    /// Max batch size in bytes. 0 = leave unchanged (default 3MB).
    /// Increase alongside max_events to prevent the byte limit from capping batch size.
    #[arg(long, default_value_t = 0)]
    drain_max_bytes: u64,

    /// Max batch age in ms during the backlog-drain (pipeline spec batch.max_ms).
    /// Lower values reduce latency at the cost of smaller batches.
    #[arg(long, default_value_t = 100)]
    drain_max_ms: u64,

    /// How many batches may be in-flight concurrently during the drain.
    /// Higher values overlap accumulation with delivery for better throughput.
    #[arg(long, default_value_t = 4)]
    drain_max_inflight: u64,

    /// Commit mode during the backlog-drain: "required" (safe) or "periodic" (faster).
    #[arg(long, default_value = "required")]
    drain_commit_mode: String,

    /// Checkpoint interval in ms — only used when --drain-commit-mode=periodic. Default: 500.
    #[arg(long, default_value_t = 500)]
    drain_commit_interval_ms: u64,

    /// Enable schema sensing during the backlog-drain.
    /// Disabling sensing (the default) greatly improves replay throughput.
    #[arg(long, default_value_t = false)]
    drain_schema_sensing: bool,

    /// rdkafka producer config overrides for the backlog-drain, as key=value pairs.
    /// Useful knobs: linger.ms, batch.size, batch.num.messages, compression.type.
    /// Example: --drain-kafka-conf linger.ms=20 --drain-kafka-conf batch.size=1048576
    #[arg(long = "drain-kafka-conf", value_name = "KEY=VALUE")]
    drain_kafka_conf: Vec<String>,

    /// Enable or disable exactly-once (transactional) Kafka delivery.
    /// Omit to leave the pipeline's current setting unchanged.
    /// Use --exactly-once=true to enable, --exactly-once=false to disable.
    #[arg(long)]
    exactly_once: Option<bool>,

    /// Bypass Toxiproxy: PATCH pipelines to connect directly to source/sink
    /// before running the scenario. Restores proxied addresses after completion.
    #[arg(long, default_value_t = false)]
    no_proxy: bool,
}

#[derive(Clone, ValueEnum)]
enum Source {
    Mysql,
    Postgres,
}

#[derive(Clone, ValueEnum)]
enum Scenario {
    /// Launch the web UI playground on localhost:7474.
    Ui,
    /// Run all scenarios for the selected source in sequence.
    All,
    // Generic (run for any source via --source)
    NetworkPartition,
    SinkOutage,
    CrashRecovery,
    SchemaDrift,
    /// Exactly-once crash recovery: verifies no partial batches are visible
    /// to a `read_committed` consumer after a crash. Requires `exactly_once: true`
    /// on the Kafka sink in the DeltaForge config.
    ExactlyOnce,
    /// DLQ validation: verifies REST API endpoints, pipeline continues after
    /// DLQ operations.
    DlqPoison,
    // MySQL-specific
    Failover,
    BinlogPurge,
    // PostgreSQL-specific
    PgFailover,
    SlotDropped,
    /// Long-running endurance test with random fault injection (MySQL only).
    /// Requires the `soak` compose profile. Use --duration-mins to control length.
    Soak,
    /// Steady-state baseline: same as Soak but with no fault injection.
    /// Writers and ALTER TABLE operations run for the full duration.
    /// Use to measure cache hit ratio, latency, and resource usage without chaos.
    SoakStable,
    // Heavy benchmark scenarios — not included in `all`.
    // Each prints a preamble explaining what it proves before running.
    /// TPC-C inspired endurance test: New-Order / Payment / Delivery transaction
    /// mix against a 9-table wholesale-supplier schema. Requires the `tpcc`
    /// compose profile. Use --duration-mins to control length.
    Tpcc,
    /// Binlog backlog drain benchmark: writes 1 M rows to MySQL while DeltaForge
    /// is stopped, then resumes and measures catch-up throughput. Requires the
    /// `soak` compose profile.
    BacklogDrain,
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

    // UI mode: start the web playground and exit.
    if matches!(cli.scenario, Scenario::Ui) {
        return ui::run(7474).await;
    }

    // Soak/Tpcc/TpcDi/TpcE target separate instances or print-and-exit;
    // skip the default port-8080 health check for all of them.
    if !matches!(
        cli.scenario,
        Scenario::Soak
            | Scenario::SoakStable
            | Scenario::BacklogDrain
            | Scenario::Tpcc
            | Scenario::TpcDi
            | Scenario::TpcE
    ) {
        info!("waiting for DeltaForge to be healthy...");
        harness
            .wait_for_deltaforge(std::time::Duration::from_secs(cli.wait_secs))
            .await?;
        info!("DeltaForge is healthy — starting chaos");
    }

    let kafka_client_conf: std::collections::HashMap<String, String> = cli
        .drain_kafka_conf
        .iter()
        .filter_map(|s| {
            let (k, v) = s.split_once('=')?;
            Some((k.trim().to_string(), v.trim().to_string()))
        })
        .collect();

    let drain_cfg = scenarios::backlog_drain::DrainConfig {
        target_events: cli.drain_target,
        writer_tasks: cli.drain_writers,
        drain_timeout_secs: cli.drain_timeout,
        max_events: cli.drain_max_events,
        max_ms: cli.drain_max_ms,
        max_inflight: cli.drain_max_inflight,
        commit_mode: cli.drain_commit_mode.clone(),
        commit_interval_ms: cli.drain_commit_interval_ms,
        schema_sensing: cli.drain_schema_sensing,
        max_bytes: cli.drain_max_bytes,
        kafka_client_conf,
        exactly_once: cli.exactly_once,
    };

    // Apply proxy bypass if requested.
    if cli.no_proxy {
        harness::set_proxy_bypass(true).await;
    }

    let results = match cli.source {
        Source::Mysql => {
            run_mysql(
                &harness,
                &cli.scenario,
                cli.duration_mins,
                cli.writer_tasks,
                cli.write_delay_ms,
                drain_cfg,
            )
            .await?
        }
        Source::Postgres => {
            run_postgres(
                &harness,
                &cli.scenario,
                cli.duration_mins,
                cli.writer_tasks,
                cli.write_delay_ms,
                drain_cfg,
            )
            .await?
        }
    };

    // Restore proxied connections after scenario completes.
    if cli.no_proxy {
        harness::set_proxy_bypass(false).await;
    }

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
    writer_tasks: usize,
    write_delay_ms: u64,
    drain_cfg: scenarios::backlog_drain::DrainConfig,
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
        Scenario::ExactlyOnce => {
            results
                .push(scenarios::exactly_once::run(harness, &backend).await?);
        }
        Scenario::DlqPoison => {
            results
                .push(scenarios::dlq_poison::run(harness, &backend).await?);
        }
        Scenario::Failover => {
            results.push(scenarios::failover::run(harness).await?);
        }
        Scenario::BinlogPurge => {
            results.push(scenarios::binlog_purge::run(harness).await?);
        }
        Scenario::Soak => {
            results.push(
                scenarios::soak::run(
                    harness,
                    duration_mins,
                    writer_tasks,
                    write_delay_ms,
                )
                .await?,
            );
        }
        Scenario::SoakStable => {
            results.push(
                scenarios::soak::run_stable(
                    harness,
                    duration_mins,
                    writer_tasks,
                    write_delay_ms,
                )
                .await?,
            );
        }
        Scenario::Tpcc => {
            results.push(scenarios::tpcc::run(harness, duration_mins).await?);
        }
        Scenario::BacklogDrain => {
            results
                .push(scenarios::backlog_drain::run(harness, drain_cfg).await?);
        }
        Scenario::TpcDi => {
            results.push(scenarios::tpc_di::run().await?);
        }
        Scenario::TpcE => {
            results.push(scenarios::tpc_e::run().await?);
        }
        Scenario::Ui => unreachable!("ui is handled before source dispatch"),
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
    duration_mins: u64,
    writer_tasks: usize,
    write_delay_ms: u64,
    drain_cfg: scenarios::backlog_drain::DrainConfig,
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
        Scenario::ExactlyOnce => {
            results
                .push(scenarios::exactly_once::run(harness, &backend).await?);
        }
        Scenario::DlqPoison => {
            results
                .push(scenarios::dlq_poison::run(harness, &backend).await?);
        }
        Scenario::PgFailover => {
            results.push(scenarios::pg_failover::run(harness).await?);
        }
        Scenario::SlotDropped => {
            results.push(scenarios::slot_dropped::run(harness).await?);
        }
        Scenario::Soak => {
            results.push(
                scenarios::soak::run_pg(
                    harness,
                    duration_mins,
                    writer_tasks,
                    write_delay_ms,
                )
                .await?,
            );
        }
        Scenario::SoakStable => {
            results.push(
                scenarios::soak::run_stable_pg(
                    harness,
                    duration_mins,
                    writer_tasks,
                    write_delay_ms,
                )
                .await?,
            );
        }
        Scenario::BacklogDrain => {
            results.push(
                scenarios::backlog_drain::run_pg(harness, drain_cfg).await?,
            );
        }
        Scenario::Ui => unreachable!("ui is handled before source dispatch"),
        Scenario::Tpcc | Scenario::Failover | Scenario::BinlogPurge => {
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
