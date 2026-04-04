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
use scenarios::soak::SoakSource;
use tracing::info;

#[derive(Parser)]
#[command(name = "chaos", about = "DeltaForge chaos scenario runner")]
struct Cli {
    /// Scenario to run, or 'all' to run every scenario for the selected source.
    #[arg(short, long, default_value = "all")]
    scenario: Scenario,

    /// CDC source to test.
    #[arg(long, default_value = "mysql")]
    source: Source,

    /// DeltaForge REST API port to target. 8080=release, 8081=debug, 8082=profile.
    #[arg(long, default_value_t = 8080)]
    port: u16,

    /// Pipeline name on the target instance. Auto-detected if omitted.
    #[arg(long)]
    pipeline: Option<String>,

    /// Kafka topic for soak/drain scenarios. Default: chaos.cdc.
    #[arg(long, default_value = "chaos.cdc")]
    topic: String,

    /// Seconds to wait for DeltaForge to become healthy before starting.
    #[arg(long, default_value_t = 30)]
    wait_secs: u64,

    /// Duration for the soak scenario in minutes.
    #[arg(long, default_value_t = 120)]
    duration_mins: u64,

    /// Number of concurrent writer tasks for the soak scenario.
    #[arg(long, default_value_t = 16)]
    writer_tasks: usize,

    /// Maximum per-task sleep between writes in milliseconds for the soak scenario.
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

    /// Max events per Kafka batch during the backlog-drain.
    #[arg(long, default_value_t = 4000)]
    drain_max_events: u64,

    /// Max batch size in bytes. 0 = leave unchanged.
    #[arg(long, default_value_t = 0)]
    drain_max_bytes: u64,

    /// Max batch age in ms during the backlog-drain.
    #[arg(long, default_value_t = 100)]
    drain_max_ms: u64,

    /// How many batches may be in-flight concurrently during the drain.
    #[arg(long, default_value_t = 4)]
    drain_max_inflight: u64,

    /// Commit mode during the backlog-drain: "required" or "periodic".
    #[arg(long, default_value = "required")]
    drain_commit_mode: String,

    /// Checkpoint interval in ms — only used when --drain-commit-mode=periodic.
    #[arg(long, default_value_t = 500)]
    drain_commit_interval_ms: u64,

    /// Enable schema sensing during the backlog-drain.
    #[arg(long, default_value_t = false)]
    drain_schema_sensing: bool,

    /// rdkafka producer config overrides for the backlog-drain, as key=value pairs.
    #[arg(long = "drain-kafka-conf", value_name = "KEY=VALUE")]
    drain_kafka_conf: Vec<String>,

    /// Enable or disable exactly-once (transactional) Kafka delivery.
    #[arg(long)]
    exactly_once: Option<bool>,

    /// Bypass Toxiproxy: PATCH pipelines to connect directly to source/sink.
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
    /// Exactly-once crash recovery.
    ExactlyOnce,
    /// DLQ validation.
    DlqPoison,
    /// Schema Registry outage (Avro encoding).
    SrOutage,
    // MySQL-specific
    Failover,
    BinlogPurge,
    // PostgreSQL-specific
    PgFailover,
    SlotDropped,
    /// Long-running endurance test with random fault injection.
    Soak,
    /// Steady-state baseline (no fault injection).
    SoakStable,
    /// TPC-C benchmark.
    Tpcc,
    /// Backlog drain benchmark (1M row catch-up throughput).
    BacklogDrain,
    /// TPC-DI (prints requirements and exits).
    TpcDi,
    /// TPC-E (prints requirements and exits).
    TpcE,
}

/// Map a DeltaForge API port to its docker-compose service name.
fn service_for_port(port: u16) -> &'static str {
    match port {
        8080 => "deltaforge-release",
        8081 => "deltaforge-debug",
        8082 => "deltaforge-profile",
        _ => "deltaforge-debug",
    }
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
    let harness = Harness::new(cli.port);

    // UI mode: start the web playground and exit.
    if matches!(cli.scenario, Scenario::Ui) {
        return ui::run(7474).await;
    }

    // Soak/Tpcc/TpcDi/TpcE have their own health checks —
    // skip the default health check for them.
    if !matches!(
        cli.scenario,
        Scenario::Soak
            | Scenario::SoakStable
            | Scenario::BacklogDrain
            | Scenario::Tpcc
            | Scenario::TpcDi
            | Scenario::TpcE
    ) {
        info!(port = cli.port, "waiting for DeltaForge to be healthy...");
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

    let source_name = match cli.source {
        Source::Mysql => "mysql",
        Source::Postgres => "postgres",
    };

    // Auto-detect pipeline name and topic from running instance if not specified.
    let (pipeline_name, topic) = detect_pipeline_info(
        cli.port,
        cli.pipeline.as_deref(),
        &cli.topic,
        source_name,
    )
    .await;

    let soak_src = SoakSource::new(
        cli.port,
        source_name,
        &pipeline_name,
        &topic,
    );

    let service = service_for_port(cli.port).to_string();

    let soak_params = SoakParams {
        src: soak_src,
        duration_mins: cli.duration_mins,
        writer_tasks: cli.writer_tasks,
        write_delay_ms: cli.write_delay_ms,
        drain_cfg,
    };

    let results = match cli.source {
        Source::Mysql => {
            let backend = MysqlBackend::new("df".into(), service);
            run_scenarios(&harness, &backend, &cli.scenario, &soak_params)
                .await?
        }
        Source::Postgres => {
            let backend = PgBackend::new("df".into(), service);
            run_scenarios(&harness, &backend, &cli.scenario, &soak_params)
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

/// Auto-detect pipeline name and sink topic from a running DeltaForge instance.
///
/// Returns `(pipeline_name, topic)`. Uses CLI overrides when provided,
/// otherwise reads from the running instance's pipeline config.
async fn detect_pipeline_info(
    port: u16,
    cli_pipeline: Option<&str>,
    cli_topic: &str,
    source_name: &str,
) -> (String, String) {
    let default_pipeline = format!("chaos-{source_name}");
    let default_topic = cli_topic.to_string();

    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build()
    {
        Ok(c) => c,
        Err(_) => {
            return (
                cli_pipeline
                    .map(|s| s.to_string())
                    .unwrap_or(default_pipeline),
                default_topic,
            )
        }
    };

    let resp = match client
        .get(format!("http://localhost:{port}/pipelines"))
        .send()
        .await
    {
        Ok(r) => r,
        Err(_) => {
            return (
                cli_pipeline
                    .map(|s| s.to_string())
                    .unwrap_or(default_pipeline),
                default_topic,
            )
        }
    };

    let pipelines: serde_json::Value = match resp.json().await {
        Ok(v) => v,
        Err(_) => {
            return (
                cli_pipeline
                    .map(|s| s.to_string())
                    .unwrap_or(default_pipeline),
                default_topic,
            )
        }
    };

    let first = match pipelines.as_array().and_then(|a| a.first()) {
        Some(p) => p,
        None => {
            return (
                cli_pipeline
                    .map(|s| s.to_string())
                    .unwrap_or(default_pipeline),
                default_topic,
            )
        }
    };

    // Extract pipeline name
    let name = cli_pipeline
        .map(|s| s.to_string())
        .or_else(|| first.get("name")?.as_str().map(|s| s.to_string()))
        .unwrap_or(default_pipeline);

    // Extract topic from first sink config (if CLI didn't override the default)
    let topic = if cli_topic != "chaos.cdc" {
        // User explicitly set --topic, use it
        cli_topic.to_string()
    } else {
        // Auto-detect from pipeline config
        first
            .pointer("/spec/spec/sinks/0/config/topic")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or(default_topic)
    };

    info!(
        pipeline = %name,
        topic = %topic,
        "auto-detected pipeline info from :{}",
        port
    );

    (name, topic)
}

/// Bundled parameters for soak/drain scenarios.
struct SoakParams {
    src: SoakSource,
    duration_mins: u64,
    writer_tasks: usize,
    write_delay_ms: u64,
    drain_cfg: scenarios::backlog_drain::DrainConfig,
}

/// Unified scenario dispatch — works for both MySQL and Postgres backends.
async fn run_scenarios<B: backend::SourceBackend>(
    harness: &Harness,
    backend: &B,
    scenario: &Scenario,
    soak: &SoakParams,
) -> Result<Vec<harness::ScenarioResult>> {
    let mut results = vec![];
    let is_mysql = backend.name() == "mysql";
    let is_postgres = backend.name() == "postgres";

    match scenario {
        // ── Generic scenarios (any source) ──────────────────────────────
        Scenario::NetworkPartition => {
            results.push(
                scenarios::network_partition::run(harness, backend).await?,
            );
        }
        Scenario::SinkOutage => {
            results
                .push(scenarios::sink_outage::run(harness, backend).await?);
        }
        Scenario::CrashRecovery => {
            results.push(
                scenarios::crash_recovery::run(harness, backend).await?,
            );
        }
        Scenario::SchemaDrift => {
            results.push(
                scenarios::schema_drift::run(harness, backend).await?,
            );
        }
        Scenario::ExactlyOnce => {
            results.push(
                scenarios::exactly_once::run(harness, backend).await?,
            );
        }
        Scenario::DlqPoison => {
            results
                .push(scenarios::dlq_poison::run(harness, backend).await?);
        }
        Scenario::SrOutage => {
            results
                .push(scenarios::sr_outage::run(harness, backend).await?);
        }

        // ── MySQL-specific ──────────────────────────────────────────────
        Scenario::Failover if is_mysql => {
            results.push(scenarios::failover::run(harness).await?);
        }
        Scenario::BinlogPurge if is_mysql => {
            results.push(scenarios::binlog_purge::run(harness).await?);
        }

        // ── PostgreSQL-specific ─────────────────────────────────────────
        Scenario::PgFailover if is_postgres => {
            results.push(scenarios::pg_failover::run(harness).await?);
        }
        Scenario::SlotDropped if is_postgres => {
            results.push(scenarios::slot_dropped::run(harness).await?);
        }

        // ── Soak / drain / benchmarks ───────────────────────────────────
        Scenario::Soak => {
            results.push(
                scenarios::soak::run_with_source(
                    harness,
                    &soak.src,
                    soak.duration_mins,
                    soak.writer_tasks,
                    soak.write_delay_ms,
                )
                .await?,
            );
        }
        Scenario::SoakStable => {
            results.push(
                scenarios::soak::run_stable_with_source(
                    harness,
                    &soak.src,
                    soak.duration_mins,
                    soak.writer_tasks,
                    soak.write_delay_ms,
                )
                .await?,
            );
        }
        Scenario::Tpcc => {
            results
                .push(scenarios::tpcc::run(harness, soak.duration_mins).await?);
        }
        Scenario::BacklogDrain => {
            results.push(
                scenarios::backlog_drain::run_with_source(
                    harness, &soak.src, soak.drain_cfg.clone(),
                )
                .await?,
            );
        }

        // ── Print-and-exit stubs ────────────────────────────────────────
        Scenario::TpcDi => {
            results.push(scenarios::tpc_di::run().await?);
        }
        Scenario::TpcE => {
            results.push(scenarios::tpc_e::run().await?);
        }

        // ── All — run generic + source-specific in order ────────────────
        Scenario::All => {
            results.push(
                scenarios::network_partition::run(harness, backend).await?,
            );
            results
                .push(scenarios::sink_outage::run(harness, backend).await?);
            results.push(
                scenarios::crash_recovery::run(harness, backend).await?,
            );
            if is_mysql {
                results.push(scenarios::failover::run(harness).await?);
            }
            if is_postgres {
                results
                    .push(scenarios::pg_failover::run(harness).await?);
            }
            results.push(
                scenarios::schema_drift::run(harness, backend).await?,
            );
            // Destructive scenarios last
            if is_mysql {
                results
                    .push(scenarios::binlog_purge::run(harness).await?);
            }
            if is_postgres {
                results
                    .push(scenarios::slot_dropped::run(harness).await?);
            }
        }

        Scenario::Ui => unreachable!("ui handled before dispatch"),

        // ── Source mismatch ─────────────────────────────────────────────
        _ => {
            eprintln!(
                "error: scenario {:?} is not available for --source {}",
                scenario.to_possible_value().unwrap().get_name(),
                backend.name()
            );
            std::process::exit(2);
        }
    }

    Ok(results)
}
