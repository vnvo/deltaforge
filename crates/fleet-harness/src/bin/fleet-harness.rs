//! `fleet-harness`: single-instance MySQL fleet qualification tooling
//! (`docs/design/mysql-fleet-qualification.md`, crate README).

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use fleet_harness::config::{RunClass, RunConfig};
use fleet_harness::run::{RunOptions, sweep};
use fleet_harness::{deltaforge, fixture, store, topology};

#[derive(Parser)]
#[command(about = "Single-instance MySQL fleet qualification harness")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Show the placeholders of a configuration and whether it could run as
    /// a qualification.
    Check { config: String },
    /// Build (or resume, or reuse) the fixture on the configured servers.
    Fixture {
        config: String,
        /// Only the first N servers.
        #[arg(long)]
        servers: Option<usize>,
        #[arg(long, default_value_t = 8)]
        concurrency: usize,
    },
    /// Print the pipeline specs the run would create.
    Render { config: String },
    /// Run the configured scenario for every source count and repetition.
    Run {
        config: String,
        #[arg(long, default_value_t = 30)]
        drain_idle_secs: u64,
        #[arg(long, default_value_t = 1800)]
        drain_max_secs: u64,
        #[arg(long, default_value_t = 900)]
        recovery_timeout_secs: u64,
        #[arg(long)]
        keep_pipelines: bool,
    },
    /// One cell of the physical state-store matrix, on an empty store.
    StoreMatrix {
        #[arg(long)]
        dsn: String,
        #[arg(long, default_value = "exploratory")]
        class: String,
        #[arg(long)]
        sources: u32,
        #[arg(long)]
        tables_per_source: u64,
        #[arg(long)]
        versions_changed: u32,
        #[arg(long)]
        changed_fraction: f64,
        #[arg(long, default_value_t = 2000)]
        samples: usize,
        #[arg(long, default_value = "results")]
        output_dir: String,
        /// Backup hook command (timed), e.g. a pg_basebackup invocation.
        #[arg(long)]
        backup: Option<String>,
        /// Restore hook command (timed).
        #[arg(long)]
        restore: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().cmd {
        Cmd::Check { config } => {
            let cfg = RunConfig::load(&config)?;
            let placeholders = cfg.placeholders();
            println!(
                "class: {:?}, scenario: {}, sources: {:?}",
                cfg.class, cfg.run.scenario, cfg.run.sources
            );
            println!("placeholders ({}):", placeholders.len());
            for p in &placeholders {
                println!("  - {p}");
            }
            let errors = cfg.qualification_errors();
            if errors.is_empty() {
                println!("can run as a qualification");
            } else {
                println!("cannot run as a qualification:");
                for e in errors {
                    println!("  - {e}");
                }
            }
        }
        Cmd::Fixture {
            config,
            servers,
            concurrency,
        } => {
            let cfg = RunConfig::load(&config)?;
            let n = servers.unwrap_or(cfg.topology.servers.len());
            for s in &cfg.topology.servers[..n] {
                let r = fixture::build(&cfg, s, concurrency, true)
                    .await
                    .with_context(|| format!("fixture on {}", s.name))?;
                println!("{}", serde_json::to_string(&r)?);
            }
        }
        Cmd::Render { config } => {
            let cfg = RunConfig::load(&config)?;
            let naming = topology::Naming {
                database_prefix: cfg.topology.database_prefix.clone(),
            };
            for s in &cfg.topology.servers {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&deltaforge::render_spec(
                        &cfg, s, &naming
                    )?)?
                );
            }
        }
        Cmd::Run {
            config,
            drain_idle_secs,
            drain_max_secs,
            recovery_timeout_secs,
            keep_pipelines,
        } => {
            let cfg = RunConfig::load(&config)?;
            // SIGINT/SIGTERM abort the run: it still drains and writes its
            // result (outcome `aborted`, the partial counters).
            let (abort_tx, abort) = tokio::sync::watch::channel(None);
            let interrupted =
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            {
                let interrupted = interrupted.clone();
                tokio::spawn(async move {
                    use tokio::signal::unix::{SignalKind, signal};
                    let mut term = signal(SignalKind::terminate()).ok();
                    let why = tokio::select! {
                        _ = tokio::signal::ctrl_c() => "interrupted (SIGINT)",
                        _ = async {
                            match term.as_mut() {
                                Some(t) => { t.recv().await; }
                                None => std::future::pending::<()>().await,
                            }
                        } => "interrupted (SIGTERM)",
                    };
                    eprintln!("{why}: aborting the run");
                    interrupted
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    abort_tx.send(Some(why.to_string())).ok();
                });
            }
            let opts = RunOptions {
                drain_idle: Duration::from_secs(drain_idle_secs),
                drain_max: Duration::from_secs(drain_max_secs),
                recovery_timeout: Duration::from_secs(recovery_timeout_secs),
                keep_pipelines,
                abort: Some(abort),
                ..RunOptions::default()
            };
            let s = sweep(cfg, opts).await?;
            println!("{}", serde_json::to_string_pretty(&s)?);
            if interrupted.load(std::sync::atomic::Ordering::SeqCst) {
                std::process::exit(130);
            }
        }
        Cmd::StoreMatrix {
            dsn,
            class,
            sources,
            tables_per_source,
            versions_changed,
            changed_fraction,
            samples,
            output_dir,
            backup,
            restore,
        } => {
            let class = match class.as_str() {
                "qualification" => RunClass::Qualification,
                _ => RunClass::Exploratory,
            };
            let mut hooks = BTreeMap::new();
            if let Some(b) = backup {
                hooks.insert("store_backup".to_string(), b);
            }
            if let Some(r) = restore {
                hooks.insert("store_restore".to_string(), r);
            }
            let cell = store::Cell {
                sources,
                tables_per_source,
                versions_changed,
                changed_fraction,
            };
            let r = store::run_cell(
                &dsn,
                class,
                cell,
                samples,
                &hooks,
                &output_dir,
            )
            .await?;
            println!("{}", serde_json::to_string_pretty(&r)?);
        }
    }
    Ok(())
}
