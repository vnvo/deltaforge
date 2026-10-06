// crates/runner/src/main.rs — full replacement

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use anyhow::{Context, Result};
use axum::Router;
use clap::{Parser, Subcommand};
use deltaforge_config::{StorageBackendKind, StorageConfig, load_cfg};
use rest_api::{
    AppState, PipelineController, SchemaState, SensingState, router_full,
};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use runner::storage_secrets;
use runner::{PipelineManager, SchemaApi, SensingApi};

mod version;

#[derive(Parser, Debug)]
#[command(name = "deltaforge")]
#[command(version = version::VERSION)]
#[command(about = "High-performance Change Data Capture Engine")]
struct Args {
    /// Optional subcommand. With none, DeltaForge starts the server.
    #[command(subcommand)]
    command: Option<Command>,
    /// Pipeline config file or directory. Omit to start with no pipelines
    /// (pipelines can be added via REST API).
    #[arg(short, long)]
    config: Option<String>,
    #[arg(long, default_value = "0.0.0.0:8080")]
    api_addr: String,
    /// Prometheus metrics listen address (host:port). Defaults to all interfaces
    /// on port 9000. The endpoint has NO authentication - restrict it to loopback
    /// or a private interface (e.g. 127.0.0.1:9000) when the scraper is local, or
    /// firewall / network-policy it otherwise. An invalid or unavailable address
    /// fails startup.
    #[arg(long, default_value = "0.0.0.0:9000")]
    metrics_addr: String,
    /// Storage backend: sqlite (default), memory, or postgres
    #[arg(long, global = true, default_value = "sqlite")]
    storage_backend: String,
    /// SQLite database path (sqlite backend only)
    #[arg(long, global = true, default_value = "./data/deltaforge.db")]
    storage_path: String,
    /// PostgreSQL DSN (postgres backend only)
    #[arg(long, global = true)]
    storage_dsn: Option<String>,
    /// Byte budget for cached latest schema versions, shared by all pipelines
    /// (primary bound; a conservative estimate of resident memory). A schema
    /// larger than the whole budget is served without being cached.
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    schema_cache_max_bytes: usize,
    /// Maximum number of cached latest schema versions, shared by all
    /// pipelines (secondary bound, one entry per table).
    #[arg(long, default_value_t = 50_000)]
    schema_cache_max_entries: usize,
    /// Snapshot connections allowed at once across every pipeline of this
    /// process (coordinator, lock, workers, intra-table readers). Snapshots
    /// beyond it queue; each source's own share is
    /// `snapshot.max_snapshot_connections`.
    #[arg(long, default_value_t = sources::snapshot_permits::DEFAULT_MAX_SNAPSHOT_CONNECTIONS)]
    max_snapshot_connections: u32,
    /// Recovery admin listener (`deltaforge recover`). Loopback only; use an
    /// SSH tunnel for remote administration.
    #[arg(long, default_value = "127.0.0.1:9091")]
    admin_addr: String,
    /// File holding the admin bearer token (mode 600, at least 32 bytes).
    /// Without it the recovery admin listener does not start.
    #[arg(long)]
    admin_token_file: Option<std::path::PathBuf>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Validate a pipeline config against its live source before deploying.
    ///
    /// Resolves secrets, connects to the source, and runs the source preflight
    /// checks (PostgreSQL: wal_level, replication slot, publication, WAL
    /// retention; MySQL: gtid_mode, binlog_format, RELOAD privilege, InnoDB) plus
    /// local config validation. Exits non-zero if any check fails.
    Preflight {
        /// Pipeline config file or directory.
        config: String,
        /// Emit a JSON report instead of human-readable text.
        #[arg(long)]
        json: bool,
    },
    /// Migrate pre-upgrade schema history to explicitly mapped sources.
    ///
    /// Dry run by default: prints the plan and its proof digest and writes
    /// nothing. `--apply --expect-proof <digest>` applies exactly the reviewed
    /// plan while holding the store gate, so the server must be stopped.
    /// Legacy records are never deleted.
    SchemaMigrate {
        /// Mapping file (YAML): explicit tables per source and asserted lineage.
        #[arg(long)]
        mapping: String,
        /// Only mapping entries of this tenant.
        #[arg(long)]
        tenant: Option<String>,
        /// Only mapping entries of this source id.
        #[arg(long)]
        source: Option<String>,
        /// Write the migration (requires --expect-proof).
        #[arg(long, requires = "expect_proof")]
        apply: bool,
        /// Proof digest printed by the dry run being applied.
        #[arg(long)]
        expect_proof: Option<String>,
        /// Take over and finish an unfinished migration (owner id from
        /// `store-gate status` or the failed run). Verify first that the
        /// recorded process is no longer running.
        #[arg(long, requires = "apply")]
        resume_owner: Option<String>,
        /// Emit JSON instead of human-readable text.
        #[arg(long)]
        json: bool,
    },
    /// Inspect or break the store gate that keeps the server and the schema
    /// migration from using the state store at the same time.
    StoreGate {
        #[command(subcommand)]
        action: GateAction,
    },
}

#[derive(Subcommand, Debug)]
enum GateAction {
    /// Show who holds the store gate.
    Status {
        #[arg(long)]
        json: bool,
    },
    /// Release a server's gate left behind by a process that is no longer
    /// running. Verify first that the recorded owner (host/pid) is not alive.
    /// A migration's gate cannot be broken: finish it with
    /// `schema-migrate ... --resume-owner`.
    Break {
        /// The owner id shown by `store-gate status`.
        #[arg(long)]
        owner: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    // The process-wide snapshot connection cap, before any pipeline starts.
    sources::snapshot_permits::configure(args.max_snapshot_connections)
        .map_err(|e| anyhow::anyhow!(e))?;

    // One-shot subcommands run before server/observability boot (no port binds).
    if let Some(Command::Preflight { config, json }) = &args.command {
        // Use the deployment's own storage backend so slot-ownership checks see
        // the same durable owner records startup uses.
        let storage_cfg = storage_config_from(&args);
        let backend = build_storage_backend(&storage_cfg)
            .await
            .context("initialise storage backend for preflight")?;
        let chkpt: Arc<dyn checkpoints::CheckpointStore> =
            Arc::new(storage::BackendCheckpointStore::new(backend));
        return runner::preflight::run(config, *json, chkpt).await;
    }
    if let Some(Command::SchemaMigrate {
        mapping,
        tenant,
        source,
        apply,
        expect_proof,
        resume_owner,
        json,
    }) = &args.command
    {
        let backend = build_storage_backend(&storage_config_from(&args))
            .await
            .context("initialise storage backend for schema-migrate")?;
        let migrate = runner::schema_migrate::MigrateArgs {
            mapping: mapping.clone(),
            tenant: tenant.clone(),
            source: source.clone(),
            apply: *apply,
            expect_proof: expect_proof.clone(),
            resume_owner: resume_owner.clone(),
            json: *json,
        };
        return runner::schema_migrate::run(migrate, backend).await;
    }
    if let Some(Command::StoreGate { action }) = &args.command {
        let backend = build_storage_backend(&storage_config_from(&args))
            .await
            .context("initialise storage backend for store-gate")?;
        return match action {
            GateAction::Status { json } => {
                runner::schema_migrate::gate_status(backend, *json).await
            }
            GateAction::Break { owner } => {
                runner::schema_migrate::gate_break(backend, owner).await
            }
        };
    }

    // Register SIGTERM/SIGINT handling first, before the store gate can be
    // acquired: from here on a signal no longer kills the process but cancels
    // `shutdown`, which startup and serving both honour, so every signal after
    // acquisition reaches the quiesce-and-release path.
    let shutdown = CancellationToken::new();
    install_shutdown_signals(shutdown.clone())?;

    eprintln!("{}", version::startup_banner());

    // Parse listen addresses up front so an invalid value fails startup clearly.
    let metrics_addr = parse_listen_addr("metrics-addr", &args.metrics_addr)?;

    let cfg = o11y::O11yConfig {
        logging: o11y::logging::Config {
            level: std::env::var("RUST_LOG").ok(),
            json: false,
            with_targets: false,
        },
        metrics: o11y::df_metrics::Config {
            enable: true,
            http_listener: Some(metrics_addr),
        },
        install_panic_hook: true,
    };
    // Fail startup if observability init fails (e.g. the metrics port is taken).
    o11y::init_all(&cfg)
        .map_err(|e| anyhow::anyhow!("initialize observability: {e}"))?;
    o11y::df_metrics::set_build_info(
        version::GIT_VERSION,
        version::GIT_HASH,
        version::BUILD_DATE,
    );

    let pipeline_specs = match &args.config {
        Some(path) => {
            load_pipeline_cfgs(path).context("load pipeline specs")?
        }
        None => vec![],
    };

    // Build summary lines before specs are consumed by manager.create()
    let pipeline_specs_summary: Vec<String> =
        pipeline_specs.iter().map(format_pipeline_summary).collect();

    // ── Build storage backend ─────────────────────────────────────────────────
    let storage_cfg = storage_config_from(&args);

    let backend = build_storage_backend(&storage_cfg)
        .await
        .context("initialise storage backend")?;

    info!(backend = %args.storage_backend, "storage backend ready");

    // ── Store gate ────────────────────────────────────────────────────────────
    // Held from before any schema access until every pipeline has stopped, so
    // the schema migration can never write while this server runs. A crash
    // leaves it held until `deltaforge store-gate break`.
    hold_startup_for_test("before-gate", &shutdown).await;
    if shutdown.is_cancelled() {
        info!("shutdown requested before startup; exiting");
        return Ok(());
    }
    let gate = storage::adapters::store_gate::acquire(
        &backend,
        storage::adapters::store_gate::GateRole::Server,
    )
    .await
    .context("acquire the store gate")?;
    info!(holder = %gate.holder(), "store gate acquired");

    let mut manager_slot = None;
    let served = serve(
        &args,
        backend,
        pipeline_specs,
        &pipeline_specs_summary,
        &mut manager_slot,
        &shutdown,
    )
    .await;

    // Quiesce every pipeline (and with it every registry writer) before the
    // gate is released, on a clean shutdown and on a startup failure alike.
    if let Some(manager) = manager_slot {
        info!("stopping pipelines");
        manager.shutdown_all().await;
    }
    let released = gate.release().await;
    // A startup/serve failure is the primary error; report it over a failed release.
    if let Err(e) = served {
        if let Err(r) = released {
            tracing::error!(error = %r, "release the store gate");
        }
        return Err(e);
    }
    released.context("release the store gate")?;
    info!("store gate released");
    Ok(())
}

async fn serve(
    args: &Args,
    backend: storage::ArcStorageBackend,
    pipeline_specs: Vec<deltaforge_config::PipelineSpec>,
    pipeline_specs_summary: &[String],
    manager_slot: &mut Option<Arc<PipelineManager>>,
    shutdown: &CancellationToken,
) -> Result<()> {
    // Startup steps are never abandoned half-way (a dropped pipeline start
    // could leave tasks the manager does not track); a shutdown requested
    // meanwhile is honoured between steps.
    let stop_requested = || {
        let requested = shutdown.is_cancelled();
        if requested {
            info!("shutdown requested during startup");
        }
        requested
    };
    // ── Build pipeline manager ────────────────────────────────────────────────
    let registry_config = storage::adapters::RegistryConfig {
        cache_max_bytes: args.schema_cache_max_bytes,
        cache_max_entries: args.schema_cache_max_entries,
        ..storage::adapters::RegistryConfig::default()
    };
    info!(
        cache_max_bytes = registry_config.cache_max_bytes,
        cache_max_entries = registry_config.cache_max_entries,
        "schema registry cache budget"
    );
    let manager = Arc::new(
        PipelineManager::with_backend_and_registry_config(
            backend,
            registry_config,
        )
        .await
        .context("build pipeline manager")?,
    );
    *manager_slot = Some(manager.clone());
    // The recovery admin listener: loopback only, a token always.
    let admin = match &args.admin_token_file {
        Some(path) => {
            let addr = runner::recovery::admin_addr(&args.admin_addr)?;
            let token = runner::recovery::load_admin_token(path)?;
            let listener = TcpListener::bind(addr)
                .await
                .with_context(|| format!("bind --admin-addr {addr}"))?;
            info!(%addr, "recovery admin listening");
            Some((listener, token))
        }
        None => {
            info!("recovery admin listener disabled (no --admin-token-file)");
            None
        }
    };
    let schema_api = Arc::new(SchemaApi::new(manager.clone()));
    let sensing_api = Arc::new(SensingApi::new(manager.clone()));

    hold_startup_for_test("after-gate", shutdown).await;
    if stop_requested() {
        return Ok(());
    }

    for ps in pipeline_specs {
        manager.create(ps).await?;
        if stop_requested() {
            return Ok(());
        }
    }

    version::print_runtime_info(
        &args.api_addr,
        &args.metrics_addr,
        &args.storage_backend,
        pipeline_specs_summary.len(),
    );
    for summary in pipeline_specs_summary {
        eprintln!("{summary}");
    }
    if pipeline_specs_summary.is_empty() {
        eprintln!(
            "  Use REST API to add pipelines: POST http://{}/pipelines",
            args.api_addr
        );
    }
    eprintln!();

    // ── HTTP server ───────────────────────────────────────────────────────────
    let app: Router = router_full(
        AppState {
            controller: manager.clone(),
        },
        SchemaState {
            controller: schema_api,
        },
        SensingState {
            controller: sensing_api,
        },
    );
    let app = app.merge(o11y::df_metrics::router_with_metrics());

    if let Some((listener, token)) = admin {
        let admin_app =
            rest_api::recovery::router(rest_api::recovery::RecoveryState {
                controller: Arc::new(runner::recovery::RecoveryService::new(
                    manager.clone(),
                )),
                token,
            });
        let stop = shutdown.clone();
        tokio::spawn(async move {
            if let Err(e) = axum::serve(
                listener,
                admin_app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(stop.cancelled_owned())
            .await
            {
                tracing::error!(error = %e, "recovery admin listener stopped");
            }
        });
    }

    let addr = parse_listen_addr("api-addr", &args.api_addr)?;
    info!(%addr, "api listening");

    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind --api-addr {addr}"))?;
    if stop_requested() {
        return Ok(());
    }
    // Connection info lets audited operator actions (incident
    // acknowledgement) record the peer address as their origin.
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown.clone().cancelled_owned())
    .await?;
    info!("api server stopped");
    Ok(())
}

/// Test hook: with `DELTAFORGE_TEST_HOLD_STARTUP=<point>`, startup waits at
/// `point` (`before-gate`: signal handling installed, gate not yet acquired;
/// `after-gate`: gate held, pipeline manager built) until a shutdown is
/// requested, so a test can signal the process at that exact point.
async fn hold_startup_for_test(point: &str, shutdown: &CancellationToken) {
    if std::env::var("DELTAFORGE_TEST_HOLD_STARTUP")
        .ok()
        .as_deref()
        != Some(point)
    {
        return;
    }
    eprintln!("DELTAFORGE_TEST_HOLD_STARTUP: holding at {point}");
    tokio::select! {
        _ = shutdown.cancelled() => {}
        _ = tokio::time::sleep(std::time::Duration::from_secs(120)) => {}
    }
}

/// Register SIGTERM and SIGINT handlers now (registration is synchronous, so
/// a signal arriving at any later point is caught) and cancel `shutdown` on
/// the first one.
fn install_shutdown_signals(shutdown: CancellationToken) -> Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate())
            .context("install the SIGTERM handler")?;
        let mut int = signal(SignalKind::interrupt())
            .context("install the SIGINT handler")?;
        tokio::spawn(async move {
            tokio::select! {
                _ = term.recv() => {}
                _ = int.recv() => {}
            }
            info!("shutdown signal received");
            shutdown.cancel();
        });
    }
    #[cfg(not(unix))]
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        info!("shutdown signal received");
        shutdown.cancel();
    });
    Ok(())
}

/// Parse a `host:port` listen address, failing with a clear message on bad input.
fn parse_listen_addr(flag: &str, value: &str) -> Result<SocketAddr> {
    value.parse().with_context(|| {
        format!(
            "invalid --{flag} '{value}' (expected host:port, e.g. 127.0.0.1:9000)"
        )
    })
}

fn storage_config_from(args: &Args) -> StorageConfig {
    StorageConfig {
        backend: match args.storage_backend.as_str() {
            "memory" => StorageBackendKind::Memory,
            "postgres" => StorageBackendKind::Postgres,
            _ => StorageBackendKind::Sqlite,
        },
        path: args.storage_path.clone(),
        dsn: args.storage_dsn.clone(),
        ..Default::default()
    }
}

async fn build_storage_backend(
    cfg: &StorageConfig,
) -> Result<storage::ArcStorageBackend> {
    match cfg.backend {
        StorageBackendKind::Memory => {
            info!("using in-memory storage backend (ephemeral)");
            Ok(Arc::new(storage::MemoryStorageBackend::new())
                as storage::ArcStorageBackend)
        }
        StorageBackendKind::Sqlite => {
            if let Some(parent) = std::path::Path::new(&cfg.path).parent() {
                std::fs::create_dir_all(parent)
                    .context("create storage directory")?;
            }
            info!(path = %cfg.path, "using SQLite storage backend");
            storage::SqliteStorageBackend::open(&cfg.path)
                .map(|b| b as storage::ArcStorageBackend)
                .context("open SQLite storage backend")
        }
        StorageBackendKind::Postgres => {
            // Resolve credentials (inline dsn | dsn_secret | base + credential refs)
            // through the bootstrap resolver and fail closed BEFORE opening the backend.
            let dsn = storage_secrets::resolve_storage_dsn(cfg)
                .await
                .context("resolve storage credentials")?
                .context(
                    "storage postgres backend requires a dsn, dsn_secret, or \
                     base dsn + credentials",
                )?;
            info!("using PostgreSQL storage backend");
            storage::PostgresStorageBackend::connect(&dsn)
                .await
                .map(|b| b as storage::ArcStorageBackend)
                .context("connect PostgreSQL storage backend")
        }
    }
}

fn load_pipeline_cfgs(
    path: &str,
) -> Result<Vec<deltaforge_config::PipelineSpec>> {
    let specs = load_cfg(path)?;
    info!(specs_found = specs.len(), "pipeline specs loaded");
    debug!(pipeline_specs = ?specs, "pipeline spec");
    Ok(specs)
}

fn format_pipeline_summary(ps: &deltaforge_config::PipelineSpec) -> String {
    use deltaforge_config::{ProcessorCfg, SourceCfg};

    let name = &ps.metadata.name;

    let source_type = match &ps.spec.source {
        SourceCfg::Mysql(_) => "mysql",
        SourceCfg::Postgres(_) => "postgres",
    };
    let source_id = ps.spec.source.source_id();

    let processors: Vec<&str> = ps
        .spec
        .processors
        .iter()
        .map(|p| match p {
            ProcessorCfg::Javascript { id, .. } => id.as_str(),
            ProcessorCfg::Outbox { .. } => "outbox",
            ProcessorCfg::Flatten { .. } => "flatten",
            ProcessorCfg::Filter { .. } => "filter",
        })
        .collect();

    let sinks: Vec<String> = ps
        .spec
        .sinks
        .iter()
        .map(|s| {
            let kind = match s {
                deltaforge_config::SinkCfg::Kafka(_) => "kafka",
                deltaforge_config::SinkCfg::Redis(_) => "redis",
                deltaforge_config::SinkCfg::Nats(_) => "nats",
                deltaforge_config::SinkCfg::Http(_) => "http",
                deltaforge_config::SinkCfg::S3(_) => "s3",
                deltaforge_config::SinkCfg::ClickHouse(_) => "clickhouse",
                deltaforge_config::SinkCfg::Elasticsearch(_) => "elasticsearch",
            };
            format!("{kind}:{}", s.sink_id())
        })
        .collect();

    let procs_str = if processors.is_empty() {
        "none".to_string()
    } else {
        processors.join(", ")
    };

    format!(
        "  [{name}]  {source_type}:{source_id} -> [{procs_str}] -> {sinks}",
        sinks = sinks.join(", "),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_listen_addr_accepts_valid_and_rejects_invalid() {
        assert!(parse_listen_addr("metrics-addr", "127.0.0.1:9000").is_ok());
        assert!(parse_listen_addr("metrics-addr", "0.0.0.0:9000").is_ok());
        assert!(parse_listen_addr("metrics-addr", "[::1]:9000").is_ok());
        // missing port
        assert!(parse_listen_addr("metrics-addr", "127.0.0.1").is_err());
        // not an address
        assert!(parse_listen_addr("metrics-addr", "nonsense").is_err());
        // empty
        assert!(parse_listen_addr("metrics-addr", "").is_err());
    }
}
