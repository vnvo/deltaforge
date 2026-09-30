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
    #[arg(long, default_value = "sqlite")]
    storage_backend: String,
    /// SQLite database path (sqlite backend only)
    #[arg(long, default_value = "./data/deltaforge.db")]
    storage_path: String,
    /// PostgreSQL DSN (postgres backend only)
    #[arg(long)]
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
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

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
    let schema_api = Arc::new(SchemaApi::new(manager.clone()));
    let sensing_api = Arc::new(SensingApi::new(manager.clone()));

    for ps in pipeline_specs {
        manager.create(ps).await?;
    }

    version::print_runtime_info(
        &args.api_addr,
        &args.metrics_addr,
        &args.storage_backend,
        pipeline_specs_summary.len(),
    );
    for summary in &pipeline_specs_summary {
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
            controller: manager,
        },
        SchemaState {
            controller: schema_api,
        },
        SensingState {
            controller: sensing_api,
        },
    );
    let app = app.merge(o11y::df_metrics::router_with_metrics());

    let addr = parse_listen_addr("api-addr", &args.api_addr)?;
    info!(%addr, "api listening");

    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind --api-addr {addr}"))?;
    axum::serve(listener, app).await?;

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
