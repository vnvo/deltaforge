// crates/runner/src/main.rs — full replacement

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use anyhow::{Context, Result};
use axum::Router;
use clap::Parser;
use deltaforge_config::{StorageBackendKind, StorageConfig, load_cfg};
use rest_api::{
    AppState, PipelineController, SchemaState, SensingState, router_full,
};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{debug, info};

use runner::{PipelineManager, SchemaApi, SensingApi};

mod version;

#[derive(Parser, Debug)]
#[command(name = "deltaforge")]
#[command(version = version::VERSION)]
#[command(about = "High-performance Change Data Capture Engine")]
struct Args {
    /// Pipeline config file or directory. Omit to start with no pipelines
    /// (pipelines can be added via REST API).
    #[arg(short, long)]
    config: Option<String>,
    #[arg(long, default_value = "0.0.0.0:8080")]
    api_addr: String,
    #[arg(long, default_value = "0.0.0.0:9095")]
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
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    eprintln!("{}", version::startup_banner());

    let cfg = o11y::O11yConfig {
        logging: o11y::logging::Config {
            level: std::env::var("RUST_LOG").ok(),
            json: false,
            with_targets: false,
        },
        metrics: o11y::df_metrics::Config {
            enable: true,
            http_listener: Some(([0, 0, 0, 0], 9000).into()),
        },
        install_panic_hook: true,
    };
    let _ = o11y::init_all(&cfg);
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
    let storage_cfg = StorageConfig {
        backend: match args.storage_backend.as_str() {
            "memory" => StorageBackendKind::Memory,
            "postgres" => StorageBackendKind::Postgres,
            _ => StorageBackendKind::Sqlite,
        },
        path: args.storage_path.clone(),
        dsn: args.storage_dsn.clone(),
    };

    let backend = build_storage_backend(&storage_cfg)
        .await
        .context("initialise storage backend")?;

    info!(backend = %args.storage_backend, "storage backend ready");

    // ── Build pipeline manager ────────────────────────────────────────────────
    let manager = Arc::new(
        PipelineManager::with_backend(backend)
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

    let addr: SocketAddr =
        args.api_addr.parse().expect("api_addr must be host:port");
    info!(%addr, "api listening");

    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
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
            let dsn = cfg.dsn.as_deref().context(
                "--storage-dsn is required when using --storage-backend postgres"
            )?;
            info!("using PostgreSQL storage backend");
            storage::PostgresStorageBackend::connect(dsn)
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
        #[cfg(feature = "turso")]
        SourceCfg::Turso(_) => "turso",
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
