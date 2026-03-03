use anyhow::{Context, Result};
use axum::Router;
use clap::Parser;
use deltaforge_config::{StorageBackendKind, StorageConfig, load_cfg};
use rest_api::{
    AppState, PipelineController, SchemaState, SensingState, router_full,
};
use std::net::SocketAddr;
use std::sync::Arc;
use storage::SqliteStorageBackend;
use tokio::net::TcpListener;
use tracing::{debug, info};

use runner::{PipelineManager, SchemaApi, SensingApi};

mod version;

#[derive(Parser, Debug)]
#[command(name = "deltaforge")]
#[command(version = version::VERSION)]
#[command(about = "High-performance Change Data Capture Engine")]
struct Args {
    #[arg(short, long)]
    config: String,
    #[arg(long, default_value = "0.0.0.0:8080")]
    api_addr: String,
    #[arg(long, default_value = "0.0.0.0:9095")]
    metrics_addr: String,
    /// Storage backend: sqlite (default) or memory
    #[arg(long, default_value = "sqlite")]
    storage_backend: String,
    /// SQLite database path (only used with --storage-backend sqlite)
    #[arg(long, default_value = "./data/deltaforge.db")]
    storage_path: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    eprintln!("{}", version::startup_banner());

    let cfg = o11y::O11yConfig {
        logging: o11y::logging::Config {
            level: None,
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

    let pipeline_specs =
        load_pipeline_cfgs(&args.config).context("load pipeline specs")?;

    let storage_cfg = StorageConfig {
        backend: match args.storage_backend.as_str() {
            "memory" => StorageBackendKind::Memory,
            _ => StorageBackendKind::Sqlite,
        },
        path: args.storage_path.clone(),
    };

    let backend = build_storage_backend(&storage_cfg)
        .context("initialise storage backend")?;

    info!(
        backend = %args.storage_backend,
        path = %args.storage_path,
        "storage backend ready"
    );

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

fn build_storage_backend(
    cfg: &StorageConfig,
) -> Result<storage::ArcStorageBackend> {
    match cfg.backend {
        StorageBackendKind::Memory => {
            info!("using in-memory storage backend (ephemeral)");
            Ok(Arc::new(storage::MemoryStorageBackend::new()))
        }
        StorageBackendKind::Sqlite => {
            // Ensure parent directory exists.
            if let Some(parent) = std::path::Path::new(&cfg.path).parent() {
                std::fs::create_dir_all(parent)
                    .context("create storage directory")?;
            }
            info!(path = %cfg.path, "using SQLite storage backend");
            SqliteStorageBackend::open(&cfg.path)
                .map(|b| b as storage::ArcStorageBackend)
                .context("open SQLite storage backend")
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
