use anyhow::{Context, Result};
use axum::Router;
use checkpoints::{CheckpointStore, FileCheckpointStore};
use clap::Parser;
use deltaforge_config::{PipelineSpec, load_cfg};
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
    #[arg(short, long)]
    config: String,
    #[arg(long, default_value = "0.0.0.0:8080")]
    api_addr: String,
    #[arg(long, default_value = "0.0.0.0:9095")]
    metrics_addr: String,
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
    let ckpt_store: Arc<dyn CheckpointStore> =
        Arc::new(FileCheckpointStore::new("./data/df_checkpoints.json")?);

    // Create manager and API wrappers
    let manager = Arc::new(PipelineManager::new(ckpt_store.clone()));
    let schema_api = Arc::new(SchemaApi::new(manager.clone()));
    let sensing_api = Arc::new(SensingApi::new(manager.clone()));

    for ps in pipeline_specs {
        manager.create(ps).await?;
    }

    // Build router with all routes
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
    let api_task = tokio::spawn(axum::serve(listener, app).into_future());
    api_task.await??;

    Ok(())
}

fn load_pipeline_cfgs(path: &str) -> Result<Vec<PipelineSpec>> {
    let specs = load_cfg(path)?;
    info!(specs_found = specs.len(), "pipeline specs loaded");
    debug!(pipeline_specs = ?specs, "pipeline spec");
    Ok(specs)
}
