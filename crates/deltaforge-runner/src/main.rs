use anyhow::{Context, Result};
use axum::Router;
use clap::Parser;
use deltaforge_api::{AppState, PipeInfo, router};
use deltaforge_checkpoints::{CheckpointStore, FileCheckpointStore};
use deltaforge_config::{PipelineSpec, load_cfg};
use deltaforge_core::{
    CheckpointMeta, Event, SourceHandle,
};
use deltaforge_metrics as metrics;
use deltaforge_processor_js::build_processors;
use deltaforge_sinks::build_sinks;
use deltaforge_sources::build_source;
 //, postgres::PostgresSource};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::{net::TcpListener, sync::mpsc, task::JoinHandle};
use tracing::{debug, error, info};

use crate::coordinator::{
    CommitCpFn, Coordinator, ProcessBatchFn, build_batch_processor,
    build_commit_fn,
};

mod coordinator;
#[derive(Parser, Debug)]
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
    metrics::init();
    let args = Args::parse();
    metrics::install_prometheus(&args.metrics_addr);

    //shared api state
    let state = AppState::default();

    let pipeline_specs =
        load_pipeline_cfgs(&args.config).context("load pipeline specs")?;
    let ckpt_store: Arc<dyn CheckpointStore> =
        Arc::new(FileCheckpointStore::new("./data/df_checkpoints.json")?);
    let mut running_pipelines: Vec<JoinHandle<Result<()>>> =
        Vec::with_capacity(pipeline_specs.len());
    let mut source_handles: Vec<SourceHandle> =
        Vec::with_capacity(pipeline_specs.len());

    for ps in pipeline_specs {
        let pipeline_name = ps.metadata.name.clone();
        let source = build_source(&ps)
            .context(format!("build source for {pipeline_name}"))?;
        let processors = build_processors(&ps);
        let sinks = build_sinks(&ps);

        let (event_tx, event_rx) = mpsc::channel::<Event>(4096);
        let src_handle = source.run(event_tx, ckpt_store.clone()).await;
        source_handles.push(src_handle);

        let batch_processor: ProcessBatchFn<CheckpointMeta> =
            build_batch_processor(processors);
        let commit_cp: CommitCpFn<CheckpointMeta> =
            build_commit_fn(ckpt_store.clone(), pipeline_name.clone());

        let coord = Coordinator::new(
            pipeline_name.clone(),
            sinks,
            ps.spec.batch.clone(),
            ps.spec.commit_policy.clone(),
            commit_cp,
            batch_processor,
        );

        let pname = pipeline_name.clone();
        let pipeline = tokio::spawn(async move {
            info!(pipeline_name=%pname, "pipeline coordinator starting ...");
            let res = coord.run(event_rx).await;
            if let Err(ref e) = res {
                error!(pipeline=%pname, error=%e, "coordinator exited with error");
            } else {
                info!(pipeline_name=%pname, "coordinator exited normally");
            }

            res
        });

        running_pipelines.push(pipeline);

        state.pipelines.write().push(PipeInfo {
            id: pipeline_name,
            status: "running".into(),
        });
    }

    let app: Router = router(state);

    let addr: SocketAddr =
        args.api_addr.parse().expect("api_addr must be host:port");
    info!(%addr, "api listening");

    let listener = TcpListener::bind(addr).await?;
    let api_task = tokio::spawn(axum::serve(listener, app).into_future());

    for p in running_pipelines {
        p.await??;
    }

    api_task.await??;

    Ok(())
}

fn load_pipeline_cfgs(path: &str) -> Result<Vec<PipelineSpec>> {
    let specs = load_cfg(path)?;
    info!(specs_found = specs.len(), "pipeline specs loaded");
    debug!(pipeline_specs=?specs, "pipeline spec");
    Ok(specs)
}
