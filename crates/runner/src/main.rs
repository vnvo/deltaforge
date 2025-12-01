use anyhow::{Context, Result};
use axum::Router;
use checkpoints::{CheckpointStore, FileCheckpointStore};
use clap::Parser;
use deltaforge_config::{PipelineSpec, load_cfg};
use deltaforge_core::{CheckpointMeta, Event, SourceHandle};
use o11y;
use sinks::build_sinks;
use sources::build_source;
use metrics::{counter, gauge};
use processors::build_processors;
use rest_api::{AppState, PipeInfo, router};
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
    let args = Args::parse();

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
        counter!("deltaforge_pipelines_total").increment(1);

        let pipeline_name = ps.metadata.name.clone();
        let source = build_source(&ps)
            .context(format!("build source for {pipeline_name}"))?;
        let processors = build_processors(&ps)?;
        let sinks = build_sinks(&ps).context("build sinks")?;

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
            gauge!("deltaforge_running_pipeline", "pipeline" => pname.clone())
                .increment(1.0);

            let res = coord.run(event_rx).await;

            if let Err(ref e) = res {
                gauge!("deltaforge_running_pipeline", "pipeline" => pname.clone()).decrement(1.0);
                error!(pipeline=%pname, error=%e, "coordinator exited with error");
            } else {
                gauge!("deltaforge_running_pipeline", "pipeline" => pname.clone()).decrement(1.0);
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
    let app = app.merge(o11y::df_metrics::router_with_metrics());

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
