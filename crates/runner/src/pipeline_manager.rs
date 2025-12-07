use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use checkpoints::CheckpointStore;
use deltaforge_config::PipelineSpec;
use deltaforge_core::{Event, SourceHandle};
use futures::future::try_join_all;
use metrics::{counter, gauge};
use parking_lot::RwLock;
use processors::build_processors;
use rest_api::{PipeInfo, PipelineController, PipelineAPIError};
use serde_json::Value;
use sinks::build_sinks;
use sources::build_source;
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::coordinator::{Coordinator, build_batch_processor, build_commit_fn};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PipelineStatus {
    Running,
    Paused,
    Stopped,
}

impl PipelineStatus {
    fn as_str(&self) -> &'static str {
        match self {
            PipelineStatus::Running => "running",
            PipelineStatus::Paused => "paused",
            PipelineStatus::Stopped => "stopped",
        }
    }
}

struct PipelineRuntime {
    spec: PipelineSpec,
    status: PipelineStatus,
    cancel: CancellationToken,
    pause: watch::Sender<bool>,
    sources: Vec<SourceHandle>,
    join: Option<JoinHandle<Result<()>>>,
}

impl PipelineRuntime {
    /// Apply pause semantics to both ends of the pipeline:
    /// - SourceHandle.pause() stops ingress at the source boundary.
    /// - The pause watch channel parks the coordinator so it does not drain the channel
    ///   while sources are paused.
    fn pause(&mut self) {
        self.sources.iter().for_each(|s| s.pause());
        let _ = self.pause.send(true);
        self.status = PipelineStatus::Paused;
    }

    /// Resume ingress (sources) and coordinator consumption together so the channel
    /// backlog can unwind immediately.
    fn resume(&mut self) {
        self.sources.iter().for_each(|s| s.resume());
        let _ = self.pause.send(false);
        self.status = PipelineStatus::Running;
    }

    fn info(&self) -> PipeInfo {
        PipeInfo {
            name: self.spec.metadata.name.clone(),
            status: self.status.as_str().to_string(),
            spec: self.spec.clone(),
        }
    }
}

#[derive(Clone)]
pub struct PipelineManager {
    pipelines: Arc<RwLock<HashMap<String, PipelineRuntime>>>,
    ckpt_store: Arc<dyn CheckpointStore>,
}

impl PipelineManager {
    pub fn new(ckpt_store: Arc<dyn CheckpointStore>) -> Self {
        Self {
            pipelines: Arc::new(RwLock::new(HashMap::new())),
            ckpt_store,
        }
    }

    async fn spawn_pipeline(
        &self,
        spec: PipelineSpec,
    ) -> Result<PipelineRuntime> {
        let pipeline_name = spec.metadata.name.clone();

        counter!("deltaforge_pipelines_total").increment(1);

        let source = build_source(&spec).context("build source")?;
        let processors = build_processors(&spec).context("build processors")?;
        let sinks = build_sinks(&spec).context("build sinks")?;

        let (event_tx, event_rx) = mpsc::channel::<Event>(4096);
        let src_handle = source.run(event_tx, self.ckpt_store.clone()).await;

        let batch_processor = build_batch_processor(processors);
        let commit_cp =
            build_commit_fn(self.ckpt_store.clone(), pipeline_name.clone());

        let (pause_tx, pause_rx) = watch::channel(false);

        let coord = Coordinator::new(
            pipeline_name.clone(),
            sinks,
            spec.spec.batch.clone(),
            spec.spec.commit_policy.clone(),
            commit_cp,
            batch_processor,
        );

        let cancel = CancellationToken::new();
        let cancel_for_task = cancel.clone();
        let pname = pipeline_name.clone();
        let join = tokio::spawn(async move {
            info!(pipeline_name=%pname, "pipeline coordinator starting ...");
            gauge!("deltaforge_running_pipeline", "pipeline" => pname.clone())
                .increment(1.0);

            let res = coord.run(event_rx, cancel_for_task, pause_rx).await;

            if let Err(ref e) = res {
                gauge!("deltaforge_running_pipeline", "pipeline" => pname.clone())
                    .decrement(1.0);
                warn!(pipeline=%pname, error=%e, "coordinator exited with error");
            } else {
                gauge!("deltaforge_running_pipeline", "pipeline" => pname.clone())
                    .decrement(1.0);
                info!(pipeline_name=%pname, "coordinator exited normally");
            }

            res
        });

        Ok(PipelineRuntime {
            spec,
            status: PipelineStatus::Running,
            cancel,
            pause: pause_tx,
            sources: vec![src_handle],
            join: Some(join),
        })
    }

    async fn shutdown_runtime(
        &self,
        mut runtime: PipelineRuntime,
    ) -> Result<()> {
        runtime.status = PipelineStatus::Stopped;
        runtime.cancel.cancel();
        runtime.sources.iter().for_each(|s| s.stop());

        if let Some(join) = runtime.join.take() {
            join.await.context("coordinator join")??;
        }

        let source_results =
            try_join_all(runtime.sources.into_iter().map(|h| h.join())).await;
        if let Err(e) = source_results {
            return Err(e).context("source join");
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl PipelineController for PipelineManager {
    async fn list(&self) -> Vec<PipeInfo> {
        self.pipelines.read().values().map(|rt| rt.info()).collect()
    }

    async fn create(
        &self,
        spec: PipelineSpec,
    ) -> Result<PipeInfo, PipelineAPIError> {
        let name = spec.metadata.name.clone();
        if self.pipelines.read().contains_key(&name) {
            return Err(PipelineAPIError::AlreadyExists(name));
        }

        let runtime = self
            .spawn_pipeline(spec)
            .await
            .map_err(PipelineAPIError::Failed)?;
        let info = runtime.info();
        self.pipelines.write().insert(name, runtime);
        Ok(info)
    }

    async fn patch(
        &self,
        name: &str,
        patch: Value,
    ) -> Result<PipeInfo, PipelineAPIError> {
        let base_spec = {
            let guard = self.pipelines.read();
            let Some(runtime) = guard.get(name) else {
                return Err(PipelineAPIError::NotFound(name.to_string()));
            };
            runtime.spec.clone()
        };

        let spec = merge_spec(&base_spec, patch)?;

        if spec.metadata.name != name {
            return Err(PipelineAPIError::NameMismatch {
                expected: name.to_string(),
                found: spec.metadata.name.clone(),
            });
        }

        let runtime = self
            .pipelines
            .write()
            .remove(name)
            .ok_or_else(|| PipelineAPIError::NotFound(name.to_string()))?;

        self.shutdown_runtime(runtime)
            .await
            .map_err(PipelineAPIError::Failed)?;

        let runtime = self
            .spawn_pipeline(spec)
            .await
            .map_err(PipelineAPIError::Failed)?;
        let info = runtime.info();
        self.pipelines.write().insert(name.to_string(), runtime);
        Ok(info)
    }

    async fn pause(&self, name: &str) -> Result<PipeInfo, PipelineAPIError> {
        let mut guard = self.pipelines.write();
        let Some(runtime) = guard.get_mut(name) else {
            return Err(PipelineAPIError::NotFound(name.to_string()));
        };

        runtime.pause();
        Ok(runtime.info())
    }

    async fn resume(&self, name: &str) -> Result<PipeInfo, PipelineAPIError> {
        let mut guard = self.pipelines.write();
        let Some(runtime) = guard.get_mut(name) else {
            return Err(PipelineAPIError::NotFound(name.to_string()));
        };

        runtime.resume();
        Ok(runtime.info())
    }

    async fn stop(&self, name: &str) -> Result<PipeInfo, PipelineAPIError> {
        let mut runtime = self
            .pipelines
            .write()
            .remove(name)
            .ok_or_else(|| PipelineAPIError::NotFound(name.to_string()))?;

        runtime.status = PipelineStatus::Stopped;
        let info = runtime.info();

        self.shutdown_runtime(runtime)
            .await
            .map_err(PipelineAPIError::Failed)?;
        Ok(info)
    }
}

fn merge_spec(
    base: &PipelineSpec,
    patch: Value,
) -> Result<PipelineSpec, PipelineAPIError> {
    let mut merged = serde_json::to_value(base)
        .map_err(|e| PipelineAPIError::Failed(e.into()))?;
    merge_values(&mut merged, patch);
    serde_json::from_value(merged).map_err(|e| PipelineAPIError::Failed(e.into()))
}

fn merge_values(base: &mut Value, patch: Value) {
    match (base, patch) {
        (Value::Object(base_map), Value::Object(patch_map)) => {
            for (key, value) in patch_map {
                match base_map.get_mut(&key) {
                    Some(base_value) => merge_values(base_value, value),
                    None => {
                        base_map.insert(key, value);
                    }
                }
            }
        }
        (base_slot, patch_value) => {
            *base_slot = patch_value;
        }
    }
}
