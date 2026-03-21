use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::coordinator::{
    Coordinator, SchemaSensorState, build_batch_processor, build_commit_fn,
};
use crate::schema_provider::SchemaLoaderAdapter;
use anyhow::{Context, Result};
use checkpoints::CheckpointStore;
use deltaforge_config::{PipelineSpec, SourceCfg};
use deltaforge_core::{Event, SourceError, SourceHandle};
use metrics::{counter, gauge};
use parking_lot::RwLock;
use processors::build_processors;
use rest_api::{PipeInfo, PipelineAPIError, PipelineController};
use serde_json::Value;
use sinks::build_sinks;
use sources::{ArcSchemaLoader, build_schema_loader, build_source};
use storage::{
    ArcStorageBackend, BackendCheckpointStore, DurableSchemaRegistry,
    MemoryStorageBackend,
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::info;

// ============================================================================
// Pipeline Runtime (internal)
// ============================================================================

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PipelineStatus {
    Running,
    Paused,
    Stopped,
}

impl PipelineStatus {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Stopped => "stopped",
        }
    }
}

pub(crate) struct PipelineRuntime {
    pub(crate) spec: PipelineSpec,
    pub(crate) status: PipelineStatus,
    /// Set to false by the coordinator task when it exits without cancellation
    /// (i.e. the source died unexpectedly). Used to drive /health.
    pub(crate) alive: Arc<AtomicBool>,
    pub(crate) cancel: CancellationToken,
    pub(crate) pause: watch::Sender<bool>,
    pub(crate) sources: Vec<SourceHandle>,
    pub(crate) join: Option<JoinHandle<Result<()>>>,
    pub(crate) schema_loader: Option<ArcSchemaLoader>,
    pub(crate) table_patterns: Vec<String>,
    pub(crate) sensor_state: Option<Arc<SchemaSensorState>>,
}

impl PipelineRuntime {
    pub(crate) fn pause(&mut self) {
        self.sources.iter().for_each(|s| s.pause());
        let _ = self.pause.send(true);
        self.status = PipelineStatus::Paused;
    }

    pub(crate) fn resume(&mut self) {
        self.sources.iter().for_each(|s| s.resume());
        let _ = self.pause.send(false);
        self.status = PipelineStatus::Running;
    }

    pub(crate) fn info(&self) -> PipeInfo {
        let status = if !self.alive.load(Ordering::Acquire) {
            "failed"
        } else {
            self.status.as_str()
        };
        PipeInfo {
            name: self.spec.metadata.name.clone(),
            status: status.to_string(),
            spec: self.spec.clone(),
        }
    }
}

// ============================================================================
// Pipeline Manager
// ============================================================================

#[derive(Clone)]
pub struct PipelineManager {
    pub(crate) pipelines: Arc<RwLock<HashMap<String, PipelineRuntime>>>,
    pub(crate) ckpt_store: Arc<dyn CheckpointStore>,
    pub(crate) registry: Arc<DurableSchemaRegistry>,
    pub(crate) backend: ArcStorageBackend,
}

impl PipelineManager {
    /// Production constructor - wires a `StorageBackend` into both
    /// checkpoint and schema registry subsystems. Replays the schema log
    /// on startup so the cache is warm before any pipeline starts.
    pub async fn with_backend(backend: ArcStorageBackend) -> Result<Self> {
        let ckpt_store: Arc<dyn CheckpointStore> =
            Arc::new(BackendCheckpointStore::new(Arc::clone(&backend)));
        let registry = DurableSchemaRegistry::new(Arc::clone(&backend)).await?;

        Ok(Self {
            pipelines: Arc::new(RwLock::new(HashMap::new())),
            ckpt_store,
            registry,
            backend,
        })
    }

    /// In-memory constructor for tests - no persistence.
    pub fn for_testing() -> Self {
        let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
        let ckpt_store: Arc<dyn CheckpointStore> =
            Arc::new(BackendCheckpointStore::new(Arc::clone(&backend)));
        let registry = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(DurableSchemaRegistry::new(Arc::clone(&backend)))
                .expect("memory DurableSchemaRegistry never fails")
        });
        Self {
            pipelines: Arc::new(RwLock::new(HashMap::new())),
            ckpt_store,
            registry,
            backend,
        }
    }

    /// Access the schema registry (for version lookups).
    pub fn registry(&self) -> &Arc<DurableSchemaRegistry> {
        &self.registry
    }

    /// Get schema loader for a pipeline.
    pub fn get_loader(
        &self,
        pipeline: &str,
    ) -> Result<ArcSchemaLoader, PipelineAPIError> {
        self.pipelines
            .read()
            .get(pipeline)
            .ok_or_else(|| PipelineAPIError::NotFound(pipeline.to_string()))?
            .schema_loader
            .clone()
            .ok_or_else(|| {
                PipelineAPIError::Failed(anyhow::anyhow!("no schema loader"))
            })
    }

    /// Get sensor state for a pipeline.
    pub fn get_sensor(
        &self,
        pipeline: &str,
    ) -> Result<Arc<SchemaSensorState>, PipelineAPIError> {
        self.pipelines
            .read()
            .get(pipeline)
            .ok_or_else(|| PipelineAPIError::NotFound(pipeline.to_string()))?
            .sensor_state
            .clone()
            .ok_or_else(|| {
                PipelineAPIError::Failed(anyhow::anyhow!(
                    "schema sensing not enabled for this pipeline"
                ))
            })
    }

    async fn spawn_pipeline(
        &self,
        spec: PipelineSpec,
    ) -> Result<PipelineRuntime> {
        let pipeline_name = spec.metadata.name.clone();
        counter!("deltaforge_pipelines_total").increment(1);

        // Create cancellation token early so it can be shared with sinks
        let cancel = CancellationToken::new();

        let source = build_source(
            &spec,
            self.registry.clone(),
            Arc::clone(&self.backend),
        )
        .context("build source")?;
        let processors = build_processors(&spec).context("build processors")?;
        let sinks =
            build_sinks(&spec, cancel.clone()).context("build sinks")?;
        let schema_loader = build_schema_loader(&spec, self.registry.clone());

        let table_patterns = match &spec.spec.source {
            SourceCfg::Mysql(c) => c.tables.clone(),
            SourceCfg::Postgres(_) => vec![],
            #[cfg(feature = "turso")]
            SourceCfg::Turso(c) => c.tables.clone(),
        };

        let alive = Arc::new(AtomicBool::new(true));

        let (event_tx, event_rx) = mpsc::channel::<Event>(4096);
        let src_handle = source.run(event_tx, self.ckpt_store.clone()).await;

        // Wrap the source JoinHandle so alive=false is set immediately when
        // the source task dies without an explicit cancellation.  The
        // coordinator may be blocked in a long I/O operation (Kafka flush,
        // SQLite commit) and never return, so we cannot rely solely on the
        // coordinator wrapper below to drive /health.
        let alive_for_src = Arc::clone(&alive);
        let cancel_for_src = cancel.clone();
        let SourceHandle {
            cancel: src_cancel,
            paused: src_paused,
            pause_notify: src_pause_notify,
            join: raw_join,
        } = src_handle;
        let monitored_join = tokio::spawn(async move {
            let res = raw_join.await;
            if !cancel_for_src.is_cancelled() {
                alive_for_src.store(false, Ordering::Release);
            }
            match res {
                Ok(r) => r,
                Err(e) => Err(SourceError::Other(anyhow::anyhow!(
                    "source task panicked: {e}"
                ))),
            }
        });
        let src_handle = SourceHandle {
            cancel: src_cancel,
            paused: src_paused,
            pause_notify: src_pause_notify,
            join: monitored_join,
        };

        let batch_processor =
            build_batch_processor(processors, pipeline_name.clone());
        let commit_cp = build_commit_fn(
            self.ckpt_store.clone(),
            spec.spec.source.source_id().to_string(),
        );

        let (pause_tx, pause_rx) = watch::channel(false);

        // Schema sensing
        let sensing_cfg = spec.spec.schema_sensing.clone();
        let sensor = sensing_cfg
            .enabled
            .then(|| Arc::new(SchemaSensorState::new(sensing_cfg)));

        let mut builder = Coordinator::builder(pipeline_name.clone())
            .sinks(sinks)
            .batch_config(spec.spec.batch.clone())
            .commit_policy(spec.spec.commit_policy.clone())
            .commit_fn(commit_cp)
            .process_fn(batch_processor);

        let sensor_for_runtime = sensor.clone();
        if let Some(s) = sensor {
            builder = builder.schema_sensor(s);
        }

        if let Some(loader) = &schema_loader {
            let provider = Arc::new(SchemaLoaderAdapter::new(loader.clone()));
            builder = builder.schema_provider(provider);
        }

        let coord = builder.build();
        let cancel_for_task = cancel.clone();
        let cancel_check = cancel.clone();
        let pname = pipeline_name.clone();

        let alive_for_task = Arc::clone(&alive);

        let join = tokio::spawn(async move {
            let result = coord.run(event_rx, cancel_for_task, pause_rx).await;
            if !cancel_check.is_cancelled() {
                // Coordinator exited without an explicit stop — also mark
                // failed (covers errors that originate inside the coordinator
                // itself rather than in the source task).
                alive_for_task.store(false, Ordering::Release);
                gauge!("deltaforge_pipeline_status", "pipeline" => pname.clone())
                    .set(-1.0);
            }
            info!(pipeline = %pname, "pipeline coordinator exited");
            result
        });

        gauge!("deltaforge_pipeline_status", "pipeline" => pipeline_name)
            .set(1.0);

        Ok(PipelineRuntime {
            spec,
            status: PipelineStatus::Running,
            alive,
            cancel,
            pause: pause_tx,
            sources: vec![src_handle],
            join: Some(join),
            schema_loader,
            table_patterns,
            sensor_state: sensor_for_runtime,
        })
    }

    pub async fn start_pipeline(
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

    pub async fn stop_pipeline(
        &self,
        name: &str,
    ) -> Result<(), PipelineAPIError> {
        let mut runtime = self
            .pipelines
            .write()
            .remove(name)
            .ok_or_else(|| PipelineAPIError::NotFound(name.to_string()))?;

        runtime.cancel.cancel();
        for src in &runtime.sources {
            src.cancel.cancel();
        }

        if let Some(join) = runtime.join.take() {
            let _ = join.await;
        }
        for src in runtime.sources {
            let _ = src.join.await;
        }

        gauge!("deltaforge_pipeline_status", "pipeline" => name.to_string())
            .set(0.0);
        Ok(())
    }

    pub fn list_pipelines(&self) -> Vec<PipeInfo> {
        self.pipelines.read().values().map(|r| r.info()).collect()
    }

    pub fn get_pipeline(&self, name: &str) -> Option<PipeInfo> {
        self.pipelines.read().get(name).map(|r| r.info())
    }
}

// ============================================================================
// PipelineController
// ============================================================================

#[async_trait::async_trait]
impl PipelineController for PipelineManager {
    async fn list(&self) -> Vec<PipeInfo> {
        self.list_pipelines()
    }

    async fn get(&self, name: &str) -> Result<PipeInfo, PipelineAPIError> {
        self.get_pipeline(name)
            .ok_or_else(|| PipelineAPIError::NotFound(name.to_string()))
    }

    async fn create(
        &self,
        spec: PipelineSpec,
    ) -> Result<PipeInfo, PipelineAPIError> {
        self.start_pipeline(spec).await
    }

    async fn patch(
        &self,
        name: &str,
        patch: Value,
    ) -> Result<PipeInfo, PipelineAPIError> {
        let old_spec = self
            .pipelines
            .read()
            .get(name)
            .ok_or_else(|| PipelineAPIError::NotFound(name.to_string()))?
            .spec
            .clone();

        let new_spec = merge_spec(old_spec, patch)?;
        self.stop_pipeline(name).await?;
        self.start_pipeline(new_spec).await
    }

    async fn pause(&self, name: &str) -> Result<PipeInfo, PipelineAPIError> {
        let mut guard = self.pipelines.write();
        let runtime = guard
            .get_mut(name)
            .ok_or_else(|| PipelineAPIError::NotFound(name.to_string()))?;
        runtime.pause();
        gauge!("deltaforge_pipeline_status", "pipeline" => name.to_string())
            .set(0.5);
        Ok(runtime.info())
    }

    async fn resume(&self, name: &str) -> Result<PipeInfo, PipelineAPIError> {
        let mut guard = self.pipelines.write();
        let runtime = guard
            .get_mut(name)
            .ok_or_else(|| PipelineAPIError::NotFound(name.to_string()))?;
        runtime.resume();
        gauge!("deltaforge_pipeline_status", "pipeline" => name.to_string())
            .set(1.0);
        Ok(runtime.info())
    }

    async fn stop(&self, name: &str) -> Result<PipeInfo, PipelineAPIError> {
        let info = self
            .pipelines
            .read()
            .get(name)
            .ok_or_else(|| PipelineAPIError::NotFound(name.to_string()))?
            .info();
        self.stop_pipeline(name).await?;
        Ok(info)
    }

    async fn delete(&self, name: &str) -> Result<(), PipelineAPIError> {
        self.stop_pipeline(name).await
    }
}

// ============================================================================
// Helpers
// ============================================================================

fn merge_spec(
    base: PipelineSpec,
    patch: Value,
) -> Result<PipelineSpec, PipelineAPIError> {
    let mut merged = serde_json::to_value(&base)
        .map_err(|e| PipelineAPIError::Failed(e.into()))?;
    merge_values(&mut merged, patch);
    serde_json::from_value(merged)
        .map_err(|e| PipelineAPIError::Failed(e.into()))
}

fn merge_values(base: &mut Value, patch: Value) {
    match (base, patch) {
        (Value::Object(b), Value::Object(p)) => {
            for (k, v) in p {
                match b.get_mut(&k) {
                    Some(bv) => merge_values(bv, v),
                    None => {
                        b.insert(k, v);
                    }
                }
            }
        }
        (b, p) => *b = p,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltaforge_config::{
        BatchConfig, Metadata, MysqlSrcCfg, RedisSinkCfg, SinkCfg, SnapshotCfg,
        SourceCfg, Spec,
    };

    fn sample_spec(name: &str) -> PipelineSpec {
        PipelineSpec {
            metadata: Metadata {
                name: name.to_string(),
                tenant: "acme".to_string(),
            },
            spec: Spec {
                sharding: None,
                source: SourceCfg::Mysql(MysqlSrcCfg {
                    id: "mysql".to_string(),
                    dsn: "mysql://root:root@localhost/db".to_string(),
                    tables: vec![],
                    outbox: None,
                    snapshot: SnapshotCfg::default(),
                    on_schema_drift: deltaforge_config::OnSchemaDrift::Adapt,
                }),
                processors: vec![],
                sinks: vec![SinkCfg::Redis(RedisSinkCfg {
                    id: "redis".to_string(),
                    uri: "redis://localhost".to_string(),
                    stream: "events".to_string(),
                    key: None,
                    required: Some(true),
                    send_timeout_secs: None,
                    batch_timeout_secs: None,
                    connect_timeout_secs: None,
                    envelope: deltaforge_config::EnvelopeCfg::Debezium,
                    encoding: deltaforge_config::EncodingCfg::Json,
                    filter: None,
                })],
                connection_policy: None,
                batch: Some(BatchConfig::default()),
                commit_policy: None,
                schema_sensing: Default::default(),
            },
        }
    }

    #[test]
    fn merge_spec_overlays_nested() {
        let base = sample_spec("p1");
        let patch =
            serde_json::json!({"spec": {"batch": {"max_events": 2000}}});
        let merged = merge_spec(base, patch).unwrap();
        assert_eq!(merged.spec.batch.as_ref().unwrap().max_events, Some(2000));
    }
}
