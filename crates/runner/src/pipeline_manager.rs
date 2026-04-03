use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::coordinator::{
    Coordinator, SchemaSensorState, build_batch_processor, build_commit_fn,
};
use crate::schema_provider::SchemaLoaderAdapter;
use anyhow::{Context, Result};
use async_trait::async_trait;
use checkpoints::{CheckpointResult, CheckpointStore};

// ── Per-sink checkpoint proxy ────────────────────────────────────────────────

/// Comparison function for opaque checkpoint bytes.
/// Each source provides its own implementation via `Source::compare_checkpoints`.
type CheckpointCmpFn =
    Arc<dyn Fn(&[u8], &[u8]) -> std::cmp::Ordering + Send + Sync>;

/// Wraps a [`CheckpointStore`] to present the **minimum** per-sink checkpoint
/// when the source calls `get_raw(source_id)`.
///
/// Per-sink checkpoints are stored as `"{source_id}::sink::{sink_id}"`.
/// On `get_raw(source_id)`, this wrapper reads all per-sink keys and returns
/// the smallest (earliest) checkpoint so the source replays from the position
/// that the slowest sink needs.
///
/// Uses the source-provided comparison function for correctness — different
/// sources have different checkpoint formats (MySQL file:pos, Postgres LSN,
/// Turso change_id) that cannot be compared lexicographically.
struct PerSinkCheckpointProxy {
    inner: Arc<dyn CheckpointStore>,
    source_id: String,
    cmp_fn: CheckpointCmpFn,
}

#[async_trait]
impl CheckpointStore for PerSinkCheckpointProxy {
    async fn get_raw(&self, key: &str) -> CheckpointResult<Option<Vec<u8>>> {
        if key == self.source_id {
            let prefix = format!("{}::sink::", self.source_id);
            let keys = self.inner.list_with_prefix(&prefix).await?;
            if keys.is_empty() {
                // Fallback: check legacy checkpoint key (pre per-sink format).
                // This allows seamless migration — old pipelines that saved
                // checkpoints under the plain source_id key still work.
                return self.inner.get_raw(key).await;
            }
            let mut min_cp: Option<Vec<u8>> = None;
            for k in &keys {
                if let Some(data) = self.inner.get_raw(k).await? {
                    min_cp = Some(match min_cp {
                        None => data,
                        Some(prev) => {
                            if (self.cmp_fn)(&data, &prev)
                                == std::cmp::Ordering::Less
                            {
                                data
                            } else {
                                prev
                            }
                        }
                    });
                }
            }
            return Ok(min_cp);
        }
        self.inner.get_raw(key).await
    }

    async fn put_raw(&self, key: &str, bytes: &[u8]) -> CheckpointResult<()> {
        self.inner.put_raw(key, bytes).await
    }

    async fn delete(&self, key: &str) -> CheckpointResult<bool> {
        self.inner.delete(key).await
    }

    async fn list(&self) -> CheckpointResult<Vec<String>> {
        self.inner.list().await
    }
}
use deltaforge_config::{PipelineSpec, SourceCfg};
use deltaforge_core::{Event, SourceError, SourceHandle};
use metrics::{counter, gauge};
use parking_lot::RwLock;
use processors::build_processors;
use rest_api::{PipeInfo, PipelineAPIError, PipelineController};
use serde_json::Value;
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

use crate::schema_provider::AvroSchemaProviderImpl;
use deltaforge_core::encoding::avro::SourceSchemaProvider;
use deltaforge_core::encoding::avro_types::TypeConversionOpts;

/// Build an Avro source schema provider if any sink uses Avro encoding.
///
/// Returns `None` if no sinks use Avro — zero overhead in that case.
fn build_avro_provider(
    spec: &PipelineSpec,
    schema_loader: &Option<ArcSchemaLoader>,
) -> Option<Arc<dyn SourceSchemaProvider>> {
    use deltaforge_config::{EncodingCfg, SinkCfg};

    // Check if any sink uses Avro encoding
    let has_avro = spec.spec.sinks.iter().any(|s| {
        let encoding = match s {
            SinkCfg::Kafka(c) => &c.encoding,
            SinkCfg::Redis(c) => &c.encoding,
            SinkCfg::Nats(c) => &c.encoding,
            SinkCfg::Http(c) => &c.encoding,
        };
        matches!(encoding, EncodingCfg::Avro { .. })
    });

    if !has_avro {
        return None;
    }

    let loader = schema_loader.as_ref()?;

    let connector = match &spec.spec.source {
        SourceCfg::Mysql(_) => "mysql",
        SourceCfg::Postgres(_) => "postgresql",
        #[cfg(feature = "turso")]
        SourceCfg::Turso(_) => "turso",
    };

    let schema_provider =
        Arc::new(SchemaLoaderAdapter::new(Arc::clone(loader)));

    Some(Arc::new(AvroSchemaProviderImpl::new(
        schema_provider,
        connector,
        TypeConversionOpts::default(),
    )))
}

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
    pub(crate) dlq_writer: Option<Arc<crate::dlq::DlqWriter>>,
    pub(crate) started_at: std::time::Instant,
}

impl PipelineRuntime {
    pub(crate) fn pause(&mut self) {
        self.sources.iter().for_each(|s| s.pause());
        let _ = self.pause.send(true);
        self.status = PipelineStatus::Paused;
        counter!(
            "deltaforge_pipeline_pauses_total",
            "pipeline" => self.spec.metadata.name.clone()
        )
        .increment(1);
    }

    pub(crate) fn resume(&mut self) {
        self.sources.iter().for_each(|s| s.resume());
        let _ = self.pause.send(false);
        self.status = PipelineStatus::Running;
        counter!(
            "deltaforge_pipeline_resumes_total",
            "pipeline" => self.spec.metadata.name.clone()
        )
        .increment(1);
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
            ops: None, // populated async by controller.get()
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
    /// Get the DLQ writer for a pipeline. Clones the Arc so the RwLock guard
    /// is released before any async calls.
    fn get_dlq_writer(
        &self,
        name: &str,
    ) -> Result<Arc<crate::dlq::DlqWriter>, PipelineAPIError> {
        let guard = self.pipelines.read();
        let runtime = guard
            .get(name)
            .ok_or_else(|| PipelineAPIError::NotFound(name.to_string()))?;
        runtime.dlq_writer.as_ref().cloned().ok_or_else(|| {
            PipelineAPIError::Failed(anyhow::anyhow!(
                "DLQ not enabled for pipeline '{}'",
                name
            ))
        })
    }

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
        let processors = build_processors(&spec, &pipeline_name)
            .context("build processors")?;
        let schema_loader = build_schema_loader(&spec, self.registry.clone());

        // Build Avro schema provider if any sink uses Avro encoding
        let avro_source_schemas = build_avro_provider(&spec, &schema_loader);

        let sinks = sinks::build_sinks_with_schemas(
            &spec,
            cancel.clone(),
            &pipeline_name,
            avro_source_schemas,
        )
        .context("build sinks")?;

        let table_patterns = match &spec.spec.source {
            SourceCfg::Mysql(c) => c.tables.clone(),
            SourceCfg::Postgres(_) => vec![],
            #[cfg(feature = "turso")]
            SourceCfg::Turso(c) => c.tables.clone(),
        };

        let alive = Arc::new(AtomicBool::new(true));

        let (event_tx, event_rx) = mpsc::channel::<Event>(32_768);
        // Wrap checkpoint store so the source reads the minimum per-sink
        // checkpoint — it replays from the position the slowest sink needs.
        // Capture the source's checkpoint comparison function for the proxy.
        let source_ref = Arc::clone(&source);
        let cmp_fn: CheckpointCmpFn =
            Arc::new(move |a, b| source_ref.compare_checkpoints(a, b));
        let source_ckpt: Arc<dyn CheckpointStore> =
            Arc::new(PerSinkCheckpointProxy {
                inner: self.ckpt_store.clone(),
                source_id: spec.spec.source.source_id().to_string(),
                cmp_fn,
            });
        let src_handle = source.run(event_tx, source_ckpt).await;

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

        // Build per-sink checkpoint commit functions.
        // Each sink gets its own checkpoint key: "{source_id}::sink::{sink_id}".
        let source_id = spec.spec.source.source_id().to_string();

        let (pause_tx, pause_rx) = watch::channel(false);

        // Schema sensing
        let sensing_cfg = spec.spec.schema_sensing.clone();
        let sensor = sensing_cfg
            .enabled
            .then(|| Arc::new(SchemaSensorState::new(sensing_cfg)));

        let mut builder = Coordinator::builder(pipeline_name.clone())
            .sinks(sinks.clone())
            .batch_config(spec.spec.batch.clone())
            .commit_policy(spec.spec.commit_policy.clone())
            .process_fn(batch_processor);

        for sink in &sinks {
            let sink_id = sink.id().to_string();
            let cp_key = format!("{}::sink::{}", source_id, sink_id);
            let commit_fn = build_commit_fn(self.ckpt_store.clone(), cp_key);
            builder = builder.commit_fn(sink_id, commit_fn);
        }

        let sensor_for_runtime = sensor.clone();
        if let Some(s) = sensor {
            builder = builder.schema_sensor(s);
        }

        if let Some(loader) = &schema_loader {
            let provider = Arc::new(SchemaLoaderAdapter::new(loader.clone()));
            builder = builder.schema_provider(provider);
        }

        // DLQ writer — opt-in via journal config.
        let dlq_writer = if spec
            .spec
            .journal
            .as_ref()
            .map(|j| j.enabled)
            .unwrap_or(false)
        {
            let journal_cfg = spec.spec.journal.clone().unwrap();
            let writer = Arc::new(crate::dlq::DlqWriter::new(
                self.backend.clone(),
                pipeline_name.clone(),
                journal_cfg.dlq.clone(),
                journal_cfg.max_event_bytes,
            ));
            builder = builder.dlq_writer(Arc::clone(&writer));
            // Spawn background cleanup for max_age expiry.
            let _cleanup_handle = writer.spawn_cleanup_task();
            tracing::info!(
                pipeline = %pipeline_name,
                max_entries = journal_cfg.dlq.max_entries,
                max_age_secs = journal_cfg.dlq.max_age_secs,
                "DLQ enabled with background cleanup"
            );
            Some(writer)
        } else {
            None
        };

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

        gauge!("deltaforge_pipeline_status", "pipeline" => pipeline_name.clone())
            .set(1.0);

        // Emit pipeline info metric with labels for Grafana joins.
        // This is a constant gauge (always 1) that carries metadata as labels.
        let tenant = spec.metadata.tenant.clone();
        let mut info_labels = vec![
            ("pipeline".to_string(), pipeline_name.clone()),
            ("tenant".to_string(), tenant),
        ];
        for (k, v) in &spec.metadata.labels {
            info_labels.push((k.clone(), v.clone()));
        }
        // Build gauge with dynamic labels — use the pipeline + tenant as fixed,
        // and emit user labels as part of the metric name context.
        gauge!(
            "deltaforge_pipeline_info",
            "pipeline" => pipeline_name.clone(),
            "tenant" => spec.metadata.tenant.clone(),
        )
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
            dlq_writer,
            started_at: std::time::Instant::now(),
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
        let (mut info, uptime) = {
            let guard = self.pipelines.read();
            let runtime = guard
                .get(name)
                .ok_or_else(|| PipelineAPIError::NotFound(name.to_string()))?;
            (runtime.info(), runtime.started_at.elapsed().as_secs_f64())
        };

        // Enrich with operational status.
        let checkpoints = self.checkpoints(name).await.unwrap_or_default();
        let dlq_count = match self.get_dlq_writer(name) {
            Ok(dlq) => dlq.len().await.unwrap_or(0),
            Err(_) => 0,
        };

        info.ops = Some(rest_api::pipelines::PipelineOpsStatus {
            lag_seconds: None,
            dlq_entries: dlq_count,
            sink_errors: Default::default(),
            uptime_seconds: Some(uptime),
            checkpoints,
        });

        Ok(info)
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

        let new_spec = merge_spec(old_spec.clone(), patch)?;

        // Clean up per-sink checkpoints for removed sinks.
        let source_id = old_spec.spec.source.source_id();
        let old_sink_ids: std::collections::HashSet<&str> =
            old_spec.spec.sinks.iter().map(|s| s.sink_id()).collect();
        let new_sink_ids: std::collections::HashSet<&str> =
            new_spec.spec.sinks.iter().map(|s| s.sink_id()).collect();

        for removed in old_sink_ids.difference(&new_sink_ids) {
            let cp_key = format!("{}::sink::{}", source_id, removed);
            if let Err(e) = self.ckpt_store.delete(&cp_key).await {
                tracing::warn!(
                    pipeline = %name,
                    sink = %removed,
                    error = %e,
                    "failed to clean up checkpoint for removed sink"
                );
            } else {
                tracing::info!(
                    pipeline = %name,
                    sink = %removed,
                    "cleaned up per-sink checkpoint for removed sink"
                );
            }
        }

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
        let status = self
            .pipelines
            .read()
            .get(name)
            .ok_or_else(|| PipelineAPIError::NotFound(name.to_string()))?
            .status;

        match status {
            PipelineStatus::Paused => {
                let mut guard = self.pipelines.write();
                let runtime = guard.get_mut(name).ok_or_else(|| {
                    PipelineAPIError::NotFound(name.to_string())
                })?;
                runtime.resume();
                gauge!("deltaforge_pipeline_status", "pipeline" => name.to_string())
                    .set(1.0);
                Ok(runtime.info())
            }
            PipelineStatus::Stopped => {
                // Re-spawn from the stored spec and replace the stopped runtime.
                let spec = self
                    .pipelines
                    .read()
                    .get(name)
                    .expect("runtime was not removed")
                    .spec
                    .clone();
                let new_runtime = self
                    .spawn_pipeline(spec)
                    .await
                    .map_err(PipelineAPIError::Failed)?;
                let info = new_runtime.info();
                self.pipelines.write().insert(name.to_string(), new_runtime);
                Ok(info)
            }
            PipelineStatus::Running => Ok(self
                .pipelines
                .read()
                .get(name)
                .expect("runtime was not removed")
                .info()),
        }
    }

    async fn stop(&self, name: &str) -> Result<PipeInfo, PipelineAPIError> {
        // Cancel tasks and clear handles, but keep the runtime in the registry
        // so the pipeline can be resumed later.
        let (cancel, sources, join) = {
            let mut guard = self.pipelines.write();
            let runtime = guard
                .get_mut(name)
                .ok_or_else(|| PipelineAPIError::NotFound(name.to_string()))?;

            if runtime.status == PipelineStatus::Stopped {
                return Ok(runtime.info());
            }

            runtime.status = PipelineStatus::Stopped;
            let cancel = runtime.cancel.clone();
            let sources = std::mem::take(&mut runtime.sources);
            let join = runtime.join.take();
            (cancel, sources, join)
        };

        cancel.cancel();
        for src in &sources {
            src.cancel.cancel();
        }
        // Update the gauge immediately — the status is already Stopped in the
        // registry. Don't wait for the join handles; the source task may be
        // stuck in a TCP read that is slow to notice cancellation.
        gauge!("deltaforge_pipeline_status", "pipeline" => name.to_string())
            .set(0.0);
        // Await cleanup in the background so the HTTP handler returns promptly.
        tokio::spawn(async move {
            if let Some(j) = join {
                let _ = j.await;
            }
            for src in sources {
                let _ = src.join.await;
            }
        });

        Ok(self
            .pipelines
            .read()
            .get(name)
            .expect("runtime was not removed")
            .info())
    }

    async fn delete(&self, name: &str) -> Result<(), PipelineAPIError> {
        // Capture source_id before stopping (spec is removed by stop).
        let source_id = self
            .pipelines
            .read()
            .get(name)
            .map(|r| r.spec.spec.source.source_id().to_string());

        self.stop_pipeline(name).await?;

        // Clean up all per-sink checkpoints for this pipeline.
        if let Some(source_id) = source_id {
            let prefix = format!("{}::sink::", source_id);
            if let Ok(keys) = self.ckpt_store.list_with_prefix(&prefix).await {
                for key in keys {
                    if let Err(e) = self.ckpt_store.delete(&key).await {
                        tracing::warn!(
                            pipeline = %name,
                            key = %key,
                            error = %e,
                            "failed to clean up per-sink checkpoint on delete"
                        );
                    }
                }
                if !prefix.is_empty() {
                    tracing::info!(
                        pipeline = %name,
                        "cleaned up per-sink checkpoints on delete"
                    );
                }
            }
        }

        Ok(())
    }

    // ── DLQ controller methods ───────────────────────────────────────────

    async fn dlq_peek(
        &self,
        name: &str,
        limit: usize,
    ) -> Result<Vec<serde_json::Value>, PipelineAPIError> {
        let dlq = self.get_dlq_writer(name)?;
        let entries =
            dlq.peek(limit).await.map_err(PipelineAPIError::Failed)?;
        entries
            .into_iter()
            .map(|e| {
                serde_json::to_value(e)
                    .map_err(|e| PipelineAPIError::Failed(e.into()))
            })
            .collect()
    }

    async fn dlq_count(&self, name: &str) -> Result<u64, PipelineAPIError> {
        let dlq = self.get_dlq_writer(name)?;
        dlq.len().await.map_err(PipelineAPIError::Failed)
    }

    async fn dlq_ack(
        &self,
        name: &str,
        up_to_seq: u64,
    ) -> Result<usize, PipelineAPIError> {
        let dlq = self.get_dlq_writer(name)?;
        dlq.ack(up_to_seq).await.map_err(PipelineAPIError::Failed)
    }

    async fn dlq_purge(&self, name: &str) -> Result<usize, PipelineAPIError> {
        let dlq = self.get_dlq_writer(name)?;
        dlq.purge().await.map_err(PipelineAPIError::Failed)
    }

    async fn checkpoints(
        &self,
        name: &str,
    ) -> Result<Vec<rest_api::pipelines::CheckpointInfo>, PipelineAPIError>
    {
        let (source_id, prefix) = {
            let guard = self.pipelines.read();
            let runtime = guard
                .get(name)
                .ok_or_else(|| PipelineAPIError::NotFound(name.to_string()))?;
            let sid = runtime.spec.spec.source.source_id().to_string();
            let pfx = format!("{}::sink::", sid);
            (sid, pfx)
        }; // guard dropped here
        let _ = source_id; // used for future expansion

        let keys = self
            .ckpt_store
            .list_with_prefix(&prefix)
            .await
            .map_err(|e| PipelineAPIError::Failed(e.into()))?;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);

        let mut result = Vec::with_capacity(keys.len());
        for key in &keys {
            let sink_id = key.strip_prefix(&prefix).unwrap_or(key).to_string();

            let position = match self.ckpt_store.get_raw(key).await {
                Ok(Some(bytes)) => serde_json::from_slice(&bytes)
                    .unwrap_or(serde_json::Value::Null),
                _ => serde_json::Value::Null,
            };

            // Checkpoint age: time since last write. We use the checkpoint
            // timestamp if available, otherwise report 0.
            let age = position
                .get("ts_ms")
                .and_then(|v| v.as_f64())
                .map(|ts| now - ts / 1000.0)
                .unwrap_or(0.0);

            result.push(rest_api::pipelines::CheckpointInfo {
                sink_id,
                position,
                age_seconds: age,
            });
        }

        Ok(result)
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
        // Deep-merge arrays element-wise by index. Patch elements that are
        // objects are merged into the corresponding base element; non-object
        // patch elements replace the base element. If the patch array is
        // longer than the base, extra elements are appended.
        (Value::Array(b), Value::Array(p)) => {
            for (i, pv) in p.into_iter().enumerate() {
                if i < b.len() {
                    merge_values(&mut b[i], pv);
                } else {
                    b.push(pv);
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
                labels: Default::default(),
                annotations: Default::default(),
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
                journal: None,
            },
        }
    }

    // ── Per-sink checkpoint proxy tests ─────────────────────────────────

    /// Test comparison function: parses `{"pos": N}` and compares numerically.
    fn test_cmp_fn() -> CheckpointCmpFn {
        Arc::new(|a: &[u8], b: &[u8]| {
            #[derive(serde::Deserialize)]
            struct Cp {
                pos: u64,
            }
            let a: Cp = serde_json::from_slice(a).unwrap_or(Cp { pos: 0 });
            let b: Cp = serde_json::from_slice(b).unwrap_or(Cp { pos: 0 });
            a.pos.cmp(&b.pos)
        })
    }

    #[tokio::test]
    async fn per_sink_proxy_returns_none_when_no_checkpoints() {
        let store = Arc::new(checkpoints::MemCheckpointStore::new().unwrap());
        let proxy = PerSinkCheckpointProxy {
            inner: store,
            source_id: "mysql".to_string(),
            cmp_fn: test_cmp_fn(),
        };
        // No per-sink checkpoints and no legacy key — fresh start.
        let result = proxy.get_raw("mysql").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn per_sink_proxy_falls_back_to_legacy_key() {
        let store = Arc::new(checkpoints::MemCheckpointStore::new().unwrap());
        // Write a legacy checkpoint under the plain source_id key (pre per-sink format).
        store.put_raw("mysql", b"{\"pos\":500}").await.unwrap();
        let proxy = PerSinkCheckpointProxy {
            inner: store,
            source_id: "mysql".to_string(),
            cmp_fn: test_cmp_fn(),
        };
        // No per-sink keys exist, so it should fall back to the legacy key.
        let result = proxy.get_raw("mysql").await.unwrap().unwrap();
        assert_eq!(result, b"{\"pos\":500}");
    }

    #[tokio::test]
    async fn per_sink_proxy_returns_min_checkpoint() {
        let store = Arc::new(checkpoints::MemCheckpointStore::new().unwrap());

        // Write per-sink checkpoints with different positions.
        // Using simple JSON strings — lexicographic comparison works for these.
        store
            .put_raw("mysql::sink::kafka", b"{\"pos\":200}")
            .await
            .unwrap();
        store
            .put_raw("mysql::sink::redis", b"{\"pos\":100}")
            .await
            .unwrap();
        store
            .put_raw("mysql::sink::nats", b"{\"pos\":300}")
            .await
            .unwrap();

        let proxy = PerSinkCheckpointProxy {
            inner: store,
            source_id: "mysql".to_string(),
            cmp_fn: test_cmp_fn(),
        };

        // Should return the minimum (redis at pos 100).
        let result = proxy.get_raw("mysql").await.unwrap().unwrap();
        assert_eq!(result, b"{\"pos\":100}");
    }

    #[tokio::test]
    async fn per_sink_proxy_passes_through_other_keys() {
        let store = Arc::new(checkpoints::MemCheckpointStore::new().unwrap());
        store.put_raw("other-key", b"other-value").await.unwrap();

        let proxy = PerSinkCheckpointProxy {
            inner: store,
            source_id: "mysql".to_string(),
            cmp_fn: test_cmp_fn(),
        };

        // Non-source-id keys pass through directly.
        let result = proxy.get_raw("other-key").await.unwrap().unwrap();
        assert_eq!(result, b"other-value");
    }

    #[tokio::test]
    async fn per_sink_proxy_single_sink_returns_that_checkpoint() {
        let store = Arc::new(checkpoints::MemCheckpointStore::new().unwrap());
        store
            .put_raw("mysql::sink::kafka", b"{\"pos\":500}")
            .await
            .unwrap();

        let proxy = PerSinkCheckpointProxy {
            inner: store,
            source_id: "mysql".to_string(),
            cmp_fn: test_cmp_fn(),
        };

        let result = proxy.get_raw("mysql").await.unwrap().unwrap();
        assert_eq!(result, b"{\"pos\":500}");
    }

    // ── SinkCfg::sink_id tests ──────────────────────────────────────────

    #[test]
    fn sink_cfg_returns_correct_id() {
        let kafka = SinkCfg::Redis(RedisSinkCfg {
            id: "my-redis".to_string(),
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
        });
        assert_eq!(kafka.sink_id(), "my-redis");
    }

    // ── Existing tests ──────────────────────────────────────────────────

    #[test]
    fn merge_spec_overlays_nested() {
        let base = sample_spec("p1");
        let patch =
            serde_json::json!({"spec": {"batch": {"max_events": 2000}}});
        let merged = merge_spec(base, patch).unwrap();
        assert_eq!(merged.spec.batch.as_ref().unwrap().max_events, Some(2000));
    }

    #[test]
    fn merge_values_deep_merges_arrays() {
        let mut base = serde_json::json!({
            "sinks": [
                {"type": "kafka", "config": {"id": "k1", "brokers": "localhost:9092", "topic": "t1"}},
                {"type": "redis", "config": {"id": "r1", "uri": "redis://localhost"}}
            ]
        });
        let patch = serde_json::json!({
            "sinks": [
                {"config": {"client_conf": {"linger.ms": "20"}}}
            ]
        });
        merge_values(&mut base, patch);
        // First sink should have client_conf merged in, keeping type/id/brokers/topic.
        let first = &base["sinks"][0];
        assert_eq!(first["type"], "kafka");
        assert_eq!(first["config"]["id"], "k1");
        assert_eq!(first["config"]["client_conf"]["linger.ms"], "20");
        // Second sink should be untouched.
        assert_eq!(base["sinks"][1]["type"], "redis");
    }
}
