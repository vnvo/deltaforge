use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use crate::coordinator::{
    Coordinator, SchemaSensorState, build_batch_processor, build_commit_fn,
};
use crate::replay_controller::{
    CoordinatorReplayDelivery, PauseIngestionControl, ReplayController,
};
use crate::replay_gate::ReplaySinkGate;
use crate::replay_job::{
    EncoderSchemaPolicy, ReplayJob, ReplayJobStore, ReplayPhase,
};
use crate::replay_worker::ReplayDelivery;
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
/// Uses the source-provided comparison function for correctness - different
/// sources have different checkpoint formats (MySQL file:pos, Postgres LSN)
/// that cannot be compared lexicographically.
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
                // This allows seamless migration - old pipelines that saved
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
use deltaforge_core::{SourceError, SourceHandle, SourceItem};
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
/// Returns `None` if no sinks use Avro - zero overhead in that case.
fn build_avro_provider(
    spec: &PipelineSpec,
    schema_loader: &Option<ArcSchemaLoader>,
) -> Option<Arc<dyn SourceSchemaProvider>> {
    use deltaforge_config::{EncodingCfg, SinkCfg};

    use deltaforge_core::encoding::avro_types::{
        EnumMode, NaiveTimestampMode, UnsignedBigintMode,
    };

    // Find the first Avro-encoded sink and extract type conversion options.
    // S3 sinks don't use EncodingCfg (they have their own format/compression)
    // so they are skipped here.
    let avro_cfg = spec.spec.sinks.iter().find_map(|s| {
        let encoding = match s {
            SinkCfg::Kafka(c) => &c.encoding,
            SinkCfg::Redis(c) => &c.encoding,
            SinkCfg::Nats(c) => &c.encoding,
            SinkCfg::Http(c) => &c.encoding,
            SinkCfg::S3(_) => return None,
            SinkCfg::Elasticsearch(_) => return None,
            // ClickHouse has its own RowBinary encoding, not EncodingCfg.
            SinkCfg::ClickHouse(_) => return None,
        };
        match encoding {
            EncodingCfg::Avro {
                unsigned_bigint_mode,
                enum_mode,
                naive_timestamp_mode,
                ..
            } => Some((
                unsigned_bigint_mode.clone(),
                enum_mode.clone(),
                naive_timestamp_mode.clone(),
            )),
            _ => None,
        }
    });

    let (ubm, em, ntm) = avro_cfg?;
    let loader = schema_loader.as_ref()?;

    let connector = match &spec.spec.source {
        SourceCfg::Mysql(_) => "mysql",
        SourceCfg::Postgres(_) => "postgresql",
    };

    let opts = TypeConversionOpts {
        unsigned_bigint_mode: match ubm.as_deref() {
            Some("long") => UnsignedBigintMode::Long,
            _ => UnsignedBigintMode::String,
        },
        enum_mode: match em.as_deref() {
            Some("enum") => EnumMode::Enum,
            _ => EnumMode::String,
        },
        naive_timestamp_mode: match ntm.as_deref() {
            Some("timestamp") => NaiveTimestampMode::Timestamp,
            _ => NaiveTimestampMode::String,
        },
    };

    let schema_provider =
        Arc::new(SchemaLoaderAdapter::new(Arc::clone(loader)));

    Some(Arc::new(AvroSchemaProviderImpl::new(
        schema_provider,
        connector,
        opts,
    )))
}

/// Build an Arrow schema resolver if any sink is an S3/Parquet sink.
/// Returns `None` when no S3 sink is configured, so the factory can skip
/// the (cheap but unnecessary) closure construction.
fn build_arrow_resolver(
    spec: &PipelineSpec,
    schema_loader: &Option<ArcSchemaLoader>,
) -> Option<sinks::s3::SchemaResolver> {
    use deltaforge_config::SinkCfg;
    use deltaforge_core::encoding::avro_types::TypeConversionOpts;

    let has_s3 = spec.spec.sinks.iter().any(|s| matches!(s, SinkCfg::S3(_)));
    if !has_s3 {
        return None;
    }

    let loader = schema_loader.as_ref()?;
    let connector = match &spec.spec.source {
        SourceCfg::Mysql(_) => "mysql",
        SourceCfg::Postgres(_) => "postgresql",
    };

    let schema_provider =
        Arc::new(SchemaLoaderAdapter::new(Arc::clone(loader)));

    // For Phase 1g.2: use default TypeConversionOpts. S3SinkCfg could expose
    // per-sink type-conversion options later (mirroring EncodingCfg::Avro).
    Some(crate::schema_provider::build_arrow_schema_resolver(
        schema_provider,
        connector,
        TypeConversionOpts::default(),
    ))
}

/// Build a ClickHouse column resolver if the pipeline has a ClickHouse sink.
fn build_clickhouse_resolver(
    spec: &deltaforge_config::PipelineSpec,
    schema_loader: &Option<ArcSchemaLoader>,
) -> Option<sinks::clickhouse::ClickHouseSchemaResolver> {
    use deltaforge_config::SinkCfg;

    let has_ch = spec
        .spec
        .sinks
        .iter()
        .any(|s| matches!(s, SinkCfg::ClickHouse(_)));
    if !has_ch {
        return None;
    }
    let loader = schema_loader.as_ref()?;
    let schema_provider =
        Arc::new(SchemaLoaderAdapter::new(Arc::clone(loader)));
    Some(crate::schema_provider::build_clickhouse_schema_resolver(
        schema_provider,
    ))
}

/// Build an Elasticsearch column resolver if the pipeline has an ES sink.
fn build_elasticsearch_resolver(
    spec: &deltaforge_config::PipelineSpec,
    schema_loader: &Option<ArcSchemaLoader>,
) -> Option<sinks::elasticsearch::EsSchemaResolver> {
    use deltaforge_config::SinkCfg;

    let has_es = spec
        .spec
        .sinks
        .iter()
        .any(|s| matches!(s, SinkCfg::Elasticsearch(_)));
    if !has_es {
        return None;
    }
    let loader = schema_loader.as_ref()?;
    let schema_provider =
        Arc::new(SchemaLoaderAdapter::new(Arc::clone(loader)));
    Some(crate::schema_provider::build_elasticsearch_schema_resolver(
        schema_provider,
    ))
}

/// Build the durable_v2 S3 sinks in the correct startup order: FIRST validate
/// the source's snapshot migration (fail closed on an interrupted legacy
/// snapshot) so a config that cannot start never probes/creates HEAD or fences a
/// healthy prior writer, THEN probe/recover/acquire each durable sink and apply
/// its configured filter. Returns an empty vec when no durable_v2 S3 sink is
/// configured. The comparator is a core trait, injected here.
async fn build_durable_s3_sinks(
    spec: &PipelineSpec,
    source: &dyn deltaforge_core::Source,
    ckpt_store: &dyn CheckpointStore,
    pipeline: &str,
    arrow_resolver: Option<sinks::s3::SchemaResolver>,
) -> Result<Vec<deltaforge_core::ArcDynSink>> {
    use deltaforge_config::{S3Durability, SinkCfg};

    let durable: Vec<&deltaforge_config::S3SinkCfg> = spec
        .spec
        .sinks
        .iter()
        .filter_map(|s| match s {
            SinkCfg::S3(c) if c.durability == S3Durability::DurableV2 => {
                Some(c)
            }
            _ => None,
        })
        .collect();
    if durable.is_empty() {
        return Ok(Vec::new());
    }

    // (2) Validate durable snapshot migration BEFORE any HEAD mutation.
    source
        .check_durable_snapshot_startup(ckpt_store)
        .await
        .context("durable_v2 snapshot startup check")?;

    // (3) Probe/recover/acquire each durable sink; apply the configured filter.
    let source_id = spec.spec.source.source_id().to_string();
    let mut out: Vec<deltaforge_core::ArcDynSink> =
        Vec::with_capacity(durable.len());
    for cfg in durable {
        let comparator: Arc<dyn deltaforge_core::CheckpointComparator> =
            Arc::new(sources::durable_checkpoint::SourceCheckpointComparator);
        let sink: deltaforge_core::ArcDynSink = Arc::new(
            sinks::s3::build_durable_s3_sink(
                cfg,
                pipeline,
                &source_id,
                comparator,
                arrow_resolver.clone(),
            )
            .await
            .context("build durable_v2 S3 sink")?,
        );
        // Apply the configured filter exactly as legacy S3 does (the filter
        // wrapper forwards the delivery context, even for a fully-filtered batch).
        let sink = match &cfg.filter {
            Some(f) if f.is_active() => {
                sinks::FilteredSink::wrap(sink, f.clone())
            }
            _ => sink,
        };
        out.push(sink);
    }
    Ok(out)
}

/// Fail closed if the constructed sink set does not exactly match the configured
/// sink IDs - guards against silently dropping or duplicating a sink.
fn validate_sink_ids(
    spec: &PipelineSpec,
    sinks: &[deltaforge_core::ArcDynSink],
) -> Result<()> {
    use std::collections::BTreeMap;
    let mut want: BTreeMap<&str, usize> = BTreeMap::new();
    for s in &spec.spec.sinks {
        *want.entry(s.sink_id()).or_default() += 1;
    }
    let mut got: BTreeMap<&str, usize> = BTreeMap::new();
    for s in sinks {
        *got.entry(s.id()).or_default() += 1;
    }
    if want != got {
        anyhow::bail!(
            "constructed sinks {:?} do not match configured sink ids {:?}",
            got.keys().collect::<Vec<_>>(),
            want.keys().collect::<Vec<_>>()
        );
    }
    Ok(())
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
    /// Background replay-retention task, when replay journaling is enabled. Owned here
    /// so it is aborted when the pipeline stops, is deleted, or the runtime is dropped.
    pub(crate) retention_task: Option<JoinHandle<()>>,
    /// Everything needed to build a replay controller (present when replay is enabled).
    pub(crate) replay_ctx: Option<ReplayContext>,
    /// The running replay controller task and its cancel handle, if a job is in progress.
    pub(crate) replay_controller: Option<(CancellationToken, JoinHandle<()>)>,
    pub(crate) started_at: std::time::Instant,
}

impl Drop for PipelineRuntime {
    fn drop(&mut self) {
        if let Some(task) = self.retention_task.take() {
            task.abort();
        }
        if let Some((cancel, task)) = self.replay_controller.take() {
            cancel.cancel();
            task.abort();
        }
    }
}

/// Everything needed to build a [`ReplayController`] for a replay-enabled pipeline. Held on
/// the runtime so a start request (Slice 4 REST) or a startup resume can spin one up. Cheap
/// to clone (Arc handles + a watch sender), so it can be lifted out from under the pipelines
/// lock before any async work.
#[derive(Clone)]
pub(crate) struct ReplayContext {
    backend: ArcStorageBackend,
    journal: Arc<dyn crate::replay_journal::JournalLog>,
    pipeline: String,
    incarnation: String,
    gate: Arc<ReplaySinkGate>,
    pause: watch::Sender<bool>,
    delivery: Arc<dyn ReplayDelivery>,
    pin: Arc<AtomicU64>,
    batch_limit: usize,
    settle: Duration,
}

impl ReplayContext {
    fn store(&self) -> ReplayJobStore {
        ReplayJobStore::new(
            self.backend.clone(),
            &self.pipeline,
            &self.incarnation,
        )
    }

    fn build_controller(&self, cancel: CancellationToken) -> ReplayController {
        ReplayController::new(
            self.store(),
            self.journal.clone(),
            self.gate.clone(),
            Arc::new(PauseIngestionControl::new(
                self.pause.clone(),
                self.settle,
            )),
            self.delivery.clone(),
            self.pin.clone(),
            cancel,
            self.batch_limit,
            self.pipeline.clone(),
        )
    }
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

        // Build Arrow schema resolver if any sink is an S3/Parquet sink
        let arrow_schema_resolver = build_arrow_resolver(&spec, &schema_loader);

        // Build ClickHouse column resolver if any sink is a ClickHouse sink
        let clickhouse_resolver =
            build_clickhouse_resolver(&spec, &schema_loader);

        // Build Elasticsearch column resolver if any sink is an ES sink
        let es_resolver = build_elasticsearch_resolver(&spec, &schema_loader);

        // Build every non-durable sink. Durable_v2 S3 sinks are deferred (the
        // builder fails closed on them via the public API): they are built below,
        // in order, only after the durable-startup guard.
        let mut sinks = sinks::build_sinks_deferring_durable_s3(
            &spec,
            cancel.clone(),
            &pipeline_name,
            avro_source_schemas,
            arrow_schema_resolver.clone(),
            clickhouse_resolver,
            es_resolver,
        )
        .context("build sinks")?;

        // Durable startup order: (2) validate the source snapshot migration
        // BEFORE (3) probing/recovering/acquiring durable S3 - so a config that
        // cannot start never creates HEAD or fences a healthy prior writer - then
        // (4) confirm the constructed sink set matches configuration.
        let durable = build_durable_s3_sinks(
            &spec,
            source.as_ref(),
            self.ckpt_store.as_ref(),
            &pipeline_name,
            arrow_schema_resolver.clone(),
        )
        .await?;
        sinks.extend(durable);
        validate_sink_ids(&spec, &sinks)
            .context("sink construction does not match configuration")?;

        let table_patterns = match &spec.spec.source {
            SourceCfg::Mysql(c) => c.tables.clone(),
            SourceCfg::Postgres(_) => vec![],
        };

        let alive = Arc::new(AtomicBool::new(true));

        let (event_tx, event_rx) = mpsc::channel::<SourceItem>(32_768);
        // Wrap checkpoint store so the source reads the minimum per-sink
        // checkpoint - it replays from the position the slowest sink needs.
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

        // Keep a processor handle + a by-id sink map for the replay delivery, which runs
        // the CURRENT processors and sends to a job's target sinks out of band.
        let replay_processors = Arc::clone(&processors);
        let sinks_by_id: HashMap<String, deltaforge_core::ArcDynSink> = sinks
            .iter()
            .map(|s| (s.id().to_string(), Arc::clone(s)))
            .collect();

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
            .sink_batch_deadline(
                spec.spec
                    .sink_batch_deadline_secs
                    .map(|s| std::time::Duration::from_secs(s.into())),
            )
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

        // DLQ writer - opt-in via journal config.
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

        // Replay journal - opt-in, requires BOTH the journal master switch and the
        // replay sub-switch (replay is a journal feature; the master switch gates it).
        let mut retention_task: Option<tokio::task::JoinHandle<()>> = None;
        let mut replay_ctx: Option<ReplayContext> = None;
        if let Some(replay_cfg) = spec
            .spec
            .journal
            .as_ref()
            .filter(|j| j.enabled)
            .and_then(|j| j.replay.clone())
            .filter(|r| r.enabled)
        {
            use crate::replay_journal::{
                BackendJournalLog, JournalLog, REPLAY_NS, RetentionConfig,
                spawn_retention_task,
            };
            // Durable, incarnation-scoped identity: minted once per lifecycle and reused
            // across restarts (the slot is deleted on pipeline delete, so a recreated
            // pipeline mints a fresh incarnation and never inherits an old stream). The
            // read-back is deterministic, so retries bind the same identity.
            let inc_key = format!("{pipeline_name}:incarnation");
            let new_id = uuid::Uuid::now_v7().to_string();
            let incarnation = match self
                .backend
                .slot_create(REPLAY_NS, &inc_key, new_id.as_bytes())
                .await?
            {
                Some(_) => new_id,
                None => {
                    let (_, bytes) = self
                        .backend
                        .slot_get(REPLAY_NS, &inc_key)
                        .await?
                        .expect("incarnation slot exists after slot_create");
                    String::from_utf8_lossy(&bytes).into_owned()
                }
            };
            let identity = deltaforge_core::replay::PipelineIdentity {
                pipeline: pipeline_name.clone(),
                incarnation: incarnation.clone(),
                // Real source DB lineage (system_identifier / server-uuid) is not yet
                // exposed by the source trait; recorded as absent rather than a
                // placeholder. Lineage IS part of the identity and the capture id, so
                // changing it (None -> Some, or a different value) requires minting a new
                // incarnation - existing envelopes stay bound to the incarnation that
                // captured them and are only readable under that identity.
                source_lineage: None,
            };
            let journal: Arc<dyn JournalLog> = Arc::new(
                BackendJournalLog::new(self.backend.clone(), identity.clone()),
            );
            // Per-sink gate + retention pin, shared between the coordinator (which excludes
            // gated sinks from live delivery and commit policy) and the replay controller
            // (which flips the gate and reports the pin). The gate is installed from the
            // durable job below, before any live delivery, so a crash cannot strand a pause.
            let gate = Arc::new(ReplaySinkGate::new());
            let pin = Arc::new(AtomicU64::new(u64::MAX));
            builder = builder.replay_gate(Arc::clone(&gate));
            builder =
                builder.replay_capture(crate::coordinator::ReplayCapture {
                    journal: Arc::clone(&journal),
                    identity,
                    max_envelope_bytes: replay_cfg.max_envelope_bytes,
                    // The schema-registry handle is not threaded here yet; the registry
                    // sequence is recorded as absent rather than a placeholder 0. It is
                    // provenance (excluded from capture_id) but still part of the stored
                    // canonical bytes, so whatever value it returns must be DETERMINISTIC
                    // and STABLE for a commit unit across retries - a differing value on
                    // retry would fail log_append_if_absent with a CaptureIdentityConflict.
                    // `None` is trivially stable. When bound for real, derive it from the
                    // unit's own retry-stable schema state (the registry sequence pinned to
                    // the events' schema versions), NOT a mutable global sequence sampled
                    // during capture.
                    registry_seq_fn: Arc::new(|| None),
                });
            let ret_cfg = RetentionConfig {
                max_age_ms: (replay_cfg.retention_secs > 0)
                    .then(|| (replay_cfg.retention_secs * 1000) as i64),
                max_entries: (replay_cfg.max_entries > 0)
                    .then_some(replay_cfg.max_entries),
                max_bytes: (replay_cfg.max_bytes > 0)
                    .then_some(replay_cfg.max_bytes),
                interval_secs: 60,
            };
            // Retention is pinned to the active replay job (if any) through the shared
            // `pin`: the controller sets it while a job runs and clears it otherwise, so
            // retention never truncates envelopes a job still needs. The task is owned by
            // the PipelineRuntime and aborted on stop/delete/drop.
            let pin_for_ret = Arc::clone(&pin);
            retention_task = Some(spawn_retention_task(
                Arc::clone(&journal),
                pipeline_name.clone(),
                ret_cfg,
                Arc::new(move || pin_for_ret.load(Ordering::SeqCst)),
            ));

            // Startup barrier: reinstall the gate from the durable job BEFORE any live
            // delivery (the coordinator has not started consuming yet), so a job that was
            // mid-flight before a restart keeps its sinks paused.
            let store = ReplayJobStore::new(
                self.backend.clone(),
                &pipeline_name,
                &incarnation,
            );
            if let Some(stored) = store.get().await? {
                if stored.job.holds_pause() {
                    gate.exclude(
                        stored
                            .job
                            .selected_sinks
                            .iter()
                            .chain(stored.job.staged_sinks.iter())
                            .cloned(),
                    );
                }
            }

            let delivery: Arc<dyn ReplayDelivery> =
                Arc::new(CoordinatorReplayDelivery::new(
                    build_batch_processor(
                        replay_processors,
                        pipeline_name.clone(),
                    ),
                    sinks_by_id,
                    pipeline_name.clone(),
                ));
            let batch_limit = spec
                .spec
                .batch
                .as_ref()
                .and_then(|b| b.max_events)
                .unwrap_or(500)
                .max(1);
            replay_ctx = Some(ReplayContext {
                backend: self.backend.clone(),
                journal,
                pipeline: pipeline_name.clone(),
                incarnation: incarnation.clone(),
                gate,
                pause: pause_tx.clone(),
                delivery,
                pin,
                batch_limit,
                settle: Duration::from_millis(200),
            });
            tracing::info!(
                pipeline = %pipeline_name,
                incarnation = %incarnation,
                "replay journaling enabled"
            );
        }

        let coord = builder.build();
        let cancel_for_task = cancel.clone();
        let cancel_check = cancel.clone();
        let pname = pipeline_name.clone();

        let alive_for_task = Arc::clone(&alive);

        let join = tokio::spawn(async move {
            let result = coord.run(event_rx, cancel_for_task, pause_rx).await;
            if !cancel_check.is_cancelled() {
                // Coordinator exited without an explicit stop - also mark
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

        // Resume an in-flight replay job across a restart: if the durable job is still
        // active, spawn a controller to drive it to completion (the startup barrier above
        // already reinstalled its pauses before the coordinator began delivering).
        let mut replay_controller: Option<(CancellationToken, JoinHandle<()>)> =
            None;
        if let Some(ctx) = &replay_ctx {
            let has_active_job = ctx
                .store()
                .get()
                .await?
                .map(|s| s.job.is_active())
                .unwrap_or(false);
            if has_active_job {
                let job_cancel = CancellationToken::new();
                let controller = ctx.build_controller(job_cancel.clone());
                let pname = pipeline_name.clone();
                let task = tokio::spawn(async move {
                    if let Err(e) = controller.run().await {
                        tracing::warn!(
                            pipeline = %pname,
                            error = %format!("{e:#}"),
                            "resumed replay job failed"
                        );
                    }
                });
                replay_controller = Some((job_cancel, task));
            }
        }

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
        // Build gauge with dynamic labels - use the pipeline + tenant as fixed,
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
            retention_task,
            replay_ctx,
            replay_controller,
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

        // Reject impossible commit-policy configurations up front (e.g. a
        // quorum larger than the sink count) so the operator gets an actionable
        // error instead of a pipeline that can never commit a checkpoint.
        crate::coordinator::validate_commit_policy(
            &spec.spec.commit_policy,
            spec.spec.sinks.len(),
        )
        .map_err(|e| PipelineAPIError::Failed(anyhow::anyhow!(e)))?;

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
        // Stop the replay-retention background task with the coordinator.
        if let Some(task) = runtime.retention_task.take() {
            task.abort();
        }
        // Stop any in-flight replay controller (a restart's startup barrier resumes it).
        if let Some((c, task)) = runtime.replay_controller.take() {
            c.cancel();
            task.abort();
        }
        let sources = std::mem::take(&mut runtime.sources);
        for src in &sources {
            src.cancel.cancel();
        }

        if let Some(join) = runtime.join.take() {
            let _ = join.await;
        }
        for src in sources {
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

    /// Lift the replay context out from under the pipelines lock (so async work does not
    /// hold the parking_lot guard).
    fn replay_ctx_of(
        &self,
        name: &str,
    ) -> Result<ReplayContext, PipelineAPIError> {
        let guard = self.pipelines.read();
        let rt = guard
            .get(name)
            .ok_or_else(|| PipelineAPIError::NotFound(name.to_string()))?;
        rt.replay_ctx.clone().ok_or_else(|| {
            PipelineAPIError::Failed(anyhow::anyhow!(
                "replay is not enabled for pipeline '{name}' (set journal.replay.enabled)"
            ))
        })
    }

    /// Start a replay job for a pipeline: create the durable job, then spawn a controller to
    /// drive it (historical delivery, catch-up, handoff). Returns the new job id. The store
    /// rejects a second active job, so callers get `AlreadyActive` rather than two runners.
    #[allow(clippy::too_many_arguments)]
    pub async fn start_replay(
        &self,
        name: &str,
        selected_sinks: Vec<String>,
        staged_sinks: Vec<String>,
        from_seq: u64,
        through_seq: Option<u64>,
        encoder_schema_policy: EncoderSchemaPolicy,
        dry_run: bool,
    ) -> Result<String, PipelineAPIError> {
        let ctx = self.replay_ctx_of(name)?;
        let job_id = uuid::Uuid::now_v7().to_string();
        let now = now_ms();
        let job = ReplayJob::new(
            job_id.clone(),
            name,
            &ctx.incarnation,
            selected_sinks,
            staged_sinks,
            from_seq,
            through_seq,
            encoder_schema_policy,
            dry_run,
            now,
        )
        .map_err(|e| PipelineAPIError::Failed(anyhow::anyhow!(e)))?;
        ctx.store()
            .create(&job)
            .await
            .map_err(PipelineAPIError::Failed)?;

        let job_cancel = CancellationToken::new();
        let controller = ctx.build_controller(job_cancel.clone());
        let pname = name.to_string();
        let task = tokio::spawn(async move {
            if let Err(e) = controller.run().await {
                tracing::warn!(
                    pipeline = %pname,
                    error = %format!("{e:#}"),
                    "replay job failed"
                );
            }
        });

        // Install the controller handle; abort any stale (finished) one it replaces.
        if let Some(rt) = self.pipelines.write().get_mut(name) {
            if let Some((c, t)) =
                rt.replay_controller.replace((job_cancel, task))
            {
                c.cancel();
                t.abort();
            }
        }
        Ok(job_id)
    }

    /// Cancel the active replay job for a pipeline: stop the controller, mark the job
    /// cancelled, and return the paused/staged sinks to the live set. Idempotent when no
    /// active job exists.
    pub async fn cancel_replay(
        &self,
        name: &str,
    ) -> Result<(), PipelineAPIError> {
        let ctx = self.replay_ctx_of(name)?;

        // Stop the controller first so it cannot race the cancellation.
        if let Some(rt) = self.pipelines.write().get_mut(name) {
            if let Some((c, t)) = rt.replay_controller.take() {
                c.cancel();
                t.abort();
            }
        }

        // Mark the durable job cancelled (only from a pre-live-restored phase) and restore
        // live delivery: release the gate and the retention pin, and unpause ingestion in
        // case the cancel landed during a handoff quiesce.
        if let Some(stored) =
            ctx.store().get().await.map_err(PipelineAPIError::Failed)?
        {
            if stored.job.is_active() {
                if let Ok(cancelled) =
                    stored.job.advance(ReplayPhase::Cancelled, now_ms())
                {
                    let _ = ctx
                        .store()
                        .compare_and_set(stored.version, &cancelled)
                        .await;
                }
                ctx.gate.include(
                    stored
                        .job
                        .selected_sinks
                        .iter()
                        .chain(stored.job.staged_sinks.iter()),
                );
                ctx.pin.store(u64::MAX, Ordering::SeqCst);
                let _ = ctx.pause.send(false);
            }
        }
        Ok(())
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
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
            // Stop the replay-retention background task with the coordinator; there is
            // nothing to retain while the pipeline is not capturing.
            if let Some(task) = runtime.retention_task.take() {
                task.abort();
            }
            // Stop any in-flight replay controller (resumed by the barrier on restart).
            if let Some((c, task)) = runtime.replay_controller.take() {
                c.cancel();
                task.abort();
            }
            (cancel, sources, join)
        };

        cancel.cancel();
        for src in &sources {
            src.cancel.cancel();
        }
        // Update the gauge immediately - the status is already Stopped in the
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

        // Fail-closed incarnation invalidation FIRST, before the pipeline is removed. If
        // clearing the replay incarnation slot fails, abort the delete: the pipeline
        // stays registered and deletable on retry, and we never report a successful
        // deletion that leaves the old incarnation behind for a recreated pipeline to
        // inherit. slot_delete is idempotent (Ok(false) when nothing was there), so a
        // non-replay pipeline is a no-op and a retry after a partial delete still works.
        let inc_key = format!("{name}:incarnation");
        self.backend
            .slot_delete(crate::replay_journal::REPLAY_NS, &inc_key)
            .await
            .map_err(|e| {
                PipelineAPIError::Failed(e.context(
                    "failed to clear replay incarnation slot; delete aborted so the \
                     pipeline is not recreated with a stale incarnation",
                ))
            })?;

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
                    table_options: Default::default(),
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
                sink_batch_deadline_secs: None,
                schema_sensing: Default::default(),
                journal: None,
            },
        }
    }

    // ── Durable_v2 wiring: fail-closed builders, ID validation, startup order ──

    use deltaforge_config::{S3Durability, S3SinkCfg};

    fn durable_s3_cfg(id: &str, bucket: &str) -> S3SinkCfg {
        S3SinkCfg {
            id: id.into(),
            bucket: bucket.into(),
            prefix: "root".into(),
            region: None,
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            virtual_hosted_style: false,
            local: true,
            format: deltaforge_config::S3FileFormat::Jsonl,
            compression: deltaforge_config::S3Compression::None,
            file_roll: Default::default(),
            send_timeout_secs: 60,
            required: Some(true),
            durability: S3Durability::DurableV2,
            filter: None,
        }
    }

    fn spec_with_sinks(sinks: Vec<SinkCfg>) -> PipelineSpec {
        let mut s = sample_spec("p");
        s.spec.sinks = sinks;
        s
    }

    /// Public build_sinks* must fail closed on a durable_v2 config - never
    /// return a sink list silently missing the configured S3 sink.
    #[test]
    fn public_build_sinks_fails_closed_on_durable_v2() {
        let spec = spec_with_sinks(vec![SinkCfg::S3(durable_s3_cfg(
            "s3",
            "/tmp/df-none",
        ))]);
        let err = sinks::build_sinks_with_schemas(
            &spec,
            CancellationToken::new(),
            "p",
            None,
            None,
            None,
            None,
        );
        assert!(err.is_err(), "durable_v2 must not build via the public API");

        // The deferring builder omits it (the runner builds it), so the list is
        // shorter - which is exactly why the runner then validates IDs.
        let deferred = sinks::build_sinks_deferring_durable_s3(
            &spec,
            CancellationToken::new(),
            "p",
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert!(deferred.is_empty(), "durable S3 deferred, not built here");
    }

    /// A recording sink for ID validation.
    struct NamedSink(String);
    #[async_trait]
    impl deltaforge_core::Sink for NamedSink {
        fn id(&self) -> &str {
            &self.0
        }
        async fn send(
            &self,
            _e: &deltaforge_core::Event,
        ) -> deltaforge_core::SinkResult<()> {
            Ok(())
        }
    }

    #[test]
    fn validate_sink_ids_rejects_missing_sink() {
        let spec = spec_with_sinks(vec![
            SinkCfg::S3(durable_s3_cfg("s3", "/tmp/df-none")),
            SinkCfg::Redis(RedisSinkCfg {
                id: "redis".into(),
                uri: "redis://x".into(),
                stream: "e".into(),
                key: None,
                required: Some(true),
                send_timeout_secs: None,
                batch_timeout_secs: None,
                connect_timeout_secs: None,
                envelope: deltaforge_config::EnvelopeCfg::Debezium,
                encoding: deltaforge_config::EncodingCfg::Json,
                filter: None,
            }),
        ]);
        // Only one of the two configured sinks constructed -> fail closed.
        let built: Vec<deltaforge_core::ArcDynSink> =
            vec![Arc::new(NamedSink("redis".into()))];
        assert!(validate_sink_ids(&spec, &built).is_err());
        // Both present -> ok.
        let both: Vec<deltaforge_core::ArcDynSink> = vec![
            Arc::new(NamedSink("redis".into())),
            Arc::new(NamedSink("s3".into())),
        ];
        assert!(validate_sink_ids(&spec, &both).is_ok());
    }

    /// A source whose durable startup check fails.
    struct FailCheckSource;
    #[async_trait]
    impl deltaforge_core::Source for FailCheckSource {
        async fn run(
            &self,
            _tx: mpsc::Sender<SourceItem>,
            _ckpt: Arc<dyn CheckpointStore>,
        ) -> SourceHandle {
            unimplemented!("not started in this test")
        }
        fn compare_checkpoints(
            &self,
            _a: &[u8],
            _b: &[u8],
        ) -> std::cmp::Ordering {
            std::cmp::Ordering::Equal
        }
        async fn check_durable_snapshot_startup(
            &self,
            _ckpt: &dyn CheckpointStore,
        ) -> Result<(), SourceError> {
            Err(SourceError::Other(anyhow::anyhow!("ambiguous legacy")))
        }
    }

    /// A failed snapshot-startup check must abort BEFORE the durable sink is
    /// built, so no probe/recovery/HEAD creation or epoch acquisition happens.
    #[tokio::test]
    async fn failed_startup_check_creates_no_head() {
        let dir = std::env::temp_dir()
            .join(format!("df-durable-guard-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let bucket = dir.to_string_lossy().to_string();
        let spec =
            spec_with_sinks(vec![SinkCfg::S3(durable_s3_cfg("s3", &bucket))]);
        let store: Arc<dyn CheckpointStore> =
            Arc::new(checkpoints::MemCheckpointStore::new().unwrap());

        let err = build_durable_s3_sinks(
            &spec,
            &FailCheckSource,
            store.as_ref(),
            "p",
            None,
        )
        .await;
        assert!(err.is_err(), "startup check must abort construction");

        // Nothing was written: the bucket dir (and its _manifest) never appeared.
        let manifest = dir.join("root").join("p").join("_manifest");
        assert!(
            !manifest.exists(),
            "no HEAD/manifest created when the startup check fails"
        );
        let _ = std::fs::remove_dir_all(&dir);
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
        // No per-sink checkpoints and no legacy key - fresh start.
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
        // Using simple JSON strings - lexicographic comparison works for these.
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
