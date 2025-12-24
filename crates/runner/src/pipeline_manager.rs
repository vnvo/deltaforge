use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use checkpoints::CheckpointStore;
use deltaforge_config::{PipelineSpec, SourceCfg};
use deltaforge_core::{Event, SourceHandle};
use metrics::{counter, gauge};
use parking_lot::RwLock;
use processors::build_processors;
use rest_api::{
    ColumnInfo, PipeInfo, PipelineAPIError, PipelineController, ReloadResult,
    SchemaController, SchemaDetail, SchemaInfo, SchemaVersionInfo,
    TableReloadStatus,
};
use schema_registry::InMemoryRegistry;
use serde_json::Value;
use sinks::build_sinks;
use sources::{ArcSchemaLoader, build_schema_loader, build_source};
use crate::schema_provider::SchemaLoaderAdapter;

use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::coordinator::{
    Coordinator, SchemaSensorState, build_batch_processor, build_commit_fn,
};

// ============================================================================
// Pipeline Runtime
// ============================================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PipelineStatus {
    Running,
    Paused,
    Stopped,
}

impl PipelineStatus {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Stopped => "stopped",
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
    schema_loader: Option<ArcSchemaLoader>,
    table_patterns: Vec<String>,
}

impl PipelineRuntime {
    fn pause(&mut self) {
        self.sources.iter().for_each(|s| s.pause());
        let _ = self.pause.send(true);
        self.status = PipelineStatus::Paused;
    }

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

// ============================================================================
// Pipeline Manager
// ============================================================================

#[derive(Clone)]
pub struct PipelineManager {
    pipelines: Arc<RwLock<HashMap<String, PipelineRuntime>>>,
    ckpt_store: Arc<dyn CheckpointStore>,
    registry: Arc<InMemoryRegistry>,
}

impl PipelineManager {
    pub fn new(ckpt_store: Arc<dyn CheckpointStore>) -> Self {
        Self {
            pipelines: Arc::new(RwLock::new(HashMap::new())),
            ckpt_store,
            registry: Arc::new(InMemoryRegistry::new()),
        }
    }

    async fn spawn_pipeline(
        &self,
        spec: PipelineSpec,
    ) -> Result<PipelineRuntime> {
        let pipeline_name = spec.metadata.name.clone();
        counter!("deltaforge_pipelines_total").increment(1);

        let source = build_source(&spec, self.registry.clone())
            .context("build source")?;
        let processors = build_processors(&spec).context("build processors")?;
        let sinks = build_sinks(&spec).context("build sinks")?;
        let schema_loader = build_schema_loader(&spec, self.registry.clone());

        let table_patterns = match &spec.spec.source {
            SourceCfg::Mysql(c) => c.tables.clone(),
            SourceCfg::Turso(c) => c.tables.clone(),
            SourceCfg::Postgres(_) => vec![],
        };

        let (event_tx, event_rx) = mpsc::channel::<Event>(4096);
        let src_handle = source.run(event_tx, self.ckpt_store.clone()).await;

        let batch_processor =
            build_batch_processor(processors, pipeline_name.clone());
        let commit_cp = build_commit_fn(
            self.ckpt_store.clone(),
            source.checkpoint_key().to_string(),
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

        if let Some(s) = sensor {
            builder = builder.schema_sensor(s);
        }

        if let Some(loader) = &schema_loader {
            let provider = Arc::new(SchemaLoaderAdapter::new(loader.clone()));
            builder = builder.schema_provider(provider);
        }

        let coord = builder.build();

        let cancel = CancellationToken::new();
        let cancel_for_task = cancel.clone();
        let pname = pipeline_name.clone();

        let join = tokio::spawn(async move {
            coord.run(event_rx, cancel_for_task, pause_rx).await?;
            info!(pipeline = %pname, "pipeline coordinator exited");
            Ok(())
        });

        gauge!("deltaforge_pipeline_status", "pipeline" => pipeline_name)
            .set(1.0);

        Ok(PipelineRuntime {
            spec,
            status: PipelineStatus::Running,
            cancel,
            pause: pause_tx,
            sources: vec![src_handle],
            join: Some(join),
            schema_loader,
            table_patterns,
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

    fn get_loader(
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
}

// ============================================================================
// PipelineController
// ============================================================================

#[async_trait::async_trait]
impl PipelineController for PipelineManager {
    async fn list(&self) -> Vec<PipeInfo> {
        self.list_pipelines()
    }

    async fn create(
        &self,
        spec: PipelineSpec,
    ) -> Result<PipeInfo, PipelineAPIError> {
        self.start_pipeline(spec).await
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
}

// ============================================================================
// SchemaController
// ============================================================================

#[async_trait::async_trait]
impl SchemaController for PipelineManager {
    async fn list_schemas(
        &self,
        pipeline: &str,
    ) -> Result<Vec<SchemaInfo>, PipelineAPIError> {
        let loader = self.get_loader(pipeline)?;
        Ok(loader
            .list_cached()
            .await
            .into_iter()
            .map(|e| SchemaInfo {
                database: e.database,
                table: e.table,
                column_count: e.column_count,
                primary_key: e.primary_key,
                fingerprint: e.fingerprint,
                registry_version: e.registry_version,
            })
            .collect())
    }

    async fn get_schema(
        &self,
        pipeline: &str,
        db: &str,
        table: &str,
    ) -> Result<SchemaDetail, PipelineAPIError> {
        let loader = self.get_loader(pipeline)?;
        let loaded = loader
            .load(db, table)
            .await
            .map_err(PipelineAPIError::Failed)?;

        // Extract columns from schema_json (source-specific)
        let columns = extract_columns(&loaded.schema_json);

        Ok(SchemaDetail {
            database: loaded.database,
            table: loaded.table,
            columns,
            primary_key: loaded.primary_key,
            engine: loaded
                .schema_json
                .get("engine")
                .and_then(|v| v.as_str())
                .map(String::from),
            charset: loaded
                .schema_json
                .get("charset")
                .and_then(|v| v.as_str())
                .map(String::from),
            collation: loaded
                .schema_json
                .get("collation")
                .and_then(|v| v.as_str())
                .map(String::from),
            fingerprint: loaded.fingerprint,
            registry_version: loaded.registry_version,
            loaded_at: loaded.loaded_at,
        })
    }

    async fn reload_schemas(
        &self,
        pipeline: &str,
    ) -> Result<ReloadResult, PipelineAPIError> {
        let (loader, patterns) = {
            let guard = self.pipelines.read();
            let runtime = guard.get(pipeline).ok_or_else(|| {
                PipelineAPIError::NotFound(pipeline.to_string())
            })?;
            (
                runtime.schema_loader.clone(),
                runtime.table_patterns.clone(),
            )
        };

        let loader = loader.ok_or_else(|| {
            PipelineAPIError::Failed(anyhow::anyhow!("no schema loader"))
        })?;
        let t0 = Instant::now();
        let tables = loader
            .reload_all(&patterns)
            .await
            .map_err(PipelineAPIError::Failed)?;

        Ok(ReloadResult {
            pipeline: pipeline.to_string(),
            tables_reloaded: tables.len(),
            tables: tables
                .iter()
                .map(|(db, t)| TableReloadStatus {
                    database: db.clone(),
                    table: t.clone(),
                    status: "ok".to_string(),
                    changed: true,
                    error: None,
                })
                .collect(),
            elapsed_ms: t0.elapsed().as_millis() as u64,
        })
    }

    async fn reload_table_schema(
        &self,
        pipeline: &str,
        db: &str,
        table: &str,
    ) -> Result<SchemaDetail, PipelineAPIError> {
        let loader = self.get_loader(pipeline)?;
        loader
            .reload(db, table)
            .await
            .map_err(PipelineAPIError::Failed)?;
        self.get_schema(pipeline, db, table).await
    }

    async fn get_schema_versions(
        &self,
        pipeline: &str,
        db: &str,
        table: &str,
    ) -> Result<Vec<SchemaVersionInfo>, PipelineAPIError> {
        let tenant = self
            .pipelines
            .read()
            .get(pipeline)
            .ok_or_else(|| PipelineAPIError::NotFound(pipeline.to_string()))?
            .spec
            .metadata
            .tenant
            .clone();

        Ok(self
            .registry
            .list_versions(&tenant, db, table)
            .into_iter()
            .map(|v| SchemaVersionInfo {
                version: v.version,
                fingerprint: v.hash,
                column_count: 0,
                registered_at: v.registered_at,
            })
            .collect())
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Extract ColumnInfo from source-specific schema JSON.
fn extract_columns(schema_json: &Value) -> Vec<ColumnInfo> {
    let Some(cols) = schema_json.get("columns").and_then(|v| v.as_array())
    else {
        return vec![];
    };

    cols.iter()
        .filter_map(|c| {
            Some(ColumnInfo {
                name: c.get("name")?.as_str()?.to_string(),
                column_type: c
                    .get("column_type")
                    .or(c.get("declared_type"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                data_type: c
                    .get("data_type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                nullable: c
                    .get("nullable")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(true),
                ordinal_position: c
                    .get("ordinal_position")
                    .or(c.get("column_index"))
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0) as u32,
                default_value: c
                    .get("default_value")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                extra: c
                    .get("extra")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                is_primary_key: c
                    .get("is_primary_key")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false),
            })
        })
        .collect()
}

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
        BatchConfig, Metadata, MysqlSrcCfg, RedisSinkCfg, SinkCfg, SourceCfg,
        Spec,
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
                }),
                processors: vec![],
                sinks: vec![SinkCfg::Redis(RedisSinkCfg {
                    id: "redis".to_string(),
                    uri: "redis://localhost".to_string(),
                    stream: "events".to_string(),
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
