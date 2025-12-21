//! Pipeline lifecycle management.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use checkpoints::CheckpointStore;
use chrono::Utc;
use deltaforge_config::{PipelineSpec, SourceCfg};
use deltaforge_core::{Event, SourceHandle};
use futures::future::try_join_all;
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
use sources::build_source;
use sources::mysql::MySqlSchemaLoader;

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
    schema_loader: Option<MySqlSchemaLoader>,
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

    //pub fn registry(&self) -> Arc<InMemoryRegistry> {
    //    self.registry.clone()
    //}

    async fn spawn_pipeline(
        &self,
        spec: PipelineSpec,
    ) -> Result<PipelineRuntime> {
        let pipeline_name = spec.metadata.name.clone();

        counter!("deltaforge_pipelines_total").increment(1);

        // Pass registry to build_source
        let source = build_source(&spec, self.registry.clone())
            .context("build source")?;
        let processors = build_processors(&spec).context("build processors")?;
        let sinks = build_sinks(&spec).context("build sinks")?;

        let (schema_loader, table_patterns) = match &spec.spec.source {
            SourceCfg::Mysql(cfg) => {
                let loader = MySqlSchemaLoader::new(
                    &cfg.dsn,
                    self.registry.clone(),
                    &spec.metadata.tenant,
                );
                (Some(loader), cfg.tables.clone())
            }
            SourceCfg::Postgres(_) => (None, vec![]),
        };

        let (event_tx, event_rx) = mpsc::channel::<Event>(4096);
        let src_handle = source.run(event_tx, self.ckpt_store.clone()).await;

        let batch_processor =
            build_batch_processor(processors, pipeline_name.clone());
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
                gauge!("deltaforge_running_pipeline", "pipeline" => pname.clone()).decrement(1.0);
                warn!(pipeline=%pname, error=%e, "coordinator exited with error");
            } else {
                gauge!("deltaforge_running_pipeline", "pipeline" => pname.clone()).decrement(1.0);
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
            schema_loader,
            table_patterns,
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

#[async_trait::async_trait]
impl SchemaController for PipelineManager {
    async fn list_schemas(
        &self,
        pipeline: &str,
    ) -> Result<Vec<SchemaInfo>, PipelineAPIError> {
        // Clone loader before await - parking_lot guards are not Send
        let loader = {
            let guard = self.pipelines.read();
            let runtime = guard.get(pipeline).ok_or_else(|| {
                PipelineAPIError::NotFound(pipeline.to_string())
            })?;

            match &runtime.schema_loader {
                Some(l) => l.clone(),
                None => return Ok(vec![]),
            }
        };

        let cached = loader.list_cached().await;
        let schemas = cached
            .into_iter()
            .map(|((db, table), loaded)| SchemaInfo {
                database: db,
                table,
                column_count: loaded.schema.columns.len(),
                primary_key: loaded.schema.primary_key.clone(),
                fingerprint: loaded.fingerprint,
                registry_version: loaded.registry_version,
            })
            .collect();

        Ok(schemas)
    }

    async fn get_schema(
        &self,
        pipeline: &str,
        db: &str,
        table: &str,
    ) -> Result<SchemaDetail, PipelineAPIError> {
        let loader = {
            let guard = self.pipelines.read();
            let runtime = guard.get(pipeline).ok_or_else(|| {
                PipelineAPIError::NotFound(pipeline.to_string())
            })?;

            runtime.schema_loader.clone().ok_or_else(|| {
                PipelineAPIError::Failed(anyhow::anyhow!(
                    "no schema loader for this source type"
                ))
            })?
        };

        let loaded = loader
            .load_schema(db, table)
            .await
            .map_err(|e| PipelineAPIError::Failed(anyhow::anyhow!("{}", e)))?;

        let columns = loaded
            .schema
            .columns
            .iter()
            .map(|c| ColumnInfo {
                name: c.name.clone(),
                column_type: c.column_type.clone(),
                data_type: c.data_type.clone(),
                nullable: c.nullable,
                ordinal_position: c.ordinal_position,
                default_value: c.default_value.clone(),
                extra: c.extra.clone(),
                is_primary_key: loaded.schema.primary_key.contains(&c.name),
            })
            .collect();

        Ok(SchemaDetail {
            database: db.to_string(),
            table: table.to_string(),
            columns,
            primary_key: loaded.schema.primary_key,
            engine: loaded.schema.engine,
            charset: loaded.schema.charset,
            collation: loaded.schema.collation,
            fingerprint: loaded.fingerprint,
            registry_version: loaded.registry_version,
            loaded_at: Utc::now(),
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

            let loader = runtime.schema_loader.clone().ok_or_else(|| {
                PipelineAPIError::Failed(anyhow::anyhow!(
                    "no schema loader for this source type"
                ))
            })?;

            (loader, runtime.table_patterns.clone())
        };

        let t0 = Instant::now();

        let tables = loader
            .reload_all(&patterns)
            .await
            .map_err(|e| PipelineAPIError::Failed(anyhow::anyhow!("{}", e)))?;

        let statuses: Vec<TableReloadStatus> = tables
            .iter()
            .map(|(db, table)| TableReloadStatus {
                database: db.clone(),
                table: table.clone(),
                status: "ok".to_string(),
                changed: true,
                error: None,
            })
            .collect();

        Ok(ReloadResult {
            pipeline: pipeline.to_string(),
            tables_reloaded: tables.len(),
            tables: statuses,
            elapsed_ms: t0.elapsed().as_millis() as u64,
        })
    }

    async fn reload_table_schema(
        &self,
        pipeline: &str,
        db: &str,
        table: &str,
    ) -> Result<SchemaDetail, PipelineAPIError> {
        let loader = {
            let guard = self.pipelines.read();
            let runtime = guard.get(pipeline).ok_or_else(|| {
                PipelineAPIError::NotFound(pipeline.to_string())
            })?;

            runtime.schema_loader.clone().ok_or_else(|| {
                PipelineAPIError::Failed(anyhow::anyhow!(
                    "no schema loader for this source type"
                ))
            })?
        };

        // Force reload
        let loaded = loader
            .reload_schema(db, table)
            .await
            .map_err(|e| PipelineAPIError::Failed(anyhow::anyhow!("{}", e)))?;

        let columns = loaded
            .schema
            .columns
            .iter()
            .map(|c| ColumnInfo {
                name: c.name.clone(),
                column_type: c.column_type.clone(),
                data_type: c.data_type.clone(),
                nullable: c.nullable,
                ordinal_position: c.ordinal_position,
                default_value: c.default_value.clone(),
                extra: c.extra.clone(),
                is_primary_key: loaded.schema.primary_key.contains(&c.name),
            })
            .collect();

        Ok(SchemaDetail {
            database: db.to_string(),
            table: table.to_string(),
            columns,
            primary_key: loaded.schema.primary_key,
            engine: loaded.schema.engine,
            charset: loaded.schema.charset,
            collation: loaded.schema.collation,
            fingerprint: loaded.fingerprint,
            registry_version: loaded.registry_version,
            loaded_at: Utc::now(),
        })
    }

    async fn get_schema_versions(
        &self,
        pipeline: &str,
        db: &str,
        table: &str,
    ) -> Result<Vec<SchemaVersionInfo>, PipelineAPIError> {
        // Extract tenant before dropping guard
        let tenant = {
            let guard = self.pipelines.read();
            let runtime = guard.get(pipeline).ok_or_else(|| {
                PipelineAPIError::NotFound(pipeline.to_string())
            })?;
            runtime.spec.metadata.tenant.clone()
        };

        let versions = self.registry.list_versions(&tenant, db, table);

        Ok(versions
            .into_iter()
            .map(|v| SchemaVersionInfo {
                version: v.version,
                fingerprint: v.hash,
                column_count: v
                    .schema_json
                    .get("columns")
                    .and_then(|c| c.as_array())
                    .map(|a| a.len())
                    .unwrap_or(0),
                registered_at: v.registered_at,
            })
            .collect())
    }
}

fn merge_spec(
    base: &PipelineSpec,
    patch: Value,
) -> Result<PipelineSpec, PipelineAPIError> {
    let mut merged = serde_json::to_value(base)
        .map_err(|e| PipelineAPIError::Failed(e.into()))?;
    merge_values(&mut merged, patch);
    serde_json::from_value(merged)
        .map_err(|e| PipelineAPIError::Failed(e.into()))
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

#[cfg(test)]
mod tests {
    use super::*;
    use checkpoints::MemCheckpointStore;
    use deltaforge_config::{
        BatchConfig, Metadata, MysqlSrcCfg, RedisSinkCfg, SinkCfg, SourceCfg,
        Spec,
    };
    use std::sync::atomic::AtomicBool;
    use tokio::sync::Notify;
    use tokio::time::{Duration, timeout};

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
            },
        }
    }

    fn runtime_with_channel(
        name: &str,
    ) -> (PipelineRuntime, watch::Receiver<bool>) {
        let spec = sample_spec(name);
        let cancel = CancellationToken::new();
        let (pause_tx, pause_rx) = watch::channel(false);

        let source_handle = SourceHandle {
            cancel: cancel.clone(),
            paused: Arc::new(AtomicBool::new(false)),
            pause_notify: Arc::new(Notify::new()),
            join: tokio::spawn(async move { Ok(()) }),
        };

        let runtime = PipelineRuntime {
            spec,
            status: PipelineStatus::Running,
            cancel,
            pause: pause_tx,
            sources: vec![source_handle],
            join: Some(tokio::spawn(async move { Ok(()) })),
            schema_loader: None,
            table_patterns: vec![],
        };

        (runtime, pause_rx)
    }

    #[test]
    fn merge_spec_overlays_nested_values() {
        let base = sample_spec("pipe-1");
        let patch = serde_json::json!({
            "metadata": {"tenant": "beta"},
            "spec": {
                "batch": {"max_events": 10},
                "sinks": [{"type": "redis", "config": {"id": "redis", "uri": "redis://changed", "stream": "updates"}}]
            }
        });

        let merged = merge_spec(&base, patch).expect("merge should succeed");

        assert_eq!(merged.metadata.name, base.metadata.name);
        assert_eq!(merged.metadata.tenant, "beta");
        assert_eq!(merged.spec.batch.unwrap().max_events, Some(10));
        assert_eq!(merged.spec.sinks.len(), 1);
    }

    #[test]
    fn merge_spec_allows_name_changes_that_callers_must_guard() {
        let base = sample_spec("pipe-1");
        let patch = serde_json::json!({"metadata": {"name": "renamed"}});

        let merged = merge_spec(&base, patch).expect("merge should succeed");

        assert_eq!(merged.metadata.name, "renamed");
    }

    #[tokio::test]
    async fn pause_and_resume_update_runtime_state() {
        let mgr =
            PipelineManager::new(Arc::new(MemCheckpointStore::new().unwrap()));
        let (runtime, mut pause_rx) = runtime_with_channel("pipe-1");
        mgr.pipelines.write().insert("pipe-1".to_string(), runtime);

        let paused = mgr.pause("pipe-1").await.expect("pause should work");
        assert_eq!(paused.status, "paused");
        timeout(Duration::from_millis(100), pause_rx.changed())
            .await
            .expect("pause signal")
            .expect("watch change");
        assert!(pause_rx.borrow().to_owned());

        let resumed = mgr.resume("pipe-1").await.expect("resume should work");
        assert_eq!(resumed.status, "running");
        timeout(Duration::from_millis(100), pause_rx.changed())
            .await
            .expect("resume signal")
            .expect("watch change");
        assert!(!pause_rx.borrow().to_owned());
    }

    #[tokio::test]
    async fn stop_cancels_and_removes_pipeline() {
        let mgr =
            PipelineManager::new(Arc::new(MemCheckpointStore::new().unwrap()));
        let (runtime, _pause_rx) = runtime_with_channel("pipe-1");
        let source_cancel = runtime.sources[0].cancel.clone();

        mgr.pipelines.write().insert("pipe-1".to_string(), runtime);

        let info = mgr.stop("pipe-1").await.expect("stop should work");
        assert_eq!(info.status, "stopped");
        assert!(source_cancel.is_cancelled());
        assert!(mgr.pipelines.read().is_empty());
    }
}
