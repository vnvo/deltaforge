//! Schema API - Database schema management endpoints.

use std::sync::Arc;
use std::time::Instant;

use rest_api::{
    ColumnInfo, PipelineAPIError, ReloadResult, SchemaController, SchemaDetail,
    SchemaInfo, SchemaVersionInfo, TableReloadStatus,
};
use serde_json::Value;

use crate::pipeline_manager::PipelineManager;

/// Typed, retryable response for schema requests made before the pipeline's
/// source has verified its server identity (its registry scope does not exist
/// yet). Never reported as an internal error or as "no schema".
fn lineage_not_established(pipeline: &str) -> PipelineAPIError {
    PipelineAPIError::SchemaLineageNotEstablished(format!(
        "pipeline {pipeline}: schemas are unavailable because its source has not \
         yet verified its database server identity (it may still be starting, or \
         failing before it connects). Retry once the source is running; if this \
         persists, check the pipeline status and the source logs."
    ))
}

/// Map a schema-loader error: an unestablished lineage becomes the typed 503,
/// anything else stays a failure.
fn loader_error(pipeline: &str, err: anyhow::Error) -> PipelineAPIError {
    if sources::registry_scope::is_lineage_not_established(&err) {
        lineage_not_established(pipeline)
    } else {
        PipelineAPIError::Failed(err)
    }
}

#[derive(Clone)]
pub struct SchemaApi(pub Arc<PipelineManager>);

impl SchemaApi {
    pub fn new(manager: Arc<PipelineManager>) -> Self {
        Self(manager)
    }
}

#[async_trait::async_trait]
impl SchemaController for SchemaApi {
    async fn list_schemas(
        &self,
        pipeline: &str,
    ) -> Result<Vec<SchemaInfo>, PipelineAPIError> {
        let loader = self.0.get_loader(pipeline)?;
        if !loader.lineage_established() {
            return Err(lineage_not_established(pipeline));
        }
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
        let loader = self.0.get_loader(pipeline)?;
        let loaded = loader
            .load(db, table)
            .await
            .map_err(|e| loader_error(pipeline, e))?;

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
            let guard = self.0.pipelines.read();
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
            .map_err(|e| loader_error(pipeline, e))?;

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
        let loader = self.0.get_loader(pipeline)?;
        loader
            .reload(db, table)
            .await
            .map_err(|e| loader_error(pipeline, e))?;
        self.get_schema(pipeline, db, table).await
    }

    async fn get_schema_versions(
        &self,
        pipeline: &str,
        db: &str,
        table: &str,
    ) -> Result<Vec<SchemaVersionInfo>, PipelineAPIError> {
        let (tenant, source_id, live_scope) = {
            let pipelines = self.0.pipelines.read();
            let rt = pipelines.get(pipeline).ok_or_else(|| {
                PipelineAPIError::NotFound(pipeline.to_string())
            })?;
            (
                rt.spec.metadata.tenant.clone(),
                rt.spec.spec.source.source_id().to_string(),
                rt.registry_scope.current().ok(),
            )
        };

        // The registry is keyed by the source's verified lineage: use the scope
        // the running source published, else the durably recorded lineage (the
        // source is stopped). Storage failures are errors, never "no versions".
        let key = match live_scope {
            Some(scope) => scope.key(db, table),
            None => {
                let record = storage::adapters::source_lineage::load(
                    self.0.backend(),
                    &tenant,
                    &source_id,
                )
                .await
                .map_err(PipelineAPIError::Failed)?
                .ok_or_else(|| lineage_not_established(pipeline))?;
                storage::adapters::SchemaKey::new(
                    tenant.as_str(),
                    source_id.as_str(),
                    record.current.lineage_hash.as_str(),
                    db,
                    table,
                )
            }
        };

        // Bounded pages; a table whose history exceeds the cap is refused rather
        // than materialized without bound.
        const MAX_VERSIONS: usize = 10_000;
        let registry = self.0.registry();
        let mut out = Vec::new();
        let mut cursor = None;
        loop {
            let page = registry
                .history_page(&key, cursor, 256)
                .await
                .map_err(PipelineAPIError::Failed)?;
            for v in page.versions {
                if out.len() == MAX_VERSIONS {
                    return Err(PipelineAPIError::BadRequest(format!(
                        "{db}.{table} has more than {MAX_VERSIONS} schema versions"
                    )));
                }
                let col_count = v
                    .schema_json
                    .get("columns")
                    .and_then(|c| c.as_array())
                    .map(|arr| arr.len())
                    .unwrap_or(0);
                out.push(SchemaVersionInfo {
                    version: v.version,
                    fingerprint: v.hash,
                    column_count: col_count,
                    registered_at: v.registered_at,
                });
            }
            match page.next {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        out.sort_by_key(|v| v.version);
        Ok(out)
    }
}

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
