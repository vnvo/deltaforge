//! Schema API - Database schema management endpoints.

use std::sync::Arc;
use std::time::Instant;

use rest_api::{
    ColumnInfo, PipelineAPIError, ReloadResult, SchemaController, SchemaDetail,
    SchemaInfo, SchemaVersionInfo, TableReloadStatus,
};
use serde_json::Value;

use crate::pipeline_manager::PipelineManager;

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
            .map_err(PipelineAPIError::Failed)?;

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
        let loader = self.0.get_loader(pipeline)?;
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
            .0
            .pipelines
            .read()
            .get(pipeline)
            .ok_or_else(|| PipelineAPIError::NotFound(pipeline.to_string()))?
            .spec
            .metadata
            .tenant
            .clone();

        Ok(self
            .0
            .registry()
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
