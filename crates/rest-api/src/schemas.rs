use async_trait::async_trait;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::PipelineAPIError;
use crate::errors::pipeline_error;

/// Schema controller trait for pipeline schema operations.
#[async_trait]
pub trait SchemaController: Send + Sync {
    async fn list_schemas(
        &self,
        pipeline: &str,
    ) -> Result<Vec<SchemaInfo>, PipelineAPIError>;

    async fn get_schema(
        &self,
        pipeline: &str,
        db: &str,
        table: &str,
    ) -> Result<SchemaDetail, PipelineAPIError>;

    async fn reload_schemas(
        &self,
        pipeline: &str,
    ) -> Result<ReloadResult, PipelineAPIError>;

    async fn reload_table_schema(
        &self,
        pipeline: &str,
        db: &str,
        table: &str,
    ) -> Result<SchemaDetail, PipelineAPIError>;

    async fn get_schema_versions(
        &self,
        pipeline: &str,
        db: &str,
        table: &str,
    ) -> Result<Vec<SchemaVersionInfo>, PipelineAPIError>;
}

/// Schema summary info.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaInfo {
    pub database: String,
    pub table: String,
    pub column_count: usize,
    pub primary_key: Vec<String>,
    pub fingerprint: String,
    pub registry_version: i32,
}

/// Detailed schema information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaDetail {
    pub database: String,
    pub table: String,
    pub columns: Vec<ColumnInfo>,
    pub primary_key: Vec<String>,
    pub engine: Option<String>,
    pub charset: Option<String>,
    pub collation: Option<String>,
    pub fingerprint: String,
    pub registry_version: i32,
    pub loaded_at: DateTime<Utc>,
}

/// Column information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub column_type: String,
    pub data_type: String,
    pub nullable: bool,
    pub ordinal_position: u32,
    pub default_value: Option<String>,
    pub extra: Option<String>,
    pub is_primary_key: bool,
}

/// Schema version history entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaVersionInfo {
    pub version: i32,
    pub fingerprint: String,
    pub column_count: usize,
    pub registered_at: DateTime<Utc>,
}

/// Result of schema reload operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReloadResult {
    pub pipeline: String,
    pub tables_reloaded: usize,
    pub tables: Vec<TableReloadStatus>,
    pub elapsed_ms: u64,
}

/// Status of individual table reload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableReloadStatus {
    pub database: String,
    pub table: String,
    pub status: String,
    pub changed: bool,
    pub error: Option<String>,
}

/// State for schema routes.
#[derive(Clone)]
pub struct SchemaState {
    pub controller: Arc<dyn SchemaController>,
}

/// Build schema management routes.
pub fn router(state: SchemaState) -> Router {
    Router::new()
        .route("/pipelines/{pipeline}/schemas", get(list_schemas))
        .route(
            "/pipelines/{pipeline}/schemas/{db}/{table}",
            get(get_schema),
        )
        .route("/pipelines/{pipeline}/schemas/reload", post(reload_schemas))
        .route(
            "/pipelines/{pipeline}/schemas/{db}/{table}/reload",
            post(reload_table_schema),
        )
        .route(
            "/pipelines/{pipeline}/schemas/{db}/{table}/versions",
            get(get_schema_versions),
        )
        .with_state(state)
}

type ApiResult<T> = Result<Json<T>, (StatusCode, String)>;

async fn list_schemas(
    State(st): State<SchemaState>,
    Path(pipeline): Path<String>,
) -> ApiResult<Vec<SchemaInfo>> {
    st.controller
        .list_schemas(&pipeline)
        .await
        .map(Json)
        .map_err(pipeline_error)
}

async fn get_schema(
    State(st): State<SchemaState>,
    Path((pipeline, db, table)): Path<(String, String, String)>,
) -> ApiResult<SchemaDetail> {
    st.controller
        .get_schema(&pipeline, &db, &table)
        .await
        .map(Json)
        .map_err(pipeline_error)
}

async fn reload_schemas(
    State(st): State<SchemaState>,
    Path(pipeline): Path<String>,
) -> ApiResult<ReloadResult> {
    st.controller
        .reload_schemas(&pipeline)
        .await
        .map(Json)
        .map_err(pipeline_error)
}

async fn reload_table_schema(
    State(st): State<SchemaState>,
    Path((pipeline, db, table)): Path<(String, String, String)>,
) -> ApiResult<SchemaDetail> {
    st.controller
        .reload_table_schema(&pipeline, &db, &table)
        .await
        .map(Json)
        .map_err(pipeline_error)
}

async fn get_schema_versions(
    State(st): State<SchemaState>,
    Path((pipeline, db, table)): Path<(String, String, String)>,
) -> ApiResult<Vec<SchemaVersionInfo>> {
    st.controller
        .get_schema_versions(&pipeline, &db, &table)
        .await
        .map(Json)
        .map_err(pipeline_error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{Body, to_bytes},
        http::{Method, Request, StatusCode},
    };
    use tower::ServiceExt;

    #[derive(Clone)]
    struct MockController;

    #[async_trait]
    impl SchemaController for MockController {
        async fn list_schemas(
            &self,
            _pipeline: &str,
        ) -> Result<Vec<SchemaInfo>, PipelineAPIError> {
            Ok(vec![SchemaInfo {
                database: "orders".into(),
                table: "items".into(),
                column_count: 5,
                primary_key: vec!["id".into()],
                fingerprint: "abc123".into(),
                registry_version: 1,
            }])
        }

        async fn get_schema(
            &self,
            _pipeline: &str,
            db: &str,
            table: &str,
        ) -> Result<SchemaDetail, PipelineAPIError> {
            Ok(SchemaDetail {
                database: db.into(),
                table: table.into(),
                columns: vec![ColumnInfo {
                    name: "id".into(),
                    column_type: "bigint(20)".into(),
                    data_type: "bigint".into(),
                    nullable: false,
                    ordinal_position: 1,
                    default_value: None,
                    extra: Some("auto_increment".into()),
                    is_primary_key: true,
                }],
                primary_key: vec!["id".into()],
                engine: Some("InnoDB".into()),
                charset: None,
                collation: None,
                fingerprint: "abc123".into(),
                registry_version: 1,
                loaded_at: Utc::now(),
            })
        }

        async fn reload_schemas(
            &self,
            pipeline: &str,
        ) -> Result<ReloadResult, PipelineAPIError> {
            Ok(ReloadResult {
                pipeline: pipeline.into(),
                tables_reloaded: 1,
                tables: vec![TableReloadStatus {
                    database: "orders".into(),
                    table: "items".into(),
                    status: "ok".into(),
                    changed: false,
                    error: None,
                }],
                elapsed_ms: 50,
            })
        }

        async fn reload_table_schema(
            &self,
            _pipeline: &str,
            db: &str,
            table: &str,
        ) -> Result<SchemaDetail, PipelineAPIError> {
            self.get_schema(_pipeline, db, table).await
        }

        async fn get_schema_versions(
            &self,
            _pipeline: &str,
            _db: &str,
            _table: &str,
        ) -> Result<Vec<SchemaVersionInfo>, PipelineAPIError> {
            Ok(vec![SchemaVersionInfo {
                version: 1,
                fingerprint: "abc123".into(),
                column_count: 5,
                registered_at: Utc::now(),
            }])
        }
    }

    #[tokio::test]
    async fn test_list_schemas() {
        let app = router(SchemaState {
            controller: Arc::new(MockController),
        });

        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/pipelines/test/schemas")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(StatusCode::OK, resp.status());
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let schemas: Vec<SchemaInfo> = serde_json::from_slice(&body).unwrap();
        assert_eq!(schemas.len(), 1);
        assert_eq!(schemas[0].table, "items");
    }

    #[tokio::test]
    async fn test_get_schema() {
        let app = router(SchemaState {
            controller: Arc::new(MockController),
        });

        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/pipelines/test/schemas/orders/items")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(StatusCode::OK, resp.status());
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let detail: SchemaDetail = serde_json::from_slice(&body).unwrap();
        assert_eq!(detail.database, "orders");
        assert_eq!(detail.columns.len(), 1);
    }

    #[tokio::test]
    async fn test_reload_schemas() {
        let app = router(SchemaState {
            controller: Arc::new(MockController),
        });

        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/pipelines/test/schemas/reload")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(StatusCode::OK, resp.status());
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let result: ReloadResult = serde_json::from_slice(&body).unwrap();
        assert_eq!(result.tables_reloaded, 1);
    }

    #[tokio::test]
    async fn test_get_schema_versions() {
        let app = router(SchemaState {
            controller: Arc::new(MockController),
        });

        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/pipelines/test/schemas/orders/items/versions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(StatusCode::OK, resp.status());
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let versions: Vec<SchemaVersionInfo> =
            serde_json::from_slice(&body).unwrap();
        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].version, 1);
    }
}
