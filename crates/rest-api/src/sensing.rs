//! Schema sensing and drift detection API endpoints.
//!
//! Exposes inferred schemas from JSON payloads and drift detection results.

use async_trait::async_trait;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::get,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::PipelineAPIError;
use crate::errors::pipeline_error;

/// Controller trait for schema sensing operations.
#[async_trait]
pub trait SensingController: Send + Sync {
    /// List all inferred schemas for a pipeline.
    async fn list_inferred(
        &self,
        pipeline: &str,
    ) -> Result<Vec<InferredSchemaInfo>, PipelineAPIError>;

    /// Get detailed inferred schema for a table.
    async fn get_inferred(
        &self,
        pipeline: &str,
        table: &str,
    ) -> Result<InferredSchemaDetail, PipelineAPIError>;

    /// Export schema as JSON Schema format.
    async fn export_json_schema(
        &self,
        pipeline: &str,
        table: &str,
    ) -> Result<serde_json::Value, PipelineAPIError>;

    /// Get drift detection results for a pipeline.
    async fn get_drift(
        &self,
        pipeline: &str,
    ) -> Result<Vec<DriftInfo>, PipelineAPIError>;

    /// Get drift for a specific table.
    async fn get_table_drift(
        &self,
        pipeline: &str,
        table: &str,
    ) -> Result<DriftInfo, PipelineAPIError>;

    /// Get cache statistics for schema sensing.
    async fn get_cache_stats(
        &self,
        pipeline: &str,
    ) -> Result<CacheStats, PipelineAPIError>;
}

/// Summary of an inferred schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferredSchemaInfo {
    /// Table name (or table:column for JSON column sensing)
    pub table: String,
    /// Current schema fingerprint
    pub fingerprint: String,
    /// Monotonic sequence number (increments on evolution)
    pub sequence: u64,
    /// Number of events observed
    pub event_count: u64,
    /// Whether schema has stabilized (stopped sampling)
    pub stabilized: bool,
    /// First observation timestamp
    pub first_seen: DateTime<Utc>,
    /// Most recent observation timestamp
    pub last_seen: DateTime<Utc>,
}

/// Detailed inferred schema with field information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferredSchemaDetail {
    /// Table name
    pub table: String,
    /// Current schema fingerprint
    pub fingerprint: String,
    /// Sequence number
    pub sequence: u64,
    /// Number of events observed
    pub event_count: u64,
    /// Whether schema has stabilized
    pub stabilized: bool,
    /// Inferred fields
    pub fields: Vec<InferredField>,
    /// First observation timestamp
    pub first_seen: DateTime<Utc>,
    /// Most recent observation timestamp
    pub last_seen: DateTime<Utc>,
}

/// An inferred field from JSON payloads.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferredField {
    /// Field name (path for nested fields, e.g., "metadata.tags")
    pub name: String,
    /// Inferred type(s) - may be multiple if field has varying types
    pub types: Vec<String>,
    /// Whether field is nullable (seen as null in some events)
    pub nullable: bool,
    /// Whether field is optional (missing in some events)
    pub optional: bool,
    /// For arrays: inferred element types
    #[serde(skip_serializing_if = "Option::is_none")]
    pub array_element_types: Option<Vec<String>>,
    /// For objects: nested field count
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nested_field_count: Option<usize>,
}

/// Drift detection information for a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriftInfo {
    /// Table name
    pub table: String,
    /// Whether drift has been detected
    pub has_drift: bool,
    /// Drift details per column
    pub columns: Vec<ColumnDrift>,
    /// Total events analyzed
    pub events_analyzed: u64,
    /// Events with drift detected
    pub events_with_drift: u64,
}

/// Drift information for a single column.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDrift {
    /// Column name
    pub column: String,
    /// Expected type from DB schema
    pub expected_type: String,
    /// Observed types in events
    pub observed_types: Vec<String>,
    /// Number of mismatches
    pub mismatch_count: u64,
    /// Example mismatched values (truncated)
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub examples: Vec<String>,
}

/// Cache statistics for schema sensing performance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheStats {
    /// Per-table cache statistics
    pub tables: Vec<TableCacheStats>,
    /// Total cache hits across all tables
    pub total_cache_hits: u64,
    /// Total cache misses across all tables
    pub total_cache_misses: u64,
    /// Overall hit rate (0.0 - 1.0)
    pub hit_rate: f64,
}

/// Cache statistics for a single table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableCacheStats {
    /// Table name
    pub table: String,
    /// Number of unique structures cached
    pub cached_structures: usize,
    /// Maximum cache size
    pub max_cache_size: usize,
    /// Events processed via cache hit
    pub cache_hits: u64,
    /// Events requiring full sensing
    pub cache_misses: u64,
}

/// State for sensing routes.
#[derive(Clone)]
pub struct SensingState {
    pub controller: Arc<dyn SensingController>,
}

/// Build schema sensing routes.
pub fn router(state: SensingState) -> Router {
    Router::new()
        // Inferred schemas
        .route(
            "/pipelines/{pipeline}/sensing/schemas",
            get(list_inferred_schemas),
        )
        .route(
            "/pipelines/{pipeline}/sensing/schemas/{table}",
            get(get_inferred_schema),
        )
        .route(
            "/pipelines/{pipeline}/sensing/schemas/{table}/json-schema",
            get(export_json_schema),
        )
        // Drift detection
        .route("/pipelines/{pipeline}/drift", get(get_drift))
        .route("/pipelines/{pipeline}/drift/{table}", get(get_table_drift))
        // Cache stats
        .route("/pipelines/{pipeline}/sensing/stats", get(get_cache_stats))
        .with_state(state)
}

type ApiResult<T> = Result<Json<T>, (StatusCode, String)>;

async fn list_inferred_schemas(
    State(st): State<SensingState>,
    Path(pipeline): Path<String>,
) -> ApiResult<Vec<InferredSchemaInfo>> {
    st.controller
        .list_inferred(&pipeline)
        .await
        .map(Json)
        .map_err(pipeline_error)
}

async fn get_inferred_schema(
    State(st): State<SensingState>,
    Path((pipeline, table)): Path<(String, String)>,
) -> ApiResult<InferredSchemaDetail> {
    st.controller
        .get_inferred(&pipeline, &table)
        .await
        .map(Json)
        .map_err(pipeline_error)
}

async fn export_json_schema(
    State(st): State<SensingState>,
    Path((pipeline, table)): Path<(String, String)>,
) -> ApiResult<serde_json::Value> {
    st.controller
        .export_json_schema(&pipeline, &table)
        .await
        .map(Json)
        .map_err(pipeline_error)
}

async fn get_drift(
    State(st): State<SensingState>,
    Path(pipeline): Path<String>,
) -> ApiResult<Vec<DriftInfo>> {
    st.controller
        .get_drift(&pipeline)
        .await
        .map(Json)
        .map_err(pipeline_error)
}

async fn get_table_drift(
    State(st): State<SensingState>,
    Path((pipeline, table)): Path<(String, String)>,
) -> ApiResult<DriftInfo> {
    st.controller
        .get_table_drift(&pipeline, &table)
        .await
        .map(Json)
        .map_err(pipeline_error)
}

async fn get_cache_stats(
    State(st): State<SensingState>,
    Path(pipeline): Path<String>,
) -> ApiResult<CacheStats> {
    st.controller
        .get_cache_stats(&pipeline)
        .await
        .map(Json)
        .map_err(pipeline_error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{Body, to_bytes},
        http::{Method, Request},
    };
    use tower::ServiceExt;

    #[derive(Clone)]
    struct MockSensingController;

    #[async_trait]
    impl SensingController for MockSensingController {
        async fn list_inferred(
            &self,
            _pipeline: &str,
        ) -> Result<Vec<InferredSchemaInfo>, PipelineAPIError> {
            Ok(vec![InferredSchemaInfo {
                table: "orders".into(),
                fingerprint: "sha256:abc123".into(),
                sequence: 3,
                event_count: 1500,
                stabilized: true,
                first_seen: Utc::now(),
                last_seen: Utc::now(),
            }])
        }

        async fn get_inferred(
            &self,
            _pipeline: &str,
            table: &str,
        ) -> Result<InferredSchemaDetail, PipelineAPIError> {
            Ok(InferredSchemaDetail {
                table: table.into(),
                fingerprint: "sha256:abc123".into(),
                sequence: 3,
                event_count: 1500,
                stabilized: true,
                fields: vec![
                    InferredField {
                        name: "id".into(),
                        types: vec!["integer".into()],
                        nullable: false,
                        optional: false,
                        array_element_types: None,
                        nested_field_count: None,
                    },
                    InferredField {
                        name: "metadata".into(),
                        types: vec!["object".into()],
                        nullable: true,
                        optional: false,
                        array_element_types: None,
                        nested_field_count: Some(5),
                    },
                ],
                first_seen: Utc::now(),
                last_seen: Utc::now(),
            })
        }

        async fn export_json_schema(
            &self,
            _pipeline: &str,
            table: &str,
        ) -> Result<serde_json::Value, PipelineAPIError> {
            Ok(serde_json::json!({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "title": table,
                "type": "object",
                "properties": {
                    "id": { "type": "integer" },
                    "metadata": { "type": ["object", "null"] }
                },
                "required": ["id", "metadata"]
            }))
        }

        async fn get_drift(
            &self,
            _pipeline: &str,
        ) -> Result<Vec<DriftInfo>, PipelineAPIError> {
            Ok(vec![DriftInfo {
                table: "orders".into(),
                has_drift: true,
                columns: vec![ColumnDrift {
                    column: "amount".into(),
                    expected_type: "decimal(10,2)".into(),
                    observed_types: vec!["string".into()],
                    mismatch_count: 42,
                    examples: vec!["\"99.99\"".into()],
                }],
                events_analyzed: 1500,
                events_with_drift: 42,
            }])
        }

        async fn get_table_drift(
            &self,
            _pipeline: &str,
            table: &str,
        ) -> Result<DriftInfo, PipelineAPIError> {
            Ok(DriftInfo {
                table: table.into(),
                has_drift: false,
                columns: vec![],
                events_analyzed: 1000,
                events_with_drift: 0,
            })
        }

        async fn get_cache_stats(
            &self,
            _pipeline: &str,
        ) -> Result<CacheStats, PipelineAPIError> {
            Ok(CacheStats {
                tables: vec![TableCacheStats {
                    table: "orders".into(),
                    cached_structures: 3,
                    max_cache_size: 100,
                    cache_hits: 1450,
                    cache_misses: 50,
                }],
                total_cache_hits: 1450,
                total_cache_misses: 50,
                hit_rate: 0.9667,
            })
        }
    }

    fn make_app() -> Router {
        router(SensingState {
            controller: Arc::new(MockSensingController),
        })
    }

    #[tokio::test]
    async fn test_list_inferred_schemas() {
        let resp = make_app()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/pipelines/test/sensing/schemas")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(StatusCode::OK, resp.status());
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let schemas: Vec<InferredSchemaInfo> =
            serde_json::from_slice(&body).unwrap();
        assert_eq!(schemas.len(), 1);
        assert_eq!(schemas[0].table, "orders");
        assert!(schemas[0].stabilized);
    }

    #[tokio::test]
    async fn test_get_inferred_schema() {
        let resp = make_app()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/pipelines/test/sensing/schemas/orders")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(StatusCode::OK, resp.status());
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let detail: InferredSchemaDetail =
            serde_json::from_slice(&body).unwrap();
        assert_eq!(detail.table, "orders");
        assert_eq!(detail.fields.len(), 2);
    }

    #[tokio::test]
    async fn test_export_json_schema() {
        let resp = make_app()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/pipelines/test/sensing/schemas/orders/json-schema")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(StatusCode::OK, resp.status());
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let schema: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(schema["type"], "object");
        assert!(schema["properties"]["id"].is_object());
    }

    #[tokio::test]
    async fn test_get_drift() {
        let resp = make_app()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/pipelines/test/drift")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(StatusCode::OK, resp.status());
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let drift: Vec<DriftInfo> = serde_json::from_slice(&body).unwrap();
        assert_eq!(drift.len(), 1);
        assert!(drift[0].has_drift);
        assert_eq!(drift[0].columns[0].mismatch_count, 42);
    }

    #[tokio::test]
    async fn test_get_cache_stats() {
        let resp = make_app()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/pipelines/test/sensing/stats")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(StatusCode::OK, resp.status());
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let stats: CacheStats = serde_json::from_slice(&body).unwrap();
        assert_eq!(stats.total_cache_hits, 1450);
        assert!(stats.hit_rate > 0.96);
    }
}
