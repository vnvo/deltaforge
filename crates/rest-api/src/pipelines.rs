use async_trait::async_trait;
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post},
};
use serde_json::Value;
use std::sync::Arc;

use crate::{PipelineAPIError, errors::pipeline_error};
use deltaforge_config::PipelineSpec;
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct AppState {
    pub controller: Arc<dyn PipelineController>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct PipeInfo {
    pub name: String,
    pub status: String,
    pub spec: PipelineSpec,
    /// Operational status - populated by the controller, optional for backward compat.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ops: Option<PipelineOpsStatus>,
}

/// Operational status fields - everything an operator needs in one response.
#[derive(Clone, Serialize, Deserialize, Default)]
pub struct PipelineOpsStatus {
    /// Replication lag in seconds (source event time vs wall clock).
    pub lag_seconds: Option<f64>,
    /// DLQ entry count (0 if journal not enabled).
    pub dlq_entries: u64,
    /// Last error per sink (empty if all healthy).
    pub sink_errors: std::collections::HashMap<String, String>,
    /// Pipeline uptime in seconds since last start/restart.
    pub uptime_seconds: Option<f64>,
    /// Per-sink checkpoint positions.
    pub checkpoints: Vec<CheckpointInfo>,
}

#[async_trait]
pub trait PipelineController: Send + Sync {
    /// List all pipelines.
    async fn list(&self) -> Vec<PipeInfo>;

    /// Get a single pipeline by name.
    async fn get(&self, name: &str) -> Result<PipeInfo, PipelineAPIError>;

    /// Create a new pipeline from spec.
    async fn create(
        &self,
        spec: PipelineSpec,
    ) -> Result<PipeInfo, PipelineAPIError>;

    /// Patch pipeline configuration.
    async fn patch(
        &self,
        name: &str,
        patch: Value,
    ) -> Result<PipeInfo, PipelineAPIError>;

    /// Pause a running pipeline.
    async fn pause(&self, name: &str) -> Result<PipeInfo, PipelineAPIError>;

    /// Resume a paused pipeline.
    async fn resume(&self, name: &str) -> Result<PipeInfo, PipelineAPIError>;

    /// Stop a pipeline (can be restarted).
    async fn stop(&self, name: &str) -> Result<PipeInfo, PipelineAPIError>;

    /// Delete a pipeline permanently.
    async fn delete(&self, name: &str) -> Result<(), PipelineAPIError>;

    // ── DLQ endpoints ──────────────────────────────────────────────────

    /// Peek DLQ entries for a pipeline. Returns JSON values.
    async fn dlq_peek(
        &self,
        name: &str,
        limit: usize,
    ) -> Result<Vec<Value>, PipelineAPIError> {
        let _ = (name, limit);
        Err(PipelineAPIError::Failed(anyhow::anyhow!(
            "DLQ not enabled for this pipeline"
        )))
    }

    /// Count of DLQ entries.
    async fn dlq_count(&self, name: &str) -> Result<u64, PipelineAPIError> {
        let _ = name;
        Err(PipelineAPIError::Failed(anyhow::anyhow!(
            "DLQ not enabled for this pipeline"
        )))
    }

    /// Acknowledge DLQ entries up to a sequence number.
    async fn dlq_ack(
        &self,
        name: &str,
        up_to_seq: u64,
    ) -> Result<usize, PipelineAPIError> {
        let _ = (name, up_to_seq);
        Err(PipelineAPIError::Failed(anyhow::anyhow!(
            "DLQ not enabled for this pipeline"
        )))
    }

    /// Purge all DLQ entries.
    async fn dlq_purge(&self, name: &str) -> Result<usize, PipelineAPIError> {
        let _ = name;
        Err(PipelineAPIError::Failed(anyhow::anyhow!(
            "DLQ not enabled for this pipeline"
        )))
    }

    // ── Event replay endpoints ─────────────────────────────────────────

    /// Start (or dry-run) a replay job for a pipeline. Returns the new job id.
    async fn replay_start(
        &self,
        name: &str,
        req: ReplayStartRequest,
    ) -> Result<ReplayStartResponse, PipelineAPIError> {
        let _ = (name, req);
        Err(PipelineAPIError::Failed(anyhow::anyhow!(
            "replay is not enabled for this pipeline"
        )))
    }

    /// The current (or most recent) replay job for a pipeline, if any.
    async fn replay_status(
        &self,
        name: &str,
    ) -> Result<Option<ReplayJobStatus>, PipelineAPIError> {
        let _ = name;
        Err(PipelineAPIError::Failed(anyhow::anyhow!(
            "replay is not enabled for this pipeline"
        )))
    }

    /// Cancel the active replay job for a pipeline (idempotent when none is active).
    async fn replay_cancel(&self, name: &str) -> Result<(), PipelineAPIError> {
        let _ = name;
        Err(PipelineAPIError::Failed(anyhow::anyhow!(
            "replay is not enabled for this pipeline"
        )))
    }

    // ── Checkpoint inspection ──────────────────────────────────────────

    /// Get per-sink checkpoint positions for a pipeline.
    async fn checkpoints(
        &self,
        name: &str,
    ) -> Result<Vec<CheckpointInfo>, PipelineAPIError> {
        let _ = name;
        Ok(vec![])
    }
}

/// Request body for `POST /pipelines/{name}/journal/replay`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplayStartRequest {
    /// Existing live sinks to pause and replay the range to.
    pub selected_sinks: Vec<String>,
    /// New sinks to backfill (not supported yet; must be empty).
    #[serde(default)]
    pub staged_sinks: Vec<String>,
    /// Lower bound of the range (log_since semantics: envelopes with seq > from_seq).
    pub from_seq: u64,
    /// Optional inclusive upper bound of the historical range.
    #[serde(default)]
    pub through_seq: Option<u64>,
    /// Encoder schema policy: "current" (default), "at_capture_seq", or "pinned:<seq>".
    #[serde(default)]
    pub encoder_schema_policy: Option<String>,
    /// Dry-run delivers nothing; reports the range/targets only.
    #[serde(default)]
    pub dry_run: bool,
}

/// Response body for a started replay job.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplayStartResponse {
    pub job_id: String,
}

/// A replay job's durable status.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplayJobStatus {
    pub job_id: String,
    pub phase: String,
    pub cursor: u64,
    pub from_seq: u64,
    pub through_seq: Option<u64>,
    pub selected_sinks: Vec<String>,
    pub staged_sinks: Vec<String>,
    pub dry_run: bool,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub error: Option<String>,
}

/// Per-sink checkpoint position returned by the inspection API.
#[derive(Clone, Serialize, Deserialize)]
pub struct CheckpointInfo {
    pub sink_id: String,
    pub position: Value,
    pub age_seconds: f64,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/pipelines", get(list_pipelines).post(create_pipeline))
        .route(
            "/pipelines/{name}",
            get(get_pipeline)
                .patch(patch_pipeline)
                .delete(delete_pipeline),
        )
        .route("/pipelines/{name}/pause", post(pause_pipeline))
        .route("/pipelines/{name}/resume", post(resume_pipeline))
        .route("/pipelines/{name}/stop", post(stop_pipeline))
        // DLQ endpoints
        .route(
            "/pipelines/{name}/journal/dlq",
            get(handle_dlq_peek).delete(handle_dlq_purge),
        )
        .route("/pipelines/{name}/journal/dlq/count", get(handle_dlq_count))
        .route("/pipelines/{name}/journal/dlq/ack", post(handle_dlq_ack))
        // Replay endpoints
        .route(
            "/pipelines/{name}/journal/replay",
            post(handle_replay_start).get(handle_replay_status),
        )
        .route(
            "/pipelines/{name}/journal/replay/cancel",
            post(handle_replay_cancel),
        )
        // Checkpoint inspection
        .route("/pipelines/{name}/checkpoints", get(handle_checkpoints))
        .with_state(state)
}

use crate::errors::ApiResult;

#[derive(Deserialize, Default)]
struct ListPipelinesParams {
    /// Filter by label: `?label=env:prod` or `?label=team:platform`.
    /// Multiple labels: `?label=env:prod&label=team:platform` (AND logic).
    #[serde(default)]
    label: Vec<String>,
}

async fn list_pipelines(
    State(st): State<AppState>,
    Query(params): Query<ListPipelinesParams>,
) -> Json<Vec<PipeInfo>> {
    let mut pipelines = st.controller.list().await;

    // Filter by labels (AND logic - all specified labels must match).
    if !params.label.is_empty() {
        pipelines.retain(|p| {
            let meta_labels = &p.spec.metadata.labels;
            params.label.iter().all(|filter| {
                if let Some((key, value)) = filter.split_once(':') {
                    meta_labels.get(key).map(|v| v == value).unwrap_or(false)
                } else {
                    // Key-only filter: label exists regardless of value
                    meta_labels.contains_key(filter)
                }
            })
        });
    }

    Json(pipelines)
}

async fn get_pipeline(
    State(st): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<PipeInfo> {
    st.controller
        .get(&name)
        .await
        .map(Json)
        .map_err(pipeline_error)
}

async fn create_pipeline(
    State(st): State<AppState>,
    Json(spec): Json<PipelineSpec>,
) -> ApiResult<PipeInfo> {
    st.controller
        .create(spec)
        .await
        .map(Json)
        .map_err(pipeline_error)
}

async fn patch_pipeline(
    State(st): State<AppState>,
    Path(name): Path<String>,
    Json(patch): Json<Value>,
) -> ApiResult<PipeInfo> {
    st.controller
        .patch(&name, patch)
        .await
        .map(Json)
        .map_err(pipeline_error)
}

async fn pause_pipeline(
    State(st): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<PipeInfo> {
    st.controller
        .pause(&name)
        .await
        .map(Json)
        .map_err(pipeline_error)
}

async fn resume_pipeline(
    State(st): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<PipeInfo> {
    st.controller
        .resume(&name)
        .await
        .map(Json)
        .map_err(pipeline_error)
}

async fn stop_pipeline(
    State(st): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<PipeInfo> {
    st.controller
        .stop(&name)
        .await
        .map(Json)
        .map_err(pipeline_error)
}

async fn delete_pipeline(
    State(st): State<AppState>,
    Path(name): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<crate::errors::ApiError>)> {
    // delete returns StatusCode directly, not wrapped in ApiResult
    st.controller
        .delete(&name)
        .await
        .map(|_| StatusCode::NO_CONTENT)
        .map_err(pipeline_error)
}

// ── DLQ handlers ─────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct DlqPeekParams {
    #[serde(default = "default_dlq_limit")]
    limit: usize,
    #[allow(dead_code)]
    sink_id: Option<String>,
    #[allow(dead_code)]
    error_kind: Option<String>,
}

fn default_dlq_limit() -> usize {
    50
}

async fn handle_dlq_peek(
    State(st): State<AppState>,
    Path(name): Path<String>,
    Query(params): Query<DlqPeekParams>,
) -> ApiResult<Vec<Value>> {
    let limit = params.limit.min(1000);
    let mut entries = st
        .controller
        .dlq_peek(&name, limit)
        .await
        .map_err(pipeline_error)?;

    // Server-side filtering by sink_id and error_kind.
    if let Some(ref sink_id) = params.sink_id {
        entries.retain(|e| {
            e.get("meta")
                .and_then(|m| m.get("sink_id"))
                .and_then(|v| v.as_str())
                .map(|s| s == sink_id)
                .unwrap_or(false)
        });
    }
    if let Some(ref error_kind) = params.error_kind {
        entries.retain(|e| {
            e.get("meta")
                .and_then(|m| m.get("error_kind"))
                .and_then(|v| v.as_str())
                .map(|s| s == error_kind)
                .unwrap_or(false)
        });
    }

    Ok(Json(entries))
}

#[derive(Serialize)]
struct DlqCountResponse {
    count: u64,
}

async fn handle_dlq_count(
    State(st): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<DlqCountResponse> {
    let count = st
        .controller
        .dlq_count(&name)
        .await
        .map_err(pipeline_error)?;
    Ok(Json(DlqCountResponse { count }))
}

#[derive(Deserialize)]
struct DlqAckRequest {
    up_to_seq: u64,
}

#[derive(Serialize)]
struct DlqAckResponse {
    acked: usize,
}

async fn handle_dlq_ack(
    State(st): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<DlqAckRequest>,
) -> ApiResult<DlqAckResponse> {
    let acked = st
        .controller
        .dlq_ack(&name, body.up_to_seq)
        .await
        .map_err(pipeline_error)?;
    Ok(Json(DlqAckResponse { acked }))
}

#[derive(Serialize)]
struct DlqPurgeResponse {
    purged: usize,
}

async fn handle_dlq_purge(
    State(st): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<DlqPurgeResponse> {
    let purged = st
        .controller
        .dlq_purge(&name)
        .await
        .map_err(pipeline_error)?;
    Ok(Json(DlqPurgeResponse { purged }))
}

// ── Checkpoint inspection handler ────────────────────────────────────────────

async fn handle_checkpoints(
    State(st): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<Vec<CheckpointInfo>> {
    st.controller
        .checkpoints(&name)
        .await
        .map(Json)
        .map_err(pipeline_error)
}

// ── Replay handlers ────────────────────────────────────────────────────────────

async fn handle_replay_start(
    State(st): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<ReplayStartRequest>,
) -> ApiResult<ReplayStartResponse> {
    st.controller
        .replay_start(&name, body)
        .await
        .map(Json)
        .map_err(pipeline_error)
}

async fn handle_replay_status(
    State(st): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<Option<ReplayJobStatus>> {
    st.controller
        .replay_status(&name)
        .await
        .map(Json)
        .map_err(pipeline_error)
}

async fn handle_replay_cancel(
    State(st): State<AppState>,
    Path(name): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<crate::errors::ApiError>)> {
    st.controller
        .replay_cancel(&name)
        .await
        .map(|_| StatusCode::NO_CONTENT)
        .map_err(pipeline_error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{Body, to_bytes},
        http::{Method, Request},
    };
    use deltaforge_config::{
        BatchConfig, Metadata, MysqlSrcCfg, RedisSinkCfg, SinkCfg, SnapshotCfg,
        SourceCfg, Spec,
    };
    use tower::ServiceExt;

    fn sample_pipe_info() -> PipeInfo {
        PipeInfo {
            name: "demo".to_string(),
            status: "running".to_string(),
            spec: deltaforge_config::PipelineSpec {
                metadata: Metadata {
                    name: "demo".to_string(),
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
                        on_schema_drift:
                            deltaforge_config::OnSchemaDrift::Adapt,
                    }),
                    processors: vec![],
                    sinks: vec![SinkCfg::Redis(RedisSinkCfg {
                        id: "redis".to_string(),
                        uri: "redis://localhost".to_string(),
                        stream: "events".to_string(),
                        key: None,
                        required: Some(true),
                        send_timeout_secs: Some(3),
                        batch_timeout_secs: Some(3),
                        connect_timeout_secs: Some(3),
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
            },
            ops: None,
        }
    }

    #[derive(Clone)]
    struct MockController {
        info: PipeInfo,
    }

    #[async_trait]
    impl PipelineController for MockController {
        async fn list(&self) -> Vec<PipeInfo> {
            vec![self.info.clone()]
        }

        async fn get(&self, name: &str) -> Result<PipeInfo, PipelineAPIError> {
            if name == self.info.name {
                Ok(self.info.clone())
            } else {
                Err(PipelineAPIError::NotFound(name.to_string()))
            }
        }

        async fn create(
            &self,
            _spec: PipelineSpec,
        ) -> Result<PipeInfo, PipelineAPIError> {
            Ok(self.info.clone())
        }

        async fn patch(
            &self,
            _name: &str,
            _patch: Value,
        ) -> Result<PipeInfo, PipelineAPIError> {
            Ok(self.info.clone())
        }

        async fn pause(
            &self,
            _name: &str,
        ) -> Result<PipeInfo, PipelineAPIError> {
            Ok(self.info.clone())
        }

        async fn resume(
            &self,
            _name: &str,
        ) -> Result<PipeInfo, PipelineAPIError> {
            Ok(self.info.clone())
        }

        async fn stop(
            &self,
            _name: &str,
        ) -> Result<PipeInfo, PipelineAPIError> {
            Ok(self.info.clone())
        }

        async fn delete(&self, name: &str) -> Result<(), PipelineAPIError> {
            if name == self.info.name {
                Ok(())
            } else {
                Err(PipelineAPIError::NotFound(name.to_string()))
            }
        }
    }

    #[tokio::test]
    async fn test_list_pipelines() {
        let app = router(AppState {
            controller: Arc::new(MockController {
                info: sample_pipe_info(),
            }),
        });

        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/pipelines")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(StatusCode::OK, resp.status());
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let pipelines: Vec<PipeInfo> = serde_json::from_slice(&body).unwrap();
        assert_eq!(pipelines.len(), 1);
        assert_eq!(pipelines[0].name, "demo");
    }

    #[tokio::test]
    async fn test_get_pipeline_found() {
        let app = router(AppState {
            controller: Arc::new(MockController {
                info: sample_pipe_info(),
            }),
        });

        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/pipelines/demo")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(StatusCode::OK, resp.status());
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let pipeline: PipeInfo = serde_json::from_slice(&body).unwrap();
        assert_eq!(pipeline.name, "demo");
        assert_eq!(pipeline.status, "running");
    }

    #[tokio::test]
    async fn test_get_pipeline_not_found() {
        let app = router(AppState {
            controller: Arc::new(MockController {
                info: sample_pipe_info(),
            }),
        });

        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/pipelines/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(StatusCode::NOT_FOUND, resp.status());
    }

    #[tokio::test]
    async fn test_delete_pipeline_success() {
        let app = router(AppState {
            controller: Arc::new(MockController {
                info: sample_pipe_info(),
            }),
        });

        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri("/pipelines/demo")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(StatusCode::NO_CONTENT, resp.status());
    }

    #[tokio::test]
    async fn test_delete_pipeline_not_found() {
        let app = router(AppState {
            controller: Arc::new(MockController {
                info: sample_pipe_info(),
            }),
        });

        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri("/pipelines/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(StatusCode::NOT_FOUND, resp.status());
    }

    /// A controller with working replay endpoints, recording the last start request.
    #[derive(Clone, Default)]
    struct ReplayMock {
        last_from_seq: Arc<std::sync::Mutex<Option<u64>>>,
        has_job: bool,
    }

    #[async_trait]
    impl PipelineController for ReplayMock {
        async fn list(&self) -> Vec<PipeInfo> {
            vec![]
        }
        async fn get(&self, name: &str) -> Result<PipeInfo, PipelineAPIError> {
            Err(PipelineAPIError::NotFound(name.to_string()))
        }
        async fn create(
            &self,
            _spec: deltaforge_config::PipelineSpec,
        ) -> Result<PipeInfo, PipelineAPIError> {
            Err(PipelineAPIError::Failed(anyhow::anyhow!("no")))
        }
        async fn patch(
            &self,
            name: &str,
            _p: Value,
        ) -> Result<PipeInfo, PipelineAPIError> {
            Err(PipelineAPIError::NotFound(name.to_string()))
        }
        async fn pause(
            &self,
            name: &str,
        ) -> Result<PipeInfo, PipelineAPIError> {
            Err(PipelineAPIError::NotFound(name.to_string()))
        }
        async fn resume(
            &self,
            name: &str,
        ) -> Result<PipeInfo, PipelineAPIError> {
            Err(PipelineAPIError::NotFound(name.to_string()))
        }
        async fn stop(&self, name: &str) -> Result<PipeInfo, PipelineAPIError> {
            Err(PipelineAPIError::NotFound(name.to_string()))
        }
        async fn delete(&self, _name: &str) -> Result<(), PipelineAPIError> {
            Ok(())
        }

        async fn replay_start(
            &self,
            _name: &str,
            req: ReplayStartRequest,
        ) -> Result<ReplayStartResponse, PipelineAPIError> {
            *self.last_from_seq.lock().unwrap() = Some(req.from_seq);
            Ok(ReplayStartResponse {
                job_id: "job-xyz".to_string(),
            })
        }
        async fn replay_status(
            &self,
            _name: &str,
        ) -> Result<Option<ReplayJobStatus>, PipelineAPIError> {
            Ok(self.has_job.then(|| ReplayJobStatus {
                job_id: "job-xyz".into(),
                phase: "running".into(),
                cursor: 5,
                from_seq: 0,
                through_seq: None,
                selected_sinks: vec!["kafka".into()],
                staged_sinks: vec![],
                dry_run: false,
                created_at_ms: 1,
                updated_at_ms: 2,
                error: None,
            }))
        }
        async fn replay_cancel(
            &self,
            _name: &str,
        ) -> Result<(), PipelineAPIError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn replay_start_returns_job_id() {
        let mock = ReplayMock::default();
        let app = router(AppState {
            controller: Arc::new(mock.clone()),
        });
        let body = serde_json::json!({
            "selected_sinks": ["kafka"],
            "from_seq": 42
        });
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/pipelines/demo/journal/replay")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(StatusCode::OK, resp.status());
        let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let out: ReplayStartResponse = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(out.job_id, "job-xyz");
        assert_eq!(*mock.last_from_seq.lock().unwrap(), Some(42));
    }

    #[tokio::test]
    async fn replay_status_reports_job_or_null() {
        // No active job -> null.
        let app = router(AppState {
            controller: Arc::new(ReplayMock::default()),
        });
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/pipelines/demo/journal/replay")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(StatusCode::OK, resp.status());
        let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let out: Option<ReplayJobStatus> =
            serde_json::from_slice(&bytes).unwrap();
        assert!(out.is_none());

        // Active job -> populated status.
        let app = router(AppState {
            controller: Arc::new(ReplayMock {
                has_job: true,
                ..Default::default()
            }),
        });
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/pipelines/demo/journal/replay")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let out: Option<ReplayJobStatus> =
            serde_json::from_slice(&bytes).unwrap();
        assert_eq!(out.unwrap().phase, "running");
    }

    #[tokio::test]
    async fn replay_cancel_returns_no_content() {
        let app = router(AppState {
            controller: Arc::new(ReplayMock::default()),
        });
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/pipelines/demo/journal/replay/cancel")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(StatusCode::NO_CONTENT, resp.status());
    }

    #[tokio::test]
    async fn replay_start_defaults_to_error_without_impl() {
        // MockController does not override replay_start -> trait default errors -> 500.
        let app = router(AppState {
            controller: Arc::new(MockController {
                info: sample_pipe_info(),
            }),
        });
        let body =
            serde_json::json!({ "selected_sinks": ["kafka"], "from_seq": 0 });
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/pipelines/demo/journal/replay")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(StatusCode::INTERNAL_SERVER_ERROR, resp.status());
    }
}
