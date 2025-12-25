use async_trait::async_trait;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get, patch, post},
};
use serde_json::Value;
use std::sync::Arc;

use crate::{PipelineAPIError, errors::pipeline_error};
use deltaforge_config::PipelineSpec;
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct AppState {
    pub manager: Arc<dyn PipelineController>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct PipeInfo {
    pub name: String,
    pub status: String,
    pub spec: PipelineSpec,
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
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/pipelines", get(list_pipelines).post(create_pipeline))
        .route(
            "/pipelines/{name}",
            get(get_pipeline).patch(patch_pipeline).delete(delete_pipeline),
        )
        .route("/pipelines/{name}/pause", post(pause_pipeline))
        .route("/pipelines/{name}/resume", post(resume_pipeline))
        .route("/pipelines/{name}/stop", post(stop_pipeline))
        .with_state(state)
}

type ApiResult<T> = Result<Json<T>, (StatusCode, String)>;

async fn list_pipelines(State(st): State<AppState>) -> Json<Vec<PipeInfo>> {
    Json(st.manager.list().await)
}

async fn get_pipeline(
    State(st): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<PipeInfo> {
    st.manager
        .get(&name)
        .await
        .map(Json)
        .map_err(pipeline_error)
}

async fn create_pipeline(
    State(st): State<AppState>,
    Json(spec): Json<PipelineSpec>,
) -> ApiResult<PipeInfo> {
    st.manager
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
    st.manager
        .patch(&name, patch)
        .await
        .map(Json)
        .map_err(pipeline_error)
}

async fn pause_pipeline(
    State(st): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<PipeInfo> {
    st.manager
        .pause(&name)
        .await
        .map(Json)
        .map_err(pipeline_error)
}

async fn resume_pipeline(
    State(st): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<PipeInfo> {
    st.manager
        .resume(&name)
        .await
        .map(Json)
        .map_err(pipeline_error)
}

async fn stop_pipeline(
    State(st): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<PipeInfo> {
    st.manager
        .stop(&name)
        .await
        .map(Json)
        .map_err(pipeline_error)
}

async fn delete_pipeline(
    State(st): State<AppState>,
    Path(name): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    st.manager
        .delete(&name)
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
        BatchConfig, Metadata, MysqlSrcCfg, RedisSinkCfg, SinkCfg, SourceCfg,
        Spec,
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
                        required: Some(true),
                    })],
                    connection_policy: None,
                    batch: Some(BatchConfig::default()),
                    commit_policy: None,
                    schema_sensing: Default::default(),
                },
            },
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
            manager: Arc::new(MockController {
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
            manager: Arc::new(MockController {
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
            manager: Arc::new(MockController {
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
            manager: Arc::new(MockController {
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
            manager: Arc::new(MockController {
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
}