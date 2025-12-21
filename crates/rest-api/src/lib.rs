use axum::Router;
mod errors;
mod health;
mod pipelines;
mod schemas;

pub use errors::{PipelineAPIError, pipeline_error};
pub use pipelines::{AppState, PipeInfo, PipelineController};
pub use schemas::{
    ColumnInfo, ReloadResult, SchemaController, SchemaDetail, SchemaInfo,
    SchemaState, SchemaVersionInfo, TableReloadStatus,
};

pub fn router(state: AppState) -> Router {
    let health_state = state.clone();
    let health = health::router(health_state);
    let pipeline_mgmt = pipelines::router(state);

    health.merge(pipeline_mgmt)
}

pub fn router_with_schemas(app_state: AppState, schema_state: SchemaState) -> Router {
    router(app_state).merge(schemas::router(schema_state))
}
#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{Body, to_bytes},
        http::{Method, Request, StatusCode},
    };
    use deltaforge_config::{
        BatchConfig, Metadata, MysqlSrcCfg, RedisSinkCfg, SinkCfg, SourceCfg,
        Spec,
    };
    use serde_json::json;
    use std::sync::Arc;
    use tower::ServiceExt;

    #[derive(Clone)]
    struct HappyController {
        info: PipeInfo,
    }

    #[async_trait::async_trait]
    impl PipelineController for HappyController {
        async fn list(&self) -> Vec<PipeInfo> {
            vec![self.info.clone()]
        }

        async fn create(
            &self,
            _spec: deltaforge_config::PipelineSpec,
        ) -> Result<PipeInfo, PipelineAPIError> {
            Ok(self.info.clone())
        }

        async fn patch(
            &self,
            _name: &str,
            _patch: serde_json::Value,
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
    }

    #[derive(Clone)]
    struct ErrorController;

    #[async_trait::async_trait]
    impl PipelineController for ErrorController {
        async fn list(&self) -> Vec<PipeInfo> {
            vec![]
        }

        async fn create(
            &self,
            _spec: deltaforge_config::PipelineSpec,
        ) -> Result<PipeInfo, PipelineAPIError> {
            Err(PipelineAPIError::AlreadyExists("demo".to_string()))
        }

        async fn patch(
            &self,
            _name: &str,
            _patch: serde_json::Value,
        ) -> Result<PipeInfo, PipelineAPIError> {
            Err(PipelineAPIError::NotFound("missing".to_string()))
        }

        async fn pause(
            &self,
            _name: &str,
        ) -> Result<PipeInfo, PipelineAPIError> {
            Err(PipelineAPIError::Failed(anyhow::anyhow!("pause failed")))
        }

        async fn resume(
            &self,
            _name: &str,
        ) -> Result<PipeInfo, PipelineAPIError> {
            Err(PipelineAPIError::NameMismatch {
                expected: "demo".to_string(),
                found: "other".to_string(),
            })
        }

        async fn stop(
            &self,
            _name: &str,
        ) -> Result<PipeInfo, PipelineAPIError> {
            Err(PipelineAPIError::Failed(anyhow::anyhow!("stop failed")))
        }
    }

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
                    })],
                    connection_policy: None,
                    batch: Some(BatchConfig::default()),
                    commit_policy: None,
                },
            },
        }
    }

    fn sample_spec_json() -> Body {
        let spec = sample_pipe_info().spec;
        Body::from(serde_json::to_vec(&spec).expect("spec serialization"))
    }

    #[tokio::test]
    async fn health_routes_expose_pipeline_state() {
        let controller = HappyController {
            info: sample_pipe_info(),
        };
        let app = router(AppState {
            manager: Arc::new(controller),
        });

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(StatusCode::OK, resp.status());
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&body[..], b"ok");

        let ready = app
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(StatusCode::OK, ready.status());
        let payload: serde_json::Value = serde_json::from_slice(
            &to_bytes(ready.into_body(), usize::MAX)
                .await
                .expect("to bytes"),
        )
        .expect("ready payload");

        assert_eq!(payload["status"], json!("ready"));
        assert_eq!(payload["pipelines"][0]["name"], json!("demo"));
    }

    #[tokio::test]
    async fn pipeline_routes_surface_errors() {
        let controller = ErrorController;
        let app = router(AppState {
            manager: Arc::new(controller),
        });

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/pipelines")
                    .header("content-type", "application/json")
                    .body(sample_spec_json())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(StatusCode::CONFLICT, resp.status());
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let body_text = String::from_utf8(body.to_vec()).unwrap();
        assert!(body_text.contains("already exists"));

        let patch = app
            .oneshot(
                Request::builder()
                    .method(Method::PATCH)
                    .uri("/pipelines/missing")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"spec": {"batch": {"max_events": 5}}})
                            .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(StatusCode::NOT_FOUND, patch.status());
    }
}
