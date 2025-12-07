use async_trait::async_trait;
use axum::{Json, Router, extract::{Path, State}, http::StatusCode, routing::{get, patch, post}};
use serde_json::Value;
use std::sync::Arc;

use deltaforge_config::PipelineSpec;
use serde::{Serialize, Deserialize};
use crate::{PipelineAPIError, errors::pipeline_error};

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
    async fn list(&self) -> Vec<PipeInfo>;
    async fn create(&self, spec: PipelineSpec) -> Result<PipeInfo, PipelineAPIError>;
    async fn patch(&self, name: &str, patch: Value) -> Result<PipeInfo, PipelineAPIError>;
    async fn pause(&self, name: &str) -> Result<PipeInfo, PipelineAPIError>;
    async fn resume(&self, name: &str) -> Result<PipeInfo, PipelineAPIError>;
    async fn stop(&self, name: &str) -> Result<PipeInfo, PipelineAPIError>;
}


pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/pipelines", get(get_pipelines).post(create_pipeline))
        .route("/pipelines/{name}", patch(patch_pipeline))
        .route("/pipelines/{name}/pause", post(pause_pipeline))
        .route("/pipelines/{name}/resume", post(resume_pipeline))
        .route("/pipelines/{name}/stop", post(stop_pipeline))
        .with_state(state)
}

type ApiResult<T> = Result<Json<T>, (StatusCode, String)>;

async fn get_pipelines(State(st): State<AppState>) -> Json<Vec<PipeInfo>> {
    Json(st.manager.list().await)
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
