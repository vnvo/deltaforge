use axum::{
    extract::{Path, State},
    routing::{get, post},
    Json, Router,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::info;

#[derive(Clone, Default)]
pub struct AppState {
    pub pipelines: Arc<RwLock<Vec<PipeInfo>>>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct PipeInfo {
    pub id: String,
    pub status: String,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/pipelines", get(get_pipelines).post(create_pipeline))
        .route("/pipelines/{id}/connection-mode", post(set_connection_mode))
        .with_state(state)
}

async fn get_pipelines(
    axum::extract::State(st): axum::extract::State<AppState>,
) -> Json<Vec<PipeInfo>> {
    Json(st.pipelines.read().clone())
}

async fn create_pipeline(
    axum::extract::State(st): axum::extract::State<AppState>,
    Json(pi): Json<PipeInfo>,
) -> Json<PipeInfo> {
    info!(id = %pi.id, "create pipeline (noop placeholder)");
    st.pipelines.write().push(pi.clone());
    Json(pi)
}

#[derive(Deserialize)]
struct ModeReq {
    mode: String,
}

async fn set_connection_mode(
    State(_st): State<AppState>,
    Path(_id): Path<String>,
    Json(_mr): Json<ModeReq>,
) -> Json<serde_json::Value> {
    // TODO: Wire to runner coordinator to perform safe cutover shared<->dedicated
    Json(serde_json::json!({"status":"accepted"}))
}
