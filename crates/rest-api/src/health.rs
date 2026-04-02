use axum::{
    Json, Router, extract::State, http::StatusCode, response::IntoResponse,
    routing::get,
};
use serde::Serialize;

use crate::pipelines::{AppState, PipeInfo};

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(healthz))
        .route("/ready", get(readyz))
        .with_state(state)
}

async fn healthz(State(st): State<AppState>) -> impl IntoResponse {
    let pipelines = st.controller.list().await;
    let failed: Vec<_> = pipelines
        .iter()
        .filter(|p| p.status == "failed")
        .map(|p| p.name.clone())
        .collect();

    if !failed.is_empty() {
        let body = serde_json::json!({
            "status": "unhealthy",
            "failed_pipelines": failed,
        });
        return (StatusCode::SERVICE_UNAVAILABLE, Json(body)).into_response();
    }
    let body = serde_json::json!({
        "status": "healthy",
        "pipelines": pipelines.len(),
    });
    (StatusCode::OK, Json(body)).into_response()
}

#[derive(Serialize)]
struct ReadyStatus {
    status: &'static str,
    pipelines: Vec<PipeInfo>,
}

async fn readyz(State(st): State<AppState>) -> Json<ReadyStatus> {
    // Basic readiness surface that reflects pipeline states.
    // Future revisions can incorporate dependency checks.
    let pipelines = st.controller.list().await;
    Json(ReadyStatus {
        status: "ready",
        pipelines,
    })
}
