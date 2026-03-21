use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::IntoResponse,
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
    if pipelines.iter().any(|p| p.status == "failed") {
        return (StatusCode::SERVICE_UNAVAILABLE, "pipeline failed\n")
            .into_response();
    }
    (StatusCode::OK, "ok\n").into_response()
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
