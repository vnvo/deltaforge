use axum::{Json, Router, extract::State, routing::get};
use serde::Serialize;

use crate::pipelines::{AppState, PipeInfo};

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .with_state(state)
}

async fn healthz() -> &'static str {
    "ok"
}

#[derive(Serialize)]
struct ReadyStatus {
    status: &'static str,
    pipelines: Vec<PipeInfo>,
}

async fn readyz(State(st): State<AppState>) -> Json<ReadyStatus> {
    // Basic readiness surface that reflects pipeline states.
    // Future revisions can incorporate dependency checks.
    let pipelines = st.manager.list().await;
    Json(ReadyStatus {
        status: "ready",
        pipelines,
    })
}
