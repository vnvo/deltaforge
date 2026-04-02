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
        .route("/log-level", get(get_log_level))
        .route("/validate", axum::routing::post(validate_config))
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

// ── Log level ────────────────────────────────────────────────────────────────

#[derive(Serialize)]
struct LogLevelResponse {
    level: String,
}

async fn get_log_level() -> Json<LogLevelResponse> {
    let level =
        std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
    Json(LogLevelResponse { level })
}

// ── Config validation ────────────────────────────────────────────────────────

/// Validate a pipeline config without creating it. Accepts JSON body.
/// Returns {"valid": true, ...} or {"valid": false, "error": "..."}.
async fn validate_config(
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    match serde_json::from_value::<deltaforge_config::PipelineSpec>(body) {
        Ok(spec) => {
            let name = &spec.metadata.name;
            let source_type = match &spec.spec.source {
                deltaforge_config::SourceCfg::Mysql(_) => "mysql",
                deltaforge_config::SourceCfg::Postgres(_) => "postgres",
                #[cfg(feature = "turso")]
                deltaforge_config::SourceCfg::Turso(_) => "turso",
            };
            let sink_count = spec.spec.sinks.len();

            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "valid": true,
                    "pipeline": name,
                    "source_type": source_type,
                    "sink_count": sink_count,
                })),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "valid": false,
                "error": e.to_string(),
            })),
        )
            .into_response(),
    }
}
