use axum::Router;
mod errors;
mod health;
mod pipelines;
pub use errors::{PipelineAPIError, pipeline_error};
pub use pipelines::{AppState, PipeInfo, PipelineController};

pub fn router(state: AppState) -> Router {
    let health_state = state.clone();
    let health = health::router(health_state);
    let pipeline_mgmt = pipelines::router(state);

    health.merge(pipeline_mgmt)
}
