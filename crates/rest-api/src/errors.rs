use axum::Json;
use axum::http::StatusCode;
use serde::Serialize;
use tracing::error;

#[derive(Debug)]
pub enum PipelineAPIError {
    NotFound(String),
    AlreadyExists(String),
    NameMismatch { expected: String, found: String },
    Failed(anyhow::Error),
}

/// Structured error response — parseable by automation and CLIs.
#[derive(Serialize)]
pub struct ApiError {
    pub code: &'static str,
    pub message: String,
}

/// Standard API result type used across all endpoint modules.
pub type ApiResult<T> = Result<Json<T>, (StatusCode, Json<ApiError>)>;

impl std::fmt::Display for PipelineAPIError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PipelineAPIError::NotFound(name) => {
                write!(f, "pipeline {name} not found")
            }
            PipelineAPIError::AlreadyExists(name) => {
                write!(f, "pipeline {name} already exists")
            }
            PipelineAPIError::NameMismatch { expected, found } => {
                write!(
                    f,
                    "pipeline name mismatch: expected {expected}, got {found}"
                )
            }
            PipelineAPIError::Failed(e) => std::fmt::Display::fmt(e, f),
        }
    }
}

impl std::error::Error for PipelineAPIError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PipelineAPIError::Failed(err) => Some(err.root_cause()),
            _ => None,
        }
    }
}

impl From<anyhow::Error> for PipelineAPIError {
    fn from(value: anyhow::Error) -> Self {
        PipelineAPIError::Failed(value)
    }
}

pub fn pipeline_error(err: PipelineAPIError) -> (StatusCode, Json<ApiError>) {
    error!(error=?err, "pipeline lifecycle operation failed");
    let (status, code) = match &err {
        PipelineAPIError::NotFound(_) => {
            (StatusCode::NOT_FOUND, "PIPELINE_NOT_FOUND")
        }
        PipelineAPIError::AlreadyExists(_) => {
            (StatusCode::CONFLICT, "PIPELINE_ALREADY_EXISTS")
        }
        PipelineAPIError::NameMismatch { .. } => {
            (StatusCode::BAD_REQUEST, "PIPELINE_NAME_MISMATCH")
        }
        PipelineAPIError::Failed(_) => {
            (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR")
        }
    };

    (
        status,
        Json(ApiError {
            code,
            message: err.to_string(),
        }),
    )
}
