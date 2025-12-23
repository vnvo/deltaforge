//! Error types for schema sensing.

use thiserror::Error;

/// Errors that can occur during schema sensing.
#[derive(Debug, Error)]
pub enum SensorError {
    /// Failed to parse JSON payload
    #[error("failed to parse JSON: {0}")]
    JsonParse(#[from] serde_json::Error),

    /// Schema inference failed
    #[error("schema inference failed: {0}")]
    Inference(String),

    /// Serialization error
    #[error("serialization error: {0}")]
    Serialization(String),
}

/// Result type for sensor operations.
pub type SensorResult<T> = Result<T, SensorError>;