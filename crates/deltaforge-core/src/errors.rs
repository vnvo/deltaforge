use std::borrow::Cow;
use std::io;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SourceError {
    #[error("operation cancelled")]
    Cancelled,

    #[error("timeout during {action}")]
    Timeout { action: Cow<'static, str> },

    #[error("connection error: {details}")]
    Connect { details: Cow<'static, str> },

    #[error("authentication error: {details}")]
    Auth { details: Cow<'static, str> },

    #[error("permission error: {details}")]
    Permission { details: Cow<'static, str> },

    #[error("resource not found: {details}")]
    NotFound { details: Cow<'static, str> },

    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("checkpoint error: {details}")]
    Checkpoint { details: Cow<'static, str> },

    #[error("incompatible configuration: {details}")]
    Incompatible { details: Cow<'static, str> },

    #[error("schema issues: {details}")]
    Schema { details: Cow<'static, str> },

    #[error("backpressure")]
    Backpressure,

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[derive(Debug, Error)]
pub enum SinkError {
    #[error("connection error: {details}")]
    Connect { details: Cow<'static, str> },

    #[error("auth error: {details}")]
    Auth { details: Cow<'static, str> },

    #[error("i/o error: {0}")]
    Io(#[from] io::Error),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("backpressure: {details}")]
    Backpressure { details: Cow<'static, str> },

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl SinkError {
    pub fn kind(&self) -> &'static str {
        match self {
            SinkError::Connect { .. } => "connect error",
            SinkError::Auth { .. } => "auth error",
            SinkError::Io(_) => "io error",
            SinkError::Serialization(_) => "serialization error",
            SinkError::Backpressure { .. } => "backpressure",
            SinkError::Other(_) => "other error",
        }
    }

    pub fn details(&self) -> String {
        match self {
            SinkError::Connect { details } => details.to_string(),
            SinkError::Auth { details } => details.to_string(),
            SinkError::Backpressure { details } => details.to_string(),
            SinkError::Io(e) => e.to_string(),
            SinkError::Serialization(e) => e.to_string(),
            SinkError::Other(e) => e.to_string(),
        }
    }
}