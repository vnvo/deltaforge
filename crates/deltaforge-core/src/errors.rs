use std::borrow::Cow;
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
    Io(#[from] std::io::Error),

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
