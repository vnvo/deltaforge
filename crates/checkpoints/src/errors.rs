use std::io;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CheckpointError {
    #[error("i/o error while accessing checkpoint: {0}")]
    Io(#[from] io::Error),

    #[error("checkpoint serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("checkpoint data format error: {0}")]
    Data(String),

    #[error("database error: {0}")]
    Database(String),

    #[error("operation not supported: {0}")]
    NotSupported(String),
        
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type CheckpointResult<T> = std::result::Result<T, CheckpointError>;
