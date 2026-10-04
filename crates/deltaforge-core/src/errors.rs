use crate::incident::{CauseCode, IncidentDraft};
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

    /// The connected server is not the verified source lineage (or its
    /// identity could not be established). Never retried.
    #[error("source lineage error: {details}")]
    Lineage { details: Cow<'static, str> },

    #[error("backpressure")]
    Backpressure,

    #[error(transparent)]
    Other(#[from] anyhow::Error),

    /// `cause`, raised as an operator incident. Displays only the draft's
    /// sanitized explanation; classification follows `cause`.
    #[error("{draft}")]
    Incident {
        draft: Box<IncidentDraft>,
        #[source]
        cause: Box<SourceError>,
    },
}

impl SourceError {
    /// Raise `cause` as the incident `draft`.
    pub fn incident(draft: IncidentDraft, cause: SourceError) -> Self {
        Self::Incident {
            draft: Box::new(draft),
            cause: Box::new(cause),
        }
    }

    /// The typed error under any incident wrapping.
    pub fn root(&self) -> &SourceError {
        match self {
            Self::Incident { cause, .. } => cause.root(),
            other => other,
        }
    }

    /// The outermost incident draft this error carries, if any.
    pub fn draft(&self) -> Option<&IncidentDraft> {
        match self {
            Self::Incident { draft, .. } => Some(draft),
            _ => None,
        }
    }

    /// The cause class of the typed error (never its text).
    pub fn cause_code(&self) -> CauseCode {
        match self.root() {
            Self::Cancelled | Self::Other(_) => CauseCode::SourceOther,
            Self::Timeout { .. } => CauseCode::SourceTimeout,
            Self::Connect { .. } => CauseCode::SourceConnect,
            Self::Auth { .. } => CauseCode::SourceAuth,
            Self::Permission { .. } => CauseCode::SourcePermission,
            Self::NotFound { .. } => CauseCode::SourceNotFound,
            Self::Io(_) => CauseCode::SourceIo,
            Self::Checkpoint { .. } => CauseCode::SourceCheckpoint,
            Self::Incompatible { .. } => CauseCode::SourceIncompatible,
            Self::Schema { .. } => CauseCode::SourceSchema,
            Self::Lineage { .. } => CauseCode::SourceLineage,
            Self::Backpressure => CauseCode::SourceBackpressure,
            Self::Incident { .. } => unreachable!("root unwraps incidents"),
        }
    }
}

#[derive(Debug, Error)]
pub enum SinkError {
    #[error("connection error: {details}")]
    Connect { details: Cow<'static, str> },

    #[error("auth error: {details}")]
    Auth { details: Cow<'static, str> },

    #[error("i/o error: {0}")]
    Io(#[from] io::Error),

    #[error("serialization error: {details}")]
    Serialization { details: Cow<'static, str> },

    #[error("backpressure: {details}")]
    Backpressure { details: Cow<'static, str> },

    #[error("routing error: {details}")]
    Routing { details: Cow<'static, str> },

    /// Unrecoverable error — the sink cannot recover and the pipeline must stop.
    /// Examples: Kafka ProducerFenced (another producer with the same
    /// transactional.id started), permanent auth revocation.
    #[error("fatal: {details}")]
    Fatal { details: Cow<'static, str> },

    #[error(transparent)]
    Other(#[from] anyhow::Error),

    /// `cause`, raised as an operator incident. Displays only the draft's
    /// sanitized explanation; classification follows `cause`.
    #[error("{draft}")]
    Incident {
        draft: Box<IncidentDraft>,
        #[source]
        cause: Box<SinkError>,
    },
}

impl SinkError {
    /// Raise `cause` as the incident `draft`.
    pub fn incident(draft: IncidentDraft, cause: SinkError) -> Self {
        Self::Incident {
            draft: Box::new(draft),
            cause: Box::new(cause),
        }
    }

    /// The typed error under any incident wrapping.
    pub fn root(&self) -> &SinkError {
        match self {
            Self::Incident { cause, .. } => cause.root(),
            other => other,
        }
    }

    /// The outermost incident draft this error carries, if any.
    pub fn draft(&self) -> Option<&IncidentDraft> {
        match self {
            Self::Incident { draft, .. } => Some(draft),
            _ => None,
        }
    }

    /// The cause class of the typed error (never its text).
    pub fn cause_code(&self) -> CauseCode {
        match self.root() {
            Self::Connect { .. } => CauseCode::SinkConnect,
            Self::Auth { .. } => CauseCode::SinkAuth,
            Self::Io(_) => CauseCode::SinkIo,
            Self::Serialization { .. } => CauseCode::SinkSerialization,
            Self::Backpressure { .. } => CauseCode::SinkBackpressure,
            Self::Routing { .. } => CauseCode::SinkRouting,
            Self::Fatal { .. } => CauseCode::SinkFatal,
            Self::Other(_) => CauseCode::SinkOther,
            Self::Incident { .. } => unreachable!("root unwraps incidents"),
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            SinkError::Incident { cause, .. } => cause.kind(),
            SinkError::Connect { .. } => "connect error",
            SinkError::Auth { .. } => "auth error",
            SinkError::Io(_) => "io error",
            SinkError::Serialization { .. } => "serialization error",
            SinkError::Backpressure { .. } => "backpressure",
            SinkError::Routing { .. } => "routing error",
            SinkError::Fatal { .. } => "fatal error",
            SinkError::Other(_) => "other error",
        }
    }

    pub fn details(&self) -> String {
        match self {
            SinkError::Incident { draft, .. } => draft.explanation(),
            SinkError::Connect { details } => details.to_string(),
            SinkError::Auth { details } => details.to_string(),
            SinkError::Backpressure { details } => details.to_string(),
            SinkError::Routing { details } => details.to_string(),
            SinkError::Serialization { details } => details.to_string(),
            SinkError::Fatal { details } => details.to_string(),
            SinkError::Io(e) => e.to_string(),
            SinkError::Other(e) => e.to_string(),
        }
    }

    /// Whether this error is attributable to a single event and should be
    /// routed to the DLQ instead of failing the entire batch.
    /// Only serialization and routing errors are per-event attributable.
    pub fn is_dlq_eligible(&self) -> bool {
        matches!(
            self.root(),
            SinkError::Serialization { .. } | SinkError::Routing { .. }
        )
    }
}

// Convenience conversion from serde_json::Error
impl From<serde_json::Error> for SinkError {
    fn from(e: serde_json::Error) -> Self {
        SinkError::Serialization {
            details: e.to_string().into(),
        }
    }
}
