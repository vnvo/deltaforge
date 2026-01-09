//! PostgreSQL source error types and loop control.

use common::RetryOutcome;
use deltaforge_core::SourceError;
use thiserror::Error;
use tracing::{error, warn};

// =============================================================================
// PostgresSourceError - for helper functions (NOT the event loop)
// =============================================================================

#[derive(Debug, Error)]
pub enum PostgresSourceError {
    #[error("invalid postgres dsn: {0}")]
    InvalidDsn(String),

    #[error("replication connection failed: {0}")]
    ReplicationConnect(String),

    #[error("auth failed: {0}")]
    Auth(String),

    #[error("checkpoint error: {0}")]
    Checkpoint(String),

    #[error("LSN parse error: {0}")]
    LsnParse(String),

    #[error("schema resolution failed for {schema}.{table}: {details}")]
    Schema {
        schema: String,
        table: String,
        details: String,
    },

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("database error: {0}")]
    Database(String),
}

pub type PostgresSourceResult<T> = Result<T, PostgresSourceError>;

impl From<PostgresSourceError> for SourceError {
    fn from(e: PostgresSourceError) -> Self {
        match e {
            PostgresSourceError::InvalidDsn(msg) => SourceError::Incompatible {
                details: msg.into(),
            },
            PostgresSourceError::ReplicationConnect(msg) => {
                SourceError::Connect {
                    details: msg.into(),
                }
            }
            PostgresSourceError::Auth(msg) => SourceError::Auth {
                details: msg.into(),
            },
            PostgresSourceError::Checkpoint(msg) => SourceError::Checkpoint {
                details: msg.into(),
            },
            PostgresSourceError::LsnParse(msg) => SourceError::Incompatible {
                details: format!("LSN: {msg}").into(),
            },
            PostgresSourceError::Schema {
                schema,
                table,
                details,
            } => SourceError::Schema {
                details: format!("{schema}.{table}: {details}").into(),
            },
            PostgresSourceError::Io(e) => SourceError::Io(e),
            PostgresSourceError::Database(msg) => {
                SourceError::Other(anyhow::anyhow!("{msg}"))
            }
        }
    }
}

impl From<tokio_postgres::Error> for PostgresSourceError {
    fn from(e: tokio_postgres::Error) -> Self {
        PostgresSourceError::Database(e.to_string())
    }
}

// =============================================================================
// LoopControl - for event loop control flow
// =============================================================================

#[derive(Debug)]
pub enum LoopControl {
    Reconnect,
    ReloadSchema {
        schema: Option<String>,
        table: Option<String>,
    },
    Stop,
    Fail(SourceError),
}

impl LoopControl {
    /// Classify RetryOutcome from watchdog/retry_async.
    pub fn from_replication_outcome<E: std::fmt::Display>(
        outcome: RetryOutcome<E>,
    ) -> Self {
        match outcome {
            RetryOutcome::Cancelled => Self::Stop,
            RetryOutcome::Timeout { .. } => Self::Reconnect,
            RetryOutcome::Failed(e)
            | RetryOutcome::Exhausted { last_error: e, .. } => {
                Self::from_error_message(&e.to_string())
            }
        }
    }

    /// Classify PgWireError (main event loop errors).
    #[allow(dead_code)]
    pub fn from_pgwire_error(err: pgwire_replication::PgWireError) -> Self {
        use pgwire_replication::PgWireError::*;

        match err {
            Io(ref msg) if msg.contains("permission denied") => {
                error!(error = %msg, "permission denied");
                Self::Fail(SourceError::Auth {
                    details: msg.clone().into(),
                })
            }
            Io(msg) => {
                warn!(error = %msg, "IO error, will reconnect");
                Self::Reconnect
            }

            Auth(msg) => {
                error!(error = %msg, "authentication failed");
                Self::Fail(SourceError::Auth {
                    details: msg.into(),
                })
            }

            Tls(msg) => {
                error!(error = %msg, "TLS error");
                Self::Fail(SourceError::Incompatible {
                    details: format!("TLS: {msg}").into(),
                })
            }

            Server(ref msg) => Self::classify_server_error(msg),

            Protocol(ref msg) if is_schema_error(msg) => {
                warn!(error = %msg, "schema error, will reload");
                Self::ReloadSchema {
                    schema: None,
                    table: None,
                }
            }
            Protocol(msg) => {
                warn!(error = %msg, "protocol error, will reconnect");
                Self::Reconnect
            }

            Task(msg) => {
                warn!(error = %msg, "task error, will reconnect");
                Self::Reconnect
            }

            Internal(msg) => {
                error!(error = %msg, "internal error");
                Self::Fail(SourceError::Other(anyhow::anyhow!(
                    "pgwire: {}",
                    msg
                )))
            }
        }
    }

    /// Classify tokio_postgres errors (control plane queries).
    pub fn from_tokio_postgres_error(err: &tokio_postgres::Error) -> Self {
        if let Some(db_err) = err.as_db_error() {
            let code = db_err.code().code();
            if code.starts_with("28") {
                return Self::Fail(SourceError::Auth {
                    details: err.to_string().into(),
                });
            }
            if code.starts_with("42") {
                return Self::ReloadSchema {
                    schema: None,
                    table: None,
                };
            }
            if code.starts_with("08")
                || code.starts_with("53")
                || code.starts_with("57")
            {
                return Self::Reconnect;
            }
        }
        Self::from_error_message(&err.to_string())
    }

    fn classify_server_error(msg: &str) -> Self {
        let lower = msg.to_lowercase();

        // Auth errors (28xxx)
        if lower.contains("28p01")
            || lower.contains("28000")
            || lower.contains("password authentication failed")
            || lower.contains("no pg_hba.conf entry")
        {
            error!(error = %msg, "auth failed");
            return Self::Fail(SourceError::Auth {
                details: msg.to_string().into(),
            });
        }

        // Schema errors (42xxx)
        if lower.contains("42p01")
            || lower.contains("42703")
            || lower.contains("42704")
            || is_schema_error(&lower)
        {
            warn!(error = %msg, "schema error, will reload");
            return Self::ReloadSchema {
                schema: None,
                table: None,
            };
        }

        // Slot errors
        if lower.contains("replication slot") {
            if lower.contains("does not exist") {
                error!(error = %msg, "slot not found");
                return Self::Fail(SourceError::Checkpoint {
                    details: msg.to_string().into(),
                });
            }
            if lower.contains("already active") {
                warn!(error = %msg, "slot in use, will retry");
                return Self::Reconnect;
            }
        }

        // WAL removed
        if lower.contains("requested wal segment") && lower.contains("removed")
        {
            error!(error = %msg, "WAL removed");
            return Self::Fail(SourceError::Checkpoint {
                details: msg.to_string().into(),
            });
        }

        // Admin shutdown (57P01) - this is retryable
        if lower.contains("57p01")
            || lower.contains("terminating connection")
            || lower.contains("administrator command")
        {
            warn!(error = %msg, "connection terminated by admin, will reconnect");
            return Self::Reconnect;
        }

        warn!(error = %msg, "server error, will reconnect");
        Self::Reconnect
    }

    fn from_error_message(msg: &str) -> Self {
        let lower = msg.to_lowercase();

        // Invalid DSN/connection string is a config error, not retryable
        if lower.contains("invalid connection string")
            || lower.contains("invalid dsn")
        {
            error!(error = %msg, "invalid connection string - check DSN format");
            return Self::Fail(SourceError::Incompatible {
                details: msg.to_string().into(),
            });
        }

        if is_auth_error(&lower) {
            return Self::Fail(SourceError::Auth {
                details: msg.to_string().into(),
            });
        }
        if is_connection_error(&lower) {
            return Self::Reconnect;
        }
        if is_schema_error(&lower) {
            return Self::ReloadSchema {
                schema: None,
                table: None,
            };
        }
        Self::Fail(SourceError::Other(anyhow::anyhow!("{}", msg)))
    }

    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Reconnect | Self::ReloadSchema { .. })
    }
}

fn is_auth_error(s: &str) -> bool {
    s.contains("password authentication failed")
        || s.contains("permission denied")
        || s.contains("no pg_hba.conf entry")
        || s.contains("insufficient privilege")
}

fn is_connection_error(s: &str) -> bool {
    s.contains("connection reset")
        || s.contains("connection refused")
        || s.contains("broken pipe")
        || s.contains("connection closed")
        || s.contains("timed out")
        || s.contains("unexpected eof")
        || s.contains("terminating connection")
        || s.contains("administrator command")
        || s.contains("57p01")
}

fn is_schema_error(s: &str) -> bool {
    (s.contains("relation") || s.contains("column") || s.contains("type"))
        && s.contains("does not exist")
}
