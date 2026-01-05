//! PostgreSQL source error types.

use deltaforge_core::SourceError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PostgresSourceError {
    #[error("invalid postgres dsn: {0}")]
    InvalidDsn(String),

    #[error("replication connection failed: {0}")]
    ReplicationConnect(String),

    #[error("auth failed: {0}")]
    Auth(String),

    #[error("publication '{publication}' not found")]
    PublicationNotFound { publication: String },

    #[error("replication slot '{slot}' not found")]
    SlotNotFound { slot: String },

    #[error("schema resolution failed for table {schema}.{table}: {details}")]
    Schema {
        schema: String,
        table: String,
        details: String,
    },

    #[error("checkpoint load failed: {0}")]
    Checkpoint(String),

    #[error("LSN parse error: {0}")]
    LsnParse(String),

    #[error("pgoutput protocol error: {0}")]
    Protocol(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("database error: {0}")]
    Database(String),

    #[error("replication error: {0}")]
    Replication(String),
}

impl From<PostgresSourceError> for SourceError {
    fn from(e: PostgresSourceError) -> Self {
        match e {
            PostgresSourceError::InvalidDsn(dsn) => SourceError::Incompatible {
                details: format!("invalid PostgreSQL DSN: {dsn}").into(),
            },
            PostgresSourceError::ReplicationConnect(msg) => SourceError::Connect {
                details: msg.into(),
            },
            PostgresSourceError::Auth(msg) => SourceError::Auth {
                details: msg.into(),
            },
            PostgresSourceError::PublicationNotFound { publication } => {
                SourceError::Incompatible {
                    details: format!("publication '{publication}' not found").into(),
                }
            }
            PostgresSourceError::SlotNotFound { slot } => SourceError::Incompatible {
                details: format!("replication slot '{slot}' not found").into(),
            },
            PostgresSourceError::Schema {
                schema,
                table,
                details,
            } => SourceError::Schema {
                details: format!("{schema}.{table}: {details}").into(),
            },
            PostgresSourceError::Checkpoint(msg) => SourceError::Checkpoint {
                details: msg.into(),
            },
            PostgresSourceError::LsnParse(msg) => SourceError::Incompatible {
                details: format!("LSN parse error: {msg}").into(),
            },
            PostgresSourceError::Protocol(msg) => SourceError::Other(anyhow::anyhow!(msg)),
            PostgresSourceError::Io(e) => SourceError::Io(e),
            PostgresSourceError::Database(msg) => {
                SourceError::Other(anyhow::anyhow!("database error: {msg}"))
            }
            PostgresSourceError::Replication(msg) => {
                SourceError::Other(anyhow::anyhow!("replication error: {msg}"))
            }
        }
    }
}

impl From<tokio_postgres::Error> for PostgresSourceError {
    fn from(e: tokio_postgres::Error) -> Self {
        PostgresSourceError::Database(e.to_string())
    }
}

impl From<pgwire_replication::PgWireError> for PostgresSourceError {
    fn from(e: pgwire_replication::PgWireError) -> Self {
        PostgresSourceError::Replication(e.to_string())
    }
}