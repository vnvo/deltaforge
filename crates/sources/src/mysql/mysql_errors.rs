use deltaforge_core::SourceError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MySqlSourceError {
    #[error("invalid mysql dns: {0}")]
    InvalidDsn(String),

    #[error("binlog connection failed: {0}")]
    BinlogConnect(String),

    #[error("bimlog auth failed: {0}")]
    Auth(String),

    #[error("schema resolution failed for table {table}: {details}")]
    Schema { db: String, table: String, details: String },

    #[error("checkpoint load failed: {0}")]
    Checkpoint(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("driver error: {0}")]
    Driver(#[from] mysql_async::Error),
}

impl From<MySqlSourceError> for SourceError {
    fn from(e: MySqlSourceError) -> Self {
        match e {
            MySqlSourceError::InvalidDsn(dsn) => SourceError::Incompatible {
                details: format!("invalid MySQL DSN: {dsn}").into(),
            },
            MySqlSourceError::BinlogConnect(msg) => SourceError::Connect {
                details: msg.into(),
            },
            MySqlSourceError::Auth(msg) => SourceError::Auth {
                details: msg.into(),
            },
            MySqlSourceError::Schema { db, table, details } => {
                SourceError::Schema {
                    details: format!("{db}.{table}: {details}").into(),
                }
            }
            MySqlSourceError::Checkpoint(msg) => {
                SourceError::Checkpoint { details: msg.into() }
            }
            MySqlSourceError::Io(e) => SourceError::Io(e.into()),
            MySqlSourceError::Driver(e) => SourceError::Other(e.into()),
        }
    }
}
