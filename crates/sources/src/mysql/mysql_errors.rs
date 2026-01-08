use common::RetryOutcome;
use deltaforge_core::SourceError;
use mysql_binlog_connector_rust::binlog_error::BinlogError;
use thiserror::Error;
use tracing::{error, warn};

#[derive(Debug, Error)]
pub enum MySqlSourceError {
    #[error("invalid mysql dsn: {0}")]
    InvalidDsn(String),

    #[error("binlog connection failed: {0}")]
    BinlogConnect(String),

    #[error("binlog auth failed: {0}")]
    Auth(String),

    #[error("schema resolution failed for table {db}.{table}: {details}")]
    Schema {
        db: String,
        table: String,
        details: String,
    },

    #[error("checkpoint load failed: {0}")]
    Checkpoint(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("driver error: {0}")]
    Driver(#[from] mysql_async::Error),
}

pub type MySqlSourceResult<T> = Result<T, MySqlSourceError>;

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
            MySqlSourceError::Checkpoint(msg) => SourceError::Checkpoint {
                details: msg.into(),
            },
            MySqlSourceError::Io(e) => SourceError::Io(e),
            MySqlSourceError::Driver(e) => SourceError::Other(e.into()),
        }
    }
}

/// Control flow decision for the binlog event loop.
///
/// Returned by error classification to tell the main loop what action to take.
#[derive(Debug)]
pub enum LoopControl {
    /// Transient network/IO error - reconnect and resume from checkpoint.
    Reconnect,

    /// Schema-related error - reload schema cache, then reconnect.
    ///
    /// Used when we encounter unexpected column types or data that might
    /// be due to a schema change we haven't picked up yet.
    /// If `table` is Some, only reload that specific table's schema.
    /// If `table` is None, reload all tracked schemas.
    ReloadSchema {
        db: Option<String>,
        table: Option<String>,
    },

    /// Graceful shutdown requested (cancellation token triggered).
    Stop,

    /// Unrecoverable error - stop the source.
    Fail(SourceError),
}

/// Try to extract db.table from a column identifier.
/// Formats: "db.table.column", "table.column", or just "column"
fn parse_column_identifier(col: &str) -> (Option<String>, Option<String>) {
    let parts: Vec<&str> = col.split('.').collect();
    match parts.as_slice() {
        [db, table, _col] => {
            (Some((*db).to_string()), Some((*table).to_string()))
        }
        [table, _col] => (None, Some((*table).to_string())),
        _ => (None, None),
    }
}

impl LoopControl {
    /// Classify a `RetryOutcome<BinlogError>` into a loop control decision.
    ///
    /// Call this after `watchdog()` returns an error to determine next steps.
    pub fn from_binlog_outcome(outcome: RetryOutcome<BinlogError>) -> Self {
        match outcome {
            RetryOutcome::Cancelled => Self::Stop,
            RetryOutcome::Timeout { .. } => Self::Reconnect,
            RetryOutcome::Failed(e)
            | RetryOutcome::Exhausted { last_error: e, .. } => {
                Self::from_binlog_error(e)
            }
        }
    }

    /// Classify a `BinlogError` into a loop control decision.
    pub fn from_binlog_error(err: BinlogError) -> Self {
        use BinlogError::*;

        match err {
            // --- Transient I/O (reconnect) ---
            IoError(ref e)
                if e.kind() == std::io::ErrorKind::PermissionDenied =>
            {
                error!(error = %e, "permission denied on binlog");
                Self::Fail(SourceError::Auth {
                    details: "permission denied on binlog read".into(),
                })
            }
            IoError(e) => {
                warn!(error = %e, "IO error, will reconnect");
                Self::Reconnect
            }

            // --- Connection errors ---
            ConnectError(ref msg) if is_auth_error(msg) => {
                error!(error = %msg, "authentication failed");
                Self::Fail(SourceError::Auth {
                    details: msg.clone().into(),
                })
            }
            ConnectError(msg) => {
                warn!(error = %msg, "connect error, will reconnect");
                Self::Reconnect
            }

            // --- Schema drift (reload + reconnect) ---
            UnsupportedColumnType(col) => {
                warn!(column = %col, "unsupported column type, will reload schema");
                // Column format is often "db.table.column" or just "column"
                let (db, table) = parse_column_identifier(&col);
                Self::ReloadSchema { db, table }
            }
            UnexpectedData(msg) => {
                warn!(details = %msg, "unexpected binlog data, will reload schema");
                // Can't determine which table, reload all
                Self::ReloadSchema {
                    db: None,
                    table: None,
                }
            }
            ParseJsonError(msg) => {
                warn!(details = %msg, "JSON parse error, will reload schema");
                Self::ReloadSchema {
                    db: None,
                    table: None,
                }
            }

            // --- Fatal config/checkpoint errors ---
            InvalidGtid(gtid) => {
                error!(gtid = %gtid, "invalid GTID - clear checkpoint or fix config");
                Self::Fail(SourceError::Checkpoint {
                    details: format!("invalid GTID: {gtid}").into(),
                })
            }
            ParseUrlError(e) => Self::Fail(SourceError::Incompatible {
                details: format!("invalid DSN: {e}").into(),
            }),

            // --- Programming errors (shouldn't happen in normal operation) ---
            FmtError(e) => Self::fail_internal(e),
            ParseIntError(e) => Self::fail_internal(e),
            FromUtf8Error(e) => Self::fail_internal(e),
        }
    }

    fn fail_internal(e: impl Into<anyhow::Error>) -> Self {
        Self::Fail(SourceError::Other(e.into()))
    }

    /// Returns true if this is a retryable condition.
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Reconnect | Self::ReloadSchema { .. })
    }

    /// Returns true if this is a stop/shutdown signal.
    pub fn is_stop(&self) -> bool {
        matches!(self, Self::Stop)
    }
}

/// Check if an error message indicates an authentication/authorization failure.
fn is_auth_error(msg: &str) -> bool {
    let lower = msg.to_lowercase();
    lower.contains("access denied")
        || lower.contains("authentication")
        || lower.contains("unauthorized")
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::RetryOutcome;
    use mysql_binlog_connector_rust::binlog_error::BinlogError;
    use std::io::{Error as IoError, ErrorKind};

    // =========================================================================
    // parse_column_identifier tests
    // =========================================================================

    #[test]
    fn parse_column_identifier_full_path() {
        let (db, table) = parse_column_identifier("mydb.mytable.mycolumn");
        assert_eq!(db, Some("mydb".to_string()));
        assert_eq!(table, Some("mytable".to_string()));
    }

    #[test]
    fn parse_column_identifier_table_and_column() {
        let (db, table) = parse_column_identifier("mytable.mycolumn");
        assert_eq!(db, None);
        assert_eq!(table, Some("mytable".to_string()));
    }

    #[test]
    fn parse_column_identifier_column_only() {
        let (db, table) = parse_column_identifier("mycolumn");
        assert_eq!(db, None);
        assert_eq!(table, None);
    }

    // =========================================================================
    // LoopControl::from_binlog_error tests
    // =========================================================================

    #[test]
    fn io_error_triggers_reconnect() {
        let err = BinlogError::IoError(IoError::new(
            ErrorKind::ConnectionReset,
            "connection reset",
        ));
        let control = LoopControl::from_binlog_error(err);
        assert!(matches!(control, LoopControl::Reconnect));
    }

    #[test]
    fn io_error_permission_denied_triggers_fail() {
        let err = BinlogError::IoError(IoError::new(
            ErrorKind::PermissionDenied,
            "permission denied",
        ));
        let control = LoopControl::from_binlog_error(err);
        assert!(matches!(
            control,
            LoopControl::Fail(SourceError::Auth { .. })
        ));
    }

    #[test]
    fn connect_error_triggers_reconnect() {
        let err = BinlogError::ConnectError("connection refused".to_string());
        let control = LoopControl::from_binlog_error(err);
        assert!(matches!(control, LoopControl::Reconnect));
    }

    #[test]
    fn connect_error_access_denied_triggers_fail() {
        let err =
            BinlogError::ConnectError("Access denied for user".to_string());
        let control = LoopControl::from_binlog_error(err);
        assert!(matches!(
            control,
            LoopControl::Fail(SourceError::Auth { .. })
        ));
    }

    #[test]
    fn connect_error_authentication_triggers_fail() {
        let err =
            BinlogError::ConnectError("authentication failed".to_string());
        let control = LoopControl::from_binlog_error(err);
        assert!(matches!(
            control,
            LoopControl::Fail(SourceError::Auth { .. })
        ));
    }

    #[test]
    fn unsupported_column_type_triggers_reload_schema() {
        let err = BinlogError::UnsupportedColumnType(
            "shop.orders.new_col".to_string(),
        );
        let control = LoopControl::from_binlog_error(err);

        match control {
            LoopControl::ReloadSchema { db, table } => {
                assert_eq!(db, Some("shop".to_string()));
                assert_eq!(table, Some("orders".to_string()));
            }
            other => panic!("expected ReloadSchema, got {:?}", other),
        }
    }

    #[test]
    fn unsupported_column_type_unknown_format_reloads_all() {
        let err =
            BinlogError::UnsupportedColumnType("weird_column".to_string());
        let control = LoopControl::from_binlog_error(err);

        match control {
            LoopControl::ReloadSchema { db, table } => {
                assert_eq!(db, None);
                assert_eq!(table, None);
            }
            other => panic!("expected ReloadSchema, got {:?}", other),
        }
    }

    #[test]
    fn unexpected_data_triggers_reload_schema() {
        let err =
            BinlogError::UnexpectedData("unexpected row format".to_string());
        let control = LoopControl::from_binlog_error(err);

        match control {
            LoopControl::ReloadSchema { db, table } => {
                assert_eq!(db, None);
                assert_eq!(table, None);
            }
            other => panic!("expected ReloadSchema, got {:?}", other),
        }
    }

    #[test]
    fn parse_json_error_triggers_reload_schema() {
        let err = BinlogError::ParseJsonError("invalid JSON".to_string());
        let control = LoopControl::from_binlog_error(err);
        assert!(matches!(control, LoopControl::ReloadSchema { .. }));
    }

    #[test]
    fn invalid_gtid_triggers_checkpoint_fail() {
        let err = BinlogError::InvalidGtid("bad-gtid-format".to_string());
        let control = LoopControl::from_binlog_error(err);

        match control {
            LoopControl::Fail(SourceError::Checkpoint { details }) => {
                assert!(details.contains("bad-gtid-format"));
            }
            other => panic!("expected Fail(Checkpoint), got {:?}", other),
        }
    }

    #[test]
    fn parse_url_error_triggers_incompatible_fail() {
        let err = BinlogError::ParseUrlError(url::ParseError::EmptyHost);
        let control = LoopControl::from_binlog_error(err);
        assert!(matches!(
            control,
            LoopControl::Fail(SourceError::Incompatible { .. })
        ));
    }

    #[test]
    fn fmt_error_triggers_internal_fail() {
        let err = BinlogError::FmtError(std::fmt::Error);
        let control = LoopControl::from_binlog_error(err);
        assert!(matches!(control, LoopControl::Fail(SourceError::Other(_))));
    }

    // =========================================================================
    // LoopControl::from_binlog_outcome tests
    // =========================================================================

    #[test]
    fn outcome_cancelled_triggers_stop() {
        let outcome: RetryOutcome<BinlogError> = RetryOutcome::Cancelled;
        let control = LoopControl::from_binlog_outcome(outcome);
        assert!(matches!(control, LoopControl::Stop));
    }

    #[test]
    fn outcome_timeout_triggers_reconnect() {
        let outcome: RetryOutcome<BinlogError> = RetryOutcome::Timeout {
            action: "binlog_read".into(),
        };
        let control = LoopControl::from_binlog_outcome(outcome);
        assert!(matches!(control, LoopControl::Reconnect));
    }

    #[test]
    fn outcome_failed_delegates_to_from_binlog_error() {
        let outcome: RetryOutcome<BinlogError> = RetryOutcome::Failed(
            BinlogError::ConnectError("connection reset".to_string()),
        );
        let control = LoopControl::from_binlog_outcome(outcome);
        assert!(matches!(control, LoopControl::Reconnect));
    }

    #[test]
    fn outcome_exhausted_delegates_to_from_binlog_error() {
        let outcome: RetryOutcome<BinlogError> = RetryOutcome::Exhausted {
            attempts: 5,
            last_error: BinlogError::IoError(IoError::new(
                ErrorKind::TimedOut,
                "timed out",
            )),
        };
        let control = LoopControl::from_binlog_outcome(outcome);
        assert!(matches!(control, LoopControl::Reconnect));
    }

    // =========================================================================
    // LoopControl helper method tests
    // =========================================================================

    #[test]
    fn is_retryable_returns_true_for_reconnect() {
        assert!(LoopControl::Reconnect.is_retryable());
    }

    #[test]
    fn is_retryable_returns_true_for_reload_schema() {
        let control = LoopControl::ReloadSchema {
            db: None,
            table: None,
        };
        assert!(control.is_retryable());
    }

    #[test]
    fn is_retryable_returns_false_for_stop() {
        assert!(!LoopControl::Stop.is_retryable());
    }

    #[test]
    fn is_retryable_returns_false_for_fail() {
        let control = LoopControl::Fail(SourceError::Cancelled);
        assert!(!control.is_retryable());
    }

    #[test]
    fn is_stop_returns_true_only_for_stop() {
        assert!(LoopControl::Stop.is_stop());
        assert!(!LoopControl::Reconnect.is_stop());
        assert!(
            !LoopControl::ReloadSchema {
                db: None,
                table: None
            }
            .is_stop()
        );
    }
}
