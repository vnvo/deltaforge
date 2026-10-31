//! The class of a MySQL connection or statement failure
//! (`docs/design/mysql-forward-proof-retry.md`, section 3.3), decided once,
//! where the error arises, from its type and server code. Everything after
//! that carries the class, never the error's text.
//!
//! Neither client negotiates TLS in this build (`mysql_async` has no TLS
//! feature; the vendored binlog connector has none), so no TLS error can
//! arise: a handshake's transport failure is an I/O error, classified by its
//! kind. Enabling a TLS feature adds an error variant the matches below do
//! not cover, so it cannot compile until TLS errors are classified.
//!
//! The vendored binlog connector keeps no server error code (its server
//! errors are [`BinlogError::ConnectError`] text): those are classified with
//! the predicates the main stream already uses for them, plus the
//! connector's own connect timeout.

use mysql_async::{DriverError, IoError};
use mysql_binlog_connector_rust::binlog_error::BinlogError;

/// What a failure says about trying again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Failure {
    /// A lock wait timeout (1205) or a deadlock (1213), with its code.
    Contention(u16),
    /// The connection could not be made or was lost.
    Transport,
    /// The server refused the credentials.
    Authentication,
    /// The account lacks a privilege.
    Authorization,
    /// The server's answer could not be understood.
    Protocol,
    /// The binlog the request needs is purged.
    Retention,
    /// The table does not exist (1146).
    Absent,
    /// Anything else.
    Other,
}

impl Failure {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Contention(1213) => "deadlock",
            Self::Contention(0) => "capture_slots_busy",
            Self::Contention(_) => "lock_wait_timeout",
            Self::Transport => "transport",
            Self::Authentication => "authentication",
            Self::Authorization => "authorization",
            Self::Protocol => "protocol",
            Self::Retention => "retention",
            Self::Absent => "absent",
            Self::Other => "other",
        }
    }
}

const ER_CON_COUNT_ERROR: u16 = 1040;
const ER_DBACCESS_DENIED_ERROR: u16 = 1044;
const ER_ACCESS_DENIED_ERROR: u16 = 1045;
const ER_SERVER_SHUTDOWN: u16 = 1053;
const ER_TABLEACCESS_DENIED_ERROR: u16 = 1142;
const ER_COLUMNACCESS_DENIED_ERROR: u16 = 1143;
const ER_NO_SUCH_TABLE: u16 = 1146;
const ER_LOCK_WAIT_TIMEOUT: u16 = 1205;
const ER_LOCK_DEADLOCK: u16 = 1213;
const ER_SPECIFIC_ACCESS_DENIED_ERROR: u16 = 1227;
const ER_NOT_SUPPORTED_AUTH_MODE: u16 = 1251;
const ER_MUST_CHANGE_PASSWORD: u16 = 1820;
const ER_MUST_CHANGE_PASSWORD_LOGIN: u16 = 1862;
const ER_CONNECTION_KILLED: u16 = 1927;
const ER_ACCOUNT_HAS_BEEN_LOCKED: u16 = 3118;

/// A server error's class, by its code.
pub(crate) fn of_code(code: u16) -> Failure {
    match code {
        ER_LOCK_WAIT_TIMEOUT | ER_LOCK_DEADLOCK => Failure::Contention(code),
        ER_CON_COUNT_ERROR | ER_SERVER_SHUTDOWN | ER_CONNECTION_KILLED => {
            Failure::Transport
        }
        ER_ACCESS_DENIED_ERROR
        | ER_NOT_SUPPORTED_AUTH_MODE
        | ER_MUST_CHANGE_PASSWORD
        | ER_MUST_CHANGE_PASSWORD_LOGIN
        | ER_ACCOUNT_HAS_BEEN_LOCKED => Failure::Authentication,
        ER_DBACCESS_DENIED_ERROR
        | ER_TABLEACCESS_DENIED_ERROR
        | ER_COLUMNACCESS_DENIED_ERROR
        | ER_SPECIFIC_ACCESS_DENIED_ERROR => Failure::Authorization,
        ER_NO_SUCH_TABLE => Failure::Absent,
        _ => Failure::Other,
    }
}

/// An I/O error's class, by its kind: only the listed kinds are transport.
pub(crate) fn of_io(kind: std::io::ErrorKind) -> Failure {
    use std::io::ErrorKind as K;
    match kind {
        K::ConnectionRefused
        | K::ConnectionReset
        | K::ConnectionAborted
        | K::BrokenPipe
        | K::TimedOut
        | K::UnexpectedEof
        | K::NotConnected
        | K::HostUnreachable
        | K::NetworkUnreachable => Failure::Transport,
        _ => Failure::Other,
    }
}

/// A `mysql_async` error's class.
pub(crate) fn of_mysql(e: &mysql_async::Error) -> Failure {
    match e {
        mysql_async::Error::Io(IoError::Io(io)) => of_io(io.kind()),
        mysql_async::Error::Server(s) => of_code(s.code),
        mysql_async::Error::Driver(d) => of_driver(d),
        mysql_async::Error::Url(_) | mysql_async::Error::Other(_) => {
            Failure::Other
        }
    }
}

fn of_driver(d: &DriverError) -> Failure {
    use DriverError as D;
    match d {
        D::ConnectionClosed => Failure::Transport,
        D::UnknownAuthPlugin { .. }
        | D::MysqlOldPasswordDisabled
        | D::CleartextPluginDisabled
        | D::NoKeyFound
        | D::NoClientSslFlagFromServer => Failure::Authentication,
        D::PacketOutOfOrder
        | D::UnexpectedPacket { .. }
        | D::PacketTooLarge
        | D::BadCompressedPacketHeader
        | D::CantParseServerVersion { .. }
        | D::FromValue { .. }
        | D::FromRow { .. } => Failure::Protocol,
        D::MissingNamedParam { .. }
        | D::MixedParams
        | D::NamedParamsForPositionalQuery
        | D::NestedTransaction
        | D::PoolDisconnected
        | D::ReadOnlyTransNotSupported
        | D::StmtParamsMismatch { .. }
        | D::StmtParamsNumberExceedsLimit { .. }
        | D::NamedPipesDisabled
        | D::LocalInfile(_) => Failure::Other,
    }
}

/// The vendored binlog connector's error's class (module docs).
pub(crate) fn of_binlog(e: &BinlogError) -> Failure {
    use super::mysql_errors::{is_auth_error, is_purged_error};
    match e {
        BinlogError::IoError(io) => of_io(io.kind()),
        BinlogError::ConnectError(m) if is_auth_error(m) => {
            Failure::Authentication
        }
        BinlogError::ConnectError(m) if is_purged_error(m) => {
            Failure::Retention
        }
        BinlogError::ConnectError(m) if is_connect_timeout(m) => {
            Failure::Transport
        }
        BinlogError::ConnectError(_) | BinlogError::ParseUrlError(_) => {
            Failure::Other
        }
        BinlogError::UnsupportedColumnType(_)
        | BinlogError::UnexpectedData(_)
        | BinlogError::FmtError(_)
        | BinlogError::ParseIntError(_)
        | BinlogError::FromUtf8Error(_)
        | BinlogError::ParseJsonError(_)
        | BinlogError::InvalidGtid(_) => Failure::Protocol,
    }
}

/// The connector's TCP connect timeout
/// (`vendor/mysql-binlog-connector-rust/src/network/packet_channel.rs`).
fn is_connect_timeout(msg: &str) -> bool {
    msg.starts_with("Connection timeout after ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn server(code: u16) -> mysql_async::Error {
        mysql_async::Error::Server(mysql_async::ServerError {
            code,
            message: String::new(),
            state: String::new(),
        })
    }

    fn io(kind: std::io::ErrorKind) -> mysql_async::Error {
        mysql_async::Error::Io(IoError::Io(std::io::Error::new(kind, "x")))
    }

    #[test]
    fn server_codes_are_classified_by_code() {
        for (code, want) in [
            (1205, Failure::Contention(1205)),
            (1213, Failure::Contention(1213)),
            (1040, Failure::Transport),
            (1053, Failure::Transport),
            (1927, Failure::Transport),
            (1045, Failure::Authentication),
            (1251, Failure::Authentication),
            (1820, Failure::Authentication),
            (1862, Failure::Authentication),
            (3118, Failure::Authentication),
            (1044, Failure::Authorization),
            (1142, Failure::Authorization),
            (1143, Failure::Authorization),
            (1227, Failure::Authorization),
            (1146, Failure::Absent),
            (1064, Failure::Other),
            (1236, Failure::Other),
        ] {
            assert_eq!(of_mysql(&server(code)), want, "{code}");
        }
    }

    #[test]
    fn only_the_listed_io_kinds_are_transport() {
        use std::io::ErrorKind as K;
        for kind in [
            K::ConnectionRefused,
            K::ConnectionReset,
            K::ConnectionAborted,
            K::BrokenPipe,
            K::TimedOut,
            K::UnexpectedEof,
            K::NotConnected,
            K::HostUnreachable,
            K::NetworkUnreachable,
        ] {
            assert_eq!(of_mysql(&io(kind)), Failure::Transport, "{kind:?}");
            assert_eq!(
                of_binlog(&BinlogError::IoError(std::io::Error::new(
                    kind, "x"
                ))),
                Failure::Transport
            );
        }
        for kind in [K::PermissionDenied, K::InvalidData, K::Other] {
            assert_eq!(of_mysql(&io(kind)), Failure::Other, "{kind:?}");
        }
    }

    #[test]
    fn driver_errors_separate_transport_credentials_and_protocol() {
        use mysql_async::Error::Driver;
        assert_eq!(
            of_mysql(&Driver(DriverError::ConnectionClosed)),
            Failure::Transport
        );
        for d in [
            DriverError::UnknownAuthPlugin { name: "x".into() },
            DriverError::MysqlOldPasswordDisabled,
            DriverError::CleartextPluginDisabled,
            DriverError::NoKeyFound,
            DriverError::NoClientSslFlagFromServer,
        ] {
            assert_eq!(of_mysql(&Driver(d)), Failure::Authentication);
        }
        for d in [
            DriverError::PacketOutOfOrder,
            DriverError::UnexpectedPacket { payload: vec![] },
            DriverError::PacketTooLarge,
            DriverError::BadCompressedPacketHeader,
        ] {
            assert_eq!(of_mysql(&Driver(d)), Failure::Protocol);
        }
        assert_eq!(
            of_mysql(&Driver(DriverError::PoolDisconnected)),
            Failure::Other,
            "the pool shut down: not a transient failure"
        );
    }

    #[test]
    fn binlog_connector_errors_use_the_main_streams_predicates() {
        let c = |m: &str| BinlogError::ConnectError(m.into());
        assert_eq!(
            of_binlog(&c("connect mysql failed: Access denied for user 'u'")),
            Failure::Authentication
        );
        assert_eq!(
            of_binlog(&c(
                "connect mysql failed: Cannot replicate because the source \
                 purged required binary logs"
            )),
            Failure::Retention
        );
        assert_eq!(
            of_binlog(&c(
                "Connection timeout after 60 seconds while connecting to x"
            )),
            Failure::Transport
        );
        assert_eq!(
            of_binlog(&c("connect mysql failed: something else")),
            Failure::Other
        );
        assert_eq!(
            of_binlog(&BinlogError::UnexpectedData("x".into())),
            Failure::Protocol
        );
    }

    /// The connector's timeout text, as the vendored source writes it.
    #[test]
    fn the_connect_timeout_text_is_the_vendored_connectors() {
        let src = include_str!(
            "../../../../vendor/mysql-binlog-connector-rust/src/network/packet_channel.rs"
        );
        assert!(src.contains("\"Connection timeout after {} seconds"));
    }
}
