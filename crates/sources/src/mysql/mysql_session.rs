//! Connections bound to a verified server identity.
//!
//! A proxy, failover endpoint, DNS rotation or load balancer can route two
//! connections to one DSN to different servers, so an identity checked on one
//! connection proves nothing about another. Every replication session and
//! every control connection whose answers are trusted therefore verifies
//! `@@GLOBAL.server_uuid` on ITSELF before it is used: a replication session
//! before its dump command, a control connection before its first query.
//!
//! [`open_replication_session`] is the only copy of the connector handshake
//! (the scanner, the initial CDC stream, reconnects and credential rotation
//! all use it). It mirrors `BinlogClient::connect` with the identity query
//! added: same timeout, start-position defaults, checksum, heartbeat.

use std::collections::HashMap;
use std::time::Duration;

use deltaforge_core::SourceError;
use mysql_async::prelude::Queryable;
use mysql_binlog_connector_rust::binlog_client::BinlogClient;
use mysql_binlog_connector_rust::binlog_parser::BinlogParser;
use mysql_binlog_connector_rust::binlog_stream::BinlogStream;
use mysql_binlog_connector_rust::command::authenticator::Authenticator;
use mysql_binlog_connector_rust::command::command_util::CommandUtil;

/// Why a connection could not be used as the expected server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SessionError {
    /// The connection reached another server (its `server_uuid`).
    OtherServer { found: String },
    /// The connection's identity could not be established.
    NoIdentity(String),
    /// Connecting or setting up the session failed (network, auth, server).
    Connect(String),
}

impl SessionError {
    /// The source error for a session that must not be used. Identity
    /// failures are typed lineage errors (never retried); connect failures
    /// stay retryable.
    pub(crate) fn into_source_error(self, expected: &str) -> SourceError {
        match self {
            Self::OtherServer { found } => SourceError::Lineage {
                details: format!(
                    "connection reached server {found}, expected {expected}"
                )
                .into(),
            },
            Self::NoIdentity(why) => SourceError::Lineage {
                details: format!(
                    "could not verify the connection is server {expected}: {why}"
                )
                .into(),
            },
            Self::Connect(why) => SourceError::Connect {
                details: why.into(),
            },
        }
    }
}

const NO_UUID: &str = "00000000-0000-0000-0000-000000000000";

/// Compare a reported `server_uuid` with the expected one.
fn check(found: Option<String>, expected: &str) -> Result<(), SessionError> {
    let found = found
        .map(|u| u.trim().to_ascii_lowercase())
        .filter(|u| !u.is_empty() && u != NO_UUID)
        .ok_or_else(|| {
            SessionError::NoIdentity("server reports no server_uuid".into())
        })?;
    if found == expected.trim().to_ascii_lowercase() {
        Ok(())
    } else {
        Err(SessionError::OtherServer { found })
    }
}

/// Open a binlog replication session for `client` and verify, on that exact
/// authenticated session and before the dump command, that the server is
/// `expected_uuid`. No event can be read from a session that failed this.
pub(crate) async fn open_replication_session(
    client: &BinlogClient,
    expected_uuid: &str,
) -> Result<BinlogStream, SessionError> {
    open_session(client, expected_uuid, false).await
}

/// [`open_replication_session`] for a short-lived reader (the interval
/// scanner): an abortive close (`SO_LINGER` 0) is armed right after the TCP
/// connection is established, before any other awaited step, so however the
/// reader ends - completion, error, timeout, or its future being cancelled,
/// even mid-open - dropping the connection resets it and the server's next
/// write ends its dump thread. Not armable: the open fails.
pub(crate) async fn open_replication_session_abortive(
    client: &BinlogClient,
    expected_uuid: &str,
) -> Result<BinlogStream, SessionError> {
    open_session(client, expected_uuid, true).await
}

async fn open_session(
    client: &BinlogClient,
    expected_uuid: &str,
    abortive: bool,
) -> Result<BinlogStream, SessionError> {
    // The connector's keepalive configuration type is not public; no
    // production client sets it, and silently dropping it is not an option.
    if client.keepalive_idle_secs != 0 || client.keepalive_interval_secs != 0 {
        return Err(SessionError::Connect(
            "TCP keepalive is not supported on verified replication sessions"
                .into(),
        ));
    }
    let connect =
        |e: mysql_binlog_connector_rust::binlog_error::BinlogError| {
            SessionError::Connect(e.to_string())
        };
    let timeout_secs = if client.timeout_secs > 0 {
        client.timeout_secs
    } else {
        60
    };
    let mut channel = Authenticator::new(&client.url, timeout_secs, None)
        .map_err(connect)?
        .connect()
        .await
        .map_err(connect)?;
    if abortive {
        channel.abort_on_drop().map_err(|e| {
            SessionError::Connect(format!(
                "cannot arm the session's abortive close: {e}"
            ))
        })?;
    }

    let identity =
        CommandUtil::execute_query(&mut channel, "SELECT @@GLOBAL.server_uuid")
            .await
            .map_err(|e| SessionError::NoIdentity(e.to_string()))
            .and_then(|rows| {
                check(
                    rows.first().and_then(|r| r.values.first()).cloned(),
                    expected_uuid,
                )
            });
    if let Err(e) = identity {
        let _ = channel.close().await;
        return Err(e);
    }

    // Verified: the rest is `BinlogClient::connect` on this same session.
    let mut start = BinlogClient {
        url: client.url.clone(),
        binlog_filename: client.binlog_filename.clone(),
        binlog_position: client.binlog_position,
        server_id: client.server_id,
        gtid_enabled: client.gtid_enabled,
        gtid_set: client.gtid_set.clone(),
        heartbeat_interval_secs: client.heartbeat_interval_secs,
        timeout_secs,
        keepalive_idle_secs: 0,
        keepalive_interval_secs: 0,
    };
    if start.gtid_enabled {
        if start.gtid_set.is_empty() {
            let (_, _, gtid_set) = CommandUtil::fetch_binlog_info(&mut channel)
                .await
                .map_err(connect)?;
            start.gtid_set = gtid_set;
        }
    } else {
        if start.binlog_filename.is_empty() {
            let (file, pos, _) = CommandUtil::fetch_binlog_info(&mut channel)
                .await
                .map_err(connect)?;
            start.binlog_filename = file;
            start.binlog_position = pos;
        }
        start.binlog_position = start.binlog_position.max(4);
    }
    let checksum = CommandUtil::fetch_binlog_checksum(&mut channel)
        .await
        .map_err(connect)?;
    CommandUtil::setup_binlog_connection(&mut channel)
        .await
        .map_err(connect)?;
    if start.heartbeat_interval_secs > 0 {
        CommandUtil::enable_heartbeat(
            &mut channel,
            start.heartbeat_interval_secs,
        )
        .await
        .map_err(connect)?;
    }
    CommandUtil::dump_binlog(&mut channel, &start)
        .await
        .map_err(connect)?;
    Ok(BinlogStream {
        channel,
        parser: BinlogParser {
            checksum_length: checksum.get_length(),
            table_map_event_by_table_id: HashMap::new(),
        },
    })
}

/// Verify that an open SQL connection is `expected_uuid`.
pub(crate) async fn verify_connection(
    conn: &mut mysql_async::Conn,
    expected_uuid: &str,
) -> Result<(), SessionError> {
    let found: Option<Option<String>> = conn
        .query_first("SELECT @@GLOBAL.server_uuid")
        .await
        .map_err(|e| SessionError::NoIdentity(e.to_string()))?;
    check(found.flatten(), expected_uuid)
}

/// Open one SQL control connection and verify it is `expected_uuid` before
/// returning it. Everything read on it is a fact about that server.
pub(crate) async fn open_control_connection(
    dsn: &str,
    expected_uuid: &str,
    connect_timeout: Duration,
) -> Result<mysql_async::Conn, SessionError> {
    let opts = mysql_async::Opts::from_url(dsn)
        .map_err(|e| SessionError::Connect(e.to_string()))?;
    let mut conn =
        tokio::time::timeout(connect_timeout, mysql_async::Conn::new(opts))
            .await
            .map_err(|_| SessionError::Connect("connect timed out".into()))?
            .map_err(|e| SessionError::Connect(e.to_string()))?;
    if let Err(e) = verify_connection(&mut conn, expected_uuid).await {
        let _ = conn.disconnect().await;
        return Err(e);
    }
    Ok(conn)
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562";

    #[test]
    fn identity_must_be_present_and_equal() {
        assert_eq!(check(Some(A.into()), A), Ok(()));
        assert_eq!(check(Some(A.to_uppercase()), A), Ok(()));
        assert_eq!(check(Some(format!(" {A}\n")), A), Ok(()));
        assert!(matches!(
            check(Some("3e11fa47-71ca-11e1-9e33-c80aa9429563".into()), A),
            Err(SessionError::OtherServer { .. })
        ));
        for absent in [None, Some(String::new()), Some(NO_UUID.into())] {
            assert!(matches!(
                check(absent, A),
                Err(SessionError::NoIdentity(_))
            ));
        }
    }

    #[test]
    fn identity_failures_are_lineage_errors_connect_failures_are_not() {
        let other = SessionError::OtherServer { found: "b".into() };
        assert!(matches!(
            other.into_source_error(A),
            SourceError::Lineage { .. }
        ));
        assert!(matches!(
            SessionError::NoIdentity("x".into()).into_source_error(A),
            SourceError::Lineage { .. }
        ));
        assert!(matches!(
            SessionError::Connect("x".into()).into_source_error(A),
            SourceError::Connect { .. }
        ));
    }
}
