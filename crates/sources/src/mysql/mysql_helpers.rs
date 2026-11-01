use crc32fast::Hasher;
use deltaforge_core::{CheckpointMeta, SourceError, SourceResult};
use mysql_async::prelude::Queryable;
use mysql_binlog_connector_rust::binlog_client::BinlogClient;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use url::Url;

use common::{
    RetryOutcome, RetryPolicy, redact_url_password as redact_password,
    retry_async,
};

use super::{MySqlCheckpoint, MySqlSourceError, MySqlSourceResult};

pub(super) async fn prepare_client(
    control: &super::mysql_control::ControlPool,
    dsn: &str,
    expected_uuid: &str,
    source_id: &str,
    last_checkpoint: Option<MySqlCheckpoint>,
) -> MySqlSourceResult<(String, String, u64, BinlogClient)> {
    let url = Url::parse(dsn)
        .map_err(|e| MySqlSourceError::InvalidDsn(e.to_string()))?;

    let host = url.host_str().unwrap_or("localhost").to_string();
    let default_db = url.path().trim_start_matches('/').to_string();
    let server_id = derive_server_id(source_id);

    debug!(source_id=%source_id, checkpoint=?last_checkpoint, "preparing client");

    let mut client = BinlogClient {
        url: dsn.to_string(),
        server_id,
        heartbeat_interval_secs: 180,
        timeout_secs: 60,
        ..Default::default()
    };

    debug!(
        url = redact_password(&client.url),
        server_id = client.server_id,
        "preparing the binlogclient"
    );

    // Prefer GTID, else file:pos, else resolve "end position" via SHOW ... STATUS
    if let Some(p) = &last_checkpoint {
        debug!(
            chkpt_file = p.file,
            chkpt_gtid_set = p.gtid_set,
            chkpt_pos = p.pos,
            "reviewing last checkpoint"
        );

        if let Some(gtid) = &p.gtid_set {
            client.gtid_enabled = true;
            // The checkpoint holds the full accumulated GTID set (built by
            // merge_gtid as events flow in). Use it directly so MySQL replays
            // exactly what we haven't seen yet. Fetching @@gtid_executed here
            // would advance past transactions inserted while the source was down.
            info!(source_id = %source_id, %gtid, "resuming via checkpoint GTID set");
            client.gtid_set = gtid.clone();
        } else {
            client.gtid_enabled = false;
            client.binlog_filename = p.file.clone();
            client.binlog_position = p.pos as u32;
            info!(
                source_id = %source_id,
                file = %p.file,
                pos = p.pos,
                "resuming via file:pos"
            );
        }
    } else {
        info!("no previous checkpoint, reading the capture boundary ..");
        let b = read_capture_boundary(control, dsn, expected_uuid).await?;
        info!(
            source_id = %source_id, file = %b.file, pos = b.pos,
            gtid_set = ?b.gtid_set, "start from the capture boundary (first run)"
        );
        client.binlog_filename = b.file;
        client.binlog_position = b.pos as u32;
        // GTID mode: the set is the boundary, consistent with the position.
        if let Some(set) = b.gtid_set {
            client.gtid_enabled = true;
            client.gtid_set = set;
        }
    }

    debug!(
        server_id = client.server_id,
        "prepare_client finished, returning .."
    );
    Ok((host, default_db, server_id, client))
}

/// A replication session opened by [`connect_binlog_with_retries`].
pub(super) enum Opened {
    /// Verified as the expected server; the dump command was issued.
    Stream(super::mysql_session::Session),
    /// The session reached another server (its `server_uuid`) and was closed
    /// before the dump command: no event was read.
    OtherServer(String),
}

/// Retry `connect_binlog`, building a new client with each attempt.
/// Instead of calling connect_binlog directly, this wrapper should be used.
/// Every session is verified as `expected_uuid` before its dump command;
/// another server is reported (never retried), an unverifiable identity is a
/// lineage error.
pub(super) async fn connect_binlog_with_retries(
    source_id: &str,
    expected_uuid: &str,
    make_client: impl FnMut() -> BinlogClient + Send + Clone + 'static,
    cancel: &CancellationToken,
    default_db_for_hints: &str,
    retry_policy: RetryPolicy,
) -> SourceResult<Opened> {
    let source_id = source_id.to_string();
    let expected = expected_uuid.to_string();
    let default_db = default_db_for_hints.to_string();
    let mk = make_client.clone();

    let result = retry_async(
        move |_| {
            let mut mk = mk.clone();
            let source_id = source_id.clone();
            let expected = expected.clone();
            let default_db = default_db.clone();
            async move {
                let client = mk();
                connect_binlog(&source_id, &expected, client, &default_db).await
            }
        },
        is_retryable_source_error,
        Duration::from_secs(30),
        retry_policy,
        cancel,
        "binlog_connect",
    )
    .await
    .map_err(|outcome| match outcome {
        RetryOutcome::Cancelled => SourceError::Cancelled,
        RetryOutcome::Timeout { action } => SourceError::Timeout { action },
        RetryOutcome::Exhausted {
            last_error,
            attempts,
        } => {
            warn!(attempts = attempts, "binlog connection exhausted retries");
            last_error
        }
        RetryOutcome::Failed(e) => e,
    });
    if matches!(result, Ok(Opened::Stream(_))) {
        // Real stream-open seam: lets tests assert a startup fault opens zero
        // streams (identity must be resolved/persisted before we get here).
        crate::stream_probe::record_stream_opened();
    }
    result
}

/// Determine if a SourceError is worth retrying.
fn is_retryable_source_error(e: &SourceError) -> bool {
    match e {
        SourceError::Timeout { .. } => true,
        SourceError::Connect { .. } => true,
        SourceError::Io(_) => true,
        SourceError::Cancelled => false,
        SourceError::Other(inner) => {
            // Check error message for retryable patterns
            let msg = inner.to_string().to_lowercase();
            msg.contains("connection")
                || msg.contains("timeout")
                || msg.contains("reset")
                || msg.contains("refused")
        }
        _ => false,
    }
}

/// Connect to binlog with timeout, on a session verified as `expected_uuid`.
async fn connect_binlog(
    source_id: &str,
    expected_uuid: &str,
    client: BinlogClient,
    default_db_for_hints: &str,
) -> Result<Opened, SourceError> {
    use super::mysql_session::{SessionError, open_replication_session};
    debug!("connecting to binlog");
    let t0 = Instant::now();

    match tokio::time::timeout(
        Duration::from_secs(30),
        open_replication_session(&client, expected_uuid, source_id),
    )
    .await
    {
        Ok(Ok(stream)) => {
            info!(
                source_id = %source_id,
                ms = t0.elapsed().as_millis() as u64,
                "connected to binlog (server identity verified)"
            );
            Ok(Opened::Stream(stream))
        }
        Ok(Err(SessionError::OtherServer { found })) => {
            warn!(
                source_id = %source_id,
                expected = %expected_uuid,
                %found,
                "replication session reached another server; closed before the dump"
            );
            Ok(Opened::OtherServer(found))
        }
        Ok(Err(e @ SessionError::NoIdentity(_))) => {
            Err(e.into_source_error(expected_uuid))
        }
        Ok(Err(SessionError::Connect(_, error_msg))) => {
            error!(source_id = %source_id, error = %error_msg, "binlog connect failed");

            if error_msg.contains("mysql_native_password") {
                error!("MySQL auth issue hints:");
                for hint in auth_hints(default_db_for_hints) {
                    error!("{hint}");
                }
            } else if error_msg.contains("Access denied") {
                error!(
                    "Access denied - check username/password and privileges"
                );
            } else if error_msg.contains("connect")
                || error_msg.contains("timeout")
            {
                error!(
                    "Connection failed - check that MySQL is running and accessible"
                );
            }

            Err(SourceError::Connect {
                details: error_msg.into(),
            })
        }
        Err(_) => Err(SourceError::Timeout {
            action: "binlog_connect".into(),
        }),
    }
}

// ----------------------------- private helpers / utilities -----------------------------

pub(super) fn derive_server_id(id: &str) -> u64 {
    let mut h = Hasher::new();
    h.update(id.as_bytes());
    1 + ((h.finalize() as u64) % 4_000_000_000u64)
}

/// A first start's capture boundary: the binlog position and executed GTID
/// set read together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct LogStatus {
    pub(super) file: String,
    pub(super) pos: u64,
    /// `None` with GTID mode off: the position alone is the boundary.
    pub(super) gtid_set: Option<String>,
}

/// The privileges [`read_capture_boundary`] needs.
pub(super) const LOG_STATUS_GRANTS: &str = "reading the capture boundary of a \
     first start needs BACKUP_ADMIN and SELECT on performance_schema.log_status: \
     GRANT BACKUP_ADMIN ON *.* TO <user>; GRANT SELECT ON \
     performance_schema.log_status TO <user>;";

/// A first start's capture boundary (no checkpoint), read in ONE
/// `performance_schema.log_status` query on a control connection verified
/// as `expected_uuid`. MySQL documents its `gtid_executed` as consistent with
/// its `binary_log_file` and `binary_log_position` (the server blocks logging
/// while it populates the table); `SHOW BINARY LOG STATUS` makes no such
/// guarantee (`docs/design/startup-capture-boundary.md`, section 5). There
/// is no fallback to positions sampled separately: a server that cannot
/// answer it stops the start.
pub(super) async fn read_capture_boundary(
    control: &super::mysql_control::ControlPool,
    dsn: &str,
    expected_uuid: &str,
) -> MySqlSourceResult<LogStatus> {
    use super::mysql_control::Class;
    let mut conn = match control
        .connect(Class::Position, dsn, expected_uuid, Class::Position.wait())
        .await
        .map_err(super::mysql_control::ControlError::into_session)
    {
        Ok(conn) => conn,
        Err(super::mysql_session::SessionError::Connect(_, e)) => {
            return Err(MySqlSourceError::BinlogConnect(format!(
                "MySQL connection failed: {e}"
            )));
        }
        Err(e) => {
            return Err(MySqlSourceError::Lineage(format!(
                "capture boundary: {e:?} (expected {expected_uuid})"
            )));
        }
    };
    let row: Result<Option<(String, String)>, _> = conn
        .query_first(
            "SELECT SERVER_UUID, CAST(LOCAL AS CHAR) \
             FROM performance_schema.log_status",
        )
        .await;
    conn.release().await;
    match row {
        Ok(Some((uuid, local))) => {
            parse_log_status(&uuid, &local, expected_uuid)
        }
        Ok(None) => Err(MySqlSourceError::Incompatible(
            "performance_schema.log_status returned no row".into(),
        )),
        // ER_TABLEACCESS_DENIED_ERROR, ER_SPECIFIC_ACCESS_DENIED_ERROR.
        Err(mysql_async::Error::Server(e)) if matches!(e.code, 1142 | 1227) => {
            Err(MySqlSourceError::Permission(format!(
                "{LOG_STATUS_GRANTS} (server error {}: {})",
                e.code, e.message
            )))
        }
        Err(e) => Err(MySqlSourceError::Driver(e)),
    }
}

/// Parse one `log_status` row. The server must be the verified one; the
/// binary log must be on; an empty `gtid_executed` means GTID mode is off.
pub(super) fn parse_log_status(
    server_uuid: &str,
    local: &str,
    expected_uuid: &str,
) -> MySqlSourceResult<LogStatus> {
    if !server_uuid.eq_ignore_ascii_case(expected_uuid) {
        return Err(MySqlSourceError::Lineage(format!(
            "capture boundary read on server {server_uuid}, expected \
             {expected_uuid}"
        )));
    }
    #[derive(serde::Deserialize)]
    struct Local {
        binary_log_file: Option<String>,
        binary_log_position: Option<u64>,
        gtid_executed: Option<String>,
    }
    let l: Local = serde_json::from_str(local).map_err(|e| {
        MySqlSourceError::Incompatible(format!(
            "unparseable performance_schema.log_status LOCAL {local:?}: {e}"
        ))
    })?;
    let (file, pos) = match (l.binary_log_file, l.binary_log_position) {
        (Some(f), Some(p)) if !f.is_empty() => (f, p),
        _ => {
            return Err(MySqlSourceError::Incompatible(
                "binary logging is disabled: no binary log position in \
                 performance_schema.log_status (start the server with \
                 --log-bin and binlog_format=ROW)"
                    .into(),
            ));
        }
    };
    let gtid_set = match l.gtid_executed.as_deref().map(str::trim) {
        None | Some("") => None,
        // The server's text, in the one canonical form.
        Some(t) => Some(
            crate::gtid_set::GtidSet::parse(t)
                .ok_or_else(|| {
                    MySqlSourceError::Incompatible(format!(
                        "unparseable gtid_executed {t:?}"
                    ))
                })?
                .to_string(),
        ),
    };
    Ok(LogStatus {
        file,
        pos,
        gtid_set,
    })
}

/// The account a binlog connection needs, as hints. The user takes the
/// server's default authentication plugin (`caching_sha2_password` on
/// MySQL 8.4, where `mysql_native_password` is disabled by default).
fn auth_hints(db: &str) -> Vec<String> {
    vec![
        "  1) CREATE USER 'df'@'%' IDENTIFIED BY 'dfpw';  (or ALTER USER ... IDENTIFIED BY ... to move an existing user to the server's default plugin)".to_string(),
        "  2) GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'df'@'%';".to_string(),
        format!("  3) GRANT SELECT, SHOW VIEW ON {db}.* TO 'df'@'%';"),
        "  4) GRANT BACKUP_ADMIN ON *.* TO 'df'@'%'; GRANT SELECT ON performance_schema.log_status TO 'df'@'%';  (a first start without a checkpoint reads its capture boundary there)".to_string(),
        "  5) FLUSH PRIVILEGES;".to_string(),
    ]
}

/// Shorten SQL for logs
pub(crate) fn short_sql(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max])
    }
}

/// The verified registry lineage hash a new checkpoint is stamped with:
/// the published scope's lineage (`None` only before the scope is published,
/// which no checkpoint-writing path reaches).
pub(crate) fn checkpoint_lineage(
    scope: &crate::registry_scope::SharedRegistryScope,
) -> Option<String> {
    scope
        .current()
        .ok()
        .map(|s| s.lineage().lineage_hash.clone())
}

pub(crate) fn make_checkpoint_meta(
    file: &str,
    pos: u64,
    gtid: &Option<String>,
    lineage: Option<String>,
) -> CheckpointMeta {
    let cp = MySqlCheckpoint {
        file: file.to_string(),
        pos,
        gtid_set: gtid.clone(),
        lineage,
        snapshot_completed: None,
        snapshot_chain: None,
    };

    let bytes = serde_json::to_vec(&cp).unwrap_or_else(|e| {
        tracing::error!(error=%e, "failed to serialize checkpoint");
        Vec::new()
    });
    CheckpointMeta::from_vec(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The account remediation MySQL 8.4 accepts: `REPLICATION SLAVE` (8.4
    /// rejects `REPLICATION REPLICA` with a syntax error), a user on the
    /// server's default authentication plugin (8.4 disables
    /// `mysql_native_password`), and the grants a first start reads its
    /// capture boundary with.
    #[test]
    fn account_remediation_is_valid_on_mysql_8_4() {
        assert_eq!(
            auth_hints("shop"),
            [
                "  1) CREATE USER 'df'@'%' IDENTIFIED BY 'dfpw';  (or ALTER USER ... IDENTIFIED BY ... to move an existing user to the server's default plugin)",
                "  2) GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'df'@'%';",
                "  3) GRANT SELECT, SHOW VIEW ON shop.* TO 'df'@'%';",
                "  4) GRANT BACKUP_ADMIN ON *.* TO 'df'@'%'; GRANT SELECT ON performance_schema.log_status TO 'df'@'%';  (a first start without a checkpoint reads its capture boundary there)",
                "  5) FLUSH PRIVILEGES;",
            ]
        );
    }

    const UUID: &str = "37b3bf36-d624-11f1-a18e-0242ac110003";

    /// GTID on (MySQL 8.4.9's row): the position and the canonical set.
    #[test]
    fn log_status_gives_the_position_and_its_gtid_set() {
        let b = parse_log_status(
            UUID,
            r#"{"gtid_executed": "37b3bf36-d624-11f1-a18e-0242ac110003:1-5", "binary_log_file": "mysql-bin.000003", "binary_log_position": 198}"#,
            &UUID.to_uppercase(),
        )
        .unwrap();
        assert_eq!(
            b,
            LogStatus {
                file: "mysql-bin.000003".into(),
                pos: 198,
                gtid_set: Some(format!("{UUID}:1-5")),
            }
        );
    }

    /// GTID off (8.4.9): `gtid_executed` is empty; the position alone.
    #[test]
    fn gtid_off_gives_the_position_alone() {
        let b = parse_log_status(
            UUID,
            r#"{"gtid_executed": "", "binary_log_file": "mysql-bin.000003", "binary_log_position": 158}"#,
            UUID,
        )
        .unwrap();
        assert_eq!(b.gtid_set, None);
        assert_eq!((b.file.as_str(), b.pos), ("mysql-bin.000003", 158));
    }

    #[test]
    fn another_server_or_no_binary_log_fails_closed() {
        let local = r#"{"gtid_executed": "", "binary_log_file": "b.1", "binary_log_position": 4}"#;
        assert!(matches!(
            parse_log_status(
                "00000000-0000-0000-0000-000000000000",
                local,
                UUID
            ),
            Err(MySqlSourceError::Lineage(_))
        ));
        assert!(matches!(
            parse_log_status(UUID, r#"{"gtid_executed": ""}"#, UUID),
            Err(MySqlSourceError::Incompatible(_))
        ));
        assert!(matches!(
            parse_log_status(UUID, "not json", UUID),
            Err(MySqlSourceError::Incompatible(_))
        ));
    }
}
