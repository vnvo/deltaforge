use crc32fast::Hasher;
use deltaforge_core::{CheckpointMeta, SourceError, SourceResult};
use mysql_async::{Row, prelude::Queryable};
use mysql_binlog_connector_rust::{
    binlog_client::BinlogClient, binlog_stream::BinlogStream,
};
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
        info!("no previous checkpoint, reading the binlog tail ..");
        match resolve_binlog_tail(dsn, expected_uuid).await {
            Ok((file, pos)) => {
                info!(source_id = %source_id, %file, %pos, "start from end (first run)");
                client.binlog_filename = file;
                client.binlog_position = pos as u32;
                // Also fetch the full executed GTID set so reconnects resume
                // from here rather than replaying the full binlog history.
                match fetch_executed_gtid_set(dsn, expected_uuid).await {
                    // With GTID off the executed set is '' (not NULL): no
                    // GTID resume position, stay on the file/position tail.
                    Ok(Some(gtid_set)) if !gtid_set.trim().is_empty() => {
                        client.gtid_enabled = true;
                        client.gtid_set = gtid_set;
                    }
                    Err(SourceError::Lineage { details }) => {
                        return Err(MySqlSourceError::Lineage(details.into()));
                    }
                    Ok(_) | Err(_) => {}
                }
            }
            Err(e) => {
                error!(source_id = %source_id, error = %e, "failed to resolve binlog tail");
                return Err(e);
            }
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
    Stream(BinlogStream),
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
        open_replication_session(&client, expected_uuid),
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
        Ok(Err(SessionError::Connect(error_msg))) => {
            error!(source_id = %source_id, error = %error_msg, "binlog connect failed");

            if error_msg.contains("mysql_native_password") {
                error!("MySQL auth issue hints:");
                error!(
                    "  1) CREATE USER 'df'@'%' IDENTIFIED WITH mysql_native_password BY 'dfpw';"
                );
                error!(
                    "  2) GRANT REPLICATION REPLICA, REPLICATION CLIENT ON *.* TO 'df'@'%';"
                );
                error!(
                    "  3) GRANT SELECT, SHOW VIEW ON {}.* TO 'df'@'%';",
                    default_db_for_hints
                );
                error!("  4) FLUSH PRIVILEGES;");
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

/// Resolve end-of-binlog with comprehensive diagnostics.
///
/// The position is read on a control connection verified as `expected_uuid`:
/// it is a fact about that server only.
pub(super) async fn resolve_binlog_tail(
    dsn: &str,
    expected_uuid: &str,
) -> MySqlSourceResult<(String, u64)> {
    info!("connecting to MySQL for binlog tail resolution");

    let mut conn = match super::mysql_session::open_control_connection(
        dsn,
        expected_uuid,
        Duration::from_secs(10),
    )
    .await
    {
        Ok(conn) => {
            info!("connected to MySQL (identity verified)");
            conn
        }
        Err(super::mysql_session::SessionError::Connect(e)) => {
            error!("failed to connect to MySQL: {}", e);
            return Err(MySqlSourceError::BinlogConnect(format!(
                "MySQL connection failed: {e}"
            )));
        }
        Err(e) => {
            return Err(MySqlSourceError::Lineage(format!(
                "binlog tail resolution: {e:?} (expected {expected_uuid})"
            )));
        }
    };

    // Who am I?
    info!("checking user privileges");
    match conn.query_first::<Row, _>("SELECT CURRENT_USER()").await {
        Ok(Some(mut row)) => {
            let current_user: String = row.take(0).unwrap_or_default();
            info!("connected as user: {}", current_user);
        }
        Ok(None) => warn!("could not determine current user"),
        Err(e) => {
            error!("failed to check current user: {}", e);
            conn.disconnect().await?;
            return Err(MySqlSourceError::BinlogConnect(format!(
                "failed to check current user: {e}"
            )));
        }
    }

    // Preferred status command
    info!("trying SHOW BINARY LOG STATUS");
    match tokio::time::timeout(
        Duration::from_secs(5),
        conn.query_first::<Row, _>("SHOW BINARY LOG STATUS"),
    )
    .await
    {
        Ok(Ok(Some(mut row))) => {
            if let (Some(file), Some(pos)) =
                (row.take::<String, _>(0), row.take::<u64, _>(1))
            {
                info!(
                    "successfully got binlog position: file={}, pos={}",
                    file, pos
                );
                conn.disconnect().await?;
                return Ok((file, pos));
            } else {
                warn!("SHOW BINARY LOG STATUS returned incomplete data");
            }
        }
        Ok(Ok(None)) => warn!(
            "SHOW BINARY LOG STATUS returned no rows - binlog might be disabled"
        ),
        Ok(Err(e)) => warn!(
            "SHOW BINARY LOG STATUS failed: {} - trying legacy syntax",
            e
        ),
        Err(_) => {
            warn!("SHOW BINARY LOG STATUS timed out - trying legacy syntax")
        }
    }

    // Legacy fallback
    info!("trying SHOW MASTER STATUS (legacy syntax)");
    match tokio::time::timeout(
        Duration::from_secs(5),
        conn.query_first::<Row, _>("SHOW MASTER STATUS"),
    )
    .await
    {
        Ok(Ok(Some(mut row))) => {
            if let (Some(file), Some(pos)) =
                (row.take::<String, _>(0), row.take::<u64, _>(1))
            {
                info!(
                    "successfully got binlog position via legacy command: file={}, pos={}",
                    file, pos
                );
                conn.disconnect().await?;
                return Ok((file, pos));
            } else {
                warn!("SHOW MASTER STATUS returned incomplete data");
            }
        }
        Ok(Ok(None)) => warn!("SHOW MASTER STATUS returned no rows"),
        Ok(Err(e)) => warn!("SHOW MASTER STATUS failed: {}", e),
        Err(_) => warn!("SHOW MASTER STATUS timed out"),
    }

    // Diagnostics
    info!("running diagnostics to determine the issue");
    let mut diagnostics = Vec::new();

    // log_bin
    match conn
        .exec_first::<(String, String), _, _>(
            "SHOW VARIABLES LIKE 'log_bin'",
            (),
        )
        .await
    {
        Ok(Some((_, value))) => {
            if value.eq_ignore_ascii_case("OFF") {
                diagnostics
                    .push("FAIL: Binary logging is DISABLED".to_string());
                diagnostics.push(
                    "   Fix: Add --log-bin to MySQL startup command"
                        .to_string(),
                );
            } else {
                diagnostics.push("OK: Binary logging is enabled".to_string());
            }
        }
        Ok(None) => diagnostics
            .push("WARN: Could not check log_bin variable".to_string()),
        Err(e) => {
            diagnostics.push(format!("FAIL: Failed to check log_bin: {}", e))
        }
    }

    // binlog_format
    match conn
        .exec_first::<(String, String), _, _>(
            "SHOW VARIABLES LIKE 'binlog_format'",
            (),
        )
        .await
    {
        Ok(Some((_, value))) => {
            if value.eq_ignore_ascii_case("ROW") {
                diagnostics.push("OK: Binlog format is ROW".to_string());
            } else {
                diagnostics.push(format!(
                    "WARN: Binlog format is '{}' (should be ROW)",
                    value
                ));
            }
        }
        Ok(None) => {
            diagnostics.push("WARN: Could not check binlog_format".to_string())
        }
        Err(e) => diagnostics
            .push(format!("FAIL: Failed to check binlog_format: {}", e)),
    }

    // grants
    match conn.query::<Row, _>("SHOW GRANTS").await {
        Ok(rows) => {
            let grants: Vec<String> = rows
                .into_iter()
                .map(|mut row| row.take::<String, _>(0).unwrap_or_default())
                .collect();
            let has_replication = grants.iter().any(|g| {
                g.contains("REPLICATION SLAVE")
                    || g.contains("REPLICATION REPLICA")
                    || g.contains("ALL PRIVILEGES")
            });
            if has_replication {
                diagnostics
                    .push("OK: User has replication privileges".to_string());
            } else {
                diagnostics.push(
                    "FAIL: User lacks REPLICATION REPLICA privilege"
                        .to_string(),
                );
                diagnostics.push("   Fix: GRANT REPLICATION REPLICA, REPLICATION CLIENT ON *.* TO user;".to_string());
            }
            debug!("user grants: {:?}", grants);
        }
        Err(e) => {
            diagnostics
                .push(format!("FAIL: Could not check user grants: {}", e));
            diagnostics.push(
                "   This usually means insufficient privileges".to_string(),
            );
        }
    }

    // version
    match conn.query_first::<Row, _>("SELECT VERSION()").await {
        Ok(Some(mut row)) => {
            let version: String = row.take(0).unwrap_or_default();
            diagnostics.push(format!("INFO: MySQL version: {}", version));
        }
        Ok(None) => diagnostics
            .push("WARN: Could not determine MySQL version".to_string()),
        Err(e) => diagnostics
            .push(format!("FAIL: Failed to get MySQL version: {}", e)),
    }

    conn.disconnect().await?;

    // Print diagnostics and bail
    error!("could not resolve binlog end position. Diagnostics:");
    for diag in &diagnostics {
        error!("  {}", diag);
    }

    Err(MySqlSourceError::BinlogConnect(
        "Failed to resolve binlog position. Common fixes:\n\
         1) Enable binary logging: --log-bin --binlog-format=ROW\n\
         2) Grant privileges: GRANT REPLICATION REPLICA, REPLICATION CLIENT ON *.* TO ...;\n\
         3) Ensure user exists: CREATE USER '..'@'%' IDENTIFIED BY 'password';"
            .to_string(),
    ))
}

/// Shorten SQL for logs
pub(crate) fn short_sql(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max])
    }
}

/// `@@GLOBAL.gtid_executed`, read on a control connection verified as
/// `expected_uuid`.
pub(super) async fn fetch_executed_gtid_set(
    dsn: &str,
    expected_uuid: &str,
) -> SourceResult<Option<String>> {
    let mut conn = super::mysql_session::open_control_connection(
        dsn,
        expected_uuid,
        Duration::from_secs(10),
    )
    .await
    .map_err(|e| e.into_source_error(expected_uuid))?;

    // Returns NULL if GTID is disabled
    let row: Option<(Option<String>,)> = conn
        .query_first("SELECT @@GLOBAL.gtid_executed")
        .await
        .map_err(|e| SourceError::Other(e.into()))?;

    conn.disconnect()
        .await
        .map_err(|e| SourceError::Other(e.into()))?;

    Ok(row.and_then(|(s,)| s))
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
