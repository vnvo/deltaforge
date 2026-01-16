use crc32fast::Hasher;
use deltaforge_core::{CheckpointMeta, SourceError, SourceResult};
use mysql_async::{Pool, Row, prelude::Queryable};
use mysql_binlog_connector_rust::{
    binlog_client::BinlogClient, binlog_stream::BinlogStream,
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use url::Url;

use checkpoints::{CheckpointStore, CheckpointStoreExt};

use common::{
    RetryOutcome, RetryPolicy, redact_url_password as redact_password,
    retry_async,
};

use super::{MySqlCheckpoint, MySqlSourceError, MySqlSourceResult};

pub(super) async fn prepare_client(
    dsn: &str,
    source_id: &str,
    ckpt_store: &Arc<dyn CheckpointStore>,
) -> MySqlSourceResult<(String, String, u64, BinlogClient)> {
    let url = Url::parse(dsn)
        .map_err(|e| MySqlSourceError::InvalidDsn(e.to_string()))?;

    let host = url.host_str().unwrap_or("localhost").to_string();
    let default_db = url.path().trim_start_matches('/').to_string();
    let server_id = derive_server_id(source_id);

    // Previous checkpoint (if any)
    let last_checkpoint: Option<MySqlCheckpoint> = ckpt_store
        .get(source_id)
        .await
        .map_err(|e| MySqlSourceError::Checkpoint(e.to_string()))?;

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
            client.gtid_set = gtid.clone();
            info!(source_id = %source_id, %gtid, "resuming via GTID");
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
        match resolve_binlog_tail(dsn).await {
            Ok((file, pos)) => {
                info!(source_id = %source_id, %file, %pos, "start from end (first run)");
                client.binlog_filename = file;
                client.binlog_position = pos as u32;
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

/// Retry `connect_binlog`, building a new client with each attempt.
/// Instead of calling connect_binlog directly, this wrapper should be used.
pub(super) async fn connect_binlog_with_retries(
    source_id: &str,
    make_client: impl FnMut() -> BinlogClient + Send + Clone + 'static,
    cancel: &CancellationToken,
    default_db_for_hints: &str,
    retry_policy: RetryPolicy,
) -> SourceResult<BinlogStream> {
    let source_id = source_id.to_string();
    let default_db = default_db_for_hints.to_string();
    let mk = make_client.clone();

    retry_async(
        move |_| {
            let mut mk = mk.clone();
            let source_id = source_id.clone();
            let default_db = default_db.clone();
            async move {
                let client = mk();
                connect_binlog(&source_id, client, &default_db).await
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
    })
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

/// Connect to binlog with timeout.
async fn connect_binlog(
    source_id: &str,
    mut client: BinlogClient,
    default_db_for_hints: &str,
) -> Result<BinlogStream, SourceError> {
    debug!("connecting to binlog");
    let t0 = Instant::now();

    match tokio::time::timeout(Duration::from_secs(30), client.connect()).await
    {
        Ok(Ok(stream)) => {
            info!(
                source_id = %source_id,
                ms = t0.elapsed().as_millis() as u64,
                "connected to binlog"
            );
            Ok(stream)
        }
        Ok(Err(e)) => {
            let error_msg = e.to_string();
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
pub(super) async fn resolve_binlog_tail(
    dsn: &str,
) -> MySqlSourceResult<(String, u64)> {
    info!("connecting to MySQL for binlog tail resolution");

    let pool = Pool::new(dsn);

    // Connect with timeout
    let mut conn =
        match tokio::time::timeout(Duration::from_secs(10), pool.get_conn())
            .await
        {
            Ok(Ok(conn)) => {
                info!("successfully connected to MySQL");
                conn
            }
            Ok(Err(e)) => {
                error!("failed to connect to MySQL: {}", e);
                return Err(MySqlSourceError::BinlogConnect(format!(
                    "MySQL connection failed: {e}"
                )));
            }
            Err(_) => {
                error!("MySQL connection timed out after 10 seconds");
                return Err(MySqlSourceError::BinlogConnect(
                    "MySQL connection timed out".to_owned(),
                ));
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
            pool.disconnect().await?;
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
                pool.disconnect().await?;
                debug!("pool.disconnect completed, returning");
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
                pool.disconnect().await?;
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

    pool.disconnect().await?;

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

#[allow(dead_code)]
pub(super) async fn fetch_executed_gtid_set(
    dsn: &str,
) -> SourceResult<Option<String>> {
    let pool = Pool::new(dsn);
    let mut conn = pool.get_conn().await.map_err(|e| SourceError::Connect {
        details: format!("mysql connect for gtid: {e}").into(),
    })?;

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

pub(crate) fn make_checkpoint_meta(
    file: &str,
    pos: u64,
    gtid: &Option<String>,
) -> CheckpointMeta {
    let cp = MySqlCheckpoint {
        file: file.to_string(),
        pos,
        gtid_set: gtid.clone(),
    };

    let bytes = serde_json::to_vec(&cp).unwrap_or_else(|e| {
        tracing::error!(error=%e, "failed to serialize checkpoint");
        Vec::new()
    });
    CheckpointMeta::from_vec(bytes)
}
