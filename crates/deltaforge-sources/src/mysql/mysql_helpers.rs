use anyhow::{Context, Result, anyhow};
use crc32fast::Hasher;
use deltaforge_core::{SourceError, SourceResult};
use mysql_async::{Pool, Row, prelude::Queryable};
use mysql_binlog_connector_rust::{
    binlog_client::BinlogClient, binlog_stream::BinlogStream,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::select;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use url::Url;

use deltaforge_checkpoints::{CheckpointStore, CheckpointStoreExt};

use crate::conn_utils::{RetryPolicy, retry_async, retryable_connect};

use super::{MySqlCheckpoint, MySqlSourceError, MySqlSourceResult};
// ----------------------------- public helpers (to mysql.rs) -----------------------------

/// Prepare BinlogClient from DSN + last checkpoint; returns (host, default_db, server_id, client).
pub(super) async fn prepare_client(
    dsn: &str,
    source_id: &str,
    tables: &[String],
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

    info!(source_id=%source_id, url=%url, checkpoint=?last_checkpoint, "preparing client");
    let mut client = BinlogClient::default();
    client.url = dsn.to_string();
    client.server_id = server_id; // connector uses u32 server_id
    client.heartbeat_interval_secs = 180;
    client.timeout_secs = 60;

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

    // Touch allow-list to avoid “unused” lint in callers that log it
    let _ = AllowList::new(tables);

    Ok((host, default_db, server_id, client))
}

/// retry `connect_binlog`, building a new client with each attempt
/// instead of calling connect_binlog directly, this wrapper should be used
pub(super) async fn connect_binlog_with_retries(
    source_id: &str,
    make_client: impl FnMut() -> BinlogClient + Send + Clone + Send + 'static,
    cancel: &CancellationToken,
    default_db_for_hints: &str,
    retry_policy: RetryPolicy,
) -> SourceResult<BinlogStream> {
    let mk = make_client.clone();

    let stream = retry_async(
        move |_| {
            let mut mk = mk.clone();
            async move {
                let client = mk();
                connect_binlog(source_id, client, cancel, default_db_for_hints)
                    .await
            }
        },
        |e| e,
        retryable_connect,
        Duration::from_secs(30),
        retry_policy,
        cancel,
        "binlog_connect",
    )
    .await?;

    Ok(stream)
}

/// Connect to binlog with cancellation and timeout.
pub(super) async fn connect_binlog(
    source_id: &str,
    mut client: BinlogClient,
    cancel: &CancellationToken,
    default_db_for_hints: &str,
) -> SourceResult<BinlogStream> {
    debug!("connecting to binlog");
    let t0 = Instant::now();
    let connect_fut = client.connect();

    let stream = select! {
        _ = cancel.cancelled() => Err(SourceError::Cancelled),
        res = tokio::time::timeout(std::time::Duration::from_secs(30), connect_fut) => {
            match res {
                Ok(Ok(s)) => Ok(s),
                Ok(Err(e)) => {
                    let error_msg = e.to_string();
                    error!(source_id = %source_id, error = %error_msg, "binlog connect failed");
                    if error_msg.contains("mysql_native_password") {
                        error!("MySQL auth issue hints:");
                        error!("  1) CREATE USER 'df'@'%' IDENTIFIED WITH mysql_native_password BY 'dfpw';");
                        error!("  2) GRANT REPLICATION REPLICA, REPLICATION CLIENT ON *.* TO 'df'@'%';");
                        error!("  3) GRANT SELECT, SHOW VIEW ON {}.* TO 'df'@'%';", default_db_for_hints);
                        error!("  4) FLUSH PRIVILEGES;");
                    } else if error_msg.contains("Access denied") {
                        error!("Access denied - check username/password and privileges");
                    } else if error_msg.contains("connect") || error_msg.contains("timeout") {
                        error!("Connection failed - check that MySQL is running and accessible");
                    }
                    Err(SourceError::Other(e.into()))
                }
                Err(_) => Err(SourceError::Timeout { action: "binlog_connect".into()}),
            }
        }
    }?;

    info!(
        source_id = %source_id,
        ms = t0.elapsed().as_millis() as u64,
        "connected to binlog"
    );
    Ok(stream)
}

/// Pause gate: wait until resume or cancel. Returns false if cancelled, true otherwise.
pub(super) async fn pause_until_resumed(
    cancel: &CancellationToken,
    paused: &AtomicBool,
    notify: &Notify,
) -> bool {
    if !paused.load(Ordering::SeqCst) {
        return true;
    }
    debug!("paused");
    select! {
        _ = cancel.cancelled() => return false,
        _ = notify.notified() => {}
    }
    true
}

/// Persist checkpoint (best effort), with logging.
pub(crate) async fn persist_checkpoint(
    source_id: &str,
    ckpt_store: &Arc<dyn CheckpointStore>,
    file: &str,
    pos: u64,
    gtid: &Option<String>,
) {
    if let Err(e) = ckpt_store
        .put(
            source_id,
            MySqlCheckpoint {
                file: file.to_string(),
                pos,
                gtid_set: gtid.clone(),
            },
        )
        .await
    {
        error!(source_id = %source_id, error = %e, "failed to persist checkpoint");
    } else {
        debug!(source_id = %source_id, file = %file, pos = pos, gtid = ?gtid, "checkpoint saved");
    }
}

// ----------------------------- private helpers / utilities -----------------------------

pub(crate) fn ts_ms(ts_sec: u32) -> i64 {
    (ts_sec as i64) * 1000
}

/// Redact password from DSN for logging.
pub(crate) fn redact_password(dsn: &str) -> String {
    if let Ok(mut url) = Url::parse(dsn) {
        if url.password().is_some() {
            let _ = url.set_password(Some("***"));
        }
        url.to_string()
    } else {
        dsn.to_string()
    }
}

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
    let mut conn = match tokio::time::timeout(
        std::time::Duration::from_secs(10),
        pool.get_conn(),
    )
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
        std::time::Duration::from_secs(5),
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
        std::time::Duration::from_secs(5),
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

/// Simple allow-list on "db.table" or just "table"
#[derive(Clone)]
pub(super) struct AllowList {
    items: Vec<(Option<String>, String)>,
}
impl AllowList {
    pub(super) fn new(list: &[String]) -> Self {
        let items = list
            .iter()
            .map(|s| {
                if let Some((db, tb)) = s.split_once('.') {
                    (Some(db.to_string()), tb.to_string())
                } else {
                    (None, s.to_string())
                }
            })
            .collect();
        Self { items }
    }
    pub(super) fn matchs(&self, db: &str, table: &str) -> bool {
        if self.items.is_empty() {
            return true;
        }
        self.items.iter().any(|(d_opt, t)| {
            (d_opt.as_deref().map(|d| d == db).unwrap_or(true)) && t == table
        })
    }
}

/// Shorten SQL for logs
pub(crate) fn short_sql(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max])
    }
}

pub(super) async fn fetch_executed_gtid_set(
    dsn: &str,
) -> SourceResult<Option<String>> {
    // Use a short-lived connection for now (simple & correct).
    // We can optimize by keeping a Pool in RunCtx later.
    let pool = Pool::new(dsn);
    let mut conn = pool
        .get_conn()
        .await
        .map_err(|e| SourceError::Other(e.into()))?;
    
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
