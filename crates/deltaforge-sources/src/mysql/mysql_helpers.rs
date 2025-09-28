use anyhow::{anyhow, Context, Result};
use base64::prelude::*;
use crc32fast::Hasher;
use mysql_async::{prelude::Queryable, Pool, Row};
use mysql_binlog_connector_rust::{
    binlog_client::BinlogClient,
    binlog_stream::BinlogStream,
    column::column_value::ColumnValue,
    event::{
        delete_rows_event::DeleteRowsEvent, event_data::EventData,
        event_header::EventHeader, table_map_event::TableMapEvent,
        update_rows_event::UpdateRowsEvent, write_rows_event::WriteRowsEvent,
    },
};
use mysql_common::{
    binlog::jsonb,
    proto::{MyDeserialize, ParseBuf},
};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Instant,
};
use tokio::select;
use tokio::sync::{mpsc, Notify, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use url::Url;

use deltaforge_checkpoints::{CheckpointStore, CheckpointStoreExt};
use deltaforge_core::{Event, Op, SourceMeta};

use super::MySqlCheckpoint;

// ----------------------------- public helpers (to mysql.rs) -----------------------------

/// Prepare BinlogClient from DSN + last checkpoint; returns (host, default_db, server_id, client).
pub(super) async fn prepare_client(
    dsn: &str,
    source_id: &str,
    tables: &[String],
    ckpt_store: &Arc<dyn CheckpointStore>,
) -> Result<(String, String, u64, BinlogClient)> {
    let url = Url::parse(dsn).context("invalid MySQL DSN")?;
    let host = url.host_str().unwrap_or("localhost").to_string();
    let default_db = url.path().trim_start_matches('/').to_string();
    let server_id = derive_server_id(source_id);

    // Previous checkpoint (if any)
    let last_checkpoint: Option<MySqlCheckpoint> =
        ckpt_store.get(source_id).await?;

    let mut client = BinlogClient::default();
    client.url = dsn.to_string();
    client.server_id = server_id as u32; // connector uses u32 server_id
    client.heartbeat_interval_secs = 15;
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

/// Connect to binlog with cancellation and timeout.
pub(super) async fn connect_binlog(
    source_id: &str,
    mut client: BinlogClient,
    cancel: &CancellationToken,
    default_db_for_hints: &str,
) -> Result<BinlogStream> {
    debug!("connecting to binlog");
    let t0 = Instant::now();
    let connect_fut = client.connect();

    let stream = select! {
        _ = cancel.cancelled() => Err(anyhow!("binlog connect cancelled")),
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
                    Err(e.into())
                }
                Err(_) => Err(anyhow!("binlog connection timeout")),
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
pub(super) async fn persist_checkpoint(
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

/// Handle a single binlog event. Returns `Some(last_pos)` at commit (XID) for checkpoint persistence.
pub(super) async fn handle_event(
    source_id: &str,
    tenant: &str,
    header: &EventHeader,
    data: EventData,
    allow: &AllowList,
    schema_cache: &MySqlSchemaCache,
    table_map: &mut HashMap<u64, TableMapEvent>,
    host: &str,
    default_db: &str,
    last_file: &mut String,
    last_gtid: &mut Option<String>,
    tx: &mpsc::Sender<Event>,
) -> Result<Option<u64>> {
    let last_pos = header.next_event_position as u64;

    debug!(
        timestamp = header.timestamp,
        event_type = header.event_type,
        event_length = header.event_length,
        server_id = header.server_id,
        next_event_pos = last_pos,
        "mysql source, next event received"
    );

    match data {
        EventData::TableMap(tm) => {
            let is_new = !table_map.contains_key(&tm.table_id);
            table_map.insert(tm.table_id, tm.clone());

            if allow.matchs(&tm.database_name, &tm.table_name) {
                if is_new {
                    info!(
                        source_id = %source_id,
                        table_id = tm.table_id,
                        db = %tm.database_name,
                        table = %tm.table_name,
                        "table mapped"
                    );
                } else {
                    debug!(
                        source_id = %source_id,
                        table_id = tm.table_id,
                        db = %tm.database_name,
                        table = %tm.table_name,
                        "table remapped"
                    );
                }
            } else {
                debug!(
                    source_id = %source_id,
                    db = %tm.database_name,
                    table = %tm.table_name,
                    "skipping table (allow-list)"
                );
            }
        }

        EventData::WriteRows(WriteRowsEvent {
            table_id,
            included_columns,
            rows,
            ..
        }) => {
            if let Some(tm) = table_map.get(&table_id) {
                if !allow.matchs(&tm.database_name, &tm.table_name) {
                    return Ok(None);
                }
                let cols = schema_cache
                    .column_names(&tm.database_name, &tm.table_name)
                    .await?;
                debug!(
                    source_id = %source_id,
                    db = %tm.database_name,
                    table = %tm.table_name,
                    rows = rows.len(),
                    "write_rows"
                );
                for row in rows {
                    let after = build_object(
                        &cols,
                        &included_columns,
                        &row.column_values,
                    );
                    let mut ev = Event::new_row(
                        tenant.to_string(),
                        SourceMeta {
                            kind: "mysql".into(),
                            host: host.to_string(),
                            db: tm.database_name.clone(),
                        },
                        format!("{}.{}", tm.database_name, tm.table_name),
                        Op::Insert,
                        None,
                        Some(after),
                        ts_ms(header.timestamp),
                    );
                    ev.tx_id = last_gtid.clone();
                    let _ = tx.send(ev).await;
                }
            } else {
                warn!(source_id = %source_id, table_id, "write_rows for unknown table_id");
            }
        }

        EventData::UpdateRows(UpdateRowsEvent {
            table_id,
            included_columns_before,
            included_columns_after,
            rows,
            ..
        }) => {
            if let Some(tm) = table_map.get(&table_id) {
                if !allow.matchs(&tm.database_name, &tm.table_name) {
                    return Ok(None);
                }
                let cols = schema_cache
                    .column_names(&tm.database_name, &tm.table_name)
                    .await?;
                debug!(
                    source_id = %source_id,
                    db = %tm.database_name,
                    table = %tm.table_name,
                    rows = rows.len(),
                    "update_rows"
                );
                for (before_row, after_row) in rows {
                    let before = build_object(
                        &cols,
                        &included_columns_before,
                        &before_row.column_values,
                    );
                    let after = build_object(
                        &cols,
                        &included_columns_after,
                        &after_row.column_values,
                    );
                    let mut ev = Event::new_row(
                        tenant.to_string(),
                        SourceMeta {
                            kind: "mysql".into(),
                            host: host.to_string(),
                            db: tm.database_name.clone(),
                        },
                        format!("{}.{}", tm.database_name, tm.table_name),
                        Op::Update,
                        Some(before),
                        Some(after),
                        ts_ms(header.timestamp),
                    );
                    ev.tx_id = last_gtid.clone();
                    let _ = tx.send(ev).await;
                }
            } else {
                warn!(source_id = %source_id, table_id, "update_rows for unknown table_id");
            }
        }

        EventData::DeleteRows(DeleteRowsEvent {
            table_id,
            included_columns,
            rows,
            ..
        }) => {
            if let Some(tm) = table_map.get(&table_id) {
                if !allow.matchs(&tm.database_name, &tm.table_name) {
                    return Ok(None);
                }
                let cols = schema_cache
                    .column_names(&tm.database_name, &tm.table_name)
                    .await?;
                debug!(
                    source_id = %source_id,
                    db = %tm.database_name,
                    table = %tm.table_name,
                    rows = rows.len(),
                    "delete_rows"
                );
                for row in rows {
                    let before = build_object(
                        &cols,
                        &included_columns,
                        &row.column_values,
                    );
                    let mut ev = Event::new_row(
                        tenant.to_string(),
                        SourceMeta {
                            kind: "mysql".into(),
                            host: host.to_string(),
                            db: tm.database_name.clone(),
                        },
                        format!("{}.{}", tm.database_name, tm.table_name),
                        Op::Delete,
                        Some(before),
                        None,
                        ts_ms(header.timestamp),
                    );
                    ev.tx_id = last_gtid.clone();
                    let _ = tx.send(ev).await;
                }
            } else {
                warn!(source_id = %source_id, table_id, "delete_rows for unknown table_id");
            }
        }

        EventData::Query(q) => {
            let db = if q.schema.is_empty() {
                default_db
            } else {
                &q.schema
            };
            let short = short_sql(&q.query, 200);
            info!(
                source_id = %source_id,
                db = %db,
                sql = %short,
                "DDL/Query event"
            );

            // crude DDL – invalidate cache for the DB
            let lower = q.query.to_lowercase();
            if lower.starts_with("create table")
                || lower.starts_with("alter table")
                || lower.starts_with("drop table")
                || lower.starts_with("rename table")
            {
                schema_cache.invalidate(db, "*").await;
            }

            let mut ev = Event::new_ddl(
                tenant.to_string(),
                SourceMeta {
                    kind: "mysql".into(),
                    host: host.to_string(),
                    db: db.to_string(),
                },
                db.to_string(),
                json!({ "sql": q.query }),
                ts_ms(header.timestamp),
            );
            ev.tx_id = last_gtid.clone();
            let _ = tx.send(ev).await;
        }

        EventData::Gtid(gt) => {
            debug!(source_id = %source_id, gtid = %gt.gtid, "gtid");
            *last_gtid = Some(gt.gtid.clone());
        }

        EventData::Rotate(rot) => {
            info!(source_id = %source_id, file = %rot.binlog_filename, pos = rot.binlog_position, "rotate");
            *last_file = rot.binlog_filename.clone();
        }

        EventData::Xid(_) => {
            // persist at commit boundaries
            return Ok(Some(last_pos));
        }

        EventData::FormatDescription(_)
        | EventData::PreviousGtids(_)
        | EventData::RowsQuery(_)
        | EventData::TransactionPayload(_)
        | EventData::XaPrepare(_)
        | EventData::HeartBeat
        | EventData::NotSupported => { /* ignore */ }

        _ => {
            debug!(source_id = %source_id, "unhandled event variant");
        }
    }

    Ok(None)
}

// ----------------------------- private helpers / utilities -----------------------------

/// Build a JSON object using the included-columns bitmap and the compact values vector.
pub(super) fn build_object(
    cols: &Arc<Vec<String>>,
    included: &Vec<bool>,
    values: &Vec<ColumnValue>,
) -> Value {
    let mut obj = serde_json::Map::with_capacity(cols.len());
    let mut vi = 0usize;

    debug!(cols = ?cols, values = ?values);

    for (idx, inc) in included.iter().enumerate() {
        if !*inc {
            continue;
        }
        let key = cols.get(idx).map(|s| s.as_str()).unwrap_or("col");
        let v = values.get(vi).unwrap_or(&ColumnValue::None);
        vi += 1;
        let jv = match v {
            ColumnValue::None => Value::Null,
            ColumnValue::Tiny(x) => json!(*x),
            ColumnValue::Short(x) => json!(*x),
            ColumnValue::Long(x) => json!(*x),
            ColumnValue::LongLong(x) => json!(*x),
            ColumnValue::Float(x) => json!(*x),
            ColumnValue::Double(x) => json!(*x),
            ColumnValue::Decimal(s) => json!(s),
            ColumnValue::Time(s) => json!(s),
            ColumnValue::Date(s) => json!(s),
            ColumnValue::DateTime(s) => json!(s),
            ColumnValue::Timestamp(ts) => json!(ts),
            ColumnValue::Year(y) => json!(y),
            ColumnValue::Bit(b) => json!(b),
            ColumnValue::Set(v) => json!(v),
            ColumnValue::Enum(v) => json!(v),
            ColumnValue::String(bytes) => match std::str::from_utf8(bytes) {
                Ok(s) => json!(s),
                Err(_) => json!({ "_base64": to_b64(bytes) }),
            },
            ColumnValue::Blob(bytes) => json!({ "_base64": to_b64(bytes) }),
            ColumnValue::Json(bytes) => handle_json(bytes),
        };
        obj.insert(key.to_string(), jv);
    }
    Value::Object(obj)
}

fn parse_mysql_jsonb(bytes: &[u8]) -> Option<Value> {
    // Parse MySQL binary JSON (JSONB)
    let mut pb = ParseBuf::new(bytes);
    let v = jsonb::Value::deserialize((), &mut pb).ok()?;
    let dom = v.parse().ok()?;
    Some(dom.into())
}

fn handle_json(bytes: &[u8]) -> Value {
    if let Some(v) = parse_mysql_jsonb(bytes) {
        v
    } else if let Ok(s) = std::str::from_utf8(bytes) {
        // Fallback: sometimes sources hand textual JSON
        serde_json::from_str::<Value>(s).unwrap_or_else(|_| json!(s))
    } else {
        // Last resort: base64-wrap
        json!({ "_base64_json": to_b64(bytes) })
    }
}

fn to_b64(bytes: &Vec<u8>) -> String {
    BASE64_STANDARD.encode(bytes)
}

fn ts_ms(ts_sec: u32) -> i64 {
    (ts_sec as i64) * 1000
}

/// Redact password from DSN for logging (used by prepare_client).
fn redact_password(dsn: &str) -> String {
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
pub(super) async fn resolve_binlog_tail(dsn: &str) -> Result<(String, u64)> {
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
            return Err(anyhow!("MySQL connection failed: {}", e));
        }
        Err(_) => {
            error!("MySQL connection timed out after 10 seconds");
            return Err(anyhow!("MySQL connection timeout"));
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
            return Err(anyhow!("failed to verify user: {}", e));
        }
    }

    // Preferred status command
    info!("trying SHOW BINARY LOG STATUS");
    match tokio::time::timeout(std::time::Duration::from_secs(5), conn.query_first::<Row, _>("SHOW BINARY LOG STATUS")).await {
        Ok(Ok(Some(mut row))) => {
            if let (Some(file), Some(pos)) = (row.take::<String, _>(0), row.take::<u64, _>(1)) {
                info!("successfully got binlog position: file={}, pos={}", file, pos);
                return Ok((file, pos));
            } else {
                warn!("SHOW BINARY LOG STATUS returned incomplete data");
            }
        }
        Ok(Ok(None)) => warn!("SHOW BINARY LOG STATUS returned no rows - binlog might be disabled"),
        Ok(Err(e)) => warn!("SHOW BINARY LOG STATUS failed: {} - trying legacy syntax", e),
        Err(_) => warn!("SHOW BINARY LOG STATUS timed out - trying legacy syntax"),
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
                info!("successfully got binlog position via legacy command: file={}, pos={}", file, pos);
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

    anyhow::bail!(
        "Failed to resolve binlog position. Common fixes:\n\
         1) Enable binary logging: --log-bin --binlog-format=ROW\n\
         2) Grant privileges: GRANT REPLICATION REPLICA, REPLICATION CLIENT ON *.* TO 'df'@'%';\n\
         3) Ensure user exists: CREATE USER 'df'@'%' IDENTIFIED BY 'password';"
    );
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

/// INFORMATION_SCHEMA column-name cache, with logging and latency measurement.
#[derive(Clone)]
pub(super) struct MySqlSchemaCache {
    pool: Pool,
    map: Arc<RwLock<HashMap<(String, String), Arc<Vec<String>>>>>,
}
impl MySqlSchemaCache {
    pub(super) fn new(dsn: &str) -> Self {
        Self {
            pool: Pool::new(dsn),
            map: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    pub(super) async fn column_names(
        &self,
        db: &str,
        table: &str,
    ) -> Result<Arc<Vec<String>>> {
        if let Some(found) = self
            .map
            .read()
            .await
            .get(&(db.to_string(), table.to_string()))
            .cloned()
        {
            debug!(db = %db, table = %table, "schema cache hit");
            return Ok(found);
        }
        let t0 = Instant::now();
        let mut conn = self.pool.get_conn().await?;
        let rows: Vec<Row> = conn
            .exec(
                r#"
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
            "#,
                (db, table),
            )
            .await?;
        let ms = t0.elapsed().as_millis() as u64;
        if ms > 200 {
            warn!(db = %db, table = %table, ms, "slow schema fetch");
        } else {
            debug!(db = %db, table = %table, ms, "schema fetched");
        }
        let cols: Vec<String> = rows
            .into_iter()
            .map(|mut r| r.take::<String, _>(0).unwrap())
            .collect();
        if cols.is_empty() {
            warn!(db = %db, table = %table, "schema fetch returned 0 columns");
        }
        let arc = Arc::new(cols);
        self.map
            .write()
            .await
            .insert((db.to_string(), table.to_string()), arc.clone());
        Ok(arc)
    }
    pub(super) async fn invalidate(&self, db: &str, _wild: &str) {
        let before = self.map.read().await.len();
        self.map.write().await.retain(|(d, _), _| d != db);
        let after = self.map.read().await.len();
        info!(db = %db, removed = before.saturating_sub(after), "schema cache invalidated");
    }
}

/// Shorten SQL for logs
pub(super) fn short_sql(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max])
    }
}
