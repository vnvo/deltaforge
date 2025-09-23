use anyhow::{Context, Result};
use async_trait::async_trait;
use crc32fast::Hasher;
use mysql_async::{prelude::Queryable, Pool, Row};
use mysql_binlog_connector_rust::{
    binlog_client::BinlogClient,
    binlog_stream::BinlogStream,
    column::column_value::ColumnValue,
    event::{
        delete_rows_event::DeleteRowsEvent, event_data::EventData,
        table_map_event::TableMapEvent, update_rows_event::UpdateRowsEvent,
        write_rows_event::WriteRowsEvent,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{collections::HashMap, fmt, sync::Arc, time::Instant};
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};
use url::Url;
use base64::prelude::*;

use deltaforge_checkpoints::{CheckpointStore, CheckpointStoreExt};
use deltaforge_core::{Event, Op, Source, SourceMeta};

#[derive(Debug, Clone)]
pub struct MySqlSource {
    pub id: String,          // checkpoint key + server_id seed
    pub dsn: String,         // mysql://user:pass@host:3306/db
    pub tables: Vec<String>, // ["db.table"]; empty = all
    pub tenant: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MySqlCheckpoint {
    pub file: String,
    pub pos: u64,
    pub gtid_set: Option<String>,
}

impl fmt::Display for MySqlCheckpoint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "[file:{}, pos:{}, gtid_set:{}]",
            self.file,
            self.pos,
            self.gtid_set.as_ref().map_or("", |v| v)
        )
    }
}

#[async_trait]
impl Source for MySqlSource {
    async fn run(
        &self,
        tx: mpsc::Sender<Event>,
        checkpoint_store: Arc<dyn CheckpointStore>,
    ) -> Result<()> {
        let url = Url::parse(&self.dsn).context("invalid MySQL DSN")?;
        let host = url.host_str().unwrap_or("localhost").to_string();
        let default_db = url.path().trim_start_matches('/').to_string();
        let server_id = derive_server_id(&self.id);

        // load the previus checkpoint, if any.
        let last_checkpoint: Option<MySqlCheckpoint> =
            checkpoint_store.get(&self.id).await?;

        info!(
            source_id = %self.id,
            %host,
            db = %default_db,
            server_id,
            //allow = %if self.tables.is_empty() { "<all>" } else { self.tables.join(",") },
            "MySQL CDC: starting source"
        );

        let mut client = BinlogClient::default();
        client.url = self.dsn.clone();
        client.server_id = server_id;
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
                info!(source_id = %self.id, %gtid, "resuming via GTID");
            } else {
                client.gtid_enabled = false;
                client.binlog_filename = p.file.clone();
                client.binlog_position = p.pos as u32;
                info!(
                    source_id = %self.id,
                    file = %p.file,
                    pos = p.pos,
                    "resuming via file:pos"
                );
            }
        } else {
            info!("no previous checkpoint, reading the binlog tail ..");
            match resolve_binlog_tail(&self.dsn).await {
                Ok((file, pos)) => {
                    info!(source_id = %self.id, %file, %pos, "start from end (first run)");
                    client.binlog_filename = file.clone();
                    client.binlog_position = pos as u32;
                }
                Err(e) => {
                    error!(source_id = %self.id, error = %e, "failed to resolve binlog tail");
                    return Err(e);
                }
            }
        }

        // Connect
        debug!("connecting to binlog");
        let t0 = Instant::now();
        let mut stream: BinlogStream = match tokio::time::timeout(
            std::time::Duration::from_secs(30),
            client.connect(),
        )
        .await
        {
            Ok(Ok(s)) => {
                info!(
                    source_id = %self.id,
                    ms = t0.elapsed().as_millis() as u64,
                    "connected to binlog"
                );
                s
            }
            Ok(Err(e)) => {
                error!(source_id = %self.id, error = %e, "binlog connect failed");

                // Add helpful context about the specific error
                let error_msg = format!("{}", e);
                if error_msg.contains("mysql_native_password") {
                    error!(
                        source_id = %self.id,
                        "MySQL authentication issue detected. This usually means:"
                    );
                    error!("1. The user doesn't exist: CREATE USER 'df'@'%' IDENTIFIED BY 'dfpw';");
                    error!("2. Missing replication privileges: GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'df'@'%';");
                    error!("3. Missing database privileges: GRANT ALL PRIVILEGES ON {}.* TO 'df'@'%';", default_db);
                    error!("4. Privileges not applied: FLUSH PRIVILEGES;");
                } else if error_msg.contains("Access denied") {
                    error!(
                        source_id = %self.id,
                        "Access denied - check username/password and privileges"
                    );
                } else if error_msg.contains("connect")
                    || error_msg.contains("timeout")
                {
                    error!(
                        source_id = %self.id,
                        "Connection failed - check that MySQL is running and accessible"
                    );
                } else {
                    error!(
                        source_id = %self.id,
                        "Unknown binlog connection error. Full error: {}", error_msg
                    );
                }
                return Err(e.into());
            }
            Err(_) => {
                error!(
                    source_id = %self.id,
                    "binlog connection timed out after 30 seconds"
                );
                return Err(anyhow::anyhow!("binlog connection timeout"));
            }
        };

        // Schema cache + allow-list
        let schema_cache = MySqlSchemaCache::new(&self.dsn);
        let allow = AllowList::new(&self.tables);

        // Binlog state
        let mut table_map: HashMap<u64, TableMapEvent> = HashMap::new();
        let mut last_file = String::new();
        let mut last_gtid: Option<String> = None;

        // Stream loop
        info!("entering binlog read loop");
        loop {
            let (header, data) = match stream.read().await {
                Ok(x) => x,
                Err(e) => {
                    error!(source_id = %self.id, error = %e, "binlog read error");
                    return Err(e.into());
                }
            };
            let last_pos = header.next_event_position as u64;

            match data {
                EventData::TableMap(tm) => {
                    table_map.insert(tm.table_id, tm.clone());

                    if allow.matchs(&tm.database_name, &tm.table_name) {
                        let is_new = !table_map.contains_key(&tm.table_id);
                        if is_new {
                            info!(
                                source_id = %self.id,
                                table_id = tm.table_id,
                                db = %tm.database_name,
                                table = %tm.table_name,
                                "table mapped"
                            );
                        } else {
                            debug!(
                                source_id = %self.id,
                                table_id = tm.table_id,
                                db = %tm.database_name,
                                table = %tm.table_name,
                                "table remapped"
                            );
                        }
                    } else {
                        debug!(
                            source_id = %self.id,
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
                            continue;
                        }
                        let cols = schema_cache
                            .column_names(&tm.database_name, &tm.table_name)
                            .await?;
                        debug!(
                            source_id = %self.id,
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
                                self.tenant.clone(),
                                SourceMeta {
                                    kind: "mysql".into(),
                                    host: host.clone(),
                                    db: tm.database_name.clone(),
                                },
                                format!(
                                    "{}.{}",
                                    tm.database_name, tm.table_name
                                ),
                                Op::Insert,
                                None,
                                Some(after),
                                (header.timestamp as i64) * 1000,
                            );
                            ev.tx_id = last_gtid.clone();
                            if let Err(e) = tx.send(ev).await {
                                error!(source_id = %self.id, error = %e, "channel send failed (insert)");
                            }
                        }
                    } else {
                        warn!(source_id = %self.id, table_id, "write_rows for unknown table_id");
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
                            continue;
                        }
                        let cols = schema_cache
                            .column_names(&tm.database_name, &tm.table_name)
                            .await?;
                        debug!(
                            source_id = %self.id,
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
                                self.tenant.clone(),
                                SourceMeta {
                                    kind: "mysql".into(),
                                    host: host.clone(),
                                    db: tm.database_name.clone(),
                                },
                                format!(
                                    "{}.{}",
                                    tm.database_name, tm.table_name
                                ),
                                Op::Update,
                                Some(before),
                                Some(after),
                                (header.timestamp as i64) * 1000,
                            );
                            ev.tx_id = last_gtid.clone();
                            if let Err(e) = tx.send(ev).await {
                                error!(source_id = %self.id, error = %e, "channel send failed (update)");
                            }
                        }
                    } else {
                        warn!(source_id = %self.id, table_id, "update_rows for unknown table_id");
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
                            continue;
                        }
                        let cols = schema_cache
                            .column_names(&tm.database_name, &tm.table_name)
                            .await?;
                        debug!(
                            source_id = %self.id,
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
                                self.tenant.clone(),
                                SourceMeta {
                                    kind: "mysql".into(),
                                    host: host.clone(),
                                    db: tm.database_name.clone(),
                                },
                                format!(
                                    "{}.{}",
                                    tm.database_name, tm.table_name
                                ),
                                Op::Delete,
                                Some(before),
                                None,
                                (header.timestamp as i64) * 1000,
                            );
                            ev.tx_id = last_gtid.clone();
                            if let Err(e) = tx.send(ev).await {
                                error!(source_id = %self.id, error = %e, "channel send failed (delete)");
                            }
                        }
                    } else {
                        warn!(source_id = %self.id, table_id, "delete_rows for unknown table_id");
                    }
                }

                EventData::Query(q) => {
                    let db = if q.schema.is_empty() {
                        &default_db
                    } else {
                        &q.schema
                    };
                    let short = short_sql(&q.query, 200);
                    info!(
                        source_id = %self.id,
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
                        self.tenant.clone(),
                        SourceMeta {
                            kind: "mysql".into(),
                            host: host.clone(),
                            db: db.to_string(),
                        },
                        db.to_string(),
                        json!({ "sql": q.query }),
                        (header.timestamp as i64) * 1000,
                    );
                    ev.tx_id = last_gtid.clone();
                    if let Err(e) = tx.send(ev).await {
                        error!(source_id = %self.id, error = %e, "channel send failed (ddl)");
                    }
                }

                EventData::Gtid(gt) => {
                    debug!(source_id = %self.id, gtid = %gt.gtid, "gtid");
                    last_gtid = Some(gt.gtid.clone());
                }

                EventData::Rotate(rot) => {
                    info!(source_id = %self.id, file = %rot.binlog_filename, pos = rot.binlog_position, "rotate");
                    last_file = rot.binlog_filename.clone();
                }

                EventData::Xid(_x) => {
                    // persist at commit boundaries
                    if let Err(e) = checkpoint_store
                        .put(
                            &self.id,
                            MySqlCheckpoint {
                                file: last_file.clone(),
                                pos: last_pos,
                                gtid_set: last_gtid.clone(),
                            },
                        )
                        .await
                    {
                        error!(source_id = %self.id, error = %e, "failed to persist checkpoint");
                    } else {
                        debug!(source_id = %self.id, file = %last_file, pos = last_pos, gtid = ?last_gtid, "checkpoint saved");
                    }
                }

                EventData::FormatDescription(_)
                | EventData::PreviousGtids(_)
                | EventData::RowsQuery(_)
                | EventData::TransactionPayload(_)
                | EventData::XaPrepare(_)
                | EventData::HeartBeat
                | EventData::NotSupported => { /* ignore */ }

                _ => {
                    debug!(source_id = %self.id, "unhandled event variant");
                }
            }
        }
    }
}

/// Redact password from DSN for logging
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

/// Build a JSON object using the included-columns bitmap and the compact values vector.
fn build_object(
    cols: &Arc<Vec<String>>,
    included: &Vec<bool>,
    values: &Vec<ColumnValue>,
) -> Value {
    let mut obj = serde_json::Map::with_capacity(cols.len());
    let mut vi = 0usize;
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
            ColumnValue::Blob(bytes) => {
                json!({ "_base64": to_b64(bytes) })
            }
            ColumnValue::Json(bytes) => {
                if let Ok(s) = std::str::from_utf8(bytes) {
                    serde_json::from_str::<Value>(s)
                        .unwrap_or_else(|_| json!(s))
                } else {
                    json!({ "_base64_json": to_b64(bytes) })
                }
            }
        };
        obj.insert(key.to_string(), jv);
    }
    Value::Object(obj) 
}

fn to_b64(bytes: &Vec<u8>) -> String {
    BASE64_STANDARD.encode(bytes)
}

fn derive_server_id(id: &str) -> u64 {
    let mut h = Hasher::new();
    h.update(id.as_bytes());
    1 + ((h.finalize() as u64) % 4_000_000_000u64)
}

/// Resolve end-of-binlog with comprehensive error handling and diagnostics
async fn resolve_binlog_tail(dsn: &str) -> Result<(String, u64)> {
    info!("connecting to MySQL for binlog tail resolution");

    let pool = Pool::new(dsn);

    // Test connection with timeout
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
            return Err(anyhow::anyhow!("MySQL connection failed: {}", e));
        }
        Err(_) => {
            error!("MySQL connection timed out after 10 seconds");
            return Err(anyhow::anyhow!("MySQL connection timeout"));
        }
    };

    // Check user privileges first
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
            return Err(anyhow::anyhow!("failed to verify user: {}", e));
        }
    }

    // Try modern syntax first (MySQL 8.0.22+)
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
                //pool.disconnect().await?;
                //debug!("pool disconneted");
                return Ok((file, pos));
            } else {
                warn!("SHOW BINARY LOG STATUS returned incomplete data");
            }
        }
        Ok(Ok(None)) => {
            warn!("SHOW BINARY LOG STATUS returned no rows - binlog might be disabled");
        }
        Ok(Err(e)) => {
            warn!(
                "SHOW BINARY LOG STATUS failed: {} - trying legacy syntax",
                e
            );
        }
        Err(_) => {
            warn!("SHOW BINARY LOG STATUS timed out - trying legacy syntax");
        }
    }

    // Try legacy syntax (deprecated but might work)
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
        Ok(Ok(None)) => {
            warn!("SHOW MASTER STATUS returned no rows");
        }
        Ok(Err(e)) => {
            warn!("SHOW MASTER STATUS failed: {}", e);
        }
        Err(_) => {
            warn!("SHOW MASTER STATUS timed out");
        }
    }

    // Comprehensive diagnostics
    info!("running diagnostics to determine the issue");
    let mut diagnostics = Vec::new();

    // Check if binary logging is enabled
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

    // Check binlog format
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

    // Check user grants
    match conn.query::<Row, _>("SHOW GRANTS").await {
        Ok(rows) => {
            let grants: Vec<String> = rows
                .into_iter()
                .map(|mut row| row.take::<String, _>(0).unwrap_or_default())
                .collect();

            let has_replication = grants.iter().any(|g| {
                g.contains("REPLICATION SLAVE") || g.contains("ALL PRIVILEGES")
            });

            if has_replication {
                diagnostics
                    .push("OK: User has replication privileges".to_string());
            } else {
                diagnostics.push(
                    "FAIL: User lacks REPLICATION SLAVE privilege".to_string(),
                );
                diagnostics.push("   Fix: GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO user;".to_string());
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

    // Check MySQL version
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

    // Print diagnostics
    error!("could not resolve binlog end position. Diagnostics:");
    for diag in &diagnostics {
        error!("  {}", diag);
    }

    anyhow::bail!(
        "Failed to resolve binlog position. Check the diagnostics above. Common fixes:\n\
         1. Enable binary logging: --log-bin --binlog-format=ROW\n\
         2. Grant privileges: GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'df'@'%';\n\
         3. Ensure user exists: CREATE USER 'df'@'%' IDENTIFIED BY 'password';"
    );
}

/// Simple allow-list on "db.table" or just "table"
#[derive(Clone)]
struct AllowList {
    items: Vec<(Option<String>, String)>,
}
impl AllowList {
    fn new(list: &[String]) -> Self {
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
    fn matchs(&self, db: &str, table: &str) -> bool {
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
struct MySqlSchemaCache {
    pool: Pool,
    map: Arc<RwLock<HashMap<(String, String), Arc<Vec<String>>>>>,
}
impl MySqlSchemaCache {
    fn new(dsn: &str) -> Self {
        Self {
            pool: Pool::new(dsn),
            map: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    async fn column_names(
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
    async fn invalidate(&self, db: &str, _wild: &str) {
        let before = self.map.read().await.len();
        self.map.write().await.retain(|(d, _), _| d != db);
        let after = self.map.read().await.len();
        info!(db = %db, removed = before.saturating_sub(after), "schema cache invalidated");
    }
}

/// Shorten SQL for logs
fn short_sql(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max])
    }
}
