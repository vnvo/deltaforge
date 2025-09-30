//! MySQL source module.
//! Uses binlog steaming to consume the change events
//! and send them to the next layer (processors)

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Instant,
};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use mysql_binlog_connector_rust::{
    binlog_stream::BinlogStream,
    event::{event_data::EventData, table_map_event::TableMapEvent},
};
use serde::{Deserialize, Serialize};
use tokio::select;
use tokio::sync::{mpsc, Notify};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use url::Url;

use deltaforge_checkpoints::{CheckpointStore, CheckpointStoreExt};
use deltaforge_core::{Event, Op, Source, SourceHandle, SourceMeta};

mod mysql_helpers;
use mysql_helpers::{
    build_object, connect_binlog, pause_until_resumed, persist_checkpoint,
    prepare_client, short_sql, ts_ms, AllowList,
};

mod mysql_schema;
use mysql_schema::MySqlSchemaCache;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MySqlCheckpoint {
    pub file: String,
    pub pos: u64,
    pub gtid_set: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MySqlSource {
    pub id: String,          // checkpoint key + server_id seed
    pub dsn: String,         // mysql://user:pass@host:3306/db
    pub tables: Vec<String>, // ["db.table"]; empty = all
    pub tenant: String,
}

impl MySqlSource {
    /// Main processing loop
    async fn run_inner(
        &self,
        tx: mpsc::Sender<Event>,
        checkpoint_store: Arc<dyn CheckpointStore>,
        cancel: CancellationToken,
        paused: Arc<AtomicBool>,
        pause_notify: Arc<Notify>,
    ) -> Result<()> {
        let (host, default_db, _server_id, client) = prepare_client(
            &self.dsn,
            &self.id,
            &self.tables,
            &checkpoint_store,
        )
        .await?;

        info!(
            source_id = %self.id,
            host = %host,
            db = %default_db,
            "MySQL CDC: starting source"
        );

        let mut stream: BinlogStream =
            connect_binlog(&self.id, client, &cancel, &default_db).await?;

        let schema_cache = MySqlSchemaCache::new(&self.dsn);
        let allow = AllowList::new(&self.tables);

        let mut table_map: HashMap<u64, TableMapEvent> = HashMap::new();
        let mut last_file = String::new();
        let mut last_gtid: Option<String> = None;

        info!("entering binlog read loop");
        loop {
            // pause gate, binlog client stays connected.
            if !pause_until_resumed(&cancel, &*paused, &*pause_notify).await {
                break;
            }

            // read or cancel - stop the operation completely on cancel
            let next = select! {
                _ = cancel.cancelled() => break,
                r = stream.read() => r,
            };

            let (header, data) = match next {
                Ok(x) => x,
                Err(e) => {
                    if cancel.is_cancelled() {
                        break;
                    }
                    error!(source_id = %self.id, error = %e, "binlog read error");
                    return Err(e.into());
                }
            };

            let last_pos = header.next_event_position as u64;
            debug!(
                timestamp = header.timestamp,
                event_type = header.event_type,
                event_length = header.event_length,
                server_id = header.server_id,
                next_event_pos = last_pos,
                "mysql source, event received"
            );

            // actual event processing block
            match data {
                EventData::TableMap(tm) => {
                    let is_new = !table_map.contains_key(&tm.table_id);
                    table_map.insert(tm.table_id, tm.clone());

                    if allow.matchs(&tm.database_name, &tm.table_name) {
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
                                "table re-mapped"
                            );
                        }
                    } else {
                        debug!(
                            source_id = %self.id,
                            db = %tm.database_name,
                            table = %tm.table_name,
                            "skipping table (not in allow-list)"
                        );
                    }
                }

                EventData::WriteRows(wr) => {
                    if let Some(tm) = table_map.get(&wr.table_id) {
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
                            rows = wr.rows.len(),
                            "write_rows"
                        );

                        for row in wr.rows {
                            let after = build_object(
                                &cols,
                                &wr.included_columns,
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
                                ts_ms(header.timestamp),
                            );

                            ev.tx_id = last_gtid.clone();
                            if tx.send(ev).await.is_err() {
                                error!(source_id = %self.id, "channel send failed (op=insert)");
                            }
                        }
                    } else {
                        warn!(source_id = %self.id, table_id = wr.table_id, "write_rows for unknown table_id");
                    }
                }

                EventData::UpdateRows(ur) => {
                    if let Some(tm) = table_map.get(&ur.table_id) {
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
                            rows = ur.rows.len(),
                            "update_rows"
                        );

                        for (before_row, after_row) in ur.rows {
                            let before = build_object(
                                &cols,
                                &ur.included_columns_before,
                                &before_row.column_values,
                            );
                            let after = build_object(
                                &cols,
                                &ur.included_columns_after,
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
                                ts_ms(header.timestamp),
                            );

                            ev.tx_id = last_gtid.clone();
                            if tx.send(ev).await.is_err() {
                                error!(source_id = %self.id, "channel send failed (op=update)");
                            }
                        }
                    } else {
                        warn!(source_id = %self.id, table_id = ur.table_id, "update_rows for unknown table_id");
                    }
                }

                EventData::DeleteRows(dr) => {
                    if let Some(tm) = table_map.get(&dr.table_id) {
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
                            rows = dr.rows.len(),
                            "delete_rows"
                        );

                        for row in dr.rows {
                            let before = build_object(
                                &cols,
                                &dr.included_columns,
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
                                ts_ms(header.timestamp),
                            );
                            ev.tx_id = last_gtid.clone();
                            if tx.send(ev).await.is_err() {
                                error!(source_id = %self.id, "channel send failed (op=delete)");
                            }
                        }
                    } else {
                        warn!(source_id = %self.id, table_id = dr.table_id, "delete_rows for unknown table_id");
                    }
                }

                EventData::Query(q) => {
                    let db = if q.schema.is_empty() {
                        &default_db
                    } else {
                        &q.schema
                    };
                    let short = short_sql(&q.query, 200);
                    info!(source_id = %self.id, db = %db, sql = %short, "DDL/Query event");

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
                        serde_json::json!({ "sql": q.query }),
                        ts_ms(header.timestamp),
                    );

                    ev.tx_id = last_gtid.clone();
                    if tx.send(ev).await.is_err() {
                        error!(source_id = %self.id, "channel send failed (op=ddl)");
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

                EventData::Xid(_) => {
                    // definitely persist at commit boundaries
                    persist_checkpoint(
                        &self.id,
                        &checkpoint_store,
                        &last_file,
                        last_pos,
                        &last_gtid,
                    )
                    .await;
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

        // Optional best-effort final checkpoint (no pos if we didn't track one)
        let _ = checkpoint_store
            .put(
                &self.id,
                MySqlCheckpoint {
                    file: last_file,
                    pos: 0,
                    gtid_set: last_gtid,
                },
            )
            .await;

        Ok(())
    }
}

#[async_trait]
impl Source for MySqlSource {
    /// start the source in a background task and return a control handle.
    /// outside callers have to use this to start a CDC source.
    /// see `SourceHandler` for available controls returned by run.
    async fn run(
        &self,
        tx: mpsc::Sender<Event>,
        chkpt_store: Arc<dyn CheckpointStore>,
    ) -> SourceHandle {
        let cancel = CancellationToken::new();
        let paused = Arc::new(AtomicBool::new(false));
        let pause_notify = Arc::new(Notify::new());

        let this = self.clone();
        let cancel_for_task = cancel.clone();
        let paused_for_task = paused.clone();
        let pause_notify_for_task = pause_notify.clone();

        let join = tokio::spawn(async move {
            let res = this
                .run_inner(
                    tx,
                    chkpt_store,
                    cancel_for_task,
                    paused_for_task,
                    pause_notify_for_task,
                )
                .await;
            if let Err(e) = &res {
                error!(error=?e, "mysql source task ended with error");
            }
            res
        });

        SourceHandle {
            cancel,
            paused,
            pause_notify,
            join,
        }
    }
}
