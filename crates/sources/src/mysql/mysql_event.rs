use deltaforge_core::{Event, Op, SourceError, SourceMeta, SourceResult};
use metrics::counter;
use mysql_binlog_connector_rust::{
    binlog_stream::BinlogStream,
    event::{
        event_data::EventData, event_header::EventHeader,
        table_map_event::TableMapEvent,
    },
};
use tracing::instrument;
use tracing::{debug, error, info, warn};

use super::mysql_object::build_object;
use crate::conn_utils::{retryable_stream, watchdog};
use crate::mysql::RunCtx;

use crate::mysql::mysql_helpers::{make_checkpoint_meta, short_sql, ts_ms};

pub(super) enum LoopControl {
    Reconnect,
    Stop,
    Fail(SourceError),
}

#[instrument(skip_all)]
pub(super) async fn read_next_event(
    stream: &mut BinlogStream,
    ctx: &RunCtx,
) -> Result<(EventHeader, EventData), LoopControl> {
    match watchdog(stream.read(), ctx.inactivity, &ctx.cancel, "binlog_read")
        .await
    {
        Ok(x) => Ok(x),
        Err(se) => {
            if ctx.cancel.is_cancelled() {
                return Err(LoopControl::Stop);
            }
            if retryable_stream(&se) {
                counter!(
                    "deltaforge_source_reconnects_total",
                    "pipeline" => ctx.pipeline.clone(),
                    "source" => ctx.source_id.clone(),
                )
                .increment(1);
                warn!(source_id=%ctx.source_id, error=%se, "binlog read failed; scheduling reconnect");
                Err(LoopControl::Reconnect)
            } else {
                error!(source_id=%ctx.source_id, error=%se, "non-retryable binlog read error");
                Err(LoopControl::Fail(se))
            }
        }
    }
}

#[instrument(skip_all)]
pub(super) async fn dispatch_event(
    ctx: &mut RunCtx,
    header: &EventHeader,
    data: EventData,
) -> SourceResult<()> {
    debug!(
        timestamp = header.timestamp,
        event_type = header.event_type,
        event_length = header.event_length,
        server_id = header.server_id,
        next_event_pos = ctx.last_pos,
        "event received"
    );

    match data {
        EventData::TableMap(tm) => handle_table_map(ctx, tm).await,
        EventData::WriteRows(wr) => handle_write_rows(ctx, header, wr).await,
        EventData::UpdateRows(ur) => handle_update_rows(ctx, header, ur).await,
        EventData::DeleteRows(dr) => handle_delete_rows(ctx, header, dr).await,
        EventData::Query(q) => handle_query(ctx, header, q).await,
        EventData::Gtid(gt) => {
            handle_gtid(ctx, gt);
            Ok(())
        }
        EventData::Rotate(rot) => {
            handle_rotate(ctx, rot);
            Ok(())
        }
        EventData::Xid(_) => {
            handle_xid(ctx).await;
            Ok(())
        }

        EventData::FormatDescription(_)
        | EventData::PreviousGtids(_)
        | EventData::RowsQuery(_)
        | EventData::TransactionPayload(_)
        | EventData::XaPrepare(_)
        | EventData::HeartBeat
        | EventData::NotSupported => Ok(()),
    }
}

#[instrument(skip_all, fields(source_id=%ctx.source_id))]
async fn handle_table_map(
    ctx: &mut RunCtx,
    tm: TableMapEvent,
) -> SourceResult<()> {
    let is_new = !ctx.table_map.contains_key(&tm.table_id);
    ctx.table_map.insert(tm.table_id, tm.clone());

    if ctx.allow.matchs(&tm.database_name, &tm.table_name) {
        if is_new {
            info!(
                table_id=tm.table_id,
                db=%tm.database_name,
                table=%tm.table_name,
                "table mapped");
        } else {
            debug!(
                table_id=tm.table_id,
                db=%tm.database_name,
                table=%tm.table_name,
                "table re-mapped");
        }
    } else {
        debug!(
            db=%tm.database_name, 
            table=%tm.table_name, 
            "skipping table (not in allow-list)");
    }
    Ok(())
}

#[instrument(skip_all)]
async fn handle_write_rows(
    ctx: &mut RunCtx,
    header: &EventHeader,
    wr: mysql_binlog_connector_rust::event::write_rows_event::WriteRowsEvent,
) -> SourceResult<()> {
    if let Some(tm) = ctx.table_map.get(&wr.table_id) {
        if !ctx.allow.matchs(&tm.database_name, &tm.table_name) {
            return Ok(());
        }
        let cols = ctx
            .schema
            .column_names(&tm.database_name, &tm.table_name)
            .await?;
        debug!(source_id=%ctx.source_id, db=%tm.database_name, table=%tm.table_name, rows=wr.rows.len(), "write_rows");

        for row in wr.rows {
            let after =
                build_object(&cols, &wr.included_columns, &row.column_values);
            let mut ev = Event::new_row(
                ctx.tenant.clone(),
                SourceMeta {
                    kind: "mysql".into(),
                    host: ctx.host.clone(),
                    db: tm.database_name.clone(),
                },
                format!("{}.{}", tm.database_name, tm.table_name),
                Op::Insert,
                None,
                Some(after),
                ts_ms(header.timestamp),
                header.event_length as usize,
            );
            ev.tx_id = ctx.last_gtid.clone();
            ev.checkpoint = Some(make_checkpoint_meta(
                &ctx.last_file,
                ctx.last_pos,
                &ctx.last_gtid,
            ));
            ev.schema_sequence = Some(ctx.schema.current_sequence());

            let table = format!("{}.{}", tm.database_name, tm.table_name);
            match ctx.tx.send(ev).await {
                Ok(_) => {
                    counter!(
                        "deltaforge_source_events_total",
                        "pipeline" => ctx.pipeline.clone(),
                        "source" => ctx.source_id.clone(),
                        "table" => table,
                    )
                    .increment(1);
                }
                Err(_) => {
                    error!(source_id=%ctx.source_id, "channel send failed (op=insert)");
                }
            }
        }
    } else {
        warn!(source_id=%ctx.source_id, table_id=wr.table_id, "write_rows for unknown table_id");
    }
    Ok(())
}

#[instrument(skip_all)]
async fn handle_update_rows(
    ctx: &mut RunCtx,
    header: &EventHeader,
    ur: mysql_binlog_connector_rust::event::update_rows_event::UpdateRowsEvent,
) -> SourceResult<()> {
    if let Some(tm) = ctx.table_map.get(&ur.table_id) {
        if !ctx.allow.matchs(&tm.database_name, &tm.table_name) {
            return Ok(());
        }
        let cols = ctx
            .schema
            .column_names(&tm.database_name, &tm.table_name)
            .await?;
        debug!(source_id=%ctx.source_id, db=%tm.database_name, table=%tm.table_name, rows=ur.rows.len(), "update_rows");

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
                ctx.tenant.clone(),
                SourceMeta {
                    kind: "mysql".into(),
                    host: ctx.host.clone(),
                    db: tm.database_name.clone(),
                },
                format!("{}.{}", tm.database_name, tm.table_name),
                Op::Update,
                Some(before),
                Some(after),
                ts_ms(header.timestamp),
                header.event_length as usize,
            );

            ev.tx_id = ctx.last_gtid.clone();
            ev.checkpoint = Some(make_checkpoint_meta(
                &ctx.last_file,
                ctx.last_pos,
                &ctx.last_gtid,
            ));
            ev.schema_sequence = Some(ctx.schema.current_sequence());

            let table = format!("{}.{}", tm.database_name, tm.table_name);
            match ctx.tx.send(ev).await {
                Ok(_) => {
                    counter!(
                        "deltaforge_source_events_total",
                        "pipeline" => ctx.pipeline.clone(),
                        "source" => ctx.source_id.clone(),
                        "table" => table,
                    )
                    .increment(1);
                }
                Err(_) => {
                    error!(source_id=%ctx.source_id, "channel send failed (op=update)");
                }
            }
        }
    } else {
        warn!(source_id=%ctx.source_id, table_id=ur.table_id, "update_rows for unknown table_id");
    }
    Ok(())
}

#[instrument(skip_all)]
async fn handle_delete_rows(
    ctx: &mut RunCtx,
    header: &EventHeader,
    dr: mysql_binlog_connector_rust::event::delete_rows_event::DeleteRowsEvent,
) -> SourceResult<()> {
    if let Some(tm) = ctx.table_map.get(&dr.table_id) {
        if !ctx.allow.matchs(&tm.database_name, &tm.table_name) {
            return Ok(());
        }
        let cols = ctx
            .schema
            .column_names(&tm.database_name, &tm.table_name)
            .await?;
        debug!(source_id=%ctx.source_id, db=%tm.database_name, table=%tm.table_name, rows=dr.rows.len(), "delete_rows");

        for row in dr.rows {
            let before =
                build_object(&cols, &dr.included_columns, &row.column_values);
            let mut ev = Event::new_row(
                ctx.tenant.clone(),
                SourceMeta {
                    kind: "mysql".into(),
                    host: ctx.host.clone(),
                    db: tm.database_name.clone(),
                },
                format!("{}.{}", tm.database_name, tm.table_name),
                Op::Delete,
                Some(before),
                None,
                ts_ms(header.timestamp),
                header.event_length as usize,
            );

            ev.tx_id = ctx.last_gtid.clone();
            ev.checkpoint = Some(make_checkpoint_meta(
                &ctx.last_file,
                ctx.last_pos,
                &ctx.last_gtid,
            ));
            ev.schema_sequence = Some(ctx.schema.current_sequence());

            let table = format!("{}.{}", tm.database_name, tm.table_name);
            match ctx.tx.send(ev).await {
                Ok(_) => {
                    counter!(
                        "deltaforge_source_events_total",
                        "pipeline" => ctx.pipeline.clone(),
                        "source" => ctx.source_id.clone(),
                        "table" => table,
                    )
                    .increment(1);
                }
                Err(_) => {
                    error!(source_id=%ctx.source_id, "channel send failed (op=delete)");
                }
            }
        }
    } else {
        warn!(source_id=%ctx.source_id, table_id=dr.table_id, "delete_rows for unknown table_id");
    }
    Ok(())
}

#[instrument(skip_all)]
async fn handle_query(
    ctx: &mut RunCtx,
    header: &EventHeader,
    q: mysql_binlog_connector_rust::event::query_event::QueryEvent,
) -> SourceResult<()> {
    let db = if q.schema.is_empty() {
        &ctx.default_db
    } else {
        &q.schema
    };
    let short = short_sql(&q.query, 200);
    info!(source_id=%ctx.source_id, db=%db, sql=%short, "DDL/Query event");

    let lower = q.query.to_lowercase();
    if lower.starts_with("create table")
        || lower.starts_with("alter table")
        || lower.starts_with("drop table")
        || lower.starts_with("rename table")
    {
        ctx.schema.invalidate_db(db).await;
    }

    let mut ev = Event::new_ddl(
        ctx.tenant.clone(),
        SourceMeta {
            kind: "mysql".into(),
            host: ctx.host.clone(),
            db: db.to_string(),
        },
        db.to_string(),
        serde_json::json!({ "sql": q.query }),
        ts_ms(header.timestamp),
        header.event_length as usize,
    );
    ev.tx_id = ctx.last_gtid.clone();
    match ctx.tx.send(ev).await {
        Ok(_) => {
            counter!(
                "deltaforge_source_events_total",
                "pipeline" => ctx.pipeline.clone(),
                "source" => ctx.source_id.clone(),
                "table" => db.to_string(),
            )
            .increment(1);
        }
        Err(_) => {
            error!(source_id=%ctx.source_id, "channel send failed (op=ddl)");
        }
    }
    Ok(())
}

#[instrument(skip_all)]
fn handle_gtid(
    ctx: &mut RunCtx,
    gt: mysql_binlog_connector_rust::event::gtid_event::GtidEvent,
) {
    debug!(source_id=%ctx.source_id, gtid=%gt.gtid, "gtid");
    ctx.last_gtid = Some(gt.gtid.clone());
}

#[instrument(skip_all)]
fn handle_rotate(
    ctx: &mut RunCtx,
    rot: mysql_binlog_connector_rust::event::rotate_event::RotateEvent,
) {
    info!(source_id=%ctx.source_id, file=%rot.binlog_filename, pos=rot.binlog_position, "rotate");
    ctx.last_file = rot.binlog_filename.clone();
}

#[instrument(skip_all, fields(source_id=%ctx.source_id))]
async fn handle_xid(ctx: &mut RunCtx) {
    // Prefer the server’s executed-set; fall back to last_gtid if GTID is off
    match crate::mysql::mysql_helpers::fetch_executed_gtid_set(&ctx.dsn).await {
        Ok(Some(s)) => {
            ctx.last_gtid = Some(s);
        }
        Ok(None) => {}
        Err(e) => {
            debug!(source_id=%ctx.source_id, error=%e, "failed to fetch executed GTID set");
        }
    };

    debug!(
        source_id=%ctx.source_id,
        file=%ctx.last_file,
        pos=ctx.last_pos,
        gtid=?ctx.last_gtid,
        "XID: transaction committed (checkpoint in events, will persist after sink ack)"
    );
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, atomic::AtomicBool},
        time::Duration,
    };
    use tokio::sync::{Notify, mpsc};
    use tokio_util::sync::CancellationToken;

    use checkpoints::{CheckpointStore, MemCheckpointStore};
    use mysql_binlog_connector_rust::{
        column::column_value::ColumnValue,
        event::{
            delete_rows_event::DeleteRowsEvent, row_event::RowEvent,
            update_rows_event::UpdateRowsEvent,
            write_rows_event::WriteRowsEvent,
        },
    };

    use crate::{
        conn_utils::RetryPolicy,
        mysql::{AllowList, MySqlCheckpoint, MySqlSchemaLoader},
    };

    use super::*;

    const TABLE_ID: u64 = 42;

    fn make_table_map() -> TableMapEvent {
        TableMapEvent {
            table_id: TABLE_ID,
            database_name: "shop".to_string(),
            table_name: "orders".to_string(),
            column_types: vec![],
            column_metas: vec![],
            null_bits: vec![],
        }
    }

    /// RunCtx suitable for unit-testing the row handlers:
    /// - in-memory checkpoint store
    /// - schema cache pre-populated for shop.orders so no DB is touched
    /// - allow-list matching 'shop.orders' only

    fn make_runctx(tx: mpsc::Sender<Event>) -> RunCtx {
        let mut cols_map = HashMap::new();
        cols_map.insert(
            ("shop".to_string(), "orders".to_string()),
            Arc::new(vec!["id".to_string(), "sku".to_string()]),
        );

        let schema = MySqlSchemaLoader::from_static(cols_map);
        let chkpt: Arc<dyn CheckpointStore> =
            Arc::new(MemCheckpointStore::new().expect("mem checkpoint store"));

        RunCtx {
            source_id: "mysql-unit".to_string(),
            pipeline: "pipe-1".to_string(),
            tenant: "data-corp".to_string(),
            dsn: "mysql://localhost/ignored".to_string(),
            host: "localhost".to_string(),
            default_db: "shop".to_string(),
            server_id: 1,
            tx,
            chkpt,
            cancel: CancellationToken::new(),
            paused: Arc::new(AtomicBool::new(false)),
            pause_notify: Arc::new(Notify::new()),
            schema,
            allow: AllowList::new(&["shop.orders".to_string()]),
            retry: RetryPolicy::default(),
            inactivity: Duration::from_secs(30),
            table_map: {
                let mut m = HashMap::new();
                m.insert(TABLE_ID, make_table_map());
                m
            },
            last_file: "mysql-bin.000001".to_string(),
            last_pos: 4,
            last_gtid: Some("GTID-UNIT".to_string()),
        }
    }

    fn make_header() -> EventHeader {
        EventHeader {
            timestamp: 1_700_000_000,
            event_type: 30,
            server_id: 1,
            event_length: 0,
            next_event_position: 0,
            event_flags: 0,
        }
    }

    #[tokio::test]
    async fn handle_write_rows_mits_insert_with_after_only() {
        let (tx, mut rx) = mpsc::channel::<Event>(8);
        let mut ctx = make_runctx(tx);
        let row = RowEvent {
            column_values: vec![
                ColumnValue::LongLong(1),
                ColumnValue::String(b"sku-1".to_vec()),
            ],
        };

        let ev = WriteRowsEvent {
            table_id: TABLE_ID,
            included_columns: vec![true, true],
            rows: vec![row],
        };

        handle_write_rows(&mut ctx, &make_header(), ev)
            .await
            .expect("write rows should succeed");

        let produced = rx.recv().await.expect("expected one event");
        assert_eq!(produced.op, Op::Insert);
        assert!(produced.before.is_none(), "insert must not have 'before'");
        let after = produced.after.expect("insert must have 'after'");
        assert_eq!(after["id"], 1);
        assert_eq!(after["sku"], "sku-1");
        assert_eq!(produced.table, "shop.orders");
        assert_eq!(produced.tx_id.as_deref(), Some("GTID-UNIT"));
    }

    #[tokio::test]
    async fn handle_update_rows_emits_update_with_before_and_after() {
        let (tx, mut rx) = mpsc::channel::<Event>(8);
        let mut ctx = make_runctx(tx);

        let before_row = RowEvent {
            column_values: vec![
                ColumnValue::LongLong(1),
                ColumnValue::String(b"sku-1".to_vec()),
            ],
        };
        let after_row = RowEvent {
            column_values: vec![
                ColumnValue::LongLong(1),
                ColumnValue::String(b"sku-1-changed".to_vec()),
            ],
        };

        let ev = UpdateRowsEvent {
            table_id: TABLE_ID,
            included_columns_before: vec![true, true],
            included_columns_after: vec![true, true],
            rows: vec![(before_row, after_row)],
        };

        handle_update_rows(&mut ctx, &make_header(), ev)
            .await
            .expect("update rows should succeed");

        let produced = rx.recv().await.expect("expected one event");
        assert_eq!(produced.op, Op::Update);

        let before = produced.before.expect("update must have `before`");
        let after = produced.after.expect("update must have `after`");

        assert_eq!(before["id"], 1);
        assert_eq!(before["sku"], "sku-1");
        assert_eq!(after["id"], 1);
        assert_eq!(after["sku"], "sku-1-changed");
        assert_eq!(produced.table, "shop.orders");
        assert_eq!(produced.tx_id.as_deref(), Some("GTID-UNIT"));
    }

    #[tokio::test]
    async fn handle_delete_rows_emits_delete_with_before_only() {
        let (tx, mut rx) = mpsc::channel::<Event>(8);
        let mut ctx = make_runctx(tx);

        let row = RowEvent {
            column_values: vec![
                ColumnValue::LongLong(1),
                ColumnValue::String(b"sku-1".to_vec()),
            ],
        };

        let ev = DeleteRowsEvent {
            table_id: TABLE_ID,
            included_columns: vec![true, true],
            rows: vec![row],
        };

        handle_delete_rows(&mut ctx, &make_header(), ev)
            .await
            .expect("delete rows should succeed");

        let produced = rx.recv().await.expect("expected one event");
        assert_eq!(produced.op, Op::Delete);
        assert!(produced.after.is_none(), "delete must not have `after`");
        let before = produced.before.expect("delete must have `before`");
        assert_eq!(before["id"], 1);
        assert_eq!(before["sku"], "sku-1");
        assert_eq!(produced.table, "shop.orders");
        assert_eq!(produced.tx_id.as_deref(), Some("GTID-UNIT"));
    }

    #[tokio::test]
    async fn events_carry_checkpoint_metadata() {
        let (tx, mut rx) = mpsc::channel::<Event>(8);
        let mut ctx = make_runctx(tx);

        // Set known position
        ctx.last_file = "mysql-bin.000005".to_string();
        ctx.last_pos = 12345;
        ctx.last_gtid = Some("abc-123:1-10".to_string());

        let row = RowEvent {
            column_values: vec![
                ColumnValue::LongLong(1),
                ColumnValue::String(b"test".to_vec()),
            ],
        };

        let ev = WriteRowsEvent {
            table_id: TABLE_ID,
            included_columns: vec![true, true],
            rows: vec![row],
        };

        handle_write_rows(&mut ctx, &make_header(), ev)
            .await
            .expect("write rows should succeed");

        let produced = rx.recv().await.expect("expected one event");
        assert!(
            produced.schema_sequence.is_some(),
            "event must have schema_sequence"
        );

        // Verify checkpoint is attached
        let cp_meta = produced.checkpoint.expect("event must have checkpoint");

        // Deserialize and verify
        let cp: MySqlCheckpoint = serde_json::from_slice(cp_meta.as_bytes())
            .expect("checkpoint must deserialize");

        assert_eq!(cp.file, "mysql-bin.000005");
        assert_eq!(cp.pos, 12345);
        assert_eq!(cp.gtid_set.as_deref(), Some("abc-123:1-10"));
    }
}
