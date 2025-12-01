use deltaforge_core::{Event, Op, SourceError, SourceMeta, SourceResult};
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

use crate::mysql::mysql_helpers::{persist_checkpoint, short_sql, ts_ms};

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

        _ => {
            debug!(source_id=%ctx.source_id, "unhandled event variant");
            Ok(())
        }
    }
}

#[instrument(skip_all, fields(source_id=%ctx.source_id))]
async fn handle_table_map(ctx: &mut RunCtx, tm: TableMapEvent) -> SourceResult<()> {
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
            );
            ev.tx_id = ctx.last_gtid.clone();
            if ctx.tx.send(ev).await.is_err() {
                error!(source_id=%ctx.source_id, "channel send failed (op=insert)");
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
            );
            ev.tx_id = ctx.last_gtid.clone();
            if ctx.tx.send(ev).await.is_err() {
                error!(source_id=%ctx.source_id, "channel send failed (op=update)");
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
            );
            ev.tx_id = ctx.last_gtid.clone();
            if ctx.tx.send(ev).await.is_err() {
                error!(source_id=%ctx.source_id, "channel send failed (op=delete)");
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
        ctx.schema.invalidate(db, "*").await;
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
    );
    ev.tx_id = ctx.last_gtid.clone();
    if ctx.tx.send(ev).await.is_err() {
        error!(source_id=%ctx.source_id, "channel send failed (op=ddl)");
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
    let gtid_set = match crate::mysql::mysql_helpers::fetch_executed_gtid_set(&ctx.dsn).await {
        Ok(Some(s)) => Some(s),
        _ => ctx.last_gtid.clone(), // fallback if GTID not enabled
    };    
    persist_checkpoint(
        &ctx.source_id,
        &ctx.chkpt,
        &ctx.last_file,
        ctx.last_pos,
        &gtid_set,
    )
    .await;
}
