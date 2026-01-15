use super::mysql_object::build_object;
use crate::mysql::LoopControl;
use crate::mysql::RunCtx;
use common::{ts_sec_to_ms, watchdog};
use deltaforge_core::{
    Event, Op, SourceInfo, SourcePosition, SourceResult, Transaction,
};
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

use crate::mysql::mysql_helpers::{make_checkpoint_meta, short_sql};

#[instrument(skip_all)]
pub(super) async fn read_next_event(
    stream: &mut BinlogStream,
    ctx: &RunCtx,
) -> Result<(EventHeader, EventData), LoopControl> {
    match watchdog(stream.read(), ctx.inactivity, &ctx.cancel, "binlog_read")
        .await
    {
        Ok(event) => Ok(event),
        Err(outcome) => {
            let control = LoopControl::from_binlog_outcome(outcome);

            if control.is_retryable() {
                counter!(
                    "deltaforge_source_reconnects_total",
                    "pipeline" => ctx.pipeline.clone(),
                    "source" => ctx.source_id.clone(),
                )
                .increment(1);
            }

            Err(control)
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

    if ctx.allow.matches(&tm.database_name, &tm.table_name) {
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

/// Build SourceInfo for MySQL events
fn build_source_info(
    ctx: &RunCtx,
    header: &EventHeader,
    db: &str,
    table: &str,
) -> SourceInfo {
    SourceInfo {
        version: concat!("deltaforge-", env!("CARGO_PKG_VERSION")).to_string(),
        connector: "mysql".to_string(),
        name: ctx.pipeline.clone(),
        ts_ms: ts_sec_to_ms(header.timestamp),
        db: db.to_string(),
        schema: None, // MySQL doesn't have schemas
        table: table.to_string(),
        snapshot: None,
        position: SourcePosition::mysql(
            ctx.server_id as u32,
            ctx.last_gtid.clone(),
            Some(ctx.last_file.clone()),
            Some(ctx.last_pos),
            None, // row number within event not tracked currently
        ),
    }
}

#[instrument(skip_all)]
async fn handle_write_rows(
    ctx: &mut RunCtx,
    header: &EventHeader,
    wr: mysql_binlog_connector_rust::event::write_rows_event::WriteRowsEvent,
) -> SourceResult<()> {
    if let Some(tm) = ctx.table_map.get(&wr.table_id) {
        if !ctx.allow.matches(&tm.database_name, &tm.table_name) {
            return Ok(());
        }
        let loaded = ctx
            .schema
            .load_schema(&tm.database_name, &tm.table_name)
            .await?;
        debug!(source_id=%ctx.source_id, db=%tm.database_name, table=%tm.table_name, rows=wr.rows.len(), "write_rows");

        for row in wr.rows {
            let after = build_object(
                &loaded.column_names,
                &wr.included_columns,
                &row.column_values,
            );

            let source_info = build_source_info(
                ctx,
                header,
                &tm.database_name,
                &tm.table_name,
            );
            let mut ev = Event::new_row(
                source_info,
                Op::Create,
                None,
                Some(after),
                ts_sec_to_ms(header.timestamp),
                header.event_length as usize,
            )
            .with_tenant(ctx.tenant.clone())
            .with_checkpoint(make_checkpoint_meta(
                &ctx.last_file,
                ctx.last_pos,
                &ctx.last_gtid,
            ));

            // Set transaction info if GTID is available
            if let Some(gtid) = &ctx.last_gtid {
                ev.transaction = Some(Transaction {
                    id: gtid.clone(),
                    total_order: None,
                    data_collection_order: None,
                });
            }

            ev.schema_version = Some(loaded.fingerprint.to_string());
            ev.schema_sequence = Some(loaded.sequence);

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
        if !ctx.allow.matches(&tm.database_name, &tm.table_name) {
            return Ok(());
        }
        let loaded = ctx
            .schema
            .load_schema(&tm.database_name, &tm.table_name)
            .await?;
        debug!(source_id=%ctx.source_id, db=%tm.database_name, table=%tm.table_name, rows=ur.rows.len(), "update_rows");

        for (before_row, after_row) in ur.rows {
            let before = build_object(
                &loaded.column_names,
                &ur.included_columns_before,
                &before_row.column_values,
            );
            let after = build_object(
                &loaded.column_names,
                &ur.included_columns_after,
                &after_row.column_values,
            );

            let source_info = build_source_info(
                ctx,
                header,
                &tm.database_name,
                &tm.table_name,
            );
            let mut ev = Event::new_row(
                source_info,
                Op::Update,
                Some(before),
                Some(after),
                ts_sec_to_ms(header.timestamp),
                header.event_length as usize,
            )
            .with_tenant(ctx.tenant.clone())
            .with_checkpoint(make_checkpoint_meta(
                &ctx.last_file,
                ctx.last_pos,
                &ctx.last_gtid,
            ));

            if let Some(gtid) = &ctx.last_gtid {
                ev.transaction = Some(Transaction {
                    id: gtid.clone(),
                    total_order: None,
                    data_collection_order: None,
                });
            }

            ev.schema_version = Some(loaded.fingerprint.to_string());
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
        if !ctx.allow.matches(&tm.database_name, &tm.table_name) {
            return Ok(());
        }
        let loaded = ctx
            .schema
            .load_schema(&tm.database_name, &tm.table_name)
            .await?;
        debug!(source_id=%ctx.source_id, db=%tm.database_name, table=%tm.table_name, rows=dr.rows.len(), "delete_rows");

        for row in dr.rows {
            let before = build_object(
                &loaded.column_names,
                &dr.included_columns,
                &row.column_values,
            );

            let source_info = build_source_info(
                ctx,
                header,
                &tm.database_name,
                &tm.table_name,
            );
            let mut ev = Event::new_row(
                source_info,
                Op::Delete,
                Some(before),
                None,
                ts_sec_to_ms(header.timestamp),
                header.event_length as usize,
            )
            .with_tenant(ctx.tenant.clone())
            .with_checkpoint(make_checkpoint_meta(
                &ctx.last_file,
                ctx.last_pos,
                &ctx.last_gtid,
            ));

            if let Some(gtid) = &ctx.last_gtid {
                ev.transaction = Some(Transaction {
                    id: gtid.clone(),
                    total_order: None,
                    data_collection_order: None,
                });
            }

            ev.schema_version = Some(loaded.fingerprint.to_string());
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

fn handle_gtid(
    ctx: &mut RunCtx,
    gt: mysql_binlog_connector_rust::event::gtid_event::GtidEvent,
) {
    let gtid_str = gt.gtid.clone();
    debug!(source_id=%ctx.source_id, gtid=%gtid_str, "gtid");
    ctx.last_gtid = Some(gtid_str);
}

fn handle_rotate(
    ctx: &mut RunCtx,
    rot: mysql_binlog_connector_rust::event::rotate_event::RotateEvent,
) {
    info!(source_id=%ctx.source_id, file=%rot.binlog_filename, pos=%rot.binlog_position, "rotate");
    ctx.last_file = rot.binlog_filename;
    ctx.last_pos = rot.binlog_position;
}

async fn handle_xid(ctx: &mut RunCtx) {
    debug!(source_id=%ctx.source_id, "xid (commit)");
    // Transaction boundary - could emit tx_end marker here if needed
}

/// Extract table name from DDL statement.
/// Handles: ALTER TABLE t, CREATE TABLE t, DROP TABLE t, TRUNCATE TABLE t, RENAME TABLE t
fn extract_table_from_ddl(sql: &str) -> Option<String> {
    let sql_upper = sql.to_uppercase();
    let sql_trimmed = sql.trim();

    // Find the position after TABLE keyword
    let table_pos = if sql_upper.starts_with("ALTER TABLE")
        || sql_upper.starts_with("CREATE TABLE")
        || sql_upper.starts_with("DROP TABLE")
        || sql_upper.starts_with("TRUNCATE TABLE")
    {
        // Skip "XXX TABLE " prefix
        sql_upper.find("TABLE").map(|p| p + 6)
    } else if sql_upper.starts_with("RENAME TABLE") {
        sql_upper.find("TABLE").map(|p| p + 6)
    } else if sql_upper.starts_with("TRUNCATE ")
        && !sql_upper.starts_with("TRUNCATE TABLE")
    {
        // TRUNCATE without TABLE keyword
        Some(9)
    } else {
        None
    }?;

    // Extract the table name (possibly with backticks or schema prefix)
    let remaining = sql_trimmed.get(table_pos..)?.trim_start();

    // Handle IF EXISTS / IF NOT EXISTS
    let remaining = if remaining.to_uppercase().starts_with("IF EXISTS ") {
        remaining.get(10..)?.trim_start()
    } else if remaining.to_uppercase().starts_with("IF NOT EXISTS ") {
        remaining.get(14..)?.trim_start()
    } else {
        remaining
    };

    // Extract identifier (handles `backticks`, schema.table, and plain names)
    let table_name = extract_identifier(remaining)?;

    // If it's schema.table format, take only the table part
    if let Some(dot_pos) = table_name.find('.') {
        Some(table_name[dot_pos + 1..].trim_matches('`').to_string())
    } else {
        Some(table_name.trim_matches('`').to_string())
    }
}

/// Extract an SQL identifier (table/schema name), handling backticks.
fn extract_identifier(s: &str) -> Option<String> {
    let s = s.trim_start();
    if s.is_empty() {
        return None;
    }

    if s.starts_with('`') {
        // Backtick-quoted identifier, possibly with schema: `schema`.`table` or `table`
        let mut result = String::new();
        let mut chars = s.chars().peekable();
        chars.next(); // skip opening backtick

        // Collect first identifier
        while let Some(&c) = chars.peek() {
            if c == '`' {
                chars.next();
                break;
            }
            result.push(c);
            chars.next();
        }

        // Check for schema.table pattern
        if chars.peek() == Some(&'.') {
            chars.next(); // skip dot
            result.push('.');
            if chars.peek() == Some(&'`') {
                chars.next(); // skip opening backtick
                while let Some(&c) = chars.peek() {
                    if c == '`' {
                        break;
                    }
                    result.push(c);
                    chars.next();
                }
            }
        }

        Some(result)
    } else {
        // Unquoted identifier - take until whitespace or special char
        let end = s
            .find(|c: char| {
                c.is_whitespace() || c == '(' || c == ';' || c == ','
            })
            .unwrap_or(s.len());
        if end == 0 {
            None
        } else {
            Some(s[..end].to_string())
        }
    }
}

#[instrument(skip_all)]
async fn handle_query(
    ctx: &mut RunCtx,
    header: &EventHeader,
    q: mysql_binlog_connector_rust::event::query_event::QueryEvent,
) -> SourceResult<()> {
    let sql_upper = q.query.to_uppercase();

    // Skip transaction markers
    if sql_upper == "BEGIN" || sql_upper == "COMMIT" || sql_upper == "ROLLBACK"
    {
        return Ok(());
    }

    // Handle DDL
    if sql_upper.starts_with("ALTER")
        || sql_upper.starts_with("CREATE")
        || sql_upper.starts_with("DROP")
        || sql_upper.starts_with("TRUNCATE")
        || sql_upper.starts_with("RENAME")
    {
        info!(
            source_id=%ctx.source_id,
            db=%q.schema,
            sql=%short_sql(&q.query, 80),
            "DDL detected"
        );

        // For DDL, we use the query's schema as both db and table context
        let source_info = SourceInfo {
            version: concat!("deltaforge-", env!("CARGO_PKG_VERSION"))
                .to_string(),
            connector: "mysql".to_string(),
            name: ctx.pipeline.clone(),
            ts_ms: ts_sec_to_ms(header.timestamp),
            db: q.schema.clone(),
            schema: None,
            table: "_ddl".to_string(), // Placeholder for DDL events
            snapshot: None,
            position: SourcePosition::mysql(
                ctx.server_id as u32,
                ctx.last_gtid.clone(),
                Some(ctx.last_file.clone()),
                Some(ctx.last_pos),
                None,
            ),
        };

        let ddl_payload = serde_json::json!({
            "sql": q.query,
            "database": q.schema,
        });

        let ev = Event::new_ddl(
            source_info,
            ddl_payload,
            ts_sec_to_ms(header.timestamp),
            header.event_length as usize,
        )
        .with_tenant(ctx.tenant.clone())
        .with_checkpoint(make_checkpoint_meta(
            &ctx.last_file,
            ctx.last_pos,
            &ctx.last_gtid,
        ));

        if let Err(_) = ctx.tx.send(ev).await {
            error!(source_id=%ctx.source_id, "channel send failed (op=ddl)");
        }

        // Reload schema after DDL to pick up changes
        // Try to extract table name from DDL for targeted reload
        if let Some(table_name) = extract_table_from_ddl(&q.query) {
            let db = if q.schema.is_empty() {
                &ctx.default_db
            } else {
                &q.schema
            };
            info!(
                source_id=%ctx.source_id,
                db=%db,
                table=%table_name,
                "reloading schema after DDL"
            );
            if let Err(e) = ctx.schema.reload_schema(db, &table_name).await {
                warn!(
                    source_id=%ctx.source_id,
                    error=?e,
                    "failed to reload schema after DDL"
                );
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mysql::MySqlCheckpoint;
    use crate::mysql::mysql_schema_loader::MySqlSchemaLoader;
    use common::AllowList;
    use mysql_binlog_connector_rust::column::column_value::ColumnValue;
    use mysql_binlog_connector_rust::event::delete_rows_event::DeleteRowsEvent;
    use mysql_binlog_connector_rust::event::row_event::RowEvent;
    use mysql_binlog_connector_rust::event::table_map_event::TableMapEvent;
    use mysql_binlog_connector_rust::event::update_rows_event::UpdateRowsEvent;
    use mysql_binlog_connector_rust::event::write_rows_event::WriteRowsEvent;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;
    use tokio::sync::{Notify, mpsc};
    use tokio_util::sync::CancellationToken;

    const TABLE_ID: u64 = 42;

    fn make_runctx(tx: mpsc::Sender<Event>) -> RunCtx {
        // Use from_static to create a test schema loader
        let cols: HashMap<(String, String), Arc<Vec<String>>> =
            HashMap::from([(
                ("shop".to_string(), "orders".to_string()),
                Arc::new(vec!["id".to_string(), "sku".to_string()]),
            )]);
        let schema = MySqlSchemaLoader::from_static(cols);

        let mut table_map = HashMap::new();
        table_map.insert(
            TABLE_ID,
            TableMapEvent {
                table_id: TABLE_ID,
                database_name: "shop".to_string(),
                table_name: "orders".to_string(),
                column_types: vec![],
                column_metas: vec![],
                null_bits: vec![],
            },
        );

        RunCtx {
            source_id: "unit-test".to_string(),
            pipeline: "test-pipeline".to_string(),
            tenant: "test-tenant".to_string(),
            dsn: "mysql://fake".to_string(),
            host: "localhost".to_string(),
            default_db: "test".to_string(),
            server_id: 1,
            tx,
            chkpt: Arc::new(checkpoints::MemCheckpointStore::new().unwrap()),
            cancel: CancellationToken::new(),
            paused: Arc::new(AtomicBool::new(false)),
            pause_notify: Arc::new(Notify::new()),
            schema,
            allow: AllowList::new(&["shop.orders".to_string()]),
            retry: common::RetryPolicy::default(),
            inactivity: Duration::from_secs(60),
            table_map,
            last_file: "mysql-bin.000001".to_string(),
            last_pos: 1234,
            last_gtid: Some("GTID-UNIT".to_string()),
        }
    }

    fn make_header() -> EventHeader {
        EventHeader {
            timestamp: 1_700_000_000,
            event_type: 0,
            server_id: 1,
            event_length: 100,
            next_event_position: 1234,
            event_flags: 0,
        }
    }

    #[tokio::test]
    async fn handle_write_rows_emits_create_event() {
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
        assert_eq!(produced.op, Op::Create);
        assert!(produced.before.is_none(), "insert must not have `before`");
        let after = produced.after.expect("insert must have `after`");
        assert_eq!(after["id"], 1);
        assert_eq!(after["sku"], "sku-1");

        // Verify new structure
        assert_eq!(produced.source.connector, "mysql");
        assert_eq!(produced.source.db, "shop");
        assert_eq!(produced.source.table, "orders");
        assert_eq!(produced.tenant_id, Some("test-tenant".to_string()));
        assert!(produced.transaction.is_some());
        assert_eq!(produced.transaction.as_ref().unwrap().id, "GTID-UNIT");
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

        // Verify new structure
        assert_eq!(produced.source.table, "orders");
        assert!(produced.transaction.is_some());
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

        // Verify new structure
        assert_eq!(produced.source.table, "orders");
        assert!(produced.transaction.is_some());
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

        // Verify source position info
        assert_eq!(
            produced.source.position.file,
            Some("mysql-bin.000005".to_string())
        );
        assert_eq!(produced.source.position.pos, Some(12345));
        assert_eq!(
            produced.source.position.gtid,
            Some("abc-123:1-10".to_string())
        );
    }

    #[tokio::test]
    async fn events_carry_schema_metadata() {
        let (tx, mut rx) = mpsc::channel::<Event>(8);
        let mut ctx = make_runctx(tx);

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

        // Verify checkpoint
        let cp_meta = produced.checkpoint.expect("event must have checkpoint");
        let cp: MySqlCheckpoint = serde_json::from_slice(cp_meta.as_bytes())
            .expect("checkpoint must deserialize");
        assert_eq!(cp.file, "mysql-bin.000005");
        assert_eq!(cp.pos, 12345);
        assert_eq!(cp.gtid_set.as_deref(), Some("abc-123:1-10"));

        // Verify schema metadata
        assert!(
            produced.schema_sequence.is_some(),
            "event must have schema_sequence"
        );

        let version = produced
            .schema_version
            .expect("event must have schema_version");
        assert!(
            version.starts_with("sha256:"),
            "fingerprint should have sha256 prefix, got: {}",
            version
        );
        assert_eq!(
            version.len(),
            71,
            "fingerprint should be 71 chars (sha256: prefix + 64 hex)"
        );
    }

    #[tokio::test]
    async fn schema_metadata_consistent_across_ops() {
        let (tx, mut rx) = mpsc::channel::<Event>(16);
        let mut ctx = make_runctx(tx);

        // Insert
        let write_ev = WriteRowsEvent {
            table_id: TABLE_ID,
            included_columns: vec![true, true],
            rows: vec![RowEvent {
                column_values: vec![
                    ColumnValue::LongLong(1),
                    ColumnValue::String(b"sku-1".to_vec()),
                ],
            }],
        };
        handle_write_rows(&mut ctx, &make_header(), write_ev)
            .await
            .unwrap();

        // Update
        let update_ev = UpdateRowsEvent {
            table_id: TABLE_ID,
            included_columns_before: vec![true, true],
            included_columns_after: vec![true, true],
            rows: vec![(
                RowEvent {
                    column_values: vec![
                        ColumnValue::LongLong(1),
                        ColumnValue::String(b"sku-1".to_vec()),
                    ],
                },
                RowEvent {
                    column_values: vec![
                        ColumnValue::LongLong(1),
                        ColumnValue::String(b"sku-2".to_vec()),
                    ],
                },
            )],
        };
        handle_update_rows(&mut ctx, &make_header(), update_ev)
            .await
            .unwrap();

        // Delete
        let delete_ev = DeleteRowsEvent {
            table_id: TABLE_ID,
            included_columns: vec![true, true],
            rows: vec![RowEvent {
                column_values: vec![
                    ColumnValue::LongLong(1),
                    ColumnValue::String(b"sku-2".to_vec()),
                ],
            }],
        };
        handle_delete_rows(&mut ctx, &make_header(), delete_ev)
            .await
            .unwrap();

        // Collect all events
        let insert = rx.recv().await.unwrap();
        let update = rx.recv().await.unwrap();
        let delete = rx.recv().await.unwrap();

        // All should have same schema metadata (same table, same schema)
        assert_eq!(insert.schema_version, update.schema_version);
        assert_eq!(update.schema_version, delete.schema_version);
        assert_eq!(insert.schema_sequence, update.schema_sequence);
        assert_eq!(update.schema_sequence, delete.schema_sequence);

        // All should have consistent source info
        assert_eq!(insert.source.connector, "mysql");
        assert_eq!(update.source.connector, "mysql");
        assert_eq!(delete.source.connector, "mysql");
    }

    #[test]
    fn extract_table_from_ddl_alter_table() {
        assert_eq!(
            extract_table_from_ddl(
                "ALTER TABLE orders ADD COLUMN status VARCHAR(32)"
            ),
            Some("orders".to_string())
        );
        assert_eq!(
            extract_table_from_ddl(
                "ALTER TABLE `orders` ADD COLUMN status VARCHAR(32)"
            ),
            Some("orders".to_string())
        );
        assert_eq!(
            extract_table_from_ddl(
                "ALTER TABLE shop.orders ADD COLUMN status VARCHAR(32)"
            ),
            Some("orders".to_string())
        );
        assert_eq!(
            extract_table_from_ddl(
                "ALTER TABLE `shop`.`orders` ADD COLUMN status VARCHAR(32)"
            ),
            Some("orders".to_string())
        );
    }

    #[test]
    fn extract_table_from_ddl_create_table() {
        assert_eq!(
            extract_table_from_ddl("CREATE TABLE users (id INT PRIMARY KEY)"),
            Some("users".to_string())
        );
        assert_eq!(
            extract_table_from_ddl("CREATE TABLE IF NOT EXISTS users (id INT)"),
            Some("users".to_string())
        );
        assert_eq!(
            extract_table_from_ddl("CREATE TABLE `my_db`.`users` (id INT)"),
            Some("users".to_string())
        );
    }

    #[test]
    fn extract_table_from_ddl_drop_table() {
        assert_eq!(
            extract_table_from_ddl("DROP TABLE orders"),
            Some("orders".to_string())
        );
        assert_eq!(
            extract_table_from_ddl("DROP TABLE IF EXISTS orders"),
            Some("orders".to_string())
        );
    }

    #[test]
    fn extract_table_from_ddl_truncate() {
        assert_eq!(
            extract_table_from_ddl("TRUNCATE TABLE orders"),
            Some("orders".to_string())
        );
        assert_eq!(
            extract_table_from_ddl("TRUNCATE orders"),
            Some("orders".to_string())
        );
    }

    #[test]
    fn extract_table_from_ddl_non_ddl() {
        assert_eq!(extract_table_from_ddl("SELECT * FROM orders"), None);
        assert_eq!(
            extract_table_from_ddl("INSERT INTO orders VALUES (1)"),
            None
        );
        assert_eq!(extract_table_from_ddl("BEGIN"), None);
    }
}
