//! PostgreSQL replication event handling.
//!
//! Processes pgoutput protocol messages from logical replication and
//! converts them to DeltaForge events.

use bytes::Bytes;
use deltaforge_core::{Event, Op, SourceMeta, SourceResult};
use metrics::counter;
use pgwire_replication::{Lsn, client::ReplicationEvent};
use tracing::{debug, error, info, instrument, warn};

use common::watchdog;

use super::RunCtx;
use super::postgres_errors::LoopControl;
use super::postgres_helpers::{make_checkpoint_meta, pg_timestamp_to_unix_ms};
use super::postgres_object::{RelationColumn, build_object, parse_tuple_data};

/// Relation metadata from pgoutput.
#[derive(Debug, Clone)]
pub struct RelationInfo {
    pub id: u32,
    pub schema: String,
    pub table: String,
    pub columns: Vec<RelationColumn>,
    /// Replica identity: d=default, n=nothing, f=full, i=index
    pub replica_identity: char,
}

/// Read next replication event with watchdog timeout.
#[instrument(skip_all)]
pub(super) async fn read_next_event(ctx: &RunCtx) -> Result<Option<ReplicationEvent>, LoopControl> {
    let mut client = ctx.repl_client.lock().await;

    match watchdog(client.recv(), ctx.inactivity, &ctx.cancel, "repl_recv").await {
        Ok(Some(event)) => Ok(Some(event)),
        Ok(None) => {
            info!(source_id = %ctx.source_id, "replication stream ended");
            Ok(None)
        }
        Err(outcome) => {
            if ctx.cancel.is_cancelled() {
                return Err(LoopControl::Stop);
            }

            let control = LoopControl::from_replication_outcome(outcome);
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

/// Dispatch a replication event to appropriate handler.
/// Returns LoopControl to signal schema reload or fatal errors.
#[instrument(skip_all)]
pub(super) async fn dispatch_event(ctx: &mut RunCtx, event: ReplicationEvent) -> Result<(), LoopControl> {
    match event {
        ReplicationEvent::XLogData { wal_start, wal_end, data, .. } => {
            debug!(wal_start = %wal_start, wal_end = %wal_end, bytes = data.len(), "xlog data");
            ctx.last_lsn = wal_end;
            handle_pgoutput_message(ctx, &data, wal_end).await?;
            ctx.repl_client.lock().await.update_applied_lsn(wal_end);
        }
        ReplicationEvent::KeepAlive { wal_end, reply_requested, .. } => {
            debug!(wal_end = %wal_end, reply_requested, "keepalive");
            ctx.last_lsn = wal_end;
        }
        ReplicationEvent::Begin { final_lsn, commit_time_micros, xid } => {
            debug!(final_lsn = %final_lsn, xid, "transaction begin");
            ctx.current_tx_id = Some(xid);
            ctx.current_tx_commit_time = Some(commit_time_micros);
        }
        ReplicationEvent::Commit { lsn, end_lsn, .. } => {
            debug!(commit_lsn = %lsn, end_lsn = %end_lsn, "transaction commit");
            ctx.last_lsn = end_lsn;
            ctx.current_tx_id = None;
            ctx.current_tx_commit_time = None;
        }
        ReplicationEvent::StoppedAt { reached } => {
            info!(reached = %reached, "replication stopped at target LSN");
        }
    }
    Ok(())
}

/// Parse and handle pgoutput protocol messages.
async fn handle_pgoutput_message(ctx: &mut RunCtx, data: &Bytes, wal_lsn: Lsn) -> Result<(), LoopControl> {
    if data.is_empty() {
        return Ok(());
    }

    let msg_type = data[0];
    let payload = &data[1..];

    match msg_type {
        b'R' => handle_relation(ctx, payload),
        b'I' => handle_insert(ctx, payload, wal_lsn).await.map_err(LoopControl::Fail),
        b'U' => handle_update(ctx, payload, wal_lsn).await.map_err(LoopControl::Fail),
        b'D' => handle_delete(ctx, payload, wal_lsn).await.map_err(LoopControl::Fail),
        b'T' => handle_truncate(ctx, payload, wal_lsn).await.map_err(LoopControl::Fail),
        b'B' | b'C' => Ok(()), // Begin/Commit handled in ReplicationEvent
        b'O' => { debug!("origin message"); Ok(()) }
        b'Y' => { debug!("type message"); Ok(()) }
        b'M' => { debug!("logical message"); Ok(()) }
        _ => { debug!(msg_type = %msg_type, "unknown pgoutput message"); Ok(()) }
    }
}

/// Handle relation (table metadata) message.
/// Returns LoopControl::ReloadSchema if schema changed and needs reload.
fn handle_relation(ctx: &mut RunCtx, payload: &[u8]) -> Result<(), LoopControl> {
    if payload.len() < 8 {
        return Ok(());
    }

    let relation_id = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
    let mut offset = 4;

    let schema = read_cstring(payload, &mut offset);
    let table = read_cstring(payload, &mut offset);

    let replica_identity = if offset < payload.len() { payload[offset] as char } else { 'd' };
    offset += 1;

    if replica_identity != 'f' {
        warn!(
            schema = %schema, table = %table, identity = %replica_identity,
            "table does not have REPLICA IDENTITY FULL - before images will be incomplete"
        );
    }

    if offset + 2 > payload.len() {
        return Ok(());
    }
    let col_count = u16::from_be_bytes([payload[offset], payload[offset + 1]]) as usize;
    offset += 2;

    let mut columns = Vec::with_capacity(col_count);
    for _ in 0..col_count {
        if offset >= payload.len() {
            break;
        }

        let flags = payload[offset];
        offset += 1;

        let name = read_cstring(payload, &mut offset);

        if offset + 8 > payload.len() {
            break;
        }
        let type_oid = u32::from_be_bytes([payload[offset], payload[offset + 1], payload[offset + 2], payload[offset + 3]]);
        offset += 4;

        let type_modifier = i32::from_be_bytes([payload[offset], payload[offset + 1], payload[offset + 2], payload[offset + 3]]);
        offset += 4;

        columns.push(RelationColumn { name, type_oid, type_modifier, flags });
    }

    // Check if this relation already exists and if schema changed
    let existing = ctx.relation_map.get(&relation_id);
    let is_new = existing.is_none();
    let schema_changed = existing
        .map(|r| r.columns.len() != columns.len() || columns_differ(&r.columns, &columns))
        .unwrap_or(false);

    // Update relation map with new column info
    ctx.relation_map.insert(
        relation_id,
        RelationInfo { id: relation_id, schema: schema.clone(), table: table.clone(), columns, replica_identity },
    );

    if ctx.allow.matches(&schema, &table) {
        if is_new {
            info!(relation_id, schema = %schema, table = %table, "relation mapped");
        } else {
            debug!(relation_id, schema = %schema, table = %table, "relation re-mapped");
        }
    }

    // If schema changed, signal main loop to reload (like MySQL does)
    if schema_changed && ctx.allow.matches(&schema, &table) {
        info!(
            relation_id, schema = %schema, table = %table,
            "schema changed, requesting reload"
        );
        return Err(LoopControl::ReloadSchema {
            schema: Some(schema),
            table: Some(table),
        });
    }

    Ok(())
}

/// Check if columns differ (by name or type).
fn columns_differ(old: &[RelationColumn], new: &[RelationColumn]) -> bool {
    if old.len() != new.len() {
        return true;
    }
    for (o, n) in old.iter().zip(new.iter()) {
        if o.name != n.name || o.type_oid != n.type_oid {
            return true;
        }
    }
    false
}

/// Handle INSERT message.
async fn handle_insert(ctx: &mut RunCtx, payload: &[u8], wal_lsn: Lsn) -> SourceResult<()> {
    if payload.len() < 5 {
        return Ok(());
    }

    let relation_id = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
    let tuple_marker = payload[4];

    if tuple_marker != b'N' {
        warn!(marker = %char::from(tuple_marker), "unexpected insert tuple marker");
        return Ok(());
    }

    let Some(relation) = ctx.relation_map.get(&relation_id) else {
        warn!(relation_id, "insert for unknown relation");
        return Ok(());
    };

    if !ctx.allow.matches(&relation.schema, &relation.table) {
        return Ok(());
    }

    let loaded = ctx.schema.load_schema(&relation.schema, &relation.table).await?;

    let tuple_data = Bytes::copy_from_slice(&payload[5..]);
    let (values, _) = parse_tuple_data(&tuple_data, relation.columns.len());
    let after = build_object(&relation.columns, &values);

    let timestamp_ms = ctx.current_tx_commit_time
        .map(pg_timestamp_to_unix_ms)
        .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());

    let mut ev = Event::new_row(
        ctx.tenant.clone(),
        SourceMeta { kind: "postgres".into(), host: ctx.host.clone(), db: relation.schema.clone() },
        format!("{}.{}", relation.schema, relation.table),
        Op::Insert,
        None,
        Some(after),
        timestamp_ms,
        payload.len(),
    );

    ev.tx_id = ctx.current_tx_id.map(|id| id.to_string());
    ev.checkpoint = Some(make_checkpoint_meta(&wal_lsn, ctx.current_tx_id));
    ev.schema_version = Some(loaded.fingerprint.to_string());
    ev.schema_sequence = Some(loaded.sequence);

    send_event(ctx, ev, &relation.schema, &relation.table, "insert").await;
    Ok(())
}

/// Handle UPDATE message.
async fn handle_update(ctx: &mut RunCtx, payload: &[u8], wal_lsn: Lsn) -> SourceResult<()> {
    if payload.len() < 5 {
        return Ok(());
    }

    let relation_id = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
    let mut offset = 4;

    let Some(relation) = ctx.relation_map.get(&relation_id) else {
        warn!(relation_id, "update for unknown relation");
        return Ok(());
    };

    if !ctx.allow.matches(&relation.schema, &relation.table) {
        return Ok(());
    }

    // Clone what we need before the mutable borrow ends
    let schema = relation.schema.clone();
    let table = relation.table.clone();
    let columns = relation.columns.clone();

    let mut before_values = None;
    let mut after_values = None;

    while offset < payload.len() {
        let marker = payload[offset];
        offset += 1;

        match marker {
            b'K' | b'O' => {
                let tuple_data = Bytes::copy_from_slice(&payload[offset..]);
                let (values, consumed) = parse_tuple_data(&tuple_data, columns.len());
                before_values = Some(values);
                offset += consumed;
            }
            b'N' => {
                let tuple_data = Bytes::copy_from_slice(&payload[offset..]);
                let (values, _) = parse_tuple_data(&tuple_data, columns.len());
                after_values = Some(values);
                break;
            }
            _ => break,
        }
    }

    let Some(after_vals) = after_values else {
        warn!("update missing new tuple");
        return Ok(());
    };

    let loaded = ctx.schema.load_schema(&schema, &table).await?;

    let before = before_values.map(|v| build_object(&columns, &v));
    let after = build_object(&columns, &after_vals);

    let timestamp_ms = ctx.current_tx_commit_time
        .map(pg_timestamp_to_unix_ms)
        .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());

    let mut ev = Event::new_row(
        ctx.tenant.clone(),
        SourceMeta { kind: "postgres".into(), host: ctx.host.clone(), db: schema.clone() },
        format!("{}.{}", schema, table),
        Op::Update,
        before,
        Some(after),
        timestamp_ms,
        payload.len(),
    );

    ev.tx_id = ctx.current_tx_id.map(|id| id.to_string());
    ev.checkpoint = Some(make_checkpoint_meta(&wal_lsn, ctx.current_tx_id));
    ev.schema_version = Some(loaded.fingerprint.to_string());
    ev.schema_sequence = Some(loaded.sequence);

    send_event(ctx, ev, &schema, &table, "update").await;
    Ok(())
}

/// Handle DELETE message.
async fn handle_delete(ctx: &mut RunCtx, payload: &[u8], wal_lsn: Lsn) -> SourceResult<()> {
    if payload.len() < 5 {
        return Ok(());
    }

    let relation_id = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
    let tuple_marker = payload[4];

    let Some(relation) = ctx.relation_map.get(&relation_id) else {
        warn!(relation_id, "delete for unknown relation");
        return Ok(());
    };

    if !ctx.allow.matches(&relation.schema, &relation.table) {
        return Ok(());
    }

    if tuple_marker != b'K' && tuple_marker != b'O' {
        warn!(marker = %char::from(tuple_marker), "unexpected delete tuple marker");
        return Ok(());
    }

    // Clone what we need
    let schema = relation.schema.clone();
    let table = relation.table.clone();
    let columns = relation.columns.clone();

    let loaded = ctx.schema.load_schema(&schema, &table).await?;

    let tuple_data = Bytes::copy_from_slice(&payload[5..]);
    let (values, _) = parse_tuple_data(&tuple_data, columns.len());
    let before = build_object(&columns, &values);

    let timestamp_ms = ctx.current_tx_commit_time
        .map(pg_timestamp_to_unix_ms)
        .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());

    let mut ev = Event::new_row(
        ctx.tenant.clone(),
        SourceMeta { kind: "postgres".into(), host: ctx.host.clone(), db: schema.clone() },
        format!("{}.{}", schema, table),
        Op::Delete,
        Some(before),
        None,
        timestamp_ms,
        payload.len(),
    );

    ev.tx_id = ctx.current_tx_id.map(|id| id.to_string());
    ev.checkpoint = Some(make_checkpoint_meta(&wal_lsn, ctx.current_tx_id));
    ev.schema_version = Some(loaded.fingerprint.to_string());
    ev.schema_sequence = Some(loaded.sequence);

    send_event(ctx, ev, &schema, &table, "delete").await;
    Ok(())
}

/// Handle TRUNCATE message.
async fn handle_truncate(ctx: &mut RunCtx, payload: &[u8], wal_lsn: Lsn) -> SourceResult<()> {
    if payload.len() < 9 {
        return Ok(());
    }

    let relation_count = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
    let options = payload[4];
    let cascade = (options & 1) != 0;
    let restart_identity = (options & 2) != 0;

    let mut offset = 5;
    let mut tables = Vec::with_capacity(relation_count as usize);

    for _ in 0..relation_count {
        if offset + 4 > payload.len() {
            break;
        }
        let rel_id = u32::from_be_bytes([payload[offset], payload[offset + 1], payload[offset + 2], payload[offset + 3]]);
        offset += 4;

        if let Some(rel) = ctx.relation_map.get(&rel_id) {
            tables.push(format!("{}.{}", rel.schema, rel.table));
        }
    }

    info!(tables = ?tables, cascade, restart_identity, lsn = %wal_lsn, "truncate received");

    for table_name in &tables {
        let mut ev = Event::new_ddl(
            ctx.tenant.clone(),
            SourceMeta { kind: "postgres".into(), host: ctx.host.clone(), db: ctx.default_schema.clone() },
            table_name.clone(),
            "TRUNCATE".into(),
            chrono::Utc::now().timestamp_millis(),
            0,
        );

        ev.tx_id = ctx.current_tx_id.map(|id| id.to_string());
        ev.checkpoint = Some(make_checkpoint_meta(&wal_lsn, ctx.current_tx_id));
        let _ = ctx.tx.send(ev).await;
    }

    Ok(())
}

/// Send event and update metrics.
async fn send_event(ctx: &RunCtx, ev: Event, schema: &str, table: &str, op: &str) {
    let table_name = format!("{}.{}", schema, table);
    match ctx.tx.send(ev).await {
        Ok(_) => {
            counter!(
                "deltaforge_source_events_total",
                "pipeline" => ctx.pipeline.clone(),
                "source" => ctx.source_id.clone(),
                "table" => table_name,
            )
            .increment(1);
        }
        Err(_) => {
            error!(source_id = %ctx.source_id, op, "channel send failed");
        }
    }
}

/// Read null-terminated C string from buffer.
fn read_cstring(data: &[u8], offset: &mut usize) -> String {
    let start = *offset;
    while *offset < data.len() && data[*offset] != 0 {
        *offset += 1;
    }
    let s = String::from_utf8_lossy(&data[start..*offset]).to_string();
    if *offset < data.len() {
        *offset += 1;
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_cstring() {
        let data = b"hello\0world\0";
        let mut offset = 0;
        assert_eq!(read_cstring(data, &mut offset), "hello");
        assert_eq!(offset, 6);
        assert_eq!(read_cstring(data, &mut offset), "world");
    }

    #[test]
    fn test_relation_column_is_key() {
        let key_col = RelationColumn { name: "id".into(), type_oid: 23, type_modifier: -1, flags: 1 };
        assert!(key_col.is_key());

        let non_key_col = RelationColumn { name: "name".into(), type_oid: 25, type_modifier: -1, flags: 0 };
        assert!(!non_key_col.is_key());
    }

    #[test]
    fn test_columns_differ() {
        let cols1 = vec![
            RelationColumn { name: "id".into(), type_oid: 23, type_modifier: -1, flags: 1 },
            RelationColumn { name: "name".into(), type_oid: 25, type_modifier: -1, flags: 0 },
        ];

        let cols2 = vec![
            RelationColumn { name: "id".into(), type_oid: 23, type_modifier: -1, flags: 1 },
            RelationColumn { name: "name".into(), type_oid: 25, type_modifier: -1, flags: 0 },
        ];

        let cols3 = vec![
            RelationColumn { name: "id".into(), type_oid: 23, type_modifier: -1, flags: 1 },
            RelationColumn { name: "name".into(), type_oid: 25, type_modifier: -1, flags: 0 },
            RelationColumn { name: "status".into(), type_oid: 25, type_modifier: -1, flags: 0 },
        ];

        assert!(!columns_differ(&cols1, &cols2));
        assert!(columns_differ(&cols1, &cols3));
    }
}