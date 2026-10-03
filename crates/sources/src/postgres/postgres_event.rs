//! PostgreSQL replication event handling.
//!
//! Processes pgoutput protocol messages from logical replication and
//! converts them to DeltaForge events.

use std::sync::Arc;

use bytes::Bytes;
use deltaforge_core::{
    CheckpointMeta, Event, Op, SourceBoundary, SourceError, SourceInfo,
    SourceItem, SourcePosition, SourceResult, Transaction,
};
use metrics::counter;
use pgwire_replication::{Lsn, client::ReplicationEvent};
use tracing::{debug, error, info, warn};

use common::watchdog;

use super::RunCtx;
use super::postgres_errors::{LoopControl, SchemaDrift};
use super::postgres_helpers::{
    make_checkpoint_meta, make_checkpoint_meta_str, pg_timestamp_to_unix_ms,
};
use super::postgres_logical_message;
use super::postgres_object::{RelationColumn, build_object, parse_tuple_data};
use super::postgres_table_schema::{
    FirstResolution, RelationIdentity, verify_first_resolution,
};

/// Relation metadata from pgoutput.
#[derive(Debug, Clone)]
pub struct RelationInfo {
    pub id: u32,
    pub schema: String,
    pub table: String,
    /// Pre-formatted "schema.table" for metrics labels (avoids per-event allocation).
    pub qualified_name: Arc<str>,
    pub columns: Arc<Vec<RelationColumn>>,
    /// Replica identity: d=default, n=nothing, f=full, i=index
    pub replica_identity: char,
}

/// Build the commit boundary: the resume checkpoint plus, atomically, the
/// durable watermark for the SAME COMMIT (frozen `system_identifier` + end_lsn).
/// The watermark is `None` when lineage is unavailable (`system_identifier == 0`)
/// - durable mode then fails closed at the sink; we never synthesize a lineage.
fn boundary_for_pg_commit(
    system_identifier: u64,
    end_lsn: &Lsn,
    tx_id: Option<u32>,
    checkpoint: CheckpointMeta,
) -> SourceBoundary {
    use crate::durable_checkpoint::{DurableWatermark, parse_lsn};
    let durable_watermark = if system_identifier != 0 {
        parse_lsn(&end_lsn.to_string()).map(|lsn| {
            Arc::from(
                DurableWatermark::pg_commit(system_identifier, lsn, tx_id)
                    .to_bytes(),
            )
        })
    } else {
        None
    };
    SourceBoundary {
        checkpoint,
        durable_watermark,
    }
}

/// Read next replication event with watchdog timeout.
pub(super) async fn read_next_event(
    ctx: &RunCtx,
) -> Result<Option<ReplicationEvent>, LoopControl> {
    let mut client = ctx.repl_client.lock().await;

    match watchdog(client.recv(), ctx.inactivity, &ctx.cancel, "repl_recv")
        .await
    {
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
            } else if let LoopControl::Fail(ref e) = control {
                counter!(
                    "deltaforge_source_errors_total",
                    "pipeline" => ctx.pipeline.clone(),
                    "source" => ctx.source_id.clone(),
                    "kind" => source_error_kind(e),
                )
                .increment(1);
            }
            Err(control)
        }
    }
}

/// Dispatch a replication event to appropriate handler.
/// Returns LoopControl to signal schema reload or fatal errors.
pub(super) async fn dispatch_event(
    ctx: &mut RunCtx,
    event: ReplicationEvent,
) -> Result<(), LoopControl> {
    match event {
        ReplicationEvent::XLogData {
            wal_start,
            wal_end,
            data,
            ..
        } => {
            debug!(wal_start = %wal_start, wal_end = %wal_end, bytes = data.len(), "xlog data");
            counter!(
                "deltaforge_source_bytes_total",
                "pipeline" => ctx.pipeline.clone(),
                "source" => ctx.source_id.clone(),
            )
            .increment(data.len() as u64);
            ctx.last_lsn = wal_end;
            handle_pgoutput_message(ctx, &data, wal_end).await?;
            // NOTE: the LSN confirmed to PostgreSQL (which releases retained WAL) is
            // driven separately from the durable per-sink checkpoint by the WAL-feedback
            // task (see `mod.rs`), never from the read position here - otherwise WAL for
            // events no sink has acknowledged would be released, losing them on restart.
        }
        ReplicationEvent::KeepAlive {
            wal_end,
            reply_requested,
            ..
        } => {
            debug!(wal_end = %wal_end, reply_requested, "keepalive");
            ctx.last_lsn = wal_end;
        }
        ReplicationEvent::Begin {
            final_lsn,
            commit_time_micros,
            xid,
        } => {
            debug!(final_lsn = %final_lsn, xid, "transaction begin");
            ctx.current_tx_id = Some(xid);
            ctx.current_tx_commit_time = Some(commit_time_micros);
            // The transaction's final LSN is the stable identity coordinate;
            // reset the per-transaction change ordinal exactly at BEGIN.
            ctx.current_final_lsn = Some(final_lsn.to_string());
            ctx.change_ordinal = 0;
            ctx.message_ordinal = 0;
            ctx.open_tx_events = 0;
            // Open the transaction on the coordinator's stream. tx_id matches the
            // xid stamped on this transaction's events and its TxCommit marker.
            let _ = ctx
                .tx
                .send(SourceItem::TxBegin {
                    tx_id: xid.to_string(),
                })
                .await;
        }
        ReplicationEvent::Commit { lsn, end_lsn, .. } => {
            debug!(commit_lsn = %lsn, end_lsn = %end_lsn, "transaction commit");
            ctx.last_lsn = end_lsn;
            // The COMMIT record is the transaction boundary: emit an explicit
            // marker carrying the commit-record checkpoint. tx_id matches the
            // xid stamped on this transaction's row events. (pgoutput never
            // decodes aborted transactions, so a rollback emits nothing here.)
            if let Some(tx_id) = ctx.current_tx_id {
                // Checkpoint and watermark BOTH describe this COMMIT boundary:
                // the COMMIT record's end_lsn (never the last row/message LSN)
                // and the frozen startup system_identifier lineage.
                let checkpoint =
                    make_checkpoint_meta(&end_lsn, ctx.current_tx_id);
                let boundary = boundary_for_pg_commit(
                    ctx.system_identifier,
                    &end_lsn,
                    ctx.current_tx_id,
                    checkpoint,
                );
                let _ = ctx
                    .tx
                    .send(SourceItem::TxCommit {
                        tx_id: tx_id.to_string(),
                        boundary,
                    })
                    .await;
            }
            ctx.current_tx_id = None;
            ctx.current_tx_commit_time = None;
            ctx.current_final_lsn = None;
            ctx.message_ordinal = 0;
            ctx.open_tx_events = 0;
            ctx.resume_lsn = end_lsn;
        }
        ReplicationEvent::StoppedAt { reached } => {
            info!(reached = %reached, "replication stopped at target LSN");
        }
        ReplicationEvent::Message {
            transactional,
            prefix,
            content,
            lsn,
        } => {
            debug!(
                prefix = %prefix, lsn = %lsn,
                transactional, bytes = content.len(),
                "logical decoding message"
            );

            // A transactional message is an identity-bearing change: it consumes
            // a change-ordinal slot so subsequent rows in the transaction do not
            // collide. (Non-transactional messages occur outside BEGIN/COMMIT
            // and have their own identity - they do not consume a tx ordinal.)
            if transactional {
                ctx.change_ordinal += 1;
            }
            // Message ordinal is assigned BEFORE filtering so a filtered message
            // never renumbers a retained one.
            let message_ordinal = ctx.message_ordinal;
            ctx.message_ordinal += 1;

            // The stable `msg` id is required - fail closed without lineage.
            if ctx.system_identifier == 0 {
                return Err(LoopControl::Fail(SourceError::Other(
                    anyhow::anyhow!(
                        "logical message identity: system_identifier unavailable"
                    ),
                )));
            }
            let msg_id = deltaforge_core::EventId::logical_message(
                &deltaforge_core::SourceLineage::Postgres {
                    system_identifier: ctx.system_identifier,
                },
                &lsn.to_string(),
                message_ordinal,
            );

            if let Some(event) = postgres_logical_message::to_event(
                msg_id,
                &prefix,
                &content,
                lsn,
                &ctx.pipeline,       // pipeline name
                &ctx.default_schema, // database name
                // Only a transactional message belongs to the open transaction; a
                // non-transactional message is its own boundary and must stay
                // unstamped so the coordinator keeps treating it as standalone.
                if transactional {
                    ctx.current_tx_id
                } else {
                    None
                },
                ctx.current_tx_commit_time,
                &ctx.outbox_prefixes,
            ) {
                ctx.tx.send(SourceItem::Event(event)).await.map_err(|e| {
                    LoopControl::Fail(SourceError::Other(e.into()))
                })?;
                if transactional {
                    ctx.open_tx_events += 1;
                }
            }

            ctx.last_lsn = lsn;
            // WAL confirmation to PostgreSQL is driven by the durable per-sink
            // checkpoint (see `mod.rs`), not the read position.
        }
    }
    Ok(())
}

/// Parse and handle pgoutput protocol messages.
async fn handle_pgoutput_message(
    ctx: &mut RunCtx,
    data: &Bytes,
    wal_lsn: Lsn,
) -> Result<(), LoopControl> {
    if data.is_empty() {
        return Ok(());
    }

    let msg_type = data[0];
    // Keep a Bytes handle for zero-copy slicing in DML handlers.
    let payload_bytes = data.slice(1..);
    let payload = payload_bytes.as_ref();

    // Identity-bearing changes consume a change ordinal *before* filtering, so
    // filtering one table cannot renumber later events. Protocol metadata
    // (Relation/Type/Origin/Begin/Commit) does not.
    match msg_type {
        b'R' => handle_relation(ctx, payload).await,
        b'I' => {
            let ordinal = ctx.change_ordinal;
            ctx.change_ordinal += 1;
            handle_insert(ctx, &payload_bytes, wal_lsn, ordinal)
                .await
                .map_err(LoopControl::Fail)
        }
        b'U' => {
            let ordinal = ctx.change_ordinal;
            ctx.change_ordinal += 1;
            handle_update(ctx, &payload_bytes, wal_lsn, ordinal)
                .await
                .map_err(LoopControl::Fail)
        }
        b'D' => {
            let ordinal = ctx.change_ordinal;
            ctx.change_ordinal += 1;
            handle_delete(ctx, &payload_bytes, wal_lsn, ordinal)
                .await
                .map_err(LoopControl::Fail)
        }
        b'T' => {
            let ordinal = ctx.change_ordinal;
            ctx.change_ordinal += 1;
            handle_truncate(ctx, payload, wal_lsn, ordinal)
                .await
                .map_err(LoopControl::Fail)
        }
        b'B' | b'C' => Ok(()), // Begin/Commit handled in ReplicationEvent
        b'O' => {
            debug!("origin message");
            Ok(())
        }
        b'Y' => {
            debug!("type message");
            Ok(())
        }
        b'M' => {
            debug!("logical message");
            Ok(())
        }
        _ => {
            debug!(msg_type = %msg_type, "unknown pgoutput message");
            Ok(())
        }
    }
}

/// Handle relation (table metadata) message.
///
/// Two drift paths, both signalling `LoopControl::SchemaDrift` before any row
/// under the changed schema is decoded:
/// - **In-stream**: the table was already mapped this run and its definition
///   changed (an ALTER while streaming) - compared against the in-memory map, no
///   catalog fetch.
/// - **First resolution (lazy restart detection)**: the first Relation for a
///   table this run is verified against the durably persisted schema, catching a
///   change made while the source was down (and re-detecting a previously
///   Halt-failed drift). A catalog fetch happens only for this active table, and
///   only when the Relation payload cannot settle the comparison on its own.
async fn handle_relation(
    ctx: &mut RunCtx,
    payload: &[u8],
) -> Result<(), LoopControl> {
    if payload.len() < 8 {
        return Ok(());
    }

    let relation_id =
        u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
    let mut offset = 4;

    let schema = read_cstring(payload, &mut offset);
    let table = read_cstring(payload, &mut offset);

    let replica_identity = if offset < payload.len() {
        payload[offset] as char
    } else {
        'd'
    };
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
    let col_count =
        u16::from_be_bytes([payload[offset], payload[offset + 1]]) as usize;
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
        let type_oid = u32::from_be_bytes([
            payload[offset],
            payload[offset + 1],
            payload[offset + 2],
            payload[offset + 3],
        ]);
        offset += 4;

        let type_modifier = i32::from_be_bytes([
            payload[offset],
            payload[offset + 1],
            payload[offset + 2],
            payload[offset + 3],
        ]);
        offset += 4;

        columns.push(RelationColumn {
            name,
            type_oid,
            type_modifier,
            flags,
        });
    }

    // Check if this relation already exists and if schema changed. Capture the
    // prior columns before the map is updated so a drift can be described.
    let existing = ctx.relation_map.get(&relation_id);
    let is_new = existing.is_none();
    let old_columns: Option<Vec<RelationColumn>> =
        existing.map(|r| r.columns.to_vec());
    let schema_changed = old_columns
        .as_ref()
        .map(|old| old.len() != columns.len() || columns_differ(old, &columns))
        .unwrap_or(false);
    // Describe the change before `columns` is moved into the relation map.
    let drift_detail = if schema_changed {
        Some(describe_schema_change(
            old_columns.as_deref().unwrap_or(&[]),
            &columns,
        ))
    } else {
        None
    };

    // The (name, type_oid) signature in pgoutput order, captured before `columns`
    // is moved into the relation map (used for the first-resolution comparison
    // below - same fields the in-stream path compares).
    let relation_signature: Vec<(String, u32)> = columns
        .iter()
        .map(|c| (c.name.clone(), c.type_oid))
        .collect();

    // Update relation map with new column info
    let qualified_name: Arc<str> = format!("{schema}.{table}").into();
    ctx.relation_map.insert(
        relation_id,
        RelationInfo {
            id: relation_id,
            schema: schema.clone(),
            table: table.clone(),
            qualified_name,
            columns: Arc::new(columns),
            replica_identity,
        },
    );

    if !ctx.allow.matches(&schema, &table) {
        return Ok(());
    }
    if is_new {
        info!(relation_id, schema = %schema, table = %table, "relation mapped");
    } else {
        debug!(relation_id, schema = %schema, table = %table, "relation re-mapped");
    }

    // In-stream drift: the table was already mapped this run and changed.
    if schema_changed {
        let detail = drift_detail.unwrap_or_default();
        info!(
            relation_id, schema = %schema, table = %table, change = %detail,
            "in-stream schema drift detected"
        );
        return Err(LoopControl::SchemaDrift(SchemaDrift {
            schema,
            table,
            detail,
        }));
    }

    // First resolution this run: verify the Relation against the durably persisted
    // schema so a change made while the source was down (or a previously
    // Halt-failed drift) is caught here, before this table's rows are decoded.
    // Read from durable history (single-flight per key), never from a loader
    // cache: a cold or contended cache cannot skip it. No history = first
    // use; unreadable history fails closed. No catalog query.
    if is_new {
        let persisted = ctx
            .schema
            .persisted(&schema, &table)
            .await
            .map_err(LoopControl::Fail)?;
        if let Some(persisted) = persisted {
            let persisted_signature = persisted.signature();
            if let FirstResolution::Drift(detail) = verify_first_resolution(
                &persisted_signature,
                &relation_signature,
            ) {
                info!(
                    relation_id, schema = %schema, table = %table, change = %detail,
                    "restart schema drift detected on first resolution"
                );
                return Err(LoopControl::SchemaDrift(SchemaDrift {
                    schema,
                    table,
                    detail,
                }));
            }
        }
        // First use accepted: its schema matches the durable one (or it is
        // new).
        ctx.drift_resolver
            .accepted(&format!("{schema}.{table}"))
            .await;
    }

    Ok(())
}

/// Describe a relation-definition change as `old cols -> new cols`, for the
/// drift log line and the Halt error message.
fn describe_schema_change(
    old: &[RelationColumn],
    new: &[RelationColumn],
) -> String {
    fn cols(c: &[RelationColumn]) -> String {
        c.iter()
            .map(|c| format!("{}:{}", c.name, c.type_oid))
            .collect::<Vec<_>>()
            .join(", ")
    }
    format!("columns [{}] -> [{}]", cols(old), cols(new))
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

/// Static version string - allocated once, not per event.
static PG_VERSION: std::sync::LazyLock<String> =
    std::sync::LazyLock::new(|| {
        concat!("deltaforge-", env!("CARGO_PKG_VERSION")).to_string()
    });

/// Build SourceInfo for PostgreSQL events.
///
/// Caches the formatted LSN string to avoid re-formatting when consecutive
/// events share the same WAL position (common within a transaction).
#[allow(clippy::too_many_arguments)]
fn build_source_info(
    ctx: &mut RunCtx,
    wal_lsn: &Lsn,
    schema: &str,
    table: &str,
    timestamp_ms: i64,
    relation_oid: u32,
    change_ordinal: u32,
) -> SourceResult<(SourceInfo, deltaforge_core::EventId)> {
    // Cache the LSN string - only reformat when it changes.
    let lsn_str = match &ctx.cached_lsn {
        Some((cached, s)) if cached == wal_lsn => s.clone(),
        _ => {
            let s = wal_lsn.to_string();
            ctx.cached_lsn = Some((*wal_lsn, s.clone()));
            s
        }
    };

    let source = SourceInfo {
        version: PG_VERSION.clone(),
        connector: "postgresql".to_string(),
        name: ctx.pipeline.clone(),
        ts_ms: timestamp_ms,
        db: ctx.default_schema.clone(),
        schema: Some(schema.to_string()),
        table: table.to_string(),
        snapshot: None,
        position: {
            let mut p = SourcePosition::postgres(
                lsn_str,
                ctx.current_tx_id.map(|id| id as i64),
                None,
            );
            // Immutable stable-identity coordinates.
            p.tx_final_lsn = ctx.current_final_lsn.clone();
            p.relation_oid = Some(relation_oid);
            p.change_ordinal = Some(change_ordinal);
            p
        },
    };
    // The stable id is required at the source boundary - fail closed if the
    // system_identifier is missing or the row has no active transaction.
    if ctx.system_identifier == 0 {
        return Err(SourceError::Other(anyhow::anyhow!(
            "pg row identity: system_identifier unavailable"
        )));
    }
    let id = super::pg_row_event_id(&source, ctx.system_identifier)
        .map_err(|e| SourceError::Other(anyhow::anyhow!(e)))?;
    Ok((source, id))
}

/// Handle INSERT message.
async fn handle_insert(
    ctx: &mut RunCtx,
    payload_bytes: &Bytes,
    wal_lsn: Lsn,
    change_ordinal: u32,
) -> SourceResult<()> {
    let payload = payload_bytes.as_ref();
    if payload.len() < 5 {
        return Ok(());
    }

    let relation_id =
        u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
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

    // Extract all needed data from relation upfront to release the borrow.
    let columns = Arc::clone(&relation.columns);
    let replica_identity = relation.replica_identity;
    let qualified_name = Arc::clone(&relation.qualified_name);
    let schema = relation.schema.clone();
    let table = relation.table.clone();

    let rel_identity = RelationIdentity {
        oid: relation_id,
        signature: columns
            .iter()
            .map(|c| (c.name.clone(), c.type_oid))
            .collect(),
        replica_identity,
    };
    let loaded = ctx
        .schema
        .load_schema_for_relation(&schema, &table, None, &rel_identity)
        .await?;

    let tuple_data = payload_bytes.slice(5..);
    let (values, _) = parse_tuple_data(&tuple_data, columns.len());
    let after = build_object(&columns, &values);

    let timestamp_ms = ctx
        .current_tx_commit_time
        .map(pg_timestamp_to_unix_ms)
        .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());

    let (source_info, event_id) = build_source_info(
        ctx,
        &wal_lsn,
        &schema,
        &table,
        timestamp_ms,
        relation_id,
        change_ordinal,
    )?;
    let lsn_str = &ctx.cached_lsn.as_ref().unwrap().1;
    let chkpt = make_checkpoint_meta_str(lsn_str, ctx.current_tx_id);
    let mut ev = Event::new_row(
        event_id,
        source_info,
        Op::Create,
        None,
        Some(after),
        timestamp_ms,
        payload.len(),
    )
    .with_tenant(ctx.tenant.clone())
    .with_checkpoint(chkpt);

    if let Some(tx_id) = ctx.current_tx_id {
        ev.transaction = Some(Transaction {
            id: tx_id.to_string(),
            total_order: None,
            data_collection_order: None,
        });
    }

    ev.schema_version = Some(loaded.fingerprint.to_string());
    ev.schema_sequence = Some(loaded.sequence);

    send_event(ctx, ev, &qualified_name, "c").await;
    Ok(())
}

/// Handle UPDATE message.
async fn handle_update(
    ctx: &mut RunCtx,
    payload_bytes: &Bytes,
    wal_lsn: Lsn,
    change_ordinal: u32,
) -> SourceResult<()> {
    let payload = payload_bytes.as_ref();
    if payload.len() < 5 {
        return Ok(());
    }

    let relation_id =
        u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
    let mut offset = 4;

    let Some(relation) = ctx.relation_map.get(&relation_id) else {
        warn!(relation_id, "update for unknown relation");
        return Ok(());
    };

    if !ctx.allow.matches(&relation.schema, &relation.table) {
        return Ok(());
    }

    let columns = Arc::clone(&relation.columns);
    let replica_identity = relation.replica_identity;
    let qualified_name = Arc::clone(&relation.qualified_name);
    let schema = relation.schema.clone();
    let table = relation.table.clone();

    let mut before_values = None;
    let mut after_values = None;

    while offset < payload.len() {
        let marker = payload[offset];
        offset += 1;

        match marker {
            b'K' | b'O' => {
                let tuple_data = payload_bytes.slice(offset..);
                let (values, consumed) =
                    parse_tuple_data(&tuple_data, columns.len());
                before_values = Some(values);
                offset += consumed;
            }
            b'N' => {
                let tuple_data = payload_bytes.slice(offset..);
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

    let rel_identity = RelationIdentity {
        oid: relation_id,
        signature: columns
            .iter()
            .map(|c| (c.name.clone(), c.type_oid))
            .collect(),
        replica_identity,
    };
    let loaded = ctx
        .schema
        .load_schema_for_relation(&schema, &table, None, &rel_identity)
        .await?;

    let before = before_values.map(|v| build_object(&columns, &v));
    let after = build_object(&columns, &after_vals);

    let timestamp_ms = ctx
        .current_tx_commit_time
        .map(pg_timestamp_to_unix_ms)
        .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());

    let (source_info, event_id) = build_source_info(
        ctx,
        &wal_lsn,
        &schema,
        &table,
        timestamp_ms,
        relation_id,
        change_ordinal,
    )?;
    let lsn_str = &ctx.cached_lsn.as_ref().unwrap().1;
    let chkpt = make_checkpoint_meta_str(lsn_str, ctx.current_tx_id);
    let mut ev = Event::new_row(
        event_id,
        source_info,
        Op::Update,
        before,
        Some(after),
        timestamp_ms,
        payload.len(),
    )
    .with_tenant(ctx.tenant.clone())
    .with_checkpoint(chkpt);

    if let Some(tx_id) = ctx.current_tx_id {
        ev.transaction = Some(Transaction {
            id: tx_id.to_string(),
            total_order: None,
            data_collection_order: None,
        });
    }

    ev.schema_version = Some(loaded.fingerprint.to_string());
    ev.schema_sequence = Some(loaded.sequence);

    send_event(ctx, ev, &qualified_name, "u").await;
    Ok(())
}

/// Handle DELETE message.
async fn handle_delete(
    ctx: &mut RunCtx,
    payload_bytes: &Bytes,
    wal_lsn: Lsn,
    change_ordinal: u32,
) -> SourceResult<()> {
    let payload = payload_bytes.as_ref();
    if payload.len() < 5 {
        return Ok(());
    }

    let relation_id =
        u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
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

    let columns = Arc::clone(&relation.columns);
    let replica_identity = relation.replica_identity;
    let qualified_name = Arc::clone(&relation.qualified_name);
    let schema = relation.schema.clone();
    let table = relation.table.clone();

    let rel_identity = RelationIdentity {
        oid: relation_id,
        signature: columns
            .iter()
            .map(|c| (c.name.clone(), c.type_oid))
            .collect(),
        replica_identity,
    };
    let loaded = ctx
        .schema
        .load_schema_for_relation(&schema, &table, None, &rel_identity)
        .await?;

    let tuple_data = payload_bytes.slice(5..);
    let (values, _) = parse_tuple_data(&tuple_data, columns.len());
    let before = build_object(&columns, &values);

    let timestamp_ms = ctx
        .current_tx_commit_time
        .map(pg_timestamp_to_unix_ms)
        .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());

    let (source_info, event_id) = build_source_info(
        ctx,
        &wal_lsn,
        &schema,
        &table,
        timestamp_ms,
        relation_id,
        change_ordinal,
    )?;
    let lsn_str = &ctx.cached_lsn.as_ref().unwrap().1;
    let chkpt = make_checkpoint_meta_str(lsn_str, ctx.current_tx_id);
    let mut ev = Event::new_row(
        event_id,
        source_info,
        Op::Delete,
        Some(before),
        None,
        timestamp_ms,
        payload.len(),
    )
    .with_tenant(ctx.tenant.clone())
    .with_checkpoint(chkpt);

    if let Some(tx_id) = ctx.current_tx_id {
        ev.transaction = Some(Transaction {
            id: tx_id.to_string(),
            total_order: None,
            data_collection_order: None,
        });
    }

    ev.schema_version = Some(loaded.fingerprint.to_string());
    ev.schema_sequence = Some(loaded.sequence);

    send_event(ctx, ev, &qualified_name, "d").await;
    Ok(())
}

/// Handle TRUNCATE message.
async fn handle_truncate(
    ctx: &mut RunCtx,
    payload: &[u8],
    wal_lsn: Lsn,
    change_ordinal: u32,
) -> SourceResult<()> {
    if payload.len() < 9 {
        return Ok(());
    }

    let relation_count =
        u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
    let options = payload[4];
    let cascade = (options & 1) != 0;
    let restart_identity = (options & 2) != 0;

    let mut offset = 5;
    let mut tables = Vec::with_capacity(relation_count as usize);

    for _ in 0..relation_count {
        if offset + 4 > payload.len() {
            break;
        }
        let rel_id = u32::from_be_bytes([
            payload[offset],
            payload[offset + 1],
            payload[offset + 2],
            payload[offset + 3],
        ]);
        offset += 4;

        if let Some(rel) = ctx.relation_map.get(&rel_id) {
            tables.push((rel_id, rel.schema.clone(), rel.table.clone()));
        }
    }

    info!(tables = ?tables, cascade, restart_identity, lsn = %wal_lsn, "truncate received");

    let timestamp_ms = chrono::Utc::now().timestamp_millis();

    for (rel_id, schema, table) in &tables {
        let source_info = SourceInfo {
            version: concat!("deltaforge-", env!("CARGO_PKG_VERSION"))
                .to_string(),
            connector: "postgresql".to_string(),
            name: ctx.pipeline.clone(),
            ts_ms: timestamp_ms,
            db: ctx.default_schema.clone(),
            schema: Some(schema.clone()),
            table: table.clone(),
            snapshot: None,
            position: {
                let mut p = SourcePosition::postgres(
                    wal_lsn.to_string(),
                    ctx.current_tx_id.map(|id| id as i64),
                    None,
                );
                // One ordinal for the truncate message; relation OID
                // disambiguates the truncated tables.
                p.tx_final_lsn = ctx.current_final_lsn.clone();
                p.relation_oid = Some(*rel_id);
                p.change_ordinal = Some(change_ordinal);
                p
            },
        };

        // Truncate carries row-like identity coordinates (relation OID +
        // per-tx change ordinal), so its id is a `pgrow`. Required - fail closed.
        if ctx.system_identifier == 0 {
            return Err(SourceError::Other(anyhow::anyhow!(
                "truncate identity: system_identifier unavailable"
            )));
        }
        let truncate_id =
            super::pg_row_event_id(&source_info, ctx.system_identifier)
                .map_err(|e| SourceError::Other(anyhow::anyhow!(e)))?;

        let ddl_payload = serde_json::json!({
            "sql": "TRUNCATE",
            "cascade": cascade,
            "restart_identity": restart_identity,
        });

        let mut ev = Event::new_ddl(
            truncate_id,
            source_info,
            ddl_payload,
            timestamp_ms,
            0,
        )
        .with_tenant(ctx.tenant.clone())
        .with_checkpoint(make_checkpoint_meta(&wal_lsn, ctx.current_tx_id));

        if let Some(tx_id) = ctx.current_tx_id {
            ev.transaction = Some(Transaction {
                id: tx_id.to_string(),
                total_order: None,
                data_collection_order: None,
            });
        }

        if ctx.tx.send(SourceItem::Event(ev)).await.is_ok()
            && ctx.current_tx_id.is_some()
        {
            ctx.open_tx_events += 1;
        }
    }

    Ok(())
}

/// Send event and update metrics.
///
/// Uses `try_send` (non-blocking) when the channel has capacity to avoid
/// the overhead of the async state machine on every single row. Falls back
/// to the async `send` only when the channel is full (backpressure).
/// Counter handles are cached per (table, op) to avoid hash lookups per event.
#[inline]
async fn send_event(
    ctx: &mut RunCtx,
    ev: Event,
    table_name: &Arc<str>,
    op: &'static str,
) {
    let ok = match ctx.tx.try_send(SourceItem::Event(ev)) {
        Ok(()) => true,
        Err(tokio::sync::mpsc::error::TrySendError::Full(item)) => {
            ctx.tx.send(item).await.is_ok()
        }
        Err(_) => false,
    };
    if ok {
        if ctx.current_tx_id.is_some() {
            ctx.open_tx_events += 1;
        }
        let key = (Arc::clone(table_name), op);
        let ctr = ctx.counter_cache.entry(key).or_insert_with_key(|k| {
            counter!(
                "deltaforge_source_events_total",
                "pipeline" => ctx.pipeline.clone(),
                "source" => ctx.source_id.clone(),
                "table" => k.0.to_string(),
                "op" => k.1,
            )
        });
        ctr.increment(1);
    } else {
        error!(source_id = %ctx.source_id, op, "channel send failed");
    }
}

fn source_error_kind(e: &SourceError) -> &'static str {
    match e.root() {
        SourceError::Auth { .. } => "auth",
        SourceError::Connect { .. } => "connect",
        SourceError::Checkpoint { .. } => "checkpoint",
        SourceError::Schema { .. } => "schema",
        SourceError::Lineage { .. } => "lineage",
        SourceError::Incompatible { .. } => "incompatible",
        SourceError::Permission { .. } => "permission",
        SourceError::NotFound { .. } => "not_found",
        SourceError::Io(_) => "io",
        SourceError::Timeout { .. } => "timeout",
        SourceError::Backpressure => "backpressure",
        SourceError::Cancelled => "cancelled",
        SourceError::Other(_) => "other",
        SourceError::Incident { .. } => unreachable!("root unwraps incidents"),
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

    mod verify_first_resolution_tests {
        use super::super::{FirstResolution, verify_first_resolution};

        // (name, type_oid) persisted signature helper.
        fn persisted(cols: &[(&str, u32)]) -> Vec<(String, Option<u32>)> {
            cols.iter()
                .map(|(n, o)| (n.to_string(), Some(*o)))
                .collect()
        }
        // pgoutput Relation signature helper.
        fn relation(cols: &[(&str, u32)]) -> Vec<(String, u32)> {
            cols.iter().map(|(n, o)| (n.to_string(), *o)).collect()
        }

        // Same names/order but a changed type OID is drift - detected purely from
        // cached state, with no catalog query (the function takes no connection).
        #[test]
        fn same_names_changed_type_oid_is_drift() {
            let out = verify_first_resolution(
                &persisted(&[("id", 23), ("sku", 1043)]),
                &relation(&[("id", 23), ("sku", 25)]), // varchar -> text
            );
            assert!(matches!(out, FirstResolution::Drift(_)));
        }

        // Added / removed / reordered columns are drift.
        #[test]
        fn structural_changes_are_drift() {
            // added
            assert!(matches!(
                verify_first_resolution(
                    &persisted(&[("id", 23), ("sku", 1043)]),
                    &relation(&[("id", 23), ("sku", 1043), ("status", 1043)]),
                ),
                FirstResolution::Drift(_)
            ));
            // removed
            assert!(matches!(
                verify_first_resolution(
                    &persisted(&[("id", 23), ("sku", 1043)]),
                    &relation(&[("id", 23)]),
                ),
                FirstResolution::Drift(_)
            ));
            // reordered
            assert!(matches!(
                verify_first_resolution(
                    &persisted(&[("id", 23), ("sku", 1043)]),
                    &relation(&[("sku", 1043), ("id", 23)]),
                ),
                FirstResolution::Drift(_)
            ));
        }

        // Identical persisted and relation signatures are not drift.
        #[test]
        fn identical_signatures_no_drift() {
            let out = verify_first_resolution(
                &persisted(&[("id", 23), ("sku", 1043)]),
                &relation(&[("id", 23), ("sku", 1043)]),
            );
            assert_eq!(out, FirstResolution::NoDrift);
        }

        // A persisted column without a type OID is unverifiable: it must fail
        // closed as drift, never be treated as "unchanged".
        #[test]
        fn missing_persisted_type_oid_fails_closed_unverifiable() {
            let out = verify_first_resolution(
                &[("id".into(), Some(23)), ("sku".into(), None)],
                &relation(&[("id", 23), ("sku", 1043)]),
            );
            match out {
                FirstResolution::Drift(detail) => {
                    assert!(
                        detail.contains("unavailable"),
                        "unverifiable reason surfaced: {detail}"
                    );
                }
                FirstResolution::NoDrift => {
                    panic!("missing type OID must not read as unchanged")
                }
            }
        }
    }

    /// The TxCommit marker carries a boundary built from the COMMIT record's
    /// end_lsn and the frozen system_identifier lineage: the checkpoint passes
    /// through and a durable watermark is produced when lineage is available.
    #[test]
    fn pg_commit_boundary_uses_commit_lsn_and_lineage() {
        let end_lsn = Lsn::from(0x00AB_CDEFu64);
        let cp = CheckpointMeta::from_vec(b"cp".to_vec());
        let boundary = boundary_for_pg_commit(42, &end_lsn, Some(7), cp);
        assert_eq!(boundary.checkpoint.as_bytes(), b"cp");
        assert!(
            boundary.durable_watermark.is_some(),
            "a known lineage must produce a durable watermark"
        );
    }

    /// `system_identifier == 0` means lineage is unavailable: fail closed with no
    /// synthesized watermark (durable mode then fails at the sink, never guesses).
    #[test]
    fn pg_commit_boundary_without_lineage_has_no_watermark() {
        let end_lsn = Lsn::from(0x00AB_CDEFu64);
        let cp = CheckpointMeta::from_vec(b"cp".to_vec());
        let boundary = boundary_for_pg_commit(0, &end_lsn, Some(7), cp);
        assert!(
            boundary.durable_watermark.is_none(),
            "missing lineage must not synthesize a watermark"
        );
    }

    #[test]
    fn test_read_cstring() {
        let data = b"hello\0world\0";
        let mut offset = 0;
        assert_eq!(read_cstring(data, &mut offset), "hello");
        assert_eq!(offset, 6);
        assert_eq!(read_cstring(data, &mut offset), "world");
    }

    #[test]
    fn read_cstring_without_terminator_stops_at_end() {
        // No trailing NUL: the scan must stop exactly at data.len() and NOT
        // advance past it (pins the two `<` bounds against `<=`, which would
        // over-read/panic or push offset beyond the buffer).
        let data = b"abc";
        let mut offset = 0;
        assert_eq!(read_cstring(data, &mut offset), "abc");
        assert_eq!(offset, 3);
    }

    #[test]
    fn columns_differ_on_name_or_oid_change() {
        let base = vec![RelationColumn {
            name: "id".into(),
            type_oid: 23,
            type_modifier: -1,
            flags: 1,
        }];
        // Name differs only → differ (pins `||`: `&&` would need oid to differ too).
        let name_only = vec![RelationColumn {
            name: "ID".into(),
            type_oid: 23,
            type_modifier: -1,
            flags: 1,
        }];
        assert!(columns_differ(&base, &name_only));
        // OID differs only → differ.
        let oid_only = vec![RelationColumn {
            name: "id".into(),
            type_oid: 25,
            type_modifier: -1,
            flags: 1,
        }];
        assert!(columns_differ(&base, &oid_only));
        // Identical → no difference.
        assert!(!columns_differ(&base, &base.clone()));
        // Length mismatch → differ.
        assert!(columns_differ(&base, &[]));
    }

    #[test]
    fn test_relation_column_is_key() {
        let key_col = RelationColumn {
            name: "id".into(),
            type_oid: 23,
            type_modifier: -1,
            flags: 1,
        };
        assert!(key_col.is_key());

        let non_key_col = RelationColumn {
            name: "name".into(),
            type_oid: 25,
            type_modifier: -1,
            flags: 0,
        };
        assert!(!non_key_col.is_key());
    }

    #[test]
    fn test_columns_differ() {
        let cols1 = vec![
            RelationColumn {
                name: "id".into(),
                type_oid: 23,
                type_modifier: -1,
                flags: 1,
            },
            RelationColumn {
                name: "name".into(),
                type_oid: 25,
                type_modifier: -1,
                flags: 0,
            },
        ];

        let cols2 = vec![
            RelationColumn {
                name: "id".into(),
                type_oid: 23,
                type_modifier: -1,
                flags: 1,
            },
            RelationColumn {
                name: "name".into(),
                type_oid: 25,
                type_modifier: -1,
                flags: 0,
            },
        ];

        let cols3 = vec![
            RelationColumn {
                name: "id".into(),
                type_oid: 23,
                type_modifier: -1,
                flags: 1,
            },
            RelationColumn {
                name: "name".into(),
                type_oid: 25,
                type_modifier: -1,
                flags: 0,
            },
            RelationColumn {
                name: "status".into(),
                type_oid: 25,
                type_modifier: -1,
                flags: 0,
            },
        ];

        assert!(!columns_differ(&cols1, &cols2));
        assert!(columns_differ(&cols1, &cols3));
    }
}
