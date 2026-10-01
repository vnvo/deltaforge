//! Read-only proofs over the retained binlog (design spec 7.7, 7.14, 7.15).
//!
//! - [`scan_interval`] reads the complete retained interval `(from, to]` on
//!   its own replication session, after proving the server's identity on
//!   that same session before the dump command, and classifies every
//!   statement with the approved DDL classifier. It succeeds only when it
//!   provably reached `to` exactly; anything else fails closed: positions that
//!   cannot be ordered or are out of order, another server, a purged or
//!   unreadable interval, malformed or unsupported events (an `INCIDENT` marks
//!   a gap), a framing error (incl. a transaction without a GTID in a GTID
//!   interval), passing `to` without reaching it, or a resource limit (a
//!   guard, never "no DDL").
//! - [`stable_capture`] reads the server position, one table's shape and the
//!   position again on ONE connection, after proving the server's identity on
//!   it, and accepts only equal positions; bounded retries, then fail closed.
//!
//! The scan digest is deterministic for an interval: it binds the classifier
//! version, both boundaries and every classified statement with its binlog
//! identity. Event and byte counts are reported but not hashed (heartbeats
//! and start-of-stream events can differ between two scans of one interval).

// Used by the activation wiring (forward proof, baseline) in the next commit;
// this allow is removed there.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_binlog_connector_rust::binlog_client::BinlogClient;
use mysql_binlog_connector_rust::event::event_data::EventData;
use mysql_binlog_connector_rust::event::event_header::EventHeader;
use sha2::{Digest, Sha256};

use super::MySqlCheckpoint;
use super::mysql_activation::EventIdentity;
use super::mysql_ddl_attribution::{BarrierScopeOf, DdlEffect, classify};
use super::mysql_schema_loader::{Live, fetch_table_schema_on};
use super::mysql_session::{SessionError, open_replication_session};
use super::mysql_table_schema::MySqlTableSchema;
use crate::durable_checkpoint::{
    Intervals, WmPos, gtid_subseteq, merge_intervals,
    mysql_checkpoint_position, order_positions, parse_gtid_set,
};
use deltaforge_core::CheckpointOrder;

/// Bumped when the classifier's output for a statement can change.
pub(crate) const CLASSIFIER_VERSION: &str = "ddl-attribution-v1";
const SCAN_DOMAIN: &[u8] = b"DeltaForge.BinlogScan.v1\0";

/// Resource guard for a scan. Reaching it fails the scan closed.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ScanLimits {
    pub max_events: u64,
    pub max_bytes: u64,
    pub max_duration: Duration,
}

impl Default for ScanLimits {
    fn default() -> Self {
        Self {
            max_events: 50_000_000,
            max_bytes: 64 * 1024 * 1024 * 1024,
            max_duration: Duration::from_secs(600),
        }
    }
}

/// One classified statement found in the interval.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Statement {
    pub event: EventIdentity,
    pub effect: DdlEffect,
}

/// A completed scan of `(from, to]`.
#[derive(Debug, Clone)]
pub(crate) struct ScanReport {
    /// Every statement that is not a no-op for table shapes, in binlog order.
    pub statements: Vec<Statement>,
    pub events: u64,
    pub bytes: u64,
    pub duration: Duration,
    pub digest: String,
}

/// Why a scan or capture failed (always fail-closed).
#[derive(Debug, thiserror::Error)]
pub(crate) enum ProofError {
    #[error("positions cannot be used: {0}")]
    Positions(String),
    #[error("the server is not the verified lineage: {0}")]
    OtherServer(String),
    #[error(
        "the interval cannot be opened (retention loss or connection failure): {0}"
    )]
    Open(String),
    #[error("reading the interval failed: {0}")]
    Read(String),
    #[error("binlog INCIDENT event: the binlog declares a gap")]
    Incident,
    #[error("unsupported binlog event type {0}")]
    UnsupportedEvent(u8),
    #[error("binlog framing error: {0}")]
    Framing(String),
    #[error("the scan passed the end position without reaching it: {0}")]
    Overshoot(String),
    #[error("the end position was not reached: {0}")]
    NotReached(String),
    #[error("scan resource limit reached ({0}); the interval is unproven")]
    Limit(String),
    #[error("no stable position could be captured after {0} attempts")]
    Unstable(u32),
    #[error("schema capture failed: {0}")]
    Capture(String),
}

type GtidSet = BTreeMap<String, Intervals>;

fn add_gtid(set: &mut GtidSet, gtid: &str) -> Result<(), ProofError> {
    let (uuid, gno) = gtid
        .rsplit_once(':')
        .and_then(|(u, n)| Some((u.trim(), n.trim().parse::<u64>().ok()?)))
        .filter(|(_, n)| *n > 0)
        .ok_or_else(|| {
            ProofError::Framing(format!("malformed GTID {gtid:?}"))
        })?;
    let ivs = set.entry(uuid.to_string()).or_default();
    ivs.push((gno, gno));
    merge_intervals(ivs);
    Ok(())
}

fn single(gtid: &str) -> Result<GtidSet, ProofError> {
    let mut s = GtidSet::new();
    add_gtid(&mut s, gtid)?;
    Ok(s)
}

fn lp(h: &mut Sha256, bytes: &[u8]) {
    h.update((bytes.len() as u64).to_be_bytes());
    h.update(bytes);
}

fn encode_effect(e: &DdlEffect) -> String {
    let tables = |v: &[super::mysql_ddl_attribution::TableName]| {
        v.iter()
            .map(|t| format!("{}\u{1f}{}", t.db, t.table))
            .collect::<Vec<_>>()
            .join("\u{1e}")
    };
    match e {
        DdlEffect::None => "none".into(),
        DdlEffect::Tables(v) => format!("tables\u{1d}{}", tables(v)),
        DdlEffect::SameShape(v) => format!("same\u{1d}{}", tables(v)),
        DdlEffect::Barrier(BarrierScopeOf::Lineage) => "barrier-lineage".into(),
        DdlEffect::Barrier(BarrierScopeOf::Database(d)) => {
            format!("barrier-db\u{1d}{d}")
        }
    }
}

fn encode_event(e: &EventIdentity) -> String {
    match e {
        EventIdentity::Gtid { gtid, ordinal } => {
            format!("gtid\u{1d}{gtid}\u{1d}{ordinal}")
        }
        EventIdentity::FilePos { file, end_pos } => {
            format!("file\u{1d}{file}\u{1d}{end_pos}")
        }
        // Never produced by a scan (statements always have an event).
        EventIdentity::StreamStart { position } => format!(
            "start\u{1d}{}",
            serde_json::to_string(position).expect("position serializes")
        ),
    }
}

/// The deterministic digest of a scan.
pub(crate) fn scan_digest(
    from: &WmPos,
    to: &WmPos,
    statements: &[Statement],
) -> String {
    let mut h = Sha256::new();
    h.update(SCAN_DOMAIN);
    lp(&mut h, CLASSIFIER_VERSION.as_bytes());
    lp(
        &mut h,
        &serde_json::to_vec(from).expect("position serializes"),
    );
    lp(
        &mut h,
        &serde_json::to_vec(to).expect("position serializes"),
    );
    lp(&mut h, &(statements.len() as u64).to_be_bytes());
    for s in statements {
        lp(&mut h, encode_event(&s.event).as_bytes());
        lp(&mut h, encode_effect(&s.effect).as_bytes());
    }
    hex::encode(h.finalize())
}

const ANONYMOUS_GTID: u8 = 34;

/// Event types the connector does not parse that cannot change table shapes
/// or hide a gap. ANONYMOUS_GTID only in file/position mode: a GTID scan
/// refuses it before this list is consulted.
fn harmless_unparsed(event_type: u8) -> bool {
    matches!(
        event_type,
        3   // STOP
        | 5  // INTVAR
        | 13 // RAND
        | 14 // USER_VAR
        | 28 // IGNORABLE
        | 34 // ANONYMOUS_GTID
        | 36 // TRANSACTION_CONTEXT
        | 37 // VIEW_CHANGE
        | 39 // PARTIAL_UPDATE_ROWS
    )
}

struct Walk {
    gtid_mode: bool,
    to_set: Option<GtidSet>,
    done: GtidSet,
    current_gtid: Option<String>,
    ordinal: u32,
    in_explicit_txn: bool,
    file: String,
    statements: Vec<Statement>,
    reached: bool,
}

impl Walk {
    fn complete_txn(&mut self) -> Result<(), ProofError> {
        self.in_explicit_txn = false;
        if self.gtid_mode {
            // A transaction without a GTID cannot be placed in the interval.
            let Some(g) = self.current_gtid.take() else {
                return Err(ProofError::Framing(
                    "a transaction without a GTID in a GTID interval".into(),
                ));
            };
            let to = self.to_set.as_ref().expect("gtid mode has a target set");
            if !gtid_subseteq(&single(&g)?, to) {
                return Err(ProofError::Overshoot(format!(
                    "transaction {g} is not part of the end position"
                )));
            }
            add_gtid(&mut self.done, &g)?;
            if gtid_subseteq(to, &self.done) {
                self.reached = true;
            }
        }
        Ok(())
    }

    fn identity(&self, header: &EventHeader) -> EventIdentity {
        match (&self.current_gtid, self.gtid_mode) {
            (Some(g), true) => EventIdentity::Gtid {
                gtid: g.clone(),
                ordinal: self.ordinal,
            },
            _ => EventIdentity::FilePos {
                file: self.file.clone(),
                end_pos: header.next_event_position as u64,
            },
        }
    }

    fn event(
        &mut self,
        header: &EventHeader,
        data: &EventData,
    ) -> Result<(), ProofError> {
        match data {
            EventData::Rotate(r) => self.file = r.binlog_filename.clone(),
            EventData::Gtid(g) => {
                if self.current_gtid.is_some() {
                    return Err(ProofError::Framing(format!(
                        "GTID {} arrived inside an open transaction",
                        g.gtid
                    )));
                }
                self.current_gtid = Some(g.gtid.clone());
                self.ordinal = 0;
                self.in_explicit_txn = false;
            }
            EventData::Query(q) => {
                self.ordinal += 1;
                let upper = q.query.trim().to_ascii_uppercase();
                if upper == "BEGIN" || upper.starts_with("XA START") {
                    self.in_explicit_txn = true;
                } else if upper == "COMMIT" || upper == "ROLLBACK" {
                    self.complete_txn()?;
                } else {
                    let effect = classify(&q.query, Some(&q.schema));
                    if effect != DdlEffect::None {
                        let event = self.identity(header);
                        self.statements.push(Statement { event, effect });
                    }
                    if !self.in_explicit_txn {
                        self.complete_txn()?;
                    }
                }
            }
            EventData::Xid(_) | EventData::XaPrepare(_) => {
                self.complete_txn()?
            }
            EventData::TransactionPayload(tp) => {
                // Inner events have no binlog position of their own; they
                // all end where the payload ends.
                for (h, d) in &tp.uncompressed_events {
                    let inner = EventHeader {
                        next_event_position: header.next_event_position,
                        ..h.clone()
                    };
                    self.event(&inner, d)?;
                }
            }
            EventData::NotSupported => {
                if header.event_type == 26 {
                    return Err(ProofError::Incident);
                }
                if header.event_type == ANONYMOUS_GTID && self.gtid_mode {
                    return Err(ProofError::Framing(
                        "an anonymous transaction in a GTID interval".into(),
                    ));
                }
                if !harmless_unparsed(header.event_type) {
                    return Err(ProofError::UnsupportedEvent(
                        header.event_type,
                    ));
                }
            }
            EventData::FormatDescription(_)
            | EventData::PreviousGtids(_)
            | EventData::HeartBeat
            | EventData::TableMap(_)
            | EventData::WriteRows(_)
            | EventData::UpdateRows(_)
            | EventData::DeleteRows(_)
            | EventData::RowsQuery(_) => {}
        }
        Ok(())
    }
}

/// Scan the retained interval `(from, to]` (module docs). Both positions must
/// carry the verified `lineage_hash`; `server_id` must differ from every
/// other replication connection of this source.
pub(crate) async fn scan_interval(
    dsn: &str,
    server_id: u64,
    server_uuid: &str,
    lineage_hash: &str,
    from: &MySqlCheckpoint,
    to: &MySqlCheckpoint,
    limits: &ScanLimits,
) -> Result<ScanReport, ProofError> {
    let started = Instant::now();
    for (name, p) in [("from", from), ("to", to)] {
        if p.lineage.as_deref() != Some(lineage_hash) {
            return Err(ProofError::Positions(format!(
                "{name} position belongs to lineage {:?}, not {lineage_hash}",
                p.lineage
            )));
        }
    }
    let pos = |p: &MySqlCheckpoint| {
        mysql_checkpoint_position(&p.file, p.pos, p.gtid_set.as_deref())
            .ok_or_else(|| {
                ProofError::Positions(format!("unparseable position {p:?}"))
            })
    };
    let (pf, pt) = (pos(from)?, pos(to)?);
    let empty = |statements: Vec<Statement>| ScanReport {
        digest: scan_digest(&pf, &pt, &statements),
        statements,
        events: 0,
        bytes: 0,
        duration: started.elapsed(),
    };
    match order_positions(&pf, &pt) {
        CheckpointOrder::Equal => return Ok(empty(Vec::new())),
        CheckpointOrder::Before => {}
        other => {
            return Err(ProofError::Positions(format!(
                "from is not before to ({other:?})"
            )));
        }
    }
    let gtid_mode = matches!(pt, WmPos::MysqlGtid { .. });
    let mut client = BinlogClient {
        url: dsn.to_string(),
        server_id,
        heartbeat_interval_secs: 5,
        timeout_secs: 30,
        ..Default::default()
    };
    let to_set = if gtid_mode {
        client.gtid_enabled = true;
        client.gtid_set = from.gtid_set.clone().unwrap_or_default();
        Some(
            parse_gtid_set(to.gtid_set.as_deref().unwrap_or_default())
                .ok_or_else(|| {
                    ProofError::Positions("unparseable end GTID set".into())
                })?,
        )
    } else {
        client.gtid_enabled = false;
        client.binlog_filename = from.file.clone();
        client.binlog_position = u32::try_from(from.pos).map_err(|_| {
            ProofError::Positions("start position out of range".into())
        })?;
        None
    };
    let done = if gtid_mode {
        parse_gtid_set(from.gtid_set.as_deref().unwrap_or_default())
            .ok_or_else(|| {
                ProofError::Positions("unparseable start GTID set".into())
            })?
    } else {
        GtidSet::new()
    };
    let mut walk = Walk {
        gtid_mode,
        to_set,
        done,
        current_gtid: None,
        ordinal: 0,
        in_explicit_txn: false,
        file: from.file.clone(),
        statements: Vec::new(),
        reached: false,
    };

    let mut stream = tokio::time::timeout(
        limits.max_duration,
        open_replication_session(&client, server_uuid),
    )
    .await
    .map_err(|_| {
        ProofError::NotReached("timed out opening the interval".into())
    })?
    .map_err(|e| match e {
        SessionError::OtherServer { found } => ProofError::OtherServer(found),
        SessionError::NoIdentity(why) => ProofError::OtherServer(why),
        SessionError::Connect(why) => ProofError::Open(why),
    })?;
    let (mut events, mut bytes) = (0u64, 0u64);
    while !walk.reached {
        let left = limits
            .max_duration
            .checked_sub(started.elapsed())
            .ok_or_else(|| ProofError::Limit("time".into()))?;
        let (header, data) = tokio::time::timeout(left, stream.read())
            .await
            .map_err(|_| ProofError::Limit("time".into()))?
            .map_err(|e| ProofError::Read(e.to_string()))?;
        if !matches!(data, EventData::HeartBeat) {
            events += 1;
            bytes += u64::from(header.event_length);
        }
        if events > limits.max_events {
            return Err(ProofError::Limit("events".into()));
        }
        if bytes > limits.max_bytes {
            return Err(ProofError::Limit("bytes".into()));
        }
        walk.event(&header, &data)?;
        if !gtid_mode
            && !matches!(
                data,
                EventData::HeartBeat
                    | EventData::Rotate(_)
                    | EventData::FormatDescription(_)
            )
        {
            // File/position mode: the end is an exact event boundary.
            let here = mysql_checkpoint_position(
                &walk.file,
                u64::from(header.next_event_position),
                None,
            )
            .ok_or_else(|| {
                ProofError::Framing(format!("unparseable file {}", walk.file))
            })?;
            match order_positions(&here, &pt) {
                CheckpointOrder::Equal
                    if walk.current_gtid.is_none() && !walk.in_explicit_txn =>
                {
                    walk.reached = true
                }
                CheckpointOrder::Before | CheckpointOrder::Equal => {}
                other => {
                    return Err(ProofError::Overshoot(format!(
                        "event ending at {}:{} is {other:?} the end position",
                        walk.file, header.next_event_position
                    )));
                }
            }
        }
    }
    let _ = stream.close().await;
    Ok(ScanReport {
        digest: scan_digest(&pf, &pt, &walk.statements),
        statements: walk.statements,
        events,
        bytes,
        duration: started.elapsed(),
    })
}

/// A consistent `(position, shape)` pair.
#[derive(Debug, Clone)]
pub(crate) struct Captured {
    pub position: MySqlCheckpoint,
    pub schema: MySqlTableSchema,
    pub attempts: u32,
}

/// Runs between the shape read and the second position read (tests use it
/// to make a capture deterministically unstable; production passes `None`).
pub(crate) type BetweenHook<'a> = &'a (
        dyn Fn() -> std::pin::Pin<
    Box<dyn std::future::Future<Output = ()> + Send>,
> + Send
            + Sync
    );

async fn read_position(
    conn: &mut mysql_async::Conn,
    gtid_mode: bool,
    lineage_hash: &str,
) -> Result<MySqlCheckpoint, ProofError> {
    let row: Option<mysql_async::Row> =
        match conn.query_first("SHOW BINARY LOG STATUS").await {
            Ok(r) => r,
            Err(_) => conn
                .query_first("SHOW MASTER STATUS")
                .await
                .map_err(|e| ProofError::Capture(e.to_string()))?,
        };
    let mut row =
        row.ok_or_else(|| ProofError::Capture("binary logging is off".into()))?;
    let file: String = row
        .take(0)
        .ok_or_else(|| ProofError::Capture("no binlog file".into()))?;
    let pos: u64 = row
        .take(1)
        .ok_or_else(|| ProofError::Capture("no binlog position".into()))?;
    let gtid: Option<String> = row.take::<Option<String>, _>(4).flatten();
    Ok(MySqlCheckpoint {
        file,
        pos,
        gtid_set: gtid_mode.then(|| gtid.unwrap_or_default()),
        lineage: Some(lineage_hash.to_string()),
    })
}

/// Capture `db.table`'s shape at a stable server position (module docs).
pub(crate) async fn stable_capture(
    dsn: &str,
    server_uuid: &str,
    lineage_hash: &str,
    db: &str,
    table: &str,
    max_attempts: u32,
    between: Option<BetweenHook<'_>>,
) -> Result<Captured, ProofError> {
    let pool = mysql_async::Pool::new(dsn);
    let result = async {
        let mut conn = pool
            .get_conn()
            .await
            .map_err(|e| ProofError::Capture(e.to_string()))?;
        let mode: Option<String> = conn
            .query_first("SELECT @@GLOBAL.gtid_mode")
            .await
            .map_err(|e| ProofError::Capture(e.to_string()))?;
        let gtid_mode = mode.is_some_and(|m| m.eq_ignore_ascii_case("ON"));
        for attempt in 1..=max_attempts.max(1) {
            let before =
                read_position(&mut conn, gtid_mode, lineage_hash).await?;
            let schema =
                match fetch_table_schema_on(&mut conn, server_uuid, db, table)
                    .await
                {
                    Ok(Live::Found(s)) => s,
                    Ok(Live::OtherLineage(who)) => {
                        return Err(ProofError::OtherServer(who));
                    }
                    Err(e) => return Err(ProofError::Capture(e.to_string())),
                };
            if let Some(hook) = between {
                hook().await;
            }
            let after =
                read_position(&mut conn, gtid_mode, lineage_hash).await?;
            let p = |c: &MySqlCheckpoint| {
                mysql_checkpoint_position(&c.file, c.pos, c.gtid_set.as_deref())
                    .ok_or_else(|| {
                        ProofError::Capture(format!(
                            "unparseable position {c:?}"
                        ))
                    })
            };
            if order_positions(&p(&before)?, &p(&after)?)
                == CheckpointOrder::Equal
            {
                conn.disconnect().await.ok();
                return Ok(Captured {
                    position: after,
                    schema,
                    attempts: attempt,
                });
            }
            tokio::time::sleep(Duration::from_millis(20 * u64::from(attempt)))
                .await;
        }
        Err(ProofError::Unstable(max_attempts.max(1)))
    }
    .await;
    pool.disconnect().await.ok();
    result
}

#[cfg(test)]
mod tests {
    use super::super::mysql_ddl_attribution::TableName;
    use super::*;

    const U: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562";

    fn g(n: u64) -> WmPos {
        WmPos::MysqlGtid {
            gtid_set: format!("{U}:1-{n}"),
        }
    }

    fn stmt(gno: u64, table: &str) -> Statement {
        Statement {
            event: EventIdentity::Gtid {
                gtid: format!("{U}:{gno}"),
                ordinal: 1,
            },
            effect: DdlEffect::Tables(vec![TableName {
                db: "app".into(),
                table: table.into(),
            }]),
        }
    }

    #[test]
    fn the_digest_binds_boundaries_statements_and_classifier() {
        let base = scan_digest(&g(1), &g(9), &[stmt(5, "a")]);
        assert_eq!(base, scan_digest(&g(1), &g(9), &[stmt(5, "a")]));
        for other in [
            scan_digest(&g(2), &g(9), &[stmt(5, "a")]),
            scan_digest(&g(1), &g(8), &[stmt(5, "a")]),
            scan_digest(&g(1), &g(9), &[stmt(6, "a")]),
            scan_digest(&g(1), &g(9), &[stmt(5, "b")]),
            scan_digest(&g(1), &g(9), &[]),
            scan_digest(&g(1), &g(9), &[stmt(5, "a"), stmt(6, "a")]),
        ] {
            assert_ne!(base, other);
        }
    }

    #[test]
    fn gtid_sets_are_exact_not_ranges() {
        let mut s = parse_gtid_set(&format!("{U}:1-5")).unwrap();
        add_gtid(&mut s, &format!("{U}:9")).unwrap();
        // 6-8 were never seen: the set must not claim them.
        assert!(!gtid_subseteq(
            &parse_gtid_set(&format!("{U}:1-9")).unwrap(),
            &s
        ));
        for n in 6..=8 {
            add_gtid(&mut s, &format!("{U}:{n}")).unwrap();
        }
        assert!(gtid_subseteq(
            &parse_gtid_set(&format!("{U}:1-9")).unwrap(),
            &s
        ));
        assert!(add_gtid(&mut s, "nonsense").is_err());
        assert!(add_gtid(&mut s, &format!("{U}:0")).is_err());
    }

    fn header(event_type: u8, next_event_position: u32) -> EventHeader {
        EventHeader {
            timestamp: 0,
            event_type,
            server_id: 1,
            event_length: 0,
            next_event_position,
            event_flags: 0,
        }
    }

    fn walk(gtid_mode: bool) -> Walk {
        Walk {
            gtid_mode,
            to_set: gtid_mode
                .then(|| parse_gtid_set(&format!("{U}:1-9")).unwrap()),
            done: GtidSet::default(),
            current_gtid: None,
            ordinal: 0,
            in_explicit_txn: false,
            file: "binlog.000001".into(),
            statements: Vec::new(),
            reached: false,
        }
    }

    fn query(sql: &str) -> EventData {
        EventData::Query(
            mysql_binlog_connector_rust::event::query_event::QueryEvent {
                thread_id: 0,
                exec_time: 0,
                error_code: 0,
                schema: "d".into(),
                query: sql.into(),
            },
        )
    }

    #[test]
    fn statements_inside_a_compressed_payload_take_the_payload_position() {
        use mysql_binlog_connector_rust::event::transaction_payload_event::TransactionPayloadEvent;
        let mut walk = walk(false);
        // Inner events carry no binlog position of their own.
        let payload = EventData::TransactionPayload(TransactionPayloadEvent {
            uncompressed_size: 0,
            uncompressed_events: vec![(
                header(2, 0),
                query("ALTER TABLE t ADD COLUMN c INT"),
            )],
        });
        walk.event(&header(40, 4321), &payload).unwrap();
        assert_eq!(
            walk.statements[0].event,
            EventIdentity::FilePos {
                file: "binlog.000001".into(),
                end_pos: 4321
            }
        );
    }

    #[test]
    fn anonymous_transactions_fail_a_gtid_scan_only() {
        // GTID mode: an anonymous transaction cannot be placed in (from, to].
        let mut w = walk(true);
        assert!(matches!(
            w.event(&header(ANONYMOUS_GTID, 100), &EventData::NotSupported),
            Err(ProofError::Framing(_))
        ));
        // Nor can a statement that arrives outside any GTID transaction.
        let mut w = walk(true);
        assert!(matches!(
            w.event(&header(2, 200), &query("ALTER TABLE t ADD c INT")),
            Err(ProofError::Framing(_))
        ));
        // File/position mode: outer event positions order it; harmless.
        let mut w = walk(false);
        w.event(&header(ANONYMOUS_GTID, 100), &EventData::NotSupported)
            .unwrap();
        w.event(&header(2, 200), &query("ALTER TABLE t ADD c INT"))
            .unwrap();
        assert_eq!(w.statements.len(), 1);
    }

    #[test]
    fn only_known_harmless_unparsed_events_are_accepted() {
        for t in [3u8, 5, 13, 14, 28, 34, 36, 37, 39] {
            assert!(harmless_unparsed(t), "{t}");
        }
        for t in [0u8, 1, 26, 99, 200] {
            assert!(!harmless_unparsed(t), "{t}");
        }
    }

    /// Live MySQL 8.4 (Docker): one GTID-mode and one file/position server.
    mod live {
        use super::*;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU32, Ordering};
        use testcontainers::core::WaitFor;
        use testcontainers::runners::AsyncRunner;
        use testcontainers::{ContainerAsync, GenericImage, ImageExt};
        use tokio::sync::OnceCell;

        const LINEAGE: &str = "0123456789abcdef0123456789abcdef";
        static GTID: OnceCell<(ContainerAsync<GenericImage>, u16)> =
            OnceCell::const_new();
        static FILEPOS: OnceCell<(ContainerAsync<GenericImage>, u16)> =
            OnceCell::const_new();

        async fn start(gtid: bool) -> (ContainerAsync<GenericImage>, u16) {
            let mut cmd = vec![
                "--server-id=7",
                "--log-bin=/var/lib/mysql/mysql-bin.log",
                "--binlog-format=ROW",
                "--binlog-checksum=NONE",
            ];
            if gtid {
                cmd.extend(["--gtid-mode=ON", "--enforce-gtid-consistency=ON"]);
            }
            let c = GenericImage::new("mysql", "8.4")
                .with_wait_for(WaitFor::message_on_stderr(
                    "ready for connections. Version: '8.4",
                ))
                .with_env_var("MYSQL_ROOT_PASSWORD", "pw")
                .with_cmd(cmd)
                .start()
                .await
                .expect("start mysql");
            let port = c.get_host_port_ipv4(3306).await.unwrap();
            // Wait until it accepts SQL.
            for _ in 0..60 {
                if mysql_async::Conn::from_url(dsn_of(port)).await.is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            (c, port)
        }

        fn dsn_of(port: u16) -> String {
            format!("mysql://root:pw@127.0.0.1:{port}/")
        }

        async fn port(gtid: bool) -> u16 {
            let cell = if gtid { &GTID } else { &FILEPOS };
            cell.get_or_init(|| start(gtid)).await.1
        }

        async fn server(gtid: bool) -> String {
            dsn_of(port(gtid).await)
        }

        /// A TCP endpoint that sends its n-th connection (from 0) to the
        /// port `route(n)`, like a failover endpoint or load balancer.
        async fn proxy(
            route: impl Fn(u32) -> u16 + Send + Sync + 'static,
        ) -> (String, Arc<AtomicU32>) {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = l.local_addr().unwrap().port();
            let opened = Arc::new(AtomicU32::new(0));
            let count = opened.clone();
            tokio::spawn(async move {
                while let Ok((mut client, _)) = l.accept().await {
                    let to = route(count.fetch_add(1, Ordering::SeqCst));
                    tokio::spawn(async move {
                        if let Ok(mut server) =
                            tokio::net::TcpStream::connect(("127.0.0.1", to))
                                .await
                        {
                            let _ = tokio::io::copy_bidirectional(
                                &mut client,
                                &mut server,
                            )
                            .await;
                        }
                    });
                }
            });
            (dsn_of(port), opened)
        }

        async fn sql(dsn: &str, stmts: &[&str]) {
            let mut c = mysql_async::Conn::from_url(dsn).await.unwrap();
            for s in stmts {
                c.query_drop(*s)
                    .await
                    .unwrap_or_else(|e| panic!("{s}: {e}"));
            }
        }

        async fn uuid(dsn: &str) -> String {
            let mut c = mysql_async::Conn::from_url(dsn).await.unwrap();
            c.query_first("SELECT @@GLOBAL.server_uuid")
                .await
                .unwrap()
                .unwrap()
        }

        async fn position(dsn: &str) -> MySqlCheckpoint {
            let mut c = mysql_async::Conn::from_url(dsn).await.unwrap();
            let mode: Option<String> =
                c.query_first("SELECT @@GLOBAL.gtid_mode").await.unwrap();
            read_position(&mut c, mode.as_deref() == Some("ON"), LINEAGE)
                .await
                .unwrap()
        }

        async fn scan(
            dsn: &str,
            from: &MySqlCheckpoint,
            to: &MySqlCheckpoint,
        ) -> Result<ScanReport, ProofError> {
            scan_interval(
                dsn,
                4_000_123,
                &uuid(dsn).await,
                LINEAGE,
                from,
                to,
                &ScanLimits {
                    max_duration: Duration::from_secs(20),
                    ..ScanLimits::default()
                },
            )
            .await
        }

        fn affected(r: &ScanReport) -> Vec<String> {
            r.statements
                .iter()
                .map(|s| match &s.effect {
                    DdlEffect::Tables(v) | DdlEffect::SameShape(v) => v
                        .iter()
                        .map(|t| format!("{}.{}", t.db, t.table))
                        .collect::<Vec<_>>()
                        .join("+"),
                    DdlEffect::Barrier(b) => format!("barrier:{b:?}"),
                    DdlEffect::None => "none".into(),
                })
                .collect()
        }

        async fn scenario(gtid: bool, db: &str) {
            let dsn = server(gtid).await;
            sql(
                &dsn,
                &[
                    &format!("DROP DATABASE IF EXISTS {db}"),
                    &format!("CREATE DATABASE {db}"),
                    &format!("CREATE TABLE {db}.a (id INT PRIMARY KEY, v INT)"),
                    &format!("CREATE TABLE {db}.b (id INT PRIMARY KEY, v INT)"),
                ],
            )
            .await;
            let from = position(&dsn).await;
            sql(&dsn, &[&format!("INSERT INTO {db}.a VALUES (1, 1)")]).await;
            sql(&dsn, &[&format!("ALTER TABLE {db}.a ADD COLUMN w INT")]).await;
            let after_a = position(&dsn).await;
            sql(&dsn, &[&format!("INSERT INTO {db}.a VALUES (2, 2, 2)")]).await;
            sql(&dsn, &[&format!("RENAME TABLE {db}.b TO {db}.c")]).await;
            let to = position(&dsn).await;

            // The whole interval: both DDL, in order, rows ignored.
            let r = scan(&dsn, &from, &to).await.unwrap();
            assert_eq!(
                affected(&r),
                [format!("{db}.a"), format!("{db}.b+{db}.c")]
            );
            assert!(r.events > 0 && r.bytes > 0);
            // Deterministic.
            assert_eq!(scan(&dsn, &from, &to).await.unwrap().digest, r.digest);

            // Exclusive start, inclusive end: the first DDL ends exactly at
            // `after_a`, so it is outside (after_a, to] and inside (from, after_a].
            let tail = scan(&dsn, &after_a, &to).await.unwrap();
            assert_eq!(affected(&tail), [format!("{db}.b+{db}.c")]);
            let head = scan(&dsn, &from, &after_a).await.unwrap();
            assert_eq!(affected(&head), [format!("{db}.a")]);
            assert_ne!(head.digest, r.digest);

            // An empty interval.
            let none = scan(&dsn, &to, &to).await.unwrap();
            assert!(none.statements.is_empty());

            // Out of order.
            assert!(matches!(
                scan(&dsn, &to, &from).await,
                Err(ProofError::Positions(_))
            ));
        }

        #[tokio::test]
        #[ignore = "requires docker"]
        async fn gtid_scans_are_complete_exact_and_deterministic() {
            scenario(true, "scan_gtid").await;
        }

        #[tokio::test]
        #[ignore = "requires docker"]
        async fn file_position_scans_are_complete_exact_and_deterministic() {
            scenario(false, "scan_filepos").await;
        }

        #[tokio::test]
        #[ignore = "requires docker"]
        async fn identity_is_verified_on_the_session_that_streams() {
            for gtid in [true, false] {
                // A is the verified server; B another one behind the same
                // endpoint.
                let (a, b) = (port(gtid).await, port(!gtid).await);
                let dsn_a = dsn_of(a);
                let db = if gtid {
                    "scan_route_gtid"
                } else {
                    "scan_route_file"
                };
                sql(
                    &dsn_a,
                    &[
                        &format!("DROP DATABASE IF EXISTS {db}"),
                        &format!("CREATE DATABASE {db}"),
                    ],
                )
                .await;
                let from = position(&dsn_a).await;
                sql(&dsn_a, &[&format!("CREATE TABLE {db}.t (id INT)")]).await;
                let to = position(&dsn_a).await;
                let uuid_a = uuid(&dsn_a).await;
                let direct = scan(&dsn_a, &from, &to).await.unwrap();
                let via = |dsn: String| {
                    let (uuid_a, from, to) =
                        (uuid_a.clone(), from.clone(), to.clone());
                    async move {
                        scan_interval(
                            &dsn,
                            4_000_125,
                            &uuid_a,
                            LINEAGE,
                            &from,
                            &to,
                            &ScanLimits {
                                max_duration: Duration::from_secs(20),
                                ..ScanLimits::default()
                            },
                        )
                        .await
                    }
                };

                // First connection to A, every later one to B: the session
                // that streams must be the one that was verified.
                let (dsn, opened) =
                    proxy(move |n| if n == 0 { a } else { b }).await;
                let r = via(dsn).await.unwrap();
                assert_eq!(r.digest, direct.digest, "gtid={gtid}");
                assert_eq!(opened.load(Ordering::SeqCst), 1, "gtid={gtid}");

                // First connection to B: refused before any event.
                let (dsn, opened) =
                    proxy(move |n| if n == 0 { b } else { a }).await;
                let r = via(dsn).await;
                assert!(
                    matches!(r, Err(ProofError::OtherServer(_))),
                    "gtid={gtid}: {r:?}"
                );
                assert_eq!(opened.load(Ordering::SeqCst), 1, "gtid={gtid}");
            }
        }

        #[tokio::test]
        #[ignore = "requires docker"]
        async fn compressed_transactions_are_scanned() {
            let dsn = server(true).await;
            sql(
                &dsn,
                &[
                    "DROP DATABASE IF EXISTS scan_zstd",
                    "CREATE DATABASE scan_zstd",
                    "CREATE TABLE scan_zstd.t (id INT PRIMARY KEY, v TEXT)",
                ],
            )
            .await;
            let from = position(&dsn).await;
            sql(
                &dsn,
                &[
                    "SET SESSION binlog_transaction_compression = ON",
                    "INSERT INTO scan_zstd.t VALUES (1, REPEAT('x', 4000))",
                    "ALTER TABLE scan_zstd.t ADD COLUMN w INT",
                ],
            )
            .await;
            let to = position(&dsn).await;
            // The interval really holds a compressed transaction.
            assert_eq!(from.file, to.file);
            let mut c =
                mysql_async::Conn::from_url(dsn.as_str()).await.unwrap();
            let kinds: Vec<String> = c
                .query_map(
                    format!(
                        "SHOW BINLOG EVENTS IN '{}' FROM {}",
                        to.file, from.pos
                    ),
                    |row: mysql_async::Row| row.get::<String, _>(2).unwrap(),
                )
                .await
                .unwrap();
            assert!(kinds.iter().any(|k| k == "Transaction_payload"));
            let r = scan(&dsn, &from, &to).await.unwrap();
            assert_eq!(affected(&r), ["scan_zstd.t"]);
            // The same workload uncompressed classifies identically and
            // ends at the same kind of boundary.
            let from2 = position(&dsn).await;
            sql(
                &dsn,
                &[
                    "SET SESSION binlog_transaction_compression = OFF",
                    "INSERT INTO scan_zstd.t VALUES (2, REPEAT('x', 4000), 1)",
                    "ALTER TABLE scan_zstd.t ADD COLUMN w2 INT",
                ],
            )
            .await;
            let to2 = position(&dsn).await;
            let r2 = scan(&dsn, &from2, &to2).await.unwrap();
            assert_eq!(
                r.statements.iter().map(|s| &s.effect).collect::<Vec<_>>(),
                r2.statements.iter().map(|s| &s.effect).collect::<Vec<_>>()
            );
        }

        #[tokio::test]
        #[ignore = "requires docker"]
        async fn an_unreachable_end_or_resource_limit_fails_closed() {
            let dsn = server(true).await;
            let from = position(&dsn).await;
            // An end position the server has not reached.
            let u = uuid(&dsn).await;
            let mut future = from.clone();
            future.gtid_set = Some(
                format!(
                    "{},{u}:1-999999",
                    from.gtid_set.clone().unwrap_or_default()
                )
                .trim_start_matches(',')
                .to_string(),
            );
            let r = scan_interval(
                &dsn,
                4_000_124,
                &u,
                LINEAGE,
                &from,
                &future,
                &ScanLimits {
                    max_duration: Duration::from_secs(3),
                    ..ScanLimits::default()
                },
            )
            .await;
            assert!(
                matches!(
                    r,
                    Err(ProofError::Limit(_)) | Err(ProofError::NotReached(_))
                ),
                "{r:?}"
            );
            // An event budget smaller than the interval.
            sql(
                &dsn,
                &[
                    "CREATE DATABASE IF NOT EXISTS scan_limit",
                    "CREATE TABLE IF NOT EXISTS scan_limit.t (id INT)",
                ],
            )
            .await;
            let to = position(&dsn).await;
            let r = scan_interval(
                &dsn,
                4_000_125,
                &u,
                LINEAGE,
                &from,
                &to,
                &ScanLimits {
                    max_events: 1,
                    ..ScanLimits::default()
                },
            )
            .await;
            assert!(matches!(r, Err(ProofError::Limit(_))), "{r:?}");
        }

        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_purged_interval_fails_closed() {
            // Own server: purging must not disturb the other tests.
            let (_c, port) = start(true).await;
            let dsn = dsn_of(port);
            sql(
                &dsn,
                &["CREATE DATABASE p", "CREATE TABLE p.t (id INT PRIMARY KEY)"],
            )
            .await;
            let from = position(&dsn).await;
            sql(&dsn, &["INSERT INTO p.t VALUES (1)", "FLUSH BINARY LOGS"])
                .await;
            let to = position(&dsn).await;
            sql(&dsn, &[&format!("PURGE BINARY LOGS TO '{}'", to.file)]).await;
            let r = scan(&dsn, &from, &to).await;
            assert!(
                matches!(
                    r,
                    Err(ProofError::Open(_)) | Err(ProofError::Read(_))
                ),
                "{r:?}"
            );
        }

        #[tokio::test]
        #[ignore = "requires docker"]
        async fn positions_of_another_lineage_or_server_are_refused() {
            let dsn = server(true).await;
            let from = position(&dsn).await;
            let to = position(&dsn).await;
            let mut foreign = to.clone();
            foreign.lineage = Some("f".repeat(32));
            assert!(matches!(
                scan(&dsn, &from, &foreign).await,
                Err(ProofError::Positions(_))
            ));
            let mut file_pos = to.clone();
            file_pos.gtid_set = None;
            assert!(matches!(
                scan(&dsn, &from, &file_pos).await,
                Err(ProofError::Positions(_))
            ));
            sql(
                &dsn,
                &[
                    "CREATE DATABASE IF NOT EXISTS scan_other",
                    "CREATE TABLE IF NOT EXISTS scan_other.t (id INT)",
                ],
            )
            .await;
            let later = position(&dsn).await;
            let r = scan_interval(
                &dsn,
                4_000_126,
                "00000000-1111-2222-3333-444444444444",
                LINEAGE,
                &from,
                &later,
                &ScanLimits::default(),
            )
            .await;
            assert!(matches!(r, Err(ProofError::OtherServer(_))), "{r:?}");
        }

        /// An end position the scan passes without ever reaching exactly - a
        /// GTID set that skips a transaction of the interval (a gap), or a
        /// file position inside an event - fails closed.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn passing_the_end_without_reaching_it_fails_closed() {
            let dsn = server(true).await;
            sql(&dsn, &["CREATE DATABASE IF NOT EXISTS scan_gap", "CREATE TABLE IF NOT EXISTS scan_gap.t (id INT PRIMARY KEY)", "DELETE FROM scan_gap.t"]).await;
            let from = position(&dsn).await;
            sql(&dsn, &["INSERT INTO scan_gap.t VALUES (1)"]).await;
            let mid = position(&dsn).await;
            sql(&dsn, &["INSERT INTO scan_gap.t VALUES (2)"]).await;
            let end = position(&dsn).await;
            // `to` = from + only the SECOND transaction.
            let u = uuid(&dsn).await;
            let parse = |p: &MySqlCheckpoint| {
                parse_gtid_set(p.gtid_set.as_deref().unwrap()).unwrap()
            };
            let (f, m, e) = (parse(&from), parse(&mid), parse(&end));
            let last = |s: &GtidSet| s.get(&u).unwrap().last().unwrap().1;
            let (first_txn, second_txn) = (last(&m), last(&e));
            assert_eq!(first_txn, last(&f) + 1);
            let mut gap = from.clone();
            gap.gtid_set = Some(format!(
                "{},{u}:{second_txn}",
                from.gtid_set.clone().unwrap()
            ));
            let r = scan(&dsn, &from, &gap).await;
            assert!(matches!(r, Err(ProofError::Overshoot(_))), "{r:?}");

            let dsn = server(false).await;
            sql(&dsn, &["CREATE DATABASE IF NOT EXISTS scan_gap_f", "CREATE TABLE IF NOT EXISTS scan_gap_f.t (id INT PRIMARY KEY)", "DELETE FROM scan_gap_f.t"]).await;
            let from = position(&dsn).await;
            sql(&dsn, &["INSERT INTO scan_gap_f.t VALUES (1)"]).await;
            let mut inside = position(&dsn).await;
            inside.pos -= 1;
            let r = scan(&dsn, &from, &inside).await;
            assert!(matches!(r, Err(ProofError::Overshoot(_))), "{r:?}");
        }

        async fn capture_table(dsn: &str, db: &str) {
            sql(dsn, &[
                &format!("DROP DATABASE IF EXISTS {db}"),
                &format!("CREATE DATABASE {db}"),
                &format!("CREATE TABLE {db}.t (id INT PRIMARY KEY, name VARCHAR(20))"),
            ])
            .await;
        }

        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_quiet_server_gives_a_stable_capture() {
            for gtid in [true, false] {
                let dsn = server(gtid).await;
                let db = if gtid { "cap_quiet_g" } else { "cap_quiet_f" };
                capture_table(&dsn, db).await;
                let c = stable_capture(
                    &dsn,
                    &uuid(&dsn).await,
                    LINEAGE,
                    db,
                    "t",
                    3,
                    None,
                )
                .await
                .unwrap();
                assert_eq!(c.attempts, 1);
                let names: Vec<_> =
                    c.schema.columns.iter().map(|x| x.name.as_str()).collect();
                assert_eq!(names, ["id", "name"]);
                assert_eq!(c.position.lineage.as_deref(), Some(LINEAGE));
                assert_eq!(c.position.gtid_set.is_some(), gtid);
                // The captured position is the server's current one.
                let now = position(&dsn).await;
                assert_eq!(
                    (c.position.file, c.position.pos),
                    (now.file, now.pos)
                );
            }
        }

        /// A write between the shape read and the second position read makes
        /// the attempt unstable; once the writes stop, a retry succeeds; if
        /// they never stop, the bounded retries fail closed.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn an_unstable_position_is_retried_then_refused() {
            let dsn = server(true).await;
            capture_table(&dsn, "cap_unstable").await;
            let calls = Arc::new(AtomicU32::new(0));
            let ids = Arc::new(AtomicU32::new(1000));
            let writer = |limit: u32| {
                let (dsn, calls, ids) =
                    (dsn.clone(), calls.clone(), ids.clone());
                move || {
                    let (dsn, calls, ids) =
                        (dsn.clone(), calls.clone(), ids.clone());
                    Box::pin(async move {
                        let n = calls.fetch_add(1, Ordering::SeqCst);
                        if n < limit {
                            let id = ids.fetch_add(1, Ordering::SeqCst);
                            sql(&dsn, &[&format!("INSERT INTO cap_unstable.t VALUES ({id}, 'x')")]).await;
                        }
                    })
                        as std::pin::Pin<
                            Box<dyn std::future::Future<Output = ()> + Send>,
                        >
                }
            };
            let disturb_once = writer(1);
            let c = stable_capture(
                &dsn,
                &uuid(&dsn).await,
                LINEAGE,
                "cap_unstable",
                "t",
                3,
                Some(&disturb_once),
            )
            .await
            .unwrap();
            assert_eq!(c.attempts, 2);
            calls.store(0, Ordering::SeqCst);
            let always = writer(u32::MAX);
            let r = stable_capture(
                &dsn,
                &uuid(&dsn).await,
                LINEAGE,
                "cap_unstable",
                "t",
                3,
                Some(&always),
            )
            .await;
            assert!(matches!(r, Err(ProofError::Unstable(3))), "{r:?}");
        }

        #[tokio::test]
        #[ignore = "requires docker"]
        async fn capture_refuses_another_server_or_a_missing_table() {
            let dsn = server(true).await;
            capture_table(&dsn, "cap_refuse").await;
            let r = stable_capture(
                &dsn,
                "00000000-1111-2222-3333-444444444444",
                LINEAGE,
                "cap_refuse",
                "t",
                3,
                None,
            )
            .await;
            assert!(matches!(r, Err(ProofError::OtherServer(_))), "{r:?}");
            let r = stable_capture(
                &dsn,
                &uuid(&dsn).await,
                LINEAGE,
                "cap_refuse",
                "missing",
                3,
                None,
            )
            .await;
            assert!(matches!(r, Err(ProofError::Capture(_))), "{r:?}");
        }
    }
}
