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
//! - [`capture`] reads the server position A, one table's shape and the
//!   position B on ONE connection, after proving the server's identity on it,
//!   while holding the table's shared-read metadata lock in a transaction.
//!   That lock is compatible with DML and conflicts with every DDL of the
//!   table: it is granted only once a DDL already running on the table has
//!   committed (binlog event written, dictionary change visible), and no DDL
//!   of the table can commit until it is released after B. So a DDL of the
//!   table at or before A is visible to the shape read (closing the window in
//!   which a binlog position already includes a DDL whose dictionary change is
//!   not yet visible), and none lands in (A, B]. The server is never required
//!   to be quiet: ordinary writes, to the table or any other, continue, and
//!   DDL of other tables is irrelevant. The lock's schema intention lock also
//!   holds off DDL of the table's database. Unattributable statements that
//!   are neither are not locked out: a scan proves the interval free of them, a covering scan by
//!   the caller ([`covers`]) or the capture's own interval
//!   ([`capture_proven`], which retries only when something affecting the
//!   table landed inside it).
//!
//! The scan digest is deterministic for an interval: it binds the classifier
//! version, both boundaries and every classified statement with its binlog
//! identity. Event and byte counts are reported but not hashed (heartbeats
//! and start-of-stream events can differ between two scans of one interval).

// Used by the activation wiring (forward proof, baseline) in the next commit;
// this allow is removed there.
#![allow(dead_code)]

use std::collections::HashMap;
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_binlog_connector_rust::binlog_client::BinlogClient;
use mysql_binlog_connector_rust::event::event_data::EventData;
use mysql_binlog_connector_rust::event::event_header::EventHeader;
use mysql_binlog_connector_rust::event::rotate_event::RotateEvent;
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use super::MySqlCheckpoint;
use super::mysql_activation::EventIdentity;
use super::mysql_ddl_attribution::{BarrierScopeOf, DdlEffect, classify};
use super::mysql_schema_loader::{Live, fetch_table_schema_on};
use super::mysql_session::{SessionError, open_replication_session_abortive};
use super::mysql_table_schema::MySqlTableSchema;
use crate::durable_checkpoint::{
    WmPos, mysql_checkpoint_position, order_positions,
};
use crate::gtid_set::GtidSet;
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
    /// The longest wait from opening the stream to its first transaction.
    /// A GTID-positioned stream (no anchor) seeks server-side through the
    /// file's earlier events; past this bound the scan fails closed with
    /// `SeekLimit` instead of paying an unbounded seek.
    pub max_seek: Duration,
}

impl Default for ScanLimits {
    fn default() -> Self {
        Self {
            max_events: 50_000_000,
            max_bytes: 64 * 1024 * 1024 * 1024,
            max_duration: Duration::from_secs(600),
            max_seek: Duration::from_secs(10),
        }
    }
}

/// A physical start for a GTID-mode scan (`docs/design/mysql-proof-seek.md`,
/// "Positioned GTID scanning"): in `file` of the server `server_uuid`
/// (lineage `lineage`), the transaction starting at `offset` is the first
/// one not in `set_before`, and `next_gtid` is its GTID when known.
///
/// An anchor is only ever taken from a binlog stream at a transaction
/// boundary, never from a status read, and lives in memory only. It is a
/// hint: the scan validates the events the server sends from it, and GTID
/// accounting (`B ⊆ A ∪ seen`) still decides the proof, so a wrong anchor
/// costs a refused scan, never an incomplete proof.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Anchor {
    pub server_uuid: String,
    pub lineage: String,
    pub file: String,
    pub offset: u64,
    pub set_before: GtidSet,
    pub next_gtid: Option<String>,
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
    pub trace: ScanTrace,
    /// GTID mode, once the end was reached: the boundary after the last
    /// transaction of `to`, observed at the terminal event (a record
    /// frontier, to extend the interval from).
    pub frontier: Option<Anchor>,
    /// GTID mode: the first transaction the stream sent, observed at its
    /// GTID event, with the start set (a session anchor).
    pub session_anchor: Option<Anchor>,
    /// Whether the stream started at an anchor, and where its first GTID
    /// was validated.
    pub positioned_at: Option<(String, u64)>,
    /// The shared scanner's physical work for the request this answers
    /// (zero for a direct scan).
    pub work: ProofWork,
}

/// The physical work of one scanner round (`docs/design/mysql-proof-seek.md`,
/// "Exact bounds"), attributed to every request the round serves.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ProofWork {
    /// Extensions of the record the round committed (at most 2).
    pub extensions_committed: u32,
    /// Scans opened at an anchor (at most one per extension).
    pub positioned_attempts: u32,
    /// GTID-positioned scans without an anchor, under `max_seek` (at most
    /// one per extension).
    pub unanchored_attempts: u32,
    /// Replication streams opened (at most 4).
    pub stream_openings: u32,
}

/// Which proof a scan or capture serves (bounded: a metric label).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProofKind {
    Lazy,
    FullFallback,
    Snapshot,
    Forward,
    FailoverDrift,
}

impl ProofKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            ProofKind::Lazy => "lazy",
            ProofKind::FullFallback => "full_fallback",
            ProofKind::Snapshot => "snapshot",
            ProofKind::Forward => "forward",
            ProofKind::FailoverDrift => "failover_drift",
        }
    }
}

/// Who asks for a scan or capture: bounded labels for its metrics.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ScanTag<'a> {
    pub source_id: &'a str,
    pub kind: ProofKind,
}

/// The qualification trace target. Everything costly - per-event
/// classification, GTID arithmetic, table names, JSON - runs only while it
/// is enabled; the trace never changes a scan's outcome.
pub(crate) const TRACE_TARGET: &str = "deltaforge::proof_trace";

pub(crate) fn trace_enabled() -> bool {
    tracing::enabled!(target: "deltaforge::proof_trace", tracing::Level::INFO)
}

static NEXT_SCAN_ID: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(1);

/// A binlog position as decoded: the file, the event's end position and
/// the GTID of the transaction it belongs to (if any).
#[derive(Debug, Clone, Default, serde::Serialize)]
pub(crate) struct DecodedAt {
    pub file: String,
    pub end_pos: u64,
    pub gtid: Option<String>,
}

/// What a scan did (always collected: a handful of integers and two
/// positions). `detail` is filled only while the trace target is enabled.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub(crate) struct ScanTrace {
    /// Process-unique: joins this scan with the proofs it served.
    pub scan_id: u64,
    /// Connect, identity proof and dump command.
    pub setup: Duration,
    /// From the dump command to the first transaction event.
    pub seek: Duration,
    /// From the first transaction event to the end position.
    pub scan: Duration,
    /// The first transaction event decoded (protocol events excluded).
    pub first: Option<DecodedAt>,
    /// The event at which the end position was proven reached.
    pub terminal: Option<DecodedAt>,
    pub detail: Option<ScanDetail>,
}

/// Event classification of a scan (trace target only).
#[derive(Debug, Clone, Default, serde::Serialize)]
pub(crate) struct ScanDetail {
    /// Rotate, format description, previous-GTIDs: protocol/file events,
    /// never evidence of decoding before the start.
    pub protocol_events: u64,
    pub heartbeats: u64,
    pub table_maps: u64,
    pub row_events: u64,
    pub query_events: u64,
    /// Transactions completed in the scan: of the source server's UUID,
    /// and of any other UUID.
    pub source_txns: u64,
    pub other_txns: u64,
    /// Transaction events decoded at or before the start position (GTID
    /// mode: a transaction already in the start set).
    pub before_start: u64,
    /// The source server's transactions in (from, to] by GTID arithmetic
    /// (other UUIDs in the executed set are not counted).
    pub interval_source_txns: Option<u64>,
}

/// Why a scan or capture failed (always fail-closed).
#[derive(Debug, Clone, thiserror::Error)]
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
    #[error(
        "a DDL or barrier affecting the table landed inside the capture interval on each of {0} attempts"
    )]
    Unstable(u32),
    #[error("schema capture failed: {0}")]
    Capture(String),
    /// The stream opened at an anchor did not start as the anchor claims:
    /// refused before any statement counted (the caller may fall back).
    #[error("the anchored start was refused: {0}")]
    AnchorMismatch(String),
    /// A GTID-positioned stream did not reach its first transaction within
    /// `max_seek`: the server's seek through the file's earlier events.
    #[error(
        "the binlog seek exceeded its bound ({0}); the interval is unproven"
    )]
    SeekLimit(String),
    /// No control-plane connection could be had within the capture's wait
    /// (`mysql_control`): `scope` (`runner` or `source`) stayed at its
    /// bound.
    #[error("{why}")]
    Saturated { scope: &'static str, why: String },
}

/// The run stopped while a capture was in flight: its session is killed
/// (as for any dropped capture) and nothing was decided. Never a
/// [`ProofError`]: a capture's outcome is `Result<Result<_, ProofError>,
/// Cancelled>`, so no caller can take a stop for a failed capture (a
/// fallback, a retry, an admission going on); `?` ends the caller as
/// [`SourceError::Cancelled`](deltaforge_core::SourceError::Cancelled).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Cancelled;

impl From<Cancelled> for deltaforge_core::SourceError {
    fn from(_: Cancelled) -> Self {
        deltaforge_core::SourceError::Cancelled
    }
}

/// A run's schema-capture context: its one authoritative DSN and its stop.
/// Built once per run ([`CaptureCtx::for_run`]) and shared by every
/// capture of the run (forward proofs, snapshot and lazy baselines,
/// failover drift, FULL fallback, activation windows), the only way to
/// start a capture: none can use another DSN or miss the run's stop.
#[derive(Clone)]
pub(crate) struct CaptureCtx {
    dsn: crate::credentials::SharedDsn,
    cancel: CancellationToken,
    /// The source's control-plane pool: every capture's connection.
    control: super::mysql_control::ControlPool,
}

impl CaptureCtx {
    /// The run's context. Called once, where the run starts.
    pub(crate) fn for_run(
        dsn: crate::credentials::SharedDsn,
        cancel: CancellationToken,
        control: super::mysql_control::ControlPool,
    ) -> Self {
        Self {
            dsn,
            cancel,
            control,
        }
    }

    /// The run's authoritative DSN.
    pub(crate) fn dsn(&self) -> &crate::credentials::SharedDsn {
        &self.dsn
    }

    /// The source's control-plane pool.
    pub(crate) fn control(&self) -> &super::mysql_control::ControlPool {
        &self.control
    }

    /// A context of its own (tests), with a pool of its own of the default
    /// size on the runner's budget, stopped with [`CaptureCtx::stop`].
    #[cfg(test)]
    pub(crate) fn detached(
        dsn: impl Into<crate::credentials::SharedDsn>,
    ) -> Self {
        Self {
            dsn: dsn.into(),
            cancel: CancellationToken::new(),
            control: super::mysql_control::ControlPool::detached(
                super::mysql_control::budget(),
                "detached",
                super::mysql_control::DEFAULT_POOL_SIZE,
            ),
        }
    }

    /// A context of its own on `control` (tests).
    #[cfg(test)]
    pub(crate) fn detached_on(
        dsn: impl Into<crate::credentials::SharedDsn>,
        control: super::mysql_control::ControlPool,
    ) -> Self {
        Self {
            dsn: dsn.into(),
            cancel: CancellationToken::new(),
            control,
        }
    }

    /// Stop the context's run (tests).
    #[cfg(test)]
    pub(crate) fn stop(&self) {
        self.cancel.cancel();
    }
}

fn add_gtid(set: &mut GtidSet, gtid: &str) -> Result<(), ProofError> {
    if set.insert(gtid) {
        Ok(())
    } else {
        Err(ProofError::Framing(format!("malformed GTID {gtid:?}")))
    }
}

fn single(gtid: &str) -> Result<GtidSet, ProofError> {
    GtidSet::single(gtid)
        .ok_or_else(|| ProofError::Framing(format!("malformed GTID {gtid:?}")))
}

fn lp(h: &mut Sha256, bytes: &[u8]) {
    h.update((bytes.len() as u64).to_be_bytes());
    h.update(bytes);
}

pub(super) fn encode_effect(e: &DdlEffect) -> String {
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

pub(super) fn encode_event(e: &EventIdentity) -> String {
    match e {
        EventIdentity::Gtid { gtid, ordinal } => {
            format!("gtid\u{1d}{gtid}\u{1d}{ordinal}")
        }
        // Statements never carry a rows-event ordinal.
        EventIdentity::FilePos { file, end_pos, .. } => {
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

/// STOP_EVENT: the server stopped writing this file (a restart follows).
const STOP_EVENT: u8 = 3;
/// INCIDENT_EVENT.
const INCIDENT_EVENT: u8 = 26;

/// Where a stream opened at an anchor stands, before its first GTID.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Chain {
    /// Nothing yet: the server's artificial Rotate must name the anchor.
    Opening,
    /// At the anchor: the first real event must start at its offset.
    AtAnchor,
    /// A Stop at the anchor offset: only the file's Rotate may follow.
    Stopped,
    /// Rotated to the next file: its setup events, then its first GTID.
    Rotated,
    /// The first GTID was validated, at this file and offset.
    Passed { file: String, offset: u64 },
}

/// The validation of a stream opened at an [`Anchor`]
/// (`docs/design/mysql-proof-seek.md`, "Validation after opening a stream at
/// an anchor"). Fed every event before the first GTID. It accepts only:
/// - the server's artificial Rotate naming the anchor;
/// - its Format_description;
/// - then, starting exactly at the anchor offset, either the GTID event,
///   or a rotation to the next file in sequence: the file's own Rotate, or
///   a Stop and the server's artificial Rotate as it opens the next file.
///   The next file's artificial Rotate and setup events follow, then its
///   first GTID (or, for a file with no transactions, its own rotation).
///
/// The first GTID must not be in the anchor's `set_before`, and must be
/// `next_gtid` when that is known. Anything transactional before it is
/// refused. It changes nothing: the walk still sees every event.
#[derive(Debug)]
pub(crate) struct AnchorCheck {
    anchor: Anchor,
    /// The file the next positioned event belongs to.
    file: String,
    state: Chain,
}

impl AnchorCheck {
    pub(crate) fn new(anchor: Anchor) -> Self {
        Self {
            file: anchor.file.clone(),
            anchor,
            state: Chain::Opening,
        }
    }

    /// The file and offset of the first GTID, once validated: the
    /// effective physical start.
    pub(crate) fn passed(&self) -> Option<(&str, u64)> {
        match &self.state {
            Chain::Passed { file, offset } => Some((file, *offset)),
            _ => None,
        }
    }

    fn refuse(&self, why: String) -> ProofError {
        ProofError::AnchorMismatch(format!(
            "{why} (anchor {}:{}, state {:?})",
            self.anchor.file, self.anchor.offset, self.state
        ))
    }

    /// A rotation to the next file of the server's normal sequence.
    fn rotate_to(&mut self, r: &RotateEvent) -> Result<(), ProofError> {
        if !is_next_file(&self.file, &r.binlog_filename)
            || r.binlog_position != 4
        {
            return Err(self.refuse(format!(
                "a Rotate out of sequence, to {}:{}",
                r.binlog_filename, r.binlog_position
            )));
        }
        self.file = r.binlog_filename.clone();
        self.state = Chain::Rotated;
        Ok(())
    }

    /// Validate one event received before the first GTID.
    pub(crate) fn check(
        &mut self,
        header: &EventHeader,
        data: &EventData,
    ) -> Result<(), ProofError> {
        if self.passed().is_some() {
            return Ok(());
        }
        let artificial = header.next_event_position == 0;
        let start = u64::from(header.next_event_position)
            .saturating_sub(u64::from(header.event_length));
        let at_anchor =
            |c: &Self| c.file == c.anchor.file && start == c.anchor.offset;
        match (data, &self.state) {
            (EventData::HeartBeat, _) => Ok(()),
            (EventData::Rotate(r), Chain::Opening) if artificial => {
                if r.binlog_filename == self.anchor.file
                    && r.binlog_position == self.anchor.offset
                {
                    self.state = Chain::AtAnchor;
                    Ok(())
                } else {
                    Err(self.refuse(format!(
                        "the stream opened at {}:{}",
                        r.binlog_filename, r.binlog_position
                    )))
                }
            }
            (_, Chain::Opening) => Err(self.refuse(
                "the stream did not open with a Rotate to the anchor".into(),
            )),
            (
                EventData::FormatDescription(_),
                Chain::AtAnchor | Chain::Rotated,
            ) => Ok(()),
            (EventData::PreviousGtids(_), Chain::Rotated) => Ok(()),
            (EventData::NotSupported, Chain::AtAnchor | Chain::Rotated)
                if header.event_type == STOP_EVENT && !artificial =>
            {
                if self.state == Chain::Rotated || at_anchor(self) {
                    self.state = Chain::Stopped;
                    Ok(())
                } else {
                    Err(self.refuse(format!("a Stop at offset {start}")))
                }
            }
            // The file's own Rotate, at the end of the anchor's file or of
            // an empty file after it; or, after a Stop (a file the server
            // stopped writing has no Rotate of its own), the server's
            // artificial Rotate as it opens the next file.
            (EventData::Rotate(r), Chain::AtAnchor | Chain::Rotated)
                if !artificial =>
            {
                if self.state == Chain::AtAnchor && !at_anchor(self) {
                    return Err(self.refuse(format!(
                        "the file's Rotate at offset {start}"
                    )));
                }
                self.rotate_to(r)
            }
            (EventData::Rotate(r), Chain::Stopped) if artificial => {
                self.rotate_to(r)
            }
            // The server's artificial Rotate as it opens the file the chain
            // just rotated to.
            (EventData::Rotate(r), Chain::Rotated) if artificial => {
                if r.binlog_filename == self.file && r.binlog_position == 4 {
                    Ok(())
                } else {
                    Err(self.refuse(format!(
                        "the server opened {}:{}",
                        r.binlog_filename, r.binlog_position
                    )))
                }
            }
            (EventData::Gtid(g), Chain::AtAnchor | Chain::Rotated) => {
                if self.state == Chain::AtAnchor && !at_anchor(self) {
                    return Err(self.refuse(format!(
                        "the first GTID {} starts at offset {start}",
                        g.gtid
                    )));
                }
                if self.anchor.set_before.contains(&g.gtid) {
                    return Err(self.refuse(format!(
                        "the first GTID {} is already in the anchor's set",
                        g.gtid
                    )));
                }
                if let Some(want) = &self.anchor.next_gtid
                    && !want.eq_ignore_ascii_case(&g.gtid)
                {
                    return Err(self.refuse(format!(
                        "the first GTID is {}, not {want}",
                        g.gtid
                    )));
                }
                self.state = Chain::Passed {
                    file: self.file.clone(),
                    offset: start,
                };
                Ok(())
            }
            (EventData::NotSupported, _)
                if header.event_type == INCIDENT_EVENT =>
            {
                Err(ProofError::Incident)
            }
            (other, _) => Err(self.refuse(format!(
                "a {} event before the first GTID",
                event_kind(header, other)
            ))),
        }
    }
}

/// `next` is the binlog file after `file`: same base name, index plus one.
fn is_next_file(file: &str, next: &str) -> bool {
    use crate::durable_checkpoint::binlog_file_parts;
    match (binlog_file_parts(file), binlog_file_parts(next)) {
        (Some((b1, i1)), Some((b2, i2))) => b1 == b2 && i2 == i1 + 1,
        _ => false,
    }
}

/// A short name for an event, for refusals.
fn event_kind(header: &EventHeader, data: &EventData) -> String {
    match data {
        EventData::Query(q) => {
            format!("query ({})", q.query.chars().take(40).collect::<String>())
        }
        EventData::TableMap(_) => "table map".into(),
        EventData::WriteRows(_)
        | EventData::UpdateRows(_)
        | EventData::DeleteRows(_) => "rows".into(),
        EventData::Xid(_) => "Xid".into(),
        EventData::TransactionPayload(_) => "transaction payload".into(),
        EventData::Rotate(_) => "Rotate".into(),
        EventData::FormatDescription(_) => "Format_description".into(),
        EventData::PreviousGtids(_) => "Previous_gtids".into(),
        EventData::Gtid(_) => "GTID".into(),
        EventData::NotSupported if header.event_type == ANONYMOUS_GTID => {
            "anonymous GTID".into()
        }
        _ => format!("type {}", header.event_type),
    }
}

struct Walk {
    gtid_mode: bool,
    to_set: Option<GtidSet>,
    done: GtidSet,
    current_gtid: Option<String>,
    ordinal: u32,
    in_explicit_txn: bool,
    /// The version of the server that wrote the binlog (its format
    /// description): it evaluated versioned executable comments.
    server_version: Option<u32>,
    file: String,
    statements: Vec<Statement>,
    reached: bool,
    /// The GTID of the last transaction completed (the terminal one once
    /// `reached`).
    last_completed: Option<String>,
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
            if !single(&g)?.is_subset(to) {
                return Err(ProofError::Overshoot(format!(
                    "transaction {g} is not part of the end position"
                )));
            }
            add_gtid(&mut self.done, &g)?;
            if to.is_subset(&self.done) {
                self.reached = true;
            }
            self.last_completed = Some(g);
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
                ordinal: 0,
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
                    let effect = classify(
                        &q.query,
                        Some(&q.schema),
                        self.server_version,
                    );
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
            EventData::FormatDescription(fde) => {
                self.server_version =
                    super::mysql_ddl_attribution::parse_server_version(
                        &fde.server_version,
                    );
            }
            EventData::PreviousGtids(_)
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
#[allow(clippy::too_many_arguments)]
pub(crate) async fn scan_interval(
    dsn: &str,
    server_id: u64,
    server_uuid: &str,
    lineage_hash: &str,
    from: &MySqlCheckpoint,
    to: &MySqlCheckpoint,
    limits: &ScanLimits,
    tag: &ScanTag<'_>,
) -> Result<ScanReport, ProofError> {
    scan_interval_at(
        dsn,
        server_id,
        server_uuid,
        lineage_hash,
        from,
        to,
        None,
        limits,
        tag,
    )
    .await
}

/// [`scan_interval`], opening a GTID-mode stream at `anchor` when one is
/// given: an anchor taken at `from` (its set before is exactly `from`'s), of
/// this server and lineage. Without one, the server seeks from the start of
/// the file, bounded by `limits.max_seek`.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn scan_interval_at(
    dsn: &str,
    server_id: u64,
    server_uuid: &str,
    lineage_hash: &str,
    from: &MySqlCheckpoint,
    to: &MySqlCheckpoint,
    anchor: Option<&Anchor>,
    limits: &ScanLimits,
    tag: &ScanTag<'_>,
) -> Result<ScanReport, ProofError> {
    let scan_id =
        NEXT_SCAN_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let detail = trace_enabled();
    let mut trace = ScanTrace {
        scan_id,
        ..Default::default()
    };
    let result = scan(
        tag.source_id,
        dsn,
        server_id,
        server_uuid,
        lineage_hash,
        from,
        to,
        anchor,
        limits,
        detail,
        &mut trace,
    )
    .await;
    observe_scan(tag, from, to, &trace, &result);
    result.map(|mut r| {
        r.trace = trace;
        r
    })
}

/// Bounded metrics for every scan; the qualification record only while the
/// trace target is enabled. Never fails.
fn observe_scan(
    tag: &ScanTag<'_>,
    from: &MySqlCheckpoint,
    to: &MySqlCheckpoint,
    trace: &ScanTrace,
    result: &Result<ScanReport, ProofError>,
) {
    let outcome = if result.is_ok() { "ok" } else { "failed" };
    let labels = [
        ("source", tag.source_id.to_string()),
        ("kind", tag.kind.as_str().to_string()),
        ("outcome", outcome.to_string()),
    ];
    let (events, bytes) = match result {
        Ok(r) => (r.events, r.bytes),
        Err(_) => (0, 0),
    };
    metrics::histogram!("deltaforge_mysql_proof_scan_events", &labels)
        .record(events as f64);
    metrics::histogram!("deltaforge_mysql_proof_scan_bytes", &labels)
        .record(bytes as f64);
    metrics::histogram!("deltaforge_mysql_proof_scan_setup_seconds", &labels)
        .record(trace.setup.as_secs_f64());
    metrics::histogram!("deltaforge_mysql_proof_scan_seek_seconds", &labels)
        .record(trace.seek.as_secs_f64());
    metrics::histogram!("deltaforge_mysql_proof_scan_seconds", &labels)
        .record(trace.scan.as_secs_f64());
    if !trace_enabled() {
        return;
    }
    let record = serde_json::json!({
        "record": "scan",
        "scan_id": trace.scan_id,
        "source": tag.source_id,
        "kind": tag.kind.as_str(),
        "outcome": outcome,
        "error": result.as_ref().err().map(|e| e.to_string()),
        "from": {"file": from.file, "pos": from.pos, "gtid_set": from.gtid_set},
        "to": {"file": to.file, "pos": to.pos, "gtid_set": to.gtid_set},
        "events": events,
        "bytes": bytes,
        "setup_ms": trace.setup.as_secs_f64() * 1e3,
        "seek_ms": trace.seek.as_secs_f64() * 1e3,
        "scan_ms": trace.scan.as_secs_f64() * 1e3,
        "first": trace.first,
        "terminal": trace.terminal,
        "detail": trace.detail,
    });
    tracing::info!(target: "deltaforge::proof_trace", "{record}");
}

/// The source server's transactions in a GTID set (other UUIDs ignored).
fn source_txns(set: &GtidSet, server_uuid: &str) -> u64 {
    set.intervals()
        .filter(|(u, _)| u.eq_ignore_ascii_case(server_uuid))
        .flat_map(|(_, ivs)| ivs.iter())
        .map(|(a, b)| b.saturating_sub(*a) + 1)
        .sum()
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_arguments)]
async fn scan(
    source: &str,
    dsn: &str,
    server_id: u64,
    server_uuid: &str,
    lineage_hash: &str,
    from: &MySqlCheckpoint,
    to: &MySqlCheckpoint,
    anchor: Option<&Anchor>,
    limits: &ScanLimits,
    detail: bool,
    trace: &mut ScanTrace,
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
        trace: ScanTrace::default(),
        frontier: None,
        session_anchor: None,
        positioned_at: None,
        work: ProofWork::default(),
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
        // Short, so the server writes to the session soon after the scan
        // resets it and ends its dump thread (see the walk below).
        heartbeat_interval_secs: 1,
        timeout_secs: 30,
        ..Default::default()
    };
    let to_set = if gtid_mode {
        client.gtid_enabled = true;
        client.gtid_set = from.gtid_set.clone().unwrap_or_default();
        Some(
            GtidSet::parse(to.gtid_set.as_deref().unwrap_or_default())
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
        GtidSet::parse(from.gtid_set.as_deref().unwrap_or_default())
            .ok_or_else(|| {
                ProofError::Positions("unparseable start GTID set".into())
            })?
    } else {
        GtidSet::new()
    };
    // An anchor: the physical start of a GTID-mode stream. Only one taken at
    // this request's A, of this server and lineage, is used; the stream it
    // opens is validated before any statement counts (`AnchorCheck`).
    let mut check = match anchor {
        None => None,
        Some(a) => {
            if !gtid_mode {
                return Err(ProofError::Positions(
                    "an anchor for a file/position interval".into(),
                ));
            }
            if !a.server_uuid.eq_ignore_ascii_case(server_uuid)
                || a.lineage != lineage_hash
            {
                return Err(ProofError::OtherServer(format!(
                    "an anchor of {} (lineage {})",
                    a.server_uuid, a.lineage
                )));
            }
            if a.set_before != done {
                return Err(ProofError::Positions(format!(
                    "an anchor after {} for an interval from {done}",
                    a.set_before
                )));
            }
            client.gtid_start = Some((
                a.file.clone(),
                u32::try_from(a.offset).map_err(|_| {
                    ProofError::Positions("anchor offset out of range".into())
                })?,
            ));
            Some(AnchorCheck::new(a.clone()))
        }
    };
    let from_set = done.clone();
    // Trace only: the start set (to recognise a transaction decoded before
    // the start) and the source server's transactions in the interval.
    let start_set = (detail && gtid_mode).then(|| done.clone());
    let mut det = detail.then(|| ScanDetail {
        interval_source_txns: to_set.as_ref().map(|to| {
            source_txns(to, server_uuid)
                .saturating_sub(source_txns(&done, server_uuid))
        }),
        ..Default::default()
    });
    let mut walk = Walk {
        gtid_mode,
        to_set,
        done,
        current_gtid: None,
        ordinal: 0,
        in_explicit_txn: false,
        server_version: None,
        file: from.file.clone(),
        statements: Vec::new(),
        reached: false,
        last_completed: None,
    };

    let mut stream = tokio::time::timeout(
        limits.max_duration,
        open_replication_session_abortive(&client, server_uuid, source),
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
    let opened = Instant::now();
    trace.setup = opened - started;
    let mut first_at: Option<Instant> = None;
    // The first transaction's GTID event: file, start offset and GTID.
    let mut first_txn: Option<(String, u64, String)> = None;
    // The session's abortive close was armed right after it connected (see
    // `open_replication_session_abortive`): however this scan ends -
    // completion, error, timeout, or the future being cancelled or dropped -
    // dropping it resets the connection, the server's next write to it (at
    // the latest the 1 s heartbeat on an idle server) fails and its dump
    // thread ends. No connection is ever killed by id (ids can be reused).
    let walked: Result<(u64, u64), ProofError> = async {
        let (mut events, mut bytes) = (0u64, 0u64);
        // The position of the last heartbeat at or past the end position:
        // two in a row there, with the end not reached, is the live end.
        let mut beat_at_end: Option<u64> = None;
        while !walk.reached {
            let mut left = limits
                .max_duration
                .checked_sub(started.elapsed())
                .ok_or_else(|| ProofError::Limit("time".into()))?;
            // In GTID mode, until the first transaction, the wait is also
            // bounded by the seek bound (the server's seek through the
            // file's earlier events, for a stream without an anchor).
            let seeking = gtid_mode && first_txn.is_none();
            if seeking {
                let seek_left =
                    limits.max_seek.checked_sub(opened.elapsed()).ok_or_else(
                        || seek_limit(limits.max_seek),
                    )?;
                left = left.min(seek_left);
            }
            let (header, data) = match tokio::time::timeout(left, stream.read())
                .await
            {
                Ok(r) => r.map_err(|e| ProofError::Read(e.to_string()))?,
                Err(_) if seeking && opened.elapsed() >= limits.max_seek => {
                    return Err(seek_limit(limits.max_seek));
                }
                Err(_) => return Err(ProofError::Limit("time".into())),
            };
            if let Some(c) = check.as_mut() {
                c.check(&header, &data)?;
            }
            if gtid_mode && matches!(data, EventData::HeartBeat) {
                let here = u64::from(header.next_event_position);
                if at_or_past(&walk.file, here, to) {
                    if beat_at_end == Some(here) {
                        return Err(ProofError::NotReached(format!(
                            "the stream is at its live end ({}:{here}) without \
                             the end position",
                            walk.file
                        )));
                    }
                    beat_at_end = Some(here);
                } else {
                    beat_at_end = None;
                }
            } else if !matches!(data, EventData::HeartBeat) {
                beat_at_end = None;
            }
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
            let protocol = matches!(
                data,
                EventData::HeartBeat
                    | EventData::Rotate(_)
                    | EventData::FormatDescription(_)
                    | EventData::PreviousGtids(_)
            );
            if let Some(d) = det.as_mut() {
                observe_event(
                    d,
                    &header,
                    &data,
                    &walk,
                    start_set.as_ref(),
                    &pf,
                    server_uuid,
                );
            }
            walk.event(&header, &data)?;
            if let EventData::Gtid(g) = &data
                && first_txn.is_none()
            {
                first_txn = Some((
                    walk.file.clone(),
                    u64::from(header.next_event_position)
                        .saturating_sub(u64::from(header.event_length)),
                    g.gtid.clone(),
                ));
            }
            if !protocol && trace.first.is_none() {
                first_at = Some(Instant::now());
                trace.first = Some(DecodedAt {
                    file: walk.file.clone(),
                    end_pos: u64::from(header.next_event_position),
                    gtid: walk.current_gtid.clone(),
                });
            }
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
            if walk.reached {
                trace.terminal = Some(DecodedAt {
                    file: walk.file.clone(),
                    end_pos: u64::from(header.next_event_position),
                    gtid: walk.last_completed.clone(),
                });
            }
        }
        Ok((events, bytes))
    }
    .await;
    drop(stream);
    let now = Instant::now();
    match first_at {
        Some(f) => {
            trace.seek = f - opened;
            trace.scan = now - f;
        }
        None => trace.seek = now - opened,
    }
    trace.detail = det;
    let (events, bytes) = walked?;
    let anchor_at =
        |file: String, offset: u64, set_before: GtidSet, next_gtid| Anchor {
            server_uuid: server_uuid.to_string(),
            lineage: lineage_hash.to_string(),
            file,
            offset,
            set_before,
            next_gtid,
        };
    // The boundary after the last transaction of `to` (at reach, `done` is
    // exactly `to`: a GTID outside it is an overshoot).
    let frontier = (gtid_mode && walk.reached)
        .then(|| trace.terminal.clone())
        .flatten()
        .map(|t| anchor_at(t.file, t.end_pos, walk.done.clone(), None));
    let session_anchor =
        first_txn.filter(|_| gtid_mode).map(|(file, offset, gtid)| {
            anchor_at(file, offset, from_set.clone(), Some(gtid))
        });
    Ok(ScanReport {
        digest: scan_digest(&pf, &pt, &walk.statements),
        statements: walk.statements,
        events,
        bytes,
        duration: started.elapsed(),
        trace: ScanTrace::default(),
        frontier,
        session_anchor,
        positioned_at: check
            .as_ref()
            .and_then(|c| c.passed())
            .map(|(f, o)| (f.to_string(), o)),
        work: ProofWork::default(),
    })
}

fn seek_limit(bound: Duration) -> ProofError {
    ProofError::SeekLimit(format!("no transaction within {bound:?} of opening"))
}

/// Is the stream at `(file, pos)` at or past `to`'s binlog position?
fn at_or_past(file: &str, pos: u64, to: &MySqlCheckpoint) -> bool {
    use crate::durable_checkpoint::binlog_file_parts;
    match (binlog_file_parts(file), binlog_file_parts(&to.file)) {
        (Some((b, i)), Some((tb, ti))) if b == tb => (i, pos) >= (ti, to.pos),
        _ => false,
    }
}

/// Classify one decoded event (trace target only; never fails).
fn observe_event(
    d: &mut ScanDetail,
    header: &EventHeader,
    data: &EventData,
    walk: &Walk,
    start_set: Option<&GtidSet>,
    start: &WmPos,
    server_uuid: &str,
) {
    match data {
        EventData::HeartBeat => d.heartbeats += 1,
        EventData::Rotate(_)
        | EventData::FormatDescription(_)
        | EventData::PreviousGtids(_) => d.protocol_events += 1,
        EventData::TableMap(_) => d.table_maps += 1,
        EventData::WriteRows(_)
        | EventData::UpdateRows(_)
        | EventData::DeleteRows(_) => d.row_events += 1,
        EventData::Query(_) => d.query_events += 1,
        _ => {}
    }
    match (data, start_set) {
        // GTID mode: a transaction already in the start set was decoded.
        (EventData::Gtid(g), Some(set)) => {
            if single(&g.gtid).is_ok_and(|one| one.is_subset(set)) {
                d.before_start += 1;
            }
            let source = g.gtid.rsplit_once(':').is_some_and(|(u, _)| {
                u.trim().eq_ignore_ascii_case(server_uuid)
            });
            if source {
                d.source_txns += 1;
            } else {
                d.other_txns += 1;
            }
        }
        // File mode: a transaction event ending at or before the start.
        (
            EventData::Query(_)
            | EventData::TableMap(_)
            | EventData::WriteRows(_)
            | EventData::UpdateRows(_)
            | EventData::DeleteRows(_)
            | EventData::Xid(_)
            | EventData::Gtid(_),
            None,
        ) => {
            let at = mysql_checkpoint_position(
                &walk.file,
                u64::from(header.next_event_position),
                None,
            );
            if at.is_some_and(|at| {
                matches!(
                    order_positions(&at, start),
                    CheckpointOrder::Before | CheckpointOrder::Equal
                )
            }) {
                d.before_start += 1;
            }
            if matches!(data, EventData::Xid(_)) {
                d.source_txns += 1;
            }
        }
        _ => {}
    }
}

/// One table's shape read between two server positions, on one connection
/// whose server identity was proven: `from` was read before the shape and
/// `position` after it (`from <= position`). The shape is the table's shape
/// at every point of `[from, position]` only if `(from, position]` holds no
/// DDL or barrier affecting the table: the caller proves that with a scan of
/// an interval covering it ([`covers`]), or uses [`capture_proven`].
/// Ordinary writes in the interval, and DDL on other tables, are irrelevant.
#[derive(Debug, Clone)]
pub(crate) struct Captured {
    pub from: MySqlCheckpoint,
    pub position: MySqlCheckpoint,
    pub schema: MySqlTableSchema,
    pub attempts: u32,
    pub trace: CaptureTrace,
}

/// Where a capture's time went (always collected), and why it was retried.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub(crate) struct CaptureTrace {
    /// Connection, GTID-mode and identity queries.
    pub connect: Duration,
    /// Waiting for the table's metadata lock (a DDL of the table running).
    pub lock_wait: Duration,
    /// The schema query itself, under the lock.
    pub schema: Duration,
    /// Both server position reads.
    pub positions: Duration,
    /// `capture_proven`: each retried attempt's cause.
    pub retries: Vec<String>,
}

/// Runs between the shape read and the second position read (tests use it
/// to land a statement inside the capture interval; production passes
/// `None`).
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
    // The server's text (it prints newlines), in the one canonical form.
    let gtid_set = if gtid_mode {
        let t = gtid.unwrap_or_default();
        Some(
            GtidSet::parse(&t)
                .ok_or_else(|| {
                    ProofError::Capture(format!(
                        "unparseable executed set {t:?}"
                    ))
                })?
                .to_string(),
        )
    } else {
        None
    };
    Ok(MySqlCheckpoint {
        file,
        pos,
        gtid_set,
        lineage: Some(lineage_hash.to_string()),
        snapshot_completed: None,
        snapshot_chain: None,
    })
}

/// Bound on waiting for the table's metadata lock (a DDL in progress on the
/// table): a capture that cannot lock fails, never reads unlocked.
pub(crate) const LOCK_WAIT_SECS: u32 = 30;

fn quoted(db: &str, table: &str) -> String {
    let q = |s: &str| format!("`{}`", s.replace('`', "``"));
    format!("{}.{}", q(db), q(table))
}

fn wm(c: &MySqlCheckpoint) -> Result<WmPos, ProofError> {
    mysql_checkpoint_position(&c.file, c.pos, c.gtid_set.as_deref()).ok_or_else(
        || ProofError::Positions(format!("unparseable position {c:?}")),
    )
}

/// The most captures holding metadata locks at once, runner-wide (`L`).
pub(crate) const MAX_LOCK_HOLDING_CAPTURES: usize = 4;
/// Cancellation connections reserved for them (`R` = `L`): every capture
/// that can need a kill has one, and a kill never waits for anything else.
pub(crate) const KILL_RESERVE: usize = MAX_LOCK_HOLDING_CAPTURES;
/// The bound on one cancellation (connect and `KILL`).
const KILL_TIMEOUT: Duration = Duration::from_secs(10);

/// The runner-wide capture bound and its cancellation reserve (one
/// guarantee: `kills` = `holding`).
pub(crate) struct CaptureSlots {
    holding: tokio::sync::Semaphore,
    kills: tokio::sync::Semaphore,
    /// Test-only: every cancellation waits this long before it connects
    /// (0 in production), so a test can observe what happens meanwhile.
    kill_delay_ms: std::sync::atomic::AtomicU64,
    /// Test-only: every cancellation fails before it connects.
    fail_kills: std::sync::atomic::AtomicBool,
    /// Slots retired after an unresolved cancellation (see `kill_failed`).
    retired: std::sync::atomic::AtomicU64,
}

impl CaptureSlots {
    pub(crate) const fn new(holding: usize, kills: usize) -> Self {
        Self {
            holding: tokio::sync::Semaphore::const_new(holding),
            kills: tokio::sync::Semaphore::const_new(kills),
            kill_delay_ms: std::sync::atomic::AtomicU64::new(0),
            fail_kills: std::sync::atomic::AtomicBool::new(false),
            retired: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Slots retired for the life of the process: their sessions may still
    /// hold metadata locks.
    pub(crate) fn retired(&self) -> u64 {
        self.retired.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// The runner's capture slots.
pub(crate) static CAPTURE_SLOTS: CaptureSlots =
    CaptureSlots::new(MAX_LOCK_HOLDING_CAPTURES, KILL_RESERVE);

static KILL_FAILURES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Cancellations that failed (could not connect, or `KILL` failed or timed
/// out), since the process started.
pub(crate) fn kill_failures() -> u64 {
    KILL_FAILURES.load(std::sync::atomic::Ordering::SeqCst)
}

/// Read `db.table`'s shape between two positions (module docs). Never
/// requires the server to be quiet: the positions may differ.
pub(crate) async fn capture(
    cx: &CaptureCtx,
    server_uuid: &str,
    lineage_hash: &str,
    db: &str,
    table: &str,
    between: Option<BetweenHook<'_>>,
) -> Result<Result<Captured, ProofError>, Cancelled> {
    capture_with(
        &CAPTURE_SLOTS,
        cx,
        server_uuid,
        lineage_hash,
        db,
        table,
        between,
    )
    .await
}

/// [`capture`] under the given capture slots.
pub(crate) async fn capture_with(
    slots: &'static CaptureSlots,
    cx: &CaptureCtx,
    server_uuid: &str,
    lineage_hash: &str,
    db: &str,
    table: &str,
    between: Option<BetweenHook<'_>>,
) -> Result<Result<Captured, ProofError>, Cancelled> {
    let dsn = &cx.dsn;
    let mut trace = CaptureTrace::default();
    let t0 = Instant::now();
    // Dropped mid-flight, the capture is killed on the server (see
    // `capture_batch_with`).
    let mut guard = KillOnDrop {
        dsn: dsn.clone(),
        source: cx.control.source().to_string(),
        id: 0,
        slots,
        permit: None,
    };
    let body = async {
        // A lock-holding slot first, within the lock-wait budget: a capture
        // waiting for one holds no connection. Then a connection of the
        // source's pool, proven to be the server, within what remains;
        // then the table's metadata lock, held until COMMIT (module docs).
        let budget = Duration::from_secs(u64::from(LOCK_WAIT_SECS));
        let waited = Instant::now();
        let permit = tokio::time::timeout(budget, slots.holding.acquire())
            .await
            .map_err(|_| {
                ProofError::Capture(format!(
                    "every lock-holding capture slot stayed busy for {LOCK_WAIT_SECS} s"
                ))
            })?
            .expect("capture slots are never closed");
        guard.permit = Some(permit);
        let mut conn = cx
            .control
            .connect(
                super::mysql_control::Class::Capture,
                dsn.current().expose(),
                server_uuid,
                budget.saturating_sub(waited.elapsed()),
            )
            .await
            .map_err(control_error)?;
        guard.id = conn.id();
        let mode: Option<String> = conn
            .query_first("SELECT @@GLOBAL.gtid_mode")
            .await
            .map_err(|e| ProofError::Capture(e.to_string()))?;
        let gtid_mode = mode.is_some_and(|m| m.eq_ignore_ascii_case("ON"));
        let remaining =
            budget.saturating_sub(waited.elapsed()).as_secs().max(1);
        for q in [
            format!("SET SESSION lock_wait_timeout = {remaining}"),
            "START TRANSACTION READ ONLY".to_string(),
        ] {
            conn.query_drop(&q)
                .await
                .map_err(|e| ProofError::Capture(format!("{q}: {e}")))?;
        }
        trace.connect = t0.elapsed();
        let t = Instant::now();
        let lock = format!("SELECT 1 FROM {} LIMIT 0", quoted(db, table));
        conn.query_drop(&lock)
            .await
            .map_err(|e| ProofError::Capture(format!("{lock}: {e}")))?;
        trace.lock_wait = t.elapsed();
        let t = Instant::now();
        let from = read_position(&mut conn, gtid_mode, lineage_hash).await?;
        trace.positions = t.elapsed();
        let t = Instant::now();
        let schema = match fetch_table_schema_on(
            &mut conn,
            server_uuid,
            db,
            table,
        )
        .await
        {
            Ok(Live::Found(s)) => s,
            Ok(Live::OtherLineage(who)) => {
                return Err(ProofError::OtherServer(who));
            }
            Err(e) => return Err(ProofError::Capture(e.to_string())),
        };
        trace.schema = t.elapsed();
        if let Some(hook) = between {
            hook().await;
        }
        let t = Instant::now();
        let position =
            read_position(&mut conn, gtid_mode, lineage_hash).await?;
        trace.positions += t.elapsed();
        match order_positions(&wm(&from)?, &wm(&position)?) {
            CheckpointOrder::Before | CheckpointOrder::Equal => {}
            other => {
                return Err(ProofError::Positions(format!(
                    "the server position went backwards during the capture ({other:?})"
                )));
            }
        }
        conn.query_drop("COMMIT")
            .await
            .map_err(|e| ProofError::Capture(format!("COMMIT: {e}")))?;
        // Unarmed first: a returned connection is never killed.
        guard.id = 0;
        conn.release().await;
        Ok(Captured {
            from,
            position,
            schema,
            attempts: 1,
            trace: CaptureTrace::default(),
        })
    };
    let result = tokio::select! {
        biased;
        // A stop: the capture is dropped where it stands and its armed
        // guard kills its session (a lock wait ends at once).
        _ = cx.cancel.cancelled() => return Err(Cancelled),
        r = body => r,
    };
    guard.id = 0;
    guard.permit = None;
    Ok(result.map(|mut c| {
        c.trace = trace;
        c
    }))
}

/// Kills its connection on the server when dropped while armed (`id`
/// non-zero), from a detached task, bounded in time.
struct KillOnDrop {
    /// The run's DSN, read when the kill connects.
    dsn: crate::credentials::SharedDsn,
    /// Its source, which the kill connection is counted for.
    source: String,
    id: u32,
    slots: &'static CaptureSlots,
    /// The capture's lock-holding slot, held until its session is gone.
    permit: Option<tokio::sync::SemaphorePermit<'static>>,
}
impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let permit = self.permit.take();
        if self.id == 0 {
            return;
        }
        let (dsn, id, slots) = (self.dsn.clone(), self.id, self.slots);
        let source = std::mem::take(&mut self.source);
        let Ok(rt) = tokio::runtime::Handle::try_current() else {
            kill_failed(slots, permit, id, "no runtime to run the kill on");
            return;
        };
        rt.spawn(async move {
            // The kill draws on its own reserve (one per slot), never on a
            // pool or the captures' slots. Its slot returns only once the
            // session is known to be gone.
            let _reserve = slots.kills.acquire().await;
            #[cfg(test)]
            {
                let delay = slots
                    .kill_delay_ms
                    .load(std::sync::atomic::Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
            let kill = async {
                #[cfg(test)]
                if slots.fail_kills.load(std::sync::atomic::Ordering::SeqCst) {
                    return Err("injected kill failure".to_string());
                }
                let now = dsn.current();
                let _counted = super::mysql_connections::Counted::new(
                    &source,
                    super::mysql_connections::Kind::Cancellation,
                );
                let mut c = mysql_async::Conn::from_url(now.expose())
                    .await
                    .map_err(|e| format!("connect: {e}"))?;
                let killed = match c.query_drop(format!("KILL {id}")).await {
                    // Already gone (ER_NO_SUCH_THREAD): nothing left to kill.
                    Err(mysql_async::Error::Server(e)) if e.code == 1094 => {
                        Ok(())
                    }
                    other => other.map_err(|e| format!("KILL {id}: {e}")),
                };
                c.disconnect().await.ok();
                killed
            };
            match tokio::time::timeout(KILL_TIMEOUT, kill).await {
                // The session is gone: its slot returns.
                Ok(Ok(())) => drop(permit),
                Ok(Err(why)) => kill_failed(slots, permit, id, &why),
                Err(_) => kill_failed(
                    slots,
                    permit,
                    id,
                    &format!("timed out after {KILL_TIMEOUT:?}"),
                ),
            }
        });
    }
}

/// A cancellation did not resolve: the session may still hold metadata
/// locks, so its slot is retired for the life of the process (capacity
/// degrades safely until a restart; the bound is never exceeded). Counted,
/// logged and exported.
fn kill_failed(
    slots: &'static CaptureSlots,
    permit: Option<tokio::sync::SemaphorePermit<'static>>,
    id: u32,
    why: &str,
) {
    if let Some(p) = permit {
        p.forget();
        let retired = slots
            .retired
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        metrics::gauge!("deltaforge_mysql_capture_slots_retired")
            .set(retired as f64);
    }
    KILL_FAILURES.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    metrics::counter!("deltaforge_mysql_capture_kill_failures_total")
        .increment(1);
    tracing::warn!(
        session = id,
        why,
        "a dropped capture's session could not be killed; its slot is retired until restart"
    );
}

/// The shapes of a window's tables read between two server positions in one
/// transaction (`docs/design/mysql-proof-seek.md`, "Batched capture"): every
/// table's metadata lock is taken first, in sorted `(schema, table)` order,
/// and held across both position reads, so each table's shape is exactly
/// what [`capture`] would read over `[from, position]`.
#[derive(Debug, Clone)]
pub(crate) struct BatchCaptured {
    pub from: MySqlCheckpoint,
    pub position: MySqlCheckpoint,
    pub schemas: HashMap<(String, String), MySqlTableSchema>,
    /// Tables that do not exist (dropped since their rows): no shape.
    pub missing: Vec<(String, String)>,
    /// Tables past the capture's budget, in rank order: not read, so
    /// neither captured nor missing (`fetch_table_schemas_within`).
    pub deferred: Vec<(String, String)>,
    /// The first deferred table's bytes when its own rows exceeded the
    /// store's item limit.
    pub oversized: Option<u64>,
    pub trace: CaptureTrace,
}

impl BatchCaptured {
    /// One table's capture, as [`capture`] returns it.
    pub(crate) fn table(&self, db: &str, table: &str) -> Option<Captured> {
        let schema = self.schemas.get(&(db.to_string(), table.to_string()))?;
        Some(Captured {
            from: self.from.clone(),
            position: self.position.clone(),
            schema: schema.clone(),
            attempts: 1,
            trace: self.trace.clone(),
        })
    }
}

/// Why a batched capture failed: a lock it could not take (a timeout or a
/// deadlock: retried in the same sorted order) or anything else.
#[derive(Debug)]
pub(crate) enum BatchCaptureError {
    Contention { code: u16, why: String },
    Failed(ProofError),
}

/// The most tables one batched capture takes (an activation window closes
/// at it), and the most bytes of its lock request: one request a server
/// accepts whatever its `max_allowed_packet` (at least 1 MiB) and limit of
/// prepared-statement parameters.
pub(crate) const MAX_BATCH_TABLES: usize = 2048;
pub(crate) const MAX_LOCK_REQUEST_BYTES: usize = 1024 * 1024;

/// The lock statements of a batch, in its sorted order, as one request:
/// one `SELECT 1 FROM t LIMIT 0` per table. `None` when the request would
/// exceed its bounds.
fn lock_request(tables: &[(&str, &str)]) -> Option<String> {
    if tables.len() > MAX_BATCH_TABLES {
        return None;
    }
    let mut sql = String::new();
    for (db, table) in tables {
        sql.push_str("SELECT 1 FROM ");
        sql.push_str(&quoted(db, table));
        sql.push_str(" LIMIT 0;");
        if sql.len() > MAX_LOCK_REQUEST_BYTES {
            return None;
        }
    }
    Some(sql)
}

const ER_LOCK_WAIT_TIMEOUT: u16 = 1205;
const ER_LOCK_DEADLOCK: u16 = 1213;
const ER_NO_SUCH_TABLE: u16 = 1146;

fn server_code(e: &mysql_async::Error) -> Option<u16> {
    match e {
        mysql_async::Error::Server(s) => Some(s.code),
        _ => None,
    }
}

/// A failed statement of a batched capture: the connection (I/O or the
/// driver) as `Open`, anything the server answered as `Capture`.
/// A capture's control connection that could not be had: saturation keeps
/// its class, a connect failure is an open failure, another server is
/// refused.
fn control_error(e: super::mysql_control::ControlError) -> ProofError {
    use super::mysql_control::ControlError;
    use super::mysql_session::SessionError;
    match e {
        ControlError::Saturated { scope, .. } => ProofError::Saturated {
            scope,
            why: e.to_string(),
        },
        ControlError::Session(SessionError::Connect(why)) => {
            ProofError::Open(format!("connect: {why}"))
        }
        ControlError::Session(SessionError::OtherServer { found }) => {
            ProofError::OtherServer(format!("{found:?}"))
        }
        ControlError::Session(SessionError::NoIdentity(why)) => {
            ProofError::OtherServer(why)
        }
    }
}

fn batch_error(e: mysql_async::Error, what: &str) -> BatchCaptureError {
    BatchCaptureError::Failed(match e {
        mysql_async::Error::Io(_) | mysql_async::Error::Driver(_) => {
            ProofError::Open(format!("{what}: {e}"))
        }
        _ => ProofError::Capture(format!("{what}: {e}")),
    })
}

/// Take `tables`' metadata locks, in their order, in one request.
async fn lock_all(
    conn: &mut mysql_async::Conn,
    tables: &[(&str, &str)],
) -> Result<(), BatchCaptureError> {
    if tables.is_empty() {
        return Ok(());
    }
    let sql = lock_request(tables).ok_or_else(|| {
        BatchCaptureError::Failed(ProofError::Capture(format!(
            "the lock request for {} tables exceeds its bounds ({MAX_BATCH_TABLES} \
             tables, {MAX_LOCK_REQUEST_BYTES} bytes)",
            tables.len()
        )))
    })?;
    let lock_failed = |e: mysql_async::Error| match server_code(&e) {
        Some(code @ (ER_LOCK_WAIT_TIMEOUT | ER_LOCK_DEADLOCK)) => {
            BatchCaptureError::Contention {
                code,
                why: format!("metadata locks: {e}"),
            }
        }
        _ => batch_error(e, "metadata locks"),
    };
    // Exactly one result per statement, read in turn: a statement's error
    // (its lock timeout or deadlock) surfaces as its result is read.
    let mut result = conn.query_iter(sql).await.map_err(lock_failed)?;
    for _ in 0..tables.len() {
        result.for_each(|_| {}).await.map_err(lock_failed)?;
    }
    let extra = !result.is_empty();
    result.drop_result().await.map_err(lock_failed)?;
    if extra {
        return Err(BatchCaptureError::Failed(ProofError::Capture(format!(
            "the lock request returned more than its {} results",
            tables.len()
        ))));
    }
    Ok(())
}

/// One batched capture attempt (see [`BatchCaptured`]); `lock_wait_secs`
/// bounds each table's lock wait. `after_first_lock` runs once the first
/// table is locked (tests use it to race a multi-table DDL; production
/// passes `None`).
pub(crate) async fn capture_batch(
    cx: &CaptureCtx,
    server_uuid: &str,
    lineage_hash: &str,
    tables: &[(String, String)],
    lock_wait_secs: u32,
    after_first_lock: Option<BetweenHook<'_>>,
) -> Result<Result<BatchCaptured, BatchCaptureError>, Cancelled> {
    capture_batch_with(
        &CAPTURE_SLOTS,
        cx,
        server_uuid,
        lineage_hash,
        tables,
        lock_wait_secs,
        after_first_lock,
    )
    .await
}

/// [`capture_batch`] under the given capture slots.
pub(crate) async fn capture_batch_with(
    slots: &'static CaptureSlots,
    cx: &CaptureCtx,
    server_uuid: &str,
    lineage_hash: &str,
    tables: &[(String, String)],
    lock_wait_secs: u32,
    after_first_lock: Option<BetweenHook<'_>>,
) -> Result<Result<BatchCaptured, BatchCaptureError>, Cancelled> {
    let dsn = &cx.dsn;
    let failed = |e: ProofError| BatchCaptureError::Failed(e);
    // The window's rank order (first touch) for the schema read; the
    // sorted order for the locks.
    let mut ranked: Vec<(String, String)> = Vec::with_capacity(tables.len());
    for t in tables {
        if !ranked.contains(t) {
            ranked.push(t.clone());
        }
    }
    let mut sorted: Vec<(String, String)> = ranked.clone();
    sorted.sort();
    let mut trace = CaptureTrace::default();
    let t0 = Instant::now();
    // A capture dropped mid-flight (its window failed, was abandoned or
    // the run stopped) is killed on the server: its read-only transaction
    // rolls back and its metadata locks are released at once, instead of
    // when its query ends.
    let mut guard = KillOnDrop {
        dsn: dsn.clone(),
        source: cx.control.source().to_string(),
        id: 0,
        slots,
        permit: None,
    };
    let body = async {
        // A lock-holding slot first (the runner-wide bound), within the
        // lock-wait budget: a capture waiting for one holds no connection
        // and no lock. Then a connection of the source's pool, proven to be
        // the server (nothing is locked or read on another), within what
        // remains; the session's own lock wait gets the rest.
        let budget = Duration::from_secs(u64::from(lock_wait_secs));
        let waited = Instant::now();
        let permit = tokio::time::timeout(budget, slots.holding.acquire())
            .await
            .map_err(|_| BatchCaptureError::Contention {
                code: 0,
                why: format!(
                    "every lock-holding capture slot stayed busy for {lock_wait_secs} s"
                ),
            })?
            .expect("capture slots are never closed");
        guard.permit = Some(permit);
        let mut conn = cx
            .control
            .connect(
                super::mysql_control::Class::Capture,
                dsn.current().expose(),
                server_uuid,
                budget.saturating_sub(waited.elapsed()),
            )
            .await
            .map_err(|e| failed(control_error(e)))?;
        guard.id = conn.id();
        let mode: Option<String> = conn
            .query_first("SELECT @@GLOBAL.gtid_mode")
            .await
            .map_err(|e| batch_error(e, "gtid_mode"))?;
        let gtid_mode = mode.is_some_and(|m| m.eq_ignore_ascii_case("ON"));
        let remaining =
            budget.saturating_sub(waited.elapsed()).as_secs().max(1);
        for q in [
            format!("SET SESSION lock_wait_timeout = {remaining}"),
            "START TRANSACTION READ ONLY".to_string(),
        ] {
            conn.query_drop(&q).await.map_err(|e| batch_error(e, &q))?;
        }
        trace.connect = t0.elapsed();
        // Which tables exist (one query): a dropped table has no shape and
        // no lock. One dropped after this check fails the attempt (1146).
        let t = Instant::now();
        let mut missing = Vec::new();
        let present: Vec<(&str, &str)> = {
            let pairs = vec!["(?, ?)"; sorted.len()].join(", ");
            let params: Vec<mysql_async::Value> = sorted
                .iter()
                .flat_map(|(db, table)| {
                    [db.as_str().into(), table.as_str().into()]
                })
                .collect();
            let existing: Vec<(String, String)> = if sorted.is_empty() {
                Vec::new()
            } else {
                conn.exec(
                    format!(
                        "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES \
                         WHERE (TABLE_SCHEMA, TABLE_NAME) IN ({pairs})"
                    ),
                    params,
                )
                .await
                .map_err(|e| batch_error(e, "table existence"))?
            };
            let existing: std::collections::HashSet<(String, String)> =
                existing.into_iter().collect();
            sorted
                .iter()
                .filter_map(|k| {
                    if existing.contains(k) {
                        Some((k.0.as_str(), k.1.as_str()))
                    } else {
                        missing.push(k.clone());
                        None
                    }
                })
                .collect()
        };
        // Every table's metadata lock, in sorted order (any other order can
        // deadlock against a multi-table DDL), held until COMMIT: one
        // request, every statement's result consumed, so a lock timeout or
        // deadlock of any of them surfaces with its code. A request beyond
        // its bounds fails the capture (never one round trip per table).
        let rest = match (after_first_lock, present.split_first()) {
            (Some(hook), Some((one, rest))) => {
                lock_all(&mut conn, std::slice::from_ref(one)).await?;
                hook().await;
                rest
            }
            _ => &present[..],
        };
        lock_all(&mut conn, rest).await?;
        trace.lock_wait = t.elapsed();
        let t = Instant::now();
        let from = read_position(&mut conn, gtid_mode, lineage_hash)
            .await
            .map_err(failed)?;
        trace.positions = t.elapsed();
        let t = Instant::now();
        // In rank order, within the plan budget: the tables past it are
        // deferred, unread (`mysql_window_budget`).
        let in_rank: Vec<(&str, &str)> = ranked
            .iter()
            .filter(|k| present.contains(&(k.0.as_str(), k.1.as_str())))
            .map(|(db, table)| (db.as_str(), table.as_str()))
            .collect();
        let within = super::mysql_schema_loader::fetch_table_schemas_within(
            &mut conn,
            &in_rank,
            super::mysql_window_budget::Caps::current().plan.bytes,
            storage::MAX_STATE_ITEM_BYTES as u64,
        )
        .await
        .map_err(|e| failed(ProofError::Capture(e.to_string())))?;
        trace.schema = t.elapsed();
        let schemas = within.schemas;
        for (db, table) in &in_rank[..within.kept] {
            if !schemas.contains_key(&(db.to_string(), table.to_string())) {
                missing.push((db.to_string(), table.to_string()));
            }
        }
        let deferred: Vec<(String, String)> = in_rank[within.kept..]
            .iter()
            .map(|(db, table)| (db.to_string(), table.to_string()))
            .collect();
        let t = Instant::now();
        let position = read_position(&mut conn, gtid_mode, lineage_hash)
            .await
            .map_err(failed)?;
        trace.positions += t.elapsed();
        match order_positions(
            &wm(&from).map_err(failed)?,
            &wm(&position).map_err(failed)?,
        ) {
            CheckpointOrder::Before | CheckpointOrder::Equal => {}
            other => {
                return Err(failed(ProofError::Positions(format!(
                    "the server position went backwards during the capture ({other:?})"
                ))));
            }
        }
        conn.query_drop("COMMIT")
            .await
            .map_err(|e| batch_error(e, "COMMIT"))?;
        // Unarmed first: a returned connection is never killed.
        guard.id = 0;
        conn.release().await;
        Ok(BatchCaptured {
            from,
            position,
            schemas,
            missing,
            deferred,
            oversized: within.oversized,
            trace: CaptureTrace::default(),
        })
    };
    let result = tokio::select! {
        biased;
        // A stop: as in `capture_with`.
        _ = cx.cancel.cancelled() => return Err(Cancelled),
        r = body => r,
    };
    guard.id = 0;
    guard.permit = None;
    Ok(result.map(|mut c| {
        c.trace = trace;
        c
    }))
}

/// Bounded capture metrics (lock wait and schema query apart). Never fails.
pub(crate) fn observe_capture(tag: &ScanTag<'_>, cap: &Captured) {
    let labels = [
        ("source", tag.source_id.to_string()),
        ("kind", tag.kind.as_str().to_string()),
    ];
    metrics::histogram!(
        "deltaforge_mysql_proof_capture_lock_wait_seconds",
        &labels
    )
    .record(cap.trace.lock_wait.as_secs_f64());
    metrics::histogram!(
        "deltaforge_mysql_proof_capture_schema_seconds",
        &labels
    )
    .record(cap.trace.schema.as_secs_f64());
    if !cap.trace.retries.is_empty() {
        metrics::counter!(
            "deltaforge_mysql_proof_capture_retries_total",
            &labels
        )
        .increment(cap.trace.retries.len() as u64);
    }
}

/// The qualification record of one table proof: the table, its capture,
/// the scan that served it (joined by `scan_id`) and what that scan found
/// for this table. Trace target only; never fails.
#[allow(clippy::too_many_arguments)]
pub(crate) fn trace_proof(
    tag: &ScanTag<'_>,
    db: &str,
    table: &str,
    cap: Option<&Captured>,
    report: Option<&ScanReport>,
    lower_case_table_names: u8,
    outcome: &str,
) {
    if !trace_enabled() {
        return;
    }
    let (mut relevant, mut unrelated, mut barriers) = (0u64, 0u64, 0u64);
    for st in report.map(|r| r.statements.as_slice()).unwrap_or_default() {
        if matches!(st.effect, DdlEffect::Barrier(_)) {
            barriers += 1;
        }
        if super::mysql_baseline::affects(st, db, table, lower_case_table_names)
        {
            relevant += 1;
        } else {
            unrelated += 1;
        }
    }
    let record = serde_json::json!({
        "record": "proof",
        "source": tag.source_id,
        "kind": tag.kind.as_str(),
        "db": db,
        "table": table,
        "outcome": outcome,
        "scan_id": report.map(|r| r.trace.scan_id),
        "capture": cap.map(|c| serde_json::json!({
            "from": {"file": c.from.file, "pos": c.from.pos, "gtid_set": c.from.gtid_set},
            "to": {"file": c.position.file, "pos": c.position.pos, "gtid_set": c.position.gtid_set},
            "attempts": c.attempts,
            "connect_ms": c.trace.connect.as_secs_f64() * 1e3,
            "lock_wait_ms": c.trace.lock_wait.as_secs_f64() * 1e3,
            "schema_ms": c.trace.schema.as_secs_f64() * 1e3,
            "positions_ms": c.trace.positions.as_secs_f64() * 1e3,
            "retries": c.trace.retries,
        })),
        "relevant_ddl": relevant,
        "unrelated_ddl": unrelated,
        "barriers": barriers,
    });
    tracing::info!(target: "deltaforge::proof_trace", "{record}");
}

/// Whether a scan of `(start, end]` covers the capture interval
/// `(cap.from, cap.position]`: `start <= cap.from` and `cap.position <= end`.
/// A covering scan in which nothing affects the table proves the captured
/// shape for the whole interval.
pub(crate) fn covers(cap: &Captured, start: &WmPos, end: &WmPos) -> bool {
    let (Ok(a), Ok(b)) = (wm(&cap.from), wm(&cap.position)) else {
        return false;
    };
    matches!(
        order_positions(start, &a),
        CheckpointOrder::Before | CheckpointOrder::Equal
    ) && matches!(
        order_positions(&b, end),
        CheckpointOrder::Before | CheckpointOrder::Equal
    )
}

/// The source's shared proof scanner and the stream's boundary, for a
/// proof whose interval it should serve.
#[derive(Clone, Copy)]
pub(crate) struct SharedProofs<'a> {
    pub scanner: &'a super::mysql_proof_scanner::ProofScanner,
    pub boundary: Option<&'a MySqlCheckpoint>,
    /// The stream anchor at the interval's start, if one was observed.
    pub anchor: Option<&'a Anchor>,
}

/// A capture whose own interval is proven: [`capture`], then a scan of
/// `(from, position]` in which nothing affects `db.table` (a DDL naming it, a
/// database barrier of its database, a lineage barrier). Ordinary writes and
/// other tables' DDL never cause a retry; only a relevant statement inside the
/// interval does, up to `attempts` times. Anything the scan cannot prove
/// (retention, unknown events, limits, another server) fails closed.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn capture_proven(
    cx: &CaptureCtx,
    scan_server_id: u64,
    server_uuid: &str,
    lineage_hash: &str,
    db: &str,
    table: &str,
    lower_case_table_names: u8,
    attempts: u32,
    limits: &ScanLimits,
    between: Option<BetweenHook<'_>>,
    tag: &ScanTag<'_>,
    shared: Option<SharedProofs<'_>>,
) -> Result<Result<Captured, ProofError>, Cancelled> {
    /// How an attempt ends early: a stop, or a proof failure.
    enum Ended {
        Stopped,
        Failed(ProofError),
    }
    impl From<ProofError> for Ended {
        fn from(e: ProofError) -> Self {
            Ended::Failed(e)
        }
    }
    let dsn = cx.dsn();
    let proven = async {
        let mut retries = Vec::new();
        for attempt in 1..=attempts.max(1) {
            let mut cap =
                capture(cx, server_uuid, lineage_hash, db, table, between)
                    .await
                    .map_err(|Cancelled| Ended::Stopped)??;
            cap.attempts = attempt;
            let report = match &shared {
                Some(sp) => {
                    super::mysql_proof_scanner::shared_interval(
                        sp.scanner,
                        dsn.current().expose(),
                        tag.source_id,
                        server_uuid,
                        lineage_hash,
                        &cap.from,
                        &cap.position,
                        sp.anchor,
                        sp.boundary,
                        tag,
                    )
                    .await?
                }
                None => {
                    scan_interval(
                        dsn.current().expose(),
                        scan_server_id,
                        server_uuid,
                        lineage_hash,
                        &cap.from,
                        &cap.position,
                        limits,
                        tag,
                    )
                    .await?
                }
            };
            let relevant = report.statements.iter().any(|st| {
                super::mysql_baseline::affects(
                    st,
                    db,
                    table,
                    lower_case_table_names,
                )
            });
            cap.trace.retries = retries.clone();
            observe_capture(tag, &cap);
            if !relevant {
                trace_proof(
                    tag,
                    db,
                    table,
                    Some(&cap),
                    Some(&report),
                    lower_case_table_names,
                    "proven",
                );
                return Ok(cap);
            }
            retries.push("relevant statement in the capture interval".into());
            tokio::time::sleep(Duration::from_millis(20 * u64::from(attempt)))
                .await;
        }
        trace_proof(
            tag,
            db,
            table,
            None,
            None,
            lower_case_table_names,
            "unstable",
        );
        Err(Ended::Failed(ProofError::Unstable(attempts.max(1))))
    };
    match proven.await {
        Ok(cap) => Ok(Ok(cap)),
        Err(Ended::Failed(e)) => Ok(Err(e)),
        Err(Ended::Stopped) => Err(Cancelled),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::super::mysql_ddl_attribution::TableName;
    use super::*;

    /// The production text of each MySQL source file (before its test
    /// module): (file, text, text with whitespace removed).
    fn production_sources() -> Vec<(String, String, String)> {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/src/mysql");
        let mut out = Vec::new();
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_none_or(|e| e != "rs") {
                continue;
            }
            let text = std::fs::read_to_string(&path).unwrap();
            // The test module, however visible it is.
            let production = match [
                "\n#[cfg(test)]\nmod tests",
                "\n#[cfg(test)]\npub(crate) mod tests",
            ]
            .iter()
            .filter_map(|m| text.find(m))
            .min()
            {
                Some(i) => &text[..i],
                None => &text[..],
            };
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            out.push((
                name,
                production.to_string(),
                production.split_whitespace().collect(),
            ));
        }
        out
    }

    /// Every schema capture of a run shares the one context the run
    /// builds: the capture APIs take only a `CaptureCtx`, production code
    /// builds exactly one (from the run's DSN and its own stop token), and
    /// each capture path passes the run's context on (forward proofs,
    /// snapshot and lazy baselines, failover drift, FULL fallback and
    /// activation windows). A capture with another token would outlast a
    /// stop by its whole lock wait.
    #[test]
    fn every_capture_path_shares_the_runs_capture_context() {
        let sources = production_sources();
        let built: Vec<&str> = sources
            .iter()
            .flat_map(|(f, _, t)| {
                std::iter::repeat_n(
                    f.as_str(),
                    t.matches("CaptureCtx::for_run(").count(),
                )
            })
            .collect();
        assert_eq!(
            built,
            ["mod.rs"],
            "where the run's capture context is built"
        );
        let run = &sources.iter().find(|(f, _, _)| f == "mod.rs").unwrap().2;
        assert!(
            run.contains(
                "letcaptures=mysql_binlog_scan::CaptureCtx::for_run(run_dsn.clone(),cancel.clone(),self.control_pool(),);"
            ),
            "the run's context is built from the run's DSN and stop"
        );
        assert!(
            run.contains("asyncfnrun_inner(&self,tx:mpsc::Sender<SourceItem>,chkpt_store:Arc<dynCheckpointStore>,cancel:CancellationToken,"),
            "`cancel` is the run's stop token"
        );
        // Every production capture, by path: what it passes as its context.
        let calls = [
            ("mysql_forward_proof.rs", "capture(&ctx.capture,", 1),
            // The lazy baseline, and the per-table snapshot path (kept as the
            // batched one's test reference).
            ("mysql_baseline.rs", "capture(&ctx.capture,", 2),
            // Snapshot baselines, one batched capture per chunk.
            ("mysql_baseline.rs", "capture_batch(&ctx.capture,", 1),
            // The per-table drift check, and snapshot-time drift per chunk.
            ("mysql_failover_drift.rs", "capture(env.capture,", 1),
            ("mysql_failover_drift.rs", "capture_batch(env.capture,", 1),
            ("mysql_selection.rs", "capture_proven(&ctx.capture,", 1),
            (
                "mysql_activation_window.rs",
                "capture_batch(&cs.capture,",
                1,
            ),
        ];
        for (file, call, n) in calls {
            let text = &sources.iter().find(|(f, _, _)| f == file).unwrap().2;
            assert_eq!(text.matches(call).count(), n, "{file}: {call}");
        }
        // Capture calls in `t`: not a definition, a path or another name's
        // suffix (`observe_capture(`).
        let entry = |t: &str| {
            let names = [
                "capture(",
                "capture_with(",
                "capture_batch(",
                "capture_batch_with(",
                "capture_proven(",
            ];
            names
                .iter()
                .map(|c| {
                    t.match_indices(c)
                        .filter(|(i, _)| {
                            let before = &t[..*i];
                            let joined =
                                before.chars().last().is_some_and(|c| {
                                    c.is_alphanumeric() || c == '_'
                                });
                            !before.ends_with("fn ")
                                && !before.ends_with("::")
                                && !joined
                        })
                        .count()
                })
                .sum::<usize>()
        };
        let mut total = 0;
        for (file, raw, _) in &sources {
            let n = entry(raw);
            let known: usize = calls
                .iter()
                .filter(|(f, _, _)| f == file)
                .map(|(_, _, n)| n)
                .sum();
            let inner = if file == "mysql_binlog_scan.rs" { 3 } else { 0 };
            assert_eq!(
                n,
                known + inner,
                "{file}: a capture call outside the listed paths"
            );
            total += n;
        }
        assert_eq!(total, 11);
        // The window's captures (also speculative ones) and the snapshot's
        // and stream's drift checks carry the run's context.
        let window = &sources
            .iter()
            .find(|(f, _, _)| f == "mysql_activation_window.rs")
            .unwrap()
            .2;
        assert!(window.contains("capture:ctx.capture.clone(),"));
        assert_eq!(run.matches("capture:&captures,").count(), 1);
        assert_eq!(run.matches("capture:captures,").count(), 1);
        let drift = &sources
            .iter()
            .find(|(f, _, _)| f == "mysql_failover_drift.rs")
            .unwrap()
            .2;
        assert_eq!(drift.matches("capture:&ctx.capture,").count(), 2);
    }

    /// A cancellation that cannot reach the server is counted (and
    /// logged), never silent (finding 3).
    #[tokio::test]
    async fn a_failed_cancellation_is_counted() {
        let slots: &'static CaptureSlots =
            Box::leak(Box::new(CaptureSlots::new(4, 4)));
        let before = kill_failures();
        drop(KillOnDrop {
            dsn: "mysql://u:p@127.0.0.1:1/".into(),
            source: "test".into(),
            id: 42,
            slots,
            permit: None,
        });
        let deadline =
            std::time::Instant::now() + std::time::Duration::from_secs(20);
        while kill_failures() == before {
            assert!(
                std::time::Instant::now() < deadline,
                "the failed cancellation was not counted"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    const U: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
    const TAG: ScanTag<'static> = ScanTag {
        source_id: "test",
        kind: ProofKind::Lazy,
    };

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

    /// One request: every table's lock statement, in the given order; a
    /// request beyond its table or byte bound is refused, never split.
    #[test]
    fn the_lock_request_is_bounded() {
        let names: Vec<(String, String)> =
            (0..3).map(|i| ("d".to_string(), format!("t{i}"))).collect();
        let tables: Vec<(&str, &str)> = names
            .iter()
            .map(|(d, t)| (d.as_str(), t.as_str()))
            .collect();
        assert_eq!(
            lock_request(&tables).unwrap(),
            "SELECT 1 FROM `d`.`t0` LIMIT 0;SELECT 1 FROM `d`.`t1` LIMIT 0;\
             SELECT 1 FROM `d`.`t2` LIMIT 0;"
        );
        let many: Vec<(String, String)> = (0..=MAX_BATCH_TABLES)
            .map(|i| ("d".to_string(), format!("t{i}")))
            .collect();
        let many: Vec<(&str, &str)> =
            many.iter().map(|(d, t)| (d.as_str(), t.as_str())).collect();
        assert!(lock_request(&many[..MAX_BATCH_TABLES]).is_some());
        assert!(lock_request(&many).is_none(), "too many tables");
        // 64-character names of 4-byte characters, all quoted: too many
        // bytes well before the table bound.
        let wide = "\u{1F600}".repeat(64);
        let wide: Vec<(&str, &str)> =
            vec![(wide.as_str(), wide.as_str()); MAX_BATCH_TABLES];
        assert!(lock_request(&wide).is_none(), "too many bytes");
    }

    #[test]
    fn gtid_sets_are_exact_not_ranges() {
        let mut s = GtidSet::parse(&format!("{U}:1-5")).unwrap();
        add_gtid(&mut s, &format!("{U}:9")).unwrap();
        // 6-8 were never seen: the set must not claim them.
        assert!(!GtidSet::parse(&format!("{U}:1-9")).unwrap().is_subset(&s));
        for n in 6..=8 {
            add_gtid(&mut s, &format!("{U}:{n}")).unwrap();
        }
        assert!(GtidSet::parse(&format!("{U}:1-9")).unwrap().is_subset(&s));
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

    mod anchor_check {
        use super::*;
        use mysql_binlog_connector_rust::event::{
            checksum_type::ChecksumType,
            format_description_event::FormatDescriptionEvent,
            gtid_event::GtidEvent, previous_gtids_event::PreviousGtidsEvent,
            rotate_event::RotateEvent, xid_event::XidEvent,
        };

        const F1: &str = "binlog.000007";
        const F2: &str = "binlog.000008";

        fn anchor(file: &str, offset: u64, next_gtid: Option<u64>) -> Anchor {
            Anchor {
                server_uuid: U.into(),
                lineage: "lin".into(),
                file: file.into(),
                offset,
                set_before: GtidSet::parse(&format!("{U}:1-10")).unwrap(),
                next_gtid: next_gtid.map(|n| format!("{U}:{n}")),
            }
        }

        /// An event of `len` bytes starting at `start` (0, 0: artificial).
        fn at(event_type: u8, start: u32, len: u32) -> EventHeader {
            EventHeader {
                event_length: len,
                next_event_position: if start == 0 { 0 } else { start + len },
                ..header(event_type, 0)
            }
        }

        fn rotate(file: &str, pos: u64) -> EventData {
            EventData::Rotate(RotateEvent {
                binlog_filename: file.into(),
                binlog_position: pos,
            })
        }

        fn fde() -> EventData {
            EventData::FormatDescription(FormatDescriptionEvent {
                binlog_version: 4,
                server_version: "8.4.0".into(),
                create_timestamp: 0,
                header_length: 19,
                checksum_type: ChecksumType::None,
            })
        }

        fn gtid(n: u64) -> EventData {
            EventData::Gtid(GtidEvent {
                flags: 0,
                gtid: format!("{U}:{n}"),
            })
        }

        /// Feed `events`; the result of the last one, and the check.
        fn feed(
            a: Anchor,
            events: Vec<(EventHeader, EventData)>,
        ) -> (Result<(), ProofError>, AnchorCheck) {
            let mut c = AnchorCheck::new(a);
            let mut last = Ok(());
            for (h, d) in &events {
                last = c.check(h, d);
                if last.is_err() {
                    break;
                }
            }
            (last, c)
        }

        /// The server's opening: its artificial Rotate and Format_description.
        fn opening(file: &str, offset: u64) -> Vec<(EventHeader, EventData)> {
            vec![
                (at(4, 0, 40), rotate(file, offset)),
                (at(15, 0, 120), fde()),
            ]
        }

        fn mismatch(r: Result<(), ProofError>) -> String {
            match r {
                Err(ProofError::AnchorMismatch(why)) => why,
                other => panic!("expected AnchorMismatch, got {other:?}"),
            }
        }

        #[test]
        fn a_transaction_start_in_the_same_file_passes() {
            let mut ev = opening(F1, 5000);
            ev.push((at(27, 0, 20), EventData::HeartBeat));
            ev.push((at(33, 5000, 79), gtid(11)));
            let (r, c) = feed(anchor(F1, 5000, Some(11)), ev);
            r.unwrap();
            assert_eq!(c.passed(), Some((F1, 5000)));
            // Later events are not checked again.
            let mut c = c;
            c.check(&at(16, 5200, 31), &EventData::Xid(XidEvent { xid: 1 }))
                .unwrap();
        }

        /// A frontier at the end of a file: the file's Rotate starts at the
        /// anchor offset, then the next file's setup events and its first
        /// GTID, which becomes the effective coordinate.
        #[test]
        fn a_frontier_at_a_files_end_passes_the_rotation_chain() {
            // The file's own Rotate at the anchor; or a Stop there and the
            // server's artificial Rotate as it opens the next file.
            let chain = |stop: bool| {
                let mut ev = opening(F1, 9000);
                if stop {
                    ev.push((at(3, 9000, 23), EventData::NotSupported));
                } else {
                    ev.push((at(4, 9000, 44), rotate(F2, 4)));
                }
                ev.extend([
                    (at(4, 0, 44), rotate(F2, 4)),
                    (at(15, 4, 122), fde()),
                    (
                        at(35, 126, 71),
                        EventData::PreviousGtids(PreviousGtidsEvent {
                            gtid_set: format!("{U}:1-10"),
                        }),
                    ),
                    (at(27, 0, 20), EventData::HeartBeat),
                    (at(33, 197, 79), gtid(11)),
                ]);
                ev
            };
            for stop in [false, true] {
                let (r, c) = feed(anchor(F1, 9000, None), chain(stop));
                r.unwrap();
                assert_eq!(c.passed(), Some((F2, 197)), "stop: {stop}");
            }
        }

        #[test]
        fn a_rotation_names_only_the_next_file() {
            // After a Stop, only the server's opening of the next file.
            let mut ev = opening(F1, 9000);
            ev.extend([
                (at(3, 9000, 23), EventData::NotSupported),
                (at(4, 9023, 44), rotate(F2, 4)),
            ]);
            mismatch(feed(anchor(F1, 9000, None), ev).0);
            let mut ev = opening(F1, 9000);
            ev.extend([
                (at(3, 9000, 23), EventData::NotSupported),
                (at(4, 0, 44), rotate("binlog.000009", 4)),
            ]);
            assert!(
                mismatch(feed(anchor(F1, 9000, None), ev).0)
                    .contains("out of sequence")
            );
            // After a rotation, the server opens exactly that file.
            for (file, pos) in [(F1, 4), (F2, 126), ("binlog.000009", 4)] {
                let mut ev = opening(F1, 9000);
                ev.extend([
                    (at(4, 9000, 44), rotate(F2, 4)),
                    (at(4, 0, 44), rotate(file, pos)),
                ]);
                assert!(
                    mismatch(feed(anchor(F1, 9000, None), ev).0)
                        .contains("opened"),
                    "{file}:{pos}"
                );
            }
        }

        #[test]
        fn files_without_transactions_chain_in_sequence() {
            let mut ev = opening(F1, 9000);
            ev.extend([
                (at(4, 9000, 44), rotate(F2, 4)),
                (at(4, 0, 44), rotate(F2, 4)),
                (at(15, 4, 122), fde()),
                (at(4, 126, 44), rotate("binlog.000009", 4)),
                (at(4, 0, 44), rotate("binlog.000009", 4)),
                (at(15, 4, 122), fde()),
                (at(33, 126, 79), gtid(11)),
            ]);
            let (r, c) = feed(anchor(F1, 9000, None), ev);
            r.unwrap();
            assert_eq!(c.passed(), Some(("binlog.000009", 126)));
        }

        #[test]
        fn an_opening_elsewhere_is_refused() {
            for (file, pos) in [(F1, 5001), (F2, 5000)] {
                let (r, _) = feed(anchor(F1, 5000, None), opening(file, pos));
                assert!(mismatch(r).contains("opened at"));
            }
            // A real event before the server's artificial Rotate.
            let (r, _) = feed(
                anchor(F1, 5000, None),
                vec![(at(33, 5000, 79), gtid(11))],
            );
            mismatch(r);
        }

        /// Probe 3: an offset inside a transaction streams its `BEGIN` (or
        /// its rows) without a GTID. Nothing transactional may come first.
        #[test]
        fn a_transactional_event_before_the_first_gtid_is_refused() {
            use mysql_binlog_connector_rust::event::{
                table_map_event::TableMapEvent,
                transaction_payload_event::TransactionPayloadEvent,
                write_rows_event::WriteRowsEvent,
            };
            let firsts: Vec<(u8, EventData)> = vec![
                (2, query("BEGIN")),
                (2, query("ALTER TABLE t ADD c INT")),
                (
                    19,
                    EventData::TableMap(TableMapEvent {
                        table_id: 1,
                        database_name: "d".into(),
                        table_name: "t".into(),
                        column_types: vec![],
                        column_metas: vec![],
                        null_bits: vec![],
                        table_metadata: None,
                    }),
                ),
                (
                    30,
                    EventData::WriteRows(WriteRowsEvent {
                        table_id: 1,
                        included_columns: vec![],
                        rows: vec![],
                    }),
                ),
                (16, EventData::Xid(XidEvent { xid: 1 })),
                (
                    40,
                    EventData::TransactionPayload(TransactionPayloadEvent {
                        uncompressed_size: 0,
                        uncompressed_events: vec![],
                    }),
                ),
                (ANONYMOUS_GTID, EventData::NotSupported),
            ];
            for (t, first) in firsts {
                // At the anchor offset, and after a valid rotation.
                let mut same = opening(F1, 5000);
                same.push((at(t, 5000, 60), first.clone()));
                let mut rotated = opening(F1, 9000);
                rotated.extend([
                    (at(4, 9000, 44), rotate(F2, 4)),
                    (at(15, 4, 122), fde()),
                    (at(t, 126, 60), first),
                ]);
                for (a, ev) in [
                    (anchor(F1, 5000, None), same),
                    (anchor(F1, 9000, None), rotated),
                ] {
                    let (r, c) = feed(a, ev);
                    assert!(
                        mismatch(r).contains("before the first GTID"),
                        "event type {t}"
                    );
                    assert_eq!(c.passed(), None);
                }
            }
        }

        #[test]
        fn a_first_event_elsewhere_than_the_anchor_is_refused() {
            let mut ev = opening(F1, 5000);
            ev.push((at(33, 5079, 79), gtid(11)));
            assert!(
                mismatch(feed(anchor(F1, 5000, None), ev).0)
                    .contains("starts at")
            );
            let mut ev = opening(F1, 5000);
            ev.push((at(4, 5100, 44), rotate(F2, 4)));
            assert!(
                mismatch(feed(anchor(F1, 5000, None), ev).0)
                    .contains("Rotate at")
            );
            let mut ev = opening(F1, 5000);
            ev.push((at(3, 5100, 23), EventData::NotSupported));
            assert!(
                mismatch(feed(anchor(F1, 5000, None), ev).0)
                    .contains("Stop at")
            );
        }

        #[test]
        fn a_rotation_out_of_sequence_is_refused() {
            for (file, pos) in [
                ("binlog.000009", 4),
                ("binlog.000006", 4),
                (F1, 4),
                (F2, 120),
                ("other.000008", 4),
            ] {
                let mut ev = opening(F1, 9000);
                ev.push((at(4, 9000, 44), rotate(file, pos)));
                assert!(
                    mismatch(feed(anchor(F1, 9000, None), ev).0)
                        .contains("out of sequence"),
                    "{file}:{pos}"
                );
            }
        }

        #[test]
        fn setup_events_belong_only_to_a_new_file() {
            let mut ev = opening(F1, 5000);
            ev.push((
                at(35, 5000, 71),
                EventData::PreviousGtids(PreviousGtidsEvent {
                    gtid_set: String::new(),
                }),
            ));
            mismatch(feed(anchor(F1, 5000, None), ev).0);
        }

        #[test]
        fn the_first_gtid_must_be_new_and_the_expected_one() {
            let first = |n: u64, next: Option<u64>| {
                let mut ev = opening(F1, 5000);
                ev.push((at(33, 5000, 79), gtid(n)));
                feed(anchor(F1, 5000, next), ev).0
            };
            assert!(mismatch(first(10, None)).contains("already in"));
            assert!(mismatch(first(12, Some(11))).contains("not"));
            first(12, None).unwrap();
            first(11, Some(11)).unwrap();
        }

        #[test]
        fn an_incident_is_refused_as_an_incident() {
            let mut ev = opening(F1, 5000);
            ev.push((at(INCIDENT_EVENT, 5000, 30), EventData::NotSupported));
            assert!(matches!(
                feed(anchor(F1, 5000, None), ev).0,
                Err(ProofError::Incident)
            ));
        }

        #[test]
        fn the_live_end_is_at_or_past_the_end_position() {
            let to = cp(F2, 500, None);
            assert!(at_or_past(F2, 500, &to));
            assert!(at_or_past(F2, 501, &to));
            assert!(at_or_past("binlog.000009", 4, &to));
            assert!(!at_or_past(F2, 499, &to));
            assert!(!at_or_past(F1, 9000, &to));
            assert!(!at_or_past("other.000009", 4, &to));
        }
    }

    fn walk(gtid_mode: bool) -> Walk {
        Walk {
            gtid_mode,
            to_set: gtid_mode
                .then(|| GtidSet::parse(&format!("{U}:1-9")).unwrap()),
            done: GtidSet::default(),
            current_gtid: None,
            ordinal: 0,
            in_explicit_txn: false,
            server_version: None,
            file: "binlog.000001".into(),
            statements: Vec::new(),
            reached: false,
            last_completed: None,
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
                end_pos: 4321,
                ordinal: 0,
            }
        );
    }

    /// The scan classifies versioned executable comments with the version
    /// of the server that wrote the binlog (its format description), as the
    /// streaming source does.
    #[test]
    fn versioned_comments_use_the_binlog_writers_version() {
        use super::super::mysql_ddl_attribution::{
            BarrierScopeOf, DdlEffect, TableName,
        };
        use mysql_binlog_connector_rust::event::{
            checksum_type::ChecksumType,
            format_description_event::FormatDescriptionEvent,
        };
        let fde = |version: &str| {
            EventData::FormatDescription(FormatDescriptionEvent {
                binlog_version: 4,
                server_version: version.into(),
                create_timestamp: 0,
                header_length: 19,
                checksum_type: ChecksumType::None,
            })
        };
        let sql = "/*!80036 ALTER TABLE d.t ADD COLUMN c INT */";
        let effect_under = |version: Option<&str>| {
            let mut w = walk(false);
            if let Some(v) = version {
                w.event(&header(15, 120), &fde(v)).unwrap();
            }
            w.event(&header(2, 200), &query(sql)).unwrap();
            w.statements.first().map(|s| s.effect.clone())
        };
        let t = DdlEffect::Tables(vec![TableName {
            db: "d".into(),
            table: "t".into(),
        }]);
        assert_eq!(effect_under(Some("8.0.36-log")), Some(t.clone()));
        assert_eq!(effect_under(Some("8.4.2")), Some(t));
        // Not executed by an older server: no table change.
        assert_eq!(effect_under(Some("8.0.35")), None);
        // Unknown writer version: a lineage barrier.
        assert_eq!(
            effect_under(None),
            Some(DdlEffect::Barrier(BarrierScopeOf::Lineage))
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

    fn protocol_events() -> Vec<EventData> {
        use mysql_binlog_connector_rust::event::{
            checksum_type::ChecksumType,
            format_description_event::FormatDescriptionEvent,
            previous_gtids_event::PreviousGtidsEvent,
            rotate_event::RotateEvent,
        };
        vec![
            EventData::Rotate(RotateEvent {
                binlog_filename: "binlog.000001".into(),
                binlog_position: 4,
            }),
            EventData::FormatDescription(FormatDescriptionEvent {
                binlog_version: 4,
                server_version: "8.4".into(),
                create_timestamp: 0,
                header_length: 19,
                checksum_type: ChecksumType::None,
            }),
            EventData::PreviousGtids(PreviousGtidsEvent {
                gtid_set: format!("{U}:1-9"),
            }),
        ]
    }

    fn gtid_event(gtid: &str) -> EventData {
        EventData::Gtid(
            mysql_binlog_connector_rust::event::gtid_event::GtidEvent {
                flags: 0,
                gtid: gtid.into(),
            },
        )
    }

    /// Rotate, format description and previous-GTIDs events at the start
    /// of a dump are protocol, never "decoded before the start".
    #[test]
    fn protocol_events_are_never_decoded_before_the_start() {
        let start_set = GtidSet::parse(&format!("{U}:1-9")).unwrap();
        for gtid_mode in [true, false] {
            let mut d = ScanDetail::default();
            let start = if gtid_mode {
                g(9)
            } else {
                mysql_checkpoint_position("binlog.000001", 500, None).unwrap()
            };
            for e in protocol_events() {
                observe_event(
                    &mut d,
                    &header(0, 120),
                    &e,
                    &walk(gtid_mode),
                    gtid_mode.then_some(&start_set),
                    &start,
                    U,
                );
            }
            assert_eq!(
                (d.protocol_events, d.before_start),
                (3, 0),
                "{gtid_mode}"
            );
        }
    }

    /// GTID mode: a transaction already in the start set was decoded before
    /// the start; transactions are counted per server UUID.
    #[test]
    fn gtid_transactions_before_the_start_and_per_uuid() {
        const OTHER: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
        let start_set = GtidSet::parse(&format!("{U}:1-9")).unwrap();
        let mut d = ScanDetail::default();
        for gtid in [format!("{U}:9"), format!("{U}:10"), format!("{OTHER}:1")]
        {
            observe_event(
                &mut d,
                &header(33, 0),
                &gtid_event(&gtid),
                &walk(true),
                Some(&start_set),
                &g(9),
                U,
            );
        }
        assert_eq!((d.before_start, d.source_txns, d.other_txns), (1, 2, 1));
    }

    /// File mode: a transaction event ending at or before the start was
    /// decoded before it; one after it was not.
    #[test]
    fn file_events_at_or_before_the_start() {
        let start =
            mysql_checkpoint_position("binlog.000001", 500, None).unwrap();
        let mut d = ScanDetail::default();
        for end in [400u32, 500, 600] {
            observe_event(
                &mut d,
                &header(2, end),
                &query("CREATE TABLE d.t (a INT)"),
                &walk(false),
                None,
                &start,
                U,
            );
        }
        assert_eq!((d.before_start, d.query_events), (2, 3));
    }

    /// The interval's transactions are the source server's only: other
    /// UUIDs carried in the executed set are not counted.
    #[test]
    fn source_transactions_ignore_other_uuids() {
        let set = GtidSet::parse(&format!(
            "{U}:1-9:12,AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE:1-100"
        ))
        .unwrap();
        assert_eq!(source_txns(&set, U), 10);
        assert_eq!(source_txns(&set, &U.to_uppercase()), 10);
    }

    /// The bounded metric labels: five proof kinds.
    #[test]
    fn proof_kinds_are_a_bounded_label_set() {
        let all = [
            ProofKind::Lazy,
            ProofKind::FullFallback,
            ProofKind::Snapshot,
            ProofKind::Forward,
            ProofKind::FailoverDrift,
        ]
        .map(ProofKind::as_str);
        assert_eq!(
            all,
            [
                "lazy",
                "full_fallback",
                "snapshot",
                "forward",
                "failover_drift"
            ]
        );
    }

    fn cp(file: &str, pos: u64, gtid: Option<&str>) -> MySqlCheckpoint {
        MySqlCheckpoint {
            file: file.into(),
            pos,
            gtid_set: gtid.map(Into::into),
            lineage: Some("l".into()),
            snapshot_completed: None,
            snapshot_chain: None,
        }
    }

    /// GTID mode, decided before any session: the same set at a later file
    /// offset is the canonical empty interval; sets that do not include
    /// each other, or a GTID against a file position, are refused.
    #[tokio::test]
    async fn gtid_sets_are_authoritative_before_any_session() {
        let nowhere = "mysql://u:p@127.0.0.1:1/";
        let set = format!("{U}:1-9");
        let r = scan_interval(
            nowhere,
            1,
            U,
            "l",
            &cp("binlog.000001", 100, Some(&set)),
            &cp("binlog.000002", 900, Some(&set)),
            &ScanLimits::default(),
            &TAG,
        )
        .await
        .unwrap();
        assert!(r.statements.is_empty());
        assert_eq!(r.digest, scan_digest(&g(9), &g(9), &[]));
        for to in [
            cp(
                "binlog.000001",
                100,
                Some("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-3"),
            ),
            cp("binlog.000001", 200, None),
        ] {
            let r = scan_interval(
                nowhere,
                1,
                U,
                "l",
                &cp("binlog.000001", 100, Some(&set)),
                &to,
                &ScanLimits::default(),
                &TAG,
            )
            .await;
            assert!(matches!(r, Err(ProofError::Positions(_))), "{r:?}");
        }
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
    pub(crate) mod live {
        use super::*;
        use crate::mysql::mysql_schema_loader::read_hook;
        use gate_ownership::GateOwned;
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
                .gate_owned()
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

        pub(crate) async fn server(gtid: bool) -> String {
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

        pub(crate) async fn sql(dsn: &str, stmts: &[&str]) {
            let mut c = mysql_async::Conn::from_url(dsn).await.unwrap();
            for s in stmts {
                c.query_drop(*s)
                    .await
                    .unwrap_or_else(|e| panic!("{s}: {e}"));
            }
        }

        pub(crate) async fn uuid(dsn: &str) -> String {
            let mut c = mysql_async::Conn::from_url(dsn).await.unwrap();
            c.query_first("SELECT @@GLOBAL.server_uuid")
                .await
                .unwrap()
                .unwrap()
        }

        pub(crate) async fn position(dsn: &str) -> MySqlCheckpoint {
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
                &TAG,
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

        /// Collects the qualification trace records, while installed as
        /// this thread's default subscriber.
        #[derive(Clone, Default)]
        struct Records(Arc<std::sync::Mutex<Vec<serde_json::Value>>>);

        impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Records {
            fn on_event(
                &self,
                event: &tracing::Event<'_>,
                _ctx: tracing_subscriber::layer::Context<'_, S>,
            ) {
                if event.metadata().target() != TRACE_TARGET {
                    return;
                }
                struct Msg(String);
                impl tracing::field::Visit for Msg {
                    fn record_debug(
                        &mut self,
                        f: &tracing::field::Field,
                        v: &dyn std::fmt::Debug,
                    ) {
                        if f.name() == "message" {
                            self.0 = format!("{v:?}");
                        }
                    }
                }
                let mut m = Msg(String::new());
                event.record(&mut m);
                if let Ok(v) = serde_json::from_str(&m.0) {
                    self.0.lock().unwrap().push(v);
                }
            }
        }

        impl Records {
            /// Trace enabled on this thread until the guard drops.
            fn install(&self) -> tracing::subscriber::DefaultGuard {
                use tracing_subscriber::layer::SubscriberExt;
                tracing::subscriber::set_default(
                    tracing_subscriber::registry().with(self.clone()),
                )
            }

            fn of(&self, record: &str) -> Vec<serde_json::Value> {
                self.0
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|v| v["record"] == record)
                    .cloned()
                    .collect()
            }
        }

        /// A scan's trace: where it started and ended, its timings and its
        /// event classification. Transactions are counted per server UUID
        /// (GTID mode: one of another UUID in the interval); nothing is
        /// decoded before the start. With the trace off the outcome and
        /// digest are identical and no detail is collected.
        async fn traced(gtid: bool, db: &str) {
            let dsn = server(gtid).await;
            sql(&dsn, &[&format!("CREATE DATABASE {db}")]).await;
            let from = position(&dsn).await;
            sql(
                &dsn,
                &[
                    &format!("CREATE TABLE {db}.t (a INT PRIMARY KEY)"),
                    &format!("INSERT INTO {db}.t VALUES (1)"),
                ],
            )
            .await;
            if gtid {
                sql(
                    &dsn,
                    &[
                        "SET gtid_next = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1'",
                        &format!("INSERT INTO {db}.t VALUES (2)"),
                        "SET gtid_next = 'AUTOMATIC'",
                    ],
                )
                .await;
            }
            let to = position(&dsn).await;

            let records = Records::default();
            let guard = records.install();
            let r = scan(&dsn, &from, &to).await.unwrap();
            drop(guard);
            let t = &r.trace;
            let d = t.detail.as_ref().expect("detail while the trace is on");
            assert_eq!(d.before_start, 0, "{d:?}");
            assert!(d.protocol_events > 0, "{d:?}");
            assert_eq!(d.row_events, if gtid { 2 } else { 1 }, "{d:?}");
            if gtid {
                assert_eq!(d.interval_source_txns, Some(2), "{d:?}");
                assert_eq!((d.source_txns, d.other_txns), (2, 1), "{d:?}");
            }
            let first = t.first.as_ref().expect("a first transaction event");
            let terminal = t.terminal.as_ref().expect("a terminal event");
            if gtid {
                assert!(first.gtid.is_some() && terminal.gtid.is_some());
            } else {
                assert_eq!(
                    (&terminal.file, terminal.end_pos),
                    (&to.file, to.pos)
                );
                assert!(first.end_pos > from.pos || first.file != from.file);
            }
            let scans = records.of("scan");
            assert_eq!(scans.len(), 1, "{scans:?}");
            assert_eq!(scans[0]["scan_id"], t.scan_id);
            assert_eq!(scans[0]["outcome"], "ok");
            assert_eq!(scans[0]["kind"], "lazy");

            // The trace off: the same outcome, nothing collected.
            let plain = scan(&dsn, &from, &to).await.unwrap();
            assert_eq!(plain.digest, r.digest);
            assert!(plain.trace.detail.is_none());
            assert!(plain.trace.terminal.is_some());

            // An interval holding only a DDL (no rows): the terminal is
            // still the proven end.
            let a = position(&dsn).await;
            sql(&dsn, &[&format!("CREATE TABLE {db}.u (a INT PRIMARY KEY)")])
                .await;
            let b = position(&dsn).await;
            let ddl = scan(&dsn, &a, &b).await.unwrap();
            assert!(ddl.trace.terminal.is_some());
            assert!(ddl.trace.first.is_some());

            // A failing scan: the trace changes nothing about the error.
            let guard = records.install();
            let err = scan(&dsn, &to, &from).await.unwrap_err();
            drop(guard);
            assert!(matches!(err, ProofError::Positions(_)), "{err:?}");
            assert_eq!(records.of("scan").last().unwrap()["outcome"], "failed");
        }

        #[tokio::test]
        #[ignore = "requires docker"]
        async fn gtid_scan_traces() {
            traced(true, "trace_g").await;
        }

        #[tokio::test]
        #[ignore = "requires docker"]
        async fn file_position_scan_traces() {
            traced(false, "trace_f").await;
        }

        /// Positioned GTID scans (`docs/design/mysql-proof-seek.md`,
        /// "Positioned GTID scanning"): a scan opened at an anchor answers
        /// exactly as a GTID-positioned one, across rotations, and a wrong
        /// anchor is refused promptly, never a proof that omits a DDL.
        mod positioned {
            use super::*;

            async fn at(
                dsn: &str,
                from: &MySqlCheckpoint,
                to: &MySqlCheckpoint,
                anchor: Option<&Anchor>,
            ) -> Result<ScanReport, ProofError> {
                scan_interval_at(
                    dsn,
                    4_000_124,
                    &uuid(dsn).await,
                    LINEAGE,
                    from,
                    to,
                    anchor,
                    &ScanLimits {
                        max_duration: Duration::from_secs(20),
                        ..ScanLimits::default()
                    },
                    &TAG,
                )
                .await
            }

            fn set(p: &MySqlCheckpoint) -> GtidSet {
                GtidSet::parse(p.gtid_set.as_deref().unwrap()).unwrap()
            }

            /// Positioned and GTID-positioned answers are statement- and
            /// digest-identical: at the session anchor a scan yields, and at
            /// the record frontier where it ended.
            #[tokio::test]
            #[ignore = "requires docker"]
            async fn positioned_scans_equal_gtid_positioned_scans() {
                let dsn = server(true).await;
                let db = "pos_same";
                sql(&dsn, &[&format!("CREATE DATABASE {db}")]).await;
                let a = position(&dsn).await;
                sql(
                    &dsn,
                    &[
                        &format!("CREATE TABLE {db}.t (id INT PRIMARY KEY)"),
                        &format!("INSERT INTO {db}.t VALUES (1)"),
                        &format!("ALTER TABLE {db}.t ADD COLUMN v INT"),
                    ],
                )
                .await;
                let b = position(&dsn).await;
                sql(
                    &dsn,
                    &[
                        &format!("INSERT INTO {db}.t VALUES (2, 2)"),
                        &format!("RENAME TABLE {db}.t TO {db}.u"),
                    ],
                )
                .await;
                let c = position(&dsn).await;

                let plain = at(&dsn, &a, &b, None).await.unwrap();
                assert_eq!(plain.positioned_at, None);
                let session =
                    plain.session_anchor.clone().expect("a session anchor");
                assert_eq!(session.set_before, set(&a));
                let frontier = plain.frontier.clone().expect("a frontier");
                assert_eq!(frontier.set_before, set(&b));
                assert_eq!(frontier.next_gtid, None);

                let again = at(&dsn, &a, &b, Some(&session)).await.unwrap();
                assert_eq!(again.digest, plain.digest);
                assert_eq!(again.statements, plain.statements);
                assert_eq!(
                    again.positioned_at,
                    Some((session.file.clone(), session.offset))
                );
                assert_eq!(again.frontier, plain.frontier);

                let next = at(&dsn, &b, &c, Some(&frontier)).await.unwrap();
                let direct = at(&dsn, &b, &c, None).await.unwrap();
                assert_eq!(next.digest, direct.digest);
                assert_eq!(affected(&next), [format!("{db}.t+{db}.u")]);
                assert_eq!(
                    next.positioned_at,
                    Some((frontier.file.clone(), frontier.offset))
                );
            }

            /// `hi` ends at a file's last transaction: the positioned scan
            /// at its frontier passes the file's Rotate, the next file's
            /// setup events (and an empty file's), and continues at the
            /// next transaction's file and offset.
            #[tokio::test]
            #[ignore = "requires docker"]
            async fn a_frontier_at_a_files_end_crosses_the_rotation() {
                let dsn = server(true).await;
                for (db, flushes) in [("pos_rot1", 1), ("pos_rot2", 2)] {
                    sql(&dsn, &[&format!("CREATE DATABASE {db}")]).await;
                    let a = position(&dsn).await;
                    sql(
                        &dsn,
                        &[&format!("CREATE TABLE {db}.t (id INT PRIMARY KEY)")],
                    )
                    .await;
                    let b = position(&dsn).await;
                    for _ in 0..flushes {
                        sql(&dsn, &["FLUSH BINARY LOGS"]).await;
                    }
                    sql(
                        &dsn,
                        &[&format!("ALTER TABLE {db}.t ADD COLUMN v INT")],
                    )
                    .await;
                    let c = position(&dsn).await;
                    assert_ne!(b.file, c.file);

                    let head = at(&dsn, &a, &b, None).await.unwrap();
                    let frontier = head.frontier.expect("a frontier");
                    assert_eq!(
                        (&frontier.file, frontier.offset),
                        (&b.file, b.pos)
                    );
                    let next = at(&dsn, &b, &c, Some(&frontier)).await.unwrap();
                    let direct = at(&dsn, &b, &c, None).await.unwrap();
                    assert_eq!(next.digest, direct.digest, "{flushes} flushes");
                    assert_eq!(affected(&next), [format!("{db}.t")]);
                    let (file, offset) =
                        next.positioned_at.expect("positioned");
                    assert_eq!(file, c.file, "{flushes} flushes");
                    assert!(offset > 4);
                }
            }

            /// A frontier at the last transaction of a file the server
            /// stopped writing: the chain is the file's Stop, then the
            /// server's opening of the next file (a stopped file has no
            /// Rotate of its own).
            #[tokio::test]
            #[ignore = "requires docker"]
            async fn a_frontier_before_a_stop_crosses_the_restart() {
                let (c, port) = start(true).await;
                let dsn = dsn_of(port);
                let db = "pos_stop";
                sql(&dsn, &[&format!("CREATE DATABASE {db}")]).await;
                let a = position(&dsn).await;
                sql(
                    &dsn,
                    &[&format!("CREATE TABLE {db}.t (id INT PRIMARY KEY)")],
                )
                .await;
                let b = position(&dsn).await;
                let frontier =
                    at(&dsn, &a, &b, None).await.unwrap().frontier.unwrap();

                c.stop_with_timeout(Some(60)).await.unwrap();
                c.start().await.unwrap();
                let port = c.get_host_port_ipv4(3306).await.unwrap();
                let dsn = dsn_of(port);
                for _ in 0..120 {
                    if mysql_async::Conn::from_url(&dsn).await.is_ok() {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                sql(&dsn, &[&format!("ALTER TABLE {db}.t ADD COLUMN v INT")])
                    .await;
                let c2 = position(&dsn).await;
                assert_ne!(b.file, c2.file);

                let next = at(&dsn, &b, &c2, Some(&frontier)).await.unwrap();
                let direct = at(&dsn, &b, &c2, None).await.unwrap();
                assert_eq!(next.digest, direct.digest);
                assert_eq!(affected(&next), [format!("{db}.t")]);
                assert_eq!(next.positioned_at.unwrap().0, c2.file);
            }

            /// The session of a capture waiting for its table's metadata
            /// lock, if any.
            async fn waiting_capture(
                dsn: &str,
                db: &str,
                table: &str,
            ) -> Vec<u64> {
                let mut c = mysql_async::Conn::from_url(dsn).await.unwrap();
                c.exec(
                    "SELECT ID FROM information_schema.PROCESSLIST WHERE INFO = ?",
                    (format!("SELECT 1 FROM `{db}`.`{table}` LIMIT 0"),),
                )
                .await
                .unwrap()
            }

            /// A single-table capture dropped while it waits for its table's
            /// metadata lock is killed on the server: its session does not
            /// stay queued for the lock (finding 3, the unguarded capture).
            #[tokio::test]
            #[ignore = "requires docker"]
            async fn a_dropped_single_capture_is_killed_while_it_waits_for_its_lock()
             {
                let dsn = server(true).await;
                let db = "cancel_single";
                sql(
                    &dsn,
                    &[
                        &format!("CREATE DATABASE {db}"),
                        &format!("CREATE TABLE {db}.t (id INT PRIMARY KEY)"),
                    ],
                )
                .await;
                // Another session holds the table: the capture queues.
                let mut holder =
                    mysql_async::Conn::from_url(&dsn).await.unwrap();
                holder
                    .query_drop(format!("LOCK TABLES {db}.t WRITE"))
                    .await
                    .unwrap();
                let slots: &'static CaptureSlots =
                    Box::leak(Box::new(CaptureSlots::new(4, 4)));
                let (d, u) = (dsn.clone(), uuid(&dsn).await);
                let task = tokio::spawn(async move {
                    capture_with(
                        slots,
                        &CaptureCtx::detached(d.as_str()),
                        &u,
                        LINEAGE,
                        db,
                        "t",
                        None,
                    )
                    .await
                    .expect("not cancelled")
                });
                let deadline = Instant::now() + Duration::from_secs(20);
                while waiting_capture(&dsn, db, "t").await.is_empty() {
                    assert!(
                        Instant::now() < deadline,
                        "the capture never queued"
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                task.abort();
                let deadline = Instant::now() + Duration::from_secs(5);
                while !waiting_capture(&dsn, db, "t").await.is_empty() {
                    assert!(
                        Instant::now() < deadline,
                        "the dropped capture's session still waits for the lock"
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                holder.query_drop("UNLOCK TABLES").await.unwrap();
            }

            /// A capture under `cx` on `db.t` (single, or the batched form
            /// with a 300 s lock wait); `hold` pauses it, once locked, until
            /// it is dropped. A failed capture is its error's text.
            pub(super) fn cancellable(
                slots: &'static CaptureSlots,
                cx: CaptureCtx,
                u: &str,
                db: &'static str,
                batched: bool,
                hold: Option<Arc<AtomicU32>>,
            ) -> tokio::task::JoinHandle<Result<Result<(), String>, Cancelled>>
            {
                let u = u.to_string();
                tokio::spawn(async move {
                    let hook = move || -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
                        let hold = hold.clone();
                        Box::pin(async move {
                            if let Some(inside) = hold {
                                inside.fetch_add(1, Ordering::SeqCst);
                                std::future::pending::<()>().await;
                            }
                        })
                    };
                    if batched {
                        let tables = vec![(db.to_string(), "t".to_string())];
                        capture_batch_with(
                            slots,
                            &cx,
                            &u,
                            LINEAGE,
                            &tables,
                            300,
                            Some(&hook),
                        )
                        .await
                        .map(|r| r.map(|_| ()).map_err(|e| format!("{e:?}")))
                    } else {
                        capture_with(
                            slots,
                            &cx,
                            &u,
                            LINEAGE,
                            db,
                            "t",
                            Some(&hook),
                        )
                        .await
                        .map(|r| r.map(|_| ()).map_err(|e| format!("{e:?}")))
                    }
                })
            }

            /// Whether `db.t`'s metadata lock is held by anyone.
            async fn locked(dsn: &str, db: &str) -> bool {
                let mut c = mysql_async::Conn::from_url(dsn).await.unwrap();
                let held: Vec<String> = c
                    .exec(
                        "SELECT LOCK_STATUS FROM performance_schema.metadata_locks \
                         WHERE OBJECT_SCHEMA = ? AND OBJECT_NAME = 't' AND LOCK_STATUS = 'GRANTED'",
                        (db,),
                    )
                    .await
                    .unwrap();
                !held.is_empty()
            }

            /// A capture cancelled while it waits for its table's metadata
            /// lock (single, with its 30 s wait, and batched, with 300 s)
            /// returns `Cancelled` at once; its session is killed and its
            /// slot returns, with no failed kill.
            #[tokio::test]
            #[ignore = "requires docker"]
            async fn a_cancelled_capture_ends_its_lock_wait_at_once() {
                let dsn = server(true).await;
                for (db, batched) in
                    [("stop_wait_one", false), ("stop_wait_batch", true)]
                {
                    sql(
                        &dsn,
                        &[
                            &format!("CREATE DATABASE {db}"),
                            &format!(
                                "CREATE TABLE {db}.t (id INT PRIMARY KEY)"
                            ),
                        ],
                    )
                    .await;
                    let mut holder =
                        mysql_async::Conn::from_url(&dsn).await.unwrap();
                    holder
                        .query_drop(format!("LOCK TABLES {db}.t WRITE"))
                        .await
                        .unwrap();
                    let slots: &'static CaptureSlots =
                        Box::leak(Box::new(CaptureSlots::new(4, 4)));
                    let cx = CaptureCtx::detached(dsn.as_str());
                    let failures = kill_failures();
                    let task = cancellable(
                        slots,
                        cx.clone(),
                        &uuid(&dsn).await,
                        db,
                        batched,
                        None,
                    );
                    let deadline = Instant::now() + Duration::from_secs(20);
                    while waiting_capture(&dsn, db, "t").await.is_empty() {
                        assert!(
                            Instant::now() < deadline,
                            "{db}: the capture never queued"
                        );
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    let t = Instant::now();
                    cx.stop();
                    let r = tokio::time::timeout(Duration::from_secs(2), task)
                        .await
                        .expect("the cancelled capture returns at once")
                        .unwrap();
                    assert!(matches!(r, Err(Cancelled)), "{db}: {r:?}");
                    assert!(
                        t.elapsed() < Duration::from_secs(1),
                        "{db}: {:?}",
                        t.elapsed()
                    );
                    let deadline = Instant::now() + Duration::from_secs(3);
                    while !waiting_capture(&dsn, db, "t").await.is_empty() {
                        assert!(
                            Instant::now() < deadline,
                            "{db}: the session still waits"
                        );
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    let deadline = Instant::now() + Duration::from_secs(3);
                    while slots.holding.available_permits() < 4 {
                        assert!(
                            Instant::now() < deadline,
                            "{db}: the slot never returned"
                        );
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                    assert_eq!(slots.retired(), 0, "{db}");
                    assert_eq!(
                        kill_failures(),
                        failures,
                        "{db}: a kill failed"
                    );
                    holder.query_drop("UNLOCK TABLES").await.unwrap();
                }
            }

            /// A capture cancelled while it holds its table's metadata lock
            /// (single and batched): a DDL queued behind it completes at
            /// once, and its slot returns.
            #[tokio::test]
            #[ignore = "requires docker"]
            async fn a_cancelled_capture_lets_a_queued_ddl_proceed() {
                let dsn = server(true).await;
                for (db, batched) in
                    [("stop_ddl_one", false), ("stop_ddl_batch", true)]
                {
                    sql(
                        &dsn,
                        &[
                            &format!("CREATE DATABASE {db}"),
                            &format!(
                                "CREATE TABLE {db}.t (id INT PRIMARY KEY)"
                            ),
                        ],
                    )
                    .await;
                    let slots: &'static CaptureSlots =
                        Box::leak(Box::new(CaptureSlots::new(4, 4)));
                    let cx = CaptureCtx::detached(dsn.as_str());
                    let inside = Arc::new(AtomicU32::new(0));
                    let task = cancellable(
                        slots,
                        cx.clone(),
                        &uuid(&dsn).await,
                        db,
                        batched,
                        Some(inside.clone()),
                    );
                    let deadline = Instant::now() + Duration::from_secs(20);
                    while inside.load(Ordering::SeqCst) == 0 {
                        assert!(
                            Instant::now() < deadline,
                            "{db}: never locked"
                        );
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                    assert!(
                        locked(&dsn, db).await,
                        "{db}: the capture holds no lock"
                    );
                    let d = dsn.clone();
                    let ddl = tokio::spawn(async move {
                        let mut c =
                            mysql_async::Conn::from_url(&d).await.unwrap();
                        c.query_drop(format!(
                            "ALTER TABLE {db}.t ADD COLUMN c INT"
                        ))
                        .await
                    });
                    let deadline = Instant::now() + Duration::from_secs(20);
                    loop {
                        let mut c =
                            mysql_async::Conn::from_url(&dsn).await.unwrap();
                        let queued: Vec<u64> = c
                            .exec(
                                "SELECT ID FROM information_schema.PROCESSLIST \
                                 WHERE STATE = 'Waiting for table metadata lock' AND INFO LIKE ?",
                                (format!("ALTER TABLE {db}.t%"),),
                            )
                            .await
                            .unwrap();
                        if !queued.is_empty() {
                            break;
                        }
                        assert!(
                            Instant::now() < deadline,
                            "{db}: the DDL never queued"
                        );
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    cx.stop();
                    let r = tokio::time::timeout(Duration::from_secs(2), task)
                        .await
                        .expect("the cancelled capture returns at once")
                        .unwrap();
                    assert!(matches!(r, Err(Cancelled)), "{db}: {r:?}");
                    tokio::time::timeout(Duration::from_secs(3), ddl)
                        .await
                        .expect("the queued DDL proceeds")
                        .unwrap()
                        .unwrap();
                    let deadline = Instant::now() + Duration::from_secs(3);
                    while slots.holding.available_permits() < 4 {
                        assert!(
                            Instant::now() < deadline,
                            "{db}: the slot never returned"
                        );
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                }
            }

            /// A cancelled capture whose kill fails still returns at once;
            /// the failure is counted and the slot retired (its session may
            /// still hold the lock).
            #[tokio::test]
            #[ignore = "requires docker"]
            async fn a_cancelled_capture_whose_kill_fails_retires_its_slot() {
                let dsn = server(true).await;
                let db = "stop_kill_fails";
                sql(
                    &dsn,
                    &[
                        &format!("CREATE DATABASE {db}"),
                        &format!("CREATE TABLE {db}.t (id INT PRIMARY KEY)"),
                    ],
                )
                .await;
                let slots: &'static CaptureSlots =
                    Box::leak(Box::new(CaptureSlots::new(2, 2)));
                slots.fail_kills.store(true, Ordering::SeqCst);
                let cx = CaptureCtx::detached(dsn.as_str());
                let inside = Arc::new(AtomicU32::new(0));
                let task = cancellable(
                    slots,
                    cx.clone(),
                    &uuid(&dsn).await,
                    db,
                    false,
                    Some(inside.clone()),
                );
                let deadline = Instant::now() + Duration::from_secs(20);
                while inside.load(Ordering::SeqCst) == 0 {
                    assert!(Instant::now() < deadline, "never locked");
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                let failures = kill_failures();
                cx.stop();
                let r = tokio::time::timeout(Duration::from_secs(2), task)
                    .await
                    .expect("the cancelled capture returns at once")
                    .unwrap();
                assert!(matches!(r, Err(Cancelled)), "{r:?}");
                let deadline = Instant::now() + Duration::from_secs(15);
                while kill_failures() == failures {
                    assert!(
                        Instant::now() < deadline,
                        "the failed kill was not counted"
                    );
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
                assert_eq!(slots.retired(), 1);
                assert_eq!(
                    slots.holding.available_permits(),
                    1,
                    "the slot came back"
                );
            }

            /// At most `holding` captures hold metadata locks at once: the
            /// next one waits, holding nothing, until a slot frees.
            #[tokio::test]
            #[ignore = "requires docker"]
            async fn at_most_the_bound_of_captures_hold_locks_at_once() {
                let dsn = server(true).await;
                let db = "cancel_bound";
                sql(&dsn, &[&format!("CREATE DATABASE {db}")]).await;
                for i in 0..5 {
                    sql(
                        &dsn,
                        &[&format!(
                            "CREATE TABLE {db}.t{i} (id INT PRIMARY KEY)"
                        )],
                    )
                    .await;
                }
                let slots: &'static CaptureSlots =
                    Box::leak(Box::new(CaptureSlots::new(4, 4)));
                let inside = Arc::new(AtomicU32::new(0));
                let release = Arc::new(tokio::sync::Semaphore::new(0));
                let u = uuid(&dsn).await;
                let mut tasks = Vec::new();
                for i in 0..5 {
                    let (d, u, inside, release) = (
                        dsn.clone(),
                        u.clone(),
                        inside.clone(),
                        release.clone(),
                    );
                    tasks.push(tokio::spawn(async move {
                        let hook = move || -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
                            let (inside, release) = (inside.clone(), release.clone());
                            Box::pin(async move {
                                inside.fetch_add(1, Ordering::SeqCst);
                                release.acquire().await.unwrap().forget();
                            })
                        };
                        capture_with(slots, &CaptureCtx::detached(d.as_str()), &u, LINEAGE, db, &format!("t{i}"), Some(&hook)).await.expect("not cancelled")
                    }));
                }
                tokio::time::sleep(Duration::from_secs(3)).await;
                assert_eq!(
                    inside.load(Ordering::SeqCst),
                    4,
                    "captures holding locks at once"
                );
                release.add_permits(5);
                for t in tasks {
                    t.await.unwrap().unwrap();
                }
                assert_eq!(inside.load(Ordering::SeqCst), 5);
            }

            /// A capture waiting for a slot spends its lock-wait budget: with
            /// every slot held, it ends as contention within that budget.
            #[tokio::test]
            #[ignore = "requires docker"]
            async fn waiting_for_a_capture_slot_counts_toward_the_lock_wait() {
                let dsn = server(true).await;
                let db = "cancel_wait";
                sql(
                    &dsn,
                    &[
                        &format!("CREATE DATABASE {db}"),
                        &format!("CREATE TABLE {db}.a (id INT PRIMARY KEY)"),
                        &format!("CREATE TABLE {db}.b (id INT PRIMARY KEY)"),
                    ],
                )
                .await;
                let slots: &'static CaptureSlots =
                    Box::leak(Box::new(CaptureSlots::new(1, 1)));
                let u = uuid(&dsn).await;
                let release = Arc::new(tokio::sync::Semaphore::new(0));
                let held = {
                    let (d, u, release) =
                        (dsn.clone(), u.clone(), release.clone());
                    tokio::spawn(async move {
                        let hook = move || -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
                            let release = release.clone();
                            Box::pin(async move { release.acquire().await.unwrap().forget(); })
                        };
                        let tables = vec![(db.to_string(), "a".to_string())];
                        capture_batch_with(
                            slots,
                            &CaptureCtx::detached(d.as_str()),
                            &u,
                            LINEAGE,
                            &tables,
                            8,
                            Some(&hook),
                        )
                        .await
                        .expect("not cancelled")
                    })
                };
                tokio::time::sleep(Duration::from_secs(1)).await;
                let started = Instant::now();
                let tables = vec![(db.to_string(), "b".to_string())];
                let waited = capture_batch_with(
                    slots,
                    &CaptureCtx::detached(dsn.as_str()),
                    &u,
                    LINEAGE,
                    &tables,
                    1,
                    None,
                )
                .await
                .expect("not cancelled");
                assert!(
                    matches!(waited, Err(BatchCaptureError::Contention { .. })),
                    "a capture ran with every slot held"
                );
                assert!(started.elapsed() < Duration::from_secs(5));
                release.add_permits(1);
                assert!(held.await.unwrap().is_ok());
            }

            /// With every slot held by a lock-holding capture, dropping them
            /// all kills every session: cancellation draws only on its own
            /// reserve, never on what the captures hold.
            #[tokio::test]
            #[ignore = "requires docker"]
            async fn every_held_capture_can_be_cancelled_at_once() {
                let dsn = server(true).await;
                let db = "cancel_all";
                sql(&dsn, &[&format!("CREATE DATABASE {db}")]).await;
                for i in 0..4 {
                    sql(
                        &dsn,
                        &[&format!(
                            "CREATE TABLE {db}.t{i} (id INT PRIMARY KEY)"
                        )],
                    )
                    .await;
                }
                let slots: &'static CaptureSlots =
                    Box::leak(Box::new(CaptureSlots::new(4, 4)));
                let u = uuid(&dsn).await;
                let inside = Arc::new(AtomicU32::new(0));
                let mut tasks = Vec::new();
                for i in 0..4 {
                    let (d, u, inside) =
                        (dsn.clone(), u.clone(), inside.clone());
                    tasks.push(tokio::spawn(async move {
                        let hook = move || -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
                            let inside = inside.clone();
                            Box::pin(async move {
                                inside.fetch_add(1, Ordering::SeqCst);
                                std::future::pending::<()>().await;
                            })
                        };
                        let tables = vec![(db.to_string(), format!("t{i}"))];
                        capture_batch_with(slots, &CaptureCtx::detached(d.as_str()), &u, LINEAGE, &tables, 8, Some(&hook)).await.expect("not cancelled")
                    }));
                }
                let deadline = Instant::now() + Duration::from_secs(20);
                while inside.load(Ordering::SeqCst) < 4 {
                    assert!(
                        Instant::now() < deadline,
                        "the captures never all held locks"
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                let failures = kill_failures();
                slots.kill_delay_ms.store(1000, Ordering::SeqCst);
                for t in &tasks {
                    t.abort();
                }
                // While the kills are delayed, no slot comes back.
                tokio::time::sleep(Duration::from_millis(300)).await;
                assert_eq!(
                    slots.holding.available_permits(),
                    0,
                    "a slot came back before its session was killed"
                );
                // Every slot comes back only after its session was killed.
                let deadline = Instant::now() + Duration::from_secs(15);
                while slots.holding.available_permits() < 4 {
                    assert!(
                        Instant::now() < deadline,
                        "a cancellation did not complete"
                    );
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                // A slot is back only once its session is gone: no metadata
                // lock of these captures remains at that moment.
                let mut c = mysql_async::Conn::from_url(&dsn).await.unwrap();
                let held: Vec<String> = c
                    .exec(
                        "SELECT OBJECT_NAME FROM performance_schema.metadata_locks WHERE OBJECT_SCHEMA = ?",
                        (db,),
                    )
                    .await
                    .unwrap();
                assert!(
                    held.is_empty(),
                    "slots came back while locks remained: {held:?}"
                );
                assert_eq!(kill_failures(), failures, "a cancellation failed");
                sql(&dsn, &[&format!("ALTER TABLE {db}.t0 ADD COLUMN v INT")])
                    .await;
            }

            /// A cancellation that does not resolve retires its slot: the
            /// session may still hold its locks, so the slot never returns,
            /// further captures get only the remaining capacity, and a
            /// successful cancellation still returns its own slot.
            #[tokio::test]
            #[ignore = "requires docker"]
            async fn an_unresolved_cancellation_retires_its_slot() {
                let dsn = server(true).await;
                let db = "cancel_fail";
                sql(&dsn, &[&format!("CREATE DATABASE {db}")]).await;
                for t in ["a", "b", "c"] {
                    sql(
                        &dsn,
                        &[&format!(
                            "CREATE TABLE {db}.{t} (id INT PRIMARY KEY)"
                        )],
                    )
                    .await;
                }
                let slots: &'static CaptureSlots =
                    Box::leak(Box::new(CaptureSlots::new(2, 2)));
                let u = uuid(&dsn).await;
                let held = |table: &'static str, inside: Arc<AtomicU32>| {
                    let (d, u) = (dsn.clone(), u.clone());
                    tokio::spawn(async move {
                        let hook = move || -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
                            let inside = inside.clone();
                            Box::pin(async move {
                                inside.fetch_add(1, Ordering::SeqCst);
                                std::future::pending::<()>().await;
                            })
                        };
                        let tables = vec![(db.to_string(), table.to_string())];
                        capture_batch_with(
                            slots,
                            &CaptureCtx::detached(d.as_str()),
                            &u,
                            LINEAGE,
                            &tables,
                            8,
                            Some(&hook),
                        )
                        .await
                        .expect("not cancelled")
                    })
                };
                let until = |inside: Arc<AtomicU32>| async move {
                    let deadline = Instant::now() + Duration::from_secs(20);
                    while inside.load(Ordering::SeqCst) == 0 {
                        assert!(
                            Instant::now() < deadline,
                            "the capture never held its lock"
                        );
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                };

                // A holds a real metadata lock, and its kill fails.
                let a_inside = Arc::new(AtomicU32::new(0));
                let a = held("a", a_inside.clone());
                until(a_inside).await;
                let mut c = mysql_async::Conn::from_url(&dsn).await.unwrap();
                let locked: Vec<String> = c
                    .exec(
                        "SELECT OBJECT_NAME FROM performance_schema.metadata_locks WHERE OBJECT_SCHEMA = ? AND OBJECT_NAME = 'a'",
                        (db,),
                    )
                    .await
                    .unwrap();
                assert!(!locked.is_empty(), "the capture held no lock");
                slots.fail_kills.store(true, Ordering::SeqCst);
                let failures = kill_failures();
                a.abort();
                let deadline = Instant::now() + Duration::from_secs(15);
                while kill_failures() == failures {
                    assert!(
                        Instant::now() < deadline,
                        "the failed kill was not counted"
                    );
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
                assert_eq!(
                    slots.retired(),
                    1,
                    "the failed kill's slot is retired"
                );
                assert_eq!(
                    slots.holding.available_permits(),
                    1,
                    "the failed kill's slot came back"
                );

                // The remaining capacity is one: B takes it, C cannot.
                slots.fail_kills.store(false, Ordering::SeqCst);
                let b_inside = Arc::new(AtomicU32::new(0));
                let b = held("b", b_inside.clone());
                until(b_inside).await;
                let tables = vec![(db.to_string(), "c".to_string())];
                let third = capture_batch_with(
                    slots,
                    &CaptureCtx::detached(dsn.as_str()),
                    &u,
                    LINEAGE,
                    &tables,
                    1,
                    None,
                )
                .await
                .expect("not cancelled");
                assert!(
                    matches!(third, Err(BatchCaptureError::Contention { .. })),
                    "a capture exceeded the remaining capacity"
                );

                // A successful cancellation still returns its slot.
                b.abort();
                let deadline = Instant::now() + Duration::from_secs(15);
                while slots.holding.available_permits() < 1 {
                    assert!(
                        Instant::now() < deadline,
                        "the successful kill's slot never came back"
                    );
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                assert_eq!(slots.holding.available_permits(), 1);
                assert_eq!(slots.retired(), 1);
                assert_eq!(
                    kill_failures(),
                    failures + 1,
                    "only the failed kill is counted"
                );
            }

            /// A cancellation connects with the run's DSN as it is when the
            /// kill runs: a capture started on credentials A and blocked on a
            /// held lock, dropped after a rotation replaced the shared DSN
            /// with B and revoked A, is still killed.
            #[tokio::test]
            #[ignore = "requires docker"]
            async fn a_cancellation_uses_the_dsn_replaced_after_its_capture_started()
             {
                let dsn = server(true).await;
                let db = "cancel_rotated";
                let user = "cancel_rot";
                sql(
                    &dsn,
                    &[
                        &format!("CREATE DATABASE {db}"),
                        &format!("CREATE TABLE {db}.t (id INT PRIMARY KEY)"),
                        &format!("DROP USER IF EXISTS '{user}'@'%'"),
                        &format!("CREATE USER '{user}'@'%' IDENTIFIED BY 'pw-before'"),
                        &format!("GRANT SELECT, REPLICATION CLIENT ON *.* TO '{user}'@'%'"),
                    ],
                )
                .await;
                let port = port(true).await;
                let as_user =
                    |pw: &str| format!("mysql://{user}:{pw}@127.0.0.1:{port}/");
                let shared =
                    crate::credentials::SharedDsn::from(as_user("pw-before"));
                let mut holder =
                    mysql_async::Conn::from_url(&dsn).await.unwrap();
                holder
                    .query_drop(format!("LOCK TABLES {db}.t WRITE"))
                    .await
                    .unwrap();
                let slots: &'static CaptureSlots =
                    Box::leak(Box::new(CaptureSlots::new(4, 4)));
                let u = uuid(&dsn).await;
                let task = {
                    // The capture's context shares the DSN the test replaces.
                    let cx = CaptureCtx::detached(shared.clone());
                    tokio::spawn(async move {
                        let tables = vec![(db.to_string(), "t".to_string())];
                        capture_batch_with(
                            slots, &cx, &u, LINEAGE, &tables, 30, None,
                        )
                        .await
                        .expect("not cancelled")
                    })
                };
                let waiting = |dsn: String| async move {
                    let mut c =
                        mysql_async::Conn::from_url(&dsn).await.unwrap();
                    let ids: Vec<u64> = c
                        .exec(
                            "SELECT ID FROM information_schema.PROCESSLIST WHERE USER = ? \
                             AND STATE = 'Waiting for table metadata lock' AND ID <> CONNECTION_ID()",
                            (user,),
                        )
                        .await
                        .unwrap();
                    ids
                };
                let deadline = Instant::now() + Duration::from_secs(20);
                while waiting(dsn.clone()).await.is_empty() {
                    assert!(
                        Instant::now() < deadline,
                        "the capture never queued"
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                // Rotate: A stops working (the queued session stays), the
                // shared DSN becomes B.
                sql(
                    &dsn,
                    &[&format!(
                        "ALTER USER '{user}'@'%' IDENTIFIED BY 'pw-after'"
                    )],
                )
                .await;
                shared.replace(as_user("pw-after").into());
                let failures = kill_failures();
                task.abort();
                let deadline = Instant::now() + Duration::from_secs(3);
                while !waiting(dsn.clone()).await.is_empty() {
                    assert!(
                        Instant::now() < deadline,
                        "the cancelled capture was not killed with the replaced DSN"
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                assert_eq!(kill_failures(), failures, "the kill failed");
                holder.query_drop("UNLOCK TABLES").await.unwrap();
                sql(&dsn, &[&format!("DROP USER '{user}'@'%'")]).await;
            }

            /// A wide table (eight 1,000-character column comments).
            fn wide(db: &str, t: &str) -> String {
                let comment = "x".repeat(1000);
                let cols: String = (1..=8)
                    .map(|k| format!(", c{k} INT COMMENT '{comment}'"))
                    .collect();
                format!("CREATE TABLE {db}.{t} (id INT PRIMARY KEY{cols})")
            }

            /// A batched capture reads its tables' schemas in the window's
            /// rank order, not by name, within the plan budget: the tables
            /// before the first one over it are captured, that one and the
            /// rest are deferred unread (neither captured nor missing), and
            /// a missing table among the kept stays missing.
            #[tokio::test]
            #[ignore = "requires docker"]
            async fn a_batched_capture_reads_in_rank_order_within_its_budget() {
                let dsn = server(true).await;
                let db = "rank_cap";
                sql(
                    &dsn,
                    &[
                        &format!("CREATE DATABASE {db}"),
                        &format!("CREATE TABLE {db}.zz (id INT PRIMARY KEY)"),
                        &wide(db, "mm"),
                        &format!("CREATE TABLE {db}.aa (id INT PRIMARY KEY)"),
                    ],
                )
                .await;
                let k = |t: &str| (db.to_string(), t.to_string());
                let ranked = vec![k("zz"), k("gone"), k("mm"), k("aa")];
                let slots: &'static CaptureSlots =
                    Box::leak(Box::new(CaptureSlots::new(4, 4)));
                crate::stream_probe::set_window_caps(Some((
                    5_000, 16_384, 5_000,
                )));
                let cap = capture_batch_with(
                    slots,
                    &CaptureCtx::detached(dsn.as_str()),
                    &uuid(&dsn).await,
                    LINEAGE,
                    &ranked,
                    8,
                    None,
                )
                .await
                .expect("not cancelled");
                crate::stream_probe::set_window_caps(None);
                let cap = cap.unwrap();
                let mut captured: Vec<_> =
                    cap.schemas.keys().cloned().collect();
                captured.sort();
                assert_eq!(captured, vec![k("zz")]);
                assert_eq!(cap.missing, vec![k("gone")]);
                assert_eq!(cap.deferred, vec![k("mm"), k("aa")]);
                assert_eq!(cap.oversized, None);
                // Unlimited, every table is captured, whatever its rank.
                let all = capture_batch_with(
                    slots,
                    &CaptureCtx::detached(dsn.as_str()),
                    &uuid(&dsn).await,
                    LINEAGE,
                    &ranked,
                    8,
                    None,
                )
                .await
                .expect("not cancelled")
                .unwrap();
                assert_eq!(all.schemas.len(), 3);
                assert!(all.deferred.is_empty());
                assert_eq!(all.schemas[&k("mm")].columns.len(), 9);
            }

            /// The largest table definition MySQL 8.4 accepts (4,096
            /// columns, the most any engine allows, each with a maximal
            /// 1,024-character comment and a default) is read whole within
            /// the store's item limit, and its encoded schema takes at most
            /// half of it: a registration of any real table is a valid item.
            #[tokio::test]
            #[ignore = "requires docker"]
            async fn the_largest_table_definition_fits_half_the_item_limit() {
                let dsn = server(true).await;
                let db = "max_def";
                let comment = "c".repeat(1024);
                let cols: String = (0..4095)
                    .map(|k| {
                        format!(", col_{k:04} INT NOT NULL DEFAULT {k} COMMENT '{comment}'")
                    })
                    .collect();
                sql(
                    &dsn,
                    &[
                        &format!("CREATE DATABASE {db}"),
                        &format!(
                            "CREATE TABLE {db}.widest (id INT PRIMARY KEY COMMENT '{comment}'{cols}) \
                             ENGINE=MyISAM"
                        ),
                    ],
                )
                .await;
                let mut c = mysql_async::Conn::from_url(&dsn).await.unwrap();
                let read = crate::mysql::mysql_schema_loader::fetch_table_schemas_within(
                    &mut c,
                    &[(db, "widest")],
                    u64::MAX,
                    storage::MAX_STATE_ITEM_BYTES as u64,
                )
                .await
                .unwrap();
                assert_eq!(read.kept, 1, "read whole within the item limit");
                assert_eq!(read.oversized, None);
                let schema =
                    &read.schemas[&(db.to_string(), "widest".to_string())];
                assert_eq!(schema.columns.len(), 4096);
                let encoded = serde_json::to_vec(schema).unwrap().len();
                eprintln!("the widest table's encoded schema: {encoded} bytes");
                // Its registration entry wraps the schema with a few fields.
                assert!(
                    encoded + 4096 <= storage::MAX_STATE_ITEM_BYTES / 2,
                    "{encoded} bytes"
                );
            }

            /// A table whose own rows exceed the item limit stops the
            /// schema read where it is reached: as the first table, nothing
            /// is kept and its size is reported; later, the tables before it
            /// are kept whole.
            #[tokio::test]
            #[ignore = "requires docker"]
            async fn a_table_over_the_item_limit_stops_the_schema_read() {
                let dsn = server(true).await;
                let db = "item_cap";
                sql(
                    &dsn,
                    &[
                        &format!("CREATE DATABASE {db}"),
                        &format!(
                            "CREATE TABLE {db}.small (id INT PRIMARY KEY)"
                        ),
                        &wide(db, "big"),
                    ],
                )
                .await;
                let read = |tables: Vec<(&'static str, &'static str)>| {
                    let dsn = dsn.clone();
                    async move {
                        let mut c =
                            mysql_async::Conn::from_url(&dsn).await.unwrap();
                        crate::mysql::mysql_schema_loader::fetch_table_schemas_within(
                            &mut c, &tables, u64::MAX, 2_000,
                        )
                        .await
                        .unwrap()
                    }
                };
                let first = read(vec![(db, "big"), (db, "small")]).await;
                assert_eq!(first.kept, 0);
                assert!(
                    first.oversized.is_some_and(|b| b > 2_000),
                    "{first:?}"
                );
                assert!(first.schemas.is_empty());
                let later = read(vec![(db, "small"), (db, "big")]).await;
                assert_eq!(later.kept, 1);
                assert!(later.oversized.is_some());
                assert_eq!(later.schemas.len(), 1);
            }

            /// A batched capture locks its tables in sorted order, whatever
            /// order it is given: a two-table RENAME taking the same locks
            /// (MySQL locks a multi-table statement's tables in that order)
            /// waits for it instead of deadlocking (probe 7: the reverse order
            /// deadlocks, 1213).
            #[tokio::test]
            #[ignore = "requires docker"]
            async fn a_batched_capture_locks_in_sorted_order() {
                let dsn = server(true).await;
                let db = "batch_order";
                sql(
                    &dsn,
                    &[
                        &format!("CREATE DATABASE {db}"),
                        &format!("CREATE TABLE {db}.t010 (id INT PRIMARY KEY)"),
                        &format!("CREATE TABLE {db}.t011 (id INT PRIMARY KEY)"),
                    ],
                )
                .await;
                let rename = std::sync::Arc::new(tokio::sync::Mutex::new(None));
                let (d, r) = (dsn.clone(), rename.clone());
                let hook = move || -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
                let (d, r) = (d.clone(), r.clone());
                Box::pin(async move {
                    // The RENAME queues behind the first lock; give it time
                    // to take what it can before the batch's second lock.
                    let task = tokio::spawn(async move {
                        let mut c = mysql_async::Conn::from_url(&d).await.unwrap();
                        c.query_drop(format!(
                            "RENAME TABLE {db}.t010 TO {db}.r010, {db}.t011 TO {db}.r011"
                        ))
                        .await
                    });
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    *r.lock().await = Some(task);
                })
            };
                let tables = vec![
                    (db.to_string(), "t011".to_string()),
                    (db.to_string(), "t010".to_string()),
                ];
                let cap = capture_batch(
                    &CaptureCtx::detached(dsn.as_str()),
                    &uuid(&dsn).await,
                    LINEAGE,
                    &tables,
                    8,
                    Some(&hook),
                )
                .await
                .expect("not cancelled");
                assert!(cap.is_ok(), "{cap:?}");
                let cap = cap.unwrap();
                assert_eq!(cap.schemas.len(), 2);
                assert!(cap.missing.is_empty());
                let task = rename.lock().await.take().expect("the rename ran");
                task.await
                    .unwrap()
                    .expect("the rename waits, then succeeds");
            }

            /// A lock timeout fails the attempt as contention, and the attempt
            /// holds no lock afterwards: a DDL on the batch's other table runs
            /// at once.
            #[tokio::test]
            #[ignore = "requires docker"]
            async fn a_lock_timeout_is_contention_and_holds_nothing() {
                let dsn = server(true).await;
                let db = "batch_timeout";
                sql(
                    &dsn,
                    &[
                        &format!("CREATE DATABASE {db}"),
                        &format!("CREATE TABLE {db}.a (id INT PRIMARY KEY)"),
                        &format!("CREATE TABLE {db}.b (id INT PRIMARY KEY)"),
                    ],
                )
                .await;
                let mut locker =
                    mysql_async::Conn::from_url(&dsn).await.unwrap();
                locker
                    .query_drop(format!("LOCK TABLES {db}.b WRITE"))
                    .await
                    .unwrap();
                let tables = vec![
                    (db.to_string(), "b".to_string()),
                    (db.to_string(), "a".to_string()),
                ];
                let r = capture_batch(
                    &CaptureCtx::detached(dsn.as_str()),
                    &uuid(&dsn).await,
                    LINEAGE,
                    &tables,
                    1,
                    None,
                )
                .await
                .expect("not cancelled");
                assert!(
                    matches!(
                        r,
                        Err(BatchCaptureError::Contention { code: 1205, .. })
                    ),
                    "{r:?}"
                );
                let mut ddl = mysql_async::Conn::from_url(&dsn).await.unwrap();
                ddl.query_drop("SET SESSION lock_wait_timeout = 2")
                    .await
                    .unwrap();
                ddl.query_drop(format!("ALTER TABLE {db}.a ADD COLUMN c INT"))
                    .await
                    .expect("no lock on a is left behind");
                locker.query_drop("UNLOCK TABLES").await.unwrap();
            }

            /// A table that no longer exists has no shape: the batch captures
            /// the others and names it missing.
            #[tokio::test]
            #[ignore = "requires docker"]
            async fn a_missing_table_does_not_fail_the_batch() {
                let dsn = server(true).await;
                let db = "batch_missing";
                sql(
                    &dsn,
                    &[
                        &format!("CREATE DATABASE {db}"),
                        &format!("CREATE TABLE {db}.a (id INT PRIMARY KEY)"),
                    ],
                )
                .await;
                let tables = vec![
                    (db.to_string(), "gone".to_string()),
                    (db.to_string(), "a".to_string()),
                ];
                let cap = capture_batch(
                    &CaptureCtx::detached(dsn.as_str()),
                    &uuid(&dsn).await,
                    LINEAGE,
                    &tables,
                    5,
                    None,
                )
                .await
                .expect("not cancelled")
                .unwrap();
                assert!(cap.table(db, "a").is_some());
                assert!(cap.table(db, "gone").is_none());
                assert_eq!(
                    cap.missing,
                    vec![(db.to_string(), "gone".to_string())]
                );
            }

            /// A boundary after A, with a DDL in between, never proves:
            /// refused at the first GTID when the next one is known, and
            /// otherwise at the live end, within seconds.
            #[tokio::test]
            #[ignore = "requires docker"]
            async fn a_late_anchor_is_refused_promptly() {
                let dsn = server(true).await;
                let db = "pos_late";
                sql(&dsn, &[&format!("CREATE DATABASE {db}")]).await;
                let a = position(&dsn).await;
                sql(
                    &dsn,
                    &[&format!("CREATE TABLE {db}.t (id INT PRIMARY KEY)")],
                )
                .await;
                let b = position(&dsn).await;
                sql(
                    &dsn,
                    &[&format!("CREATE TABLE {db}.u (id INT PRIMARY KEY)")],
                )
                .await;
                let c = position(&dsn).await;
                let head = at(&dsn, &a, &b, None).await.unwrap();
                let true_next = head.session_anchor.unwrap().next_gtid;
                // B's frontier, claimed as a start at A.
                let late = |next_gtid| Anchor {
                    set_before: set(&a),
                    next_gtid,
                    ..head.frontier.clone().unwrap()
                };

                let err =
                    at(&dsn, &a, &c, Some(&late(true_next))).await.unwrap_err();
                assert!(
                    matches!(err, ProofError::AnchorMismatch(_)),
                    "{err:?}"
                );

                let started = Instant::now();
                let err =
                    at(&dsn, &a, &c, Some(&late(None))).await.unwrap_err();
                assert!(matches!(err, ProofError::NotReached(_)), "{err:?}");
                assert!(started.elapsed() < Duration::from_secs(10));

                // The direct scan has the DDL the late start would omit.
                let direct = at(&dsn, &a, &c, None).await.unwrap();
                assert_eq!(
                    affected(&direct),
                    [format!("{db}.t"), format!("{db}.u")]
                );
            }

            #[tokio::test]
            #[ignore = "requires docker"]
            async fn an_anchor_of_another_server_lineage_or_boundary_is_refused()
             {
                let dsn = server(true).await;
                let db = "pos_foreign";
                sql(&dsn, &[&format!("CREATE DATABASE {db}")]).await;
                let a = position(&dsn).await;
                sql(
                    &dsn,
                    &[&format!("CREATE TABLE {db}.t (id INT PRIMARY KEY)")],
                )
                .await;
                let b = position(&dsn).await;
                let good = at(&dsn, &a, &b, None)
                    .await
                    .unwrap()
                    .session_anchor
                    .unwrap();
                let other = Anchor {
                    server_uuid: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee".into(),
                    ..good.clone()
                };
                assert!(matches!(
                    at(&dsn, &a, &b, Some(&other)).await,
                    Err(ProofError::OtherServer(_))
                ));
                let lineage = Anchor {
                    lineage: "ffff".into(),
                    ..good.clone()
                };
                assert!(matches!(
                    at(&dsn, &a, &b, Some(&lineage)).await,
                    Err(ProofError::OtherServer(_))
                ));
                let boundary = Anchor {
                    set_before: set(&b),
                    ..good.clone()
                };
                assert!(matches!(
                    at(&dsn, &a, &b, Some(&boundary)).await,
                    Err(ProofError::Positions(_))
                ));
                // Inside an event: the server refuses the dump.
                let inside = Anchor {
                    offset: good.offset + 1,
                    ..good.clone()
                };
                assert!(at(&dsn, &a, &b, Some(&inside)).await.is_err());
                at(&dsn, &a, &b, Some(&good)).await.unwrap();
            }

            /// The bounded alternative: no transaction within `max_seek` of
            /// opening is a typed refusal, not a wait for the time limit.
            #[tokio::test]
            #[ignore = "requires docker"]
            async fn the_seek_is_bounded() {
                let dsn = server(true).await;
                let db = "pos_seek";
                sql(&dsn, &[&format!("CREATE DATABASE {db}")]).await;
                let a = position(&dsn).await;
                sql(
                    &dsn,
                    &[&format!("CREATE TABLE {db}.t (id INT PRIMARY KEY)")],
                )
                .await;
                let b = position(&dsn).await;
                let r = scan_interval_at(
                    &dsn,
                    4_000_125,
                    &uuid(&dsn).await,
                    LINEAGE,
                    &a,
                    &b,
                    None,
                    &ScanLimits {
                        max_seek: Duration::ZERO,
                        ..ScanLimits::default()
                    },
                    &TAG,
                )
                .await;
                assert!(matches!(r, Err(ProofError::SeekLimit(_))), "{r:?}");
            }
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
                            &TAG,
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
                &TAG,
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
                &TAG,
            )
            .await;
            assert!(matches!(r, Err(ProofError::Limit(_))), "{r:?}");
        }

        /// A finished scan leaves no dump thread on the server, also when it
        /// fails: the scan resets its own session, and an idle server's next
        /// heartbeat write then fails (without it the thread stays until the
        /// server next writes a binlog event). Nothing is killed by id.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_scan_ends_its_server_session() {
            // Own server: no other replication session may be connected.
            let (_c, port) = start(true).await;
            let dsn = dsn_of(port);
            sql(
                &dsn,
                &["CREATE DATABASE s", "CREATE TABLE s.t (id INT PRIMARY KEY)"],
            )
            .await;
            let from = position(&dsn).await;
            sql(&dsn, &["INSERT INTO s.t VALUES (1)"]).await;
            let to = position(&dsn).await;
            let dumps = || async {
                let mut c =
                    mysql_async::Conn::from_url(dsn.as_str()).await.unwrap();
                let n: Vec<u64> = c
                    .query(
                        "SELECT ID FROM information_schema.PROCESSLIST \
                         WHERE COMMAND LIKE 'Binlog Dump%'",
                    )
                    .await
                    .unwrap();
                c.disconnect().await.ok();
                n
            };
            // A complete scan, and one that fails after its session opened
            // (a resource limit).
            let uuid = uuid(&dsn).await;
            for limits in [
                ScanLimits::default(),
                ScanLimits {
                    max_events: 1,
                    ..ScanLimits::default()
                },
            ] {
                let r = scan_interval(
                    &dsn, 4_000_124, &uuid, LINEAGE, &from, &to, &limits, &TAG,
                )
                .await;
                assert_eq!(
                    r.is_ok(),
                    limits.max_events > 1,
                    "{:?}",
                    r.map(|r| r.events)
                );
                let deadline =
                    std::time::Instant::now() + Duration::from_secs(5);
                loop {
                    let left = dumps().await;
                    if left.is_empty() {
                        eprintln!(
                            "dump thread gone {} ms after the scan",
                            5000 - deadline
                                .saturating_duration_since(
                                    std::time::Instant::now()
                                )
                                .as_millis()
                        );
                        break;
                    }
                    assert!(
                        std::time::Instant::now() < deadline,
                        "dump threads left behind: {left:?}"
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }

        /// The scanner's sessions are armed for an abortive close as soon as
        /// they are open, before anything is read; the stream's are not.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn scan_sessions_are_armed_for_an_abortive_close_on_open() {
            use crate::mysql::mysql_session::{
                open_replication_session, open_replication_session_abortive,
            };
            let dsn = server(true).await;
            let uuid = uuid(&dsn).await;
            let client = BinlogClient {
                url: dsn.clone(),
                server_id: 4_000_126,
                gtid_enabled: true,
                gtid_set: position(&dsn).await.gtid_set.unwrap(),
                timeout_secs: 30,
                ..Default::default()
            };
            let scan =
                open_replication_session_abortive(&client, &uuid, "test")
                    .await
                    .unwrap();
            assert_eq!(scan.linger().unwrap(), Some(Duration::ZERO));
            drop(scan);
            let stream = open_replication_session(&client, &uuid, "test")
                .await
                .unwrap();
            assert_ne!(stream.linger().unwrap(), Some(Duration::ZERO));
        }

        /// A scan cancelled while it waits (its future dropped mid-read)
        /// also leaves no dump thread: the session's abortive close was armed
        /// right after it opened.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_cancelled_scan_ends_its_server_session() {
            // Own server: no other replication session may be connected.
            let (_c, port) = start(true).await;
            let dsn = dsn_of(port);
            let uuid = uuid(&dsn).await;
            let dumps = |dsn: String| async move {
                let mut c =
                    mysql_async::Conn::from_url(dsn.as_str()).await.unwrap();
                let n: Vec<u64> = c
                    .query(
                        "SELECT ID FROM information_schema.PROCESSLIST \
                         WHERE COMMAND LIKE 'Binlog Dump%'",
                    )
                    .await
                    .unwrap();
                c.disconnect().await.ok();
                n
            };
            let from = position(&dsn).await;
            // An end not yet executed: the scan waits for events.
            let mut to = from.clone();
            let set = from.gtid_set.clone().unwrap();
            let (head, last) = set.rsplit_once('-').unwrap();
            let n: u64 = last.parse().unwrap();
            to.gtid_set = Some(format!("{head}-{}", n + 1000));
            let task = tokio::spawn({
                let (dsn, uuid, from, to) =
                    (dsn.clone(), uuid.clone(), from, to);
                async move {
                    scan_interval(
                        &dsn,
                        4_000_125,
                        &uuid,
                        LINEAGE,
                        &from,
                        &to,
                        &ScanLimits {
                            max_duration: Duration::from_secs(120),
                            ..ScanLimits::default()
                        },
                        &TAG,
                    )
                    .await
                }
            });
            let deadline = std::time::Instant::now() + Duration::from_secs(20);
            while dumps(dsn.clone()).await.is_empty() {
                assert!(
                    std::time::Instant::now() < deadline,
                    "the scan never opened"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            let cancelled = std::time::Instant::now();
            loop {
                let left = dumps(dsn.clone()).await;
                if left.is_empty() {
                    eprintln!(
                        "dump thread gone {} ms after the cancellation",
                        cancelled.elapsed().as_millis()
                    );
                    break;
                }
                assert!(
                    cancelled.elapsed() < Duration::from_secs(5),
                    "dump thread left behind: {left:?}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
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
                &TAG,
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
                GtidSet::parse(p.gtid_set.as_deref().unwrap()).unwrap()
            };
            let (f, m, e) = (parse(&from), parse(&mid), parse(&end));
            let last = |s: &GtidSet| {
                s.intervals()
                    .find(|(uu, _)| uu.eq_ignore_ascii_case(&u))
                    .unwrap()
                    .1
                    .last()
                    .unwrap()
                    .1
            };
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

        type Action = Arc<
            dyn Fn() -> std::pin::Pin<
                    Box<dyn std::future::Future<Output = ()> + Send>,
                > + Send
                + Sync,
        >;

        /// Runs `stmts` the first `times` calls, then nothing.
        fn sql_times(dsn: &str, stmts: Vec<String>, times: u32) -> Action {
            let (dsn, calls) = (dsn.to_string(), Arc::new(AtomicU32::new(0)));
            Arc::new(move || {
                let (dsn, stmts, calls) =
                    (dsn.clone(), stmts.clone(), calls.clone());
                Box::pin(async move {
                    if calls.fetch_add(1, Ordering::SeqCst) < times {
                        let s: Vec<&str> =
                            stmts.iter().map(String::as_str).collect();
                        sql(&dsn, &s).await;
                    }
                })
            })
        }

        fn limits() -> ScanLimits {
            ScanLimits {
                max_duration: Duration::from_secs(20),
                ..ScanLimits::default()
            }
        }

        async fn proven(
            dsn: &str,
            db: &str,
            attempts: u32,
            between: Option<&Action>,
        ) -> Result<Captured, ProofError> {
            let hook = between.map(|a| {
                let a = a.clone();
                move || a()
            });
            capture_proven(
                &CaptureCtx::detached(dsn),
                4_000_140,
                &uuid(dsn).await,
                LINEAGE,
                db,
                "t",
                0,
                attempts,
                &limits(),
                hook.as_ref().map(|h| h as BetweenHook<'_>),
                &TAG,
                None,
            )
            .await
            .expect("not cancelled")
        }

        /// The shared proof scanner equals direct scans on a real server:
        /// nested and overlapping requests over a workload with DDL (create,
        /// alter, rename, a database operation) between DML.
        /// A DDL committed inside a gap of boundary A's GTID set (with a
        /// number below A's highest) belongs to (A,B]: it is proved, and the
        /// shared and direct scans agree. An A that over-claims the gap (as
        /// `merge_gtid` once produced) would have the server skip it.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_ddl_filling_a_gtid_gap_after_the_boundary_is_proved() {
            use super::super::super::mysql_proof_scanner::{
                LiveSegments, ProofScanner, Segments,
            };
            let dsn = server(true).await;
            let id = uuid(&dsn).await;
            let db = "scan_gtid_gap";
            sql(
                &dsn,
                &[
                    &format!("CREATE DATABASE IF NOT EXISTS {db}"),
                    &format!(
                        "CREATE TABLE IF NOT EXISTS {db}.t (a INT PRIMARY KEY)"
                    ),
                    &format!("DELETE FROM {db}.t"),
                ],
            )
            .await;
            let own_last = |p: &MySqlCheckpoint| {
                GtidSet::parse(p.gtid_set.as_deref().unwrap())
                    .unwrap()
                    .intervals()
                    .find(|(u, _)| u.eq_ignore_ascii_case(&id))
                    .and_then(|(_, ivs)| ivs.last().map(|iv| iv.1))
                    .unwrap()
            };
            let n = own_last(&position(&dsn).await);
            // Leave a gap: a transaction with an explicit, later number.
            // Other tests' automatic GTIDs take the lowest free numbers, so
            // the gap is wide and the DDL's number explicit too.
            sql(
                &dsn,
                &[
                    &format!("SET gtid_next = '{id}:{}'", n + 100),
                    &format!("INSERT INTO {db}.t VALUES (1)"),
                    "SET gtid_next = 'AUTOMATIC'",
                ],
            )
            .await;
            let a = position(&dsn).await;
            let in_gap = format!("{id}:{}", n + 50);
            let a_set = GtidSet::parse(a.gtid_set.as_deref().unwrap()).unwrap();
            assert!(!a_set.contains(&in_gap), "A keeps the gap: {a_set}");
            sql(
                &dsn,
                &[
                    &format!("SET gtid_next = '{in_gap}'"),
                    &format!("ALTER TABLE {db}.t ADD COLUMN c INT"),
                    "SET gtid_next = 'AUTOMATIC'",
                ],
            )
            .await;
            // The rest of the gap fills, as it does later on a real server
            // (then an A over-claiming the gap is ordered before B).
            let mut fill = Vec::new();
            for g in (n + 1..n + 100).filter(|g| *g != n + 50) {
                fill.push(format!("SET gtid_next = '{id}:{g}'"));
                fill.push(format!("INSERT INTO {db}.t (a) VALUES ({g})"));
            }
            fill.push("SET gtid_next = 'AUTOMATIC'".into());
            sql(&dsn, &fill.iter().map(String::as_str).collect::<Vec<_>>())
                .await;
            let b = position(&dsn).await;
            let direct = scan(&dsn, &a, &b).await.unwrap();
            let in_proof = |r: &ScanReport| {
                r.statements.iter().any(|st| {
                    matches!(&st.event, EventIdentity::Gtid { gtid, .. } if *gtid == in_gap)
                })
            };
            assert!(
                in_proof(&direct),
                "the DDL in the gap is proved: {:?}",
                direct.statements
            );
            // Shared and direct agree on the gapped interval.
            let scanner = ProofScanner::default();
            let segments: Arc<dyn Segments> = Arc::new(LiveSegments {
                dsn: dsn.clone(),
                server_id: 4_000_151,
                server_uuid: id.clone(),
                lineage_hash: LINEAGE.into(),
                limits: ScanLimits::default(),
            });
            let shared = scanner
                .interval(
                    segments.clone(),
                    &id,
                    LINEAGE,
                    &a,
                    &b,
                    None,
                    Some(&a),
                    &TAG,
                )
                .await
                .unwrap();
            assert_eq!(shared.statements, direct.statements);
            assert_eq!(shared.digest, direct.digest);
            // What the defect did: an A claiming the gap is ordered before
            // B, and its scan silently misses the DDL.
            let mut over = a.clone();
            let mut claimed = a_set.clone();
            for g in n + 1..=n + 100 {
                assert!(claimed.insert(&format!("{id}:{g}")));
            }
            over.gtid_set = Some(claimed.to_string());
            let missed = scan(&dsn, &over, &b).await.unwrap();
            assert!(
                !in_proof(&missed),
                "an over-claimed A skips the gap's DDL"
            );
        }

        async fn shared_equals_direct(gtid: bool, db: &str) {
            use super::super::super::mysql_proof_scanner::{
                LiveSegments, ProofScanner, Segments,
            };
            let dsn = server(gtid).await;
            let id = uuid(&dsn).await;
            sql(
                &dsn,
                &[
                    &format!("CREATE DATABASE {db}"),
                    &format!("CREATE TABLE {db}.t (a INT PRIMARY KEY)"),
                ],
            )
            .await;
            let mut points = vec![position(&dsn).await];
            for i in 0..16 {
                let stmt = match i % 4 {
                    0 => format!("CREATE TABLE {db}.x{i} (a INT PRIMARY KEY)"),
                    1 => format!("INSERT INTO {db}.t (a) VALUES ({i})"),
                    2 => format!("ALTER TABLE {db}.t ADD COLUMN c{i} INT"),
                    _ => format!("RENAME TABLE {db}.x{} TO {db}.y{i}", i - 3),
                };
                sql(&dsn, &[&stmt]).await;
                if i == 9 {
                    sql(&dsn, &[&format!("CREATE DATABASE {db}_other")]).await;
                }
                points.push(position(&dsn).await);
            }
            let scanner = ProofScanner::default();
            let segments: Arc<dyn Segments> = Arc::new(LiveSegments {
                dsn: dsn.clone(),
                server_id: 4_000_150,
                server_uuid: id.clone(),
                lineage_hash: LINEAGE.into(),
                limits: ScanLimits::default(),
            });
            let n = points.len();
            for i in 0..n {
                for j in [i, (i + 3).min(n - 1), n - 1] {
                    let shared = scanner
                        .interval(
                            segments.clone(),
                            &id,
                            LINEAGE,
                            &points[i],
                            &points[j],
                            None,
                            Some(&points[i]),
                            &TAG,
                        )
                        .await
                        .unwrap();
                    let direct =
                        scan(&dsn, &points[i], &points[j]).await.unwrap();
                    assert_eq!(
                        shared.statements, direct.statements,
                        "({i}, {j}]"
                    );
                    assert_eq!(shared.digest, direct.digest, "({i}, {j}]");
                }
            }
            let (first, last) = (&points[0], &points[n - 1]);
            use super::super::super::mysql_proof_scanner::RecordLimits;

            // The record's bounds fail closed (and clear it).
            for limits in [
                RecordLimits {
                    max_statements: 3,
                    max_bytes: 1 << 20,
                },
                RecordLimits {
                    max_statements: 1000,
                    max_bytes: 200,
                },
            ] {
                let small = ProofScanner::new(limits);
                let r = small
                    .interval(
                        segments.clone(),
                        &id,
                        LINEAGE,
                        first,
                        last,
                        None,
                        None,
                        &TAG,
                    )
                    .await;
                assert!(matches!(r, Err(ProofError::Limit(_))), "{r:?}");
                assert_eq!(small.retained().await, None);
            }

            // A request cancelled during its extension leaves the record
            // as it was; the next request equals the direct scan.
            let fresh = ProofScanner::default();
            fresh
                .interval(
                    segments.clone(),
                    &id,
                    LINEAGE,
                    first,
                    &points[2],
                    None,
                    Some(first),
                    &TAG,
                )
                .await
                .unwrap();
            let before = fresh.retained().await;
            // Cancelled mid-extension, deterministically: polled once, it
            // holds the record and waits on the server; then it is dropped.
            // (A timeout could lose the race to fast local I/O.)
            let mut req = Box::pin(fresh.interval(
                segments.clone(),
                &id,
                LINEAGE,
                first,
                last,
                None,
                Some(first),
                &TAG,
            ));
            let first_poll = std::future::poll_fn(|cx| {
                std::task::Poll::Ready(std::future::Future::poll(
                    req.as_mut(),
                    cx,
                ))
            })
            .await;
            assert!(
                first_poll.is_pending(),
                "the request is in flight on the server"
            );
            drop(req);
            assert_eq!(fresh.retained().await, before);
            assert_eq!(fresh.pinned(), 0);
            let r = fresh
                .interval(
                    segments.clone(),
                    &id,
                    LINEAGE,
                    first,
                    last,
                    None,
                    Some(first),
                    &TAG,
                )
                .await
                .unwrap();
            assert_eq!(r.digest, scan(&dsn, first, last).await.unwrap().digest);

            // Another lineage never reuses the record: it scans again.
            const OTHER: &str = "fedcba9876543210fedcba9876543210";
            let relabel = |p: &MySqlCheckpoint| MySqlCheckpoint {
                lineage: Some(OTHER.into()),
                ..p.clone()
            };
            let other: Arc<dyn Segments> = Arc::new(LiveSegments {
                dsn: dsn.clone(),
                server_id: 4_000_152,
                server_uuid: id.clone(),
                lineage_hash: OTHER.into(),
                limits: ScanLimits::default(),
            });
            let r = fresh
                .interval(
                    other,
                    &id,
                    OTHER,
                    &relabel(first),
                    &relabel(last),
                    None,
                    None,
                    &TAG,
                )
                .await
                .unwrap();
            assert!(r.events > 0, "a new lineage rescans");
        }

        /// GTID mode: B at A's file offset but holding one more transaction
        /// (a DDL) is a real interval: the direct and the shared scans both
        /// read and prove it, with identical statements and digest.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn the_same_file_offset_with_one_more_gtid_is_scanned() {
            use super::super::super::mysql_proof_scanner::{
                LiveSegments, ProofScanner,
            };
            let dsn = server(true).await;
            let id = uuid(&dsn).await;
            sql(&dsn, &["CREATE DATABASE dual_pos"]).await;
            let a = position(&dsn).await;
            sql(&dsn, &["CREATE TABLE dual_pos.t (a INT PRIMARY KEY)"]).await;
            let real_b = position(&dsn).await;
            let b = MySqlCheckpoint {
                file: a.file.clone(),
                pos: a.pos,
                ..real_b.clone()
            };
            let direct = scan(&dsn, &a, &b).await.unwrap();
            assert_eq!(direct.statements.len(), 1, "{:?}", direct.statements);
            assert_eq!(
                direct.digest,
                scan(&dsn, &a, &real_b).await.unwrap().digest
            );
            let shared = ProofScanner::default()
                .interval(
                    Arc::new(LiveSegments {
                        dsn: dsn.clone(),
                        server_id: 4_000_153,
                        server_uuid: id.clone(),
                        lineage_hash: LINEAGE.into(),
                        limits: ScanLimits::default(),
                    }),
                    &id,
                    LINEAGE,
                    &a,
                    &b,
                    None,
                    Some(&a),
                    &TAG,
                )
                .await
                .unwrap();
            assert_eq!(shared.statements, direct.statements);
            assert_eq!(shared.digest, direct.digest);
        }

        #[tokio::test]
        #[ignore = "requires docker"]
        async fn gtid_shared_scans_equal_direct_scans() {
            shared_equals_direct(true, "shared_g").await;
        }

        #[tokio::test]
        #[ignore = "requires docker"]
        async fn file_position_shared_scans_equal_direct_scans() {
            shared_equals_direct(false, "shared_f").await;
        }

        /// A FULL-fallback capture records its proof: lock wait and schema
        /// query apart, the scan that proved it.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_proven_capture_is_traced() {
            let dsn = server(true).await;
            sql(
                &dsn,
                &[
                    "CREATE DATABASE trace_c",
                    "CREATE TABLE trace_c.t (a INT PRIMARY KEY)",
                ],
            )
            .await;
            let records = Records::default();
            let guard = records.install();
            let cap = proven(&dsn, "trace_c", 3, None).await.unwrap();
            drop(guard);
            let proofs = records.of("proof");
            assert_eq!(proofs.len(), 1, "{proofs:?}");
            let p = &proofs[0];
            assert_eq!(
                (p["kind"].as_str(), p["outcome"].as_str()),
                (Some("lazy"), Some("proven"))
            );
            assert_eq!(
                (p["db"].as_str(), p["table"].as_str()),
                (Some("trace_c"), Some("t"))
            );
            assert!(p["capture"]["lock_wait_ms"].is_number());
            assert!(p["capture"]["schema_ms"].is_number());
            assert!(cap.trace.schema > Duration::ZERO);
            let scans = records.of("scan");
            assert_eq!(p["scan_id"], scans.last().unwrap()["scan_id"]);
        }

        fn columns(c: &Captured) -> Vec<&str> {
            c.schema.columns.iter().map(|x| x.name.as_str()).collect()
        }

        fn strictly_before(a: &MySqlCheckpoint, b: &MySqlCheckpoint) -> bool {
            order_positions(&wm(a).unwrap(), &wm(b).unwrap())
                == CheckpointOrder::Before
        }

        /// Writers committing continuously, until stopped, into the
        /// captured table and into unrelated tables (same and another
        /// database).
        struct Busy {
            stop: Arc<std::sync::atomic::AtomicBool>,
            tasks: Vec<tokio::task::JoinHandle<u64>>,
        }

        impl Busy {
            async fn start(dsn: &str, db: &str) -> Self {
                sql(dsn, &[
                    &format!("CREATE TABLE {db}.busy (id BIGINT AUTO_INCREMENT PRIMARY KEY, v INT)"),
                    &format!("CREATE DATABASE IF NOT EXISTS {db}_other"),
                    &format!("CREATE TABLE IF NOT EXISTS {db}_other.busy (id BIGINT AUTO_INCREMENT PRIMARY KEY, v INT)"),
                ])
                .await;
                let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let targets = [
                    format!("INSERT INTO {db}.busy (v) VALUES (1)"),
                    format!("INSERT INTO {db}_other.busy (v) VALUES (1)"),
                    // The captured table itself (ids far above the tests').
                    format!(
                        "INSERT INTO {db}.t (id, name) SELECT COALESCE(MAX(id), 0) + 1000000, 'w' FROM {db}.t AS x"
                    ),
                ];
                let tasks = targets
                    .into_iter()
                    .map(|q| {
                        let (dsn, stop) = (dsn.to_string(), stop.clone());
                        tokio::spawn(async move {
                            let mut c =
                                mysql_async::Conn::from_url(dsn).await.unwrap();
                            let mut n = 0;
                            while !stop.load(Ordering::SeqCst) {
                                c.query_drop(&q)
                                    .await
                                    .unwrap_or_else(|e| panic!("{q}: {e}"));
                                n += 1;
                            }
                            n
                        })
                    })
                    .collect();
                // Running before the capture starts.
                tokio::time::sleep(Duration::from_millis(200)).await;
                Self { stop, tasks }
            }

            async fn finish(self) -> u64 {
                self.stop.store(true, Ordering::SeqCst);
                let mut total = 0;
                for t in self.tasks {
                    total += t.await.unwrap();
                }
                total
            }
        }

        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_quiet_server_gives_an_empty_capture_interval() {
            for gtid in [true, false] {
                let dsn = server(gtid).await;
                let db = if gtid { "cap_quiet_g" } else { "cap_quiet_f" };
                capture_table(&dsn, db).await;
                let c = proven(&dsn, db, 1, None).await.unwrap();
                assert_eq!(c.attempts, 1);
                assert_eq!(columns(&c), ["id", "name"]);
                assert_eq!(c.position.lineage.as_deref(), Some(LINEAGE));
                assert_eq!(c.position.gtid_set.is_some(), gtid);
                assert_eq!(c.from, c.position);
                let now = position(&dsn).await;
                assert_eq!(
                    (c.position.file, c.position.pos),
                    (now.file, now.pos)
                );
            }
        }

        /// The busy-server regression: continuous commits, on the captured
        /// table and on unrelated ones, never prevent a capture or its proof.
        /// The positions differ (the server never stands still) and the
        /// first attempt is accepted.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn ordinary_writes_never_prevent_a_capture() {
            for gtid in [true, false] {
                let dsn = server(gtid).await;
                let db = if gtid { "cap_busy_g" } else { "cap_busy_f" };
                capture_table(&dsn, db).await;
                let busy = Busy::start(&dsn, db).await;
                // And one commit certainly inside each capture interval.
                let ids = Arc::new(AtomicU32::new(1));
                let (d, i) = (dsn.clone(), ids.clone());
                let write: Action = Arc::new(move || {
                    let (dsn, ids) = (d.clone(), i.clone());
                    Box::pin(async move {
                        let id = ids.fetch_add(1, Ordering::SeqCst);
                        sql(
                            &dsn,
                            &[&format!(
                                "INSERT INTO {db}.t VALUES ({id}, 'x')"
                            )],
                        )
                        .await;
                    })
                });
                for _ in 0..5 {
                    let c = proven(&dsn, db, 1, Some(&write)).await.unwrap();
                    assert_eq!(c.attempts, 1);
                    assert_eq!(columns(&c), ["id", "name"]);
                    assert!(strictly_before(&c.from, &c.position), "{c:?}");
                }
                let hook = {
                    let w = write.clone();
                    move || w()
                };
                let c = capture(
                    &CaptureCtx::detached(dsn.as_str()),
                    &uuid(&dsn).await,
                    LINEAGE,
                    db,
                    "t",
                    Some(&hook),
                )
                .await
                .expect("not cancelled")
                .unwrap();
                assert!(strictly_before(&c.from, &c.position));
                assert!(busy.finish().await > 0);
            }
        }

        /// DDL of other tables (same database or another) inside the
        /// capture interval, between any two of its reads, is irrelevant.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn unrelated_ddl_does_not_invalidate_a_capture() {
            for gtid in [true, false] {
                let dsn = server(gtid).await;
                let db = if gtid { "cap_unrel_g" } else { "cap_unrel_f" };
                capture_table(&dsn, db).await;
                sql(
                    &dsn,
                    &[
                        &format!("CREATE TABLE {db}.u (id INT PRIMARY KEY)"),
                        &format!("DROP DATABASE IF EXISTS {db}_o"),
                        &format!("CREATE DATABASE {db}_o"),
                        &format!("CREATE TABLE {db}_o.t (id INT PRIMARY KEY)"),
                    ],
                )
                .await;
                let n = Arc::new(AtomicU32::new(0));
                let (d, n2) = (dsn.clone(), n.clone());
                read_hook::set(
                    db,
                    Arc::new(move |_| {
                        let (dsn, n) = (d.clone(), n2.clone());
                        Box::pin(async move {
                            let k = n.fetch_add(1, Ordering::SeqCst);
                            sql(&dsn, &[
                                &format!("ALTER TABLE {db}.u ADD COLUMN c{k} INT"),
                                &format!("ALTER TABLE {db}_o.t ADD COLUMN c{k} INT"),
                                &format!("RENAME TABLE {db}_o.t TO {db}_o.t2, {db}_o.t2 TO {db}_o.t"),
                            ])
                            .await;
                        })
                    }),
                );
                let between = sql_times(
                    &dsn,
                    vec![
                        format!("CREATE TABLE {db}.v (id INT PRIMARY KEY)"),
                        format!("ALTER DATABASE {db}_o CHARACTER SET utf8mb4"),
                    ],
                    1,
                );
                let c = proven(&dsn, db, 1, Some(&between)).await;
                read_hook::clear(db);
                let c = c.unwrap();
                assert_eq!(c.attempts, 1);
                assert_eq!(columns(&c), ["id", "name"]);
                assert_eq!(n.load(Ordering::SeqCst), 3);
            }
        }

        /// The schema read is four separate queries. A DDL of the table or
        /// its database (an ALTER, a rename swap, an ALTER DATABASE) issued between any two of them, or after
        /// the last, waits for the capture's lock: the capture reads one
        /// coherent shape (never one assembled from two versions), the DDL
        /// lands after its interval, and the next capture reads the new
        /// shape.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_ddl_between_any_two_reads_waits_for_the_capture() {
            for gtid in [true, false] {
                let dsn = server(gtid).await;
                for step in 1..=4u8 {
                    for (k, ddl, new_shape) in [
                        (
                            "alter",
                            "ALTER TABLE {db}.t ADD COLUMN c INT",
                            vec!["id", "name", "c"],
                        ),
                        (
                            "database",
                            "ALTER DATABASE {db} CHARACTER SET utf8mb4",
                            vec!["id", "name"],
                        ),
                        (
                            "swap",
                            "RENAME TABLE {db}.t TO {db}.t_tmp, {db}.t_x TO {db}.t, {db}.t_tmp TO {db}.t_x",
                            vec!["id", "other"],
                        ),
                    ] {
                        let db = format!(
                            "cap_rel_{}_{step}_{k}",
                            if gtid { "g" } else { "f" }
                        );
                        let db: &'static str = Box::leak(db.into_boxed_str());
                        capture_table(&dsn, db).await;
                        sql(&dsn, &[&format!(
                            "CREATE TABLE {db}.t_x (id INT PRIMARY KEY, other INT)"
                        )])
                        .await;
                        let ddl = ddl.replace("{db}", db);
                        let task: Arc<
                            std::sync::Mutex<
                                Option<tokio::task::JoinHandle<()>>,
                            >,
                        > = Default::default();
                        let (d, slot) = (dsn.clone(), task.clone());
                        let issue: Action = Arc::new(move || {
                            let (dsn, slot, ddl) =
                                (d.clone(), slot.clone(), ddl.clone());
                            Box::pin(async move {
                                let t = tokio::spawn(async move {
                                    sql(&dsn, &[&ddl]).await;
                                });
                                tokio::time::sleep(Duration::from_millis(300))
                                    .await;
                                assert!(!t.is_finished(), "the DDL must wait");
                                *slot.lock().unwrap() = Some(t);
                            })
                        });
                        let between = if step == 4 {
                            Some(issue)
                        } else {
                            read_hook::set(
                                db,
                                Arc::new(move |s| {
                                    let issue = issue.clone();
                                    Box::pin(async move {
                                        if s == step {
                                            issue().await;
                                        }
                                    })
                                }),
                            );
                            None
                        };
                        let r = proven(&dsn, db, 1, between.as_ref()).await;
                        read_hook::clear(db);
                        let c = r.unwrap();
                        assert_eq!(
                            columns(&c),
                            ["id", "name"],
                            "gtid={gtid} step={step} {k}"
                        );
                        let t =
                            task.lock().unwrap().take().expect("DDL issued");
                        tokio::time::timeout(Duration::from_secs(10), t)
                            .await
                            .expect("the DDL proceeds after the capture")
                            .unwrap();
                        assert!(strictly_before(
                            &c.position,
                            &position(&dsn).await
                        ));
                        let after = proven(&dsn, db, 1, None).await.unwrap();
                        assert_eq!(
                            columns(&after),
                            new_shape,
                            "gtid={gtid} step={step} {k}"
                        );
                    }
                }
            }
        }

        /// Lineage barriers (unattributable statements) that are not DDL of
        /// the table or its database are not held off by the lock: one inside
        /// the interval refuses the attempt.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn barriers_in_the_interval_are_detected() {
            for gtid in [true, false] {
                let dsn = server(gtid).await;
                let db = if gtid { "cap_bar_g" } else { "cap_bar_f" };
                for stmts in [
                    // A multi-statement body cannot be attributed.
                    vec![
                        format!("DROP PROCEDURE IF EXISTS {db}.p"),
                        format!(
                            "CREATE PROCEDURE {db}.p() BEGIN SELECT 1; SELECT 2; END"
                        ),
                    ],
                    // A possible ANSI_QUOTES identifier cannot be
                    // interpreted.
                    vec![
                        format!("DROP TABLE IF EXISTS {db}.w"),
                        format!("CREATE TABLE {db}.w (id INT) COMMENT \"w\""),
                    ],
                ] {
                    capture_table(&dsn, db).await;
                    let between = sql_times(&dsn, stmts.clone(), u32::MAX);
                    let r = proven(&dsn, db, 2, Some(&between)).await;
                    assert!(
                        matches!(r, Err(ProofError::Unstable(2))),
                        "gtid={gtid} {stmts:?}: {r:?}"
                    );
                }
            }
        }

        /// Whatever the scan of the capture interval cannot prove fails
        /// closed, never retried into a pass: purged binlogs, a resource
        /// limit, another server, a missing table.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn an_unprovable_capture_interval_fails_closed() {
            // Own server: purging must not disturb the other tests.
            let (_c, port) = start(true).await;
            let dsn = dsn_of(port);
            capture_table(&dsn, "cap_fail").await;
            let purge = {
                let d = dsn.clone();
                Arc::new(move || {
                    let dsn = d.clone();
                    Box::pin(async move {
                        sql(
                            &dsn,
                            &[
                                "INSERT INTO cap_fail.t VALUES (1, 'x')",
                                "FLUSH BINARY LOGS",
                            ],
                        )
                        .await;
                        let now = position(&dsn).await;
                        sql(
                            &dsn,
                            &[&format!("PURGE BINARY LOGS TO '{}'", now.file)],
                        )
                        .await;
                    })
                        as std::pin::Pin<
                            Box<dyn std::future::Future<Output = ()> + Send>,
                        >
                }) as Action
            };
            let r = proven(&dsn, "cap_fail", 3, Some(&purge)).await;
            assert!(
                matches!(
                    r,
                    Err(ProofError::Open(_)) | Err(ProofError::Read(_))
                ),
                "{r:?}"
            );

            let write = sql_times(
                &dsn,
                vec!["INSERT INTO cap_fail.t VALUES (2, 'y'), (3, 'z')".into()],
                u32::MAX,
            );
            let hook = {
                let w = write.clone();
                move || w()
            };
            let r = capture_proven(
                &CaptureCtx::detached(dsn.as_str()),
                4_000_141,
                &uuid(&dsn).await,
                LINEAGE,
                "cap_fail",
                "t",
                0,
                3,
                &ScanLimits {
                    max_events: 1,
                    ..limits()
                },
                Some(&hook),
                &TAG,
                None,
            )
            .await
            .expect("not cancelled");
            assert!(matches!(r, Err(ProofError::Limit(_))), "{r:?}");

            let r = capture(
                &CaptureCtx::detached(dsn.as_str()),
                "00000000-1111-2222-3333-444444444444",
                LINEAGE,
                "cap_fail",
                "t",
                None,
            )
            .await
            .expect("not cancelled");
            assert!(matches!(r, Err(ProofError::OtherServer(_))), "{r:?}");
            let r = capture(
                &CaptureCtx::detached(dsn.as_str()),
                &uuid(&dsn).await,
                LINEAGE,
                "cap_fail",
                "missing",
                None,
            )
            .await
            .expect("not cancelled");
            assert!(matches!(r, Err(ProofError::Capture(_))), "{r:?}");
        }

        async fn live_columns(dsn: &str, db: &str) -> u64 {
            let mut c = mysql_async::Conn::from_url(dsn).await.unwrap();
            let n: u64 = c
                .query_first(format!(
                    "SELECT COUNT(*) FROM information_schema.COLUMNS \
                     WHERE TABLE_SCHEMA = '{db}' AND TABLE_NAME = 't'"
                ))
                .await
                .unwrap()
                .unwrap();
            c.disconnect().await.ok();
            n
        }

        /// While the capture runs, its connection holds the table's
        /// SHARED_READ metadata lock for the transaction: inserts and
        /// updates of the table proceed, a DDL of the table waits until the
        /// capture ends and lands after its interval.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn the_capture_lock_admits_writes_and_holds_off_ddl() {
            for gtid in [true, false] {
                let dsn = server(gtid).await;
                let db = if gtid { "cap_lock_g" } else { "cap_lock_f" };
                capture_table(&dsn, db).await;
                sql(&dsn, &[&format!("INSERT INTO {db}.t VALUES (1, 'a')")])
                    .await;
                let alter: Arc<
                    std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
                > = Default::default();
                let (d, slot) = (dsn.clone(), alter.clone());
                let hook = move || {
                    let (dsn, slot) = (d.clone(), slot.clone());
                    Box::pin(async move {
                        let mut c =
                            mysql_async::Conn::from_url(&dsn).await.unwrap();
                        let held: Option<String> = c
                            .query_first(format!(
                                "SELECT CONCAT(LOCK_TYPE, '/', LOCK_DURATION, '/', LOCK_STATUS) \
                                 FROM performance_schema.metadata_locks \
                                 WHERE OBJECT_TYPE = 'TABLE' AND OBJECT_SCHEMA = '{db}' \
                                 AND OBJECT_NAME = 't'"
                            ))
                            .await
                            .unwrap();
                        assert_eq!(
                            held.as_deref(),
                            Some("SHARED_READ/TRANSACTION/GRANTED")
                        );
                        // Writes to the table are not held off.
                        tokio::time::timeout(Duration::from_secs(5), async {
                            c.query_drop(format!(
                                "INSERT INTO {db}.t VALUES (2, 'b')"
                            ))
                            .await
                            .unwrap();
                            c.query_drop(format!(
                                "UPDATE {db}.t SET name = 'u' WHERE id = 1"
                            ))
                            .await
                            .unwrap();
                        })
                        .await
                        .expect("DML proceeds under the capture lock");
                        c.disconnect().await.ok();
                        // A DDL of the table waits for the lock.
                        let task = tokio::spawn(async move {
                            sql(
                                &dsn,
                                &[&format!(
                                    "ALTER TABLE {db}.t ADD COLUMN c INT"
                                )],
                            )
                            .await;
                        });
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        assert!(!task.is_finished(), "the DDL must wait");
                        *slot.lock().unwrap() = Some(task);
                    })
                        as std::pin::Pin<
                            Box<dyn std::future::Future<Output = ()> + Send>,
                        >
                };
                let c = capture(
                    &CaptureCtx::detached(dsn.as_str()),
                    &uuid(&dsn).await,
                    LINEAGE,
                    db,
                    "t",
                    Some(&hook),
                )
                .await
                .expect("not cancelled")
                .unwrap();
                assert_eq!(columns(&c), ["id", "name"]);
                let task = alter.lock().unwrap().take().unwrap();
                tokio::time::timeout(Duration::from_secs(10), task)
                    .await
                    .expect("the DDL proceeds once the capture ends")
                    .unwrap();
                assert!(strictly_before(&c.position, &position(&dsn).await));
                // The DDL is after the interval: the capture's own proof holds.
                let r = scan(&dsn, &c.from, &c.position).await.unwrap();
                assert!(affected(&r).iter().all(|a| a != &format!("{db}.t")));
            }
        }

        /// The adversarial window: a DDL of the table whose binlog event is
        /// already written (the binlog position includes it) while its
        /// dictionary change is not yet visible (the group commit holds it
        /// before the engine commit). A capture then must not read the prior
        /// shape as if valid at that position: it waits for the DDL and reads
        /// the new shape.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_ddl_in_the_binlog_but_not_yet_visible_is_never_captured_stale()
         {
            for gtid in [false, true] {
                // Own server: the commit delay is global.
                let (_c, port) = start(gtid).await;
                let dsn = dsn_of(port);
                let db = "cap_window";
                capture_table(&dsn, db).await;
                let before = position(&dsn).await;
                sql(
                    &dsn,
                    &["SET GLOBAL binlog_group_commit_sync_delay = 1000000"],
                )
                .await;
                let d = dsn.clone();
                let alter = tokio::spawn(async move {
                    sql(&d, &[&format!("ALTER TABLE {db}.t ADD COLUMN c INT")])
                        .await;
                });
                // The window: the binlog position already includes the DDL,
                // the catalog still shows the prior shape.
                let deadline =
                    std::time::Instant::now() + Duration::from_millis(800);
                let mut seen = false;
                while std::time::Instant::now() < deadline {
                    let now = position(&dsn).await;
                    if (now.file.as_str(), now.pos)
                        != (before.file.as_str(), before.pos)
                        && live_columns(&dsn, db).await == 2
                    {
                        seen = true;
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                assert!(seen, "gtid={gtid}: the window was not reproduced");
                let c = proven(&dsn, db, 3, None).await;
                sql(&dsn, &["SET GLOBAL binlog_group_commit_sync_delay = 0"])
                    .await;
                alter.await.unwrap();
                let c = c.unwrap();
                assert_eq!(columns(&c), ["id", "name", "c"], "gtid={gtid}");
            }
        }

        /// A covering scan must contain the whole capture interval.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn covers_requires_the_whole_capture_interval() {
            let dsn = server(true).await;
            capture_table(&dsn, "cap_cover").await;
            let before = wm(&position(&dsn).await).unwrap();
            let write = sql_times(
                &dsn,
                vec!["INSERT INTO cap_cover.t VALUES (1, 'x')".into()],
                1,
            );
            let hook = {
                let w = write.clone();
                move || w()
            };
            let c = capture(
                &CaptureCtx::detached(dsn.as_str()),
                &uuid(&dsn).await,
                LINEAGE,
                "cap_cover",
                "t",
                Some(&hook),
            )
            .await
            .expect("not cancelled")
            .unwrap();
            let (a, b) = (wm(&c.from).unwrap(), wm(&c.position).unwrap());
            assert!(covers(&c, &before, &b));
            assert!(covers(&c, &a, &b));
            // Starting inside the interval, or ending before its end.
            assert!(!covers(&c, &b, &b));
            assert!(!covers(&c, &before, &a));
        }

        /// The control-plane connection bounds (design section 5): every
        /// count is of one dedicated account's sessions, so nothing else on
        /// the server is counted.
        mod control_connections {
            use super::*;

            /// The per-source pool's default size (`P`).
            const P: usize = 4;

            /// A dedicated account on `dsn`'s server, and a DSN for it.
            async fn account(dsn: &str, user: &str) -> String {
                sql(
                    dsn,
                    &[
                        &format!("CREATE USER '{user}'@'%' IDENTIFIED BY 'pw'"),
                        &format!("GRANT ALL ON *.* TO '{user}'@'%'"),
                    ],
                )
                .await;
                dsn.replacen("root:pw@", &format!("{user}:pw@"), 1)
            }

            /// `user`'s open sessions.
            async fn sessions(dsn: &str, user: &str) -> usize {
                let mut c = mysql_async::Conn::from_url(dsn).await.unwrap();
                let n: Option<u64> = c
                    .exec_first(
                        "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE USER = ?",
                        (user,),
                    )
                    .await
                    .unwrap();
                c.disconnect().await.ok();
                n.unwrap_or(0) as usize
            }

            /// Samples `user`'s sessions until stopped; returns the peak.
            fn peak(
                dsn: &str,
                user: &'static str,
            ) -> (Arc<AtomicU32>, tokio::task::JoinHandle<usize>) {
                let stop = Arc::new(AtomicU32::new(0));
                let (s, d) = (stop.clone(), dsn.to_string());
                let task = tokio::spawn(async move {
                    let mut most = 0;
                    while s.load(Ordering::SeqCst) == 0 {
                        most = most.max(sessions(&d, user).await);
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                    most
                });
                (stop, task)
            }

            /// Captures queued for a lock-holding slot (single and batched)
            /// stay within the source's pool: one source never holds more
            /// than `P` control connections, however many captures wait.
            #[tokio::test]
            #[ignore = "requires docker"]
            async fn queued_captures_stay_within_the_sources_pool() {
                let root = server(true).await;
                let db = "control_queue";
                sql(
                    &root,
                    &[
                        &format!("CREATE DATABASE {db}"),
                        &format!("CREATE TABLE {db}.t (id INT PRIMARY KEY)"),
                    ],
                )
                .await;
                let dsn = account(&root, "queue_u").await;
                let u = uuid(&root).await;
                let slots: &'static CaptureSlots =
                    Box::leak(Box::new(CaptureSlots::new(1, 1)));
                // The only lock-holding slot is taken: every capture queues.
                let held = slots.holding.acquire().await.unwrap();
                let cx = CaptureCtx::detached(dsn.as_str());
                let tasks: Vec<_> = (0..2 * P)
                    .map(|i| {
                        positioned::cancellable(
                            slots,
                            cx.clone(),
                            &u,
                            db,
                            i % 2 == 1,
                            None,
                        )
                    })
                    .collect();
                let (stop, sampler) = peak(&root, "queue_u");
                tokio::time::sleep(Duration::from_secs(3)).await;
                drop(held);
                for t in tasks {
                    let r = t.await.unwrap().expect("not cancelled");
                    assert!(r.is_ok(), "{r:?}");
                }
                stop.store(1, Ordering::SeqCst);
                let most = sampler.await.unwrap();
                assert!(
                    most <= P,
                    "{} queued captures held {most} connections, above P = {P}",
                    2 * P
                );
            }

            /// Concurrent schema reads stay within the source's pool, and
            /// once they finish at most one idle connection is retained.
            #[tokio::test]
            #[ignore = "requires docker"]
            async fn schema_reads_stay_within_the_pool_and_retain_one_idle() {
                let root = server(true).await;
                let db = "control_reads";
                let n = 4 * P;
                let mut ddl = vec![format!("CREATE DATABASE {db}")];
                ddl.extend((0..n).map(|i| {
                    format!("CREATE TABLE {db}.t{i} (id INT PRIMARY KEY)")
                }));
                let ddl: Vec<&str> = ddl.iter().map(String::as_str).collect();
                sql(&root, &ddl).await;
                let dsn = account(&root, "reads_u").await;
                let scope = crate::registry_scope::SharedRegistryScope::new(
                    "control_reads",
                );
                scope.publish_for_test(
                    "acme",
                    storage::adapters::LineageDescriptor::mysql(
                        &uuid(&root).await,
                    )
                    .unwrap(),
                );
                let loader = crate::mysql::MySqlSchemaLoader::new(
                    dsn.as_str(),
                    storage::DurableSchemaRegistry::for_testing(),
                    "acme",
                    scope,
                );
                let (stop, sampler) = peak(&root, "reads_u");
                let reads: Vec<_> = (0..n)
                    .map(|i| {
                        let l = loader.clone();
                        tokio::spawn(async move {
                            l.load_schema(db, &format!("t{i}")).await
                        })
                    })
                    .collect();
                for r in reads {
                    r.await.unwrap().expect("schema read");
                }
                stop.store(1, Ordering::SeqCst);
                let most = sampler.await.unwrap();
                assert!(
                    most <= P,
                    "{n} concurrent schema reads held {most} connections, above P = {P}"
                );
                let idle = sessions(&root, "reads_u").await;
                assert!(idle <= 1, "{idle} idle connections retained");
            }

            /// A pool of its own on `budget` (`P` = 2).
            fn pool_on(
                budget: &'static crate::mysql::mysql_control::Budget,
                name: &str,
            ) -> crate::mysql::mysql_control::ControlPool {
                crate::mysql::mysql_control::ControlPool::detached(
                    budget, name, 2,
                )
            }

            /// Cancellation never waits behind ordinary work: with every
            /// ordinary permit of the runner held by `L` captures queued
            /// for their tables' metadata locks (single and batched), a stop
            /// kills each of them through the reserve, at once, with no
            /// failed kill.
            #[tokio::test]
            #[ignore = "requires docker"]
            async fn cancellation_draws_on_its_reserve_with_the_budget_full() {
                let root = server(true).await;
                let db = "control_cancel";
                sql(
                    &root,
                    &[
                        &format!("CREATE DATABASE {db}"),
                        &format!("CREATE TABLE {db}.t (id INT PRIMARY KEY)"),
                    ],
                )
                .await;
                let mut holder =
                    mysql_async::Conn::from_url(&root).await.unwrap();
                holder
                    .query_drop(format!("LOCK TABLES {db}.t WRITE"))
                    .await
                    .unwrap();
                const L: usize = 2;
                // G = R + L: the L captures take every ordinary permit.
                let budget: &'static crate::mysql::mysql_control::Budget =
                    Box::leak(Box::new(
                        crate::mysql::mysql_control::Budget::new(
                            KILL_RESERVE + L,
                        ),
                    ));
                let slots: &'static CaptureSlots =
                    Box::leak(Box::new(CaptureSlots::new(L, L)));
                let cx = CaptureCtx::detached_on(
                    root.as_str(),
                    pool_on(budget, "cancel"),
                );
                let u = uuid(&root).await;
                let failures = kill_failures();
                let tasks: Vec<_> = (0..L)
                    .map(|i| {
                        positioned::cancellable(
                            slots,
                            cx.clone(),
                            &u,
                            db,
                            i % 2 == 1,
                            None,
                        )
                    })
                    .collect();
                let deadline = Instant::now() + Duration::from_secs(20);
                while positioned_waiting(&root, db).await < L {
                    assert!(
                        Instant::now() < deadline,
                        "the captures never queued"
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                assert_eq!(
                    budget.available(),
                    0,
                    "every ordinary permit is held"
                );
                let t = Instant::now();
                cx.stop();
                for task in tasks {
                    let r = tokio::time::timeout(Duration::from_secs(2), task)
                        .await
                        .expect("a cancelled capture returns at once")
                        .unwrap();
                    assert!(matches!(r, Err(Cancelled)), "{r:?}");
                }
                let deadline = Instant::now() + Duration::from_secs(5);
                while positioned_waiting(&root, db).await > 0 {
                    assert!(
                        Instant::now() < deadline,
                        "a killed session still waits"
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                assert!(
                    t.elapsed() < Duration::from_secs(5),
                    "{:?}",
                    t.elapsed()
                );
                assert_eq!(kill_failures(), failures, "a kill failed");
                assert_eq!(slots.retired(), 0);
                holder.query_drop("UNLOCK TABLES").await.unwrap();
            }

            /// Sessions queued for `db.t`'s metadata lock.
            async fn positioned_waiting(dsn: &str, db: &str) -> usize {
                let mut c = mysql_async::Conn::from_url(dsn).await.unwrap();
                let n: Option<u64> = c
                    .exec_first(
                        "SELECT COUNT(*) FROM information_schema.PROCESSLIST \
                         WHERE STATE = 'Waiting for table metadata lock' AND DB IS NULL \
                         AND INFO LIKE ?",
                        (format!("%`{db}`.`t`%"),),
                    )
                    .await
                    .unwrap();
                c.disconnect().await.ok();
                n.unwrap_or(0) as usize
            }

            /// A capture that gets no control-plane connection within its
            /// wait is `Saturated`, naming the bound that stayed full, and
            /// becomes the `mysql_control_plane_saturated` incident; neither
            /// carries the DSN or its credentials.
            #[tokio::test]
            #[ignore = "requires docker"]
            async fn a_capture_without_a_connection_is_saturated() {
                use crate::mysql::mysql_control::Class;
                let root = server(true).await;
                let db = "control_saturated";
                sql(
                    &root,
                    &[
                        &format!("CREATE DATABASE {db}"),
                        &format!("CREATE TABLE {db}.t (id INT PRIMARY KEY)"),
                    ],
                )
                .await;
                let dsn = account(&root, "sat_u").await;
                let u = uuid(&root).await;
                // G = R + 1: another source holds the only ordinary permit.
                let budget: &'static crate::mysql::mysql_control::Budget =
                    Box::leak(Box::new(
                        crate::mysql::mysql_control::Budget::new(
                            KILL_RESERVE + 1,
                        ),
                    ));
                let other = pool_on(budget, "other");
                let held = other
                    .connect(Class::Catalog, &dsn, &u, Duration::from_secs(5))
                    .await
                    .unwrap();
                let cx = CaptureCtx::detached_on(
                    dsn.as_str(),
                    pool_on(budget, "starved"),
                );
                let tables = vec![(db.to_string(), "t".to_string())];
                let t = Instant::now();
                let r = capture_batch(&cx, &u, LINEAGE, &tables, 1, None)
                    .await
                    .expect("not cancelled");
                let Err(BatchCaptureError::Failed(
                    e @ ProofError::Saturated { scope, .. },
                )) = r
                else {
                    panic!("{r:?}");
                };
                assert_eq!(scope, "runner");
                assert!(
                    t.elapsed() >= Duration::from_secs(1),
                    "{:?}",
                    t.elapsed()
                );
                let at = position(&root).await;
                let err = crate::incident_drafts::mysql_control_plane_saturated(
                    "src",
                    "activation window",
                    &tables,
                    scope,
                    &at,
                );
                let deltaforge_core::SourceError::Incident { draft, .. } = &err
                else {
                    panic!("{err:?}");
                };
                assert_eq!(
                    draft.reason_code,
                    deltaforge_core::incident::ReasonCode::MysqlControlPlaneSaturated
                );
                for text in
                    [e.to_string(), format!("{err}"), format!("{err:?}")]
                {
                    assert!(
                        !text.contains("sat_u") && !text.contains(":pw@"),
                        "{text}"
                    );
                }
                held.release().await;
                // With the permit back, the capture proceeds.
                let ok = capture_batch(&cx, &u, LINEAGE, &tables, 5, None)
                    .await
                    .expect("not cancelled");
                assert!(ok.is_ok(), "{ok:?}");
            }
        }
    }
}
