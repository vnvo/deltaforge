//! The speculative window (`docs/design/mysql-proof-seek.md`, revision 5).
//!
//! While a window (the current one) is captured, scanned, decided and
//! committed, the stream is read and its events are held. The next window
//! is collected from those held events, and once it closes, its batched
//! capture and its shared scan run in an owned task, overlapping the
//! current window's admission. Nothing else is speculative: the next
//! window decides only when the stream reaches its head event, after the
//! current window has committed and its rows have been dispatched in order,
//! and it then recomputes its required tables with the unchanged functions
//! and must find each of them captured, at the same start, here.
//!
//! The collector holds no part of `RunCtx`: its candidate filter reads a
//! snapshot of the cache's proven flags taken when the current admission
//! starts, and the tables the current window reserved. Its task writes
//! nothing and changes no cache; it is aborted (its capture connection
//! dropped, so the server ends its read-only transaction)
//! whenever the window is abandoned or the run stops.
//!
//! Up to `activation_capture_concurrency` windows are collected ahead, in
//! order (a [`Chain`]): each one after the last, reserving the tables of
//! the current window and of every window before it. Each row of a
//! reserved table is tagged with the window that reserved it and checked
//! against that window's decisions when it commits ([`first_unheld`]);
//! the first window that cannot hold such a row is abandoned with every
//! window after it.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::AllowList;
use deltaforge_core::{CheckpointOrder, SourceError};
use mysql_binlog_connector_rust::event::event_data::EventData;
use mysql_binlog_connector_rust::event::event_header::EventHeader;

use super::MySqlCheckpoint;
use super::mysql_activation::EventIdentity;
use super::mysql_activation_window::{
    Ahead, CaptureScan, CapturedWindow, capture_and_scan,
    is_transaction_control, touched, window_full,
};
use super::mysql_window_budget::{Caps, Fill};
use crate::durable_checkpoint::{mysql_checkpoint_position, order_positions};
use crate::gtid_set::GtidSet;
use crate::registry_scope::RegistryScope;

/// A held event: when it was received, its held sequence number (0: not
/// held), its header and data.
pub(crate) type Held = (Instant, u64, EventHeader, EventData);

/// A window's number within a run: which window reserved a table.
pub(crate) type WindowId = u64;

/// A row of a reserved table in a window's events: the window that
/// reserved the table, the table, and its transaction's start.
pub(crate) type ReservedRow = (WindowId, (String, String), MySqlCheckpoint);

/// What one decision covers of its table's other held rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Covers {
    /// The rows of the decided transaction only (a position-specific
    /// outcome: `no_binding`, `not_decisive`).
    Transaction,
    /// The rows before this statement, which affects the table (a stable
    /// exclusion: `relevant_ddl`, or no capture with the dropping
    /// statement in the proof answer).
    UntilStatement(EventIdentity),
}

/// How a window's decision of a table governs its other held rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Governs {
    /// Admitted: its later rows select over the committed timeline (or
    /// follow the path of a statement that changes the table).
    Admitted,
    /// Refused, or not captured: the rows it covers go on without a lazy
    /// baseline, never captured on their own.
    Refused(Covers),
    /// Cut from the window's prefix by its limits (`mysql_window_budget`):
    /// nothing of it was decided or recorded, so it covers none of its rows;
    /// they go back to the stream for a later window.
    Deferred,
}

/// Whether `statement` precedes the transaction starting at `boundary`
/// (it is executed in the boundary's set, or ends at or before its
/// coordinate). `None` when that cannot be known.
pub(crate) fn statement_before(
    statement: &EventIdentity,
    boundary: &MySqlCheckpoint,
) -> Option<bool> {
    match statement {
        EventIdentity::Gtid { gtid, .. } => {
            let set = GtidSet::parse(boundary.gtid_set.as_deref()?)?;
            Some(set.contains(gtid))
        }
        EventIdentity::FilePos { file, end_pos, .. } => {
            let at = mysql_checkpoint_position(file, *end_pos, None)?;
            let b =
                mysql_checkpoint_position(&boundary.file, boundary.pos, None)?;
            match order_positions(&at, &b) {
                CheckpointOrder::Before | CheckpointOrder::Equal => Some(true),
                CheckpointOrder::After => Some(false),
                _ => None,
            }
        }
        EventIdentity::StreamStart { .. } => None,
    }
}

fn same_boundary(a: &MySqlCheckpoint, b: &MySqlCheckpoint) -> bool {
    (&a.file, a.pos, &a.gtid_set) == (&b.file, b.pos, &b.gtid_set)
}

/// Whether a decision at `decided` covers a row of its table whose
/// transaction starts at `row`. Anything unknowable is not covered.
pub(crate) fn covers_row(
    governs: &Governs,
    decided: &MySqlCheckpoint,
    row: &MySqlCheckpoint,
) -> bool {
    match governs {
        Governs::Admitted => true,
        Governs::Refused(Covers::Transaction) => same_boundary(decided, row),
        Governs::Refused(Covers::UntilStatement(d)) => {
            statement_before(d, row) == Some(false)
        }
        Governs::Deferred => false,
    }
}

/// The first statement of `statements` that `affects` the table.
pub(crate) fn first_affecting(
    statements: &[super::mysql_binlog_scan::Statement],
    affects: impl Fn(&super::mysql_binlog_scan::Statement) -> bool,
) -> Option<&EventIdentity> {
    statements.iter().find(|s| affects(s)).map(|s| &s.event)
}

/// The decision-time rule: every required table must have been captured
/// by the speculation, at exactly the same start. Unused candidates are
/// discarded. Any difference is an invariant violation (never repaired by
/// a fresh capture).
pub(crate) fn check_required(
    required: &BTreeMap<(String, String), MySqlCheckpoint>,
    speculated: &BTreeMap<(String, String), MySqlCheckpoint>,
) -> Result<(), String> {
    for ((db, table), a) in required {
        match speculated.get(&(db.clone(), table.clone())) {
            None => {
                return Err(format!(
                    "required table {db}.{table} was not captured by the speculation"
                ));
            }
            Some(s) if !same_boundary(s, a) => {
                return Err(format!(
                    "required table {db}.{table} starts at {a:?}, the speculation \
                     captured it from {s:?}"
                ));
            }
            Some(_) => {}
        }
    }
    Ok(())
}

/// After window `id` committed with `decisions`: the index of the first
/// queued window (in order) holding a row of one of `id`'s tables that its
/// decision does not cover. That window and every later one are abandoned
/// (each later one was collected assuming the earlier ones decide as
/// speculated). `None`: every queued window holds.
pub(crate) fn first_unheld<'a>(
    queue: impl IntoIterator<Item = &'a [ReservedRow]>,
    id: WindowId,
    decisions: &HashMap<(String, String), (MySqlCheckpoint, Governs)>,
) -> Option<usize> {
    queue.into_iter().position(|rows| {
        rows.iter().any(|(owner, k, at)| {
            *owner == id
                && !decisions.get(k).is_some_and(|(decided, governs)| {
                    covers_row(governs, decided, at)
                })
        })
    })
}

/// How long a speculative capture task kept its sessions busy: until it
/// finished, or so far.
#[derive(Clone)]
pub(crate) struct Busy {
    started: Instant,
    done_us: Arc<std::sync::atomic::AtomicU64>,
}

impl Busy {
    fn start() -> Self {
        Self {
            started: Instant::now(),
            done_us: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    fn finish(&self) {
        let us = self.started.elapsed().as_micros().max(1);
        self.done_us.store(
            u64::try_from(us).unwrap_or(u64::MAX),
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    pub(crate) fn elapsed(&self) -> Duration {
        match self.done_us.load(std::sync::atomic::Ordering::Relaxed) {
            0 => self.started.elapsed(),
            us => Duration::from_micros(us),
        }
    }
}

/// A task aborted when dropped (a speculative capture never outlives its
/// window).
pub(crate) struct AbortOnDrop<T>(pub tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// What a speculative capture task needs, owned.
#[derive(Clone)]
pub(crate) struct SpecInputs {
    pub capture: CaptureScan,
    pub lineage: String,
}

/// A speculative window that closed during the current admission and kept
/// its capture task: held until the stream reaches its head.
pub(crate) struct Pending {
    pub id: WindowId,
    pub head_seq: u64,
    pub end_seq: u64,
    pub closed_by: &'static str,
    pub lineage: String,
    pub batch: BTreeMap<(String, String), MySqlCheckpoint>,
    /// The batch's tables in first-touch order: the capture's rank order.
    pub order: Vec<(String, String)>,
    pub task: AbortOnDrop<Result<CapturedWindow, SourceError>>,
    pub busy: Busy,
    /// Rows of tables reserved by earlier windows, in this window's events.
    pub reserved_rows: Vec<ReservedRow>,
    /// The tracker's state after its last event, and whether a window may
    /// be collected after it (not after a statement or a heartbeat, which
    /// may change what is proven, nor after broken framing).
    pub ahead: Ahead,
    pub chainable: bool,
}

impl std::fmt::Debug for Pending {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pending")
            .field("id", &self.id)
            .field("head_seq", &self.head_seq)
            .field("end_seq", &self.end_seq)
            .field("tables", &self.batch.len())
            .finish()
    }
}

/// What became of the next window during an admission (the trace).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum NextWindow {
    /// No candidate was held.
    #[default]
    None,
    /// It closed and its capture ran during the admission.
    Formed,
    /// It had not closed when the admission ended: dropped, its events
    /// stay held for an ordinary window.
    Unclosed,
    /// A statement came before its head: nothing speculated.
    GaveUp,
    /// It formed, then the admission's decisions left a held row of a
    /// reserved table uncovered (or cut the queue): discarded unused.
    Abandoned,
    /// As many windows as allowed were already collected ahead, or the
    /// last of them ended where nothing may follow it: none collected.
    Full,
}

impl NextWindow {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            NextWindow::None => "none",
            NextWindow::Formed => "formed",
            NextWindow::Unclosed => "unclosed",
            NextWindow::GaveUp => "gave_up",
            NextWindow::Abandoned => "abandoned",
            NextWindow::Full => "full",
        }
    }
}

/// The speculative window's collection over held events, in order.
pub(crate) struct Collector {
    id: WindowId,
    /// Events up to this held sequence number belong to windows before it.
    start_after: u64,
    ahead: Ahead,
    allow: AllowList,
    scope: Arc<RegistryScope>,
    proven: super::mysql_proven_index::ProvenSnapshot,
    /// The tables reserved by the windows before it, each with its window.
    reserved: HashMap<(String, String), WindowId>,
    /// The candidates taken, at their costs before capture.
    fill: Fill,
    bound: Duration,
    max_events: usize,
    max_bytes: u64,
    head: Option<(u64, Instant, MySqlCheckpoint)>,
    head_anchor: Option<super::mysql_binlog_scan::Anchor>,
    batch: BTreeMap<(String, String), MySqlCheckpoint>,
    /// The batch's tables in first-touch order.
    order: Vec<(String, String)>,
    events: usize,
    bytes: u64,
    end_seq: u64,
    closed: Option<&'static str>,
    gave_up: bool,
    /// Rows of reserved tables seen in the held events (by the time this
    /// window closed): checked against their windows' decisions.
    reserved_rows: Vec<ReservedRow>,
    inputs: SpecInputs,
    task: Option<(AbortOnDrop<Result<CapturedWindow, SourceError>>, Busy)>,
}

impl Collector {
    /// The window `id`, collected from the held events after `start_after`
    /// (with the tracker state `ahead` there).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        id: WindowId,
        start_after: u64,
        ahead: Ahead,
        allow: AllowList,
        scope: Arc<RegistryScope>,
        proven: super::mysql_proven_index::ProvenSnapshot,
        reserved: HashMap<(String, String), WindowId>,
        fill: Fill,
        cfg: &deltaforge_config::MysqlProofCfg,
        inputs: SpecInputs,
    ) -> Self {
        Self {
            id,
            start_after,
            ahead,
            allow,
            scope,
            proven,
            reserved,
            fill,
            bound: Duration::from_millis(cfg.activation_window_ms),
            max_events: cfg.activation_window_events,
            max_bytes: cfg.activation_window_bytes,
            head: None,
            head_anchor: None,
            batch: BTreeMap::new(),
            order: Vec::new(),
            events: 0,
            bytes: 0,
            end_seq: start_after,
            closed: None,
            gave_up: false,
            reserved_rows: Vec::new(),
            inputs,
            task: None,
        }
    }

    /// The window after this closed one: from its last event, reserving
    /// its tables too.
    fn successor(&self, id: WindowId) -> Self {
        let mut reserved = self.reserved.clone();
        for k in &self.order {
            reserved.insert(k.clone(), self.id);
        }
        Self {
            id,
            start_after: self.end_seq,
            ahead: self.ahead.clone(),
            allow: self.allow.clone(),
            scope: Arc::clone(&self.scope),
            proven: self.proven.clone(),
            reserved,
            fill: Fill::new(Caps::current(), self.fill.other_lineages()),
            bound: self.bound,
            max_events: self.max_events,
            max_bytes: self.max_bytes,
            head: None,
            head_anchor: None,
            batch: BTreeMap::new(),
            order: Vec::new(),
            events: 0,
            bytes: 0,
            end_seq: self.end_seq,
            closed: None,
            gave_up: false,
            reserved_rows: Vec::new(),
            inputs: self.inputs.clone(),
            task: None,
        }
    }

    /// Whether a window may be collected after this closed one: not after
    /// a statement or a heartbeat (what is proven may change), nor after
    /// broken framing.
    fn chainable(&self) -> bool {
        matches!(
            self.closed,
            Some("time" | "events" | "bytes" | "tables" | "budget")
        )
    }

    fn done(&self) -> bool {
        self.closed.is_some() || self.gave_up
    }

    /// When the open window's time bound passes (none before its head).
    pub(crate) fn deadline(&self) -> Option<Instant> {
        match (self.done(), &self.head) {
            (false, Some((_, received, _))) => Some(*received + self.bound),
            _ => None,
        }
    }

    /// The time bound passed with nothing more received within it.
    pub(crate) fn time_up(&mut self) {
        if !self.done() && self.head.is_some() {
            self.close("time");
        }
    }

    /// One held event, in order.
    pub(crate) fn observe(
        &mut self,
        seq: u64,
        received: Instant,
        h: &EventHeader,
        d: &EventData,
    ) {
        if self.done() || seq <= self.start_after {
            return;
        }
        if let Some((_, head_at, _)) = &self.head
            && received > *head_at + self.bound
        {
            self.close("time");
            return;
        }
        let close = match d {
            EventData::HeartBeat => Some("heartbeat"),
            EventData::Query(q) if !is_transaction_control(&q.query) => {
                Some("statement")
            }
            EventData::Rotate(r) => {
                self.ahead.file = r.binlog_filename.clone();
                None
            }
            _ => None,
        };
        if self.head.is_some() {
            self.events += 1;
            self.bytes += u64::from(h.event_length);
        }
        if let Some(why) = close {
            if self.head.is_some() {
                self.end_seq = seq;
                self.close(why);
            } else if why == "statement" {
                // A statement before the head may change what is proven:
                // the snapshot no longer holds, so nothing is speculated.
                self.gave_up = true;
            }
            return;
        }
        let mut found = Vec::new();
        touched(&mut self.ahead, h, d, &mut found);
        if self.ahead.broken {
            if self.head.is_some() {
                self.end_seq = seq;
                self.close("framing");
            } else {
                self.gave_up = true;
            }
            return;
        }
        for (db, table, at) in found {
            let k = (db, table);
            if let Some(owner) = self.reserved.get(&k) {
                self.reserved_rows.push((*owner, k, at));
                continue;
            }
            if self.batch.contains_key(&k)
                || !self.allow.matches(&k.0, &k.1)
                || mysql_checkpoint_position(
                    &at.file,
                    at.pos,
                    at.gtid_set.as_deref(),
                )
                .is_none()
                || self
                    .proven
                    .contains(&self.scope.key(&k.0, &k.1).backend_key())
            {
                continue;
            }
            if self.head.is_none() {
                self.head = Some((seq, received, at.clone()));
                // Its scan starts at its own head's stream anchor, as an
                // ordinary window's does at dispatch.
                self.head_anchor = self.ahead.txn_anchor.clone();
            }
            if self.batch.insert(k.clone(), at).is_none() {
                let key_bytes = self.scope.key(&k.0, &k.1).backend_key().len();
                self.fill.take(key_bytes as u64);
                self.order.push(k);
            }
        }
        if self.head.is_some() {
            self.end_seq = seq;
            if let Some(why) = window_full(self.batch.len(), Some(&self.fill)) {
                self.close(why);
            } else if self.events >= self.max_events {
                self.close("events");
            } else if self.bytes >= self.max_bytes {
                self.close("bytes");
            }
        }
    }

    /// Close the window and start its capture and scan.
    fn close(&mut self, why: &'static str) {
        self.closed = Some(why);
        // First-touch order: the capture's rank order.
        let tables: Vec<(String, String)> = self.order.clone();
        let froms: Vec<MySqlCheckpoint> = tables
            .iter()
            .map(|k| MySqlCheckpoint {
                lineage: Some(self.inputs.lineage.clone()),
                ..self.batch[k].clone()
            })
            .collect();
        let head = self.head.as_ref().map(|(_, _, h)| MySqlCheckpoint {
            lineage: Some(self.inputs.lineage.clone()),
            ..h.clone()
        });
        let Some(head) = head else {
            return;
        };
        let mut capture = self.inputs.capture.clone();
        capture.anchor = self.head_anchor.clone().map(|a| {
            super::mysql_binlog_scan::Anchor {
                server_uuid: capture.server_uuid.clone(),
                lineage: capture.lineage.clone(),
                ..a
            }
        });
        let busy = Busy::start();
        let done = busy.clone();
        let task = AbortOnDrop(tokio::spawn(async move {
            let r = capture_and_scan(&capture, &tables, &froms, &head).await;
            done.finish();
            r
        }));
        self.task = Some((task, busy));
    }

    /// The outcome after the admission: a pending window, or what became
    /// of the collection.
    pub(crate) fn finish(mut self) -> (NextWindow, Option<Pending>) {
        if self.gave_up {
            return (NextWindow::GaveUp, None);
        }
        let chainable = self.chainable();
        match (self.closed, self.task.take(), self.head) {
            (Some(closed_by), Some((task, busy)), Some((head_seq, _, _))) => (
                NextWindow::Formed,
                Some(Pending {
                    id: self.id,
                    head_seq,
                    end_seq: self.end_seq,
                    closed_by,
                    lineage: self.inputs.lineage.clone(),
                    batch: self.batch,
                    order: self.order,
                    task,
                    busy,
                    reserved_rows: self.reserved_rows,
                    ahead: self.ahead,
                    chainable,
                }),
            ),
            (None, _, Some(_)) => (NextWindow::Unclosed, None),
            _ => (NextWindow::None, None),
        }
    }

    pub(crate) fn candidates(&self) -> usize {
        self.batch.len()
    }

    /// Whether the event `seq` was taken (not when the window closed on
    /// time before it: it then belongs to whatever follows).
    fn took(&self, seq: u64) -> bool {
        self.end_seq >= seq
    }
}

/// The windows collected ahead during one admission, in order: each closes
/// and starts its capture, then the next is collected after it while there
/// is room (`activation_capture_concurrency`, less the windows already
/// queued).
pub(crate) struct Chain {
    active: Option<Collector>,
    room: usize,
    next_id: WindowId,
    formed: Vec<Pending>,
    /// What became of the first window collected here, and its candidates.
    first: Option<(NextWindow, usize)>,
}

impl Chain {
    /// `first`: the next window's collector, `None` when no room is left
    /// or nothing may follow the last queued window. `next_id`: the id
    /// after `first`'s, for its successors.
    pub(crate) fn new(
        first: Option<Collector>,
        room: usize,
        next_id: WindowId,
    ) -> Self {
        let none = first.is_none();
        Self {
            active: first,
            room,
            next_id,
            formed: Vec::new(),
            first: none.then_some((NextWindow::Full, 0)),
        }
    }

    /// The next unused window id.
    pub(crate) fn next_id(&self) -> WindowId {
        self.next_id
    }

    /// One held event, in order.
    pub(crate) fn observe(
        &mut self,
        seq: u64,
        received: Instant,
        h: &EventHeader,
        d: &EventData,
    ) {
        while let Some(c) = self.active.as_mut() {
            c.observe(seq, received, h, d);
            if !c.done() {
                return;
            }
            let took = c.took(seq);
            self.roll();
            if took {
                return;
            }
        }
    }

    /// When the open window's time bound passes.
    pub(crate) fn deadline(&self) -> Option<Instant> {
        self.active.as_ref().and_then(Collector::deadline)
    }

    /// The open window's time bound passed.
    pub(crate) fn time_up(&mut self) {
        if let Some(c) = self.active.as_mut() {
            c.time_up();
            if c.done() {
                self.roll();
            }
        }
    }

    /// The open window is done: queue it if it formed, and collect the
    /// next one after it while there is room.
    fn roll(&mut self) {
        let Some(c) = self.active.take() else {
            return;
        };
        let successor = (c.chainable() && self.room > 1).then(|| {
            let id = self.next_id;
            self.next_id += 1;
            c.successor(id)
        });
        let candidates = c.candidates();
        let (next, pending) = c.finish();
        self.first.get_or_insert((next, candidates));
        if let Some(p) = pending {
            self.formed.push(p);
            self.room = self.room.saturating_sub(1);
            self.active = successor;
        }
    }

    /// After the admission: what became of the first window collected
    /// here, its candidates, and the windows formed (in order). A window
    /// still open is dropped unformed; its events stay held.
    pub(crate) fn finish(mut self) -> (NextWindow, usize, Vec<Pending>) {
        if let Some(c) = self.active.take() {
            let candidates = c.candidates();
            let (next, _) = c.finish();
            self.first.get_or_insert((next, candidates));
        }
        let (next, candidates) = self.first.unwrap_or_default();
        (next, candidates, self.formed)
    }
}

/// The queue index of the first row of a decided table that its decision
/// does not cover (the queue is cut there; a later window decides it).
pub(crate) fn first_uncovered(
    rows: &[(usize, (String, String), MySqlCheckpoint)],
    decisions: &HashMap<(String, String), (MySqlCheckpoint, Governs)>,
) -> Option<usize> {
    rows.iter()
        .find(|(_, k, at)| {
            decisions
                .get(k)
                .is_some_and(|(decided, g)| !covers_row(g, decided, at))
        })
        .map(|(i, ..)| *i)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cp(gtids: &str) -> MySqlCheckpoint {
        MySqlCheckpoint {
            file: "mysql-bin.000001".into(),
            pos: 4,
            gtid_set: Some(gtids.into()),
            lineage: None,
            snapshot_completed: None,
            snapshot_chain: None,
        }
    }

    fn fp(pos: u64) -> MySqlCheckpoint {
        MySqlCheckpoint {
            file: "mysql-bin.000001".into(),
            pos,
            gtid_set: None,
            lineage: None,
            snapshot_completed: None,
            snapshot_chain: None,
        }
    }

    const U: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562";

    fn key(t: &str) -> (String, String) {
        ("d".into(), t.into())
    }

    #[test]
    fn a_statement_precedes_the_boundaries_that_include_it() {
        let d = EventIdentity::Gtid {
            gtid: format!("{U}:5"),
            ordinal: 0,
        };
        assert_eq!(statement_before(&d, &cp(&format!("{U}:1-4"))), Some(false));
        assert_eq!(statement_before(&d, &cp(&format!("{U}:1-5"))), Some(true));
        let d = EventIdentity::FilePos {
            file: "mysql-bin.000001".into(),
            end_pos: 500,
            ordinal: 0,
        };
        assert_eq!(statement_before(&d, &fp(400)), Some(false));
        assert_eq!(statement_before(&d, &fp(500)), Some(true));
        let d = EventIdentity::StreamStart {
            position: mysql_checkpoint_position("mysql-bin.000001", 4, None)
                .unwrap(),
        };
        assert_eq!(statement_before(&d, &fp(400)), None);
    }

    /// Admission covers every later row; a stable exclusion covers the rows
    /// before its statement only; a position-specific outcome covers its
    /// own transaction only; anything unknowable is not covered.
    #[test]
    fn decisions_cover_exactly_their_rows() {
        let a = cp(&format!("{U}:1-3"));
        let later = cp(&format!("{U}:1-4"));
        let after = cp(&format!("{U}:1-6"));
        assert!(covers_row(&Governs::Admitted, &a, &after));
        let d = EventIdentity::Gtid {
            gtid: format!("{U}:5"),
            ordinal: 0,
        };
        let stable = Governs::Refused(Covers::UntilStatement(d));
        assert!(covers_row(&stable, &a, &a));
        assert!(covers_row(&stable, &a, &later));
        assert!(!covers_row(&stable, &a, &after));
        let specific = Governs::Refused(Covers::Transaction);
        assert!(covers_row(&specific, &a, &a));
        assert!(!covers_row(&specific, &a, &later));
        let unknown = Governs::Refused(Covers::UntilStatement(
            EventIdentity::StreamStart {
                position: mysql_checkpoint_position(
                    "mysql-bin.000001",
                    4,
                    None,
                )
                .unwrap(),
            },
        ));
        assert!(!covers_row(&unknown, &a, &later));
    }

    /// The required set must be a subset of the speculation, each entry at
    /// exactly the same start; extra candidates are fine.
    #[test]
    fn required_tables_must_be_speculated_at_the_same_start() {
        let a = cp(&format!("{U}:1-3"));
        let b = cp(&format!("{U}:1-4"));
        let spec = BTreeMap::from([
            (key("t1"), a.clone()),
            (key("t2"), b.clone()),
            (key("extra"), a.clone()),
        ]);
        let required =
            BTreeMap::from([(key("t1"), a.clone()), (key("t2"), b.clone())]);
        assert_eq!(check_required(&required, &spec), Ok(()));
        let missing = BTreeMap::from([(key("t3"), a.clone())]);
        assert!(
            check_required(&missing, &spec)
                .unwrap_err()
                .contains("not captured")
        );
        let moved = BTreeMap::from([(key("t1"), b.clone())]);
        assert!(
            check_required(&moved, &spec)
                .unwrap_err()
                .contains("starts at")
        );
    }

    /// Several transactions of one table in a window: after admission or a
    /// stable exclusion its later rows are covered (up to the excluding
    /// statement); after a position-specific refusal the queue is cut at
    /// its next transaction. Undecided (proven) tables never cut.
    #[test]
    fn the_queue_is_cut_at_the_first_uncovered_row() {
        let a = cp(&format!("{U}:1-3"));
        let later = cp(&format!("{U}:1-4"));
        let last = cp(&format!("{U}:1-7"));
        let d = EventIdentity::Gtid {
            gtid: format!("{U}:6"),
            ordinal: 0,
        };
        let rows = vec![
            (0, key("adm"), a.clone()),
            (1, key("ex"), a.clone()),
            (2, key("spec"), a.clone()),
            (3, key("other"), later.clone()),
            (4, key("adm"), later.clone()),
            (5, key("ex"), later.clone()),
            (6, key("spec"), later.clone()),
            (7, key("ex"), last.clone()),
        ];
        let mut decisions = HashMap::from([
            (key("adm"), (a.clone(), Governs::Admitted)),
            (
                key("ex"),
                (a.clone(), Governs::Refused(Covers::UntilStatement(d))),
            ),
        ]);
        assert_eq!(
            first_uncovered(&rows, &decisions),
            Some(7),
            "after the DDL"
        );
        decisions.insert(
            key("spec"),
            (a.clone(), Governs::Refused(Covers::Transaction)),
        );
        assert_eq!(first_uncovered(&rows, &decisions), Some(6));
        assert_eq!(first_uncovered(&rows[..6], &decisions), None);
    }

    /// A table cut from the window's prefix (deferred) covers none of its
    /// rows: the queue is cut at its first one, before any later row
    /// (admitted tables' included), and a reserved row of it is uncovered.
    #[test]
    fn a_deferred_table_cuts_at_its_first_row() {
        let a = cp(&format!("{U}:1-3"));
        let later = cp(&format!("{U}:1-4"));
        let rows = vec![
            (0, key("adm"), a.clone()),
            (1, key("adm"), a.clone()),
            (2, key("def"), a.clone()),
            (3, key("adm"), later.clone()),
            (4, key("def"), later.clone()),
        ];
        let decisions = HashMap::from([
            (key("adm"), (a.clone(), Governs::Admitted)),
            (key("def"), (a.clone(), Governs::Deferred)),
        ]);
        assert_eq!(first_uncovered(&rows, &decisions), Some(2));
        assert!(!covers_row(&Governs::Deferred, &a, &a));
        let rows = [(7, key("def"), later)];
        assert_eq!(first_unheld([&rows[..]], 7, &decisions), Some(0));
    }

    #[test]
    fn reserved_rows_need_a_covering_decision() {
        let a = cp(&format!("{U}:1-3"));
        let later = cp(&format!("{U}:1-4"));
        let decisions = HashMap::from([
            (key("adm"), (a.clone(), Governs::Admitted)),
            (
                key("spec"),
                (a.clone(), Governs::Refused(Covers::Transaction)),
            ),
        ]);
        let held = |k: &str| [(1, key(k), later.clone())];
        assert_eq!(first_unheld([&held("adm")[..]], 1, &decisions), None);
        assert_eq!(first_unheld([&held("spec")[..]], 1, &decisions), Some(0));
        // A reserved table the window did not decide (an extra candidate)
        // is not held either.
        assert_eq!(
            first_unheld([&held("unknown")[..]], 1, &decisions),
            Some(0)
        );
    }

    /// Only the committing window's rows are checked, in each queued window
    /// in order: the first queued window holding an uncovered one is where
    /// the queue is abandoned, whatever follows it.
    #[test]
    fn the_first_queued_window_that_cannot_hold_ends_the_queue() {
        let a = cp(&format!("{U}:1-3"));
        let later = cp(&format!("{U}:1-4"));
        let decisions = HashMap::from([
            (key("adm"), (a.clone(), Governs::Admitted)),
            (
                key("ref"),
                (a.clone(), Governs::Refused(Covers::Transaction)),
            ),
        ]);
        // Window 1 commits; windows 2, 3 and 4 are queued.
        let w2 = [
            (1, key("adm"), later.clone()),
            (5, key("ref"), later.clone()),
        ];
        let w3 = [(2, key("x"), later.clone()), (1, key("ref"), later.clone())];
        let w4 = [(1, key("adm"), later.clone())];
        assert_eq!(
            first_unheld([&w2[..], &w3[..], &w4[..]], 1, &decisions),
            Some(1),
            "window 2's uncovered row belongs to window 5, not to window 1"
        );
        assert_eq!(first_unheld([&w2[..], &w4[..]], 1, &decisions), None);
        assert_eq!(first_unheld([&w3[..], &w2[..]], 1, &decisions), Some(0));
        assert_eq!(
            first_unheld(std::iter::empty::<&[ReservedRow]>(), 1, &decisions),
            None
        );
    }
}
