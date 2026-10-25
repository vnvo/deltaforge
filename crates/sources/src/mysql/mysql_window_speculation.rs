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
//! dropped, so the server rolls back and releases its metadata locks)
//! whenever the window is abandoned or the run stops.

use std::collections::{BTreeMap, HashMap, HashSet};
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
    is_transaction_control, touched,
};
use super::mysql_binlog_scan::MAX_BATCH_TABLES;
use crate::durable_checkpoint::{mysql_checkpoint_position, order_positions};
use crate::gtid_set::GtidSet;
use crate::registry_scope::RegistryScope;

/// A held event: when it was received, its held sequence number (0: not
/// held), its header and data.
pub(crate) type Held = (Instant, u64, EventHeader, EventData);

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
    pub head_seq: u64,
    pub end_seq: u64,
    pub closed_by: &'static str,
    pub lineage: String,
    pub batch: BTreeMap<(String, String), MySqlCheckpoint>,
    pub task: AbortOnDrop<Result<CapturedWindow, SourceError>>,
}

impl std::fmt::Debug for Pending {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pending")
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
}

impl NextWindow {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            NextWindow::None => "none",
            NextWindow::Formed => "formed",
            NextWindow::Unclosed => "unclosed",
            NextWindow::GaveUp => "gave_up",
            NextWindow::Abandoned => "abandoned",
        }
    }
}

/// The speculative window's collection over held events, in order.
pub(crate) struct Collector {
    ahead: Ahead,
    allow: AllowList,
    scope: Arc<RegistryScope>,
    proven: HashSet<String>,
    reserved: HashSet<(String, String)>,
    bound: Duration,
    max_events: usize,
    max_bytes: u64,
    head: Option<(u64, Instant, MySqlCheckpoint)>,
    head_anchor: Option<super::mysql_binlog_scan::Anchor>,
    batch: BTreeMap<(String, String), MySqlCheckpoint>,
    events: usize,
    bytes: u64,
    end_seq: u64,
    closed: Option<&'static str>,
    gave_up: bool,
    /// Rows of reserved tables seen in the held events (by the time this
    /// window closed): checked against the current window's decisions.
    pub reserved_rows: Vec<((String, String), MySqlCheckpoint)>,
    inputs: SpecInputs,
    task: Option<AbortOnDrop<Result<CapturedWindow, SourceError>>>,
}

impl Collector {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        ahead: Ahead,
        allow: AllowList,
        scope: Arc<RegistryScope>,
        proven: HashSet<String>,
        reserved: HashSet<(String, String)>,
        cfg: &deltaforge_config::MysqlProofCfg,
        inputs: SpecInputs,
    ) -> Self {
        Self {
            ahead,
            allow,
            scope,
            proven,
            reserved,
            bound: Duration::from_millis(cfg.activation_window_ms),
            max_events: cfg.activation_window_events,
            max_bytes: cfg.activation_window_bytes,
            head: None,
            head_anchor: None,
            batch: BTreeMap::new(),
            events: 0,
            bytes: 0,
            end_seq: 0,
            closed: None,
            gave_up: false,
            reserved_rows: Vec::new(),
            inputs,
            task: None,
        }
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
        if self.done() {
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
            if self.reserved.contains(&k) {
                self.reserved_rows.push((k, at));
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
            self.batch.insert(k, at);
        }
        if self.head.is_some() {
            self.end_seq = seq;
            if self.batch.len() >= MAX_BATCH_TABLES {
                self.close("tables");
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
        let tables: Vec<(String, String)> =
            self.batch.keys().cloned().collect();
        let froms: Vec<MySqlCheckpoint> = self
            .batch
            .values()
            .map(|a| MySqlCheckpoint {
                lineage: Some(self.inputs.lineage.clone()),
                ..a.clone()
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
        self.task = Some(AbortOnDrop(tokio::spawn(async move {
            capture_and_scan(&capture, &tables, &froms, &head).await
        })));
    }

    /// The outcome after the admission: a pending window, or what became
    /// of the collection.
    pub(crate) fn finish(mut self) -> (NextWindow, Option<Pending>) {
        if self.gave_up {
            return (NextWindow::GaveUp, None);
        }
        match (self.closed, self.task.take(), self.head) {
            (Some(closed_by), Some(task), Some((head_seq, _, _))) => (
                NextWindow::Formed,
                Some(Pending {
                    head_seq,
                    end_seq: self.end_seq,
                    closed_by,
                    lineage: self.inputs.lineage.clone(),
                    batch: self.batch,
                    task,
                }),
            ),
            (None, _, Some(_)) => (NextWindow::Unclosed, None),
            _ => (NextWindow::None, None),
        }
    }

    pub(crate) fn candidates(&self) -> usize {
        self.batch.len()
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

/// Whether every recorded row of a reserved table is covered by its
/// table's decision.
pub(crate) fn reserved_rows_covered(
    rows: &[((String, String), MySqlCheckpoint)],
    decisions: &HashMap<(String, String), (MySqlCheckpoint, Governs)>,
) -> bool {
    rows.iter().all(|(k, at)| {
        decisions
            .get(k)
            .is_some_and(|(decided, governs)| covers_row(governs, decided, at))
    })
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
        assert!(reserved_rows_covered(
            &[(key("adm"), later.clone())],
            &decisions
        ));
        assert!(!reserved_rows_covered(
            &[(key("spec"), later.clone())],
            &decisions
        ));
        assert!(!reserved_rows_covered(
            &[(key("unknown"), later)],
            &decisions
        ));
    }
}
