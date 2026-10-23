//! One proof scanner per MySQL source: a shared, ordered record of the
//! classified statements (DDL, database and lineage barriers - never DML)
//! of one contiguous covered interval `(lo, hi]` of the source's binlog.
//!
//! A proof asks for its interval `(A, B]` (a batch: several `A_i` with one
//! `B`). When `(lo, hi]` covers it, the answer comes from the record;
//! otherwise the record is extended by a physical scan of only the missing
//! part - `(hi, B]` to the right, `(A, lo]` to the left - with the existing
//! scan (identity proven on its session, complete, exact, fail-closed). Each
//! proof still applies its own `affects` and `covers` rules to the
//! statements of its own interval, and its digest is the canonical
//! [`scan_digest`] of `(A, B]` over them, so a table first seen at a
//! position the record already covers costs no rescan of the history
//! between them.
//!
//! Coalescing (`docs/design/mysql-proof-seek.md`, "Layer 2: the coalescing
//! scanner"):
//! - `A == B`: the canonical empty proof, no session; `B < A` or unordered
//!   positions fail closed.
//! - A request pins its `A` before it waits, so pruning never removes what a
//!   queued request needs; the pin is removed when the request ends or is
//!   dropped.
//! - Requests are served in rounds, one at a time, on the scanner's own
//!   task. A round takes every request queued before it starts: one queued
//!   while a round runs is served by exactly the next round. A round's
//!   targets are fixed when it starts (the least `A` before `lo`, the
//!   greatest `B` after `hi`), so it makes at most two extensions.
//! - Each extension makes at most one positioned attempt (the right one at
//!   the record's frontier anchor, the left one at the stream anchor of its
//!   boundary) and at most one bounded unanchored attempt, only without an
//!   anchor or after the positioned one was refused or failed. No third.
//! - A round builds its extensions into a staging record; the record, its
//!   frontier and the anchor cache change only when every extension
//!   reached its target and the record's bounds hold. A failed or cancelled
//!   round leaves them exactly as they were.
//! - A waiter that is cancelled leaves; the round continues while any of
//!   its waiters remains, and is dropped (its stream reset, nothing
//!   committed) only when none does.
//! - The record belongs to one server identity and lineage; another one
//!   clears it.
//! - Pruning (at each round) drops statements at or before the lowest of
//!   the round's stream boundaries and every pin; a lowest point past `hi`
//!   clears the record (it restarts at the next request's `A`).
//!
//! Operational semantics: the existing scan limits bound each physical
//! extension (the work done); answering from the record re-reads no DML, so
//! a request is no longer limited by the size of its whole logical interval.
//! The record's own bounds limit retained state.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use deltaforge_core::CheckpointOrder;
use tokio::sync::oneshot;

use super::MySqlCheckpoint;
use super::mysql_activation::EventIdentity;
use super::mysql_binlog_scan::{
    Anchor, ProofError, ProofKind, ProofWork, ScanLimits, ScanReport, ScanTag,
    ScanTrace, Statement, encode_effect, encode_event, scan_digest,
    scan_interval_at, trace_enabled,
};
use crate::durable_checkpoint::{
    WmPos, mysql_checkpoint_position, order_positions,
};
use crate::gtid_set::GtidSet;

/// Bounds on the retained record.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RecordLimits {
    pub max_statements: usize,
    pub max_bytes: usize,
}

impl Default for RecordLimits {
    fn default() -> Self {
        Self {
            max_statements: 100_000,
            max_bytes: 64 * 1024 * 1024,
        }
    }
}

/// Session anchors kept for later requests at the same boundary.
const ANCHOR_CACHE: usize = 64;

/// A physical scan of `(from, to]`, opened at `anchor` when one is given
/// (production: [`LiveSegments`]).
#[async_trait]
pub(crate) trait Segments: Send + Sync {
    async fn scan(
        &self,
        from: &MySqlCheckpoint,
        to: &MySqlCheckpoint,
        anchor: Option<&Anchor>,
        tag: &ScanTag<'_>,
    ) -> Result<ScanReport, ProofError>;
}

/// Physical scans on the source's verified server.
pub(crate) struct LiveSegments {
    pub dsn: String,
    pub server_id: u64,
    pub server_uuid: String,
    pub lineage_hash: String,
    pub limits: ScanLimits,
}

#[async_trait]
impl Segments for LiveSegments {
    async fn scan(
        &self,
        from: &MySqlCheckpoint,
        to: &MySqlCheckpoint,
        anchor: Option<&Anchor>,
        tag: &ScanTag<'_>,
    ) -> Result<ScanReport, ProofError> {
        scan_interval_at(
            &self.dsn,
            self.server_id,
            &self.server_uuid,
            &self.lineage_hash,
            from,
            to,
            anchor,
            &self.limits,
            tag,
        )
        .await
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Identity {
    server_uuid: String,
    lineage_hash: String,
}

impl Identity {
    fn owns(&self, a: &Anchor) -> bool {
        a.server_uuid.eq_ignore_ascii_case(&self.server_uuid)
            && a.lineage == self.lineage_hash
    }
}

#[derive(Clone)]
struct Covered {
    identity: Identity,
    lo: MySqlCheckpoint,
    hi: MySqlCheckpoint,
    statements: Vec<Statement>,
    bytes: usize,
    /// The boundary after `hi`'s last transaction, observed where the scan
    /// that reached `hi` ended (GTID mode).
    frontier: Option<Anchor>,
}

/// A queued request: one `(head, B]` with the stream anchor at `head`.
struct Request {
    identity: Identity,
    head: MySqlCheckpoint,
    a: WmPos,
    to: MySqlCheckpoint,
    b: WmPos,
    boundary: Option<MySqlCheckpoint>,
    anchor: Option<Anchor>,
    segments: Arc<dyn Segments>,
    source_id: String,
    kind: ProofKind,
}

/// What a round gives each of its requests: the committed record (which
/// covers the request) and the round's physical work.
struct Served {
    record: Arc<Covered>,
    tally: Tally,
}

/// A round's physical work: its counters, the traces of the scans that
/// completed, and the events and bytes they read.
#[derive(Clone, Default)]
struct Tally {
    work: ProofWork,
    traces: Vec<ScanTrace>,
    events: u64,
    bytes: u64,
}

type Reply = oneshot::Sender<Result<Served, ProofError>>;

#[derive(Default)]
struct Inner {
    record: Option<Arc<Covered>>,
    /// Session anchors observed by committed extensions, newest last.
    anchors: VecDeque<Anchor>,
    queue: Vec<(Request, Reply)>,
    /// Whether the scanner's task is running rounds.
    driving: bool,
}

type Pins = Arc<std::sync::Mutex<BTreeMap<u64, MySqlCheckpoint>>>;

/// The shared proof scanner of one source.
pub(crate) struct ProofScanner {
    inner: Arc<std::sync::Mutex<Inner>>,
    pins: Pins,
    next_pin: AtomicU64,
    limits: RecordLimits,
}

impl Default for ProofScanner {
    fn default() -> Self {
        Self::new(RecordLimits::default())
    }
}

/// Removes its pin when the request ends or is dropped.
struct Pin {
    pins: Pins,
    id: u64,
}

impl Drop for Pin {
    fn drop(&mut self) {
        self.pins.lock().expect("pins").remove(&self.id);
    }
}

fn wm(p: &MySqlCheckpoint) -> Result<WmPos, ProofError> {
    mysql_checkpoint_position(&p.file, p.pos, p.gtid_set.as_deref()).ok_or_else(
        || ProofError::Positions(format!("unparseable position {p:?}")),
    )
}

/// The exact GTID set of a GTID-mode position.
fn gtid_set_of(p: &WmPos) -> Option<GtidSet> {
    match p {
        WmPos::MysqlGtid { gtid_set } => GtidSet::parse(gtid_set),
        _ => None,
    }
}

fn entry_bytes(s: &Statement) -> usize {
    encode_event(&s.event).len() + encode_effect(&s.effect).len() + 64
}

/// Whether statement `s` lies at or before position `p`.
fn at_or_before(s: &Statement, p: &WmPos) -> Option<bool> {
    match (&s.event, p) {
        (EventIdentity::Gtid { gtid, .. }, WmPos::MysqlGtid { gtid_set }) => {
            let set = GtidSet::parse(gtid_set)?;
            let one = GtidSet::parse(gtid)?;
            Some(one.is_subset(&set))
        }
        (EventIdentity::FilePos { file, end_pos, .. }, _) => {
            let here = mysql_checkpoint_position(file, *end_pos, None)?;
            match order_positions(&here, p) {
                CheckpointOrder::Before | CheckpointOrder::Equal => Some(true),
                CheckpointOrder::After => Some(false),
                CheckpointOrder::Incomparable => None,
            }
        }
        _ => None,
    }
}

/// The recorded statements of `(a, b]`.
fn inside(
    c: &Covered,
    a: &WmPos,
    b: &WmPos,
) -> Result<Vec<Statement>, ProofError> {
    let (lo, hi) = (wm(&c.lo)?, wm(&c.hi)?);
    if !matches!(
        order_positions(&lo, a),
        CheckpointOrder::Before | CheckpointOrder::Equal
    ) || !matches!(
        order_positions(b, &hi),
        CheckpointOrder::Before | CheckpointOrder::Equal
    ) {
        return Err(ProofError::Positions(
            "the record does not cover the interval".into(),
        ));
    }
    let unordered =
        || ProofError::Positions("a recorded statement is unordered".into());
    let mut out = Vec::new();
    for s in &c.statements {
        let after_a = !at_or_before(s, a).ok_or_else(unordered)?;
        let within_b = at_or_before(s, b).ok_or_else(unordered)?;
        if after_a && within_b {
            out.push(s.clone());
        }
    }
    Ok(out)
}

impl ProofScanner {
    pub(crate) fn new(limits: RecordLimits) -> Self {
        Self {
            inner: Default::default(),
            pins: Default::default(),
            next_pin: AtomicU64::new(1),
            limits,
        }
    }

    fn pin(&self, at: &MySqlCheckpoint) -> Pin {
        let id = self.next_pin.fetch_add(1, Ordering::Relaxed);
        self.pins.lock().expect("pins").insert(id, at.clone());
        Pin {
            pins: self.pins.clone(),
            id,
        }
    }

    /// The statements of `(from, to]` and its canonical digest (tests: the
    /// source asks through [`shared_interval`]). `anchor`:
    /// the stream anchor at `from`, when the caller observed one.
    /// `boundary`: the stream's last transaction boundary (pruning).
    /// `events`/`bytes` and `work` report the physical work of the round
    /// that served this request (0 when answered from the record).
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn interval(
        &self,
        segments: Arc<dyn Segments>,
        server_uuid: &str,
        lineage_hash: &str,
        from: &MySqlCheckpoint,
        to: &MySqlCheckpoint,
        anchor: Option<&Anchor>,
        boundary: Option<&MySqlCheckpoint>,
        tag: &ScanTag<'_>,
    ) -> Result<ScanReport, ProofError> {
        let mut answers = self
            .intervals(
                segments,
                server_uuid,
                lineage_hash,
                std::slice::from_ref(from),
                to,
                anchor,
                boundary,
                tag,
            )
            .await?;
        Ok(answers.pop().expect("one answer per start"))
    }

    /// A batch: the statements and digest of every `(from_i, to]`, from one
    /// request on the least start (`anchor`: the stream anchor there). One
    /// answer per start, in order, each its own canonical proof; one
    /// failure fails them all.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn intervals(
        &self,
        segments: Arc<dyn Segments>,
        server_uuid: &str,
        lineage_hash: &str,
        froms: &[MySqlCheckpoint],
        to: &MySqlCheckpoint,
        anchor: Option<&Anchor>,
        boundary: Option<&MySqlCheckpoint>,
        tag: &ScanTag<'_>,
    ) -> Result<Vec<ScanReport>, ProofError> {
        let started = std::time::Instant::now();
        for (name, p) in froms.iter().map(|f| ("from", f)).chain([("to", to)]) {
            if p.lineage.as_deref() != Some(lineage_hash) {
                return Err(ProofError::Positions(format!(
                    "{name} position belongs to lineage {:?}, not {lineage_hash}",
                    p.lineage
                )));
            }
        }
        let b = wm(to)?;
        // Each start: its position, and whether its interval is non-empty.
        let mut starts = Vec::with_capacity(froms.len());
        let mut head: Option<(usize, WmPos)> = None;
        for (i, f) in froms.iter().enumerate() {
            let a = wm(f)?;
            let open = match order_positions(&a, &b) {
                CheckpointOrder::Equal => false,
                CheckpointOrder::Before => true,
                other => {
                    return Err(ProofError::Positions(format!(
                        "from is not before to ({other:?})"
                    )));
                }
            };
            if open {
                head = match head {
                    None => Some((i, a.clone())),
                    Some((j, h)) => match order_positions(&a, &h) {
                        CheckpointOrder::Before => Some((i, a.clone())),
                        CheckpointOrder::Equal | CheckpointOrder::After => {
                            Some((j, h))
                        }
                        CheckpointOrder::Incomparable => {
                            return Err(ProofError::Positions(
                                "the batch's starts are unordered".into(),
                            ));
                        }
                    },
                };
            }
            starts.push((a, open));
        }
        let Some((h, a_head)) = head else {
            for f in froms {
                observe_request(tag, f, to, "empty", 0, 0, &[], None);
            }
            return Ok(starts
                .iter()
                .map(|(a, _)| answer(a, &b, Vec::new(), 0, 0, started))
                .collect());
        };
        // Pinned before waiting: pruning keeps everything after the head
        // while this request is queued or running.
        let _pin = self.pin(&froms[h]);
        let identity = Identity {
            server_uuid: server_uuid.to_ascii_lowercase(),
            lineage_hash: lineage_hash.to_string(),
        };
        let (reply, served) = oneshot::channel();
        self.enqueue(
            Request {
                identity,
                head: froms[h].clone(),
                a: a_head,
                to: to.clone(),
                b: b.clone(),
                boundary: boundary.cloned(),
                anchor: anchor.cloned(),
                segments,
                source_id: tag.source_id.to_string(),
                kind: tag.kind,
            },
            reply,
        );
        let served = match served.await {
            Ok(r) => r,
            Err(_) => Err(ProofError::Open("the proof scanner stopped".into())),
        };
        let served = match served {
            Ok(s) => s,
            Err(e) => {
                for f in froms {
                    observe_request(tag, f, to, "failed", 0, 0, &[], None);
                }
                return Err(e);
            }
        };
        let c = &served.record;
        let Tally {
            work,
            traces,
            events,
            bytes,
        } = &served.tally;
        let (events, bytes) = (*events, *bytes);
        let mut answers = Vec::with_capacity(froms.len());
        for (f, (a, open)) in froms.iter().zip(&starts) {
            let statements = if *open { inside(c, a, &b)? } else { Vec::new() };
            let how = match (open, traces.is_empty()) {
                (false, _) => "empty",
                (true, true) => "record",
                (true, false) => "scanned",
            };
            observe_request(
                tag,
                f,
                to,
                how,
                events,
                bytes,
                traces,
                Some((c.statements.len(), c.bytes)),
            );
            let mut report = answer(a, &b, statements, events, bytes, started);
            report.work = *work;
            if let Some(last) = traces.last() {
                report.trace = last.clone();
            }
            answers.push(report);
        }
        Ok(answers)
    }

    /// Queue `req` for the next round, starting the scanner's task if no
    /// round is running.
    fn enqueue(&self, req: Request, reply: Reply) {
        let mut g = self.inner.lock().expect("scanner");
        g.queue.push((req, reply));
        if !g.driving {
            g.driving = true;
            tokio::spawn(drive(
                self.inner.clone(),
                self.pins.clone(),
                self.limits,
            ));
        }
    }

    /// The retained record: (statements, bytes).
    #[cfg(test)]
    pub(crate) async fn retained(&self) -> Option<(usize, usize)> {
        self.inner
            .lock()
            .expect("scanner")
            .record
            .as_ref()
            .map(|c| (c.statements.len(), c.bytes))
    }

    /// The record's bounds and frontier, and the cached anchors.
    #[cfg(test)]
    fn snapshot(
        &self,
    ) -> Option<(
        MySqlCheckpoint,
        MySqlCheckpoint,
        Option<Anchor>,
        Vec<Anchor>,
    )> {
        let g = self.inner.lock().expect("scanner");
        g.record.as_ref().map(|c| {
            (
                c.lo.clone(),
                c.hi.clone(),
                c.frontier.clone(),
                g.anchors.iter().cloned().collect(),
            )
        })
    }

    #[cfg(test)]
    pub(crate) fn pinned(&self) -> usize {
        self.pins.lock().expect("pins").len()
    }
}

/// Stops the scanner's task cleanly, also on a panic: later requests start
/// a new one, and requests still queued are dropped (their waiters fail).
struct Driving {
    inner: Arc<std::sync::Mutex<Inner>>,
    armed: bool,
}

impl Drop for Driving {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        g.queue.clear();
        g.driving = false;
    }
}

/// The scanner's task: rounds, one at a time, while requests are queued.
async fn drive(
    inner: Arc<std::sync::Mutex<Inner>>,
    pins: Pins,
    limits: RecordLimits,
) {
    let mut guard = Driving {
        inner: inner.clone(),
        armed: true,
    };
    loop {
        let round: Vec<(Request, Reply)> = {
            let mut g = inner.lock().expect("scanner");
            let round: Vec<_> = std::mem::take(&mut g.queue)
                .into_iter()
                .filter(|(_, reply)| !reply.is_closed())
                .collect();
            if round.is_empty() {
                // Under the lock that queues: a request queued after this
                // starts a new task.
                g.driving = false;
                drop(g);
                guard.armed = false;
                return;
            }
            round
        };
        run_round(&inner, &pins, limits, round).await;
    }
}

/// A round's extension: `(from, to]`, at `anchor` when one is known.
struct Extension {
    from: MySqlCheckpoint,
    to: MySqlCheckpoint,
    anchor: Option<Anchor>,
}

async fn run_round(
    inner: &std::sync::Mutex<Inner>,
    pins: &Pins,
    limits: RecordLimits,
    round: Vec<(Request, Reply)>,
) {
    // One identity per round: the first request's. A concurrent request of
    // another identity fails closed.
    let identity = round[0].0.identity.clone();
    let (round, others): (Vec<_>, Vec<_>) =
        round.into_iter().partition(|(r, _)| r.identity == identity);
    for (_, reply) in others {
        let _ = reply.send(Err(ProofError::OtherServer(
            "a concurrent proof request of another server identity".into(),
        )));
    }

    // Prune at the lowest pin and stream boundary, and fix the snapshot the
    // round builds on.
    let (record, cached) = {
        let mut g = inner.lock().expect("scanner");
        if g.record.as_ref().is_some_and(|c| c.identity != identity) {
            g.record = None;
        }
        g.anchors.retain(|a| identity.owns(a));
        let boundaries: Vec<MySqlCheckpoint> = round
            .iter()
            .filter_map(|(r, _)| r.boundary.clone())
            .collect();
        prune(&mut g.record, pins, &boundaries);
        (g.record.clone(), g.anchors.clone())
    };

    // The targets: the least start before `lo`, the greatest end after
    // `hi` (with no record: the least start and the greatest end). A
    // request unordered with the record or the round is refused alone.
    let (lo, hi) = match &record {
        Some(c) => match (wm(&c.lo), wm(&c.hi)) {
            (Ok(lo), Ok(hi)) => (Some(lo), Some(hi)),
            _ => (None, None),
        },
        None => (None, None),
    };
    let record = record.filter(|_| lo.is_some());
    let mut left: Option<(usize, WmPos)> = None;
    let mut right: Option<(usize, WmPos)> = None;
    let mut members: Vec<(Request, Reply)> = Vec::new();
    for (r, reply) in round {
        let before_lo = match &lo {
            None => Ok(true),
            Some(lo) => match order_positions(&r.a, lo) {
                CheckpointOrder::Before => Ok(true),
                CheckpointOrder::Equal | CheckpointOrder::After => Ok(false),
                CheckpointOrder::Incomparable => {
                    Err("the start is unordered with the covered interval")
                }
            },
        };
        let after_hi = match &hi {
            None => Ok(true),
            Some(hi) => match order_positions(&r.b, hi) {
                CheckpointOrder::After => Ok(true),
                CheckpointOrder::Before | CheckpointOrder::Equal => Ok(false),
                CheckpointOrder::Incomparable => {
                    Err("the end is unordered with the covered interval")
                }
            },
        };
        let i = members.len();
        let new_left = match before_lo {
            Ok(true) => match &left {
                None => Ok(Some((i, r.a.clone()))),
                Some((_, l)) => match order_positions(&r.a, l) {
                    CheckpointOrder::Before => Ok(Some((i, r.a.clone()))),
                    CheckpointOrder::Equal | CheckpointOrder::After => Ok(None),
                    CheckpointOrder::Incomparable => {
                        Err("the start is unordered with the round's")
                    }
                },
            },
            Ok(false) => Ok(None),
            Err(e) => Err(e),
        };
        let new_right = match after_hi {
            Ok(true) => match &right {
                None => Ok(Some((i, r.b.clone()))),
                Some((_, rt)) => match order_positions(&r.b, rt) {
                    CheckpointOrder::After => Ok(Some((i, r.b.clone()))),
                    CheckpointOrder::Equal | CheckpointOrder::Before => {
                        Ok(None)
                    }
                    CheckpointOrder::Incomparable => {
                        Err("the end is unordered with the round's")
                    }
                },
            },
            Ok(false) => Ok(None),
            Err(e) => Err(e),
        };
        match (new_left, new_right) {
            (Ok(l), Ok(rt)) => {
                if l.is_some() {
                    left = l;
                }
                if rt.is_some() {
                    right = rt;
                }
                members.push((r, reply));
            }
            (Err(why), _) | (_, Err(why)) => {
                let _ = reply.send(Err(ProofError::Positions(why.into())));
            }
        }
    }
    if members.is_empty() {
        return;
    }

    // The stream anchor at a start: the request's own, else a cached
    // session anchor; only one taken at exactly that set, of this identity.
    let anchor_at = |i: usize| -> Option<Anchor> {
        let r = &members[i].0;
        let set = gtid_set_of(&r.a)?;
        r.anchor
            .iter()
            .chain(cached.iter().rev())
            .find(|a| identity.owns(a) && a.set_before == set)
            .cloned()
    };
    let mut extensions: Vec<Extension> = Vec::new();
    match &record {
        None => {
            let (l, _) = left.as_ref().expect("a start without a record");
            let (rt, _) = right.as_ref().expect("an end without a record");
            extensions.push(Extension {
                from: members[*l].0.head.clone(),
                to: members[*rt].0.to.clone(),
                anchor: anchor_at(*l),
            });
        }
        Some(c) => {
            if let Some((l, _)) = &left {
                extensions.push(Extension {
                    from: members[*l].0.head.clone(),
                    to: c.lo.clone(),
                    anchor: anchor_at(*l),
                });
            }
            if let Some((rt, _)) = &right {
                let hi_set = hi.as_ref().and_then(gtid_set_of);
                extensions.push(Extension {
                    from: c.hi.clone(),
                    to: members[*rt].0.to.clone(),
                    anchor: c.frontier.clone().filter(|f| {
                        identity.owns(f)
                            && Some(&f.set_before) == hi_set.as_ref()
                    }),
                });
            }
        }
    }
    let segments = members[0].0.segments.clone();
    let source_id = members[0].0.source_id.clone();
    let kind = members[0].0.kind;
    let (requests, mut replies): (Vec<Request>, Vec<Reply>) =
        members.into_iter().unzip();
    let left_first = record.is_some() && left.is_some();

    let mut tally = Tally::default();
    let built = {
        let physical = async {
            let tag = ScanTag {
                source_id: &source_id,
                kind,
            };
            let mut staging: Option<Covered> = record.as_deref().cloned();
            let mut yielded: Vec<Anchor> = Vec::new();
            for (n, ext) in extensions.iter().enumerate() {
                let r =
                    extend(segments.as_ref(), ext, &tag, &mut tally).await?;
                yielded.extend(r.session_anchor.clone());
                let size: usize = r.statements.iter().map(entry_bytes).sum();
                match staging.as_mut() {
                    None => {
                        staging = Some(Covered {
                            identity: identity.clone(),
                            lo: ext.from.clone(),
                            hi: ext.to.clone(),
                            statements: r.statements,
                            bytes: size,
                            frontier: r.frontier,
                        })
                    }
                    Some(c) if left_first && n == 0 => {
                        let mut s = r.statements;
                        s.append(&mut c.statements);
                        c.statements = s;
                        c.bytes += size;
                        c.lo = ext.from.clone();
                    }
                    Some(c) => {
                        c.statements.extend(r.statements);
                        c.bytes += size;
                        c.hi = ext.to.clone();
                        c.frontier = r.frontier;
                    }
                }
            }
            let staging =
                staging.expect("a round has a record or an extension");
            if staging.statements.len() > limits.max_statements
                || staging.bytes > limits.max_bytes
            {
                return Err(ProofError::Limit(format!(
                    "proof record bound ({} statements, {} bytes)",
                    staging.statements.len(),
                    staging.bytes
                )));
            }
            Ok((staging, yielded))
        };
        // The round runs while any waiter remains.
        let gone = async {
            for r in replies.iter_mut() {
                r.closed().await;
            }
        };
        tokio::select! {
            built = physical => Some(built),
            () = gone => None,
        }
    };
    drop(requests);
    let Some(built) = built else {
        return;
    };
    match built {
        Ok((staging, yielded)) => {
            tally.work.extensions_committed = extensions.len() as u32;
            let committed = Arc::new(staging);
            {
                let mut g = inner.lock().expect("scanner");
                g.record = Some(committed.clone());
                for a in yielded {
                    g.anchors.retain(|c| c.set_before != a.set_before);
                    g.anchors.push_back(a);
                    while g.anchors.len() > ANCHOR_CACHE {
                        g.anchors.pop_front();
                    }
                }
            }
            for reply in replies {
                let _ = reply.send(Ok(Served {
                    record: committed.clone(),
                    tally: tally.clone(),
                }));
            }
        }
        Err(e) => {
            observe_round_failure(&source_id, kind, &tally.work);
            for reply in replies {
                let _ = reply.send(Err(e.clone()));
            }
        }
    }
}

/// Errors after which a positioned attempt is followed by one bounded
/// unanchored attempt: the start was refused or the stream failed. An
/// identity, ordering, limit or framing refusal is not retried.
fn falls_back(e: &ProofError) -> bool {
    matches!(
        e,
        ProofError::AnchorMismatch(_)
            | ProofError::NotReached(_)
            | ProofError::Open(_)
            | ProofError::Read(_)
            | ProofError::SeekLimit(_)
    )
}

/// One extension: at most one positioned attempt, then at most one
/// unanchored one. Never a third.
async fn extend(
    segments: &dyn Segments,
    ext: &Extension,
    tag: &ScanTag<'_>,
    tally: &mut Tally,
) -> Result<ScanReport, ProofError> {
    let gtid = ext.from.gtid_set.is_some();
    let mut reason = "no_anchor";
    if let Some(a) = &ext.anchor {
        tally.work.positioned_attempts += 1;
        tally.work.stream_openings += 1;
        match segments.scan(&ext.from, &ext.to, Some(a), tag).await {
            Ok(r) => {
                tally.record(&r);
                return Ok(r);
            }
            Err(e) if falls_back(&e) => {
                tracing::warn!(
                    source_id = tag.source_id,
                    kind = tag.kind.as_str(),
                    error = %e,
                    "proof scan: the anchored start was refused; one bounded unanchored scan follows"
                );
                reason = "refused_anchor";
            }
            Err(e) => return Err(e),
        }
    }
    tally.work.stream_openings += 1;
    if gtid {
        tally.work.unanchored_attempts += 1;
        metrics::counter!(
            "deltaforge_mysql_proof_unanchored_total",
            "kind" => tag.kind.as_str(),
            "reason" => reason,
        )
        .increment(1);
    }
    let r = segments.scan(&ext.from, &ext.to, None, tag).await?;
    tally.record(&r);
    Ok(r)
}

impl Tally {
    fn record(&mut self, r: &ScanReport) {
        self.traces.push(r.trace.clone());
        self.events += r.events;
        self.bytes += r.bytes;
    }
}

/// Drop recorded statements at or before the lowest of `boundaries` and
/// every pin; a lowest point past `hi` clears the record. Unordered points
/// prune nothing.
fn prune(
    state: &mut Option<Arc<Covered>>,
    pins: &Pins,
    boundaries: &[MySqlCheckpoint],
) {
    let Some(current) = state.as_ref() else {
        return;
    };
    let mut points: Vec<(WmPos, MySqlCheckpoint)> = Vec::new();
    for p in pins.lock().expect("pins").values().chain(boundaries) {
        match wm(p) {
            Ok(w) => points.push((w, p.clone())),
            Err(_) => return,
        }
    }
    let Some((mut low, mut low_cp)) = points.first().cloned() else {
        return;
    };
    for (p, cp) in &points[1..] {
        match order_positions(p, &low) {
            CheckpointOrder::Before => (low, low_cp) = (p.clone(), cp.clone()),
            CheckpointOrder::Equal | CheckpointOrder::After => {}
            CheckpointOrder::Incomparable => return,
        }
    }
    let (Ok(lo), Ok(hi)) = (wm(&current.lo), wm(&current.hi)) else {
        *state = None;
        return;
    };
    match (order_positions(&low, &lo), order_positions(&low, &hi)) {
        (CheckpointOrder::Before | CheckpointOrder::Equal, _) => {}
        (_, CheckpointOrder::After) => *state = None,
        (
            CheckpointOrder::After,
            CheckpointOrder::Before | CheckpointOrder::Equal,
        ) => {
            let mut keep = Vec::with_capacity(current.statements.len());
            for s in &current.statements {
                match at_or_before(s, &low) {
                    Some(true) => {}
                    Some(false) => keep.push(s.clone()),
                    None => {
                        *state = None;
                        return;
                    }
                }
            }
            let mut c = Covered::clone(current);
            c.bytes = keep.iter().map(entry_bytes).sum();
            c.statements = keep;
            c.lo = MySqlCheckpoint {
                lineage: c.lo.lineage.clone(),
                ..low_cp
            };
            *state = Some(Arc::new(c));
        }
        _ => *state = None,
    }
}

/// A failed round's work, for its metrics; never fails.
fn observe_round_failure(source_id: &str, kind: ProofKind, work: &ProofWork) {
    metrics::counter!(
        "deltaforge_mysql_proof_rounds_failed_total",
        "source" => source_id.to_string(),
        "kind" => kind.as_str(),
    )
    .increment(1);
    tracing::debug!(
        source_id,
        kind = kind.as_str(),
        ?work,
        "proof round failed"
    );
}

/// Bounded metrics of a request (source, kind, how it was served) and the
/// record's size; the qualification record while the trace target is on.
/// Never fails.
#[allow(clippy::too_many_arguments)]
fn observe_request(
    tag: &ScanTag<'_>,
    from: &MySqlCheckpoint,
    to: &MySqlCheckpoint,
    served: &str,
    events: u64,
    bytes: u64,
    segments: &[ScanTrace],
    retained: Option<(usize, usize)>,
) {
    let labels = [
        ("source", tag.source_id.to_string()),
        ("kind", tag.kind.as_str().to_string()),
        ("served", served.to_string()),
    ];
    metrics::counter!("deltaforge_mysql_proof_requests_total", &labels)
        .increment(1);
    if let Some((statements, record_bytes)) = retained {
        let source = [("source", tag.source_id.to_string())];
        metrics::gauge!("deltaforge_mysql_proof_record_statements", &source)
            .set(statements as f64);
        metrics::gauge!("deltaforge_mysql_proof_record_bytes", &source)
            .set(record_bytes as f64);
    }
    if !trace_enabled() {
        return;
    }
    let record = serde_json::json!({
        "record": "request",
        "source": tag.source_id,
        "kind": tag.kind.as_str(),
        "served": served,
        "from": {"file": from.file, "pos": from.pos, "gtid_set": from.gtid_set},
        "to": {"file": to.file, "pos": to.pos, "gtid_set": to.gtid_set},
        "events": events,
        "bytes": bytes,
        "scan_ids": segments.iter().map(|t| t.scan_id).collect::<Vec<_>>(),
        "record_statements": retained.map(|r| r.0),
        "record_bytes": retained.map(|r| r.1),
    });
    tracing::info!(target: "deltaforge::proof_trace", "{record}");
}

/// A request on the source's shared scanner, with physical extensions on
/// its verified server. `anchor`: the stream anchor at `from`, if any.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn shared_interval(
    scanner: &ProofScanner,
    dsn: &str,
    source_id: &str,
    server_uuid: &str,
    lineage_hash: &str,
    from: &MySqlCheckpoint,
    to: &MySqlCheckpoint,
    anchor: Option<&Anchor>,
    boundary: Option<&MySqlCheckpoint>,
    tag: &ScanTag<'_>,
) -> Result<ScanReport, ProofError> {
    let mut answers = shared_intervals(
        scanner,
        dsn,
        source_id,
        server_uuid,
        lineage_hash,
        std::slice::from_ref(from),
        to,
        anchor,
        boundary,
        tag,
    )
    .await?;
    Ok(answers.pop().expect("one answer per start"))
}

/// A batch on the source's shared scanner: every `(from_i, to]`, from one
/// request (see [`ProofScanner::intervals`]).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn shared_intervals(
    scanner: &ProofScanner,
    dsn: &str,
    source_id: &str,
    server_uuid: &str,
    lineage_hash: &str,
    froms: &[MySqlCheckpoint],
    to: &MySqlCheckpoint,
    anchor: Option<&Anchor>,
    boundary: Option<&MySqlCheckpoint>,
    tag: &ScanTag<'_>,
) -> Result<Vec<ScanReport>, ProofError> {
    let segments = Arc::new(LiveSegments {
        dsn: dsn.to_string(),
        server_id: super::mysql_helpers::derive_server_id(&format!(
            "{source_id}/proofs"
        )),
        server_uuid: server_uuid.to_string(),
        lineage_hash: lineage_hash.to_string(),
        limits: ScanLimits::default(),
    });
    scanner
        .intervals(
            segments,
            server_uuid,
            lineage_hash,
            froms,
            to,
            anchor,
            boundary,
            tag,
        )
        .await
}

fn answer(
    a: &WmPos,
    b: &WmPos,
    statements: Vec<Statement>,
    events: u64,
    bytes: u64,
    started: std::time::Instant,
) -> ScanReport {
    ScanReport {
        digest: scan_digest(a, b, &statements),
        statements,
        events,
        bytes,
        duration: started.elapsed(),
        trace: Default::default(),
        frontier: None,
        session_anchor: None,
        positioned_at: None,
        work: ProofWork::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::super::mysql_binlog_scan::ProofKind;
    use super::super::mysql_ddl_attribution::{
        BarrierScopeOf, DdlEffect, TableName,
    };
    use super::*;
    use std::sync::Mutex;
    use tokio::sync::Notify;

    const U: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
    const L: &str = "0123456789abcdef0123456789abcdef";
    const TAG: ScanTag<'static> = ScanTag {
        source_id: "t",
        kind: ProofKind::Lazy,
    };

    fn table(t: &str) -> DdlEffect {
        DdlEffect::Tables(vec![TableName {
            db: "d".into(),
            table: t.into(),
        }])
    }

    /// A synthetic binlog: transaction k (1..) ends at position k and holds
    /// the statements `ddl[k]` (possibly several).
    /// An attempt: (from, to, the anchor's offset if positioned).
    type Attempt = (u64, u64, Option<u64>);
    /// Reached and release signals of a blocked scan.
    type Block = Option<(Arc<Notify>, Arc<Notify>)>;

    #[derive(Clone)]
    struct Synth {
        gtid: bool,
        ddl: BTreeMap<u64, Vec<DdlEffect>>,
        /// Physical segments scanned: (from, to) transaction numbers.
        scanned: Arc<Mutex<Vec<(u64, u64)>>>,
        /// Every attempt: (from, to, the anchor's offset if positioned).
        attempts: Arc<Mutex<Vec<Attempt>>>,
        fail_next: Arc<Mutex<bool>>,
        /// Attempts from this transaction fail, while set.
        fail_at: Arc<Mutex<Option<u64>>>,
        /// Attempts at an anchor fail with this, while set.
        refuse_anchors: Arc<Mutex<Option<ProofError>>>,
        /// Attempts without an anchor fail with this, while set.
        fail_unanchored: Arc<Mutex<Option<ProofError>>>,
        block: Arc<Mutex<Block>>,
    }

    impl Synth {
        fn new(gtid: bool) -> Self {
            Self {
                gtid,
                ddl: BTreeMap::new(),
                scanned: Default::default(),
                attempts: Default::default(),
                fail_next: Default::default(),
                fail_at: Default::default(),
                refuse_anchors: Default::default(),
                fail_unanchored: Default::default(),
                block: Default::default(),
            }
        }

        fn seg(&self) -> Arc<dyn Segments> {
            Arc::new(self.clone())
        }

        /// The boundary after transaction `k`, as a scan observes it.
        fn anchor(&self, k: u64, next: bool) -> Anchor {
            let p = self.at(k);
            Anchor {
                server_uuid: U.into(),
                lineage: L.into(),
                file: p.file.clone(),
                offset: p.pos,
                set_before: GtidSet::parse(p.gtid_set.as_deref().unwrap())
                    .unwrap(),
                next_gtid: next.then(|| format!("{U}:{}", k + 1)),
            }
        }

        fn with(mut self, k: u64, effects: Vec<DdlEffect>) -> Self {
            self.ddl.insert(k, effects);
            self
        }

        fn at(&self, k: u64) -> MySqlCheckpoint {
            MySqlCheckpoint {
                file: "binlog.000001".into(),
                pos: 4 + 100 * k,
                gtid_set: self.gtid.then(|| {
                    if k == 0 {
                        String::new()
                    } else {
                        format!("{U}:1-{k}")
                    }
                }),
                lineage: Some(L.into()),
                snapshot_completed: None,
                snapshot_chain: None,
            }
        }

        fn number(&self, p: &MySqlCheckpoint) -> u64 {
            if self.gtid {
                let g = p.gtid_set.clone().unwrap_or_default();
                g.rsplit_once('-')
                    .map(|(_, n)| n.parse().unwrap())
                    .unwrap_or(0)
            } else {
                (p.pos - 4) / 100
            }
        }

        fn statements(&self, from: u64, to: u64) -> Vec<Statement> {
            let mut out = Vec::new();
            if from >= to {
                return out;
            }
            for (k, effects) in self.ddl.range(from + 1..=to) {
                for (j, effect) in effects.iter().enumerate() {
                    let event = if self.gtid {
                        EventIdentity::Gtid {
                            gtid: format!("{U}:{k}"),
                            ordinal: j as u32 + 1,
                        }
                    } else {
                        EventIdentity::FilePos {
                            file: "binlog.000001".into(),
                            end_pos: 4 + 100 * (k - 1) + 10 * (j as u64 + 1),
                            ordinal: 0,
                        }
                    };
                    out.push(Statement {
                        event,
                        effect: effect.clone(),
                    });
                }
            }
            out
        }

        /// What one direct scan of (from, to] returns.
        fn direct(&self, from: u64, to: u64) -> (Vec<Statement>, String) {
            let s = self.statements(from, to);
            let d = scan_digest(
                &wm(&self.at(from)).unwrap(),
                &wm(&self.at(to)).unwrap(),
                &s,
            );
            (s, d)
        }

        fn work(&self) -> u64 {
            self.scanned
                .lock()
                .unwrap()
                .iter()
                .map(|(a, b)| b - a)
                .sum()
        }
    }

    #[async_trait]
    impl Segments for Synth {
        async fn scan(
            &self,
            from: &MySqlCheckpoint,
            to: &MySqlCheckpoint,
            anchor: Option<&Anchor>,
            _tag: &ScanTag<'_>,
        ) -> Result<ScanReport, ProofError> {
            let block = self.block.lock().unwrap().clone();
            if let Some((reached, release)) = block {
                reached.notify_one();
                release.notified().await;
            }
            let (a, b) = (self.number(from), self.number(to));
            self.attempts.lock().unwrap().push((
                a,
                b,
                anchor.map(|x| x.offset),
            ));
            if std::mem::take(&mut *self.fail_next.lock().unwrap())
                || *self.fail_at.lock().unwrap() == Some(a)
            {
                return Err(ProofError::Open("purged".into()));
            }
            if let Some(a) = anchor {
                // An anchor is only ever one taken at the start.
                assert_eq!(a.offset, from.pos, "an anchor of another boundary");
                if let Some(e) = self.refuse_anchors.lock().unwrap().clone() {
                    return Err(e);
                }
            } else if let Some(e) = self.fail_unanchored.lock().unwrap().clone()
            {
                return Err(e);
            }
            self.scanned.lock().unwrap().push((a, b));
            Ok(ScanReport {
                digest: String::new(),
                statements: self.statements(a, b),
                events: b - a,
                bytes: 100 * (b - a),
                duration: Default::default(),
                trace: Default::default(),
                frontier: self.gtid.then(|| self.anchor(b, false)),
                session_anchor: self.gtid.then(|| self.anchor(a, true)),
                positioned_at: anchor.map(|x| (x.file.clone(), x.offset)),
                work: Default::default(),
            })
        }
    }

    async fn ask(
        sc: &ProofScanner,
        syn: &Synth,
        a: u64,
        b: u64,
        boundary: Option<u64>,
    ) -> Result<ScanReport, ProofError> {
        let bd = boundary.map(|k| syn.at(k));
        sc.interval(
            syn.seg(),
            U,
            L,
            &syn.at(a),
            &syn.at(b),
            None,
            bd.as_ref(),
            &TAG,
        )
        .await
    }

    async fn same_as_direct(sc: &ProofScanner, syn: &Synth, a: u64, b: u64) {
        let r = ask(sc, syn, a, b, Some(a)).await.unwrap();
        let (s, d) = syn.direct(a, b);
        assert_eq!(r.statements, s, "({a}, {b}]");
        assert_eq!(r.digest, d, "({a}, {b}]");
    }

    fn modes() -> [bool; 2] {
        [true, false]
    }

    #[tokio::test]
    async fn an_empty_interval_is_the_canonical_empty_proof_without_a_scan() {
        for gtid in modes() {
            let syn = Synth::new(gtid);
            let sc = ProofScanner::default();
            let r = ask(&sc, &syn, 7, 7, None).await.unwrap();
            assert!(r.statements.is_empty());
            assert_eq!(r.digest, syn.direct(7, 7).1);
            assert!(syn.scanned.lock().unwrap().is_empty());
            assert!(matches!(
                ask(&sc, &syn, 9, 7, None).await,
                Err(ProofError::Positions(_))
            ));
        }
    }

    #[tokio::test]
    async fn unordered_positions_fail_closed() {
        let (g, f) = (Synth::new(true), Synth::new(false));
        let sc = ProofScanner::default();
        let r = sc
            .interval(g.seg(), U, L, &g.at(1), &f.at(5), None, None, &TAG)
            .await;
        assert!(matches!(r, Err(ProofError::Positions(_))), "{r:?}");
        let r = sc
            .interval(g.seg(), U, "other", &g.at(1), &g.at(5), None, None, &TAG)
            .await;
        assert!(matches!(r, Err(ProofError::Positions(_))), "{r:?}");
    }

    /// Nested and overlapping requests (a new table at each stream position,
    /// each up to the live end) equal their direct scans and read the
    /// distinct interval once; DDL at the beginning, middle and end of the
    /// intervals, barriers, a rename and many statements under one GTID
    /// land exactly in the right proofs.
    #[tokio::test]
    async fn nested_requests_share_one_scan_and_equal_direct_scans() {
        for gtid in modes() {
            let rename = DdlEffect::Tables(vec![
                TableName {
                    db: "d".into(),
                    table: "old".into(),
                },
                TableName {
                    db: "d".into(),
                    table: "new".into(),
                },
            ]);
            let syn = Synth::new(gtid)
                .with(1, vec![table("a")])
                .with(50, vec![table("b"), table("c"), rename])
                .with(
                    51,
                    vec![DdlEffect::Barrier(BarrierScopeOf::Database(
                        "d".into(),
                    ))],
                )
                .with(99, vec![DdlEffect::Barrier(BarrierScopeOf::Lineage)])
                .with(100, vec![table("z")]);
            let sc = ProofScanner::default();
            let n = 100;
            for i in 0..n {
                // Table i first seen at stream position i, live end 10 later.
                same_as_direct(&sc, &syn, i, (i + 10).min(n)).await;
            }
            assert!(syn.work() <= n, "{gtid}: read {} for {n}", syn.work());
            // A request inside the (pruned) record reads nothing; one
            // behind it extends only the missing part.
            let before = syn.work();
            same_as_direct(&sc, &syn, 99, 100).await;
            assert_eq!(syn.work(), before);
            same_as_direct(&sc, &syn, 95, 100).await;
            assert_eq!(syn.work(), before + 4);
        }
    }

    /// The stream's boundary passing `hi` clears the record: the next
    /// request starts at its own `A`, never scanning (hi, A].
    #[tokio::test]
    async fn the_stream_passing_the_frontier_restarts_the_record() {
        for gtid in modes() {
            let syn = Synth::new(gtid).with(30, vec![table("x")]);
            let sc = ProofScanner::default();
            ask(&sc, &syn, 0, 10, Some(0)).await.unwrap();
            let r = ask(&sc, &syn, 40, 50, Some(40)).await.unwrap();
            assert_eq!(r.statements, syn.direct(40, 50).0);
            assert_eq!(*syn.scanned.lock().unwrap(), vec![(0, 10), (40, 50)]);
            // Pruning below the frontier keeps only what is after it.
            ask(&sc, &syn, 45, 50, Some(45)).await.unwrap();
            assert_eq!(sc.retained().await.map(|r| r.0), Some(0));
        }
    }

    /// A queued request's `A` is pinned before it waits: a request ahead
    /// of it, pruning at a later stream boundary, keeps what it needs.
    #[tokio::test]
    async fn a_queued_request_is_protected_from_pruning() {
        for gtid in modes() {
            let syn = Arc::new(Synth::new(gtid).with(35, vec![table("q")]));
            let sc = Arc::new(ProofScanner::default());
            ask(&sc, &syn, 10, 100, Some(10)).await.unwrap();
            // A request holding the scanner (blocked inside its extension).
            let (reached, release) =
                (Arc::new(Notify::new()), Arc::new(Notify::new()));
            *syn.block.lock().unwrap() =
                Some((reached.clone(), release.clone()));
            let r0 = tokio::spawn({
                let (sc, syn) = (sc.clone(), syn.clone());
                async move { ask(&sc, &syn, 100, 120, Some(10)).await }
            });
            reached.notified().await;
            *syn.block.lock().unwrap() = None;
            // r1 prunes at boundary 80; r2, queued behind it, needs (30, 40].
            let r1 = tokio::spawn({
                let (sc, syn) = (sc.clone(), syn.clone());
                async move { ask(&sc, &syn, 80, 90, Some(80)).await }
            });
            tokio::task::yield_now().await;
            let r2 = tokio::spawn({
                let (sc, syn) = (sc.clone(), syn.clone());
                async move { ask(&sc, &syn, 30, 40, Some(30)).await }
            });
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            assert_eq!(sc.pinned(), 3);
            release.notify_one();
            r0.await.unwrap().unwrap();
            r1.await.unwrap().unwrap();
            let r = r2.await.unwrap().unwrap();
            assert_eq!(r.statements, syn.direct(30, 40).0);
            assert_eq!(r.events, 0, "answered from the record");
            assert_eq!(sc.pinned(), 0);
        }
    }

    /// Cancelled while queued: its pin goes. Cancelled inside its
    /// extension: the record is as it was, and the next request is right.
    #[tokio::test]
    async fn cancellation_before_and_after_acquiring_the_scanner() {
        for gtid in modes() {
            let syn = Arc::new(Synth::new(gtid).with(15, vec![table("c")]));
            let sc = Arc::new(ProofScanner::default());
            ask(&sc, &syn, 0, 10, Some(0)).await.unwrap();
            let (reached, release) =
                (Arc::new(Notify::new()), Arc::new(Notify::new()));
            *syn.block.lock().unwrap() = Some((reached.clone(), release));
            let running = tokio::spawn({
                let (sc, syn) = (sc.clone(), syn.clone());
                async move { ask(&sc, &syn, 5, 20, Some(5)).await }
            });
            reached.notified().await;
            let queued = tokio::spawn({
                let (sc, syn) = (sc.clone(), syn.clone());
                async move { ask(&sc, &syn, 6, 30, Some(6)).await }
            });
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            assert_eq!(sc.pinned(), 2);
            queued.abort();
            let _ = queued.await;
            assert_eq!(sc.pinned(), 1, "the queued pin is removed");
            running.abort();
            let _ = running.await;
            assert_eq!(sc.pinned(), 0);
            *syn.block.lock().unwrap() = None;
            assert_eq!(sc.retained().await, Some((0, 0)), "record unchanged");
            same_as_direct(&sc, &syn, 5, 20).await;
        }
    }

    async fn ask_at(
        sc: &ProofScanner,
        syn: &Synth,
        a: u64,
        b: u64,
        boundary: Option<u64>,
        anchor: Option<&Anchor>,
    ) -> Result<ScanReport, ProofError> {
        let bd = boundary.map(|k| syn.at(k));
        sc.interval(
            syn.seg(),
            U,
            L,
            &syn.at(a),
            &syn.at(b),
            anchor,
            bd.as_ref(),
            &TAG,
        )
        .await
    }

    fn work(
        extensions_committed: u32,
        positioned_attempts: u32,
        unanchored_attempts: u32,
        stream_openings: u32,
    ) -> ProofWork {
        ProofWork {
            extensions_committed,
            positioned_attempts,
            unanchored_attempts,
            stream_openings,
        }
    }

    /// No partial scanner state: a round that fails - its right extension
    /// after its left one succeeded, its left one, or every attempt - leaves
    /// the record, its frontier and the anchor cache exactly as they were,
    /// and the next request equals a direct scan.
    #[tokio::test]
    async fn a_failed_round_changes_nothing() {
        for gtid in modes() {
            let syn = Synth::new(gtid)
                .with(8, vec![table("f")])
                .with(25, vec![table("g")]);
            let sc = ProofScanner::default();
            ask(&sc, &syn, 10, 20, Some(10)).await.unwrap();
            let before = sc.snapshot();
            let fail = |e: Option<ProofError>| {
                *syn.refuse_anchors.lock().unwrap() = e.clone();
                *syn.fail_unanchored.lock().unwrap() = e;
            };
            // The right extension fails after the left one succeeded.
            *syn.fail_at.lock().unwrap() = Some(20);
            let r = ask(&sc, &syn, 5, 30, Some(5)).await;
            assert!(matches!(r, Err(ProofError::Open(_))), "{r:?}");
            assert!(sc.snapshot() == before, "{gtid}: right failed");
            *syn.fail_at.lock().unwrap() = None;
            // The left extension fails.
            *syn.fail_at.lock().unwrap() = Some(5);
            assert!(ask(&sc, &syn, 5, 30, Some(5)).await.is_err());
            assert!(sc.snapshot() == before, "{gtid}: left failed");
            *syn.fail_at.lock().unwrap() = None;
            // Every attempt fails: at most two per extension, and the round
            // stops at its first failed extension.
            // With a stream anchor (GTID mode), the positioned attempt and
            // one unanchored attempt; never a third.
            fail(Some(ProofError::Open("down".into())));
            let tried = syn.attempts.lock().unwrap().len();
            let anchor = gtid.then(|| syn.anchor(5, true));
            assert!(
                ask_at(&sc, &syn, 5, 30, Some(5), anchor.as_ref())
                    .await
                    .is_err()
            );
            let made = syn.attempts.lock().unwrap().len() - tried;
            assert_eq!(made, if gtid { 2 } else { 1 }, "{gtid}");
            assert!(sc.snapshot() == before, "{gtid}: all failed");
            fail(None);
            same_as_direct(&sc, &syn, 5, 30).await;
        }
    }

    /// GTID mode, the normal path: the first request (no anchor) makes one
    /// bounded unanchored attempt; every later extension to the right is
    /// positioned at the record's frontier; a request at a boundary with a
    /// stream anchor extends to the left from it.
    #[tokio::test]
    async fn extensions_are_positioned_at_their_anchors() {
        let syn = Synth::new(true).with(15, vec![table("p")]);
        let sc = ProofScanner::default();
        let r = ask(&sc, &syn, 10, 20, Some(10)).await.unwrap();
        assert_eq!(r.work, work(1, 0, 1, 1));
        let r = ask(&sc, &syn, 12, 30, Some(12)).await.unwrap();
        assert_eq!(r.work, work(1, 1, 0, 1));
        assert_eq!(
            syn.attempts.lock().unwrap().last(),
            Some(&(20, 30, Some(syn.at(20).pos)))
        );
        // Left (with the stream anchor at 5) and right in one round.
        let r = ask_at(&sc, &syn, 5, 40, Some(5), Some(&syn.anchor(5, true)))
            .await
            .unwrap();
        assert_eq!(r.work, work(2, 2, 0, 2));
        assert_eq!(r.statements, syn.direct(5, 40).0);
        assert_eq!(r.digest, syn.direct(5, 40).1);
        // Answered from the record: no work.
        let r = ask(&sc, &syn, 6, 40, Some(5)).await.unwrap();
        assert_eq!(r.work, ProofWork::default());
    }

    /// A refused anchor costs exactly one bounded unanchored attempt; with
    /// both extensions' anchors refused, a round makes at most 2
    /// positioned and 2 unanchored attempts and 4 stream openings, and its
    /// answers equal direct scans.
    #[tokio::test]
    async fn refused_anchors_are_bounded_per_extension() {
        let syn = Synth::new(true)
            .with(15, vec![table("p")])
            .with(3, vec![table("q")]);
        let sc = ProofScanner::default();
        ask(&sc, &syn, 10, 20, Some(10)).await.unwrap();
        *syn.refuse_anchors.lock().unwrap() =
            Some(ProofError::AnchorMismatch("refused".into()));
        let r = ask(&sc, &syn, 12, 30, Some(12)).await.unwrap();
        assert_eq!(r.work, work(1, 1, 1, 2));
        assert_eq!(r.statements, syn.direct(12, 30).0);
        let r = ask_at(&sc, &syn, 2, 40, Some(2), Some(&syn.anchor(2, true)))
            .await
            .unwrap();
        assert_eq!(r.work, work(2, 2, 2, 4));
        assert_eq!(r.digest, syn.direct(2, 40).1);
        // A refusal that is not the start's (another server): no fallback.
        *syn.refuse_anchors.lock().unwrap() =
            Some(ProofError::OtherServer("x".into()));
        let tried = syn.attempts.lock().unwrap().len();
        let r = ask(&sc, &syn, 30, 50, Some(30)).await;
        assert!(matches!(r, Err(ProofError::OtherServer(_))), "{r:?}");
        assert_eq!(syn.attempts.lock().unwrap().len() - tried, 1);
    }

    /// The bounded alternative's yield: a session anchor at the boundary it
    /// started from, which positions a later request at that boundary.
    #[tokio::test]
    async fn an_unanchored_scan_yields_a_session_anchor() {
        let syn = Synth::new(true);
        let sc = ProofScanner::default();
        let r = ask(&sc, &syn, 40, 50, Some(40)).await.unwrap();
        assert_eq!(r.work, work(1, 0, 1, 1));
        // The stream moves on: the record is pruned to 45.
        ask(&sc, &syn, 45, 60, Some(45)).await.unwrap();
        // A request at 40 again (a snapshot baseline, say): positioned.
        let r = ask(&sc, &syn, 40, 60, Some(40)).await.unwrap();
        assert_eq!(r.work, work(1, 1, 0, 1));
        assert_eq!(
            syn.attempts.lock().unwrap().last(),
            Some(&(40, 45, Some(syn.at(40).pos)))
        );
        // Only an anchor at exactly the start's set is used.
        let r = ask(&sc, &syn, 41, 60, Some(41)).await.unwrap();
        assert_eq!(r.work, ProofWork::default(), "covered");
        let r = ask(&sc, &syn, 39, 60, Some(39)).await.unwrap();
        assert_eq!(r.work, work(1, 0, 1, 1));
    }

    /// A request queued while a round runs is served by exactly the next
    /// round, with every other request queued meanwhile: one extension to
    /// their greatest end, one answer each, equal to direct scans.
    #[tokio::test]
    async fn a_request_queued_during_a_round_joins_exactly_the_next_round() {
        for gtid in modes() {
            let syn = Arc::new(
                Synth::new(gtid)
                    .with(7, vec![table("a")])
                    .with(25, vec![table("b")])
                    .with(35, vec![table("c")]),
            );
            let sc = Arc::new(ProofScanner::default());
            let (reached, release) =
                (Arc::new(Notify::new()), Arc::new(Notify::new()));
            *syn.block.lock().unwrap() =
                Some((reached.clone(), release.clone()));
            let r0 = tokio::spawn({
                let (sc, syn) = (sc.clone(), syn.clone());
                async move { ask(&sc, &syn, 0, 10, Some(0)).await }
            });
            reached.notified().await;
            *syn.block.lock().unwrap() = None;
            let queued: Vec<_> = [(5, 30), (8, 40), (2, 20)]
                .into_iter()
                .map(|(a, b)| {
                    let (sc, syn) = (sc.clone(), syn.clone());
                    tokio::spawn(async move {
                        (a, b, ask(&sc, &syn, a, b, Some(a)).await)
                    })
                })
                .collect();
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            release.notify_one();
            r0.await.unwrap().unwrap();
            let mut works = Vec::new();
            for q in queued {
                let (a, b, r) = q.await.unwrap();
                let r = r.unwrap();
                assert_eq!((r.statements, r.digest), syn.direct(a, b));
                works.push(r.work);
            }
            assert_eq!(
                *syn.scanned.lock().unwrap(),
                vec![(0, 10), (10, 40)],
                "{gtid}"
            );
            assert!(works.iter().all(|w| *w == works[0]), "{works:?}");
            assert_eq!(works[0].extensions_committed, 1);
        }
    }

    /// A batch (one window's tables, each from its own start): one request,
    /// one extension, an answer per start identical to a direct scan.
    #[tokio::test]
    async fn a_batch_is_one_request_with_an_answer_per_start() {
        for gtid in modes() {
            let syn = Synth::new(gtid)
                .with(2, vec![table("a")])
                .with(5, vec![table("b")])
                .with(9, vec![table("c")]);
            let sc = ProofScanner::default();
            let starts = [3, 1, 7, 9];
            let froms: Vec<_> = starts.iter().map(|&k| syn.at(k)).collect();
            let answers = sc
                .intervals(
                    syn.seg(),
                    U,
                    L,
                    &froms,
                    &syn.at(9),
                    None,
                    Some(&syn.at(1)),
                    &TAG,
                )
                .await
                .unwrap();
            assert_eq!(answers.len(), starts.len());
            for (k, r) in starts.iter().zip(&answers) {
                assert_eq!(
                    (r.statements.clone(), r.digest.clone()),
                    syn.direct(*k, 9)
                );
            }
            assert_eq!(*syn.scanned.lock().unwrap(), vec![(1, 9)]);
            assert_eq!(sc.pinned(), 0);
        }
    }

    /// Shared work survives a cancelled waiter: the round continues for the
    /// waiter that remains, and commits.
    #[tokio::test]
    async fn a_round_continues_while_any_waiter_remains() {
        for gtid in modes() {
            let syn = Arc::new(Synth::new(gtid).with(15, vec![table("w")]));
            let sc = Arc::new(ProofScanner::default());
            ask(&sc, &syn, 0, 10, Some(0)).await.unwrap();
            let (reached, release) =
                (Arc::new(Notify::new()), Arc::new(Notify::new()));
            // Hold the scanner in a round, so the next two share a round.
            *syn.block.lock().unwrap() =
                Some((reached.clone(), release.clone()));
            let hold = tokio::spawn({
                let (sc, syn) = (sc.clone(), syn.clone());
                async move { ask(&sc, &syn, 1, 11, Some(1)).await }
            });
            reached.notified().await;
            let leaves = tokio::spawn({
                let (sc, syn) = (sc.clone(), syn.clone());
                async move { ask(&sc, &syn, 2, 20, Some(2)).await }
            });
            let stays = tokio::spawn({
                let (sc, syn) = (sc.clone(), syn.clone());
                async move { ask(&sc, &syn, 3, 20, Some(3)).await }
            });
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            release.notify_one();
            hold.await.unwrap().unwrap();
            // Their round is now blocked in its extension.
            reached.notified().await;
            leaves.abort();
            let _ = leaves.await;
            *syn.block.lock().unwrap() = None;
            release.notify_one();
            let r = stays.await.unwrap().unwrap();
            assert_eq!((r.statements, r.digest), syn.direct(3, 20));
            assert_eq!(sc.snapshot().unwrap().1, syn.at(20));
        }
    }

    /// A FULL-fallback request at the live end (its capture interval past
    /// the frontier) extends only to the right: never to the left. The
    /// extension covers the gap up to the capture, which the stream's later
    /// proofs then find recorded.
    #[tokio::test]
    async fn a_request_at_the_live_end_extends_only_to_the_right() {
        for gtid in modes() {
            let syn = Synth::new(gtid).with(55, vec![table("g")]);
            let sc = ProofScanner::default();
            ask(&sc, &syn, 0, 50, Some(0)).await.unwrap();
            // The capture interval (60, 61], the stream at 10.
            let r = ask(&sc, &syn, 60, 61, Some(10)).await.unwrap();
            assert_eq!(r.statements, syn.direct(60, 61).0);
            // A later stream proof (12, 61]: answered from the record.
            let r = ask(&sc, &syn, 12, 61, Some(12)).await.unwrap();
            assert_eq!(r.statements, syn.direct(12, 61).0);
            assert_eq!(r.events, 0);
            assert_eq!(*syn.scanned.lock().unwrap(), vec![(0, 50), (50, 61)]);
        }
    }

    /// GTID mode: a position whose GTID set is the one after transaction
    /// `g` and whose file offset is the one after transaction `pos` (the
    /// two are not read atomically by the server).
    fn mixed(syn: &Synth, g: u64, pos: u64) -> MySqlCheckpoint {
        MySqlCheckpoint {
            pos: syn.at(pos).pos,
            ..syn.at(g)
        }
    }

    /// GTID mode: the GTID set is authoritative, the file offset only
    /// diagnostic. The same offset with one more transaction is a real,
    /// non-empty interval; the same set at a later offset is empty; sets
    /// that do not include each other are refused.
    #[tokio::test]
    async fn gtid_sets_decide_intervals_not_file_offsets() {
        let syn = Synth::new(true).with(5, vec![table("x")]);
        let sc = ProofScanner::default();
        // (1) Same offset (after 4), B holds transaction 5.
        let r = sc
            .interval(
                syn.seg(),
                U,
                L,
                &syn.at(4),
                &mixed(&syn, 5, 4),
                None,
                None,
                &TAG,
            )
            .await
            .unwrap();
        assert_eq!(r.statements, syn.direct(4, 5).0);
        assert_eq!(r.digest, syn.direct(4, 5).1);
        assert_eq!(*syn.scanned.lock().unwrap(), vec![(4, 5)]);
        // (2) Same set, a later offset: empty, no scan.
        let r = sc
            .interval(
                syn.seg(),
                U,
                L,
                &syn.at(5),
                &mixed(&syn, 5, 9),
                None,
                None,
                &TAG,
            )
            .await
            .unwrap();
        assert!(r.statements.is_empty());
        assert_eq!(syn.scanned.lock().unwrap().len(), 1);
        // (3) Sets of different servers: refused.
        let other = MySqlCheckpoint {
            gtid_set: Some("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-9".into()),
            ..syn.at(9)
        };
        let r = sc
            .interval(syn.seg(), U, L, &syn.at(4), &other, None, None, &TAG)
            .await;
        assert!(matches!(r, Err(ProofError::Positions(_))), "{r:?}");
        // A GTID position against a file position: refused.
        let f = Synth::new(false);
        let r = sc
            .interval(syn.seg(), U, L, &syn.at(4), &f.at(9), None, None, &TAG)
            .await;
        assert!(matches!(r, Err(ProofError::Positions(_))), "{r:?}");
    }

    /// Pruning and coverage follow the authoritative coordinate: in GTID
    /// mode a boundary or a request start whose file offset is far ahead
    /// of its GTID set is placed by its set; in file mode by its offset.
    #[tokio::test]
    async fn pruning_and_coverage_use_the_authoritative_coordinate() {
        let syn = Synth::new(true)
            .with(10, vec![table("a")])
            .with(30, vec![table("b")]);
        let sc = ProofScanner::default();
        ask(&sc, &syn, 0, 40, Some(0)).await.unwrap();
        // The boundary's set is after 5, its offset after 35: prune by the
        // set, so the statement at 10 stays recorded.
        let r = sc
            .interval(
                syn.seg(),
                U,
                L,
                &mixed(&syn, 5, 35),
                &syn.at(40),
                None,
                Some(&mixed(&syn, 5, 35)),
                &TAG,
            )
            .await
            .unwrap();
        assert_eq!(r.statements, syn.direct(5, 40).0);
        assert_eq!(r.events, 0, "covered: answered from the record");
        assert_eq!(sc.retained().await.map(|r| r.0), Some(2));

        let syn = Synth::new(false)
            .with(10, vec![table("a")])
            .with(30, vec![table("b")]);
        let sc = ProofScanner::default();
        ask(&sc, &syn, 0, 40, Some(0)).await.unwrap();
        // File mode: the offset decides (a stray GTID set is not consulted:
        // file positions carry none).
        ask(&sc, &syn, 20, 40, Some(20)).await.unwrap();
        assert_eq!(sc.retained().await.map(|r| r.0), Some(1));
    }

    /// Another server identity or lineage never reuses the record.
    #[tokio::test]
    async fn another_identity_starts_a_new_record() {
        let syn = Synth::new(true).with(3, vec![table("i")]);
        let sc = ProofScanner::default();
        ask(&sc, &syn, 0, 10, None).await.unwrap();
        let other = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
        sc.interval(
            syn.seg(),
            other,
            L,
            &syn.at(0),
            &syn.at(10),
            None,
            None,
            &TAG,
        )
        .await
        .unwrap();
        assert_eq!(*syn.scanned.lock().unwrap(), vec![(0, 10), (0, 10)]);
    }

    /// The record's statement and byte bounds fail closed and clear it; a
    /// DDL naming many tables counts by its size; pruning keeps many small
    /// extensions within bounds.
    #[tokio::test]
    async fn record_bounds_fail_closed() {
        for gtid in modes() {
            let many = DdlEffect::Tables(
                (0..1000)
                    .map(|i| TableName {
                        db: "d".into(),
                        table: format!("t{i}"),
                    })
                    .collect(),
            );
            let syn = Synth::new(gtid).with(2, vec![many]);
            let small = ProofScanner::new(RecordLimits {
                max_statements: 100,
                max_bytes: 4096,
            });
            assert!(matches!(
                ask(&small, &syn, 0, 5, None).await,
                Err(ProofError::Limit(_))
            ));
            assert_eq!(small.retained().await, None);
            let big = ProofScanner::default();
            same_as_direct(&big, &syn, 0, 5).await;

            let mut syn = Synth::new(gtid);
            for k in 1..=50 {
                syn = syn.with(k, vec![table(&format!("s{k}"))]);
            }
            let few = ProofScanner::new(RecordLimits {
                max_statements: 10,
                max_bytes: 1 << 20,
            });
            for k in 1..=50 {
                // Many small extensions, the stream following: the record
                // stays small.
                same_as_direct(&few, &syn, k - 1, k).await;
            }
            assert!(few.retained().await.unwrap().0 <= 1);
            let no_prune = ProofScanner::new(RecordLimits {
                max_statements: 10,
                max_bytes: 1 << 20,
            });
            let mut failed = false;
            for k in 1..=50 {
                if ask(&no_prune, &syn, 0, k, None).await.is_err() {
                    failed = true;
                    break;
                }
            }
            assert!(failed, "an unpruned record past its bound fails closed");
        }
    }
}
