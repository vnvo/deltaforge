//! One proof scanner per MySQL source: a shared, ordered record of the
//! classified statements (DDL, database and lineage barriers - never DML)
//! of one contiguous covered interval `(lo, hi]` of the source's binlog.
//!
//! A proof asks for its interval `(A, B]`. When `(lo, hi]` covers it, the
//! answer comes from the record; otherwise the record is extended by a
//! physical scan of only the missing part - `(hi, B]` to the right, `(A, lo]`
//! to the left - with the existing [`scan_interval`] (identity proven on its
//! session, complete, exact, fail-closed). Each proof still applies its own
//! `affects` and `covers` rules to the statements of its own interval, and
//! its digest is the canonical [`scan_digest`] of `(A, B]` over them, so a
//! table first seen at a position the record already covers costs no
//! rescan of the history between them.
//!
//! - `A == B`: the canonical empty proof, no session; `B < A` or unordered
//!   positions fail closed.
//! - Requests are single-flight (one mutex). A request pins its `A` before
//!   it waits, so pruning never removes what a queued request needs; the pin
//!   is removed when the request ends or is dropped.
//! - An extension's result is applied only once it completed exactly: a
//!   cancelled one leaves the record as it was; a failed one (gap, purged
//!   binlog, framing, limit, another server) clears the record.
//! - The record belongs to one server identity and lineage; another one
//!   clears it.
//! - Pruning (at each request) drops statements at or before the lowest of
//!   the stream's boundary and every pin; a boundary past `hi` clears the
//!   record (it restarts at the next request's `A`).
//! - Retained state is bounded by statement count and encoded bytes;
//!   exceeding either clears the record and fails the request closed.
//!
//! Operational semantics: the existing scan limits bound each physical
//! extension (the work done); answering from the record re-reads no DML, so
//! a request is no longer limited by the size of its whole logical interval.
//! The record's own bounds limit retained state.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use deltaforge_core::CheckpointOrder;

use super::MySqlCheckpoint;
use super::mysql_activation::EventIdentity;
use super::mysql_binlog_scan::{
    ProofError, ScanLimits, ScanReport, ScanTag, ScanTrace, Statement,
    encode_effect, encode_event, scan_digest, scan_interval, trace_enabled,
};
use crate::durable_checkpoint::{
    WmPos, gtid_subseteq, mysql_checkpoint_position, order_positions,
    parse_gtid_set,
};

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

/// A physical scan of `(from, to]` (production: [`LiveSegments`]).
#[async_trait]
pub(crate) trait Segments: Send + Sync {
    async fn scan(
        &self,
        from: &MySqlCheckpoint,
        to: &MySqlCheckpoint,
        tag: &ScanTag<'_>,
    ) -> Result<ScanReport, ProofError>;
}

/// Physical scans on the source's verified server.
pub(crate) struct LiveSegments<'a> {
    pub dsn: &'a str,
    pub server_id: u64,
    pub server_uuid: &'a str,
    pub lineage_hash: &'a str,
    pub limits: ScanLimits,
}

#[async_trait]
impl Segments for LiveSegments<'_> {
    async fn scan(
        &self,
        from: &MySqlCheckpoint,
        to: &MySqlCheckpoint,
        tag: &ScanTag<'_>,
    ) -> Result<ScanReport, ProofError> {
        scan_interval(
            self.dsn,
            self.server_id,
            self.server_uuid,
            self.lineage_hash,
            from,
            to,
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

struct Covered {
    identity: Identity,
    lo: MySqlCheckpoint,
    hi: MySqlCheckpoint,
    statements: Vec<Statement>,
    bytes: usize,
}

/// The shared proof scanner of one source.
pub(crate) struct ProofScanner {
    state: tokio::sync::Mutex<Option<Covered>>,
    pins: Arc<std::sync::Mutex<BTreeMap<u64, MySqlCheckpoint>>>,
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
    pins: Arc<std::sync::Mutex<BTreeMap<u64, MySqlCheckpoint>>>,
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

fn entry_bytes(s: &Statement) -> usize {
    encode_event(&s.event).len() + encode_effect(&s.effect).len() + 64
}

/// Whether statement `s` lies at or before position `p`.
fn at_or_before(s: &Statement, p: &WmPos) -> Option<bool> {
    match (&s.event, p) {
        (EventIdentity::Gtid { gtid, .. }, WmPos::MysqlGtid { gtid_set }) => {
            let set = parse_gtid_set(gtid_set)?;
            let one = parse_gtid_set(gtid)?;
            Some(gtid_subseteq(&one, &set))
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

impl ProofScanner {
    pub(crate) fn new(limits: RecordLimits) -> Self {
        Self {
            state: tokio::sync::Mutex::new(None),
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

    /// The statements of `(from, to]` and its canonical digest. `boundary`:
    /// the stream's last transaction boundary (pruning). `events`/`bytes`
    /// report the physical work this request did (0 when answered from the
    /// record).
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn interval(
        &self,
        segments: &dyn Segments,
        server_uuid: &str,
        lineage_hash: &str,
        from: &MySqlCheckpoint,
        to: &MySqlCheckpoint,
        boundary: Option<&MySqlCheckpoint>,
        tag: &ScanTag<'_>,
    ) -> Result<ScanReport, ProofError> {
        let started = std::time::Instant::now();
        for (name, p) in [("from", from), ("to", to)] {
            if p.lineage.as_deref() != Some(lineage_hash) {
                return Err(ProofError::Positions(format!(
                    "{name} position belongs to lineage {:?}, not {lineage_hash}",
                    p.lineage
                )));
            }
        }
        let (a, b) = (wm(from)?, wm(to)?);
        match order_positions(&a, &b) {
            CheckpointOrder::Equal => {
                observe_request(tag, from, to, "empty", 0, 0, &[], None);
                return Ok(answer(&a, &b, Vec::new(), 0, 0, started));
            }
            CheckpointOrder::Before => {}
            other => {
                return Err(ProofError::Positions(format!(
                    "from is not before to ({other:?})"
                )));
            }
        }
        // Pinned before waiting: pruning keeps everything after `A` while
        // this request is queued or running.
        let _pin = self.pin(from);
        let mut state = self.state.lock().await;
        let identity = Identity {
            server_uuid: server_uuid.to_ascii_lowercase(),
            lineage_hash: lineage_hash.to_string(),
        };
        if state.as_ref().is_some_and(|c| c.identity != identity) {
            *state = None;
        }
        self.prune(&mut state, boundary);

        let (mut events, mut bytes) = (0u64, 0u64);
        let mut segments_run: Vec<ScanTrace> = Vec::new();
        let result: Result<(), ProofError> = async {
            let Some(c) = state.as_mut() else {
                let r = segments.scan(from, to, tag).await?;
                (events, bytes) = (r.events, r.bytes);
                segments_run.push(r.trace.clone());
                let size = r.statements.iter().map(entry_bytes).sum();
                *state = Some(Covered {
                    identity: identity.clone(),
                    lo: from.clone(),
                    hi: to.clone(),
                    statements: r.statements,
                    bytes: size,
                });
                return Ok(());
            };
            let (lo, hi) = (wm(&c.lo)?, wm(&c.hi)?);
            match order_positions(&a, &lo) {
                CheckpointOrder::Before => {
                    let r = segments.scan(from, &c.lo.clone(), tag).await?;
                    segments_run.push(r.trace.clone());
                    events += r.events;
                    bytes += r.bytes;
                    c.bytes +=
                        r.statements.iter().map(entry_bytes).sum::<usize>();
                    let mut s = r.statements;
                    s.append(&mut c.statements);
                    c.statements = s;
                    c.lo = from.clone();
                }
                CheckpointOrder::Equal | CheckpointOrder::After => {}
                CheckpointOrder::Incomparable => {
                    return Err(ProofError::Positions(
                        "the start is unordered with the covered interval"
                            .into(),
                    ));
                }
            }
            match order_positions(&b, &hi) {
                CheckpointOrder::After => {
                    let r = segments.scan(&c.hi.clone(), to, tag).await?;
                    segments_run.push(r.trace.clone());
                    events += r.events;
                    bytes += r.bytes;
                    c.bytes +=
                        r.statements.iter().map(entry_bytes).sum::<usize>();
                    c.statements.extend(r.statements);
                    c.hi = to.clone();
                }
                CheckpointOrder::Before | CheckpointOrder::Equal => {}
                CheckpointOrder::Incomparable => {
                    return Err(ProofError::Positions(
                        "the end is unordered with the covered interval".into(),
                    ));
                }
            }
            Ok(())
        }
        .await;
        match result {
            // Unordered positions refuse this request only.
            Err(e @ ProofError::Positions(_)) => {
                observe_request(
                    tag,
                    from,
                    to,
                    "failed",
                    events,
                    bytes,
                    &segments_run,
                    None,
                );
                return Err(e);
            }
            Err(e) => {
                *state = None;
                observe_request(
                    tag,
                    from,
                    to,
                    "failed",
                    events,
                    bytes,
                    &segments_run,
                    None,
                );
                return Err(e);
            }
            Ok(()) => {}
        }
        let c = state.as_ref().expect("covered after a successful request");
        if c.statements.len() > self.limits.max_statements
            || c.bytes > self.limits.max_bytes
        {
            let why = format!(
                "proof record bound ({} statements, {} bytes)",
                c.statements.len(),
                c.bytes
            );
            *state = None;
            observe_request(
                tag,
                from,
                to,
                "failed",
                events,
                bytes,
                &segments_run,
                None,
            );
            return Err(ProofError::Limit(why));
        }
        let mut inside = Vec::new();
        for s in &c.statements {
            let after_a = at_or_before(s, &a).ok_or_else(|| {
                ProofError::Positions(
                    "a recorded statement is unordered".into(),
                )
            })?;
            let within_b = at_or_before(s, &b).ok_or_else(|| {
                ProofError::Positions(
                    "a recorded statement is unordered".into(),
                )
            })?;
            if !after_a && within_b {
                inside.push(s.clone());
            }
        }
        let served = if segments_run.is_empty() {
            "record"
        } else {
            "scanned"
        };
        observe_request(
            tag,
            from,
            to,
            served,
            events,
            bytes,
            &segments_run,
            Some((c.statements.len(), c.bytes)),
        );
        let mut report = answer(&a, &b, inside, events, bytes, started);
        if let Some(last) = segments_run.pop() {
            report.trace = last;
        }
        Ok(report)
    }

    /// Drop recorded statements at or before the lowest of `boundary` and
    /// every pin; a lowest point past `hi` clears the record. Unordered
    /// points prune nothing.
    fn prune(
        &self,
        state: &mut Option<Covered>,
        boundary: Option<&MySqlCheckpoint>,
    ) {
        let Some(c) = state.as_mut() else { return };
        let mut points: Vec<(WmPos, MySqlCheckpoint)> = Vec::new();
        for p in self.pins.lock().expect("pins").values().chain(boundary) {
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
                CheckpointOrder::Before => {
                    (low, low_cp) = (p.clone(), cp.clone())
                }
                CheckpointOrder::Equal | CheckpointOrder::After => {}
                CheckpointOrder::Incomparable => return,
            }
        }
        let (Ok(lo), Ok(hi)) = (wm(&c.lo), wm(&c.hi)) else {
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
                let mut keep = Vec::with_capacity(c.statements.len());
                let mut unordered = false;
                for s in c.statements.drain(..) {
                    match at_or_before(&s, &low) {
                        Some(true) => {}
                        Some(false) => keep.push(s),
                        None => unordered = true,
                    }
                }
                if unordered {
                    *state = None;
                    return;
                }
                c.bytes = keep.iter().map(entry_bytes).sum();
                c.statements = keep;
                c.lo = MySqlCheckpoint {
                    lineage: c.lo.lineage.clone(),
                    ..low_cp
                };
            }
            _ => *state = None,
        }
    }

    /// The retained record: (statements, bytes).
    #[cfg(test)]
    pub(crate) async fn retained(&self) -> Option<(usize, usize)> {
        self.state
            .lock()
            .await
            .as_ref()
            .map(|c| (c.statements.len(), c.bytes))
    }

    #[cfg(test)]
    pub(crate) fn pinned(&self) -> usize {
        self.pins.lock().expect("pins").len()
    }
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
/// its verified server.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn shared_interval(
    scanner: &ProofScanner,
    dsn: &str,
    source_id: &str,
    server_uuid: &str,
    lineage_hash: &str,
    from: &MySqlCheckpoint,
    to: &MySqlCheckpoint,
    boundary: Option<&MySqlCheckpoint>,
    tag: &ScanTag<'_>,
) -> Result<ScanReport, ProofError> {
    let segments = LiveSegments {
        dsn,
        server_id: super::mysql_helpers::derive_server_id(&format!(
            "{source_id}/proofs"
        )),
        server_uuid,
        lineage_hash,
        limits: ScanLimits::default(),
    };
    scanner
        .interval(
            &segments,
            server_uuid,
            lineage_hash,
            from,
            to,
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
    struct Synth {
        gtid: bool,
        ddl: BTreeMap<u64, Vec<DdlEffect>>,
        /// Physical segments scanned: (from, to) transaction numbers.
        scanned: Mutex<Vec<(u64, u64)>>,
        fail_next: Mutex<bool>,
        block: Mutex<Option<(Arc<Notify>, Arc<Notify>)>>,
    }

    impl Synth {
        fn new(gtid: bool) -> Self {
            Self {
                gtid,
                ddl: BTreeMap::new(),
                scanned: Default::default(),
                fail_next: Mutex::new(false),
                block: Mutex::new(None),
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
            _tag: &ScanTag<'_>,
        ) -> Result<ScanReport, ProofError> {
            let block = self.block.lock().unwrap().clone();
            if let Some((reached, release)) = block {
                reached.notify_one();
                release.notified().await;
            }
            if std::mem::take(&mut *self.fail_next.lock().unwrap()) {
                return Err(ProofError::Open("purged".into()));
            }
            let (a, b) = (self.number(from), self.number(to));
            self.scanned.lock().unwrap().push((a, b));
            Ok(ScanReport {
                digest: String::new(),
                statements: self.statements(a, b),
                events: b - a,
                bytes: 100 * (b - a),
                duration: Default::default(),
                trace: Default::default(),
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
        sc.interval(syn, U, L, &syn.at(a), &syn.at(b), bd.as_ref(), &TAG)
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
        let r = sc.interval(&g, U, L, &g.at(1), &f.at(5), None, &TAG).await;
        assert!(matches!(r, Err(ProofError::Positions(_))), "{r:?}");
        let r = sc
            .interval(&g, U, "other", &g.at(1), &g.at(5), None, &TAG)
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

    /// A failed extension clears the record: the next request rescans from
    /// its own start, and is right.
    #[tokio::test]
    async fn a_failed_extension_clears_the_record() {
        for gtid in modes() {
            let syn = Synth::new(gtid).with(8, vec![table("f")]);
            let sc = ProofScanner::default();
            ask(&sc, &syn, 0, 10, Some(0)).await.unwrap();
            *syn.fail_next.lock().unwrap() = true;
            assert!(matches!(
                ask(&sc, &syn, 5, 20, Some(5)).await,
                Err(ProofError::Open(_))
            ));
            assert_eq!(sc.retained().await, None);
            same_as_direct(&sc, &syn, 5, 20).await;
            assert_eq!(syn.scanned.lock().unwrap().last(), Some(&(5, 20)));
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

    /// Another server identity or lineage never reuses the record.
    #[tokio::test]
    async fn another_identity_starts_a_new_record() {
        let syn = Synth::new(true).with(3, vec![table("i")]);
        let sc = ProofScanner::default();
        ask(&sc, &syn, 0, 10, None).await.unwrap();
        let other = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
        sc.interval(&syn, other, L, &syn.at(0), &syn.at(10), None, &TAG)
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
