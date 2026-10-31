//! Forward proof after a live DDL (design spec 7.15, binding rulings Rounds
//! 24, 25 and 30).
//!
//! When the stream reads a DDL that may change a tracked table, the table's
//! `ddl` record (3b-1) makes the shape after it pending. Before the DDL event
//! is emitted, and so before any later event is read, the source proves the
//! post-DDL shape:
//!
//! 1. a stable capture of the table's shape (position, shape, position equal
//!    on one verified connection) at S_t;
//! 2. D (the DDL's position) and S (the last capture's position, at or after
//!    every S_t) comparable in the verified lineage with D <= S;
//! 3. one complete scan of (D, S] in which nothing affects the table: no DDL
//!    attributed to it, no database barrier for its database, no lineage
//!    barrier;
//! 4. only then: the captured shape's registration and
//!    `Observed { binds: ddl_id, proof }` at D commit in one atomic batch
//!    (`docs/design/mysql-ddl-proof-transactions.md`, C1). Every table is
//!    wholly in one batch; a multi-table DDL whose tables do not fit one is
//!    committed in several, in order.
//!
//! The pending `ddl` record is written before the capture, on its own. Any
//! failure leaves the DDL pending (rows after it stay unproven), with no
//! registration or binding from an uncommitted batch and no checkpoint
//! advance; a connection that reached another server stops the source. A
//! DROP gets no forward proof. Replay: a binding already present for the
//! `ddl` record is the durable evidence only once `resolve_binding`
//! validates it; nothing is then captured or appended again. A crash before
//! a batch commits leaves its tables pending, and the restart proves them
//! again.

use deltaforge_core::incident::{
    CauseCode, Evidence, EvidenceKey, IncidentId, Retryability,
};
use deltaforge_core::{CheckpointOrder, SourceError, SourceResult};
use std::time::Duration;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::mysql_activation::{self, ForwardProof, resolve_binding};
use super::mysql_baseline::affects;
use super::mysql_binlog_scan::{
    CLASSIFIER_VERSION, Cancelled, ProofError, ProofKind, ScanTag, capture,
    covers, observe_capture, trace_proof,
};
use super::mysql_binlog_scan::{Captured, ScanReport};
use super::mysql_failure::Failure;
use super::mysql_selection::{PlannedVersions, Timeline};
use super::mysql_window_budget::{Caps, Cost, Prefix, proof_table_cost};
use super::{MySqlCheckpoint, RunCtx, WindowInvalid};
use crate::durable_checkpoint::{
    WmPos, mysql_checkpoint_position, order_positions,
};
use crate::registry_scope::RegistryScope;
use schema_registry::SourceSchema;
use std::sync::Arc;
use storage::DurableSchemaRegistry;
use storage::adapters::{RegistrationPlanning, RegistrationRequest, SchemaKey};

/// A tracked table whose shape a DDL left pending.
#[derive(Clone)]
pub(crate) struct Pending {
    pub db: String,
    pub table: String,
    /// The exact capture identity of the table's pending `ddl` record.
    pub ddl_id: String,
    /// Its cached timeline before the `ddl` record, when C3 may install.
    pub snapshot: Option<Snapshot>,
}

/// A pending table's cached timeline, taken before its `ddl` record was
/// appended (`docs/design/mysql-ddl-proof-transactions.md`, C3), with the
/// sequence that append stored the record at (an append that found the
/// record already stored is a replay: no snapshot).
#[derive(Clone)]
pub(crate) struct Snapshot {
    pub timeline: Arc<Timeline>,
    pub ddl_seq: u64,
}

/// A table's timeline after its proof committed, found current by
/// [`fresh_timelines`]: the caller installs it if its cache still holds
/// `snapshot`.
pub(crate) struct Fresh {
    pub key: SchemaKey,
    pub snapshot: Arc<Timeline>,
    pub timeline: Timeline,
}

/// What [`commit_proofs`] committed, and why it stopped short, if it did.
#[derive(Default)]
struct CommitOutcome {
    committed: Vec<Committed>,
    halted: Option<Halted>,
}

/// What a forward proof's batches committed for one table: its binding's
/// sequence, and the version it binds with that version's hash.
struct Committed {
    table: usize,
    binding_seq: u64,
    version: i32,
    hash: String,
}

fn other_server(expected: &str, found: &str) -> SourceError {
    SourceError::Lineage {
        details: format!(
            "a forward-proof connection reached another server ({found}), \
             expected {expected}"
        )
        .into(),
    }
}

/// The MySQL work of one attempt: a table's capture and the interval's
/// scan. [`LiveProbe`] runs them on the server; tests run the same retry
/// loop ([`prove_with`]) over scripted outcomes.
pub(crate) trait Probe {
    async fn capture_table(
        &self,
        db: &str,
        table: &str,
        lock_wait: Duration,
    ) -> Result<Result<Captured, ProofError>, Cancelled>;

    async fn scan(
        &self,
        d: &MySqlCheckpoint,
        s: &MySqlCheckpoint,
    ) -> Result<ScanReport, ProofError>;
}

/// The run's server: captures on the run's capture context, scans on its
/// shared proof scanner.
struct LiveProbe<'a> {
    ctx: &'a RunCtx,
    server_uuid: &'a str,
    lineage: &'a str,
    tag: ScanTag<'a>,
}

impl Probe for LiveProbe<'_> {
    async fn capture_table(
        &self,
        db: &str,
        table: &str,
        lock_wait: Duration,
    ) -> Result<Result<Captured, ProofError>, Cancelled> {
        let ctx = self.ctx;
        capture(
            &ctx.capture,
            self.server_uuid,
            self.lineage,
            db,
            table,
            None,
            lock_wait,
        )
        .await
    }

    async fn scan(
        &self,
        d: &MySqlCheckpoint,
        s: &MySqlCheckpoint,
    ) -> Result<ScanReport, ProofError> {
        super::mysql_proof_scanner::shared_interval(
            &self.ctx.proof_scanner,
            self.ctx.dsn.current().expose(),
            &self.ctx.source_id,
            self.server_uuid,
            self.lineage,
            d,
            s,
            None,
            Some(d),
            &self.tag,
        )
        .await
    }
}

/// When a proof's work must not start: the run stopped, or the proof's
/// deadline passed.
pub(crate) struct Halt<'a> {
    pub deadline: Instant,
    pub cancel: &'a CancellationToken,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Halted {
    Stopped,
    Deadline,
}

impl Halt<'_> {
    fn check(&self) -> Option<Halted> {
        if self.cancel.is_cancelled() {
            Some(Halted::Stopped)
        } else if Instant::now() >= self.deadline {
            Some(Halted::Deadline)
        } else {
            None
        }
    }

    fn left(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }
}

/// A retryable failure's class
/// (`docs/design/mysql-forward-proof-retry.md`, section 3.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Retry {
    Contention(u16),
    Transport,
    Read,
    NotReached,
    /// The deadline left too little time to try, or ran out mid-attempt.
    Deadline,
}

impl Retry {
    fn as_str(self) -> &'static str {
        match self {
            Self::Contention(code) => Failure::Contention(code).as_str(),
            Self::Transport => "transport",
            Self::Read => "read",
            Self::NotReached => "not_reached",
            Self::Deadline => "deadline",
        }
    }

    fn cause(self) -> CauseCode {
        match self {
            Self::Contention(_) => CauseCode::SourceSchema,
            _ => CauseCode::SourceConnect,
        }
    }
}

/// What ends a proof before its tables are decided.
enum Stop {
    Cancelled,
    /// A connection reached another server, or its identity is unknown.
    Identity(String),
    /// No control-plane connection, for these tables.
    Saturated(&'static str, Vec<(String, String)>),
    /// Authentication or authorization, for this table.
    Denied(Failure, String),
    /// A store failure.
    Store(SourceError),
}

enum Decision {
    Retry(Retry),
    /// Deferred as before retries existed: the table stays pending.
    Defer(&'static str),
    Stop(Stop),
}

/// What a failed capture or scan means for the proof (section 3.3). Every
/// variant is decided here; a new one does not compile until it is.
fn decide(e: &ProofError) -> Decision {
    match e {
        ProofError::Contention { code, .. } => {
            Decision::Retry(Retry::Contention(*code))
        }
        ProofError::Open(class, _) | ProofError::Read(class, _) => {
            match class {
                Failure::Contention(code) => {
                    Decision::Retry(Retry::Contention(*code))
                }
                Failure::Transport => Decision::Retry(match e {
                    ProofError::Read(..) => Retry::Read,
                    _ => Retry::Transport,
                }),
                Failure::Authentication | Failure::Authorization => {
                    Decision::Stop(Stop::Denied(*class, String::new()))
                }
                Failure::Protocol => Decision::Defer("protocol"),
                Failure::Retention => Decision::Defer("retention"),
                Failure::Absent | Failure::Other => Decision::Defer("other"),
            }
        }
        ProofError::NotReached(_) => Decision::Retry(Retry::NotReached),
        ProofError::OtherServer(found) => {
            Decision::Stop(Stop::Identity(found.clone()))
        }
        ProofError::Saturated { scope, .. } => {
            Decision::Stop(Stop::Saturated(scope, Vec::new()))
        }
        ProofError::Positions(_)
        | ProofError::Incident
        | ProofError::UnsupportedEvent(_)
        | ProofError::Framing(_)
        | ProofError::Overshoot(_)
        | ProofError::AnchorMismatch(_) => Decision::Defer("evidence"),
        ProofError::Limit(_) | ProofError::SeekLimit(_) => {
            Decision::Defer("bound")
        }
        ProofError::Unstable(_) => Decision::Defer("decision"),
        ProofError::Capture(_) => Decision::Defer("other"),
    }
}

/// A pending table's state in its proof (section 3.2). Only `Retrying`
/// changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// Not yet tried (`None`), or its last failure was retryable.
    Retrying(Option<Retry>),
    Proven,
    Deferred(&'static str),
}

struct Table {
    p: Pending,
    state: State,
    /// It had a retryable failure: in the incident's scope.
    scoped: bool,
}

/// One proof's tables and its retrying incident.
struct Retries {
    tables: Vec<Table>,
    attempts: u32,
    last: Option<Retry>,
    incident: Option<IncidentId>,
    ddl: MySqlCheckpoint,
    before: MySqlCheckpoint,
}

impl Retries {
    fn retrying(&self) -> Vec<usize> {
        (0..self.tables.len())
            .filter(|&i| matches!(self.tables[i].state, State::Retrying(_)))
            .collect()
    }

    fn name(&self, i: usize) -> String {
        format!("{}.{}", self.tables[i].p.db, self.tables[i].p.table)
    }

    /// The incident's draft, counting `tables` from `first`.
    fn draft(
        &self,
        ctx: &RunCtx,
        first: &str,
        tables: u64,
        class: &str,
        retryability: Retryability,
        cause: CauseCode,
    ) -> deltaforge_core::incident::IncidentDraft {
        crate::incident_drafts::mysql_forward_proof_unavailable(
            &crate::incident_drafts::ForwardProof {
                source_id: &ctx.source_id,
                table: first,
                tables,
                attempts: self.attempts,
                class,
                ddl: &self.ddl,
                before: &self.before,
            },
            retryability,
            cause,
        )
    }

    /// Table `i` failed for a retryable reason: it stays `Retrying`, joins
    /// the incident's scope, and the incident is recorded as retrying if
    /// it is not yet.
    async fn fail(&mut self, ctx: &RunCtx, i: usize, class: Retry) {
        self.tables[i].state = State::Retrying(Some(class));
        self.tables[i].scoped = true;
        self.last = Some(class);
        metrics::counter!(
            "deltaforge_mysql_forward_proof_retries_total",
            "class" => class.as_str(),
        )
        .increment(1);
        if self.incident.is_none() {
            let draft = self.draft(
                ctx,
                &self.name(i),
                1,
                class.as_str(),
                Retryability::AutoRetry,
                class.cause(),
            );
            self.incident =
                crate::incident_drafts::record_retrying(&ctx.incidents, &draft)
                    .await;
        }
    }

    /// The retrying incident ends without a recovery: withdrawn, with why.
    async fn withdraw(&mut self, ctx: &RunCtx, why: &str, tables: u64) {
        let Some(id) = self.incident.take() else {
            return;
        };
        let mut update = Evidence::new();
        update
            .text(EvidenceKey::ReasonClass, why)
            .count(EvidenceKey::BlockingCount, tables)
            .count(EvidenceKey::Attempts, u64::from(self.attempts));
        warn!(source_id = %ctx.source_id, incident = %id, why, "forward proof: the retrying incident is withdrawn");
        crate::incident_drafts::cancel_retrying_with(
            &ctx.incidents,
            Some(id),
            &update,
        )
        .await;
    }
}

fn outcome(scoped: bool, what: &'static str) {
    if scoped {
        metrics::counter!(
            "deltaforge_mysql_forward_proof_outcomes_total",
            "outcome" => what,
        )
        .increment(1);
    }
}

/// Prove the post-DDL shape of every `pending` table at the DDL's position
/// (the stream's current position), retrying transient failures in place
/// within the run's bounds (`docs/design/mysql-forward-proof-retry.md`).
/// Returns the committed tables' timelines found current (C3); the caller
/// invalidates every other touched table. An error stops the source before
/// the DDL is emitted or checkpointed.
pub(crate) async fn prove(
    ctx: &RunCtx,
    pending: Vec<Pending>,
) -> SourceResult<Vec<Fresh>> {
    if pending.is_empty() {
        return Ok(Vec::new());
    }
    let scope = ctx.registry_scope.current()?;
    let lineage = scope.lineage().lineage_hash.clone();
    let server_uuid = ctx.expected_uuid()?;
    let probe = LiveProbe {
        ctx,
        server_uuid: &server_uuid,
        lineage: &lineage,
        tag: ScanTag {
            source_id: &ctx.source_id,
            kind: ProofKind::Forward,
        },
    };
    prove_with(ctx, &probe, pending).await
}

/// [`prove`] over `probe`'s MySQL work.
pub(crate) async fn prove_with<P: Probe>(
    ctx: &RunCtx,
    probe: &P,
    pending: Vec<Pending>,
) -> SourceResult<Vec<Fresh>> {
    if pending.is_empty() {
        return Ok(Vec::new());
    }
    let scope = ctx.registry_scope.current()?;
    let lineage = scope.lineage().lineage_hash.clone();
    let d_cp = MySqlCheckpoint {
        file: ctx.last_file.clone(),
        pos: ctx.last_pos,
        // The DDL statement completes its transaction: D includes it.
        gtid_set: ctx.executed_through_current().map(|s| s.to_string()),
        lineage: Some(lineage.clone()),
        snapshot_completed: None,
        snapshot_chain: None,
    };
    let Some(d) = mysql_checkpoint_position(
        &d_cp.file,
        d_cp.pos,
        d_cp.gtid_set.as_deref(),
    ) else {
        warn!(source_id = %ctx.source_id, "unparseable DDL position: the DDL stays pending");
        return Ok(Vec::new());
    };

    // Replay: a binding already recorded for the pending DDL is the proof.
    let replay_phase = crate::perf_phases::Phase::start("replay_check");
    let mut open = Vec::new();
    for p in pending {
        if !bound_now(ctx, &scope, &p).await? {
            open.push(p);
        }
    }
    drop(replay_phase);
    if open.is_empty() {
        return Ok(Vec::new());
    }

    let limits = &ctx.proofs.forward_proof_retry;
    let halt = Halt {
        deadline: Instant::now() + limits.deadline,
        cancel: &ctx.cancel,
    };
    let mut run = Retries {
        tables: open
            .into_iter()
            .map(|p| Table {
                p,
                state: State::Retrying(None),
                scoped: false,
            })
            .collect(),
        attempts: 0,
        last: None,
        incident: None,
        before: before_ddl(ctx, &lineage),
        ddl: d_cp,
    };
    let mut fresh = Vec::new();
    let end = loop {
        if run.retrying().is_empty() {
            break None;
        }
        if run.attempts >= limits.attempts {
            break Some(None);
        }
        if run.attempts > 0 {
            // A table bound meanwhile (an earlier attempt's batch resolved
            // as committed) is proven, never captured again.
            let mut gone = None;
            for i in run.retrying() {
                match bound_now(ctx, &scope, &run.tables[i].p).await {
                    Ok(true) => run.tables[i].state = State::Proven,
                    Ok(false) => {}
                    Err(e) => {
                        gone = Some(Stop::Store(e));
                        break;
                    }
                }
            }
            if let Some(stop) = gone {
                break Some(Some(stop));
            }
            if run.retrying().is_empty() {
                break None;
            }
            let wait = limits
                .backoff
                .get(run.attempts as usize - 1)
                .or(limits.backoff.last())
                .copied()
                .unwrap_or_default()
                .min(halt.left());
            tokio::select! {
                biased;
                _ = ctx.cancel.cancelled() => break Some(Some(Stop::Cancelled)),
                _ = tokio::time::sleep(wait) => {}
            }
        }
        if halt.left() < limits.min_capture {
            break Some(None);
        }
        run.attempts += 1;
        if let Err(stop) = attempt(
            ctx, probe, &scope, &halt, &d, &lineage, &mut run, &mut fresh,
        )
        .await
        {
            break Some(Some(stop));
        }
    };
    finish(ctx, run, end, fresh).await
}

/// Whether `p`'s pending DDL already has a binding, validated as complete,
/// consistent evidence; an invalid one stops here (never a fresh proof
/// over later server state).
async fn bound_now(
    ctx: &RunCtx,
    scope: &RegistryScope,
    p: &Pending,
) -> SourceResult<bool> {
    let key = scope.key(&p.db, &p.table);
    let records = mysql_activation::read_table(
        &ctx.registry_backend,
        ctx.schema.registry(),
        &key,
    )
    .await
    .map_err(|e| {
        SourceError::Other(e.context("forward proof: activation timeline"))
    })?;
    match resolve_binding(
        ctx.schema.registry(),
        &key,
        &records,
        &p.ddl_id,
        CLASSIFIER_VERSION,
    )
    .await
    {
        Ok(found) => Ok(found.is_some()),
        Err(e) => Err(SourceError::Schema {
            details: format!("{e:#}").into(),
        }),
    }
}

/// One attempt over the tables still retrying: their captures in order,
/// one scan of (D, S], and the commit of what it proves. Nothing of an
/// earlier attempt's captures or scan is used.
#[allow(clippy::too_many_arguments)]
async fn attempt<P: Probe>(
    ctx: &RunCtx,
    probe: &P,
    scope: &RegistryScope,
    halt: &Halt<'_>,
    d: &WmPos,
    lineage: &str,
    run: &mut Retries,
    fresh: &mut Vec<Fresh>,
) -> Result<(), Stop> {
    let limits = &ctx.proofs.forward_proof_retry;
    let tag = ScanTag {
        source_id: &ctx.source_id,
        kind: ProofKind::Forward,
    };
    let lctn = ctx.lower_case_table_names;
    // 1. Captures, bound to their intervals by step 3.
    let capture_phase = crate::perf_phases::Phase::start("capture");
    let mut captured: Vec<(usize, Captured)> = Vec::new();
    for i in run.retrying() {
        let left = halt.left();
        if left < limits.min_capture {
            run.fail(ctx, i, Retry::Deadline).await;
            continue;
        }
        let (db, table) =
            (run.tables[i].p.db.clone(), run.tables[i].p.table.clone());
        // Under the time left: an expired capture is dropped, and its
        // session killed on the server (`KillOnDrop`).
        let got = tokio::select! {
            biased;
            _ = ctx.cancel.cancelled() => return Err(Stop::Cancelled),
            r = tokio::time::timeout(
                left,
                probe.capture_table(&db, &table, limits.lock_wait.min(left)),
            ) => r,
        };
        match got {
            Err(_) => run.fail(ctx, i, Retry::Deadline).await,
            Ok(Err(Cancelled)) => return Err(Stop::Cancelled),
            Ok(Ok(Ok(cap))) => {
                observe_capture(&tag, &cap);
                captured.push((i, cap));
            }
            Ok(Ok(Err(e))) => match decide(&e) {
                Decision::Retry(class) => {
                    warn!(source_id = %ctx.source_id, %db, %table, error = ?e, attempt = run.attempts, "forward proof: no capture; retried within the bound");
                    run.fail(ctx, i, class).await;
                }
                Decision::Defer(why) => {
                    warn!(source_id = %ctx.source_id, %db, %table, error = ?e, "forward proof: no capture; the DDL stays pending");
                    trace_proof(
                        &tag,
                        &db,
                        &table,
                        None,
                        None,
                        lctn,
                        "no_capture",
                    );
                    run.tables[i].state = State::Deferred(why);
                }
                Decision::Stop(stop) => {
                    return Err(match stop {
                        Stop::Saturated(scope, _) => {
                            Stop::Saturated(scope, vec![(db, table)])
                        }
                        Stop::Denied(class, _) => {
                            Stop::Denied(class, format!("{db}.{table}"))
                        }
                        other => other,
                    });
                }
            },
        }
    }
    drop(capture_phase);
    let Some(s_cp) = captured.last().map(|(_, cap)| cap.position.clone())
    else {
        return Ok(());
    };
    let defer_all = |run: &mut Retries, captured: &[(usize, Captured)], why| {
        for (i, _) in captured {
            run.tables[*i].state = State::Deferred(why);
        }
    };
    let Some(s) = mysql_checkpoint_position(
        &s_cp.file,
        s_cp.pos,
        s_cp.gtid_set.as_deref(),
    ) else {
        defer_all(run, &captured, "evidence");
        return Ok(());
    };

    // 2. D <= S in the verified lineage.
    if !matches!(
        order_positions(d, &s),
        CheckpointOrder::Before | CheckpointOrder::Equal
    ) {
        warn!(source_id = %ctx.source_id, "forward proof: the capture is not at or after the DDL; the DDL stays pending");
        defer_all(run, &captured, "decision");
        return Ok(());
    }

    // 3. One complete scan of (D, S], under the time left.
    let left = halt.left();
    if left.is_zero() {
        for (i, _) in &captured {
            run.fail(ctx, *i, Retry::Deadline).await;
        }
        return Ok(());
    }
    let scan_phase = crate::perf_phases::Phase::start("scan");
    let scanned = tokio::select! {
        biased;
        _ = ctx.cancel.cancelled() => return Err(Stop::Cancelled),
        r = tokio::time::timeout(left, probe.scan(&run.ddl, &s_cp)) => r,
    };
    drop(scan_phase);
    let report = match scanned {
        Err(_) => {
            for (i, _) in &captured {
                run.fail(ctx, *i, Retry::Deadline).await;
            }
            return Ok(());
        }
        Ok(Ok(report)) => report,
        Ok(Err(e)) => match decide(&e) {
            Decision::Retry(class) => {
                warn!(source_id = %ctx.source_id, error = ?e, attempt = run.attempts, "forward proof: the interval scan failed; retried within the bound");
                for (i, _) in &captured {
                    run.fail(ctx, *i, class).await;
                }
                return Ok(());
            }
            Decision::Defer(why) => {
                warn!(source_id = %ctx.source_id, error = ?e, "forward proof: the interval scan failed; the DDL stays pending");
                for (i, cap) in &captured {
                    let p = &run.tables[*i].p;
                    trace_proof(
                        &tag,
                        &p.db,
                        &p.table,
                        Some(cap),
                        None,
                        lctn,
                        "scan_failed",
                    );
                }
                defer_all(run, &captured, why);
                return Ok(());
            }
            Decision::Stop(stop) => {
                let tables = captured
                    .iter()
                    .map(|(i, _)| {
                        let p = &run.tables[*i].p;
                        (p.db.clone(), p.table.clone())
                    })
                    .collect::<Vec<_>>();
                return Err(match stop {
                    Stop::Saturated(scope, _) => Stop::Saturated(scope, tables),
                    Stop::Denied(class, _) => Stop::Denied(
                        class,
                        tables
                            .first()
                            .map(|(d, t)| format!("{d}.{t}"))
                            .unwrap_or_default(),
                    ),
                    other => other,
                });
            }
        },
    };

    // 4. Register and bind: the tables the scan proves, in atomic batches.
    let mut proven = Vec::with_capacity(captured.len());
    let mut index = Vec::with_capacity(captured.len());
    for (i, cap) in captured {
        let p = &run.tables[i].p;
        if !covers(&cap, d, &s) {
            warn!(source_id = %ctx.source_id, db = %p.db, table = %p.table, "forward proof: the scan does not cover the capture interval; the DDL stays pending");
            trace_proof(
                &tag,
                &p.db,
                &p.table,
                Some(&cap),
                Some(&report),
                lctn,
                "not_covered",
            );
            run.tables[i].state = State::Deferred("not_covered");
            continue;
        }
        if report
            .statements
            .iter()
            .any(|st| affects(st, &p.db, &p.table, ctx.lower_case_table_names))
        {
            info!(source_id = %ctx.source_id, db = %p.db, table = %p.table, "forward proof: a DDL or barrier in (D, S]; the DDL stays pending");
            trace_proof(
                &tag,
                &p.db,
                &p.table,
                Some(&cap),
                Some(&report),
                lctn,
                "relevant_ddl",
            );
            run.tables[i].state = State::Deferred("relevant_ddl");
            continue;
        }
        proven.push((p.clone(), cap));
        index.push(i);
    }
    if proven.is_empty() {
        return Ok(());
    }
    let checkpoint = serde_json::to_vec(&run.ddl)
        .map_err(|e| Stop::Store(SourceError::Other(e.into())))?;
    let bound = Bound {
        d,
        s: &s,
        report: &report,
        checkpoint: &checkpoint,
        lineage,
    };
    let register_phase = crate::perf_phases::Phase::start("register_bind");
    let outcome = commit_proofs(ctx, scope, &tag, &bound, &proven, halt)
        .await
        .map_err(Stop::Store)?;
    drop(register_phase);
    info!(source_id = %ctx.source_id, bindings = outcome.committed.len(), attempt = run.attempts, "forward proof after DDL");
    let mut committed = vec![false; proven.len()];
    for c in &outcome.committed {
        committed[c.table] = true;
        run.tables[index[c.table]].state = State::Proven;
    }
    match outcome.halted {
        Some(Halted::Stopped) => return Err(Stop::Cancelled),
        Some(Halted::Deadline) => {
            for (k, &i) in index.iter().enumerate() {
                if !committed[k] {
                    run.fail(ctx, i, Retry::Deadline).await;
                }
            }
        }
        None => {}
    }
    // Past the deadline the freshness reads are skipped: those tables are
    // invalidated and reload, which is always safe.
    if halt.check().is_none() {
        let _phase = crate::perf_phases::Phase::start("fresh_check");
        fresh.extend(
            fresh_timelines(ctx, scope, &proven, &outcome.committed).await,
        );
    }
    Ok(())
}

/// The proof's end (section 3.8): `None` completed (no table retrying),
/// `Some(None)` exhausted, `Some(Some(stop))` stopped.
async fn finish(
    ctx: &RunCtx,
    mut run: Retries,
    end: Option<Option<Stop>>,
    fresh: Vec<Fresh>,
) -> SourceResult<Vec<Fresh>> {
    let scoped = run.tables.iter().any(|t| t.scoped);
    match end {
        None => {
            let deferred = run
                .tables
                .iter()
                .filter(|t| t.scoped && matches!(t.state, State::Deferred(_)))
                .count() as u64;
            if deferred == 0 {
                if let Some(id) = run.incident.take()
                    && let Err(e) = ctx
                        .incidents
                        .resolve(
                            &id,
                            storage::adapters::incidents::Resolution::VerifiedRecovery {
                                check: "forward_proof_bound".to_string(),
                            },
                        )
                        .await
                {
                    warn!(error = %format!("{e:#}"), "could not resolve the retrying forward-proof incident");
                }
                outcome(scoped, "recovered");
            } else {
                run.withdraw(ctx, "ended_deferred", deferred).await;
                outcome(scoped, "ended_deferred");
            }
            Ok(fresh)
        }
        Some(None) => {
            let retrying = run.retrying();
            let first = run.name(retrying[0]);
            let class = run.last.unwrap_or(Retry::Deadline);
            outcome(scoped, "exhausted");
            let draft = run.draft(
                ctx,
                &first,
                retrying.len() as u64,
                class.as_str(),
                Retryability::OperatorAction,
                class.cause(),
            );
            Err(SourceError::incident(
                draft,
                SourceError::Schema {
                    details: format!(
                        "the forward proof of {} table(s) (first {first}) failed \
                         after {} attempt(s) (last: {}); stopped before the DDL",
                        retrying.len(),
                        run.attempts,
                        class.as_str()
                    )
                    .into(),
                },
            ))
        }
        Some(Some(Stop::Cancelled)) => {
            let n = run.retrying().len() as u64;
            run.withdraw(ctx, "ended_cancelled", n).await;
            outcome(scoped, "cancelled");
            Err(SourceError::Cancelled)
        }
        Some(Some(Stop::Denied(class, table))) => {
            // The same boundary's retrying incident, if any: the supervisor
            // reclassifies it to operator action.
            outcome(scoped, "ended_by_terminal");
            let draft = run.draft(
                ctx,
                &table,
                1,
                class.as_str(),
                Retryability::OperatorAction,
                CauseCode::SourceAuth,
            );
            Err(SourceError::incident(
                draft,
                SourceError::Auth {
                    details: format!(
                        "the forward proof of {table} was refused \
                         ({}); stopped before the DDL",
                        class.as_str()
                    )
                    .into(),
                },
            ))
        }
        Some(Some(Stop::Identity(found))) => {
            let n = run.retrying().len() as u64;
            run.withdraw(ctx, "ended_by_lineage", n).await;
            outcome(scoped, "ended_by_terminal");
            Err(other_server(
                &ctx.expected_uuid().unwrap_or_default(),
                &found,
            ))
        }
        Some(Some(Stop::Saturated(scope, tables))) => {
            let n = run.retrying().len() as u64;
            run.withdraw(ctx, "ended_by_mysql_control_plane_saturated", n)
                .await;
            outcome(scoped, "ended_by_terminal");
            // Saturation never defers: a pending DDL's next rows have no
            // lazy proof. The source stops before the DDL is emitted or
            // checkpointed; the restart replays the DDL and proves it again.
            Err(crate::incident_drafts::mysql_control_plane_saturated(
                &ctx.source_id,
                "forward proof",
                &tables,
                scope,
                &run.before,
            ))
        }
        Some(Some(Stop::Store(e))) => {
            let n = run.retrying().len() as u64;
            run.withdraw(ctx, "ended_by_store", n).await;
            outcome(scoped, "ended_by_terminal");
            Err(e)
        }
    }
}

/// What every binding of one forward proof shares.
struct Bound<'a> {
    d: &'a WmPos,
    s: &'a WmPos,
    report: &'a ScanReport,
    /// The registration checkpoint: D.
    checkpoint: &'a [u8],
    lineage: &'a str,
}

/// The checkpoint before the DDL: where a stopped proof leaves the source.
fn before_ddl(ctx: &RunCtx, lineage: &str) -> MySqlCheckpoint {
    MySqlCheckpoint {
        file: ctx.last_file.clone(),
        pos: ctx.last_event_start,
        gtid_set: ctx.gtid_text(),
        lineage: Some(lineage.to_string()),
        snapshot_completed: None,
        snapshot_chain: None,
    }
}

/// One proven table's request and cost, from its capture.
fn proof_request(
    key: &SchemaKey,
    cap: &Captured,
    checkpoint: &[u8],
    other_lineages: u64,
) -> SourceResult<(RegistrationRequest, Cost)> {
    let schema_json = serde_json::to_value(&cap.schema)
        .map_err(|e| SourceError::Other(e.into()))?;
    let schema_bytes = serde_json::to_vec(&schema_json)
        .map_err(|e| SourceError::Other(e.into()))?
        .len() as u64;
    let cost = proof_table_cost(
        schema_bytes,
        other_lineages,
        key.backend_key().len() as u64,
    );
    Ok((
        RegistrationRequest {
            key: key.clone(),
            hash: cap.schema.fingerprint().to_string(),
            schema_json,
            checkpoint: Some(checkpoint.to_vec()),
        },
        cost,
    ))
}

/// The tables of a batch, in order, cut where the next one would take the
/// batch over a cap; `Err` names a table no batch can take alone.
fn group(
    caps: Caps,
    costs: &[(usize, Cost)],
) -> Result<Vec<Vec<usize>>, (usize, WindowInvalid)> {
    let mut groups = Vec::new();
    let mut current = Vec::new();
    let mut prefix = Prefix::new(caps);
    for (i, cost) in costs {
        Prefix::fits_alone(caps, *cost).map_err(|e| (*i, e))?;
        if !prefix.offer(*cost).map_err(|e| (*i, e))? {
            groups.push(std::mem::take(&mut current));
            prefix = Prefix::new(caps);
            prefix.offer(*cost).map_err(|e| (*i, e))?;
        }
        current.push(*i);
    }
    if !current.is_empty() {
        groups.push(current);
    }
    Ok(groups)
}

/// Commit the registrations and bindings of the `proven` tables
/// (`docs/design/mysql-ddl-proof-transactions.md`, C1): every table wholly
/// in one atomic batch, its registration and its binding together. The
/// preflight sizes every table before the first commit, keeping one cost
/// per table; a table no batch can take alone stops the source with
/// nothing of the proof written. A batch's plan exists only while it is
/// committed. A conflict plans the batch again from fresh reads and sizes
/// it again (bounded); a batch that no longer fits is split. The pending
/// `ddl` records stay as written; a stop leaves every table not yet
/// committed pending. Past `halt` (the run's stop or the proof's deadline)
/// no batch or replan starts: a batch already started resolves first
/// (its commit-marker resolution included), and what was committed is
/// returned with why it halted.
async fn commit_proofs(
    ctx: &RunCtx,
    scope: &RegistryScope,
    tag: &ScanTag<'_>,
    bound: &Bound<'_>,
    proven: &[(Pending, Captured)],
    halt: &Halt<'_>,
) -> SourceResult<CommitOutcome> {
    use super::mysql_activation::{
        ACTIVATION_NS, Kind, Record, encode_record, forward_capture_id,
        table_stream,
    };
    use super::mysql_activation_window::{
        BATCH_CONFLICT_RETRIES, BATCH_STORAGE_RETRIES,
    };
    use storage::{AtomicBatch, BatchOp, CommitResult, commit_atomic_counted};

    if proven.is_empty() {
        return Ok(CommitOutcome::default());
    }
    // One store holds the registry and the activation records, or no batch
    // can commit them together.
    if !std::sync::Arc::ptr_eq(
        ctx.schema.registry().backend(),
        &ctx.registry_backend,
    ) {
        return Err(SourceError::Other(anyhow::anyhow!(
            "the forward proof needs the schema registry and the activation \
             records in one state store"
        )));
    }
    let lctn = ctx.lower_case_table_names;
    let keys: Vec<SchemaKey> = proven
        .iter()
        .map(|(p, _)| scope.key(&p.db, &p.table))
        .collect();
    let stopped = |i: usize, invalid: WindowInvalid| {
        let (p, _) = &proven[i];
        crate::incident_drafts::mysql_table_too_large(
            &ctx.source_id,
            &p.db,
            &p.table,
            &invalid,
            &before_ddl(ctx, bound.lineage),
        )
    };
    let too_large = |i: usize, too: &storage::StateItemTooLarge| {
        let (p, _) = &proven[i];
        crate::incident_drafts::state_item_too_large(
            &ctx.source_id,
            &p.db,
            &p.table,
            too,
            &before_ddl(ctx, bound.lineage),
        )
    };
    let other_lineages = || async {
        storage::adapters::source_lineage::load(
            &ctx.registry_backend,
            &ctx.tenant,
            &ctx.source_id,
        )
        .await
        .map(|r| r.map_or(0, |r| r.prior_lineage_hashes.len() as u64))
        .map_err(SourceError::Other)
    };
    // The costs of the tables in `of`, from their captures; an entry over
    // the store's item limit stops the source here.
    let costs = |of: &[usize],
                 others: u64|
     -> SourceResult<Vec<(usize, Cost)>> {
        let mut out = Vec::with_capacity(of.len());
        for &i in of {
            let (req, cost) = proof_request(
                &keys[i],
                &proven[i].1,
                bound.checkpoint,
                others,
            )?;
            let size = DurableSchemaRegistry::registration_entry_bound(&req)
                .map_err(SourceError::Other)?;
            if size > storage::MAX_STATE_ITEM_BYTES {
                return Err(too_large(
                    i,
                    &storage::StateItemTooLarge {
                        size,
                        key_digest: storage::state_key_digest(
                            storage::adapters::schema_registry::V1_NS,
                            &keys[i].backend_key(),
                        ),
                    },
                ));
            }
            out.push((i, cost));
        }
        Ok(out)
    };

    // Preflight: every table sized, one cost each, before any commit.
    let caps = Caps::current();
    let all: Vec<usize> = (0..proven.len()).collect();
    let mut queue: std::collections::VecDeque<Vec<usize>> =
        group(caps, &costs(&all, other_lineages().await?)?)
            .map_err(|(i, e)| stopped(i, e))?
            .into();

    let registry = ctx.schema.registry();
    let mut out = Vec::with_capacity(proven.len());
    let mut applies = 0;
    while let Some(mut batch_tables) = queue.pop_front() {
        let mut attempt = 0;
        loop {
            // No batch or replan starts past the stop or the deadline.
            if let Some(halted) = halt.check() {
                return Ok(CommitOutcome {
                    committed: out,
                    halted: Some(halted),
                });
            }
            attempt += 1;
            if attempt > 1 {
                // Sized again from fresh reads: a batch that no longer fits
                // is split, its rest committed after it.
                let mut regrouped = group(
                    caps,
                    &costs(&batch_tables, other_lineages().await?)?,
                )
                .map_err(|(i, e)| stopped(i, e))?;
                batch_tables = regrouped.remove(0);
                for rest in regrouped.into_iter().rev() {
                    queue.push_front(rest);
                }
            }
            let mut reqs = Vec::with_capacity(batch_tables.len());
            for &i in &batch_tables {
                reqs.push(
                    proof_request(&keys[i], &proven[i].1, bound.checkpoint, 0)?
                        .0,
                );
            }
            let plan_phase = crate::perf_phases::Phase::start("plan");
            let planning =
                registry.plan_registrations(&reqs, chrono::Utc::now()).await;
            drop(plan_phase);
            let (preconditions, regs) = match planning.map_err(|e| {
                SourceError::Other(
                    e.context("plan the forward proof's registrations"),
                )
            })? {
                RegistrationPlanning::Planned {
                    preconditions,
                    registrations,
                } => (preconditions, registrations),
                RegistrationPlanning::NeedsRepair(broken) => {
                    for k in &broken {
                        registry
                            .repair_key(k)
                            .await
                            .map_err(SourceError::Other)?;
                    }
                    if attempt > BATCH_CONFLICT_RETRIES {
                        return Err(SourceError::Other(anyhow::anyhow!(
                            "the forward proof's registrations still need repair \
                             after {attempt} attempts; the DDL stays pending"
                        )));
                    }
                    continue;
                }
            };
            let mut batch = AtomicBatch {
                preconditions,
                ops: Vec::new(),
            };
            // Each table's registration entry op, and the table of every op
            // (to name the table of an item the store refuses).
            let mut entry_ops = Vec::with_capacity(regs.len());
            let mut binding_ops = Vec::with_capacity(regs.len());
            let mut op_table = Vec::new();
            for (&i, reg) in batch_tables.iter().zip(&regs) {
                let (p, _) = &proven[i];
                let key = &keys[i];
                let base = batch.push_group(reg.ops.clone());
                entry_ops.push(reg.entry_op.map(|e| base + e));
                let record = Record::new(
                    bound.d.clone(),
                    Kind::Observed {
                        version: reg.version,
                        binds: Some(p.ddl_id.clone()),
                        proof: Some(ForwardProof {
                            classifier_version: CLASSIFIER_VERSION.to_string(),
                            to: bound.s.clone(),
                            scan_digest: bound.report.digest.clone(),
                            schema_hash: reg.hash.clone(),
                        }),
                        full: None,
                    },
                );
                let stream = table_stream(key);
                let capture_id =
                    forward_capture_id(&key.lineage_hash, &stream, &record)
                        .map_err(SourceError::Other)?;
                binding_ops.push(batch.ops.len());
                batch.ops.push(BatchOp::LogAppendIdempotent {
                    ns: ACTIVATION_NS.into(),
                    key: stream,
                    capture_id,
                    value: encode_record(&record)
                        .map_err(SourceError::Other)?,
                });
                op_table.resize(batch.ops.len(), i);
            }
            let commit_phase = crate::perf_phases::Phase::start("commit");
            let result = commit_atomic_counted(
                ctx.registry_backend.as_ref(),
                &batch,
                BATCH_STORAGE_RETRIES,
                &mut applies,
            )
            .await;
            drop(commit_phase);
            let committed = match result {
                Ok(c) => c,
                Err(e) => {
                    if let Some(too) = e.chain().find_map(|c| {
                        c.downcast_ref::<storage::StateItemTooLarge>()
                    }) && let Some(i) =
                        super::mysql_activation_window::refused_table(
                            &batch,
                            &op_table,
                            &too.key_digest,
                        )
                    {
                        return Err(too_large(i, too));
                    }
                    return Err(SourceError::Other(
                        e.context("persist a forward-proof binding"),
                    ));
                }
            };
            let seqs = match committed {
                CommitResult::Committed(seqs) => seqs,
                CommitResult::Conflict(why) => {
                    if attempt > BATCH_CONFLICT_RETRIES {
                        return Err(SourceError::Other(anyhow::anyhow!(
                            "the forward proof's batch conflicted {attempt} times \
                             (last: {why}); the DDL stays pending"
                        )));
                    }
                    warn!(source_id = %ctx.source_id, attempt, why, "forward proof: the batch conflicted; planning again");
                    continue;
                }
            };
            // Committed: the registry's cache learns it, and only now.
            for (reg, op) in regs.iter().zip(&entry_ops) {
                if let Some(op) = op {
                    registry.admit_committed(
                        reg,
                        seqs[*op].expect("a committed log write"),
                    );
                }
            }
            for ((&i, reg), op) in
                batch_tables.iter().zip(&regs).zip(&binding_ops)
            {
                let (p, cap) = &proven[i];
                trace_proof(
                    tag,
                    &p.db,
                    &p.table,
                    Some(cap),
                    Some(bound.report),
                    lctn,
                    "proven",
                );
                out.push(Committed {
                    table: i,
                    binding_seq: seqs[*op].expect("a committed log write"),
                    version: reg.version,
                    hash: reg.hash.clone(),
                });
            }
            break;
        }
    }
    Ok(CommitOutcome {
        committed: out,
        halted: None,
    })
}

/// The committed tables' timelines that are still current (C3), each its
/// snapshot followed by exactly its `ddl` record and its binding, decoded
/// from the store as a load decodes them and validated as a load validates
/// them. Current means, at this read: the table stream holds exactly those
/// two entries after the snapshot's head, and every barrier stream is where
/// the snapshot read it. Freshness holds at the read, not through the
/// install; the interval after it is safe only because no other writer
/// exists, so a table has a snapshot only under the store-gate witness
/// ([`storage::adapters::store_gate::server_holds`], checked where the
/// snapshot is taken). A table that fails any check, or a read error, is
/// left out (the caller invalidates it).
async fn fresh_timelines(
    ctx: &RunCtx,
    scope: &RegistryScope,
    proven: &[(Pending, Captured)],
    committed: &[Committed],
) -> Vec<Fresh> {
    use super::mysql_activation::{ACTIVATION_NS, BARRIER_NS, table_stream};
    use storage::{BatchRead, BatchReadResult};

    let backend = &ctx.registry_backend;
    let candidates: Vec<(&Committed, &Snapshot)> = committed
        .iter()
        .filter_map(|c| proven[c.table].0.snapshot.as_ref().map(|s| (c, s)))
        .collect();
    if candidates.is_empty() {
        return Vec::new();
    }
    let reads: Vec<BatchRead> = candidates
        .iter()
        .flat_map(|(_, s)| {
            s.timeline.streams.iter().map(|b| BatchRead::LogHead {
                ns: BARRIER_NS.into(),
                key: b.stream.clone(),
            })
        })
        .collect();
    let heads = match backend.read_batch(&reads).await {
        Ok(h) if h.len() == reads.len() => h,
        other => {
            warn!(source_id = %ctx.source_id, outcome = ?other.map(|h| h.len()), "forward proof: barrier heads unread; the proven timelines reload");
            return Vec::new();
        }
    };
    let mut heads = heads.into_iter();
    let mut out = Vec::new();
    for (c, snap) in candidates {
        let t = &snap.timeline;
        // Every head is consumed, current or not, to keep the rest aligned.
        let mut barriers_current = true;
        for b in &t.streams {
            barriers_current &= matches!(
                heads.next(),
                Some(BatchReadResult::Head(h)) if h == b.head
            );
        }
        if !barriers_current {
            continue;
        }
        let (p, _) = &proven[c.table];
        let key = scope.key(&p.db, &p.table);
        let stream = table_stream(&key);
        let after = match backend
            .log_read_meta_since(ACTIVATION_NS, &stream, t.table_head, 3)
            .await
        {
            Ok(after) => after,
            Err(e) => {
                warn!(source_id = %ctx.source_id, error = ?e, "forward proof: the table stream is unread; its timeline reloads");
                continue;
            }
        };
        if !matches!(
            after.as_slice(),
            [ddl, binding] if ddl.seq == snap.ddl_seq && binding.seq == c.binding_seq
        ) {
            continue;
        }
        let Ok((records, head)) =
            super::mysql_activation::decode_stream(&stream, &after)
        else {
            continue;
        };
        let table = super::mysql_history::TableStream {
            records: t.records.iter().cloned().chain(records).collect(),
            head,
            bytes: t.bytes
                + after.iter().map(storage::log_entry_bytes).sum::<u64>(),
        };
        if super::mysql_history::within_limit(&key, &table, &t.streams).is_err()
        {
            continue;
        }
        let timeline = Timeline {
            records: table.records,
            table_head: table.head,
            streams: t.streams.clone(),
            bytes: table.bytes,
        };
        let planned = PlannedVersions::from([(c.version, c.hash.clone())]);
        if super::mysql_selection::validate_planned(
            ctx.schema.registry(),
            &key,
            &timeline,
            &planned,
        )
        .await
        .is_err()
        {
            continue;
        }
        out.push(Fresh {
            key,
            snapshot: t.clone(),
            timeline,
        });
    }
    out
}

/// Test seams of C3's interval between the freshness check and the install.
#[cfg(test)]
pub(crate) mod hooks {
    use super::RunCtx;
    use std::sync::Mutex;

    /// What [`BEFORE_INSTALL`] runs.
    pub(crate) type BeforeInstall = Box<dyn FnOnce(&mut RunCtx) + Send>;

    /// One-shot: runs on the source's task after the proof's freshness
    /// check, before the install (a writer acting in that interval).
    pub(crate) static BEFORE_INSTALL: Mutex<Option<BeforeInstall>> =
        Mutex::new(None);

    /// Every timeline installed, with its table's backend key and the
    /// snapshot's estimated bytes.
    pub(crate) static INSTALLED: Mutex<
        Vec<(String, u64, super::super::mysql_selection::Timeline)>,
    > = Mutex::new(Vec::new());

    pub(crate) fn before_install(ctx: &mut RunCtx) {
        let hook = BEFORE_INSTALL.lock().unwrap().take();
        if let Some(hook) = hook {
            hook(ctx);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Caps, Cost, WindowInvalid, group};

    fn caps() -> Caps {
        Caps {
            plan: Cost {
                bytes: 100,
                read_items: 50,
                read_key_bytes: 10_000,
                write_ops: 20,
            },
            table_bytes: 1000,
        }
    }

    fn cost(bytes: u64, read_items: u64) -> (usize, Cost) {
        (
            0,
            Cost {
                bytes,
                read_items,
                read_key_bytes: read_items,
                write_ops: 7,
            },
        )
    }

    fn costs(of: &[(u64, u64)]) -> Vec<(usize, Cost)> {
        of.iter()
            .enumerate()
            .map(|(i, (b, r))| (i, cost(*b, *r).1))
            .collect()
    }

    /// Tables are batched in order, each batch closing before the table
    /// that would take it over a cap; every table is in exactly one batch.
    #[test]
    fn tables_are_batched_in_order_within_the_caps() {
        assert_eq!(
            group(caps(), &costs(&[(40, 1), (40, 1), (40, 1), (10, 1)])),
            Ok(vec![vec![0, 1], vec![2, 3]])
        );
        assert_eq!(
            group(caps(), &costs(&[(1, 30), (1, 30), (1, 10)])),
            Ok(vec![vec![0], vec![1, 2]])
        );
        assert_eq!(group(caps(), &[]), Ok(Vec::<Vec<usize>>::new()));
    }

    /// A table no batch can take alone is refused, by its index, even
    /// under the per-table limit (over a batch cap): nothing is grouped.
    #[test]
    fn a_table_over_a_batch_cap_is_refused_before_any_batch() {
        assert_eq!(
            group(caps(), &costs(&[(10, 1), (101, 1)])),
            Err((
                1,
                WindowInvalid::StateTooLarge {
                    bytes: 101,
                    limit: 100
                }
            ))
        );
        assert_eq!(
            group(caps(), &costs(&[(10, 51)])),
            Err((
                0,
                WindowInvalid::RegistryTooLarge {
                    read_items: 51,
                    limit: 50
                }
            ))
        );
    }
    /// The proof's store path without MySQL: [`commit_proofs`] and
    /// [`fresh_timelines`] on a real (in-memory) store, with synthetic
    /// captures and injected store faults. The live tests below cover the
    /// same path end to end against MySQL.
    mod store {
        use super::super::*;
        use crate::gtid_set::GtidSet;
        use crate::mysql::mysql_activation::{
            ACTIVATION_NS, BARRIER_NS, BarrierScope, EventIdentity, Kind,
            Record, append, barrier_stream, read_table, record_ddl,
            table_stream,
        };
        use crate::mysql::mysql_binlog_scan::ScanTag;
        use crate::mysql::mysql_schema_loader::MySqlSchemaLoader;
        use crate::mysql::mysql_table_schema::{MySqlColumn, MySqlTableSchema};
        use deltaforge_core::incident::ReasonCode;
        use std::time::Duration;
        use storage::adapters::test_util::{BatchHook, FaultBackend};
        use storage::{
            ArcStorageBackend, BatchFault, BatchFaults, MemoryStorageBackend,
        };

        const U: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562";

        /// A store whose batches can fail inside the transaction
        /// (`memory`), behind the fault wrapper the context writes through.
        struct Store {
            memory: Arc<MemoryStorageBackend>,
            fault: Arc<FaultBackend>,
        }

        fn store() -> Store {
            let memory = Arc::new(MemoryStorageBackend::new());
            let fault = Arc::new(FaultBackend::wrap(memory.clone()));
            Store { memory, fault }
        }

        async fn ctx_on(backend: ArcStorageBackend) -> RunCtx {
            let (tx, _rx) = tokio::sync::mpsc::channel(8);
            let mut ctx = crate::mysql::mysql_event::tests::make_runctx(tx);
            ctx.registry_backend = backend.clone();
            let registry =
                DurableSchemaRegistry::new(backend.clone()).await.unwrap();
            ctx.schema = MySqlSchemaLoader::new(
                "mysql://fake",
                registry,
                &ctx.tenant,
                ctx.registry_scope.clone(),
            );
            crate::registry_scope::establish_scope(
                &backend,
                &ctx.registry_scope,
                &ctx.tenant,
                &ctx.source_id,
                storage::adapters::LineageDescriptor::mysql(U).unwrap(),
            )
            .await
            .unwrap();
            ctx.last_gtid = Some(GtidSet::parse(&format!("{U}:1-60")).unwrap());
            ctx
        }

        fn at(k: u64) -> MySqlCheckpoint {
            MySqlCheckpoint {
                file: "mysql-bin.000001".into(),
                pos: 4 + 100 * k,
                gtid_set: Some(format!("{U}:1-{k}")),
                lineage: None,
                snapshot_completed: None,
                snapshot_chain: None,
            }
        }

        fn wm(k: u64) -> WmPos {
            let p = at(k);
            mysql_checkpoint_position(&p.file, p.pos, p.gtid_set.as_deref())
                .unwrap()
        }

        fn key(ctx: &RunCtx, t: &str) -> SchemaKey {
            ctx.registry_scope.current().unwrap().key("d", t)
        }

        /// A capture of `t` at S (70) with the columns `id` and `col`.
        fn captured(col: &str) -> Captured {
            Captured {
                from: at(65),
                position: at(70),
                schema: MySqlTableSchema::new(vec![
                    MySqlColumn::new("id", "int", "int", true, 1),
                    MySqlColumn::new(col, "int", "int", true, 2),
                ]),
                attempts: 1,
                trace: Default::default(),
            }
        }

        fn report() -> ScanReport {
            ScanReport {
                statements: Vec::new(),
                events: 0,
                bytes: 0,
                duration: Duration::ZERO,
                digest: "digest".into(),
                trace: Default::default(),
                frontier: None,
                session_anchor: None,
                positioned_at: None,
                work: Default::default(),
            }
        }

        /// The tables' pending `ddl` records at D (60), as the stream writes
        /// them, each with its cached timeline when `snapshot` (taken before
        /// the record, as the caller takes it).
        async fn pending(
            ctx: &mut RunCtx,
            tables: &[(&str, &str)],
            snapshot: bool,
        ) -> Vec<(Pending, Captured)> {
            let mut out = Vec::new();
            for (i, (t, col)) in tables.iter().enumerate() {
                let k = key(ctx, t);
                let cached = if snapshot {
                    crate::mysql::mysql_selection::load_timelines(
                        ctx,
                        std::slice::from_ref(&k),
                    )
                    .await;
                    Some(
                        ctx.selection
                            .timeline(&k.backend_key())
                            .expect("cached"),
                    )
                } else {
                    None
                };
                let (ddl_id, stored) = record_ddl(
                    &ctx.registry_backend,
                    ctx.schema.registry(),
                    &k,
                    &EventIdentity::Gtid {
                        gtid: format!("{U}:60"),
                        ordinal: i as u32,
                    },
                    wm(60),
                )
                .await
                .unwrap();
                out.push((
                    Pending {
                        db: "d".into(),
                        table: t.to_string(),
                        ddl_id,
                        snapshot: cached.map(|timeline| Snapshot {
                            timeline,
                            ddl_seq: stored.seq,
                        }),
                    },
                    captured(col),
                ));
            }
            out
        }

        /// [`commit_proofs`] of `proven` at D = 60, S = 70.
        async fn commit(
            ctx: &RunCtx,
            proven: &[(Pending, Captured)],
        ) -> SourceResult<Vec<Committed>> {
            let scope = ctx.registry_scope.current().unwrap();
            let lineage = scope.lineage().lineage_hash.clone();
            let (d, s, report) = (wm(60), wm(70), report());
            let checkpoint = serde_json::to_vec(&at(60)).unwrap();
            let tag = ScanTag {
                source_id: &ctx.source_id,
                kind: crate::mysql::mysql_binlog_scan::ProofKind::Forward,
            };
            let bound = Bound {
                d: &d,
                s: &s,
                report: &report,
                checkpoint: &checkpoint,
                lineage: &lineage,
            };
            let cancel = CancellationToken::new();
            let halt = Halt {
                deadline: Instant::now() + Duration::from_secs(3600),
                cancel: &cancel,
            };
            commit_proofs(ctx, &scope, &tag, &bound, proven, &halt)
                .await
                .map(|o| o.committed)
        }

        /// What the store holds for `t`, read through a registry of its own
        /// (no cache of the context's): the registered versions' hashes, and
        /// the version the validated binding of `ddl_id` binds.
        async fn durable(
            ctx: &RunCtx,
            t: &str,
            ddl_id: &str,
        ) -> (Vec<String>, Option<i32>) {
            let k = key(ctx, t);
            let registry =
                DurableSchemaRegistry::new(ctx.registry_backend.clone())
                    .await
                    .unwrap();
            let mut hashes = Vec::new();
            for v in 1.. {
                match registry.version_hash(&k, v).await.unwrap() {
                    Some(h) => hashes.push(h),
                    None => break,
                }
            }
            let records = read_table(&ctx.registry_backend, &registry, &k)
                .await
                .unwrap();
            assert_eq!(
                records.iter().filter(|s| s.capture_id == ddl_id).count(),
                1,
                "the pending record stays, once: {records:?}"
            );
            let bound = resolve_binding(
                &registry,
                &k,
                &records,
                ddl_id,
                CLASSIFIER_VERSION,
            )
            .await
            .expect("valid binding evidence");
            (hashes, bound)
        }

        fn hash(cap: &Captured) -> String {
            cap.schema.fingerprint().to_string()
        }

        /// Every table's registration and binding commit together: the
        /// store holds one version per table, bound by validated evidence,
        /// and the context's registry reports it.
        #[tokio::test]
        async fn a_proof_commits_each_table_registered_and_bound() {
            let st = store();
            let mut ctx = ctx_on(st.fault.clone()).await;
            let proven =
                pending(&mut ctx, &[("a", "x"), ("b", "y")], false).await;
            let committed = commit(&ctx, &proven).await.unwrap();
            assert_eq!(committed.len(), 2);
            for (p, cap) in &proven {
                let (hashes, bound) = durable(&ctx, &p.table, &p.ddl_id).await;
                assert_eq!(hashes, [hash(cap)], "{}", p.table);
                assert_eq!(bound, Some(1), "{}", p.table);
                let latest = ctx
                    .schema
                    .registry()
                    .get_latest(&key(&ctx, &p.table))
                    .await
                    .unwrap()
                    .expect("the committed version");
                assert_eq!((latest.version, latest.hash), (1, hash(cap)));
            }
        }

        /// A store that fails inside the batch's transaction, on every
        /// attempt: nothing of the proof is stored (no version, no binding,
        /// the `ddl` record pending) and the context's registry learned
        /// nothing. Once the store recovers, the same proof commits once.
        #[tokio::test]
        async fn a_failed_batch_leaves_nothing_of_the_proof() {
            fn fail_every_batch(
                st: Arc<FaultBackend>,
                memory: Arc<MemoryStorageBackend>,
            ) {
                let again = st.clone();
                *st.before_next_batch.lock().unwrap() =
                    Some(BatchHook(Box::new(move |_inner| {
                        Box::pin(async move {
                            memory.set_batch_fault(Some(
                                BatchFault::AfterWrites(1),
                            ));
                            fail_every_batch(again, memory);
                        })
                    })));
            }
            let st = store();
            let mut ctx = ctx_on(st.fault.clone()).await;
            let proven =
                pending(&mut ctx, &[("a", "x"), ("b", "y")], false).await;
            fail_every_batch(st.fault.clone(), st.memory.clone());
            assert!(commit(&ctx, &proven).await.is_err());
            *st.fault.before_next_batch.lock().unwrap() = None;
            st.memory.set_batch_fault(None);
            for (p, _) in &proven {
                assert_eq!(
                    durable(&ctx, &p.table, &p.ddl_id).await,
                    (vec![], None)
                );
                assert!(
                    ctx.schema
                        .registry()
                        .get_latest(&key(&ctx, &p.table))
                        .await
                        .unwrap()
                        .is_none(),
                    "no version admitted from an uncommitted batch"
                );
            }

            commit(&ctx, &proven).await.unwrap();
            for (p, cap) in &proven {
                assert_eq!(
                    durable(&ctx, &p.table, &p.ddl_id).await,
                    (vec![hash(cap)], Some(1))
                );
            }
        }

        /// An attempt whose outcome the store did not acknowledge is
        /// resolved from its commit marker: committed (acknowledgement lost)
        /// or not (failed before the commit, applied again). Either way the
        /// store holds exactly one version and one binding per table, and
        /// the returned binding is the stored one.
        #[tokio::test]
        async fn an_unacknowledged_batch_is_resolved_from_its_marker() {
            for fault in [BatchFault::AfterCommit, BatchFault::AfterWrites(1)] {
                let st = store();
                let mut ctx = ctx_on(st.fault.clone()).await;
                let proven = pending(&mut ctx, &[("a", "x")], false).await;
                st.memory.set_batch_fault(Some(fault));
                let committed = commit(&ctx, &proven).await.unwrap();
                assert!(!st.memory.batch_fault_armed(), "{fault:?} was hit");
                let (p, cap) = &proven[0];
                assert_eq!(
                    durable(&ctx, "a", &p.ddl_id).await,
                    (vec![hash(cap)], Some(1)),
                    "{fault:?}"
                );
                let k = key(&ctx, "a");
                let entries = ctx
                    .registry_backend
                    .log_read_meta_since(
                        ACTIVATION_NS,
                        &table_stream(&k),
                        0,
                        10,
                    )
                    .await
                    .unwrap();
                assert_eq!(entries.len(), 2, "the ddl record and one binding");
                assert_eq!(
                    committed[0].binding_seq, entries[1].seq,
                    "{fault:?}"
                );
            }
        }

        /// A table whose registration entry would exceed the store's item
        /// limit is refused with `state_item_too_large` before any batch:
        /// no table of the proof is written, the smaller one included.
        #[tokio::test]
        async fn an_oversized_item_is_refused_before_any_write() {
            let st = store();
            let mut ctx = ctx_on(st.fault.clone()).await;
            let mut proven =
                pending(&mut ctx, &[("a", "x"), ("b", "y")], false).await;
            let huge = "c".repeat(storage::MAX_STATE_ITEM_BYTES);
            proven[1].1.schema = MySqlTableSchema::new(vec![MySqlColumn::new(
                "id", &huge, "int", true, 1,
            )]);
            let err = commit(&ctx, &proven).await.err().expect("refused");
            assert_eq!(
                err.draft().map(|d| d.reason_code),
                Some(ReasonCode::StateItemTooLarge),
                "{err}"
            );
            for (p, _) in &proven {
                assert_eq!(
                    durable(&ctx, &p.table, &p.ddl_id).await,
                    (vec![], None)
                );
            }
        }

        /// A table no batch can take alone (here: a source with so many
        /// earlier lineages that one registration's reads exceed a plan) is
        /// refused with its classified incident (here the registry's reads:
        /// `mysql_table_registry_too_large`) before any batch: no table of
        /// the proof is written.
        #[tokio::test]
        async fn a_table_no_batch_can_take_is_refused_before_any_write() {
            let st = store();
            let mut ctx = ctx_on(st.fault.clone()).await;
            let proven =
                pending(&mut ctx, &[("a", "x"), ("b", "y")], false).await;
            let backend = ctx.registry_backend.clone();
            let mut lineage = storage::adapters::source_lineage::load(
                &backend,
                &ctx.tenant,
                &ctx.source_id,
            )
            .await
            .unwrap()
            .unwrap();
            let caps =
                super::super::super::mysql_window_budget::PLAN_READ_ITEMS;
            lineage.prior_lineage_hashes =
                (0..caps / 2).map(|i| format!("{i:032x}")).collect();
            // The record as the store keeps it (`schema_lineage`,
            // `{tenant}/{source_id}`); the load below proves the layout.
            backend
                .kv_put(
                    "schema_lineage",
                    &format!("{}/{}", ctx.tenant, ctx.source_id),
                    &serde_json::to_vec(&lineage).unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                storage::adapters::source_lineage::load(
                    &backend,
                    &ctx.tenant,
                    &ctx.source_id
                )
                .await
                .unwrap()
                .unwrap()
                .prior_lineage_hashes
                .len() as u64,
                caps / 2
            );
            let err = commit(&ctx, &proven).await.err().expect("refused");
            assert_eq!(
                err.draft().map(|d| d.reason_code),
                Some(ReasonCode::MysqlTableRegistryTooLarge),
                "{err}"
            );
            for (p, _) in &proven {
                assert_eq!(
                    durable(&ctx, &p.table, &p.ddl_id).await,
                    (vec![], None)
                );
            }
        }

        /// Another writer registering the table before every batch
        /// commits: `n` times, the proof plans again and binds the version
        /// after the other writer's; on every attempt, the proof stops
        /// after its bounded re-plans with nothing of it stored.
        #[tokio::test]
        async fn a_conflicting_batch_is_planned_again_a_bounded_number_of_times()
         {
            use super::super::super::mysql_activation_window::BATCH_CONFLICT_RETRIES;
            fn register_before(
                st: Arc<FaultBackend>,
                key: SchemaKey,
                left: Arc<std::sync::atomic::AtomicUsize>,
            ) {
                let again = st.clone();
                *st.before_next_batch.lock().unwrap() =
                    Some(BatchHook(Box::new(move |inner| {
                        Box::pin(async move {
                            use std::sync::atomic::Ordering::SeqCst;
                            if left.fetch_sub(1, SeqCst) == 0 {
                                left.store(0, SeqCst);
                                return;
                            }
                            let other = DurableSchemaRegistry::new(inner)
                                .await
                                .unwrap();
                            let n = other
                                .get_latest(&key)
                                .await
                                .unwrap()
                                .map_or(0, |v| v.version);
                            other
                                .register_with_checkpoint(
                                    &key,
                                    &format!("other-{n}"),
                                    &serde_json::json!({ "other": n }),
                                    None,
                                )
                                .await
                                .unwrap();
                            register_before(again, key, left);
                        })
                    })));
            }
            for (others, commits) in [
                (1, true),
                (BATCH_CONFLICT_RETRIES, true),
                (BATCH_CONFLICT_RETRIES + 1, false),
            ] {
                let st = store();
                let mut ctx = ctx_on(st.fault.clone()).await;
                let proven = pending(&mut ctx, &[("a", "x")], false).await;
                let k = key(&ctx, "a");
                register_before(
                    st.fault.clone(),
                    k.clone(),
                    Arc::new(std::sync::atomic::AtomicUsize::new(others)),
                );
                let result = commit(&ctx, &proven).await;
                *st.fault.before_next_batch.lock().unwrap() = None;
                let (p, cap) = &proven[0];
                let (hashes, bound) = durable(&ctx, "a", &p.ddl_id).await;
                let theirs: Vec<String> =
                    (0..others).map(|n| format!("other-{n}")).collect();
                if commits {
                    assert_eq!(result.unwrap().len(), 1, "{others}");
                    assert_eq!(hashes[..others], theirs[..]);
                    assert_eq!(hashes[others..], [hash(cap)], "after theirs");
                    assert_eq!(bound, Some(others as i32 + 1));
                } else {
                    assert!(result.is_err(), "{others}");
                    assert_eq!(hashes, theirs, "no version from the proof");
                    assert_eq!(bound, None);
                }
            }
        }

        /// The committed tables' timelines, found current or not.
        async fn fresh(
            ctx: &RunCtx,
            proven: &[(Pending, Captured)],
            committed: &[Committed],
        ) -> Vec<Fresh> {
            let scope = ctx.registry_scope.current().unwrap();
            fresh_timelines(ctx, &scope, proven, committed).await
        }

        /// A proven table whose stream holds exactly its snapshot, its
        /// `ddl` record and its binding is current: the timeline equals a
        /// load of the store, and installs while the cache still holds the
        /// snapshot. Once the cache has moved on, nothing installs.
        #[tokio::test]
        async fn a_current_timeline_equals_a_load_and_installs_only_over_its_snapshot()
         {
            let st = store();
            let mut ctx = ctx_on(st.fault.clone()).await;
            let proven =
                pending(&mut ctx, &[("a", "x"), ("b", "y")], true).await;
            let committed = commit(&ctx, &proven).await.unwrap();
            let got = fresh(&ctx, &proven, &committed).await;
            assert_eq!(got.len(), 2);
            let mut loaded = ctx_on(st.fault.clone()).await;
            for f in &got {
                crate::mysql::mysql_selection::load_timelines(
                    &mut loaded,
                    std::slice::from_ref(&f.key),
                )
                .await;
                let load =
                    loaded.selection.timeline(&f.key.backend_key()).unwrap();
                assert_eq!(f.timeline.records, load.records);
                assert_eq!(f.timeline.table_head, load.table_head);
                assert_eq!(f.timeline.streams.len(), load.streams.len());
            }
            let (a, b) = (&got[0], &got[1]);
            assert!(ctx.selection.install_if_current(
                &a.key,
                &a.snapshot,
                Arc::new(a.timeline.clone())
            ));
            assert_eq!(
                ctx.selection
                    .timeline(&a.key.backend_key())
                    .unwrap()
                    .records,
                a.timeline.records
            );
            ctx.selection.invalidate(&b.key);
            assert!(!ctx.selection.install_if_current(
                &b.key,
                &b.snapshot,
                Arc::new(b.timeline.clone())
            ));
            assert!(ctx.selection.timeline(&b.key.backend_key()).is_none());
        }

        /// Not current, so the caller reloads instead: another writer's
        /// entry in the table stream or in a barrier stream of the table
        /// after the snapshot, a table without a snapshot (a replayed
        /// record, or no store-gate witness), or a store read that fails.
        #[tokio::test]
        async fn a_timeline_not_current_is_left_to_reload() {
            for case in ["table", "barrier", "no snapshot", "read fails"] {
                let st = store();
                let mut ctx = ctx_on(st.fault.clone()).await;
                let proven =
                    pending(&mut ctx, &[("a", "x")], case != "no snapshot")
                        .await;
                let k = key(&ctx, "a");
                // Another writer's entry, after the snapshot.
                let foreign = match case {
                    "table" => {
                        Some((ACTIVATION_NS, table_stream(&k), Kind::Ddl))
                    }
                    "barrier" => {
                        let scope = BarrierScope::Database { db: "d".into() };
                        Some((
                            BARRIER_NS,
                            barrier_stream(&k, &scope),
                            Kind::Barrier { scope },
                        ))
                    }
                    _ => None,
                };
                if let Some((ns, stream, kind)) = foreign {
                    append(
                        &ctx.registry_backend,
                        ctx.schema.registry(),
                        &k,
                        ns,
                        &stream,
                        "foreign",
                        &Record::new(wm(61), kind),
                    )
                    .await
                    .unwrap();
                }
                let committed = commit(&ctx, &proven).await.unwrap();
                st.fault.fail_log_read_meta.store(
                    case == "read fails",
                    std::sync::atomic::Ordering::SeqCst,
                );
                assert!(
                    fresh(&ctx, &proven, &committed).await.is_empty(),
                    "{case}"
                );
                st.fault
                    .fail_log_read_meta
                    .store(false, std::sync::atomic::Ordering::SeqCst);
                let (p, cap) = &proven[0];
                assert_eq!(
                    durable(&ctx, "a", &p.ddl_id).await,
                    (vec![hash(cap)], Some(1)),
                    "{case}: the proof itself is committed"
                );
            }
        }

        /// The retry loop ([`prove_with`]) over scripted MySQL work, on the
        /// real store and incident store, on a paused clock: the deadline,
        /// the attempt bound, the table states and the incident's outcome
        /// (`docs/design/mysql-forward-proof-retry.md`).
        mod retry {
            use super::*;
            use crate::mysql::mysql_binlog_scan::Cancelled;
            use crate::mysql::mysql_failure::Failure;
            use deltaforge_core::incident::{
                CauseCode, EvidenceKey, EvidenceValue, Retryability,
            };
            use std::collections::{HashMap, VecDeque};
            use std::sync::Mutex;
            use storage::adapters::incidents::{
                IncidentRecord, IncidentStatus, Resolution,
            };

            /// One scripted capture: its outcome after its time (capped by
            /// the capture's lock wait, as a real lock wait is).
            #[derive(Clone)]
            enum Step {
                Ok(&'static str, Duration),
                Fail(ProofError, Duration),
            }

            fn contention() -> Step {
                Step::Fail(
                    ProofError::Contention {
                        code: 1205,
                        why: "lock wait".into(),
                    },
                    Duration::MAX,
                )
            }

            fn ok() -> Step {
                Step::Ok("x", Duration::ZERO)
            }

            /// Each table's captures in turn (the last repeats); scans take
            /// `scan_time` and succeed with an empty report.
            #[derive(Default)]
            struct Script {
                captures: Mutex<HashMap<String, VecDeque<Step>>>,
                scan_time: Mutex<Duration>,
                calls: Mutex<Vec<(String, Instant)>>,
            }

            impl Script {
                fn with(steps: &[(&str, Vec<Step>)]) -> Self {
                    let s = Self::default();
                    for (t, v) in steps {
                        s.captures
                            .lock()
                            .unwrap()
                            .insert(t.to_string(), v.iter().cloned().collect());
                    }
                    s
                }

                /// The captures so far: table and start, from `t0`.
                fn calls(&self, t0: Instant) -> Vec<(String, u64)> {
                    self.calls
                        .lock()
                        .unwrap()
                        .iter()
                        .map(|(t, at)| (t.clone(), (*at - t0).as_secs()))
                        .collect()
                }
            }

            impl Probe for Script {
                async fn capture_table(
                    &self,
                    _db: &str,
                    table: &str,
                    lock_wait: Duration,
                ) -> Result<Result<Captured, ProofError>, Cancelled>
                {
                    self.calls
                        .lock()
                        .unwrap()
                        .push((table.to_string(), Instant::now()));
                    let step = {
                        let mut all = self.captures.lock().unwrap();
                        let q = all.entry(table.to_string()).or_default();
                        if q.len() > 1 {
                            q.pop_front().unwrap()
                        } else {
                            q.front().cloned().unwrap_or_else(ok)
                        }
                    };
                    match step {
                        Step::Ok(col, t) => {
                            tokio::time::sleep(t.min(lock_wait)).await;
                            Ok(Ok(captured(col)))
                        }
                        Step::Fail(e, t) => {
                            tokio::time::sleep(t.min(lock_wait)).await;
                            Ok(Err(e))
                        }
                    }
                }

                async fn scan(
                    &self,
                    _d: &MySqlCheckpoint,
                    _s: &MySqlCheckpoint,
                ) -> Result<ScanReport, ProofError> {
                    let t = *self.scan_time.lock().unwrap();
                    tokio::time::sleep(t).await;
                    Ok(report())
                }
            }

            /// A context whose retries run within `retry`.
            async fn ctx_with(
                st: &Store,
                retry: deltaforge_config::ForwardProofRetry,
            ) -> RunCtx {
                let mut ctx = ctx_on(st.fault.clone()).await;
                ctx.proofs.forward_proof_retry = retry;
                ctx
            }

            fn bounds() -> deltaforge_config::ForwardProofRetry {
                Default::default()
            }

            async fn tables(ctx: &mut RunCtx, names: &[&str]) -> Vec<Pending> {
                let named: Vec<(&str, &str)> =
                    names.iter().map(|t| (*t, "x")).collect();
                pending(ctx, &named, false)
                    .await
                    .into_iter()
                    .map(|(p, _)| p)
                    .collect()
            }

            /// The identity the supervisor gives the draft the source stops
            /// on (bound to the current recovery epoch, as the retrying
            /// incident was).
            async fn stop_identity(
                ctx: &RunCtx,
                draft: &deltaforge_core::incident::IncidentDraft,
            ) -> deltaforge_core::incident::IncidentId {
                let epoch = ctx.incidents.recovery_epoch().await.unwrap();
                storage::adapters::incidents::bind_epoch(draft.clone(), epoch)
                    .incident_id(&ctx.pipeline)
            }

            async fn only_incident(ctx: &RunCtx) -> IncidentRecord {
                let all = ctx.incidents.list().await.unwrap();
                assert_eq!(all.len(), 1, "{all:?}");
                all.into_iter().next().unwrap()
            }

            fn recovered(rec: &IncidentRecord) -> bool {
                matches!(
                    &rec.status,
                    IncidentStatus::Resolved {
                        by: Resolution::VerifiedRecovery { check },
                        ..
                    } if check == "forward_proof_bound"
                )
            }

            fn withdrawn(rec: &IncidentRecord, why: &str) -> bool {
                matches!(
                    rec.status,
                    IncidentStatus::Resolved {
                        by: Resolution::OperationCancelled,
                        ..
                    }
                ) && rec.evidence.holds(EvidenceKey::ReasonClass, why)
            }

            fn count(
                e: &deltaforge_core::incident::Evidence,
                k: EvidenceKey,
            ) -> u64 {
                match e.get(k) {
                    Some(EvidenceValue::Count { value }) => *value,
                    other => panic!("{k:?}: {other:?}"),
                }
            }

            /// With the production bounds, attempts start at 0, 31, 65 and
            /// 111 s (each 30 s lock wait, then 1, 4 and 16 s of backoff);
            /// the fourth, the last the bounds permit, has 9 s and proves
            /// the table. The retrying incident is a verified recovery.
            #[tokio::test(start_paused = true)]
            async fn the_last_permitted_attempt_can_recover() {
                let st = store();
                let mut ctx = ctx_with(&st, bounds()).await;
                let p = tables(&mut ctx, &["a"]).await;
                let id = p[0].ddl_id.clone();
                let script = Script::with(&[(
                    "a",
                    vec![contention(), contention(), contention(), ok()],
                )]);
                let t0 = Instant::now();
                prove_with(&ctx, &script, p).await.unwrap();
                assert_eq!(
                    script.calls(t0),
                    [
                        ("a".into(), 0),
                        ("a".into(), 31),
                        ("a".into(), 65),
                        ("a".into(), 111)
                    ]
                );
                assert_eq!(durable(&ctx, "a", &id).await.1, Some(1));
                assert!(recovered(&only_incident(&ctx).await));
            }

            /// Every attempt fails: four, the last started at 111 s with the
            /// 9 s left, then the stop's operator-action draft of the same
            /// incident; nothing is bound and nothing ran past 120 s.
            #[tokio::test(start_paused = true)]
            async fn four_failed_attempts_exhaust_the_proof() {
                let st = store();
                let mut ctx = ctx_with(&st, bounds()).await;
                let p = tables(&mut ctx, &["a"]).await;
                let id = p[0].ddl_id.clone();
                let script = Script::with(&[("a", vec![contention()])]);
                let t0 = Instant::now();
                let err = prove_with(&ctx, &script, p).await.err().unwrap();
                assert!(t0.elapsed() <= Duration::from_secs(120));
                assert_eq!(
                    script.calls(t0).iter().map(|c| c.1).collect::<Vec<_>>(),
                    [0, 31, 65, 111]
                );
                let draft = err.draft().unwrap();
                assert_eq!(
                    draft.reason_code,
                    ReasonCode::MysqlForwardProofUnavailable
                );
                assert_eq!(draft.retryability, Retryability::OperatorAction);
                assert_eq!(count(&draft.evidence, EvidenceKey::Attempts), 4);
                assert_eq!(
                    count(&draft.evidence, EvidenceKey::BlockingCount),
                    1
                );
                assert!(
                    draft
                        .evidence
                        .holds(EvidenceKey::ReasonClass, "lock_wait_timeout")
                );
                let rec = only_incident(&ctx).await;
                assert_eq!(rec.retryability, Retryability::AutoRetry);
                assert_eq!(rec.incident_id, stop_identity(&ctx, draft).await);
                assert_eq!(durable(&ctx, "a", &id).await, (vec![], None));
            }

            /// No attempt starts after the deadline: with 100 s, the fourth
            /// would start at 111 s; the backoff is cut to the 5 s left and
            /// the proof is exhausted after three.
            #[tokio::test(start_paused = true)]
            async fn no_attempt_starts_after_the_deadline() {
                let st = store();
                let mut ctx = ctx_with(
                    &st,
                    deltaforge_config::ForwardProofRetry {
                        deadline: Duration::from_secs(100),
                        ..bounds()
                    },
                )
                .await;
                let p = tables(&mut ctx, &["a"]).await;
                let script = Script::with(&[("a", vec![contention()])]);
                let t0 = Instant::now();
                let err = prove_with(&ctx, &script, p).await.err().unwrap();
                assert_eq!(
                    script.calls(t0).iter().map(|c| c.1).collect::<Vec<_>>(),
                    [0, 31, 65]
                );
                assert_eq!((Instant::now() - t0).as_secs(), 100);
                assert_eq!(
                    count(
                        &err.draft().unwrap().evidence,
                        EvidenceKey::Attempts
                    ),
                    3
                );
            }

            /// Within an attempt, a capture starts only with the smallest
            /// useful budget left: after the first table's 9.5 s of a 10 s
            /// deadline, the second and third are not captured and stay
            /// retrying (`deadline`).
            #[tokio::test(start_paused = true)]
            async fn a_capture_needs_its_smallest_budget_left() {
                let st = store();
                let mut ctx = ctx_with(
                    &st,
                    deltaforge_config::ForwardProofRetry {
                        deadline: Duration::from_secs(10),
                        ..bounds()
                    },
                )
                .await;
                let p = tables(&mut ctx, &["a", "b", "c"]).await;
                let script = Script::with(&[(
                    "a",
                    vec![Step::Fail(
                        ProofError::Contention {
                            code: 1205,
                            why: "x".into(),
                        },
                        Duration::from_millis(9500),
                    )],
                )]);
                let t0 = Instant::now();
                let err = prove_with(&ctx, &script, p).await.err().unwrap();
                assert_eq!(script.calls(t0), [("a".into(), 0)]);
                let ev = &err.draft().unwrap().evidence;
                assert_eq!(count(ev, EvidenceKey::BlockingCount), 3);
                assert!(ev.holds(EvidenceKey::ReasonClass, "deadline"));
                assert!(Instant::now() - t0 <= Duration::from_secs(10));
            }

            /// A scan still running at the deadline is dropped; its tables
            /// stay retrying and the proof is exhausted, nothing bound.
            #[tokio::test(start_paused = true)]
            async fn a_scan_is_cut_at_the_deadline() {
                let st = store();
                let mut ctx = ctx_with(
                    &st,
                    deltaforge_config::ForwardProofRetry {
                        deadline: Duration::from_secs(10),
                        ..bounds()
                    },
                )
                .await;
                let p = tables(&mut ctx, &["a"]).await;
                let id = p[0].ddl_id.clone();
                let script = Script::default();
                *script.scan_time.lock().unwrap() = Duration::from_secs(60);
                let t0 = Instant::now();
                let err = prove_with(&ctx, &script, p).await.err().unwrap();
                assert_eq!((Instant::now() - t0).as_secs(), 10);
                assert!(
                    err.draft()
                        .unwrap()
                        .evidence
                        .holds(EvidenceKey::ReasonClass, "deadline")
                );
                assert_eq!(durable(&ctx, "a", &id).await.1, None);
            }

            /// Recovery is not deferral: a table that failed transiently and
            /// then is refused (untrusted evidence) ends deferred; the
            /// retrying incident is withdrawn as `ended_deferred`, never
            /// resolved as recovered, and the DDL goes on with it pending.
            #[tokio::test(start_paused = true)]
            async fn a_transient_failure_then_a_refusal_is_not_a_recovery() {
                let st = store();
                let mut ctx = ctx_with(&st, bounds()).await;
                let p = tables(&mut ctx, &["a"]).await;
                let id = p[0].ddl_id.clone();
                let script = Script::with(&[(
                    "a",
                    vec![
                        contention(),
                        Step::Fail(
                            ProofError::Positions("backwards".into()),
                            Duration::ZERO,
                        ),
                    ],
                )]);
                prove_with(&ctx, &script, p).await.unwrap();
                assert_eq!(durable(&ctx, "a", &id).await.1, None);
                let rec = only_incident(&ctx).await;
                assert!(withdrawn(&rec, "ended_deferred"), "{rec:?}");
                assert!(!recovered(&rec));
                assert_eq!(count(&rec.evidence, EvidenceKey::BlockingCount), 1);
                assert!(
                    rec.evidence.holds(EvidenceKey::Table, "d.a"),
                    "the original evidence stays"
                );
            }

            /// Mixed: `a` fails transiently and is then proven; `b` is
            /// deferred without a transient failure, outside the incident's
            /// scope. A verified recovery for `a`; `b` stays pending.
            #[tokio::test(start_paused = true)]
            async fn a_deferral_outside_the_scope_leaves_a_recovery() {
                let st = store();
                let mut ctx = ctx_with(&st, bounds()).await;
                let p = tables(&mut ctx, &["a", "b"]).await;
                let (ia, ib) = (p[0].ddl_id.clone(), p[1].ddl_id.clone());
                let script = Script::with(&[
                    ("a", vec![contention(), ok()]),
                    (
                        "b",
                        vec![Step::Fail(
                            ProofError::Capture("no such table".into()),
                            Duration::ZERO,
                        )],
                    ),
                ]);
                prove_with(&ctx, &script, p).await.unwrap();
                assert_eq!(durable(&ctx, "a", &ia).await.1, Some(1));
                assert_eq!(durable(&ctx, "b", &ib).await.1, None);
                assert_eq!(
                    script
                        .calls
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|c| c.0 == "b")
                        .count(),
                    1,
                    "a deferred table is not captured again"
                );
                assert!(recovered(&only_incident(&ctx).await));
            }

            /// A table proven in the first attempt commits once while another
            /// retries; after exhaustion the restart resolves it by its
            /// binding (not captured again, no second version) and proves
            /// the other.
            #[tokio::test(start_paused = true)]
            async fn a_free_table_commits_once_while_another_retries() {
                let st = store();
                let mut ctx = ctx_with(&st, bounds()).await;
                let p = tables(&mut ctx, &["a", "b"]).await;
                let (ia, ib) = (p[0].ddl_id.clone(), p[1].ddl_id.clone());
                let script = Script::with(&[
                    ("a", vec![ok()]),
                    ("b", vec![contention()]),
                ]);
                prove_with(&ctx, &script, p.clone()).await.err().unwrap();
                assert_eq!(
                    script
                        .calls
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|c| c.0 == "a")
                        .count(),
                    1
                );
                let (versions, bound) = durable(&ctx, "a", &ia).await;
                assert_eq!((versions.len(), bound), (1, Some(1)));
                assert_eq!(durable(&ctx, "b", &ib).await.1, None);

                let restart = Script::default();
                prove_with(&ctx, &restart, p).await.unwrap();
                let t0 = Instant::now();
                assert_eq!(
                    restart
                        .calls(t0)
                        .into_iter()
                        .map(|c| c.0)
                        .collect::<Vec<_>>(),
                    ["b"]
                );
                let (versions, bound) = durable(&ctx, "a", &ia).await;
                assert_eq!((versions.len(), bound), (1, Some(1)));
                assert_eq!(durable(&ctx, "b", &ib).await.1, Some(1));
            }

            /// An authentication failure after a transient one stops the
            /// source on the same incident's operator-action draft (cause
            /// `source_auth`), with no further attempt.
            #[tokio::test(start_paused = true)]
            async fn an_authentication_failure_stops_without_a_retry() {
                let st = store();
                let mut ctx = ctx_with(&st, bounds()).await;
                let p = tables(&mut ctx, &["a"]).await;
                let script = Script::with(&[(
                    "a",
                    vec![
                        contention(),
                        Step::Fail(
                            ProofError::Open(
                                Failure::Authentication,
                                "denied".into(),
                            ),
                            Duration::ZERO,
                        ),
                    ],
                )]);
                let err = prove_with(&ctx, &script, p).await.err().unwrap();
                assert_eq!(script.calls.lock().unwrap().len(), 2);
                let draft = err.draft().unwrap();
                assert_eq!(draft.retryability, Retryability::OperatorAction);
                assert_eq!(draft.cause_code, CauseCode::SourceAuth);
                assert!(
                    draft
                        .evidence
                        .holds(EvidenceKey::ReasonClass, "authentication")
                );
                let rec = only_incident(&ctx).await;
                assert_eq!(rec.incident_id, stop_identity(&ctx, draft).await);
                assert!(matches!(
                    err,
                    SourceError::Incident { ref cause, .. }
                        if matches!(**cause, SourceError::Auth { .. })
                ));
            }

            /// Authorization, with no earlier failure: the same stop, and no
            /// retrying incident was ever recorded.
            #[tokio::test(start_paused = true)]
            async fn an_authorization_failure_stops_at_once() {
                let st = store();
                let mut ctx = ctx_with(&st, bounds()).await;
                let p = tables(&mut ctx, &["a"]).await;
                let script = Script::with(&[(
                    "a",
                    vec![Step::Fail(
                        ProofError::Open(Failure::Authorization, "1142".into()),
                        Duration::ZERO,
                    )],
                )]);
                let err = prove_with(&ctx, &script, p).await.err().unwrap();
                assert_eq!(script.calls.lock().unwrap().len(), 1);
                assert!(
                    err.draft()
                        .unwrap()
                        .evidence
                        .holds(EvidenceKey::ReasonClass, "authorization")
                );
                assert!(ctx.incidents.list().await.unwrap().is_empty());
            }

            /// Another server on a retry stops the source (lineage) with no
            /// further attempt; the retrying incident is withdrawn
            /// (`ended_by_lineage`).
            #[tokio::test(start_paused = true)]
            async fn another_server_stops_the_retries() {
                let st = store();
                let mut ctx = ctx_with(&st, bounds()).await;
                let p = tables(&mut ctx, &["a"]).await;
                let script = Script::with(&[(
                    "a",
                    vec![
                        contention(),
                        Step::Fail(
                            ProofError::OtherServer("other".into()),
                            Duration::ZERO,
                        ),
                    ],
                )]);
                let err = prove_with(&ctx, &script, p).await.err().unwrap();
                assert!(matches!(err, SourceError::Lineage { .. }), "{err}");
                assert_eq!(script.calls.lock().unwrap().len(), 2);
                assert!(withdrawn(
                    &only_incident(&ctx).await,
                    "ended_by_lineage"
                ));
            }

            /// A saturated control plane on a retry stops the source on
            /// `mysql_control_plane_saturated`, as before retries existed;
            /// the retrying incident is withdrawn
            /// (`ended_by_mysql_control_plane_saturated`).
            #[tokio::test(start_paused = true)]
            async fn saturation_stops_the_retries() {
                let st = store();
                let mut ctx = ctx_with(&st, bounds()).await;
                let p = tables(&mut ctx, &["a"]).await;
                let script = Script::with(&[(
                    "a",
                    vec![
                        contention(),
                        Step::Fail(
                            ProofError::Saturated {
                                scope: "runner",
                                why: "full".into(),
                            },
                            Duration::ZERO,
                        ),
                    ],
                )]);
                let err = prove_with(&ctx, &script, p).await.err().unwrap();
                assert_eq!(
                    err.draft().map(|d| d.reason_code),
                    Some(ReasonCode::MysqlControlPlaneSaturated)
                );
                assert_eq!(script.calls.lock().unwrap().len(), 2);
                let rec = ctx
                    .incidents
                    .list()
                    .await
                    .unwrap()
                    .into_iter()
                    .find(|r| {
                        r.reason_code
                            == ReasonCode::MysqlForwardProofUnavailable
                    })
                    .unwrap();
                assert!(
                    withdrawn(&rec, "ended_by_mysql_control_plane_saturated"),
                    "{rec:?}"
                );
            }

            /// A stop during a backoff (30.5 s, in the first one), or during
            /// a capture's lock wait (40 s, in the second attempt), ends the
            /// proof at once: `Cancelled`, the retrying incident withdrawn
            /// (`ended_cancelled`), nothing bound.
            #[tokio::test(start_paused = true)]
            async fn a_stop_ends_the_retries() {
                for (at, captures) in [(30_500, 1), (40_000, 2)] {
                    let st = store();
                    let mut ctx = ctx_with(&st, bounds()).await;
                    let p = tables(&mut ctx, &["a"]).await;
                    let id = p[0].ddl_id.clone();
                    let script = Script::with(&[("a", vec![contention()])]);
                    let cancel = ctx.cancel.clone();
                    let stopper = tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_millis(at)).await;
                        cancel.cancel();
                    });
                    let t0 = Instant::now();
                    let err = prove_with(&ctx, &script, p).await.err().unwrap();
                    stopper.abort();
                    assert!(matches!(err, SourceError::Cancelled), "{err}");
                    assert_eq!(
                        (Instant::now() - t0).as_millis(),
                        u128::from(at),
                        "at once"
                    );
                    assert_eq!(script.calls.lock().unwrap().len(), captures);
                    assert!(withdrawn(
                        &only_incident(&ctx).await,
                        "ended_cancelled"
                    ));
                    assert_eq!(durable(&ctx, "a", &id).await.1, None);
                }
            }

            /// Two tables whose registrations need about 10,000 reads each
            /// (5,000 earlier lineages): one batch each.
            async fn two_batches(ctx: &RunCtx) {
                let backend = ctx.registry_backend.clone();
                let mut lineage = storage::adapters::source_lineage::load(
                    &backend,
                    &ctx.tenant,
                    &ctx.source_id,
                )
                .await
                .unwrap()
                .unwrap();
                lineage.prior_lineage_hashes =
                    (0..5000u64).map(|i| format!("{i:032x}")).collect();
                backend
                    .kv_put(
                        "schema_lineage",
                        &format!("{}/{}", ctx.tenant, ctx.source_id),
                        &serde_json::to_vec(&lineage).unwrap(),
                    )
                    .await
                    .unwrap();
            }

            /// The deadline between batches: the first batch, started before
            /// it, resolves (here after it); no second batch starts. The
            /// first table is bound once, the second not, and it stays
            /// retrying: the proof is exhausted.
            #[tokio::test(start_paused = true)]
            async fn no_batch_starts_after_the_deadline() {
                let st = store();
                let mut ctx = ctx_with(
                    &st,
                    deltaforge_config::ForwardProofRetry {
                        deadline: Duration::from_secs(10),
                        ..bounds()
                    },
                )
                .await;
                let p = tables(&mut ctx, &["a", "b"]).await;
                let (ia, ib) = (p[0].ddl_id.clone(), p[1].ddl_id.clone());
                two_batches(&ctx).await;
                *st.fault.before_next_batch.lock().unwrap() =
                    Some(BatchHook(Box::new(|_| {
                        Box::pin(tokio::time::sleep(Duration::from_secs(20)))
                    })));
                let batches = || {
                    st.fault
                        .write_calls
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|c| *c == "batch")
                        .count()
                };
                let before = batches();
                let err = prove_with(&ctx, &Script::default(), p)
                    .await
                    .err()
                    .unwrap();
                assert_eq!(batches() - before, 1, "no second batch");
                assert_eq!(durable(&ctx, "a", &ia).await.1, Some(1));
                assert_eq!(durable(&ctx, "b", &ib).await.1, None);
                let ev = &err.draft().unwrap().evidence;
                assert_eq!(count(ev, EvidenceKey::BlockingCount), 1);
                assert!(ev.holds(EvidenceKey::Table, "d.b"));
                assert!(ev.holds(EvidenceKey::ReasonClass, "deadline"));
            }

            /// A stop while a batch's outcome is being resolved: the commit
            /// lands, its acknowledgement is lost, and the batch resolves
            /// through its marker; then no further batch or replan starts.
            /// `a` is bound once, `b` stays pending, the proof ends
            /// `Cancelled` and the retrying incident is withdrawn.
            #[tokio::test(start_paused = true)]
            async fn a_stop_during_commit_resolution_starts_no_further_batch() {
                let st = store();
                let mut ctx = ctx_with(&st, bounds()).await;
                let p = tables(&mut ctx, &["a", "b"]).await;
                let (ia, ib) = (p[0].ddl_id.clone(), p[1].ddl_id.clone());
                two_batches(&ctx).await;
                let script = Script::with(&[
                    ("a", vec![contention(), ok()]),
                    ("b", vec![contention(), ok()]),
                ]);
                let cancel = ctx.cancel.clone();
                let memory = st.memory.clone();
                *st.fault.before_next_batch.lock().unwrap() =
                    Some(BatchHook(Box::new(move |_| {
                        cancel.cancel();
                        memory.set_batch_fault(Some(BatchFault::AfterCommit));
                        Box::pin(async {})
                    })));
                let err = prove_with(&ctx, &script, p).await.err().unwrap();
                assert!(matches!(err, SourceError::Cancelled), "{err}");
                let (versions, bound) = durable(&ctx, "a", &ia).await;
                assert_eq!((versions.len(), bound), (1, Some(1)));
                assert_eq!(durable(&ctx, "b", &ib).await, (vec![], None));
                let rec = only_incident(&ctx).await;
                assert!(withdrawn(&rec, "ended_cancelled"), "{rec:?}");
                assert_eq!(count(&rec.evidence, EvidenceKey::BlockingCount), 1);
            }

            /// Past the deadline the freshness reads are skipped: a batch that
            /// resolved after the deadline still binds the table, but its
            /// timeline is not offered for install (it reloads). Before the
            /// deadline the same proof offers it.
            #[tokio::test(start_paused = true)]
            async fn freshness_is_skipped_past_the_deadline() {
                for late in [false, true] {
                    let st = store();
                    let mut ctx = ctx_with(
                        &st,
                        deltaforge_config::ForwardProofRetry {
                            deadline: Duration::from_secs(10),
                            ..bounds()
                        },
                    )
                    .await;
                    let p: Vec<Pending> =
                        pending(&mut ctx, &[("a", "x")], true)
                            .await
                            .into_iter()
                            .map(|(p, _)| p)
                            .collect();
                    let id = p[0].ddl_id.clone();
                    if late {
                        *st.fault.before_next_batch.lock().unwrap() =
                            Some(BatchHook(Box::new(|_| {
                                Box::pin(tokio::time::sleep(
                                    Duration::from_secs(20),
                                ))
                            })));
                    }
                    let fresh =
                        prove_with(&ctx, &Script::default(), p).await.unwrap();
                    assert_eq!(durable(&ctx, "a", &id).await.1, Some(1));
                    assert_eq!(fresh.len(), usize::from(!late), "late: {late}");
                }
            }

            /// Every failure has one decision; the transient ones (and only
            /// they) are retried, identity, saturation and credentials stop,
            /// and evidence, bounds, retention and protocol defer.
            #[test]
            fn every_proof_error_is_decided() {
                let retried =
                    |e: ProofError| matches!(decide(&e), Decision::Retry(_));
                let deferred =
                    |e: ProofError| matches!(decide(&e), Decision::Defer(_));
                let stopped =
                    |e: ProofError| matches!(decide(&e), Decision::Stop(_));
                let s = || String::from("x");
                assert!(retried(ProofError::Contention {
                    code: 1205,
                    why: s()
                }));
                assert!(retried(ProofError::Contention { code: 0, why: s() }));
                assert!(retried(ProofError::Open(Failure::Transport, s())));
                assert!(retried(ProofError::Read(Failure::Transport, s())));
                assert!(retried(ProofError::NotReached(s())));
                for class in [Failure::Authentication, Failure::Authorization] {
                    assert!(stopped(ProofError::Open(class, s())));
                    assert!(stopped(ProofError::Read(class, s())));
                }
                assert!(stopped(ProofError::OtherServer(s())));
                assert!(stopped(ProofError::Saturated {
                    scope: "runner",
                    why: s()
                }));
                for class in [
                    Failure::Protocol,
                    Failure::Retention,
                    Failure::Absent,
                    Failure::Other,
                ] {
                    assert!(deferred(ProofError::Open(class, s())));
                    assert!(deferred(ProofError::Read(class, s())));
                }
                for e in [
                    ProofError::Positions(s()),
                    ProofError::Incident,
                    ProofError::UnsupportedEvent(1),
                    ProofError::Framing(s()),
                    ProofError::Overshoot(s()),
                    ProofError::AnchorMismatch(s()),
                    ProofError::Limit(s()),
                    ProofError::SeekLimit(s()),
                    ProofError::Unstable(2),
                    ProofError::Capture(s()),
                ] {
                    assert!(deferred(e));
                }
            }
        }
    }

    /// Live MySQL 8.4 (Docker).
    mod live {
        use super::super::*;
        use crate::mysql::MySqlSource;
        use crate::mysql::mysql_activation::{
            Kind, Selection, Stored, read_barriers, read_table, select,
        };
        use checkpoints::{CheckpointStore, MemCheckpointStore};
        use deltaforge_config::{OnSchemaDrift, SnapshotCfg, SnapshotMode};
        use deltaforge_core::incident::{
            EvidenceKey, EvidenceValue, Retryability,
        };
        use deltaforge_core::{Source, SourceHandle, SourceItem};
        use gate_ownership::GateOwned;
        use mysql_async::prelude::Queryable;
        use std::sync::Arc;
        use std::time::{Duration, Instant};
        use storage::adapters::incidents::{IncidentStatus, Resolution};
        use storage::adapters::test_util::FaultBackend;
        use storage::{
            ArcStorageBackend, DurableSchemaRegistry, MemoryStorageBackend,
        };
        use testcontainers::core::WaitFor;
        use testcontainers::runners::AsyncRunner;
        use testcontainers::{ContainerAsync, GenericImage, ImageExt};
        use tokio::sync::{OnceCell, mpsc};

        static GTID: OnceCell<(ContainerAsync<GenericImage>, u16)> =
            OnceCell::const_new();
        static FILEPOS: OnceCell<(ContainerAsync<GenericImage>, u16)> =
            OnceCell::const_new();

        async fn start(gtid: bool) -> (ContainerAsync<GenericImage>, u16) {
            let mut cmd = vec![
                "--server-id=61",
                "--log-bin=mysql-bin",
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
            for _ in 0..60 {
                if mysql_async::Conn::from_url(dsn(port)).await.is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            (c, port)
        }

        async fn server(gtid: bool) -> u16 {
            let cell = if gtid { &GTID } else { &FILEPOS };
            cell.get_or_init(|| start(gtid)).await.1
        }

        fn dsn(port: u16) -> String {
            format!("mysql://root:pw@127.0.0.1:{port}/")
        }

        async fn sql(port: u16, stmts: &[&str]) {
            let mut c = mysql_async::Conn::from_url(dsn(port)).await.unwrap();
            for s in stmts {
                c.query_drop(*s)
                    .await
                    .unwrap_or_else(|e| panic!("{s}: {e}"));
            }
            c.disconnect().await.ok();
        }

        struct State {
            backend: ArcStorageBackend,
            registry: Arc<DurableSchemaRegistry>,
            ckpt: Arc<dyn CheckpointStore>,
            scope: crate::registry_scope::SharedRegistryScope,
        }

        async fn state(backend: ArcStorageBackend) -> State {
            State {
                registry: DurableSchemaRegistry::new(backend.clone())
                    .await
                    .unwrap(),
                ckpt: Arc::new(MemCheckpointStore::new().unwrap()),
                scope: crate::registry_scope::SharedRegistryScope::default(),
                backend,
            }
        }

        struct Run {
            handle: SourceHandle,
            rx: mpsc::Receiver<SourceItem>,
        }

        async fn run(st: &State, port: u16, id: &str, tables: &[&str]) -> Run {
            run_with(st, port, id, tables, Default::default()).await
        }

        /// [`run`] with this run's forward-proof retry bounds.
        async fn run_with(
            st: &State,
            port: u16,
            id: &str,
            tables: &[&str],
            retry: deltaforge_config::ForwardProofRetry,
        ) -> Run {
            run_as(st, &dsn(port), id, tables, retry).await
        }

        /// [`run_with`] on `dsn`.
        async fn run_as(
            st: &State,
            dsn: &str,
            id: &str,
            tables: &[&str],
            retry: deltaforge_config::ForwardProofRetry,
        ) -> Run {
            let src = MySqlSource {
                id: id.into(),
                dsn: dsn.into(),
                tables: tables.iter().map(|t| t.to_string()).collect(),
                tenant: "acme".into(),
                pipeline: "test".into(),
                registry: st.registry.clone(),
                registry_scope: st.scope.clone(),
                backend: st.backend.clone(),
                outbox_tables: common::AllowList::default(),
                snapshot_cfg: SnapshotCfg {
                    mode: SnapshotMode::Never,
                    ..Default::default()
                },
                on_schema_drift: OnSchemaDrift::Adapt,
                table_options: Default::default(),
                rotation: None,
                snapshot_cohort: Default::default(),
                proofs: deltaforge_config::MysqlProofCfg {
                    forward_proof_retry: retry,
                    ..Default::default()
                },
                type_mapping: deltaforge_core::TypeMapping::V2,
            };
            let (tx, rx) = mpsc::channel(4096);
            let handle = src.run(tx, st.ckpt.clone()).await;
            tokio::time::sleep(Duration::from_secs(3)).await;
            Run { handle, rx }
        }

        async fn stop(handle: SourceHandle) {
            handle.stop();
            tokio::time::timeout(Duration::from_secs(30), handle.join)
                .await
                .ok();
        }

        /// Wait for the DDL event containing `needle`.
        async fn ddl_event(rx: &mut mpsc::Receiver<SourceItem>, needle: &str) {
            let deadline = Instant::now() + Duration::from_secs(60);
            while let Some(left) =
                deadline.checked_duration_since(Instant::now())
            {
                match tokio::time::timeout(left, rx.recv()).await {
                    Ok(Some(SourceItem::Event(e)))
                        if e.ddl.as_ref().is_some_and(|d| {
                            d.to_string().contains(needle)
                        }) =>
                    {
                        return;
                    }
                    Ok(Some(_)) => {}
                    _ => break,
                }
            }
            panic!("no DDL event containing {needle:?}");
        }

        async fn records(st: &State, db: &str, table: &str) -> Vec<Stored> {
            let key = st.scope.current().unwrap().key(db, table);
            read_table(&st.backend, &st.registry, &key).await.unwrap()
        }

        async fn barriers(st: &State, db: &str, table: &str) -> Vec<Stored> {
            let key = st.scope.current().unwrap().key(db, table);
            read_barriers(&st.backend, &key).await.unwrap()
        }

        fn ddls(recs: &[Stored]) -> Vec<&Stored> {
            recs.iter().filter(|s| s.record.kind == Kind::Ddl).collect()
        }

        /// The number of registered versions of `db.table`.
        async fn versions(st: &State, db: &str, table: &str) -> usize {
            let key = st.scope.current().unwrap().key(db, table);
            crate::mysql::mysql_activation::tests_support::history(
                &st.registry,
                &key,
            )
            .await
            .len()
        }

        /// The store writes made, one per call (`batch`: one atomic batch),
        /// to the registry and activation namespaces.
        fn schema_writes(fault: &FaultBackend) -> Vec<String> {
            fault
                .write_calls
                .lock()
                .unwrap()
                .iter()
                .filter(|ns| ns.starts_with("schemas") || *ns == "batch")
                .cloned()
                .collect()
        }

        /// The validated forward binding of `ddl` (panics on invalid
        /// evidence).
        async fn bound(
            st: &State,
            db: &str,
            table: &str,
            recs: &[Stored],
            ddl: &Stored,
        ) -> Option<i32> {
            let key = st.scope.current().unwrap().key(db, table);
            resolve_binding(
                &st.registry,
                &key,
                recs,
                &ddl.capture_id,
                CLASSIFIER_VERSION,
            )
            .await
            .expect("valid binding evidence")
        }

        async fn prepare(port: u16, db: &str) {
            sql(
                port,
                &[
                    &format!("DROP DATABASE IF EXISTS {db}"),
                    &format!("CREATE DATABASE {db}"),
                    &format!("CREATE TABLE {db}.t (id INT PRIMARY KEY, v INT)"),
                    &format!("CREATE TABLE {db}.other (id INT PRIMARY KEY)"),
                ],
            )
            .await;
        }

        async fn a_live_ddl_is_bound(gtid: bool) {
            let port = server(gtid).await;
            let db = if gtid { "fwd_g" } else { "fwd_f" };
            prepare(port, db).await;
            let st = state(Arc::new(MemoryStorageBackend::new())).await;
            // A committed position before the DDL (for the replay below).
            let r = run(&st, port, db, &[&format!("{db}.t")]).await;
            stop(r.handle).await;
            let mut r = run(&st, port, db, &[&format!("{db}.t")]).await;
            sql(port, &[&format!("ALTER TABLE {db}.t ADD COLUMN w INT")]).await;
            ddl_event(&mut r.rx, "ADD COLUMN w").await;
            // Durable before the DDL event: the pending DDL and its binding.
            let recs = records(&st, db, "t").await;
            let d = ddls(&recs);
            assert_eq!(d.len(), 1, "{recs:?}");
            let version = bound(&st, db, "t", &recs, d[0])
                .await
                .expect("the DDL is bound");
            let key = st.scope.current().unwrap().key(db, "t");
            let shape = st.registry.version_hash(&key, version).await.unwrap();
            assert!(shape.is_some());
            let all: Vec<Stored> = recs
                .iter()
                .cloned()
                .chain(barriers(&st, db, "t").await)
                .collect();
            assert_eq!(
                select(&all, &d[0].record.position),
                Selection::Proven { version }
            );
            let history =
                crate::mysql::mysql_activation::tests_support::history(
                    &st.registry,
                    &key,
                )
                .await;
            assert!(
                history.last().unwrap().contains("\"w\""),
                "the bound version has the new column"
            );

            // A crash after the binding (no checkpoint past the DDL): the
            // replay re-derives nothing new.
            let before = records(&st, db, "t").await;
            r.handle.join.abort();
            // The server moves on before the restart: a re-proof would capture
            // a different S (another record); the existing binding is reused.
            sql(port, &[&format!("INSERT INTO {db}.other VALUES (1)")]).await;
            let mut r = run(&st, port, db, &[&format!("{db}.t")]).await;
            ddl_event(&mut r.rx, "ADD COLUMN w").await;
            assert_eq!(records(&st, db, "t").await, before);
            stop(r.handle).await;
        }

        /// A saturated control plane for the next capture: a runner budget
        /// whose only ordinary permit another source holds (released by
        /// dropping what is returned); the capture waits at most 1 s.
        async fn starve_next_capture(
            dsn: &str,
        ) -> crate::mysql::mysql_control::ControlConn {
            use crate::mysql::mysql_control::{Budget, Class, ControlPool};
            let mut c = mysql_async::Conn::from_url(dsn).await.unwrap();
            let uuid: String = c
                .query_first("SELECT @@GLOBAL.server_uuid")
                .await
                .unwrap()
                .unwrap();
            c.disconnect().await.ok();
            let budget: &'static Budget = Box::leak(Box::new(Budget::new(
                crate::mysql::mysql_binlog_scan::KILL_RESERVE + 1,
            )));
            let held = ControlPool::detached(budget, "other", 2)
                .connect(Class::Catalog, dsn, &uuid, Duration::from_secs(5))
                .await
                .unwrap();
            *crate::mysql::mysql_binlog_scan::STARVE_NEXT.lock().unwrap() =
                Some((
                    ControlPool::detached(budget, "starved", 2),
                    Duration::from_secs(1),
                ));
            held
        }

        /// A saturated control plane at a DDL's forward proof is the
        /// `mysql_control_plane_saturated` incident (AutoRetry), never a
        /// deferral: the source stops before the DDL is emitted or
        /// checkpointed, reads no later row, and keeps no proof of it (no
        /// binding, no new version). The restart replays the DDL, binds it
        /// and delivers the row after it exactly once. The incident carries
        /// no DSN or credential.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_saturated_forward_proof_stops_and_the_restart_binds_the_ddl()
        {
            let port = server(true).await;
            let db = "fwd_saturated";
            prepare(port, db).await;
            let st = state(Arc::new(MemoryStorageBackend::new())).await;
            let t = format!("{db}.t");
            // A committed position before the DDL.
            let r = run(&st, port, db, &[&t]).await;
            stop(r.handle).await;
            let committed = st.ckpt.get_raw(db).await.unwrap();
            assert!(committed.is_some());
            let versions = || {
                let key = st.scope.current().unwrap().key(db, "t");
                let reg = st.registry.clone();
                async move {
                    crate::mysql::mysql_activation::tests_support::history(
                        &reg, &key,
                    )
                    .await
                    .len()
                }
            };
            let registered = versions().await;
            let deferrals = crate::mysql::mysql_control::saturation_deferrals();

            let mut r = run(&st, port, db, &[&t]).await;
            let held = starve_next_capture(&dsn(port)).await;
            sql(
                port,
                &[
                    &format!("ALTER TABLE {t} ADD COLUMN w INT"),
                    &format!("INSERT INTO {t} VALUES (1, 1, 7)"),
                ],
            )
            .await;
            let ended =
                tokio::time::timeout(Duration::from_secs(60), r.handle.join)
                    .await
                    .expect("stops")
                    .expect("task");
            drop(held);
            let err = ended.expect_err("the saturated proof stops the source");
            assert_eq!(
                err.draft().map(|d| d.reason_code),
                Some(
                    deltaforge_core::incident::ReasonCode::MysqlControlPlaneSaturated
                ),
                "{err}"
            );
            for text in [format!("{err}"), format!("{err:?}")] {
                assert!(
                    !text.contains("root:") && !text.contains(":pw@"),
                    "{text}"
                );
            }
            let mut emitted = Vec::new();
            while let Ok(item) = r.rx.try_recv() {
                if let SourceItem::Event(e) = item {
                    emitted.push(e);
                }
            }
            assert!(
                emitted.iter().all(|e| e.source.table != "t"),
                "neither the DDL nor the row after it: {emitted:?}"
            );
            assert_eq!(st.ckpt.get_raw(db).await.unwrap(), committed);
            let recs = records(&st, db, "t").await;
            for d in ddls(&recs) {
                assert_eq!(bound(&st, db, "t", &recs, d).await, None);
            }
            assert_eq!(versions().await, registered, "no version from it");
            assert_eq!(
                crate::mysql::mysql_control::saturation_deferrals(),
                deferrals,
                "not a deferral"
            );

            // The restart: the DDL replays and is bound; the row after it
            // is delivered once, decoded with the new column.
            let mut r = run(&st, port, db, &[&t]).await;
            ddl_event(&mut r.rx, "ADD COLUMN w").await;
            let recs = records(&st, db, "t").await;
            let d = ddls(&recs);
            assert_eq!(d.len(), 1, "{recs:?}");
            assert!(bound(&st, db, "t", &recs, d[0]).await.is_some());
            let seven = deltaforge_core::canonical::fixture::value(
                &serde_json::json!(7),
            );
            let mut rows = 0;
            let deadline = Instant::now() + Duration::from_secs(10);
            while let Some(left) =
                deadline.checked_duration_since(Instant::now())
            {
                match tokio::time::timeout(left, r.rx.recv()).await {
                    Ok(Some(SourceItem::Event(e)))
                        if e.ddl.is_none() && e.source.table == "t" =>
                    {
                        assert_eq!(
                            e.after.as_ref().unwrap().get("w"),
                            Some(&seven),
                            "decoded with the new column"
                        );
                        rows += 1;
                    }
                    Ok(Some(_)) => {}
                    _ => break,
                }
            }
            assert_eq!(rows, 1, "the row after the DDL, exactly once");
            stop(r.handle).await;
        }

        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_live_ddl_is_bound_by_its_forward_proof_gtid() {
            a_live_ddl_is_bound(true).await;
        }

        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_live_ddl_is_bound_by_its_forward_proof_file_position() {
            a_live_ddl_is_bound(false).await;
        }

        /// The registration and the binding are one atomic batch: a crash at
        /// the binding's write leaves neither (the DDL pending, no version
        /// from it), and the restart proves it again with one new version.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_crash_at_the_binding_leaves_no_registration() {
            let port = server(true).await;
            let db = "fwd_crash";
            prepare(port, db).await;
            let fault = Arc::new(FaultBackend::new());
            let st = state(fault.clone()).await;
            let r = run(&st, port, db, &[&format!("{db}.t")]).await;
            stop(r.handle).await;
            let before = versions(&st, db, "t").await;
            let r = run(&st, port, db, &[&format!("{db}.t")]).await;
            // The DDL's activation writes: its `ddl` record, then the
            // binding, where the process crashes.
            *fault.crash_after_writes_to.lock().unwrap() =
                Some((crate::mysql::mysql_activation::ACTIVATION_NS.into(), 1));
            sql(port, &[&format!("ALTER TABLE {db}.t ADD COLUMN z INT")]).await;
            let res =
                tokio::time::timeout(Duration::from_secs(60), r.handle.join)
                    .await
                    .expect("stops")
                    .expect("task");
            assert!(res.is_err(), "the failed batch stops the source");
            let recs = records(&st, db, "t").await;
            let d = ddls(&recs);
            assert_eq!(d.len(), 1, "the pending record stays");
            assert_eq!(bound(&st, db, "t", &recs, d[0]).await, None);
            assert_eq!(
                versions(&st, db, "t").await,
                before,
                "no registration without its binding"
            );

            // Restart: the DDL replays, is proven and bound once.
            *fault.crash_after_writes_to.lock().unwrap() = None;
            let mut r = run(&st, port, db, &[&format!("{db}.t")]).await;
            ddl_event(&mut r.rx, "ADD COLUMN z").await;
            let recs = records(&st, db, "t").await;
            let d = ddls(&recs);
            assert_eq!(d.len(), 1);
            assert!(bound(&st, db, "t", &recs, d[0]).await.is_some());
            assert_eq!(versions(&st, db, "t").await, before + 1);
            stop(r.handle).await;
        }

        #[tokio::test]
        #[ignore = "requires docker"]
        async fn only_a_clean_interval_proves_the_post_ddl_shape() {
            let port = server(true).await;
            let db = "fwd_scan";
            prepare(port, db).await;
            let st = state(Arc::new(MemoryStorageBackend::new())).await;
            let tables =
                [format!("{db}.t"), format!("{db}.x"), format!("{db}.u")];
            let tables: Vec<&str> = tables.iter().map(String::as_str).collect();
            let r = run(&st, port, db, &tables).await;
            stop(r.handle).await;

            // While stopped (the restart replays them, so each proof's
            // interval holds the later statements):
            sql(
                port,
                &[
                    // a second DDL on t inside the first one's (D, S];
                    &format!("ALTER TABLE {db}.t ADD COLUMN a INT"),
                    &format!("ALTER TABLE {db}.t ADD COLUMN b INT"),
                    // an unrelated table's DDL does not block other's proof;
                    &format!("ALTER TABLE {db}.other ADD COLUMN c INT"),
                    &format!("CREATE TABLE {db}.u (id INT PRIMARY KEY)"),
                    // a DROP gets no proof; the recreate is its own DDL;
                    &format!("DROP TABLE {db}.other"),
                    &format!(
                        "CREATE TABLE {db}.other (id INT PRIMARY KEY, n INT)"
                    ),
                    // a rename binds its destination.
                    &format!("CREATE TABLE {db}.y (id INT PRIMARY KEY)"),
                    &format!("RENAME TABLE {db}.y TO {db}.x"),
                ],
            )
            .await;
            let tracked = [
                format!("{db}.t"),
                format!("{db}.x"),
                format!("{db}.u"),
                format!("{db}.other"),
            ];
            let tracked: Vec<&str> =
                tracked.iter().map(String::as_str).collect();
            let mut r = run(&st, port, db, &tracked).await;
            ddl_event(&mut r.rx, &format!("RENAME TABLE {db}.y TO {db}.x"))
                .await;

            let t = records(&st, db, "t").await;
            let td = ddls(&t);
            assert_eq!(td.len(), 2, "{t:?}");
            assert_eq!(
                bound(&st, db, "t", &t, td[0]).await,
                None,
                "a second DDL in (D, S]"
            );
            assert!(bound(&st, db, "t", &t, td[1]).await.is_some());

            let u = records(&st, db, "u").await;
            assert!(
                bound(&st, db, "u", &u, ddls(&u)[0]).await.is_some(),
                "unrelated DDLs do not block"
            );

            let o = records(&st, db, "other").await;
            let od = ddls(&o);
            assert_eq!(od.len(), 3, "{o:?}"); // ALTER, DROP, CREATE
            assert_eq!(
                bound(&st, db, "other", &o, od[1]).await,
                None,
                "a DROP gets no proof"
            );
            assert!(
                bound(&st, db, "other", &o, od[2]).await.is_some(),
                "the recreate is proven"
            );

            let x = records(&st, db, "x").await;
            assert!(
                bound(&st, db, "x", &x, ddls(&x)[0]).await.is_some(),
                "the rename destination"
            );
            stop(r.handle).await;

            // A lineage barrier in (D, S] leaves the DDL pending.
            sql(
                port,
                &[
                    &format!("ALTER TABLE {db}.u ADD COLUMN q INT"),
                    &format!("CREATE TABLE {db}.w (id INT) COMMENT \"w\""),
                ],
            )
            .await;
            let mut r = run(&st, port, db, &tracked).await;
            ddl_event(&mut r.rx, "CREATE TABLE").await;
            let u = records(&st, db, "u").await;
            let ud = ddls(&u);
            assert_eq!(
                bound(&st, db, "u", &u, ud[ud.len() - 1]).await,
                None,
                "barrier in (D, S]"
            );
            stop(r.handle).await;
        }

        #[tokio::test]
        #[ignore = "requires docker"]
        async fn an_invalid_binding_stops_the_replay_without_a_new_proof() {
            let port = server(true).await;
            let db = "fwd_invalid";
            prepare(port, db).await;
            let st = state(Arc::new(MemoryStorageBackend::new())).await;
            let r = run(&st, port, db, &[&format!("{db}.t")]).await;
            stop(r.handle).await;
            let mut r = run(&st, port, db, &[&format!("{db}.t")]).await;
            sql(port, &[&format!("ALTER TABLE {db}.t ADD COLUMN k INT")]).await;
            ddl_event(&mut r.rx, "ADD COLUMN k").await;
            r.handle.join.abort();

            // A second record binding the same DDL (duplicate evidence).
            let key = st.scope.current().unwrap().key(db, "t");
            let stream = crate::mysql::mysql_activation::table_stream(&key);
            let recs = records(&st, db, "t").await;
            let binding = recs
                .iter()
                .find(|s| matches!(s.record.kind, Kind::Observed { .. }))
                .unwrap();
            st.backend
                .log_append_if_absent(
                    crate::mysql::mysql_activation::ACTIVATION_NS,
                    &stream,
                    "forged",
                    &serde_json::to_vec(&binding.record).unwrap(),
                )
                .await
                .unwrap();
            let tampered = records(&st, db, "t").await;

            // The replay stops on the invalid evidence: no new proof, no DDL
            // event.
            let mut r = run(&st, port, db, &[&format!("{db}.t")]).await;
            let res =
                tokio::time::timeout(Duration::from_secs(60), r.handle.join)
                    .await
                    .expect("stops")
                    .expect("task");
            assert!(
                matches!(&res, Err(SourceError::Schema { details }) if details.contains("2 bindings")),
                "{res:?}"
            );
            while let Ok(item) = r.rx.try_recv() {
                if let SourceItem::Event(e) = item {
                    assert!(e.ddl.is_none(), "the DDL event was emitted");
                }
            }
            assert_eq!(records(&st, db, "t").await, tampered);
        }

        /// A forward proof writes two store transactions: the pending `ddl`
        /// record, then one atomic batch holding the registration and the
        /// binding.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_forward_proof_is_two_write_transactions() {
            let port = server(true).await;
            let db = "fwd_txns";
            prepare(port, db).await;
            let fault = Arc::new(FaultBackend::new());
            let st = state(fault.clone()).await;
            let t = format!("{db}.t");
            let r = run(&st, port, db, &[&t]).await;
            stop(r.handle).await;
            let mut r = run(&st, port, db, &[&t]).await;
            fault.write_calls.lock().unwrap().clear();
            sql(port, &[&format!("ALTER TABLE {t} ADD COLUMN w INT")]).await;
            ddl_event(&mut r.rx, "ADD COLUMN w").await;
            let writes = schema_writes(&fault);
            stop(r.handle).await;
            let recs = records(&st, db, "t").await;
            let d = ddls(&recs);
            assert_eq!(d.len(), 1, "{recs:?}");
            assert!(bound(&st, db, "t", &recs, d[0]).await.is_some());
            assert_eq!(
                writes,
                [crate::mysql::mysql_activation::ACTIVATION_NS, "batch"],
                "the pending record, then one batch"
            );
        }

        /// The tables of a multi-table DDL that do not fit one batch are
        /// committed in several, each table wholly in one. A crash between
        /// them leaves the first bound and the second pending with no
        /// version from the proof; the restart validates the first binding
        /// and proves the second.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_multi_table_ddl_over_the_budget_commits_per_table_batches() {
            let port = server(true).await;
            let db = "fwd_split";
            sql(
                port,
                &[
                    &format!("DROP DATABASE IF EXISTS {db}"),
                    &format!("CREATE DATABASE {db}"),
                    &format!("CREATE TABLE {db}.a (id INT PRIMARY KEY)"),
                    &format!("CREATE TABLE {db}.c (id INT PRIMARY KEY)"),
                ],
            )
            .await;
            let fault = Arc::new(FaultBackend::new());
            let st = state(fault.clone()).await;
            let tracked = [format!("{db}.b"), format!("{db}.d")];
            let tracked: Vec<&str> =
                tracked.iter().map(String::as_str).collect();
            let r = run(&st, port, db, &tracked).await;
            stop(r.handle).await;
            // One table per batch: a table's plan reads fit, two do not.
            crate::stream_probe::set_window_caps(Some((
                crate::mysql::mysql_window_budget::PLAN_BYTES,
                40,
                crate::mysql::mysql_window_budget::TABLE_BYTES,
            )));
            let r = run(&st, port, db, &tracked).await;
            fault.write_calls.lock().unwrap().clear();
            // The `ddl` records of both names of both tables and the first
            // batch's binding, then a crash at the second batch.
            *fault.crash_after_writes_to.lock().unwrap() =
                Some((crate::mysql::mysql_activation::ACTIVATION_NS.into(), 5));
            sql(
                port,
                &[&format!("RENAME TABLE {db}.a TO {db}.b, {db}.c TO {db}.d")],
            )
            .await;
            let res =
                tokio::time::timeout(Duration::from_secs(60), r.handle.join)
                    .await
                    .expect("stops")
                    .expect("task");
            let writes = schema_writes(&fault);
            assert!(res.is_err(), "the failed batch stops the source");
            let activation = crate::mysql::mysql_activation::ACTIVATION_NS;
            assert_eq!(
                writes,
                [activation, activation, activation, activation, "batch"],
                "the pending records, then the first table's batch; the \
                 second table's never reached the store"
            );
            let b = records(&st, db, "b").await;
            assert!(bound(&st, db, "b", &b, ddls(&b)[0]).await.is_some());
            let d = records(&st, db, "d").await;
            assert_eq!(bound(&st, db, "d", &d, ddls(&d)[0]).await, None);
            assert_eq!(versions(&st, db, "d").await, 0, "no version for d");

            *fault.crash_after_writes_to.lock().unwrap() = None;
            let mut r = run(&st, port, db, &tracked).await;
            ddl_event(&mut r.rx, "RENAME TABLE").await;
            crate::stream_probe::set_window_caps(None);
            for t in ["b", "d"] {
                let recs = records(&st, db, t).await;
                assert!(
                    bound(&st, db, t, &recs, ddls(&recs)[0]).await.is_some()
                );
                assert_eq!(versions(&st, db, t).await, 1, "{t}");
            }
            stop(r.handle).await;
        }

        /// A table whose own contribution exceeds an empty batch stops the
        /// proof with the classified incident before anything of it is
        /// written: the DDL stays pending, no version, no checkpoint
        /// advance.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn an_oversized_table_stops_the_proof_with_nothing_written() {
            let port = server(true).await;
            let db = "fwd_oversized";
            prepare(port, db).await;
            let st = state(Arc::new(MemoryStorageBackend::new())).await;
            let t = format!("{db}.t");
            let r = run(&st, port, db, &[&t]).await;
            stop(r.handle).await;
            let committed = st.ckpt.get_raw(db).await.unwrap();
            let before = versions(&st, db, "t").await;
            crate::stream_probe::set_window_caps(Some((
                crate::mysql::mysql_window_budget::PLAN_BYTES,
                16_384,
                10,
            )));
            let r = run(&st, port, db, &[&t]).await;
            sql(port, &[&format!("ALTER TABLE {t} ADD COLUMN w INT")]).await;
            let res =
                tokio::time::timeout(Duration::from_secs(60), r.handle.join)
                    .await
                    .expect("stops")
                    .expect("task");
            crate::stream_probe::set_window_caps(None);
            let err = res.expect_err("the oversized table stops the source");
            assert_eq!(
                err.draft().map(|d| d.reason_code),
                Some(deltaforge_core::incident::ReasonCode::MysqlTableStateTooLarge),
                "{err}"
            );
            let recs = records(&st, db, "t").await;
            let d = ddls(&recs);
            assert_eq!(d.len(), 1, "the pending record stays");
            assert_eq!(bound(&st, db, "t", &recs, d[0]).await, None);
            assert_eq!(versions(&st, db, "t").await, before);
            assert_eq!(st.ckpt.get_raw(db).await.unwrap(), committed);
        }

        /// Another registration of the same table between the plan and the
        /// commit fails the batch's preconditions: nothing is written, the
        /// proof plans again and numbers after it, as the serial path does.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_concurrent_registration_replans_the_proof() {
            let port = server(true).await;
            let db = "fwd_conflict";
            prepare(port, db).await;
            let fault = Arc::new(FaultBackend::new());
            let st = state(fault.clone()).await;
            let t = format!("{db}.t");
            let r = run(&st, port, db, &[&t]).await;
            stop(r.handle).await;
            let mut r = run(&st, port, db, &[&t]).await;
            let key = st.scope.current().unwrap().key(db, "t");
            let injected = Arc::new(std::sync::Mutex::new(None));
            let seen = injected.clone();
            let k = key.clone();
            *fault.before_next_batch.lock().unwrap() =
                Some(storage::adapters::test_util::BatchHook(Box::new(
                    move |inner| {
                        Box::pin(async move {
                            let other = DurableSchemaRegistry::new(inner)
                                .await
                                .unwrap();
                            let v = other
                                .register_with_checkpoint(
                                    &k,
                                    "concurrent",
                                    &serde_json::json!({"concurrent": true}),
                                    None,
                                )
                                .await
                                .unwrap();
                            *seen.lock().unwrap() = Some(v);
                        })
                    },
                )));
            sql(port, &[&format!("ALTER TABLE {t} ADD COLUMN w INT")]).await;
            ddl_event(&mut r.rx, "ADD COLUMN w").await;
            stop(r.handle).await;
            let other = injected.lock().unwrap().expect("the hook ran");
            let recs = records(&st, db, "t").await;
            let d = ddls(&recs);
            let version = bound(&st, db, "t", &recs, d[0])
                .await
                .expect("the DDL is bound");
            assert_eq!(version, other + 1, "numbered after the other");
            let shape = st.registry.version_hash(&key, version).await.unwrap();
            assert_ne!(shape.as_deref(), Some("concurrent"));
        }

        /// A registration that conflicts on every attempt stops the source
        /// after the bounded re-plans: the DDL stays pending, unbound, with
        /// no version from the proof.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_proof_that_always_conflicts_stops_with_the_ddl_pending() {
            fn arm(
                fault: Arc<FaultBackend>,
                key: storage::adapters::SchemaKey,
                n: Arc<std::sync::atomic::AtomicUsize>,
            ) {
                let again = fault.clone();
                *fault.before_next_batch.lock().unwrap() =
                    Some(storage::adapters::test_util::BatchHook(Box::new(
                        move |inner| {
                            Box::pin(async move {
                                let i = n.fetch_add(
                                    1,
                                    std::sync::atomic::Ordering::SeqCst,
                                );
                                let other = DurableSchemaRegistry::new(inner)
                                    .await
                                    .unwrap();
                                other
                                    .register_with_checkpoint(
                                        &key,
                                        &format!("concurrent-{i}"),
                                        &serde_json::json!({"concurrent": i}),
                                        None,
                                    )
                                    .await
                                    .unwrap();
                                arm(again, key, n);
                            })
                        },
                    )));
            }
            let port = server(true).await;
            let db = "fwd_conflict_always";
            prepare(port, db).await;
            let fault = Arc::new(FaultBackend::new());
            let st = state(fault.clone()).await;
            let t = format!("{db}.t");
            let r = run(&st, port, db, &[&t]).await;
            stop(r.handle).await;
            let committed = st.ckpt.get_raw(db).await.unwrap();
            let r = run(&st, port, db, &[&t]).await;
            let key = st.scope.current().unwrap().key(db, "t");
            let conflicts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            arm(fault.clone(), key.clone(), conflicts.clone());
            sql(port, &[&format!("ALTER TABLE {t} ADD COLUMN w INT")]).await;
            let res =
                tokio::time::timeout(Duration::from_secs(60), r.handle.join)
                    .await
                    .expect("stops")
                    .expect("task");
            *fault.before_next_batch.lock().unwrap() = None;
            assert!(res.is_err(), "{res:?}");
            let recs = records(&st, db, "t").await;
            let d = ddls(&recs);
            assert_eq!(d.len(), 1, "the pending record stays");
            assert_eq!(bound(&st, db, "t", &recs, d[0]).await, None);
            let history =
                crate::mysql::mysql_activation::tests_support::history(
                    &st.registry,
                    &key,
                )
                .await;
            assert!(
                history.iter().all(|h| !h.contains("\"w\"")),
                "no version from the proof: {history:?}"
            );
            assert_eq!(st.ckpt.get_raw(db).await.unwrap(), committed);
        }

        // ---- C3: installing a proven table's timeline -------------------

        /// The table stream reads of `db.table` made since the last clear.
        fn table_reads(
            fault: &FaultBackend,
            st: &State,
            db: &str,
            t: &str,
        ) -> usize {
            let key = st.scope.current().unwrap().key(db, t);
            let stream = crate::mysql::mysql_activation::table_stream(&key);
            fault
                .stream_reads
                .lock()
                .unwrap()
                .iter()
                .filter(|(ns, k)| {
                    ns == crate::mysql::mysql_activation::ACTIVATION_NS
                        && *k == stream
                })
                .count()
        }

        /// The timelines installed for `db.table` since the last clear.
        fn installed(
            st: &State,
            db: &str,
            t: &str,
        ) -> Vec<(u64, crate::mysql::mysql_selection::Timeline)> {
            let key = st.scope.current().unwrap().key(db, t).backend_key();
            super::super::hooks::INSTALLED
                .lock()
                .unwrap()
                .iter()
                .filter(|(k, _, _)| *k == key)
                .map(|(_, b, t)| (*b, t.clone()))
                .collect()
        }

        /// Wait for a row event of `table` whose `w` is `w`.
        async fn row_with_w(
            rx: &mut mpsc::Receiver<SourceItem>,
            table: &str,
            w: i64,
        ) {
            let want = deltaforge_core::canonical::fixture::value(
                &serde_json::json!(w),
            );
            let deadline = Instant::now() + Duration::from_secs(60);
            while let Some(left) =
                deadline.checked_duration_since(Instant::now())
            {
                match tokio::time::timeout(left, rx.recv()).await {
                    Ok(Some(SourceItem::Event(e)))
                        if e.ddl.is_none()
                            && e.source.table == table
                            && e.after.as_ref().and_then(|a| a.get("w"))
                                == Some(&want) =>
                    {
                        return;
                    }
                    Ok(Some(_)) => {}
                    _ => break,
                }
            }
            panic!("no row of {table} with w = {w}");
        }

        /// A record appended to `stream` by another writer, straight into
        /// `backend` (no source, no cache).
        async fn foreign_append(
            backend: &ArcStorageBackend,
            ns: &str,
            stream: &str,
            id: &str,
            kind: Kind,
        ) {
            let record = crate::mysql::mysql_activation::Record::new(
                crate::durable_checkpoint::WmPos::MysqlGtid {
                    gtid_set: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-1000000"
                        .into(),
                },
                kind,
            );
            backend
                .log_append_if_absent(
                    ns,
                    stream,
                    id,
                    &crate::mysql::mysql_activation::encode_record(&record)
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        /// A tracked table, cached by a row before its DDL, on a store whose
        /// gate this process holds (`gated`) or not, with the C3 probes
        /// cleared; returns the run and the table's key.
        async fn cached_table(
            db: &str,
            gated: bool,
        ) -> (Arc<FaultBackend>, State, Run, u16) {
            let port = server(true).await;
            prepare(port, db).await;
            let fault = Arc::new(FaultBackend::new());
            let st = state(fault.clone()).await;
            if gated {
                let backend: ArcStorageBackend = fault.clone();
                storage::adapters::store_gate::acquire(
                    &backend,
                    storage::adapters::store_gate::GateRole::Server,
                )
                .await
                .unwrap();
                assert!(storage::adapters::store_gate::server_holds(
                    &st.backend
                ));
            }
            let t = format!("{db}.t");
            let r = run(&st, port, db, &[&t]).await;
            stop(r.handle).await;
            let mut r = run(&st, port, db, &[&t]).await;
            sql(port, &[&format!("INSERT INTO {t} VALUES (1, 1)")]).await;
            let mut seen = false;
            let deadline = Instant::now() + Duration::from_secs(30);
            while !seen && Instant::now() < deadline {
                if let Ok(Some(SourceItem::Event(e))) =
                    tokio::time::timeout(Duration::from_secs(1), r.rx.recv())
                        .await
                {
                    seen = e.ddl.is_none() && e.source.table == "t";
                }
            }
            assert!(seen, "the row before the DDL caches the timeline");
            fault.stream_reads.lock().unwrap().clear();
            fault
                .batched_stream_reads
                .store(0, std::sync::atomic::Ordering::SeqCst);
            super::super::hooks::INSTALLED.lock().unwrap().clear();
            *super::super::hooks::BEFORE_INSTALL.lock().unwrap() = None;
            (fault, st, r, port)
        }

        /// With the store-gate witness, a cached table's proven timeline is
        /// installed: the row after the DDL needs no history load (the table
        /// stream is read twice: the replay check and the freshness check),
        /// and the installed timeline equals a fresh read of the store
        /// (records, head and bytes).
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_cached_proven_table_takes_its_records_without_a_reload() {
            let db = "fwd_install";
            let (fault, st, mut r, port) = cached_table(db, true).await;
            sql(
                port,
                &[
                    &format!("ALTER TABLE {db}.t ADD COLUMN w INT"),
                    &format!("INSERT INTO {db}.t VALUES (2, 2, 7)"),
                ],
            )
            .await;
            ddl_event(&mut r.rx, "ADD COLUMN w").await;
            row_with_w(&mut r.rx, "t", 7).await;
            assert_eq!(
                table_reads(&fault, &st, db, "t"),
                2,
                "the replay check and the freshness check"
            );
            assert_eq!(
                fault
                    .batched_stream_reads
                    .load(std::sync::atomic::Ordering::SeqCst),
                0,
                "no history load"
            );
            stop(r.handle).await;
            let got = installed(&st, db, "t");
            assert_eq!(got.len(), 1);
            let key = st.scope.current().unwrap().key(db, "t");
            let stream = crate::mysql::mysql_activation::table_stream(&key);
            let entries = st
                .backend
                .log_read_meta_since(
                    crate::mysql::mysql_activation::ACTIVATION_NS,
                    &stream,
                    0,
                    10_000,
                )
                .await
                .unwrap();
            let fresh = records(&st, db, "t").await;
            let (snapshot_bytes, got) = &got[0];
            assert_eq!(got.records, fresh, "the records a load reads");
            assert_eq!(got.table_head, entries.last().unwrap().seq);
            assert_eq!(
                got.bytes - snapshot_bytes,
                entries[entries.len() - 2..]
                    .iter()
                    .map(storage::log_entry_bytes)
                    .sum::<u64>(),
                "the snapshot's estimate and its two new entries, as a load \
                 counts them"
            );
        }

        /// Without the witness, nothing is installed: the proven table
        /// reloads, and its row after the DDL is decoded with its new shape.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn without_the_store_gate_witness_nothing_is_installed() {
            let db = "fwd_no_witness";
            let (fault, st, mut r, port) = cached_table(db, false).await;
            sql(
                port,
                &[
                    &format!("ALTER TABLE {db}.t ADD COLUMN w INT"),
                    &format!("INSERT INTO {db}.t VALUES (2, 2, 7)"),
                ],
            )
            .await;
            ddl_event(&mut r.rx, "ADD COLUMN w").await;
            row_with_w(&mut r.rx, "t", 7).await;
            stop(r.handle).await;
            assert!(installed(&st, db, "t").is_empty());
            assert!(
                fault
                    .batched_stream_reads
                    .load(std::sync::atomic::Ordering::SeqCst)
                    >= 1,
                "the table reloads"
            );
        }

        /// Another writer's append to the table stream, or to a barrier
        /// stream of the table, before the proof's commit: the freshness
        /// check sees it and nothing is installed.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn an_append_or_barrier_before_the_check_prevents_the_install() {
            use crate::mysql::mysql_activation::{
                ACTIVATION_NS, BARRIER_NS, BarrierScope, barrier_stream,
                table_stream,
            };
            for (i, barrier) in [false, true].into_iter().enumerate() {
                let db = if barrier {
                    "fwd_pre_barrier"
                } else {
                    "fwd_pre_append"
                };
                let (fault, st, mut r, port) = cached_table(db, true).await;
                let key = st.scope.current().unwrap().key(db, "t");
                let (ns, stream, kind) = if barrier {
                    let scope = BarrierScope::Database { db: db.into() };
                    (
                        BARRIER_NS,
                        barrier_stream(&key, &scope),
                        Kind::Barrier { scope },
                    )
                } else {
                    (ACTIVATION_NS, table_stream(&key), Kind::Ddl)
                };
                *fault.before_next_batch.lock().unwrap() =
                    Some(storage::adapters::test_util::BatchHook(Box::new(
                        move |inner| {
                            Box::pin(async move {
                                foreign_append(
                                    &inner,
                                    ns,
                                    &stream,
                                    &format!("foreign-{i}"),
                                    kind,
                                )
                                .await;
                            })
                        },
                    )));
                sql(port, &[&format!("ALTER TABLE {db}.t ADD COLUMN w INT")])
                    .await;
                ddl_event(&mut r.rx, "ADD COLUMN w").await;
                stop(r.handle).await;
                assert!(installed(&st, db, "t").is_empty(), "{db}");
            }
        }

        /// An in-process barrier or eviction between the check and the
        /// install: the cache no longer holds the snapshot, and nothing is
        /// installed.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn an_invalidation_after_the_check_prevents_the_install() {
            use crate::mysql::mysql_activation::{
                BarrierScope, barrier_stream,
            };
            for evict in [false, true] {
                let db = if evict {
                    "fwd_post_evict"
                } else {
                    "fwd_post_barrier"
                };
                let (_fault, st, mut r, port) = cached_table(db, true).await;
                let key = st.scope.current().unwrap().key(db, "t");
                let lineage = barrier_stream(&key, &BarrierScope::Lineage);
                *super::super::hooks::BEFORE_INSTALL.lock().unwrap() =
                    Some(Box::new(move |ctx| {
                        if evict {
                            ctx.selection.invalidate_all();
                        } else {
                            ctx.selection.barrier_appended(&lineage);
                        }
                    }));
                sql(port, &[&format!("ALTER TABLE {db}.t ADD COLUMN w INT")])
                    .await;
                ddl_event(&mut r.rx, "ADD COLUMN w").await;
                stop(r.handle).await;
                assert!(installed(&st, db, "t").is_empty(), "{db}");
            }
        }

        /// Another process's write between the check and the install is not
        /// seen: freshness holds at the check only. With the witness the
        /// install goes ahead without it (the timeline then misses the
        /// write), which is why C3 rests on ownership: while this process
        /// holds the gate, no other server can start on the store. Without
        /// the witness the same write is followed by a reload.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_write_after_the_check_is_excluded_only_by_ownership() {
            use crate::mysql::mysql_activation::{ACTIVATION_NS, table_stream};
            for gated in [true, false] {
                let db = if gated {
                    "fwd_post_gated"
                } else {
                    "fwd_post_open"
                };
                let (_fault, st, mut r, port) = cached_table(db, gated).await;
                let key = st.scope.current().unwrap().key(db, "t");
                let stream = table_stream(&key);
                let backend = st.backend.clone();
                *super::super::hooks::BEFORE_INSTALL.lock().unwrap() =
                    Some(Box::new(move |_ctx| {
                        // Another process: its own runtime, on its own thread.
                        std::thread::spawn(move || {
                            tokio::runtime::Builder::new_current_thread()
                                .build()
                                .unwrap()
                                .block_on(foreign_append(
                                    &backend,
                                    ACTIVATION_NS,
                                    &stream,
                                    "foreign-after-check",
                                    Kind::Ddl,
                                ));
                        })
                        .join()
                        .unwrap();
                    }));
                sql(port, &[&format!("ALTER TABLE {db}.t ADD COLUMN w INT")])
                    .await;
                ddl_event(&mut r.rx, "ADD COLUMN w").await;
                stop(r.handle).await;
                let got = installed(&st, db, "t");
                if gated {
                    assert_eq!(got.len(), 1);
                    assert_ne!(
                        got[0].1.records,
                        records(&st, db, "t").await,
                        "not seen: the interval is excluded by ownership only"
                    );
                    let err = storage::adapters::store_gate::acquire(
                        &st.backend,
                        storage::adapters::store_gate::GateRole::Server,
                    )
                    .await
                    .expect_err("a second server cannot start on the store");
                    assert!(matches!(
                        err,
                        storage::adapters::store_gate::GateError::Held(_)
                    ));
                } else {
                    assert!(got.is_empty());
                }
            }
        }
        /// Retry bounds for the live tests: 2 s lock waits within 8 s.
        fn limits() -> deltaforge_config::ForwardProofRetry {
            deltaforge_config::ForwardProofRetry {
                attempts: 4,
                deadline: Duration::from_secs(8),
                backoff: vec![
                    Duration::from_millis(500),
                    Duration::from_secs(1),
                    Duration::from_secs(2),
                ],
                min_capture: Duration::from_secs(1),
                lock_wait: Duration::from_secs(2),
            }
        }

        /// The `observed` records binding `ddl`: exactly one once bound.
        fn bindings(recs: &[Stored], ddl: &Stored) -> usize {
            recs.iter()
                .filter(|s| {
                    matches!(&s.record.kind, Kind::Observed { binds: Some(b), .. } if *b == ddl.capture_id)
                })
                .count()
        }

        /// The pipeline's incidents.
        async fn incidents(
            st: &State,
        ) -> Vec<storage::adapters::incidents::IncidentRecord> {
            storage::adapters::incidents::IncidentStore::new(
                st.backend.clone(),
                "test",
            )
            .list()
            .await
            .unwrap()
        }

        /// The pipeline's one incident.
        async fn only_incident(
            st: &State,
        ) -> storage::adapters::incidents::IncidentRecord {
            let all = incidents(st).await;
            assert_eq!(all.len(), 1, "{all:?}");
            all.into_iter().next().unwrap()
        }

        /// A second session that holds `t`'s metadata lock: `LOCK TABLES t
        /// WRITE`, then the DDL under it (so the proof's capture waits),
        /// released by [`Blocker::release`].
        struct Blocker(mysql_async::Conn);

        impl Blocker {
            async fn alter(port: u16, t: &str, ddl: &str) -> Self {
                let mut c =
                    mysql_async::Conn::from_url(dsn(port)).await.unwrap();
                c.query_drop(format!("LOCK TABLES {t} WRITE"))
                    .await
                    .unwrap();
                c.query_drop(format!("ALTER TABLE {t} {ddl}"))
                    .await
                    .unwrap();
                Self(c)
            }

            /// Another statement in the blocking session (still holding the
            /// lock).
            async fn then(&mut self, stmt: &str) {
                self.0.query_drop(stmt).await.unwrap();
            }

            async fn release(mut self) {
                self.0.query_drop("UNLOCK TABLES").await.unwrap();
                self.0.disconnect().await.ok();
            }
        }

        /// Wait up to `within` for the DDL event containing `needle`.
        async fn ddl_event_within(
            rx: &mut mpsc::Receiver<SourceItem>,
            needle: &str,
            within: Duration,
        ) -> bool {
            let deadline = Instant::now() + within;
            while let Some(left) =
                deadline.checked_duration_since(Instant::now())
            {
                match tokio::time::timeout(left, rx.recv()).await {
                    Ok(Some(SourceItem::Event(e)))
                        if e.ddl.as_ref().is_some_and(|d| {
                            d.to_string().contains(needle)
                        }) =>
                    {
                        return true;
                    }
                    Ok(Some(_)) => {}
                    _ => return false,
                }
            }
            false
        }

        /// The next row event of `table` within 10 s.
        async fn row_of(
            rx: &mut mpsc::Receiver<SourceItem>,
            table: &str,
        ) -> deltaforge_core::Event {
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    match rx.recv().await {
                        Some(SourceItem::Event(e))
                            if e.ddl.is_none() && e.source.table == table =>
                        {
                            break e;
                        }
                        Some(_) => {}
                        None => panic!("the source ended"),
                    }
                }
            })
            .await
            .expect("a row")
        }

        /// The capture sessions waiting for a metadata lock.
        async fn lock_waiters(port: u16) -> u64 {
            let mut c = mysql_async::Conn::from_url(dsn(port)).await.unwrap();
            let n: Option<u64> = c
                .query_first(
                    "SELECT COUNT(*) FROM information_schema.processlist \
                     WHERE state = 'Waiting for table metadata lock'",
                )
                .await
                .unwrap();
            c.disconnect().await.ok();
            n.unwrap_or(0)
        }

        /// A metadata lock held past the first capture's lock wait is a
        /// transient failure: the proof is attempted again in place, binds
        /// the DDL once, and the row after it is decoded with the new
        /// column (by the forward proof, not a lazy baseline). The
        /// retrying incident is resolved as a verified recovery.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_transient_lock_wait_is_retried_and_the_ddl_bound() {
            let port = server(true).await;
            let db = "fwd_retry_ok";
            prepare(port, db).await;
            let st = state(Arc::new(MemoryStorageBackend::new())).await;
            let t = format!("{db}.t");
            let r = run(&st, port, db, &[&t]).await;
            stop(r.handle).await;
            let before = versions(&st, db, "t").await;
            let mut r = run_with(&st, port, db, &[&t], limits()).await;
            let blocker = Blocker::alter(port, &t, "ADD COLUMN w INT").await;
            // Past the first capture's 2 s lock wait, released in the first
            // backoff.
            tokio::time::sleep(Duration::from_millis(2300)).await;
            blocker.release().await;
            sql(port, &[&format!("INSERT INTO {t} VALUES (1, 1, 7)")]).await;
            assert!(
                ddl_event_within(
                    &mut r.rx,
                    "ADD COLUMN w",
                    Duration::from_secs(60)
                )
                .await,
                "the DDL is emitted"
            );
            let recs = records(&st, db, "t").await;
            let d = ddls(&recs);
            assert_eq!(d.len(), 1, "{recs:?}");
            assert!(
                bound(&st, db, "t", &recs, d[0]).await.is_some(),
                "bound by a retried forward proof, not deferred"
            );
            assert_eq!(bindings(&recs, d[0]), 1, "one durable binding");
            assert_eq!(versions(&st, db, "t").await, before + 1);
            assert!(
                !recs
                    .iter()
                    .any(|s| matches!(s.record.kind, Kind::Baseline { .. })),
                "no lazy baseline: {recs:?}"
            );
            let seven = deltaforge_core::canonical::fixture::value(
                &serde_json::json!(7),
            );
            let row = row_of(&mut r.rx, "t").await;
            assert_eq!(row.after.as_ref().unwrap().get("w"), Some(&seven));
            let incident = only_incident(&st).await;
            assert!(
                matches!(
                    &incident.status,
                    IncidentStatus::Resolved {
                        by: Resolution::VerifiedRecovery { check },
                        ..
                    } if check == "forward_proof_bound"
                ),
                "{incident:?}"
            );
            stop(r.handle).await;
        }

        /// A metadata lock held past every attempt stops the source before
        /// the DDL is emitted or checkpointed, on the operator-action
        /// `mysql_forward_proof_unavailable` draft of the incident recorded
        /// while it retried; no attempt starts after the deadline, no
        /// capture session outlives it, and nothing of the DDL is bound.
        /// The restart, once the lock is gone, binds it once and delivers
        /// the row after it.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn retries_exhausted_stop_the_source_before_the_ddl() {
            let port = server(true).await;
            let db = "fwd_retry_exhausted";
            prepare(port, db).await;
            let st = state(Arc::new(MemoryStorageBackend::new())).await;
            let t = format!("{db}.t");
            let r = run(&st, port, db, &[&t]).await;
            stop(r.handle).await;
            let committed = st.ckpt.get_raw(db).await.unwrap();
            let mut r = run_with(&st, port, db, &[&t], limits()).await;
            let blocker = Blocker::alter(port, &t, "ADD COLUMN w INT").await;
            sql(port, &[&format!("INSERT INTO {db}.other VALUES (1)")]).await;
            let started = Instant::now();
            let mut join = r.handle.join;
            // The channel closes when the source stops: only an emitted DDL
            // fails here.
            let ended = tokio::select! {
                ended = &mut join => ended,
                emitted = ddl_event_within(&mut r.rx, "ADD COLUMN w", Duration::from_secs(60)) => {
                    assert!(!emitted, "the DDL was emitted instead of stopping the source");
                    join.await
                }
            }
            .expect("task");
            let elapsed = started.elapsed();
            let err = ended.expect_err("exhausted retries stop the source");
            let draft = err.draft().expect("an incident").clone();
            assert_eq!(
                draft.reason_code,
                deltaforge_core::incident::ReasonCode::MysqlForwardProofUnavailable
            );
            assert_eq!(draft.retryability, Retryability::OperatorAction);
            // The 8 s deadline bounds the attempts (the stop included).
            assert!(elapsed < Duration::from_secs(11), "{elapsed:?}");
            // At most the 4 attempts, each started before the deadline
            // (the exact boundary is a Docker-free test's, on a paused
            // clock).
            assert!(
                matches!(
                    draft.evidence.get(EvidenceKey::Attempts),
                    Some(EvidenceValue::Count { value: 2..=4 })
                ),
                "{draft:?}"
            );
            assert!(
                draft
                    .evidence
                    .holds(EvidenceKey::ReasonClass, "lock_wait_timeout"),
                "{draft:?}"
            );
            // No capture session of the proof is still waiting.
            tokio::time::sleep(Duration::from_secs(1)).await;
            assert_eq!(lock_waiters(port).await, 0);
            // Recorded while retrying, as auto retry; the supervisor turns
            // the same incident into operator action from the stop's draft.
            let incident = only_incident(&st).await;
            assert_eq!(incident.retryability, Retryability::AutoRetry);
            let store = storage::adapters::incidents::IncidentStore::new(
                st.backend.clone(),
                "test",
            );
            let epoch = store.recovery_epoch().await.unwrap();
            assert_eq!(
                incident.incident_id,
                storage::adapters::incidents::bind_epoch(draft.clone(), epoch)
                    .incident_id("test"),
                "the stop's draft is the retrying incident"
            );
            assert_eq!(st.ckpt.get_raw(db).await.unwrap(), committed);
            let mut emitted = Vec::new();
            while let Ok(item) = r.rx.try_recv() {
                if let SourceItem::Event(e) = item {
                    emitted.push(e);
                }
            }
            assert!(
                emitted.iter().all(|e| e.source.table != "t"),
                "nothing of the DDL: {emitted:?}"
            );
            let recs = records(&st, db, "t").await;
            let d = ddls(&recs);
            assert_eq!(d.len(), 1, "{recs:?}");
            assert_eq!(bound(&st, db, "t", &recs, d[0]).await, None);
            blocker.release().await;
            sql(port, &[&format!("INSERT INTO {t} VALUES (1, 1, 7)")]).await;

            let mut r = run(&st, port, db, &[&t]).await;
            assert!(
                ddl_event_within(
                    &mut r.rx,
                    "ADD COLUMN w",
                    Duration::from_secs(60)
                )
                .await
            );
            let recs = records(&st, db, "t").await;
            let d = ddls(&recs);
            assert_eq!(d.len(), 1);
            assert!(bound(&st, db, "t", &recs, d[0]).await.is_some());
            assert_eq!(bindings(&recs, d[0]), 1, "one durable binding");
            let row = row_of(&mut r.rx, "t").await;
            assert_eq!(
                row.after.as_ref().unwrap().get("w"),
                Some(&deltaforge_core::canonical::fixture::value(
                    &serde_json::json!(7)
                ))
            );
            // Within this uninterrupted run the row is delivered once (a
            // duplicate across the stop and replay would be allowed: delivery
            // is at-least-once).
            assert!(
                tokio::time::timeout(Duration::from_secs(2), async {
                    loop {
                        match r.rx.recv().await {
                            Some(SourceItem::Event(e))
                                if e.ddl.is_none() && e.source.table == "t" =>
                            {
                                break;
                            }
                            Some(_) => {}
                            None => std::future::pending::<()>().await,
                        }
                    }
                })
                .await
                .is_err(),
                "the row once"
            );
            stop(r.handle).await;
        }

        /// A transient failure followed by an evidence refusal is a
        /// deferral, not a recovery: the blocking session changes the table
        /// again before releasing it, so the retried proof finds that DDL
        /// in (D, S]. The retrying incident is withdrawn (`ended_deferred`),
        /// never resolved as recovered; the DDL is emitted with the table
        /// pending, and the stream proves the table after the second DDL.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_transient_failure_then_a_refusal_is_a_deferral() {
            let port = server(true).await;
            let db = "fwd_retry_deferred";
            prepare(port, db).await;
            let st = state(Arc::new(MemoryStorageBackend::new())).await;
            let t = format!("{db}.t");
            let r = run(&st, port, db, &[&t]).await;
            stop(r.handle).await;
            let mut r = run_with(&st, port, db, &[&t], limits()).await;
            let mut blocker =
                Blocker::alter(port, &t, "ADD COLUMN w INT").await;
            tokio::time::sleep(Duration::from_millis(2300)).await;
            blocker
                .then(&format!("ALTER TABLE {t} ADD COLUMN z INT"))
                .await;
            blocker.release().await;
            assert!(
                ddl_event_within(
                    &mut r.rx,
                    "ADD COLUMN w",
                    Duration::from_secs(60)
                )
                .await,
                "the first DDL is emitted (deferred)"
            );
            assert!(
                ddl_event_within(
                    &mut r.rx,
                    "ADD COLUMN z",
                    Duration::from_secs(60)
                )
                .await
            );
            let recs = records(&st, db, "t").await;
            let d = ddls(&recs);
            assert_eq!(d.len(), 2, "{recs:?}");
            assert_eq!(
                bound(&st, db, "t", &recs, d[0]).await,
                None,
                "the first stays pending"
            );
            assert!(bound(&st, db, "t", &recs, d[1]).await.is_some());
            let incident = only_incident(&st).await;
            assert!(
                matches!(
                    incident.status,
                    IncidentStatus::Resolved {
                        by: Resolution::OperationCancelled,
                        ..
                    }
                ),
                "{incident:?}"
            );
            assert!(
                incident
                    .evidence
                    .holds(EvidenceKey::ReasonClass, "ended_deferred"),
                "{incident:?}"
            );
            assert_eq!(
                incident.evidence.get(EvidenceKey::BlockingCount),
                Some(&EvidenceValue::Count { value: 1 })
            );
            stop(r.handle).await;
        }

        /// A stop while the proof's capture waits for the metadata lock:
        /// the source ends at once, the capture's session is killed on the
        /// server, nothing of the DDL is emitted, checkpointed or bound, and
        /// the retrying incident is withdrawn (`ended_cancelled`). The
        /// restart binds the DDL.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn a_stop_during_a_lock_wait_ends_the_proof() {
            let port = server(true).await;
            let db = "fwd_retry_stop";
            prepare(port, db).await;
            let st = state(Arc::new(MemoryStorageBackend::new())).await;
            let t = format!("{db}.t");
            let r = run(&st, port, db, &[&t]).await;
            stop(r.handle).await;
            let committed = st.ckpt.get_raw(db).await.unwrap();
            let r = run_with(
                &st,
                port,
                db,
                &[&t],
                deltaforge_config::ForwardProofRetry {
                    lock_wait: Duration::from_secs(5),
                    deadline: Duration::from_secs(60),
                    ..limits()
                },
            )
            .await;
            let blocker = Blocker::alter(port, &t, "ADD COLUMN w INT").await;
            // The first attempt fails after ~4 s (recorded as retrying);
            // the stop lands in the second attempt's lock wait.
            let waiting = Instant::now() + Duration::from_secs(30);
            while lock_waiters(port).await == 0 {
                assert!(Instant::now() < waiting, "no capture waits");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            tokio::time::sleep(Duration::from_secs(7)).await;
            assert_eq!(lock_waiters(port).await, 1, "the second attempt waits");
            let stopped = Instant::now();
            r.handle.stop();
            let ended =
                tokio::time::timeout(Duration::from_secs(10), r.handle.join)
                    .await
                    .expect("stops at once")
                    .expect("task");
            assert!(stopped.elapsed() < Duration::from_secs(3));
            assert!(
                matches!(
                    ended,
                    Err(deltaforge_core::SourceError::Cancelled) | Ok(())
                ),
                "{ended:?}"
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
            assert_eq!(lock_waiters(port).await, 0, "the session is killed");
            assert_eq!(st.ckpt.get_raw(db).await.unwrap(), committed);
            let recs = records(&st, db, "t").await;
            assert_eq!(bound(&st, db, "t", &recs, ddls(&recs)[0]).await, None);
            let incident = only_incident(&st).await;
            assert!(
                matches!(
                    incident.status,
                    IncidentStatus::Resolved {
                        by: Resolution::OperationCancelled,
                        ..
                    }
                ) && incident
                    .evidence
                    .holds(EvidenceKey::ReasonClass, "ended_cancelled"),
                "{incident:?}"
            );
            blocker.release().await;
            let mut r = run(&st, port, db, &[&t]).await;
            assert!(
                ddl_event_within(
                    &mut r.rx,
                    "ADD COLUMN w",
                    Duration::from_secs(60)
                )
                .await
            );
            let recs = records(&st, db, "t").await;
            assert!(bound(&st, db, "t", &recs, ddls(&recs)[0]).await.is_some());
            stop(r.handle).await;
        }

        /// An authorization failure at the proof's capture (the account's
        /// `SELECT` on the table revoked) is never retried: the source stops
        /// before the DDL on the operator-action draft, cause `source_auth`,
        /// with no retrying incident. With the grant back, the restart binds
        /// the DDL.
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn an_authorization_failure_stops_before_the_ddl() {
            let port = server(true).await;
            let db = "fwd_retry_denied";
            prepare(port, db).await;
            let user = "fwd_denied";
            sql(
                port,
                &[
                    &format!("DROP USER IF EXISTS '{user}'@'%'"),
                    &format!("CREATE USER '{user}'@'%' IDENTIFIED BY 'pw'"),
                    &format!(
                        "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* \
                         TO '{user}'@'%'"
                    ),
                    &format!("GRANT SELECT ON {db}.t TO '{user}'@'%'"),
                    &format!("GRANT SELECT ON {db}.other TO '{user}'@'%'"),
                ],
            )
            .await;
            let user_dsn = format!("mysql://{user}:pw@127.0.0.1:{port}/");
            let st = state(Arc::new(MemoryStorageBackend::new())).await;
            let t = format!("{db}.t");
            let r = run_as(&st, &user_dsn, db, &[&t], limits()).await;
            stop(r.handle).await;
            let committed = st.ckpt.get_raw(db).await.unwrap();
            let r = run_as(&st, &user_dsn, db, &[&t], limits()).await;
            // Revoked before the DDL (MySQL checks the privilege before any
            // metadata-lock wait): the proof's first capture is refused.
            sql(
                port,
                &[
                    &format!("REVOKE SELECT ON {db}.t FROM '{user}'@'%'"),
                    &format!("ALTER TABLE {t} ADD COLUMN w INT"),
                ],
            )
            .await;
            let ended =
                tokio::time::timeout(Duration::from_secs(30), r.handle.join)
                    .await
                    .expect("stops")
                    .expect("task");
            let err = ended.expect_err("the refused proof stops the source");
            let draft = err.draft().expect("an incident");
            assert_eq!(
                draft.reason_code,
                deltaforge_core::incident::ReasonCode::MysqlForwardProofUnavailable
            );
            assert_eq!(draft.retryability, Retryability::OperatorAction);
            assert_eq!(
                draft.cause_code,
                deltaforge_core::incident::CauseCode::SourceAuth
            );
            assert!(
                draft
                    .evidence
                    .holds(EvidenceKey::ReasonClass, "authorization"),
                "{draft:?}"
            );
            assert!(matches!(
                err.root(),
                deltaforge_core::SourceError::Auth { .. }
            ));
            for text in [format!("{err}"), format!("{err:?}")] {
                assert!(!text.contains(":pw@"), "{text}");
            }
            assert_eq!(st.ckpt.get_raw(db).await.unwrap(), committed);
            let recs = records(&st, db, "t").await;
            assert_eq!(bound(&st, db, "t", &recs, ddls(&recs)[0]).await, None);
            assert!(incidents(&st).await.is_empty(), "never retried");

            sql(port, &[&format!("GRANT SELECT ON {db}.t TO '{user}'@'%'")])
                .await;
            let mut r = run_as(&st, &user_dsn, db, &[&t], limits()).await;
            assert!(
                ddl_event_within(
                    &mut r.rx,
                    "ADD COLUMN w",
                    Duration::from_secs(60)
                )
                .await
            );
            let recs = records(&st, db, "t").await;
            assert!(bound(&st, db, "t", &recs, ddls(&recs)[0]).await.is_some());
            stop(r.handle).await;
        }
    }
}
