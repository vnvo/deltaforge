//! Durable operator incidents: one versioned record per incident (slot
//! namespace `incidents`, changed only with create/CAS), an append-only audit
//! log of lifecycle transitions (`incidents.audit`) and a per-pipeline control
//! record (`incidents.control`).
//!
//! - Lifecycle Open -> Acknowledged -> Resolved: acknowledgement means an
//!   operator has seen it and never clears it; only a verified resolver (or a
//!   recovery operation) resolves. Each lifecycle transition advances the
//!   record's semantic `transition_seq` and is written with its complete audit
//!   entry as `pending_audit` in the same CAS; the entry is then appended
//!   idempotently (`log_append_if_absent`, capture id = incident id + sequence)
//!   and cleared. A crash at any point is repaired by replaying the pending
//!   entry, which never creates another transition; every later transition
//!   repairs first.
//! - Bounds: at most [`MAX_ORDINARY_OPEN`] ordinary open incidents per pipeline
//!   plus one fixed overflow incident that counts the rest by reason code.
//!   Blocking incidents take priority: one may displace the oldest open,
//!   unacknowledged non-blocking incident (counted in the overflow). Blocking
//!   or acknowledged incidents are never evicted. At most [`MAX_RESOLVED`]
//!   resolved records are kept (the audit log keeps the history).
//! - Recovery epoch: the control record holds the epoch that identities of
//!   conditions settled by a verified start are bound to. Verified recovery
//!   advances it with one CAS that also names the incidents it resolves; the
//!   resolutions are completed (and repaired after a crash) from that record,
//!   so there is never more than one active generation and no current
//!   incident is lost.
//!
//! Records hold codes and allow-listed evidence only; nothing here stores an
//! error's Display/Debug text.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::{Context, Result};
use deltaforge_core::incident::{
    ActionCode, CauseCode, Component, EvidenceKey, IncidentDraft, IncidentId,
    ReasonCode, Retryability, SafetyState,
};
use metrics::{counter, gauge};
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::ArcStorageBackend;

/// Slot namespace of incident records.
pub const INCIDENTS_NS: &str = "incidents";
/// Log namespace of the incident audit trail.
pub const AUDIT_NS: &str = "incidents.audit";
/// Slot namespace of the per-pipeline incident control record.
pub const CONTROL_NS: &str = "incidents.control";
/// Ordinary open (or acknowledged) incidents kept per pipeline, beside the
/// one overflow incident.
pub const MAX_ORDINARY_OPEN: usize = 63;
/// Resolved incidents kept per pipeline.
pub const MAX_RESOLVED: usize = 100;
/// Longest operator-supplied acknowledgement field kept.
pub const MAX_ACK_TEXT: usize = 256;
/// Most recent reason codes an overflow record keeps.
pub const OVERFLOW_RECENT: usize = 8;
/// The resolver recorded by verified pipeline recovery.
pub const PIPELINE_RECOVERED: &str = "pipeline_recovered";
/// The source verified it is connected to its recorded lineage.
pub const LINEAGE_VERIFIED: &str = "lineage_verified";
/// The source verified its resume position is available.
pub const POSITION_VERIFIED: &str = "position_verified";
/// A later stream proved its slot is a PostgreSQL failover slot.
pub const FAILOVER_SLOT_VERIFIED: &str = "failover_slot_verified";
/// The table's schema was accepted at its first use in a later run.
pub const SCHEMA_ACCEPTED: &str = "schema_accepted";
/// An authoritative read of exactly the uncertain boundary proved the write
/// applied.
pub const SINK_BOUNDARY_COMMITTED: &str = "sink_boundary_committed";
/// An authoritative read of exactly the uncertain boundary proved the write
/// did not apply.
pub const SINK_BOUNDARY_ABSENT: &str = "sink_boundary_absent";

const RECORD_FORMAT: u32 = 2;
const CONTROL_FORMAT: u32 = 1;

/// How an incident was resolved.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Resolution {
    /// A verified check passed (for `unclassified_failure`:
    /// `pipeline_recovered`, which does not claim the cause was understood).
    VerifiedRecovery { check: String },
    /// A recovery operation changed what it was about (e.g. the source epoch).
    RecoveryOperation { operation: String },
    /// The operation it was retrying was stopped on purpose (the pipeline was
    /// stopped while it retried automatically): not a failure.
    OperationCancelled,
}

/// Where an incident is in its lifecycle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum IncidentStatus {
    Open,
    /// An operator has seen it. Still blocking, still open for metrics and
    /// readiness. `asserted_actor` is caller-supplied, not authenticated.
    Acknowledged {
        asserted_actor: String,
        actor_verified: bool,
        origin: Option<String>,
        reason: String,
        at_ms: i64,
    },
    Resolved {
        by: Resolution,
        at_ms: i64,
    },
}

impl IncidentStatus {
    pub fn is_resolved(&self) -> bool {
        matches!(self, Self::Resolved { .. })
    }
}

/// What the overflow incident counts: incidents not kept individually, by
/// stable reason code.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OverflowSummary {
    pub by_reason: BTreeMap<ReasonCode, u64>,
    /// How many of them were blocking.
    pub blocking: u64,
    /// The most recent reason codes, newest last.
    pub recent: Vec<ReasonCode>,
}

impl OverflowSummary {
    fn add(&mut self, reason: ReasonCode, blocking: bool, n: u64) {
        *self.by_reason.entry(reason).or_default() += n;
        if blocking {
            self.blocking += n;
        }
        self.recent.push(reason);
        if self.recent.len() > OVERFLOW_RECENT {
            self.recent.remove(0);
        }
    }

    fn total(&self) -> u64 {
        self.by_reason.values().sum()
    }
}

/// The durable record. Holds codes and allow-listed evidence only; the
/// explanation is generated from them on read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IncidentRecord {
    pub format: u32,
    pub incident_id: IncidentId,
    pub pipeline: String,
    pub reason_code: ReasonCode,
    pub component: Component,
    pub retryability: Retryability,
    pub safety_state: SafetyState,
    pub cause_code: CauseCode,
    pub evidence: deltaforge_core::incident::Evidence,
    pub actions: Vec<ActionCode>,
    pub first_seen_ms: i64,
    pub last_seen_ms: i64,
    pub occurrences: u64,
    pub status: IncidentStatus,
    /// Lifecycle transitions so far (opened = 1).
    pub transition_seq: u64,
    /// The audit entry of the latest transition until it is appended.
    pub pending_audit: Option<AuditEntry>,
    /// Present on the overflow incident only.
    pub overflow: Option<OverflowSummary>,
    /// What a scoped resolver matches on (opaque digest), if any.
    #[serde(default)]
    pub resolve_scope: Option<String>,
}

impl IncidentRecord {
    /// The sanitized explanation (generated, never stored).
    pub fn explanation(&self) -> String {
        deltaforge_core::incident::explain(
            self.reason_code,
            &self.component,
            self.cause_code,
            &self.evidence,
        )
    }

    /// Open or acknowledged, and stopping the pipeline.
    pub fn is_blocking(&self) -> bool {
        !self.status.is_resolved() && self.safety_state.is_blocking()
    }

    fn is_overflow(&self) -> bool {
        self.reason_code == ReasonCode::IncidentOverflow
    }
}

/// One lifecycle transition in the audit log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditEntry {
    pub at_ms: i64,
    pub pipeline: String,
    pub incident_id: IncidentId,
    pub reason_code: ReasonCode,
    pub transition_seq: u64,
    pub transition: Transition,
}

impl AuditEntry {
    /// The deterministic capture identity of this transition.
    pub fn capture_id(&self) -> String {
        format!("{}:{}", self.incident_id, self.transition_seq)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Transition {
    Opened,
    Reopened,
    Acknowledged {
        asserted_actor: String,
        actor_verified: bool,
        origin: Option<String>,
        reason: String,
    },
    Resolved {
        by: Resolution,
    },
    /// A non-blocking incident displaced by a blocking one at the limit; it
    /// is counted in the overflow incident from now on.
    Displaced,
    /// The same condition raised with another classification (an automatic
    /// retry that exhausted its budget now needs an operator). The record
    /// takes the new retryability, safety state and actions.
    Reclassified {
        retryability: Retryability,
        safety_state: SafetyState,
    },
}

/// Why an acknowledgement or resolution did not apply.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum LifecycleError {
    #[error("incident {0} not found")]
    NotFound(String),
    #[error("incident {0} is already resolved")]
    AlreadyResolved(String),
}

/// What became of a raised draft.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Raised {
    /// Recorded (or counted) on its own record.
    Recorded(IncidentRecord),
    /// The limit was reached: counted on the overflow incident.
    Overflowed(IncidentRecord),
}

impl Raised {
    pub fn record(&self) -> &IncidentRecord {
        match self {
            Self::Recorded(r) | Self::Overflowed(r) => r,
        }
    }
}

/// The per-pipeline control record.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IncidentControl {
    pub format: u32,
    /// The generation `unclassified_failure` identities are bound to.
    pub recovery_epoch: u64,
    /// A verified recovery whose resolutions are not all applied yet.
    pub pending_recovery: Option<PendingRecovery>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingRecovery {
    pub to_epoch: u64,
    /// Each incident with the check that resolves it.
    pub resolves: Vec<(IncidentId, String)>,
}

/// The check a verified start (the pipeline's source ready while its
/// coordinator runs and nothing failed) resolves `rec` with, if it settles
/// it: any unclassified failure, and the source's identity and position
/// conditions (they are checked before the source reports ready).
pub fn settled_by_verified_start(rec: &IncidentRecord) -> Option<&'static str> {
    verified_start_check(rec.reason_code, &rec.component)
}

/// [`settled_by_verified_start`] by reason and component. Identities of these
/// conditions are bound to the recovery epoch, so a recurrence after a
/// genuine recovery is a new occurrence.
pub fn verified_start_check(
    reason: ReasonCode,
    component: &Component,
) -> Option<&'static str> {
    match (reason, component) {
        (ReasonCode::UnclassifiedFailure, _) => Some(PIPELINE_RECOVERED),
        (ReasonCode::PgDifferentCluster, Component::Source { .. }) => {
            Some(LINEAGE_VERIFIED)
        }
        (
            ReasonCode::PgContinuityUnproven
            | ReasonCode::MysqlGtidPositionUnavailable,
            Component::Source { .. },
        ) => Some(POSITION_VERIFIED),
        _ => None,
    }
}

/// Bind a source's draft to the recovery epoch when a verified start settles
/// its reason (so a recurrence after a genuine recovery is a new occurrence;
/// before one, the same incident).
pub fn bind_epoch(draft: IncidentDraft, epoch: u64) -> IncidentDraft {
    if verified_start_check(draft.reason_code, &draft.component).is_some()
        && draft.reason_code != ReasonCode::UnclassifiedFailure
    {
        draft.discriminate("recovery_epoch", epoch.to_string())
    } else {
        draft
    }
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// Percent-encode `/` and `%` so a pipeline name is one unambiguous key
/// segment (`a/b` and `a` never share a prefix).
fn segment(s: &str) -> String {
    s.replace('%', "%25").replace('/', "%2F")
}

fn bounded(s: &str) -> String {
    s.chars().take(MAX_ACK_TEXT).collect()
}

fn overflow_draft() -> IncidentDraft {
    IncidentDraft::new(
        ReasonCode::IncidentOverflow,
        Component::Pipeline,
        Retryability::OperatorAction,
        SafetyState::RunningDegraded,
        CauseCode::IncidentLimit,
    )
    .discriminate("overflow", "1")
    .with_actions(&[ActionCode::InspectLogs])
}

/// Durable incident records, their audit trail and control record for one
/// pipeline.
#[derive(Clone)]
pub struct IncidentStore {
    backend: ArcStorageBackend,
    pipeline: String,
}

impl IncidentStore {
    pub fn new(backend: ArcStorageBackend, pipeline: &str) -> Self {
        Self {
            backend,
            pipeline: pipeline.to_string(),
        }
    }

    pub fn pipeline(&self) -> &str {
        &self.pipeline
    }

    fn prefix(&self) -> String {
        format!("{}/", segment(&self.pipeline))
    }

    fn key(&self, id: &IncidentId) -> String {
        format!("{}{}", self.prefix(), id.0)
    }

    async fn append_audit(&self, entry: &AuditEntry) -> Result<()> {
        self.backend
            .log_append_if_absent(
                AUDIT_NS,
                &segment(&self.pipeline),
                &entry.capture_id(),
                &serde_json::to_vec(entry)?,
            )
            .await
            .context("append incident audit entry")?;
        Ok(())
    }

    async fn read(
        &self,
        id: &IncidentId,
    ) -> Result<Option<(u64, IncidentRecord)>> {
        match self.backend.slot_get(INCIDENTS_NS, &self.key(id)).await? {
            None => Ok(None),
            Some((v, bytes)) => Ok(Some((
                v,
                serde_json::from_slice(&bytes)
                    .context("decode incident record")?,
            ))),
        }
    }

    async fn cas(
        &self,
        id: &IncidentId,
        version: u64,
        rec: &IncidentRecord,
    ) -> Result<bool> {
        self.backend
            .slot_cas(
                INCIDENTS_NS,
                &self.key(id),
                version,
                &serde_json::to_vec(rec)?,
            )
            .await
    }

    /// The record with any pending audit entry appended and cleared (no new
    /// transition). Returns its current version.
    async fn repaired(
        &self,
        id: &IncidentId,
    ) -> Result<Option<(u64, IncidentRecord)>> {
        loop {
            let Some((version, rec)) = self.read(id).await? else {
                return Ok(None);
            };
            let Some(entry) = &rec.pending_audit else {
                return Ok(Some((version, rec)));
            };
            self.append_audit(entry).await?;
            let mut cleared = rec.clone();
            cleared.pending_audit = None;
            // A lost race re-reads; whoever wins has appended the same entry.
            self.cas(id, version, &cleared).await?;
        }
    }

    /// Apply one lifecycle transition (after repairing an earlier pending
    /// one): `f` returns the next record and its transition, or `None` for no
    /// change. The transition and its audit entry are written in one CAS.
    async fn transition(
        &self,
        id: &IncidentId,
        mut f: impl FnMut(&IncidentRecord) -> Option<(IncidentRecord, Transition)>,
    ) -> Result<Option<IncidentRecord>> {
        loop {
            let Some((version, rec)) = self.repaired(id).await? else {
                return Ok(None);
            };
            let Some((mut next, transition)) = f(&rec) else {
                return Ok(Some(rec));
            };
            next.transition_seq = rec.transition_seq + 1;
            let entry = AuditEntry {
                at_ms: now_ms(),
                pipeline: self.pipeline.clone(),
                incident_id: id.clone(),
                reason_code: next.reason_code,
                transition_seq: next.transition_seq,
                transition,
            };
            next.pending_audit = Some(entry);
            if self.cas(id, version, &next).await? {
                return Ok(self.repaired(id).await?.map(|(_, r)| r));
            }
        }
    }

    /// Create `rec` with its opening transition; `false` if it already exists.
    async fn create(&self, mut rec: IncidentRecord) -> Result<bool> {
        rec.transition_seq = 1;
        rec.pending_audit = Some(AuditEntry {
            at_ms: rec.first_seen_ms,
            pipeline: self.pipeline.clone(),
            incident_id: rec.incident_id.clone(),
            reason_code: rec.reason_code,
            transition_seq: 1,
            transition: Transition::Opened,
        });
        let id = rec.incident_id.clone();
        let created = self
            .backend
            .slot_create(
                INCIDENTS_NS,
                &self.key(&id),
                &serde_json::to_vec(&rec)?,
            )
            .await?;
        if created.is_none() {
            return Ok(false);
        }
        self.repaired(&id).await?;
        Ok(true)
    }

    pub async fn get(&self, id: &IncidentId) -> Result<Option<IncidentRecord>> {
        Ok(self.read(id).await?.map(|(_, r)| r))
    }

    /// Every record of this pipeline (bounded by the retention limits).
    pub async fn list(&self) -> Result<Vec<IncidentRecord>> {
        let mut out = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let page = self
                .backend
                .slot_list(
                    INCIDENTS_NS,
                    Some(&self.prefix()),
                    cursor.as_deref(),
                    crate::SLOT_LIST_MAX_LIMIT,
                )
                .await?;
            for rec in page.records {
                out.push(
                    serde_json::from_slice(&rec.value)
                        .context("decode incident record")?,
                );
            }
            match page.next_cursor {
                Some(c) => cursor = Some(c),
                None => return Ok(out),
            }
        }
    }

    /// Append every pending audit entry of this pipeline (crash repair).
    pub async fn repair_all(&self) -> Result<()> {
        for rec in self.list().await? {
            if rec.pending_audit.is_some() {
                self.repaired(&rec.incident_id).await?;
            }
        }
        Ok(())
    }

    fn fresh(&self, id: IncidentId, draft: &IncidentDraft) -> IncidentRecord {
        let now = now_ms();
        IncidentRecord {
            format: RECORD_FORMAT,
            incident_id: id,
            pipeline: self.pipeline.clone(),
            reason_code: draft.reason_code,
            component: draft.component.clone(),
            retryability: draft.retryability,
            safety_state: draft.safety_state,
            cause_code: draft.cause_code,
            evidence: draft.evidence.clone(),
            actions: draft.actions.clone(),
            first_seen_ms: now,
            last_seen_ms: now,
            occurrences: 1,
            status: IncidentStatus::Open,
            transition_seq: 0,
            pending_audit: None,
            overflow: None,
            resolve_scope: draft.resolve_scope.clone(),
        }
    }

    /// The automatic retry behind `id` was stopped on purpose: resolve it as
    /// [`Resolution::OperationCancelled`], only while it is still unresolved
    /// and `auto_retry` (a stop that needs an operator is never withdrawn).
    /// Returns whether it was resolved.
    pub async fn cancel_auto_retry(&self, id: &IncidentId) -> Result<bool> {
        let rec = self
            .transition(id, |r| {
                if r.status.is_resolved()
                    || r.retryability != Retryability::AutoRetry
                {
                    return None;
                }
                let mut next = r.clone();
                next.status = IncidentStatus::Resolved {
                    by: Resolution::OperationCancelled,
                    at_ms: now_ms(),
                };
                Some((
                    next,
                    Transition::Resolved {
                        by: Resolution::OperationCancelled,
                    },
                ))
            })
            .await?;
        let resolved = rec.is_some_and(|r| {
            r.status.is_resolved()
                && matches!(
                    r.status,
                    IncidentStatus::Resolved {
                        by: Resolution::OperationCancelled,
                        ..
                    }
                )
        });
        if resolved {
            self.prune_resolved().await?;
            self.refresh_metrics().await;
        }
        Ok(resolved)
    }

    /// Open `draft`'s incident, or count `occurrences` more of it (a resolved
    /// record of the same identity is reopened). At the limit it may displace
    /// a non-blocking incident (when it is blocking) or is counted on the
    /// overflow incident.
    pub async fn raise(
        &self,
        draft: &IncidentDraft,
        occurrences: u64,
    ) -> Result<Raised> {
        let n = occurrences.max(1);
        let id = draft.incident_id(&self.pipeline);
        loop {
            match self.repaired(&id).await? {
                Some((_, rec)) if rec.status.is_resolved() => {
                    let evidence = draft.evidence.clone();
                    let reopened = self
                        .transition(&id, |r| {
                            if !r.status.is_resolved() {
                                return None;
                            }
                            let mut next = r.clone();
                            next.status = IncidentStatus::Open;
                            next.evidence = evidence.clone();
                            next.retryability = draft.retryability;
                            next.safety_state = draft.safety_state;
                            next.cause_code = draft.cause_code;
                            next.actions = draft.actions.clone();
                            next.occurrences =
                                next.occurrences.saturating_add(n);
                            next.last_seen_ms = now_ms();
                            Some((next, Transition::Reopened))
                        })
                        .await?;
                    if let Some(rec) = reopened
                        && !rec.status.is_resolved()
                    {
                        self.count_raised(rec.reason_code);
                        self.refresh_metrics().await;
                        return Ok(Raised::Recorded(rec));
                    }
                }
                Some((_, rec))
                    if (rec.retryability, rec.safety_state, &rec.actions)
                        != (
                            draft.retryability,
                            draft.safety_state,
                            &draft.actions,
                        ) =>
                {
                    let reclassified = self
                        .transition(&id, |r| {
                            if r.status.is_resolved() {
                                return None;
                            }
                            let mut next = r.clone();
                            next.retryability = draft.retryability;
                            next.safety_state = draft.safety_state;
                            next.cause_code = draft.cause_code;
                            next.actions = draft.actions.clone();
                            next.evidence = draft.evidence.clone();
                            next.occurrences =
                                next.occurrences.saturating_add(n);
                            next.last_seen_ms = now_ms();
                            Some((
                                next,
                                Transition::Reclassified {
                                    retryability: draft.retryability,
                                    safety_state: draft.safety_state,
                                },
                            ))
                        })
                        .await?;
                    if let Some(rec) = reclassified
                        && !rec.status.is_resolved()
                    {
                        self.refresh_metrics().await;
                        return Ok(Raised::Recorded(rec));
                    }
                }
                Some((version, mut rec)) => {
                    rec.occurrences = rec.occurrences.saturating_add(n);
                    rec.last_seen_ms = now_ms();
                    if self.cas(&id, version, &rec).await? {
                        return Ok(Raised::Recorded(rec));
                    }
                }
                None => {
                    let all = self.list().await?;
                    let ordinary_open: Vec<&IncidentRecord> = all
                        .iter()
                        .filter(|r| !r.status.is_resolved() && !r.is_overflow())
                        .collect();
                    if ordinary_open.len() >= MAX_ORDINARY_OPEN {
                        if draft.safety_state.is_blocking()
                            && let Some(victim) = ordinary_open
                                .iter()
                                .filter(|r| {
                                    r.status == IncidentStatus::Open
                                        && !r.safety_state.is_blocking()
                                })
                                .min_by_key(|r| {
                                    (r.first_seen_ms, r.incident_id.0.clone())
                                })
                        {
                            self.displace(victim).await?;
                            continue;
                        }
                        let ov = self
                            .count_overflow(
                                draft.reason_code,
                                draft.safety_state.is_blocking(),
                                n,
                            )
                            .await?;
                        self.count_raised(draft.reason_code);
                        self.refresh_metrics().await;
                        return Ok(Raised::Overflowed(ov));
                    }
                    let mut rec = self.fresh(id.clone(), draft);
                    rec.occurrences = n;
                    if self.create(rec).await? {
                        let rec = self
                            .get(&id)
                            .await?
                            .context("incident record vanished")?;
                        self.count_raised(rec.reason_code);
                        self.refresh_metrics().await;
                        return Ok(Raised::Recorded(rec));
                    }
                }
            }
        }
    }

    /// Move a non-blocking open incident into the overflow count and remove
    /// it (audited as `Displaced`). A crash between the two leaves it counted
    /// once more than it should be, never lost.
    async fn displace(&self, victim: &IncidentRecord) -> Result<()> {
        self.count_overflow(victim.reason_code, false, victim.occurrences)
            .await?;
        let entry = AuditEntry {
            at_ms: now_ms(),
            pipeline: self.pipeline.clone(),
            incident_id: victim.incident_id.clone(),
            reason_code: victim.reason_code,
            transition_seq: victim.transition_seq + 1,
            transition: Transition::Displaced,
        };
        self.append_audit(&entry).await?;
        self.backend
            .slot_delete(INCIDENTS_NS, &self.key(&victim.incident_id))
            .await?;
        warn!(
            pipeline = %self.pipeline,
            incident = %victim.incident_id,
            reason = victim.reason_code.as_str(),
            "non-blocking incident displaced into the overflow count"
        );
        Ok(())
    }

    /// Count `n` incidents of `reason` on the overflow incident (created, or
    /// reopened, as needed; it is never limited).
    async fn count_overflow(
        &self,
        reason: ReasonCode,
        blocking: bool,
        n: u64,
    ) -> Result<IncidentRecord> {
        let draft = overflow_draft();
        let id = draft.incident_id(&self.pipeline);
        warn!(
            pipeline = %self.pipeline,
            reason = reason.as_str(),
            blocking,
            "incident limit reached; counted on the overflow incident"
        );
        let apply = |rec: &mut IncidentRecord| {
            let summary = rec.overflow.get_or_insert_with(Default::default);
            summary.add(reason, blocking, n);
            let (total, blocking_total) = (summary.total(), summary.blocking);
            rec.evidence.count(EvidenceKey::Attempts, total);
            rec.evidence
                .count(EvidenceKey::BlockingCount, blocking_total);
            if blocking_total > 0 {
                rec.safety_state = SafetyState::HaltedUncertain;
            }
            rec.occurrences = total;
            rec.last_seen_ms = now_ms();
        };
        loop {
            match self.repaired(&id).await? {
                None => {
                    let mut rec = self.fresh(id.clone(), &draft);
                    apply(&mut rec);
                    if self.create(rec).await? {
                        return self
                            .get(&id)
                            .await?
                            .context("overflow record vanished");
                    }
                }
                Some((_, rec)) if rec.status.is_resolved() => {
                    let reopened = self
                        .transition(&id, |r| {
                            if !r.status.is_resolved() {
                                return None;
                            }
                            let mut next = r.clone();
                            next.status = IncidentStatus::Open;
                            next.overflow = None;
                            next.evidence = Default::default();
                            next.safety_state = SafetyState::RunningDegraded;
                            apply(&mut next);
                            Some((next, Transition::Reopened))
                        })
                        .await?;
                    if let Some(rec) = reopened
                        && !rec.status.is_resolved()
                    {
                        return Ok(rec);
                    }
                }
                Some((version, mut rec)) => {
                    apply(&mut rec);
                    if self.cas(&id, version, &rec).await? {
                        return Ok(rec);
                    }
                }
            }
        }
    }

    /// Open -> Acknowledged (an acknowledged one is re-acknowledged). Never
    /// resolves. `asserted_actor` is caller-supplied and recorded as such.
    pub async fn acknowledge(
        &self,
        id: &IncidentId,
        asserted_actor: &str,
        origin: Option<&str>,
        reason: &str,
    ) -> Result<std::result::Result<IncidentRecord, LifecycleError>> {
        let (asserted_actor, origin, reason) = (
            bounded(asserted_actor),
            origin.map(bounded),
            bounded(reason),
        );
        let mut resolved = false;
        let rec = self
            .transition(id, |r| {
                if r.status.is_resolved() {
                    resolved = true;
                    return None;
                }
                let mut next = r.clone();
                next.status = IncidentStatus::Acknowledged {
                    asserted_actor: asserted_actor.clone(),
                    actor_verified: false,
                    origin: origin.clone(),
                    reason: reason.clone(),
                    at_ms: now_ms(),
                };
                Some((
                    next,
                    Transition::Acknowledged {
                        asserted_actor: asserted_actor.clone(),
                        actor_verified: false,
                        origin: origin.clone(),
                        reason: reason.clone(),
                    },
                ))
            })
            .await?;
        Ok(match rec {
            None => Err(LifecycleError::NotFound(id.0.clone())),
            Some(_) if resolved => {
                Err(LifecycleError::AlreadyResolved(id.0.clone()))
            }
            Some(r) => Ok(r),
        })
    }

    /// Open/Acknowledged -> Resolved. Resolving a resolved incident is a
    /// no-op. Prunes the oldest resolved records beyond [`MAX_RESOLVED`].
    pub async fn resolve(
        &self,
        id: &IncidentId,
        by: Resolution,
    ) -> Result<std::result::Result<IncidentRecord, LifecycleError>> {
        let rec = self
            .transition(id, |r| {
                if r.status.is_resolved() {
                    return None;
                }
                let mut next = r.clone();
                next.status = IncidentStatus::Resolved {
                    by: by.clone(),
                    at_ms: now_ms(),
                };
                Some((next, Transition::Resolved { by: by.clone() }))
            })
            .await?;
        match rec {
            None => Ok(Err(LifecycleError::NotFound(id.0.clone()))),
            Some(r) => {
                self.prune_resolved().await?;
                self.refresh_metrics().await;
                Ok(Ok(r))
            }
        }
    }

    async fn prune_resolved(&self) -> Result<()> {
        let mut resolved: Vec<(i64, IncidentId)> = self
            .list()
            .await?
            .into_iter()
            .filter_map(|r| match r.status {
                IncidentStatus::Resolved { at_ms, .. }
                    if r.pending_audit.is_none() =>
                {
                    Some((at_ms, r.incident_id))
                }
                _ => None,
            })
            .collect();
        if resolved.len() <= MAX_RESOLVED {
            return Ok(());
        }
        resolved.sort_by(|a, b| (a.0, &a.1.0).cmp(&(b.0, &b.1.0)));
        let excess = resolved.len() - MAX_RESOLVED;
        for (_, id) in resolved.into_iter().take(excess) {
            self.backend
                .slot_delete(INCIDENTS_NS, &self.key(&id))
                .await?;
        }
        Ok(())
    }

    /// The audit trail of this pipeline, oldest first, at most `limit`
    /// entries.
    pub async fn audit_trail(&self, limit: usize) -> Result<Vec<AuditEntry>> {
        let entries = self
            .backend
            .log_since(AUDIT_NS, &segment(&self.pipeline), 0)
            .await?;
        entries
            .into_iter()
            .take(limit)
            .map(|(_, bytes)| {
                serde_json::from_slice(&bytes).context("decode audit entry")
            })
            .collect()
    }

    /// Resolve every open incident of `reason` raised by `component` whose
    /// resolve scope is `scope` (any scope when `None`) with `check`: a
    /// scoped resolver, e.g. a table accepted at its first use, or a sink
    /// acknowledging a later batch. Returns how many were resolved.
    pub async fn resolve_matching(
        &self,
        reason: ReasonCode,
        component: &Component,
        scope: Option<&str>,
        check: &str,
    ) -> Result<usize> {
        let mut n = 0;
        for rec in self.list().await? {
            if rec.status.is_resolved()
                || rec.reason_code != reason
                || &rec.component != component
                || scope
                    .is_some_and(|s| rec.resolve_scope.as_deref() != Some(s))
            {
                continue;
            }
            if self
                .resolve(
                    &rec.incident_id,
                    Resolution::VerifiedRecovery {
                        check: check.to_string(),
                    },
                )
                .await?
                .is_ok()
            {
                n += 1;
            }
        }
        Ok(n)
    }

    // ---- control record / recovery epoch -----------------------------------

    async fn control(&self) -> Result<(u64, IncidentControl)> {
        let key = segment(&self.pipeline);
        loop {
            if let Some((v, bytes)) =
                self.backend.slot_get(CONTROL_NS, &key).await?
            {
                return Ok((
                    v,
                    serde_json::from_slice(&bytes)
                        .context("decode incident control record")?,
                ));
            }
            let fresh = IncidentControl {
                format: CONTROL_FORMAT,
                ..Default::default()
            };
            self.backend
                .slot_create(CONTROL_NS, &key, &serde_json::to_vec(&fresh)?)
                .await?;
        }
    }

    /// Complete an interrupted recovery: resolve what it named, then clear it.
    async fn finish_recovery(&self) -> Result<u64> {
        loop {
            let (version, ctl) = self.control().await?;
            let Some(pending) = &ctl.pending_recovery else {
                return Ok(ctl.recovery_epoch);
            };
            for (id, check) in &pending.resolves {
                self.resolve(
                    id,
                    Resolution::VerifiedRecovery {
                        check: check.clone(),
                    },
                )
                .await?
                .ok();
            }
            let mut cleared = ctl.clone();
            cleared.pending_recovery = None;
            self.backend
                .slot_cas(
                    CONTROL_NS,
                    &segment(&self.pipeline),
                    version,
                    &serde_json::to_vec(&cleared)?,
                )
                .await?;
        }
    }

    /// The current recovery epoch (what [`bind_epoch`] binds identities to).
    pub async fn recovery_epoch(&self) -> Result<u64> {
        Ok(self.control().await?.1.recovery_epoch)
    }

    /// Repair pending audit entries and an interrupted recovery; returns the
    /// current recovery epoch. Run before a pipeline's tasks start.
    pub async fn prepare(&self) -> Result<u64> {
        self.repair_all().await?;
        self.finish_recovery().await
    }

    /// Verified recovery: resolve every open incident a verified start
    /// settles ([`settled_by_verified_start`]) and advance the recovery epoch,
    /// as one durable step (the epoch and the incidents it resolves, with
    /// their checks, are written together; the resolutions are completed from
    /// that record, also after a crash). Returns the new epoch, or `None` when
    /// there was nothing to resolve.
    pub async fn recover(&self) -> Result<Option<u64>> {
        loop {
            self.finish_recovery().await?;
            // The control version is read before the incidents: every
            // recovery changes it, so a recovery completed after this
            // listing fails the CAS below and the incidents are listed
            // again (never resolved twice from a stale listing).
            let (version, ctl) = self.control().await?;
            if ctl.pending_recovery.is_some() {
                continue;
            }
            let resolves: Vec<(IncidentId, String)> = self
                .list()
                .await?
                .into_iter()
                .filter(|r| !r.status.is_resolved())
                .filter_map(|r| {
                    settled_by_verified_start(&r)
                        .map(|check| (r.incident_id.clone(), check.to_string()))
                })
                .collect();
            if resolves.is_empty() {
                return Ok(None);
            }
            let to_epoch = ctl.recovery_epoch + 1;
            let next = IncidentControl {
                format: CONTROL_FORMAT,
                recovery_epoch: to_epoch,
                pending_recovery: Some(PendingRecovery { to_epoch, resolves }),
            };
            if self
                .backend
                .slot_cas(
                    CONTROL_NS,
                    &segment(&self.pipeline),
                    version,
                    &serde_json::to_vec(&next)?,
                )
                .await?
            {
                self.finish_recovery().await?;
                return Ok(Some(to_epoch));
            }
        }
    }
}

/// How often repeated occurrences of one incident are written durably.
pub const OCCURRENCE_FLUSH: Duration = Duration::from_secs(30);

/// Raises drafts for conditions that recur while the pipeline keeps running
/// (non-blocking): the first sighting of an identity in this process is
/// written at once; repeats are counted in memory and written at most every
/// [`OCCURRENCE_FLUSH`], so a retry loop updates one record without
/// amplifying writes.
pub struct IncidentRecorder {
    store: IncidentStore,
    seen:
        Mutex<std::collections::HashMap<IncidentId, (std::time::Instant, u64)>>,
}

impl IncidentRecorder {
    pub fn new(store: IncidentStore) -> Self {
        Self {
            store,
            seen: std::sync::Mutex::new(Default::default()),
        }
    }

    pub fn store(&self) -> &IncidentStore {
        &self.store
    }

    /// Record one occurrence of `draft`. Returns its identity.
    pub async fn record(&self, draft: &IncidentDraft) -> Result<IncidentId> {
        let id = draft.incident_id(self.store.pipeline());
        let flush = {
            let mut seen = self.seen.lock().expect("recorder lock");
            match seen.get_mut(&id) {
                None => {
                    seen.insert(id.clone(), (std::time::Instant::now(), 0));
                    Some(1)
                }
                Some((last, pending)) => {
                    *pending += 1;
                    if last.elapsed() >= OCCURRENCE_FLUSH {
                        *last = std::time::Instant::now();
                        Some(std::mem::take(pending))
                    } else {
                        None
                    }
                }
            }
        };
        if let Some(n) = flush {
            self.store.raise(draft, n).await?;
        }
        Ok(id)
    }
}

/// Unresolved incidents by (reason, safety state): the open-incident gauge.
/// Acknowledged incidents are still open.
pub fn open_counts(
    records: &[IncidentRecord],
) -> BTreeMap<(ReasonCode, SafetyState), u64> {
    let mut counts = BTreeMap::new();
    for reason in ReasonCode::ALL {
        for safety in SafetyState::ALL {
            counts.insert((reason, safety), 0);
        }
    }
    for r in records.iter().filter(|r| !r.status.is_resolved()) {
        *counts.entry((r.reason_code, r.safety_state)).or_default() += 1;
    }
    counts
}

impl IncidentStore {
    /// Set `deltaforge_incidents_open{pipeline, reason_code, safety_state}`
    /// for every label combination (closed enums only).
    async fn refresh_metrics(&self) {
        let Ok(records) = self.list().await else {
            return;
        };
        for ((reason, safety), n) in open_counts(&records) {
            gauge!(
                "deltaforge_incidents_open",
                "pipeline" => self.pipeline.clone(),
                "reason_code" => reason.as_str(),
                "safety_state" => safety.as_str(),
            )
            .set(n as f64);
        }
    }

    fn count_raised(&self, reason: ReasonCode) {
        counter!(
            "deltaforge_incidents_raised_total",
            "pipeline" => self.pipeline.clone(),
            "reason_code" => reason.as_str(),
        )
        .increment(1);
        if reason == ReasonCode::UnclassifiedFailure {
            counter!(
                "deltaforge_incidents_unclassified_total",
                "pipeline" => self.pipeline.clone(),
            )
            .increment(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryStorageBackend;
    use crate::adapters::test_util::FaultBackend;
    use deltaforge_core::incident::EvidenceKey;
    use std::sync::Arc;

    fn backend() -> ArcStorageBackend {
        Arc::new(MemoryStorageBackend::new())
    }

    fn draft(n: u32) -> IncidentDraft {
        IncidentDraft::new(
            ReasonCode::PgDifferentCluster,
            Component::Source { id: "pg".into() },
            Retryability::OperatorAction,
            SafetyState::HaltedSafe,
            CauseCode::SourceLineage,
        )
        .discriminate("transition", n.to_string())
        .with_evidence(|e| {
            e.text(EvidenceKey::Slot, "s");
        })
    }

    fn degraded(n: u32) -> IncidentDraft {
        let mut d = draft(n);
        d.safety_state = SafetyState::RunningDegraded;
        d
    }

    fn kinds(trail: &[AuditEntry]) -> Vec<&'static str> {
        trail
            .iter()
            .map(|e| match e.transition {
                Transition::Opened => "opened",
                Transition::Reopened => "reopened",
                Transition::Acknowledged { .. } => "acknowledged",
                Transition::Resolved { .. } => "resolved",
                Transition::Displaced => "displaced",
                Transition::Reclassified { .. } => "reclassified",
            })
            .collect()
    }

    fn recovered() -> Resolution {
        Resolution::VerifiedRecovery {
            check: PIPELINE_RECOVERED.into(),
        }
    }

    async fn id_of(store: &IncidentStore, d: &IncidentDraft) -> IncidentId {
        store
            .raise(d, 1)
            .await
            .unwrap()
            .record()
            .incident_id
            .clone()
    }

    #[tokio::test]
    async fn a_repeated_condition_is_one_record() {
        let store = IncidentStore::new(backend(), "p");
        let a = store.raise(&draft(1), 1).await.unwrap();
        let b = store.raise(&draft(1), 3).await.unwrap();
        assert_eq!(a.record().incident_id, b.record().incident_id);
        assert_eq!(b.record().occurrences, 4);
        assert_eq!(b.record().transition_seq, 1, "repeats are not transitions");
        assert_eq!(store.list().await.unwrap().len(), 1);
        store.raise(&draft(2), 1).await.unwrap();
        assert_eq!(store.list().await.unwrap().len(), 2);
        assert_eq!(store.audit_trail(100).await.unwrap().len(), 2);
    }

    /// A condition whose classification changes (an automatic retry that
    /// exhausted its budget) stays one incident: the later raise reclassifies
    /// it, as an audited transition; a repeat with the same classification
    /// is not a transition.
    #[tokio::test]
    async fn a_changed_classification_reclassifies_the_same_incident() {
        let store = IncidentStore::new(backend(), "p");
        let mut retrying = draft(1);
        retrying.retryability = Retryability::AutoRetry;
        retrying.actions = vec![ActionCode::InspectLogs];
        let a = store.raise(&retrying, 1).await.unwrap();
        store.raise(&retrying, 1).await.unwrap();
        let mut exhausted = draft(1);
        exhausted.actions = vec![ActionCode::VerifyEndpoint];
        let b = store.raise(&exhausted, 1).await.unwrap();
        assert_eq!(a.record().incident_id, b.record().incident_id);
        let rec = store.get(&a.record().incident_id).await.unwrap().unwrap();
        assert_eq!(rec.retryability, Retryability::OperatorAction);
        assert_eq!(rec.actions, [ActionCode::VerifyEndpoint]);
        assert_eq!(rec.occurrences, 3);
        assert_eq!(store.list().await.unwrap().len(), 1);
        let trail = store.audit_trail(100).await.unwrap();
        assert_eq!(kinds(&trail), ["opened", "reclassified"]);
        assert_eq!(
            trail[1].transition,
            Transition::Reclassified {
                retryability: Retryability::OperatorAction,
                safety_state: SafetyState::HaltedSafe,
            }
        );
    }

    /// A resolved incident raised again reopens with the draft's current
    /// classification, not the one it was resolved with.
    #[tokio::test]
    async fn a_reopened_incident_takes_the_new_classification() {
        let store = IncidentStore::new(backend(), "p");
        let mut retrying = draft(1);
        retrying.retryability = Retryability::AutoRetry;
        retrying.actions = vec![ActionCode::InspectLogs];
        let id = id_of(&store, &retrying).await;
        store.resolve(&id, recovered()).await.unwrap().unwrap();
        let mut stopped = draft(1);
        stopped.actions = vec![ActionCode::VerifyEndpoint];
        store.raise(&stopped, 1).await.unwrap();
        let rec = store.get(&id).await.unwrap().unwrap();
        assert_eq!(rec.status, IncidentStatus::Open);
        assert_eq!(rec.retryability, Retryability::OperatorAction);
        assert_eq!(rec.actions, [ActionCode::VerifyEndpoint]);
    }

    /// An automatic retry stopped on purpose is withdrawn as
    /// `operation_cancelled` (no longer blocking); one that already needs an
    /// operator is never withdrawn.
    #[tokio::test]
    async fn a_cancelled_auto_retry_is_withdrawn_but_operator_action_is_not() {
        let store = IncidentStore::new(backend(), "p");
        let mut retrying = draft(1);
        retrying.retryability = Retryability::AutoRetry;
        let id = id_of(&store, &retrying).await;
        assert!(store.cancel_auto_retry(&id).await.unwrap());
        let rec = store.get(&id).await.unwrap().unwrap();
        assert!(matches!(
            rec.status,
            IncidentStatus::Resolved {
                by: Resolution::OperationCancelled,
                ..
            }
        ));
        assert!(!rec.is_blocking());

        let operator = id_of(&store, &draft(2)).await;
        assert!(!store.cancel_auto_retry(&operator).await.unwrap());
        assert!(store.get(&operator).await.unwrap().unwrap().is_blocking());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_raises_create_one_record() {
        // Reads yield, so the 16 read-modify-write cycles interleave and
        // their CAS writes conflict.
        let fault = FaultBackend::new();
        fault
            .yield_after_slot_reads
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let b: ArcStorageBackend = Arc::new(fault);
        let mut tasks = Vec::new();
        for _ in 0..16 {
            let store = IncidentStore::new(b.clone(), "p");
            tasks.push(tokio::spawn(async move {
                store.raise(&draft(1), 1).await.unwrap()
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        let store = IncidentStore::new(b, "p");
        let all = store.list().await.unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].occurrences, 16, "no occurrence lost to a race");
        assert_eq!(kinds(&store.audit_trail(100).await.unwrap()), ["opened"]);
    }

    #[tokio::test]
    async fn acknowledgement_never_resolves() {
        let store = IncidentStore::new(backend(), "p");
        let id = id_of(&store, &draft(1)).await;
        let rec = store
            .acknowledge(&id, "alice", Some("10.0.0.1"), "looking")
            .await
            .unwrap()
            .unwrap();
        assert!(rec.is_blocking(), "an acknowledged incident still blocks");
        match &rec.status {
            IncidentStatus::Acknowledged {
                asserted_actor,
                actor_verified,
                ..
            } => {
                assert_eq!(asserted_actor, "alice");
                assert!(!actor_verified, "a caller-supplied actor");
            }
            other => panic!("{other:?}"),
        }
        let rec = store.resolve(&id, recovered()).await.unwrap().unwrap();
        assert!(!rec.is_blocking());
        assert_eq!(
            store.acknowledge(&id, "bob", None, "late").await.unwrap(),
            Err(LifecycleError::AlreadyResolved(id.0.clone()))
        );
        let trail = store.audit_trail(100).await.unwrap();
        assert_eq!(kinds(&trail), ["opened", "acknowledged", "resolved"]);
        let seqs: Vec<u64> = trail.iter().map(|e| e.transition_seq).collect();
        assert_eq!(seqs, [1, 2, 3]);
    }

    #[tokio::test]
    async fn acknowledgement_text_is_bounded() {
        let store = IncidentStore::new(backend(), "p");
        let id = id_of(&store, &draft(1)).await;
        let long = "x".repeat(10 * MAX_ACK_TEXT);
        let rec = store
            .acknowledge(&id, &long, Some(&long), &long)
            .await
            .unwrap()
            .unwrap();
        let json = serde_json::to_string(&rec).unwrap();
        assert!(json.len() < 4 * MAX_ACK_TEXT + 2048, "{}", json.len());
    }

    #[tokio::test]
    async fn a_resolved_identity_seen_again_reopens() {
        let store = IncidentStore::new(backend(), "p");
        let id = id_of(&store, &draft(1)).await;
        store.resolve(&id, recovered()).await.unwrap().unwrap();
        let rec = store.raise(&draft(1), 1).await.unwrap().record().clone();
        assert_eq!(rec.status, IncidentStatus::Open);
        assert_eq!(
            kinds(&store.audit_trail(100).await.unwrap()),
            ["opened", "resolved", "reopened"]
        );
    }

    // ---- audit crash repair ------------------------------------------------

    fn fault() -> (Arc<FaultBackend>, IncidentStore) {
        let f = Arc::new(FaultBackend::new());
        let b: ArcStorageBackend = f.clone();
        (f, IncidentStore::new(b, "p"))
    }

    fn crash_at(f: &FaultBackend, ns: &str, after: u64) {
        *f.fail_after_writes_to.lock().unwrap() = Some((ns.to_string(), after));
    }

    async fn assert_audit_exact(store: &IncidentStore, want: &[&str]) {
        store.repair_all().await.unwrap();
        let trail = store.audit_trail(100).await.unwrap();
        assert_eq!(kinds(&trail), want);
        let seqs: Vec<u64> = trail.iter().map(|e| e.transition_seq).collect();
        assert_eq!(seqs, (1..=want.len() as u64).collect::<Vec<_>>());
        for rec in store.list().await.unwrap() {
            assert!(rec.pending_audit.is_none(), "repair cleared it");
        }
    }

    #[tokio::test]
    async fn a_crash_before_the_incident_cas_changes_nothing() {
        let (f, store) = fault();
        let id = id_of(&store, &draft(1)).await;
        // The acknowledgement's CAS fails: no transition, no audit entry.
        crash_at(&f, INCIDENTS_NS, 0);
        assert!(store.acknowledge(&id, "a", None, "r").await.is_err());
        let rec = store.get(&id).await.unwrap().unwrap();
        assert_eq!(rec.status, IncidentStatus::Open);
        assert_eq!(rec.transition_seq, 1);
        assert_audit_exact(&store, &["opened"]).await;
        // Retried, it applies once.
        store
            .acknowledge(&id, "a", None, "r")
            .await
            .unwrap()
            .unwrap();
        assert_audit_exact(&store, &["opened", "acknowledged"]).await;
    }

    #[tokio::test]
    async fn a_crash_after_the_cas_before_the_audit_append_is_repaired() {
        let (f, store) = fault();
        let id = id_of(&store, &draft(1)).await;
        crash_at(&f, AUDIT_NS, 0);
        assert!(store.acknowledge(&id, "a", None, "r").await.is_err());
        let rec = store.get(&id).await.unwrap().unwrap();
        assert!(matches!(rec.status, IncidentStatus::Acknowledged { .. }));
        assert_eq!(rec.transition_seq, 2);
        assert!(rec.pending_audit.is_some(), "the entry waits in the record");
        assert_audit_exact(&store, &["opened", "acknowledged"]).await;
        assert_eq!(store.get(&id).await.unwrap().unwrap().transition_seq, 2);
    }

    #[tokio::test]
    async fn a_crash_after_the_append_before_clearing_is_repaired_once() {
        let (f, store) = fault();
        let id = id_of(&store, &draft(1)).await;
        // Writes of the acknowledgement: CAS (1st), append, clearing CAS (2nd).
        crash_at(&f, INCIDENTS_NS, 1);
        assert!(store.acknowledge(&id, "a", None, "r").await.is_err());
        assert!(
            store
                .get(&id)
                .await
                .unwrap()
                .unwrap()
                .pending_audit
                .is_some()
        );
        assert_eq!(store.audit_trail(100).await.unwrap().len(), 2);
        assert_audit_exact(&store, &["opened", "acknowledged"]).await;
    }

    #[tokio::test]
    async fn a_later_transition_repairs_the_earlier_audit_first() {
        let (f, store) = fault();
        let id = id_of(&store, &draft(1)).await;
        crash_at(&f, AUDIT_NS, 0);
        assert!(store.acknowledge(&id, "a", None, "r").await.is_err());
        // The resolution repairs the acknowledgement's entry, then applies.
        store.resolve(&id, recovered()).await.unwrap().unwrap();
        let trail = store.audit_trail(100).await.unwrap();
        assert_eq!(kinds(&trail), ["opened", "acknowledged", "resolved"]);
        assert_audit_exact(&store, &["opened", "acknowledged", "resolved"])
            .await;
    }

    #[tokio::test]
    async fn an_interrupted_opening_is_repaired() {
        let (f, store) = fault();
        crash_at(&f, AUDIT_NS, 0);
        assert!(store.raise(&draft(1), 1).await.is_err());
        assert_eq!(store.list().await.unwrap().len(), 1, "the record exists");
        assert_audit_exact(&store, &["opened"]).await;
    }

    // ---- bounds and overflow -----------------------------------------------

    #[tokio::test]
    async fn the_overflow_incident_is_never_limited() {
        let store = IncidentStore::new(backend(), "p");
        for n in 0..MAX_ORDINARY_OPEN as u32 {
            assert!(matches!(
                store.raise(&draft(n), 1).await.unwrap(),
                Raised::Recorded(_)
            ));
        }
        let Raised::Overflowed(ov) =
            store.raise(&draft(9999), 1).await.unwrap()
        else {
            panic!("expected overflow");
        };
        assert_eq!(ov.reason_code, ReasonCode::IncidentOverflow);
        let summary = ov.overflow.as_ref().unwrap();
        assert_eq!(summary.by_reason[&ReasonCode::PgDifferentCluster], 1);
        assert_eq!(summary.blocking, 1);
        assert!(ov.is_blocking(), "it carries blocking incidents");
        store.raise(&draft(10000), 1).await.unwrap();
        let ov = store.get(&ov.incident_id).await.unwrap().unwrap();
        assert_eq!(ov.overflow.unwrap().blocking, 2);
        // 63 ordinary + 1 overflow; an existing one still counts repeats.
        assert_eq!(store.list().await.unwrap().len(), MAX_ORDINARY_OPEN + 1);
        assert!(matches!(
            store.raise(&draft(0), 1).await.unwrap(),
            Raised::Recorded(_)
        ));
        // The overflow record never takes an ordinary place: once one is
        // resolved, a new incident is kept individually again.
        let freed = draft(1).incident_id("p");
        store.resolve(&freed, recovered()).await.unwrap().unwrap();
        assert!(matches!(
            store.raise(&draft(20000), 1).await.unwrap(),
            Raised::Recorded(_)
        ));
    }

    #[tokio::test]
    async fn a_blocking_incident_displaces_an_open_non_blocking_one() {
        let store = IncidentStore::new(backend(), "p");
        let victim = id_of(&store, &degraded(0)).await;
        for n in 1..MAX_ORDINARY_OPEN as u32 {
            store.raise(&draft(n), 1).await.unwrap();
        }
        let r = store.raise(&draft(5000), 1).await.unwrap();
        assert!(matches!(r, Raised::Recorded(_)), "kept individually");
        assert!(store.get(&victim).await.unwrap().is_none(), "displaced");
        let ov = store
            .list()
            .await
            .unwrap()
            .into_iter()
            .find(|r| r.reason_code == ReasonCode::IncidentOverflow)
            .expect("counted on the overflow");
        let summary = ov.overflow.unwrap();
        assert_eq!(summary.blocking, 0);
        assert_eq!(summary.total(), 1);
        assert!(
            store
                .audit_trail(1000)
                .await
                .unwrap()
                .iter()
                .any(|e| e.incident_id == victim
                    && e.transition == Transition::Displaced)
        );
    }

    #[tokio::test]
    async fn acknowledged_or_blocking_incidents_are_never_displaced() {
        let store = IncidentStore::new(backend(), "p");
        let acked = id_of(&store, &degraded(0)).await;
        store
            .acknowledge(&acked, "a", None, "seen")
            .await
            .unwrap()
            .unwrap();
        for n in 1..MAX_ORDINARY_OPEN as u32 {
            store.raise(&draft(n), 1).await.unwrap();
        }
        let r = store.raise(&draft(5000), 1).await.unwrap();
        assert!(matches!(r, Raised::Overflowed(_)));
        assert!(store.get(&acked).await.unwrap().is_some());
        // A non-blocking incident at the limit never displaces anything.
        let r = store.raise(&degraded(6000), 1).await.unwrap();
        assert!(matches!(r, Raised::Overflowed(_)));
    }

    #[tokio::test]
    async fn resolved_incidents_are_pruned_oldest_first() {
        let store = IncidentStore::new(backend(), "p");
        let mut ids = Vec::new();
        for n in 0..(MAX_RESOLVED as u32 + 5) {
            let id = id_of(&store, &draft(n)).await;
            store.resolve(&id, recovered()).await.unwrap().unwrap();
            ids.push(id);
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        assert_eq!(store.list().await.unwrap().len(), MAX_RESOLVED);
        assert!(store.get(&ids[0]).await.unwrap().is_none(), "oldest pruned");
        assert!(store.get(ids.last().unwrap()).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn pipelines_never_share_records() {
        let b = backend();
        let a = IncidentStore::new(b.clone(), "a");
        let ab = IncidentStore::new(b.clone(), "a/b");
        a.raise(&draft(1), 1).await.unwrap();
        ab.raise(&draft(1), 1).await.unwrap();
        assert_eq!(a.list().await.unwrap().len(), 1);
        assert_eq!(ab.list().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn repeats_are_throttled_but_counted() {
        let recorder =
            IncidentRecorder::new(IncidentStore::new(backend(), "p"));
        let id = recorder.record(&degraded(1)).await.unwrap();
        for _ in 0..50 {
            recorder.record(&degraded(1)).await.unwrap();
        }
        let rec = recorder.store().get(&id).await.unwrap().unwrap();
        assert_eq!(rec.occurrences, 1, "within the interval only the first");
        recorder.seen.lock().unwrap().get_mut(&id).unwrap().0 =
            std::time::Instant::now() - OCCURRENCE_FLUSH;
        recorder.record(&degraded(1)).await.unwrap();
        let rec = recorder.store().get(&id).await.unwrap().unwrap();
        assert_eq!(rec.occurrences, 52);
    }

    #[tokio::test]
    async fn open_counts_cover_every_label_and_count_acknowledged_as_open() {
        let store = IncidentStore::new(backend(), "p");
        let a = id_of(&store, &draft(1)).await;
        let b = id_of(&store, &draft(2)).await;
        let c = id_of(&store, &unc(0)).await;
        store
            .acknowledge(&a, "x", None, "y")
            .await
            .unwrap()
            .unwrap();
        store.resolve(&b, recovered()).await.unwrap().unwrap();
        let counts = open_counts(&store.list().await.unwrap());
        // Every (reason, safety) label pair is present: bounded and complete.
        assert_eq!(
            counts.len(),
            ReasonCode::ALL.len() * SafetyState::ALL.len()
        );
        assert_eq!(
            counts[&(ReasonCode::PgDifferentCluster, SafetyState::HaltedSafe)],
            1,
            "acknowledged counts as open, resolved does not"
        );
        assert_eq!(
            counts[&(
                ReasonCode::UnclassifiedFailure,
                SafetyState::HaltedUncertain
            )],
            1
        );
        let _ = c;
    }

    // ---- recovery epoch ----------------------------------------------------

    /// An `unclassified_failure` draft bound to `epoch` (as the runner's
    /// classifier builds it).
    fn unc(epoch: u64) -> IncidentDraft {
        IncidentDraft::new(
            ReasonCode::UnclassifiedFailure,
            Component::Source { id: "s".into() },
            Retryability::AutoRetry,
            SafetyState::HaltedUncertain,
            CauseCode::SourceConnect,
        )
        .discriminate("cause", "source_connect")
        .discriminate("recovery_epoch", epoch.to_string())
    }

    #[tokio::test]
    async fn recovery_resolves_unclassified_and_starts_a_new_generation() {
        let store = IncidentStore::new(backend(), "p");
        assert_eq!(store.prepare().await.unwrap(), 0);
        let first = id_of(&store, &unc(0)).await;
        let mapped = id_of(
            &store,
            &mapped(
                ReasonCode::SchemaDriftBlocked,
                Component::Source { id: "pg".into() },
            ),
        )
        .await;
        // Before recovery the same failure is the same occurrence.
        assert_eq!(id_of(&store, &unc(0)).await, first);
        assert_eq!(store.recover().await.unwrap(), Some(1));
        match store.get(&first).await.unwrap().unwrap().status {
            IncidentStatus::Resolved { by, .. } => assert_eq!(by, recovered()),
            other => panic!("{other:?}"),
        }
        assert!(
            !store
                .get(&mapped)
                .await
                .unwrap()
                .unwrap()
                .status
                .is_resolved(),
            "a drifted table is not settled by a verified start"
        );
        // After recovery the same failure is a new occurrence.
        let second = id_of(&store, &unc(1)).await;
        assert_ne!(second, first);
        // Nothing to resolve: no new generation.
        store.resolve(&second, recovered()).await.unwrap().unwrap();
        assert_eq!(store.recover().await.unwrap(), None);
        assert_eq!(store.prepare().await.unwrap(), 1);
    }

    fn mapped(reason: ReasonCode, component: Component) -> IncidentDraft {
        IncidentDraft::new(
            reason,
            component,
            Retryability::OperatorAction,
            SafetyState::HaltedSafe,
            CauseCode::SourceCheckpoint,
        )
        .discriminate("recovery_epoch", "0")
    }

    fn resolved_by(rec: &IncidentRecord) -> Option<String> {
        match &rec.status {
            IncidentStatus::Resolved {
                by: Resolution::VerifiedRecovery { check },
                ..
            } => Some(check.clone()),
            _ => None,
        }
    }

    #[tokio::test]
    async fn a_verified_start_settles_the_sources_identity_and_position() {
        let store = IncidentStore::new(backend(), "p");
        let src = Component::Source { id: "pg".into() };
        let cluster =
            id_of(&store, &mapped(ReasonCode::PgDifferentCluster, src.clone()))
                .await;
        let position = id_of(
            &store,
            &mapped(ReasonCode::MysqlGtidPositionUnavailable, src.clone()),
        )
        .await;
        let drift =
            id_of(&store, &mapped(ReasonCode::SchemaDriftBlocked, src.clone()))
                .await;
        let sink = id_of(
            &store,
            &mapped(
                ReasonCode::SinkAckUncertain,
                Component::Sink { id: "k".into() },
            ),
        )
        .await;
        assert_eq!(store.recover().await.unwrap(), Some(1));
        let get = |id| {
            let store = store.clone();
            async move { store.get(&id).await.unwrap().unwrap() }
        };
        assert_eq!(
            resolved_by(&get(cluster).await).as_deref(),
            Some(LINEAGE_VERIFIED)
        );
        assert_eq!(
            resolved_by(&get(position).await).as_deref(),
            Some(POSITION_VERIFIED)
        );
        // Not settled by a verified start: a drifted table is accepted only at
        // its own first use; a sink only by acknowledging a later batch.
        assert!(!get(drift).await.status.is_resolved());
        assert!(!get(sink).await.status.is_resolved());
    }

    #[tokio::test]
    async fn a_scoped_resolver_resolves_only_its_scope() {
        let store = IncidentStore::new(backend(), "p");
        let src = Component::Source { id: "pg".into() };
        let orders = id_of(
            &store,
            &mapped(ReasonCode::SchemaDriftBlocked, src.clone())
                .discriminate("table", "orders")
                .resolved_by_scope("public.orders"),
        )
        .await;
        let items = id_of(
            &store,
            &mapped(ReasonCode::SchemaDriftBlocked, src.clone())
                .discriminate("table", "items")
                .resolved_by_scope("public.items"),
        )
        .await;
        let key = deltaforge_core::incident::scope_key(&src, "public.orders");
        let n = store
            .resolve_matching(
                ReasonCode::SchemaDriftBlocked,
                &src,
                Some(&key),
                SCHEMA_ACCEPTED,
            )
            .await
            .unwrap();
        assert_eq!(n, 1);
        assert_eq!(
            resolved_by(&store.get(&orders).await.unwrap().unwrap()).as_deref(),
            Some(SCHEMA_ACCEPTED)
        );
        assert!(
            !store
                .get(&items)
                .await
                .unwrap()
                .unwrap()
                .status
                .is_resolved()
        );
        // Another component's scope never matches.
        let other = Component::Source { id: "other".into() };
        let key = deltaforge_core::incident::scope_key(&other, "public.items");
        assert_eq!(
            store
                .resolve_matching(
                    ReasonCode::SchemaDriftBlocked,
                    &src,
                    Some(&key),
                    SCHEMA_ACCEPTED
                )
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn a_crash_inside_recovery_keeps_one_generation_and_the_incident() {
        let (f, store) = fault();
        let first = id_of(&store, &unc(0)).await;
        // The epoch CAS succeeds, the resolution's CAS crashes.
        crash_at(&f, INCIDENTS_NS, 0);
        assert!(store.recover().await.is_err());
        // The incident is still open (not lost), the epoch advanced once.
        assert!(
            !store
                .get(&first)
                .await
                .unwrap()
                .unwrap()
                .status
                .is_resolved()
        );
        // The next start completes the named resolution.
        assert_eq!(store.prepare().await.unwrap(), 1);
        assert!(
            store
                .get(&first)
                .await
                .unwrap()
                .unwrap()
                .status
                .is_resolved()
        );
        assert_eq!(store.prepare().await.unwrap(), 1, "never advanced twice");
        assert_audit_exact(&store, &["opened", "resolved"]).await;
    }

    /// A recovery that listed the open incidents before another recovery
    /// resolved them must not advance the epoch again from that stale list.
    #[tokio::test]
    async fn a_recovery_with_a_stale_listing_does_not_advance_the_epoch_again()
    {
        let fault = Arc::new(FaultBackend::new());
        let b: ArcStorageBackend = fault.clone();
        IncidentStore::new(b.clone(), "p")
            .raise(&unc(0), 1)
            .await
            .unwrap();
        let reached = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        *fault.pause_after_slot_list.lock().unwrap() =
            Some((INCIDENTS_NS.to_string(), reached.clone(), release.clone()));
        // B lists the incident as open, then stops there...
        let late = IncidentStore::new(b.clone(), "p");
        let late = tokio::spawn(async move { late.recover().await.unwrap() });
        reached.notified().await;
        // ...while A recovers completely.
        let first = IncidentStore::new(b.clone(), "p");
        assert_eq!(first.recover().await.unwrap(), Some(1));
        release.notify_one();
        assert_eq!(late.await.unwrap(), None, "nothing left to resolve");
        assert_eq!(IncidentStore::new(b, "p").prepare().await.unwrap(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_recoveries_advance_the_epoch_once() {
        let fault = FaultBackend::new();
        fault
            .yield_after_slot_reads
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let b: ArcStorageBackend = Arc::new(fault);
        IncidentStore::new(b.clone(), "p")
            .raise(&unc(0), 1)
            .await
            .unwrap();
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let store = IncidentStore::new(b.clone(), "p");
            tasks.push(tokio::spawn(
                async move { store.recover().await.unwrap() },
            ));
        }
        let mut advanced = 0;
        for t in tasks {
            if t.await.unwrap().is_some() {
                advanced += 1;
            }
        }
        assert_eq!(advanced, 1);
        assert_eq!(IncidentStore::new(b, "p").prepare().await.unwrap(), 1);
    }

    // ---- classification ----------------------------------------------------
}
