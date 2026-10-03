//! Durable operator incidents and pipeline failure supervision.
//!
//! - [`IncidentStore`]: one versioned record per incident (slot namespace
//!   `incidents`, changed only with create/CAS) and an append-only audit log of
//!   lifecycle transitions (`incidents.audit`). The lifecycle is
//!   Open -> Acknowledged -> Resolved: acknowledgement means an operator has
//!   seen it and never clears it; only a verified resolver (or a recovery
//!   operation) resolves. Each lifecycle transition advances the record's
//!   semantic `transition_seq` and is written with its complete audit entry as
//!   `pending_audit` in the same CAS; the entry is then appended idempotently
//!   (`log_append_if_absent`, capture id = incident id + sequence) and cleared.
//!   A crash at any point is repaired by replaying the pending entry, which
//!   never creates another transition; every later transition repairs first.
//! - Bounds: at most [`MAX_ORDINARY_OPEN`] ordinary open incidents per pipeline
//!   plus one fixed overflow incident that counts the rest by reason code.
//!   Blocking incidents take priority: one may displace the oldest open,
//!   unacknowledged non-blocking incident (counted in the overflow). Blocking
//!   or acknowledged incidents are never evicted. At most [`MAX_RESOLVED`]
//!   resolved records are kept (the audit log keeps the history).
//! - Recovery epoch: a per-pipeline control slot (`incidents.control`) holds
//!   the epoch that `unclassified_failure` identities are bound to. Verified
//!   recovery advances it with one CAS that also names the incidents it
//!   resolves; the resolutions are completed (and repaired after a crash) from
//!   that record, so there is never more than one active generation and no
//!   current incident is lost.
//! - [`PipelineHealth`]: supervises the source and coordinator exits. The
//!   pipeline turns Failed synchronously at the first failure; every distinct
//!   failure is recorded (with persistence retried while the store is down);
//!   the primary incident is chosen deterministically once both exits are known
//!   or the grace period passes.
//!
//! Nothing here stores or serves an error's Display/Debug text.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use deltaforge_core::incident::{
    ActionCode, CauseCode, Component, EvidenceKey, IncidentDraft, IncidentId,
    ReasonCode, Retryability, SafetyState,
};
use deltaforge_core::{SinkError, SourceError};
use metrics::{counter, gauge};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use storage::ArcStorageBackend;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::coordinator::{
    OversizedTxError, SinkDeliveryError, TxProtocolError,
};

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
    pub resolves: Vec<IncidentId>,
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
                    storage::SLOT_LIST_MAX_LIMIT,
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
        }
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
            for id in &pending.resolves {
                self.resolve(
                    id,
                    Resolution::VerifiedRecovery {
                        check: PIPELINE_RECOVERED.into(),
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

    /// Repair pending audit entries and an interrupted recovery; returns the
    /// current recovery epoch. Run before a pipeline's tasks start.
    pub async fn prepare(&self) -> Result<u64> {
        self.repair_all().await?;
        self.finish_recovery().await
    }

    /// Verified recovery: resolve every open `unclassified_failure` incident
    /// as `pipeline_recovered` and advance the recovery epoch, as one durable
    /// step (the epoch and the incidents it resolves are written together;
    /// the resolutions are completed from that record, also after a crash).
    /// Returns the new epoch, or `None` when there was nothing to resolve.
    pub async fn recover(&self) -> Result<Option<u64>> {
        loop {
            self.finish_recovery().await?;
            let resolves: Vec<IncidentId> = self
                .list()
                .await?
                .into_iter()
                .filter(|r| {
                    !r.status.is_resolved()
                        && r.reason_code == ReasonCode::UnclassifiedFailure
                })
                .map(|r| r.incident_id)
                .collect();
            if resolves.is_empty() {
                return Ok(None);
            }
            let (version, ctl) = self.control().await?;
            if ctl.pending_recovery.is_some() {
                continue;
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
            seen: Mutex::new(Default::default()),
        }
    }

    pub fn store(&self) -> &IncidentStore {
        &self.store
    }

    /// Record one occurrence of `draft`. Returns its identity.
    pub async fn record(&self, draft: &IncidentDraft) -> Result<IncidentId> {
        let id = draft.incident_id(self.store.pipeline());
        let flush = {
            let mut seen = self.seen.lock();
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

// ---------------------------------------------------------------------------
// Views and metrics
// ---------------------------------------------------------------------------

/// The API view of a durable record: codes, allow-listed evidence and the
/// generated explanation only.
pub fn record_view(rec: &IncidentRecord) -> serde_json::Value {
    serde_json::json!({
        "incident_id": rec.incident_id,
        "reason_code": rec.reason_code,
        "component": rec.component,
        "retryability": rec.retryability,
        "safety_state": rec.safety_state,
        "cause_code": rec.cause_code,
        "explanation": rec.explanation(),
        "evidence": rec.evidence,
        "recommended_actions": rec.actions,
        "status": rec.status,
        "blocking": rec.is_blocking(),
        "occurrences": rec.occurrences,
        "first_seen_ms": rec.first_seen_ms,
        "last_seen_ms": rec.last_seen_ms,
        "durable": true,
        // The latest transition's audit entry is not appended yet.
        "audit_pending": rec.pending_audit.is_some(),
        "transition_seq": rec.transition_seq,
        "overflow": rec.overflow,
    })
}

/// The API view of an incident known only in memory (its durable write is
/// still being retried): not durable, not auditable yet.
pub fn known_view(k: &KnownIncident) -> serde_json::Value {
    let d = &k.draft;
    serde_json::json!({
        "incident_id": k.id,
        "reason_code": d.reason_code,
        "component": d.component,
        "retryability": d.retryability,
        "safety_state": d.safety_state,
        "cause_code": d.cause_code,
        "explanation": d.explanation(),
        "evidence": d.evidence,
        "recommended_actions": d.actions,
        "status": IncidentStatus::Open,
        "blocking": d.safety_state.is_blocking(),
        "durable": k.durable,
        "audit_pending": !k.durable,
    })
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

// ---------------------------------------------------------------------------
// Classification of task exits
// ---------------------------------------------------------------------------

fn retryability_of(cause: CauseCode) -> Retryability {
    use CauseCode as C;
    match cause {
        C::SourceTimeout
        | C::SourceConnect
        | C::SourceIo
        | C::SourceBackpressure
        | C::SinkConnect
        | C::SinkIo
        | C::SinkBackpressure => Retryability::AutoRetry,
        _ => Retryability::OperatorAction,
    }
}

/// An `unclassified_failure` draft: the conservative fallback for a failure no
/// mapping classifies. Halted-uncertain, from the error's type only. Within
/// one recovery epoch the same component and cause are one occurrence.
pub fn unclassified(
    component: Component,
    cause: CauseCode,
    epoch: u64,
) -> IncidentDraft {
    IncidentDraft::new(
        ReasonCode::UnclassifiedFailure,
        component,
        retryability_of(cause),
        SafetyState::HaltedUncertain,
        cause,
    )
    .discriminate("cause", cause.as_str())
    .discriminate("recovery_epoch", epoch.to_string())
    .with_actions(&[ActionCode::InspectLogs])
}

/// The draft for a source task that ended on its own: its own draft when it
/// raised one, otherwise unclassified. `Ok` = ended without an error;
/// `panicked` = the task panicked.
pub fn classify_source_exit(
    source_id: &str,
    result: std::result::Result<(), &SourceError>,
    panicked: bool,
    epoch: u64,
) -> IncidentDraft {
    let component = Component::Source {
        id: source_id.to_string(),
    };
    match result {
        _ if panicked => {
            unclassified(component, CauseCode::SourcePanicked, epoch)
        }
        Ok(()) => unclassified(component, CauseCode::SourceEnded, epoch),
        Err(e) => match e.draft() {
            Some(d) => d.clone(),
            None => unclassified(component, e.cause_code(), epoch),
        },
    }
}

/// The draft for a coordinator that failed: a sink's own draft when the
/// failure came from one, otherwise unclassified from the error's type.
pub fn classify_coordinator_exit(
    error: &anyhow::Error,
    epoch: u64,
) -> IncidentDraft {
    for cause in error.chain() {
        if let Some(sd) = cause.downcast_ref::<SinkDeliveryError>() {
            return match sd.error.draft() {
                Some(d) => d.clone(),
                None => unclassified(
                    Component::Sink {
                        id: sd.sink_id.clone(),
                    },
                    sd.error.cause_code(),
                    epoch,
                ),
            };
        }
        if let Some(se) = cause.downcast_ref::<SinkError>()
            && let Some(d) = se.draft()
        {
            return d.clone();
        }
        if cause.downcast_ref::<OversizedTxError>().is_some() {
            return unclassified(
                Component::Coordinator,
                CauseCode::OversizedTransaction,
                epoch,
            );
        }
        if cause.downcast_ref::<TxProtocolError>().is_some() {
            return unclassified(
                Component::Coordinator,
                CauseCode::TransactionProtocol,
                epoch,
            );
        }
    }
    unclassified(Component::Coordinator, CauseCode::CoordinatorOther, epoch)
}

/// The deterministic order in which one of several concurrent failures is
/// the pipeline's primary incident (smallest first):
/// 1. a mapped reason before `unclassified_failure`;
/// 2. `halted_uncertain` (a side effect may have happened) before
///    `halted_safe`, before `running_degraded`;
/// 3. source, then sink, then coordinator, then pipeline;
/// 4. the incident id.
///
/// A coordinator stop caused by its source closing the channel is not a
/// candidate while any other failure exists.
pub fn primary_rank(
    draft: &IncidentDraft,
    id: &IncidentId,
) -> (u8, u8, u8, String) {
    let mapped = u8::from(draft.reason_code == ReasonCode::UnclassifiedFailure);
    let safety = match draft.safety_state {
        SafetyState::HaltedUncertain => 0,
        SafetyState::HaltedSafe => 1,
        SafetyState::RunningDegraded => 2,
    };
    let component = match draft.component {
        Component::Source { .. } => 0,
        Component::Sink { .. } => 1,
        Component::Coordinator => 2,
        Component::Pipeline => 3,
    };
    (mapped, safety, component, id.0.clone())
}

// ---------------------------------------------------------------------------
// Pipeline health
// ---------------------------------------------------------------------------

/// How long the supervisor waits, after a pipeline's first failure, for the
/// other task's exit before fixing the primary incident.
pub const FAILURE_GRACE: Duration = Duration::from_secs(5);
/// Persistence retry backoff bounds while the state store is unavailable.
pub const PERSIST_RETRY_MIN: Duration = Duration::from_millis(500);
pub const PERSIST_RETRY_MAX: Duration = Duration::from_secs(30);

/// Which supervised task exited.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Task {
    Source,
    Coordinator,
}

/// How a supervised task exited.
#[derive(Debug, Clone)]
pub enum TaskExit {
    /// Stopped by the operator (or by the pipeline's own shutdown).
    Clean,
    /// A failure with its classified draft.
    Failed(IncidentDraft),
    /// The coordinator stopped because its source closed the event channel:
    /// derivative of the source's own exit.
    ChannelClosed,
}

/// One incident this runtime knows about, and whether it is durable yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnownIncident {
    pub id: IncidentId,
    pub draft: IncidentDraft,
    /// `false` while persisting it fails (status must say so).
    pub durable: bool,
}

#[derive(Debug, Default)]
struct HealthInner {
    failing: bool,
    finalized: bool,
    source_exited: bool,
    coordinator_exited: bool,
    channel_closed: bool,
    known: Vec<KnownIncident>,
    primary: Option<IncidentId>,
}

/// The supervised state of a pipeline's tasks and the incidents of this
/// runtime. Failed is set synchronously at the first failure (before any
/// I/O), so status never reads Running after a critical task has exited.
pub struct PipelineHealth {
    store: IncidentStore,
    epoch: AtomicU64,
    inner: Mutex<HealthInner>,
    /// Stops persistence retries when the runtime is replaced or removed.
    shutdown: CancellationToken,
}

impl PipelineHealth {
    /// Health for a new runtime: repairs pending audit entries and an
    /// interrupted recovery, and carries over incidents a previous runtime
    /// could not persist (they stay visible as not durable and keep being
    /// retried).
    pub async fn start(
        store: IncidentStore,
        carried: Vec<IncidentDraft>,
    ) -> Result<Arc<Self>> {
        let epoch = store.prepare().await?;
        let health = Arc::new(Self {
            store,
            epoch: AtomicU64::new(epoch),
            inner: Mutex::new(HealthInner::default()),
            shutdown: CancellationToken::new(),
        });
        for draft in carried {
            let id = draft.incident_id(health.store.pipeline());
            health.inner.lock().known.push(KnownIncident {
                id,
                draft: draft.clone(),
                durable: false,
            });
            health.persist_in_background(draft);
        }
        Ok(health)
    }

    /// Health without startup repair (tests and runtimes built in place).
    pub fn new_unprepared(store: IncidentStore) -> Arc<Self> {
        Arc::new(Self {
            store,
            epoch: AtomicU64::new(0),
            inner: Mutex::new(HealthInner::default()),
            shutdown: CancellationToken::new(),
        })
    }

    pub fn store(&self) -> &IncidentStore {
        &self.store
    }

    /// The recovery epoch `unclassified_failure` identities bind to now.
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::SeqCst)
    }

    /// Whether a critical task has exited unexpectedly.
    pub fn is_failed(&self) -> bool {
        self.inner.lock().failing
    }

    /// The primary blocking incident (provisional until both exits are known
    /// or the grace period has passed; see [`Self::primary_is_final`]).
    pub fn blocking_incident(&self) -> Option<IncidentId> {
        self.inner.lock().primary.clone()
    }

    pub fn primary_is_final(&self) -> bool {
        self.inner.lock().finalized
    }

    /// Every incident this runtime raised or carried, with durability.
    pub fn known(&self) -> Vec<KnownIncident> {
        self.inner.lock().known.clone()
    }

    /// Whether any of them is not yet durable.
    pub fn durability_pending(&self) -> bool {
        self.inner.lock().known.iter().any(|k| !k.durable)
    }

    /// Record a supervised task's exit. State changes synchronously; every
    /// distinct failure is recorded durably in the background.
    pub fn exit(self: &Arc<Self>, task: Task, exit: TaskExit) {
        let mut to_persist = None;
        let mut start_grace = false;
        let mut finalize_now = false;
        {
            let mut inner = self.inner.lock();
            match task {
                Task::Source => inner.source_exited = true,
                Task::Coordinator => inner.coordinator_exited = true,
            }
            match exit {
                TaskExit::Clean => {}
                TaskExit::ChannelClosed => inner.channel_closed = true,
                TaskExit::Failed(draft) => {
                    let id = draft.incident_id(self.store.pipeline());
                    if !inner.known.iter().any(|k| k.id == id) {
                        inner.known.push(KnownIncident {
                            id,
                            draft: draft.clone(),
                            durable: false,
                        });
                        to_persist = Some(draft);
                    }
                }
            }
            let failure = inner.channel_closed || to_persist.is_some();
            if failure && !inner.failing {
                inner.failing = true;
                start_grace = true;
            }
            if inner.failing && !inner.finalized {
                Self::choose_primary(&mut inner);
                if inner.source_exited && inner.coordinator_exited {
                    finalize_now = true;
                }
            }
        }
        if let Some(draft) = to_persist {
            self.persist_in_background(draft);
        }
        if finalize_now {
            self.finalize();
        } else if start_grace {
            let health = Arc::clone(self);
            tokio::spawn(async move {
                tokio::select! {
                    _ = tokio::time::sleep(FAILURE_GRACE) => health.finalize(),
                    _ = health.shutdown.cancelled() => {}
                }
            });
        }
    }

    fn choose_primary(inner: &mut HealthInner) {
        inner.primary = inner
            .known
            .iter()
            .min_by_key(|k| primary_rank(&k.draft, &k.id))
            .map(|k| k.id.clone());
    }

    /// Fix the primary incident. A coordinator stop caused by its source
    /// closing the channel becomes an incident only when nothing else failed.
    fn finalize(self: &Arc<Self>) {
        let mut to_persist = None;
        {
            let mut inner = self.inner.lock();
            if inner.finalized || !inner.failing {
                return;
            }
            inner.finalized = true;
            if inner.known.is_empty() && inner.channel_closed {
                let draft = unclassified(
                    Component::Coordinator,
                    CauseCode::CoordinatorEnded,
                    self.epoch(),
                );
                let id = draft.incident_id(self.store.pipeline());
                inner.known.push(KnownIncident {
                    id,
                    draft: draft.clone(),
                    durable: false,
                });
                to_persist = Some(draft);
            }
            Self::choose_primary(&mut inner);
            if let Some(primary) = &inner.primary
                && let Some(k) = inner.known.iter().find(|k| &k.id == primary)
            {
                error!(
                    pipeline = %self.store.pipeline(),
                    incident = %primary,
                    reason = k.draft.reason_code.as_str(),
                    cause = k.draft.cause_code.as_str(),
                    incidents = inner.known.len(),
                    "pipeline failed: {}",
                    k.draft.explanation()
                );
            }
        }
        if let Some(draft) = to_persist {
            self.persist_in_background(draft);
        }
    }

    fn mark_durable(&self, id: &IncidentId) {
        if let Some(k) =
            self.inner.lock().known.iter_mut().find(|k| &k.id == id)
        {
            k.durable = true;
        }
    }

    /// Persist `draft`, retrying with bounded backoff until it is durable or
    /// the runtime shuts down.
    fn persist_in_background(self: &Arc<Self>, draft: IncidentDraft) {
        let health = Arc::clone(self);
        tokio::spawn(async move {
            let id = draft.incident_id(health.store.pipeline());
            let mut delay = PERSIST_RETRY_MIN;
            loop {
                match health.store.raise(&draft, 1).await {
                    Ok(_) => {
                        health.mark_durable(&id);
                        return;
                    }
                    Err(e) => warn!(
                        pipeline = %health.store.pipeline(),
                        incident = %id,
                        error = %format!("{e:#}"),
                        retry_in_ms = delay.as_millis(),
                        "incident not persisted yet (durability pending); retrying"
                    ),
                }
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = health.shutdown.cancelled() => return,
                }
                delay = (delay * 2).min(PERSIST_RETRY_MAX);
            }
        });
    }

    /// One more persistence attempt for every incident not durable yet (used
    /// before a resume); returns the drafts that are still not durable.
    pub async fn flush(&self) -> Vec<IncidentDraft> {
        let pending: Vec<KnownIncident> = self
            .inner
            .lock()
            .known
            .iter()
            .filter(|k| !k.durable)
            .cloned()
            .collect();
        let mut still = Vec::new();
        for k in pending {
            match self.store.raise(&k.draft, 1).await {
                Ok(_) => self.mark_durable(&k.id),
                Err(_) => still.push(k.draft),
            }
        }
        still
    }

    /// Stop background work of this runtime (persistence retries, grace).
    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }

    /// The source signalled readiness: when the coordinator is still running
    /// and nothing has failed, the pipeline has reached verified Running, and
    /// its open `unclassified_failure` incidents are resolved as
    /// `pipeline_recovered` (the recovery epoch advances).
    pub async fn source_ready(&self) {
        {
            let inner = self.inner.lock();
            if inner.failing || inner.source_exited || inner.coordinator_exited
            {
                return;
            }
        }
        match self.store.recover().await {
            Ok(Some(epoch)) => {
                self.epoch.store(epoch, Ordering::SeqCst);
                info!(
                    pipeline = %self.store.pipeline(),
                    recovery_epoch = epoch,
                    "pipeline recovered: unclassified incidents resolved"
                );
            }
            Ok(None) => {}
            Err(e) => warn!(
                pipeline = %self.store.pipeline(),
                error = %format!("{e:#}"),
                "could not record pipeline recovery; incidents stay open"
            ),
        }
    }
}

impl Drop for PipelineHealth {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltaforge_core::incident::EvidenceKey;
    use storage::MemoryStorageBackend;
    use storage::adapters::test_util::FaultBackend;

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
        recorder.seen.lock().get_mut(&id).unwrap().0 =
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

    fn unc(epoch: u64) -> IncidentDraft {
        unclassified(
            Component::Source { id: "s".into() },
            CauseCode::SourceConnect,
            epoch,
        )
    }

    #[tokio::test]
    async fn recovery_resolves_unclassified_and_starts_a_new_generation() {
        let store = IncidentStore::new(backend(), "p");
        assert_eq!(store.prepare().await.unwrap(), 0);
        let first = id_of(&store, &unc(0)).await;
        let mapped = id_of(&store, &draft(1)).await;
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
            "only unclassified incidents are resolved by recovery"
        );
        // After recovery the same failure is a new occurrence.
        let second = id_of(&store, &unc(1)).await;
        assert_ne!(second, first);
        // Nothing to resolve: no new generation.
        store.resolve(&second, recovered()).await.unwrap().unwrap();
        assert_eq!(store.recover().await.unwrap(), None);
        assert_eq!(store.prepare().await.unwrap(), 1);
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

    #[tokio::test]
    async fn records_hold_no_error_text() {
        let store = IncidentStore::new(backend(), "p");
        let err = SourceError::Connect {
            details: "connect to postgres://u:hunter2@h/db failed".into(),
        };
        let d = classify_source_exit("pg", Err(&err), false, 0);
        let rec = store.raise(&d, 1).await.unwrap().record().clone();
        let json = serde_json::to_string(&rec).unwrap();
        assert!(!json.contains("hunter2") && !json.contains("postgres://"));
        assert_eq!(rec.reason_code, ReasonCode::UnclassifiedFailure);
        assert_eq!(rec.cause_code, CauseCode::SourceConnect);
        assert_eq!(rec.retryability, Retryability::AutoRetry);
        assert_eq!(rec.safety_state, SafetyState::HaltedUncertain);
        assert!(!rec.explanation().contains("hunter2"));
    }

    #[test]
    fn a_component_draft_is_used_as_raised() {
        let raised = draft(7);
        let err = SourceError::incident(
            raised.clone(),
            SourceError::Lineage {
                details: "x".into(),
            },
        );
        assert_eq!(classify_source_exit("pg", Err(&err), false, 0), raised);
        assert_eq!(err.to_string(), raised.explanation());
        assert!(matches!(err.root(), SourceError::Lineage { .. }));
        assert_eq!(err.cause_code(), CauseCode::SourceLineage);
    }

    #[test]
    fn coordinator_failures_are_attributed_to_the_failing_sink() {
        let sink_err = anyhow::Error::new(SinkDeliveryError {
            sink_id: "kafka".into(),
            error: SinkError::Connect {
                details: "broker".into(),
            },
        })
        .context("commit policy not satisfied");
        let d = classify_coordinator_exit(&sink_err, 0);
        assert_eq!(d.component, Component::Sink { id: "kafka".into() });
        assert_eq!(d.cause_code, CauseCode::SinkConnect);
        let raised = draft(3);
        let with_draft = anyhow::Error::new(SinkDeliveryError {
            sink_id: "kafka".into(),
            error: SinkError::incident(
                raised.clone(),
                SinkError::Fatal {
                    details: "x".into(),
                },
            ),
        });
        assert_eq!(classify_coordinator_exit(&with_draft, 0), raised);
        assert_eq!(
            classify_coordinator_exit(&anyhow::anyhow!("x"), 0).cause_code,
            CauseCode::CoordinatorOther
        );
    }

    // ---- supervision -------------------------------------------------------

    async fn health_on(b: ArcStorageBackend) -> Arc<PipelineHealth> {
        PipelineHealth::start(IncidentStore::new(b, "p"), vec![])
            .await
            .unwrap()
    }

    async fn settle() {
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
    }

    fn sink_uncertain() -> IncidentDraft {
        IncidentDraft::new(
            ReasonCode::SinkAckUncertain,
            Component::Sink { id: "k".into() },
            Retryability::OperatorAction,
            SafetyState::HaltedUncertain,
            CauseCode::SinkConnect,
        )
        .discriminate("batch", "b1")
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_failures_are_all_recorded_with_a_deterministic_primary()
    {
        let h = health_on(backend()).await;
        let source = unc(0);
        h.exit(Task::Source, TaskExit::Failed(source.clone()));
        assert!(h.is_failed(), "failed at once");
        h.exit(Task::Coordinator, TaskExit::Failed(sink_uncertain()));
        settle().await;
        assert!(h.primary_is_final(), "both exits known");
        // The mapped sink uncertainty outranks the unclassified source error,
        // and neither is suppressed.
        assert_eq!(
            h.blocking_incident(),
            Some(sink_uncertain().incident_id("p"))
        );
        let ids: Vec<IncidentId> = h
            .store()
            .list()
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.incident_id)
            .collect();
        assert!(ids.contains(&source.incident_id("p")));
        assert!(ids.contains(&sink_uncertain().incident_id("p")));
        // Arrival order does not matter.
        let h2 = health_on(backend()).await;
        h2.exit(Task::Coordinator, TaskExit::Failed(sink_uncertain()));
        h2.exit(Task::Source, TaskExit::Failed(source));
        assert_eq!(h2.blocking_incident(), h.blocking_incident());
    }

    #[tokio::test(start_paused = true)]
    async fn the_source_error_wins_over_a_derivative_channel_close() {
        let h = health_on(backend()).await;
        h.exit(Task::Coordinator, TaskExit::ChannelClosed);
        assert!(h.is_failed(), "failed at once, before attribution");
        assert!(h.blocking_incident().is_none());
        let d = draft(1);
        h.exit(Task::Source, TaskExit::Failed(d.clone()));
        settle().await;
        assert_eq!(h.blocking_incident(), Some(d.incident_id("p")));
        let recs = h.store().list().await.unwrap();
        assert_eq!(recs.len(), 1, "the derivative stop is not an incident");
    }

    #[tokio::test(start_paused = true)]
    async fn a_coordinator_stop_without_a_source_failure_is_the_incident() {
        let h = health_on(backend()).await;
        h.exit(Task::Coordinator, TaskExit::ChannelClosed);
        tokio::time::sleep(FAILURE_GRACE + Duration::from_secs(1)).await;
        settle().await;
        let id = h.blocking_incident().expect("attributed");
        let rec = h.store().get(&id).await.unwrap().unwrap();
        assert_eq!(rec.cause_code, CauseCode::CoordinatorEnded);
        assert!(h.primary_is_final());
    }

    #[tokio::test(start_paused = true)]
    async fn persistence_is_retried_while_the_store_is_down() {
        let f = Arc::new(FaultBackend::new());
        let b: ArcStorageBackend = f.clone();
        let h = health_on(b).await;
        f.fail_writes_to.lock().unwrap().push(INCIDENTS_NS.into());
        let d = draft(1);
        h.exit(Task::Source, TaskExit::Failed(d.clone()));
        settle().await;
        assert_eq!(h.blocking_incident(), Some(d.incident_id("p")));
        assert!(h.durability_pending(), "status must say it is not durable");
        assert_eq!(h.flush().await, vec![d.clone()], "still not durable");
        f.fail_writes_to.lock().unwrap().clear();
        tokio::time::sleep(PERSIST_RETRY_MAX).await;
        settle().await;
        assert!(!h.durability_pending(), "retried until durable");
        assert!(h.store().get(&d.incident_id("p")).await.unwrap().is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn an_incident_not_yet_durable_is_carried_into_the_next_runtime() {
        let f = Arc::new(FaultBackend::new());
        let b: ArcStorageBackend = f.clone();
        let old = health_on(b.clone()).await;
        f.fail_writes_to.lock().unwrap().push(INCIDENTS_NS.into());
        old.exit(Task::Source, TaskExit::Failed(draft(1)));
        settle().await;
        let still = old.flush().await;
        old.shutdown();
        let new = PipelineHealth::start(IncidentStore::new(b, "p"), still)
            .await
            .unwrap();
        assert!(!new.is_failed(), "the new runtime runs");
        assert!(new.durability_pending(), "the carried incident is visible");
        f.fail_writes_to.lock().unwrap().clear();
        tokio::time::sleep(PERSIST_RETRY_MAX).await;
        settle().await;
        assert!(!new.durability_pending());
    }

    #[tokio::test]
    async fn recovery_needs_the_verified_running_barrier() {
        let b = backend();
        let store = IncidentStore::new(b.clone(), "p");
        let open = id_of(&store, &unc(0)).await;

        // A failed runtime never recovers, even if its source got ready.
        let failed = health_on(b.clone()).await;
        failed.exit(Task::Coordinator, TaskExit::Failed(unc(0)));
        failed.source_ready().await;
        assert!(
            !store
                .get(&open)
                .await
                .unwrap()
                .unwrap()
                .status
                .is_resolved()
        );
        failed.shutdown();
        // Nor one whose coordinator already stopped cleanly.
        let stopped = health_on(b.clone()).await;
        stopped.exit(Task::Coordinator, TaskExit::Clean);
        stopped.source_ready().await;
        assert!(
            !store
                .get(&open)
                .await
                .unwrap()
                .unwrap()
                .status
                .is_resolved()
        );

        // A healthy runtime whose source is ready recovers.
        let healthy = health_on(b.clone()).await;
        healthy.source_ready().await;
        assert!(
            store
                .get(&open)
                .await
                .unwrap()
                .unwrap()
                .status
                .is_resolved()
        );
        assert_eq!(healthy.epoch(), 1);
        // A later identical failure is a new occurrence, not a reopen.
        assert_ne!(unc(healthy.epoch()).incident_id("p"), open);
    }
}
