//! Durable operator incidents and pipeline failure supervision.
//!
//! - [`IncidentStore`]: one versioned record per incident (slot namespace
//!   `incidents`, created and changed only with create/CAS) and an append-only
//!   audit log of lifecycle transitions (`incidents.audit`). The lifecycle is
//!   Open -> Acknowledged -> Resolved: acknowledgement means an operator has
//!   seen it and never clears it; only a verified resolver (or a recovery
//!   operation) resolves.
//! - [`IncidentRecorder`]: raises drafts into the store once per identity and
//!   throttles repeated occurrences, so a retry loop updates one record without
//!   amplifying writes.
//! - [`classify_source_exit`] / [`classify_coordinator_exit`]: turn a task's
//!   exit into the blocking incident draft - the component's own draft when it
//!   raised one, otherwise `unclassified_failure` built from the error's type
//!   (never its text).
//! - [`PipelineHealth`]: the pipeline's Failed state, set synchronously when a
//!   task exit is observed, before anything is persisted.
//!
//! Nothing here stores or serves an error's Display/Debug text.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use deltaforge_core::incident::{
    ActionCode, CauseCode, Component, Evidence, IncidentDraft, IncidentId,
    ReasonCode, Retryability, SafetyState,
};
use deltaforge_core::{SinkError, SourceError};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use storage::ArcStorageBackend;
use tracing::{error, warn};

use crate::coordinator::{
    OversizedTxError, SinkDeliveryError, TxProtocolError,
};

/// Slot namespace of incident records.
pub const INCIDENTS_NS: &str = "incidents";
/// Log namespace of the incident audit trail.
pub const AUDIT_NS: &str = "incidents.audit";
/// Open (or acknowledged) incidents kept per pipeline; beyond it, a single
/// overflow incident stands for the rest.
pub const MAX_OPEN: usize = 64;
/// Resolved incidents kept per pipeline (the audit log keeps the history).
pub const MAX_RESOLVED: usize = 100;
/// Longest operator-supplied acknowledgement field kept.
pub const MAX_ACK_TEXT: usize = 256;
/// How often repeated occurrences of one incident are written durably.
pub const OCCURRENCE_FLUSH: Duration = Duration::from_secs(30);

const RECORD_FORMAT: u32 = 1;

/// How an incident was resolved.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Resolution {
    /// The check that raised it passed again.
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
    pub evidence: Evidence,
    pub actions: Vec<ActionCode>,
    pub first_seen_ms: i64,
    pub last_seen_ms: i64,
    pub occurrences: u64,
    pub status: IncidentStatus,
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
}

/// One lifecycle transition in the audit log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditEntry {
    pub at_ms: i64,
    pub pipeline: String,
    pub incident_id: IncidentId,
    pub reason_code: ReasonCode,
    pub transition: Transition,
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
}

/// Why an acknowledgement or resolution did not apply.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum LifecycleError {
    #[error("incident {0} not found")]
    NotFound(String),
    #[error("incident {0} is already resolved")]
    AlreadyResolved(String),
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

/// Durable incident records and their audit trail for one pipeline.
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

    fn prefix(&self) -> String {
        format!("{}/", segment(&self.pipeline))
    }

    fn key(&self, id: &IncidentId) -> String {
        format!("{}{}", self.prefix(), id.0)
    }

    async fn audit(
        &self,
        rec: &IncidentRecord,
        transition: Transition,
    ) -> Result<()> {
        let entry = AuditEntry {
            at_ms: now_ms(),
            pipeline: self.pipeline.clone(),
            incident_id: rec.incident_id.clone(),
            reason_code: rec.reason_code,
            transition,
        };
        self.backend
            .log_append(
                AUDIT_NS,
                &segment(&self.pipeline),
                &serde_json::to_vec(&entry)?,
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
        }
    }

    /// Open `draft`'s incident, or count `occurrences` more of it. A resolved
    /// record of the same identity is reopened. Beyond [`MAX_OPEN`] open
    /// incidents a new one is not created (returns `None`); the caller logs it.
    pub async fn raise(
        &self,
        draft: &IncidentDraft,
        occurrences: u64,
    ) -> Result<Option<IncidentRecord>> {
        let id = draft.incident_id(&self.pipeline);
        loop {
            match self.read(&id).await? {
                None => {
                    let open = self
                        .list()
                        .await?
                        .iter()
                        .filter(|r| !r.status.is_resolved())
                        .count();
                    if open >= MAX_OPEN {
                        return Ok(None);
                    }
                    let mut rec = self.fresh(id.clone(), draft);
                    rec.occurrences = occurrences.max(1);
                    let created = self
                        .backend
                        .slot_create(
                            INCIDENTS_NS,
                            &self.key(&id),
                            &serde_json::to_vec(&rec)?,
                        )
                        .await?;
                    if created.is_some() {
                        self.audit(&rec, Transition::Opened).await?;
                        return Ok(Some(rec));
                    }
                }
                Some((version, mut rec)) => {
                    let reopened = rec.status.is_resolved();
                    if reopened {
                        rec.status = IncidentStatus::Open;
                        rec.evidence = draft.evidence.clone();
                    }
                    rec.occurrences =
                        rec.occurrences.saturating_add(occurrences);
                    rec.last_seen_ms = now_ms();
                    if self
                        .backend
                        .slot_cas(
                            INCIDENTS_NS,
                            &self.key(&id),
                            version,
                            &serde_json::to_vec(&rec)?,
                        )
                        .await?
                    {
                        if reopened {
                            self.audit(&rec, Transition::Reopened).await?;
                        }
                        return Ok(Some(rec));
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
        loop {
            let Some((version, mut rec)) = self.read(id).await? else {
                return Ok(Err(LifecycleError::NotFound(id.0.clone())));
            };
            if rec.status.is_resolved() {
                return Ok(Err(LifecycleError::AlreadyResolved(id.0.clone())));
            }
            let (asserted_actor, origin, reason) = (
                bounded(asserted_actor),
                origin.map(bounded),
                bounded(reason),
            );
            rec.status = IncidentStatus::Acknowledged {
                asserted_actor: asserted_actor.clone(),
                actor_verified: false,
                origin: origin.clone(),
                reason: reason.clone(),
                at_ms: now_ms(),
            };
            if self
                .backend
                .slot_cas(
                    INCIDENTS_NS,
                    &self.key(id),
                    version,
                    &serde_json::to_vec(&rec)?,
                )
                .await?
            {
                self.audit(
                    &rec,
                    Transition::Acknowledged {
                        asserted_actor,
                        actor_verified: false,
                        origin,
                        reason,
                    },
                )
                .await?;
                return Ok(Ok(rec));
            }
        }
    }

    /// Open/Acknowledged -> Resolved. Resolving a resolved incident is a
    /// no-op. Prunes the oldest resolved records beyond [`MAX_RESOLVED`].
    pub async fn resolve(
        &self,
        id: &IncidentId,
        by: Resolution,
    ) -> Result<std::result::Result<IncidentRecord, LifecycleError>> {
        loop {
            let Some((version, mut rec)) = self.read(id).await? else {
                return Ok(Err(LifecycleError::NotFound(id.0.clone())));
            };
            if rec.status.is_resolved() {
                return Ok(Ok(rec));
            }
            rec.status = IncidentStatus::Resolved {
                by: by.clone(),
                at_ms: now_ms(),
            };
            if self
                .backend
                .slot_cas(
                    INCIDENTS_NS,
                    &self.key(id),
                    version,
                    &serde_json::to_vec(&rec)?,
                )
                .await?
            {
                self.audit(&rec, Transition::Resolved { by }).await?;
                self.prune_resolved().await?;
                return Ok(Ok(rec));
            }
        }
    }

    async fn prune_resolved(&self) -> Result<()> {
        let mut resolved: Vec<(i64, IncidentId)> = self
            .list()
            .await?
            .into_iter()
            .filter_map(|r| match r.status {
                IncidentStatus::Resolved { at_ms, .. } => {
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
}

/// Raises drafts into an [`IncidentStore`]: the first sighting of an identity
/// in this process is written at once; repeats are counted in memory and
/// written at most every [`OCCURRENCE_FLUSH`].
pub struct IncidentRecorder {
    store: IncidentStore,
    seen: Mutex<HashMap<IncidentId, (Instant, u64)>>,
}

impl IncidentRecorder {
    pub fn new(store: IncidentStore) -> Self {
        Self {
            store,
            seen: Mutex::new(HashMap::new()),
        }
    }

    pub fn store(&self) -> &IncidentStore {
        &self.store
    }

    /// Record one occurrence of `draft`. Returns its identity.
    pub async fn record(&self, draft: &IncidentDraft) -> Result<IncidentId> {
        let id = draft.incident_id(&self.store.pipeline);
        let flush = {
            let mut seen = self.seen.lock();
            match seen.get_mut(&id) {
                None => {
                    seen.insert(id.clone(), (Instant::now(), 0));
                    Some(1)
                }
                Some((last, pending)) => {
                    *pending += 1;
                    if last.elapsed() >= OCCURRENCE_FLUSH {
                        *last = Instant::now();
                        Some(std::mem::take(pending))
                    } else {
                        None
                    }
                }
            }
        };
        if let Some(n) = flush
            && self.store.raise(draft, n).await?.is_none()
        {
            warn!(
                pipeline = %self.store.pipeline,
                reason = draft.reason_code.as_str(),
                "incident not recorded: the pipeline already has the maximum \
                 number of open incidents"
            );
        }
        Ok(id)
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
/// mapping classifies. Halted-uncertain, from the error's type only.
pub fn unclassified(component: Component, cause: CauseCode) -> IncidentDraft {
    IncidentDraft::new(
        ReasonCode::UnclassifiedFailure,
        component,
        retryability_of(cause),
        SafetyState::HaltedUncertain,
        cause,
    )
    .discriminate("cause", cause.as_str())
    .with_actions(&[ActionCode::InspectLogs])
}

/// The blocking draft for a source task that ended on its own: its own draft
/// when it raised one, otherwise unclassified. `None` result = ended without
/// an error; `panicked` = the task panicked.
pub fn classify_source_exit(
    source_id: &str,
    result: std::result::Result<(), &SourceError>,
    panicked: bool,
) -> IncidentDraft {
    let component = Component::Source {
        id: source_id.to_string(),
    };
    match result {
        _ if panicked => unclassified(component, CauseCode::SourcePanicked),
        Ok(()) => unclassified(component, CauseCode::SourceEnded),
        Err(e) => match e.draft() {
            Some(d) => d.clone(),
            None => unclassified(component, e.cause_code()),
        },
    }
}

/// The blocking draft for a coordinator that failed: a sink's own draft when
/// the failure came from one, otherwise unclassified from the error's type.
pub fn classify_coordinator_exit(error: &anyhow::Error) -> IncidentDraft {
    for cause in error.chain() {
        if let Some(sd) = cause.downcast_ref::<SinkDeliveryError>() {
            return match sd.error.draft() {
                Some(d) => d.clone(),
                None => unclassified(
                    Component::Sink {
                        id: sd.sink_id.clone(),
                    },
                    sd.error.cause_code(),
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
            );
        }
        if cause.downcast_ref::<TxProtocolError>().is_some() {
            return unclassified(
                Component::Coordinator,
                CauseCode::TransactionProtocol,
            );
        }
    }
    unclassified(Component::Coordinator, CauseCode::CoordinatorOther)
}

// ---------------------------------------------------------------------------
// Pipeline health
// ---------------------------------------------------------------------------

/// How long a coordinator that stopped because its source closed the event
/// channel waits for the source's own exit to attribute the failure.
pub const SOURCE_ATTRIBUTION_GRACE: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq, Eq)]
enum HealthState {
    Healthy,
    /// The coordinator stopped because the source closed its channel; the
    /// source's own exit is expected and takes precedence.
    AwaitingSource,
    Failed {
        incident_id: IncidentId,
    },
}

/// The supervised state of a pipeline's tasks. Set synchronously when an exit
/// is observed (before any I/O), so status never reads Running after a
/// critical task has exited.
pub struct PipelineHealth {
    pipeline: String,
    state: Mutex<HealthState>,
    recorder: Arc<IncidentRecorder>,
}

impl PipelineHealth {
    pub fn new(pipeline: &str, recorder: Arc<IncidentRecorder>) -> Arc<Self> {
        Arc::new(Self {
            pipeline: pipeline.to_string(),
            state: Mutex::new(HealthState::Healthy),
            recorder,
        })
    }

    /// Whether a critical task has exited unexpectedly.
    pub fn is_failed(&self) -> bool {
        !matches!(*self.state.lock(), HealthState::Healthy)
    }

    /// The blocking incident, once attributed.
    pub fn blocking_incident(&self) -> Option<IncidentId> {
        match &*self.state.lock() {
            HealthState::Failed { incident_id } => Some(incident_id.clone()),
            _ => None,
        }
    }

    pub fn recorder(&self) -> &Arc<IncidentRecorder> {
        &self.recorder
    }

    /// A failure with an attributed draft. The first attributed failure wins;
    /// it replaces a pending source attribution.
    pub async fn failed(&self, draft: IncidentDraft) {
        let id = draft.incident_id(&self.pipeline);
        {
            let mut state = self.state.lock();
            if matches!(*state, HealthState::Failed { .. }) {
                return;
            }
            *state = HealthState::Failed {
                incident_id: id.clone(),
            };
        }
        error!(
            pipeline = %self.pipeline,
            incident = %id,
            reason = draft.reason_code.as_str(),
            cause = draft.cause_code.as_str(),
            "pipeline failed: {}",
            draft.explanation()
        );
        if let Err(e) = self.recorder.record(&draft).await {
            error!(
                pipeline = %self.pipeline,
                incident = %id,
                error = %format!("{e:#}"),
                "failed to record the pipeline's blocking incident durably"
            );
        }
    }

    /// The coordinator stopped because its source closed the event channel.
    /// Marks the pipeline failed at once and waits for the source's own exit
    /// to attribute it; without one within the grace period the coordinator's
    /// stop is the incident.
    pub async fn coordinator_ended(self: &Arc<Self>) {
        {
            let mut state = self.state.lock();
            if *state != HealthState::Healthy {
                return;
            }
            *state = HealthState::AwaitingSource;
        }
        let health = Arc::clone(self);
        tokio::spawn(async move {
            tokio::time::sleep(SOURCE_ATTRIBUTION_GRACE).await;
            if *health.state.lock() == HealthState::AwaitingSource {
                health
                    .failed(unclassified(
                        Component::Coordinator,
                        CauseCode::CoordinatorEnded,
                    ))
                    .await;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltaforge_core::incident::EvidenceKey;
    use storage::MemoryStorageBackend;

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

    #[tokio::test]
    async fn a_repeated_condition_is_one_record() {
        let store = IncidentStore::new(backend(), "p");
        let a = store.raise(&draft(1), 1).await.unwrap().unwrap();
        let b = store.raise(&draft(1), 3).await.unwrap().unwrap();
        assert_eq!(a.incident_id, b.incident_id);
        assert_eq!(b.occurrences, 4);
        assert_eq!(store.list().await.unwrap().len(), 1);
        // A different occurrence (another transition) is another incident.
        store.raise(&draft(2), 1).await.unwrap().unwrap();
        assert_eq!(store.list().await.unwrap().len(), 2);
        // One audit entry per opening, none per repeat.
        assert_eq!(store.audit_trail(100).await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn the_same_condition_after_a_restart_is_the_same_incident() {
        let b = backend();
        let id = IncidentStore::new(b.clone(), "p")
            .raise(&draft(1), 1)
            .await
            .unwrap()
            .unwrap()
            .incident_id;
        // A new process: a fresh store and recorder over the same backend.
        let recorder = IncidentRecorder::new(IncidentStore::new(b, "p"));
        assert_eq!(recorder.record(&draft(1)).await.unwrap(), id);
        let rec = recorder.store().get(&id).await.unwrap().unwrap();
        assert_eq!(rec.occurrences, 2);
        assert_eq!(recorder.store().list().await.unwrap().len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_raises_create_one_record() {
        // Reads yield, so the 16 read-modify-write cycles interleave and
        // their CAS writes conflict.
        let fault = storage::adapters::test_util::FaultBackend::new();
        fault
            .yield_after_slot_reads
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let b: ArcStorageBackend = Arc::new(fault);
        let mut tasks = Vec::new();
        for _ in 0..16 {
            let store = IncidentStore::new(b.clone(), "p");
            tasks.push(tokio::spawn(async move {
                store.raise(&draft(1), 1).await.unwrap().unwrap()
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        let store = IncidentStore::new(b, "p");
        let all = store.list().await.unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].occurrences, 16, "no occurrence lost to a race");
        assert_eq!(store.audit_trail(100).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn acknowledgement_never_resolves() {
        let store = IncidentStore::new(backend(), "p");
        let id = store
            .raise(&draft(1), 1)
            .await
            .unwrap()
            .unwrap()
            .incident_id;
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
        // Only a resolver resolves.
        let rec = store
            .resolve(
                &id,
                Resolution::VerifiedRecovery {
                    check: "lineage_verified".into(),
                },
            )
            .await
            .unwrap()
            .unwrap();
        assert!(!rec.is_blocking());
        assert_eq!(
            store.acknowledge(&id, "bob", None, "late").await.unwrap(),
            Err(LifecycleError::AlreadyResolved(id.0.clone()))
        );
        let kinds: Vec<String> = store
            .audit_trail(100)
            .await
            .unwrap()
            .into_iter()
            .map(|e| match e.transition {
                Transition::Opened => "opened".into(),
                Transition::Reopened => "reopened".into(),
                Transition::Acknowledged { .. } => "acknowledged".into(),
                Transition::Resolved { .. } => "resolved".into(),
            })
            .collect();
        assert_eq!(kinds, ["opened", "acknowledged", "resolved"]);
    }

    #[tokio::test]
    async fn acknowledgement_text_is_bounded() {
        let store = IncidentStore::new(backend(), "p");
        let id = store
            .raise(&draft(1), 1)
            .await
            .unwrap()
            .unwrap()
            .incident_id;
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
        let id = store
            .raise(&draft(1), 1)
            .await
            .unwrap()
            .unwrap()
            .incident_id;
        store
            .resolve(&id, Resolution::VerifiedRecovery { check: "c".into() })
            .await
            .unwrap()
            .unwrap();
        let rec = store.raise(&draft(1), 1).await.unwrap().unwrap();
        assert_eq!(rec.status, IncidentStatus::Open);
        assert!(matches!(
            store
                .audit_trail(100)
                .await
                .unwrap()
                .last()
                .unwrap()
                .transition,
            Transition::Reopened
        ));
    }

    #[tokio::test]
    async fn open_incidents_are_bounded() {
        let store = IncidentStore::new(backend(), "p");
        for n in 0..MAX_OPEN as u32 {
            assert!(store.raise(&draft(n), 1).await.unwrap().is_some());
        }
        assert!(store.raise(&draft(9999), 1).await.unwrap().is_none());
        // An existing one still counts its occurrences.
        assert!(store.raise(&draft(0), 1).await.unwrap().is_some());
        assert_eq!(store.list().await.unwrap().len(), MAX_OPEN);
    }

    #[tokio::test]
    async fn resolved_incidents_are_pruned_oldest_first() {
        let store = IncidentStore::new(backend(), "p");
        let mut ids = Vec::new();
        for n in 0..(MAX_RESOLVED as u32 + 5) {
            let id = store
                .raise(&draft(n), 1)
                .await
                .unwrap()
                .unwrap()
                .incident_id;
            store
                .resolve(
                    &id,
                    Resolution::VerifiedRecovery { check: "c".into() },
                )
                .await
                .unwrap()
                .unwrap();
            ids.push(id);
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        let kept = store.list().await.unwrap();
        assert_eq!(kept.len(), MAX_RESOLVED);
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
        let id = recorder.record(&draft(1)).await.unwrap();
        for _ in 0..50 {
            recorder.record(&draft(1)).await.unwrap();
        }
        // Within the flush interval only the first occurrence is durable.
        let rec = recorder.store().get(&id).await.unwrap().unwrap();
        assert_eq!(rec.occurrences, 1);
        // Once the interval has passed the pending count is written.
        recorder.seen.lock().get_mut(&id).unwrap().0 =
            Instant::now() - OCCURRENCE_FLUSH;
        recorder.record(&draft(1)).await.unwrap();
        let rec = recorder.store().get(&id).await.unwrap().unwrap();
        assert_eq!(rec.occurrences, 52);
    }

    #[tokio::test]
    async fn records_hold_no_error_text() {
        let store = IncidentStore::new(backend(), "p");
        let err = SourceError::Connect {
            details: "connect to postgres://u:hunter2@h/db failed".into(),
        };
        let d = classify_source_exit("pg", Err(&err), false);
        let rec = store.raise(&d, 1).await.unwrap().unwrap();
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
        assert_eq!(classify_source_exit("pg", Err(&err), false), raised);
        // The wrapper displays only the sanitized explanation and keeps the
        // typed cause for classification.
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
        let d = classify_coordinator_exit(&sink_err);
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
        assert_eq!(classify_coordinator_exit(&with_draft), raised);

        let other = anyhow::anyhow!("something");
        assert_eq!(
            classify_coordinator_exit(&other).cause_code,
            CauseCode::CoordinatorOther
        );
    }

    fn health() -> Arc<PipelineHealth> {
        PipelineHealth::new(
            "p",
            Arc::new(IncidentRecorder::new(IncidentStore::new(backend(), "p"))),
        )
    }

    #[tokio::test]
    async fn the_source_failure_wins_over_the_coordinators_channel_close() {
        let h = health();
        h.coordinator_ended().await;
        assert!(h.is_failed(), "failed at once, before attribution");
        assert!(h.blocking_incident().is_none());
        let d = draft(1);
        h.failed(d.clone()).await;
        assert_eq!(h.blocking_incident(), Some(d.incident_id("p")));
        // A later failure does not replace the first attributed one.
        h.failed(draft(2)).await;
        assert_eq!(h.blocking_incident(), Some(d.incident_id("p")));
        let recs = h.recorder().store().list().await.unwrap();
        assert_eq!(recs.len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_coordinator_stop_without_a_source_exit_is_the_incident() {
        let h = health();
        h.coordinator_ended().await;
        tokio::time::sleep(SOURCE_ATTRIBUTION_GRACE + Duration::from_secs(1))
            .await;
        // Let the attribution task finish its durable write.
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        let id = h.blocking_incident().expect("attributed");
        let rec = h.recorder().store().get(&id).await.unwrap().unwrap();
        assert_eq!(rec.cause_code, CauseCode::CoordinatorEnded);
        assert_eq!(rec.component, Component::Coordinator);
    }
}
