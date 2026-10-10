//! Pipeline failure supervision and the incident views the API serves.
//!
//! The durable incident store lives in [`storage::adapters::incidents`] (it is
//! re-exported here); this module adds:
//! - [`classify_source_exit`] / [`classify_coordinator_exit`]: a task exit's
//!   blocking draft - the component's own when it raised one, otherwise
//!   `unclassified_failure` from the error's type (never its text);
//! - [`PipelineHealth`]: supervises the source and coordinator exits. The
//!   pipeline turns Failed synchronously at the first failure; every distinct
//!   failure is recorded (persistence retried while the store is down); the
//!   primary incident is chosen deterministically once both exits are known or
//!   the grace period passes;
//! - [`record_view`] / [`known_view`]: sanitized API views.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Result;
use deltaforge_core::incident::{
    ActionCode, CauseCode, Component, IncidentDraft, IncidentId, ReasonCode,
    Retryability, SafetyState,
};
use deltaforge_core::{SinkError, SourceError};
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::coordinator::{
    OversizedTxError, SinkDeliveryError, TxProtocolError,
};
pub use storage::adapters::incidents::*;

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
            Some(d) => bind_epoch(d.clone(), epoch),
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
    /// Held across each persistence attempt and the marking that follows
    /// it, so an incident whose write is in flight is never raised again by
    /// another attempt (each known incident is one occurrence).
    persist: tokio::sync::Mutex<()>,
    /// Set by [`Self::hand_over`]: background attempts stop; what is still
    /// not durable is carried by the next runtime.
    retired: std::sync::atomic::AtomicBool,
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
            persist: tokio::sync::Mutex::new(()),
            retired: std::sync::atomic::AtomicBool::new(false),
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
            persist: tokio::sync::Mutex::new(()),
            retired: std::sync::atomic::AtomicBool::new(false),
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

    fn is_durable(&self, id: &IncidentId) -> bool {
        self.inner
            .lock()
            .known
            .iter()
            .any(|k| &k.id == id && k.durable)
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
                let attempt = health.persist.lock().await;
                if health.retired.load(Ordering::SeqCst)
                    || health.is_durable(&id)
                {
                    return;
                }
                let raised = health.store.raise(&draft, 1).await;
                if raised.is_ok() {
                    health.mark_durable(&id);
                }
                drop(attempt);
                match raised {
                    Ok(_) => return,
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

    /// Hand this runtime's incidents to the next one (a resume): background
    /// persistence stops, then one more attempt for every incident not
    /// durable yet; the drafts returned are the next runtime's to persist.
    /// No attempt of this runtime starts afterwards, so none of them is
    /// raised by both runtimes.
    pub async fn hand_over(&self) -> Vec<IncidentDraft> {
        self.retired.store(true, Ordering::SeqCst);
        self.flush().await
    }

    /// One more persistence attempt for every incident not durable yet;
    /// returns the drafts that are still not durable (background retries
    /// go on). An attempt in flight finishes first: every occurrence is
    /// raised once.
    pub async fn flush(&self) -> Vec<IncidentDraft> {
        let _attempts = self.persist.lock().await;
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
    use storage::adapters::test_util::FaultBackend;
    use storage::{ArcStorageBackend, MemoryStorageBackend};

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
        assert_eq!(
            classify_source_exit("pg", Err(&err), false, 0),
            bind_epoch(raised.clone(), 0)
        );
        // Settled by a verified start: bound to the recovery epoch.
        assert_ne!(
            classify_source_exit("pg", Err(&err), false, 0).incident_id("p"),
            classify_source_exit("pg", Err(&err), false, 1).incident_id("p")
        );
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

    fn unc(epoch: u64) -> IncidentDraft {
        unclassified(
            Component::Source { id: "s".into() },
            CauseCode::SourceConnect,
            epoch,
        )
    }

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

    /// The occurrence counter of `d`'s incident in `store`.
    async fn occurrences(store: &IncidentStore, d: &IncidentDraft) -> u64 {
        store
            .get(&d.incident_id(store.pipeline()))
            .await
            .unwrap()
            .expect("the incident is recorded")
            .occurrences
    }

    /// A resume's flush never raises an occurrence whose background write
    /// already landed but is not yet marked durable: one failure, one count.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_flush_never_raises_an_occurrence_being_persisted() {
        let f = Arc::new(FaultBackend::new());
        let b: ArcStorageBackend = f.clone();
        let health = health_on(b.clone()).await;
        let store = IncidentStore::new(b, "p");
        // Raising a new incident lists the incidents twice: before its write
        // (the open-incident limit) and after it (metrics). Hold the
        // background write at the second listing: written, not yet durable.
        let arm = || {
            let reached = Arc::new(tokio::sync::Notify::new());
            let release = Arc::new(tokio::sync::Notify::new());
            *f.pause_after_slot_list.lock().unwrap() =
                Some((INCIDENTS_NS.into(), reached.clone(), release.clone()));
            (reached, release)
        };
        let (reached, release) = arm();
        health.exit(Task::Source, TaskExit::Failed(draft(1)));
        reached.notified().await;
        let (written, hold) = arm();
        release.notify_one();
        written.notified().await;
        assert_eq!(occurrences(&store, &draft(1)).await, 1);
        assert!(health.durability_pending(), "written, not marked durable");

        let flushing = tokio::spawn({
            let health = Arc::clone(&health);
            async move { health.flush().await }
        });
        // Long enough for a flush that does not wait to raise again.
        tokio::time::sleep(Duration::from_millis(300)).await;
        hold.notify_one();
        let still = flushing.await.unwrap();
        assert!(still.is_empty(), "durable: nothing carried");
        assert!(!health.durability_pending());
        assert_eq!(occurrences(&store, &draft(1)).await, 1, "counted once");
    }

    /// After a flush persisted an incident, the background retry that was
    /// waiting out its backoff does not raise it again.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_background_retry_after_a_flush_does_not_count_again() {
        let f = Arc::new(FaultBackend::new());
        let b: ArcStorageBackend = f.clone();
        let health = health_on(b.clone()).await;
        let store = IncidentStore::new(b, "p");
        f.fail_writes_to.lock().unwrap().push(INCIDENTS_NS.into());
        health.exit(Task::Source, TaskExit::Failed(draft(1)));
        // The first background attempt fails; the retry waits its backoff.
        tokio::time::sleep(PERSIST_RETRY_MIN / 2).await;
        f.fail_writes_to.lock().unwrap().clear();
        assert!(health.flush().await.is_empty());
        assert_eq!(occurrences(&store, &draft(1)).await, 1);
        // Past the backoff: the retry would have run.
        tokio::time::sleep(PERSIST_RETRY_MIN * 3).await;
        assert_eq!(occurrences(&store, &draft(1)).await, 1, "counted once");
    }

    /// A runtime that handed its incidents over (a resume) never raises
    /// them itself afterwards, even once the store accepts writes: the next
    /// runtime carries them, so none is counted by both.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_handed_over_incident_is_not_raised_by_the_old_runtime() {
        let f = Arc::new(FaultBackend::new());
        let b: ArcStorageBackend = f.clone();
        let old = health_on(b.clone()).await;
        let store = IncidentStore::new(b, "p");
        f.fail_writes_to.lock().unwrap().push(INCIDENTS_NS.into());
        old.exit(Task::Source, TaskExit::Failed(draft(1)));
        tokio::time::sleep(PERSIST_RETRY_MIN / 2).await;
        assert_eq!(old.hand_over().await, vec![draft(1)], "carried");
        f.fail_writes_to.lock().unwrap().clear();
        // Past the old runtime's backoff: its retry would have run.
        tokio::time::sleep(PERSIST_RETRY_MIN * 3).await;
        assert!(
            store
                .get(&draft(1).incident_id("p"))
                .await
                .unwrap()
                .is_none(),
            "only the next runtime raises it"
        );
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
