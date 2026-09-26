//! Durable replay-job model and phase state machine (Event Replay, Slice 3 checkpoint a).
//!
//! A [`ReplayJob`] is the durable record of one in-progress replay for a pipeline: which
//! sinks it targets, the journal range it covers, its encoder schema policy, and its
//! progress cursor and phase. At most one ACTIVE (non-terminal) job may exist per pipeline
//! incarnation.
//!
//! This module owns only the model, its legal phase transitions, and slot-backed
//! persistence. Wiring a job into the coordinator (pause derivation, handoff barrier) and
//! the replay worker that reads the journal and delivers to sinks arrives in later
//! checkpoints; the fields and phases here are the contract those pieces build on.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use storage::ArcStorageBackend;

use crate::replay_journal::REPLAY_NS;

/// The replay-job slot key, incarnation-scoped so a recreated pipeline never inherits an
/// old pipeline's job (mirrors the replay stream key).
pub fn replay_job_key(pipeline: &str, incarnation: &str) -> String {
    format!("{pipeline}:{incarnation}:replayjob")
}

/// How the target encoder resolves its schema during replay. Replay never re-decodes
/// source bytes (events are stored already decoded), so this is the only schema resolution
/// that affects replay output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(tag = "policy", rename_all = "snake_case")]
pub enum EncoderSchemaPolicy {
    /// Resolve against the registry's CURRENT schema. This is how replay repairs consumer
    /// bugs against the latest schema, and the default.
    #[default]
    Current,
    /// Resolve against the schema-registry sequence captured with each envelope
    /// (`SchemaBinding::registry_seq_at_capture`). Requires that sequence to have been
    /// bound at capture time; it is currently always absent (the registry handle is not
    /// yet threaded into capture), so a job requesting this policy is rejected at start
    /// until capture records a real sequence.
    AtCaptureSeq,
    /// Resolve against one explicit, pinned registry sequence for the whole job.
    Pinned { seq: u64 },
}

/// The durable phase of a replay job. Canonical progression:
///
/// ```text
/// Running -> CatchingUp -> HandoffQuiesced(H) -> DeliveredThrough(H) -> LiveRestored
///         -> Completed
/// ```
///
/// `Cancelled` and `Failed` are terminal off-ramps. The handoff sequence `H` is carried in
/// the phase itself so it is durable and cannot drift between the quiesced and delivered
/// phases.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum ReplayPhase {
    /// Historical delivery of the requested range from the journal to the selected sinks
    /// (sole writer; live delivery to those sinks paused).
    Running,
    /// Delivering envelopes captured after the job started, toward the tail.
    CatchingUp,
    /// Ingestion briefly quiesced so nothing is captured beyond tail seq `H`.
    HandoffQuiesced { handoff_seq: u64 },
    /// The worker has delivered the selected sinks through `H`.
    DeliveredThrough { handoff_seq: u64 },
    /// Selected sinks returned to the live set; live delivery resumes strictly after `H`.
    LiveRestored,
    /// Terminal: the replay finished and live delivery is fully restored.
    Completed,
    /// Terminal: cancelled before live restoration; pauses released, no handoff.
    Cancelled,
    /// Terminal: failed; `ReplayJob::error` carries the reason.
    Failed,
}

impl ReplayPhase {
    /// A stable, lower-case name for logs, metrics, and error messages.
    pub fn name(&self) -> &'static str {
        match self {
            ReplayPhase::Running => "running",
            ReplayPhase::CatchingUp => "catching_up",
            ReplayPhase::HandoffQuiesced { .. } => "handoff_quiesced",
            ReplayPhase::DeliveredThrough { .. } => "delivered_through",
            ReplayPhase::LiveRestored => "live_restored",
            ReplayPhase::Completed => "completed",
            ReplayPhase::Cancelled => "cancelled",
            ReplayPhase::Failed => "failed",
        }
    }

    /// A terminal phase accepts no further transitions.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            ReplayPhase::Completed
                | ReplayPhase::Cancelled
                | ReplayPhase::Failed
        )
    }

    /// While in a pre-`LiveRestored` phase the job holds its selected sinks paused (out of
    /// the live delivery set). The startup barrier and pause derivation (later checkpoints)
    /// use this to reinstall pauses before the source starts.
    pub fn holds_pause(&self) -> bool {
        matches!(
            self,
            ReplayPhase::Running
                | ReplayPhase::CatchingUp
                | ReplayPhase::HandoffQuiesced { .. }
                | ReplayPhase::DeliveredThrough { .. }
        )
    }
}

impl std::fmt::Display for ReplayPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// Whether `from -> to` is a legal phase transition.
///
/// - No transition leaves a terminal phase.
/// - `Failed` is reachable from any non-terminal phase (any step may error out).
/// - `Cancelled` is reachable only before `LiveRestored` (once live is restored there is
///   nothing to cancel; a late finalization problem is a `Failed`, not a cancel).
/// - The forward path is strictly `Running -> CatchingUp -> HandoffQuiesced(H) ->
///   DeliveredThrough(H) -> LiveRestored -> Completed`, and `H` must match across the
///   quiesced and delivered phases.
pub fn is_legal_transition(from: &ReplayPhase, to: &ReplayPhase) -> bool {
    use ReplayPhase::*;
    if from.is_terminal() {
        return false;
    }
    match to {
        Failed => true,
        Cancelled => !matches!(from, LiveRestored),
        _ => {
            matches!(
                (from, to),
                (Running, CatchingUp)
                    | (CatchingUp, HandoffQuiesced { .. })
                    | (DeliveredThrough { .. }, LiveRestored)
                    | (LiveRestored, Completed)
            ) || matches!(
                (from, to),
                (HandoffQuiesced { handoff_seq: h }, DeliveredThrough { handoff_seq: h2 }) if h == h2
            )
        }
    }
}

/// Errors from the replay-job model and its store.
#[derive(Debug, thiserror::Error)]
pub enum ReplayJobError {
    #[error("illegal replay-job phase transition from {from} to {to}")]
    IllegalTransition { from: String, to: String },
    #[error(
        "a replay job is already active for this pipeline (job {job_id}, phase {phase})"
    )]
    AlreadyActive { job_id: String, phase: String },
    #[error("replay job store version conflict (expected version {expected})")]
    VersionConflict { expected: u64 },
    #[error("replay job cursor cannot move backward (from {from} to {to})")]
    CursorRegression { from: u64, to: u64 },
}

/// The durable record of one replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayJob {
    pub job_id: String,
    pub pipeline: String,
    /// The pipeline incarnation whose replay stream this job reads. Ties the job to one
    /// stream, so a delete + recreate never resumes an old job against a new stream.
    pub incarnation: String,
    /// Existing LIVE sinks to pause and replay the range to.
    pub selected_sinks: Vec<String>,
    /// New sinks being backfilled: eligible to receive replay but not in the live delivery
    /// set (and excluded from commit policy) until the handoff barrier promotes them.
    pub staged_sinks: Vec<String>,
    /// Lower bound of the range, in `log_since` semantics (the worker reads envelopes with
    /// seq > `from_seq`). Must be >= the stream's `min_valid_from_seq` at start.
    pub from_seq: u64,
    /// Optional inclusive upper bound of the requested historical range. `None` = to tail.
    pub through_seq: Option<u64>,
    pub encoder_schema_policy: EncoderSchemaPolicy,
    /// Dry-run jobs deliver nothing (report count/range/targets only) and therefore never
    /// pause a live sink.
    pub dry_run: bool,
    pub phase: ReplayPhase,
    /// Highest journal seq durably delivered so far. Resume re-reads from here
    /// (at-least-once); retention is pinned so entries beyond it are never truncated.
    pub cursor: u64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    /// Set when `phase` is `Failed`.
    pub error: Option<String>,
}

impl ReplayJob {
    /// Create a fresh job in `Running` with the cursor at `from_seq` (nothing delivered
    /// beyond the lower bound yet).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        job_id: impl Into<String>,
        pipeline: impl Into<String>,
        incarnation: impl Into<String>,
        selected_sinks: Vec<String>,
        staged_sinks: Vec<String>,
        from_seq: u64,
        through_seq: Option<u64>,
        encoder_schema_policy: EncoderSchemaPolicy,
        dry_run: bool,
        now_ms: i64,
    ) -> Self {
        Self {
            job_id: job_id.into(),
            pipeline: pipeline.into(),
            incarnation: incarnation.into(),
            selected_sinks,
            staged_sinks,
            from_seq,
            through_seq,
            encoder_schema_policy,
            dry_run,
            phase: ReplayPhase::Running,
            cursor: from_seq,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
            error: None,
        }
    }

    /// A job is active while its phase is non-terminal.
    pub fn is_active(&self) -> bool {
        !self.phase.is_terminal()
    }

    /// The lowest journal seq this job still needs retained, or `None` when the job is
    /// terminal (it then pins nothing). Retention must never remove `seq >= pin_seq`.
    pub fn pin_seq(&self) -> Option<u64> {
        self.is_active().then_some(self.cursor)
    }

    /// Return a copy transitioned to `to`, or an error if the transition is illegal.
    pub fn advance(
        &self,
        to: ReplayPhase,
        now_ms: i64,
    ) -> Result<Self, ReplayJobError> {
        if !is_legal_transition(&self.phase, &to) {
            return Err(ReplayJobError::IllegalTransition {
                from: self.phase.name().into(),
                to: to.name().into(),
            });
        }
        let mut next = self.clone();
        next.phase = to;
        next.updated_at_ms = now_ms;
        Ok(next)
    }

    /// Transition to `Failed`, recording `reason`.
    pub fn fail(
        &self,
        reason: impl Into<String>,
        now_ms: i64,
    ) -> Result<Self, ReplayJobError> {
        let mut next = self.advance(ReplayPhase::Failed, now_ms)?;
        next.error = Some(reason.into());
        Ok(next)
    }

    /// Return a copy with the progress cursor advanced to `cursor` (never backward).
    pub fn with_cursor(
        &self,
        cursor: u64,
        now_ms: i64,
    ) -> Result<Self, ReplayJobError> {
        if cursor < self.cursor {
            return Err(ReplayJobError::CursorRegression {
                from: self.cursor,
                to: cursor,
            });
        }
        let mut next = self.clone();
        next.cursor = cursor;
        next.updated_at_ms = now_ms;
        Ok(next)
    }
}

/// A job as read back from its slot, with the slot's CAS version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredReplayJob {
    pub version: u64,
    pub job: ReplayJob,
}

/// Slot-backed persistence for the single per-pipeline-incarnation replay job.
pub struct ReplayJobStore {
    backend: ArcStorageBackend,
    key: String,
}

impl ReplayJobStore {
    pub fn new(
        backend: ArcStorageBackend,
        pipeline: &str,
        incarnation: &str,
    ) -> Self {
        Self {
            backend,
            key: replay_job_key(pipeline, incarnation),
        }
    }

    /// Load the current job and its CAS version, or `None` when no job exists.
    pub async fn get(&self) -> Result<Option<StoredReplayJob>> {
        match self.backend.slot_get(REPLAY_NS, &self.key).await? {
            Some((version, bytes)) => {
                let job = serde_json::from_slice(&bytes)?;
                Ok(Some(StoredReplayJob { version, job }))
            }
            None => Ok(None),
        }
    }

    /// Create a new job. Fails with [`ReplayJobError::AlreadyActive`] when a non-terminal
    /// job already exists; a terminal record (completed/cancelled/failed) is replaced so a
    /// pipeline can start a fresh replay after the previous one finished.
    pub async fn create(&self, job: &ReplayJob) -> Result<StoredReplayJob> {
        let bytes = serde_json::to_vec(job)?;
        if let Some(version) = self
            .backend
            .slot_create(REPLAY_NS, &self.key, &bytes)
            .await?
        {
            return Ok(StoredReplayJob {
                version,
                job: job.clone(),
            });
        }
        // The slot already holds a job. Only a terminal one may be replaced.
        let existing = self
            .get()
            .await?
            .expect("replay job slot exists after slot_create returned None");
        if existing.job.is_active() {
            return Err(ReplayJobError::AlreadyActive {
                job_id: existing.job.job_id,
                phase: existing.job.phase.name().into(),
            }
            .into());
        }
        if !self
            .backend
            .slot_cas(REPLAY_NS, &self.key, existing.version, &bytes)
            .await?
        {
            return Err(ReplayJobError::VersionConflict {
                expected: existing.version,
            }
            .into());
        }
        self.get()
            .await?
            .ok_or_else(|| anyhow::anyhow!("replay job vanished after replace"))
    }

    /// Persist `job` if the slot is still at `expected_version`. Returns the new stored
    /// job (with its bumped version) or [`ReplayJobError::VersionConflict`] on a stale
    /// version (a concurrent writer moved the job first).
    pub async fn compare_and_set(
        &self,
        expected_version: u64,
        job: &ReplayJob,
    ) -> Result<StoredReplayJob> {
        let bytes = serde_json::to_vec(job)?;
        if !self
            .backend
            .slot_cas(REPLAY_NS, &self.key, expected_version, &bytes)
            .await?
        {
            return Err(ReplayJobError::VersionConflict {
                expected: expected_version,
            }
            .into());
        }
        self.get().await?.ok_or_else(|| {
            anyhow::anyhow!("replay job vanished after compare_and_set")
        })
    }

    /// Remove the job record. Returns whether a record existed.
    pub async fn delete(&self) -> Result<bool> {
        self.backend.slot_delete(REPLAY_NS, &self.key).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use storage::MemoryStorageBackend;

    fn job() -> ReplayJob {
        ReplayJob::new(
            "job-1",
            "p",
            "inc-1",
            vec!["kafka".into()],
            vec![],
            10,
            None,
            EncoderSchemaPolicy::Current,
            false,
            1_000,
        )
    }

    #[test]
    fn canonical_path_is_legal_and_shortcuts_are_not() {
        let steps = [
            ReplayPhase::Running,
            ReplayPhase::CatchingUp,
            ReplayPhase::HandoffQuiesced { handoff_seq: 42 },
            ReplayPhase::DeliveredThrough { handoff_seq: 42 },
            ReplayPhase::LiveRestored,
            ReplayPhase::Completed,
        ];
        for pair in steps.windows(2) {
            assert!(
                is_legal_transition(&pair[0], &pair[1]),
                "{} -> {} should be legal",
                pair[0],
                pair[1]
            );
        }
        // Skipping a phase is illegal.
        assert!(!is_legal_transition(
            &ReplayPhase::Running,
            &ReplayPhase::HandoffQuiesced { handoff_seq: 1 }
        ));
        assert!(!is_legal_transition(
            &ReplayPhase::CatchingUp,
            &ReplayPhase::LiveRestored
        ));
        // No going backward.
        assert!(!is_legal_transition(
            &ReplayPhase::LiveRestored,
            &ReplayPhase::Running
        ));
    }

    #[test]
    fn handoff_seq_must_match_across_quiesced_and_delivered() {
        assert!(is_legal_transition(
            &ReplayPhase::HandoffQuiesced { handoff_seq: 7 },
            &ReplayPhase::DeliveredThrough { handoff_seq: 7 }
        ));
        assert!(!is_legal_transition(
            &ReplayPhase::HandoffQuiesced { handoff_seq: 7 },
            &ReplayPhase::DeliveredThrough { handoff_seq: 8 }
        ));
    }

    #[test]
    fn terminal_phases_reject_all_transitions() {
        for terminal in [
            ReplayPhase::Completed,
            ReplayPhase::Cancelled,
            ReplayPhase::Failed,
        ] {
            for to in [
                ReplayPhase::Running,
                ReplayPhase::Completed,
                ReplayPhase::Failed,
                ReplayPhase::Cancelled,
            ] {
                assert!(
                    !is_legal_transition(&terminal, &to),
                    "{terminal} -> {to} must be rejected"
                );
            }
        }
    }

    #[test]
    fn cancel_allowed_before_live_restored_not_after() {
        for from in [
            ReplayPhase::Running,
            ReplayPhase::CatchingUp,
            ReplayPhase::HandoffQuiesced { handoff_seq: 1 },
            ReplayPhase::DeliveredThrough { handoff_seq: 1 },
        ] {
            assert!(is_legal_transition(&from, &ReplayPhase::Cancelled));
        }
        assert!(!is_legal_transition(
            &ReplayPhase::LiveRestored,
            &ReplayPhase::Cancelled
        ));
    }

    #[test]
    fn fail_allowed_from_any_nonterminal_phase() {
        for from in [
            ReplayPhase::Running,
            ReplayPhase::CatchingUp,
            ReplayPhase::HandoffQuiesced { handoff_seq: 1 },
            ReplayPhase::DeliveredThrough { handoff_seq: 1 },
            ReplayPhase::LiveRestored,
        ] {
            assert!(is_legal_transition(&from, &ReplayPhase::Failed));
        }
    }

    #[test]
    fn advance_and_fail_set_phase_and_error() {
        let j = job();
        let caught = j.advance(ReplayPhase::CatchingUp, 2_000).unwrap();
        assert_eq!(caught.phase, ReplayPhase::CatchingUp);
        assert_eq!(caught.updated_at_ms, 2_000);
        assert!(j.advance(ReplayPhase::LiveRestored, 2_000).is_err());

        let failed = caught.fail("sink exploded", 3_000).unwrap();
        assert_eq!(failed.phase, ReplayPhase::Failed);
        assert_eq!(failed.error.as_deref(), Some("sink exploded"));
    }

    #[test]
    fn cursor_advances_forward_only() {
        let j = job(); // cursor starts at from_seq = 10
        let moved = j.with_cursor(15, 2_000).unwrap();
        assert_eq!(moved.cursor, 15);
        assert!(matches!(
            moved.with_cursor(14, 2_000),
            Err(ReplayJobError::CursorRegression { from: 15, to: 14 })
        ));
        // Same position is allowed (idempotent re-record).
        assert_eq!(moved.with_cursor(15, 2_500).unwrap().cursor, 15);
    }

    #[test]
    fn pin_seq_tracks_cursor_while_active_and_is_none_when_terminal() {
        let j = job();
        assert_eq!(j.pin_seq(), Some(10));
        let moved = j.with_cursor(20, 2_000).unwrap();
        assert_eq!(moved.pin_seq(), Some(20));
        let done = moved
            .advance(ReplayPhase::CatchingUp, 2_100)
            .unwrap()
            .advance(ReplayPhase::HandoffQuiesced { handoff_seq: 20 }, 2_200)
            .unwrap()
            .advance(ReplayPhase::DeliveredThrough { handoff_seq: 20 }, 2_300)
            .unwrap()
            .advance(ReplayPhase::LiveRestored, 2_400)
            .unwrap()
            .advance(ReplayPhase::Completed, 2_500)
            .unwrap();
        assert_eq!(done.pin_seq(), None);
    }

    #[test]
    fn job_serde_roundtrips() {
        let j = job()
            .advance(ReplayPhase::CatchingUp, 2_000)
            .unwrap()
            .advance(ReplayPhase::HandoffQuiesced { handoff_seq: 99 }, 2_100)
            .unwrap();
        let bytes = serde_json::to_vec(&j).unwrap();
        let back: ReplayJob = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(j, back);
    }

    #[tokio::test]
    async fn store_roundtrips_and_get_none_when_absent() {
        let be: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
        let store = ReplayJobStore::new(be, "p", "inc-1");
        assert!(store.get().await.unwrap().is_none());
        let created = store.create(&job()).await.unwrap();
        let loaded = store.get().await.unwrap().unwrap();
        assert_eq!(loaded.job, job());
        assert_eq!(loaded.version, created.version);
    }

    #[tokio::test]
    async fn create_rejects_a_second_active_job() {
        let be: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
        let store = ReplayJobStore::new(be, "p", "inc-1");
        store.create(&job()).await.unwrap();
        let mut second = job();
        second.job_id = "job-2".into();
        let err = store.create(&second).await.unwrap_err();
        assert!(
            err.downcast_ref::<ReplayJobError>()
                .is_some_and(|e| matches!(
                    e,
                    ReplayJobError::AlreadyActive { .. }
                )),
            "expected AlreadyActive, got: {err}"
        );
    }

    #[tokio::test]
    async fn create_replaces_a_terminal_job() {
        let be: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
        let store = ReplayJobStore::new(be, "p", "inc-1");
        let created = store.create(&job()).await.unwrap();
        // Drive the first job to a terminal phase and persist it.
        let cancelled =
            created.job.advance(ReplayPhase::Cancelled, 5_000).unwrap();
        store
            .compare_and_set(created.version, &cancelled)
            .await
            .unwrap();
        // A fresh job may now take the slot.
        let mut fresh = job();
        fresh.job_id = "job-2".into();
        let replaced = store.create(&fresh).await.unwrap();
        assert_eq!(replaced.job.job_id, "job-2");
        assert_eq!(replaced.job.phase, ReplayPhase::Running);
    }

    #[tokio::test]
    async fn compare_and_set_conflicts_on_stale_version() {
        let be: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
        let store = ReplayJobStore::new(be, "p", "inc-1");
        let created = store.create(&job()).await.unwrap();
        let moved = created.job.with_cursor(20, 2_000).unwrap();
        let after = store
            .compare_and_set(created.version, &moved)
            .await
            .unwrap();
        assert!(after.version > created.version);
        // The stale version no longer wins.
        let err = store
            .compare_and_set(created.version, &moved)
            .await
            .unwrap_err();
        assert!(
            err.downcast_ref::<ReplayJobError>()
                .is_some_and(|e| matches!(
                    e,
                    ReplayJobError::VersionConflict { .. }
                )),
            "expected VersionConflict, got: {err}"
        );
    }
}
