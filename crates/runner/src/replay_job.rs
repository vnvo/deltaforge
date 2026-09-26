//! Durable replay-job model and phase state machine (Event Replay, Slice 3 checkpoint a).
//!
//! A [`ReplayJob`] is the durable record of one in-progress replay for a pipeline: which
//! sinks it targets, the journal range it covers, its encoder schema policy, and its
//! progress cursor and phase. At most one ACTIVE (non-terminal) job may exist per pipeline
//! incarnation.
//!
//! The model owns its legal phase transitions and construction invariants; the store is
//! the authoritative persistence boundary and re-validates every write against the current
//! durable record (immutable identity/config, legal phase step or same-phase progress,
//! monotonic cursor, error/terminal invariants) so a caller cannot persist an illegal
//! record by hand-building one and calling CAS. Wiring a job into the coordinator (pause
//! derivation, handoff barrier) and the replay worker arrives in later checkpoints.

use std::collections::HashSet;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use storage::ArcStorageBackend;

use crate::replay_journal::REPLAY_NS;

/// Durable schema version of a persisted [`ReplayJob`] record. Distinct from the slot's
/// CAS version (which counts writes); this versions the record's shape so a load can fail
/// closed on an unsupported record rather than mis-deserializing.
pub const REPLAY_JOB_RECORD_VERSION: u16 = 1;

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
/// A dry-run job (which never pauses or delivers) instead completes directly from
/// `Running`, bypassing the quiesce/handoff phases. `Cancelled` and `Failed` are terminal
/// off-ramps. The handoff sequence `H` is carried in the phase itself so it is durable and
/// cannot drift between the quiesced and delivered phases.
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

    /// Whether this phase (ignoring dry-run) keeps the selected sinks paused. Callers
    /// should prefer [`ReplayJob::holds_pause`], which also accounts for dry-run.
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

/// Whether `from -> to` is a legal phase transition for a NON-dry-run job.
///
/// - No transition leaves a terminal phase.
/// - `Failed` is reachable from any non-terminal phase (any step may error out).
/// - `Cancelled` is reachable only before `LiveRestored` (once live is restored there is
///   nothing to cancel; a late finalization problem is a `Failed`, not a cancel).
/// - The forward path is strictly `Running -> CatchingUp -> HandoffQuiesced(H) ->
///   DeliveredThrough(H) -> LiveRestored -> Completed`, and `H` must match across the
///   quiesced and delivered phases.
///
/// Dry-run jobs use [`ReplayJob::is_legal_next_phase`], which completes directly from
/// `Running`.
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
    #[error("replay job immutable field changed: {field}")]
    ImmutableFieldChanged { field: &'static str },
    #[error(
        "replay job identity mismatch (record is {got}, store is for {expected})"
    )]
    IdentityMismatch { expected: String, got: String },
    #[error(
        "unsupported replay job record version {got} (expected {expected})"
    )]
    UnsupportedRecordVersion { got: u16, expected: u16 },
    #[error(
        "replay job has no target sinks (selected and staged are both empty)"
    )]
    NoTargets,
    #[error(
        "replay job sink sets are invalid: duplicate within, or overlap between, \
         selected and staged"
    )]
    OverlappingOrDuplicateSinks,
    #[error(
        "replay job range is invalid: through_seq {through} < from_seq {from}"
    )]
    InvalidRange { from: u64, through: u64 },
    #[error("a dry-run replay job cannot stage sinks for backfill")]
    DryRunWithStaged,
    #[error(
        "replay job error/terminal invariant violated (error is set iff phase is failed)"
    )]
    ErrorInvariant,
}

/// The durable record of one replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayJob {
    /// Record schema version; see [`REPLAY_JOB_RECORD_VERSION`].
    pub record_version: u16,
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
    /// pause a live sink or run a handoff.
    pub dry_run: bool,
    pub phase: ReplayPhase,
    /// Highest journal seq durably delivered so far. Resume re-reads from seq > `cursor`
    /// (at-least-once).
    pub cursor: u64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    /// Set when, and only when, `phase` is `Failed`.
    pub error: Option<String>,
}

impl ReplayJob {
    /// Construct a fresh job in `Running` with the cursor at `from_seq`, validating the
    /// construction invariants (targets, sink-set hygiene, range, dry-run/staged rules).
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
    ) -> Result<Self, ReplayJobError> {
        let job = Self {
            record_version: REPLAY_JOB_RECORD_VERSION,
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
        };
        job.validate()?;
        Ok(job)
    }

    /// Validate the record's construction invariants. Called by [`ReplayJob::new`] and,
    /// authoritatively, by [`ReplayJobStore::create`] (so a hand-built job is checked too).
    pub fn validate(&self) -> Result<(), ReplayJobError> {
        if self.record_version != REPLAY_JOB_RECORD_VERSION {
            return Err(ReplayJobError::UnsupportedRecordVersion {
                got: self.record_version,
                expected: REPLAY_JOB_RECORD_VERSION,
            });
        }
        if self.selected_sinks.is_empty() && self.staged_sinks.is_empty() {
            return Err(ReplayJobError::NoTargets);
        }
        if has_duplicates(&self.selected_sinks)
            || has_duplicates(&self.staged_sinks)
            || self
                .selected_sinks
                .iter()
                .any(|s| self.staged_sinks.contains(s))
        {
            return Err(ReplayJobError::OverlappingOrDuplicateSinks);
        }
        if let Some(through) = self.through_seq {
            if through < self.from_seq {
                return Err(ReplayJobError::InvalidRange {
                    from: self.from_seq,
                    through,
                });
            }
        }
        if self.dry_run && !self.staged_sinks.is_empty() {
            return Err(ReplayJobError::DryRunWithStaged);
        }
        self.check_error_invariant()?;
        Ok(())
    }

    /// A job is active while its phase is non-terminal.
    pub fn is_active(&self) -> bool {
        !self.phase.is_terminal()
    }

    /// Whether this job keeps its selected sinks paused: a non-dry-run job in a
    /// pre-`LiveRestored` phase. Dry-run jobs never pause.
    pub fn holds_pause(&self) -> bool {
        !self.dry_run && self.phase.holds_pause()
    }

    /// The lowest journal seq this job still needs retained, or `None` when the job is
    /// terminal (it then pins nothing). Resume reads seq > `cursor`, so the next needed seq
    /// is `cursor + 1`; retention must never remove `seq >= pin_seq` (C3).
    pub fn pin_seq(&self) -> Option<u64> {
        self.is_active().then(|| self.cursor.saturating_add(1))
    }

    /// Whether `to` is a legal next phase for THIS job, accounting for dry-run (which
    /// completes directly from `Running`, never through quiesce/handoff).
    pub fn is_legal_next_phase(&self, to: &ReplayPhase) -> bool {
        if self.phase.is_terminal() {
            return false;
        }
        if self.dry_run {
            matches!(
                to,
                ReplayPhase::Completed
                    | ReplayPhase::Cancelled
                    | ReplayPhase::Failed
            )
        } else {
            is_legal_transition(&self.phase, to)
        }
    }

    /// Return a copy transitioned to `to`, or an error if the transition is illegal.
    pub fn advance(
        &self,
        to: ReplayPhase,
        now_ms: i64,
    ) -> Result<Self, ReplayJobError> {
        if !self.is_legal_next_phase(&to) {
            return Err(ReplayJobError::IllegalTransition {
                from: self.phase.name().into(),
                to: to.name().into(),
            });
        }
        let mut next = self.clone();
        next.phase = to;
        next.error = None;
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

    /// `error` must be set exactly when the phase is `Failed`.
    fn check_error_invariant(&self) -> Result<(), ReplayJobError> {
        let is_failed = matches!(self.phase, ReplayPhase::Failed);
        if is_failed != self.error.is_some() {
            return Err(ReplayJobError::ErrorInvariant);
        }
        Ok(())
    }
}

/// Validate a proposed durable update against the current durable record: immutable
/// identity and configuration must be unchanged, the phase must be the same (a progress
/// update) or a legal transition, the cursor must not regress, and the error/terminal
/// invariant must hold. This is the persistence boundary's guard against illegal writes.
fn validate_durable_update(
    current: &ReplayJob,
    proposed: &ReplayJob,
) -> Result<(), ReplayJobError> {
    macro_rules! immutable {
        ($field:ident) => {
            if current.$field != proposed.$field {
                return Err(ReplayJobError::ImmutableFieldChanged {
                    field: stringify!($field),
                });
            }
        };
    }
    immutable!(record_version);
    immutable!(job_id);
    immutable!(pipeline);
    immutable!(incarnation);
    immutable!(selected_sinks);
    immutable!(staged_sinks);
    immutable!(from_seq);
    immutable!(through_seq);
    immutable!(encoder_schema_policy);
    immutable!(dry_run);
    immutable!(created_at_ms);

    if proposed.phase != current.phase
        && !current.is_legal_next_phase(&proposed.phase)
    {
        return Err(ReplayJobError::IllegalTransition {
            from: current.phase.name().into(),
            to: proposed.phase.name().into(),
        });
    }
    if proposed.cursor < current.cursor {
        return Err(ReplayJobError::CursorRegression {
            from: current.cursor,
            to: proposed.cursor,
        });
    }
    proposed.check_error_invariant()?;
    Ok(())
}

fn has_duplicates(v: &[String]) -> bool {
    let mut seen = HashSet::with_capacity(v.len());
    !v.iter().all(|s| seen.insert(s))
}

/// A job as read back from its slot, with the slot's CAS version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredReplayJob {
    pub version: u64,
    pub job: ReplayJob,
}

/// Slot-backed persistence for the single per-pipeline-incarnation replay job, and the
/// authoritative validation boundary: every write is checked against the store's identity
/// and the current durable record before it is committed.
pub struct ReplayJobStore {
    backend: ArcStorageBackend,
    pipeline: String,
    incarnation: String,
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
            pipeline: pipeline.to_string(),
            incarnation: incarnation.to_string(),
            key: replay_job_key(pipeline, incarnation),
        }
    }

    /// The record's version and identity must match the store; used on every load and
    /// write so a corrupt or foreign record fails closed rather than being trusted.
    fn check_identity(&self, job: &ReplayJob) -> Result<(), ReplayJobError> {
        if job.record_version != REPLAY_JOB_RECORD_VERSION {
            return Err(ReplayJobError::UnsupportedRecordVersion {
                got: job.record_version,
                expected: REPLAY_JOB_RECORD_VERSION,
            });
        }
        if job.pipeline != self.pipeline || job.incarnation != self.incarnation
        {
            return Err(ReplayJobError::IdentityMismatch {
                expected: format!("{}:{}", self.pipeline, self.incarnation),
                got: format!("{}:{}", job.pipeline, job.incarnation),
            });
        }
        Ok(())
    }

    /// Load the current job and its CAS version, or `None` when no job exists. Fails closed
    /// on an unsupported record version or an identity that does not match the store key.
    pub async fn get(&self) -> Result<Option<StoredReplayJob>> {
        match self.backend.slot_get(REPLAY_NS, &self.key).await? {
            Some((version, bytes)) => {
                let job: ReplayJob = serde_json::from_slice(&bytes)?;
                self.check_identity(&job)?;
                Ok(Some(StoredReplayJob { version, job }))
            }
            None => Ok(None),
        }
    }

    /// Create a new job. Fails with [`ReplayJobError::AlreadyActive`] when a non-terminal
    /// job already exists; a terminal record (completed/cancelled/failed) is replaced so a
    /// pipeline can start a fresh replay after the previous one finished. Validates the
    /// job's identity and construction invariants first.
    pub async fn create(&self, job: &ReplayJob) -> Result<StoredReplayJob> {
        self.check_identity(job)?;
        job.validate()?;
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

    /// Persist `job` if the slot is still at `expected_version`, after validating the write
    /// against the current durable record (immutable identity/config, legal phase step or
    /// same-phase progress, monotonic cursor, error/terminal invariant). Returns the new
    /// stored job or an error: [`ReplayJobError::VersionConflict`] on a stale version, or a
    /// specific invariant error on an illegal write.
    pub async fn compare_and_set(
        &self,
        expected_version: u64,
        job: &ReplayJob,
    ) -> Result<StoredReplayJob> {
        self.check_identity(job)?;
        let current = self.get().await?.ok_or_else(|| {
            anyhow::anyhow!("no replay job to update (slot is empty)")
        })?;
        if current.version != expected_version {
            return Err(ReplayJobError::VersionConflict {
                expected: expected_version,
            }
            .into());
        }
        validate_durable_update(&current.job, job)?;
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
        .unwrap()
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
        assert!(!is_legal_transition(
            &ReplayPhase::Running,
            &ReplayPhase::HandoffQuiesced { handoff_seq: 1 }
        ));
        assert!(!is_legal_transition(
            &ReplayPhase::CatchingUp,
            &ReplayPhase::LiveRestored
        ));
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
    fn dry_run_completes_directly_and_never_pauses() {
        let dry = ReplayJob::new(
            "job-dry",
            "p",
            "inc-1",
            vec!["kafka".into()],
            vec![],
            0,
            None,
            EncoderSchemaPolicy::Current,
            true,
            1_000,
        )
        .unwrap();
        assert!(!dry.holds_pause(), "dry-run never pauses live sinks");
        // Completes directly from Running, bypassing quiesce/handoff.
        assert!(dry.is_legal_next_phase(&ReplayPhase::Completed));
        assert!(!dry.is_legal_next_phase(&ReplayPhase::CatchingUp));
        assert!(!dry.is_legal_next_phase(&ReplayPhase::HandoffQuiesced {
            handoff_seq: 1
        }));
        let done = dry.advance(ReplayPhase::Completed, 2_000).unwrap();
        assert_eq!(done.phase, ReplayPhase::Completed);
    }

    #[test]
    fn non_dry_run_running_holds_pause() {
        assert!(job().holds_pause());
    }

    #[test]
    fn construction_rejects_invalid_jobs() {
        let mk = |selected: Vec<String>,
                  staged: Vec<String>,
                  through: Option<u64>,
                  dry: bool| {
            ReplayJob::new(
                "j",
                "p",
                "inc-1",
                selected,
                staged,
                10,
                through,
                EncoderSchemaPolicy::Current,
                dry,
                1_000,
            )
        };
        assert!(matches!(
            mk(vec![], vec![], None, false),
            Err(ReplayJobError::NoTargets)
        ));
        assert!(matches!(
            mk(vec!["a".into(), "a".into()], vec![], None, false),
            Err(ReplayJobError::OverlappingOrDuplicateSinks)
        ));
        assert!(matches!(
            mk(vec!["a".into()], vec!["a".into()], None, false),
            Err(ReplayJobError::OverlappingOrDuplicateSinks)
        ));
        assert!(matches!(
            mk(vec!["a".into()], vec![], Some(5), false),
            Err(ReplayJobError::InvalidRange {
                from: 10,
                through: 5
            })
        ));
        assert!(matches!(
            mk(vec!["a".into()], vec!["b".into()], None, true),
            Err(ReplayJobError::DryRunWithStaged)
        ));
        // through == from is valid.
        assert!(mk(vec!["a".into()], vec![], Some(10), false).is_ok());
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
        failed.check_error_invariant().unwrap();
    }

    #[test]
    fn cursor_advances_forward_only() {
        let j = job();
        let moved = j.with_cursor(15, 2_000).unwrap();
        assert_eq!(moved.cursor, 15);
        assert!(matches!(
            moved.with_cursor(14, 2_000),
            Err(ReplayJobError::CursorRegression { from: 15, to: 14 })
        ));
        assert_eq!(moved.with_cursor(15, 2_500).unwrap().cursor, 15);
    }

    #[test]
    fn pin_seq_is_next_needed_seq_while_active_and_none_when_terminal() {
        let j = job(); // cursor starts at from_seq = 10
        assert_eq!(j.pin_seq(), Some(11));
        let moved = j.with_cursor(20, 2_000).unwrap();
        assert_eq!(moved.pin_seq(), Some(21));
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
        let cancelled =
            created.job.advance(ReplayPhase::Cancelled, 5_000).unwrap();
        store
            .compare_and_set(created.version, &cancelled)
            .await
            .unwrap();
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

    #[tokio::test]
    async fn compare_and_set_enforces_the_state_machine() {
        let be: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
        let store = ReplayJobStore::new(be, "p", "inc-1");
        let created = store.create(&job()).await.unwrap();
        let v = created.version;

        // Illegal phase skip (Running -> LiveRestored).
        let mut skip = created.job.clone();
        skip.phase = ReplayPhase::LiveRestored;
        assert!(matches!(
            store
                .compare_and_set(v, &skip)
                .await
                .unwrap_err()
                .downcast_ref::<ReplayJobError>(),
            Some(ReplayJobError::IllegalTransition { .. })
        ));

        // Immutable range change.
        let mut range = created.job.clone();
        range.from_seq = 0;
        range.cursor = 0;
        assert!(matches!(
            store
                .compare_and_set(v, &range)
                .await
                .unwrap_err()
                .downcast_ref::<ReplayJobError>(),
            Some(ReplayJobError::ImmutableFieldChanged { .. })
        ));

        // Immutable identity change (job_id).
        let mut ident = created.job.clone();
        ident.job_id = "other".into();
        assert!(matches!(
            store
                .compare_and_set(v, &ident)
                .await
                .unwrap_err()
                .downcast_ref::<ReplayJobError>(),
            Some(ReplayJobError::ImmutableFieldChanged { .. })
        ));

        // Cursor regression bypassing with_cursor (from_seq and cursor both start at 10).
        let mut back = created.job.clone();
        back.cursor = 9;
        assert!(matches!(
            store
                .compare_and_set(v, &back)
                .await
                .unwrap_err()
                .downcast_ref::<ReplayJobError>(),
            Some(ReplayJobError::CursorRegression { .. })
        ));

        // A legal progress update still works.
        let ok = created.job.with_cursor(12, 2_000).unwrap();
        assert!(store.compare_and_set(v, &ok).await.is_ok());
    }

    #[tokio::test]
    async fn get_and_create_fail_closed_on_foreign_identity() {
        let be: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
        let store = ReplayJobStore::new(be.clone(), "p", "inc-1");
        // A job built for a different incarnation cannot be created here.
        let mut foreign = job();
        foreign.incarnation = "inc-2".into();
        assert!(matches!(
            store
                .create(&foreign)
                .await
                .unwrap_err()
                .downcast_ref::<ReplayJobError>(),
            Some(ReplayJobError::IdentityMismatch { .. })
        ));

        // A record written directly under the key with a foreign identity fails the load.
        let bytes = serde_json::to_vec(&foreign).unwrap();
        be.slot_upsert(REPLAY_NS, &replay_job_key("p", "inc-1"), &bytes)
            .await
            .unwrap();
        assert!(matches!(
            store
                .get()
                .await
                .unwrap_err()
                .downcast_ref::<ReplayJobError>(),
            Some(ReplayJobError::IdentityMismatch { .. })
        ));
    }

    #[tokio::test]
    async fn get_fails_closed_on_unsupported_record_version() {
        let be: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
        let store = ReplayJobStore::new(be.clone(), "p", "inc-1");
        let mut future = job();
        future.record_version = REPLAY_JOB_RECORD_VERSION + 1;
        let bytes = serde_json::to_vec(&future).unwrap();
        be.slot_upsert(REPLAY_NS, &replay_job_key("p", "inc-1"), &bytes)
            .await
            .unwrap();
        assert!(matches!(
            store
                .get()
                .await
                .unwrap_err()
                .downcast_ref::<ReplayJobError>(),
            Some(ReplayJobError::UnsupportedRecordVersion { .. })
        ));
    }
}
