//! Durable recovery operations (`docs/design/recovery-cli.md`).
//!
//! One operation per pipeline at a time, in the slot `recovery/{pipeline}`:
//! claimed by create (or CAS from a completed record), advanced step by step
//! by CAS, and completed only after its audit is durable. Every step names
//! the exact pre-state it changes and the post-state it leaves, as digests:
//! a step finds either its pre-state (performed now) or its post-state
//! (already done, skipped); anything else stops the operation with the
//! record kept as evidence. A resumed operation runs the plan stored in its
//! record, never a recomputed one, and there is no abandonment.
//!
//! The canonical plan is hashed into the operation's proof: fixed field
//! order, sorted maps, strings and integers only.

use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::CorruptRecord;
use super::incidents::{
    IncidentStore, LifecycleError, RecoveryAudit, now_ms, segment,
};
use crate::ArcStorageBackend;
use deltaforge_core::incident::IncidentId;

/// Slot namespace of the per-pipeline recovery record.
pub const RECOVERY_NS: &str = "recovery";
/// Log namespace of the per-pipeline recovery audit trail.
pub const RECOVERY_AUDIT_NS: &str = "recovery.audit";
/// The domain of the canonical plan (hashed into every proof).
pub const PLAN_DOMAIN: &str = "DeltaForge.Recovery.Plan.v1";
pub const PLAN_FORMAT: u32 = 1;
pub const RECORD_FORMAT: u32 = 1;

/// The SHA-256 of `bytes`, hex.
pub fn digest_bytes(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// The digest of a value's canonical JSON (its fields in declaration
/// order, maps sorted).
pub fn digest_json<T: Serialize>(value: &T) -> Result<String> {
    Ok(digest_bytes(&serde_json::to_vec(value)?))
}

/// One write of a plan: the exact state it changes and leaves.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanStep {
    /// What the step does (an operation-defined name).
    pub name: String,
    /// The digest of the state the step changes.
    pub pre: String,
    /// The digest of the state the step leaves.
    pub post: String,
    /// What the step writes, for the operator (bound into the proof).
    pub detail: BTreeMap<String, String>,
}

/// The canonical plan of one operation. Its digest is the proof.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalPlan {
    pub format: u32,
    pub domain: String,
    pub operation: String,
    pub pipeline: String,
    pub source: String,
    /// Everything the plan was computed from: a change refuses the apply.
    pub bindings: BTreeMap<String, String>,
    pub steps: Vec<PlanStep>,
    pub consequences: Vec<String>,
}

impl CanonicalPlan {
    pub fn new(
        operation: &str,
        pipeline: &str,
        source: &str,
        bindings: BTreeMap<String, String>,
        steps: Vec<PlanStep>,
        consequences: Vec<String>,
    ) -> Self {
        Self {
            format: PLAN_FORMAT,
            domain: PLAN_DOMAIN.into(),
            operation: operation.into(),
            pipeline: pipeline.into(),
            source: source.into(),
            bindings,
            steps,
            consequences,
        }
    }

    /// The proof: the SHA-256 of the canonical plan.
    pub fn proof(&self) -> String {
        digest_json(self).expect("a canonical plan serializes")
    }
}

/// The placeholder of an identifier a plan creates (for example a new
/// snapshot or continuity chain), in the plan its [`plan_seed`] is taken
/// over.
pub const PLANNED_ID: &str = "planned-identifier";

/// The seed of the identifiers a plan creates: the digest, under its own
/// domain, of the canonical plan in which each such identifier is
/// [`PLANNED_ID`]. The identifiers are then derived from it
/// ([`derived_id`]) and put into the final plan, whose digest is the proof.
/// So the identifiers depend on every bound observation and input, and a
/// recomputed plan yields the same identifiers and the same proof; the
/// proof is never an input to what it proves.
pub fn plan_seed(seeded: &CanonicalPlan) -> String {
    let mut h = Sha256::new();
    h.update(b"DeltaForge.Recovery.PlanSeed.v1");
    h.update(serde_json::to_vec(seeded).expect("a canonical plan serializes"));
    hex::encode(h.finalize())
}

/// A 128-bit identifier for `purpose`, derived from a [`plan_seed`].
pub fn derived_id(seed: &str, purpose: &str) -> String {
    let mut h = Sha256::new();
    for part in [
        b"DeltaForge.Recovery.DerivedId.v1".as_slice(),
        purpose.as_bytes(),
        seed.as_bytes(),
    ] {
        h.update((part.len() as u64).to_be_bytes());
        h.update(part);
    }
    hex::encode(&h.finalize()[..16])
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordState {
    Applying,
    Completed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Observed {
    /// The step's pre-state: it was performed.
    Pre,
    /// The step's post-state: it was already done.
    Post,
}

/// A step whose post-state was verified.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerifiedStep {
    pub step: u32,
    /// What the step found before acting.
    pub found: Observed,
    /// The post-state digest verified after it.
    pub digest: String,
}

/// A step that found neither its pre- nor its post-state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Divergence {
    pub step: u32,
    pub expected_pre: String,
    pub expected_post: String,
    pub found: String,
    pub at_ms: i64,
}

/// The durable record of one recovery operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryRecord {
    pub format: u32,
    pub operation: String,
    pub pipeline: String,
    pub source: String,
    pub proof: String,
    pub plan: CanonicalPlan,
    pub applied: RecoveryAudit,
    /// The incidents the operation resolves.
    pub resolves: Vec<IncidentId>,
    pub started_at_ms: i64,
    /// The next step to run (`plan.steps.len()` once every step is done).
    pub step: u32,
    pub verified: Vec<VerifiedStep>,
    /// Recorded outcomes of steps (for example what was done to a slot).
    pub outcomes: BTreeMap<String, String>,
    /// The last step that diverged, kept until it is resolved.
    pub divergence: Option<Divergence>,
    /// Set once every step is done: the audit and resolutions are being
    /// written; the record completes only after them.
    pub audit_pending: bool,
    /// When the audit phase began: the audit entry's time, fixed so that a
    /// resumed write is byte-identical.
    pub audit_at_ms: Option<i64>,
    pub state: RecordState,
}

impl RecoveryRecord {
    /// A new record for `plan`, about to be claimed.
    pub fn new(
        plan: CanonicalPlan,
        applied: RecoveryAudit,
        resolves: Vec<IncidentId>,
    ) -> Self {
        Self {
            format: RECORD_FORMAT,
            operation: plan.operation.clone(),
            pipeline: plan.pipeline.clone(),
            source: plan.source.clone(),
            proof: plan.proof(),
            plan,
            applied,
            resolves,
            started_at_ms: now_ms(),
            step: 0,
            verified: Vec::new(),
            outcomes: BTreeMap::new(),
            divergence: None,
            audit_pending: false,
            audit_at_ms: None,
            state: RecordState::Applying,
        }
    }
}

fn applied_event() -> String {
    "applied".into()
}

/// One entry of the recovery audit trail.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryAuditEntry {
    /// `applied` (the operation), or a later effect of it (for example
    /// `slot_recreated`, recorded by the start it authorized).
    #[serde(default = "applied_event")]
    pub event: String,
    pub at_ms: i64,
    pub pipeline: String,
    pub source: String,
    pub applied: RecoveryAudit,
    pub resolves: Vec<IncidentId>,
    pub outcomes: BTreeMap<String, String>,
}

/// The outcome of a claim.
#[derive(Debug)]
pub enum Claim {
    /// This call claimed the slot; the record is at this version.
    Claimed(u64),
    /// Another operation is in progress.
    Pending(Box<RecoveryRecord>),
}

/// How a run of the steps ended.
#[derive(Debug, PartialEq, Eq)]
pub enum Driven {
    /// Every step done and the audit durable: the record is completed.
    Completed,
    /// A step found neither state; nothing further was written.
    Diverged(Divergence),
}

/// The operation-specific side of a step: reading the live state and
/// performing the write.
#[async_trait]
pub trait StepExecutor: Send + Sync {
    /// The digest of the live state step `step` is about.
    async fn observe(&self, step: &PlanStep) -> Result<String>;
    /// Perform the step's write (the live state was its pre-state). Returns
    /// outcomes to record.
    async fn perform(
        &self,
        step: &PlanStep,
    ) -> Result<BTreeMap<String, String>>;
}

/// The per-pipeline recovery record.
#[derive(Clone)]
pub struct RecoveryStore {
    backend: ArcStorageBackend,
    pipeline: String,
}

impl RecoveryStore {
    pub fn new(backend: ArcStorageBackend, pipeline: &str) -> Self {
        Self {
            backend,
            pipeline: pipeline.to_string(),
        }
    }

    fn key(&self) -> String {
        segment(&self.pipeline)
    }

    /// The record and its version. A record this release cannot read is a
    /// [`CorruptRecord`]: nothing is ever written over it.
    pub async fn read(&self) -> Result<Option<(u64, RecoveryRecord)>> {
        let Some((version, bytes)) =
            self.backend.slot_get(RECOVERY_NS, &self.key()).await?
        else {
            return Ok(None);
        };
        let rec: RecoveryRecord =
            serde_json::from_slice(&bytes).map_err(|e| {
                anyhow::Error::new(CorruptRecord(format!(
                    "recovery record of pipeline {}: {e}",
                    self.pipeline
                )))
            })?;
        if rec.format != RECORD_FORMAT || rec.plan.format != PLAN_FORMAT {
            return Err(CorruptRecord(format!(
                "recovery record of pipeline {}: format {} (plan {}) is not \
                 known to this release",
                self.pipeline, rec.format, rec.plan.format
            ))
            .into());
        }
        Ok(Some((version, rec)))
    }

    /// Claim the slot for `rec`: created when absent, or replacing a
    /// completed record. An operation in progress is returned instead.
    pub async fn claim(&self, rec: &RecoveryRecord) -> Result<Claim> {
        let bytes = serde_json::to_vec(rec)?;
        loop {
            match self.read().await? {
                None => {
                    if let Some(v) = self
                        .backend
                        .slot_create(RECOVERY_NS, &self.key(), &bytes)
                        .await?
                    {
                        return Ok(Claim::Claimed(v));
                    }
                }
                Some((_, cur)) if cur.state == RecordState::Applying => {
                    return Ok(Claim::Pending(Box::new(cur)));
                }
                Some((version, _)) => {
                    if self
                        .backend
                        .slot_cas(RECOVERY_NS, &self.key(), version, &bytes)
                        .await?
                    {
                        return Ok(Claim::Claimed(version + 1));
                    }
                }
            }
        }
    }

    /// Write `rec` over exactly `version`; the new version, or an error when
    /// the record moved (another writer: the operation stops).
    async fn update(&self, version: u64, rec: &RecoveryRecord) -> Result<u64> {
        if self
            .backend
            .slot_cas(
                RECOVERY_NS,
                &self.key(),
                version,
                &serde_json::to_vec(rec)?,
            )
            .await?
        {
            Ok(version + 1)
        } else {
            bail!(
                "the recovery record of pipeline {} changed while it was \
                 being applied",
                self.pipeline
            )
        }
    }

    /// Run the remaining steps of `rec` (claimed at `version`), then write
    /// its audit and resolutions and complete it.
    pub async fn drive(
        &self,
        mut version: u64,
        mut rec: RecoveryRecord,
        exec: &dyn StepExecutor,
        incidents: &IncidentStore,
    ) -> Result<Driven> {
        while (rec.step as usize) < rec.plan.steps.len() {
            let step = rec.plan.steps[rec.step as usize].clone();
            let found = exec.observe(&step).await?;
            let observed = if found == step.post {
                Observed::Post
            } else if found == step.pre {
                let outcomes = exec.perform(&step).await?;
                rec.outcomes.extend(outcomes);
                let after = exec.observe(&step).await?;
                if after != step.post {
                    return self
                        .diverge(version, rec, &step, after)
                        .await
                        .map(Driven::Diverged);
                }
                Observed::Pre
            } else {
                return self
                    .diverge(version, rec, &step, found)
                    .await
                    .map(Driven::Diverged);
            };
            rec.verified.push(VerifiedStep {
                step: rec.step,
                found: observed,
                digest: step.post.clone(),
            });
            rec.step += 1;
            rec.divergence = None;
            version = self.update(version, &rec).await?;
        }
        if !rec.audit_pending {
            rec.audit_pending = true;
            rec.audit_at_ms = Some(now_ms());
            version = self.update(version, &rec).await?;
        }
        self.write_audit(&rec, incidents).await?;
        rec.audit_pending = false;
        rec.state = RecordState::Completed;
        self.update(version, &rec).await?;
        Ok(Driven::Completed)
    }

    async fn diverge(
        &self,
        version: u64,
        mut rec: RecoveryRecord,
        step: &PlanStep,
        found: String,
    ) -> Result<Divergence> {
        let d = Divergence {
            step: rec.step,
            expected_pre: step.pre.clone(),
            expected_post: step.post.clone(),
            found,
            at_ms: now_ms(),
        };
        rec.divergence = Some(d.clone());
        self.update(version, &rec).await?;
        Ok(d)
    }

    /// The recovery audit entry and every resolution, each idempotent: the
    /// entry is appended once per proof, and resolving a resolved incident
    /// changes nothing.
    async fn write_audit(
        &self,
        rec: &RecoveryRecord,
        incidents: &IncidentStore,
    ) -> Result<()> {
        let entry = RecoveryAuditEntry {
            event: applied_event(),
            at_ms: rec.audit_at_ms.unwrap_or(rec.started_at_ms),
            pipeline: rec.pipeline.clone(),
            source: rec.source.clone(),
            applied: rec.applied.clone(),
            resolves: rec.resolves.clone(),
            outcomes: rec.outcomes.clone(),
        };
        self.backend
            .log_append_if_absent(
                RECOVERY_AUDIT_NS,
                &self.key(),
                &rec.proof,
                &serde_json::to_vec(&entry)?,
            )
            .await
            .context("append the recovery audit entry")?;
        for id in &rec.resolves {
            match incidents.resolve_by_recovery(id, &rec.applied).await? {
                Ok(_) => {}
                // Pruned since the plan bound it: nothing left to resolve.
                Err(LifecycleError::NotFound(_)) => {}
                Err(e) => bail!("resolve incident {}: {e}", id.0),
            }
        }
        Ok(())
    }

    /// Record a later effect `event` of the operation applied with `proof`
    /// (once per proof and event), with the operation's audit fields when
    /// its record is still the current one.
    /// `at_ms` is the event's recorded time, so a repeated append is
    /// byte-identical.
    pub async fn append_event(
        &self,
        proof: &str,
        event: &str,
        outcomes: BTreeMap<String, String>,
        at_ms: i64,
    ) -> Result<()> {
        let (applied, resolves, source) = match self.read().await? {
            Some((_, rec)) if rec.proof == proof => {
                (rec.applied, rec.resolves, rec.source)
            }
            _ => (
                RecoveryAudit {
                    operation: String::new(),
                    proof: proof.to_string(),
                    asserted_actor: String::new(),
                    actor_verified: false,
                    credential: String::new(),
                    origin: None,
                    reason: String::new(),
                },
                Vec::new(),
                String::new(),
            ),
        };
        let entry = RecoveryAuditEntry {
            event: event.to_string(),
            at_ms,
            pipeline: self.pipeline.clone(),
            source,
            applied,
            resolves,
            outcomes,
        };
        self.backend
            .log_append_if_absent(
                RECOVERY_AUDIT_NS,
                &self.key(),
                &format!("{proof}:{event}"),
                &serde_json::to_vec(&entry)?,
            )
            .await
            .context("append the recovery audit event")?;
        Ok(())
    }

    /// The recovery audit trail of this pipeline, oldest first.
    pub async fn audit_trail(
        &self,
        limit: usize,
    ) -> Result<Vec<RecoveryAuditEntry>> {
        self.backend
            .log_since(RECOVERY_AUDIT_NS, &self.key(), 0)
            .await?
            .into_iter()
            .take(limit)
            .map(|(_, bytes)| {
                serde_json::from_slice(&bytes)
                    .context("decode recovery audit entry")
            })
            .collect()
    }
}

#[cfg(test)]
pub(crate) mod contract {
    //! The recovery record contract, on any backend.

    use super::*;
    use deltaforge_core::incident::{
        CauseCode, Component, IncidentDraft, ReasonCode, Retryability,
        SafetyState,
    };
    use std::sync::Mutex;

    fn audit(proof: &str) -> RecoveryAudit {
        RecoveryAudit {
            operation: "test-op".into(),
            proof: proof.into(),
            asserted_actor: "alice".into(),
            actor_verified: false,
            credential: "token".into(),
            origin: Some("127.0.0.1:1".into()),
            reason: "because".into(),
        }
    }

    fn plan(pipeline: &str, steps: &[(&str, &str, &str)]) -> CanonicalPlan {
        CanonicalPlan::new(
            "test-op",
            pipeline,
            "src",
            BTreeMap::from([("lineage".into(), "l1".into())]),
            steps
                .iter()
                .map(|(n, pre, post)| PlanStep {
                    name: n.to_string(),
                    pre: pre.to_string(),
                    post: post.to_string(),
                    detail: BTreeMap::new(),
                })
                .collect(),
            vec!["nothing is copied".into()],
        )
    }

    /// A live state per step name; `perform` moves it to the post-state
    /// unless told to land elsewhere, and can fail once.
    struct Fake {
        state: Mutex<BTreeMap<String, String>>,
        performed: Mutex<Vec<String>>,
        fail_perform: Mutex<Option<String>>,
        land_on: Mutex<Option<String>>,
    }

    impl Fake {
        fn new(state: &[(&str, &str)]) -> Self {
            Self {
                state: Mutex::new(
                    state
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                ),
                performed: Mutex::new(Vec::new()),
                fail_perform: Mutex::new(None),
                land_on: Mutex::new(None),
            }
        }
    }

    #[async_trait]
    impl StepExecutor for Fake {
        async fn observe(&self, step: &PlanStep) -> Result<String> {
            Ok(self.state.lock().unwrap()[&step.name].clone())
        }
        async fn perform(
            &self,
            step: &PlanStep,
        ) -> Result<BTreeMap<String, String>> {
            if self.fail_perform.lock().unwrap().as_deref() == Some(&step.name)
            {
                *self.fail_perform.lock().unwrap() = None;
                bail!("crash in {}", step.name);
            }
            self.performed.lock().unwrap().push(step.name.clone());
            let to = self
                .land_on
                .lock()
                .unwrap()
                .take()
                .unwrap_or_else(|| step.post.clone());
            self.state.lock().unwrap().insert(step.name.clone(), to);
            Ok(BTreeMap::from([(step.name.clone(), "done".into())]))
        }
    }

    async fn incident(store: &IncidentStore) -> IncidentId {
        store
            .raise(
                &IncidentDraft::new(
                    ReasonCode::SnapshotAnchorUnavailable,
                    Component::Source { id: "src".into() },
                    Retryability::OperatorAction,
                    SafetyState::HaltedSafe,
                    CauseCode::SourceLineage,
                ),
                1,
            )
            .await
            .unwrap()
            .record()
            .incident_id
            .clone()
    }

    pub(crate) async fn all(backend: ArcStorageBackend, prefix: &str) {
        proof_is_deterministic();
        round_trip(backend.clone(), &format!("{prefix}-rt")).await;
        crash_resumes_from_the_stored_plan(
            backend.clone(),
            &format!("{prefix}-crash"),
        )
        .await;
        divergence_stops_and_keeps_evidence(
            backend.clone(),
            &format!("{prefix}-div"),
        )
        .await;
        unknown_record_is_refused(backend.clone(), &format!("{prefix}-unk"))
            .await;
        audit_pending_completes_on_resume(
            backend.clone(),
            &format!("{prefix}-audit"),
        )
        .await;
    }

    fn proof_is_deterministic() {
        let a = plan("p", &[("s1", "a", "b"), ("s2", "c", "d")]);
        let mut bindings = BTreeMap::new();
        // Insertion order never matters.
        bindings.insert("z".to_string(), "1".to_string());
        bindings.insert("a".to_string(), "2".to_string());
        let mut x = a.clone();
        x.bindings = bindings.clone();
        let mut y = a.clone();
        y.bindings = bindings.into_iter().rev().collect();
        assert_eq!(x.proof(), y.proof());
        assert_eq!(a.proof(), a.clone().proof());
        // Every field is bound.
        let mut b = a.clone();
        b.steps[1].post = "e".into();
        assert_ne!(a.proof(), b.proof());
        let mut c = a.clone();
        c.consequences.push("more".into());
        assert_ne!(a.proof(), c.proof());
    }

    async fn round_trip(backend: ArcStorageBackend, p: &str) {
        let store = RecoveryStore::new(backend.clone(), p);
        let incidents = IncidentStore::new(backend.clone(), p);
        let id = incident(&incidents).await;
        let plan = plan(p, &[("s1", "a", "b"), ("s2", "c", "d")]);
        let rec = RecoveryRecord::new(
            plan.clone(),
            audit(&plan.proof()),
            vec![id.clone()],
        );
        let Claim::Claimed(v) = store.claim(&rec).await.unwrap() else {
            panic!("claimed")
        };
        // A second claim while applying sees the pending operation.
        assert!(matches!(
            store.claim(&rec).await.unwrap(),
            Claim::Pending(_)
        ));
        let fake = Fake::new(&[("s1", "a"), ("s2", "d")]);
        assert_eq!(
            store.drive(v, rec, &fake, &incidents).await.unwrap(),
            Driven::Completed
        );
        // s2 was already in its post-state: skipped.
        assert_eq!(*fake.performed.lock().unwrap(), vec!["s1".to_string()]);
        let (_, done) = store.read().await.unwrap().unwrap();
        assert_eq!(done.state, RecordState::Completed);
        assert_eq!(done.step, 2);
        assert_eq!(
            done.verified.iter().map(|v| v.found).collect::<Vec<_>>(),
            vec![Observed::Pre, Observed::Post]
        );
        let inc = incidents.get(&id).await.unwrap().unwrap();
        assert!(inc.status.is_resolved());
        let trail = store.audit_trail(10).await.unwrap();
        assert_eq!(trail.len(), 1);
        assert_eq!(trail[0].applied.asserted_actor, "alice");
        // A completed record can be replaced by the next operation.
        let next =
            RecoveryRecord::new(plan.clone(), audit(&plan.proof()), Vec::new());
        assert!(matches!(
            store.claim(&next).await.unwrap(),
            Claim::Claimed(_)
        ));
    }

    async fn crash_resumes_from_the_stored_plan(
        backend: ArcStorageBackend,
        p: &str,
    ) {
        let store = RecoveryStore::new(backend.clone(), p);
        let incidents = IncidentStore::new(backend.clone(), p);
        let plan = plan(p, &[("s1", "a", "b"), ("s2", "c", "d")]);
        let rec =
            RecoveryRecord::new(plan.clone(), audit(&plan.proof()), vec![]);
        let Claim::Claimed(v) = store.claim(&rec).await.unwrap() else {
            panic!("claimed")
        };
        let fake = Fake::new(&[("s1", "a"), ("s2", "c")]);
        *fake.fail_perform.lock().unwrap() = Some("s2".into());
        assert!(store.drive(v, rec, &fake, &incidents).await.is_err());
        // The resume runs the stored record from its step: s1 is not
        // performed again, s2 is.
        let (v, stored) = store.read().await.unwrap().unwrap();
        assert_eq!(stored.state, RecordState::Applying);
        assert_eq!(stored.step, 1);
        assert_eq!(
            store.drive(v, stored, &fake, &incidents).await.unwrap(),
            Driven::Completed
        );
        assert_eq!(
            *fake.performed.lock().unwrap(),
            vec!["s1".to_string(), "s2".to_string()]
        );
    }

    async fn divergence_stops_and_keeps_evidence(
        backend: ArcStorageBackend,
        p: &str,
    ) {
        let store = RecoveryStore::new(backend.clone(), p);
        let incidents = IncidentStore::new(backend.clone(), p);
        let id = incident(&incidents).await;
        let plan = plan(p, &[("s1", "a", "b")]);
        let rec = RecoveryRecord::new(
            plan.clone(),
            audit(&plan.proof()),
            vec![id.clone()],
        );
        let Claim::Claimed(v) = store.claim(&rec).await.unwrap() else {
            panic!("claimed")
        };
        let fake = Fake::new(&[("s1", "x")]);
        let Driven::Diverged(d) =
            store.drive(v, rec, &fake, &incidents).await.unwrap()
        else {
            panic!("diverged")
        };
        assert_eq!((d.step, d.found.as_str()), (0, "x"));
        assert!(fake.performed.lock().unwrap().is_empty(), "nothing written");
        let (v, stored) = store.read().await.unwrap().unwrap();
        assert_eq!(stored.state, RecordState::Applying);
        assert_eq!(stored.divergence.as_ref(), Some(&d));
        assert!(
            !incidents
                .get(&id)
                .await
                .unwrap()
                .unwrap()
                .status
                .is_resolved()
        );
        // A write landing elsewhere diverges after the perform too.
        fake.state.lock().unwrap().insert("s1".into(), "a".into());
        *fake.land_on.lock().unwrap() = Some("y".into());
        assert!(matches!(
            store.drive(v, stored, &fake, &incidents).await.unwrap(),
            Driven::Diverged(Divergence { found, .. }) if found == "y"
        ));
        // After a manual repair to the pre-state the same record completes.
        let (v, stored) = store.read().await.unwrap().unwrap();
        fake.state.lock().unwrap().insert("s1".into(), "a".into());
        assert_eq!(
            store.drive(v, stored, &fake, &incidents).await.unwrap(),
            Driven::Completed
        );
        let (_, done) = store.read().await.unwrap().unwrap();
        assert_eq!(done.divergence, None);
    }

    async fn unknown_record_is_refused(backend: ArcStorageBackend, p: &str) {
        let plan = plan(p, &[("s1", "a", "b")]);
        let rec =
            RecoveryRecord::new(plan.clone(), audit(&plan.proof()), vec![]);
        // Well-formed but of a future format, and plainly malformed.
        let mut future = serde_json::to_value(&rec).unwrap();
        future["format"] = 9.into();
        for (i, bytes) in [
            serde_json::to_vec(&future).unwrap(),
            br#"{"format":1}"#.to_vec(),
        ]
        .into_iter()
        .enumerate()
        {
            let key = format!("{p}-{i}");
            let store = RecoveryStore::new(backend.clone(), &key);
            backend
                .slot_create(RECOVERY_NS, &segment(&key), &bytes)
                .await
                .unwrap();
            let err = store.read().await.unwrap_err();
            assert!(err.downcast_ref::<CorruptRecord>().is_some(), "{err:#}");
            assert!(store.claim(&rec).await.is_err(), "never written over");
            let (_, kept) = backend
                .slot_get(RECOVERY_NS, &segment(&key))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(kept, bytes);
        }
    }

    async fn audit_pending_completes_on_resume(
        backend: ArcStorageBackend,
        p: &str,
    ) {
        let store = RecoveryStore::new(backend.clone(), p);
        let incidents = IncidentStore::new(backend.clone(), p);
        let id = incident(&incidents).await;
        let plan = plan(p, &[]);
        let mut rec = RecoveryRecord::new(
            plan.clone(),
            audit(&plan.proof()),
            vec![id.clone()],
        );
        // As left by a crash after the steps, before the audit.
        rec.audit_pending = true;
        rec.audit_at_ms = Some(now_ms());
        let Claim::Claimed(v) = store.claim(&rec).await.unwrap() else {
            panic!("claimed")
        };
        assert_eq!(
            store
                .drive(v, rec.clone(), &Fake::new(&[]), &incidents)
                .await
                .unwrap(),
            Driven::Completed
        );
        // Running the audit again (a crash before the completing CAS)
        // records nothing twice.
        store.write_audit(&rec, &incidents).await.unwrap();
        assert_eq!(store.audit_trail(10).await.unwrap().len(), 1);
        let resolutions = incidents
            .audit_trail(100)
            .await
            .unwrap()
            .into_iter()
            .filter(|e| {
                matches!(
                    e.transition,
                    super::super::incidents::Transition::ResolvedByRecovery { .. }
                )
            })
            .count();
        assert_eq!(resolutions, 1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryStorageBackend;
    use std::sync::Arc;

    #[test]
    fn derived_identifiers_come_from_the_seeded_plan() {
        let plan = |source: &str, binding: &str| {
            CanonicalPlan::new(
                "op",
                "p",
                source,
                BTreeMap::from([("b".to_string(), binding.to_string())]),
                vec![PlanStep {
                    name: "s".into(),
                    pre: "a".into(),
                    post: PLANNED_ID.into(),
                    detail: BTreeMap::new(),
                }],
                vec![],
            )
        };
        let seed = plan_seed(&plan("src", "1"));
        assert_eq!(seed, plan_seed(&plan("src", "1")), "deterministic");
        assert_ne!(seed, plan_seed(&plan("src", "2")), "binds observations");
        assert_ne!(seed, plan_seed(&plan("other", "1")), "binds the source");
        let id = derived_id(&seed, "snapshot_chain");
        assert_eq!(id.len(), 32, "128 bits");
        assert_eq!(id, derived_id(&seed, "snapshot_chain"));
        // Domain separation: another purpose, and the plan digest itself.
        assert_ne!(id, derived_id(&seed, "continuity_chain"));
        assert_ne!(seed, plan("src", "1").proof());
        assert!(!plan("src", "1").proof().starts_with(&id));
    }

    #[tokio::test]
    async fn recovery_contract_holds_in_memory() {
        contract::all(Arc::new(MemoryStorageBackend::new()), "mem").await;
    }

    #[tokio::test]
    async fn recovery_contract_holds_on_sqlite() {
        let backend = crate::SqliteStorageBackend::in_memory().unwrap();
        contract::all(backend, "sqlite").await;
    }

    /// The contract on a live PostgreSQL backend (the gate's `serial-pg`
    /// lane).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires a live PostgreSQL (DELTAFORGE_IT_PG_DSN)"]
    async fn pg_recovery_contract_holds_on_postgresql() {
        let dsn = std::env::var("DELTAFORGE_IT_PG_DSN")
            .expect("DELTAFORGE_IT_PG_DSN must be set to run this test");
        let backend: ArcStorageBackend =
            crate::PostgresStorageBackend::connect(&dsn).await.unwrap();
        let prefix = format!("pg{}", uuid::Uuid::new_v4().simple());
        contract::all(backend, &prefix).await;
    }
}
