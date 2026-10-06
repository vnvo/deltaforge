//! Recovery operations in the server (`docs/design/recovery-cli.md`).
//!
//! The admin API (`rest_api::recovery`) calls [`RecoveryService`]. An apply
//! runs strictly in this order: take the pipeline's recovery lock (under the
//! manager's lifecycle lock, after confirming its tasks are joined); read
//! the durable recovery record; recompute the plan and verify the proof
//! while holding the lock; claim the record; and only then drive the steps.
//! The lock is held until the operation completed or stopped. A pipeline
//! with an unfinished operation is registered stopped and never started;
//! start, resume, patch, pause and delete all refuse it.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use checkpoints::CheckpointStore;
use deltaforge_config::PipelineSpec;
use deltaforge_core::incident::IncidentId;
use parking_lot::Mutex;
use rest_api::recovery::{
    ApplyRequest, Caller, PlanRequest, RecoveryApiError, RecoveryController,
};
use rest_api::{PipelineAPIError, PipelineRecovery};
use serde_json::{Value, json};
use storage::ArcStorageBackend;
use storage::adapters::CorruptRecord;
use storage::adapters::incidents::{
    IncidentRecord, IncidentStore, RecoveryAudit,
};
use storage::adapters::recovery::{
    CanonicalPlan, Claim, Driven, RecordState, RecoveryRecord, RecoveryStore,
    StepExecutor, digest_bytes,
};

use crate::pipeline_manager::{
    PipelineManager, PipelineRuntime, PipelineStatus,
};

/// What an operation is planned and applied with.
pub struct OperationContext {
    pub pipeline: String,
    pub spec: PipelineSpec,
    pub backend: ArcStorageBackend,
    pub ckpt_store: Arc<dyn CheckpointStore>,
    /// The incident the operation answers, when named.
    pub incident: Option<IncidentRecord>,
    pub args: BTreeMap<String, String>,
}

impl OperationContext {
    pub fn source_id(&self) -> &str {
        self.spec.spec.source.source_id()
    }

    /// What every plan binds (design section 4): the incident and its
    /// transition, the source lineage, the recovery epoch, and the complete
    /// sorted set of the source's checkpoint keys with a digest of each
    /// value.
    pub async fn base_bindings(
        &self,
    ) -> Result<BTreeMap<String, String>, RecoveryApiError> {
        let mut b = BTreeMap::new();
        if let Some(i) = &self.incident {
            b.insert("incident".into(), i.incident_id.0.clone());
            b.insert(
                "incident.transition_seq".into(),
                i.transition_seq.to_string(),
            );
        }
        let lineage = storage::adapters::source_lineage::load(
            &self.backend,
            &self.spec.metadata.tenant,
            self.source_id(),
        )
        .await
        .map_err(|e| state_error("the source lineage record", &e))?;
        b.insert(
            "lineage".into(),
            match lineage {
                Some(r) => {
                    digest_bytes(&serde_json::to_vec(&r).map_err(|e| {
                        state_error("the source lineage record", &e.into())
                    })?)
                }
                None => "absent".into(),
            },
        );
        let epoch = IncidentStore::new(self.backend.clone(), &self.pipeline)
            .recovery_epoch()
            .await
            .map_err(|e| state_error("the recovery epoch", &e))?;
        b.insert("recovery_epoch".into(), epoch.to_string());
        for (key, digest) in self.checkpoint_digests().await? {
            b.insert(format!("checkpoint.{key}"), digest);
        }
        Ok(b)
    }

    /// Every checkpoint key of the source (per sink, and the legacy
    /// aggregate key) with the SHA-256 of its bytes, or `absent`.
    pub async fn checkpoint_digests(
        &self,
    ) -> Result<BTreeMap<String, String>, RecoveryApiError> {
        let source = self.source_id();
        let mut keys: Vec<String> = self
            .ckpt_store
            .list_with_prefix(&format!("{source}::sink::"))
            .await
            .map_err(|e| state_error("the checkpoints", &anyhow::anyhow!(e)))?;
        keys.push(source.to_string());
        keys.sort();
        keys.dedup();
        let mut out = BTreeMap::new();
        for k in keys {
            let v = self.ckpt_store.get_raw(&k).await.map_err(|e| {
                state_error("a checkpoint", &anyhow::anyhow!(e))
            })?;
            out.insert(
                k,
                v.map(|b| digest_bytes(&b))
                    .unwrap_or_else(|| "absent".into()),
            );
        }
        Ok(out)
    }
}

/// A computed plan.
pub struct Planned {
    pub plan: CanonicalPlan,
    pub resolves: Vec<IncidentId>,
    /// Shown to the operator, never part of the proof.
    pub observed: BTreeMap<String, String>,
}

/// One recovery operation.
#[async_trait]
pub trait RecoveryOperation: Send + Sync {
    fn name(&self) -> &'static str;
    /// Whether the operation answers `incident` (for diagnose).
    fn applies_to(&self, incident: &IncidentRecord) -> bool;
    /// The canonical plan over the current state. Read-only.
    async fn plan(
        &self,
        ctx: &OperationContext,
    ) -> Result<Planned, RecoveryApiError>;
    /// The executor of `plan`'s steps (a stored plan on a resume).
    async fn executor(
        &self,
        ctx: &OperationContext,
        plan: &CanonicalPlan,
    ) -> Result<Box<dyn StepExecutor>, RecoveryApiError>;
}

/// A storage or decoding failure as an API error: a fixed message naming
/// what could not be read, the cause logged, never returned.
pub fn state_error(what: &str, e: &anyhow::Error) -> RecoveryApiError {
    if e.chain()
        .any(|c| c.downcast_ref::<CorruptRecord>().is_some())
    {
        return RecoveryApiError::new(
            409,
            "state_unreadable",
            format!("{what} cannot be interpreted: manual repair is required"),
        );
    }
    tracing::warn!(error = %format!("{e:#}"), "recovery: reading {what}");
    RecoveryApiError::new(
        503,
        "store_unavailable",
        format!("{what} could not be read"),
    )
}

/// Held while an operation is applied to the pipeline; released on drop.
pub struct RecoveryGuard {
    set: Arc<Mutex<HashSet<String>>>,
    name: String,
}

impl Drop for RecoveryGuard {
    fn drop(&mut self) {
        self.set.lock().remove(&self.name);
    }
}

fn conflict(code: &'static str, msg: impl Into<String>) -> RecoveryApiError {
    RecoveryApiError::new(409, code, msg)
}

impl PipelineManager {
    /// The pipeline's recovery lock, taken only when it is quiescent:
    /// stopped or failed, with its source and coordinator tasks joined.
    pub(crate) async fn begin_recovery(
        &self,
        name: &str,
    ) -> Result<RecoveryGuard, RecoveryApiError> {
        let _lifecycle = self.lifecycle.lock().await;
        {
            let guard = self.pipelines.read();
            let rt = guard.get(name).ok_or_else(|| {
                RecoveryApiError::new(404, "not_found", "no such pipeline")
            })?;
            if rt.status == PipelineStatus::Deleting {
                return Err(conflict(
                    "pipeline_deleting",
                    "the pipeline is being deleted",
                ));
            }
            if !is_quiescent(rt) {
                return Err(conflict(
                    "pipeline_not_quiescent",
                    "stop the pipeline and wait until it has stopped",
                ));
            }
        }
        if !self.recovering.lock().insert(name.to_string()) {
            return Err(conflict(
                "recovery_in_progress",
                "another recovery request is being applied to this pipeline",
            ));
        }
        Ok(RecoveryGuard {
            set: Arc::clone(&self.recovering),
            name: name.to_string(),
        })
    }

    /// Refuse a lifecycle change while an operation is applied or pending.
    /// Called with the lifecycle lock held.
    pub(crate) async fn refuse_while_recovering(
        &self,
        name: &str,
    ) -> Result<(), PipelineAPIError> {
        if self.recovering.lock().contains(name) {
            return Err(PipelineAPIError::Conflict(format!(
                "recovery_in_progress: a recovery operation is being applied \
                 to pipeline '{name}'"
            )));
        }
        match RecoveryStore::new(self.backend.clone(), name).read().await {
            Ok(Some((_, rec))) if rec.state == RecordState::Applying => {
                Err(PipelineAPIError::Conflict(format!(
                    "recovery_pending: pipeline '{name}' has an unfinished \
                     recovery operation '{}' (proof {}); finish it with \
                     `deltaforge recover apply` and the same proof",
                    rec.operation, rec.proof
                )))
            }
            Ok(_) => Ok(()),
            Err(e) if is_corrupt(&e) => {
                Err(PipelineAPIError::Conflict(format!(
                    "recovery_record_unreadable: the recovery record of \
                     pipeline '{name}' cannot be interpreted; manual repair \
                     is required"
                )))
            }
            Err(e) => Err(PipelineAPIError::Failed(
                e.context("read the recovery record"),
            )),
        }
    }

    /// A stopped runtime with no task for a pipeline whose recovery record
    /// is unfinished or unreadable, or `None` when it may start.
    pub(crate) async fn held_for_recovery(
        &self,
        spec: &PipelineSpec,
    ) -> Result<Option<PipelineRuntime>, PipelineAPIError> {
        let name = &spec.metadata.name;
        let held =
            match RecoveryStore::new(self.backend.clone(), name).read().await {
                Ok(Some((_, rec))) => rec.state == RecordState::Applying,
                Ok(None) => false,
                Err(e) if is_corrupt(&e) => true,
                Err(e) => {
                    return Err(PipelineAPIError::Failed(
                        e.context("read the recovery record"),
                    ));
                }
            };
        if !held {
            return Ok(None);
        }
        tracing::warn!(
            pipeline = %name,
            "a recovery operation is unfinished: the pipeline stays stopped"
        );
        Ok(Some(PipelineRuntime::held(
            spec.clone(),
            IncidentStore::new(self.backend.clone(), name),
        )))
    }

    /// The pipeline's recovery state, when it holds the pipeline.
    pub(crate) async fn recovery_status(
        &self,
        name: &str,
    ) -> Option<PipelineRecovery> {
        match RecoveryStore::new(self.backend.clone(), name).read().await {
            Ok(Some((_, rec))) if rec.state == RecordState::Applying => {
                Some(PipelineRecovery {
                    state: "recovery_pending".into(),
                    operation: Some(rec.operation),
                    proof: Some(rec.proof),
                    step: Some(rec.step),
                    steps: Some(rec.plan.steps.len() as u32),
                    diverged: rec.divergence.is_some(),
                })
            }
            Err(e) if is_corrupt(&e) => Some(PipelineRecovery {
                state: "recovery_record_unreadable".into(),
                ..Default::default()
            }),
            _ => None,
        }
    }
}

fn is_corrupt(e: &anyhow::Error) -> bool {
    e.chain()
        .any(|c| c.downcast_ref::<CorruptRecord>().is_some())
}

fn is_quiescent(rt: &PipelineRuntime) -> bool {
    (rt.status == PipelineStatus::Stopped || rt.is_failed())
        && rt.join.as_ref().is_none_or(|j| j.is_finished())
        && rt.sources.iter().all(|s| s.join.is_finished())
}

/// The recovery admin API's controller.
pub struct RecoveryService {
    manager: Arc<PipelineManager>,
    ops: BTreeMap<&'static str, Arc<dyn RecoveryOperation>>,
}

impl RecoveryService {
    pub fn new(manager: Arc<PipelineManager>) -> Self {
        Self {
            manager,
            ops: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn with_operation(mut self, op: Arc<dyn RecoveryOperation>) -> Self {
        self.ops.insert(op.name(), op);
        self
    }

    fn op(
        &self,
        name: &str,
    ) -> Result<Arc<dyn RecoveryOperation>, RecoveryApiError> {
        self.ops.get(name).cloned().ok_or_else(|| {
            RecoveryApiError::new(
                400,
                "unknown_operation",
                format!(
                    "unknown operation '{name}'; operations in this release: {}",
                    self.ops.keys().copied().collect::<Vec<_>>().join(", ")
                ),
            )
        })
    }

    async fn context(
        &self,
        pipeline: &str,
        req: &PlanRequest,
    ) -> Result<OperationContext, RecoveryApiError> {
        let spec = self
            .manager
            .pipelines
            .read()
            .get(pipeline)
            .map(|rt| rt.spec.clone())
            .ok_or_else(|| {
                RecoveryApiError::new(404, "not_found", "no such pipeline")
            })?;
        let backend = self.manager.backend.clone();
        let incident = match &req.incident {
            None => None,
            Some(id) => {
                let rec = IncidentStore::new(backend.clone(), pipeline)
                    .get(&IncidentId(id.clone()))
                    .await
                    .map_err(|e| state_error("the incident", &e))?
                    .ok_or_else(|| {
                        RecoveryApiError::new(
                            404,
                            "not_found",
                            "no such incident",
                        )
                    })?;
                if rec.status.is_resolved() {
                    return Err(conflict(
                        "incident_resolved",
                        "the incident is already resolved",
                    ));
                }
                Some(rec)
            }
        };
        Ok(OperationContext {
            pipeline: pipeline.to_string(),
            spec,
            backend,
            ckpt_store: Arc::clone(&self.manager.ckpt_store),
            incident,
            args: req.args.clone(),
        })
    }

    /// Why an apply is impossible right now, if it is.
    async fn apply_blocked_by(
        &self,
        pipeline: &str,
        proof: &str,
    ) -> Option<&'static str> {
        if self.manager.recovering.lock().contains(pipeline) {
            return Some("recovery_in_progress");
        }
        let quiescent = self
            .manager
            .pipelines
            .read()
            .get(pipeline)
            .is_some_and(is_quiescent);
        if !quiescent {
            return Some("pipeline_not_quiescent");
        }
        match RecoveryStore::new(self.manager.backend.clone(), pipeline)
            .read()
            .await
        {
            Ok(Some((_, rec)))
                if rec.state == RecordState::Applying && rec.proof != proof =>
            {
                Some("recovery_pending")
            }
            Err(_) => Some("recovery_record_unreadable"),
            _ => None,
        }
    }
}

fn record_view(rec: &RecoveryRecord) -> Value {
    let remediation = match (&rec.state, &rec.divergence) {
        (RecordState::Completed, _) => {
            "none: the operation completed".to_string()
        }
        (RecordState::Applying, None) => format!(
            "re-run `deltaforge recover apply {} --expect-proof {}` with an \
             actor and reason to finish it; it cannot be abandoned",
            rec.operation, rec.proof
        ),
        (RecordState::Applying, Some(d)) => format!(
            "step {} found a state that is neither its expected pre-state \
             nor its post-state; repair it to the expected pre-state, then \
             re-run the apply with the same proof; it cannot be abandoned",
            d.step
        ),
    };
    json!({
        "operation": rec.operation,
        "proof": rec.proof,
        "state": rec.state,
        "step": rec.step,
        "steps": rec.plan.steps.len(),
        "current_step": rec.plan.steps.get(rec.step as usize).map(|s| json!({
            "name": s.name, "expected_pre": s.pre, "expected_post": s.post,
        })),
        "last_verified": rec.verified.last(),
        "divergence": rec.divergence,
        "outcomes": rec.outcomes,
        "audit_pending": rec.audit_pending,
        "applied": {
            "asserted_actor": rec.applied.asserted_actor,
            "actor_verified": rec.applied.actor_verified,
            "origin": rec.applied.origin,
            "reason": rec.applied.reason,
        },
        "remediation": remediation,
    })
}

fn plan_view(planned: &Planned) -> Value {
    json!({
        "plan": planned.plan,
        "proof": planned.plan.proof(),
        "resolves": planned.resolves,
        "observed": planned.observed,
    })
}

#[async_trait]
impl RecoveryController for RecoveryService {
    async fn diagnose(
        &self,
        pipeline: &str,
    ) -> Result<Value, RecoveryApiError> {
        let (status, quiescent) = {
            let guard = self.manager.pipelines.read();
            let rt = guard.get(pipeline).ok_or_else(|| {
                RecoveryApiError::new(404, "not_found", "no such pipeline")
            })?;
            (rt.info().status, is_quiescent(rt))
        };
        let backend = self.manager.backend.clone();
        let record = match RecoveryStore::new(backend.clone(), pipeline)
            .read()
            .await
        {
            Ok(r) => json!(r.map(|(_, rec)| record_view(&rec))),
            Err(e) => {
                json!({ "error": state_error("the recovery record", &e).message })
            }
        };
        let incidents = IncidentStore::new(backend, pipeline)
            .list()
            .await
            .map_err(|e| state_error("the incidents", &e))?;
        let open: Vec<Value> = incidents
            .iter()
            .filter(|i| !i.status.is_resolved())
            .map(|i| {
                let ops: Vec<&str> = self
                    .ops
                    .values()
                    .filter(|op| op.applies_to(i))
                    .map(|op| op.name())
                    .collect();
                json!({
                    "incident_id": i.incident_id,
                    "reason_code": i.reason_code,
                    "transition_seq": i.transition_seq,
                    "explanation": i.explanation(),
                    "evidence": i.evidence,
                    "operations": ops,
                })
            })
            .collect();
        Ok(json!({
            "pipeline": pipeline,
            "status": status,
            "quiescent": quiescent,
            "recovery_in_progress": self.manager.recovering.lock().contains(pipeline),
            "recovery": record,
            "incidents": open,
            "operations": self.ops.keys().collect::<Vec<_>>(),
        }))
    }

    async fn plan(
        &self,
        pipeline: &str,
        req: PlanRequest,
    ) -> Result<Value, RecoveryApiError> {
        let op = self.op(&req.operation)?;
        let ctx = self.context(pipeline, &req).await?;
        let planned = op.plan(&ctx).await?;
        let mut view = plan_view(&planned);
        let blocked =
            self.apply_blocked_by(pipeline, &planned.plan.proof()).await;
        view["apply"] = json!({
            "possible": blocked.is_none(),
            "blocked_by": blocked,
        });
        Ok(view)
    }

    async fn apply(
        &self,
        pipeline: &str,
        req: ApplyRequest,
        caller: Caller,
    ) -> Result<Value, RecoveryApiError> {
        let op = self.op(&req.plan.operation)?;
        // 1. The lock, with the pipeline quiescent; held until the end.
        let _guard = self.manager.begin_recovery(pipeline).await?;
        let backend = self.manager.backend.clone();
        let store = RecoveryStore::new(backend.clone(), pipeline);
        let incidents = IncidentStore::new(backend.clone(), pipeline);
        let ctx = self.context(pipeline, &req.plan).await?;
        // 2. The record: an unfinished operation is resumed from its stored
        // plan, with the same proof only.
        let (version, rec) = match store
            .read()
            .await
            .map_err(|e| state_error("the recovery record", &e))?
        {
            Some((v, rec)) if rec.state == RecordState::Applying => {
                if rec.proof != req.expect_proof || rec.operation != op.name() {
                    return Err(conflict(
                        "recovery_pending",
                        "another recovery operation is unfinished; finish it \
                         with its own proof",
                    )
                    .with_detail(json!({
                        "operation": rec.operation, "proof": rec.proof,
                    })));
                }
                (v, rec)
            }
            _ => {
                // 3. Recompute and verify the proof under the lock.
                let planned = op.plan(&ctx).await?;
                let proof = planned.plan.proof();
                if proof != req.expect_proof {
                    return Err(conflict(
                        "proof_mismatch",
                        "the state changed since the plan: review the new \
                         plan",
                    )
                    .with_detail(plan_view(&planned)));
                }
                let applied = RecoveryAudit {
                    operation: op.name().into(),
                    proof: proof.clone(),
                    asserted_actor: req.actor.clone(),
                    actor_verified: false,
                    credential: caller.credential.into(),
                    origin: Some(caller.origin.clone()),
                    reason: req.reason.clone(),
                };
                let rec = RecoveryRecord::new(
                    planned.plan,
                    applied,
                    planned.resolves,
                );
                // 4. Claim.
                match store
                    .claim(&rec)
                    .await
                    .map_err(|e| state_error("the recovery record", &e))?
                {
                    Claim::Claimed(v) => (v, rec),
                    Claim::Pending(other) => {
                        return Err(conflict(
                            "recovery_pending",
                            "another recovery operation is unfinished",
                        )
                        .with_detail(json!({
                            "operation": other.operation, "proof": other.proof,
                        })));
                    }
                }
            }
        };
        // 5. Only now any write.
        let exec = op.executor(&ctx, &rec.plan).await?;
        let proof = rec.proof.clone();
        match store.drive(version, rec, exec.as_ref(), &incidents).await {
            Ok(Driven::Completed) => {
                let outcomes = store
                    .read()
                    .await
                    .ok()
                    .flatten()
                    .map(|(_, r)| r.outcomes)
                    .unwrap_or_default();
                Ok(
                    json!({ "state": "completed", "proof": proof, "outcomes": outcomes }),
                )
            }
            Ok(Driven::Diverged(d)) => Err(conflict(
                "recovery_diverged",
                "a step found an unexpected state; nothing further was \
                 written; run diagnose",
            )
            .with_detail(json!(d))),
            Err(e) => {
                tracing::error!(
                    pipeline = %pipeline,
                    error = %format!("{e:#}"),
                    "recovery operation stopped"
                );
                Err(RecoveryApiError::new(
                    500,
                    "apply_stopped",
                    "the operation stopped; run diagnose, then re-apply with \
                     the same proof",
                ))
            }
        }
    }
}

/// Load the admin token from `path` (`docs/design/recovery-cli.md`, section
/// 2.2): a regular file, not a symlink, not accessible by group or others,
/// holding a non-empty token of at least 32 bytes with no newline,
/// carriage return or NUL (one trailing newline, as editors write it, is
/// not part of the token). The file is checked through the opened handle,
/// so it cannot be swapped between the check and the read.
pub fn load_admin_token(
    path: &std::path::Path,
) -> anyhow::Result<rest_api::recovery::AdminToken> {
    use anyhow::{Context, bail};
    use std::io::Read;
    let shown = path.display();
    let link = std::fs::symlink_metadata(path)
        .with_context(|| format!("admin token file {shown}"))?;
    if link.file_type().is_symlink() {
        bail!(
            "admin token file {shown} is a symlink; point to the file itself"
        );
    }
    let file = std::fs::File::open(path)
        .with_context(|| format!("admin token file {shown}"))?;
    let meta = file
        .metadata()
        .with_context(|| format!("admin token file {shown}"))?;
    if !meta.is_file() {
        bail!("admin token file {shown} is not a regular file");
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if (meta.dev(), meta.ino()) != (link.dev(), link.ino()) {
            bail!("admin token file {shown} changed while it was opened");
        }
        let mode = meta.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            bail!(
                "admin token file {shown} is accessible by group or others \
                 (mode {mode:o}); restrict it with `chmod 600`"
            );
        }
    }
    let mut raw = Vec::new();
    file.take(64 * 1024)
        .read_to_end(&mut raw)
        .with_context(|| format!("admin token file {shown}"))?;
    let text = String::from_utf8(raw).map_err(|_| {
        anyhow::anyhow!("admin token file {shown} is not UTF-8")
    })?;
    let token = text.strip_suffix('\n').unwrap_or(&text);
    rest_api::recovery::AdminToken::new(token)
        .map_err(|e| anyhow::anyhow!("admin token file {shown}: {e}"))
}

/// The admin listener address: loopback only in this release (no TLS).
pub fn admin_addr(value: &str) -> anyhow::Result<std::net::SocketAddr> {
    let addr: std::net::SocketAddr = value.parse().map_err(|_| {
        anyhow::anyhow!(
            "invalid --admin-addr '{value}' (expected a loopback host:port, \
             e.g. 127.0.0.1:9091)"
        )
    })?;
    if !addr.ip().is_loopback() {
        anyhow::bail!(
            "--admin-addr {addr} is not a loopback address: the recovery \
             admin listener is loopback-only (use an SSH tunnel for remote \
             administration)"
        );
    }
    Ok(addr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    const TOKEN: &str = "0123456789abcdef0123456789abcdef";

    fn write(
        dir: &tempfile::TempDir,
        name: &str,
        body: &[u8],
        mode: u32,
    ) -> std::path::PathBuf {
        let p = dir.path().join(name);
        std::fs::write(&p, body).unwrap();
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(mode))
            .unwrap();
        p
    }

    #[test]
    fn the_token_file_is_validated() {
        let dir = tempfile::tempdir().unwrap();
        let ok = write(&dir, "ok", format!("{TOKEN}\n").as_bytes(), 0o600);
        assert!(load_admin_token(&ok).unwrap().matches(TOKEN.as_bytes()));
        let ro = write(&dir, "ro", TOKEN.as_bytes(), 0o400);
        assert!(load_admin_token(&ro).is_ok());
        let refused = [
            ("group", format!("{TOKEN}\n").into_bytes(), 0o640),
            ("other", TOKEN.as_bytes().to_vec(), 0o604),
            ("empty", Vec::new(), 0o600),
            ("blank", b"\n".to_vec(), 0o600),
            ("short", b"abc".to_vec(), 0o600),
            (
                "inner_newline",
                format!("{TOKEN}\n{TOKEN}").into_bytes(),
                0o600,
            ),
            ("two_newlines", format!("{TOKEN}\n\n").into_bytes(), 0o600),
            ("nul", format!("{TOKEN}\0").into_bytes(), 0o600),
            ("crlf", format!("{TOKEN}\r\n").into_bytes(), 0o600),
        ];
        for (name, body, mode) in refused {
            let p = write(&dir, name, &body, mode);
            let err = load_admin_token(&p).unwrap_err().to_string();
            assert!(!err.contains(TOKEN), "{name}: the token leaked: {err}");
        }
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&ok, &link).unwrap();
        assert!(
            load_admin_token(&link)
                .unwrap_err()
                .to_string()
                .contains("symlink")
        );
        let sub = dir.path().join("dir");
        std::fs::create_dir(&sub).unwrap();
        std::fs::set_permissions(&sub, std::fs::Permissions::from_mode(0o700))
            .unwrap();
        assert!(load_admin_token(&sub).is_err());
        assert!(load_admin_token(&dir.path().join("missing")).is_err());
    }

    #[test]
    fn the_admin_listener_is_loopback_only() {
        assert!(admin_addr("127.0.0.1:9091").is_ok());
        assert!(admin_addr("[::1]:9091").is_ok());
        assert!(admin_addr("0.0.0.0:9091").is_err());
        assert!(admin_addr("10.1.2.3:9091").is_err());
        assert!(admin_addr("localhost:9091").is_err());
    }
}
