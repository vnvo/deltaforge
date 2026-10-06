//! The recovery operation `resnapshot` (`docs/design/recovery-cli.md`,
//! section 5.1): replace the source's snapshot generation by a recovery
//! allocation of the next one (or allocate a first one), so the next start
//! copies every table again under a new anchor.
//!
//! Sink checkpoints are never deleted or advanced: the new generation's
//! start barrier moves each sink into it. The pipeline is not started. On
//! PostgreSQL, a lost slot this source provably owns is dropped as an
//! explicit step (the next start creates the new one); a foreign, ambiguous
//! or active slot refuses the plan.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use checkpoints::CheckpointStore;
use deltaforge_config::{SnapshotMode, SourceCfg};
use deltaforge_core::incident::{ActionCode, ReasonCode};
use rest_api::recovery::RecoveryApiError;
use sources::snapshot_queue::{
    GenerationControl, PolicySnapshot, QueueStore, State, Stored,
    control_digest, recovery_first, recovery_successor,
};
use sources::snapshot_recovery::{
    CheckpointKind, Engine, SlotRecreation, classify_checkpoint,
    config_fingerprint, lineage_matches, persisted_lineage,
    read_slot_recreation, write_slot_recreation,
};
use storage::adapters::incidents::IncidentRecord;
use storage::adapters::recovery::{
    CanonicalPlan, PLANNED_ID, PlanStep, StepExecutor, derived_id, plan_seed,
};

use crate::recovery::{
    OperationContext, Planned, RecoveryOperation, state_error,
};

pub const NAME: &str = "resnapshot";

const REPLACE: &str = "replace_generation";
const ALLOCATE: &str = "allocate_generation";
const RECLAIM: &str = "reclaim_plan";
const DROP_SLOT: &str = "drop_lost_slot";
const AUTHORIZE_SLOT: &str = "authorize_slot_recreation";
const AUTHORIZATION_ABSENT: &str = "authorization:absent";

const ABSENT: &str = "absent";
const UNREADABLE: &str = "unreadable";
const ITEMS_PRESENT: &str = "items:present";
const ITEMS_ABSENT: &str = "items:absent";
const SLOT_ABSENT: &str = "slot:absent";

fn manual(msg: impl Into<String>) -> RecoveryApiError {
    RecoveryApiError::new(409, "manual_repair", msg)
}

fn not_applicable(msg: impl Into<String>) -> RecoveryApiError {
    RecoveryApiError::new(409, "not_applicable", msg)
}

fn source_unavailable(what: &str, e: &anyhow::Error) -> RecoveryApiError {
    tracing::warn!(error = %format!("{e:#}"), "resnapshot: {what}");
    RecoveryApiError::new(503, "source_unavailable", format!("{what} failed"))
}

/// What `resnapshot` needs of the configured source.
struct SourceView {
    engine: Engine,
    tables: Vec<String>,
    mode: SnapshotMode,
    /// PostgreSQL: the replication slot.
    slot: Option<String>,
}

fn source_view(ctx: &OperationContext) -> Result<SourceView, RecoveryApiError> {
    match &ctx.spec.spec.source {
        SourceCfg::Postgres(c) => Ok(SourceView {
            engine: Engine::Postgres,
            tables: c.tables.clone(),
            mode: c.snapshot.mode.clone(),
            slot: Some(c.slot.clone()),
        }),
        SourceCfg::Mysql(c) => Ok(SourceView {
            engine: Engine::Mysql,
            tables: c.tables.clone(),
            mode: c.snapshot.mode.clone(),
            slot: None,
        }),
        #[allow(unreachable_patterns)]
        _ => Err(not_applicable(
            "resnapshot applies to PostgreSQL and MySQL sources",
        )),
    }
}

async fn resolved_dsn(
    ctx: &OperationContext,
) -> Result<String, RecoveryApiError> {
    let resolver =
        sources::source_secret_resolver(&ctx.spec)
            .await
            .map_err(|e| {
                source_unavailable("resolving the source credentials", &e)
            })?;
    let dsn = sources::resolve_source_dsn(&ctx.spec, resolver.as_ref())
        .await
        .map_err(|e| source_unavailable("resolving the source DSN", &e))?;
    Ok(dsn.expose().to_string())
}

fn queue_error(e: sources::snapshot_queue::QueueError) -> RecoveryApiError {
    use sources::snapshot_queue::QueueError as Q;
    match e {
        Q::Store(_) => {
            state_error("the snapshot control record", &anyhow::anyhow!("{e}"))
        }
        other => manual(format!(
            "the snapshot state cannot be interpreted ({other}); it is left \
             untouched for manual repair"
        )),
    }
}

/// The `resnapshot` operation.
pub struct Resnapshot;

#[async_trait]
impl RecoveryOperation for Resnapshot {
    fn name(&self) -> &'static str {
        NAME
    }

    fn applies_to(&self, i: &IncidentRecord) -> bool {
        i.actions.contains(&ActionCode::Resnapshot)
            && i.reason_code != ReasonCode::SnapshotStateInvalid
    }

    async fn plan(
        &self,
        ctx: &OperationContext,
    ) -> Result<Planned, RecoveryApiError> {
        let view = source_view(ctx)?;
        if view.mode == SnapshotMode::Never {
            return Err(not_applicable(
                "snapshot.mode is never: set it to initial or always to \
                 resnapshot this source",
            ));
        }
        if let Some(i) = &ctx.incident
            && !self.applies_to(i)
        {
            return Err(not_applicable(format!(
                "resnapshot does not answer incident {} ({})",
                i.incident_id.0,
                i.reason_code.as_str()
            )));
        }
        let source = ctx.source_id().to_string();
        let mut bindings = ctx.base_bindings().await?;
        bindings.insert("engine".into(), view.engine.name().into());
        bindings.insert(
            "snapshot.mode".into(),
            serde_json::to_value(&view.mode)
                .ok()
                .and_then(|v| v.as_str().map(str::to_string))
                .unwrap_or_default(),
        );
        let mut observed = BTreeMap::new();
        // The configured commit policy and cohort: a change between plan
        // and apply refuses the proof.
        bindings.insert(
            "policy.configured".into(),
            PolicySnapshot::from(&crate::pipeline_manager::snapshot_cohort_of(
                &ctx.spec,
            ))
            .digest,
        );

        let lineage = storage::adapters::source_lineage::load(
            &ctx.backend,
            &ctx.spec.metadata.tenant,
            &source,
        )
        .await
        .map_err(|e| state_error("the source lineage record", &e))?
        .ok_or_else(|| {
            manual("the source has no recorded lineage; it never verified its server")
        })?;
        let recorded = &lineage.current.descriptor;

        // Every checkpoint of the source, by the engine's own rules.
        let mut kinds = BTreeMap::new();
        for key in ctx.checkpoint_digests().await?.keys() {
            let raw = ctx.ckpt_store.get_raw(key).await.map_err(|e| {
                state_error("a checkpoint", &anyhow::anyhow!(e))
            })?;
            if let Some(raw) = raw {
                let kind = classify_checkpoint(view.engine, &raw);
                if kind == CheckpointKind::Malformed {
                    return Err(manual(format!(
                        "checkpoint {key} cannot be interpreted; it is left \
                         untouched for manual repair"
                    )));
                }
                kinds.insert(key.clone(), (kind, raw));
            }
        }

        // What the recovery generation freezes: the configuration now.
        let configured_policy = PolicySnapshot::from(
            &crate::pipeline_manager::snapshot_cohort_of(&ctx.spec),
        );
        configured_policy.validate().map_err(not_applicable)?;
        let configured_fp = config_fingerprint(view.engine, &view.tables);
        bindings.insert("config.fingerprint".into(), configured_fp.clone());

        let queue = QueueStore::new(ctx.backend.clone(), &source);
        let mut steps = Vec::new();
        let next_generation;
        match queue.read().await.map_err(queue_error)? {
            Some(Stored::Legacy { .. }) => {
                return Err(not_applicable(
                    "the snapshot state is from an earlier release: the next \
                     start classifies it (and copies it once more if its \
                     completion is unproven); resume the pipeline instead",
                ));
            }
            Some(Stored::Current { version, control }) => {
                match lineage_matches(&control.lineage, recorded) {
                    Some(true) => {}
                    Some(false) => {
                        return Err(manual(
                            "the snapshot control record belongs to another \
                             source lineage; it is left untouched",
                        ));
                    }
                    None => {
                        return Err(manual(
                            "the snapshot control record's lineage cannot be \
                             related to the source's recorded lineage",
                        ));
                    }
                }
                if control.state == State::RowsProduced
                    && control.blocked.is_none()
                {
                    return Err(not_applicable(
                        "the generation produced all its rows: the next start \
                         completes it or replaces it; resume the pipeline",
                    ));
                }
                let next = recovery_successor(
                    &control,
                    &configured_fp,
                    configured_policy.clone(),
                );
                next_generation = next.generation;
                bindings.insert("control.version".into(), version.to_string());
                bindings.insert(
                    "control.generation".into(),
                    control.generation.to_string(),
                );
                bindings.insert(
                    "control.state".into(),
                    serde_json::to_value(control.state)
                        .ok()
                        .and_then(|v| v.as_str().map(str::to_string))
                        .unwrap_or_default(),
                );
                bindings.insert(
                    "control.blocked".into(),
                    control
                        .blocked
                        .as_ref()
                        .map_or("no".into(), |b| b.reason.clone()),
                );
                bindings.insert(
                    "control.chain".into(),
                    control.snapshot_chain.clone(),
                );
                bindings.insert(
                    "next.generation".into(),
                    next.generation.to_string(),
                );
                steps.push(step(
                    REPLACE,
                    control_digest(&control),
                    control_digest(&next),
                    &next,
                    [
                        ("generation", control.generation.to_string()),
                        ("next_generation", next.generation.to_string()),
                        ("snapshot_chain", next.snapshot_chain.clone()),
                    ],
                ));
                if queue
                    .has_items(control.generation)
                    .await
                    .map_err(queue_error)?
                {
                    steps.push(PlanStep {
                        name: RECLAIM.into(),
                        pre: ITEMS_PRESENT.into(),
                        post: ITEMS_ABSENT.into(),
                        detail: BTreeMap::from([(
                            "generation".into(),
                            control.generation.to_string(),
                        )]),
                    });
                }
            }
            None => {
                if let Some((key, _)) = kinds
                    .iter()
                    .find(|(_, (k, _))| *k == CheckpointKind::Snapshot)
                {
                    return Err(manual(format!(
                        "checkpoint {key} is a snapshot position but the \
                         snapshot control record is missing; manual repair is \
                         required"
                    )));
                }
                for (key, (_, raw)) in &kinds {
                    let foreign = sources::snapshot_recovery::stream_checkpoint_foreign(
                        view.engine,
                        &ctx.backend,
                        &source,
                        raw,
                        &lineage.current.lineage_hash,
                    )
                    .await
                    .map_err(|e| {
                        manual(format!(
                            "checkpoint {key} cannot be interpreted ({e}); manual \
                             repair is required"
                        ))
                    })?;
                    if let Some(why) = foreign {
                        return Err(manual(format!(
                            "checkpoint {key} is not provably this source's: {why}"
                        )));
                    }
                }
                let persisted = persisted_lineage(recorded).ok_or_else(|| {
                    manual("the source's recorded lineage cannot seed a snapshot chain")
                })?;
                // The chain is derived from the plan seed (see
                // `finalize_chain`); while seeding the plan carries
                // `PLANNED_ID` in its place.
                let first = recovery_first(
                    PLANNED_ID.into(),
                    persisted,
                    &configured_fp,
                    configured_policy.clone(),
                );
                next_generation = 1;
                bindings.insert("control".into(), ABSENT.into());
                bindings.insert("next.generation".into(), "1".into());
                steps.push(step(
                    ALLOCATE,
                    ABSENT.into(),
                    control_digest(&first),
                    &first,
                    [("next_generation", "1".to_string())],
                ));
            }
        }

        if let Some(slot) = &view.slot {
            let dsn = resolved_dsn(ctx).await?;
            let obs = sources::postgres::postgres_slot_owner::observe_slot(
                &dsn,
                slot,
                &ctx.pipeline,
                &source,
                &ctx.ckpt_store,
            )
            .await
            .map_err(|e| {
                source_unavailable("observing the replication slot", &e)
            })?;
            bindings.insert("slot".into(), obs.digest());
            observed.insert(
                "slot".into(),
                serde_json::to_string(&obs).unwrap_or_default(),
            );
            let authorization =
                match read_slot_recreation(&ctx.backend, &source).await {
                    Ok(None) => AUTHORIZATION_ABSENT.to_string(),
                    Ok(Some((_, a))) => a.digest(),
                    Err(e) => {
                        return Err(state_error(
                            "the slot recreation authorization",
                            &e,
                        ));
                    }
                };
            bindings.insert("slot.authorization".into(), authorization.clone());
            // The single-use authorization for the recovery generation's
            // start to recreate the slot this source owned.
            let authorize = |steps: &mut Vec<PlanStep>| {
                steps.push(PlanStep {
                    name: AUTHORIZE_SLOT.into(),
                    pre: authorization.clone(),
                    post: SlotRecreation::authorized(
                        &source,
                        &ctx.pipeline,
                        slot,
                        next_generation,
                        &obs.owner_record,
                    )
                    .digest(),
                    detail: BTreeMap::from([
                        ("slot".into(), slot.clone()),
                        ("generation".into(), next_generation.to_string()),
                        ("owner_record".into(), obs.owner_record.clone()),
                    ]),
                });
            };
            match &obs.present {
                None if obs.owner_proven => authorize(&mut steps),
                None => {
                    return Err(manual(format!(
                        "replication slot '{slot}' is absent and the ownership \
                         record does not prove this source created it (missing, \
                         partial, foreign or changed); recovery never \
                         authorizes recreating it"
                    )));
                }
                Some(p) if !p.owned || p.active => {
                    return Err(manual(format!(
                        "replication slot '{slot}' exists and is {}; recovery \
                         never drops or replaces it",
                        if p.active {
                            "active"
                        } else {
                            "not provably owned by this source"
                        }
                    )));
                }
                Some(_) if obs.lost() => {
                    steps.push(PlanStep {
                        name: DROP_SLOT.into(),
                        pre: obs.digest(),
                        post: SLOT_ABSENT.into(),
                        detail: BTreeMap::from([("slot".into(), slot.clone())]),
                    });
                    authorize(&mut steps);
                }
                Some(_) => {
                    observed.insert(
                        "slot.next_start".into(),
                        "owned and inactive: the next start re-anchors it"
                            .into(),
                    );
                }
            }
        }

        let consequences = vec![
            "THE COMPLETE SNAPSHOT IS COPIED AGAIN: every captured table is \
             read in full under a new anchor"
                .to_string(),
            "sinks receive every row again: duplicates are expected \
             (at-least-once)"
                .to_string(),
            "existing sink checkpoints are kept, never deleted or advanced; \
             each sink moves into the new generation through its start barrier"
                .to_string(),
            "the pipeline is not started: resume it explicitly afterwards"
                .to_string(),
            "only the incident named in this plan is resolved; a later \
             failure raises its own incident"
                .to_string(),
        ];
        let resolves = ctx
            .incident
            .as_ref()
            .map(|i| vec![i.incident_id.clone()])
            .unwrap_or_default();
        let plan = finalize_chain(CanonicalPlan::new(
            NAME,
            &ctx.pipeline,
            &source,
            bindings,
            steps,
            consequences,
        ));
        Ok(Planned {
            plan,
            resolves,
            observed,
        })
    }

    async fn executor(
        &self,
        ctx: &OperationContext,
        plan: &CanonicalPlan,
    ) -> Result<Box<dyn StepExecutor>, RecoveryApiError> {
        let view = source_view(ctx)?;
        let dsn = if plan.steps.iter().any(|s| s.name == DROP_SLOT) {
            Some(resolved_dsn(ctx).await?)
        } else {
            None
        };
        Ok(Box::new(Exec {
            proof: plan.proof(),
            backend: ctx.backend.clone(),
            queue: QueueStore::new(ctx.backend.clone(), ctx.source_id()),
            ckpt: Arc::clone(&ctx.ckpt_store),
            pipeline: ctx.pipeline.clone(),
            source: ctx.source_id().to_string(),
            slot: view.slot,
            dsn,
        }))
    }
}

/// A first recovery allocation's chain: derived from the seed of the plan
/// that carries `PLANNED_ID` in its place, then put into the final plan
/// (its control record, post-state and detail). Other plans are returned
/// unchanged.
fn finalize_chain(seeded: CanonicalPlan) -> CanonicalPlan {
    let Some(i) = seeded.steps.iter().position(|s| s.name == ALLOCATE) else {
        return seeded;
    };
    let chain = derived_id(&plan_seed(&seeded), "snapshot_chain");
    let mut plan = seeded;
    let step = &mut plan.steps[i];
    let mut first: GenerationControl =
        serde_json::from_str(&step.detail["control"])
            .expect("a planned control record");
    first.snapshot_chain = chain.clone();
    step.post = control_digest(&first);
    step.detail.insert(
        "control".into(),
        serde_json::to_string(&first).expect("a control record serializes"),
    );
    step.detail.insert("snapshot_chain".into(), chain);
    plan
}

fn step<const N: usize>(
    name: &str,
    pre: String,
    post: String,
    control: &GenerationControl,
    detail: [(&str, String); N],
) -> PlanStep {
    let mut d: BTreeMap<String, String> = detail
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
    d.insert(
        "control".into(),
        serde_json::to_string(control).expect("a control record serializes"),
    );
    PlanStep {
        name: name.into(),
        pre,
        post,
        detail: d,
    }
}

struct Exec {
    proof: String,
    backend: storage::ArcStorageBackend,
    queue: QueueStore,
    ckpt: Arc<dyn CheckpointStore>,
    pipeline: String,
    source: String,
    slot: Option<String>,
    dsn: Option<String>,
}

impl Exec {
    fn generation(step: &PlanStep) -> anyhow::Result<u64> {
        step.detail
            .get("generation")
            .and_then(|g| g.parse().ok())
            .ok_or_else(|| {
                anyhow::anyhow!("step {} names no generation", step.name)
            })
    }

    fn planned_control(step: &PlanStep) -> anyhow::Result<GenerationControl> {
        let raw = step.detail.get("control").ok_or_else(|| {
            anyhow::anyhow!("step {} carries no control record", step.name)
        })?;
        let control: GenerationControl = serde_json::from_str(raw)?;
        anyhow::ensure!(
            control_digest(&control) == step.post,
            "step {} carries a control record that is not its post-state",
            step.name
        );
        Ok(control)
    }

    fn slot_args(&self) -> anyhow::Result<(&str, &str)> {
        match (&self.dsn, &self.slot) {
            (Some(d), Some(s)) => Ok((d, s)),
            _ => anyhow::bail!("no replication slot to act on"),
        }
    }
}

#[async_trait]
impl StepExecutor for Exec {
    async fn observe(&self, step: &PlanStep) -> anyhow::Result<String> {
        match step.name.as_str() {
            REPLACE | ALLOCATE => Ok(match self.queue.read().await {
                Ok(None) => ABSENT.into(),
                Ok(Some(Stored::Current { control, .. })) => {
                    control_digest(&control)
                }
                Ok(Some(Stored::Legacy { .. })) | Err(_) => UNREADABLE.into(),
            }),
            AUTHORIZE_SLOT => Ok(
                match read_slot_recreation(&self.backend, &self.source).await {
                    Ok(None) => AUTHORIZATION_ABSENT.into(),
                    Ok(Some((_, mut a))) => {
                        // The proof is observed as the plan's empty one.
                        if a.proof == self.proof {
                            a.proof = String::new();
                        }
                        a.digest()
                    }
                    Err(_) => UNREADABLE.into(),
                },
            ),
            RECLAIM => {
                let g = Self::generation(step)?;
                Ok(if self.queue.has_items(g).await? {
                    ITEMS_PRESENT.into()
                } else {
                    ITEMS_ABSENT.into()
                })
            }
            DROP_SLOT => {
                let (dsn, slot) = self.slot_args()?;
                let obs = sources::postgres::postgres_slot_owner::observe_slot(
                    dsn,
                    slot,
                    &self.pipeline,
                    &self.source,
                    &self.ckpt,
                )
                .await?;
                Ok(if obs.present.is_none() {
                    SLOT_ABSENT.into()
                } else {
                    obs.digest()
                })
            }
            other => anyhow::bail!("unknown resnapshot step {other}"),
        }
    }

    async fn perform(
        &self,
        step: &PlanStep,
    ) -> anyhow::Result<BTreeMap<String, String>> {
        match step.name.as_str() {
            REPLACE => {
                let next = Self::planned_control(step)?;
                let Some(Stored::Current { version, control }) =
                    self.queue.read().await?
                else {
                    anyhow::bail!(
                        "the control record changed before the replacement"
                    );
                };
                anyhow::ensure!(
                    control_digest(&control) == step.pre,
                    "the control record changed before the replacement"
                );
                self.queue.write_recovery(Some(version), &next).await?;
                Ok(BTreeMap::from([(
                    "generation".into(),
                    next.generation.to_string(),
                )]))
            }
            ALLOCATE => {
                let first = Self::planned_control(step)?;
                self.queue.write_recovery(None, &first).await?;
                Ok(BTreeMap::from([
                    ("generation".into(), "1".into()),
                    ("snapshot_chain".into(), first.snapshot_chain),
                ]))
            }
            AUTHORIZE_SLOT => {
                let get = |k: &str| {
                    step.detail
                        .get(k)
                        .cloned()
                        .ok_or_else(|| anyhow::anyhow!("step names no {k}"))
                };
                let (slot, generation, owner) = (
                    get("slot")?,
                    get("generation")?.parse::<u64>()?,
                    get("owner_record")?,
                );
                let mut auth = SlotRecreation::authorized(
                    &self.source,
                    &self.pipeline,
                    &slot,
                    generation,
                    &owner,
                );
                anyhow::ensure!(
                    auth.digest() == step.post,
                    "the authorization is not the planned one"
                );
                auth.proof = self.proof.clone();
                let current =
                    read_slot_recreation(&self.backend, &self.source).await?;
                write_slot_recreation(
                    &self.backend,
                    current.map(|(v, _)| v),
                    &auth,
                )
                .await?;
                Ok(BTreeMap::from([(
                    "slot_recreation".into(),
                    format!("authorized for generation {generation}"),
                )]))
            }
            RECLAIM => {
                let n = self.queue.reclaim(Self::generation(step)?).await?;
                Ok(BTreeMap::from([("reclaimed_items".into(), n.to_string())]))
            }
            DROP_SLOT => {
                let (dsn, slot) = self.slot_args()?;
                sources::postgres::postgres_slot_owner::drop_lost_owned_slot(
                    dsn,
                    slot,
                    &self.pipeline,
                    &self.source,
                    &self.ckpt,
                    &step.pre,
                )
                .await?;
                Ok(BTreeMap::from([(
                    "slot".into(),
                    "dropped_owned_lost_slot".into(),
                )]))
            }
            other => anyhow::bail!("unknown resnapshot step {other}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline_manager::{PipelineManager, PipelineRuntime};
    use crate::recovery::RecoveryService;
    use deltaforge_core::incident::{
        CauseCode, Component, IncidentDraft, IncidentId, Retryability,
        SafetyState,
    };
    use rest_api::recovery::{
        ApplyRequest, Caller, PlanRequest, RecoveryController,
    };
    use serde_json::Value;
    use sources::snapshot_queue::{
        AllocationMark, Blocked, CONTROL_NS, ITEM_FORMAT, PlanItem, control_key,
    };
    use storage::adapters::LineageDescriptor;
    use storage::adapters::incidents::{IncidentStore, Transition};
    use storage::adapters::recovery::{
        RecordState, RecoveryRecord, RecoveryStore,
    };
    use storage::{ArcStorageBackend, MemoryStorageBackend};

    const UUID: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
    const SOURCE: &str = "mysql";
    const SINK_KEY: &str = "mysql::sink::redis";

    struct F {
        backend: ArcStorageBackend,
        manager: Arc<PipelineManager>,
        service: RecoveryService,
        queue: QueueStore,
        lineage_hash: String,
    }

    fn spec(mode: SnapshotMode) -> deltaforge_config::PipelineSpec {
        let mut spec = crate::pipeline_manager::tests_support_spec("p");
        if let SourceCfg::Mysql(c) = &mut spec.spec.source {
            c.snapshot.mode = mode;
            c.tables = vec!["shop.*".into()];
        }
        spec
    }

    async fn fixture(mode: SnapshotMode) -> F {
        let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
        let manager = Arc::new(
            PipelineManager::with_backend(backend.clone())
                .await
                .unwrap(),
        );
        manager.pipelines.write().insert(
            "p".into(),
            PipelineRuntime::held(
                spec(mode),
                IncidentStore::new(backend.clone(), "p"),
            ),
        );
        let est = storage::adapters::source_lineage::establish(
            &backend,
            "acme",
            SOURCE,
            LineageDescriptor::Mysql {
                server_uuid: UUID.into(),
            },
        )
        .await
        .unwrap();
        let service = RecoveryService::new(Arc::clone(&manager))
            .with_operation(Arc::new(Resnapshot));
        F {
            queue: QueueStore::new(backend.clone(), SOURCE),
            backend,
            manager,
            service,
            lineage_hash: est.record.current.lineage_hash,
        }
    }

    fn policy(f: &F) -> PolicySnapshot {
        let spec = f.manager.pipelines.read()["p"].spec.clone();
        PolicySnapshot::from(&crate::pipeline_manager::snapshot_cohort_of(
            &spec,
        ))
    }

    fn uuid_lineage(u: &str) -> sources::snapshot_generation::PersistedLineage {
        persisted_lineage(&LineageDescriptor::Mysql {
            server_uuid: u.into(),
        })
        .unwrap()
    }

    /// Generation 1 with one plan item, blocked; returns the incident.
    async fn blocked(f: &F) -> IncidentId {
        let (v, c) = f
            .queue
            .allocate_first(uuid_lineage(UUID), "fp", policy(f))
            .await
            .unwrap();
        f.queue
            .put_item(
                &c,
                &PlanItem {
                    item_format: ITEM_FORMAT,
                    qualifier: "shop".into(),
                    table: "t".into(),
                    identity: serde_json::json!(["id"]),
                    cursor_kind:
                        sources::durable_checkpoint::CursorKind::Signed,
                    schema_version: 1,
                    signature: "s".into(),
                },
            )
            .await
            .unwrap();
        let id = IncidentStore::new(f.backend.clone(), "p")
            .raise(
                &IncidentDraft::new(
                    ReasonCode::SnapshotAnchorUnavailable,
                    Component::Source { id: SOURCE.into() },
                    Retryability::OperatorAction,
                    SafetyState::HaltedSafe,
                    CauseCode::SourceLineage,
                )
                .with_actions(&[ActionCode::Resnapshot]),
                1,
            )
            .await
            .unwrap()
            .record()
            .incident_id
            .clone();
        f.queue
            .block(
                v,
                &c,
                Blocked {
                    reason: "snapshot_anchor_unavailable".into(),
                    incident: id.0.clone(),
                    since_ms: 0,
                },
            )
            .await
            .unwrap();
        id
    }

    fn stream_checkpoint(lineage: Option<&str>) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "file": "binlog.000003", "pos": 1234, "gtid_set": null, "lineage": lineage,
        }))
        .unwrap()
    }

    fn req(incident: Option<&IncidentId>) -> PlanRequest {
        PlanRequest {
            operation: NAME.into(),
            incident: incident.map(|i| i.0.clone()),
            args: Default::default(),
        }
    }

    async fn plan(
        f: &F,
        incident: Option<&IncidentId>,
    ) -> Result<Value, RecoveryApiError> {
        f.service.plan("p", req(incident)).await
    }

    async fn apply(
        f: &F,
        incident: Option<&IncidentId>,
        proof: &str,
    ) -> Result<Value, RecoveryApiError> {
        f.service
            .apply(
                "p",
                ApplyRequest {
                    plan: req(incident),
                    expect_proof: proof.into(),
                    actor: "alice".into(),
                    reason: "binlog purged".into(),
                },
                Caller {
                    origin: "127.0.0.1:1".into(),
                    credential: "token",
                },
            )
            .await
    }

    async fn control(f: &F) -> (u64, GenerationControl) {
        match f.queue.read().await.unwrap().unwrap() {
            Stored::Current { version, control } => (version, *control),
            other => panic!("{other:?}"),
        }
    }

    fn proof(v: &Value) -> String {
        v["proof"].as_str().unwrap().to_string()
    }

    #[tokio::test]
    async fn a_blocked_generation_becomes_a_recovery_allocation() {
        let f = fixture(SnapshotMode::Initial).await;
        let id = blocked(&f).await;
        let cp = stream_checkpoint(Some(&f.lineage_hash));
        f.manager.ckpt_store.put_raw(SINK_KEY, &cp).await.unwrap();
        let planned = plan(&f, Some(&id)).await.unwrap();
        let b = &planned["plan"]["bindings"];
        assert_eq!(b["control.generation"], "1");
        assert_eq!(b["next.generation"], "2");
        assert_eq!(b["snapshot.mode"], "initial");
        assert_eq!(b["incident"], id.0.as_str());
        assert!(b[&format!("checkpoint.{SINK_KEY}")].is_string());
        let steps: Vec<&str> = planned["plan"]["steps"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["name"].as_str().unwrap())
            .collect();
        assert_eq!(steps, vec![REPLACE, RECLAIM]);
        assert!(
            planned["plan"]["consequences"][0]
                .as_str()
                .unwrap()
                .contains("COMPLETE SNAPSHOT IS COPIED AGAIN")
        );
        let p = proof(&planned);
        let done = apply(&f, Some(&id), &p).await.unwrap();
        assert_eq!(done["state"], "completed");
        let (v2, c2) = control(&f).await;
        assert_eq!(c2.generation, 2);
        assert_eq!(c2.allocation, Some(AllocationMark::Recovery));
        assert_eq!(c2.blocked, None);
        assert_eq!(c2.adoption, sources::snapshot_queue::Adoption::Pending);
        assert_eq!(c2.replaced, Some(1));
        assert!(
            !f.queue.has_items(1).await.unwrap(),
            "generation 1 reclaimed"
        );
        // Checkpoints untouched; the incident resolved by this proof; the
        // pipeline still stopped.
        assert_eq!(
            f.manager.ckpt_store.get_raw(SINK_KEY).await.unwrap(),
            Some(cp)
        );
        let incidents = IncidentStore::new(f.backend.clone(), "p");
        assert!(
            incidents
                .get(&id)
                .await
                .unwrap()
                .unwrap()
                .status
                .is_resolved()
        );
        assert!(incidents.audit_trail(50).await.unwrap().iter().any(|e| matches!(
            &e.transition,
            Transition::ResolvedByRecovery { applied, .. } if applied.proof == p
        )));
        assert_eq!(f.manager.pipelines.read()["p"].info().status, "stopped");
        // The same proof again: nothing recomputed, audited or written.
        let trail = incidents.audit_trail(50).await.unwrap().len();
        let again = apply(&f, Some(&id), &p).await.unwrap();
        assert_eq!(again["already_completed"], true);
        assert_eq!(control(&f).await.0, v2);
        assert_eq!(incidents.audit_trail(50).await.unwrap().len(), trail);
    }

    /// The recovery generation freezes the configuration of now, not the
    /// blocked generation's: a policy changed since is frozen (so the next
    /// start runs it in place), and bound by the plan.
    #[tokio::test]
    async fn the_recovery_generation_freezes_the_configured_policy() {
        let f = fixture(SnapshotMode::Initial).await;
        let id = blocked(&f).await;
        let frozen = control(&f).await.1.policy;
        f.manager
            .pipelines
            .write()
            .get_mut("p")
            .unwrap()
            .spec
            .spec
            .commit_policy = Some(deltaforge_config::CommitPolicy::All);
        let configured = policy(&f);
        assert_ne!(frozen, configured);
        let planned = plan(&f, Some(&id)).await.unwrap();
        assert_eq!(
            planned["plan"]["bindings"]["policy.configured"],
            configured.digest.as_str()
        );
        apply(&f, Some(&id), &proof(&planned)).await.unwrap();
        let (_, c) = control(&f).await;
        assert_eq!(c.policy, configured);
        assert_eq!(
            c.config_fingerprint,
            config_fingerprint(Engine::Mysql, &["shop.*".to_string()])
        );
    }

    #[tokio::test]
    async fn a_crash_before_or_after_the_replacement_resumes_once() {
        for after in [false, true] {
            let f = fixture(SnapshotMode::Initial).await;
            blocked(&f).await;
            let planned = plan(&f, None).await.unwrap();
            let p = proof(&planned);
            // The record as an apply left it: claimed, step 0 not advanced.
            let canonical: storage::adapters::recovery::CanonicalPlan =
                serde_json::from_value(planned["plan"].clone()).unwrap();
            let rec = RecoveryRecord::new(
                canonical.clone(),
                storage::adapters::incidents::RecoveryAudit {
                    operation: NAME.into(),
                    proof: p.clone(),
                    asserted_actor: "alice".into(),
                    actor_verified: false,
                    credential: "token".into(),
                    origin: None,
                    reason: "r".into(),
                },
                vec![],
            );
            RecoveryStore::new(f.backend.clone(), "p")
                .claim(&rec)
                .await
                .unwrap();
            if after {
                let (v, c) = control(&f).await;
                f.queue
                    .write_recovery(
                        Some(v),
                        &recovery_successor(
                            &c,
                            &config_fingerprint(
                                Engine::Mysql,
                                &["shop.*".to_string()],
                            ),
                            policy(&f),
                        ),
                    )
                    .await
                    .unwrap();
            }
            let done = apply(&f, None, &p).await.unwrap();
            assert_eq!(done["state"], "completed", "after={after}");
            assert_eq!(
                control(&f).await.1.generation,
                2,
                "after={after}: no extra generation"
            );
            let (_, stored) = RecoveryStore::new(f.backend.clone(), "p")
                .read()
                .await
                .unwrap()
                .unwrap();
            assert_eq!(stored.state, RecordState::Completed);
            let found = stored.verified[0].found;
            assert_eq!(
                found,
                if after {
                    storage::adapters::recovery::Observed::Post
                } else {
                    storage::adapters::recovery::Observed::Pre
                }
            );
        }
    }

    #[tokio::test]
    async fn a_stale_proof_mutates_nothing() {
        type Drift =
            fn(&F, &IncidentId) -> futures::future::BoxFuture<'static, ()>;
        let drifts: Vec<(&str, Drift)> = vec![
            ("checkpoint", |f, _| {
                let s = Arc::clone(&f.manager.ckpt_store);
                let cp = stream_checkpoint(Some(&f.lineage_hash));
                Box::pin(async move {
                    let mut v: Value = serde_json::from_slice(&cp).unwrap();
                    v["pos"] = 999.into();
                    s.put_raw(SINK_KEY, &serde_json::to_vec(&v).unwrap())
                        .await
                        .unwrap()
                })
            }),
            ("lineage", |f, _| {
                let b = f.backend.clone();
                Box::pin(async move {
                    storage::adapters::source_lineage::establish(
                        &b,
                        "acme",
                        SOURCE,
                        LineageDescriptor::Mysql {
                            server_uuid: "4e11fa47-71ca-11e1-9e33-c80aa9429562"
                                .into(),
                        },
                    )
                    .await
                    .unwrap();
                })
            }),
            ("policy", |f, _| {
                f.manager
                    .pipelines
                    .write()
                    .get_mut("p")
                    .unwrap()
                    .spec
                    .spec
                    .commit_policy = Some(deltaforge_config::CommitPolicy::All);
                Box::pin(async {})
            }),
            ("incident", |f, id| {
                let s = IncidentStore::new(f.backend.clone(), "p");
                let id = id.clone();
                Box::pin(async move {
                    s.acknowledge(&id, "bob", None, "seen")
                        .await
                        .unwrap()
                        .unwrap();
                })
            }),
            ("control version", |f, _| {
                let b = f.backend.clone();
                Box::pin(async move {
                    let (v, bytes) = b
                        .slot_get(CONTROL_NS, &control_key(SOURCE))
                        .await
                        .unwrap()
                        .unwrap();
                    assert!(
                        b.slot_cas(CONTROL_NS, &control_key(SOURCE), v, &bytes)
                            .await
                            .unwrap()
                    );
                })
            }),
        ];
        for (what, drift) in drifts {
            let f = fixture(SnapshotMode::Initial).await;
            let id = blocked(&f).await;
            let p = proof(&plan(&f, Some(&id)).await.unwrap());
            drift(&f, &id).await;
            let before = f
                .backend
                .slot_get(CONTROL_NS, &control_key(SOURCE))
                .await
                .unwrap();
            let e = apply(&f, Some(&id), &p).await.unwrap_err();
            // A changed lineage makes the control record foreign: refused
            // before any proof comparison.
            let want = if what == "lineage" {
                "manual_repair"
            } else {
                "proof_mismatch"
            };
            assert_eq!(e.code, want, "{what}: {e:?}");
            assert_eq!(
                f.backend
                    .slot_get(CONTROL_NS, &control_key(SOURCE))
                    .await
                    .unwrap(),
                before,
                "{what}: the control record changed"
            );
            assert!(
                RecoveryStore::new(f.backend.clone(), "p")
                    .read()
                    .await
                    .unwrap()
                    .is_none(),
                "{what}"
            );
        }
    }

    #[tokio::test]
    async fn unsupported_or_uninterpretable_state_fails_closed() {
        // Mode never.
        let f = fixture(SnapshotMode::Never).await;
        blocked(&f).await;
        assert_eq!(plan(&f, None).await.unwrap_err().code, "not_applicable");
        // A snapshot-chain checkpoint without its control record.
        let f = fixture(SnapshotMode::Initial).await;
        f.manager
            .ckpt_store
            .put_raw(
                SINK_KEY,
                &sources::snapshot_position::encode_adopted("c", 2, "d"),
            )
            .await
            .unwrap();
        let e = plan(&f, None).await.unwrap_err();
        assert_eq!(e.code, "manual_repair");
        assert!(e.message.contains("control record is missing"), "{e:?}");
        // A corrupt control record, left untouched.
        let f = fixture(SnapshotMode::Initial).await;
        f.backend
            .slot_create(CONTROL_NS, &control_key(SOURCE), b"{garbage")
            .await
            .unwrap();
        assert_eq!(plan(&f, None).await.unwrap_err().code, "manual_repair");
        assert_eq!(
            f.backend
                .slot_get(CONTROL_NS, &control_key(SOURCE))
                .await
                .unwrap()
                .unwrap()
                .1,
            b"{garbage"
        );
        // A control record of another lineage.
        let f = fixture(SnapshotMode::Initial).await;
        f.queue
            .allocate_first(
                uuid_lineage("4e11fa47-71ca-11e1-9e33-c80aa9429562"),
                "fp",
                policy(&f),
            )
            .await
            .unwrap();
        assert_eq!(plan(&f, None).await.unwrap_err().code, "manual_repair");
        // Stream checkpoints of unverified or another server's lineage,
        // without a control record.
        let other = "ab".repeat(16);
        for lineage in [None, Some(other.as_str())] {
            let f = fixture(SnapshotMode::Initial).await;
            f.manager
                .ckpt_store
                .put_raw(SINK_KEY, &stream_checkpoint(lineage))
                .await
                .unwrap();
            let e = plan(&f, None).await.unwrap_err();
            assert_eq!(e.code, "manual_repair", "{lineage:?}");
            // Without a lineage hash the engine's own comparator already
            // refuses the position; with another server's it is foreign.
            if lineage.is_some() {
                assert!(
                    e.message.contains("not provably this source's"),
                    "{e:?}"
                );
            }
        }
        // A malformed checkpoint, with and without a control record.
        for with_control in [false, true] {
            let f = fixture(SnapshotMode::Initial).await;
            if with_control {
                blocked(&f).await;
            }
            f.manager
                .ckpt_store
                .put_raw(SINK_KEY, b"\xff")
                .await
                .unwrap();
            let e = plan(&f, None).await.unwrap_err();
            assert_eq!(e.code, "manual_repair");
            assert!(e.message.contains("left untouched"), "{e:?}");
        }
    }

    #[tokio::test]
    async fn a_first_recovery_allocation_over_verified_cdc_checkpoints() {
        let f = fixture(SnapshotMode::Initial).await;
        let cp = stream_checkpoint(Some(&f.lineage_hash));
        f.manager.ckpt_store.put_raw(SINK_KEY, &cp).await.unwrap();
        let a = plan(&f, None).await.unwrap();
        let b = plan(&f, None).await.unwrap();
        assert_eq!(proof(&a), proof(&b), "a deterministic chain and proof");
        apply(&f, None, &proof(&a)).await.unwrap();
        let (_, c) = control(&f).await;
        assert_eq!(c.generation, 1);
        assert_eq!(c.allocation, Some(AllocationMark::Recovery));
        assert_eq!(c.policy, policy(&f));
        // The chain is derived from the plan seed and is in the plan.
        assert_ne!(c.snapshot_chain, PLANNED_ID);
        assert_eq!(c.snapshot_chain.len(), 32);
        let allocate = &a["plan"]["steps"][0];
        assert_eq!(
            allocate["detail"]["snapshot_chain"],
            c.snapshot_chain.as_str()
        );
        assert_eq!(allocate["post"], control_digest(&c).as_str());
    }
}
