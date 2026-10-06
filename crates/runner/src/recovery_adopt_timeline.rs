//! The recovery operation `pg-adopt-timeline` (`docs/design/recovery-cli.md`,
//! section 5.2), over `sources::postgres::postgres_adoption`: for a
//! `timeline_unrecorded` incident, create the source's continuity record
//! for the server's current timeline at transition 0, proven at the
//! checkpoint position F. No checkpoint moves; the pipeline is not started.
//!
//! The record's chain id is derived from the plan seed (the plan with the
//! chain at `PLANNED_ID`), then put into the final plan, so a recomputed
//! plan yields the same chain and proof.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use checkpoints::CheckpointStore;
use deltaforge_config::SourceCfg;
use deltaforge_core::incident::{EvidenceKey, EvidenceValue, ReasonCode};
use rest_api::recovery::RecoveryApiError;
use sources::postgres::postgres_adoption::{
    AdoptionFacts, AdoptionInput, AdoptionPlan, AdoptionRefusal,
    CONTINUITY_ABSENT, FactsHook, adopted_record, apply_adoption,
    continuity_state, plan_adoption, record_digest,
};
use storage::adapters::incidents::IncidentRecord;
use storage::adapters::recovery::{
    CanonicalPlan, PLANNED_ID, PlanStep, StepExecutor, derived_id, plan_seed,
};

use crate::recovery::{OperationContext, Planned, RecoveryOperation};

pub const NAME: &str = "pg-adopt-timeline";
const CREATE: &str = "create_continuity_record";

fn not_applicable(msg: impl Into<String>) -> RecoveryApiError {
    RecoveryApiError::new(409, "not_applicable", msg)
}

fn refusal(r: AdoptionRefusal) -> RecoveryApiError {
    match r {
        AdoptionRefusal::NotApplicable(m) => not_applicable(m),
        AdoptionRefusal::Precondition(m) => {
            RecoveryApiError::new(409, "precondition_failed", m)
        }
        AdoptionRefusal::Manual(m) => {
            RecoveryApiError::new(409, "manual_repair", m)
        }
        AdoptionRefusal::Unavailable(e) => {
            tracing::warn!(error = %format!("{e:#}"), "pg-adopt-timeline: the source");
            RecoveryApiError::new(
                503,
                "source_unavailable",
                "the source or the state store could not be read",
            )
        }
    }
}

/// Everything the core needs, owned.
struct Target {
    dsn: String,
    slot: String,
    publication: String,
    source: String,
    tenant: String,
    backend: storage::ArcStorageBackend,
    checkpoints: Arc<dyn CheckpointStore>,
    sinks: Vec<String>,
    hook: Option<FactsHook>,
}

impl Target {
    async fn of(
        ctx: &OperationContext,
        hook: Option<FactsHook>,
    ) -> Result<Self, RecoveryApiError> {
        let SourceCfg::Postgres(c) = &ctx.spec.spec.source else {
            return Err(not_applicable(
                "pg-adopt-timeline applies to PostgreSQL sources",
            ));
        };
        let resolver = sources::source_secret_resolver(&ctx.spec)
            .await
            .map_err(|e| refusal(AdoptionRefusal::Unavailable(e)))?;
        let dsn = sources::resolve_source_dsn(&ctx.spec, resolver.as_ref())
            .await
            .map_err(|e| refusal(AdoptionRefusal::Unavailable(e)))?;
        Ok(Self {
            dsn: dsn.expose().to_string(),
            slot: c.slot.clone(),
            publication: c.publication.clone(),
            source: ctx.source_id().to_string(),
            tenant: ctx.spec.metadata.tenant.clone(),
            backend: ctx.backend.clone(),
            checkpoints: Arc::clone(&ctx.ckpt_store),
            sinks: ctx
                .spec
                .spec
                .sinks
                .iter()
                .map(|s| s.sink_id().to_string())
                .collect(),
            hook,
        })
    }

    fn input(&self) -> AdoptionInput<'_> {
        AdoptionInput {
            dsn: &self.dsn,
            slot: &self.slot,
            publication: &self.publication,
            source_id: &self.source,
            tenant: &self.tenant,
            backend: &self.backend,
            checkpoints: &self.checkpoints,
            sinks: &self.sinks,
            hook: self.hook.as_ref(),
        }
    }
}

/// The `pg-adopt-timeline` operation.
#[derive(Default)]
pub struct AdoptTimeline {
    hook: Option<FactsHook>,
}

impl AdoptTimeline {
    /// For tests: alter what every session showed (see [`FactsHook`]).
    pub fn with_session_facts_hook(hook: FactsHook) -> Self {
        Self { hook: Some(hook) }
    }
}

fn timeline_unrecorded(i: &IncidentRecord) -> bool {
    i.reason_code == ReasonCode::PgContinuityUnproven
        && matches!(
            i.evidence.get(EvidenceKey::ReasonClass),
            Some(EvidenceValue::Text { value }) if value == "timeline_unrecorded"
        )
}

/// The final plan: the chain id derived from the seed of `seeded` (whose
/// record carries `PLANNED_ID`), put into the record, its post-state and
/// detail.
fn finalize_chain(
    seeded: CanonicalPlan,
    facts: &AdoptionFacts,
    f: &str,
) -> CanonicalPlan {
    let chain = derived_id(&plan_seed(&seeded), "continuity_chain");
    let record = adopted_record(&chain, facts, f);
    let mut plan = seeded;
    let step = &mut plan.steps[0];
    step.post = record_digest(&record);
    step.detail.insert(
        "record".into(),
        String::from_utf8(record).expect("JSON is UTF-8"),
    );
    step.detail.insert("chain_id".into(), chain);
    plan
}

#[async_trait]
impl RecoveryOperation for AdoptTimeline {
    fn name(&self) -> &'static str {
        NAME
    }

    fn applies_to(&self, i: &IncidentRecord) -> bool {
        timeline_unrecorded(i)
    }

    async fn plan(
        &self,
        ctx: &OperationContext,
    ) -> Result<Planned, RecoveryApiError> {
        if !matches!(ctx.spec.spec.source, SourceCfg::Postgres(_)) {
            return Err(not_applicable(
                "pg-adopt-timeline applies to PostgreSQL sources",
            ));
        }
        match &ctx.incident {
            Some(i) if timeline_unrecorded(i) => {}
            _ => {
                return Err(not_applicable(
                    "pg-adopt-timeline answers a pg_continuity_unproven incident \
                     of class timeline_unrecorded: name it with --incident",
                ));
            }
        }
        let target = Target::of(ctx, self.hook.clone()).await?;
        let planned = plan_adoption(&target.input()).await.map_err(refusal)?;
        let mut bindings = ctx.base_bindings().await?;
        bindings.insert("slot".into(), target.slot.clone());
        bindings.insert("f".into(), planned.f.clone());
        bindings.insert(
            "server".into(),
            serde_json::to_string(&planned.facts).expect("facts serialize"),
        );
        bindings
            .insert("slot_owner_record".into(), planned.owner_record.clone());
        bindings.insert("continuity".into(), CONTINUITY_ABSENT.into());
        for (k, v) in &planned.checkpoints {
            // The same set the base binds; asserted, not trusted.
            if bindings.get(&format!("checkpoint.{k}")) != Some(v) {
                return Err(RecoveryApiError::new(
                    409,
                    "proof_mismatch",
                    "the checkpoints changed while the plan was computed",
                ));
            }
        }
        let template = adopted_record(PLANNED_ID, &planned.facts, &planned.f);
        let step = PlanStep {
            name: CREATE.into(),
            pre: CONTINUITY_ABSENT.into(),
            post: record_digest(&template),
            detail: BTreeMap::from([
                ("f".into(), planned.f.clone()),
                (
                    "server".into(),
                    serde_json::to_string(&planned.facts)
                        .expect("facts serialize"),
                ),
                ("transition".into(), "0".into()),
                ("timeline".into(), planned.facts.timeline.to_string()),
                (
                    "record".into(),
                    String::from_utf8(template).expect("JSON is UTF-8"),
                ),
            ]),
        };
        let consequences = vec![
            "no checkpoint moves: the next start runs the full continuity proof \
             against the new record and stamps every checkpoint at transition 0"
                .to_string(),
            format!(
                "the operator asserts that the server history from the checkpoint {} \
                 to now is timeline {}'s; a same-timeline rewind cannot be detected \
                 and is unsupported",
                planned.f, planned.facts.timeline
            ),
            "the pipeline is not started: resume it explicitly afterwards".to_string(),
        ];
        let seeded = CanonicalPlan::new(
            NAME,
            &ctx.pipeline,
            &target.source,
            bindings,
            vec![step],
            consequences,
        );
        let plan = finalize_chain(seeded, &planned.facts, &planned.f);
        Ok(Planned {
            plan,
            resolves: ctx
                .incident
                .as_ref()
                .map(|i| vec![i.incident_id.clone()])
                .unwrap_or_default(),
            observed: BTreeMap::from([("wal_flush".into(), planned.flush)]),
        })
    }

    async fn executor(
        &self,
        ctx: &OperationContext,
        _plan: &CanonicalPlan,
    ) -> Result<Box<dyn StepExecutor>, RecoveryApiError> {
        Ok(Box::new(Exec {
            target: Target::of(ctx, self.hook.clone()).await?,
        }))
    }
}

struct Exec {
    target: Target,
}

#[async_trait]
impl StepExecutor for Exec {
    async fn observe(&self, step: &PlanStep) -> anyhow::Result<String> {
        anyhow::ensure!(step.name == CREATE, "unknown step {}", step.name);
        continuity_state(&self.target.backend, &self.target.source).await
    }

    async fn perform(
        &self,
        step: &PlanStep,
    ) -> anyhow::Result<BTreeMap<String, String>> {
        let get = |k: &str| {
            step.detail
                .get(k)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("the step names no {k}"))
        };
        let record = get("record")?;
        anyhow::ensure!(
            record_digest(record.as_bytes()) == step.post,
            "the step's record is not its post-state"
        );
        let planned = AdoptionPlan {
            f: get("f")?,
            checkpoints: BTreeMap::new(),
            facts: serde_json::from_str(&get("server")?)?,
            flush: String::new(),
            owner_record: String::new(),
        };
        apply_adoption(&self.target.input(), &planned, record.as_bytes())
            .await
            .map_err(|r| anyhow::anyhow!("{r}"))?;
        Ok(BTreeMap::from([
            ("continuity_chain".into(), get("chain_id")?),
            ("timeline".into(), get("timeline")?),
            ("transition".into(), "0".into()),
            ("proven_at".into(), planned.f),
        ]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltaforge_core::incident::{
        CauseCode, Component, IncidentDraft, Retryability, SafetyState,
    };

    fn record(class: &str, reason: ReasonCode) -> IncidentRecord {
        let draft = IncidentDraft::new(
            reason,
            Component::Source { id: "pg".into() },
            Retryability::OperatorAction,
            SafetyState::HaltedSafe,
            CauseCode::SourceLineage,
        )
        .with_evidence(|e| {
            e.text(EvidenceKey::ReasonClass, class);
        });
        let v = serde_json::json!({
            "format": 1, "incident_id": "i", "pipeline": "p",
            "reason_code": draft.reason_code, "component": draft.component,
            "retryability": draft.retryability, "safety_state": draft.safety_state,
            "cause_code": draft.cause_code, "evidence": draft.evidence,
            "actions": [], "first_seen_ms": 0, "last_seen_ms": 0, "occurrences": 1,
            "status": { "state": "open" }, "transition_seq": 1,
        });
        serde_json::from_value(v).unwrap()
    }

    #[test]
    fn it_answers_only_timeline_unrecorded() {
        let op = AdoptTimeline::default();
        assert!(op.applies_to(&record(
            "timeline_unrecorded",
            ReasonCode::PgContinuityUnproven
        )));
        assert!(!op.applies_to(&record(
            "slot_missing",
            ReasonCode::PgContinuityUnproven
        )));
        assert!(!op.applies_to(&record(
            "timeline_unrecorded",
            ReasonCode::PgDifferentCluster
        )));
    }

    #[test]
    fn the_chain_is_derived_from_the_seeded_plan() {
        let facts = AdoptionFacts {
            system_identifier: 7,
            database_oid: 5,
            timeline: 2,
            server_major: 17,
            in_recovery: false,
            slot: None,
        };
        let seeded = |f: &str| {
            CanonicalPlan::new(
                NAME,
                "p",
                "src",
                BTreeMap::from([("f".to_string(), f.to_string())]),
                vec![PlanStep {
                    name: CREATE.into(),
                    pre: CONTINUITY_ABSENT.into(),
                    post: record_digest(&adopted_record(PLANNED_ID, &facts, f)),
                    detail: BTreeMap::new(),
                }],
                vec![],
            )
        };
        let a = finalize_chain(seeded("0/10"), &facts, "0/10");
        let b = finalize_chain(seeded("0/10"), &facts, "0/10");
        assert_eq!(a.proof(), b.proof(), "recomputing yields the same proof");
        let chain = &a.steps[0].detail["chain_id"];
        assert_eq!(chain.len(), 32);
        assert_ne!(chain, PLANNED_ID);
        // The record in the final plan carries it, at transition 0 and F.
        let rec: serde_json::Value =
            serde_json::from_str(&a.steps[0].detail["record"]).unwrap();
        assert_eq!(rec["chain_id"], chain.as_str());
        assert_eq!(rec["transition_id"], 0);
        assert_eq!(rec["proven_at"], "0/10");
        assert_eq!(
            a.steps[0].post,
            record_digest(a.steps[0].detail["record"].as_bytes())
        );
        // Another observation, another chain; and not the snapshot purpose.
        let c = finalize_chain(seeded("0/20"), &facts, "0/20");
        assert_ne!(&c.steps[0].detail["chain_id"], chain);
        assert_ne!(
            chain,
            &derived_id(&plan_seed(&seeded("0/10")), "snapshot_chain")
        );
    }
}
