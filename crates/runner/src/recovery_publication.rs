//! The recovery operation `pg-publication-maintenance`: record how a stopped
//! PostgreSQL source continues after its publication is changed through the
//! database-wide maintenance procedure (every source of the database
//! stopped, every registration removed, enforcement uninstalled, the change
//! applied, the publications registered again):
//!
//! - `--arg mode=resnapshot`: the source accepts the new registration only
//!   when it starts a new snapshot generation (run `resnapshot` too);
//! - `--arg mode=abandon`: the source starts at the new registration's
//!   position; the backlog before it is skipped. This plan is the audit.
//!
//! The decision names the registration the source accepted last, so it
//! answers exactly one maintenance. Nothing else moves; the pipeline is not
//! started.

use std::collections::BTreeMap;

use async_trait::async_trait;
use deltaforge_config::SourceCfg;
use deltaforge_core::incident::ReasonCode;
use rest_api::recovery::RecoveryApiError;
use sources::postgres::postgres_publication::{
    Decision, accepted, decide, decision, record_key,
};
use storage::adapters::incidents::IncidentRecord;
use storage::adapters::recovery::{
    CanonicalPlan, PlanStep, StepExecutor, digest_bytes,
};

use crate::recovery::{
    OperationContext, Planned, RecoveryOperation, state_error,
};

pub const NAME: &str = "pg-publication-maintenance";
const RECORD: &str = "record_decision";
const ABSENT: &str = "decision:absent";

fn not_applicable(msg: impl Into<String>) -> RecoveryApiError {
    RecoveryApiError::new(409, "not_applicable", msg)
}

/// The `pg-publication-maintenance` operation.
#[derive(Default)]
pub struct PublicationMaintenance;

struct Target {
    key: String,
    backend: storage::ArcStorageBackend,
}

impl Target {
    fn of(ctx: &OperationContext) -> Result<Self, RecoveryApiError> {
        let SourceCfg::Postgres(c) = &ctx.spec.spec.source else {
            return Err(not_applicable(
                "pg-publication-maintenance applies to PostgreSQL sources",
            ));
        };
        Ok(Self {
            key: record_key(
                &ctx.spec.metadata.tenant,
                ctx.source_id(),
                &c.publication,
            ),
            backend: ctx.backend.clone(),
        })
    }

    async fn state(&self) -> anyhow::Result<String> {
        Ok(match decision(&self.backend, &self.key).await? {
            None => ABSENT.to_string(),
            Some(d) => digest_bytes(&serde_json::to_vec(&d)?),
        })
    }
}

#[async_trait]
impl RecoveryOperation for PublicationMaintenance {
    fn name(&self) -> &'static str {
        NAME
    }

    fn applies_to(&self, i: &IncidentRecord) -> bool {
        i.reason_code == ReasonCode::PgPublicationChanged
    }

    async fn plan(
        &self,
        ctx: &OperationContext,
    ) -> Result<Planned, RecoveryApiError> {
        let target = Target::of(ctx)?;
        let mode = ctx.args.get("mode").map(String::as_str);
        let mode = match mode {
            Some(m @ ("resnapshot" | "abandon")) => m.to_string(),
            _ => {
                return Err(RecoveryApiError::new(
                    400,
                    "bad_request",
                    "pg-publication-maintenance needs --arg mode=resnapshot \
                     or --arg mode=abandon",
                ));
            }
        };
        let acc = accepted(&target.backend, &target.key)
            .await
            .map_err(|e| state_error("the accepted registration", &e))?
            .ok_or_else(|| {
                not_applicable(
                    "the source never accepted a publication registration: \
                     nothing to replace",
                )
            })?;
        let pre = target
            .state()
            .await
            .map_err(|e| state_error("the maintenance decision", &e))?;
        let d = Decision {
            mode: mode.clone(),
            for_digest: acc.digest.clone(),
            for_lsn: acc.registered_lsn.clone(),
        };
        let bytes = serde_json::to_vec(&d).expect("a decision serializes");
        let mut bindings = ctx.base_bindings().await?;
        bindings.insert("registration".into(), acc.digest.clone());
        bindings.insert("registration_lsn".into(), acc.registered_lsn.clone());
        bindings.insert("mode".into(), mode.clone());
        let step = PlanStep {
            name: RECORD.into(),
            pre,
            post: digest_bytes(&bytes),
            detail: BTreeMap::from([(
                "decision".into(),
                String::from_utf8(bytes).expect("JSON is UTF-8"),
            )]),
        };
        let mut consequences = vec![
            format!(
                "the source replaces the registration it accepted ({} at {}) \
                 with the database's next registration of its publication",
                acc.digest, acc.registered_lsn
            ),
            "publication maintenance is database-wide: every DeltaForge source \
             of the database must be stopped, every registration removed and \
             enforcement uninstalled before any publication changes"
                .to_string(),
            "the pipeline is not started: resume it explicitly afterwards"
                .to_string(),
        ];
        consequences.push(if mode == "abandon" {
            "ABANDON: every change between the source's position and the new \
             registration is skipped and never delivered"
                .to_string()
        } else {
            "RESNAPSHOT: the source accepts the new registration only when it \
             starts a new snapshot generation (run the resnapshot operation)"
                .to_string()
        });
        let plan = CanonicalPlan::new(
            NAME,
            &ctx.pipeline,
            ctx.source_id(),
            bindings,
            vec![step],
            consequences,
        );
        Ok(Planned {
            plan,
            resolves: ctx
                .incident
                .as_ref()
                .map(|i| vec![i.incident_id.clone()])
                .unwrap_or_default(),
            observed: BTreeMap::new(),
        })
    }

    async fn executor(
        &self,
        ctx: &OperationContext,
        _plan: &CanonicalPlan,
    ) -> Result<Box<dyn StepExecutor>, RecoveryApiError> {
        Ok(Box::new(Exec {
            target: Target::of(ctx)?,
        }))
    }
}

struct Exec {
    target: Target,
}

#[async_trait]
impl StepExecutor for Exec {
    async fn observe(&self, step: &PlanStep) -> anyhow::Result<String> {
        anyhow::ensure!(step.name == RECORD, "unknown step {}", step.name);
        self.target.state().await
    }

    async fn perform(
        &self,
        step: &PlanStep,
    ) -> anyhow::Result<BTreeMap<String, String>> {
        let text = step
            .detail
            .get("decision")
            .ok_or_else(|| anyhow::anyhow!("the step names no decision"))?;
        anyhow::ensure!(
            digest_bytes(text.as_bytes()) == step.post,
            "the step's decision is not its post-state"
        );
        let d: Decision = serde_json::from_str(text)?;
        decide(&self.target.backend, &self.target.key, &d).await?;
        Ok(BTreeMap::from([
            ("mode".into(), d.mode),
            ("for_registration".into(), d.for_digest),
        ]))
    }
}
