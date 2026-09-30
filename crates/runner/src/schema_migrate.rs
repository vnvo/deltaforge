//! `deltaforge schema-migrate` and `deltaforge store-gate`: the operator side of
//! the explicit, proof-gated migration of pre-upgrade schema history.
//!
//! The dry run (default) is read-only and takes no gate. `--apply` requires the
//! proof printed by a dry run and holds the store gate (role `migration`,
//! recording that proof) while it recomputes the plan and writes. The gate is
//! released only when the migration completed, or when a fresh run found that
//! the plan no longer matches the proof (nothing was written). Any other
//! failure, like a crash, leaves the gate held, because part of the plan may
//! already be written and a server must never read partially migrated
//! history. The only way forward is `--resume-owner <id>`, which takes the held
//! gate over in one step and finishes the same proof.

use anyhow::{Context, Result};
use storage::ArcStorageBackend;
use storage::adapters::DurableSchemaRegistry;
use storage::adapters::schema_migration::{
    self, Classification, Filters, Mapping, Plan, ProofMismatch,
};
use storage::adapters::store_gate::{self, GateState};

/// Options of `schema-migrate`.
#[derive(Debug, Clone)]
pub struct MigrateArgs {
    pub mapping: String,
    pub tenant: Option<String>,
    pub source: Option<String>,
    pub apply: bool,
    pub expect_proof: Option<String>,
    /// Owner id of the unfinished migration to take over and finish.
    pub resume_owner: Option<String>,
    pub json: bool,
}

fn load_mapping(path: &str) -> Result<Mapping> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("read mapping file {path}"))?;
    let mapping: Mapping = serde_yaml::from_str(&text)
        .with_context(|| format!("parse mapping file {path}"))?;
    anyhow::ensure!(
        !mapping.mappings.is_empty(),
        "mapping file {path} lists no mappings"
    );
    for e in &mapping.mappings {
        anyhow::ensure!(
            !e.tables.is_empty(),
            "mapping for {}/{} lists no tables",
            e.tenant,
            e.source_id
        );
        for t in &e.tables {
            anyhow::ensure!(
                !t.db.contains('*') && !t.table.contains('*'),
                "mapping for {}/{} uses a wildcard ({}.{}); list tables explicitly",
                e.tenant,
                e.source_id,
                t.db,
                t.table
            );
        }
    }
    Ok(mapping)
}

fn render_plan(plan: &Plan) -> String {
    let mut out = String::new();
    for (ei, entry) in plan.canonical.entries.iter().enumerate() {
        out.push_str(&format!(
            "{}/{}  asserted lineage {}  current lineage {}\n",
            entry.tenant,
            entry.source_id,
            entry.asserted_lineage_hash,
            entry
                .current_lineage
                .as_ref()
                .map(|l| format!("{} {:?}", l.lineage_hash, l.descriptor))
                .unwrap_or_else(|| "(none recorded)".into())
        ));
        for (ti, t) in entry.tables.iter().enumerate() {
            let p = &plan.progress[ei][ti];
            let class = match &t.classification {
                Classification::Migrate if p.marker_written => {
                    "already migrated".to_string()
                }
                Classification::Migrate => "migrate".to_string(),
                Classification::Ambiguous(r) => format!("AMBIGUOUS: {r}"),
                Classification::Rejected(r) => format!("REJECTED: {r}"),
            };
            out.push_str(&format!(
                "  {}.{}  legacy versions {}  already present {}  -> {class}\n",
                t.db, t.table, t.legacy_versions, p.versions_present
            ));
            if let Some(d) = &t.legacy_digest {
                out.push_str(&format!("      legacy content digest {d}\n"));
            }
            for c in &t.conflicts {
                out.push_str(&format!(
                    "      conflict: version {} is {} in the legacy history but {} \
                     in the lineage-scoped history\n",
                    c.version, c.legacy_hash, c.v1_hash
                ));
            }
            if let Some(m) = &t.foreign_marker {
                out.push_str(&format!(
                    "      existing marker from a different migration: {:?}\n",
                    m.provenance
                ));
            }
        }
    }
    let totals = &plan.totals;
    out.push_str(&format!(
        "\nwould migrate {}  already migrated {}  ambiguous {}  rejected {}\n",
        totals.migrate,
        totals.already_migrated,
        totals.ambiguous,
        totals.rejected
    ));
    out.push_str(&format!("proof {}\n", plan.proof));
    out
}

fn apply_command(
    args: &MigrateArgs,
    proof: &str,
    resume_owner: Option<&str>,
) -> String {
    format!(
        "deltaforge schema-migrate --mapping {}{}{} --apply --expect-proof {proof}{}",
        args.mapping,
        args.tenant
            .as_ref()
            .map(|t| format!(" --tenant {t}"))
            .unwrap_or_default(),
        args.source
            .as_ref()
            .map(|s| format!(" --source {s}"))
            .unwrap_or_default(),
        resume_owner
            .map(|o| format!(" --resume-owner {o}"))
            .unwrap_or_default()
    )
}

/// Run `schema-migrate`.
pub async fn run(args: MigrateArgs, backend: ArcStorageBackend) -> Result<()> {
    let mapping = load_mapping(&args.mapping)?;
    let filters = Filters {
        tenant: args.tenant.clone(),
        source_id: args.source.clone(),
    };

    if !args.apply {
        let registry =
            DurableSchemaRegistry::open_for_inspection(backend.clone())
                .await
                .context("open schema registry for inspection")?;
        let plan =
            schema_migration::plan(&backend, &registry, &mapping, &filters)
                .await?;
        if args.json {
            println!("{}", serde_json::to_string_pretty(&plan)?);
        } else {
            print!("{}", render_plan(&plan));
            println!(
                "\nDry run: nothing was written. To apply exactly this plan (the \
                 DeltaForge server must be stopped):\n  {}",
                apply_command(&args, &plan.proof, None)
            );
        }
        return Ok(());
    }

    let expected = args
        .expect_proof
        .as_deref()
        .context("--apply requires --expect-proof <digest> from a dry run")?;
    let gate = match &args.resume_owner {
        Some(owner) => store_gate::resume_migration(&backend, owner, expected)
            .await
            .context("take over the unfinished migration's store gate")?,
        None => store_gate::acquire_for_migration(&backend, expected)
            .await
            .context("acquire the store gate for the migration")?,
    };
    let result = async {
        let registry = DurableSchemaRegistry::new(backend.clone())
            .await
            .context("open schema registry")?;
        schema_migration::apply(
            &backend, &registry, &mapping, &filters, expected,
        )
        .await
    }
    .await;
    let outcome = match result {
        Ok(outcome) => {
            gate.release()
                .await
                .context("release the store gate after the migration")?;
            outcome
        }
        // A fresh run that finds a different plan wrote nothing, and a fresh
        // acquisition proves no earlier migration was left unfinished (that
        // would still hold the gate), so the store holds no partial work.
        Err(e)
            if args.resume_owner.is_none()
                && e.downcast_ref::<ProofMismatch>().is_some() =>
        {
            gate.release()
                .await
                .context("release the store gate after a proof mismatch")?;
            return Err(e);
        }
        Err(e) => {
            let owner = gate.holder().owner_id.clone();
            // Dropped without release: the gate stays held.
            drop(gate);
            return Err(e.context(format!(
                "the migration did not complete. The store gate stays held \
                 (owner {owner}) so no server can read partially migrated \
                 history. Fix the cause, then finish it with: {}",
                apply_command(&args, expected, Some(&owner))
            )));
        }
    };
    if args.json {
        println!("{}", serde_json::to_string_pretty(&outcome)?);
    } else {
        println!(
            "migrated {}  already migrated {}  ambiguous {}  rejected {}  (proof {})",
            outcome.migrated,
            outcome.already_migrated,
            outcome.ambiguous,
            outcome.rejected,
            outcome.proof
        );
    }
    Ok(())
}

/// `store-gate status`.
pub async fn gate_status(backend: ArcStorageBackend, json: bool) -> Result<()> {
    let state = store_gate::status(&backend).await?;
    match (&state, json) {
        (GateState::Unlocked, true) => {
            println!("{}", serde_json::json!({"state": "unlocked"}))
        }
        (GateState::Held(h), true) => {
            println!("{}", serde_json::json!({"state": "held", "holder": h}))
        }
        (GateState::Unlocked, false) => println!("store gate: unlocked"),
        (GateState::Held(h), false) => println!("store gate: held by {h}"),
    }
    Ok(())
}

/// `store-gate break --owner <id>`: release a gate left by a crashed process.
pub async fn gate_break(backend: ArcStorageBackend, owner: &str) -> Result<()> {
    let released = store_gate::break_gate(&backend, owner).await?;
    println!("store gate released (was held by {released})");
    Ok(())
}
