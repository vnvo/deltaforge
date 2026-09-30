//! Explicit, proof-gated migration of pre-upgrade (unscoped) schema history.
//!
//! Pre-upgrade history under the flat key `{tenant}/{db}/{table}` carries no
//! source or database lineage, so it can never be attributed automatically.
//! An operator asserts ownership in a mapping (explicit tables, a verified
//! lineage hash, no wildcards); [`plan`] turns the mapping into a read-only
//! proof report with a digest; [`apply`] recomputes the plan and writes only if
//! the digest is exactly the one the operator reviewed.
//!
//! The proof binds every byte that will be adopted (each legacy record's exact
//! stored bytes, version and original sequence), the target lineage, conflicts,
//! foreign markers, filters and mapping entries. It is computed only from
//! inputs this proof's own progress cannot change, so a retry after a crash at
//! any point reproduces the same digest and converges.
//!
//! The caller of [`apply`] must hold the store gate (role `migration`).

use std::collections::HashMap;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::schema_key::SchemaKey;
use super::schema_registry::{
    DurableSchemaRegistry, LegacyReadRefused, MigrationProvenance,
};
use super::source_lineage::{self, LineageRef};
use crate::ArcStorageBackend;

const PLAN_DOMAIN: &str = "DeltaForge.SchemaMigration.Plan.v1";
const LEGACY_DOMAIN: &[u8] = b"DeltaForge.SchemaMigration.Legacy.v1\0";
const IDENTITY_DOMAIN: &[u8] = b"DeltaForge.SchemaMigration.Identity.v1\0";
const PAGE: usize = 256;

/// The operator's mapping file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Mapping {
    pub mappings: Vec<MappingEntry>,
}

/// One explicit ownership assertion: every listed table's whole legacy stream
/// belongs to `source_id`'s verified lineage `lineage_hash`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MappingEntry {
    pub tenant: String,
    pub source_id: String,
    pub lineage_hash: String,
    pub tables: Vec<TableRef>,
}

/// An explicit table (PostgreSQL: schema + table; MySQL: database + table).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableRef {
    pub db: String,
    pub table: String,
}

/// Narrow a run to part of the mapping.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Filters {
    pub tenant: Option<String>,
    pub source_id: Option<String>,
}

impl Filters {
    fn selects(&self, e: &MappingEntry) -> bool {
        self.tenant.as_ref().is_none_or(|t| *t == e.tenant)
            && self.source_id.as_ref().is_none_or(|s| *s == e.source_id)
    }
}

/// Planned action for one table (part of the proof).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "reason", rename_all = "snake_case")]
pub enum Classification {
    /// Will be migrated (or is already migrated by this same action).
    Migrate,
    /// Ownership is structurally unclear; never written.
    Ambiguous(String),
    /// Not migratable as mapped; never written.
    Rejected(String),
}

/// A legacy version whose number already exists in v1 with another hash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Conflict {
    pub version: i32,
    pub legacy_hash: String,
    pub v1_hash: String,
}

/// Canonical per-table plan (bound by the proof digest).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TablePlan {
    pub db: String,
    pub table: String,
    pub legacy_key: String,
    pub legacy_versions: u64,
    pub legacy_digest: Option<String>,
    pub migration_identity: Option<String>,
    pub conflicts: Vec<Conflict>,
    /// Provenance of a marker left by a DIFFERENT action (reviewed, rejects).
    pub foreign_marker: Option<ForeignMarker>,
    pub classification: Classification,
}

/// A pre-existing marker that is not this action's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForeignMarker {
    pub migrated_at_ms: i64,
    pub provenance: Option<MigrationProvenance>,
}

/// Canonical per-entry plan (bound by the proof digest).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntryPlan {
    pub tenant: String,
    pub source_id: String,
    pub asserted_lineage_hash: String,
    /// The source's current verified lineage, when recorded.
    pub current_lineage: Option<LineageRef>,
    pub tables: Vec<TablePlan>,
}

/// The canonical plan the proof digest is computed over.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalPlan {
    pub domain: String,
    pub filters: Filters,
    pub entries: Vec<EntryPlan>,
}

/// Progress of this action on a table (reported, NOT part of the proof).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Progress {
    pub versions_present: u64,
    pub marker_written: bool,
}

/// Plan + proof + progress.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Plan {
    pub canonical: CanonicalPlan,
    pub proof: String,
    /// Keyed like `entries[i].tables[j]`: `(entry index, table index)`.
    pub progress: Vec<Vec<Progress>>,
    pub totals: Totals,
}

/// Counts over the selected tables.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Totals {
    /// `Migrate` tables not yet complete (dry run) or completed now (apply).
    pub migrate: u64,
    /// `Migrate` tables whose marker from this action already exists.
    pub already_migrated: u64,
    pub ambiguous: u64,
    pub rejected: u64,
}

fn lp(h: &mut Sha256, bytes: &[u8]) {
    h.update((bytes.len() as u64).to_be_bytes());
    h.update(bytes);
}

fn migration_identity(
    tenant: &str,
    source_id: &str,
    lineage_hash: &str,
    t: &TableRef,
    legacy_digest: &str,
) -> String {
    let mut h = Sha256::new();
    h.update(IDENTITY_DOMAIN);
    for part in [
        tenant,
        source_id,
        lineage_hash,
        &t.db,
        &t.table,
        legacy_digest,
    ] {
        lp(&mut h, part.as_bytes());
    }
    hex::encode(h.finalize())
}

/// Result of streaming a legacy stream once.
struct LegacyScan {
    versions: u64,
    digest: String,
    conflicts: Vec<Conflict>,
    present: u64,
}

/// Stream the legacy history of `t` (paged): content digest over every
/// version's number, original sequence and exact stored-record digest, plus
/// conflicts / progress against the qualified target `key`.
async fn scan_legacy(
    registry: &DurableSchemaRegistry,
    tenant: &str,
    t: &TableRef,
    key: &SchemaKey,
) -> Result<std::result::Result<LegacyScan, String>> {
    let mut h = Sha256::new();
    h.update(LEGACY_DOMAIN);
    let mut scan = LegacyScan {
        versions: 0,
        digest: String::new(),
        conflicts: Vec::new(),
        present: 0,
    };
    let mut cursor = None;
    loop {
        let page = match registry
            .legacy_page(tenant, &t.db, &t.table, cursor, PAGE)
            .await
        {
            Ok(p) => p,
            Err(e) => {
                if let Some(refused) = e.downcast_ref::<LegacyReadRefused>() {
                    return Ok(Err(refused.to_string()));
                }
                return Err(e);
            }
        };
        for lv in &page.versions {
            scan.versions += 1;
            h.update(lv.version.to_be_bytes());
            h.update(lv.sequence.to_be_bytes());
            lp(&mut h, lv.record_digest.as_bytes());
            match registry.version_hash(key, lv.version).await? {
                Some(v1) if v1 == lv.hash => scan.present += 1,
                Some(v1) => scan.conflicts.push(Conflict {
                    version: lv.version,
                    legacy_hash: lv.hash.clone(),
                    v1_hash: v1,
                }),
                None => {}
            }
        }
        match page.next {
            Some(n) => cursor = Some(n),
            None => break,
        }
    }
    scan.digest = hex::encode(h.finalize());
    Ok(Ok(scan))
}

fn legacy_key(tenant: &str, t: &TableRef) -> String {
    format!("{tenant}/{}/{}", t.db, t.table)
}

/// Build the read-only proof report. Never writes.
pub async fn plan(
    backend: &ArcStorageBackend,
    registry: &DurableSchemaRegistry,
    mapping: &Mapping,
    filters: &Filters,
) -> Result<Plan> {
    // How many entries (in the WHOLE mapping, regardless of filters) claim
    // each legacy stream: more than one is ambiguous ownership.
    let mut claims: HashMap<String, usize> = HashMap::new();
    for e in &mapping.mappings {
        let mut seen = std::collections::HashSet::new();
        for t in &e.tables {
            if seen.insert(t.clone()) {
                *claims.entry(legacy_key(&e.tenant, t)).or_default() += 1;
            }
        }
    }

    let mut entries = Vec::new();
    let mut progress = Vec::new();
    let mut totals = Totals::default();
    for e in mapping.mappings.iter().filter(|e| filters.selects(e)) {
        let record = source_lineage::load(backend, &e.tenant, &e.source_id)
            .await
            .with_context(|| {
                format!(
                    "read the lineage record of {}/{}",
                    e.tenant, e.source_id
                )
            })?;
        let current_lineage = record.map(|r| r.current);
        let lineage_problem = match &current_lineage {
            None => Some(format!(
                "source {}/{} has no verified lineage record (start it once so \
                 it verifies its server)",
                e.tenant, e.source_id
            )),
            Some(c) if c.lineage_hash != e.lineage_hash => Some(format!(
                "asserted lineage {} is not the source's current verified \
                 lineage {}",
                e.lineage_hash, c.lineage_hash
            )),
            Some(_) => None,
        };

        let mut tables = Vec::new();
        let mut entry_progress = Vec::new();
        for t in &e.tables {
            let lk = legacy_key(&e.tenant, t);
            let key = SchemaKey::new(
                e.tenant.as_str(),
                e.source_id.as_str(),
                e.lineage_hash.as_str(),
                t.db.as_str(),
                t.table.as_str(),
            );
            let mut tp = TablePlan {
                db: t.db.clone(),
                table: t.table.clone(),
                legacy_key: lk.clone(),
                legacy_versions: 0,
                legacy_digest: None,
                migration_identity: None,
                conflicts: Vec::new(),
                foreign_marker: None,
                classification: Classification::Migrate,
            };
            let mut prog = Progress::default();

            let ambiguous = if [&e.tenant, &t.db, &t.table]
                .iter()
                .any(|s| s.contains('/'))
            {
                Some(
                    "the pre-upgrade key is ambiguous (a name contains `/`)"
                        .to_string(),
                )
            } else if claims.get(&lk).copied().unwrap_or(0) > 1 {
                Some("the same legacy stream is claimed by more than one mapping entry".to_string())
            } else {
                None
            };

            tp.classification = if let Some(reason) = ambiguous {
                Classification::Ambiguous(reason)
            } else if let Some(reason) = &lineage_problem {
                Classification::Rejected(reason.clone())
            } else {
                match scan_legacy(registry, &e.tenant, t, &key).await? {
                    Err(refused) => Classification::Rejected(refused),
                    Ok(scan) => {
                        tp.legacy_versions = scan.versions;
                        prog.versions_present = scan.present;
                        let identity = migration_identity(
                            &e.tenant,
                            &e.source_id,
                            &e.lineage_hash,
                            t,
                            &scan.digest,
                        );
                        tp.legacy_digest = Some(scan.digest);
                        tp.migration_identity = Some(identity.clone());
                        tp.conflicts = scan.conflicts;
                        let marker = registry.migration_marker(&key).await?;
                        let own = marker.as_ref().is_some_and(|m| {
                            m.provenance.as_ref().is_some_and(|p| {
                                p.migration_identity == identity
                            })
                        });
                        if let Some(m) = marker.filter(|_| !own) {
                            tp.foreign_marker = Some(ForeignMarker {
                                migrated_at_ms: m.migrated_at_ms,
                                provenance: m.provenance,
                            });
                        }
                        prog.marker_written = own;
                        if scan.versions == 0 {
                            Classification::Rejected(
                                "no pre-upgrade history".into(),
                            )
                        } else if !tp.conflicts.is_empty() {
                            Classification::Rejected(format!(
                                "{} legacy version number(s) already exist with a \
                                 different hash",
                                tp.conflicts.len()
                            ))
                        } else if tp.foreign_marker.is_some() {
                            Classification::Rejected(
                                "a marker from a different migration exists; review it"
                                    .into(),
                            )
                        } else {
                            Classification::Migrate
                        }
                    }
                }
            };
            match &tp.classification {
                Classification::Migrate if prog.marker_written => {
                    totals.already_migrated += 1
                }
                Classification::Migrate => totals.migrate += 1,
                Classification::Ambiguous(_) => totals.ambiguous += 1,
                Classification::Rejected(_) => totals.rejected += 1,
            }
            tables.push(tp);
            entry_progress.push(prog);
        }
        entries.push(EntryPlan {
            tenant: e.tenant.clone(),
            source_id: e.source_id.clone(),
            asserted_lineage_hash: e.lineage_hash.clone(),
            current_lineage,
            tables,
        });
        progress.push(entry_progress);
    }

    let canonical = CanonicalPlan {
        domain: PLAN_DOMAIN.to_string(),
        filters: filters.clone(),
        entries,
    };
    let proof = hex::encode(Sha256::digest(
        serde_json::to_vec(&canonical).context("encode canonical plan")?,
    ));
    Ok(Plan {
        canonical,
        proof,
        progress,
        totals,
    })
}

/// Outcome of [`apply`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplyOutcome {
    pub proof: String,
    pub migrated: u64,
    pub already_migrated: u64,
    pub ambiguous: u64,
    pub rejected: u64,
}

#[derive(Debug, thiserror::Error)]
#[error(
    "the migration plan changed since it was reviewed (expected proof \
     {expected}, current proof {actual}); nothing was written. Re-run the dry \
     run and review the new plan."
)]
pub struct ProofMismatch {
    pub expected: String,
    pub actual: String,
}

/// Recompute the plan and, only if its proof equals `expected_proof`, adopt
/// every `Migrate` table's legacy versions (identity preserved, idempotent) and
/// then write its marker. `Ambiguous` / `Rejected` tables are never written.
/// Legacy records are never deleted. The caller must hold the store gate.
pub async fn apply(
    backend: &ArcStorageBackend,
    registry: &DurableSchemaRegistry,
    mapping: &Mapping,
    filters: &Filters,
    expected_proof: &str,
) -> Result<ApplyOutcome> {
    let plan = plan(backend, registry, mapping, filters).await?;
    if plan.proof != expected_proof {
        return Err(ProofMismatch {
            expected: expected_proof.to_string(),
            actual: plan.proof,
        }
        .into());
    }
    let mut out = ApplyOutcome {
        proof: plan.proof.clone(),
        migrated: 0,
        already_migrated: plan.totals.already_migrated,
        ambiguous: plan.totals.ambiguous,
        rejected: plan.totals.rejected,
    };
    for (ei, entry) in plan.canonical.entries.iter().enumerate() {
        for (ti, tp) in entry.tables.iter().enumerate() {
            if tp.classification != Classification::Migrate
                || plan.progress[ei][ti].marker_written
            {
                continue;
            }
            let key = SchemaKey::new(
                entry.tenant.as_str(),
                entry.source_id.as_str(),
                entry.asserted_lineage_hash.as_str(),
                tp.db.as_str(),
                tp.table.as_str(),
            );
            // Adopt page by page, re-deriving the content digest as we go: the
            // marker is written only if what was adopted is exactly the planned
            // content.
            let mut h = Sha256::new();
            h.update(LEGACY_DOMAIN);
            let mut adopted = 0u64;
            let mut cursor = None;
            loop {
                let page = registry
                    .legacy_page(&entry.tenant, &tp.db, &tp.table, cursor, PAGE)
                    .await?;
                for lv in &page.versions {
                    h.update(lv.version.to_be_bytes());
                    h.update(lv.sequence.to_be_bytes());
                    lp(&mut h, lv.record_digest.as_bytes());
                    registry.adopt_legacy_version(&key, lv).await?;
                    adopted += 1;
                }
                match page.next {
                    Some(n) => cursor = Some(n),
                    None => break,
                }
            }
            let digest = hex::encode(h.finalize());
            anyhow::ensure!(
                Some(&digest) == tp.legacy_digest.as_ref(),
                "legacy history of {} changed during the migration; the marker \
                 was not written",
                tp.legacy_key
            );
            registry
                .mark_migrated_with(
                    &key,
                    MigrationProvenance {
                        migration_identity: tp
                            .migration_identity
                            .clone()
                            .expect("planned Migrate has an identity"),
                        proof_digest: plan.proof.clone(),
                        legacy_digest: digest,
                        legacy_versions: adopted,
                    },
                )
                .await?;
            out.migrated += 1;
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::schema_key::LineageDescriptor;
    use crate::adapters::test_util::FaultBackend;
    use serde_json::json;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    const TENANT: &str = "acme";
    const SOURCE: &str = "pg-orders";

    fn lineage() -> LineageDescriptor {
        LineageDescriptor::postgres(11, 22).unwrap()
    }

    fn fresh() -> (Arc<FaultBackend>, ArcStorageBackend) {
        let f = Arc::new(FaultBackend::new());
        let b: ArcStorageBackend = f.clone();
        (f, b)
    }

    /// Deterministic pre-upgrade entry (fixed timestamp so seeded backends are
    /// byte-identical).
    fn legacy_entry(
        hash: &str,
        payload: serde_json::Value,
        checkpoint: Option<Vec<u8>>,
    ) -> Vec<u8> {
        serde_json::to_vec(&json!({
            "hash": hash,
            "schema_json": payload,
            "registered_at": "2026-01-01T00:00:00Z",
            "checkpoint": checkpoint,
        }))
        .unwrap()
    }

    async fn seed_table(b: &ArcStorageBackend, table: &str, versions: usize) {
        for i in 1..=versions {
            b.log_append(
                "schemas",
                &format!("{TENANT}/public/{table}"),
                &legacy_entry(&format!("{table}-h{i}"), json!({"v": i}), None),
            )
            .await
            .unwrap();
        }
    }

    async fn establish(b: &ArcStorageBackend) -> String {
        source_lineage::establish(b, TENANT, SOURCE, lineage())
            .await
            .unwrap()
            .record
            .current
            .lineage_hash
    }

    fn mapping(lineage_hash: &str, tables: &[&str]) -> Mapping {
        Mapping {
            mappings: vec![MappingEntry {
                tenant: TENANT.into(),
                source_id: SOURCE.into(),
                lineage_hash: lineage_hash.into(),
                tables: tables
                    .iter()
                    .map(|t| TableRef {
                        db: "public".into(),
                        table: t.to_string(),
                    })
                    .collect(),
            }],
        }
    }

    async fn registry(b: &ArcStorageBackend) -> Arc<DurableSchemaRegistry> {
        DurableSchemaRegistry::new(Arc::clone(b)).await.unwrap()
    }

    fn key(lineage_hash: &str, table: &str) -> SchemaKey {
        SchemaKey::new(TENANT, SOURCE, lineage_hash, "public", table)
    }

    /// Everything observable about the migrated state (timestamps of markers
    /// excluded).
    async fn final_state(
        r: &DurableSchemaRegistry,
        lh: &str,
        tables: &[&str],
    ) -> Vec<(
        String,
        Vec<(i32, String, u64, String)>,
        Option<MigrationProvenance>,
    )> {
        let mut out = Vec::new();
        for t in tables {
            let k = key(lh, t);
            let mut versions = Vec::new();
            let mut cursor = None;
            loop {
                let p = r.history_page(&k, cursor, 100).await.unwrap();
                for v in p.versions {
                    versions.push((
                        v.version,
                        v.hash,
                        v.sequence,
                        v.schema_json.to_string(),
                    ));
                }
                match p.next {
                    Some(n) => cursor = Some(n),
                    None => break,
                }
            }
            let marker = r
                .migration_marker(&k)
                .await
                .unwrap()
                .and_then(|m| m.provenance);
            out.push((t.to_string(), versions, marker));
        }
        out
    }

    #[tokio::test]
    async fn dry_run_never_writes() {
        let (f, b) = fresh();
        seed_table(&b, "orders", 2).await;
        let lh = establish(&b).await;
        // Even opening the registry must not write (no high-water bootstrap).
        f.allow_writes(0);
        let r = DurableSchemaRegistry::open_for_inspection(Arc::clone(&b))
            .await
            .expect("inspection open performs no writes");
        let p = plan(&b, &r, &mapping(&lh, &["orders"]), &Filters::default())
            .await
            .expect("a dry run performs no writes");
        assert_eq!(p.totals.migrate, 1);
        assert_eq!(p.canonical.entries[0].tables[0].legacy_versions, 2);
    }

    #[tokio::test]
    async fn apply_requires_the_reviewed_proof() {
        let (_f, b) = fresh();
        seed_table(&b, "orders", 2).await;
        let lh = establish(&b).await;
        let r = registry(&b).await;
        let m = mapping(&lh, &["orders"]);
        let err = apply(&b, &r, &m, &Filters::default(), "not-the-proof")
            .await
            .unwrap_err();
        assert!(err.downcast_ref::<ProofMismatch>().is_some(), "{err:#}");
        assert!(
            r.history_page(&key(&lh, "orders"), None, 10)
                .await
                .unwrap()
                .versions
                .is_empty()
        );
        assert!(
            r.migration_marker(&key(&lh, "orders"))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn apply_preserves_identities_and_is_idempotent() {
        let (_f, b) = fresh();
        seed_table(&b, "orders", 3).await;
        seed_table(&b, "items", 2).await;
        let lh = establish(&b).await;
        let r = registry(&b).await;
        let m = mapping(&lh, &["orders", "items"]);
        let p = plan(&b, &r, &m, &Filters::default()).await.unwrap();
        let out = apply(&b, &r, &m, &Filters::default(), &p.proof)
            .await
            .unwrap();
        assert_eq!((out.migrated, out.already_migrated), (2, 0));

        let orders = r
            .history_page(&key(&lh, "orders"), None, 10)
            .await
            .unwrap()
            .versions;
        let hashes: Vec<_> = orders
            .iter()
            .map(|v| (v.version, v.hash.as_str()))
            .collect();
        assert_eq!(
            hashes,
            vec![(1, "orders-h1"), (2, "orders-h2"), (3, "orders-h3")]
        );
        let legacy = r
            .legacy_page(TENANT, "public", "orders", None, 10)
            .await
            .unwrap()
            .versions;
        let seqs: Vec<_> = orders.iter().map(|v| v.sequence).collect();
        assert_eq!(
            seqs,
            legacy.iter().map(|l| l.sequence).collect::<Vec<_>>(),
            "original sequences kept"
        );
        let marker = r
            .migration_marker(&key(&lh, "orders"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(marker.provenance.unwrap().proof_digest, p.proof);
        assert_eq!(legacy.len(), 3, "legacy records are never deleted");

        // The proof is stable after completion, and a rerun changes nothing.
        let again = plan(&b, &r, &m, &Filters::default()).await.unwrap();
        assert_eq!(again.proof, p.proof);
        assert_eq!(
            (again.totals.migrate, again.totals.already_migrated),
            (0, 2)
        );
        let before = final_state(&r, &lh, &["orders", "items"]).await;
        let out = apply(&b, &r, &m, &Filters::default(), &p.proof)
            .await
            .unwrap();
        assert_eq!((out.migrated, out.already_migrated), (0, 2));
        assert_eq!(final_state(&r, &lh, &["orders", "items"]).await, before);
    }

    /// Crash at EVERY write boundary of an apply (after each adopted version,
    /// after each completed table, before/after each marker), then retry the
    /// original command with the original proof: it must be accepted and
    /// converge to exactly the uninterrupted result.
    #[tokio::test]
    async fn retry_after_a_crash_at_any_write_converges_under_the_original_proof()
     {
        let tables = ["orders", "items"];
        async fn seeded() -> (Arc<FaultBackend>, ArcStorageBackend, String) {
            let (f, b) = fresh();
            seed_table(&b, "orders", 3).await;
            seed_table(&b, "items", 2).await;
            let lh = establish(&b).await;
            (f, b, lh)
        }

        // Reference run, counting its writes.
        let (f, b, lh) = seeded().await;
        let r = registry(&b).await;
        let m = mapping(&lh, &tables);
        let proof = plan(&b, &r, &m, &Filters::default()).await.unwrap().proof;
        const BUDGET: u64 = 1_000_000;
        f.allow_writes(BUDGET);
        apply(&b, &r, &m, &Filters::default(), &proof)
            .await
            .unwrap();
        let writes = BUDGET - f.writes_left.load(Ordering::SeqCst);
        let reference = final_state(&r, &lh, &tables).await;
        assert!(writes > 5, "the apply performs several writes ({writes})");
        eprintln!("crash matrix: {writes} write boundaries");

        for crash_after in 0..writes {
            let (f, b, lh2) = seeded().await;
            assert_eq!(lh2, lh);
            let r = registry(&b).await;
            assert_eq!(
                plan(&b, &r, &m, &Filters::default()).await.unwrap().proof,
                proof
            );

            f.allow_writes(crash_after);
            assert!(
                apply(&b, &r, &m, &Filters::default(), &proof)
                    .await
                    .is_err(),
                "crash injected after {crash_after} writes"
            );

            // Restart: new process, same command, same proof.
            f.allow_writes(u64::MAX);
            let r = registry(&b).await;
            let replanned =
                plan(&b, &r, &m, &Filters::default()).await.unwrap();
            assert_eq!(
                replanned.proof, proof,
                "the proof must survive a crash after {crash_after} writes"
            );
            apply(&b, &r, &m, &Filters::default(), &proof)
                .await
                .unwrap_or_else(|e| {
                    panic!("retry after {crash_after} writes: {e:#}")
                });
            assert_eq!(
                final_state(&r, &lh, &tables).await,
                reference,
                "state after crash at write {crash_after} + retry"
            );
        }
    }

    #[tokio::test]
    async fn proof_binds_payload_checkpoint_and_registration_time() {
        async fn proof_with(entry: Vec<u8>) -> String {
            let (_f, b) = fresh();
            b.log_append("schemas", &format!("{TENANT}/public/orders"), &entry)
                .await
                .unwrap();
            let lh = establish(&b).await;
            let r = registry(&b).await;
            plan(&b, &r, &mapping(&lh, &["orders"]), &Filters::default())
                .await
                .unwrap()
                .proof
        }
        let base = proof_with(legacy_entry("h1", json!({"c": 1}), None)).await;
        // Same declared hash, different payload.
        assert_ne!(
            base,
            proof_with(legacy_entry("h1", json!({"c": 2}), None)).await
        );
        // Same hash and payload, different checkpoint.
        assert_ne!(
            base,
            proof_with(legacy_entry("h1", json!({"c": 1}), Some(vec![1])))
                .await
        );
        // Same everything but the registration time.
        let later = serde_json::to_vec(&json!({
            "hash": "h1", "schema_json": {"c": 1},
            "registered_at": "2026-01-02T00:00:00Z", "checkpoint": null,
        }))
        .unwrap();
        assert_ne!(base, proof_with(later).await);
        // Deterministic for identical content.
        assert_eq!(
            base,
            proof_with(legacy_entry("h1", json!({"c": 1}), None)).await
        );
    }

    #[tokio::test]
    async fn proof_binds_filters_mapping_and_lineage() {
        let (_f, b) = fresh();
        seed_table(&b, "orders", 1).await;
        seed_table(&b, "items", 1).await;
        let lh = establish(&b).await;
        let r = registry(&b).await;
        let both = plan(
            &b,
            &r,
            &mapping(&lh, &["orders", "items"]),
            &Filters::default(),
        )
        .await
        .unwrap();
        let one = plan(&b, &r, &mapping(&lh, &["orders"]), &Filters::default())
            .await
            .unwrap();
        assert_ne!(both.proof, one.proof, "mapping identity is bound");
        let filtered = plan(
            &b,
            &r,
            &mapping(&lh, &["orders", "items"]),
            &Filters {
                tenant: Some(TENANT.into()),
                source_id: None,
            },
        )
        .await
        .unwrap();
        assert_ne!(both.proof, filtered.proof, "filters are bound");
        // A replaced database (new verified lineage) changes the proof and
        // rejects the stale assertion.
        source_lineage::establish(
            &b,
            TENANT,
            SOURCE,
            LineageDescriptor::postgres(33, 44).unwrap(),
        )
        .await
        .unwrap();
        let replaced = plan(
            &b,
            &r,
            &mapping(&lh, &["orders", "items"]),
            &Filters::default(),
        )
        .await
        .unwrap();
        assert_ne!(both.proof, replaced.proof);
        assert_eq!(replaced.totals.rejected, 2);
    }

    #[tokio::test]
    async fn unprovable_or_unsafe_tables_are_never_written() {
        let (_f, b) = fresh();
        for t in ["orders", "dup", "conflict", "foreign"] {
            seed_table(&b, t, 2).await;
        }
        let lh = establish(&b).await;
        let r = registry(&b).await;
        // Conflict: version 1 already present with another hash.
        let forged = r
            .legacy_page(TENANT, "public", "conflict", None, 10)
            .await
            .unwrap()
            .versions
            .remove(0);
        r.adopt_legacy_version(
            &key(&lh, "conflict"),
            &crate::adapters::LegacyVersion {
                hash: "other".into(),
                ..forged
            },
        )
        .await
        .unwrap();
        // Foreign marker: written without this action's identity.
        r.mark_migrated(&key(&lh, "foreign")).await.unwrap();

        let mut m = mapping(
            &lh,
            &["orders", "dup", "conflict", "foreign", "empty", "a/b"],
        );
        m.mappings.push(MappingEntry {
            tenant: TENANT.into(),
            source_id: "other-source".into(),
            lineage_hash: lh.clone(),
            tables: vec![TableRef {
                db: "public".into(),
                table: "dup".into(),
            }],
        });
        let p = plan(&b, &r, &m, &Filters::default()).await.unwrap();
        let class = |t: &str| {
            p.canonical.entries[0]
                .tables
                .iter()
                .find(|tp| tp.table == t)
                .unwrap()
                .classification
                .clone()
        };
        assert_eq!(class("orders"), Classification::Migrate);
        assert!(
            matches!(class("dup"), Classification::Ambiguous(r) if r.contains("more than one"))
        );
        assert!(
            matches!(class("a/b"), Classification::Ambiguous(r) if r.contains("`/`"))
        );
        assert!(
            matches!(class("conflict"), Classification::Rejected(r) if r.contains("different hash"))
        );
        assert!(
            matches!(class("foreign"), Classification::Rejected(r) if r.contains("different migration"))
        );
        assert!(
            matches!(class("empty"), Classification::Rejected(r) if r.contains("no pre-upgrade"))
        );
        // The other source's entry has no lineage record at all.
        assert!(matches!(
            &p.canonical.entries[1].tables[0].classification,
            Classification::Ambiguous(_)
        ));
        assert!(
            p.canonical.entries[0]
                .tables
                .iter()
                .find(|t| t.table == "foreign")
                .unwrap()
                .foreign_marker
                .is_some()
        );

        let out = apply(&b, &r, &m, &Filters::default(), &p.proof)
            .await
            .unwrap();
        assert_eq!(out.migrated, 1);
        for t in ["dup", "empty"] {
            assert!(
                r.migration_marker(&key(&lh, t)).await.unwrap().is_none(),
                "{t} untouched"
            );
            assert!(
                r.history_page(&key(&lh, t), None, 10)
                    .await
                    .unwrap()
                    .versions
                    .is_empty(),
                "{t} untouched"
            );
        }
        let conflict = r
            .history_page(&key(&lh, "conflict"), None, 10)
            .await
            .unwrap()
            .versions;
        assert_eq!(conflict.len(), 1, "the conflicting table was not extended");
    }

    #[tokio::test]
    async fn missing_or_wrong_lineage_is_rejected() {
        let (_f, b) = fresh();
        seed_table(&b, "orders", 1).await;
        let r = registry(&b).await;
        let p = plan(
            &b,
            &r,
            &mapping(&"a".repeat(32), &["orders"]),
            &Filters::default(),
        )
        .await
        .unwrap();
        assert!(matches!(
            &p.canonical.entries[0].tables[0].classification,
            Classification::Rejected(r) if r.contains("no verified lineage")
        ));
        let lh = establish(&b).await;
        let wrong = if lh.starts_with('a') {
            "b".repeat(32)
        } else {
            "a".repeat(32)
        };
        let p =
            plan(&b, &r, &mapping(&wrong, &["orders"]), &Filters::default())
                .await
                .unwrap();
        assert!(matches!(
            &p.canonical.entries[0].tables[0].classification,
            Classification::Rejected(r) if r.contains("is not the source's current")
        ));
    }
}
