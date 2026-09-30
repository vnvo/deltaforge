//! Synthetic durable schema registries, generated through the real
//! registration path and described by a manifest stored alongside them.

use std::time::Instant;

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use storage::ArcStorageBackend;
use storage::DurableSchemaRegistry;
use storage::adapters::{LineageDescriptor, SchemaKey, source_lineage};

/// Bumped whenever the generated content or layout changes.
pub const GENERATOR_FORMAT_VERSION: u32 = 1;
pub const TENANT: &str = "acme";
pub const DB: &str = "public";

const MANIFEST_NS: &str = "scale_harness";
const MANIFEST_KEY: &str = "manifest";
/// Pre-upgrade (flat key) schema namespace.
const LEGACY_NS: &str = "schemas";

/// Shape of the synthetic schema payloads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PayloadConfig {
    /// Columns of version 1; each later version adds one.
    pub columns: u32,
}

/// What to generate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FixtureSpec {
    pub seed: u64,
    /// Tables per source. Every source uses the SAME table names, so logical
    /// names collide across sources.
    pub tables: u64,
    pub versions: u32,
    pub sources: u32,
    pub payload: PayloadConfig,
    /// Pre-upgrade tables per legacy population (for the migration scenario).
    pub legacy_tables: u64,
    /// One legacy population per entry, with that many versions per table.
    pub legacy_versions: Vec<u32>,
}

/// Stored with the fixture; a reused store must match it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    pub generator_format_version: u32,
    pub spec: FixtureSpec,
    pub backend: String,
    pub git_revision: String,
    /// False until generation finished.
    pub complete: bool,
    pub generation_secs: f64,
}

/// A generated source: its id and verified lineage hash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SourceRef {
    pub source_id: String,
    pub lineage_hash: String,
}

/// A ready fixture.
#[derive(Debug, Clone, Serialize)]
pub struct Fixture {
    pub manifest: Manifest,
    pub sources: Vec<SourceRef>,
    /// True when an existing store was reused.
    pub reused: bool,
}

impl Fixture {
    pub fn key(&self, source: usize, table: u64) -> SchemaKey {
        let s = &self.sources[source];
        SchemaKey::new(
            TENANT,
            s.source_id.as_str(),
            s.lineage_hash.as_str(),
            DB,
            table_name(table),
        )
    }
}

pub fn git_revision() -> &'static str {
    env!("SCALE_HARNESS_GIT_REVISION")
}

pub fn source_id(i: u32) -> String {
    format!("src-{i}")
}

pub fn table_name(j: u64) -> String {
    format!("t{j:07}")
}

pub fn legacy_table_name(population: usize, j: u64) -> String {
    format!("legacy_p{population}_t{j:07}")
}

fn descriptor(i: u32) -> Result<LineageDescriptor> {
    LineageDescriptor::postgres(1_000_000 + u64::from(i), 16_384 + u64::from(i))
}

/// Small deterministic generator (SplitMix64).
fn mix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

const TYPES: &[(&str, u32)] = &[
    ("bigint", 20),
    ("integer", 23),
    ("text", 25),
    ("numeric", 1700),
    ("timestamp with time zone", 1184),
    ("boolean", 16),
    ("jsonb", 3802),
    ("character varying", 1043),
];

/// A PostgreSQL-like table schema. Depends only on (seed, source, table,
/// version, columns), never on the fixture size, so equal tables in fixtures
/// of different sizes are byte-identical.
pub fn payload(
    seed: u64,
    source: u32,
    table: u64,
    version: u32,
    cfg: &PayloadConfig,
    name: &str,
) -> Value {
    let base = mix(seed ^ mix(table) ^ (u64::from(source) << 48));
    let columns: Vec<Value> = (0..cfg.columns + version.saturating_sub(1))
        .map(|c| {
            let (ty, oid) =
                TYPES[(mix(base ^ u64::from(c)) as usize) % TYPES.len()];
            json!({
                "name": format!("c{c}"),
                "data_type": ty,
                "type_oid": oid,
                "nullable": c != 0,
                "ordinal_position": c + 1,
            })
        })
        .collect();
    json!({
        "schema_name": DB,
        "table_name": name,
        "oid": 16_384 + table,
        "replica_identity": "default",
        "primary_key": ["c0"],
        "columns": columns,
        "x_harness": {"source": source, "table": table, "version": version},
    })
}

fn hash_of(v: &Value) -> Result<String> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(v)?)))
}

async fn read_manifest(
    backend: &ArcStorageBackend,
) -> Result<Option<Manifest>> {
    match backend.kv_get(MANIFEST_NS, MANIFEST_KEY).await? {
        Some(b) => Ok(Some(
            serde_json::from_slice(&b).context("corrupt fixture manifest")?,
        )),
        None => Ok(None),
    }
}

async fn write_manifest(
    backend: &ArcStorageBackend,
    m: &Manifest,
) -> Result<()> {
    backend
        .kv_put(MANIFEST_NS, MANIFEST_KEY, &serde_json::to_vec(m)?)
        .await
}

async fn load_sources(
    backend: &ArcStorageBackend,
    n: u32,
) -> Result<Vec<SourceRef>> {
    let mut out = Vec::new();
    for i in 0..n {
        let id = source_id(i);
        let rec = source_lineage::load(backend, TENANT, &id)
            .await?
            .with_context(|| {
                format!("fixture source {id} has no lineage record")
            })?;
        out.push(SourceRef {
            source_id: id,
            lineage_hash: rec.current.lineage_hash,
        });
    }
    Ok(out)
}

/// Establish `source_id`'s lineage and register `spec.tables` x
/// `spec.versions` synthetic tables for it through the real registration path.
/// `index` selects the payload family. Returns the lineage hash.
pub async fn populate_source(
    backend: &ArcStorageBackend,
    index: u32,
    source_id: &str,
    descriptor: LineageDescriptor,
    spec: &FixtureSpec,
    progress: bool,
) -> Result<String> {
    let registry = DurableSchemaRegistry::new(backend.clone()).await?;
    let lineage_hash =
        source_lineage::establish(backend, TENANT, source_id, descriptor)
            .await?
            .record
            .current
            .lineage_hash;
    let started = Instant::now();
    let total = spec.tables * u64::from(spec.versions);
    let mut done = 0u64;
    for j in 0..spec.tables {
        let name = table_name(j);
        let key = SchemaKey::new(
            TENANT,
            source_id,
            lineage_hash.as_str(),
            DB,
            name.as_str(),
        );
        for v in 1..=spec.versions {
            let schema = payload(spec.seed, index, j, v, &spec.payload, &name);
            registry
                .register_with_checkpoint(
                    &key,
                    &hash_of(&schema)?,
                    &schema,
                    None,
                )
                .await?;
            done += 1;
            if progress && done.is_multiple_of(50_000) {
                eprintln!(
                    "  {source_id}: {done}/{total} versions ({:.0}/s)",
                    done as f64 / started.elapsed().as_secs_f64()
                );
            }
        }
    }
    Ok(lineage_hash)
}

/// Reuse the fixture in `backend` if its manifest matches exactly, generate
/// it if the store is empty, and refuse anything else.
pub async fn open_or_generate(
    backend: &ArcStorageBackend,
    backend_kind: &str,
    spec: &FixtureSpec,
    allow_revision_mismatch: bool,
    progress: bool,
) -> Result<Fixture> {
    if let Some(m) = read_manifest(backend).await? {
        ensure!(
            m.generator_format_version == GENERATOR_FORMAT_VERSION,
            "store was generated by generator format {} (this build writes {}); \
             use a fresh store",
            m.generator_format_version,
            GENERATOR_FORMAT_VERSION
        );
        ensure!(
            m.complete,
            "store holds an incomplete fixture (generation was interrupted); \
             use a fresh store"
        );
        ensure!(
            m.spec == *spec,
            "store fixture does not match the requested one\n  stored:    {:?}\n  \
             requested: {:?}",
            m.spec,
            spec
        );
        ensure!(
            m.backend == backend_kind,
            "store fixture was generated on backend {}, not {backend_kind}",
            m.backend
        );
        ensure!(
            allow_revision_mismatch || m.git_revision == git_revision(),
            "store fixture was generated at revision {}, this build is {}; pass \
             --allow-revision-mismatch to reuse it anyway",
            m.git_revision,
            git_revision()
        );
        let sources = load_sources(backend, spec.sources).await?;
        return Ok(Fixture {
            manifest: m,
            sources,
            reused: true,
        });
    }

    // Only an empty store may be generated into.
    for ns in [LEGACY_NS, "schemas.v1"] {
        if backend.log_ns_max_seq(ns).await? != 0 {
            bail!(
                "store has schema data but no fixture manifest; refusing to \
                 generate into it"
            );
        }
    }
    let mut manifest = Manifest {
        generator_format_version: GENERATOR_FORMAT_VERSION,
        spec: spec.clone(),
        backend: backend_kind.to_string(),
        git_revision: git_revision().to_string(),
        complete: false,
        generation_secs: 0.0,
    };
    write_manifest(backend, &manifest).await?;
    let started = Instant::now();

    // Pre-upgrade populations first, so their sequences do not depend on the
    // registry population's size. The pre-upgrade registry writer no longer
    // exists; entries are written in its exact record format.
    for (p, &versions) in spec.legacy_versions.iter().enumerate() {
        for j in 0..spec.legacy_tables {
            let name = legacy_table_name(p, j);
            for v in 1..=versions {
                let schema = payload(spec.seed, 0, j, v, &spec.payload, &name);
                let entry = json!({
                    "hash": hash_of(&schema)?,
                    "schema_json": schema,
                    "registered_at": "2026-01-01T00:00:00Z",
                    "checkpoint": null,
                });
                backend
                    .log_append(
                        LEGACY_NS,
                        &format!("{TENANT}/{DB}/{name}"),
                        &serde_json::to_vec(&entry)?,
                    )
                    .await?;
            }
        }
    }

    let mut sources = Vec::new();
    for i in 0..spec.sources {
        let id = source_id(i);
        let lineage_hash =
            populate_source(backend, i, &id, descriptor(i)?, spec, progress)
                .await?;
        sources.push(SourceRef {
            source_id: id,
            lineage_hash,
        });
    }

    manifest.complete = true;
    manifest.generation_secs = started.elapsed().as_secs_f64();
    write_manifest(backend, &manifest).await?;
    Ok(Fixture {
        manifest,
        sources,
        reused: false,
    })
}
