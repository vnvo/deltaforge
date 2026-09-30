//! Registry scenarios (1-7). Every phase runs against a fresh registry
//! instance (empty cache) over the counting backend, with counters reset at
//! the start of the phase.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use serde::Serialize;
use serde_json::Value;
use storage::adapters::schema_migration::{
    self, Filters, Mapping, MappingEntry, TableRef,
};
use storage::adapters::{
    LineageDescriptor, RegistryConfig, RegistryMetrics, source_lineage,
};
use storage::{ArcStorageBackend, DurableSchemaRegistry};

use crate::counting::{CountingBackend, KeyRead, OpVector};
use crate::fixture::{DB, Fixture, TENANT, legacy_table_name};
use crate::memory::{self, Rss};

/// Scenario knobs.
#[derive(Debug, Clone, Serialize)]
pub struct ScenarioConfig {
    /// Active tables of source 0 (looked up in scenarios 2, 3 and 6).
    pub active: u64,
    pub cache_max_bytes: usize,
    pub cache_max_entries: usize,
    /// Concurrent lookups of one cold key (scenario 4).
    pub concurrency: usize,
    /// Page size for the history scenario (5).
    pub history_page: usize,
}

/// Latency distribution of one phase's individual operations.
#[derive(Debug, Clone, Serialize)]
pub struct Latency {
    pub count: usize,
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub max_us: u64,
}

fn latency(mut v: Vec<Duration>) -> Option<Latency> {
    if v.is_empty() {
        return None;
    }
    v.sort_unstable();
    let at = |q: f64| {
        let i = ((v.len() as f64 - 1.0) * q).round() as usize;
        v[i].as_micros() as u64
    };
    Some(Latency {
        count: v.len(),
        p50_us: at(0.50),
        p95_us: at(0.95),
        p99_us: at(0.99),
        max_us: v.last().unwrap().as_micros() as u64,
    })
}

/// Registry counters (serializable view of [`RegistryMetrics`]).
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
pub struct Metrics {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub backend_loads: u64,
    pub coalesced_loads: u64,
    pub evictions: u64,
    pub oversized_uncached: u64,
    pub resident_entries: usize,
    pub resident_bytes: usize,
}

impl From<RegistryMetrics> for Metrics {
    fn from(m: RegistryMetrics) -> Self {
        Self {
            cache_hits: m.cache_hits,
            cache_misses: m.cache_misses,
            backend_loads: m.backend_loads,
            coalesced_loads: m.coalesced_loads,
            evictions: m.evictions,
            oversized_uncached: m.oversized_uncached,
            resident_entries: m.resident_entries,
            resident_bytes: m.resident_bytes,
        }
    }
}

/// Measurements of one phase.
#[derive(Debug, Clone, Serialize)]
pub struct Phase {
    /// Complete per-primitive operation vector.
    pub ops: OpVector,
    pub enumeration_calls: u64,
    pub elapsed_ms: f64,
    pub latency: Option<Latency>,
    pub registry: Option<Metrics>,
    pub memory: Rss,
}

fn phase(
    cb: &CountingBackend,
    started: Instant,
    lat: Vec<Duration>,
    reg: Option<&DurableSchemaRegistry>,
) -> Phase {
    let ops = cb.ops();
    Phase {
        enumeration_calls: CountingBackend::enumeration_calls(&ops),
        ops,
        elapsed_ms: started.elapsed().as_secs_f64() * 1e3,
        latency: latency(lat),
        registry: reg.map(|r| r.metrics().into()),
        memory: memory::now(),
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SmallCache {
    pub phase: Phase,
    pub budget_bytes: usize,
    pub lookups: u64,
    pub resident_within_budget: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct SingleFlight {
    pub phase: Phase,
    pub concurrency: usize,
    pub backend_loads: u64,
    /// Lookups that were served without their own load (coalesced or hit).
    pub served_without_load: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct History {
    pub phase: Phase,
    pub page_size: usize,
    pub pages: u64,
    pub versions_read: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Isolation {
    pub phase: Phase,
    pub reads: usize,
    /// Reads under any other source's key prefix.
    pub foreign_reads: usize,
    pub foreign_examples: Vec<KeyRead>,
    /// Every lookup returned the requested source's schema (the logical table
    /// names collide across sources).
    pub identity_ok: bool,
    /// Cache residency afterwards belongs to the looked-up source only.
    pub residency_only_own: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct RegistryReport {
    pub config: ScenarioConfig,
    pub startup: Phase,
    pub lookup_cold: Phase,
    pub lookup_cold_identity_ok: bool,
    pub lookup_hot: Phase,
    pub small_cache: SmallCache,
    pub single_flight: SingleFlight,
    pub history: History,
    pub isolation: Isolation,
}

fn registry_config(cfg: &ScenarioConfig) -> RegistryConfig {
    RegistryConfig {
        cache_max_bytes: cfg.cache_max_bytes,
        cache_max_entries: cfg.cache_max_entries,
        ..RegistryConfig::default()
    }
}

/// The schema's harness identity: (source, table, version).
fn identity(schema: &Value) -> Option<(u64, u64, u64)> {
    let x = schema.get("x_harness")?;
    Some((
        x.get("source")?.as_u64()?,
        x.get("table")?.as_u64()?,
        x.get("version")?.as_u64()?,
    ))
}

async fn lookups(
    reg: &DurableSchemaRegistry,
    fx: &Fixture,
    source: usize,
    tables: u64,
) -> Result<(Vec<Duration>, bool)> {
    let versions = u64::from(fx.manifest.spec.versions);
    let mut lat = Vec::with_capacity(tables as usize);
    let mut identity_ok = true;
    for j in 0..tables {
        let t = Instant::now();
        let v = reg
            .get_latest(&fx.key(source, j))
            .await?
            .with_context(|| format!("table {j} of source {source} missing"))?;
        lat.push(t.elapsed());
        identity_ok &=
            identity(&v.schema_json) == Some((source as u64, j, versions));
    }
    Ok((lat, identity_ok))
}

/// Key prefix (`v1/<tenant>/<source>/<lineage>/`) of a source's qualified keys.
fn source_prefix(fx: &Fixture, source: usize) -> String {
    let k = fx.key(source, 0).backend_key();
    let parts: Vec<&str> = k.split('/').take(4).collect();
    format!("{}/", parts.join("/"))
}

/// Scenarios 1-6 over `cb` (which must wrap the fixture's store).
pub async fn run_registry(
    cb: &Arc<CountingBackend>,
    fx: &Fixture,
    cfg: &ScenarioConfig,
) -> Result<RegistryReport> {
    let backend: ArcStorageBackend = cb.clone();
    ensure!(
        cfg.active <= fx.manifest.spec.tables,
        "active set larger than the fixture"
    );
    ensure!(
        fx.sources.len() >= 2,
        "isolation needs at least two sources"
    );

    // 1. Startup.
    cb.reset();
    let t = Instant::now();
    let reg = DurableSchemaRegistry::with_config(
        backend.clone(),
        registry_config(cfg),
    )
    .await?;
    let startup = phase(cb, t, vec![], Some(&reg));

    // 2. Cold then hot lookups of the active set.
    cb.reset();
    let t = Instant::now();
    let (lat, lookup_cold_identity_ok) =
        lookups(&reg, fx, 0, cfg.active).await?;
    let lookup_cold = phase(cb, t, lat, Some(&reg));
    cb.reset();
    let t = Instant::now();
    let (lat, _) = lookups(&reg, fx, 0, cfg.active).await?;
    let lookup_hot = phase(cb, t, lat, Some(&reg));
    let avg_weight = lookup_cold
        .registry
        .map(|m| m.resident_bytes / (m.resident_entries.max(1)))
        .unwrap_or(1)
        .max(1);
    drop(reg);

    // 3. Working set larger than the cache: budget for a quarter of it.
    let budget = (avg_weight * (cfg.active as usize / 4).max(1)).max(1);
    let small = DurableSchemaRegistry::with_config(
        backend.clone(),
        RegistryConfig {
            cache_max_bytes: budget,
            ..registry_config(cfg)
        },
    )
    .await?;
    cb.reset();
    let t = Instant::now();
    let (mut lat, _) = lookups(&small, fx, 0, cfg.active).await?;
    lat.extend(lookups(&small, fx, 0, cfg.active).await?.0);
    let small_phase = phase(cb, t, lat, Some(&small));
    let resident = small.metrics().resident_bytes;
    let small_cache = SmallCache {
        phase: small_phase,
        budget_bytes: budget,
        lookups: cfg.active * 2,
        resident_within_budget: resident <= budget,
    };
    drop(small);

    // 4. Single-flight: concurrent lookups of one cold key.
    let sf = DurableSchemaRegistry::with_config(
        backend.clone(),
        registry_config(cfg),
    )
    .await?;
    cb.reset();
    let t = Instant::now();
    let gate = Arc::new(tokio::sync::Barrier::new(cfg.concurrency));
    let mut tasks = Vec::new();
    for _ in 0..cfg.concurrency {
        let (sf, gate, key) = (sf.clone(), gate.clone(), fx.key(0, 0));
        tasks.push(tokio::spawn(async move {
            gate.wait().await;
            let t = Instant::now();
            sf.get_latest(&key).await.map(|v| (v, t.elapsed()))
        }));
    }
    let mut lat = Vec::new();
    for task in tasks {
        let (v, d) = task.await??;
        ensure!(v.is_some(), "single-flight lookup returned nothing");
        lat.push(d);
    }
    let sf_phase = phase(cb, t, lat, Some(&sf));
    let m = sf.metrics();
    let single_flight = SingleFlight {
        phase: sf_phase,
        concurrency: cfg.concurrency,
        backend_loads: m.backend_loads,
        served_without_load: m.coalesced_loads + m.cache_hits,
    };
    drop(sf);

    // 5. Historical pages of one table.
    let hist = DurableSchemaRegistry::with_config(
        backend.clone(),
        registry_config(cfg),
    )
    .await?;
    cb.reset();
    let t = Instant::now();
    let (mut cursor, mut pages, mut versions_read, mut lat) =
        (None, 0u64, 0u64, Vec::new());
    loop {
        let pt = Instant::now();
        let page = hist
            .history_page(&fx.key(0, 0), cursor, cfg.history_page)
            .await?;
        lat.push(pt.elapsed());
        pages += 1;
        versions_read += page.versions.len() as u64;
        match page.next {
            Some(n) => cursor = Some(n),
            None => break,
        }
    }
    let history = History {
        phase: phase(cb, t, lat, Some(&hist)),
        page_size: cfg.history_page,
        pages,
        versions_read,
    };
    drop(hist);

    // 6. Cross-source isolation with colliding logical names.
    let foreign: Vec<String> = (1..fx.sources.len())
        .map(|s| source_prefix(fx, s))
        .collect();
    let iso = DurableSchemaRegistry::with_config(
        backend.clone(),
        registry_config(cfg),
    )
    .await?;
    cb.reset();
    cb.record_keys(true);
    let t = Instant::now();
    let (lat, identity_ok) = lookups(&iso, fx, 0, cfg.active).await?;
    let iso_phase = phase(cb, t, lat, Some(&iso));
    let reads = cb.reads();
    cb.record_keys(false);
    let foreign_hits: Vec<KeyRead> = reads
        .iter()
        .filter(|r| foreign.iter().any(|p| r.key.contains(p.as_str())))
        .cloned()
        .collect();
    let own = &fx.sources[0];
    let residency_only_own =
        iso.per_source_accounting()
            .iter()
            .all(|((tenant, src, lh), _, _)| {
                tenant == TENANT
                    && *src == own.source_id
                    && *lh == own.lineage_hash
            });
    let isolation = Isolation {
        phase: iso_phase,
        reads: reads.len(),
        foreign_reads: foreign_hits.len(),
        foreign_examples: foreign_hits.into_iter().take(5).collect(),
        identity_ok,
        residency_only_own,
    };

    Ok(RegistryReport {
        config: cfg.clone(),
        startup,
        lookup_cold,
        lookup_cold_identity_ok,
        lookup_hot,
        small_cache,
        single_flight,
        history,
        isolation,
    })
}

/// One migration run (scenario 7).
#[derive(Debug, Clone, Serialize)]
pub struct MigrationRun {
    pub population: usize,
    pub tables: u64,
    pub versions_per_table: u32,
    pub page_size: usize,
    pub target_source: String,
    pub plan: Phase,
    pub apply: Phase,
    pub migrated: u64,
    pub rejected: u64,
    pub ambiguous: u64,
    /// RSS when the run started, and the peak during mapping + plan + apply.
    pub rss_before_bytes: u64,
    pub peak_bytes: u64,
    pub peak_delta_bytes: u64,
    pub peak_delta_bytes_per_table: f64,
    /// False when the peak could not be reset (the peak then covers the whole
    /// process lifetime).
    pub peak_reset: bool,
}

/// Scenario 7: migrate legacy population `population` into a fresh target
/// source `target_source` (so the legacy data is never modified and every run
/// starts from the same state), with legacy page size `page_size`.
pub async fn run_migration(
    cb: &Arc<CountingBackend>,
    fx: &Fixture,
    population: usize,
    page_size: usize,
    target_source: &str,
) -> Result<MigrationRun> {
    let backend: ArcStorageBackend = cb.clone();
    let spec = &fx.manifest.spec;
    let versions = *spec
        .legacy_versions
        .get(population)
        .context("no such legacy population")?;
    let sysid = 3_000_000
        + u64::from_str_radix(
            &hex::encode(sha2::Sha256::digest(target_source.as_bytes()))[..8],
            16,
        )?;
    let lineage_hash = source_lineage::establish(
        &backend,
        TENANT,
        target_source,
        LineageDescriptor::postgres(sysid, 16_384)?,
    )
    .await?
    .record
    .current
    .lineage_hash;

    let peak_reset = memory::reset_peak();
    let before = memory::now();
    let mapping = Mapping {
        mappings: vec![MappingEntry {
            tenant: TENANT.into(),
            source_id: target_source.into(),
            lineage_hash,
            tables: (0..spec.legacy_tables)
                .map(|j| TableRef {
                    db: DB.into(),
                    table: legacy_table_name(population, j),
                })
                .collect(),
        }],
    };
    let filters = Filters::default();

    cb.reset();
    let t = Instant::now();
    let inspect =
        DurableSchemaRegistry::open_for_inspection(backend.clone()).await?;
    let plan = schema_migration::plan_paged(
        &backend, &inspect, &mapping, &filters, page_size,
    )
    .await?;
    let plan_phase = phase(cb, t, vec![], None);
    drop(inspect);

    cb.reset();
    let t = Instant::now();
    let reg = DurableSchemaRegistry::new(backend.clone()).await?;
    let out = schema_migration::apply_paged(
        &backend,
        &reg,
        &mapping,
        &filters,
        &plan.proof,
        page_size,
    )
    .await?;
    let apply_phase = phase(cb, t, vec![], Some(&reg));
    let after = memory::now();
    let peak_delta = after.peak_bytes.saturating_sub(before.rss_bytes);
    Ok(MigrationRun {
        population,
        tables: spec.legacy_tables,
        versions_per_table: versions,
        page_size,
        target_source: target_source.into(),
        plan: plan_phase,
        apply: apply_phase,
        migrated: out.migrated,
        rejected: out.rejected,
        ambiguous: out.ambiguous,
        rss_before_bytes: before.rss_bytes,
        peak_bytes: after.peak_bytes,
        peak_delta_bytes: peak_delta,
        peak_delta_bytes_per_table: peak_delta as f64
            / spec.legacy_tables.max(1) as f64,
        peak_reset,
    })
}

use sha2::Digest as _;
