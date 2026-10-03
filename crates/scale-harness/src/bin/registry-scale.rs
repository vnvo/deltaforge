//! Scale tier of the dense-catalog harness: generate (or reuse) a synthetic
//! durable registry and record every scenario's numbers in a JSON report.
//!
//!   cargo run --release -p scale-harness --bin registry-scale -- \
//!       --tables 100000 --versions 3 --sources 2 \
//!       --backend sqlite --store /data/reg-100k.db --out report-100k.json

use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use scale_harness::counting::CountingBackend;
use scale_harness::fixture::{self, FixtureSpec, PayloadConfig};
use scale_harness::scenarios::{self, ScenarioConfig};
use serde::Serialize;
use storage::ArcStorageBackend;

#[derive(Parser, Debug)]
#[command(name = "registry-scale")]
struct Args {
    /// sqlite (default), postgres, or memory.
    #[arg(long, default_value = "sqlite")]
    backend: String,
    /// SQLite store path (reused if it holds a matching fixture).
    #[arg(long, default_value = "./registry-scale.db")]
    store: String,
    /// PostgreSQL DSN (postgres backend).
    #[arg(long)]
    dsn: Option<String>,
    #[arg(long, default_value_t = 42)]
    seed: u64,
    /// Tables per source (names collide across sources).
    #[arg(long, default_value_t = 1000)]
    tables: u64,
    #[arg(long, default_value_t = 3)]
    versions: u32,
    #[arg(long, default_value_t = 2)]
    sources: u32,
    /// Columns of each table's first version.
    #[arg(long, default_value_t = 12)]
    columns: u32,
    /// Pre-upgrade tables per legacy population (0 skips the migration).
    #[arg(long, default_value_t = 1000)]
    legacy_tables: u64,
    /// Versions per table of each legacy population.
    #[arg(long, value_delimiter = ',', default_value = "1,5")]
    legacy_versions: Vec<u32>,
    /// Legacy page sizes to migrate with.
    #[arg(long, value_delimiter = ',', default_value = "16,256")]
    migration_pages: Vec<usize>,
    /// Active tables of source 0.
    #[arg(long, default_value_t = 100)]
    active: u64,
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    cache_max_bytes: usize,
    #[arg(long, default_value_t = 50_000)]
    cache_max_entries: usize,
    #[arg(long, default_value_t = 64)]
    concurrency: usize,
    #[arg(long, default_value_t = 256)]
    history_page: usize,
    /// Reuse a store generated at another git revision.
    #[arg(long)]
    allow_revision_mismatch: bool,
    /// Also measure time to first CDC event against live databases
    /// (postgres, mysql; needs Docker).
    #[arg(long, value_delimiter = ',')]
    live: Vec<String>,
    /// Synthetic tables registered for the live source.
    #[arg(long, default_value_t = 100_000)]
    live_tables: u64,
    /// Real tables created in the live database, matched by the source's
    /// table pattern.
    #[arg(long, default_value_t = 0)]
    live_catalog_tables: u64,
    /// Directory for the live scenario's fresh SQLite stores.
    #[arg(long, default_value = ".")]
    live_dir: String,
    /// Skip the synthetic-registry scenarios (1-7); only run --live.
    #[arg(long)]
    live_only: bool,
    /// Internal: run one migration `<population>:<page>:<target>` on the reused
    /// store and print its JSON (each migration is measured in a fresh
    /// process, so its peak memory is its own).
    #[arg(long, hide = true)]
    migration_child: Option<String>,
    /// Report path.
    #[arg(long, default_value = "registry-scale-report.json")]
    out: String,
}

#[derive(Serialize)]
struct Report {
    harness_revision: &'static str,
    started_at: String,
    host: Host,
    fixture: Option<fixture::Fixture>,
    registry: Option<scenarios::RegistryReport>,
    migration: Vec<serde_json::Value>,
    live: Vec<scale_harness::live::LiveReport>,
}

#[derive(Serialize)]
struct Host {
    os: &'static str,
    arch: &'static str,
    cpus: usize,
}

async fn open_backend(a: &Args) -> Result<ArcStorageBackend> {
    Ok(match a.backend.as_str() {
        "memory" => Arc::new(storage::MemoryStorageBackend::new()),
        "sqlite" => storage::SqliteStorageBackend::open(&a.store)
            .with_context(|| format!("open {}", a.store))?,
        "postgres" => {
            storage::PostgresStorageBackend::connect(
                a.dsn.as_deref().context("--dsn is required for postgres")?,
            )
            .await?
        }
        other => anyhow::bail!("unknown backend {other}"),
    })
}

fn mib(b: u64) -> f64 {
    b as f64 / (1024.0 * 1024.0)
}

async fn synthetic(
    a: &Args,
    spec: &FixtureSpec,
) -> Result<(
    fixture::Fixture,
    scenarios::RegistryReport,
    Vec<serde_json::Value>,
)> {
    let store = open_backend(a).await?;
    eprintln!("fixture: {spec:?} on {}", a.backend);
    let fx = fixture::open_or_generate(
        &store,
        &a.backend,
        spec,
        a.allow_revision_mismatch,
        true,
    )
    .await?;
    eprintln!(
        "fixture {} (generated in {:.1}s at {})",
        if fx.reused { "reused" } else { "generated" },
        fx.manifest.generation_secs,
        fx.manifest.git_revision
    );

    let cb = Arc::new(CountingBackend::new(store));
    let cfg = ScenarioConfig {
        active: a.active.min(a.tables),
        cache_max_bytes: a.cache_max_bytes,
        cache_max_entries: a.cache_max_entries,
        concurrency: a.concurrency,
        history_page: a.history_page,
    };
    let registry = scenarios::run_registry(&cb, &fx, &cfg).await?;

    let mut migration = Vec::new();
    if a.legacy_tables > 0 {
        let tag = chrono::Utc::now().timestamp_millis();
        for p in 0..a.legacy_versions.len() {
            for &page in &a.migration_pages {
                let target = format!("mig-{tag}-p{p}-pg{page}");
                eprintln!(
                    "migration: population {p}, page {page} -> {target} (fresh process)"
                );
                let out = std::process::Command::new(std::env::current_exe()?)
                    .args(std::env::args().skip(1))
                    .args([
                        "--migration-child",
                        &format!("{p}:{page}:{target}"),
                    ])
                    .stderr(std::process::Stdio::inherit())
                    .output()
                    .context("run the migration child")?;
                anyhow::ensure!(out.status.success(), "migration child failed");
                migration.push(
                    serde_json::from_slice::<serde_json::Value>(&out.stdout)
                        .context("migration child output")?,
                );
            }
        }
    }

    let r = &registry;
    println!(
        "registry startup   : {:.2} ms, ops {:?}",
        r.startup.elapsed_ms, r.startup.ops
    );
    for (name, p) in [
        ("lookup cold", &r.lookup_cold),
        ("lookup hot", &r.lookup_hot),
    ] {
        println!(
            "{name:<18} : {:?} ops {:?}",
            p.latency.as_ref().map(|l| (l.p50_us, l.p99_us)),
            p.ops
        );
    }
    println!(
        "small cache        : budget {} B, within budget {}, evictions {:?}",
        r.small_cache.budget_bytes,
        r.small_cache.resident_within_budget,
        r.small_cache.phase.registry.map(|m| m.evictions)
    );
    println!(
        "single-flight      : {} lookups, {} backend load(s)",
        r.single_flight.concurrency, r.single_flight.backend_loads
    );
    println!(
        "history            : {} versions in {} page(s)",
        r.history.versions_read, r.history.pages
    );
    println!(
        "isolation          : {} reads, {} foreign, identity ok {}",
        r.isolation.reads, r.isolation.foreign_reads, r.isolation.identity_ok
    );
    for m in &migration {
        let n = |k: &str| m[k].as_f64().unwrap_or(0.0);
        println!(
            "migration p{} v{} page {:>4}: {} tables in {:.0} ms (plan {:.0} ms), \
             process peak {:.1} MiB, +{:.1} MiB over start ({:.0} B/table)",
            m["population"],
            m["versions_per_table"],
            m["page_size"],
            m["migrated"],
            m["apply"]["elapsed_ms"].as_f64().unwrap_or(0.0),
            m["plan"]["elapsed_ms"].as_f64().unwrap_or(0.0),
            mib(n("peak_bytes") as u64),
            mib(n("peak_delta_bytes") as u64),
            n("peak_delta_bytes_per_table"),
        );
    }

    Ok((fx, registry, migration))
}

/// One migration measurement in this (fresh) process on the reused store.
async fn migration_child(a: &Args, child: &str) -> Result<()> {
    let mut parts = child.splitn(3, ':');
    let (Some(p), Some(page), Some(target)) =
        (parts.next(), parts.next(), parts.next())
    else {
        anyhow::bail!("bad --migration-child {child}");
    };
    let spec = FixtureSpec {
        seed: a.seed,
        tables: a.tables,
        versions: a.versions,
        sources: a.sources,
        payload: PayloadConfig { columns: a.columns },
        legacy_tables: a.legacy_tables,
        legacy_versions: a.legacy_versions.clone(),
    };
    let store = open_backend(a).await?;
    let fx = fixture::open_or_generate(
        &store,
        &a.backend,
        &spec,
        a.allow_revision_mismatch,
        false,
    )
    .await?;
    anyhow::ensure!(
        fx.reused,
        "the migration child must reuse the generated store"
    );
    let cb = Arc::new(CountingBackend::new(store));
    let run =
        scenarios::run_migration(&cb, &fx, p.parse()?, page.parse()?, target)
            .await?;
    println!("{}", serde_json::to_string(&run)?);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let a = Args::parse();
    let started_at = chrono::Utc::now().to_rfc3339();
    if let Some(child) = &a.migration_child {
        return migration_child(&a, child).await;
    }
    let spec = FixtureSpec {
        seed: a.seed,
        tables: a.tables,
        versions: a.versions,
        sources: a.sources,
        payload: PayloadConfig { columns: a.columns },
        legacy_tables: a.legacy_tables,
        legacy_versions: a.legacy_versions.clone(),
    };
    let (fx, registry, migration) = if a.live_only {
        (None, None, Vec::new())
    } else {
        let (fx, registry, migration) = synthetic(&a, &spec).await?;
        (Some(fx), Some(registry), migration)
    };

    let mut live = Vec::new();
    for engine in &a.live {
        let engine = match engine.as_str() {
            "postgres" => scale_harness::live::Engine::Postgres,
            "mysql" => scale_harness::live::Engine::Mysql,
            other => anyhow::bail!("unknown --live engine {other}"),
        };
        let path = std::path::Path::new(&a.live_dir).join(format!(
            "live-{engine:?}-{}.db",
            chrono::Utc::now().timestamp_millis()
        ));
        let store: ArcStorageBackend =
            storage::SqliteStorageBackend::open(&path)?;
        eprintln!(
            "live {engine:?}: {} synthetic tables, store {}",
            a.live_tables,
            path.display()
        );
        let r = scale_harness::live::run(
            &scale_harness::live::LiveConfig {
                engine,
                tables: a.live_tables,
                catalog_tables: a.live_catalog_tables,
                matched_tables: a.live_catalog_tables,
                versions: a.versions,
                columns: a.columns,
                seed: a.seed,
            },
            store,
        )
        .await?;
        println!(
            "live {:?}: first event after {:.0} ms ({} synthetic tables, {} storage \
             reads, {} enumeration calls)",
            r.engine,
            r.time_to_first_event_ms,
            r.synthetic_tables,
            r.reads_by_namespace.iter().map(|(_, n)| n).sum::<usize>(),
            r.enumeration_calls
        );
        live.push(r);
    }

    let report = Report {
        harness_revision: fixture::git_revision(),
        started_at,
        host: Host {
            os: std::env::consts::OS,
            arch: std::env::consts::ARCH,
            cpus: std::thread::available_parallelism().map_or(0, |n| n.get()),
        },
        fixture: fx,
        registry,
        migration,
        live,
    };
    std::fs::write(&a.out, serde_json::to_vec_pretty(&report)?)
        .with_context(|| format!("write {}", a.out))?;
    eprintln!("report written to {}", a.out);
    Ok(())
}
