//! Measurement of an initial snapshot over many single-row tables, at 1,000
//! and 10,000 tables (`SNAPSHOT_SCALE_TABLES` overrides, comma-separated),
//! over a SQLite state store: elapsed time, peak resident memory, boundary
//! and plan bytes. With both sizes it checks the acceptance gates of
//! `docs/design/snapshot-durable-queue.md` section 15 (10K against 1K):
//! boundary (frontier) plus progress bytes at most 12x, wall time at most
//! 15x, resident plan state flat (the same discovery page and worker
//! bounds at both sizes; design I12). Process RSS growth is reported, not
//! gated: it includes the table-count-proportional schema caches (the
//! loader's budgeted cache, the registry's), which are not queue state.
//! The schema registry shares the SQLite store. Run with:
//! `cargo test -p sources --test snapshot_scale_measure -- --include-ignored --nocapture --test-threads=1`

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use checkpoints::{CheckpointStore, MemCheckpointStore};
use deltaforge_config::{SnapshotCfg, SnapshotMode};
use deltaforge_core::{Op, Source, SourceItem};
use mysql_async::prelude::Queryable;

mod test_common;
use test_common::{MYSQL_CDC_USER, pg_admin_dsn, pg_drop_db, pg_setup};

fn sizes() -> Vec<usize> {
    std::env::var("SNAPSHOT_SCALE_TABLES")
        .ok()
        .map(|v| v.split(',').filter_map(|n| n.trim().parse().ok()).collect())
        .unwrap_or_else(|| vec![1_000, 10_000])
}

fn rss_kb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmRSS:"))
                .and_then(|l| l.split_whitespace().nth(1)?.parse().ok())
        })
        .unwrap_or(0)
}

/// Run `src` until `n` snapshot rows arrived and print the figures.
/// One measured snapshot.
#[derive(Debug, Clone, Copy)]
struct Measured {
    tables: usize,
    wall_s: f64,
    rss_growth_kib: u64,
    /// Resident plan bounds: the largest discovery page and the most
    /// concurrent table tasks.
    max_page: u64,
    max_tasks: u64,
    /// Boundary (frontier) plus progress bytes.
    tracked_bytes: u64,
    plan_bytes: u64,
}

/// The section-15 gates between the smallest and the largest measured size.
fn gates(engine: &str, runs: &[Measured]) {
    let (Some(small), Some(large)) = (runs.first(), runs.last()) else {
        return;
    };
    if large.tables != 10 * small.tables {
        return;
    }
    let bytes = large.tracked_bytes as f64 / small.tracked_bytes.max(1) as f64;
    let wall = large.wall_s / small.wall_s.max(0.001);
    let rss = large.rss_growth_kib as f64 / small.rss_growth_kib.max(1) as f64;
    println!(
        "{engine} gates 10K/1K: bytes {bytes:.2}x (<= 12), wall {wall:.2}x \
         (<= 15), resident plan bounds page {} -> {} and tasks {} -> {} \
         (equal), RSS growth {rss:.2}x ({} -> {} KiB, reported), plan {} -> \
         {} bytes (durable, paged)",
        small.max_page,
        large.max_page,
        small.max_tasks,
        large.max_tasks,
        small.rss_growth_kib,
        large.rss_growth_kib,
        small.plan_bytes,
        large.plan_bytes
    );
    assert!(bytes <= 12.0, "{engine}: bytes {bytes:.2}x");
    assert!(wall <= 15.0, "{engine}: wall time {wall:.2}x");
    assert_eq!(
        (large.max_page, large.max_tasks),
        (small.max_page, small.max_tasks),
        "{engine}: resident plan bounds grew"
    );
}

async fn measure(
    engine: &str,
    src: impl Source + Clone + 'static,
    id: &str,
    n: usize,
    backend: &storage::ArcStorageBackend,
) -> Measured {
    sources::snapshot_probe::reset();
    let before = rss_kb();
    let peak = Arc::new(AtomicU64::new(before));
    let sampler = {
        let peak = Arc::clone(&peak);
        tokio::spawn(async move {
            loop {
                peak.fetch_max(rss_kb(), Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
    };
    let chkpt: Arc<dyn CheckpointStore> =
        Arc::new(MemCheckpointStore::new().unwrap());
    let (tx, mut rx) = test_common::acked_channel(&src, &chkpt, id, 8192);
    let t0 = Instant::now();
    let handle = src.run(tx, chkpt).await;
    let mut reads = 0;
    while reads < n {
        match tokio::time::timeout(Duration::from_secs(1800), rx.recv()).await {
            Ok(Some(SourceItem::Event(ev))) if ev.op == Op::Read => reads += 1,
            Ok(Some(_)) => continue,
            _ => break,
        }
    }
    let elapsed = t0.elapsed();
    handle.stop();
    handle.join().await.ok();
    sampler.abort();
    assert_eq!(reads, n, "every table copied");
    let s = sources::snapshot_probe::shape();
    let mib = |kib: u64| kib as f64 / 1024.0;
    println!(
        "{engine} tables={n} total_s={:.1} discovery_s={:.2} preparation_s={:.1} \
         peak_rss_mib={:.1} rss_before_mib={:.1} pages={} max_page={} \
         verification_pages={} preparation_live_fetches={} worker_live_fetches={} \
         registry_reads={} max_table_tasks={} plan_kib={:.1} frontier_tables={}",
        elapsed.as_secs_f64(),
        s.discovery_micros as f64 / 1e6,
        s.preparation_micros as f64 / 1e6,
        mib(peak.load(Ordering::Relaxed)),
        mib(before),
        s.discovery_queries,
        s.max_discovery_page,
        s.verification_queries,
        s.preparation_live_fetches,
        s.worker_live_fetches,
        s.registry_reads,
        s.max_table_tasks,
        s.plan_bytes as f64 / 1024.0,
        s.frontier_tables,
    );
    let secs = |us: u64| us as f64 / 1e6;
    println!(
        "{engine} tables={n} phases: preflight_anchor_s={:.1} row_copy_s={:.1} \
         finalization_s={:.1} | progress_writes={} progress_mib={:.1} \
         progress_s={:.1} | boundaries={} boundary_mib={:.1} boundary_s={:.1}",
        secs(s.phase_micros[0]),
        secs(s.phase_micros[1]),
        secs(s.phase_micros[2]),
        s.progress_writes,
        mib(s.progress_bytes / 1024),
        secs(s.progress_micros),
        s.boundaries,
        mib(s.boundary_bytes / 1024),
        secs(s.boundary_micros),
    );
    let plan_bytes =
        match sources::snapshot_queue::QueueStore::new(backend.clone(), id)
            .read()
            .await
        {
            Ok(Some(sources::snapshot_queue::Stored::Current {
                control,
                ..
            })) => control.plan.bytes,
            _ => 0,
        };
    Measured {
        tables: n,
        wall_s: elapsed.as_secs_f64(),
        rss_growth_kib: peak.load(Ordering::Relaxed).saturating_sub(before),
        max_page: s.max_discovery_page,
        max_tasks: s.max_table_tasks,
        tracked_bytes: s.boundary_bytes + s.progress_bytes,
        plan_bytes,
    }
}

/// A SQLite state store in a fresh directory.
fn sqlite() -> (tempfile::TempDir, storage::ArcStorageBackend) {
    let dir = tempfile::tempdir().unwrap();
    let backend =
        storage::SqliteStorageBackend::open(dir.path().join("state.db"))
            .unwrap();
    (dir, backend)
}

#[tokio::test]
#[ignore = "manual measurement"]
async fn postgres_snapshot_scale() -> Result<()> {
    let mut runs = Vec::new();
    for n in sizes() {
        let (_dir, backend) = sqlite();
        let (db, client) = pg_setup(&format!("scale{n}")).await?;
        // Batches of 500 tables, each its own transaction (one transaction
        // creating every table exhausts the lock table).
        for start in (1..=n).step_by(500) {
            let end = (start + 499).min(n);
            client
                .batch_execute(&format!(
                    "DO $$ BEGIN FOR i IN {start}..{end} LOOP \
                       EXECUTE format('CREATE TABLE sc_%s (id INT PRIMARY KEY); \
                                       INSERT INTO sc_%s VALUES (1)', i, i); \
                     END LOOP; END $$;"
                ))
                .await?;
        }
        client
            .batch_execute("CREATE PUBLICATION pub_scale FOR ALL TABLES")
            .await?;
        let slot = format!("slot_scale{n}");
        let src = sources::postgres::PostgresSource {
            id: format!("scale{n}"),
            dsn: pg_admin_dsn(&db).await.into(),
            slot: slot.clone(),
            publication: "pub_scale".into(),
            tables: vec!["public.sc_*".into()],
            tenant: "acme".into(),
            pipeline: "test".into(),
            registry: storage::DurableSchemaRegistry::new(backend.clone())
                .await?,
            registry_scope:
                sources::registry_scope::SharedRegistryScope::default(),
            backend: backend.clone(),
            outbox_prefixes: common::AllowList::default(),
            snapshot_cfg: SnapshotCfg {
                mode: SnapshotMode::Initial,
                ..Default::default()
            },
            on_schema_drift: deltaforge_config::OnSchemaDrift::Adapt,
            table_options: Default::default(),
            rotation: None,
            snapshot_cohort: Default::default(),
        };
        let id = src.id.clone();
        runs.push(measure("postgres", src, &id, n, &backend).await);
        client
            .execute("SELECT pg_drop_replication_slot($1)", &[&slot])
            .await
            .ok();
        pg_drop_db(&db).await;
    }
    gates("postgres", &runs);
    Ok(())
}

#[tokio::test]
#[ignore = "manual measurement"]
async fn mysql_snapshot_scale() -> Result<()> {
    let mut runs = Vec::new();
    for n in sizes() {
        let (_dir, backend) = sqlite();
        let (db, pool, _dsn) =
            test_common::mysql_setup(&format!("scale{n}")).await?;
        let mut conn = pool.get_conn().await?;
        conn.query_drop(format!("USE {db}")).await?;
        for chunk in (1..=n).collect::<Vec<_>>().chunks(200) {
            let sql: String = chunk
                .iter()
                .map(|i| {
                    format!(
                        "CREATE TABLE sc_{i} (id INT PRIMARY KEY); \
                         INSERT INTO sc_{i} VALUES (1);"
                    )
                })
                .collect();
            conn.query_drop(sql).await?;
        }
        conn.query_drop(format!(
            "GRANT SELECT ON {db}.* TO '{MYSQL_CDC_USER}'@'%'"
        ))
        .await?;
        let src = sources::mysql::MySqlSource {
            id: format!("scale{n}"),
            dsn: test_common::mysql_cdc_dsn(&db).await.into(),
            tables: vec![format!("{db}.sc_*")],
            tenant: "acme".into(),
            pipeline: "test".into(),
            registry: storage::DurableSchemaRegistry::new(backend.clone())
                .await?,
            registry_scope:
                sources::registry_scope::SharedRegistryScope::default(),
            outbox_tables: common::AllowList::default(),
            snapshot_cfg: SnapshotCfg {
                mode: SnapshotMode::Initial,
                ..Default::default()
            },
            backend: backend.clone(),
            on_schema_drift: deltaforge_config::OnSchemaDrift::Adapt,
            table_options: Default::default(),
            rotation: None,
            snapshot_cohort: Default::default(),
        };
        let id = src.id.clone();
        runs.push(measure("mysql", src, &id, n, &backend).await);
        test_common::mysql_drop_db(&pool, &db).await;
    }
    gates("mysql", &runs);
    Ok(())
}
