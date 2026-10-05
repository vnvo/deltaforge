//! Manual measurement (not a gate suite): elapsed time and peak resident
//! memory of an initial snapshot over many single-row tables, at 1,000 and
//! 10,000 tables (`SNAPSHOT_SCALE_TABLES` overrides, comma-separated).
//! Prints the figures; asserts only that every table was copied. Run with:
//! `cargo test -p sources --test snapshot_scale_measure -- --include-ignored --nocapture --test-threads=1`

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use checkpoints::{CheckpointStore, MemCheckpointStore};
use deltaforge_config::{SnapshotCfg, SnapshotMode};
use deltaforge_core::{Op, Source, SourceItem};
use mysql_async::prelude::Queryable;
use tokio::sync::mpsc;

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
async fn measure(engine: &str, src: impl Source, n: usize) {
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
    let (tx, mut rx) = mpsc::channel(8192);
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
}

#[tokio::test]
#[ignore = "manual measurement"]
async fn postgres_snapshot_scale() -> Result<()> {
    for n in sizes() {
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
            registry: test_common::make_registry().await,
            registry_scope:
                sources::registry_scope::SharedRegistryScope::default(),
            backend: test_common::make_storage_backend().await,
            outbox_prefixes: common::AllowList::default(),
            snapshot_cfg: SnapshotCfg {
                mode: SnapshotMode::Initial,
                ..Default::default()
            },
            on_schema_drift: deltaforge_config::OnSchemaDrift::Adapt,
            table_options: Default::default(),
            rotation: None,
        };
        measure("postgres", src, n).await;
        client
            .execute("SELECT pg_drop_replication_slot($1)", &[&slot])
            .await
            .ok();
        pg_drop_db(&db).await;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "manual measurement"]
async fn mysql_snapshot_scale() -> Result<()> {
    for n in sizes() {
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
            registry: test_common::make_registry().await,
            registry_scope:
                sources::registry_scope::SharedRegistryScope::default(),
            outbox_tables: common::AllowList::default(),
            snapshot_cfg: SnapshotCfg {
                mode: SnapshotMode::Initial,
                ..Default::default()
            },
            backend: test_common::make_storage_backend().await,
            on_schema_drift: deltaforge_config::OnSchemaDrift::Adapt,
            table_options: Default::default(),
            rotation: None,
        };
        measure("mysql", src, n).await;
        test_common::mysql_drop_db(&pool, &db).await;
    }
    Ok(())
}
