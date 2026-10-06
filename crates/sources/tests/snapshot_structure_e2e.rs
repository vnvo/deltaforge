//! Structural evidence for the durable snapshot queue
//! (`docs/design/snapshot-durable-queue.md`, section 15): the durable
//! operations of one complete snapshot generation, counted on the real
//! storage path, at 10, 30 and 90 tables, on both engines.
//!
//! - control writes: exactly 5 (allocate, seal, adoption done, rows
//!   produced, completed);
//! - plan writes: one create per table, and one delete per table after
//!   completion;
//! - plan reads: whole page walks over the plan, linear in pages;
//! - ownership checks: exactly one control read per publish (every chunk,
//!   plus the start and terminal barriers);
//! - other control reads: bounded by a constant (decision, adoption and
//!   completion polls);
//! - boundary bytes: constant per boundary; progress writes: none.
//!
//! Run with:
//! `cargo test -p sources --test snapshot_structure_e2e -- --include-ignored --nocapture --test-threads=1`

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use checkpoints::{CheckpointStore, MemCheckpointStore};
use deltaforge_config::{SnapshotCfg, SnapshotMode};
use deltaforge_core::{Op, Source, SourceItem};
use mysql_async::prelude::Queryable;
use scale_harness::counting::CountingBackend;
use storage::ArcStorageBackend;

mod test_common;
use test_common::{
    MYSQL_CDC_USER, acked_channel, pg_admin_dsn, pg_drop_db, pg_setup,
    snapshot_state,
};

const SIZES: [u64; 3] = [10, 30, 90];
const PAGE: usize = 10;

/// What one complete generation did.
#[derive(Debug)]
struct Counts {
    tables: u64,
    slots: BTreeMap<(&'static str, String), u64>,
    shape: sources::snapshot_probe::SnapshotShape,
}

impl Counts {
    fn slot(&self, primitive: &str, ns: &str) -> u64 {
        self.slots
            .iter()
            .filter(|((p, n), _)| *p == primitive && n == ns)
            .map(|(_, c)| c)
            .sum()
    }
}

fn cfg() -> SnapshotCfg {
    SnapshotCfg {
        mode: SnapshotMode::Initial,
        discovery_page_size: PAGE,
        max_parallel_tables: 3,
        ..Default::default()
    }
}

/// Drive `src` (over `counting`) through one complete generation of
/// `tables` single-row tables; `inner` is the uncounted backend polled for
/// completion.
async fn complete<S>(
    src: &S,
    id: &str,
    tables: u64,
    counting: &CountingBackend,
    inner: &ArcStorageBackend,
) -> Result<Counts>
where
    S: Source + Clone + 'static,
{
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    sources::snapshot_probe::reset();
    counting.reset();
    let (tx, mut rx) = acked_channel(src, &ckpt, id, 8192);
    let handle = src.run(tx, Arc::clone(&ckpt)).await;
    let mut reads = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(300);
    while reads < tables {
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(SourceItem::Event(ev))) if ev.op == Op::Read => reads += 1,
            Ok(Some(_)) => continue,
            other => {
                anyhow::bail!("{id}: no more rows after {reads}: {other:?}")
            }
        }
    }
    loop {
        if snapshot_state(inner, id).await.map(|s| s.0)
            == Some("completed".into())
        {
            break;
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "{id}: never completed"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // Let the completion's plan reclamation finish.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let counts = Counts {
        tables,
        slots: counting.slot_ops(),
        shape: sources::snapshot_probe::shape(),
    };
    handle.stop();
    let _ = handle.join().await;
    Ok(counts)
}

/// The section-15 formulas, for one engine over the three sizes.
fn assert_structure(engine: &str, runs: &[Counts]) {
    let walks: Vec<u64> = runs
        .iter()
        .map(|c| c.slot("slot_list", "snapshot_plan"))
        .collect();
    for c in runs {
        let n = c.tables;
        let what = format!("{engine} n={n}");
        eprintln!(
            "{what}: control create={} cas={} get={}, plan create={} \
             delete={} list={}, owner checks={}, boundaries={} ({} bytes), \
             progress writes={}",
            c.slot("slot_create", "snapshot_state"),
            c.slot("slot_cas", "snapshot_state"),
            c.slot("slot_get", "snapshot_state"),
            c.slot("slot_create", "snapshot_plan"),
            c.slot("slot_delete", "snapshot_plan"),
            c.slot("slot_list", "snapshot_plan"),
            c.shape.owner_checks,
            c.shape.boundaries,
            c.shape.boundary_bytes,
            c.shape.progress_writes,
        );
        // Control writes: allocate, then seal, adoption, rows, completed.
        assert_eq!(c.slot("slot_create", "snapshot_state"), 1, "{what}");
        assert_eq!(c.slot("slot_cas", "snapshot_state"), 4, "{what}");
        // Plan writes: one item per table, deleted after completion.
        assert_eq!(c.slot("slot_create", "snapshot_plan"), n, "{what}");
        assert_eq!(c.slot("slot_delete", "snapshot_plan"), n, "{what}");
        assert_eq!(c.slot("slot_cas", "snapshot_plan"), 0, "{what}");
        // One ownership check per publish: every chunk and both barriers.
        assert_eq!(c.shape.owner_checks, c.shape.boundaries + 2, "{what}");
        // Every other control read is a constant (decision, adoption and
        // completion polls), independent of the table count.
        let other = c.slot("slot_get", "snapshot_state") - c.shape.owner_checks;
        assert!(other <= 40, "{what}: {other} other control reads");
        // O(1) boundaries; no progress record, no per-table frontier.
        assert!(c.shape.boundaries >= n, "{what}");
        assert!(
            c.shape.boundary_bytes / c.shape.boundaries.max(1) <= 256,
            "{what}: {} bytes per boundary",
            c.shape.boundary_bytes / c.shape.boundaries.max(1)
        );
        assert_eq!(c.shape.progress_writes, 0, "{what}");
        assert_eq!(c.shape.frontier_tables, 0, "{what}");
    }
    // Plan reads are whole page walks: linear in pages (30 -> 90 tables is
    // three times the pages 10 -> 30 adds).
    assert_eq!(
        walks[2] - walks[1],
        3 * (walks[1] - walks[0]),
        "{engine}: plan page reads {walks:?}"
    );
    // Bytes per boundary do not grow with the table count.
    let per = |c: &Counts| c.shape.boundary_bytes / c.shape.boundaries.max(1);
    assert!(
        per(&runs[2]) <= per(&runs[0]) + 16,
        "{engine}: bytes per boundary grew: {} -> {}",
        per(&runs[0]),
        per(&runs[2])
    );
}

/// The snapshot probe is process-global: one generation at a time.
static RUN: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_generation_operations_are_structurally_linear() -> Result<()>
{
    let _run = RUN.lock().await;
    let mut runs = Vec::new();
    for n in SIZES {
        let (db, client) = pg_setup(&format!("struct{n}")).await?;
        for i in 0..n {
            client
                .batch_execute(&format!(
                    "CREATE TABLE st_{i:03} (id INT PRIMARY KEY); \
                     INSERT INTO st_{i:03} VALUES (1);"
                ))
                .await?;
        }
        let id = format!("struct{n}");
        client
            .batch_execute(&format!(
                "CREATE PUBLICATION pub_{id} FOR ALL TABLES"
            ))
            .await?;
        let inner = test_common::make_storage_backend().await;
        let counting = Arc::new(CountingBackend::new(inner.clone()));
        let src = sources::postgres::PostgresSource {
            id: id.clone(),
            dsn: pg_admin_dsn(&db).await.into(),
            slot: format!("slot_{id}"),
            publication: format!("pub_{id}"),
            tables: vec!["public.st_*".into()],
            tenant: "acme".into(),
            pipeline: "test".into(),
            registry: test_common::make_registry().await,
            registry_scope:
                sources::registry_scope::SharedRegistryScope::default(),
            backend: counting.clone(),
            outbox_prefixes: common::AllowList::default(),
            snapshot_cfg: cfg(),
            on_schema_drift: deltaforge_config::OnSchemaDrift::Adapt,
            table_options: Default::default(),
            rotation: None,
            snapshot_cohort: Default::default(),
        };
        runs.push(complete(&src, &id, n, &counting, &inner).await?);
        client
            .execute("SELECT pg_drop_replication_slot($1)", &[&src.slot])
            .await
            .ok();
        pg_drop_db(&db).await;
    }
    assert_structure("postgres", &runs);
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_generation_operations_are_structurally_linear() -> Result<()> {
    let _run = RUN.lock().await;
    let mut runs = Vec::new();
    for n in SIZES {
        let (db, pool, _dsn) =
            test_common::mysql_setup(&format!("struct{n}")).await?;
        let mut conn = pool.get_conn().await?;
        conn.query_drop(format!("USE {db}")).await?;
        for i in 0..n {
            conn.query_drop(format!(
                "CREATE TABLE st_{i:03} (id INT PRIMARY KEY); \
                 INSERT INTO st_{i:03} VALUES (1);"
            ))
            .await?;
        }
        conn.query_drop(format!(
            "GRANT SELECT ON {db}.* TO '{MYSQL_CDC_USER}'@'%'"
        ))
        .await?;
        let id = format!("struct{n}");
        let inner = test_common::make_storage_backend().await;
        let counting = Arc::new(CountingBackend::new(inner.clone()));
        let src = sources::mysql::MySqlSource {
            id: id.clone(),
            dsn: test_common::mysql_cdc_dsn(&db).await.into(),
            tables: vec![format!("{db}.st_*")],
            tenant: "acme".into(),
            pipeline: "test".into(),
            registry: test_common::make_registry().await,
            registry_scope:
                sources::registry_scope::SharedRegistryScope::default(),
            outbox_tables: common::AllowList::default(),
            snapshot_cfg: cfg(),
            backend: counting.clone(),
            on_schema_drift: deltaforge_config::OnSchemaDrift::Adapt,
            table_options: Default::default(),
            rotation: None,
            snapshot_cohort: Default::default(),
        };
        runs.push(complete(&src, &id, n, &counting, &inner).await?);
        test_common::mysql_drop_db(&pool, &db).await;
    }
    assert_structure("mysql", &runs);
    Ok(())
}
