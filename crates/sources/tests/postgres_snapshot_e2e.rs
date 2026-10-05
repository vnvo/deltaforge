//! PostgreSQL snapshot integration tests.
//!
//! Run with:
//! cargo test -p sources --test postgres_snapshot_e2e -- --include-ignored --nocapture --test-threads=1

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use checkpoints::{CheckpointStore, MemCheckpointStore};
use deltaforge_config::{SnapshotCfg, SnapshotMode};
use deltaforge_core::{Event, Op, SourceItem};
use pgwire_replication::Lsn;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

mod test_common;
use test_common::{pg_admin_dsn, pg_drop_db, pg_make_schema_loader, pg_setup};

use ctor::dtor;

use sources::postgres::postgres_slot_owner::prepare_snapshot_slot_anchor;
use sources::postgres::postgres_snapshot::{
    self, SnapshotProgress, plan_table, progress_key, run_snapshot,
};
use sources::snapshot_generation::PersistedLineage;

#[dtor]
fn cleanup() {
    if let Some((c, _)) = test_common::PG_CONTAINER.get() {
        std::process::Command::new("docker")
            .args(["rm", "-f", c.id()])
            .output()
            .ok();
    }
}

// ============================================================================
// Helpers
// ============================================================================

async fn collect_reads(
    rx: &mut mpsc::Receiver<SourceItem>,
    timeout: Duration,
) -> Vec<Event> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut events = Vec::new();
    loop {
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(SourceItem::Event(ev))) if ev.op == Op::Read => {
                events.push(ev)
            }
            // Skip non-Read items (e.g. table/snapshot boundaries) rather than
            // stopping - with parallel workers a boundary can interleave before
            // another table's Read events.
            Ok(Some(_)) => continue,
            // Channel closed (all snapshot senders dropped) or timed out.
            Ok(None) | Err(_) => break,
        }
    }
    events
}

fn initial_cfg() -> SnapshotCfg {
    SnapshotCfg {
        mode: SnapshotMode::Initial,
        ..Default::default()
    }
}

// ============================================================================
// Tests
// ============================================================================

/// Basic integer PK table — all rows arrive as Op::Read with correct payloads.
#[tokio::test]
#[ignore = "requires docker"]
async fn pg_snapshot_captures_all_rows_integer_pk() -> Result<()> {
    let _probe = PROBED_RUN.read().await;
    let (db, client) = pg_setup("snap_basic").await?;

    client
        .execute(
            "CREATE TABLE orders (id BIGSERIAL PRIMARY KEY, sku TEXT NOT NULL, qty INT)",
            &[],
        )
        .await?;

    for i in 1..=500i64 {
        client
            .execute(
                "INSERT INTO orders (sku, qty) VALUES ($1, $2)",
                &[&format!("sku-{i}"), &(i as i32)],
            )
            .await?;
    }

    let (tx, mut rx) = mpsc::channel(1024);
    let schema_loader = pg_make_schema_loader(&pg_admin_dsn(&db).await).await?;
    let chkpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let snapshot_ctx = postgres_snapshot::PgSnapshotCtx {
        dsn: &pg_admin_dsn(&db).await,
        source_id: "snap-basic",
        pipeline: "test-pipeline",
        tenant: "acme",
        cfg: &initial_cfg(),
        schema_loader: &schema_loader,
        chkpt_store: chkpt.clone(),
        tx: tx.clone(),
        cancel: CancellationToken::new(),
        slot_name: None,
        generation: 1,
        lineage: PersistedLineage::Postgres {
            system_identifier: 0,
        },
    };

    run_snapshot(
        &snapshot_ctx,
        &[plan_table(&schema_loader, "public", "orders", vec![]).await?],
        Lsn::from(0u64),
    )
    .await?;
    drop(snapshot_ctx);
    drop(tx);

    let events = collect_reads(&mut rx, Duration::from_secs(10)).await;
    assert_eq!(events.len(), 500);

    let first = &events[0];
    assert!(first.before.is_none());
    let after = first.after.as_ref().unwrap();
    assert!(after.get("id").is_some());
    assert!(after.get("sku").is_some());
    assert_eq!(first.source.snapshot.as_deref(), Some("true"));

    pg_drop_db(&db).await;
    Ok(())
}

/// Multiple tables snapshotted concurrently — all rows from all tables received.
#[tokio::test]
#[ignore = "requires docker"]
async fn pg_snapshot_parallel_tables() -> Result<()> {
    let _probe = PROBED_RUN.read().await;
    let (db, client) = pg_setup("snap_parallel").await?;

    for table in ["users", "products", "orders"] {
        client
            .execute(
                &format!(
                    "CREATE TABLE {table} (id BIGSERIAL PRIMARY KEY, name TEXT)"
                ),
                &[],
            )
            .await?;
        for i in 1..=100i64 {
            client
                .execute(
                    &format!("INSERT INTO {table} (name) VALUES ($1)"),
                    &[&format!("{table}-{i}")],
                )
                .await?;
        }
    }

    let (tx, mut rx) = mpsc::channel(1024);
    let schema_loader = pg_make_schema_loader(&pg_admin_dsn(&db).await).await?;
    let chkpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);

    let cfg = SnapshotCfg {
        mode: SnapshotMode::Initial,
        max_parallel_tables: 3,
        ..Default::default()
    };

    let snapshot_ctx = postgres_snapshot::PgSnapshotCtx {
        dsn: &pg_admin_dsn(&db).await,
        source_id: "snap-parallel",
        pipeline: "test",
        tenant: "acme",
        cfg: &cfg,
        schema_loader: &schema_loader,
        chkpt_store: chkpt.clone(),
        tx: tx.clone(),
        cancel: CancellationToken::new(),
        slot_name: None,
        generation: 1,
        lineage: PersistedLineage::Postgres {
            system_identifier: 0,
        },
    };

    run_snapshot(
        &snapshot_ctx,
        &[
            plan_table(&schema_loader, "public", "users", vec![]).await?,
            plan_table(&schema_loader, "public", "products", vec![]).await?,
            plan_table(&schema_loader, "public", "orders", vec![]).await?,
        ],
        Lsn::from(0u64),
    )
    .await?;
    drop(snapshot_ctx);
    drop(tx);

    let events = collect_reads(&mut rx, Duration::from_secs(15)).await;
    assert_eq!(events.len(), 300, "100 rows × 3 tables");

    pg_drop_db(&db).await;
    Ok(())
}

/// Crash resume: pre-seed progress with t1 done, verify only t2 rows arrive.
#[tokio::test]
#[ignore = "requires docker"]
async fn pg_snapshot_resumes_after_partial_completion() -> Result<()> {
    let _probe = PROBED_RUN.read().await;
    let (db, client) = pg_setup("snap_resume").await?;

    for table in ["t1", "t2"] {
        client
            .execute(
                &format!(
                    "CREATE TABLE {table} (id BIGSERIAL PRIMARY KEY, v INT)"
                ),
                &[],
            )
            .await?;
        for i in 1..=50i64 {
            client
                .execute(
                    &format!("INSERT INTO {table} (v) VALUES ($1)"),
                    &[&(i as i32)],
                )
                .await?;
        }
    }

    let chkpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let schema_loader = pg_make_schema_loader(&pg_admin_dsn(&db).await).await?;

    // Capture a real LSN to put in the fake progress.
    let (coord, conn) = tokio_postgres::connect(
        &pg_admin_dsn(&db).await,
        tokio_postgres::NoTls,
    )
    .await?;
    tokio::spawn(async move {
        conn.await.ok();
    });
    let row = coord
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await?;
    let lsn: String = row.get(0);

    let fake = SnapshotProgress {
        start_lsn: lsn,
        done_tables: ["public.t1".to_string()].into(),
        finished: false,
        anchor_version: 0,
    };
    chkpt
        .put_raw(&progress_key("snap-resume"), &serde_json::to_vec(&fake)?)
        .await?;

    let (tx, mut rx) = mpsc::channel(256);
    let dsn = pg_admin_dsn(&db).await;
    let snapshot_ctx = postgres_snapshot::PgSnapshotCtx {
        dsn: &dsn,
        source_id: "snap-resume",
        pipeline: "test",
        tenant: "acme",
        cfg: &initial_cfg(),
        schema_loader: &schema_loader,
        chkpt_store: chkpt.clone(),
        tx: tx.clone(),
        cancel: CancellationToken::new(),
        slot_name: None,
        generation: 1,
        lineage: PersistedLineage::Postgres {
            system_identifier: 0,
        },
    };

    run_snapshot(
        &snapshot_ctx,
        &[
            plan_table(&schema_loader, "public", "t1", vec![]).await?,
            plan_table(&schema_loader, "public", "t2", vec![]).await?,
        ],
        Lsn::from(0u64),
    )
    .await?;
    drop(snapshot_ctx);
    drop(tx);

    let events = collect_reads(&mut rx, Duration::from_secs(10)).await;
    assert_eq!(events.len(), 50, "only t2 rows — t1 was skipped");
    assert!(events.iter().all(|e| e.source.table == "t2"));

    pg_drop_db(&db).await;
    Ok(())
}

/// ctid fallback: UUID PK - all rows still captured.
#[tokio::test]
#[ignore = "requires docker"]
async fn pg_snapshot_ctid_fallback_for_uuid_pk() -> Result<()> {
    let _probe = PROBED_RUN.read().await;
    let (db, client) = pg_setup("snap_ctid").await?;

    client
        .execute(
            "CREATE TABLE events (id UUID DEFAULT gen_random_uuid() PRIMARY KEY, payload TEXT)",
            &[],
        )
        .await?;
    for i in 1..=200i64 {
        client
            .execute(
                "INSERT INTO events (payload) VALUES ($1)",
                &[&format!("p-{i}")],
            )
            .await?;
    }

    let (tx, mut rx) = mpsc::channel(512);
    let schema_loader = pg_make_schema_loader(&pg_admin_dsn(&db).await).await?;
    let chkpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let dsn = pg_admin_dsn(&db).await;
    let snapshot_ctx = postgres_snapshot::PgSnapshotCtx {
        dsn: &dsn,
        source_id: "snap-ctid",
        pipeline: "test",
        tenant: "acme",
        cfg: &initial_cfg(),
        schema_loader: &schema_loader,
        chkpt_store: chkpt.clone(),
        tx: tx.clone(),
        cancel: CancellationToken::new(),
        slot_name: None,
        generation: 1,
        lineage: PersistedLineage::Postgres {
            system_identifier: 0,
        },
    };

    run_snapshot(
        &snapshot_ctx,
        &[plan_table(&schema_loader, "public", "events", vec![]).await?],
        Lsn::from(0u64),
    )
    .await?;
    drop(snapshot_ctx);
    drop(tx);

    let events = collect_reads(&mut rx, Duration::from_secs(10)).await;
    assert_eq!(events.len(), 200);

    pg_drop_db(&db).await;
    Ok(())
}

/// LSN is captured before rows are read; progress is persisted with finished=true.
#[tokio::test]
#[ignore = "requires docker"]
async fn pg_snapshot_persists_lsn_and_marks_finished() -> Result<()> {
    let _probe = PROBED_RUN.read().await;
    let (db, client) = pg_setup("snap_lsn").await?;

    client
        .execute("CREATE TABLE items (id BIGSERIAL PRIMARY KEY, v TEXT)", &[])
        .await?;
    for i in 1..=50i64 {
        client
            .execute("INSERT INTO items (v) VALUES ($1)", &[&format!("v-{i}")])
            .await?;
    }

    let chkpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let schema_loader = pg_make_schema_loader(&pg_admin_dsn(&db).await).await?;
    let (tx, mut rx) = mpsc::channel(256);
    let snapshot_ctx = postgres_snapshot::PgSnapshotCtx {
        dsn: &pg_admin_dsn(&db).await,
        source_id: "snap-lsn",
        pipeline: "test",
        tenant: "acme",
        cfg: &initial_cfg(),
        schema_loader: &schema_loader,
        chkpt_store: chkpt.clone(),
        tx: tx.clone(),
        cancel: CancellationToken::new(),
        slot_name: None,
        generation: 1,
        lineage: PersistedLineage::Postgres {
            system_identifier: 0,
        },
    };

    let returned_lsn = run_snapshot(
        &snapshot_ctx,
        &[plan_table(&schema_loader, "public", "items", vec![]).await?],
        Lsn::from(0u64),
    )
    .await?;
    drop(snapshot_ctx);
    drop(tx);

    // Progress must be saved and marked finished.
    let saved = chkpt.get_raw(&progress_key("snap-lsn")).await?.unwrap();
    let progress: SnapshotProgress = serde_json::from_slice(&saved)?;
    assert!(progress.finished);
    assert_eq!(progress.start_lsn, returned_lsn.to_string());

    let events = collect_reads(&mut rx, Duration::from_secs(5)).await;
    assert_eq!(events.len(), 50);

    pg_drop_db(&db).await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker"]
async fn pg_snapshot_already_finished_returns_saved_lsn() -> Result<()> {
    let _probe = PROBED_RUN.read().await;
    let (db, client) = pg_setup("snap_idempotent").await?;
    client
        .execute("CREATE TABLE t (id BIGSERIAL PRIMARY KEY)", &[])
        .await?;
    for _i in 1..=10i64 {
        client.execute("INSERT INTO t DEFAULT VALUES", &[]).await?;
    }

    let chkpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let schema_loader = pg_make_schema_loader(&pg_admin_dsn(&db).await).await?;
    let dsn = pg_admin_dsn(&db).await;
    let cfg = initial_cfg();
    let make_ctx =
        |tx: mpsc::Sender<SourceItem>| postgres_snapshot::PgSnapshotCtx {
            dsn: &dsn,
            source_id: "snap-idempotent",
            pipeline: "test",
            tenant: "acme",
            cfg: &cfg,
            schema_loader: &schema_loader,
            chkpt_store: chkpt.clone(),
            tx,
            cancel: CancellationToken::new(),
            slot_name: None,
            generation: 1,
            lineage: PersistedLineage::Postgres {
                system_identifier: 0,
            },
        };

    let (tx1, mut rx1) = mpsc::channel(64);
    let lsn1 = run_snapshot(
        &make_ctx(tx1),
        &[plan_table(&schema_loader, "public", "t", vec![]).await?],
        Lsn::from(0u64),
    )
    .await?;
    let events1 = collect_reads(&mut rx1, Duration::from_secs(5)).await;
    assert_eq!(events1.len(), 10);

    // Second call — must return the same LSN, emit zero rows.
    let (tx2, mut rx2) = mpsc::channel(64);
    let lsn2 = run_snapshot(
        &make_ctx(tx2),
        &[plan_table(&schema_loader, "public", "t", vec![]).await?],
        Lsn::from(0u64),
    )
    .await?;
    let events2 = collect_reads(&mut rx2, Duration::from_secs(2)).await;

    assert_eq!(lsn1, lsn2, "second run must return the same saved LSN");
    assert!(events2.is_empty(), "second run must emit no rows");

    pg_drop_db(&db).await;
    Ok(())
}

/// PG-A-lite seam: the snapshot anchors at the slot's consistent point C (created
/// by prepare_snapshot_slot_anchor), so no pre-anchor row is lost, and rows
/// committed in (C, snapshot-export] appear in BOTH the snapshot and the CDC
/// range from C - a bounded at-least-once overlap (NOT exactly-once).
#[tokio::test]
#[ignore = "requires docker"]
async fn pg_snapshot_anchor_zero_loss_bounded_overlap() -> Result<()> {
    let (db, client) = pg_setup("snap_anchor").await?;
    client
        .execute("CREATE TABLE orders (id BIGINT PRIMARY KEY, tag TEXT)", &[])
        .await?;

    // Baseline: committed BEFORE the anchor.
    for i in 1..=50i64 {
        client
            .execute("INSERT INTO orders (id, tag) VALUES ($1, 'base')", &[&i])
            .await?;
    }

    let chkpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let dsn = pg_admin_dsn(&db).await;
    let slot = "anchor_ovl_slot";

    // Real production anchor: creates the slot and returns its consistent point C.
    let c =
        prepare_snapshot_slot_anchor(&dsn, slot, "test", "snap-anchor", &chkpt)
            .await
            .expect("establish anchor");

    // "during": committed AFTER C but before the snapshot export.
    for i in 1000..=1049i64 {
        client
            .execute(
                "INSERT INTO orders (id, tag) VALUES ($1, 'during')",
                &[&i],
            )
            .await?;
    }

    let (tx, mut rx) = mpsc::channel(4096);
    let schema_loader = pg_make_schema_loader(&dsn).await?;
    let snapshot_ctx = postgres_snapshot::PgSnapshotCtx {
        dsn: &dsn,
        source_id: "snap-anchor",
        pipeline: "test",
        tenant: "acme",
        cfg: &initial_cfg(),
        schema_loader: &schema_loader,
        chkpt_store: chkpt.clone(),
        tx: tx.clone(),
        cancel: CancellationToken::new(),
        slot_name: Some(slot),
        generation: 1,
        lineage: PersistedLineage::Postgres {
            system_identifier: 0,
        },
    };

    let returned = run_snapshot(
        &snapshot_ctx,
        &[plan_table(&schema_loader, "public", "orders", vec![]).await?],
        c,
    )
    .await?;
    assert_eq!(
        returned.to_string(),
        c.to_string(),
        "anchor must be the returned start LSN"
    );
    drop(snapshot_ctx);
    drop(tx);

    let s_events = collect_reads(&mut rx, Duration::from_secs(15)).await;
    let s: std::collections::HashSet<i64> = s_events
        .iter()
        .filter_map(|e| {
            let v = e.after.as_ref()?.get("id")?;
            v.as_i64()
                .or_else(|| v.as_str().and_then(|x| x.parse().ok()))
        })
        .collect();

    let a: std::collections::HashSet<i64> = client
        .query("SELECT id FROM orders", &[])
        .await?
        .iter()
        .map(|r| r.get::<_, i64>(0))
        .collect();

    // 1. No pre-anchor loss: every baseline id is in the snapshot.
    for i in 1..=50i64 {
        assert!(s.contains(&i), "baseline id {i} lost from snapshot");
    }
    // 2. No loss overall: anything committed but absent from the snapshot must have
    //    been committed after the anchor C (id >= 1000), so CDC from C delivers it.
    let missing: Vec<i64> = a.difference(&s).copied().collect();
    assert!(
        missing.iter().all(|id| *id >= 1000),
        "pre-anchor rows lost (missing from snapshot, not after C): {missing:?}"
    );
    // 3. Bounded at-least-once overlap: post-anchor rows that also appear in the
    //    snapshot. They are duplicated across snapshot + CDC - explicitly NOT
    //    exactly-once.
    let overlap = (1000..=1049i64).filter(|id| s.contains(id)).count();
    assert_eq!(
        overlap, 50,
        "expected all 50 post-anchor rows to overlap into the snapshot; got {overlap}"
    );
    eprintln!(
        "PG-A-lite: 50 baseline rows preserved (zero loss); measured at-least-once \
         overlap = {overlap} rows committed in (C, snapshot] present in both \
         snapshot and CDC range."
    );

    // cleanup: drop the slot so the database can be dropped.
    client
        .execute("SELECT pg_drop_replication_slot($1)", &[&slot])
        .await
        .ok();
    pg_drop_db(&db).await;
    Ok(())
}

// ============================================================================
// Paged discovery and preparation: structural shape
// ============================================================================

/// One snapshot start over `n` tables matching `public.pgt*` (plus tables
/// it does not match), with discovery pages of 10 and 3 parallel tables;
/// returns its structural shape.
async fn snapshot_shape(
    n: usize,
) -> Result<sources::snapshot_probe::SnapshotShape> {
    let run = snapshot_with_ddl(n, None).await?;
    let (shape, reads) = (run.shape, run.reads);
    assert_eq!(reads, n, "every matched table's row was snapshotted");
    Ok(shape)
}

/// Like [`snapshot_shape`], optionally running `ddl` (an admin statement)
/// after the first discovery page; returns the shape, the rows read and
/// the run's outcome.
async fn snapshot_with_ddl(
    n: usize,
    ddl: Option<(Hold, &str)>,
) -> Result<SnapRun> {
    snapshot_with_setup(n, "", ddl).await
}

/// The snapshot probe is process-global: a probed run holds this
/// exclusively, every other snapshot test shared, so no other run's
/// operations reach the probe while it counts.
static PROBED_RUN: tokio::sync::RwLock<()> = tokio::sync::RwLock::const_new(());

/// [`snapshot_with_ddl`] after running `setup` once the tables exist.
async fn snapshot_with_setup(
    n: usize,
    setup: &str,
    ddl: Option<(Hold, &str)>,
) -> Result<SnapRun> {
    use sources::postgres::PostgresSource;
    use sources::snapshot_probe;
    let _probe = PROBED_RUN.write().await;
    let (db, client) = pg_setup(&format!("shape{n}")).await?;
    for i in 0..n {
        client
            .batch_execute(&format!(
                "CREATE TABLE pgt_{i:03} (id INT PRIMARY KEY); \
                 INSERT INTO pgt_{i:03} VALUES (1);"
            ))
            .await?;
    }
    client
        .batch_execute(
            "CREATE TABLE other_a (id INT PRIMARY KEY); \
             CREATE TABLE zz_other (id INT PRIMARY KEY); \
             CREATE PUBLICATION pub_shape FOR ALL TABLES;",
        )
        .await?;
    client.batch_execute(setup).await?;
    let slot = format!("slot_shape{n}");
    let src = PostgresSource {
        id: format!("shape{n}"),
        dsn: pg_admin_dsn(&db).await.into(),
        slot: slot.clone(),
        publication: "pub_shape".into(),
        tables: vec!["public.pgt*".into()],
        tenant: "acme".into(),
        pipeline: "test".into(),
        registry: test_common::make_registry().await,
        registry_scope: sources::registry_scope::SharedRegistryScope::default(),
        backend: test_common::make_storage_backend().await,
        outbox_prefixes: common::AllowList::default(),
        snapshot_cfg: SnapshotCfg {
            max_parallel_tables: 3,
            discovery_page_size: 10,
            ..initial_cfg()
        },
        on_schema_drift: deltaforge_config::OnSchemaDrift::Adapt,
        table_options: Default::default(),
        rotation: None,
    };
    let chkpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let backend = src.backend.clone();
    let (tx, mut rx) = mpsc::channel(4096);
    snapshot_probe::reset();
    let hold = ddl.map(|(when, _)| match when {
        Hold::FirstPage => snapshot_probe::hold_after_discovery_page(),
        Hold::BeforeAnchor => snapshot_probe::hold_before_anchor(),
    });
    let handle = deltaforge_core::Source::run(&src, tx, chkpt.clone()).await;
    if let (Some((reached, release)), Some((_, ddl))) = (&hold, ddl) {
        reached.notified().await;
        client.batch_execute(ddl).await?;
        release.notify_one();
    }
    let mut reads = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    while reads < n {
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(SourceItem::Event(ev))) if ev.op == Op::Read => reads += 1,
            Ok(Some(_)) => continue,
            _ => break,
        }
    }
    let shape = snapshot_probe::shape();
    handle.stop();
    // Stopping a running source ends it with `Cancelled`: not a failure.
    let err = handle
        .join()
        .await
        .err()
        .map(|e| format!("{e:#}"))
        .filter(|e| e != "operation cancelled");
    let finished = chkpt
        .get_raw(&progress_key(&format!("shape{n}")))
        .await?
        .map(|b| serde_json::from_slice::<serde_json::Value>(&b).unwrap())
        .is_some_and(|v| v["finished"] == true);
    let generation_status =
        generation_status(&backend, &format!("shape{n}")).await;
    client
        .execute("SELECT pg_drop_replication_slot($1)", &[&slot])
        .await
        .ok();
    pg_drop_db(&db).await;
    Ok(SnapRun {
        shape,
        reads,
        err,
        finished,
        generation_status,
    })
}

/// Where a test changes the catalog during a snapshot start.
#[derive(Clone, Copy)]
enum Hold {
    /// After the first discovery page, before its tables are prepared.
    FirstPage,
    /// After preparation, right before the snapshot anchor.
    BeforeAnchor,
}

struct SnapRun {
    shape: sources::snapshot_probe::SnapshotShape,
    reads: usize,
    err: Option<String>,
    /// The snapshot progress records the snapshot as finished.
    finished: bool,
    /// The generation record's status (`None`: no record).
    generation_status: Option<String>,
}

async fn generation_status(
    backend: &storage::ArcStorageBackend,
    source_id: &str,
) -> Option<String> {
    use checkpoints::SnapshotStateStore;
    let store = storage::adapters::BackendCheckpointStore::new(backend.clone());
    store
        .get_versioned(&format!("snapshot_generation:{source_id}"))
        .await
        .ok()
        .flatten()
        .map(|(_, b)| {
            serde_json::from_slice::<serde_json::Value>(&b).unwrap()["status"]
                .as_str()
                .unwrap_or_default()
                .to_string()
        })
}

/// Discovery reads the catalog in pages and each table is prepared once:
/// catalog queries are `floor(selected rows / page size) + 1`, every matched
/// table is discovered and prepared exactly once, a page never exceeds the
/// page size, workers make no live catalog schema fetch, and at most
/// `max_parallel_tables` table tasks are alive. Doubling the tables changes
/// only the per-page and per-table counters.
#[tokio::test]
#[ignore = "requires docker"]
async fn discovery_is_paged_and_each_table_prepared_once() -> Result<()> {
    let s25 = snapshot_shape(25).await?;
    let s50 = snapshot_shape(50).await?;
    for (n, s) in [(25u64, s25), (50, s50)] {
        assert_eq!(s.discovery_queries, n / 10 + 1, "pages for {n}: {s:?}");
        assert_eq!(s.discovered_tables, n, "{s:?}");
        assert_eq!(s.prepared_tables, n, "{s:?}");
        assert_eq!(s.max_discovery_page, 10, "{s:?}");
        assert_eq!(s.worker_live_fetches, 0, "{s:?}");
        assert_eq!(s.max_table_tasks, 3, "{s:?}");
    }
    // Fixed operations: identical, once each (PostgreSQL verifies the plan
    // in the exported row snapshot, page by page).
    assert_eq!(s25.fixed, s50.fixed);
    assert_eq!(s25.fixed, [1, 1, 1, 1, 1, 0, 1], "{s25:?}");
    assert_eq!((s25.verification_queries, s50.verification_queries), (3, 5));
    assert_eq!(s25.registry_reads, 25, "one registry read per table");
    assert_eq!(s25.frontier_tables, 25);
    // Doubling: only the per-page and per-table counters move.
    assert_eq!(s50.discovery_queries - s25.discovery_queries, 3);
    assert_eq!(s50.prepared_tables, 2 * s25.prepared_tables);
    assert_eq!(
        (
            s25.max_discovery_page,
            s25.worker_live_fetches,
            s25.max_table_tasks
        ),
        (
            s50.max_discovery_page,
            s50.worker_live_fetches,
            s50.max_table_tasks
        )
    );
    Ok(())
}

/// Interrupted legacy progress under a wildcard pattern is recognized: the
/// check compares the tables the patterns expand to (not the pattern
/// strings) with the completed tables.
#[tokio::test]
#[ignore = "requires docker"]
async fn wildcard_legacy_progress_ambiguity_is_detected() -> Result<()> {
    let _probe = PROBED_RUN.read().await;
    use deltaforge_core::Source;
    use sources::postgres::PostgresSource;
    let (db, client) = pg_setup("ambig").await?;
    client
        .batch_execute(
            "CREATE TABLE amb_a (id INT PRIMARY KEY); \
             CREATE TABLE amb_b (id INT PRIMARY KEY);",
        )
        .await?;
    let src = PostgresSource {
        id: "ambig".into(),
        dsn: pg_admin_dsn(&db).await.into(),
        slot: "slot_ambig".into(),
        publication: "pub_ambig".into(),
        tables: vec!["public.amb*".into()],
        tenant: "acme".into(),
        pipeline: "test".into(),
        registry: test_common::make_registry().await,
        registry_scope: sources::registry_scope::SharedRegistryScope::default(),
        backend: test_common::make_storage_backend().await,
        outbox_prefixes: common::AllowList::default(),
        snapshot_cfg: initial_cfg(),
        on_schema_drift: deltaforge_config::OnSchemaDrift::Adapt,
        table_options: Default::default(),
        rotation: None,
    };
    let store = MemCheckpointStore::new()?;
    let put = |done: &[&str], finished: bool| {
        serde_json::to_vec(&serde_json::json!({
            "start_lsn": "0/1", "done_tables": done, "finished": finished
        }))
        .unwrap()
    };
    store
        .put_raw(&progress_key("ambig"), &put(&["public.amb_a"], false))
        .await?;
    assert!(
        src.check_durable_snapshot_startup(&store).await.is_err(),
        "one of the expanded tables done, one pending"
    );
    for (done, finished) in [
        (&["public.amb_a", "public.amb_b"][..], false),
        (&["public.amb_a"][..], true),
        (&[][..], false),
    ] {
        store
            .put_raw(&progress_key("ambig"), &put(done, finished))
            .await?;
        assert!(
            src.check_durable_snapshot_startup(&store).await.is_ok(),
            "{done:?} finished={finished}"
        );
    }
    pg_drop_db(&db).await;
    Ok(())
}

/// Discovery and preparation read one catalog snapshot, and the rows are
/// read only if the exported snapshot still shows the prepared plan:
/// - a table created after the first page (sorting into a later page) is
///   not discovered, so exactly the tables that existed together are copied;
/// - a table dropped after the first page is still in the preparation
///   snapshot, is gone in the row snapshot, and the snapshot stops before any
///   row;
/// - a discovered table's columns altered after the first page: the plan is
///   the pre-change view, the row snapshot shows the change, and the
///   snapshot stops before any row, never finished; likewise for a table
///   altered after preparation, right before the anchor.
#[tokio::test]
#[ignore = "requires docker"]
async fn discovery_pages_share_one_catalog_snapshot() -> Result<()> {
    let run = snapshot_with_ddl(
        25,
        Some((
            Hold::FirstPage,
            "CREATE TABLE pgt_999 (id INT PRIMARY KEY); INSERT INTO pgt_999 VALUES (1);",
        )),
    )
    .await?;
    assert_eq!(run.shape.discovered_tables, 25, "{:?}", run.shape);
    assert_eq!(
        run.reads, 25,
        "the table created mid-discovery is not copied"
    );
    assert!(run.err.is_none() && run.finished);

    let run =
        snapshot_with_ddl(25, Some((Hold::FirstPage, "DROP TABLE pgt_020")))
            .await?;
    let err = run.err.expect("the snapshot stops");
    assert!(
        err.contains("pgt_020") && err.contains("no longer exists"),
        "{err}"
    );
    assert_eq!(run.reads, 0, "before any row");
    assert!(!run.finished);

    let run = snapshot_with_ddl(
        25,
        Some((Hold::FirstPage, "ALTER TABLE pgt_005 ADD COLUMN extra TEXT")),
    )
    .await?;
    let err = run.err.expect("the snapshot stops");
    assert!(
        err.contains("pgt_005") && err.contains("schema changed"),
        "{err}"
    );
    assert_eq!(run.reads, 0, "before any row");
    assert!(!run.finished, "never finished");
    assert_ne!(run.generation_status.as_deref(), Some("completed"));

    // Altered after preparation, before the exported snapshot: the same.
    let run = snapshot_with_ddl(
        25,
        Some((
            Hold::BeforeAnchor,
            "ALTER TABLE pgt_012 ADD COLUMN extra TEXT",
        )),
    )
    .await?;
    let err = run.err.expect("the snapshot stops");
    assert!(
        err.contains("pgt_012") && err.contains("schema changed"),
        "{err}"
    );
    assert_eq!(run.reads, 0, "before any row");
    assert!(!run.finished);
    Ok(())
}

/// The anchor compares the whole registered schema, not a reduced shape: a
/// change that keeps every column's name, type OID and nullability (a type
/// modifier or time precision, an identity property, the replica identity)
/// still stops the snapshot before any row, unfinished and never completed.
#[tokio::test]
#[ignore = "requires docker"]
async fn the_anchor_compares_the_registered_schema() -> Result<()> {
    let setup = "ALTER TABLE pgt_012 ADD COLUMN v VARCHAR(20), ADD COLUMN n INT, \
                 ADD COLUMN at TIMESTAMP(3); \
                 UPDATE pgt_012 SET n = 1; \
                 ALTER TABLE pgt_012 ALTER COLUMN n SET NOT NULL;";
    for (what, ddl) in [
        (
            "type modifier",
            "ALTER TABLE pgt_012 ALTER COLUMN v TYPE VARCHAR(40)",
        ),
        (
            "time precision",
            "ALTER TABLE pgt_012 ALTER COLUMN at TYPE TIMESTAMP(6)",
        ),
        (
            "identity",
            "ALTER TABLE pgt_012 ALTER COLUMN n ADD GENERATED ALWAYS AS IDENTITY",
        ),
        (
            "replica identity",
            "ALTER TABLE pgt_012 REPLICA IDENTITY FULL",
        ),
    ] {
        let run =
            snapshot_with_setup(25, setup, Some((Hold::BeforeAnchor, ddl)))
                .await?;
        let err = run.err.unwrap_or_else(|| panic!("{what}: not stopped"));
        assert!(
            err.contains("pgt_012") && err.contains("schema changed"),
            "{what}: {err}"
        );
        assert_eq!(run.reads, 0, "{what}: before any row");
        assert!(!run.finished, "{what}: never finished");
        assert_ne!(
            run.generation_status.as_deref(),
            Some("completed"),
            "{what}"
        );
    }
    Ok(())
}

/// Upgrade: a snapshot generation recorded by an earlier release (no
/// fingerprint format) whose snapshot never completed is not resumed - its
/// fingerprint cannot be compared - and is not refused as a configuration
/// change: the snapshot runs again as the next generation.
#[tokio::test]
#[ignore = "requires docker"]
async fn an_earlier_format_generation_restarts_as_the_next_one() -> Result<()> {
    let _probe = PROBED_RUN.read().await;
    use checkpoints::SnapshotStateStore;
    use sources::postgres::PostgresSource;
    let (db, client) = pg_setup("fpupgrade").await?;
    client
        .batch_execute(
            "CREATE TABLE up_a (id INT PRIMARY KEY); INSERT INTO up_a VALUES (1), (2); \
             CREATE PUBLICATION pub_up FOR ALL TABLES;",
        )
        .await?;
    let backend = test_common::make_storage_backend().await;
    // The source's own lineage: an earlier-format record is replaced only
    // within it.
    let sysid: i64 = client
        .query_one("SELECT system_identifier FROM pg_control_system()", &[])
        .await?
        .get(0);
    let legacy = serde_json::json!({
        "generation": 3,
        "lineage": PersistedLineage::Postgres { system_identifier: sysid as u64 },
        "status": "running",
        "config_fingerprint": "a-format-1-fingerprint",
    });
    let store = storage::adapters::BackendCheckpointStore::new(backend.clone());
    store
        .compare_and_swap(
            "snapshot_generation:fpupgrade",
            None,
            &serde_json::to_vec(&legacy)?,
        )
        .await?;
    let src = PostgresSource {
        id: "fpupgrade".into(),
        dsn: pg_admin_dsn(&db).await.into(),
        slot: "slot_fpupgrade".into(),
        publication: "pub_up".into(),
        tables: vec!["public.up_*".into()],
        tenant: "acme".into(),
        pipeline: "test".into(),
        registry: test_common::make_registry().await,
        registry_scope: sources::registry_scope::SharedRegistryScope::default(),
        backend,
        outbox_prefixes: common::AllowList::default(),
        snapshot_cfg: initial_cfg(),
        on_schema_drift: deltaforge_config::OnSchemaDrift::Adapt,
        table_options: Default::default(),
        rotation: None,
    };
    let chkpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let (tx, mut rx) = mpsc::channel(256);
    let handle = deltaforge_core::Source::run(&src, tx, chkpt).await;
    let mut reads = Vec::new();
    while reads.len() < 2 {
        match tokio::time::timeout(Duration::from_secs(60), rx.recv()).await {
            Ok(Some(SourceItem::Event(ev))) if ev.op == Op::Read => {
                reads.push(ev)
            }
            Ok(Some(_)) => continue,
            _ => break,
        }
    }
    handle.stop();
    // Stopping a running source ends it with `Cancelled`: not a failure.
    let err = handle
        .join()
        .await
        .err()
        .map(|e| format!("{e:#}"))
        .filter(|e| e != "operation cancelled");
    assert!(err.is_none(), "{err:?}");
    assert_eq!(reads.len(), 2, "the snapshot ran");
    assert!(
        reads
            .iter()
            .all(|e| e.source.position.snapshot_generation == Some(4)),
        "the next generation"
    );
    client
        .execute("SELECT pg_drop_replication_slot('slot_fpupgrade')", &[])
        .await
        .ok();
    pg_drop_db(&db).await;
    Ok(())
}
