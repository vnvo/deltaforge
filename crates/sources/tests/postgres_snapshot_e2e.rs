//! PostgreSQL snapshot integration tests.
//!
//! Run with:
//! cargo test -p sources --test postgres_snapshot_e2e -- --include-ignored --nocapture --test-threads=1

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use checkpoints::{CheckpointStore, MemCheckpointStore};
use deltaforge_config::{SnapshotCfg, SnapshotMode};
use deltaforge_core::{Event, Op, Source, SourceItem};

mod test_common;
use test_common::{
    TEST_SINK, acked_channel, pg_admin_dsn, pg_drop_db, pg_setup,
    snapshot_state,
};

use ctor::dtor;

use sources::postgres::PostgresSource;
use sources::postgres::postgres_snapshot::progress_key;
use sources::snapshot_generation::PersistedLineage;
use sources::snapshot_queue::{QueueStore, Stored};

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

fn initial_cfg() -> SnapshotCfg {
    SnapshotCfg {
        mode: SnapshotMode::Initial,
        ..Default::default()
    }
}

/// A source over every table of `db` matching `tables`, publishing all of
/// them (the publication is created here).
async fn pg_source(
    db: &str,
    client: &tokio_postgres::Client,
    id: &str,
    tables: &[&str],
    cfg: SnapshotCfg,
    backend: storage::ArcStorageBackend,
) -> Result<PostgresSource> {
    client
        .batch_execute(&format!("CREATE PUBLICATION pub_{id} FOR ALL TABLES"))
        .await?;
    Ok(PostgresSource {
        id: id.into(),
        dsn: pg_admin_dsn(db).await.into(),
        slot: format!("slot_{id}"),
        publication: format!("pub_{id}"),
        tables: tables.iter().map(|t| t.to_string()).collect(),
        tenant: "acme".into(),
        pipeline: "test".into(),
        registry: test_common::make_registry().await,
        registry_scope: sources::registry_scope::SharedRegistryScope::default(),
        backend,
        outbox_prefixes: common::AllowList::default(),
        snapshot_cfg: cfg,
        on_schema_drift: deltaforge_config::OnSchemaDrift::Adapt,
        table_options: Default::default(),
        rotation: None,
        snapshot_cohort: Default::default(),
    })
}

/// One run of `src`: snapshot rows until `want` arrived (or the source
/// ends), then, when `complete`, until the generation is completed; then
/// stop. Returns the rows and the run's error (a stop is none).
async fn run_until(
    src: &PostgresSource,
    ckpt: &Arc<dyn CheckpointStore>,
    want: usize,
    complete: bool,
) -> Result<(Vec<Event>, Option<String>)> {
    let (tx, mut rx) = acked_channel(src, ckpt, &src.id, 4096);
    let handle = src.run(tx, Arc::clone(ckpt)).await;
    let mut reads = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    while reads.len() < want {
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(SourceItem::Event(ev))) if ev.op == Op::Read => {
                reads.push(ev)
            }
            Ok(Some(_)) => continue,
            _ => break,
        }
    }
    if complete {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        while snapshot_state(&src.backend, &src.id).await.map(|s| s.0)
            != Some("completed".into())
            && tokio::time::Instant::now() < deadline
            && !handle.join.is_finished()
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
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
    Ok((reads, err))
}

async fn drop_slot(client: &tokio_postgres::Client, src: &PostgresSource) {
    client
        .execute("SELECT pg_drop_replication_slot($1)", &[&src.slot])
        .await
        .ok();
}

fn ids(events: &[Event]) -> std::collections::HashSet<i64> {
    events
        .iter()
        .filter_map(|e| {
            let v = e.after.as_ref()?.get("id")?;
            v.as_i64()
                .or_else(|| v.as_str().and_then(|x| x.parse().ok()))
        })
        .collect()
}

// ============================================================================
// Tests
// ============================================================================

/// Basic integer PK table — all rows arrive as Op::Read with correct
/// payloads, and the generation completes once the sink acknowledged its
/// terminal barrier: the sink's checkpoint is the completing position at
/// exactly the recorded anchor and its continuity stamp.
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

    let backend = test_common::make_storage_backend().await;
    let src = pg_source(
        &db,
        &client,
        "snap_basic",
        &["public.orders"],
        initial_cfg(),
        backend.clone(),
    )
    .await?;
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let (events, err) = run_until(&src, &ckpt, 500, true).await?;
    assert!(err.is_none(), "{err:?}");
    assert_eq!(events.len(), 500);

    let first = &events[0];
    assert!(first.before.is_none());
    let after = first.after.as_ref().unwrap();
    assert!(after.get("id").is_some());
    assert!(after.get("sku").is_some());
    assert_eq!(first.source.snapshot.as_deref(), Some("true"));
    assert!(
        events
            .iter()
            .all(|e| e.source.position.snapshot_generation == Some(1))
    );

    // Completed over the frozen cohort, with the acknowledging sink.
    let Some(Stored::Current { control, .. }) =
        QueueStore::new(backend, &src.id).read().await?
    else {
        panic!("a control record");
    };
    assert_eq!(
        serde_json::to_value(control.state)?,
        serde_json::json!("completed")
    );
    let completion = control.completion.clone().expect("completion");
    assert_eq!(completion.acks, [TEST_SINK]);
    let anchor = match control.anchor.clone().expect("anchor") {
        sources::snapshot_queue::EngineAnchor::Postgres {
            lsn,
            timeline,
            chain,
            transition,
        } => {
            assert!(
                timeline.is_some() && chain.is_some() && transition.is_some(),
                "the anchor carries its continuity stamp"
            );
            (lsn, timeline, chain, transition)
        }
        other => panic!("{other:?}"),
    };
    let sink: serde_json::Value = serde_json::from_slice(
        &ckpt
            .get_raw(&format!("{}::sink::{TEST_SINK}", src.id))
            .await?
            .expect("the sink's checkpoint"),
    )?;
    assert_eq!(sink["lsn"], anchor.0.as_str());
    assert_eq!(sink["timeline"], serde_json::json!(anchor.1));
    assert_eq!(sink["chain"], serde_json::json!(anchor.2));
    assert_eq!(sink["transition"], serde_json::json!(anchor.3));
    assert_eq!(sink["snapshot_completed"], 1);
    assert_eq!(sink["snapshot_chain"], control.snapshot_chain.as_str());

    drop_slot(&client, &src).await;
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

    let src = pg_source(
        &db,
        &client,
        "snap_parallel",
        &["public.*"],
        SnapshotCfg {
            mode: SnapshotMode::Initial,
            max_parallel_tables: 3,
            ..Default::default()
        },
        test_common::make_storage_backend().await,
    )
    .await?;
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let (events, err) = run_until(&src, &ckpt, 300, true).await?;
    assert!(err.is_none(), "{err:?}");
    assert_eq!(events.len(), 300, "100 rows × 3 tables");

    drop_slot(&client, &src).await;
    pg_drop_db(&db).await;
    Ok(())
}

/// A generation lives and dies with its read view: an interrupted
/// generation is never resumed. The next start replaces it with the next
/// generation of the same chain, which copies every table again.
#[tokio::test]
#[ignore = "requires docker"]
async fn an_interrupted_generation_is_replaced_and_copied_in_full() -> Result<()>
{
    let _probe = PROBED_RUN.read().await;
    let (db, client) = pg_setup("snap_replace").await?;

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

    let backend = test_common::make_storage_backend().await;
    let cfg = SnapshotCfg {
        mode: SnapshotMode::Initial,
        max_parallel_tables: 1,
        chunk_size: 10,
        ..Default::default()
    };
    let src = pg_source(
        &db,
        &client,
        "snap_replace",
        &["public.t*"],
        cfg,
        backend.clone(),
    )
    .await?;
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);

    // Interrupted after some rows of generation 1.
    let (first, err) = run_until(&src, &ckpt, 20, false).await?;
    assert!(err.is_none(), "{err:?}");
    assert!(first.len() >= 20);
    let interrupted = snapshot_state(&backend, &src.id).await;
    assert_ne!(
        interrupted.as_ref().map(|s| s.0.as_str()),
        Some("completed"),
        "{interrupted:?}"
    );

    // The next start: generation 2, every row again, then completed.
    let (second, err) = run_until(&src, &ckpt, 100, true).await?;
    assert!(err.is_none(), "{err:?}");
    assert_eq!(second.len(), 100, "both tables in full");
    assert!(
        second
            .iter()
            .all(|e| e.source.position.snapshot_generation == Some(2)),
        "the replacing generation"
    );
    assert_eq!(
        snapshot_state(&backend, &src.id).await,
        Some(("completed".into(), 2))
    );

    drop_slot(&client, &src).await;
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

    let src = pg_source(
        &db,
        &client,
        "snap_ctid",
        &["public.events"],
        initial_cfg(),
        test_common::make_storage_backend().await,
    )
    .await?;
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let (events, err) = run_until(&src, &ckpt, 200, true).await?;
    assert!(err.is_none(), "{err:?}");
    assert_eq!(events.len(), 200);

    drop_slot(&client, &src).await;
    pg_drop_db(&db).await;
    Ok(())
}

/// After a completed generation a restart streams: no row is copied again,
/// and a change made since arrives through the stream.
#[tokio::test]
#[ignore = "requires docker"]
async fn a_restart_after_completion_streams_without_copying() -> Result<()> {
    let _probe = PROBED_RUN.read().await;
    let (db, client) = pg_setup("snap_restart").await?;
    client
        .execute("CREATE TABLE t (id BIGSERIAL PRIMARY KEY, v INT)", &[])
        .await?;
    for _ in 1..=10i64 {
        client.execute("INSERT INTO t (v) VALUES (1)", &[]).await?;
    }

    let backend = test_common::make_storage_backend().await;
    let src = pg_source(
        &db,
        &client,
        "snap_restart",
        &["public.t"],
        initial_cfg(),
        backend.clone(),
    )
    .await?;
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let (events, err) = run_until(&src, &ckpt, 10, true).await?;
    assert!(err.is_none(), "{err:?}");
    assert_eq!(events.len(), 10);
    assert_eq!(
        snapshot_state(&backend, &src.id).await,
        Some(("completed".into(), 1))
    );

    let (tx, mut rx) = acked_channel(&src, &ckpt, &src.id, 256);
    let handle = src.run(tx, Arc::clone(&ckpt)).await;
    tokio::time::timeout(Duration::from_secs(30), handle.ready.wait())
        .await
        .expect("streaming");
    client.execute("INSERT INTO t (v) VALUES (2)", &[]).await?;
    let mut changes = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while changes.is_empty() {
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(SourceItem::Event(ev))) => {
                assert_ne!(ev.op, Op::Read, "no row is copied again");
                changes.push(ev);
            }
            Ok(Some(_)) => continue,
            _ => break,
        }
    }
    handle.stop();
    let _ = handle.join().await;
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].op, Op::Create);
    assert_eq!(
        snapshot_state(&backend, &src.id).await,
        Some(("completed".into(), 1)),
        "no new generation"
    );

    drop_slot(&client, &src).await;
    pg_drop_db(&db).await;
    Ok(())
}

/// PG-A-lite seam: the generation anchors at the slot's consistent point C,
/// so no pre-anchor row is lost, and rows committed in (C, snapshot-export]
/// appear in BOTH the snapshot and the stream from C - a bounded
/// at-least-once overlap (NOT exactly-once).
#[tokio::test]
#[ignore = "requires docker"]
async fn pg_snapshot_anchor_zero_loss_bounded_overlap() -> Result<()> {
    use sources::snapshot_probe;
    let _probe = PROBED_RUN.write().await;
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

    let backend = test_common::make_storage_backend().await;
    let src = pg_source(
        &db,
        &client,
        "snap_anchor",
        &["public.orders"],
        initial_cfg(),
        backend.clone(),
    )
    .await?;
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    snapshot_probe::reset();
    let (reached, release) = snapshot_probe::hold_before_anchor();
    let (tx, mut rx) = acked_channel(&src, &ckpt, &src.id, 4096);
    let handle = src.run(tx, Arc::clone(&ckpt)).await;
    // The slot anchor C is taken; the read view is not exported yet.
    reached.notified().await;
    for i in 1000..=1049i64 {
        client
            .execute(
                "INSERT INTO orders (id, tag) VALUES ($1, 'during')",
                &[&i],
            )
            .await?;
    }
    release.notify_one();
    let mut s_events = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while s_events.len() < 100 {
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(SourceItem::Event(ev))) if ev.op == Op::Read => {
                s_events.push(ev)
            }
            Ok(Some(_)) => continue,
            _ => break,
        }
    }
    handle.stop();
    let _ = handle.join().await;
    let s = ids(&s_events);

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

    drop_slot(&client, &src).await;
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
        snapshot_cohort: Default::default(),
    };
    let chkpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let backend = src.backend.clone();
    let (tx, mut rx) = acked_channel(&src, &chkpt, &src.id, 4096);
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
    let generation_status = snapshot_state(&backend, &format!("shape{n}"))
        .await
        .map(|(state, _)| state);
    let finished = matches!(
        generation_status.as_deref(),
        Some("rows_produced" | "completed")
    );
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
    /// Every row was produced (the control record is `rows_produced` or
    /// `completed`).
    finished: bool,
    /// The control record's state (`None`: no record).
    generation_status: Option<String>,
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
    // O(1) boundaries: no per-table frontier is held.
    assert_eq!(s25.frontier_tables, 0);
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
        snapshot_cohort: Default::default(),
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

/// The state an earlier release leaves after a completed snapshot of
/// generation 3: its owned slot (the anchor is the slot's consistent
/// point), its progress record (`anchor_version`), a pre-queue control
/// record, and a sink checkpoint strictly past the anchor. Returns the
/// source and its stores.
async fn legacy_completed(
    db: &str,
    client: &tokio_postgres::Client,
    id: &str,
    anchor_version: u32,
) -> Result<(PostgresSource, Arc<dyn CheckpointStore>)> {
    use checkpoints::SnapshotStateStore;
    client
        .batch_execute(
            "CREATE TABLE up_a (id INT PRIMARY KEY); \
             INSERT INTO up_a VALUES (1), (2);",
        )
        .await?;
    let backend = test_common::make_storage_backend().await;
    let src =
        pg_source(db, client, id, &["public.up_*"], initial_cfg(), backend)
            .await?;
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let anchor =
        sources::postgres::postgres_slot_owner::prepare_snapshot_slot_anchor(
            &pg_admin_dsn(db).await,
            &src.slot,
            &src.pipeline,
            id,
            &ckpt,
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let progress = serde_json::json!({
        "start_lsn": anchor.to_string(),
        "done_tables": ["public.up_a"],
        "finished": true,
        "anchor_version": anchor_version,
    });
    ckpt.put_raw(&progress_key(id), &serde_json::to_vec(&progress)?)
        .await?;
    let sysid: i64 = client
        .query_one("SELECT system_identifier FROM pg_control_system()", &[])
        .await?
        .get(0);
    let legacy = serde_json::json!({
        "generation": 3,
        "lineage": PersistedLineage::Postgres { system_identifier: sysid as u64 },
        "status": "completed",
        "config_fingerprint": "a-format-2-fingerprint",
        "fingerprint_format": 2,
    });
    storage::adapters::BackendCheckpointStore::new(src.backend.clone())
        .compare_and_swap(
            &format!("snapshot_generation:{id}"),
            None,
            &serde_json::to_vec(&legacy)?,
        )
        .await?;
    // The sink committed past the anchor: a change after it.
    client.execute("INSERT INTO up_a VALUES (3)", &[]).await?;
    let after: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await?
        .get(0);
    ckpt.put_raw(
        &format!("{id}::sink::{TEST_SINK}"),
        format!(r#"{{"lsn":"{after}","tx_id":null}}"#).as_bytes(),
    )
    .await?;
    Ok((src, ckpt))
}

/// Upgrade: a snapshot an earlier release took under the pre-hardening
/// anchor (anchor version 0) is never proven, even though it looks
/// completed and its sink committed past the anchor: rows committed at the
/// snapshot-to-stream seam may be missing. It is copied once more, as the
/// next generation (generation 4) of a new chain.
#[tokio::test]
#[ignore = "requires docker"]
async fn a_version_0_legacy_snapshot_is_copied_once_more() -> Result<()> {
    let _probe = PROBED_RUN.read().await;
    let (db, client) = pg_setup("legacy_v0").await?;
    let (src, ckpt) = legacy_completed(&db, &client, "legacy_v0", 0).await?;
    let (reads, err) = run_until(&src, &ckpt, 3, true).await?;
    assert!(err.is_none(), "{err:?}");
    assert_eq!(reads.len(), 3, "every row copied again");
    assert!(
        reads
            .iter()
            .all(|e| e.source.position.snapshot_generation == Some(4)),
        "the next generation"
    );
    assert_eq!(
        snapshot_state(&src.backend, &src.id).await,
        Some(("completed".into(), 4))
    );
    assert!(
        ckpt.get_raw(&progress_key(&src.id)).await?.is_none(),
        "the legacy progress is retired"
    );
    drop_slot(&client, &src).await;
    pg_drop_db(&db).await;
    Ok(())
}

/// Upgrade: the same state under the hardened anchor (anchor version 1),
/// with the sink checkpoint proving completion over the policy, is upgraded
/// in place to a completed generation 3 of a new chain: nothing is copied
/// again and the stream continues from the anchor.
#[tokio::test]
#[ignore = "requires docker"]
async fn a_proven_hardened_legacy_snapshot_is_upgraded_in_place() -> Result<()>
{
    let _probe = PROBED_RUN.read().await;
    let (db, client) = pg_setup("legacy_v1").await?;
    let (src, ckpt) = legacy_completed(&db, &client, "legacy_v1", 1).await?;
    let (tx, mut rx) = acked_channel(&src, &ckpt, &src.id, 256);
    let handle = src.run(tx, Arc::clone(&ckpt)).await;
    tokio::time::timeout(Duration::from_secs(60), handle.ready.wait())
        .await
        .expect("streaming");
    client.execute("INSERT INTO up_a VALUES (4)", &[]).await?;
    let mut events = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while !events
        .iter()
        .any(|e: &Event| ids(std::slice::from_ref(e)).contains(&4))
    {
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(SourceItem::Event(ev))) => events.push(ev),
            Ok(Some(_)) => continue,
            _ => break,
        }
    }
    handle.stop();
    let _ = handle.join().await;
    assert!(
        events.iter().all(|e| e.op != Op::Read),
        "nothing copied again: {events:?}"
    );
    assert!(ids(&events).contains(&4), "the stream continues");
    let Some(Stored::Current { control, .. }) =
        QueueStore::new(src.backend.clone(), &src.id).read().await?
    else {
        panic!("upgraded")
    };
    assert_eq!(
        serde_json::to_value(control.state)?,
        serde_json::json!("completed")
    );
    assert_eq!((control.generation, control.legacy_through), (3, Some(3)));
    assert!(
        ckpt.get_raw(&progress_key(&src.id)).await?.is_none(),
        "the legacy progress is retired"
    );
    drop_slot(&client, &src).await;
    pg_drop_db(&db).await;
    Ok(())
}
