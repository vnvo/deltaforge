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
    self, SnapshotProgress, progress_key, run_snapshot,
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
        identity_map: Default::default(),
    };

    run_snapshot(&snapshot_ctx, &[("public".into(), "orders".into())], Lsn::from(0u64)).await?;
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
        identity_map: Default::default(),
    };

    run_snapshot(
        &snapshot_ctx,
        &[
            ("public".into(), "users".into()),
            ("public".into(), "products".into()),
            ("public".into(), "orders".into()),
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
        done_tables: vec!["public.t1".into()],
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
        identity_map: Default::default(),
    };

    run_snapshot(
        &snapshot_ctx,
        &[
            ("public".into(), "t1".into()),
            ("public".into(), "t2".into()),
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
        identity_map: Default::default(),
    };

    run_snapshot(&snapshot_ctx, &[("public".into(), "events".into())], Lsn::from(0u64)).await?;
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
        identity_map: Default::default(),
    };

    let returned_lsn =
        run_snapshot(&snapshot_ctx, &[("public".into(), "items".into())], Lsn::from(0u64))
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
            identity_map: Default::default(),
        };

    let (tx1, mut rx1) = mpsc::channel(64);
    let lsn1 =
        run_snapshot(&make_ctx(tx1), &[("public".into(), "t".into())], Lsn::from(0u64)).await?;
    let events1 = collect_reads(&mut rx1, Duration::from_secs(5)).await;
    assert_eq!(events1.len(), 10);

    // Second call — must return the same LSN, emit zero rows.
    let (tx2, mut rx2) = mpsc::channel(64);
    let lsn2 =
        run_snapshot(&make_ctx(tx2), &[("public".into(), "t".into())], Lsn::from(0u64)).await?;
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
    let c = prepare_snapshot_slot_anchor(&dsn, slot, "test", "snap-anchor", &chkpt)
        .await
        .expect("establish anchor");

    // "during": committed AFTER C but before the snapshot export.
    for i in 1000..=1049i64 {
        client
            .execute("INSERT INTO orders (id, tag) VALUES ($1, 'during')", &[&i])
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
        identity_map: Default::default(),
    };

    let returned = run_snapshot(
        &snapshot_ctx,
        &[("public".into(), "orders".into())],
        c,
    )
    .await?;
    assert_eq!(returned.to_string(), c.to_string(), "anchor must be the returned start LSN");
    drop(snapshot_ctx);
    drop(tx);

    let s_events = collect_reads(&mut rx, Duration::from_secs(15)).await;
    let s: std::collections::HashSet<i64> = s_events
        .iter()
        .filter_map(|e| {
            let v = e.after.as_ref()?.get("id")?;
            v.as_i64().or_else(|| v.as_str().and_then(|x| x.parse().ok()))
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
