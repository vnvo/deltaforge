//! PostgreSQL snapshot integration tests.
//!
//! Run with:
//! cargo test -p sources --test postgres_snapshot_e2e -- --include-ignored --nocapture --test-threads=1

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use checkpoints::{CheckpointStore, MemCheckpointStore};
use deltaforge_config::{SnapshotCfg, SnapshotMode};
use deltaforge_core::{Event, Op};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

mod test_common;
use test_common::{pg_admin_dsn, pg_drop_db, pg_make_schema_loader, pg_setup};

use ctor::dtor;

use sources::postgres::postgres_snapshot::{
    self, SnapshotProgress, progress_key, run_snapshot,
};

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
    rx: &mut mpsc::Receiver<Event>,
    timeout: Duration,
) -> Vec<Event> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut events = Vec::new();
    loop {
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(ev)) if ev.op == Op::Read => events.push(ev),
            _ => break,
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
async fn snapshot_captures_all_rows_integer_pk() -> Result<()> {
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
    };

    run_snapshot(&snapshot_ctx, &[("public".into(), "orders".into())]).await?;

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
async fn snapshot_parallel_tables() -> Result<()> {
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
    };

    run_snapshot(
        &snapshot_ctx,
        &[
            ("public".into(), "users".into()),
            ("public".into(), "products".into()),
            ("public".into(), "orders".into()),
        ],
    )
    .await?;

    let events = collect_reads(&mut rx, Duration::from_secs(15)).await;
    assert_eq!(events.len(), 300, "100 rows × 3 tables");

    pg_drop_db(&db).await;
    Ok(())
}

/// Crash resume: pre-seed progress with t1 done, verify only t2 rows arrive.
#[tokio::test]
#[ignore = "requires docker"]
async fn snapshot_resumes_after_partial_completion() -> Result<()> {
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
    };
    chkpt
        .put_raw(&progress_key("snap-resume"), &serde_json::to_vec(&fake)?)
        .await?;

    let (tx, mut rx) = mpsc::channel(256);
    let snapshot_ctx = postgres_snapshot::PgSnapshotCtx {
        dsn: &pg_admin_dsn(&db).await,
        source_id: "snap-resume",
        pipeline: "test",
        tenant: "acme",
        cfg: &initial_cfg(),
        schema_loader: &schema_loader,
        chkpt_store: chkpt.clone(),
        tx: tx.clone(),
        cancel: CancellationToken::new(),
    };

    run_snapshot(
        &snapshot_ctx,
        &[
            ("public".into(), "t1".into()),
            ("public".into(), "t2".into()),
        ],
    )
    .await?;

    let events = collect_reads(&mut rx, Duration::from_secs(10)).await;
    assert_eq!(events.len(), 50, "only t2 rows — t1 was skipped");
    assert!(events.iter().all(|e| e.source.table == "t2"));

    pg_drop_db(&db).await;
    Ok(())
}

/// ctid fallback: UUID PK - all rows still captured.
#[tokio::test]
#[ignore = "requires docker"]
async fn snapshot_ctid_fallback_for_uuid_pk() -> Result<()> {
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
    let snapshot_ctx = postgres_snapshot::PgSnapshotCtx {
        dsn: &pg_admin_dsn(&db).await,
        source_id: "snap-ctid",
        pipeline: "test",
        tenant: "acme",
        cfg: &initial_cfg(),
        schema_loader: &schema_loader,
        chkpt_store: chkpt.clone(),
        tx: tx.clone(),
        cancel: CancellationToken::new(),
    };

    run_snapshot(&snapshot_ctx, &[("public".into(), "events".into())]).await?;

    let events = collect_reads(&mut rx, Duration::from_secs(10)).await;
    assert_eq!(events.len(), 200);

    pg_drop_db(&db).await;
    Ok(())
}

/// LSN is captured before rows are read; progress is persisted with finished=true.
#[tokio::test]
#[ignore = "requires docker"]
async fn snapshot_persists_lsn_and_marks_finished() -> Result<()> {
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
    };

    let returned_lsn =
        run_snapshot(&snapshot_ctx, &[("public".into(), "items".into())])
            .await?;

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
