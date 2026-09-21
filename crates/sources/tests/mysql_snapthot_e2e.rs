//! MySQL snapshot e2e tests. Run with:
//! `cargo test -p sources --test mysql_snapshot_e2e -- --include-ignored --nocapture --test-threads=1`

use anyhow::Result;
use checkpoints::{CheckpointStore, MemCheckpointStore};
use common::AllowList;
use ctor::dtor;
use deltaforge_config::{SnapshotCfg, SnapshotMode};
use deltaforge_core::{Event, Op, Source, SourceItem};
use mysql_async::prelude::Queryable;
use sources::mysql::MySqlSource;
use std::sync::Arc;
use std::time::Instant;
use tokio::{
    sync::mpsc,
    time::{Duration, sleep, timeout},
};
use tracing::info;

mod test_common;
use test_common::{
    MYSQL_CDC_USER, MYSQL_CONTAINER, make_registry, mysql_cdc_dsn,
    mysql_drop_db, mysql_setup,
};

use crate::test_common::make_storage_backend;

#[dtor]
fn cleanup() {
    if let Some((c, _)) = MYSQL_CONTAINER.get() {
        std::process::Command::new("docker")
            .args(["rm", "-f", c.id()])
            .output()
            .ok();
    }
}

// ============================================================================
// Helpers
// ============================================================================

async fn make_source(
    id: &str,
    db: &str,
    tables: Vec<String>,
    snapshot_cfg: SnapshotCfg,
) -> MySqlSource {
    let dsn = mysql_cdc_dsn(db).await;
    MySqlSource {
        id: id.into(),
        dsn,
        tables,
        tenant: "acme".into(),
        pipeline: "test".into(),
        registry: make_registry().await,
        outbox_tables: AllowList::default(),
        snapshot_cfg,
        backend: make_storage_backend().await,
        on_schema_drift: deltaforge_config::OnSchemaDrift::Adapt,
        table_options: Default::default(),
    }
}

async fn collect_until<F>(
    rx: &mut mpsc::Receiver<SourceItem>,
    dur: Duration,
    pred: F,
) -> Vec<Event>
where
    F: Fn(&[Event]) -> bool,
{
    let mut events = Vec::new();
    let deadline = Instant::now() + dur;
    while Instant::now() < deadline {
        match timeout(Duration::from_millis(100), rx.recv()).await {
            Ok(Some(SourceItem::Event(e))) => {
                events.push(e);
                if pred(&events) {
                    return events;
                }
            }
            _ => continue,
        }
    }
    events
}

fn has_id(e: &Event, id: i64) -> bool {
    e.after
        .as_ref()
        .and_then(|v| v.get("id"))
        .and_then(|v| v.as_i64())
        .map(|v| v == id)
        .unwrap_or(false)
}

// ============================================================================
// Tests
// ============================================================================

/// Snapshot captures all rows as Op::Read events, then CDC picks up new writes.
#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_snapshot_captures_existing_rows() -> Result<()> {
    let (db, pool, _dsn) = mysql_setup("snap_basic").await?;
    let mut conn = pool.get_conn().await?;

    conn.query_drop(format!("USE {db}")).await?;
    conn.query_drop(
        "CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(64))",
    )
    .await?;
    conn.query_drop(format!(
        "GRANT SELECT ON {db}.orders TO '{MYSQL_CDC_USER}'@'%'"
    ))
    .await?;

    // Pre-existing rows — must appear as Op::Read snapshot events.
    for i in 1..=5 {
        conn.query_drop(format!("INSERT INTO orders VALUES ({i}, 'sku-{i}')"))
            .await?;
    }

    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let src = make_source(
        "snap-basic",
        &db,
        vec![format!("{db}.orders")],
        SnapshotCfg {
            mode: SnapshotMode::Initial,
            ..Default::default()
        },
    )
    .await;

    let (tx, mut rx) = mpsc::channel(128);
    let handle = src.run(tx, ckpt).await;

    let events = collect_until(&mut rx, Duration::from_secs(30), |e| {
        e.iter().filter(|x| matches!(x.op, Op::Read)).count() >= 5
    })
    .await;

    let reads: Vec<_> =
        events.iter().filter(|e| matches!(e.op, Op::Read)).collect();
    assert_eq!(reads.len(), 5, "should have 5 snapshot (Read) events");

    for e in &reads {
        assert_eq!(e.source.connector, "mysql");
        assert_eq!(e.source.snapshot.as_deref(), Some("true"));
        assert!(e.after.is_some());
    }
    info!("✓ snapshot captured 5 rows as Op::Read");

    // Post-snapshot insert should arrive as Op::Create via CDC.
    conn.query_drop("INSERT INTO orders VALUES (6, 'sku-6')")
        .await?;
    let events = collect_until(&mut rx, Duration::from_secs(20), |e| {
        e.iter().any(|x| has_id(x, 6) && matches!(x.op, Op::Create))
    })
    .await;

    assert!(
        events
            .iter()
            .any(|e| has_id(e, 6) && matches!(e.op, Op::Create)),
        "post-snapshot insert should arrive as Op::Create"
    );
    info!("✓ CDC picks up post-snapshot inserts");

    handle.stop();
    handle.join().await.ok();
    mysql_drop_db(&pool, &db).await;
    Ok(())
}

/// Snapshot with SnapshotMode::Never skips the snapshot entirely.
#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_snapshot_never_skips_existing_rows() -> Result<()> {
    let (db, pool, _dsn) = mysql_setup("snap_never").await?;
    let mut conn = pool.get_conn().await?;

    conn.query_drop(format!("USE {db}")).await?;
    conn.query_drop(
        "CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(64))",
    )
    .await?;
    conn.query_drop(format!(
        "GRANT SELECT ON {db}.orders TO '{MYSQL_CDC_USER}'@'%'"
    ))
    .await?;
    conn.query_drop("INSERT INTO orders VALUES (1, 'pre-existing')")
        .await?;

    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let src = make_source(
        "snap-never",
        &db,
        vec![format!("{db}.orders")],
        SnapshotCfg {
            mode: SnapshotMode::Never,
            ..Default::default()
        },
    )
    .await;

    let (tx, mut rx) = mpsc::channel(128);
    let handle = src.run(tx, ckpt).await;
    sleep(Duration::from_secs(3)).await;

    // Pre-existing row must NOT appear.
    let snapshot_events: Vec<_> =
        collect_until(&mut rx, Duration::from_secs(3), |_| false)
            .await
            .into_iter()
            .filter(|e| matches!(e.op, Op::Read))
            .collect();
    assert!(snapshot_events.is_empty(), "No::Never should skip snapshot");
    info!("✓ SnapshotMode::Never skips existing rows");

    // New insert should still stream via CDC.
    conn.query_drop("INSERT INTO orders VALUES (2, 'post')")
        .await?;
    let events = collect_until(&mut rx, Duration::from_secs(15), |e| {
        e.iter().any(|x| has_id(x, 2))
    })
    .await;
    assert!(events.iter().any(|e| has_id(e, 2)));
    info!("✓ CDC still streams after SnapshotMode::Never");

    handle.stop();
    handle.join().await.ok();
    mysql_drop_db(&pool, &db).await;
    Ok(())
}

/// Snapshot resumes after a partial completion (crash recovery at table level).
#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_snapshot_resumes_after_partial_completion() -> Result<()> {
    use sources::mysql::MysqlSnapshotProgress;
    use sources::mysql::mysql_snapshot::progress_key;

    let (db, pool, _dsn) = mysql_setup("snap_resume").await?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop(format!("USE {db}")).await?;

    for tbl in &["table_a", "table_b"] {
        conn.query_drop(format!(
            "CREATE TABLE {tbl} (id INT PRIMARY KEY, val VARCHAR(32))"
        ))
        .await?;
        conn.query_drop(format!(
            "GRANT SELECT ON {db}.{tbl} TO '{MYSQL_CDC_USER}'@'%'"
        ))
        .await?;
        for i in 1..=3 {
            conn.query_drop(format!("INSERT INTO {tbl} VALUES ({i}, 'v{i}')"))
                .await?;
        }
    }

    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);

    // Seed progress directly - table_a done, table_b pending.
    let mut pos_conn = pool.get_conn().await?;
    let row: mysql_async::Row = pos_conn
        .query_first("SHOW BINARY LOG STATUS")
        .await?
        .unwrap();
    let file: String = row.get(0).unwrap();
    let pos: u32 = row.get(1).unwrap();

    let fake = MysqlSnapshotProgress {
        start_position: serde_json::to_string(
            &sources::mysql::MySqlCheckpoint {
                file,
                pos: pos as u64,
                gtid_set: None,
            },
        )?,
        done_tables: vec![format!("{db}.table_a")],
        finished: false,
    };
    ckpt.put_raw(&progress_key("snap-resume"), &serde_json::to_vec(&fake)?)
        .await?;

    // Second run — should only snapshot table_b.
    let src = make_source(
        "snap-resume",
        &db,
        vec![format!("{db}.table_a"), format!("{db}.table_b")],
        SnapshotCfg {
            mode: SnapshotMode::Initial,
            ..Default::default()
        },
    )
    .await;
    let (tx, mut rx) = mpsc::channel(256);
    let handle = src.run(tx, ckpt.clone()).await;

    let events = collect_until(&mut rx, Duration::from_secs(30), |e| {
        e.iter().filter(|x| matches!(x.op, Op::Read)).count() >= 3
    })
    .await;

    let reads: Vec<_> =
        events.iter().filter(|e| matches!(e.op, Op::Read)).collect();
    // Should only get table_b rows (table_a was already marked done).
    assert!(
        reads.iter().all(|e| e.source.table == "table_b"),
        "resume should only snapshot table_b"
    );
    assert_eq!(reads.len(), 3);
    info!("✓ snapshot resumed from table_b only");

    handle.stop();
    handle.join().await.ok();
    mysql_drop_db(&pool, &db).await;
    Ok(())
}

/// Snapshot uses PK-range chunking for integer PK tables.
#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_snapshot_parallel_tables() -> Result<()> {
    let (db, pool, _dsn) = mysql_setup("snap_parallel").await?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop(format!("USE {db}")).await?;

    let tables = ["orders", "customers", "products"];
    for tbl in &tables {
        conn.query_drop(format!(
            "CREATE TABLE {tbl} (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(64))"
        ))
        .await?;
        conn.query_drop(format!(
            "GRANT SELECT ON {db}.{tbl} TO '{MYSQL_CDC_USER}'@'%'"
        ))
        .await?;
        for i in 1..=10 {
            conn.query_drop(format!(
                "INSERT INTO {tbl} (name) VALUES ('item-{i}')"
            ))
            .await?;
        }
    }

    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let src = make_source(
        "snap-parallel",
        &db,
        tables.iter().map(|t| format!("{db}.{t}")).collect(),
        SnapshotCfg {
            mode: SnapshotMode::Initial,
            max_parallel_tables: 3,
            ..Default::default()
        },
    )
    .await;

    let t0 = Instant::now();
    let (tx, mut rx) = mpsc::channel(256);
    let handle = src.run(tx, ckpt).await;

    let events = collect_until(&mut rx, Duration::from_secs(30), |e| {
        e.iter().filter(|x| matches!(x.op, Op::Read)).count() >= 30
    })
    .await;

    let reads: Vec<_> =
        events.iter().filter(|e| matches!(e.op, Op::Read)).collect();
    assert_eq!(
        reads.len(),
        30,
        "should have 30 snapshot rows across 3 tables"
    );
    info!(
        elapsed_ms = t0.elapsed().as_millis(),
        "✓ parallel snapshot captured 30 rows across 3 tables"
    );

    handle.stop();
    handle.join().await.ok();
    mysql_drop_db(&pool, &db).await;
    Ok(())
}

/// SnapshotMode::Always re-snapshots on every run.
#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_snapshot_always_reruns() -> Result<()> {
    let (db, pool, _dsn) = mysql_setup("snap_always").await?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop(format!("USE {db}")).await?;
    conn.query_drop(
        "CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(64))",
    )
    .await?;
    conn.query_drop(format!(
        "GRANT SELECT ON {db}.orders TO '{MYSQL_CDC_USER}'@'%'"
    ))
    .await?;
    conn.query_drop("INSERT INTO orders VALUES (1, 'sku-1')")
        .await?;

    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);

    // First run.
    {
        let src = make_source(
            "snap-always",
            &db,
            vec![format!("{db}.orders")],
            SnapshotCfg {
                mode: SnapshotMode::Always,
                ..Default::default()
            },
        )
        .await;
        let (tx, mut rx) = mpsc::channel(64);
        let handle = src.run(tx, ckpt.clone()).await;
        let events = collect_until(&mut rx, Duration::from_secs(20), |e| {
            e.iter().any(|x| matches!(x.op, Op::Read))
        })
        .await;
        assert!(events.iter().any(|e| matches!(e.op, Op::Read)));
        handle.stop();
        handle.join().await.ok();
    }

    conn.query_drop("INSERT INTO orders VALUES (2, 'sku-2')")
        .await?;

    // Second run - SnapshotMode::Always means snapshot runs again.
    {
        let src = make_source(
            "snap-always",
            &db,
            vec![format!("{db}.orders")],
            SnapshotCfg {
                mode: SnapshotMode::Always,
                ..Default::default()
            },
        )
        .await;
        let (tx, mut rx) = mpsc::channel(64);
        let handle = src.run(tx, ckpt.clone()).await;
        let events = collect_until(&mut rx, Duration::from_secs(20), |e| {
            e.iter().filter(|x| matches!(x.op, Op::Read)).count() >= 2
        })
        .await;
        let reads: Vec<_> =
            events.iter().filter(|e| matches!(e.op, Op::Read)).collect();
        assert_eq!(reads.len(), 2, "Always mode should re-snapshot both rows");
        info!("✓ SnapshotMode::Always re-runs snapshot on second start");
        handle.stop();
        handle.join().await.ok();
    }

    mysql_drop_db(&pool, &db).await;
    Ok(())
}

// ============================================================================
// Stable snapshot identity (dfid:v1) — generation lifecycle + keyless rejection
// ============================================================================

use deltaforge_core::EventId;
use storage::ArcStorageBackend;

/// Build a source sharing a specific storage backend (so the durable snapshot
/// generation persists across runs) and optional per-table options.
async fn make_source_on(
    backend: ArcStorageBackend,
    id: &str,
    db: &str,
    tables: Vec<String>,
    snapshot_cfg: SnapshotCfg,
    table_options: std::collections::BTreeMap<
        String,
        deltaforge_config::TableOptions,
    >,
) -> MySqlSource {
    let dsn = mysql_cdc_dsn(db).await;
    MySqlSource {
        id: id.into(),
        dsn,
        tables,
        tenant: "acme".into(),
        pipeline: "test".into(),
        registry: make_registry().await,
        outbox_tables: AllowList::default(),
        snapshot_cfg,
        backend,
        on_schema_drift: deltaforge_config::OnSchemaDrift::Adapt,
        table_options,
    }
}

fn snapshot_ids(events: &[Event]) -> Vec<EventId> {
    events
        .iter()
        .filter(|e| matches!(e.op, Op::Read))
        .filter_map(|e| e.event_id)
        .collect()
}

/// Every snapshot row carries the allocated generation and a provisional
/// stable id; ids are unique per row and all `snap`-class.
#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_snapshot_rows_carry_generation_and_stable_ids() -> Result<()> {
    let (db, pool, _dsn) = mysql_setup("snap_ids").await?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop(format!("USE {db}")).await?;
    conn.query_drop(
        "CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(64))",
    )
    .await?;
    conn.query_drop(format!(
        "GRANT SELECT ON {db}.orders TO '{MYSQL_CDC_USER}'@'%'"
    ))
    .await?;
    for i in 1..=5 {
        conn.query_drop(format!("INSERT INTO orders VALUES ({i}, 'sku-{i}')"))
            .await?;
    }

    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let src = make_source_on(
        make_storage_backend().await,
        "snap-ids",
        &db,
        vec![format!("{db}.orders")],
        SnapshotCfg {
            mode: SnapshotMode::Initial,
            ..Default::default()
        },
        Default::default(),
    )
    .await;

    let (tx, mut rx) = mpsc::channel(128);
    let handle = src.run(tx, ckpt).await;
    let events = collect_until(&mut rx, Duration::from_secs(30), |e| {
        e.iter().filter(|x| matches!(x.op, Op::Read)).count() >= 5
    })
    .await;

    let reads: Vec<_> =
        events.iter().filter(|e| matches!(e.op, Op::Read)).collect();
    assert_eq!(reads.len(), 5);
    for e in &reads {
        assert_eq!(e.source.position.snapshot_generation, Some(1));
        assert!(
            e.event_id.is_some(),
            "snapshot row must carry a provisional stable id"
        );
        assert_eq!(
            e.event_id.unwrap().class(),
            deltaforge_core::EventClass::Snap
        );
    }
    let ids = snapshot_ids(&events);
    let unique: std::collections::HashSet<_> = ids.iter().collect();
    assert_eq!(unique.len(), ids.len(), "ids must be unique per row");

    handle.stop();
    handle.join().await.ok();
    mysql_drop_db(&pool, &db).await;
    Ok(())
}

/// An explicit re-snapshot (`SnapshotMode::Always`) allocates a NEW generation,
/// so the same rows get different ids. Uses a shared backend so the generation
/// counter persists across runs.
#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_resnapshot_allocates_new_generation() -> Result<()> {
    let (db, pool, _dsn) = mysql_setup("snap_resnap").await?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop(format!("USE {db}")).await?;
    conn.query_drop(
        "CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(64))",
    )
    .await?;
    conn.query_drop(format!(
        "GRANT SELECT ON {db}.orders TO '{MYSQL_CDC_USER}'@'%'"
    ))
    .await?;
    for i in 1..=3 {
        conn.query_drop(format!("INSERT INTO orders VALUES ({i}, 'sku-{i}')"))
            .await?;
    }

    let backend = make_storage_backend().await;
    let run = |mode| {
        let backend = backend.clone();
        let db = db.clone();
        async move {
            let ckpt: Arc<dyn CheckpointStore> =
                Arc::new(MemCheckpointStore::new().unwrap());
            let src = make_source_on(
                backend,
                "snap-resnap",
                &db,
                vec![format!("{db}.orders")],
                SnapshotCfg {
                    mode,
                    ..Default::default()
                },
                Default::default(),
            )
            .await;
            let (tx, mut rx) = mpsc::channel(128);
            let handle = src.run(tx, ckpt).await;
            let events = collect_until(&mut rx, Duration::from_secs(30), |e| {
                e.iter().filter(|x| matches!(x.op, Op::Read)).count() >= 3
            })
            .await;
            handle.stop();
            handle.join().await.ok();
            events
        }
    };

    let first = run(SnapshotMode::Initial).await;
    let second = run(SnapshotMode::Always).await;

    let first_read = first
        .iter()
        .find(|e| matches!(e.op, Op::Read))
        .expect("initial snapshot emitted no rows");
    let second_read = second
        .iter()
        .find(|e| matches!(e.op, Op::Read))
        .expect("resnapshot emitted no rows");
    assert_eq!(first_read.source.position.snapshot_generation, Some(1));
    assert_eq!(
        second_read.source.position.snapshot_generation,
        Some(2),
        "resnapshot must allocate a new generation"
    );

    let ids1: std::collections::HashSet<_> =
        snapshot_ids(&first).into_iter().collect();
    let ids2: std::collections::HashSet<_> =
        snapshot_ids(&second).into_iter().collect();
    assert!(
        ids1.is_disjoint(&ids2),
        "a new generation must change every row's id"
    );

    mysql_drop_db(&pool, &db).await;
    Ok(())
}

/// A keyless table (no PK, no `identity_columns`) is rejected before any
/// snapshot row is emitted — no `Op::Read` events arrive.
#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_keyless_table_rejected_before_rows() -> Result<()> {
    let (db, pool, _dsn) = mysql_setup("snap_keyless").await?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop(format!("USE {db}")).await?;
    // No primary key.
    conn.query_drop("CREATE TABLE logs (msg VARCHAR(64), lvl VARCHAR(8))")
        .await?;
    conn.query_drop(format!(
        "GRANT SELECT ON {db}.logs TO '{MYSQL_CDC_USER}'@'%'"
    ))
    .await?;
    conn.query_drop("INSERT INTO logs VALUES ('boot', 'info')")
        .await?;

    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let src = make_source_on(
        make_storage_backend().await,
        "snap-keyless",
        &db,
        vec![format!("{db}.logs")],
        SnapshotCfg {
            mode: SnapshotMode::Initial,
            ..Default::default()
        },
        Default::default(),
    )
    .await;

    let (tx, mut rx) = mpsc::channel(128);
    let handle = src.run(tx, ckpt).await;
    let events =
        collect_until(&mut rx, Duration::from_secs(8), |_| false).await;
    let reads = events.iter().filter(|e| matches!(e.op, Op::Read)).count();
    assert_eq!(reads, 0, "keyless table must emit no snapshot rows");

    handle.stop();
    handle.join().await.ok();
    mysql_drop_db(&pool, &db).await;
    Ok(())
}
