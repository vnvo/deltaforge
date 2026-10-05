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
        dsn: dsn.into(),
        tables,
        tenant: "acme".into(),
        pipeline: "test".into(),
        registry: make_registry().await,
        registry_scope: sources::registry_scope::SharedRegistryScope::default(),
        outbox_tables: AllowList::default(),
        snapshot_cfg,
        backend: make_storage_backend().await,
        on_schema_drift: deltaforge_config::OnSchemaDrift::Adapt,
        table_options: Default::default(),
        rotation: None,
        snapshot_cohort: Default::default(),
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
    let _probe = PROBED_RUN.read().await;
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

    let (tx, mut rx) = test_common::acked_channel(&src, &ckpt, &src.id, 128);
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
    let _probe = PROBED_RUN.read().await;
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

    let (tx, mut rx) = test_common::acked_channel(&src, &ckpt, &src.id, 128);
    let handle = src.run(tx, ckpt).await;
    sleep(Duration::from_secs(3)).await;

    // Pre-existing row must NOT appear.
    let snapshot_events: Vec<_> =
        collect_until(&mut rx, Duration::from_secs(3), |_| false)
            .await
            .into_iter()
            // Rows of this table only: DDL events of other tests' databases
            // on the shared server also arrive as reads.
            .filter(|e| matches!(e.op, Op::Read) && e.source.table == "orders")
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

/// A generation lives and dies with its read view: an interrupted
/// generation is never resumed. The next start replaces it with the next
/// generation, which takes a new anchor and copies every table again, so a
/// change made after the interruption (to a table already copied or not) is
/// delivered.
#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_an_interrupted_snapshot_restarts_in_full() -> Result<()> {
    let _probe = PROBED_RUN.read().await;
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

    let backend = make_storage_backend().await;
    let tables = vec![format!("{db}.table_a"), format!("{db}.table_b")];
    let cfg = || SnapshotCfg {
        mode: SnapshotMode::Initial,
        chunk_size: 1,
        max_parallel_tables: 1,
        ..Default::default()
    };
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);

    // Generation 1, interrupted after some rows.
    {
        let first = make_source_on(
            backend.clone(),
            "snap-resume",
            &db,
            tables.clone(),
            cfg(),
            Default::default(),
        )
        .await;
        let (tx, mut rx) =
            test_common::acked_channel(&first, &ckpt, &first.id, 256);
        let handle = first.run(tx, ckpt.clone()).await;
        collect_until(&mut rx, Duration::from_secs(30), |e| {
            e.iter().filter(|x| matches!(x.op, Op::Read)).count() >= 2
        })
        .await;
        handle.stop();
        handle.join().await.ok();
    }
    let interrupted =
        test_common::snapshot_state(&backend, "snap-resume").await;
    assert_ne!(
        interrupted.as_ref().map(|s| s.0.as_str()),
        Some("completed"),
        "{interrupted:?}"
    );

    // A change after the interruption.
    conn.query_drop(
        "INSERT INTO table_a VALUES (99, 'after the interruption')",
    )
    .await?;

    let src = make_source_on(
        backend.clone(),
        "snap-resume",
        &db,
        tables,
        cfg(),
        Default::default(),
    )
    .await;
    let (tx, mut rx) = test_common::acked_channel(&src, &ckpt, &src.id, 256);
    let handle = src.run(tx, ckpt.clone()).await;
    let events = collect_until(&mut rx, Duration::from_secs(60), |e| {
        e.iter().filter(|x| matches!(x.op, Op::Read)).count() >= 7
    })
    .await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while test_common::snapshot_state(&backend, "snap-resume")
        .await
        .map(|s| s.0)
        != Some("completed".into())
        && tokio::time::Instant::now() < deadline
    {
        sleep(Duration::from_millis(100)).await;
    }
    handle.stop();
    handle.join().await.ok();

    let reads: Vec<_> =
        events.iter().filter(|e| matches!(e.op, Op::Read)).collect();
    let in_table =
        |t: &str| reads.iter().filter(|e| e.source.table == t).count();
    assert_eq!(in_table("table_a"), 4, "every row again, and the change");
    assert_eq!(in_table("table_b"), 3);
    assert!(
        reads
            .iter()
            .all(|e| e.source.position.snapshot_generation == Some(2)),
        "the replacing generation"
    );
    assert_eq!(
        test_common::snapshot_state(&backend, "snap-resume").await,
        Some(("completed".into(), 2))
    );

    mysql_drop_db(&pool, &db).await;
    Ok(())
}

/// Baselines precede completion: the process crashes at the earliest point
/// the generation can complete - right after the sinks acknowledged its
/// terminal barrier. Every snapshotted table's anchor baseline is already
/// durable, and the restart completes the generation without copying again
/// and streams on.
#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_baselines_precede_completion() -> Result<()> {
    use sources::snapshot_probe;
    use storage::adapters::{LineageDescriptor, SchemaKey};
    let _probe = PROBED_RUN.write().await;
    let (db, pool, _dsn) = mysql_setup("snap_base").await?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop(format!("USE {db}")).await?;
    for tbl in ["ta", "tb"] {
        conn.query_drop(format!(
            "CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT); \
             INSERT INTO {tbl} VALUES (1, 1), (2, 2);"
        ))
        .await?;
        conn.query_drop(format!(
            "GRANT SELECT ON {db}.{tbl} TO '{MYSQL_CDC_USER}'@'%'"
        ))
        .await?;
    }
    let backend = make_storage_backend().await;
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let tables = vec![format!("{db}.ta"), format!("{db}.tb")];
    let cfg = || SnapshotCfg {
        mode: SnapshotMode::Initial,
        ..Default::default()
    };
    let id = "snap-base";

    // Generation 1, until the sink acknowledged its terminal barrier.
    let src = make_source_on(
        backend.clone(),
        id,
        &db,
        tables.clone(),
        cfg(),
        Default::default(),
    )
    .await;
    let (reached, _release) = snapshot_probe::hold_after_terminal();
    let (tx, _rx) = test_common::acked_channel(&src, &ckpt, &src.id, 256);
    let handle = src.run(tx, ckpt.clone()).await;
    tokio::time::timeout(Duration::from_secs(120), reached.notified())
        .await
        .expect("the terminal barrier is sent");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let acked = ckpt
            .get_raw(&format!("{id}::sink::{}", test_common::TEST_SINK))
            .await?
            .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok())
            .is_some_and(|v| v.get("snapshot_completed").is_some());
        if acked {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "never acknowledged");
        sleep(Duration::from_millis(50)).await;
    }
    // The crash: nothing more happens in this process.
    handle.join.abort();
    let _ = handle.join.await;
    assert_eq!(
        test_common::snapshot_state(&backend, id).await,
        Some(("rows_produced".into(), 1)),
        "not completed before the crash"
    );

    // The baselines are already durable.
    let server_uuid: String = conn
        .query_first("SELECT @@global.server_uuid")
        .await?
        .expect("server_uuid");
    let hash = LineageDescriptor::mysql(&server_uuid)
        .unwrap()
        .lineage_hash();
    for tbl in ["ta", "tb"] {
        let key = SchemaKey::new("acme", id, &hash, &db, tbl).backend_key();
        let records: Vec<serde_json::Value> = backend
            .log_list("schemas.v1.activation", &key)
            .await?
            .into_iter()
            .map(|(_, b)| serde_json::from_slice(&b).unwrap())
            .collect();
        assert!(
            records.iter().any(|r| r["kind"] == "baseline"),
            "{tbl}: no baseline before completion: {records:?}"
        );
    }

    // The restart completes it without copying and streams (one schema
    // registry across the restart, as in production).
    let registry = src.registry.clone();
    let src = MySqlSource {
        registry,
        ..make_source_on(
            backend.clone(),
            id,
            &db,
            tables,
            cfg(),
            Default::default(),
        )
        .await
    };
    let (tx, mut rx) = test_common::acked_channel(&src, &ckpt, &src.id, 256);
    let handle = src.run(tx, ckpt.clone()).await;
    sleep(Duration::from_secs(3)).await;
    conn.query_drop("INSERT INTO ta VALUES (3, 3)").await?;
    let events = collect_until(&mut rx, Duration::from_secs(30), |e| {
        e.iter().any(|x| matches!(x.op, Op::Create))
    })
    .await;
    handle.stop();
    handle.join.await.ok();
    assert!(
        events.iter().all(|e| !matches!(e.op, Op::Read)),
        "nothing copied again"
    );
    assert!(events.iter().any(|e| matches!(e.op, Op::Create)), "streams");
    assert_eq!(
        test_common::snapshot_state(&backend, id).await,
        Some(("completed".into(), 1))
    );
    mysql_drop_db(&pool, &db).await;
    Ok(())
}

/// Snapshot uses PK-range chunking for integer PK tables.
#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_snapshot_parallel_tables() -> Result<()> {
    let _probe = PROBED_RUN.read().await;
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
    let (tx, mut rx) = test_common::acked_channel(&src, &ckpt, &src.id, 256);
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
    let _probe = PROBED_RUN.read().await;
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
    // One state store across the restart, as in production.
    let backend = make_storage_backend().await;

    // First run.
    {
        let src = make_source_on(
            backend.clone(),
            "snap-always",
            &db,
            vec![format!("{db}.orders")],
            SnapshotCfg {
                mode: SnapshotMode::Always,
                ..Default::default()
            },
            Default::default(),
        )
        .await;
        let (tx, mut rx) = test_common::acked_channel(&src, &ckpt, &src.id, 64);
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
        let src = make_source_on(
            backend.clone(),
            "snap-always",
            &db,
            vec![format!("{db}.orders")],
            SnapshotCfg {
                mode: SnapshotMode::Always,
                ..Default::default()
            },
            Default::default(),
        )
        .await;
        let (tx, mut rx) = test_common::acked_channel(&src, &ckpt, &src.id, 64);
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
        dsn: dsn.into(),
        tables,
        tenant: "acme".into(),
        pipeline: "test".into(),
        registry: make_registry().await,
        registry_scope: sources::registry_scope::SharedRegistryScope::default(),
        outbox_tables: AllowList::default(),
        snapshot_cfg,
        backend,
        on_schema_drift: deltaforge_config::OnSchemaDrift::Adapt,
        table_options,
        rotation: None,
        snapshot_cohort: Default::default(),
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
    let _probe = PROBED_RUN.read().await;
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

    let (tx, mut rx) = test_common::acked_channel(&src, &ckpt, &src.id, 128);
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
    let _probe = PROBED_RUN.read().await;
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
            let (tx, mut rx) =
                test_common::acked_channel(&src, &ckpt, &src.id, 128);
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

    let (tx, mut rx) = test_common::acked_channel(&src, &ckpt, &src.id, 128);
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

// ============================================================================
// Paged discovery and preparation: structural shape
// ============================================================================

/// One snapshot start over `n` tables matching `{db}.myt*` (plus tables it
/// does not match), with discovery pages of 10 and 3 parallel tables;
/// returns its structural shape.
async fn snapshot_shape(
    n: usize,
) -> Result<sources::snapshot_probe::SnapshotShape> {
    let run = snapshot_with_ddl(n, None).await?;
    let (shape, reads) = (run.shape, run.reads);
    assert_eq!(reads, n, "every matched table's row was snapshotted");
    Ok(shape)
}

/// Like [`snapshot_shape`], optionally running `ddl` (in the test database)
/// after the first discovery page; returns the shape, the rows read and the
/// run's outcome.
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
    let (db, pool, _dsn) = mysql_setup(&format!("shape{n}")).await?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop(format!("USE {db}")).await?;
    for i in 0..n {
        conn.query_drop(format!(
            "CREATE TABLE myt_{i:03} (id INT PRIMARY KEY); \
             INSERT INTO myt_{i:03} VALUES (1);"
        ))
        .await?;
    }
    conn.query_drop(
        "CREATE TABLE other_a (id INT PRIMARY KEY); \
         CREATE TABLE zz_other (id INT PRIMARY KEY);",
    )
    .await?;
    if !setup.is_empty() {
        conn.query_drop(setup).await?;
    }
    conn.query_drop(format!(
        "GRANT SELECT ON {db}.* TO '{MYSQL_CDC_USER}'@'%'"
    ))
    .await?;
    let src = make_source(
        &format!("shape{n}"),
        &db,
        vec![format!("{db}.myt*")],
        SnapshotCfg {
            mode: SnapshotMode::Initial,
            max_parallel_tables: 3,
            discovery_page_size: 10,
            ..Default::default()
        },
    )
    .await;
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let backend = src.backend.clone();
    let (tx, mut rx) = test_common::acked_channel(&src, &ckpt, &src.id, 4096);
    snapshot_probe::reset();
    let hold = ddl.map(|(when, _)| match when {
        Hold::FirstPage => snapshot_probe::hold_after_discovery_page(),
        Hold::BeforeAnchor => snapshot_probe::hold_before_anchor(),
    });
    let handle = src.run(tx, ckpt.clone()).await;
    if let (Some((reached, release)), Some((_, ddl))) = (&hold, ddl) {
        reached.notified().await;
        conn.query_drop(ddl).await?;
        release.notify_one();
    }
    // A run that stops early closes the channel; otherwise wait for n rows.
    let wait = if ddl.is_some() { 30 } else { 120 };
    let events = collect_until(&mut rx, Duration::from_secs(wait), |e| {
        e.iter().filter(|x| matches!(x.op, Op::Read)).count() >= n
    })
    .await;
    let shape = snapshot_probe::shape();
    handle.stop();
    // Stopping a running source ends it with `Cancelled`: not a failure.
    let err = handle
        .join()
        .await
        .err()
        .map(|e| format!("{e:#}"))
        .filter(|e| e != "operation cancelled");
    let reads = events.iter().filter(|e| matches!(e.op, Op::Read)).count();
    let id = format!("shape{n}");
    let generation_status = test_common::snapshot_state(&backend, &id)
        .await
        .map(|(state, _)| state);
    let finished = matches!(
        generation_status.as_deref(),
        Some("rows_produced" | "completed")
    );
    mysql_drop_db(&pool, &db).await;
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

/// Discovery reads the catalog in pages and each table is prepared once:
/// catalog queries are `floor(selected rows / page size) + 1`, every matched
/// table is discovered and prepared exactly once, a page never exceeds the
/// page size, workers make no live catalog schema fetch, and at most
/// `max_parallel_tables` table tasks are alive. Doubling the tables changes
/// only the per-page and per-table counters.
#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_discovery_is_paged_and_each_table_prepared_once() -> Result<()> {
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
    // Fixed operations: identical, once each (MySQL also re-reads the
    // catalog under the anchor's read lock, page for page).
    assert_eq!(s25.fixed, s50.fixed);
    assert_eq!(s25.fixed, [1, 0, 1, 1, 1, 1, 1], "{s25:?}");
    for s in [s25, s50] {
        assert_eq!(s.verification_queries, s.discovery_queries, "{s:?}");
    }
    assert_eq!(s25.registry_reads, 25, "one registry read per table");
    // O(1) boundaries: no per-table frontier is held.
    assert_eq!(s25.frontier_tables, 0);
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
async fn mysql_wildcard_legacy_progress_ambiguity_is_detected() -> Result<()> {
    let _probe = PROBED_RUN.read().await;
    let (db, pool, _dsn) = mysql_setup("ambig").await?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop(format!("USE {db}")).await?;
    conn.query_drop(
        "CREATE TABLE amb_a (id INT PRIMARY KEY); \
         CREATE TABLE amb_b (id INT PRIMARY KEY);",
    )
    .await?;
    conn.query_drop(format!(
        "GRANT SELECT ON {db}.* TO '{MYSQL_CDC_USER}'@'%'"
    ))
    .await?;
    let src = make_source(
        "ambig",
        &db,
        vec![format!("{db}.amb*")],
        SnapshotCfg {
            mode: SnapshotMode::Initial,
            ..Default::default()
        },
    )
    .await;
    let store = MemCheckpointStore::new()?;
    let key = sources::mysql::mysql_snapshot::progress_key("ambig");
    let put = |done: Vec<String>, finished: bool| {
        serde_json::to_vec(&serde_json::json!({
            "start_position": "", "done_tables": done, "finished": finished
        }))
        .unwrap()
    };
    store
        .put_raw(&key, &put(vec![format!("{db}.amb_a")], false))
        .await?;
    assert!(
        src.check_durable_snapshot_startup(&store).await.is_err(),
        "one of the expanded tables done, one pending"
    );
    for (done, finished) in [
        (vec![format!("{db}.amb_a"), format!("{db}.amb_b")], false),
        (vec![format!("{db}.amb_a")], true),
        (vec![], false),
    ] {
        store.put_raw(&key, &put(done.clone(), finished)).await?;
        assert!(
            src.check_durable_snapshot_startup(&store).await.is_ok(),
            "{done:?} finished={finished}"
        );
    }
    mysql_drop_db(&pool, &db).await;
    Ok(())
}

/// MySQL's catalog is not read in one consistent snapshot, so the anchor
/// re-reads it under its read lock (no DDL can run) and requires exactly the
/// planned tables: a table created after the first discovery page into the
/// range that page already covered (discovery never sees it) fails the
/// snapshot before any row is read.
#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_catalog_movement_during_discovery_fails_closed() -> Result<()> {
    let run = snapshot_with_ddl(
        25,
        // Sorts between myt_000 and myt_001: behind the cursor.
        Some((
            Hold::FirstPage,
            "CREATE TABLE myt_0005 (id INT PRIMARY KEY)",
        )),
    )
    .await?;
    let (shape, reads, err) = (run.shape, run.reads, run.err);
    let err = err.expect("the snapshot stops");
    assert!(
        err.contains("changed between discovery and the snapshot anchor")
            && err.contains("myt_0005"),
        "{err}"
    );
    assert_eq!(reads, 0, "before any row");
    assert_eq!(shape.fixed[6], 0, "verification did not pass");
    Ok(())
}

/// A prepared table altered before the anchor (the plan's schema no longer
/// matches): the anchor re-reads every planned table's shape under its read
/// lock and stops the snapshot before any row, never finished.
#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_a_table_altered_before_the_anchor_stops_the_snapshot()
-> Result<()> {
    let run = snapshot_with_ddl(
        25,
        Some((
            Hold::BeforeAnchor,
            "ALTER TABLE myt_007 ADD COLUMN extra TEXT",
        )),
    )
    .await?;
    let err = run.err.expect("the snapshot stops");
    assert!(
        err.contains("the schema of") && err.contains("myt_007"),
        "{err}"
    );
    assert_eq!(run.reads, 0, "before any row");
    assert!(!run.finished, "never finished");
    assert_ne!(run.generation_status.as_deref(), Some("completed"));
    Ok(())
}

/// The anchor compares the whole registered schema, not a reduced shape: a
/// change that keeps every column's name, `COLUMN_TYPE` and nullability (a
/// collation, a primary-key prefix length) still stops the snapshot before
/// any row, unfinished and never completed.
#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_the_anchor_compares_the_registered_schema() -> Result<()> {
    let setup = "ALTER TABLE myt_007 \
                 ADD COLUMN v VARCHAR(20) NOT NULL DEFAULT 'a' \
                 COLLATE utf8mb4_0900_ai_ci, \
                 DROP PRIMARY KEY, ADD PRIMARY KEY (id, v(10))";
    for (what, ddl) in [
        (
            "collation",
            "ALTER TABLE myt_007 MODIFY v VARCHAR(20) NOT NULL DEFAULT 'a' \
             COLLATE utf8mb4_bin",
        ),
        (
            "primary-key prefix",
            "ALTER TABLE myt_007 DROP PRIMARY KEY, ADD PRIMARY KEY (id, v(12))",
        ),
    ] {
        let run =
            snapshot_with_setup(25, setup, Some((Hold::BeforeAnchor, ddl)))
                .await?;
        let err = run.err.unwrap_or_else(|| panic!("{what}: not stopped"));
        assert!(
            err.contains("the schema of") && err.contains("myt_007"),
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

/// Upgrade: a snapshot generation recorded by an earlier release is not
/// taken for this release's state. Until its classification with
/// sink-checkpoint proof (design section 10) lands, the start fails closed
/// before any row, with the record untouched.
#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_an_earlier_format_generation_fails_closed_untouched()
-> Result<()> {
    let _probe = PROBED_RUN.read().await;
    use checkpoints::SnapshotStateStore;
    let (db, pool, _dsn) = mysql_setup("fpupgrade").await?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop(format!("USE {db}")).await?;
    conn.query_drop(
        "CREATE TABLE up_a (id INT PRIMARY KEY); INSERT INTO up_a VALUES (1), (2);",
    )
    .await?;
    conn.query_drop(format!(
        "GRANT SELECT ON {db}.* TO '{MYSQL_CDC_USER}'@'%'"
    ))
    .await?;
    let src = make_source(
        "fpupgrade",
        &db,
        vec![format!("{db}.up_*")],
        SnapshotCfg {
            mode: SnapshotMode::Initial,
            ..Default::default()
        },
    )
    .await;
    // The source's own lineage: an earlier-format record is replaced only
    // within it.
    let server_uuid: String = conn
        .query_first("SELECT @@global.server_uuid")
        .await?
        .expect("server_uuid");
    let lineage = sources::snapshot_generation::PersistedLineage::MysqlGtid {
        source_uuid: hex::decode(server_uuid.replace('-', ""))?
            .try_into()
            .expect("a 16-byte uuid"),
    };
    let legacy = serde_json::json!({
        "generation": 3,
        "lineage": lineage,
        "status": "running",
        "config_fingerprint": "a-format-1-fingerprint",
    });
    storage::adapters::BackendCheckpointStore::new(src.backend.clone())
        .compare_and_swap(
            "snapshot_generation:fpupgrade",
            None,
            &serde_json::to_vec(&legacy)?,
        )
        .await?;
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let (tx, mut rx) = test_common::acked_channel(&src, &ckpt, &src.id, 256);
    let handle = src.run(tx, ckpt).await;
    let events = collect_until(&mut rx, Duration::from_secs(30), |e| {
        e.iter().any(|x| matches!(x.op, Op::Read))
    })
    .await;
    let err = handle.join().await.err().map(|e| format!("{e:#}"));
    assert!(events.iter().all(|e| !matches!(e.op, Op::Read)), "no row");
    let err = err.expect("refused");
    assert!(err.contains("legacy"), "{err}");
    let (_, stored) =
        storage::adapters::BackendCheckpointStore::new(src.backend.clone())
            .get_versioned("snapshot_generation:fpupgrade")
            .await?
            .expect("kept");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&stored)?,
        legacy,
        "untouched"
    );
    mysql_drop_db(&pool, &db).await;
    Ok(())
}
