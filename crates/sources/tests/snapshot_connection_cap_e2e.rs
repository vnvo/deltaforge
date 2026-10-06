//! The process-wide snapshot connection cap
//! (`docs/design/snapshot-durable-queue.md`, section 9) between two
//! PostgreSQL sources in one process: with a cap of exactly one snapshot's
//! base, the second snapshot queues holding nothing until the first one's
//! copy ends; a queued snapshot is cancellable; a source whose share cannot
//! hold its base fails its validation. Its own test binary: the cap is set
//! once per process.
//!
//! Run with:
//! `cargo test -p sources --test snapshot_connection_cap_e2e -- --include-ignored --nocapture --test-threads=1`

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use checkpoints::{CheckpointStore, MemCheckpointStore};
use deltaforge_config::{SnapshotCfg, SnapshotMode};
use deltaforge_core::{Op, Source, SourceItem};

mod test_common;
use test_common::{acked_channel, pg_admin_dsn, pg_drop_db, pg_setup};

/// One PostgreSQL snapshot's base: coordinator, guard, first worker.
const CAP: u32 = 3;

async fn source(
    db: &str,
    client: &tokio_postgres::Client,
    id: &str,
    rows: i64,
    share: Option<u32>,
) -> Result<sources::postgres::PostgresSource> {
    client
        .batch_execute(&format!(
            "CREATE TABLE {id} (id BIGINT PRIMARY KEY); \
             INSERT INTO {id} SELECT g FROM generate_series(1, {rows}) g; \
             CREATE PUBLICATION pub_{id} FOR TABLE {id};"
        ))
        .await?;
    Ok(sources::postgres::PostgresSource {
        id: id.into(),
        dsn: pg_admin_dsn(db).await.into(),
        slot: format!("slot_{id}"),
        publication: format!("pub_{id}"),
        tables: vec![format!("public.{id}")],
        tenant: "acme".into(),
        pipeline: id.into(),
        registry: test_common::make_registry().await,
        registry_scope: sources::registry_scope::SharedRegistryScope::default(),
        backend: test_common::make_storage_backend().await,
        outbox_prefixes: common::AllowList::default(),
        snapshot_cfg: SnapshotCfg {
            mode: SnapshotMode::Initial,
            max_parallel_tables: 1,
            chunk_size: 5,
            max_snapshot_connections: share,
            ..Default::default()
        },
        on_schema_drift: deltaforge_config::OnSchemaDrift::Adapt,
        table_options: Default::default(),
        rotation: None,
        snapshot_cohort: Default::default(),
    })
}

/// Snapshot rows that arrive within `wait`.
async fn reads_within(
    rx: &mut tokio::sync::mpsc::Receiver<SourceItem>,
    want: usize,
    wait: Duration,
) -> usize {
    let deadline = tokio::time::Instant::now() + wait;
    let mut n = 0;
    while n < want {
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(SourceItem::Event(ev))) if ev.op == Op::Read => n += 1,
            Ok(Some(_)) => continue,
            _ => break,
        }
    }
    n
}

#[tokio::test]
#[ignore = "requires docker"]
async fn snapshots_share_the_process_wide_cap() -> Result<()> {
    sources::snapshot_permits::configure(CAP)
        .map_err(|e| anyhow::anyhow!(e))?;
    let (db, client) = pg_setup("capshare").await?;
    let a = source(&db, &client, "cap_a", 200, None).await?;
    let b = source(&db, &client, "cap_b", 10, None).await?;
    let (ck_a, ck_b): (Arc<dyn CheckpointStore>, Arc<dyn CheckpointStore>) = (
        Arc::new(MemCheckpointStore::new()?),
        Arc::new(MemCheckpointStore::new()?),
    );

    // A's copy holds the whole cap: its rows are not read, so it blocks on a
    // small channel mid-copy.
    let (tx, mut rx_a) = acked_channel(&a, &ck_a, &a.id, 4);
    let handle_a = a.run(tx, Arc::clone(&ck_a)).await;
    assert!(reads_within(&mut rx_a, 1, Duration::from_secs(60)).await == 1);

    // B queues holding nothing: no row, and cancellable while queued.
    let (tx, mut rx_b) = acked_channel(&b, &ck_b, &b.id, 64);
    let handle_b = b.run(tx, Arc::clone(&ck_b)).await;
    assert_eq!(
        reads_within(&mut rx_b, 1, Duration::from_secs(5)).await,
        0,
        "B queues while A holds the cap"
    );
    handle_b.stop();
    let stopped =
        tokio::time::timeout(Duration::from_secs(10), handle_b.join()).await;
    let stopped = stopped
        .expect("a queued snapshot stops at once")
        .err()
        .map(|e| format!("{e:#}"));
    assert_eq!(stopped.as_deref(), Some("operation cancelled"));

    // Queued again; it runs once A's copy is done.
    let (tx, mut rx_b) = acked_channel(&b, &ck_b, &b.id, 64);
    let handle_b = b.run(tx, Arc::clone(&ck_b)).await;
    assert_eq!(
        reads_within(&mut rx_b, 1, Duration::from_secs(3)).await,
        0,
        "still queued"
    );
    assert_eq!(
        reads_within(&mut rx_a, 199, Duration::from_secs(60)).await,
        199,
        "A completes its copy"
    );
    assert_eq!(
        reads_within(&mut rx_b, 10, Duration::from_secs(60)).await,
        10,
        "B runs after A released the cap"
    );
    handle_a.stop();
    handle_b.stop();
    let _ = handle_a.join().await;
    let _ = handle_b.join().await;

    // A share that cannot hold the base fails the source's validation.
    let c = source(&db, &client, "cap_c", 1, Some(CAP - 1)).await?;
    let ck_c: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let (tx, _rx) = acked_channel(&c, &ck_c, &c.id, 64);
    let refused = tokio::time::timeout(
        Duration::from_secs(60),
        c.run(tx, Arc::clone(&ck_c)).await.join(),
    )
    .await?;
    let refused = refused.err().map(|e| format!("{e:#}")).unwrap_or_default();
    assert!(refused.contains("max_snapshot_connections"), "{refused}");

    for s in ["slot_cap_a", "slot_cap_b"] {
        client
            .execute("SELECT pg_drop_replication_slot($1)", &[&s])
            .await
            .ok();
    }
    pg_drop_db(&db).await;
    Ok(())
}
