//! MySQL: replaying retained binlog rows across a DDL fails closed.
//!
//! MySQL rows are decoded positionally against the table's schema. When a source
//! restarts from a binlog position that precedes a DDL, the retained rows were
//! written under the OLD table definition while the loaded schema is the NEW
//! one. Decoding them would put values under the wrong columns, so the source
//! must refuse: stop with a typed schema error, emit no event for those rows,
//! and leave the checkpoint where it was.
//!
//! Each case captures the binlog position, writes a row, applies a DDL, writes
//! again, then starts a fresh source at the captured position (so the live
//! schema is newer than the first retained row).
//!
//! Correct replay across a DDL requires historical schema resolution, which is
//! not implemented for MySQL yet; until then these are fail-closed regressions.
//!
//! Run with:
//! ```bash
//! cargo test -p sources --test mysql_ddl_replay_failclosed -- --include-ignored --nocapture --test-threads=1
//! ```

use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use checkpoints::{CheckpointStore, CheckpointStoreExt, MemCheckpointStore};
use common::AllowList;
use ctor::dtor;
use deltaforge_config::SnapshotCfg;
use deltaforge_core::{Event, Op, Source, SourceItem};
use mysql_async::prelude::Queryable;
use sources::MySqlCheckpoint;
use sources::mysql::MySqlSource;
use tokio::sync::mpsc;
use tokio::time::{Duration, timeout};

mod test_common;
use test_common::{
    make_registry, make_storage_backend, mysql_drop_db, mysql_setup,
};

#[dtor]
fn cleanup() {
    if let Some(container) = test_common::MYSQL_CONTAINER.get() {
        std::process::Command::new("docker")
            .args(["rm", "-f", container.0.id()])
            .output()
            .ok();
    }
}

const SOURCE_ID: &str = "ddl-replay";

/// Binlog coordinates + executed GTID set before the writes to replay.
async fn current_position(
    conn: &mut mysql_async::Conn,
) -> Result<MySqlCheckpoint> {
    let status: mysql_async::Row = conn
        .query_first("SHOW BINARY LOG STATUS")
        .await?
        .expect("binary log status");
    Ok(MySqlCheckpoint {
        lineage: None,
        file: status.get("File").unwrap(),
        pos: status.get("Position").unwrap(),
        gtid_set: status
            .get::<String, _>("Executed_Gtid_Set")
            .filter(|s| !s.is_empty()),
    })
}

struct Replay {
    events: Vec<Event>,
    result: anyhow::Result<()>,
    checkpoint_after: Option<MySqlCheckpoint>,
}

/// Start a fresh source at `checkpoint`, collect whatever it emits until it
/// stops (or a deadline), and report how it ended plus the stored checkpoint.
async fn replay(
    dsn: &str,
    db: &str,
    checkpoint: MySqlCheckpoint,
) -> Result<Replay> {
    let store: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    store.put(SOURCE_ID, checkpoint).await?;
    let src = MySqlSource {
        id: SOURCE_ID.into(),
        dsn: dsn.to_string().into(),
        tables: vec![format!("{db}.t")],
        tenant: "acme".into(),
        pipeline: "ddl-replay".into(),
        registry: make_registry().await,
        registry_scope: sources::registry_scope::SharedRegistryScope::new(
            SOURCE_ID,
        ),
        backend: make_storage_backend().await,
        outbox_tables: AllowList::default(),
        snapshot_cfg: SnapshotCfg::default(),
        on_schema_drift: deltaforge_config::OnSchemaDrift::Adapt,
        table_options: Default::default(),
        rotation: None,
    };

    let (tx, mut rx) = mpsc::channel::<SourceItem>(128);
    let handle = src.run(tx, Arc::clone(&store)).await;
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut events = Vec::new();
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match timeout(remaining, rx.recv()).await {
            Ok(Some(SourceItem::Event(e))) => events.push(e),
            Ok(Some(_)) => continue,
            // Channel closed: the source task has ended.
            Ok(None) | Err(_) => break,
        }
    }
    handle.cancel.cancel();
    let result = handle.join().await;
    let checkpoint_after = store.get::<MySqlCheckpoint>(SOURCE_ID).await?;
    Ok(Replay {
        events,
        result,
        checkpoint_after,
    })
}

/// The source refused the retained rows: typed error, no row events, and the
/// stored checkpoint is still the seeded one.
fn assert_failed_closed(r: &Replay, seeded: &MySqlCheckpoint) {
    let err = r
        .result
        .as_ref()
        .expect_err("replaying rows across a DDL must fail closed");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("written under a different table definition"),
        "unexpected error: {msg}"
    );
    let rows: Vec<_> = r
        .events
        .iter()
        .filter(|e| matches!(e.op, Op::Create | Op::Update | Op::Delete))
        .collect();
    assert!(
        rows.is_empty(),
        "no row may be emitted for undecodable rows, got {:?}",
        rows.iter().map(|e| &e.after).collect::<Vec<_>>()
    );
    // The position (file, position, and executed GTID set) is exactly the
    // seeded one: nothing past the refused rows was recorded. The seeded
    // checkpoint predates lineage, so startup adopted it into the verified
    // server lineage (only that field is added).
    let after = r
        .checkpoint_after
        .as_ref()
        .expect("the checkpoint is still stored");
    assert_eq!(
        (&after.file, after.pos, &after.gtid_set),
        (&seeded.file, seeded.pos, &seeded.gtid_set),
        "the checkpoint must not advance past the refused rows"
    );
    assert_eq!(seeded.lineage, None);
    let lineage = after.lineage.as_deref().expect("adopted lineage");
    assert!(
        lineage.len() == 32
            && lineage
                .bytes()
                .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')),
        "lineage {lineage}"
    );
}

#[tokio::test]
#[ignore = "requires docker"]
async fn replay_across_drop_column_fails_closed() -> Result<()> {
    let (db, pool, dsn) = mysql_setup("replay_drop_col").await?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop(format!("USE {db}")).await?;
    conn.query_drop(
        "CREATE TABLE t (id INT PRIMARY KEY, a VARCHAR(16), b VARCHAR(16))",
    )
    .await?;

    let checkpoint = current_position(&mut conn).await?;
    conn.query_drop("INSERT INTO t VALUES (1, 'A1', 'B1')")
        .await?;
    conn.query_drop("ALTER TABLE t DROP COLUMN a").await?;
    conn.query_drop("INSERT INTO t VALUES (2, 'B2')").await?;

    let r = replay(&dsn, &db, checkpoint.clone()).await?;
    mysql_drop_db(&pool, &db).await;
    assert_failed_closed(&r, &checkpoint);
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker"]
async fn replay_across_drop_and_recreate_fails_closed() -> Result<()> {
    let (db, pool, dsn) = mysql_setup("replay_recreate").await?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop(format!("USE {db}")).await?;
    conn.query_drop("CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(16))")
        .await?;

    let checkpoint = current_position(&mut conn).await?;
    conn.query_drop("INSERT INTO t VALUES (1, 'x')").await?;
    conn.query_drop("DROP TABLE t").await?;
    conn.query_drop(
        "CREATE TABLE t (id INT PRIMARY KEY, w INT, v VARCHAR(16))",
    )
    .await?;
    conn.query_drop("INSERT INTO t VALUES (2, 5, 'y')").await?;

    let r = replay(&dsn, &db, checkpoint.clone()).await?;
    mysql_drop_db(&pool, &db).await;
    assert_failed_closed(&r, &checkpoint);
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker"]
async fn replay_across_mid_table_add_column_fails_closed() -> Result<()> {
    let (db, pool, dsn) = mysql_setup("replay_add_after").await?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop(format!("USE {db}")).await?;
    conn.query_drop(
        "CREATE TABLE t (id INT PRIMARY KEY, a VARCHAR(16), b VARCHAR(16))",
    )
    .await?;

    let checkpoint = current_position(&mut conn).await?;
    conn.query_drop("INSERT INTO t VALUES (1, 'A1', 'B1')")
        .await?;
    conn.query_drop("ALTER TABLE t ADD COLUMN m VARCHAR(16) AFTER id")
        .await?;
    conn.query_drop("INSERT INTO t VALUES (2, 'M2', 'A2', 'B2')")
        .await?;

    let r = replay(&dsn, &db, checkpoint.clone()).await?;
    mysql_drop_db(&pool, &db).await;
    assert_failed_closed(&r, &checkpoint);
    Ok(())
}

/// Positive control: replay from an older position with NO intervening DDL is
/// unaffected by the check and decodes every row.
#[tokio::test]
#[ignore = "requires docker"]
async fn replay_without_ddl_decodes_normally() -> Result<()> {
    let (db, pool, dsn) = mysql_setup("replay_no_ddl").await?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop(format!("USE {db}")).await?;
    conn.query_drop(
        "CREATE TABLE t (id INT PRIMARY KEY, a VARCHAR(16), b VARCHAR(16))",
    )
    .await?;

    let checkpoint = current_position(&mut conn).await?;
    conn.query_drop("INSERT INTO t VALUES (1, 'A1', 'B1'), (2, 'A2', 'B2')")
        .await?;

    // Stop as soon as both rows arrive (the source keeps running otherwise).
    let store: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    store.put(SOURCE_ID, checkpoint).await?;
    let src = MySqlSource {
        id: SOURCE_ID.into(),
        dsn: dsn.clone().into(),
        tables: vec![format!("{db}.t")],
        tenant: "acme".into(),
        pipeline: "ddl-replay".into(),
        registry: make_registry().await,
        registry_scope: sources::registry_scope::SharedRegistryScope::new(
            SOURCE_ID,
        ),
        backend: make_storage_backend().await,
        outbox_tables: AllowList::default(),
        snapshot_cfg: SnapshotCfg::default(),
        on_schema_drift: deltaforge_config::OnSchemaDrift::Adapt,
        table_options: Default::default(),
        rotation: None,
    };
    let (tx, mut rx) = mpsc::channel::<SourceItem>(128);
    let handle = src.run(tx, store).await;
    let mut rows = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    while rows.len() < 2 && Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match timeout(remaining, rx.recv()).await {
            Ok(Some(SourceItem::Event(e))) if e.op == Op::Create => {
                rows.push(e)
            }
            Ok(Some(_)) => continue,
            Ok(None) | Err(_) => break,
        }
    }
    handle.cancel.cancel();
    let _ = handle.join().await;
    mysql_drop_db(&pool, &db).await;

    assert_eq!(rows.len(), 2, "both rows must be decoded");
    for (e, (a, b)) in rows.iter().zip([("A1", "B1"), ("A2", "B2")]) {
        let after = e.after.as_ref().unwrap();
        assert_eq!(after["a"], a);
        assert_eq!(after["b"], b);
    }
    Ok(())
}
