//! MySQL startup reconciliation of stored checkpoints with the verified server
//! lineage, against a live server.
//!
//! A refused reconciliation must stop the source at startup - before snapshot
//! progress or the resume position is read - and rewrite nothing, for every
//! unsafe case, including the ones a resume fold cannot see (a single
//! checkpoint, or several pre-lineage ones). A failover predecessor's GTID
//! checkpoints are carried over only after their GTID sets are verified on the
//! current server.
//!
//! A recorded failover edge (predecessor -> current) is produced by recording a
//! made-up predecessor server UUID before the source first starts against the
//! real server.
//!
//! Run with:
//! ```bash
//! cargo test -p sources --test mysql_checkpoint_lineage_e2e -- --include-ignored --nocapture --test-threads=1
//! ```

use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use checkpoints::{CheckpointStore, MemCheckpointStore};
use common::AllowList;
use ctor::dtor;
use deltaforge_config::SnapshotCfg;
use deltaforge_core::{Event, Op, Source, SourceItem};
use mysql_async::prelude::Queryable;
use sources::MySqlCheckpoint;
use sources::mysql::MySqlSource;
use storage::ArcStorageBackend;
use storage::adapters::{LineageDescriptor, source_lineage};
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

const SOURCE_ID: &str = "ckpt-lineage";
const TENANT: &str = "acme";
/// A predecessor server that never existed: its GTIDs are not on the server.
const PREDECESSOR_UUID: &str = "9a8b7c6d-71ca-11e1-9e33-c80aa9429562";
const FOREIGN: &str = "0123456789abcdef0123456789abcdef";

fn predecessor_hash() -> String {
    LineageDescriptor::mysql(PREDECESSOR_UUID)
        .unwrap()
        .lineage_hash()
}

/// Binlog coordinates + executed GTID set of the live server.
async fn position(
    conn: &mut mysql_async::Conn,
) -> Result<(String, u64, String)> {
    let status: mysql_async::Row = conn
        .query_first("SHOW BINARY LOG STATUS")
        .await?
        .expect("binary log status");
    Ok((
        status.get("File").unwrap(),
        status.get("Position").unwrap(),
        status
            .get::<String, _>("Executed_Gtid_Set")
            .unwrap_or_default(),
    ))
}

fn checkpoint(
    file: &str,
    pos: u64,
    gtid: Option<&str>,
    lineage: Option<&str>,
) -> Vec<u8> {
    serde_json::to_vec(&MySqlCheckpoint {
        file: file.into(),
        pos,
        gtid_set: gtid.map(str::to_string),
        lineage: lineage.map(str::to_string),
        snapshot_completed: None,
    })
    .unwrap()
}

struct Started {
    events: Vec<Event>,
    /// `Some` when the source task ended within the window.
    ended: Option<anyhow::Result<()>>,
}

/// Start the source with `store` / `backend`, optionally insert a row once it
/// runs, and collect for `window`.
async fn start(
    dsn: &str,
    db: &str,
    store: Arc<dyn CheckpointStore>,
    backend: ArcStorageBackend,
    insert_after: Option<Duration>,
    window: Duration,
) -> Result<Started> {
    let src = MySqlSource {
        id: SOURCE_ID.into(),
        dsn: dsn.to_string().into(),
        tables: vec![format!("{db}.t")],
        tenant: TENANT.into(),
        pipeline: "ckpt-lineage".into(),
        registry: make_registry().await,
        registry_scope: sources::registry_scope::SharedRegistryScope::new(
            SOURCE_ID,
        ),
        backend,
        outbox_tables: AllowList::default(),
        snapshot_cfg: SnapshotCfg::default(),
        on_schema_drift: deltaforge_config::OnSchemaDrift::Adapt,
        table_options: Default::default(),
        rotation: None,
    };
    let (tx, mut rx) = mpsc::channel::<SourceItem>(128);
    let handle = src.run(tx, store).await;
    let started = Instant::now();
    let deadline = started + window;
    let mut inserted = false;
    let mut events = Vec::new();
    let mut closed = false;
    while Instant::now() < deadline {
        if let Some(after) = insert_after
            && !inserted
            && started.elapsed() >= after
        {
            let (_, pool, _) = mysql_setup("ckpt_lineage_writer").await?;
            let mut conn = pool.get_conn().await?;
            conn.query_drop(format!("INSERT INTO {db}.t VALUES (42, 'x')"))
                .await?;
            inserted = true;
        }
        match timeout(Duration::from_millis(250), rx.recv()).await {
            Ok(Some(SourceItem::Event(e))) => events.push(e),
            Ok(Some(_)) | Err(_) => continue,
            Ok(None) => {
                closed = true;
                break;
            }
        }
    }
    let ended = if closed {
        Some(handle.join().await)
    } else {
        handle.cancel.cancel();
        let _ = handle.join().await;
        None
    };
    Ok(Started { events, ended })
}

async fn snapshot(store: &MemCheckpointStore) -> Vec<(String, Vec<u8>)> {
    let mut out = Vec::new();
    for k in store.list().await.unwrap() {
        out.push((k.clone(), store.get_raw(&k).await.unwrap().unwrap()));
    }
    out.sort();
    out
}

/// Backend whose lineage record already holds the made-up predecessor, so
/// the source's first start records the edge predecessor -> live server.
async fn backend_with_predecessor() -> ArcStorageBackend {
    let backend = make_storage_backend().await;
    source_lineage::establish(
        &backend,
        TENANT,
        SOURCE_ID,
        LineageDescriptor::mysql(PREDECESSOR_UUID).unwrap(),
    )
    .await
    .unwrap();
    backend
}

async fn table(name: &str) -> Result<(String, String, mysql_async::Pool)> {
    let (db, pool, dsn) = mysql_setup(name).await?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop(format!(
        "CREATE TABLE {db}.t (id INT PRIMARY KEY, v VARCHAR(16))"
    ))
    .await?;
    Ok((db, dsn, pool))
}

/// The source stopped at startup with the checkpoint refusal, emitted no row,
/// and every stored checkpoint is byte-identical.
fn assert_refused(
    s: &Started,
    before: &[(String, Vec<u8>)],
    after: &[(String, Vec<u8>)],
) {
    let ended = s.ended.as_ref().expect("the source must stop at startup");
    let err = ended.as_ref().expect_err("startup must fail");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("cannot be attributed to the verified server lineage"),
        "unexpected error: {msg}"
    );
    assert!(
        !s.events
            .iter()
            .any(|e| matches!(e.op, Op::Create | Op::Update | Op::Delete)),
        "no row may be emitted"
    );
    assert_eq!(after, before, "no checkpoint may be rewritten");
}

async fn refused_case(
    name: &str,
    backend: ArcStorageBackend,
    seed: &[(&str, Vec<u8>)],
) -> Result<()> {
    let (db, dsn, pool) = table(name).await?;
    let store = Arc::new(MemCheckpointStore::new()?);
    for (k, v) in seed {
        store.put_raw(k, v).await?;
    }
    let before = snapshot(&store).await;
    let s = start(
        &dsn,
        &db,
        store.clone(),
        backend,
        None,
        Duration::from_secs(20),
    )
    .await?;
    assert_refused(&s, &before, &snapshot(&store).await);
    mysql_drop_db(&pool, &db).await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker"]
async fn one_legacy_aggregate_checkpoint_after_a_lineage_change_stops_startup()
-> Result<()> {
    let (_, pool, _) = mysql_setup("ckpt_lineage_pos").await?;
    let (file, pos, gtid) = position(&mut pool.get_conn().await?).await?;
    refused_case(
        "ckpt_legacy_one",
        backend_with_predecessor().await,
        &[(SOURCE_ID, checkpoint(&file, pos, Some(&gtid), None))],
    )
    .await
}

#[tokio::test]
#[ignore = "requires docker"]
async fn several_legacy_sink_checkpoints_after_a_lineage_change_stop_startup()
-> Result<()> {
    let (_, pool, _) = mysql_setup("ckpt_lineage_pos").await?;
    let (file, pos, gtid) = position(&mut pool.get_conn().await?).await?;
    refused_case(
        "ckpt_legacy_many",
        backend_with_predecessor().await,
        &[
            (
                "ckpt-lineage::sink::kafka",
                checkpoint(&file, pos, Some(&gtid), None),
            ),
            (
                "ckpt-lineage::sink::s3",
                checkpoint(&file, pos, Some(&gtid), None),
            ),
        ],
    )
    .await
}

#[tokio::test]
#[ignore = "requires docker"]
async fn one_foreign_lineage_checkpoint_stops_startup() -> Result<()> {
    let (_, pool, _) = mysql_setup("ckpt_lineage_pos").await?;
    let (file, pos, gtid) = position(&mut pool.get_conn().await?).await?;
    refused_case(
        "ckpt_foreign",
        make_storage_backend().await,
        &[(
            SOURCE_ID,
            checkpoint(&file, pos, Some(&gtid), Some(FOREIGN)),
        )],
    )
    .await
}

#[tokio::test]
#[ignore = "requires docker"]
async fn one_malformed_checkpoint_stops_startup() -> Result<()> {
    refused_case(
        "ckpt_malformed",
        make_storage_backend().await,
        &[(SOURCE_ID, b"{\"file\":\"x\"".to_vec())],
    )
    .await
}

/// The predecessor's GTID set was never executed by the live server.
#[tokio::test]
#[ignore = "requires docker"]
async fn unavailable_predecessor_gtids_stop_startup() -> Result<()> {
    let p = predecessor_hash();
    refused_case(
        "ckpt_unavailable",
        backend_with_predecessor().await,
        &[(
            SOURCE_ID,
            checkpoint(
                "binlog.000001",
                4,
                Some(&format!("{PREDECESSOR_UUID}:1-5")),
                Some(&p),
            ),
        )],
    )
    .await
}

/// Failover carry-over: the predecessor's GTID checkpoint names transactions
/// the live server has executed, so it is re-attributed to the live lineage
/// and the source resumes and streams.
#[tokio::test]
#[ignore = "requires docker"]
async fn verified_predecessor_gtids_are_carried_over_and_stream() -> Result<()>
{
    let (db, dsn, pool) = table("ckpt_carry").await?;
    let (file, pos, gtid) = position(&mut pool.get_conn().await?).await?;
    let p = predecessor_hash();
    let store = Arc::new(MemCheckpointStore::new()?);
    store
        .put_raw(SOURCE_ID, &checkpoint(&file, pos, Some(&gtid), Some(&p)))
        .await?;
    let backend = backend_with_predecessor().await;
    let s = start(
        &dsn,
        &db,
        store.clone(),
        backend.clone(),
        Some(Duration::from_secs(6)),
        Duration::from_secs(25),
    )
    .await?;
    assert!(
        s.ended.is_none(),
        "the source must keep running: {:?}",
        s.ended
    );
    assert!(
        s.events.iter().any(|e| e
            .after
            .as_ref()
            .and_then(|a| a.get("id"))
            .and_then(|v| v.as_i64())
            == Some(42)),
        "the source must stream after the carry-over"
    );
    let live = source_lineage::load(&backend, TENANT, SOURCE_ID)
        .await?
        .unwrap()
        .current
        .lineage_hash;
    let stored: MySqlCheckpoint =
        serde_json::from_slice(&store.get_raw(SOURCE_ID).await?.unwrap())?;
    assert_eq!(stored.lineage.as_deref(), Some(live.as_str()));
    mysql_drop_db(&pool, &db).await;
    Ok(())
}

/// A crash part-way through a carry-over leaves predecessor and current
/// checkpoints mixed; the next start finishes it.
#[tokio::test]
#[ignore = "requires docker"]
async fn mixed_predecessor_and_current_checkpoints_are_finished() -> Result<()>
{
    let (db, dsn, pool) = table("ckpt_mixed").await?;
    let (file, pos, gtid) = position(&mut pool.get_conn().await?).await?;
    let backend = backend_with_predecessor().await;
    // The edge predecessor -> live is recorded by the source's own startup;
    // compute the live hash from the server.
    let uuid: String = pool
        .get_conn()
        .await?
        .query_first("SELECT @@GLOBAL.server_uuid")
        .await?
        .unwrap();
    let live = LineageDescriptor::mysql(&uuid).unwrap().lineage_hash();
    let store = Arc::new(MemCheckpointStore::new()?);
    store
        .put_raw(
            "ckpt-lineage::sink::kafka",
            &checkpoint(&file, pos, Some(&gtid), Some(&live)),
        )
        .await?;
    store
        .put_raw(
            "ckpt-lineage::sink::s3",
            &checkpoint(&file, pos, Some(&gtid), Some(&predecessor_hash())),
        )
        .await?;
    let s = start(
        &dsn,
        &db,
        store.clone(),
        backend,
        None,
        Duration::from_secs(10),
    )
    .await?;
    assert!(
        s.ended.is_none(),
        "the source must keep running: {:?}",
        s.ended
    );
    for k in ["ckpt-lineage::sink::kafka", "ckpt-lineage::sink::s3"] {
        let cp: MySqlCheckpoint =
            serde_json::from_slice(&store.get_raw(k).await?.unwrap())?;
        assert_eq!(cp.lineage.as_deref(), Some(live.as_str()), "{k}");
        assert_eq!((cp.file.as_str(), cp.pos), (file.as_str(), pos), "{k}");
    }
    mysql_drop_db(&pool, &db).await;
    Ok(())
}
