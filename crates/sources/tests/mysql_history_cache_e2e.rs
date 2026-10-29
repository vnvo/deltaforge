//! Cached activation histories under mass invalidation
//! (`docs/design/mysql-mass-invalidation.md`, section 6): what known
//! tables cost the state store when a barrier lands or they return to the
//! stream, counted on the real storage path.
//!
//! Run with:
//! `cargo test -p sources --test mysql_history_cache_e2e -- --include-ignored --test-threads=1`

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use checkpoints::{CheckpointStore, MemCheckpointStore};
use deltaforge_config::{OnSchemaDrift, SnapshotCfg, SnapshotMode};
use deltaforge_core::{Source, SourceItem};
use gate_ownership::GateOwned;
use mysql_async::prelude::Queryable;
use scale_harness::counting::CountingBackend;
use sources::MySqlSource;
use storage::{ArcStorageBackend, DurableSchemaRegistry, MemoryStorageBackend};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt, core::WaitFor, runners::AsyncRunner,
};
use tokio::time::{Duration, Instant, sleep};

mod test_common;

const DB: &str = "hist";

async fn start() -> (ContainerAsync<GenericImage>, u16) {
    let c = GenericImage::new("mysql", "8.4")
        .with_wait_for(WaitFor::message_on_stderr(
            "ready for connections. Version: '8.4",
        ))
        .with_env_var("MYSQL_ROOT_PASSWORD", "pw")
        .with_cmd([
            "--server-id=11",
            "--log-bin=/var/lib/mysql/mysql-bin.log",
            "--binlog-format=ROW",
            "--gtid-mode=ON",
            "--enforce-gtid-consistency=ON",
        ])
        .gate_owned()
        .start()
        .await
        .expect("start mysql");
    let port = c.get_host_port_ipv4(3306).await.unwrap();
    for _ in 0..60 {
        if mysql_async::Conn::from_url(dsn(port, "")).await.is_ok() {
            break;
        }
        sleep(Duration::from_millis(500)).await;
    }
    (c, port)
}

fn dsn(port: u16, db: &str) -> String {
    format!("mysql://root:pw@127.0.0.1:{port}/{db}")
}

async fn sql(port: u16, stmts: &[String]) {
    let mut c = mysql_async::Conn::from_url(dsn(port, "")).await.unwrap();
    for s in stmts {
        c.query_drop(s).await.unwrap_or_else(|e| panic!("{s}: {e}"));
    }
    c.disconnect().await.ok();
}

/// Rows delivered per table.
type Delivered = Arc<Mutex<BTreeMap<String, usize>>>;

/// Run a source over `counting` on every table of `DB`, counting the rows
/// it delivers per table.
async fn run(
    port: u16,
    id: &str,
    counting: &Arc<CountingBackend>,
) -> (deltaforge_core::SourceHandle, Delivered) {
    let backend: ArcStorageBackend = counting.clone();
    let src = MySqlSource {
        id: id.into(),
        dsn: dsn(port, DB).into(),
        tables: vec![format!("{DB}.*")],
        tenant: "acme".into(),
        pipeline: "hist".into(),
        registry: DurableSchemaRegistry::new(backend.clone()).await.unwrap(),
        registry_scope: sources::registry_scope::SharedRegistryScope::default(),
        outbox_tables: Default::default(),
        snapshot_cfg: SnapshotCfg {
            mode: SnapshotMode::Never,
            ..Default::default()
        },
        backend,
        on_schema_drift: OnSchemaDrift::Adapt,
        table_options: Default::default(),
        rotation: None,
        snapshot_cohort: Default::default(),
        proofs: Default::default(),
        type_mapping: deltaforge_core::TypeMapping::V2,
    };
    let ckpt: Arc<dyn CheckpointStore> =
        Arc::new(MemCheckpointStore::new().unwrap());
    let (tx, mut rx) = test_common::acked_channel(&src, &ckpt, id, 8192);
    let handle = src.run(tx, ckpt).await;
    let deadline = Instant::now() + Duration::from_secs(60);
    while !handle.ready.is_ready() {
        assert!(Instant::now() < deadline, "the source never streamed");
        sleep(Duration::from_millis(50)).await;
    }
    let delivered: Delivered = Default::default();
    let seen = delivered.clone();
    tokio::spawn(async move {
        while let Some(item) = rx.recv().await {
            if let SourceItem::Event(e) = item
                && e.ddl.is_none()
            {
                *seen
                    .lock()
                    .unwrap()
                    .entry(e.source.table.clone())
                    .or_default() += 1;
            }
        }
    });
    (handle, delivered)
}

async fn delivered(rows: &Delivered, tables: usize, each: usize) {
    let deadline = Instant::now() + Duration::from_secs(180);
    loop {
        let done = rows
            .lock()
            .unwrap()
            .values()
            .filter(|n| **n >= each)
            .count();
        if done == tables {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{done} of {tables} tables delivered {each} row(s)"
        );
        sleep(Duration::from_millis(50)).await;
    }
}

/// Every store call, and the stream reads among them.
fn calls(counting: &CountingBackend) -> (u64, u64) {
    let ops = counting.ops();
    let total = ops.values().map(|s| s.calls).sum();
    let reads = ops
        .iter()
        .filter(|(p, _)| p.starts_with("log_read"))
        .map(|(_, s)| s.calls)
        .sum();
    (total, reads)
}

/// A barrier of another database leaves every cached table of this one
/// cached: their next rows read no table stream again and cost the store
/// no per-table round trips (it used to clear every cached timeline).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn a_barrier_elsewhere_keeps_cached_histories() {
    const N: usize = 300;
    let (container, port) = start().await;
    let mut ddl = vec![
        format!("CREATE DATABASE {DB}"),
        "CREATE DATABASE other".to_string(),
    ];
    ddl.extend((0..N).map(|i| {
        format!("CREATE TABLE {DB}.t{i:03} (id INT PRIMARY KEY, v INT)")
    }));
    sql(port, &ddl).await;
    let counting =
        Arc::new(CountingBackend::new(Arc::new(MemoryStorageBackend::new())));
    let (handle, rows) = run(port, "hist-barrier", &counting).await;
    let burst = |id: usize| -> Vec<String> {
        (0..N)
            .map(|i| format!("INSERT INTO {DB}.t{i:03} VALUES ({id}, {id})"))
            .collect()
    };
    // Every table activated, proven and cached.
    sql(port, &burst(1)).await;
    delivered(&rows, N, 1).await;
    // A database barrier of another database.
    sql(
        port,
        &["ALTER DATABASE other CHARACTER SET utf8mb4".to_string()],
    )
    .await;
    sleep(Duration::from_secs(1)).await;
    counting.reset();
    sql(port, &burst(2)).await;
    delivered(&rows, N, 2).await;
    let (total, reads) = calls(&counting);
    eprintln!(
        "{N} cached tables after a barrier elsewhere: {total} store calls, \
         {reads} stream reads"
    );
    handle.stop();
    let _ = tokio::time::timeout(Duration::from_secs(30), handle.join).await;
    container.rm().await.ok();
    assert_eq!(reads, 0, "no cached table's history read again");
    assert!(total <= 20, "{total} store calls for {N} cached tables");
}

/// Known tables the cache evicted return in batches: a burst of their next
/// rows (separate transactions) costs the store a few calls per batch, not
/// a series of reads per table.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn evicted_tables_return_in_batched_reads() {
    const N: usize = 300;
    let (container, port) = start().await;
    let mut ddl = vec![format!("CREATE DATABASE {DB}")];
    ddl.extend((0..N).map(|i| {
        format!("CREATE TABLE {DB}.t{i:03} (id INT PRIMARY KEY, v INT)")
    }));
    sql(port, &ddl).await;
    let counting =
        Arc::new(CountingBackend::new(Arc::new(MemoryStorageBackend::new())));
    // A cache of 64 tables: most of them are evicted between the bursts.
    sources::stream_probe::set_selection_cache_limits(Some((64, 256 << 20)));
    let (handle, rows) = run(port, "hist-evict", &counting).await;
    sources::stream_probe::set_selection_cache_limits(None);
    let burst = |id: usize| -> Vec<String> {
        (0..N)
            .map(|i| format!("INSERT INTO {DB}.t{i:03} VALUES ({id}, {id})"))
            .collect()
    };
    sql(port, &burst(1)).await;
    delivered(&rows, N, 1).await;
    sleep(Duration::from_secs(1)).await;
    counting.reset();
    sql(port, &burst(2)).await;
    delivered(&rows, N, 2).await;
    let (total, reads) = calls(&counting);
    eprintln!(
        "{N} tables returning to a 64-table cache: {total} store calls, \
         {reads} stream reads"
    );
    handle.stop();
    let _ = tokio::time::timeout(Duration::from_secs(30), handle.join).await;
    container.rm().await.ok();
    // It used to be 7 serial calls per returning table (2,100 here).
    assert!(
        total <= (N / 4) as u64,
        "{total} store calls for {N} tables"
    );
}
