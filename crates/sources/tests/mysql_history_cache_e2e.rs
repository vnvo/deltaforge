//! Cached activation histories under mass invalidation
//! (`docs/design/mysql-mass-invalidation.md`, section 6): what known
//! tables cost the state store when a barrier lands or they return to the
//! stream, counted on the real storage path.
//!
//! Run with:
//! `cargo test -p sources --test mysql_history_cache_e2e -- --include-ignored --test-threads=1`

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use checkpoints::{CheckpointStore, MemCheckpointStore};
use deltaforge_config::{
    MysqlProofCfg, OnSchemaDrift, SnapshotCfg, SnapshotMode,
};
use deltaforge_core::{Source, SourceItem};
use gate_ownership::GateOwned;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
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
    proofs: MysqlProofCfg,
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
        proofs,
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

/// Every table delivered exactly `each` rows: none lost, none twice.
fn exactly(rows: &Delivered, tables: usize, each: usize) {
    let rows = rows.lock().unwrap();
    assert_eq!(rows.len(), tables, "every table delivered");
    let off: Vec<_> = rows.iter().filter(|(_, n)| **n != each).collect();
    assert!(off.is_empty(), "not {each} row(s) each: {off:?}");
}

/// The process's metrics, recorded for reading back (installed once).
fn recorded() -> &'static Snapshotter {
    static SNAP: OnceLock<Snapshotter> = OnceLock::new();
    SNAP.get_or_init(|| {
        let rec = DebuggingRecorder::new();
        let snap = rec.snapshotter();
        metrics::set_global_recorder(rec).expect("no other recorder");
        snap
    })
}

/// The selection cache's published bytes and timelines for source `id`.
fn cache_gauges(snap: &Snapshotter, id: &str) -> (u64, u64) {
    let (mut bytes, mut timelines) = (None, None);
    for (key, _, _, value) in snap.snapshot().into_vec() {
        let key = key.key();
        if !key.labels().any(|l| l.key() == "source" && l.value() == id) {
            continue;
        }
        let DebugValue::Gauge(v) = value else {
            continue;
        };
        match key.name() {
            "deltaforge_mysql_selection_cache_bytes" => {
                bytes = Some(v.into_inner() as u64)
            }
            "deltaforge_mysql_selection_cache_timelines" => {
                timelines = Some(v.into_inner() as u64)
            }
            _ => {}
        }
    }
    (
        bytes.expect("cache bytes published"),
        timelines.expect("cache timelines published"),
    )
}

/// Sample `id`'s published cache bytes every few milliseconds until
/// stopped; the highest sample.
fn sample_cache_bytes(
    snap: &'static Snapshotter,
    id: &'static str,
) -> (Arc<AtomicBool>, tokio::task::JoinHandle<u64>) {
    let stop = Arc::new(AtomicBool::new(false));
    let stopped = stop.clone();
    let task = tokio::spawn(async move {
        let mut peak = 0;
        while !stopped.load(Ordering::SeqCst) {
            peak = peak.max(cache_gauges(snap, id).0);
            sleep(Duration::from_millis(5)).await;
        }
        peak.max(cache_gauges(snap, id).0)
    });
    (stop, task)
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
    let (handle, rows) =
        run(port, "hist-barrier", &counting, MysqlProofCfg::default()).await;
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

/// Known tables the cache evicted return in batches. The source is held at
/// the first event of a burst of their next rows (separate transactions)
/// until the whole burst is in the binlog, on the same run and cache, so
/// every history load can take as many returning tables as the cache's
/// batch room allows: one bounded store read each, a few calls per batch,
/// every row delivered once, within the cache's bound. A further burst
/// arriving live is checked for delivery and bound only (how it batches
/// depends on how much of it has arrived when a table is reached).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn evicted_tables_return_in_batched_reads() {
    use sources::stream_probe::{Point, hold_after, reset_event_hooks};
    const N: usize = 300;
    const ID: &str = "hist-evict";
    // A cache of about 64 tables: most of them are evicted between bursts.
    const BOUND: u64 = 512 << 10;
    let snap = recorded();
    let (container, port) = start().await;
    let mut ddl = vec![format!("CREATE DATABASE {DB}")];
    ddl.extend((0..N).map(|i| {
        format!("CREATE TABLE {DB}.t{i:03} (id INT PRIMARY KEY, v INT)")
    }));
    sql(port, &ddl).await;
    let counting =
        Arc::new(CountingBackend::new(Arc::new(MemoryStorageBackend::new())));
    sources::stream_probe::set_selection_cache_bytes(Some(BOUND));
    // A read-ahead bounded by its tables, events and bytes, not by time:
    // the held burst is read from what the stream already holds.
    let proofs = MysqlProofCfg {
        activation_window_ms: *deltaforge_config::MYSQL_PROOF_WINDOW_MS.end(),
        ..Default::default()
    };
    let (handle, rows) = run(port, ID, &counting, proofs).await;
    sources::stream_probe::set_selection_cache_bytes(None);
    let burst = |id: usize| -> Vec<String> {
        (0..N)
            .map(|i| format!("INSERT INTO {DB}.t{i:03} VALUES ({id}, {id})"))
            .collect()
    };
    sql(port, &burst(1)).await;
    delivered(&rows, N, 1).await;
    exactly(&rows, N, 1);
    sleep(Duration::from_secs(1)).await;

    let (bytes, timelines) = cache_gauges(snap, ID);
    assert!(bytes <= BOUND, "{bytes} cached bytes over {BOUND}");
    assert!(
        (timelines as usize) < N / 2,
        "{timelines} of {N} tables cached: most are evicted"
    );

    // Held after the burst's first event until all of it is in the binlog.
    let (reached, release) = hold_after(Point::AfterGtid, 1);
    counting.reset();
    sources::stream_probe::record_history_batches(true);
    let (stop, peak) = sample_cache_bytes(snap, ID);
    sql(port, &burst(2)).await;
    tokio::time::timeout(Duration::from_secs(60), reached.notified())
        .await
        .expect("held at the burst's first transaction");
    release.notify_one();
    delivered(&rows, N, 2).await;
    reset_event_hooks();
    let batches = sources::stream_probe::history_batches();
    sources::stream_probe::record_history_batches(false);
    let ops: BTreeMap<&str, u64> = counting
        .ops()
        .into_iter()
        .map(|(p, s)| (p, s.calls))
        .collect();
    let (total, reads) = calls(&counting);
    exactly(&rows, N, 2);

    // The constraint every batch works under: a load admits at most half
    // the bound, each table with its version as one charge. With the
    // largest charge any batch weighed, a load that fills its half caches
    // at least `fill` tables; the burst then needs at most `ceiling`
    // batches, each one batched history read, provided every batch reads
    // at least `fill` tables (checked below, not assumed).
    let largest = batches.iter().map(|b| b.largest_charge).max().unwrap_or(1);
    let fill = (BOUND / 2 / largest).saturating_sub(1) as usize;
    assert!(fill >= 4, "{fill} tables per half bound at {largest} bytes");
    let ceiling = N.div_ceil(fill) as u64;
    eprintln!(
        "{N} held tables returning to a {BOUND}-byte cache ({timelines} \
         cached in {bytes} bytes): {total} store calls, {reads} stream reads; \
         largest charge {largest}, {fill} tables per half bound, ceiling \
         {ceiling}; by operation {ops:?}; batches (read, admitted, largest, \
         room), first 12 of {}: {:?}",
        batches.len(),
        batches
            .iter()
            .take(12)
            .map(|b| (b.read, b.admitted, b.largest_charge, b.room))
            .collect::<Vec<_>>()
    );

    // A live burst: delivered once each, within the bound.
    sql(port, &burst(3)).await;
    delivered(&rows, N, 3).await;
    exactly(&rows, N, 3);
    stop.store(true, Ordering::SeqCst);
    let peak = peak.await.unwrap();
    eprintln!("highest sampled cache bytes {peak}");

    handle.stop();
    let _ = tokio::time::timeout(Duration::from_secs(30), handle.join).await;
    container.rm().await.ok();
    assert!(peak <= BOUND, "{peak} cached bytes over {BOUND}");
    // No table's version read alone (an index lookup and a log read each).
    for op in ["kv_get", "log_read_meta_since"] {
        assert_eq!(ops.get(op), None, "{op}: versions read table by table");
    }
    // Every history read is a batch's, and a batch costs a few calls.
    let batched = ops.get("log_read_streams_bounded").copied().unwrap_or(0);
    assert_eq!(reads, batched, "only batched history reads: {ops:?}");
    assert_eq!(batched, batches.len() as u64, "one read per batch");
    assert!(reads > 0, "the returning tables were read: {total} calls");
    assert!(
        total <= 5 * reads,
        "{total} store calls in {reads} batched reads: a few per batch"
    );
    // The accounting: a batch that left tables uncached stopped at a charge
    // that did not fit what was left, so it cached at least its room over
    // the largest charge, less the one refused.
    for b in &batches {
        if b.admitted < b.read {
            let least = (b.room / b.largest_charge).saturating_sub(1) as usize;
            assert!(
                b.admitted >= least,
                "{b:?}: cached {} of a room for {least}",
                b.admitted
            );
        }
    }
    // Batches large enough for the ceiling: every batch but the last read
    // at least a half bound's worth of tables.
    if let Some((_, rest)) = batches.split_last() {
        for b in rest {
            assert!(b.read >= fill, "{b:?}: read fewer than {fill} tables");
        }
    }
    assert!(
        reads <= ceiling,
        "{reads} batched reads for {N} tables at {fill} per batch: \
         not batched (ceiling {ceiling})"
    );
}
