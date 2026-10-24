//! The activation window (`docs/design/mysql-proof-seek.md`, "Layer 1: the
//! activation window"), on a live MySQL 8.4: a burst of newly active
//! tables costs a bounded number of captures and scans, a schema change
//! closes a window, a capture that cannot lock fails closed, and an idle
//! stream's single first touch is not held for a heartbeat.
//!
//! Each test reads the proof trace (`deltaforge::proof_trace`) of its own
//! source: the `window`, `request`, `scan` and `proof` records.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use checkpoints::{CheckpointStore, MemCheckpointStore};
use deltaforge_config::{
    MysqlProofCfg, OnSchemaDrift, SnapshotCfg, SnapshotMode,
};
use deltaforge_core::incident::{EvidenceKey, ReasonCode};
use deltaforge_core::{Source, SourceHandle, SourceItem};
use gate_ownership::GateOwned;
use mysql_async::prelude::Queryable;
use sources::MySqlSource;
use storage::{DurableSchemaRegistry, MemoryStorageBackend};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt, core::WaitFor, runners::AsyncRunner,
};
use tokio::time::{Duration, sleep};

mod test_common;

const ROOT_PW: &str = "pw";
const DB: &str = "win";

/// The trace records, by source id.
static RECORDS: Mutex<Vec<serde_json::Value>> = Mutex::new(Vec::new());

struct Collect;

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Collect {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if event.metadata().target() != "deltaforge::proof_trace" {
            return;
        }
        struct Msg(String);
        impl tracing::field::Visit for Msg {
            fn record_debug(
                &mut self,
                f: &tracing::field::Field,
                v: &dyn std::fmt::Debug,
            ) {
                if f.name() == "message" {
                    self.0 = format!("{v:?}");
                }
            }
        }
        let mut m = Msg(String::new());
        event.record(&mut m);
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&m.0) {
            RECORDS.lock().unwrap().push(v);
        }
    }
}

fn install_trace() {
    use tracing_subscriber::Layer as _;
    use tracing_subscriber::layer::SubscriberExt;
    let _ = tracing::subscriber::set_global_default(
        tracing_subscriber::registry().with(Collect).with(
            tracing_subscriber::fmt::layer()
                .with_test_writer()
                .with_filter(tracing_subscriber::EnvFilter::new("warn")),
        ),
    );
}

/// The records of `kind` written for `source`.
fn records(source: &str, kind: &str) -> Vec<serde_json::Value> {
    RECORDS
        .lock()
        .unwrap()
        .iter()
        .filter(|r| r["record"] == kind && r["source"] == source)
        .cloned()
        .collect()
}

async fn start(gtid: bool) -> (ContainerAsync<GenericImage>, u16) {
    let mut cmd = vec![
        "--server-id=71".to_string(),
        "--log-bin=mysql-bin".into(),
        "--binlog-format=ROW".into(),
        "--binlog-row-image=FULL".into(),
    ];
    if gtid {
        cmd.push("--gtid-mode=ON".into());
        cmd.push("--enforce-gtid-consistency=ON".into());
    }
    let c = GenericImage::new("mysql", "8.4")
        .with_wait_for(WaitFor::message_on_stderr(
            "ready for connections. Version: '8.4",
        ))
        .with_env_var("MYSQL_ROOT_PASSWORD", ROOT_PW)
        .with_cmd(cmd)
        .gate_owned()
        .start()
        .await
        .expect("start mysql");
    let port = c.get_host_port_ipv4(3306).await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(90);
    while mysql_async::Conn::from_url(dsn(port, "")).await.is_err() {
        assert!(Instant::now() < deadline, "mysql on {port} not ready");
        sleep(Duration::from_millis(500)).await;
    }
    let mut c0 = mysql_async::Conn::from_url(dsn(port, "")).await.unwrap();
    c0.query_drop(format!("CREATE DATABASE {DB}"))
        .await
        .unwrap();
    (c, port)
}

fn dsn(port: u16, db: &str) -> String {
    format!("mysql://root:{ROOT_PW}@127.0.0.1:{port}/{db}")
}

async fn conn(port: u16) -> mysql_async::Conn {
    mysql_async::Conn::from_url(dsn(port, DB)).await.unwrap()
}

/// A running source of `DB.*`, at the tail, and the first row it delivered
/// of each table: table -> (id, v).
struct Running {
    id: String,
    handle: SourceHandle,
    firsts: Arc<Mutex<BTreeMap<String, (i64, i64)>>>,
    /// Every delivered row of the `t*` tables.
    rows: Arc<std::sync::atomic::AtomicUsize>,
    drain: tokio::task::JoinHandle<()>,
}

async fn run(port: u16, id: &str, proofs: MysqlProofCfg) -> Running {
    install_trace();
    let backend: storage::ArcStorageBackend =
        Arc::new(MemoryStorageBackend::new());
    let src = MySqlSource {
        id: id.into(),
        dsn: dsn(port, DB).into(),
        tables: vec![format!("{DB}.*")],
        tenant: "acme".into(),
        pipeline: "window".into(),
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
    let (tx, mut rx) = test_common::acked_channel(&src, &ckpt, &src.id, 8192);
    let handle = src.run(tx, Arc::clone(&ckpt)).await;
    let deadline = Instant::now() + Duration::from_secs(60);
    while !handle.ready.is_ready() {
        assert!(!handle.join.is_finished(), "the source ended");
        assert!(Instant::now() < deadline, "the source never streamed");
        sleep(Duration::from_millis(50)).await;
    }
    let firsts: Arc<Mutex<BTreeMap<String, (i64, i64)>>> = Default::default();
    let seen = firsts.clone();
    let rows: Arc<std::sync::atomic::AtomicUsize> = Default::default();
    let counted = rows.clone();
    let drain = tokio::spawn(async move {
        while let Some(item) = rx.recv().await {
            if let SourceItem::Event(e) = item
                && e.source.table.starts_with('t')
                && let Some(a) = e.after.as_ref().map(test_common::row_view)
            {
                counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                seen.lock()
                    .unwrap()
                    .entry(e.source.table.clone())
                    .or_insert((
                        a["id"].as_i64().unwrap(),
                        a["v"].as_i64().unwrap(),
                    ));
            }
        }
    });
    Running {
        id: id.into(),
        handle,
        firsts,
        rows,
        drain,
    }
}

impl Running {
    async fn delivered(&self, n: usize, within: Duration) {
        let deadline = Instant::now() + within;
        while self.firsts.lock().unwrap().len() < n {
            assert!(!self.handle.join.is_finished(), "the source ended");
            assert!(
                Instant::now() < deadline,
                "only {} of {n} tables delivered",
                self.firsts.lock().unwrap().len()
            );
            sleep(Duration::from_millis(20)).await;
        }
    }

    async fn stop(self) {
        self.handle.stop();
        let _ = tokio::time::timeout(Duration::from_secs(60), self.handle.join)
            .await;
        self.drain.abort();
    }
}

async fn tables(c: &mut mysql_async::Conn, n: usize) {
    for i in 0..n {
        c.query_drop(format!(
            "CREATE TABLE t{i:04} (id INT PRIMARY KEY, v INT NOT NULL)"
        ))
        .await
        .unwrap();
    }
}

fn count(r: &serde_json::Value, field: &str) -> u64 {
    r[field]
        .as_u64()
        .unwrap_or_else(|| panic!("{field} in {r}"))
}

/// 200 tables first touched in a burst: a few windows, each with one
/// capture transaction and at most two stream openings (one, at the stream
/// anchor, in GTID mode), and every table proven by a lazy baseline whose
/// rows arrive correctly decoded. Without windows this costs 200 captures
/// and 200 scan requests.
async fn burst(gtid: bool) {
    let (container, port) = start(gtid).await;
    let mut c = conn(port).await;
    let n = 200;
    tables(&mut c, n).await;
    let id = format!("burst-{gtid}");
    let r = run(port, &id, MysqlProofCfg::default()).await;
    r.handle.pause();
    for i in 0..n {
        c.query_drop(format!("INSERT INTO t{i:04} VALUES (1, {i})"))
            .await
            .unwrap();
    }
    r.handle.resume();
    r.delivered(n, Duration::from_secs(120)).await;
    let got = r.firsts.lock().unwrap().clone();
    for i in 0..n as i64 {
        assert_eq!(got.get(&format!("t{i:04}")), Some(&(1, i)), "t{i:04}");
    }
    r.stop().await;
    container.rm().await.ok();

    let windows = records(&id, "window");
    let tabled: u64 = windows.iter().map(|w| count(w, "tables")).sum();
    assert_eq!(tabled, n as u64, "every table in exactly one window");
    assert!(windows.len() <= 4, "{} windows: {windows:?}", windows.len());
    for w in &windows {
        assert_eq!(count(w, "capture_attempts"), 1, "{w}");
        assert!(count(w, "stream_openings") <= 2, "{w}");
        assert!(count(w, "extensions_committed") <= 2, "{w}");
        if gtid {
            assert_eq!(count(w, "unanchored_attempts"), 0, "{w}");
        }
    }
    let proven = records(&id, "proof")
        .iter()
        .filter(|p| p["outcome"] == "proven" && p["kind"] == "lazy")
        .count();
    assert_eq!(proven, n, "every table proven by its lazy baseline");
    let scans = records(&id, "scan")
        .iter()
        .filter(|s| s["outcome"] == "ok")
        .count();
    assert!(
        scans <= 2 * windows.len(),
        "{scans} scans for {} windows",
        windows.len()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn a_burst_is_batched_gtid() {
    burst(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn a_burst_is_batched_file_position() {
    burst(false).await;
}

/// A schema change closes the window before it: the tables touched before
/// it are one batch, those touched after it are proven after it (the
/// changed table by its DDL's forward proof), all delivered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn a_schema_change_closes_the_window() {
    let (container, port) = start(true).await;
    let mut c = conn(port).await;
    tables(&mut c, 20).await;
    let id = "statement";
    let r = run(port, id, MysqlProofCfg::default()).await;
    r.handle.pause();
    for i in 0..10 {
        c.query_drop(format!("INSERT INTO t{i:04} VALUES (1, {i})"))
            .await
            .unwrap();
    }
    c.query_drop("ALTER TABLE t0015 ADD COLUMN w INT")
        .await
        .unwrap();
    for i in 10..20 {
        c.query_drop(format!("INSERT INTO t{i:04} (id, v) VALUES (1, {i})"))
            .await
            .unwrap();
    }
    r.handle.resume();
    r.delivered(20, Duration::from_secs(120)).await;
    let got = r.firsts.lock().unwrap().clone();
    for i in 0..20 {
        assert_eq!(got.get(&format!("t{i:04}")), Some(&(1, i)), "t{i:04}");
    }
    r.stop().await;
    container.rm().await.ok();
    let windows = records(id, "window");
    assert_eq!(windows[0]["closed_by"], "statement", "{windows:?}");
    assert_eq!(count(&windows[0], "tables"), 10, "{windows:?}");
    // The changed table is proven by the forward proof of its DDL before
    // its rows: no window needs it.
    let forward = records(id, "proof")
        .iter()
        .filter(|p| {
            p["table"] == "t0015"
                && p["kind"] == "forward"
                && p["outcome"] == "proven"
        })
        .count();
    assert_eq!(forward, 1);
    let tabled: u64 = windows.iter().map(|w| count(w, "tables")).sum();
    assert_eq!(tabled, 19, "{windows:?}");
}

/// A batch table write-locked throughout: three sorted capture attempts,
/// then a `mysql_capture_contention` incident; nothing of the window is
/// delivered and no per-table capture is made.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn capture_contention_fails_closed() {
    let (container, port) = start(true).await;
    let mut c = conn(port).await;
    tables(&mut c, 3).await;
    let id = "contention";
    let r = run(
        port,
        id,
        MysqlProofCfg {
            capture_lock_wait_secs: 1,
            ..Default::default()
        },
    )
    .await;
    r.handle.pause();
    for i in 0..3 {
        c.query_drop(format!("INSERT INTO t{i:04} VALUES (1, {i})"))
            .await
            .unwrap();
    }
    // Held by another session until the test ends.
    let mut locker = conn(port).await;
    locker.query_drop("LOCK TABLES t0001 WRITE").await.unwrap();
    r.handle.resume();
    let ended = tokio::time::timeout(Duration::from_secs(60), r.handle.join)
        .await
        .expect("the source stops")
        .expect("the source task completes");
    let err = ended.expect_err("the window fails closed");
    let draft = err.draft().expect("an incident");
    assert_eq!(
        draft.reason_code,
        ReasonCode::MysqlCaptureContention,
        "{err}"
    );
    assert_eq!(
        draft.evidence.text_of(EvidenceKey::ReasonClass),
        Some("lock_wait_timeout")
    );
    assert!(r.firsts.lock().unwrap().is_empty(), "nothing of the window");
    assert!(records(&r.id, "window").is_empty(), "no window completed");
    assert!(
        records(&r.id, "proof").is_empty(),
        "no per-table capture or proof"
    );
    r.drain.abort();
    locker.query_drop("UNLOCK TABLES").await.unwrap();
    container.rm().await.ok();
}

/// On an idle stream, a single first touch closes its window by time: its
/// rows arrive in well under the stream's 15 s heartbeat.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn an_idle_first_touch_is_not_held_for_a_heartbeat() {
    let (container, port) = start(true).await;
    let mut c = conn(port).await;
    tables(&mut c, 1).await;
    let id = "idle";
    let r = run(port, id, MysqlProofCfg::default()).await;
    sleep(Duration::from_secs(1)).await;
    let t = Instant::now();
    c.query_drop("INSERT INTO t0000 VALUES (1, 0)")
        .await
        .unwrap();
    r.delivered(1, Duration::from_secs(10)).await;
    let latency = t.elapsed();
    r.stop().await;
    container.rm().await.ok();
    assert!(latency < Duration::from_secs(3), "{latency:?}");
    let windows = records(id, "window");
    assert_eq!(windows.len(), 1, "{windows:?}");
    assert_eq!(windows[0]["closed_by"], "time", "{windows:?}");
}

/// Window 1 (`t0000`) waits for a lock while the other 19 tables arrive in
/// one transaction; `drop` cuts the stream right after window 1's
/// transaction is dispatched, with window 2's events held. Every table's
/// row arrives exactly once. Returns the source's window records.
async fn admission_meanwhile(id: &str, drop: bool) -> Vec<serde_json::Value> {
    let (container, port) = start(true).await;
    let mut c = conn(port).await;
    let n = 20;
    tables(&mut c, n).await;
    sources::stream_probe::reset_streams_opened();
    let r = run(port, id, MysqlProofCfg::default()).await;
    r.handle.pause();
    c.query_drop("INSERT INTO t0000 VALUES (1, 0)")
        .await
        .unwrap();
    let mut locker = conn(port).await;
    locker.query_drop("LOCK TABLES t0000 WRITE").await.unwrap();
    if drop {
        sources::stream_probe::reset_event_hooks();
        sources::stream_probe::disconnect_after(
            sources::stream_probe::Point::AfterXid,
            1,
        );
    }
    r.handle.resume();
    // Window 1 has closed and its capture waits for the lock.
    sleep(Duration::from_millis(500)).await;
    let mut txn = vec!["BEGIN".to_string()];
    for i in 1..n {
        txn.push(format!("INSERT INTO t{i:04} VALUES (1, {i})"));
    }
    txn.push("COMMIT".to_string());
    c.query_drop(txn.join("; ")).await.unwrap();
    sleep(Duration::from_millis(300)).await;
    locker.query_drop("UNLOCK TABLES").await.unwrap();
    r.delivered(n, Duration::from_secs(60)).await;
    // Anything delivered twice would have arrived by now.
    sleep(Duration::from_secs(2)).await;
    let got = r.firsts.lock().unwrap().clone();
    for i in 0..n as i64 {
        assert_eq!(got.get(&format!("t{i:04}")), Some(&(1, i)), "t{i:04}");
    }
    assert_eq!(
        r.rows.load(std::sync::atomic::Ordering::SeqCst),
        n,
        "every row exactly once"
    );
    assert_eq!(
        sources::stream_probe::streams_opened(),
        if drop { 2 } else { 1 },
        "streams opened"
    );
    sources::stream_probe::reset_event_hooks();
    r.stop().await;
    container.rm().await.ok();
    records(id, "window")
}

/// The stream is read while a window is admitted: tables first touched
/// meanwhile are held with when they were received, and the next window
/// takes them without waiting again for its time bound, which counts from
/// its first event's receipt: window 2 has all 19 tables and closes by
/// time at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn tables_touched_during_an_admission_are_prefetched() {
    let n = 20;
    let windows = admission_meanwhile("prefetch", false).await;
    assert_eq!(windows.len(), 2, "{windows:?}");
    assert_eq!(count(&windows[0], "tables"), 1, "{windows:?}");
    assert!(
        count(&windows[0], "prefetched") >= (n - 1),
        "the second window's events read during the first's admission: \
         {windows:?}"
    );
    assert_eq!(count(&windows[1], "tables"), n - 1, "{windows:?}");
    assert_eq!(windows[1]["closed_by"], "time", "{windows:?}");
    let read_ms = windows[1]["read_ms"].as_f64().unwrap();
    assert!(
        read_ms < 25.0,
        "the second window does not wait again for its bound: {read_ms} ms"
    );
}

/// Prefetched events go with a dropped stream: the resume reads them from
/// the server again, and none is delivered twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn prefetched_events_go_with_a_dropped_stream() {
    let windows = admission_meanwhile("prefetch-drop", true).await;
    assert!(count(&windows[0], "prefetched") > 0, "{windows:?}");
    let tabled: u64 = windows.iter().map(|w| count(w, "tables")).sum();
    assert_eq!(tabled, 20, "{windows:?}");
}
