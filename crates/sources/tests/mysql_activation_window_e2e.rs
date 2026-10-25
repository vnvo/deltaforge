//! The activation window (`docs/design/mysql-proof-seek.md`, "Layer 1: the
//! activation window"), on a live MySQL 8.4: a burst of newly active
//! tables costs a bounded number of captures and scans, a schema change
//! closes a window, a capture that cannot lock fails closed, and an idle
//! stream's single first touch is not held for a heartbeat.
//!
//! The speculative window (revision 5): the next window is captured and
//! scanned while the current one is admitted, and decides only after it,
//! in order; failures, cancellation and disconnects with two windows
//! present; a DDL spanning both windows' tables; held rows governed by the
//! decision that covers them.
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
use storage::adapters::test_util::FaultBackend;
use storage::adapters::{LineageDescriptor, SchemaKey};
use storage::{
    ArcStorageBackend, BatchFault, BatchFaults, DurableSchemaRegistry,
    MemoryStorageBackend,
};
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
    start_with(gtid, false).await
}

/// `full`: `binlog_row_metadata = FULL` (rows without a positional proof
/// are then selected by their FULL signature instead of stopping).
async fn start_with(
    gtid: bool,
    full: bool,
) -> (ContainerAsync<GenericImage>, u16) {
    let mut cmd = vec![
        "--server-id=71".to_string(),
        "--log-bin=mysql-bin".into(),
        "--binlog-format=ROW".into(),
        "--binlog-row-image=FULL".into(),
    ];
    if full {
        cmd.push("--binlog-row-metadata=FULL".into());
    }
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

/// A source's state store (registry and activations in one) and
/// checkpoints, kept across its runs.
#[derive(Clone)]
struct Store {
    backend: ArcStorageBackend,
    ckpt: Arc<dyn CheckpointStore>,
}

impl Store {
    fn on(backend: ArcStorageBackend) -> Self {
        Self {
            backend,
            ckpt: Arc::new(MemCheckpointStore::new().unwrap()),
        }
    }

    fn memory() -> Self {
        Self::on(Arc::new(MemoryStorageBackend::new()))
    }

    /// The table's activation records (seq, record) under `port`'s lineage.
    async fn activation(
        &self,
        port: u16,
        id: &str,
        table: &str,
    ) -> Vec<(u64, serde_json::Value)> {
        let key = lineage_key(port, id, table).await.backend_key();
        self.backend
            .log_list("schemas.v1.activation", &key)
            .await
            .unwrap()
            .into_iter()
            .map(|(seq, b)| (seq, serde_json::from_slice(&b).unwrap()))
            .collect()
    }

    /// Whether the table has a registered schema under `port`'s lineage.
    async fn registered(&self, port: u16, id: &str, table: &str) -> bool {
        DurableSchemaRegistry::new(self.backend.clone())
            .await
            .unwrap()
            .get_latest(&lineage_key(port, id, table).await)
            .await
            .unwrap()
            .is_some()
    }
}

async fn lineage_key(port: u16, id: &str, table: &str) -> SchemaKey {
    let mut c = conn(port).await;
    let uuid: String = c
        .query_first("SELECT @@GLOBAL.server_uuid")
        .await
        .unwrap()
        .unwrap();
    let hash = LineageDescriptor::mysql(&uuid).unwrap().lineage_hash();
    SchemaKey::new("acme", id, &hash, DB, table)
}

/// A running source of `DB.*`, at the tail, and the first row it delivered
/// of each table: table -> (id, v).
struct Running {
    id: String,
    handle: SourceHandle,
    firsts: Arc<Mutex<BTreeMap<String, (i64, i64)>>>,
    /// Every delivered row of the `t*` tables.
    rows: Arc<std::sync::atomic::AtomicUsize>,
    /// Every delivered row of the `t*` tables, in delivery order:
    /// (table, id).
    order: Arc<Mutex<Vec<(String, i64)>>>,
    drain: tokio::task::JoinHandle<()>,
}

async fn run(port: u16, id: &str, proofs: MysqlProofCfg) -> Running {
    run_on(port, id, proofs, &Store::memory()).await
}

async fn run_on(
    port: u16,
    id: &str,
    proofs: MysqlProofCfg,
    store: &Store,
) -> Running {
    install_trace();
    let backend = store.backend.clone();
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
    let ckpt = store.ckpt.clone();
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
    let order: Arc<Mutex<Vec<(String, i64)>>> = Default::default();
    let ordered = order.clone();
    let drain = tokio::spawn(async move {
        while let Some(item) = rx.recv().await {
            if let SourceItem::Event(e) = item
                && e.source.table.starts_with('t')
                && let Some(a) = e.after.as_ref().map(test_common::row_view)
            {
                counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                ordered
                    .lock()
                    .unwrap()
                    .push((e.source.table.clone(), a["id"].as_i64().unwrap()));
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
        order,
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
    // Exactly the bounded attempts, never a fourth.
    assert!(
        matches!(
            draft.evidence.get(EvidenceKey::Attempts),
            Some(deltaforge_core::incident::EvidenceValue::Count { value: 3 })
        ),
        "{:?}",
        draft.evidence.get(EvidenceKey::Attempts)
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
    let expected: Vec<(String, i64)> =
        (0..n).map(|i| (format!("t{i:04}"), 1)).collect();
    assert_eq!(*r.order.lock().unwrap(), expected, "stream order");
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
    // The second window was captured during the first's admission, its
    // scan positioned at its own head's stream anchor (never unanchored).
    assert_eq!(windows[0]["next"], "formed", "{windows:?}");
    assert_eq!(count(&windows[1], "speculated"), n - 1, "{windows:?}");
    assert_eq!(count(&windows[1], "unanchored_attempts"), 0, "{windows:?}");
    // Measurement records: each table's first row released once, with its
    // window and admission rank.
    let releases = records("prefetch", "release");
    assert_eq!(releases.len() as u64, n, "{releases:?}");
    let second: Vec<u64> = releases
        .iter()
        .filter(|r| r["window"] == windows[1]["window_seq"])
        .map(|r| count(r, "rank"))
        .collect();
    assert_eq!(second, (0..n - 1).collect::<Vec<_>>(), "{releases:?}");
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
    // Two windows were present when the stream dropped.
    assert_eq!(windows[0]["next"], "formed", "{windows:?}");
    let tabled: u64 = windows.iter().map(|w| count(w, "tables")).sum();
    assert_eq!(tabled, 20, "{windows:?}");
}

/// Sessions waiting for a metadata lock on `table` (a blocked capture).
async fn mdl_waiters(port: u16, table: &str) -> Vec<u64> {
    let mut c = conn(port).await;
    let ids: Vec<u64> = c
        .query(format!(
            "SELECT ID FROM information_schema.PROCESSLIST \
             WHERE STATE = 'Waiting for table metadata lock' \
             AND INFO LIKE '%{table}%'"
        ))
        .await
        .unwrap();
    c.disconnect().await.ok();
    ids
}

async fn wait_for_waiter(port: u16, table: &str) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(id) = mdl_waiters(port, table).await.first() {
            return *id;
        }
        assert!(Instant::now() < deadline, "no capture waits on {table}");
        sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_no_waiter(port: u16, table: &str) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !mdl_waiters(port, table).await.is_empty() {
        assert!(
            Instant::now() < deadline,
            "a capture still waits on {table}"
        );
        sleep(Duration::from_millis(100)).await;
    }
}

/// Kill the capture session waiting on `table`, `times` times (each
/// attempt of a capture fails as a lost connection).
async fn kill_waiting(port: u16, table: &str, times: usize) {
    let mut c = conn(port).await;
    for _ in 0..times {
        let id = wait_for_waiter(port, table).await;
        c.query_drop(format!("KILL {id}")).await.ok();
        // The next attempt is a new session.
        let deadline = Instant::now() + Duration::from_secs(10);
        while mdl_waiters(port, table).await.contains(&id) {
            assert!(Instant::now() < deadline, "{id} survived KILL");
            sleep(Duration::from_millis(20)).await;
        }
    }
}

/// What a source left when it ended.
struct Ended {
    rows: Arc<std::sync::atomic::AtomicUsize>,
    order: Arc<Mutex<Vec<(String, i64)>>>,
    drain: tokio::task::JoinHandle<()>,
}

/// The source's end, within `within`.
async fn ended(
    r: Running,
    within: Duration,
) -> (Ended, deltaforge_core::SourceResult<()>) {
    let end = tokio::time::timeout(within, r.handle.join)
        .await
        .expect("the source stops")
        .expect("the source task completes");
    // Let the drain take what was sent before the end.
    sleep(Duration::from_millis(500)).await;
    (
        Ended {
            rows: r.rows,
            order: r.order,
            drain: r.drain,
        },
        end,
    )
}

/// Window 1 (`t0000`) waits for a lock; the next window's tables
/// `t0001..t0004` arrive meanwhile, and `t0004` is locked so the next
/// window's capture waits too. Returns the lockers of `t0000` and `t0004`.
async fn two_windows_present(
    port: u16,
    r: &Running,
    c: &mut mysql_async::Conn,
) -> (mysql_async::Conn, mysql_async::Conn) {
    r.handle.pause();
    c.query_drop("INSERT INTO t0000 VALUES (1, 0)")
        .await
        .unwrap();
    let mut first = conn(port).await;
    first.query_drop("LOCK TABLES t0000 WRITE").await.unwrap();
    r.handle.resume();
    wait_for_waiter(port, "t0000").await;
    c.query_drop(
        "BEGIN; INSERT INTO t0001 VALUES (1, 1); INSERT INTO t0002 VALUES (1, 2); \
         INSERT INTO t0003 VALUES (1, 3); INSERT INTO t0004 VALUES (1, 4); COMMIT",
    )
    .await
    .unwrap();
    let mut second = conn(port).await;
    second.query_drop("LOCK TABLES t0004 WRITE").await.unwrap();
    wait_for_waiter(port, "t0004").await;
    (first, second)
}

/// The first window fails before its commit (the store rejects it) while
/// the second window's capture is in flight: the speculative capture is
/// aborted (its metadata-lock wait ends at once), nothing of either window
/// is delivered or written, and no schema or activation record exists for
/// the second window's tables.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn a_failed_first_window_aborts_the_speculative_capture() {
    let (container, port) = start(true).await;
    let mut c = conn(port).await;
    tables(&mut c, 5).await;
    let id = "spec-fail-first";
    let faults = Arc::new(FaultBackend::new());
    let store = Store::on(faults.clone());
    let r = run_on(port, id, MysqlProofCfg::default(), &store).await;
    let (mut first, mut second) = two_windows_present(port, &r, &mut c).await;
    faults
        .fail_writes_to
        .lock()
        .unwrap()
        .push("schemas.v1.activation".into());
    first.query_drop("UNLOCK TABLES").await.unwrap();
    let (r, end) = ended(r, Duration::from_secs(60)).await;
    assert!(end.is_err(), "the first window fails closed");
    wait_for_no_waiter(port, "t0004").await;
    assert_eq!(r.rows.load(std::sync::atomic::Ordering::SeqCst), 0);
    for i in 0..5 {
        let t = format!("t{i:04}");
        assert!(store.activation(port, id, &t).await.is_empty(), "{t}");
    }
    for i in 1..5 {
        let t = format!("t{i:04}");
        assert!(
            !store.registered(port, id, &t).await,
            "no speculative registration of {t}"
        );
    }
    second.query_drop("UNLOCK TABLES").await.unwrap();
    r.drain.abort();
    container.rm().await.ok();
}

/// The first window's commit succeeds but its acknowledgement is lost:
/// the read-back resolves it (with its original sequences) before the
/// second window decides, whose records then follow it. Each table has
/// exactly one activation record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn a_lost_first_commit_ack_is_resolved_before_the_next_window_decides() {
    let (container, port) = start(true).await;
    let mut c = conn(port).await;
    tables(&mut c, 5).await;
    let id = "spec-lost-ack";
    let memory = Arc::new(MemoryStorageBackend::new());
    let store = Store::on(memory.clone());
    let r = run_on(port, id, MysqlProofCfg::default(), &store).await;
    memory.set_batch_fault(Some(BatchFault::AfterCommit));
    let (mut first, mut second) = two_windows_present(port, &r, &mut c).await;
    second.query_drop("UNLOCK TABLES").await.unwrap();
    first.query_drop("UNLOCK TABLES").await.unwrap();
    r.delivered(5, Duration::from_secs(60)).await;
    assert!(
        !memory.batch_fault_armed(),
        "the first window's commit lost its acknowledgement"
    );
    let firsts = store.activation(port, id, "t0000").await;
    assert_eq!(firsts.len(), 1, "{firsts:?}");
    for i in 1..5 {
        let t = format!("t{i:04}");
        let records = store.activation(port, id, &t).await;
        assert_eq!(records.len(), 1, "{t}: {records:?}");
        assert!(records[0].0 > firsts[0].0, "{t} after the first window");
    }
    assert_eq!(r.rows.load(std::sync::atomic::Ordering::SeqCst), 5);
    r.stop().await;
    container.rm().await.ok();
    let windows = records(id, "window");
    assert_eq!(windows[0]["next"], "formed", "{windows:?}");
    assert_eq!(count(&windows[1], "speculated"), 4, "{windows:?}");
}

/// The second window's capture fails (each attempt's session killed)
/// before the first window commits: the first window is still admitted
/// and delivered, and the failure surfaces only then, with its class (a
/// lost connection); nothing of the second window is delivered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn a_speculative_failure_surfaces_in_order_before_the_first_commit() {
    let (container, port) = start(true).await;
    let mut c = conn(port).await;
    tables(&mut c, 5).await;
    let id = "spec-fail-before";
    let r = run(port, id, MysqlProofCfg::default()).await;
    let (mut first, mut second) = two_windows_present(port, &r, &mut c).await;
    kill_waiting(port, "t0004", 3).await;
    first.query_drop("UNLOCK TABLES").await.unwrap();
    let (r, end) = ended(r, Duration::from_secs(60)).await;
    let err = end.expect_err("the second window fails closed");
    assert!(
        matches!(err.root(), deltaforge_core::SourceError::Connect { .. }),
        "a lost connection keeps its class: {err}"
    );
    assert_eq!(
        *r.order.lock().unwrap(),
        vec![("t0000".to_string(), 1)],
        "the first window delivered, nothing of the second"
    );
    second.query_drop("UNLOCK TABLES").await.unwrap();
    r.drain.abort();
    container.rm().await.ok();
}

/// The second window's capture fails after the first window committed and
/// was delivered. Meanwhile nothing of the second window was written:
/// no registration, no activation record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn a_speculative_failure_surfaces_in_order_after_the_first_commit() {
    let (container, port) = start(true).await;
    let mut c = conn(port).await;
    tables(&mut c, 5).await;
    let id = "spec-fail-after";
    let store = Store::memory();
    let r = run_on(port, id, MysqlProofCfg::default(), &store).await;
    let (mut first, mut second) = two_windows_present(port, &r, &mut c).await;
    first.query_drop("UNLOCK TABLES").await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    while r.order.lock().unwrap().is_empty() {
        assert!(Instant::now() < deadline, "the first window delivered");
        sleep(Duration::from_millis(20)).await;
    }
    // No speculative mutation while the second window waits.
    assert_eq!(store.activation(port, id, "t0000").await.len(), 1);
    for i in 1..5 {
        let t = format!("t{i:04}");
        assert!(store.activation(port, id, &t).await.is_empty(), "{t}");
        assert!(!store.registered(port, id, &t).await, "{t}");
    }
    kill_waiting(port, "t0004", 3).await;
    let (r, end) = ended(r, Duration::from_secs(60)).await;
    let err = end.expect_err("the second window fails closed");
    assert!(
        matches!(err.root(), deltaforge_core::SourceError::Connect { .. }),
        "{err}"
    );
    assert_eq!(*r.order.lock().unwrap(), vec![("t0000".to_string(), 1)]);
    second.query_drop("UNLOCK TABLES").await.unwrap();
    r.drain.abort();
    container.rm().await.ok();
}

/// A table of the first window (`t0000`) gets more rows while it is
/// reserved: they follow its decision (admitted), and the second window,
/// which cannot take it, is still used. `t0009` holds the first window.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn a_reserved_tables_later_rows_follow_its_decision() {
    let (container, port) = start(true).await;
    let mut c = conn(port).await;
    tables(&mut c, 10).await;
    let id = "spec-reserved";
    let r = run(port, id, MysqlProofCfg::default()).await;
    r.handle.pause();
    c.query_drop("INSERT INTO t0000 VALUES (1, 0)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO t0009 VALUES (1, 9)")
        .await
        .unwrap();
    let mut locker = conn(port).await;
    locker.query_drop("LOCK TABLES t0009 WRITE").await.unwrap();
    r.handle.resume();
    wait_for_waiter(port, "t0009").await;
    c.query_drop("INSERT INTO t0000 VALUES (2, 0)")
        .await
        .unwrap();
    c.query_drop(
        "BEGIN; INSERT INTO t0001 VALUES (1, 1); INSERT INTO t0002 VALUES (1, 2); \
         INSERT INTO t0003 VALUES (1, 3); COMMIT",
    )
    .await
    .unwrap();
    sleep(Duration::from_millis(300)).await;
    locker.query_drop("UNLOCK TABLES").await.unwrap();
    r.delivered(5, Duration::from_secs(60)).await;
    sleep(Duration::from_secs(1)).await;
    let order = r.order.lock().unwrap().clone();
    let expected: Vec<(String, i64)> = [
        ("t0000", 1),
        ("t0009", 1),
        ("t0000", 2),
        ("t0001", 1),
        ("t0002", 1),
        ("t0003", 1),
    ]
    .iter()
    .map(|(t, i)| (t.to_string(), *i))
    .collect();
    assert_eq!(order, expected);
    r.stop().await;
    container.rm().await.ok();
    let windows = records(id, "window");
    assert_eq!(count(&windows[0], "tables"), 2, "{windows:?}");
    assert_eq!(windows[0]["next"], "formed", "{windows:?}");
    assert_eq!(count(&windows[1], "tables"), 3, "{windows:?}");
    assert_eq!(count(&windows[1], "speculated"), 3, "{windows:?}");
    let t0000 = records(id, "proof")
        .iter()
        .filter(|p| p["table"] == "t0000" && p["kind"] == "lazy")
        .count();
    assert_eq!(t0000, 1, "t0000 decided once, never captured again");
}

/// A stop with two windows present: the admission is dropped, the
/// speculative capture aborted (no metadata-lock wait remains), and a
/// restart from the same store delivers every row exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn stopping_with_two_windows_present_aborts_the_speculation() {
    let (container, port) = start(true).await;
    let mut c = conn(port).await;
    tables(&mut c, 5).await;
    let id = "spec-stop";
    let store = Store::memory();
    let r = run_on(port, id, MysqlProofCfg::default(), &store).await;
    let (mut first, mut second) = two_windows_present(port, &r, &mut c).await;
    r.handle.stop();
    let (r, end) = ended(r, Duration::from_secs(15)).await;
    assert!(end.is_ok(), "a clean stop: {end:?}");
    wait_for_no_waiter(port, "t0004").await;
    wait_for_no_waiter(port, "t0000").await;
    assert_eq!(r.rows.load(std::sync::atomic::Ordering::SeqCst), 0);
    r.drain.abort();
    first.query_drop("UNLOCK TABLES").await.unwrap();
    second.query_drop("UNLOCK TABLES").await.unwrap();
    let again = run_on(port, id, MysqlProofCfg::default(), &store).await;
    again.delivered(5, Duration::from_secs(60)).await;
    sleep(Duration::from_secs(1)).await;
    assert_eq!(again.rows.load(std::sync::atomic::Ordering::SeqCst), 5);
    again.stop().await;
    container.rm().await.ok();
}

/// A multi-table RENAME spanning both windows' tables while both captures
/// wait: `t0000` and `t0002` are the first window's (its capture holds
/// `t0000` and waits for `t0002`), `t0001` and `t0003` the second's (its
/// capture holds `t0001` and waits for `t0003`), and the RENAME names
/// `t0000` and `t0001`. Every party locks in the global sorted order, so it
/// completes once the captures do: no deadlock, one capture attempt per
/// window, every row delivered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn a_rename_spanning_both_windows_never_deadlocks() {
    let (container, port) = start(true).await;
    let mut c = conn(port).await;
    tables(&mut c, 4).await;
    let id = "spec-rename";
    let r = run(port, id, MysqlProofCfg::default()).await;
    r.handle.pause();
    c.query_drop("INSERT INTO t0000 VALUES (1, 0)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO t0002 VALUES (1, 2)")
        .await
        .unwrap();
    let mut first = conn(port).await;
    first.query_drop("LOCK TABLES t0002 WRITE").await.unwrap();
    r.handle.resume();
    wait_for_waiter(port, "t0002").await;
    c.query_drop(
        "BEGIN; INSERT INTO t0001 VALUES (1, 1); INSERT INTO t0003 VALUES (1, 3); COMMIT",
    )
    .await
    .unwrap();
    let mut second = conn(port).await;
    second.query_drop("LOCK TABLES t0003 WRITE").await.unwrap();
    wait_for_waiter(port, "t0003").await;
    let rename = tokio::spawn(async move {
        let mut c = conn(port).await;
        c.query_drop("RENAME TABLE t0000 TO r0000, t0001 TO r0001")
            .await
            .unwrap();
    });
    sleep(Duration::from_millis(300)).await;
    first.query_drop("UNLOCK TABLES").await.unwrap();
    sleep(Duration::from_millis(300)).await;
    second.query_drop("UNLOCK TABLES").await.unwrap();
    tokio::time::timeout(Duration::from_secs(30), rename)
        .await
        .expect("the RENAME completes")
        .unwrap();
    r.delivered(4, Duration::from_secs(60)).await;
    let got = r.firsts.lock().unwrap().clone();
    for i in 0..4 {
        assert_eq!(got.get(&format!("t{i:04}")), Some(&(1, i)), "t{i:04}");
    }
    r.stop().await;
    container.rm().await.ok();
    let windows = records(id, "window");
    assert_eq!(windows[0]["next"], "formed", "{windows:?}");
    for w in &windows[..2] {
        assert_eq!(count(w, "capture_attempts"), 1, "{w}");
    }
    let lazy = records(id, "proof")
        .iter()
        .filter(|p| p["kind"] == "lazy")
        .count();
    assert_eq!(lazy, 4, "no per-table capture");
}

/// Held events stay within one window's bounds, and every window makes one
/// capture attempt: a burst much larger than the event bound while the
/// first window waits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn held_events_stay_within_a_windows_bounds() {
    let (container, port) = start(true).await;
    let mut c = conn(port).await;
    let n = 61;
    tables(&mut c, n).await;
    let id = "spec-bounds";
    let bound = 30;
    let r = run(
        port,
        id,
        MysqlProofCfg {
            activation_window_events: bound,
            ..Default::default()
        },
    )
    .await;
    r.handle.pause();
    c.query_drop("INSERT INTO t0000 VALUES (1, 0)")
        .await
        .unwrap();
    let mut locker = conn(port).await;
    locker.query_drop("LOCK TABLES t0000 WRITE").await.unwrap();
    r.handle.resume();
    wait_for_waiter(port, "t0000").await;
    for i in 1..n {
        c.query_drop(format!("INSERT INTO t{i:04} VALUES (1, {i})"))
            .await
            .unwrap();
    }
    sleep(Duration::from_millis(300)).await;
    locker.query_drop("UNLOCK TABLES").await.unwrap();
    r.delivered(n, Duration::from_secs(120)).await;
    sleep(Duration::from_secs(1)).await;
    assert_eq!(r.rows.load(std::sync::atomic::Ordering::SeqCst), n);
    r.stop().await;
    container.rm().await.ok();
    let windows = records(id, "window");
    for w in &windows {
        assert!(count(w, "prefetched") <= bound as u64, "{w}");
        assert!(count(w, "events") <= bound as u64, "{w}");
        assert_eq!(count(w, "capture_attempts"), 1, "{w}");
        assert!(count(w, "stream_openings") <= 2, "{w}");
    }
    let lazy = records(id, "proof")
        .iter()
        .filter(|p| p["kind"] == "lazy")
        .count();
    assert_eq!(lazy, n, "each table decided once, by its window");
}

/// Several transactions of one table, then a DDL of it right after the
/// window (it closes the window and lies in the proof interval): the
/// table is refused for the rows before the DDL, and those rows go on by
/// their FULL signature (its earlier registered shape) without any capture
/// of their own; its rows after the DDL follow the DDL. The other table of
/// the window is admitted. `t0001` was proven, then made unproven by a
/// database barrier (`ALTER DATABASE`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn a_ddl_right_after_the_window_governs_the_rows_before_it() {
    let (container, port) = start_with(true, true).await;
    let mut c = conn(port).await;
    tables(&mut c, 2).await;
    let id = "spec-ddl-after";
    let r = run(port, id, MysqlProofCfg::default()).await;
    c.query_drop("INSERT INTO t0001 VALUES (1, 1)")
        .await
        .unwrap();
    r.delivered(1, Duration::from_secs(30)).await;
    r.handle.pause();
    c.query_drop(format!("ALTER DATABASE {DB} COLLATE utf8mb4_bin"))
        .await
        .unwrap();
    c.query_drop("INSERT INTO t0000 VALUES (1, 0)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO t0001 VALUES (2, 1)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO t0001 VALUES (3, 1)")
        .await
        .unwrap();
    c.query_drop("ALTER TABLE t0001 ADD COLUMN w INT")
        .await
        .unwrap();
    c.query_drop("INSERT INTO t0001 VALUES (4, 1, 7)")
        .await
        .unwrap();
    r.handle.resume();
    let deadline = Instant::now() + Duration::from_secs(60);
    while r.rows.load(std::sync::atomic::Ordering::SeqCst) < 5 {
        assert!(!r.handle.join.is_finished(), "the source ended");
        assert!(Instant::now() < deadline, "{:?}", r.order.lock().unwrap());
        sleep(Duration::from_millis(50)).await;
    }
    sleep(Duration::from_secs(1)).await;
    let order = r.order.lock().unwrap().clone();
    let expected: Vec<(String, i64)> = [
        ("t0001", 1),
        ("t0000", 1),
        ("t0001", 2),
        ("t0001", 3),
        ("t0001", 4),
    ]
    .iter()
    .map(|(t, i)| (t.to_string(), *i))
    .collect();
    assert_eq!(order, expected);
    r.stop().await;
    container.rm().await.ok();
    let windows = records(id, "window");
    let last = windows.last().unwrap();
    assert_eq!(last["closed_by"], "statement", "{windows:?}");
    assert_eq!(count(last, "tables"), 2, "{windows:?}");
    assert_eq!(last["cut"], false, "{windows:?}");
    let lazy: Vec<_> = records(id, "proof")
        .into_iter()
        .filter(|p| p["table"] == "t0001" && p["kind"] == "lazy")
        .collect();
    assert_eq!(
        lazy.iter()
            .filter(|p| p["outcome"] == "relevant_ddl")
            .count(),
        1,
        "refused once: {lazy:?}"
    );
    assert_eq!(
        lazy.len(),
        2,
        "its first activation, then the refusal; never captured again: {lazy:?}"
    );
}

/// A DDL right before the window: its table is proven by the DDL's
/// forward proof and never joins the window.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn a_ddl_right_before_the_window_keeps_its_table_out() {
    let (container, port) = start(true).await;
    let mut c = conn(port).await;
    tables(&mut c, 3).await;
    let id = "spec-ddl-before";
    let r = run(port, id, MysqlProofCfg::default()).await;
    r.handle.pause();
    c.query_drop("ALTER TABLE t0002 ADD COLUMN w INT")
        .await
        .unwrap();
    c.query_drop("INSERT INTO t0002 VALUES (1, 2, 0)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO t0000 VALUES (1, 0)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO t0001 VALUES (1, 1)")
        .await
        .unwrap();
    r.handle.resume();
    r.delivered(3, Duration::from_secs(60)).await;
    r.stop().await;
    container.rm().await.ok();
    let windows = records(id, "window");
    assert_eq!(count(&windows[0], "tables"), 2, "{windows:?}");
    let t0002: Vec<_> = records(id, "proof")
        .into_iter()
        .filter(|p| p["table"] == "t0002")
        .collect();
    assert!(
        t0002.iter().all(|p| p["kind"] == "forward"),
        "proven by its DDL only: {t0002:?}"
    );
}

/// A harmless extra candidate: after a restart the cache flags nothing
/// proven, so the next window speculatively captures tables a full
/// selection finds proven; they are discarded at decision time and the
/// window commits its one required table.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn extra_speculative_candidates_are_discarded() {
    let (container, port) = start(true).await;
    let mut c = conn(port).await;
    tables(&mut c, 7).await;
    let id = "spec-extra";
    let store = Store::memory();
    let r = run_on(port, id, MysqlProofCfg::default(), &store).await;
    for i in 1..6 {
        c.query_drop(format!("INSERT INTO t{i:04} VALUES (1, {i})"))
            .await
            .unwrap();
    }
    r.delivered(5, Duration::from_secs(60)).await;
    r.stop().await;
    let before = records(id, "window").len();

    let r = run_on(port, id, MysqlProofCfg::default(), &store).await;
    r.handle.pause();
    c.query_drop("INSERT INTO t0000 VALUES (1, 0)")
        .await
        .unwrap();
    let mut locker = conn(port).await;
    locker.query_drop("LOCK TABLES t0000 WRITE").await.unwrap();
    r.handle.resume();
    wait_for_waiter(port, "t0000").await;
    let mut txn = vec!["BEGIN".to_string()];
    for i in 1..6 {
        txn.push(format!("INSERT INTO t{i:04} VALUES (2, {i})"));
    }
    txn.push("INSERT INTO t0006 VALUES (1, 6)".into());
    txn.push("COMMIT".into());
    c.query_drop(txn.join("; ")).await.unwrap();
    sleep(Duration::from_millis(300)).await;
    locker.query_drop("UNLOCK TABLES").await.unwrap();
    r.delivered(7, Duration::from_secs(60)).await;
    sleep(Duration::from_secs(1)).await;
    assert_eq!(r.rows.load(std::sync::atomic::Ordering::SeqCst), 7);
    r.stop().await;
    container.rm().await.ok();
    let windows = records(id, "window").split_off(before);
    assert_eq!(windows[0]["next"], "formed", "{windows:?}");
    assert_eq!(count(&windows[1], "speculated"), 6, "{windows:?}");
    assert_eq!(count(&windows[1], "tables"), 1, "{windows:?}");
}

/// A statement held during an admission ends the speculation (the proven
/// state may change before the next window's head): the next window then
/// forms in the ordinary way over the held events, and its time bound
/// still counts from its first event's receipt, so it closes at once
/// instead of waiting a full bound again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn held_events_after_a_statement_form_an_ordinary_window_by_receipt() {
    let (container, port) = start(true).await;
    let mut c = conn(port).await;
    tables(&mut c, 10).await;
    let id = "spec-gave-up";
    let r = run(port, id, MysqlProofCfg::default()).await;
    r.handle.pause();
    c.query_drop("INSERT INTO t0000 VALUES (1, 0)")
        .await
        .unwrap();
    let mut locker = conn(port).await;
    locker.query_drop("LOCK TABLES t0000 WRITE").await.unwrap();
    r.handle.resume();
    wait_for_waiter(port, "t0000").await;
    c.query_drop("ALTER TABLE t0009 ADD COLUMN w INT")
        .await
        .unwrap();
    c.query_drop(
        "BEGIN; INSERT INTO t0001 VALUES (1, 1); INSERT INTO t0002 VALUES (1, 2); \
         INSERT INTO t0003 VALUES (1, 3); INSERT INTO t0004 VALUES (1, 4); COMMIT",
    )
    .await
    .unwrap();
    sleep(Duration::from_millis(300)).await;
    locker.query_drop("UNLOCK TABLES").await.unwrap();
    r.delivered(5, Duration::from_secs(60)).await;
    sleep(Duration::from_secs(1)).await;
    let expected: Vec<(String, i64)> =
        (0..5).map(|i| (format!("t{i:04}"), 1)).collect();
    assert_eq!(
        *r.order.lock().unwrap(),
        expected,
        "every row once, in order"
    );
    r.stop().await;
    container.rm().await.ok();
    let windows = records(id, "window");
    assert_eq!(windows[0]["next"], "gave_up", "{windows:?}");
    assert_eq!(count(&windows[1], "tables"), 4, "{windows:?}");
    assert!(windows[1]["speculated"].is_null(), "{windows:?}");
    let read_ms = windows[1]["read_ms"].as_f64().unwrap();
    assert!(
        read_ms < 25.0,
        "the ordinary window over held events counts from receipt: {read_ms} ms"
    );
}

/// A DDL closes the current window, and new tables' rows arrive while it is
/// admitted (its capture held by a lock): the DDL completes its implicit
/// transaction in the read-ahead exactly as dispatch completes it, so the
/// speculative window captures from the transaction after the DDL and its
/// recomputed tables match. Every row arrives once, in order, the source
/// keeps running (no invariant incident) and the checkpoint reaches the
/// server's position after the last insert.
async fn ddl_closes_a_window_during_admission(gtid: bool, ddl: &str, id: &str) {
    let (container, port) = start(gtid).await;
    let mut c = conn(port).await;
    tables(&mut c, 10).await;
    let store = Store::memory();
    let r = run_on(port, id, MysqlProofCfg::default(), &store).await;
    r.handle.pause();
    c.query_drop("INSERT INTO t0000 VALUES (1, 0)")
        .await
        .unwrap();
    c.query_drop(ddl).await.unwrap();
    let mut locker = conn(port).await;
    locker.query_drop("LOCK TABLES t0000 WRITE").await.unwrap();
    r.handle.resume();
    wait_for_waiter(port, "t0000").await;
    c.query_drop(
        "BEGIN; INSERT INTO t0001 VALUES (1, 1); INSERT INTO t0002 VALUES (1, 2); \
         INSERT INTO t0003 VALUES (1, 3); INSERT INTO t0004 VALUES (1, 4); COMMIT",
    )
    .await
    .unwrap();
    let (file, pos): (String, u64) = c
        .query_first("SHOW BINARY LOG STATUS")
        .await
        .unwrap()
        .map(|row: mysql_async::Row| {
            let mut row = row;
            (row.take(0).unwrap(), row.take(1).unwrap())
        })
        .unwrap();
    sleep(Duration::from_millis(300)).await;
    locker.query_drop("UNLOCK TABLES").await.unwrap();
    r.delivered(5, Duration::from_secs(60)).await;
    sleep(Duration::from_secs(2)).await;
    assert!(!r.handle.join.is_finished(), "the source keeps running");
    let expected: Vec<(String, i64)> =
        (0..5).map(|i| (format!("t{i:04}"), 1)).collect();
    assert_eq!(
        *r.order.lock().unwrap(),
        expected,
        "every row once, in order"
    );
    r.stop().await;
    let cp: sources::MySqlCheckpoint = serde_json::from_slice(
        &store.ckpt.get_raw(id).await.unwrap().expect("a checkpoint"),
    )
    .unwrap();
    assert_eq!(cp.file, file, "{cp:?}");
    assert!(cp.pos >= pos, "the checkpoint {cp:?} reaches {file}:{pos}");
    container.rm().await.ok();
    let windows = records(id, "window");
    assert_eq!(windows[0]["closed_by"], "statement", "{windows:?}");
    assert_eq!(windows[0]["next"], "formed", "{windows:?}");
    assert_eq!(count(&windows[1], "tables"), 4, "{windows:?}");
    assert_eq!(count(&windows[1], "speculated"), 4, "{windows:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn a_ddl_closing_a_window_starts_the_next_after_it_gtid() {
    ddl_closes_a_window_during_admission(
        true,
        "ALTER TABLE t0009 ADD COLUMN w INT",
        "ddl-close-gtid",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn a_ddl_closing_a_window_starts_the_next_after_it_file_position() {
    ddl_closes_a_window_during_admission(
        false,
        "ALTER TABLE t0009 ADD COLUMN w INT",
        "ddl-close-filepos",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn an_executable_comment_ddl_closing_a_window_starts_the_next_after_it() {
    ddl_closes_a_window_during_admission(
        true,
        "/*!50700 ALTER TABLE t0009 ADD COLUMN w INT */",
        "ddl-close-comment",
    )
    .await;
}
