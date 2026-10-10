//! A stream that drops at any event boundary resumes from a proven
//! transaction boundary: a transaction cut short is read again (duplicates
//! allowed), never skipped in part. GTID and file-position modes; a dropped
//! connection injected after the GTID, BEGIN, TableMap, between rows events,
//! right before the XID and right after it; and a real killed connection
//! while the source is backpressured mid-transaction.
//!
//! Run with:
//! ```bash
//! cargo test -p sources --test mysql_reconnect_boundaries_e2e -- --include-ignored --test-threads=1
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use checkpoints::{CheckpointStore, MemCheckpointStore};
use ctor::dtor;
use deltaforge_config::{OnSchemaDrift, SnapshotCfg, SnapshotMode};
use deltaforge_core::{Event, Source, SourceHandle, SourceItem};
use gate_ownership::GateOwned;
use mysql_async::prelude::Queryable;
use sources::MySqlSource;
use sources::stream_probe::{self, Point};
use storage::{DurableSchemaRegistry, MemoryStorageBackend};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt, core::WaitFor, runners::AsyncRunner,
};
use tokio::sync::{Mutex, OnceCell, mpsc};
use tokio::time::{Duration, sleep, timeout};

mod test_common;

const ROOT_PW: &str = "pw";

type Server = (ContainerAsync<GenericImage>, u16);
static GTID: OnceCell<Server> = OnceCell::const_new();
static FILEPOS: OnceCell<Server> = OnceCell::const_new();
/// The probe is process-global: one case at a time.
static SERIAL: Mutex<()> = Mutex::const_new(());

#[dtor]
fn cleanup() {
    for cell in [&GTID, &FILEPOS] {
        if let Some((c, _)) = cell.get() {
            std::process::Command::new("docker")
                .args(["rm", "-f", "-v", c.id()])
                .output()
                .ok();
        }
    }
}

async fn start(gtid: bool) -> Server {
    let mut cmd = vec![
        "--server-id=43".to_string(),
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
    let deadline = Instant::now() + Duration::from_secs(60);
    while mysql_async::Conn::from_url(dsn(port, "")).await.is_err() {
        assert!(Instant::now() < deadline, "mysql on {port} not ready");
        sleep(Duration::from_millis(500)).await;
    }
    (c, port)
}

async fn server(gtid: bool) -> u16 {
    let cell = if gtid { &GTID } else { &FILEPOS };
    cell.get_or_init(|| start(gtid)).await.1
}

fn dsn(port: u16, db: &str) -> String {
    format!("mysql://root:{ROOT_PW}@127.0.0.1:{port}/{db}")
}

async fn sql(port: u16, db: &str, stmts: &[&str]) {
    let mut c = mysql_async::Conn::from_url(dsn(port, db)).await.unwrap();
    for s in stmts {
        c.query_drop(*s)
            .await
            .unwrap_or_else(|e| panic!("{s}: {e}"));
    }
    c.disconnect().await.ok();
}

/// What a coordinator would deliver: a `TxBegin` opens a buffer, `TxAbort`
/// drops it, `TxCommit` delivers it; standalone events are delivered as
/// they come.
#[derive(Default)]
struct Delivered {
    rows: Vec<Event>,
    open: Option<(String, Vec<Event>)>,
    aborts: usize,
    commit_checkpoints: Vec<serde_json::Value>,
}

impl Delivered {
    fn apply(&mut self, item: SourceItem) {
        match item {
            SourceItem::TxBegin { tx_id } => self.open = Some((tx_id, vec![])),
            SourceItem::TxAbort { tx_id } => {
                let open = self.open.take();
                assert_eq!(
                    open.as_ref().map(|o| o.0.as_str()),
                    Some(tx_id.as_str()),
                    "abort of a transaction that is not open"
                );
                self.aborts += 1;
            }
            SourceItem::TxCommit { tx_id, boundary } => {
                let (open_id, rows) =
                    self.open.take().expect("commit without begin");
                assert_eq!(open_id, tx_id);
                self.rows.extend(rows);
                self.commit_checkpoints.push(
                    serde_json::from_slice(boundary.checkpoint.as_bytes())
                        .unwrap(),
                );
            }
            SourceItem::Event(e) => match self.open.as_mut() {
                Some((_, rows)) => rows.push(e),
                None => self.rows.push(e),
            },
            _ => {}
        }
    }

    /// `(table, id) -> versions` in delivery order.
    fn versions(&self) -> BTreeMap<(String, i64), Vec<i64>> {
        let mut out: BTreeMap<_, Vec<i64>> = BTreeMap::new();
        for e in &self.rows {
            let img = e.after.as_ref().or(e.before.as_ref()).unwrap();
            let id = img["id"].as_i64().unwrap();
            let v = if e.after.is_some() {
                img["v"].as_i64().unwrap()
            } else {
                -1 // deleted
            };
            out.entry((e.source.table.clone(), id)).or_default().push(v);
        }
        out
    }
}

struct Case {
    port: u16,
    db: String,
    handle: SourceHandle,
    rx: mpsc::Receiver<SourceItem>,
    delivered: Delivered,
    ckpt: Arc<dyn CheckpointStore>,
}

impl Case {
    async fn start(gtid: bool, name: &str, channel: usize) -> Self {
        let port = server(gtid).await;
        let db = format!("rb_{name}");
        sql(
            port,
            "",
            &[
                &format!("DROP DATABASE IF EXISTS {db}"),
                &format!("CREATE DATABASE {db}"),
            ],
        )
        .await;
        sql(
            port,
            &db,
            &[
                "CREATE TABLE a (id INT PRIMARY KEY, v INT NOT NULL)",
                "CREATE TABLE b (id INT PRIMARY KEY, v INT NOT NULL)",
                "INSERT INTO b VALUES (1, 0), (2, 0)",
            ],
        )
        .await;
        let backend: storage::ArcStorageBackend =
            Arc::new(MemoryStorageBackend::new());
        let src = MySqlSource {
            id: format!("rb-{name}"),
            dsn: dsn(port, &db).into(),
            tables: vec![format!("{db}.*")],
            tenant: "acme".into(),
            pipeline: "test".into(),
            registry: DurableSchemaRegistry::new(backend.clone())
                .await
                .unwrap(),
            registry_scope:
                sources::registry_scope::SharedRegistryScope::default(),
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
        };
        let ckpt: Arc<dyn CheckpointStore> =
            Arc::new(MemCheckpointStore::new().unwrap());
        let (tx, rx) =
            test_common::acked_channel(&src, &ckpt, &src.id, channel);
        let handle = src.run(tx, Arc::clone(&ckpt)).await;
        let deadline = Instant::now() + Duration::from_secs(30);
        while !handle.ready.is_ready() {
            assert!(!handle.join.is_finished(), "the source ended");
            assert!(Instant::now() < deadline, "the source never streamed");
            sleep(Duration::from_millis(50)).await;
        }
        Self {
            port,
            db,
            handle,
            rx,
            delivered: Delivered::default(),
            ckpt,
        }
    }

    /// Transaction 1: three rows events over two tables. Transaction 2: one
    /// more row (proves the stream continued past the cut).
    async fn write(&self) {
        sql(
            self.port,
            &self.db,
            &[
                "BEGIN",
                "INSERT INTO a VALUES (1, 1), (2, 1), (3, 1)",
                "UPDATE b SET v = 1 WHERE id IN (1, 2)",
                "INSERT INTO a VALUES (4, 1)",
                "COMMIT",
                "INSERT INTO a VALUES (5, 1)",
            ],
        )
        .await;
    }

    /// Drain until `pred` holds on what was delivered (or the deadline).
    async fn until(&mut self, pred: impl Fn(&Delivered) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while !pred(&self.delivered) {
            assert!(
                !self.handle.join.is_finished(),
                "the source ended while draining"
            );
            assert!(Instant::now() < deadline, "rows never delivered");
            if let Ok(Some(item)) =
                timeout(Duration::from_millis(200), self.rx.recv()).await
            {
                self.delivered.apply(item);
            }
        }
    }

    /// Completeness, per-key order and final state against the database.
    async fn assert_complete(&mut self) {
        self.until(|d| {
            let v = d.versions();
            v.contains_key(&("a".into(), 5))
        })
        .await;
        let versions = self.delivered.versions();
        let mut c = mysql_async::Conn::from_url(dsn(self.port, &self.db))
            .await
            .unwrap();
        for table in ["a", "b"] {
            let rows: Vec<(i64, i64)> = c
                .query(format!("SELECT id, v FROM {table} ORDER BY id"))
                .await
                .unwrap();
            for (id, v) in rows {
                if table == "b" && v == 0 {
                    continue; // never changed in the stream
                }
                let seen =
                    versions.get(&(table.to_string(), id)).unwrap_or_else(
                        || panic!("{table}.{id} missing: {versions:?}"),
                    );
                assert!(
                    seen.windows(2).all(|w| w[0] <= w[1]),
                    "{table}.{id} out of order: {seen:?}"
                );
                assert_eq!(seen.last(), Some(&v), "{table}.{id} final state");
            }
        }
        c.disconnect().await.ok();
        assert!(!self.handle.join.is_finished(), "the source stopped");
    }

    async fn stop(self) {
        stream_probe::reset_event_hooks();
        self.handle.stop();
        timeout(Duration::from_secs(30), self.handle.join)
            .await
            .expect("the source stops promptly")
            .ok();
    }
}

/// One injected disconnect at `point` (the `nth` such event after arming).
async fn cut_at(
    gtid: bool,
    name: &str,
    point: Point,
    nth: usize,
    aborts: usize,
) {
    let _serial = SERIAL.lock().await;
    stream_probe::reset_event_hooks();
    let mut case = Case::start(gtid, name, 64).await;
    let reached = stream_probe::disconnect_after(point, nth);
    case.write().await;
    timeout(Duration::from_secs(30), reached.notified())
        .await
        .expect("the disconnect point was never reached");
    case.assert_complete().await;
    assert_eq!(
        case.delivered.aborts, aborts,
        "{name}: aborted transactions"
    );
    if gtid {
        // The last commit's checkpoint covers both transactions.
        let last = case.delivered.commit_checkpoints.last().unwrap();
        let mut c = mysql_async::Conn::from_url(dsn(case.port, ""))
            .await
            .unwrap();
        let executed: String = c
            .query_first("SELECT @@GLOBAL.gtid_executed")
            .await
            .unwrap()
            .unwrap();
        c.disconnect().await.ok();
        let set = last["gtid_set"].as_str().unwrap().to_string();
        let mut c = mysql_async::Conn::from_url(dsn(case.port, ""))
            .await
            .unwrap();
        let covered: i64 = c
            .exec_first(
                "SELECT GTID_SUBSET(?, ?)",
                (executed.clone(), set.clone()),
            )
            .await
            .unwrap()
            .unwrap();
        c.disconnect().await.ok();
        assert_eq!(covered, 1, "checkpoint {set} does not cover {executed}");
    }
    let _ = &case.ckpt;
    case.stop().await;
}

macro_rules! cases {
    ($($name:ident: $gtid:expr, $point:expr, $nth:expr, $aborts:expr;)*) => {$(
        #[tokio::test]
        #[ignore = "requires docker"]
        async fn $name() {
            test_common::init_test_tracing();
            cut_at($gtid, stringify!($name), $point, $nth, $aborts).await;
        }
    )*};
}

// The cut transaction is aborted (GTID mode) and read again; a cut after
// its XID needs no abort. File-position mode has no transaction markers:
// the cut transaction's rows are delivered again (duplicates).
cases! {
    gtid_after_gtid: true, Point::AfterGtid, 1, 1;
    gtid_after_begin: true, Point::AfterBegin, 1, 1;
    gtid_after_table_map: true, Point::AfterTableMap, 1, 1;
    gtid_between_rows: true, Point::AfterRows, 1, 1;
    gtid_before_xid: true, Point::AfterRows, 3, 1;
    gtid_after_xid: true, Point::AfterXid, 1, 0;
    file_after_begin: false, Point::AfterBegin, 1, 0;
    file_after_table_map: false, Point::AfterTableMap, 1, 0;
    file_between_rows: false, Point::AfterRows, 1, 0;
    file_before_xid: false, Point::AfterRows, 3, 0;
    file_after_xid: false, Point::AfterXid, 1, 0;
}

/// A real killed connection while the source is backpressured inside a
/// transaction (tiny channel, consumer stalled): the stream resumes from the
/// boundary and nothing is skipped.
async fn killed_during_backpressure(gtid: bool, name: &str) {
    let _serial = SERIAL.lock().await;
    stream_probe::reset_event_hooks();
    let mut case = Case::start(gtid, name, 1).await;
    // The consumer is not draining: the transaction (about ten items) does
    // not fit the channels, so the source blocks inside it.
    case.write().await;
    sleep(Duration::from_secs(2)).await;
    assert!(
        case.delivered.rows.is_empty() && !case.handle.join.is_finished(),
        "nothing drained yet"
    );
    let mut c = mysql_async::Conn::from_url(dsn(case.port, ""))
        .await
        .unwrap();
    let ids: Vec<u64> = c
        .query("SELECT ID FROM information_schema.PROCESSLIST WHERE COMMAND LIKE 'Binlog Dump%'")
        .await
        .unwrap();
    assert!(!ids.is_empty(), "no binlog dump thread");
    for id in ids {
        c.query_drop(format!("KILL CONNECTION {id}")).await.unwrap();
    }
    c.disconnect().await.ok();
    case.assert_complete().await;
    case.stop().await;
}

#[tokio::test]
#[ignore = "requires docker"]
async fn gtid_killed_during_backpressure() {
    test_common::init_test_tracing();
    killed_during_backpressure(true, "gtid_bp").await;
}

#[tokio::test]
#[ignore = "requires docker"]
async fn file_killed_during_backpressure() {
    test_common::init_test_tracing();
    killed_during_backpressure(false, "file_bp").await;
}
