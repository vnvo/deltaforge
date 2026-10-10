//! `log_replica_updates` is required on every MySQL source. With it OFF, the
//! transactions a server applies through replication (a channel configured
//! now or later) advance its executed GTID set and its schema without reaching
//! its binlog (whose file position does not move), so changes are missed and
//! an interval of that binlog can look free of a DDL that changed a table.
//!
//! Such a server is refused before anything durable is written, any snapshot
//! table is read or any event is read: at startup, on every reconnect (the
//! setting changes only with a restart, which ends the stream), and before a
//! failover candidate is reconciled. GTID and file-position replication are
//! covered separately.
//!
//! Every refusal runs over a backend that fails every write: a refusal that
//! reports the setting (not a write failure) proves nothing durable was
//! attempted before it.
//!
//! Run with:
//! ```bash
//! cargo test -p sources --test mysql_replica_binlog_e2e -- --include-ignored --test-threads=1
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Instant;

use checkpoints::{CheckpointStore, MemCheckpointStore};
use common::AllowList;
use deltaforge_config::{OnSchemaDrift, SnapshotCfg, SnapshotMode};
use deltaforge_core::{Event, Op, Source, SourceHandle, SourceItem};
use gate_ownership::GateOwned;
use mysql_async::prelude::Queryable;
use sources::MySqlSource;
use sources::failover::identity::{
    IdentityComparison, IdentityStore, ServerIdentity,
};
use sources::mysql::mysql_health::MySqlServerIdentity;
use storage::adapters::test_util::FaultBackend;
use storage::{ArcStorageBackend, DurableSchemaRegistry, MemoryStorageBackend};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt, core::WaitFor, runners::AsyncRunner,
};
use tokio::sync::mpsc;
use tokio::time::{Duration, sleep, timeout};

mod test_common;
use test_common::init_test_tracing;

const ROOT_PW: &str = "pw";
const DB: &str = "rb";
const REFUSAL: &str = "must be ON";

type Server = (ContainerAsync<GenericImage>, u16);

/// `updates`: `--log-replica-updates` ON/OFF, or the server default (ON).
async fn start(server_id: u32, gtid: bool, updates: Option<bool>) -> Server {
    let mut cmd = vec![
        format!("--server-id={server_id}"),
        "--log-bin=mysql-bin".into(),
        "--binlog-format=ROW".into(),
        "--binlog-row-image=FULL".into(),
        "--binlog-checksum=NONE".into(),
    ];
    if let Some(on) = updates {
        cmd.push(format!(
            "--log-replica-updates={}",
            if on { "ON" } else { "OFF" }
        ));
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
    ready(port).await;
    (c, port)
}

async fn ready(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while mysql_async::Conn::from_url(dsn(port)).await.is_err() {
        assert!(Instant::now() < deadline, "mysql on {port} not ready");
        sleep(Duration::from_millis(500)).await;
    }
}

fn dsn(port: u16) -> String {
    format!("mysql://root:{ROOT_PW}@127.0.0.1:{port}/")
}

async fn sql(port: u16, stmts: &[String]) {
    let mut c = mysql_async::Conn::from_url(dsn(port)).await.unwrap();
    for s in stmts {
        c.query_drop(s.as_str())
            .await
            .unwrap_or_else(|e| panic!("{s}: {e}"));
    }
    c.disconnect().await.ok();
}

async fn one<T: mysql_async::prelude::FromValue + Send + 'static>(
    port: u16,
    q: &str,
) -> T {
    let mut c = mysql_async::Conn::from_url(dsn(port)).await.unwrap();
    let v: T = c.query_first(q).await.unwrap().unwrap();
    c.disconnect().await.ok();
    v
}

async fn create_table(port: u16) {
    sql(
        port,
        &[
            format!("CREATE DATABASE IF NOT EXISTS {DB}"),
            format!("CREATE TABLE {DB}.t (id INT PRIMARY KEY, v INT)"),
        ],
    )
    .await;
}

/// (file, position, executed GTID set) of `port`'s own binlog.
async fn status(port: u16) -> (String, u64, String) {
    let mut c = mysql_async::Conn::from_url(dsn(port)).await.unwrap();
    let r: mysql_async::Row = c
        .query_first("SHOW BINARY LOG STATUS")
        .await
        .unwrap()
        .unwrap();
    c.disconnect().await.ok();
    (
        r.get("File").unwrap(),
        r.get("Position").unwrap(),
        r.get("Executed_Gtid_Set").unwrap(),
    )
}

/// Statements in `port`'s own binlog.
async fn logged(port: u16) -> Vec<String> {
    let mut c = mysql_async::Conn::from_url(dsn(port)).await.unwrap();
    let logs: Vec<mysql_async::Row> =
        c.query("SHOW BINARY LOGS").await.unwrap();
    let mut out = Vec::new();
    for l in logs {
        let file: String = l.get(0).unwrap();
        let events: Vec<mysql_async::Row> = c
            .query(format!("SHOW BINLOG EVENTS IN '{file}'"))
            .await
            .unwrap();
        out.extend(
            events
                .into_iter()
                .map(|e| e.get::<String, _>("Info").unwrap()),
        );
    }
    c.disconnect().await.ok();
    out
}

async fn columns(port: u16) -> u64 {
    one(
        port,
        &format!(
            "SELECT COUNT(*) FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = '{DB}' AND TABLE_NAME = 't'"
        ),
    )
    .await
}

async fn replicate(replica: u16, primary_ip: &str, gtid: bool) {
    let position = if gtid {
        "SOURCE_AUTO_POSITION = 1".to_string()
    } else {
        "SOURCE_LOG_FILE = 'mysql-bin.000001', SOURCE_LOG_POS = 4".to_string()
    };
    sql(
        replica,
        &[
            format!(
                "CHANGE REPLICATION SOURCE TO SOURCE_HOST = '{primary_ip}', \
                 SOURCE_PORT = 3306, SOURCE_USER = 'root', \
                 SOURCE_PASSWORD = '{ROOT_PW}', GET_SOURCE_PUBLIC_KEY = 1, \
                 {position}"
            ),
            "START REPLICA".into(),
        ],
    )
    .await;
}

async fn until(what: &str, mut check: impl AsyncFnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while !check().await {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        sleep(Duration::from_millis(200)).await;
    }
}

/// A TCP endpoint (VIP, DNS) sending every new connection to the current
/// target; switching drops every open connection.
struct Proxy {
    port: u16,
    to: Arc<AtomicU16>,
    live: Arc<std::sync::Mutex<Vec<tokio::task::AbortHandle>>>,
}

impl Proxy {
    async fn start(to: u16) -> Self {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        let target = Arc::new(AtomicU16::new(to));
        let live = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (t, lv) = (target.clone(), live.clone());
        tokio::spawn(async move {
            while let Ok((mut client, _)) = l.accept().await {
                let to = t.load(Ordering::SeqCst);
                let task = tokio::spawn(async move {
                    if let Ok(mut server) =
                        tokio::net::TcpStream::connect(("127.0.0.1", to)).await
                    {
                        let _ = tokio::io::copy_bidirectional(
                            &mut client,
                            &mut server,
                        )
                        .await;
                    }
                });
                lv.lock().unwrap().push(task.abort_handle());
            }
        });
        Self {
            port,
            to: target,
            live,
        }
    }

    fn switch_to(&self, to: u16) {
        self.to.store(to, Ordering::SeqCst);
        for t in self.live.lock().unwrap().drain(..) {
            t.abort();
        }
    }
}

struct State {
    backend: Arc<FaultBackend>,
    registry: Arc<DurableSchemaRegistry>,
    ckpt: Arc<dyn CheckpointStore>,
}

impl State {
    async fn new() -> Self {
        let backend =
            Arc::new(FaultBackend::wrap(Arc::new(MemoryStorageBackend::new())));
        let b: ArcStorageBackend = backend.clone();
        Self {
            registry: DurableSchemaRegistry::new(b).await.unwrap(),
            ckpt: Arc::new(MemCheckpointStore::new().unwrap()),
            backend,
        }
    }

    /// Every later write fails.
    fn freeze(&self) {
        self.backend.allow_writes(0);
    }

    fn source(&self, id: &str, port: u16, mode: SnapshotMode) -> MySqlSource {
        MySqlSource {
            id: id.into(),
            dsn: format!("mysql://root:{ROOT_PW}@127.0.0.1:{port}/{DB}")
                .as_str()
                .into(),
            tables: vec![format!("{DB}.t")],
            tenant: "acme".into(),
            pipeline: "test".into(),
            registry: self.registry.clone(),
            registry_scope:
                sources::registry_scope::SharedRegistryScope::default(),
            backend: self.backend.clone(),
            outbox_tables: AllowList::default(),
            snapshot_cfg: SnapshotCfg {
                mode,
                ..Default::default()
            },
            on_schema_drift: OnSchemaDrift::Adapt,
            table_options: Default::default(),
            rotation: None,
            snapshot_cohort: Default::default(),
        }
    }

    async fn run(
        &self,
        src: MySqlSource,
    ) -> (SourceHandle, mpsc::Receiver<SourceItem>) {
        let id = src.id.clone();
        let (tx, rx) = test_common::acked_channel(&src, &self.ckpt, &id, 64);
        (src.run(tx, self.ckpt.clone()).await, rx)
    }

    async fn identity_is(&self, id: &str, port: u16) -> bool {
        let uuid: String = one(port, "SELECT @@GLOBAL.server_uuid").await;
        let cmp = IdentityStore::new(self.backend.clone())
            .compare(
                id,
                &ServerIdentity::MySql(MySqlServerIdentity {
                    server_uuid: uuid,
                }),
            )
            .await
            .unwrap();
        matches!(cmp, IdentityComparison::Same)
    }
}

/// The source ends with the completeness refusal; returns how many items it
/// had sent.
async fn refused(
    handle: SourceHandle,
    rx: &mut mpsc::Receiver<SourceItem>,
) -> Vec<Event> {
    let ended = timeout(Duration::from_secs(90), handle.join)
        .await
        .expect("refused promptly")
        .expect("task");
    let msg = format!("{:#}", ended.expect_err("must be refused"));
    assert!(
        msg.contains("log_replica_updates") && msg.contains(REFUSAL),
        "{msg}"
    );
    rows(rx, Duration::from_millis(300)).await
}

/// Row events received until `quiet` passes without one.
async fn rows(
    rx: &mut mpsc::Receiver<SourceItem>,
    quiet: Duration,
) -> Vec<Event> {
    let mut out = Vec::new();
    while let Ok(Some(item)) = timeout(quiet, rx.recv()).await {
        if let SourceItem::Event(e) = item {
            if matches!(e.op, Op::Create | Op::Update | Op::Delete | Op::Read) {
                out.push(e);
            }
        }
    }
    out
}

/// The next row with id `id`.
async fn row(rx: &mut mpsc::Receiver<SourceItem>, id: i64) -> Event {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let left = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_else(|| panic!("no row {id}"));
        match timeout(left, rx.recv()).await {
            Ok(Some(SourceItem::Event(e)))
                if e.after.as_ref().is_some_and(|a| a["id"] == id) =>
            {
                return e;
            }
            Ok(Some(_)) => {}
            other => panic!("no row {id}: {other:?}"),
        }
    }
}

/// The behavior that makes the setting mandatory, then the refusal of an
/// OFF replica and the streaming of an ON one.
async fn replica_binlog(gtid: bool) {
    init_test_tracing();
    let base = if gtid { 70 } else { 80 };
    let (primary_c, primary) = start(base, gtid, Some(true)).await;
    let (_off_c, off) = start(base + 1, gtid, Some(false)).await;
    let (_on_c, on) = start(base + 2, gtid, Some(true)).await;
    let ip = primary_c.get_bridge_ip_address().await.unwrap().to_string();
    create_table(primary).await;
    for r in [off, on] {
        replicate(r, &ip, gtid).await;
        until("the table on the replica", async || columns(r).await == 2).await;
    }

    let (off_before, on_before) = (status(off).await, status(on).await);
    sql(
        primary,
        &[
            format!("ALTER TABLE {DB}.t ADD COLUMN w INT"),
            format!("INSERT INTO {DB}.t VALUES (1, 1, 1)"),
        ],
    )
    .await;
    for r in [off, on] {
        until("the DDL and row on the replica", async || {
            columns(r).await == 3
                && one::<u64>(r, &format!("SELECT COUNT(*) FROM {DB}.t")).await
                    == 1
        })
        .await;
    }
    // OFF: the schema changed, the binlog position did not move and the
    // binlog holds no DDL, while the executed GTID set advanced.
    let off_after = status(off).await;
    assert_eq!(
        (&off_after.0, off_after.1),
        (&off_before.0, off_before.1),
        "gtid={gtid}"
    );
    assert!(
        !logged(off).await.iter().any(|s| s.contains("ADD COLUMN w")),
        "gtid={gtid}"
    );
    if gtid {
        assert_ne!(off_after.2, off_before.2);
    }
    // ON: the replica's binlog holds the DDL.
    let on_after = status(on).await;
    assert_ne!((&on_after.0, on_after.1), (&on_before.0, on_before.1));
    assert!(logged(on).await.iter().any(|s| s.contains("ADD COLUMN w")));

    let st = State::new().await;
    st.freeze();
    let (h, mut rx) = st.run(st.source("off", off, SnapshotMode::Never)).await;
    assert!(refused(h, &mut rx).await.is_empty());
    assert!(st.ckpt.get_raw("off").await.unwrap().is_none());

    let st = State::new().await;
    let (h, mut rx) = st.run(st.source("on", on, SnapshotMode::Never)).await;
    sleep(Duration::from_secs(3)).await;
    sql(primary, &[format!("INSERT INTO {DB}.t VALUES (2, 2, 2)")]).await;
    assert_eq!(row(&mut rx, 2).await.after.unwrap()["w"], 2);
    h.stop();
    timeout(Duration::from_secs(30), h.join).await.ok();
}

#[tokio::test]
#[ignore = "requires docker"]
async fn replica_behavior_and_refusal_gtid() {
    replica_binlog(true).await;
}

#[tokio::test]
#[ignore = "requires docker"]
async fn replica_behavior_and_refusal_file_position() {
    replica_binlog(false).await;
}

/// A server with no replication channel and the setting OFF is refused: a
/// channel can be configured on it at any time. Configuring one does not
/// change the outcome.
async fn no_channel_is_not_enough(gtid: bool) {
    init_test_tracing();
    let (_c, off) = start(if gtid { 90 } else { 91 }, gtid, Some(false)).await;
    create_table(off).await;
    for configure_channel in [false, true] {
        if configure_channel {
            sql(
                off,
                &["CHANGE REPLICATION SOURCE TO SOURCE_HOST = '127.0.0.1', \
                   SOURCE_PORT = 3307, SOURCE_USER = 'nobody'"
                    .into()],
            )
            .await;
        }
        let st = State::new().await;
        st.freeze();
        let (h, mut rx) =
            st.run(st.source("primary", off, SnapshotMode::Never)).await;
        assert!(refused(h, &mut rx).await.is_empty());
        assert!(st.ckpt.get_raw("primary").await.unwrap().is_none());
    }
}

#[tokio::test]
#[ignore = "requires docker"]
async fn a_primary_without_log_replica_updates_is_refused_gtid() {
    no_channel_is_not_enough(true).await;
}

#[tokio::test]
#[ignore = "requires docker"]
async fn a_primary_without_log_replica_updates_is_refused_file_position() {
    no_channel_is_not_enough(false).await;
}

/// A channel added to a running source's server (ON, as required) keeps its
/// binlog complete: the replicated rows reach the stream.
async fn a_channel_added_while_streaming(gtid: bool) {
    init_test_tracing();
    let base = if gtid { 100 } else { 110 };
    let (primary_c, primary) = start(base, gtid, None).await;
    let (_c, server) = start(base + 1, gtid, None).await;
    let ip = primary_c.get_bridge_ip_address().await.unwrap().to_string();
    // The source's database exists; its table arrives by replication.
    sql(server, &[format!("CREATE DATABASE {DB}")]).await;
    let st = State::new().await;
    let (h, mut rx) = st
        .run(st.source("grows", server, SnapshotMode::Never))
        .await;
    sleep(Duration::from_secs(3)).await;
    create_table(primary).await;
    sql(primary, &[format!("INSERT INTO {DB}.t VALUES (1, 1)")]).await;
    replicate(server, &ip, gtid).await;
    assert_eq!(row(&mut rx, 1).await.after.unwrap()["v"], 1);
    h.stop();
    timeout(Duration::from_secs(30), h.join).await.ok();
}

#[tokio::test]
#[ignore = "requires docker"]
async fn a_channel_added_while_streaming_stays_complete_gtid() {
    a_channel_added_while_streaming(true).await;
}

#[tokio::test]
#[ignore = "requires docker"]
async fn a_channel_added_while_streaming_stays_complete_file_position() {
    a_channel_added_while_streaming(false).await;
}

/// An initial snapshot on an OFF server is refused before anything: no
/// durable write (the backend fails every one), no checkpoint, no row, and
/// no statement touching the table or the catalog (performance_schema).
#[tokio::test]
#[ignore = "requires docker"]
async fn an_initial_snapshot_on_an_off_server_persists_and_reads_nothing() {
    init_test_tracing();
    let (_c, off) = start(120, true, Some(false)).await;
    create_table(off).await;
    sql(
        off,
        &[
            format!("INSERT INTO {DB}.t VALUES (1, 1), (2, 2)"),
            "UPDATE performance_schema.setup_consumers SET ENABLED = 'YES' \
             WHERE NAME = 'events_statements_history_long'"
                .into(),
            "TRUNCATE TABLE performance_schema.events_statements_history_long"
                .into(),
            "TRUNCATE TABLE performance_schema.table_io_waits_summary_by_table"
                .into(),
        ],
    )
    .await;
    let st = State::new().await;
    st.freeze();
    let (h, mut rx) =
        st.run(st.source("snap", off, SnapshotMode::Always)).await;
    assert!(refused(h, &mut rx).await.is_empty(), "no snapshot rows");
    assert!(st.ckpt.get_raw("snap").await.unwrap().is_none());

    let mut c = mysql_async::Conn::from_url(dsn(off)).await.unwrap();
    let seen: Vec<String> = c
        .query(
            "SELECT SQL_TEXT FROM performance_schema.events_statements_history_long \
             WHERE THREAD_ID <> PS_CURRENT_THREAD_ID() AND SQL_TEXT IS NOT NULL",
        )
        .await
        .unwrap();
    for s in &seen {
        let s = s.to_ascii_lowercase();
        assert!(
            ![
                "rb",
                "information_schema",
                "flush",
                "start transaction",
                "lock"
            ]
            .iter()
            .any(|w| s.contains(w)),
            "the refused source ran: {s}"
        );
    }
    let reads: Option<u64> = c
        .query_first(format!(
            "SELECT COUNT_READ FROM performance_schema.table_io_waits_summary_by_table \
             WHERE OBJECT_SCHEMA = '{DB}' AND OBJECT_NAME = 't'"
        ))
        .await
        .unwrap();
    assert!(reads.unwrap_or(0) == 0, "table read: {reads:?}");
    c.disconnect().await.ok();
}

/// The endpoint fails over, while streaming, to a promoted replica of A
/// (it executed exactly A's transactions). OFF: refused before the
/// failover is reconciled or recorded (writes fail from the switch on; the
/// identity stays A's). ON: the failover is reconciled and B streams.
async fn failover(candidate_on: bool) {
    init_test_tracing();
    let base = if candidate_on { 130 } else { 140 };
    let (_a_c, a) = start(base, true, None).await;
    let (_b_c, b) = start(base + 1, true, Some(candidate_on)).await;
    create_table(a).await;
    create_table(b).await;

    let proxy = Proxy::start(a).await;
    let st = State::new().await;
    let (h, mut rx) = st
        .run(st.source("fo", proxy.port, SnapshotMode::Never))
        .await;
    sleep(Duration::from_secs(3)).await;
    sql(a, &[format!("INSERT INTO {DB}.t VALUES (1, 1)")]).await;
    row(&mut rx, 1).await;
    assert!(st.identity_is("fo", a).await);
    // B executed exactly A's transactions, row 1 included.
    let executed: String = one(a, "SELECT @@GLOBAL.gtid_executed").await;
    sql(b, &[format!("SET GLOBAL gtid_purged = '{executed}'")]).await;

    if candidate_on {
        proxy.switch_to(b);
        sleep(Duration::from_secs(2)).await;
        sql(b, &[format!("INSERT INTO {DB}.t VALUES (2, 2)")]).await;
        row(&mut rx, 2).await;
        assert!(st.identity_is("fo", b).await);
        h.stop();
        timeout(Duration::from_secs(30), h.join).await.ok();
    } else {
        st.freeze();
        proxy.switch_to(b);
        sql(b, &[format!("INSERT INTO {DB}.t VALUES (2, 2)")]).await;
        assert!(refused(h, &mut rx).await.is_empty());
        assert!(st.identity_is("fo", a).await, "no failover recorded");
    }
}

#[tokio::test]
#[ignore = "requires docker"]
async fn a_failover_to_an_off_replica_stops_before_reconciliation() {
    failover(false).await;
}

#[tokio::test]
#[ignore = "requires docker"]
async fn a_failover_to_an_on_replica_succeeds() {
    failover(true).await;
}

/// The setting is read-only: it changes only with a restart, which ends
/// the stream. A server restarted with it OFF (same identity, same
/// endpoint) is refused on the reconnect, before any event is read: a row
/// written after the restart is never emitted.
async fn a_restart_into_off(gtid: bool) {
    init_test_tracing();
    let (c, port) = start(if gtid { 150 } else { 151 }, gtid, None).await;
    create_table(port).await;
    let proxy = Proxy::start(port).await;
    let st = State::new().await;
    let (h, mut rx) = st
        .run(st.source("restart", proxy.port, SnapshotMode::Never))
        .await;
    sleep(Duration::from_secs(3)).await;
    sql(port, &[format!("INSERT INTO {DB}.t VALUES (1, 1)")]).await;
    row(&mut rx, 1).await;

    sql(port, &["SET PERSIST_ONLY log_replica_updates = OFF".into()]).await;
    c.stop().await.unwrap();
    c.start().await.unwrap();
    let port = c.get_host_port_ipv4(3306).await.unwrap();
    ready(port).await;
    assert_eq!(
        one::<u8>(port, "SELECT @@GLOBAL.log_replica_updates").await,
        0
    );
    sql(port, &[format!("INSERT INTO {DB}.t VALUES (2, 2)")]).await;
    st.freeze();
    proxy.switch_to(port);
    let after = refused(h, &mut rx).await;
    assert!(
        !after
            .iter()
            .any(|e| e.after.as_ref().is_some_and(|a| a["id"] == 2)),
        "a row after the restart was emitted"
    );
}

#[tokio::test]
#[ignore = "requires docker"]
async fn a_restart_into_off_is_refused_on_reconnect_gtid() {
    a_restart_into_off(true).await;
}

#[tokio::test]
#[ignore = "requires docker"]
async fn a_restart_into_off_is_refused_on_reconnect_file_position() {
    a_restart_into_off(false).await;
}

/// A failover at every boundary of a transaction. The source is held inside
/// it; B (a replica with `log_replica_updates`, so its own binlog holds the
/// transaction with its original GTID) applies it and is promoted; the
/// endpoint switches to B and the stream is cut at the held point. The
/// source resumes on B from the last proven boundary: the cut transaction is
/// read again (its partial prefix aborted), none of it skipped.
async fn failover_at(
    point: sources::stream_probe::Point,
    nth: usize,
    base: u32,
) {
    use sources::stream_probe;
    init_test_tracing();
    let (a_c, a) = start(base, true, Some(true)).await;
    let (_b_c, b) = start(base + 1, true, Some(true)).await;
    create_table(a).await;
    let ip = a_c.get_bridge_ip_address().await.unwrap().to_string();
    replicate(b, &ip, true).await;
    until("the replica has the table", async || columns(b).await == 2).await;

    let proxy = Proxy::start(a).await;
    let st = State::new().await;
    let (h, mut rx) = st
        .run(st.source("fob", proxy.port, SnapshotMode::Never))
        .await;
    sleep(Duration::from_secs(3)).await;
    stream_probe::reset_event_hooks();
    let (reached, release) = stream_probe::hold_then_disconnect(point, nth);
    sql(
        a,
        &[
            "BEGIN".into(),
            format!("INSERT INTO {DB}.t VALUES (1, 1), (2, 1)"),
            format!("INSERT INTO {DB}.t VALUES (3, 1)"),
            format!("UPDATE {DB}.t SET v = 2 WHERE id = 1"),
            "COMMIT".into(),
        ],
    )
    .await;
    timeout(Duration::from_secs(30), reached.notified())
        .await
        .expect("the boundary was never reached");
    // B applies the transaction, then is promoted; the endpoint follows.
    let executed: String = one(a, "SELECT @@GLOBAL.gtid_executed").await;
    until("the replica caught up", async || {
        one::<i64>(
            b,
            &format!(
                "SELECT GTID_SUBSET('{executed}', @@GLOBAL.gtid_executed)"
            ),
        )
        .await
            == 1
    })
    .await;
    sql(b, &["STOP REPLICA".into(), "RESET REPLICA ALL".into()]).await;
    proxy.switch_to(b);
    release.notify_one();
    sql(b, &[format!("INSERT INTO {DB}.t VALUES (4, 1)")]).await;

    // What a coordinator delivers: TxAbort drops the open prefix.
    let mut delivered: Vec<Event> = Vec::new();
    let mut open: Option<Vec<Event>> = None;
    let deadline = Instant::now() + Duration::from_secs(60);
    while !delivered
        .iter()
        .any(|e| e.after.as_ref().is_some_and(|a| a["id"] == 4))
    {
        assert!(
            Instant::now() < deadline,
            "row 4 never delivered: {delivered:?}"
        );
        assert!(!h.join.is_finished(), "the source ended");
        match timeout(Duration::from_millis(500), rx.recv()).await {
            Ok(Some(SourceItem::TxBegin { .. })) => open = Some(vec![]),
            Ok(Some(SourceItem::TxAbort { .. })) => open = None,
            Ok(Some(SourceItem::TxCommit { .. })) => {
                delivered.extend(open.take().unwrap_or_default())
            }
            Ok(Some(SourceItem::Event(e)))
                if matches!(e.op, Op::Create | Op::Update | Op::Delete) =>
            {
                match open.as_mut() {
                    Some(o) => o.push(e),
                    None => delivered.push(e),
                }
            }
            _ => {}
        }
    }
    let mut last = std::collections::BTreeMap::new();
    for e in &delivered {
        let a = e.after.as_ref().unwrap();
        let (id, v) = (a["id"].as_i64().unwrap(), a["v"].as_i64().unwrap());
        if let Some(prev) = last.insert(id, v) {
            assert!(prev <= v, "row {id} went back from {prev} to {v}");
        }
    }
    assert_eq!(
        last,
        std::collections::BTreeMap::from([(1, 2), (2, 1), (3, 1), (4, 1)]),
        "complete, final state"
    );
    assert!(st.identity_is("fob", b).await, "the failover was recorded");
    stream_probe::reset_event_hooks();
    h.stop();
    timeout(Duration::from_secs(30), h.join).await.ok();
}

#[tokio::test]
#[ignore = "requires docker"]
async fn a_failover_after_a_gtid_rereads_the_transaction() {
    failover_at(sources::stream_probe::Point::AfterGtid, 1, 160).await;
}

#[tokio::test]
#[ignore = "requires docker"]
async fn a_failover_after_a_table_map_rereads_the_transaction() {
    failover_at(sources::stream_probe::Point::AfterTableMap, 1, 162).await;
}

#[tokio::test]
#[ignore = "requires docker"]
async fn a_failover_between_rows_rereads_the_transaction() {
    failover_at(sources::stream_probe::Point::AfterRows, 1, 164).await;
}

#[tokio::test]
#[ignore = "requires docker"]
async fn a_failover_before_the_xid_rereads_the_transaction() {
    failover_at(sources::stream_probe::Point::AfterRows, 3, 166).await;
}

#[tokio::test]
#[ignore = "requires docker"]
async fn a_failover_after_the_xid_continues() {
    failover_at(sources::stream_probe::Point::AfterXid, 1, 168).await;
}
