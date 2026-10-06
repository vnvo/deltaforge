//! Every MySQL connection the source trusts proves its server identity on
//! itself before it is used: replication sessions before their dump command,
//! control connections before their first query.
//!
//! A TCP proxy in front of two servers stands in for a failover endpoint, DNS
//! rotation or load balancer: it sends chosen connections to the other server.
//! The sweeps first count the connections a healthy phase opens (startup,
//! reconnect, credential rotation), then send each one in turn to the wrong
//! server. Every attempt must stop (or reject the rotation) without a row,
//! checkpoint, identity or lineage record, schema version or activation record
//! from the wrong server, and the wrong server must never see a binlog dump.
//!
//! Run with:
//! ```bash
//! cargo test -p sources --test mysql_connection_identity_e2e -- --include-ignored --test-threads=1
//! ```

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use checkpoints::{CheckpointStore, MemCheckpointStore};
use common::AllowList;
use ctor::dtor;
use deltaforge_config::{
    CredentialRotationCfg, MysqlSrcCfg, OnSchemaDrift, RotationTriggerCfg,
    SnapshotCfg, SnapshotMode, SourceCredentialsCfg,
};
use deltaforge_core::{Source, SourceError, SourceHandle, SourceItem};
use gate_ownership::GateOwned;
use mysql_async::prelude::Queryable;
use secrets::{
    FileMode, FilePolicy, FileResolver, SecretProvider, SecretReference,
    SecretResolver,
};
use sources::MySqlCheckpoint;
use sources::credentials::resolve_mysql_credentials;
use sources::failover::identity::{IdentityStore, ServerIdentity};
use sources::mysql::{MySqlSource, mysql_rotation};
use storage::adapters::test_util::FaultBackend;
use storage::adapters::{LineageDescriptor, SchemaKey, source_lineage};
use storage::{ArcStorageBackend, DurableSchemaRegistry, MemoryStorageBackend};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt, core::WaitFor, runners::AsyncRunner,
};
use tokio::sync::{OnceCell, mpsc};
use tokio::time::{Duration, sleep, timeout};

mod test_common;
use test_common::{init_test_tracing, registry_history};

const ROOT_PW: &str = "pw";
const TENANT: &str = "acme";

// ----------------------------------------------------------------------------
// Servers: a GTID pair, a file/position pair, and a failover target.
// ----------------------------------------------------------------------------

type Server = (ContainerAsync<GenericImage>, u16);
static GTID_A: OnceCell<Server> = OnceCell::const_new();
static GTID_B: OnceCell<Server> = OnceCell::const_new();
static GTID_C: OnceCell<Server> = OnceCell::const_new();
static FILE_A: OnceCell<Server> = OnceCell::const_new();
static FILE_B: OnceCell<Server> = OnceCell::const_new();

#[dtor]
fn cleanup() {
    for cell in [&GTID_A, &GTID_B, &GTID_C, &FILE_A, &FILE_B] {
        if let Some((c, _)) = cell.get() {
            std::process::Command::new("docker")
                .args(["rm", "-f", "-v", c.id()])
                .output()
                .ok();
        }
    }
}

async fn start(gtid: bool, server_id: u32) -> Server {
    let mut cmd = vec![
        format!("--server-id={server_id}"),
        "--log-bin=mysql-bin".into(),
        "--binlog-format=ROW".into(),
        "--binlog-row-image=FULL".into(),
        "--binlog-checksum=NONE".into(),
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
    while mysql_async::Conn::from_url(root_dsn(port, ""))
        .await
        .is_err()
    {
        assert!(Instant::now() < deadline, "mysql on {port} not ready");
        sleep(Duration::from_millis(500)).await;
    }
    (c, port)
}

async fn port(cell: &'static OnceCell<Server>, gtid: bool, id: u32) -> u16 {
    cell.get_or_init(|| start(gtid, id)).await.1
}

/// The (verified, wrong) server pair of a binlog mode.
async fn pair(gtid: bool) -> (u16, u16) {
    if gtid {
        (port(&GTID_A, true, 11).await, port(&GTID_B, true, 12).await)
    } else {
        (
            port(&FILE_A, false, 21).await,
            port(&FILE_B, false, 22).await,
        )
    }
}

fn root_dsn(port: u16, db: &str) -> String {
    format!("mysql://root:{ROOT_PW}@127.0.0.1:{port}/{db}")
}

async fn sql(port: u16, stmts: &[String]) {
    let mut c = mysql_async::Conn::from_url(root_dsn(port, ""))
        .await
        .unwrap();
    for s in stmts {
        c.query_drop(s).await.unwrap_or_else(|e| panic!("{s}: {e}"));
    }
    c.disconnect().await.ok();
}

async fn uuid(port: u16) -> String {
    let mut c = mysql_async::Conn::from_url(root_dsn(port, ""))
        .await
        .unwrap();
    let u: String = c
        .query_first("SELECT @@GLOBAL.server_uuid")
        .await
        .unwrap()
        .unwrap();
    c.disconnect().await.ok();
    u
}

fn lineage_hash(server_uuid: &str) -> String {
    LineageDescriptor::mysql(server_uuid)
        .unwrap()
        .lineage_hash()
}

/// `db.t` on a server, holding one row tagged with the server's name. The
/// wrong server's table has an extra column, so its shape differs too.
async fn prepare(port: u16, db: &str, tag: &str) {
    let extra = if tag == "a" { "" } else { ", b_only INT" };
    sql(
        port,
        &[
            format!("DROP DATABASE IF EXISTS {db}"),
            format!("CREATE DATABASE {db}"),
            format!(
                "CREATE TABLE {db}.t (id INT PRIMARY KEY, src VARCHAR(8){extra})"
            ),
            format!("INSERT INTO {db}.t (id, src) VALUES (0, '{tag}')"),
        ],
    )
    .await;
}

async fn insert(port: u16, db: &str, id: u32, tag: &str) {
    sql(
        port,
        &[format!(
            "INSERT INTO {db}.t (id, src) VALUES ({id}, '{tag}')"
        )],
    )
    .await;
}

// ----------------------------------------------------------------------------
// The routing proxy.
// ----------------------------------------------------------------------------

type Route = Arc<dyn Fn(u32) -> u16 + Send + Sync>;

/// Sends its n-th connection (from 0, over its lifetime) to `route(n)`.
struct Proxy {
    port: u16,
    route: Arc<Mutex<Route>>,
    opened: Arc<AtomicU32>,
    live: Arc<Mutex<Vec<tokio::task::AbortHandle>>>,
}

impl Proxy {
    async fn start(to: u16) -> Self {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        let route: Arc<Mutex<Route>> =
            Arc::new(Mutex::new(Arc::new(move |_| to)));
        let opened = Arc::new(AtomicU32::new(0));
        let live = Arc::new(Mutex::new(Vec::new()));
        let (r, o, lv) = (route.clone(), opened.clone(), live.clone());
        tokio::spawn(async move {
            while let Ok((mut client, _)) = l.accept().await {
                let n = o.fetch_add(1, Ordering::SeqCst);
                let to = (r.lock().unwrap().clone())(n);
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
            route,
            opened,
            live,
        }
    }

    fn dsn(&self, db: &str) -> String {
        root_dsn(self.port, db)
    }

    fn opened(&self) -> u32 {
        self.opened.load(Ordering::SeqCst)
    }

    fn all_to(&self, to: u16) {
        *self.route.lock().unwrap() = Arc::new(move |_| to);
    }

    /// Send the k-th connection opened from now to `odd`, all others to
    /// `normal`.
    fn divert(&self, k: u32, odd: u16, normal: u16) {
        let at = self.opened() + k;
        *self.route.lock().unwrap() =
            Arc::new(move |n| if n == at { odd } else { normal });
    }

    /// Drop every open connection (the source sees its stream end).
    fn sever(&self) {
        for t in self.live.lock().unwrap().drain(..) {
            t.abort();
        }
    }

    /// Connections opened since `base`, once no new one has appeared for
    /// `quiet`.
    async fn settled_since(&self, base: u32, quiet: Duration) -> u32 {
        let mut last = self.opened();
        loop {
            sleep(quiet).await;
            let now = self.opened();
            if now == last {
                return now - base;
            }
            last = now;
        }
    }
}

/// Records whether `port` ever shows a binlog dump thread while it runs.
struct DumpWatch {
    seen: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl DumpWatch {
    fn start(port: u16) -> Self {
        let (seen, stop) = (
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
        );
        let (s, st) = (seen.clone(), stop.clone());
        let task = tokio::spawn(async move {
            let mut c = mysql_async::Conn::from_url(root_dsn(port, ""))
                .await
                .unwrap();
            while !st.load(Ordering::SeqCst) {
                let n: Option<i64> = c
                    .query_first(
                        "SELECT COUNT(*) FROM information_schema.PROCESSLIST \
                         WHERE COMMAND LIKE 'Binlog Dump%'",
                    )
                    .await
                    .unwrap();
                if n.unwrap_or(0) > 0 {
                    s.store(true, Ordering::SeqCst);
                }
                sleep(Duration::from_millis(50)).await;
            }
        });
        Self { seen, stop, task }
    }

    async fn saw_dump(self) -> bool {
        self.stop.store(true, Ordering::SeqCst);
        self.task.await.ok();
        self.seen.load(Ordering::SeqCst)
    }
}

// ----------------------------------------------------------------------------
// Sources and what they leave behind.
// ----------------------------------------------------------------------------

struct State {
    backend: ArcStorageBackend,
    ckpt: Arc<dyn CheckpointStore>,
    registry: Arc<DurableSchemaRegistry>,
}

impl State {
    async fn new() -> Self {
        Self::over(Arc::new(MemoryStorageBackend::new())).await
    }

    /// State whose storage writes can be failed per namespace.
    async fn faulty() -> (Self, Arc<FaultBackend>) {
        let fault = Arc::new(FaultBackend::new());
        (Self::over(fault.clone()).await, fault)
    }

    async fn over(backend: ArcStorageBackend) -> Self {
        Self {
            registry: DurableSchemaRegistry::new(backend.clone())
                .await
                .unwrap(),
            ckpt: Arc::new(MemCheckpointStore::new().unwrap()),
            backend,
        }
    }

    async fn identity(&self, id: &str) -> Option<String> {
        match IdentityStore::new(self.backend.clone())
            .load(id)
            .await
            .unwrap()
        {
            Some(ServerIdentity::MySql(i)) => Some(i.server_uuid),
            None => None,
            Some(other) => panic!("identity {other:?}"),
        }
    }

    async fn lineage(&self, id: &str) -> Option<(String, Option<String>)> {
        source_lineage::load(&self.backend, TENANT, id)
            .await
            .unwrap()
            .map(|r| {
                (r.current.lineage_hash, r.previous.map(|p| p.lineage_hash))
            })
    }

    /// Nothing durable from the server `wrong` (or anything but `right`).
    async fn assert_nothing_from(
        &self,
        id: &str,
        db: &str,
        right: &str,
        wrong: &str,
        what: &str,
    ) {
        for key in self.ckpt.list().await.unwrap() {
            let raw = self.ckpt.get_raw(&key).await.unwrap().unwrap();
            if let Ok(cp) = serde_json::from_slice::<MySqlCheckpoint>(&raw) {
                assert_eq!(
                    cp.lineage.as_deref(),
                    Some(lineage_hash(right).as_str()),
                    "{what}: checkpoint {key} not from the verified server"
                );
            }
        }
        match IdentityStore::new(self.backend.clone())
            .load(id)
            .await
            .unwrap()
        {
            None => {}
            Some(ServerIdentity::MySql(i)) => assert_eq!(
                i.server_uuid, right,
                "{what}: identity record names another server"
            ),
            Some(other) => panic!("{what}: identity {other:?}"),
        }
        if let Some(rec) = source_lineage::load(&self.backend, TENANT, id)
            .await
            .unwrap()
        {
            let wrong_hash = lineage_hash(wrong);
            assert_ne!(rec.current.lineage_hash, wrong_hash, "{what}: lineage");
            assert!(
                rec.previous.as_ref().map(|p| &p.lineage_hash)
                    != Some(&wrong_hash),
                "{what}: lineage edge from the wrong server"
            );
        }
        let key = SchemaKey::new(TENANT, id, lineage_hash(wrong), db, "t");
        assert!(
            registry_history(&self.registry, &key).await.is_empty(),
            "{what}: schema recorded under the wrong server's lineage"
        );
        // No activation record (table stream, database or lineage barrier)
        // under the wrong server's lineage.
        let wrong_hash = lineage_hash(wrong);
        for (ns, stream) in [
            ("schemas.v1.activation", key.backend_key()),
            (
                "schemas.v1.activation.barrier",
                SchemaKey::new(TENANT, id, wrong_hash.as_str(), db, "")
                    .backend_key(),
            ),
            (
                "schemas.v1.activation.barrier",
                SchemaKey::source_prefix(TENANT, id, &wrong_hash),
            ),
        ] {
            assert!(
                self.backend.log_list(ns, &stream).await.unwrap().is_empty(),
                "{what}: activation record under the wrong lineage in {ns}"
            );
        }
    }
}

/// Every stored checkpoint key and its bytes.
async fn stored(st: &State) -> Vec<(String, Vec<u8>)> {
    let mut out = Vec::new();
    for key in st.ckpt.list().await.unwrap() {
        out.push((key.clone(), st.ckpt.get_raw(&key).await.unwrap().unwrap()));
    }
    out.sort();
    out
}

/// The lineage stamped on the source's aggregate checkpoint.
async fn stored_lineage(st: &State, id: &str) -> Option<String> {
    let raw = st.ckpt.get_raw(id).await.unwrap()?;
    serde_json::from_slice::<MySqlCheckpoint>(&raw)
        .unwrap()
        .lineage
}

struct Run {
    handle: SourceHandle,
    rx: mpsc::Receiver<SourceItem>,
}

fn source(
    id: &str,
    dsn: String,
    db: &str,
    st: &State,
    drift: OnSchemaDrift,
) -> MySqlSource {
    MySqlSource {
        id: id.into(),
        dsn: dsn.as_str().into(),
        tables: vec![format!("{db}.t")],
        tenant: TENANT.into(),
        pipeline: "test".into(),
        registry: st.registry.clone(),
        registry_scope: sources::registry_scope::SharedRegistryScope::default(),
        backend: st.backend.clone(),
        outbox_tables: AllowList::default(),
        snapshot_cfg: SnapshotCfg::default(),
        on_schema_drift: drift,
        table_options: Default::default(),
        rotation: None,
        snapshot_cohort: Default::default(),
    }
}

async fn run(src: MySqlSource, st: &State) -> Run {
    let (tx, rx) = test_common::acked_channel(&src, &st.ckpt, &src.id, 1024);
    let handle = src.run(tx, st.ckpt.clone()).await;
    Run { handle, rx }
}

/// Collect `(id, src)` of row events until `want` arrives or `dur` passes.
async fn rows_until(
    rx: &mut mpsc::Receiver<SourceItem>,
    want: Option<i64>,
    dur: Duration,
) -> Vec<(i64, String)> {
    let deadline = Instant::now() + dur;
    let mut out = Vec::new();
    while let Some(left) = deadline.checked_duration_since(Instant::now()) {
        match timeout(left, rx.recv()).await {
            Ok(Some(SourceItem::Event(e))) => {
                if let Some(row) = e.after.as_ref().or(e.before.as_ref()) {
                    let id = row.get("id").and_then(|v| v.as_i64());
                    let src = row.get("src").and_then(|v| v.as_str());
                    if let (Some(id), Some(src)) = (id, src) {
                        out.push((id, src.to_string()));
                        if want == Some(id) {
                            break;
                        }
                    }
                }
            }
            Ok(Some(_)) => {}
            _ => break,
        }
    }
    out
}

/// The source must stop with a typed lineage error.
async fn assert_lineage_stop(handle: SourceHandle, what: &str) {
    let res = timeout(Duration::from_secs(90), handle.join)
        .await
        .unwrap_or_else(|_| panic!("{what}: source did not stop"))
        .expect("source task");
    assert!(
        matches!(res, Err(SourceError::Lineage { .. })),
        "{what}: expected a lineage error, got {res:?}"
    );
}

async fn stop(handle: SourceHandle) {
    handle.stop();
    timeout(Duration::from_secs(30), handle.join).await.ok();
}

// ----------------------------------------------------------------------------
// Startup: every connection before the stream is ready.
// ----------------------------------------------------------------------------

/// A source that runs an initial snapshot first (lock, worker and position
/// connections) when `snapshot` is set.
fn startup_source(
    id: &str,
    dsn: String,
    db: &str,
    st: &State,
    snapshot: bool,
) -> MySqlSource {
    let mut src = source(id, dsn, db, st, OnSchemaDrift::Adapt);
    if snapshot {
        src.snapshot_cfg.mode = SnapshotMode::Initial;
    }
    src
}

async fn startup_sweep(gtid: bool, snapshot: bool) {
    init_test_tracing();
    let (a, b) = pair(gtid).await;
    let (ua, ub) = (uuid(a).await, uuid(b).await);
    let db = match (gtid, snapshot) {
        (true, false) => "ident_start_g",
        (false, false) => "ident_start_f",
        (_, true) => "ident_start_snap",
    };
    prepare(a, db, "a").await;
    prepare(b, db, "b").await;
    let proxy = Proxy::start(a).await;

    // A healthy startup through the proxy: how many connections it opens.
    let n = {
        let st = State::new().await;
        let base = proxy.opened();
        let mut r = run(
            startup_source(
                &format!("{db}_cal"),
                proxy.dsn(db),
                db,
                &st,
                snapshot,
            ),
            &st,
        )
        .await;
        let n = proxy.settled_since(base, Duration::from_secs(3)).await;
        insert(a, db, 1, "a").await;
        let rows =
            rows_until(&mut r.rx, Some(1), Duration::from_secs(30)).await;
        assert!(rows.contains(&(1, "a".into())), "calibration streams");
        stop(r.handle).await;
        // The resume position is in the server's own mode: a GTID set in
        // GTID mode, file/position (never an empty GTID set) otherwise.
        let raw = st
            .ckpt
            .get_raw(&format!("{db}_cal"))
            .await
            .unwrap()
            .unwrap();
        let cp: MySqlCheckpoint = serde_json::from_slice(&raw).unwrap();
        if gtid {
            assert!(
                cp.gtid_set.as_ref().is_some_and(|g| !g.is_empty()),
                "{cp:?}"
            );
        } else {
            assert!(cp.gtid_set.is_none() && cp.pos > 4, "{cp:?}");
        }
        n
    };
    assert!(n >= 3, "a startup opens several connections, saw {n}");

    for k in 0..n {
        let what = format!("startup gtid={gtid} snapshot={snapshot} k={k}/{n}");
        let st = State::new().await;
        let id = format!("{db}_{k}");
        let watch = DumpWatch::start(b);
        proxy.divert(k, b, a);
        let mut r =
            run(startup_source(&id, proxy.dsn(db), db, &st, snapshot), &st)
                .await;
        assert_lineage_stop(r.handle, &what).await;
        let rows =
            rows_until(&mut r.rx, None, Duration::from_millis(200)).await;
        assert!(
            rows.iter().all(|(_, s)| s != "b"),
            "{what}: row from the wrong server {rows:?}"
        );
        st.assert_nothing_from(&id, db, &ua, &ub, &what).await;
        assert!(
            !watch.saw_dump().await,
            "{what}: binlog dump on the wrong server"
        );
    }
    proxy.all_to(a);
}

#[tokio::test]
#[ignore = "requires docker"]
async fn startup_connections_prove_the_server_gtid() {
    startup_sweep(true, false).await;
}

#[tokio::test]
#[ignore = "requires docker"]
async fn snapshot_connections_prove_the_server() {
    startup_sweep(true, true).await;
}

#[tokio::test]
#[ignore = "requires docker"]
async fn startup_connections_prove_the_server_file_position() {
    startup_sweep(false, false).await;
}

// ----------------------------------------------------------------------------
// Reconnect: every connection from a dropped stream to the next row.
// ----------------------------------------------------------------------------

async fn reconnect_sweep(gtid: bool) {
    init_test_tracing();
    let (a, b) = pair(gtid).await;
    let (ua, ub) = (uuid(a).await, uuid(b).await);
    let db = if gtid {
        "ident_recon_g"
    } else {
        "ident_recon_f"
    };
    prepare(a, db, "a").await;
    prepare(b, db, "b").await;
    let proxy = Proxy::start(a).await;
    let mut next_id = 1u32;
    let mut row = || {
        next_id += 1;
        next_id
    };

    // A healthy reconnect: how many connections it opens.
    let n = {
        let st = State::new().await;
        let mut r = run(
            source(
                &format!("{db}_cal"),
                proxy.dsn(db),
                db,
                &st,
                OnSchemaDrift::Adapt,
            ),
            &st,
        )
        .await;
        let first = row();
        proxy
            .settled_since(proxy.opened(), Duration::from_secs(3))
            .await;
        insert(a, db, first, "a").await;
        let got =
            rows_until(&mut r.rx, Some(first.into()), Duration::from_secs(30))
                .await;
        assert!(
            got.contains(&(first.into(), "a".into())),
            "calibration streams"
        );
        proxy
            .settled_since(proxy.opened(), Duration::from_secs(2))
            .await;
        let base = proxy.opened();
        proxy.sever();
        let second = row();
        insert(a, db, second, "a").await;
        let got =
            rows_until(&mut r.rx, Some(second.into()), Duration::from_secs(60))
                .await;
        assert!(
            got.contains(&(second.into(), "a".into())),
            "calibration reconnects"
        );
        let n = proxy.settled_since(base, Duration::from_secs(3)).await;
        stop(r.handle).await;
        n
    };
    assert!(n >= 1, "a reconnect opens a session, saw {n}");

    for k in 0..n {
        let what = format!("reconnect gtid={gtid} k={k}/{n}");
        let st = State::new().await;
        let id = format!("{db}_{k}");
        let mut r = run(
            source(&id, proxy.dsn(db), db, &st, OnSchemaDrift::Adapt),
            &st,
        )
        .await;
        let first = row();
        proxy
            .settled_since(proxy.opened(), Duration::from_secs(3))
            .await;
        insert(a, db, first, "a").await;
        let got =
            rows_until(&mut r.rx, Some(first.into()), Duration::from_secs(30))
                .await;
        assert!(got.contains(&(first.into(), "a".into())), "{what}: streams");
        proxy
            .settled_since(proxy.opened(), Duration::from_secs(2))
            .await;

        let watch = DumpWatch::start(b);
        proxy.divert(k, b, a);
        proxy.sever();
        insert(a, db, row(), "a").await;
        insert(b, db, 5000 + k, "b").await;
        assert_lineage_stop(r.handle, &what).await;
        let rows =
            rows_until(&mut r.rx, None, Duration::from_millis(200)).await;
        assert!(
            rows.iter().all(|(_, s)| s != "b"),
            "{what}: row from the wrong server {rows:?}"
        );
        st.assert_nothing_from(&id, db, &ua, &ub, &what).await;
        assert!(
            !watch.saw_dump().await,
            "{what}: binlog dump on the wrong server"
        );
        proxy.all_to(a);
    }
}

#[tokio::test]
#[ignore = "requires docker"]
async fn reconnect_connections_prove_the_server_gtid() {
    reconnect_sweep(true).await;
}

#[tokio::test]
#[ignore = "requires docker"]
async fn reconnect_connections_prove_the_server_file_position() {
    reconnect_sweep(false).await;
}

// ----------------------------------------------------------------------------
// Forward proof: every connection a live DDL's proof opens.
// ----------------------------------------------------------------------------

/// The next DDL event containing `needle`, or `None` when the stream ends.
async fn ddl_until(
    rx: &mut mpsc::Receiver<SourceItem>,
    needle: &str,
    dur: Duration,
) -> bool {
    let deadline = Instant::now() + dur;
    while let Some(left) = deadline.checked_duration_since(Instant::now()) {
        match timeout(left, rx.recv()).await {
            Ok(Some(SourceItem::Event(e)))
                if e.ddl
                    .as_ref()
                    .is_some_and(|d| d.to_string().contains(needle)) =>
            {
                return true;
            }
            Ok(Some(_)) => {}
            _ => return false,
        }
    }
    false
}

/// Run the source until startup settles, then stop it cleanly: a committed
/// position to restart (and replay) from.
async fn commit_a_position(proxy: &Proxy, st: &State, id: &str, db: &str) {
    let r =
        run(source(id, proxy.dsn(db), db, st, OnSchemaDrift::Adapt), st).await;
    proxy
        .settled_since(proxy.opened(), Duration::from_secs(3))
        .await;
    stop(r.handle).await;
}

#[tokio::test]
#[ignore = "requires docker"]
async fn forward_proof_connections_prove_the_server() {
    init_test_tracing();
    let (a, b) = pair(true).await;
    let (ua, ub) = (uuid(a).await, uuid(b).await);
    let db = "ident_forward";
    prepare(a, db, "a").await;
    prepare(b, db, "b").await;
    let proxy = Proxy::start(a).await;
    let mut column = 0u32;
    let mut alter = || {
        column += 1;
        format!("ALTER TABLE {db}.t ADD COLUMN c{column} INT")
    };

    // The DDL and a later unrelated write happen while the source is
    // stopped, so the restart replays the DDL and its proof scans a
    // non-empty (D, S] (the capture lands after the write): the proof opens
    // a capture connection and a scan session.
    // A healthy replayed DDL: how many connections the restart opens until
    // the DDL event (startup, then the proof's capture and scan).
    let n = {
        let st = State::new().await;
        let id = "ident_forward_cal".to_string();
        commit_a_position(&proxy, &st, &id, db).await;
        let stmt = alter();
        sql(
            a,
            &[
                stmt.clone(),
                format!("INSERT INTO {db}.t (id, src) VALUES (100, 'a')"),
            ],
        )
        .await;
        let base = proxy.opened();
        let mut r = run(
            source(&id, proxy.dsn(db), db, &st, OnSchemaDrift::Adapt),
            &st,
        )
        .await;
        assert!(ddl_until(&mut r.rx, &stmt, Duration::from_secs(60)).await);
        let n = proxy.settled_since(base, Duration::from_secs(3)).await;
        stop(r.handle).await;
        n
    };

    for k in 0..n {
        let what = format!("forward proof k={k}/{n}");
        let st = State::new().await;
        let id = format!("ident_forward_{k}");
        commit_a_position(&proxy, &st, &id, db).await;
        let stmt = alter();
        sql(
            a,
            &[
                stmt.clone(),
                format!(
                    "INSERT INTO {db}.t (id, src) VALUES ({}, 'a')",
                    200 + k
                ),
            ],
        )
        .await;
        let watch = DumpWatch::start(b);
        proxy.divert(k, b, a);
        let mut r = run(
            source(&id, proxy.dsn(db), db, &st, OnSchemaDrift::Adapt),
            &st,
        )
        .await;
        insert(b, db, 9900 + k, "b").await;
        assert_lineage_stop(r.handle, &what).await;
        let rows =
            rows_until(&mut r.rx, None, Duration::from_millis(200)).await;
        assert!(rows.iter().all(|(_, s)| s != "b"), "{what}: {rows:?}");
        st.assert_nothing_from(&id, db, &ua, &ub, &what).await;
        assert!(
            !watch.saw_dump().await,
            "{what}: binlog dump on the wrong server"
        );
        proxy.all_to(a);
    }
}

// ----------------------------------------------------------------------------
// Failover: genuine, and rejected by reconciliation.
// ----------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires docker"]
async fn a_genuine_online_failover_is_reconciled_then_streams() {
    init_test_tracing();
    let a = port(&GTID_A, true, 11).await;
    let c = port(&GTID_C, true, 13).await;
    let (ua, uc) = (uuid(a).await, uuid(c).await);
    let db = "ident_failover";
    prepare(a, db, "a").await;
    // The promoted server has the same table shape.
    sql(
        c,
        &[
            format!("DROP DATABASE IF EXISTS {db}"),
            format!("CREATE DATABASE {db}"),
            format!("CREATE TABLE {db}.t (id INT PRIMARY KEY, src VARCHAR(8))"),
        ],
    )
    .await;
    let proxy = Proxy::start(a).await;
    let st = State::new().await;
    let id = "ident_failover";

    // Run 1 on A, stopped cleanly: a checkpoint of A's lineage is stored.
    let mut r = run(
        source(id, proxy.dsn(db), db, &st, OnSchemaDrift::Adapt),
        &st,
    )
    .await;
    proxy
        .settled_since(proxy.opened(), Duration::from_secs(3))
        .await;
    insert(a, db, 1, "a").await;
    let got = rows_until(&mut r.rx, Some(1), Duration::from_secs(30)).await;
    assert!(got.contains(&(1, "a".into())));
    stop(r.handle).await;
    assert_eq!(stored_lineage(&st, id).await, Some(lineage_hash(&ua)));

    // Run 2 resumes on A, then the endpoint fails over to C online.
    let mut r = run(
        source(id, proxy.dsn(db), db, &st, OnSchemaDrift::Adapt),
        &st,
    )
    .await;
    proxy
        .settled_since(proxy.opened(), Duration::from_secs(3))
        .await;
    // No new A transaction: C's history below then matches the stored
    // checkpoint exactly (a promoted replica that applied everything).

    // C executed everything A did (a promoted replica).
    let mut conn = mysql_async::Conn::from_url(root_dsn(a, "")).await.unwrap();
    let executed: String = conn
        .query_first("SELECT @@GLOBAL.gtid_executed")
        .await
        .unwrap()
        .unwrap();
    conn.disconnect().await.ok();
    sql(
        c,
        &[format!(
            "SET GLOBAL gtid_purged = '+{}'",
            executed.replace('\n', "")
        )],
    )
    .await;

    proxy.all_to(c);
    proxy.sever();
    insert(c, db, 3000, "c").await;
    let got = rows_until(&mut r.rx, Some(3000), Duration::from_secs(90)).await;
    assert!(got.contains(&(3000, "c".into())), "streams on C: {got:?}");

    // Reconciled, then recorded: C is the lineage, with the edge from A, and
    // the identity authority.
    let rec = source_lineage::load(&st.backend, TENANT, id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rec.current.lineage_hash, lineage_hash(&uc));
    assert_eq!(
        rec.previous.map(|p| p.lineage_hash),
        Some(lineage_hash(&ua))
    );
    match IdentityStore::new(st.backend.clone())
        .load(id)
        .await
        .unwrap()
    {
        Some(ServerIdentity::MySql(i)) => assert_eq!(i.server_uuid, uc),
        other => panic!("identity {other:?}"),
    }

    // A crash now (no stop-time checkpoint) leaves A's checkpoint. The next
    // start carries it over to C (its GTIDs verified on C) and streams.
    r.handle.join.abort();
    assert_eq!(stored_lineage(&st, id).await, Some(lineage_hash(&ua)));
    let mut r = run(
        source(id, proxy.dsn(db), db, &st, OnSchemaDrift::Adapt),
        &st,
    )
    .await;
    proxy
        .settled_since(proxy.opened(), Duration::from_secs(3))
        .await;
    assert_eq!(
        stored_lineage(&st, id).await,
        Some(lineage_hash(&uc)),
        "the predecessor checkpoint is carried over"
    );
    insert(c, db, 3001, "c").await;
    let got = rows_until(&mut r.rx, Some(3001), Duration::from_secs(30)).await;
    assert!(got.contains(&(3001, "c".into())), "streams after restart");
    stop(r.handle).await;
}

#[tokio::test]
#[ignore = "requires docker"]
async fn a_failover_rejected_by_reconciliation_changes_nothing_durable() {
    init_test_tracing();
    let (a, b) = pair(true).await;
    let (ua, ub) = (uuid(a).await, uuid(b).await);
    let db = "ident_rejected";
    prepare(a, db, "a").await;
    prepare(b, db, "b").await; // different shape; A's history absent on B
    let proxy = Proxy::start(a).await;
    let st = State::new().await;
    let id = "ident_rejected";
    // Run 1 on A, stopped cleanly: A's checkpoint is stored.
    let r =
        run(source(id, proxy.dsn(db), db, &st, OnSchemaDrift::Halt), &st).await;
    proxy
        .settled_since(proxy.opened(), Duration::from_secs(3))
        .await;
    stop(r.handle).await;
    let checkpoints = stored(&st).await;
    let lineage_before = source_lineage::load(&st.backend, TENANT, id)
        .await
        .unwrap()
        .map(|r| (r.current.lineage_hash, r.previous.map(|p| p.lineage_hash)));
    assert!(!checkpoints.is_empty());

    let mut r =
        run(source(id, proxy.dsn(db), db, &st, OnSchemaDrift::Halt), &st).await;
    proxy
        .settled_since(proxy.opened(), Duration::from_secs(3))
        .await;
    insert(a, db, 1, "a").await;
    let got = rows_until(&mut r.rx, Some(1), Duration::from_secs(30)).await;
    assert!(got.contains(&(1, "a".into())));

    let watch = DumpWatch::start(b);
    proxy.all_to(b);
    proxy.sever();
    let res = timeout(Duration::from_secs(90), r.handle.join)
        .await
        .expect("source stops")
        .expect("source task");
    assert!(res.is_err(), "reconciliation must refuse B");
    let rows = rows_until(&mut r.rx, None, Duration::from_millis(200)).await;
    assert!(rows.iter().all(|(_, s)| s != "b"), "{rows:?}");
    st.assert_nothing_from(id, db, &ua, &ub, "rejected failover")
        .await;
    // The old durable lineage and checkpoints are unchanged.
    assert_eq!(stored(&st).await, checkpoints, "checkpoints changed");
    assert_eq!(
        source_lineage::load(&st.backend, TENANT, id)
            .await
            .unwrap()
            .map(|r| (
                r.current.lineage_hash,
                r.previous.map(|p| p.lineage_hash)
            )),
        lineage_before,
        "lineage record changed"
    );
    assert!(
        !watch.saw_dump().await,
        "binlog dump on the rejected server"
    );
    proxy.all_to(a);
}

// ----------------------------------------------------------------------------
// Failover continuation: exact resume position, the stored reconciliation
// record, and schema reloads.
// ----------------------------------------------------------------------------

/// A's `@@GLOBAL.gtid_executed`.
async fn executed(port: u16) -> String {
    let mut c = mysql_async::Conn::from_url(root_dsn(port, ""))
        .await
        .unwrap();
    let s: String = c
        .query_first("SELECT @@GLOBAL.gtid_executed")
        .await
        .unwrap()
        .unwrap();
    c.disconnect().await.ok();
    s.replace('\n', "")
}

/// A fresh GTID server holding `db.t` (with an extra column when `drift`)
/// that has executed `set` of another server (a promoted replica).
async fn promoted(id: u32, db: &str, drift: bool, set: &str) -> Server {
    let srv = start(true, id).await;
    let extra = if drift { ", b_only INT" } else { "" };
    sql(
        srv.1,
        &[
            format!("CREATE DATABASE {db}"),
            format!(
                "CREATE TABLE {db}.t (id INT PRIMARY KEY, src VARCHAR(8){extra})"
            ),
            format!("SET GLOBAL gtid_purged = '+{set}'"),
        ],
    )
    .await;
    srv
}

/// Run a source on A until it streams, stop it cleanly (A's checkpoint is
/// stored), and start it again on A. Returns the second run.
async fn streaming_on_a(
    proxy: &Proxy,
    a: u16,
    id: &str,
    db: &str,
    st: &State,
    drift: OnSchemaDrift,
) -> Run {
    let mut r = run(source(id, proxy.dsn(db), db, st, drift.clone()), st).await;
    proxy
        .settled_since(proxy.opened(), Duration::from_secs(3))
        .await;
    insert(a, db, 1, "a").await;
    let got = rows_until(&mut r.rx, Some(1), Duration::from_secs(30)).await;
    assert!(got.contains(&(1, "a".into())), "streams on A");
    stop(r.handle).await;
    let r = run(source(id, proxy.dsn(db), db, st, drift), st).await;
    proxy
        .settled_since(proxy.opened(), Duration::from_secs(3))
        .await;
    r
}

async fn stopped(handle: SourceHandle) -> Result<(), SourceError> {
    timeout(Duration::from_secs(90), handle.join)
        .await
        .expect("source stops")
        .expect("source task")
}

#[tokio::test]
#[ignore = "requires docker"]
async fn failover_must_prove_the_exact_resume_set_not_the_checkpoint() {
    init_test_tracing();
    let a = port(&GTID_A, true, 11).await;
    let ua = uuid(a).await;
    let db = "ident_exact";
    prepare(a, db, "a").await;
    let proxy = Proxy::start(a).await;
    let st = State::new().await;
    let id = "ident_exact";
    let mut r =
        streaming_on_a(&proxy, a, id, db, &st, OnSchemaDrift::Adapt).await;
    // The stored checkpoint ends here; the target executed exactly it.
    let checkpoint_set = executed(a).await;
    // One more A transaction is consumed: the reconnect resumes after it.
    insert(a, db, 2, "a").await;
    let got = rows_until(&mut r.rx, Some(2), Duration::from_secs(30)).await;
    assert!(got.contains(&(2, "a".into())));
    let (_g, g) = promoted(31, db, false, &checkpoint_set).await;
    let ug = uuid(g).await;
    let before = (stored(&st).await, st.lineage(id).await);

    let watch = DumpWatch::start(g);
    proxy.all_to(g);
    proxy.sever();
    insert(g, db, 7000, "g").await;
    let res = stopped(r.handle).await;
    assert!(
        matches!(
            res.as_ref().map_err(|e| e.root()),
            Err(SourceError::Checkpoint { .. })
        ),
        "the unproven resume set stops the failover: {res:?}"
    );
    let rows = rows_until(&mut r.rx, None, Duration::from_millis(200)).await;
    assert!(rows.iter().all(|(_, s)| s != "g"), "{rows:?}");
    assert_eq!((stored(&st).await, st.lineage(id).await), before);
    assert_eq!(st.identity(id).await, Some(ua.clone()));
    st.assert_nothing_from(id, db, &ua, &ug, "exact resume set")
        .await;
    assert!(
        !watch.saw_dump().await,
        "binlog dump on the unproven server"
    );
    proxy.all_to(a);
}

#[tokio::test]
#[ignore = "requires docker"]
async fn file_position_failover_stops_even_when_the_file_name_matches() {
    init_test_tracing();
    let (a, b) = pair(false).await;
    let (ua, ub) = (uuid(a).await, uuid(b).await);
    let db = "ident_samefile";
    prepare(a, db, "a").await;
    prepare(b, db, "a").await; // same shape: only the position is at stake
    let proxy = Proxy::start(a).await;
    let st = State::new().await;
    let id = "ident_samefile";
    let mut r =
        streaming_on_a(&proxy, a, id, db, &st, OnSchemaDrift::Adapt).await;
    insert(a, db, 2, "a").await;
    let got = rows_until(&mut r.rx, Some(2), Duration::from_secs(30)).await;
    assert!(got.contains(&(2, "a".into())));

    // B deliberately has A's current binlog file name.
    let mut c = mysql_async::Conn::from_url(root_dsn(a, "")).await.unwrap();
    let row: mysql_async::Row = c
        .query_first("SHOW BINARY LOG STATUS")
        .await
        .unwrap()
        .unwrap();
    let a_file: String = row.get(0).unwrap();
    c.disconnect().await.ok();
    for _ in 0..50 {
        let mut c = mysql_async::Conn::from_url(root_dsn(b, "")).await.unwrap();
        let files: Vec<String> = c
            .query_map("SHOW BINARY LOGS", |r: mysql_async::Row| {
                r.get::<String, _>(0).unwrap()
            })
            .await
            .unwrap();
        if files.contains(&a_file) {
            break;
        }
        c.query_drop("FLUSH BINARY LOGS").await.unwrap();
    }
    let before = (stored(&st).await, st.lineage(id).await);

    let watch = DumpWatch::start(b);
    proxy.all_to(b);
    proxy.sever();
    insert(b, db, 8000, "b").await;
    assert_lineage_stop(r.handle, "file/position failover").await;
    let rows = rows_until(&mut r.rx, None, Duration::from_millis(200)).await;
    assert!(rows.iter().all(|(id, _)| *id < 8000), "{rows:?}");
    assert_eq!((stored(&st).await, st.lineage(id).await), before);
    assert_eq!(st.identity(id).await, Some(ua.clone()));
    st.assert_nothing_from(id, db, &ua, &ub, "file/position failover")
        .await;
    assert!(!watch.saw_dump().await, "binlog dump on the other server");
    proxy.all_to(a);
}

/// Fail over to a drifted promoted replica with the lineage write failing:
/// the reconciliation record is persisted, the lineage is not published.
async fn interrupted_before_lineage_publication(
    drift_policy: OnSchemaDrift,
    db: &str,
    server_id: u32,
) -> (State, Proxy, u16, Server, String) {
    let a = port(&GTID_A, true, 11).await;
    let ua = uuid(a).await;
    prepare(a, db, "a").await;
    let proxy = Proxy::start(a).await;
    let (st, fault) = State::faulty().await;
    let id = db.to_string();
    let r = streaming_on_a(&proxy, a, &id, db, &st, drift_policy.clone()).await;
    let target = promoted(server_id, db, true, &executed(a).await).await;
    let before = stored(&st).await;

    fault
        .fail_writes_to
        .lock()
        .unwrap()
        .push("schema_lineage".into());
    let watch = DumpWatch::start(target.1);
    proxy.all_to(target.1);
    proxy.sever();
    let res = stopped(r.handle).await;
    assert!(res.is_err(), "the interrupted failover stops: {res:?}");
    fault.fail_writes_to.lock().unwrap().clear();
    // Nothing moved: lineage, identity and checkpoints are A's.
    assert_eq!(st.lineage(&id).await.map(|l| l.0), Some(lineage_hash(&ua)));
    assert_eq!(st.identity(&id).await, Some(ua));
    assert_eq!(stored(&st).await, before);
    assert!(!watch.saw_dump().await, "binlog dump before reconciliation");
    (st, proxy, a, target, id)
}

#[tokio::test]
#[ignore = "requires docker"]
async fn a_failover_record_persisted_before_lineage_publication_is_resumed_adapt()
 {
    init_test_tracing();
    let db = "ident_rec_adapt";
    let (st, proxy, a, (_t, t), id) =
        interrupted_before_lineage_publication(OnSchemaDrift::Adapt, db, 32)
            .await;
    let ut = uuid(t).await;
    // Restart on the target: the stored record drives the reload, then the
    // stream opens.
    let mut r = run(
        source(&id, proxy.dsn(db), db, &st, OnSchemaDrift::Adapt),
        &st,
    )
    .await;
    insert(t, db, 9000, "t").await;
    let got = rows_until(&mut r.rx, Some(9000), Duration::from_secs(60)).await;
    assert!(got.contains(&(9000, "t".into())), "streams on the target");
    assert_eq!(st.identity(&id).await, Some(ut.clone()));
    let key = SchemaKey::new(TENANT, id.as_str(), lineage_hash(&ut), db, "t");
    let versions = registry_history(&st.registry, &key).await;
    assert!(
        versions
            .iter()
            .any(|v| v.schema_json.to_string().contains("b_only")),
        "the drifted shape is registered under the target lineage"
    );
    stop(r.handle).await;
    proxy.all_to(a);
}

#[tokio::test]
#[ignore = "requires docker"]
async fn a_failover_record_persisted_before_lineage_publication_is_resumed_halt()
 {
    init_test_tracing();
    let db = "ident_rec_halt";
    let (st, proxy, a, (_t, t), id) =
        interrupted_before_lineage_publication(OnSchemaDrift::Halt, db, 33)
            .await;
    let ut = uuid(t).await;
    // Drift is decided lazily per table (spec 7.22): the restart completes
    // the reconciliation, then the table's first event under the target
    // stops the source under halt - before anything is registered or
    // emitted for it - and, as nothing records completion, every restart
    // stops again.
    for attempt in 0..2 {
        let mut r = run(
            source(&id, proxy.dsn(db), db, &st, OnSchemaDrift::Halt),
            &st,
        )
        .await;
        insert(t, db, 9100 + attempt, "t").await;
        let res = stopped(r.handle).await;
        assert!(
            matches!(res.as_ref().map_err(|e| e.root()), Err(SourceError::Schema { details }) if details.contains("on_schema_drift=halt")),
            "restart {attempt} halts again: {res:?}"
        );
        let rows =
            rows_until(&mut r.rx, None, Duration::from_millis(200)).await;
        assert!(rows.is_empty(), "{rows:?}");
        assert_eq!(st.identity(&id).await, Some(ut.clone()));
        let key =
            SchemaKey::new(TENANT, id.as_str(), lineage_hash(&ut), db, "t");
        assert!(
            registry_history(&st.registry, &key).await.is_empty(),
            "nothing registered under the target while halted"
        );
    }
    proxy.all_to(a);
}

/// After a failover, a table's drift check registers the proven shape under
/// the new lineage at the table's first event (spec 7.22). When that write
/// fails, the source stops before the table's rows are emitted; nothing
/// records completion, so every restart repeats the check (and stops again)
/// until it succeeds, and then the rows stream.
#[tokio::test]
#[ignore = "requires docker"]
async fn a_failed_drift_registration_stops_before_the_rows_and_is_retried() {
    init_test_tracing();
    let a = port(&GTID_A, true, 11).await;
    let db = "ident_reload";
    prepare(a, db, "a").await;
    let proxy = Proxy::start(a).await;
    let (st, fault) = State::faulty().await;
    let id = "ident_reload";
    let r = streaming_on_a(&proxy, a, id, db, &st, OnSchemaDrift::Adapt).await;
    // A promoted replica whose binlog starts at the failover position: the
    // table's first event under it is its rows, so the drift check proves
    // and registers its shape.
    let (_t, t) = start(true, 34).await;
    sql(
        t,
        &[
            format!("CREATE DATABASE {db}"),
            format!("CREATE TABLE {db}.t (id INT PRIMARY KEY, src VARCHAR(8), b_only INT)"),
            "RESET BINARY LOGS AND GTIDS".to_string(),
            format!("SET GLOBAL gtid_purged = '{}'", executed(a).await),
        ],
    )
    .await;
    let ut = uuid(t).await;
    let fail_registry = || {
        let mut f = fault.fail_writes_to.lock().unwrap();
        f.push("schemas.v1".into());
        f.push("schemas.v1.index".into());
    };
    let drift_markers = |st: &State| {
        let key = SchemaKey::new(TENANT, id, lineage_hash(&ut), db, "t")
            .backend_key();
        let backend = st.backend.clone();
        async move {
            backend
                .log_list("schemas.v1.failover.drift", &key)
                .await
                .unwrap()
                .len()
        }
    };

    // Online failover, then the table's first rows: the registration fails.
    fail_registry();
    proxy.all_to(t);
    proxy.sever();
    insert(t, db, 9500, "t").await;
    let mut r = r;
    assert!(
        stopped(r.handle).await.is_err(),
        "the failed registration stops"
    );
    let rows = rows_until(&mut r.rx, None, Duration::from_millis(200)).await;
    assert!(rows.iter().all(|(_, s)| s != "t"), "{rows:?}");
    assert_eq!(drift_markers(&st).await, 0, "no completion recorded");

    // Restart, registration still failing: the check is repeated and stops.
    let mut r = run(
        source(id, proxy.dsn(db), db, &st, OnSchemaDrift::Adapt),
        &st,
    )
    .await;
    insert(t, db, 9501, "t").await;
    assert!(stopped(r.handle).await.is_err(), "the repeated check stops");
    let rows = rows_until(&mut r.rx, None, Duration::from_millis(200)).await;
    assert!(rows.iter().all(|(_, s)| s != "t"), "{rows:?}");
    assert_eq!(drift_markers(&st).await, 0, "no completion recorded");

    // Restart, registration succeeds: recorded once, then the rows stream.
    fault.fail_writes_to.lock().unwrap().clear();
    let mut r = run(
        source(id, proxy.dsn(db), db, &st, OnSchemaDrift::Adapt),
        &st,
    )
    .await;
    insert(t, db, 9502, "t").await;
    let got = rows_until(&mut r.rx, Some(9502), Duration::from_secs(60)).await;
    assert!(
        got.contains(&(9502, "t".into())),
        "streams after the check completes"
    );
    assert_eq!(drift_markers(&st).await, 1);
    assert_eq!(st.identity(id).await, Some(ut));
    stop(r.handle).await;
    proxy.all_to(a);
}

// ----------------------------------------------------------------------------
// Credential rotation: preflight, recheck, replacement and recovery.
// ----------------------------------------------------------------------------

static UNIQ: AtomicU64 = AtomicU64::new(0);

/// A Kubernetes-projected-Secret-like credential directory.
struct Projected {
    root: PathBuf,
    version: u64,
}

impl Projected {
    fn new(user: &str, pw: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "df-ident-{}-{}",
            std::process::id(),
            UNIQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        let mut p = Self { root, version: 0 };
        p.publish(user, pw);
        p
    }

    fn publish(&mut self, user: &str, pw: &str) {
        use std::os::unix::fs::symlink;
        self.version += 1;
        let data = self.root.join(format!("..{:04}", self.version));
        std::fs::create_dir_all(&data).unwrap();
        std::fs::write(data.join("username"), user).unwrap();
        std::fs::write(data.join("password"), pw).unwrap();
        let link = self.root.join("..data");
        let _ = std::fs::remove_file(&link);
        symlink(&data, &link).unwrap();
        for name in ["username", "password"] {
            let key = self.root.join(name);
            let _ = std::fs::remove_file(&key);
            symlink(self.root.join("..data").join(name), &key).unwrap();
        }
    }

    fn key(&self, name: &str) -> SecretReference {
        SecretReference::new(
            SecretProvider::File,
            self.root.join(name).to_str().unwrap(),
        )
    }
}

impl Drop for Projected {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

/// A replication user with the same password on every given server.
async fn replication_user(ports: &[u16], user: &str) {
    for &p in ports {
        sql(
            p,
            &[
                format!("DROP USER IF EXISTS '{user}'@'%'"),
                format!("CREATE USER '{user}'@'%' IDENTIFIED BY 'pw_{user}'"),
                format!(
                    "GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* \
                     TO '{user}'@'%'"
                ),
            ],
        )
        .await;
    }
}

/// Users of the binlog dump threads on `port`.
async fn dump_users(port: u16) -> Vec<String> {
    let mut c = mysql_async::Conn::from_url(root_dsn(port, ""))
        .await
        .unwrap();
    let users: Vec<String> = c
        .query(
            "SELECT USER FROM information_schema.PROCESSLIST \
             WHERE COMMAND LIKE 'Binlog Dump%'",
        )
        .await
        .unwrap();
    c.disconnect().await.ok();
    users
}

async fn wait_dump_user(port: u16, user: &str, dur: Duration) -> bool {
    let deadline = Instant::now() + dur;
    while Instant::now() < deadline {
        if dump_users(port).await == [user.to_string()] {
            return true;
        }
        sleep(Duration::from_millis(200)).await;
    }
    false
}

#[tokio::test]
#[ignore = "requires docker"]
async fn rotation_connections_prove_the_server() {
    init_test_tracing();
    let (a, b) = pair(true).await;
    let (ua, ub) = (uuid(a).await, uuid(b).await);
    let db = "ident_rotation";
    prepare(a, db, "a").await;
    prepare(b, db, "b").await;
    let users = ["rot_init", "rot_0", "rot_1", "rot_2", "rot_ok"];
    for u in users {
        replication_user(&[a, b], u).await;
    }
    let proxy = Proxy::start(a).await;
    let mut proj = Projected::new("rot_init", "pw_rot_init");
    let cfg = MysqlSrcCfg {
        id: "ident-rot".into(),
        dsn: Some(format!("mysql://127.0.0.1:{}/{db}", proxy.port)),
        dsn_secret: None,
        credentials: Some(SourceCredentialsCfg {
            username: Some(proj.key("username")),
            password: Some(proj.key("password")),
        }),
        tables: vec![format!("{db}.t")],
        table_options: Default::default(),
        outbox: None,
        snapshot: SnapshotCfg {
            mode: SnapshotMode::Never,
            ..Default::default()
        },
        on_schema_drift: OnSchemaDrift::Adapt,
        rotation: Some(CredentialRotationCfg {
            trigger: RotationTriggerCfg::File {
                trusted_root: proj.root.clone(),
            },
            poll_interval_ms: 300,
            debounce_ms: 300,
            max_secret_bytes: 1 << 20,
            apply_timeout_ms: 30_000,
        }),
    };
    let resolver: Arc<dyn SecretResolver> =
        Arc::new(FileResolver::new(FilePolicy {
            max_size: 1 << 20,
            mode: FileMode::ProjectedVolume {
                trusted_root: proj.root.clone(),
            },
            trim_trailing_newline: false,
        }));
    let dsn = resolve_mysql_credentials(&cfg, resolver.as_ref())
        .await
        .unwrap()
        .build_dsn()
        .unwrap();
    let st = State::new().await;
    let mut src = source(&cfg.id, String::new(), db, &st, OnSchemaDrift::Adapt);
    src.dsn = dsn;
    src.rotation = mysql_rotation::build_spec(&cfg, resolver.clone())
        .await
        .unwrap();
    assert!(src.rotation.is_some());
    let mut r = run(src, &st).await;
    assert!(
        wait_dump_user(a, "rot_init", Duration::from_secs(30)).await,
        "streams as rot_init"
    );
    let mut next = 100u32;

    // The rotation opens, in order: Stage-A preflight (control), Stage-B
    // recheck (control), the replacement session, and, if that fails, the
    // recovery session. Each in turn reaches the wrong server.
    for (k, user) in ["rot_0", "rot_1", "rot_2"].into_iter().enumerate() {
        let what = format!("rotation k={k}");
        // Startup (schema preload) and any earlier attempt are done: the
        // next connection is the rotation's own.
        proxy
            .settled_since(proxy.opened(), Duration::from_secs(3))
            .await;
        let watch = DumpWatch::start(b);
        proxy.divert(k as u32, b, a);
        proj.publish(user, &format!("pw_{user}"));
        sleep(Duration::from_secs(8)).await;
        // Not applied: still streaming on A with the previous credentials.
        assert!(
            wait_dump_user(a, "rot_init", Duration::from_secs(20)).await,
            "{what}: rotation applied or stream lost: {:?}",
            dump_users(a).await
        );
        next += 1;
        insert(a, db, next, "a").await;
        insert(b, db, 6000 + k as u32, "b").await;
        let got =
            rows_until(&mut r.rx, Some(next.into()), Duration::from_secs(30))
                .await;
        assert!(got.contains(&(next.into(), "a".into())), "{what}: streams");
        assert!(got.iter().all(|(_, s)| s != "b"), "{what}: {got:?}");
        assert!(
            !watch.saw_dump().await,
            "{what}: binlog dump on the wrong server"
        );
        proxy.all_to(a);
    }

    // Correctly routed, the next rotation applies and streaming continues.
    proj.publish("rot_ok", "pw_rot_ok");
    assert!(
        wait_dump_user(a, "rot_ok", Duration::from_secs(40)).await,
        "rotation applies when routed correctly: {:?}",
        dump_users(a).await
    );
    next += 1;
    insert(a, db, next, "a").await;
    let got =
        rows_until(&mut r.rx, Some(next.into()), Duration::from_secs(30)).await;
    assert!(got.contains(&(next.into(), "a".into())));
    st.assert_nothing_from(&cfg.id, db, &ua, &ub, "rotation")
        .await;
    stop(r.handle).await;
}
