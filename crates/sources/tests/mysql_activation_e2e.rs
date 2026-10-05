//! Activation records in the live MySQL stream (design spec 7.5): `ddl`
//! records and barriers are durable before the DDL event, replay re-derives
//! byte-identical records, and a stream discontinuity (a start that does not
//! continue from the committed resume position) writes a lineage barrier.
//!
//! Run with:
//! ```bash
//! cargo test -p sources --test mysql_activation_e2e -- --include-ignored --test-threads=1
//! ```

use std::sync::Arc;
use std::time::Instant;

use checkpoints::{CheckpointStore, MemCheckpointStore};
use common::AllowList;
use ctor::dtor;
use deltaforge_config::{OnSchemaDrift, SnapshotCfg, SnapshotMode};
use deltaforge_core::{Event, Source, SourceHandle, SourceItem};
use gate_ownership::GateOwned;
use mysql_async::prelude::Queryable;
use serde_json::Value;
use sources::MySqlSource;
use storage::adapters::test_util::FaultBackend;
use storage::adapters::{LineageDescriptor, SchemaKey};
use storage::{ArcStorageBackend, DurableSchemaRegistry, MemoryStorageBackend};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt, core::WaitFor, runners::AsyncRunner,
};
use tokio::sync::{OnceCell, mpsc};
use tokio::time::{Duration, sleep, timeout};

mod test_common;
use test_common::init_test_tracing;

const ROOT_PW: &str = "pw";
const TENANT: &str = "acme";
const ACTIVATION_NS: &str = "schemas.v1.activation";
const BARRIER_NS: &str = "schemas.v1.activation.barrier";

type Server = (ContainerAsync<GenericImage>, u16);
static GTID: OnceCell<Server> = OnceCell::const_new();
static FILEPOS: OnceCell<Server> = OnceCell::const_new();

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
        "--server-id=41".to_string(),
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

async fn lineage_hash(port: u16) -> String {
    let mut c = mysql_async::Conn::from_url(dsn(port, "")).await.unwrap();
    let u: String = c
        .query_first("SELECT @@GLOBAL.server_uuid")
        .await
        .unwrap()
        .unwrap();
    c.disconnect().await.ok();
    LineageDescriptor::mysql(&u).unwrap().lineage_hash()
}

/// The `ddl` records of a stream.
fn ddl(records: Vec<Value>) -> Vec<Value> {
    records.into_iter().filter(|r| r["kind"] == "ddl").collect()
}

struct State {
    backend: ArcStorageBackend,
    ckpt: Arc<dyn CheckpointStore>,
    registry: Arc<DurableSchemaRegistry>,
}

impl State {
    async fn over(backend: ArcStorageBackend) -> Self {
        Self {
            registry: DurableSchemaRegistry::new(backend.clone())
                .await
                .unwrap(),
            ckpt: Arc::new(MemCheckpointStore::new().unwrap()),
            backend,
        }
    }

    async fn new() -> Self {
        Self::over(Arc::new(MemoryStorageBackend::new())).await
    }

    /// The decoded records of one stream, oldest first.
    async fn stream(&self, ns: &str, key: &str) -> Vec<Value> {
        self.backend
            .log_list(ns, key)
            .await
            .unwrap()
            .into_iter()
            .map(|(_, bytes)| serde_json::from_slice(&bytes).unwrap())
            .collect()
    }

    async fn table(
        &self,
        id: &str,
        hash: &str,
        db: &str,
        t: &str,
    ) -> Vec<Value> {
        let key = SchemaKey::new(TENANT, id, hash, db, t).backend_key();
        self.stream(ACTIVATION_NS, &key).await
    }

    async fn lineage_barriers(&self, id: &str, hash: &str) -> Vec<Value> {
        let key = SchemaKey::source_prefix(TENANT, id, hash);
        self.stream(BARRIER_NS, &key).await
    }

    async fn database_barriers(
        &self,
        id: &str,
        hash: &str,
        db: &str,
    ) -> Vec<Value> {
        let key = SchemaKey::new(TENANT, id, hash, db, "").backend_key();
        self.stream(BARRIER_NS, &key).await
    }

    /// Every activation record's raw bytes (for byte-identical replay).
    async fn all_bytes(&self, keys: &[(&str, String)]) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        for (ns, key) in keys {
            for (_, bytes) in self.backend.log_list(ns, key).await.unwrap() {
                out.push(bytes);
            }
        }
        out
    }
}

fn source(
    id: &str,
    port: u16,
    db: &str,
    st: &State,
    mode: SnapshotMode,
) -> MySqlSource {
    MySqlSource {
        id: id.into(),
        dsn: dsn(port, db).as_str().into(),
        tables: vec![format!("{db}.t")],
        tenant: TENANT.into(),
        pipeline: "test".into(),
        registry: st.registry.clone(),
        registry_scope: sources::registry_scope::SharedRegistryScope::default(),
        backend: st.backend.clone(),
        outbox_tables: AllowList::default(),
        snapshot_cfg: SnapshotCfg {
            mode,
            ..Default::default()
        },
        on_schema_drift: OnSchemaDrift::Adapt,
        table_options: Default::default(),
        rotation: None,
    }
}

struct Run {
    handle: SourceHandle,
    rx: mpsc::Receiver<SourceItem>,
}

async fn run(src: MySqlSource, st: &State) -> Run {
    let (tx, rx) = mpsc::channel(1024);
    let handle = src.run(tx, st.ckpt.clone()).await;
    // Let startup finish (barriers, preload) before the test writes.
    sleep(Duration::from_secs(3)).await;
    Run { handle, rx }
}

async fn stop(handle: SourceHandle) {
    handle.stop();
    timeout(Duration::from_secs(30), handle.join).await.ok();
}

/// The next DDL event whose SQL contains `needle`.
async fn ddl_event(rx: &mut mpsc::Receiver<SourceItem>, needle: &str) -> Event {
    let deadline = Instant::now() + Duration::from_secs(30);
    while let Some(left) = deadline.checked_duration_since(Instant::now()) {
        match timeout(left, rx.recv()).await {
            Ok(Some(SourceItem::Event(e))) => {
                if e.ddl
                    .as_ref()
                    .is_some_and(|d| d.to_string().contains(needle))
                {
                    return e;
                }
            }
            Ok(Some(_)) => {}
            _ => break,
        }
    }
    panic!("no DDL event containing {needle:?}");
}

async fn prepare(port: u16, db: &str) {
    sql(
        port,
        "",
        &[
            &format!("DROP DATABASE IF EXISTS {db}"),
            &format!("CREATE DATABASE {db}"),
            &format!("CREATE TABLE {db}.t (id INT PRIMARY KEY, v INT)"),
            &format!("DROP DATABASE IF EXISTS {db}_other"),
            &format!("CREATE DATABASE {db}_other"),
        ],
    )
    .await;
}

async fn records_precede_events_and_replay_identically(gtid: bool) {
    init_test_tracing();
    let port = server(gtid).await;
    let hash = lineage_hash(port).await;
    let db = if gtid { "act_g" } else { "act_f" };
    prepare(port, db).await;
    let st = State::new().await;
    let id = db;

    // A first start (no committed position): one lineage barrier.
    let r = run(source(id, port, db, &st, SnapshotMode::Never), &st).await;
    stop(r.handle).await;
    assert_eq!(st.lineage_barriers(id, &hash).await.len(), 1);
    let before_ddl = st.ckpt.get_raw(id).await.unwrap().unwrap();
    // The committed position is a real one (never an artificial event's 0).
    let cp: Value = serde_json::from_slice(&before_ddl).unwrap();
    assert!(cp["pos"].as_u64().unwrap() > 4, "{cp}");

    // A non-DDL statement while stopped; a restart consumes it and stops at
    // a later committed position. Restarts at a committed position write no
    // barrier, so the stream that records the DDLs below starts elsewhere
    // than the replay at the end (which starts before this statement).
    sql(port, "", &["FLUSH PRIVILEGES"]).await;
    let r = run(source(id, port, db, &st, SnapshotMode::Never), &st).await;
    stop(r.handle).await;
    assert_ne!(st.ckpt.get_raw(id).await.unwrap().unwrap(), before_ddl);

    // An ordinary restart at the committed position: no barrier. Each DDL's
    // records are durable when its event arrives.
    let mut r = run(source(id, port, db, &st, SnapshotMode::Never), &st).await;
    assert_eq!(st.lineage_barriers(id, &hash).await.len(), 1);
    sql(port, db, &["ALTER TABLE t ADD COLUMN w INT"]).await;
    ddl_event(&mut r.rx, "ADD COLUMN w").await;
    // The table's baseline (3b-2) precedes its DDL record.
    let recs = ddl(st.table(id, &hash, db, "t").await);
    assert_eq!(recs.len(), 1, "{recs:?}");
    assert_eq!(recs[0]["kind"], "ddl");
    assert_eq!(recs[0]["format_version"], 1);

    sql(port, "", &[&format!("DROP DATABASE {db}_other")]).await;
    ddl_event(&mut r.rx, &format!("DROP DATABASE {db}_other")).await;
    let other = st
        .database_barriers(id, &hash, &format!("{db}_other"))
        .await;
    assert_eq!(other.len(), 1, "{other:?}");
    assert_eq!(other[0]["scope"]["scope"], "database");

    // A versioned comment cannot be interpreted: a lineage barrier.
    sql(
        port,
        db,
        &["CREATE TABLE u (id INT PRIMARY KEY) /*!50100 ENGINE=InnoDB */"],
    )
    .await;
    ddl_event(&mut r.rx, "CREATE TABLE u").await;
    let lineage = st.lineage_barriers(id, &hash).await;
    assert_eq!(lineage.len(), 2, "{lineage:?}");
    assert_eq!(lineage[1]["scope"]["scope"], "lineage");
    stop(r.handle).await;

    // Replay the same statements from the earlier committed position (before
    // the FLUSH): every record is re-derived byte for byte, none is added.
    let keys = [
        (
            ACTIVATION_NS,
            SchemaKey::new(TENANT, id, hash.as_str(), db, "t").backend_key(),
        ),
        (
            BARRIER_NS,
            SchemaKey::new(
                TENANT,
                id,
                hash.as_str(),
                format!("{db}_other").as_str(),
                "",
            )
            .backend_key(),
        ),
        (BARRIER_NS, SchemaKey::source_prefix(TENANT, id, &hash)),
    ];
    let recorded = st.all_bytes(&keys).await;
    st.ckpt.put_raw(id, &before_ddl).await.unwrap();
    let mut r = run(source(id, port, db, &st, SnapshotMode::Never), &st).await;
    ddl_event(&mut r.rx, "CREATE TABLE u").await;
    stop(r.handle).await;
    assert_eq!(
        st.all_bytes(&keys).await,
        recorded,
        "replay changed records"
    );
}

#[tokio::test]
#[ignore = "requires docker"]
async fn records_precede_events_and_replay_identically_gtid() {
    records_precede_events_and_replay_identically(true).await;
}

#[tokio::test]
#[ignore = "requires docker"]
async fn records_precede_events_and_replay_identically_file_position() {
    records_precede_events_and_replay_identically(false).await;
}

#[tokio::test]
#[ignore = "requires docker"]
async fn a_ddl_whose_record_cannot_be_persisted_is_not_emitted_or_passed() {
    init_test_tracing();
    let port = server(true).await;
    let hash = lineage_hash(port).await;
    let db = "act_fault";
    prepare(port, db).await;
    let fault = Arc::new(FaultBackend::new());
    let st = State::over(fault.clone()).await;
    let id = db;
    let r = run(source(id, port, db, &st, SnapshotMode::Never), &st).await;
    stop(r.handle).await;
    let committed = st.ckpt.get_raw(id).await.unwrap().unwrap();

    let mut r = run(source(id, port, db, &st, SnapshotMode::Never), &st).await;
    fault
        .fail_writes_to
        .lock()
        .unwrap()
        .push(ACTIVATION_NS.into());
    sql(port, db, &["ALTER TABLE t ADD COLUMN z INT"]).await;
    let res = timeout(Duration::from_secs(60), r.handle.join)
        .await
        .expect("source stops")
        .expect("source task");
    assert!(res.is_err(), "an unpersisted record stops the source");
    while let Ok(item) = r.rx.try_recv() {
        if let SourceItem::Event(e) = item {
            assert!(e.ddl.is_none(), "the DDL event was emitted");
        }
    }
    assert_eq!(st.ckpt.get_raw(id).await.unwrap().unwrap(), committed);
    assert!(ddl(st.table(id, &hash, db, "t").await).is_empty());

    // Without the fault the DDL is recorded, then emitted.
    fault.fail_writes_to.lock().unwrap().clear();
    let mut r = run(source(id, port, db, &st, SnapshotMode::Never), &st).await;
    ddl_event(&mut r.rx, "ADD COLUMN z").await;
    assert_eq!(ddl(st.table(id, &hash, db, "t").await).len(), 1);
    stop(r.handle).await;
}

#[tokio::test]
#[ignore = "requires docker"]
async fn a_snapshot_start_is_a_discontinuity() {
    init_test_tracing();
    let port = server(true).await;
    let hash = lineage_hash(port).await;
    let db = "act_snap";
    prepare(port, db).await;
    let st = State::new().await;
    let id = db;
    let r = run(source(id, port, db, &st, SnapshotMode::Never), &st).await;
    stop(r.handle).await;
    assert_eq!(st.lineage_barriers(id, &hash).await.len(), 1);
    sql(port, db, &["INSERT INTO t VALUES (1, 1)"]).await;
    // A snapshot re-anchors the stream: a barrier at the new anchor.
    let r = run(source(id, port, db, &st, SnapshotMode::Always), &st).await;
    stop(r.handle).await;
    let barriers = st.lineage_barriers(id, &hash).await;
    assert_eq!(barriers.len(), 2, "{barriers:?}");
    assert_ne!(barriers[0]["position"], barriers[1]["position"]);
}
