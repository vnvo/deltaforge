//! Row-time schema selection against a live server (design spec 7.6, 7.20):
//! every row is decoded with the version in effect at its position, proven
//! by the activation timeline (baseline, forward proof) or, with FULL
//! TableMap metadata and no positional proof, by a unique signature match.
//! Without either the source fails closed. Each emitted row is stamped with
//! the selected version's hash and that version's own registry sequence.
//!
//! Run with:
//! ```bash
//! cargo test -p sources --test mysql_proof_matrix_e2e -- --include-ignored --test-threads=1
//! ```

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Instant;

use checkpoints::{CheckpointStore, CheckpointStoreExt, MemCheckpointStore};
use common::AllowList;
use ctor::dtor;
use deltaforge_config::{OnSchemaDrift, SnapshotCfg, SnapshotMode};
use deltaforge_core::{
    Event, Op, Source, SourceHandle, SourceItem, SourceResult,
};
use gate_ownership::GateOwned;
use mysql_async::prelude::Queryable;
use schema_registry::SchemaVersion;
use serde_json::Value;
use sources::{MySqlCheckpoint, MySqlSource};
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
const T: &str =
    "CREATE TABLE t (id INT PRIMARY KEY, a VARCHAR(16), b VARCHAR(16))";

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
        "--server-id=43".to_string(),
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

/// The server, with `binlog_row_metadata` set for the rows the test writes.
async fn server(gtid: bool, full: bool) -> u16 {
    let cell = if gtid { &GTID } else { &FILEPOS };
    let port = cell.get_or_init(|| start(gtid)).await.1;
    let metadata = if full { "FULL" } else { "MINIMAL" };
    sql(
        port,
        "",
        &[format!("SET GLOBAL binlog_row_metadata = '{metadata}'")],
    )
    .await;
    port
}

fn dsn(port: u16, db: &str) -> String {
    format!("mysql://root:{ROOT_PW}@127.0.0.1:{port}/{db}")
}

async fn sql(port: u16, db: &str, stmts: &[String]) {
    let mut c = mysql_async::Conn::from_url(dsn(port, db)).await.unwrap();
    for s in stmts {
        c.query_drop(s.as_str())
            .await
            .unwrap_or_else(|e| panic!("{s}: {e}"));
    }
    c.disconnect().await.ok();
}

fn stmts(s: &[&str]) -> Vec<String> {
    s.iter().map(|s| s.to_string()).collect()
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

/// The server's current position, as a committed checkpoint of `hash`.
async fn position(port: u16, hash: &str) -> MySqlCheckpoint {
    let mut c = mysql_async::Conn::from_url(dsn(port, "")).await.unwrap();
    let s: mysql_async::Row = c
        .query_first("SHOW BINARY LOG STATUS")
        .await
        .unwrap()
        .unwrap();
    c.disconnect().await.ok();
    MySqlCheckpoint {
        lineage: Some(hash.to_string()),
        file: s.get("File").unwrap(),
        pos: s.get("Position").unwrap(),
        gtid_set: s
            .get::<String, _>("Executed_Gtid_Set")
            .filter(|g| !g.is_empty()),
    }
}

/// Fresh databases `db` and `{db}_other`, each running `ddl`.
async fn prepare(port: u16, db: &str, ddl: &[&str]) {
    let mut s = Vec::new();
    for d in [db.to_string(), format!("{db}_other")] {
        s.push(format!("DROP DATABASE IF EXISTS {d}"));
        s.push(format!("CREATE DATABASE {d}"));
        for t in ddl {
            s.push(format!("USE {d}"));
            s.push(t.to_string());
        }
    }
    sql(port, "", &s).await;
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

    /// The table's activation records, oldest first.
    async fn activation(
        &self,
        id: &str,
        hash: &str,
        db: &str,
        t: &str,
    ) -> Vec<Value> {
        let key = SchemaKey::new(TENANT, id, hash, db, t).backend_key();
        self.backend
            .log_list(ACTIVATION_NS, &key)
            .await
            .unwrap()
            .into_iter()
            .map(|(_, bytes)| serde_json::from_slice(&bytes).unwrap())
            .collect()
    }

    /// The registered version an event was stamped with: the one whose hash
    /// is its `schema_version` and whose own sequence is its
    /// `schema_sequence`.
    async fn stamped(&self, id: &str, hash: &str, e: &Event) -> SchemaVersion {
        let key =
            SchemaKey::new(TENANT, id, hash, &e.source.db, &e.source.table);
        let latest = self
            .registry
            .get_latest(&key)
            .await
            .unwrap()
            .expect("registered")
            .version;
        for v in 1..=latest {
            let sv = self.registry.get_version(&key, v).await.unwrap().unwrap();
            if e.schema_version.as_deref() == Some(sv.hash.as_str())
                && e.schema_sequence == Some(sv.sequence)
            {
                return sv;
            }
        }
        panic!(
            "no registered version has hash {:?} and sequence {:?}",
            e.schema_version, e.schema_sequence
        );
    }

    /// Asserts each row decoded with exactly the columns of the version it
    /// is stamped with, and returns those versions.
    async fn decoded(
        &self,
        id: &str,
        hash: &str,
        rows: &[Event],
    ) -> Vec<SchemaVersion> {
        let mut out = Vec::new();
        for e in rows {
            let v = self.stamped(id, hash, e).await;
            let columns: BTreeSet<String> = v.schema_json["columns"]
                .as_array()
                .unwrap()
                .iter()
                .map(|c| c["name"].as_str().unwrap().to_string())
                .collect();
            for image in [&e.before, &e.after].into_iter().flatten() {
                let keys: BTreeSet<String> =
                    image.as_object().unwrap().keys().cloned().collect();
                assert_eq!(keys, columns, "row decoded with another shape");
            }
            out.push(v);
        }
        out
    }
}

fn source(
    id: &str,
    port: u16,
    db: &str,
    tables: &[&str],
    st: &State,
) -> MySqlSource {
    MySqlSource {
        id: id.into(),
        dsn: dsn(port, db).as_str().into(),
        tables: tables.iter().map(|t| format!("{db}.{t}")).collect(),
        tenant: TENANT.into(),
        pipeline: "test".into(),
        registry: st.registry.clone(),
        registry_scope: sources::registry_scope::SharedRegistryScope::default(),
        backend: st.backend.clone(),
        outbox_tables: AllowList::default(),
        snapshot_cfg: SnapshotCfg {
            mode: SnapshotMode::Never,
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
    // Let startup finish (barriers, baselines, preload) before writes.
    sleep(Duration::from_secs(3)).await;
    Run { handle, rx }
}

async fn stop(handle: SourceHandle) {
    handle.stop();
    timeout(Duration::from_secs(30), handle.join).await.ok();
}

fn is_row(e: &Event) -> bool {
    matches!(e.op, Op::Create | Op::Update | Op::Delete)
}

/// The next `n` row events (fewer if the source stops or 30s pass).
async fn rows(rx: &mut mpsc::Receiver<SourceItem>, n: usize) -> Vec<Event> {
    let mut out = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    while out.len() < n {
        let Some(left) = deadline.checked_duration_since(Instant::now()) else {
            break;
        };
        match timeout(left, rx.recv()).await {
            Ok(Some(SourceItem::Event(e))) if is_row(&e) => out.push(e),
            Ok(Some(_)) => {}
            _ => break,
        }
    }
    out
}

/// Every row the source emits until it stops, and how it ended.
async fn until_stopped(r: Run) -> (Vec<Event>, SourceResult<()>) {
    let Run { handle, mut rx } = r;
    let mut out = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(60);
    while let Some(left) = deadline.checked_duration_since(Instant::now()) {
        match timeout(left, rx.recv()).await {
            Ok(Some(SourceItem::Event(e))) if is_row(&e) => out.push(e),
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(_) => panic!("the source did not stop"),
        }
    }
    let res = timeout(Duration::from_secs(30), handle.join)
        .await
        .expect("source stops")
        .expect("source task");
    (out, res)
}

/// The fields a replay must reproduce.
fn image(rows: &[Event]) -> Vec<Value> {
    rows.iter()
        .map(|e| {
            serde_json::json!([
                e.source.table,
                format!("{:?}", e.op),
                e.before,
                e.after,
                e.schema_version,
                e.schema_sequence,
            ])
        })
        .collect()
}

fn fails_closed(res: &SourceResult<()>, needle: &str) {
    let msg = format!("{:#}", res.as_ref().expect_err("must fail closed"));
    assert!(
        msg.contains(needle) && msg.contains("fail-closed"),
        "unexpected error: {msg}"
    );
}

/// Runs each phase's statements (default database `session_db`) under a
/// live source, waiting for that phase's rows before the next one, then
/// replays every row from a checkpoint committed before them. Both runs
/// decode identically; returns the rows and their selected versions.
///
/// Pacing matters: a DDL is proven forward only when no later DDL of the
/// table precedes the source's capture, so a phase ends before a next DDL.
#[allow(clippy::too_many_arguments)]
/// Prove `tables` the way a running source does before a scenario that
/// assumes it: one row each, decoded through a lazy baseline (startup
/// establishes nothing).
async fn prime(
    port: u16,
    db: &str,
    tables: &[&str],
    rx: &mut mpsc::Receiver<SourceItem>,
) {
    let s: Vec<String> = tables
        .iter()
        .map(|t| format!("INSERT INTO {db}.{t} (id) VALUES (-1)"))
        .collect();
    sql(port, "", &s).await;
    let got = rows(rx, tables.len()).await;
    assert_eq!(got.len(), tables.len(), "priming rows: {:?}", image(&got));
}

async fn live_then_replay(
    port: u16,
    st: &State,
    id: &str,
    db: &str,
    tables: &[&str],
    session_db: &str,
    phases: &[(Vec<String>, usize)],
) -> (Vec<Event>, Vec<SchemaVersion>) {
    let hash = lineage_hash(port).await;
    let mut r = run(source(id, port, db, tables, st), st).await;
    prime(port, db, tables, &mut r.rx).await;
    let before = position(port, &hash).await;
    let mut live = Vec::new();
    for (steps, n) in phases {
        sql(port, session_db, steps).await;
        let got = rows(&mut r.rx, *n).await;
        assert_eq!(got.len(), *n, "live rows: {:?}", image(&got));
        live.extend(got);
    }
    stop(r.handle).await;
    let n = live.len();

    st.ckpt.put(id, before).await.unwrap();
    let mut r = run(source(id, port, db, tables, st), st).await;
    let replayed = rows(&mut r.rx, n).await;
    stop(r.handle).await;
    assert_eq!(image(&replayed), image(&live), "replay decodes identically");
    let versions = st.decoded(id, &hash, &live).await;
    (live, versions)
}

fn after(e: &Event, col: &str) -> Value {
    e.after.as_ref().unwrap()[col].clone()
}

// ---------------------------------------------------------------------------
// Replay across a DDL (forward proof) and stamping
// ---------------------------------------------------------------------------

/// Two rows (insert, update) under the old shape, the DDL, then three rows
/// (insert, update, delete) under the new one.
async fn replay_across(
    gtid: bool,
    db: &str,
    ddl: &[&str],
    new_columns: &[&str],
) {
    init_test_tracing();
    let port = server(gtid, false).await;
    prepare(port, db, &[T]).await;
    let st = State::new().await;
    let mut steps = stmts(&[
        "INSERT INTO t VALUES (1, 'A1', 'B1')",
        "UPDATE t SET b = 'B1u' WHERE id = 1",
    ]);
    steps.extend(stmts(ddl));
    steps.extend(stmts(&[
        "INSERT INTO t (id, b) VALUES (2, 'B2')",
        "UPDATE t SET b = 'B2u' WHERE id = 2",
        "DELETE FROM t WHERE id = 2",
    ]));
    let (rows, v) =
        live_then_replay(port, &st, db, db, &["t"], db, &[(steps, 5)]).await;

    assert_eq!(after(&rows[0], "a"), "A1");
    assert_eq!(after(&rows[1], "b"), "B1u");
    assert_eq!(after(&rows[2], "b"), "B2");
    assert_eq!(after(&rows[3], "b"), "B2u");
    assert_eq!(rows[4].before.as_ref().unwrap()["b"], "B2u");
    let names = |v: &SchemaVersion| -> Vec<String> {
        v.schema_json["columns"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["name"].as_str().unwrap().to_string())
            .collect()
    };
    for old in &v[..2] {
        assert_eq!(names(old), ["id", "a", "b"]);
        assert_eq!(old.hash, v[0].hash);
    }
    for new in &v[2..] {
        assert_eq!(names(new), new_columns);
        assert_eq!(new.hash, v[2].hash);
    }
    assert_ne!(v[0].hash, v[2].hash);
    assert!(v[0].sequence < v[2].sequence);
}

#[tokio::test]
#[ignore = "requires docker"]
async fn replay_across_drop_column_gtid() {
    replay_across(
        true,
        "pm_dropcol",
        &["ALTER TABLE t DROP COLUMN a"],
        &["id", "b"],
    )
    .await;
}

#[tokio::test]
#[ignore = "requires docker"]
async fn replay_across_drop_column_file_position() {
    replay_across(
        false,
        "pm_dropcol_fp",
        &["ALTER TABLE t DROP COLUMN a"],
        &["id", "b"],
    )
    .await;
}

#[tokio::test]
#[ignore = "requires docker"]
async fn replay_across_drop_and_recreate() {
    replay_across(
        true,
        "pm_recreate",
        &[
            "DROP TABLE t",
            "CREATE TABLE t (id INT PRIMARY KEY, w INT, b VARCHAR(16))",
        ],
        &["id", "w", "b"],
    )
    .await;
}

#[tokio::test]
#[ignore = "requires docker"]
async fn replay_across_mid_table_add_column() {
    replay_across(
        true,
        "pm_midadd",
        &["ALTER TABLE t ADD COLUMN m VARCHAR(16) AFTER id"],
        &["id", "m", "a", "b"],
    )
    .await;
}

/// A -> B -> A: the first and last rows select the same shape (hash), each
/// stamped with the sequence of the version actually selected.
#[tokio::test]
#[ignore = "requires docker"]
async fn a_shape_that_returns_is_selected_again() {
    init_test_tracing();
    let db = "pm_aba";
    let port = server(true, false).await;
    prepare(port, db, &[T]).await;
    let st = State::new().await;
    let phases = [
        (stmts(&["INSERT INTO t VALUES (1, 'A1', 'B1')"]), 1),
        (
            stmts(&[
                "ALTER TABLE t ADD COLUMN x INT",
                "INSERT INTO t VALUES (2, 'A2', 'B2', 7)",
            ]),
            1,
        ),
        (
            stmts(&[
                "ALTER TABLE t DROP COLUMN x",
                "INSERT INTO t VALUES (3, 'A3', 'B3')",
            ]),
            1,
        ),
    ];
    let (rows, v) =
        live_then_replay(port, &st, db, db, &["t"], db, &phases).await;
    assert_eq!(after(&rows[1], "x"), 7);
    assert_eq!(v[0].hash, v[2].hash);
    assert_ne!(v[0].hash, v[1].hash);
}

/// A table-ID change (FLUSH TABLES) around a DDL changes nothing.
#[tokio::test]
#[ignore = "requires docker"]
async fn table_id_changes_do_not_affect_selection() {
    init_test_tracing();
    let db = "pm_flush";
    let port = server(true, false).await;
    prepare(port, db, &[T]).await;
    let st = State::new().await;
    let steps = stmts(&[
        "INSERT INTO t VALUES (1, 'A1', 'B1')",
        "FLUSH TABLES",
        "ALTER TABLE t ADD COLUMN x INT",
        "INSERT INTO t VALUES (2, 'A2', 'B2', 7)",
        "FLUSH TABLES",
        "INSERT INTO t VALUES (3, 'A3', 'B3', 8)",
    ]);
    let (rows, v) =
        live_then_replay(port, &st, db, db, &["t"], db, &[(steps, 3)]).await;
    assert_eq!(after(&rows[2], "x"), 8);
    assert_ne!(v[0].hash, v[1].hash);
    assert_eq!(v[1].hash, v[2].hash);
}

// ---------------------------------------------------------------------------
// Rename swaps, multi-table and qualified DDL
// ---------------------------------------------------------------------------

/// Two tables whose TableMaps are indistinguishable under MINIMAL metadata
/// swap names. With positional proof every row gets its own table's
/// column names; without it the source fails closed instead of decoding by
/// the (identical) signature.
#[tokio::test]
#[ignore = "requires docker"]
async fn a_same_signature_swap_needs_positional_proof() {
    init_test_tracing();
    let db = "pm_swap";
    let port = server(true, false).await;
    let hash = lineage_hash(port).await;
    prepare(
        port,
        db,
        &[
            "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)",
            "CREATE TABLE u (id INT PRIMARY KEY, c INT, d INT)",
        ],
    )
    .await;
    let st = State::new().await;
    let steps = stmts(&[
        "INSERT INTO t VALUES (1, 10, 11)",
        "INSERT INTO u VALUES (1, 20, 21)",
        "RENAME TABLE t TO tmp, u TO t, tmp TO u",
        "INSERT INTO t VALUES (2, 30, 31)",
        "INSERT INTO u VALUES (2, 40, 41)",
    ]);
    let before = position(port, &hash).await;
    let (rows, _) =
        live_then_replay(port, &st, db, db, &["t", "u"], db, &[(steps, 4)])
            .await;
    let cols = |e: &Event| -> BTreeSet<String> {
        e.after
            .as_ref()
            .unwrap()
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect()
    };
    assert_eq!(
        (rows[0].source.table.as_str(), cols(&rows[0])),
        ("t", BTreeSet::from(["id", "a", "b"].map(String::from)))
    );
    assert_eq!(
        (rows[2].source.table.as_str(), after(&rows[2], "c")),
        ("t", 30.into())
    );
    assert_eq!(
        (rows[3].source.table.as_str(), after(&rows[3], "a")),
        ("u", 40.into())
    );

    // A source that never observed the swap: no positional proof for the
    // pre-swap rows, and MINIMAL metadata cannot tell the tables apart.
    let fresh = State::new().await;
    let id = "pm_swap_fresh";
    fresh.ckpt.put(id, before).await.unwrap();
    let (emitted, res) = until_stopped(
        run(source(id, port, db, &["t", "u"], &fresh), &fresh).await,
    )
    .await;
    fails_closed(&res, "no positional proof");
    assert!(emitted.is_empty(), "rows emitted: {:?}", image(&emitted));
}

/// A DDL qualified with the tracked database but issued from another one,
/// and an unqualified DDL of a same-named table in that other database:
/// only the first changes the tracked table's shape.
#[tokio::test]
#[ignore = "requires docker"]
async fn qualified_ddl_from_another_database_is_attributed_exactly() {
    init_test_tracing();
    let db = "pm_qual";
    let port = server(true, false).await;
    prepare(port, db, &[T]).await;
    let st = State::new().await;
    let steps = vec![
        format!("INSERT INTO {db}.t VALUES (1, 'A1', 'B1')"),
        format!("ALTER TABLE {db}.t ADD COLUMN x INT"),
        "ALTER TABLE t ADD COLUMN y INT".to_string(),
        format!("INSERT INTO {db}.t VALUES (2, 'A2', 'B2', 7)"),
    ];
    let other = format!("{db}_other");
    let (rows, v) =
        live_then_replay(port, &st, db, db, &["t"], &other, &[(steps, 2)])
            .await;
    assert_eq!(after(&rows[1], "x"), 7);
    assert_ne!(v[0].hash, v[1].hash);
}

// ---------------------------------------------------------------------------
// DDLs while the source is stopped
// ---------------------------------------------------------------------------

/// Commit a start position for `id` (a start and a stop with no writes).
/// Commit a position for `id` at which `t` is proven (a start, one priming
/// row, a stop).
async fn commit_a_position(port: u16, st: &State, id: &str, db: &str) {
    let mut r = run(source(id, port, db, &["t"], st), st).await;
    prime(port, db, &["t"], &mut r.rx).await;
    stop(r.handle).await;
}

/// One DDL while stopped: the rows before it are proven by the baseline
/// established at the committed start, the rows after it by its forward
/// proof on the restart.
#[tokio::test]
#[ignore = "requires docker"]
async fn a_ddl_while_stopped_is_proven_on_restart() {
    init_test_tracing();
    let db = "pm_stopped";
    let port = server(true, false).await;
    let hash = lineage_hash(port).await;
    prepare(port, db, &[T]).await;
    let st = State::new().await;
    commit_a_position(port, &st, db, db).await;
    sql(
        port,
        db,
        &stmts(&[
            "INSERT INTO t VALUES (1, 'A1', 'B1')",
            "ALTER TABLE t DROP COLUMN a",
            "INSERT INTO t VALUES (2, 'B2')",
        ]),
    )
    .await;
    let mut r = run(source(db, port, db, &["t"], &st), &st).await;
    let got = rows(&mut r.rx, 2).await;
    stop(r.handle).await;
    assert_eq!(got.len(), 2);
    let v = st.decoded(db, &hash, &got).await;
    assert_eq!(after(&got[0], "a"), "A1");
    assert_eq!(after(&got[1], "b"), "B2");
    assert_ne!(v[0].hash, v[1].hash);
}

/// Two DDLs while stopped: the first one's forward proof sees the second
/// in its interval, so the shape between them was never proven nor
/// captured. MINIMAL fails closed there; FULL finds no candidate matching
/// that shape and fails closed too. The row before both DDLs is emitted,
/// nothing after, and the committed checkpoint does not move.
async fn two_ddls_while_stopped(full: bool) {
    init_test_tracing();
    let db = if full { "pm_two_full" } else { "pm_two_min" };
    let port = server(true, full).await;
    prepare(port, db, &[T]).await;
    let st = State::new().await;
    commit_a_position(port, &st, db, db).await;
    let committed = st.ckpt.get_raw(db).await.unwrap().unwrap();
    sql(
        port,
        db,
        &stmts(&[
            "INSERT INTO t VALUES (1, 'A1', 'B1')",
            "ALTER TABLE t ADD COLUMN c1 INT",
            "INSERT INTO t VALUES (2, 'A2', 'B2', 1)",
            "ALTER TABLE t ADD COLUMN c2 INT",
            "INSERT INTO t VALUES (3, 'A3', 'B3', 1, 2)",
        ]),
    )
    .await;
    let (got, res) =
        until_stopped(run(source(db, port, db, &["t"], &st), &st).await).await;
    fails_closed(&res, "");
    assert_eq!(got.len(), 1, "rows: {:?}", image(&got));
    assert_eq!(after(&got[0], "a"), "A1");
    assert_eq!(st.ckpt.get_raw(db).await.unwrap().unwrap(), committed);
}

#[tokio::test]
#[ignore = "requires docker"]
async fn two_ddls_while_stopped_fail_closed_minimal() {
    two_ddls_while_stopped(false).await;
}

#[tokio::test]
#[ignore = "requires docker"]
async fn two_ddls_while_stopped_fail_closed_full() {
    two_ddls_while_stopped(true).await;
}

// ---------------------------------------------------------------------------
// Barriers and the FULL fallback
// ---------------------------------------------------------------------------

/// A versioned-comment DDL: executed by the server, but not attributable,
/// so a lineage barrier (and here it changes the shape).
const OPAQUE_DDL: &str = "/*!50100 ALTER TABLE t ADD COLUMN x INT */";

fn full_observations(records: &[Value]) -> Vec<&Value> {
    records
        .iter()
        .filter(|r| r["kind"] == "observed" && !r["full"].is_null())
        .collect()
}

/// MINIMAL after a barrier: the table's next rows restore proof by a lazy
/// baseline when nothing changes the table between them and the capture;
/// when a DDL of the table follows them before they are read, nothing can
/// prove them and they are refused (not emitted, checkpoint unchanged).
#[tokio::test]
#[ignore = "requires docker"]
async fn a_barrier_is_recovered_by_a_lazy_baseline_or_fails_closed() {
    init_test_tracing();
    let db = "pm_barrier_min";
    let port = server(true, false).await;
    prepare(port, db, &[T]).await;
    let st = State::new().await;
    commit_a_position(port, &st, db, db).await;
    let mut r = run(source(db, port, db, &["t"], &st), &st).await;
    sql(
        port,
        db,
        &stmts(&[
            "INSERT INTO t VALUES (1, 'A1', 'B1')",
            OPAQUE_DDL,
            "INSERT INTO t VALUES (2, 'A2', 'B2', 7)",
        ]),
    )
    .await;
    let got = rows(&mut r.rx, 2).await;
    stop(r.handle).await;
    assert_eq!(got.len(), 2, "rows: {:?}", image(&got));
    assert_eq!(after(&got[1], "x"), 7);
    let committed = st.ckpt.get_raw(db).await.unwrap().unwrap();

    // While stopped: a row, another barrier, a row, then a DDL of the table.
    sql(
        port,
        db,
        &stmts(&[
            "INSERT INTO t VALUES (3, 'A3', 'B3', 8)",
            "/*!50100 ALTER TABLE t ADD COLUMN y INT */",
            "INSERT INTO t VALUES (4, 'A4', 'B4', 8, 9)",
            "ALTER TABLE t ADD COLUMN z INT",
        ]),
    )
    .await;
    let (got, res) =
        until_stopped(run(source(db, port, db, &["t"], &st), &st).await).await;
    fails_closed(&res, "binlog_row_metadata is not FULL");
    assert_eq!(
        got.len(),
        1,
        "only the row before the barrier: {:?}",
        image(&got)
    );
    assert_eq!(after(&got[0], "x"), 8);
    assert_eq!(st.ckpt.get_raw(db).await.unwrap().unwrap(), committed);
}

/// A shape change the binlog does not carry (an unlogged DDL) contradicts
/// the proven version: the rows are refused, not decoded with it.
#[tokio::test]
#[ignore = "requires docker"]
async fn a_proven_version_that_contradicts_the_rows_fails_closed() {
    init_test_tracing();
    let db = "pm_contradict";
    let port = server(true, false).await;
    prepare(port, db, &[T]).await;
    let st = State::new().await;
    commit_a_position(port, &st, db, db).await;
    let committed = st.ckpt.get_raw(db).await.unwrap().unwrap();
    let r = run(source(db, port, db, &["t"], &st), &st).await;
    sql(
        port,
        db,
        &stmts(&[
            "SET SESSION sql_log_bin = 0",
            "ALTER TABLE t ADD COLUMN x INT",
            "SET SESSION sql_log_bin = 1",
            "INSERT INTO t VALUES (1, 'A1', 'B1', 7)",
        ]),
    )
    .await;
    let (got, res) = until_stopped(r).await;
    fails_closed(&res, "contradicts the binlog rows");
    assert!(got.is_empty(), "rows: {:?}", image(&got));
    assert_eq!(st.ckpt.get_raw(db).await.unwrap().unwrap(), committed);
}

/// A versioned-comment DDL that keeps the shape: still unattributable (a
/// lineage barrier).
const KEEPING_BARRIER: &str = "/*!50100 ALTER TABLE t COMMENT 'c' */";

/// While stopped: a row, `barrier`, two rows in separate transactions, then a
/// DDL of the table (so no lazy baseline can prove the rows after the
/// barrier) and a row under the new shape.
fn rows_past_a_barrier(barrier: &str) -> Vec<String> {
    stmts(&[
        "INSERT INTO t VALUES (1, 'A1', 'B1')",
        barrier,
        "INSERT INTO t VALUES (2, 'A2', 'B2')",
        "INSERT INTO t VALUES (3, 'A3', 'B3')",
        "ALTER TABLE t ADD COLUMN x INT",
        "INSERT INTO t VALUES (4, 'A4', 'B4', 9)",
    ])
}

/// FULL: after a barrier, with no lazy baseline possible, the live shape is
/// captured and registered, the unique matching recorded shape is selected
/// and recorded (binding the barrier) before the row is emitted; the next
/// transaction reuses that observation, and a replay reproduces everything.
async fn full_past_a_barrier(db: &str, barrier: &str) {
    init_test_tracing();
    let port = server(true, true).await;
    let hash = lineage_hash(port).await;
    prepare(port, db, &[T]).await;
    let st = State::new().await;
    commit_a_position(port, &st, db, db).await;
    let committed = st.ckpt.get_raw(db).await.unwrap().unwrap();
    sql(port, db, &rows_past_a_barrier(barrier)).await;

    let mut r = run(source(db, port, db, &["t"], &st), &st).await;
    let live = rows(&mut r.rx, 4).await;
    stop(r.handle).await;
    assert_eq!(live.len(), 4, "rows: {:?}", image(&live));
    assert_eq!(after(&live[3], "x"), 9);
    let v = st.decoded(db, &hash, &live).await;
    assert_eq!(v[0].hash, v[1].hash);
    assert_eq!(v[1].hash, v[2].hash);
    assert_ne!(v[2].hash, v[3].hash);
    let records = st.activation(db, &hash, db, "t").await;
    let full = full_observations(&records);
    assert_eq!(full.len(), 1, "one FULL observation: {records:?}");
    assert!(
        full[0]["binds"].is_string(),
        "binds the barrier: {}",
        full[0]
    );
    assert_eq!(full[0]["full"]["schema_hash"], v[1].hash.as_str());

    st.ckpt.put_raw(db, &committed).await.unwrap();
    let mut r = run(source(db, port, db, &["t"], &st), &st).await;
    let replayed = rows(&mut r.rx, 4).await;
    stop(r.handle).await;
    assert_eq!(image(&replayed), image(&live), "replay decodes identically");
    let records = st.activation(db, &hash, db, "t").await;
    assert_eq!(full_observations(&records).len(), 1, "{records:?}");
}

#[tokio::test]
#[ignore = "requires docker"]
async fn full_metadata_selects_past_a_barrier_by_a_unique_match() {
    full_past_a_barrier("pm_barrier_full", KEEPING_BARRIER).await;
}

/// The same past a database barrier.
#[tokio::test]
#[ignore = "requires docker"]
async fn full_metadata_selects_past_a_database_barrier() {
    let db = "pm_dbbarrier_full";
    full_past_a_barrier(
        db,
        &format!("ALTER DATABASE {db} CHARACTER SET utf8mb4"),
    )
    .await;
}

/// A crash after the live shape is registered but before the FULL
/// observation is durable: the row is not emitted and the checkpoint does
/// not move; the restart re-derives the observation, then emits the row.
#[tokio::test]
#[ignore = "requires docker"]
async fn a_full_observation_is_durable_before_its_row() {
    init_test_tracing();
    let db = "pm_full_crash";
    let port = server(true, true).await;
    let hash = lineage_hash(port).await;
    prepare(port, db, &[T]).await;
    let fault = Arc::new(FaultBackend::new());
    let st = State::over(fault.clone()).await;
    commit_a_position(port, &st, db, db).await;
    let committed = st.ckpt.get_raw(db).await.unwrap().unwrap();
    sql(port, db, &rows_past_a_barrier(KEEPING_BARRIER)).await;

    fault
        .fail_writes_to
        .lock()
        .unwrap()
        .push(ACTIVATION_NS.into());
    let (got, res) =
        until_stopped(run(source(db, port, db, &["t"], &st), &st).await).await;
    let msg = format!("{:#}", res.expect_err("the source stops"));
    assert!(msg.contains("persist a FULL schema observation"), "{msg}");
    assert_eq!(got.len(), 1, "rows: {:?}", image(&got));
    assert_eq!(st.ckpt.get_raw(db).await.unwrap().unwrap(), committed);
    assert!(
        full_observations(&st.activation(db, &hash, db, "t").await).is_empty()
    );

    fault.fail_writes_to.lock().unwrap().clear();
    let mut r = run(source(db, port, db, &["t"], &st), &st).await;
    let got = rows(&mut r.rx, 4).await;
    stop(r.handle).await;
    assert_eq!(got.len(), 4);
    st.decoded(db, &hash, &got).await;
    assert_eq!(
        full_observations(&st.activation(db, &hash, db, "t").await).len(),
        1
    );
}

// ---------------------------------------------------------------------------
// Read-time validation
// ---------------------------------------------------------------------------

/// A restart after a DDL does not replay it: its rows are decoded from the
/// stored forward proof, validated on read. A planted record contradicting
/// that evidence makes the timeline invalid, and the source fails closed.
#[tokio::test]
#[ignore = "requires docker"]
async fn stored_evidence_is_validated_on_read() {
    init_test_tracing();
    let db = "pm_validate";
    let port = server(true, false).await;
    let hash = lineage_hash(port).await;
    prepare(port, db, &[T]).await;
    let st = State::new().await;
    let mut r = run(source(db, port, db, &["t"], &st), &st).await;
    prime(port, db, &["t"], &mut r.rx).await;
    sql(
        port,
        db,
        &stmts(&[
            "INSERT INTO t VALUES (1, 'A1', 'B1')",
            "ALTER TABLE t DROP COLUMN a",
        ]),
    )
    .await;
    assert_eq!(rows(&mut r.rx, 1).await.len(), 1);
    // Wait until the DDL is bound by its forward proof.
    let deadline = Instant::now() + Duration::from_secs(30);
    let proof = loop {
        let records = st.activation(db, &hash, db, "t").await;
        if let Some(p) = records
            .iter()
            .find(|r| r["kind"] == "observed" && !r["proof"].is_null())
        {
            break p.clone();
        }
        assert!(Instant::now() < deadline, "no forward proof: {records:?}");
        sleep(Duration::from_millis(200)).await;
    };
    let after_ddl = position(port, &hash).await;
    sql(port, db, &stmts(&["INSERT INTO t VALUES (2, 'B2')"])).await;
    let live = rows(&mut r.rx, 1).await;
    stop(r.handle).await;

    // Resume after the DDL: no DDL replay, the stored proof decides.
    st.ckpt.put(db, after_ddl.clone()).await.unwrap();
    let mut r = run(source(db, port, db, &["t"], &st), &st).await;
    let resumed = rows(&mut r.rx, 1).await;
    stop(r.handle).await;
    assert_eq!(image(&resumed), image(&live));
    st.decoded(db, &hash, &resumed).await;

    // A forged copy of the binding naming the pre-DDL version.
    let mut forged = proof.clone();
    forged["version"] = 1.into();
    let key = SchemaKey::new(TENANT, db, &hash, db, "t").backend_key();
    st.backend
        .log_append_if_absent(
            ACTIVATION_NS,
            &key,
            "forged",
            &serde_json::to_vec(&forged).unwrap(),
        )
        .await
        .unwrap();
    st.ckpt.put(db, after_ddl).await.unwrap();
    let (got, res) =
        until_stopped(run(source(db, port, db, &["t"], &st), &st).await).await;
    let msg = format!("{:#}", res.expect_err("the source stops"));
    assert!(msg.contains("schema activation timeline"), "{msg}");
    assert!(got.is_empty(), "rows: {:?}", image(&got));
}

// ---------------------------------------------------------------------------
// Rows-event identity and compressed transactions
// ---------------------------------------------------------------------------

/// A FULL observation names the rows event that established it: the
/// transaction (GTID, or file and end position) and the event's ordinal
/// among that transaction's rows events, counted before table filtering, in
/// nested order inside a compressed transaction, and restarting at every
/// transaction. Compressed transactions are decoded like any other.
async fn rows_event_identity(gtid: bool) {
    init_test_tracing();
    let db = if gtid { "pm_ordinal" } else { "pm_ordinal_fp" };
    let port = server(gtid, true).await;
    let hash = lineage_hash(port).await;
    prepare(port, db, &[T, "CREATE TABLE o (id INT PRIMARY KEY)"]).await;
    let st = State::new().await;
    commit_a_position(port, &st, db, db).await;
    let compressed = |body: &[&str]| {
        let mut s = stmts(&[
            "SET SESSION binlog_transaction_compression = ON",
            "BEGIN",
        ]);
        s.extend(stmts(body));
        s.push("COMMIT".into());
        s
    };
    // While stopped: a compressed row (proven); a barrier, then a
    // compressed transaction whose second rows event is the tracked one
    // (the first is untracked); another barrier, then a plain row; finally a
    // DDL of the table, so no lazy baseline proves the rows after the
    // barriers and each takes the FULL path.
    let mut steps = compressed(&["INSERT INTO t VALUES (1, 'A1', 'B1')"]);
    steps.push(KEEPING_BARRIER.into());
    steps.extend(compressed(&[
        "INSERT INTO o VALUES (1)",
        "INSERT INTO t VALUES (2, 'A2', 'B2')",
    ]));
    steps.extend(stmts(&[
        "/*!50100 ALTER TABLE t COMMENT 'd' */",
        "INSERT INTO t VALUES (3, 'A3', 'B3')",
        "ALTER TABLE t ADD COLUMN x INT",
    ]));
    sql(port, db, &steps).await;
    let mut r = run(source(db, port, db, &["t"], &st), &st).await;
    let got = rows(&mut r.rx, 3).await;
    stop(r.handle).await;
    let ids: Vec<Value> = got.iter().map(|e| after(e, "id")).collect();
    assert_eq!(ids, [1, 2, 3], "every row, the compressed ones included");

    let records = st.activation(db, &hash, db, "t").await;
    let events: Vec<&Value> = full_observations(&records)
        .iter()
        .map(|r| &r["full"]["row_event"])
        .collect();
    assert_eq!(events.len(), 2, "{records:?}");
    let kind = if gtid { "gtid" } else { "file_pos" };
    for e in &events {
        assert_eq!(e["event"], kind, "{e}");
    }
    assert_eq!(events[0]["ordinal"], 2, "{}", events[0]);
    assert_eq!(events[1]["ordinal"], 1, "{}", events[1]);
}

#[tokio::test]
#[ignore = "requires docker"]
async fn rows_event_identity_gtid() {
    rows_event_identity(true).await;
}

#[tokio::test]
#[ignore = "requires docker"]
async fn rows_event_identity_file_position() {
    rows_event_identity(false).await;
}

/// The FULL fallback writes its observation, and emits the row, only if
/// the observation then decides the row: another unmatched record at the
/// row's position (here an `unknown` record beside the barrier it binds)
/// keeps the rows unproven, and nothing is written.
#[tokio::test]
#[ignore = "requires docker"]
async fn full_fallback_writes_nothing_unless_it_decides_the_row() {
    init_test_tracing();
    let db = "pm_full_admit";
    let port = server(true, true).await;
    let hash = lineage_hash(port).await;
    prepare(port, db, &[T]).await;
    let st = State::new().await;
    commit_a_position(port, &st, db, db).await;
    let committed = st.ckpt.get_raw(db).await.unwrap().unwrap();
    let r = run(source(db, port, db, &["t"], &st), &st).await;
    sql(port, db, &stmts(&[OPAQUE_DDL])).await;

    // The barrier is durable before the source reads on; plant an
    // `unknown` record at its position in the table's stream.
    let barriers = SchemaKey::source_prefix(TENANT, db, &hash);
    let deadline = Instant::now() + Duration::from_secs(30);
    let barrier: Value = loop {
        // The first is the stream-start barrier of the first run.
        let found: Vec<Value> = st
            .backend
            .log_list(BARRIER_NS, &barriers)
            .await
            .unwrap()
            .into_iter()
            .map(|(_, b)| serde_json::from_slice::<Value>(&b).unwrap())
            .collect();
        if found.len() >= 2 {
            break found.last().unwrap().clone();
        }
        assert!(Instant::now() < deadline, "no barrier record: {found:?}");
        sleep(Duration::from_millis(200)).await;
    };
    let unknown = serde_json::json!({
        "format_version": barrier["format_version"],
        "position": barrier["position"],
        "kind": "unknown",
    });
    let table = SchemaKey::new(TENANT, db, &hash, db, "t").backend_key();
    st.backend
        .log_append_if_absent(
            ACTIVATION_NS,
            &table,
            "planted-unknown",
            &serde_json::to_vec(&unknown).unwrap(),
        )
        .await
        .unwrap();

    sql(
        port,
        db,
        &stmts(&["INSERT INTO t VALUES (1, 'A1', 'B1', 7)"]),
    )
    .await;
    let (got, res) = until_stopped(r).await;
    fails_closed(&res, "would not resolve selection");
    assert!(got.is_empty(), "rows: {:?}", image(&got));
    assert!(
        full_observations(&st.activation(db, &hash, db, "t").await).is_empty()
    );
    assert_eq!(st.ckpt.get_raw(db).await.unwrap().unwrap(), committed);
}

/// A lazy baseline is written only if it decides the rows: with a record
/// incomparable with the rows' position in the table's timeline (here
/// planted: another server's GTID set), the capture and scan succeed but the
/// baseline would not prove the rows, so nothing is written and the rows are
/// refused.
#[tokio::test]
#[ignore = "requires docker"]
async fn a_lazy_baseline_is_written_only_if_it_decides_the_rows() {
    init_test_tracing();
    let db = "pm_lazy_admit";
    let port = server(true, false).await;
    let hash = lineage_hash(port).await;
    prepare(port, db, &[T]).await;
    let st = State::new().await;
    let r = run(source(db, port, db, &["t"], &st), &st).await;
    let incomparable = serde_json::json!({
        "format_version": 1,
        "position": { "MysqlGtid": {
            "gtid_set": "4e11fa47-71ca-11e1-9e33-c80aa9429562:1-2"
        } },
        "kind": "ddl",
    });
    let key = SchemaKey::new(TENANT, db, &hash, db, "t").backend_key();
    st.backend
        .log_append_if_absent(
            ACTIVATION_NS,
            &key,
            "planted-incomparable",
            &serde_json::to_vec(&incomparable).unwrap(),
        )
        .await
        .unwrap();
    sql(port, db, &stmts(&["INSERT INTO t VALUES (1, 'A1', 'B1')"])).await;
    let (got, res) = until_stopped(r).await;
    fails_closed(&res, "no positional proof");
    assert!(got.is_empty(), "rows: {:?}", image(&got));
    let records = st.activation(db, &hash, db, "t").await;
    assert!(
        records.iter().all(|r| r["kind"] != "baseline"),
        "no baseline written: {records:?}"
    );
}
