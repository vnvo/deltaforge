//! DDL attribution, end to end on a live MySQL 8.4 (GTID, binlog row
//! metadata MINIMAL): every DDL event names what its statement acts on
//! (never the session database), no DDL reloads or registers anything from
//! the live catalog, and every schema version registered while catching up
//! carries its positional proof.
//!
//! The source DSN names no database, as the fleet harness uses. The server
//! keeps a general query log (`mysql.general_log`) so a test can prove the
//! source never queried the catalog for a table.
//!
//! ```bash
//! cargo test -p sources --test mysql_ddl_attribution_e2e -- --include-ignored --test-threads=1
//! ```

use std::sync::{Arc, Mutex, OnceLock};
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
use storage::adapters::{LineageDescriptor, SchemaKey};
use storage::{ArcStorageBackend, DurableSchemaRegistry, MemoryStorageBackend};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt, core::WaitFor, runners::AsyncRunner,
};
use tokio::sync::{OnceCell, mpsc};
use tokio::time::{Duration, sleep, timeout};

mod test_common;

const ROOT_PW: &str = "pw";
const TENANT: &str = "acme";

type Server = (ContainerAsync<GenericImage>, u16);
static SERVER: OnceCell<Server> = OnceCell::const_new();

#[dtor]
fn cleanup() {
    if let Some((c, _)) = SERVER.get() {
        std::process::Command::new("docker")
            .args(["rm", "-f", "-v", c.id()])
            .output()
            .ok();
    }
}

// ---------------------------------------------------------------------------
// Log capture: every `sources::*` event at INFO or above, as one line each.
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<String>>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Capture {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let md = event.metadata();
        if !md.target().starts_with("sources")
            || *md.level() > tracing::Level::INFO
        {
            return;
        }
        struct V(String);
        impl tracing::field::Visit for V {
            fn record_debug(
                &mut self,
                f: &tracing::field::Field,
                v: &dyn std::fmt::Debug,
            ) {
                self.0.push_str(&format!(" {}={:?}", f.name(), v));
            }
        }
        let mut v = V(format!("{} {}:", md.level(), md.target()));
        event.record(&mut v);
        self.0.lock().unwrap().push(v.0);
    }
}

fn capture() -> &'static Capture {
    static CAP: OnceLock<Capture> = OnceLock::new();
    CAP.get_or_init(|| {
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;
        use tracing_subscriber::{EnvFilter, fmt};
        let cap = Capture::default();
        let filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("info,sources=info"));
        let _ = tracing_subscriber::registry()
            .with(cap.clone())
            .with(
                fmt::layer()
                    .with_test_writer()
                    .compact()
                    .with_filter(filter),
            )
            .try_init();
        cap
    })
}

use tracing_subscriber::Layer as _;

/// Captured lines for `source_id` that contain `needle`.
fn logs(source_id: &str, needle: &str) -> Vec<String> {
    capture()
        .0
        .lock()
        .unwrap()
        .iter()
        .filter(|l| {
            l.contains(needle) && l.contains(&format!("source_id={source_id}"))
        })
        .cloned()
        .collect()
}

// ---------------------------------------------------------------------------
// Server and source
// ---------------------------------------------------------------------------

async fn start() -> Server {
    let cmd = vec![
        "--server-id=47".to_string(),
        "--log-bin=mysql-bin".into(),
        "--binlog-format=ROW".into(),
        "--binlog-row-image=FULL".into(),
        "--binlog-checksum=NONE".into(),
        "--gtid-mode=ON".into(),
        "--enforce-gtid-consistency=ON".into(),
        // Every statement any connection sends, for the "no catalog access"
        // checks.
        "--general-log=ON".into(),
        "--log-output=TABLE".into(),
    ];
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
    while mysql_async::Conn::from_url(dsn(port)).await.is_err() {
        assert!(Instant::now() < deadline, "mysql on {port} not ready");
        sleep(Duration::from_millis(500)).await;
    }
    (c, port)
}

async fn server() -> u16 {
    SERVER.get_or_init(start).await.1
}

/// A DSN with NO default database (as the fleet harness uses).
fn dsn(port: u16) -> String {
    format!("mysql://root:{ROOT_PW}@127.0.0.1:{port}/")
}

/// Run `stmts` in order on ONE session opened with no default database (a
/// `USE` among them changes the active database for the rest).
async fn sql(port: u16, stmts: &[String]) {
    let mut c = mysql_async::Conn::from_url(dsn(port)).await.unwrap();
    for s in stmts {
        c.query_drop(s.as_str())
            .await
            .unwrap_or_else(|e| panic!("{s}: {e}"));
    }
    c.disconnect().await.ok();
}

async fn lineage_hash(port: u16) -> String {
    let mut c = mysql_async::Conn::from_url(dsn(port)).await.unwrap();
    let u: String = c
        .query_first("SELECT @@GLOBAL.server_uuid")
        .await
        .unwrap()
        .unwrap();
    c.disconnect().await.ok();
    LineageDescriptor::mysql(&u).unwrap().lineage_hash()
}

struct State {
    backend: ArcStorageBackend,
    ckpt: Arc<dyn CheckpointStore>,
    registry: Arc<DurableSchemaRegistry>,
}

impl State {
    async fn new() -> Self {
        let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
        Self {
            registry: DurableSchemaRegistry::new(backend.clone())
                .await
                .unwrap(),
            ckpt: Arc::new(MemCheckpointStore::new().unwrap()),
            backend,
        }
    }
}

fn source(id: &str, port: u16, tables: &[String], st: &State) -> MySqlSource {
    MySqlSource {
        id: id.into(),
        dsn: dsn(port).as_str().into(),
        tables: tables.to_vec(),
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
        snapshot_cohort: Default::default(),
        type_mapping: deltaforge_core::TypeMapping::V2,
    }
}

struct Run {
    handle: SourceHandle,
    rx: mpsc::Receiver<SourceItem>,
    /// DDL events seen while waiting for rows.
    ddls: Vec<Event>,
}

async fn run(src: MySqlSource, st: &State) -> Run {
    let (tx, rx) = test_common::acked_channel(&src, &st.ckpt, &src.id, 1024);
    let handle = src.run(tx, st.ckpt.clone()).await;
    // Let startup finish (barriers) before the test writes.
    sleep(Duration::from_secs(3)).await;
    Run {
        handle,
        rx,
        ddls: Vec::new(),
    }
}

/// The outcome of waiting for one row event.
enum Next {
    Row(Box<Event>),
    /// The channel closed: the source stopped (failed closed).
    Closed,
    TimedOut,
}

/// The next row event of `db.t` whose `after.id == id` (DDL events on the
/// way are kept in `r.ddls`).
async fn row(r: &mut Run, db: &str, id: i64) -> Next {
    row_in(r, db, "t", id).await
}

/// The next row event of `db.table` whose `after.id == id`.
async fn row_in(r: &mut Run, db: &str, table: &str, id: i64) -> Next {
    let deadline = Instant::now() + Duration::from_secs(60);
    while let Some(left) = deadline.checked_duration_since(Instant::now()) {
        match timeout(left, r.rx.recv()).await {
            Ok(Some(SourceItem::Event(e))) if e.ddl.is_some() => r.ddls.push(e),
            Ok(Some(SourceItem::Event(e))) => {
                if e.ddl.is_none()
                    && e.source.db == db
                    && e.source.table == table
                    && e.after
                        .as_ref()
                        .map(test_common::row_view)
                        .and_then(|a| a.get("id").cloned())
                        .and_then(|v| v.as_i64())
                        == Some(id)
                {
                    return Next::Row(Box::new(e));
                }
            }
            Ok(Some(_)) => {}
            Ok(None) => return Next::Closed,
            Err(_) => return Next::TimedOut,
        }
    }
    Next::TimedOut
}

async fn expect_row(r: &mut Run, db: &str, id: i64, what: &str) -> Event {
    match row(r, db, id).await {
        Next::Row(e) => *e,
        Next::Closed => {
            let res =
                timeout(Duration::from_secs(10), &mut r.handle.join).await;
            panic!(
                "{what}: source stopped before emitting row id={id} \
                 (fail-closed): {res:?}"
            )
        }
        Next::TimedOut => panic!("{what}: no row id={id} within 60s"),
    }
}

async fn stop(handle: SourceHandle) {
    handle.stop();
    timeout(Duration::from_secs(30), handle.join).await.ok();
}

/// Sorted column names of an emitted row image.
fn cols(e: &Event) -> Vec<String> {
    let mut v: Vec<String> = e
        .after
        .as_ref()
        .map(|r| r.names().map(String::from).collect())
        .unwrap_or_default();
    v.sort();
    v
}

// ---------------------------------------------------------------------------
// DDL events, the catalog log and registry versions
// ---------------------------------------------------------------------------

/// The (`source.db`, payload) of the DDL event whose SQL (or redacted
/// operation) contains `needle`, waiting up to 60 s; row events on the way
/// are dropped.
async fn ddl(r: &mut Run, needle: &str) -> (String, Value) {
    let matches = |e: &Event| {
        let p = e.ddl.as_ref().unwrap();
        p["sql"].as_str().is_some_and(|s| s.contains(needle))
            || p["operation"].as_str().is_some_and(|s| s.contains(needle))
    };
    if let Some(e) = r.ddls.iter().find(|e| matches(e)) {
        return (e.source.db.clone(), e.ddl.clone().unwrap());
    }
    let deadline = Instant::now() + Duration::from_secs(60);
    while let Some(left) = deadline.checked_duration_since(Instant::now()) {
        match timeout(left, r.rx.recv()).await {
            Ok(Some(SourceItem::Event(e))) if e.ddl.is_some() => {
                assert_eq!(e.source.table, "_ddl");
                if matches(&e) {
                    return (e.source.db.clone(), e.ddl.clone().unwrap());
                }
                r.ddls.push(e);
            }
            Ok(Some(_)) => {}
            Ok(None) => panic!("the source stopped before the DDL {needle:?}"),
            Err(_) => break,
        }
    }
    panic!("no DDL event for {needle:?} within 60s: {:?}", r.ddls)
}

fn obj(kind: &str, db: &str, name: &str) -> Value {
    serde_json::json!({"kind": kind, "database": db, "name": name})
}

/// The server's clock, to bound general-log searches.
async fn now(port: u16) -> String {
    let mut c = mysql_async::Conn::from_url(dsn(port)).await.unwrap();
    let t: String = c
        .query_first("SELECT CAST(NOW(6) AS CHAR)")
        .await
        .unwrap()
        .unwrap();
    c.disconnect().await.ok();
    t
}

/// Catalog (`information_schema`) statements any connection sent since
/// `since` that mention `needle`.
async fn catalog_queries(port: u16, since: &str, needle: &str) -> Vec<String> {
    let mut c = mysql_async::Conn::from_url(dsn(port)).await.unwrap();
    let rows: Vec<String> = c
        .exec_map(
            "SELECT CAST(argument AS CHAR) FROM mysql.general_log \
             WHERE event_time >= ? AND command_type IN ('Query', 'Execute') \
             AND LOWER(CAST(argument AS CHAR)) LIKE '%information_schema%' \
             AND CAST(argument AS CHAR) LIKE ? \
             AND CAST(argument AS CHAR) NOT LIKE '%general_log%'",
            (since, format!("%{needle}%")),
            |a: String| a,
        )
        .await
        .unwrap();
    c.disconnect().await.ok();
    rows
}

/// Every registered version of `db.table` for source `id`, in order:
/// (version, columns, binding checkpoint).
async fn versions(
    st: &State,
    id: &str,
    hash: &str,
    db: &str,
    table: &str,
) -> Vec<(i32, Vec<String>, Option<Vec<u8>>)> {
    let key = SchemaKey::new(TENANT, id, hash, db, table);
    let mut out = Vec::new();
    for v in 1.. {
        let Some(sv) = st.registry.get_version(&key, v).await.unwrap() else {
            break;
        };
        let cols = sv.schema_json["columns"]
            .as_array()
            .map(|a| {
                a.iter()
                    .map(|c| c["name"].as_str().unwrap_or_default().to_string())
                    .collect()
            })
            .unwrap_or_default();
        out.push((sv.version, cols, sv.checkpoint.clone()));
    }
    out
}

/// The highest transaction number of the server's own UUID in `gtid_set`.
fn own_gno(gtid_set: &str, uuid: &str) -> u64 {
    gtid_set
        .replace('\n', "")
        .split(',')
        .filter_map(|e| {
            let mut parts = e.trim().split(':');
            (parts.next()?.eq_ignore_ascii_case(uuid)).then(|| {
                parts
                    .filter_map(|iv| iv.rsplit('-').next()?.parse::<u64>().ok())
                    .max()
                    .unwrap_or(0)
            })
        })
        .max()
        .unwrap_or(0)
}

async fn executed(port: u16) -> (String, String) {
    let mut c = mysql_async::Conn::from_url(dsn(port)).await.unwrap();
    let (u, g): (String, String) = c
        .query_first("SELECT @@GLOBAL.server_uuid, @@GLOBAL.gtid_executed")
        .await
        .unwrap()
        .unwrap();
    c.disconnect().await.ok();
    (u, g)
}

/// A fresh `db` with `db.t (id, v INT)`, a source tracking `tables`, and
/// row id=1 streamed (so `db.t`'s shape is resolved before any DDL).
async fn setup(
    id: &str,
    db: &str,
    tables: &[String],
    extra: &[String],
) -> (u16, State, Run) {
    capture();
    let port = server().await;
    let mut stmts = vec![
        format!("DROP DATABASE IF EXISTS {db}"),
        format!("CREATE DATABASE {db}"),
        format!("CREATE TABLE {db}.t (id INT PRIMARY KEY, v INT)"),
    ];
    stmts.extend(extra.iter().cloned());
    sql(port, &stmts).await;
    let st = State::new().await;
    let mut r = run(source(id, port, tables, &st), &st).await;
    sql(port, &[format!("INSERT INTO {db}.t VALUES (1, 10)")]).await;
    expect_row(&mut r, db, 1, "pre-DDL row").await;
    (port, st, r)
}

fn no_legacy_reload(id: &str) {
    for needle in ["reloading schema after DDL", "failed to reload schema"] {
        let found = logs(id, needle);
        assert!(found.is_empty(), "legacy reload ran: {found:?}");
    }
}

// ---------------------------------------------------------------------------
// Cases
// ---------------------------------------------------------------------------

/// Qualified DDL with no active database: the event names the table's
/// database; the rows after it have the new shape; nothing is reloaded.
#[tokio::test]
#[ignore = "requires docker"]
async fn qualified_ddl_without_an_active_database() {
    let (id, db) = ("da_a", "da_a");
    let (port, _st, mut r) = setup(id, db, &[format!("{db}.t")], &[]).await;
    sql(port, &[format!("ALTER TABLE {db}.t RENAME COLUMN v TO w")]).await;
    sql(
        port,
        &[format!("INSERT INTO {db}.t (id, w) VALUES (2, 20)")],
    )
    .await;
    let post = expect_row(&mut r, db, 2, "post-DDL").await;
    assert_eq!(cols(&post), ["id", "w"]);
    let (source_db, p) = ddl(&mut r, "RENAME COLUMN").await;
    assert_eq!(source_db, db);
    assert_eq!(p["ddl_format"], 2);
    assert_eq!(p["scope"], "tables");
    assert_eq!(p["objects"], serde_json::json!([obj("table", db, "t")]));
    assert_eq!(p["database"], db);
    assert_eq!(p["session_database"], Value::Null);
    stop(r.handle).await;
    no_legacy_reload(id);
}

/// Qualified DDL while another database is active: the event names the
/// table's database (the session one only as `session_database`), and the
/// active database's unrelated `t` is never queried nor registered.
#[tokio::test]
#[ignore = "requires docker"]
async fn qualified_ddl_while_another_database_is_active() {
    let (id, db, other) = ("da_e", "da_e", "da_e_other");
    let (port, st, mut r) = setup(
        id,
        db,
        &[format!("{db}.t")],
        &[
            format!("DROP DATABASE IF EXISTS {other}"),
            format!("CREATE DATABASE {other}"),
            format!("CREATE TABLE {other}.t (id INT PRIMARY KEY, v INT)"),
        ],
    )
    .await;
    let since = now(port).await;
    sql(
        port,
        &[
            format!("USE {other}"),
            format!("ALTER TABLE {db}.t RENAME COLUMN v TO w"),
        ],
    )
    .await;
    sql(
        port,
        &[format!("INSERT INTO {db}.t (id, w) VALUES (2, 20)")],
    )
    .await;
    let post = expect_row(&mut r, db, 2, "post-DDL").await;
    assert_eq!(cols(&post), ["id", "w"]);
    let (source_db, p) = ddl(&mut r, "RENAME COLUMN").await;
    assert_eq!(source_db, db);
    assert_eq!(p["database"], db);
    assert_eq!(p["objects"], serde_json::json!([obj("table", db, "t")]));
    assert_eq!(p["session_database"], other);
    stop(r.handle).await;
    no_legacy_reload(id);
    let touched = catalog_queries(port, &since, other).await;
    assert!(
        touched.is_empty(),
        "the unrelated table was queried: {touched:?}"
    );
    let hash = lineage_hash(port).await;
    assert!(
        versions(&st, id, &hash, other, "t").await.is_empty(),
        "the unrelated table was registered"
    );
}

/// Unqualified DDL with an active database is attributed to it.
#[tokio::test]
#[ignore = "requires docker"]
async fn unqualified_ddl_with_an_active_database() {
    let (id, db) = ("da_u", "da_u");
    let (port, _st, mut r) = setup(id, db, &[format!("{db}.t")], &[]).await;
    sql(
        port,
        &[format!("USE {db}"), "ALTER TABLE t ADD COLUMN x INT".into()],
    )
    .await;
    sql(port, &[format!("INSERT INTO {db}.t VALUES (2, 20, 30)")]).await;
    let post = expect_row(&mut r, db, 2, "post-DDL").await;
    assert_eq!(cols(&post), ["id", "v", "x"]);
    let (source_db, p) = ddl(&mut r, "ADD COLUMN x").await;
    assert_eq!(source_db, db);
    assert_eq!(p["objects"], serde_json::json!([obj("table", db, "t")]));
    assert_eq!(p["session_database"], db);
    stop(r.handle).await;
    no_legacy_reload(id);
}

/// CREATE VIEW is attributed to its schema (scope none); CREATE USER is an
/// unattributed, redacted event; a plain GRANT emits no DDL event and the
/// stream continues.
#[tokio::test]
#[ignore = "requires docker"]
async fn create_view_and_grant() {
    let (id, db) = ("da_v", "da_v");
    let (port, _st, mut r) = setup(id, db, &[format!("{db}.t")], &[]).await;
    sql(
        port,
        &[
            "DROP USER IF EXISTS 'da_v_user'@'%'".into(),
            format!("CREATE VIEW {db}.v AS SELECT id FROM {db}.t"),
            "CREATE USER 'da_v_user'@'%' IDENTIFIED BY 'not-in-the-event'"
                .into(),
            format!("GRANT SELECT ON {db}.* TO 'da_v_user'@'%'"),
            format!("INSERT INTO {db}.t VALUES (2, 20)"),
        ],
    )
    .await;
    expect_row(&mut r, db, 2, "after GRANT").await;
    // MySQL rewrites the statement in the binlog
    // (`CREATE ALGORITHM=.. DEFINER=.. SQL SECURITY .. VIEW ...`).
    let (source_db, p) = ddl(&mut r, "VIEW").await;
    assert_eq!(source_db, db);
    assert_eq!(p["scope"], "none");
    assert_eq!(p["objects"], serde_json::json!([obj("view", db, "v")]));
    let (source_db, p) = ddl(&mut r, "CREATE USER").await;
    assert_eq!(source_db, "");
    assert_eq!(p["scope"], "none");
    assert_eq!(p["objects"], serde_json::json!([]));
    assert_eq!(p["database"], Value::Null);
    assert!(!p.to_string().contains("not-in-the-event"));
    assert!(
        r.ddls.iter().all(|e| !e
            .ddl
            .as_ref()
            .unwrap()
            .to_string()
            .contains("GRANT")),
        "a plain GRANT emits no DDL event: {:?}",
        r.ddls
    );
    stop(r.handle).await;
}

/// Renames within and across databases list source then target; the
/// cross-database one names no single database.
#[tokio::test]
#[ignore = "requires docker"]
async fn same_and_cross_database_renames() {
    let (id, db, other) = ("da_r", "da_r", "da_r_other");
    let (port, _st, mut r) = setup(
        id,
        db,
        &[format!("{db}.*"), format!("{other}.*")],
        &[
            format!("DROP DATABASE IF EXISTS {other}"),
            format!("CREATE DATABASE {other}"),
        ],
    )
    .await;
    sql(
        port,
        &[
            format!("RENAME TABLE {db}.t TO {db}.t2"),
            format!("RENAME TABLE {db}.t2 TO {other}.t3"),
        ],
    )
    .await;
    let (source_db, p) = ddl(&mut r, "TO da_r.t2").await;
    assert_eq!(source_db, db);
    assert_eq!(p["database"], db);
    assert_eq!(
        p["objects"],
        serde_json::json!([obj("table", db, "t"), obj("table", db, "t2")])
    );
    let (source_db, p) = ddl(&mut r, "TO da_r_other.t3").await;
    assert_eq!(source_db, "");
    assert_eq!(p["database"], Value::Null);
    assert_eq!(
        p["objects"],
        serde_json::json!([obj("table", db, "t2"), obj("table", other, "t3")])
    );
    stop(r.handle).await;
    no_legacy_reload(id);
}

/// A multi-table DROP names every table in statement order, and dropped
/// tables are forgotten without any catalog query.
#[tokio::test]
#[ignore = "requires docker"]
async fn multi_table_drop_without_catalog_reload() {
    let (id, db) = ("da_d", "da_d");
    let (port, _st, mut r) = setup(
        id,
        db,
        &[format!("{db}.*")],
        &[
            format!("CREATE TABLE {db}.dx (id INT PRIMARY KEY)"),
            format!("CREATE TABLE {db}.dy (id INT PRIMARY KEY)"),
        ],
    )
    .await;
    sql(
        port,
        &[
            format!("INSERT INTO {db}.dx VALUES (1)"),
            format!("INSERT INTO {db}.dy VALUES (1)"),
        ],
    )
    .await;
    // Their rows are streamed (and their shapes resolved) before the DROP:
    // MINIMAL rows of a table already dropped cannot be proven.
    for t in ["dx", "dy"] {
        assert!(
            matches!(row_in(&mut r, db, t, 1).await, Next::Row(_)),
            "{t} row"
        );
    }
    let since = now(port).await;
    sql(port, &[format!("DROP TABLE {db}.dy, {db}.dx")]).await;
    let (source_db, p) = ddl(&mut r, "DROP TABLE").await;
    assert_eq!(source_db, db);
    assert_eq!(p["scope"], "tables");
    assert_eq!(
        p["objects"],
        serde_json::json!([obj("table", db, "dy"), obj("table", db, "dx")])
    );
    sql(port, &[format!("INSERT INTO {db}.t VALUES (2, 20)")]).await;
    expect_row(&mut r, db, 2, "after DROP").await;
    stop(r.handle).await;
    no_legacy_reload(id);
    for t in ["dx", "dy"] {
        let q = catalog_queries(port, &since, t).await;
        assert!(q.is_empty(), "a dropped table was reloaded: {q:?}");
    }
}

/// A database barrier names its database; an unattributable statement (an
/// ANSI_QUOTES identifier) is a lineage barrier naming nothing.
#[tokio::test]
#[ignore = "requires docker"]
async fn database_and_lineage_barriers() {
    let (id, db, gone) = ("da_b", "da_b", "da_b_gone");
    let (port, _st, mut r) = setup(
        id,
        db,
        &[format!("{db}.*")],
        &[
            format!("DROP DATABASE IF EXISTS {gone}"),
            format!("CREATE DATABASE {gone}"),
        ],
    )
    .await;
    sql(
        port,
        &[
            format!("DROP DATABASE {gone}"),
            "SET SESSION sql_mode = CONCAT(@@sql_mode, ',ANSI_QUOTES')".into(),
            format!("CREATE TABLE \"{db}\".\"q\" (id INT PRIMARY KEY)"),
        ],
    )
    .await;
    let (source_db, p) = ddl(&mut r, "DROP DATABASE").await;
    assert_eq!(source_db, gone);
    assert_eq!(p["scope"], "database");
    assert_eq!(
        p["objects"],
        serde_json::json!([obj("database", gone, gone)])
    );
    let (source_db, p) = ddl(&mut r, "\"q\"").await;
    assert_eq!(source_db, "");
    assert_eq!(p["scope"], "lineage");
    assert_eq!(p["objects"], serde_json::json!([]));
    assert_eq!(p["database"], Value::Null);
    stop(r.handle).await;
    no_legacy_reload(id);
}

/// Two DDLs while the source is stopped, then catch-up: every schema
/// version registered while catching up carries its positional proof, and
/// the live (latest) shape is never registered against the earlier DDL.
#[tokio::test]
#[ignore = "requires docker"]
async fn ddls_while_stopped_register_only_proven_versions() {
    let (id, db) = ("da_c", "da_c");
    let (port, st, r) = setup(id, db, &[format!("{db}.t")], &[]).await;
    stop(r.handle).await;
    let hash = lineage_hash(port).await;
    let before = versions(&st, id, &hash, db, "t").await;
    assert!(!before.is_empty());

    // Unqualified, in the table's own database: the case in which a catalog
    // reload at the first DDL would find the table and register its latest
    // (second-DDL) shape against the first.
    sql(
        port,
        &[
            format!("USE {db}"),
            "ALTER TABLE t RENAME COLUMN v TO w".into(),
        ],
    )
    .await;
    let (uuid, g) = executed(port).await;
    let ddl1 = own_gno(&g, &uuid);
    sql(
        port,
        &[
            format!("USE {db}"),
            "ALTER TABLE t RENAME COLUMN w TO z".into(),
        ],
    )
    .await;
    let (_, g) = executed(port).await;
    let ddl2 = own_gno(&g, &uuid);
    assert!(ddl2 > ddl1);

    let mut r = run(source(id, port, &[format!("{db}.t")], &st), &st).await;
    sql(
        port,
        &[format!("INSERT INTO {db}.t (id, z) VALUES (2, 20)")],
    )
    .await;
    let post = expect_row(&mut r, db, 2, "after catch-up").await;
    assert_eq!(cols(&post), ["id", "z"]);
    stop(r.handle).await;

    let after = versions(&st, id, &hash, db, "t").await;
    let new: Vec<_> = after
        .iter()
        .filter(|v| v.0 > before.last().unwrap().0)
        .collect();
    assert!(
        !new.is_empty(),
        "the post-DDL shape was registered: {after:?}"
    );
    for (version, cols, cp) in &new {
        let cp = cp.as_ref().unwrap_or_else(|| {
            panic!("version {version} {cols:?} has no proof checkpoint")
        });
        let text = String::from_utf8_lossy(cp);
        let bound = own_gno(
            &serde_json::from_slice::<Value>(cp)
                .ok()
                .and_then(|v| find_gtid_set(&v))
                .unwrap_or_default(),
            &uuid,
        );
        if cols.iter().any(|c| c == "z") {
            assert!(
                bound >= ddl2,
                "version {version} {cols:?} bound at transaction {bound}, before the DDL ({ddl2}) that produced it: {text}"
            );
        }
    }
    no_legacy_reload(id);
}

/// The GTID set string anywhere inside a serialized checkpoint.
fn find_gtid_set(v: &Value) -> Option<String> {
    match v {
        Value::Object(m) => m.iter().find_map(|(k, v)| match v {
            Value::String(s) if k.contains("gtid") => Some(s.clone()),
            other => find_gtid_set(other),
        }),
        Value::Array(a) => a.iter().find_map(find_gtid_set),
        _ => None,
    }
}

/// Executable-comment DDL is attributed as executed and emitted with its
/// original SQL (an applicable guard executes, a non-applicable one does
/// not); a leading plain comment hides nothing; CREATE, ALTER and DROP
/// DATABASE are scoped alike, quoted names and IF [NOT] EXISTS included; an
/// executable-comment account statement is redacted. The statements run
/// while the source is stopped are emitted the same way on catch-up.
#[tokio::test]
#[ignore = "requires docker"]
async fn executable_comment_and_database_ddl() {
    const SECRET: &str = "Da-x-Secret-4417";
    let (id, db, dbx) = ("da_x", "da_x", "da x");
    let (port, st, mut r) = setup(
        id,
        db,
        &[format!("{db}.t")],
        &[
            format!("DROP DATABASE IF EXISTS `{dbx}`"),
            format!("CREATE TABLE {db}.a (id INT PRIMARY KEY)"),
            "DROP USER IF EXISTS 'da_x_user'@'%'".into(),
        ],
    )
    .await;
    let t = obj("table", db, "t");
    let database = obj("database", dbx, dbx);

    // Live.
    let applicable =
        format!("/*!50100 ALTER TABLE {db}.t ADD COLUMN c1 INT */");
    let partial = format!(
        "ALTER TABLE {db}.t ADD COLUMN c2 INT /*!99999 , ADD COLUMN c3 INT */"
    );
    let rename = format!("/*! RENAME TABLE {db}.a TO {db}.a2 */");
    sql(
        port,
        &[
            applicable.clone(),
            partial.clone(),
            rename.clone(),
            format!("CREATE DATABASE IF NOT EXISTS `{dbx}`"),
            format!("ALTER DATABASE `{dbx}` CHARACTER SET utf8mb4"),
            format!(
                "/*!80000 CREATE USER 'da_x_user'@'%' IDENTIFIED BY '{SECRET}' */"
            ),
            format!("INSERT INTO {db}.t (id, v, c1, c2) VALUES (2, 20, 1, 2)"),
        ],
    )
    .await;
    let row2 = expect_row(&mut r, db, 2, "after executable-comment DDL").await;
    assert_eq!(
        cols(&row2),
        ["c1", "c2", "id", "v"],
        "c3 was never executed"
    );

    // The server logs a non-applicable guard as a plain comment (`/*!` ->
    // `/* `); the event carries the statement exactly as logged.
    let partial_logged = partial.replace("/*!99999", "/* 99999");
    for (needle, sql_text, objects) in [
        ("ADD COLUMN c1", &applicable, serde_json::json!([t])),
        ("ADD COLUMN c2", &partial_logged, serde_json::json!([t])),
        (
            "RENAME TABLE",
            &rename,
            serde_json::json!([obj("table", db, "a"), obj("table", db, "a2")]),
        ),
    ] {
        let (source_db, p) = ddl(&mut r, needle).await;
        assert_eq!(p["sql"], sql_text.as_str(), "original SQL kept");
        assert_eq!(p["scope"], "tables", "{p}");
        assert_eq!(p["objects"], objects, "{p}");
        assert_eq!(source_db, db);
    }
    for needle in ["CREATE DATABASE", "ALTER DATABASE"] {
        let (source_db, p) = ddl(&mut r, needle).await;
        assert_eq!(p["scope"], "database", "{needle}: {p}");
        assert_eq!(p["objects"], serde_json::json!([database]), "{needle}");
        assert_eq!(p["database"], dbx);
        assert_eq!(source_db, dbx);
    }
    let (_, p) = ddl(&mut r, "CREATE USER").await;
    assert_eq!(p["redacted"], true, "{p}");
    assert!(p.get("sql").is_none() && !p.to_string().contains(SECRET));
    stop(r.handle).await;

    // While stopped, then catch-up.
    // (DROP TABLE is regenerated in the binlog; ALTER is logged as written.)
    let commented = format!("/* audit */ ALTER TABLE {db}.t ADD COLUMN c5 INT");
    let drop_db = format!("/*!40101 DROP DATABASE IF EXISTS `{dbx}` */");
    let c4 = format!("/*!50100 ALTER TABLE {db}.t ADD COLUMN c4 INT */");
    sql(
        port,
        &[
            commented.clone(),
            drop_db.clone(),
            c4.clone(),
            "DROP USER 'da_x_user'@'%'".into(),
        ],
    )
    .await;
    let mut r = run(source(id, port, &[format!("{db}.t")], &st), &st).await;
    sql(
        port,
        &[format!(
            "INSERT INTO {db}.t (id, v, c1, c2, c4) VALUES (3, 30, 1, 2, 4)"
        )],
    )
    .await;
    let row3 = expect_row(&mut r, db, 3, "after catch-up").await;
    assert_eq!(cols(&row3), ["c1", "c2", "c4", "c5", "id", "v"]);
    let (_, p) = ddl(&mut r, "ADD COLUMN c5").await;
    assert_eq!(p["sql"], commented.as_str());
    assert_eq!(p["objects"], serde_json::json!([t]));
    let (source_db, p) = ddl(&mut r, "DROP DATABASE").await;
    assert_eq!(p["sql"], drop_db.as_str());
    assert_eq!(p["scope"], "database");
    assert_eq!(p["objects"], serde_json::json!([database]));
    assert_eq!(source_db, dbx);
    let (_, p) = ddl(&mut r, "ADD COLUMN c4").await;
    assert_eq!(p["sql"], c4.as_str());
    assert_eq!(p["objects"], serde_json::json!([t]));
    stop(r.handle).await;

    assert!(logs(id, SECRET).is_empty(), "the secret was logged");
    no_legacy_reload(id);
}
