//! Lazy per-table failover drift (design spec 7.22): after a failover from
//! server A to server B, each table's schema on B is compared with its last
//! version under A at the table's first event under B's lineage - CDC rows,
//! a DDL, or a snapshot - never by enumerating tables. B is set up like a
//! promoted replica: its schema exists, its binlog holds nothing before the
//! failover position (`RESET BINARY LOGS AND GTIDS`, then `gtid_purged` =
//! A's executed set), unless a scenario adds a DDL after that.
//!
//! Run with:
//! ```bash
//! cargo test -p sources --test mysql_failover_drift_e2e -- --include-ignored --test-threads=1
//! ```

use std::sync::Arc;
use std::time::Instant;

use checkpoints::{CheckpointStore, MemCheckpointStore};
use common::AllowList;
use ctor::dtor;
use deltaforge_config::{OnSchemaDrift, SnapshotCfg, SnapshotMode};
use deltaforge_core::{Event, Op, Source, SourceItem, SourceResult};
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
use test_common::init_test_tracing;

const ROOT_PW: &str = "pw";
const TENANT: &str = "acme";
const MARKER_NS: &str = "schemas.v1.failover.drift";
const ORDERS: &str =
    "CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(32))";

type Server = (ContainerAsync<GenericImage>, u16);
static A: OnceCell<Server> = OnceCell::const_new();
static B: OnceCell<Server> = OnceCell::const_new();

#[dtor]
fn cleanup() {
    for cell in [&A, &B] {
        if let Some((c, _)) = cell.get() {
            std::process::Command::new("docker")
                .args(["rm", "-f", "-v", c.id()])
                .output()
                .ok();
        }
    }
}

async fn start(server_id: u32) -> Server {
    let c = GenericImage::new("mysql", "8.4")
        .with_wait_for(WaitFor::message_on_stderr(
            "ready for connections. Version: '8.4",
        ))
        .with_env_var("MYSQL_ROOT_PASSWORD", ROOT_PW)
        .with_cmd(vec![
            format!("--server-id={server_id}"),
            "--log-bin=mysql-bin".into(),
            "--binlog-format=ROW".into(),
            "--binlog-row-image=FULL".into(),
            "--binlog-checksum=NONE".into(),
            "--gtid-mode=ON".into(),
            "--enforce-gtid-consistency=ON".into(),
        ])
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

async fn servers() -> (u16, u16) {
    let a = A.get_or_init(|| start(61)).await.1;
    let b = B.get_or_init(|| start(62)).await.1;
    (a, b)
}

fn dsn(port: u16, db: &str) -> String {
    format!("mysql://root:{ROOT_PW}@127.0.0.1:{port}/{db}")
}

async fn sql(port: u16, stmts: &[String]) {
    let mut c = mysql_async::Conn::from_url(dsn(port, "")).await.unwrap();
    for s in stmts {
        c.query_drop(s.as_str())
            .await
            .unwrap_or_else(|e| panic!("{s}: {e}"));
    }
    c.disconnect().await.ok();
}

async fn query_one(port: u16, q: &str) -> String {
    let mut c = mysql_async::Conn::from_url(dsn(port, "")).await.unwrap();
    let v: String = c.query_first(q).await.unwrap().unwrap();
    c.disconnect().await.ok();
    v
}

struct State {
    backend: ArcStorageBackend,
    registry: Arc<DurableSchemaRegistry>,
    ckpt: Arc<dyn CheckpointStore>,
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

    /// The table's activation records under `port`'s lineage.
    async fn activation(
        &self,
        port: u16,
        id: &str,
        db: &str,
        table: &str,
    ) -> Vec<Value> {
        let uuid = query_one(port, "SELECT @@GLOBAL.server_uuid").await;
        let hash = LineageDescriptor::mysql(&uuid).unwrap().lineage_hash();
        let key = SchemaKey::new(TENANT, id, &hash, db, table).backend_key();
        self.backend
            .log_list("schemas.v1.activation", &key)
            .await
            .unwrap()
            .into_iter()
            .map(|(_, b)| serde_json::from_slice(&b).unwrap())
            .collect()
    }

    /// The table's failover drift marker under `port`'s lineage.
    async fn marker(
        &self,
        port: u16,
        id: &str,
        db: &str,
        table: &str,
    ) -> Option<Value> {
        let uuid = query_one(port, "SELECT @@GLOBAL.server_uuid").await;
        let hash = LineageDescriptor::mysql(&uuid).unwrap().lineage_hash();
        let key = SchemaKey::new(TENANT, id, &hash, db, table).backend_key();
        self.backend
            .log_list(MARKER_NS, &key)
            .await
            .unwrap()
            .into_iter()
            .map(|(_, b)| serde_json::from_slice(&b).unwrap())
            .next()
    }
}

struct Scenario<'a> {
    id: &'a str,
    db: &'a str,
    tables: Vec<String>,
}

fn source(
    sc: &Scenario<'_>,
    port: u16,
    st: &State,
    policy: OnSchemaDrift,
    mode: SnapshotMode,
) -> MySqlSource {
    MySqlSource {
        id: sc.id.into(),
        dsn: dsn(port, sc.db).as_str().into(),
        tables: sc.tables.clone(),
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
        on_schema_drift: policy,
        table_options: Default::default(),
        rotation: None,
    }
}

fn rows(items: &[SourceItem]) -> Vec<&Event> {
    items
        .iter()
        .filter_map(|i| match i {
            SourceItem::Event(e)
                if e.ddl.is_none()
                    && matches!(
                        e.op,
                        Op::Create | Op::Update | Op::Delete | Op::Read
                    ) =>
            {
                Some(e)
            }
            _ => None,
        })
        .collect()
}

/// Run `src`, execute `during` once it is streaming, then collect until it
/// stops or `wait` passes; returns everything it sent and how it ended
/// (`None`: still running, then stopped).
async fn run(
    src: MySqlSource,
    st: &State,
    port: u16,
    during: &[String],
    wait: Duration,
) -> (Vec<SourceItem>, Option<SourceResult<()>>) {
    let (tx, mut rx) = mpsc::channel(1024);
    let handle = src.run(tx, st.ckpt.clone()).await;
    let collector = tokio::spawn(async move {
        let mut items = Vec::new();
        while let Some(i) = rx.recv().await {
            items.push(i);
        }
        items
    });
    sleep(Duration::from_secs(4)).await;
    sql(port, during).await;
    let deadline = Instant::now() + wait;
    while !handle.join.is_finished() && Instant::now() < deadline {
        sleep(Duration::from_millis(200)).await;
    }
    let ended = if handle.join.is_finished() {
        Some(
            timeout(Duration::from_secs(5), handle.join)
                .await
                .expect("joined")
                .expect("task"),
        )
    } else {
        handle.stop();
        timeout(Duration::from_secs(30), handle.join).await.ok();
        None
    };
    let items = collector.await.unwrap();
    (items, ended)
}

/// Run 1 on A: the table's schema (id, sku) is registered under A's
/// lineage and a committed position recorded.
async fn on_a(sc: &Scenario<'_>, st: &State, a: u16) {
    sql(
        a,
        &[
            format!("DROP DATABASE IF EXISTS {}", sc.db),
            format!("CREATE DATABASE {}", sc.db),
            format!("USE {}", sc.db),
            ORDERS.to_string(),
        ],
    )
    .await;
    let src = source(sc, a, st, OnSchemaDrift::Adapt, SnapshotMode::Never);
    let (items, _) = run(
        src,
        st,
        a,
        &[format!("INSERT INTO {}.orders VALUES (1, 'on-a')", sc.db)],
        Duration::from_secs(4),
    )
    .await;
    assert_eq!(rows(&items).len(), 1, "run 1 on A");
}

/// B as a promoted replica of A: the scenario's schema (with `drift`
/// applied when given) and `rows` exist; B's binlog holds nothing before the
/// failover position.
async fn promote_b(
    sc: &Scenario<'_>,
    a: u16,
    b: u16,
    drift: Option<&str>,
    rows: &[&str],
) {
    let mut s = vec![
        format!("DROP DATABASE IF EXISTS {}", sc.db),
        format!("CREATE DATABASE {}", sc.db),
        format!("USE {}", sc.db),
        ORDERS.to_string(),
    ];
    if let Some(d) = drift {
        s.push(d.to_string());
    }
    for r in rows {
        s.push(r.to_string());
    }
    sql(b, &s).await;
    let executed = query_one(a, "SELECT @@GLOBAL.gtid_executed").await;
    sql(
        b,
        &[
            "RESET BINARY LOGS AND GTIDS".to_string(),
            format!("SET GLOBAL gtid_purged = '{executed}'"),
        ],
    )
    .await;
}

fn halted(ended: &Option<SourceResult<()>>, needle: &str) {
    let msg = match ended {
        // The typed cause keeps the detailed message (the incident's own
        // display is its sanitized explanation).
        Some(Err(e)) => e.root().to_string(),
        other => panic!("the source must stop: {other:?}"),
    };
    assert!(
        msg.contains(needle) && msg.contains("on_schema_drift=halt"),
        "unexpected error: {msg}"
    );
}

const STATUS: &str = "ALTER TABLE orders ADD COLUMN status VARCHAR(32)";

/// Proven drift under halt: B's table gained a column before the failover
/// position. The first rows under B stop the source before anything is
/// registered or emitted, the committed checkpoint does not move, and a
/// restart stops again (no completion is recorded).
#[tokio::test]
#[ignore = "requires docker"]
async fn proven_drift_halts_before_any_row() {
    init_test_tracing();
    let (a, b) = servers().await;
    let sc = Scenario {
        id: "fd_halt",
        db: "fd_halt",
        tables: vec!["fd_halt.orders".into()],
    };
    let st = State::new().await;
    on_a(&sc, &st, a).await;
    promote_b(&sc, a, b, Some(STATUS), &[]).await;
    let committed: sources::MySqlCheckpoint =
        serde_json::from_slice(&st.ckpt.get_raw(sc.id).await.unwrap().unwrap())
            .unwrap();
    let insert = [format!(
        "INSERT INTO {}.orders VALUES (2, 'on-b', 'x')",
        sc.db
    )];
    for attempt in 0..2 {
        let src = source(&sc, b, &st, OnSchemaDrift::Halt, SnapshotMode::Never);
        let during: &[String] = if attempt == 0 { &insert } else { &[] };
        let (items, ended) =
            run(src, &st, b, during, Duration::from_secs(20)).await;
        halted(&ended, "schema drift since the failover");
        assert!(rows(&items).is_empty(), "no row under halt");
        // The position does not move (startup re-attributes the carried
        // checkpoint to B's lineage; its coordinates stay).
        let now: sources::MySqlCheckpoint = serde_json::from_slice(
            &st.ckpt.get_raw(sc.id).await.unwrap().unwrap(),
        )
        .unwrap();
        assert_eq!(
            (&now.file, now.pos, &now.gtid_set),
            (&committed.file, committed.pos, &committed.gtid_set)
        );
        assert!(st.marker(b, sc.id, sc.db, "orders").await.is_none());
    }
}

/// Proven drift under adapt, for a table matched by a wildcard: the rows
/// decode with B's shape and the outcome is recorded with both hashes.
#[tokio::test]
#[ignore = "requires docker"]
async fn proven_drift_adapts_wildcard_tables() {
    init_test_tracing();
    let (a, b) = servers().await;
    let sc = Scenario {
        id: "fd_adapt",
        db: "fd_adapt",
        tables: vec!["fd_adapt.*".into()],
    };
    let st = State::new().await;
    on_a(&sc, &st, a).await;
    promote_b(&sc, a, b, Some(STATUS), &[]).await;
    let src = source(&sc, b, &st, OnSchemaDrift::Adapt, SnapshotMode::Never);
    let (items, ended) = run(
        src,
        &st,
        b,
        &[format!(
            "INSERT INTO {}.orders VALUES (2, 'on-b', 'x')",
            sc.db
        )],
        Duration::from_secs(6),
    )
    .await;
    assert!(ended.is_none(), "still streaming: {ended:?}");
    let got = rows(&items);
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].after.as_ref().unwrap()["status"], "x");
    let m = st
        .marker(b, sc.id, sc.db, "orders")
        .await
        .expect("recorded");
    assert_eq!(m["outcome"], "adapted", "{m}");
    assert!(m["previous_schema_hash"].is_string(), "{m}");
    assert!(m["current_schema_hash"].is_string(), "{m}");
    assert_ne!(m["previous_schema_hash"], m["current_schema_hash"], "{m}");

    // The check completed: a restart under halt streams on.
    let src = source(&sc, b, &st, OnSchemaDrift::Halt, SnapshotMode::Never);
    let (items, ended) = run(
        src,
        &st,
        b,
        &[format!(
            "INSERT INTO {}.orders VALUES (3, 'on-b', 'y')",
            sc.db
        )],
        Duration::from_secs(6),
    )
    .await;
    assert!(ended.is_none(), "still streaming: {ended:?}");
    assert_eq!(rows(&items).len(), 1);
}

/// No drift under halt: streaming continues, recorded as unchanged.
#[tokio::test]
#[ignore = "requires docker"]
async fn no_drift_continues_under_halt() {
    init_test_tracing();
    let (a, b) = servers().await;
    let sc = Scenario {
        id: "fd_same",
        db: "fd_same",
        tables: vec!["fd_same.orders".into()],
    };
    let st = State::new().await;
    on_a(&sc, &st, a).await;
    promote_b(&sc, a, b, None, &[]).await;
    let src = source(&sc, b, &st, OnSchemaDrift::Halt, SnapshotMode::Never);
    let (items, ended) = run(
        src,
        &st,
        b,
        &[format!("INSERT INTO {}.orders VALUES (2, 'on-b')", sc.db)],
        Duration::from_secs(6),
    )
    .await;
    assert!(ended.is_none(), "still streaming: {ended:?}");
    assert_eq!(rows(&items).len(), 1);
    let m = st
        .marker(b, sc.id, sc.db, "orders")
        .await
        .expect("recorded");
    assert_eq!(m["outcome"], "unchanged", "{m}");
}

/// The table's first event under B is a DDL (executed on B after the
/// failover position): its shape at the failover position cannot be
/// proven. Halt stops before anything is recorded; adapt records it as
/// adapted-unprovable and continues through normal DDL proof.
#[tokio::test]
#[ignore = "requires docker"]
async fn a_first_event_ddl_is_unprovable() {
    init_test_tracing();
    let (a, b) = servers().await;
    for (policy, id) in [
        (OnSchemaDrift::Halt, "fd_ddl_halt"),
        (OnSchemaDrift::Adapt, "fd_ddl_adapt"),
    ] {
        let sc = Scenario {
            id,
            db: id,
            tables: vec![format!("{id}.orders")],
        };
        let st = State::new().await;
        on_a(&sc, &st, a).await;
        promote_b(&sc, a, b, None, &[]).await;
        sql(
            b,
            &[format!(
                "ALTER TABLE {id}.orders ADD COLUMN status VARCHAR(32)"
            )],
        )
        .await;
        let src = source(&sc, b, &st, policy.clone(), SnapshotMode::Never);
        let (items, ended) = run(
            src,
            &st,
            b,
            &[format!("INSERT INTO {id}.orders VALUES (2, 'on-b', 'x')")],
            Duration::from_secs(8),
        )
        .await;
        if policy == OnSchemaDrift::Halt {
            halted(&ended, "cannot be proven");
            assert!(rows(&items).is_empty());
            assert!(st.marker(b, id, id, "orders").await.is_none());
            // Stopped before the DDL's records were written.
            assert!(
                st.activation(b, id, id, "orders").await.is_empty(),
                "no activation record under B"
            );
        } else {
            assert!(ended.is_none(), "still streaming: {ended:?}");
            let got = rows(&items);
            assert_eq!(got.len(), 1);
            assert_eq!(got[0].after.as_ref().unwrap()["status"], "x");
            let m = st.marker(b, id, id, "orders").await.expect("recorded");
            assert_eq!(m["outcome"], "adapted_unprovable", "{m}");
            assert!(m["current_schema_hash"].is_null(), "{m}");
        }
    }
}

/// A snapshot after the failover: the shape at the failover position is
/// proven by a complete scan from it. Halt stops before any snapshot row;
/// adapt records the drift, then the snapshot copies B's rows.
#[tokio::test]
#[ignore = "requires docker"]
async fn a_snapshot_after_failover_applies_the_policy_first() {
    init_test_tracing();
    let (a, b) = servers().await;
    for (policy, id) in [
        (OnSchemaDrift::Halt, "fd_snap_halt"),
        (OnSchemaDrift::Adapt, "fd_snap_adapt"),
    ] {
        let sc = Scenario {
            id,
            db: id,
            tables: vec![format!("{id}.orders")],
        };
        let st = State::new().await;
        on_a(&sc, &st, a).await;
        promote_b(
            &sc,
            a,
            b,
            Some(STATUS),
            &["INSERT INTO orders VALUES (5, 'on-b', 'x')"],
        )
        .await;
        let src = source(&sc, b, &st, policy.clone(), SnapshotMode::Always);
        let (items, ended) =
            run(src, &st, b, &[], Duration::from_secs(8)).await;
        if policy == OnSchemaDrift::Halt {
            halted(&ended, "schema drift since the failover");
            assert!(rows(&items).is_empty(), "no snapshot row under halt");
        } else {
            assert!(ended.is_none(), "still streaming: {ended:?}");
            let got = rows(&items);
            assert_eq!(got.len(), 1, "the snapshot row");
            let m = st.marker(b, id, id, "orders").await.expect("recorded");
            assert_eq!(m["outcome"], "adapted", "{m}");
        }
    }
}

/// An unattributable statement on B after the failover position (here a
/// versioned-comment DDL that also changes the table) precedes the table's
/// first rows: the shape at the failover position cannot be proven from
/// the rows on, and halt stops before any row.
#[tokio::test]
#[ignore = "requires docker"]
async fn a_barrier_after_the_failover_position_is_unprovable() {
    init_test_tracing();
    let (a, b) = servers().await;
    let sc = Scenario {
        id: "fd_barrier",
        db: "fd_barrier",
        tables: vec!["fd_barrier.orders".into()],
    };
    let st = State::new().await;
    on_a(&sc, &st, a).await;
    promote_b(&sc, a, b, None, &[]).await;
    sql(
        b,
        &[format!(
            "/*!50100 ALTER TABLE {}.orders ADD COLUMN status VARCHAR(32) */",
            sc.db
        )],
    )
    .await;
    let src = source(&sc, b, &st, OnSchemaDrift::Halt, SnapshotMode::Never);
    let (items, ended) = run(
        src,
        &st,
        b,
        &[format!(
            "INSERT INTO {}.orders VALUES (2, 'on-b', 'x')",
            sc.db
        )],
        Duration::from_secs(20),
    )
    .await;
    halted(&ended, "cannot be proven");
    assert!(rows(&items).is_empty());
}
