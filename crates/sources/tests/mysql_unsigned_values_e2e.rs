//! Unsigned integer columns keep their values: a `* UNSIGNED` value above
//! the signed range of its width is emitted as that unsigned value (never
//! negative) by the snapshot, by inserts, by both images of an update and by deletes,
//! in the event JSON a sink serializes, under both `binlog_row_metadata`
//! settings; and the registered schema records the columns as unsigned.
//!
//! ```bash
//! cargo test -p sources --test mysql_unsigned_values_e2e -- --include-ignored --test-threads=1
//! ```

use std::sync::Arc;
use std::time::Instant;

use checkpoints::{CheckpointStore, MemCheckpointStore};
use common::AllowList;
use ctor::dtor;
use deltaforge_config::{OnSchemaDrift, SnapshotCfg, SnapshotMode};
use deltaforge_core::{Event, Op, Source, SourceItem};
use gate_ownership::GateOwned;
use mysql_async::prelude::Queryable;
use serde_json::{Value, json};
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

async fn start() -> Server {
    let c = GenericImage::new("mysql", "8.4")
        .with_wait_for(WaitFor::message_on_stderr(
            "ready for connections. Version: '8.4",
        ))
        .with_env_var("MYSQL_ROOT_PASSWORD", ROOT_PW)
        .with_cmd(vec![
            "--server-id=48",
            "--log-bin=mysql-bin",
            "--binlog-format=ROW",
            "--binlog-row-image=FULL",
            "--binlog-checksum=NONE",
            "--gtid-mode=ON",
            "--enforce-gtid-consistency=ON",
        ])
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

/// Each width at its signed boundary + 1 and at its unsigned maximum.
const COLUMNS: &str = "id INT PRIMARY KEY, \
     ti TINYINT UNSIGNED, si SMALLINT UNSIGNED, mi MEDIUMINT UNSIGNED, \
     i INT UNSIGNED, b BIGINT UNSIGNED";
const LOW: [u64; 5] = [128, 32_768, 8_388_608, 2_147_483_648, 1 << 63];
const HIGH: [u64; 5] = [
    255,
    65_535,
    16_777_215,
    4_294_967_295,
    18_446_744_073_709_551_615,
];
const NAMES: [&str; 5] = ["ti", "si", "mi", "i", "b"];

fn insert(db: &str, id: u32, v: [u64; 5]) -> String {
    format!(
        "INSERT INTO {db}.t VALUES ({id}, {}, {}, {}, {}, {})",
        v[0], v[1], v[2], v[3], v[4]
    )
}

/// How a value is represented: the stream emits JSON numbers; the
/// snapshot (MySQL's text protocol) emits every column value as a string,
/// a separate snapshot/stream representation difference recorded with
/// this check, not an unsigned defect.
#[derive(Clone, Copy)]
enum Repr {
    Number,
    Text,
}

/// The image's unsigned columns equal `v`, in the event and in the JSON a
/// sink serializes from it.
fn assert_image(e: &Event, image: &str, v: [u64; 5], repr: Repr, what: &str) {
    let serialized: Value = serde_json::to_value(e).unwrap();
    for (n, want) in NAMES.iter().zip(v) {
        let want = match repr {
            Repr::Number => json!(want),
            Repr::Text => json!(want.to_string()),
        };
        let got = match image {
            "after" => &e.after.as_ref().unwrap()[n],
            _ => &e.before.as_ref().unwrap()[n],
        };
        assert_eq!(got, &want, "{what}: {image}.{n}: {e:?}");
        assert_eq!(serialized[image][n], want, "{what}: serialized");
    }
}

struct Run {
    rx: mpsc::Receiver<SourceItem>,
    handle: deltaforge_core::SourceHandle,
    registry: Arc<DurableSchemaRegistry>,
}

async fn run(port: u16, id: &str, db: &str, snapshot: SnapshotMode) -> Run {
    let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
    let registry = DurableSchemaRegistry::new(backend.clone()).await.unwrap();
    let ckpt: Arc<dyn CheckpointStore> =
        Arc::new(MemCheckpointStore::new().unwrap());
    let src = MySqlSource {
        id: id.into(),
        dsn: dsn(port).as_str().into(),
        tables: vec![format!("{db}.t")],
        tenant: TENANT.into(),
        pipeline: "test".into(),
        registry: registry.clone(),
        registry_scope: sources::registry_scope::SharedRegistryScope::default(),
        backend,
        outbox_tables: AllowList::default(),
        snapshot_cfg: SnapshotCfg {
            mode: snapshot,
            ..Default::default()
        },
        on_schema_drift: OnSchemaDrift::Adapt,
        table_options: Default::default(),
        rotation: None,
        snapshot_cohort: Default::default(),
    };
    let (tx, rx) = test_common::acked_channel(&src, &ckpt, &src.id, 1024);
    let handle = src.run(tx, ckpt.clone()).await;
    sleep(Duration::from_secs(3)).await;
    Run {
        rx,
        handle,
        registry,
    }
}

/// The next row event of `id` with operation `op`.
async fn next(r: &mut Run, op: Op, id: i64) -> Event {
    let deadline = Instant::now() + Duration::from_secs(60);
    while let Some(left) = deadline.checked_duration_since(Instant::now()) {
        match timeout(left, r.rx.recv()).await {
            Ok(Some(SourceItem::Event(e)))
                if e.ddl.is_none()
                    && e.op == op
                    && e.after
                        .as_ref()
                        .or(e.before.as_ref())
                        .map(|a| &a["id"])
                        .is_some_and(|v| {
                            v.as_i64() == Some(id)
                                || v.as_str() == Some(&id.to_string())
                        }) =>
            {
                return e;
            }
            Ok(Some(_)) => {}
            Ok(None) => panic!("the source stopped before {op:?} id={id}"),
            Err(_) => break,
        }
    }
    panic!("no {op:?} id={id} within 60s")
}

/// The registered schema records every unsigned column as unsigned.
async fn assert_schema(r: &Run, port: u16, source: &str, db: &str) {
    let mut c = mysql_async::Conn::from_url(dsn(port)).await.unwrap();
    let uuid: String = c
        .query_first("SELECT @@GLOBAL.server_uuid")
        .await
        .unwrap()
        .unwrap();
    c.disconnect().await.ok();
    let hash = LineageDescriptor::mysql(&uuid).unwrap().lineage_hash();
    let key = SchemaKey::new(TENANT, source, &hash, db, "t");
    let v = r
        .registry
        .get_latest(&key)
        .await
        .unwrap()
        .expect("a version");
    for n in NAMES {
        let col = v.schema_json["columns"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["name"] == n)
            .unwrap_or_else(|| panic!("{n} in {}", v.schema_json));
        assert!(
            col["column_type"].as_str().unwrap().contains("unsigned"),
            "{n}: {col}"
        );
    }
}

async fn stream_case(row_metadata: &str) {
    let port = SERVER.get_or_init(start).await.1;
    let (id, db) = (
        format!("uns_{}", row_metadata.to_lowercase()),
        format!("uns_{}", row_metadata.to_lowercase()),
    );
    sql(
        port,
        &[
            format!("SET GLOBAL binlog_row_metadata = {row_metadata}"),
            format!("DROP DATABASE IF EXISTS {db}"),
            format!("CREATE DATABASE {db}"),
            format!("CREATE TABLE {db}.t ({COLUMNS})"),
        ],
    )
    .await;
    let mut r = run(port, &id, &db, SnapshotMode::Never).await;
    sql(port, &[insert(&db, 1, LOW), insert(&db, 2, HIGH)]).await;
    let e = next(&mut r, Op::Create, 1).await;
    assert_image(
        &e,
        "after",
        LOW,
        Repr::Number,
        "insert at the signed boundary",
    );
    let e = next(&mut r, Op::Create, 2).await;
    assert_image(
        &e,
        "after",
        HIGH,
        Repr::Number,
        "insert at the unsigned maximum",
    );
    sql(
        port,
        &[format!(
            "UPDATE {db}.t SET ti={}, si={}, mi={}, i={}, b={} WHERE id=1",
            HIGH[0], HIGH[1], HIGH[2], HIGH[3], HIGH[4]
        )],
    )
    .await;
    let e = next(&mut r, Op::Update, 1).await;
    assert_image(&e, "before", LOW, Repr::Number, "update");
    assert_image(&e, "after", HIGH, Repr::Number, "update");
    sql(port, &[format!("DELETE FROM {db}.t WHERE id=2")]).await;
    let e = next(&mut r, Op::Delete, 2).await;
    assert_image(&e, "before", HIGH, Repr::Number, "delete");
    assert_schema(&r, port, &id, &db).await;
    r.handle.stop();
}

#[tokio::test]
#[ignore = "requires docker"]
async fn unsigned_values_stream_unchanged_with_minimal_row_metadata() {
    stream_case("MINIMAL").await;
}

#[tokio::test]
#[ignore = "requires docker"]
async fn unsigned_values_stream_unchanged_with_full_row_metadata() {
    stream_case("FULL").await;
}

#[tokio::test]
#[ignore = "requires docker"]
async fn unsigned_values_snapshot_unchanged() {
    let port = SERVER.get_or_init(start).await.1;
    let (id, db) = ("uns_snap", "uns_snap");
    sql(
        port,
        &[
            format!("DROP DATABASE IF EXISTS {db}"),
            format!("CREATE DATABASE {db}"),
            format!("CREATE TABLE {db}.t ({COLUMNS})"),
            insert(db, 1, LOW),
            insert(db, 2, HIGH),
        ],
    )
    .await;
    let mut r = run(port, id, db, SnapshotMode::Initial).await;
    let e = next(&mut r, Op::Read, 1).await;
    assert_image(
        &e,
        "after",
        LOW,
        Repr::Text,
        "snapshot at the signed boundary",
    );
    let e = next(&mut r, Op::Read, 2).await;
    assert_image(
        &e,
        "after",
        HIGH,
        Repr::Text,
        "snapshot at the unsigned maximum",
    );
    assert_schema(&r, port, id, db).await;
    r.handle.stop();
}
