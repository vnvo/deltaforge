//! Failover integration tests — real infrastructure conditions.
//!
//! Each test spins up two containers representing "primary before failover"
//! and "primary after failover". The source runs against the first, stops,
//! then runs against the second. Identity change is detected naturally from
//! real server UUIDs / system identifiers — no identity store pre-seeding.
//!
//! Run with:
//! ```bash
//! cargo test -p sources --test failover_e2e -- --include-ignored --nocapture --test-threads=1
//! ```

use anyhow::Result;
use checkpoints::{CheckpointStore, MemCheckpointStore};
use common::AllowList;
use deltaforge_config::SnapshotCfg;
use deltaforge_core::{Event, Source};
use mysql_async::prelude::Queryable;
use sources::failover::identity::{
    IdentityComparison, IdentityStore, ServerIdentity,
};
use sources::failover::reconciler::{SchemaDelta, SchemaReconciler};
use sources::mysql::{MySqlSource, mysql_health};
use sources::postgres::{PostgresSource, postgres_health};
use std::sync::Arc;
use std::time::Instant;
use storage::{ArcStorageBackend, MemoryStorageBackend};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt, core::WaitFor, runners::AsyncRunner,
};
use tokio::sync::mpsc;
use tokio::time::{Duration, sleep, timeout};
use tokio_postgres::NoTls;
use tracing::info;

mod test_common;
use test_common::{
    MYSQL_CDC_PASSWORD, MYSQL_CDC_USER, MYSQL_ROOT_PASSWORD, PG_PASS, PG_USER,
    init_test_tracing, make_registry,
};

// ============================================================================
// Per-test MySQL container
// MySQL 8.4 auto-generates server_uuid into auto.cnf — two containers will
// naturally have distinct UUIDs, which is all the tests require.
// ============================================================================

async fn start_mysql() -> (ContainerAsync<GenericImage>, u16) {
    let c = GenericImage::new("mysql", "8.4")
        .with_wait_for(WaitFor::message_on_stderr("ready for connections"))
        .with_env_var("MYSQL_ROOT_PASSWORD", MYSQL_ROOT_PASSWORD)
        .with_cmd([
            "--server-id=1",
            "--log-bin=mysql-bin",
            "--binlog-format=ROW",
            "--binlog-row-image=FULL",
            "--gtid-mode=ON",
            "--enforce-gtid-consistency=ON",
            "--binlog-checksum=NONE",
        ])
        .start()
        .await
        .expect("start mysql");
    let port = c.get_host_port_ipv4(3306).await.expect("mysql port");
    sleep(Duration::from_secs(10)).await;
    provision_mysql_cdc_user(port).await;
    (c, port)
}

async fn provision_mysql_cdc_user(port: u16) {
    let dsn = format!("mysql://root:{MYSQL_ROOT_PASSWORD}@127.0.0.1:{port}/");
    let pool =
        mysql_async::Pool::new(mysql_async::Opts::from_url(&dsn).unwrap());
    let mut conn = pool.get_conn().await.unwrap();
    conn.query_drop(format!(
        "CREATE USER IF NOT EXISTS '{MYSQL_CDC_USER}'@'%' IDENTIFIED BY '{MYSQL_CDC_PASSWORD}'"
    )).await.unwrap();
    conn.query_drop(format!(
        "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '{MYSQL_CDC_USER}'@'%'"
    )).await.unwrap();
    conn.query_drop("FLUSH PRIVILEGES").await.unwrap();
}

fn mysql_root_dsn(port: u16) -> String {
    format!("mysql://root:{MYSQL_ROOT_PASSWORD}@127.0.0.1:{port}/")
}

fn mysql_cdc_dsn(port: u16, db: &str) -> String {
    format!(
        "mysql://{MYSQL_CDC_USER}:{MYSQL_CDC_PASSWORD}@127.0.0.1:{port}/{db}"
    )
}

async fn mysql_root_pool(port: u16) -> mysql_async::Pool {
    mysql_async::Pool::new(
        mysql_async::Opts::from_url(&mysql_root_dsn(port)).unwrap(),
    )
}

async fn mysql_fetch_uuid(port: u16) -> String {
    let dsn = mysql_cdc_dsn(port, "");
    mysql_health::fetch_server_identity(&dsn)
        .await
        .expect("fetch identity")
        .expect("identity present")
        .server_uuid
}

async fn mysql_create_schema(port: u16, db: &str) {
    let pool = mysql_root_pool(port).await;
    let mut conn = pool.get_conn().await.unwrap();
    conn.query_drop(format!("CREATE DATABASE IF NOT EXISTS {db}"))
        .await
        .unwrap();
    conn.query_drop(format!("USE {db}")).await.unwrap();
    conn.query_drop(
        "CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(64))",
    )
    .await
    .unwrap();
    conn.query_drop(format!(
        "GRANT SELECT, SHOW VIEW ON {db}.* TO '{MYSQL_CDC_USER}'@'%'"
    ))
    .await
    .unwrap();
}

// ============================================================================
// Per-test PostgreSQL container
// initdb assigns each container a unique system_identifier automatically.
// ============================================================================

async fn start_postgres() -> (ContainerAsync<GenericImage>, u16) {
    let c = GenericImage::new("postgres", "17")
        .with_wait_for(WaitFor::message_on_stderr("database system is ready"))
        .with_env_var("POSTGRES_USER", PG_USER)
        .with_env_var("POSTGRES_PASSWORD", PG_PASS)
        .with_cmd([
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=10",
            "-c",
            "max_wal_senders=10",
        ])
        .start()
        .await
        .expect("start postgres");
    let port = c.get_host_port_ipv4(5432).await.expect("pg port");
    sleep(Duration::from_secs(5)).await;
    (c, port)
}

async fn pg_admin_client(port: u16, db: &str) -> tokio_postgres::Client {
    let dsn = format!(
        "host=127.0.0.1 port={port} user={PG_USER} password={PG_PASS} dbname={db}"
    );
    let (client, conn) = tokio_postgres::connect(&dsn, NoTls).await.unwrap();
    tokio::spawn(async move {
        conn.await.ok();
    });
    client
}

fn pg_dsn(port: u16, db: &str) -> String {
    format!(
        "host=127.0.0.1 port={port} user={PG_USER} password={PG_PASS} dbname={db}"
    )
}

async fn pg_create_schema(port: u16, db: &str, slot: &str, pub_name: &str) {
    let root = pg_admin_client(port, "postgres").await;
    root.execute(&format!("CREATE DATABASE {db}"), &[])
        .await
        .ok();
    drop(root);

    let client = pg_admin_client(port, db).await;
    client
        .execute("CREATE TABLE orders (id INT PRIMARY KEY, sku TEXT)", &[])
        .await
        .unwrap();
    client
        .execute("ALTER TABLE orders REPLICA IDENTITY FULL", &[])
        .await
        .unwrap();
    client
        .execute(&format!("DROP PUBLICATION IF EXISTS {pub_name}"), &[])
        .await
        .ok();
    client
        .execute(
            &format!("CREATE PUBLICATION {pub_name} FOR TABLE orders"),
            &[],
        )
        .await
        .unwrap();
    client.execute(
        &format!("SELECT pg_create_logical_replication_slot('{slot}', 'pgoutput')"),
        &[],
    ).await.unwrap();
}

// ============================================================================
// Source builders
// ============================================================================

async fn make_mysql_source(
    id: &str,
    dsn: &str,
    db: &str,
    backend: ArcStorageBackend,
) -> MySqlSource {
    MySqlSource {
        id: id.into(),
        checkpoint_key: format!("mysql-{id}"),
        dsn: dsn.into(),
        tables: vec![format!("{db}.orders")],
        tenant: "acme".into(),
        pipeline: "test".into(),
        registry: make_registry().await,
        backend,
        outbox_tables: AllowList::default(),
        snapshot_cfg: SnapshotCfg::default(),
        on_schema_drift: deltaforge_config::OnSchemaDrift::Adapt,
    }
}

async fn make_pg_source(
    id: &str,
    dsn: &str,
    slot: &str,
    pub_name: &str,
    backend: ArcStorageBackend,
) -> PostgresSource {
    PostgresSource {
        id: id.into(),
        checkpoint_key: format!("pg-{id}"),
        dsn: dsn.into(),
        slot: slot.into(),
        publication: pub_name.into(),
        tables: vec!["public.orders".into()],
        tenant: "acme".into(),
        pipeline: "test".into(),
        registry: make_registry().await,
        backend,
        outbox_prefixes: AllowList::default(),
        snapshot_cfg: SnapshotCfg::default(),
        on_schema_drift: deltaforge_config::OnSchemaDrift::Adapt,
    }
}

// ============================================================================
// Helpers
// ============================================================================

async fn collect_until<F>(
    rx: &mut mpsc::Receiver<Event>,
    dur: Duration,
    mut cond: F,
) -> Vec<Event>
where
    F: FnMut(&[Event]) -> bool,
{
    let deadline = Instant::now() + dur;
    let mut events = Vec::new();
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match timeout(remaining, rx.recv()).await {
            Ok(Some(e)) => {
                events.push(e);
                if cond(&events) {
                    break;
                }
            }
            _ => break,
        }
    }
    events
}

fn has_id(e: &Event, id: i64) -> bool {
    [e.after.as_ref(), e.before.as_ref()]
        .into_iter()
        .flatten()
        .any(|v| v.get("id").and_then(|v| v.as_i64()) == Some(id))
}

// ============================================================================
// MySQL: identity change → reconciliation → streaming resumes
// ============================================================================

#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_failover_streaming_resumes_after_identity_change() -> Result<()>
{
    init_test_tracing();
    const DB: &str = "shop";

    let (_c_a, port_a) = start_mysql().await;
    mysql_create_schema(port_a, DB).await;

    let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);

    // run 1: against A - UUID_A naturally stored by check_identity_post_reconnect
    {
        let src = make_mysql_source(
            "fo",
            &mysql_cdc_dsn(port_a, DB),
            DB,
            Arc::clone(&backend),
        )
        .await;
        let (tx, mut rx) = mpsc::channel(64);
        let handle = src.run(tx, Arc::clone(&ckpt)).await;
        sleep(Duration::from_secs(4)).await;

        let pool = mysql_root_pool(port_a).await;
        let mut conn = pool.get_conn().await?;
        conn.query_drop(format!("USE {DB}")).await?;
        conn.query_drop("INSERT INTO orders VALUES (1, 'on-a')")
            .await?;
        let evts = collect_until(&mut rx, Duration::from_secs(10), |e| {
            e.iter().any(|x| has_id(x, 1))
        })
        .await;
        assert!(evts.iter().any(|e| has_id(e, 1)));

        handle.stop();
        handle.join().await.ok();
        info!("✓ Run 1 complete — UUID_A stored");
    }

    let uuid_a = mysql_fetch_uuid(port_a).await;

    // failover: B is a fresh promoted replica with gtid_purged covering A's range
    let (_c_b, port_b) = start_mysql().await;
    mysql_create_schema(port_b, DB).await;
    {
        let pool = mysql_root_pool(port_b).await;
        let mut conn = pool.get_conn().await?;
        conn.query_drop(format!("SET GLOBAL gtid_purged='{uuid_a}:1-100'"))
            .await?;
    }

    // run 2: same backend (UUID_A stored), new DSN (B)
    {
        let src = make_mysql_source(
            "fo",
            &mysql_cdc_dsn(port_b, DB),
            DB,
            Arc::clone(&backend),
        )
        .await;
        let (tx, mut rx) = mpsc::channel(64);
        let handle = src.run(tx, Arc::clone(&ckpt)).await;
        sleep(Duration::from_secs(5)).await;

        let uuid_b = mysql_fetch_uuid(port_b).await;
        let store = IdentityStore::new(Arc::clone(&backend));
        let cmp = store
            .compare(
                "fo",
                &ServerIdentity::MySql(mysql_health::MySqlServerIdentity {
                    server_uuid: uuid_b,
                }),
            )
            .await?;
        assert!(
            matches!(cmp, IdentityComparison::Same),
            "identity must be updated to B"
        );
        info!("✓ identity updated to B's UUID");

        let pool = mysql_root_pool(port_b).await;
        let mut conn = pool.get_conn().await?;
        conn.query_drop(format!("USE {DB}")).await?;
        conn.query_drop("INSERT INTO orders VALUES (2, 'on-b')")
            .await?;

        let evts = collect_until(&mut rx, Duration::from_secs(15), |e| {
            e.iter().any(|x| has_id(x, 2))
        })
        .await;
        assert!(
            evts.iter().any(|e| has_id(e, 2)),
            "must receive events after failover"
        );
        info!("✓ streaming resumed on B");

        handle.stop();
        handle.join().await.ok();
    }

    Ok(())
}

// ============================================================================
// MySQL: no GTID overlap after failover → position Lost → source stops
// ============================================================================

#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_failover_position_lost_stops_source() -> Result<()> {
    init_test_tracing();
    const DB: &str = "shop";

    let (_c_a, port_a) = start_mysql().await;
    mysql_create_schema(port_a, DB).await;

    let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);

    // run 1: checkpoint saved at A's binlog position
    {
        let src = make_mysql_source(
            "fo_lost",
            &mysql_cdc_dsn(port_a, DB),
            DB,
            Arc::clone(&backend),
        )
        .await;
        let (tx, mut rx) = mpsc::channel(64);
        let handle = src.run(tx, Arc::clone(&ckpt)).await;
        sleep(Duration::from_secs(4)).await;

        let pool = mysql_root_pool(port_a).await;
        let mut conn = pool.get_conn().await?;
        conn.query_drop(format!("USE {DB}")).await?;
        conn.query_drop("INSERT INTO orders VALUES (1, 'before-failover')")
            .await?;
        collect_until(&mut rx, Duration::from_secs(10), |e| {
            e.iter().any(|x| has_id(x, 1))
        })
        .await;

        handle.stop();
        handle.join().await.ok();
        info!("✓ checkpoint at A's position saved");
    }

    // primary B: fresh, NO gtid_purged - zero GTID overlap with A
    let (_c_b, port_b) = start_mysql().await;
    mysql_create_schema(port_b, DB).await;

    // run 2: Changed detected, position Lost -> must stop with error
    {
        let src = make_mysql_source(
            "fo_lost",
            &mysql_cdc_dsn(port_b, DB),
            DB,
            Arc::clone(&backend),
        )
        .await;
        let (tx, _rx) = mpsc::channel(64);
        let handle = src.run(tx, Arc::clone(&ckpt)).await;

        match timeout(Duration::from_secs(20), handle.join()).await {
            Ok(Err(e)) => {
                assert!(
                    e.to_string().contains("position lost")
                        || e.to_string().contains("Re-snapshot"),
                    "unexpected error: {e}"
                );
                info!("✓ source stopped with position-lost error");
            }
            Ok(Ok(())) => {
                panic!("source must not succeed when position is lost")
            }
            Err(_) => panic!("source did not stop within timeout"),
        }
    }

    Ok(())
}

// ============================================================================
// MySQL: column added on new primary → ColumnAdded delta recorded
// ============================================================================

#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_failover_schema_drift_detected() -> Result<()> {
    init_test_tracing();
    const DB: &str = "shop";

    let (_c_a, port_a) = start_mysql().await;
    mysql_create_schema(port_a, DB).await;

    let registry = make_registry().await;
    let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);

    // run 1: schema (id, sku) registered from A
    {
        let src = MySqlSource {
            id: "fo_drift".into(),
            checkpoint_key: "mysql-fo_drift".into(),
            dsn: mysql_cdc_dsn(port_a, DB),
            tables: vec![format!("{DB}.orders")],
            tenant: "acme".into(),
            pipeline: "test".into(),
            registry: Arc::clone(&registry),
            backend: Arc::clone(&backend),
            outbox_tables: AllowList::default(),
            snapshot_cfg: SnapshotCfg::default(),
            on_schema_drift: deltaforge_config::OnSchemaDrift::Adapt,
        };
        let (tx, mut rx) = mpsc::channel(64);
        let handle = src.run(tx, Arc::clone(&ckpt)).await;
        sleep(Duration::from_secs(4)).await;

        let pool = mysql_root_pool(port_a).await;
        let mut conn = pool.get_conn().await?;
        conn.query_drop(format!("USE {DB}")).await?;
        conn.query_drop("INSERT INTO orders VALUES (1, 'before')")
            .await?;
        collect_until(&mut rx, Duration::from_secs(10), |e| {
            e.iter().any(|x| has_id(x, 1))
        })
        .await;

        handle.stop();
        handle.join().await.ok();
        info!("✓ (id, sku) schema registered from A");
    }

    let uuid_a = mysql_fetch_uuid(port_a).await;

    // primary B: extra column 'status' added before DeltaForge reconnects
    let (_c_b, port_b) = start_mysql().await;
    mysql_create_schema(port_b, DB).await;
    {
        let pool = mysql_root_pool(port_b).await;
        let mut conn = pool.get_conn().await?;
        conn.query_drop(format!("USE {DB}")).await?;
        conn.query_drop("ALTER TABLE orders ADD COLUMN status VARCHAR(32)")
            .await?;
        conn.query_drop(format!("SET GLOBAL gtid_purged='{uuid_a}:1-100'"))
            .await?;
    }

    // run 2: reconciliation diffs registry (id,sku) vs live (id,sku,status)
    {
        let src = MySqlSource {
            id: "fo_drift".into(),
            checkpoint_key: "mysql-fo_drift".into(),
            dsn: mysql_cdc_dsn(port_b, DB),
            tables: vec![format!("{DB}.orders")],
            tenant: "acme".into(),
            pipeline: "test".into(),
            registry: Arc::clone(&registry),
            backend: Arc::clone(&backend),
            outbox_tables: AllowList::default(),
            snapshot_cfg: SnapshotCfg::default(),
            on_schema_drift: deltaforge_config::OnSchemaDrift::Adapt,
        };
        let (tx, mut rx) = mpsc::channel(64);
        let handle = src.run(tx, Arc::clone(&ckpt)).await;
        sleep(Duration::from_secs(6)).await;

        let pool = mysql_root_pool(port_b).await;
        let mut conn = pool.get_conn().await?;
        conn.query_drop(format!("USE {DB}")).await?;
        conn.query_drop("INSERT INTO orders VALUES (2, 'after', 'active')")
            .await?;

        let evts = collect_until(&mut rx, Duration::from_secs(15), |e| {
            e.iter().any(|x| has_id(x, 2))
        })
        .await;
        let ev = evts
            .iter()
            .find(|e| has_id(e, 2))
            .expect("must receive post-failover event");
        assert!(
            ev.after.as_ref().and_then(|v| v.get("status")).is_some(),
            "event must include 'status' after schema reload"
        );
        info!("✓ drifted column present in event");

        handle.stop();
        handle.join().await.ok();

        let uuid_b = mysql_fetch_uuid(port_b).await;
        let reconciler = SchemaReconciler::new(
            Arc::clone(&registry),
            Arc::clone(&backend),
            "acme",
        );
        let id_a = ServerIdentity::MySql(mysql_health::MySqlServerIdentity {
            server_uuid: uuid_a,
        });
        let id_b = ServerIdentity::MySql(mysql_health::MySqlServerIdentity {
            server_uuid: uuid_b,
        });
        let record = reconciler
            .already_completed("fo_drift", &id_a, &id_b)
            .await?
            .expect("reconciliation record must exist");

        let orders = record
            .table_results
            .iter()
            .find(|r| r.table == "orders")
            .expect("orders must appear in results");
        assert!(
            orders.deltas.iter().any(|d| matches!(
                d, SchemaDelta::ColumnAdded { column } if column.name == "status"
            )),
            "expected ColumnAdded(status), got: {:?}", orders.deltas
        );
        info!("✓ ColumnAdded delta recorded for 'status'");
    }

    Ok(())
}

// ============================================================================
// MySQL: on_schema_drift=halt + drift detected → source stops
// ============================================================================

#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_failover_schema_drift_halts_source() -> Result<()> {
    init_test_tracing();
    const DB: &str = "shop";

    let (_c_a, port_a) = start_mysql().await;
    mysql_create_schema(port_a, DB).await;

    let registry = make_registry().await;
    let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);

    // run 1: register A's schema (id, sku)
    {
        let src = MySqlSource {
            id: "fo_halt".into(),
            checkpoint_key: "mysql-fo_halt".into(),
            dsn: mysql_cdc_dsn(port_a, DB),
            tables: vec![format!("{DB}.orders")],
            tenant: "acme".into(),
            pipeline: "test".into(),
            registry: Arc::clone(&registry),
            backend: Arc::clone(&backend),
            outbox_tables: AllowList::default(),
            snapshot_cfg: SnapshotCfg::default(),
            on_schema_drift: deltaforge_config::OnSchemaDrift::Adapt,
        };
        let (tx, mut rx) = mpsc::channel(64);
        let handle = src.run(tx, Arc::clone(&ckpt)).await;
        sleep(Duration::from_secs(4)).await;

        let pool = mysql_root_pool(port_a).await;
        let mut conn = pool.get_conn().await?;
        conn.query_drop(format!("USE {DB}")).await?;
        conn.query_drop("INSERT INTO orders VALUES (1, 'before')")
            .await?;
        collect_until(&mut rx, Duration::from_secs(10), |e| {
            e.iter().any(|x| has_id(x, 1))
        })
        .await;

        handle.stop();
        handle.join().await.ok();
    }

    let uuid_a = mysql_fetch_uuid(port_a).await;

    // B: extra column added, simulating an un-synced replica schema
    let (_c_b, port_b) = start_mysql().await;
    mysql_create_schema(port_b, DB).await;
    {
        let pool = mysql_root_pool(port_b).await;
        let mut conn = pool.get_conn().await?;
        conn.query_drop(format!("USE {DB}")).await?;
        conn.query_drop("ALTER TABLE orders ADD COLUMN status VARCHAR(32)")
            .await?;
        conn.query_drop(format!("SET GLOBAL gtid_purged='{uuid_a}:1-100'"))
            .await?;
    }

    // run 2: on_schema_drift=halt → source must stop with drift error
    {
        let src = MySqlSource {
            id: "fo_halt".into(),
            checkpoint_key: "mysql-fo_halt".into(),
            dsn: mysql_cdc_dsn(port_b, DB),
            tables: vec![format!("{DB}.orders")],
            tenant: "acme".into(),
            pipeline: "test".into(),
            registry: Arc::clone(&registry),
            backend: Arc::clone(&backend),
            outbox_tables: AllowList::default(),
            snapshot_cfg: SnapshotCfg::default(),
            on_schema_drift: deltaforge_config::OnSchemaDrift::Halt,
        };
        let (tx, _rx) = mpsc::channel(64);
        let handle = src.run(tx, Arc::clone(&ckpt)).await;

        match timeout(Duration::from_secs(20), handle.join()).await {
            Ok(Err(e)) => {
                assert!(
                    e.to_string().contains("schema drift"),
                    "unexpected error: {e}"
                );
                info!("✓ source stopped with schema drift error");
            }
            Ok(Ok(())) => panic!(
                "source must not succeed when schema drift detected with halt policy"
            ),
            Err(_) => panic!("source did not stop within timeout"),
        }
    }

    Ok(())
}

// ============================================================================
// MySQL: on_schema_drift=halt + no drift → source continues normally
// ============================================================================

#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_failover_schema_drift_halt_no_drift_continues() -> Result<()> {
    init_test_tracing();
    const DB: &str = "shop";

    let (_c_a, port_a) = start_mysql().await;
    mysql_create_schema(port_a, DB).await;

    let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);

    // run 1: register A's schema
    {
        let src = make_mysql_source(
            "fo_halt_nodrift",
            &mysql_cdc_dsn(port_a, DB),
            DB,
            Arc::clone(&backend),
        )
        .await;
        let (tx, mut rx) = mpsc::channel(64);
        let handle = src.run(tx, Arc::clone(&ckpt)).await;
        sleep(Duration::from_secs(4)).await;

        let pool = mysql_root_pool(port_a).await;
        let mut conn = pool.get_conn().await?;
        conn.query_drop(format!("USE {DB}")).await?;
        conn.query_drop("INSERT INTO orders VALUES (1, 'on-a')")
            .await?;
        collect_until(&mut rx, Duration::from_secs(10), |e| {
            e.iter().any(|x| has_id(x, 1))
        })
        .await;

        handle.stop();
        handle.join().await.ok();
    }

    let uuid_a = mysql_fetch_uuid(port_a).await;

    // B: identical schema, GTID purged to simulate promoted replica
    let (_c_b, port_b) = start_mysql().await;
    mysql_create_schema(port_b, DB).await;
    {
        let pool = mysql_root_pool(port_b).await;
        let mut conn = pool.get_conn().await?;
        conn.query_drop(format!("SET GLOBAL gtid_purged='{uuid_a}:1-100'"))
            .await?;
    }

    // run 2: on_schema_drift=halt, but B has no drift → must stream normally
    {
        let src = MySqlSource {
            id: "fo_halt_nodrift".into(),
            checkpoint_key: "mysql-fo_halt_nodrift".into(),
            dsn: mysql_cdc_dsn(port_b, DB),
            tables: vec![format!("{DB}.orders")],
            tenant: "acme".into(),
            pipeline: "test".into(),
            registry: make_registry().await,
            backend: Arc::clone(&backend),
            outbox_tables: AllowList::default(),
            snapshot_cfg: SnapshotCfg::default(),
            on_schema_drift: deltaforge_config::OnSchemaDrift::Halt,
        };
        let (tx, mut rx) = mpsc::channel(64);
        let handle = src.run(tx, Arc::clone(&ckpt)).await;
        sleep(Duration::from_secs(5)).await;

        let pool = mysql_root_pool(port_b).await;
        let mut conn = pool.get_conn().await?;
        conn.query_drop(format!("USE {DB}")).await?;
        conn.query_drop("INSERT INTO orders VALUES (2, 'on-b')")
            .await?;

        let evts = collect_until(&mut rx, Duration::from_secs(15), |e| {
            e.iter().any(|x| has_id(x, 2))
        })
        .await;
        assert!(
            evts.iter().any(|e| has_id(e, 2)),
            "halt policy must not block streaming when schema is unchanged"
        );
        info!("✓ streaming continued with halt policy and no schema drift");

        handle.stop();
        handle.join().await.ok();
    }

    Ok(())
}

#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_failover_streaming_resumes_after_identity_change()
-> Result<()> {
    init_test_tracing();
    const DB: &str = "shop";
    const SLOT: &str = "slot_fo";
    const PUB: &str = "pub_fo";

    let (_c_a, port_a) = start_postgres().await;
    pg_create_schema(port_a, DB, SLOT, PUB).await;

    let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);

    // run 1: A's system_identifier stored
    {
        let src = make_pg_source(
            "fo",
            &pg_dsn(port_a, DB),
            SLOT,
            PUB,
            Arc::clone(&backend),
        )
        .await;
        let (tx, mut rx) = mpsc::channel(64);
        let handle = src.run(tx, Arc::clone(&ckpt)).await;
        sleep(Duration::from_secs(4)).await;

        let client = pg_admin_client(port_a, DB).await;
        client
            .execute("INSERT INTO orders VALUES (1, 'on-a')", &[])
            .await?;
        let evts = collect_until(&mut rx, Duration::from_secs(10), |e| {
            e.iter().any(|x| has_id(x, 1))
        })
        .await;
        assert!(evts.iter().any(|e| has_id(e, 1)));

        handle.stop();
        handle.join().await.ok();
        info!("✓ A's system_identifier stored");
    }

    // primary B: fresh container → different system_identifier by construction
    let (_c_b, port_b) = start_postgres().await;
    pg_create_schema(port_b, DB, SLOT, PUB).await;

    // run 2: Changed detected, reconciled, checkpoint reset to B's slot position,
    //          streaming on B
    {
        let src = make_pg_source(
            "fo",
            &pg_dsn(port_b, DB),
            SLOT,
            PUB,
            Arc::clone(&backend),
        )
        .await;
        let (tx, mut rx) = mpsc::channel(64);
        let handle = src.run(tx, Arc::clone(&ckpt)).await;
        sleep(Duration::from_secs(5)).await;

        let id_b = postgres_health::fetch_server_identity(&pg_dsn(port_b, DB))
            .await?
            .expect("system_identifier must exist");
        let store = IdentityStore::new(Arc::clone(&backend));
        let cmp = store.compare("fo", &ServerIdentity::Postgres(id_b)).await?;
        assert!(
            matches!(cmp, IdentityComparison::Same),
            "identity must be updated to B"
        );
        info!("✓ identity updated to B's system_identifier");

        let client = pg_admin_client(port_b, DB).await;
        client
            .execute("INSERT INTO orders VALUES (2, 'on-b')", &[])
            .await?;
        let evts = collect_until(&mut rx, Duration::from_secs(15), |e| {
            e.iter().any(|x| has_id(x, 2))
        })
        .await;
        assert!(
            evts.iter().any(|e| has_id(e, 2)),
            "must receive events from B"
        );
        info!("✓ streaming resumed on B");

        handle.stop();
        handle.join().await.ok();
    }

    Ok(())
}

// ============================================================================
// PostgreSQL: slot absent on new primary → source stops
// ============================================================================

#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_failover_slot_absent_stops_source() -> Result<()> {
    init_test_tracing();
    const DB: &str = "shop";
    const SLOT: &str = "slot_fo_lost";
    const PUB: &str = "pub_fo_lost";

    let (_c_a, port_a) = start_postgres().await;
    pg_create_schema(port_a, DB, SLOT, PUB).await;

    let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);

    // run 1: A's identity stored
    {
        let src = make_pg_source(
            "fo_lost",
            &pg_dsn(port_a, DB),
            SLOT,
            PUB,
            Arc::clone(&backend),
        )
        .await;
        let (tx, mut rx) = mpsc::channel(64);
        let handle = src.run(tx, Arc::clone(&ckpt)).await;
        sleep(Duration::from_secs(4)).await;

        let client = pg_admin_client(port_a, DB).await;
        client
            .execute("INSERT INTO orders VALUES (1, 'before')", &[])
            .await?;
        collect_until(&mut rx, Duration::from_secs(10), |e| {
            e.iter().any(|x| has_id(x, 1))
        })
        .await;

        handle.stop();
        handle.join().await.ok();
    }

    // primary B: publication present, replication slot absent
    let (_c_b, port_b) = start_postgres().await;
    {
        let root = pg_admin_client(port_b, "postgres").await;
        root.execute(&format!("CREATE DATABASE {DB}"), &[])
            .await
            .ok();
        drop(root);
        let client = pg_admin_client(port_b, DB).await;
        client
            .execute("CREATE TABLE orders (id INT PRIMARY KEY, sku TEXT)", &[])
            .await
            .unwrap();
        client
            .execute("ALTER TABLE orders REPLICA IDENTITY FULL", &[])
            .await
            .unwrap();
        client
            .execute(&format!("CREATE PUBLICATION {PUB} FOR TABLE orders"), &[])
            .await
            .unwrap();
        // No slot - represents a new primary where the slot was not preserved.
    }

    // run 2: Changed detected, slot absent → Lost → must stop
    {
        let src = make_pg_source(
            "fo_lost",
            &pg_dsn(port_b, DB),
            SLOT,
            PUB,
            Arc::clone(&backend),
        )
        .await;
        let (tx, _rx) = mpsc::channel(64);
        let handle = src.run(tx, Arc::clone(&ckpt)).await;

        match timeout(Duration::from_secs(30), handle.join()).await {
            Ok(Err(e)) => {
                assert!(
                    e.to_string().contains("position lost")
                        || e.to_string().contains("Re-snapshot"),
                    "unexpected error: {e}"
                );
                info!("✓ source stopped with position-lost error");
            }
            Ok(Ok(())) => panic!("source must not succeed when slot is absent"),
            Err(_) => panic!("source did not stop within timeout"),
        }
    }

    Ok(())
}
