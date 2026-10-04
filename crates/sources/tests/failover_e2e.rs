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
use deltaforge_config::{SnapshotCfg, SnapshotMode};
use deltaforge_core::{Event, Source, SourceError, SourceItem};
use mysql_async::prelude::Queryable;
use sources::failover::identity::{
    IdentityComparison, IdentityStore, ServerIdentity,
};
use sources::mysql::{MySqlSource, mysql_health};
use sources::postgres::{PostgresSource, postgres_health};
use sources::stream_probe::{reset_streams_opened, streams_opened};
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
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
    // No fixed sleep — provision_mysql_cdc_user retries until MySQL accepts TCP.
    provision_mysql_cdc_user(port).await;
    (c, port)
}

async fn provision_mysql_cdc_user(port: u16) {
    let dsn = format!("mysql://root:{MYSQL_ROOT_PASSWORD}@127.0.0.1:{port}/");
    let pool =
        mysql_async::Pool::new(mysql_async::Opts::from_url(&dsn).unwrap());
    // Retry connection — MySQL logs "ready for connections" before TCP is
    // fully accepting, so the container may look ready while connections fail.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut conn = loop {
        match pool.get_conn().await {
            Ok(c) => break c,
            Err(_) if Instant::now() < deadline => {
                sleep(Duration::from_millis(500)).await;
            }
            Err(e) => panic!("MySQL not ready after 30s on port {port}: {e}"),
        }
    };
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

/// `@@GLOBAL.gtid_executed` of the server on `port`: what a replica promoted
/// from it has executed.
async fn mysql_gtid_executed(port: u16) -> String {
    let pool = mysql_root_pool(port).await;
    let mut conn = pool.get_conn().await.unwrap();
    let set: String = conn
        .query_first("SELECT @@GLOBAL.gtid_executed")
        .await
        .unwrap()
        .unwrap();
    set.replace('\n', "")
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
        dsn: dsn.into(),
        tables: vec![format!("{db}.orders")],
        tenant: "acme".into(),
        pipeline: "test".into(),
        registry: make_registry().await,
        registry_scope: sources::registry_scope::SharedRegistryScope::default(),
        backend,
        outbox_tables: AllowList::default(),
        snapshot_cfg: SnapshotCfg::default(),
        on_schema_drift: deltaforge_config::OnSchemaDrift::Adapt,
        table_options: Default::default(),
        rotation: None,
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
        dsn: dsn.into(),
        slot: slot.into(),
        publication: pub_name.into(),
        tables: vec!["public.orders".into()],
        tenant: "acme".into(),
        pipeline: "test".into(),
        registry: make_registry().await,
        registry_scope: sources::registry_scope::SharedRegistryScope::default(),
        backend,
        outbox_prefixes: AllowList::default(),
        snapshot_cfg: SnapshotCfg::default(),
        on_schema_drift: deltaforge_config::OnSchemaDrift::Adapt,
        table_options: Default::default(),
        rotation: None,
    }
}

// ============================================================================
// Helpers
// ============================================================================

async fn collect_until<F>(
    rx: &mut mpsc::Receiver<SourceItem>,
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
            Ok(Some(SourceItem::Event(e))) => {
                events.push(e);
                if cond(&events) {
                    break;
                }
            }
            Ok(Some(SourceItem::TxBegin { .. })) => continue,
            Ok(Some(SourceItem::TxCommit { .. })) => continue,
            _ => break,
        }
    }
    events
}

/// The failover drift marker of `db.table` under the lineage of
/// `server_uuid`, if its check completed.
async fn drift_marker(
    backend: &ArcStorageBackend,
    source_id: &str,
    server_uuid: &str,
    db: &str,
    table: &str,
) -> Option<serde_json::Value> {
    let hash = storage::adapters::LineageDescriptor::mysql(server_uuid)
        .unwrap()
        .lineage_hash();
    let key =
        storage::adapters::SchemaKey::new("acme", source_id, &hash, db, table)
            .backend_key();
    backend
        .log_list("schemas.v1.failover.drift", &key)
        .await
        .unwrap()
        .into_iter()
        .map(|(_, b)| serde_json::from_slice(&b).unwrap())
        .next()
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

    // failover: B is a fresh promoted replica that executed exactly A's
    // transactions (gtid_purged = A's gtid_executed)
    let (_c_b, port_b) = start_mysql().await;
    mysql_create_schema(port_b, DB).await;
    {
        let pool = mysql_root_pool(port_b).await;
        let mut conn = pool.get_conn().await?;
        conn.query_drop(format!(
            "SET GLOBAL gtid_purged='{}'",
            mysql_gtid_executed(port_a).await
        ))
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
            dsn: mysql_cdc_dsn(port_a, DB).into(),
            tables: vec![format!("{DB}.orders")],
            tenant: "acme".into(),
            pipeline: "test".into(),
            registry: Arc::clone(&registry),
            registry_scope:
                sources::registry_scope::SharedRegistryScope::default(),
            backend: Arc::clone(&backend),
            outbox_tables: AllowList::default(),
            snapshot_cfg: SnapshotCfg::default(),
            on_schema_drift: deltaforge_config::OnSchemaDrift::Adapt,
            table_options: Default::default(),
            rotation: None,
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
        conn.query_drop(format!(
            "SET GLOBAL gtid_purged='{}'",
            mysql_gtid_executed(port_a).await
        ))
        .await?;
    }

    // run 2: reconciliation diffs registry (id,sku) vs live (id,sku,status)
    {
        let src = MySqlSource {
            id: "fo_drift".into(),
            dsn: mysql_cdc_dsn(port_b, DB).into(),
            tables: vec![format!("{DB}.orders")],
            tenant: "acme".into(),
            pipeline: "test".into(),
            registry: Arc::clone(&registry),
            registry_scope:
                sources::registry_scope::SharedRegistryScope::default(),
            backend: Arc::clone(&backend),
            outbox_tables: AllowList::default(),
            snapshot_cfg: SnapshotCfg::default(),
            on_schema_drift: deltaforge_config::OnSchemaDrift::Adapt,
            table_options: Default::default(),
            rotation: None,
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

        // Drift is checked lazily per table. B's own schema DDL follows the
        // failover position in its binlog, so the table's first event under
        // the new lineage is a DDL: its shape at the failover position is
        // unprovable, and under adapt that is recorded durably.
        let uuid_b = mysql_fetch_uuid(port_b).await;
        let _ = uuid_a;
        let marker = drift_marker(&backend, "fo_drift", &uuid_b, DB, "orders")
            .await
            .expect("the table's failover drift check is recorded");
        assert_eq!(marker["outcome"], "adapted_unprovable", "{marker}");
        assert!(marker["previous_schema_hash"].is_string(), "{marker}");
        info!("✓ adapted-unprovable drift outcome recorded");
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
            dsn: mysql_cdc_dsn(port_a, DB).into(),
            tables: vec![format!("{DB}.orders")],
            tenant: "acme".into(),
            pipeline: "test".into(),
            registry: Arc::clone(&registry),
            registry_scope:
                sources::registry_scope::SharedRegistryScope::default(),
            backend: Arc::clone(&backend),
            outbox_tables: AllowList::default(),
            snapshot_cfg: SnapshotCfg::default(),
            on_schema_drift: deltaforge_config::OnSchemaDrift::Adapt,
            table_options: Default::default(),
            rotation: None,
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

    // B: extra column added, simulating an un-synced replica schema
    let (_c_b, port_b) = start_mysql().await;
    mysql_create_schema(port_b, DB).await;
    {
        let pool = mysql_root_pool(port_b).await;
        let mut conn = pool.get_conn().await?;
        conn.query_drop(format!("USE {DB}")).await?;
        conn.query_drop("ALTER TABLE orders ADD COLUMN status VARCHAR(32)")
            .await?;
        conn.query_drop(format!(
            "SET GLOBAL gtid_purged='{}'",
            mysql_gtid_executed(port_a).await
        ))
        .await?;
    }

    // run 2: on_schema_drift=halt → source must stop with drift error
    {
        let src = MySqlSource {
            id: "fo_halt".into(),
            dsn: mysql_cdc_dsn(port_b, DB).into(),
            tables: vec![format!("{DB}.orders")],
            tenant: "acme".into(),
            pipeline: "test".into(),
            registry: Arc::clone(&registry),
            registry_scope:
                sources::registry_scope::SharedRegistryScope::default(),
            backend: Arc::clone(&backend),
            outbox_tables: AllowList::default(),
            snapshot_cfg: SnapshotCfg::default(),
            on_schema_drift: deltaforge_config::OnSchemaDrift::Halt,
            table_options: Default::default(),
            rotation: None,
        };
        let (tx, _rx) = mpsc::channel(64);
        let handle = src.run(tx, Arc::clone(&ckpt)).await;

        match timeout(Duration::from_secs(20), handle.join()).await {
            Ok(Err(e)) => {
                // The table's first event under the new lineage is B's own
                // DDL: its shape at the failover position cannot be proven,
                // which halt treats like drift.
                let msg = e.to_string();
                assert!(
                    msg.contains("cannot be proven")
                        && msg.contains("on_schema_drift=halt"),
                    "unexpected error: {e}"
                );
                info!("✓ source stopped before any write under halt");
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

    // B: identical schema, GTID purged to simulate promoted replica
    let (_c_b, port_b) = start_mysql().await;
    mysql_create_schema(port_b, DB).await;
    {
        let pool = mysql_root_pool(port_b).await;
        let mut conn = pool.get_conn().await?;
        conn.query_drop(format!(
            "SET GLOBAL gtid_purged='{}'",
            mysql_gtid_executed(port_a).await
        ))
        .await?;
    }

    // run 2: on_schema_drift=halt, but B has no drift → must stream normally
    {
        let src = MySqlSource {
            id: "fo_halt_nodrift".into(),
            dsn: mysql_cdc_dsn(port_b, DB).into(),
            tables: vec![format!("{DB}.orders")],
            tenant: "acme".into(),
            pipeline: "test".into(),
            registry: make_registry().await,
            registry_scope:
                sources::registry_scope::SharedRegistryScope::default(),
            backend: Arc::clone(&backend),
            outbox_tables: AllowList::default(),
            snapshot_cfg: SnapshotCfg::default(),
            on_schema_drift: deltaforge_config::OnSchemaDrift::Halt,
            table_options: Default::default(),
            rotation: None,
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

// ============================================================================
// PostgreSQL: another cluster is never resumed
//
// A checkpoint LSN and a slot position belong to one cluster's WAL history.
// Server B is an independently initialised cluster (another system_identifier)
// whose WAL and slot are pushed beyond A's checkpoint, so every numeric LSN
// check would pass. The source must stop before START_REPLICATION, a snapshot,
// or any identity, lineage, checkpoint or slot change.
// ============================================================================

fn parse_lsn(s: &str) -> u64 {
    let (hi, lo) = s.split_once('/').expect("lsn");
    (u64::from_str_radix(hi, 16).unwrap() << 32)
        | u64::from_str_radix(lo, 16).unwrap()
}

/// The LSN of a stored PostgreSQL checkpoint.
fn checkpoint_lsn(raw: &[u8]) -> u64 {
    let v: serde_json::Value = serde_json::from_slice(raw).unwrap();
    parse_lsn(v["lsn"].as_str().expect("checkpoint lsn"))
}

/// `(restart_lsn, confirmed_flush_lsn)` of `slot` on the server at `port`.
async fn slot_position(port: u16, db: &str, slot: &str) -> Option<(u64, u64)> {
    pg_admin_client(port, db)
        .await
        .query_opt(
            "SELECT restart_lsn::text, confirmed_flush_lsn::text \
             FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .unwrap()
        .map(|r| (parse_lsn(r.get(0)), parse_lsn(r.get(1))))
}

/// Push the WAL of the server at `port` beyond `lsn`, then create `slot`
/// there: its restart and confirmed positions both lie beyond `lsn`.
async fn slot_beyond(port: u16, db: &str, slot: &str, lsn: u64) {
    let c = pg_admin_client(port, db).await;
    c.execute("CREATE TABLE IF NOT EXISTS wal_filler (n INT)", &[])
        .await
        .unwrap();
    loop {
        let r = c
            .query_one("SELECT pg_current_wal_lsn()::text", &[])
            .await
            .unwrap();
        if parse_lsn(r.get(0)) > lsn {
            break;
        }
        c.execute("INSERT INTO wal_filler VALUES (1)", &[])
            .await
            .unwrap();
        c.execute("SELECT pg_switch_wal()", &[]).await.unwrap();
    }
    c.execute(
        &format!(
            "SELECT pg_create_logical_replication_slot('{slot}', 'pgoutput')"
        ),
        &[],
    )
    .await
    .unwrap();
    let (restart, confirmed) = slot_position(port, db, slot).await.unwrap();
    assert!(
        restart > lsn && confirmed > lsn,
        "B's slot is beyond A's LSN"
    );
}

/// What a refused start must leave untouched.
struct Untouched {
    checkpoint: Option<Vec<u8>>,
    lineage: serde_json::Value,
    identity: ServerIdentity,
}

impl Untouched {
    async fn capture(
        backend: &ArcStorageBackend,
        ckpt: &Arc<dyn CheckpointStore>,
        id: &str,
        port: u16,
    ) -> Self {
        let lineage =
            storage::adapters::source_lineage::load(backend, "acme", id)
                .await
                .unwrap()
                .expect("lineage recorded");
        Self {
            checkpoint: ckpt.get_raw(id).await.unwrap(),
            lineage: serde_json::to_value(lineage).unwrap(),
            identity: ServerIdentity::Postgres(
                postgres_health::fetch_server_identity(&pg_dsn(port, "shop"))
                    .await
                    .unwrap()
                    .unwrap(),
            ),
        }
    }

    async fn assert_kept(
        &self,
        backend: &ArcStorageBackend,
        ckpt: &Arc<dyn CheckpointStore>,
        id: &str,
    ) {
        assert_eq!(
            ckpt.get_raw(id).await.unwrap(),
            self.checkpoint,
            "checkpoint changed"
        );
        let lineage =
            storage::adapters::source_lineage::load(backend, "acme", id)
                .await
                .unwrap()
                .expect("lineage recorded");
        assert_eq!(
            serde_json::to_value(lineage).unwrap(),
            self.lineage,
            "lineage record changed"
        );
        assert!(
            matches!(
                IdentityStore::new(Arc::clone(backend))
                    .compare(id, &self.identity)
                    .await
                    .unwrap(),
                IdentityComparison::Same
            ),
            "recorded identity changed"
        );
    }
}

/// The run's terminal error, which must be the typed lineage refusal.
async fn refused(handle: deltaforge_core::SourceHandle) {
    match timeout(Duration::from_secs(60), handle.join()).await {
        Ok(Err(e)) => assert!(
            matches!(
                e.downcast_ref::<SourceError>(),
                Some(SourceError::Lineage { .. })
            ),
            "another cluster must be refused with a lineage error: {e:?}"
        ),
        Ok(Ok(())) => panic!("the source must not run on another cluster"),
        Err(_) => panic!("the source did not stop"),
    }
}

async fn no_events(rx: &mut mpsc::Receiver<SourceItem>) {
    let evts = collect_until(rx, Duration::from_secs(2), |_| false).await;
    assert!(evts.is_empty(), "no event from another cluster: {evts:?}");
}

#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_another_cluster_is_refused_before_anything() -> Result<()> {
    init_test_tracing();
    const DB: &str = "shop";
    const SLOT: &str = "slot_fo";
    const PUB: &str = "pub_fo";
    const ID: &str = "fo";

    let (_c_a, port_a) = start_postgres().await;
    pg_create_schema(port_a, DB, SLOT, PUB).await;

    let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);

    // Positive control for the stream-open seam: a normal run increments it,
    // so a zero reading below is meaningful.
    reset_streams_opened();

    // Run 1 on A: identity, lineage and a checkpoint F recorded.
    {
        let src = make_pg_source(
            ID,
            &pg_dsn(port_a, DB),
            SLOT,
            PUB,
            Arc::clone(&backend),
        )
        .await;
        let (tx, mut rx) = mpsc::channel(64);
        let handle = src.run(tx, Arc::clone(&ckpt)).await;
        sleep(Duration::from_secs(4)).await;
        pg_admin_client(port_a, DB)
            .await
            .execute("INSERT INTO orders VALUES (1, 'on-a')", &[])
            .await?;
        let evts = collect_until(&mut rx, Duration::from_secs(10), |e| {
            e.iter().any(|x| has_id(x, 1))
        })
        .await;
        assert!(evts.iter().any(|e| has_id(e, 1)));
        handle.stop();
        handle.join().await.ok();
    }
    assert!(streams_opened() >= 1, "positive control");
    let kept = Untouched::capture(&backend, &ckpt, ID, port_a).await;
    let f = checkpoint_lsn(kept.checkpoint.as_deref().expect("checkpoint F"));

    // B: another cluster, same database, publication and slot name, with WAL
    // and slot positions beyond F; a row is waiting there.
    let (_c_b, port_b) = start_postgres().await;
    pg_create_schema(port_b, DB, "slot_unused", PUB).await;
    slot_beyond(port_b, DB, SLOT, f).await;
    pg_admin_client(port_b, DB)
        .await
        .execute("INSERT INTO orders VALUES (2, 'on-b')", &[])
        .await?;
    let b_slot = slot_position(port_b, DB, SLOT).await;

    // Resume from F on B: refused before any stream opens.
    reset_streams_opened();
    {
        let src = make_pg_source(
            ID,
            &pg_dsn(port_b, DB),
            SLOT,
            PUB,
            Arc::clone(&backend),
        )
        .await;
        let (tx, mut rx) = mpsc::channel(64);
        refused(src.run(tx, Arc::clone(&ckpt)).await).await;
        no_events(&mut rx).await;
    }

    // A snapshot start on B (no checkpoint, a slot it would create): refused
    // before the snapshot reads a row or creates the slot.
    let fresh: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    {
        let mut src = make_pg_source(
            ID,
            &pg_dsn(port_b, DB),
            "slot_snap",
            PUB,
            Arc::clone(&backend),
        )
        .await;
        src.snapshot_cfg.mode = SnapshotMode::Initial;
        let (tx, mut rx) = mpsc::channel(64);
        refused(src.run(tx, Arc::clone(&fresh)).await).await;
        no_events(&mut rx).await;
    }

    assert_eq!(streams_opened(), 0, "no replication stream opened on B");
    assert_eq!(
        slot_position(port_b, DB, SLOT).await,
        b_slot,
        "B's slot never moved"
    );
    assert!(
        slot_position(port_b, DB, "slot_snap").await.is_none(),
        "no slot created on B"
    );
    assert!(fresh.get_raw(ID).await?.is_none(), "no snapshot checkpoint");
    kept.assert_kept(&backend, &ckpt, ID).await;
    Ok(())
}

/// A TCP proxy standing in for a failover endpoint (DNS, VIP, load
/// balancer): every new connection goes to the current target.
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

    /// Send new connections to `to` and drop every open one.
    fn switch_to(&self, to: u16) {
        self.to.store(to, Ordering::SeqCst);
        for t in self.live.lock().unwrap().drain(..) {
            t.abort();
        }
    }
}

/// The endpoint moves to another cluster while the source runs: the reconnect
/// is refused before START_REPLICATION reaches it.
#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_a_reconnect_to_another_cluster_is_refused() -> Result<()> {
    init_test_tracing();
    const DB: &str = "shop";
    const SLOT: &str = "slot_rc";
    const PUB: &str = "pub_rc";
    const ID: &str = "rc";

    let (_c_a, port_a) = start_postgres().await;
    pg_create_schema(port_a, DB, SLOT, PUB).await;
    let (_c_b, port_b) = start_postgres().await;
    pg_create_schema(port_b, DB, "slot_unused", PUB).await;

    let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let proxy = Proxy::start(port_a).await;

    let src = make_pg_source(
        ID,
        &pg_dsn(proxy.port, DB),
        SLOT,
        PUB,
        Arc::clone(&backend),
    )
    .await;
    let (tx, mut rx) = mpsc::channel(64);
    let handle = src.run(tx, Arc::clone(&ckpt)).await;
    sleep(Duration::from_secs(4)).await;
    pg_admin_client(port_a, DB)
        .await
        .execute("INSERT INTO orders VALUES (1, 'on-a')", &[])
        .await?;
    let evts = collect_until(&mut rx, Duration::from_secs(10), |e| {
        e.iter().any(|x| has_id(x, 1))
    })
    .await;
    assert!(evts.iter().any(|e| has_id(e, 1)));
    let kept = Untouched::capture(&backend, &ckpt, ID, port_a).await;
    // Any position the source holds is at or before A's current WAL end.
    let f = parse_lsn(
        pg_admin_client(port_a, DB)
            .await
            .query_one("SELECT pg_current_wal_lsn()::text", &[])
            .await?
            .get(0),
    );

    // B looks like a perfect continuation: same names, slot beyond F.
    slot_beyond(port_b, DB, SLOT, f).await;
    pg_admin_client(port_b, DB)
        .await
        .execute("INSERT INTO orders VALUES (2, 'on-b')", &[])
        .await?;
    let b_slot = slot_position(port_b, DB, SLOT).await;

    reset_streams_opened();
    proxy.switch_to(port_b);
    refused(handle).await;
    no_events(&mut rx).await;

    assert_eq!(streams_opened(), 0, "no replication stream opened on B");
    assert_eq!(
        slot_position(port_b, DB, SLOT).await,
        b_slot,
        "B's slot never moved"
    );
    kept.assert_kept(&backend, &ckpt, ID).await;
    Ok(())
}

// ============================================================================
// R3-C2: startup identity-persistence failure must open ZERO replication streams
//
// The FirstSeen identity must be persisted BEFORE the source opens replication.
// These tests inject a backend whose identity-namespace writes fail, run the
// source against a fresh server (FirstSeen), and assert the source stops with the
// identity error AND that the real connect-path counter (stream_probe) recorded
// zero opens - proving no stream was ever opened, not merely opened-then-dropped
// before a post-mortem query. Covers PostgreSQL, MySQL GTID mode, and MySQL
// non-GTID (file/pos) mode.
// ============================================================================

/// A backend that fails writes to the failover/identity namespace, delegating
/// everything else to a real in-memory backend. Simulates a durable identity
/// store that cannot persist, so a FirstSeen write fails while snapshot,
/// checkpoint, and registry writes still succeed.
#[derive(Debug)]
struct FailIdentityWrites {
    inner: ArcStorageBackend,
}

impl FailIdentityWrites {
    fn new() -> Self {
        Self {
            inner: Arc::new(MemoryStorageBackend::new()),
        }
    }
}

#[async_trait::async_trait]
impl storage::StorageBackend for FailIdentityWrites {
    async fn kv_get(
        &self,
        ns: &str,
        key: &str,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        self.inner.kv_get(ns, key).await
    }
    async fn kv_put(
        &self,
        ns: &str,
        key: &str,
        value: &[u8],
    ) -> anyhow::Result<()> {
        if ns == "failover" {
            return Err(anyhow::anyhow!("injected identity write failure"));
        }
        self.inner.kv_put(ns, key, value).await
    }
    async fn kv_put_with_ttl(
        &self,
        ns: &str,
        key: &str,
        value: &[u8],
        ttl_secs: u64,
    ) -> anyhow::Result<()> {
        self.inner.kv_put_with_ttl(ns, key, value, ttl_secs).await
    }
    async fn kv_delete(&self, ns: &str, key: &str) -> anyhow::Result<bool> {
        self.inner.kv_delete(ns, key).await
    }
    async fn kv_list(
        &self,
        ns: &str,
        prefix: Option<&str>,
    ) -> anyhow::Result<Vec<String>> {
        self.inner.kv_list(ns, prefix).await
    }
    async fn log_append(
        &self,
        ns: &str,
        key: &str,
        value: &[u8],
    ) -> anyhow::Result<u64> {
        self.inner.log_append(ns, key, value).await
    }
    async fn log_list(
        &self,
        ns: &str,
        key: &str,
    ) -> anyhow::Result<Vec<(u64, Vec<u8>)>> {
        self.inner.log_list(ns, key).await
    }
    async fn log_since(
        &self,
        ns: &str,
        key: &str,
        since_seq: u64,
    ) -> anyhow::Result<Vec<(u64, Vec<u8>)>> {
        self.inner.log_since(ns, key, since_seq).await
    }
    async fn log_latest(
        &self,
        ns: &str,
        key: &str,
    ) -> anyhow::Result<Option<(u64, Vec<u8>)>> {
        self.inner.log_latest(ns, key).await
    }
    async fn log_ns_max_seq(&self, ns: &str) -> anyhow::Result<u64> {
        self.inner.log_ns_max_seq(ns).await
    }
    async fn log_append_if_absent(
        &self,
        ns: &str,
        key: &str,
        capture_id: &str,
        value: &[u8],
    ) -> anyhow::Result<storage::LogAppendOutcome> {
        self.inner
            .log_append_if_absent(ns, key, capture_id, value)
            .await
    }
    async fn log_truncate(
        &self,
        ns: &str,
        key: &str,
        req: storage::LogTruncateRequest,
    ) -> anyhow::Result<storage::LogTruncateOutcome> {
        self.inner.log_truncate(ns, key, req).await
    }
    async fn log_stream_meta(
        &self,
        ns: &str,
        key: &str,
    ) -> anyhow::Result<storage::LogStreamMeta> {
        self.inner.log_stream_meta(ns, key).await
    }
    async fn log_read_meta_since(
        &self,
        ns: &str,
        key: &str,
        since_seq: u64,
        limit: usize,
    ) -> anyhow::Result<Vec<storage::LogEntryMeta>> {
        self.inner
            .log_read_meta_since(ns, key, since_seq, limit)
            .await
    }
    async fn slot_upsert(
        &self,
        ns: &str,
        key: &str,
        state: &[u8],
    ) -> anyhow::Result<u64> {
        self.inner.slot_upsert(ns, key, state).await
    }
    async fn slot_get(
        &self,
        ns: &str,
        key: &str,
    ) -> anyhow::Result<Option<(u64, Vec<u8>)>> {
        self.inner.slot_get(ns, key).await
    }
    async fn slot_cas(
        &self,
        ns: &str,
        key: &str,
        expected_version: u64,
        state: &[u8],
    ) -> anyhow::Result<bool> {
        self.inner.slot_cas(ns, key, expected_version, state).await
    }
    async fn slot_create(
        &self,
        ns: &str,
        key: &str,
        state: &[u8],
    ) -> anyhow::Result<Option<u64>> {
        self.inner.slot_create(ns, key, state).await
    }
    async fn slot_delete(&self, ns: &str, key: &str) -> anyhow::Result<bool> {
        self.inner.slot_delete(ns, key).await
    }
    async fn slot_list(
        &self,
        ns: &str,
        prefix: Option<&str>,
        cursor: Option<&str>,
        limit: usize,
    ) -> anyhow::Result<storage::SlotPage> {
        self.inner.slot_list(ns, prefix, cursor, limit).await
    }
    async fn queue_push(
        &self,
        ns: &str,
        key: &str,
        value: &[u8],
    ) -> anyhow::Result<u64> {
        self.inner.queue_push(ns, key, value).await
    }
    async fn queue_peek(
        &self,
        ns: &str,
        key: &str,
        limit: usize,
    ) -> anyhow::Result<Vec<(u64, Vec<u8>)>> {
        self.inner.queue_peek(ns, key, limit).await
    }
    async fn queue_ack(
        &self,
        ns: &str,
        key: &str,
        up_to_id: u64,
    ) -> anyhow::Result<usize> {
        self.inner.queue_ack(ns, key, up_to_id).await
    }
    async fn queue_len(&self, ns: &str, key: &str) -> anyhow::Result<u64> {
        self.inner.queue_len(ns, key).await
    }
    async fn queue_drop_oldest(
        &self,
        ns: &str,
        key: &str,
        count: usize,
    ) -> anyhow::Result<usize> {
        self.inner.queue_drop_oldest(ns, key, count).await
    }
}

/// MySQL 8.4 container in non-GTID (file/pos) mode.
async fn start_mysql_nongtid() -> (ContainerAsync<GenericImage>, u16) {
    let c = GenericImage::new("mysql", "8.4")
        .with_wait_for(WaitFor::message_on_stderr("ready for connections"))
        .with_env_var("MYSQL_ROOT_PASSWORD", MYSQL_ROOT_PASSWORD)
        .with_cmd([
            "--server-id=1",
            "--log-bin=mysql-bin",
            "--binlog-format=ROW",
            "--binlog-row-image=FULL",
            "--binlog-checksum=NONE",
        ])
        .start()
        .await
        .expect("start mysql (non-gtid)");
    let port = c.get_host_port_ipv4(3306).await.expect("mysql port");
    provision_mysql_cdc_user(port).await;
    (c, port)
}

#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_startup_identity_persist_failure_opens_no_stream()
-> Result<()> {
    init_test_tracing();
    const DB: &str = "shop";
    const SLOT: &str = "slot_no_open";
    const PUB: &str = "pub_no_open";

    let (_c, port) = start_postgres().await;
    pg_create_schema(port, DB, SLOT, PUB).await;

    // Fresh backend -> FirstSeen; identity writes fail.
    let backend: ArcStorageBackend = Arc::new(FailIdentityWrites::new());
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);

    let src =
        make_pg_source("no_open", &pg_dsn(port, DB), SLOT, PUB, backend).await;
    let (tx, _rx) = mpsc::channel(64);
    reset_streams_opened();
    let handle = src.run(tx, ckpt).await;

    match timeout(Duration::from_secs(30), handle.join()).await {
        Ok(Err(e)) => {
            assert!(
                e.to_string().contains("IdentityStore")
                    || e.to_string().contains("identity"),
                "expected an identity-persistence error, got: {e}"
            );
            info!("✓ pg source stopped on identity-persist failure");
        }
        Ok(Ok(())) => {
            panic!("source must not succeed when identity persistence fails")
        }
        Err(_) => panic!("source did not stop within timeout"),
    }

    // Deterministic: the real connect path increments this counter on every
    // successful open. It must be zero - the stream was never opened, not merely
    // opened-then-dropped-before-a-post-mortem-query.
    assert_eq!(
        streams_opened(),
        0,
        "no replication stream must ever be opened when identity persist fails"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_startup_identity_persist_failure_opens_no_stream() -> Result<()>
{
    init_test_tracing();
    const DB: &str = "shop";

    let (_c, port) = start_mysql().await;
    mysql_create_schema(port, DB).await;

    let backend: ArcStorageBackend = Arc::new(FailIdentityWrites::new());
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);

    let src =
        make_mysql_source("no_open", &mysql_cdc_dsn(port, DB), DB, backend)
            .await;
    let (tx, _rx) = mpsc::channel(64);
    reset_streams_opened();
    let handle = src.run(tx, ckpt).await;

    match timeout(Duration::from_secs(30), handle.join()).await {
        Ok(Err(e)) => {
            assert!(
                e.to_string().contains("IdentityStore")
                    || e.to_string().contains("identity"),
                "expected an identity-persistence error, got: {e}"
            );
            info!("✓ mysql (gtid) source stopped on identity-persist failure");
        }
        Ok(Ok(())) => {
            panic!("source must not succeed when identity persistence fails")
        }
        Err(_) => panic!("source did not stop within timeout"),
    }

    assert_eq!(
        streams_opened(),
        0,
        "no binlog stream must ever be opened when identity persist fails"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_nongtid_startup_identity_persist_failure_opens_no_stream()
-> Result<()> {
    init_test_tracing();
    const DB: &str = "shop";

    // Non-GTID (file/pos) mode: previously the startup path performed NO identity
    // comparison before opening the stream. It must now fail closed too.
    let (_c, port) = start_mysql_nongtid().await;
    mysql_create_schema(port, DB).await;

    let backend: ArcStorageBackend = Arc::new(FailIdentityWrites::new());
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);

    let src =
        make_mysql_source("no_open", &mysql_cdc_dsn(port, DB), DB, backend)
            .await;
    let (tx, _rx) = mpsc::channel(64);
    reset_streams_opened();
    let handle = src.run(tx, ckpt).await;

    match timeout(Duration::from_secs(30), handle.join()).await {
        Ok(Err(e)) => {
            assert!(
                e.to_string().contains("IdentityStore")
                    || e.to_string().contains("identity"),
                "expected an identity-persistence error, got: {e}"
            );
            info!(
                "✓ mysql (non-gtid) source stopped on identity-persist failure"
            );
        }
        Ok(Ok(())) => {
            panic!("source must not succeed when identity persistence fails")
        }
        Err(_) => panic!("source did not stop within timeout"),
    }

    assert_eq!(
        streams_opened(),
        0,
        "no binlog stream must ever be opened when identity persist fails"
    );
    Ok(())
}

/// A port with nothing listening: connections to it are refused.
async fn dead_port() -> u16 {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    l.local_addr().unwrap().port()
}

/// A source item reduced to what the protocol checks need.
#[derive(Debug)]
enum Seen {
    Begin(String),
    Row(i64),
    Commit(String),
    Abort(String),
}

/// What has been received so far.
#[derive(Default)]
struct Received {
    seen: Vec<Seen>,
    rows: usize,
    commits: usize,
}

/// Receive items until `done` holds or `dur` passes.
async fn recv_until(
    rx: &mut mpsc::Receiver<SourceItem>,
    r: &mut Received,
    dur: Duration,
    done: impl Fn(&Received) -> bool,
) {
    let deadline = Instant::now() + dur;
    while !done(r) {
        let left = deadline.saturating_duration_since(Instant::now());
        let Ok(Some(item)) = timeout(left, rx.recv()).await else {
            return;
        };
        match item {
            SourceItem::TxBegin { tx_id } => r.seen.push(Seen::Begin(tx_id)),
            SourceItem::Event(e) => {
                let id = e
                    .after
                    .as_ref()
                    .and_then(|v| v.get("id"))
                    .and_then(|v| v.as_i64())
                    .expect("row id");
                r.rows += 1;
                r.seen.push(Seen::Row(id));
            }
            SourceItem::TxCommit { tx_id, .. } => {
                r.commits += 1;
                r.seen.push(Seen::Commit(tx_id));
            }
            SourceItem::TxAbort { tx_id } => r.seen.push(Seen::Abort(tx_id)),
            SourceItem::Boundary { .. } => {}
        }
    }
}

/// One transaction attempt on the source stream: its id, its rows in order,
/// and whether it ended in a commit (true) or an abort (false).
type Attempt = (String, Vec<i64>, bool);

/// Split `items` into transaction attempts, checking the protocol: every row
/// belongs to an open transaction, and each attempt ends in its own commit or
/// abort marker.
fn attempts(seen: &[Seen]) -> Vec<Attempt> {
    let mut out = Vec::new();
    let mut open: Option<(String, Vec<i64>)> = None;
    for i in seen {
        match i {
            Seen::Begin(tx_id) => {
                assert!(
                    open.is_none(),
                    "begin of {tx_id} inside a transaction"
                );
                open = Some((tx_id.clone(), Vec::new()));
            }
            Seen::Row(id) => open
                .as_mut()
                .expect("a row outside a transaction")
                .1
                .push(*id),
            Seen::Commit(tx_id) | Seen::Abort(tx_id) => {
                let (id, rows) =
                    open.take().expect("a marker outside a transaction");
                assert_eq!(&id, tx_id, "the marker names the open transaction");
                out.push((id, rows, matches!(i, Seen::Commit(_))));
            }
        }
    }
    out
}

/// Every expected transaction committed exactly once with all its rows in
/// order, and every abandoned attempt a prefix of its transaction's replay.
fn assert_each_committed_once(attempts: &[Attempt], expected: &[Vec<i64>]) {
    let committed: Vec<&Attempt> = attempts.iter().filter(|a| a.2).collect();
    assert_eq!(
        committed.iter().map(|a| a.1.clone()).collect::<Vec<_>>(),
        expected,
        "each transaction committed once, rows in order, nothing lost"
    );
    for (id, rows, done) in attempts {
        if !done {
            let replay = committed
                .iter()
                .find(|c| &c.0 == id)
                .expect("an abandoned transaction is replayed");
            assert!(
                replay.1.starts_with(rows),
                "the abandoned attempt is a prefix of its replay"
            );
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Cut {
    /// Inside a large transaction.
    Inside,
    /// After the first transaction's last row, before its commit marker is read.
    BeforeCommit,
    /// Right after the first transaction's commit marker is read.
    AfterCommit,
}

/// The stream is closed cleanly by the peer (a proxy dropping it) at `cut`.
/// The source reconnects on its own server, resuming at the end of the last
/// transaction it handed on: a transaction cut off is abandoned (TxAbort) and
/// sent again from its BEGIN, a handed-on one is never sent again, and nothing
/// is lost.
async fn stream_closed_at(cut: Cut) -> Result<()> {
    const DB: &str = "shop";
    // Larger than what the replication client buffers ahead (8192 events)
    // plus the socket buffers, so a cut lands inside it.
    const BIG: i64 = 200_000;

    let (_c, port) = start_postgres().await;
    pg_create_schema(port, DB, "slot_cut", "pub_cut").await;
    let proxy = Proxy::start(port).await;
    let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let src = make_pg_source(
        "cut",
        &pg_dsn(proxy.port, DB),
        "slot_cut",
        "pub_cut",
        Arc::clone(&backend),
    )
    .await;
    // One slot: the source hands on one item at a time, so the cut lands
    // where the test stops reading.
    let (tx, mut rx) = mpsc::channel(1);
    let handle = src.run(tx, Arc::clone(&ckpt)).await;
    sleep(Duration::from_secs(4)).await;

    let admin = pg_admin_client(port, DB).await;
    admin
        .execute(
            "INSERT INTO orders SELECT g, 'small' FROM generate_series(1, 3) g",
            &[],
        )
        .await?;
    admin
        .execute(
            &format!(
                "INSERT INTO orders SELECT g, 'big' FROM generate_series(101, {}) g",
                100 + BIG
            ),
            &[],
        )
        .await?;

    let mut r = Received::default();
    recv_until(&mut rx, &mut r, Duration::from_secs(30), |r| match cut {
        Cut::Inside => r.rows >= 200,
        Cut::BeforeCommit => r.rows >= 3,
        Cut::AfterCommit => r.commits >= 1,
    })
    .await;
    proxy.switch_to(port);
    admin
        .execute("INSERT INTO orders VALUES (900000, 'after')", &[])
        .await?;
    recv_until(&mut rx, &mut r, Duration::from_secs(300), |r| {
        r.commits >= 3
    })
    .await;

    let all = attempts(&r.seen);
    assert_each_committed_once(
        &all,
        &[(1..=3).collect(), (101..=100 + BIG).collect(), vec![900000]],
    );
    if matches!(cut, Cut::Inside) {
        assert!(
            all.iter().any(|a| !a.2),
            "the transaction cut inside was abandoned and replayed"
        );
    }
    assert!(!handle.join.is_finished(), "the source keeps running");
    handle.stop();
    handle.join().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_a_stream_closed_inside_a_transaction_replays_it_once()
-> Result<()> {
    init_test_tracing();
    stream_closed_at(Cut::Inside).await
}

#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_a_stream_closed_before_a_commit_loses_nothing() -> Result<()>
{
    init_test_tracing();
    stream_closed_at(Cut::BeforeCommit).await
}

#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_a_stream_closed_after_a_commit_loses_nothing() -> Result<()> {
    init_test_tracing();
    stream_closed_at(Cut::AfterCommit).await
}

/// The endpoint is unreachable for longer than the server-verification
/// retries: the reconnect keeps backing off (it never fails the source and
/// never streams unverified), resumes when the same server returns, and a stop
/// during a later outage ends the source promptly.
#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_a_reconnect_outlasts_an_outage_and_stops_on_cancel()
-> Result<()> {
    init_test_tracing();
    const DB: &str = "shop";

    let (_c, port) = start_postgres().await;
    pg_create_schema(port, DB, "slot_out", "pub_out").await;
    let proxy = Proxy::start(port).await;
    let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let src = make_pg_source(
        "out",
        &pg_dsn(proxy.port, DB),
        "slot_out",
        "pub_out",
        Arc::clone(&backend),
    )
    .await;
    let (tx, mut rx) = mpsc::channel(64);
    let handle = src.run(tx, Arc::clone(&ckpt)).await;
    sleep(Duration::from_secs(4)).await;
    let admin = pg_admin_client(port, DB).await;

    let dead = dead_port().await;
    proxy.switch_to(dead);
    admin
        .execute("INSERT INTO orders VALUES (1, 'during')", &[])
        .await?;
    sleep(Duration::from_secs(20)).await;
    assert!(
        !handle.join.is_finished(),
        "an outage must not fail the source"
    );

    proxy.switch_to(port);
    let evts = collect_until(&mut rx, Duration::from_secs(90), |e| {
        e.iter().any(|x| has_id(x, 1))
    })
    .await;
    assert!(
        evts.iter().any(|e| has_id(e, 1)),
        "resumed after the outage"
    );

    proxy.switch_to(dead);
    sleep(Duration::from_secs(3)).await;
    handle.stop();
    let joined = timeout(Duration::from_secs(10), handle.join())
        .await
        .expect("a stop during an outage ends the source promptly");
    if let Err(e) = &joined {
        assert!(
            matches!(
                e.downcast_ref::<SourceError>(),
                Some(SourceError::Cancelled)
            ),
            "a stop is not a failure: {e:?}"
        );
    }
    Ok(())
}

/// A stop while the source is inside a transaction is a shutdown, not a
/// disconnect: no reconnect and no abort marker; the open transaction is left
/// to the coordinator's shutdown handling.
#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_a_stop_inside_a_transaction_aborts_nothing() -> Result<()> {
    init_test_tracing();
    const DB: &str = "shop";

    let (_c, port) = start_postgres().await;
    pg_create_schema(port, DB, "slot_stop", "pub_stop").await;
    let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let src = make_pg_source(
        "stop",
        &pg_dsn(port, DB),
        "slot_stop",
        "pub_stop",
        Arc::clone(&backend),
    )
    .await;
    let (tx, mut rx) = mpsc::channel(1);
    let handle = src.run(tx, Arc::clone(&ckpt)).await;
    sleep(Duration::from_secs(4)).await;
    pg_admin_client(port, DB)
        .await
        .execute(
            "INSERT INTO orders SELECT g, 'big' FROM generate_series(1, 200000) g",
            &[],
        )
        .await?;

    let mut r = Received::default();
    recv_until(&mut rx, &mut r, Duration::from_secs(30), |r| r.rows >= 200)
        .await;
    assert_eq!(r.commits, 0, "stopped inside the transaction");
    reset_streams_opened();
    handle.stop();
    // Drain whatever the source still hands on until it closes its channel.
    recv_until(&mut rx, &mut r, Duration::from_secs(30), |_| false).await;
    timeout(Duration::from_secs(30), handle.join())
        .await
        .expect("the stop ends the source")?;
    assert!(
        !r.seen.iter().any(|s| matches!(s, Seen::Abort(_))),
        "a stop never abandons a transaction"
    );
    assert_eq!(streams_opened(), 0, "a stop never reconnects");
    Ok(())
}
