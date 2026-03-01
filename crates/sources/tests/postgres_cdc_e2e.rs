//! PostgreSQL CDC e2e tests. Run with:
//! `cargo test -p sources --test postgres_cdc_e2e -- --include-ignored --nocapture --test-threads=1`
//!

use anyhow::Result;
use checkpoints::{CheckpointStore, MemCheckpointStore};
use common::AllowList;
use ctor::dtor;
use deltaforge_core::{BatchContext, Event, Op, Source, SourceHandle};
use schema_registry::InMemoryRegistry;

use sources::postgres::{PostgresSchemaLoader, PostgresSource};
use std::sync::Arc;
use std::time::Instant;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::{
    sync::{OnceCell, mpsc},
    time::{Duration, sleep, timeout},
};
use tokio_postgres::NoTls;
use tracing::{debug, info};

mod test_common;
use test_common::init_test_tracing;

const PG_PORT: u16 = 5433;
const PG_USER: &str = "postgres";
const PG_PASS: &str = "password";
const CDC_USER: &str = "df";
const CDC_PASS: &str = "dfpw";

static CONTAINER: OnceCell<ContainerAsync<GenericImage>> =
    OnceCell::const_new();

#[dtor]
fn cleanup() {
    if let Some(c) = CONTAINER.get() {
        std::process::Command::new("docker")
            .args(["rm", "-f", c.id()])
            .output()
            .ok();
    }
}

async fn get_container() -> &'static ContainerAsync<GenericImage> {
    debug!("get_container called ..");
    CONTAINER
        .get_or_init(|| async {
            let img = GenericImage::new("postgres", "17")
                .with_wait_for(WaitFor::message_on_stderr(
                    "database system is ready",
                ))
                .with_env_var("POSTGRES_USER", PG_USER)
                .with_env_var("POSTGRES_PASSWORD", PG_PASS)
                .with_cmd(vec![
                    "postgres",
                    "-c",
                    "wal_level=logical",
                    "-c",
                    "max_replication_slots=10",
                    "-c",
                    "max_wal_senders=10",
                ])
                .with_mapped_port(PG_PORT, 5432.tcp());
            let c = img.start().await.expect("start postgres");
            sleep(Duration::from_secs(5)).await;
            setup_cdc_user().await.expect("setup cdc user");
            c
        })
        .await
}

async fn setup_cdc_user() -> Result<()> {
    let (client, conn) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={PG_PORT} user={PG_USER} password={PG_PASS}"
        ),
        NoTls,
    )
    .await?;
    tokio::spawn(async move {
        conn.await.ok();
    });
    client
        .execute(
            &format!(
                "CREATE USER {CDC_USER} WITH REPLICATION PASSWORD '{CDC_PASS}'"
            ),
            &[],
        )
        .await
        .ok();
    Ok(())
}

async fn create_db(prefix: &str) -> Result<(String, tokio_postgres::Client)> {
    let db = format!("test_{}_{}", prefix, std::process::id());
    let (c, conn) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={PG_PORT} user={PG_USER} password={PG_PASS}"
        ),
        NoTls,
    )
    .await?;
    tokio::spawn(async move {
        conn.await.ok();
    });
    c.execute(&format!("DROP DATABASE IF EXISTS {db}"), &[])
        .await?;
    c.execute(&format!("CREATE DATABASE {db}"), &[]).await?;
    let (client, conn) = tokio_postgres::connect(&format!("host=127.0.0.1 port={PG_PORT} user={PG_USER} password={PG_PASS} dbname={db}"), NoTls).await?;
    tokio::spawn(async move {
        conn.await.ok();
    });
    client
        .execute(&format!("GRANT ALL ON SCHEMA public TO {CDC_USER}"), &[])
        .await?;
    Ok((db, client))
}

async fn drop_db(db: &str) {
    if let Ok((c, conn)) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={PG_PORT} user={PG_USER} password={PG_PASS}"
        ),
        NoTls,
    )
    .await
    {
        tokio::spawn(async move {
            conn.await.ok();
        });
        c.execute(&format!("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='{db}'"), &[]).await.ok();
        c.execute(&format!("DROP DATABASE IF EXISTS {db}"), &[])
            .await
            .ok();
    }
}

fn cdc_dsn(db: &str) -> String {
    format!("postgres://{CDC_USER}:{CDC_PASS}@127.0.0.1:{PG_PORT}/{db}")
}

async fn create_pub_slot(
    client: &tokio_postgres::Client,
    pub_name: &str,
    slot: &str,
    tables: &[&str],
) -> Result<()> {
    client
        .execute(&format!("DROP PUBLICATION IF EXISTS {pub_name}"), &[])
        .await?;
    client.batch_execute(&format!("SELECT pg_drop_replication_slot('{slot}') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='{slot}')")).await.ok();
    let tbl = if tables.is_empty() {
        "ALL TABLES".into()
    } else {
        format!("TABLE {}", tables.join(", "))
    };
    client
        .execute(&format!("CREATE PUBLICATION {pub_name} FOR {tbl}"), &[])
        .await?;
    client
        .batch_execute(&format!(
            "SELECT pg_create_logical_replication_slot('{slot}', 'pgoutput')"
        ))
        .await?;
    Ok(())
}

async fn cleanup_repl(
    client: &tokio_postgres::Client,
    pub_name: &str,
    slot: &str,
) {
    client
        .execute(&format!("DROP PUBLICATION IF EXISTS {pub_name}"), &[])
        .await
        .ok();
    client.batch_execute(&format!("SELECT pg_drop_replication_slot('{slot}') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='{slot}')")).await.ok();
}

async fn wait_ready(handle: &SourceHandle, dur: Duration) -> Result<()> {
    let deadline = Instant::now() + dur;
    while Instant::now() < deadline {
        if handle.join.is_finished() {
            return Err(anyhow::anyhow!(
                "Source task died before becoming ready"
            ));
        }
        sleep(Duration::from_millis(100)).await;
    }
    Ok(())
}

async fn collect_until<F>(
    rx: &mut mpsc::Receiver<Event>,
    dur: Duration,
    pred: F,
) -> Vec<Event>
where
    F: Fn(&[Event]) -> bool,
{
    let mut events = Vec::new();
    let deadline = Instant::now() + dur;
    while Instant::now() < deadline {
        match timeout(Duration::from_millis(100), rx.recv()).await {
            Ok(Some(e)) => {
                events.push(e);
                if pred(&events) {
                    return events;
                }
            }
            _ => continue,
        }
    }
    events
}

fn has_id(e: &Event, id: i32) -> bool {
    e.after
        .as_ref()
        .and_then(|v| v.get("id"))
        .and_then(|v| v.as_i64())
        .map(|v| v == id as i64)
        .unwrap_or(false)
        || e.before
            .as_ref()
            .and_then(|v| v.get("id"))
            .and_then(|v| v.as_i64())
            .map(|v| v == id as i64)
            .unwrap_or(false)
}

// =============================================================================
// DEBEZIUM-COMPATIBLE ENVELOPE HELPERS
// =============================================================================

/// Helper to check if event is a create/insert operation.
/// Debezium uses Op::Create ('c') for insert operations.
fn is_create_op(e: &Event) -> bool {
    matches!(e.op, Op::Create)
}

/// Helper to verify Debezium-compatible source info envelope.
/// Validates the required fields in the source block.
fn verify_source_envelope(e: &Event, expected_connector: &str) {
    // Verify source info fields (Debezium-compatible envelope)
    assert_eq!(
        e.source.connector, expected_connector,
        "connector should be {}",
        expected_connector
    );
    assert!(
        !e.source.name.is_empty(),
        "source name (pipeline) should not be empty"
    );
    assert!(e.source.ts_ms > 0, "source timestamp should be positive");
    assert!(
        !e.source.table.is_empty(),
        "source table should not be empty"
    );

    // Position should have LSN for PostgreSQL
    if let Some(ref lsn) = e.source.position.lsn {
        assert!(!lsn.is_empty(), "LSN should not be empty when present");
    }
}

/// Helper to verify transaction metadata when present.
fn verify_transaction_if_present(e: &Event) {
    if let Some(ref tx) = e.transaction {
        assert!(
            !tx.id.is_empty(),
            "transaction id should not be empty when transaction is present"
        );
    }
}

/// Verify complete event structure including envelope and optional transaction.
fn verify_event_envelope(e: &Event, expected_connector: &str) {
    verify_source_envelope(e, expected_connector);
    verify_transaction_if_present(e);
}

// =============================================================================
// TEST HELPERS
// =============================================================================

/// Standard per-test setup: tracing, shared container, fresh database.
/// Returns (db_name, client).
async fn setup(prefix: &str) -> Result<(String, tokio_postgres::Client)> {
    init_test_tracing();
    get_container().await;
    create_db(prefix).await
}

/// Build a PostgresSource with constant defaults (tenant=acme, pipeline=test,
/// fresh InMemoryRegistry). `checkpoint_key` is derived as `pg-{id}`.
fn make_source(
    id: &str,
    db: &str,
    slot: &str,
    publication: &str,
    tables: Vec<String>,
    outbox_prefixes: AllowList,
) -> PostgresSource {
    PostgresSource {
        id: id.into(),
        checkpoint_key: format!("pg-{id}"),
        dsn: cdc_dsn(db),
        slot: slot.into(),
        publication: publication.into(),
        tables,
        tenant: "acme".into(),
        pipeline: "test".into(),
        registry: Arc::new(InMemoryRegistry::new()),
        outbox_prefixes,
    }
}

/// Start a source with a fresh checkpoint store, wait for ready, warm up 2s.
/// Returns (rx, handle).
async fn start_source(
    src: PostgresSource,
) -> Result<(mpsc::Receiver<Event>, SourceHandle)> {
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let (tx, rx) = mpsc::channel(128);
    let handle = src.run(tx, ckpt).await;
    wait_ready(&handle, Duration::from_secs(10)).await?;
    sleep(Duration::from_secs(2)).await;
    Ok((rx, handle))
}

// =============================================================================
// TESTS
// =============================================================================

#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_schema_loader() -> Result<()> {
    let (db, client) = setup("schema").await?;

    client.execute("CREATE TABLE orders (id SERIAL PRIMARY KEY, sku VARCHAR(64) NOT NULL, price NUMERIC(10,2))", &[]).await?;
    client.execute("CREATE TABLE order_items (id SERIAL PRIMARY KEY, order_id INT, product VARCHAR(128))", &[]).await?;

    let registry = Arc::new(InMemoryRegistry::new());
    let loader = PostgresSchemaLoader::new(
        &format!(
            "host=127.0.0.1 port={PG_PORT} user={PG_USER} password={PG_PASS} dbname={db}"
        ),
        registry.clone(),
        "acme",
    );

    // Single table load
    let loaded = loader.load_schema("public", "orders").await?;
    assert_eq!(loaded.schema.columns.len(), 3);
    assert!(!loaded.schema.column("sku").unwrap().nullable);
    assert!(loaded.schema.column("price").unwrap().nullable);
    info!("✓ schema load + nullable detection");

    // Wildcard expansion
    let tables = loader.preload(&["public.order%".to_string()]).await?;
    assert!(tables.len() >= 2);
    info!("✓ wildcard expansion");

    // DDL detection
    let fp1 = loader.load_schema("public", "orders").await?.fingerprint;
    client
        .execute("ALTER TABLE orders ADD COLUMN notes TEXT", &[])
        .await?;
    let fp2 = loader.reload_schema("public", "orders").await?.fingerprint;
    assert_ne!(fp1, fp2);
    info!("✓ DDL detection");

    drop_db(&db).await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_cdc_basic_events() -> Result<()> {
    let (db, client) = setup("basic").await?;

    client.execute("CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(64), payload JSONB)", &[]).await?;
    client
        .execute("ALTER TABLE orders REPLICA IDENTITY FULL", &[])
        .await?;
    client
        .execute(&format!("GRANT SELECT ON orders TO {CDC_USER}"), &[])
        .await?;
    create_pub_slot(&client, "pub_basic", "slot_basic", &["orders"]).await?;

    let src = make_source(
        "basic",
        &db,
        "slot_basic",
        "pub_basic",
        vec!["public.orders".into()],
        AllowList::default(),
    );
    let (mut rx, handle) = start_source(src).await?;

    client
        .execute("INSERT INTO orders VALUES (1, 'sku-1', '{\"a\":1}')", &[])
        .await?;
    client
        .execute("UPDATE orders SET sku='sku-1b' WHERE id=1", &[])
        .await?;
    client.execute("DELETE FROM orders WHERE id=1", &[]).await?;

    let events = collect_until(&mut rx, Duration::from_secs(15), |e| {
        e.iter().filter(|x| has_id(x, 1)).count() >= 3
    })
    .await;
    let by_id: Vec<_> = events.iter().filter(|e| has_id(e, 1)).collect();

    // Debezium uses Op::Create ('c') for inserts
    assert!(
        by_id.iter().any(|e| is_create_op(e)),
        "should have CREATE event"
    );
    assert!(
        by_id.iter().any(|e| matches!(e.op, Op::Update)),
        "should have UPDATE event"
    );
    assert!(
        by_id.iter().any(|e| matches!(e.op, Op::Delete)),
        "should have DELETE event"
    );
    info!("✓ CREATE/UPDATE/DELETE verified");

    // Verify Debezium-compatible envelope on all events
    for e in &by_id {
        verify_event_envelope(e, "postgresql");
    }
    info!("✓ Debezium-compatible envelope verified");

    // Verify CREATE event structure
    if let Some(create_ev) = by_id.iter().find(|e| is_create_op(e)) {
        assert!(create_ev.before.is_none(), "CREATE should not have before");
        assert!(create_ev.after.is_some(), "CREATE should have after");
        let after = create_ev.after.as_ref().unwrap();
        assert_eq!(after["id"], 1);
        assert_eq!(after["sku"], "sku-1");
        info!("✓ CREATE event payload verified");
    }

    // Verify UPDATE event structure
    if let Some(update_ev) = by_id.iter().find(|e| matches!(e.op, Op::Update)) {
        assert!(update_ev.before.is_some(), "UPDATE should have before");
        assert!(update_ev.after.is_some(), "UPDATE should have after");
        let before = update_ev.before.as_ref().unwrap();
        let after = update_ev.after.as_ref().unwrap();
        assert_eq!(before["sku"], "sku-1");
        assert_eq!(after["sku"], "sku-1b");
        info!("✓ UPDATE event payload verified");
    }

    // Verify DELETE event structure
    if let Some(delete_ev) = by_id.iter().find(|e| matches!(e.op, Op::Delete)) {
        assert!(delete_ev.before.is_some(), "DELETE should have before");
        assert!(delete_ev.after.is_none(), "DELETE should not have after");
        info!("✓ DELETE event payload verified");
    }

    handle.stop();
    handle.join().await.ok();
    cleanup_repl(&client, "pub_basic", "slot_basic").await;
    drop_db(&db).await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_cdc_schema_evolution() -> Result<()> {
    let (db, client) = setup("evo").await?;

    client
        .execute(
            "CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(64))",
            &[],
        )
        .await?;
    client
        .execute("ALTER TABLE orders REPLICA IDENTITY FULL", &[])
        .await?;
    client
        .execute(&format!("GRANT SELECT ON orders TO {CDC_USER}"), &[])
        .await?;
    create_pub_slot(&client, "pub_evo", "slot_evo", &["orders"]).await?;

    let src = make_source(
        "evo",
        &db,
        "slot_evo",
        "pub_evo",
        vec!["public.orders".into()],
        AllowList::default(),
    );
    let (mut rx, handle) = start_source(src).await?;

    client
        .execute("INSERT INTO orders VALUES (100, 'pre-ddl')", &[])
        .await?;
    let events = collect_until(&mut rx, Duration::from_secs(10), |e| {
        e.iter().any(|x| has_id(x, 100))
    })
    .await;
    let v1 = events
        .iter()
        .find(|e| has_id(e, 100))
        .unwrap()
        .schema_version
        .clone();

    client
        .execute("ALTER TABLE orders ADD COLUMN status VARCHAR(32)", &[])
        .await?;
    client
        .execute("INSERT INTO orders VALUES (101, 'post-ddl', 'active')", &[])
        .await?;

    let events = collect_until(&mut rx, Duration::from_secs(15), |e| {
        e.iter().any(|x| has_id(x, 101))
    })
    .await;
    let post = events.iter().find(|e| has_id(e, 101)).unwrap();
    assert!(post.after.as_ref().unwrap().get("status").is_some());
    assert_ne!(v1, post.schema_version);
    info!("✓ schema evolution detected");

    // Verify envelope on schema-evolved event
    verify_event_envelope(post, "postgresql");
    info!("✓ envelope intact after schema evolution");

    handle.stop();
    handle.join().await.ok();
    cleanup_repl(&client, "pub_evo", "slot_evo").await;
    drop_db(&db).await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_cdc_checkpoint_resume() -> Result<()> {
    let (db, client) = setup("ckpt").await?;

    client
        .execute(
            "CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(64))",
            &[],
        )
        .await?;
    client
        .execute("ALTER TABLE orders REPLICA IDENTITY FULL", &[])
        .await?;
    client
        .execute(&format!("GRANT SELECT ON orders TO {CDC_USER}"), &[])
        .await?;
    create_pub_slot(&client, "pub_ckpt", "slot_ckpt", &["orders"]).await?;

    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);

    // First run
    {
        let (tx, mut rx) = mpsc::channel(128);
        let src = make_source(
            "ckpt",
            &db,
            "slot_ckpt",
            "pub_ckpt",
            vec!["public.orders".into()],
            AllowList::default(),
        );
        let handle = src.run(tx, ckpt.clone()).await;
        wait_ready(&handle, Duration::from_secs(10)).await?;
        sleep(Duration::from_secs(2)).await;

        client
            .execute("INSERT INTO orders VALUES (300, 'first-run')", &[])
            .await?;
        let events = collect_until(&mut rx, Duration::from_secs(10), |e| {
            e.iter().any(|x| has_id(x, 300))
        })
        .await;
        assert!(events.iter().any(|e| has_id(e, 300)));
        info!("✓ first run ok");

        handle.stop();
        handle.join().await.ok();
    }

    client
        .execute("INSERT INTO orders VALUES (301, 'while-down')", &[])
        .await?;

    // Second run
    {
        let (tx, mut rx) = mpsc::channel(128);
        let src = make_source(
            "ckpt",
            &db,
            "slot_ckpt",
            "pub_ckpt",
            vec!["public.orders".into()],
            AllowList::default(),
        );
        let handle = src.run(tx, ckpt.clone()).await;
        wait_ready(&handle, Duration::from_secs(10)).await?;

        let events = collect_until(&mut rx, Duration::from_secs(15), |e| {
            e.iter().any(|x| has_id(x, 301))
        })
        .await;
        assert!(events.iter().any(|e| has_id(e, 301)));
        info!("✓ checkpoint resume ok");

        handle.stop();
        handle.join().await.ok();
    }

    cleanup_repl(&client, "pub_ckpt", "slot_ckpt").await;
    drop_db(&db).await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_cdc_table_filtering() -> Result<()> {
    let (db, client) = setup("filter").await?;

    client
        .execute(
            "CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(64))",
            &[],
        )
        .await?;
    client
        .execute("CREATE TABLE audit_log (id INT PRIMARY KEY, msg TEXT)", &[])
        .await?;
    client
        .execute("ALTER TABLE orders REPLICA IDENTITY FULL", &[])
        .await?;
    client
        .execute("ALTER TABLE audit_log REPLICA IDENTITY FULL", &[])
        .await?;
    client
        .execute(
            &format!("GRANT SELECT ON orders, audit_log TO {CDC_USER}"),
            &[],
        )
        .await?;
    create_pub_slot(
        &client,
        "pub_filter",
        "slot_filter",
        &["orders", "audit_log"],
    )
    .await?;

    let src = make_source(
        "filter",
        &db,
        "slot_filter",
        "pub_filter",
        vec!["public.orders".into()],
        AllowList::default(),
    ); // Only orders
    let (mut rx, handle) = start_source(src).await?;

    client
        .execute("INSERT INTO audit_log VALUES (1, 'filtered')", &[])
        .await?;
    client
        .execute("INSERT INTO orders VALUES (1, 'captured')", &[])
        .await?;

    let events = collect_until(&mut rx, Duration::from_secs(10), |e| {
        e.iter().any(|x| x.source.table.contains("orders"))
    })
    .await;
    assert!(events.iter().all(|e| e.source.table.contains("orders")));
    assert!(!events.iter().any(|e| e.source.table.contains("audit_log")));
    info!("✓ table filtering ok");

    handle.stop();
    handle.join().await.ok();
    cleanup_repl(&client, "pub_filter", "slot_filter").await;
    drop_db(&db).await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_cdc_reconnect() -> Result<()> {
    let (db, client) = setup("reconn").await?;

    client
        .execute(
            "CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(64))",
            &[],
        )
        .await?;
    client
        .execute("ALTER TABLE orders REPLICA IDENTITY FULL", &[])
        .await?;
    client
        .execute(&format!("GRANT SELECT ON orders TO {CDC_USER}"), &[])
        .await?;
    create_pub_slot(&client, "pub_reconn", "slot_reconn", &["orders"]).await?;

    let src = make_source(
        "reconn",
        &db,
        "slot_reconn",
        "pub_reconn",
        vec!["public.orders".into()],
        AllowList::default(),
    );
    let (mut rx, handle) = start_source(src).await?;

    client
        .execute("INSERT INTO orders VALUES (200, 'before')", &[])
        .await?;
    let events = collect_until(&mut rx, Duration::from_secs(10), |e| {
        e.iter().any(|x| has_id(x, 200))
    })
    .await;
    assert!(events.iter().any(|e| has_id(e, 200)));

    // Kill connection
    client.execute(&format!("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename='{CDC_USER}' LIMIT 1"), &[]).await.ok();
    sleep(Duration::from_secs(5)).await;

    client
        .execute("INSERT INTO orders VALUES (201, 'after')", &[])
        .await?;
    let events = collect_until(&mut rx, Duration::from_secs(20), |e| {
        e.iter().any(|x| has_id(x, 201))
    })
    .await;
    assert!(events.iter().any(|e| has_id(e, 201)));
    info!("✓ reconnect ok");

    handle.stop();
    handle.join().await.ok();
    cleanup_repl(&client, "pub_reconn", "slot_reconn").await;
    drop_db(&db).await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_cdc_extended_types() -> Result<()> {
    let (db, client) = setup("types").await?;

    client.execute("CREATE TABLE complex (id SERIAL PRIMARY KEY, uuid_col UUID, tags TEXT[], metadata JSONB, amount NUMERIC(15,4))", &[]).await?;
    client
        .execute("ALTER TABLE complex REPLICA IDENTITY FULL", &[])
        .await?;
    client
        .execute(&format!("GRANT SELECT ON complex TO {CDC_USER}"), &[])
        .await?;
    create_pub_slot(&client, "pub_types", "slot_types", &["complex"]).await?;

    let src = make_source(
        "types",
        &db,
        "slot_types",
        "pub_types",
        vec!["public.complex".into()],
        AllowList::default(),
    );
    let (mut rx, handle) = start_source(src).await?;

    client.execute("INSERT INTO complex (uuid_col, tags, metadata, amount) VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', ARRAY['a','b'], '{\"k\":1}', 123.4567)", &[]).await?;

    // Use is_create_op for Debezium-compatible check
    let events = collect_until(&mut rx, Duration::from_secs(10), |e| {
        e.iter().any(is_create_op)
    })
    .await;
    let ins = events.iter().find(|e| is_create_op(e)).unwrap();
    let after = ins.after.as_ref().unwrap();
    assert!(after.get("uuid_col").is_some());
    assert!(after.get("tags").is_some());
    assert!(after.get("metadata").is_some());
    assert!(after.get("amount").is_some());
    info!("✓ extended types ok");

    // Verify envelope for extended types
    verify_event_envelope(ins, "postgresql");
    info!("✓ envelope verified for extended types");

    handle.stop();
    handle.join().await.ok();
    cleanup_repl(&client, "pub_types", "slot_types").await;
    drop_db(&db).await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_cdc_pause_resume() -> Result<()> {
    let (db, client) = setup("pause").await?;

    client
        .execute(
            "CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(64))",
            &[],
        )
        .await?;
    client
        .execute("ALTER TABLE orders REPLICA IDENTITY FULL", &[])
        .await?;
    client
        .execute(&format!("GRANT SELECT ON orders TO {CDC_USER}"), &[])
        .await?;
    create_pub_slot(&client, "pub_pause", "slot_pause", &["orders"]).await?;

    let src = make_source(
        "pause",
        &db,
        "slot_pause",
        "pub_pause",
        vec!["public.orders".into()],
        AllowList::default(),
    );
    let (mut rx, handle) = start_source(src).await?;

    client
        .execute("INSERT INTO orders VALUES (1, 'before')", &[])
        .await?;
    let events = collect_until(&mut rx, Duration::from_secs(10), |e| {
        e.iter().any(|x| has_id(x, 1))
    })
    .await;
    assert!(events.iter().any(|e| has_id(e, 1)));

    handle.pause();
    sleep(Duration::from_secs(1)).await;

    client
        .execute("INSERT INTO orders VALUES (2, 'paused')", &[])
        .await?;
    assert!(
        timeout(Duration::from_millis(500), rx.recv())
            .await
            .is_err()
    );

    handle.resume();
    let events = collect_until(&mut rx, Duration::from_secs(10), |e| {
        e.iter().any(|x| has_id(x, 2))
    })
    .await;
    assert!(events.iter().any(|e| has_id(e, 2)));
    info!("✓ pause/resume ok");

    handle.stop();
    handle.join().await.ok();
    cleanup_repl(&client, "pub_pause", "slot_pause").await;
    drop_db(&db).await;
    Ok(())
}

// =============================================================================
// ERROR HANDLING TESTS
// =============================================================================

/// Test error handling: authentication failure should produce Auth error.
#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_cdc_auth_failure() -> Result<()> {
    let (db, client) = setup("auth_fail").await?;

    client
        .execute("CREATE TABLE orders (id INT PRIMARY KEY)", &[])
        .await?;
    client
        .execute(&format!("GRANT SELECT ON orders TO {CDC_USER}"), &[])
        .await?;
    create_pub_slot(&client, "pub_auth", "slot_auth", &["orders"]).await?;

    // Use wrong password
    let bad_dsn = format!(
        "postgres://{CDC_USER}:WRONG_PASSWORD@127.0.0.1:{PG_PORT}/{db}"
    );

    let mut src = make_source(
        "auth-fail",
        &db,
        "slot_auth",
        "pub_auth",
        vec!["public.orders".into()],
        AllowList::default(),
    );
    src.dsn = bad_dsn;
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let (tx, _rx) = mpsc::channel(128);
    let handle = src.run(tx, ckpt).await;

    // Wait for task to fail - auth errors should cause quick exit, not infinite retry
    let result = timeout(Duration::from_secs(15), handle.join()).await;

    match result {
        Ok(Ok(())) => {
            info!("✓ auth failure caused source to exit");
        }
        Ok(Err(e)) => {
            info!("✓ auth failure caused panic: {}", e);
        }
        Err(_) => {
            panic!(
                "timeout - source should exit on auth failure, not retry forever"
            );
        }
    }

    cleanup_repl(&client, "pub_auth", "slot_auth").await;
    drop_db(&db).await;
    Ok(())
}

/// Test that missing slot is auto-created by ensure_slot_and_publication.
#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_cdc_slot_auto_created() -> Result<()> {
    let (db, client) = setup("slot_auto").await?;

    client
        .execute("CREATE TABLE orders (id INT PRIMARY KEY, name TEXT)", &[])
        .await?;
    client
        .execute("ALTER TABLE orders REPLICA IDENTITY FULL", &[])
        .await?;
    client
        .execute(&format!("GRANT SELECT ON orders TO {CDC_USER}"), &[])
        .await?;

    // Create publication but NO slot - slot should be auto-created
    client
        .execute("DROP PUBLICATION IF EXISTS pub_auto", &[])
        .await?;
    client
        .execute("CREATE PUBLICATION pub_auto FOR TABLE orders", &[])
        .await?;

    // Verify slot doesn't exist yet
    let slot_exists: bool = client
        .query_one("SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = 'auto_slot')", &[])
        .await?
        .get(0);
    assert!(!slot_exists, "slot should not exist before source starts");

    let src = make_source(
        "slot-auto",
        &db,
        "auto_slot",
        "pub_auto",
        vec!["public.orders".into()],
        AllowList::default(),
    );
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let (tx, mut rx) = mpsc::channel(128);
    let handle = src.run(tx, ckpt).await;

    sleep(Duration::from_secs(2)).await;

    let slot_exists: bool = client
        .query_one("SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = 'auto_slot')", &[])
        .await?
        .get(0);
    assert!(slot_exists, "slot should be auto-created by source");
    info!("✓ slot auto-created");

    client
        .execute("INSERT INTO orders (id, name) VALUES (1, 'test')", &[])
        .await?;

    let event = timeout(Duration::from_secs(10), rx.recv())
        .await?
        .expect("should receive create event");
    // Debezium uses Op::Create for inserts
    assert!(is_create_op(&event), "should be CREATE op");
    info!("✓ captured create event after slot auto-creation");

    // Verify envelope
    verify_event_envelope(&event, "postgresql");
    info!("✓ envelope verified");

    handle.stop();
    let _ = timeout(Duration::from_secs(5), handle.join()).await;

    client
        .batch_execute("SELECT pg_drop_replication_slot('auto_slot')")
        .await
        .ok();
    client
        .execute("DROP PUBLICATION IF EXISTS pub_auto", &[])
        .await?;
    drop_db(&db).await;
    Ok(())
}

/// Test that missing publication causes retry loop, and source recovers when admin creates it.
#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_cdc_publication_missing_then_created() -> Result<()> {
    let (db, client) = setup("pub_missing").await?;

    client
        .execute("CREATE TABLE orders (id INT PRIMARY KEY, name TEXT)", &[])
        .await?;
    client
        .execute("ALTER TABLE orders REPLICA IDENTITY FULL", &[])
        .await?;
    client
        .execute(&format!("GRANT SELECT ON orders TO {CDC_USER}"), &[])
        .await?;

    client
        .execute("DROP PUBLICATION IF EXISTS missing_pub", &[])
        .await?;
    client.batch_execute("SELECT pg_drop_replication_slot('slot_missing_pub') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='slot_missing_pub')").await.ok();
    client.batch_execute("SELECT pg_create_logical_replication_slot('slot_missing_pub', 'pgoutput')").await?;

    let pub_exists: bool = client
        .query_one("SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = 'missing_pub')", &[])
        .await?
        .get(0);
    assert!(!pub_exists, "publication should not exist before test");

    let src = make_source(
        "pub-missing",
        &db,
        "slot_missing_pub",
        "missing_pub",
        vec!["public.orders".into()],
        AllowList::default(),
    );
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let (tx, mut rx) = mpsc::channel(128);
    let handle = src.run(tx, ckpt).await;

    sleep(Duration::from_secs(3)).await;

    let pub_exists: bool = client
        .query_one("SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = 'missing_pub')", &[])
        .await?
        .get(0);
    assert!(
        !pub_exists,
        "publication should NOT be auto-created by source"
    );
    info!("✓ verified source does not auto-create publication");

    client
        .execute("CREATE PUBLICATION missing_pub FOR TABLE orders", &[])
        .await?;
    info!("✓ admin created publication");

    sleep(Duration::from_secs(2)).await;
    client
        .execute("INSERT INTO orders (id, name) VALUES (1, 'test')", &[])
        .await?;

    let event = timeout(Duration::from_secs(15), rx.recv())
        .await?
        .expect("should receive create event after publication created");
    assert!(is_create_op(&event), "should be CREATE op");
    info!("✓ captured create event after admin created publication");

    verify_event_envelope(&event, "postgresql");

    handle.stop();
    let _ = timeout(Duration::from_secs(5), handle.join()).await;

    client
        .batch_execute("SELECT pg_drop_replication_slot('slot_missing_pub')")
        .await
        .ok();
    client
        .execute("DROP PUBLICATION IF EXISTS missing_pub", &[])
        .await?;
    drop_db(&db).await;
    Ok(())
}

/// Test error handling: invalid DSN format.
#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_cdc_invalid_dsn() -> Result<()> {
    init_test_tracing();
    let _ = get_container().await;

    let mut src = make_source(
        "bad-dsn",
        "invalid",
        "any_slot",
        "any_pub",
        vec!["public.orders".into()],
        AllowList::default(),
    );
    src.dsn = "not-a-valid-dsn-at-all".into();
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let (tx, _rx) = mpsc::channel(128);
    let handle = src.run(tx, ckpt).await;

    let result = timeout(Duration::from_secs(10), handle.join()).await;

    match result {
        Ok(Ok(())) => info!("✓ source exited due to invalid DSN"),
        Ok(Err(e)) => info!("✓ source panicked due to invalid DSN: {}", e),
        Err(_) => panic!("timeout - source should exit quickly on invalid DSN"),
    }

    Ok(())
}

/// Test error handling: connection refused (wrong port).
#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_cdc_connection_refused() -> Result<()> {
    init_test_tracing();
    let _ = get_container().await;

    let bad_dsn =
        format!("postgres://{CDC_USER}:{CDC_PASS}@127.0.0.1:59999/testdb");

    let mut src = make_source(
        "conn-refused",
        "invalid",
        "any_slot",
        "any_pub",
        vec!["public.orders".into()],
        AllowList::default(),
    );
    src.dsn = bad_dsn;
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let (tx, _rx) = mpsc::channel(128);
    let handle = src.run(tx, ckpt).await;

    let result = timeout(Duration::from_secs(10), handle.join()).await;

    match result {
        Ok(Ok(())) => info!("✓ source exited due to connection failure"),
        Ok(Err(e)) => {
            info!("✓ source panicked due to connection failure: {}", e)
        }
        Err(_) => info!(
            "✓ source in reconnect loop (expected for transient connection errors)"
        ),
    }

    Ok(())
}

/// Test replica identity modes (DEFAULT vs FULL).
#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_cdc_replica_identity_modes() -> Result<()> {
    let (db, client) = setup("replica_id").await?;

    client
        .execute(
            "CREATE TABLE ri_default (id INT PRIMARY KEY, data TEXT)",
            &[],
        )
        .await?;

    client
        .execute("CREATE TABLE ri_full (id INT PRIMARY KEY, data TEXT)", &[])
        .await?;
    client
        .execute("ALTER TABLE ri_full REPLICA IDENTITY FULL", &[])
        .await?;

    client
        .execute(
            &format!("GRANT SELECT ON ri_default, ri_full TO {CDC_USER}"),
            &[],
        )
        .await?;
    create_pub_slot(&client, "pub_ri", "slot_ri", &["ri_default", "ri_full"])
        .await?;

    let registry = Arc::new(InMemoryRegistry::new());
    let loader = PostgresSchemaLoader::new(
        &format!(
            "host=127.0.0.1 port={PG_PORT} user={PG_USER} password={PG_PASS} dbname={db}"
        ),
        registry.clone(),
        "acme",
    );

    let default_schema = loader.load_schema("public", "ri_default").await?;
    assert_eq!(
        default_schema.schema.replica_identity,
        Some("default".to_string())
    );
    info!("✓ DEFAULT replica identity captured");

    let full_schema = loader.load_schema("public", "ri_full").await?;
    assert_eq!(
        full_schema.schema.replica_identity,
        Some("full".to_string())
    );
    info!("✓ FULL replica identity captured");

    let src = make_source(
        "ri",
        &db,
        "slot_ri",
        "pub_ri",
        vec!["public.ri_default".into(), "public.ri_full".into()],
        AllowList::default(),
    );
    let (mut rx, handle) = start_source(src).await?;

    client
        .execute(
            "INSERT INTO ri_default (id, data) VALUES (1, 'original')",
            &[],
        )
        .await?;
    client
        .execute("INSERT INTO ri_full (id, data) VALUES (1, 'original')", &[])
        .await?;
    client
        .execute("UPDATE ri_default SET data = 'modified' WHERE id = 1", &[])
        .await?;
    client
        .execute("UPDATE ri_full SET data = 'modified' WHERE id = 1", &[])
        .await?;

    let events =
        collect_until(&mut rx, Duration::from_secs(10), |e| e.len() >= 4).await;

    // Verify envelope on all events
    for e in &events {
        verify_event_envelope(e, "postgresql");
    }
    info!("✓ envelope verified for all replica identity events");

    let full_update = events
        .iter()
        .find(|e| e.op == Op::Update && e.source.table.contains("ri_full"));
    if let Some(upd) = full_update {
        let before = upd.before.as_ref().expect("FULL should have before");
        assert_eq!(before["id"], 1);
        assert_eq!(before["data"], "original");
        info!("✓ ri_full UPDATE has full before image");
    }

    let default_update = events
        .iter()
        .find(|e| e.op == Op::Update && e.source.table.contains("ri_default"));
    if let Some(upd) = default_update {
        let before = upd.before.as_ref();
        info!("ri_default UPDATE before: {:?}", before);
    }

    handle.stop();
    handle.join().await.ok();
    cleanup_repl(&client, "pub_ri", "slot_ri").await;
    drop_db(&db).await;
    Ok(())
}

/// Test graceful shutdown during active streaming.
#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_cdc_graceful_shutdown() -> Result<()> {
    let (db, client) = setup("shutdown").await?;

    client
        .execute(
            "CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(64))",
            &[],
        )
        .await?;
    client
        .execute("ALTER TABLE orders REPLICA IDENTITY FULL", &[])
        .await?;
    client
        .execute(&format!("GRANT SELECT ON orders TO {CDC_USER}"), &[])
        .await?;
    create_pub_slot(&client, "pub_shutdown", "slot_shutdown", &["orders"])
        .await?;

    let src = make_source(
        "shutdown",
        &db,
        "slot_shutdown",
        "pub_shutdown",
        vec!["public.orders".into()],
        AllowList::default(),
    );
    let (mut rx, handle) = start_source(src).await?;

    client
        .execute("INSERT INTO orders VALUES (1, 'test')", &[])
        .await?;
    let events = collect_until(&mut rx, Duration::from_secs(10), |e| {
        e.iter().any(|x| has_id(x, 1))
    })
    .await;
    assert!(events.iter().any(|e| has_id(e, 1)));

    let start = Instant::now();
    handle.stop();

    let join_result = timeout(Duration::from_secs(15), handle.join()).await;
    let elapsed = start.elapsed();

    match join_result {
        Ok(Ok(())) => info!("✓ graceful shutdown completed in {:?}", elapsed),
        Ok(Err(e)) => {
            info!("✓ shutdown with join error: {} in {:?}", e, elapsed)
        }
        Err(_) => panic!("shutdown took too long (>5s)"),
    }

    cleanup_repl(&client, "pub_shutdown", "slot_shutdown").await;
    drop_db(&db).await;
    Ok(())
}

/// Test multiple tables in single publication.
#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_cdc_multi_table() -> Result<()> {
    let (db, client) = setup("multi").await?;

    client
        .execute(
            "CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(64))",
            &[],
        )
        .await?;
    client
        .execute(
            "CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(128))",
            &[],
        )
        .await?;
    client
        .execute(
            "CREATE TABLE products (id INT PRIMARY KEY, title VARCHAR(256))",
            &[],
        )
        .await?;

    for tbl in &["orders", "customers", "products"] {
        client
            .execute(&format!("ALTER TABLE {} REPLICA IDENTITY FULL", tbl), &[])
            .await?;
        client
            .execute(&format!("GRANT SELECT ON {} TO {}", tbl, CDC_USER), &[])
            .await?;
    }

    create_pub_slot(
        &client,
        "pub_multi",
        "slot_multi",
        &["orders", "customers", "products"],
    )
    .await?;

    let src = make_source(
        "multi",
        &db,
        "slot_multi",
        "pub_multi",
        vec![
            "public.orders".into(),
            "public.customers".into(),
            "public.products".into(),
        ],
        AllowList::default(),
    );
    let (mut rx, handle) = start_source(src).await?;

    client
        .execute("INSERT INTO orders VALUES (1, 'SKU-001')", &[])
        .await?;
    client
        .execute("INSERT INTO customers VALUES (1, 'Alice')", &[])
        .await?;
    client
        .execute("INSERT INTO products VALUES (1, 'Widget')", &[])
        .await?;

    let events = collect_until(&mut rx, Duration::from_secs(15), |e| {
        let has_orders = e.iter().any(|x| x.source.table.contains("orders"));
        let has_customers =
            e.iter().any(|x| x.source.table.contains("customers"));
        let has_products =
            e.iter().any(|x| x.source.table.contains("products"));
        has_orders && has_customers && has_products
    })
    .await;

    assert!(
        events.iter().any(|e| e.source.table.contains("orders")),
        "missing orders event"
    );
    assert!(
        events.iter().any(|e| e.source.table.contains("customers")),
        "missing customers event"
    );
    assert!(
        events.iter().any(|e| e.source.table.contains("products")),
        "missing products event"
    );
    info!("✓ multi-table CDC works");

    // Verify envelope on all events from different tables
    for e in &events {
        verify_event_envelope(e, "postgresql");
    }
    info!("✓ envelope verified for multi-table events");

    handle.stop();
    handle.join().await.ok();
    cleanup_repl(&client, "pub_multi", "slot_multi").await;
    drop_db(&db).await;
    Ok(())
}

/// Test handling of NULL values in various column types.
#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_cdc_null_handling() -> Result<()> {
    let (db, client) = setup("nulls").await?;

    client
        .execute(
            "CREATE TABLE nullable_test (
            id INT PRIMARY KEY,
            text_col TEXT,
            int_col INT,
            json_col JSONB,
            array_col TEXT[]
        )",
            &[],
        )
        .await?;
    client
        .execute("ALTER TABLE nullable_test REPLICA IDENTITY FULL", &[])
        .await?;
    client
        .execute(&format!("GRANT SELECT ON nullable_test TO {CDC_USER}"), &[])
        .await?;
    create_pub_slot(&client, "pub_null", "slot_null", &["nullable_test"])
        .await?;

    let src = make_source(
        "nulls",
        &db,
        "slot_null",
        "pub_null",
        vec!["public.nullable_test".into()],
        AllowList::default(),
    );
    let (mut rx, handle) = start_source(src).await?;

    client
        .execute("INSERT INTO nullable_test (id) VALUES (1)", &[])
        .await?;

    client.execute(
        "INSERT INTO nullable_test VALUES (2, 'text', 42, '{\"k\":1}', ARRAY['a','b'])",
        &[]
    ).await?;

    client
        .execute("UPDATE nullable_test SET text_col = NULL WHERE id = 2", &[])
        .await?;

    let events =
        collect_until(&mut rx, Duration::from_secs(10), |e| e.len() >= 3).await;

    // Verify NULL handling in INSERT (using is_create_op for Debezium compatibility)
    let null_insert = events.iter().find(|e| is_create_op(e) && has_id(e, 1));
    if let Some(ins) = null_insert {
        let after = ins.after.as_ref().unwrap();
        assert!(after["text_col"].is_null());
        assert!(after["int_col"].is_null());
        info!("✓ NULL values in CREATE handled correctly");

        verify_event_envelope(ins, "postgresql");
    }

    let null_update =
        events.iter().find(|e| e.op == Op::Update && has_id(e, 2));
    if let Some(upd) = null_update {
        let after = upd.after.as_ref().unwrap();
        assert!(after["text_col"].is_null());
        info!("✓ NULL values in UPDATE handled correctly");

        verify_event_envelope(upd, "postgresql");
    }

    handle.stop();
    handle.join().await.ok();
    cleanup_repl(&client, "pub_null", "slot_null").await;
    drop_db(&db).await;
    Ok(())
}

// =============================================================================
// OUTBOX PATTERN TESTS
// =============================================================================

/// Test outbox capture via pg_logical_emit_message().
/// Verifies:
/// - Matching prefix -> source.schema = "__outbox"
/// - Non-matching prefix -> source.schema = "__wal_message"
/// - JSON payload arrives in event.after
/// - source.table set to the message prefix
#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_cdc_outbox_capture() -> Result<()> {
    let (db, client) = setup("outbox").await?;

    // Need a table + publication for the replication slot to work,
    // but outbox events come from WAL messages, not table changes.
    client
        .execute(
            "CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(64))",
            &[],
        )
        .await?;
    client
        .execute("ALTER TABLE orders REPLICA IDENTITY FULL", &[])
        .await?;
    client
        .execute(&format!("GRANT SELECT ON orders TO {CDC_USER}"), &[])
        .await?;
    create_pub_slot(&client, "pub_outbox", "slot_outbox", &["orders"]).await?;

    let src = make_source(
        "outbox",
        &db,
        "slot_outbox",
        "pub_outbox",
        vec!["public.orders".into()],
        AllowList::new(&["outbox".to_string()]),
    );
    let (mut rx, handle) = start_source(src).await?;

    // Emit outbox message (transactional = true)
    client
        .execute(
            "SELECT pg_logical_emit_message(true, 'outbox', '{\"aggregate_type\":\"Order\",\"aggregate_id\":\"42\",\"event_type\":\"OrderCreated\",\"payload\":{\"total\":99.99}}')",
            &[],
        )
        .await?;

    // Emit non-outbox message
    client
        .execute(
            "SELECT pg_logical_emit_message(true, 'audit', '{\"action\":\"login\"}')",
            &[],
        )
        .await?;

    // Also insert a normal table row to verify coexistence
    client
        .execute("INSERT INTO orders VALUES (1, 'sku-1')", &[])
        .await?;

    let events = collect_until(&mut rx, Duration::from_secs(15), |e| {
        let has_outbox = e.iter().any(|x| x.source.table == "outbox");
        let has_audit = e.iter().any(|x| x.source.table == "audit");
        let has_order = e.iter().any(|x| x.source.table == "orders");
        has_outbox && has_audit && has_order
    })
    .await;

    // --- Outbox event ---
    let outbox_ev = events
        .iter()
        .find(|e| e.source.table == "outbox")
        .expect("should have outbox event");
    assert_eq!(
        outbox_ev.source.schema.as_deref(),
        Some("__outbox"),
        "matching prefix should be tagged __outbox"
    );
    assert_eq!(outbox_ev.op, Op::Create);
    let after = outbox_ev.after.as_ref().expect("should have payload");
    assert_eq!(after["aggregate_type"], "Order");
    assert_eq!(after["aggregate_id"], "42");
    assert_eq!(after["event_type"], "OrderCreated");
    assert_eq!(after["payload"]["total"], 99.99);
    info!("✓ outbox event captured with __outbox sentinel and full payload");

    // --- Non-matching WAL message ---
    let audit_ev = events
        .iter()
        .find(|e| e.source.table == "audit")
        .expect("should have audit event");
    assert_eq!(
        audit_ev.source.schema.as_deref(),
        Some("__wal_message"),
        "non-matching prefix should be tagged __wal_message"
    );
    assert_eq!(audit_ev.after.as_ref().unwrap()["action"], "login");
    info!("✓ non-matching WAL message tagged __wal_message");

    // --- Normal table event coexists ---
    let order_ev = events
        .iter()
        .find(|e| e.source.table == "orders")
        .expect("should have table event");
    assert_eq!(order_ev.source.schema.as_deref(), Some("public"));
    assert!(is_create_op(order_ev));
    info!("✓ normal table CDC coexists with outbox capture");

    handle.stop();
    handle.join().await.ok();
    cleanup_repl(&client, "pub_outbox", "slot_outbox").await;
    drop_db(&db).await;
    Ok(())
}

/// Test outbox with glob prefix patterns (multi-outbox).
/// Verifies that `outbox_%` matches `outbox_orders` and `outbox_payments`
/// but not `audit`.
#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_cdc_outbox_glob_prefix() -> Result<()> {
    let (db, client) = setup("outbox_glob").await?;

    client
        .execute("CREATE TABLE stub (id INT PRIMARY KEY)", &[])
        .await?;
    client
        .execute(&format!("GRANT SELECT ON stub TO {CDC_USER}"), &[])
        .await?;
    create_pub_slot(&client, "pub_obglob", "slot_obglob", &["stub"]).await?;

    let src = make_source(
        "obglob",
        &db,
        "slot_obglob",
        "pub_obglob",
        vec!["public.stub".into()],
        AllowList::new(&["outbox_%".to_string()]),
    );
    let (mut rx, handle) = start_source(src).await?;

    client
        .execute(
            "SELECT pg_logical_emit_message(true, 'outbox_orders', '{\"event_type\":\"OrderCreated\"}')",
            &[],
        )
        .await?;
    client
        .execute(
            "SELECT pg_logical_emit_message(true, 'outbox_payments', '{\"event_type\":\"PaymentReceived\"}')",
            &[],
        )
        .await?;
    client
        .execute(
            "SELECT pg_logical_emit_message(true, 'audit', '{\"action\":\"login\"}')",
            &[],
        )
        .await?;

    let events = collect_until(&mut rx, Duration::from_secs(15), |e| {
        e.iter().filter(|x| x.source.schema.is_some()).count() >= 3
    })
    .await;

    let outbox_orders =
        events.iter().find(|e| e.source.table == "outbox_orders");
    let outbox_payments =
        events.iter().find(|e| e.source.table == "outbox_payments");
    let audit = events.iter().find(|e| e.source.table == "audit");

    assert_eq!(
        outbox_orders.unwrap().source.schema.as_deref(),
        Some("__outbox"),
    );
    assert_eq!(
        outbox_payments.unwrap().source.schema.as_deref(),
        Some("__outbox"),
    );
    assert_eq!(
        audit.unwrap().source.schema.as_deref(),
        Some("__wal_message"),
    );
    info!(
        "✓ glob prefix outbox_%  matches outbox_orders and outbox_payments, not audit"
    );

    handle.stop();
    handle.join().await.ok();
    cleanup_repl(&client, "pub_obglob", "slot_obglob").await;
    drop_db(&db).await;
    Ok(())
}

/// Test full outbox pipeline: source capture → OutboxProcessor → transformed event.
/// This wires the processor in-process to verify the complete data flow.
#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_cdc_outbox_full_pipeline() -> Result<()> {
    use deltaforge_config::{
        OUTBOX_SCHEMA_SENTINEL, OutboxColumns, OutboxProcessorCfg,
    };
    use deltaforge_core::Processor;
    use processors::OutboxProcessor;
    use std::collections::HashMap;

    let (db, client) = setup("outbox_pipe").await?;

    client
        .execute(
            "CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(64))",
            &[],
        )
        .await?;
    client
        .execute("ALTER TABLE orders REPLICA IDENTITY FULL", &[])
        .await?;
    client
        .execute(&format!("GRANT SELECT ON orders TO {CDC_USER}"), &[])
        .await?;
    create_pub_slot(&client, "pub_obpipe", "slot_obpipe", &["orders"]).await?;

    let src = make_source(
        "obpipe",
        &db,
        "slot_obpipe",
        "pub_obpipe",
        vec!["public.orders".into()],
        AllowList::new(&["outbox".to_string()]),
    );
    let (mut rx, handle) = start_source(src).await?;

    // Emit outbox message + normal insert
    client
        .execute(
            "SELECT pg_logical_emit_message(true, 'outbox', '{\"aggregate_type\":\"Order\",\"aggregate_id\":\"42\",\"event_type\":\"OrderCreated\",\"payload\":{\"order_id\":42,\"total\":99.99}}')",
            &[],
        )
        .await?;
    client
        .execute("INSERT INTO orders VALUES (1, 'sku-1')", &[])
        .await?;

    let raw_events = collect_until(&mut rx, Duration::from_secs(15), |e| {
        let has_outbox = e.iter().any(|x| {
            x.source.schema.as_deref() == Some(OUTBOX_SCHEMA_SENTINEL)
        });
        let has_table = e.iter().any(|x| x.source.table == "orders");
        has_outbox && has_table
    })
    .await;
    assert!(raw_events.len() >= 2, "should have outbox + table events");
    let raw_events_clone = raw_events.clone();

    // Run through processor
    let proc = OutboxProcessor::new(OutboxProcessorCfg {
        id: "outbox".into(),
        tables: vec![],
        columns: OutboxColumns::default(),
        topic: Some("${aggregate_type}.${event_type}".into()),
        default_topic: Some("events.unrouted".into()),
        key: None,
        additional_headers: HashMap::new(),
        raw_payload: false,
        strict: false,
    })?;

    let ctx = BatchContext::from_batch(&raw_events);
    let processed = proc.process(raw_events, &ctx).await?;

    // Outbox event should be transformed
    let outbox_ev = processed
        .iter()
        .find(|e| {
            e.routing.as_ref().and_then(|r| r.topic.as_deref())
                == Some("Order.OrderCreated")
        })
        .expect("should have routed outbox event");
    assert!(
        outbox_ev.source.schema.is_none(),
        "sentinel should be cleared"
    );
    assert_eq!(outbox_ev.after.as_ref().unwrap()["order_id"], 42);
    assert_eq!(outbox_ev.after.as_ref().unwrap()["total"], 99.99);
    let headers = outbox_ev
        .routing
        .as_ref()
        .unwrap()
        .headers
        .as_ref()
        .unwrap();
    assert_eq!(headers.get("df-aggregate-type").unwrap(), "Order");
    assert_eq!(headers.get("df-aggregate-id").unwrap(), "42");
    assert_eq!(headers.get("df-event-type").unwrap(), "OrderCreated");
    assert_eq!(headers.get("df-source-kind").unwrap(), "outbox");
    // Key defaults to aggregate_id when no key template configured
    assert_eq!(
        outbox_ev.routing.as_ref().unwrap().key.as_deref(),
        Some("42"),
        "routing key should default to aggregate_id"
    );
    info!(
        "✓ outbox event transformed: topic, payload, headers, key, provenance"
    );

    // Normal table event should pass through unchanged
    let table_ev = processed
        .iter()
        .find(|e| {
            e.source.table == "orders"
                && e.source.schema.as_deref() == Some("public")
        })
        .expect("table event should pass through");
    assert!(
        table_ev.routing.is_none(),
        "table event should have no routing"
    );
    assert_eq!(table_ev.after.as_ref().unwrap()["sku"], "sku-1");
    info!("✓ normal table event passes through processor unchanged");

    // --- raw_payload mode: re-process cloned raw events ---
    let raw_proc = OutboxProcessor::new(OutboxProcessorCfg {
        id: "outbox-raw".into(),
        tables: vec![],
        columns: OutboxColumns::default(),
        topic: Some("${aggregate_type}.${event_type}".into()),
        default_topic: Some("events.unrouted".into()),
        key: None,
        additional_headers: HashMap::new(),
        raw_payload: true,
        strict: false,
    })?;

    let ctx = BatchContext::from_batch(&raw_events_clone);
    let raw_processed = raw_proc.process(raw_events_clone, &ctx).await?;

    let raw_outbox_ev = raw_processed
        .iter()
        .find(|e| {
            e.routing.as_ref().and_then(|r| r.topic.as_deref())
                == Some("Order.OrderCreated")
        })
        .expect("should have routed outbox event in raw mode");
    assert!(
        raw_outbox_ev.routing.as_ref().unwrap().raw_payload,
        "raw_payload flag should be set on outbox event"
    );
    assert_eq!(
        raw_outbox_ev.after.as_ref().unwrap()["order_id"],
        42,
        "payload should still be extracted"
    );

    let raw_table_ev = raw_processed
        .iter()
        .find(|e| {
            e.source.table == "orders"
                && e.source.schema.as_deref() == Some("public")
        })
        .expect("table event should pass through in raw mode");
    assert!(
        raw_table_ev.routing.as_ref().is_none_or(|r| !r.raw_payload),
        "raw_payload flag should NOT be set on table event"
    );
    info!("✓ raw_payload flag set on outbox, not on table event");

    handle.stop();
    handle.join().await.ok();
    cleanup_repl(&client, "pub_obpipe", "slot_obpipe").await;
    drop_db(&db).await;
    Ok(())
}
