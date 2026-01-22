//! End-to-end integration tests for MySQL CDC source.
//!
//! These tests require Docker and pull `mysql:8.4`.
//!
//! Run with:
//! ```bash
//! cargo test -p sources --test mysql_cdc_e2e -- --include-ignored --nocapture --test-threads=1
//! ```

use anyhow::Result;
use checkpoints::{CheckpointStore, MemCheckpointStore};
use ctor::dtor;
use deltaforge_core::{Event, Op, Source, SourceHandle};
use mysql_async::Opts;
use mysql_async::{Pool as MySQLPool, prelude::Queryable};
use schema_registry::{InMemoryRegistry, SourceSchema};
use sources::SourceSchemaLoader;
use sources::mysql::{MySqlSchemaLoader, MySqlSource};
use std::sync::Arc;
use std::time::Instant;
use testcontainers::runners::AsyncRunner;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt, core::IntoContainerPort,
    core::WaitFor,
};
use tokio::sync::OnceCell;
use tokio::{
    io::AsyncBufReadExt,
    sync::mpsc,
    time::{Duration, sleep, timeout},
};
use tracing::{debug, info, warn};

mod test_common;
use test_common::init_test_tracing;

// =============================================================================
// Shared Test Infrastructure
// =============================================================================

const MYSQL_PORT: u16 = 3308;
const ROOT_PASSWORD: &str = "rootpw";
const CDC_USER: &str = "df";
const CDC_PASSWORD: &str = "dfpw";

/// Shared container - initialized once, reused by all tests.
static MYSQL_CONTAINER: OnceCell<ContainerAsync<GenericImage>> =
    OnceCell::const_new();

#[dtor]
fn cleanup() {
    // Force container cleanup on process exit
    if let Some(container) = MYSQL_CONTAINER.get() {
        std::process::Command::new("docker")
            .args(["rm", "-f", container.id()])
            .output()
            .ok();
    }
}

/// Get or start the shared MySQL container.
async fn get_mysql_container() -> &'static ContainerAsync<GenericImage> {
    MYSQL_CONTAINER
        .get_or_init(|| async {
            info!("starting MySQL container...");

            let image = GenericImage::new("mysql", "8.4")
                .with_wait_for(WaitFor::message_on_stderr(
                    "ready for connections",
                ))
                .with_env_var("MYSQL_ROOT_PASSWORD", ROOT_PASSWORD)
                .with_cmd(vec![
                    "--server-id=999",
                    "--log-bin=/var/lib/mysql/mysql-bin.log",
                    "--binlog-format=ROW",
                    "--binlog-row-image=FULL",
                    "--gtid-mode=ON",
                    "--enforce-gtid-consistency=ON",
                    "--binlog-checksum=NONE",
                ])
                .with_mapped_port(MYSQL_PORT, 3306.tcp());

            let container = image.start().await.expect("start mysql container");
            info!("MySQL container started: {}", container.id());

            // Spawn log follower for debugging
            let mut stderr = container.stderr(true);
            tokio::spawn(async move {
                let mut line = String::new();
                loop {
                    line.clear();
                    match stderr.read_line(&mut line).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {
                            if line.contains("ERROR")
                                || line.contains("Warning")
                                || line.contains("ready for connections")
                            {
                                print!("[mysql] {}", line);
                            }
                        }
                    }
                }
            });

            // Wait for MySQL to be fully ready
            wait_for_mysql(&root_dsn(), Duration::from_secs(60))
                .await
                .expect("MySQL should be ready");

            // Create CDC user with replication privileges
            provision_cdc_user(&root_dsn())
                .await
                .expect("provision CDC user");

            container
        })
        .await
}

/// Poll MySQL until it's ready to accept connections.
async fn wait_for_mysql(dsn: &str, timeout_duration: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout_duration;
    let opts = Opts::from_url(dsn)?;
    let pool = MySQLPool::new(opts);

    while Instant::now() < deadline {
        if let Ok(mut conn) = pool.get_conn().await {
            if conn.query_drop("SELECT 1").await.is_ok() {
                let log_bin: Option<(String, String)> = conn
                    .query_first("SHOW VARIABLES LIKE 'log_bin'")
                    .await
                    .ok()
                    .flatten();

                if let Some((_, value)) = log_bin {
                    if value.eq_ignore_ascii_case("ON") {
                        info!("MySQL is ready (binlog enabled)");
                        return Ok(());
                    }
                }
            }
        }
        sleep(Duration::from_millis(500)).await;
    }

    anyhow::bail!("MySQL not ready after {:?}", timeout_duration)
}

/// Create CDC user with replication privileges (runs once per container).
async fn provision_cdc_user(root_dsn: &str) -> Result<()> {
    let opts = Opts::from_url(root_dsn)?;
    let pool = MySQLPool::new(opts);
    let mut conn = pool.get_conn().await?;

    conn.query_drop(format!(
        "CREATE USER IF NOT EXISTS '{}'@'%' IDENTIFIED BY '{}'",
        CDC_USER, CDC_PASSWORD
    ))
    .await?;

    conn.query_drop(format!(
        "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '{}'@'%'",
        CDC_USER
    ))
    .await?;

    conn.query_drop("FLUSH PRIVILEGES").await?;

    info!(
        "CDC user '{}' created with replication privileges",
        CDC_USER
    );
    Ok(())
}

/// Create a fresh test database with a unique name.
async fn create_test_database(test_name: &str) -> Result<(String, MySQLPool)> {
    let db_name = format!("test_{}", test_name.replace('-', "_"));
    let opts = Opts::from_url(&root_dsn())?;
    let pool = MySQLPool::new(opts);
    let mut conn = pool.get_conn().await?;

    conn.query_drop(format!("DROP DATABASE IF EXISTS {}", db_name))
        .await?;
    conn.query_drop(format!("CREATE DATABASE {}", db_name))
        .await?;
    conn.query_drop(format!(
        "GRANT SELECT, SHOW VIEW ON {}.* TO '{}'@'%'",
        db_name, CDC_USER
    ))
    .await?;

    debug!("created test database: {}", db_name);
    Ok((db_name, pool))
}

/// Drop test database (cleanup).
async fn drop_test_database(pool: &MySQLPool, db_name: &str) {
    if let Ok(mut conn) = pool.get_conn().await {
        let _ = conn
            .query_drop(format!("DROP DATABASE IF EXISTS {}", db_name))
            .await;
        debug!("dropped test database: {}", db_name);
    }
}

fn root_dsn() -> String {
    format!("mysql://root:{}@127.0.0.1:{}/", ROOT_PASSWORD, MYSQL_PORT)
}

fn cdc_dsn(db: &str) -> String {
    format!(
        "mysql://{}:{}@127.0.0.1:{}/{}",
        CDC_USER, CDC_PASSWORD, MYSQL_PORT, db
    )
}

/// Collect events until condition is met or timeout.
async fn collect_events_until<F>(
    rx: &mut mpsc::Receiver<Event>,
    timeout_duration: Duration,
    mut condition: F,
) -> Vec<Event>
where
    F: FnMut(&[Event]) -> bool,
{
    let deadline = Instant::now() + timeout_duration;
    let mut events = Vec::new();

    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match timeout(remaining, rx.recv()).await {
            Ok(Some(e)) => {
                debug!(op = ?e.op, table = %e.source.full_table_name(), "received event");
                events.push(e);
                if condition(&events) {
                    break;
                }
            }
            Ok(None) | Err(_) => break,
        }
    }

    events
}

/// Helper to check if event has a specific id.
fn event_has_id(e: &Event, id: i64) -> bool {
    e.after
        .as_ref()
        .and_then(|v| v.get("id"))
        .and_then(|v| v.as_i64())
        == Some(id)
        || e.before
            .as_ref()
            .and_then(|v| v.get("id"))
            .and_then(|v| v.as_i64())
            == Some(id)
}

/// Wait for source to be ready and verify it's running.
async fn wait_for_source_ready(
    handle: &SourceHandle,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;

    while Instant::now() < deadline {
        if handle.join.is_finished() {
            // Task died - this is a problem
            return Err(anyhow::anyhow!(
                "Source task died before becoming ready"
            ));
        }

        // Could also check for specific ready signal here
        sleep(Duration::from_millis(200)).await;
    }

    Ok(())
}

// =============================================================================
// Tests
// =============================================================================

/// Test schema loader: pattern expansion, column loading, fingerprinting, DDL detection.
#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_schema_loader() -> Result<()> {
    init_test_tracing();
    let _container = get_mysql_container().await;

    let (db_name, pool) = create_test_database("schema_loader").await?;
    let mut conn = pool.get_conn().await?;
    let dsn = cdc_dsn(&db_name);

    conn.query_drop(format!("USE {}", db_name)).await?;
    conn.query_drop(
        r#"CREATE TABLE orders (
            id INT PRIMARY KEY,
            sku VARCHAR(64),
            payload JSON,
            blobz BLOB
        )"#,
    )
    .await?;

    let registry = Arc::new(InMemoryRegistry::new());
    let schema_loader = MySqlSchemaLoader::new(&dsn, registry.clone(), "acme");

    // Test: expand exact pattern
    {
        let tables = schema_loader
            .expand_patterns(&[format!("{}.orders", db_name)])
            .await?;
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0], (db_name.clone(), "orders".to_string()));
        info!("✓ expand_patterns exact match");
    }

    // Test: load schema and verify columns
    {
        let loaded = schema_loader.load_schema(&db_name, "orders").await?;
        let schema = &loaded.schema;

        assert_eq!(schema.columns.len(), 4);
        assert_eq!(
            schema.column_names(),
            vec!["id", "sku", "payload", "blobz"]
        );
        assert_eq!(schema.primary_key, vec!["id".to_string()]);

        let id_col = schema.column("id").expect("id column");
        assert_eq!(id_col.data_type, "int");
        assert!(!id_col.nullable);

        let sku_col = schema.column("sku").expect("sku column");
        assert_eq!(sku_col.data_type, "varchar");
        assert!(sku_col.nullable);
        assert_eq!(sku_col.char_max_length, Some(64));

        info!("✓ load_schema returns correct columns");
    }

    // Test: fingerprint stability
    {
        let loaded1 = schema_loader.load_schema(&db_name, "orders").await?;
        let loaded2 = schema_loader.load_schema(&db_name, "orders").await?;
        assert_eq!(loaded1.fingerprint, loaded2.fingerprint);
        assert!(!loaded1.fingerprint.is_empty());
        info!("✓ fingerprint is stable");
    }

    // Test: schema registered in registry
    {
        let versions = registry.list_versions("acme", &db_name, "orders");
        assert!(!versions.is_empty());
        assert_eq!(versions[0].version, 1);
        info!("✓ schema registered in registry");
    }

    // Test: wildcard pattern expansion
    {
        conn.query_drop(
            r#"CREATE TABLE order_items (
                id INT PRIMARY KEY,
                order_id INT,
                product VARCHAR(128)
            )"#,
        )
        .await?;

        let tables = schema_loader
            .expand_patterns(&[format!("{}.order%", db_name)])
            .await?;

        assert!(tables.len() >= 2);
        assert!(tables.iter().any(|(db, t)| db == &db_name && t == "orders"));
        assert!(
            tables
                .iter()
                .any(|(db, t)| db == &db_name && t == "order_items")
        );
        info!("✓ wildcard pattern expansion");
    }

    // Test: schema reload detects DDL
    {
        let fp_before = schema_loader
            .load_schema(&db_name, "orders")
            .await?
            .fingerprint;

        conn.query_drop("ALTER TABLE orders ADD COLUMN notes TEXT")
            .await?;

        let reloaded = schema_loader.reload_schema(&db_name, "orders").await?;

        assert_ne!(fp_before, reloaded.fingerprint);
        assert_eq!(reloaded.schema.columns.len(), 5);
        assert!(reloaded.schema.column("notes").is_some());

        let versions = registry.list_versions("acme", &db_name, "orders");
        assert!(versions.len() >= 2);
        info!("✓ reload detects DDL changes");
    }

    // Test: cache behavior
    {
        let cached = schema_loader.list_cached().await;
        assert!(!cached.is_empty());
        assert!(
            cached
                .iter()
                .any(|e| e.database == db_name && e.table == "orders")
        );
        info!("✓ schema caching works");
    }

    drop_test_database(&pool, &db_name).await;
    Ok(())
}

/// Test basic CDC events: INSERT, UPDATE, DELETE with payload verification.
#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_cdc_basic_events() -> Result<()> {
    init_test_tracing();
    let _container = get_mysql_container().await;

    let (db_name, pool) = create_test_database("cdc_basic").await?;
    let mut conn = pool.get_conn().await?;
    let dsn = cdc_dsn(&db_name);

    conn.query_drop(format!("USE {}", db_name)).await?;
    conn.query_drop(
        r#"CREATE TABLE orders (
            id INT PRIMARY KEY,
            sku VARCHAR(64),
            payload JSON,
            blobz BLOB
        )"#,
    )
    .await?;

    let registry = Arc::new(InMemoryRegistry::new());
    let src = MySqlSource {
        id: "cdc-basic".into(),
        checkpoint_key: "mysql-cdc-basic".to_string(),
        dsn: dsn.clone(),
        tables: vec![format!("{}.orders", db_name)],
        tenant: "acme".into(),
        pipeline: "test".to_string(),
        registry: registry.clone(),
    };

    let ckpt_store: Arc<dyn CheckpointStore> =
        Arc::new(MemCheckpointStore::new()?);
    let (tx, mut rx) = mpsc::channel::<Event>(128);
    let handle = src.run(tx, ckpt_store).await;
    wait_for_source_ready(&handle, Duration::from_secs(10)).await?;

    sleep(Duration::from_secs(3)).await;

    // Perform DML
    conn.query_drop(
        r#"INSERT INTO orders (id, sku, payload, blobz) 
           VALUES (1, 'sku-1', '{"a":1}', X'DEADBEEF')"#,
    )
    .await?;

    conn.query_drop("UPDATE orders SET sku = 'sku-1b' WHERE id = 1")
        .await?;

    conn.query_drop("DELETE FROM orders WHERE id = 1").await?;

    // Collect events
    let events =
        collect_events_until(&mut rx, Duration::from_secs(15), |evts| {
            let has_insert = evts.iter().any(|e| e.op == Op::Create);
            let has_update = evts.iter().any(|e| e.op == Op::Update);
            let has_delete = evts.iter().any(|e| e.op == Op::Delete);
            has_insert && has_update && has_delete
        })
        .await;

    // Verify all operation types received
    assert!(
        events.iter().any(|e| e.op == Op::Create),
        "missing INSERT event"
    );
    assert!(
        events.iter().any(|e| e.op == Op::Update),
        "missing UPDATE event"
    );
    assert!(
        events.iter().any(|e| e.op == Op::Delete),
        "missing DELETE event"
    );

    // Filter to id=1 events
    let by_id: Vec<&Event> =
        events.iter().filter(|e| event_has_id(e, 1)).collect();
    assert_eq!(by_id.len(), 3, "expected 3 events for id=1");

    // Verify order: INSERT -> UPDATE -> DELETE
    let ops: Vec<Op> = by_id.iter().map(|e| e.op).collect();
    assert_eq!(ops, vec![Op::Create, Op::Update, Op::Delete]);

    // Verify INSERT payload
    let ins = by_id.iter().find(|e| e.op == Op::Create).unwrap();
    let after = ins.after.as_ref().expect("INSERT must have after");
    assert_eq!(after["id"], 1);
    assert_eq!(after["sku"], "sku-1");
    assert_eq!(after["payload"]["a"], 1);
    assert!(
        after["blobz"]["_base64"].is_string(),
        "BLOB should be base64 encoded"
    );
    info!("✓ INSERT payload correct");

    // Verify UPDATE payload
    let upd = by_id.iter().find(|e| e.op == Op::Update).unwrap();
    let upd_before = upd.before.as_ref().expect("UPDATE must have before");
    let upd_after = upd.after.as_ref().expect("UPDATE must have after");
    assert_eq!(upd_before["sku"], "sku-1");
    assert_eq!(upd_after["sku"], "sku-1b");
    info!("✓ UPDATE payload correct");

    // Verify DELETE payload
    let del = by_id.iter().find(|e| e.op == Op::Delete).unwrap();
    assert!(del.before.is_some(), "DELETE must have before");
    assert!(del.after.is_none(), "DELETE must not have after");
    assert_eq!(del.before.as_ref().unwrap()["id"], 1);
    info!("✓ DELETE payload correct");

    // Verify metadata on all events
    for e in &by_id {
        assert_eq!(e.source.full_table_name(), format!("{}.orders", db_name));
        assert_eq!(e.source.db, db_name);
        assert!(e.schema_version.is_some(), "missing schema_version");
        assert!(e.schema_sequence.is_some(), "missing schema_sequence");
        assert!(e.checkpoint.is_some(), "missing checkpoint");
    }
    info!("✓ event metadata correct");

    handle.stop();
    let _ = handle.join().await;

    drop_test_database(&pool, &db_name).await;
    Ok(())
}

/// Test schema reload when DDL occurs during CDC streaming.
#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_cdc_schema_reload_on_ddl() -> Result<()> {
    init_test_tracing();
    let _container = get_mysql_container().await;

    let (db_name, pool) = create_test_database("schema_ddl").await?;
    let mut conn = pool.get_conn().await?;
    let dsn = cdc_dsn(&db_name);

    conn.query_drop(format!("USE {}", db_name)).await?;
    conn.query_drop(
        r#"CREATE TABLE orders (
            id INT PRIMARY KEY,
            sku VARCHAR(64)
        )"#,
    )
    .await?;

    let registry = Arc::new(InMemoryRegistry::new());
    let src = MySqlSource {
        id: "schema-ddl".into(),
        checkpoint_key: "mysql-schema-ddl".to_string(),
        dsn: dsn.clone(),
        tables: vec![format!("{}.orders", db_name)],
        tenant: "acme".into(),
        pipeline: "test".to_string(),
        registry: registry.clone(),
    };

    let ckpt_store: Arc<dyn CheckpointStore> =
        Arc::new(MemCheckpointStore::new()?);
    let (tx, mut rx) = mpsc::channel::<Event>(128);
    let handle = src.run(tx, ckpt_store).await;
    wait_for_source_ready(&handle, Duration::from_secs(10)).await?;

    sleep(Duration::from_secs(3)).await;

    // Insert with original schema (2 columns)
    conn.query_drop("INSERT INTO orders (id, sku) VALUES (100, 'pre-ddl')")
        .await?;

    let events =
        collect_events_until(&mut rx, Duration::from_secs(10), |evts| {
            evts.iter().any(|e| event_has_id(e, 100))
        })
        .await;

    let pre_ddl = events
        .iter()
        .find(|e| event_has_id(e, 100))
        .expect("pre-DDL event");
    let schema_v1 = pre_ddl.schema_version.clone().expect("schema_version");
    info!("pre-DDL schema: {}", schema_v1);

    // ALTER TABLE - add column
    conn.query_drop(
        "ALTER TABLE orders ADD COLUMN status VARCHAR(32) DEFAULT 'pending'",
    )
    .await?;
    info!("executed ALTER TABLE");

    // Insert with new schema (3 columns)
    conn.query_drop("INSERT INTO orders (id, sku, status) VALUES (101, 'post-ddl', 'active')")
        .await?;

    let events =
        collect_events_until(&mut rx, Duration::from_secs(15), |evts| {
            evts.iter().any(|e| event_has_id(e, 101))
        })
        .await;

    let post_ddl = events
        .iter()
        .find(|e| event_has_id(e, 101))
        .expect("post-DDL event");

    // Verify new column present
    let after = post_ddl.after.as_ref().unwrap();
    assert!(
        after.get("status").is_some(),
        "post-DDL event should have 'status' column"
    );
    assert_eq!(after["status"], "active");

    // Verify schema version changed
    let schema_v2 = post_ddl.schema_version.clone().expect("schema_version");
    assert_ne!(
        schema_v1, schema_v2,
        "schema version should change after DDL"
    );
    info!("✓ schema version changed: {} -> {}", schema_v1, schema_v2);

    // Verify registry has multiple versions
    let versions = registry.list_versions("acme", &db_name, "orders");
    assert!(
        versions.len() >= 2,
        "registry should have at least 2 schema versions"
    );
    info!("✓ registry has {} versions", versions.len());

    handle.stop();
    let _ = handle.join().await;

    drop_test_database(&pool, &db_name).await;
    Ok(())
}

/// Test checkpoint persistence across source restarts.
#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_cdc_checkpoint_resume() -> Result<()> {
    init_test_tracing();
    let _container = get_mysql_container().await;

    let (db_name, pool) = create_test_database("checkpoint").await?;
    let mut conn = pool.get_conn().await?;
    let dsn = cdc_dsn(&db_name);

    conn.query_drop(format!("USE {}", db_name)).await?;
    conn.query_drop(
        r#"CREATE TABLE orders (
            id INT PRIMARY KEY,
            sku VARCHAR(64)
        )"#,
    )
    .await?;

    // Shared checkpoint store across restarts
    let ckpt_store: Arc<dyn CheckpointStore> =
        Arc::new(MemCheckpointStore::new()?);

    // First run
    info!("--- First run ---");
    {
        let (tx, mut rx) = mpsc::channel::<Event>(128);
        let src = MySqlSource {
            id: "ckpt-test".into(),
            checkpoint_key: "mysql-ckpt".to_string(),
            dsn: dsn.clone(),
            tables: vec![format!("{}.orders", db_name)],
            tenant: "acme".into(),
            pipeline: "test".to_string(),
            registry: Arc::new(InMemoryRegistry::new()),
        };

        let handle = src.run(tx, ckpt_store.clone()).await;
        sleep(Duration::from_secs(3)).await;

        conn.query_drop(
            "INSERT INTO orders (id, sku) VALUES (300, 'first-run')",
        )
        .await?;

        let events =
            collect_events_until(&mut rx, Duration::from_secs(10), |evts| {
                evts.iter().any(|e| event_has_id(e, 300))
            })
            .await;

        assert!(
            events.iter().any(|e| event_has_id(e, 300)),
            "first run should receive event"
        );
        info!("✓ first run received event for id=300");

        handle.stop();
        let _ = handle.join().await;
    }

    // Insert while source is down
    conn.query_drop("INSERT INTO orders (id, sku) VALUES (301, 'while-down')")
        .await?;
    info!("inserted id=301 while source was down");
    sleep(Duration::from_millis(500)).await;

    // Second run - should resume from checkpoint
    info!("--- Second run ---");
    {
        let (tx, mut rx) = mpsc::channel::<Event>(128);
        let src = MySqlSource {
            id: "ckpt-test".into(),
            checkpoint_key: "mysql-ckpt".to_string(),
            dsn: dsn.clone(),
            tables: vec![format!("{}.orders", db_name)],
            tenant: "acme".into(),
            pipeline: "test".to_string(),
            registry: Arc::new(InMemoryRegistry::new()),
        };

        let handle = src.run(tx, ckpt_store.clone()).await;

        let events =
            collect_events_until(&mut rx, Duration::from_secs(15), |evts| {
                evts.iter().any(|e| event_has_id(e, 301))
            })
            .await;

        assert!(
            events.iter().any(|e| event_has_id(e, 301)),
            "second run should receive event inserted while down"
        );
        info!("✓ second run received missed event for id=301");

        let count_300 = events.iter().filter(|e| event_has_id(e, 300)).count();
        if count_300 > 0 {
            warn!("received {} duplicate events for id=300", count_300);
        }

        handle.stop();
        let _ = handle.join().await;
    }

    drop_test_database(&pool, &db_name).await;
    Ok(())
}

/// Test reconnection after forced disconnect.
#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_cdc_reconnect_after_disconnect() -> Result<()> {
    init_test_tracing();
    let _container = get_mysql_container().await;

    let (db_name, pool) = create_test_database("reconnect").await?;
    let mut conn = pool.get_conn().await?;
    let dsn = cdc_dsn(&db_name);

    conn.query_drop(format!("USE {}", db_name)).await?;
    conn.query_drop(
        r#"CREATE TABLE orders (
            id INT PRIMARY KEY,
            sku VARCHAR(64)
        )"#,
    )
    .await?;

    let src = MySqlSource {
        id: "reconnect".into(),
        checkpoint_key: "mysql-reconnect".to_string(),
        dsn: dsn.clone(),
        tables: vec![format!("{}.orders", db_name)],
        tenant: "acme".into(),
        pipeline: "test".to_string(),
        registry: Arc::new(InMemoryRegistry::new()),
    };

    let ckpt_store: Arc<dyn CheckpointStore> =
        Arc::new(MemCheckpointStore::new()?);
    let (tx, mut rx) = mpsc::channel::<Event>(128);
    let handle = src.run(tx, ckpt_store).await;
    wait_for_source_ready(&handle, Duration::from_secs(10)).await?;

    sleep(Duration::from_secs(3)).await;

    // Insert before disconnect
    conn.query_drop(
        "INSERT INTO orders (id, sku) VALUES (200, 'before-disconnect')",
    )
    .await?;

    let events =
        collect_events_until(&mut rx, Duration::from_secs(10), |evts| {
            evts.iter().any(|e| event_has_id(e, 200))
        })
        .await;

    assert!(
        events.iter().any(|e| event_has_id(e, 200)),
        "should receive event before disconnect"
    );
    info!("✓ received event before disconnect");

    // Kill CDC user's connection
    let kill_result: Option<(u64,)> = conn
        .query_first(format!(
            "SELECT id FROM information_schema.processlist WHERE user = '{}' LIMIT 1",
            CDC_USER
        ))
        .await?;

    if let Some((pid,)) = kill_result {
        conn.query_drop(format!("KILL {}", pid)).await.ok();
        info!("killed CDC connection pid={}", pid);
    } else {
        warn!("no CDC connection found to kill");
    }

    sleep(Duration::from_secs(3)).await;

    // Insert after reconnect
    conn.query_drop(
        "INSERT INTO orders (id, sku) VALUES (201, 'after-reconnect')",
    )
    .await?;

    let events =
        collect_events_until(&mut rx, Duration::from_secs(20), |evts| {
            evts.iter().any(|e| event_has_id(e, 201))
        })
        .await;

    assert!(
        events.iter().any(|e| event_has_id(e, 201)),
        "should receive event after reconnect"
    );
    info!("✓ received event after reconnect");

    handle.stop();
    let _ = handle.join().await;

    drop_test_database(&pool, &db_name).await;
    Ok(())
}

/// Test that events for non-matching tables are filtered out.
#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_cdc_table_filtering() -> Result<()> {
    init_test_tracing();
    let _container = get_mysql_container().await;

    let (db_name, pool) = create_test_database("filtering").await?;
    let mut conn = pool.get_conn().await?;
    let dsn = cdc_dsn(&db_name);

    conn.query_drop(format!("USE {}", db_name)).await?;
    conn.query_drop(
        "CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(64))",
    )
    .await?;
    conn.query_drop(
        "CREATE TABLE audit_log (id INT PRIMARY KEY, msg VARCHAR(256))",
    )
    .await?;

    // Only subscribe to orders, not audit_log
    let src = MySqlSource {
        id: "filtering".into(),
        checkpoint_key: "mysql-filtering".to_string(),
        dsn: dsn.clone(),
        tables: vec![format!("{}.orders", db_name)],
        tenant: "acme".into(),
        pipeline: "test".to_string(),
        registry: Arc::new(InMemoryRegistry::new()),
    };

    let ckpt_store: Arc<dyn CheckpointStore> =
        Arc::new(MemCheckpointStore::new()?);
    let (tx, mut rx) = mpsc::channel::<Event>(128);
    let handle = src.run(tx, ckpt_store).await;
    wait_for_source_ready(&handle, Duration::from_secs(10)).await?;

    sleep(Duration::from_secs(3)).await;

    // Insert into both tables
    conn.query_drop(
        "INSERT INTO audit_log (id, msg) VALUES (1, 'should be filtered')",
    )
    .await?;
    conn.query_drop(
        "INSERT INTO orders (id, sku) VALUES (1, 'should be captured')",
    )
    .await?;

    let events =
        collect_events_until(&mut rx, Duration::from_secs(10), |evts| {
            evts.iter().any(|e| {
                e.source.full_table_name() == format!("{}.orders", db_name)
            })
        })
        .await;

    // Should only have orders events
    // No audit_log events should be captured
    assert!(
        events
            .iter()
            .all(|e| !e.source.full_table_name().contains("audit_log")),
        "audit_log should be filtered out"
    );

    // Orders events should be captured
    assert!(
        events
            .iter()
            .filter(|e| e.ddl.is_none())
            .all(
                |e| e.source.full_table_name() == format!("{}.orders", db_name)
            ),
        "only orders table should be captured"
    );

    info!("✓ table filtering works");

    handle.stop();
    let _ = handle.join().await;

    drop_test_database(&pool, &db_name).await;
    Ok(())
}
