use anyhow::Result;
use checkpoints::{CheckpointStore, MemCheckpointStore};
use deltaforge_core::{Event, Op, Source};
use mysql_async::{Pool as MySQLPool, prelude::Queryable};
use schema_registry::{InMemoryRegistry, SourceSchema};
use sources::mysql::{MySqlSchemaLoader, MySqlSource};
use std::sync::Arc;
use std::time::Instant;
use testcontainers::runners::AsyncRunner;
use testcontainers::{
    GenericImage, ImageExt, core::IntoContainerPort, core::WaitFor,
};
use tokio::{
    io::AsyncBufReadExt,
    sync::mpsc,
    task,
    time::{Duration, sleep, timeout},
};
use tracing::{debug, info};

mod common;
use common::init_test_tracing;

/// End-to-end CDC test for the MySQL source.
///
/// This is intentionally a "fat" integration test:
/// - boots a real MySQL 8 container with GTID + ROW binlog
/// - provisions schema / user / privileges
/// - tests schema loading with nullable columns, wildcards, registry integration
/// - starts `MySqlSource` with an in-memory checkpoint store
/// - performs INSERT/UPDATE/DELETE
/// - asserts that corresponding CDC events are emitted with correct payloads.
///
/// NOTE: this test requires Docker and the ability to pull/run `mysql:8.4`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_cdc_end_to_end() -> Result<()> {
    init_test_tracing();

    // boot MySQL 8 with binlog/GTID
    let image = GenericImage::new("mysql", "8.4")
        .with_wait_for(WaitFor::message_on_stderr("ready for connections"))
        .with_env_var("MYSQL_ROOT_PASSWORD", "password")
        .with_cmd(vec![
            "--server-id=999",
            "--log-bin=/var/lib/mysql/mysql-bin.log",
            "--binlog-format=ROW",
            "--binlog-row-image=FULL",
            "--gtid-mode=ON",
            "--enforce-gtid-consistency=ON",
            "--binlog-checksum=NONE",
        ])
        // fixed host port keeps the DSN simple; tests are marked as E2E and
        // should not be run in parallel with other MySQL-using tests.
        .with_mapped_port(3307, 3306.tcp());

    let container = image.start().await.expect("start mysql");
    info!("Container ID: {}", container.id());

    // container log followers (helpful when this fails in CI)
    {
        let mut out = container.stdout(true); // follow=true
        task::spawn(async move {
            let mut line = String::new();
            loop {
                line.clear();
                match out.read_line(&mut line).await {
                    Ok(0) => break, // EOF, container stopped
                    Ok(_) => print!("STDOUT: {}", line),
                    Err(e) => {
                        eprint!("stdout read error: {e}");
                        break;
                    }
                }
            }
        });
    }
    {
        let mut err = container.stderr(true);
        task::spawn(async move {
            let mut line = String::new();
            loop {
                line.clear();
                match err.read_line(&mut line).await {
                    Ok(0) => break,
                    Ok(_) => print!("STDERR: {}", line),
                    Err(e) => {
                        eprint!("stderr read error: {e}");
                        break;
                    }
                }
            }
        });
    }

    // give MySQL a bit of time to finish init scripts.
    // we still validate that the required binlog settings are enabled below.
    sleep(Duration::from_secs(8)).await;
    info!("starting MySQL CDC e2e test ...");

    let port = 3307;
    let root_dsn = format!("mysql://root:password@127.0.0.1:{}/", port);
    let dsn = "mysql://df:dfpw@127.0.0.1:3307/shop";

    // provision schema + user
    let pool = MySQLPool::new(root_dsn.as_str());
    let mut conn = pool.get_conn().await?;

    // sanity-check that binlog + GTID are actually enabled in the container.
    let v: (String, String) = conn
        .query_first("SHOW VARIABLES LIKE 'log_bin'")
        .await?
        .unwrap();
    assert_eq!(v.1.to_uppercase(), "ON", "log_bin must be ON for CDC");

    let gtid: (String, String) = conn
        .query_first("SHOW VARIABLES LIKE 'gtid_mode'")
        .await?
        .unwrap();
    assert!(
        gtid.1.eq_ignore_ascii_case("ON"),
        "gtid_mode must be ON for CDC"
    );

    conn.query_drop("CREATE DATABASE IF NOT EXISTS shop")
        .await?;
    debug!("database `shop` created.");

    conn.query_drop("USE shop").await?;
    conn.query_drop(
        r#"
        CREATE TABLE IF NOT EXISTS orders(
            id INT PRIMARY KEY,
            sku VARCHAR(64),
            payload JSON,
            blobz BLOB
        )"#,
    )
    .await?;
    debug!("table `orders` created.");

    conn.query_drop(
        r#"CREATE USER IF NOT EXISTS 'df'@'%' IDENTIFIED BY 'dfpw'"#,
    )
    .await?;
    debug!("user `df` created");

    conn.query_drop(
        r#"GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'df'@'%'"#,
    )
    .await?;
    conn.query_drop(r#"GRANT SELECT, SHOW VIEW ON shop.* TO 'df'@'%'"#)
        .await?;
    conn.query_drop("FLUSH PRIVILEGES").await?;

    // =========================================================================
    // SCHEMA LOADER TESTS
    // =========================================================================
    info!("--- Testing schema loader ---");

    let registry = Arc::new(InMemoryRegistry::new());
    let schema_loader = MySqlSchemaLoader::new(dsn, registry.clone(), "acme");

    // Test 1: Expand exact pattern
    {
        let tables = schema_loader
            .expand_patterns(&["shop.orders".to_string()])
            .await?;
        assert_eq!(tables.len(), 1, "should find exactly one table");
        assert_eq!(tables[0], ("shop".to_string(), "orders".to_string()));
        info!("✓ expand_patterns exact match works");
    }

    // Test 2: Load schema and verify columns (tests NULL handling!)
    {
        let loaded = schema_loader.load_schema("shop", "orders").await?;
        let schema = &loaded.schema;

        assert_eq!(
            schema.columns.len(),
            4,
            "orders table should have 4 columns"
        );

        // Check column names using SourceSchema trait
        let col_names = schema.column_names();
        assert_eq!(col_names, vec!["id", "sku", "payload", "blobz"]);

        // Check primary key
        assert_eq!(schema.primary_key, vec!["id"]);

        // Check specific column types
        let id_col = schema.column("id").expect("id column");
        assert_eq!(id_col.data_type, "int");
        assert!(!id_col.nullable, "id should not be nullable (PK)");

        let sku_col = schema.column("sku").expect("sku column");
        assert_eq!(sku_col.data_type, "varchar");
        assert!(sku_col.nullable, "sku should be nullable");
        assert_eq!(sku_col.char_max_length, Some(64));

        let payload_col = schema.column("payload").expect("payload column");
        assert_eq!(payload_col.data_type, "json");

        let blob_col = schema.column("blobz").expect("blobz column");
        assert_eq!(blob_col.data_type, "blob");

        info!(
            "✓ load_schema returns correct column info (NULL handling works)"
        );
    }

    // Test 3: Schema fingerprint stability
    {
        let loaded1 = schema_loader.load_schema("shop", "orders").await?;
        let loaded2 = schema_loader.load_schema("shop", "orders").await?;

        assert_eq!(
            loaded1.fingerprint, loaded2.fingerprint,
            "fingerprint should be stable"
        );
        assert!(
            !loaded1.fingerprint.is_empty(),
            "fingerprint should not be empty"
        );
        info!("✓ fingerprint is stable across loads");
    }

    // Test 4: Schema registered in registry
    {
        let versions = registry.list_versions("acme", "shop", "orders");
        assert!(!versions.is_empty(), "schema should be registered");
        assert_eq!(versions[0].version, 1);
        info!("✓ schema is registered with registry");
    }

    // Test 5: Preload with wildcard patterns
    {
        // Create another table for wildcard testing
        conn.query_drop(
            r#"CREATE TABLE IF NOT EXISTS order_items(
                id INT PRIMARY KEY,
                order_id INT,
                product VARCHAR(64)
            )"#,
        )
        .await?;

        let tables =
            schema_loader.preload(&["shop.order%".to_string()]).await?;
        assert!(tables.len() >= 2, "should match orders and order_items");
        assert!(tables.iter().any(|(db, t)| db == "shop" && t == "orders"));
        assert!(
            tables
                .iter()
                .any(|(db, t)| db == "shop" && t == "order_items")
        );
        info!("✓ wildcard pattern expansion works");
    }

    // Test 6: Schema reload after DDL
    {
        let fp_before = schema_loader
            .load_schema("shop", "orders")
            .await?
            .fingerprint;

        // Add a column
        conn.query_drop("ALTER TABLE orders ADD COLUMN notes TEXT")
            .await?;

        // Reload schema (force refresh)
        let loaded = schema_loader.reload_schema("shop", "orders").await?;
        let fp_after = loaded.fingerprint;

        assert_ne!(fp_before, fp_after, "fingerprint should change after DDL");
        assert_eq!(loaded.schema.columns.len(), 5, "should have 5 columns now");
        assert!(
            loaded.schema.column("notes").is_some(),
            "notes column should exist"
        );

        // Check registry has new version
        let versions = registry.list_versions("acme", "shop", "orders");
        assert!(
            versions.len() >= 2,
            "should have multiple versions after DDL"
        );

        info!("✓ schema reload detects DDL changes");
    }

    // Test 7: Cache behavior
    {
        let cached = schema_loader.list_cached().await;
        assert!(!cached.is_empty(), "cache should not be empty");

        let orders_cached = cached
            .iter()
            .find(|((db, t), _)| db == "shop" && t == "orders");
        assert!(orders_cached.is_some(), "orders should be in cache");

        info!("✓ schema caching works");
    }

    // =========================================================================
    // CDC EVENT TESTS
    // =========================================================================
    info!("--- Testing CDC events ---");

    // start source with df
    let src = MySqlSource {
        id: "it-mysql".into(),
        dsn: dsn.to_string(),
        tables: vec!["shop.orders".into()],
        tenant: "acme".into(),
        pipeline: "pipe-2".to_string(),
        registry: Arc::new(InMemoryRegistry::new()),
    };

    let ckpt_store: Arc<dyn CheckpointStore> =
        Arc::new(MemCheckpointStore::new()?);

    let (tx, mut rx) = mpsc::channel::<Event>(128);
    let handle = src.run(tx, ckpt_store).await;
    info!("source is running ...");

    // give the source a small head-start so it can connect and subscribe
    sleep(Duration::from_secs(3)).await;

    // 1) INSERT
    conn.query_drop(
        r#"INSERT INTO orders (id, sku, payload, blobz) VALUES (1,'sku-1','{"a":1}', X'DEADBEEF')"#,
    )
    .await?;

    // 2) UPDATE
    conn.query_drop("UPDATE orders SET sku='sku-1b' WHERE id=1")
        .await?;

    // 3) DELETE
    conn.query_drop("DELETE FROM orders WHERE id=1").await?;
    debug!(
        "insert, update and delete performed on `orders` - expecting CDC events ..."
    );

    // expect at least 3 events (insert/update/delete for our row), but there
    // may be additional events from DDL or other internal activity. we collect
    // all events until we see the three operations we care about or a timeout.
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut got = Vec::new();
    let mut seen_insert = false;
    let mut seen_update = false;
    let mut seen_delete = false;

    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match timeout(remaining, rx.recv()).await {
            Ok(Some(e)) => {
                debug!(?e, "event received");
                match e.op {
                    Op::Insert => seen_insert = true,
                    Op::Update => seen_update = true,
                    Op::Delete => seen_delete = true,
                    Op::Ddl => {}
                }
                got.push(e);

                if seen_insert && seen_update && seen_delete {
                    info!("all expected events have arrived.");
                    break;
                }
            }
            Ok(None) => {
                // source ended: stop waiting
                break;
            }
            Err(_elapsed) => {
                // deadline hit: stop waiting
                break;
            }
        }
    }

    // --- structural assertions on the event stream ---

    // we must have seen each operation at least once.
    assert!(
        got.iter().any(|e| matches!(e.op, Op::Insert)),
        "expected at least one INSERT event"
    );
    assert!(
        got.iter().any(|e| matches!(e.op, Op::Update)),
        "expected at least one UPDATE event"
    );
    assert!(
        got.iter().any(|e| matches!(e.op, Op::Delete)),
        "expected at least one DELETE event"
    );

    // filter down to events related to id=1. For DELETE, the id will be in
    // `before`; for INSERT/UPDATE it's in `after`.
    let by_id: Vec<&Event> = got
        .iter()
        .filter(|e| {
            e.after
                .as_ref()
                .and_then(|v| v.get("id"))
                .and_then(|v| v.as_i64())
                .map(|id| id == 1)
                .unwrap_or(false)
                || e.before
                    .as_ref()
                    .and_then(|v| v.get("id"))
                    .and_then(|v| v.as_i64())
                    .map(|id| id == 1)
                    .unwrap_or(false)
        })
        .collect();

    assert_eq!(
        by_id.len(),
        3,
        "expected exactly 3 CDC events for id=1 (insert, update, delete); got {by_id_len}",
        by_id_len = by_id.len()
    );

    // preserve binlog order: for our single row this should be
    // INSERT -> UPDATE -> DELETE.
    let ops: Vec<Op> = by_id.iter().map(|e| e.op).collect();
    assert_eq!(
        ops,
        vec![Op::Insert, Op::Update, Op::Delete],
        "unexpected operation order for id=1"
    );

    // --- payload assertions on the INSERT event (JSON + BLOB) ---

    let ins = by_id
        .iter()
        .find(|e| matches!(e.op, Op::Insert))
        .expect("missing INSERT event for id=1");
    let after = ins.after.as_ref().expect("INSERT must have `after`");
    assert_eq!(after["id"], 1);
    assert_eq!(after["sku"], "sku-1");
    assert_eq!(after["payload"]["a"], 1);
    assert!(
        after["blobz"]["_base64"].is_string(),
        "BLOB field should be base64-wrapped"
    );

    // assert update event shape
    let upd = by_id
        .iter()
        .find(|e| matches!(e.op, Op::Update))
        .expect("missing UPDATE event for id=1");
    let upd_before = upd.before.as_ref().expect("UPDATE must have `before`");
    let upd_after = upd.after.as_ref().expect("UPDATE must have `after`");
    assert_eq!(upd_before["id"], 1);
    assert_eq!(upd_before["sku"], "sku-1");
    assert_eq!(upd_after["id"], 1);
    assert_eq!(upd_after["sku"], "sku-1b");

    // check del event shape
    let del = by_id
        .iter()
        .find(|e| matches!(e.op, Op::Delete))
        .expect("missing DELETE event for id=1");
    let del_before = del.before.as_ref().expect("DELETE must have `before`");
    assert_eq!(del_before["id"], 1);
    assert!(del.after.is_none(), "DELETE must not have `after`");

    for e in &by_id {
        assert_eq!(e.table, "shop.orders");
        assert_eq!(e.source.db, "shop");
    }

    info!("all MySQL CDC e2e assertions are successful!");

    // clean shutdown for source
    handle.stop();
    let _ = handle.join().await;

    conn.disconnect().await?;
    pool.disconnect().await?;

    Ok(())
}
