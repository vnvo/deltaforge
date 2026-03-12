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
use common::AllowList;
use ctor::dtor;
use deltaforge_config::SnapshotCfg;
use deltaforge_core::{BatchContext, Event, Op, Source, SourceHandle};
use mysql_async::prelude::Queryable;
use schema_registry::SourceSchema;
use sources::SourceSchemaLoader;
use sources::mysql::{MySqlSchemaLoader, MySqlSource};
use std::sync::Arc;
use std::time::Instant;
use tokio::{
    sync::mpsc,
    time::{Duration, sleep, timeout},
};
use tracing::{debug, info, warn};

mod test_common;
use test_common::{MYSQL_CDC_USER, make_registry, mysql_drop_db, mysql_setup};

use crate::test_common::make_storage_backend;

#[dtor]
fn cleanup() {
    // Force container cleanup on process exit
    if let Some(container) = test_common::MYSQL_CONTAINER.get() {
        std::process::Command::new("docker")
            .args(["rm", "-f", container.0.id()])
            .output()
            .ok();
    }
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
// Test Helpers
// =============================================================================

/// Build a MySqlSource with constant defaults (tenant=acme, pipeline=test,
/// fresh InMemoryRegistry). `id` is also used as the checkpoint key suffix.
async fn make_source(
    id: &str,
    dsn: &str,
    tables: Vec<String>,
    outbox_tables: AllowList,
) -> MySqlSource {
    MySqlSource {
        id: id.into(),
        checkpoint_key: format!("mysql-{}", id),
        dsn: dsn.to_string(),
        tables,
        tenant: "acme".into(),
        pipeline: "test".to_string(),
        registry: make_registry().await,
        outbox_tables,
        snapshot_cfg: SnapshotCfg::default(),
        backend: make_storage_backend().await,
        on_schema_drift: deltaforge_config::OnSchemaDrift::Adapt,
    }
}

/// Start a source with a fresh checkpoint store, wait for ready, then warm up
/// for 3 seconds. Returns (rx, handle).
async fn start_source(
    src: MySqlSource,
) -> Result<(mpsc::Receiver<Event>, SourceHandle)> {
    let ckpt_store: Arc<dyn CheckpointStore> =
        Arc::new(MemCheckpointStore::new()?);
    let (tx, rx) = mpsc::channel::<Event>(128);
    let handle = src.run(tx, ckpt_store).await;
    wait_for_source_ready(&handle, Duration::from_secs(10)).await?;
    sleep(Duration::from_secs(3)).await;
    Ok((rx, handle))
}

// =============================================================================
// Tests
// =============================================================================

/// Test schema loader: pattern expansion, column loading, fingerprinting, DDL detection.
#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_schema_loader() -> Result<()> {
    let (db_name, pool, dsn) = mysql_setup("schema_loader").await?;
    let mut conn = pool.get_conn().await?;
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

    let registry = make_registry().await;
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

    mysql_drop_db(&pool, &db_name).await;
    Ok(())
}

/// Test basic CDC events: INSERT, UPDATE, DELETE with payload verification.
#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_cdc_basic_events() -> Result<()> {
    let (db_name, pool, dsn) = mysql_setup("cdc_basic").await?;
    let mut conn = pool.get_conn().await?;
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

    let registry = make_registry().await;
    let backend = make_storage_backend().await;
    let src = MySqlSource {
        id: "cdc-basic".into(),
        checkpoint_key: "mysql-cdc-basic".to_string(),
        dsn: dsn.clone(),
        tables: vec![format!("{}.orders", db_name)],
        tenant: "acme".into(),
        pipeline: "test".to_string(),
        registry: registry.clone(),
        outbox_tables: AllowList::default(),
        snapshot_cfg: SnapshotCfg::default(),
        backend,
        on_schema_drift: deltaforge_config::OnSchemaDrift::Adapt,
    };
    let (mut rx, handle) = start_source(src).await?;

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

    mysql_drop_db(&pool, &db_name).await;
    Ok(())
}

/// Test schema reload when DDL occurs during CDC streaming.
#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_cdc_schema_reload_on_ddl() -> Result<()> {
    let (db_name, pool, dsn) = mysql_setup("schema_ddl").await?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop(format!("USE {}", db_name)).await?;
    conn.query_drop(
        r#"CREATE TABLE orders (
            id INT PRIMARY KEY,
            sku VARCHAR(64)
        )"#,
    )
    .await?;

    let registry = make_registry().await;
    let src = MySqlSource {
        id: "schema-ddl".into(),
        checkpoint_key: "mysql-schema-ddl".to_string(),
        dsn: dsn.clone(),
        tables: vec![format!("{}.orders", db_name)],
        tenant: "acme".into(),
        pipeline: "test".to_string(),
        registry: registry.clone(),
        outbox_tables: AllowList::default(),
        snapshot_cfg: SnapshotCfg::default(),
        backend: make_storage_backend().await,
        on_schema_drift: deltaforge_config::OnSchemaDrift::Adapt,
    };
    let (mut rx, handle) = start_source(src).await?;

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

    mysql_drop_db(&pool, &db_name).await;
    Ok(())
}

/// Test checkpoint persistence across source restarts.
#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_cdc_checkpoint_resume() -> Result<()> {
    let (db_name, pool, dsn) = mysql_setup("checkpoint").await?;
    let mut conn = pool.get_conn().await?;
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
        let src = make_source(
            "ckpt-test",
            &dsn,
            vec![format!("{}.orders", db_name)],
            AllowList::default(),
        )
        .await;

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
        let src = make_source(
            "ckpt-test",
            &dsn,
            vec![format!("{}.orders", db_name)],
            AllowList::default(),
        )
        .await;

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

    mysql_drop_db(&pool, &db_name).await;
    Ok(())
}

/// Test reconnection after forced disconnect.
#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_cdc_reconnect_after_disconnect() -> Result<()> {
    let (db_name, pool, dsn) = mysql_setup("reconnect").await?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop(format!("USE {}", db_name)).await?;
    conn.query_drop(
        r#"CREATE TABLE orders (
            id INT PRIMARY KEY,
            sku VARCHAR(64)
        )"#,
    )
    .await?;

    let src = make_source(
        "reconnect",
        &dsn,
        vec![format!("{}.orders", db_name)],
        AllowList::default(),
    )
    .await;
    let (mut rx, handle) = start_source(src).await?;

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
            MYSQL_CDC_USER
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

    mysql_drop_db(&pool, &db_name).await;
    Ok(())
}

/// Test that events for non-matching tables are filtered out.
#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_cdc_table_filtering() -> Result<()> {
    let (db_name, pool, dsn) = mysql_setup("filtering").await?;
    let mut conn = pool.get_conn().await?;
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
    let src = make_source(
        "filtering",
        &dsn,
        vec![format!("{}.orders", db_name)],
        AllowList::default(),
    )
    .await;
    let (mut rx, handle) = start_source(src).await?;

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

    mysql_drop_db(&pool, &db_name).await;
    Ok(())
}

// =============================================================================
// OUTBOX PATTERN TESTS
// =============================================================================

/// Test outbox capture via MySQL table matching.
/// Verifies:
/// - Matching table -> source.schema = "__outbox"
/// - Non-matching table -> normal CDC event (no sentinel)
/// - Outbox JSON payload arrives in event.after
#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_cdc_outbox_capture() -> Result<()> {
    let (db_name, pool, dsn) = mysql_setup("outbox").await?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop(format!("USE {}", db_name)).await?;
    conn.query_drop(
        r#"CREATE TABLE outbox (
            id INT AUTO_INCREMENT PRIMARY KEY,
            aggregate_type VARCHAR(64),
            aggregate_id VARCHAR(64),
            event_type VARCHAR(64),
            payload JSON
        )"#,
    )
    .await?;
    conn.query_drop(
        "CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(64))",
    )
    .await?;

    let src = make_source(
        "outbox",
        &dsn,
        vec![format!("{}.outbox", db_name), format!("{}.orders", db_name)],
        AllowList::new(&[format!("{}.outbox", db_name)]),
    )
    .await;
    let (mut rx, handle) = start_source(src).await?;

    // Insert outbox event
    conn.query_drop(
        r#"INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload)
           VALUES ('Order', '42', 'OrderCreated', '{"total": 99.99}')"#,
    )
    .await?;

    // Insert normal table row
    conn.query_drop("INSERT INTO orders (id, sku) VALUES (1, 'sku-1')")
        .await?;

    let events =
        collect_events_until(&mut rx, Duration::from_secs(15), |evts| {
            let has_outbox = evts.iter().any(|e| e.source.table == "outbox");
            let has_order = evts.iter().any(|e| e.source.table == "orders");
            has_outbox && has_order
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
        "matching table should be tagged __outbox"
    );
    assert_eq!(outbox_ev.op, Op::Create);
    let after = outbox_ev.after.as_ref().expect("should have payload");
    assert_eq!(after["aggregate_type"], "Order");
    assert_eq!(after["aggregate_id"], "42");
    assert_eq!(after["event_type"], "OrderCreated");
    info!("✓ outbox event captured with __outbox sentinel");

    // --- Normal table event ---
    let order_ev = events
        .iter()
        .find(|e| e.source.table == "orders")
        .expect("should have orders event");
    assert_ne!(
        order_ev.source.schema.as_deref(),
        Some("__outbox"),
        "non-outbox table should not be tagged"
    );
    assert_eq!(order_ev.after.as_ref().unwrap()["sku"], "sku-1");
    info!("✓ normal table CDC coexists with outbox capture");

    handle.stop();
    let _ = handle.join().await;
    mysql_drop_db(&pool, &db_name).await;
    Ok(())
}

/// Test outbox with wildcard table patterns.
/// `*.outbox` should match outbox tables across databases.
#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_cdc_outbox_wildcard_tables() -> Result<()> {
    let (db_name, pool, dsn) = mysql_setup("outbox_wild").await?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop(format!("USE {}", db_name)).await?;
    conn.query_drop(
        r#"CREATE TABLE outbox (
            id INT AUTO_INCREMENT PRIMARY KEY,
            aggregate_type VARCHAR(64),
            event_type VARCHAR(64),
            payload JSON
        )"#,
    )
    .await?;
    conn.query_drop(
        "CREATE TABLE audit_log (id INT PRIMARY KEY, msg VARCHAR(256))",
    )
    .await?;

    let src = make_source(
        "outbox_wild",
        &dsn,
        vec![
            format!("{}.outbox", db_name),
            format!("{}.audit_log", db_name),
        ],
        AllowList::new(&["*.outbox".to_string()]),
    )
    .await;
    let (mut rx, handle) = start_source(src).await?;

    conn.query_drop(
        r#"INSERT INTO outbox (aggregate_type, event_type, payload)
           VALUES ('Order', 'Created', '{"id": 1}')"#,
    )
    .await?;
    conn.query_drop("INSERT INTO audit_log (id, msg) VALUES (1, 'login')")
        .await?;

    let events =
        collect_events_until(&mut rx, Duration::from_secs(15), |evts| {
            let has_outbox = evts.iter().any(|e| e.source.table == "outbox");
            let has_audit = evts.iter().any(|e| e.source.table == "audit_log");
            has_outbox && has_audit
        })
        .await;

    let outbox_ev = events.iter().find(|e| e.source.table == "outbox").unwrap();
    let audit_ev = events
        .iter()
        .find(|e| e.source.table == "audit_log")
        .unwrap();

    assert_eq!(outbox_ev.source.schema.as_deref(), Some("__outbox"));
    assert_ne!(audit_ev.source.schema.as_deref(), Some("__outbox"));
    info!("✓ wildcard *.outbox matches outbox but not audit_log");

    handle.stop();
    let _ = handle.join().await;
    mysql_drop_db(&pool, &db_name).await;
    Ok(())
}

/// Test full outbox pipeline: source capture → OutboxProcessor → transformed event.
#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_cdc_outbox_full_pipeline() -> Result<()> {
    use deltaforge_config::{
        OUTBOX_SCHEMA_SENTINEL, OutboxColumns, OutboxProcessorCfg,
    };
    use deltaforge_core::Processor;
    use processors::OutboxProcessor;
    use std::collections::HashMap;

    let (db_name, pool, dsn) = mysql_setup("outbox_pipe").await?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop(format!("USE {}", db_name)).await?;
    conn.query_drop(
        r#"CREATE TABLE outbox (
            id INT AUTO_INCREMENT PRIMARY KEY,
            aggregate_type VARCHAR(64),
            aggregate_id VARCHAR(64),
            event_type VARCHAR(64),
            payload JSON
        )"#,
    )
    .await?;
    conn.query_drop(
        "CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(64))",
    )
    .await?;

    let src = make_source(
        "outbox_pipe",
        &dsn,
        vec![format!("{}.outbox", db_name), format!("{}.orders", db_name)],
        AllowList::new(&[format!("{}.outbox", db_name)]),
    )
    .await;
    let (mut rx, handle) = start_source(src).await?;

    conn.query_drop(
        r#"INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload)
           VALUES ('Order', '42', 'OrderCreated', '{"order_id": 42, "total": 99.99}')"#,
    )
    .await?;
    conn.query_drop("INSERT INTO orders (id, sku) VALUES (1, 'sku-1')")
        .await?;

    let raw_events =
        collect_events_until(&mut rx, Duration::from_secs(15), |evts| {
            let has_outbox = evts.iter().any(|e| {
                e.source.schema.as_deref() == Some(OUTBOX_SCHEMA_SENTINEL)
            });
            let has_table = evts.iter().any(|e| e.source.table == "orders");
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
    let after = outbox_ev.after.as_ref().unwrap();
    assert_eq!(after["order_id"], 42);
    assert_eq!(after["total"], 99.99);
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
        .find(|e| e.source.table == "orders")
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
        .find(|e| e.source.table == "orders")
        .expect("table event should pass through in raw mode");
    assert!(
        raw_table_ev.routing.as_ref().is_none_or(|r| !r.raw_payload),
        "raw_payload flag should NOT be set on table event"
    );
    info!("✓ raw_payload flag set on outbox, not on table event");

    handle.stop();
    let _ = handle.join().await;
    mysql_drop_db(&pool, &db_name).await;
    Ok(())
}
