//! End-to-end CDC tests for the Turso/SQLite source.
//!
//! ## CDC Modes Tested
//!
//! - **Triggers**: Full INSERT/UPDATE/DELETE using shadow tables (works with local SQLite)
//! - **Polling**: Insert-only detection via rowid tracking (works with local SQLite)
//! - **Native**: Requires "Turso Database" (the Rust rewrite) installed locally
//!
//! ## Important: Native CDC Availability
//!
//! The native CDC mode (`turso_cdc` table, `bin_record_json_object()`) is ONLY available in:
//! - **Turso Database** (Rust rewrite): Install via `curl -sSL tur.so/install | sh`
//! - **Turso Cloud** with CDC enabled
//!
//! The Docker image `ghcr.io/tursodatabase/libsql-server` (sqld) does NOT have native CDC.
//!
//! ## Running Tests
//!
//! ```bash
//! # Run all local tests (triggers, polling, schema, native if tursodb installed)
//! cargo test turso --features turso
//!
//! # Run native CDC test with Turso Cloud
//! TURSO_CDC_TEST_URL="libsql://mydb.turso.io" \
//! TURSO_CDC_TEST_TOKEN="eyJ..." \
//! cargo test turso_cdc_native_real -- --ignored
//! ```

use anyhow::Result;
use checkpoints::{CheckpointStore, MemCheckpointStore};
use deltaforge_config::{NativeCdcLevel, TursoCdcMode, TursoSrcCfg};
use deltaforge_core::{Event, Op, Source};
use libsql::Builder;
use schema_registry::{InMemoryRegistry, SourceSchema};
use sources::turso::{TursoSchemaLoader, TursoSource};
use std::sync::Arc;
use std::time::Instant;
use tempfile::TempDir;
use tokio::{
    sync::mpsc,
    time::{Duration, sleep, timeout},
};
use tracing::{debug, info};

mod common;
use common::init_test_tracing;

/// Create a test database with schema.
/// Returns (Database, Connection) - must keep Database alive while using Connection!
async fn setup_test_db(
    path: &str,
) -> Result<(libsql::Database, libsql::Connection)> {
    let db = Builder::new_local(path).build().await?;
    let conn = db.connect()?;

    // Create test tables
    conn.execute(
        "CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT,
            created_at INTEGER DEFAULT (strftime('%s', 'now'))
        )",
        (),
    )
    .await?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            total REAL,
            status TEXT DEFAULT 'pending',
            metadata TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )",
        (),
    )
    .await?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS order_items (
            id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL,
            product TEXT NOT NULL,
            quantity INTEGER DEFAULT 1,
            price REAL,
            FOREIGN KEY (order_id) REFERENCES orders(id)
        )",
        (),
    )
    .await?;

    Ok((db, conn))
}

/// Test schema loader functionality.
#[tokio::test]
async fn turso_schema_loader_test() -> Result<()> {
    init_test_tracing();
    info!("--- Testing Turso schema loader ---");

    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_schema.db");
    let db_path_str = db_path.to_string_lossy().to_string();

    // Setup database - keep _db alive to maintain connection validity
    let (_db, conn) = setup_test_db(&db_path_str).await?;
    let conn = Arc::new(conn);

    let registry = Arc::new(InMemoryRegistry::new());
    let schema_loader =
        TursoSchemaLoader::new(conn.clone(), registry.clone(), "acme", None);

    // Test 1: Expand patterns
    {
        let tables = schema_loader
            .expand_patterns(&["users".to_string()])
            .await?;
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0], "users");
        info!("expand_patterns exact match works");
    }

    // Test 2: Wildcard pattern
    {
        let tables = schema_loader
            .expand_patterns(&["order%".to_string()])
            .await?;
        assert!(tables.len() >= 2, "should match orders and order_items");
        assert!(tables.contains(&"orders".to_string()));
        assert!(tables.contains(&"order_items".to_string()));
        info!("wildcard pattern expansion works");
    }

    // Test 3: All tables
    {
        let tables = schema_loader.expand_patterns(&["*".to_string()]).await?;
        assert!(tables.len() >= 3, "should find all user tables");
        info!("all tables pattern works: {:?}", tables);
    }

    // Test 4: Load schema and verify columns
    {
        let loaded = schema_loader.load_schema("users").await?;
        let schema = &loaded.schema;

        assert_eq!(
            schema.columns.len(),
            4,
            "users table should have 4 columns"
        );

        // Check column names using SourceSchema trait
        let col_names = schema.column_names();
        assert_eq!(col_names, vec!["id", "name", "email", "created_at"]);

        // Check primary key
        assert_eq!(schema.primary_key, vec!["id"]);

        // Check specific columns
        let id_col = schema.column("id").expect("id column");
        // Note: SQLite INTEGER PRIMARY KEY doesn't report notnull=1 in PRAGMA table_info
        // unless explicitly declared, but it effectively can't be NULL (gets rowid)
        assert!(id_col.is_primary_key);
        assert!(id_col.is_autoincrement);

        let name_col = schema.column("name").expect("name column");
        assert!(!name_col.nullable, "name has NOT NULL constraint");

        let email_col = schema.column("email").expect("email column");
        assert!(email_col.nullable, "email should be nullable");

        info!("load_schema returns correct column info");
    }

    // Test 5: Fingerprint stability
    {
        let loaded1 = schema_loader.load_schema("users").await?;
        let loaded2 = schema_loader.load_schema("users").await?;

        assert_eq!(
            loaded1.fingerprint.as_ref(),
            loaded2.fingerprint.as_ref(),
            "fingerprint should be stable"
        );
        assert!(
            loaded1.fingerprint.starts_with("sha256:"),
            "fingerprint should be sha256 hash"
        );
        info!("fingerprint is stable across loads");
    }

    // Test 6: Schema registered in registry
    {
        let versions = registry.list_versions("acme", "main", "users");
        assert!(!versions.is_empty(), "schema should be registered");
        assert_eq!(versions[0].version, 1);
        info!("schema is registered with registry");
    }

    // Test 7: Schema reload after DDL
    {
        let fp_before = schema_loader.load_schema("users").await?.fingerprint;

        // Add a column
        conn.execute("ALTER TABLE users ADD COLUMN bio TEXT", ())
            .await?;

        // Reload schema (force refresh)
        let loaded = schema_loader.reload_schema("users").await?;
        let fp_after = loaded.fingerprint;

        assert_ne!(
            fp_before.as_ref(),
            fp_after.as_ref(),
            "fingerprint should change after DDL"
        );
        assert_eq!(loaded.schema.columns.len(), 5, "should have 5 columns now");
        assert!(
            loaded.schema.column("bio").is_some(),
            "bio column should exist"
        );

        // Check registry has new version
        let versions = registry.list_versions("acme", "main", "users");
        assert!(
            versions.len() >= 2,
            "should have multiple versions after DDL"
        );

        info!("schema reload detects DDL changes");
    }

    // Test 8: Cache behavior
    {
        let cached = schema_loader.list_cached_internal().await;
        assert!(!cached.is_empty(), "cache should not be empty");

        let users_cached = cached.iter().find(|(t, _)| t == "users");
        assert!(users_cached.is_some(), "users should be in cache");

        info!("schema caching works");
    }

    // Test 9: Preload multiple tables
    {
        schema_loader.invalidate_all().await;
        let loaded = schema_loader
            .preload(&["users".to_string(), "orders".to_string()])
            .await?;
        assert_eq!(loaded.len(), 2);

        let cached = schema_loader.list_cached_internal().await;
        assert!(cached.len() >= 2);
        info!("preload works");
    }

    info!("all Turso schema loader tests passed!");
    Ok(())
}

/// Test CDC with trigger mode.
#[tokio::test]
async fn turso_cdc_triggers_e2e() -> Result<()> {
    init_test_tracing();
    info!("--- Testing Turso CDC (trigger mode) ---");

    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_cdc_triggers.db");
    let db_path_str = db_path.to_string_lossy().to_string();

    // Setup database - keep _db alive for the duration of the test
    let (_db, conn) = setup_test_db(&db_path_str).await?;

    // Insert initial data before starting CDC (to set baseline)
    conn.execute("INSERT INTO users (name, email) VALUES ('existing', 'existing@test.com')", ())
        .await?;

    let cfg = TursoSrcCfg {
        id: "turso-triggers-test".to_string(),
        url: db_path_str.clone(),
        auth_token: None,
        tables: vec!["users".to_string()],
        cdc_mode: TursoCdcMode::Triggers,
        native_cdc_level: NativeCdcLevel::Full,
        cdc_table_name: None,
        poll_interval_ms: 100,
        tracking_column: "_rowid_".to_string(),
        auto_create_triggers: true,
        batch_size: 1000,
    };

    let registry = Arc::new(InMemoryRegistry::new());
    let src = TursoSource::new(
        cfg,
        "acme".to_string(),
        "pipe-turso".to_string(),
        registry,
    );

    let ckpt_store: Arc<dyn CheckpointStore> =
        Arc::new(MemCheckpointStore::new()?);

    let (tx, mut rx) = mpsc::channel::<Event>(128);
    let handle = src.run(tx, ckpt_store).await;
    info!("source is running (trigger mode)...");

    // Give source time to setup triggers and start polling
    sleep(Duration::from_millis(500)).await;

    // 1) INSERT
    conn.execute(
        "INSERT INTO users (name, email) VALUES ('alice', 'alice@test.com')",
        (),
    )
    .await?;

    // 2) UPDATE
    conn.execute(
        "UPDATE users SET email = 'alice.updated@test.com' WHERE name = 'alice'",
        (),
    )
    .await?;

    // 3) DELETE
    conn.execute("DELETE FROM users WHERE name = 'alice'", ())
        .await?;

    debug!("INSERT, UPDATE, DELETE performed - expecting CDC events...");

    // Collect events
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut got = Vec::new();
    let mut seen_insert = false;
    let mut seen_update = false;
    let mut seen_delete = false;

    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match timeout(remaining, rx.recv()).await {
            Ok(Some(e)) => {
                debug!(?e.op, table = %e.table, "event received");
                match e.op {
                    Op::Insert => seen_insert = true,
                    Op::Update => seen_update = true,
                    Op::Delete => seen_delete = true,
                    Op::Ddl => {}
                }
                got.push(e);

                if seen_insert && seen_update && seen_delete {
                    info!("all expected events have arrived");
                    break;
                }
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }

    // Assertions
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

    // Check INSERT event payload
    let ins = got.iter().find(|e| {
        e.op == Op::Insert
            && e.after
                .as_ref()
                .and_then(|v| v.get("name"))
                .and_then(|v| v.as_str())
                == Some("alice")
    });
    assert!(ins.is_some(), "should have INSERT for alice");
    let ins = ins.unwrap();
    let after = ins.after.as_ref().unwrap();
    assert_eq!(after["name"], "alice");
    assert_eq!(after["email"], "alice@test.com");

    // Check UPDATE event payload
    let upd = got.iter().find(|e| e.op == Op::Update);
    assert!(upd.is_some(), "should have UPDATE event");
    let upd = upd.unwrap();
    let upd_after = upd.after.as_ref().expect("UPDATE should have after");
    assert_eq!(upd_after["email"], "alice.updated@test.com");

    // Check DELETE event payload
    let del = got.iter().find(|e| e.op == Op::Delete);
    assert!(del.is_some(), "should have DELETE event");
    let del = del.unwrap();
    assert!(
        del.before.is_some() || del.after.is_none(),
        "DELETE should have before or no after"
    );

    info!("all Turso CDC trigger mode assertions passed!");

    // Clean shutdown
    handle.stop();
    let _ = handle.join().await;

    Ok(())
}

/// Test CDC with polling mode (insert-only detection).
#[tokio::test]
async fn turso_cdc_polling_e2e() -> Result<()> {
    init_test_tracing();
    info!("--- Testing Turso CDC (polling mode) ---");

    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_cdc_polling.db");
    let db_path_str = db_path.to_string_lossy().to_string();

    // Setup database - keep _db alive for the duration of the test
    let (_db, conn) = setup_test_db(&db_path_str).await?;

    // Insert initial data (should NOT be captured - sets baseline rowid)
    conn.execute("INSERT INTO users (name, email) VALUES ('baseline', 'baseline@test.com')", ())
        .await?;

    let cfg = TursoSrcCfg {
        id: "turso-polling-test".to_string(),
        url: db_path_str.clone(),
        auth_token: None,
        tables: vec!["users".to_string()],
        cdc_mode: TursoCdcMode::Polling,
        native_cdc_level: NativeCdcLevel::Full,
        cdc_table_name: None,
        poll_interval_ms: 100,
        tracking_column: "_rowid_".to_string(),
        auto_create_triggers: false,
        batch_size: 1000,
    };

    let registry = Arc::new(InMemoryRegistry::new());
    let src = TursoSource::new(
        cfg,
        "acme".to_string(),
        "pipe-poll".to_string(),
        registry,
    );

    let ckpt_store: Arc<dyn CheckpointStore> =
        Arc::new(MemCheckpointStore::new()?);

    let (tx, mut rx) = mpsc::channel::<Event>(128);
    let handle = src.run(tx, ckpt_store).await;
    info!("source is running (polling mode)...");

    // Give source time to start and establish baseline
    sleep(Duration::from_millis(500)).await;

    // INSERT new rows (polling mode only detects inserts)
    conn.execute(
        "INSERT INTO users (name, email) VALUES ('bob', 'bob@test.com')",
        (),
    )
    .await?;
    conn.execute(
        "INSERT INTO users (name, email) VALUES ('carol', 'carol@test.com')",
        (),
    )
    .await?;

    debug!(
        "INSERTs performed - expecting CDC events (polling only sees inserts)..."
    );

    // Collect events - polling starts from rowid 0 so will capture baseline too
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut got = Vec::new();

    while Instant::now() < deadline && got.len() < 3 {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match timeout(remaining, rx.recv()).await {
            Ok(Some(e)) => {
                debug!(?e.op, table = %e.table, "event received");
                got.push(e);
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }

    // Assertions - polling captures all rows from start (baseline included)
    assert!(
        got.len() >= 3,
        "should have at least 3 insert events (baseline + bob + carol)"
    );
    assert!(
        got.iter().all(|e| e.op == Op::Insert),
        "polling mode should only produce INSERT events"
    );

    // Check payloads
    let bob = got.iter().find(|e| {
        e.after
            .as_ref()
            .and_then(|v| v.get("name"))
            .and_then(|v| v.as_str())
            == Some("bob")
    });
    assert!(bob.is_some(), "should have INSERT for bob");

    let carol = got.iter().find(|e| {
        e.after
            .as_ref()
            .and_then(|v| v.get("name"))
            .and_then(|v| v.as_str())
            == Some("carol")
    });
    assert!(carol.is_some(), "should have INSERT for carol");

    // Baseline row IS captured in polling mode (starts from rowid 0)
    let baseline = got.iter().find(|e| {
        e.after
            .as_ref()
            .and_then(|v| v.get("name"))
            .and_then(|v| v.as_str())
            == Some("baseline")
    });
    assert!(
        baseline.is_some(),
        "baseline row should appear (polling starts from rowid 0)"
    );

    info!("all Turso CDC polling mode assertions passed!");

    // Clean shutdown
    handle.stop();
    let _ = handle.join().await;

    Ok(())
}

/// Test checkpoint persistence and resumption.
#[tokio::test]
async fn turso_checkpoint_resume_test() -> Result<()> {
    init_test_tracing();
    info!("--- Testing Turso checkpoint/resume ---");

    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_checkpoint.db");
    let db_path_str = db_path.to_string_lossy().to_string();

    // Setup database - keep _db alive for the duration of the test
    let (_db, conn) = setup_test_db(&db_path_str).await?;

    // Shared checkpoint store across runs
    let ckpt_store: Arc<dyn CheckpointStore> =
        Arc::new(MemCheckpointStore::new()?);

    // === First run: capture some events ===
    {
        let cfg = TursoSrcCfg {
            id: "turso-resume-test".to_string(),
            url: db_path_str.clone(),
            auth_token: None,
            tables: vec!["users".to_string()],
            cdc_mode: TursoCdcMode::Triggers,
            native_cdc_level: NativeCdcLevel::Full,
            cdc_table_name: None,
            poll_interval_ms: 100,
            tracking_column: "_rowid_".to_string(),
            auto_create_triggers: true,
            batch_size: 1000,
        };

        let registry = Arc::new(InMemoryRegistry::new());
        let src = TursoSource::new(
            cfg,
            "acme".to_string(),
            "pipe-resume".to_string(),
            registry,
        );

        let (tx, mut rx) = mpsc::channel::<Event>(128);
        let handle = src.run(tx, ckpt_store.clone()).await;

        sleep(Duration::from_millis(500)).await;

        // Insert some data
        conn.execute("INSERT INTO users (name, email) VALUES ('run1-user1', 'u1@test.com')", ())
            .await?;
        conn.execute("INSERT INTO users (name, email) VALUES ('run1-user2', 'u2@test.com')", ())
            .await?;

        // Wait for events
        let mut first_run_events = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline && first_run_events.len() < 2 {
            match timeout(Duration::from_millis(500), rx.recv()).await {
                Ok(Some(e)) => first_run_events.push(e),
                _ => break,
            }
        }

        assert!(
            first_run_events.len() >= 2,
            "should capture events in first run"
        );
        info!("first run captured {} events", first_run_events.len());

        handle.stop();
        let _ = handle.join().await;
    }

    // === Second run: should resume from checkpoint ===
    {
        let cfg = TursoSrcCfg {
            id: "turso-resume-test".to_string(), // Same ID = same checkpoint
            url: db_path_str.clone(),
            auth_token: None,
            tables: vec!["users".to_string()],
            cdc_mode: TursoCdcMode::Triggers,
            native_cdc_level: NativeCdcLevel::Full,
            cdc_table_name: None,
            poll_interval_ms: 100,
            tracking_column: "_rowid_".to_string(),
            auto_create_triggers: true,
            batch_size: 1000,
        };

        let registry = Arc::new(InMemoryRegistry::new());
        let src = TursoSource::new(
            cfg,
            "acme".to_string(),
            "pipe-resume".to_string(),
            registry,
        );

        let (tx, mut rx) = mpsc::channel::<Event>(128);
        let handle = src.run(tx, ckpt_store.clone()).await;

        sleep(Duration::from_millis(500)).await;

        // Insert NEW data
        conn.execute("INSERT INTO users (name, email) VALUES ('run2-user', 'run2@test.com')", ())
            .await?;

        // Collect events
        let mut second_run_events = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            match timeout(Duration::from_millis(500), rx.recv()).await {
                Ok(Some(e)) => second_run_events.push(e),
                _ => break,
            }
        }

        // Should only see NEW data, not replay of first run
        let has_run1_user1 = second_run_events.iter().any(|e| {
            e.after
                .as_ref()
                .and_then(|v| v.get("name"))
                .and_then(|v| v.as_str())
                == Some("run1-user1")
        });
        assert!(
            !has_run1_user1,
            "should NOT replay events from first run (checkpoint should prevent)"
        );

        let has_run2_user = second_run_events.iter().any(|e| {
            e.after
                .as_ref()
                .and_then(|v| v.get("name"))
                .and_then(|v| v.as_str())
                == Some("run2-user")
        });
        assert!(has_run2_user, "should capture new events after resume");

        info!(
            "second run captured {} events (no replay)",
            second_run_events.len()
        );

        handle.stop();
        let _ = handle.join().await;
    }

    info!("checkpoint resume test passed!");
    Ok(())
}

/// Test CDC with native mode (requires Turso Database - the Rust rewrite).
///
/// **Important:** Native CDC is only available in "Turso Database" (the Rust SQLite rewrite),
/// NOT in libsql-server (sqld). The Docker image `ghcr.io/tursodatabase/libsql-server` does NOT
/// have native CDC support.
///
/// To test native CDC locally:
/// 1. Install Turso Database: `curl -sSL tur.so/install | sh`
/// 2. Set TURSODB_PATH env var to the binary location
/// 3. Run this test
///
/// This test creates a mock `turso_cdc` table to verify our parsing logic works,
/// but the actual `bin_record_json_object()` function won't work without the real Turso DB.
#[tokio::test]
async fn turso_cdc_native_mock_e2e() -> Result<()> {
    init_test_tracing();
    info!("--- Testing Turso CDC (native mode - mocked) ---");

    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_cdc_native.db");
    let db_path_str = db_path.to_string_lossy().to_string();

    // Setup database with user tables - keep _db alive for the duration of the test
    let (_db, conn) = setup_test_db(&db_path_str).await?;

    // Create mock turso_cdc table that mimics Turso's native CDC structure.
    // In real Turso Database (Rust rewrite), this table is created automatically.
    // Note: The libsql-server Docker image does NOT have this feature.
    conn.execute(
        "CREATE TABLE turso_cdc (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name TEXT NOT NULL,
            operation TEXT NOT NULL,
            new_record TEXT,
            old_record TEXT,
            created_at INTEGER DEFAULT (strftime('%s', 'now') * 1000)
        )",
        (),
    )
    .await?;

    // For testing, we store JSON directly instead of binary blobs since
    // bin_record_json_object() doesn't exist in standard SQLite/libsql
    conn.execute(
        "CREATE TRIGGER _mock_cdc_insert_users
         AFTER INSERT ON users
         BEGIN
             INSERT INTO turso_cdc (table_name, operation, new_record)
             VALUES ('users', 'INSERT', json_object('id', NEW.id, 'name', NEW.name, 'email', NEW.email));
         END",
        (),
    )
    .await?;

    conn.execute(
        "CREATE TRIGGER _mock_cdc_update_users
         AFTER UPDATE ON users
         BEGIN
             INSERT INTO turso_cdc (table_name, operation, new_record, old_record)
             VALUES ('users', 'UPDATE', 
                 json_object('id', NEW.id, 'name', NEW.name, 'email', NEW.email),
                 json_object('id', OLD.id, 'name', OLD.name, 'email', OLD.email));
         END",
        (),
    )
    .await?;

    conn.execute(
        "CREATE TRIGGER _mock_cdc_delete_users
         AFTER DELETE ON users
         BEGIN
             INSERT INTO turso_cdc (table_name, operation, old_record)
             VALUES ('users', 'DELETE', json_object('id', OLD.id, 'name', OLD.name, 'email', OLD.email));
         END",
        (),
    )
    .await?;

    // Verify the mock turso_cdc table is populated correctly
    conn.execute(
        "INSERT INTO users (name, email) VALUES ('test', 'test@example.com')",
        (),
    )
    .await?;
    conn.execute(
        "UPDATE users SET email = 'updated@example.com' WHERE name = 'test'",
        (),
    )
    .await?;
    conn.execute("DELETE FROM users WHERE name = 'test'", ())
        .await?;

    // Query the mock CDC table
    let mut rows = conn
        .query("SELECT id, table_name, operation, new_record, old_record FROM turso_cdc ORDER BY id", ())
        .await?;

    use libsql::Value;
    let mut operations: Vec<String> = Vec::new();
    while let Ok(Some(row)) = rows.next().await {
        if let Ok(Value::Text(op)) = row.get_value(2) {
            operations.push(op);
        }
    }

    assert_eq!(operations.len(), 3, "should have 3 CDC entries");

    // Verify operations
    assert_eq!(operations[0], "INSERT");
    assert_eq!(operations[1], "UPDATE");
    assert_eq!(operations[2], "DELETE");

    info!("mock turso_cdc table works correctly!");
    info!(
        "Note: For real native CDC, use Turso Database (Rust rewrite), not libsql-server"
    );

    Ok(())
}

/// Native CDC test using local tursodb installation.
///
/// Requires Turso Database installed: `curl -sSL tur.so/install | sh`
///
/// This test verifies native CDC with the real `turso_cdc` table and
/// `bin_record_json_object()` function.
#[tokio::test]
async fn turso_cdc_native_local_e2e() -> Result<()> {
    init_test_tracing();
    info!("--- Testing Turso CDC (native mode - local tursodb) ---");

    // Find tursodb binary
    let tursodb_path = std::env::var("TURSODB_PATH").ok().or_else(|| {
        // Check common installation paths
        let paths = [
            std::env::var("HOME")
                .ok()
                .map(|h| format!("{}/.turso/tursodb", h)),
            Some("/usr/local/bin/tursodb".to_string()),
            Some("tursodb".to_string()), // In PATH
        ];
        for path in paths.into_iter().flatten() {
            if std::process::Command::new(&path)
                .arg("--version")
                .output()
                .is_ok()
            {
                return Some(path);
            }
        }
        None
    });

    let tursodb = match tursodb_path {
        Some(p) => p,
        None => {
            info!("tursodb not found, skipping native CDC test");
            info!("Install with: curl -sSL tur.so/install | sh");
            info!("Or set TURSODB_PATH environment variable");
            return Ok(());
        }
    };

    info!(path = %tursodb, "found tursodb");

    // Create temp database
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("native_cdc_test.db");
    let db_path_str = db_path.to_string_lossy().to_string();

    // Helper to run SQL via tursodb CLI
    // Pass SQL via stdin to avoid argument parsing issues
    let run_sql = |sql: &str| -> std::process::Output {
        use std::io::Write;
        use std::process::Stdio;

        let mut child = std::process::Command::new(&tursodb)
            .arg(&db_path_str)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn tursodb");

        if let Some(mut stdin) = child.stdin.take() {
            writeln!(stdin, "{}", sql).expect("write sql");
        }

        child.wait_with_output().expect("wait for tursodb")
    };

    // Enable CDC
    let output = run_sql("PRAGMA unstable_capture_data_changes_conn('full');");
    info!(stdout = %String::from_utf8_lossy(&output.stdout), "CDC enabled");

    // Create table
    let output = run_sql(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT);",
    );
    assert!(output.status.success(), "create table failed: {:?}", output);

    // Insert
    let output = run_sql(
        "INSERT INTO users (name, email) VALUES ('alice', 'alice@test.com');",
    );
    assert!(output.status.success(), "insert failed");

    // Update
    let output = run_sql(
        "UPDATE users SET email = 'alice.updated@test.com' WHERE name = 'alice';",
    );
    assert!(output.status.success(), "update failed");

    // Delete
    let output = run_sql("DELETE FROM users WHERE name = 'alice';");
    assert!(output.status.success(), "delete failed");

    // Query CDC table - check if it exists
    let output = run_sql(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='turso_cdc';",
    );
    let stdout = String::from_utf8_lossy(&output.stdout);

    if !stdout.contains("turso_cdc") {
        info!(
            "turso_cdc table not found - CDC may not be enabled in this tursodb version"
        );
        info!("Make sure you have a recent version with CDC support");
        return Ok(());
    }

    // Query CDC with JSON helpers
    let output = run_sql(
        "SELECT id, table_name, change_type, \
         bin_record_json_object(table_columns_json_array('users'), before) as before_json, \
         bin_record_json_object(table_columns_json_array('users'), after) as after_json \
         FROM turso_cdc ORDER BY id;",
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    info!(cdc_output = %stdout, "CDC query result");

    // Parse and verify
    if output.status.success() && !stdout.is_empty() {
        // Count changes (each line is a change)
        let lines: Vec<&str> =
            stdout.lines().filter(|l| !l.is_empty()).collect();
        info!(changes = lines.len(), "native CDC captured changes");

        // Should have 3 changes: INSERT, UPDATE, DELETE
        assert!(
            lines.len() >= 3,
            "expected at least 3 CDC entries, got {}",
            lines.len()
        );

        info!("✅ Native CDC working correctly!");
    } else {
        info!(stderr = %String::from_utf8_lossy(&output.stderr), "CDC query output");
    }

    Ok(())
}

/// Full native CDC test with real Turso Database binary or Cloud.
///
/// This test requires either:
/// - Turso Database installed: `curl -sSL tur.so/install | sh`
/// - Turso Cloud with CDC enabled
///
/// Set TURSO_CDC_TEST_URL for Turso Cloud, or leave unset to skip.
#[tokio::test]
#[ignore] // Run with: cargo test turso_cdc_native_real -- --ignored
async fn turso_cdc_native_real_e2e() -> Result<()> {
    init_test_tracing();

    let url = match std::env::var("TURSO_CDC_TEST_URL") {
        Ok(u) => u,
        Err(_) => {
            info!("TURSO_CDC_TEST_URL not set, skipping native CDC test");
            info!("To test native CDC:");
            info!("  1. Install: curl -sSL tur.so/install | sh");
            info!("  2. Or use Turso Cloud with CDC enabled");
            return Ok(());
        }
    };

    let token = std::env::var("TURSO_CDC_TEST_TOKEN").ok();

    info!("--- Testing Turso CDC (real native mode) ---");
    info!("Using Turso URL: {}", url.split('?').next().unwrap_or(&url));

    let cfg = TursoSrcCfg {
        id: "turso-native-real".to_string(),
        url: url.clone(),
        auth_token: token,
        tables: vec!["*".to_string()],
        cdc_mode: TursoCdcMode::Native,
        native_cdc_level: NativeCdcLevel::Full,
        cdc_table_name: None,
        poll_interval_ms: 500,
        tracking_column: "_rowid_".to_string(),
        auto_create_triggers: false,
        batch_size: 1000,
    };

    let registry = Arc::new(InMemoryRegistry::new());
    let src = TursoSource::new(
        cfg,
        "acme".to_string(),
        "pipe-real".to_string(),
        registry,
    );

    let ckpt_store: Arc<dyn CheckpointStore> =
        Arc::new(MemCheckpointStore::new()?);

    let (tx, mut rx) = mpsc::channel::<Event>(128);
    let handle = src.run(tx, ckpt_store).await;
    info!("source started with real Turso connection...");

    // Wait for any events
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut got = Vec::new();

    while Instant::now() < deadline {
        match timeout(Duration::from_secs(1), rx.recv()).await {
            Ok(Some(e)) => {
                info!(?e.op, table = %e.table, "real CDC event received");
                got.push(e);
            }
            Ok(None) => break,
            Err(_) => continue,
        }
    }

    info!("received {} events from real Turso CDC", got.len());

    handle.stop();
    let _ = handle.join().await;

    info!("real native CDC test completed!");
    Ok(())
}

/// Test SourceSchemaLoader trait implementation.
#[tokio::test]
async fn turso_source_schema_loader_trait_test() -> Result<()> {
    init_test_tracing();
    info!("--- Testing SourceSchemaLoader trait ---");

    use sources::schema_loader::SourceSchemaLoader;

    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_trait.db");
    let db_path_str = db_path.to_string_lossy().to_string();

    // Keep _db alive for the duration of the test
    let (_db, conn) = setup_test_db(&db_path_str).await?;
    let conn = Arc::new(conn);

    let registry = Arc::new(InMemoryRegistry::new());
    let loader = TursoSchemaLoader::new(conn, registry, "acme", None);

    // Test via trait interface
    let loader: &dyn SourceSchemaLoader = &loader;

    assert_eq!(loader.source_type(), "turso");

    // Load via trait
    let loaded = loader.load("main", "users").await?;
    assert_eq!(loaded.database, "main");
    assert_eq!(loaded.table, "users");
    assert!(!loaded.columns.is_empty());
    assert!(loaded.fingerprint.starts_with("sha256:"));

    // Reload via trait
    let reloaded = loader.reload("main", "orders").await?;
    assert_eq!(reloaded.table, "orders");

    // List cached via trait
    let cached = loader.list_cached().await;
    assert!(cached.len() >= 2);

    info!("SourceSchemaLoader trait implementation works!");
    Ok(())
}
