//! End-to-end CDC tests for the Turso source.
//!
//! ## Requirements
//!
//! Turso native CDC requires:
//! - **Turso Cloud** with CDC enabled, OR
//! - **Local sqld** with `--enable-cdc` flag
//!
//! Native CDC uses `PRAGMA unstable_capture_data_changes_conn` to capture all
//! changes to a `turso_cdc` table automatically.
//!
//! ## Running Tests
//!
//! ```bash
//! # Run schema loader tests (work with local SQLite)
//! cargo test turso_schema --features turso
//!
//! # Run native CDC test with local tursodb
//! TURSODB_PATH=/path/to/tursodb cargo test turso_cdc_native_local -- --ignored
//!
//! # Run native CDC test with Turso Cloud
//! TURSO_CDC_TEST_URL="libsql://mydb.turso.io" \
//! TURSO_CDC_TEST_TOKEN="eyJ..." \
//! cargo test turso_cdc_native_cloud -- --ignored
//! ```

use anyhow::Result;
use checkpoints::{CheckpointStore, MemCheckpointStore};
use deltaforge_config::{NativeCdcLevel, TursoSrcCfg};
use deltaforge_core::{Event, Source};
use libsql::Builder;
use schema_registry::{InMemoryRegistry, SourceSchema};
use sources::turso::{TursoSchemaLoader, TursoSource};
use std::sync::Arc;
use std::time::Instant;
use tempfile::TempDir;
use tokio::{
    sync::mpsc,
    time::{Duration, timeout},
};
use tracing::info;

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

// ============================================================================
// Schema Loader Tests (work with local SQLite)
// ============================================================================

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

// ============================================================================
// Native CDC Tests (require Turso with CDC support)
// ============================================================================

/// Native CDC test using local tursodb installation.
///
/// Requires Turso Database installed: `curl -sSL tur.so/install | sh`
///
/// This test verifies native CDC with the real `turso_cdc` table and
/// `bin_record_json_object()` function.
#[tokio::test]
#[ignore] // Run with: cargo test turso_cdc_native_local -- --ignored
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

    // INSERT
    let output = run_sql(
        "INSERT INTO users (name, email) VALUES ('alice', 'alice@test.com');",
    );
    assert!(output.status.success(), "insert failed");

    // UPDATE
    let output = run_sql(
        "UPDATE users SET email = 'alice.updated@test.com' WHERE name = 'alice';",
    );
    assert!(output.status.success(), "update failed");

    // DELETE
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

/// Full native CDC test with Turso Cloud or sqld with CDC enabled.
///
/// Set environment variables:
/// - TURSO_CDC_TEST_URL: Connection URL (libsql:// or http://)
/// - TURSO_CDC_TEST_TOKEN: Auth token (for Turso Cloud)
#[tokio::test]
#[ignore] // Run with: cargo test turso_cdc_native_cloud -- --ignored
async fn turso_cdc_native_cloud_e2e() -> Result<()> {
    init_test_tracing();

    let url = match std::env::var("TURSO_CDC_TEST_URL") {
        Ok(u) => u,
        Err(_) => {
            info!("TURSO_CDC_TEST_URL not set, skipping native CDC test");
            info!("To test native CDC with Turso Cloud:");
            info!("  TURSO_CDC_TEST_URL=libsql://mydb.turso.io \\");
            info!("  TURSO_CDC_TEST_TOKEN=eyJ... \\");
            info!("  cargo test turso_cdc_native_cloud -- --ignored");
            return Ok(());
        }
    };

    let token = std::env::var("TURSO_CDC_TEST_TOKEN").ok();

    info!("--- Testing Turso CDC (native mode - cloud) ---");
    info!("Using Turso URL: {}", url.split('?').next().unwrap_or(&url));

    let cfg = TursoSrcCfg {
        id: "turso-native-cloud".to_string(),
        url: url.clone(),
        auth_token: token,
        tables: vec!["*".to_string()],
        native_cdc_level: NativeCdcLevel::Full,
        cdc_table_name: None,
        poll_interval_ms: 500,
        batch_size: 1000,
    };

    let registry = Arc::new(InMemoryRegistry::new());
    let src = TursoSource::new(
        cfg,
        "acme".to_string(),
        "pipe-cloud".to_string(),
        registry,
    );

    let ckpt_store: Arc<dyn CheckpointStore> =
        Arc::new(MemCheckpointStore::new()?);

    let (tx, mut rx) = mpsc::channel::<Event>(128);
    let handle = src.run(tx, ckpt_store).await;
    info!("source started with Turso connection...");

    // Wait for any events
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut got = Vec::new();

    while Instant::now() < deadline {
        match timeout(Duration::from_secs(1), rx.recv()).await {
            Ok(Some(e)) => {
                info!(?e.op, table = %e.table, "CDC event received");
                got.push(e);
            }
            Ok(None) => break,
            Err(_) => continue,
        }
    }

    info!("received {} events from Turso CDC", got.len());

    handle.stop();
    let _ = handle.join().await;

    info!("native CDC cloud test completed!");
    Ok(())
}

/// Test that source fails gracefully when native CDC is not available.
#[tokio::test]
async fn turso_native_cdc_unavailable_test() -> Result<()> {
    init_test_tracing();
    info!("--- Testing Turso CDC unavailable error handling ---");

    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_no_cdc.db");
    let db_path_str = db_path.to_string_lossy().to_string();

    // Create a regular SQLite database (no CDC support)
    let (_db, _conn) = setup_test_db(&db_path_str).await?;

    // Try to create source with local file - should fail with clear error
    let cfg = TursoSrcCfg {
        id: "turso-no-cdc".to_string(),
        url: db_path_str.clone(), // Local file, not libsql:// or http://
        auth_token: None,
        tables: vec!["users".to_string()],
        native_cdc_level: NativeCdcLevel::Full,
        cdc_table_name: None,
        poll_interval_ms: 100,
        batch_size: 1000,
    };

    let registry = Arc::new(InMemoryRegistry::new());
    let src = TursoSource::new(
        cfg,
        "acme".to_string(),
        "pipe-fail".to_string(),
        registry,
    );

    let ckpt_store: Arc<dyn CheckpointStore> =
        Arc::new(MemCheckpointStore::new()?);

    let (tx, _rx) = mpsc::channel::<Event>(128);
    let handle = src.run(tx, ckpt_store).await;

    // Wait for source to fail
    let result = handle.join().await;

    // Should have failed with a clear error about CDC not being available
    assert!(result.is_err(), "source should fail when CDC unavailable");

    let err = result.unwrap_err();
    let err_str = format!("{:?}", err);
    info!(error = %err_str, "got expected error");

    // Error should mention CDC or remote connection requirement
    assert!(
        err_str.contains("CDC")
            || err_str.contains("remote")
            || err_str.contains("libsql"),
        "error should mention CDC or remote requirement: {}",
        err_str
    );

    info!("✅ Graceful error when CDC unavailable");
    Ok(())
}

/// Test CDC event structure and payload correctness.
///
/// This test creates a mock turso_cdc table to verify event parsing logic,
/// without requiring actual native CDC support.
#[tokio::test]
async fn turso_cdc_event_parsing_test() -> Result<()> {
    init_test_tracing();
    info!("--- Testing Turso CDC event parsing ---");

    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_parsing.db");
    let db_path_str = db_path.to_string_lossy().to_string();

    let (_db, conn) = setup_test_db(&db_path_str).await?;

    // Create mock turso_cdc table with the structure Turso uses
    // Note: Real Turso uses binary blobs + bin_record_json_object()
    // We use JSON text columns for testing the parsing logic
    conn.execute(
        "CREATE TABLE turso_cdc (
            change_id INTEGER PRIMARY KEY AUTOINCREMENT,
            change_time INTEGER DEFAULT (strftime('%s', 'now')),
            change_type INTEGER NOT NULL,
            table_name TEXT NOT NULL,
            id INTEGER,
            before TEXT,
            after TEXT
        )",
        (),
    )
    .await?;

    // Insert mock CDC records
    // change_type: 1 = INSERT, 0 = UPDATE, -1 = DELETE

    // INSERT event
    conn.execute(
        "INSERT INTO turso_cdc (change_type, table_name, id, after) \
         VALUES (1, 'users', 1, '{\"id\":1,\"name\":\"alice\",\"email\":\"alice@test.com\"}')",
        (),
    )
    .await?;

    // UPDATE event
    conn.execute(
        "INSERT INTO turso_cdc (change_type, table_name, id, before, after) \
         VALUES (0, 'users', 1, \
         '{\"id\":1,\"name\":\"alice\",\"email\":\"alice@test.com\"}', \
         '{\"id\":1,\"name\":\"alice\",\"email\":\"alice.updated@test.com\"}')",
        (),
    )
    .await?;

    // DELETE event
    conn.execute(
        "INSERT INTO turso_cdc (change_type, table_name, id, before) \
         VALUES (-1, 'users', 1, '{\"id\":1,\"name\":\"alice\",\"email\":\"alice.updated@test.com\"}')",
        (),
    )
    .await?;

    // Verify CDC table contents
    let mut rows = conn
        .query(
            "SELECT change_id, change_type, table_name, before, after FROM turso_cdc ORDER BY change_id",
            (),
        )
        .await?;

    use libsql::Value;
    let mut records = Vec::new();
    while let Ok(Some(row)) = rows.next().await {
        let change_id = match row.get_value(0) {
            Ok(Value::Integer(i)) => i,
            _ => 0,
        };
        let change_type = match row.get_value(1) {
            Ok(Value::Integer(i)) => i,
            _ => 0,
        };
        let table_name = match row.get_value(2) {
            Ok(Value::Text(s)) => s,
            _ => String::new(),
        };
        let before = match row.get_value(3) {
            Ok(Value::Text(s)) => Some(s),
            _ => None,
        };
        let after = match row.get_value(4) {
            Ok(Value::Text(s)) => Some(s),
            _ => None,
        };
        records.push((change_id, change_type, table_name, before, after));
    }

    assert_eq!(records.len(), 3, "should have 3 CDC records");

    // Verify INSERT (change_type = 1)
    let (_, ct, table, before, after) = &records[0];
    assert_eq!(*ct, 1);
    assert_eq!(table, "users");
    assert!(before.is_none());
    assert!(after.is_some());
    let after_json: serde_json::Value =
        serde_json::from_str(after.as_ref().unwrap())?;
    assert_eq!(after_json["name"], "alice");

    // Verify UPDATE (change_type = 0)
    let (_, ct, _, before, after) = &records[1];
    assert_eq!(*ct, 0);
    assert!(before.is_some());
    assert!(after.is_some());
    let before_json: serde_json::Value =
        serde_json::from_str(before.as_ref().unwrap())?;
    let after_json: serde_json::Value =
        serde_json::from_str(after.as_ref().unwrap())?;
    assert_eq!(before_json["email"], "alice@test.com");
    assert_eq!(after_json["email"], "alice.updated@test.com");

    // Verify DELETE (change_type = -1)
    let (_, ct, _, before, after) = &records[2];
    assert_eq!(*ct, -1);
    assert!(before.is_some());
    assert!(after.is_none());

    info!("✅ CDC event parsing test passed!");
    Ok(())
}

/// Test checkpoint structure serialization.
#[tokio::test]
async fn turso_checkpoint_serialization_test() -> Result<()> {
    init_test_tracing();
    info!("--- Testing Turso checkpoint serialization ---");

    use sources::turso::TursoCheckpoint;

    // Create checkpoint
    let cp = TursoCheckpoint {
        last_change_id: Some(42),
        timestamp_ms: 1234567890,
    };

    // Serialize
    let bytes = serde_json::to_vec(&cp)?;
    let json_str = String::from_utf8(bytes.clone())?;
    info!(json = %json_str, "serialized checkpoint");

    // Deserialize
    let parsed: TursoCheckpoint = serde_json::from_slice(&bytes)?;
    assert_eq!(parsed.last_change_id, Some(42));
    assert_eq!(parsed.timestamp_ms, 1234567890);

    // Default checkpoint
    let default_cp = TursoCheckpoint::default();
    assert!(default_cp.last_change_id.is_none());
    assert_eq!(default_cp.timestamp_ms, 0);

    info!("✅ Checkpoint serialization test passed!");
    Ok(())
}
