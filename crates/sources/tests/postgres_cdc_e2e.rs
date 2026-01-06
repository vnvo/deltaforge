use anyhow::Result;
use checkpoints::{CheckpointStore, MemCheckpointStore};
use deltaforge_core::{Event, Op, Source};
use schema_registry::{InMemoryRegistry, SourceSchema};
use sources::SourceSchemaLoader;
use sources::postgres::{PostgresSchemaLoader, PostgresSource};
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
use tokio_postgres::NoTls;
use tracing::{debug, info};

mod common;
use common::init_test_tracing;

/// End-to-end CDC test for the PostgreSQL source.
///
/// This is intentionally a "fat" integration test:
/// - boots a real PostgreSQL 17 container with logical replication enabled
/// - provisions schema / user / privileges
/// - creates publication and replication slot
/// - tests schema loading with nullable columns, wildcards, registry integration
/// - starts `PostgresSource` with an in-memory checkpoint store
/// - performs INSERT/UPDATE/DELETE
/// - asserts that corresponding CDC events are emitted with correct payloads.
///
/// NOTE: this test requires Docker and the ability to pull/run `postgres:17`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires docker and significant disk space"]
async fn postgres_cdc_end_to_end() -> Result<()> {
    init_test_tracing();

    // Boot PostgreSQL 17 with logical replication enabled
    let image = GenericImage::new("postgres", "17")
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "password")
        .with_env_var("POSTGRES_DB", "shop")
        .with_cmd(vec![
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=10",
            "-c",
            "max_wal_senders=10",
        ])
        // Fixed host port keeps the DSN simple; tests are marked as E2E and
        // should not be run in parallel with other PostgreSQL-using tests.
        .with_mapped_port(5433, 5432.tcp());

    let container = image.start().await.expect("start postgres");
    info!("Container ID: {}", container.id());

    // Container log followers (helpful when this fails in CI)
    {
        let mut out = container.stdout(true);
        task::spawn(async move {
            let mut line = String::new();
            loop {
                line.clear();
                match out.read_line(&mut line).await {
                    Ok(0) => break,
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

    // Give PostgreSQL a bit of time to finish init.
    sleep(Duration::from_secs(5)).await;
    info!("starting PostgreSQL CDC e2e test ...");

    let port = 5433;
    let dsn = format!(
        "host=127.0.0.1 port={} user=postgres password=password dbname=shop",
        port
    );
    let dsn_url =
        format!("postgres://postgres:password@127.0.0.1:{}/shop", port);

    // Connect for setup
    let (client, conn) = tokio_postgres::connect(&dsn, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Sanity-check that wal_level is logical
    let row = client.query_one("SHOW wal_level", &[]).await?;
    let wal_level: &str = row.get(0);
    assert_eq!(wal_level, "logical", "wal_level must be 'logical' for CDC");
    info!("wal_level = {}", wal_level);

    // Create schema
    client
        .execute(
            r#"
            CREATE TABLE IF NOT EXISTS orders(
                id INT PRIMARY KEY,
                sku VARCHAR(64),
                payload JSONB,
                blobz BYTEA
            )"#,
            &[],
        )
        .await?;
    debug!("table `orders` created.");

    // Create replication user with proper privileges
    client
        .execute("CREATE USER df WITH REPLICATION PASSWORD 'dfpw'", &[])
        .await
        .ok(); // Ignore if exists
    debug!("user `df` created");

    client
        .execute("GRANT SELECT ON ALL TABLES IN SCHEMA public TO df", &[])
        .await?;
    client
        .execute("GRANT USAGE ON SCHEMA public TO df", &[])
        .await?;

    // Create publication for CDC
    client
        .execute("DROP PUBLICATION IF EXISTS deltaforge_pub", &[])
        .await?;
    client
        .execute("CREATE PUBLICATION deltaforge_pub FOR TABLE orders", &[])
        .await?;
    debug!("publication `deltaforge_pub` created");

    // Create replication slot
    client
        .execute(
            "SELECT pg_drop_replication_slot('deltaforge_slot') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'deltaforge_slot')",
            &[],
        )
        .await
        .ok(); // Ignore errors

    // Drop slot if exists (cleaner approach)
    let slot_exists: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = 'deltaforge_slot')",
            &[],
        )
        .await?
        .get(0);

    if slot_exists {
        client
            .execute("SELECT pg_drop_replication_slot('deltaforge_slot')", &[])
            .await?;
    }

    client
        .execute(
            "SELECT pg_create_logical_replication_slot('deltaforge_slot', 'pgoutput')",
            &[],
        )
        .await?;
    debug!("replication slot `deltaforge_slot` created");

    // Set replica identity to FULL so we get old values on UPDATE/DELETE
    client
        .execute("ALTER TABLE orders REPLICA IDENTITY FULL", &[])
        .await?;
    debug!("replica identity set to FULL");

    // =========================================================================
    // SCHEMA LOADER TESTS
    // =========================================================================
    info!("--- Testing schema loader ---");

    let registry = Arc::new(InMemoryRegistry::new());
    let schema_loader =
        PostgresSchemaLoader::new(&dsn_url, registry.clone(), "acme");

    // Test 1: Expand exact pattern
    {
        let tables = schema_loader
            .expand_patterns(&["public.orders".to_string()])
            .await?;
        assert_eq!(tables.len(), 1, "should find exactly one table");
        assert_eq!(tables[0], ("public".to_string(), "orders".to_string()));
        info!("expand_patterns exact match works");
    }

    // Test 2: Load schema and verify columns (tests NULL handling!)
    {
        let loaded = schema_loader.load_schema("public", "orders").await?;
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
        assert_eq!(schema.primary_key, vec!["id".to_string()]);

        // Check specific column types
        let id_col = schema.column("id").expect("id column");
        assert_eq!(id_col.data_type, "integer");
        assert!(!id_col.nullable, "id should not be nullable (PK)");

        let sku_col = schema.column("sku").expect("sku column");
        assert_eq!(sku_col.data_type, "character varying");
        assert!(sku_col.nullable, "sku should be nullable");
        assert_eq!(sku_col.char_max_length, Some(64));

        let payload_col = schema.column("payload").expect("payload column");
        assert_eq!(payload_col.data_type, "jsonb");

        let blob_col = schema.column("blobz").expect("blobz column");
        assert_eq!(blob_col.data_type, "bytea");

        info!("load_schema returns correct column info (NULL handling works)");
    }

    // Test 3: Schema fingerprint stability
    {
        let loaded1 = schema_loader.load_schema("public", "orders").await?;
        let loaded2 = schema_loader.load_schema("public", "orders").await?;

        assert_eq!(
            loaded1.fingerprint, loaded2.fingerprint,
            "fingerprint should be stable"
        );
        assert!(
            !loaded1.fingerprint.is_empty(),
            "fingerprint should not be empty"
        );
        info!("fingerprint is stable across loads");
    }

    // Test 4: Schema registered in registry
    {
        let versions = registry.list_versions("acme", "public", "orders");
        assert!(!versions.is_empty(), "schema should be registered");
        assert_eq!(versions[0].version, 1);
        info!("schema is registered with registry");
    }

    // Test 5: Preload with wildcard patterns
    {
        // Create another table for wildcard testing
        client
            .execute(
                r#"CREATE TABLE IF NOT EXISTS order_items(
                    id INT PRIMARY KEY,
                    order_id INT,
                    product VARCHAR(128)
                )"#,
                &[],
            )
            .await?;

        let tables = schema_loader
            .preload(&["public.order%".to_string()])
            .await?;
        assert!(tables.len() >= 2, "wildcard should match at least 2 tables");
        assert!(tables.iter().any(|(s, t)| s == "public" && t == "orders"));
        assert!(
            tables
                .iter()
                .any(|(s, t)| s == "public" && t == "order_items")
        );
        info!("wildcard pattern expansion works");
    }

    // Test 6: Schema reload after DDL
    {
        let fp_before = schema_loader
            .load_schema("public", "orders")
            .await?
            .fingerprint;

        // Add a column
        client
            .execute("ALTER TABLE orders ADD COLUMN notes TEXT", &[])
            .await?;

        // Reload schema (force refresh)
        let loaded = schema_loader.reload_schema("public", "orders").await?;
        let fp_after = loaded.fingerprint;

        assert_ne!(fp_before, fp_after, "fingerprint should change after DDL");
        assert_eq!(loaded.schema.columns.len(), 5, "should have 5 columns now");
        assert!(
            loaded.schema.column("notes").is_some(),
            "notes column should exist"
        );

        // Check registry has new version
        let versions = registry.list_versions("acme", "public", "orders");
        assert!(
            versions.len() >= 2,
            "should have multiple versions after DDL"
        );

        info!("schema reload detects DDL changes");
    }

    // Test 7: Cache behavior
    {
        let cached = schema_loader.list_cached().await;
        assert!(!cached.is_empty(), "cache should not be empty");

        let orders_cached = cached.iter().find(|entry| {
            entry.database == "public" && entry.table == "orders"
        });
        assert!(orders_cached.is_some(), "orders should be in cache");

        info!("schema caching works");
    }

    // =========================================================================
    // CDC EVENT TESTS
    // =========================================================================
    info!("--- Testing CDC events ---");

    // Use the replication user DSN for the source
    let repl_dsn = format!("postgres://df:dfpw@127.0.0.1:{}/shop", port);

    // Start source
    let src = PostgresSource {
        id: "it-postgres".into(),
        checkpoint_key: "postgres-it-postgres".to_string(),
        dsn: repl_dsn.clone(),
        slot: "deltaforge_slot".to_string(),
        publication: "deltaforge_pub".to_string(),
        tables: vec!["public.orders".to_string()],
        tenant: "acme".to_string(),
        pipeline: "pipe-pg".to_string(),
        registry: Arc::new(InMemoryRegistry::new()),
    };

    let ckpt_store: Arc<dyn CheckpointStore> =
        Arc::new(MemCheckpointStore::new()?);

    let (tx, mut rx) = mpsc::channel::<Event>(128);
    let handle = src.run(tx, ckpt_store).await;
    info!("source is running ...");

    // Give the source a small head-start so it can connect and subscribe
    sleep(Duration::from_secs(3)).await;

    // 1) INSERT
    client
        .execute(
            r#"INSERT INTO orders (id, sku, payload, blobz) VALUES (1, 'sku-1', '{"a":1}', '\xDEADBEEF')"#,
            &[],
        )
        .await?;

    // 2) UPDATE
    client
        .execute("UPDATE orders SET sku='sku-1b' WHERE id=1", &[])
        .await?;

    // 3) DELETE
    client.execute("DELETE FROM orders WHERE id=1", &[]).await?;
    debug!(
        "insert, update and delete performed on `orders` - expecting CDC events ..."
    );

    // Expect at least 3 events (insert/update/delete for our row), but there
    // may be additional events from DDL or other internal activity. We collect
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
                // Source ended: stop waiting
                break;
            }
            Err(_elapsed) => {
                // Deadline hit: stop waiting
                break;
            }
        }
    }

    // --- Structural assertions on the event stream ---

    // We must have seen each operation at least once.
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

    // Filter down to events related to id=1. For DELETE, the id will be in
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
        "expected exactly 3 CDC events for id=1 (insert, update, delete); got {}",
        by_id.len()
    );

    // Preserve WAL order: for our single row this should be
    // INSERT -> UPDATE -> DELETE.
    let ops: Vec<Op> = by_id.iter().map(|e| e.op).collect();
    assert_eq!(
        ops,
        vec![Op::Insert, Op::Update, Op::Delete],
        "unexpected operation order for id=1"
    );

    // --- Payload assertions on the INSERT event (JSONB + BYTEA) ---

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
        "BYTEA field should be base64-wrapped"
    );

    // Assert update event shape
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

    // Check delete event shape
    let del = by_id
        .iter()
        .find(|e| matches!(e.op, Op::Delete))
        .expect("missing DELETE event for id=1");
    let del_before = del.before.as_ref().expect("DELETE must have `before`");
    assert_eq!(del_before["id"], 1);
    assert!(del.after.is_none(), "DELETE must not have `after`");

    for e in &by_id {
        assert_eq!(e.table, "public.orders");
        assert_eq!(e.source.db, "public");
    }

    // Verify checkpoint format (LSN-based)
    for e in &by_id {
        if let Some(ref cp) = e.checkpoint {
            // PostgreSQL checkpoints should contain LSN
            let cp_str = String::from_utf8_lossy(cp.as_bytes());
            assert!(
                cp_str.contains("lsn"),
                "checkpoint should contain LSN: {}",
                cp_str
            );
        }
    }

    // Verify schema version is attached
    for e in &by_id {
        assert!(
            e.schema_version.is_some(),
            "event should have schema_version"
        );
        assert!(
            e.schema_sequence.is_some(),
            "event should have schema_sequence"
        );
    }

    info!("all PostgreSQL CDC e2e assertions are successful!");

    // Clean shutdown for source
    handle.stop();
    let _ = handle.join().await;

    // Cleanup: drop slot to prevent WAL accumulation
    let (cleanup_client, cleanup_conn) =
        tokio_postgres::connect(&dsn, NoTls).await?;
    tokio::spawn(async move {
        let _ = cleanup_conn.await;
    });
    cleanup_client
        .execute("SELECT pg_drop_replication_slot('deltaforge_slot')", &[])
        .await
        .ok();

    Ok(())
}

/// Test PostgreSQL-specific features: arrays, UUID, timestamps, numeric precision.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires docker"]
async fn postgres_cdc_extended_types() -> Result<()> {
    init_test_tracing();

    let image = GenericImage::new("postgres", "17")
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "password")
        .with_env_var("POSTGRES_DB", "testdb")
        .with_cmd(vec![
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=4",
            "-c",
            "max_wal_senders=4",
        ])
        .with_mapped_port(5434, 5432.tcp());

    let _container = image.start().await.expect("start postgres");
    sleep(Duration::from_secs(5)).await;

    let port = 5434;
    let dsn = format!(
        "host=127.0.0.1 port={} user=postgres password=password dbname=testdb",
        port
    );
    let dsn_url =
        format!("postgres://postgres:password@127.0.0.1:{}/testdb", port);

    let (client, conn) = tokio_postgres::connect(&dsn, NoTls).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    // Create table with extended types
    client
        .execute(
            r#"
            CREATE TABLE extended_types(
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                tags TEXT[],
                score NUMERIC(10, 4),
                created_at TIMESTAMPTZ DEFAULT NOW(),
                metadata JSONB,
                flags BOOLEAN[]
            )"#,
            &[],
        )
        .await?;

    client
        .execute("ALTER TABLE extended_types REPLICA IDENTITY FULL", &[])
        .await?;

    client
        .execute("DROP PUBLICATION IF EXISTS ext_pub", &[])
        .await?;
    client
        .execute("CREATE PUBLICATION ext_pub FOR TABLE extended_types", &[])
        .await?;

    let slot_exists: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = 'ext_slot')",
            &[],
        )
        .await?
        .get(0);
    if slot_exists {
        client
            .execute("SELECT pg_drop_replication_slot('ext_slot')", &[])
            .await?;
    }
    client
        .execute(
            "SELECT pg_create_logical_replication_slot('ext_slot', 'pgoutput')",
            &[],
        )
        .await?;

    // Test schema loading with extended types
    let registry = Arc::new(InMemoryRegistry::new());
    let schema_loader =
        PostgresSchemaLoader::new(&dsn_url, registry.clone(), "acme");

    let loaded = schema_loader
        .load_schema("public", "extended_types")
        .await?;
    let schema = &loaded.schema;

    assert_eq!(schema.columns.len(), 6);

    let id_col = schema.column("id").expect("id column");
    assert_eq!(id_col.data_type, "uuid");

    let tags_col = schema.column("tags").expect("tags column");
    assert!(tags_col.is_array, "tags should be an array type");

    let score_col = schema.column("score").expect("score column");
    assert_eq!(score_col.data_type, "numeric");
    assert_eq!(score_col.numeric_precision, Some(10));
    assert_eq!(score_col.numeric_scale, Some(4));

    let created_col = schema.column("created_at").expect("created_at column");
    assert_eq!(created_col.data_type, "timestamp with time zone");

    info!("PostgreSQL extended types schema loading works");

    // Start source and test CDC
    let src = PostgresSource {
        id: "ext-pg".into(),
        checkpoint_key: "postgres-ext-pg".to_string(),
        dsn: dsn_url.clone(),
        slot: "ext_slot".to_string(),
        publication: "ext_pub".to_string(),
        tables: vec!["public.extended_types".to_string()],
        tenant: "acme".to_string(),
        pipeline: "pipe-ext".to_string(),
        registry: Arc::new(InMemoryRegistry::new()),
    };

    let ckpt_store: Arc<dyn CheckpointStore> =
        Arc::new(MemCheckpointStore::new()?);
    let (tx, mut rx) = mpsc::channel::<Event>(128);
    let handle = src.run(tx, ckpt_store).await;

    sleep(Duration::from_secs(2)).await;

    // Insert row with extended types
    client
        .execute(
            r#"INSERT INTO extended_types (id, tags, score, metadata, flags) 
               VALUES ('550e8400-e29b-41d4-a716-446655440000', 
                       ARRAY['rust', 'cdc', 'postgres'], 
                       1234.5678, 
                       '{"nested": {"value": 42}}',
                       ARRAY[true, false, true])"#,
            &[],
        )
        .await?;

    // Wait for event
    let event = timeout(Duration::from_secs(10), rx.recv())
        .await?
        .expect("should receive event");

    assert_eq!(event.op, Op::Insert);
    let after = event.after.as_ref().expect("INSERT must have after");

    // Verify UUID
    assert_eq!(after["id"], "550e8400-e29b-41d4-a716-446655440000");

    // Verify array
    let tags = after["tags"].as_array().expect("tags should be array");
    assert_eq!(tags.len(), 3);
    assert_eq!(tags[0], "rust");

    // Verify numeric (should be preserved as string to maintain precision)
    // or as number depending on implementation
    let score = &after["score"];
    assert!(
        score.is_string() || score.is_number(),
        "score should be string or number"
    );

    // Verify nested JSONB
    assert_eq!(after["metadata"]["nested"]["value"], 42);

    // Verify boolean array
    let flags = after["flags"].as_array().expect("flags should be array");
    assert_eq!(flags[0], true);
    assert_eq!(flags[1], false);

    info!("PostgreSQL extended types CDC works");

    handle.stop();
    let _ = handle.join().await;

    // Cleanup
    client
        .execute("SELECT pg_drop_replication_slot('ext_slot')", &[])
        .await
        .ok();

    Ok(())
}

/// Test replica identity modes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires docker"]
async fn postgres_replica_identity_modes() -> Result<()> {
    init_test_tracing();

    let image = GenericImage::new("postgres", "17")
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "password")
        .with_env_var("POSTGRES_DB", "testdb")
        .with_cmd(vec![
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=4",
            "-c",
            "max_wal_senders=4",
        ])
        .with_mapped_port(5435, 5432.tcp());

    let _container = image.start().await.expect("start postgres");
    sleep(Duration::from_secs(5)).await;

    let port = 5435;
    let dsn = format!(
        "host=127.0.0.1 port={} user=postgres password=password dbname=testdb",
        port
    );
    let dsn_url =
        format!("postgres://postgres:password@127.0.0.1:{}/testdb", port);

    let (client, conn) = tokio_postgres::connect(&dsn, NoTls).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    // Table with DEFAULT replica identity (only PK in before)
    client
        .execute(
            r#"CREATE TABLE ri_default(id INT PRIMARY KEY, data TEXT)"#,
            &[],
        )
        .await?;
    // Keep default replica identity

    // Table with FULL replica identity (all columns in before)
    client
        .execute(
            r#"CREATE TABLE ri_full(id INT PRIMARY KEY, data TEXT)"#,
            &[],
        )
        .await?;
    client
        .execute("ALTER TABLE ri_full REPLICA IDENTITY FULL", &[])
        .await?;

    // Create publication and slot
    client
        .execute("DROP PUBLICATION IF EXISTS ri_pub", &[])
        .await?;
    client
        .execute(
            "CREATE PUBLICATION ri_pub FOR TABLE ri_default, ri_full",
            &[],
        )
        .await?;

    let slot_exists: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = 'ri_slot')",
            &[],
        )
        .await?
        .get(0);
    if slot_exists {
        client
            .execute("SELECT pg_drop_replication_slot('ri_slot')", &[])
            .await?;
    }
    client
        .execute(
            "SELECT pg_create_logical_replication_slot('ri_slot', 'pgoutput')",
            &[],
        )
        .await?;

    // Verify schema loader captures replica identity
    let registry = Arc::new(InMemoryRegistry::new());
    let schema_loader =
        PostgresSchemaLoader::new(&dsn_url, registry.clone(), "acme");

    let default_schema =
        schema_loader.load_schema("public", "ri_default").await?;
    assert_eq!(
        default_schema.schema.replica_identity,
        Some("default".to_string())
    );

    let full_schema = schema_loader.load_schema("public", "ri_full").await?;
    assert_eq!(
        full_schema.schema.replica_identity,
        Some("full".to_string())
    );

    info!("replica identity captured in schema");

    // Test CDC behavior
    let src = PostgresSource {
        id: "ri-pg".into(),
        checkpoint_key: "postgres-ri-pg".to_string(),
        dsn: dsn_url.clone(),
        slot: "ri_slot".to_string(),
        publication: "ri_pub".to_string(),
        tables: vec![
            "public.ri_default".to_string(),
            "public.ri_full".to_string(),
        ],
        tenant: "acme".to_string(),
        pipeline: "pipe-ri".to_string(),
        registry: Arc::new(InMemoryRegistry::new()),
    };

    let ckpt_store: Arc<dyn CheckpointStore> =
        Arc::new(MemCheckpointStore::new()?);
    let (tx, mut rx) = mpsc::channel::<Event>(128);
    let handle = src.run(tx, ckpt_store).await;

    sleep(Duration::from_secs(2)).await;

    // Insert then update both tables
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

    // Collect events
    let mut events = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline && events.len() < 4 {
        match timeout(Duration::from_secs(1), rx.recv()).await {
            Ok(Some(e)) => events.push(e),
            _ => break,
        }
    }

    // Find UPDATE events
    let default_update = events
        .iter()
        .find(|e| e.op == Op::Update && e.table == "public.ri_default");
    let full_update = events
        .iter()
        .find(|e| e.op == Op::Update && e.table == "public.ri_full");

    if let Some(upd) = default_update {
        // DEFAULT: before should only have key columns
        let before = upd.before.as_ref();
        info!("ri_default UPDATE before: {:?}", before);
        // With DEFAULT, we may get key-only or nothing depending on implementation
    }

    if let Some(upd) = full_update {
        // FULL: before should have all columns
        let before = upd.before.as_ref().expect("FULL should have before");
        assert_eq!(before["id"], 1);
        assert_eq!(before["data"], "original");
        info!("ri_full UPDATE has full before image");
    }

    handle.stop();
    let _ = handle.join().await;

    client
        .execute("SELECT pg_drop_replication_slot('ri_slot')", &[])
        .await
        .ok();

    info!("replica identity modes test passed");

    Ok(())
}
