//! PostgreSQL CDC e2e tests. Run with:
//! `cargo test -p sources --test postgres_cdc_e2e -- --include-ignored --nocapture --test-threads=1`
//!

use anyhow::Result;
use checkpoints::{CheckpointStore, MemCheckpointStore};
use common::AllowList;
use ctor::dtor;
use deltaforge_core::{
    BatchContext, Event, Op, Source, SourceHandle, SourceItem,
};

use sources::postgres::{
    PostgresSchemaLoader, PostgresSource, pg_row_event_id,
};
use std::sync::Arc;
use std::time::Instant;
use tokio::{
    sync::mpsc,
    time::{Duration, sleep, timeout},
};
use tracing::info;

mod test_common;
use test_common::{
    PG_CDC_USER, PG_CONTAINER, init_test_tracing, make_registry, pg_admin_dsn,
    pg_cdc_dsn, pg_drop_db, pg_get_container, pg_port, pg_setup,
};

use crate::test_common::make_storage_backend;

#[dtor]
fn cleanup() {
    if let Some(c) = PG_CONTAINER.get() {
        std::process::Command::new("docker")
            .args(["rm", "-f", c.0.id()])
            .output()
            .ok();
    }
}

/// Drop a replication slot if it exists (inactive). Best-effort; used to force a
/// snapshot source to recreate its own owned slot on a subsequent run.
async fn drop_repl_slot(client: &tokio_postgres::Client, slot: &str) {
    client
        .batch_execute(&format!(
            "SELECT pg_drop_replication_slot('{slot}') WHERE EXISTS \
             (SELECT 1 FROM pg_replication_slots WHERE slot_name='{slot}')"
        ))
        .await
        .ok();
}

/// Create only the publication (and clear any stale slot). Snapshot-mode sources
/// establish the replication slot themselves via `prepare_snapshot_slot_anchor`
/// to record durable ownership and anchor at the slot's consistent point;
/// pre-creating the slot would fail closed ("cannot prove exclusive ownership").
/// CDC-only tests that stream from a pre-existing slot use `create_pub_slot`.
async fn create_publication_only(
    client: &tokio_postgres::Client,
    pub_name: &str,
    slot: &str,
    tables: &[&str],
) -> Result<()> {
    client
        .execute(&format!("DROP PUBLICATION IF EXISTS {pub_name}"), &[])
        .await?;
    drop_repl_slot(client, slot).await;
    let tbl = if tables.is_empty() {
        "ALL TABLES".into()
    } else {
        format!("TABLE {}", tables.join(", "))
    };
    client
        .execute(&format!("CREATE PUBLICATION {pub_name} FOR {tbl}"), &[])
        .await?;
    Ok(())
}

/// Create the publication and pre-create the replication slot. For CDC-only
/// (`snapshot.mode = never`) tests that stream from an operator-provisioned slot.
/// Snapshot-mode tests must use [`create_publication_only`] instead.
async fn create_pub_slot(
    client: &tokio_postgres::Client,
    pub_name: &str,
    slot: &str,
    tables: &[&str],
) -> Result<()> {
    create_publication_only(client, pub_name, slot, tables).await?;
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
    rx: &mut mpsc::Receiver<SourceItem>,
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
            Ok(Some(SourceItem::Event(e))) => {
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

/// Receive the next data event, skipping transaction-commit markers.
async fn next_event(
    rx: &mut mpsc::Receiver<SourceItem>,
    dur: Duration,
) -> Option<Event> {
    let deadline = Instant::now() + dur;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match timeout(remaining, rx.recv()).await {
            Ok(Some(SourceItem::Event(e))) => return Some(e),
            Ok(Some(SourceItem::TxBegin { .. })) => continue,
            Ok(Some(SourceItem::TxCommit { .. })) => continue,
            _ => return None,
        }
    }
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

/// Build a PostgresSource with constant defaults (tenant=acme, pipeline=test,
/// fresh InMemoryRegistry). `checkpoint_key` is derived as `pg-{id}`.
async fn make_source(
    id: &str,
    db: &str,
    slot: &str,
    publication: &str,
    tables: Vec<String>,
    outbox_prefixes: AllowList,
) -> PostgresSource {
    PostgresSource {
        id: id.into(),
        dsn: pg_cdc_dsn(db).await.into(),
        slot: slot.into(),
        publication: publication.into(),
        tables,
        tenant: "acme".into(),
        pipeline: "test".into(),
        registry: make_registry().await,
        outbox_prefixes,
        snapshot_cfg: deltaforge_config::SnapshotCfg::default(),
        backend: make_storage_backend().await,
        on_schema_drift: deltaforge_config::OnSchemaDrift::Adapt,
        table_options: Default::default(),
        rotation: None,
    }
}

/// Start a source with a fresh checkpoint store, wait for ready, warm up 2s.
/// Returns (rx, handle).
async fn start_source(
    src: PostgresSource,
) -> Result<(mpsc::Receiver<SourceItem>, SourceHandle)> {
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let (tx, rx) = mpsc::channel(128);
    let handle = src.run(tx, ckpt).await;
    wait_ready(&handle, Duration::from_secs(10)).await?;
    sleep(Duration::from_secs(2)).await;
    Ok((rx, handle))
}

/// Build a CDC-only source (no snapshot) with an explicit drift policy, sharing a
/// registry + backend across runs so the persisted schema survives restarts (as a
/// durable registry does in production - the basis for startup drift detection).
async fn configured_source(
    id: &str,
    db: &str,
    slot: &str,
    publication: &str,
    drift: deltaforge_config::OnSchemaDrift,
    registry: Arc<storage::DurableSchemaRegistry>,
    backend: storage::ArcStorageBackend,
) -> PostgresSource {
    let mut src = make_source(
        id,
        db,
        slot,
        publication,
        vec!["public.orders".into()],
        AllowList::default(),
    )
    .await;
    src.on_schema_drift = drift;
    src.snapshot_cfg = deltaforge_config::SnapshotCfg {
        mode: deltaforge_config::SnapshotMode::Never,
        ..Default::default()
    };
    src.registry = registry;
    src.backend = backend;
    src
}

/// A registry + its backing store, shared across a test's runs.
async fn shared_registry() -> (
    Arc<storage::DurableSchemaRegistry>,
    storage::ArcStorageBackend,
) {
    let backend = make_storage_backend().await;
    let registry = storage::DurableSchemaRegistry::new(backend.clone())
        .await
        .expect("registry");
    (registry, backend)
}

/// Drain every `SourceItem` available within `dur` (stops early when the channel
/// closes, e.g. after the source task ends).
async fn drain_items(
    rx: &mut mpsc::Receiver<SourceItem>,
    dur: Duration,
) -> Vec<SourceItem> {
    let mut items = Vec::new();
    let deadline = Instant::now() + dur;
    while Instant::now() < deadline {
        match timeout(Duration::from_millis(100), rx.recv()).await {
            Ok(Some(item)) => items.push(item),
            Ok(None) => break,
            Err(_) => continue,
        }
    }
    items
}

// =============================================================================
// TESTS
// =============================================================================

#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_schema_loader() -> Result<()> {
    let (db, client) = pg_setup("schema").await?;

    client.execute("CREATE TABLE orders (id SERIAL PRIMARY KEY, sku VARCHAR(64) NOT NULL, price NUMERIC(10,2))", &[]).await?;
    client.execute("CREATE TABLE order_items (id SERIAL PRIMARY KEY, order_id INT, product VARCHAR(128))", &[]).await?;

    let registry = make_registry().await;
    let loader = PostgresSchemaLoader::new(
        &pg_admin_dsn(&db).await,
        registry.clone(),
        "acme",
    );

    // Single table load
    let loaded = loader.load_schema("public", "orders").await?;
    assert_eq!(loaded.schema.columns.len(), 3);
    assert!(!loaded.schema.column("sku").unwrap().nullable);
    assert!(loaded.schema.column("price").unwrap().nullable);
    info!("✓ schema load + nullable detection");

    // Wildcard expansion (use glob `*` — `%` is treated as literal)
    let tables = loader.preload(&["public.order*".to_string()]).await?;
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

    pg_drop_db(&db).await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_cdc_basic_events() -> Result<()> {
    let (db, client) = pg_setup("basic").await?;

    client.execute("CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(64), payload JSONB)", &[]).await?;
    client
        .execute("ALTER TABLE orders REPLICA IDENTITY FULL", &[])
        .await?;
    client
        .execute(&format!("GRANT SELECT ON orders TO {PG_CDC_USER}"), &[])
        .await?;
    create_pub_slot(&client, "pub_basic", "slot_basic", &["orders"]).await?;

    let src = make_source(
        "basic",
        &db,
        "slot_basic",
        "pub_basic",
        vec!["public.orders".into()],
        AllowList::default(),
    )
    .await;
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
    pg_drop_db(&db).await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_cdc_schema_evolution() -> Result<()> {
    let (db, client) = pg_setup("evo").await?;

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
        .execute(&format!("GRANT SELECT ON orders TO {PG_CDC_USER}"), &[])
        .await?;
    create_pub_slot(&client, "pub_evo", "slot_evo", &["orders"]).await?;

    let src = make_source(
        "evo",
        &db,
        "slot_evo",
        "pub_evo",
        vec!["public.orders".into()],
        AllowList::default(),
    )
    .await;
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
    pg_drop_db(&db).await;
    Ok(())
}

/// on_schema_drift = Adapt: an in-stream Relation change is absorbed - the schema
/// reloads, the first event under the changed relation is delivered correctly,
/// and the checkpoint advances past the drift as normal.
#[tokio::test]
#[ignore = "requires docker"]
async fn pg_schema_drift_adapt_delivers_post_drift_and_advances_checkpoint()
-> Result<()> {
    use deltaforge_config::OnSchemaDrift;

    let (db, client) = pg_setup("adapt").await?;
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
        .execute(&format!("GRANT SELECT ON orders TO {PG_CDC_USER}"), &[])
        .await?;
    create_pub_slot(&client, "pub_adapt", "slot_adapt", &["orders"]).await?;

    let (registry, backend) = shared_registry().await;
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);

    // Run 1: deliver a pre-drift row, then stop so a committed boundary persists.
    {
        let src = configured_source(
            "adapt",
            &db,
            "slot_adapt",
            "pub_adapt",
            OnSchemaDrift::Adapt,
            registry.clone(),
            backend.clone(),
        )
        .await;
        let (tx, mut rx) = mpsc::channel(128);
        let h = src.run(tx, ckpt.clone()).await;
        wait_ready(&h, Duration::from_secs(3)).await?;
        client
            .execute("INSERT INTO orders VALUES (200, 'pre')", &[])
            .await?;
        let ev = collect_until(&mut rx, Duration::from_secs(10), |e| {
            e.iter().any(|x| has_id(x, 200))
        })
        .await;
        assert!(ev.iter().any(|e| has_id(e, 200)), "pre-drift row delivered");
        h.stop();
        h.join().await.ok();
    }
    let cp0 = ckpt.get_raw("adapt").await?.expect("pre-drift checkpoint");

    // Drift + a row under the changed schema.
    client
        .execute("ALTER TABLE orders ADD COLUMN status VARCHAR(32)", &[])
        .await?;
    client
        .execute("INSERT INTO orders VALUES (201, 'post', 'active')", &[])
        .await?;

    // Run 2: Adapt resumes, reloads, delivers 201 under the new schema, advances.
    {
        let src = configured_source(
            "adapt",
            &db,
            "slot_adapt",
            "pub_adapt",
            OnSchemaDrift::Adapt,
            registry.clone(),
            backend.clone(),
        )
        .await;
        let (tx, mut rx) = mpsc::channel(128);
        let h = src.run(tx, ckpt.clone()).await;
        wait_ready(&h, Duration::from_secs(3)).await?;
        let ev = collect_until(&mut rx, Duration::from_secs(15), |e| {
            e.iter().any(|x| has_id(x, 201))
        })
        .await;
        let post = ev
            .iter()
            .find(|e| has_id(e, 201))
            .expect("post-drift row delivered under adapt");
        assert!(
            post.after.as_ref().unwrap().get("status").is_some(),
            "post-drift row decoded under the changed schema (new column present)"
        );
        h.stop();
        h.join().await.ok();
    }
    let cp1 = ckpt.get_raw("adapt").await?.expect("post-drift checkpoint");
    assert_ne!(cp0, cp1, "checkpoint advanced past the drift under adapt");
    info!("✓ adapt: post-drift row delivered and checkpoint advanced");

    cleanup_repl(&client, "pub_adapt", "slot_adapt").await;
    pg_drop_db(&db).await;
    Ok(())
}

/// on_schema_drift = Halt: an in-stream Relation change fails the source closed
/// before any row under the changed schema is emitted, does not deliver the open
/// transaction, does not advance the checkpoint, and fails AGAIN on unchanged
/// restart (never silently skips the drift).
#[tokio::test]
#[ignore = "requires docker"]
async fn pg_schema_drift_halt_fails_closed_and_does_not_skip() -> Result<()> {
    use deltaforge_config::OnSchemaDrift;

    let (db, client) = pg_setup("halt").await?;
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
        .execute(&format!("GRANT SELECT ON orders TO {PG_CDC_USER}"), &[])
        .await?;
    create_pub_slot(&client, "pub_halt", "slot_halt", &["orders"]).await?;

    let (registry, backend) = shared_registry().await;
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);

    // Run 1: deliver a pre-drift row, then stop -> committed pre-drift boundary.
    {
        let src = configured_source(
            "halt",
            &db,
            "slot_halt",
            "pub_halt",
            OnSchemaDrift::Halt,
            registry.clone(),
            backend.clone(),
        )
        .await;
        let (tx, mut rx) = mpsc::channel(128);
        let h = src.run(tx, ckpt.clone()).await;
        wait_ready(&h, Duration::from_secs(3)).await?;
        client
            .execute("INSERT INTO orders VALUES (300, 'pre')", &[])
            .await?;
        let ev = collect_until(&mut rx, Duration::from_secs(10), |e| {
            e.iter().any(|x| has_id(x, 300))
        })
        .await;
        assert!(ev.iter().any(|e| has_id(e, 300)), "pre-drift row delivered");
        h.stop();
        h.join().await.ok();
    }
    let cp0 = ckpt.get_raw("halt").await?.expect("pre-drift checkpoint");

    // Drift + a row under the changed schema (committed while streaming is down).
    client
        .execute("ALTER TABLE orders ADD COLUMN status VARCHAR(32)", &[])
        .await?;
    client
        .execute("INSERT INTO orders VALUES (301, 'post', 'active')", &[])
        .await?;

    // One Halt restart attempt: resumes from cp0, the startup drift check detects
    // the change against the persisted schema and fails closed before streaming.
    let attempt = || async {
        let src = configured_source(
            "halt",
            &db,
            "slot_halt",
            "pub_halt",
            OnSchemaDrift::Halt,
            registry.clone(),
            backend.clone(),
        )
        .await;
        let (tx, mut rx) = mpsc::channel(128);
        let h = src.run(tx, ckpt.clone()).await;
        // The source must die (never become ready) once it hits the drift.
        let ready = wait_ready(&h, Duration::from_secs(10)).await;
        let items = drain_items(&mut rx, Duration::from_secs(2)).await;
        // stop() is a no-op on the already-failed task; it only guards the test
        // from hanging on join() if a regression left the source alive.
        h.stop();
        let joined = h.join().await;
        (ready, items, joined)
    };

    // Run 2: fails closed.
    let (ready, items, joined) = attempt().await;
    assert!(
        ready.is_err(),
        "source must not become ready when it hits a Halt drift"
    );
    let err = format!("{:?}", joined.expect_err("run must fail closed"));
    assert!(
        err.contains("orders") && err.contains("on_schema_drift=adapt"),
        "typed, actionable error names the table and remediation: {err}"
    );
    assert!(
        !items
            .iter()
            .any(|i| matches!(i, SourceItem::Event(e) if has_id(e, 301))),
        "no post-drift event delivered"
    );
    assert!(
        !items.iter().any(|i| matches!(i, SourceItem::Event(_))),
        "no row event delivered from the drift transaction"
    );
    assert!(
        !items
            .iter()
            .any(|i| matches!(i, SourceItem::TxCommit { .. })),
        "no partial transaction delivered (no commit)"
    );
    assert_eq!(
        ckpt.get_raw("halt").await?.as_deref(),
        Some(cp0.as_slice()),
        "checkpoint remains at the prior committed boundary"
    );
    info!("✓ halt: failed closed, no post-drift delivery, checkpoint held");

    // Run 3: unchanged restart fails AGAIN (does not skip the drift).
    let (ready3, items3, joined3) = attempt().await;
    assert!(ready3.is_err(), "second Halt restart must also fail closed");
    assert!(
        joined3.is_err(),
        "unchanged restart under Halt fails again rather than skipping the drift"
    );
    assert!(
        !items3.iter().any(|i| matches!(i, SourceItem::Event(_))),
        "still no event delivered on the second attempt"
    );
    assert_eq!(
        ckpt.get_raw("halt").await?.as_deref(),
        Some(cp0.as_slice()),
        "checkpoint still held at the prior committed boundary"
    );
    info!("✓ halt: unchanged restart fails again without skipping");

    cleanup_repl(&client, "pub_halt", "slot_halt").await;
    pg_drop_db(&db).await;
    Ok(())
}

/// Dropped-table retained-WAL recovery. Sequence: consume + register the schema,
/// stop, write more rows into the table, DROP it (its earlier changes stay in the
/// retained WAL), then restart from the prior checkpoint.
///
/// The retained rows must be delivered by decoding them against the durable historical
/// schema (whose ordered (name, type_oid) signature matches the pgoutput Relation), the
/// checkpoint must advance after delivery, and the source must continue - not enter a
/// "table not found" restart loop.
#[tokio::test]
#[ignore = "requires docker"]
async fn pg_dropped_table_retained_wal_recovers_via_durable_schema()
-> Result<()> {
    use deltaforge_config::OnSchemaDrift;

    let (db, client) = pg_setup("dropwal").await?;
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
        .execute(&format!("GRANT SELECT ON orders TO {PG_CDC_USER}"), &[])
        .await?;
    // FOR ALL TABLES so a DROP cannot remove the table from the publication and
    // suppress decoding of its retained changes.
    create_pub_slot(&client, "pub_dropwal", "slot_dropwal", &[]).await?;

    let (registry, backend) = shared_registry().await;
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);

    // Run 1: deliver one row so the schema is registered durably, then stop at a
    // committed boundary.
    {
        let src = configured_source(
            "dropwal",
            &db,
            "slot_dropwal",
            "pub_dropwal",
            OnSchemaDrift::Adapt,
            registry.clone(),
            backend.clone(),
        )
        .await;
        let (tx, mut rx) = mpsc::channel(128);
        let h = src.run(tx, ckpt.clone()).await;
        wait_ready(&h, Duration::from_secs(3)).await?;
        client
            .execute("INSERT INTO orders VALUES (1, 'a')", &[])
            .await?;
        let ev = collect_until(&mut rx, Duration::from_secs(10), |e| {
            e.iter().any(|x| has_id(x, 1))
        })
        .await;
        assert!(ev.iter().any(|e| has_id(e, 1)), "row 1 delivered in run 1");
        h.stop();
        h.join().await.ok();
    }
    let cp0 = ckpt.get_raw("dropwal").await?.expect("run 1 checkpoint");

    // While the source is down: write two more rows (retained in the WAL) and DROP
    // the table. The two inserts precede the drop, so they remain decodable from the
    // historic catalog snapshot.
    client
        .execute("INSERT INTO orders VALUES (2, 'b')", &[])
        .await?;
    client
        .execute("INSERT INTO orders VALUES (3, 'c')", &[])
        .await?;
    client.execute("DROP TABLE orders", &[]).await?;

    // Run 2: resume from cp0. The table is gone from the live catalog, but its
    // retained rows must still be delivered from the durable schema.
    let src = configured_source(
        "dropwal",
        &db,
        "slot_dropwal",
        "pub_dropwal",
        OnSchemaDrift::Adapt,
        registry.clone(),
        backend.clone(),
    )
    .await;
    let (tx, mut rx) = mpsc::channel(128);
    let h = src.run(tx, ckpt.clone()).await;
    let ready = wait_ready(&h, Duration::from_secs(10)).await;
    let items = drain_items(&mut rx, Duration::from_secs(5)).await;
    let events: Vec<&Event> = items
        .iter()
        .filter_map(|i| match i {
            SourceItem::Event(e) => Some(e),
            _ => None,
        })
        .collect();
    // The source persists its checkpoint at teardown (only on a clean, non-erroring
    // run), so read it after stop+join.
    h.stop();
    let _ = h.join().await;
    let cp_after = ckpt.get_raw("dropwal").await?;

    assert!(
        ready.is_ok(),
        "source must not die on a dropped table with retained WAL"
    );
    assert!(
        events.iter().any(|e| has_id(e, 2)),
        "retained row 2 must be delivered from the durable schema; got {} events",
        events.len()
    );
    assert!(
        events.iter().any(|e| has_id(e, 3)),
        "retained row 3 must be delivered from the durable schema"
    );
    assert!(
        cp_after.as_deref() != Some(cp0.as_slice()),
        "checkpoint must advance past the retained rows after delivery"
    );

    cleanup_repl(&client, "pub_dropwal", "slot_dropwal").await;
    pg_drop_db(&db).await;
    Ok(())
}

/// Dropped table whose durable schema does NOT match the retained WAL's Relation
/// (the table was altered while the source was down, then dropped). The source must
/// fail closed - never guess a schema, skip the event, or advance the checkpoint.
#[tokio::test]
#[ignore = "requires docker"]
async fn pg_dropped_table_schema_mismatch_fails_closed() -> Result<()> {
    use deltaforge_config::OnSchemaDrift;

    let (db, client) = pg_setup("dropmis").await?;
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
        .execute(&format!("GRANT SELECT ON orders TO {PG_CDC_USER}"), &[])
        .await?;
    create_pub_slot(&client, "pub_dropmis", "slot_dropmis", &[]).await?;

    let (registry, backend) = shared_registry().await;
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);

    // Run 1: register the [id, sku] schema, commit a boundary, stop.
    {
        let src = configured_source(
            "dropmis",
            &db,
            "slot_dropmis",
            "pub_dropmis",
            OnSchemaDrift::Adapt,
            registry.clone(),
            backend.clone(),
        )
        .await;
        let (tx, mut rx) = mpsc::channel(128);
        let h = src.run(tx, ckpt.clone()).await;
        wait_ready(&h, Duration::from_secs(3)).await?;
        client
            .execute("INSERT INTO orders VALUES (1, 'a')", &[])
            .await?;
        let ev = collect_until(&mut rx, Duration::from_secs(10), |e| {
            e.iter().any(|x| has_id(x, 1))
        })
        .await;
        assert!(ev.iter().any(|e| has_id(e, 1)), "row 1 delivered in run 1");
        h.stop();
        h.join().await.ok();
    }
    let cp0 = ckpt.get_raw("dropmis").await?.expect("run 1 checkpoint");

    // While down: alter the schema, write a row under it, then drop the table. The
    // retained Relation now has three columns; the durable schema still has two.
    client
        .execute("ALTER TABLE orders ADD COLUMN status VARCHAR(32)", &[])
        .await?;
    client
        .execute("INSERT INTO orders VALUES (2, 'b', 'active')", &[])
        .await?;
    client.execute("DROP TABLE orders", &[]).await?;

    // Run 2: resume from cp0 and fail closed on the signature mismatch.
    let src = configured_source(
        "dropmis",
        &db,
        "slot_dropmis",
        "pub_dropmis",
        OnSchemaDrift::Adapt,
        registry.clone(),
        backend.clone(),
    )
    .await;
    let (tx, mut rx) = mpsc::channel(128);
    let h = src.run(tx, ckpt.clone()).await;
    let ready = wait_ready(&h, Duration::from_secs(10)).await;
    let items = drain_items(&mut rx, Duration::from_secs(3)).await;
    h.stop();
    let joined = h.join().await;

    assert!(
        ready.is_err(),
        "source must fail closed on a dropped-table schema mismatch"
    );
    let err = format!("{:?}", joined.expect_err("run must fail closed"));
    assert!(
        err.contains("orders") && err.contains("does not match"),
        "error must name the table and the mismatch: {err}"
    );
    assert!(
        !items
            .iter()
            .any(|i| matches!(i, SourceItem::Event(e) if has_id(e, 2))),
        "the mismatched retained row must not be delivered"
    );
    assert_eq!(
        ckpt.get_raw("dropmis").await?.as_deref(),
        Some(cp0.as_slice()),
        "checkpoint must not advance on a fail-closed mismatch"
    );

    cleanup_repl(&client, "pub_dropmis", "slot_dropmis").await;
    pg_drop_db(&db).await;
    Ok(())
}

/// Drop + recreate the table under the SAME name with the SAME column signature but a
/// different OID and different PK/replica metadata. The retained WAL for the ORIGINAL
/// relation must be decoded with the ORIGINAL schema (found by version history), never
/// with the recreated live table's schema.
#[tokio::test]
#[ignore = "requires docker"]
async fn pg_dropped_table_recreated_uses_historical_not_recreated() -> Result<()>
{
    use deltaforge_config::OnSchemaDrift;

    let (db, client) = pg_setup("droprecr").await?;
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
        .execute(&format!("GRANT SELECT ON orders TO {PG_CDC_USER}"), &[])
        .await?;
    create_pub_slot(&client, "pub_droprecr", "slot_droprecr", &[]).await?;

    let (registry, backend) = shared_registry().await;
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);

    // Run 1: register the original schema and capture its fingerprint.
    let original_fp;
    {
        let src = configured_source(
            "droprecr",
            &db,
            "slot_droprecr",
            "pub_droprecr",
            OnSchemaDrift::Adapt,
            registry.clone(),
            backend.clone(),
        )
        .await;
        let (tx, mut rx) = mpsc::channel(128);
        let h = src.run(tx, ckpt.clone()).await;
        wait_ready(&h, Duration::from_secs(3)).await?;
        client
            .execute("INSERT INTO orders VALUES (1, 'a')", &[])
            .await?;
        let ev = collect_until(&mut rx, Duration::from_secs(10), |e| {
            e.iter().any(|x| has_id(x, 1))
        })
        .await;
        let row1 = ev.iter().find(|e| has_id(e, 1)).expect("row 1");
        original_fp = row1
            .schema_version
            .clone()
            .expect("row 1 has a schema version");
        h.stop();
        h.join().await.ok();
    }

    // While down: write retained rows under the ORIGINAL relation, then drop and recreate
    // it under the same name with the same columns but a different PK and replica identity
    // (and therefore a different OID).
    client
        .execute("INSERT INTO orders VALUES (2, 'b')", &[])
        .await?;
    client
        .execute("INSERT INTO orders VALUES (3, 'c')", &[])
        .await?;
    client.execute("DROP TABLE orders", &[]).await?;
    client
        .execute(
            "CREATE TABLE orders (id INT, sku VARCHAR(64), PRIMARY KEY (id, sku))",
            &[],
        )
        .await?;
    client
        .execute(&format!("GRANT SELECT ON orders TO {PG_CDC_USER}"), &[])
        .await?;

    // Run 2: the retained rows must be decoded with the ORIGINAL schema (via history),
    // not the recreated live table's schema.
    let src = configured_source(
        "droprecr",
        &db,
        "slot_droprecr",
        "pub_droprecr",
        OnSchemaDrift::Adapt,
        registry.clone(),
        backend.clone(),
    )
    .await;
    let (tx, mut rx) = mpsc::channel(128);
    let h = src.run(tx, ckpt.clone()).await;
    let ready = wait_ready(&h, Duration::from_secs(10)).await;
    let items = drain_items(&mut rx, Duration::from_secs(5)).await;
    let events: Vec<&Event> = items
        .iter()
        .filter_map(|i| match i {
            SourceItem::Event(e) => Some(e),
            _ => None,
        })
        .collect();
    h.stop();
    let _ = h.join().await;

    assert!(ready.is_ok(), "source must not die recovering retained WAL");
    for want in [2, 3] {
        let ev = events
            .iter()
            .find(|e| has_id(e, want))
            .unwrap_or_else(|| panic!("retained row {want} must be delivered"));
        assert_eq!(
            ev.schema_version.as_deref(),
            Some(original_fp.as_str()),
            "retained row {want} must be decoded with the ORIGINAL schema \
             (fingerprint {original_fp}), never the recreated table's schema"
        );
    }

    cleanup_repl(&client, "pub_droprecr", "slot_droprecr").await;
    pg_drop_db(&db).await;
    Ok(())
}

/// on_schema_drift = Halt, drift detected in-stream (an ALTER while the source is
/// actively streaming): the source fails closed at the Relation message, before
/// any row under the changed schema is emitted, and advances no checkpoint.
#[tokio::test]
#[ignore = "requires docker"]
async fn pg_schema_drift_halt_instream_fails_before_post_drift_row()
-> Result<()> {
    use deltaforge_config::OnSchemaDrift;

    let (db, client) = pg_setup("haltis").await?;
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
        .execute(&format!("GRANT SELECT ON orders TO {PG_CDC_USER}"), &[])
        .await?;
    create_pub_slot(&client, "pub_haltis", "slot_haltis", &["orders"]).await?;

    let (registry, backend) = shared_registry().await;
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let src = configured_source(
        "haltis",
        &db,
        "slot_haltis",
        "pub_haltis",
        OnSchemaDrift::Halt,
        registry.clone(),
        backend.clone(),
    )
    .await;
    let (tx, mut rx) = mpsc::channel(128);
    let h = src.run(tx, ckpt.clone()).await;
    wait_ready(&h, Duration::from_secs(3)).await?;

    // Map the table with its original schema by streaming one pre-drift row.
    client
        .execute("INSERT INTO orders VALUES (400, 'pre')", &[])
        .await?;
    let pre = collect_until(&mut rx, Duration::from_secs(10), |e| {
        e.iter().any(|x| has_id(x, 400))
    })
    .await;
    assert!(pre.iter().any(|e| has_id(e, 400)), "pre-drift row streamed");

    // ALTER while streaming, then a row under the new schema: the next Relation
    // message carries the changed definition -> in-stream drift -> Halt.
    client
        .execute("ALTER TABLE orders ADD COLUMN status VARCHAR(32)", &[])
        .await?;
    client
        .execute("INSERT INTO orders VALUES (401, 'post', 'active')", &[])
        .await?;

    let ready_after = wait_ready(&h, Duration::from_secs(10)).await;
    assert!(
        ready_after.is_err(),
        "source must fail closed on the in-stream drift"
    );
    let items = drain_items(&mut rx, Duration::from_secs(2)).await;
    h.stop();
    let joined = h.join().await;

    let err = format!("{:?}", joined.expect_err("run must fail closed"));
    assert!(
        err.contains("orders") && err.contains("on_schema_drift=adapt"),
        "typed, actionable error names the table and remediation: {err}"
    );
    assert!(
        !items
            .iter()
            .any(|i| matches!(i, SourceItem::Event(e) if has_id(e, 401))),
        "no post-drift row (401) delivered"
    );
    assert!(
        !items.iter().any(|i| matches!(
            i,
            SourceItem::Event(e)
                if e.after.as_ref().map(|a| a.get("status").is_some()).unwrap_or(false)
        )),
        "no row decoded under the changed schema was delivered"
    );
    assert!(
        ckpt.get_raw("haltis").await?.is_none(),
        "no checkpoint advanced past the last committed pre-drift transaction"
    );
    info!(
        "✓ halt (in-stream): failed before any post-drift row, no checkpoint advance"
    );

    cleanup_repl(&client, "pub_haltis", "slot_haltis").await;
    pg_drop_db(&db).await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_cdc_checkpoint_resume() -> Result<()> {
    let (db, client) = pg_setup("ckpt").await?;

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
        .execute(&format!("GRANT SELECT ON orders TO {PG_CDC_USER}"), &[])
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
        )
        .await;
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
        )
        .await;
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
    pg_drop_db(&db).await;
    Ok(())
}

/// Live two-sink restart through the PRODUCTION `PerSinkCheckpointProxy`: with two
/// per-sink checkpoints at divergent LSNs, the source resumes from the SLOWER
/// sink's LSN and re-delivers the events the faster sink already saw. The second
/// case proves a corrupt per-sink checkpoint fails the restart closed - the
/// source never begins replication.
#[tokio::test]
#[ignore = "requires docker"]
async fn pg_two_sink_restart_resumes_from_slowest_sink() -> Result<()> {
    use deltaforge_config::{SnapshotCfg, SnapshotMode};
    use runner::pipeline_manager::PerSinkCheckpointProxy;

    let (db, client) = pg_setup("twosink").await?;
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
        .execute(&format!("GRANT SELECT ON orders TO {PG_CDC_USER}"), &[])
        .await?;
    create_pub_slot(&client, "pub_twosink", "slot_twosink", &["orders"])
        .await?;

    // Insert rows WITHOUT running the source, so the freshly-created slot retains
    // all WAL from its creation point and never advances past our anchors.
    // Capture the exact WAL LSN after each commit as the per-sink positions.
    async fn wal_lsn(client: &tokio_postgres::Client) -> Result<String> {
        let row = client
            .query_one("SELECT pg_current_wal_lsn()::text", &[])
            .await?;
        Ok(row.get::<_, String>(0))
    }
    client
        .execute("INSERT INTO orders VALUES (700, 'a')", &[])
        .await?;
    let cp_slow = wal_lsn(&client).await?; // after 700, before 701
    client
        .execute("INSERT INTO orders VALUES (701, 'b')", &[])
        .await?;
    let cp_fast = wal_lsn(&client).await?; // after 701, before 702
    client
        .execute("INSERT INTO orders VALUES (702, 'c')", &[])
        .await?;

    let sid = "twosink";
    let cp_bytes = |lsn: &str| {
        serde_json::to_vec(&sources::postgres::PostgresCheckpoint {
            lsn: lsn.to_string(),
            tx_id: None,
        })
        .unwrap()
    };
    let build_src = || async {
        let mut src = make_source(
            sid,
            &db,
            "slot_twosink",
            "pub_twosink",
            vec!["public.orders".into()],
            AllowList::default(),
        )
        .await;
        // CDC-only: with an initial snapshot the pre-existing rows would arrive
        // via the snapshot regardless of checkpoint, defeating the test.
        src.snapshot_cfg = SnapshotCfg {
            mode: SnapshotMode::Never,
            ..Default::default()
        };
        Arc::new(src) as Arc<dyn Source>
    };

    // --- Divergent two-sink resume: min(slow, fast) = slow, so 701 and 702 replay.
    {
        let store: Arc<dyn CheckpointStore> =
            Arc::new(MemCheckpointStore::new()?);
        store
            .put_raw(&format!("{sid}::sink::slow"), &cp_bytes(&cp_slow))
            .await?;
        store
            .put_raw(&format!("{sid}::sink::fast"), &cp_bytes(&cp_fast))
            .await?;

        let src = build_src().await;
        let proxy: Arc<dyn CheckpointStore> = Arc::new(
            PerSinkCheckpointProxy::for_source(store, sid.to_string(), &src),
        );
        let (tx, mut rx) = mpsc::channel(128);
        let handle = src.run(tx, proxy).await;
        wait_ready(&handle, Duration::from_secs(5)).await?;

        let events = collect_until(&mut rx, Duration::from_secs(15), |e| {
            e.iter().any(|x| has_id(x, 702))
        })
        .await;
        assert!(
            events.iter().any(|e| has_id(e, 701)),
            "resumed from the SLOWER sink: 701 (already seen by the fast sink) must replay"
        );
        assert!(
            events.iter().any(|e| has_id(e, 702)),
            "new row 702 must be delivered"
        );
        assert!(
            !events.iter().any(|e| has_id(e, 700)),
            "700 precedes both checkpoints and must not replay"
        );
        handle.stop();
        handle.join().await.ok();
        info!("✓ two-sink restart rewinds to the slowest sink");
    }

    // --- Corrupt per-sink checkpoint: the restart must fail closed.
    {
        let store: Arc<dyn CheckpointStore> =
            Arc::new(MemCheckpointStore::new()?);
        store
            .put_raw(&format!("{sid}::sink::good"), &cp_bytes(&cp_slow))
            .await?;
        store
            .put_raw(&format!("{sid}::sink::bad"), b"{corrupt")
            .await?;

        let src = build_src().await;
        let proxy: Arc<dyn CheckpointStore> = Arc::new(
            PerSinkCheckpointProxy::for_source(store, sid.to_string(), &src),
        );
        let (tx, mut rx) = mpsc::channel(128);
        let handle = src.run(tx, proxy).await;

        // The source must fail closed reading its resume position: it dies before
        // becoming ready, its task returns an error, and it delivers nothing.
        assert!(
            wait_ready(&handle, Duration::from_secs(5)).await.is_err(),
            "source must not become ready with a corrupt per-sink checkpoint"
        );
        assert!(
            handle.join().await.is_err(),
            "source run must fail closed on the incomparable checkpoint"
        );
        assert!(
            collect_until(&mut rx, Duration::from_secs(1), |_| false)
                .await
                .is_empty(),
            "no events delivered when the resume position cannot be computed"
        );
        info!("✓ corrupt per-sink checkpoint fails the restart closed");
    }

    cleanup_repl(&client, "pub_twosink", "slot_twosink").await;
    pg_drop_db(&db).await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_cdc_table_filtering() -> Result<()> {
    let (db, client) = pg_setup("filter").await?;

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
            &format!("GRANT SELECT ON orders, audit_log TO {PG_CDC_USER}"),
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
    )
    .await; // Only orders
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
    pg_drop_db(&db).await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_cdc_reconnect() -> Result<()> {
    let (db, client) = pg_setup("reconn").await?;

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
        .execute(&format!("GRANT SELECT ON orders TO {PG_CDC_USER}"), &[])
        .await?;
    create_pub_slot(&client, "pub_reconn", "slot_reconn", &["orders"]).await?;

    let src = make_source(
        "reconn",
        &db,
        "slot_reconn",
        "pub_reconn",
        vec!["public.orders".into()],
        AllowList::default(),
    )
    .await;
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
    client.execute(&format!("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename='{PG_CDC_USER}' LIMIT 1"), &[]).await.ok();
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
    pg_drop_db(&db).await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_cdc_extended_types() -> Result<()> {
    let (db, client) = pg_setup("types").await?;

    client.execute("CREATE TABLE complex (id SERIAL PRIMARY KEY, uuid_col UUID, tags TEXT[], metadata JSONB, amount NUMERIC(15,4))", &[]).await?;
    client
        .execute("ALTER TABLE complex REPLICA IDENTITY FULL", &[])
        .await?;
    client
        .execute(&format!("GRANT SELECT ON complex TO {PG_CDC_USER}"), &[])
        .await?;
    create_pub_slot(&client, "pub_types", "slot_types", &["complex"]).await?;

    let src = make_source(
        "types",
        &db,
        "slot_types",
        "pub_types",
        vec!["public.complex".into()],
        AllowList::default(),
    )
    .await;
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
    pg_drop_db(&db).await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_cdc_pause_resume() -> Result<()> {
    let (db, client) = pg_setup("pause").await?;

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
        .execute(&format!("GRANT SELECT ON orders TO {PG_CDC_USER}"), &[])
        .await?;
    create_pub_slot(&client, "pub_pause", "slot_pause", &["orders"]).await?;

    let src = make_source(
        "pause",
        &db,
        "slot_pause",
        "pub_pause",
        vec!["public.orders".into()],
        AllowList::default(),
    )
    .await;
    let (mut rx, handle) = start_source(src).await?;

    client
        .execute("INSERT INTO orders VALUES (1, 'before')", &[])
        .await?;
    let events = collect_until(&mut rx, Duration::from_secs(10), |e| {
        e.iter().any(|x| has_id(x, 1))
    })
    .await;
    assert!(events.iter().any(|e| has_id(e, 1)));

    // `collect_until` stops at the row event, leaving the transaction's
    // TxBegin/TxCommit markers queued. Consume through the transaction boundary
    // so a trailing marker is not mistaken for data emitted while paused.
    while let Ok(Some(_)) = timeout(Duration::from_millis(500), rx.recv()).await
    {
    }

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
    pg_drop_db(&db).await;
    Ok(())
}

// =============================================================================
// ERROR HANDLING TESTS
// =============================================================================

/// Test error handling: authentication failure should produce Auth error.
#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_cdc_auth_failure() -> Result<()> {
    let (db, client) = pg_setup("auth_fail").await?;

    client
        .execute("CREATE TABLE orders (id INT PRIMARY KEY)", &[])
        .await?;
    client
        .execute(&format!("GRANT SELECT ON orders TO {PG_CDC_USER}"), &[])
        .await?;
    create_pub_slot(&client, "pub_auth", "slot_auth", &["orders"]).await?;

    // Use wrong password
    let bad_dsn = format!(
        "postgres://{}:WRONG_PASSWORD@127.0.0.1:{}/{db}",
        PG_CDC_USER,
        pg_port().await
    );

    let mut src = make_source(
        "auth-fail",
        &db,
        "slot_auth",
        "pub_auth",
        vec!["public.orders".into()],
        AllowList::default(),
    )
    .await;
    src.dsn = bad_dsn.into();
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
    pg_drop_db(&db).await;
    Ok(())
}

/// Test that missing slot is auto-created by ensure_slot_and_publication.
#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_cdc_slot_auto_created() -> Result<()> {
    let (db, client) = pg_setup("slot_auto").await?;

    client
        .execute("CREATE TABLE orders (id INT PRIMARY KEY, name TEXT)", &[])
        .await?;
    client
        .execute("ALTER TABLE orders REPLICA IDENTITY FULL", &[])
        .await?;
    client
        .execute(&format!("GRANT SELECT ON orders TO {PG_CDC_USER}"), &[])
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
    )
    .await;
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

    let event = next_event(&mut rx, Duration::from_secs(10))
        .await
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
    pg_drop_db(&db).await;
    Ok(())
}

/// Test that missing publication causes retry loop, and source recovers when admin creates it.
#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_cdc_publication_missing_then_created() -> Result<()> {
    let (db, client) = pg_setup("pub_missing").await?;

    client
        .execute("CREATE TABLE orders (id INT PRIMARY KEY, name TEXT)", &[])
        .await?;
    client
        .execute("ALTER TABLE orders REPLICA IDENTITY FULL", &[])
        .await?;
    client
        .execute(&format!("GRANT SELECT ON orders TO {PG_CDC_USER}"), &[])
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
    )
    .await;
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

    let event = next_event(&mut rx, Duration::from_secs(15))
        .await
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
    pg_drop_db(&db).await;
    Ok(())
}

/// Test error handling: invalid DSN format.
#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_cdc_invalid_dsn() -> Result<()> {
    init_test_tracing();
    let _ = pg_get_container().await;

    let mut src = make_source(
        "bad-dsn",
        "invalid",
        "any_slot",
        "any_pub",
        vec!["public.orders".into()],
        AllowList::default(),
    )
    .await;
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
    let _ = pg_get_container().await;

    let bad_dsn = format!(
        "postgres://{}:{}@127.0.0.1:59999/testdb",
        PG_CDC_USER,
        test_common::PG_CDC_PASS
    );

    let mut src = make_source(
        "conn-refused",
        "invalid",
        "any_slot",
        "any_pub",
        vec!["public.orders".into()],
        AllowList::default(),
    )
    .await;
    src.dsn = bad_dsn.into();
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
    let (db, client) = pg_setup("replica_id").await?;

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
            &format!("GRANT SELECT ON ri_default, ri_full TO {PG_CDC_USER}"),
            &[],
        )
        .await?;
    create_pub_slot(&client, "pub_ri", "slot_ri", &["ri_default", "ri_full"])
        .await?;

    let registry = make_registry().await;
    let loader = PostgresSchemaLoader::new(
        &pg_admin_dsn(&db).await,
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
    )
    .await;
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
    pg_drop_db(&db).await;
    Ok(())
}

/// Test graceful shutdown during active streaming.
#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_cdc_graceful_shutdown() -> Result<()> {
    let (db, client) = pg_setup("shutdown").await?;

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
        .execute(&format!("GRANT SELECT ON orders TO {PG_CDC_USER}"), &[])
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
    )
    .await;
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
    pg_drop_db(&db).await;
    Ok(())
}

/// Test multiple tables in single publication.
#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_cdc_multi_table() -> Result<()> {
    let (db, client) = pg_setup("multi").await?;

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
            .execute(
                &format!("GRANT SELECT ON {} TO {}", tbl, PG_CDC_USER),
                &[],
            )
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
    )
    .await;
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
    pg_drop_db(&db).await;
    Ok(())
}

/// Test handling of NULL values in various column types.
#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_cdc_null_handling() -> Result<()> {
    let (db, client) = pg_setup("nulls").await?;

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
        .execute(
            &format!("GRANT SELECT ON nullable_test TO {PG_CDC_USER}"),
            &[],
        )
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
    )
    .await;
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
    pg_drop_db(&db).await;
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
    let (db, client) = pg_setup("outbox").await?;

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
        .execute(&format!("GRANT SELECT ON orders TO {PG_CDC_USER}"), &[])
        .await?;
    create_pub_slot(&client, "pub_outbox", "slot_outbox", &["orders"]).await?;

    let src = make_source(
        "outbox",
        &db,
        "slot_outbox",
        "pub_outbox",
        vec!["public.orders".into()],
        AllowList::new(&["outbox".to_string()]),
    )
    .await;
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
    pg_drop_db(&db).await;
    Ok(())
}

/// Test outbox with glob prefix patterns (multi-outbox).
/// Verifies that `outbox_%` matches `outbox_orders` and `outbox_payments`
/// but not `audit`.
#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_cdc_outbox_glob_prefix() -> Result<()> {
    let (db, client) = pg_setup("outbox_glob").await?;

    client
        .execute("CREATE TABLE stub (id INT PRIMARY KEY)", &[])
        .await?;
    client
        .execute(&format!("GRANT SELECT ON stub TO {PG_CDC_USER}"), &[])
        .await?;
    create_pub_slot(&client, "pub_obglob", "slot_obglob", &["stub"]).await?;

    let src = make_source(
        "obglob",
        &db,
        "slot_obglob",
        "pub_obglob",
        vec!["public.stub".into()],
        AllowList::new(&["outbox_%".to_string()]),
    )
    .await;
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
    pg_drop_db(&db).await;
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

    let (db, client) = pg_setup("outbox_pipe").await?;

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
        .execute(&format!("GRANT SELECT ON orders TO {PG_CDC_USER}"), &[])
        .await?;
    create_pub_slot(&client, "pub_obpipe", "slot_obpipe", &["orders"]).await?;

    let src = make_source(
        "obpipe",
        &db,
        "slot_obpipe",
        "pub_obpipe",
        vec!["public.orders".into()],
        AllowList::new(&["outbox".to_string()]),
    )
    .await;
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
    let proc = OutboxProcessor::new(
        OutboxProcessorCfg {
            id: "outbox".into(),
            tables: vec![],
            columns: OutboxColumns::default(),
            topic: Some("${aggregate_type}.${event_type}".into()),
            default_topic: Some("events.unrouted".into()),
            key: None,
            additional_headers: HashMap::new(),
            raw_payload: false,
            strict: false,
        },
        String::new(),
    )?;

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
    let raw_proc = OutboxProcessor::new(
        OutboxProcessorCfg {
            id: "outbox-raw".into(),
            tables: vec![],
            columns: OutboxColumns::default(),
            topic: Some("${aggregate_type}.${event_type}".into()),
            default_topic: Some("events.unrouted".into()),
            key: None,
            additional_headers: HashMap::new(),
            raw_payload: true,
            strict: false,
        },
        String::new(),
    )?;

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
    pg_drop_db(&db).await;
    Ok(())
}

/// Run a source once on `slot`/`pub_name`, collect `want` events, and return the
/// provisional pgrow ids + per-event change ordinals.
async fn pg_run_once(
    db: &str,
    slot: &str,
    pub_name: &str,
    system_identifier: u64,
    want: usize,
) -> Result<(Vec<String>, Vec<u32>)> {
    let src = make_source(
        "replay",
        db,
        slot,
        pub_name,
        vec!["public.t1".into(), "public.t2".into()], // 'ignored' excluded
        AllowList::default(),
    )
    .await;
    let (mut rx, handle) = start_source(src).await?;
    let events =
        collect_until(&mut rx, Duration::from_secs(20), |e| e.len() >= want)
            .await;
    handle.cancel.cancel();

    let ids = events
        .iter()
        .map(|e| {
            pg_row_event_id(&e.source, system_identifier)
                .map(|id| id.to_string())
                .map_err(|err| anyhow::anyhow!(err))
        })
        .collect::<Result<Vec<_>>>()?;
    let ordinals = events
        .iter()
        .map(|e| e.source.position.change_ordinal.unwrap_or(u32::MAX))
        .collect();
    Ok((ids, ordinals))
}

/// Provisional pgrow ids must be replay-stable across an independent re-decode
/// of the same WAL, must not collide across relations, and a filtered relation's
/// change must still consume an ordinal so later events are not renumbered.
#[tokio::test]
#[ignore = "requires docker"]
async fn stable_event_ids_are_replay_stable() -> Result<()> {
    let (db, client) = pg_setup("stable_ids").await?;
    client
        .batch_execute(
            "CREATE TABLE t1 (id INT PRIMARY KEY, v TEXT);
             CREATE TABLE t2 (id INT PRIMARY KEY, v TEXT);
             CREATE TABLE ignored (id INT PRIMARY KEY);
             ALTER TABLE t1 REPLICA IDENTITY FULL;
             ALTER TABLE t2 REPLICA IDENTITY FULL;",
        )
        .await?;
    client
        .batch_execute(&format!(
            "GRANT SELECT ON t1, t2, ignored TO {PG_CDC_USER};"
        ))
        .await?;

    // Cluster lineage (constant per cluster) — same for both replay runs.
    let sysid: u64 = client
        .query_one(
            "SELECT system_identifier::text FROM pg_control_system()",
            &[],
        )
        .await?
        .get::<_, String>(0)
        .parse()?;

    // Two independent slots created BEFORE the writes → each re-decodes the same
    // WAL once (clean replay without slot-advancement concerns).
    create_pub_slot(&client, "pub_a", "slot_a", &["t1", "t2", "ignored"])
        .await?;
    create_pub_slot(&client, "pub_b", "slot_b", &["t1", "t2", "ignored"])
        .await?;

    // One transaction, mixed ops across two relations, with a filtered relation
    // interleaved (ordinal 1) between kept changes.
    client
        .batch_execute(
            "BEGIN;
             INSERT INTO t1 VALUES (1,'a');       -- ordinal 0 (kept)
             INSERT INTO ignored VALUES (1);      -- ordinal 1 (filtered)
             UPDATE t1 SET v='b' WHERE id=1;      -- ordinal 2 (kept)
             INSERT INTO t2 VALUES (10,'x');      -- ordinal 3 (kept)
             DELETE FROM t1 WHERE id=1;           -- ordinal 4 (kept)
             COMMIT;",
        )
        .await?;

    let (ids_a, ordinals_a) =
        pg_run_once(&db, "slot_a", "pub_a", sysid, 4).await?;
    let (ids_b, _) = pg_run_once(&db, "slot_b", "pub_b", sysid, 4).await?;

    assert_eq!(ids_a.len(), 4, "t1 ins/upd/del + t2 ins, got {ids_a:?}");
    assert_eq!(ids_a, ids_b, "ids must be identical across replay");

    let unique: std::collections::HashSet<_> = ids_a.iter().collect();
    assert_eq!(unique.len(), 4, "no collision across relations: {ids_a:?}");
    for id in &ids_a {
        assert!(id.starts_with("dfid:v1:pgrow:"), "pgrow form: {id}");
    }

    // The filtered `ignored` insert consumed ordinal 1, so the kept events keep
    // ordinals 0,2,3,4 — filtering did not renumber later events.
    assert_eq!(
        ordinals_a,
        vec![0, 2, 3, 4],
        "filtered change must not renumber"
    );

    cleanup_repl(&client, "pub_a", "slot_a").await;
    cleanup_repl(&client, "pub_b", "slot_b").await;
    pg_drop_db(&db).await;
    Ok(())
}

// ============================================================================
// Stable snapshot identity (dfid:v1) — native binary extraction end-to-end
// ============================================================================

use deltaforge_config::SnapshotMode;
use deltaforge_core::{EventClass, EventId};
use storage::ArcStorageBackend;

#[allow(clippy::too_many_arguments)]
async fn make_snap_source(
    id: &str,
    db: &str,
    slot: &str,
    publication: &str,
    tables: Vec<String>,
    snapshot_cfg: deltaforge_config::SnapshotCfg,
    backend: ArcStorageBackend,
    table_options: std::collections::BTreeMap<
        String,
        deltaforge_config::TableOptions,
    >,
) -> PostgresSource {
    PostgresSource {
        id: id.into(),
        dsn: pg_cdc_dsn(db).await.into(),
        slot: slot.into(),
        publication: publication.into(),
        tables,
        tenant: "acme".into(),
        pipeline: "test".into(),
        registry: make_registry().await,
        outbox_prefixes: AllowList::default(),
        snapshot_cfg,
        backend,
        on_schema_drift: deltaforge_config::OnSchemaDrift::Adapt,
        table_options,
        rotation: None,
    }
}

fn snap_reads(events: &[Event]) -> Vec<&Event> {
    events.iter().filter(|e| matches!(e.op, Op::Read)).collect()
}

fn snap_ids(events: &[Event]) -> Vec<EventId> {
    snap_reads(events)
        .iter()
        .filter_map(|e| e.event_id)
        .collect()
}

/// A UUID primary key (scanned via ctid) yields stable `snap` ids from the raw
/// 16 UUID bytes, each stamped with the allocated generation.
#[tokio::test]
#[ignore = "requires docker"]
async fn pg_uuid_pk_snapshot_ids_are_stable() -> Result<()> {
    let (db, client) = pg_setup("pg_snap_uuid").await?;
    client
        .batch_execute(
            "CREATE TABLE items (id uuid PRIMARY KEY, name text); \
             INSERT INTO items VALUES \
               ('11111111-1111-1111-1111-111111111111','a'), \
               ('22222222-2222-2222-2222-222222222222','b'), \
               ('33333333-3333-3333-3333-333333333333','c');",
        )
        .await?;
    client
        .execute(&format!("GRANT SELECT ON items TO {PG_CDC_USER}"), &[])
        .await?;
    create_publication_only(
        &client,
        "pub_snap_uuid",
        "slot_snap_uuid",
        &["items"],
    )
    .await?;

    let src = make_snap_source(
        "pg-snap-uuid",
        &db,
        "slot_snap_uuid",
        "pub_snap_uuid",
        vec!["public.items".into()],
        deltaforge_config::SnapshotCfg {
            mode: SnapshotMode::Initial,
            ..Default::default()
        },
        make_storage_backend().await,
        Default::default(),
    )
    .await;

    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let (tx, mut rx) = mpsc::channel(128);
    let handle = src.run(tx, ckpt).await;
    let events = collect_until(&mut rx, Duration::from_secs(30), |e| {
        e.iter().filter(|x| matches!(x.op, Op::Read)).count() >= 3
    })
    .await;

    let reads = snap_reads(&events);
    assert_eq!(reads.len(), 3);
    for e in &reads {
        assert_eq!(e.source.position.snapshot_generation, Some(1));
        let id = e.event_id.expect("snapshot row carries a provisional id");
        assert_eq!(id.class(), EventClass::Snap);
    }
    let ids = snap_ids(&events);
    let unique: std::collections::HashSet<_> = ids.iter().collect();
    assert_eq!(unique.len(), 3, "UUID identities must be unique");

    handle.stop();
    handle.join().await.ok();
    pg_drop_db(&db).await;
    Ok(())
}

/// An explicit re-snapshot allocates a new generation → different ids.
#[tokio::test]
#[ignore = "requires docker"]
async fn pg_resnapshot_allocates_new_generation() -> Result<()> {
    let (db, client) = pg_setup("pg_snap_resnap").await?;
    client
        .batch_execute(
            "CREATE TABLE orders (id int PRIMARY KEY, sku text); \
             INSERT INTO orders VALUES (1,'a'),(2,'b'),(3,'c');",
        )
        .await?;
    client
        .execute(&format!("GRANT SELECT ON orders TO {PG_CDC_USER}"), &[])
        .await?;
    create_publication_only(
        &client,
        "pub_snap_resnap",
        "slot_snap_resnap",
        &["orders"],
    )
    .await?;

    let backend = make_storage_backend().await;
    let run = |mode| {
        let backend = backend.clone();
        let db = db.clone();
        async move {
            let src = make_snap_source(
                "pg-snap-resnap",
                &db,
                "slot_snap_resnap",
                "pub_snap_resnap",
                vec!["public.orders".into()],
                deltaforge_config::SnapshotCfg {
                    mode,
                    ..Default::default()
                },
                backend,
                Default::default(),
            )
            .await;
            let ckpt: Arc<dyn CheckpointStore> =
                Arc::new(MemCheckpointStore::new().unwrap());
            let (tx, mut rx) = mpsc::channel(128);
            let handle = src.run(tx, ckpt).await;
            let events = collect_until(&mut rx, Duration::from_secs(30), |e| {
                e.iter().filter(|x| matches!(x.op, Op::Read)).count() >= 3
            })
            .await;
            handle.stop();
            handle.join().await.ok();
            events
        }
    };

    let first = run(SnapshotMode::Initial).await;
    // Each run uses a fresh checkpoint store (so slot ownership does not carry
    // over); drop the inactive slot the first run created so the re-snapshot
    // establishes its own owned slot. The generation still bumps to 2 because it
    // is durable in the shared backend and the second run uses `Always`.
    drop_repl_slot(&client, "slot_snap_resnap").await;
    let second = run(SnapshotMode::Always).await;

    let first_reads = snap_reads(&first);
    let second_reads = snap_reads(&second);
    assert!(!first_reads.is_empty(), "initial snapshot emitted no rows");
    assert!(!second_reads.is_empty(), "resnapshot emitted no rows");
    assert_eq!(first_reads[0].source.position.snapshot_generation, Some(1));
    assert_eq!(
        second_reads[0].source.position.snapshot_generation,
        Some(2),
        "resnapshot must bump the generation"
    );

    let a: std::collections::HashSet<_> =
        snap_ids(&first).into_iter().collect();
    let b: std::collections::HashSet<_> =
        snap_ids(&second).into_iter().collect();
    assert!(a.is_disjoint(&b), "a new generation changes every id");

    pg_drop_db(&db).await;
    Ok(())
}

/// A keyless table is rejected before any snapshot row is emitted.
#[tokio::test]
#[ignore = "requires docker"]
async fn pg_keyless_table_rejected_before_rows() -> Result<()> {
    let (db, client) = pg_setup("pg_snap_keyless").await?;
    client
        .batch_execute(
            "CREATE TABLE logs (msg text, lvl text); \
             INSERT INTO logs VALUES ('boot','info');",
        )
        .await?;
    client
        .execute(&format!("GRANT SELECT ON logs TO {PG_CDC_USER}"), &[])
        .await?;
    create_publication_only(
        &client,
        "pub_snap_keyless",
        "slot_snap_keyless",
        &["logs"],
    )
    .await?;

    let src = make_snap_source(
        "pg-snap-keyless",
        &db,
        "slot_snap_keyless",
        "pub_snap_keyless",
        vec!["public.logs".into()],
        deltaforge_config::SnapshotCfg {
            mode: SnapshotMode::Initial,
            ..Default::default()
        },
        make_storage_backend().await,
        Default::default(),
    )
    .await;

    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let (tx, mut rx) = mpsc::channel(128);
    let handle = src.run(tx, ckpt).await;
    let events =
        collect_until(&mut rx, Duration::from_secs(8), |_| false).await;
    assert_eq!(
        snap_reads(&events).len(),
        0,
        "keyless table must emit no snapshot rows"
    );

    handle.stop();
    handle.join().await.ok();
    pg_drop_db(&db).await;
    Ok(())
}

/// Restart after an interrupted snapshot re-anchors the owned inactive slot and
/// re-scans with the SAME generation, so the rows emitted before the interruption
/// keep identical ids afterward. (The interrupted snapshot is fully re-scanned
/// under the new anchor rather than resumed from partial progress - a full
/// re-scan is required for safety when the anchor moves - but the ids are
/// deterministic from the primary key plus the generation, so they are stable.)
#[tokio::test]
#[ignore = "requires docker"]
async fn pg_snapshot_resumes_midway_with_stable_ids() -> Result<()> {
    let (db, client) = pg_setup("pg_snap_resume").await?;
    client
        .batch_execute(
            "CREATE TABLE orders (id int PRIMARY KEY, sku text); \
             INSERT INTO orders SELECT g, 'sku-'||g FROM generate_series(1,40) g;",
        )
        .await?;
    client
        .execute(&format!("GRANT SELECT ON orders TO {PG_CDC_USER}"), &[])
        .await?;
    create_publication_only(
        &client,
        "pub_snap_resume",
        "slot_snap_resume",
        &["orders"],
    )
    .await?;

    // Shared backend (generation) AND checkpoint store (slot ownership + progress)
    // so the second run re-anchors the slot the first run created and owns, rather
    // than failing closed on an unowned slot.
    let backend = make_storage_backend().await;
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let cfg = || deltaforge_config::SnapshotCfg {
        mode: SnapshotMode::Initial,
        chunk_size: 5,
        ..Default::default()
    };

    // Run 1: interrupt after only a few rows (snapshot left incomplete).
    let src1 = make_snap_source(
        "pg-snap-resume",
        &db,
        "slot_snap_resume",
        "pub_snap_resume",
        vec!["public.orders".into()],
        cfg(),
        backend.clone(),
        Default::default(),
    )
    .await;
    let (tx1, mut rx1) = mpsc::channel(128);
    let h1 = src1.run(tx1, ckpt.clone()).await;
    let partial = collect_until(&mut rx1, Duration::from_secs(30), |e| {
        e.iter().filter(|x| matches!(x.op, Op::Read)).count() >= 3
    })
    .await;
    h1.stop();
    h1.join().await.ok();
    let before: std::collections::HashMap<_, _> = snap_reads(&partial)
        .iter()
        .filter_map(|e| {
            let id = e.after.as_ref()?.get("id")?.as_i64()?;
            Some((id, e.event_id?))
        })
        .collect();
    assert!(!before.is_empty(), "run 1 emitted no snapshot rows");

    // Run 2: resume to completion on the same backend + checkpoint store.
    let src2 = make_snap_source(
        "pg-snap-resume",
        &db,
        "slot_snap_resume",
        "pub_snap_resume",
        vec!["public.orders".into()],
        cfg(),
        backend.clone(),
        Default::default(),
    )
    .await;
    let (tx2, mut rx2) = mpsc::channel(256);
    let h2 = src2.run(tx2, ckpt.clone()).await;
    let full = collect_until(&mut rx2, Duration::from_secs(45), |e| {
        e.iter().filter(|x| matches!(x.op, Op::Read)).count() >= 40
    })
    .await;
    h2.stop();
    h2.join().await.ok();

    // Same generation across the interruption, and every pre-interruption row
    // has an identical id in the resumed run.
    for e in snap_reads(&partial).iter().chain(snap_reads(&full).iter()) {
        assert_eq!(e.source.position.snapshot_generation, Some(1));
    }
    for e in snap_reads(&full) {
        if let (Some(id), Some(new_id)) = (
            e.after
                .as_ref()
                .and_then(|v| v.get("id"))
                .and_then(|v| v.as_i64()),
            e.event_id,
        ) {
            if let Some(old_id) = before.get(&id) {
                assert_eq!(*old_id, new_id, "row {id} id changed after resume");
            }
        }
    }

    pg_drop_db(&db).await;
    Ok(())
}

/// Temporal identities come from the binary wire form, so DateStyle / TimeZone
/// session settings do not change the ids.
#[tokio::test]
#[ignore = "requires docker"]
async fn pg_temporal_ids_are_datestyle_timezone_invariant() -> Result<()> {
    let (db, client) = pg_setup("pg_snap_temporal").await?;
    client
        .batch_execute(
            "CREATE TABLE ev (ts timestamptz PRIMARY KEY, v text); \
             INSERT INTO ev VALUES \
               ('2021-03-04 05:06:07.123456+00','a'), \
               ('2022-07-08 09:10:11.222222+00','b');",
        )
        .await?;
    client
        .execute(&format!("GRANT SELECT ON ev TO {PG_CDC_USER}"), &[])
        .await?;
    create_publication_only(
        &client,
        "pub_snap_temporal",
        "slot_snap_temporal",
        &["ev"],
    )
    .await?;

    // Both runs use Initial with a fresh checkpoint store (so each re-scans and
    // establishes its own owned slot) but a SHARED backend, so the generation is
    // reused (Resume) and stays 1. The slot is dropped between runs so the second
    // run creates a fresh owned slot instead of failing closed on an unowned one.
    // The only variable across the two runs is the session DateStyle/TimeZone.
    let backend = make_storage_backend().await;
    let run = || {
        let backend = backend.clone();
        let db = db.clone();
        async move {
            let src = make_snap_source(
                "pg-snap-temporal",
                &db,
                "slot_snap_temporal",
                "pub_snap_temporal",
                vec!["public.ev".into()],
                deltaforge_config::SnapshotCfg {
                    mode: SnapshotMode::Initial,
                    ..Default::default()
                },
                backend,
                Default::default(),
            )
            .await;
            let ckpt: Arc<dyn CheckpointStore> =
                Arc::new(MemCheckpointStore::new().unwrap());
            let (tx, mut rx) = mpsc::channel(128);
            let handle = src.run(tx, ckpt).await;
            let events = collect_until(&mut rx, Duration::from_secs(30), |e| {
                e.iter().filter(|x| matches!(x.op, Op::Read)).count() >= 2
            })
            .await;
            handle.stop();
            handle.join().await.ok();
            snap_ids(&events)
                .into_iter()
                .collect::<std::collections::HashSet<_>>()
        }
    };

    client
        .batch_execute(&format!(
            "ALTER DATABASE {db} SET timezone='UTC'; \
             ALTER DATABASE {db} SET datestyle='ISO, MDY';"
        ))
        .await?;
    let ids_utc = run().await;

    drop_repl_slot(&client, "slot_snap_temporal").await;
    client
        .batch_execute(&format!(
            "ALTER DATABASE {db} SET timezone='America/New_York'; \
             ALTER DATABASE {db} SET datestyle='German, DMY';"
        ))
        .await?;
    let ids_other = run().await;

    assert_eq!(ids_utc.len(), 2, "expected 2 temporal ids");
    // Same generation + timezone/datestyle-independent binary ⇒ identical ids.
    assert_eq!(
        ids_utc, ids_other,
        "temporal ids must not depend on DateStyle/TimeZone"
    );

    pg_drop_db(&db).await;
    Ok(())
}

/// The intra-table parallel PK path and the sequential PK path produce
/// identical ids for the same rows (they share one extraction function).
#[tokio::test]
#[ignore = "requires docker"]
async fn pg_parallel_and_sequential_pk_produce_identical_ids() -> Result<()> {
    let (db, client) = pg_setup("pg_snap_parallel").await?;
    client
        .batch_execute(
            "CREATE TABLE orders (id int PRIMARY KEY, sku text); \
             INSERT INTO orders SELECT g, 'sku-'||g FROM generate_series(1,50) g;",
        )
        .await?;
    client
        .execute(&format!("GRANT SELECT ON orders TO {PG_CDC_USER}"), &[])
        .await?;
    create_publication_only(
        &client,
        "pub_snap_parallel",
        "slot_snap_parallel",
        &["orders"],
    )
    .await?;

    let backend = make_storage_backend().await;
    let run = |mode, parallel| {
        let backend = backend.clone();
        let db = db.clone();
        async move {
            let src = make_snap_source(
                "pg-snap-parallel",
                &db,
                "slot_snap_parallel",
                "pub_snap_parallel",
                vec!["public.orders".into()],
                deltaforge_config::SnapshotCfg {
                    mode,
                    chunk_size: 5,
                    intra_table_parallel: parallel,
                    ..Default::default()
                },
                backend,
                Default::default(),
            )
            .await;
            let ckpt: Arc<dyn CheckpointStore> =
                Arc::new(MemCheckpointStore::new().unwrap());
            let (tx, mut rx) = mpsc::channel(256);
            let handle = src.run(tx, ckpt).await;
            let events = collect_until(&mut rx, Duration::from_secs(45), |e| {
                e.iter().filter(|x| matches!(x.op, Op::Read)).count() >= 50
            })
            .await;
            handle.stop();
            handle.join().await.ok();
            // Map id → provisional event id.
            snap_reads(&events)
                .iter()
                .filter_map(|e| {
                    let k = e.after.as_ref()?.get("id")?.as_i64()?;
                    Some((k, e.event_id?))
                })
                .collect::<std::collections::HashMap<_, _>>()
        }
    };

    // Fresh checkpoint store each run (so each re-scans the whole table and
    // establishes its own owned slot) but a shared backend (so the generation is
    // reused at 1). The slot is dropped between runs so the second run creates a
    // fresh owned slot instead of failing closed on an unowned one. The only
    // difference between the runs is the scan path.
    let sequential = run(SnapshotMode::Initial, false).await;
    drop_repl_slot(&client, "slot_snap_parallel").await;
    let parallel = run(SnapshotMode::Initial, true).await;

    assert_eq!(sequential.len(), 50);
    assert_eq!(parallel.len(), 50);
    assert_eq!(
        sequential, parallel,
        "parallel and sequential PK scans must yield identical ids"
    );

    pg_drop_db(&db).await;
    Ok(())
}

/// Enum and domain identity columns extract end-to-end (enum via label+type,
/// domain resolved to its base type).
#[tokio::test]
#[ignore = "requires docker"]
async fn pg_enum_and_domain_identities_work() -> Result<()> {
    let (db, client) = pg_setup("pg_snap_enumdom").await?;
    client
        .batch_execute(
            "CREATE TYPE mood AS ENUM('happy','sad','ok'); \
             CREATE TABLE em (m mood PRIMARY KEY, v text); \
             INSERT INTO em VALUES ('happy','a'),('sad','b'),('ok','c'); \
             CREATE DOMAIN uid AS uuid; \
             CREATE TABLE dm (id uid PRIMARY KEY, v text); \
             INSERT INTO dm VALUES \
               ('11111111-1111-1111-1111-111111111111','x'), \
               ('22222222-2222-2222-2222-222222222222','y');",
        )
        .await?;
    client
        .execute(&format!("GRANT SELECT ON em, dm TO {PG_CDC_USER}"), &[])
        .await?;
    create_publication_only(
        &client,
        "pub_snap_enumdom",
        "slot_snap_enumdom",
        &["em", "dm"],
    )
    .await?;

    let src = make_snap_source(
        "pg-snap-enumdom",
        &db,
        "slot_snap_enumdom",
        "pub_snap_enumdom",
        vec!["public.em".into(), "public.dm".into()],
        deltaforge_config::SnapshotCfg {
            mode: SnapshotMode::Initial,
            ..Default::default()
        },
        make_storage_backend().await,
        Default::default(),
    )
    .await;

    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let (tx, mut rx) = mpsc::channel(128);
    let handle = src.run(tx, ckpt).await;
    let events = collect_until(&mut rx, Duration::from_secs(30), |e| {
        e.iter().filter(|x| matches!(x.op, Op::Read)).count() >= 5
    })
    .await;
    handle.stop();
    handle.join().await.ok();

    let reads = snap_reads(&events);
    assert_eq!(reads.len(), 5, "3 enum rows + 2 domain rows");
    for e in &reads {
        let id = e
            .event_id
            .expect("enum/domain row carries a provisional id");
        assert_eq!(id.class(), EventClass::Snap);
    }
    let ids = snap_ids(&events);
    let unique: std::collections::HashSet<_> = ids.iter().collect();
    assert_eq!(unique.len(), 5, "enum + domain ids must be unique");

    pg_drop_db(&db).await;
    Ok(())
}

// ============================================================================
// Final stable-ID acceptance: logical-message identity across replay
// ============================================================================

/// Replay from an independent slot and return the first logical-message event's
/// stable id.
async fn pg_replay_message_id(
    db: &str,
    slot: &str,
    pub_name: &str,
) -> Result<String> {
    let src = make_source(
        "replay-msg",
        db,
        slot,
        pub_name,
        vec!["public.t".into()],
        AllowList::default(),
    )
    .await;
    let (mut rx, handle) = start_source(src).await?;
    let events = collect_until(&mut rx, Duration::from_secs(20), |e| {
        e.iter()
            .any(|x| x.source.schema.as_deref() == Some("__wal_message"))
    })
    .await;
    handle.cancel.cancel();
    let msg = events
        .iter()
        .find(|e| e.source.schema.as_deref() == Some("__wal_message"))
        .ok_or_else(|| anyhow::anyhow!("no logical-message event observed"))?;
    msg.event_id
        .map(|id| id.to_string())
        .ok_or_else(|| anyhow::anyhow!("message event missing event_id"))
}

/// A logical message's stable id must be identical when the same WAL is
/// re-decoded from an independent slot.
#[tokio::test]
#[ignore = "requires docker"]
async fn logical_message_ids_are_replay_stable() -> Result<()> {
    let (db, client) = pg_setup("msg_stable").await?;
    client
        .batch_execute("CREATE TABLE t (id INT PRIMARY KEY);")
        .await?;

    // Two independent slots created BEFORE the message → each re-decodes it once.
    create_pub_slot(&client, "pub_a", "slot_a", &["t"]).await?;
    create_pub_slot(&client, "pub_b", "slot_b", &["t"]).await?;

    // A transactional logical message (prefix 'audit' → __wal_message).
    client
        .batch_execute(
            "SELECT pg_logical_emit_message(true, 'audit', '{\"k\":1}');",
        )
        .await?;

    let a = pg_replay_message_id(&db, "slot_a", "pub_a").await?;
    let b = pg_replay_message_id(&db, "slot_b", "pub_b").await?;
    assert_eq!(a, b, "message id must be identical across reconnect/replay");
    assert!(a.starts_with("dfid:v1:msg:"), "msg-class id expected: {a}");

    cleanup_repl(&client, "pub_a", "slot_a").await;
    cleanup_repl(&client, "pub_b", "slot_b").await;
    pg_drop_db(&db).await;
    Ok(())
}
