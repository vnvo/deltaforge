//! Source→coordinator acceptance tests for transaction-aware batching.
//!
//! These run the *real* coordinator over a *real* CDC source (MySQL / Postgres)
//! and prove the two end-to-end invariants that coordinator unit tests cannot:
//!   1. A multi-row source transaction is delivered to the sink intact - as a
//!      single batch, never split.
//!   2. After a restart the source resumes from the coordinator's *committed*
//!      checkpoint (the commit boundary), so already-committed rows are not
//!      re-read and the new transaction is delivered.
//!
//! Run with:
//! ```bash
//! cargo test -p sources --test txn_coordinator_e2e -- --include-ignored --nocapture --test-threads=1
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use checkpoints::{CheckpointResult, CheckpointStore, MemCheckpointStore};
use common::AllowList;
use ctor::dtor;
use deltaforge_config::BatchConfig;
use deltaforge_core::{
    ArcDynProcessor, ArcDynSink, BatchResult, Event, Sink, SinkResult, Source,
    SourceHandle, SourceItem,
};
use runner::{Coordinator, build_batch_processor, build_commit_fn};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

mod test_common;

#[dtor]
fn cleanup() {
    if let Some((c, _)) = test_common::PG_CONTAINER.get() {
        std::process::Command::new("docker")
            .args(["rm", "-f", c.id()])
            .output()
            .ok();
    }
    if let Some(container) = test_common::MYSQL_CONTAINER.get() {
        std::process::Command::new("docker")
            .args(["rm", "-f", container.0.id()])
            .output()
            .ok();
    }
}

// ============================================================================
// A sink that records the composition of every delivered batch.
// ============================================================================

struct RecordingSink {
    id: String,
    total: AtomicUsize,
    batches: std::sync::Mutex<Vec<Vec<i64>>>,
}

impl RecordingSink {
    fn new(id: &str) -> Arc<Self> {
        Arc::new(Self {
            id: id.to_string(),
            total: AtomicUsize::new(0),
            batches: std::sync::Mutex::new(Vec::new()),
        })
    }

    fn total(&self) -> usize {
        self.total.load(Ordering::Relaxed)
    }

    /// The event ids of each delivered batch, in delivery order.
    fn batches(&self) -> Vec<Vec<i64>> {
        self.batches.lock().unwrap().clone()
    }
}

fn event_id(e: &Event) -> Option<i64> {
    e.after
        .as_ref()
        .and_then(|v| v.get("id"))
        .and_then(|v| v.as_i64())
}

#[async_trait]
impl Sink for RecordingSink {
    fn id(&self) -> &str {
        &self.id
    }

    async fn send(&self, event: &Event) -> SinkResult<()> {
        self.send_batch(std::slice::from_ref(event))
            .await
            .map(|_| ())
    }

    async fn send_batch(&self, events: &[Event]) -> SinkResult<BatchResult> {
        let ids: Vec<i64> = events.iter().filter_map(event_id).collect();
        self.total.fetch_add(events.len(), Ordering::Relaxed);
        self.batches.lock().unwrap().push(ids);
        Ok(BatchResult::ok())
    }
}

// ============================================================================
// A checkpoint store that mirrors the production wiring: the source resumes
// from the coordinator's *committed* per-sink checkpoint, not its own read
// position. `get_raw(source_id)` is redirected to the sink's committed key.
// ============================================================================

struct CommittedCheckpointProxy {
    inner: Arc<dyn CheckpointStore>,
    source_id: String,
    sink_key: String,
}

#[async_trait]
impl CheckpointStore for CommittedCheckpointProxy {
    async fn get_raw(&self, key: &str) -> CheckpointResult<Option<Vec<u8>>> {
        if key == self.source_id {
            return self.inner.get_raw(&self.sink_key).await;
        }
        self.inner.get_raw(key).await
    }

    async fn put_raw(&self, key: &str, bytes: &[u8]) -> CheckpointResult<()> {
        self.inner.put_raw(key, bytes).await
    }

    async fn delete(&self, key: &str) -> CheckpointResult<bool> {
        self.inner.delete(key).await
    }

    async fn list(&self) -> CheckpointResult<Vec<String>> {
        self.inner.list().await
    }
}

// ============================================================================
// Pipeline harness: run one source + one coordinator over a shared store.
// ============================================================================

struct RunningPipeline {
    // Held for the pipeline's lifetime: if this sender is dropped, the
    // coordinator's `pause_rx.changed()` returns Err and its accumulation loop
    // exits, dropping the source receiver. Keeping it alive keeps the pipeline
    // running until we explicitly shut the source down.
    _pause_tx: tokio::sync::watch::Sender<bool>,
    sink: Arc<RecordingSink>,
    src_handle: SourceHandle,
    coord_task: Option<JoinHandle<Result<()>>>,
}

impl RunningPipeline {
    /// Start `source` feeding a coordinator whose single `RecordingSink` commits
    /// to `{source_id}::sink::rec`. The source resumes from that committed key.
    async fn start<S: Source>(
        source: S,
        store: Arc<dyn CheckpointStore>,
        source_id: &str,
    ) -> Self {
        // Large soft limits so a small multi-row tx is never split by size; a short
        // timer flushes it once the source goes idle.
        let cfg = BatchConfig {
            max_events: Some(100_000),
            max_bytes: Some(1 << 30),
            max_ms: Some(100),
            respect_source_tx: Some(true),
            max_inflight: Some(1),
            ..BatchConfig::default()
        };
        Self::start_with_batch_config(source, store, source_id, cfg).await
    }

    /// Like [`start`], but with an explicit [`BatchConfig`] - used to drive small
    /// soft limits that make a mid-transaction split deterministic if one occurs.
    async fn start_with_batch_config<S: Source>(
        source: S,
        store: Arc<dyn CheckpointStore>,
        source_id: &str,
        cfg: BatchConfig,
    ) -> Self {
        let sink = RecordingSink::new("rec");
        let sinks: Vec<ArcDynSink> = vec![Arc::clone(&sink) as ArcDynSink];
        let cp_key = format!("{source_id}::sink::rec");
        let commit = build_commit_fn(store.clone(), cp_key.clone());
        let procs: Arc<[ArcDynProcessor]> = Arc::from(vec![]);
        let batch_processor = build_batch_processor(procs, "acc".to_string());

        let coord = Coordinator::builder("acc")
            .sinks(sinks)
            .batch_config(Some(cfg))
            .commit_fn("rec", commit)
            .process_fn(batch_processor)
            .build();

        let (tx, rx) = mpsc::channel::<SourceItem>(1024);
        let proxy: Arc<dyn CheckpointStore> =
            Arc::new(CommittedCheckpointProxy {
                inner: store,
                source_id: source_id.to_string(),
                sink_key: cp_key,
            });
        let src_handle = source.run(tx, proxy).await;

        let cancel = CancellationToken::new();
        let (pause_tx, pause_rx) = tokio::sync::watch::channel(false);
        let coord_task = tokio::spawn(coord.run(rx, cancel, pause_rx));

        RunningPipeline {
            _pause_tx: pause_tx,
            sink,
            src_handle,
            coord_task: Some(coord_task),
        }
    }

    /// Poll until the sink has delivered at least `n` rows. If the coordinator
    /// task exits early (it should run until the source stops), surface its
    /// error immediately rather than waiting out the timeout.
    async fn wait_for_rows(
        &mut self,
        n: usize,
        timeout: Duration,
    ) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            if self.sink.total() >= n {
                return Ok(());
            }
            if self.coord_task.as_ref().is_some_and(|t| t.is_finished()) {
                let task = self.coord_task.take().unwrap();
                return match task.await {
                    Ok(Ok(())) => Err(anyhow::anyhow!(
                        "coordinator exited early after {} of {n} rows",
                        self.sink.total()
                    )),
                    Ok(Err(e)) => {
                        Err(e.context("coordinator failed while streaming"))
                    }
                    Err(e) => {
                        Err(anyhow::anyhow!("coordinator task panicked: {e}"))
                    }
                };
            }
            if Instant::now() >= deadline {
                return Err(anyhow::anyhow!(
                    "timed out waiting for {n} rows (delivered {})",
                    self.sink.total()
                ));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Stop the source; the coordinator drains whole transactions, commits the
    /// boundary checkpoint, then exits. `_pause_tx` stays alive on `self` until
    /// this returns, so the coordinator never sees a spurious pause-channel
    /// close before it finishes.
    async fn shutdown(mut self) -> Result<Vec<Vec<i64>>> {
        let batches = self.sink.batches();
        self.src_handle.stop();
        let _ = self.src_handle.join().await;
        // The source dropped its sender; the coordinator finishes and returns.
        if let Some(task) = self.coord_task.take() {
            task.await??;
        }
        Ok(batches)
    }
}

// ============================================================================
// MySQL
// ============================================================================

async fn mysql_source(
    id: &str,
    dsn: &str,
    db: &str,
) -> sources::mysql::MySqlSource {
    sources::mysql::MySqlSource {
        id: id.into(),
        dsn: dsn.to_string(),
        tables: vec![format!("{db}.orders")],
        tenant: "acme".into(),
        pipeline: "acc".into(),
        registry: test_common::make_registry().await,
        outbox_tables: AllowList::default(),
        snapshot_cfg: deltaforge_config::SnapshotCfg::default(),
        backend: test_common::make_storage_backend().await,
        on_schema_drift: deltaforge_config::OnSchemaDrift::Adapt,
        table_options: Default::default(),
    }
}

#[tokio::test]
#[ignore = "requires docker"]
async fn mysql_tx_intact_and_resume_from_commit() -> Result<()> {
    use mysql_async::prelude::Queryable;
    let (db, pool, dsn) = test_common::mysql_setup("txcoord").await?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop(format!("USE {db}")).await?;
    conn.query_drop(
        "CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(64))",
    )
    .await?;

    let store: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);

    // ── Phase 1: a single 3-row transaction must arrive as one intact batch. ──
    let batches = {
        let mut pipe = RunningPipeline::start(
            mysql_source("txc", &dsn, &db).await,
            store.clone(),
            "txc",
        )
        .await;
        // Warm up so the binlog stream is established before the transaction.
        tokio::time::sleep(Duration::from_secs(3)).await;

        conn.query_drop("START TRANSACTION").await?;
        conn.query_drop("INSERT INTO orders VALUES (1,'a'),(2,'b'),(3,'c')")
            .await?;
        conn.query_drop("COMMIT").await?;

        pipe.wait_for_rows(3, Duration::from_secs(15)).await?;
        pipe.shutdown().await?
    };

    // The 3-row transaction must appear as one intact batch.
    assert!(
        batches.iter().any(|b| b == &vec![1, 2, 3]),
        "the 3-row transaction must be delivered as one intact batch, got {batches:?}"
    );

    // ── Insert a second transaction while the pipeline is down. ──
    conn.query_drop("START TRANSACTION").await?;
    conn.query_drop("INSERT INTO orders VALUES (4,'d'),(5,'e'),(6,'f')")
        .await?;
    conn.query_drop("COMMIT").await?;

    // ── Phase 2: restart resumes from the committed checkpoint. ──
    let mut pipe = RunningPipeline::start(
        mysql_source("txc", &dsn, &db).await,
        store.clone(),
        "txc",
    )
    .await;
    pipe.wait_for_rows(3, Duration::from_secs(20)).await?;
    let batches2 = pipe.shutdown().await?;

    let ids2: Vec<i64> = batches2.iter().flatten().copied().collect();
    assert!(
        ids2.contains(&4) && ids2.contains(&5) && ids2.contains(&6),
        "phase 2 must deliver the second transaction, got {batches2:?}"
    );
    assert!(
        !ids2.contains(&1) && !ids2.contains(&2) && !ids2.contains(&3),
        "resume from the commit checkpoint must not re-deliver committed rows, got {batches2:?}"
    );

    test_common::mysql_drop_db(&pool, &db).await;
    Ok(())
}

// ============================================================================
// PostgreSQL
// ============================================================================

async fn pg_source(
    id: &str,
    db: &str,
    slot: &str,
    publication: &str,
) -> sources::postgres::PostgresSource {
    sources::postgres::PostgresSource {
        id: id.into(),
        dsn: test_common::pg_cdc_dsn(db).await,
        slot: slot.into(),
        publication: publication.into(),
        tables: vec!["public.orders".into()],
        tenant: "acme".into(),
        pipeline: "acc".into(),
        registry: test_common::make_registry().await,
        outbox_prefixes: AllowList::default(),
        snapshot_cfg: deltaforge_config::SnapshotCfg::default(),
        backend: test_common::make_storage_backend().await,
        on_schema_drift: deltaforge_config::OnSchemaDrift::Adapt,
        table_options: Default::default(),
    }
}

#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_tx_intact_and_resume_from_commit() -> Result<()> {
    let (db, client) = test_common::pg_setup("txcoord").await?;
    client
        .execute("CREATE TABLE orders (id INT PRIMARY KEY, sku TEXT)", &[])
        .await?;
    client
        .execute("ALTER TABLE orders REPLICA IDENTITY FULL", &[])
        .await?;
    client
        .execute(
            &format!("GRANT SELECT ON orders TO {}", test_common::PG_CDC_USER),
            &[],
        )
        .await?;
    test_common::pg_create_pub_slot(
        &client,
        "pub_txc",
        "slot_txc",
        &["orders"],
    )
    .await?;

    let store: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);

    // ── Phase 1: a single 3-row transaction must arrive as one intact batch. ──
    let batches = {
        let mut pipe = RunningPipeline::start(
            pg_source("txc", &db, "slot_txc", "pub_txc").await,
            store.clone(),
            "txc",
        )
        .await;
        tokio::time::sleep(Duration::from_secs(3)).await;

        client
            .batch_execute(
                "BEGIN; \
                 INSERT INTO orders VALUES (1,'a'),(2,'b'),(3,'c'); \
                 COMMIT;",
            )
            .await?;

        pipe.wait_for_rows(3, Duration::from_secs(15)).await?;
        pipe.shutdown().await?
    };

    assert!(
        batches.iter().any(|b| b == &vec![1, 2, 3]),
        "the 3-row transaction must be delivered as one intact batch, got {batches:?}"
    );

    // ── Insert a second transaction while the pipeline is down. ──
    client
        .batch_execute(
            "BEGIN; \
             INSERT INTO orders VALUES (4,'d'),(5,'e'),(6,'f'); \
             COMMIT;",
        )
        .await?;

    // ── Phase 2: restart resumes from the committed checkpoint. ──
    let mut pipe = RunningPipeline::start(
        pg_source("txc", &db, "slot_txc", "pub_txc").await,
        store.clone(),
        "txc",
    )
    .await;
    pipe.wait_for_rows(3, Duration::from_secs(20)).await?;
    let batches2 = pipe.shutdown().await?;

    let ids2: Vec<i64> = batches2.iter().flatten().copied().collect();
    assert!(
        ids2.contains(&4) && ids2.contains(&5) && ids2.contains(&6),
        "phase 2 must deliver the second transaction, got {batches2:?}"
    );
    assert!(
        !ids2.contains(&1) && !ids2.contains(&2) && !ids2.contains(&3),
        "resume from the commit checkpoint must not re-deliver committed rows, got {batches2:?}"
    );

    test_common::pg_cleanup_repl(&client, "pub_txc", "slot_txc").await;
    test_common::pg_drop_db(&db).await;
    Ok(())
}

/// A PostgreSQL transactional logical message emitted *inside* a transaction must
/// not split it: the surrounding rows are delivered as one intact batch. With a
/// small soft limit, an unstamped (standalone) message would deterministically
/// flush mid-transaction and split it - provider-level proof for the fix that
/// stamps transactional logical messages with the active xid.
#[tokio::test]
#[ignore = "requires docker"]
async fn postgres_transactional_message_does_not_split_tx() -> Result<()> {
    let (db, client) = test_common::pg_setup("txcmsg").await?;
    client
        .execute("CREATE TABLE orders (id INT PRIMARY KEY, sku TEXT)", &[])
        .await?;
    client
        .execute("ALTER TABLE orders REPLICA IDENTITY FULL", &[])
        .await?;
    client
        .execute(
            &format!("GRANT SELECT ON orders TO {}", test_common::PG_CDC_USER),
            &[],
        )
        .await?;
    test_common::pg_create_pub_slot(
        &client,
        "pub_txcm",
        "slot_txcm",
        &["orders"],
    )
    .await?;

    let store: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);

    // Small soft limit: were the message treated as a standalone boundary, it would
    // flush mid-transaction (a deterministic split); with the fix the whole tx is
    // one batch regardless.
    let cfg = BatchConfig {
        max_events: Some(2),
        max_bytes: Some(1 << 30),
        max_ms: Some(100),
        respect_source_tx: Some(true),
        max_inflight: Some(1),
        ..BatchConfig::default()
    };

    let batches = {
        let mut pipe = RunningPipeline::start_with_batch_config(
            pg_source("txcm", &db, "slot_txcm", "pub_txcm").await,
            store.clone(),
            "txcm",
            cfg,
        )
        .await;
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Row 1, then a TRANSACTIONAL logical message, then rows 2 and 3 - all in one
        // transaction. pgoutput streams the message (the client sends messages 'true').
        client
            .batch_execute(
                "BEGIN; \
                 INSERT INTO orders VALUES (1,'a'); \
                 SELECT pg_logical_emit_message(true, 'audit', '{\"note\":\"m\"}'); \
                 INSERT INTO orders VALUES (2,'b'),(3,'c'); \
                 COMMIT;",
            )
            .await?;

        pipe.wait_for_rows(3, Duration::from_secs(15)).await?;
        pipe.shutdown().await?
    };

    assert!(
        batches.iter().any(|b| b == &vec![1, 2, 3]),
        "the transaction (with an interleaved transactional message) must be one \
         intact batch, got {batches:?}"
    );

    test_common::pg_cleanup_repl(&client, "pub_txcm", "slot_txcm").await;
    test_common::pg_drop_db(&db).await;
    Ok(())
}
