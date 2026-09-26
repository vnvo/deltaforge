//! Event-replay acceptance tests against a REAL PostgreSQL backend (Slice 5, Option 3).
//!
//! These exercise the replay engine (journal + durable job store + controller + real
//! coordinator-backed delivery) over a live PostgreSQL `StorageBackend`, so capture-time
//! durability and replay run against the same storage a production deployment uses.
//!
//! They are `#[ignore]`d and gated on `DELTAFORGE_IT_PG_DSN` (matching the storage crate's
//! PostgreSQL contract test); run them with, e.g.:
//!
//! ```text
//! DELTAFORGE_IT_PG_DSN='host=127.0.0.1 port=55432 user=postgres password=postgres dbname=postgres' \
//!   cargo test -p runner --test replay_pg_e2e -- --ignored
//! ```
//!
//! Each test isolates itself under a unique pipeline incarnation so cases can share one
//! database. Scenarios covered here: deterministic capture/replay, at-least-once
//! re-delivery, and restart from a durable handoff phase. Operator-pause overlap,
//! snapshot-chunk/transaction boundary quiescence, and coordinator-loss are covered by the
//! in-process unit tests in the runner crate.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use anyhow::Result;
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use deltaforge_core::replay::{
    PipelineIdentity, REPLAY_ENVELOPE_VERSION, ReplayEnvelopePayload,
    ReplayEventRecord, SchemaBinding, SourceBoundaryRecord,
};
use deltaforge_core::{
    ArcDynSink, BatchResult, CheckpointMeta, Event, EventId, Op, SinkResult,
    SourceBoundary, SourceInfo,
};
use storage::{ArcStorageBackend, PostgresStorageBackend};

use runner::coordinator::build_batch_processor;
use runner::replay_controller::{
    CoordinatorReplayDelivery, IngestionControl, ReplayController,
};
use runner::replay_gate::ReplaySinkGate;
use runner::replay_job::{
    EncoderSchemaPolicy, ReplayJob, ReplayJobStore, ReplayPhase,
};
use runner::replay_journal::{BackendJournalLog, JournalLog};
use runner::replay_worker::ReplayDelivery;

/// Skip (returning None) unless a live PostgreSQL DSN is configured.
async fn pg_backend() -> Option<ArcStorageBackend> {
    let dsn = std::env::var("DELTAFORGE_IT_PG_DSN").ok()?;
    Some(PostgresStorageBackend::connect(&dsn).await.unwrap()
        as ArcStorageBackend)
}

fn identity(incarnation: &str) -> PipelineIdentity {
    PipelineIdentity {
        pipeline: "replay-e2e".into(),
        incarnation: incarnation.into(),
        source_lineage: None,
    }
}

/// A real, serializable `Event` carrying `id`.
fn row_event(id: i64) -> Event {
    let source = SourceInfo {
        version: "test".into(),
        connector: "mysql".into(),
        name: "test".into(),
        db: "db".into(),
        schema: None,
        table: "t".into(),
        ts_ms: 0,
        snapshot: None,
        position: Default::default(),
    };
    Event::new_row(
        EventId::mysql_row_server(1, "t", 1, id as u32),
        source,
        Op::Create,
        None,
        Some(serde_json::json!({ "id": id })),
        0,
        10,
    )
}

/// One commit unit (one boundary) carrying one row event.
fn envelope(incarnation: &str, id: i64, cp: &[u8]) -> ReplayEnvelopePayload {
    ReplayEnvelopePayload {
        version: REPLAY_ENVELOPE_VERSION,
        pipeline_identity: identity(incarnation),
        boundary: SourceBoundaryRecord::from_boundary(
            &SourceBoundary::checkpoint_only(CheckpointMeta::from_vec(
                cp.to_vec(),
            )),
        ),
        events: vec![ReplayEventRecord {
            offset: 0,
            event_id: format!("e-{id}"),
            tx_id: None,
            event: serde_json::to_value(row_event(id)).unwrap(),
        }],
        schema_binding: SchemaBinding {
            source_tables: vec!["db.t".into()],
            registry_seq_at_capture: None,
        },
    }
}

/// A sink that records the `id` of every delivered event.
#[derive(Default)]
struct RecordingSink {
    ids: std::sync::Mutex<Vec<i64>>,
}
#[async_trait]
impl deltaforge_core::Sink for RecordingSink {
    fn id(&self) -> &str {
        "kafka"
    }
    async fn send(&self, _e: &Event) -> SinkResult<()> {
        Ok(())
    }
    async fn send_batch(&self, events: &[Event]) -> SinkResult<BatchResult> {
        let mut ids = self.ids.lock().unwrap();
        for e in events {
            if let Some(id) = e.after.as_ref().and_then(|a| a.get("id")) {
                ids.push(id.as_i64().unwrap_or(-1));
            }
        }
        Ok(BatchResult::ok())
    }
}

/// Ingestion control that acknowledges immediately (no coordinator in these tests).
#[derive(Default)]
struct ImmediateIngestion {
    quiesced: AtomicUsize,
    resumed: AtomicUsize,
}
#[async_trait]
impl IngestionControl for ImmediateIngestion {
    async fn quiesce(&self) -> Result<()> {
        self.quiesced.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn resume(&self) {
        self.resumed.fetch_add(1, Ordering::SeqCst);
    }
}

/// Build a controller wired to a recording sink over the given backend/journal/store.
fn controller(
    backend: &ArcStorageBackend,
    incarnation: &str,
    journal: Arc<dyn JournalLog>,
    sink: Arc<RecordingSink>,
) -> ReplayController {
    let store = ReplayJobStore::new(backend.clone(), "replay-e2e", incarnation);
    let mut sinks: HashMap<String, ArcDynSink> = HashMap::new();
    sinks.insert("kafka".to_string(), sink as ArcDynSink);
    let delivery: Arc<dyn ReplayDelivery> =
        Arc::new(CoordinatorReplayDelivery::new(
            build_batch_processor(Arc::from(vec![]), "replay-e2e".to_string()),
            sinks,
            None,
            None,
            "replay-e2e",
        ));
    ReplayController::new(
        store,
        journal,
        Arc::new(ReplaySinkGate::new()),
        Arc::new(ImmediateIngestion::default()),
        delivery,
        Arc::new(AtomicU64::new(u64::MAX)),
        CancellationToken::new(),
        16,
        "replay-e2e",
    )
}

fn new_job(incarnation: &str) -> ReplayJob {
    ReplayJob::new(
        format!("job-{incarnation}"),
        "replay-e2e",
        incarnation,
        vec!["kafka".into()],
        vec![],
        0,
        None,
        EncoderSchemaPolicy::Current,
        false,
        1,
    )
    .unwrap()
}

async fn append_units(
    journal: &Arc<dyn JournalLog>,
    incarnation: &str,
    n: i64,
) -> Vec<u64> {
    let mut seqs = Vec::new();
    for id in 0..n {
        let out = journal
            .append(&envelope(incarnation, id, &id.to_le_bytes()))
            .await
            .unwrap();
        seqs.push(out.seq);
    }
    seqs
}

/// Deterministic capture/replay: every captured commit unit is replayed to the target sink,
/// in order, exactly matching what was journaled.
#[tokio::test]
#[ignore = "requires a live PostgreSQL (DELTAFORGE_IT_PG_DSN)"]
async fn pg_deterministic_capture_replay() {
    let Some(backend) = pg_backend().await else {
        return;
    };
    let incarnation = uuid::Uuid::now_v7().to_string();
    let journal: Arc<dyn JournalLog> = Arc::new(BackendJournalLog::new(
        backend.clone(),
        identity(&incarnation),
    ));
    append_units(&journal, &incarnation, 5).await;

    let store =
        ReplayJobStore::new(backend.clone(), "replay-e2e", &incarnation);
    store.create(&new_job(&incarnation)).await.unwrap();

    let sink = Arc::new(RecordingSink::default());
    let ctrl =
        controller(&backend, &incarnation, journal.clone(), sink.clone());
    ctrl.run().await.unwrap();

    assert_eq!(
        *sink.ids.lock().unwrap(),
        vec![0, 1, 2, 3, 4],
        "all units replayed in order"
    );
    let after = store.get().await.unwrap().unwrap();
    assert_eq!(after.job.phase, ReplayPhase::Completed);
}

/// At-least-once: replaying the same range again re-delivers every unit (idempotent capture
/// in the journal, at-least-once delivery to the sink).
#[tokio::test]
#[ignore = "requires a live PostgreSQL (DELTAFORGE_IT_PG_DSN)"]
async fn pg_at_least_once_redelivery() {
    let Some(backend) = pg_backend().await else {
        return;
    };
    let incarnation = uuid::Uuid::now_v7().to_string();
    let journal: Arc<dyn JournalLog> = Arc::new(BackendJournalLog::new(
        backend.clone(),
        identity(&incarnation),
    ));
    append_units(&journal, &incarnation, 3).await;
    // Re-appending identical units is idempotent (capture_id), so the stream still has 3.
    append_units(&journal, &incarnation, 3).await;
    assert_eq!(journal.stream_meta().await.unwrap().len, 3);

    let store =
        ReplayJobStore::new(backend.clone(), "replay-e2e", &incarnation);
    let sink = Arc::new(RecordingSink::default());

    // First replay.
    store.create(&new_job(&incarnation)).await.unwrap();
    controller(&backend, &incarnation, journal.clone(), sink.clone())
        .run()
        .await
        .unwrap();
    // Second replay of the same range (the completed job is replaced).
    store.create(&new_job(&incarnation)).await.unwrap();
    controller(&backend, &incarnation, journal.clone(), sink.clone())
        .run()
        .await
        .unwrap();

    assert_eq!(
        *sink.ids.lock().unwrap(),
        vec![0, 1, 2, 0, 1, 2],
        "each unit delivered at least once per run"
    );
}

/// Restart from a durable handoff phase: a job left at HandoffQuiesced(H) in PostgreSQL is
/// recovered - delivered through H, restored, and completed.
#[tokio::test]
#[ignore = "requires a live PostgreSQL (DELTAFORGE_IT_PG_DSN)"]
async fn pg_restart_from_handoff_quiesced() {
    let Some(backend) = pg_backend().await else {
        return;
    };
    let incarnation = uuid::Uuid::now_v7().to_string();
    let journal: Arc<dyn JournalLog> = Arc::new(BackendJournalLog::new(
        backend.clone(),
        identity(&incarnation),
    ));
    let seqs = append_units(&journal, &incarnation, 3).await;
    let h = *seqs.last().unwrap();

    let store =
        ReplayJobStore::new(backend.clone(), "replay-e2e", &incarnation);
    let created = store.create(&new_job(&incarnation)).await.unwrap();
    // Drive the durable job to HandoffQuiesced(H), simulating a crash mid-handoff.
    let s = store
        .compare_and_set(
            created.version,
            &created.job.advance(ReplayPhase::CatchingUp, 2).unwrap(),
        )
        .await
        .unwrap();
    store
        .compare_and_set(
            s.version,
            &s.job
                .advance(ReplayPhase::HandoffQuiesced { handoff_seq: h }, 3)
                .unwrap(),
        )
        .await
        .unwrap();

    let sink = Arc::new(RecordingSink::default());
    controller(&backend, &incarnation, journal.clone(), sink.clone())
        .run()
        .await
        .unwrap();

    // Recovery delivered the full range through H and completed.
    assert_eq!(*sink.ids.lock().unwrap(), vec![0, 1, 2]);
    let after = store.get().await.unwrap().unwrap();
    assert_eq!(after.job.phase, ReplayPhase::Completed);
}
