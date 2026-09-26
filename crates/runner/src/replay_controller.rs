//! Replay controller and coordinator-backed delivery (Event Replay, Slice 3 checkpoint c).
//!
//! The controller orchestrates one replay job end to end: it installs the per-sink pause
//! (the startup barrier), runs the [`ReplayWorker`] through the historical and catch-up
//! phases, then performs the handoff - quiesce ingestion, deliver through the frozen tail
//! `H`, restore the paused/staged sinks to the live set, and resume ingestion so the first
//! live delivery to a restored sink is strictly after `H`. Pause state is derived from the
//! active job through the [`ReplaySinkGate`], never persisted per sink, so a crash cannot
//! strand a pause: the manager reinstalls the gate from the durable job before the source
//! starts, and the controller resumes from the durable phase/cursor.
//!
//! [`CoordinatorReplayDelivery`] is the real delivery: it reconstructs each envelope's
//! events, runs the pipeline's CURRENT processors, and sends to the job's target sinks.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use deltaforge_core::replay::StoredReplayEnvelope;
use deltaforge_core::{ArcDynSink, CheckpointMeta, Event, SinkBatchContext};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::coordinator::ProcessBatchFn;
use crate::replay_gate::ReplaySinkGate;
use crate::replay_job::{ReplayJob, ReplayJobStore, ReplayPhase};
use crate::replay_journal::JournalLog;
use crate::replay_worker::{
    ReplayDelivery, ReplayWorker, ResolvedEncoderSchema,
};

/// Controls pipeline ingestion during the handoff: `quiesce` stops the coordinator from
/// capturing or delivering new commit units (so the journal tail is stable) and `resume`
/// lets it continue. Abstracted so the controller can be tested without a live coordinator.
#[async_trait]
pub trait IngestionControl: Send + Sync {
    async fn quiesce(&self);
    async fn resume(&self);
}

/// Real ingestion control over the coordinator's pause channel. Quiesce pauses the
/// coordinator and waits a short settle so any in-flight capture completes before the tail
/// is read; resume unpauses.
pub struct PauseIngestionControl {
    pause: watch::Sender<bool>,
    settle: Duration,
}

impl PauseIngestionControl {
    pub fn new(pause: watch::Sender<bool>, settle: Duration) -> Self {
        Self { pause, settle }
    }
}

#[async_trait]
impl IngestionControl for PauseIngestionControl {
    async fn quiesce(&self) {
        let _ = self.pause.send(true);
        tokio::time::sleep(self.settle).await;
    }
    async fn resume(&self) {
        let _ = self.pause.send(false);
    }
}

/// The set of sink ids a job targets: its selected (live) sinks plus its staged (backfill)
/// sinks.
fn job_targets(job: &ReplayJob) -> Vec<String> {
    job.selected_sinks
        .iter()
        .chain(job.staged_sinks.iter())
        .cloned()
        .collect()
}

/// Coordinator-backed [`ReplayDelivery`]: reconstructs an envelope's events, runs the
/// pipeline's current processors, and sends the result to the job's target sinks. Replay
/// does not commit source or per-sink checkpoints; it only delivers.
pub struct CoordinatorReplayDelivery {
    process: ProcessBatchFn<CheckpointMeta>,
    sinks_by_id: HashMap<String, ArcDynSink>,
    pipeline: String,
}

impl CoordinatorReplayDelivery {
    pub fn new(
        process: ProcessBatchFn<CheckpointMeta>,
        sinks_by_id: HashMap<String, ArcDynSink>,
        pipeline: impl Into<String>,
    ) -> Self {
        Self {
            process,
            sinks_by_id,
            pipeline: pipeline.into(),
        }
    }
}

#[async_trait]
impl ReplayDelivery for CoordinatorReplayDelivery {
    async fn deliver(
        &self,
        job: &ReplayJob,
        envelope: &StoredReplayEnvelope,
        schema: ResolvedEncoderSchema,
    ) -> Result<()> {
        // The current Sink API has no schema-selection parameter, so only the default
        // `Current` policy is honorable today. A pinned/at-capture schema fails closed
        // rather than silently encoding against the current schema.
        if !matches!(schema, ResolvedEncoderSchema::Current) {
            anyhow::bail!(
                "replay encoder schema pinning is not yet supported by the sink API; \
                 use the default 'current' encoder_schema_policy"
            );
        }

        // Reconstruct the stored (already decoded) events.
        let mut events: Vec<Event> =
            Vec::with_capacity(envelope.payload.events.len());
        for rec in &envelope.payload.events {
            let ev: Event = serde_json::from_value(rec.event.clone())
                .with_context(|| {
                    format!(
                        "replay could not reconstruct event {} at seq {}",
                        rec.event_id, envelope.seq
                    )
                })?;
            events.push(ev);
        }

        // Run the CURRENT processors (this is how replay repairs consumer/processor bugs).
        let processed = (self.process)(events)
            .await
            .with_context(|| {
                format!("replay processing failed at seq {}", envelope.seq)
            })?
            .events;
        if processed.is_empty() {
            // Everything was filtered by the current processors: nothing to deliver.
            return Ok(());
        }

        let ctx = build_ctx(envelope)?;
        for id in job_targets(job) {
            let sink = self.sinks_by_id.get(&id).ok_or_else(|| {
                anyhow::anyhow!(
                    "replay target sink '{id}' not found in pipeline"
                )
            })?;
            let result = sink
                .send_batch_with_context(&processed, &ctx)
                .await
                .map_err(|e| {
                anyhow::anyhow!(
                    "replay delivery to sink '{id}' failed at seq {}: {e}",
                    envelope.seq
                )
            })?;
            // Replay is fail-closed on per-event encoding failures: surface them as a job
            // error rather than silently dropping (there is no live DLQ path for replay).
            if !result.dlq_failures.is_empty() {
                anyhow::bail!(
                    "replay delivery to sink '{id}' produced {} per-event failure(s) at \
                     seq {}",
                    result.dlq_failures.len(),
                    envelope.seq
                );
            }
        }
        let _ = &self.pipeline;
        Ok(())
    }
}

/// Build the sink delivery context from a stored envelope's boundary (checkpoint and
/// durable watermark, decoded from hex). Replay does not commit checkpoints, so this is
/// informational for sinks that consult it.
fn build_ctx(envelope: &StoredReplayEnvelope) -> Result<SinkBatchContext> {
    let checkpoint = CheckpointMeta::from_vec(hex_decode(
        &envelope.payload.boundary.checkpoint_hex,
    )?);
    let durable_watermark = match &envelope.payload.boundary.watermark_hex {
        Some(h) => Some(hex_decode(h)?),
        None => None,
    };
    Ok(SinkBatchContext {
        checkpoint,
        durable_watermark,
        batch_id: None,
    })
}

fn hex_decode(s: &str) -> Result<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        anyhow::bail!("invalid hex length in replay boundary");
    }
    (0..s.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&s[i..i + 2], 16).map_err(|e| {
                anyhow::anyhow!("invalid hex in replay boundary: {e}")
            })
        })
        .collect()
}

/// Orchestrates one replay job: startup barrier, worker delivery, and the handoff.
pub struct ReplayController {
    store: ReplayJobStore,
    journal: Arc<dyn JournalLog>,
    gate: Arc<ReplaySinkGate>,
    ingestion: Arc<dyn IngestionControl>,
    worker: ReplayWorker,
    cancel: CancellationToken,
    /// Retention pin shared with the retention task: the lowest seq the active job needs
    /// retained (`u64::MAX` = nothing pinned). Set while the job runs, reset when it ends,
    /// so retention never truncates envelopes the job has not yet replayed.
    pin: Arc<AtomicU64>,
    pipeline: String,
}

impl ReplayController {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        store: ReplayJobStore,
        journal: Arc<dyn JournalLog>,
        gate: Arc<ReplaySinkGate>,
        ingestion: Arc<dyn IngestionControl>,
        delivery: Arc<dyn ReplayDelivery>,
        pin: Arc<AtomicU64>,
        cancel: CancellationToken,
        batch_limit: usize,
        pipeline: impl Into<String>,
    ) -> Self {
        let pipeline = pipeline.into();
        let worker = ReplayWorker::new(
            store.clone(),
            journal.clone(),
            delivery,
            cancel.clone(),
            batch_limit,
            pipeline.clone(),
        );
        Self {
            store,
            journal,
            gate,
            ingestion,
            worker,
            cancel,
            pin,
            pipeline,
        }
    }

    /// Install the per-sink pause for an active, non-dry-run job. Idempotent, and safe to
    /// call before the source starts (the startup barrier) and again when the controller
    /// runs.
    pub async fn install_barrier(&self) -> Result<()> {
        if let Some(stored) = self.store.get().await? {
            if stored.job.holds_pause() {
                self.gate.exclude(job_targets(&stored.job));
            }
        }
        Ok(())
    }

    /// Run the job to completion (or until cancelled). On any error the controller fails
    /// safe: it resumes ingestion and returns the paused/staged sinks to the live set so a
    /// failure never strands the pipeline paused.
    pub async fn run(&self) -> Result<()> {
        let outcome = self.run_inner().await;
        // Whatever happened, the job is no longer advancing: release the retention pin.
        // A still-active job (cancelled controller) is re-pinned by the startup barrier on
        // the next run.
        self.pin.store(u64::MAX, Ordering::SeqCst);
        match outcome {
            Ok(()) => Ok(()),
            Err(e) => {
                self.recover_from_failure(&e).await;
                Err(e)
            }
        }
    }

    async fn run_inner(&self) -> Result<()> {
        let stored = match self.store.get().await? {
            Some(s) if s.job.is_active() => s,
            _ => return Ok(()),
        };
        // Pin retention at the seq the job still needs, so retention cannot truncate
        // envelopes ahead of the cursor while the job runs.
        if let Some(pin) = stored.job.pin_seq() {
            self.pin.store(pin, Ordering::SeqCst);
        }
        // Startup barrier: ensure the gate reflects the job's exclusions.
        if stored.job.holds_pause() {
            self.gate.exclude(job_targets(&stored.job));
        }

        // Historical + catch-up delivery.
        let phase = match self.worker.run_catch_up().await? {
            Some(p) => p,
            None => return Ok(()),
        };
        if self.cancel.is_cancelled() {
            return Ok(());
        }
        match phase {
            // Dry-run finished (delivered nothing); no handoff, nothing to restore.
            ReplayPhase::Completed => Ok(()),
            // Caught up to the tail: perform the handoff.
            ReplayPhase::CatchingUp => self.handoff().await,
            // Failed/cancelled/other: nothing more to do here.
            _ => Ok(()),
        }
    }

    /// Quiesce ingestion, deliver through the frozen tail `H`, then restore the sinks to
    /// the live set and resume ingestion. Ordered so the first live delivery to a restored
    /// sink is strictly after `H` (new captures get seq > H once ingestion resumes).
    async fn handoff(&self) -> Result<()> {
        self.ingestion.quiesce().await;
        // With ingestion quiesced, no new commit units are captured, so the head is a
        // stable frozen tail `H`.
        let h = self.journal.stream_meta().await?.head_seq;
        self.set_phase(ReplayPhase::HandoffQuiesced { handoff_seq: h })
            .await?;

        // Deliver anything captured between catch-up and quiesce, through H.
        let mut stored = self.store.get().await?.ok_or_else(|| {
            anyhow::anyhow!("replay job vanished during handoff")
        })?;
        self.worker.deliver_through(&mut stored, h).await?;
        self.set_phase(ReplayPhase::DeliveredThrough { handoff_seq: h })
            .await?;

        // Restore: the sinks rejoin the live set, then ingestion resumes, so their first
        // live delivery is strictly after H.
        let targets = self
            .store
            .get()
            .await?
            .map(|s| job_targets(&s.job))
            .unwrap_or_default();
        self.gate.include(targets);
        self.set_phase(ReplayPhase::LiveRestored).await?;
        self.ingestion.resume().await;
        self.set_phase(ReplayPhase::Completed).await?;
        info!(pipeline = %self.pipeline, handoff_seq = h, "replay completed and live delivery restored");
        Ok(())
    }

    async fn set_phase(&self, to: ReplayPhase) -> Result<()> {
        let stored = self.store.get().await?.ok_or_else(|| {
            anyhow::anyhow!("replay job vanished before phase change")
        })?;
        let next = stored.job.advance(to, now_ms())?;
        self.store.compare_and_set(stored.version, &next).await?;
        Ok(())
    }

    /// Fail-safe cleanup: never leave ingestion paused or sinks out of the live set.
    async fn recover_from_failure(&self, err: &anyhow::Error) {
        warn!(pipeline = %self.pipeline, error = %format!("{err:#}"), "replay controller failed; restoring live delivery");
        self.ingestion.resume().await;
        if let Ok(Some(stored)) = self.store.get().await {
            self.gate.include(job_targets(&stored.job));
        }
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    use deltaforge_core::SourceBoundary;
    use deltaforge_core::replay::{
        PipelineIdentity, REPLAY_ENVELOPE_VERSION, ReplayEnvelopePayload,
        ReplayEventRecord, SchemaBinding, SourceBoundaryRecord,
    };
    use storage::{ArcStorageBackend, MemoryStorageBackend};

    use crate::replay_job::{EncoderSchemaPolicy, ReplayJob};
    use crate::replay_journal::BackendJournalLog;

    fn identity() -> PipelineIdentity {
        PipelineIdentity {
            pipeline: "p".into(),
            incarnation: "inc-1".into(),
            source_lineage: None,
        }
    }

    fn envelope(cp: &[u8]) -> ReplayEnvelopePayload {
        ReplayEnvelopePayload {
            version: REPLAY_ENVELOPE_VERSION,
            pipeline_identity: identity(),
            boundary: SourceBoundaryRecord::from_boundary(
                &SourceBoundary::checkpoint_only(CheckpointMeta::from_vec(
                    cp.to_vec(),
                )),
            ),
            events: vec![ReplayEventRecord {
                offset: 0,
                event_id: format!("e-{}", cp[0]),
                tx_id: None,
                event: serde_json::json!({ "cp": cp[0] }),
            }],
            schema_binding: SchemaBinding {
                source_tables: vec!["db.t".into()],
                registry_seq_at_capture: None,
            },
        }
    }

    #[derive(Default)]
    struct RecordingDelivery {
        delivered: Mutex<Vec<u64>>,
    }

    #[async_trait]
    impl ReplayDelivery for RecordingDelivery {
        async fn deliver(
            &self,
            _job: &ReplayJob,
            envelope: &StoredReplayEnvelope,
            _schema: ResolvedEncoderSchema,
        ) -> Result<()> {
            self.delivered.lock().unwrap().push(envelope.seq);
            Ok(())
        }
    }

    #[derive(Default)]
    struct MockIngestion {
        quiesced: AtomicUsize,
        resumed: AtomicUsize,
    }

    #[async_trait]
    impl IngestionControl for MockIngestion {
        async fn quiesce(&self) {
            self.quiesced.fetch_add(1, Ordering::SeqCst);
        }
        async fn resume(&self) {
            self.resumed.fetch_add(1, Ordering::SeqCst);
        }
    }

    async fn journal_with(
        n: usize,
    ) -> (ArcStorageBackend, Arc<dyn JournalLog>, Vec<u64>) {
        let be: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
        let jl: Arc<dyn JournalLog> =
            Arc::new(BackendJournalLog::new(be.clone(), identity()));
        let mut seqs = Vec::new();
        for i in 0..n {
            seqs.push(jl.append(&envelope(&[i as u8])).await.unwrap().seq);
        }
        (be, jl, seqs)
    }

    fn job(dry_run: bool) -> ReplayJob {
        ReplayJob::new(
            "job-1",
            "p",
            "inc-1",
            vec!["kafka".into()],
            vec!["s3-new".into()],
            0,
            None,
            EncoderSchemaPolicy::Current,
            dry_run,
            1_000,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn full_run_delivers_hands_off_and_restores_live() {
        let (be, jl, seqs) = journal_with(3).await;
        let store = ReplayJobStore::new(be.clone(), "p", "inc-1");
        store.create(&job(false)).await.unwrap();
        let gate = Arc::new(ReplaySinkGate::new());
        let ingestion = Arc::new(MockIngestion::default());
        let delivery = Arc::new(RecordingDelivery::default());
        let controller = ReplayController::new(
            store.clone(),
            jl,
            gate.clone(),
            ingestion.clone(),
            delivery.clone(),
            Arc::new(AtomicU64::new(u64::MAX)),
            CancellationToken::new(),
            10,
            "p",
        );

        controller.run().await.unwrap();

        // Every envelope delivered to the targets.
        assert_eq!(*delivery.delivered.lock().unwrap(), seqs);
        // The job completed and live delivery was restored (gate cleared).
        let after = store.get().await.unwrap().unwrap();
        assert_eq!(after.job.phase, ReplayPhase::Completed);
        assert!(!gate.any_excluded(), "sinks returned to the live set");
        // Ingestion was quiesced once and resumed at least once.
        assert_eq!(ingestion.quiesced.load(Ordering::SeqCst), 1);
        assert!(ingestion.resumed.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn barrier_excludes_targets_while_active() {
        let (be, jl, _seqs) = journal_with(1).await;
        let store = ReplayJobStore::new(be.clone(), "p", "inc-1");
        store.create(&job(false)).await.unwrap();
        let gate = Arc::new(ReplaySinkGate::new());
        let controller = ReplayController::new(
            store.clone(),
            jl,
            gate.clone(),
            Arc::new(MockIngestion::default()),
            Arc::new(RecordingDelivery::default()),
            Arc::new(AtomicU64::new(u64::MAX)),
            CancellationToken::new(),
            10,
            "p",
        );
        controller.install_barrier().await.unwrap();
        assert_eq!(gate.excluded_ids(), vec!["kafka", "s3-new"]);
    }

    #[tokio::test]
    async fn dry_run_does_not_pause_or_hand_off() {
        let (be, jl, _seqs) = journal_with(2).await;
        let store = ReplayJobStore::new(be.clone(), "p", "inc-1");
        // Dry-run cannot stage sinks; use a selected-only dry-run job.
        let dry = ReplayJob::new(
            "job-dry",
            "p",
            "inc-1",
            vec!["kafka".into()],
            vec![],
            0,
            None,
            EncoderSchemaPolicy::Current,
            true,
            1_000,
        )
        .unwrap();
        store.create(&dry).await.unwrap();
        let gate = Arc::new(ReplaySinkGate::new());
        let ingestion = Arc::new(MockIngestion::default());
        let delivery = Arc::new(RecordingDelivery::default());
        let controller = ReplayController::new(
            store.clone(),
            jl,
            gate.clone(),
            ingestion.clone(),
            delivery.clone(),
            Arc::new(AtomicU64::new(u64::MAX)),
            CancellationToken::new(),
            10,
            "p",
        );
        controller.run().await.unwrap();
        assert!(delivery.delivered.lock().unwrap().is_empty());
        assert!(!gate.any_excluded(), "dry-run never pauses");
        assert_eq!(ingestion.quiesced.load(Ordering::SeqCst), 0, "no handoff");
        let after = store.get().await.unwrap().unwrap();
        assert_eq!(after.job.phase, ReplayPhase::Completed);
    }

    #[tokio::test]
    async fn hex_roundtrips_through_ctx() {
        let (_be, jl, _seqs) = journal_with(0).await;
        let _ = jl;
        let payload = envelope(&[0xde, 0xad, 0xbe, 0xef]);
        let stored = StoredReplayEnvelope {
            seq: 1,
            stored_at_ms: 0,
            capture_id: payload.capture_id(),
            content_hash: "x".into(),
            payload,
        };
        let ctx = build_ctx(&stored).unwrap();
        assert_eq!(ctx.checkpoint.as_bytes(), &[0xde, 0xad, 0xbe, 0xef]);
    }
}
