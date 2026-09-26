//! Replay worker (Event Replay, Slice 3 checkpoint b).
//!
//! The worker reads committed envelopes from a pipeline's replay journal, starting at the
//! job's durable cursor, and delivers each commit unit to the job's target sinks through a
//! [`ReplayDelivery`]. It advances the durable cursor after every delivered envelope, so a
//! crash resumes from the last durably-recorded position (at-least-once), and it is
//! cancellable at any point. Delivery is fail-closed: a delivery error (or an unresolvable
//! encoder schema) fails the job and stops.
//!
//! This checkpoint covers the historical (`Running`) and catch-up (`CatchingUp`) delivery
//! and the encoder-schema-policy resolution. The quiesce/handoff/live-restore steps, which
//! need the coordinator to briefly stop ingestion and swap sink ownership under one lock,
//! land in checkpoint c; the reusable primitive they build on, [`ReplayWorker::deliver_through`],
//! is defined here.

use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use deltaforge_core::replay::{SchemaBinding, StoredReplayEnvelope};
use metrics::counter;
use tokio_util::sync::CancellationToken;

use crate::replay_job::{
    EncoderSchemaPolicy, ReplayJob, ReplayJobError, ReplayJobStore,
    ReplayPhase, StoredReplayJob,
};
use crate::replay_journal::JournalLog;

/// The encoder schema a delivery should resolve against for one envelope, after applying
/// the job's [`EncoderSchemaPolicy`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolvedEncoderSchema {
    /// Resolve against the registry's current schema.
    Current,
    /// Resolve against this exact registry sequence.
    AtSeq(u64),
}

/// Apply an [`EncoderSchemaPolicy`] to one envelope's [`SchemaBinding`]. `AtCaptureSeq`
/// fails closed when the envelope has no captured registry sequence (the registry handle is
/// not yet threaded into capture, so this is currently always the case for that policy).
pub fn resolve_encoder_schema(
    policy: &EncoderSchemaPolicy,
    binding: &SchemaBinding,
    seq: u64,
) -> Result<ResolvedEncoderSchema, ReplayJobError> {
    match policy {
        EncoderSchemaPolicy::Current => Ok(ResolvedEncoderSchema::Current),
        EncoderSchemaPolicy::Pinned { seq } => {
            Ok(ResolvedEncoderSchema::AtSeq(*seq))
        }
        EncoderSchemaPolicy::AtCaptureSeq => {
            match binding.registry_seq_at_capture {
                Some(s) => Ok(ResolvedEncoderSchema::AtSeq(s)),
                None => Err(ReplayJobError::EncoderSchemaUnavailable { seq }),
            }
        }
    }
}

/// Delivers one commit unit's events to a job's target sinks, running the current
/// processing/encoding path against the resolved encoder schema. The coordinator-backed
/// implementation lands in checkpoint c; the worker is unit-tested with a recording double.
#[async_trait]
pub trait ReplayDelivery: Send + Sync {
    /// Deliver one envelope's events to `job`'s selected (and staged) sinks. `Ok(())` means
    /// the unit was durably accepted by the targets; an `Err` fails the job (fail-closed).
    async fn deliver(
        &self,
        job: &ReplayJob,
        envelope: &StoredReplayEnvelope,
        schema: ResolvedEncoderSchema,
    ) -> Result<()>;
}

/// Drives one replay job's historical and catch-up delivery over a [`JournalLog`], keeping
/// the durable cursor and phase in a [`ReplayJobStore`].
pub struct ReplayWorker {
    store: ReplayJobStore,
    journal: Arc<dyn JournalLog>,
    delivery: Arc<dyn ReplayDelivery>,
    cancel: CancellationToken,
    batch_limit: usize,
    pipeline: String,
}

impl ReplayWorker {
    pub fn new(
        store: ReplayJobStore,
        journal: Arc<dyn JournalLog>,
        delivery: Arc<dyn ReplayDelivery>,
        cancel: CancellationToken,
        batch_limit: usize,
        pipeline: impl Into<String>,
    ) -> Self {
        Self {
            store,
            journal,
            delivery,
            cancel,
            batch_limit: batch_limit.max(1),
            pipeline: pipeline.into(),
        }
    }

    /// Run the job forward through `Running` (deliver the requested historical range) and
    /// `CatchingUp` (chase the tail until stable). Returns the phase reached, or `None` when
    /// no job exists. A dry-run job delivers nothing and completes directly. Cancellation
    /// returns the current phase, leaving the job resumable; a delivery failure fails the
    /// job and returns the error.
    pub async fn run_catch_up(&self) -> Result<Option<ReplayPhase>> {
        let mut stored = match self.store.get().await? {
            Some(s) => s,
            None => return Ok(None),
        };
        if !stored.job.is_active() {
            return Ok(Some(stored.job.phase));
        }

        // Running: deliver the requested historical range [from_seq, end].
        if matches!(stored.job.phase, ReplayPhase::Running) {
            let end = self.range_end(&stored.job).await?;
            self.deliver_through(&mut stored, end).await?;
            if self.cancel.is_cancelled() {
                return Ok(Some(stored.job.phase));
            }
            if stored.job.dry_run {
                // Dry-run reports count/range/targets and completes without a handoff.
                self.set_phase(&mut stored, ReplayPhase::Completed).await?;
                return Ok(Some(stored.job.phase));
            }
            self.set_phase(&mut stored, ReplayPhase::CatchingUp).await?;
        }

        // CatchingUp: keep delivering toward the (possibly growing) tail until the cursor
        // catches up. The quiesce/handoff that finalizes from here is checkpoint c.
        if matches!(stored.job.phase, ReplayPhase::CatchingUp) {
            loop {
                if self.cancel.is_cancelled() {
                    break;
                }
                let end = self.range_end(&stored.job).await?;
                if stored.job.cursor >= end {
                    break;
                }
                self.deliver_through(&mut stored, end).await?;
            }
        }

        Ok(Some(stored.job.phase))
    }

    /// The upper seq bound to deliver toward: the requested `through_seq` if set, capped at
    /// the current stream head (so we never wait past available data), else the head.
    async fn range_end(&self, job: &ReplayJob) -> Result<u64> {
        let head = self.journal.stream_meta().await?.head_seq;
        Ok(job.through_seq.map(|t| t.min(head)).unwrap_or(head))
    }

    /// Deliver every envelope in `(cursor, up_to_seq]` to the job's targets, advancing the
    /// durable cursor after each. Resumable (starts from the durable cursor) and
    /// cancellable (checked between envelopes). Fail-closed: on a delivery or schema error
    /// the job is transitioned to `Failed` durably and the error is returned.
    pub async fn deliver_through(
        &self,
        stored: &mut StoredReplayJob,
        up_to_seq: u64,
    ) -> Result<()> {
        while stored.job.cursor < up_to_seq {
            if self.cancel.is_cancelled() {
                return Ok(());
            }
            let batch = self
                .journal
                .read_since(stored.job.cursor, self.batch_limit)
                .await?;
            if batch.is_empty() {
                break;
            }
            for env in batch {
                if env.seq > up_to_seq {
                    return Ok(());
                }
                if self.cancel.is_cancelled() {
                    return Ok(());
                }
                if let Err(e) = self.process_envelope(stored, &env).await {
                    self.fail_job(stored, format!("{e:#}")).await;
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    /// Resolve the schema, deliver one envelope (unless dry-run), and advance the durable
    /// cursor to its seq.
    async fn process_envelope(
        &self,
        stored: &mut StoredReplayJob,
        env: &StoredReplayEnvelope,
    ) -> Result<()> {
        if !stored.job.dry_run {
            let schema = resolve_encoder_schema(
                &stored.job.encoder_schema_policy,
                &env.payload.schema_binding,
                env.seq,
            )?;
            self.delivery
                .deliver(&stored.job, env, schema)
                .await
                .with_context(|| {
                    format!("replay delivery failed at seq {}", env.seq)
                })?;
        }
        let advanced = stored.job.with_cursor(env.seq, now_ms())?;
        *stored = self
            .store
            .compare_and_set(stored.version, &advanced)
            .await?;
        counter!(
            "deltaforge_replay_delivered_total",
            "pipeline" => self.pipeline.clone(),
        )
        .increment(1);
        Ok(())
    }

    /// Transition the durable job to `to`.
    async fn set_phase(
        &self,
        stored: &mut StoredReplayJob,
        to: ReplayPhase,
    ) -> Result<()> {
        let next = stored.job.advance(to, now_ms())?;
        *stored = self.store.compare_and_set(stored.version, &next).await?;
        Ok(())
    }

    /// Best-effort fail-closed transition to `Failed`. Never masks the original error: if
    /// the job cannot be marked failed (e.g. a concurrent writer changed it), the caller
    /// still returns the underlying error.
    async fn fail_job(&self, stored: &mut StoredReplayJob, reason: String) {
        counter!(
            "deltaforge_replay_job_failed_total",
            "pipeline" => self.pipeline.clone(),
        )
        .increment(1);
        if let Ok(failed) = stored.job.fail(reason, now_ms()) {
            if let Ok(s) =
                self.store.compare_and_set(stored.version, &failed).await
            {
                *stored = s;
            }
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

    use deltaforge_core::CheckpointMeta;
    use deltaforge_core::SourceBoundary;
    use deltaforge_core::replay::{
        PipelineIdentity, REPLAY_ENVELOPE_VERSION, ReplayEnvelopePayload,
        ReplayEventRecord, SourceBoundaryRecord,
    };
    use storage::{ArcStorageBackend, MemoryStorageBackend};

    use crate::replay_job::ReplayJob;
    use crate::replay_journal::BackendJournalLog;

    fn identity() -> PipelineIdentity {
        PipelineIdentity {
            pipeline: "p".into(),
            incarnation: "inc-1".into(),
            source_lineage: None,
        }
    }

    fn envelope(cp: &[u8], reg_seq: Option<u64>) -> ReplayEnvelopePayload {
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
                event_id: format!("e-{}", hex(cp)),
                tx_id: None,
                event: serde_json::json!({ "cp": hex(cp) }),
            }],
            schema_binding: SchemaBinding {
                source_tables: vec!["db.t".into()],
                registry_seq_at_capture: reg_seq,
            },
        }
    }

    fn hex(b: &[u8]) -> String {
        b.iter().map(|x| format!("{x:02x}")).collect()
    }

    /// Append `n` envelopes to a fresh in-memory journal; return the backend and journal.
    async fn journal_with(
        n: usize,
        reg_seq: Option<u64>,
    ) -> (ArcStorageBackend, Arc<dyn JournalLog>, Vec<u64>) {
        let be: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
        let jl: Arc<dyn JournalLog> =
            Arc::new(BackendJournalLog::new(be.clone(), identity()));
        let mut seqs = Vec::new();
        for i in 0..n {
            let out = jl.append(&envelope(&[i as u8], reg_seq)).await.unwrap();
            seqs.push(out.seq);
        }
        (be, jl, seqs)
    }

    #[derive(Default)]
    struct RecordingDelivery {
        delivered: Mutex<Vec<(u64, ResolvedEncoderSchema)>>,
        fail_at: Option<u64>,
    }

    #[async_trait]
    impl ReplayDelivery for RecordingDelivery {
        async fn deliver(
            &self,
            _job: &ReplayJob,
            envelope: &StoredReplayEnvelope,
            schema: ResolvedEncoderSchema,
        ) -> Result<()> {
            if Some(envelope.seq) == self.fail_at {
                anyhow::bail!("boom at {}", envelope.seq);
            }
            self.delivered.lock().unwrap().push((envelope.seq, schema));
            Ok(())
        }
    }

    fn store_for(be: &ArcStorageBackend) -> ReplayJobStore {
        ReplayJobStore::new(be.clone(), "p", "inc-1")
    }

    fn job(
        from_seq: u64,
        dry_run: bool,
        policy: EncoderSchemaPolicy,
    ) -> ReplayJob {
        ReplayJob::new(
            "job-1",
            "p",
            "inc-1",
            vec!["kafka".into()],
            vec![],
            from_seq,
            None,
            policy,
            dry_run,
            1_000,
        )
        .unwrap()
    }

    #[test]
    fn encoder_policy_resolution() {
        let with_seq = SchemaBinding {
            source_tables: vec![],
            registry_seq_at_capture: Some(7),
        };
        let without = SchemaBinding {
            source_tables: vec![],
            registry_seq_at_capture: None,
        };
        assert_eq!(
            resolve_encoder_schema(&EncoderSchemaPolicy::Current, &without, 1)
                .unwrap(),
            ResolvedEncoderSchema::Current
        );
        assert_eq!(
            resolve_encoder_schema(
                &EncoderSchemaPolicy::Pinned { seq: 3 },
                &without,
                1
            )
            .unwrap(),
            ResolvedEncoderSchema::AtSeq(3)
        );
        assert_eq!(
            resolve_encoder_schema(
                &EncoderSchemaPolicy::AtCaptureSeq,
                &with_seq,
                1
            )
            .unwrap(),
            ResolvedEncoderSchema::AtSeq(7)
        );
        assert!(matches!(
            resolve_encoder_schema(
                &EncoderSchemaPolicy::AtCaptureSeq,
                &without,
                5
            ),
            Err(ReplayJobError::EncoderSchemaUnavailable { seq: 5 })
        ));
    }

    #[tokio::test]
    async fn delivers_range_and_advances_cursor_to_catching_up() {
        let (be, jl, seqs) = journal_with(3, None).await;
        let store = store_for(&be);
        store
            .create(&job(0, false, EncoderSchemaPolicy::Current))
            .await
            .unwrap();
        let delivery = Arc::new(RecordingDelivery::default());
        let worker = ReplayWorker::new(
            store_for(&be),
            jl,
            delivery.clone(),
            CancellationToken::new(),
            10,
            "p",
        );
        let phase = worker.run_catch_up().await.unwrap();
        assert_eq!(phase, Some(ReplayPhase::CatchingUp));
        let got: Vec<u64> = delivery
            .delivered
            .lock()
            .unwrap()
            .iter()
            .map(|d| d.0)
            .collect();
        assert_eq!(got, seqs, "every envelope delivered in order");
        let after = store_for(&be).get().await.unwrap().unwrap();
        assert_eq!(after.job.cursor, *seqs.last().unwrap());
    }

    #[tokio::test]
    async fn resumes_from_durable_cursor() {
        let (be, jl, seqs) = journal_with(3, None).await;
        let store = store_for(&be);
        let created = store
            .create(&job(0, false, EncoderSchemaPolicy::Current))
            .await
            .unwrap();
        // Pretend the first envelope was already delivered before a restart.
        let advanced = created.job.with_cursor(seqs[0], 2_000).unwrap();
        store
            .compare_and_set(created.version, &advanced)
            .await
            .unwrap();

        let delivery = Arc::new(RecordingDelivery::default());
        let worker = ReplayWorker::new(
            store_for(&be),
            jl,
            delivery.clone(),
            CancellationToken::new(),
            10,
            "p",
        );
        worker.run_catch_up().await.unwrap();
        let got: Vec<u64> = delivery
            .delivered
            .lock()
            .unwrap()
            .iter()
            .map(|d| d.0)
            .collect();
        assert_eq!(got, seqs[1..], "only the un-delivered tail is replayed");
    }

    #[tokio::test]
    async fn cancellation_stops_delivery_and_leaves_job_resumable() {
        let (be, jl, _seqs) = journal_with(3, None).await;
        let store = store_for(&be);
        store
            .create(&job(0, false, EncoderSchemaPolicy::Current))
            .await
            .unwrap();
        let cancel = CancellationToken::new();
        cancel.cancel(); // cancelled before the worker runs
        let delivery = Arc::new(RecordingDelivery::default());
        let worker = ReplayWorker::new(
            store_for(&be),
            jl,
            delivery.clone(),
            cancel,
            10,
            "p",
        );
        let phase = worker.run_catch_up().await.unwrap();
        assert_eq!(phase, Some(ReplayPhase::Running), "phase not advanced");
        assert!(delivery.delivered.lock().unwrap().is_empty());
        let after = store_for(&be).get().await.unwrap().unwrap();
        assert!(after.job.is_active(), "job still resumable");
        assert_eq!(after.job.cursor, 0);
    }

    #[tokio::test]
    async fn dry_run_delivers_nothing_and_completes() {
        let (be, jl, seqs) = journal_with(2, None).await;
        let store = store_for(&be);
        store
            .create(&job(0, true, EncoderSchemaPolicy::Current))
            .await
            .unwrap();
        let delivery = Arc::new(RecordingDelivery::default());
        let worker = ReplayWorker::new(
            store_for(&be),
            jl,
            delivery.clone(),
            CancellationToken::new(),
            10,
            "p",
        );
        let phase = worker.run_catch_up().await.unwrap();
        assert_eq!(phase, Some(ReplayPhase::Completed));
        assert!(delivery.delivered.lock().unwrap().is_empty());
        // The cursor still advanced through the range (so a dry-run can report a count).
        let after = store_for(&be).get().await.unwrap().unwrap();
        assert_eq!(after.job.cursor, *seqs.last().unwrap());
    }

    #[tokio::test]
    async fn delivery_failure_fails_the_job_closed() {
        let (be, jl, seqs) = journal_with(3, None).await;
        let store = store_for(&be);
        store
            .create(&job(0, false, EncoderSchemaPolicy::Current))
            .await
            .unwrap();
        let delivery = Arc::new(RecordingDelivery {
            delivered: Mutex::new(Vec::new()),
            fail_at: Some(seqs[1]),
        });
        let worker = ReplayWorker::new(
            store_for(&be),
            jl,
            delivery.clone(),
            CancellationToken::new(),
            10,
            "p",
        );
        let err = worker.run_catch_up().await.unwrap_err();
        assert!(err.to_string().contains("replay delivery failed"));
        let after = store_for(&be).get().await.unwrap().unwrap();
        assert_eq!(after.job.phase, ReplayPhase::Failed);
        assert!(after.job.error.is_some());
        // The first envelope committed durably before the failure; the cursor stopped
        // short of the failing one (fail-closed, no advance past it).
        assert_eq!(after.job.cursor, seqs[0]);
    }

    #[tokio::test]
    async fn at_capture_seq_without_registry_seq_fails_closed() {
        let (be, jl, _seqs) = journal_with(1, None).await; // envelope has no reg seq
        let store = store_for(&be);
        store
            .create(&job(0, false, EncoderSchemaPolicy::AtCaptureSeq))
            .await
            .unwrap();
        let delivery = Arc::new(RecordingDelivery::default());
        let worker = ReplayWorker::new(
            store_for(&be),
            jl,
            delivery,
            CancellationToken::new(),
            10,
            "p",
        );
        let err = worker.run_catch_up().await.unwrap_err();
        assert!(err.to_string().contains("at_capture_seq"));
        let after = store_for(&be).get().await.unwrap().unwrap();
        assert_eq!(after.job.phase, ReplayPhase::Failed);
    }
}
