//! Replay journal: durable, idempotent capture of committed source commit units, plus
//! age/capacity retention, over the `StorageBackend` log primitives.
//!
//! Symmetric with the DLQ writer (`dlq.rs`) but log-backed (append-only, retained)
//! rather than queue-backed. The coordinator captures one envelope per commit unit at
//! its boundary (fail-closed); a background task enforces retention, never past the
//! pin of an active replay job.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use deltaforge_core::replay::{
    PipelineIdentity, ReplayEnvelopePayload, StoredReplayEnvelope,
};
use metrics::counter;
use storage::{
    ArcStorageBackend, LogAppendOutcome, LogStreamMeta, LogTruncateOutcome,
    LogTruncateRequest,
};
use tracing::{error, warn};

/// Journal namespace shared with the DLQ (distinct keys per stream/pipeline).
pub const REPLAY_NS: &str = "journal";

/// The replay stream key is incarnation-scoped so a recreated pipeline never inherits
/// an old stream.
pub fn replay_stream_key(pipeline: &str, incarnation: &str) -> String {
    format!("{pipeline}:{incarnation}:replay")
}

/// Append-only replay log. The coordinator appends; the replay engine (later slice)
/// reads back; the retention task truncates.
#[async_trait]
pub trait JournalLog: Send + Sync {
    /// Idempotently append one commit-unit envelope. Identical retries return the
    /// existing sequence; a same-identity/different-bytes append is a hard error.
    async fn append(
        &self,
        payload: &ReplayEnvelopePayload,
    ) -> Result<LogAppendOutcome>;

    /// Read up to `limit` envelopes with `seq > from_seq`, each decoded and verified
    /// fail-closed (version, pipeline identity, capture id, and content digest).
    async fn read_since(
        &self,
        from_seq: u64,
        limit: usize,
    ) -> Result<Vec<StoredReplayEnvelope>>;

    /// The stream's durable metadata (horizon, head, oldest, len).
    async fn stream_meta(&self) -> Result<LogStreamMeta>;

    /// Remove old envelopes and advance the horizon, never past `req.pin_seq`.
    async fn truncate(
        &self,
        req: LogTruncateRequest,
    ) -> Result<LogTruncateOutcome>;
}

/// `JournalLog` over any `StorageBackend`. Holds the reading pipeline's identity so
/// reads can be verified fail-closed against it.
pub struct BackendJournalLog {
    backend: ArcStorageBackend,
    identity: PipelineIdentity,
    key: String,
}

impl BackendJournalLog {
    pub fn new(backend: ArcStorageBackend, identity: PipelineIdentity) -> Self {
        let key = replay_stream_key(&identity.pipeline, &identity.incarnation);
        Self {
            backend,
            identity,
            key,
        }
    }
}

#[async_trait]
impl JournalLog for BackendJournalLog {
    async fn append(
        &self,
        payload: &ReplayEnvelopePayload,
    ) -> Result<LogAppendOutcome> {
        let capture_id = payload.capture_id();
        let value = payload.canonical_bytes();
        self.backend
            .log_append_if_absent(REPLAY_NS, &self.key, &capture_id, &value)
            .await
    }

    async fn read_since(
        &self,
        from_seq: u64,
        limit: usize,
    ) -> Result<Vec<StoredReplayEnvelope>> {
        let metas = self
            .backend
            .log_read_meta_since(REPLAY_NS, &self.key, from_seq, limit)
            .await?;
        metas
            .into_iter()
            .map(|m| {
                let capture_id = m.capture_id.ok_or_else(|| {
                    anyhow::anyhow!(
                        "replay entry seq {} has no capture_id (not a replay envelope)",
                        m.seq
                    )
                })?;
                let content_hash = m.content_hash.ok_or_else(|| {
                    anyhow::anyhow!(
                        "replay entry seq {} has no content_hash",
                        m.seq
                    )
                })?;
                StoredReplayEnvelope::decode_verified(
                    m.seq,
                    m.stored_at_ms,
                    &capture_id,
                    &content_hash,
                    &m.value,
                    &self.identity,
                )
                .map_err(anyhow::Error::from)
            })
            .collect()
    }

    async fn stream_meta(&self) -> Result<LogStreamMeta> {
        self.backend.log_stream_meta(REPLAY_NS, &self.key).await
    }

    async fn truncate(
        &self,
        req: LogTruncateRequest,
    ) -> Result<LogTruncateOutcome> {
        self.backend.log_truncate(REPLAY_NS, &self.key, req).await
    }
}

/// A source of the minimum sequence still pinned by any active replay job. Until the
/// replay engine (later slice) exists, the default pins nothing (`u64::MAX`).
pub type PinSeqFn = Arc<dyn Fn() -> u64 + Send + Sync>;

/// Retention configuration for the background task.
#[derive(Debug, Clone, Copy)]
pub struct RetentionConfig {
    /// Drop envelopes older than this (ms). `None` = no age limit.
    pub max_age_ms: Option<i64>,
    /// Optional capacity caps. `None` = unbounded.
    pub max_entries: Option<u64>,
    pub max_bytes: Option<u64>,
    /// How often the task runs.
    pub interval_secs: u64,
}

/// Spawn the background retention task. It periodically truncates the stream by
/// age/capacity, never past the current active-job pin (from `pin_seq_fn`). A
/// `capacity_pinned` outcome (a cap blocked by an active job) is surfaced as a metric
/// and a warning, never by removing pinned data.
pub fn spawn_retention_task(
    journal: Arc<dyn JournalLog>,
    pipeline: String,
    cfg: RetentionConfig,
    pin_seq_fn: PinSeqFn,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(
            cfg.interval_secs.max(1),
        ));
        loop {
            ticker.tick().await;
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;
            let req = LogTruncateRequest {
                older_than_ms: cfg.max_age_ms.map(|age| now_ms - age),
                pin_seq: pin_seq_fn(),
                max_entries: cfg.max_entries,
                max_bytes: cfg.max_bytes,
            };
            match journal.truncate(req).await {
                Ok(out) => {
                    if out.removed > 0 {
                        counter!(
                            "deltaforge_replay_retention_removed_total",
                            "pipeline" => pipeline.clone(),
                        )
                        .increment(out.removed as u64);
                    }
                    if out.capacity_pinned {
                        counter!(
                            "deltaforge_replay_retention_capacity_pinned_total",
                            "pipeline" => pipeline.clone(),
                        )
                        .increment(1);
                        warn!(
                            pipeline = %pipeline,
                            "replay retention cap not honored: an active job pins old \
                             envelopes; the log may exceed its cap until the job finishes"
                        );
                    }
                }
                Err(e) => {
                    error!(pipeline = %pipeline, error = %e, "replay retention failed");
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltaforge_core::CheckpointMeta;
    use deltaforge_core::SourceBoundary;
    use deltaforge_core::replay::{
        PipelineIdentity, REPLAY_ENVELOPE_VERSION, ReplayEnvelopePayload,
        ReplayEventRecord, SchemaBinding, SourceBoundaryRecord,
    };
    use storage::{AppendStatus, MemoryStorageBackend};

    fn identity() -> PipelineIdentity {
        PipelineIdentity {
            pipeline: "p".into(),
            incarnation: "inc-1".into(),
            source_lineage: None,
        }
    }

    fn envelope(cp: &[u8], ids: &[&str]) -> ReplayEnvelopePayload {
        ReplayEnvelopePayload {
            version: REPLAY_ENVELOPE_VERSION,
            pipeline_identity: identity(),
            boundary: SourceBoundaryRecord::from_boundary(
                &SourceBoundary::checkpoint_only(CheckpointMeta::from_vec(
                    cp.to_vec(),
                )),
            ),
            events: ids
                .iter()
                .enumerate()
                .map(|(i, id)| ReplayEventRecord {
                    offset: i as u32,
                    event_id: (*id).into(),
                    tx_id: None,
                    event: serde_json::json!({ "id": id }),
                })
                .collect(),
            schema_binding: SchemaBinding {
                source_tables: vec!["db.t".into()],
                registry_seq_at_capture: None,
            },
        }
    }

    #[tokio::test]
    async fn append_is_idempotent_and_reads_back_verified() {
        let be: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
        let jl = BackendJournalLog::new(be, identity());
        let e1 = envelope(b"cp1", &["a", "b"]);
        let e2 = envelope(b"cp2", &["c"]);

        let a = jl.append(&e1).await.unwrap();
        assert_eq!(a.status, AppendStatus::Inserted);
        // Idempotent retry.
        let a2 = jl.append(&e1).await.unwrap();
        assert_eq!(a2.status, AppendStatus::AlreadyPresent);
        assert_eq!(a.seq, a2.seq);
        jl.append(&e2).await.unwrap();

        let all = jl.read_since(0, 100).await.unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].payload, e1);
        assert_eq!(all[1].payload, e2);
        // Verified metadata is exposed.
        assert_eq!(all[0].capture_id, e1.capture_id());
        assert!(all[0].stored_at_ms > 0);
        assert_eq!(jl.stream_meta().await.unwrap().len, 2);
    }

    /// Reading under a different identity fails closed (cannot read another
    /// incarnation's stream as your own).
    #[tokio::test]
    async fn read_rejects_foreign_identity() {
        let be: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
        let writer = BackendJournalLog::new(be.clone(), identity());
        writer.append(&envelope(b"cp1", &["a"])).await.unwrap();

        let other = PipelineIdentity {
            pipeline: "p".into(),
            incarnation: "inc-2".into(),
            source_lineage: None,
        };
        // Different incarnation => different stream key => reads nothing; but a reader
        // pointed at the same key with a foreign identity would fail-closed on decode.
        let foreign_same_key = BackendJournalLog {
            backend: be,
            identity: other,
            key: replay_stream_key("p", "inc-1"),
        };
        let err = foreign_same_key.read_since(0, 100).await.unwrap_err();
        assert!(
            err.to_string().contains("identity"),
            "expected identity mismatch, got: {err}"
        );
    }
}
