//! Dead Letter Queue writer - routes per-event failures to a durable queue
//! backed by the StorageBackend.
//!
//! The DLQ is a bounded FIFO queue with configurable overflow policy.
//! Entries are written during batch delivery when individual events fail
//! serialization or routing. The pipeline continues with the remaining events.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use deltaforge_config::{DlqStreamConfig, OverflowPolicy};
use deltaforge_core::journal::{DlqMeta, JournalEntry};
use deltaforge_core::{Event, SinkError};
use metrics::{counter, gauge};
use storage::ArcStorageBackend;
use tracing::{debug, error, warn};

/// Namespace used for all journal queue entries in the StorageBackend.
const JOURNAL_NS: &str = "journal";

/// Maximum time the `Block` overflow policy waits for space before failing
/// closed (returning [`DlqWrite::Dropped`]) so a full DLQ can never hang the
/// delivery task indefinitely.
const BLOCK_MAX_WAIT: Duration = Duration::from_secs(60);

/// Whether a per-row DLQ write durably captured the row. A required sink may
/// only acknowledge a batch (and advance its checkpoint) when every isolated
/// row is [`DlqWrite::Persisted`]; a [`DlqWrite::Dropped`] row is not durably
/// captured and must block the checkpoint to avoid permanent data loss.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DlqWrite {
    /// The row was durably appended to the DLQ with no data loss.
    Persisted,
    /// The row was not durably captured (overflow drop/reject, serialization
    /// failure, or backend write failure).
    Dropped,
}

/// DLQ writer - thin wrapper over StorageBackend.queue_* primitives.
pub struct DlqWriter {
    backend: ArcStorageBackend,
    pipeline: String,
    queue_key: String,
    config: DlqStreamConfig,
    max_event_bytes: usize,
    /// Notified when entries are acked (unblocks Block overflow policy).
    ack_notify: tokio::sync::Notify,
    /// Upper bound on the `Block` overflow wait before failing closed.
    block_max_wait: Duration,
}

impl DlqWriter {
    pub fn new(
        backend: ArcStorageBackend,
        pipeline: String,
        config: DlqStreamConfig,
        max_event_bytes: usize,
    ) -> Self {
        let queue_key = format!("{}:dlq", pipeline);
        Self {
            backend,
            pipeline,
            queue_key,
            config,
            max_event_bytes,
            ack_notify: tokio::sync::Notify::new(),
            block_max_wait: BLOCK_MAX_WAIT,
        }
    }

    /// Write a failed event to the DLQ.
    ///
    /// Handles payload truncation, overflow policy, and metrics. Returns whether
    /// the row was **durably captured**: [`DlqWrite::Persisted`] only when the
    /// entry is durably appended with no data loss, and [`DlqWrite::Dropped`]
    /// when it was not (overflow drop/reject, serialization failure, or backend
    /// write failure). Callers gating a required sink's acknowledgement MUST
    /// treat `Dropped` as an unhandled failure and refuse to advance the
    /// checkpoint - otherwise the row is permanently lost.
    pub async fn write(
        &self,
        event: &Event,
        sink_id: &str,
        error: &SinkError,
    ) -> DlqWrite {
        let error_kind = error.kind().to_string();

        // Build the journal entry.
        let mut entry = JournalEntry {
            seq: 0, // assigned by storage backend
            timestamp: now_epoch_secs(),
            pipeline: self.pipeline.clone(),
            stream: "dlq".into(),
            event_id: event
                .event_id
                .map(|id| id.to_string())
                .unwrap_or_default(),
            source_cursor: event.checkpoint().map(|cp| {
                serde_json::from_slice(cp.as_bytes())
                    .unwrap_or(serde_json::Value::Null)
            }),
            event: serde_json::to_value(event)
                .unwrap_or(serde_json::Value::Null),
            payload_truncated: false,
            meta: DlqMeta {
                sink_id: sink_id.to_string(),
                error_kind: error_kind.clone(),
                error_message: error.details(),
                attempts: 1,
            }
            .to_json(),
        };

        // Truncate oversized payloads.
        entry.truncate_payload(self.max_event_bytes);

        // Check overflow. `overflow_dropped` records that satisfying this write
        // cost previously-captured data (eviction), which is a durability loss:
        // a required sink must not acknowledge on that basis.
        let mut overflow_dropped = false;
        let current_len = self
            .backend
            .queue_len(JOURNAL_NS, &self.queue_key)
            .await
            .unwrap_or(0);
        if current_len >= self.config.max_entries {
            match self.config.overflow_policy {
                OverflowPolicy::DropOldest => {
                    overflow_dropped = true;
                    let to_drop =
                        (current_len - self.config.max_entries + 1) as usize;
                    if let Err(e) = self
                        .backend
                        .queue_drop_oldest(JOURNAL_NS, &self.queue_key, to_drop)
                        .await
                    {
                        error!(
                            pipeline = %self.pipeline,
                            error = %e,
                            "DLQ overflow: failed to drop oldest entries"
                        );
                    }
                    counter!(
                        "deltaforge_dlq_evicted_total",
                        "pipeline" => self.pipeline.clone(),
                    )
                    .increment(to_drop as u64);
                }
                OverflowPolicy::Reject => {
                    warn!(
                        pipeline = %self.pipeline,
                        sink = sink_id,
                        event_id = ?event.event_id,
                        "DLQ overflow (reject): event dropped, not persisted"
                    );
                    counter!(
                        "deltaforge_dlq_rejected_total",
                        "pipeline" => self.pipeline.clone(),
                    )
                    .increment(1);
                    return DlqWrite::Dropped;
                }
                OverflowPolicy::Block => {
                    // Block until space is available (operator acks entries) -
                    // but bounded and fail-closed. An unbounded wait would hang
                    // the delivery task forever (past any sink deadline), and a
                    // backend that keeps erroring must not be mistaken for a full
                    // queue. On the deadline or a backend error we return
                    // `Dropped` so a required sink refuses to acknowledge rather
                    // than blocking indefinitely or silently losing the row.
                    warn!(
                        pipeline = %self.pipeline,
                        "DLQ overflow (block): waiting up to {}s for operator acks",
                        self.block_max_wait.as_secs(),
                    );
                    let deadline =
                        std::time::Instant::now() + self.block_max_wait;
                    loop {
                        let remaining = deadline.saturating_duration_since(
                            std::time::Instant::now(),
                        );
                        if remaining.is_zero() {
                            warn!(
                                pipeline = %self.pipeline,
                                "DLQ overflow (block): timed out waiting for space; failing closed"
                            );
                            counter!(
                                "deltaforge_dlq_write_failures_total",
                                "pipeline" => self.pipeline.clone(),
                            )
                            .increment(1);
                            return DlqWrite::Dropped;
                        }
                        // Wake on an ack, or re-check periodically on timeout.
                        let _ = tokio::time::timeout(
                            remaining.min(Duration::from_secs(1)),
                            self.ack_notify.notified(),
                        )
                        .await;
                        match self
                            .backend
                            .queue_len(JOURNAL_NS, &self.queue_key)
                            .await
                        {
                            Ok(new_len) => {
                                if new_len < self.config.max_entries {
                                    break;
                                }
                            }
                            Err(e) => {
                                error!(
                                    pipeline = %self.pipeline,
                                    error = %e,
                                    "DLQ overflow (block): backend error while waiting; failing closed"
                                );
                                counter!(
                                    "deltaforge_dlq_write_failures_total",
                                    "pipeline" => self.pipeline.clone(),
                                )
                                .increment(1);
                                return DlqWrite::Dropped;
                            }
                        }
                    }
                }
            }
        }

        // Serialize and push to queue.
        let bytes = match serde_json::to_vec(&entry) {
            Ok(b) => b,
            Err(e) => {
                error!(
                    pipeline = %self.pipeline,
                    error = %e,
                    "failed to serialize DLQ entry"
                );
                counter!(
                    "deltaforge_dlq_write_failures_total",
                    "pipeline" => self.pipeline.clone(),
                )
                .increment(1);
                return DlqWrite::Dropped;
            }
        };

        let outcome = match self
            .backend
            .queue_push(JOURNAL_NS, &self.queue_key, &bytes)
            .await
        {
            Ok(seq) => {
                debug!(
                    pipeline = %self.pipeline,
                    sink = sink_id,
                    event_id = ?event.event_id,
                    seq,
                    error_kind = %error_kind,
                    "event routed to DLQ"
                );
                counter!(
                    "deltaforge_dlq_events_total",
                    "pipeline" => self.pipeline.clone(),
                    "sink" => sink_id.to_string(),
                    "error_kind" => error_kind,
                )
                .increment(1);
                // Durably appended - but if we had to evict older entries to
                // make room, that eviction is itself a loss.
                if overflow_dropped {
                    DlqWrite::Dropped
                } else {
                    DlqWrite::Persisted
                }
            }
            Err(e) => {
                error!(
                    pipeline = %self.pipeline,
                    error = %e,
                    "failed to push to DLQ queue"
                );
                counter!(
                    "deltaforge_dlq_write_failures_total",
                    "pipeline" => self.pipeline.clone(),
                )
                .increment(1);
                DlqWrite::Dropped
            }
        };

        // Update gauges.
        self.update_gauges().await;
        outcome
    }

    /// Peek at the oldest N unacked entries.
    pub async fn peek(&self, limit: usize) -> Result<Vec<JournalEntry>> {
        let raw = self
            .backend
            .queue_peek(JOURNAL_NS, &self.queue_key, limit)
            .await
            .context("DLQ peek")?;

        raw.into_iter()
            .map(|(seq, bytes)| {
                let mut entry: JournalEntry = serde_json::from_slice(&bytes)
                    .context("deserialize DLQ entry")?;
                entry.seq = seq;
                Ok(entry)
            })
            .collect()
    }

    /// Acknowledge (remove) entries from the head up to `up_to_seq`.
    pub async fn ack(&self, up_to_seq: u64) -> Result<usize> {
        let count = self
            .backend
            .queue_ack(JOURNAL_NS, &self.queue_key, up_to_seq)
            .await
            .context("DLQ ack")?;
        self.update_gauges().await;
        if count > 0 {
            self.ack_notify.notify_one();
        }
        Ok(count)
    }

    /// Count of unacked entries.
    pub async fn len(&self) -> Result<u64> {
        self.backend
            .queue_len(JOURNAL_NS, &self.queue_key)
            .await
            .context("DLQ len")
    }

    /// Remove all entries.
    pub async fn purge(&self) -> Result<usize> {
        // Ack up to u64::MAX removes everything.
        let count = self
            .backend
            .queue_ack(JOURNAL_NS, &self.queue_key, u64::MAX)
            .await
            .context("DLQ purge")?;
        self.update_gauges().await;
        if count > 0 {
            self.ack_notify.notify_one();
        }
        Ok(count)
    }

    /// Remove entries older than `max_age_secs`. Returns count of removed entries.
    /// Called by the background cleanup task.
    pub async fn cleanup_expired(&self) -> Result<usize> {
        if self.config.max_age_secs == 0 {
            return Ok(0);
        }
        let cutoff = now_epoch_secs() - self.config.max_age_secs as i64;
        // Peek a batch of entries from the head and ack up to the last expired one.
        // Since the queue is FIFO and entries are ordered by insertion time,
        // we can stop at the first non-expired entry.
        let entries = self.peek(1000).await.unwrap_or_default();
        let mut last_expired_seq: Option<u64> = None;
        for entry in &entries {
            if entry.timestamp <= cutoff {
                last_expired_seq = Some(entry.seq);
            } else {
                break; // FIFO order - all remaining are newer
            }
        }
        if let Some(seq) = last_expired_seq {
            let count = self.ack(seq).await?;
            if count > 0 {
                debug!(
                    pipeline = %self.pipeline,
                    count,
                    "DLQ cleanup: removed expired entries"
                );
            }
            Ok(count)
        } else {
            Ok(0)
        }
    }

    /// Spawn a background cleanup task that runs every 60 seconds.
    /// Returns a JoinHandle that can be aborted when the pipeline stops.
    pub fn spawn_cleanup_task(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let dlq = Arc::clone(self);
        tokio::spawn(async move {
            // Best-effort startup cleanup (bounded to 5s).
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                dlq.cleanup_expired(),
            )
            .await;

            let mut interval =
                tokio::time::interval(std::time::Duration::from_secs(60));
            interval.set_missed_tick_behavior(
                tokio::time::MissedTickBehavior::Delay,
            );
            loop {
                interval.tick().await;
                let _ = dlq.cleanup_expired().await;
            }
        })
    }

    /// Update the DLQ gauge metrics.
    async fn update_gauges(&self) {
        let len = self
            .backend
            .queue_len(JOURNAL_NS, &self.queue_key)
            .await
            .unwrap_or(0);
        gauge!(
            "deltaforge_dlq_entries",
            "pipeline" => self.pipeline.clone(),
        )
        .set(len as f64);

        if self.config.max_entries > 0 {
            gauge!(
                "deltaforge_dlq_saturation_ratio",
                "pipeline" => self.pipeline.clone(),
            )
            .set(len as f64 / self.config.max_entries as f64);
        }

        // Health signals.
        let ratio = if self.config.max_entries > 0 {
            len as f64 / self.config.max_entries as f64
        } else {
            0.0
        };
        if ratio >= 0.95 {
            error!(pipeline = %self.pipeline, ratio, "DLQ saturation critical (>=95%)");
        } else if ratio >= 0.80 {
            warn!(pipeline = %self.pipeline, ratio, "DLQ saturation warning (>=80%)");
        }
    }
}

fn now_epoch_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltaforge_core::{Event, Op, SourceInfo, SourcePosition};
    use serde_json::json;
    use std::sync::Arc;
    use storage::MemoryStorageBackend;

    fn make_test_event(id: i64) -> Event {
        Event::new_row(
            deltaforge_core::EventId::mysql_row_server(1, "t", 1, 0),
            SourceInfo {
                version: "test".into(),
                connector: "test".into(),
                name: "test".into(),
                ts_ms: 0,
                db: "testdb".into(),
                schema: None,
                table: "t".into(),
                snapshot: None,
                position: SourcePosition::default(),
            },
            Op::Create,
            None,
            Some(json!({"id": id})),
            0,
            64,
        )
    }

    fn make_dlq_writer(
        backend: ArcStorageBackend,
        max_entries: u64,
        policy: OverflowPolicy,
    ) -> DlqWriter {
        DlqWriter::new(
            backend,
            "test-pipeline".into(),
            DlqStreamConfig {
                max_entries,
                max_age_secs: 3600,
                overflow_policy: policy,
            },
            256 * 1024,
        )
    }

    /// A backend that delegates to an inner `MemoryStorageBackend` for everything
    /// except `queue_push`, which always fails - to prove the DLQ write reports
    /// `Dropped` (and callers fail closed) when the durable append cannot commit.
    #[derive(Debug)]
    struct QueuePushFailBackend(MemoryStorageBackend);

    #[async_trait::async_trait]
    impl storage::StorageBackend for QueuePushFailBackend {
        async fn kv_get(
            &self,
            ns: &str,
            key: &str,
        ) -> anyhow::Result<Option<Vec<u8>>> {
            self.0.kv_get(ns, key).await
        }
        async fn kv_put(
            &self,
            ns: &str,
            key: &str,
            value: &[u8],
        ) -> anyhow::Result<()> {
            self.0.kv_put(ns, key, value).await
        }
        async fn kv_put_with_ttl(
            &self,
            ns: &str,
            key: &str,
            value: &[u8],
            ttl_secs: u64,
        ) -> anyhow::Result<()> {
            self.0.kv_put_with_ttl(ns, key, value, ttl_secs).await
        }
        async fn kv_delete(&self, ns: &str, key: &str) -> anyhow::Result<bool> {
            self.0.kv_delete(ns, key).await
        }
        async fn kv_list(
            &self,
            ns: &str,
            prefix: Option<&str>,
        ) -> anyhow::Result<Vec<String>> {
            self.0.kv_list(ns, prefix).await
        }
        async fn log_append(
            &self,
            ns: &str,
            key: &str,
            value: &[u8],
        ) -> anyhow::Result<u64> {
            self.0.log_append(ns, key, value).await
        }
        async fn log_list(
            &self,
            ns: &str,
            key: &str,
        ) -> anyhow::Result<Vec<(u64, Vec<u8>)>> {
            self.0.log_list(ns, key).await
        }
        async fn log_since(
            &self,
            ns: &str,
            key: &str,
            since_seq: u64,
        ) -> anyhow::Result<Vec<(u64, Vec<u8>)>> {
            self.0.log_since(ns, key, since_seq).await
        }
        async fn log_latest(
            &self,
            ns: &str,
            key: &str,
        ) -> anyhow::Result<Option<(u64, Vec<u8>)>> {
            self.0.log_latest(ns, key).await
        }
        async fn log_ns_max_seq(&self, ns: &str) -> anyhow::Result<u64> {
            self.0.log_ns_max_seq(ns).await
        }
        async fn log_append_if_absent(
            &self,
            ns: &str,
            key: &str,
            capture_id: &str,
            value: &[u8],
        ) -> anyhow::Result<storage::LogAppendOutcome> {
            self.0
                .log_append_if_absent(ns, key, capture_id, value)
                .await
        }
        async fn log_truncate(
            &self,
            ns: &str,
            key: &str,
            req: storage::LogTruncateRequest,
        ) -> anyhow::Result<storage::LogTruncateOutcome> {
            self.0.log_truncate(ns, key, req).await
        }
        async fn log_stream_meta(
            &self,
            ns: &str,
            key: &str,
        ) -> anyhow::Result<storage::LogStreamMeta> {
            self.0.log_stream_meta(ns, key).await
        }
        async fn log_read_meta_since(
            &self,
            ns: &str,
            key: &str,
            since_seq: u64,
            limit: usize,
        ) -> anyhow::Result<Vec<storage::LogEntryMeta>> {
            self.0.log_read_meta_since(ns, key, since_seq, limit).await
        }
        async fn slot_upsert(
            &self,
            ns: &str,
            key: &str,
            state: &[u8],
        ) -> anyhow::Result<u64> {
            self.0.slot_upsert(ns, key, state).await
        }
        async fn slot_get(
            &self,
            ns: &str,
            key: &str,
        ) -> anyhow::Result<Option<(u64, Vec<u8>)>> {
            self.0.slot_get(ns, key).await
        }
        async fn slot_cas(
            &self,
            ns: &str,
            key: &str,
            expected_version: u64,
            state: &[u8],
        ) -> anyhow::Result<bool> {
            self.0.slot_cas(ns, key, expected_version, state).await
        }
        async fn slot_create(
            &self,
            ns: &str,
            key: &str,
            state: &[u8],
        ) -> anyhow::Result<Option<u64>> {
            self.0.slot_create(ns, key, state).await
        }
        async fn slot_delete(
            &self,
            ns: &str,
            key: &str,
        ) -> anyhow::Result<bool> {
            self.0.slot_delete(ns, key).await
        }
        async fn slot_list(
            &self,
            ns: &str,
            prefix: Option<&str>,
            cursor: Option<&str>,
            limit: usize,
        ) -> anyhow::Result<storage::SlotPage> {
            self.0.slot_list(ns, prefix, cursor, limit).await
        }
        async fn queue_push(
            &self,
            _ns: &str,
            _key: &str,
            _value: &[u8],
        ) -> anyhow::Result<u64> {
            Err(anyhow::anyhow!("injected queue_push failure"))
        }
        async fn queue_peek(
            &self,
            ns: &str,
            key: &str,
            limit: usize,
        ) -> anyhow::Result<Vec<(u64, Vec<u8>)>> {
            self.0.queue_peek(ns, key, limit).await
        }
        async fn queue_ack(
            &self,
            ns: &str,
            key: &str,
            up_to_id: u64,
        ) -> anyhow::Result<usize> {
            self.0.queue_ack(ns, key, up_to_id).await
        }
        async fn queue_len(&self, ns: &str, key: &str) -> anyhow::Result<u64> {
            self.0.queue_len(ns, key).await
        }
        async fn queue_drop_oldest(
            &self,
            ns: &str,
            key: &str,
            count: usize,
        ) -> anyhow::Result<usize> {
            self.0.queue_drop_oldest(ns, key, count).await
        }
    }

    /// R3-C1 durability boundary: when the storage backend's `queue_push` fails,
    /// the DLQ write must report `Dropped` (nothing durably captured) so a
    /// required sink refuses to acknowledge and the checkpoint is held.
    #[tokio::test]
    async fn queue_push_backend_failure_returns_dropped() {
        let backend: ArcStorageBackend =
            Arc::new(QueuePushFailBackend(MemoryStorageBackend::new()));
        // Large capacity so this is the plain push path, not overflow.
        let dlq = make_dlq_writer(backend, 1000, OverflowPolicy::Reject);
        let err = SinkError::Serialization {
            details: "bad".into(),
        };
        let outcome = dlq.write(&make_test_event(0), "kafka", &err).await;
        assert_eq!(
            outcome,
            DlqWrite::Dropped,
            "a failed queue_push must report Dropped, not silently succeed"
        );
        // Nothing was durably captured.
        assert_eq!(dlq.len().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn write_and_peek() {
        let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
        let dlq = make_dlq_writer(backend, 100, OverflowPolicy::DropOldest);

        let event = make_test_event(1);
        let err = SinkError::Serialization {
            details: "bad encoding".into(),
        };

        dlq.write(&event, "kafka", &err).await;

        let entries = dlq.peek(10).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].pipeline, "test-pipeline");
        assert_eq!(entries[0].stream, "dlq");

        let meta = DlqMeta::from_json(&entries[0].meta).unwrap();
        assert_eq!(meta.sink_id, "kafka");
        assert_eq!(meta.error_kind, "serialization error");
    }

    #[tokio::test]
    async fn ack_removes_entries() {
        let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
        let dlq = make_dlq_writer(backend, 100, OverflowPolicy::DropOldest);

        let err = SinkError::Serialization {
            details: "bad".into(),
        };
        for i in 0..5 {
            dlq.write(&make_test_event(i), "kafka", &err).await;
        }

        assert_eq!(dlq.len().await.unwrap(), 5);

        let entries = dlq.peek(5).await.unwrap();
        let seq_2 = entries[1].seq; // ack first 2
        let acked = dlq.ack(seq_2).await.unwrap();
        assert_eq!(acked, 2);
        assert_eq!(dlq.len().await.unwrap(), 3);
    }

    #[tokio::test]
    async fn purge_removes_all() {
        let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
        let dlq = make_dlq_writer(backend, 100, OverflowPolicy::DropOldest);

        let err = SinkError::Routing {
            details: "no topic".into(),
        };
        for i in 0..3 {
            dlq.write(&make_test_event(i), "nats", &err).await;
        }

        let purged = dlq.purge().await.unwrap();
        assert_eq!(purged, 3);
        assert_eq!(dlq.len().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn overflow_drop_oldest() {
        let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
        let dlq = make_dlq_writer(backend, 3, OverflowPolicy::DropOldest);

        let err = SinkError::Serialization {
            details: "bad".into(),
        };
        for i in 0..5 {
            dlq.write(&make_test_event(i), "kafka", &err).await;
        }

        // Only 3 entries should remain (oldest 2 evicted).
        assert_eq!(dlq.len().await.unwrap(), 3);

        let entries = dlq.peek(10).await.unwrap();
        // The remaining entries should be the 3 most recent.
        let ids: Vec<_> = entries
            .iter()
            .map(|e| e.event["after"]["id"].as_i64().unwrap())
            .collect();
        assert_eq!(ids, vec![2, 3, 4]);
    }

    #[tokio::test]
    async fn overflow_block_waits_for_ack() {
        let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
        let dlq = Arc::new(make_dlq_writer(
            Arc::clone(&backend),
            2,
            OverflowPolicy::Block,
        ));

        let err = SinkError::Serialization {
            details: "bad".into(),
        };

        // Fill the queue to capacity.
        dlq.write(&make_test_event(0), "kafka", &err).await;
        dlq.write(&make_test_event(1), "kafka", &err).await;
        assert_eq!(dlq.len().await.unwrap(), 2);

        // Spawn a write that should block because queue is full.
        let dlq_clone = Arc::clone(&dlq);
        let write_handle = tokio::spawn(async move {
            dlq_clone
                .write(
                    &make_test_event(2),
                    "kafka",
                    &SinkError::Serialization {
                        details: "bad".into(),
                    },
                )
                .await;
        });

        // Give the write a moment to block.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!write_handle.is_finished(), "write should be blocked");

        // Ack one entry - this should unblock the writer.
        let entries = dlq.peek(1).await.unwrap();
        dlq.ack(entries[0].seq).await.unwrap();

        // Writer should complete within a reasonable time.
        tokio::time::timeout(std::time::Duration::from_secs(2), write_handle)
            .await
            .expect("write should unblock after ack")
            .expect("write task should not panic");

        // Queue should have 2 entries (1 original + 1 new, after 1 acked).
        assert_eq!(dlq.len().await.unwrap(), 2);
    }

    /// R3-C2 review follow-up: the Block overflow policy must be bounded - if no
    /// operator ack arrives, the write fails closed (Dropped) instead of hanging
    /// the delivery task forever.
    #[tokio::test]
    async fn overflow_block_times_out_and_fails_closed() {
        let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
        let mut dlq = make_dlq_writer(backend, 1, OverflowPolicy::Block);
        dlq.block_max_wait = Duration::from_millis(300);

        let err = SinkError::Serialization {
            details: "bad".into(),
        };
        // Fill to capacity: the next write hits Block with no ack coming.
        assert_eq!(
            dlq.write(&make_test_event(0), "kafka", &err).await,
            DlqWrite::Persisted
        );

        let start = std::time::Instant::now();
        let outcome = dlq.write(&make_test_event(1), "kafka", &err).await;
        let elapsed = start.elapsed();

        assert_eq!(
            outcome,
            DlqWrite::Dropped,
            "Block must fail closed when no space is freed"
        );
        assert!(
            elapsed >= Duration::from_millis(250)
                && elapsed < Duration::from_secs(5),
            "Block wait must be bounded (~block_max_wait), took {elapsed:?}"
        );
        // The blocked row was NOT persisted (queue still at capacity).
        assert_eq!(dlq.len().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn overflow_reject_drops_new() {
        let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
        let dlq = make_dlq_writer(backend, 2, OverflowPolicy::Reject);

        let err = SinkError::Serialization {
            details: "bad".into(),
        };
        for i in 0..5 {
            dlq.write(&make_test_event(i), "kafka", &err).await;
        }

        // Only first 2 entries persisted, rest rejected.
        assert_eq!(dlq.len().await.unwrap(), 2);

        let entries = dlq.peek(10).await.unwrap();
        let ids: Vec<_> = entries
            .iter()
            .map(|e| e.event["after"]["id"].as_i64().unwrap())
            .collect();
        assert_eq!(ids, vec![0, 1]);
    }

    #[tokio::test]
    async fn cleanup_expired_removes_old_entries() {
        let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
        let dlq = DlqWriter::new(
            backend,
            "test-cleanup".into(),
            DlqStreamConfig {
                max_entries: 100,
                max_age_secs: 2, // 2 second TTL
                overflow_policy: OverflowPolicy::DropOldest,
            },
            256 * 1024,
        );

        let err = SinkError::Serialization {
            details: "bad".into(),
        };

        // Write 3 entries.
        for i in 0..3 {
            dlq.write(&make_test_event(i), "kafka", &err).await;
        }
        assert_eq!(dlq.len().await.unwrap(), 3);

        // Cleanup immediately - nothing should expire (entries are <2s old).
        let removed = dlq.cleanup_expired().await.unwrap();
        assert_eq!(removed, 0);
        assert_eq!(dlq.len().await.unwrap(), 3);

        // Wait for entries to expire.
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        // Cleanup should remove all expired entries.
        let removed = dlq.cleanup_expired().await.unwrap();
        assert_eq!(removed, 3);
        assert_eq!(dlq.len().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn cleanup_zero_max_age_is_noop() {
        let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
        let dlq = DlqWriter::new(
            backend,
            "test-no-expiry".into(),
            DlqStreamConfig {
                max_entries: 100,
                max_age_secs: 0, // disabled
                overflow_policy: OverflowPolicy::DropOldest,
            },
            256 * 1024,
        );

        let err = SinkError::Serialization {
            details: "bad".into(),
        };
        dlq.write(&make_test_event(0), "kafka", &err).await;

        let removed = dlq.cleanup_expired().await.unwrap();
        assert_eq!(removed, 0);
        assert_eq!(dlq.len().await.unwrap(), 1);
    }
}
