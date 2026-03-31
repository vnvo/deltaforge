//! Dead Letter Queue writer — routes per-event failures to a durable queue
//! backed by the StorageBackend.
//!
//! The DLQ is a bounded FIFO queue with configurable overflow policy.
//! Entries are written during batch delivery when individual events fail
//! serialization or routing. The pipeline continues with the remaining events.

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use deltaforge_config::{DlqStreamConfig, OverflowPolicy};
use deltaforge_core::journal::{DlqMeta, JournalEntry};
use deltaforge_core::{Event, SinkError};
use metrics::{counter, gauge};
use storage::ArcStorageBackend;
use tracing::{debug, error, warn};

/// Namespace used for all journal queue entries in the StorageBackend.
const JOURNAL_NS: &str = "journal";

/// DLQ writer — thin wrapper over StorageBackend.queue_* primitives.
pub struct DlqWriter {
    backend: ArcStorageBackend,
    pipeline: String,
    queue_key: String,
    config: DlqStreamConfig,
    max_event_bytes: usize,
    /// Notified when entries are acked (unblocks Block overflow policy).
    ack_notify: tokio::sync::Notify,
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
        }
    }

    /// Write a failed event to the DLQ.
    ///
    /// Handles payload truncation, overflow policy, and metrics.
    /// Errors from the DLQ write itself are logged but do not propagate —
    /// a broken DLQ should not stop the pipeline.
    pub async fn write(&self, event: &Event, sink_id: &str, error: &SinkError) {
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
            source_cursor: event.checkpoint.as_ref().map(|cp| {
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

        // Check overflow.
        let current_len = self
            .backend
            .queue_len(JOURNAL_NS, &self.queue_key)
            .await
            .unwrap_or(0);
        if current_len >= self.config.max_entries {
            match self.config.overflow_policy {
                OverflowPolicy::DropOldest => {
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
                    return;
                }
                OverflowPolicy::Block => {
                    // Block until space is available (operator acks entries).
                    // The ack() method calls ack_notify.notify_one() after removing entries.
                    warn!(
                        pipeline = %self.pipeline,
                        "DLQ overflow (block): pipeline blocked until operator acks entries"
                    );
                    loop {
                        self.ack_notify.notified().await;
                        let new_len = self
                            .backend
                            .queue_len(JOURNAL_NS, &self.queue_key)
                            .await
                            .unwrap_or(current_len);
                        if new_len < self.config.max_entries {
                            break;
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
                return;
            }
        };

        match self
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
            }
        }

        // Update gauges.
        self.update_gauges().await;
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

        // Ack one entry — this should unblock the writer.
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
}
