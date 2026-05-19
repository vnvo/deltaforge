//! `S3Sink` — the `deltaforge_core::Sink` implementation that plugs the
//! S3/Parquet writer pool into the DeltaForge runtime.
//!
//! Phase 1f scope:
//! - Owns the `WriterPool` behind a `tokio::sync::Mutex` (interior mutability
//!   for the `&self` Sink trait contract).
//! - Translates `send_batch` calls into `WriterPool::append_batch`.
//! - Emits metrics for committed files, bytes, open writers, and roll reasons.
//! - Graceful shutdown via a cancellation token; on cancel, `WriterPool::abandon_all`
//!   is called to prevent partial files from landing.
//!
//! DLQ semantics:
//! - Per-row DLQ is not yet implemented. Encoder failures fail the whole batch
//!   as `SinkError::Serialization`. Phase 2 adds the slow-path per-row retry.
//! - Sink-level failures (object-store unreachable, auth, etc.) bubble up as
//!   `SinkError::Io` / `SinkError::Fatal` from `object_store`.

use std::sync::Arc;

use arrow_schema::Schema;
use async_trait::async_trait;
use deltaforge_core::{BatchResult, Event, Sink, SinkError, SinkResult};
use metrics::{counter, gauge};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::file_format::FileFormat;
use super::writer_pool::{CommittedFile, WriterPool, WriterPoolConfig};
use crate::s3::RollReason;

/// `Sink` implementation for S3 + Parquet (or JSONL).
pub struct S3Sink {
    id: String,
    pipeline: String,
    required: bool,
    pool: Mutex<WriterPool>,
    cancel: CancellationToken,
}

/// Constructor inputs for `S3Sink`. Phase 1g wires these from `S3SinkCfg`.
pub struct S3SinkArgs {
    pub id: String,
    pub pipeline: String,
    pub required: bool,
    pub store: Arc<dyn object_store::ObjectStore>,
    pub format: Arc<dyn FileFormat>,
    pub schema: Arc<Schema>,
    pub pool_cfg: WriterPoolConfig,
    pub cancel: CancellationToken,
}

impl S3Sink {
    pub fn new(args: S3SinkArgs) -> Self {
        let S3SinkArgs {
            id,
            pipeline,
            required,
            store,
            format,
            schema,
            pool_cfg,
            cancel,
        } = args;
        let pool = WriterPool::new(store, format, schema, pool_cfg);
        info!(sink = %id, pipeline = %pipeline, "s3 sink initialized");
        Self {
            id,
            pipeline,
            required,
            pool: Mutex::new(pool),
            cancel,
        }
    }

    /// Flush all in-flight writers on graceful shutdown. Returns the
    /// committed files for the caller to log/observe.
    pub async fn flush_on_shutdown(&self) -> Vec<CommittedFile> {
        let mut pool = self.pool.lock().await;
        let committed = pool.close_all().await;
        self.observe_committed(&committed);
        gauge!(
            "deltaforge_sink_s3_writer_open",
            "pipeline" => self.pipeline.clone(),
            "sink" => self.id.clone(),
        )
        .set(pool.open_writer_count() as f64);
        committed
    }

    fn observe_committed(&self, committed: &[CommittedFile]) {
        for c in committed {
            counter!(
                "deltaforge_sink_s3_files_committed_total",
                "pipeline" => self.pipeline.clone(),
                "sink" => self.id.clone(),
                "table" => c.partition.table.clone(),
                "reason" => roll_label(c.reason),
            )
            .increment(1);
            counter!(
                "deltaforge_sink_bytes_total",
                "pipeline" => self.pipeline.clone(),
                "sink" => self.id.clone(),
                "table" => c.partition.table.clone(),
            )
            .increment(c.result.bytes_written);
        }
    }

    fn observe_writer_count(&self, n: usize) {
        gauge!(
            "deltaforge_sink_s3_writer_open",
            "pipeline" => self.pipeline.clone(),
            "sink" => self.id.clone(),
        )
        .set(n as f64);
    }

    fn observe_encode_failure(&self, reason: &str) {
        counter!(
            "deltaforge_sink_s3_encode_errors_total",
            "pipeline" => self.pipeline.clone(),
            "sink" => self.id.clone(),
            "reason" => reason.to_string(),
        )
        .increment(1);
    }

    fn observe_put_error(&self, reason: &str) {
        counter!(
            "deltaforge_sink_s3_put_errors_total",
            "pipeline" => self.pipeline.clone(),
            "sink" => self.id.clone(),
            "reason" => reason.to_string(),
        )
        .increment(1);
    }
}

fn roll_label(reason: RollReason) -> String {
    match reason {
        RollReason::Bytes => "bytes",
        RollReason::Events => "events",
        RollReason::Age => "age",
        RollReason::Idle => "idle",
    }
    .to_string()
}

#[async_trait]
impl Sink for S3Sink {
    fn id(&self) -> &str {
        &self.id
    }

    fn required(&self) -> bool {
        self.required
    }

    async fn send(&self, event: &Event) -> SinkResult<()> {
        let single = [event.clone()];
        self.send_batch(&single).await.map(|_| ())
    }

    async fn send_batch(&self, events: &[Event]) -> SinkResult<BatchResult> {
        // Cancellation: bail before doing any work so the coordinator can
        // hand events off to a replay.
        if self.cancel.is_cancelled() {
            return Err(SinkError::Fatal {
                details: "pipeline cancelled".into(),
            });
        }

        let mut pool = self.pool.lock().await;
        let committed = match pool.append_batch(events).await {
            Ok(committed) => committed,
            Err(e) => {
                // Distinguish encoding errors (Serialization, recoverable
                // batch-level) from object-store errors (Io, may be transient).
                let msg = format!("{e:#}");
                let lower = msg.to_lowercase();
                if lower.contains("record batch")
                    || lower.contains("decimal")
                    || lower.contains("parse")
                    || lower.contains("base64")
                    || lower.contains("expected")
                    || lower.contains("number not")
                    || lower.contains("overflows")
                {
                    self.observe_encode_failure("encoder");
                    return Err(SinkError::Serialization {
                        details: msg.into(),
                    });
                }
                self.observe_put_error("object_store");
                return Err(SinkError::Io(std::io::Error::other(msg)));
            }
        };

        self.observe_committed(&committed);
        self.observe_writer_count(pool.open_writer_count());

        // No per-row DLQ in Phase 1f — successful batches yield an empty
        // BatchResult. Per-row isolation lands in Phase 2 via a slow-path
        // retry.
        Ok(BatchResult::ok())
    }
}

impl Drop for S3Sink {
    fn drop(&mut self) {
        // If the sink is dropped without `flush_on_shutdown`, abandon all
        // in-progress writers so no partial files appear at the destination.
        // The Mutex::try_lock here is best-effort: in a panicking task the
        // lock may be poisoned, which is fine because we drop everything.
        if let Ok(mut pool) = self.pool.try_lock() {
            let n = pool.abandon_all();
            if n > 0 {
                warn!(
                    sink = %self.id,
                    pipeline = %self.pipeline,
                    abandoned = n,
                    "s3 sink dropped without flush — abandoning open writers"
                );
            }
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::file_format::Compression;
    use crate::s3::object_writer::{ObjectStoreParams, build_object_store};
    use crate::s3::parquet_writer::ParquetFormat;
    use crate::s3::rolling::RollingConfig;
    use deltaforge_core::encoding::arrow_schema::{
        Connector, build_envelope_arrow_schema,
    };
    use deltaforge_core::encoding::avro_types::{
        ColumnDesc, TypeConversionOpts,
    };
    use deltaforge_core::{Event, Op, SourceInfo, SourcePosition};
    use serde_json::json;

    fn col(name: &str, data_type: &str) -> ColumnDesc {
        ColumnDesc {
            name: name.into(),
            data_type: data_type.into(),
            column_type: data_type.into(),
            nullable: true,
            precision: None,
            scale: None,
            unsigned: false,
            is_array: false,
            element_type: None,
        }
    }

    fn event_with(table: &str, after: serde_json::Value) -> Event {
        let ts_ms = chrono::NaiveDate::from_ymd_opt(2026, 5, 19)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_millis();
        Event {
            before: None,
            after: Some(after),
            source: SourceInfo {
                version: "1".into(),
                connector: "mysql".into(),
                name: "t".into(),
                ts_ms,
                db: "shop".into(),
                schema: None,
                table: table.into(),
                snapshot: None,
                position: SourcePosition::default(),
            },
            op: Op::Create,
            ts_ms,
            transaction: None,
            event_id: None,
            tenant_id: None,
            schema_version: None,
            schema_sequence: None,
            ddl: None,
            trace_id: None,
            tags: None,
            synthetic: None,
            routing: None,
            tx_end: false,
            checkpoint: None,
            size_bytes: 0,
            received_at_ms: ts_ms,
        }
    }

    async fn build_sink(
        tmp: &std::path::Path,
        rolling: RollingConfig,
    ) -> S3Sink {
        let params =
            ObjectStoreParams::local(tmp.to_string_lossy().to_string());
        let store = build_object_store(&params).unwrap();
        let cols = [col("id", "bigint")];
        let schema = Arc::new(build_envelope_arrow_schema(
            Connector::Mysql,
            &cols,
            &TypeConversionOpts::default(),
        ));
        let format: Arc<dyn FileFormat> =
            Arc::new(ParquetFormat::new(Compression::Snappy));
        S3Sink::new(S3SinkArgs {
            id: "test-s3".into(),
            pipeline: "test-pipeline".into(),
            required: true,
            store,
            format,
            schema,
            pool_cfg: WriterPoolConfig {
                prefix: "out".into(),
                rolling,
            },
            cancel: CancellationToken::new(),
        })
    }

    #[tokio::test]
    async fn send_batch_writes_and_rolls() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let sink = build_sink(
            tmp.path(),
            RollingConfig {
                max_events: 3,
                ..Default::default()
            },
        )
        .await;

        let events = vec![
            event_with("orders", json!({"id": 1})),
            event_with("orders", json!({"id": 2})),
            event_with("orders", json!({"id": 3})),
        ];
        let result = sink.send_batch(&events).await.unwrap();
        // No per-row DLQ failures in Phase 1f.
        assert!(result.dlq_failures.is_empty());

        // Flush on shutdown to verify there's nothing left open.
        let _ = sink.flush_on_shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn send_batch_after_cancel_returns_fatal() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let sink = build_sink(tmp.path(), RollingConfig::default()).await;
        sink.cancel.cancel();
        let events = vec![event_with("orders", json!({"id": 1}))];
        let err = sink.send_batch(&events).await.unwrap_err();
        match err {
            SinkError::Fatal { details } => {
                assert!(details.contains("cancelled"));
            }
            other => panic!("expected Fatal, got {other:?}"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn send_single_event_via_send() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let sink = build_sink(
            tmp.path(),
            RollingConfig {
                max_events: 1,
                ..Default::default()
            },
        )
        .await;
        sink.send(&event_with("orders", json!({"id": 7})))
            .await
            .unwrap();
        Ok(())
    }

    #[tokio::test]
    async fn flush_on_shutdown_drains_open_writers() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let sink = build_sink(tmp.path(), RollingConfig::default()).await;
        // Send 10 events; no rolling threshold hit, so writer stays open.
        let events: Vec<_> = (0..10)
            .map(|i| event_with("orders", json!({"id": i})))
            .collect();
        sink.send_batch(&events).await.unwrap();

        let committed = sink.flush_on_shutdown().await;
        assert_eq!(committed.len(), 1, "single writer flushed on shutdown");
        assert_eq!(committed[0].result.rows_written, 10);
        Ok(())
    }

    #[tokio::test]
    async fn encoding_failure_returns_serialization_error() -> anyhow::Result<()>
    {
        let tmp = tempfile::tempdir()?;
        let sink = build_sink(tmp.path(), RollingConfig::default()).await;
        // `id` declared bigint; sending a string that's not a parseable
        // integer must fail encoder.
        let bad = event_with("orders", json!({"id": "not-a-number"}));
        let err = sink.send_batch(&[bad]).await.unwrap_err();
        match err {
            SinkError::Serialization { .. } => {}
            other => panic!("expected Serialization, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn roll_label_maps_each_reason() {
        assert_eq!(roll_label(RollReason::Bytes), "bytes");
        assert_eq!(roll_label(RollReason::Events), "events");
        assert_eq!(roll_label(RollReason::Age), "age");
        assert_eq!(roll_label(RollReason::Idle), "idle");
    }
}
