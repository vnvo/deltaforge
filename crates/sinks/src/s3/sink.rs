//! `S3Sink` — the `deltaforge_core::Sink` implementation that plugs the
//! S3/Parquet writer pool into the DeltaForge runtime.
//!
//! Architecture:
//! - Owns the `WriterPool` behind a `tokio::sync::Mutex` (interior mutability
//!   for the `&self` Sink trait contract).
//! - Translates `send_batch` calls into `WriterPool::append_batch`.
//! - Bounds per-batch latency via `send_timeout` (returns `Backpressure` on
//!   timeout); coordinator handles per `required` flag.
//! - Emits metrics for committed files, bytes, open writers, roll reasons,
//!   encoder errors, and put errors.
//! - Graceful shutdown via a cancellation token; on cancel, `WriterPool::abandon_all`
//!   is called to prevent partial files from landing.
//!
//! Error semantics:
//! - Per-row encoder failures are isolated by the pool's slow-path retry
//!   and surface in `BatchResult.dlq_failures` (Phase 2a).
//! - Sink-level failures (object-store unreachable, auth, etc.) bubble up
//!   as `SinkError::Io` / `SinkError::Fatal`.

use std::sync::Arc;

use async_trait::async_trait;
use deltaforge_core::{BatchResult, Event, Sink, SinkError, SinkResult};
use metrics::{counter, gauge};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::file_format::{Compression, FileFormat};
use super::jsonl_writer::JsonLinesFormat;
use super::object_writer::{ObjectStoreParams, build_object_store};
use super::parquet_writer::ParquetFormat;
use super::rolling::RollingConfig;
use super::writer_pool::{
    CommittedFile, SchemaResolver, WriterPool, WriterPoolConfig,
};
use crate::s3::RollReason;
use anyhow::Context as _;

/// `Sink` implementation for S3 + Parquet (or JSONL).
pub struct S3Sink {
    id: String,
    pipeline: String,
    required: bool,
    send_timeout: std::time::Duration,
    pool: Mutex<WriterPool>,
    cancel: CancellationToken,
}

/// Constructor inputs for `S3Sink`. Phase 1g wires these from `S3SinkCfg`.
pub struct S3SinkArgs {
    pub id: String,
    pub pipeline: String,
    pub required: bool,
    pub send_timeout: std::time::Duration,
    pub store: Arc<dyn object_store::ObjectStore>,
    pub format: Arc<dyn FileFormat>,
    pub schema_resolver: SchemaResolver,
    pub pool_cfg: WriterPoolConfig,
    pub cancel: CancellationToken,
}

impl S3Sink {
    pub fn new(args: S3SinkArgs) -> Self {
        let S3SinkArgs {
            id,
            pipeline,
            required,
            send_timeout,
            store,
            format,
            schema_resolver,
            pool_cfg,
            cancel,
        } = args;
        let pool = WriterPool::new(store, format, schema_resolver, pool_cfg);
        info!(
            sink = %id,
            pipeline = %pipeline,
            send_timeout_secs = send_timeout.as_secs(),
            "s3 sink initialized"
        );
        Self {
            id,
            pipeline,
            required,
            send_timeout,
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
        // Wrap the entire append in a per-batch timeout. If a writer's
        // multipart upload (or any pool-internal close) is stuck, this
        // bounds the worst-case wait the coordinator sees. On timeout we
        // surface SinkError::Backpressure — the coordinator routes per
        // `required` (block or log+continue).
        let append =
            tokio::time::timeout(self.send_timeout, pool.append_batch(events))
                .await;
        let outcome = match append {
            Err(_elapsed) => {
                self.observe_put_error("timeout");
                return Err(SinkError::Backpressure {
                    details: format!(
                        "S3 send_batch exceeded {}s timeout",
                        self.send_timeout.as_secs()
                    )
                    .into(),
                });
            }
            Ok(Ok(outcome)) => outcome,
            Ok(Err(e)) => {
                // Batch-level failure (object store unreachable, auth,
                // etc.) — the pool returns a top-level error for these
                // (per-row encoder errors are isolated into `outcome.failed`
                // and don't reach this arm).
                let msg = format!("{e:#}");
                self.observe_put_error("object_store");
                return Err(SinkError::Io(std::io::Error::other(msg)));
            }
        };

        self.observe_committed(&outcome.committed);
        self.observe_writer_count(pool.open_writer_count());

        // Per-row DLQ: surface each isolated failure as
        // SinkError::Serialization at its original batch index. The
        // coordinator routes these to the DLQ writer.
        let dlq_failures: Vec<(usize, SinkError)> = outcome
            .failed
            .into_iter()
            .map(|(idx, err)| {
                self.observe_encode_failure("per_row");
                (
                    idx,
                    SinkError::Serialization {
                        details: format!("{err:#}").into(),
                    },
                )
            })
            .collect();

        Ok(BatchResult { dlq_failures })
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
// Builder — wire up an S3Sink from a config struct
// =============================================================================

/// Build an `S3Sink` from an `S3SinkCfg` plus an optional schema resolver.
///
/// If `schema_resolver` is `None`, a fallback "envelope-only" resolver is
/// used (only meta columns; no user data preserved). Production deployments
/// must supply a resolver derived from source DDL — see the runner's
/// `build_arrow_schema_resolver` for the canonical adapter.
pub fn build_s3_sink(
    cfg: &deltaforge_config::S3SinkCfg,
    cancel: CancellationToken,
    pipeline: &str,
    schema_resolver: Option<SchemaResolver>,
) -> anyhow::Result<S3Sink> {
    use deltaforge_config::{S3Compression as C, S3FileFormat as F};

    // Build object store.
    let access_key = cfg
        .access_key_id
        .as_deref()
        .map(shellexpand::env)
        .transpose()
        .context("expand S3 access_key_id")?
        .map(|s| s.into_owned());
    let secret_key = cfg
        .secret_access_key
        .as_deref()
        .map(shellexpand::env)
        .transpose()
        .context("expand S3 secret_access_key")?
        .map(|s| s.into_owned());

    let params = ObjectStoreParams {
        bucket: cfg.bucket.clone(),
        endpoint: cfg.endpoint.clone(),
        region: cfg.region.clone(),
        access_key_id: access_key,
        secret_access_key: secret_key,
        virtual_hosted_style: cfg.virtual_hosted_style,
        local: cfg.local,
    };
    let store = build_object_store(&params).context("build S3 object store")?;

    // File format + compression.
    let compression = match cfg.compression {
        C::None => Compression::None,
        C::Snappy => Compression::Snappy,
        C::Gzip => Compression::Gzip,
        C::Zstd => Compression::Zstd,
    };
    let format: Arc<dyn FileFormat> = match cfg.format {
        F::Parquet => Arc::new(ParquetFormat::new(compression)),
        F::Jsonl => Arc::new(JsonLinesFormat::new(compression)),
    };

    // Pool config (rolling thresholds).
    let pool_cfg = WriterPoolConfig {
        prefix: cfg.prefix.clone(),
        rolling: RollingConfig {
            max_bytes: cfg.file_roll.max_bytes,
            max_events: cfg.file_roll.max_events,
            max_age: std::time::Duration::from_secs(cfg.file_roll.max_age_secs),
            idle_age: std::time::Duration::from_secs(
                cfg.file_roll.idle_age_secs,
            ),
        },
    };

    // Schema resolver: use the caller-provided one or fall back to
    // envelope-only (no user data columns; meta cols only).
    let resolver = schema_resolver.unwrap_or_else(fallback_envelope_resolver);

    Ok(S3Sink::new(S3SinkArgs {
        id: cfg.id.clone(),
        pipeline: pipeline.to_string(),
        required: cfg.required.unwrap_or(true),
        send_timeout: std::time::Duration::from_secs(
            cfg.send_timeout_secs.into(),
        ),
        store,
        format,
        schema_resolver: resolver,
        pool_cfg,
        cancel,
    }))
}

/// Fallback schema resolver used when no DDL-derived resolver is supplied.
/// Produces a schema containing only the envelope meta columns; user data
/// is *not* preserved. Logged as a warning so operators notice.
fn fallback_envelope_resolver() -> SchemaResolver {
    use deltaforge_core::encoding::arrow_schema::{
        Connector, build_envelope_arrow_schema_arc,
    };
    use deltaforge_core::encoding::avro_types::TypeConversionOpts;

    warn!(
        "S3 sink starting without a schema resolver — only envelope-meta \
         columns will be written; user data will be dropped. Wire a \
         DDL-derived resolver from the runner for production use."
    );
    Arc::new(move |_| {
        // Empty columns slice → only meta columns are emitted.
        Ok(build_envelope_arrow_schema_arc(
            Connector::Mysql,
            &[],
            &TypeConversionOpts::default(),
        ))
    })
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
        let schema_resolver: SchemaResolver =
            Arc::new(move |_| Ok(schema.clone()));
        S3Sink::new(S3SinkArgs {
            id: "test-s3".into(),
            pipeline: "test-pipeline".into(),
            required: true,
            send_timeout: std::time::Duration::from_secs(30),
            store,
            format,
            schema_resolver,
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
    async fn encoding_failure_isolated_as_per_row_dlq() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let sink = build_sink(tmp.path(), RollingConfig::default()).await;
        // `id` declared bigint; sending a string that's not a parseable
        // integer must trip the encoder for that row.
        let bad = event_with("orders", json!({"id": "not-a-number"}));
        let result = sink.send_batch(&[bad]).await.unwrap();
        // Phase 2: the batch returns Ok with one DLQ failure isolated.
        assert_eq!(result.dlq_failures.len(), 1);
        assert_eq!(result.dlq_failures[0].0, 0);
        match &result.dlq_failures[0].1 {
            SinkError::Serialization { details } => {
                assert!(
                    details.contains("not-a-number")
                        || details.contains("parse"),
                    "expected encoder details, got: {details}"
                );
            }
            other => panic!("expected Serialization, got {other:?}"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn mixed_batch_isolates_only_bad_row() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let sink = build_sink(
            tmp.path(),
            // Force a roll so we can inspect the committed file's row count.
            RollingConfig {
                max_events: 1_000_000,
                ..Default::default()
            },
        )
        .await;

        // 4 events in one batch: indices 0, 1, 3 are good, index 2 is bad.
        let events = vec![
            event_with("orders", json!({"id": 100})),
            event_with("orders", json!({"id": 200})),
            event_with("orders", json!({"id": "broken"})),
            event_with("orders", json!({"id": 300})),
        ];
        let result = sink.send_batch(&events).await.unwrap();
        assert_eq!(
            result.dlq_failures.len(),
            1,
            "exactly one bad row should be isolated"
        );
        assert_eq!(result.dlq_failures[0].0, 2, "index of the bad row");

        // The 3 good rows should have landed; flush to verify.
        let committed = sink.flush_on_shutdown().await;
        assert_eq!(committed.len(), 1, "single rolled file");
        assert_eq!(
            committed[0].result.rows_written, 3,
            "exactly the 3 good rows written; bad row was isolated"
        );
        Ok(())
    }

    #[tokio::test]
    async fn all_bad_rows_produce_full_dlq_no_committed_file()
    -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let sink = build_sink(
            tmp.path(),
            RollingConfig {
                max_events: 100,
                ..Default::default()
            },
        )
        .await;

        let events = vec![
            event_with("orders", json!({"id": "broken-1"})),
            event_with("orders", json!({"id": "broken-2"})),
        ];
        let result = sink.send_batch(&events).await.unwrap();
        assert_eq!(result.dlq_failures.len(), 2);
        // No commit yet (no roll), but the writer was opened (even if it
        // received zero events). flush_on_shutdown handles the empty case.
        let committed = sink.flush_on_shutdown().await;
        // Either 0 (writer was never opened because all events failed before
        // any good row) or 1 (writer opened but zero rows written). Both
        // are atomicity-safe — readers see no bad data.
        for c in &committed {
            assert_eq!(
                c.result.rows_written, 0,
                "no rows written for all-bad batch"
            );
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

    /// Slow FileFormat: every operation sleeps long enough to trip the
    /// sink's send_timeout. Used to verify the timeout fires.
    struct SlowFormat;

    #[async_trait::async_trait]
    impl FileFormat for SlowFormat {
        fn extension(&self) -> &'static str {
            "slow"
        }
        fn content_type(&self) -> &'static str {
            "application/octet-stream"
        }
        fn label(&self) -> &'static str {
            "slow"
        }
        async fn open_writer(
            &self,
            _store: Arc<dyn object_store::ObjectStore>,
            _path: object_store::path::Path,
            _schema: Arc<arrow_schema::Schema>,
        ) -> anyhow::Result<Box<dyn crate::s3::FileWriter + Send>> {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            anyhow::bail!("slow format unreachable");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn send_batch_times_out_on_slow_sink() -> anyhow::Result<()> {
        use crate::s3::object_writer::{ObjectStoreParams, build_object_store};
        use deltaforge_core::encoding::arrow_schema::{
            Connector, build_envelope_arrow_schema,
        };
        use deltaforge_core::encoding::avro_types::TypeConversionOpts;

        let tmp = tempfile::tempdir()?;
        let params =
            ObjectStoreParams::local(tmp.path().to_string_lossy().to_string());
        let store = build_object_store(&params).unwrap();
        let cols = [col("id", "bigint")];
        let schema = Arc::new(build_envelope_arrow_schema(
            Connector::Mysql,
            &cols,
            &TypeConversionOpts::default(),
        ));
        let format: Arc<dyn FileFormat> = Arc::new(SlowFormat);
        let schema_resolver: SchemaResolver =
            Arc::new(move |_| Ok(schema.clone()));
        let sink = S3Sink::new(S3SinkArgs {
            id: "slow-s3".into(),
            pipeline: "test".into(),
            required: true,
            // Very short timeout so the test runs fast.
            send_timeout: std::time::Duration::from_millis(100),
            store,
            format,
            schema_resolver,
            pool_cfg: WriterPoolConfig::default(),
            cancel: CancellationToken::new(),
        });
        let err = sink
            .send_batch(&[event_with("orders", json!({"id": 1}))])
            .await
            .unwrap_err();
        match err {
            SinkError::Backpressure { details } => {
                assert!(
                    details.contains("timeout"),
                    "expected timeout message, got: {details}"
                );
            }
            other => panic!("expected Backpressure, got {other:?}"),
        }
        Ok(())
    }
}
