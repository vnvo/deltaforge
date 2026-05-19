//! Multi-partition writer pool.
//!
//! Holds one in-progress `FileWriter` per active `PartitionKey`. On each
//! batch:
//! 1. Group events by partition.
//! 2. Append each group to its writer (creating one on first event).
//! 3. Sweep all writers: close + upload any that crossed a rolling threshold.
//! 4. Idle sweep: close writers whose `last_event_at` is older than `idle_age`.
//!
//! Concurrency: per-partition writers are independent, so close+upload runs
//! in parallel across partitions via `join_all`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use arrow_schema::Schema;
use deltaforge_core::Event;
use futures::future::join_all;
use object_store::{ObjectStore, path::Path};
use tracing::{debug, warn};
use ulid::Ulid;

use super::file_format::{FileFormat, FileWriter, WriteResult};
use super::partition_for;
use super::rolling::{RollReason, RollingConfig, should_roll};
use super::router::PartitionKey;

/// A single closed file ready to be reported to metrics / coordinator.
#[derive(Debug, Clone)]
pub struct CommittedFile {
    pub partition: PartitionKey,
    pub path: String,
    pub result: WriteResult,
    pub reason: RollReason,
}

/// One row in the pool — an open writer plus its lifecycle metadata.
struct ActiveWriter {
    writer: Box<dyn FileWriter + Send>,
    path: Path,
    opened_at: Instant,
    last_event_at: Instant,
}

/// Pool configuration. `prefix` is prepended to the Hive partition path:
/// `{prefix}/table=X/year=Y/month=MM/day=DD/<ulid>.<ext>`.
#[derive(Debug, Clone, Default)]
pub struct WriterPoolConfig {
    pub prefix: String,
    pub rolling: RollingConfig,
}

/// Multi-partition writer pool. Single-owner: not `Sync`. Wrap in a `Mutex`
/// or own from a single task if shared.
pub struct WriterPool {
    store: Arc<dyn ObjectStore>,
    format: Arc<dyn FileFormat>,
    schema: Arc<Schema>,
    cfg: WriterPoolConfig,
    writers: HashMap<PartitionKey, ActiveWriter>,
}

impl WriterPool {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        format: Arc<dyn FileFormat>,
        schema: Arc<Schema>,
        cfg: WriterPoolConfig,
    ) -> Self {
        Self {
            store,
            format,
            schema,
            cfg,
            writers: HashMap::new(),
        }
    }

    /// Append a batch of events.
    ///
    /// Returns the list of files that were committed during this call
    /// (because they crossed a rolling threshold). The pool retains the
    /// remaining open writers for subsequent batches.
    pub async fn append_batch(
        &mut self,
        events: &[Event],
    ) -> Result<Vec<CommittedFile>> {
        if events.is_empty() {
            return Ok(self.idle_sweep().await);
        }

        // 1. Group events by partition key. The number of distinct
        //    partitions per batch is small (typically 1-3), so an index
        //    vector is cheaper than a HashMap.
        let mut groups: Vec<(PartitionKey, Vec<&Event>)> = Vec::new();
        for e in events {
            let key = partition_for(e);
            match groups.iter_mut().find(|(k, _)| *k == key) {
                Some((_, vec)) => vec.push(e),
                None => groups.push((key, vec![e])),
            }
        }

        // 2. Append each group, creating writers on demand.
        let now = Instant::now();
        for (key, group) in groups {
            // Borrow events as a slice. We collected &Event so reborrow.
            let owned: Vec<Event> = group.into_iter().cloned().collect();
            let writer = self.writer_for(&key, now).await?;
            writer.writer.append(&owned).await.with_context(|| {
                format!("append to writer for {}", key.hive_path())
            })?;
            writer.last_event_at = now;
        }

        // 3. Roll any writers that crossed a threshold.
        self.roll_threshold_writers(now).await
    }

    /// Close any writer that has been idle longer than `idle_age` without
    /// receiving new events. Called at the start of every `append_batch`
    /// when the batch is empty, and indirectly via `roll_threshold_writers`
    /// otherwise.
    pub async fn idle_sweep(&mut self) -> Vec<CommittedFile> {
        let now = Instant::now();
        let to_close = self.collect_rollable(now);
        self.close_many(to_close).await
    }

    /// Close all open writers and return the committed files. Call this on
    /// **graceful** pipeline shutdown to flush in-flight partitions.
    pub async fn close_all(&mut self) -> Vec<CommittedFile> {
        let keys: Vec<_> = self.writers.keys().cloned().collect();
        let pairs: Vec<_> = keys
            .into_iter()
            .map(|k| (k, RollReason::Age /* shutdown */))
            .collect();
        self.close_many(pairs).await
    }

    /// Abandon all open writers without finalizing their files.
    ///
    /// Use this on **forced** shutdown (cancellation token fired, panic
    /// during batch, source replay needed) when you do not want partial
    /// data to land. Returns the number of writers that were abandoned.
    ///
    /// **Atomicity guarantee:** abandoned writers never produce a visible
    /// file at the object store. For S3-compatible backends, dropping a
    /// `BufWriter` mid-multipart leaves orphan parts that the bucket's
    /// lifecycle policy is expected to expire (see
    /// `docs/specs/s3-parquet-sink.md` for the recommended policy).
    pub fn abandon_all(&mut self) -> usize {
        let n = self.writers.len();
        // Just drop everything. `Box<dyn FileWriter>::drop` cascades to the
        // underlying AsyncArrowWriter and BufWriter; neither calls
        // shutdown/abort on drop, so the multipart is left abandoned. No
        // partial file appears at the target path.
        self.writers.clear();
        n
    }

    /// Number of currently open writers (one per active partition).
    pub fn open_writer_count(&self) -> usize {
        self.writers.len()
    }

    // -----------------------------------------------------------------------
    // Internals
    // -----------------------------------------------------------------------

    async fn writer_for(
        &mut self,
        key: &PartitionKey,
        _now: Instant,
    ) -> Result<&mut ActiveWriter> {
        if !self.writers.contains_key(key) {
            let writer = self.open_new_writer(key).await?;
            self.writers.insert(key.clone(), writer);
            debug!(
                partition = key.hive_path(),
                "opened new writer for partition"
            );
        }
        Ok(self.writers.get_mut(key).unwrap())
    }

    async fn open_new_writer(
        &mut self,
        key: &PartitionKey,
    ) -> Result<ActiveWriter> {
        // Pull the pieces we need out of `self` before the await so the
        // future doesn't capture `&Self` (which would require WriterPool: Sync).
        let ulid = Ulid::new().to_string();
        let ext = self.format.extension();
        let prefix = if self.cfg.prefix.is_empty() {
            String::new()
        } else {
            format!("{}/", self.cfg.prefix.trim_end_matches('/'))
        };
        let path_str = format!(
            "{prefix}{partition}/{ulid}.{ext}",
            partition = key.hive_path()
        );
        let path = Path::from(path_str);

        let store = self.store.clone();
        let schema = self.schema.clone();
        let format = self.format.clone();
        let hive_path = key.hive_path();
        let writer = format
            .open_writer(store, path.clone(), schema)
            .await
            .with_context(|| {
                format!("open writer for partition {hive_path}")
            })?;

        let opened_at = Instant::now();
        Ok(ActiveWriter {
            writer,
            path,
            opened_at,
            last_event_at: opened_at,
        })
    }

    fn collect_rollable(
        &self,
        now: Instant,
    ) -> Vec<(PartitionKey, RollReason)> {
        self.writers
            .iter()
            .filter_map(|(key, w)| {
                should_roll(
                    &self.cfg.rolling,
                    w.writer.bytes_written(),
                    w.writer.events_written(),
                    w.opened_at,
                    w.last_event_at,
                    now,
                )
                .map(|r| (key.clone(), r))
            })
            .collect()
    }

    async fn roll_threshold_writers(
        &mut self,
        now: Instant,
    ) -> Result<Vec<CommittedFile>> {
        let to_close = self.collect_rollable(now);
        Ok(self.close_many(to_close).await)
    }

    async fn close_many(
        &mut self,
        pairs: Vec<(PartitionKey, RollReason)>,
    ) -> Vec<CommittedFile> {
        if pairs.is_empty() {
            return Vec::new();
        }

        // Take ownership of each writer's state so we can close concurrently.
        let mut tasks = Vec::with_capacity(pairs.len());
        for (key, reason) in pairs {
            if let Some(active) = self.writers.remove(&key) {
                tasks.push(close_one(key, active, reason));
            }
        }

        let mut committed = Vec::new();
        for res in join_all(tasks).await {
            match res {
                Ok(c) => committed.push(c),
                Err((key, e)) => {
                    warn!(partition = key.hive_path(), error = %e,
                          "failed to close writer; file abandoned");
                }
            }
        }
        committed
    }
}

async fn close_one(
    key: PartitionKey,
    active: ActiveWriter,
    reason: RollReason,
) -> Result<CommittedFile, (PartitionKey, anyhow::Error)> {
    let ActiveWriter { writer, path, .. } = active;
    let path_str = path.to_string();
    match writer.close().await {
        Ok(result) => Ok(CommittedFile {
            partition: key,
            path: path_str,
            result,
            reason,
        }),
        Err(e) => Err((key, e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::file_format::Compression;
    use crate::s3::object_writer::{ObjectStoreParams, build_object_store};
    use crate::s3::parquet_writer::ParquetFormat;
    use deltaforge_core::encoding::arrow_schema::{
        Connector, build_envelope_arrow_schema,
    };
    use deltaforge_core::encoding::avro_types::{
        ColumnDesc, TypeConversionOpts,
    };
    use deltaforge_core::{Event, Op, SourceInfo, SourcePosition};
    use serde_json::json;
    use std::time::Duration;

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

    fn build_schema(cols: &[ColumnDesc]) -> Arc<Schema> {
        Arc::new(build_envelope_arrow_schema(
            Connector::Mysql,
            cols,
            &TypeConversionOpts::default(),
        ))
    }

    fn make_event(table: &str, day: u32, id: i64) -> Event {
        let ts_ms = chrono::NaiveDate::from_ymd_opt(2026, 5, day)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_millis();
        Event {
            before: None,
            after: Some(json!({"id": id})),
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

    async fn make_pool(
        tmp: &std::path::Path,
        cols: &[ColumnDesc],
        cfg: WriterPoolConfig,
    ) -> WriterPool {
        let params =
            ObjectStoreParams::local(tmp.to_string_lossy().to_string());
        let store = build_object_store(&params).unwrap();
        let format: Arc<dyn FileFormat> =
            Arc::new(ParquetFormat::new(Compression::Snappy));
        let schema = build_schema(cols);
        WriterPool::new(store, format, schema, cfg)
    }

    #[tokio::test]
    async fn empty_batch_returns_no_files() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let mut pool = make_pool(
            tmp.path(),
            &[col("id", "bigint")],
            WriterPoolConfig::default(),
        )
        .await;
        let committed = pool.append_batch(&[]).await?;
        assert!(committed.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn distinct_partitions_open_distinct_writers() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let mut pool = make_pool(
            tmp.path(),
            &[col("id", "bigint")],
            WriterPoolConfig::default(),
        )
        .await;
        let events = vec![
            make_event("orders", 10, 1),
            make_event("orders", 11, 2),
            make_event("customers", 10, 3),
        ];
        let committed = pool.append_batch(&events).await?;
        assert!(committed.is_empty(), "no rolling threshold hit yet");
        assert_eq!(pool.open_writer_count(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn roll_on_event_count_closes_and_commits_file() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let cfg = WriterPoolConfig {
            prefix: "lake".into(),
            rolling: RollingConfig {
                max_events: 5,
                ..Default::default()
            },
        };
        let mut pool = make_pool(tmp.path(), &[col("id", "bigint")], cfg).await;
        let events: Vec<_> =
            (0..5).map(|i| make_event("orders", 10, i as i64)).collect();
        let committed = pool.append_batch(&events).await?;
        assert_eq!(committed.len(), 1, "should roll on event count");
        assert_eq!(committed[0].reason, RollReason::Events);
        assert_eq!(committed[0].partition.table, "orders");
        assert!(committed[0].path.starts_with("lake/table=orders/"));
        assert!(committed[0].path.ends_with(".parquet"));
        assert_eq!(committed[0].result.rows_written, 5);
        assert_eq!(pool.open_writer_count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn idle_sweep_closes_old_writers() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let cfg = WriterPoolConfig {
            prefix: String::new(),
            rolling: RollingConfig {
                idle_age: Duration::from_millis(50),
                ..Default::default()
            },
        };
        let mut pool = make_pool(tmp.path(), &[col("id", "bigint")], cfg).await;
        let events = vec![make_event("orders", 10, 1)];
        pool.append_batch(&events).await?;
        assert_eq!(pool.open_writer_count(), 1);
        // Sleep past idle threshold then call append_batch with no events,
        // which triggers an idle sweep.
        tokio::time::sleep(Duration::from_millis(120)).await;
        let committed = pool.append_batch(&[]).await?;
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].reason, RollReason::Idle);
        assert_eq!(pool.open_writer_count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn close_all_flushes_remaining_writers() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let mut pool = make_pool(
            tmp.path(),
            &[col("id", "bigint")],
            WriterPoolConfig::default(),
        )
        .await;
        let events =
            vec![make_event("orders", 10, 1), make_event("customers", 10, 2)];
        pool.append_batch(&events).await?;
        assert_eq!(pool.open_writer_count(), 2);
        let committed = pool.close_all().await;
        assert_eq!(committed.len(), 2);
        assert_eq!(pool.open_writer_count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn abandon_all_produces_no_visible_file() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let mut pool = make_pool(
            tmp.path(),
            &[col("id", "bigint")],
            WriterPoolConfig::default(),
        )
        .await;
        let events: Vec<_> = (0..1000)
            .map(|i| make_event("orders", 10, i as i64))
            .collect();
        pool.append_batch(&events).await?;
        assert_eq!(pool.open_writer_count(), 1);

        // Force-abandon — no close call.
        let abandoned = pool.abandon_all();
        assert_eq!(abandoned, 1);
        assert_eq!(pool.open_writer_count(), 0);

        // No file should be visible at any path under the prefix.
        let params =
            ObjectStoreParams::local(tmp.path().to_string_lossy().to_string());
        let store = build_object_store(&params)?;
        let mut listed: Vec<_> =
            futures::TryStreamExt::try_collect::<Vec<_>>(store.list(None))
                .await?;
        listed.sort_by(|a, b| a.location.cmp(&b.location));
        assert!(
            listed.is_empty(),
            "abandoned writer must not produce a visible file; got {} files",
            listed.len()
        );
        Ok(())
    }

    #[tokio::test]
    async fn drop_pool_without_close_produces_no_visible_file() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        {
            let mut pool = make_pool(
                tmp.path(),
                &[col("id", "bigint")],
                WriterPoolConfig::default(),
            )
            .await;
            let events: Vec<_> = (0..1000)
                .map(|i| make_event("orders", 10, i as i64))
                .collect();
            pool.append_batch(&events).await?;
            assert_eq!(pool.open_writer_count(), 1);
            // Pool drops here without close_all / abandon_all.
        }

        let params =
            ObjectStoreParams::local(tmp.path().to_string_lossy().to_string());
        let store = build_object_store(&params)?;
        let listed: Vec<_> =
            futures::TryStreamExt::try_collect::<Vec<_>>(store.list(None))
                .await?;
        // Local FS may leave a zero-byte file as multipart upload stub.
        // Either there is no file at all or any file is empty. The
        // atomicity guarantee is that no readable Parquet content lands.
        for entry in &listed {
            assert!(
                entry.size == 0,
                "dropped writer produced a non-empty file: {} ({} bytes)",
                entry.location,
                entry.size
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn partition_path_is_hive_style_with_ulid() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let cfg = WriterPoolConfig {
            prefix: "deltaforge/orders".into(),
            rolling: RollingConfig {
                max_events: 1,
                ..Default::default()
            },
        };
        let mut pool = make_pool(tmp.path(), &[col("id", "bigint")], cfg).await;
        let events = vec![make_event("orders", 15, 1)];
        let committed = pool.append_batch(&events).await?;
        assert_eq!(committed.len(), 1);
        let path = &committed[0].path;
        // expected: deltaforge/orders/table=orders/year=2026/month=05/day=15/<ULID>.parquet
        assert!(path.starts_with(
            "deltaforge/orders/table=orders/year=2026/month=05/day=15/"
        ));
        assert!(path.ends_with(".parquet"));
        // ULID is 26 chars, validate the basename length is right.
        let ulid_part = path
            .rsplit('/')
            .next()
            .unwrap()
            .trim_end_matches(".parquet");
        assert_eq!(ulid_part.len(), 26);
        Ok(())
    }
}
