//! Crash-durable S3 sink (`durable_v2`).
//!
//! A batch is acknowledged only after its objects are content-addressed in S3
//! AND its manifest entry + HEAD compare-and-swap land. This sink is strongly
//! isolated from the legacy rolling sink: only `send_batch_with_context` is
//! valid, the delivery context's durable watermark is authoritative (never
//! inferred from events), and every prerequisite (context, watermark,
//! comparator, lineage) is checked before any object is uploaded.
//!
//! One epoch per sink: the [`DurableWriter`] is acquired once at construction
//! (after the conditional-write probe and verified recovery) and shared by every
//! clone through an `Arc`, so clone/retry paths never acquire a second epoch and
//! all publication serializes through the one writer's mutex.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_schema::Schema;
use async_trait::async_trait;
use deltaforge_core::{
    BatchResult, CheckpointComparator, Event, Sink, SinkBatchContext,
    SinkError, SinkResult,
};

use super::batch_upload::TableObject;
use super::durable_encode::{
    JSONL_ENCODER_VERSION, PARQUET_ENCODER_VERSION, encode_jsonl,
    encode_parquet,
};
use super::file_format::Compression;
use super::head::DurableWriter;
use super::keys::EncodingDomain;
use super::router::{PartitionKey, partition_for};
use super::store_cond::{
    ConditionalStore, ObjectStoreConditional, probe_conditional_writes,
};
use super::writer_pool::SchemaResolver;

/// Encoding format for durable objects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurableFormat {
    Parquet,
    Jsonl,
}

/// Partition-spec identity for durable v2. Currently only table partitioning;
/// carried explicitly in the encoding domain so a future scheme cannot be mixed.
const PARTITION_SPEC: &str = "table";
const PARTITION_VERSION: u16 = 1;

/// Stable codec label for the encoding domain (part of object identity).
fn compression_label(c: Compression) -> &'static str {
    match c {
        Compression::None => "none",
        Compression::Snappy => "snappy",
        Compression::Gzip => "gzip",
        Compression::Zstd => "zstd",
    }
}

/// Constructor inputs for the durable S3 sink.
pub struct DurableS3Args {
    pub id: String,
    pub pipeline: String,
    pub source_id: String,
    pub required: bool,
    /// Object-store root prefix for this pipeline's durable layout.
    pub prefix: String,
    pub store: Arc<dyn object_store::ObjectStore>,
    pub format: DurableFormat,
    pub compression: Compression,
    /// Resolves the Arrow schema for a table (Parquet only).
    pub schema_resolver: SchemaResolver,
    /// Source-aware watermark comparator (injected by the runner; carries no
    /// source dependency here).
    pub comparator: Arc<dyn CheckpointComparator>,
}

/// Shared, single-epoch state. Held behind an `Arc` so every clone of the sink
/// shares one `DurableWriter` (its verified state + publication mutex).
struct DurableInner {
    id: String,
    required: bool,
    format: DurableFormat,
    compression: Compression,
    schema_resolver: SchemaResolver,
    writer: DurableWriter<dyn ConditionalStore>,
}

/// The crash-durable S3 sink. Cheap to clone (shares one epoch/writer).
#[derive(Clone)]
pub struct DurableS3Sink {
    inner: Arc<DurableInner>,
}

impl DurableS3Sink {
    /// Build the sink: probe conditional-write capability, then acquire an epoch
    /// with verified recovery. BOTH complete before the sink can accept a batch;
    /// any failure is fatal (fail closed, never a silent legacy fallback).
    pub async fn create(args: DurableS3Args) -> SinkResult<Self> {
        // Durable objects are content-addressed; Gzip's bytes are not stable
        // across build configs, so it is not permitted here (see durable_encode).
        if args.format == DurableFormat::Parquet
            && args.compression == Compression::Gzip
        {
            return Err(fatal(
                "durable_v2 Parquet does not permit Gzip (non-reproducible \
                 bytes); use none, snappy or zstd",
            ));
        }

        let cond: Arc<dyn ConditionalStore> =
            Arc::new(ObjectStoreConditional::new(args.store));

        // 1. Capability probe: the store must honor create-only + CAS.
        probe_conditional_writes(cond.as_ref(), &args.prefix)
            .await
            .map_err(|e| {
                fatal(format!("conditional-write probe failed: {e}"))
            })?;

        // 2. Acquire the epoch with verified recovery of any existing HEAD/chain.
        let writer = DurableWriter::acquire(
            cond,
            &args.prefix,
            &args.pipeline,
            &args.source_id,
            &args.id,
            args.comparator,
        )
        .await
        .map_err(|e| fatal(format!("durable HEAD acquire/recovery: {e}")))?;

        // 0 = this sink acknowledges durably. Reported so the non-durable alerting
        // series (`deltaforge_sink_s3_non_durable_ack_mode`) has a healthy baseline
        // and durable mode never leaves the metric unset/stale at 1 after a rollback
        // and roll-forward.
        metrics::gauge!(
            "deltaforge_sink_s3_non_durable_ack_mode",
            "pipeline" => args.pipeline.clone(),
            "sink" => args.id.clone(),
        )
        .set(0.0);

        Ok(Self {
            inner: Arc::new(DurableInner {
                id: args.id,
                required: args.required,
                format: args.format,
                compression: args.compression,
                schema_resolver: args.schema_resolver,
                writer,
            }),
        })
    }

    /// Encode one table's events into a durable object.
    fn encode_table(
        &self,
        table: &str,
        first: &Event,
        events: &[&Event],
    ) -> SinkResult<TableObject> {
        let owned: Vec<Event> = events.iter().map(|e| (*e).clone()).collect();
        let compression = compression_label(self.inner.compression);
        match self.inner.format {
            DurableFormat::Jsonl => {
                let bytes = encode_jsonl(&owned).map_err(|e| {
                    fatal(format!("jsonl encode for {table}: {e}"))
                })?;
                Ok(TableObject {
                    table: table.to_string(),
                    bytes,
                    domain: EncodingDomain::new(
                        "jsonl",
                        JSONL_ENCODER_VERSION,
                        "jsonl",
                        compression,
                        PARTITION_SPEC,
                        PARTITION_VERSION,
                    ),
                    ext: "jsonl",
                })
            }
            DurableFormat::Parquet => {
                let pk = partition_for(first);
                let schema = self.resolve_schema(&pk)?;
                let bytes =
                    encode_parquet(&schema, &owned, self.inner.compression)
                        .map_err(|e| {
                            fatal(format!("parquet encode for {table}: {e}"))
                        })?;
                Ok(TableObject {
                    table: table.to_string(),
                    bytes,
                    domain: EncodingDomain::new(
                        "parquet",
                        PARQUET_ENCODER_VERSION,
                        schema_fingerprint(&schema),
                        compression,
                        PARTITION_SPEC,
                        PARTITION_VERSION,
                    ),
                    ext: "parquet",
                })
            }
        }
    }

    fn resolve_schema(&self, pk: &PartitionKey) -> SinkResult<Arc<Schema>> {
        (self.inner.schema_resolver)(pk).map_err(|e| {
            fatal(format!(
                "schema resolve for {}.{}: {e}",
                pk.namespace, pk.table
            ))
        })
    }
}

#[async_trait]
impl Sink for DurableS3Sink {
    fn id(&self) -> &str {
        &self.inner.id
    }

    fn required(&self) -> bool {
        self.inner.required
    }

    /// Not valid in durable_v2: a single event has no delivery context, so it
    /// cannot carry the authoritative watermark. Fail closed.
    async fn send(&self, _event: &Event) -> SinkResult<()> {
        Err(fatal(
            "durable_v2 requires send_batch_with_context; single-event send() \
             is not supported",
        ))
    }

    /// Not valid in durable_v2: the legacy batch path has no context, so no
    /// authoritative watermark. Fail closed.
    async fn send_batch(&self, _events: &[Event]) -> SinkResult<BatchResult> {
        Err(fatal(
            "durable_v2 requires send_batch_with_context; context-free \
             send_batch() is not supported",
        ))
    }

    /// The only valid delivery path. Requires the context's checkpoint and
    /// durable watermark; groups events by fully-qualified table identity;
    /// uploads content-addressed objects; and returns success ONLY after the
    /// manifest entry + HEAD CAS publish. An empty batch still publishes a
    /// deterministic zero-object manifest entry and advances HEAD (e.g. a
    /// snapshot completion boundary).
    async fn send_batch_with_context(
        &self,
        events: &[Event],
        ctx: &SinkBatchContext,
    ) -> SinkResult<BatchResult> {
        // Prerequisites are checked BEFORE uploading any object (fail closed).
        // The comparator + lineage are enforced inside publish (lineage lives in
        // the watermark; an unparseable/mismatched one is Incomparable = fatal).
        let watermark = ctx.durable_watermark.as_ref().ok_or_else(|| {
            fatal("durable_v2 batch is missing its durable watermark")
        })?;
        if ctx.checkpoint.as_bytes().is_empty() {
            return Err(fatal("durable_v2 batch is missing its checkpoint"));
        }

        // Group by fully-qualified identity (namespace/db/schema + table) so
        // equal table names in different databases/schemas never collide.
        let mut groups: BTreeMap<String, Vec<&Event>> = BTreeMap::new();
        for e in events {
            groups
                .entry(e.source.full_table_name())
                .or_default()
                .push(e);
        }

        let mut objects = Vec::with_capacity(groups.len());
        for (table, group) in &groups {
            let first = group[0];
            objects.push(self.encode_table(table, first, group)?);
        }

        // Publish: objects durable, then manifest entry + HEAD CAS. Success is
        // returned only after HEAD publication. A fatal error propagates to the
        // coordinator, which then does NOT advance the checkpoint.
        self.inner
            .writer
            .publish(watermark, objects, events.len() as u64)
            .await
            .map_err(|e| {
                if e.is_fatal() {
                    fatal(format!("durable publish failed: {e}"))
                } else {
                    SinkError::Backpressure {
                        details: format!("durable publish retryable: {e}")
                            .into(),
                    }
                }
            })?;

        Ok(BatchResult::ok())
    }
}

fn fatal(msg: impl Into<String>) -> SinkError {
    SinkError::Fatal {
        details: msg.into().into(),
    }
}

/// Build a durable S3 sink from config, injecting the source-aware `comparator`
/// (a core trait, so this carries no source dependency). The runner calls this
/// for a `durable_v2` S3 sink; it probes conditional writes and completes
/// verified recovery before returning, so the sink is ready to accept batches.
pub async fn build_durable_s3_sink(
    cfg: &deltaforge_config::S3SinkCfg,
    pipeline: &str,
    source_id: &str,
    comparator: Arc<dyn CheckpointComparator>,
    schema_resolver: Option<SchemaResolver>,
) -> anyhow::Result<DurableS3Sink> {
    use super::object_writer::{ObjectStoreParams, build_object_store};
    use deltaforge_config::{S3Compression as C, S3FileFormat as F};

    let expand = |v: &Option<String>| -> anyhow::Result<Option<String>> {
        Ok(v.as_deref()
            .map(shellexpand::env)
            .transpose()?
            .map(|s| s.into_owned()))
    };
    let params = ObjectStoreParams {
        bucket: cfg.bucket.clone(),
        endpoint: cfg.endpoint.clone(),
        region: cfg.region.clone(),
        access_key_id: expand(&cfg.access_key_id)?,
        secret_access_key: expand(&cfg.secret_access_key)?,
        virtual_hosted_style: cfg.virtual_hosted_style,
        local: cfg.local,
    };
    let store = build_object_store(&params)
        .map_err(|e| anyhow::anyhow!("build S3 object store: {e}"))?;

    let compression = match cfg.compression {
        C::None => Compression::None,
        C::Snappy => Compression::Snappy,
        C::Gzip => Compression::Gzip,
        C::Zstd => Compression::Zstd,
    };
    let format = match cfg.format {
        F::Parquet => DurableFormat::Parquet,
        F::Jsonl => DurableFormat::Jsonl,
    };
    let schema_resolver =
        schema_resolver.unwrap_or_else(super::sink::fallback_envelope_resolver);

    DurableS3Sink::create(DurableS3Args {
        id: cfg.id.clone(),
        pipeline: pipeline.to_string(),
        source_id: source_id.to_string(),
        required: cfg.required.unwrap_or(true),
        prefix: cfg.prefix.clone(),
        store,
        format,
        compression,
        schema_resolver,
        comparator,
    })
    .await
    .map_err(|e| anyhow::anyhow!("build durable S3 sink '{}': {e}", cfg.id))
}

/// Stable schema identity for the encoding domain: a hash of the field
/// names + Arrow data types, so two different schemas content-address to
/// different keys and recovery can tell encodings apart.
fn schema_fingerprint(schema: &Schema) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    for f in schema.fields() {
        h.update(f.name().as_bytes());
        h.update([0u8]);
        h.update(format!("{:?}", f.data_type()).as_bytes());
        h.update([0u8]);
    }
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltaforge_core::{CheckpointMeta, CheckpointOrder, Op, SourceInfo};
    use futures::StreamExt;
    use object_store::memory::InMemory;
    use object_store::{ObjectStore, ObjectStoreExt};

    /// Test comparator: watermarks are ASCII decimal; unparseable = Incomparable
    /// (stands in for a lineage/generation mismatch).
    struct NumCmp;
    impl CheckpointComparator for NumCmp {
        fn order(&self, proposed: &[u8], reference: &[u8]) -> CheckpointOrder {
            let parse = |b: &[u8]| {
                std::str::from_utf8(b)
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
            };
            match (parse(proposed), parse(reference)) {
                (Some(p), Some(r)) => match p.cmp(&r) {
                    std::cmp::Ordering::Less => CheckpointOrder::Before,
                    std::cmp::Ordering::Equal => CheckpointOrder::Equal,
                    std::cmp::Ordering::Greater => CheckpointOrder::After,
                },
                _ => CheckpointOrder::Incomparable,
            }
        }
    }

    fn dummy_resolver() -> SchemaResolver {
        // Jsonl needs no schema; this is never called in these tests.
        Arc::new(|_pk: &PartitionKey| {
            Ok(Arc::new(Schema::new(Vec::<arrow_schema::Field>::new())))
        })
    }

    async fn sink_over(
        store: Arc<dyn object_store::ObjectStore>,
        id: &str,
    ) -> SinkResult<DurableS3Sink> {
        DurableS3Sink::create(DurableS3Args {
            id: id.to_string(),
            pipeline: "pipe".into(),
            source_id: "src".into(),
            required: true,
            prefix: "root".into(),
            store,
            format: DurableFormat::Jsonl,
            compression: Compression::None,
            schema_resolver: dummy_resolver(),
            comparator: Arc::new(NumCmp),
        })
        .await
    }

    fn row(schema: Option<&str>, db: &str, table: &str, id: i64) -> Event {
        let source = SourceInfo {
            version: "1".into(),
            connector: "mysql".into(),
            name: "pipe".into(),
            ts_ms: 0,
            db: db.into(),
            schema: schema.map(|s| s.to_string()),
            table: table.into(),
            snapshot: Some("true".into()),
            position: Default::default(),
        };
        Event::new_row(
            deltaforge_core::EventId::mysql_row_server(1, table, 1, id as u32),
            source,
            Op::Read,
            None,
            Some(serde_json::json!({ "id": id })),
            0,
            0,
        )
    }

    fn ctx(watermark: &str) -> SinkBatchContext {
        SinkBatchContext {
            checkpoint: CheckpointMeta::from_vec(b"cp".to_vec()),
            durable_watermark: Some(watermark.as_bytes().to_vec()),
            batch_id: None,
        }
    }

    async fn keys(store: &Arc<dyn ObjectStore>) -> Vec<String> {
        let mut out: Vec<String> = store
            .list(None)
            .filter_map(
                |r| async move { r.ok().map(|m| m.location.to_string()) },
            )
            .collect()
            .await;
        out.sort();
        out
    }

    #[tokio::test]
    async fn send_and_send_batch_are_fatal_only_context_is_valid() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let sink = sink_over(store, "s").await.unwrap();
        assert!(matches!(
            sink.send(&row(None, "db", "t", 1)).await,
            Err(SinkError::Fatal { .. })
        ));
        assert!(matches!(
            sink.send_batch(&[row(None, "db", "t", 1)]).await,
            Err(SinkError::Fatal { .. })
        ));
    }

    #[tokio::test]
    async fn missing_watermark_or_checkpoint_is_fatal() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let sink = sink_over(store, "s").await.unwrap();
        // Missing watermark.
        let no_wm = SinkBatchContext {
            checkpoint: CheckpointMeta::from_vec(b"cp".to_vec()),
            durable_watermark: None,
            batch_id: None,
        };
        assert!(matches!(
            sink.send_batch_with_context(&[], &no_wm).await,
            Err(SinkError::Fatal { .. })
        ));
        // Missing (empty) checkpoint.
        let no_cp = SinkBatchContext {
            checkpoint: CheckpointMeta::from_vec(Vec::new()),
            durable_watermark: Some(b"1".to_vec()),
            batch_id: None,
        };
        assert!(matches!(
            sink.send_batch_with_context(&[], &no_cp).await,
            Err(SinkError::Fatal { .. })
        ));
        // Nothing was uploaded on the fail-closed paths.
        let store2 = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let s2 = sink_over(Arc::clone(&store2), "s2").await.unwrap();
        let _ = s2.send_batch_with_context(&[], &no_wm).await;
        let ks = keys(&store2).await;
        assert!(
            !ks.iter().any(|k| k.contains("_manifest/entries")),
            "no manifest entry written on missing-watermark: {ks:?}"
        );
    }

    #[tokio::test]
    async fn zero_object_completion_writes_entry_and_advances_head() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let sink = sink_over(Arc::clone(&store), "s").await.unwrap();
        // Empty batch + a completion watermark: deterministic zero-object entry.
        sink.send_batch_with_context(&[], &ctx("100"))
            .await
            .unwrap();
        let ks = keys(&store).await;
        assert!(
            ks.iter().any(|k| k.contains("_manifest/entries")),
            "zero-object manifest entry exists: {ks:?}"
        );
        assert!(
            ks.iter().any(|k| k.contains("_manifest/HEAD")),
            "HEAD exists: {ks:?}"
        );
        // No data objects for an empty batch.
        assert!(
            !ks.iter().any(|k| k.contains("/data/")),
            "no data objects for a zero-object batch: {ks:?}"
        );
    }

    #[tokio::test]
    async fn multi_table_grouping_and_duplicate_names_across_schemas() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let sink = sink_over(Arc::clone(&store), "s").await.unwrap();
        // Two schemas each with an "orders" table plus a distinct "users":
        // grouped by fully-qualified identity, so a.orders and b.orders never
        // collide.
        let batch = vec![
            row(Some("a"), "a", "orders", 1),
            row(Some("a"), "a", "orders", 2),
            row(Some("b"), "b", "orders", 3),
            row(Some("a"), "a", "users", 4),
        ];
        sink.send_batch_with_context(&batch, &ctx("100"))
            .await
            .unwrap();
        let ks = keys(&store).await;
        // Data objects are the encoded table files (".jsonl"); manifest/HEAD are
        // under "_manifest".
        let data: Vec<&String> =
            ks.iter().filter(|k| k.ends_with(".jsonl")).collect();
        // Three distinct fully-qualified tables -> three data objects.
        assert_eq!(data.len(), 3, "one object per fq table: {ks:?}");
        assert!(data.iter().any(|k| k.contains("a.orders")), "{data:?}");
        assert!(data.iter().any(|k| k.contains("b.orders")), "{data:?}");
        assert!(data.iter().any(|k| k.contains("a.users")), "{data:?}");
    }

    #[tokio::test]
    async fn recovery_before_first_send_advances_epoch() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        // First run: genesis + one publish.
        {
            let s1 = sink_over(Arc::clone(&store), "s").await.unwrap();
            s1.send_batch_with_context(&[row(None, "db", "t", 1)], &ctx("100"))
                .await
                .unwrap();
        }
        // Second run over the same store: acquire must verify the existing HEAD
        // and chain and advance the epoch BEFORE accepting a batch. A later
        // watermark then publishes on top of the recovered chain.
        let s2 = sink_over(Arc::clone(&store), "s").await.unwrap();
        s2.send_batch_with_context(&[row(None, "db", "t", 2)], &ctx("200"))
            .await
            .unwrap();
        // An already-durable replay (Before HEAD) is acknowledged without error.
        s2.send_batch_with_context(&[row(None, "db", "t", 1)], &ctx("100"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn clones_share_one_epoch_and_serialize_publication() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let sink = sink_over(Arc::clone(&store), "s").await.unwrap();
        let a = sink.clone();
        let b = sink.clone();
        // Concurrent publishes from two clones. If they held separate epochs one
        // would fence the other (Fenced error); sharing one writer, both succeed
        // and publication serializes through the shared mutex.
        let batch_a = [row(None, "db", "t", 1)];
        let batch_b = [row(None, "db", "t", 2)];
        let ctx_a = ctx("100");
        let ctx_b = ctx("200");
        let (ra, rb) = tokio::join!(
            a.send_batch_with_context(&batch_a, &ctx_a),
            b.send_batch_with_context(&batch_b, &ctx_b),
        );
        assert!(ra.is_ok(), "clone A: {ra:?}");
        assert!(rb.is_ok(), "clone B: {rb:?}");
    }

    async fn manifest_event_counts(store: &Arc<dyn ObjectStore>) -> Vec<u64> {
        let mut out = Vec::new();
        for k in keys(store).await {
            if k.contains("_manifest/entries") {
                let path = object_store::path::Path::from(k);
                let bytes = store
                    .as_ref()
                    .get(&path)
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap();
                let v: serde_json::Value =
                    serde_json::from_slice(&bytes).unwrap();
                out.push(v["event_count"].as_u64().unwrap());
            }
        }
        out
    }

    #[tokio::test]
    async fn filtered_durable_sink_manifest_event_count_matches_survivors() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let sink: deltaforge_core::ArcDynSink =
            Arc::new(sink_over(Arc::clone(&store), "s").await.unwrap());
        // Wrap the durable sink in a filter that drops synthetic events, exactly
        // as the runner does for a configured cfg.filter.
        let filter = deltaforge_config::SinkFilter {
            exclude_synthetic: true,
            ..Default::default()
        };
        let wrapped = crate::FilteredSink::wrap(sink, filter);

        // Partial: 2 source rows survive, 1 synthetic filtered -> event_count 2.
        let batch = vec![
            row(None, "db", "t", 1),
            row(None, "db", "t", 2).mark_synthetic("proc"),
            row(None, "db", "t", 3),
        ];
        wrapped
            .send_batch_with_context(&batch, &ctx("100"))
            .await
            .unwrap();
        assert_eq!(manifest_event_counts(&store).await, vec![2]);

        // Fully filtered: still forwards, publishing a zero-object entry (HEAD
        // advances). A later watermark proves HEAD moved (After 100).
        let all_synth = vec![row(None, "db", "t", 4).mark_synthetic("proc")];
        wrapped
            .send_batch_with_context(&all_synth, &ctx("200"))
            .await
            .unwrap();
        let counts = manifest_event_counts(&store).await;
        assert_eq!(
            counts,
            vec![2, 0],
            "partial=2 then fully-filtered=0 entries"
        );
    }

    #[tokio::test]
    async fn incomparable_publication_failure_is_fatal() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let sink = sink_over(Arc::clone(&store), "s").await.unwrap();
        sink.send_batch_with_context(&[row(None, "db", "t", 1)], &ctx("100"))
            .await
            .unwrap();
        // A watermark the comparator cannot order against HEAD (stand-in for a
        // lineage mismatch) is fatal - the coordinator then does not advance.
        let bad = SinkBatchContext {
            checkpoint: CheckpointMeta::from_vec(b"cp".to_vec()),
            durable_watermark: Some(b"not-a-number".to_vec()),
            batch_id: None,
        };
        assert!(matches!(
            sink.send_batch_with_context(&[row(None, "db", "t", 2)], &bad)
                .await,
            Err(SinkError::Fatal { .. })
        ));
        // A later valid replay Before HEAD still acks (HEAD was not advanced by
        // the failed publish).
        sink.send_batch_with_context(&[row(None, "db", "t", 1)], &ctx("50"))
            .await
            .unwrap();
    }

    // Durable mode MUST report the non-durable-ack metric as 0 (a healthy
    // baseline for the alerting series), never leaving it unset or stale at 1.
    #[test]
    fn durable_mode_reports_non_durable_ack_metric_zero() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        let recorder = DebuggingRecorder::new();
        let snap = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
                sink_over(store, "durable-s3")
                    .await
                    .expect("durable sink creates");
            });
        });

        let value =
            snap.snapshot()
                .into_vec()
                .into_iter()
                .find_map(|(ck, _, _, v)| match v {
                    DebugValue::Gauge(g)
                        if ck.key().name()
                            == "deltaforge_sink_s3_non_durable_ack_mode" =>
                    {
                        Some(g.into_inner())
                    }
                    _ => None,
                });
        assert_eq!(
            value,
            Some(0.0),
            "durable mode must report non-durable ack mode = 0"
        );
    }
}
