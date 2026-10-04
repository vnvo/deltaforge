//! MiniStack canary for the default S3 path, `durable_v2`.
//!
//! The legacy canary (`s3_ministack_canary.rs`) covers the rolling sink; this
//! one covers what an unconfigured S3 sink actually runs: content-addressed
//! data objects, a manifest entry and a conditional (CAS) HEAD publication as
//! the acknowledgement boundary, against an AWS emulator rather than MinIO.
//! The sink is built through the production builder from a configuration that
//! leaves `durability` at its default.
//!
//! Run with: `cargo test -p runner --test s3_ministack_durable_canary -- --ignored`

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Result;
use async_trait::async_trait;
use deltaforge_config::{
    S3Compression, S3Durability, S3FileFormat, S3FileRoll, S3SinkCfg,
};
use deltaforge_core::incident::ReasonCode;
use deltaforge_core::{
    BoundaryOutcome, CheckpointMeta, Event, Op, Sink, SinkBatchContext,
    SourceInfo, SourcePosition,
};
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, ObjectStoreExt, PutMode, PutMultipartOptions,
    PutOptions, PutPayload, PutResult,
};
use serde_json::{Value, json};
use sinks::s3::{
    Compression, DurableFormat, DurableS3Args, DurableS3Sink,
    ObjectStoreParams, build_durable_s3_sink, build_object_store,
};
use sources::durable_checkpoint::{
    DurableWatermark, SourceCheckpointComparator,
};

mod ministack;
use ministack::{BUCKET, MS_KEY, MS_SECRET, ministack};

const PIPELINE: &str = "canary";
const SOURCE_ID: &str = "src";

// =============================================================================
// Fixtures
// =============================================================================

/// The configuration an operator writes for an S3 sink: `durability` left at
/// its default.
fn sink_cfg(prefix: &str, endpoint: &str) -> S3SinkCfg {
    let cfg = S3SinkCfg {
        id: "durable-canary".into(),
        bucket: BUCKET.into(),
        prefix: prefix.into(),
        region: Some("us-east-1".into()),
        endpoint: Some(endpoint.into()),
        access_key_id: Some(MS_KEY.into()),
        secret_access_key: Some(MS_SECRET.into()),
        session_token: None,
        access_key_id_ref: None,
        secret_access_key_ref: None,
        session_token_ref: None,
        virtual_hosted_style: false,
        local: false,
        format: S3FileFormat::Jsonl,
        compression: S3Compression::None,
        file_roll: S3FileRoll::default(),
        send_timeout_secs: 60,
        required: Some(true),
        durability: Default::default(),
        filter: None,
    };
    assert_eq!(cfg.durability, S3Durability::DurableV2, "the default path");
    cfg
}

/// The production sink: the builder the pipeline manager uses, with the
/// source-aware checkpoint comparator.
async fn production_sink(cfg: &S3SinkCfg) -> Result<DurableS3Sink> {
    build_durable_s3_sink(
        cfg,
        PIPELINE,
        SOURCE_ID,
        Arc::new(SourceCheckpointComparator),
        None,
        &sinks::ResolvedSinkCreds::default(),
    )
    .await
}

/// A PostgreSQL commit watermark at `lsn` (a real source watermark the
/// production comparator orders).
fn ctx(lsn: u64) -> SinkBatchContext {
    let wm = DurableWatermark::pg_commit(42, lsn, None).to_bytes();
    SinkBatchContext {
        checkpoint: CheckpointMeta::from_vec(wm.clone()),
        durable_watermark: Some(wm),
        batch_id: None,
    }
}

fn row(id: u64) -> Event {
    Event::new_row(
        deltaforge_core::EventId::mysql_row_server(1, "t", id, 0),
        SourceInfo {
            version: "deltaforge-canary".into(),
            connector: "postgresql".into(),
            name: "canary-db".into(),
            ts_ms: 1_700_000_000_000,
            db: "shop".into(),
            schema: Some("public".into()),
            table: "orders".into(),
            snapshot: None,
            position: SourcePosition::default(),
        },
        Op::Create,
        None,
        Some(json!({"id": id, "marker": format!("canary-row-{id}")})),
        1_700_000_000_000,
        64,
    )
}

fn raw_store(endpoint: &str) -> Result<Arc<dyn ObjectStore>> {
    build_object_store(&ObjectStoreParams::s3_minio(
        BUCKET, endpoint, MS_KEY, MS_SECRET,
    ))
}

fn head_key(prefix: &str) -> Path {
    Path::from(format!("{prefix}/{PIPELINE}/_manifest/HEAD"))
}

async fn read_json(store: &Arc<dyn ObjectStore>, key: &str) -> Result<Value> {
    let bytes = store.get(&Path::from(key)).await?.bytes().await?;
    Ok(serde_json::from_slice(&bytes)?)
}

async fn read_text(store: &Arc<dyn ObjectStore>, key: &str) -> Result<String> {
    let bytes = store.get(&Path::from(key)).await?.bytes().await?;
    Ok(String::from_utf8(bytes.to_vec())?)
}

/// Manifest entries written under the pipeline (referenced or not).
async fn entry_count(store: &Arc<dyn ObjectStore>, prefix: &str) -> usize {
    futures::TryStreamExt::try_collect::<Vec<_>>(store.list(Some(&Path::from(
        format!("{prefix}/{PIPELINE}/_manifest/entries"),
    ))))
    .await
    .expect("list manifest entries")
    .len()
}

fn unique_prefix(name: &str) -> String {
    format!(
        "{name}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

// =============================================================================
// A lost HEAD publication response
// =============================================================================

/// Delegates to the emulator's store; when armed, applies the next
/// conditional HEAD write and then reports an error instead of its response
/// (the response is lost), and fails the HEAD reread that follows, so the
/// sink cannot settle the outcome itself.
#[derive(Debug)]
struct LostHeadResponse {
    inner: Arc<dyn ObjectStore>,
    lose_next_head_cas: AtomicBool,
    fail_next_head_read: AtomicBool,
}

impl LostHeadResponse {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            lose_next_head_cas: AtomicBool::new(false),
            fail_next_head_read: AtomicBool::new(false),
        }
    }

    fn lost() -> object_store::Error {
        object_store::Error::Generic {
            store: "canary",
            source: "connection reset before the response arrived".into(),
        }
    }
}

impl fmt::Display for LostHeadResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LostHeadResponse({})", self.inner)
    }
}

fn is_head(location: &Path) -> bool {
    location.as_ref().ends_with("/_manifest/HEAD")
}

#[async_trait]
impl ObjectStore for LostHeadResponse {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let conditional = matches!(opts.mode, PutMode::Update(_));
        if is_head(location)
            && conditional
            && self.lose_next_head_cas.swap(false, Ordering::SeqCst)
        {
            self.inner.put_opts(location, payload, opts).await?;
            self.fail_next_head_read.store(true, Ordering::SeqCst);
            return Err(Self::lost());
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if is_head(location)
            && self.fail_next_head_read.swap(false, Ordering::SeqCst)
        {
            return Err(Self::lost());
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

// =============================================================================
// Tests
// =============================================================================

/// The default path acknowledges a batch only at a published boundary: its
/// data object and manifest entry are written, the conditional HEAD
/// publication succeeds, and the batch is found by walking the published
/// chain from HEAD. A restart that redelivers the acknowledged batch
/// acknowledges it again without a new manifest entry; the next batch extends
/// the chain.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn durable_v2_publishes_a_discoverable_boundary_idempotently()
-> Result<()> {
    let infra = ministack().await;
    let prefix = unique_prefix("durable-ack");
    let cfg = sink_cfg(&prefix, &infra.endpoint);
    let raw = raw_store(&infra.endpoint)?;

    let sink = production_sink(&cfg).await?;
    sink.send_batch_with_context(&[row(1), row(2)], &ctx(100))
        .await?;

    // The published boundary: HEAD -> manifest entry -> data objects.
    let head = read_json(&raw, head_key(&prefix).as_ref()).await?;
    assert_eq!(head["seq"], 1, "{head}");
    let wm_hex = hex::encode(ctx(100).durable_watermark.unwrap());
    assert_eq!(head["watermark_hex"], wm_hex.as_str());
    let entry_key = head["head_entry_key"].as_str().expect("entry key");
    let entry = read_json(&raw, entry_key).await?;
    assert_eq!(entry["watermark_hex"], wm_hex.as_str());
    assert_eq!(entry["event_count"], 2);
    let objects = entry["objects"].as_array().expect("objects");
    assert!(!objects.is_empty());
    let mut found = String::new();
    for o in objects {
        found += &read_text(&raw, o["key"].as_str().unwrap()).await?;
    }
    assert!(found.contains("canary-row-1") && found.contains("canary-row-2"));
    assert_eq!(entry_count(&raw, &prefix).await, 1);
    let epoch = head["epoch"].as_u64().unwrap();

    // Restart and redeliver the acknowledged batch: acknowledged again, no
    // second boundary.
    drop(sink);
    let restarted = production_sink(&cfg).await?;
    restarted
        .send_batch_with_context(&[row(1), row(2)], &ctx(100))
        .await?;
    let head = read_json(&raw, head_key(&prefix).as_ref()).await?;
    assert_eq!(head["seq"], 1, "no new boundary: {head}");
    assert_eq!(head["head_entry_key"], entry_key);
    assert!(
        head["epoch"].as_u64().unwrap() > epoch,
        "a new writer epoch"
    );
    assert_eq!(entry_count(&raw, &prefix).await, 1, "no duplicate entry");

    // The next batch extends the chain from the first boundary.
    restarted
        .send_batch_with_context(&[row(3)], &ctx(200))
        .await?;
    let head = read_json(&raw, head_key(&prefix).as_ref()).await?;
    assert_eq!(head["seq"], 2);
    let next =
        read_json(&raw, head["head_entry_key"].as_str().unwrap()).await?;
    assert_eq!(next["prev"]["key"], entry_key, "{next}");
    assert_eq!(entry_count(&raw, &prefix).await, 2);
    Ok(())
}

/// A HEAD publication that applied but whose response was lost (and whose
/// reread failed) is reported as an uncertain boundary, not acknowledged. A
/// reread of exactly that boundary proves it committed, also after a restart;
/// redelivering the batch then acknowledges it without writing or
/// publishing a second boundary.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn a_lost_head_response_is_settled_as_committed_not_republished()
-> Result<()> {
    let infra = ministack().await;
    let prefix = unique_prefix("durable-lost");
    let cfg = sink_cfg(&prefix, &infra.endpoint);
    let raw = raw_store(&infra.endpoint)?;

    let lossy = Arc::new(LostHeadResponse::new(Arc::clone(&raw)));
    let sink = DurableS3Sink::create(DurableS3Args {
        id: cfg.id.clone(),
        pipeline: PIPELINE.into(),
        source_id: SOURCE_ID.into(),
        required: true,
        prefix: prefix.clone(),
        store: lossy.clone(),
        format: DurableFormat::Jsonl,
        compression: Compression::None,
        schema_resolver: Arc::new(|_| {
            anyhow::bail!("JSON Lines has no schema")
        }),
        comparator: Arc::new(SourceCheckpointComparator),
    })
    .await?;
    lossy.lose_next_head_cas.store(true, Ordering::SeqCst);

    let err = sink
        .send_batch_with_context(&[row(1)], &ctx(100))
        .await
        .expect_err("an unsettled publication is never acknowledged");
    let draft = err.draft().expect("an incident");
    assert_eq!(draft.reason_code, ReasonCode::SinkAckUncertain);

    // It did apply: the emulator's HEAD references one entry at seq 1.
    let head = read_json(&raw, head_key(&prefix).as_ref()).await?;
    assert_eq!(head["seq"], 1, "{head}");
    assert_eq!(entry_count(&raw, &prefix).await, 1);

    // A reread of exactly that boundary proves it committed.
    assert_eq!(
        sink.settle_uncertain(&draft.evidence).await,
        BoundaryOutcome::Committed
    );
    drop(sink);
    let restarted = production_sink(&cfg).await?;
    assert_eq!(
        restarted.settle_uncertain(&draft.evidence).await,
        BoundaryOutcome::Committed,
        "also after a restart"
    );

    // Redelivery after the restart: acknowledged, no second boundary.
    restarted
        .send_batch_with_context(&[row(1)], &ctx(100))
        .await?;
    let after = read_json(&raw, head_key(&prefix).as_ref()).await?;
    assert_eq!(after["seq"], 1, "{after}");
    assert_eq!(after["head_entry_key"], head["head_entry_key"]);
    assert_eq!(entry_count(&raw, &prefix).await, 1, "no duplicate entry");
    Ok(())
}
