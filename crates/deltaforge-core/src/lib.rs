use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use deltaforge_checkpoints::CheckpointStore;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    sync::{Notify, mpsc},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use uuid::Uuid;

pub mod errors;
pub use errors::{SinkError, SourceError};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Op {
    Insert,
    Update,
    Delete,
    Ddl,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceMeta {
    pub kind: String,
    pub host: String,
    pub db: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ChangeKind {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Change {
    pub kind: ChangeKind,
    pub before: Option<Value>,
    pub after: Option<Value>,
}
// Canonical CDC event emited by Deltaforge
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    /// Stable, globally unique ID for this emitted event (used for dedupe, tracing, audit, etc)
    pub event_id: Uuid,

    /// Tenant that owns this data/pipeline (used for isolation, routing, quotas, etc)
    /// This is a static value set by the pipeline creator at the moment.
    pub tenant_id: String,

    /// Source data source metadata (e.g, {kind: "mysql"|"postgres", host, db})
    /// See `SourceMeta` for exact fields.
    pub source: SourceMeta,

    /// Logical table name that produced that change.
    /// Postgres: "schema.table"; MySQL: "db.table"
    pub table: String,

    /// Operation type: Insert | Update | Delte | DDL
    pub op: Op,

    /// Data source transaction ID if known (PG XID, MySQL XID/GTID, ...)
    /// Useful for grouping events from the same commit, etc
    pub tx_id: Option<String>,

    /// Row image *before* the change (when available)
    /// Typically present for Update/Delete may be partial depending on WAL/binlog/etc config.
    pub before: Option<serde_json::Value>,

    /// Row image *after* the change (when available).
    /// Present for Insert/Update; null for Delete. Can reflect transforms done by the pipeline processors.
    pub after: Option<serde_json::Value>,

    /// Schema registry version/hash that `before`/`after` conform to at emit time
    /// Lets consumers validate compatibility across schema evolution
    pub schema_version: Option<String>,

    /// DDL payload for schema changes when `op == Op::DDL`.
    /// Usually includes fields like {"sql": "...", "normalized": "...", "diff": "..."}.
    pub ddl: Option<serde_json::Value>,

    /// Event timestamp in UTC.
    /// Prefer the source commit time when available; otherwise the capture/ingest time.
    pub timestamp: DateTime<Utc>,

    /// Dist tracing corelation ID
    pub trace_id: Option<String>,

    /// Free-form labels attached by processors or the platform (e.g. ["normalized", "pii:redacted", "variant:summary"]).
    pub tags: Option<Vec<String>>,

    /// Event checkpoint info
    pub checkpoint: Option<CheckpointMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CheckpointMeta {
    Opaque(Vec<u8>),
}

impl Event {
    pub fn idempotency_key(&self) -> String {
        format!(
            "{}|{}|{}|{}",
            self.tenant_id,
            self.table,
            self.tx_id.clone().unwrap_or_default(),
            self.event_id
        )
    }

    pub fn new_row(
        tenant_id: String,
        source: SourceMeta,
        table: String,
        op: Op,
        before: Option<Value>,
        after: Option<Value>,
        ts_ms: i64,
    ) -> Self {
        let ts = DateTime::<Utc>::from_timestamp_millis(ts_ms)
            .unwrap_or_else(Utc::now);
        Self {
            event_id: Uuid::new_v4(),
            tenant_id,
            source,
            table,
            op,
            tx_id: None,
            before,
            after,
            schema_version: None,
            ddl: None,
            timestamp: ts,
            trace_id: None,
            tags: None,
            checkpoint: None,
        }
    }

    pub fn new_ddl(
        tenant_id: String,
        source: SourceMeta,
        table: String,
        ddl: Value,
        ts_ms: i64,
    ) -> Self {
        let ts = DateTime::<Utc>::from_timestamp_millis(ts_ms)
            .unwrap_or_else(Utc::now);
        Self {
            event_id: Uuid::new_v4(),
            tenant_id,
            source,
            table,
            op: Op::Ddl,
            tx_id: None,
            before: None,
            after: None,
            schema_version: None,
            ddl: Some(ddl),
            timestamp: ts,
            trace_id: None,
            tags: None,
            checkpoint: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ShardCtx {
    pub shard_id: String,
    pub tenant_id: String,
}

#[derive(Debug, Clone, Copy)]
pub enum ConnctionMode {
    Shared,
    Dedicated,
}

pub type SourceResult<T> = Result<T, SourceError>;
pub type SinkResult<T> = std::result::Result<T, SinkError>;

/// SourceHandle as the control interface for a source
/// Caller/Controller of a source should use it to interact with a running source
pub struct SourceHandle {
    pub cancel: CancellationToken,
    pub paused: Arc<AtomicBool>,
    pub pause_notify: Arc<Notify>,
    pub join: JoinHandle<SourceResult<()>>,
}

impl SourceHandle {
    /// Pause the source, temporarily.
    /// Each source decides on the mechanics and side effects of pause on its own.
    pub fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
    }

    /// Un-Pause/Resume the operation.
    pub fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
        self.pause_notify.notify_waiters();
    }

    /// Stop/Cancel the operation completely.
    /// Should be used on shutdowns and/or restarts.
    /// The caller is responsible for re-initiating the source if needed.
    /// The source is responsible for any and all required cleanups for its own scope.
    pub fn stop(&self) {
        self.cancel.cancel();
        self.pause_notify.notify_waiters();
    }

    pub async fn join(self) -> Result<()> {
        match self.join.await {
            Ok(r) => Ok(r?),
            Err(e) => Err(anyhow::anyhow!("source task panicked: {e}")),
        }
    }

    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }
}

#[async_trait]
pub trait Source: Send + Sync {
    async fn run(
        &self,
        tx: mpsc::Sender<Event>,
        checkpoint_store: Arc<dyn CheckpointStore>,
    ) -> SourceHandle;
}

#[async_trait]
pub trait Processor: Send + Sync {
    /// Takes ownership of a "batch" of events and returns a new batch.
    ///
    /// Implementations are free to:
    /// - modify events in place
    /// - drop events
    /// - add new events (duplicates / variants)
    fn id(&self) -> &str;
    async fn process(&self, events:  Vec<Event>) -> Result<Vec<Event>>;
}

#[async_trait]
pub trait Sink: Send + Sync {
    fn id(&self) -> &str;
    async fn send(&self, event: Event) -> SinkResult<()>;
}

#[async_trait]
pub trait SchemaRegistry: Send + Sync {
    async fn register(
        &self,
        tenant: &str,
        db: &str,
        table: &str,
        hash: &str,
        schema_json: &serde_json::Value,
    ) -> Result<i32>;

    async fn latest(
        &self,
        tenant: &str,
        db: &str,
        table: &str,
    ) -> Result<Option<(i32, String)>>; // (version, hash)
}

pub type ArcDynSource = Arc<dyn Source>;
pub type ArcDynProcessor = Arc<dyn Processor>;
pub type ArcDynSink = Arc<dyn Sink>;

pub struct Pipeline {
    pub id: String,
    pub sources: Vec<ArcDynSource>,
    pub processors: Vec<ArcDynProcessor>,
    pub sinks: Vec<ArcDynSink>,
}

/// the handle to a running pipeline to be used by upper layers/caller
/// to interact with a CDC pipeline
pub struct PipelineHandle {
    id: String,
    cancel: CancellationToken,
    source_handles: Vec<SourceHandle>,
    join: JoinHandle<Result<()>>,
}

impl PipelineHandle {
    /// Pause the pipeline operation.
    /// It is a temporary stop for the Pipeline and its components.
    /// The pause is forwarded to all components (sources, processors and sinks).
    /// It is up to each component how to handle the pause but it needs to be resumable with no extra infomation.
    pub fn pause(&self) {
        warn!(pipeline_id = &self.id, "pausing the pipeline is requested");
        // forward the pause to all source handles
        let _ = &self.source_handles.iter().for_each(|h| h.pause());
    }

    pub fn resume(&self) {
        warn!(pipeline_id = &self.id, "resuming the pipeline is requested");
        let _ = &self.source_handles.iter().for_each(|h| h.resume());
    }

    /// Stop the pipeline and all its components.
    /// Any in-flight events should get processed before stop is complete:
    /// - Cancels the dispatch loop
    /// - Cancels all the sinks, processros and sources in that order.
    pub fn stop(&self) {
        warn!(pipeline_id = &self.id, "stopping the pipeline is requested");
        self.cancel.cancel();
        let _ = &self.source_handles.iter().for_each(|h| h.stop());
    }

    /// Await the dispatcher loop to finish
    pub async fn join(self) -> Result<()> {
        match self.join.await {
            Ok(r) => r,
            Err(e) => Err(anyhow!("pipeline {} task panicked: {}", self.id, e)),
        }
    }

    pub async fn join_all_components(self) -> Result<()> {
        let PipelineHandle {
            id,
            cancel,
            source_handles,
            join,
        } = self;

        let mut first_err: Option<anyhow::Error> = None;
        if let Err(e) = match join.await {
            Ok(r) => r,
            Err(e) => Err(anyhow!("pipeline {} task panicked: {}", id, e)),
        } {
            first_err = Some(e)
        }

        for h in source_handles {
            if let Err(e) = h.join().await {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }

        if let Some(e) = first_err {
            Err(e)
        } else {
            Ok(())
        }
    }
}

// impl Pipeline {
//     pub async fn start(
//         &self,
//         ckpt_store: Arc<dyn CheckpointStore>,
//     ) -> Result<PipelineHandle> {
//         let (tx, mut rx) = mpsc::channel::<Event>(1024);

//         let pipeline_id = self.id.clone();
//         info!(pipeline_id = pipeline_id, "pipeline starting ...");
//         //spawn sources
//         let mut source_handles: Vec<SourceHandle> =
//             Vec::with_capacity(self.sources.len());

//         for src in self.sources.iter().cloned() {
//             let src_handle = src.run(tx.clone(), ckpt_store.clone()).await;
//             source_handles.push(src_handle);
//         }
//         drop(tx);

//         let cancel = CancellationToken::new();
//         let cancel_for_task = cancel.clone();

//         let processors = self.processors.clone();
//         let sinks = self.sinks.clone();

//         // main dispatcher loop
//         let join = tokio::spawn(async move {
//             loop {
//                 tokio::select! {
//                     _ = cancel_for_task.cancelled() => {
//                         break; //graceful shutdown
//                     }

//                     maybe_ev = rx.recv() => {
//                         let Some(ev) = maybe_ev else { break; };

//                         //sequential processing
//                         let mut out: Vec<Event> = vec![ev];
//                         for p in processors.iter() {
//                             let mut next = Vec::new();
//                             for mut e in out.drain(..) {
//                                 match p.process(&mut e).await {
//                                     Ok(produced) => next.extend(produced),
//                                     Err(err) => error!(error = %err, "processor error; dropping event"),
//                                 }
//                             }
//                             out = next;
//                         }

//                         // dispatch to sinks
//                         for e in out.into_iter() {
//                             for s in sinks.iter() {
//                                 if let Err(err) = s.send(e.clone()).await {
//                                     error!(error = %err, "sink send failed");
//                                 }
//                             }
//                         }
//                     }
//                 };
//             }

//             Ok(())
//         });

//         Ok(PipelineHandle {
//             id: pipeline_id,
//             cancel,
//             source_handles,
//             join,
//         })
//     }
// }
