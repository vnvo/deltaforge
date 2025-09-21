use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;
use uuid::Uuid;

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

#[async_trait]
pub trait Source: Send + Sync {
    async fn run(&self, tx: mpsc::Sender<Event>) -> Result<()>;
}

#[async_trait]
pub trait Processor: Send + Sync {
    async fn process(&self, event: Event) -> Result<Vec<Event>>;
}

#[async_trait]
pub trait Sink: Send + Sync {
    async fn send(&self, event: Event) -> Result<()>;
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

pub type DynSource = Box<dyn Source>;
pub type DynProcessor = Box<dyn Processor>;
pub type DynSink = Box<dyn Sink>;

pub struct Pipeline {
    pub sources: Vec<DynSource>,
    pub processors: Vec<DynProcessor>,
    pub sinks: Vec<DynSink>,
}

impl Pipeline {
    pub async fn start(self) -> Result<()> {
        let (tx, mut rx) = mpsc::channel::<Event>(1024);

        //spawn sources
        for src in self.sources.into_iter() {
            let tx_clone = tx.clone();
            tokio::spawn(async move {
                if let Err(e) = src.run(tx_clone).await {
                    tracing::error!(error = %e, "source failed")
                }
            });
        }
        drop(tx);

        // main consumer/process/dispath loop
        while let Some(ev) = rx.recv().await {
            // sequetial processing
            let mut out: Vec<Event> = vec![ev];
            for p in self.processors.iter() {
                let mut next = Vec::new();
                for e in out.into_iter() {
                    match p.process(e).await {
                        Ok(mut produced) => next.append(&mut produced),
                        Err(err) => {
                            tracing::error!(error = %err, "processor error; dropping event");
                        }
                    }
                }
                out = next
            }

            // dispatch to sinks (fire-n-forget)
            for e in out.into_iter() {
                for s in self.sinks.iter() {
                    if let Err(err) = s.send(e.clone()).await {
                        tracing::error!(error = %err, "sink send failed");
                    }
                }
            }
        }

        Ok(())
    }
}
