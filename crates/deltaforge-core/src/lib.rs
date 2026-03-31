//! DeltaForge Core Types
//!
//! This crate defines the core CDC event structure and traits used throughout DeltaForge.
//! The Event structure is designed to be Debezium-compatible at the payload level, enabling
//! seamless integration with existing CDC consumers and tooling.

use std::str::FromStr;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use checkpoints::CheckpointStore;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use tokio::{
    sync::{Notify, mpsc},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::warn;
use uuid::Uuid;

// ── Fast UUID v7 ─────────────────────────────────────────────────────────────
// Standard `Uuid::now_v7()` calls `getrandom` per UUID (~3% CPU in benchmarks).
// This implementation uses a global atomic counter for the random portion,
// avoiding the syscall entirely. The result is a valid RFC 9562 UUID v7 with
// monotonic ordering within a millisecond — ideal for CDC event IDs where
// uniqueness matters but cryptographic randomness does not.

static UUID_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Generate a UUID v7 using timestamp + atomic counter (no getrandom syscall).
#[inline]
pub fn fast_uuid_v7() -> Uuid {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let seq = UUID_COUNTER.fetch_add(1, Ordering::Relaxed);

    let mut b = [0u8; 16];
    // 48-bit unix timestamp (ms), big-endian
    b[0] = (millis >> 40) as u8;
    b[1] = (millis >> 32) as u8;
    b[2] = (millis >> 24) as u8;
    b[3] = (millis >> 16) as u8;
    b[4] = (millis >> 8) as u8;
    b[5] = millis as u8;
    // version 7 (4 bits) + 12 bits from counter
    b[6] = 0x70 | ((seq >> 8) as u8 & 0x0F);
    b[7] = seq as u8;
    // variant 10 (2 bits) + 62 bits from counter
    b[8] = 0x80 | ((seq >> 56) as u8 & 0x3F);
    b[9] = (seq >> 48) as u8;
    b[10] = (seq >> 40) as u8;
    b[11] = (seq >> 32) as u8;
    b[12] = (seq >> 24) as u8;
    b[13] = (seq >> 16) as u8;
    b[14] = (seq >> 8) as u8;
    b[15] = seq as u8;

    Uuid::from_bytes(b)
}

pub mod encoding;
pub mod envelope;
pub mod errors;
pub use errors::{SinkError, SourceError};

pub mod routing;
pub use routing::EventRouting;

pub mod batch_context;
pub use batch_context::BatchContext;

pub mod journal;
pub use journal::{DlqMeta, JournalEntry};

// ============================================================================
// Operation Type
// ============================================================================

/// CDC operation type.
///
/// Serializes to Debezium-compatible single-character codes:
/// - `Create` → "c" (insert)
/// - `Update` → "u"
/// - `Delete` → "d"
/// - `Read` → "r" (snapshot)
/// - `Truncate` → "t"
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Op {
    Create,
    Update,
    Delete,
    Read,
    Truncate,
}

impl Serialize for Op {
    fn serialize<S: Serializer>(
        &self,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for Op {
    fn deserialize<D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(|_| {
            serde::de::Error::unknown_variant(&s, &["c", "u", "d", "r", "t"])
        })
    }
}

impl Op {
    /// Returns the Debezium-compatible string code.
    #[inline]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Op::Create => "c",
            Op::Update => "u",
            Op::Delete => "d",
            Op::Read => "r",
            Op::Truncate => "t",
        }
    }
}

/// Error type for parsing Op from string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseOpError(String);

impl std::fmt::Display for ParseOpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unknown operation code: '{}'", self.0)
    }
}

impl std::error::Error for ParseOpError {}

impl FromStr for Op {
    type Err = ParseOpError;

    /// Parse from Debezium string code.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "c" => Ok(Op::Create),
            "u" => Ok(Op::Update),
            "d" => Ok(Op::Delete),
            "r" => Ok(Op::Read),
            "t" => Ok(Op::Truncate),
            _ => Err(ParseOpError(s.to_string())),
        }
    }
}

// ============================================================================
// Source Metadata (Debezium-compatible)
// ============================================================================

/// Source metadata block - matches Debezium's `source` structure.
///
/// Contains information about where the event originated, including
/// connector-specific position information for resume/replay.
///
/// Named `SourceInfo` to avoid collision with the `Source` trait.
/// Serializes to the `"source"` field in the Event JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceInfo {
    /// DeltaForge version string (e.g., "deltaforge-0.1.0")
    #[serde(default = "default_version")]
    pub version: String,

    /// Connector type: "mysql", "postgresql", "mongodb", etc.
    pub connector: String,

    /// Logical server/pipeline name - primary identifier for consumers
    pub name: String,

    /// Source event timestamp in milliseconds since epoch
    pub ts_ms: i64,

    /// Database name
    pub db: String,

    /// Schema name (PostgreSQL) - None for MySQL
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,

    /// Table name (without schema/db prefix)
    pub table: String,

    /// Snapshot marker: "true", "first", "last", or None
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<String>,

    /// Connector-specific position fields (flattened into source block)
    #[serde(flatten)]
    pub position: SourcePosition,
}

fn default_version() -> String {
    concat!("deltaforge-", env!("CARGO_PKG_VERSION")).to_string()
}

impl SourceInfo {
    /// Returns the fully-qualified table name (schema.table or db.table)
    pub fn full_table_name(&self) -> String {
        match &self.schema {
            Some(schema) => format!("{}.{}", schema, self.table),
            None => format!("{}.{}", self.db, self.table),
        }
    }
}

// ============================================================================
// Source Position (Connector-specific)
// ============================================================================

/// Connector-specific position information.
///
/// Flattened into the `source` block to match Debezium's format where
/// position fields appear alongside other source metadata.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct SourcePosition {
    // MySQL-specific fields
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_id: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub gtid: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub pos: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub row: Option<u32>,

    // PostgreSQL-specific fields
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lsn: Option<String>,

    #[serde(rename = "txId", skip_serializing_if = "Option::is_none")]
    pub tx_id: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub xmin: Option<i64>,

    // Generic sequence (for other sources)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequence: Option<String>,
}

impl SourcePosition {
    /// Create MySQL position info
    pub fn mysql(
        server_id: u32,
        gtid: Option<String>,
        file: Option<String>,
        pos: Option<u64>,
        row: Option<u32>,
    ) -> Self {
        Self {
            server_id: Some(server_id),
            gtid,
            file,
            pos,
            row,
            ..Default::default()
        }
    }

    /// Create PostgreSQL position info
    pub fn postgres(
        lsn: String,
        tx_id: Option<i64>,
        xmin: Option<i64>,
    ) -> Self {
        Self {
            lsn: Some(lsn),
            tx_id,
            xmin,
            ..Default::default()
        }
    }

    /// Create generic position info
    pub fn generic(sequence: String) -> Self {
        Self {
            sequence: Some(sequence),
            ..Default::default()
        }
    }
}

// ============================================================================
// Transaction Metadata
// ============================================================================

/// Transaction metadata for event grouping.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    /// Transaction identifier (GTID, XID, etc.)
    pub id: String,

    /// Global ordering across all transactions
    #[serde(rename = "total_order", skip_serializing_if = "Option::is_none")]
    pub total_order: Option<u64>,

    /// Ordering within this transaction's data collections
    #[serde(
        rename = "data_collection_order",
        skip_serializing_if = "Option::is_none"
    )]
    pub data_collection_order: Option<u64>,
}

// ============================================================================
// CDC Event (Debezium-compatible payload)
// ============================================================================

/// CDC Event - Debezium-compatible at the payload level.
///
/// The struct is designed so that `serde_json::to_vec(&event)` produces
/// JSON that Debezium consumers can parse directly. DeltaForge-specific
/// extensions are additive and ignored by standard Debezium consumers.
///
/// # Wire Format
///
/// Native serialization produces Debezium's payload structure:
/// ```json
/// {
///   "before": null,
///   "after": {"id": 1, "name": "Alice"},
///   "source": {
///     "version": "deltaforge-0.1.0",
///     "connector": "mysql",
///     "name": "prod-db",
///     "ts_ms": 1700000000000,
///     "db": "inventory",
///     "table": "customers",
///     "gtid": "abc:123"
///   },
///   "op": "c",
///   "ts_ms": 1700000000000
/// }
/// ```
///
/// For full Debezium envelope format `{"payload": {...}}`, use the
/// envelope module at serialization time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    // ========================================================================
    // Debezium-standard fields (ordered to match Debezium output)
    // ========================================================================
    /// Row image before the change (Update/Delete)
    pub before: Option<Value>,

    /// Row image after the change (Create/Update)
    pub after: Option<Value>,

    /// Source metadata (Debezium-compatible structure)
    pub source: SourceInfo,

    /// Operation type: "c", "u", "d", "r", "t"
    pub op: Op,

    /// Event timestamp in milliseconds since epoch
    pub ts_ms: i64,

    /// Transaction metadata
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction: Option<Transaction>,

    // ========================================================================
    // DeltaForge extensions (Debezium consumers ignore unknown fields)
    // ========================================================================
    /// Globally unique event ID for deduplication and tracing
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_id: Option<Uuid>,

    /// Tenant ID for multi-tenant deployments
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,

    /// Schema registry version/fingerprint
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_version: Option<String>,

    /// Schema sequence number for replay correlation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_sequence: Option<u64>,

    /// DDL payload for schema change events
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ddl: Option<Value>,

    /// Distributed tracing correlation ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,

    /// Processing tags (e.g., ["pii:redacted", "transformed"])
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,

    /// Processor ID that synthesized this event (`Some`), or `None` if it
    /// came directly from a database source.
    ///
    /// Set automatically by `SyntheticMarkingProcessor` - processors that
    /// create new events (metrics, js fan-out) do not need to set this
    /// themselves; the framework detects new event IDs and fills it in.
    ///
    /// The value is the `id()` of the processor that first produced the event.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub synthetic: Option<String>,

    /// Routing overrides for sinks (topic, key, headers).
    /// Visible to processors but excluded from wire output by envelopes.
    #[serde(skip)]
    pub routing: Option<EventRouting>,

    /// Transaction boundary marker (true = last event in transaction)
    /// Useful for batching decisions in the coordinator.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub tx_end: bool,

    // ========================================================================
    // Internal fields (never serialized to wire)
    // ========================================================================
    /// Checkpoint data for resumption (internal use only)
    #[serde(skip)]
    pub checkpoint: Option<CheckpointMeta>,

    /// Estimated event size in bytes for batching
    #[serde(skip)]
    pub size_bytes: usize,

    /// Wall-clock time (ms since epoch) when this event was first parsed by
    /// the pipeline. Used for pipeline-internal latency tracking; always
    /// millisecond-precise regardless of source timestamp granularity.
    #[serde(skip)]
    pub received_at_ms: i64,
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

impl Event {
    /// Create a new row-change event.
    #[allow(clippy::too_many_arguments)]
    pub fn new_row(
        source: SourceInfo,
        op: Op,
        before: Option<Value>,
        after: Option<Value>,
        ts_ms: i64,
        size_bytes: usize,
    ) -> Self {
        Self {
            before,
            after,
            source,
            op,
            ts_ms,
            transaction: None,
            event_id: Some(fast_uuid_v7()),
            tenant_id: None,
            schema_version: None,
            schema_sequence: None,
            ddl: None,
            trace_id: None,
            tags: None,
            synthetic: None,
            routing: None,
            tx_end: true,
            checkpoint: None,
            size_bytes,
            received_at_ms: now_ms(),
        }
    }

    /// Create a new DDL/schema change event.
    pub fn new_ddl(
        source: SourceInfo,
        ddl: Value,
        ts_ms: i64,
        size_bytes: usize,
    ) -> Self {
        Self {
            before: None,
            after: None,
            source,
            op: Op::Read, // DDL events use "r" in Debezium
            ts_ms,
            transaction: None,
            event_id: Some(fast_uuid_v7()),
            tenant_id: None,
            schema_version: None,
            schema_sequence: None,
            ddl: Some(ddl),
            trace_id: None,
            tags: None,
            synthetic: None,
            routing: None,
            tx_end: true,
            checkpoint: None,
            size_bytes,
            received_at_ms: now_ms(),
        }
    }

    /// Create a snapshot read event.
    pub fn new_snapshot(
        source: SourceInfo,
        after: Value,
        ts_ms: i64,
        size_bytes: usize,
    ) -> Self {
        Self {
            before: None,
            after: Some(after),
            source,
            op: Op::Read,
            ts_ms,
            transaction: None,
            event_id: Some(fast_uuid_v7()),
            tenant_id: None,
            schema_version: None,
            schema_sequence: None,
            ddl: None,
            trace_id: None,
            tags: None,
            synthetic: None,
            routing: None,
            tx_end: true,
            checkpoint: None,
            size_bytes,
            received_at_ms: now_ms(),
        }
    }

    /// Set transaction metadata.
    pub fn with_transaction(
        mut self,
        transaction: Transaction,
        tx_end: bool,
    ) -> Self {
        self.transaction = Some(transaction);
        self.tx_end = tx_end;
        self
    }

    /// Set tenant ID.
    pub fn with_tenant(mut self, tenant_id: impl Into<String>) -> Self {
        self.tenant_id = Some(tenant_id.into());
        self
    }

    /// Set checkpoint metadata (internal use).
    pub fn with_checkpoint(mut self, checkpoint: CheckpointMeta) -> Self {
        self.checkpoint = Some(checkpoint);
        self
    }

    /// Generate idempotency key for deduplication.
    pub fn idempotency_key(&self) -> String {
        use std::fmt::Write;
        // Pre-size: tenant(~8) + db(~8) + table(~16) + tx_id(~8) + uuid(36) + separators(4)
        let mut key = String::with_capacity(80);
        let _ = write!(
            key,
            "{}|{}.{}|{}|",
            self.tenant_id.as_deref().unwrap_or("_"),
            self.source.db,
            self.source.table,
            self.transaction
                .as_ref()
                .map(|t| t.id.as_str())
                .unwrap_or(""),
        );
        match self.event_id {
            Some(u) => {
                // Write UUID hyphenated directly into the buffer (no intermediate String).
                let _ = write!(key, "{}", u.as_hyphenated());
            }
            None => key.push('_'),
        }
        key
    }

    /// Returns the fully-qualified table name.
    #[inline]
    pub fn full_table_name(&self) -> String {
        self.source.full_table_name()
    }

    /// Mark this event as synthesized by the given processor.
    ///
    /// Intended for processors that conjure events from nothing - metrics
    /// windows, alert fan-outs, etc. Call on new events before returning
    /// them from `process()`.
    #[must_use]
    pub fn mark_synthetic(mut self, processor_id: impl Into<String>) -> Self {
        self.synthetic = Some(processor_id.into());
        self
    }

    /// Returns true if this event was created by a processor rather than a source.
    #[inline]
    pub fn is_synthetic(&self) -> bool {
        self.synthetic.is_some()
    }
}

// ============================================================================
// Checkpoint (Internal)
// ============================================================================

/// Opaque checkpoint data for source resumption.
///
/// This is internal bookkeeping data, not part of the wire format.
/// Each source defines its own checkpoint structure serialized as bytes.
#[derive(Debug, Clone)]
pub enum CheckpointMeta {
    Opaque(Arc<[u8]>),
}

impl CheckpointMeta {
    pub fn from_vec(data: Vec<u8>) -> Self {
        Self::Opaque(data.into())
    }

    pub fn from_slice(data: &[u8]) -> Self {
        Self::Opaque(Arc::from(data))
    }

    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Opaque(data) => data,
        }
    }
}

impl Serialize for CheckpointMeta {
    fn serialize<S: Serializer>(
        &self,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match self {
            CheckpointMeta::Opaque(data) => serializer.serialize_bytes(data),
        }
    }
}

impl<'de> Deserialize<'de> for CheckpointMeta {
    fn deserialize<D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Self, D::Error> {
        let bytes: Vec<u8> = Deserialize::deserialize(deserializer)?;
        Ok(CheckpointMeta::Opaque(bytes.into()))
    }
}

// ============================================================================
// Supporting Types
// ============================================================================

#[derive(Debug, Clone)]
pub struct ShardCtx {
    pub shard_id: String,
    pub tenant_id: String,
}

#[derive(Debug, Clone, Copy)]
pub enum ConnectionMode {
    Shared,
    Dedicated,
}

pub type SourceResult<T> = Result<T, SourceError>;
pub type SinkResult<T> = std::result::Result<T, SinkError>;

/// Result of a batch send. Contains per-event failures that should be
/// routed to the DLQ alongside the successful delivery.
#[derive(Debug, Default)]
pub struct BatchResult {
    /// Events that failed preparation (serialization/routing).
    /// Each entry is `(index_in_original_batch, error)`.
    pub dlq_failures: Vec<(usize, SinkError)>,
}

impl BatchResult {
    /// Create an empty result (no DLQ failures).
    pub fn ok() -> Self {
        Self {
            dlq_failures: Vec::new(),
        }
    }
}

// ============================================================================
// Source Handle
// ============================================================================

/// Control handle for a running source.
pub struct SourceHandle {
    pub cancel: CancellationToken,
    pub paused: Arc<AtomicBool>,
    pub pause_notify: Arc<Notify>,
    pub join: JoinHandle<SourceResult<()>>,
}

impl SourceHandle {
    /// Pause the source temporarily.
    pub fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
    }

    /// Resume a paused source.
    pub fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
        self.pause_notify.notify_waiters();
    }

    /// Stop the source completely.
    pub fn stop(&self) {
        self.cancel.cancel();
        self.pause_notify.notify_waiters();
    }

    /// Wait for the source task to complete.
    pub async fn join(self) -> Result<()> {
        match self.join.await {
            Ok(r) => Ok(r?),
            Err(e) => Err(anyhow!("source task panicked: {e}")),
        }
    }

    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }
}

// ============================================================================
// Traits
// ============================================================================

#[async_trait]
pub trait Source: Send + Sync {
    async fn run(
        &self,
        tx: mpsc::Sender<Event>,
        checkpoint_store: Arc<dyn CheckpointStore>,
    ) -> SourceHandle;

    /// Compare two checkpoint byte slices, returning their ordering.
    ///
    /// Used by the per-sink checkpoint system to find the minimum (earliest)
    /// checkpoint across all sinks so the source replays from the position
    /// the slowest sink needs.
    ///
    /// Each source MUST implement this correctly for its checkpoint format.
    /// Returning `Equal` on parse failure is safe (no replay regression) but
    /// may cause unnecessary replay.
    fn compare_checkpoints(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering;
}

#[async_trait]
pub trait Processor: Send + Sync {
    fn id(&self) -> &str;
    async fn process(
        &self,
        events: Vec<Event>,
        ctx: &BatchContext,
    ) -> Result<Vec<Event>>;
}

#[async_trait]
pub trait Sink: Send + Sync {
    fn id(&self) -> &str;

    fn required(&self) -> bool {
        true
    }

    async fn send(&self, event: &Event) -> SinkResult<()>;

    /// Send a batch of events. Returns `BatchResult` which may contain
    /// per-event DLQ failures (serialization/routing errors) alongside
    /// successful delivery of the remaining events.
    ///
    /// The `SinkError` return is for sink-level failures (connection, auth, etc.)
    /// that affect the entire batch. Per-event failures go in `BatchResult.dlq_failures`.
    async fn send_batch(&self, events: &[Event]) -> SinkResult<BatchResult> {
        for event in events {
            self.send(event).await?;
        }
        Ok(BatchResult::ok())
    }
}

#[async_trait]
pub trait SchemaRegistry: Send + Sync {
    async fn register(
        &self,
        tenant: &str,
        db: &str,
        table: &str,
        hash: &str,
        schema_json: &Value,
    ) -> Result<i32>;

    async fn latest(
        &self,
        tenant: &str,
        db: &str,
        table: &str,
    ) -> Result<Option<(i32, String)>>;
}

// ============================================================================
// Pipeline Types
// ============================================================================

pub type ArcDynSource = Arc<dyn Source>;
pub type ArcDynProcessor = Arc<dyn Processor>;
pub type ArcDynSink = Arc<dyn Sink>;

pub struct Pipeline {
    pub id: String,
    pub sources: Vec<ArcDynSource>,
    pub processors: Vec<ArcDynProcessor>,
    pub sinks: Vec<ArcDynSink>,
}

/// Handle for controlling a running pipeline.
pub struct PipelineHandle {
    id: String,
    cancel: CancellationToken,
    source_handles: Vec<SourceHandle>,
    join: JoinHandle<Result<()>>,
}

impl PipelineHandle {
    pub fn new(
        id: String,
        cancel: CancellationToken,
        source_handles: Vec<SourceHandle>,
        join: JoinHandle<Result<()>>,
    ) -> Self {
        Self {
            id,
            cancel,
            source_handles,
            join,
        }
    }

    pub fn pause(&self) {
        warn!(pipeline_id = %self.id, "pausing pipeline");
        self.source_handles.iter().for_each(|h| h.pause());
    }

    pub fn resume(&self) {
        warn!(pipeline_id = %self.id, "resuming pipeline");
        self.source_handles.iter().for_each(|h| h.resume());
    }

    pub fn stop(&self) {
        warn!(pipeline_id = %self.id, "stopping pipeline");
        self.cancel.cancel();
        self.source_handles.iter().for_each(|h| h.stop());
    }

    pub async fn join(self) -> Result<()> {
        match self.join.await {
            Ok(r) => r,
            Err(e) => Err(anyhow!("pipeline {} task panicked: {}", self.id, e)),
        }
    }

    pub async fn join_all_components(self) -> Result<()> {
        let PipelineHandle {
            id,
            cancel: _,
            source_handles,
            join,
        } = self;

        let mut first_err: Option<anyhow::Error> = None;

        if let Err(e) = match join.await {
            Ok(r) => r,
            Err(e) => Err(anyhow!("pipeline {} task panicked: {}", id, e)),
        } {
            first_err = Some(e);
        }

        for h in source_handles {
            if let Err(e) = h.join().await {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }

        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn test_source() -> SourceInfo {
        SourceInfo {
            version: "deltaforge-0.1.0".to_string(),
            connector: "mysql".to_string(),
            name: "prod-db".to_string(),
            ts_ms: 1700000000000,
            db: "inventory".to_string(),
            schema: None,
            table: "customers".to_string(),
            snapshot: None,
            position: SourcePosition::mysql(
                1,
                Some("abc:123".to_string()),
                Some("mysql-bin.000001".to_string()),
                Some(12345),
                Some(0),
            ),
        }
    }

    #[test]
    fn op_serializes_to_debezium_codes() {
        assert_eq!(serde_json::to_string(&Op::Create).unwrap(), r#""c""#);
        assert_eq!(serde_json::to_string(&Op::Update).unwrap(), r#""u""#);
        assert_eq!(serde_json::to_string(&Op::Delete).unwrap(), r#""d""#);
        assert_eq!(serde_json::to_string(&Op::Read).unwrap(), r#""r""#);
        assert_eq!(serde_json::to_string(&Op::Truncate).unwrap(), r#""t""#);
    }

    #[test]
    fn event_serializes_to_debezium_structure() {
        let event = Event::new_row(
            test_source(),
            Op::Create,
            None,
            Some(json!({"id": 1, "name": "Alice"})),
            1700000000000,
            128,
        );

        let json = serde_json::to_value(&event).unwrap();

        // Verify Debezium-standard fields
        assert_eq!(json["op"], "c");
        assert_eq!(json["ts_ms"], 1700000000000i64);
        assert!(json["before"].is_null());
        assert_eq!(json["after"]["name"], "Alice");

        // Verify source block structure
        assert_eq!(json["source"]["connector"], "mysql");
        assert_eq!(json["source"]["db"], "inventory");
        assert_eq!(json["source"]["table"], "customers");

        // Verify position fields are flattened into source
        assert_eq!(json["source"]["gtid"], "abc:123");
        assert_eq!(json["source"]["file"], "mysql-bin.000001");
        assert_eq!(json["source"]["pos"], 12345);
    }

    #[test]
    fn event_roundtrip() {
        let original = Event::new_row(
            test_source(),
            Op::Update,
            Some(json!({"id": 1, "name": "Alice"})),
            Some(json!({"id": 1, "name": "Alice Smith"})),
            1700000000000,
            256,
        )
        .with_tenant("acme");

        let json = serde_json::to_string(&original).unwrap();
        let parsed: Event = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.op, Op::Update);
        assert_eq!(parsed.tenant_id, Some("acme".to_string()));
        assert_eq!(parsed.source.connector, "mysql");
        assert_eq!(parsed.before.unwrap()["name"], "Alice");
        assert_eq!(parsed.after.unwrap()["name"], "Alice Smith");
    }
}
