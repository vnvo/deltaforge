//! DeltaForge Core Types
//!
//! This crate defines the core CDC event structure and traits used throughout DeltaForge.
//! The Event structure is designed to be Debezium-compatible at the payload level, enabling
//! seamless integration with existing CDC consumers and tooling.

use std::str::FromStr;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
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

pub mod encoding;
pub mod envelope;
pub mod errors;
pub use errors::{SinkError, SourceError};

pub mod routing;
pub use routing::EventRouting;

pub mod batch_context;
pub use batch_context::BatchContext;

pub mod event_id;
pub use event_id::{
    EventClass, EventId, IdentityCell, IdentityKind, IdentityValue,
    SourceLineage, TemporalKind,
};

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

    /// Connector type: "mysql", "postgresql", etc.
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

    /// The transaction's final LSN (from the `BEGIN` message) - the stable
    /// identity coordinate, distinct from the per-message `lsn`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_final_lsn: Option<String>,

    /// The relation OID for the changed table (stable across rename).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relation_oid: Option<u32>,

    /// Per-transaction change ordinal - reset at `BEGIN`, incremented for every
    /// identity-bearing change before filtering.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub change_ordinal: Option<u32>,

    // Snapshot-specific fields
    /// Durable snapshot generation for stable snapshot-row identity. Serialized
    /// (optional) - has lasting operational value.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_generation: Option<u64>,

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
    /// Stable, deterministic event identity (`dfid:v1:…`) for deduplication and
    /// tracing. The single authoritative identity location: every event that
    /// leaves a source or processor carries one. `Option` only so the JS bridge
    /// can build a transient event before assigning the resolved id - such an
    /// event must never escape without it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_id: Option<EventId>,

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
    /// This event's durable boundary: its resume checkpoint and, atomically, the
    /// durable watermark for the SAME source state. One composite field (not two
    /// independent ones) so a caller cannot set a checkpoint without its matching
    /// watermark or vice versa - the pairing is type-enforced. Read via
    /// [`Event::checkpoint`]/[`Event::durable_watermark`], set via
    /// [`Event::set_boundary`]/[`Event::with_boundary`]/[`Event::with_checkpoint`].
    /// `None` for events with no durable boundary; the durable sink fails closed
    /// when a boundary is required and absent. Internal - never serialized.
    #[serde(skip)]
    pub boundary: Option<SourceBoundary>,

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
    /// Stamp this event's durable boundary: its resume checkpoint and the durable
    /// watermark for the SAME source state, carried together as one
    /// [`SourceBoundary`] so the two can never drift.
    pub fn set_boundary(&mut self, boundary: SourceBoundary) {
        self.boundary = Some(boundary);
    }

    /// Builder form of [`Event::set_boundary`].
    pub fn with_boundary(mut self, boundary: SourceBoundary) -> Self {
        self.set_boundary(boundary);
        self
    }

    /// The resume checkpoint for this event's boundary, if any.
    pub fn checkpoint(&self) -> Option<&CheckpointMeta> {
        self.boundary.as_ref().map(|b| &b.checkpoint)
    }

    /// The durable watermark for this event's boundary, if any. Present only when
    /// the boundary carries one (a checkpoint-only boundary has none).
    pub fn durable_watermark(&self) -> Option<&Arc<[u8]>> {
        self.boundary
            .as_ref()
            .and_then(|b| b.durable_watermark.as_ref())
    }

    /// Create a new row-change event. The stable `event_id` is required -
    /// sources must derive it before emission.
    #[allow(clippy::too_many_arguments)]
    pub fn new_row(
        event_id: EventId,
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
            event_id: Some(event_id),
            tenant_id: None,
            schema_version: None,
            schema_sequence: None,
            ddl: None,
            trace_id: None,
            tags: None,
            synthetic: None,
            routing: None,
            tx_end: true,
            boundary: None,
            size_bytes,

            received_at_ms: now_ms(),
        }
    }

    /// Create a new DDL/schema change event. `event_id` is required.
    pub fn new_ddl(
        event_id: EventId,
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
            event_id: Some(event_id),
            tenant_id: None,
            schema_version: None,
            schema_sequence: None,
            ddl: Some(ddl),
            trace_id: None,
            tags: None,
            synthetic: None,
            routing: None,
            tx_end: true,
            boundary: None,
            size_bytes,

            received_at_ms: now_ms(),
        }
    }

    /// Create a snapshot read event. `event_id` is required.
    pub fn new_snapshot(
        event_id: EventId,
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
            event_id: Some(event_id),
            tenant_id: None,
            schema_version: None,
            schema_sequence: None,
            ddl: None,
            trace_id: None,
            tags: None,
            synthetic: None,
            routing: None,
            tx_end: true,
            boundary: None,
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

    /// Set a checkpoint-only boundary (no durable watermark). Convenience for
    /// sources that resume from a checkpoint but do not (yet) carry a durable
    /// watermark; equivalent to `set_boundary(SourceBoundary::checkpoint_only(..))`.
    pub fn set_checkpoint(&mut self, checkpoint: CheckpointMeta) {
        self.boundary = Some(SourceBoundary::checkpoint_only(checkpoint));
    }

    /// Builder form of [`Event::set_checkpoint`].
    pub fn with_checkpoint(mut self, checkpoint: CheckpointMeta) -> Self {
        self.set_checkpoint(checkpoint);
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
        match &self.event_id {
            // The stable EventId's textual form is already globally unique.
            Some(id) => {
                let _ = write!(key, "{id}");
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
// Source stream item
// ============================================================================

/// An item on the source→coordinator stream. `TxCommit` is an **internal**
/// transaction-boundary marker - never a public wire event and never delivered
/// to sinks. It lets the coordinator form transaction-aligned batches and
/// checkpoint only at commit boundaries (even for empty or fully-filtered
/// transactions).
// `Event` is the hot-path variant - one per row - and was already moved by
// value through the old `Sender<Event>`. Boxing it to shrink the enum would add
// a heap allocation per event that the previous channel never paid; `TxCommit`
// is rare (once per transaction), so the size difference is intentional.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum SourceItem {
    /// A change event.
    Event(Event),
    /// The start of a source transaction. `tx_id` matches the `transaction.id`
    /// stamped on that transaction's events and the `tx_id` of its closing
    /// [`SourceItem::TxCommit`]. Emitted exactly once per real transaction, so
    /// the coordinator can tell a valid **empty** transaction (a begin followed
    /// immediately by a commit) from a commit for an unknown transaction.
    TxBegin { tx_id: String },
    /// A committed-transaction boundary. `boundary` carries the COMMIT/XID
    /// **record's** checkpoint (not the last data event's) and, atomically, the
    /// durable watermark for that same boundary - both generated together by the
    /// source and passed through unchanged. `tx_id` matches the `transaction.id`
    /// carried by that transaction's events. Only an actual commit emits this -
    /// never a rollback, and never a transactional logical message (those are
    /// transaction *contents*).
    TxCommit {
        tx_id: String,
        boundary: SourceBoundary,
    },
}

/// A legal checkpoint boundary emitted by a source: the resume `checkpoint`
/// (the source's own persisted/deserialized format) plus, atomically, the
/// `durable_watermark` for the *same* boundary state. The two are always
/// produced together so a sink can never pair a checkpoint with a watermark
/// from a different snapshot state. The watermark is opaque here (a serialized,
/// source-aware watermark); the durable S3 sink's injected comparator interprets
/// it, and legacy sinks ignore it.
#[derive(Debug, Clone)]
pub struct SourceBoundary {
    pub checkpoint: CheckpointMeta,
    pub durable_watermark: Option<Arc<[u8]>>,
}

impl SourceBoundary {
    /// A boundary carrying only a resume checkpoint (no durable watermark).
    pub fn checkpoint_only(checkpoint: CheckpointMeta) -> Self {
        Self {
            checkpoint,
            durable_watermark: None,
        }
    }
}

impl SourceItem {
    /// Convenience: wrap an event as a stream item.
    pub fn event(ev: Event) -> Self {
        SourceItem::Event(ev)
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
// Checkpoint ordering (source-aware)
// ============================================================================

/// Structured ordering of two source checkpoints. Unlike `std::cmp::Ordering`
/// this has an `Incomparable` case for checkpoints from different lineages or
/// generations (e.g. a MySQL failover to a new server, or a snapshot-generation
/// change) that must not be ordered against each other.
///
/// Ordering MUST use structured source semantics (PostgreSQL LSN + transaction
/// boundary, MySQL GTID/binlog coordinates, snapshot generation, etc.), never a
/// lexical comparison of serialized bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointOrder {
    /// `proposed` precedes `reference`.
    Before,
    /// `proposed` is the same position as `reference`.
    Equal,
    /// `proposed` follows `reference`.
    After,
    /// The two checkpoints belong to different lineages/generations and cannot
    /// be ordered; callers must fail closed.
    Incomparable,
}

/// Compares source checkpoints with [`CheckpointOrder`] semantics. Used by the
/// durable S3 sink to enforce that a published watermark only ever advances,
/// even when a valid new-epoch writer receives replayed older batches after a
/// crash between the durable HEAD CAS and the coordinator checkpoint.
pub trait CheckpointComparator: Send + Sync {
    /// Order `proposed` relative to `reference` using structured source
    /// semantics. Returns [`CheckpointOrder::Incomparable`] when the two cannot
    /// be meaningfully ordered.
    fn order(&self, proposed: &[u8], reference: &[u8]) -> CheckpointOrder;
}

// ============================================================================
// Traits
// ============================================================================

#[async_trait]
pub trait Source: Send + Sync {
    async fn run(
        &self,
        tx: mpsc::Sender<SourceItem>,
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

    /// Stable identity digest for this processor - the `processor_digest`
    /// component of a synthetic [`EventId`]. Sensitive to anything that changes
    /// the processor's output (source bytes and/or canonical config). Computed
    /// once at construction and returned by reference; never recomputed per
    /// event.
    fn identity_digest(&self) -> &str;
}

/// Authoritative per-batch delivery context, constructed by the coordinator /
/// runner from **pre-processing** source state. It carries the batch's commit
/// checkpoint and durable watermark so a sink never has to infer them from the
/// surviving (post-processor) events - which is unreliable, because a processor
/// can drop the event that carried the checkpoint and a fully-filtered
/// transaction can leave an empty batch whose checkpoint must still advance.
#[derive(Debug, Clone)]
pub struct SinkBatchContext {
    /// The batch's commit checkpoint (the source position it covers), captured
    /// before any processor ran.
    pub checkpoint: CheckpointMeta,
    /// Serialized, source-aware durable watermark (carrying source lineage) for
    /// HEAD ordering. Opaque here; the sink's injected [`CheckpointComparator`]
    /// interprets it. `None` when durable ordering is not in use.
    pub durable_watermark: Option<Vec<u8>>,
    /// Optional stable batch identity for diagnostics/logging.
    pub batch_id: Option<String>,
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

    /// Send a batch with its authoritative [`SinkBatchContext`]. The default
    /// delegates to [`Sink::send_batch`], so existing sinks are unaffected; the
    /// durable S3 sink overrides this to derive its watermark from the context
    /// (never from the events) and requires it to be present.
    async fn send_batch_with_context(
        &self,
        events: &[Event],
        _ctx: &SinkBatchContext,
    ) -> SinkResult<BatchResult> {
        self.send_batch(events).await
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
    fn op_parses_from_debezium_codes() {
        assert_eq!("c".parse::<Op>().unwrap(), Op::Create);
        assert_eq!("u".parse::<Op>().unwrap(), Op::Update);
        assert_eq!("d".parse::<Op>().unwrap(), Op::Delete);
        assert_eq!("r".parse::<Op>().unwrap(), Op::Read);
        assert_eq!("t".parse::<Op>().unwrap(), Op::Truncate);
        assert!("x".parse::<Op>().is_err());
    }

    #[test]
    fn sink_error_dlq_eligibility() {
        // Only per-event serialization/routing errors are DLQ-eligible.
        assert!(
            SinkError::Serialization {
                details: "bad".into()
            }
            .is_dlq_eligible()
        );
        assert!(
            SinkError::Routing {
                details: "no route".into()
            }
            .is_dlq_eligible()
        );
        // Batch-fatal / transport errors are NOT (must fail the batch).
        assert!(
            !SinkError::Fatal {
                details: "fenced".into()
            }
            .is_dlq_eligible()
        );
        assert!(
            !SinkError::Connect {
                details: "refused".into()
            }
            .is_dlq_eligible()
        );
    }

    #[test]
    fn event_serializes_to_debezium_structure() {
        let event = Event::new_row(
            crate::EventId::mysql_row_server(1, "t", 1, 0),
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
            crate::EventId::mysql_row_server(1, "t", 1, 0),
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
