//! Pipeline coordinator with batching, delivery, and schema sensing.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use checkpoints::CheckpointStore;
use futures::future::{BoxFuture, join_all};
use metrics::{counter, gauge, histogram};
use parking_lot::Mutex;
use tokio::sync::watch;
use tokio::time::{Instant, interval};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use uuid::Uuid;

use deltaforge_config::{BatchConfig, CommitPolicy, SchemaSensingConfig};
use deltaforge_core::{
    ArcDynProcessor, ArcDynSink, BatchContext, CheckpointMeta, Event,
    SinkError, SourceItem,
};
use schema_sensing::{ObserveResult, SchemaSensor};

use crate::drift_detector::{DriftDetector, DriftSummary};
use crate::schema_provider::{ArcSchemaProvider, TableSchemaInfo};

/// Persist/commit the token after the batch is successfully delivered to sinks.
pub type CommitCpFn<Tok> =
    Box<dyn Fn(Tok) -> BoxFuture<'static, Result<()>> + Send + Sync + 'static>;

pub struct ProcessedBatch<Tok> {
    pub events: Vec<Event>,
    pub last_checkpoint: Option<Tok>,
}

/// Process a *mutable* batch of events (can modify/duplicate/drop).
/// Takes ownership of the current batch Vec and returns the processed Vec.
pub type ProcessBatchFn<Tok> = Arc<
    dyn Fn(Vec<Event>) -> BoxFuture<'static, Result<ProcessedBatch<Tok>>>
        + Send
        + Sync
        + 'static,
>;

/// During accumulation, we own a mutable Vec<Event>.
///
/// `committed_len`/`committed_bytes` track the prefix of `raw` that forms whole
/// transactions (or standalone boundary events) and is therefore safe to flush.
/// Anything past that prefix is an open transaction whose commit marker has not
/// arrived yet; it must never be flushed or checkpointed on its own.
struct BuildingBatch {
    started_at: Instant,
    raw: Vec<Event>,
    bytes: usize,
    committed_len: usize,
    committed_bytes: usize,
    /// The batch's last committed source boundary: its resume checkpoint and the
    /// durable watermark for the SAME source state, carried together (never
    /// re-derived) into the delivery context. It is the single watermark
    /// authority. `None` for non-durable / no-boundary batches.
    boundary: Option<deltaforge_core::SourceBoundary>,
    /// Whether the approaching-cap warning has already fired for the currently-open
    /// transaction (per dimension), so it is emitted at most once per transaction.
    /// Reset when the open region closes at a boundary.
    approaching_events_warned: bool,
    approaching_bytes_warned: bool,
}

impl BuildingBatch {
    fn with_capacity(cap: usize) -> Self {
        Self {
            started_at: Instant::now(),
            raw: Vec::with_capacity(cap),
            bytes: 0,
            committed_len: 0,
            committed_bytes: 0,
            boundary: None,
            approaching_events_warned: false,
            approaching_bytes_warned: false,
        }
    }

    /// Reset the per-open-transaction approaching-cap warning flags. Called when the
    /// open region closes at a boundary, so the next transaction warns afresh.
    fn reset_approaching_warnings(&mut self) {
        self.approaching_events_warned = false;
        self.approaching_bytes_warned = false;
    }

    /// Number of events in the currently-open (uncommitted) transaction.
    fn open_events(&self) -> usize {
        self.raw.len() - self.committed_len
    }

    /// Serialized bytes accumulated for the currently-open transaction.
    fn open_bytes(&self) -> usize {
        self.bytes - self.committed_bytes
    }

    /// True while events past the last committed boundary await their marker.
    fn mid_tx(&self) -> bool {
        self.raw.len() > self.committed_len
    }
}

/// Which per-transaction cap a source transaction breached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OversizedLimit {
    Events,
    Bytes,
    Both,
}

impl std::fmt::Display for OversizedLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            OversizedLimit::Events => "events",
            OversizedLimit::Bytes => "bytes",
            OversizedLimit::Both => "events+bytes",
        })
    }
}

/// A single source transaction exceeded a configured accumulation cap. Internal
/// (no tx id); the run loop turns this into an [`OversizedTxError`] with full
/// context.
#[derive(Debug, Clone, Copy)]
struct OversizedTx {
    events: usize,
    bytes: usize,
    exceeded: OversizedLimit,
}

/// Fatal, typed error for `oversized_tx: Fail`: a source transaction exceeded its
/// configured per-transaction cap. Carries structured context - the transaction id,
/// observed events/bytes, the configured event/byte limits, and which limit was
/// exceeded - so callers match on fields instead of parsing the message. The
/// transaction is replayed whole on restart.
#[derive(Debug, Clone, thiserror::Error)]
#[error(
    "source transaction {tx_id} exceeds its size cap ({exceeded}): observed \
     {observed_events} events / {observed_bytes} bytes against limits \
     {max_tx_events} events / {max_tx_bytes} bytes; oversized_tx policy is Fail - \
     reduce the transaction at the source or raise max_tx_events/max_tx_bytes, then \
     restart to replay the whole transaction"
)]
pub struct OversizedTxError {
    pub tx_id: String,
    pub observed_events: usize,
    pub observed_bytes: usize,
    pub max_tx_events: usize,
    pub max_tx_bytes: usize,
    pub exceeded: OversizedLimit,
}

/// Append one transactional event to the open transaction, enforcing the hard
/// per-transaction caps (the `oversized_tx` safety valve). Never splits: the
/// event is always buffered; an `Err` signals the caps were breached and the
/// caller must fail the pipeline (`OversizedTxPolicy::Fail`).
fn push_tx_event(
    b: &mut BuildingBatch,
    ev: Event,
    max_tx_events: usize,
    max_tx_bytes: usize,
) -> Result<(), OversizedTx> {
    b.bytes += event_size_hint(&ev);
    b.raw.push(ev);
    let events = b.open_events();
    let bytes = b.open_bytes();
    let over_events = events > max_tx_events;
    let over_bytes = bytes > max_tx_bytes;
    if over_events || over_bytes {
        let exceeded = match (over_events, over_bytes) {
            (true, true) => OversizedLimit::Both,
            (true, false) => OversizedLimit::Events,
            (false, true) => OversizedLimit::Bytes,
            (false, false) => unreachable!(),
        };
        return Err(OversizedTx {
            events,
            bytes,
            exceeded,
        });
    }
    Ok(())
}

/// Close the open transaction at a commit marker. The boundary `checkpoint`
/// (the commit record's position) is stamped onto the batch's last event so the
/// batch checkpoints at the boundary rather than at the last data row. For an
/// empty or fully source-filtered transaction (no new events since the last
/// boundary) the checkpoint is advanced onto the prior boundary event when one
/// exists; otherwise the next transaction covers it. Returns whether the
/// transaction contributed events.
fn close_tx(
    b: &mut BuildingBatch,
    boundary: deltaforge_core::SourceBoundary,
) -> bool {
    let had_events = b.mid_tx();
    // The whole boundary (checkpoint + watermark, from one source state) becomes
    // the batch boundary and is stamped on the last event for resume.
    b.boundary = Some(boundary.clone());
    if let Some(last) = b.raw.last_mut() {
        last.set_boundary(boundary);
    }
    b.committed_len = b.raw.len();
    b.committed_bytes = b.bytes;
    b.reset_approaching_warnings();
    had_events
}

/// Apply a data-less boundary (a [`SourceItem::Boundary`], e.g. snapshot
/// completion) to the batch: it becomes the batch boundary and the whole batch is
/// a committed prefix. Boundaries only arrive between transactions/standalone
/// rows, never mid-transaction.
fn close_boundary(
    b: &mut BuildingBatch,
    boundary: deltaforge_core::SourceBoundary,
) {
    b.boundary = Some(boundary);
    b.committed_len = b.raw.len();
    b.committed_bytes = b.bytes;
    b.reset_approaching_warnings();
}

/// Record that a standalone (non-transactional) event is its own boundary. Its
/// source boundary (checkpoint + watermark, set atomically at the source via
/// `Event::set_boundary`) becomes the batch boundary, so a non-GTID CDC row or
/// snapshot row carries a boundary exactly like a commit marker does. The latest
/// boundary wins; an event without one leaves the current boundary untouched.
fn commit_standalone(b: &mut BuildingBatch) {
    b.committed_len = b.raw.len();
    b.committed_bytes = b.bytes;
    b.reset_approaching_warnings();
    if let Some(bd) = b.raw.last().and_then(|e| e.boundary.clone()) {
        b.boundary = Some(bd);
    }
}

/// A flush is due when the whole-transaction prefix has reached the soft size
/// limits. Only ever evaluated at a boundary, so it never splits a transaction.
fn soft_limit_reached(
    b: &BuildingBatch,
    max_events: usize,
    max_bytes: usize,
) -> bool {
    b.committed_len >= max_events || b.committed_bytes >= max_bytes
}

/// Drop any open (uncommitted) transaction suffix, keeping only whole
/// transactions. Used on shutdown/cancel: a transaction with no commit marker
/// was never durable at the source and is replayed on restart. Returns the
/// batch only if whole transactions remain to flush.
fn keep_whole_txs(mut b: BuildingBatch) -> Option<BuildingBatch> {
    b.raw.truncate(b.committed_len);
    b.bytes = b.committed_bytes;
    if b.raw.is_empty() { None } else { Some(b) }
}

/// Prepare the in-flight batch for a terminal flush (cancel / source-closed /
/// disconnect). In tx-aligned mode any uncommitted partial transaction is
/// discarded - it was never durable at the source, so its checkpoint must not
/// advance and it replays whole on restart. The discard is recorded so an
/// operator can see it happened. Returns the remainder to flush, if any.
///
/// A rolled-back transaction needs no special handling here: MySQL binlog and
/// PostgreSQL logical replication only stream committed transactions, so the
/// coordinator never sees aborted events - an in-progress suffix at shutdown is
/// simply a transaction whose commit had not yet been read.
fn finalize_batch(
    b: BuildingBatch,
    respect_source_tx: bool,
    pipeline: &str,
) -> Option<BuildingBatch> {
    if !respect_source_tx {
        return if b.raw.is_empty() { None } else { Some(b) };
    }
    let discarded = b.raw.len() - b.committed_len;
    if discarded > 0 {
        warn!(
            pipeline = %pipeline,
            discarded_events = discarded,
            "discarding uncommitted partial transaction; it replays from the \
             prior committed boundary on restart"
        );
        counter!(
            "deltaforge_discarded_partial_tx_events_total",
            "pipeline" => pipeline.to_string(),
        )
        .increment(discarded as u64);
    }
    keep_whole_txs(b)
}

/// Item sent from the accumulation loop to the delivery task.
struct DeliveryItem {
    batch: BuildingBatch,
    reason: &'static str,
}

/// After processing, freeze as Arc<[Event]> for zero-copy sharing.
#[derive(Debug)]
#[allow(dead_code)]
struct FrozenBatch {
    id: Uuid,
    events: Arc<[Event]>,
    bytes: usize,
}

fn event_size_hint(ev: &Event) -> usize {
    ev.size_bytes
}

fn is_tx_boundary(ev: &Event) -> bool {
    ev.tx_end
}

/// Push one event into the building batch. If the batch would exceed limits,
/// split it first and return the full batch for delivery.
fn check_and_split(
    b: &mut BuildingBatch,
    ev: Event,
    max_events: usize,
    max_bytes: usize,
) -> Option<BuildingBatch> {
    let would_exceed_events = b.raw.len() >= max_events;
    let would_exceed_bytes = b.bytes + event_size_hint(&ev) >= max_bytes;
    let limit_hit = would_exceed_events || would_exceed_bytes;
    let boundary = is_tx_boundary(&ev);

    let flush = if (!b.raw.is_empty() || !boundary) && limit_hit {
        Some(std::mem::replace(
            b,
            BuildingBatch::with_capacity(max_events),
        ))
    } else {
        None
    };

    b.bytes += event_size_hint(&ev);
    b.raw.push(ev);
    flush
}

/// Emit an approaching-limit warning + metric once, as the open transaction
/// crosses 80% of the event cap. Fires on the exact count so it logs once per
/// oversized-approaching transaction rather than on every subsequent event.
fn maybe_warn_approaching(
    b: &mut BuildingBatch,
    max_tx_events: usize,
    max_tx_bytes: usize,
    pipeline: &str,
) {
    // Warn (once per open transaction, per dimension) when the open transaction
    // reaches 80% of a cap, so operators see it coming before the fail-closed cut.
    let events_threshold = (max_tx_events.saturating_mul(8) / 10).max(1);
    if !b.approaching_events_warned && b.open_events() >= events_threshold {
        b.approaching_events_warned = true;
        warn!(
            pipeline = %pipeline,
            open_events = b.open_events(),
            max_tx_events,
            "source transaction approaching the event cap"
        );
        counter!(
            "deltaforge_tx_approaching_limit_total",
            "pipeline" => pipeline.to_string(),
            "limit" => "events",
        )
        .increment(1);
    }

    let bytes_threshold = (max_tx_bytes.saturating_mul(8) / 10).max(1);
    if !b.approaching_bytes_warned && b.open_bytes() >= bytes_threshold {
        b.approaching_bytes_warned = true;
        warn!(
            pipeline = %pipeline,
            open_bytes = b.open_bytes(),
            max_tx_bytes,
            "source transaction approaching the byte cap"
        );
        counter!(
            "deltaforge_tx_approaching_limit_total",
            "pipeline" => pipeline.to_string(),
            "limit" => "bytes",
        )
        .increment(1);
    }
}

/// A violation of the source→coordinator transaction protocol. Any of these is
/// fatal: a wrong or stale marker must never close a transaction or advance a
/// checkpoint, so the coordinator fails closed and the source replays from the
/// last committed boundary.
#[derive(Debug, thiserror::Error)]
pub enum TxProtocolError {
    #[error(
        "transaction {new} began while transaction {active} was still open"
    )]
    NestedBegin { active: String, new: String },
    #[error(
        "event for transaction {event_tx} arrived with no open transaction"
    )]
    EventWithoutBegin { event_tx: String },
    #[error(
        "event for transaction {event_tx} arrived while transaction {active} \
         was open"
    )]
    EventTxMismatch { active: String, event_tx: String },
    #[error(
        "commit marker for transaction {marker} does not match the open \
         transaction {active}"
    )]
    CommitMismatch { active: String, marker: String },
    #[error(
        "commit marker for transaction {marker} with no open transaction \
         (duplicate or unknown commit)"
    )]
    CommitWithoutBegin { marker: String },
    #[error(
        "standalone (non-transactional) event arrived while transaction {active} \
         was still open"
    )]
    StandaloneEventInTx { active: String },
}

/// Enforces the source transaction protocol on the marker/event stream: a single
/// transaction is open at a time, its events carry its id, and only its own
/// commit marker (exactly once) closes it. This is the locked invariant that a
/// wrong or stale marker cannot advance a checkpoint.
#[derive(Default)]
struct TxTracker {
    active: Option<String>,
}

impl TxTracker {
    /// The currently open transaction id, if any.
    fn active(&self) -> Option<&str> {
        self.active.as_deref()
    }

    /// A `TxBegin` opens a transaction. Only one may be open at a time.
    fn begin(&mut self, tx_id: &str) -> Result<(), TxProtocolError> {
        if let Some(active) = &self.active {
            return Err(TxProtocolError::NestedBegin {
                active: active.clone(),
                new: tx_id.to_string(),
            });
        }
        self.active = Some(tx_id.to_string());
        Ok(())
    }

    /// A transactional event must belong to the open transaction. Standalone
    /// (non-transactional, e.g. snapshot) events are not tracked.
    fn observe_event(&self, ev: &Event) -> Result<(), TxProtocolError> {
        match &ev.transaction {
            Some(txn) => match &self.active {
                None => Err(TxProtocolError::EventWithoutBegin {
                    event_tx: txn.id.clone(),
                }),
                Some(active) if *active != txn.id => {
                    Err(TxProtocolError::EventTxMismatch {
                        active: active.clone(),
                        event_tx: txn.id.clone(),
                    })
                }
                _ => Ok(()),
            },
            // A non-transactional event while a transaction is open would be
            // committed as a standalone boundary and could split the open
            // transaction. Fail closed instead.
            None => match &self.active {
                Some(active) => Err(TxProtocolError::StandaloneEventInTx {
                    active: active.clone(),
                }),
                None => Ok(()),
            },
        }
    }

    /// A `TxCommit` closes the open transaction and must match it. A commit with
    /// no open transaction is a duplicate or unknown commit - rejected so it
    /// cannot advance the checkpoint twice.
    fn commit(&mut self, tx_id: &str) -> Result<(), TxProtocolError> {
        match &self.active {
            None => Err(TxProtocolError::CommitWithoutBegin {
                marker: tx_id.to_string(),
            }),
            Some(active) if active != tx_id => {
                Err(TxProtocolError::CommitMismatch {
                    active: active.clone(),
                    marker: tx_id.to_string(),
                })
            }
            _ => {
                self.active = None;
                Ok(())
            }
        }
    }
}

/// Send a completed batch to the delivery task. Returns Err if the delivery
/// task has stopped (e.g. due to a sink error).
async fn send_to_delivery(
    tx: &tokio::sync::mpsc::Sender<DeliveryItem>,
    batch: BuildingBatch,
    reason: &'static str,
) -> Result<()> {
    tx.send(DeliveryItem { batch, reason })
        .await
        .map_err(|_| anyhow::anyhow!("delivery task stopped unexpectedly"))
}

fn policy_satisfied(
    policy: &Option<CommitPolicy>,
    total_sinks: usize,
    required_total: usize,
    required_acks: usize,
    total_acks: usize,
) -> bool {
    match policy.as_ref().unwrap_or(&CommitPolicy::Required) {
        // `All` means *every* sink acked - required and optional alike. It must
        // compare against the total sink count, not the required count (else a
        // pipeline with any optional sink could never satisfy `All`, and a
        // *failed* optional sink would spuriously satisfy it).
        CommitPolicy::All => total_acks == total_sinks,
        CommitPolicy::Required => required_acks == required_total,
        CommitPolicy::Quorum { quorum } => total_acks >= *quorum,
    }
}

/// Validate a commit policy against the number of sinks in the pipeline.
/// Rejects impossible quorum configurations before the pipeline starts, so an
/// operator gets an actionable error instead of a pipeline that can never
/// commit.
pub fn validate_commit_policy(
    policy: &Option<CommitPolicy>,
    sink_count: usize,
) -> Result<(), String> {
    if let Some(CommitPolicy::Quorum { quorum }) = policy.as_ref() {
        if *quorum == 0 {
            return Err("commit_policy quorum must be >= 1".to_string());
        }
        if *quorum > sink_count {
            return Err(format!(
                "commit_policy quorum {quorum} exceeds sink count {sink_count}"
            ));
        }
    }
    Ok(())
}

fn is_sink_required(sink: &ArcDynSink) -> bool {
    sink.required()
}

/// Schema sensing state, wrapped for interior mutability.
pub struct SchemaSensorState {
    sensor: Mutex<SchemaSensor>,
    drift_detector: Mutex<DriftDetector>,
    config: SchemaSensingConfig,
}

impl SchemaSensorState {
    pub fn new(config: SchemaSensingConfig) -> Self {
        Self {
            sensor: Mutex::new(SchemaSensor::new(config.clone())),
            drift_detector: Mutex::new(DriftDetector::new()),
            config,
        }
    }

    /// Register a table schema for drift detection.
    pub fn register_table_schema(&self, schema: TableSchemaInfo) {
        self.drift_detector.lock().register_table(schema);
    }

    /// Observe a batch of events with guided JSON-only sensing.
    ///
    /// When `db_schemas` is provided, only JSON columns are deep-inspected,
    /// and drift detection runs against expected types.
    ///
    /// When `db_schemas` is None (schemaless sources), observes entire payload.
    ///
    /// Returns the number of schema evolutions detected.
    pub fn observe_and_enrich(
        &self,
        events: &mut [Event],
        db_schemas: Option<&HashMap<String, TableSchemaInfo>>,
    ) -> usize {
        if !self.config.enabled {
            return 0;
        }

        let mut evolutions = 0;
        let mut sensor = self.sensor.lock();
        let mut drift = self.drift_detector.lock();

        for event in events.iter_mut() {
            let Some(after) = &event.after else { continue };

            // Run drift detection if we have DB schema
            if let Some(schemas) = db_schemas
                && let Some(_schema) = schemas.get(&event.source.table)
            {
                drift.observe(&event.source.table, after);
            }

            // Guided vs full sensing
            if let Some(schemas) = db_schemas {
                if let Some(schema) = schemas.get(&event.source.table) {
                    // Guided: only observe JSON columns
                    evolutions += self.observe_json_columns(
                        &mut sensor,
                        &event.source.table,
                        after,
                        schema,
                    );
                } else {
                    // No schema for this table - observe full payload
                    evolutions += self.observe_full_payload(
                        &mut sensor,
                        &event.source.table,
                        after,
                    );
                }
            } else {
                // No DB schemas at all - observe full payload
                evolutions += self.observe_full_payload(
                    &mut sensor,
                    &event.source.table,
                    after,
                );
            }

            // Enrich event with schema version info
            if let Some(version) = sensor.get_version(&event.source.table) {
                event.schema_version = Some(version.fingerprint.clone());
                event.schema_sequence = Some(version.sequence);
            }
        }

        evolutions
    }

    /// Observe only JSON columns in a row.
    fn observe_json_columns(
        &self,
        sensor: &mut SchemaSensor,
        table: &str,
        row: &serde_json::Value,
        schema: &TableSchemaInfo,
    ) -> usize {
        let Some(obj) = row.as_object() else {
            return 0;
        };

        let mut evolutions = 0;

        for col in schema.json_columns() {
            if let Some(json_value) = obj.get(&col.name) {
                // Skip null JSON columns
                if json_value.is_null() {
                    continue;
                }

                // Create a unique key for this table:column
                let key = format!("{}:{}", table, col.name);

                match sensor.observe_value(&key, json_value) {
                    Ok(result) => match &result {
                        ObserveResult::Evolved {
                            new_fingerprint,
                            new_sequence,
                            ..
                        } => {
                            evolutions += 1;
                            info!(
                                table = %table,
                                column = %col.name,
                                fingerprint = %new_fingerprint,
                                sequence = %new_sequence,
                                "JSON column schema evolved"
                            );
                        }
                        ObserveResult::NewSchema {
                            fingerprint,
                            sequence,
                        } => {
                            info!(
                                table = %table,
                                column = %col.name,
                                fingerprint = %fingerprint,
                                sequence = %sequence,
                                "new JSON column schema discovered"
                            );
                        }
                        _ => {}
                    },
                    Err(e) => {
                        warn!(
                            table = %table,
                            column = %col.name,
                            error = %e,
                            "JSON column schema sensing failed"
                        );
                    }
                }
            }
        }

        evolutions
    }

    /// Observe entire payload (for schemaless sources).
    fn observe_full_payload(
        &self,
        sensor: &mut SchemaSensor,
        table: &str,
        row: &serde_json::Value,
    ) -> usize {
        match sensor.observe_value(table, row) {
            Ok(result) => match &result {
                ObserveResult::Evolved {
                    new_fingerprint,
                    new_sequence,
                    ..
                } => {
                    info!(
                        table = %table,
                        fingerprint = %new_fingerprint,
                        sequence = %new_sequence,
                        "schema evolved"
                    );
                    1
                }
                ObserveResult::NewSchema {
                    fingerprint,
                    sequence,
                } => {
                    info!(
                        table = %table,
                        fingerprint = %fingerprint,
                        sequence = %sequence,
                        "new schema discovered"
                    );
                    0
                }
                _ => 0,
            },
            Err(e) => {
                warn!(table = %table, error = %e, "schema sensing failed");
                0
            }
        }
    }

    /// Get the underlying sensor for API access.
    pub fn sensor(&self) -> &Mutex<SchemaSensor> {
        &self.sensor
    }

    /// Get drift detector for API access.
    pub fn drift_detector(&self) -> &Mutex<DriftDetector> {
        &self.drift_detector
    }

    /// Check if drift has been detected.
    pub fn has_drift(&self) -> bool {
        self.drift_detector.lock().has_drift()
    }

    /// Get all drift summaries.
    pub fn drift_summaries(&self) -> Vec<DriftSummary> {
        self.drift_detector.lock().all_summaries()
    }
}

/// Replay-capture wiring, present only when `journal.replay` is enabled. Capture is
/// orthogonal to batching: it accumulates raw pre-processing events across coordinator
/// soft-limit flushes and closes one envelope per source commit unit at its boundary.
#[derive(Clone)]
pub struct ReplayCapture {
    pub journal: Arc<dyn crate::replay_journal::JournalLog>,
    pub identity: deltaforge_core::replay::PipelineIdentity,
    /// A captured envelope larger than this fails closed (0 = unbounded). A commit unit
    /// is never split to fit.
    pub max_envelope_bytes: usize,
    /// Schema-registry sequence at capture time, or `None` when the registry handle is
    /// not available (recorded honestly as unavailable rather than a placeholder).
    pub registry_seq_fn: Arc<dyn Fn() -> Option<u64> + Send + Sync>,
}

/// Fail-closed replay-capture errors for invalid raw events. A commit unit that cannot
/// be captured verbatim aborts the pipeline rather than storing corrupt identity/content.
#[derive(Debug, thiserror::Error)]
pub enum ReplayCaptureError {
    #[error(
        "replay capture: event has no stable event_id and cannot be captured; \
         a replay-enabled pipeline requires stable event ids on every event"
    )]
    MissingEventId,
    #[error("replay capture: event failed to serialize: {0}")]
    SerializeFailed(String),
}

/// A single source commit unit's replay envelope exceeds the configured size cap. The
/// unit is never split; this is fail-closed (the checkpoint does not advance).
#[derive(Debug, thiserror::Error)]
#[error(
    "replay envelope is {bytes} bytes ({events} events), over the {max}-byte cap; a \
     source commit unit is never split - raise journal.replay.max_envelope_bytes or \
     reduce the source transaction/snapshot-chunk size, then restart to replay the unit"
)]
pub struct OversizedReplayEnvelopeError {
    pub bytes: usize,
    pub max: usize,
    pub events: usize,
}

pub struct Coordinator<Tok> {
    pipeline_name: Arc<str>,
    sinks: Vec<ArcDynSink>,
    batch_cfg_eff: BatchConfig,
    commit_policy: Option<CommitPolicy>,
    /// Coordinator-level deadline for any single sink's `send_batch`.
    /// `None` disables the outer bound. See `Spec::sink_batch_deadline_secs`.
    sink_batch_deadline: Option<std::time::Duration>,
    /// Per-sink checkpoint commit functions, keyed by sink ID.
    /// Each sink that successfully delivers a batch gets its own checkpoint
    /// committed independently.
    commit_cp_per_sink: HashMap<String, CommitCpFn<Tok>>,
    process_batch: ProcessBatchFn<Tok>,
    /// Optional schema sensing
    schema_sensor: Option<Arc<SchemaSensorState>>,
    /// Optional schema provider for guided sensing
    schema_provider: Option<ArcSchemaProvider>,
    /// Cached DB schemas for fast lookup during batch processing
    db_schema_cache: Mutex<HashMap<String, TableSchemaInfo>>,
    /// Optional DLQ writer for routing per-event failures.
    dlq_writer: Option<Arc<crate::dlq::DlqWriter>>,
    /// Optional replay capture (enabled by `journal.replay`).
    replay_capture: Option<ReplayCapture>,
}

pub struct CoordinatorBuilder<Tok> {
    pipeline_name: String,
    sinks: Vec<ArcDynSink>,
    batch_config: Option<BatchConfig>,
    commit_policy: Option<CommitPolicy>,
    sink_batch_deadline: Option<std::time::Duration>,
    commit_fns: HashMap<String, CommitCpFn<Tok>>,
    process_fn: Option<ProcessBatchFn<Tok>>,
    schema_sensor: Option<Arc<SchemaSensorState>>,
    schema_provider: Option<ArcSchemaProvider>,
    dlq_writer: Option<Arc<crate::dlq::DlqWriter>>,
    replay_capture: Option<ReplayCapture>,
}

impl<Tok: Send + Clone + 'static> CoordinatorBuilder<Tok> {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            pipeline_name: name.into(),
            sinks: Vec::new(),
            batch_config: None,
            commit_policy: None,
            sink_batch_deadline: None,
            commit_fns: HashMap::new(),
            process_fn: None,
            schema_sensor: None,
            schema_provider: None,
            dlq_writer: None,
            replay_capture: None,
        }
    }

    pub fn sinks(mut self, sinks: Vec<ArcDynSink>) -> Self {
        self.sinks = sinks;
        self
    }

    pub fn batch_config(mut self, config: Option<BatchConfig>) -> Self {
        self.batch_config = config;
        self
    }

    pub fn commit_policy(mut self, policy: Option<CommitPolicy>) -> Self {
        self.commit_policy = policy;
        self
    }

    /// Set the coordinator-level deadline for any single sink's `send_batch`.
    /// `None` disables the outer bound. Pass the `sink_batch_deadline_secs`
    /// from the pipeline spec converted to a `Duration`.
    pub fn sink_batch_deadline(
        mut self,
        d: Option<std::time::Duration>,
    ) -> Self {
        self.sink_batch_deadline = d;
        self
    }

    /// Register a per-sink checkpoint commit function.
    pub fn commit_fn(
        mut self,
        sink_id: impl Into<String>,
        f: CommitCpFn<Tok>,
    ) -> Self {
        self.commit_fns.insert(sink_id.into(), f);
        self
    }

    pub fn process_fn(mut self, f: ProcessBatchFn<Tok>) -> Self {
        self.process_fn = Some(f);
        self
    }

    pub fn schema_sensor(mut self, sensor: Arc<SchemaSensorState>) -> Self {
        self.schema_sensor = Some(sensor);
        self
    }

    pub fn schema_provider(mut self, provider: ArcSchemaProvider) -> Self {
        self.schema_provider = Some(provider);
        self
    }

    pub fn dlq_writer(mut self, writer: Arc<crate::dlq::DlqWriter>) -> Self {
        self.dlq_writer = Some(writer);
        self
    }

    pub fn replay_capture(mut self, capture: ReplayCapture) -> Self {
        self.replay_capture = Some(capture);
        self
    }

    pub fn build(self) -> Coordinator<Tok> {
        let batch_cfg_eff = Coordinator::<Tok>::effective(&self.batch_config);
        assert!(
            !self.commit_fns.is_empty(),
            "at least one per-sink commit_fn is required"
        );

        Coordinator {
            pipeline_name: self.pipeline_name.into(),
            sinks: self.sinks,
            batch_cfg_eff,
            commit_policy: self.commit_policy,
            sink_batch_deadline: self.sink_batch_deadline,
            commit_cp_per_sink: self.commit_fns,
            process_batch: self.process_fn.expect("process_fn is required"),
            schema_sensor: self.schema_sensor,
            schema_provider: self.schema_provider,
            db_schema_cache: Mutex::new(HashMap::new()),
            dlq_writer: self.dlq_writer,
            replay_capture: self.replay_capture,
        }
    }
}

impl<Tok: Send + Clone + 'static> Coordinator<Tok> {
    pub fn builder(name: impl Into<String>) -> CoordinatorBuilder<Tok> {
        CoordinatorBuilder::new(name)
    }

    /// Close one captured commit unit: build a replay envelope from the accumulated
    /// raw (pre-processing) events plus `boundary`, persist it idempotently, then clear
    /// the accumulator. A data-less boundary (empty accumulator) is a genuine commit unit
    /// and is captured as an empty-events envelope, UNLESS it repeats the checkpoint of
    /// the unit just captured (a redundant snapshot-completion marker), which is skipped
    /// so it does not create a duplicate unit. Fail-closed: an invalid event, an append,
    /// or a size-cap failure returns Err so the caller aborts before the unit's checkpoint
    /// can commit; the accumulator is left intact on the error path (the source replays
    /// the unit and re-captures idempotently).
    async fn close_capture_unit(
        &self,
        accum: &mut Vec<Event>,
        boundary: &deltaforge_core::SourceBoundary,
        last_captured_boundary: &mut Option<
            deltaforge_core::replay::SourceBoundaryRecord,
        >,
    ) -> Result<()> {
        use deltaforge_core::replay::{
            REPLAY_ENVELOPE_VERSION, ReplayEnvelopePayload, ReplayEventRecord,
            SchemaBinding, SourceBoundaryRecord,
        };
        let Some(cap) = &self.replay_capture else {
            accum.clear();
            return Ok(());
        };
        let boundary_rec = SourceBoundaryRecord::from_boundary(boundary);
        // Redundant data-less boundary: no new events and the SAME complete boundary
        // (checkpoint AND durable watermark) as the unit just captured, e.g. a
        // snapshot-completion marker re-emitted after its chunk-final event. Skip so it
        // does not create a duplicate unit. Comparing the full boundary record (not just
        // the checkpoint) avoids collapsing two boundaries that share a checkpoint but
        // carry different watermarks.
        if accum.is_empty()
            && last_captured_boundary.as_ref() == Some(&boundary_rec)
        {
            return Ok(());
        }
        // Build records fail-closed: a missing event_id or a serialization failure is a
        // hard error (never store corrupt identity/content).
        let mut events: Vec<ReplayEventRecord> =
            Vec::with_capacity(accum.len());
        for (i, ev) in accum.iter().enumerate() {
            let event_id = ev
                .event_id
                .as_ref()
                .ok_or(ReplayCaptureError::MissingEventId)?
                .to_string();
            let event = serde_json::to_value(ev).map_err(|e| {
                ReplayCaptureError::SerializeFailed(e.to_string())
            })?;
            events.push(ReplayEventRecord {
                offset: i as u32,
                event_id,
                tx_id: ev.transaction.as_ref().map(|t| t.id.clone()),
                event,
            });
        }
        // Honest schema provenance: distinct fully-qualified source tables, sorted for
        // deterministic bytes (so an idempotent retry produces identical content).
        let mut source_tables: Vec<String> =
            accum.iter().map(|ev| ev.source.full_table_name()).collect();
        source_tables.sort_unstable();
        source_tables.dedup();
        let payload = ReplayEnvelopePayload {
            version: REPLAY_ENVELOPE_VERSION,
            pipeline_identity: cap.identity.clone(),
            boundary: boundary_rec.clone(),
            events,
            schema_binding: SchemaBinding {
                source_tables,
                registry_seq_at_capture: (cap.registry_seq_fn)(),
            },
        };
        if cap.max_envelope_bytes > 0 {
            let bytes = payload.canonical_bytes().len();
            if bytes > cap.max_envelope_bytes {
                counter!(
                    "deltaforge_replay_capture_oversized_total",
                    "pipeline" => self.pipeline_name.to_string(),
                )
                .increment(1);
                return Err(OversizedReplayEnvelopeError {
                    bytes,
                    max: cap.max_envelope_bytes,
                    events: payload.events.len(),
                }
                .into());
            }
        }
        cap.journal.append(&payload).await.map_err(|e| {
            counter!(
                "deltaforge_replay_capture_failures_total",
                "pipeline" => self.pipeline_name.to_string(),
            )
            .increment(1);
            e.context(
                "replay journal capture failed (fail closed; checkpoint not advanced)",
            )
        })?;
        counter!(
            "deltaforge_replay_captured_total",
            "pipeline" => self.pipeline_name.to_string(),
        )
        .increment(1);
        *last_captured_boundary = Some(boundary_rec);
        accum.clear();
        Ok(())
    }

    /// Get access to schema sensor state (for API exposure).
    pub fn schema_sensor(&self) -> Option<&Arc<SchemaSensorState>> {
        self.schema_sensor.as_ref()
    }

    fn effective(cfg: &Option<BatchConfig>) -> BatchConfig {
        let defaults = BatchConfig::default();
        match cfg {
            Some(c) => BatchConfig {
                max_events: c.max_events.or(defaults.max_events),
                max_bytes: c.max_bytes.or(defaults.max_bytes),
                max_ms: c.max_ms.or(defaults.max_ms),
                respect_source_tx: c
                    .respect_source_tx
                    .or(defaults.respect_source_tx),
                max_inflight: c.max_inflight.or(defaults.max_inflight),
                max_tx_events: c.max_tx_events.or(defaults.max_tx_events),
                max_tx_bytes: c.max_tx_bytes.or(defaults.max_tx_bytes),
                oversized_tx: c.oversized_tx.or(defaults.oversized_tx),
            },
            None => defaults,
        }
    }

    /// Fetch and cache schema for a table from the provider.
    async fn get_or_fetch_schema(
        &self,
        table: &str,
    ) -> Option<TableSchemaInfo> {
        // Check cache first
        {
            let cache = self.db_schema_cache.lock();
            if let Some(schema) = cache.get(table) {
                return Some(schema.clone());
            }
        }

        // Fetch from provider
        let provider = self.schema_provider.as_ref()?;
        let schema = provider.get_table_schema(table).await?;

        // Register with drift detector
        if let Some(ref sensor) = self.schema_sensor {
            sensor.register_table_schema(schema.clone());
        }

        // Cache it
        {
            let mut cache = self.db_schema_cache.lock();
            cache.insert(table.to_string(), schema.clone());
        }

        Some(schema)
    }

    /// Build - process (mutable) - sense - freeze (Arc<[Event]>) - deliver - maybe commit.
    ///
    /// When `max_inflight > 1`, accumulation and delivery run concurrently:
    /// the accumulation loop sends completed batches through a bounded channel
    /// to a delivery task, so the next batch starts filling while the previous
    /// one is being delivered to sinks. The delivery task processes batches in
    /// FIFO order, preserving checkpoint ordering.
    pub async fn run(
        self,
        mut event_rx: tokio::sync::mpsc::Receiver<SourceItem>,
        cancel: CancellationToken,
        mut pause_rx: watch::Receiver<bool>,
    ) -> Result<()> {
        let tick_ms = self.batch_cfg_eff.max_ms.unwrap_or(200);
        let max_events = self.batch_cfg_eff.max_events.unwrap_or(usize::MAX);
        let max_bytes = self.batch_cfg_eff.max_bytes.unwrap_or(usize::MAX);
        let max_inflight = self.batch_cfg_eff.max_inflight.unwrap_or(1);
        let respect_source_tx =
            self.batch_cfg_eff.respect_source_tx.unwrap_or(false);
        let max_tx_events =
            self.batch_cfg_eff.max_tx_events.unwrap_or(usize::MAX);
        let max_tx_bytes =
            self.batch_cfg_eff.max_tx_bytes.unwrap_or(usize::MAX);

        // Transaction-aligned batching is single-in-flight for the first cut: a
        // second concurrent delivery could flush part of a transaction before
        // its commit marker arrives, breaking the all-or-nothing boundary. Fail
        // fast at startup rather than silently splitting transactions.
        if respect_source_tx && max_inflight != 1 {
            return Err(anyhow::anyhow!(
                "respect_source_tx requires max_inflight = 1 (got {max_inflight}); \
                 transaction-aligned batching does not yet support concurrent \
                 in-flight batches"
            ));
        }

        // Replay capture derives commit-unit boundaries from transaction markers, so
        // it requires transaction-aligned batching. With respect_source_tx = false the
        // legacy path ignores tx markers and would capture nothing; fail fast at
        // startup rather than run a replay-enabled pipeline that silently journals no
        // commit units.
        if self.replay_capture.is_some() && !respect_source_tx {
            return Err(anyhow::anyhow!(
                "replay capture requires respect_source_tx = true; a replay-enabled \
                 pipeline cannot run with transaction-aligned batching disabled"
            ));
        }

        let coord = Arc::new(self);

        // Bounded channel for pipelined delivery - capacity = max_inflight.
        // When max_inflight=1 this degrades gracefully to back-pressure after
        // every batch (same throughput as the old sequential path).
        let (deliver_tx, mut deliver_rx) =
            tokio::sync::mpsc::channel::<DeliveryItem>(max_inflight.max(1));

        // Shared error slot: the delivery task writes here on failure.
        let delivery_error: Arc<Mutex<Option<anyhow::Error>>> =
            Arc::new(Mutex::new(None));

        // Spawn delivery task - processes batches in FIFO order so checkpoints
        // are committed in sequence.
        let d_coord = Arc::clone(&coord);
        let d_cancel = cancel.clone();
        let d_error = Arc::clone(&delivery_error);
        let delivery_handle = tokio::spawn(async move {
            while let Some(item) = deliver_rx.recv().await {
                if let Err(e) = d_coord
                    .process_deliver_and_maybe_commit(item.batch, item.reason)
                    .await
                {
                    *d_error.lock() = Some(e);
                    d_cancel.cancel();
                    break;
                }
            }
        });

        info!(
            pipeline = %coord.pipeline_name,
            max_inflight,
            max_events,
            tick_ms,
            "coordinator started"
        );

        // ── Accumulation loop ────────────────────────────────────────────
        let mut ticker = interval(Duration::from_millis(tick_ms));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut building: Option<BuildingBatch> = None;
        let mut drain_buf: Vec<SourceItem> = Vec::with_capacity(256);
        let mut tx_tracker = TxTracker::default();
        // Replay capture accumulator: raw pre-processing events for the currently-open
        // commit unit. Separate from batching, so it spans soft-limit flushes; closed at
        // a boundary, discarded (dropped) on cancel/shutdown/error before a boundary.
        let replay_on = coord.replay_capture.is_some();
        let mut capture_accum: Vec<Event> = Vec::new();
        // Full boundary record of the last commit unit actually captured. Used to
        // distinguish a genuine data-less boundary (worth capturing) from a redundant
        // snapshot-completion marker that repeats the already-captured boundary.
        let mut last_captured_boundary: Option<
            deltaforge_core::replay::SourceBoundaryRecord,
        > = None;

        let accum_result: Result<()> = async {
            loop {
                if *pause_rx.borrow() {
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        changed = pause_rx.changed() => {
                            if changed.is_err() { break; }
                            continue;
                        }
                    }
                }

                tokio::select! {
                    _ = cancel.cancelled() => {
                        if let Some(b) = building.take() {
                            if let Some(b) = finalize_batch(
                                b, respect_source_tx, &coord.pipeline_name,
                            ) {
                                send_to_delivery(&deliver_tx, b, "cancelled").await?;
                            }
                        }
                        break;
                    }

                    _ = ticker.tick() => {
                        if let Some(b) = building.take() {
                            let elapsed = b.started_at.elapsed()
                                >= Duration::from_millis(tick_ms);
                            // In tx-aligned mode only whole transactions may
                            // flush - never mid-transaction.
                            let flushable = if respect_source_tx {
                                b.committed_len > 0 && !b.mid_tx()
                            } else {
                                !b.raw.is_empty()
                            };
                            if flushable && elapsed {
                                send_to_delivery(&deliver_tx, b, "timer").await?;
                            } else {
                                building = Some(b);
                            }
                        }
                    }

                    changed = pause_rx.changed() => {
                        if changed.is_err() { break; }
                        continue;
                    }

                    maybe_ev = event_rx.recv() => {
                        let Some(first) = maybe_ev else {
                            if let Some(b) = building.take() {
                                if let Some(b) = finalize_batch(
                                    b, respect_source_tx, &coord.pipeline_name,
                                ) {
                                    send_to_delivery(&deliver_tx, b, "shutdown").await?;
                                }
                            }
                            break;
                        };

                        if building.is_none() {
                            building = Some(BuildingBatch::with_capacity(max_events));
                            debug!(pipeline=%coord.pipeline_name, "building new batch");
                        }
                        let mut b = building.take().unwrap();

                        // Gather the first item plus an immediately-available
                        // burst. The timeout bounds the wait so an idle source
                        // at the WAL tail lets the ticker branch flush.
                        drain_buf.clear();
                        drain_buf.push(first);
                        let _ = tokio::time::timeout(
                            Duration::from_millis(tick_ms),
                            event_rx.recv_many(&mut drain_buf, 512),
                        )
                        .await;

                        for item in drain_buf.drain(..) {
                            if respect_source_tx {
                                match item {
                                    SourceItem::TxBegin { tx_id } => {
                                        // Open the transaction; a second open
                                        // before a commit is a protocol error.
                                        tx_tracker.begin(&tx_id)?;
                                    }
                                    SourceItem::Event(ev) => {
                                        // Every transactional event must belong
                                        // to the open transaction.
                                        tx_tracker.observe_event(&ev)?;
                                        if ev.transaction.is_some() {
                                            // Accumulate the raw event for replay before
                                            // it is moved into the batch.
                                            if replay_on {
                                                capture_accum.push(ev.clone());
                                            }
                                            // In-transaction event: buffer without
                                            // splitting; enforce the hard caps.
                                            if let Err(o) = push_tx_event(
                                                &mut b, ev, max_tx_events, max_tx_bytes,
                                            ) {
                                                counter!(
                                                    "deltaforge_oversized_tx_total",
                                                    "pipeline" => coord.pipeline_name.to_string(),
                                                )
                                                .increment(1);
                                                return Err(OversizedTxError {
                                                    tx_id: tx_tracker
                                                        .active()
                                                        .unwrap_or("<unknown>")
                                                        .to_string(),
                                                    observed_events: o.events,
                                                    observed_bytes: o.bytes,
                                                    max_tx_events,
                                                    max_tx_bytes,
                                                    exceeded: o.exceeded,
                                                }
                                                .into());
                                            }
                                            maybe_warn_approaching(
                                                &mut b,
                                                max_tx_events,
                                                max_tx_bytes,
                                                &coord.pipeline_name,
                                            );
                                        } else {
                                            // Standalone/snapshot event: its own
                                            // boundary; split by soft limits. Accumulate
                                            // the raw event and note whether it carries a
                                            // boundary (snapshot chunk-final rows do; the
                                            // rest accumulate across soft-limit flushes).
                                            let ev_boundary = if replay_on {
                                                ev.boundary.clone()
                                            } else {
                                                None
                                            };
                                            if replay_on {
                                                capture_accum.push(ev.clone());
                                            }
                                            b.bytes += event_size_hint(&ev);
                                            b.raw.push(ev);
                                            commit_standalone(&mut b);
                                            // Close the replay unit at the boundary event,
                                            // before any flush that could commit its
                                            // checkpoint (fail-closed).
                                            if let Some(bd) = ev_boundary {
                                                coord
                                                    .close_capture_unit(
                                                        &mut capture_accum,
                                                        &bd,
                                                        &mut last_captured_boundary,
                                                    )
                                                    .await?;
                                            }
                                            if soft_limit_reached(&b, max_events, max_bytes) {
                                                let full = std::mem::replace(
                                                    &mut b,
                                                    BuildingBatch::with_capacity(max_events),
                                                );
                                                send_to_delivery(&deliver_tx, full, "limits").await?;
                                            }
                                        }
                                    }
                                    SourceItem::TxCommit { tx_id, boundary } => {
                                        // The marker must match the open tx and
                                        // may close it exactly once - a stale or
                                        // duplicate marker cannot advance twice.
                                        tx_tracker.commit(&tx_id)?;
                                        // Commit boundary: close the open tx with
                                        // the boundary's checkpoint + watermark
                                        // (atomic), flush only if soft limits are
                                        // reached.
                                        close_tx(&mut b, boundary.clone());
                                        // Persist the replay envelope for the whole tx
                                        // before any flush can commit its checkpoint
                                        // (fail-closed).
                                        coord
                                            .close_capture_unit(
                                                &mut capture_accum,
                                                &boundary,
                                                &mut last_captured_boundary,
                                            )
                                            .await?;
                                        if soft_limit_reached(&b, max_events, max_bytes) {
                                            let full = std::mem::replace(
                                                &mut b,
                                                BuildingBatch::with_capacity(max_events),
                                            );
                                            send_to_delivery(&deliver_tx, full, "tx_commit").await?;
                                        }
                                    }
                                    SourceItem::Boundary { boundary } => {
                                        // A data-less boundary (snapshot table /
                                        // full completion). Apply it to the pending
                                        // batch and flush now so it is delivered -
                                        // and durably acked - promptly, even when
                                        // the batch is empty because prior data
                                        // already flushed (the sink then publishes a
                                        // zero-object entry + HEAD CAS for it).
                                        // Close any accumulated commit unit at this
                                        // data-less boundary before the flush commits its
                                        // checkpoint (fail-closed). Usually empty (a
                                        // chunk-final event already closed the unit).
                                        coord
                                            .close_capture_unit(
                                                &mut capture_accum,
                                                &boundary,
                                                &mut last_captured_boundary,
                                            )
                                            .await?;
                                        close_boundary(&mut b, boundary);
                                        let full = std::mem::replace(
                                            &mut b,
                                            BuildingBatch::with_capacity(max_events),
                                        );
                                        send_to_delivery(&deliver_tx, full, "boundary").await?;
                                    }
                                }
                            } else {
                                // Legacy path: soft-limit splitting; markers ignored.
                                let SourceItem::Event(ev) = item else { continue; };
                                if let Some(full) = check_and_split(&mut b, ev, max_events, max_bytes) {
                                    send_to_delivery(&deliver_tx, full, "limits").await?;
                                }
                            }
                        }

                        building = Some(b);
                    }
                }
            }
            Ok(())
        }
        .await;

        // Signal delivery task to drain remaining items and stop.
        drop(deliver_tx);
        let _ = delivery_handle.await;

        // Propagate delivery errors (take precedence over accumulation errors).
        if let Some(err) = delivery_error.lock().take() {
            return Err(err);
        }

        accum_result
    }

    async fn process_deliver_and_maybe_commit(
        &self,
        mut b: BuildingBatch,
        reason: &str,
    ) -> Result<()> {
        // 1) PROCESS: processors can modify/duplicate/drop events
        let proc_start = Instant::now();
        let processed =
            match (self.process_batch)(std::mem::take(&mut b.raw)).await {
                Ok(p) => p,
                Err(e) => {
                    warn!(
                        pipeline=%self.pipeline_name,
                        error=%e,
                        reason=%reason,
                        "processor failed, NOT saving checkpoint"
                    );
                    return Err(e).context("process batch");
                }
            };

        histogram!(
            "deltaforge_stage_latency_seconds",
            "pipeline" => self.pipeline_name.to_string(),
            "stage" => "process",
            "trigger" => reason.to_string()
        )
        .record(proc_start.elapsed().as_secs_f64());

        debug!(
            pipeline=%self.pipeline_name,
            processed_count=%processed.events.len(),
            "received events from processors"
        );

        let last_cp = processed.last_checkpoint;

        // 2) SCHEMA SENSING: observe and enrich events
        let mut events = processed.events;
        if let Some(ref sensor_state) = self.schema_sensor {
            let sense_start = Instant::now();

            // Build schema map for guided sensing (if we have a provider)
            let db_schemas = if self.schema_provider.is_some() {
                // Collect unique table names in this batch
                let tables: std::collections::HashSet<&str> =
                    events.iter().map(|e| e.source.table.as_str()).collect();

                // Fetch schemas for each table
                let mut schema_map = HashMap::new();
                for table in tables {
                    if let Some(schema) = self.get_or_fetch_schema(table).await
                    {
                        schema_map.insert(table.to_string(), schema);
                    }
                }

                if schema_map.is_empty() {
                    None
                } else {
                    Some(schema_map)
                }
            } else {
                None
            };

            // Run sensing (guided if we have schemas, full if not)
            let evolutions = sensor_state
                .observe_and_enrich(&mut events, db_schemas.as_ref());

            histogram!(
                "deltaforge_stage_latency_seconds",
                "pipeline" => self.pipeline_name.to_string(),
                "stage" => "schema_sensing",
                "trigger" => reason.to_string()
            )
            .record(sense_start.elapsed().as_secs_f64());

            if evolutions > 0 {
                counter!(
                    "deltaforge_pipeline_evolutions_total",
                    "pipeline" => self.pipeline_name.to_string()
                )
                .increment(evolutions as u64);
            }

            // Check for drift and emit warning metric
            if sensor_state.has_drift() {
                counter!(
                    "deltaforge_schema_drift_detected",
                    "pipeline" => self.pipeline_name.to_string()
                )
                .increment(1);
            }
        }

        // Recompute size after processing
        let bytes: usize = events.iter().map(event_size_hint).sum();

        // Replication lag and e2e latency relative to wall-clock.
        if let Some(last_ts_ms) = events.last().map(|e| e.ts_ms) {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            let lag_secs = ((now_ms - last_ts_ms).max(0) as f64) / 1000.0;
            gauge!(
                "deltaforge_source_lag_seconds",
                "pipeline" => self.pipeline_name.to_string()
            )
            .set(lag_secs);

            // Per-table lag: track the last event timestamp per table in this batch.
            let mut table_lag: HashMap<String, f64> = HashMap::new();
            for ev in &events {
                let table_key = format!("{}.{}", ev.source.db, ev.source.table);
                let ev_lag = ((now_ms - ev.ts_ms).max(0) as f64) / 1000.0;
                table_lag.insert(table_key, ev_lag);
            }
            for (table, lag) in &table_lag {
                gauge!(
                    "deltaforge_source_table_lag_seconds",
                    "pipeline" => self.pipeline_name.to_string(),
                    "table" => table.clone(),
                )
                .set(*lag);
            }

            // E2E latency: first event's pipeline-receive time → now (before sink
            // delivery). Uses received_at_ms (wall-clock at parse time) rather than
            // ts_ms (binlog header timestamp, second-precision) so the metric reflects
            // actual pipeline processing time, not source clock granularity.
            let first_received_ms = events
                .first()
                .map(|e| e.received_at_ms)
                .filter(|&t| t > 0)
                .unwrap_or(last_ts_ms);
            histogram!(
                "deltaforge_e2e_latency_seconds",
                "pipeline" => self.pipeline_name.to_string(),
            )
            .record(((now_ms - first_received_ms).max(0) as f64) / 1000.0);
        }

        histogram!(
            "deltaforge_batch_events",
            "pipeline" => self.pipeline_name.to_string(),
        )
        .record(events.len() as f64);

        histogram!(
            "deltaforge_batch_bytes",
            "pipeline" => self.pipeline_name.to_string(),
        )
        .record(bytes as f64);

        counter!(
            "deltaforge_bytes_total",
            "pipeline" => self.pipeline_name.to_string(),
        )
        .increment(bytes as u64);

        // 3) FREEZE for zero-copy sharing
        let frozen = FrozenBatch {
            id: Uuid::now_v7(),
            bytes,
            events: Arc::<[Event]>::from(events),
        };

        // 4) DELIVER to all sinks concurrently
        //
        // Each sink gets the same Arc<[Event]> - no cloning of event data.
        // We drive all futures simultaneously and collect per-sink outcomes,
        // then fold the results into ack counters
        let required_total =
            self.sinks.iter().filter(|s| is_sink_required(s)).count();

        debug!(
            pipeline=%self.pipeline_name,
            sink_count=self.sinks.len(),
            event_count=frozen.events.len(),
            "sending batch to sink(s) concurrently"
        );

        // Build one future per sink, tagged with its id and required flag.
        // Each future is optionally wrapped in `tokio::time::timeout` using
        // the coordinator-level `sink_batch_deadline` (see
        // `Spec::sink_batch_deadline_secs`). A timeout becomes
        // `SinkError::Backpressure` for that sink, routed per `required`.
        let deadline = self.sink_batch_deadline;
        let pipeline_name_str: String = self.pipeline_name.to_string();

        // The delivery context comes entirely from the batch's source boundary
        // (captured at the commit marker, before processing, so it survives
        // filtering). Checkpoint and watermark are the SAME source state - the
        // single watermark authority; nothing re-derives a watermark here.
        let ctx = Arc::new(deltaforge_core::SinkBatchContext {
            checkpoint: b
                .boundary
                .as_ref()
                .map(|bd| bd.checkpoint.clone())
                .unwrap_or_else(|| CheckpointMeta::from_vec(Vec::new())),
            durable_watermark: b
                .boundary
                .as_ref()
                .and_then(|bd| bd.durable_watermark.as_ref())
                .map(|w| w.to_vec()),
            batch_id: None,
        });

        let sink_futs = self.sinks.iter().map(|sink| {
            let start = Instant::now();
            let events = Arc::clone(&frozen.events);
            let required = is_sink_required(sink);
            let sink_id = sink.id().to_string();
            let pipeline = pipeline_name_str.clone();
            let ctx = Arc::clone(&ctx);

            async move {
                let result = match deadline {
                    Some(d) => {
                        match tokio::time::timeout(
                            d,
                            sink.send_batch_with_context(&events, &ctx),
                        )
                        .await
                        {
                            Ok(r) => r,
                            Err(_elapsed) => {
                                counter!(
                                    "deltaforge_coordinator_sink_timeout_total",
                                    "pipeline" => pipeline.clone(),
                                    "sink" => sink_id.clone(),
                                )
                                .increment(1);
                                tracing::warn!(
                                    pipeline = %pipeline,
                                    sink = %sink_id,
                                    deadline_secs = d.as_secs(),
                                    "sink exceeded coordinator deadline"
                                );
                                Err(SinkError::Backpressure {
                                    details: format!(
                                        "sink exceeded coordinator deadline of {}s",
                                        d.as_secs()
                                    )
                                    .into(),
                                })
                            }
                        }
                    }
                    None => sink.send_batch_with_context(&events, &ctx).await,
                };
                (sink_id, required, start.elapsed(), result)
            }
        });

        let raw_outcomes = futures::future::join_all(sink_futs).await;

        let mut required_acks = 0usize;
        let mut total_acks = 0usize;

        // Collect per-sink success/failure for checkpoint commits.
        // (sink_id, required, succeeded)
        let mut sink_results: Vec<(String, bool, bool)> =
            Vec::with_capacity(raw_outcomes.len());

        for (sink_id, required, elapsed, result) in raw_outcomes {
            match result {
                Ok(batch_result) => {
                    // Route per-event DLQ failures if a DLQ writer is configured.
                    if !batch_result.dlq_failures.is_empty() {
                        if let Some(dlq) = &self.dlq_writer {
                            for &(idx, ref err) in &batch_result.dlq_failures {
                                if idx < frozen.events.len() {
                                    dlq.write(
                                        &frozen.events[idx],
                                        &sink_id,
                                        err,
                                    )
                                    .await;
                                }
                            }
                        }
                    }

                    let delivered =
                        frozen.events.len() - batch_result.dlq_failures.len();

                    total_acks += 1;
                    if required {
                        required_acks += 1;
                    }
                    sink_results.push((sink_id.clone(), required, true));

                    counter!(
                        "deltaforge_sink_batch_total",
                        "pipeline" => self.pipeline_name.to_string(),
                        "sink" => sink_id.clone()
                    )
                    .increment(1);

                    counter!(
                        "deltaforge_sink_events_total",
                        "pipeline" => self.pipeline_name.to_string(),
                        "sink" => sink_id.clone()
                    )
                    .increment(delivered as u64);

                    histogram!(
                        "deltaforge_sink_latency_seconds",
                        "pipeline" => self.pipeline_name.to_string(),
                        "sink" => sink_id
                    )
                    .record(elapsed.as_secs_f64());
                }
                Err(e) => {
                    sink_results.push((sink_id.clone(), required, false));

                    counter!(
                        "deltaforge_sink_errors_total",
                        "pipeline" => self.pipeline_name.to_string(),
                        "sink" => sink_id.clone()
                    )
                    .increment(1);

                    warn!(
                        pipeline=%self.pipeline_name,
                        sink=%sink_id,
                        error=%e,
                        "sink delivery failed"
                    );
                }
            }
        }
        // 5) COMMIT per-sink checkpoints.
        // Emit per-sink delivery status gauges (always, regardless of policy).
        for (sink_id, required, succeeded) in &sink_results {
            gauge!(
                "deltaforge_sink_checkpoint_status",
                "pipeline" => self.pipeline_name.to_string(),
                "sink" => sink_id.clone(),
                "required" => if *required { "true" } else { "false" },
            )
            .set(if *succeeded { 1.0 } else { 0.0 });
        }

        // Policy gate - check BEFORE committing any checkpoints so that a
        // failed required sink never leaves optional sinks with advanced
        // checkpoints while the required sink is behind.
        if !policy_satisfied(
            &self.commit_policy,
            self.sinks.len(),
            required_total,
            required_acks,
            total_acks,
        ) {
            anyhow::bail!(
                "commit policy not satisfied: required {required_acks}/{required_total} acks, \
                 total {total_acks}"
            );
        }

        //
        // Per-sink checkpoint commit - only reached when the commit policy is
        // satisfied. Each sink that successfully delivered the batch gets its
        // own checkpoint committed independently. Failed sinks do not advance.
        // Commits run concurrently to avoid serializing latency across sinks.
        if let Some(cp) = last_cp {
            let commit_start = Instant::now();

            // Build futures for all successful sinks that have a commit fn.
            let commit_futs: Vec<_> = sink_results
                .iter()
                .filter(|(_, _, succeeded)| *succeeded)
                .filter_map(|(sink_id, _, _)| {
                    if let Some(commit_fn) = self.commit_cp_per_sink.get(sink_id.as_str()) {
                        Some((sink_id.clone(), (commit_fn)(cp.clone())))
                    } else {
                        warn!(
                            pipeline=%self.pipeline_name,
                            sink=%sink_id,
                            "sink delivered successfully but has no checkpoint commit function"
                        );
                        None
                    }
                })
                .collect();

            let results = join_all(
                commit_futs
                    .into_iter()
                    .map(|(sink_id, fut)| async move { (sink_id, fut.await) }),
            )
            .await;

            let mut committed = 0u64;
            let mut first_err: Option<anyhow::Error> = None;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs_f64())
                .unwrap_or(0.0);

            for (sink_id, result) in results {
                match result {
                    Ok(()) => {
                        committed += 1;
                        gauge!(
                            "deltaforge_sink_last_checkpoint_ts",
                            "pipeline" => self.pipeline_name.to_string(),
                            "sink" => sink_id,
                        )
                        .set(now);
                    }
                    Err(e) => {
                        warn!(
                            pipeline=%self.pipeline_name,
                            sink=%sink_id,
                            error=%e,
                            "per-sink checkpoint commit failed"
                        );
                        if first_err.is_none() {
                            first_err = Some(e);
                        }
                    }
                }
            }

            if let Some(e) = first_err {
                return Err(e).context("commit checkpoint");
            }

            if committed > 0 {
                histogram!(
                    "deltaforge_stage_latency_seconds",
                    "pipeline" => self.pipeline_name.to_string(),
                    "stage" => "commit",
                    "trigger" => reason.to_string()
                )
                .record(commit_start.elapsed().as_secs_f64());

                counter!(
                    "deltaforge_checkpoints_total",
                    "pipeline" => self.pipeline_name.to_string()
                )
                .increment(committed);

                gauge!(
                    "deltaforge_last_checkpoint_ts",
                    "pipeline" => self.pipeline_name.to_string()
                )
                .set(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs_f64())
                        .unwrap_or(0.0),
                );
            }
        }

        Ok(())
    }
}

/// Build the commit function for checkpoints.
pub fn build_commit_fn(
    store: Arc<dyn CheckpointStore>,
    key: String,
) -> CommitCpFn<CheckpointMeta> {
    use futures::FutureExt;

    Box::new(move |cp: CheckpointMeta| {
        let store = Arc::clone(&store);
        let key = key.clone();
        async move {
            store.put_raw(&key, cp.as_bytes()).await?;
            debug!(checkpoint_key=%key, bytes=cp.as_bytes().len(), "checkpoint saved");
            Ok(())
        }
        .boxed()
    })
}

/// Build the batch processor function.
pub fn build_batch_processor(
    processors: Arc<[ArcDynProcessor]>,
    pipeline: String,
) -> ProcessBatchFn<CheckpointMeta> {
    use futures::FutureExt;

    let pipeline_name: Arc<str> = pipeline.into();

    Arc::new(move |events: Vec<Event>| {
        let procs = Arc::clone(&processors);
        let pipeline = Arc::clone(&pipeline_name);

        async move {
            let last_cp =
                events.iter().rev().find_map(|e| e.checkpoint()).cloned();

            if procs.is_empty() {
                return Ok(ProcessedBatch {
                    events,
                    last_checkpoint: last_cp,
                });
            }

            // snapshot IDs once before any processor runs
            let ctx = BatchContext::from_batch(&events);

            let mut batch = events;
            for p in procs.iter() {
                let pid = p.id();
                let start = Instant::now();
                batch = match p.process(batch, &ctx).await {
                    Ok(b) => b,
                    Err(e) => {
                        counter!(
                            "deltaforge_processor_errors_total",
                            "pipeline" => pipeline.to_string(),
                            "processor" => pid.to_string(),
                        )
                        .increment(1);
                        return Err(e).with_context(|| {
                            format!("processor {pid} failed")
                        });
                    }
                };

                histogram!(
                    "deltaforge_processor_latency_seconds",
                    "pipeline" => pipeline.to_string(),
                    "processor" => pid.to_string(),
                )
                .record(start.elapsed().as_secs_f64());
            }

            Ok(ProcessedBatch {
                events: batch,
                last_checkpoint: last_cp,
            })
        }
        .boxed()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltaforge_core::{CheckpointMeta, SinkError, SinkResult};
    use std::sync::atomic::{AtomicBool, AtomicUsize};

    #[test]
    fn test_effective_batch_config_defaults() {
        let eff = Coordinator::<CheckpointMeta>::effective(&None);
        assert!(eff.max_events.is_some());
        assert!(eff.max_bytes.is_some());
        assert!(eff.max_ms.is_some());
    }

    #[test]
    fn test_effective_batch_config_override() {
        let cfg = Some(BatchConfig {
            max_events: Some(500),
            max_bytes: None,
            max_ms: Some(100),
            respect_source_tx: None,
            max_inflight: None,
            ..BatchConfig::default()
        });

        let eff = Coordinator::<CheckpointMeta>::effective(&cfg);
        assert_eq!(eff.max_events, Some(500));
        assert_eq!(eff.max_ms, Some(100));
        assert!(eff.max_bytes.is_some());
    }

    /// Transaction-aligned batching does not yet support more than one in-flight
    /// batch: a concurrent delivery task could flush half a transaction before
    /// the commit marker arrives. `run` must reject that combination at startup.
    #[tokio::test]
    async fn respect_source_tx_requires_max_inflight_one() {
        use checkpoints::MemCheckpointStore;

        let store = Arc::new(MemCheckpointStore::new().unwrap());
        let sink = MockSink::new("kafka", true);
        let sinks: Vec<ArcDynSink> = vec![Arc::clone(&sink) as ArcDynSink];
        let cp_fn =
            build_commit_fn(store.clone(), "src::sink::kafka".to_string());
        let processors: Arc<[deltaforge_core::ArcDynProcessor]> =
            Arc::from(vec![]);
        let batch_processor =
            build_batch_processor(processors, "test".to_string());

        let coord = Coordinator::builder("test-validate")
            .sinks(sinks)
            .batch_config(Some(BatchConfig {
                respect_source_tx: Some(true),
                max_inflight: Some(2),
                ..BatchConfig::default()
            }))
            .commit_fn("kafka", cp_fn)
            .process_fn(batch_processor)
            .build();

        let (tx, rx) = tokio::sync::mpsc::channel(64);
        drop(tx); // Closed channel: without validation, run() would exit Ok(()).
        let cancel = tokio_util::sync::CancellationToken::new();
        let (_pause_tx, pause_rx) = tokio::sync::watch::channel(false);

        let err = coord
            .run(rx, cancel, pause_rx)
            .await
            .expect_err("must reject max_inflight > 1 with respect_source_tx");
        let msg = err.to_string();
        assert!(
            msg.contains("respect_source_tx") && msg.contains("max_inflight"),
            "error should explain the constraint, got: {msg}"
        );
    }

    // ── Transaction-aligned batching (respect_source_tx) ─────────────────

    /// Build an in-transaction row event carrying its own per-event checkpoint.
    fn tx_event(id: i64, tx_id: &str, checkpoint: &[u8]) -> Event {
        let source = deltaforge_core::SourceInfo {
            version: "test".into(),
            connector: "mysql".into(),
            name: "test".into(),
            db: "db".into(),
            schema: None,
            table: "t".into(),
            ts_ms: 0,
            snapshot: None,
            position: deltaforge_core::SourcePosition::default(),
        };
        let mut e = Event::new_row(
            deltaforge_core::EventId::mysql_row_server(1, "t", 1, id as u32),
            source,
            deltaforge_core::Op::Create,
            None,
            Some(serde_json::json!({ "id": id })),
            0,
            10,
        );
        e.transaction = Some(deltaforge_core::Transaction {
            id: tx_id.to_string(),
            total_order: None,
            data_collection_order: None,
        });
        e.set_checkpoint(CheckpointMeta::from_vec(checkpoint.to_vec()));
        e
    }

    /// Build a coordinator wired to one `MockSink` with the given batch config.
    fn tx_coord(
        store: Arc<checkpoints::MemCheckpointStore>,
        sink: Arc<MockSink>,
        cfg: BatchConfig,
    ) -> Coordinator<CheckpointMeta> {
        let sinks: Vec<ArcDynSink> = vec![Arc::clone(&sink) as ArcDynSink];
        let cp_fn = build_commit_fn(store, "src::sink::kafka".to_string());
        let processors: Arc<[deltaforge_core::ArcDynProcessor]> =
            Arc::from(vec![]);
        let batch_processor =
            build_batch_processor(processors, "test".to_string());
        Coordinator::builder("tx-test")
            .sinks(sinks)
            .batch_config(Some(cfg))
            .commit_fn("kafka", cp_fn)
            .process_fn(batch_processor)
            .build()
    }

    /// Like `tx_coord` but with replay capture wired to `journal`.
    fn cap_coord(
        store: Arc<checkpoints::MemCheckpointStore>,
        sink: Arc<MockSink>,
        cfg: BatchConfig,
        journal: Arc<dyn crate::replay_journal::JournalLog>,
        max_envelope_bytes: usize,
    ) -> Coordinator<CheckpointMeta> {
        let sinks: Vec<ArcDynSink> = vec![Arc::clone(&sink) as ArcDynSink];
        let cp_fn = build_commit_fn(store, "src::sink::kafka".to_string());
        let processors: Arc<[deltaforge_core::ArcDynProcessor]> =
            Arc::from(vec![]);
        let batch_processor =
            build_batch_processor(processors, "test".to_string());
        let capture = ReplayCapture {
            journal,
            identity: deltaforge_core::replay::PipelineIdentity {
                pipeline: "p".into(),
                incarnation: "inc-1".into(),
                source_lineage: None,
            },
            max_envelope_bytes,
            registry_seq_fn: Arc::new(|| None),
        };
        Coordinator::builder("cap-test")
            .sinks(sinks)
            .batch_config(Some(cfg))
            .commit_fn("kafka", cp_fn)
            .process_fn(batch_processor)
            .replay_capture(capture)
            .build()
    }

    /// A standalone (non-transactional) event, carrying a boundary only when
    /// `boundary_cp` is `Some` (snapshot chunk-final rows do; the rest do not).
    fn snap_event(id: i64, boundary_cp: Option<&[u8]>) -> Event {
        let mut e = tx_event(id, "ignored", b"");
        e.transaction = None;
        e.boundary = boundary_cp.map(|cp| {
            deltaforge_core::SourceBoundary::checkpoint_only(
                CheckpointMeta::from_vec(cp.to_vec()),
            )
        });
        e
    }

    fn cap_cfg(max_events: usize) -> BatchConfig {
        BatchConfig {
            max_events: Some(max_events),
            max_ms: Some(60_000),
            respect_source_tx: Some(true),
            max_inflight: Some(1),
            ..BatchConfig::default()
        }
    }

    /// A journal that always fails, to prove fail-closed capture.
    struct FailingJournal;
    #[async_trait::async_trait]
    impl crate::replay_journal::JournalLog for FailingJournal {
        async fn append(
            &self,
            _p: &deltaforge_core::replay::ReplayEnvelopePayload,
        ) -> Result<storage::LogAppendOutcome> {
            anyhow::bail!("journal unavailable")
        }
        async fn read_since(
            &self,
            _f: u64,
            _l: usize,
        ) -> Result<Vec<deltaforge_core::replay::StoredReplayEnvelope>>
        {
            Ok(vec![])
        }
        async fn stream_meta(&self) -> Result<storage::LogStreamMeta> {
            anyhow::bail!("n/a")
        }
        async fn truncate(
            &self,
            _r: storage::LogTruncateRequest,
        ) -> Result<storage::LogTruncateOutcome> {
            anyhow::bail!("n/a")
        }
    }

    async fn feed_and_run(
        coord: Coordinator<CheckpointMeta>,
        items: Vec<SourceItem>,
    ) -> Result<()> {
        let (tx, rx) = tokio::sync::mpsc::channel(256);
        let cancel = tokio_util::sync::CancellationToken::new();
        let (_pause_tx, pause_rx) = tokio::sync::watch::channel(false);
        for item in items {
            tx.send(item).await.unwrap();
        }
        drop(tx);
        coord.run(rx, cancel, pause_rx).await
    }

    /// A snapshot chunk whose rows flush across several coordinator batches is
    /// captured as ONE envelope, closed by the chunk-final boundary.
    #[tokio::test]
    async fn snapshot_chunk_across_batches_is_one_envelope() {
        use checkpoints::MemCheckpointStore;
        let store = Arc::new(MemCheckpointStore::new().unwrap());
        let sink = MockSink::new("kafka", true);
        let be: storage::ArcStorageBackend =
            Arc::new(storage::MemoryStorageBackend::new());
        let journal = Arc::new(crate::replay_journal::BackendJournalLog::new(
            be,
            deltaforge_core::replay::PipelineIdentity {
                pipeline: "p".into(),
                incarnation: "inc-1".into(),
                source_lineage: None,
            },
        ));
        let coord = cap_coord(
            store,
            Arc::clone(&sink),
            cap_cfg(2), // small soft limit forces mid-chunk flushes
            journal.clone(),
            0,
        );

        let mut items = Vec::new();
        for i in 1..=4 {
            items.push(SourceItem::Event(snap_event(i, None)));
        }
        items.push(SourceItem::Event(snap_event(5, Some(b"chunk-cp"))));
        feed_and_run(coord, items).await.unwrap();

        let envs =
            crate::replay_journal::JournalLog::read_since(&*journal, 0, 100)
                .await
                .unwrap();
        assert_eq!(envs.len(), 1, "whole chunk is one envelope");
        assert_eq!(envs[0].payload.events.len(), 5);
        assert_eq!(
            envs[0].payload.boundary.checkpoint_hex,
            hex_of(b"chunk-cp"),
            "envelope boundary is the chunk-final checkpoint"
        );
    }

    fn hex_of(b: &[u8]) -> String {
        b.iter().map(|x| format!("{x:02x}")).collect()
    }

    /// A journal append failure at a commit boundary fails the pipeline and does not
    /// advance the checkpoint.
    #[tokio::test]
    async fn capture_failure_fails_closed_no_checkpoint() {
        use checkpoints::MemCheckpointStore;
        let store = Arc::new(MemCheckpointStore::new().unwrap());
        let sink = MockSink::new("kafka", true);
        let coord = cap_coord(
            store.clone(),
            sink,
            cap_cfg(1000),
            Arc::new(FailingJournal),
            0,
        );
        let res = feed_and_run(
            coord,
            vec![
                begin("g1"),
                SourceItem::Event(tx_event(1, "g1", b"row")),
                commit("g1", b"cp-1"),
            ],
        )
        .await;
        assert!(res.is_err(), "capture failure must fail the pipeline");
        assert!(
            store.get_raw("src::sink::kafka").await.unwrap().is_none(),
            "checkpoint must not advance on capture failure"
        );
    }

    /// A partial chunk with no boundary before shutdown captures nothing (the
    /// accumulator is discarded; the source replays the chunk on restart).
    #[tokio::test]
    async fn partial_chunk_without_boundary_captures_nothing() {
        let be: storage::ArcStorageBackend =
            Arc::new(storage::MemoryStorageBackend::new());
        let journal = Arc::new(crate::replay_journal::BackendJournalLog::new(
            be,
            deltaforge_core::replay::PipelineIdentity {
                pipeline: "p".into(),
                incarnation: "inc-1".into(),
                source_lineage: None,
            },
        ));
        let coord = cap_coord(
            Arc::new(checkpoints::MemCheckpointStore::new().unwrap()),
            MockSink::new("kafka", true),
            cap_cfg(1000),
            journal.clone(),
            0,
        );
        feed_and_run(
            coord,
            vec![
                SourceItem::Event(snap_event(1, None)),
                SourceItem::Event(snap_event(2, None)),
            ],
        )
        .await
        .unwrap();
        let envs =
            crate::replay_journal::JournalLog::read_since(&*journal, 0, 100)
                .await
                .unwrap();
        assert!(envs.is_empty(), "no boundary -> nothing captured");
    }

    /// Replaying the same chunk (e.g. after a restart before the boundary) produces a
    /// single idempotent envelope.
    #[tokio::test]
    async fn replaying_same_chunk_is_idempotent() {
        let be: storage::ArcStorageBackend =
            Arc::new(storage::MemoryStorageBackend::new());
        let chunk = || {
            vec![
                SourceItem::Event(snap_event(1, None)),
                SourceItem::Event(snap_event(2, None)),
                SourceItem::Event(snap_event(3, Some(b"chunk-cp"))),
            ]
        };
        for _ in 0..2 {
            let journal =
                Arc::new(crate::replay_journal::BackendJournalLog::new(
                    be.clone(),
                    deltaforge_core::replay::PipelineIdentity {
                        pipeline: "p".into(),
                        incarnation: "inc-1".into(),
                        source_lineage: None,
                    },
                ));
            let coord = cap_coord(
                Arc::new(checkpoints::MemCheckpointStore::new().unwrap()),
                MockSink::new("kafka", true),
                cap_cfg(1000),
                journal,
                0,
            );
            feed_and_run(coord, chunk()).await.unwrap();
        }
        let journal = crate::replay_journal::BackendJournalLog::new(
            be,
            deltaforge_core::replay::PipelineIdentity {
                pipeline: "p".into(),
                incarnation: "inc-1".into(),
                source_lineage: None,
            },
        );
        let envs =
            crate::replay_journal::JournalLog::read_since(&journal, 0, 100)
                .await
                .unwrap();
        assert_eq!(envs.len(), 1, "same chunk captured exactly once");
    }

    /// An envelope over the size cap fails closed (never split); the checkpoint does
    /// not advance.
    #[tokio::test]
    async fn oversized_envelope_fails_closed() {
        use checkpoints::MemCheckpointStore;
        let store = Arc::new(MemCheckpointStore::new().unwrap());
        let be: storage::ArcStorageBackend =
            Arc::new(storage::MemoryStorageBackend::new());
        let journal = Arc::new(crate::replay_journal::BackendJournalLog::new(
            be,
            deltaforge_core::replay::PipelineIdentity {
                pipeline: "p".into(),
                incarnation: "inc-1".into(),
                source_lineage: None,
            },
        ));
        let coord = cap_coord(
            store.clone(),
            MockSink::new("kafka", true),
            cap_cfg(1000),
            journal.clone(),
            10, // tiny cap: any real envelope exceeds it
        );
        let res = feed_and_run(
            coord,
            vec![
                begin("g1"),
                SourceItem::Event(tx_event(1, "g1", b"row")),
                commit("g1", b"cp-1"),
            ],
        )
        .await;
        let err = res.expect_err("oversized envelope must fail closed");
        assert!(
            err.downcast_ref::<OversizedReplayEnvelopeError>().is_some(),
            "expected OversizedReplayEnvelopeError: {err}"
        );
        assert!(store.get_raw("src::sink::kafka").await.unwrap().is_none());
        assert!(
            crate::replay_journal::JournalLog::read_since(&*journal, 0, 100)
                .await
                .unwrap()
                .is_empty()
        );
    }

    fn mem_journal() -> Arc<crate::replay_journal::BackendJournalLog> {
        let be: storage::ArcStorageBackend =
            Arc::new(storage::MemoryStorageBackend::new());
        Arc::new(crate::replay_journal::BackendJournalLog::new(
            be,
            deltaforge_core::replay::PipelineIdentity {
                pipeline: "p".into(),
                incarnation: "inc-1".into(),
                source_lineage: None,
            },
        ))
    }

    /// A replay-enabled pipeline must run with transaction-aligned batching; with
    /// `respect_source_tx = false` the legacy path ignores tx markers and would capture
    /// nothing, so startup fails fast rather than journaling silently empty.
    #[tokio::test]
    async fn replay_requires_respect_source_tx() {
        use checkpoints::MemCheckpointStore;
        let store = Arc::new(MemCheckpointStore::new().unwrap());
        let cfg = BatchConfig {
            max_events: Some(1000),
            max_ms: Some(60_000),
            respect_source_tx: Some(false),
            max_inflight: Some(1),
            ..BatchConfig::default()
        };
        let coord = cap_coord(
            store,
            MockSink::new("kafka", true),
            cfg,
            mem_journal(),
            0,
        );
        let err = feed_and_run(coord, vec![])
            .await
            .expect_err("replay + respect_source_tx=false must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("replay") && msg.contains("respect_source_tx"),
            "error should explain the constraint, got: {msg}"
        );
    }

    /// A genuine data-less boundary (no accumulated events, a new checkpoint) is a real
    /// commit unit and is captured as an empty envelope.
    #[tokio::test]
    async fn genuine_dataless_boundary_is_captured() {
        let journal = mem_journal();
        let coord = cap_coord(
            Arc::new(checkpoints::MemCheckpointStore::new().unwrap()),
            MockSink::new("kafka", true),
            cap_cfg(1000),
            journal.clone(),
            0,
        );
        feed_and_run(
            coord,
            vec![SourceItem::Boundary {
                boundary: deltaforge_core::SourceBoundary::checkpoint_only(
                    CheckpointMeta::from_vec(b"done-cp".to_vec()),
                ),
            }],
        )
        .await
        .unwrap();
        let envs =
            crate::replay_journal::JournalLog::read_since(&*journal, 0, 100)
                .await
                .unwrap();
        assert_eq!(envs.len(), 1, "data-less boundary is captured");
        assert!(envs[0].payload.events.is_empty());
        assert_eq!(envs[0].payload.boundary.checkpoint_hex, hex_of(b"done-cp"));
    }

    /// A redundant snapshot-completion marker (same checkpoint as the just-captured
    /// chunk-final unit, no new events) does not create a duplicate unit.
    #[tokio::test]
    async fn redundant_snapshot_completion_marker_is_skipped() {
        let journal = mem_journal();
        let coord = cap_coord(
            Arc::new(checkpoints::MemCheckpointStore::new().unwrap()),
            MockSink::new("kafka", true),
            cap_cfg(1000),
            journal.clone(),
            0,
        );
        feed_and_run(
            coord,
            vec![
                SourceItem::Event(snap_event(1, Some(b"chunk-cp"))),
                // The source re-emits the completion checkpoint as a data-less boundary.
                SourceItem::Boundary {
                    boundary: deltaforge_core::SourceBoundary::checkpoint_only(
                        CheckpointMeta::from_vec(b"chunk-cp".to_vec()),
                    ),
                },
            ],
        )
        .await
        .unwrap();
        let envs =
            crate::replay_journal::JournalLog::read_since(&*journal, 0, 100)
                .await
                .unwrap();
        assert_eq!(
            envs.len(),
            1,
            "redundant completion marker is not a new unit"
        );
        assert_eq!(envs[0].payload.events.len(), 1);
    }

    /// Two data-less boundaries sharing a checkpoint but carrying DIFFERENT durable
    /// watermarks are distinct commit units and must not be collapsed by dedup.
    #[tokio::test]
    async fn dataless_boundaries_differ_by_watermark() {
        let journal = mem_journal();
        let coord = cap_coord(
            Arc::new(checkpoints::MemCheckpointStore::new().unwrap()),
            MockSink::new("kafka", true),
            cap_cfg(1000),
            journal.clone(),
            0,
        );
        let boundary = |wm: &[u8]| deltaforge_core::SourceBoundary {
            checkpoint: CheckpointMeta::from_vec(b"same-cp".to_vec()),
            durable_watermark: Some(std::sync::Arc::from(wm)),
        };
        feed_and_run(
            coord,
            vec![
                SourceItem::Boundary {
                    boundary: boundary(b"wm-1"),
                },
                SourceItem::Boundary {
                    boundary: boundary(b"wm-2"),
                },
            ],
        )
        .await
        .unwrap();
        let envs =
            crate::replay_journal::JournalLog::read_since(&*journal, 0, 100)
                .await
                .unwrap();
        assert_eq!(
            envs.len(),
            2,
            "same checkpoint + different watermark are distinct units"
        );
        assert_eq!(
            envs[0].payload.boundary.watermark_hex,
            Some(hex_of(b"wm-1"))
        );
        assert_eq!(
            envs[1].payload.boundary.watermark_hex,
            Some(hex_of(b"wm-2"))
        );
    }

    /// Capture fails closed on a raw event with no event id: never store an envelope
    /// with a corrupt identity, and never advance the checkpoint.
    #[tokio::test]
    async fn missing_event_id_fails_closed() {
        use checkpoints::MemCheckpointStore;
        let store = Arc::new(MemCheckpointStore::new().unwrap());
        let journal = mem_journal();
        let coord = cap_coord(
            store.clone(),
            MockSink::new("kafka", true),
            cap_cfg(1000),
            journal.clone(),
            0,
        );
        let mut ev = snap_event(1, Some(b"chunk-cp"));
        ev.event_id = None;
        let res = feed_and_run(coord, vec![SourceItem::Event(ev)]).await;
        let err = res.expect_err("missing event_id must fail capture");
        assert!(
            err.downcast_ref::<ReplayCaptureError>().is_some(),
            "expected ReplayCaptureError, got: {err}"
        );
        assert!(
            store.get_raw("src::sink::kafka").await.unwrap().is_none(),
            "checkpoint must not advance"
        );
        assert!(
            crate::replay_journal::JournalLog::read_since(&*journal, 0, 100)
                .await
                .unwrap()
                .is_empty()
        );
    }

    /// A transaction larger than the soft `max_events` limit is delivered whole
    /// (one batch, never split), and the committed checkpoint is the commit
    /// marker's position - not any data event's.
    #[tokio::test]
    async fn tx_not_split_at_soft_limit_and_checkpoints_at_marker() {
        use checkpoints::MemCheckpointStore;
        let store = Arc::new(MemCheckpointStore::new().unwrap());
        let sink = MockSink::new("kafka", true);
        let coord = tx_coord(
            store.clone(),
            Arc::clone(&sink),
            BatchConfig {
                max_events: Some(3),  // soft limit far below the tx size
                max_ms: Some(60_000), // timer must not interfere
                respect_source_tx: Some(true),
                max_inflight: Some(1),
                ..BatchConfig::default()
            },
        );

        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let cancel = tokio_util::sync::CancellationToken::new();
        let (_pause_tx, pause_rx) = tokio::sync::watch::channel(false);

        tx.send(SourceItem::TxBegin {
            tx_id: "gtid:1".into(),
        })
        .await
        .unwrap();
        for i in 0..10 {
            tx.send(SourceItem::Event(tx_event(
                i,
                "gtid:1",
                format!("row-{i}").as_bytes(),
            )))
            .await
            .unwrap();
        }
        tx.send(SourceItem::TxCommit {
            tx_id: "gtid:1".into(),
            boundary: deltaforge_core::SourceBoundary::checkpoint_only(
                CheckpointMeta::from_vec(b"commit-1".to_vec()),
            ),
        })
        .await
        .unwrap();
        drop(tx);

        coord.run(rx, cancel, pause_rx).await.unwrap();

        assert_eq!(
            sink.batch_sizes(),
            vec![10],
            "the 10-event transaction must be delivered as one unsplit batch"
        );
        let cp = store.get_raw("src::sink::kafka").await.unwrap();
        assert_eq!(
            cp.as_deref(),
            Some(&b"commit-1"[..]),
            "checkpoint must be the commit marker's position, not a data row's"
        );
    }

    /// Several whole transactions accumulate into a single batch and flush
    /// together at the soft limit, checkpointing at the last marker.
    #[tokio::test]
    async fn multiple_whole_txs_share_one_batch() {
        use checkpoints::MemCheckpointStore;
        let store = Arc::new(MemCheckpointStore::new().unwrap());
        let sink = MockSink::new("kafka", true);
        let coord = tx_coord(
            store.clone(),
            Arc::clone(&sink),
            BatchConfig {
                max_events: Some(100),
                max_ms: Some(60_000),
                respect_source_tx: Some(true),
                max_inflight: Some(1),
                ..BatchConfig::default()
            },
        );

        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let cancel = tokio_util::sync::CancellationToken::new();
        let (_pause_tx, pause_rx) = tokio::sync::watch::channel(false);

        for (txn, cp) in [("gtid:1", "cp-1"), ("gtid:2", "cp-2")] {
            tx.send(SourceItem::TxBegin { tx_id: txn.into() })
                .await
                .unwrap();
            for i in 0..2 {
                tx.send(SourceItem::Event(tx_event(i, txn, b"row")))
                    .await
                    .unwrap();
            }
            tx.send(SourceItem::TxCommit {
                tx_id: txn.into(),
                boundary: deltaforge_core::SourceBoundary::checkpoint_only(
                    CheckpointMeta::from_vec(cp.as_bytes().to_vec()),
                ),
            })
            .await
            .unwrap();
        }
        drop(tx);

        coord.run(rx, cancel, pause_rx).await.unwrap();

        assert_eq!(
            sink.batch_sizes(),
            vec![4],
            "both whole transactions flush together as one batch on shutdown"
        );
        let cp = store.get_raw("src::sink::kafka").await.unwrap();
        assert_eq!(cp.as_deref(), Some(&b"cp-2"[..]));
    }

    /// A transaction that exceeds `max_tx_events` fails the pipeline
    /// (OversizedTxPolicy::Fail) and never advances the checkpoint.
    #[tokio::test]
    async fn oversized_transaction_fails_pipeline() {
        use checkpoints::MemCheckpointStore;
        let store = Arc::new(MemCheckpointStore::new().unwrap());
        let sink = MockSink::new("kafka", true);
        let coord = tx_coord(
            store.clone(),
            Arc::clone(&sink),
            BatchConfig {
                max_ms: Some(60_000),
                respect_source_tx: Some(true),
                max_inflight: Some(1),
                max_tx_events: Some(3),
                ..BatchConfig::default()
            },
        );

        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let cancel = tokio_util::sync::CancellationToken::new();
        let (_pause_tx, pause_rx) = tokio::sync::watch::channel(false);

        tx.send(SourceItem::TxBegin {
            tx_id: "gtid:1".into(),
        })
        .await
        .unwrap();
        for i in 0..4 {
            tx.send(SourceItem::Event(tx_event(i, "gtid:1", b"row")))
                .await
                .unwrap();
        }
        // No marker: the 4th in-tx event breaches the cap.
        drop(tx);

        let err = coord
            .run(rx, cancel, pause_rx)
            .await
            .expect_err("oversized transaction must fail the pipeline");
        // Assert on the concrete typed error and its structured fields, not a
        // formatted string.
        let oversized = err
            .downcast_ref::<OversizedTxError>()
            .unwrap_or_else(|| panic!("expected OversizedTxError, got: {err}"));
        assert_eq!(oversized.tx_id, "gtid:1");
        assert_eq!(oversized.observed_events, 4);
        assert_eq!(oversized.max_tx_events, 3);
        assert_eq!(oversized.exceeded, OversizedLimit::Events);
        let cp = store.get_raw("src::sink::kafka").await.unwrap();
        assert!(cp.is_none(), "oversized tx must not advance the checkpoint");
    }

    /// The approaching-cap warning fires once per dimension per open transaction:
    /// the byte-cap warning (G3) mirrors the event-cap one, and neither re-fires.
    #[test]
    fn approaching_warning_fires_once_per_dimension() {
        let mut b = BuildingBatch::with_capacity(4);
        // Simulate an open transaction at 90% of a 1000-byte cap, well under the
        // event cap. open_bytes = bytes - committed_bytes (both start committed=0).
        b.bytes = 900;
        maybe_warn_approaching(&mut b, 1_000_000, 1000, "p");
        assert!(b.approaching_bytes_warned, "byte-cap warning should fire");
        assert!(
            !b.approaching_events_warned,
            "event-cap warning should not fire below its threshold"
        );
        // Idempotent: a second call does not reset or re-fire.
        maybe_warn_approaching(&mut b, 1_000_000, 1000, "p");
        assert!(b.approaching_bytes_warned);
        // Closing the open region rearms the warnings for the next transaction.
        b.reset_approaching_warnings();
        assert!(!b.approaching_bytes_warned);
    }

    /// Shutdown mid-transaction discards the uncommitted suffix: only whole
    /// transactions (those with a commit marker) are flushed and checkpointed.
    #[tokio::test]
    async fn shutdown_discards_partial_transaction() {
        use checkpoints::MemCheckpointStore;
        let store = Arc::new(MemCheckpointStore::new().unwrap());
        let sink = MockSink::new("kafka", true);
        let coord = tx_coord(
            store.clone(),
            Arc::clone(&sink),
            BatchConfig {
                max_events: Some(1000),
                max_ms: Some(60_000),
                respect_source_tx: Some(true),
                max_inflight: Some(1),
                ..BatchConfig::default()
            },
        );

        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let cancel = tokio_util::sync::CancellationToken::new();
        let (_pause_tx, pause_rx) = tokio::sync::watch::channel(false);

        // One whole tx (2 events + marker) ...
        tx.send(SourceItem::TxBegin {
            tx_id: "gtid:1".into(),
        })
        .await
        .unwrap();
        for i in 0..2 {
            tx.send(SourceItem::Event(tx_event(i, "gtid:1", b"row")))
                .await
                .unwrap();
        }
        tx.send(SourceItem::TxCommit {
            tx_id: "gtid:1".into(),
            boundary: deltaforge_core::SourceBoundary::checkpoint_only(
                CheckpointMeta::from_vec(b"cp-1".to_vec()),
            ),
        })
        .await
        .unwrap();
        // ... then a partial tx (begin + one event, no marker) to discard.
        tx.send(SourceItem::TxBegin {
            tx_id: "gtid:2".into(),
        })
        .await
        .unwrap();
        tx.send(SourceItem::Event(tx_event(99, "gtid:2", b"row")))
            .await
            .unwrap();
        drop(tx);

        coord.run(rx, cancel, pause_rx).await.unwrap();

        assert_eq!(
            sink.batch_sizes(),
            vec![2],
            "only the whole transaction is flushed; the partial suffix is dropped"
        );
        let cp = store.get_raw("src::sink::kafka").await.unwrap();
        assert_eq!(cp.as_deref(), Some(&b"cp-1"[..]));
    }

    /// An empty (or fully source-filtered) transaction carries no events; the
    /// next committed transaction's boundary advances the checkpoint past it.
    #[tokio::test]
    async fn empty_transaction_covered_by_next_boundary() {
        use checkpoints::MemCheckpointStore;
        let store = Arc::new(MemCheckpointStore::new().unwrap());
        let sink = MockSink::new("kafka", true);
        let coord = tx_coord(
            store.clone(),
            Arc::clone(&sink),
            BatchConfig {
                max_events: Some(1000),
                max_ms: Some(60_000),
                respect_source_tx: Some(true),
                max_inflight: Some(1),
                ..BatchConfig::default()
            },
        );

        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let cancel = tokio_util::sync::CancellationToken::new();
        let (_pause_tx, pause_rx) = tokio::sync::watch::channel(false);

        // Empty transaction: begin immediately followed by commit, no events.
        tx.send(SourceItem::TxBegin {
            tx_id: "gtid:1".into(),
        })
        .await
        .unwrap();
        tx.send(SourceItem::TxCommit {
            tx_id: "gtid:1".into(),
            boundary: deltaforge_core::SourceBoundary::checkpoint_only(
                CheckpointMeta::from_vec(b"empty".to_vec()),
            ),
        })
        .await
        .unwrap();
        // A real transaction follows.
        tx.send(SourceItem::TxBegin {
            tx_id: "gtid:2".into(),
        })
        .await
        .unwrap();
        tx.send(SourceItem::Event(tx_event(1, "gtid:2", b"row")))
            .await
            .unwrap();
        tx.send(SourceItem::TxCommit {
            tx_id: "gtid:2".into(),
            boundary: deltaforge_core::SourceBoundary::checkpoint_only(
                CheckpointMeta::from_vec(b"real".to_vec()),
            ),
        })
        .await
        .unwrap();
        drop(tx);

        coord.run(rx, cancel, pause_rx).await.unwrap();

        assert_eq!(sink.batch_sizes(), vec![1]);
        let cp = store.get_raw("src::sink::kafka").await.unwrap();
        assert_eq!(cp.as_deref(), Some(&b"real"[..]));
    }

    /// Run a coordinator over a fixed item stream and return its result. The
    /// channel is closed after the items, so `run` returns once they are drained.
    async fn run_items(
        items: Vec<SourceItem>,
    ) -> (Result<()>, Arc<checkpoints::MemCheckpointStore>) {
        use checkpoints::MemCheckpointStore;
        let store = Arc::new(MemCheckpointStore::new().unwrap());
        let sink = MockSink::new("kafka", true);
        let coord = tx_coord(
            store.clone(),
            sink,
            BatchConfig {
                max_events: Some(1000),
                max_ms: Some(60_000),
                respect_source_tx: Some(true),
                max_inflight: Some(1),
                ..BatchConfig::default()
            },
        );
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let cancel = tokio_util::sync::CancellationToken::new();
        let (_pause_tx, pause_rx) = tokio::sync::watch::channel(false);
        for item in items {
            tx.send(item).await.unwrap();
        }
        drop(tx);
        (coord.run(rx, cancel, pause_rx).await, store)
    }

    fn begin(tx_id: &str) -> SourceItem {
        SourceItem::TxBegin {
            tx_id: tx_id.into(),
        }
    }
    fn commit(tx_id: &str, cp: &[u8]) -> SourceItem {
        SourceItem::TxCommit {
            tx_id: tx_id.into(),
            boundary: deltaforge_core::SourceBoundary::checkpoint_only(
                CheckpointMeta::from_vec(cp.to_vec()),
            ),
        }
    }

    /// A commit marker with no matching begin (an unknown or duplicate commit)
    /// is a fatal protocol error - it must not advance any checkpoint.
    #[tokio::test]
    async fn commit_without_begin_is_rejected() {
        let (res, store) = run_items(vec![commit("gtid:1", b"cp")]).await;
        let err = res.expect_err("lone commit must be rejected");
        assert!(
            err.to_string().contains("no open transaction"),
            "unexpected error: {err}"
        );
        assert!(store.get_raw("src::sink::kafka").await.unwrap().is_none());
    }

    /// A duplicate commit marker cannot advance the checkpoint twice.
    #[tokio::test]
    async fn duplicate_commit_is_rejected() {
        let (res, _) = run_items(vec![
            begin("gtid:1"),
            SourceItem::Event(tx_event(1, "gtid:1", b"row")),
            commit("gtid:1", b"cp-1"),
            commit("gtid:1", b"cp-1"), // duplicate
        ])
        .await;
        let err = res.expect_err("duplicate commit must be rejected");
        assert!(err.to_string().contains("no open transaction"));
    }

    /// A non-transactional (transaction=None) event arriving while a transaction is
    /// open must be rejected. Otherwise it is treated as a standalone boundary and
    /// can flush/checkpoint mid-transaction, splitting the open transaction - the
    /// failure mode of an unstamped PG transactional logical message. Defense in
    /// depth: even if a source forgets to stamp such an event, the coordinator fails
    /// closed rather than splitting.
    #[tokio::test]
    async fn standalone_event_inside_open_tx_is_rejected() {
        let mut standalone = tx_event(1, "gtid:1", b"row");
        standalone.transaction = None; // simulate an unstamped in-tx event
        let (res, store) =
            run_items(vec![begin("gtid:1"), SourceItem::Event(standalone)])
                .await;
        let err = res
            .expect_err("standalone event inside an open tx must be rejected");
        assert!(
            err.to_string().contains("standalone"),
            "unexpected error: {err}"
        );
        assert!(store.get_raw("src::sink::kafka").await.unwrap().is_none());
    }

    /// A second transaction cannot begin before the first commits.
    #[tokio::test]
    async fn nested_begin_is_rejected() {
        let (res, _) = run_items(vec![begin("gtid:1"), begin("gtid:2")]).await;
        let err = res.expect_err("nested begin must be rejected");
        assert!(
            err.to_string().contains("still open"),
            "unexpected error: {err}"
        );
    }

    /// A commit whose id does not match the open transaction is rejected.
    #[tokio::test]
    async fn commit_id_mismatch_is_rejected() {
        let (res, _) = run_items(vec![
            begin("gtid:1"),
            SourceItem::Event(tx_event(1, "gtid:1", b"row")),
            commit("gtid:2", b"cp"), // wrong id
        ])
        .await;
        let err = res.expect_err("mismatched commit must be rejected");
        assert!(err.to_string().contains("does not match"));
    }

    /// An in-transaction event whose id does not match the open transaction is
    /// rejected.
    #[tokio::test]
    async fn event_tx_mismatch_is_rejected() {
        let (res, _) = run_items(vec![
            begin("gtid:1"),
            SourceItem::Event(tx_event(1, "gtid:2", b"row")),
        ])
        .await;
        let err = res.expect_err("mismatched event must be rejected");
        assert!(err.to_string().contains("was open"));
    }

    /// A transactional event with no open transaction is rejected.
    #[tokio::test]
    async fn event_without_begin_is_rejected() {
        let (res, _) =
            run_items(vec![SourceItem::Event(tx_event(1, "gtid:1", b"row"))])
                .await;
        let err = res.expect_err("event without begin must be rejected");
        assert!(err.to_string().contains("no open transaction"));
    }

    /// A well-formed begin→event→commit stream is accepted and checkpoints.
    #[tokio::test]
    async fn well_formed_transaction_is_accepted() {
        let (res, store) = run_items(vec![
            begin("gtid:1"),
            SourceItem::Event(tx_event(1, "gtid:1", b"row")),
            commit("gtid:1", b"cp-1"),
        ])
        .await;
        res.expect("well-formed transaction must succeed");
        assert_eq!(
            store.get_raw("src::sink::kafka").await.unwrap().as_deref(),
            Some(&b"cp-1"[..])
        );
    }

    /// Drops every event - models a transaction whose rows are all filtered by a
    /// processor.
    struct DropAllProcessor;
    #[async_trait::async_trait]
    impl deltaforge_core::Processor for DropAllProcessor {
        fn id(&self) -> &str {
            "drop-all"
        }
        async fn process(
            &self,
            _events: Vec<Event>,
            _ctx: &BatchContext,
        ) -> anyhow::Result<Vec<Event>> {
            Ok(Vec::new())
        }
        fn identity_digest(&self) -> &str {
            "drop-all-v1"
        }
    }

    /// A transaction whose every event is processor-filtered still advances the
    /// commit checkpoint: the marker's position rides on the pre-processing
    /// events, so the boundary is committed even though nothing is delivered.
    #[tokio::test]
    async fn fully_filtered_transaction_still_checkpoints() {
        use checkpoints::MemCheckpointStore;
        let store = Arc::new(MemCheckpointStore::new().unwrap());
        let sink = MockSink::new("kafka", true);
        let sinks: Vec<ArcDynSink> = vec![Arc::clone(&sink) as ArcDynSink];
        let cp_fn =
            build_commit_fn(store.clone(), "src::sink::kafka".to_string());
        let procs: Arc<[deltaforge_core::ArcDynProcessor]> =
            Arc::from(vec![Arc::new(DropAllProcessor) as _]);
        let batch_processor = build_batch_processor(procs, "test".to_string());
        let coord = Coordinator::builder("tx-test")
            .sinks(sinks)
            .batch_config(Some(BatchConfig {
                max_events: Some(1000),
                max_ms: Some(60_000),
                respect_source_tx: Some(true),
                max_inflight: Some(1),
                ..BatchConfig::default()
            }))
            .commit_fn("kafka", cp_fn)
            .process_fn(batch_processor)
            .build();

        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let cancel = tokio_util::sync::CancellationToken::new();
        let (_pause_tx, pause_rx) = tokio::sync::watch::channel(false);

        tx.send(SourceItem::TxBegin {
            tx_id: "gtid:1".into(),
        })
        .await
        .unwrap();
        for i in 0..3 {
            tx.send(SourceItem::Event(tx_event(i, "gtid:1", b"row")))
                .await
                .unwrap();
        }
        tx.send(SourceItem::TxCommit {
            tx_id: "gtid:1".into(),
            boundary: deltaforge_core::SourceBoundary::checkpoint_only(
                CheckpointMeta::from_vec(b"cp-1".to_vec()),
            ),
        })
        .await
        .unwrap();
        drop(tx);

        coord.run(rx, cancel, pause_rx).await.unwrap();

        assert_eq!(sink.delivery_count(), 0, "all events were filtered");
        let cp = store.get_raw("src::sink::kafka").await.unwrap();
        assert_eq!(
            cp.as_deref(),
            Some(&b"cp-1"[..]),
            "a fully-filtered transaction must still advance the checkpoint"
        );
    }

    /// A completed transaction that never reaches the soft size limit is flushed
    /// by the timer once the source goes idle - while it stays open (no marker)
    /// the timer must not flush it.
    #[tokio::test]
    async fn timer_flushes_whole_tx_when_idle() {
        use checkpoints::MemCheckpointStore;
        let store = Arc::new(MemCheckpointStore::new().unwrap());
        let sink = MockSink::new("kafka", true);
        let coord = tx_coord(
            store.clone(),
            Arc::clone(&sink),
            BatchConfig {
                max_events: Some(1000), // never reached
                max_ms: Some(50),       // short timer
                respect_source_tx: Some(true),
                max_inflight: Some(1),
                ..BatchConfig::default()
            },
        );

        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let cancel = tokio_util::sync::CancellationToken::new();
        let (_pause_tx, pause_rx) = tokio::sync::watch::channel(false);

        tx.send(SourceItem::TxBegin {
            tx_id: "gtid:1".into(),
        })
        .await
        .unwrap();
        for i in 0..2 {
            tx.send(SourceItem::Event(tx_event(i, "gtid:1", b"row")))
                .await
                .unwrap();
        }
        tx.send(SourceItem::TxCommit {
            tx_id: "gtid:1".into(),
            boundary: deltaforge_core::SourceBoundary::checkpoint_only(
                CheckpointMeta::from_vec(b"cp-1".to_vec()),
            ),
        })
        .await
        .unwrap();
        // Keep the channel open (idle source at the WAL tail).

        let cancel_c = cancel.clone();
        let sink_c = Arc::clone(&sink);
        let waiter = tokio::spawn(async move {
            let deadline = Instant::now() + Duration::from_secs(2);
            loop {
                if sink_c.delivery_count() >= 2 {
                    cancel_c.cancel();
                    return true;
                }
                if Instant::now() > deadline {
                    cancel_c.cancel();
                    return false;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });

        coord.run(rx, cancel, pause_rx).await.unwrap();
        assert!(
            waiter.await.unwrap(),
            "timer should flush the idle whole tx"
        );
        assert_eq!(sink.batch_sizes(), vec![2]);
        let cp = store.get_raw("src::sink::kafka").await.unwrap();
        assert_eq!(cp.as_deref(), Some(&b"cp-1"[..]));
    }

    /// The soft `max_events` limit closes a batch only at a commit boundary:
    /// transactions accumulate until a marker pushes the batch past the limit,
    /// splitting the stream into batches that each contain whole transactions.
    #[tokio::test]
    async fn soft_limit_splits_batches_at_tx_boundaries() {
        use checkpoints::MemCheckpointStore;
        let store = Arc::new(MemCheckpointStore::new().unwrap());
        let sink = MockSink::new("kafka", true);
        let coord = tx_coord(
            store.clone(),
            Arc::clone(&sink),
            BatchConfig {
                max_events: Some(3),
                max_ms: Some(60_000),
                respect_source_tx: Some(true),
                max_inflight: Some(1),
                ..BatchConfig::default()
            },
        );

        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let cancel = tokio_util::sync::CancellationToken::new();
        let (_pause_tx, pause_rx) = tokio::sync::watch::channel(false);

        for (txn, cp) in
            [("gtid:1", "cp-1"), ("gtid:2", "cp-2"), ("gtid:3", "cp-3")]
        {
            tx.send(SourceItem::TxBegin { tx_id: txn.into() })
                .await
                .unwrap();
            for i in 0..2 {
                tx.send(SourceItem::Event(tx_event(i, txn, b"row")))
                    .await
                    .unwrap();
            }
            tx.send(SourceItem::TxCommit {
                tx_id: txn.into(),
                boundary: deltaforge_core::SourceBoundary::checkpoint_only(
                    CheckpointMeta::from_vec(cp.as_bytes().to_vec()),
                ),
            })
            .await
            .unwrap();
        }
        drop(tx);

        coord.run(rx, cancel, pause_rx).await.unwrap();

        // tx1+tx2 (4 events) cross the limit at tx2's marker → one batch; tx3
        // (2 events) flushes on shutdown → second batch. Neither splits a tx.
        assert_eq!(sink.batch_sizes(), vec![4, 2]);
        let cp = store.get_raw("src::sink::kafka").await.unwrap();
        assert_eq!(cp.as_deref(), Some(&b"cp-3"[..]));
    }

    /// G4: the stream ends immediately BEFORE a COMMIT. The open transaction is
    /// discarded and no checkpoint advances - on restart the source replays it whole.
    #[tokio::test]
    async fn disconnect_before_commit_discards_and_no_checkpoint() {
        let (res, store) = run_items(vec![
            begin("gtid:1"),
            SourceItem::Event(tx_event(1, "gtid:1", b"r1")),
            SourceItem::Event(tx_event(2, "gtid:1", b"r2")),
            // stream ends here: no commit marker
        ])
        .await;
        res.expect("clean stream end");
        assert!(
            store.get_raw("src::sink::kafka").await.unwrap().is_none(),
            "a partial tx (disconnect before COMMIT) must not checkpoint"
        );
    }

    /// G4: the stream ends immediately AFTER a COMMIT. The whole transaction is
    /// flushed and the checkpoint is the commit marker's position.
    #[tokio::test]
    async fn disconnect_after_commit_flushes_and_checkpoints() {
        let (res, store) = run_items(vec![
            begin("gtid:1"),
            SourceItem::Event(tx_event(1, "gtid:1", b"r1")),
            commit("gtid:1", b"cp-1"),
            // stream ends right after the commit marker
        ])
        .await;
        res.expect("clean stream end");
        assert_eq!(
            store.get_raw("src::sink::kafka").await.unwrap().as_deref(),
            Some(&b"cp-1"[..]),
            "a committed tx (disconnect after COMMIT) checkpoints at the marker"
        );
    }

    /// G5: the flush timer must never flush an open (uncommitted) transaction, no
    /// matter how long the source sits mid-transaction.
    #[tokio::test]
    async fn timer_does_not_flush_partial_transaction() {
        use checkpoints::MemCheckpointStore;
        let store = Arc::new(MemCheckpointStore::new().unwrap());
        let sink = MockSink::new("kafka", true);
        let coord = tx_coord(
            store.clone(),
            Arc::clone(&sink),
            BatchConfig {
                max_events: Some(1000),
                max_ms: Some(50), // short timer, many chances to fire
                respect_source_tx: Some(true),
                max_inflight: Some(1),
                ..BatchConfig::default()
            },
        );

        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let cancel = tokio_util::sync::CancellationToken::new();
        let (_pause_tx, pause_rx) = tokio::sync::watch::channel(false);

        tx.send(begin("gtid:1")).await.unwrap();
        for i in 0..2 {
            tx.send(SourceItem::Event(tx_event(i, "gtid:1", b"row")))
                .await
                .unwrap();
        }
        // No commit marker; keep the channel open so the timer can fire repeatedly.

        let cancel_c = cancel.clone();
        let sink_c = Arc::clone(&sink);
        let waiter = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            let delivered = sink_c.delivery_count();
            cancel_c.cancel();
            delivered
        });

        coord.run(rx, cancel, pause_rx).await.unwrap();
        assert_eq!(
            waiter.await.unwrap(),
            0,
            "timer must not flush an open partial transaction"
        );
        assert_eq!(
            sink.delivery_count(),
            0,
            "the partial tx is discarded on cancel, never delivered"
        );
        assert!(store.get_raw("src::sink::kafka").await.unwrap().is_none());
    }

    /// G6: an oversized transaction fails without advancing the checkpoint; on
    /// restart the ENTIRE transaction replays and, under a larger cap, commits as
    /// one whole batch.
    #[tokio::test]
    async fn oversized_tx_restart_replays_whole_transaction() {
        use checkpoints::MemCheckpointStore;
        let store = Arc::new(MemCheckpointStore::new().unwrap());

        // Phase 1: cap too small -> fail closed, no checkpoint, nothing delivered.
        {
            let sink = MockSink::new("kafka", true);
            let coord = tx_coord(
                store.clone(),
                Arc::clone(&sink),
                BatchConfig {
                    max_ms: Some(60_000),
                    respect_source_tx: Some(true),
                    max_inflight: Some(1),
                    max_tx_events: Some(3),
                    ..BatchConfig::default()
                },
            );
            let (tx, rx) = tokio::sync::mpsc::channel(64);
            let cancel = tokio_util::sync::CancellationToken::new();
            let (_pause_tx, pause_rx) = tokio::sync::watch::channel(false);
            tx.send(begin("gtid:1")).await.unwrap();
            for i in 0..4 {
                tx.send(SourceItem::Event(tx_event(i, "gtid:1", b"row")))
                    .await
                    .unwrap();
            }
            drop(tx);
            coord
                .run(rx, cancel, pause_rx)
                .await
                .expect_err("oversized tx must fail");
            assert_eq!(sink.delivery_count(), 0);
            assert!(store.get_raw("src::sink::kafka").await.unwrap().is_none());
        }

        // Phase 2: restart with a larger cap. The same whole transaction replays
        // (all 4 events + its commit) and now delivers as one batch and checkpoints.
        {
            let sink = MockSink::new("kafka", true);
            let coord = tx_coord(
                store.clone(),
                Arc::clone(&sink),
                BatchConfig {
                    max_ms: Some(60_000),
                    respect_source_tx: Some(true),
                    max_inflight: Some(1),
                    max_tx_events: Some(10),
                    ..BatchConfig::default()
                },
            );
            let (tx, rx) = tokio::sync::mpsc::channel(64);
            let cancel = tokio_util::sync::CancellationToken::new();
            let (_pause_tx, pause_rx) = tokio::sync::watch::channel(false);
            tx.send(begin("gtid:1")).await.unwrap();
            for i in 0..4 {
                tx.send(SourceItem::Event(tx_event(i, "gtid:1", b"row")))
                    .await
                    .unwrap();
            }
            tx.send(commit("gtid:1", b"cp-1")).await.unwrap();
            drop(tx);
            coord.run(rx, cancel, pause_rx).await.unwrap();
            assert_eq!(
                sink.batch_sizes(),
                vec![4],
                "the whole transaction replays as one batch"
            );
            assert_eq!(
                store.get_raw("src::sink::kafka").await.unwrap().as_deref(),
                Some(&b"cp-1"[..])
            );
        }
    }

    /// G7: with `respect_source_tx = false`, the legacy path ignores tx markers and
    /// the hard event limit splits by count (it may split a transaction - by design).
    #[tokio::test]
    async fn legacy_path_ignores_markers_and_splits_by_count() {
        use checkpoints::MemCheckpointStore;
        let store = Arc::new(MemCheckpointStore::new().unwrap());
        let sink = MockSink::new("kafka", true);
        let coord = tx_coord(
            store.clone(),
            Arc::clone(&sink),
            BatchConfig {
                max_events: Some(2),
                max_ms: Some(60_000),
                respect_source_tx: Some(false),
                max_inflight: Some(1),
                ..BatchConfig::default()
            },
        );

        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let cancel = tokio_util::sync::CancellationToken::new();
        let (_pause_tx, pause_rx) = tokio::sync::watch::channel(false);

        // A 3-event transaction with markers; the legacy path drops the markers and
        // splits the events by count into [2, 1].
        tx.send(begin("gtid:1")).await.unwrap();
        for i in 0..3 {
            tx.send(SourceItem::Event(tx_event(i, "gtid:1", b"row")))
                .await
                .unwrap();
        }
        tx.send(commit("gtid:1", b"cp-1")).await.unwrap();
        drop(tx);

        coord.run(rx, cancel, pause_rx).await.unwrap();
        assert_eq!(sink.batch_sizes(), vec![2, 1]);
    }

    // ── Pure batch-accumulation helpers (check_and_split / policy) ───────

    /// Build a minimal row event with a controllable size hint and tx-end
    /// flag - the only two fields `check_and_split` reads.
    fn sized_event(size_bytes: usize, tx_end: bool) -> Event {
        let source = deltaforge_core::SourceInfo {
            version: "test".into(),
            connector: "mysql".into(),
            name: "test".into(),
            db: "db".into(),
            schema: None,
            table: "t".into(),
            ts_ms: 0,
            snapshot: None,
            position: deltaforge_core::SourcePosition::default(),
        };
        let mut e = Event::new_row(
            deltaforge_core::EventId::mysql_row_server(1, "t", 1, 0),
            source,
            deltaforge_core::Op::Create,
            None,
            Some(serde_json::json!({"id": 1})),
            0,
            0,
        );
        e.size_bytes = size_bytes;
        e.tx_end = tx_end;
        e
    }

    #[test]
    fn check_and_split_flushes_at_max_events() {
        // max_events=2, byte limit effectively unbounded.
        let mut b = BuildingBatch::with_capacity(4);
        assert!(
            check_and_split(&mut b, sized_event(1, false), 2, usize::MAX)
                .is_none(),
            "1st event fits"
        );
        assert!(
            check_and_split(&mut b, sized_event(1, false), 2, usize::MAX)
                .is_none(),
            "2nd event fits (len 1 < 2)"
        );
        // 3rd event: len 2 >= max_events(2) → flush the full batch, restart.
        let flushed =
            check_and_split(&mut b, sized_event(1, false), 2, usize::MAX)
                .expect("must flush at max_events");
        assert_eq!(flushed.raw.len(), 2);
        assert_eq!(b.raw.len(), 1, "current event carried into new batch");
    }

    #[test]
    fn check_and_split_flushes_at_max_bytes_and_accumulates() {
        // max_bytes=100, event count effectively unbounded.
        let mut b = BuildingBatch::with_capacity(64);
        assert!(
            check_and_split(&mut b, sized_event(60, false), 10_000, 100)
                .is_none()
        );
        assert_eq!(b.bytes, 60, "byte hint accumulated (kills +=/size-hint)");
        // 60 + 60 = 120 >= 100 → flush. Distinguishes `+` from `-` (0 >= 100
        // would not flush) at the size check.
        let flushed =
            check_and_split(&mut b, sized_event(60, false), 10_000, 100)
                .expect("must flush at max_bytes");
        assert_eq!(flushed.bytes, 60);
        assert_eq!(b.bytes, 60, "new batch seeded with current event bytes");
    }

    #[test]
    fn check_and_split_size_check_is_addition_not_product() {
        // Accumulator already holds 60 bytes (seeded directly), a zero-sized
        // event arrives, max_bytes=50. Sum: 60 + 0 = 60 >= 50 → flush.
        // Product (mutant): 60 * 0 = 0 >= 50 → no flush. Pins `+` vs `*`.
        let mut b = BuildingBatch::with_capacity(64);
        b.bytes = 60;
        b.raw.push(sized_event(60, false)); // non-empty so the guard allows flush
        let flushed =
            check_and_split(&mut b, sized_event(0, false), 10_000, 50);
        assert!(flushed.is_some(), "60 + 0 >= 50 must flush");
    }

    #[test]
    fn check_and_split_transaction_boundary_guard() {
        // Case B: empty batch + a tx-boundary event that alone exceeds the
        // byte limit → do NOT flush (nothing to flush; can't split a lone
        // boundary). Pins both `!` operators in the guard.
        let mut b = BuildingBatch::with_capacity(64);
        assert!(
            check_and_split(&mut b, sized_event(20, true), 10_000, 10)
                .is_none(),
            "empty batch + boundary event must not flush"
        );

        // Case C: non-empty batch, incoming event IS a boundary, limit hit →
        // flush (the || must stay ||; && would suppress it).
        let mut b = BuildingBatch::with_capacity(64);
        assert!(
            check_and_split(&mut b, sized_event(1, false), 1, usize::MAX)
                .is_none()
        );
        assert!(
            check_and_split(&mut b, sized_event(1, true), 1, usize::MAX)
                .is_some(),
            "full batch must flush even when next event is a boundary"
        );

        // Case D: empty batch, NON-boundary event over the byte limit →
        // flush. Pins is_tx_boundary against a constant `true`.
        let mut b = BuildingBatch::with_capacity(64);
        assert!(
            check_and_split(&mut b, sized_event(20, false), 10_000, 10)
                .is_some(),
            "empty batch + non-boundary oversized event must flush"
        );
    }

    #[test]
    fn policy_satisfied_covers_each_variant() {
        // signature: (policy, total_sinks, required_total, required_acks, total_acks)
        // Default (Required): all required sinks must ack.
        assert!(policy_satisfied(&None, 5, 2, 2, 5));
        assert!(!policy_satisfied(&None, 5, 2, 1, 5));
        // Required
        assert!(policy_satisfied(&Some(CommitPolicy::Required), 3, 3, 3, 3));
        assert!(!policy_satisfied(&Some(CommitPolicy::Required), 9, 3, 2, 9));
        // All: every sink (required + optional) must ack.
        assert!(policy_satisfied(&Some(CommitPolicy::All), 4, 2, 2, 4));
        assert!(!policy_satisfied(&Some(CommitPolicy::All), 4, 2, 2, 3));
        // Quorum
        let q = Some(CommitPolicy::Quorum { quorum: 2 });
        assert!(policy_satisfied(&q, 5, 0, 0, 2));
        assert!(!policy_satisfied(&q, 5, 0, 0, 1));
    }

    #[test]
    fn all_policy_requires_every_sink_including_optional() {
        // 1 required + 1 optional (total_sinks=2, required_total=1).
        // Both ack → satisfied.
        assert!(policy_satisfied(&Some(CommitPolicy::All), 2, 1, 1, 2));
        // Optional sink fails (total_acks=1): must NOT be satisfied. Regression
        // guard - the old code compared total_acks to required_total (1) and
        // spuriously passed here.
        assert!(!policy_satisfied(&Some(CommitPolicy::All), 2, 1, 1, 1));
    }

    #[test]
    fn validate_commit_policy_rejects_impossible_quorum() {
        assert!(
            validate_commit_policy(
                &Some(CommitPolicy::Quorum { quorum: 0 }),
                3
            )
            .is_err()
        );
        assert!(
            validate_commit_policy(
                &Some(CommitPolicy::Quorum { quorum: 4 }),
                3
            )
            .is_err()
        );
        assert!(
            validate_commit_policy(
                &Some(CommitPolicy::Quorum { quorum: 2 }),
                3
            )
            .is_ok()
        );
        assert!(validate_commit_policy(&Some(CommitPolicy::All), 3).is_ok());
        assert!(validate_commit_policy(&None, 0).is_ok());
    }

    // ── Per-sink commit tests ────────────────────────────────────────────

    /// Mock sink that tracks delivery calls and optionally fails.
    struct MockSink {
        id: String,
        required: bool,
        fail: AtomicBool,
        delivered: AtomicUsize,
        batch_sizes: std::sync::Mutex<Vec<usize>>,
    }

    impl MockSink {
        fn new(id: &str, required: bool) -> Arc<Self> {
            Arc::new(Self {
                id: id.to_string(),
                required,
                fail: AtomicBool::new(false),
                delivered: AtomicUsize::new(0),
                batch_sizes: std::sync::Mutex::new(Vec::new()),
            })
        }

        fn set_fail(&self, fail: bool) {
            self.fail.store(fail, std::sync::atomic::Ordering::Relaxed);
        }

        fn delivery_count(&self) -> usize {
            self.delivered.load(std::sync::atomic::Ordering::Relaxed)
        }

        /// Event counts of each delivered batch, in order.
        fn batch_sizes(&self) -> Vec<usize> {
            self.batch_sizes.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl deltaforge_core::Sink for MockSink {
        fn id(&self) -> &str {
            &self.id
        }

        fn required(&self) -> bool {
            self.required
        }

        async fn send(&self, _event: &Event) -> SinkResult<()> {
            self.send_batch(std::slice::from_ref(_event))
                .await
                .map(|_| ())
        }

        async fn send_batch(
            &self,
            events: &[Event],
        ) -> SinkResult<deltaforge_core::BatchResult> {
            if self.fail.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(SinkError::Backpressure {
                    details: "mock failure".into(),
                });
            }
            self.delivered
                .fetch_add(events.len(), std::sync::atomic::Ordering::Relaxed);
            self.batch_sizes.lock().unwrap().push(events.len());
            Ok(deltaforge_core::BatchResult::ok())
        }
    }

    #[tokio::test]
    async fn test_per_sink_checkpoint_only_advances_on_success() {
        use checkpoints::MemCheckpointStore;

        let store = Arc::new(MemCheckpointStore::new().unwrap());

        let kafka_sink = MockSink::new("kafka", true);
        let redis_sink = MockSink::new("redis", false);
        redis_sink.set_fail(true); // Redis will fail delivery.

        let sinks: Vec<ArcDynSink> = vec![
            Arc::clone(&kafka_sink) as ArcDynSink,
            Arc::clone(&redis_sink) as ArcDynSink,
        ];

        let kafka_cp =
            build_commit_fn(store.clone(), "mysql::sink::kafka".to_string());
        let redis_cp =
            build_commit_fn(store.clone(), "mysql::sink::redis".to_string());

        let processors: Arc<[deltaforge_core::ArcDynProcessor]> =
            Arc::from(vec![]);
        let batch_processor =
            build_batch_processor(processors, "test".to_string());

        let coord = Coordinator::builder("test")
            .sinks(sinks)
            .batch_config(Some(BatchConfig {
                max_events: Some(10),
                max_bytes: None,
                max_ms: Some(100),
                respect_source_tx: None,
                max_inflight: Some(1),
                ..BatchConfig::default()
            }))
            .commit_fn("kafka", kafka_cp)
            .commit_fn("redis", redis_cp)
            .process_fn(batch_processor)
            .build();

        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let cancel = tokio_util::sync::CancellationToken::new();
        let (_pause_tx, pause_rx) = tokio::sync::watch::channel(false);

        // Send one event with a checkpoint, then drop the sender
        // so the coordinator sees channel-closed and exits.
        let source = deltaforge_core::SourceInfo {
            version: "test".into(),
            connector: "mysql".into(),
            name: "test".into(),
            db: "db".into(),
            schema: None,
            table: "table".into(),
            ts_ms: 0,
            snapshot: None,
            position: deltaforge_core::SourcePosition::default(),
        };
        let mut event = Event::new_row(
            deltaforge_core::EventId::mysql_row_server(1, "t", 1, 0),
            source,
            deltaforge_core::Op::Create,
            None,
            Some(serde_json::json!({"id": 1})),
            0,
            0,
        );
        event
            .set_checkpoint(CheckpointMeta::from_vec(b"{\"pos\":42}".to_vec()));
        event.tx_end = true;
        tx.send(SourceItem::Event(event)).await.unwrap();
        drop(tx); // Close channel so coordinator exits after processing.

        let _ = coord.run(rx, cancel, pause_rx).await;

        // Kafka succeeded - checkpoint should be written.
        let kafka_cp = store.get_raw("mysql::sink::kafka").await.unwrap();
        assert!(
            kafka_cp.is_some(),
            "kafka checkpoint should be saved after successful delivery"
        );
        assert_eq!(kafka_cp.unwrap(), b"{\"pos\":42}");

        // Redis failed - checkpoint should NOT be written.
        let redis_cp = store.get_raw("mysql::sink::redis").await.unwrap();
        assert!(
            redis_cp.is_none(),
            "redis checkpoint should not be saved after failed delivery"
        );

        // Kafka should have received 1 event.
        assert_eq!(kafka_sink.delivery_count(), 1);
        // Redis was called but failed.
        assert_eq!(redis_sink.delivery_count(), 0);
    }

    /// Regression test: a partial batch (fewer events than max_events) must be
    /// flushed by the timer when the source goes idle - not stuck waiting for
    /// more events to fill the batch.
    #[tokio::test]
    async fn test_partial_batch_flushed_by_timer() {
        use checkpoints::MemCheckpointStore;

        let store = Arc::new(MemCheckpointStore::new().unwrap());
        let sink = MockSink::new("kafka", true);
        let sinks: Vec<ArcDynSink> = vec![Arc::clone(&sink) as ArcDynSink];

        let cp_fn =
            build_commit_fn(store.clone(), "src::sink::kafka".to_string());
        let processors: Arc<[deltaforge_core::ArcDynProcessor]> =
            Arc::from(vec![]);
        let batch_processor =
            build_batch_processor(processors, "test".to_string());

        // Large max_events so the batch never fills, short timer (50ms).
        let coord = Coordinator::builder("test-timer")
            .sinks(sinks)
            .batch_config(Some(BatchConfig {
                max_events: Some(10000),
                max_bytes: None,
                max_ms: Some(50),
                respect_source_tx: None,
                max_inflight: Some(1),
                ..BatchConfig::default()
            }))
            .commit_fn("kafka", cp_fn)
            .process_fn(batch_processor)
            .build();

        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let cancel = tokio_util::sync::CancellationToken::new();
        let (_pause_tx, pause_rx) = tokio::sync::watch::channel(false);

        let source = deltaforge_core::SourceInfo {
            version: "test".into(),
            connector: "mysql".into(),
            name: "test".into(),
            db: "db".into(),
            schema: None,
            table: "table".into(),
            ts_ms: 0,
            snapshot: None,
            position: deltaforge_core::SourcePosition::default(),
        };

        // Send 3 events (well below max_events=10000), then go idle.
        for i in 0..3 {
            let mut ev = Event::new_row(
                deltaforge_core::EventId::mysql_row_server(1, "t", 1, 0),
                source.clone(),
                deltaforge_core::Op::Create,
                None,
                Some(serde_json::json!({"id": i})),
                0,
                0,
            );
            if i == 2 {
                ev.set_checkpoint(CheckpointMeta::from_vec(
                    b"{\"pos\":99}".to_vec(),
                ));
                ev.tx_end = true;
            }
            tx.send(SourceItem::Event(ev)).await.unwrap();
        }

        // Don't close the channel - simulate source idle at WAL tail.
        // The timer should flush the partial batch within max_ms.
        // Wait up to 2 seconds for the sink to receive the events.
        let cancel_clone = cancel.clone();
        let sink_clone = Arc::clone(&sink);
        let wait_handle = tokio::spawn(async move {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            loop {
                if sink_clone.delivery_count() >= 3 {
                    // Events delivered - cancel coordinator to stop the test.
                    cancel_clone.cancel();
                    return true;
                }
                if tokio::time::Instant::now() > deadline {
                    cancel_clone.cancel();
                    return false;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });

        let _ = coord.run(rx, cancel, pause_rx).await;
        let flushed = wait_handle.await.unwrap();

        assert!(
            flushed,
            "partial batch (3 events) should be flushed by timer within max_ms, \
             but sink received {} events",
            sink.delivery_count()
        );
        assert_eq!(sink.delivery_count(), 3);

        // Checkpoint should also be committed.
        let cp = store.get_raw("src::sink::kafka").await.unwrap();
        assert_eq!(cp.unwrap(), b"{\"pos\":99}");
    }

    // ── DLQ integration tests ────────────────────────────────────────────

    /// Mock sink that returns DLQ failures for events at specific indices.
    /// Simulates a sink where some events fail serialization/routing but
    /// the rest are delivered successfully.
    struct DlqMockSink {
        id: String,
        /// Event indices (within each batch) that should fail.
        fail_indices: Vec<usize>,
        delivered: AtomicUsize,
    }

    impl DlqMockSink {
        fn new(id: &str, fail_indices: Vec<usize>) -> Arc<Self> {
            Arc::new(Self {
                id: id.to_string(),
                fail_indices,
                delivered: AtomicUsize::new(0),
            })
        }

        fn delivery_count(&self) -> usize {
            self.delivered.load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    #[async_trait::async_trait]
    impl deltaforge_core::Sink for DlqMockSink {
        fn id(&self) -> &str {
            &self.id
        }

        fn required(&self) -> bool {
            true
        }

        async fn send(&self, _event: &Event) -> SinkResult<()> {
            Ok(())
        }

        async fn send_batch(
            &self,
            events: &[Event],
        ) -> SinkResult<deltaforge_core::BatchResult> {
            let mut dlq_failures = Vec::new();
            let mut delivered = 0usize;

            for (i, _) in events.iter().enumerate() {
                if self.fail_indices.contains(&i) {
                    dlq_failures.push((
                        i,
                        SinkError::Serialization {
                            details: format!(
                                "mock serialization failure at index {i}"
                            )
                            .into(),
                        },
                    ));
                } else {
                    delivered += 1;
                }
            }

            self.delivered
                .fetch_add(delivered, std::sync::atomic::Ordering::Relaxed);
            Ok(deltaforge_core::BatchResult { dlq_failures })
        }
    }

    /// Send a batch where some events fail serialization → DLQ.
    /// Verify: good events delivered, bad events in DLQ, pipeline continues,
    /// checkpoint committed.
    #[tokio::test]
    async fn test_dlq_routes_failed_events_and_pipeline_continues() {
        use checkpoints::MemCheckpointStore;
        use deltaforge_config::DlqStreamConfig;
        use storage::MemoryStorageBackend;

        let ckpt_store = Arc::new(MemCheckpointStore::new().unwrap());
        let storage_backend: storage::ArcStorageBackend =
            Arc::new(MemoryStorageBackend::new());

        // Sink fails events at index 1 and 3 (out of 5).
        let sink = DlqMockSink::new("kafka", vec![1, 3]);
        let sinks: Vec<ArcDynSink> = vec![Arc::clone(&sink) as ArcDynSink];

        let cp_fn =
            build_commit_fn(ckpt_store.clone(), "src::sink::kafka".to_string());
        let processors: Arc<[deltaforge_core::ArcDynProcessor]> =
            Arc::from(vec![]);
        let batch_processor =
            build_batch_processor(processors, "test".to_string());

        // Create DLQ writer.
        let dlq_writer = Arc::new(crate::dlq::DlqWriter::new(
            storage_backend.clone(),
            "test-dlq".to_string(),
            DlqStreamConfig::default(),
            256 * 1024,
        ));

        let coord = Coordinator::builder("test-dlq")
            .sinks(sinks)
            .batch_config(Some(BatchConfig {
                max_events: Some(10),
                max_bytes: None,
                max_ms: Some(100),
                respect_source_tx: None,
                max_inflight: Some(1),
                ..BatchConfig::default()
            }))
            .commit_fn("kafka", cp_fn)
            .process_fn(batch_processor)
            .dlq_writer(dlq_writer.clone())
            .build();

        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let cancel = tokio_util::sync::CancellationToken::new();
        let (_pause_tx, pause_rx) = tokio::sync::watch::channel(false);

        let source = deltaforge_core::SourceInfo {
            version: "test".into(),
            connector: "mysql".into(),
            name: "test".into(),
            db: "db".into(),
            schema: None,
            table: "table".into(),
            ts_ms: 0,
            snapshot: None,
            position: deltaforge_core::SourcePosition::default(),
        };

        // Send 5 events. Events at index 1 and 3 will fail.
        for i in 0..5 {
            let mut ev = Event::new_row(
                deltaforge_core::EventId::mysql_row_server(1, "t", 1, 0),
                source.clone(),
                deltaforge_core::Op::Create,
                None,
                Some(serde_json::json!({"id": i})),
                0,
                0,
            );
            if i == 4 {
                ev.set_checkpoint(CheckpointMeta::from_vec(
                    b"{\"pos\":100}".to_vec(),
                ));
                ev.tx_end = true;
            }
            tx.send(SourceItem::Event(ev)).await.unwrap();
        }
        drop(tx);

        let _ = coord.run(rx, cancel, pause_rx).await;

        // 3 events delivered (indices 0, 2, 4).
        assert_eq!(
            sink.delivery_count(),
            3,
            "only 3 events should be delivered (2 routed to DLQ)"
        );

        // 2 events in DLQ.
        let dlq_len = dlq_writer.len().await.unwrap();
        assert_eq!(dlq_len, 2, "2 events should be in the DLQ");

        // Verify DLQ entries have correct metadata.
        let dlq_entries = dlq_writer.peek(10).await.unwrap();
        assert_eq!(dlq_entries.len(), 2);

        let meta0 =
            deltaforge_core::journal::DlqMeta::from_json(&dlq_entries[0].meta)
                .expect("DLQ entry should have valid DlqMeta");
        assert_eq!(meta0.sink_id, "kafka");
        assert_eq!(meta0.error_kind, "serialization error");
        assert!(meta0.error_message.contains("index 1"));

        let meta1 =
            deltaforge_core::journal::DlqMeta::from_json(&dlq_entries[1].meta)
                .expect("DLQ entry should have valid DlqMeta");
        assert!(meta1.error_message.contains("index 3"));

        // Checkpoint should still be committed (pipeline continued).
        let cp = ckpt_store.get_raw("src::sink::kafka").await.unwrap();
        assert!(
            cp.is_some(),
            "checkpoint should be committed despite DLQ events"
        );
        assert_eq!(cp.unwrap(), b"{\"pos\":100}");
    }

    /// Verify that a batch where ALL events fail serialization does not
    /// call send on the sink and all events go to DLQ.
    #[tokio::test]
    async fn test_dlq_all_events_fail_no_send() {
        use checkpoints::MemCheckpointStore;
        use deltaforge_config::DlqStreamConfig;
        use storage::MemoryStorageBackend;

        let ckpt_store = Arc::new(MemCheckpointStore::new().unwrap());
        let storage_backend: storage::ArcStorageBackend =
            Arc::new(MemoryStorageBackend::new());

        // All 3 events fail.
        let sink = DlqMockSink::new("kafka", vec![0, 1, 2]);
        let sinks: Vec<ArcDynSink> = vec![Arc::clone(&sink) as ArcDynSink];

        let cp_fn =
            build_commit_fn(ckpt_store.clone(), "src::sink::kafka".to_string());
        let processors: Arc<[deltaforge_core::ArcDynProcessor]> =
            Arc::from(vec![]);
        let batch_processor =
            build_batch_processor(processors, "test".to_string());

        let dlq_writer = Arc::new(crate::dlq::DlqWriter::new(
            storage_backend.clone(),
            "test-all-fail".to_string(),
            DlqStreamConfig::default(),
            256 * 1024,
        ));

        let coord = Coordinator::builder("test-all-fail")
            .sinks(sinks)
            .batch_config(Some(BatchConfig {
                max_events: Some(10),
                max_bytes: None,
                max_ms: Some(100),
                respect_source_tx: None,
                max_inflight: Some(1),
                ..BatchConfig::default()
            }))
            .commit_fn("kafka", cp_fn)
            .process_fn(batch_processor)
            .dlq_writer(dlq_writer.clone())
            .build();

        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let cancel = tokio_util::sync::CancellationToken::new();
        let (_pause_tx, pause_rx) = tokio::sync::watch::channel(false);

        let source = deltaforge_core::SourceInfo {
            version: "test".into(),
            connector: "mysql".into(),
            name: "test".into(),
            db: "db".into(),
            schema: None,
            table: "table".into(),
            ts_ms: 0,
            snapshot: None,
            position: deltaforge_core::SourcePosition::default(),
        };

        for i in 0..3 {
            let mut ev = Event::new_row(
                deltaforge_core::EventId::mysql_row_server(1, "t", 1, 0),
                source.clone(),
                deltaforge_core::Op::Create,
                None,
                Some(serde_json::json!({"id": i})),
                0,
                0,
            );
            if i == 2 {
                ev.set_checkpoint(CheckpointMeta::from_vec(
                    b"{\"pos\":50}".to_vec(),
                ));
                ev.tx_end = true;
            }
            tx.send(SourceItem::Event(ev)).await.unwrap();
        }
        drop(tx);

        let _ = coord.run(rx, cancel, pause_rx).await;

        // No events delivered to sink.
        assert_eq!(sink.delivery_count(), 0);

        // All 3 in DLQ.
        assert_eq!(dlq_writer.len().await.unwrap(), 3);

        // Checkpoint should still be committed (batch "succeeded" with all
        // events routed to DLQ - the sink returned Ok(BatchResult) not Err).
        let cp = ckpt_store.get_raw("src::sink::kafka").await.unwrap();
        assert!(cp.is_some(), "checkpoint should be committed");
    }

    /// Without a DLQ writer configured, DLQ failures in BatchResult are
    /// silently ignored (no panic, no error). Pipeline continues normally.
    #[tokio::test]
    async fn test_dlq_failures_ignored_when_no_writer() {
        use checkpoints::MemCheckpointStore;

        let ckpt_store = Arc::new(MemCheckpointStore::new().unwrap());

        // Sink fails event at index 0 but no DLQ writer is configured.
        let sink = DlqMockSink::new("kafka", vec![0]);
        let sinks: Vec<ArcDynSink> = vec![Arc::clone(&sink) as ArcDynSink];

        let cp_fn =
            build_commit_fn(ckpt_store.clone(), "src::sink::kafka".to_string());
        let processors: Arc<[deltaforge_core::ArcDynProcessor]> =
            Arc::from(vec![]);
        let batch_processor =
            build_batch_processor(processors, "test".to_string());

        // No DLQ writer - default (None).
        let coord = Coordinator::builder("test-no-dlq")
            .sinks(sinks)
            .batch_config(Some(BatchConfig {
                max_events: Some(10),
                max_bytes: None,
                max_ms: Some(100),
                respect_source_tx: None,
                max_inflight: Some(1),
                ..BatchConfig::default()
            }))
            .commit_fn("kafka", cp_fn)
            .process_fn(batch_processor)
            .build();

        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let cancel = tokio_util::sync::CancellationToken::new();
        let (_pause_tx, pause_rx) = tokio::sync::watch::channel(false);

        let source = deltaforge_core::SourceInfo {
            version: "test".into(),
            connector: "mysql".into(),
            name: "test".into(),
            db: "db".into(),
            schema: None,
            table: "table".into(),
            ts_ms: 0,
            snapshot: None,
            position: deltaforge_core::SourcePosition::default(),
        };

        let mut ev = Event::new_row(
            deltaforge_core::EventId::mysql_row_server(1, "t", 1, 0),
            source,
            deltaforge_core::Op::Create,
            None,
            Some(serde_json::json!({"id": 1})),
            0,
            0,
        );
        ev.set_checkpoint(CheckpointMeta::from_vec(b"{\"pos\":10}".to_vec()));
        ev.tx_end = true;
        tx.send(SourceItem::Event(ev)).await.unwrap();
        drop(tx);

        // Should not panic even with DLQ failures and no writer.
        let result = coord.run(rx, cancel, pause_rx).await;
        assert!(result.is_ok(), "pipeline should complete without error");

        // Checkpoint committed.
        let cp = ckpt_store.get_raw("src::sink::kafka").await.unwrap();
        assert!(cp.is_some());
    }

    // -----------------------------------------------------------------------
    // Coordinator-level sink_batch_deadline (Phase 2d)
    // -----------------------------------------------------------------------

    /// A sink that sleeps for `delay` inside `send_batch`. Used to drive the
    /// coordinator's outer deadline.
    struct SlowSink {
        id: String,
        required: bool,
        delay: Duration,
    }

    #[async_trait::async_trait]
    impl deltaforge_core::Sink for SlowSink {
        fn id(&self) -> &str {
            &self.id
        }
        fn required(&self) -> bool {
            self.required
        }
        async fn send(&self, e: &Event) -> SinkResult<()> {
            self.send_batch(std::slice::from_ref(e)).await.map(|_| ())
        }
        async fn send_batch(
            &self,
            _events: &[Event],
        ) -> SinkResult<deltaforge_core::BatchResult> {
            tokio::time::sleep(self.delay).await;
            Ok(deltaforge_core::BatchResult::ok())
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn slow_sink_trips_coordinator_deadline_required() {
        // required: true → the coordinator's commit policy fails, but the
        // pipeline doesn't lose data (source replays on restart). We assert
        // the timeout categorization (SinkError::Backpressure) by observing
        // that the failed sink's checkpoint never advances.
        use checkpoints::MemCheckpointStore;

        let store = Arc::new(MemCheckpointStore::new().unwrap());
        let slow: Arc<SlowSink> = Arc::new(SlowSink {
            id: "slow".into(),
            required: true,
            delay: Duration::from_millis(500),
        });
        let sinks: Vec<ArcDynSink> = vec![Arc::clone(&slow) as ArcDynSink];

        let cp_fn =
            build_commit_fn(store.clone(), "src::sink::slow".to_string());
        let processors: Arc<[deltaforge_core::ArcDynProcessor]> =
            Arc::from(vec![]);
        let batch_processor =
            build_batch_processor(processors, "test-deadline".to_string());

        let coord = Coordinator::builder("test-deadline")
            .sinks(sinks)
            // Tight deadline forces the timeout to fire well before the
            // 500ms sink delay completes.
            .sink_batch_deadline(Some(Duration::from_millis(50)))
            .batch_config(Some(BatchConfig {
                max_events: Some(1),
                max_bytes: None,
                max_ms: Some(50),
                respect_source_tx: None,
                max_inflight: Some(1),
                ..BatchConfig::default()
            }))
            .commit_fn("slow", cp_fn)
            .process_fn(batch_processor)
            .build();

        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let cancel = tokio_util::sync::CancellationToken::new();
        let (_pause_tx, pause_rx) = tokio::sync::watch::channel(false);

        let source = deltaforge_core::SourceInfo {
            version: "test".into(),
            connector: "mysql".into(),
            name: "test".into(),
            db: "db".into(),
            schema: None,
            table: "t".into(),
            ts_ms: 0,
            snapshot: None,
            position: deltaforge_core::SourcePosition::default(),
        };
        let mut ev = Event::new_row(
            deltaforge_core::EventId::mysql_row_server(1, "t", 1, 0),
            source,
            deltaforge_core::Op::Create,
            None,
            Some(serde_json::json!({"id": 1})),
            0,
            0,
        );
        ev.set_checkpoint(CheckpointMeta::from_vec(b"{\"pos\":1}".to_vec()));
        ev.tx_end = true;
        tx.send(SourceItem::Event(ev)).await.unwrap();
        drop(tx);

        // Give the coordinator enough time to trip the deadline (50ms) and
        // attempt cleanup; the sink's sleep (500ms) is bypassed by the
        // timeout.
        let cancel_clone = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            cancel_clone.cancel();
        });

        let _ = coord.run(rx, cancel, pause_rx).await;

        // The required sink timed out → policy unsatisfied → checkpoint
        // never advanced. (If the deadline didn't fire, the 500ms sink
        // delay would have succeeded and the checkpoint would be set.)
        let cp = store.get_raw("src::sink::slow").await.unwrap();
        assert!(
            cp.is_none(),
            "checkpoint must not advance when required sink hits coordinator deadline"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn slow_sink_trips_coordinator_deadline_optional_kafka_progresses() {
        // Two sinks: one slow + optional, one fast + required.
        // With the coordinator deadline, the slow one times out, but the
        // fast sink still succeeds and its checkpoint advances.
        use checkpoints::MemCheckpointStore;

        let store = Arc::new(MemCheckpointStore::new().unwrap());
        let fast = MockSink::new("kafka", true);
        let slow: Arc<SlowSink> = Arc::new(SlowSink {
            id: "slow-s3".into(),
            required: false,
            delay: Duration::from_millis(500),
        });
        let sinks: Vec<ArcDynSink> = vec![
            Arc::clone(&fast) as ArcDynSink,
            Arc::clone(&slow) as ArcDynSink,
        ];

        let cp_fast =
            build_commit_fn(store.clone(), "src::sink::kafka".to_string());
        let cp_slow =
            build_commit_fn(store.clone(), "src::sink::slow-s3".to_string());
        let processors: Arc<[deltaforge_core::ArcDynProcessor]> =
            Arc::from(vec![]);
        let batch_processor = build_batch_processor(
            processors,
            "test-deadline-mixed".to_string(),
        );

        let coord = Coordinator::builder("test-deadline-mixed")
            .sinks(sinks)
            .sink_batch_deadline(Some(Duration::from_millis(50)))
            .batch_config(Some(BatchConfig {
                max_events: Some(1),
                max_bytes: None,
                max_ms: Some(50),
                respect_source_tx: None,
                max_inflight: Some(1),
                ..BatchConfig::default()
            }))
            .commit_fn("kafka", cp_fast)
            .commit_fn("slow-s3", cp_slow)
            .process_fn(batch_processor)
            .build();

        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let cancel = tokio_util::sync::CancellationToken::new();
        let (_pause_tx, pause_rx) = tokio::sync::watch::channel(false);

        let source = deltaforge_core::SourceInfo {
            version: "t".into(),
            connector: "mysql".into(),
            name: "t".into(),
            db: "db".into(),
            schema: None,
            table: "t".into(),
            ts_ms: 0,
            snapshot: None,
            position: deltaforge_core::SourcePosition::default(),
        };
        let mut ev = Event::new_row(
            deltaforge_core::EventId::mysql_row_server(1, "t", 1, 0),
            source,
            deltaforge_core::Op::Create,
            None,
            Some(serde_json::json!({"id": 1})),
            0,
            0,
        );
        ev.set_checkpoint(CheckpointMeta::from_vec(b"{\"pos\":1}".to_vec()));
        ev.tx_end = true;
        tx.send(SourceItem::Event(ev)).await.unwrap();
        drop(tx);

        let cancel_clone = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            cancel_clone.cancel();
        });

        let _ = coord.run(rx, cancel, pause_rx).await;

        // Fast sink delivered the event → its checkpoint advanced.
        assert_eq!(
            fast.delivery_count(),
            1,
            "required fast sink should have received the event"
        );
        let cp_fast = store.get_raw("src::sink::kafka").await.unwrap();
        assert!(
            cp_fast.is_some(),
            "fast sink's checkpoint advances despite slow optional sink timing out"
        );
        // Slow optional sink did NOT advance.
        let cp_slow = store.get_raw("src::sink::slow-s3").await.unwrap();
        assert!(
            cp_slow.is_none(),
            "slow optional sink's checkpoint stays behind on coordinator timeout"
        );
    }

    // ── Delivery context: the authoritative checkpoint reaches sinks ─────────

    /// One recorded delivery: (event count, checkpoint bytes, watermark bytes).
    type CtxDelivery = (usize, Vec<u8>, Option<Vec<u8>>);

    /// Records the SinkBatchContext each delivery carried.
    struct CtxSink {
        id: String,
        seen: std::sync::Mutex<Vec<CtxDelivery>>,
    }
    impl CtxSink {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                id: "ctx".into(),
                seen: std::sync::Mutex::new(Vec::new()),
            })
        }
        fn deliveries(&self) -> Vec<CtxDelivery> {
            self.seen.lock().unwrap().clone()
        }
    }
    #[async_trait::async_trait]
    impl deltaforge_core::Sink for CtxSink {
        fn id(&self) -> &str {
            &self.id
        }
        async fn send(&self, _e: &Event) -> SinkResult<()> {
            Ok(())
        }
        async fn send_batch_with_context(
            &self,
            events: &[Event],
            ctx: &deltaforge_core::SinkBatchContext,
        ) -> SinkResult<deltaforge_core::BatchResult> {
            self.seen.lock().unwrap().push((
                events.len(),
                ctx.checkpoint.as_bytes().to_vec(),
                ctx.durable_watermark.clone(),
            ));
            Ok(deltaforge_core::BatchResult::ok())
        }
    }

    struct DropLastProcessor;
    #[async_trait::async_trait]
    impl deltaforge_core::Processor for DropLastProcessor {
        fn id(&self) -> &str {
            "drop-last"
        }
        async fn process(
            &self,
            mut events: Vec<Event>,
            _ctx: &BatchContext,
        ) -> anyhow::Result<Vec<Event>> {
            events.pop();
            Ok(events)
        }
        fn identity_digest(&self) -> &str {
            "drop-last-v1"
        }
    }

    struct FanOutProcessor;
    #[async_trait::async_trait]
    impl deltaforge_core::Processor for FanOutProcessor {
        fn id(&self) -> &str {
            "fan-out"
        }
        async fn process(
            &self,
            events: Vec<Event>,
            _ctx: &BatchContext,
        ) -> anyhow::Result<Vec<Event>> {
            let mut out = events.clone();
            out.extend(events);
            Ok(out)
        }
        fn identity_digest(&self) -> &str {
            "fan-out-v1"
        }
    }

    /// Run 3 standalone events (the last carrying an `authoritative` source
    /// boundary: checkpoint + watermark) through a coordinator with `proc`,
    /// returning the sink's recorded contexts. The context is built solely from
    /// the batch's source boundary - there is no independent watermark builder.
    async fn deliver_with_processor(
        proc: deltaforge_core::ArcDynProcessor,
    ) -> Vec<CtxDelivery> {
        use checkpoints::MemCheckpointStore;
        let store = Arc::new(MemCheckpointStore::new().unwrap());
        let sink = CtxSink::new();
        let sinks: Vec<ArcDynSink> = vec![Arc::clone(&sink) as ArcDynSink];
        let cp_fn = build_commit_fn(store, "src::sink::ctx".to_string());
        let procs: Arc<[deltaforge_core::ArcDynProcessor]> =
            Arc::from(vec![proc]);
        let batch_processor = build_batch_processor(procs, "test".to_string());
        let coord = Coordinator::builder("test-ctx")
            .sinks(sinks)
            .batch_config(Some(BatchConfig {
                max_events: Some(10),
                max_ms: Some(50),
                respect_source_tx: Some(true),
                max_inflight: Some(1),
                ..BatchConfig::default()
            }))
            .commit_fn("ctx", cp_fn)
            .process_fn(batch_processor)
            .build();

        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let cancel = tokio_util::sync::CancellationToken::new();
        let (_pause_tx, pause_rx) = tokio::sync::watch::channel(false);

        let source = deltaforge_core::SourceInfo {
            version: "t".into(),
            connector: "mysql".into(),
            name: "t".into(),
            db: "db".into(),
            schema: None,
            table: "t".into(),
            ts_ms: 0,
            snapshot: None,
            position: deltaforge_core::SourcePosition::default(),
        };
        for i in 0..3 {
            let mut ev = Event::new_row(
                deltaforge_core::EventId::mysql_row_server(1, "t", 1, i),
                source.clone(),
                deltaforge_core::Op::Create,
                None,
                Some(serde_json::json!({ "id": i })),
                0,
                0,
            );
            ev.transaction = None; // standalone
            // Only the LAST event carries the authoritative source boundary.
            if i == 2 {
                ev.set_boundary(deltaforge_core::SourceBoundary {
                    checkpoint: CheckpointMeta::from_vec(
                        b"authoritative".to_vec(),
                    ),
                    durable_watermark: Some(std::sync::Arc::from(
                        b"authoritative".to_vec(),
                    )),
                });
            }
            tx.send(SourceItem::Event(ev)).await.unwrap();
        }
        drop(tx);
        coord.run(rx, cancel, pause_rx).await.unwrap();
        sink.deliveries()
    }

    #[tokio::test]
    async fn context_checkpoint_survives_filtered_last_event() {
        // The processor drops the last event (which carried the checkpoint); the
        // context checkpoint must still be the authoritative pre-processing one.
        let d = deliver_with_processor(Arc::new(DropLastProcessor)).await;
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].0, 2, "two events survived");
        assert_eq!(d[0].1, b"authoritative");
        assert_eq!(d[0].2.as_deref(), Some(&b"authoritative"[..]));
    }

    #[tokio::test]
    async fn context_present_for_fully_filtered_batch() {
        // All events dropped: the sink still receives an empty batch with the
        // authoritative context so it can publish a zero-object entry.
        let d = deliver_with_processor(Arc::new(DropAllProcessor)).await;
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].0, 0, "empty batch delivered");
        assert_eq!(d[0].1, b"authoritative");
        assert_eq!(d[0].2.as_deref(), Some(&b"authoritative"[..]));
    }

    #[tokio::test]
    async fn context_watermark_unchanged_by_fan_out() {
        // Fan-out doubles the events but must not change the source watermark.
        let d = deliver_with_processor(Arc::new(FanOutProcessor)).await;
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].0, 6, "3 events fanned out to 6");
        assert_eq!(d[0].1, b"authoritative");
        assert_eq!(d[0].2.as_deref(), Some(&b"authoritative"[..]));
    }

    /// The commit marker's boundary watermark reaches the sink context even when
    /// a processor filters the whole transaction away.
    #[tokio::test]
    async fn context_boundary_watermark_survives_full_filtering() {
        use checkpoints::MemCheckpointStore;
        let store = Arc::new(MemCheckpointStore::new().unwrap());
        let sink = CtxSink::new();
        let sinks: Vec<ArcDynSink> = vec![Arc::clone(&sink) as ArcDynSink];
        let cp_fn = build_commit_fn(store, "src::sink::ctx".to_string());
        let procs: Arc<[deltaforge_core::ArcDynProcessor]> =
            Arc::from(vec![Arc::new(DropAllProcessor) as _]);
        let batch_processor = build_batch_processor(procs, "test".to_string());
        let coord = Coordinator::builder("test-ctx-wm")
            .sinks(sinks)
            .batch_config(Some(BatchConfig {
                max_events: Some(1000),
                max_ms: Some(60_000),
                respect_source_tx: Some(true),
                max_inflight: Some(1),
                ..BatchConfig::default()
            }))
            .commit_fn("ctx", cp_fn)
            .process_fn(batch_processor)
            .build();

        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let cancel = tokio_util::sync::CancellationToken::new();
        let (_pause_tx, pause_rx) = tokio::sync::watch::channel(false);

        tx.send(SourceItem::TxBegin {
            tx_id: "gtid:1".into(),
        })
        .await
        .unwrap();
        tx.send(SourceItem::Event(tx_event(1, "gtid:1", b"row")))
            .await
            .unwrap();
        tx.send(SourceItem::TxCommit {
            tx_id: "gtid:1".into(),
            boundary: deltaforge_core::SourceBoundary {
                checkpoint: CheckpointMeta::from_vec(b"cp-1".to_vec()),
                durable_watermark: Some(std::sync::Arc::from(
                    b"WATERMARK".to_vec(),
                )),
            },
        })
        .await
        .unwrap();
        drop(tx);
        coord.run(rx, cancel, pause_rx).await.unwrap();

        let d = sink.deliveries();
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].0, 0, "fully filtered -> empty batch delivered");
        assert_eq!(
            d[0].2.as_deref(),
            Some(&b"WATERMARK"[..]),
            "boundary watermark reaches the context despite full filtering"
        );
    }

    /// A standalone (non-transactional) CDC event - e.g. a non-GTID MySQL row,
    /// which has no TxCommit - carries its durable watermark atomically via
    /// `Event::set_boundary`; that watermark reaches the sink context and
    /// survives a processor that filters the event away.
    #[tokio::test]
    async fn standalone_event_boundary_watermark_reaches_context() {
        use checkpoints::MemCheckpointStore;
        let store = Arc::new(MemCheckpointStore::new().unwrap());
        let sink = CtxSink::new();
        let sinks: Vec<ArcDynSink> = vec![Arc::clone(&sink) as ArcDynSink];
        let cp_fn = build_commit_fn(store, "src::sink::ctx".to_string());
        // Filter the event away to prove the watermark is carried on the batch,
        // not on the surviving events.
        let procs: Arc<[deltaforge_core::ArcDynProcessor]> =
            Arc::from(vec![Arc::new(DropAllProcessor) as _]);
        let batch_processor = build_batch_processor(procs, "test".to_string());
        let coord = Coordinator::builder("test-standalone-wm")
            .sinks(sinks)
            .batch_config(Some(BatchConfig {
                max_events: Some(1000),
                max_ms: Some(60_000),
                respect_source_tx: Some(true),
                max_inflight: Some(1),
                ..BatchConfig::default()
            }))
            .commit_fn("ctx", cp_fn)
            .process_fn(batch_processor)
            .build();

        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let cancel = tokio_util::sync::CancellationToken::new();
        let (_pause_tx, pause_rx) = tokio::sync::watch::channel(false);

        // A standalone event (no `transaction`, no TxBegin/TxCommit) carrying an
        // atomic boundary: checkpoint + binlog file/pos watermark.
        let mut ev = tx_event(1, "unused", b"unused");
        ev.transaction = None;
        ev.set_boundary(deltaforge_core::SourceBoundary {
            checkpoint: CheckpointMeta::from_vec(b"binlog-cp".to_vec()),
            durable_watermark: Some(std::sync::Arc::from(
                b"BINLOG-WM".to_vec(),
            )),
        });
        tx.send(SourceItem::Event(ev)).await.unwrap();
        drop(tx);
        coord.run(rx, cancel, pause_rx).await.unwrap();

        let d = sink.deliveries();
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].0, 0, "standalone event filtered -> empty batch");
        assert_eq!(
            d[0].2.as_deref(),
            Some(&b"BINLOG-WM"[..]),
            "standalone event's carried watermark reaches the context"
        );
    }

    /// Build a coordinator + CtxSink for the boundary-item tests. `max_events`
    /// controls whether the standalone rows flush before the boundary arrives.
    fn boundary_coord(
        max_events: usize,
    ) -> (Coordinator<CheckpointMeta>, Arc<CtxSink>) {
        use checkpoints::MemCheckpointStore;
        let store = Arc::new(MemCheckpointStore::new().unwrap());
        let sink = CtxSink::new();
        let sinks: Vec<ArcDynSink> = vec![Arc::clone(&sink) as ArcDynSink];
        let cp_fn = build_commit_fn(store, "src::sink::ctx".to_string());
        let batch_processor =
            build_batch_processor(Arc::from(vec![]), "test".to_string());
        let coord = Coordinator::builder("test-boundary")
            .sinks(sinks)
            .batch_config(Some(BatchConfig {
                max_events: Some(max_events),
                max_ms: Some(60_000),
                respect_source_tx: Some(true),
                max_inflight: Some(1),
                ..BatchConfig::default()
            }))
            .commit_fn("ctx", cp_fn)
            .process_fn(batch_processor)
            .build();
        (coord, sink)
    }

    fn snapshot_completion_boundary() -> SourceItem {
        SourceItem::Boundary {
            boundary: deltaforge_core::SourceBoundary {
                checkpoint: CheckpointMeta::from_vec(b"snap-done".to_vec()),
                durable_watermark: Some(std::sync::Arc::from(
                    b"COMPLETED".to_vec(),
                )),
            },
        }
    }

    fn standalone_row(id: i64) -> SourceItem {
        let mut ev = tx_event(id, "unused", b"unused");
        ev.transaction = None;
        SourceItem::Event(ev)
    }

    /// A boundary item applies to a pending snapshot batch: the buffered rows and
    /// the completion boundary are delivered together in one batch.
    #[tokio::test]
    async fn boundary_item_applies_to_pending_batch() {
        let (coord, sink) = boundary_coord(1000); // rows stay buffered
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let cancel = tokio_util::sync::CancellationToken::new();
        let (_p, pause_rx) = tokio::sync::watch::channel(false);

        tx.send(standalone_row(1)).await.unwrap();
        tx.send(standalone_row(2)).await.unwrap();
        tx.send(snapshot_completion_boundary()).await.unwrap();
        drop(tx);
        coord.run(rx, cancel, pause_rx).await.unwrap();

        let d = sink.deliveries();
        assert_eq!(d.len(), 1, "buffered rows + boundary deliver as one batch");
        assert_eq!(d[0].0, 2, "both rows present");
        assert_eq!(
            d[0].1, b"snap-done",
            "context checkpoint is the boundary's"
        );
        assert_eq!(d[0].2.as_deref(), Some(&b"COMPLETED"[..]));
    }

    /// When prior data already flushed, a boundary item still delivers - as an
    /// empty batch carrying the completion boundary (the sink can then write a
    /// zero-object entry + HEAD CAS for it).
    #[tokio::test]
    async fn boundary_item_delivers_empty_batch_when_data_already_flushed() {
        let (coord, sink) = boundary_coord(1); // each row flushes immediately
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let cancel = tokio_util::sync::CancellationToken::new();
        let (_p, pause_rx) = tokio::sync::watch::channel(false);

        tx.send(standalone_row(1)).await.unwrap();
        tx.send(snapshot_completion_boundary()).await.unwrap();
        drop(tx);
        coord.run(rx, cancel, pause_rx).await.unwrap();

        let d = sink.deliveries();
        assert_eq!(d.len(), 2, "row batch, then the boundary's empty batch");
        assert_eq!(d[0].0, 1, "first batch carried the row");
        assert_eq!(
            d[1].0, 0,
            "completion boundary delivered as an empty batch"
        );
        assert_eq!(d[1].1, b"snap-done");
        assert_eq!(d[1].2.as_deref(), Some(&b"COMPLETED"[..]));
    }
}
