//! Operator-facing incidents: one structured description of a condition that
//! stops a pipeline (or degrades it), shared by every connector.
//!
//! An incident is built from closed vocabularies only - reason, cause, action
//! and evidence-key enums - plus allow-listed evidence values, so what is
//! stored and served never contains an error's free text (driver messages can
//! carry DSNs, SQL, row values or paths). The human-readable explanation is
//! generated from those fields, never stored separately.
//!
//! A component raises a blocking incident by returning an error that carries an
//! [`IncidentDraft`] around its typed cause; the pipeline supervisor turns the
//! draft into the durable record once. Identity is deterministic: the same
//! condition (same reason and discriminator, which includes the durable epoch
//! or boundary it is about) is the same incident across retries and restarts,
//! and a later independent occurrence is a new one.

use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt;

/// Why an incident exists. Wire names are stable.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ReasonCode {
    /// A PostgreSQL source reached another cluster (or a replaced database).
    PgDifferentCluster,
    /// A PostgreSQL resume position cannot be proven available on the server.
    PgContinuityUnproven,
    /// A PostgreSQL 17+ replication slot is not a failover slot: the source
    /// runs, but cannot continue across a failover.
    PgFailoverSlotUnavailable,
    /// A MySQL resume position (GTID set) is not available on the server.
    MysqlGtidPositionUnavailable,
    /// `on_schema_drift = halt` stopped on a schema change it could not accept.
    SchemaDriftBlocked,
    /// A sink write was submitted but its outcome is unknown.
    SinkAckUncertain,
    /// A failure no mapping classifies yet.
    UnclassifiedFailure,
    /// The pipeline's open-incident limit was reached; this record counts the
    /// incidents not kept individually.
    IncidentOverflow,
    /// A sink is behind a completed snapshot generation (outside the policy
    /// that completed it, or added later): it will not receive that
    /// generation's remaining rows.
    SinkSnapshotIncomplete,
    /// An incomplete snapshot generation was replaced by the next one (for
    /// example its commit policy or sink cohort changed).
    SnapshotReplaced,
    /// A snapshot's anchor is no longer, or soon not, retained by the source.
    SnapshotAnchorUnavailable,
    /// Snapshot state is unreadable, of an unknown format, or written by
    /// another owner.
    SnapshotStateInvalid,
    /// A snapshot exceeded a hard resource bound.
    SnapshotBoundExceeded,
    /// A snapshot is approaching a resource bound.
    SnapshotBoundWarning,
}

impl ReasonCode {
    /// Every reason (bounded metric label values).
    pub const ALL: [ReasonCode; 14] = [
        Self::PgDifferentCluster,
        Self::PgContinuityUnproven,
        Self::PgFailoverSlotUnavailable,
        Self::MysqlGtidPositionUnavailable,
        Self::SchemaDriftBlocked,
        Self::SinkAckUncertain,
        Self::UnclassifiedFailure,
        Self::IncidentOverflow,
        Self::SinkSnapshotIncomplete,
        Self::SnapshotReplaced,
        Self::SnapshotAnchorUnavailable,
        Self::SnapshotStateInvalid,
        Self::SnapshotBoundExceeded,
        Self::SnapshotBoundWarning,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::PgDifferentCluster => "pg_different_cluster",
            Self::PgContinuityUnproven => "pg_continuity_unproven",
            Self::PgFailoverSlotUnavailable => "pg_failover_slot_unavailable",
            Self::MysqlGtidPositionUnavailable => {
                "mysql_gtid_position_unavailable"
            }
            Self::SchemaDriftBlocked => "schema_drift_blocked",
            Self::SinkAckUncertain => "sink_ack_uncertain",
            Self::UnclassifiedFailure => "unclassified_failure",
            Self::IncidentOverflow => "incident_overflow",
            Self::SinkSnapshotIncomplete => "sink_snapshot_incomplete",
            Self::SnapshotReplaced => "snapshot_replaced",
            Self::SnapshotAnchorUnavailable => "snapshot_anchor_unavailable",
            Self::SnapshotStateInvalid => "snapshot_state_invalid",
            Self::SnapshotBoundExceeded => "snapshot_bound_exceeded",
            Self::SnapshotBoundWarning => "snapshot_bound_warning",
        }
    }
}

/// Whether the condition can clear without an operator.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum Retryability {
    /// Transient: a retry or restart can clear it.
    AutoRetry,
    /// An operator must change something (routing, configuration, data).
    OperatorAction,
    /// Cannot clear for this source epoch.
    Terminal,
}

impl Retryability {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AutoRetry => "auto_retry",
            Self::OperatorAction => "operator_action",
            Self::Terminal => "terminal",
        }
    }
}

/// What is known about the data at the point of the incident.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum SafetyState {
    /// Stopped before crossing the known-safe boundary.
    HaltedSafe,
    /// Stopped, but an external side effect may have occurred.
    HaltedUncertain,
    /// Still processing correctly; a non-safety capability is degraded.
    RunningDegraded,
}

impl SafetyState {
    /// Every safety state (bounded metric label values).
    pub const ALL: [SafetyState; 3] = [
        Self::HaltedSafe,
        Self::HaltedUncertain,
        Self::RunningDegraded,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::HaltedSafe => "halted_safe",
            Self::HaltedUncertain => "halted_uncertain",
            Self::RunningDegraded => "running_degraded",
        }
    }

    /// Whether the pipeline is stopped by it.
    pub fn is_blocking(self) -> bool {
        !matches!(self, Self::RunningDegraded)
    }
}

/// A recommended operator action. Wire names are stable.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ActionCode {
    VerifyEndpoint,
    UseNewSourceId,
    Resnapshot,
    RestoreMissingTransactions,
    ReviewSchemaChange,
    RestartWithAdapt,
    VerifySinkState,
    InspectLogs,
    EnableFailoverSlot,
    /// Give a sink a fresh baseline (a re-snapshot reaching it, or a
    /// re-bootstrap from another sink).
    RebootstrapSink,
}

/// The pipeline part that raised it.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Component {
    Source {
        id: String,
    },
    Sink {
        id: String,
    },
    Coordinator,
    /// The pipeline as a whole (e.g. its incident overflow record).
    Pipeline,
}

impl fmt::Display for Component {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source { id } => write!(f, "source {}", safe_or_redacted(id)),
            Self::Sink { id } => write!(f, "sink {}", safe_or_redacted(id)),
            Self::Coordinator => f.write_str("coordinator"),
            Self::Pipeline => f.write_str("pipeline"),
        }
    }
}

/// The classified underlying cause (from the error's type, never its text).
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum CauseCode {
    SourceTimeout,
    SourceConnect,
    SourceAuth,
    SourcePermission,
    SourceNotFound,
    SourceIo,
    SourceCheckpoint,
    SourceIncompatible,
    SourceSchema,
    SourceLineage,
    SourceBackpressure,
    SourceOther,
    SourcePanicked,
    /// The source task ended without an error and without being stopped.
    SourceEnded,
    SinkConnect,
    SinkAuth,
    SinkIo,
    SinkSerialization,
    SinkBackpressure,
    SinkRouting,
    SinkFatal,
    SinkOther,
    OversizedTransaction,
    TransactionProtocol,
    CoordinatorOther,
    /// The coordinator stopped on its own without an error.
    CoordinatorEnded,
    /// The pipeline's incident limit (overflow accounting).
    IncidentLimit,
}

impl CauseCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SourceTimeout => "source_timeout",
            Self::SourceConnect => "source_connect",
            Self::SourceAuth => "source_auth",
            Self::SourcePermission => "source_permission",
            Self::SourceNotFound => "source_not_found",
            Self::SourceIo => "source_io",
            Self::SourceCheckpoint => "source_checkpoint",
            Self::SourceIncompatible => "source_incompatible",
            Self::SourceSchema => "source_schema",
            Self::SourceLineage => "source_lineage",
            Self::SourceBackpressure => "source_backpressure",
            Self::SourceOther => "source_other",
            Self::SourcePanicked => "source_panicked",
            Self::SourceEnded => "source_ended",
            Self::SinkConnect => "sink_connect",
            Self::SinkAuth => "sink_auth",
            Self::SinkIo => "sink_io",
            Self::SinkSerialization => "sink_serialization",
            Self::SinkBackpressure => "sink_backpressure",
            Self::SinkRouting => "sink_routing",
            Self::SinkFatal => "sink_fatal",
            Self::SinkOther => "sink_other",
            Self::OversizedTransaction => "oversized_transaction",
            Self::TransactionProtocol => "transaction_protocol",
            Self::CoordinatorOther => "coordinator_other",
            Self::CoordinatorEnded => "coordinator_ended",
            Self::IncidentLimit => "incident_limit",
        }
    }
}

/// Allow-listed evidence keys. Wire names are stable.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceKey {
    SourceId,
    SinkId,
    Slot,
    ExpectedSystemIdentifier,
    LiveSystemIdentifier,
    ExpectedDatabaseOid,
    LiveDatabaseOid,
    LineageTransition,
    CheckpointPosition,
    ServerUuid,
    GtidSet,
    Table,
    SchemaChange,
    Policy,
    ReasonClass,
    BatchBoundary,
    Attempts,
    BlockingCount,
    ObjectKey,
    ExpectedGeneration,
    ContentIdentity,
    SlotRestartPosition,
    SlotConfirmedPosition,
    RecordedTimeline,
    LiveTimeline,
    TimelineSwitchPosition,
    WalFlushPosition,
    ReadPosition,
    CheckpointChain,
    CheckpointTransition,
    RecordedChain,
    RecordedTransition,
    SnapshotChain,
}

impl EvidenceKey {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SourceId => "source_id",
            Self::SinkId => "sink_id",
            Self::Slot => "slot",
            Self::ExpectedSystemIdentifier => "expected_system_identifier",
            Self::LiveSystemIdentifier => "live_system_identifier",
            Self::ExpectedDatabaseOid => "expected_database_oid",
            Self::LiveDatabaseOid => "live_database_oid",
            Self::LineageTransition => "lineage_transition",
            Self::CheckpointPosition => "checkpoint_position",
            Self::ServerUuid => "server_uuid",
            Self::GtidSet => "gtid_set",
            Self::Table => "table",
            Self::SchemaChange => "schema_change",
            Self::Policy => "policy",
            Self::ReasonClass => "reason_class",
            Self::BatchBoundary => "batch_boundary",
            Self::Attempts => "attempts",
            Self::BlockingCount => "blocking_count",
            Self::ObjectKey => "object_key",
            Self::ExpectedGeneration => "expected_generation",
            Self::ContentIdentity => "content_identity",
            Self::SlotRestartPosition => "slot_restart_position",
            Self::SlotConfirmedPosition => "slot_confirmed_position",
            Self::RecordedTimeline => "recorded_timeline",
            Self::LiveTimeline => "live_timeline",
            Self::TimelineSwitchPosition => "timeline_switch_position",
            Self::WalFlushPosition => "wal_flush_position",
            Self::ReadPosition => "read_position",
            Self::CheckpointChain => "checkpoint_chain",
            Self::CheckpointTransition => "checkpoint_transition",
            Self::RecordedChain => "recorded_chain",
            Self::RecordedTransition => "recorded_transition",
            Self::SnapshotChain => "snapshot_chain",
        }
    }
}

/// An evidence value: short safe text, a count, or a digest standing for a
/// value too large (or not safe) to expose.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EvidenceValue {
    Text { value: String },
    Count { value: u64 },
    Digest { sha256: String, items: u64 },
}

impl fmt::Display for EvidenceValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Text { value } => f.write_str(value),
            Self::Count { value } => write!(f, "{value}"),
            Self::Digest { sha256, items } => {
                write!(f, "sha256:{} ({items} item(s))", &sha256[..16])
            }
        }
    }
}

/// Longest text an evidence value may hold.
pub const MAX_EVIDENCE_TEXT: usize = 256;
/// Most evidence entries an incident may hold.
pub const MAX_EVIDENCE_ENTRIES: usize = 16;

fn is_safe_text(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= MAX_EVIDENCE_TEXT
        && s.chars().all(|c| {
            c.is_ascii_alphanumeric()
                || matches!(c, '_' | '-' | '.' | ':' | '/' | ',' | '+' | '@')
        })
}

fn safe_or_redacted(s: &str) -> &str {
    if is_safe_text(s) { s } else { "<redacted>" }
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// Bounded, allow-listed evidence. Text that is too long or contains
/// characters outside identifiers/positions is stored as a digest.
#[derive(
    Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize,
)]
pub struct Evidence(BTreeMap<EvidenceKey, EvidenceValue>);

impl Evidence {
    pub fn new() -> Self {
        Self::default()
    }

    fn put(&mut self, key: EvidenceKey, value: EvidenceValue) -> &mut Self {
        if self.0.len() < MAX_EVIDENCE_ENTRIES || self.0.contains_key(&key) {
            self.0.insert(key, value);
        }
        self
    }

    /// An identifier, position or name: kept as text when short and made of
    /// identifier characters only, otherwise as its digest.
    pub fn text(&mut self, key: EvidenceKey, value: &str) -> &mut Self {
        let v = if is_safe_text(value) {
            EvidenceValue::Text {
                value: value.to_string(),
            }
        } else {
            EvidenceValue::Digest {
                sha256: sha256_hex(value.as_bytes()),
                items: 1,
            }
        };
        self.put(key, v)
    }

    pub fn count(&mut self, key: EvidenceKey, value: u64) -> &mut Self {
        self.put(key, EvidenceValue::Count { value })
    }

    /// A large value (e.g. a GTID set) exposed only as its digest and size.
    pub fn digest(
        &mut self,
        key: EvidenceKey,
        bytes: &[u8],
        items: u64,
    ) -> &mut Self {
        self.put(
            key,
            EvidenceValue::Digest {
                sha256: sha256_hex(bytes),
                items,
            },
        )
    }

    pub fn get(&self, key: EvidenceKey) -> Option<&EvidenceValue> {
        self.0.get(&key)
    }

    /// Whether `key` holds `value`: as text, or as the digest it was stored
    /// as. A verifier recomputes a value and checks it here, so a digest
    /// still identifies exactly one value.
    pub fn holds(&self, key: EvidenceKey, value: &str) -> bool {
        match self.0.get(&key) {
            Some(EvidenceValue::Text { value: v }) => v == value,
            Some(EvidenceValue::Digest { sha256, .. }) => {
                *sha256 == sha256_hex(value.as_bytes())
            }
            _ => false,
        }
    }

    /// The text held under `key`, if it was stored as text.
    pub fn text_of(&self, key: EvidenceKey) -> Option<&str> {
        match self.0.get(&key) {
            Some(EvidenceValue::Text { value }) => Some(value),
            _ => None,
        }
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn show(&self, key: EvidenceKey) -> String {
        self.get(key)
            .map(|v| v.to_string())
            .unwrap_or_else(|| "unknown".into())
    }
}

/// The deterministic identity of an incident (64 hex chars).
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct IncidentId(pub String);

impl fmt::Display for IncidentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// What a component knows about an incident it raises. The durable record is
/// made from it once, by the pipeline supervisor.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IncidentDraft {
    pub reason_code: ReasonCode,
    pub component: Component,
    pub retryability: Retryability,
    pub safety_state: SafetyState,
    pub cause_code: CauseCode,
    /// What makes this occurrence distinct: it must include the durable epoch
    /// or boundary the condition is about (lineage transition id, checkpoint
    /// or transaction boundary, table and schema transition, sink batch).
    /// Hashed in full; never exposed.
    pub discriminator: BTreeMap<String, String>,
    pub evidence: Evidence,
    pub actions: Vec<ActionCode>,
    /// What a scoped resolver matches on (e.g. the table a schema-drift
    /// incident is about), as an opaque digest. Not identity.
    #[serde(default)]
    pub resolve_scope: Option<String>,
}

/// The opaque scope key of `scope` within `component` (see
/// [`IncidentDraft::resolve_scope`]).
pub fn scope_key(component: &Component, scope: &str) -> String {
    let canonical =
        serde_json::json!({ "component": component, "scope": scope });
    sha256_hex(&crate::canonical_json_bytes(&canonical))
}

impl IncidentDraft {
    pub fn new(
        reason_code: ReasonCode,
        component: Component,
        retryability: Retryability,
        safety_state: SafetyState,
        cause_code: CauseCode,
    ) -> Self {
        Self {
            reason_code,
            component,
            retryability,
            safety_state,
            cause_code,
            discriminator: BTreeMap::new(),
            evidence: Evidence::new(),
            actions: Vec::new(),
            resolve_scope: None,
        }
    }

    pub fn discriminate(mut self, key: &str, value: impl Into<String>) -> Self {
        self.discriminator.insert(key.to_string(), value.into());
        self
    }

    pub fn with_evidence(mut self, f: impl FnOnce(&mut Evidence)) -> Self {
        f(&mut self.evidence);
        self
    }

    pub fn with_actions(mut self, actions: &[ActionCode]) -> Self {
        self.actions = actions.to_vec();
        self
    }

    /// Let a scoped resolver (e.g. "this table was accepted") resolve it.
    pub fn resolved_by_scope(mut self, scope: &str) -> Self {
        self.resolve_scope = Some(scope_key(&self.component, scope));
        self
    }

    /// The identity of this occurrence within `pipeline`: a hash of the full
    /// canonical (pipeline, component, reason, discriminator).
    pub fn incident_id(&self, pipeline: &str) -> IncidentId {
        let canonical = serde_json::json!({
            "v": 1,
            "pipeline": pipeline,
            "component": self.component,
            "reason": self.reason_code,
            "discriminator": self.discriminator,
        });
        IncidentId(sha256_hex(&crate::canonical_json_bytes(&canonical)))
    }

    /// The sanitized explanation, generated from codes and evidence only.
    pub fn explanation(&self) -> String {
        explain(
            self.reason_code,
            &self.component,
            self.cause_code,
            &self.evidence,
        )
    }
}

impl fmt::Display for IncidentDraft {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.explanation())
    }
}

/// The explanation template of each reason. Only enum names and evidence
/// (already allow-listed) are interpolated.
pub fn explain(
    reason: ReasonCode,
    component: &Component,
    cause: CauseCode,
    ev: &Evidence,
) -> String {
    use EvidenceKey as K;
    match reason {
        ReasonCode::PgDifferentCluster => format!(
            "{component} is connected to a different PostgreSQL cluster or \
             database (system_identifier {}, database oid {}; expected {}, {}). \
             It stopped before streaming, snapshotting or recording anything.",
            ev.show(K::LiveSystemIdentifier),
            ev.show(K::LiveDatabaseOid),
            ev.show(K::ExpectedSystemIdentifier),
            ev.show(K::ExpectedDatabaseOid),
        ),
        ReasonCode::PgContinuityUnproven => format!(
            "{component} cannot prove that its resume position {} is \
             available through slot {} ({}). It stopped before opening \
             replication.",
            ev.show(K::CheckpointPosition),
            ev.show(K::Slot),
            ev.show(K::ReasonClass),
        ),
        ReasonCode::PgFailoverSlotUnavailable => format!(
            "{component} streams through slot {}, which is not a failover \
             slot: it keeps running, but cannot continue on a promoted \
             standby after a failover.",
            ev.show(K::Slot),
        ),
        ReasonCode::MysqlGtidPositionUnavailable => format!(
            "{component} cannot resume: its GTID position {} is not available \
             on server {} ({}). It stopped before reading any event.",
            ev.show(K::GtidSet),
            ev.show(K::ServerUuid),
            ev.show(K::ReasonClass),
        ),
        ReasonCode::SchemaDriftBlocked => format!(
            "{component} stopped on a schema change of table {} ({}) under \
             on_schema_drift = {}. No event under the changed schema was \
             emitted and the checkpoint was not advanced.",
            ev.show(K::Table),
            ev.show(K::SchemaChange),
            ev.show(K::Policy),
        ),
        ReasonCode::SinkAckUncertain => format!(
            "{component} submitted a write for batch {} but its outcome is \
             unknown after {} attempt(s). The checkpoint was not advanced; the \
             batch may already be visible downstream.{}",
            ev.show(K::BatchBoundary),
            ev.show(K::Attempts),
            match ev.get(K::ObjectKey) {
                Some(_) => format!(
                    " The write was a compare-and-swap of {} from generation \
                     {} to content {}.",
                    ev.show(K::ObjectKey),
                    ev.show(K::ExpectedGeneration),
                    ev.show(K::ContentIdentity),
                ),
                None => String::new(),
            },
        ),
        ReasonCode::SinkSnapshotIncomplete => format!(
            "{component} is behind completed snapshot generation {} of chain \
             {}: it was outside the policy that completed it (or added later) \
             and will not receive that generation's remaining rows. It keeps \
             receiving changes; give it a fresh baseline.",
            ev.show(K::ExpectedGeneration),
            ev.show(K::SnapshotChain),
        ),
        ReasonCode::SnapshotReplaced => format!(
            "{component} replaced incomplete snapshot generation {} ({}): \
             the whole snapshot is copied again.",
            ev.show(K::ExpectedGeneration),
            ev.show(K::ReasonClass),
        ),
        ReasonCode::SnapshotAnchorUnavailable => format!(
            "{component} stopped snapshot generation {} before its anchor \
             could be lost ({}). Nothing is copied again automatically: \
             resnapshot explicitly.",
            ev.show(K::ExpectedGeneration),
            ev.show(K::ReasonClass),
        ),
        ReasonCode::SnapshotStateInvalid => format!(
            "{component} stopped: its snapshot state is not usable ({}). \
             Nothing is changed automatically: resnapshot explicitly.",
            ev.show(K::ReasonClass),
        ),
        ReasonCode::SnapshotBoundExceeded => format!(
            "{component} stopped snapshot generation {}: it exceeded a \
             resource bound ({}). Resnapshot explicitly after raising the \
             bound or narrowing the tables.",
            ev.show(K::ExpectedGeneration),
            ev.show(K::ReasonClass),
        ),
        ReasonCode::SnapshotBoundWarning => format!(
            "{component} snapshot generation {} is approaching a resource \
             bound ({}); it keeps running.",
            ev.show(K::ExpectedGeneration),
            ev.show(K::ReasonClass),
        ),
        ReasonCode::IncidentOverflow => format!(
            "{component} reached its open-incident limit; {} further \
             incident(s) are counted here instead of recorded individually \
             ({} of them blocking).",
            ev.show(K::Attempts),
            ev.show(K::BlockingCount),
        ),
        ReasonCode::UnclassifiedFailure => format!(
            "{component} failed ({}). The pipeline stopped; this failure is \
             not classified yet, see the logs for details.",
            cause.as_str(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn draft() -> IncidentDraft {
        IncidentDraft::new(
            ReasonCode::PgDifferentCluster,
            Component::Source { id: "pg1".into() },
            Retryability::OperatorAction,
            SafetyState::HaltedSafe,
            CauseCode::SourceLineage,
        )
        .discriminate("expected", "1:5")
        .discriminate("live", "2:5")
    }

    #[test]
    fn identity_is_stable_and_bound_to_the_whole_discriminator() {
        let a = draft();
        assert_eq!(a.incident_id("p"), draft().incident_id("p"));
        assert_eq!(a.incident_id("p").0.len(), 64);
        // Evidence and actions are not identity.
        let b = draft()
            .with_evidence(|e| {
                e.count(EvidenceKey::Attempts, 3);
            })
            .with_actions(&[ActionCode::VerifyEndpoint]);
        assert_eq!(a.incident_id("p"), b.incident_id("p"));
        // Every discriminator entry, the pipeline, component and reason are.
        assert_ne!(
            a.incident_id("p"),
            draft().discriminate("transition", "t2").incident_id("p")
        );
        assert_ne!(a.incident_id("p"), a.incident_id("q"));
        let mut other = draft();
        other.component = Component::Source { id: "pg2".into() };
        assert_ne!(a.incident_id("p"), other.incident_id("p"));
        let mut reason = draft();
        reason.reason_code = ReasonCode::UnclassifiedFailure;
        assert_ne!(a.incident_id("p"), reason.incident_id("p"));
    }

    #[test]
    fn evidence_never_keeps_unsafe_or_oversized_text() {
        let mut e = Evidence::new();
        e.text(EvidenceKey::Slot, "slot_a");
        e.text(
            EvidenceKey::SourceId,
            "postgres://user:secret@host/db password=x",
        );
        e.text(EvidenceKey::Table, &"t".repeat(MAX_EVIDENCE_TEXT + 1));
        e.text(EvidenceKey::SchemaChange, "select * from t where a = 'v'");
        assert_eq!(
            e.get(EvidenceKey::Slot),
            Some(&EvidenceValue::Text {
                value: "slot_a".into()
            })
        );
        for k in [
            EvidenceKey::SourceId,
            EvidenceKey::Table,
            EvidenceKey::SchemaChange,
        ] {
            assert!(
                matches!(e.get(k), Some(EvidenceValue::Digest { .. })),
                "{k:?}"
            );
        }
        let json = serde_json::to_string(&e).unwrap();
        assert!(!json.contains("secret") && !json.contains("select"));
    }

    #[test]
    fn evidence_is_bounded() {
        let mut e = Evidence::new();
        for (i, k) in [
            EvidenceKey::SourceId,
            EvidenceKey::SinkId,
            EvidenceKey::Slot,
            EvidenceKey::ExpectedSystemIdentifier,
            EvidenceKey::LiveSystemIdentifier,
            EvidenceKey::ExpectedDatabaseOid,
            EvidenceKey::LiveDatabaseOid,
            EvidenceKey::LineageTransition,
            EvidenceKey::CheckpointPosition,
            EvidenceKey::ServerUuid,
            EvidenceKey::GtidSet,
            EvidenceKey::Table,
            EvidenceKey::SchemaChange,
            EvidenceKey::Policy,
            EvidenceKey::ReasonClass,
            EvidenceKey::BatchBoundary,
            EvidenceKey::Attempts,
            EvidenceKey::BlockingCount,
        ]
        .into_iter()
        .enumerate()
        {
            e.count(k, i as u64);
        }
        assert_eq!(e.len(), MAX_EVIDENCE_ENTRIES);
        // An existing key can still be updated at the bound.
        e.count(EvidenceKey::SourceId, 99);
        assert_eq!(
            e.get(EvidenceKey::SourceId),
            Some(&EvidenceValue::Count { value: 99 })
        );
    }

    #[test]
    fn explanations_use_only_codes_and_evidence() {
        let d = draft().with_evidence(|e| {
            e.text(EvidenceKey::LiveSystemIdentifier, "2")
                .text(EvidenceKey::ExpectedSystemIdentifier, "1");
        });
        let s = d.explanation();
        assert!(s.contains("source pg1") && s.contains("system_identifier 2"));
        assert_eq!(d.to_string(), s);
        // An unsafe component id is never echoed.
        let mut u = IncidentDraft::new(
            ReasonCode::UnclassifiedFailure,
            Component::Sink {
                id: "a b; DROP".into(),
            },
            Retryability::AutoRetry,
            SafetyState::HaltedUncertain,
            CauseCode::SinkConnect,
        );
        u.evidence.text(EvidenceKey::SinkId, "x");
        let s = u.explanation();
        assert!(s.contains("<redacted>") && !s.contains("DROP"), "{s}");
        assert!(s.contains("sink_connect"));
    }

    #[test]
    fn wire_names_are_stable() {
        assert_eq!(
            serde_json::to_value(ReasonCode::MysqlGtidPositionUnavailable)
                .unwrap(),
            "mysql_gtid_position_unavailable"
        );
        assert_eq!(
            ReasonCode::SinkAckUncertain.as_str(),
            serde_json::to_value(ReasonCode::SinkAckUncertain).unwrap()
        );
        assert_eq!(
            serde_json::to_value(SafetyState::HaltedUncertain).unwrap(),
            "halted_uncertain"
        );
        assert_eq!(
            serde_json::to_value(CauseCode::SourceLineage).unwrap(),
            CauseCode::SourceLineage.as_str()
        );
    }
}
