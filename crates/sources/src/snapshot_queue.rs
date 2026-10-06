//! The durable state of initial snapshots: the generation control record and
//! the immutable plan items (`docs/design/snapshot-durable-queue.md`).
//!
//! A generation is one complete initial load over one sealed plan, read in one
//! database read view. Losing that view replaces the generation with the next
//! one of its snapshot chain; nothing resumes a read. The control record is the
//! only mutable record and moves only by compare-and-swap; plan items are
//! created once while their generation is `allocated` and deleted only after
//! the control record has moved past that generation.
//!
//! The control record keeps the key and slot namespace of the earlier
//! generation record (`snapshot_generation:{source}` in `snapshot_state`), so
//! it is upgraded in place.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use storage::ArcStorageBackend;

use crate::snapshot_generation::{PersistedLineage, SnapshotGenerationRecord};

/// The slot namespace of the control record.
pub const CONTROL_NS: &str = "snapshot_state";
/// The slot namespace of plan items.
pub const PLAN_NS: &str = "snapshot_plan";
/// The control record format this release writes and reads.
pub const RECORD_FORMAT: u32 = 3;
/// The plan item format this release writes and reads.
pub const ITEM_FORMAT: u32 = 1;
/// The configuration fingerprint format of control records of this release
/// (configuration only; schema is bound per plan item).
pub const CONFIG_FINGERPRINT_FORMAT: u32 = 3;
/// Plan items listed per page.
pub const ITEM_PAGE: usize = 1000;

/// The control record's key for `source`.
pub fn control_key(source: &str) -> String {
    format!("snapshot_generation:{source}")
}

/// Lifecycle state of a generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    Allocated,
    Running,
    RowsProduced,
    Completed,
}

/// The commit policy mode frozen into a generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyMode {
    All,
    Required,
    Quorum,
}

/// One sink of the frozen cohort.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicySink {
    pub id: String,
    pub required: bool,
}

/// The commit policy and sink cohort a generation completes under, frozen
/// when it is allocated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicySnapshot {
    pub mode: PolicyMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quorum: Option<u32>,
    /// Sorted by id.
    pub sinks: Vec<PolicySink>,
    pub digest: String,
}

impl PolicySnapshot {
    /// The snapshot of a configuration (sinks in any order).
    pub fn new(
        mode: PolicyMode,
        quorum: Option<u32>,
        mut sinks: Vec<PolicySink>,
    ) -> Self {
        sinks.sort_by(|a, b| a.id.cmp(&b.id));
        let mut h = Sha256::new();
        let field = |h: &mut Sha256, v: &[u8]| {
            h.update((v.len() as u64).to_be_bytes());
            h.update(v);
        };
        field(&mut h, b"dfsnappolicy:v1");
        field(
            &mut h,
            match mode {
                PolicyMode::All => b"all",
                PolicyMode::Required => b"required",
                PolicyMode::Quorum => b"quorum",
            },
        );
        field(&mut h, &quorum.unwrap_or(0).to_be_bytes());
        field(&mut h, &(sinks.len() as u64).to_be_bytes());
        for s in &sinks {
            field(&mut h, s.id.as_bytes());
            field(&mut h, &[u8::from(s.required)]);
        }
        Self {
            mode,
            quorum,
            sinks,
            digest: hex::encode(h.finalize()),
        }
    }

    /// Refuse a cohort no completion can be decided over: no sinks, an empty
    /// or duplicate sink id (one sink counted twice), a quorum that is zero,
    /// above the cohort size or set outside quorum mode, sinks out of their
    /// canonical order, or a digest its contents do not rederive.
    pub fn validate(&self) -> std::result::Result<(), String> {
        if self.sinks.is_empty() {
            return Err("the sink cohort is empty".into());
        }
        for s in &self.sinks {
            if s.id.trim().is_empty() {
                return Err("the sink cohort has an empty sink id".into());
            }
        }
        for w in self.sinks.windows(2) {
            match w[0].id.cmp(&w[1].id) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Equal => {
                    return Err(format!(
                        "sink {} appears more than once in the cohort",
                        w[0].id
                    ));
                }
                std::cmp::Ordering::Greater => {
                    return Err(
                        "the sink cohort is not in canonical order".into()
                    );
                }
            }
        }
        match (self.mode, self.quorum) {
            (PolicyMode::Quorum, Some(q)) => {
                if q == 0 || q as usize > self.sinks.len() {
                    return Err(format!(
                        "quorum {q} is outside 1..={} (the cohort size)",
                        self.sinks.len()
                    ));
                }
            }
            (PolicyMode::Quorum, None) => {
                return Err("quorum mode without a quorum".into());
            }
            (_, Some(_)) => {
                return Err("a quorum outside quorum mode".into());
            }
            (_, None) => {}
        }
        let canonical = Self::new(self.mode, self.quorum, self.sinks.clone());
        if canonical.digest != self.digest {
            return Err(
                "the policy digest does not match the policy's contents".into(),
            );
        }
        Ok(())
    }
}

impl From<&deltaforge_core::SnapshotCohort> for PolicySnapshot {
    fn from(c: &deltaforge_core::SnapshotCohort) -> Self {
        use deltaforge_core::CohortPolicy;
        let (mode, quorum) = match c.policy {
            CohortPolicy::All => (PolicyMode::All, None),
            CohortPolicy::Required => (PolicyMode::Required, None),
            CohortPolicy::Quorum(q) => (PolicyMode::Quorum, Some(q)),
        };
        Self::new(
            mode,
            quorum,
            c.sinks
                .iter()
                .map(|s| PolicySink {
                    id: s.id.clone(),
                    required: s.required,
                })
                .collect(),
        )
    }
}

/// The stream position a generation's CDC starts at.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "engine", rename_all = "snake_case")]
pub enum EngineAnchor {
    Postgres {
        lsn: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timeline: Option<u32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        chain: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        transition: Option<u64>,
    },
    Mysql {
        file: String,
        pos: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        gtid_set: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        lineage: Option<String>,
    },
}

/// The sealed plan's summary.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanSummary {
    pub sealed: bool,
    pub items: u64,
    pub bytes: u64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub digest: String,
}

/// The terminal barrier's identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Terminal {
    pub digest: String,
}

/// What justified completion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Completion {
    /// The cohort sinks at or past the terminal.
    pub acks: Vec<String>,
    /// The frontier position that satisfied the policy (stored bytes, as
    /// text).
    pub frontier: String,
}

/// A failure that requires operator recovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Blocked {
    pub reason: String,
    pub incident: String,
    pub since_ms: i64,
}

/// The generation's start barrier (design section 5.4): every allocation
/// and replacement sets it `Pending`; no row is published until it is `Done`.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum Adoption {
    #[default]
    None,
    Pending,
    Done,
}

/// The generation control record (`record_format` 3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenerationControl {
    pub record_format: u32,
    pub snapshot_chain: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub legacy_through: Option<u64>,
    pub generation: u64,
    pub lineage: PersistedLineage,
    pub fingerprint_format: u32,
    pub config_fingerprint: String,
    pub state: State,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run: Option<String>,
    pub plan: PlanSummary,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor: Option<EngineAnchor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchored_at_ms: Option<i64>,
    pub policy: PolicySnapshot,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal: Option<Terminal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion: Option<Completion>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocked: Option<Blocked>,
    #[serde(default)]
    pub adoption: Adoption,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replaced: Option<u64>,
    /// How the generation was allocated, when it matters to a start: a
    /// recovery allocation (`docs/design/recovery-cli.md`) with nothing
    /// planned yet is run in place instead of being replaced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allocation: Option<AllocationMark>,
}

/// See [`GenerationControl::allocation`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AllocationMark {
    /// Allocated by the recovery operation `resnapshot`.
    Recovery,
}

/// The digest of a control record's encoded bytes (a recovery step's
/// observation of it).
pub fn control_digest(control: &GenerationControl) -> String {
    hex::encode(Sha256::digest(
        serde_json::to_vec(control).expect("a control record serializes"),
    ))
}

/// The generation `resnapshot` replaces `control` with: the next one of its
/// chain, of its (verified) lineage, freezing the currently configured
/// table fingerprint and policy, its block cleared and its start barrier
/// pending. Pure, so a plan can bind it.
pub fn recovery_successor(
    control: &GenerationControl,
    config_fingerprint: &str,
    policy: PolicySnapshot,
) -> GenerationControl {
    GenerationControl {
        record_format: RECORD_FORMAT,
        snapshot_chain: control.snapshot_chain.clone(),
        legacy_through: control.legacy_through,
        generation: control.generation + 1,
        lineage: control.lineage.clone(),
        fingerprint_format: CONFIG_FINGERPRINT_FORMAT,
        config_fingerprint: config_fingerprint.to_string(),
        state: State::Allocated,
        run: None,
        plan: PlanSummary::default(),
        anchor: None,
        anchored_at_ms: None,
        policy,
        terminal: None,
        completion: None,
        blocked: None,
        adoption: Adoption::Pending,
        replaced: Some(control.generation),
        allocation: Some(AllocationMark::Recovery),
    }
}

/// The first generation `resnapshot` allocates for a source without a
/// control record, in the given (deterministic) chain.
pub fn recovery_first(
    snapshot_chain: String,
    lineage: PersistedLineage,
    config_fingerprint: &str,
    policy: PolicySnapshot,
) -> GenerationControl {
    GenerationControl {
        record_format: RECORD_FORMAT,
        snapshot_chain,
        legacy_through: None,
        generation: 1,
        lineage,
        fingerprint_format: CONFIG_FINGERPRINT_FORMAT,
        config_fingerprint: config_fingerprint.to_string(),
        state: State::Allocated,
        run: None,
        plan: PlanSummary::default(),
        anchor: None,
        anchored_at_ms: None,
        policy,
        terminal: None,
        completion: None,
        blocked: None,
        adoption: Adoption::Pending,
        replaced: None,
        allocation: Some(AllocationMark::Recovery),
    }
}

impl GenerationControl {
    /// The terminal digest of this generation over its sealed plan and frozen
    /// policy.
    pub fn terminal_digest(&self) -> String {
        let mut h = Sha256::new();
        for part in [
            b"dfsnapterminal:v1".as_slice(),
            self.snapshot_chain.as_bytes(),
            &self.generation.to_be_bytes(),
            self.plan.digest.as_bytes(),
            self.policy.digest.as_bytes(),
        ] {
            h.update((part.len() as u64).to_be_bytes());
            h.update(part);
        }
        hex::encode(h.finalize())
    }
}

/// One planned table of a generation (`item_format` 1). Immutable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanItem {
    pub item_format: u32,
    pub qualifier: String,
    pub table: String,
    /// The engine's identity description (PostgreSQL identity specs, MySQL
    /// identity columns).
    pub identity: serde_json::Value,
    pub cursor_kind: crate::durable_checkpoint::CursorKind,
    pub schema_version: u64,
    /// The whole registered schema model's signature (#128).
    pub signature: String,
}

/// The stored control record, as classified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Stored {
    /// A record of this release's format.
    Current {
        version: u64,
        control: Box<GenerationControl>,
    },
    /// A record of an earlier release (no `record_format`): legacy state,
    /// classified by the caller with sink-checkpoint proof.
    Legacy {
        version: u64,
        record: SnapshotGenerationRecord,
    },
}

/// Why a queue operation was refused.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum QueueError {
    #[error("snapshot state store error: {0}")]
    Store(String),
    #[error("corrupt snapshot state: {0}")]
    Corrupt(String),
    #[error("snapshot state of a format this release does not know: {0}")]
    UnsupportedFormat(String),
    #[error(
        "snapshot generation {generation} belongs to another source lineage"
    )]
    ForeignLineage { generation: u64 },
    #[error("the snapshot state changed concurrently; read it again")]
    Conflict,
    #[error(
        "plan items of generation {generation} already exist (state lost or \
         replaced): refusing to allocate it"
    )]
    StaleItems { generation: u64 },
    #[error("plan item {key} already exists with other content")]
    ItemConflict { key: String },
    #[error("snapshot generation {generation} is blocked ({reason})")]
    Blocked { generation: u64, reason: String },
    #[error("invalid snapshot state transition: {0}")]
    InvalidTransition(String),
    #[error("invalid snapshot commit policy: {0}")]
    InvalidPolicy(String),
}

type Result<T> = std::result::Result<T, QueueError>;

fn store_err(e: anyhow::Error) -> QueueError {
    QueueError::Store(format!("{e:#}"))
}

fn new_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

/// Streams a plan digest over items in key order.
#[derive(Default)]
pub struct PlanDigest {
    h: Sha256,
    items: u64,
    bytes: u64,
}

impl PlanDigest {
    pub fn add(&mut self, key: &str, item_bytes: &[u8]) {
        for part in [key.as_bytes(), item_bytes] {
            self.h.update((part.len() as u64).to_be_bytes());
            self.h.update(part);
        }
        self.items += 1;
        self.bytes += item_bytes.len() as u64;
    }

    /// The sealed summary.
    pub fn seal(self) -> PlanSummary {
        let mut h = self.h;
        h.update(self.items.to_be_bytes());
        PlanSummary {
            sealed: true,
            items: self.items,
            bytes: self.bytes,
            digest: hex::encode(h.finalize()),
        }
    }
}

/// The snapshot state of one source.
#[derive(Clone)]
pub struct QueueStore {
    backend: ArcStorageBackend,
    source: String,
}

impl QueueStore {
    pub fn new(backend: ArcStorageBackend, source: &str) -> Self {
        Self {
            backend,
            source: source.to_string(),
        }
    }

    fn key(&self) -> String {
        control_key(&self.source)
    }

    /// The prefix of generation `g`'s plan items.
    fn item_prefix(&self, generation: u64) -> String {
        format!("{}/{generation:016x}/", hex::encode(&self.source))
    }

    /// The plan item key of `qualifier.table` in generation `g`; key order is
    /// the bytewise `(qualifier, table)` discovery order.
    pub fn item_key(
        &self,
        generation: u64,
        qualifier: &str,
        table: &str,
    ) -> String {
        format!(
            "{}{}/{}",
            self.item_prefix(generation),
            hex::encode(qualifier),
            hex::encode(table)
        )
    }

    /// Read and classify the control record (fails closed on undecodable or
    /// unknown formats).
    pub async fn read(&self) -> Result<Option<Stored>> {
        let Some((version, bytes)) = self
            .backend
            .slot_get(CONTROL_NS, &self.key())
            .await
            .map_err(store_err)?
        else {
            return Ok(None);
        };
        classify(version, &bytes).map(Some)
    }

    async fn create(&self, control: &GenerationControl) -> Result<u64> {
        self.ensure_no_items(control.generation).await?;
        let bytes = encode(control)?;
        self.backend
            .slot_create(CONTROL_NS, &self.key(), &bytes)
            .await
            .map_err(store_err)?
            .ok_or(QueueError::Conflict)
    }

    async fn cas(
        &self,
        expected: u64,
        control: &GenerationControl,
    ) -> Result<u64> {
        let bytes = encode(control)?;
        if self
            .backend
            .slot_cas(CONTROL_NS, &self.key(), expected, &bytes)
            .await
            .map_err(store_err)?
        {
            Ok(expected + 1)
        } else {
            Err(QueueError::Conflict)
        }
    }

    /// Refuse a generation whose plan items already exist (the version-reuse
    /// probe: state was lost or rewound).
    async fn ensure_no_items(&self, generation: u64) -> Result<()> {
        let page = self
            .backend
            .slot_list(PLAN_NS, Some(&self.item_prefix(generation)), None, 1)
            .await
            .map_err(store_err)?;
        if page.records.is_empty() {
            Ok(())
        } else {
            Err(QueueError::StaleItems { generation })
        }
    }

    /// The first generation of a new chain (no record exists).
    pub async fn allocate_first(
        &self,
        lineage: PersistedLineage,
        config_fingerprint: &str,
        policy: PolicySnapshot,
    ) -> Result<(u64, GenerationControl)> {
        policy.validate().map_err(QueueError::InvalidPolicy)?;
        let control = GenerationControl {
            record_format: RECORD_FORMAT,
            snapshot_chain: new_id(),
            legacy_through: None,
            generation: 1,
            lineage,
            fingerprint_format: CONFIG_FINGERPRINT_FORMAT,
            config_fingerprint: config_fingerprint.to_string(),
            state: State::Allocated,
            run: None,
            plan: PlanSummary::default(),
            anchor: None,
            anchored_at_ms: None,
            policy,
            terminal: None,
            completion: None,
            blocked: None,
            adoption: Adoption::Pending,
            replaced: None,
            allocation: None,
        };
        let version = self.create(&control).await?;
        Ok((version, control))
    }

    /// Replace the stored generation by the next one (design section 4).
    ///
    /// - A current record: the next generation of its chain, its start
    ///   barrier pending. A blocked or completed generation is
    ///   refused unless `by_recovery` (the recovery CLI's `resnapshot`, or
    ///   mode `always` for a completed one).
    /// - A legacy record: the first generation of a new chain whose start
    ///   barrier accepts every legacy generation up to the stored one.
    ///
    /// Lineage is verified first; nothing is written on a refusal.
    pub async fn replace(
        &self,
        stored: &Stored,
        lineage: &PersistedLineage,
        config_fingerprint: &str,
        policy: PolicySnapshot,
        by_recovery: bool,
    ) -> Result<(u64, GenerationControl)> {
        policy.validate().map_err(QueueError::InvalidPolicy)?;
        let (version, next) = match stored {
            Stored::Current { version, control } => {
                if !control.lineage.stable_matches(lineage) {
                    return Err(QueueError::ForeignLineage {
                        generation: control.generation,
                    });
                }
                if let Some(b) = &control.blocked
                    && !by_recovery
                {
                    return Err(QueueError::Blocked {
                        generation: control.generation,
                        reason: b.reason.clone(),
                    });
                }
                if control.state == State::Completed && !by_recovery {
                    return Err(QueueError::InvalidTransition(
                        "a completed generation is replaced only explicitly"
                            .into(),
                    ));
                }
                (
                    *version,
                    GenerationControl {
                        record_format: RECORD_FORMAT,
                        snapshot_chain: control.snapshot_chain.clone(),
                        legacy_through: control.legacy_through,
                        generation: control.generation + 1,
                        lineage: lineage.clone(),
                        fingerprint_format: CONFIG_FINGERPRINT_FORMAT,
                        config_fingerprint: config_fingerprint.to_string(),
                        state: State::Allocated,
                        run: None,
                        plan: PlanSummary::default(),
                        anchor: None,
                        anchored_at_ms: None,
                        policy,
                        terminal: None,
                        completion: None,
                        blocked: None,
                        adoption: Adoption::Pending,
                        replaced: Some(control.generation),
                        allocation: None,
                    },
                )
            }
            Stored::Legacy { version, record } => {
                if !record.lineage.stable_matches(lineage) {
                    return Err(QueueError::ForeignLineage {
                        generation: record.generation,
                    });
                }
                (
                    *version,
                    GenerationControl {
                        record_format: RECORD_FORMAT,
                        snapshot_chain: new_id(),
                        legacy_through: Some(record.generation),
                        generation: record.generation + 1,
                        lineage: lineage.clone(),
                        fingerprint_format: CONFIG_FINGERPRINT_FORMAT,
                        config_fingerprint: config_fingerprint.to_string(),
                        state: State::Allocated,
                        run: None,
                        plan: PlanSummary::default(),
                        anchor: None,
                        anchored_at_ms: None,
                        policy,
                        terminal: None,
                        completion: None,
                        blocked: None,
                        adoption: Adoption::Pending,
                        replaced: Some(record.generation),
                        allocation: None,
                    },
                )
            }
        };
        self.ensure_no_items(next.generation).await?;
        let version = self.cas(version, &next).await?;
        Ok((version, next))
    }

    /// A legacy record whose completion the caller proved from the sink
    /// checkpoints at its `anchor`, upgraded in place: `completed` in a new
    /// chain whose later generations' start barriers accept its legacy
    /// positions.
    pub async fn upgrade_completed_legacy(
        &self,
        stored: &Stored,
        lineage: &PersistedLineage,
        config_fingerprint: &str,
        policy: PolicySnapshot,
        completion: Completion,
        anchor: EngineAnchor,
    ) -> Result<(u64, GenerationControl)> {
        policy.validate().map_err(QueueError::InvalidPolicy)?;
        let Stored::Legacy { version, record } = stored else {
            return Err(QueueError::InvalidTransition(
                "only a legacy record is upgraded".into(),
            ));
        };
        if !record.lineage.stable_matches(lineage) {
            return Err(QueueError::ForeignLineage {
                generation: record.generation,
            });
        }
        let control = GenerationControl {
            record_format: RECORD_FORMAT,
            snapshot_chain: new_id(),
            legacy_through: Some(record.generation),
            generation: record.generation,
            lineage: lineage.clone(),
            fingerprint_format: CONFIG_FINGERPRINT_FORMAT,
            config_fingerprint: config_fingerprint.to_string(),
            state: State::Completed,
            run: None,
            plan: PlanSummary::default(),
            anchor: Some(anchor),
            anchored_at_ms: None,
            policy,
            terminal: None,
            completion: Some(completion),
            blocked: None,
            adoption: Adoption::None,
            replaced: None,
            allocation: None,
        };
        let version = self.cas(*version, &control).await?;
        Ok((version, control))
    }

    /// Create one plan item of `control`'s generation while it is
    /// `allocated` and unsealed. Retrying with identical content is accepted;
    /// different content is a conflict.
    pub async fn put_item(
        &self,
        control: &GenerationControl,
        item: &PlanItem,
    ) -> Result<(String, Vec<u8>)> {
        if control.state != State::Allocated || control.plan.sealed {
            return Err(QueueError::InvalidTransition(
                "plan items are created only while the generation is \
                 allocated and unsealed"
                    .into(),
            ));
        }
        let key =
            self.item_key(control.generation, &item.qualifier, &item.table);
        let bytes = serde_json::to_vec(item)
            .map_err(|e| QueueError::Corrupt(e.to_string()))?;
        if self
            .backend
            .slot_create(PLAN_NS, &key, &bytes)
            .await
            .map_err(store_err)?
            .is_some()
        {
            return Ok((key, bytes));
        }
        match self
            .backend
            .slot_get(PLAN_NS, &key)
            .await
            .map_err(store_err)?
        {
            Some((_, existing)) if existing == bytes => Ok((key, bytes)),
            _ => Err(QueueError::ItemConflict { key }),
        }
    }

    /// One page of generation `g`'s plan items in key order, after `after`
    /// (exclusive); the second value is the cursor of the next page.
    pub async fn items_page(
        &self,
        generation: u64,
        after: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<(String, PlanItem)>, Option<String>)> {
        let page = self
            .backend
            .slot_list(
                PLAN_NS,
                Some(&self.item_prefix(generation)),
                after,
                limit,
            )
            .await
            .map_err(store_err)?;
        let mut items = Vec::with_capacity(page.records.len());
        for r in page.records {
            let item: PlanItem =
                serde_json::from_slice(&r.value).map_err(|e| {
                    QueueError::Corrupt(format!("plan item {}: {e}", r.key))
                })?;
            if item.item_format != ITEM_FORMAT {
                return Err(QueueError::UnsupportedFormat(format!(
                    "plan item {} of format {}",
                    r.key, item.item_format
                )));
            }
            items.push((r.key, item));
        }
        Ok((items, page.next_cursor))
    }

    /// `allocated` to `running`: the sealed plan, the anchor and the run that
    /// owns the generation.
    pub async fn seal_and_run(
        &self,
        version: u64,
        control: &GenerationControl,
        plan: PlanSummary,
        anchor: EngineAnchor,
        run: &str,
        now_ms: i64,
    ) -> Result<(u64, GenerationControl)> {
        if control.state != State::Allocated || control.blocked.is_some() {
            return Err(QueueError::InvalidTransition(format!(
                "seal from {:?}",
                control.state
            )));
        }
        if !plan.sealed {
            return Err(QueueError::InvalidTransition(
                "the plan is not sealed".into(),
            ));
        }
        let next = GenerationControl {
            state: State::Running,
            plan,
            anchor: Some(anchor),
            anchored_at_ms: Some(now_ms),
            run: Some(run.to_string()),
            ..control.clone()
        };
        let version = self.cas(version, &next).await?;
        Ok((version, next))
    }

    /// `running` to `rows_produced`, by the run that owns the generation.
    pub async fn rows_produced(
        &self,
        version: u64,
        control: &GenerationControl,
        run: &str,
    ) -> Result<(u64, GenerationControl)> {
        if control.state != State::Running
            || control.run.as_deref() != Some(run)
            || control.blocked.is_some()
        {
            return Err(QueueError::InvalidTransition(format!(
                "rows produced from {:?} by another run or blocked",
                control.state
            )));
        }
        let next = GenerationControl {
            state: State::RowsProduced,
            terminal: Some(Terminal {
                digest: control.terminal_digest(),
            }),
            ..control.clone()
        };
        let version = self.cas(version, &next).await?;
        Ok((version, next))
    }

    /// `rows_produced` to `completed`, verifying the terminal and the frozen
    /// policy.
    pub async fn complete(
        &self,
        version: u64,
        control: &GenerationControl,
        terminal_digest: &str,
        policy_digest: &str,
        completion: Completion,
    ) -> Result<(u64, GenerationControl)> {
        if control.state != State::RowsProduced || control.blocked.is_some() {
            return Err(QueueError::InvalidTransition(format!(
                "complete from {:?}",
                control.state
            )));
        }
        if control.terminal.as_ref().map(|t| t.digest.as_str())
            != Some(terminal_digest)
            || control.terminal_digest() != terminal_digest
            || control.policy.digest != policy_digest
        {
            return Err(QueueError::InvalidTransition(
                "the terminal or the frozen policy does not match".into(),
            ));
        }
        let next = GenerationControl {
            state: State::Completed,
            completion: Some(completion),
            ..control.clone()
        };
        let version = self.cas(version, &next).await?;
        Ok((version, next))
    }

    /// Block a non-completed generation until operator recovery.
    pub async fn block(
        &self,
        version: u64,
        control: &GenerationControl,
        blocked: Blocked,
    ) -> Result<(u64, GenerationControl)> {
        if control.state == State::Completed {
            return Err(QueueError::InvalidTransition(
                "a completed generation is not blocked".into(),
            ));
        }
        let next = GenerationControl {
            blocked: Some(blocked),
            ..control.clone()
        };
        let version = self.cas(version, &next).await?;
        Ok((version, next))
    }

    /// Record that every cohort sink passed the generation's start barrier.
    pub async fn adoption_done(
        &self,
        version: u64,
        control: &GenerationControl,
    ) -> Result<(u64, GenerationControl)> {
        if control.adoption != Adoption::Pending {
            return Err(QueueError::InvalidTransition(
                "no adoption is pending".into(),
            ));
        }
        let next = GenerationControl {
            adoption: Adoption::Done,
            ..control.clone()
        };
        let version = self.cas(version, &next).await?;
        Ok((version, next))
    }

    /// Write a recovery allocation (the recovery-only write): `next` over
    /// exactly `expected` (the version a plan bound), or as the first
    /// record when `expected` is `None`. Its plan items must not exist.
    pub async fn write_recovery(
        &self,
        expected: Option<u64>,
        next: &GenerationControl,
    ) -> Result<u64> {
        if next.allocation != Some(AllocationMark::Recovery)
            || next.state != State::Allocated
        {
            return Err(QueueError::InvalidTransition(
                "only a recovery allocation is written by recovery".into(),
            ));
        }
        next.policy.validate().map_err(QueueError::InvalidPolicy)?;
        match expected {
            Some(v) => {
                self.ensure_no_items(next.generation).await?;
                self.cas(v, next).await
            }
            None => self.create(next).await,
        }
    }

    /// Whether generation `g` has any plan item.
    pub async fn has_items(&self, generation: u64) -> Result<bool> {
        match self.ensure_no_items(generation).await {
            Ok(()) => Ok(false),
            Err(QueueError::StaleItems { .. }) => Ok(true),
            Err(e) => Err(e),
        }
    }

    /// Delete generation `g`'s plan items, page by page, once the control
    /// record has moved past it (a later generation, or `g` completed).
    /// Idempotent; returns how many were deleted.
    pub async fn reclaim(&self, generation: u64) -> Result<u64> {
        match self.read().await? {
            Some(Stored::Current { control, .. })
                if control.generation > generation
                    || (control.generation == generation
                        && control.state == State::Completed) => {}
            _ => {
                return Err(QueueError::InvalidTransition(format!(
                    "generation {generation} is still current"
                )));
            }
        }
        let mut deleted = 0;
        loop {
            let page = self
                .backend
                .slot_list(
                    PLAN_NS,
                    Some(&self.item_prefix(generation)),
                    None,
                    ITEM_PAGE,
                )
                .await
                .map_err(store_err)?;
            if page.records.is_empty() {
                return Ok(deleted);
            }
            for r in page.records {
                if self
                    .backend
                    .slot_delete(PLAN_NS, &r.key)
                    .await
                    .map_err(store_err)?
                {
                    deleted += 1;
                }
            }
        }
    }
}

fn encode(control: &GenerationControl) -> Result<Vec<u8>> {
    serde_json::to_vec(control).map_err(|e| QueueError::Corrupt(e.to_string()))
}

/// Classify stored control bytes by format only.
fn classify(version: u64, bytes: &[u8]) -> Result<Stored> {
    let value: serde_json::Value = serde_json::from_slice(bytes)
        .map_err(|e| QueueError::Corrupt(format!("control record: {e}")))?;
    match value.get("record_format").map(serde_json::Value::as_u64) {
        None => {
            let record: SnapshotGenerationRecord =
                serde_json::from_value(value).map_err(|e| {
                    QueueError::Corrupt(format!("legacy control record: {e}"))
                })?;
            // Legacy fingerprint formats: absent (1) and 2.
            if !matches!(record.fingerprint_format, 0 | 2) {
                return Err(QueueError::UnsupportedFormat(format!(
                    "legacy control record with fingerprint format {}",
                    record.fingerprint_format
                )));
            }
            Ok(Stored::Legacy { version, record })
        }
        Some(Some(f)) if f == u64::from(RECORD_FORMAT) => {
            let control: GenerationControl = serde_json::from_value(value)
                .map_err(|e| {
                    QueueError::Corrupt(format!("control record: {e}"))
                })?;
            control.policy.validate().map_err(|e| {
                QueueError::Corrupt(format!("control record policy: {e}"))
            })?;
            if control.fingerprint_format != CONFIG_FINGERPRINT_FORMAT {
                return Err(QueueError::UnsupportedFormat(format!(
                    "control record with fingerprint format {}",
                    control.fingerprint_format
                )));
            }
            Ok(Stored::Current {
                version,
                control: Box::new(control),
            })
        }
        Some(f) => Err(QueueError::UnsupportedFormat(format!(
            "control record of format {f:?}"
        ))),
    }
}

/// The shared contract every backend must satisfy (run against the
/// in-memory, SQLite and PostgreSQL backends).
#[cfg(test)]
pub(crate) mod contract {
    use super::*;

    pub fn lineage() -> PersistedLineage {
        PersistedLineage::Postgres {
            system_identifier: 42,
        }
    }

    pub fn policy() -> PolicySnapshot {
        PolicySnapshot::new(
            PolicyMode::Required,
            None,
            vec![
                PolicySink {
                    id: "s3".into(),
                    required: true,
                },
                PolicySink {
                    id: "kafka".into(),
                    required: false,
                },
            ],
        )
    }

    pub fn item(table: &str) -> PlanItem {
        PlanItem {
            item_format: ITEM_FORMAT,
            qualifier: "public".into(),
            table: table.into(),
            identity: serde_json::json!(["id"]),
            cursor_kind: crate::durable_checkpoint::CursorKind::Signed,
            schema_version: 1,
            signature: "sig".into(),
        }
    }

    fn anchor() -> EngineAnchor {
        EngineAnchor::Postgres {
            lsn: "0/300".into(),
            timeline: None,
            chain: None,
            transition: None,
        }
    }

    async fn plan(
        q: &QueueStore,
        control: &GenerationControl,
        tables: &[&str],
    ) -> PlanSummary {
        let mut digest = PlanDigest::default();
        for t in tables {
            let (key, bytes) = q.put_item(control, &item(t)).await.unwrap();
            digest.add(&key, &bytes);
        }
        digest.seal()
    }

    async fn current(q: &QueueStore) -> (u64, GenerationControl) {
        match q.read().await.unwrap().unwrap() {
            Stored::Current { version, control } => (version, *control),
            other => panic!("not current: {other:?}"),
        }
    }

    /// A generation's whole life: allocate, plan, seal and anchor, rows
    /// produced, completed; then its items are reclaimed.
    pub async fn lifecycle(backend: ArcStorageBackend, source: &str) {
        let q = QueueStore::new(backend, source);
        assert_eq!(q.read().await.unwrap(), None);
        let (v, c) = q.allocate_first(lineage(), "fp", policy()).await.unwrap();
        assert_eq!(
            (c.generation, c.state, c.adoption),
            (1, State::Allocated, Adoption::Pending)
        );
        let summary = plan(&q, &c, &["orders", "users", "accounts"]).await;
        // Key order is discovery order.
        let (page, _) = q.items_page(1, None, 10).await.unwrap();
        let tables: Vec<&str> =
            page.iter().map(|(_, i)| i.table.as_str()).collect();
        assert_eq!(tables, ["accounts", "orders", "users"]);

        let (v, c) = q
            .seal_and_run(v, &c, summary, anchor(), "run-1", 1_000)
            .await
            .unwrap();
        // Sealed: no further item.
        assert!(matches!(
            q.put_item(&c, &item("late")).await,
            Err(QueueError::InvalidTransition(_))
        ));
        // Only the owning run produces the rows.
        assert!(q.rows_produced(v, &c, "run-2").await.is_err());
        let (v, c) = q.rows_produced(v, &c, "run-1").await.unwrap();
        let terminal = c.terminal.clone().unwrap().digest;
        assert_eq!(terminal, c.terminal_digest());
        // Completion verifies the terminal and the frozen policy.
        let completion = Completion {
            acks: vec!["s3".into()],
            frontier: "x".into(),
        };
        assert!(
            q.complete(v, &c, "other", &c.policy.digest, completion.clone())
                .await
                .is_err()
        );
        assert!(
            q.complete(v, &c, &terminal, "other", completion.clone())
                .await
                .is_err()
        );
        // Not reclaimable while current and not completed.
        assert!(q.reclaim(1).await.is_err());
        let (_, c) = q
            .complete(v, &c, &terminal, &c.policy.digest, completion)
            .await
            .unwrap();
        assert_eq!(c.state, State::Completed);
        assert_eq!(current(&q).await.1, c);
        assert_eq!(q.reclaim(1).await.unwrap(), 3);
        assert_eq!(q.reclaim(1).await.unwrap(), 0, "idempotent");
    }

    /// Replacement within a chain: the next generation, same chain; blocked
    /// and completed generations only by recovery; stale items refused.
    pub async fn replacement(backend: ArcStorageBackend, source: &str) {
        let q = QueueStore::new(backend.clone(), source);
        let (v, c) = q.allocate_first(lineage(), "fp", policy()).await.unwrap();
        plan(&q, &c, &["a"]).await;
        let stored = q.read().await.unwrap().unwrap();
        let (_, c2) = q
            .replace(&stored, &lineage(), "fp", policy(), false)
            .await
            .unwrap();
        assert_eq!(c2.generation, 2);
        assert_eq!(c2.snapshot_chain, c.snapshot_chain);
        assert_eq!(c2.replaced, Some(1));
        // The stale read lost: nothing written.
        assert_eq!(
            q.replace(&stored, &lineage(), "fp", policy(), false).await,
            Err(QueueError::Conflict)
        );
        let _ = v;
        // Generation 1's items are reclaimable now.
        assert_eq!(q.reclaim(1).await.unwrap(), 1);

        // Another lineage: refused, record untouched.
        let before = q.read().await.unwrap().unwrap();
        let foreign = PersistedLineage::Postgres {
            system_identifier: 7,
        };
        assert_eq!(
            q.replace(&before, &foreign, "fp", policy(), false).await,
            Err(QueueError::ForeignLineage { generation: 2 })
        );
        assert_eq!(q.read().await.unwrap().unwrap(), before);

        // Blocked: halted until recovery.
        let (v, c) = current(&q).await;
        let blocked = Blocked {
            reason: "snapshot_anchor_unavailable".into(),
            incident: "inc".into(),
            since_ms: 5,
        };
        q.block(v, &c, blocked).await.unwrap();
        let stored = q.read().await.unwrap().unwrap();
        assert!(matches!(
            q.replace(&stored, &lineage(), "fp", policy(), false).await,
            Err(QueueError::Blocked { generation: 2, .. })
        ));
        let (_, c3) = q
            .replace(&stored, &lineage(), "fp", policy(), true)
            .await
            .unwrap();
        assert_eq!((c3.generation, c3.blocked.clone()), (3, None));

        // Stale items of the next generation (state rewound): refused.
        let rewound = QueueStore::new(backend, source);
        let (_, c3) = current(&rewound).await;
        plan(&rewound, &c3, &["x"]).await;
        let (v, c3) = current(&rewound).await;
        let next_items = rewound.item_key(4, "public", "y");
        rewound
            .backend
            .slot_create(PLAN_NS, &next_items, b"{}")
            .await
            .unwrap();
        let stored = Stored::Current {
            version: v,
            control: Box::new(c3),
        };
        assert_eq!(
            rewound
                .replace(&stored, &lineage(), "fp", policy(), false)
                .await,
            Err(QueueError::StaleItems { generation: 4 })
        );
    }

    /// Legacy records: replaced into a new adopting chain, or upgraded in
    /// place when proven complete; unknown formats refused untouched.
    pub async fn legacy(backend: ArcStorageBackend, source: &str) {
        let q = QueueStore::new(backend.clone(), source);
        let legacy = serde_json::json!({
            "generation": 4,
            "lineage": lineage(),
            "status": "allocated",
            "config_fingerprint": "old",
            "fingerprint_format": 2,
        });
        backend
            .slot_create(
                CONTROL_NS,
                &control_key(source),
                &serde_json::to_vec(&legacy).unwrap(),
            )
            .await
            .unwrap();
        let stored = q.read().await.unwrap().unwrap();
        assert!(matches!(stored, Stored::Legacy { .. }));
        let (_, c) = q
            .replace(&stored, &lineage(), "fp", policy(), false)
            .await
            .unwrap();
        assert_eq!(c.generation, 5);
        assert_eq!(c.legacy_through, Some(4));
        assert_eq!(c.adoption, Adoption::Pending);
        let Stored::Current { version, control } =
            q.read().await.unwrap().unwrap()
        else {
            panic!("not current");
        };
        let (_, done) = q.adoption_done(version, &control).await.unwrap();
        assert_eq!(done.adoption, Adoption::Done);
        // Every replacement starts its generation's barrier again.
        let stored = q.read().await.unwrap().unwrap();
        let (_, c) = q
            .replace(&stored, &lineage(), "fp", policy(), false)
            .await
            .unwrap();
        assert_eq!((c.generation, c.adoption), (6, Adoption::Pending));

        // A proven-complete legacy record: upgraded in place.
        let q2 = QueueStore::new(backend.clone(), &format!("{source}-done"));
        backend
            .slot_create(
                CONTROL_NS,
                &control_key(&format!("{source}-done")),
                &serde_json::to_vec(&legacy).unwrap(),
            )
            .await
            .unwrap();
        let stored = q2.read().await.unwrap().unwrap();
        let (_, c) = q2
            .upgrade_completed_legacy(
                &stored,
                &lineage(),
                "fp",
                policy(),
                Completion {
                    acks: vec!["s3".into()],
                    frontier: "f".into(),
                },
                EngineAnchor::Postgres {
                    lsn: "0/10".into(),
                    timeline: None,
                    chain: None,
                    transition: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            (c.generation, c.state, c.legacy_through, c.adoption),
            (4, State::Completed, Some(4), Adoption::None)
        );

        // Unknown formats: refused, untouched.
        for (suffix, raw) in [
            (
                "rf",
                serde_json::json!({"record_format": 4, "generation": 1}),
            ),
            (
                "fp",
                serde_json::json!({
                    "generation": 1, "lineage": lineage(), "status": "running",
                    "config_fingerprint": "x", "fingerprint_format": 9
                }),
            ),
        ] {
            let s = format!("{source}-{suffix}");
            let bytes = serde_json::to_vec(&raw).unwrap();
            backend
                .slot_create(CONTROL_NS, &control_key(&s), &bytes)
                .await
                .unwrap();
            let q = QueueStore::new(backend.clone(), &s);
            assert!(matches!(
                q.read().await,
                Err(QueueError::UnsupportedFormat(_))
            ));
            assert_eq!(
                backend
                    .slot_get(CONTROL_NS, &control_key(&s))
                    .await
                    .unwrap()
                    .unwrap()
                    .1,
                bytes
            );
        }
    }

    /// Plan items: idempotent creation, conflicts, paging, corruption.
    pub async fn items(backend: ArcStorageBackend, source: &str) {
        let q = QueueStore::new(backend.clone(), source);
        let (_, c) = q.allocate_first(lineage(), "fp", policy()).await.unwrap();
        let tables: Vec<String> = (0..25).map(|i| format!("t{i:02}")).collect();
        for t in &tables {
            q.put_item(&c, &item(t)).await.unwrap();
        }
        // Retrying identical content is accepted; other content conflicts.
        q.put_item(&c, &item("t03")).await.unwrap();
        let mut other = item("t03");
        other.schema_version = 2;
        assert!(matches!(
            q.put_item(&c, &other).await,
            Err(QueueError::ItemConflict { .. })
        ));
        // Paged in key order.
        let mut seen = Vec::new();
        let mut after: Option<String> = None;
        loop {
            let (page, next) =
                q.items_page(1, after.as_deref(), 10).await.unwrap();
            seen.extend(page.into_iter().map(|(_, i)| i.table));
            match next {
                Some(n) => after = Some(n),
                None => break,
            }
        }
        assert_eq!(seen, tables);
        // A corrupt item fails closed.
        backend
            .slot_upsert(PLAN_NS, &q.item_key(1, "public", "t10"), b"garbage")
            .await
            .unwrap();
        assert!(matches!(
            q.items_page(1, None, 100).await,
            Err(QueueError::Corrupt(_))
        ));
    }

    /// A corrupt control record fails closed and is not rewritten.
    pub async fn corrupt_control(backend: ArcStorageBackend, source: &str) {
        backend
            .slot_create(CONTROL_NS, &control_key(source), b"{not json")
            .await
            .unwrap();
        let q = QueueStore::new(backend.clone(), source);
        assert!(matches!(q.read().await, Err(QueueError::Corrupt(_))));
    }

    /// Concurrent replacements: exactly one wins.
    pub async fn concurrent_replacement(
        backend: ArcStorageBackend,
        source: &str,
    ) {
        let q = QueueStore::new(backend.clone(), source);
        q.allocate_first(lineage(), "fp", policy()).await.unwrap();
        let stored = q.read().await.unwrap().unwrap();
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let (q, stored) = (q.clone(), stored.clone());
            tasks.push(tokio::spawn(async move {
                q.replace(&stored, &lineage(), "fp", policy(), false).await
            }));
        }
        let mut won = 0;
        for t in tasks {
            match t.await.unwrap() {
                Ok(_) => won += 1,
                Err(QueueError::Conflict)
                | Err(QueueError::StaleItems { .. }) => {}
                Err(e) => panic!("{e}"),
            }
        }
        assert_eq!(won, 1);
        assert_eq!(current(&q).await.1.generation, 2);
    }

    /// Every contract case, under `prefix`-unique source ids.
    pub async fn all(backend: ArcStorageBackend, prefix: &str) {
        lifecycle(backend.clone(), &format!("{prefix}-life")).await;
        replacement(backend.clone(), &format!("{prefix}-repl")).await;
        legacy(backend.clone(), &format!("{prefix}-legacy")).await;
        items(backend.clone(), &format!("{prefix}-items")).await;
        corrupt_control(backend.clone(), &format!("{prefix}-corrupt")).await;
        concurrent_replacement(backend, &format!("{prefix}-conc")).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_contract_holds_in_memory() {
        let backend: ArcStorageBackend =
            Arc::new(storage::MemoryStorageBackend::new());
        contract::all(backend, "mem").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_contract_holds_on_sqlite() {
        let dir = tempfile::tempdir().unwrap();
        let backend: ArcStorageBackend =
            storage::SqliteStorageBackend::open(dir.path().join("q.db"))
                .unwrap();
        contract::all(backend, "sqlite").await;
    }

    /// The contract on a live PostgreSQL backend (run by the gate's
    /// `serial-pg` lane).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires a live PostgreSQL (DELTAFORGE_IT_PG_DSN)"]
    async fn pg_queue_contract_holds_on_postgresql() {
        let dsn = std::env::var("DELTAFORGE_IT_PG_DSN")
            .expect("DELTAFORGE_IT_PG_DSN must be set to run this test");
        let backend: ArcStorageBackend =
            storage::PostgresStorageBackend::connect(&dsn)
                .await
                .unwrap();
        let prefix = format!("pg{}", uuid::Uuid::new_v4().simple());
        contract::all(backend, &prefix).await;
    }

    /// Reopening the SQLite file after each step finds the same state.
    #[tokio::test]
    async fn sqlite_state_survives_reopening() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db");
        let open = || -> ArcStorageBackend {
            storage::SqliteStorageBackend::open(&path).unwrap()
        };
        let q = QueueStore::new(open(), "src");
        let (_, c) = q
            .allocate_first(contract::lineage(), "fp", contract::policy())
            .await
            .unwrap();
        q.put_item(&c, &contract::item("a")).await.unwrap();
        drop(q);
        let q = QueueStore::new(open(), "src");
        let Some(Stored::Current { control, .. }) = q.read().await.unwrap()
        else {
            panic!("record lost");
        };
        assert_eq!(*control, c);
        assert_eq!(q.items_page(1, None, 10).await.unwrap().0.len(), 1);
    }

    /// SQLite with `synchronous=NORMAL` may lose the most recent commits on
    /// power loss: an earlier copy of the files is a consistent earlier state,
    /// from which the store continues (a lost tail costs a replacement, never
    /// a mixed record).
    #[tokio::test]
    async fn sqlite_continues_from_an_earlier_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db");
        let saved = dir.path().join("saved");
        std::fs::create_dir(&saved).unwrap();
        let files = |from: &std::path::Path, to: &std::path::Path| {
            for ext in ["", "-wal", "-shm"] {
                let name = format!("q.db{ext}");
                if from.join(&name).exists() {
                    std::fs::copy(from.join(&name), to.join(&name)).unwrap();
                }
            }
        };
        {
            let q = QueueStore::new(
                storage::SqliteStorageBackend::open(&path).unwrap(),
                "src",
            );
            q.allocate_first(contract::lineage(), "fp", contract::policy())
                .await
                .unwrap();
        }
        files(dir.path(), &saved);
        {
            let q = QueueStore::new(
                storage::SqliteStorageBackend::open(&path).unwrap(),
                "src",
            );
            let stored = q.read().await.unwrap().unwrap();
            q.replace(
                &stored,
                &contract::lineage(),
                "fp",
                contract::policy(),
                false,
            )
            .await
            .unwrap();
        }
        // The later commit is lost: back to the saved files.
        files(&saved, dir.path());
        let q = QueueStore::new(
            storage::SqliteStorageBackend::open(&path).unwrap(),
            "src",
        );
        let stored = q.read().await.unwrap().unwrap();
        let Stored::Current { control, .. } = &stored else {
            panic!("not current");
        };
        assert_eq!(control.generation, 1);
        let (_, c) = q
            .replace(
                &stored,
                &contract::lineage(),
                "fp",
                contract::policy(),
                false,
            )
            .await
            .unwrap();
        assert_eq!(c.generation, 2);
    }

    /// A crash at each control write leaves either the old or the new record
    /// (never a mix), and a retry from a fresh read proceeds.
    #[tokio::test]
    async fn a_crash_at_a_control_write_leaves_a_whole_record() {
        use storage::adapters::test_util::FaultBackend;
        let fault = Arc::new(FaultBackend::new());
        let backend: ArcStorageBackend = fault.clone();
        let q = QueueStore::new(backend, "src");
        q.allocate_first(contract::lineage(), "fp", contract::policy())
            .await
            .unwrap();
        let before = q.read().await.unwrap().unwrap();
        *fault.fail_after_writes_to.lock().unwrap() =
            Some((CONTROL_NS.to_string(), 0));
        assert!(matches!(
            q.replace(
                &before,
                &contract::lineage(),
                "fp",
                contract::policy(),
                false
            )
            .await,
            Err(QueueError::Store(_))
        ));
        assert_eq!(q.read().await.unwrap().unwrap(), before);
        let (_, c) = q
            .replace(
                &before,
                &contract::lineage(),
                "fp",
                contract::policy(),
                false,
            )
            .await
            .unwrap();
        assert_eq!(c.generation, 2);
    }

    #[test]
    fn an_invalid_cohort_is_refused() {
        let s = |id: &str, required| PolicySink {
            id: id.into(),
            required,
        };
        let ab = || vec![s("a", true), s("b", false)];
        let q = |n| PolicySnapshot::new(PolicyMode::Quorum, Some(n), ab());
        // Reordered sinks are the same, valid policy.
        let fwd = q(2);
        let rev = PolicySnapshot::new(
            PolicyMode::Quorum,
            Some(2),
            vec![s("b", false), s("a", true)],
        );
        assert_eq!(fwd, rev);
        assert!(fwd.validate().is_ok());
        assert!(q(1).validate().is_ok());
        // One sink twice would count twice.
        let dup = PolicySnapshot::new(
            PolicyMode::Quorum,
            Some(2),
            vec![s("a", true), s("a", true)],
        );
        assert!(dup.validate().unwrap_err().contains("more than once"));
        for (bad, why) in [
            (q(0), "quorum 0"),
            (q(3), "quorum above the cohort size"),
            (
                PolicySnapshot::new(PolicyMode::Quorum, None, ab()),
                "no quorum",
            ),
            (
                PolicySnapshot::new(PolicyMode::All, Some(1), ab()),
                "a quorum outside quorum mode",
            ),
            (
                PolicySnapshot::new(
                    PolicyMode::All,
                    None,
                    vec![s("", true), s("a", true)],
                ),
                "empty id",
            ),
            (
                PolicySnapshot::new(PolicyMode::All, None, vec![]),
                "empty cohort",
            ),
            (
                PolicySnapshot {
                    digest: "0".repeat(64),
                    ..q(2)
                },
                "digest not rederived",
            ),
            (
                PolicySnapshot {
                    sinks: vec![s("b", false), s("a", true)],
                    ..q(2)
                },
                "non-canonical order",
            ),
            (
                PolicySnapshot {
                    quorum: Some(1),
                    ..q(2)
                },
                "contents changed under the digest",
            ),
        ] {
            assert!(bad.validate().is_err(), "{why}");
        }
    }

    #[tokio::test]
    async fn an_invalid_policy_is_never_stored_or_trusted() {
        let backend: ArcStorageBackend =
            Arc::new(storage::MemoryStorageBackend::new());
        let q = QueueStore::new(backend.clone(), "src");
        let dup = PolicySnapshot::new(
            PolicyMode::All,
            None,
            vec![
                PolicySink {
                    id: "s3".into(),
                    required: true,
                },
                PolicySink {
                    id: "s3".into(),
                    required: true,
                },
            ],
        );
        assert!(matches!(
            q.allocate_first(contract::lineage(), "fp", dup).await,
            Err(QueueError::InvalidPolicy(_))
        ));
        assert!(q.read().await.unwrap().is_none(), "nothing allocated");
        // A stored policy whose digest does not rederive is corrupt.
        q.allocate_first(contract::lineage(), "fp", contract::policy())
            .await
            .unwrap();
        let (version, bytes) = backend
            .slot_get(CONTROL_NS, &control_key("src"))
            .await
            .unwrap()
            .unwrap();
        let mut v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        v["policy"]["sinks"][0]["required"] = serde_json::json!(true);
        assert!(
            backend
                .slot_cas(
                    CONTROL_NS,
                    &control_key("src"),
                    version,
                    &serde_json::to_vec(&v).unwrap(),
                )
                .await
                .unwrap()
        );
        assert!(matches!(q.read().await, Err(QueueError::Corrupt(_))));
    }

    #[test]
    fn the_policy_digest_binds_cohort_and_parameters() {
        let s = |id: &str, required| PolicySink {
            id: id.into(),
            required,
        };
        let base = PolicySnapshot::new(
            PolicyMode::Required,
            None,
            vec![s("a", true), s("b", false)],
        );
        // Order-independent.
        assert_eq!(
            base,
            PolicySnapshot::new(
                PolicyMode::Required,
                None,
                vec![s("b", false), s("a", true)]
            )
        );
        for other in [
            PolicySnapshot::new(
                PolicyMode::All,
                None,
                vec![s("a", true), s("b", false)],
            ),
            PolicySnapshot::new(
                PolicyMode::Required,
                None,
                vec![s("a", true), s("b", true)],
            ),
            PolicySnapshot::new(PolicyMode::Required, None, vec![s("a", true)]),
            PolicySnapshot::new(
                PolicyMode::Quorum,
                Some(1),
                vec![s("a", true), s("b", false)],
            ),
            PolicySnapshot::new(
                PolicyMode::Quorum,
                Some(2),
                vec![s("a", true), s("b", false)],
            ),
        ] {
            assert_ne!(base.digest, other.digest);
        }
    }
}
