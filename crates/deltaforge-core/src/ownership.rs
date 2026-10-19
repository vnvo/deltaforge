//! Destination ownership with destination-enforced write fencing.
//!
//! A destination a sink writes rows to by key (an Elasticsearch index, a
//! ClickHouse table) is created by the sink with an immutable **marker**
//! naming its owner (deployment, pipeline, sink), the one source relation it
//! holds, its own identity and its encoding, and an immutable random
//! **instance**. Writes carry a rotating **fence** token the destination
//! itself enforces (an Elasticsearch write alias named after it, a ClickHouse
//! CHECK constraint on a reserved column): after the destination commits a
//! rotation, every newly evaluated write carrying an older fence is rejected
//! by the server. An operation the server had already accepted may still
//! complete, so writes around an ownership transfer are at least once, as
//! everywhere else.
//!
//! The destination is the enforcement authority. The state store keeps the
//! record a claim needs to recover from a crash: the destination's immutable
//! instance, its current fence and a pending one (see [`OwnershipRecord`]).
//! A claim ([`claim`]) proves both agree, then rotates the fence; every path
//! that is not a proven, committed rotation leaves the destination
//! unverified.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::incident::{
    ActionCode, CauseCode, Component, EvidenceKey as K, IncidentDraft,
    ReasonCode, Retryability, SafetyState,
};
use crate::{SinkError, SourceRelationIdentity};

/// The marker's `format`.
pub const MARKER_FORMAT: &str = "deltaforge.destination-owner";
/// The only marker version this build reads or writes.
pub const MARKER_VERSION: u64 = 1;

/// Who owns a destination: one sink of one pipeline of one deployment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerIdentity {
    /// The deployment identity: 128 random bits (32 lowercase hex) created
    /// once per state store.
    pub deployment: String,
    pub pipeline: String,
    pub sink: String,
}

/// The source relation a destination holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelationMarker {
    pub connector: String,
    pub source: String,
    pub database: String,
    pub namespace: String,
    pub table: String,
}

impl From<&SourceRelationIdentity> for RelationMarker {
    fn from(r: &SourceRelationIdentity) -> Self {
        RelationMarker {
            connector: r.connector.clone(),
            source: r.source.clone(),
            database: r.database.clone(),
            namespace: r.namespace.clone(),
            table: r.table.clone(),
        }
    }
}

impl RelationMarker {
    pub fn identity(&self) -> SourceRelationIdentity {
        SourceRelationIdentity {
            connector: self.connector.clone(),
            source: self.source.clone(),
            database: self.database.clone(),
            namespace: self.namespace.clone(),
            table: self.table.clone(),
        }
    }
}

/// The destination's own identity: a concrete index of one Elasticsearch
/// cluster, or a table (its database and name) of the ClickHouse server the
/// sink addresses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DestinationMarker {
    /// `elasticsearch` or `clickhouse`.
    pub kind: String,
    /// The Elasticsearch `cluster_uuid`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster: Option<String>,
    /// The concrete Elasticsearch index.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index: Option<String>,
    /// The ClickHouse database.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    /// The ClickHouse table.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
}

impl DestinationMarker {
    pub fn elasticsearch(cluster: &str, index: &str) -> Self {
        DestinationMarker {
            kind: "elasticsearch".into(),
            cluster: Some(cluster.into()),
            index: Some(index.into()),
            database: None,
            table: None,
        }
    }

    pub fn clickhouse(database: &str, table: &str) -> Self {
        DestinationMarker {
            kind: "clickhouse".into(),
            cluster: None,
            index: None,
            database: Some(database.into()),
            table: Some(table.into()),
        }
    }

    /// The name the destination is known by in refusals and lock keys.
    pub fn name(&self) -> String {
        match self.kind.as_str() {
            "elasticsearch" => format!(
                "elasticsearch/{}/{}",
                self.cluster.as_deref().unwrap_or(""),
                self.index.as_deref().unwrap_or("")
            ),
            _ => format!(
                "clickhouse/{}.{}",
                self.database.as_deref().unwrap_or(""),
                self.table.as_deref().unwrap_or("")
            ),
        }
    }
}

/// How rows are written to the destination.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EncodingMarker {
    /// `elasticsearch.document.v1` or `clickhouse.row.v1`.
    pub layout: String,
    /// The events' type mapping (`v2`).
    pub type_mapping: String,
    /// The ClickHouse mode (`append`, `replacing`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    /// The ClickHouse extensions column.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extensions_column: Option<String>,
}

/// The immutable marker a destination is created with. It names no fence:
/// the fence lives where the destination enforces it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Marker {
    pub format: String,
    pub version: u64,
    pub owner: OwnerIdentity,
    pub relation: RelationMarker,
    pub destination: DestinationMarker,
    pub encoding: EncodingMarker,
    /// 128 random bits (32 lowercase hex), fixed at creation.
    pub instance: String,
}

/// What a sink expects a destination's marker to say.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Expected {
    pub owner: OwnerIdentity,
    pub relation: RelationMarker,
    pub destination: DestinationMarker,
    pub encoding: EncodingMarker,
}

impl Expected {
    /// The marker of a new destination with `instance`.
    pub fn marker(&self, instance: &str) -> Marker {
        Marker {
            format: MARKER_FORMAT.into(),
            version: MARKER_VERSION,
            owner: self.owner.clone(),
            relation: self.relation.clone(),
            destination: self.destination.clone(),
            encoding: self.encoding.clone(),
            instance: instance.into(),
        }
    }

    /// A digest of everything a verification is valid for (owner, relation,
    /// destination, encoding): a verified destination is shared only with a
    /// caller expecting exactly the same.
    pub fn digest(&self) -> String {
        let v = serde_json::json!({
            "owner": self.owner,
            "relation": self.relation,
            "destination": self.destination,
            "encoding": self.encoding,
        });
        short_digest(v.to_string().as_bytes(), 16)
    }
}

/// A lowercase hex SHA-256 prefix of `bytes` (`len` bytes).
fn short_digest(bytes: &[u8], len: usize) -> String {
    use sha2::Digest;
    hex::encode(&sha2::Sha256::digest(bytes)[..len])
}

/// 128 random bits as 32 lowercase hex: a destination instance, a fence, a
/// deployment identity.
pub fn random_token() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

/// Whether `s` is a token [`random_token`] could have produced.
pub fn is_token(s: &str) -> bool {
    s.len() == 32
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Why a marker was not accepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkerFault {
    Malformed,
    UnsupportedVersion,
}

/// Parse a destination's marker (the JSON value under `deltaforge`):
/// exactly the version 1 schema, no field missing or unknown.
pub fn parse_marker(v: &serde_json::Value) -> Result<Marker, MarkerFault> {
    let obj = v.as_object().ok_or(MarkerFault::Malformed)?;
    if obj.get("format").and_then(|f| f.as_str()) != Some(MARKER_FORMAT) {
        return Err(MarkerFault::Malformed);
    }
    match obj.get("version").and_then(|v| v.as_u64()) {
        Some(MARKER_VERSION) => {}
        Some(_) => return Err(MarkerFault::UnsupportedVersion),
        None => return Err(MarkerFault::Malformed),
    }
    let m: Marker = serde_json::from_value(v.clone())
        .map_err(|_| MarkerFault::Malformed)?;
    if !is_token(&m.instance) || !is_token(&m.owner.deployment) {
        return Err(MarkerFault::Malformed);
    }
    Ok(m)
}

/// The state-store record of a destination (part of crash recovery; the
/// destination enforces): its immutable instance, the fence this
/// deployment last committed, a fence a claim is rotating to, and the
/// server identity observed at creation (ClickHouse `serverUUID()`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnershipRecord {
    pub destination_instance: String,
    pub fence: Option<String>,
    pub pending: Option<String>,
    pub server_identity: Option<String>,
}

/// The durable store of ownership records: create-only and
/// compare-and-swap by version.
#[async_trait]
pub trait OwnershipStore: Send + Sync {
    /// The record and its version.
    async fn get(
        &self,
        key: &str,
    ) -> anyhow::Result<Option<(u64, OwnershipRecord)>>;

    /// Create the record; `None` when one exists.
    async fn create(
        &self,
        key: &str,
        rec: &OwnershipRecord,
    ) -> anyhow::Result<Option<u64>>;

    /// Replace the record at `version`; `None` when it changed.
    async fn cas(
        &self,
        key: &str,
        version: u64,
        rec: &OwnershipRecord,
    ) -> anyhow::Result<Option<u64>>;

    /// Remove the record (only with the reset of the sink's checkpoint).
    async fn remove(&self, key: &str) -> anyhow::Result<bool>;
}

/// An in-memory store (tests).
#[derive(Default)]
pub struct MemoryOwnershipStore {
    records: std::sync::Mutex<
        std::collections::HashMap<String, (u64, OwnershipRecord)>,
    >,
}

impl MemoryOwnershipStore {
    /// A copy of every record (a cloned or restored state store).
    pub fn copy(&self) -> MemoryOwnershipStore {
        MemoryOwnershipStore {
            records: std::sync::Mutex::new(
                self.records.lock().expect("records").clone(),
            ),
        }
    }
}

#[async_trait]
impl OwnershipStore for MemoryOwnershipStore {
    async fn get(
        &self,
        key: &str,
    ) -> anyhow::Result<Option<(u64, OwnershipRecord)>> {
        Ok(self.records.lock().expect("records").get(key).cloned())
    }

    async fn create(
        &self,
        key: &str,
        rec: &OwnershipRecord,
    ) -> anyhow::Result<Option<u64>> {
        let mut m = self.records.lock().expect("records");
        if m.contains_key(key) {
            return Ok(None);
        }
        m.insert(key.to_string(), (1, rec.clone()));
        Ok(Some(1))
    }

    async fn cas(
        &self,
        key: &str,
        version: u64,
        rec: &OwnershipRecord,
    ) -> anyhow::Result<Option<u64>> {
        let mut m = self.records.lock().expect("records");
        match m.get_mut(key) {
            Some((v, r)) if *v == version => {
                *v += 1;
                *r = rec.clone();
                Ok(Some(*v))
            }
            _ => Ok(None),
        }
    }

    async fn remove(&self, key: &str) -> anyhow::Result<bool> {
        Ok(self.records.lock().expect("records").remove(key).is_some())
    }
}

/// Why a destination was refused (`sink_destination_conflict` classes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnershipClass {
    MarkerMissing,
    MarkerMalformed,
    MarkerUnsupportedVersion,
    OwnedByOther,
    BoundToAnotherRelation,
    DestinationIdentityMismatch,
    EncodingMismatch,
    Fenced,
    FenceMalformed,
    AliasNotSupported,
    DestinationMissing,
    UnrecordedDestination,
    ServerChanged,
    FencingUnenforced,
    UnsupportedTopology,
}

impl OwnershipClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MarkerMissing => "marker_missing",
            Self::MarkerMalformed => "marker_malformed",
            Self::MarkerUnsupportedVersion => "marker_unsupported_version",
            Self::OwnedByOther => "owned_by_other",
            Self::BoundToAnotherRelation => "bound_to_another_relation",
            Self::DestinationIdentityMismatch => {
                "destination_identity_mismatch"
            }
            Self::EncodingMismatch => "encoding_mismatch",
            Self::Fenced => "fenced",
            Self::FenceMalformed => "fence_malformed",
            Self::AliasNotSupported => "alias_not_supported",
            Self::DestinationMissing => "destination_missing",
            Self::UnrecordedDestination => "unrecorded_destination",
            Self::ServerChanged => "server_changed",
            Self::FencingUnenforced => "fencing_unenforced",
            Self::UnsupportedTopology => "unsupported_topology",
        }
    }

    fn action(self) -> ActionCode {
        match self {
            Self::FencingUnenforced | Self::UnsupportedTopology => {
                ActionCode::VerifyEndpoint
            }
            Self::DestinationMissing => ActionCode::RebootstrapSink,
            _ => ActionCode::VerifySinkState,
        }
    }
}

/// A claim's failure: a refusal (fatal, an incident), or the destination or
/// state store was unavailable or answered ambiguously (retried; the
/// destination stays unverified).
#[derive(Debug, Clone)]
pub enum ClaimFailure {
    Refused {
        class: OwnershipClass,
        /// The relation the destination holds, when it names another.
        bound: Option<String>,
        /// The marker's owner, when it names another.
        owner: Option<OwnerIdentity>,
    },
    Unavailable(String),
}

impl ClaimFailure {
    pub fn refused(class: OwnershipClass) -> Self {
        ClaimFailure::Refused {
            class,
            bound: None,
            owner: None,
        }
    }

    /// As a sink error: an incident for a refusal (fatal, checkpoint held),
    /// a retryable error otherwise.
    pub fn into_sink_error(
        self,
        sink_id: &str,
        destination: &DestinationMarker,
        relation: &SourceRelationIdentity,
    ) -> SinkError {
        match self {
            ClaimFailure::Unavailable(details) => SinkError::Backpressure {
                details: format!(
                    "sink {sink_id}: destination {} not verified: {details}",
                    destination.name()
                )
                .into(),
            },
            ClaimFailure::Refused {
                class,
                bound,
                owner,
            } => ownership_refusal(
                sink_id,
                destination,
                relation,
                class,
                bound.as_deref(),
                owner.as_ref(),
            ),
        }
    }
}

/// The `sink_destination_conflict` incident of an ownership refusal: halts
/// the pipeline with the sink's checkpoint held. Evidence names the sink,
/// the destination, the relation, the class and (when another owner holds
/// it) a digest of that owner; never a marker, a row or a credential.
pub fn ownership_refusal(
    sink_id: &str,
    destination: &DestinationMarker,
    relation: &SourceRelationIdentity,
    class: OwnershipClass,
    bound: Option<&str>,
    owner: Option<&OwnerIdentity>,
) -> SinkError {
    let name = destination.name();
    let relation = relation.canonical();
    let owner_digest = owner.map(|o| {
        short_digest(serde_json::to_string(o).unwrap_or_default().as_bytes(), 8)
    });
    let draft = IncidentDraft::new(
        ReasonCode::SinkDestinationConflict,
        Component::Sink {
            id: sink_id.to_string(),
        },
        Retryability::OperatorAction,
        SafetyState::HaltedSafe,
        CauseCode::SinkFatal,
    )
    .discriminate("destination", &name)
    .discriminate("class", class.as_str())
    .with_evidence(|e| {
        e.text(K::SinkId, sink_id)
            .text(K::Destination, &name)
            .text(K::Relation, &relation)
            .text(K::ReasonClass, class.as_str());
        if let Some(b) = bound {
            e.text(K::BoundRelation, b);
        }
        if let Some(d) = &owner_digest {
            e.text(K::Owner, d);
        }
    })
    .with_actions(&[class.action()]);
    SinkError::incident(
        draft,
        SinkError::Fatal {
            details: format!(
                "sink {sink_id}: destination {name} refused for relation \
                 {relation} ({})",
                class.as_str()
            )
            .into(),
        },
    )
}

/// What a destination looks like now.
#[derive(Debug, Clone)]
pub enum Observation {
    /// No such destination. `server`: the server identity it would be
    /// created on (ClickHouse).
    Absent { server: Option<String> },
    Present {
        /// The marker's JSON value; `None` when the destination has none.
        marker: Option<serde_json::Value>,
        /// The fence the destination enforces now; `Err` when its
        /// enforcement is not exactly the expected shape.
        fence: Result<String, String>,
        server: Option<String>,
    },
}

/// The result of a remote compare-and-swap (create or fence swap).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteOutcome {
    /// The destination committed it.
    Applied,
    /// The destination refused it (exists already, or the fence moved).
    Refused,
    /// No definite answer (a timeout, a broken connection).
    Unknown,
}

/// One destination's remote operations, as a sink implements them.
#[async_trait]
pub trait FencedDestination: Send + Sync {
    /// Read the destination: marker, enforced fence, server. Preflight
    /// conditions (an alias in place of an index, an unsupported engine,
    /// unproven fencing) are refusals.
    async fn read(&self) -> Result<Observation, ClaimFailure>;

    /// Create it atomically with `marker` and `fence` enforced.
    async fn create(
        &self,
        marker: &Marker,
        fence: &str,
    ) -> Result<RemoteOutcome, ClaimFailure>;

    /// Atomically replace the enforced fence `from` with `to`.
    async fn swap_fence(
        &self,
        instance: &str,
        from: &str,
        to: &str,
    ) -> Result<RemoteOutcome, ClaimFailure>;
}

#[async_trait]
impl<T: FencedDestination + ?Sized> FencedDestination for &T {
    async fn read(&self) -> Result<Observation, ClaimFailure> {
        (**self).read().await
    }
    async fn create(
        &self,
        marker: &Marker,
        fence: &str,
    ) -> Result<RemoteOutcome, ClaimFailure> {
        (**self).create(marker, fence).await
    }
    async fn swap_fence(
        &self,
        instance: &str,
        from: &str,
        to: &str,
    ) -> Result<RemoteOutcome, ClaimFailure> {
        (**self).swap_fence(instance, from, to).await
    }
}

/// A successful claim: the destination's instance and the fence writes
/// must carry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claimed {
    pub instance: String,
    pub fence: String,
}

/// How many times a claim restarts after losing a state-store race.
const CLAIM_ATTEMPTS: usize = 4;

fn store_err(e: anyhow::Error) -> ClaimFailure {
    ClaimFailure::Unavailable(format!("ownership store: {e:#}"))
}

/// Claim `dest` for `expected`: create it, or prove its marker and the
/// state-store record agree and rotate its fence. Returns the fence writes
/// must carry. Only a committed rotation (or creation) returns `Ok`; every
/// other path leaves the destination unverified.
pub async fn claim(
    dest: &dyn FencedDestination,
    store: &dyn OwnershipStore,
    key: &str,
    expected: &Expected,
) -> Result<Claimed, ClaimFailure> {
    for _ in 0..CLAIM_ATTEMPTS {
        let rec = store.get(key).await.map_err(store_err)?;
        match dest.read().await? {
            Observation::Absent { server } => {
                // Reuse an unconfirmed creation intent (its remote create
                // failed or its outcome was unknown); a committed creation
                // whose destination is gone is never recreated.
                let (version, intent) = match rec {
                    Some((_, r)) if r.fence.is_some() => {
                        return Err(ClaimFailure::refused(
                            OwnershipClass::DestinationMissing,
                        ));
                    }
                    Some((v, r)) if r.pending.is_some() => (v, r),
                    Some(_) => {
                        return Err(ClaimFailure::refused(
                            OwnershipClass::FenceMalformed,
                        ));
                    }
                    None => {
                        // Intent first: a crash after the remote create
                        // leaves the fence pending, which the next claim
                        // commits.
                        let intent = OwnershipRecord {
                            destination_instance: random_token(),
                            fence: None,
                            pending: Some(random_token()),
                            server_identity: server,
                        };
                        match store
                            .create(key, &intent)
                            .await
                            .map_err(store_err)?
                        {
                            Some(v) => (v, intent),
                            None => continue, // recorded meanwhile: read again
                        }
                    }
                };
                let fence = intent.pending.clone().expect("pending intent");
                let marker = expected.marker(&intent.destination_instance);
                match dest.create(&marker, &fence).await? {
                    RemoteOutcome::Applied => {
                        let instance = intent.destination_instance.clone();
                        let done = OwnershipRecord {
                            fence: Some(fence.clone()),
                            pending: None,
                            ..intent
                        };
                        // A failed commit keeps the fence pending: the next
                        // claim finds the destination with it and commits.
                        return match store
                            .cas(key, version, &done)
                            .await
                            .map_err(store_err)?
                        {
                            Some(_) => Ok(Claimed { instance, fence }),
                            None => Err(ClaimFailure::Unavailable(
                                "ownership record changed during creation"
                                    .into(),
                            )),
                        };
                    }
                    // It exists now (created concurrently): read it again;
                    // its marker and instance decide.
                    RemoteOutcome::Refused => continue,
                    RemoteOutcome::Unknown => {
                        return Err(ClaimFailure::Unavailable(
                            "destination creation outcome unknown".into(),
                        ));
                    }
                }
            }
            Observation::Present {
                marker,
                fence,
                server,
            } => {
                let marker = match marker {
                    None => {
                        return Err(ClaimFailure::refused(
                            OwnershipClass::MarkerMissing,
                        ));
                    }
                    Some(v) => parse_marker(&v).map_err(|f| {
                        ClaimFailure::refused(match f {
                            MarkerFault::Malformed => {
                                OwnershipClass::MarkerMalformed
                            }
                            MarkerFault::UnsupportedVersion => {
                                OwnershipClass::MarkerUnsupportedVersion
                            }
                        })
                    })?,
                };
                if marker.owner != expected.owner {
                    return Err(ClaimFailure::Refused {
                        class: OwnershipClass::OwnedByOther,
                        bound: None,
                        owner: Some(marker.owner),
                    });
                }
                if marker.relation != expected.relation {
                    return Err(ClaimFailure::Refused {
                        class: OwnershipClass::BoundToAnotherRelation,
                        bound: Some(marker.relation.identity().canonical()),
                        owner: None,
                    });
                }
                if marker.destination != expected.destination {
                    return Err(ClaimFailure::refused(
                        OwnershipClass::DestinationIdentityMismatch,
                    ));
                }
                if marker.encoding != expected.encoding {
                    return Err(ClaimFailure::refused(
                        OwnershipClass::EncodingMismatch,
                    ));
                }
                let Some((mut version, mut rec)) = rec else {
                    return Err(ClaimFailure::refused(
                        OwnershipClass::UnrecordedDestination,
                    ));
                };
                if marker.instance != rec.destination_instance {
                    return Err(ClaimFailure::refused(
                        OwnershipClass::DestinationIdentityMismatch,
                    ));
                }
                if rec.server_identity.is_some()
                    && server.is_some()
                    && rec.server_identity != server
                {
                    return Err(ClaimFailure::refused(
                        OwnershipClass::ServerChanged,
                    ));
                }
                let current = fence.map_err(|_| {
                    ClaimFailure::refused(OwnershipClass::FenceMalformed)
                })?;
                if rec.pending.as_deref() == Some(current.as_str()) {
                    // An earlier claim's remote swap (or creation) did
                    // commit: commit it here.
                    let committed = OwnershipRecord {
                        fence: Some(current.clone()),
                        pending: None,
                        ..rec.clone()
                    };
                    match store
                        .cas(key, version, &committed)
                        .await
                        .map_err(store_err)?
                    {
                        Some(v) => {
                            version = v;
                            rec = committed;
                        }
                        None => continue,
                    }
                } else if rec.fence.as_deref() != Some(current.as_str()) {
                    return Err(ClaimFailure::refused(OwnershipClass::Fenced));
                }
                // The destination enforces `current`, the one we committed:
                // rotate. A stale pending (the remote swap never happened)
                // is replaced by this claim's.
                let next = random_token();
                let rotating = OwnershipRecord {
                    pending: Some(next.clone()),
                    ..rec.clone()
                };
                let Some(v) = store
                    .cas(key, version, &rotating)
                    .await
                    .map_err(store_err)?
                else {
                    continue;
                };
                match dest
                    .swap_fence(&rec.destination_instance, &current, &next)
                    .await?
                {
                    RemoteOutcome::Applied => {
                        let done = OwnershipRecord {
                            fence: Some(next.clone()),
                            pending: None,
                            ..rotating
                        };
                        // A failed commit keeps `next` pending: the next
                        // claim finds the destination at `next` and
                        // commits it.
                        return match store
                            .cas(key, v, &done)
                            .await
                            .map_err(store_err)?
                        {
                            Some(_) => Ok(Claimed {
                                instance: rec.destination_instance.clone(),
                                fence: next,
                            }),
                            None => Err(ClaimFailure::Unavailable(
                                "ownership record changed during rotation"
                                    .into(),
                            )),
                        };
                    }
                    RemoteOutcome::Refused => {
                        return Err(ClaimFailure::refused(
                            OwnershipClass::Fenced,
                        ));
                    }
                    // Keep `next` pending; a fresh read resolves it.
                    RemoteOutcome::Unknown => {
                        return Err(ClaimFailure::Unavailable(
                            "fence rotation outcome unknown".into(),
                        ));
                    }
                }
            }
        }
    }
    Err(ClaimFailure::Unavailable(
        "ownership record kept changing during the claim".into(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// A destination that enforces its fence like the real ones: a compare
    /// and swap on the enforced token, and writes checked against it.
    #[derive(Default)]
    struct State {
        /// `(marker, fence, server)`.
        dest: Option<(serde_json::Value, String, Option<String>)>,
        /// The next remote CAS answers this, `(outcome, whether it applied)`.
        inject_swap: Option<(RemoteOutcome, bool)>,
        inject_create: Option<(RemoteOutcome, bool)>,
        server: Option<String>,
        broken_fence: bool,
    }

    #[derive(Clone, Default)]
    struct Fake(Arc<Mutex<State>>);

    impl Fake {
        fn write(&self, fence: &str) -> bool {
            matches!(&self.0.lock().unwrap().dest, Some((_, f, _)) if f == fence)
        }
    }

    #[async_trait]
    impl FencedDestination for Fake {
        async fn read(&self) -> Result<Observation, ClaimFailure> {
            tokio::task::yield_now().await;
            let s = self.0.lock().unwrap();
            Ok(match &s.dest {
                None => Observation::Absent {
                    server: s.server.clone(),
                },
                Some((m, f, server)) => Observation::Present {
                    marker: (!m.is_null()).then(|| m.clone()),
                    fence: if s.broken_fence {
                        Err("two fences".into())
                    } else {
                        Ok(f.clone())
                    },
                    server: server.clone(),
                },
            })
        }

        async fn create(
            &self,
            marker: &Marker,
            fence: &str,
        ) -> Result<RemoteOutcome, ClaimFailure> {
            tokio::task::yield_now().await;
            let mut s = self.0.lock().unwrap();
            let (outcome, apply) = s
                .inject_create
                .take()
                .unwrap_or((RemoteOutcome::Applied, true));
            if s.dest.is_some() {
                return Ok(RemoteOutcome::Refused);
            }
            if apply {
                let server = s.server.clone();
                s.dest = Some((
                    serde_json::to_value(marker).unwrap(),
                    fence.into(),
                    server,
                ));
            }
            Ok(outcome)
        }

        async fn swap_fence(
            &self,
            _instance: &str,
            from: &str,
            to: &str,
        ) -> Result<RemoteOutcome, ClaimFailure> {
            tokio::task::yield_now().await;
            let mut s = self.0.lock().unwrap();
            let (outcome, apply) = s
                .inject_swap
                .take()
                .unwrap_or((RemoteOutcome::Applied, true));
            match &mut s.dest {
                Some((_, f, _)) if f == from => {
                    if apply {
                        *f = to.into();
                    }
                    Ok(outcome)
                }
                _ => Ok(RemoteOutcome::Refused),
            }
        }
    }

    /// A store whose next `cas` calls fail when `fail_cas` is set.
    #[derive(Default)]
    struct FlakyStore {
        inner: MemoryOwnershipStore,
        fail_cas: Mutex<usize>,
    }

    #[async_trait]
    impl OwnershipStore for FlakyStore {
        async fn get(
            &self,
            key: &str,
        ) -> anyhow::Result<Option<(u64, OwnershipRecord)>> {
            tokio::task::yield_now().await;
            self.inner.get(key).await
        }
        async fn create(
            &self,
            key: &str,
            rec: &OwnershipRecord,
        ) -> anyhow::Result<Option<u64>> {
            self.inner.create(key, rec).await
        }
        async fn cas(
            &self,
            key: &str,
            version: u64,
            rec: &OwnershipRecord,
        ) -> anyhow::Result<Option<u64>> {
            let fail = {
                let mut n = self.fail_cas.lock().unwrap();
                let fail = *n > 0;
                if fail {
                    *n -= 1;
                }
                fail
            };
            if fail {
                anyhow::bail!("store unavailable");
            }
            self.inner.cas(key, version, rec).await
        }
        async fn remove(&self, key: &str) -> anyhow::Result<bool> {
            self.inner.remove(key).await
        }
    }

    impl FlakyStore {
        /// A copy (a cloned or restored state store).
        async fn copy(&self, key: &str) -> FlakyStore {
            let c = FlakyStore::default();
            if let Some((_, r)) = self.inner.get(key).await.unwrap() {
                c.inner.create(key, &r).await.unwrap();
            }
            c
        }
    }

    fn expected(deployment: &str) -> Expected {
        Expected {
            owner: OwnerIdentity {
                deployment: deployment.into(),
                pipeline: "p".into(),
                sink: "es".into(),
            },
            relation: RelationMarker {
                connector: "postgresql".into(),
                source: "pg".into(),
                database: "db".into(),
                namespace: "public".into(),
                table: "orders".into(),
            },
            destination: DestinationMarker::elasticsearch("c1", "orders"),
            encoding: EncodingMarker {
                layout: "elasticsearch.document.v1".into(),
                type_mapping: "v2".into(),
                mode: None,
                extensions_column: None,
            },
        }
    }

    const D1: &str = "11111111111111111111111111111111";
    const D2: &str = "22222222222222222222222222222222";
    const KEY: &str = "elasticsearch/c1/orders";

    fn class(r: Result<Claimed, ClaimFailure>) -> OwnershipClass {
        match r {
            Err(ClaimFailure::Refused { class, .. }) => class,
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    fn unavailable(r: &Result<Claimed, ClaimFailure>) -> bool {
        matches!(r, Err(ClaimFailure::Unavailable(_)))
    }

    #[tokio::test]
    async fn creation_then_restart_rotates_and_fences_the_old_token() {
        let dest = Fake::default();
        let store = FlakyStore::default();
        let f1 = claim(&dest, &store, KEY, &expected(D1))
            .await
            .unwrap()
            .fence;
        assert!(dest.write(&f1));
        let (_, rec) = store.get(KEY).await.unwrap().unwrap();
        assert_eq!(rec.fence.as_deref(), Some(f1.as_str()));
        assert_eq!(rec.pending, None);
        // The marker holds the immutable instance, never a fence.
        let marker = dest.0.lock().unwrap().dest.clone().unwrap().0;
        assert_eq!(marker["instance"], rec.destination_instance);
        assert!(!marker.to_string().contains(&f1));

        let f2 = claim(&dest, &store, KEY, &expected(D1))
            .await
            .unwrap()
            .fence;
        assert_ne!(f1, f2);
        assert!(dest.write(&f2) && !dest.write(&f1), "old fence rejected");
    }

    #[tokio::test]
    async fn concurrent_creators_of_separate_deployments_one_wins() {
        let dest = Fake::default();
        let (a, b) = (FlakyStore::default(), FlakyStore::default());
        let (e1, e2) = (expected(D1), expected(D2));
        let (ra, rb) = tokio::join!(
            claim(&dest, &a, KEY, &e1),
            claim(&dest, &b, KEY, &e2)
        );
        let wins = [&ra, &rb].iter().filter(|r| r.is_ok()).count();
        assert_eq!(wins, 1, "{ra:?} {rb:?}");
        let loser = if ra.is_ok() { rb } else { ra };
        assert_eq!(class(loser), OwnershipClass::OwnedByOther);
    }

    #[tokio::test]
    async fn a_copied_store_fences_the_original_at_its_next_write() {
        let dest = Fake::default();
        let original = FlakyStore::default();
        let fa = claim(&dest, &original, KEY, &expected(D1))
            .await
            .unwrap()
            .fence;
        let clone = original.copy(KEY).await;
        // The clone claims (the same identity, the same record).
        let fb = claim(&dest, &clone, KEY, &expected(D1))
            .await
            .unwrap()
            .fence;
        // The destination now rejects the original's very next write ...
        assert!(!dest.write(&fa) && dest.write(&fb));
        // ... and its next claim.
        assert_eq!(
            class(claim(&dest, &original, KEY, &expected(D1)).await),
            OwnershipClass::Fenced
        );
    }

    #[tokio::test]
    async fn concurrently_claiming_copies_never_both_hold_the_fence() {
        let dest = Fake::default();
        let original = FlakyStore::default();
        claim(&dest, &original, KEY, &expected(D1)).await.unwrap();
        let clone = original.copy(KEY).await;
        let e = expected(D1);
        let (ra, rb) = tokio::join!(
            claim(&dest, &original, KEY, &e),
            claim(&dest, &clone, KEY, &e)
        );
        let held: Vec<_> = [ra, rb]
            .into_iter()
            .filter_map(Result::ok)
            .filter(|c| dest.write(&c.fence))
            .collect();
        assert_eq!(held.len(), 1, "exactly one writer holds the fence");
    }

    #[tokio::test]
    async fn an_older_store_restore_is_fenced() {
        let dest = Fake::default();
        let store = FlakyStore::default();
        claim(&dest, &store, KEY, &expected(D1)).await.unwrap();
        let backup = store.copy(KEY).await;
        claim(&dest, &store, KEY, &expected(D1)).await.unwrap();
        assert_eq!(
            class(claim(&dest, &backup, KEY, &expected(D1)).await),
            OwnershipClass::Fenced
        );
    }

    #[tokio::test]
    async fn markers_and_records_that_do_not_match_are_refused() {
        let dest = Fake::default();
        let store = FlakyStore::default();
        claim(&dest, &store, KEY, &expected(D1)).await.unwrap();

        let mut other = expected(D1);
        other.relation.namespace = "sales".into();
        assert_eq!(
            class(claim(&dest, &store, KEY, &other).await),
            OwnershipClass::BoundToAnotherRelation
        );
        let mut other = expected(D1);
        other.encoding.type_mapping = "v1".into();
        assert_eq!(
            class(claim(&dest, &store, KEY, &other).await),
            OwnershipClass::EncodingMismatch
        );
        let mut other = expected(D1);
        other.destination = DestinationMarker::elasticsearch("c2", "orders");
        assert_eq!(
            class(claim(&dest, &store, KEY, &other).await),
            OwnershipClass::DestinationIdentityMismatch
        );
        // Our marker, but this store never recorded the destination.
        assert_eq!(
            class(
                claim(&dest, &FlakyStore::default(), KEY, &expected(D1)).await
            ),
            OwnershipClass::UnrecordedDestination
        );

        let set = |m: serde_json::Value| {
            dest.0.lock().unwrap().dest.as_mut().unwrap().0 = m
        };
        let good = dest.0.lock().unwrap().dest.clone().unwrap().0;
        set(serde_json::Value::Null);
        assert_eq!(
            class(claim(&dest, &store, KEY, &expected(D1)).await),
            OwnershipClass::MarkerMissing
        );
        let mut extra = good.clone();
        extra["fence"] = serde_json::json!("x");
        set(extra);
        assert_eq!(
            class(claim(&dest, &store, KEY, &expected(D1)).await),
            OwnershipClass::MarkerMalformed
        );
        let mut v2 = good.clone();
        v2["version"] = serde_json::json!(2);
        set(v2);
        assert_eq!(
            class(claim(&dest, &store, KEY, &expected(D1)).await),
            OwnershipClass::MarkerUnsupportedVersion
        );
        let mut bad = good.clone();
        bad["instance"] = serde_json::json!("not-a-token");
        set(bad);
        assert_eq!(
            class(claim(&dest, &store, KEY, &expected(D1)).await),
            OwnershipClass::MarkerMalformed
        );
        set(good);
        dest.0.lock().unwrap().broken_fence = true;
        assert_eq!(
            class(claim(&dest, &store, KEY, &expected(D1)).await),
            OwnershipClass::FenceMalformed
        );
    }

    #[tokio::test]
    async fn a_created_destination_that_vanished_is_never_recreated() {
        let dest = Fake::default();
        let store = FlakyStore::default();
        claim(&dest, &store, KEY, &expected(D1)).await.unwrap();
        dest.0.lock().unwrap().dest = None;
        assert_eq!(
            class(claim(&dest, &store, KEY, &expected(D1)).await),
            OwnershipClass::DestinationMissing
        );
        assert!(dest.0.lock().unwrap().dest.is_none());
    }

    #[tokio::test]
    async fn a_recreated_destination_does_not_transfer_ownership() {
        let dest = Fake::default();
        let store = FlakyStore::default();
        claim(&dest, &store, KEY, &expected(D1)).await.unwrap();
        // Deleted and recreated by another deployment.
        dest.0.lock().unwrap().dest = None;
        claim(&dest, &FlakyStore::default(), KEY, &expected(D2))
            .await
            .unwrap();
        assert_eq!(
            class(claim(&dest, &store, KEY, &expected(D1)).await),
            OwnershipClass::OwnedByOther
        );
    }

    #[tokio::test]
    async fn a_destination_of_the_same_owner_with_another_instance_is_refused()
    {
        let dest = Fake::default();
        let store = FlakyStore::default();
        claim(&dest, &store, KEY, &expected(D1)).await.unwrap();
        // Deleted, then recreated by a copy of the store (the same owner)
        // that recorded nothing for it.
        let copy = FlakyStore::default();
        dest.0.lock().unwrap().dest = None;
        claim(&dest, &copy, KEY, &expected(D1)).await.unwrap();
        assert_eq!(
            class(claim(&dest, &store, KEY, &expected(D1)).await),
            OwnershipClass::DestinationIdentityMismatch
        );
    }

    #[tokio::test]
    async fn an_unknown_swap_that_applied_is_committed_by_the_next_claim() {
        let dest = Fake::default();
        let store = FlakyStore::default();
        claim(&dest, &store, KEY, &expected(D1)).await.unwrap();
        dest.0.lock().unwrap().inject_swap =
            Some((RemoteOutcome::Unknown, true));
        let r = claim(&dest, &store, KEY, &expected(D1)).await;
        assert!(unavailable(&r), "unverified: {r:?}");
        let (_, rec) = store.get(KEY).await.unwrap().unwrap();
        let pending = rec.pending.clone().expect("pending kept");
        assert!(dest.write(&pending), "the swap did apply");
        let f = claim(&dest, &store, KEY, &expected(D1))
            .await
            .unwrap()
            .fence;
        assert!(dest.write(&f));
        let (_, rec) = store.get(KEY).await.unwrap().unwrap();
        assert_eq!(
            (rec.fence.as_deref(), rec.pending),
            (Some(f.as_str()), None)
        );
    }

    #[tokio::test]
    async fn a_stale_pending_with_the_old_fence_is_replaced() {
        let dest = Fake::default();
        let store = FlakyStore::default();
        let f1 = claim(&dest, &store, KEY, &expected(D1))
            .await
            .unwrap()
            .fence;
        dest.0.lock().unwrap().inject_swap =
            Some((RemoteOutcome::Unknown, false));
        assert!(unavailable(&claim(&dest, &store, KEY, &expected(D1)).await));
        assert!(dest.write(&f1), "the swap never happened");
        let f = claim(&dest, &store, KEY, &expected(D1))
            .await
            .unwrap()
            .fence;
        assert!(dest.write(&f) && !dest.write(&f1));
    }

    #[tokio::test]
    async fn a_failed_final_commit_fails_the_claim_and_recovers() {
        let dest = Fake::default();
        let store = FlakyStore::default();
        claim(&dest, &store, KEY, &expected(D1)).await.unwrap();
        // The pending write succeeds, the commit after the swap fails.
        *store.fail_cas.lock().unwrap() = 0;
        let fail_second = async {
            // Fail only the commit: let the pending CAS through first.
            struct OnceAfter<'a>(&'a FlakyStore, Mutex<usize>);
            #[async_trait]
            impl OwnershipStore for OnceAfter<'_> {
                async fn get(
                    &self,
                    k: &str,
                ) -> anyhow::Result<Option<(u64, OwnershipRecord)>>
                {
                    self.0.get(k).await
                }
                async fn create(
                    &self,
                    k: &str,
                    r: &OwnershipRecord,
                ) -> anyhow::Result<Option<u64>> {
                    self.0.create(k, r).await
                }
                async fn cas(
                    &self,
                    k: &str,
                    v: u64,
                    r: &OwnershipRecord,
                ) -> anyhow::Result<Option<u64>> {
                    let n = {
                        let mut n = self.1.lock().unwrap();
                        *n += 1;
                        *n
                    };
                    if n == 2 {
                        anyhow::bail!("store unavailable at commit");
                    }
                    self.0.cas(k, v, r).await
                }
                async fn remove(&self, k: &str) -> anyhow::Result<bool> {
                    self.0.remove(k).await
                }
            }
            claim(&dest, &OnceAfter(&store, Mutex::new(0)), KEY, &expected(D1))
                .await
        };
        let r = fail_second.await;
        assert!(unavailable(&r), "{r:?}");
        let (_, rec) = store.get(KEY).await.unwrap().unwrap();
        assert!(rec.pending.is_some(), "pending kept");
        let f = claim(&dest, &store, KEY, &expected(D1))
            .await
            .unwrap()
            .fence;
        assert!(dest.write(&f));
    }

    #[tokio::test]
    async fn an_unknown_creation_is_resolved_by_the_next_claim() {
        for applied in [true, false] {
            let dest = Fake::default();
            let store = FlakyStore::default();
            dest.0.lock().unwrap().inject_create =
                Some((RemoteOutcome::Unknown, applied));
            assert!(unavailable(
                &claim(&dest, &store, KEY, &expected(D1)).await
            ));
            let f = claim(&dest, &store, KEY, &expected(D1))
                .await
                .unwrap()
                .fence;
            assert!(dest.write(&f), "applied={applied}");
            let (_, rec) = store.get(KEY).await.unwrap().unwrap();
            assert_eq!(rec.pending, None);
        }
    }

    #[tokio::test]
    async fn a_different_server_is_refused() {
        let dest = Fake::default();
        dest.0.lock().unwrap().server = Some("s1".into());
        let store = FlakyStore::default();
        claim(&dest, &store, KEY, &expected(D1)).await.unwrap();
        dest.0.lock().unwrap().dest.as_mut().unwrap().2 = Some("s2".into());
        assert_eq!(
            class(claim(&dest, &store, KEY, &expected(D1)).await),
            OwnershipClass::ServerChanged
        );
    }

    #[test]
    fn a_refusal_names_no_marker_and_digests_the_other_owner() {
        let e = expected(D1);
        let err = ClaimFailure::Refused {
            class: OwnershipClass::OwnedByOther,
            bound: None,
            owner: Some(expected(D2).owner),
        }
        .into_sink_error("es", &e.destination, &e.relation.identity());
        let draft = err.draft().expect("an incident");
        assert_eq!(draft.reason_code, ReasonCode::SinkDestinationConflict);
        let ev = serde_json::to_string(&draft.evidence).unwrap();
        assert!(!ev.contains(D2), "the owner only as a digest: {ev}");
        assert!(ev.contains("owned_by_other"));
    }
}
