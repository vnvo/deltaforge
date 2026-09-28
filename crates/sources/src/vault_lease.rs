//! Vault secret-lease lifecycle machinery (feature `vault`), Phase 2.
//!
//! Models + an authoritative durable store + a pure lifecycle state machine. No
//! Vault I/O and no source-DSN change or reconnect here - those are the combined
//! integration slice and Phase 3. Kept small and reusable: the Vault lease client
//! (next slice) works in terms of a `lease_id: &str`, so it does not depend on
//! these types.
//!
//! The persistence boundary is authoritative in the same way `ReplayJobStore` is:
//! every load validates the record against its key and version and fails closed on
//! anything corrupt or foreign; every write loads the current record first and
//! rejects any illegal transition or mutation of immutable identity. Finalizing a
//! lease is a CAS to the terminal `Revoked` tombstone, which is **retained** - there
//! is no physical delete. Retention keeps the tombstone that enforces "generation
//! ids are never reused" and avoids the read-then-`slot_delete` race that could erase
//! a concurrently recreated key. Physical compaction is deferred to a future
//! conditional-delete primitive.
//!
//! Two versions are kept distinct: [`LeaseRecord::record_version`] is the serialized
//! payload format; the backend **slot version** returned by the store is the
//! authoritative concurrency token used for CAS. There is no CAS counter in the
//! payload.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use secrets::CredentialSet;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};
use storage::ArcStorageBackend;
use zeroize::Zeroizing;

use crate::rotation::ApplyOutcome;
use crate::rotation_manager::is_transient;

/// Durable namespace for lease-ownership records.
pub(crate) const VAULT_LEASES_NS: &str = "vault_leases";
/// Serialized lease-record format version (distinct from the authoritative backend
/// slot version used for CAS).
pub(crate) const LEASE_RECORD_VERSION: u32 = 1;

// -----------------------------------------------------------------------------
// Errors
// -----------------------------------------------------------------------------

/// Typed errors for the lease persistence boundary and state machine.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub(crate) enum LeaseError {
    #[error("unsupported lease record version {got} (expected {expected})")]
    UnsupportedRecordVersion { got: u32, expected: u32 },
    #[error("lease record identity does not match the store scope/incarnation")]
    IdentityMismatch,
    #[error("lease record does not match its slot key")]
    ForeignKey,
    #[error("lease record immutable field changed: {field}")]
    ImmutableFieldChanged { field: &'static str },
    #[error("illegal lease transition from {from} to {to}")]
    IllegalTransition {
        from: &'static str,
        to: &'static str,
    },
    #[error("lease record is terminal (revoked) and immutable")]
    TerminalImmutable,
    #[error("lease record has an empty identity field: {field}")]
    EmptyIdentity { field: &'static str },
    #[error("lease record has malformed or out-of-order timing")]
    MalformedTiming,
    #[error("lease expiry cannot move backward")]
    ExpiryRegression,
    #[error("lease auth-session epoch cannot move backward")]
    SessionEpochRegression,
    #[error("illegal lease event {event} in state {state}")]
    IllegalEvent {
        state: &'static str,
        event: &'static str,
    },
    #[error("lease store version conflict (expected {expected})")]
    VersionConflict { expected: u64 },
    #[error("lease record already exists for this key")]
    AlreadyExists,
}

// -----------------------------------------------------------------------------
// Small helpers
// -----------------------------------------------------------------------------

/// Convert a `SystemTime` to unix-ms, fallible with checked conversions: a pre-epoch
/// time or one whose millisecond count overflows `u64` is rejected rather than
/// silently mapped to zero or truncated.
fn to_ms(t: SystemTime) -> Option<u64> {
    let d = t.duration_since(UNIX_EPOCH).ok()?;
    u64::try_from(d.as_millis()).ok()
}

/// Reconstruct a `SystemTime` from unix-ms, fallible so corrupt persisted values
/// cannot panic.
fn from_ms(ms: u64) -> Option<SystemTime> {
    UNIX_EPOCH.checked_add(Duration::from_millis(ms))
}

fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// A stable, delimiter-safe, collision-resistant 16-byte hex hash over
/// length-prefixed parts. Used for every key segment so arbitrary or
/// attacker-influenced field contents cannot inject the `/` separator, exceed a
/// bound, or collide across identities.
fn hash16(parts: &[&str]) -> String {
    let mut h = Sha256::new();
    for p in parts {
        h.update((p.len() as u64).to_le_bytes());
        h.update(p.as_bytes());
    }
    to_hex(&h.finalize()[..16])
}

fn scope_seg(
    tenant: &str,
    pipeline: &str,
    source: &str,
    purpose: &str,
) -> String {
    hash16(&[tenant, pipeline, source, purpose])
}

// -----------------------------------------------------------------------------
// Models
// -----------------------------------------------------------------------------

/// A Vault lease id: a durable identifier that must never appear in logs, status, or
/// metrics. Redacted in `Debug`, zeroized on drop; persisted so it round-trips
/// through the store, but exposed only to renew/revoke the lease.
#[derive(Clone)]
pub(crate) struct LeaseId(Zeroizing<String>);

impl LeaseId {
    pub(crate) fn new(id: impl Into<String>) -> Self {
        Self(Zeroizing::new(id.into()))
    }
    pub(crate) fn expose(&self) -> &str {
        self.0.as_str()
    }
}

impl std::fmt::Debug for LeaseId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("LeaseId(REDACTED)")
    }
}
impl PartialEq for LeaseId {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_str() == other.0.as_str()
    }
}
impl Eq for LeaseId {}
impl Serialize for LeaseId {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.0.as_str())
    }
}
impl<'de> Deserialize<'de> for LeaseId {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Ok(LeaseId::new(String::deserialize(d)?))
    }
}

/// A leased secret's runtime identity and timing. Not a credential value.
#[derive(Clone)]
pub(crate) struct LeaseHandle {
    lease_id: LeaseId,
    pub(crate) mount: String,
    pub(crate) role: String,
    pub(crate) lease_duration: Duration,
    pub(crate) renewable: bool,
    pub(crate) issued_at: SystemTime,
    pub(crate) expires_at: SystemTime,
}

impl LeaseHandle {
    /// Construct a handle, computing `expires_at = issued_at + lease_duration` with
    /// checked time arithmetic. Fails closed if the sum overflows `SystemTime`.
    pub(crate) fn new(
        lease_id: LeaseId,
        mount: impl Into<String>,
        role: impl Into<String>,
        lease_duration: Duration,
        renewable: bool,
        issued_at: SystemTime,
    ) -> Result<Self, LeaseError> {
        let expires_at = issued_at
            .checked_add(lease_duration)
            .ok_or(LeaseError::MalformedTiming)?;
        Ok(Self {
            lease_id,
            mount: mount.into(),
            role: role.into(),
            lease_duration,
            renewable,
            issued_at,
            expires_at,
        })
    }

    pub(crate) fn lease_id(&self) -> &LeaseId {
        &self.lease_id
    }

    /// The effective (safety) expiry: normal operation must stop at
    /// `expires_at - safety_margin`, not at true expiry, leaving time to reissue.
    pub(crate) fn effective_expiry(
        &self,
        safety_margin: Duration,
    ) -> SystemTime {
        self.expires_at
            .checked_sub(safety_margin)
            .unwrap_or(self.issued_at)
    }
}

impl std::fmt::Debug for LeaseHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaseHandle")
            .field("mount", &self.mount)
            .field("role", &self.role)
            .field("renewable", &self.renewable)
            .field("expires_at", &self.expires_at)
            .finish_non_exhaustive()
    }
}

/// A lease and the atomic credential set it governs.
pub(crate) struct LeasedCredentialSet {
    pub(crate) credentials: CredentialSet,
    pub(crate) lease: LeaseHandle,
}

// -----------------------------------------------------------------------------
// Durable record + lifecycle state
// -----------------------------------------------------------------------------

/// The lifecycle state of a durable lease record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LeaseState {
    /// Written before the credential material is exposed; not yet in use.
    Pending,
    /// In use (or under renewal).
    Active,
    /// A revoke has been decided but not yet confirmed at Vault.
    RevokePending,
    /// Not actionable with the current token; left to TTL / operator cleanup.
    Orphaned,
    /// Terminal tombstone: Vault confirmed the lease is revoked or absent. The only
    /// state from which physical deletion is allowed; no transition out of it is
    /// legal, and generation ids are never reused, so the delete is safe/idempotent.
    Revoked,
}

impl LeaseState {
    pub(crate) fn name(self) -> &'static str {
        match self {
            LeaseState::Pending => "pending",
            LeaseState::Active => "active",
            LeaseState::RevokePending => "revoke_pending",
            LeaseState::Orphaned => "orphaned",
            LeaseState::Revoked => "revoked",
        }
    }

    pub(crate) fn is_terminal(self) -> bool {
        matches!(self, LeaseState::Revoked)
    }

    /// Whether a direct transition to `next` is allowed.
    pub(crate) fn can_transition_to(self, next: LeaseState) -> bool {
        use LeaseState::*;
        matches!(
            (self, next),
            (Pending, Active)
                | (Pending, RevokePending)
                | (Pending, Orphaned)
                | (Active, RevokePending)
                | (Active, Orphaned)
                | (RevokePending, Revoked)
                | (RevokePending, Orphaned)
                | (Orphaned, Revoked)
        )
    }
}

/// The durable lease record persisted as a slot value. Identity is layered:
/// - the **scope** (`tenant`/`pipeline`/`source`/`credential_purpose`) is stable
///   across restarts and forms the outer key segment for orphan cleanup;
/// - `source_incarnation` forms the middle key segment - it is part of durable
///   identity, so a normal restart (same incarnation) recovers its own leases while
///   a delete/recreate (new incarnation) does not silently act on the prior run's
///   leases through the normal path;
/// - `lease_generation_id` (hashed) is the opaque, never-reused inner segment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LeaseRecord {
    pub(crate) record_version: u32,
    pub(crate) state: LeaseState,

    pub(crate) tenant: String,
    pub(crate) pipeline: String,
    pub(crate) source: String,
    pub(crate) source_incarnation: String,
    pub(crate) credential_purpose: String,
    pub(crate) lease_generation_id: String,

    pub(crate) lease_id: LeaseId,
    pub(crate) mount: String,
    pub(crate) role: String,
    pub(crate) lease_duration_ms: u64,
    pub(crate) renewable: bool,
    pub(crate) issued_at_ms: u64,
    pub(crate) expires_at_ms: u64,

    /// Diagnostic only: a local auth-session counter, NOT proof of lease ownership
    /// (Vault responses are authoritative for that). Monotonic.
    pub(crate) auth_session_epoch: u64,
}

impl LeaseRecord {
    /// Build a `Pending` record for a freshly issued lease. Fails closed on any
    /// timing value that cannot be represented as checked unix-ms (pre-epoch,
    /// overflow, or a duration exceeding `u64` milliseconds).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn pending(
        tenant: impl Into<String>,
        pipeline: impl Into<String>,
        source: impl Into<String>,
        source_incarnation: impl Into<String>,
        credential_purpose: impl Into<String>,
        lease_generation_id: impl Into<String>,
        lease: &LeaseHandle,
        auth_session_epoch: u64,
    ) -> Result<Self, LeaseError> {
        let lease_duration_ms = u64::try_from(lease.lease_duration.as_millis())
            .map_err(|_| LeaseError::MalformedTiming)?;
        let issued_at_ms =
            to_ms(lease.issued_at).ok_or(LeaseError::MalformedTiming)?;
        let expires_at_ms =
            to_ms(lease.expires_at).ok_or(LeaseError::MalformedTiming)?;
        Ok(Self {
            record_version: LEASE_RECORD_VERSION,
            state: LeaseState::Pending,
            tenant: tenant.into(),
            pipeline: pipeline.into(),
            source: source.into(),
            source_incarnation: source_incarnation.into(),
            credential_purpose: credential_purpose.into(),
            lease_generation_id: lease_generation_id.into(),
            lease_id: lease.lease_id.clone(),
            mount: lease.mount.clone(),
            role: lease.role.clone(),
            lease_duration_ms,
            renewable: lease.renewable,
            issued_at_ms,
            expires_at_ms,
            auth_session_epoch,
        })
    }

    /// Self-consistency: supported version, non-empty identities, reconstructible and
    /// ordered timing. A record failing this must never become an actionable lease.
    pub(crate) fn validate(&self) -> Result<(), LeaseError> {
        if self.record_version != LEASE_RECORD_VERSION {
            return Err(LeaseError::UnsupportedRecordVersion {
                got: self.record_version,
                expected: LEASE_RECORD_VERSION,
            });
        }
        for (field, val) in [
            ("tenant", self.tenant.as_str()),
            ("pipeline", self.pipeline.as_str()),
            ("source", self.source.as_str()),
            ("source_incarnation", self.source_incarnation.as_str()),
            ("credential_purpose", self.credential_purpose.as_str()),
            ("lease_generation_id", self.lease_generation_id.as_str()),
            ("lease_id", self.lease_id.expose()),
            ("mount", self.mount.as_str()),
            ("role", self.role.as_str()),
        ] {
            if val.is_empty() {
                return Err(LeaseError::EmptyIdentity { field });
            }
        }
        let issued =
            from_ms(self.issued_at_ms).ok_or(LeaseError::MalformedTiming)?;
        let expires =
            from_ms(self.expires_at_ms).ok_or(LeaseError::MalformedTiming)?;
        if expires < issued {
            return Err(LeaseError::MalformedTiming);
        }
        Ok(())
    }

    /// The runtime handle for this record's lease. Fails closed on malformed timing.
    pub(crate) fn lease(&self) -> Result<LeaseHandle, LeaseError> {
        let issued =
            from_ms(self.issued_at_ms).ok_or(LeaseError::MalformedTiming)?;
        let expires =
            from_ms(self.expires_at_ms).ok_or(LeaseError::MalformedTiming)?;
        Ok(LeaseHandle {
            lease_id: self.lease_id.clone(),
            mount: self.mount.clone(),
            role: self.role.clone(),
            lease_duration: Duration::from_millis(self.lease_duration_ms),
            renewable: self.renewable,
            issued_at: issued,
            expires_at: expires,
        })
    }

    /// A copy transitioned to `next` (state only), or `None` if the transition is
    /// illegal.
    pub(crate) fn transitioned(&self, next: LeaseState) -> Option<LeaseRecord> {
        if !self.state.can_transition_to(next) {
            return None;
        }
        let mut r = self.clone();
        r.state = next;
        Some(r)
    }

    fn scope_segment(&self) -> String {
        scope_seg(
            &self.tenant,
            &self.pipeline,
            &self.source,
            &self.credential_purpose,
        )
    }

    /// The durable key: `<scope-hash>/<incarnation-hash>/<generation-hash>`.
    pub(crate) fn key(&self) -> String {
        format!(
            "{}/{}/{}",
            self.scope_segment(),
            hash16(&[&self.source_incarnation]),
            hash16(&[&self.lease_generation_id]),
        )
    }
}

// -----------------------------------------------------------------------------
// Authoritative durable store
// -----------------------------------------------------------------------------

/// One page of recovered lease records plus an opaque cursor for the next page.
pub(crate) struct LeasePage {
    pub(crate) records: Vec<(String, u64, LeaseRecord)>, // (key, slot version, record)
    pub(crate) next_cursor: Option<String>,
}

/// Durable lease-ownership store over the `StorageBackend` slot primitives, bound to
/// one scope (`tenant`/`pipeline`/`source`/`credential_purpose`) and the current
/// `source_incarnation`. Every load and write is validated against this identity and
/// the current durable record before it is trusted or committed.
pub(crate) struct LeaseStore {
    backend: ArcStorageBackend,
    tenant: String,
    pipeline: String,
    source: String,
    credential_purpose: String,
    incarnation: String,
}

impl LeaseStore {
    pub(crate) fn new(
        backend: ArcStorageBackend,
        tenant: &str,
        pipeline: &str,
        source: &str,
        credential_purpose: &str,
        incarnation: &str,
    ) -> Self {
        Self {
            backend,
            tenant: tenant.to_string(),
            pipeline: pipeline.to_string(),
            source: source.to_string(),
            credential_purpose: credential_purpose.to_string(),
            incarnation: incarnation.to_string(),
        }
    }

    fn scope_segment(&self) -> String {
        scope_seg(
            &self.tenant,
            &self.pipeline,
            &self.source,
            &self.credential_purpose,
        )
    }

    /// Prefix for the current incarnation's leases (normal recovery).
    fn current_incarnation_prefix(&self) -> String {
        format!(
            "{}/{}/",
            self.scope_segment(),
            hash16(&[self.incarnation.as_str()])
        )
    }

    /// Prefix for the whole scope across all incarnations (orphan cleanup after a
    /// delete/recreate replaced the incarnation).
    fn scope_prefix(&self) -> String {
        format!("{}/", self.scope_segment())
    }

    fn in_scope(&self, r: &LeaseRecord) -> bool {
        r.tenant == self.tenant
            && r.pipeline == self.pipeline
            && r.source == self.source
            && r.credential_purpose == self.credential_purpose
    }

    /// Validate a loaded record against a key: self-consistency, key match, and scope
    /// membership. Records outside this store's scope, or inconsistent with their
    /// key, fail closed.
    fn check_loaded(
        &self,
        record: &LeaseRecord,
        key: &str,
    ) -> Result<(), LeaseError> {
        record.validate()?;
        if record.key() != key {
            return Err(LeaseError::ForeignKey);
        }
        if !self.in_scope(record) {
            return Err(LeaseError::IdentityMismatch);
        }
        Ok(())
    }

    /// Persist a new record in `Pending` **before** the credential is exposed. The
    /// record must belong to this store's scope and current incarnation. Fails if a
    /// record already exists for the key (generation ids are never reused).
    pub(crate) async fn create_pending(
        &self,
        record: &LeaseRecord,
    ) -> Result<u64> {
        record.validate()?;
        if record.state != LeaseState::Pending {
            return Err(LeaseError::IllegalTransition {
                from: "<new>",
                to: record.state.name(),
            }
            .into());
        }
        if !self.in_scope(record)
            || record.source_incarnation != self.incarnation
        {
            return Err(LeaseError::IdentityMismatch.into());
        }
        let key = record.key();
        let bytes = serde_json::to_vec(record)?;
        match self
            .backend
            .slot_create(VAULT_LEASES_NS, &key, &bytes)
            .await?
        {
            Some(version) => Ok(version),
            None => Err(LeaseError::AlreadyExists.into()),
        }
    }

    /// Load a record by key with its authoritative slot version, or `None`. Fails
    /// closed on corruption, a foreign key, or a foreign scope.
    pub(crate) async fn get(
        &self,
        key: &str,
    ) -> Result<Option<(u64, LeaseRecord)>> {
        match self.backend.slot_get(VAULT_LEASES_NS, key).await? {
            Some((version, bytes)) => {
                let record: LeaseRecord = serde_json::from_slice(&bytes)?;
                self.check_loaded(&record, key)?;
                Ok(Some((version, record)))
            }
            None => Ok(None),
        }
    }

    /// Compare-and-swap `record` in if the slot is still at `expected_version`, after
    /// authoritatively validating the write against the current durable record. The
    /// proposed record must be self-consistent, keep its key/identity, be a legal
    /// transition, and change only approved timing/renewability fields monotonically.
    pub(crate) async fn cas(
        &self,
        key: &str,
        expected_version: u64,
        record: &LeaseRecord,
    ) -> Result<bool> {
        self.check_loaded(record, key)?;
        let (current_version, current) =
            self.get(key).await?.ok_or(LeaseError::VersionConflict {
                expected: expected_version,
            })?;
        if current_version != expected_version {
            return Err(LeaseError::VersionConflict {
                expected: expected_version,
            }
            .into());
        }
        validate_durable_update(&current, record)?;
        let bytes = serde_json::to_vec(record)?;
        self.backend
            .slot_cas(VAULT_LEASES_NS, key, expected_version, &bytes)
            .await
    }

    // NOTE: there is deliberately no physical-delete method. Finalizing a lease is a
    // CAS to the terminal `Revoked` tombstone, which is RETAINED. Physical removal is
    // race-unsafe with the current unconditional `slot_delete` (another actor could
    // recreate the same key between the read and the delete, erasing a new live
    // record) and would also drop the tombstone that enforces "generation ids are
    // never reused". Compaction is deferred until a conditional-delete primitive or a
    // proven retention protocol exists.

    /// List one bounded page of the **current incarnation's** records (normal
    /// recovery). Each record is validated and confirmed to belong to this
    /// incarnation; a stray record under the prefix fails closed.
    pub(crate) async fn list_current_incarnation(
        &self,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<LeasePage> {
        let prefix = self.current_incarnation_prefix();
        self.list(&prefix, cursor, limit, true).await
    }

    /// List one bounded page across the whole **scope** (all incarnations), for
    /// orphan cleanup after a delete/recreate replaced the incarnation.
    pub(crate) async fn list_scope(
        &self,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<LeasePage> {
        let prefix = self.scope_prefix();
        self.list(&prefix, cursor, limit, false).await
    }

    async fn list(
        &self,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
        require_current_incarnation: bool,
    ) -> Result<LeasePage> {
        let page = self
            .backend
            .slot_list(VAULT_LEASES_NS, Some(prefix), cursor, limit)
            .await?;
        let mut records = Vec::with_capacity(page.records.len());
        for r in page.records {
            let record: LeaseRecord = serde_json::from_slice(&r.value)?;
            self.check_loaded(&record, &r.key)?;
            if require_current_incarnation
                && record.source_incarnation != self.incarnation
            {
                return Err(LeaseError::IdentityMismatch.into());
            }
            records.push((r.key, r.version, record));
        }
        Ok(LeasePage {
            records,
            next_cursor: page.next_cursor,
        })
    }
}

/// Validate a proposed durable update against the current record: immutable identity
/// unchanged, a legal (or same-state) transition, and only approved
/// timing/renewability fields changed, monotonically. The persistence boundary's
/// guard against illegal writes.
fn validate_durable_update(
    current: &LeaseRecord,
    proposed: &LeaseRecord,
) -> Result<(), LeaseError> {
    if current.state.is_terminal() {
        return Err(LeaseError::TerminalImmutable);
    }
    macro_rules! immutable {
        ($field:ident) => {
            if current.$field != proposed.$field {
                return Err(LeaseError::ImmutableFieldChanged {
                    field: stringify!($field),
                });
            }
        };
    }
    immutable!(record_version);
    immutable!(tenant);
    immutable!(pipeline);
    immutable!(source);
    immutable!(source_incarnation);
    immutable!(credential_purpose);
    immutable!(lease_generation_id);
    immutable!(lease_id);
    immutable!(mount);
    immutable!(role);
    immutable!(issued_at_ms);

    if proposed.state != current.state
        && !current.state.can_transition_to(proposed.state)
    {
        return Err(LeaseError::IllegalTransition {
            from: current.state.name(),
            to: proposed.state.name(),
        });
    }
    // Only timing/renewability may change, and expiry/epoch never regress.
    if proposed.expires_at_ms < current.expires_at_ms {
        return Err(LeaseError::ExpiryRegression);
    }
    if proposed.auth_session_epoch < current.auth_session_epoch {
        return Err(LeaseError::SessionEpochRegression);
    }
    Ok(())
}

// -----------------------------------------------------------------------------
// Pure lifecycle state machine (event -> action)
// -----------------------------------------------------------------------------

/// A lifecycle event for one lease handle. "Apply outcome" is expressed per handle
/// as [`Superseded`](LeaseEvent::Superseded) (the old lease when the swap applied),
/// [`Promoted`](LeaseEvent::Promoted) (the new lease when it is adopted), and
/// [`Rejected`](LeaseEvent::Rejected) (the new lease when it is not) - the caller
/// routes these via [`lease_revoke_decision`], which is where the active/pending
/// two-handle invariant is enforced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LeaseEvent {
    /// A renewal extended the same credential material.
    RenewSucceeded {
        new_expires_at_ms: u64,
        renewable: bool,
    },
    /// The safety deadline (expiry - margin) was reached: reissue proactively. This
    /// is distinct from actual expiry, which is a terminal fail-closed condition, not
    /// an event handled here.
    SafetyDeadlineReached,
    /// A replacement (pending) lease was issued alongside this active one.
    ReplacementIssued,
    /// This (old, active) lease was superseded by an applied replacement.
    Superseded,
    /// This (new, pending) lease was adopted at the boundary.
    Promoted,
    /// This (new, pending) lease was not adopted and must be revoked.
    Rejected,
    /// Vault confirmed revocation.
    RevokeConfirmed,
    /// Vault refused (403): not actionable by the current token.
    RevokeUnauthorized,
    /// Vault reports the lease absent (already gone / expired).
    LeaseAbsent,
}

impl LeaseEvent {
    fn name(self) -> &'static str {
        match self {
            LeaseEvent::RenewSucceeded { .. } => "renew_succeeded",
            LeaseEvent::SafetyDeadlineReached => "safety_deadline_reached",
            LeaseEvent::ReplacementIssued => "replacement_issued",
            LeaseEvent::Superseded => "superseded",
            LeaseEvent::Promoted => "promoted",
            LeaseEvent::Rejected => "rejected",
            LeaseEvent::RevokeConfirmed => "revoke_confirmed",
            LeaseEvent::RevokeUnauthorized => "revoke_unauthorized",
            LeaseEvent::LeaseAbsent => "lease_absent",
        }
    }
}

/// What the caller should do after applying an event (in addition to persisting the
/// returned next state via a validated CAS).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LeaseAction {
    /// No side effect beyond any state change.
    Retain,
    /// Update the record's timing/renewability to these values (via CAS).
    UpdateTiming { expires_at_ms: u64, renewable: bool },
    /// Issue a replacement lease and drive it through the boundary swap.
    Reissue,
    /// Call Vault to revoke this lease, then feed back the confirmed/unauthorized/
    /// absent event.
    Revoke,
    /// Mark orphaned; rely on TTL / operator cleanup.
    MarkOrphaned,
    /// Terminal: the record is now the retained `Revoked` tombstone. It is NOT
    /// physically deleted - the tombstone stays so generation ids can never be
    /// reused; physical compaction is deferred to a future conditional-delete
    /// primitive.
    FinalizeRevoked,
}

/// The pure lifecycle step: given a lease's current state and an event, return the
/// next state and the action to take. Illegal `(state, event)` pairs fail closed.
pub(crate) fn on_lease_event(
    state: LeaseState,
    event: LeaseEvent,
) -> Result<(LeaseState, LeaseAction), LeaseError> {
    use LeaseEvent::*;
    use LeaseState::*;
    let out = match (state, event) {
        // Renewal keeps the same state, updates timing.
        (
            Active | Pending,
            RenewSucceeded {
                new_expires_at_ms,
                renewable,
            },
        ) => (
            state,
            LeaseAction::UpdateTiming {
                expires_at_ms: new_expires_at_ms,
                renewable,
            },
        ),
        // Proactive reissue at the safety deadline (NOT expiry): keep serving.
        (Active | Pending, SafetyDeadlineReached) => {
            (state, LeaseAction::Reissue)
        }
        // A replacement now coexists; the active lease keeps serving until apply.
        (Active, ReplacementIssued) => (Active, LeaseAction::Retain),
        // Apply outcomes (two-handle):
        (Active, Superseded) => (RevokePending, LeaseAction::Revoke),
        (Pending, Promoted) => (Active, LeaseAction::Retain),
        (Pending, Rejected) => (RevokePending, LeaseAction::Revoke),
        // Revoke resolution.
        (RevokePending | Orphaned, RevokeConfirmed) => {
            (Revoked, LeaseAction::FinalizeRevoked)
        }
        (RevokePending | Orphaned, LeaseAbsent) => {
            (Revoked, LeaseAction::FinalizeRevoked)
        }
        (RevokePending, RevokeUnauthorized) => {
            (Orphaned, LeaseAction::MarkOrphaned)
        }
        // Terminal is immutable.
        (Revoked, _) => return Err(LeaseError::TerminalImmutable),
        _ => {
            return Err(LeaseError::IllegalEvent {
                state: state.name(),
                event: event.name(),
            });
        }
    };
    Ok(out)
}

// -----------------------------------------------------------------------------
// Pure applied-vs-rejected revocation decision
// -----------------------------------------------------------------------------

/// What to do with the old (active) and new (pending replacement) leases once a
/// boundary-swap outcome is known.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LeaseRevokeDecision {
    /// The swap applied: promote the new lease, revoke the old.
    PromoteNewRevokeOld,
    /// The new lease was never adopted: revoke it; keep the old active.
    RevokeNew,
    /// A transient failure will be retried: keep and renew both leases.
    RetainBoth,
}

/// Decide the revocation action from the apply outcome and whether the coordinator
/// will still retry this generation. Never revokes a lease whose outcome is unknown.
pub(crate) fn lease_revoke_decision(
    outcome: &ApplyOutcome,
    retry_pending: bool,
) -> LeaseRevokeDecision {
    match outcome {
        ApplyOutcome::Applied => LeaseRevokeDecision::PromoteNewRevokeOld,
        ApplyOutcome::KeptOld { reason }
            if is_transient(*reason) && retry_pending =>
        {
            LeaseRevokeDecision::RetainBoth
        }
        _ => LeaseRevokeDecision::RevokeNew,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rotation::RotationReject;
    use std::sync::Arc;
    use storage::MemoryStorageBackend;

    const EPOCH_MS: u64 = 1_000_000_000;

    fn handle(id: &str, secs: u64, renewable: bool) -> LeaseHandle {
        LeaseHandle::new(
            LeaseId::new(id),
            "database",
            "orders-ro",
            Duration::from_secs(secs),
            renewable,
            from_ms(EPOCH_MS).unwrap(),
        )
        .unwrap()
    }

    fn rec(gen_id: &str) -> LeaseRecord {
        LeaseRecord::pending(
            "acme",
            "pipe",
            "pg1",
            "inc-1",
            "source-db",
            gen_id,
            &handle("vault-lease-id", 3600, true),
            1,
        )
        .unwrap()
    }

    fn store(backend: &ArcStorageBackend) -> LeaseStore {
        LeaseStore::new(
            backend.clone(),
            "acme",
            "pipe",
            "pg1",
            "source-db",
            "inc-1",
        )
    }

    // --- models ---

    #[test]
    fn lease_id_redacted_but_round_trips() {
        let id = LeaseId::new("secret-lease");
        assert_eq!(format!("{id:?}"), "LeaseId(REDACTED)");
        let j = serde_json::to_string(&id).unwrap();
        assert!(j.contains("secret-lease"));
        assert_eq!(serde_json::from_str::<LeaseId>(&j).unwrap(), id);
    }

    #[test]
    fn effective_expiry_precedes_true_expiry() {
        let h = handle("x", 100, true);
        assert_eq!(
            h.effective_expiry(Duration::from_secs(10)),
            h.expires_at - Duration::from_secs(10)
        );
    }

    #[test]
    fn record_debug_and_handle_debug_redact_lease_id() {
        assert!(!format!("{:?}", rec("g")).contains("vault-lease-id"));
        assert!(!format!("{:?}", handle("leak", 1, true)).contains("leak"));
    }

    // --- key hierarchy ---

    #[test]
    fn key_is_three_segment_scope_incarnation_generation() {
        let r = rec("gen-1");
        let key = r.key();
        let segs: Vec<&str> = key.split('/').collect();
        assert_eq!(segs.len(), 3, "scope/incarnation/generation");
        // Same scope, different incarnation -> same scope seg, different incar seg.
        let mut r2 = rec("gen-1");
        r2.source_incarnation = "inc-2".to_string();
        let k2: Vec<String> = r2.key().split('/').map(String::from).collect();
        assert_eq!(segs[0], k2[0], "scope segment stable across incarnations");
        assert_ne!(segs[1], k2[1], "incarnation is part of the key");
    }

    // --- self validation ---

    #[test]
    fn validate_rejects_bad_records() {
        let mut r = rec("g");
        r.record_version = 999;
        assert_eq!(
            r.validate(),
            Err(LeaseError::UnsupportedRecordVersion {
                got: 999,
                expected: LEASE_RECORD_VERSION
            })
        );
        let mut r = rec("g");
        r.tenant = String::new();
        assert_eq!(
            r.validate(),
            Err(LeaseError::EmptyIdentity { field: "tenant" })
        );
        let mut r = rec("g");
        r.expires_at_ms = r.issued_at_ms - 1;
        assert_eq!(r.validate(), Err(LeaseError::MalformedTiming));
    }

    // --- state transitions ---

    #[test]
    fn transitions_and_terminal() {
        use LeaseState::*;
        assert!(Pending.can_transition_to(Active));
        assert!(RevokePending.can_transition_to(Revoked));
        assert!(Orphaned.can_transition_to(Revoked));
        assert!(!Active.can_transition_to(Pending));
        assert!(!Revoked.can_transition_to(Active));
        assert!(Revoked.is_terminal());
    }

    // --- revoke decision table ---

    #[test]
    fn revoke_decision_table() {
        use LeaseRevokeDecision::*;
        assert_eq!(
            lease_revoke_decision(&ApplyOutcome::Applied, false),
            PromoteNewRevokeOld
        );
        assert_eq!(
            lease_revoke_decision(
                &ApplyOutcome::KeptOld {
                    reason: RotationReject::PreflightFailed
                },
                true
            ),
            RetainBoth
        );
        assert_eq!(
            lease_revoke_decision(
                &ApplyOutcome::KeptOld {
                    reason: RotationReject::PreflightFailed
                },
                false
            ),
            RevokeNew
        );
        assert_eq!(
            lease_revoke_decision(
                &ApplyOutcome::KeptOld {
                    reason: RotationReject::IdentityMismatch
                },
                true
            ),
            RevokeNew
        );
        assert_eq!(
            lease_revoke_decision(&ApplyOutcome::CloseUncertain, true),
            RevokeNew
        );
        assert_eq!(
            lease_revoke_decision(&ApplyOutcome::FailedClosed, true),
            RevokeNew
        );
    }

    // --- event/action state machine ---

    #[test]
    fn event_machine_core_paths() {
        use LeaseAction::*;
        use LeaseEvent::*;
        use LeaseState::*;
        assert_eq!(
            on_lease_event(
                Active,
                RenewSucceeded {
                    new_expires_at_ms: 5,
                    renewable: true
                }
            )
            .unwrap(),
            (
                Active,
                UpdateTiming {
                    expires_at_ms: 5,
                    renewable: true
                }
            )
        );
        // Safety deadline is distinct from expiry: reissue, stay Active.
        assert_eq!(
            on_lease_event(Active, SafetyDeadlineReached).unwrap(),
            (Active, Reissue)
        );
        assert_eq!(
            on_lease_event(Active, ReplacementIssued).unwrap(),
            (Active, Retain)
        );
        assert_eq!(
            on_lease_event(Active, Superseded).unwrap(),
            (RevokePending, Revoke)
        );
        assert_eq!(
            on_lease_event(Pending, Promoted).unwrap(),
            (Active, Retain)
        );
        assert_eq!(
            on_lease_event(Pending, Rejected).unwrap(),
            (RevokePending, Revoke)
        );
        assert_eq!(
            on_lease_event(RevokePending, RevokeConfirmed).unwrap(),
            (Revoked, FinalizeRevoked)
        );
        assert_eq!(
            on_lease_event(RevokePending, LeaseAbsent).unwrap(),
            (Revoked, FinalizeRevoked)
        );
        assert_eq!(
            on_lease_event(RevokePending, RevokeUnauthorized).unwrap(),
            (Orphaned, MarkOrphaned)
        );
        assert_eq!(
            on_lease_event(Orphaned, LeaseAbsent).unwrap(),
            (Revoked, FinalizeRevoked)
        );
        // Terminal + illegal.
        assert_eq!(
            on_lease_event(Revoked, RevokeConfirmed),
            Err(LeaseError::TerminalImmutable)
        );
        assert!(matches!(
            on_lease_event(Active, Promoted),
            Err(LeaseError::IllegalEvent { .. })
        ));
    }

    // --- authoritative store: happy path ---

    #[tokio::test]
    async fn store_full_lifecycle() {
        let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
        let s = store(&backend);
        let r = rec("gen-1");
        let key = r.key();

        s.create_pending(&r).await.unwrap();
        assert!(s.create_pending(&r).await.is_err(), "no id reuse");

        // Pending -> Active.
        let (v, cur) = s.get(&key).await.unwrap().unwrap();
        let active = cur.transitioned(LeaseState::Active).unwrap();
        assert!(s.cas(&key, v, &active).await.unwrap());

        // Renew: extend expiry (monotonic) in Active.
        let (v, cur) = s.get(&key).await.unwrap().unwrap();
        let mut renewed = cur.clone();
        renewed.expires_at_ms += 1000;
        renewed.auth_session_epoch += 1;
        assert!(s.cas(&key, v, &renewed).await.unwrap());

        // Active -> RevokePending -> Revoked (a retained tombstone; no physical
        // delete).
        let (v, cur) = s.get(&key).await.unwrap().unwrap();
        let rp = cur.transitioned(LeaseState::RevokePending).unwrap();
        assert!(s.cas(&key, v, &rp).await.unwrap());
        let (v, cur) = s.get(&key).await.unwrap().unwrap();
        let revoked = cur.transitioned(LeaseState::Revoked).unwrap();
        assert!(s.cas(&key, v, &revoked).await.unwrap());

        // The tombstone is retained and blocks any reuse of the generation key.
        let (_, tomb) = s.get(&key).await.unwrap().unwrap();
        assert_eq!(tomb.state, LeaseState::Revoked);
        assert_eq!(
            s.create_pending(&rec("gen-1"))
                .await
                .unwrap_err()
                .downcast_ref::<LeaseError>(),
            Some(&LeaseError::AlreadyExists),
            "a retained tombstone prevents generation-id reuse"
        );
    }

    // --- store-bypass / fail-closed tests (replay-job standard) ---

    #[tokio::test]
    async fn cas_rejects_illegal_transition() {
        let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
        let s = store(&backend);
        let r = rec("g");
        let key = r.key();
        s.create_pending(&r).await.unwrap();
        let (v, cur) = s.get(&key).await.unwrap().unwrap();
        // Force an illegal Pending -> Revoked (skips RevokePending).
        let mut bad = cur.clone();
        bad.state = LeaseState::Revoked;
        let err = s.cas(&key, v, &bad).await.unwrap_err();
        assert_eq!(
            err.downcast_ref::<LeaseError>(),
            Some(&LeaseError::IllegalTransition {
                from: "pending",
                to: "revoked"
            })
        );
    }

    #[tokio::test]
    async fn cas_rejects_immutable_mutation() {
        let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
        let s = store(&backend);
        let r = rec("g");
        let key = r.key();
        s.create_pending(&r).await.unwrap();
        let (v, cur) = s.get(&key).await.unwrap().unwrap();
        let mut bad = cur.clone();
        bad.mount = "different".to_string();
        let err = s.cas(&key, v, &bad).await.unwrap_err();
        assert_eq!(
            err.downcast_ref::<LeaseError>(),
            Some(&LeaseError::ImmutableFieldChanged { field: "mount" })
        );
    }

    #[tokio::test]
    async fn cas_rejects_expiry_regression() {
        let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
        let s = store(&backend);
        let r = rec("g");
        let key = r.key();
        s.create_pending(&r).await.unwrap();
        let (v, cur) = s.get(&key).await.unwrap().unwrap();
        let mut bad = cur.clone();
        bad.expires_at_ms -= 1; // regress
        let err = s.cas(&key, v, &bad).await.unwrap_err();
        assert_eq!(
            err.downcast_ref::<LeaseError>(),
            Some(&LeaseError::ExpiryRegression)
        );
    }

    #[tokio::test]
    async fn cas_rejects_terminal_mutation() {
        let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
        let s = store(&backend);
        // Drive to Revoked.
        let r = rec("g");
        let key = r.key();
        s.create_pending(&r).await.unwrap();
        for next in [
            LeaseState::Active,
            LeaseState::RevokePending,
            LeaseState::Revoked,
        ] {
            let (cv, cur) = s.get(&key).await.unwrap().unwrap();
            let n = cur.transitioned(next).unwrap();
            assert!(s.cas(&key, cv, &n).await.unwrap());
        }
        // Any further update is rejected as terminal.
        let (cv, cur) = s.get(&key).await.unwrap().unwrap();
        let mut attempt = cur.clone();
        attempt.auth_session_epoch += 1;
        let err = s.cas(&key, cv, &attempt).await.unwrap_err();
        assert_eq!(
            err.downcast_ref::<LeaseError>(),
            Some(&LeaseError::TerminalImmutable)
        );
    }

    /// A finalized generation key is never reusable, and re-creating it after a
    /// same-key delete/recreate race cannot occur because the tombstone is retained
    /// (there is no physical-delete path that could remove it).
    #[tokio::test]
    async fn revoked_tombstone_is_retained_and_blocks_recreation() {
        let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
        let s = store(&backend);
        let r = rec("g");
        let key = r.key();
        s.create_pending(&r).await.unwrap();
        // Drive Pending -> RevokePending -> Revoked.
        for next in [LeaseState::RevokePending, LeaseState::Revoked] {
            let (v, cur) = s.get(&key).await.unwrap().unwrap();
            let n = cur.transitioned(next).unwrap();
            assert!(s.cas(&key, v, &n).await.unwrap());
        }
        // Tombstone retained; the same generation key cannot be recreated.
        let (_, tomb) = s.get(&key).await.unwrap().unwrap();
        assert_eq!(tomb.state, LeaseState::Revoked);
        assert_eq!(
            s.create_pending(&rec("g"))
                .await
                .unwrap_err()
                .downcast_ref::<LeaseError>(),
            Some(&LeaseError::AlreadyExists)
        );
    }

    // --- timing overflow / truncation regressions ---

    #[test]
    fn lease_handle_new_rejects_overflowing_expiry() {
        // issued_at + Duration::MAX overflows SystemTime -> fail closed, no panic.
        let err = LeaseHandle::new(
            LeaseId::new("id"),
            "database",
            "orders-ro",
            Duration::MAX,
            true,
            from_ms(EPOCH_MS).unwrap(),
        )
        .unwrap_err();
        assert_eq!(err, LeaseError::MalformedTiming);
    }

    #[test]
    fn pending_rejects_oversized_duration() {
        // A handle whose duration exceeds u64 milliseconds cannot be persisted.
        let h = LeaseHandle {
            lease_id: LeaseId::new("id"),
            mount: "database".to_string(),
            role: "orders-ro".to_string(),
            lease_duration: Duration::MAX,
            renewable: true,
            issued_at: from_ms(EPOCH_MS).unwrap(),
            expires_at: from_ms(EPOCH_MS).unwrap(),
        };
        let err = LeaseRecord::pending(
            "acme",
            "pipe",
            "pg1",
            "inc-1",
            "source-db",
            "g",
            &h,
            0,
        )
        .unwrap_err();
        assert_eq!(err, LeaseError::MalformedTiming);
    }

    #[test]
    fn to_ms_rejects_pre_epoch() {
        let before = UNIX_EPOCH.checked_sub(Duration::from_secs(1)).unwrap();
        assert_eq!(to_ms(before), None, "pre-epoch is rejected, not zeroed");
        assert_eq!(to_ms(from_ms(EPOCH_MS).unwrap()), Some(EPOCH_MS));
    }

    #[tokio::test]
    async fn get_fails_closed_on_foreign_key_version_scope_and_timing() {
        let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
        let s = store(&backend);

        // Foreign key: write a record under the WRONG key.
        let r = rec("g");
        let good_key = r.key();
        backend
            .slot_create(
                VAULT_LEASES_NS,
                "wrong/key/here",
                &serde_json::to_vec(&r).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            s.get("wrong/key/here")
                .await
                .unwrap_err()
                .downcast_ref::<LeaseError>(),
            Some(&LeaseError::ForeignKey)
        );

        // Unsupported version under its own (recomputed) key.
        let mut bad_ver = rec("v");
        bad_ver.record_version = 999;
        backend
            .slot_create(
                VAULT_LEASES_NS,
                &bad_ver.key(),
                &serde_json::to_vec(&bad_ver).unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(
            s.get(&bad_ver.key())
                .await
                .unwrap_err()
                .downcast_ref::<LeaseError>(),
            Some(&LeaseError::UnsupportedRecordVersion { .. })
        ));

        // Foreign scope: a record from another tenant, under its own key.
        let mut foreign = rec("f");
        foreign.tenant = "other-tenant".to_string();
        backend
            .slot_create(
                VAULT_LEASES_NS,
                &foreign.key(),
                &serde_json::to_vec(&foreign).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            s.get(&foreign.key())
                .await
                .unwrap_err()
                .downcast_ref::<LeaseError>(),
            Some(&LeaseError::IdentityMismatch)
        );

        // Corrupt timing.
        let mut bad_time = rec("t");
        bad_time.expires_at_ms = bad_time.issued_at_ms - 5;
        backend
            .slot_create(
                VAULT_LEASES_NS,
                &bad_time.key(),
                &serde_json::to_vec(&bad_time).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            s.get(&bad_time.key())
                .await
                .unwrap_err()
                .downcast_ref::<LeaseError>(),
            Some(&LeaseError::MalformedTiming)
        );

        let _ = good_key;
    }

    #[tokio::test]
    async fn create_rejects_foreign_incarnation() {
        let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
        let s = store(&backend); // incarnation inc-1
        let mut r = rec("g");
        r.source_incarnation = "inc-2".to_string();
        assert_eq!(
            s.create_pending(&r)
                .await
                .unwrap_err()
                .downcast_ref::<LeaseError>(),
            Some(&LeaseError::IdentityMismatch)
        );
    }

    #[tokio::test]
    async fn list_current_incarnation_vs_scope() {
        let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
        let s = store(&backend); // inc-1
        // Current incarnation leases.
        for i in 0..3 {
            s.create_pending(&rec(&format!("cur-{i}"))).await.unwrap();
        }
        // A prior incarnation's lease (same scope), written directly.
        let mut prior = rec("old-1");
        prior.source_incarnation = "inc-0".to_string();
        backend
            .slot_create(
                VAULT_LEASES_NS,
                &prior.key(),
                &serde_json::to_vec(&prior).unwrap(),
            )
            .await
            .unwrap();

        // Normal recovery: current incarnation only.
        let page = s.list_current_incarnation(None, 50).await.unwrap();
        let mut gens: Vec<_> = page
            .records
            .iter()
            .map(|(_, _, r)| r.lease_generation_id.clone())
            .collect();
        gens.sort();
        assert_eq!(gens, ["cur-0", "cur-1", "cur-2"]);

        // Orphan cleanup: whole scope, including the prior incarnation.
        let page = s.list_scope(None, 50).await.unwrap();
        assert_eq!(page.records.len(), 4);
        assert!(
            page.records
                .iter()
                .any(|(_, _, r)| r.source_incarnation == "inc-0")
        );
    }
}
