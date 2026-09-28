//! Vault lease manager + scheduler (feature `vault`), Phase 2 combined integration.
//!
//! Issues dynamic database credentials through the secrets Vault lease client,
//! persists lease ownership through the authoritative [`LeaseStore`], and drives
//! renew-before-expiry and reissue-at-safety-deadline through the pure
//! [`on_lease_event`] state machine.
//!
//! Phase 2 boundary: this exercises the full Vault lease lifecycle
//! (issue/renew/reissue/revoke) but MUST NOT change any source DSN or trigger a
//! reconnect - that is Phase 3. A reissue here mints and adopts a fresh lease and
//! revokes the superseded one; no source connection is touched. The state machine's
//! apply-outcome events ([`LeaseEvent::Superseded`]/[`Promoted`](LeaseEvent::Promoted))
//! are used to sequence the swap, but the "apply" is a no-op adoption rather than a
//! DSN rotation.
//!
//! Time is passed in (`now: SystemTime`) rather than read from a clock, matching the
//! rest of the source crate's decision functions, so scheduling is deterministic
//! under test. The async run loop supplies `SystemTime::now()`.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::Result;
use async_trait::async_trait;
use metrics::{counter, gauge};
use secrets::{LeaseInfo, LeasedRead, ProviderFailureKind, SecretError};

use crate::rotation::ApplyOutcome;
use crate::vault_lease::{
    LeaseAction, LeaseEvent, LeaseHandle, LeaseId, LeaseRecord,
    LeaseRevokeDecision, LeaseState, LeaseStore, LeasedCredentialSet,
    lease_revoke_decision, on_lease_event,
};

// -----------------------------------------------------------------------------
// Scheduling policy (pure)
// -----------------------------------------------------------------------------

/// When to renew or reissue a lease, expressed relative to the granted lease
/// duration and remaining lifetime. Deliberately small: no jitter or interval knobs
/// yet, since a lease's schedule is driven by the TTL Vault actually granted.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LeaseScheduleConfig {
    /// Stop serving on the current lease at least this long before true expiry; a
    /// reissue must complete before the safety deadline (`expires_at - safety_margin`).
    pub safety_margin: Duration,
    /// Renew (or, if non-renewable, proactively reissue) once this fraction of the
    /// granted lease duration has been consumed, i.e. when the remaining lifetime has
    /// dropped to `(1 - fraction)` of the grant. In `(0.0, 1.0]`.
    pub renew_fraction: f32,
    /// Floor for any scheduled wait, so a near-deadline lease cannot spin.
    pub min_poll: Duration,
    /// Ceiling for any scheduled wait, so a long lease still re-checks periodically
    /// (config/timing changes are noticed).
    pub max_poll: Duration,
}

impl Default for LeaseScheduleConfig {
    fn default() -> Self {
        Self {
            safety_margin: Duration::from_secs(30),
            renew_fraction: 0.66,
            min_poll: Duration::from_secs(1),
            max_poll: Duration::from_secs(300),
        }
    }
}

/// What the run loop should do for the active lease at `now`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LeaseTick {
    /// Sleep this long, then re-evaluate. Never shorter than `min_poll`.
    Wait(Duration),
    /// Renew the active (renewable) lease now.
    Renew,
    /// Reissue now: inside the safety window (`expires_at - safety_margin <= now <
    /// expires_at`), or a non-renewable lease has consumed its renew fraction. There is
    /// still time to obtain a fresh lease before true expiry.
    Reissue,
    /// Terminal fail-closed: the lease has reached (or passed) true expiry. The active
    /// credential is no longer valid and service MUST stop; a reissue that did not
    /// complete before this point does not permit continued service.
    Expired,
}

/// The pure scheduling step. Given `now` and the active lease handle, decide whether
/// to renew, reissue, wait, or fail closed. Works from the *remaining* lifetime against
/// the granted duration, so the decision is stable across recomputes: waking early only
/// shortens the returned wait, it never triggers a premature action. Past true expiry
/// the result is the terminal [`LeaseTick::Expired`] - never `Reissue` - so a failed or
/// too-late reissue cannot be mistaken for permission to keep serving.
pub(crate) fn lease_tick(
    now: SystemTime,
    handle: &LeaseHandle,
    cfg: &LeaseScheduleConfig,
) -> LeaseTick {
    let remaining = handle
        .expires_at
        .duration_since(now)
        .unwrap_or(Duration::ZERO);

    // At or past true expiry: fail closed. The credential is unusable; stop serving.
    if remaining.is_zero() {
        return LeaseTick::Expired;
    }

    // Inside the safety window (before true expiry): still time to get a fresh lease.
    if remaining <= cfg.safety_margin {
        return LeaseTick::Reissue;
    }

    // The remaining-lifetime threshold at which we act. Never below the safety margin,
    // so we always leave time to reissue before true expiry.
    let act_at_remaining = handle
        .lease_duration
        .mul_f32(1.0 - cfg.renew_fraction)
        .max(cfg.safety_margin);

    if remaining <= act_at_remaining {
        return if handle.renewable {
            LeaseTick::Renew
        } else {
            LeaseTick::Reissue
        };
    }

    // Wait until the action point, clamped so we neither spin nor sleep past a
    // re-check window.
    let wait = (remaining - act_at_remaining)
        .max(cfg.min_poll)
        .min(cfg.max_poll);
    LeaseTick::Wait(wait)
}

// -----------------------------------------------------------------------------
// Vault lease provider (I/O boundary, mockable)
// -----------------------------------------------------------------------------

/// The Vault lease operations the manager needs, abstracted so unit tests run without
/// a live Vault. The real implementation wraps a `secrets::VaultResolver`.
#[async_trait]
pub(crate) trait LeaseProvider: Send + Sync {
    /// Issue a fresh dynamic credential, creating a Vault lease.
    async fn issue(
        &self,
        mount: &str,
        role: &str,
    ) -> Result<LeasedRead, SecretError>;
    /// Renew an existing lease, optionally requesting an increment (seconds).
    async fn renew(
        &self,
        lease_id: &str,
        increment_secs: Option<u64>,
    ) -> Result<LeaseInfo, SecretError>;
    /// Revoke a lease.
    async fn revoke(&self, lease_id: &str) -> Result<(), SecretError>;
}

/// Adapter: the production [`LeaseProvider`] backed by a `secrets::VaultResolver`.
pub(crate) struct VaultLeaseProvider {
    resolver: Arc<secrets::VaultResolver>,
}

impl VaultLeaseProvider {
    pub(crate) fn new(resolver: Arc<secrets::VaultResolver>) -> Self {
        Self { resolver }
    }
}

#[async_trait]
impl LeaseProvider for VaultLeaseProvider {
    async fn issue(
        &self,
        mount: &str,
        role: &str,
    ) -> Result<LeasedRead, SecretError> {
        self.resolver.read_db_credentials(mount, role).await
    }
    async fn renew(
        &self,
        lease_id: &str,
        increment_secs: Option<u64>,
    ) -> Result<LeaseInfo, SecretError> {
        self.resolver.renew_lease(lease_id, increment_secs).await
    }
    async fn revoke(&self, lease_id: &str) -> Result<(), SecretError> {
        self.resolver.revoke_lease(lease_id).await
    }
}

/// How a Vault provider failure should be handled by the lifecycle. Keeps the state
/// machine free of transport concerns while still distinguishing the three outcomes
/// the exit gate calls out: retryable outage, unauthorized (not owned by this token),
/// and already-absent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OpFault {
    /// A retryable outage (unavailable/timeout/tls). Keep the record; retry later.
    Transient,
    /// The token is not authorized for this lease (403). It cannot be actioned here.
    Unauthorized,
    /// Vault reports the lease absent (already revoked/expired). Idempotent success.
    Absent,
    /// A non-retryable, non-classifiable failure. Fail closed.
    Fatal,
}

/// Classify a `SecretError` from a lease operation into an [`OpFault`].
pub(crate) fn classify_fault(err: &SecretError) -> OpFault {
    match err {
        SecretError::NotFound(_) => OpFault::Absent,
        SecretError::Provider { kind, .. } => match kind {
            ProviderFailureKind::Unavailable
            | ProviderFailureKind::Timeout
            | ProviderFailureKind::Tls => OpFault::Transient,
            ProviderFailureKind::Forbidden
            | ProviderFailureKind::Unauthorized => OpFault::Unauthorized,
            ProviderFailureKind::InvalidResponse
            | ProviderFailureKind::Other => OpFault::Fatal,
        },
        _ => OpFault::Fatal,
    }
}

// -----------------------------------------------------------------------------
// Lease identity
// -----------------------------------------------------------------------------

/// The durable identity a manager issues leases under. The scope
/// (`tenant`/`pipeline`/`source`/`credential_purpose`) is stable across restarts;
/// `incarnation` distinguishes a delete/recreate of the source from a plain restart.
#[derive(Debug, Clone)]
pub(crate) struct LeaseIdentity {
    pub tenant: String,
    pub pipeline: String,
    pub source: String,
    pub credential_purpose: String,
    pub incarnation: String,
}

impl LeaseIdentity {
    fn store(&self, backend: storage::ArcStorageBackend) -> LeaseStore {
        LeaseStore::new(
            backend,
            &self.tenant,
            &self.pipeline,
            &self.source,
            &self.credential_purpose,
            &self.incarnation,
        )
    }
}

/// Mint an opaque, never-reused lease generation id. Includes the incarnation (stable
/// across restart) plus a monotonic counter and 128 bits of randomness, so ids never
/// collide with a prior run's under the same incarnation (which would hit the retained
/// tombstone).
fn mint_generation_id(incarnation: &str, counter: u64) -> String {
    let r: u128 = rand::random();
    format!("{incarnation}:{counter}:{r:032x}")
}

// -----------------------------------------------------------------------------
// Lease manager
// -----------------------------------------------------------------------------

/// Owns the active leased credential set for one source scope, persists lease
/// ownership, and performs issue/renew/reissue/revoke through the provider and store.
///
/// Reissue follows the approved **two-handle protocol**: a replacement lease is minted
/// as a second `pending` handle while the current lease keeps serving; promotion of the
/// new lease and revocation of the old happen only once an apply outcome is known
/// (`resolve_reissue`). Phase 2 has no source DSN change or DB reconnect, so the apply
/// outcome is supplied explicitly; Phase 3 will supply the real reconnect result.
pub(crate) struct LeaseManager {
    provider: Arc<dyn LeaseProvider>,
    store: LeaseStore,
    id: LeaseIdentity,
    mount: String,
    role: String,
    cfg: LeaseScheduleConfig,
    /// Monotonic auth-session epoch (diagnostic; recorded on each issue).
    epoch: u64,
    /// Monotonic generation counter feeding [`mint_generation_id`].
    gen_counter: u64,
    /// The active (serving) lease's durable key and current slot version, once issued.
    active: Option<ActiveLease>,
    /// A pending replacement minted by `reissue`, awaiting an apply outcome. Coexists
    /// with `active` until `resolve_reissue` promotes or revokes it.
    pending: Option<ActiveLease>,
    /// A superseded/rejected lease staged for revocation. It stays installed here until
    /// its revoke reaches a terminal (`Revoked`) or `Orphaned` outcome, so a store/Vault
    /// failure mid-revoke never loses the handle or its retry capability
    /// ([`retry_revoking`](Self::retry_revoking) completes it).
    revoking: Option<ActiveLease>,
}

/// The manager's handle on a durable lease: its key and last-known slot version.
struct ActiveLease {
    key: String,
    version: u64,
}

impl LeaseManager {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        provider: Arc<dyn LeaseProvider>,
        backend: storage::ArcStorageBackend,
        id: LeaseIdentity,
        mount: impl Into<String>,
        role: impl Into<String>,
        cfg: LeaseScheduleConfig,
    ) -> Self {
        let store = id.store(backend);
        Self {
            provider,
            store,
            id,
            mount: mount.into(),
            role: role.into(),
            cfg,
            epoch: 0,
            gen_counter: 0,
            active: None,
            pending: None,
            revoking: None,
        }
    }

    /// Issue a fresh lease and adopt it as the active one, persisting a `Pending`
    /// record **before** exposing the credential material and transitioning it to
    /// `Active` only after it is durably owned.
    pub(crate) async fn issue(
        &mut self,
        now: SystemTime,
    ) -> Result<LeasedCredentialSet> {
        let (slot, leased) = self.mint_pending(now).await?;
        // Pending -> Active: the credential is durably owned, adopt it.
        let version =
            self.transition_key(&slot.key, LeaseState::Active).await?;
        self.active = Some(ActiveLease {
            key: slot.key,
            version,
        });
        Ok(leased)
    }

    /// Issue a lease from Vault and persist it as a `Pending` durable record (written
    /// **before** the credential material is exposed). Returns the durable handle and
    /// the leased credentials; does not mutate `active`/`pending` (the caller places
    /// the handle).
    async fn mint_pending(
        &mut self,
        now: SystemTime,
    ) -> Result<(ActiveLease, LeasedCredentialSet)> {
        let read = self.provider.issue(&self.mount, &self.role).await?;
        self.epoch += 1;
        self.gen_counter += 1;
        let generation_id =
            mint_generation_id(&self.id.incarnation, self.gen_counter);
        let handle = LeaseHandle::new(
            LeaseId::new(read.lease_id),
            self.mount.clone(),
            self.role.clone(),
            read.lease_duration,
            read.renewable,
            now,
        )?;
        let record = LeaseRecord::pending(
            self.id.tenant.clone(),
            self.id.pipeline.clone(),
            self.id.source.clone(),
            self.id.incarnation.clone(),
            self.id.credential_purpose.clone(),
            generation_id,
            &handle,
            self.epoch,
        )?;
        let version = self.store.create_pending(&record).await?;
        self.metric_lifecycle("issued");
        self.metric_expiry(record.expires_at_ms);
        Ok((
            ActiveLease {
                key: record.key(),
                version,
            },
            LeasedCredentialSet {
                credentials: read.credentials,
                lease: handle,
            },
        ))
    }

    /// Load, transition to `next`, and CAS the record at `key`, returning the post-CAS
    /// slot version. Fails closed if the transition is illegal for the current state.
    async fn transition_key(&self, key: &str, next: LeaseState) -> Result<u64> {
        let (version, current) = self
            .store
            .get(key)
            .await?
            .ok_or_else(|| anyhow::anyhow!("lease record missing"))?;
        let updated = current
            .transitioned(next)
            .ok_or_else(|| anyhow::anyhow!("illegal lease transition"))?;
        if !self.store.cas(key, version, &updated).await? {
            anyhow::bail!("lease store version conflict on transition");
        }
        let (new_version, _) = self
            .store
            .get(key)
            .await?
            .ok_or_else(|| anyhow::anyhow!("lease record missing"))?;
        Ok(new_version)
    }

    /// Renew the lease at `key`. On success, advance timing via the state machine's
    /// `UpdateTiming` action (persisted with a validated CAS). Returns the new slot
    /// version and the refreshed record. Loads the current slot version fresh, so it is
    /// robust to a stale tracked version.
    async fn renew_key(
        &self,
        key: &str,
        now: SystemTime,
    ) -> Result<(u64, LeaseRecord)> {
        let (version, current) = self
            .store
            .get(key)
            .await?
            .ok_or_else(|| anyhow::anyhow!("lease record missing"))?;
        let lease_id = current.lease_id.expose().to_string();
        let info = self.provider.renew(&lease_id, None).await?;
        let new_expires = now
            .checked_add(info.lease_duration)
            .ok_or_else(|| anyhow::anyhow!("renewed expiry overflow"))?;
        let new_expires_ms = system_time_to_ms(new_expires)?;
        let (next_state, action) = on_lease_event(
            current.state,
            LeaseEvent::RenewSucceeded {
                new_expires_at_ms: new_expires_ms,
                renewable: info.renewable,
            },
        )?;
        let mut updated = current.clone();
        updated.state = next_state;
        if let LeaseAction::UpdateTiming {
            expires_at_ms,
            renewable,
        } = action
        {
            // Renew extends from now; keep issued_at, refresh duration/expiry with
            // checked conversions/arithmetic (no truncating `as u64`).
            updated.lease_duration_ms = u64::try_from(
                info.lease_duration.as_millis(),
            )
            .map_err(|_| anyhow::anyhow!("renewed lease duration overflow"))?;
            updated.expires_at_ms = expires_at_ms;
            updated.renewable = renewable;
            updated.auth_session_epoch =
                updated.auth_session_epoch.checked_add(1).ok_or_else(|| {
                    anyhow::anyhow!("auth session epoch overflow")
                })?;
        }
        if !self.store.cas(key, version, &updated).await? {
            anyhow::bail!("lease store version conflict on renew");
        }
        let (new_version, refreshed) = self
            .store
            .get(key)
            .await?
            .ok_or_else(|| anyhow::anyhow!("lease record missing"))?;
        Ok((new_version, refreshed))
    }

    /// Renew the active (serving) lease and refresh its tracked version. On any failure
    /// the active handle is left installed unchanged for retry.
    pub(crate) async fn renew(
        &mut self,
        now: SystemTime,
    ) -> Result<LeaseHandle> {
        let key = self
            .active
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("no active lease"))?
            .key
            .clone();
        let (new_version, refreshed) = self.renew_key(&key, now).await?;
        self.active = Some(ActiveLease {
            key,
            version: new_version,
        });
        self.metric_lifecycle("renewed");
        self.metric_expiry(refreshed.expires_at_ms);
        Ok(refreshed.lease()?)
    }

    /// Renew the **pending replacement** lease and refresh its tracked version. Needed
    /// while a reissue is retained across apply retries (`RetainBoth`), so the pending
    /// lease cannot expire during a prolonged reconnect. On failure the pending handle
    /// is left installed unchanged for retry.
    pub(crate) async fn renew_pending(
        &mut self,
        now: SystemTime,
    ) -> Result<LeaseHandle> {
        let key = self
            .pending
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("no pending replacement"))?
            .key
            .clone();
        let (new_version, refreshed) = self.renew_key(&key, now).await?;
        self.pending = Some(ActiveLease {
            key,
            version: new_version,
        });
        self.metric_lifecycle("renewed");
        self.metric_expiry(refreshed.expires_at_ms);
        Ok(refreshed.lease()?)
    }

    /// Begin a reissue under the two-handle protocol: mint a replacement lease as a
    /// **pending** second handle while the current lease keeps serving. The replacement
    /// is NOT promoted and the old lease is NOT revoked here - that waits for an apply
    /// outcome via [`resolve_reissue`]. Requires an active lease; any store failure
    /// while confirming it is propagated (never swallowed, so the old lease can never be
    /// lost while a new one is issued).
    pub(crate) async fn reissue(
        &mut self,
        now: SystemTime,
    ) -> Result<LeasedCredentialSet> {
        // Confirm a current active lease exists and its record loads; fail closed on a
        // corrupt/unavailable store rather than orphaning it.
        let _ = self.load_active().await?;
        if self.pending.is_some() {
            anyhow::bail!("a pending replacement lease already exists");
        }
        // A superseded lease still awaiting revocation owns the single `revoking` slot;
        // starting another reissue could overwrite it and lose retry ownership. Refuse
        // until `retry_revoking` has drained it.
        if self.revoking.is_some() {
            anyhow::bail!(
                "a superseded lease is still awaiting revocation; \
                 retry_revoking must complete first"
            );
        }
        let (slot, leased) = self.mint_pending(now).await?;
        self.pending = Some(slot);
        self.metric_lifecycle("reissued");
        Ok(leased)
    }

    /// Resolve a pending reissue given the apply outcome, per the approved revoke
    /// decision table ([`lease_revoke_decision`]):
    /// - `Applied` -> promote the pending replacement to active, then revoke the old
    ///   lease;
    /// - not adopted (terminal/kept-old/uncertain/failed) -> revoke the pending
    ///   replacement, keep the old lease active;
    /// - transient with a retry pending -> retain both (the caller retries apply and
    ///   renews both meanwhile).
    ///
    /// In Phase 2 the outcome is supplied by the caller (no DB reconnect); Phase 3 will
    /// pass the real reconnect result.
    pub(crate) async fn resolve_reissue(
        &mut self,
        outcome: ApplyOutcome,
        retry_pending: bool,
    ) -> Result<LeaseRevokeDecision> {
        let decision = lease_revoke_decision(&outcome, retry_pending);
        match decision {
            LeaseRevokeDecision::PromoteNewRevokeOld => {
                // (1) Promote the replacement durably (fatal on failure - see
                // `promote_pending`); (2) revoke the old lease, staged for retry on
                // failure.
                self.promote_pending().await?;
                self.drain_revoking().await?;
            }
            LeaseRevokeDecision::RevokeNew => {
                // Stage the pending replacement for revocation (still installed, now in
                // `revoking`), then revoke it; a failure keeps it staged for retry.
                let pending = self.pending.take().ok_or_else(|| {
                    anyhow::anyhow!("no pending replacement to revoke")
                })?;
                self.revoking = Some(pending);
                self.drain_revoking().await?;
                // The active lease is unchanged.
            }
            LeaseRevokeDecision::RetainBoth => {
                // Keep both handles; the caller retries apply and renews both (see
                // `renew`/`renew_pending`).
            }
        }
        Ok(decision)
    }

    /// Promote the pending replacement to the serving (active) lease and stage the old
    /// lease for revocation, WITHOUT revoking it yet. Used on an `Applied` apply outcome,
    /// where the DB stream has already switched to the new credential.
    ///
    /// The promotion CAS is the only fallible step and is done first: on failure `self`
    /// is left unchanged (old still active, pending still installed) and `Err` is
    /// returned. Because the caller reaches here only after `Applied`, a promotion
    /// failure means the DB is already streaming on a credential whose durable lease
    /// ownership was not recorded - the caller MUST treat this as fatal and stop without
    /// advancing the checkpoint. After a successful promotion the moves are infallible.
    pub(crate) async fn promote_pending(&mut self) -> Result<()> {
        if self.active.is_none() {
            anyhow::bail!("no active lease to supersede");
        }
        let pending_key = self
            .pending
            .as_ref()
            .ok_or_else(|| {
                anyhow::anyhow!("no pending replacement to promote")
            })?
            .key
            .clone();
        let version = self
            .transition_key(&pending_key, LeaseState::Active)
            .await?;
        let old = self.active.take().expect("active present (checked above)");
        self.active = Some(ActiveLease {
            key: pending_key,
            version,
        });
        self.pending = None;
        self.revoking = Some(old);
        Ok(())
    }

    /// Drive the lease staged in `revoking` to its terminal (`Revoked`) or `Orphaned`
    /// outcome and clear the slot on success. On any store/Vault failure the slot is
    /// left populated so the revoke can be retried without losing ownership.
    async fn drain_revoking(&mut self) -> Result<RevokeOutcome> {
        let key = self
            .revoking
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("no lease staged for revocation"))?
            .key
            .clone();
        let (version, record) =
            self.store.get(&key).await?.ok_or_else(|| {
                anyhow::anyhow!("staged lease record missing")
            })?;
        let outcome = self.drive_revoke(key, version, record).await?;
        self.revoking = None;
        Ok(outcome)
    }

    /// Retry a revoke left staged by an earlier failure. Returns `None` when nothing is
    /// staged. Keeps the handle staged if the retry fails again.
    pub(crate) async fn retry_revoking(
        &mut self,
    ) -> Result<Option<RevokeOutcome>> {
        if self.revoking.is_none() {
            return Ok(None);
        }
        Ok(Some(self.drain_revoking().await?))
    }

    /// Revoke the active lease and finalize it to the retained `Revoked` tombstone.
    pub(crate) async fn revoke_active(&mut self) -> Result<RevokeOutcome> {
        let (key, version, record) = self.load_active().await?;
        let outcome = self.drive_revoke(key, version, record).await?;
        self.active = None;
        Ok(outcome)
    }

    /// Drive one lease to its terminal (or `Orphaned`) state by calling Vault and
    /// feeding the outcome through the state machine. The pre-revoke transition is
    /// chosen from the record's current state, so this serves normal revoke, reissue
    /// supersede, crash recovery, and orphan reconciliation alike:
    /// - `Active` -> `RevokePending` (superseded),
    /// - `Pending` -> `RevokePending` (a never-adopted lease is rejected),
    /// - `RevokePending`/`Orphaned` -> revoke is retried from where it was left,
    /// - `Revoked` -> already terminal, no-op.
    ///
    /// Idempotent on an absent lease; a 403 leaves the lease `Orphaned`. A transient
    /// outage returns `Err` so the caller retries later without corrupting state.
    async fn drive_revoke(
        &self,
        key: String,
        version: u64,
        record: LeaseRecord,
    ) -> Result<RevokeOutcome> {
        // Move a live/never-adopted lease into RevokePending first; a lease already in
        // RevokePending/Orphaned is retried in place.
        let pre_event = match record.state {
            LeaseState::Revoked => return Ok(RevokeOutcome::Finalized),
            LeaseState::Active => Some(LeaseEvent::Superseded),
            LeaseState::Pending => Some(LeaseEvent::Rejected),
            LeaseState::RevokePending | LeaseState::Orphaned => None,
        };
        if let Some(event) = pre_event {
            let (rp_state, _) = on_lease_event(record.state, event)?;
            let mut rp = record.clone();
            rp.state = rp_state;
            if !self.store.cas(&key, version, &rp).await? {
                anyhow::bail!(
                    "lease store version conflict on revoke transition"
                );
            }
        }

        let lease_id = record.lease_id.expose().to_string();
        let event = match self.provider.revoke(&lease_id).await {
            Ok(()) => LeaseEvent::RevokeConfirmed,
            Err(e) => match classify_fault(&e) {
                OpFault::Absent => LeaseEvent::LeaseAbsent,
                OpFault::Unauthorized => LeaseEvent::RevokeUnauthorized,
                OpFault::Transient | OpFault::Fatal => return Err(e.into()),
            },
        };
        let outcome = self.resolve_revoke(&key, event).await?;
        self.metric_lifecycle(match outcome {
            RevokeOutcome::Finalized => "revoked",
            RevokeOutcome::Orphaned => "orphaned",
        });
        Ok(outcome)
    }

    /// Apply a revoke-resolution event to the record at `key` (reloading for its
    /// current version), persisting the terminal/orphaned state. Idempotent: a record
    /// already terminal, or already `Orphaned` when Vault stays unauthorized, is left
    /// as-is rather than driven through an illegal transition.
    async fn resolve_revoke(
        &self,
        key: &str,
        event: LeaseEvent,
    ) -> Result<RevokeOutcome> {
        let (version, current) =
            self.store.get(key).await?.ok_or_else(|| {
                anyhow::anyhow!("revoking lease record missing")
            })?;
        if current.state.is_terminal() {
            return Ok(RevokeOutcome::Finalized);
        }
        if current.state == LeaseState::Orphaned
            && event == LeaseEvent::RevokeUnauthorized
        {
            return Ok(RevokeOutcome::Orphaned);
        }
        let (next_state, _) = on_lease_event(current.state, event)?;
        let mut updated = current.clone();
        updated.state = next_state;
        if !self.store.cas(key, version, &updated).await? {
            anyhow::bail!("lease store version conflict on revoke resolution");
        }
        Ok(match next_state {
            LeaseState::Orphaned => RevokeOutcome::Orphaned,
            _ => RevokeOutcome::Finalized,
        })
    }

    /// Load the active lease's key, version, and current record.
    async fn load_active(&self) -> Result<(String, u64, LeaseRecord)> {
        let active = self
            .active
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("no active lease"))?;
        let (version, record) =
            self.store.get(&active.key).await?.ok_or_else(|| {
                anyhow::anyhow!("active lease record missing")
            })?;
        Ok((active.key.clone(), version, record))
    }

    /// Recover this run's leases after a restart. Scans the **current incarnation** and
    /// **reclaims** (revokes + finalizes) every non-terminal lease it finds.
    ///
    /// A recovered lease is deliberately **not** adopted as a usable active credential:
    /// the credential material (the password) is never persisted and cannot be
    /// reconstructed from a lease lookup/renew, so a lease known only from durable
    /// metadata cannot be served. The safe action is to revoke it (cleanup) and let the
    /// caller issue a fresh lease. `Pending` leases were never handed out; `Active` were
    /// serving before the crash; `RevokePending` were mid-revoke - all are reclaimed. A
    /// 403 leaves a lease `Orphaned`; `Revoked` tombstones are counted and left.
    /// `active`/`pending` remain unset after recovery.
    pub(crate) async fn recover(&mut self) -> Result<RecoverySummary> {
        let mut summary = RecoverySummary::default();
        let mut to_revoke: Vec<(String, u64, LeaseRecord)> = Vec::new();

        let mut cursor: Option<String> = None;
        loop {
            let page = self
                .store
                .list_current_incarnation(cursor.as_deref(), RECONCILE_PAGE)
                .await?;
            for (key, version, record) in page.records {
                if record.state.is_terminal() {
                    summary.tombstones += 1;
                } else {
                    to_revoke.push((key, version, record));
                }
            }
            match page.next_cursor {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }

        for (key, version, record) in to_revoke {
            match self.drive_revoke(key, version, record).await? {
                RevokeOutcome::Finalized => summary.reclaimed += 1,
                RevokeOutcome::Orphaned => summary.orphaned += 1,
            }
        }
        Ok(summary)
    }

    /// Reconcile leases left behind by a **prior incarnation** of this source (a
    /// delete/recreate replaced the incarnation). Scans the whole scope and best-effort
    /// revokes every non-terminal foreign-incarnation lease. A 403 leaves the lease
    /// `Orphaned` for the operator; a transient outage aborts the sweep to retry later.
    pub(crate) async fn reconcile_orphans(&mut self) -> Result<OrphanSummary> {
        let mut summary = OrphanSummary::default();
        let mut foreign: Vec<(String, u64, LeaseRecord)> = Vec::new();

        let mut cursor: Option<String> = None;
        loop {
            let page = self
                .store
                .list_scope(cursor.as_deref(), RECONCILE_PAGE)
                .await?;
            for (key, version, record) in page.records {
                if record.source_incarnation != self.id.incarnation
                    && !record.state.is_terminal()
                {
                    foreign.push((key, version, record));
                }
            }
            match page.next_cursor {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }

        for (key, version, record) in foreign {
            match self.drive_revoke(key, version, record).await? {
                RevokeOutcome::Finalized => summary.revoked += 1,
                RevokeOutcome::Orphaned => summary.still_orphaned += 1,
            }
        }
        Ok(summary)
    }

    /// A secret-free snapshot of the active lease for operational status. Carries no
    /// lease id and no credential material - only the lifecycle state, role/mount,
    /// renewability, expiry timing, and how many leases this run has issued. Safe to
    /// log or return from an admin/status endpoint.
    pub(crate) async fn status(&self) -> Result<LeaseStatus> {
        let mut status = LeaseStatus {
            state: "none",
            mount: self.mount.clone(),
            role: self.role.clone(),
            renewable: false,
            expires_at_ms: None,
            issued_count: self.gen_counter,
        };
        if let Some(active) = &self.active {
            if let Some((_, record)) = self.store.get(&active.key).await? {
                status.state = record.state.name();
                status.renewable = record.renewable;
                status.expires_at_ms = Some(record.expires_at_ms);
            }
        }
        Ok(status)
    }

    /// Increment a lifecycle counter, labeled by the non-secret scope identifiers only.
    /// `event` is a fixed lifecycle label (`issued`/`renewed`/`reissued`/`revoked`/
    /// `orphaned`), never anything derived from a lease id or credential.
    fn metric_lifecycle(&self, event: &'static str) {
        counter!(
            "deltaforge_vault_lease_events_total",
            "pipeline" => self.id.pipeline.clone(),
            "source" => self.id.source.clone(),
            "purpose" => self.id.credential_purpose.clone(),
            "event" => event,
        )
        .increment(1);
    }

    /// Record the active lease's true expiry (unix seconds). Timing is not secret; the
    /// lease id and credential are never emitted.
    fn metric_expiry(&self, expires_at_ms: u64) {
        gauge!(
            "deltaforge_vault_lease_active_expires_at_seconds",
            "pipeline" => self.id.pipeline.clone(),
            "source" => self.id.source.clone(),
            "purpose" => self.id.credential_purpose.clone(),
        )
        .set((expires_at_ms / 1000) as f64);
    }
}

/// A secret-free operational status snapshot of the active lease. Deliberately holds
/// no lease id and no credential material.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LeaseStatus {
    /// The active lease's lifecycle state name, or `"none"` when idle.
    pub state: &'static str,
    pub mount: String,
    pub role: String,
    pub renewable: bool,
    /// True expiry as unix milliseconds, if a lease is active.
    pub expires_at_ms: Option<u64>,
    /// How many leases this run has issued (monotonic).
    pub issued_count: u64,
}

/// Bounded page size for recovery / reconciliation scans.
const RECONCILE_PAGE: usize = 256;

/// The terminal disposition of a revoke attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RevokeOutcome {
    /// Vault confirmed (or the lease was already absent): the retained tombstone stands.
    Finalized,
    /// Not actionable by this token (403): left `Orphaned` for reconciliation.
    Orphaned,
}

/// Non-secret counts from a restart recovery scan (for logging/metrics). Recovery never
/// adopts a lease (credential material is not reconstructible), so there is no
/// "adopted" count - the caller issues fresh credentials afterwards.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RecoverySummary {
    /// Non-terminal leases reclaimed (revoked + finalized) as unusable on restart.
    pub reclaimed: u32,
    /// Leases left `Orphaned` (unauthorized) during reclamation.
    pub orphaned: u32,
    /// Retained `Revoked` tombstones skipped.
    pub tombstones: u32,
}

/// Non-secret counts from an orphan (prior-incarnation) reconciliation sweep.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OrphanSummary {
    /// Foreign-incarnation leases finalized.
    pub revoked: u32,
    /// Foreign-incarnation leases left `Orphaned` (unauthorized).
    pub still_orphaned: u32,
}

fn system_time_to_ms(t: SystemTime) -> Result<u64> {
    let d = t
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| anyhow::anyhow!("pre-epoch time"))?;
    u64::try_from(d.as_millis()).map_err(|_| anyhow::anyhow!("time overflow"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::time::UNIX_EPOCH;
    use storage::MemoryStorageBackend;

    const EPOCH_MS: u64 = 1_700_000_000_000;

    fn t(ms: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_millis(ms)
    }

    fn cfg() -> LeaseScheduleConfig {
        LeaseScheduleConfig {
            safety_margin: Duration::from_secs(30),
            renew_fraction: 0.66,
            min_poll: Duration::from_secs(1),
            max_poll: Duration::from_secs(300),
        }
    }

    fn handle(secs: u64, renewable: bool, issued_ms: u64) -> LeaseHandle {
        LeaseHandle::new(
            LeaseId::new("vault-lease-id"),
            "database",
            "orders-ro",
            Duration::from_secs(secs),
            renewable,
            t(issued_ms),
        )
        .unwrap()
    }

    // --- scheduler ---

    #[test]
    fn waits_early_in_lease_then_renews_after_fraction() {
        let h = handle(3600, true, EPOCH_MS);
        // Just issued: wait, and the wait is bounded by max_poll.
        assert_eq!(
            lease_tick(t(EPOCH_MS), &h, &cfg()),
            LeaseTick::Wait(Duration::from_secs(300))
        );
        // Consumed 66% (2376s in) -> remaining 1224s ~= 34% -> renew.
        assert_eq!(
            lease_tick(t(EPOCH_MS + 2_400_000), &h, &cfg()),
            LeaseTick::Renew
        );
    }

    #[test]
    fn reissues_in_safety_window_then_fails_closed_at_expiry() {
        let h = handle(3600, true, EPOCH_MS);
        // 10s before expiry (< 30s safety margin) -> reissue, not renew.
        assert_eq!(
            lease_tick(t(EPOCH_MS + 3_590_000), &h, &cfg()),
            LeaseTick::Reissue
        );
        // At true expiry -> terminal fail-closed, not reissue.
        assert_eq!(
            lease_tick(t(EPOCH_MS + 3_600_000), &h, &cfg()),
            LeaseTick::Expired
        );
        // Past true expiry -> still Expired (never continued service).
        assert_eq!(
            lease_tick(t(EPOCH_MS + 4_000_000), &h, &cfg()),
            LeaseTick::Expired
        );
    }

    #[test]
    fn non_renewable_reissues_at_fraction() {
        let h = handle(3600, false, EPOCH_MS);
        // Early: wait.
        assert!(matches!(
            lease_tick(t(EPOCH_MS), &h, &cfg()),
            LeaseTick::Wait(_)
        ));
        // At the fraction point a non-renewable lease reissues rather than renews.
        assert_eq!(
            lease_tick(t(EPOCH_MS + 2_400_000), &h, &cfg()),
            LeaseTick::Reissue
        );
    }

    #[test]
    fn short_lease_action_point_never_precedes_safety_margin() {
        // 40s lease, 30s margin: (1-0.66)*40 = 13.6s < margin, so act_at clamps to the
        // 30s margin. Remaining 35s -> still wait; remaining 25s -> reissue (<= margin).
        let h = handle(40, true, EPOCH_MS);
        assert!(matches!(
            lease_tick(t(EPOCH_MS + 5_000), &h, &cfg()),
            LeaseTick::Wait(_)
        ));
        assert_eq!(
            lease_tick(t(EPOCH_MS + 15_000), &h, &cfg()),
            LeaseTick::Reissue
        );
    }

    // --- manager (mock provider) ---

    #[derive(Default)]
    struct MockProvider {
        issued: Mutex<u64>,
        renews: Mutex<u64>,
        revokes: Mutex<Vec<String>>,
        renewable: bool,
        revoke_fault: Option<SecretError>,
        /// Fail this many revoke calls transiently (Unavailable) before succeeding.
        revoke_transient: Mutex<u32>,
    }

    impl MockProvider {
        fn renewable() -> Self {
            Self {
                renewable: true,
                ..Default::default()
            }
        }

        fn set_revoke_transient(&self, n: u32) {
            *self.revoke_transient.lock().unwrap() = n;
        }

        fn transient_err() -> SecretError {
            let r = secrets::SecretReference::new(
                secrets::SecretProvider::Vault,
                "sys/leases/revoke",
            );
            SecretError::Provider {
                reference: r.safe(),
                kind: ProviderFailureKind::Unavailable,
            }
        }
    }

    #[async_trait]
    impl LeaseProvider for MockProvider {
        async fn issue(
            &self,
            _mount: &str,
            _role: &str,
        ) -> Result<LeasedRead, SecretError> {
            let mut n = self.issued.lock().unwrap();
            *n += 1;
            let id = format!("lease-{n}");
            // The grouped/expiry-aware credential set is built inside the real
            // `read_db_credentials` (unit-tested in the secrets crate). The manager
            // only passes the set through, so the mock builds a plain one from the
            // public API.
            let reference = secrets::SecretReference::new(
                secrets::SecretProvider::Vault,
                "database/creds/orders-ro",
            );
            let mut credentials = secrets::CredentialSet::new();
            for (k, v) in [("username", "u"), ("password", "p")] {
                let material = secrets::SecretMaterial::Utf8(
                    secrets::SecretString::new(
                        v.to_string(),
                        secrets::DEFAULT_MAX_SECRET_BYTES,
                        &reference,
                    )
                    .unwrap(),
                );
                credentials
                    .insert(k, secrets::ResolvedSecret::new(material))
                    .unwrap();
            }
            Ok(LeasedRead {
                credentials,
                lease_id: id,
                lease_duration: Duration::from_secs(3600),
                renewable: self.renewable,
            })
        }
        async fn renew(
            &self,
            _lease_id: &str,
            _increment_secs: Option<u64>,
        ) -> Result<LeaseInfo, SecretError> {
            *self.renews.lock().unwrap() += 1;
            Ok(LeaseInfo {
                lease_duration: Duration::from_secs(3600),
                renewable: self.renewable,
            })
        }
        async fn revoke(&self, lease_id: &str) -> Result<(), SecretError> {
            {
                let mut remaining = self.revoke_transient.lock().unwrap();
                if *remaining > 0 {
                    *remaining -= 1;
                    return Err(MockProvider::transient_err());
                }
            }
            if let Some(e) = &self.revoke_fault {
                return Err(clone_err(e));
            }
            self.revokes.lock().unwrap().push(lease_id.to_string());
            Ok(())
        }
    }

    fn clone_err(e: &SecretError) -> SecretError {
        match e {
            SecretError::NotFound(r) => SecretError::NotFound(r.clone()),
            SecretError::Provider { reference, kind } => {
                SecretError::Provider {
                    reference: reference.clone(),
                    kind: *kind,
                }
            }
            _ => unreachable!("mock only uses NotFound/Provider"),
        }
    }

    fn identity() -> LeaseIdentity {
        identity_with("inc-1")
    }

    fn identity_with(incarnation: &str) -> LeaseIdentity {
        LeaseIdentity {
            tenant: "acme".into(),
            pipeline: "pipe".into(),
            source: "pg1".into(),
            credential_purpose: "source-db".into(),
            incarnation: incarnation.into(),
        }
    }

    fn manager(provider: Arc<dyn LeaseProvider>) -> LeaseManager {
        manager_on(provider, Arc::new(MemoryStorageBackend::new()), identity())
    }

    fn manager_on(
        provider: Arc<dyn LeaseProvider>,
        backend: storage::ArcStorageBackend,
        id: LeaseIdentity,
    ) -> LeaseManager {
        LeaseManager::new(provider, backend, id, "database", "orders-ro", cfg())
    }

    /// A storage backend that delegates to an inner one but can be told to fail
    /// `slot_get`/`slot_cas` for a specific key, to inject store faults at precise
    /// points (promotion CAS, record load) without racing.
    #[derive(Debug)]
    struct FaultyBackend {
        inner: storage::ArcStorageBackend,
        fail_get_key: Mutex<Option<String>>,
        fail_cas_key: Mutex<Option<String>>,
    }

    impl FaultyBackend {
        fn new(inner: storage::ArcStorageBackend) -> Self {
            Self {
                inner,
                fail_get_key: Mutex::new(None),
                fail_cas_key: Mutex::new(None),
            }
        }
        fn fail_get_for(&self, key: Option<String>) {
            *self.fail_get_key.lock().unwrap() = key;
        }
        fn fail_cas_for(&self, key: Option<String>) {
            *self.fail_cas_key.lock().unwrap() = key;
        }
    }

    #[async_trait]
    impl storage::StorageBackend for FaultyBackend {
        async fn kv_get(&self, ns: &str, key: &str) -> Result<Option<Vec<u8>>> {
            self.inner.kv_get(ns, key).await
        }
        async fn kv_put(&self, ns: &str, key: &str, v: &[u8]) -> Result<()> {
            self.inner.kv_put(ns, key, v).await
        }
        async fn kv_put_with_ttl(
            &self,
            ns: &str,
            key: &str,
            v: &[u8],
            ttl: u64,
        ) -> Result<()> {
            self.inner.kv_put_with_ttl(ns, key, v, ttl).await
        }
        async fn kv_delete(&self, ns: &str, key: &str) -> Result<bool> {
            self.inner.kv_delete(ns, key).await
        }
        async fn kv_list(
            &self,
            ns: &str,
            prefix: Option<&str>,
        ) -> Result<Vec<String>> {
            self.inner.kv_list(ns, prefix).await
        }
        async fn log_append(
            &self,
            ns: &str,
            key: &str,
            v: &[u8],
        ) -> Result<u64> {
            self.inner.log_append(ns, key, v).await
        }
        async fn log_list(
            &self,
            ns: &str,
            key: &str,
        ) -> Result<Vec<(u64, Vec<u8>)>> {
            self.inner.log_list(ns, key).await
        }
        async fn log_since(
            &self,
            ns: &str,
            key: &str,
            since: u64,
        ) -> Result<Vec<(u64, Vec<u8>)>> {
            self.inner.log_since(ns, key, since).await
        }
        async fn log_latest(
            &self,
            ns: &str,
            key: &str,
        ) -> Result<Option<(u64, Vec<u8>)>> {
            self.inner.log_latest(ns, key).await
        }
        async fn log_append_if_absent(
            &self,
            ns: &str,
            key: &str,
            cid: &str,
            v: &[u8],
        ) -> Result<storage::LogAppendOutcome> {
            self.inner.log_append_if_absent(ns, key, cid, v).await
        }
        async fn log_truncate(
            &self,
            ns: &str,
            key: &str,
            req: storage::LogTruncateRequest,
        ) -> Result<storage::LogTruncateOutcome> {
            self.inner.log_truncate(ns, key, req).await
        }
        async fn log_stream_meta(
            &self,
            ns: &str,
            key: &str,
        ) -> Result<storage::LogStreamMeta> {
            self.inner.log_stream_meta(ns, key).await
        }
        async fn log_read_meta_since(
            &self,
            ns: &str,
            key: &str,
            since: u64,
            limit: usize,
        ) -> Result<Vec<storage::LogEntryMeta>> {
            self.inner.log_read_meta_since(ns, key, since, limit).await
        }
        async fn slot_upsert(
            &self,
            ns: &str,
            key: &str,
            s: &[u8],
        ) -> Result<u64> {
            self.inner.slot_upsert(ns, key, s).await
        }
        async fn slot_get(
            &self,
            ns: &str,
            key: &str,
        ) -> Result<Option<(u64, Vec<u8>)>> {
            if self.fail_get_key.lock().unwrap().as_deref() == Some(key) {
                anyhow::bail!("injected slot_get failure for {key}");
            }
            self.inner.slot_get(ns, key).await
        }
        async fn slot_cas(
            &self,
            ns: &str,
            key: &str,
            expected: u64,
            s: &[u8],
        ) -> Result<bool> {
            if self.fail_cas_key.lock().unwrap().as_deref() == Some(key) {
                anyhow::bail!("injected slot_cas failure for {key}");
            }
            self.inner.slot_cas(ns, key, expected, s).await
        }
        async fn slot_create(
            &self,
            ns: &str,
            key: &str,
            s: &[u8],
        ) -> Result<Option<u64>> {
            self.inner.slot_create(ns, key, s).await
        }
        async fn slot_delete(&self, ns: &str, key: &str) -> Result<bool> {
            self.inner.slot_delete(ns, key).await
        }
        async fn slot_list(
            &self,
            ns: &str,
            prefix: Option<&str>,
            cursor: Option<&str>,
            limit: usize,
        ) -> Result<storage::SlotPage> {
            self.inner.slot_list(ns, prefix, cursor, limit).await
        }
        async fn queue_push(
            &self,
            ns: &str,
            key: &str,
            v: &[u8],
        ) -> Result<u64> {
            self.inner.queue_push(ns, key, v).await
        }
        async fn queue_peek(
            &self,
            ns: &str,
            key: &str,
            limit: usize,
        ) -> Result<Vec<(u64, Vec<u8>)>> {
            self.inner.queue_peek(ns, key, limit).await
        }
        async fn queue_ack(
            &self,
            ns: &str,
            key: &str,
            up_to: u64,
        ) -> Result<usize> {
            self.inner.queue_ack(ns, key, up_to).await
        }
        async fn queue_len(&self, ns: &str, key: &str) -> Result<u64> {
            self.inner.queue_len(ns, key).await
        }
        async fn queue_drop_oldest(
            &self,
            ns: &str,
            key: &str,
            count: usize,
        ) -> Result<usize> {
            self.inner.queue_drop_oldest(ns, key, count).await
        }
    }

    #[tokio::test]
    async fn issue_persists_active_record() {
        let mut m = manager(Arc::new(MockProvider::renewable()));
        let leased = m.issue(t(EPOCH_MS)).await.unwrap();
        assert_eq!(
            leased
                .credentials
                .require("username")
                .unwrap()
                .material()
                .as_utf8(),
            Some("u")
        );
        // The active record is durably Active.
        let (_, rec) = m
            .store
            .get(&m.active.as_ref().unwrap().key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rec.state, LeaseState::Active);
    }

    #[tokio::test]
    async fn renew_extends_expiry_monotonically() {
        let provider = Arc::new(MockProvider::renewable());
        let mut m = manager(provider.clone());
        m.issue(t(EPOCH_MS)).await.unwrap();
        let (_, before) = m
            .store
            .get(&m.active.as_ref().unwrap().key)
            .await
            .unwrap()
            .unwrap();
        // Renew at +2000s: new expiry = now + 3600s > old expiry.
        let refreshed = m.renew(t(EPOCH_MS + 2_000_000)).await.unwrap();
        assert!(refreshed.expires_at > handle_expiry(before.expires_at_ms));
        assert_eq!(*provider.renews.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn reissue_keeps_old_active_until_apply() {
        // Two-handle protocol: reissue mints a PENDING replacement; the old lease keeps
        // serving (Active) and nothing is revoked until an apply outcome arrives.
        let provider = Arc::new(MockProvider::renewable());
        let mut m = manager(provider.clone());
        m.issue(t(EPOCH_MS)).await.unwrap();
        let old_key = m.active.as_ref().unwrap().key.clone();

        m.reissue(t(EPOCH_MS + 3_000_000)).await.unwrap();
        let new_key = m.pending.as_ref().unwrap().key.clone();
        assert_ne!(old_key, new_key);

        // Old is still Active; new is Pending; nothing revoked yet.
        let (_, old_rec) = m.store.get(&old_key).await.unwrap().unwrap();
        assert_eq!(old_rec.state, LeaseState::Active);
        let (_, new_rec) = m.store.get(&new_key).await.unwrap().unwrap();
        assert_eq!(new_rec.state, LeaseState::Pending);
        assert_eq!(provider.revokes.lock().unwrap().len(), 0);

        // A second reissue while one is pending is rejected.
        assert!(m.reissue(t(EPOCH_MS + 3_100_000)).await.is_err());
    }

    #[tokio::test]
    async fn resolve_reissue_applied_promotes_new_and_revokes_old() {
        let provider = Arc::new(MockProvider::renewable());
        let mut m = manager(provider.clone());
        m.issue(t(EPOCH_MS)).await.unwrap();
        let old_key = m.active.as_ref().unwrap().key.clone();
        m.reissue(t(EPOCH_MS + 3_000_000)).await.unwrap();
        let new_key = m.pending.as_ref().unwrap().key.clone();

        let decision = m
            .resolve_reissue(ApplyOutcome::Applied, false)
            .await
            .unwrap();
        assert_eq!(decision, LeaseRevokeDecision::PromoteNewRevokeOld);
        assert_eq!(m.active.as_ref().unwrap().key, new_key);
        assert!(m.pending.is_none());

        let (_, new_rec) = m.store.get(&new_key).await.unwrap().unwrap();
        assert_eq!(new_rec.state, LeaseState::Active);
        let (_, old_rec) = m.store.get(&old_key).await.unwrap().unwrap();
        assert_eq!(old_rec.state, LeaseState::Revoked);
        assert_eq!(provider.revokes.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn resolve_reissue_not_applied_revokes_new_keeps_old() {
        use crate::rotation::RotationReject;
        let provider = Arc::new(MockProvider::renewable());
        let mut m = manager(provider.clone());
        m.issue(t(EPOCH_MS)).await.unwrap();
        let old_key = m.active.as_ref().unwrap().key.clone();
        m.reissue(t(EPOCH_MS + 3_000_000)).await.unwrap();
        let new_key = m.pending.as_ref().unwrap().key.clone();

        // A non-transient kept-old outcome: revoke the new replacement, keep the old.
        let decision = m
            .resolve_reissue(
                ApplyOutcome::KeptOld {
                    reason: RotationReject::IdentityMismatch,
                },
                false,
            )
            .await
            .unwrap();
        assert_eq!(decision, LeaseRevokeDecision::RevokeNew);
        assert_eq!(m.active.as_ref().unwrap().key, old_key);
        assert!(m.pending.is_none());
        let (_, old_rec) = m.store.get(&old_key).await.unwrap().unwrap();
        assert_eq!(old_rec.state, LeaseState::Active);
        let (_, new_rec) = m.store.get(&new_key).await.unwrap().unwrap();
        assert_eq!(new_rec.state, LeaseState::Revoked);
    }

    #[tokio::test]
    async fn resolve_reissue_transient_retains_both() {
        use crate::rotation::RotationReject;
        let provider = Arc::new(MockProvider::renewable());
        let mut m = manager(provider.clone());
        m.issue(t(EPOCH_MS)).await.unwrap();
        let old_key = m.active.as_ref().unwrap().key.clone();
        m.reissue(t(EPOCH_MS + 3_000_000)).await.unwrap();
        let new_key = m.pending.as_ref().unwrap().key.clone();

        let decision = m
            .resolve_reissue(
                ApplyOutcome::KeptOld {
                    reason: RotationReject::PreflightFailed,
                },
                true,
            )
            .await
            .unwrap();
        assert_eq!(decision, LeaseRevokeDecision::RetainBoth);
        // Both handles survive; nothing revoked.
        assert_eq!(m.active.as_ref().unwrap().key, old_key);
        assert_eq!(m.pending.as_ref().unwrap().key, new_key);
        assert_eq!(provider.revokes.lock().unwrap().len(), 0);
    }

    // --- injected-failure resilience (handles retained until durable success) ---

    async fn issued_and_reissued(
        provider: Arc<MockProvider>,
        backend: storage::ArcStorageBackend,
    ) -> (LeaseManager, String, String) {
        let mut m = manager_on(provider, backend, identity());
        m.issue(t(EPOCH_MS)).await.unwrap();
        let old_key = m.active.as_ref().unwrap().key.clone();
        m.reissue(t(EPOCH_MS + 3_000_000)).await.unwrap();
        let new_key = m.pending.as_ref().unwrap().key.clone();
        (m, old_key, new_key)
    }

    #[tokio::test]
    async fn resolve_reissue_promotion_failure_keeps_both_handles() {
        let provider = Arc::new(MockProvider::renewable());
        let faulty =
            Arc::new(FaultyBackend::new(Arc::new(MemoryStorageBackend::new())));
        let (mut m, old_key, new_key) =
            issued_and_reissued(provider.clone(), faulty.clone()).await;

        // Fail the promotion CAS on the pending key.
        faulty.fail_cas_for(Some(new_key.clone()));
        assert!(
            m.resolve_reissue(ApplyOutcome::Applied, false)
                .await
                .is_err()
        );
        // Both handles remain installed; the old lease is still serving.
        assert_eq!(m.active.as_ref().unwrap().key, old_key);
        assert_eq!(m.pending.as_ref().unwrap().key, new_key);
        assert_eq!(provider.revokes.lock().unwrap().len(), 0);

        // Clear the fault and retry: the reissue completes.
        faulty.fail_cas_for(None);
        let decision = m
            .resolve_reissue(ApplyOutcome::Applied, false)
            .await
            .unwrap();
        assert_eq!(decision, LeaseRevokeDecision::PromoteNewRevokeOld);
        assert_eq!(m.active.as_ref().unwrap().key, new_key);
        assert!(m.pending.is_none() && m.revoking.is_none());
    }

    #[tokio::test]
    async fn resolve_reissue_old_record_load_failure_stages_for_retry() {
        let provider = Arc::new(MockProvider::renewable());
        let faulty =
            Arc::new(FaultyBackend::new(Arc::new(MemoryStorageBackend::new())));
        let (mut m, old_key, new_key) =
            issued_and_reissued(provider.clone(), faulty.clone()).await;

        // Promotion succeeds; the old-record load in drain_revoking fails.
        faulty.fail_get_for(Some(old_key.clone()));
        assert!(
            m.resolve_reissue(ApplyOutcome::Applied, false)
                .await
                .is_err()
        );
        // The new lease is promoted and serving; the old lease is staged for retry.
        assert_eq!(m.active.as_ref().unwrap().key, new_key);
        assert!(m.pending.is_none());
        assert_eq!(m.revoking.as_ref().unwrap().key, old_key);
        assert_eq!(provider.revokes.lock().unwrap().len(), 0);

        // Clear the fault and retry the staged revoke.
        faulty.fail_get_for(None);
        assert_eq!(
            m.retry_revoking().await.unwrap(),
            Some(RevokeOutcome::Finalized)
        );
        assert!(m.revoking.is_none());
        let (_, old_rec) = m.store.get(&old_key).await.unwrap().unwrap();
        assert_eq!(old_rec.state, LeaseState::Revoked);
    }

    #[tokio::test]
    async fn resolve_reissue_revoke_failure_stages_for_retry() {
        let provider = Arc::new(MockProvider::renewable());
        provider.set_revoke_transient(1); // first revoke fails transiently
        let backend: storage::ArcStorageBackend =
            Arc::new(MemoryStorageBackend::new());
        let (mut m, old_key, new_key) =
            issued_and_reissued(provider.clone(), backend).await;

        // Promotion + old load succeed; the Vault revoke fails transiently.
        assert!(
            m.resolve_reissue(ApplyOutcome::Applied, false)
                .await
                .is_err()
        );
        assert_eq!(m.active.as_ref().unwrap().key, new_key);
        assert_eq!(m.revoking.as_ref().unwrap().key, old_key);

        // Retry: the revoke now succeeds and the old lease is finalized.
        assert_eq!(
            m.retry_revoking().await.unwrap(),
            Some(RevokeOutcome::Finalized)
        );
        assert!(m.revoking.is_none());
        let (_, old_rec) = m.store.get(&old_key).await.unwrap().unwrap();
        assert_eq!(old_rec.state, LeaseState::Revoked);
    }

    #[tokio::test]
    async fn promote_pending_success_stages_old_failure_keeps_handles() {
        // Success: pending -> Active, old staged in `revoking`, not yet revoked.
        let provider = Arc::new(MockProvider::renewable());
        let mut m = manager(provider);
        m.issue(t(EPOCH_MS)).await.unwrap();
        let old_key = m.active.as_ref().unwrap().key.clone();
        m.reissue(t(EPOCH_MS + 3_000_000)).await.unwrap();
        let new_key = m.pending.as_ref().unwrap().key.clone();
        m.promote_pending().await.unwrap();
        assert_eq!(m.active.as_ref().unwrap().key, new_key);
        assert!(m.pending.is_none());
        assert_eq!(m.revoking.as_ref().unwrap().key, old_key);
        let (_, new_rec) = m.store.get(&new_key).await.unwrap().unwrap();
        assert_eq!(new_rec.state, LeaseState::Active);

        // Failure (promotion CAS fails): handles unchanged for retry / fatal signalling.
        let provider2 = Arc::new(MockProvider::renewable());
        let faulty =
            Arc::new(FaultyBackend::new(Arc::new(MemoryStorageBackend::new())));
        let (mut m2, old2, new2) =
            issued_and_reissued(provider2, faulty.clone()).await;
        faulty.fail_cas_for(Some(new2.clone()));
        assert!(m2.promote_pending().await.is_err());
        assert_eq!(m2.active.as_ref().unwrap().key, old2);
        assert_eq!(m2.pending.as_ref().unwrap().key, new2);
        assert!(m2.revoking.is_none());
    }

    #[tokio::test]
    async fn reissue_refused_while_a_lease_awaits_revocation() {
        // A transient revoke failure leaves the old lease staged in `revoking`; a new
        // reissue must be refused until retry_revoking drains it, so the staged handle
        // is never overwritten.
        let provider = Arc::new(MockProvider::renewable());
        provider.set_revoke_transient(1);
        let backend: storage::ArcStorageBackend =
            Arc::new(MemoryStorageBackend::new());
        let (mut m, old_key, _new_key) =
            issued_and_reissued(provider.clone(), backend).await;

        // Applied: promotion + old load succeed, revoke fails transiently -> staged.
        assert!(
            m.resolve_reissue(ApplyOutcome::Applied, false)
                .await
                .is_err()
        );
        assert_eq!(m.revoking.as_ref().unwrap().key, old_key);

        // Another reissue is refused while the earlier lease is still staged.
        assert!(
            m.reissue(t(EPOCH_MS + 6_000_000)).await.is_err(),
            "reissue must be refused while a lease awaits revocation"
        );
        assert_eq!(
            m.revoking.as_ref().unwrap().key,
            old_key,
            "the staged handle is preserved"
        );

        // Drain the staged revoke, then reissue is allowed again.
        assert_eq!(
            m.retry_revoking().await.unwrap(),
            Some(RevokeOutcome::Finalized)
        );
        assert!(m.revoking.is_none());
        m.reissue(t(EPOCH_MS + 7_000_000)).await.unwrap();
        assert!(m.pending.is_some());
    }

    #[tokio::test]
    async fn resolve_reissue_revoke_new_failure_keeps_old_and_stages_new() {
        use crate::rotation::RotationReject;
        let provider = Arc::new(MockProvider::renewable());
        provider.set_revoke_transient(1);
        let backend: storage::ArcStorageBackend =
            Arc::new(MemoryStorageBackend::new());
        let (mut m, old_key, new_key) =
            issued_and_reissued(provider.clone(), backend).await;

        // Not adopted -> revoke the pending; the first revoke fails transiently.
        assert!(
            m.resolve_reissue(
                ApplyOutcome::KeptOld {
                    reason: RotationReject::IdentityMismatch,
                },
                false,
            )
            .await
            .is_err()
        );
        // Old lease still serving; the pending is staged for revoke retry.
        assert_eq!(m.active.as_ref().unwrap().key, old_key);
        assert_eq!(m.revoking.as_ref().unwrap().key, new_key);

        assert_eq!(
            m.retry_revoking().await.unwrap(),
            Some(RevokeOutcome::Finalized)
        );
        let (_, new_rec) = m.store.get(&new_key).await.unwrap().unwrap();
        assert_eq!(new_rec.state, LeaseState::Revoked);
        // The old lease is untouched and still active.
        let (_, old_rec) = m.store.get(&old_key).await.unwrap().unwrap();
        assert_eq!(old_rec.state, LeaseState::Active);
    }

    #[tokio::test]
    async fn renew_pending_extends_across_retries_beyond_initial_ttl() {
        // RetainBoth keeps a pending replacement alive across a prolonged reconnect;
        // renew_pending must extend it beyond its initial TTL.
        let provider = Arc::new(MockProvider::renewable());
        let mut m = manager(provider);
        m.issue(t(EPOCH_MS)).await.unwrap();
        m.reissue(t(EPOCH_MS + 100_000)).await.unwrap();
        let pending_key = m.pending.as_ref().unwrap().key.clone();

        // Initial pending expiry ~ issued(+100s) + 3600s.
        let (_, before) = m.store.get(&pending_key).await.unwrap().unwrap();
        let initial_expiry = before.expires_at_ms;

        // Renew repeatedly, each well beyond the previous grant (each renew extends to
        // now + 3600s). Walk past the initial TTL.
        for step_s in [3000u64, 6000, 9000] {
            let h = m.renew_pending(t(EPOCH_MS + step_s * 1000)).await.unwrap();
            // Expiry keeps advancing and always stays in the future of `now`.
            assert!(h.expires_at > t(EPOCH_MS + step_s * 1000));
        }
        let (_, after) = m.store.get(&pending_key).await.unwrap().unwrap();
        assert!(
            after.expires_at_ms > initial_expiry,
            "pending lease renewed beyond its initial TTL"
        );
        // The active lease is untouched by pending renewal.
        assert!(m.active.is_some());
    }

    #[tokio::test]
    async fn revoke_active_finalizes_tombstone() {
        let provider = Arc::new(MockProvider::renewable());
        let mut m = manager(provider.clone());
        m.issue(t(EPOCH_MS)).await.unwrap();
        let key = m.active.as_ref().unwrap().key.clone();
        m.revoke_active().await.unwrap();
        assert!(m.active.is_none());
        let (_, rec) = m.store.get(&key).await.unwrap().unwrap();
        assert_eq!(rec.state, LeaseState::Revoked);
    }

    #[tokio::test]
    async fn revoke_absent_lease_is_idempotent_success() {
        let reference = secrets::SecretReference::new(
            secrets::SecretProvider::Vault,
            "database/creds/orders-ro",
        );
        let provider = Arc::new(MockProvider {
            renewable: true,
            revoke_fault: Some(SecretError::NotFound(reference.safe())),
            ..Default::default()
        });
        let mut m = manager(provider);
        m.issue(t(EPOCH_MS)).await.unwrap();
        let key = m.active.as_ref().unwrap().key.clone();
        // An absent lease at Vault still finalizes the record (idempotent).
        m.revoke_active().await.unwrap();
        let (_, rec) = m.store.get(&key).await.unwrap().unwrap();
        assert_eq!(rec.state, LeaseState::Revoked);
    }

    #[tokio::test]
    async fn revoke_forbidden_lease_becomes_orphaned() {
        let reference = secrets::SecretReference::new(
            secrets::SecretProvider::Vault,
            "database/creds/orders-ro",
        );
        let provider = Arc::new(MockProvider {
            renewable: true,
            revoke_fault: Some(SecretError::Provider {
                reference: reference.safe(),
                kind: ProviderFailureKind::Forbidden,
            }),
            ..Default::default()
        });
        let mut m = manager(provider);
        m.issue(t(EPOCH_MS)).await.unwrap();
        let key = m.active.as_ref().unwrap().key.clone();
        m.revoke_active().await.unwrap();
        let (_, rec) = m.store.get(&key).await.unwrap().unwrap();
        assert_eq!(rec.state, LeaseState::Orphaned, "403 -> orphaned");
    }

    #[test]
    fn classify_fault_table() {
        let r = secrets::SecretReference::new(
            secrets::SecretProvider::Vault,
            "x/creds/y",
        );
        let p = |k| SecretError::Provider {
            reference: r.safe(),
            kind: k,
        };
        assert_eq!(
            classify_fault(&SecretError::NotFound(r.safe())),
            OpFault::Absent
        );
        assert_eq!(
            classify_fault(&p(ProviderFailureKind::Unavailable)),
            OpFault::Transient
        );
        assert_eq!(
            classify_fault(&p(ProviderFailureKind::Timeout)),
            OpFault::Transient
        );
        assert_eq!(
            classify_fault(&p(ProviderFailureKind::Forbidden)),
            OpFault::Unauthorized
        );
        assert_eq!(
            classify_fault(&p(ProviderFailureKind::Other)),
            OpFault::Fatal
        );
    }

    // --- recovery / orphan reconciliation ---

    /// Seed a `Pending` record directly (simulating a crash after `create_pending` but
    /// before the material was ever handed out).
    async fn seed_pending(
        m: &LeaseManager,
        incarnation: &str,
        gen_id: &str,
        lease_id: &str,
    ) {
        let handle = LeaseHandle::new(
            LeaseId::new(lease_id),
            "database",
            "orders-ro",
            Duration::from_secs(3600),
            true,
            t(EPOCH_MS),
        )
        .unwrap();
        let record = LeaseRecord::pending(
            "acme",
            "pipe",
            "pg1",
            incarnation,
            "source-db",
            gen_id,
            &handle,
            1,
        )
        .unwrap();
        m.store.create_pending(&record).await.unwrap();
    }

    #[tokio::test]
    async fn recover_reclaims_current_incarnation_without_adopting() {
        let backend: storage::ArcStorageBackend =
            Arc::new(MemoryStorageBackend::new());
        let provider = Arc::new(MockProvider::renewable());

        // First run: issue an Active lease, then crash leaving a stray Pending.
        let mut m1 = manager_on(provider.clone(), backend.clone(), identity());
        m1.issue(t(EPOCH_MS)).await.unwrap();
        let active_key = m1.active.as_ref().unwrap().key.clone();
        seed_pending(&m1, "inc-1", "stray-gen", "stray-lease").await;

        // Second run over the same store: recover reclaims BOTH (credential material is
        // not reconstructible, so a recovered lease is never adopted as usable).
        let mut m2 = manager_on(provider.clone(), backend.clone(), identity());
        let summary = m2.recover().await.unwrap();

        assert_eq!(
            summary.reclaimed, 2,
            "old Active + stray Pending reclaimed"
        );
        assert!(m2.active.is_none(), "recovery never adopts a usable lease");
        let (_, active) = m2.store.get(&active_key).await.unwrap().unwrap();
        assert_eq!(active.state, LeaseState::Revoked);

        // The caller issues fresh credentials afterwards.
        m2.issue(t(EPOCH_MS + 1000)).await.unwrap();
        assert_eq!(m2.status().await.unwrap().state, "active");
    }

    #[tokio::test]
    async fn recover_leaves_foreign_incarnation_untouched() {
        let backend: storage::ArcStorageBackend =
            Arc::new(MemoryStorageBackend::new());
        let provider = Arc::new(MockProvider::renewable());

        // A prior incarnation's Active lease.
        let mut old = manager_on(
            provider.clone(),
            backend.clone(),
            identity_with("inc-0"),
        );
        old.issue(t(EPOCH_MS)).await.unwrap();
        let old_key = old.active.as_ref().unwrap().key.clone();

        // New incarnation recovers: nothing of its own to reclaim, old lease untouched.
        let mut new = manager_on(
            provider.clone(),
            backend.clone(),
            identity_with("inc-1"),
        );
        let summary = new.recover().await.unwrap();
        assert_eq!(summary.reclaimed, 0);
        assert!(new.active.is_none());
        let (_, rec) = new.store.get(&old_key).await.unwrap().unwrap();
        assert_eq!(
            rec.state,
            LeaseState::Active,
            "foreign incarnation untouched"
        );
    }

    #[tokio::test]
    async fn reconcile_orphans_revokes_prior_incarnation() {
        let backend: storage::ArcStorageBackend =
            Arc::new(MemoryStorageBackend::new());
        let provider = Arc::new(MockProvider::renewable());

        let mut old = manager_on(
            provider.clone(),
            backend.clone(),
            identity_with("inc-0"),
        );
        old.issue(t(EPOCH_MS)).await.unwrap();
        let old_key = old.active.as_ref().unwrap().key.clone();

        let mut new = manager_on(
            provider.clone(),
            backend.clone(),
            identity_with("inc-1"),
        );
        let summary = new.reconcile_orphans().await.unwrap();
        assert_eq!(summary.revoked, 1);
        assert_eq!(summary.still_orphaned, 0);
        let (_, rec) = new.store.get(&old_key).await.unwrap().unwrap();
        assert_eq!(rec.state, LeaseState::Revoked);
    }

    #[tokio::test]
    async fn reconcile_orphans_leaves_forbidden_lease_orphaned() {
        let reference = secrets::SecretReference::new(
            secrets::SecretProvider::Vault,
            "database/creds/orders-ro",
        );
        let backend: storage::ArcStorageBackend =
            Arc::new(MemoryStorageBackend::new());
        // Old run can revoke; new run's provider is denied (403).
        let ok_provider = Arc::new(MockProvider::renewable());
        let mut old =
            manager_on(ok_provider, backend.clone(), identity_with("inc-0"));
        old.issue(t(EPOCH_MS)).await.unwrap();
        let old_key = old.active.as_ref().unwrap().key.clone();

        let denied = Arc::new(MockProvider {
            renewable: true,
            revoke_fault: Some(SecretError::Provider {
                reference: reference.safe(),
                kind: ProviderFailureKind::Forbidden,
            }),
            ..Default::default()
        });
        let mut new =
            manager_on(denied, backend.clone(), identity_with("inc-1"));
        let summary = new.reconcile_orphans().await.unwrap();
        assert_eq!(summary.revoked, 0);
        assert_eq!(summary.still_orphaned, 1);
        let (_, rec) = new.store.get(&old_key).await.unwrap().unwrap();
        assert_eq!(rec.state, LeaseState::Orphaned);
    }

    // --- operational status (secret-free) ---

    #[tokio::test]
    async fn status_is_secret_free_and_reflects_active_lease() {
        let mut m = manager(Arc::new(MockProvider::renewable()));
        // Idle before issuing.
        assert_eq!(m.status().await.unwrap().state, "none");

        m.issue(t(EPOCH_MS)).await.unwrap();
        let s = m.status().await.unwrap();
        assert_eq!(s.state, "active");
        assert_eq!(s.role, "orders-ro");
        assert!(s.renewable);
        assert!(s.expires_at_ms.is_some());
        assert_eq!(s.issued_count, 1);

        // The status must never carry the lease id (mock issues "lease-1").
        let dbg = format!("{s:?}");
        assert!(!dbg.contains("lease-"), "status leaks a lease id: {dbg}");
        assert!(!dbg.contains("username") && !dbg.contains("password"));
    }

    // helper for private-field assertions
    fn handle_expiry(ms: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_millis(ms)
    }
}
