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
use secrets::{LeaseInfo, LeasedRead, ProviderFailureKind, SecretError};

use crate::vault_lease::{
    LeaseAction, LeaseEvent, LeaseHandle, LeaseId, LeaseRecord, LeaseState,
    LeaseStore, LeasedCredentialSet, on_lease_event,
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
    /// Reissue now: the safety deadline was reached, or a non-renewable lease has
    /// consumed its renew fraction.
    Reissue,
}

/// The pure scheduling step. Given `now` and the active lease handle, decide whether
/// to renew, reissue, or wait. Works from the *remaining* lifetime against the granted
/// duration, so the decision is stable across recomputes: waking early only shortens
/// the returned wait, it never triggers a premature action.
pub(crate) fn lease_tick(
    now: SystemTime,
    handle: &LeaseHandle,
    cfg: &LeaseScheduleConfig,
) -> LeaseTick {
    let remaining = handle
        .expires_at
        .duration_since(now)
        .unwrap_or(Duration::ZERO);

    // At or past the safety deadline: must stop serving the old lease -> reissue.
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
    /// The active lease's durable key and current slot version, once issued.
    active: Option<ActiveLease>,
}

/// The manager's handle on the currently-serving lease.
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
        }
    }

    /// Issue a fresh lease and adopt it as the active one, persisting a `Pending`
    /// record **before** exposing the credential material and transitioning it to
    /// `Active` only after it is durably owned.
    pub(crate) async fn issue(
        &mut self,
        now: SystemTime,
    ) -> Result<LeasedCredentialSet> {
        let leased = self.issue_pending(now).await?;
        // Pending -> Active: the credential is durably owned, adopt it.
        self.promote_active().await?;
        Ok(leased)
    }

    /// Issue a lease and persist it as `Pending`, recording it as the active lease.
    /// Split out so reissue can create the replacement before superseding the old one.
    async fn issue_pending(
        &mut self,
        now: SystemTime,
    ) -> Result<LeasedCredentialSet> {
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
        self.active = Some(ActiveLease {
            key: record.key(),
            version,
        });
        Ok(LeasedCredentialSet {
            credentials: read.credentials,
            lease: handle,
        })
    }

    /// Transition the active lease `Pending -> Active` via a validated CAS.
    async fn promote_active(&mut self) -> Result<()> {
        self.apply_transition(LeaseState::Active).await
    }

    /// Load, transition to `next`, and CAS the active record, refreshing the tracked
    /// slot version. Fails closed if the transition is illegal for the current state.
    async fn apply_transition(&mut self, next: LeaseState) -> Result<()> {
        let active = self
            .active
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("no active lease"))?;
        let key = active.key.clone();
        let (version, current) =
            self.store.get(&key).await?.ok_or_else(|| {
                anyhow::anyhow!("active lease record missing")
            })?;
        let updated = current
            .transitioned(next)
            .ok_or_else(|| anyhow::anyhow!("illegal lease transition"))?;
        if !self.store.cas(&key, version, &updated).await? {
            anyhow::bail!("lease store version conflict on transition");
        }
        // Re-read to capture the post-CAS slot version.
        let (new_version, _) =
            self.store.get(&key).await?.ok_or_else(|| {
                anyhow::anyhow!("active lease record missing")
            })?;
        self.active = Some(ActiveLease {
            key,
            version: new_version,
        });
        Ok(())
    }

    /// Renew the active lease. On success, advance timing via the state machine's
    /// `UpdateTiming` action (persisted with a validated CAS). Returns the refreshed
    /// handle timing so the caller can reschedule.
    pub(crate) async fn renew(
        &mut self,
        now: SystemTime,
    ) -> Result<LeaseHandle> {
        let (key, version, current) = self.load_active().await?;
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
            // Renew extends from now; keep issued_at, refresh duration/expiry.
            updated.lease_duration_ms = info.lease_duration.as_millis() as u64;
            updated.expires_at_ms = expires_at_ms;
            updated.renewable = renewable;
            updated.auth_session_epoch += 1;
        }
        if !self.store.cas(&key, version, &updated).await? {
            anyhow::bail!("lease store version conflict on renew");
        }
        let (new_version, refreshed) =
            self.store.get(&key).await?.ok_or_else(|| {
                anyhow::anyhow!("active lease record missing")
            })?;
        self.active = Some(ActiveLease {
            key,
            version: new_version,
        });
        Ok(refreshed.lease()?)
    }

    /// Reissue: mint a fresh lease, adopt it as active, then revoke the superseded
    /// one. Phase 2 does not touch any source DSN; the new credential simply becomes
    /// the active lease. Returns the new leased credential set.
    pub(crate) async fn reissue(
        &mut self,
        now: SystemTime,
    ) -> Result<LeasedCredentialSet> {
        // Remember the outgoing lease so we can revoke it after the new one is owned.
        let old = self.load_active().await.ok();

        // Mint + adopt the replacement (Pending -> Active). `issue_pending` overwrites
        // `self.active` with the new lease.
        let leased = self.issue_pending(now).await?;
        self.promote_active().await?;

        // Supersede + revoke the old lease, if there was one.
        if let Some((old_key, old_version, old_record)) = old {
            self.drive_revoke(old_key, old_version, old_record).await?;
        }
        Ok(leased)
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
        self.resolve_revoke(&key, event).await
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

    /// Recover this run's leases after a restart, from the durable store alone (no
    /// Vault call for the adopt path). Scans the **current incarnation**:
    /// - adopts the newest `Active` record as the serving lease;
    /// - revokes any older `Active` duplicates (a crash mid-reissue can leave two);
    /// - revokes `Pending` records - a `Pending` lease's material was never handed out
    ///   (it becomes `Active` only after it is durably owned), so its Vault lease is a
    ///   stray to clean up;
    /// - re-drives `RevokePending` records to completion (a crash mid-revoke).
    ///
    /// `Orphaned`/`Revoked` records are counted and left in place.
    pub(crate) async fn recover(&mut self) -> Result<RecoverySummary> {
        let mut summary = RecoverySummary::default();
        let mut actives: Vec<(String, u64, LeaseRecord)> = Vec::new();
        let mut to_revoke: Vec<(String, u64, LeaseRecord)> = Vec::new();

        let mut cursor: Option<String> = None;
        loop {
            let page = self
                .store
                .list_current_incarnation(cursor.as_deref(), RECONCILE_PAGE)
                .await?;
            for (key, version, record) in page.records {
                match record.state {
                    LeaseState::Active => actives.push((key, version, record)),
                    LeaseState::Pending | LeaseState::RevokePending => {
                        to_revoke.push((key, version, record))
                    }
                    LeaseState::Orphaned => summary.orphaned += 1,
                    LeaseState::Revoked => summary.tombstones += 1,
                }
            }
            match page.next_cursor {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }

        // Adopt the newest Active (latest expiry); revoke the rest.
        actives.sort_by_key(|(_, _, r)| r.expires_at_ms);
        if let Some((key, version, _)) = actives.pop() {
            self.active = Some(ActiveLease { key, version });
            summary.adopted = 1;
        }
        for (key, version, record) in actives.into_iter().chain(to_revoke) {
            match self.drive_revoke(key, version, record).await? {
                RevokeOutcome::Finalized => summary.revoked += 1,
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

/// Non-secret counts from a restart recovery scan (for logging/metrics).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RecoverySummary {
    /// Active leases re-adopted as the serving lease (0 or 1).
    pub adopted: u32,
    /// Leases finalized (stray Pending / interrupted RevokePending / Active duplicates).
    pub revoked: u32,
    /// Leases now left `Orphaned` (unauthorized) plus any already `Orphaned`.
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
    fn reissues_at_safety_deadline_even_if_renewable() {
        let h = handle(3600, true, EPOCH_MS);
        // 10s before expiry (< 30s safety margin) -> reissue, not renew.
        assert_eq!(
            lease_tick(t(EPOCH_MS + 3_590_000), &h, &cfg()),
            LeaseTick::Reissue
        );
        // Past true expiry -> still reissue (fail forward to a fresh lease).
        assert_eq!(
            lease_tick(t(EPOCH_MS + 4_000_000), &h, &cfg()),
            LeaseTick::Reissue
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
    }

    impl MockProvider {
        fn renewable() -> Self {
            Self {
                renewable: true,
                ..Default::default()
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
    async fn reissue_adopts_new_and_revokes_old() {
        let provider = Arc::new(MockProvider::renewable());
        let mut m = manager(provider.clone());
        m.issue(t(EPOCH_MS)).await.unwrap();
        let old_key = m.active.as_ref().unwrap().key.clone();

        m.reissue(t(EPOCH_MS + 3_000_000)).await.unwrap();
        let new_key = m.active.as_ref().unwrap().key.clone();
        assert_ne!(old_key, new_key, "a fresh generation is adopted");

        // New lease is Active; old lease is the retained Revoked tombstone.
        let (_, new_rec) = m.store.get(&new_key).await.unwrap().unwrap();
        assert_eq!(new_rec.state, LeaseState::Active);
        let (_, old_rec) = m.store.get(&old_key).await.unwrap().unwrap();
        assert_eq!(old_rec.state, LeaseState::Revoked);
        assert_eq!(provider.revokes.lock().unwrap().len(), 1);
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
    async fn recover_adopts_active_and_cleans_stray_pending() {
        let backend: storage::ArcStorageBackend =
            Arc::new(MemoryStorageBackend::new());
        let provider = Arc::new(MockProvider::renewable());

        // First run: issue an Active lease, then crash leaving a stray Pending.
        let mut m1 = manager_on(provider.clone(), backend.clone(), identity());
        m1.issue(t(EPOCH_MS)).await.unwrap();
        let active_key = m1.active.as_ref().unwrap().key.clone();
        seed_pending(&m1, "inc-1", "stray-gen", "stray-lease").await;

        // Second run over the same store: recover.
        let mut m2 = manager_on(provider.clone(), backend.clone(), identity());
        let summary = m2.recover().await.unwrap();

        assert_eq!(summary.adopted, 1);
        assert_eq!(summary.revoked, 1, "the stray Pending is finalized");
        // The recovered Active is adopted and still Active.
        assert_eq!(m2.active.as_ref().unwrap().key, active_key);
        let (_, active) = m2.store.get(&active_key).await.unwrap().unwrap();
        assert_eq!(active.state, LeaseState::Active);
    }

    #[tokio::test]
    async fn recover_does_not_adopt_foreign_incarnation() {
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

        // New incarnation recovers: nothing of its own to adopt.
        let mut new = manager_on(
            provider.clone(),
            backend.clone(),
            identity_with("inc-1"),
        );
        let summary = new.recover().await.unwrap();
        assert_eq!(summary.adopted, 0);
        assert!(new.active.is_none());
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

    // helper for private-field assertions
    fn handle_expiry(ms: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_millis(ms)
    }
}
