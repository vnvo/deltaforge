//! Vault lease-rotation driver (feature `vault`), Phase 3 bounded vertical.
//!
//! Bridges the Phase-2 [`LeaseManager`] (Vault-side lease lifecycle) to the existing
//! credential [`RotationCoordinator`](crate::rotation) (DB-side reconnect at a frozen
//! boundary). The driver:
//!
//! - runs the lease schedule ([`lease_tick`]): **renew** extends the same lease (no DSN
//!   change, no reconnect); **reissue** mints a new lease, composes its DSN, and publishes
//!   a [`Candidate`] so the coordinator reconnects;
//! - consumes the reconnect's [`ApplyOutcome`] (with the coordinator's own `retry_pending`)
//!   and drives the lease revoke decision: promote-new/revoke-old on `Applied`, revoke-new
//!   on terminal/close-uncertain/failed, retain-both (renewing both) on a transient retry;
//! - fails closed by latching a typed [`LeaseFatalReason`] into a `watch` the PG/MySQL run
//!   loops select on, when the active lease truly expires or a post-apply promotion fails.
//!
//! It reuses the pure [`compose`] / DSN-injection machinery; it adds no second composer.
//! Renewal is cancellation-owned and bounded (one Vault call per lease per wake).

use std::time::{Duration, SystemTime};

use anyhow::Result;
use secrets::CredentialSet;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

use crate::credentials::ProtectedDsn;
use crate::rotation::{ApplyOutcome, Candidate, RotationComposition, compose};
use crate::rotation_manager::is_transient;
use crate::vault_lease_manager::{
    LeaseManager, LeaseScheduleConfig, LeaseTick, lease_tick,
};

/// A typed, redacted reason the leased-credential lifecycle stopped the source. Carries
/// no lease id or credential material; latched into a `watch` and selected by the idle
/// PG/MySQL run loops, which convert it to a fail-closed stop without advancing the
/// checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LeaseFatalReason {
    /// The active (serving) lease reached true expiry with no applied replacement.
    ActiveLeaseExpired,
    /// A replacement was applied to the DB stream, but its durable lease ownership could
    /// not be promoted. Streaming on a credential whose ownership is not recorded is not
    /// allowed.
    PromotionFailedAfterApply,
    /// The durable lease store failed in a way that prevents safe scheduling.
    StoreFailure,
}

impl LeaseFatalReason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            LeaseFatalReason::ActiveLeaseExpired => "active_lease_expired",
            LeaseFatalReason::PromotionFailedAfterApply => {
                "promotion_failed_after_apply"
            }
            LeaseFatalReason::StoreFailure => "store_failure",
        }
    }
}

/// One reconnect outcome fed back from the rotation coordinator for the generation the
/// driver published. `retry_pending` is the coordinator's own decision (whether it will
/// re-hand this generation), never inferred by the driver.
#[derive(Debug)]
pub(crate) struct ApplyFeedback {
    pub generation: u64,
    pub outcome: ApplyOutcome,
    pub retry_pending: bool,
}

/// Drives leased-credential rotation for one source.
pub(crate) struct LeaseRotationDriver {
    manager: LeaseManager,
    composition: RotationComposition,
    cfg: LeaseScheduleConfig,
    tx_candidate: watch::Sender<Option<Candidate>>,
    rx_feedback: mpsc::Receiver<ApplyFeedback>,
    tx_fatal: watch::Sender<Option<LeaseFatalReason>>,
    /// Monotonic candidate generation (driver-local, mirrors `spawn_vault_manager`).
    generation: u64,
    /// The generation of the reissue currently awaiting an apply outcome, if any.
    inflight: Option<u64>,
}

impl LeaseRotationDriver {
    pub(crate) fn new(
        manager: LeaseManager,
        composition: RotationComposition,
        cfg: LeaseScheduleConfig,
        tx_candidate: watch::Sender<Option<Candidate>>,
        rx_feedback: mpsc::Receiver<ApplyFeedback>,
        tx_fatal: watch::Sender<Option<LeaseFatalReason>>,
    ) -> Self {
        Self {
            manager,
            composition,
            cfg,
            tx_candidate,
            rx_feedback,
            tx_fatal,
            generation: 0,
            inflight: None,
        }
    }

    /// The cancellation-owned run loop. Prioritizes feedback (so the bounded outcome
    /// channel never blocks the rotation path for long) and cancellation over the
    /// schedule timer.
    pub(crate) async fn run(mut self, cancel: CancellationToken) {
        loop {
            if self.tx_fatal.borrow().is_some() {
                break; // already fatal
            }
            let now = SystemTime::now();
            let tick = match self.active_tick(now).await {
                Ok(t) => t,
                Err(e) => {
                    error!(error = %e, "lease schedule evaluation failed");
                    self.latch_fatal(LeaseFatalReason::StoreFailure);
                    break;
                }
            };
            let wait = match tick {
                LeaseTick::Wait(d) => d,
                _ => Duration::ZERO,
            };
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                fb = self.rx_feedback.recv() => match fb {
                    Some(fb) => self.handle_feedback(fb).await,
                    None => break, // rotation path gone
                },
                _ = tokio::time::sleep(wait) => {
                    self.act_on_tick(tick, SystemTime::now()).await;
                }
            }
        }
    }

    /// Evaluate the schedule for the active lease. While a reissue is in flight, never
    /// start another and never busy-spin: collapse an actionable tick to a bounded wait
    /// (on which the driver renews both leases), but preserve a terminal `Expired` so an
    /// active-lease expiry stays fatal even mid-reissue.
    async fn active_tick(&self, now: SystemTime) -> Result<LeaseTick> {
        let base = match self.manager.active_handle().await? {
            Some(h) => lease_tick(now, &h, &self.cfg),
            None => LeaseTick::Wait(self.cfg.min_poll),
        };
        if self.inflight.is_some() && base != LeaseTick::Expired {
            return Ok(LeaseTick::Wait(self.cfg.min_poll));
        }
        Ok(base)
    }

    async fn act_on_tick(&mut self, tick: LeaseTick, now: SystemTime) {
        match tick {
            LeaseTick::Wait(_) => {
                // While a reissue is retained, keep both leases alive across the retry
                // window.
                if self.inflight.is_some() {
                    self.do_renew(now).await;
                }
            }
            LeaseTick::Renew => self.do_renew(now).await,
            LeaseTick::Reissue => {
                if self.inflight.is_some() {
                    self.do_renew(now).await;
                } else {
                    self.do_reissue(now).await;
                }
            }
            LeaseTick::Expired => {
                error!("active lease expired with no applied replacement");
                self.latch_fatal(LeaseFatalReason::ActiveLeaseExpired);
            }
        }
        // Best-effort drain of any revoke staged by an earlier transient failure.
        if self.manager.has_revoking() {
            if let Err(e) = self.manager.retry_revoking().await {
                warn!(error = %e, "staged lease revoke retry deferred");
            }
        }
    }

    /// Renew the active lease, and the retained pending replacement when one exists
    /// (RetainBoth). Bounded: one Vault renew per lease. Failures are non-fatal here; a
    /// lease that cannot be renewed will reach `Expired` and fail closed.
    async fn do_renew(&mut self, now: SystemTime) {
        if let Err(e) = self.manager.renew(now).await {
            warn!(error = %e, "active lease renew failed");
        }
        if self.manager.has_pending() {
            if let Err(e) = self.manager.renew_pending(now).await {
                warn!(error = %e, "pending lease renew failed");
            }
        }
    }

    /// Mint a replacement lease, compose its DSN, and publish a candidate for the
    /// coordinator to apply. Non-fatal on failure (retried on a later tick); the active
    /// lease keeps serving until it expires.
    async fn do_reissue(&mut self, now: SystemTime) {
        if self.manager.has_pending() || self.manager.has_revoking() {
            return; // a reissue/revoke is already occupying its slot
        }
        let leased = match self.manager.reissue(now).await {
            Ok(l) => l,
            Err(e) => {
                warn!(error = %e, "lease reissue failed; will retry");
                return;
            }
        };
        let expires_at = leased.lease.expires_at;
        let dsn = match compose(&leased.credentials, &self.composition) {
            Ok(d) => d,
            Err(e) => {
                // Composition of freshly issued credentials should not fail; if it does,
                // the pending lease is unusable - revoke it (keep the old serving).
                error!(error = %e, "composing reissued DSN failed; revoking pending");
                let _ = self
                    .manager
                    .resolve_reissue(ApplyOutcome::FailedClosed, false)
                    .await;
                return;
            }
        };
        self.generation += 1;
        let candidate =
            Candidate::new(self.generation, dsn, now, Some(expires_at));
        let _ = self.tx_candidate.send(Some(candidate));
        self.inflight = Some(self.generation);
    }

    /// Apply one reconnect outcome to the lease lifecycle.
    async fn handle_feedback(&mut self, fb: ApplyFeedback) {
        if self.inflight != Some(fb.generation) {
            return; // stale / not the in-flight reissue
        }
        match fb.outcome {
            ApplyOutcome::Applied => {
                // DB stream already switched to the new credential. Promote durable
                // ownership; a failure here is fatal (must not stream on an unowned
                // credential). A post-promotion old-lease revoke failure is retryable.
                match self.manager.promote_pending().await {
                    Ok(()) => {
                        self.inflight = None;
                        if let Err(e) = self.manager.retry_revoking().await {
                            warn!(error = %e, "old lease revoke deferred after apply");
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "durable lease promotion failed after apply");
                        self.latch_fatal(
                            LeaseFatalReason::PromotionFailedAfterApply,
                        );
                    }
                }
            }
            ApplyOutcome::KeptOld { reason } => {
                if is_transient(reason) && fb.retry_pending {
                    // Retain both; the coordinator will re-hand this generation. Keep
                    // `inflight` set; both leases are renewed on later ticks.
                    if let Err(e) = self
                        .manager
                        .resolve_reissue(ApplyOutcome::KeptOld { reason }, true)
                        .await
                    {
                        warn!(error = %e, "retain-both resolve failed");
                    }
                } else {
                    // Terminal (including an expired pending candidate): revoke the new
                    // lease, keep the old serving. The driver may issue another
                    // replacement on a later tick if the active lease is still safe.
                    self.resolve_and_clear(ApplyOutcome::KeptOld { reason })
                        .await;
                }
            }
            outcome @ (ApplyOutcome::CloseUncertain
            | ApplyOutcome::FailedClosed) => {
                // No successful new stream exists: revoke the unused new lease, retain the
                // old lease record for recovery. The source loop stops fail-closed on
                // these outcomes via its own Err path; the driver does not latch its own
                // fatal here.
                self.resolve_and_clear(outcome).await;
            }
        }
    }

    /// Resolve a non-adopted outcome (revoke the new lease, keep the old) and clear the
    /// in-flight marker. A revoke failure leaves the lease staged for `retry_revoking`.
    async fn resolve_and_clear(&mut self, outcome: ApplyOutcome) {
        if let Err(e) = self.manager.resolve_reissue(outcome, false).await {
            warn!(error = %e, "revoking superseded/rejected lease deferred");
        }
        self.inflight = None;
    }

    fn latch_fatal(&self, reason: LeaseFatalReason) {
        self.tx_fatal.send_if_modified(|cur| {
            if cur.is_none() {
                *cur = Some(reason);
                true
            } else {
                false
            }
        });
    }
}

/// Compose the initial source DSN from a leased credential set, reusing the pure
/// rotation composition. Used by startup after the first `issue`.
pub(crate) fn compose_leased_dsn(
    credentials: &CredentialSet,
    composition: &RotationComposition,
) -> Result<ProtectedDsn> {
    compose(credentials, composition)
        .map_err(|e| anyhow::anyhow!("compose leased dsn: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::UNIX_EPOCH;

    use async_trait::async_trait;
    use secrets::{LeaseInfo, LeasedRead, SecretError};
    use storage::MemoryStorageBackend;

    use crate::rotation::{DbKind, FieldSource, RotationReject};
    use crate::vault_lease_manager::{LeaseIdentity, LeaseProvider};

    const EPOCH_MS: u64 = 1_700_000_000_000;
    fn t(ms: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_millis(ms)
    }

    // A mock lease provider yielding distinct dynamic users each issue.
    #[derive(Default)]
    struct MockProvider {
        issued: Mutex<u64>,
        revokes: Mutex<u64>,
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
            let user = format!("dyn-{n}");
            let reference = secrets::SecretReference::new(
                secrets::SecretProvider::Vault,
                "database/creds/orders-ro",
            );
            let mut credentials = secrets::CredentialSet::new();
            for (k, v) in [("username", user.as_str()), ("password", "pw")] {
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
                renewable: true,
            })
        }
        async fn renew(
            &self,
            _lease_id: &str,
            _increment_secs: Option<u64>,
        ) -> Result<LeaseInfo, SecretError> {
            Ok(LeaseInfo {
                lease_duration: Duration::from_secs(3600),
                renewable: true,
            })
        }
        async fn revoke(&self, _lease_id: &str) -> Result<(), SecretError> {
            *self.revokes.lock().unwrap() += 1;
            Ok(())
        }
    }

    fn cfg() -> LeaseScheduleConfig {
        LeaseScheduleConfig {
            safety_margin: Duration::from_secs(5),
            renew_fraction: 0.5,
            min_poll: Duration::from_secs(1),
            max_poll: Duration::from_secs(30),
        }
    }

    fn composition() -> RotationComposition {
        RotationComposition::UsernamePassword {
            db: DbKind::Postgres,
            base_dsn: "host=127.0.0.1 port=5432 dbname=app".to_string(),
            username: FieldSource::Watched("username".to_string()),
            password: FieldSource::Watched("password".to_string()),
        }
    }

    struct Harness {
        driver: LeaseRotationDriver,
        rx_candidate: watch::Receiver<Option<Candidate>>,
        tx_feedback: mpsc::Sender<ApplyFeedback>,
        rx_fatal: watch::Receiver<Option<LeaseFatalReason>>,
    }

    async fn harness(provider: Arc<MockProvider>) -> Harness {
        let backend: storage::ArcStorageBackend =
            Arc::new(MemoryStorageBackend::new());
        let id = LeaseIdentity {
            tenant: "acme".into(),
            pipeline: "pipe".into(),
            source: "pg1".into(),
            credential_purpose: "source-db".into(),
            incarnation: "inc-1".into(),
        };
        let mut manager = LeaseManager::new(
            provider,
            backend,
            id,
            "database",
            "orders-ro",
            cfg(),
        );
        manager.issue(t(EPOCH_MS)).await.unwrap();

        let (tx_candidate, rx_candidate) = watch::channel(None);
        let (tx_feedback, rx_feedback) = mpsc::channel(4);
        let (tx_fatal, rx_fatal) = watch::channel(None);
        let driver = LeaseRotationDriver::new(
            manager,
            composition(),
            cfg(),
            tx_candidate,
            rx_feedback,
            tx_fatal,
        );
        Harness {
            driver,
            rx_candidate,
            tx_feedback,
            rx_fatal,
        }
    }

    fn published_generation(rx: &watch::Receiver<Option<Candidate>>) -> u64 {
        rx.borrow().as_ref().expect("a candidate").generation
    }

    #[tokio::test]
    async fn reissue_publishes_candidate_with_expiry() {
        let mut h = harness(Arc::new(MockProvider::default())).await;
        h.driver.do_reissue(t(EPOCH_MS + 3_000_000)).await;
        let cand = h.rx_candidate.borrow().clone().expect("candidate");
        assert_eq!(cand.generation, 1);
        assert!(cand.expires_at.is_some(), "leased candidate carries expiry");
        assert_eq!(h.driver.inflight, Some(1));
    }

    #[tokio::test]
    async fn applied_promotes_and_revokes_old() {
        let provider = Arc::new(MockProvider::default());
        let mut h = harness(provider.clone()).await;
        h.driver.do_reissue(t(EPOCH_MS + 3_000_000)).await;
        let gen_pub = published_generation(&h.rx_candidate);
        h.driver
            .handle_feedback(ApplyFeedback {
                generation: gen_pub,
                outcome: ApplyOutcome::Applied,
                retry_pending: false,
            })
            .await;
        assert_eq!(h.driver.inflight, None);
        // Old lease revoked (one revoke call), new lease is active.
        assert_eq!(*provider.revokes.lock().unwrap(), 1);
        assert!(h.rx_fatal.borrow().is_none());
        let status = h.driver.manager.status().await.unwrap();
        assert_eq!(status.state, "active");
    }

    #[tokio::test]
    async fn kept_old_terminal_revokes_new_keeps_old() {
        let provider = Arc::new(MockProvider::default());
        let mut h = harness(provider.clone()).await;
        h.driver.do_reissue(t(EPOCH_MS + 3_000_000)).await;
        let gen_pub = published_generation(&h.rx_candidate);
        h.driver
            .handle_feedback(ApplyFeedback {
                generation: gen_pub,
                outcome: ApplyOutcome::KeptOld {
                    reason: RotationReject::IdentityMismatch,
                },
                retry_pending: false,
            })
            .await;
        assert_eq!(h.driver.inflight, None);
        assert_eq!(*provider.revokes.lock().unwrap(), 1, "new lease revoked");
        assert!(h.rx_fatal.borrow().is_none());
    }

    #[tokio::test]
    async fn expired_candidate_revokes_new_and_allows_another_reissue() {
        let provider = Arc::new(MockProvider::default());
        let mut h = harness(provider.clone()).await;
        h.driver.do_reissue(t(EPOCH_MS + 3_000_000)).await;
        let gen_pub = published_generation(&h.rx_candidate);
        // Stage-B recheck reports the pending candidate expired -> terminal.
        h.driver
            .handle_feedback(ApplyFeedback {
                generation: gen_pub,
                outcome: ApplyOutcome::KeptOld {
                    reason: RotationReject::Expired,
                },
                retry_pending: false,
            })
            .await;
        assert_eq!(h.driver.inflight, None);
        assert_eq!(*provider.revokes.lock().unwrap(), 1);
        // The active lease is still safe, so another reissue is allowed.
        h.driver.do_reissue(t(EPOCH_MS + 3_100_000)).await;
        assert_eq!(h.driver.inflight, Some(2));
    }

    #[tokio::test]
    async fn transient_retry_retains_both() {
        let provider = Arc::new(MockProvider::default());
        let mut h = harness(provider.clone()).await;
        h.driver.do_reissue(t(EPOCH_MS + 3_000_000)).await;
        let gen_pub = published_generation(&h.rx_candidate);
        h.driver
            .handle_feedback(ApplyFeedback {
                generation: gen_pub,
                outcome: ApplyOutcome::KeptOld {
                    reason: RotationReject::PreflightFailed,
                },
                retry_pending: true,
            })
            .await;
        // Retain both: inflight kept, nothing revoked yet.
        assert_eq!(h.driver.inflight, Some(gen_pub));
        assert_eq!(*provider.revokes.lock().unwrap(), 0);
        assert!(h.driver.manager.has_pending());
    }

    #[tokio::test]
    async fn close_uncertain_revokes_new_no_driver_fatal() {
        let provider = Arc::new(MockProvider::default());
        let mut h = harness(provider.clone()).await;
        h.driver.do_reissue(t(EPOCH_MS + 3_000_000)).await;
        let gen_pub = published_generation(&h.rx_candidate);
        h.driver
            .handle_feedback(ApplyFeedback {
                generation: gen_pub,
                outcome: ApplyOutcome::CloseUncertain,
                retry_pending: false,
            })
            .await;
        assert_eq!(h.driver.inflight, None);
        assert_eq!(*provider.revokes.lock().unwrap(), 1, "unused new revoked");
        // The driver does not latch its own fatal here (the source loop stops via its Err
        // path); the old lease record is retained.
        assert!(h.rx_fatal.borrow().is_none());
    }

    #[tokio::test]
    async fn active_expiry_is_fatal() {
        let mut h = harness(Arc::new(MockProvider::default())).await;
        // Past true expiry of the active lease (issued at EPOCH, 3600s TTL).
        h.driver
            .act_on_tick(LeaseTick::Expired, t(EPOCH_MS + 4_000_000))
            .await;
        assert_eq!(
            *h.rx_fatal.borrow(),
            Some(LeaseFatalReason::ActiveLeaseExpired)
        );
    }

    #[tokio::test]
    async fn while_inflight_tick_collapses_to_wait_and_renews_both() {
        let provider = Arc::new(MockProvider::default());
        let mut h = harness(provider).await;
        h.driver.do_reissue(t(EPOCH_MS + 3_000_000)).await;
        // With a reissue in flight, an otherwise-actionable schedule collapses to Wait.
        let tick = h.driver.active_tick(t(EPOCH_MS + 3_550_000)).await.unwrap();
        assert!(matches!(tick, LeaseTick::Wait(_)));
        // But a true active expiry still surfaces as fatal even mid-reissue.
        let tick = h.driver.active_tick(t(EPOCH_MS + 4_000_000)).await.unwrap();
        assert_eq!(tick, LeaseTick::Expired);
    }

    #[tokio::test]
    async fn compose_leased_dsn_injects_credentials() {
        let provider = MockProvider::default();
        // Build a set via the provider and compose.
        let read = provider.issue("database", "orders-ro").await.unwrap();
        let dsn =
            compose_leased_dsn(&read.credentials, &composition()).unwrap();
        assert!(dsn.expose().contains("dyn-1"));
        assert!(dsn.expose().contains("pw"));
    }
}
