//! Source-agnostic core of controlled credential-rotation reconnect.
//!
//! This module holds the safety-critical, side-effect-free logic that the PG and
//! MySQL run loops drive; the per-source control-connection preflight and stream
//! open/close live with each loop (later increments). The pieces here are:
//!
//! - [`compose`] / [`earliest_expiry`]: build **one** `ProtectedDsn` from the file
//!   material the [`FileWatcher`](secrets::FileWatcher) already validated inside its
//!   pre/post fingerprint bracket, composed with immutable env fields resolved once,
//!   and compute the candidate's expiry as the earliest expiry among participating
//!   fields (immutable env/file material yields `None`). This reads already-validated
//!   values only - it never re-resolves.
//! - [`RotationCoordinator`]: consumes the `tokio::sync::watch` channel of
//!   [`Candidate`]s and enforces monotonic-generation supersession, expiry
//!   ineligibility, **terminal vs. transient** rejection (terminal permanently
//!   suppresses a generation; transient keeps it for bounded backoff retries until
//!   superseded, expired, or past a retry deadline), and in-flight de-duplication.
//! - [`apply_two_stage`]: the Stage-B sequence - recheck the frozen boundary
//!   position **before** closing the old stream, then close and open the
//!   replacement, with bounded recovery on the old credentials and a fail-closed
//!   result when neither reconnects. Each step is wrapped in an internal timeout so
//!   the whole sequence is bounded; critically, once `close_old` runs, an
//!   `open_new` timeout/failure always proceeds to `open_old` recovery so the source
//!   is never left unintentionally disconnected.
//!
//! No secret value ever appears in a reject reason, outcome, or `Debug` here.

use std::future::Future;
use std::time::{Duration, Instant, SystemTime};

use secrets::{CredentialSet, SecretString};
use tokio::sync::watch;

use crate::credentials::{CredentialError, ProtectedDsn};

/// Which database the base DSN is for (governs credential-injection form).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbKind {
    Postgres,
    Mysql,
}

/// Where a credential field's value comes from at composition time.
pub enum FieldSource {
    /// A file-backed field resolved (and validated) by the watcher, by set name.
    Watched(String),
    /// An immutable value resolved once at startup (e.g. an env var). Held in a
    /// zeroize-on-drop [`SecretString`]; never logged. Immutable material never
    /// expires, so it contributes `None` to the candidate expiry.
    Fixed(SecretString),
}

/// How to assemble a rotated `ProtectedDsn` from the watcher's validated set.
pub enum RotationComposition {
    /// The entire DSN is one watched file field (a `dsn_secret`).
    WholeDsn { field: String },
    /// A non-secret base DSN plus username/password, each watched or fixed.
    UsernamePassword {
        db: DbKind,
        base_dsn: String,
        username: FieldSource,
        password: FieldSource,
    },
}

impl std::fmt::Debug for RotationComposition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never render field values (Fixed holds a secret).
        match self {
            Self::WholeDsn { field } => {
                f.debug_struct("WholeDsn").field("field", field).finish()
            }
            Self::UsernamePassword { db, .. } => f
                .debug_struct("UsernamePassword")
                .field("db", db)
                .finish_non_exhaustive(),
        }
    }
}

fn field_value<'a>(
    set: &'a CredentialSet,
    src: &'a FieldSource,
    which: &'static str,
) -> Result<&'a str, CredentialError> {
    match src {
        FieldSource::Watched(name) => set
            .require(name)?
            .material()
            .as_utf8()
            .ok_or(CredentialError::NotUtf8(which)),
        FieldSource::Fixed(s) => Ok(s.expose_secret()),
    }
}

/// Compose one `ProtectedDsn` from the watcher's already-validated `set` (plus any
/// fixed env fields). Pure: reads validated values, performs string injection, and
/// wraps the result. No filesystem or provider access.
pub fn compose(
    set: &CredentialSet,
    comp: &RotationComposition,
) -> Result<ProtectedDsn, CredentialError> {
    match comp {
        RotationComposition::WholeDsn { field } => {
            let dsn = set
                .require(field)?
                .material()
                .as_utf8()
                .ok_or(CredentialError::NotUtf8("dsn"))?;
            Ok(ProtectedDsn::from(dsn))
        }
        RotationComposition::UsernamePassword {
            db,
            base_dsn,
            username,
            password,
        } => {
            let user = field_value(set, username, "username")?;
            let pass = field_value(set, password, "password")?;
            let dsn = match db {
                DbKind::Postgres => {
                    if base_dsn.starts_with("postgres://")
                        || base_dsn.starts_with("postgresql://")
                    {
                        common::dsn::inject_url_credentials(
                            base_dsn, user, pass,
                        )
                    } else {
                        common::dsn::replace_libpq_credentials(
                            base_dsn, user, pass,
                        )
                    }
                }
                DbKind::Mysql => {
                    common::dsn::inject_url_credentials(base_dsn, user, pass)
                }
            }
            .map_err(|_| CredentialError::DsnParse)?;
            Ok(ProtectedDsn::from(dsn))
        }
    }
}

/// The earliest expiry among the fields participating in `comp` (a field that never
/// expires contributes nothing). Immutable env/file material yields `None`, so
/// today's env/file candidates are non-expiring; the field is wired for future
/// dynamic (e.g. Vault) credentials that report `expires_at`.
pub fn earliest_expiry(
    set: &CredentialSet,
    comp: &RotationComposition,
) -> Option<SystemTime> {
    let watched = |src: &FieldSource| -> Option<SystemTime> {
        match src {
            FieldSource::Watched(name) => {
                set.get(name).and_then(|r| r.expires_at())
            }
            FieldSource::Fixed(_) => None,
        }
    };
    match comp {
        RotationComposition::WholeDsn { field } => {
            set.get(field).and_then(|r| r.expires_at())
        }
        RotationComposition::UsernamePassword {
            username, password, ..
        } => earliest([watched(username), watched(password)]),
    }
}

/// The earliest present expiry, or `None` if none of the inputs expire.
fn earliest(
    times: impl IntoIterator<Item = Option<SystemTime>>,
) -> Option<SystemTime> {
    times.into_iter().flatten().min()
}

/// A resolver-validated (not yet authenticated) rotation candidate.
#[derive(Clone)]
pub struct Candidate {
    /// Monotonic generation from the watcher; strictly increases per validated set.
    pub generation: u64,
    /// The composed replacement DSN (redacted; authentication happens at preflight).
    pub dsn: ProtectedDsn,
    /// When this candidate was composed (for age metrics / diagnostics).
    pub resolved_at: SystemTime,
    /// The earliest expiry among participating credential fields; `None` for
    /// immutable env/file material.
    pub expires_at: Option<SystemTime>,
}

impl Candidate {
    pub fn new(
        generation: u64,
        dsn: ProtectedDsn,
        resolved_at: SystemTime,
        expires_at: Option<SystemTime>,
    ) -> Self {
        Self {
            generation,
            dsn,
            resolved_at,
            expires_at,
        }
    }

    /// Whether this candidate is already expired at `wall_now`.
    pub fn is_expired(&self, wall_now: SystemTime) -> bool {
        self.expires_at.is_some_and(|exp| wall_now >= exp)
    }
}

impl std::fmt::Debug for Candidate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Candidate")
            .field("generation", &self.generation)
            .field("expires_at", &self.expires_at)
            .finish_non_exhaustive()
    }
}

/// Backoff schedule for transient (retryable) preflight/open failures.
#[derive(Debug, Clone, Copy)]
pub struct RetryConfig {
    pub base: Duration,
    pub max: Duration,
    /// Total time a single candidate generation may be retried before it is given
    /// up (suppressed as terminal).
    pub deadline: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            base: Duration::from_secs(1),
            max: Duration::from_secs(30),
            deadline: Duration::from_secs(300),
        }
    }
}

struct RetryState {
    generation: u64,
    attempts: u32,
    next_at: Instant,
    deadline_at: Instant,
}

/// Consumes the latest-candidate `watch` channel and gates application.
pub struct RotationCoordinator {
    rx: watch::Receiver<Option<Candidate>>,
    retry_cfg: RetryConfig,
    /// Highest generation permanently suppressed (terminal reject / applied / gave
    /// up). Nothing at or below this is ever handed out again.
    floor: u64,
    /// Generation currently handed out and awaiting a result.
    in_flight: Option<u64>,
    /// Transient-retry state for the current generation, if any.
    retry: Option<RetryState>,
}

impl RotationCoordinator {
    pub fn new(
        rx: watch::Receiver<Option<Candidate>>,
        retry_cfg: RetryConfig,
    ) -> Self {
        Self {
            rx,
            retry_cfg,
            floor: 0,
            in_flight: None,
            retry: None,
        }
    }

    /// Await the next channel change (for the run loop's `select!`). Returns `false`
    /// when the sender is gone.
    pub async fn changed(&mut self) -> bool {
        self.rx.changed().await.is_ok()
    }

    /// Whether `generation` is still the latest published candidate. A preflight
    /// result is accepted only while this holds, so an older completion never
    /// replaces a newer pending candidate.
    pub fn is_current(&self, generation: u64) -> bool {
        self.rx
            .borrow()
            .as_ref()
            .is_some_and(|c| c.generation == generation)
    }

    /// The generation currently handed out and awaiting a result, if any.
    pub fn in_flight(&self) -> Option<u64> {
        self.in_flight
    }

    /// Decide what the run loop should do with the latest candidate. Enforces, in
    /// order: terminal floor, in-flight de-duplication, supersession (a newer
    /// generation abandons an older in-flight one so the loop can cancel its
    /// preflight and take the newer), expiry (distinct, observable), and transient
    /// backoff/deadline.
    pub fn take_eligible(
        &mut self,
        now: Instant,
        wall_now: SystemTime,
    ) -> Eligibility {
        let cand = match self.rx.borrow().clone() {
            Some(c) => c,
            None => return Eligibility::Idle,
        };
        let generation = cand.generation;

        if generation <= self.floor {
            return Eligibility::Idle; // superseded, applied, or suppressed
        }
        match self.in_flight {
            Some(inf) if inf == generation => return Eligibility::Idle, // in flight
            Some(inf) if generation > inf => {
                // A newer generation supersedes the in-flight one: abandon it so the
                // loop can cancel its preflight and process the newer candidate. A
                // late result for the abandoned generation is ignored (guarded
                // record_* below).
                self.in_flight = None;
                self.retry = None;
            }
            _ => {}
        }

        // Expiry is terminal for this generation: suppress it and report it so the
        // wiring can alarm and enforce the active-credential expiry policy.
        if cand.is_expired(wall_now) {
            self.floor = self.floor.max(generation);
            self.retry = None;
            return Eligibility::Expired { generation };
        }

        match &self.retry {
            Some(r) if r.generation == generation => {
                if now < r.next_at {
                    return Eligibility::Idle; // backoff window not elapsed
                }
                if now >= r.deadline_at {
                    self.floor = self.floor.max(generation); // gave up
                    self.retry = None;
                    return Eligibility::Idle;
                }
                // Backoff elapsed and within deadline: eligible for another attempt.
            }
            Some(_) => {
                // Retry state was for an older generation; a newer one obsoletes it.
                self.retry = None;
            }
            None => {}
        }

        self.in_flight = Some(generation);
        Eligibility::Ready(cand)
    }

    /// The candidate was applied. Ignored (`Stale`) unless `generation` is still the
    /// one in flight, so a delayed result never clears a newer in-flight generation.
    #[must_use]
    pub fn record_applied(&mut self, generation: u64) -> RecordOutcome {
        if self.in_flight != Some(generation) {
            return RecordOutcome::Stale;
        }
        self.floor = self.floor.max(generation);
        self.in_flight = None;
        self.retry = None;
        RecordOutcome::Recorded
    }

    /// The candidate failed terminally (identity/position/malformed/expired):
    /// permanently suppress it. Ignored (`Stale`) unless it is still in flight.
    #[must_use]
    pub fn record_terminal_rejection(
        &mut self,
        generation: u64,
    ) -> RecordOutcome {
        if self.in_flight != Some(generation) {
            return RecordOutcome::Stale;
        }
        self.floor = self.floor.max(generation);
        self.in_flight = None;
        self.retry = None;
        RecordOutcome::Recorded
    }

    /// The candidate failed transiently (control-connection outage, replacement
    /// open failure): keep it for a bounded backoff retry. Ignored (`Stale`) unless
    /// it is still in flight, so a delayed failure never clears a newer in-flight
    /// generation. If it is in flight but has since been superseded in the channel,
    /// the in-flight marker is cleared but no retry is scheduled.
    #[must_use]
    pub fn record_transient_failure(
        &mut self,
        generation: u64,
        now: Instant,
    ) -> RecordOutcome {
        if self.in_flight != Some(generation) {
            return RecordOutcome::Stale;
        }
        self.in_flight = None;
        if !self.is_current(generation) || generation <= self.floor {
            return RecordOutcome::Recorded; // superseded: no retry scheduled
        }
        let (attempts, deadline_at) = match &self.retry {
            Some(r) if r.generation == generation => {
                (r.attempts + 1, r.deadline_at)
            }
            _ => (1, now + self.retry_cfg.deadline),
        };
        let backoff = self
            .retry_cfg
            .base
            .saturating_mul(1u32 << (attempts - 1).min(16))
            .min(self.retry_cfg.max);
        self.retry = Some(RetryState {
            generation,
            attempts,
            next_at: now + backoff,
            deadline_at,
        });
        RecordOutcome::Recorded
    }
}

/// Whether a reported rotation result was applied to coordinator state or ignored
/// because it referred to a generation no longer in flight.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordOutcome {
    Recorded,
    Stale,
}

/// What the run loop should do with the latest candidate this tick.
#[derive(Debug)]
pub enum Eligibility {
    /// Nothing to do (no candidate, already handled/superseded, in flight, or the
    /// backoff window has not elapsed).
    Idle,
    /// Hand this candidate to preflight/apply; it is now marked in flight.
    Ready(Candidate),
    /// The latest candidate is already expired and has been suppressed. The loop
    /// must emit a redacted alarm and enforce the active-credential expiry policy
    /// (fail closed if the active credential is itself expired with no replacement).
    Expired { generation: u64 },
}

/// A typed, redacted reason a rotation was not applied. Carries no secret value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RotationReject {
    /// Immutable source identity did not match on the replacement (wrong server or
    /// database).
    IdentityMismatch,
    /// The frozen boundary position is not reachable on the replacement.
    PositionIncompatible,
    /// Authentication/connect/recheck failed or timed out at preflight.
    PreflightFailed,
    /// The credential is expired or within its unsafe margin.
    Expired,
    /// The replacement stream failed to open at the boundary (old creds recovered).
    ReplacementOpenFailed,
}

/// Result of the Stage-B apply sequence.
#[derive(Debug, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// Replacement opened; the caller swaps the runtime DSN and continues.
    Applied,
    /// Rotation not applied; the current verified connection is retained (either the
    /// boundary recheck failed before any close, or the old stream was reopened
    /// after the replacement failed). Emit a redacted alarm.
    KeptOld { reason: RotationReject },
    /// `close_old` did not **confirm** closure (it errored or timed out), so the old
    /// stream's state is uncertain. Neither `open_new` nor `open_old` was invoked -
    /// opening a second stream from an uncertain state could run two consumers. The
    /// caller must **stop fail-closed without advancing the checkpoint** and let
    /// source teardown drop the old handle.
    CloseUncertain,
    /// Neither credential set reconnected. The caller must **stop fail-closed
    /// without advancing the checkpoint**.
    FailedClosed,
}

/// The Stage-B two-stage apply, run at a quiesced whole-transaction boundary.
///
/// Order is safety-critical and each step is bounded by `timeout`:
/// 1. `recheck` the **frozen current boundary** on a fresh/authenticated control
///    connection (never the stale preflight snapshot). Failure/timeout leaves the
///    old stream untouched (`KeptOld`).
/// 2. `close_old` and **confirm** the old stream is closed (ideally via a
///    source-specific inactive check). If it errors or times out, the old stream's
///    state is uncertain: return `CloseUncertain` and open **neither** stream.
/// 3. Only after confirmed closure, `open_new` at the frozen position; if it fails
///    or times out, attempt bounded `open_old` recovery; if that also fails/times
///    out, return `FailedClosed`.
///
/// **This future must be awaited to completion, not raced against a shutdown
/// token.** Each step is internally bounded so it completes promptly. There is never
/// a moment with two active replication streams: a second stream is opened only
/// after `close_old` confirms the first is gone.
pub async fn apply_two_stage<RF, CF, NF, OF, EC, EN, EO>(
    timeout: Duration,
    recheck: impl FnOnce() -> RF,
    close_old: impl FnOnce() -> CF,
    open_new: impl FnOnce() -> NF,
    open_old: impl FnOnce() -> OF,
) -> ApplyOutcome
where
    RF: Future<Output = Result<(), RotationReject>>,
    CF: Future<Output = Result<(), EC>>,
    NF: Future<Output = Result<(), EN>>,
    OF: Future<Output = Result<(), EO>>,
{
    // Stage B step 1: recheck the frozen boundary before touching the old stream.
    match tokio::time::timeout(timeout, recheck()).await {
        Ok(Ok(())) => {}
        Ok(Err(reason)) => return ApplyOutcome::KeptOld { reason },
        Err(_timeout) => {
            return ApplyOutcome::KeptOld {
                reason: RotationReject::PreflightFailed,
            };
        }
    }

    // Step 2: close old and CONFIRM closure. If closure is not confirmed (error or
    // timeout), the old stream may still be active -> do not open new or recovery.
    match tokio::time::timeout(timeout, close_old()).await {
        Ok(Ok(())) => {} // confirmed closed
        _ => return ApplyOutcome::CloseUncertain,
    }

    // Step 3: open replacement; any failure/timeout falls through to recovery so we
    // are never left disconnected after close_old.
    let new_ok =
        matches!(tokio::time::timeout(timeout, open_new()).await, Ok(Ok(())));
    if new_ok {
        return ApplyOutcome::Applied;
    }
    match tokio::time::timeout(timeout, open_old()).await {
        Ok(Ok(())) => ApplyOutcome::KeptOld {
            reason: RotationReject::ReplacementOpenFailed,
        },
        _ => ApplyOutcome::FailedClosed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrets::{
        DEFAULT_MAX_SECRET_BYTES, ResolvedSecret, SecretMaterial,
        SecretProvider, SecretReference, SecretString,
    };
    use std::cell::Cell;

    fn sref() -> SecretReference {
        SecretReference::new(SecretProvider::File, "/x")
    }

    fn utf8(v: &str) -> ResolvedSecret {
        ResolvedSecret::new(SecretMaterial::Utf8(
            SecretString::new(v.into(), DEFAULT_MAX_SECRET_BYTES, &sref())
                .unwrap(),
        ))
    }

    fn set_with(fields: &[(&str, &str)]) -> CredentialSet {
        let mut cs = CredentialSet::new();
        for (k, v) in fields {
            cs.insert(*k, utf8(v)).unwrap();
        }
        cs
    }

    fn fixed(v: &str) -> FieldSource {
        FieldSource::Fixed(
            SecretString::new(v.into(), DEFAULT_MAX_SECRET_BYTES, &sref())
                .unwrap(),
        )
    }

    // ---- compose ----

    #[test]
    fn compose_whole_dsn() {
        let set = set_with(&[("dsn", "postgres://u:p@h/db")]);
        let dsn = compose(
            &set,
            &RotationComposition::WholeDsn {
                field: "dsn".into(),
            },
        )
        .unwrap();
        assert_eq!(dsn.expose(), "postgres://u:p@h/db");
    }

    #[test]
    fn compose_pg_url_base_with_watched_fields() {
        let set = set_with(&[("username", "svc"), ("password", "p@ss:w0rd")]);
        let dsn = compose(
            &set,
            &RotationComposition::UsernamePassword {
                db: DbKind::Postgres,
                base_dsn: "postgres://db.internal/orders".into(),
                username: FieldSource::Watched("username".into()),
                password: FieldSource::Watched("password".into()),
            },
        )
        .unwrap();
        let c =
            common::dsn::DsnComponents::from_url(dsn.expose(), 5432).unwrap();
        assert_eq!(c.user, "svc");
        assert_eq!(c.password, "p@ss:w0rd");
    }

    #[test]
    fn compose_pg_keyvalue_base_preserves_options() {
        let set = set_with(&[("username", "svc"), ("password", "p w")]);
        let dsn = compose(
            &set,
            &RotationComposition::UsernamePassword {
                db: DbKind::Postgres,
                base_dsn: "host=db dbname=orders sslmode=require".into(),
                username: FieldSource::Watched("username".into()),
                password: FieldSource::Watched("password".into()),
            },
        )
        .unwrap();
        assert!(dsn.expose().contains("sslmode=require"));
        let c = common::dsn::DsnComponents::from_keyvalue(
            dsn.expose(),
            5432,
            "",
            "",
        );
        assert_eq!(c.password, "p w");
    }

    #[test]
    fn compose_mixed_env_and_file() {
        let set = set_with(&[("password", "filepass")]);
        let dsn = compose(
            &set,
            &RotationComposition::UsernamePassword {
                db: DbKind::Mysql,
                base_dsn: "mysql://db.internal/orders".into(),
                username: fixed("envuser"),
                password: FieldSource::Watched("password".into()),
            },
        )
        .unwrap();
        let url = url::Url::parse(dsn.expose()).unwrap();
        assert_eq!(url.username(), "envuser");
    }

    #[test]
    fn compose_missing_field_fails() {
        let set = set_with(&[("username", "svc")]);
        let err = compose(
            &set,
            &RotationComposition::UsernamePassword {
                db: DbKind::Postgres,
                base_dsn: "postgres://h/db".into(),
                username: FieldSource::Watched("username".into()),
                password: FieldSource::Watched("password".into()),
            },
        )
        .unwrap_err();
        assert!(matches!(
            err,
            CredentialError::Resolve(secrets::SecretError::MissingField(_))
        ));
    }

    // ---- expiry selection ----

    #[test]
    fn earliest_expiry_selection() {
        let t1 = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let t2 = SystemTime::UNIX_EPOCH + Duration::from_secs(200);
        assert_eq!(earliest([None, None]), None);
        assert_eq!(earliest([Some(t2), None, Some(t1)]), Some(t1));
        assert_eq!(earliest([Some(t1)]), Some(t1));
    }

    #[test]
    fn earliest_expiry_none_for_immutable_file_and_env() {
        // File-resolved material carries no expiry today, env is fixed -> None.
        let set = set_with(&[("password", "p")]);
        let comp = RotationComposition::UsernamePassword {
            db: DbKind::Mysql,
            base_dsn: "mysql://h/db".into(),
            username: fixed("u"),
            password: FieldSource::Watched("password".into()),
        };
        assert_eq!(earliest_expiry(&set, &comp), None);
    }

    // ---- apply_two_stage ----

    fn short() -> Duration {
        Duration::from_secs(5)
    }

    fn ok() -> Result<(), ()> {
        Ok(())
    }

    #[tokio::test]
    async fn apply_recheck_failure_keeps_old_without_closing() {
        let closed = Cell::new(false);
        let opened_new = Cell::new(false);
        let outcome = apply_two_stage(
            short(),
            || async { Err(RotationReject::PositionIncompatible) },
            || async {
                closed.set(true);
                ok()
            },
            || async {
                opened_new.set(true);
                ok()
            },
            || async { ok() },
        )
        .await;
        assert_eq!(
            outcome,
            ApplyOutcome::KeptOld {
                reason: RotationReject::PositionIncompatible
            }
        );
        assert!(!closed.get(), "old stream must not close on recheck fail");
        assert!(!opened_new.get());
    }

    #[tokio::test]
    async fn apply_success_in_order() {
        let order = Cell::new(0u8);
        let outcome = apply_two_stage(
            short(),
            || async {
                order.set(1);
                Ok(())
            },
            || async {
                assert_eq!(order.get(), 1);
                order.set(2);
                ok()
            },
            || async {
                assert_eq!(order.get(), 2);
                order.set(3);
                ok()
            },
            || async { ok() },
        )
        .await;
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert_eq!(order.get(), 3);
    }

    #[tokio::test]
    async fn apply_new_fails_old_recovers() {
        let opened_old = Cell::new(false);
        let outcome = apply_two_stage(
            short(),
            || async { Ok(()) },
            || async { ok() },
            || async { Err::<(), ()>(()) },
            || async {
                opened_old.set(true);
                ok()
            },
        )
        .await;
        assert_eq!(
            outcome,
            ApplyOutcome::KeptOld {
                reason: RotationReject::ReplacementOpenFailed
            }
        );
        assert!(opened_old.get());
    }

    #[tokio::test]
    async fn apply_neither_reconnects_fails_closed() {
        let outcome = apply_two_stage(
            short(),
            || async { Ok(()) },
            || async { ok() },
            || async { Err::<(), ()>(()) },
            || async { Err::<(), ()>(()) },
        )
        .await;
        assert_eq!(outcome, ApplyOutcome::FailedClosed);
    }

    #[tokio::test]
    async fn apply_open_new_timeout_recovers_old() {
        // open_new hangs -> its timeout fires -> old-credential recovery runs.
        let opened_old = Cell::new(false);
        let outcome = apply_two_stage(
            Duration::from_millis(50),
            || async { Ok(()) },
            || async { ok() },
            || async {
                std::future::pending::<()>().await; // never resolves
                ok()
            },
            || async {
                opened_old.set(true);
                ok()
            },
        )
        .await;
        assert_eq!(
            outcome,
            ApplyOutcome::KeptOld {
                reason: RotationReject::ReplacementOpenFailed
            }
        );
        assert!(opened_old.get(), "close_old must be followed by recovery");
    }

    #[tokio::test]
    async fn apply_close_timeout_opens_nothing() {
        // close_old hangs -> its timeout fires -> closure uncertain -> open NEITHER.
        let opened_new = Cell::new(false);
        let opened_old = Cell::new(false);
        let outcome = apply_two_stage(
            Duration::from_millis(50),
            || async { Ok(()) },
            || async {
                std::future::pending::<()>().await; // never resolves
                ok()
            },
            || async {
                opened_new.set(true);
                ok()
            },
            || async {
                opened_old.set(true);
                ok()
            },
        )
        .await;
        assert_eq!(outcome, ApplyOutcome::CloseUncertain);
        assert!(!opened_new.get(), "no new stream after uncertain close");
        assert!(
            !opened_old.get(),
            "no recovery stream after uncertain close"
        );
    }

    #[tokio::test]
    async fn apply_close_error_opens_nothing() {
        let opened_new = Cell::new(false);
        let opened_old = Cell::new(false);
        let outcome = apply_two_stage(
            short(),
            || async { Ok(()) },
            || async { Err::<(), ()>(()) }, // close reported an error
            || async {
                opened_new.set(true);
                ok()
            },
            || async {
                opened_old.set(true);
                ok()
            },
        )
        .await;
        assert_eq!(outcome, ApplyOutcome::CloseUncertain);
        assert!(!opened_new.get());
        assert!(!opened_old.get());
    }

    // ---- coordinator: supersession / terminal vs transient / expiry ----

    fn candidate(generation: u64) -> Candidate {
        Candidate::new(
            generation,
            ProtectedDsn::from("postgres://h/db"),
            SystemTime::UNIX_EPOCH,
            None,
        )
    }

    fn expiring(generation: u64, expires_at: SystemTime) -> Candidate {
        Candidate::new(
            generation,
            ProtectedDsn::from("postgres://h/db"),
            SystemTime::UNIX_EPOCH,
            Some(expires_at),
        )
    }

    fn coord(rx: watch::Receiver<Option<Candidate>>) -> RotationCoordinator {
        RotationCoordinator::new(
            rx,
            RetryConfig {
                base: Duration::from_secs(1),
                max: Duration::from_secs(10),
                deadline: Duration::from_secs(60),
            },
        )
    }

    fn ready(e: Eligibility) -> Candidate {
        match e {
            Eligibility::Ready(c) => c,
            other => panic!("expected Ready, got {other:?}"),
        }
    }

    fn idle(e: Eligibility) {
        assert!(matches!(e, Eligibility::Idle), "expected Idle, got {e:?}");
    }

    #[test]
    fn take_eligible_and_supersession() {
        let (tx, rx) = watch::channel(None);
        let mut c = coord(rx);
        let (now, wall) = (Instant::now(), SystemTime::now());

        idle(c.take_eligible(now, wall));
        tx.send(Some(candidate(1))).unwrap();
        assert_eq!(ready(c.take_eligible(now, wall)).generation, 1);
        idle(c.take_eligible(now, wall)); // in-flight, not re-handed

        // gen 2 supersedes the in-flight gen 1 (loop cancels 1's preflight).
        tx.send(Some(candidate(2))).unwrap();
        assert!(c.is_current(2));
        assert!(!c.is_current(1));
        assert_eq!(ready(c.take_eligible(now, wall)).generation, 2);
    }

    #[test]
    fn terminal_rejection_is_permanent() {
        let (tx, rx) = watch::channel(None);
        let mut c = coord(rx);
        let (now, wall) = (Instant::now(), SystemTime::now());
        tx.send(Some(candidate(5))).unwrap();
        let cand = ready(c.take_eligible(now, wall));
        assert_eq!(
            c.record_terminal_rejection(cand.generation),
            RecordOutcome::Recorded
        );
        tx.send(Some(candidate(5))).unwrap(); // same gen re-published
        idle(c.take_eligible(now, wall));
        tx.send(Some(candidate(6))).unwrap();
        assert_eq!(ready(c.take_eligible(now, wall)).generation, 6);
    }

    #[test]
    fn transient_failure_retries_after_backoff() {
        let (tx, rx) = watch::channel(None);
        let mut c = coord(rx);
        let wall = SystemTime::now();
        let t0 = Instant::now();
        tx.send(Some(candidate(7))).unwrap();
        let cand = ready(c.take_eligible(t0, wall));
        assert_eq!(
            c.record_transient_failure(cand.generation, t0),
            RecordOutcome::Recorded
        );
        idle(c.take_eligible(t0 + Duration::from_millis(500), wall));
        assert_eq!(
            ready(c.take_eligible(t0 + Duration::from_secs(2), wall))
                .generation,
            7
        );
    }

    #[test]
    fn transient_retry_deadline_gives_up() {
        let (tx, rx) = watch::channel(None);
        let mut c = coord(rx);
        let wall = SystemTime::now();
        let t0 = Instant::now();
        tx.send(Some(candidate(8))).unwrap();
        let cand = ready(c.take_eligible(t0, wall));
        let _ = c.record_transient_failure(cand.generation, t0);
        idle(c.take_eligible(t0 + Duration::from_secs(61), wall));
        tx.send(Some(candidate(8))).unwrap();
        idle(c.take_eligible(t0 + Duration::from_secs(62), wall));
    }

    #[test]
    fn newer_generation_obsoletes_retry_during_backoff() {
        let (tx, rx) = watch::channel(None);
        let mut c = coord(rx);
        let wall = SystemTime::now();
        let t0 = Instant::now();
        tx.send(Some(candidate(9))).unwrap();
        let cand = ready(c.take_eligible(t0, wall));
        let _ = c.record_transient_failure(cand.generation, t0);
        tx.send(Some(candidate(10))).unwrap();
        assert_eq!(
            ready(c.take_eligible(t0 + Duration::from_millis(1), wall))
                .generation,
            10
        );
    }

    #[test]
    fn transient_failure_on_superseded_generation_drops_retry() {
        let (tx, rx) = watch::channel(None);
        let mut c = coord(rx);
        let wall = SystemTime::now();
        let t0 = Instant::now();
        tx.send(Some(candidate(11))).unwrap();
        let cand = ready(c.take_eligible(t0, wall));
        // A newer candidate arrives while 11 is still in flight.
        tx.send(Some(candidate(12))).unwrap();
        // 11 is in flight so its failure is recorded (clears in-flight) but, being
        // superseded in the channel, schedules no retry.
        assert_eq!(
            c.record_transient_failure(cand.generation, t0),
            RecordOutcome::Recorded
        );
        assert_eq!(ready(c.take_eligible(t0, wall)).generation, 12);
    }

    #[test]
    fn already_expired_candidate_reports_expired_then_suppressed() {
        let (tx, rx) = watch::channel(None);
        let mut c = coord(rx);
        let now = Instant::now();
        let wall = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        let past = SystemTime::UNIX_EPOCH + Duration::from_secs(500);
        tx.send(Some(expiring(1, past))).unwrap();
        assert!(matches!(
            c.take_eligible(now, wall),
            Eligibility::Expired { generation: 1 }
        ));
        // Suppressed after the one Expired report.
        idle(c.take_eligible(now, wall));
        // A future expiry is eligible.
        let future = SystemTime::UNIX_EPOCH + Duration::from_secs(2_000);
        tx.send(Some(expiring(2, future))).unwrap();
        assert_eq!(ready(c.take_eligible(now, wall)).generation, 2);
    }

    #[test]
    fn expiry_ends_transient_retries() {
        let (tx, rx) = watch::channel(None);
        let mut c = coord(rx);
        let t0 = Instant::now();
        let wall_ok = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        let expires = SystemTime::UNIX_EPOCH + Duration::from_secs(1_500);
        tx.send(Some(expiring(3, expires))).unwrap();
        let cand = ready(c.take_eligible(t0, wall_ok));
        let _ = c.record_transient_failure(cand.generation, t0);
        // Backoff elapsed but the credential has now expired -> reported Expired.
        let wall_expired = SystemTime::UNIX_EPOCH + Duration::from_secs(1_600);
        assert!(matches!(
            c.take_eligible(t0 + Duration::from_secs(2), wall_expired),
            Eligibility::Expired { generation: 3 }
        ));
    }

    // ---- stale results never clear a newer in-flight generation ----

    /// Set up a coordinator with generation N+1 in flight after N was handed out and
    /// then superseded by N+1 via `take_eligible`.
    fn with_newer_in_flight() -> (RotationCoordinator, u64, u64) {
        let (tx, rx) = watch::channel(None);
        let mut c = coord(rx);
        let (now, wall) = (Instant::now(), SystemTime::now());
        tx.send(Some(candidate(11))).unwrap();
        let _n = ready(c.take_eligible(now, wall)); // 11 in flight
        tx.send(Some(candidate(12))).unwrap();
        let _n1 = ready(c.take_eligible(now, wall)); // 12 supersedes -> 12 in flight
        assert_eq!(c.in_flight(), Some(12));
        (c, 11, 12)
    }

    #[test]
    fn stale_transient_failure_does_not_clear_newer_in_flight() {
        let (mut c, n, n1) = with_newer_in_flight();
        assert_eq!(
            c.record_transient_failure(n, Instant::now()),
            RecordOutcome::Stale
        );
        assert_eq!(c.in_flight(), Some(n1), "newer must stay in flight");
        // n1 is not handed out again while in flight.
        idle(c.take_eligible(Instant::now(), SystemTime::now()));
    }

    #[test]
    fn stale_terminal_rejection_does_not_clear_newer_in_flight() {
        let (mut c, n, n1) = with_newer_in_flight();
        assert_eq!(c.record_terminal_rejection(n), RecordOutcome::Stale);
        assert_eq!(c.in_flight(), Some(n1));
        idle(c.take_eligible(Instant::now(), SystemTime::now()));
    }

    #[test]
    fn stale_applied_does_not_clear_newer_in_flight() {
        let (mut c, n, n1) = with_newer_in_flight();
        assert_eq!(c.record_applied(n), RecordOutcome::Stale);
        assert_eq!(c.in_flight(), Some(n1));
        idle(c.take_eligible(Instant::now(), SystemTime::now()));
        // The genuine result for n1 still records.
        assert_eq!(c.record_applied(n1), RecordOutcome::Recorded);
    }
}
