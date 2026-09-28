//! PostgreSQL-specific credential-rotation operations that plug into the
//! source-agnostic engine in [`crate::rotation`].
//!
//! Two layers live here:
//!
//! - **Control-connection ops** ([`preflight`], [`confirm_slot_inactive`]) that run
//!   on an ordinary (non-consumer) connection, so Stage-A preflight is safe to
//!   overlap with the live replication stream. They enforce three reviewer gates:
//!   - **Identity via durable slot-ownership authority** (gate 2): the replacement
//!     connection's observed `(system_identifier, database, database_oid)` must match
//!     the durable slot-ownership record, not merely be self-consistent. The new
//!     connection's own `database_oid` is never trusted alone - it is compared
//!     against the record that already bound it.
//!   - **Position recheck at the frozen boundary** (gate 3): the slot's
//!     `confirmed_flush_lsn` must not be ahead of the frozen boundary LSN, so
//!     resuming the replacement at that LSN cannot skip committed changes.
//!   - **Slot-inactive confirmation** (gate 4): after the old replication client is
//!     dropped, the slot must read back `active = false` before a close is
//!     considered confirmed; otherwise the old consumer may still hold it and a
//!     replacement must not be opened.
//! - **The [`RotationRuntime`]**, which the run loop drives: it owns the file
//!   watcher/manager task and the [`RotationCoordinator`], schedules Stage-A
//!   preflight concurrently with the stream, and applies the two-stage swap at a
//!   whole-transaction boundary. This layer enforces the remaining gates:
//!   - **Expiry recheck after preflight and immediately before Stage B** (gate 1).
//!   - **Await the apply to completion, never raced against cancellation** (gate 5).
//!   - **`CloseUncertain`/`FailedClosed` are fatal with no checkpoint advance**
//!     (gate 6): the run loop propagates the error and skips the teardown
//!     checkpoint put.
//!   - **Supersede an older preflight when a newer generation arrives** (gate 7).
//!
//! **Secret handling:** DSNs are carried as [`ProtectedDsn`] (zeroize-on-drop,
//! redacted `Debug`) through the manager, the preflight task, and the apply
//! closures; plaintext is borrowed via `expose()` only at the actual connect/parse
//! call, and never retained in a `String`.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime};

use checkpoints::CheckpointStore;
use common::RetryPolicy;
use deltaforge_config::PostgresSrcCfg;
use deltaforge_core::{SourceError, SourceResult};
use metrics::{counter, gauge};
use pgwire_replication::Lsn;
use secrets::{
    CredentialFieldRequest, FileWatcher, RotationOutcome, SecretProvider,
    SecretReference, SecretResolver, SecretString,
};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::credentials::ProtectedDsn;
use crate::rotation::{
    ApplyOutcome, Candidate, DbKind, Eligibility, FieldSource, RetryConfig,
    RotationComposition, RotationCoordinator, RotationReject, apply_two_stage,
    compose, earliest_expiry,
};

use super::RunCtx;
use super::fetch_slot_confirmed_lsn;
use super::postgres_helpers::{
    build_replication_config, connect_replication_with_retries, parse_dsn,
};
use super::postgres_slot_owner::{
    OwnerRead, connect, fetch_identity, ownership_proven, read_owner,
};

/// Stage-A preflight for a replacement DSN, on a fresh control connection.
///
/// Authenticates (the connect proves it), proves identity from the durable
/// slot-ownership record (gate 2), and rechecks the slot position against
/// `frozen_lsn` (gate 3). Returns a typed, redacted [`RotationReject`] on any
/// mismatch; the caller keeps the active stream.
pub(crate) async fn preflight(
    new_dsn: &str,
    source_id: &str,
    pipeline: &str,
    slot: &str,
    frozen_lsn: Lsn,
    chkpt: &Arc<dyn CheckpointStore>,
) -> Result<(), RotationReject> {
    // Authenticate on a control connection (off the replication stream).
    let client = connect(new_dsn)
        .await
        .map_err(|_| RotationReject::PreflightFailed)?;

    // Identity: observe (system_identifier, database, database_oid) live, then prove
    // it against the durable slot-ownership record. The new connection's OID is only
    // accepted because it matches the record that already bound it. A transient
    // authority-read failure is retryable; only missing/malformed/mismatch is fatal.
    let live = fetch_identity(&client)
        .await
        .map_err(|_| RotationReject::PreflightFailed)?;
    let owner = owner_to_reject(read_owner(chkpt, source_id).await)?;
    if !ownership_proven(&owner, source_id, pipeline, slot, &live) {
        return Err(RotationReject::IdentityMismatch);
    }

    // Position: the slot must not have flushed past the frozen boundary; resuming at
    // frozen_lsn must not skip committed changes. A failure to read the position is
    // transient (retryable); only a successfully-read ahead-of-boundary is terminal.
    let confirmed = fetch_slot_confirmed_lsn(new_dsn, slot)
        .await
        .map_err(|_| RotationReject::PreflightFailed)?;
    if confirmed > frozen_lsn {
        return Err(RotationReject::PositionIncompatible);
    }

    Ok(())
}

/// Confirm the logical slot is inactive (its previous consumer released it) before a
/// close is treated as successful (gate 4). Queried on a control connection with the
/// replacement DSN (any connection to the same server sees the shared slot state).
///
/// `Ok(())` only when the slot exists and reads `active = false`. A still-active
/// slot, a missing slot, or a query error is a non-confirmation - the caller must
/// treat the close as uncertain and open no replacement.
pub(crate) async fn confirm_slot_inactive(
    dsn: &str,
    slot: &str,
) -> Result<(), RotationReject> {
    let client = connect(dsn)
        .await
        .map_err(|_| RotationReject::PreflightFailed)?;
    match super::postgres_slot_owner::slot_status(&client, slot).await {
        Ok(Some(false)) => Ok(()),
        _ => Err(RotationReject::ReplacementOpenFailed),
    }
}

/// Map a slot-ownership authority read to the owner record or a typed rejection: a
/// store outage is transient ([`RotationReject::PreflightFailed`]) and retryable; a
/// missing or malformed record is a terminal identity/integrity failure.
fn owner_to_reject(
    read: OwnerRead,
) -> Result<super::postgres_slot_owner::SlotOwnership, RotationReject> {
    match read {
        OwnerRead::Present(rec) => Ok(rec),
        // Store unreachable: retry rather than permanently suppress the candidate.
        OwnerRead::Unavailable => Err(RotationReject::PreflightFailed),
        // No record, or a corrupt one: a real identity/integrity failure.
        OwnerRead::Missing | OwnerRead::Malformed => {
            Err(RotationReject::IdentityMismatch)
        }
    }
}

/// What the manager should do for one observed [`RotationOutcome`], derived by the
/// [`WatchAlarm`] edge detector so alarms fire once per transition (not per poll).
#[derive(Debug, PartialEq, Eq)]
enum WatchAction {
    /// A validated change: compose and publish the candidate. `recovered` is true
    /// when this change also cleared a prior alarm (emit a recovery log first).
    Publish { recovered: bool },
    /// Healthy no-op (unchanged, or a change still debouncing).
    Idle,
    /// The watched set returned to a healthy state after an alarm (no new candidate
    /// to publish): emit a recovery log.
    Recovered,
    /// First transition into an incomplete/transient watcher failure (missing or
    /// mid-swap material): warn once and count once.
    AlarmIncomplete,
    /// First transition into rejected/unsafe material (escaping, oversize,
    /// non-UTF-8, or malformed): warn once and count once.
    AlarmRejected,
}

/// Edge detector over watcher outcomes: turns a per-poll stream of
/// [`RotationOutcome`]s into one action per state transition, so a persistently
/// bad watched set alarms exactly once and recovers exactly once. Retaining the
/// active credentials is implicit - no failing outcome ever yields `Publish`.
#[derive(Default)]
struct WatchAlarm {
    in_alarm: bool,
}

impl WatchAlarm {
    fn observe(&mut self, outcome: &RotationOutcome) -> WatchAction {
        match outcome {
            RotationOutcome::Rotated { .. } => {
                let recovered = self.in_alarm;
                self.in_alarm = false;
                WatchAction::Publish { recovered }
            }
            RotationOutcome::NoChange => {
                if self.in_alarm {
                    self.in_alarm = false;
                    WatchAction::Recovered
                } else {
                    WatchAction::Idle
                }
            }
            RotationOutcome::Debouncing => WatchAction::Idle,
            RotationOutcome::Incomplete => {
                if self.in_alarm {
                    WatchAction::Idle
                } else {
                    self.in_alarm = true;
                    WatchAction::AlarmIncomplete
                }
            }
            RotationOutcome::Rejected(_) => {
                if self.in_alarm {
                    WatchAction::Idle
                } else {
                    self.in_alarm = true;
                    WatchAction::AlarmRejected
                }
            }
        }
    }
}

/// Whether a rejection is worth a bounded retry (transient outage) versus permanent
/// suppression (wrong server/database, position irrecoverable, or expired).
fn is_transient(reject: RotationReject) -> bool {
    matches!(
        reject,
        RotationReject::PreflightFailed | RotationReject::ReplacementOpenFailed
    )
}

/// What the caller should do after an apply outcome is recorded.
#[derive(Debug, PartialEq, Eq)]
enum ApplyStep {
    /// Replacement opened: swap the runtime DSN and continue.
    Applied,
    /// Current credentials retained (recheck failed, or bad replacement recovered).
    Kept,
}

/// Record an [`ApplyOutcome`] against the coordinator and decide the run loop's next
/// step. `CloseUncertain`/`FailedClosed` map to a fatal error (gate 6); the caller
/// must stop without advancing the checkpoint. Pure w.r.t. the connection so it is
/// unit-testable in isolation.
fn record_and_decide(
    coordinator: &mut RotationCoordinator,
    generation: u64,
    outcome: ApplyOutcome,
) -> SourceResult<ApplyStep> {
    match outcome {
        ApplyOutcome::Applied => {
            let _ = coordinator.record_applied(generation);
            Ok(ApplyStep::Applied)
        }
        ApplyOutcome::KeptOld { reason } => {
            if is_transient(reason) {
                let _ = coordinator
                    .record_transient_failure(generation, Instant::now());
            } else {
                let _ = coordinator.record_terminal_rejection(generation);
            }
            Ok(ApplyStep::Kept)
        }
        ApplyOutcome::CloseUncertain => {
            let _ = coordinator.record_terminal_rejection(generation);
            Err(SourceError::Other(anyhow::anyhow!(
                "credential rotation close-uncertain: old replication stream \
                 closure unconfirmed"
            )))
        }
        ApplyOutcome::FailedClosed => {
            let _ = coordinator.record_terminal_rejection(generation);
            Err(SourceError::Other(anyhow::anyhow!(
                "credential rotation failed closed: no replication stream \
                 reconnected"
            )))
        }
    }
}

fn fail_closed(
    details: impl Into<std::borrow::Cow<'static, str>>,
) -> SourceError {
    SourceError::Incompatible {
        details: details.into(),
    }
}

/// Everything needed to run controlled rotation for a PostgreSQL source, derived
/// from config at build time. Held behind an `Arc` on the source because it embeds
/// non-`Clone` composition material (a fixed env value is a `SecretString`).
pub struct PgRotationSpec {
    /// Watched credential fields (file-backed references only). Never printed:
    /// [`PgRotationSpec`]'s `Debug` redacts the references.
    fields: Vec<CredentialFieldRequest>,
    /// How to assemble a rotated DSN from the watcher's validated set, plus any
    /// fixed (env) fields resolved once at build time.
    composition: RotationComposition,
    /// Projected-volume trusted root for symlink resolution.
    trusted_root: std::path::PathBuf,
    max_size: usize,
    debounce: Duration,
    poll_interval: Duration,
    apply_timeout: Duration,
}

impl std::fmt::Debug for PgRotationSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Redact the watched references; expose only non-secret shape/timing.
        f.debug_struct("PgRotationSpec")
            .field("watched_fields", &self.fields.len())
            .field("trusted_root", &self.trusted_root)
            .field("max_size", &self.max_size)
            .field("debounce", &self.debounce)
            .field("poll_interval", &self.poll_interval)
            .field("apply_timeout", &self.apply_timeout)
            .finish_non_exhaustive()
    }
}

/// Validate a watched file reference at startup by canonicalizing the trusted root
/// and the reference, requiring the resolved target to be a regular file inside the
/// canonical root. This fails closed on `..` traversal, an escaping symlink, or a
/// missing target - unlike a lexical prefix check, which the watcher would only
/// reject later, after the feature had already started. Runs the blocking
/// filesystem work off the async runtime. The watcher retains its own runtime checks
/// for rotation-time races.
async fn validate_watched_path(
    location: &str,
    trusted_root: &Path,
) -> SourceResult<()> {
    let location = location.to_string();
    let trusted_root = trusted_root.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let canon_root = std::fs::canonicalize(&trusted_root).map_err(|e| {
            fail_closed(format!(
                "rotation trusted_root {} is not accessible: {e}",
                trusted_root.display()
            ))
        })?;
        let canon = std::fs::canonicalize(&location).map_err(|_| {
            fail_closed(
                "rotation watched secret path could not be resolved (missing \
                 or inaccessible target)",
            )
        })?;
        if !canon.starts_with(&canon_root) {
            return Err(fail_closed(format!(
                "rotation watched secret resolves outside the configured \
                 trusted_root ({}); point the reference inside the projected \
                 volume or correct trusted_root",
                canon_root.display()
            )));
        }
        let meta = std::fs::metadata(&canon).map_err(|_| {
            fail_closed("rotation watched secret is not accessible")
        })?;
        if !meta.is_file() {
            return Err(fail_closed(
                "rotation watched secret target is not a regular file",
            ));
        }
        Ok(())
    })
    .await
    .map_err(|e| {
        fail_closed(format!("rotation path validation task failed: {e}"))
    })?
}

/// Resolve an env-backed credential field once into a fixed [`SecretString`]. Env
/// values are process-immutable, so they do not rotate: they are composed alongside
/// the watched file field(s).
async fn resolve_fixed_env(
    resolver: &dyn SecretResolver,
    reference: &SecretReference,
    limit: usize,
) -> SourceResult<SecretString> {
    let resolved = resolver
        .resolve(reference)
        .await
        .map_err(|e| fail_closed(format!("rotation env credential: {e}")))?;
    let value = resolved
        .material()
        .as_utf8()
        .ok_or_else(|| fail_closed("rotation env credential is not UTF-8"))?;
    SecretString::new(value.to_owned(), limit, reference)
        .map_err(|e| fail_closed(format!("rotation env credential: {e}")))
}

/// Build a [`FieldSource`] for one credential field: a file-backed reference is
/// watched (and validated to lie under the trusted root), an env reference is
/// resolved once into a fixed value, and any other provider fails closed.
async fn field_source_for(
    reference: &SecretReference,
    name: &'static str,
    trusted_root: &Path,
    max_size: usize,
    fields: &mut Vec<CredentialFieldRequest>,
    resolver: &dyn SecretResolver,
) -> SourceResult<FieldSource> {
    match reference.provider {
        SecretProvider::File => {
            validate_watched_path(&reference.location, trusted_root).await?;
            fields.push(CredentialFieldRequest::new(name, reference.clone()));
            Ok(FieldSource::Watched(name.to_string()))
        }
        SecretProvider::Env => {
            let fixed =
                resolve_fixed_env(resolver, reference, max_size).await?;
            Ok(FieldSource::Fixed(fixed))
        }
        SecretProvider::Vault => Err(fail_closed(format!(
            "rotation credential `{name}` uses the Vault provider, which is \
             not supported for rotation in this release; use a file-backed or \
             env reference"
        ))),
    }
}

/// Build a rotation spec from source config.
///
/// Fail-closed: `rotation: None` yields `Ok(None)`, but rotation configured with
/// nothing file-backed to watch, a reference outside the trusted root, an
/// unsupported provider, or missing required data yields an `Err` at startup rather
/// than silently disabling the requested safety feature. Mixed env/file credentials
/// are supported: the env field is resolved once as fixed and the file field is
/// watched.
pub async fn build_spec(
    cfg: &PostgresSrcCfg,
    resolver: &dyn SecretResolver,
) -> SourceResult<Option<Arc<PgRotationSpec>>> {
    let Some(rot) = cfg.rotation.as_ref() else {
        return Ok(None);
    };
    let max_size = rot.max_secret_bytes;
    let trusted_root = rot.trusted_root.clone();
    let debounce = Duration::from_millis(rot.debounce_ms);
    let poll_interval = Duration::from_millis(rot.poll_interval_ms);
    let apply_timeout = Duration::from_millis(rot.apply_timeout_ms);

    // Whole-DSN secret.
    if let Some(dsn_ref) = &cfg.dsn_secret {
        return match dsn_ref.provider {
            SecretProvider::File => {
                validate_watched_path(&dsn_ref.location, &trusted_root).await?;
                let fields =
                    vec![CredentialFieldRequest::new("dsn", dsn_ref.clone())];
                let composition = RotationComposition::WholeDsn {
                    field: "dsn".to_string(),
                };
                Ok(Some(Arc::new(PgRotationSpec {
                    fields,
                    composition,
                    trusted_root,
                    max_size,
                    debounce,
                    poll_interval,
                    apply_timeout,
                })))
            }
            _ => Err(fail_closed(
                "rotation is configured but `dsn_secret` is not a \
                 file-backed reference; env/Vault secrets do not rotate. Use a \
                 file-backed `dsn_secret` or remove `rotation`.",
            )),
        };
    }

    // Username/password over a base DSN.
    let creds = cfg.credentials.as_ref().ok_or_else(|| {
        fail_closed(
            "rotation is configured but the source has no `dsn_secret` or \
             `credentials` to rotate; remove `rotation` or reference a \
             file-backed secret.",
        )
    })?;
    let username = creds.username.as_ref().ok_or_else(|| {
        fail_closed("rotation requires `credentials.username`")
    })?;
    let password = creds.password.as_ref().ok_or_else(|| {
        fail_closed("rotation requires `credentials.password`")
    })?;
    let base_dsn = cfg.dsn.clone().ok_or_else(|| {
        fail_closed("rotation with `credentials` requires a base `dsn`")
    })?;

    let mut fields = Vec::new();
    let username_src = field_source_for(
        username,
        "username",
        &trusted_root,
        max_size,
        &mut fields,
        resolver,
    )
    .await?;
    let password_src = field_source_for(
        password,
        "password",
        &trusted_root,
        max_size,
        &mut fields,
        resolver,
    )
    .await?;

    // At least one field must be file-backed, or nothing ever rotates.
    if fields.is_empty() {
        return Err(fail_closed(
            "rotation is configured but neither credential field is \
             file-backed; there is nothing to watch. Use at least one \
             file-backed credential or remove `rotation`.",
        ));
    }

    let composition = RotationComposition::UsernamePassword {
        db: DbKind::Postgres,
        base_dsn,
        username: username_src,
        password: password_src,
    };
    Ok(Some(Arc::new(PgRotationSpec {
        fields,
        composition,
        trusted_root,
        max_size,
        debounce,
        poll_interval,
        apply_timeout,
    })))
}

/// An in-flight Stage-A preflight for one candidate generation.
struct Preflight {
    generation: u64,
    candidate: Candidate,
    handle: JoinHandle<Result<(), RotationReject>>,
    /// Set once the handle has resolved (or panicked); harvested at the boundary.
    done: Option<Result<(), RotationReject>>,
}

/// Runtime the PostgreSQL run loop drives to apply credential rotation.
pub(crate) struct RotationRuntime {
    coordinator: RotationCoordinator,
    /// The file-watcher/manager task publishing candidates; a child of the source
    /// cancel token so it dies with the source.
    manager: JoinHandle<()>,
    running: Arc<AtomicBool>,
    child_cancel: CancellationToken,
    source_id: String,
    pipeline: String,
    slot: String,
    publication: String,
    chkpt: Arc<dyn CheckpointStore>,
    apply_timeout: Duration,
    preflight: Option<Preflight>,
}

impl RotationRuntime {
    /// Spawn the file-watcher manager and build the runtime. `initial_dsn` is the
    /// startup DSN; the manager suppresses a first candidate that composes to it, so
    /// the initial projected-volume load does not trigger a needless reconnect.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn spawn(
        spec: &Arc<PgRotationSpec>,
        initial_dsn: ProtectedDsn,
        source_id: String,
        pipeline: String,
        slot: String,
        publication: String,
        chkpt: Arc<dyn CheckpointStore>,
        parent_cancel: &CancellationToken,
    ) -> SourceResult<Self> {
        let watcher = FileWatcher::projected_volume(
            spec.trusted_root.clone(),
            spec.max_size,
            spec.fields.clone(),
            spec.debounce,
        )
        .map_err(|e| {
            fail_closed(format!("credential rotation watcher: {e}"))
        })?;

        let (tx, rx) = watch::channel::<Option<Candidate>>(None);
        let running = Arc::new(AtomicBool::new(true));
        let child_cancel = parent_cancel.child_token();

        let manager = spawn_manager(
            watcher,
            Arc::clone(spec),
            tx,
            initial_dsn,
            running.clone(),
            child_cancel.clone(),
            source_id.clone(),
            pipeline.clone(),
        );

        Ok(Self {
            coordinator: RotationCoordinator::new(rx, RetryConfig::default()),
            manager,
            running,
            child_cancel,
            source_id,
            pipeline,
            slot,
            publication,
            chkpt,
            apply_timeout: spec.apply_timeout,
            preflight: None,
        })
    }

    /// Wait for rotation activity for the run loop's `select!`: either a newly
    /// published candidate or completion of the in-flight preflight. Cancellation
    /// safe - dropping this future leaves the coordinator and preflight intact.
    pub(crate) async fn wait_activity(&mut self) {
        let Self {
            coordinator,
            preflight,
            ..
        } = self;
        match preflight {
            Some(pf) if pf.done.is_none() => {
                tokio::select! {
                    _ = coordinator.changed() => {}
                    res = &mut pf.handle => {
                        pf.done = Some(match res {
                            Ok(r) => r,
                            // A panicked/aborted preflight task is a transient failure.
                            Err(_join) => Err(RotationReject::PreflightFailed),
                        });
                    }
                }
            }
            _ => {
                coordinator.changed().await;
            }
        }
    }

    /// Stage A: if the latest candidate is eligible and none is in flight, spawn its
    /// preflight on a control connection concurrently with the live stream. A newer
    /// generation supersedes and aborts an older preflight (gate 7). Safe to call on
    /// every loop turn; it spawns at most one preflight per generation.
    pub(crate) fn drive_preflight(&mut self, ctx: &RunCtx) {
        match self
            .coordinator
            .take_eligible(Instant::now(), SystemTime::now())
        {
            Eligibility::Ready(candidate) => {
                // Supersede any older, now-obsolete preflight (gate 7).
                if let Some(old) = self.preflight.take() {
                    old.handle.abort();
                }
                let generation = candidate.generation;
                // Carry the protected DSN into the task; expose only at the call.
                let candidate_dsn = candidate.dsn.clone();
                let source_id = self.source_id.clone();
                let pipeline = self.pipeline.clone();
                let slot = self.slot.clone();
                let frozen = ctx.last_lsn;
                let chkpt = Arc::clone(&self.chkpt);
                let handle = tokio::spawn(async move {
                    preflight(
                        candidate_dsn.expose(),
                        &source_id,
                        &pipeline,
                        &slot,
                        frozen,
                        &chkpt,
                    )
                    .await
                });
                self.preflight = Some(Preflight {
                    generation,
                    candidate,
                    handle,
                    done: None,
                });
            }
            Eligibility::Expired { generation } => {
                // An expired candidate is permanently suppressed: count it as a
                // terminal rejection (take_eligible has already floored it).
                self.metric_terminal();
                warn!(
                    source_id = %self.source_id,
                    generation,
                    "rotation candidate expired before it could be applied; \
                     retaining current credentials"
                );
            }
            Eligibility::Idle => {}
        }
    }

    /// Stage B: if a preflight has completed, apply its result at the current
    /// whole-transaction boundary. Must be called only when `ctx.current_tx_id` is
    /// `None`. The two-stage apply is awaited to completion (gate 5). On
    /// `CloseUncertain`/`FailedClosed` returns an error so the run loop stops without
    /// advancing the checkpoint (gate 6).
    pub(crate) async fn apply_at_boundary(
        &mut self,
        ctx: &mut RunCtx,
    ) -> SourceResult<()> {
        let ready = matches!(&self.preflight, Some(pf) if pf.done.is_some());
        if !ready {
            return Ok(());
        }
        let pf = self.preflight.take().expect("checked ready");
        let generation = pf.generation;

        // Harvest the Stage-A result. A failed preflight keeps the current stream.
        if let Err(reject) = pf.done.expect("checked done") {
            if is_transient(reject) {
                self.metric_transient();
                let _ = self
                    .coordinator
                    .record_transient_failure(generation, Instant::now());
            } else {
                self.metric_terminal();
                let _ = self.coordinator.record_terminal_rejection(generation);
            }
            warn!(
                source_id = %self.source_id,
                generation,
                reject = ?reject,
                "credential rotation preflight failed; retaining current \
                 credentials"
            );
            return Ok(());
        }

        // Stage-B closures. Each captures owned/protected state so nothing borrows
        // `ctx` across the awaited apply; DSNs stay protected until the connect/parse.
        let frozen = ctx.last_lsn;
        let new_dsn = pf.candidate.dsn.clone();
        let old_dsn = ctx.dsn.clone();
        let candidate = pf.candidate.clone();
        let source_id = self.source_id.clone();
        let pipeline = self.pipeline.clone();
        let slot = self.slot.clone();
        let publication = self.publication.clone();
        let chkpt = Arc::clone(&self.chkpt);
        let repl_client = Arc::clone(&ctx.repl_client);
        let cancel = self.child_cancel.clone();

        // recheck (gate 1 + gate 3): candidate not expired, and a fresh re-preflight
        // against the frozen boundary on a new control connection.
        let recheck = {
            let new_dsn = new_dsn.clone();
            let source_id = source_id.clone();
            let pipeline = pipeline.clone();
            let slot = slot.clone();
            let chkpt = Arc::clone(&chkpt);
            move || async move {
                if candidate.is_expired(SystemTime::now()) {
                    return Err(RotationReject::Expired);
                }
                preflight(
                    new_dsn.expose(),
                    &source_id,
                    &pipeline,
                    &slot,
                    frozen,
                    &chkpt,
                )
                .await
            }
        };

        // close_old (gate 4): shut down the old consumer, then confirm the slot went
        // inactive. Any failure leaves closure unconfirmed -> CloseUncertain.
        let close_old = {
            let repl_client = Arc::clone(&repl_client);
            let confirm_dsn = new_dsn.clone();
            let slot = slot.clone();
            move || async move {
                {
                    let mut guard = repl_client.lock().await;
                    guard
                        .shutdown()
                        .await
                        .map_err(|_| RotationReject::ReplacementOpenFailed)?;
                }
                confirm_slot_inactive(confirm_dsn.expose(), &slot).await
            }
        };

        // open_new: replacement stream at the frozen position with the new creds.
        let open_new = {
            let repl_client = Arc::clone(&repl_client);
            let new_dsn = new_dsn.clone();
            let source_id = source_id.clone();
            let slot = slot.clone();
            let publication = publication.clone();
            let cancel = cancel.clone();
            move || async move {
                let client = open_stream(
                    new_dsn.expose(),
                    &source_id,
                    &slot,
                    &publication,
                    frozen,
                    &cancel,
                )
                .await?;
                *repl_client.lock().await = client;
                Ok::<(), ()>(())
            }
        };

        // open_old: bounded recovery to the old creds if the replacement fails.
        let open_old = {
            let repl_client = Arc::clone(&repl_client);
            let old_dsn = old_dsn.clone();
            let source_id = source_id.clone();
            let slot = slot.clone();
            let publication = publication.clone();
            let cancel = cancel.clone();
            move || async move {
                let client = open_stream(
                    old_dsn.expose(),
                    &source_id,
                    &slot,
                    &publication,
                    frozen,
                    &cancel,
                )
                .await?;
                *repl_client.lock().await = client;
                Ok::<(), ()>(())
            }
        };

        // Gate 5: awaited to completion, never raced against cancellation.
        let outcome = apply_two_stage(
            self.apply_timeout,
            recheck,
            close_old,
            open_new,
            open_old,
        )
        .await;

        // Metrics (bounded, secret-free labels: pipeline + source).
        match &outcome {
            ApplyOutcome::Applied => self.metric_applied(generation),
            ApplyOutcome::KeptOld { reason } if is_transient(*reason) => {
                self.metric_transient()
            }
            ApplyOutcome::KeptOld { .. } => self.metric_terminal(),
            ApplyOutcome::CloseUncertain | ApplyOutcome::FailedClosed => {
                self.metric_fatal()
            }
        }

        // Gate 6: fatal outcomes propagate as an error (no checkpoint advance).
        let step =
            record_and_decide(&mut self.coordinator, generation, outcome);
        match &step {
            Ok(ApplyStep::Applied) => {
                ctx.dsn = pf.candidate.dsn.clone();
                ctx.schema.set_dsn(pf.candidate.dsn.clone());
                info!(
                    source_id = %self.source_id,
                    generation,
                    "credential rotation applied at transaction boundary"
                );
            }
            Ok(ApplyStep::Kept) => {
                warn!(
                    source_id = %self.source_id,
                    generation,
                    "credential rotation not applied; retained current \
                     credentials"
                );
            }
            Err(_) => {
                error!(
                    source_id = %self.source_id,
                    generation,
                    "credential rotation could not complete safely; stopping \
                     without advancing the checkpoint"
                );
            }
        }
        step.map(|_| ())
    }

    fn metric_applied(&self, generation: u64) {
        counter!(
            "deltaforge_rotation_applied_total",
            "pipeline" => self.pipeline.clone(),
            "source" => self.source_id.clone(),
        )
        .increment(1);
        gauge!(
            "deltaforge_rotation_applied_generation",
            "pipeline" => self.pipeline.clone(),
            "source" => self.source_id.clone(),
        )
        .set(generation as f64);
    }

    fn metric_transient(&self) {
        counter!(
            "deltaforge_rotation_transient_failures_total",
            "pipeline" => self.pipeline.clone(),
            "source" => self.source_id.clone(),
        )
        .increment(1);
    }

    fn metric_terminal(&self) {
        counter!(
            "deltaforge_rotation_terminal_rejections_total",
            "pipeline" => self.pipeline.clone(),
            "source" => self.source_id.clone(),
        )
        .increment(1);
    }

    fn metric_fatal(&self) {
        counter!(
            "deltaforge_rotation_fatal_failures_total",
            "pipeline" => self.pipeline.clone(),
            "source" => self.source_id.clone(),
        )
        .increment(1);
    }

    /// Cancel and join the manager and any in-flight preflight, so no rotation task
    /// outlives the source. Called on the run loop's normal and fatal exit paths;
    /// [`Drop`] is the backstop for panics.
    pub(crate) async fn shutdown(mut self) {
        self.running.store(false, Ordering::Relaxed);
        self.child_cancel.cancel();
        if let Some(mut pf) = self.preflight.take() {
            pf.handle.abort();
            let _ = (&mut pf.handle).await;
        }
        let _ = (&mut self.manager).await;
    }
}

impl Drop for RotationRuntime {
    /// Backstop for exit paths that do not call [`RotationRuntime::shutdown`] (for
    /// example a panic): synchronously signal cancellation and abort the spawned
    /// tasks. `Drop` cannot await, so this aborts rather than joins - the async
    /// `shutdown` is the path that joins.
    fn drop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
        self.child_cancel.cancel();
        self.manager.abort();
        if let Some(pf) = self.preflight.take() {
            pf.handle.abort();
        }
    }
}

/// Open a replication stream for `dsn` at `start_lsn` via the production connect
/// path (same builder and retry policy as startup/reconnect).
async fn open_stream(
    dsn: &str,
    source_id: &str,
    slot: &str,
    publication: &str,
    start_lsn: Lsn,
    cancel: &CancellationToken,
) -> Result<pgwire_replication::ReplicationClient, ()> {
    let components = parse_dsn(dsn).map_err(|_| ())?;
    let config =
        build_replication_config(&components, slot, publication, start_lsn);
    connect_replication_with_retries(
        source_id,
        config,
        cancel,
        RetryPolicy::default(),
    )
    .await
    .map_err(|_| ())
}

/// Spawn the file-watcher manager: poll the projected volume, and on each validated
/// change compose a candidate DSN and publish it. Suppresses a candidate identical
/// to `initial_dsn` (or the previous candidate) so no needless reconnect fires. DSNs
/// are compared as protected values via [`ProtectedDsn::same_dsn`]; no plaintext is
/// retained.
#[allow(clippy::too_many_arguments)]
fn spawn_manager(
    mut watcher: FileWatcher,
    spec: Arc<PgRotationSpec>,
    tx: watch::Sender<Option<Candidate>>,
    initial_dsn: ProtectedDsn,
    running: Arc<AtomicBool>,
    cancel: CancellationToken,
    source_id: String,
    pipeline: String,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut last_published = initial_dsn;
        let mut alarm = WatchAlarm::default();
        let poll = spec.poll_interval;
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(poll) => {}
            }
            if !running.load(Ordering::Relaxed) {
                break;
            }

            // Observe EVERY outcome, not just successful rotations, so a bad watched
            // set raises an alarm and metric instead of silently retaining the old
            // credentials.
            let outcome = watcher.tick(Instant::now()).await;
            match alarm.observe(&outcome) {
                WatchAction::Idle => {}
                WatchAction::Recovered => {
                    info!(
                        source_id = %source_id,
                        pipeline = %pipeline,
                        "rotation watched secret is valid again"
                    );
                }
                WatchAction::AlarmIncomplete => {
                    // Redacted: no path/value/error body.
                    warn!(
                        source_id = %source_id,
                        pipeline = %pipeline,
                        "rotation watched secret is incomplete (missing or \
                         mid-swap); retaining current credentials"
                    );
                    watch_counter(
                        "deltaforge_rotation_watch_incomplete_total",
                        &pipeline,
                        &source_id,
                    );
                }
                WatchAction::AlarmRejected => {
                    warn!(
                        source_id = %source_id,
                        pipeline = %pipeline,
                        "rotation watched secret was rejected as unsafe \
                         (escaping, oversize, non-UTF-8, or malformed); \
                         retaining current credentials"
                    );
                    watch_counter(
                        "deltaforge_rotation_watch_rejected_total",
                        &pipeline,
                        &source_id,
                    );
                }
                WatchAction::Publish { recovered } => {
                    if recovered {
                        info!(
                            source_id = %source_id,
                            pipeline = %pipeline,
                            "rotation watched secret is valid again"
                        );
                    }
                    let Some(set) = watcher.latest() else {
                        continue;
                    };
                    match compose(&set, &spec.composition) {
                        Ok(dsn) => {
                            if dsn.same_dsn(&last_published) {
                                continue;
                            }
                            let expires_at =
                                earliest_expiry(&set, &spec.composition);
                            let generation = watcher.generation();
                            last_published = dsn.clone();
                            let candidate = Candidate::new(
                                generation,
                                dsn,
                                SystemTime::now(),
                                expires_at,
                            );
                            let _ = tx.send(Some(candidate));
                        }
                        Err(_e) => {
                            // Validated material that cannot be composed is unsafe;
                            // treat as a rejection (redacted, no error body).
                            if !alarm.in_alarm {
                                alarm.in_alarm = true;
                                warn!(
                                    source_id = %source_id,
                                    pipeline = %pipeline,
                                    "rotation candidate composition failed; \
                                     retaining current credentials"
                                );
                                watch_counter(
                                    "deltaforge_rotation_watch_rejected_total",
                                    &pipeline,
                                    &source_id,
                                );
                            }
                        }
                    }
                }
            }
        }
    })
}

/// Increment a watcher-failure counter with bounded, secret-free labels.
fn watch_counter(name: &'static str, pipeline: &str, source_id: &str) {
    counter!(
        name,
        "pipeline" => pipeline.to_string(),
        "source" => source_id.to_string(),
    )
    .increment(1);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn coordinator_with_candidate(generation: u64) -> RotationCoordinator {
        // A coordinator holding one in-flight candidate, so record_* apply rather
        // than being ignored as stale.
        let dsn = ProtectedDsn::from("host=h dbname=d user=u password=p");
        let candidate =
            Candidate::new(generation, dsn, SystemTime::now(), None);
        let (tx, rx) = watch::channel(Some(candidate));
        // Keep the sender alive for the channel's lifetime.
        std::mem::forget(tx);
        let mut c = RotationCoordinator::new(rx, RetryConfig::default());
        // Move the candidate in-flight.
        let _ = c.take_eligible(Instant::now(), SystemTime::now());
        c
    }

    #[test]
    fn close_uncertain_is_fatal() {
        let mut c = coordinator_with_candidate(1);
        let r = record_and_decide(&mut c, 1, ApplyOutcome::CloseUncertain);
        assert!(r.is_err(), "CloseUncertain must be fatal");
        assert_eq!(c.in_flight(), None, "generation is cleared (terminal)");
    }

    #[test]
    fn failed_closed_is_fatal() {
        let mut c = coordinator_with_candidate(1);
        let r = record_and_decide(&mut c, 1, ApplyOutcome::FailedClosed);
        assert!(r.is_err(), "FailedClosed must be fatal");
        assert_eq!(c.in_flight(), None);
    }

    #[test]
    fn applied_advances_and_is_not_fatal() {
        let mut c = coordinator_with_candidate(1);
        let r = record_and_decide(&mut c, 1, ApplyOutcome::Applied);
        assert_eq!(r.ok(), Some(ApplyStep::Applied));
        assert_eq!(c.in_flight(), None);
    }

    #[test]
    fn kept_old_transient_schedules_retry_not_fatal() {
        let mut c = coordinator_with_candidate(1);
        let r = record_and_decide(
            &mut c,
            1,
            ApplyOutcome::KeptOld {
                reason: RotationReject::PreflightFailed,
            },
        );
        assert_eq!(r.ok(), Some(ApplyStep::Kept));
    }

    #[test]
    fn kept_old_terminal_is_not_fatal() {
        let mut c = coordinator_with_candidate(1);
        let r = record_and_decide(
            &mut c,
            1,
            ApplyOutcome::KeptOld {
                reason: RotationReject::IdentityMismatch,
            },
        );
        assert_eq!(r.ok(), Some(ApplyStep::Kept));
    }

    #[test]
    fn transient_classification() {
        assert!(is_transient(RotationReject::PreflightFailed));
        assert!(is_transient(RotationReject::ReplacementOpenFailed));
        assert!(!is_transient(RotationReject::IdentityMismatch));
        assert!(!is_transient(RotationReject::PositionIncompatible));
        assert!(!is_transient(RotationReject::Expired));
    }

    #[test]
    fn owner_read_unavailable_is_transient_retryable() {
        // A store outage must be retryable, not a permanent identity rejection.
        let err = owner_to_reject(OwnerRead::Unavailable).unwrap_err();
        assert_eq!(err, RotationReject::PreflightFailed);
        assert!(is_transient(err));
    }

    #[test]
    fn owner_read_missing_or_malformed_is_terminal() {
        assert_eq!(
            owner_to_reject(OwnerRead::Missing).unwrap_err(),
            RotationReject::IdentityMismatch
        );
        assert_eq!(
            owner_to_reject(OwnerRead::Malformed).unwrap_err(),
            RotationReject::IdentityMismatch
        );
        assert!(!is_transient(RotationReject::IdentityMismatch));
    }

    #[test]
    fn transient_failure_retries_same_generation() {
        use crate::rotation::RecordOutcome;
        // Generation 7 is in flight.
        let mut c = coordinator_with_candidate(7);
        let now = Instant::now();
        // A transient failure is recorded and schedules a bounded retry.
        assert_eq!(c.record_transient_failure(7, now), RecordOutcome::Recorded);
        // Within the backoff window, nothing is offered.
        assert!(matches!(
            c.take_eligible(now, SystemTime::now()),
            Eligibility::Idle
        ));
        // After the backoff window, the SAME generation is retried.
        let later = now + Duration::from_secs(5);
        match c.take_eligible(later, SystemTime::now()) {
            Eligibility::Ready(cand) => assert_eq!(cand.generation, 7),
            other => panic!("expected Ready(7) after backoff, got {other:?}"),
        }
    }

    fn rejected() -> RotationOutcome {
        RotationOutcome::Rejected(secrets::SecretError::NotFound(
            SecretReference::new(SecretProvider::File, "/x").safe(),
        ))
    }

    #[test]
    fn watch_alarm_incomplete_fires_once_then_recovers() {
        let mut a = WatchAlarm::default();
        // One transition into the alarm; further bad polls do not re-fire.
        assert_eq!(
            a.observe(&RotationOutcome::Incomplete),
            WatchAction::AlarmIncomplete
        );
        assert_eq!(a.observe(&RotationOutcome::Incomplete), WatchAction::Idle);
        assert_eq!(a.observe(&RotationOutcome::Incomplete), WatchAction::Idle);
        // Recovery when the set is valid again (unchanged from last-good).
        assert_eq!(
            a.observe(&RotationOutcome::NoChange),
            WatchAction::Recovered
        );
        // Steady healthy state does not re-log recovery.
        assert_eq!(a.observe(&RotationOutcome::NoChange), WatchAction::Idle);
    }

    #[test]
    fn watch_alarm_rejected_fires_once_and_stays_latched() {
        let mut a = WatchAlarm::default();
        assert_eq!(a.observe(&rejected()), WatchAction::AlarmRejected);
        // Repeated rejection: no new increment.
        assert_eq!(a.observe(&rejected()), WatchAction::Idle);
        // A different bad outcome while already alarmed also does not re-fire.
        assert_eq!(a.observe(&RotationOutcome::Incomplete), WatchAction::Idle);
    }

    #[test]
    fn watch_alarm_recovers_and_publishes_via_rotated() {
        let mut a = WatchAlarm::default();
        assert_eq!(a.observe(&rejected()), WatchAction::AlarmRejected);
        // A valid change clears the alarm and publishes.
        assert_eq!(
            a.observe(&RotationOutcome::Rotated { generation: 2 }),
            WatchAction::Publish { recovered: true }
        );
        // A later healthy change publishes without a recovery log.
        assert_eq!(
            a.observe(&RotationOutcome::Rotated { generation: 3 }),
            WatchAction::Publish { recovered: false }
        );
    }

    #[test]
    fn watch_alarm_healthy_never_alarms() {
        let mut a = WatchAlarm::default();
        // A change in progress is neither an alarm nor a publish.
        assert_eq!(a.observe(&RotationOutcome::Debouncing), WatchAction::Idle);
        assert_eq!(
            a.observe(&RotationOutcome::Rotated { generation: 1 }),
            WatchAction::Publish { recovered: false }
        );
        assert_eq!(a.observe(&RotationOutcome::NoChange), WatchAction::Idle);
    }

    #[tokio::test]
    async fn shutdown_cancels_and_joins_manager_promptly() {
        use checkpoints::MemCheckpointStore;

        let dir = std::env::temp_dir().join(format!(
            "df-rot-shutdown-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();

        let spec = Arc::new(PgRotationSpec {
            fields: vec![CredentialFieldRequest::new(
                "dsn",
                SecretReference::new(
                    SecretProvider::File,
                    dir.join("dsn").to_str().unwrap(),
                ),
            )],
            composition: RotationComposition::WholeDsn {
                field: "dsn".to_string(),
            },
            trusted_root: dir.clone(),
            max_size: 1 << 20,
            debounce: Duration::from_millis(50),
            poll_interval: Duration::from_millis(50),
            apply_timeout: Duration::from_secs(1),
        });
        let chkpt: Arc<dyn CheckpointStore> =
            Arc::new(MemCheckpointStore::new().unwrap());
        let cancel = CancellationToken::new();
        let rt = RotationRuntime::spawn(
            &spec,
            ProtectedDsn::from("host=h dbname=d"),
            "s".to_string(),
            "p".to_string(),
            "slot".to_string(),
            "pub".to_string(),
            chkpt,
            &cancel,
        )
        .unwrap();

        // shutdown must cancel the manager and join it promptly.
        tokio::time::timeout(Duration::from_secs(5), rt.shutdown())
            .await
            .expect("shutdown should cancel and join the manager promptly");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
