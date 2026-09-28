//! Source-agnostic wiring that drives the credential-rotation core
//! ([`crate::rotation`]) from a running source's run loop. Shared by the
//! PostgreSQL and MySQL sources so the projected-volume resolver policy, fail-closed
//! spec construction, watcher alarms, protected-DSN handling, metrics, and async
//! task teardown are defined once.
//!
//! What lives here:
//! - [`build_spec`]: fail-closed, canonicalizing construction of a [`RotationSpec`]
//!   from a source's `dsn`/`dsn_secret`/`credentials`/`rotation` config, supporting
//!   mixed env/file credentials.
//! - [`WatchAlarm`]: an edge detector that turns a per-poll stream of watcher
//!   outcomes into one alarm (and one metric) per state transition, with recovery.
//! - [`RotationManager`]: the coordinator + file-watcher manager task + in-flight
//!   preflight lifecycle. A source wraps it and supplies only the source-specific
//!   Stage-A preflight and Stage-B apply (the connection handling).
//!
//! The source-specific pieces (identity/position preflight, closing the old stream,
//! opening the replacement) stay in each source's own `*_rotation.rs`.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime};

use deltaforge_config::{CredentialRotationCfg, SourceCredentialsCfg};
use deltaforge_core::{SourceError, SourceResult};
use metrics::{counter, gauge};
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
    RotationComposition, RotationCoordinator, RotationReject, compose,
    earliest_expiry,
};

pub(crate) fn fail_closed(
    details: impl Into<std::borrow::Cow<'static, str>>,
) -> SourceError {
    SourceError::Incompatible {
        details: details.into(),
    }
}

/// Whether a rejection is worth a bounded retry (transient outage) versus permanent
/// suppression (wrong server/database, position irrecoverable, or expired).
pub(crate) fn is_transient(reject: RotationReject) -> bool {
    matches!(
        reject,
        RotationReject::PreflightFailed | RotationReject::ReplacementOpenFailed
    )
}

/// What the caller should do after an apply outcome is recorded.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ApplyStep {
    /// Replacement opened: swap the runtime DSN and continue.
    Applied,
    /// Current credentials retained (recheck failed, or bad replacement recovered).
    Kept,
}

/// Everything needed to run controlled rotation for a source, derived from config at
/// build time. Held behind an `Arc` on the source because it embeds non-`Clone`
/// composition material (a fixed env value is a `SecretString`). Opaque: an
/// implementation handle with no public fields.
pub struct RotationSpec {
    /// Watched credential fields (file-backed references only). Never printed:
    /// [`RotationSpec`]'s `Debug` redacts the references.
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

impl std::fmt::Debug for RotationSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Redact the watched references; expose only non-secret shape/timing.
        f.debug_struct("RotationSpec")
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

/// Build a rotation spec from a source's credential config (source-agnostic).
///
/// Fail-closed: `rotation` absent yields `Ok(None)`, but rotation configured with
/// nothing file-backed to watch, a reference outside the trusted root, an
/// unsupported provider, or missing required data yields an `Err` at startup rather
/// than silently disabling the requested safety feature. Mixed env/file credentials
/// are supported: the env field is resolved once as fixed and the file field is
/// watched. `db` governs the credential-injection form used by `compose`.
pub(crate) async fn build_spec(
    dsn: Option<&str>,
    dsn_secret: Option<&SecretReference>,
    credentials: Option<&SourceCredentialsCfg>,
    rotation: Option<&CredentialRotationCfg>,
    db: DbKind,
    resolver: &dyn SecretResolver,
) -> SourceResult<Option<Arc<RotationSpec>>> {
    let Some(rot) = rotation else {
        return Ok(None);
    };
    let max_size = rot.max_secret_bytes;
    let trusted_root = rot.trusted_root.clone();
    let debounce = Duration::from_millis(rot.debounce_ms);
    let poll_interval = Duration::from_millis(rot.poll_interval_ms);
    let apply_timeout = Duration::from_millis(rot.apply_timeout_ms);

    // Whole-DSN secret.
    if let Some(dsn_ref) = dsn_secret {
        return match dsn_ref.provider {
            SecretProvider::File => {
                validate_watched_path(&dsn_ref.location, &trusted_root).await?;
                let fields =
                    vec![CredentialFieldRequest::new("dsn", dsn_ref.clone())];
                let composition = RotationComposition::WholeDsn {
                    field: "dsn".to_string(),
                };
                Ok(Some(Arc::new(RotationSpec {
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
    let creds = credentials.ok_or_else(|| {
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
    let base_dsn = dsn.map(str::to_string).ok_or_else(|| {
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
        db,
        base_dsn,
        username: username_src,
        password: password_src,
    };
    Ok(Some(Arc::new(RotationSpec {
        fields,
        composition,
        trusted_root,
        max_size,
        debounce,
        poll_interval,
        apply_timeout,
    })))
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

/// An in-flight Stage-A preflight for one candidate generation.
struct Preflight {
    generation: u64,
    candidate: Candidate,
    handle: JoinHandle<Result<(), RotationReject>>,
    /// Set once the handle has resolved (or panicked); harvested at the boundary.
    done: Option<Result<(), RotationReject>>,
}

/// A completed preflight ready to apply at the next transaction boundary.
pub(crate) struct ReadyPreflight {
    pub(crate) generation: u64,
    pub(crate) candidate: Candidate,
    pub(crate) result: Result<(), RotationReject>,
}

/// The coordinator + file-watcher manager task + in-flight preflight lifecycle.
/// Source-agnostic: a source's `*_rotation.rs` owns one of these and supplies the
/// source-specific Stage-A preflight (spawned via [`RotationManager::drive_preflight`])
/// and Stage-B apply (recorded via [`RotationManager::finish_apply`]).
pub(crate) struct RotationManager {
    coordinator: RotationCoordinator,
    /// The file-watcher/manager task publishing candidates; a child of the source
    /// cancel token so it dies with the source.
    manager: JoinHandle<()>,
    running: Arc<AtomicBool>,
    child_cancel: CancellationToken,
    source_id: String,
    pipeline: String,
    apply_timeout: Duration,
    preflight: Option<Preflight>,
}

impl RotationManager {
    /// Spawn the file-watcher manager and build the manager. `initial_dsn` is the
    /// startup DSN; the file-watcher manager suppresses a first candidate that
    /// composes to it, so the initial projected-volume load does not trigger a
    /// needless reconnect.
    pub(crate) fn spawn(
        spec: &Arc<RotationSpec>,
        initial_dsn: ProtectedDsn,
        source_id: String,
        pipeline: String,
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
            apply_timeout: spec.apply_timeout,
            preflight: None,
        })
    }

    pub(crate) fn apply_timeout(&self) -> Duration {
        self.apply_timeout
    }

    pub(crate) fn child_cancel(&self) -> CancellationToken {
        self.child_cancel.clone()
    }

    pub(crate) fn source_id(&self) -> &str {
        &self.source_id
    }

    pub(crate) fn pipeline(&self) -> &str {
        &self.pipeline
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
    /// preflight (via `spawn`, which receives the candidate and returns the task
    /// handle) concurrently with the live stream. A newer generation supersedes and
    /// aborts an older preflight. An expired candidate is a terminal rejection.
    pub(crate) fn drive_preflight(
        &mut self,
        spawn: impl FnOnce(&Candidate) -> JoinHandle<Result<(), RotationReject>>,
    ) {
        match self
            .coordinator
            .take_eligible(Instant::now(), SystemTime::now())
        {
            Eligibility::Ready(candidate) => {
                // Supersede any older, now-obsolete preflight.
                if let Some(old) = self.preflight.take() {
                    old.handle.abort();
                }
                let generation = candidate.generation;
                let handle = spawn(&candidate);
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

    /// Harvest a completed preflight, if one is ready to apply. Returns `None` while
    /// no preflight has resolved.
    pub(crate) fn take_ready(&mut self) -> Option<ReadyPreflight> {
        let ready = matches!(&self.preflight, Some(pf) if pf.done.is_some());
        if !ready {
            return None;
        }
        let pf = self.preflight.take().expect("checked ready");
        Some(ReadyPreflight {
            generation: pf.generation,
            candidate: pf.candidate,
            result: pf.done.expect("checked done"),
        })
    }

    /// Record a failed Stage-A preflight: transient failures schedule a retry,
    /// terminal ones suppress the generation. Emits the matching metric and a
    /// redacted warning; the active credentials are retained.
    pub(crate) fn record_preflight_failure(
        &mut self,
        generation: u64,
        reject: RotationReject,
    ) {
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
    }

    /// Record a Stage-B [`ApplyOutcome`], emit the matching metric, and decide the
    /// run loop's next step. `CloseUncertain`/`FailedClosed` map to a fatal error
    /// (the caller must stop without advancing the checkpoint).
    pub(crate) fn finish_apply(
        &mut self,
        generation: u64,
        outcome: ApplyOutcome,
    ) -> SourceResult<ApplyStep> {
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
        let step =
            record_and_decide(&mut self.coordinator, generation, outcome);
        match &step {
            Ok(ApplyStep::Applied) => {
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
        step
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

impl Drop for RotationManager {
    /// Backstop for exit paths that do not call [`RotationManager::shutdown`] (for
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

/// Record an [`ApplyOutcome`] against the coordinator and decide the run loop's next
/// step. `CloseUncertain`/`FailedClosed` map to a fatal error; the caller must stop
/// without advancing the checkpoint. Pure w.r.t. the connection so it is
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
                "credential rotation close-uncertain: old stream closure \
                 unconfirmed"
            )))
        }
        ApplyOutcome::FailedClosed => {
            let _ = coordinator.record_terminal_rejection(generation);
            Err(SourceError::Other(anyhow::anyhow!(
                "credential rotation failed closed: no stream reconnected"
            )))
        }
    }
}

/// Spawn the file-watcher manager: poll the projected volume, observe every outcome,
/// and on each validated change compose a candidate DSN and publish it. Suppresses a
/// candidate identical to `initial_dsn` (or the previous candidate) so no needless
/// reconnect fires. DSNs are compared as protected values via
/// [`ProtectedDsn::same_dsn`]; no plaintext is retained. Invalid material raises one
/// alarm + one metric per transition and never publishes a candidate.
#[allow(clippy::too_many_arguments)]
fn spawn_manager(
    mut watcher: FileWatcher,
    spec: Arc<RotationSpec>,
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
        let dir = std::env::temp_dir().join(format!(
            "df-rotmgr-shutdown-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();

        let spec = Arc::new(RotationSpec {
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
        let cancel = CancellationToken::new();
        let mgr = RotationManager::spawn(
            &spec,
            ProtectedDsn::from("host=h dbname=d"),
            "s".to_string(),
            "p".to_string(),
            &cancel,
        )
        .unwrap();

        tokio::time::timeout(Duration::from_secs(5), mgr.shutdown())
            .await
            .expect("shutdown should cancel and join the manager promptly");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
