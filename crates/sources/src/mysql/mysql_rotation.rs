//! MySQL-specific credential-rotation operations, plugged into the source-agnostic
//! [`crate::rotation_manager`] (watcher/manager task, coordinator, alarms, metrics,
//! teardown) and the core in [`crate::rotation`].
//!
//! MySQL-specific gates:
//! - **GTID mode required** ([`require_gtid_mode`]): rotation is refused at startup
//!   unless `@@GLOBAL.gtid_mode = ON`. There is no file/position downgrade for the
//!   rotation safety boundary.
//! - **Stage-A / boundary recheck** ([`preflight`]): validate `server_uuid` against
//!   the durable identity authority, and the frozen accumulated GTID set against
//!   `@@GLOBAL.gtid_executed` (`GTID_SUBSET`). Transient authority/read failures are
//!   retryable; identity or GTID incompatibility is terminal.
//! - **Close-before-open, actively confirmed**: the old binlog client is
//!   bidirectionally shut down (`BinlogStream::close` -> TCP `shutdown(Both)`) and
//!   that shutdown must succeed before the replacement opens - the old consumer is
//!   closed on our initiative, never fenced by a server_id collision.
//! - **Reconnect from the exact accumulated GTID set** ([`open_binlog`]): the
//!   replacement resumes from the frozen `gtid_executed`-relative set; file/position
//!   fallback state is preserved but is never the rotation boundary.

use std::sync::Arc;
use std::time::SystemTime;

use common::RetryPolicy;
use deltaforge_config::MysqlSrcCfg;
use deltaforge_core::{SourceError, SourceResult};
use mysql_async::prelude::Queryable;
use mysql_binlog_connector_rust::binlog_client::BinlogClient;
use secrets::SecretResolver;
use storage::ArcStorageBackend;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::failover::identity::{IdentityStore, ServerIdentity};
use crate::rotation::{DbKind, RotationReject, apply_two_stage};
use crate::rotation_manager::{
    ApplyStep, ReadyPreflight, RotationManager, RotationSpec, fail_closed,
};

use std::time::Duration;

use super::mysql_control::{Class, ControlError, ControlPool};
use super::mysql_helpers::{Opened, connect_binlog_with_retries};
use super::mysql_session::SessionError;
use super::{HEARTBEAT_INTERVAL_SECS, READ_TIMEOUT, RunCtx};

// ----------------------------------------------------------------------------
// Control-plane queries (the source's control pool; never touch the binlog
// stream)
// ----------------------------------------------------------------------------

async fn query_gtid_mode(
    control: &ControlPool,
    dsn: &str,
    expected_uuid: &str,
) -> Result<Option<String>, SourceError> {
    let mut conn = control
        .connect(Class::Position, dsn, expected_uuid, Class::Position.wait())
        .await
        .map_err(|e| e.into_source_error(expected_uuid))?;
    let row: Result<Option<(Option<String>,)>, _> =
        conn.query_first("SELECT @@GLOBAL.gtid_mode").await;
    if row.is_ok() {
        conn.release().await;
    }
    let row = row.map_err(|e| SourceError::Connect {
        details: format!("rotation gtid_mode check failed: {e}").into(),
    })?;
    Ok(row.and_then(|(s,)| s))
}

/// Whether the frozen accumulated GTID set is a subset of the server's executed set
/// (the replacement has executed everything we have consumed - we are not ahead).
/// Asked on a connection whose identity the caller verified.
async fn gtid_set_is_subset(
    conn: &mut mysql_async::Conn,
    frozen: &str,
) -> anyhow::Result<bool> {
    let row: Option<(Option<i64>,)> = conn
        .exec_first("SELECT GTID_SUBSET(?, @@GLOBAL.gtid_executed)", (frozen,))
        .await?;
    Ok(matches!(row, Some((Some(1),))))
}

/// Startup gate: rotation requires the server to run in GTID mode. Fails closed if
/// `@@GLOBAL.gtid_mode` is not `ON`, or if it cannot be read on a connection
/// verified as `expected_uuid`.
pub(crate) async fn require_gtid_mode(
    control: &ControlPool,
    dsn: &str,
    expected_uuid: &str,
) -> SourceResult<()> {
    match query_gtid_mode(control, dsn, expected_uuid).await {
        Ok(Some(mode)) if mode.eq_ignore_ascii_case("ON") => Ok(()),
        Ok(mode) => Err(fail_closed(format!(
            "controlled credential rotation requires @@GLOBAL.gtid_mode = ON, \
             but the server reports {}; enable GTID mode or remove `rotation`",
            mode.as_deref().unwrap_or("an unknown value")
        ))),
        Err(e) => Err(e),
    }
}

/// Stage-A preflight for a replacement DSN, on ONE fresh control connection
/// that first proves it is the server of the durable identity authority; the
/// frozen accumulated GTID set is then checked against `@@GLOBAL.gtid_executed`
/// on that same connection. A transient authority/read failure is retryable
/// (`PreflightFailed`); a different server (`IdentityMismatch`) or a divergent
/// GTID set (`PositionIncompatible`) is terminal.
pub async fn preflight(
    new_dsn: &str,
    source_id: &str,
    backend: &ArcStorageBackend,
    frozen_gtid: Option<&str>,
) -> Result<(), RotationReject> {
    let control = ControlPool::standalone(source_id);
    preflight_on(&control, new_dsn, source_id, backend, frozen_gtid).await
}

/// [`preflight`] on the source's control pool.
async fn preflight_on(
    control: &ControlPool,
    new_dsn: &str,
    source_id: &str,
    backend: &ArcStorageBackend,
    frozen_gtid: Option<&str>,
) -> Result<(), RotationReject> {
    // Identity: the replacement must be the server of the durable
    // IdentityStore, proven on the connection that answers the position check.
    let expected = match IdentityStore::new(Arc::clone(backend))
        .load(source_id)
        .await
    {
        // Store unreachable: retry rather than permanently suppress the candidate.
        Err(_) => return Err(RotationReject::PreflightFailed),
        Ok(Some(ServerIdentity::MySql(id))) => id.server_uuid,
        // No durable identity to prove against: terminal.
        Ok(_) => return Err(RotationReject::IdentityMismatch),
    };
    let mut conn = match control
        .connect(Class::Position, new_dsn, &expected, Class::Position.wait())
        .await
        .map_err(ControlError::into_session)
    {
        Ok(conn) => conn,
        Err(SessionError::OtherServer { .. }) => {
            return Err(RotationReject::IdentityMismatch);
        }
        // Unreachable, saturated, or no uuid reported yet: transient,
        // retryable.
        Err(SessionError::Connect(_) | SessionError::NoIdentity(_)) => {
            return Err(RotationReject::PreflightFailed);
        }
    };

    // Position: the frozen accumulated GTID set must be a subset of the server's
    // executed set (we are not ahead of the replacement). A read failure is
    // transient; a proven non-subset is terminal.
    if let Some(frozen) = frozen_gtid {
        if !frozen.is_empty() {
            let subset = gtid_set_is_subset(&mut conn, frozen).await;
            if subset.is_ok() {
                conn.release().await;
            }
            match subset {
                Err(_) => return Err(RotationReject::PreflightFailed),
                Ok(true) => {}
                Ok(false) => return Err(RotationReject::PositionIncompatible),
            }
            return Ok(());
        }
    }
    conn.release().await;
    Ok(())
}

/// How long a replacement stream may take to deliver its first event.
pub(crate) const CONFIRM_TIMEOUT: Duration = Duration::from_secs(10);

/// Stage-B confirmation: a replacement stream proves the server accepted its
/// dump by delivering its first decoded event within `bound`. Connecting is
/// not enough: a server refuses the dump of a user without the replication
/// privilege only on the stream's first read. A read error (that refusal, a
/// protocol error, a decode failure) or the bound passing refuses the
/// replacement, with no credential in the reason. The event is returned,
/// to be handed on exactly once ([`hand_on`]).
pub(crate) async fn confirm_first_event<F>(
    first: F,
    bound: Duration,
) -> Result<
    (
        mysql_binlog_connector_rust::event::event_header::EventHeader,
        mysql_binlog_connector_rust::event::event_data::EventData,
    ),
    RotationReject,
>
where
    F: std::future::Future<
            Output = Result<
                (
                    mysql_binlog_connector_rust::event::event_header::EventHeader,
                    mysql_binlog_connector_rust::event::event_data::EventData,
                ),
                mysql_binlog_connector_rust::binlog_error::BinlogError,
            >,
        >,
{
    match tokio::time::timeout(bound, first).await {
        Ok(Ok(event)) => Ok(event),
        Ok(Err(_)) | Err(_) => Err(RotationReject::ReplacementOpenFailed),
    }
}

/// Hand the confirmed replacement's first event to the run loop: it is
/// read next, exactly once, through the normal path (position tracking,
/// then dispatch). Rotation applies only with nothing queued, so it is the
/// queue's only event.
pub(crate) fn hand_on(
    prefetched: &mut std::collections::VecDeque<
        super::mysql_window_speculation::Held,
    >,
    next_held_seq: &mut u64,
    (header, data): (
        mysql_binlog_connector_rust::event::event_header::EventHeader,
        mysql_binlog_connector_rust::event::event_data::EventData,
    ),
) {
    debug_assert!(prefetched.is_empty(), "rotation applied with events queued");
    prefetched.push_front((
        std::time::Instant::now(),
        *next_held_seq,
        header,
        data,
    ));
    *next_held_seq += 1;
}

/// Open a binlog stream for `dsn` resuming from the exact frozen accumulated GTID
/// set, via the production connect path (same server_id, heartbeat/timeout,
/// retry policy and session identity verification as startup/reconnect). A
/// session that reached another server is an `IdentityMismatch`.
#[allow(clippy::too_many_arguments)]
async fn open_binlog(
    control: &ControlPool,
    dsn: &str,
    expected_uuid: &str,
    source_id: &str,
    server_id: u64,
    frozen_gtid: &str,
    default_db: &str,
    cancel: &CancellationToken,
) -> Result<super::mysql_session::Session, RotationReject> {
    let url = dsn.to_string();
    let gtid = frozen_gtid.to_string();
    let make_client = move || BinlogClient {
        url: url.clone(),
        server_id,
        heartbeat_interval_secs: HEARTBEAT_INTERVAL_SECS,
        timeout_secs: READ_TIMEOUT,
        gtid_enabled: true,
        gtid_set: gtid.clone(),
        ..Default::default()
    };
    match connect_binlog_with_retries(
        source_id,
        expected_uuid,
        make_client,
        cancel,
        default_db,
        RetryPolicy::default(),
    )
    .await
    {
        // Open, nothing read: refused unless the binlog is complete (the
        // server may have restarted with another configuration). The kept
        // stream, if the server did restart, ends and reconnects through
        // the same check.
        Ok(Opened::Stream(stream)) => {
            match super::require_complete_binlog(control, dsn, expected_uuid)
                .await
            {
                Ok(()) => Ok(stream),
                Err(_) => Err(RotationReject::ReplacementOpenFailed),
            }
        }
        Ok(Opened::OtherServer(_)) | Err(SourceError::Lineage { .. }) => {
            Err(RotationReject::IdentityMismatch)
        }
        Err(_) => Err(RotationReject::ReplacementOpenFailed),
    }
}

/// Build a MySQL rotation spec from source config (fail-closed; see
/// [`crate::rotation_manager::build_spec`]). Requires GTID mode is validated
/// separately at startup ([`require_gtid_mode`]).
pub async fn build_spec(
    cfg: &MysqlSrcCfg,
    resolver: Arc<dyn SecretResolver>,
) -> SourceResult<Option<Arc<RotationSpec>>> {
    crate::rotation_manager::build_spec(
        cfg.dsn.as_deref(),
        cfg.dsn_secret.as_ref(),
        cfg.credentials.as_ref(),
        cfg.rotation.as_ref(),
        DbKind::Mysql,
        resolver,
    )
    .await
}

/// Runtime the MySQL run loop drives to apply credential rotation. Thin wrapper over
/// the shared [`RotationManager`] with the MySQL Stage-A/Stage-B logic. Unlike
/// PostgreSQL (whose client lives behind an `Arc<Mutex>` in `RunCtx`), the MySQL
/// binlog stream is owned by the run loop: [`apply_at_boundary`] takes it by value
/// and returns the replacement (or the retained/recovered stream).
pub(crate) struct MySqlRotationRuntime {
    mgr: RotationManager,
    /// The source's control pool: preflights and checks draw on it, and it
    /// is drained once the new credentials are applied.
    control: ControlPool,
    backend: ArcStorageBackend,
    server_id: u64,
    default_db: String,
}

impl MySqlRotationRuntime {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn spawn(
        spec: &Arc<RotationSpec>,
        initial_dsn: crate::credentials::ProtectedDsn,
        source_id: String,
        pipeline: String,
        control: ControlPool,
        backend: ArcStorageBackend,
        server_id: u64,
        default_db: String,
        parent_cancel: &CancellationToken,
    ) -> SourceResult<Self> {
        Ok(Self {
            mgr: RotationManager::spawn(
                spec,
                initial_dsn,
                source_id,
                pipeline,
                parent_cancel,
            )?,
            control,
            backend,
            server_id,
            default_db,
        })
    }

    /// Stage A: schedule preflight for the latest eligible candidate on a control
    /// connection, using the current accumulated GTID set as the frozen boundary.
    pub(crate) fn drive_preflight(&mut self, ctx: &RunCtx) {
        let source_id = self.mgr.source_id().to_string();
        let backend = Arc::clone(&self.backend);
        let frozen = ctx.gtid_text();
        let control = self.control.clone();
        self.mgr.drive_preflight(|candidate| {
            let dsn = candidate.dsn.clone();
            tokio::spawn(async move {
                preflight_on(
                    &control,
                    dsn.expose(),
                    &source_id,
                    &backend,
                    frozen.as_deref(),
                )
                .await
            })
        });
    }

    pub(crate) async fn wait_activity(&mut self) {
        self.mgr.wait_activity().await
    }

    /// Stage B: apply a completed preflight at the current transaction boundary. The
    /// caller must ensure `ctx.current_gtid.is_none()` (between transactions) and
    /// `ctx.last_gtid.is_some()` (a GTID boundary exists). Takes the live stream by
    /// value and returns the resulting stream and whether it was replaced; a fatal
    /// outcome returns an error so the run loop stops without advancing the
    /// checkpoint. `lost`: the stream already ended (the server closed it), so
    /// there is no reader to shut down.
    pub(crate) async fn apply_at_boundary(
        &mut self,
        ctx: &mut RunCtx,
        stream: super::mysql_session::Session,
        lost: bool,
    ) -> SourceResult<(super::mysql_session::Session, bool)> {
        let Some(ready) = self.mgr.take_ready() else {
            return Ok((stream, false));
        };
        let ReadyPreflight {
            generation,
            candidate,
            result,
        } = ready;
        if let Err(reject) = result {
            self.mgr.record_preflight_failure(generation, reject);
            return Ok((stream, false));
        }

        // The live stream is moved into a shared holder so the Stage-B closures can
        // close it and install the replacement; the resulting stream is taken back
        // out afterwards.
        let holder = Arc::new(Mutex::new(Some(stream)));
        let replaced = Arc::new(std::sync::atomic::AtomicBool::new(false));
        // The confirmed replacement's first event, handed on only once the
        // rotation is applied.
        let confirmed = Arc::new(Mutex::new(None));
        // Both the replacement and a recovery session must prove they are the
        // verified server before their dump command.
        let expected = ctx.expected_uuid()?;
        let frozen_gtid = ctx.gtid_text().unwrap_or_default();
        let new_dsn = candidate.dsn.clone();
        let old_dsn = ctx.dsn.current();
        let source_id = self.mgr.source_id().to_string();
        let backend = Arc::clone(&self.backend);
        let server_id = self.server_id;
        let default_db = self.default_db.clone();
        let cancel = self.mgr.child_cancel();
        let apply_timeout = self.mgr.apply_timeout();

        // recheck: candidate not expired, fresh re-preflight against the frozen
        // accumulated GTID set immediately before the handoff.
        let recheck = {
            let cand = candidate.clone();
            let new_dsn = new_dsn.clone();
            let source_id = source_id.clone();
            let backend = Arc::clone(&backend);
            let frozen = frozen_gtid.clone();
            let control = self.control.clone();
            move || async move {
                if cand.is_expired(SystemTime::now()) {
                    return Err(RotationReject::Expired);
                }
                preflight_on(
                    &control,
                    new_dsn.expose(),
                    &source_id,
                    &backend,
                    Some(&frozen),
                )
                .await
            }
        };

        // close_old: bidirectionally shut down the old binlog client's socket
        // (`BinlogStream::close` -> TCP `shutdown(Both)`). This confirms the LOCAL
        // reader is gone - not that the server-side dump-thread record has already
        // disappeared (it lingers until the server notices the closed socket). The
        // safety argument holds regardless: no local reader survives to consume a
        // second stream, so we do not rely on a server_id collision to fence the
        // old connection. A shutdown error leaves closure unconfirmed ->
        // CloseUncertain.
        let close_old = {
            let holder = Arc::clone(&holder);
            move || async move {
                let old = {
                    let mut guard = holder.lock().await;
                    guard.take()
                };
                if let Some(mut stream) = old
                    && !lost
                {
                    stream
                        .close()
                        .await
                        .map_err(|_| RotationReject::ReplacementOpenFailed)?;
                    // Drop after the confirmed shutdown closes both directions.
                    drop(stream);
                }
                Ok::<(), RotationReject>(())
            }
        };

        // open_new: replacement stream from the frozen GTID set with the new creds.
        let open_new = {
            let replaced = Arc::clone(&replaced);
            let confirmed = Arc::clone(&confirmed);
            let expected = expected.clone();
            let holder = Arc::clone(&holder);
            let new_dsn = new_dsn.clone();
            let source_id = source_id.clone();
            let default_db = default_db.clone();
            let cancel = cancel.clone();
            let frozen = frozen_gtid.clone();
            let control = self.control.clone();
            move || async move {
                let mut s = open_binlog(
                    &control,
                    new_dsn.expose(),
                    &expected,
                    &source_id,
                    server_id,
                    &frozen,
                    &default_db,
                    &cancel,
                )
                .await?;
                // The candidate must deliver its first event before it can
                // replace anything; refused, it is closed and the old
                // credentials recover the stream.
                let first = match confirm_first_event(s.read(), CONFIRM_TIMEOUT)
                    .await
                {
                    Ok(first) => first,
                    Err(reject) => {
                        s.close().await.ok();
                        drop(s);
                        return Err(reject);
                    }
                };
                *confirmed.lock().await = Some(first);
                *holder.lock().await = Some(s);
                replaced.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok::<(), RotationReject>(())
            }
        };

        // open_old: bounded recovery to the old creds if the replacement fails.
        let open_old = {
            let replaced = Arc::clone(&replaced);
            let expected = expected.clone();
            let holder = Arc::clone(&holder);
            let old_dsn = old_dsn.clone();
            let source_id = source_id.clone();
            let default_db = default_db.clone();
            let cancel = cancel.clone();
            let frozen = frozen_gtid.clone();
            let control = self.control.clone();
            move || async move {
                let s = open_binlog(
                    &control,
                    old_dsn.expose(),
                    &expected,
                    &source_id,
                    server_id,
                    &frozen,
                    &default_db,
                    &cancel,
                )
                .await?;
                *holder.lock().await = Some(s);
                replaced.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok::<(), RotationReject>(())
            }
        };

        let outcome = apply_two_stage(
            apply_timeout,
            recheck,
            close_old,
            open_new,
            open_old,
        )
        .await;

        if let ApplyStep::Applied =
            self.mgr.finish_apply(generation, outcome)?
        {
            // The run's one authoritative DSN: every later connection of
            // the run (stream, captures, scans, schema reads, kills) takes
            // it. Replaced only here, once the new stream is applied.
            ctx.dsn.replace(candidate.dsn.clone());
            // Nothing opened with the old credentials is used again: the
            // idle connection closes now, those in use when returned.
            self.control
                .drain(super::mysql_control::DRAIN_TIMEOUT)
                .await;
            if let Some(first) = confirmed.lock().await.take() {
                hand_on(&mut ctx.prefetched, &mut ctx.next_held_seq, first);
            }
        }

        // Take the resulting stream back out (Applied: new; Kept: old/recovered).
        let stream = holder
            .lock()
            .await
            .take()
            .ok_or_else(|| fail_closed("rotation left no binlog stream"))?;
        Ok((stream, replaced.load(std::sync::atomic::Ordering::SeqCst)))
    }

    pub(crate) async fn shutdown(self) {
        self.mgr.shutdown().await
    }
}

#[cfg(test)]
mod confirm_tests {
    fn first_event() -> (
        mysql_binlog_connector_rust::event::event_header::EventHeader,
        mysql_binlog_connector_rust::event::event_data::EventData,
    ) {
        (
            mysql_binlog_connector_rust::event::event_header::EventHeader {
                timestamp: 0,
                event_type: 4,
                server_id: 1,
                event_length: 44,
                next_event_position: 0,
                event_flags: 0x20,
            },
            mysql_binlog_connector_rust::event::event_data::EventData::NotSupported,
        )
    }

    /// A replacement's first decoded event confirms it, and is returned
    /// intact to be handed on.
    #[tokio::test]
    async fn a_first_event_confirms_the_replacement() {
        let (header, _) = super::confirm_first_event(
            async { Ok(first_event()) },
            std::time::Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert_eq!((header.event_type, header.event_length), (4, 44));
    }

    /// An authorization refusal, a protocol error or a first event that does
    /// not decode refuses the replacement, with no credential in the reason.
    #[tokio::test]
    async fn a_refused_or_malformed_first_event_refuses_the_replacement() {
        use mysql_binlog_connector_rust::binlog_error::BinlogError;
        for err in [
            BinlogError::ConnectError(
                "Access denied; you need (at least one of) the REPLICATION SLAVE privilege(s)".into(),
            ),
            BinlogError::UnexpectedData("malformed first event".into()),
        ] {
            let r = super::confirm_first_event(
                async move { Err(err) },
                std::time::Duration::from_secs(1),
            )
            .await;
            assert!(
                matches!(r, Err(crate::rotation::RotationReject::ReplacementOpenFailed)),
                "{r:?}"
            );
        }
    }

    /// A replacement that never delivers an event is refused at the bound.
    #[tokio::test(start_paused = true)]
    async fn a_silent_replacement_is_refused_at_the_bound() {
        let r = super::confirm_first_event(
            std::future::pending(),
            super::CONFIRM_TIMEOUT,
        )
        .await;
        assert!(matches!(
            r,
            Err(crate::rotation::RotationReject::ReplacementOpenFailed)
        ));
    }

    /// The confirmed first event is queued exactly once, as the next held
    /// event, for the run loop to read through its normal path.
    #[test]
    fn the_confirmed_event_is_handed_on_once() {
        let mut queue = std::collections::VecDeque::new();
        let mut next = 7;
        super::hand_on(&mut queue, &mut next, first_event());
        assert_eq!(queue.len(), 1);
        let (_, seq, header, _) = queue.pop_front().unwrap();
        assert_eq!((seq, next, header.event_type), (7, 8, 4));
    }
}
