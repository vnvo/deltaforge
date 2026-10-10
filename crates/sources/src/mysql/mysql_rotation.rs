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
use mysql_binlog_connector_rust::{
    binlog_client::BinlogClient, binlog_stream::BinlogStream,
};
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

use super::mysql_helpers::{Opened, connect_binlog_with_retries};
use super::mysql_session::{SessionError, open_control_connection};
use super::{HEARTBEAT_INTERVAL_SECS, READ_TIMEOUT, RunCtx};

// ----------------------------------------------------------------------------
// Control-plane queries (own short-lived pool; never touch the binlog stream)
// ----------------------------------------------------------------------------

async fn query_gtid_mode(
    dsn: &str,
    expected_uuid: &str,
) -> Result<Option<String>, SourceError> {
    let mut conn =
        open_control_connection(dsn, expected_uuid, Duration::from_secs(10))
            .await
            .map_err(|e| e.into_source_error(expected_uuid))?;
    let row: Result<Option<(Option<String>,)>, _> =
        conn.query_first("SELECT @@GLOBAL.gtid_mode").await;
    conn.disconnect().await.ok();
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
    dsn: &str,
    expected_uuid: &str,
) -> SourceResult<()> {
    match query_gtid_mode(dsn, expected_uuid).await {
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
    let mut conn = match open_control_connection(
        new_dsn,
        &expected,
        Duration::from_secs(10),
    )
    .await
    {
        Ok(conn) => conn,
        Err(SessionError::OtherServer { .. }) => {
            return Err(RotationReject::IdentityMismatch);
        }
        // Unreachable, or no uuid reported yet: transient, retryable.
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
            conn.disconnect().await.ok();
            match subset {
                Err(_) => return Err(RotationReject::PreflightFailed),
                Ok(true) => {}
                Ok(false) => return Err(RotationReject::PositionIncompatible),
            }
            return Ok(());
        }
    }
    conn.disconnect().await.ok();
    Ok(())
}

/// Open a binlog stream for `dsn` resuming from the exact frozen accumulated GTID
/// set, via the production connect path (same server_id, heartbeat/timeout,
/// retry policy and session identity verification as startup/reconnect). A
/// session that reached another server is an `IdentityMismatch`.
async fn open_binlog(
    dsn: &str,
    expected_uuid: &str,
    source_id: &str,
    server_id: u64,
    frozen_gtid: &str,
    default_db: &str,
    cancel: &CancellationToken,
) -> Result<BinlogStream, RotationReject> {
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
            match super::require_complete_binlog(dsn, expected_uuid).await {
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
        let frozen = ctx.last_gtid.clone();
        self.mgr.drive_preflight(|candidate| {
            let dsn = candidate.dsn.clone();
            tokio::spawn(async move {
                preflight(dsn.expose(), &source_id, &backend, frozen.as_deref())
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
        stream: BinlogStream,
        lost: bool,
    ) -> SourceResult<(BinlogStream, bool)> {
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
        // Both the replacement and a recovery session must prove they are the
        // verified server before their dump command.
        let expected = ctx.expected_uuid()?;
        let frozen_gtid = ctx.last_gtid.clone().unwrap_or_default();
        let new_dsn = candidate.dsn.clone();
        let old_dsn = ctx.dsn.clone();
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
            move || async move {
                if cand.is_expired(SystemTime::now()) {
                    return Err(RotationReject::Expired);
                }
                preflight(new_dsn.expose(), &source_id, &backend, Some(&frozen))
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
            let expected = expected.clone();
            let holder = Arc::clone(&holder);
            let new_dsn = new_dsn.clone();
            let source_id = source_id.clone();
            let default_db = default_db.clone();
            let cancel = cancel.clone();
            let frozen = frozen_gtid.clone();
            move || async move {
                let s = open_binlog(
                    new_dsn.expose(),
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
            move || async move {
                let s = open_binlog(
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
            ctx.dsn = candidate.dsn.clone();
            ctx.schema.set_dsn(candidate.dsn.clone());
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
