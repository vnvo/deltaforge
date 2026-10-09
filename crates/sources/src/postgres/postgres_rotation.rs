//! PostgreSQL-specific credential-rotation operations, plugged into the
//! source-agnostic [`crate::rotation_manager`] (which owns the watcher/manager task,
//! the coordinator, alarms, metrics, and teardown) and the core in [`crate::rotation`].
//!
//! Only the PostgreSQL-specific pieces live here:
//! - [`preflight`] (Stage A / boundary recheck): identity via the durable
//!   slot-ownership record (gate 2), slot position vs the frozen boundary (gate 3),
//!   with transient authority failures kept retryable.
//! - [`confirm_slot_inactive`] (gate 4): the old slot must read `active = false`
//!   before the replacement opens.
//! - [`open_stream`]: open a replication stream via the production connect path.
//! - [`RotationRuntime`]: a thin wrapper over [`RotationManager`] that supplies the
//!   PG Stage-A preflight and Stage-B apply (recheck -> close+confirm -> open ->
//!   recover), swapping `ctx.repl_client` on success.

use std::sync::Arc;
use std::time::SystemTime;

use checkpoints::CheckpointStore;
use common::RetryPolicy;
use deltaforge_config::PostgresSrcCfg;
use deltaforge_core::SourceResult;
use pgwire_replication::Lsn;
use secrets::SecretResolver;
use tokio_util::sync::CancellationToken;

use crate::rotation::{DbKind, RotationReject, apply_two_stage};
use crate::rotation_manager::{
    ApplyStep, ReadyPreflight, RotationManager, RotationSpec,
};

use super::RunCtx;
use super::fetch_slot_confirmed_lsn;
use super::postgres_helpers::{
    StreamProof, build_replication_config, connect_replication_with_retries,
    parse_dsn,
};
use super::postgres_slot_owner::{
    OwnerRead, connect, fetch_identity, ownership_proven, read_owner,
};

/// Stage-A preflight for a replacement DSN, on a fresh control connection.
///
/// Authenticates (the connect proves it), proves identity from the durable
/// slot-ownership record (gate 2), and rechecks the slot position against
/// `frozen_lsn` (gate 3). A transient authority-read failure is retryable
/// (`PreflightFailed`); only a missing/malformed record or an ahead-of-boundary
/// position is terminal. Returns a typed, redacted [`RotationReject`] on any
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

    let live = fetch_identity(&client)
        .await
        .map_err(|_| RotationReject::PreflightFailed)?;
    let owner = owner_to_reject(read_owner(chkpt, source_id).await)?;
    if !ownership_proven(&owner, source_id, pipeline, slot, &live) {
        return Err(RotationReject::IdentityMismatch);
    }

    // Position: the slot must not have flushed past the frozen boundary; resuming at
    // frozen_lsn must not skip committed changes. A failure to read the position is
    // transient; only a successfully-read ahead-of-boundary is terminal.
    let confirmed = fetch_slot_confirmed_lsn(new_dsn, slot)
        .await
        .map_err(|_| RotationReject::PreflightFailed)?;
    if confirmed > frozen_lsn {
        return Err(RotationReject::PositionIncompatible);
    }

    Ok(())
}

/// Map a slot-ownership authority read to the owner record or a typed rejection: a
/// store outage is transient ([`RotationReject::PreflightFailed`]) and retryable; a
/// missing or malformed record is a terminal identity/integrity failure.
fn owner_to_reject(
    read: OwnerRead,
) -> Result<super::postgres_slot_owner::SlotOwnership, RotationReject> {
    match read {
        OwnerRead::Present(rec) => Ok(rec),
        OwnerRead::Unavailable => Err(RotationReject::PreflightFailed),
        OwnerRead::Missing | OwnerRead::Malformed => {
            Err(RotationReject::IdentityMismatch)
        }
    }
}

/// Confirm the logical slot is inactive (its previous consumer released it) before a
/// close is treated as successful (gate 4). Queried on a control connection with the
/// replacement DSN. `Ok(())` only when the slot exists and reads `active = false`.
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

/// Open a replication stream for `dsn` at `start_lsn` via the production connect
/// path (same builder, continuity proof and retry policy as startup/reconnect):
/// continuity of the durable checkpoint, proven on the stream's own session,
/// is the safety boundary. The stream is activated (its continuity persisted
/// and stamped) before it is returned; a replacement candidate
/// (`allow_transition` false) that would change the continuity is refused
/// before anything is persisted or stamped.
async fn open_stream(
    dsn: &crate::credentials::ProtectedDsn,
    proof: &StreamProof,
    publication: &str,
    start_lsn: Lsn,
    allow_transition: bool,
    cancel: &CancellationToken,
) -> Result<pgwire_replication::ReplicationClient, ()> {
    let components = parse_dsn(dsn.expose()).map_err(|_| ())?;
    let config = build_replication_config(
        &components,
        &proof.slot,
        publication,
        start_lsn,
    );
    connect_replication_with_retries(
        &proof.source_id,
        config,
        &proof.with_dsn(dsn.clone()),
        allow_transition,
        cancel,
        RetryPolicy::default(),
    )
    .await
    .map_err(|_| ())
}

/// Build a PostgreSQL rotation spec from source config (fail-closed; see
/// [`crate::rotation_manager::build_spec`]).
pub async fn build_spec(
    cfg: &PostgresSrcCfg,
    resolver: Arc<dyn SecretResolver>,
) -> SourceResult<Option<Arc<RotationSpec>>> {
    crate::rotation_manager::build_spec(
        cfg.dsn.as_deref(),
        cfg.dsn_secret.as_ref(),
        cfg.credentials.as_ref(),
        cfg.rotation.as_ref(),
        DbKind::Postgres,
        resolver,
    )
    .await
}

/// Runtime the PostgreSQL run loop drives to apply credential rotation. Thin wrapper
/// over the shared [`RotationManager`] with the PG Stage-A/Stage-B logic.
pub(crate) struct RotationRuntime {
    mgr: RotationManager,
    slot: String,
    publication: String,
    chkpt: Arc<dyn CheckpointStore>,
    proof: StreamProof,
}

impl RotationRuntime {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn spawn(
        spec: &Arc<RotationSpec>,
        initial_dsn: crate::credentials::ProtectedDsn,
        source_id: String,
        pipeline: String,
        slot: String,
        publication: String,
        chkpt: Arc<dyn CheckpointStore>,
        proof: StreamProof,
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
            slot,
            publication,
            chkpt,
            proof,
        })
    }

    /// Stage A: schedule preflight for the latest eligible candidate on a control
    /// connection, concurrently with the live stream.
    pub(crate) fn drive_preflight(&mut self, ctx: &RunCtx) {
        let source_id = self.mgr.source_id().to_string();
        let pipeline = self.mgr.pipeline().to_string();
        let slot = self.slot.clone();
        let chkpt = Arc::clone(&self.chkpt);
        let frozen = ctx.last_lsn;
        self.mgr.drive_preflight(|candidate| {
            let dsn = candidate.dsn.clone();
            tokio::spawn(async move {
                preflight(
                    dsn.expose(),
                    &source_id,
                    &pipeline,
                    &slot,
                    frozen,
                    &chkpt,
                )
                .await
            })
        });
    }

    pub(crate) async fn wait_activity(&mut self) {
        self.mgr.wait_activity().await
    }

    /// Stage B: apply a completed preflight at the current whole-transaction
    /// boundary (`ctx.current_tx_id.is_none()`). Awaited to completion; fatal
    /// outcomes return an error so the run loop stops without advancing the
    /// checkpoint.
    pub(crate) async fn apply_at_boundary(
        &mut self,
        ctx: &mut RunCtx,
    ) -> SourceResult<()> {
        let Some(ready) = self.mgr.take_ready() else {
            return Ok(());
        };
        let ReadyPreflight {
            generation,
            candidate,
            result,
        } = ready;
        if let Err(reject) = result {
            self.mgr.record_preflight_failure(generation, reject);
            return Ok(());
        }

        // Stage-B closures capture owned/protected state so nothing borrows `ctx`
        // across the awaited apply; DSNs stay protected until the connect/parse.
        let frozen = ctx.last_lsn;
        let new_dsn = candidate.dsn.clone();
        let old_dsn = ctx.dsn.clone();
        let source_id = self.mgr.source_id().to_string();
        let pipeline = self.mgr.pipeline().to_string();
        let slot = self.slot.clone();
        let publication = self.publication.clone();
        let chkpt = Arc::clone(&self.chkpt);
        let repl_client = Arc::clone(&ctx.repl_client);
        let cancel = self.mgr.child_cancel();

        // recheck (gate 1 + gate 3): candidate not expired, fresh re-preflight
        // against the frozen boundary on a new control connection.
        let recheck = {
            let cand = candidate.clone();
            let new_dsn = new_dsn.clone();
            let source_id = source_id.clone();
            let pipeline = pipeline.clone();
            let slot = slot.clone();
            let chkpt = Arc::clone(&chkpt);
            move || async move {
                if cand.is_expired(SystemTime::now()) {
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
            let proof = self.proof.clone();
            let publication = publication.clone();
            let cancel = cancel.clone();
            move || async move {
                // A credential rotation never crosses a continuity
                // transition: the old stream is closed here, and a candidate
                // proving another chain, transition or timeline is refused
                // (then the old credentials reopen with a full proof).
                let client = open_stream(
                    &new_dsn,
                    &proof,
                    &publication,
                    frozen,
                    false,
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
            let proof = self.proof.clone();
            let publication = publication.clone();
            let cancel = cancel.clone();
            move || async move {
                let client = open_stream(
                    &old_dsn,
                    &proof,
                    &publication,
                    frozen,
                    true,
                    &cancel,
                )
                .await?;
                *repl_client.lock().await = client;
                Ok::<(), ()>(())
            }
        };

        let outcome = apply_two_stage(
            self.mgr.apply_timeout(),
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
            // The catalog session follows the rotated credentials too.
            ctx.catalog.set_dsn(candidate.dsn.clone());
        }
        Ok(())
    }

    pub(crate) async fn shutdown(self) {
        self.mgr.shutdown().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rotation_manager::is_transient;

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
}
