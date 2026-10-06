//! Durable replication-slot ownership and the snapshot anchor (PG-A-lite).
//!
//! The initial snapshot anchors CDC at the replication slot's **consistent
//! point** - the LSN `pg_create_logical_replication_slot` returns - instead of an
//! independently sampled `pg_current_wal_lsn`. Rows committed in
//! `(consistent_point, snapshot-export]` are re-delivered by CDC (a bounded
//! at-least-once overlap), never lost.
//!
//! To create or recreate that slot safely, DeltaForge records durable ownership
//! bound to the server + database + slot + plugin + source, with a lifecycle
//! (`creating` -> `created`). It will only ever drop a slot it can prove it owns,
//! whose lineage matches, and that is **inactive**; anything else fails closed.

use std::sync::Arc;

use anyhow::{Context, Result};
use checkpoints::CheckpointStore;
use deltaforge_core::SourceError;
use pgwire_replication::Lsn;
use serde::{Deserialize, Serialize};
use tokio_postgres::NoTls;
use tracing::{info, warn};

/// Schema version of the persisted ownership record.
pub const SLOT_OWNER_RECORD_VERSION: u32 = 1;
const PLUGIN: &str = "pgoutput";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SlotLifecycle {
    /// Intent persisted before slot creation; not yet a proof of ownership.
    Creating,
    /// Slot created and the consistent point recorded.
    Created,
}

/// Durable proof that this pipeline owns a replication slot on a specific server
/// and database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotOwnership {
    pub record_version: u32,
    pub source_id: String,
    pub pipeline: String,
    pub system_identifier: String,
    pub database: String,
    pub database_oid: i64,
    pub slot: String,
    pub plugin: String,
    pub lifecycle: SlotLifecycle,
    pub consistent_lsn: Option<String>,
    pub created_at_ms: u64,
}

/// Server + database identity binding an ownership record to one server/db, so a
/// record cannot be honored against a different (e.g. failed-over) server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerDbIdentity {
    pub system_identifier: String,
    pub database: String,
    pub database_oid: i64,
}

fn owner_key(source_id: &str) -> String {
    format!("slot_owner:{source_id}")
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Pure ownership check: does `rec` prove `source_id`/`pipeline` owns `slot` on `id`?
///
/// Requires a finalized (`Created`) record of the current schema version whose
/// source, pipeline, server identity, database identity, slot, and plugin all
/// match. A `Creating` (partial/ambiguous) record never proves ownership.
pub fn ownership_proven(
    rec: &SlotOwnership,
    source_id: &str,
    pipeline: &str,
    slot: &str,
    id: &ServerDbIdentity,
) -> bool {
    rec.record_version == SLOT_OWNER_RECORD_VERSION
        && rec.lifecycle == SlotLifecycle::Created
        && rec.source_id == source_id
        && rec.pipeline == pipeline
        && rec.slot == slot
        && rec.plugin == PLUGIN
        && rec.system_identifier == id.system_identifier
        && rec.database == id.database
        && rec.database_oid == id.database_oid
}

pub(super) async fn connect(dsn: &str) -> Result<tokio_postgres::Client> {
    let (client, conn) = tokio_postgres::connect(dsn, NoTls)
        .await
        .context("connect")?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            warn!(error = %e, "slot-owner control connection error");
        }
    });
    Ok(client)
}

pub(super) async fn fetch_identity(
    client: &tokio_postgres::Client,
) -> Result<ServerDbIdentity> {
    let sid: String = client
        .query_one(
            "SELECT system_identifier::text FROM pg_control_system()",
            &[],
        )
        .await
        .context("read system_identifier")?
        .get(0);
    let row = client
        .query_one(
            "SELECT current_database()::text, \
             (SELECT oid::int8 FROM pg_database WHERE datname = current_database())",
            &[],
        )
        .await
        .context("read database identity")?;
    Ok(ServerDbIdentity {
        system_identifier: sid,
        database: row.get(0),
        database_oid: row.get(1),
    })
}

/// `None` = slot missing; `Some(active)` = slot exists with that active flag.
pub(super) async fn slot_status(
    client: &tokio_postgres::Client,
    slot: &str,
) -> Result<Option<bool>> {
    let row = client
        .query_opt(
            "SELECT active FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .context("query slot status")?;
    Ok(row.map(|r| r.get::<_, bool>(0)))
}

/// Create the logical slot and return its consistent point.
async fn create_slot(
    client: &tokio_postgres::Client,
    slot: &str,
) -> Result<Lsn> {
    let lsn_str = create_logical_slot(client, slot)
        .await
        .context("pg_create_logical_replication_slot")?;
    Lsn::parse(&lsn_str).context("parse slot consistent LSN")
}

/// Create a persistent `pgoutput` slot; returns its consistent point (text).
/// On PostgreSQL 17+ it is a failover slot, so its position is synchronized
/// to standbys and can continue after a promotion (the continuity proof
/// requires that after a timeline switch).
pub(crate) async fn create_logical_slot(
    client: &tokio_postgres::Client,
    slot: &str,
) -> Result<String, tokio_postgres::Error> {
    let version: i32 = client
        .query_one("SELECT current_setting('server_version_num')::int", &[])
        .await?
        .get(0);
    let sql = if version as u32
        >= super::postgres_continuity::FAILOVER_SLOTS_VERSION
    {
        "SELECT lsn::text FROM pg_create_logical_replication_slot(\
         $1, 'pgoutput', false, false, true)"
    } else {
        "SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput')"
    };
    Ok(client.query_one(sql, &[&slot]).await?.get(0))
}

async fn drop_slot(client: &tokio_postgres::Client, slot: &str) -> Result<()> {
    client
        .execute("SELECT pg_drop_replication_slot($1)", &[&slot])
        .await
        .context("pg_drop_replication_slot")?;
    Ok(())
}

/// Outcome of reading the durable slot-ownership record, keeping the transient
/// (retryable) case distinct from the terminal ones so a temporary checkpoint-store
/// outage never permanently suppresses a valid credential generation.
pub(super) enum OwnerRead {
    /// The record was read and deserialized.
    Present(SlotOwnership),
    /// The store is reachable but has no record for this source (terminal identity).
    Missing,
    /// The store could not be read (transient/unavailable).
    Unavailable,
    /// A record exists but did not deserialize (terminal integrity).
    Malformed,
}

/// Read the slot-ownership record, distinguishing store-unavailable (transient)
/// from missing/malformed (terminal). Prefer this over [`load_owner`] where the
/// caller must retry a transient authority outage rather than fail closed.
pub(super) async fn read_owner(
    chkpt: &Arc<dyn CheckpointStore>,
    source_id: &str,
) -> OwnerRead {
    match chkpt.get_raw(&owner_key(source_id)).await {
        Err(_) => OwnerRead::Unavailable,
        Ok(None) => OwnerRead::Missing,
        Ok(Some(bytes)) => {
            match serde_json::from_slice::<SlotOwnership>(&bytes) {
                Ok(rec) => OwnerRead::Present(rec),
                Err(_) => OwnerRead::Malformed,
            }
        }
    }
}

/// Preflight verdict for the configured replication slot, evaluated under the
/// same durable-ownership rules startup uses.
#[derive(Debug, PartialEq, Eq)]
pub enum SlotPreflight {
    /// Slot does not exist; DeltaForge will create and own it.
    AbsentWillCreate,
    /// Slot exists, this pipeline's ownership is proven, and it is inactive.
    OwnedInactive,
    /// Slot exists and is owned by this pipeline but currently active (another
    /// consumer is connected).
    OwnedActive,
    /// Slot exists but ownership cannot be proven (no or mismatched owner record):
    /// a foreign slot that startup would refuse to take over.
    Foreign,
    /// The ownership store could not be read (transient); cannot classify.
    OwnerStoreUnavailable,
}

/// Classify the configured slot for a deployment preflight, using the same
/// ownership rules as startup: an existing slot is OK only when this pipeline's
/// durable ownership record proves it owns that slot on this exact server/db, and
/// it is not already in active use.
pub async fn preflight_classify_slot(
    chkpt: &Arc<dyn CheckpointStore>,
    dsn: &str,
    source_id: &str,
    pipeline: &str,
    slot: &str,
) -> Result<SlotPreflight> {
    let client = connect(dsn).await?;
    let active = match slot_status(&client, slot).await? {
        None => return Ok(SlotPreflight::AbsentWillCreate),
        Some(active) => active,
    };
    let id = fetch_identity(&client).await?;
    match read_owner(chkpt, source_id).await {
        OwnerRead::Unavailable => Ok(SlotPreflight::OwnerStoreUnavailable),
        OwnerRead::Present(rec)
            if ownership_proven(&rec, source_id, pipeline, slot, &id) =>
        {
            if active {
                Ok(SlotPreflight::OwnedActive)
            } else {
                Ok(SlotPreflight::OwnedInactive)
            }
        }
        // Missing, malformed, or mismatched record -> not provably ours.
        _ => Ok(SlotPreflight::Foreign),
    }
}

pub(super) async fn load_owner(
    chkpt: &Arc<dyn CheckpointStore>,
    source_id: &str,
) -> Option<SlotOwnership> {
    match read_owner(chkpt, source_id).await {
        OwnerRead::Present(rec) => Some(rec),
        _ => None,
    }
}

async fn write_owner(
    chkpt: &Arc<dyn CheckpointStore>,
    rec: &SlotOwnership,
) -> Result<()> {
    let bytes = serde_json::to_vec(rec).context("serialize slot ownership")?;
    chkpt
        .put_raw(&owner_key(&rec.source_id), &bytes)
        .await
        .context("persist slot ownership")?;
    Ok(())
}

/// Create a fresh slot with the full intent -> finalize ownership lifecycle and
/// return its consistent point.
async fn create_owned_slot(
    client: &tokio_postgres::Client,
    chkpt: &Arc<dyn CheckpointStore>,
    slot: &str,
    pipeline: &str,
    source_id: &str,
    id: &ServerDbIdentity,
) -> Result<Lsn> {
    // Persist intent BEFORE creating the slot.
    let mut rec = SlotOwnership {
        record_version: SLOT_OWNER_RECORD_VERSION,
        source_id: source_id.to_string(),
        pipeline: pipeline.to_string(),
        system_identifier: id.system_identifier.clone(),
        database: id.database.clone(),
        database_oid: id.database_oid,
        slot: slot.to_string(),
        plugin: PLUGIN.to_string(),
        lifecycle: SlotLifecycle::Creating,
        consistent_lsn: None,
        created_at_ms: now_ms(),
    };
    write_owner(chkpt, &rec).await?;

    let c = create_slot(client, slot).await?;

    // Finalize after creation.
    rec.lifecycle = SlotLifecycle::Created;
    rec.consistent_lsn = Some(c.to_string());
    write_owner(chkpt, &rec).await?;
    Ok(c)
}

fn fail_closed(msg: String) -> SourceError {
    SourceError::Incompatible {
        details: msg.into(),
    }
}

/// Establish the snapshot anchor: return the replication slot's consistent point,
/// creating or safely re-anchoring the slot as needed.
///
/// - Slot missing: create it (intent -> finalize) and return its consistent point.
/// - Slot exists, ownership proven, and slot **inactive**: re-anchor (drop +
///   recreate) for the new snapshot generation, and return the new consistent
///   point.
/// - Otherwise (no/partial/mismatched record, or an **active** slot): fail closed
///   with remediation. Never drop an active or ambiguously owned slot.
pub async fn prepare_snapshot_slot_anchor(
    dsn: &str,
    slot: &str,
    pipeline: &str,
    source_id: &str,
    chkpt: &Arc<dyn CheckpointStore>,
    backend: &storage::ArcStorageBackend,
    generation: u64,
) -> Result<Lsn, SourceError> {
    let client = connect(dsn).await.map_err(|e| SourceError::Connect {
        details: e.to_string().into(),
    })?;
    let id = fetch_identity(&client).await.map_err(SourceError::Other)?;

    match slot_status(&client, slot)
        .await
        .map_err(SourceError::Other)?
    {
        None => {
            // A slot this source created and lost is recreated only under a
            // `resnapshot` authorization for exactly this generation, taken
            // before the slot is created (or resumed, when a start of this
            // generation took it and stopped before creating). Fresh: no
            // finalized record, and any stale Creating record is overwritten
            // (no slot exists, so nothing is ambiguous).
            let authorized = authorize_recreation(
                chkpt, backend, source_id, pipeline, slot, generation,
            )
            .await?;
            if authorized.is_some()
                && crate::snapshot_probe::slot_recreation_point(
                    crate::snapshot_probe::SlotRecreationPoint::BeforeCreate,
                )
                .await
            {
                return Err(SourceError::Other(anyhow::anyhow!(
                    "injected crash before the slot was created"
                )));
            }
            let c = create_owned_slot(
                &client, chkpt, slot, pipeline, source_id, &id,
            )
            .await
            .map_err(SourceError::Other)?;
            info!(source_id, slot, consistent_lsn = %c, "created replication slot at consistent point");
            if authorized.is_some() {
                if crate::snapshot_probe::slot_recreation_point(
                    crate::snapshot_probe::SlotRecreationPoint::AfterCreate,
                )
                .await
                {
                    return Err(SourceError::Other(anyhow::anyhow!(
                        "injected crash after the slot was created"
                    )));
                }
                complete_recreation(
                    chkpt, backend, source_id, pipeline, slot, generation, c,
                )
                .await?;
            }
            Ok(c)
        }
        Some(active) => {
            let owned = load_owner(chkpt, source_id)
                .await
                .map(|rec| {
                    ownership_proven(&rec, source_id, pipeline, slot, &id)
                })
                .unwrap_or(false);

            if owned && !active {
                // The slot this generation's authorized recreation created
                // (its completion maybe not recorded yet): the post-state of
                // that creation, kept as the anchor and audited; never
                // created again.
                if let Some(c) = recreated_by_this_generation(
                    chkpt, backend, source_id, pipeline, slot, generation,
                )
                .await?
                {
                    return Ok(c);
                }
                // Interrupted owned snapshot (or Always re-snapshot): re-anchor.
                drop_slot(&client, slot).await.map_err(SourceError::Other)?;
                let c = create_owned_slot(
                    &client, chkpt, slot, pipeline, source_id, &id,
                )
                .await
                .map_err(SourceError::Other)?;
                warn!(source_id, slot, consistent_lsn = %c, "re-anchored owned inactive slot; performing a full re-snapshot");
                Ok(c)
            } else {
                let why = if active {
                    "the slot is active (in use by another consumer or session)"
                } else {
                    "DeltaForge cannot prove exclusive ownership of it (no or \
                     mismatched/partial ownership record)"
                };
                Err(fail_closed(format!(
                    "replication slot '{slot}' already exists and {why}. Refusing \
                     to snapshot to avoid corrupting another consumer or losing \
                     data. Remediation: confirm no other consumer uses it, then \
                     drop it (SELECT pg_drop_replication_slot('{slot}')) and \
                     restart; or set snapshot mode to 'never' to stream from the \
                     current position without an initial load."
                )))
            }
        }
    }
}

/// The ownership record's bytes, its digest, and its lifecycle.
async fn owner_state(
    chkpt: &Arc<dyn CheckpointStore>,
    source_id: &str,
) -> Result<Option<(String, SlotOwnership)>, SourceError> {
    let raw = chkpt.get_raw(&owner_key(source_id)).await.map_err(|e| {
        SourceError::Checkpoint {
            details: format!("read the slot ownership record: {e}").into(),
        }
    })?;
    Ok(raw.and_then(|b| {
        let rec = serde_json::from_slice::<SlotOwnership>(&b).ok()?;
        use sha2::{Digest, Sha256};
        Some((hex::encode(Sha256::digest(&b)), rec))
    }))
}

/// Before a missing slot is created: when the ownership record shows this
/// source created it before (or a start of this generation began creating
/// it), take or resume the matching `resnapshot` authorization, or refuse.
/// `None`: a fresh creation, no authorization involved.
async fn authorize_recreation(
    chkpt: &Arc<dyn CheckpointStore>,
    backend: &storage::ArcStorageBackend,
    source_id: &str,
    pipeline: &str,
    slot: &str,
    generation: u64,
) -> Result<Option<crate::snapshot_recovery::SlotRecreation>, SourceError> {
    let Some((digest, owner)) = owner_state(chkpt, source_id).await? else {
        return Ok(None);
    };
    let creating = owner.lifecycle == SlotLifecycle::Creating;
    let taken = crate::snapshot_recovery::take_slot_recreation(
        backend, source_id, pipeline, slot, generation, &digest, creating,
    )
    .await
    .map_err(SourceError::Other)?;
    match taken {
        Some(auth) => {
            warn!(
                source_id,
                slot,
                generation,
                "recreating the lost replication slot under its resnapshot authorization"
            );
            Ok(Some(auth))
        }
        // A creation intent with no authorization: a fresh creation.
        None if creating => Ok(None),
        None => Err(fail_closed(format!(
            "replication slot '{slot}' was created by this source and is gone. \
             It is never recreated implicitly: run `deltaforge recover plan \
             resnapshot` and apply it, which authorizes recreating it for the \
             new snapshot generation."
        ))),
    }
}

/// After an authorized creation: record it (`created`, bound to the
/// ownership record the creation wrote) and audit it, once.
async fn complete_recreation(
    chkpt: &Arc<dyn CheckpointStore>,
    backend: &storage::ArcStorageBackend,
    source_id: &str,
    pipeline: &str,
    slot: &str,
    generation: u64,
    consistent: Lsn,
) -> Result<(), SourceError> {
    let (digest, _) =
        owner_state(chkpt, source_id).await?.ok_or_else(|| {
            SourceError::Other(anyhow::anyhow!(
                "the slot ownership record vanished after the slot was created"
            ))
        })?;
    let done = crate::snapshot_recovery::complete_slot_recreation(
        backend,
        source_id,
        pipeline,
        slot,
        generation,
        &digest,
        &consistent.to_string(),
    )
    .await
    .map_err(SourceError::Other)?;
    audit_recreation(backend, &done).await
}

async fn audit_recreation(
    backend: &storage::ArcStorageBackend,
    done: &crate::snapshot_recovery::SlotRecreation,
) -> Result<(), SourceError> {
    let created = done.created.as_ref().ok_or_else(|| {
        SourceError::Other(anyhow::anyhow!("the recreation is not completed"))
    })?;
    storage::adapters::recovery::RecoveryStore::new(
        backend.clone(),
        &done.pipeline,
    )
    .append_event(
        &done.proof,
        "slot_recreated",
        std::collections::BTreeMap::from([
            ("slot".to_string(), done.slot.clone()),
            ("generation".to_string(), done.generation.to_string()),
            ("consistent_lsn".to_string(), created.consistent_lsn.clone()),
        ]),
        created.at_ms,
    )
    .await
    .map_err(SourceError::Other)
}

/// The slot, owned and present, when this generation's authorized
/// recreation created it: its consistent point (completion recorded and
/// audit repaired if a stop interrupted them). `None` otherwise.
async fn recreated_by_this_generation(
    chkpt: &Arc<dyn CheckpointStore>,
    backend: &storage::ArcStorageBackend,
    source_id: &str,
    pipeline: &str,
    slot: &str,
    generation: u64,
) -> Result<Option<Lsn>, SourceError> {
    use crate::snapshot_recovery::SlotRecreationState as S;
    let Some(auth) = crate::snapshot_recovery::slot_recreation_of(
        backend, source_id, pipeline, slot, generation,
    )
    .await
    .map_err(SourceError::Other)?
    else {
        return Ok(None);
    };
    let Some((digest, owner)) = owner_state(chkpt, source_id).await? else {
        return Ok(None);
    };
    let Some(consistent) = owner
        .consistent_lsn
        .as_deref()
        .and_then(|l| Lsn::parse(l).ok())
    else {
        return Ok(None);
    };
    let ours = match (&auth.state, &auth.created) {
        // Created after the authorization was taken: the record is the
        // creation's own (finalized, not the one authorized over).
        (S::Consumed, None) => {
            owner.lifecycle == SlotLifecycle::Created
                && digest != auth.owner_record
        }
        (S::Created, Some(c)) => c.owner_record == digest,
        _ => false,
    };
    if !ours {
        return Ok(None);
    }
    complete_recreation(
        chkpt, backend, source_id, pipeline, slot, generation, consistent,
    )
    .await?;
    info!(source_id, slot, consistent_lsn = %consistent, "kept the slot this generation's authorized recreation created");
    Ok(Some(consistent))
}

/// The configured replication slot as the recovery operation `resnapshot`
/// sees it (`docs/design/recovery-cli.md`, section 5.1): observed on one
/// connection, with the durable ownership proof startup uses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SlotObservation {
    pub slot: String,
    pub system_identifier: String,
    pub database: String,
    pub database_oid: i64,
    /// The ownership record's bytes digest, or `absent`.
    pub owner_record: String,
    /// Whether the ownership record proves this source (and pipeline) owns
    /// a slot of this name on this server and database, under the startup
    /// rules (a finalized record), whether or not the slot exists now.
    pub owner_proven: bool,
    /// `None`: no slot by that name.
    pub present: Option<SlotPresent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SlotPresent {
    pub active: bool,
    pub wal_status: Option<String>,
    pub invalidation: Option<String>,
    /// Ownership proven under the startup rules (finalized record of this
    /// source, pipeline, server, database, slot and plugin).
    pub owned: bool,
}

impl SlotObservation {
    /// The slot exists but its retention is lost or it was invalidated.
    pub fn lost(&self) -> bool {
        self.present.as_ref().is_some_and(|p| {
            p.wal_status.as_deref() == Some("lost") || p.invalidation.is_some()
        })
    }

    pub fn digest(&self) -> String {
        use sha2::{Digest, Sha256};
        hex::encode(Sha256::digest(
            serde_json::to_vec(self).expect("an observation serializes"),
        ))
    }
}

async fn observe_on(
    client: &tokio_postgres::Client,
    slot: &str,
    pipeline: &str,
    source_id: &str,
    chkpt: &Arc<dyn CheckpointStore>,
) -> Result<SlotObservation> {
    let id = fetch_identity(client).await?;
    let raw = chkpt
        .get_raw(&owner_key(source_id))
        .await
        .map_err(|e| anyhow::anyhow!("read the slot ownership record: {e}"))?;
    let owner_record = match &raw {
        None => "absent".to_string(),
        Some(b) => {
            use sha2::{Digest, Sha256};
            hex::encode(Sha256::digest(b))
        }
    };
    let row = client
        .query_opt(
            &format!(
                "SELECT active, wal_status::text, {} \
                 FROM pg_replication_slots s WHERE slot_name = $1",
                super::postgres_health::INVALIDATION
            ),
            &[&slot],
        )
        .await
        .context("query the replication slot")?;
    let owner_proven = raw
        .as_deref()
        .and_then(|b| serde_json::from_slice::<SlotOwnership>(b).ok())
        .is_some_and(|rec| {
            ownership_proven(&rec, source_id, pipeline, slot, &id)
        });
    let present = row.map(|r| SlotPresent {
        active: r.get(0),
        wal_status: r.get(1),
        invalidation: r.get(2),
        owned: owner_proven,
    });
    Ok(SlotObservation {
        slot: slot.to_string(),
        system_identifier: id.system_identifier,
        database: id.database,
        database_oid: id.database_oid,
        owner_record,
        owner_proven,
        present,
    })
}

/// Observe the configured slot (see [`SlotObservation`]).
pub async fn observe_slot(
    dsn: &str,
    slot: &str,
    pipeline: &str,
    source_id: &str,
    chkpt: &Arc<dyn CheckpointStore>,
) -> Result<SlotObservation> {
    let client = connect(dsn).await?;
    observe_on(&client, slot, pipeline, source_id, chkpt).await
}

/// Drop the configured slot, only when, observed on the same connection, it
/// is exactly `expected` (the planned observation) and that is an owned,
/// inactive, lost slot. The next start creates the new slot through the
/// ownership-recording path.
pub async fn drop_lost_owned_slot(
    dsn: &str,
    slot: &str,
    pipeline: &str,
    source_id: &str,
    chkpt: &Arc<dyn CheckpointStore>,
    expected: &str,
) -> Result<()> {
    let client = connect(dsn).await?;
    let now = observe_on(&client, slot, pipeline, source_id, chkpt).await?;
    let droppable = now.present.as_ref().is_some_and(|p| p.owned && !p.active)
        && now.lost();
    anyhow::ensure!(
        now.digest() == expected && droppable,
        "replication slot '{slot}' is not the owned, inactive, lost slot the \
         plan observed; nothing was dropped"
    );
    drop_slot(&client, slot).await?;
    warn!(
        source_id,
        slot, "dropped the owned lost replication slot for resnapshot"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use checkpoints::{CheckpointError, CheckpointResult};

    /// A store whose `get_raw` always errors - to prove a transient authority
    /// outage is classified as retryable, not a permanent identity failure.
    struct FailingGetStore;

    #[async_trait]
    impl CheckpointStore for FailingGetStore {
        async fn get_raw(
            &self,
            _source_id: &str,
        ) -> CheckpointResult<Option<Vec<u8>>> {
            Err(CheckpointError::Database("injected get_raw failure".into()))
        }
        async fn put_raw(
            &self,
            _source_id: &str,
            _bytes: &[u8],
        ) -> CheckpointResult<()> {
            Ok(())
        }
        async fn delete(&self, _source_id: &str) -> CheckpointResult<bool> {
            Ok(false)
        }
        async fn list(&self) -> CheckpointResult<Vec<String>> {
            Ok(vec![])
        }
    }

    #[tokio::test]
    async fn read_owner_store_error_is_unavailable() {
        let store: Arc<dyn CheckpointStore> = Arc::new(FailingGetStore);
        assert!(matches!(
            read_owner(&store, "src1").await,
            OwnerRead::Unavailable
        ));
    }

    #[tokio::test]
    async fn read_owner_absent_is_missing() {
        let store: Arc<dyn CheckpointStore> =
            Arc::new(checkpoints::MemCheckpointStore::new().unwrap());
        assert!(matches!(
            read_owner(&store, "src1").await,
            OwnerRead::Missing
        ));
    }

    #[tokio::test]
    async fn read_owner_corrupt_bytes_is_malformed() {
        let store: Arc<dyn CheckpointStore> =
            Arc::new(checkpoints::MemCheckpointStore::new().unwrap());
        store
            .put_raw(&owner_key("src1"), b"not-json")
            .await
            .unwrap();
        assert!(matches!(
            read_owner(&store, "src1").await,
            OwnerRead::Malformed
        ));
    }

    #[tokio::test]
    async fn read_owner_valid_record_is_present() {
        let store: Arc<dyn CheckpointStore> =
            Arc::new(checkpoints::MemCheckpointStore::new().unwrap());
        write_owner(&store, &created_record()).await.unwrap();
        match read_owner(&store, "src1").await {
            OwnerRead::Present(rec) => {
                assert_eq!(rec.source_id, "src1");
                assert_eq!(rec.database_oid, 16384);
            }
            _ => panic!("expected Present owner record"),
        }
    }

    fn id() -> ServerDbIdentity {
        ServerDbIdentity {
            system_identifier: "6912345678901234567".into(),
            database: "app".into(),
            database_oid: 16384,
        }
    }

    fn created_record() -> SlotOwnership {
        SlotOwnership {
            record_version: SLOT_OWNER_RECORD_VERSION,
            source_id: "src1".into(),
            pipeline: "p".into(),
            system_identifier: "6912345678901234567".into(),
            database: "app".into(),
            database_oid: 16384,
            slot: "df_slot".into(),
            plugin: "pgoutput".into(),
            lifecycle: SlotLifecycle::Created,
            consistent_lsn: Some("0/1A2B3C0".into()),
            created_at_ms: 1,
        }
    }

    #[test]
    fn proven_when_all_match_and_created() {
        assert!(ownership_proven(
            &created_record(),
            "src1",
            "p",
            "df_slot",
            &id()
        ));
    }

    #[test]
    fn not_proven_when_creating() {
        let mut r = created_record();
        r.lifecycle = SlotLifecycle::Creating;
        assert!(!ownership_proven(&r, "src1", "p", "df_slot", &id()));
    }

    #[test]
    fn not_proven_on_pipeline_mismatch() {
        // Same source and slot on the same server/db, but a different pipeline
        // must not be treated as the owner.
        let base = created_record();
        assert!(!ownership_proven(
            &base,
            "src1",
            "other-pipeline",
            "df_slot",
            &id()
        ));
    }

    #[test]
    fn not_proven_on_identity_or_lineage_mismatch() {
        let base = created_record();
        // different source
        assert!(!ownership_proven(&base, "other", "p", "df_slot", &id()));
        // different slot
        assert!(!ownership_proven(&base, "src1", "p", "other_slot", &id()));
        // different server
        let mut other_server = id();
        other_server.system_identifier = "9999999999999999999".into();
        assert!(!ownership_proven(
            &base,
            "src1",
            "p",
            "df_slot",
            &other_server
        ));
        // different database
        let mut other_db = id();
        other_db.database_oid = 99999;
        assert!(!ownership_proven(&base, "src1", "p", "df_slot", &other_db));
        // wrong record version
        let mut old_ver = created_record();
        old_ver.record_version = 0;
        assert!(!ownership_proven(&old_ver, "src1", "p", "df_slot", &id()));
    }
}
