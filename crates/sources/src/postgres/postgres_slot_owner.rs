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

use super::postgres_snapshot::{SnapshotProgress, progress_key};

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

async fn connect(dsn: &str) -> Result<tokio_postgres::Client> {
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

async fn fetch_identity(
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
async fn slot_status(
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
    let row = client
        .query_one(
            "SELECT lsn::text FROM \
             pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&slot],
        )
        .await
        .context("pg_create_logical_replication_slot")?;
    let lsn_str: String = row.get(0);
    Lsn::parse(&lsn_str).context("parse slot consistent LSN")
}

async fn drop_slot(client: &tokio_postgres::Client, slot: &str) -> Result<()> {
    client
        .execute("SELECT pg_drop_replication_slot($1)", &[&slot])
        .await
        .context("pg_drop_replication_slot")?;
    Ok(())
}

async fn load_owner(
    chkpt: &Arc<dyn CheckpointStore>,
    source_id: &str,
) -> Option<SlotOwnership> {
    chkpt
        .get_raw(&owner_key(source_id))
        .await
        .ok()
        .flatten()
        .and_then(|b| serde_json::from_slice(&b).ok())
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

/// Reset snapshot progress to empty. Fallible and must be awaited with `?`: if the
/// reset does not persist, `run_snapshot` could reload stale completed-table state
/// and skip tables under the new anchor, reintroducing loss - so the caller fails
/// closed rather than snapshotting on unreset progress.
pub(super) async fn reset_snapshot_progress(
    chkpt: &Arc<dyn CheckpointStore>,
    source_id: &str,
) -> Result<()> {
    let bytes = serde_json::to_vec(&SnapshotProgress::default())
        .context("serialize reset snapshot progress")?;
    chkpt
        .put_raw(&progress_key(source_id), &bytes)
        .await
        .context("persist reset snapshot progress")?;
    Ok(())
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
///   recreate), reset snapshot progress for a full re-snapshot, and return the
///   new consistent point.
/// - Otherwise (no/partial/mismatched record, or an **active** slot): fail closed
///   with remediation. Never drop an active or ambiguously owned slot.
pub async fn prepare_snapshot_slot_anchor(
    dsn: &str,
    slot: &str,
    pipeline: &str,
    source_id: &str,
    chkpt: &Arc<dyn CheckpointStore>,
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
            // Fresh: any stale Creating record is overwritten (no slot exists,
            // so nothing is ambiguous).
            let c = create_owned_slot(
                &client, chkpt, slot, pipeline, source_id, &id,
            )
            .await
            .map_err(SourceError::Other)?;
            info!(source_id, slot, consistent_lsn = %c, "created replication slot at consistent point");
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
                // Interrupted owned snapshot (or Always re-snapshot): re-anchor.
                drop_slot(&client, slot).await.map_err(SourceError::Other)?;
                let c = create_owned_slot(
                    &client, chkpt, slot, pipeline, source_id, &id,
                )
                .await
                .map_err(SourceError::Other)?;
                // Full re-snapshot: discard table-level progress. Fail closed if
                // this does not persist - snapshotting on stale progress would
                // skip tables under the new anchor and reintroduce loss.
                reset_snapshot_progress(chkpt, source_id)
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

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use checkpoints::{CheckpointError, CheckpointResult};

    /// A checkpoint store whose `put_raw` always fails - to prove the snapshot
    /// progress reset propagates persistence failures (fail closed).
    struct FailingPutStore;

    #[async_trait]
    impl CheckpointStore for FailingPutStore {
        async fn get_raw(
            &self,
            _source_id: &str,
        ) -> CheckpointResult<Option<Vec<u8>>> {
            Ok(None)
        }
        async fn put_raw(
            &self,
            _source_id: &str,
            _bytes: &[u8],
        ) -> CheckpointResult<()> {
            Err(CheckpointError::Database("injected put_raw failure".into()))
        }
        async fn delete(&self, _source_id: &str) -> CheckpointResult<bool> {
            Ok(false)
        }
        async fn list(&self) -> CheckpointResult<Vec<String>> {
            Ok(vec![])
        }
    }

    #[tokio::test]
    async fn reset_snapshot_progress_fails_closed_on_persist_error() {
        let store: Arc<dyn CheckpointStore> = Arc::new(FailingPutStore);
        let res = reset_snapshot_progress(&store, "src1").await;
        assert!(
            res.is_err(),
            "reset must propagate a persistence failure so the caller fails \
             closed instead of snapshotting on stale progress"
        );
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
