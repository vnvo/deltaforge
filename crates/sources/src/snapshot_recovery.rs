//! What the recovery operation `resnapshot` needs from the snapshot state
//! (`docs/design/recovery-cli.md`, section 5.1): classifying a source's
//! checkpoints with the engine's own rules, relating a control record's
//! lineage to the source's recorded lineage, and the inputs of a first
//! recovery allocation, all without a running source.

use deltaforge_core::CheckpointOrder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use storage::adapters::LineageDescriptor;

use crate::snapshot_generation::{
    PersistedLineage, SnapshotFingerprintBuilder,
};
use crate::snapshot_position::{
    Classified, EngineOrder, classify_stored, order,
};

/// The source engines a snapshot runs on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Engine {
    Postgres,
    Mysql,
}

impl Engine {
    pub fn name(self) -> &'static str {
        match self {
            Engine::Postgres => "postgres",
            Engine::Mysql => "mysql",
        }
    }
}

/// A source checkpoint as `resnapshot` sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointKind {
    /// A well-formed stream (CDC) position.
    Stream,
    /// A snapshot, generation-start or snapshot-chain position.
    Snapshot,
    /// Unknown format or malformed: never interpreted.
    Malformed,
}

fn kind<E: EngineOrder>(engine: &E, raw: &[u8]) -> CheckpointKind {
    match classify_stored(engine, raw) {
        None => CheckpointKind::Malformed,
        Some(Classified::Stream) => {
            // Well-formed bytes compare equal to themselves.
            if order(engine, raw, raw) == CheckpointOrder::Equal {
                CheckpointKind::Stream
            } else {
                CheckpointKind::Malformed
            }
        }
        Some(_) => CheckpointKind::Snapshot,
    }
}

/// Classify stored checkpoint bytes with the engine's rules.
pub fn classify_checkpoint(engine: Engine, raw: &[u8]) -> CheckpointKind {
    match engine {
        Engine::Postgres => kind(&crate::postgres::PgOrder, raw),
        Engine::Mysql => kind(&crate::mysql::MyOrder, raw),
    }
}

/// Why a stream checkpoint is not provably of the source's lineage, or
/// `None` when it is: a MySQL position must carry the source's current
/// registry lineage hash; a stamped PostgreSQL position must carry the
/// source's continuity chain (an unstamped one predates continuity records
/// and is the source's by its key).
pub async fn stream_checkpoint_foreign(
    engine: Engine,
    backend: &storage::ArcStorageBackend,
    source_id: &str,
    raw: &[u8],
    lineage_hash: &str,
) -> anyhow::Result<Option<String>> {
    match engine {
        Engine::Mysql => {
            let cp: crate::mysql::MySqlCheckpoint =
                serde_json::from_slice(raw)?;
            Ok(match cp.lineage.as_deref() {
                Some(h) if h == lineage_hash => None,
                Some(_) => Some("it belongs to another server lineage".into()),
                None => Some(
                    "it predates lineage recording, so its server cannot be \
                     verified"
                        .into(),
                ),
            })
        }
        Engine::Postgres => {
            let cp: crate::postgres::PostgresCheckpoint =
                serde_json::from_slice(raw)?;
            let Some(chain) = cp.chain else {
                return Ok(None);
            };
            let record = crate::postgres::postgres_continuity::load_record(
                backend, source_id,
            )
            .await?;
            Ok(match record {
                Some(r) if r.chain_id == chain => None,
                _ => Some(
                    "its continuity chain is not the source's recorded one"
                        .into(),
                ),
            })
        }
    }
}

/// The lineage a snapshot generation records for the source's recorded
/// lineage, when it has one (MySQL needs a parseable server UUID).
pub fn persisted_lineage(
    recorded: &LineageDescriptor,
) -> Option<PersistedLineage> {
    match recorded {
        LineageDescriptor::Postgres {
            system_identifier, ..
        } => Some(PersistedLineage::Postgres {
            system_identifier: *system_identifier,
        }),
        LineageDescriptor::Mysql { server_uuid } => {
            crate::mysql::mysql_event_id::parse_uuid16(server_uuid)
                .map(|source_uuid| PersistedLineage::MysqlGtid { source_uuid })
        }
    }
}

/// Whether a control record's lineage is the source's recorded lineage.
/// `None`: they cannot be related (a non-GTID MySQL lineage, or an
/// unparseable recorded one), which is a manual repair.
pub fn lineage_matches(
    control: &PersistedLineage,
    recorded: &LineageDescriptor,
) -> Option<bool> {
    match (control, persisted_lineage(recorded)?) {
        (PersistedLineage::MysqlServer { .. }, _) => None,
        (c, r) => Some(c.stable_matches(&r)),
    }
}

/// The configuration fingerprint a source of `engine` capturing `tables`
/// freezes into its generations (format 3: the table patterns).
pub fn config_fingerprint(engine: Engine, tables: &[String]) -> String {
    SnapshotFingerprintBuilder::new(engine.name(), tables)
        .finish()
        .as_str()
        .to_string()
}

/// Namespace of single-use recovery authorizations.
pub const AUTHORIZATION_NS: &str = "recovery.authorization";

fn slot_authorization_key(source_id: &str) -> String {
    format!("pg_slot_recreation:{source_id}")
}

/// A single-use authorization to recreate a PostgreSQL slot this source
/// created and lost (`docs/design/recovery-cli.md`, section 5.1): written
/// by `resnapshot`, consumed by the start of exactly its generation before
/// the slot is created.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SlotRecreation {
    pub format: u32,
    pub source: String,
    pub pipeline: String,
    pub slot: String,
    /// The recovery generation whose start may recreate the slot.
    pub generation: u64,
    /// The digest of the ownership record that proved the slot was this
    /// source's; a changed record voids the authorization.
    pub owner_record: String,
    /// The recovery proof that authorized it (empty in the plan: the proof
    /// cannot be part of what it proves).
    pub proof: String,
    pub state: SlotRecreationState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SlotRecreationState {
    Authorized,
    /// Taken by the start of its generation, before the slot was created.
    Consumed,
}

impl SlotRecreation {
    pub fn authorized(
        source: &str,
        pipeline: &str,
        slot: &str,
        generation: u64,
        owner_record: &str,
    ) -> Self {
        Self {
            format: 1,
            source: source.into(),
            pipeline: pipeline.into(),
            slot: slot.into(),
            generation,
            owner_record: owner_record.into(),
            proof: String::new(),
            state: SlotRecreationState::Authorized,
        }
    }

    pub fn digest(&self) -> String {
        hex::encode(Sha256::digest(
            serde_json::to_vec(self).expect("an authorization serializes"),
        ))
    }
}

/// The stored slot recreation authorization of `source_id` and its
/// version. An unreadable one is an error (never overwritten).
pub async fn read_slot_recreation(
    backend: &storage::ArcStorageBackend,
    source_id: &str,
) -> anyhow::Result<Option<(u64, SlotRecreation)>> {
    let Some((v, bytes)) = backend
        .slot_get(AUTHORIZATION_NS, &slot_authorization_key(source_id))
        .await?
    else {
        return Ok(None);
    };
    let rec: SlotRecreation = serde_json::from_slice(&bytes).map_err(|e| {
        anyhow::Error::new(storage::adapters::CorruptRecord(format!(
            "slot recreation authorization of {source_id}: {e}"
        )))
    })?;
    anyhow::ensure!(
        rec.format == 1,
        storage::adapters::CorruptRecord(format!(
            "slot recreation authorization of {source_id}: format {}",
            rec.format
        ))
    );
    Ok(Some((v, rec)))
}

/// Write `rec` over exactly the stored version `expected` (or create it).
pub async fn write_slot_recreation(
    backend: &storage::ArcStorageBackend,
    expected: Option<u64>,
    rec: &SlotRecreation,
) -> anyhow::Result<()> {
    let key = slot_authorization_key(&rec.source);
    let bytes = serde_json::to_vec(rec)?;
    let written = match expected {
        None => backend
            .slot_create(AUTHORIZATION_NS, &key, &bytes)
            .await?
            .is_some(),
        Some(v) => backend.slot_cas(AUTHORIZATION_NS, &key, v, &bytes).await?,
    };
    anyhow::ensure!(
        written,
        "the slot recreation authorization of {} changed concurrently",
        rec.source
    );
    Ok(())
}

/// Consume the authorization to recreate `slot` at the start of
/// `generation`: only an `authorized` record of exactly this source,
/// pipeline, slot, generation and ownership record, by one CAS. Returns
/// the consumed authorization, or `None` when there is none to consume.
pub async fn consume_slot_recreation(
    backend: &storage::ArcStorageBackend,
    source_id: &str,
    pipeline: &str,
    slot: &str,
    generation: u64,
    owner_record: &str,
) -> anyhow::Result<Option<SlotRecreation>> {
    let Some((v, rec)) = read_slot_recreation(backend, source_id).await? else {
        return Ok(None);
    };
    let matches = rec.state == SlotRecreationState::Authorized
        && rec.source == source_id
        && rec.pipeline == pipeline
        && rec.slot == slot
        && rec.generation == generation
        && rec.owner_record == owner_record;
    if !matches {
        return Ok(None);
    }
    let consumed = SlotRecreation {
        state: SlotRecreationState::Consumed,
        ..rec
    };
    write_slot_recreation(backend, Some(v), &consumed).await?;
    Ok(Some(consumed))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoints_are_classified_by_the_engine_rules() {
        let snapshot =
            br#"{"snapshot":{"format":1,"generation":2,"anchor":{}}}"#;
        let future = br#"{"snapshot_adopted":{"format":99}}"#;
        for engine in [Engine::Postgres, Engine::Mysql] {
            assert_eq!(
                classify_checkpoint(engine, future),
                CheckpointKind::Malformed,
                "{engine:?}"
            );
            assert_ne!(
                classify_checkpoint(engine, snapshot),
                CheckpointKind::Stream,
                "{engine:?}"
            );
            assert_eq!(
                classify_checkpoint(engine, b"\xff garbage"),
                CheckpointKind::Malformed,
                "{engine:?}"
            );
        }
    }

    #[test]
    fn lineages_relate_only_by_stable_identity() {
        let pg = LineageDescriptor::Postgres {
            system_identifier: 7,
            database_oid: 5,
        };
        assert_eq!(
            lineage_matches(
                &PersistedLineage::Postgres {
                    system_identifier: 7
                },
                &pg
            ),
            Some(true)
        );
        assert_eq!(
            lineage_matches(
                &PersistedLineage::Postgres {
                    system_identifier: 8
                },
                &pg
            ),
            Some(false)
        );
        let my = LineageDescriptor::Mysql {
            server_uuid: "3e11fa47-71ca-11e1-9e33-c80aa9429562".into(),
        };
        let uuid = persisted_lineage(&my).unwrap();
        assert_eq!(lineage_matches(&uuid, &my), Some(true));
        assert_eq!(
            lineage_matches(
                &PersistedLineage::Postgres {
                    system_identifier: 7
                },
                &my
            ),
            Some(false)
        );
        assert_eq!(
            lineage_matches(
                &PersistedLineage::MysqlServer {
                    server_id: 1,
                    file: "b.1".into()
                },
                &my
            ),
            None
        );
        assert_eq!(
            persisted_lineage(&LineageDescriptor::Mysql {
                server_uuid: "x".into()
            }),
            None
        );
    }

    #[tokio::test]
    async fn a_slot_recreation_is_consumed_once_by_its_generation() {
        let b: storage::ArcStorageBackend =
            std::sync::Arc::new(storage::MemoryStorageBackend::new());
        let auth = SlotRecreation::authorized("src", "p", "slot", 3, "owner");
        write_slot_recreation(&b, None, &auth).await.unwrap();
        // Another generation, slot, pipeline or ownership record: nothing.
        for (p, s, g, o) in [
            ("p", "slot", 2, "owner"),
            ("p", "other", 3, "owner"),
            ("q", "slot", 3, "owner"),
            ("p", "slot", 3, "changed"),
        ] {
            assert!(
                consume_slot_recreation(&b, "src", p, s, g, o)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        let taken = consume_slot_recreation(&b, "src", "p", "slot", 3, "owner")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(taken.state, SlotRecreationState::Consumed);
        // Single use.
        assert!(
            consume_slot_recreation(&b, "src", "p", "slot", 3, "owner")
                .await
                .unwrap()
                .is_none()
        );
        // An unreadable record is an error, never consumed or replaced.
        b.slot_upsert(AUTHORIZATION_NS, "pg_slot_recreation:bad", b"{")
            .await
            .unwrap();
        assert!(read_slot_recreation(&b, "bad").await.is_err());
    }
}
