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
/// by `resnapshot` (`authorized`), taken by the start of exactly its
/// generation before the slot is created (`consumed`), and completed once
/// the slot exists (`created`). Every state stays bound to the generation,
/// proof, slot and the ownership record it was authorized over, so a start
/// interrupted after taking it finishes the creation, and one interrupted
/// after creating the slot recognizes it; no other generation uses it.
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
    /// Once `created`: what the creation left.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created: Option<SlotCreated>,
}

/// The slot an authorized recreation created.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SlotCreated {
    /// The digest of the ownership record the creation wrote.
    pub owner_record: String,
    /// The new slot's consistent point.
    pub consistent_lsn: String,
    /// When it was recorded (the audit entry's time, fixed for repairs).
    pub at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SlotRecreationState {
    Authorized,
    /// Taken by the start of its generation, before the slot was created.
    Consumed,
    /// The slot was created by that start.
    Created,
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
            created: None,
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

fn same_target(
    rec: &SlotRecreation,
    source_id: &str,
    pipeline: &str,
    slot: &str,
    generation: u64,
) -> bool {
    rec.source == source_id
        && rec.pipeline == pipeline
        && rec.slot == slot
        && rec.generation == generation
}

/// Before the start of `generation` creates the missing `slot`: take the
/// authorization (`authorized` over exactly `owner_record`, by one CAS to
/// `consumed`), or resume one this generation already took (`consumed`,
/// with the ownership record still the authorized one, or replaced by the
/// creation's own `Creating` intent). `None`: no authorization for this
/// creation.
pub async fn take_slot_recreation(
    backend: &storage::ArcStorageBackend,
    source_id: &str,
    pipeline: &str,
    slot: &str,
    generation: u64,
    owner_record: &str,
    owner_creating: bool,
) -> anyhow::Result<Option<SlotRecreation>> {
    let Some((v, rec)) = read_slot_recreation(backend, source_id).await? else {
        return Ok(None);
    };
    if !same_target(&rec, source_id, pipeline, slot, generation) {
        return Ok(None);
    }
    match rec.state {
        SlotRecreationState::Authorized
            if !owner_creating && rec.owner_record == owner_record =>
        {
            let consumed = SlotRecreation {
                state: SlotRecreationState::Consumed,
                ..rec
            };
            write_slot_recreation(backend, Some(v), &consumed).await?;
            Ok(Some(consumed))
        }
        SlotRecreationState::Consumed
            if owner_creating || rec.owner_record == owner_record =>
        {
            Ok(Some(rec))
        }
        _ => Ok(None),
    }
}

/// Record that the start of `generation` created the slot (`consumed` to
/// `created`, once): the ownership record it wrote and its consistent
/// point. Already `created` with exactly these: unchanged. Returns the
/// record, with the audit time to use.
pub async fn complete_slot_recreation(
    backend: &storage::ArcStorageBackend,
    source_id: &str,
    pipeline: &str,
    slot: &str,
    generation: u64,
    owner_record: &str,
    consistent_lsn: &str,
) -> anyhow::Result<SlotRecreation> {
    let Some((v, rec)) = read_slot_recreation(backend, source_id).await? else {
        anyhow::bail!(
            "the slot recreation authorization of {source_id} is gone"
        );
    };
    anyhow::ensure!(
        same_target(&rec, source_id, pipeline, slot, generation),
        "the slot recreation authorization of {source_id} is for another \
         generation or slot"
    );
    match (&rec.state, &rec.created) {
        (SlotRecreationState::Created, Some(c))
            if c.owner_record == owner_record
                && c.consistent_lsn == consistent_lsn =>
        {
            Ok(rec)
        }
        (SlotRecreationState::Consumed, None) => {
            let created = SlotRecreation {
                state: SlotRecreationState::Created,
                created: Some(SlotCreated {
                    owner_record: owner_record.to_string(),
                    consistent_lsn: consistent_lsn.to_string(),
                    at_ms: chrono::Utc::now().timestamp_millis(),
                }),
                ..rec
            };
            write_slot_recreation(backend, Some(v), &created).await?;
            Ok(created)
        }
        _ => anyhow::bail!(
            "the slot recreation authorization of {source_id} does not match \
             the slot that exists"
        ),
    }
}

/// The authorization of `generation`'s recreation of `slot`, if any.
pub async fn slot_recreation_of(
    backend: &storage::ArcStorageBackend,
    source_id: &str,
    pipeline: &str,
    slot: &str,
    generation: u64,
) -> anyhow::Result<Option<SlotRecreation>> {
    Ok(read_slot_recreation(backend, source_id)
        .await?
        .map(|(_, r)| r)
        .filter(|r| same_target(r, source_id, pipeline, slot, generation)))
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
    async fn a_slot_recreation_is_taken_resumed_and_completed_by_its_generation_only()
     {
        let b: storage::ArcStorageBackend =
            std::sync::Arc::new(storage::MemoryStorageBackend::new());
        let take = |p: &'static str,
                    s: &'static str,
                    g: u64,
                    o: &'static str,
                    creating: bool| {
            let b = b.clone();
            async move {
                take_slot_recreation(&b, "src", p, s, g, o, creating)
                    .await
                    .unwrap()
            }
        };
        let auth = SlotRecreation::authorized("src", "p", "slot", 3, "owner");
        write_slot_recreation(&b, None, &auth).await.unwrap();
        // Another generation, slot, pipeline or ownership record: nothing,
        // and the authorization is untouched.
        for (p, s, g, o) in [
            ("p", "slot", 2, "owner"),
            ("p", "slot", 4, "owner"),
            ("p", "other", 3, "owner"),
            ("q", "slot", 3, "owner"),
            ("p", "slot", 3, "changed"),
        ] {
            assert!(take(p, s, g, o, false).await.is_none());
        }
        assert!(
            take("p", "slot", 3, "owner", true).await.is_none(),
            "an authorized one needs the authorized record"
        );
        let (_, still) =
            read_slot_recreation(&b, "src").await.unwrap().unwrap();
        assert_eq!(still.state, SlotRecreationState::Authorized);
        // Taken: consumed, still bound to everything.
        let taken = take("p", "slot", 3, "owner", false).await.unwrap();
        assert_eq!(taken.state, SlotRecreationState::Consumed);
        assert_eq!(
            (taken.generation, taken.owner_record.as_str()),
            (3, "owner")
        );
        // A start of the same generation interrupted before creating:
        // resumed (also over the creation's Creating intent); never by
        // another generation.
        assert!(take("p", "slot", 3, "owner", false).await.is_some());
        assert!(take("p", "slot", 3, "intent", true).await.is_some());
        assert!(take("p", "slot", 3, "changed", false).await.is_none());
        assert!(take("p", "slot", 4, "owner", false).await.is_none());
        // Completed once; the same completion again is a no-op; another
        // slot state is refused.
        let done =
            complete_slot_recreation(&b, "src", "p", "slot", 3, "new", "0/10")
                .await
                .unwrap();
        assert_eq!(done.state, SlotRecreationState::Created);
        let again =
            complete_slot_recreation(&b, "src", "p", "slot", 3, "new", "0/10")
                .await
                .unwrap();
        assert_eq!(again, done, "the same audit time");
        assert!(
            complete_slot_recreation(
                &b, "src", "p", "slot", 3, "other", "0/10"
            )
            .await
            .is_err()
        );
        assert!(
            complete_slot_recreation(&b, "src", "p", "slot", 4, "new", "0/10")
                .await
                .is_err()
        );
        // Created: never taken again, by any generation.
        assert!(take("p", "slot", 3, "new", false).await.is_none());
        assert!(take("p", "slot", 3, "owner", false).await.is_none());
        assert!(
            slot_recreation_of(&b, "src", "p", "slot", 4)
                .await
                .unwrap()
                .is_none()
        );
        // An unreadable record is an error, never taken or replaced.
        b.slot_upsert(AUTHORIZATION_NS, "pg_slot_recreation:bad", b"{")
            .await
            .unwrap();
        assert!(read_slot_recreation(&b, "bad").await.is_err());
    }
}
