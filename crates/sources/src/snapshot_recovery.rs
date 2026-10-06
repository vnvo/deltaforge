//! What the recovery operation `resnapshot` needs from the snapshot state
//! (`docs/design/recovery-cli.md`, section 5.1): classifying a source's
//! checkpoints with the engine's own rules, relating a control record's
//! lineage to the source's recorded lineage, and the inputs of a first
//! recovery allocation, all without a running source.

use deltaforge_core::CheckpointOrder;
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

/// The chain of a first recovery allocation: derived from what the plan
/// binds, so recomputing the plan yields the same chain.
pub fn recovery_chain(
    source_id: &str,
    lineage_hash: &str,
    bound: &str,
) -> String {
    let mut h = Sha256::new();
    for part in [
        b"dfsnaprecoverychain:v1".as_slice(),
        source_id.as_bytes(),
        lineage_hash.as_bytes(),
        bound.as_bytes(),
    ] {
        h.update((part.len() as u64).to_be_bytes());
        h.update(part);
    }
    hex::encode(&h.finalize()[..16])
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

    #[test]
    fn the_recovery_chain_is_deterministic() {
        let a = recovery_chain("s", "l", "b");
        assert_eq!(a, recovery_chain("s", "l", "b"));
        assert_eq!(a.len(), 32);
        assert_ne!(a, recovery_chain("s", "l", "c"));
    }
}
