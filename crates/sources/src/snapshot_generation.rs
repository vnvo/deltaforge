//! Durable, atomically-allocated **snapshot generation** lifecycle.
//!
//! A snapshot generation is a monotonic id minted when a (re)snapshot starts.
//! It is embedded in every snapshot row's stable [`EventId`], so it must be:
//!
//! - **durable** (not process-local) — a resumed snapshot reuses it;
//! - **atomically allocated** — two starts can never mint the same generation
//!   for two *different* snapshot passes (via [`SnapshotStateStore`] CAS);
//! - captured **with its lineage and config fingerprint**, all persisted
//!   together **before the first row is emitted**.
//!
//! The persisted lineage is captured **once** at creation and reused verbatim;
//! in particular the MySQL non-GTID fallback file is **not** recomputed from the
//! current binlog filename on resume (rotation must not change ids for the same
//! generation).
//!
//! **Boundary:** CAS guarantees a *unique* allocation but does not stop two
//! separate process owners from each running a *different* generation. Until
//! leases/fencing exist, snapshot execution keeps a single-owner constraint —
//! see [`SnapshotStateStore`].

use checkpoints::{CasOutcome, CheckpointError, SnapshotStateStore};
use deltaforge_core::SourceLineage;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Owned, persistable form of [`SourceLineage`] — the immutable cluster/source
/// identity captured once when a generation is created.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PersistedLineage {
    /// PostgreSQL cluster `system_identifier`.
    Postgres { system_identifier: u64 },
    /// MySQL GTID lineage — the server UUID (`@@server_uuid`).
    MysqlGtid { source_uuid: [u8; 16] },
    /// MySQL non-GTID fallback — server id + the binlog file at creation time.
    MysqlServer { server_id: u32, file: String },
}

impl PersistedLineage {
    /// Borrow as a core [`SourceLineage`] for id derivation.
    pub fn as_source_lineage(&self) -> SourceLineage<'_> {
        match self {
            PersistedLineage::Postgres { system_identifier } => {
                SourceLineage::Postgres {
                    system_identifier: *system_identifier,
                }
            }
            PersistedLineage::MysqlGtid { source_uuid } => {
                SourceLineage::MysqlGtid {
                    source_uuid: *source_uuid,
                }
            }
            PersistedLineage::MysqlServer { server_id, file } => {
                SourceLineage::MysqlServer {
                    server_id: *server_id,
                    file,
                }
            }
        }
    }

    /// Whether two lineages refer to the same source cluster, comparing only the
    /// **stable** identity. The MySQL fallback file is deliberately ignored — it
    /// rotates within one lineage, and treating a rotation as a new lineage
    /// would force a spurious re-snapshot.
    pub fn stable_matches(&self, other: &PersistedLineage) -> bool {
        use PersistedLineage::*;
        match (self, other) {
            (
                Postgres {
                    system_identifier: a,
                },
                Postgres {
                    system_identifier: b,
                },
            ) => a == b,
            (MysqlGtid { source_uuid: a }, MysqlGtid { source_uuid: b }) => {
                a == b
            }
            (
                MysqlServer { server_id: a, .. },
                MysqlServer { server_id: b, .. },
            ) => a == b,
            _ => false,
        }
    }
}

/// Lifecycle status of a snapshot generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SnapshotStatus {
    /// Allocated (persisted) but no rows emitted yet.
    Allocated,
    /// Rows are being emitted.
    Running,
    /// Snapshot finished.
    Completed,
}

/// The durable record for a source's current snapshot generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotGenerationRecord {
    /// Monotonic generation number (starts at 1).
    pub generation: u64,
    /// Lineage captured at creation, reused verbatim for id derivation.
    pub lineage: PersistedLineage,
    /// Lifecycle status.
    pub status: SnapshotStatus,
    /// Fingerprint of the identity-relevant snapshot configuration.
    pub config_fingerprint: String,
}

/// One table's identity-relevant configuration, hashed into the fingerprint.
#[derive(Debug, Clone)]
pub struct TableIdentitySpec {
    /// Database name.
    pub db: String,
    /// Schema name (PostgreSQL), if any.
    pub schema: Option<String>,
    /// Table name.
    pub table: String,
    /// Effective identity columns **in declared order** (order is significant —
    /// it changes composite-key ids, so a reorder must change the fingerprint).
    pub identity_columns: Vec<String>,
}

/// A stable fingerprint over the identity-relevant snapshot configuration
/// (the set of tables and each table's effective identity columns). A resume
/// may reuse a generation only when this matches the persisted one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotConfigFingerprint(String);

impl SnapshotConfigFingerprint {
    /// Compute the fingerprint. Independent of the *listing order* of tables
    /// (they are sorted), but sensitive to each table's identity and to the
    /// **order** of its identity columns.
    pub fn compute(tables: &[TableIdentitySpec]) -> Self {
        let mut canon: Vec<String> = tables
            .iter()
            .map(|t| {
                format!(
                    "{}\u{1f}{}\u{1f}{}\u{1f}{}",
                    t.db,
                    t.schema.as_deref().unwrap_or(""),
                    t.table,
                    t.identity_columns.join("\u{1f}"),
                )
            })
            .collect();
        canon.sort();
        let mut h = Sha256::new();
        h.update(b"dfsnapfp:v1");
        for row in &canon {
            h.update((row.len() as u64).to_be_bytes());
            h.update(row.as_bytes());
        }
        SnapshotConfigFingerprint(hex::encode(h.finalize()))
    }

    /// The fingerprint as a hex string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// How a generation should be allocated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AllocationMode {
    /// Resume: reuse the existing generation iff fingerprint + stable lineage
    /// match; otherwise a new snapshot is required (does not silently reuse).
    Resume,
    /// Explicit re-snapshot: atomically allocate a brand-new generation.
    ForceNew,
}

/// The result of a successful allocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllocatedGeneration {
    /// Store CAS version, needed to update the record's status later.
    pub version: u64,
    /// The allocated (or resumed) record.
    pub record: SnapshotGenerationRecord,
}

/// Errors from generation allocation.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotGenerationError {
    /// The persisted config fingerprint or stable lineage no longer matches;
    /// the operator must trigger an explicit re-snapshot rather than resume.
    #[error(
        "snapshot config or lineage changed since generation {generation}; \
         an explicit re-snapshot is required"
    )]
    ConfigChanged {
        /// The generation whose config no longer matches.
        generation: u64,
    },
    /// The state store cannot provide atomic allocation — snapshot must not
    /// proceed. Surfaced *before* any row is emitted.
    #[error("snapshot state store does not support atomic allocation: {0}")]
    UnsupportedAtomic(String),
    /// A stored record could not be decoded.
    #[error("corrupt snapshot generation record: {0}")]
    Corrupt(String),
    /// The underlying state store failed.
    #[error("snapshot state store error: {0}")]
    Store(String),
    /// Too many concurrent writers; CAS retry budget exhausted.
    #[error("exceeded retry budget allocating snapshot generation")]
    RetryExhausted,
}

fn map_store_err(e: CheckpointError) -> SnapshotGenerationError {
    match e {
        CheckpointError::UnsupportedAtomicOperation(m) => {
            SnapshotGenerationError::UnsupportedAtomic(m)
        }
        other => SnapshotGenerationError::Store(other.to_string()),
    }
}

fn decode(
    bytes: &[u8],
) -> Result<SnapshotGenerationRecord, SnapshotGenerationError> {
    serde_json::from_slice(bytes)
        .map_err(|e| SnapshotGenerationError::Corrupt(e.to_string()))
}

fn encode(
    rec: &SnapshotGenerationRecord,
) -> Result<Vec<u8>, SnapshotGenerationError> {
    serde_json::to_vec(rec)
        .map_err(|e| SnapshotGenerationError::Corrupt(e.to_string()))
}

const MAX_RETRIES: usize = 16;

/// Allocate (or resume) the snapshot generation for `key`, persisting the
/// generation, lineage, and fingerprint together **before** returning — the
/// caller emits rows only after this succeeds.
pub async fn allocate_generation(
    store: &dyn SnapshotStateStore,
    key: &str,
    current_lineage: PersistedLineage,
    fingerprint: &SnapshotConfigFingerprint,
    mode: AllocationMode,
) -> Result<AllocatedGeneration, SnapshotGenerationError> {
    for _ in 0..MAX_RETRIES {
        let current = store.get_versioned(key).await.map_err(map_store_err)?;

        match mode {
            AllocationMode::Resume => match &current {
                None => {
                    // First ever start: create generation 1 atomically.
                    let rec = SnapshotGenerationRecord {
                        generation: 1,
                        lineage: current_lineage.clone(),
                        status: SnapshotStatus::Allocated,
                        config_fingerprint: fingerprint.as_str().to_string(),
                    };
                    match store
                        .compare_and_swap(key, None, &encode(&rec)?)
                        .await
                        .map_err(map_store_err)?
                    {
                        CasOutcome::Committed { version } => {
                            return Ok(AllocatedGeneration {
                                version,
                                record: rec,
                            });
                        }
                        // Lost the create race — retry; the winner's record is
                        // now present and will be evaluated on the next pass.
                        CasOutcome::Mismatch { .. } => continue,
                    }
                }
                Some((version, bytes)) => {
                    let rec = decode(bytes)?;
                    // Reuse only when both the config and the *stable* lineage
                    // match. The persisted record (incl. its original file) is
                    // returned unchanged — never recomputed from current state.
                    if rec.config_fingerprint == fingerprint.as_str()
                        && rec.lineage.stable_matches(&current_lineage)
                    {
                        return Ok(AllocatedGeneration {
                            version: *version,
                            record: rec,
                        });
                    }
                    return Err(SnapshotGenerationError::ConfigChanged {
                        generation: rec.generation,
                    });
                }
            },
            AllocationMode::ForceNew => {
                let (expected, next_gen) = match &current {
                    Some((v, bytes)) => {
                        (Some(*v), decode(bytes)?.generation + 1)
                    }
                    None => (None, 1),
                };
                let rec = SnapshotGenerationRecord {
                    generation: next_gen,
                    lineage: current_lineage.clone(),
                    status: SnapshotStatus::Allocated,
                    config_fingerprint: fingerprint.as_str().to_string(),
                };
                match store
                    .compare_and_swap(key, expected, &encode(&rec)?)
                    .await
                    .map_err(map_store_err)?
                {
                    CasOutcome::Committed { version } => {
                        return Ok(AllocatedGeneration {
                            version,
                            record: rec,
                        });
                    }
                    CasOutcome::Mismatch { .. } => continue,
                }
            }
        }
    }
    Err(SnapshotGenerationError::RetryExhausted)
}

/// Update the status of the current generation record (e.g. `Running`,
/// `Completed`), retrying on concurrent version changes.
pub async fn update_status(
    store: &dyn SnapshotStateStore,
    key: &str,
    status: SnapshotStatus,
) -> Result<AllocatedGeneration, SnapshotGenerationError> {
    for _ in 0..MAX_RETRIES {
        let (version, bytes) = store
            .get_versioned(key)
            .await
            .map_err(map_store_err)?
            .ok_or_else(|| {
                SnapshotGenerationError::Corrupt(
                    "no generation record to update".into(),
                )
            })?;
        let mut rec = decode(&bytes)?;
        rec.status = status;
        match store
            .compare_and_swap(key, Some(version), &encode(&rec)?)
            .await
            .map_err(map_store_err)?
        {
            CasOutcome::Committed { version } => {
                return Ok(AllocatedGeneration {
                    version,
                    record: rec,
                });
            }
            CasOutcome::Mismatch { .. } => continue,
        }
    }
    Err(SnapshotGenerationError::RetryExhausted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use checkpoints::{CheckpointResult, MemCheckpointStore};
    use std::sync::Arc;

    fn pg(sysid: u64) -> PersistedLineage {
        PersistedLineage::Postgres {
            system_identifier: sysid,
        }
    }

    fn fp(cols: &[&str]) -> SnapshotConfigFingerprint {
        SnapshotConfigFingerprint::compute(&[TableIdentitySpec {
            db: "shop".into(),
            schema: Some("public".into()),
            table: "orders".into(),
            identity_columns: cols.iter().map(|s| s.to_string()).collect(),
        }])
    }

    const KEY: &str = "snapshot_generation:src-1";

    #[tokio::test]
    async fn fresh_allocation_starts_at_generation_1() {
        let store = MemCheckpointStore::new().unwrap();
        let a = allocate_generation(
            &store,
            KEY,
            pg(1),
            &fp(&["id"]),
            AllocationMode::Resume,
        )
        .await
        .unwrap();
        assert_eq!(a.record.generation, 1);
        assert_eq!(a.record.status, SnapshotStatus::Allocated);
        assert_eq!(a.record.lineage, pg(1));
    }

    #[tokio::test]
    async fn resume_reuses_generation_lineage_and_fingerprint() {
        let store = MemCheckpointStore::new().unwrap();
        let first = allocate_generation(
            &store,
            KEY,
            pg(1),
            &fp(&["id"]),
            AllocationMode::Resume,
        )
        .await
        .unwrap();
        let resumed = allocate_generation(
            &store,
            KEY,
            pg(1),
            &fp(&["id"]),
            AllocationMode::Resume,
        )
        .await
        .unwrap();
        assert_eq!(resumed.record.generation, first.record.generation);
        assert_eq!(resumed.record, first.record);
    }

    #[tokio::test]
    async fn resume_reuses_persisted_mysql_file_after_rotation() {
        let store = MemCheckpointStore::new().unwrap();
        let created = PersistedLineage::MysqlServer {
            server_id: 7,
            file: "mysql-bin.000008".into(),
        };
        allocate_generation(
            &store,
            KEY,
            created.clone(),
            &fp(&["id"]),
            AllocationMode::Resume,
        )
        .await
        .unwrap();

        // Binlog has since rotated; resume passes the *current* (rotated) file.
        let rotated = PersistedLineage::MysqlServer {
            server_id: 7,
            file: "mysql-bin.000009".into(),
        };
        let resumed = allocate_generation(
            &store,
            KEY,
            rotated,
            &fp(&["id"]),
            AllocationMode::Resume,
        )
        .await
        .unwrap();
        // The persisted (creation-time) file is reused — not the rotated one —
        // so ids stay stable across rotation.
        assert_eq!(resumed.record.lineage, created);
    }

    #[tokio::test]
    async fn resume_with_changed_fingerprint_requires_new_snapshot() {
        let store = MemCheckpointStore::new().unwrap();
        allocate_generation(
            &store,
            KEY,
            pg(1),
            &fp(&["id"]),
            AllocationMode::Resume,
        )
        .await
        .unwrap();
        let err = allocate_generation(
            &store,
            KEY,
            pg(1),
            &fp(&["id", "region"]), // identity columns changed
            AllocationMode::Resume,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            SnapshotGenerationError::ConfigChanged { generation: 1 }
        ));
    }

    #[tokio::test]
    async fn resume_with_changed_lineage_requires_new_snapshot() {
        let store = MemCheckpointStore::new().unwrap();
        allocate_generation(
            &store,
            KEY,
            pg(1),
            &fp(&["id"]),
            AllocationMode::Resume,
        )
        .await
        .unwrap();
        let err = allocate_generation(
            &store,
            KEY,
            pg(2), // different cluster
            &fp(&["id"]),
            AllocationMode::Resume,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, SnapshotGenerationError::ConfigChanged { .. }));
    }

    #[tokio::test]
    async fn force_new_increments_generation() {
        let store = MemCheckpointStore::new().unwrap();
        allocate_generation(
            &store,
            KEY,
            pg(1),
            &fp(&["id"]),
            AllocationMode::Resume,
        )
        .await
        .unwrap();
        let resnap = allocate_generation(
            &store,
            KEY,
            pg(1),
            &fp(&["id"]),
            AllocationMode::ForceNew,
        )
        .await
        .unwrap();
        assert_eq!(resnap.record.generation, 2);
    }

    #[tokio::test]
    async fn concurrent_resume_allocation_is_consistent() {
        // Many concurrent first-starts must converge on a single generation 1
        // with identical lineage/fingerprint — no double-mint.
        let store = Arc::new(MemCheckpointStore::new().unwrap());
        let mut handles = Vec::new();
        for _ in 0..16 {
            let s = store.clone();
            handles.push(tokio::spawn(async move {
                allocate_generation(
                    s.as_ref(),
                    KEY,
                    pg(1),
                    &fp(&["id"]),
                    AllocationMode::Resume,
                )
                .await
                .unwrap()
            }));
        }
        for h in handles {
            let a = h.await.unwrap();
            assert_eq!(a.record.generation, 1);
            assert_eq!(a.record.lineage, pg(1));
        }
    }

    #[tokio::test]
    async fn unsupported_atomic_store_fails_fast() {
        struct Unsupported;
        #[async_trait]
        impl SnapshotStateStore for Unsupported {
            async fn get_versioned(
                &self,
                _: &str,
            ) -> CheckpointResult<Option<(u64, Vec<u8>)>> {
                Err(CheckpointError::UnsupportedAtomicOperation("mock".into()))
            }
            async fn compare_and_swap(
                &self,
                _: &str,
                _: Option<u64>,
                _: &[u8],
            ) -> CheckpointResult<CasOutcome> {
                Err(CheckpointError::UnsupportedAtomicOperation("mock".into()))
            }
        }
        let err = allocate_generation(
            &Unsupported,
            KEY,
            pg(1),
            &fp(&["id"]),
            AllocationMode::Resume,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, SnapshotGenerationError::UnsupportedAtomic(_)));
    }

    #[test]
    fn fingerprint_ignores_table_order_but_tracks_identity() {
        let a = TableIdentitySpec {
            db: "d".into(),
            schema: None,
            table: "a".into(),
            identity_columns: vec!["id".into()],
        };
        let b = TableIdentitySpec {
            db: "d".into(),
            schema: None,
            table: "b".into(),
            identity_columns: vec!["k1".into(), "k2".into()],
        };
        let ab = SnapshotConfigFingerprint::compute(&[a.clone(), b.clone()]);
        let ba = SnapshotConfigFingerprint::compute(&[b, a]);
        assert_eq!(ab, ba, "listing order must not matter");

        // Composite-key column order is significant.
        let k12 = SnapshotConfigFingerprint::compute(&[TableIdentitySpec {
            db: "d".into(),
            schema: None,
            table: "t".into(),
            identity_columns: vec!["k1".into(), "k2".into()],
        }]);
        let k21 = SnapshotConfigFingerprint::compute(&[TableIdentitySpec {
            db: "d".into(),
            schema: None,
            table: "t".into(),
            identity_columns: vec!["k2".into(), "k1".into()],
        }]);
        assert_ne!(k12, k21, "identity column order must matter");
    }

    #[tokio::test]
    async fn update_status_transitions_and_persists() {
        let store = MemCheckpointStore::new().unwrap();
        allocate_generation(
            &store,
            KEY,
            pg(1),
            &fp(&["id"]),
            AllocationMode::Resume,
        )
        .await
        .unwrap();
        update_status(&store, KEY, SnapshotStatus::Completed)
            .await
            .unwrap();
        // A resume now sees the Completed status on the same generation.
        let resumed = allocate_generation(
            &store,
            KEY,
            pg(1),
            &fp(&["id"]),
            AllocationMode::Resume,
        )
        .await
        .unwrap();
        assert_eq!(resumed.record.status, SnapshotStatus::Completed);
        assert_eq!(resumed.record.generation, 1);
    }
}
