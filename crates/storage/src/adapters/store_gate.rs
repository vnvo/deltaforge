//! Durable, mutually exclusive store gate shared by the DeltaForge server and
//! the schema migration command.
//!
//! Exactly one record (`store_gate/gate`) holds the gate state and it changes
//! only by create-only insert or version compare-and-swap, so a server start
//! and a migration apply can never both acquire it. The server acquires it
//! before any schema access and releases it only after a clean shutdown; the
//! migration command acquires it before planning and releases it when done.
//!
//! Nothing expires. A process that dies while holding the gate leaves it held.
//! A server's gate is released only by an explicit [`break_gate`] naming the
//! recorded owner, after an operator has verified that the owner is no longer
//! alive. A migration's gate is never broken: the migration may have written
//! part of its plan, so the only way forward is [`resume_migration`], which
//! hands the held gate to a new process for the same proof without ever
//! unlocking it. This favours correctness over automatic stale-lock recovery.

use anyhow::Context;
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::ArcStorageBackend;

const NS: &str = "store_gate";
const KEY: &str = "gate";
const FORMAT_VERSION: u32 = 1;
/// CAS retries when the record changes between read and write.
const ACQUIRE_ATTEMPTS: usize = 8;

/// Who holds the gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateRole {
    Server,
    Migration,
}

impl std::fmt::Display for GateRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            GateRole::Server => "server",
            GateRole::Migration => "migration",
        })
    }
}

/// The recorded holder, for diagnostics and for an explicit break.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GateHolder {
    pub role: GateRole,
    /// Unique per acquisition (UUIDv4).
    pub owner_id: String,
    pub hostname: String,
    pub pid: u32,
    pub acquired_at_ms: i64,
    /// For a migration: the proof digest being applied. A resume must apply
    /// the same proof.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub migration_proof: Option<String>,
}

impl std::fmt::Display for GateHolder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let since =
            chrono::DateTime::from_timestamp_millis(self.acquired_at_ms)
                .map(|t| t.to_rfc3339())
                .unwrap_or_else(|| self.acquired_at_ms.to_string());
        write!(
            f,
            "{} (owner {}, host {}, pid {}, since {since}",
            self.role, self.owner_id, self.hostname, self.pid
        )?;
        if let Some(proof) = &self.migration_proof {
            write!(f, ", proof {proof}")?;
        }
        f.write_str(")")
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct GateRecord {
    format_version: u32,
    holder: Option<GateHolder>,
}

/// Current gate state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateState {
    Unlocked,
    Held(GateHolder),
}

/// What an operator can do about a gate held by `h`.
fn remedy(h: &GateHolder) -> String {
    match h.role {
        GateRole::Server => format!(
            "If that process is no longer running (for example it crashed), \
             verify it and release the gate with `deltaforge store-gate break \
             --owner {}`",
            h.owner_id
        ),
        GateRole::Migration => format!(
            "A schema migration holds it and may have written part of its \
             plan, so the gate cannot be broken. If that process is no longer \
             running, verify it and finish the migration with `deltaforge \
             schema-migrate --mapping <same mapping> --apply --expect-proof {} \
             --resume-owner {}`",
            h.migration_proof.as_deref().unwrap_or("<proof>"),
            h.owner_id
        ),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GateError {
    #[error("the store is held by {0}. {}", remedy(.0))]
    Held(GateHolder),

    #[error(
        "the store gate is held by an unfinished schema migration: {0}. {}",
        remedy(.0)
    )]
    MigrationUnfinished(GateHolder),

    #[error("cannot resume: {reason}; current state: {current}")]
    NotResumable { reason: String, current: String },

    #[error(
        "store gate is not held by owner {owner}; current state: {current}"
    )]
    NotOwner { owner: String, current: String },

    #[error("store gate kept changing while acquiring it; retry")]
    Contended,

    #[error("store gate record is unreadable or unsupported: {0}")]
    Corrupt(String),

    #[error("store gate storage failure: {0:#}")]
    Storage(anyhow::Error),
}

/// Proof of holding the gate. Release it explicitly with [`GateGuard::release`];
/// dropping it without releasing leaves the gate held, exactly like a crash.
#[derive(Debug)]
pub struct GateGuard {
    backend: ArcStorageBackend,
    holder: GateHolder,
}

impl GateGuard {
    pub fn holder(&self) -> &GateHolder {
        &self.holder
    }

    /// Release the gate (only if this guard's owner still holds it).
    pub async fn release(self) -> Result<(), GateError> {
        release_owned(&self.backend, &self.holder.owner_id)
            .await
            .map(|_| ())
    }
}

fn hostname() -> String {
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .ok()
        .map(|h| h.trim().to_string())
        .filter(|h| !h.is_empty())
        .or_else(|| std::env::var("HOSTNAME").ok())
        .unwrap_or_else(|| "unknown".to_string())
}

async fn read(
    backend: &ArcStorageBackend,
) -> Result<Option<(u64, GateRecord)>, GateError> {
    let Some((version, bytes)) = backend
        .slot_get(NS, KEY)
        .await
        .context("read store gate")
        .map_err(GateError::Storage)?
    else {
        return Ok(None);
    };
    let record: GateRecord = serde_json::from_slice(&bytes)
        .map_err(|e| GateError::Corrupt(e.to_string()))?;
    if record.format_version != FORMAT_VERSION {
        return Err(GateError::Corrupt(format!(
            "format_version {} (this build reads {FORMAT_VERSION})",
            record.format_version
        )));
    }
    Ok(Some((version, record)))
}

fn encode(holder: Option<&GateHolder>) -> Vec<u8> {
    serde_json::to_vec(&GateRecord {
        format_version: FORMAT_VERSION,
        holder: holder.cloned(),
    })
    .expect("gate record serializes")
}

/// Current state of the gate.
pub async fn status(
    backend: &ArcStorageBackend,
) -> Result<GateState, GateError> {
    Ok(match read(backend).await? {
        Some((
            _,
            GateRecord {
                holder: Some(h), ..
            },
        )) => GateState::Held(h),
        _ => GateState::Unlocked,
    })
}

fn new_holder(role: GateRole, migration_proof: Option<String>) -> GateHolder {
    GateHolder {
        role,
        owner_id: uuid::Uuid::new_v4().to_string(),
        hostname: hostname(),
        pid: std::process::id(),
        acquired_at_ms: Utc::now().timestamp_millis(),
        migration_proof,
    }
}

/// Acquire the gate for `role`. Fails with [`GateError::Held`] if anyone (any
/// role) holds it.
pub async fn acquire(
    backend: &ArcStorageBackend,
    role: GateRole,
) -> Result<GateGuard, GateError> {
    acquire_as(backend, new_holder(role, None)).await
}

/// Acquire the gate for a migration applying `proof`.
pub async fn acquire_for_migration(
    backend: &ArcStorageBackend,
    proof: &str,
) -> Result<GateGuard, GateError> {
    acquire_as(backend, new_holder(GateRole::Migration, Some(proof.into())))
        .await
}

async fn acquire_as(
    backend: &ArcStorageBackend,
    holder: GateHolder,
) -> Result<GateGuard, GateError> {
    for _ in 0..ACQUIRE_ATTEMPTS {
        let won = match read(backend).await? {
            None => backend
                .slot_create(NS, KEY, &encode(Some(&holder)))
                .await
                .context("create store gate")
                .map_err(GateError::Storage)?
                .is_some(),
            Some((
                _,
                GateRecord {
                    holder: Some(h), ..
                },
            )) => {
                return Err(GateError::Held(h));
            }
            Some((version, GateRecord { holder: None, .. })) => backend
                .slot_cas(NS, KEY, version, &encode(Some(&holder)))
                .await
                .context("acquire store gate")
                .map_err(GateError::Storage)?,
        };
        if won {
            return Ok(GateGuard {
                backend: backend.clone(),
                holder,
            });
        }
    }
    Err(GateError::Contended)
}

/// Take over a gate held by the unfinished migration `owner_id` for the same
/// `proof`, in one CAS: the gate passes from the old owner to a new one
/// without ever being unlocked. The operator must have verified that the old
/// owner is no longer running. Exactly one concurrent resume can win.
pub async fn resume_migration(
    backend: &ArcStorageBackend,
    owner_id: &str,
    proof: &str,
) -> Result<GateGuard, GateError> {
    let holder = new_holder(GateRole::Migration, Some(proof.into()));
    for _ in 0..ACQUIRE_ATTEMPTS {
        let (version, current) = match read(backend).await? {
            Some((
                version,
                GateRecord {
                    holder: Some(h), ..
                },
            )) => (version, h),
            _ => {
                return Err(GateError::NotResumable {
                    reason: "no migration holds the store gate".into(),
                    current: "unlocked".into(),
                });
            }
        };
        let reason = if current.role != GateRole::Migration {
            Some("the gate is not held by a migration".to_string())
        } else if current.owner_id != owner_id {
            Some(format!("the gate is not held by owner {owner_id}"))
        } else if current.migration_proof.as_deref() != Some(proof) {
            Some(format!(
                "the unfinished migration applies proof {}, not {proof}",
                current.migration_proof.as_deref().unwrap_or("(none)")
            ))
        } else {
            None
        };
        if let Some(reason) = reason {
            return Err(GateError::NotResumable {
                reason,
                current: format!("held by {current}"),
            });
        }
        if backend
            .slot_cas(NS, KEY, version, &encode(Some(&holder)))
            .await
            .context("take over store gate")
            .map_err(GateError::Storage)?
        {
            return Ok(GateGuard {
                backend: backend.clone(),
                holder,
            });
        }
    }
    Err(GateError::Contended)
}

/// Operator break after a crash: release a SERVER's gate if, and only if,
/// `owner_id` still holds it. A migration's gate is refused
/// ([`GateError::MigrationUnfinished`]); finish it with [`resume_migration`].
/// Returns the holder that was released.
pub async fn break_gate(
    backend: &ArcStorageBackend,
    owner_id: &str,
) -> Result<GateHolder, GateError> {
    if let Some((
        _,
        GateRecord {
            holder: Some(h), ..
        },
    )) = read(backend).await?
        && h.role == GateRole::Migration
    {
        return Err(GateError::MigrationUnfinished(h));
    }
    release_owned(backend, owner_id).await
}

/// Release the gate if, and only if, `owner_id` still holds it.
async fn release_owned(
    backend: &ArcStorageBackend,
    owner_id: &str,
) -> Result<GateHolder, GateError> {
    for _ in 0..ACQUIRE_ATTEMPTS {
        let (version, holder) = match read(backend).await? {
            Some((
                version,
                GateRecord {
                    holder: Some(h), ..
                },
            )) if h.owner_id == owner_id => (version, h),
            other => {
                return Err(GateError::NotOwner {
                    owner: owner_id.to_string(),
                    current: match other {
                        Some((
                            _,
                            GateRecord {
                                holder: Some(h), ..
                            },
                        )) => {
                            format!("held by {h}")
                        }
                        _ => "unlocked".to_string(),
                    },
                });
            }
        };
        if backend
            .slot_cas(NS, KEY, version, &encode(None))
            .await
            .context("release store gate")
            .map_err(GateError::Storage)?
        {
            return Ok(holder);
        }
    }
    Err(GateError::Contended)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryStorageBackend;
    use std::sync::Arc;

    fn backend() -> ArcStorageBackend {
        Arc::new(MemoryStorageBackend::new())
    }

    #[tokio::test]
    async fn acquire_release_cycle_for_both_roles() {
        let b = backend();
        assert_eq!(status(&b).await.unwrap(), GateState::Unlocked);
        let server = acquire(&b, GateRole::Server).await.unwrap();
        assert!(
            matches!(status(&b).await.unwrap(), GateState::Held(h) if h.role == GateRole::Server)
        );
        server.release().await.unwrap();
        assert_eq!(status(&b).await.unwrap(), GateState::Unlocked);
        let migration = acquire(&b, GateRole::Migration).await.unwrap();
        migration.release().await.unwrap();
        assert_eq!(status(&b).await.unwrap(), GateState::Unlocked);
    }

    #[tokio::test]
    async fn roles_exclude_each_other() {
        let b = backend();
        let server = acquire(&b, GateRole::Server).await.unwrap();
        let err = acquire(&b, GateRole::Migration).await.unwrap_err();
        assert!(
            matches!(err, GateError::Held(ref h) if h.owner_id == server.holder().owner_id)
        );
        assert!(err.to_string().contains("store-gate break --owner"));
        assert!(matches!(
            acquire(&b, GateRole::Server).await.unwrap_err(),
            GateError::Held(_)
        ));
    }

    #[tokio::test]
    async fn concurrent_acquires_have_exactly_one_winner() {
        for _ in 0..25 {
            let b = backend();
            let (a, m) = tokio::join!(
                acquire(&b, GateRole::Server),
                acquire(&b, GateRole::Migration)
            );
            assert_eq!(
                [a.is_ok(), m.is_ok()].iter().filter(|w| **w).count(),
                1,
                "exactly one role may hold the store"
            );
        }
    }

    #[tokio::test]
    async fn a_dropped_guard_stays_held_until_an_explicit_break() {
        let b = backend();
        let owner = {
            let guard = acquire(&b, GateRole::Server).await.unwrap();
            guard.holder().owner_id.clone()
            // dropped without release: models a crash
        };
        assert!(matches!(status(&b).await.unwrap(), GateState::Held(_)));
        assert!(acquire(&b, GateRole::Migration).await.is_err());

        // Breaking with the wrong owner changes nothing.
        let err = break_gate(&b, "not-the-owner").await.unwrap_err();
        assert!(matches!(err, GateError::NotOwner { .. }));
        assert!(matches!(status(&b).await.unwrap(), GateState::Held(_)));

        let released = break_gate(&b, &owner).await.unwrap();
        assert_eq!(released.owner_id, owner);
        assert_eq!(status(&b).await.unwrap(), GateState::Unlocked);
        // A second break of the same owner finds nothing to release.
        assert!(break_gate(&b, &owner).await.is_err());
    }

    #[tokio::test]
    async fn a_stale_guard_cannot_release_a_newer_holder() {
        let b = backend();
        let first = acquire(&b, GateRole::Server).await.unwrap();
        let first_owner = first.holder().owner_id.clone();
        break_gate(&b, &first_owner).await.unwrap(); // operator break after a "crash"
        let second = acquire(&b, GateRole::Migration).await.unwrap();
        // The original guard is released late: it must not free the new holder.
        assert!(first.release().await.is_err());
        assert!(
            matches!(status(&b).await.unwrap(), GateState::Held(h) if h.owner_id == second.holder().owner_id)
        );
    }

    #[tokio::test]
    async fn an_unfinished_migration_cannot_be_broken_only_resumed() {
        let b = backend();
        let owner = {
            let g = acquire_for_migration(&b, "proof-a").await.unwrap();
            g.holder().owner_id.clone()
            // dropped: the migration died mid-apply
        };

        // Nobody else gets in, and the operator break is refused.
        let held = acquire(&b, GateRole::Server).await.unwrap_err();
        assert!(held.to_string().contains("--resume-owner"), "{held}");
        assert!(held.to_string().contains("proof-a"), "{held}");
        assert!(matches!(
            break_gate(&b, &owner).await.unwrap_err(),
            GateError::MigrationUnfinished(h) if h.owner_id == owner
        ));

        // A resume must name the recorded owner and the same proof.
        for (who, proof) in [("someone-else", "proof-a"), (&*owner, "proof-b")]
        {
            assert!(matches!(
                resume_migration(&b, who, proof).await.unwrap_err(),
                GateError::NotResumable { .. }
            ));
        }

        // The handover never unlocks: the new owner holds it immediately and
        // the old owner id is no longer valid for anything.
        let resumed = resume_migration(&b, &owner, "proof-a").await.unwrap();
        assert_ne!(resumed.holder().owner_id, owner);
        assert!(matches!(
            status(&b).await.unwrap(),
            GateState::Held(h) if h.owner_id == resumed.holder().owner_id
                && h.role == GateRole::Migration
                && h.migration_proof.as_deref() == Some("proof-a")
        ));
        assert!(resume_migration(&b, &owner, "proof-a").await.is_err());
        assert!(release_owned(&b, &owner).await.is_err());

        resumed.release().await.unwrap();
        assert_eq!(status(&b).await.unwrap(), GateState::Unlocked);
    }

    #[tokio::test]
    async fn concurrent_resumes_have_exactly_one_winner() {
        for _ in 0..25 {
            let b = backend();
            let owner = acquire_for_migration(&b, "p")
                .await
                .unwrap()
                .holder()
                .owner_id
                .clone();
            let (x, y) = tokio::join!(
                resume_migration(&b, &owner, "p"),
                resume_migration(&b, &owner, "p")
            );
            assert_eq!(
                [x.is_ok(), y.is_ok()].iter().filter(|w| **w).count(),
                1
            );
        }
    }

    #[tokio::test]
    async fn a_server_gate_cannot_be_resumed_as_a_migration() {
        let b = backend();
        let owner = acquire(&b, GateRole::Server)
            .await
            .unwrap()
            .holder()
            .owner_id
            .clone();
        assert!(matches!(
            resume_migration(&b, &owner, "p").await.unwrap_err(),
            GateError::NotResumable { .. }
        ));
        break_gate(&b, &owner).await.unwrap();
    }

    /// Mutual exclusion on SQLite through two independent handles to one file,
    /// as a server and a migration process would open it.
    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn sqlite_gate_is_mutually_exclusive_across_handles() {
        let dir = std::env::temp_dir()
            .join(format!("gate_it_{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("store.db");
        let a: ArcStorageBackend =
            crate::SqliteStorageBackend::open(&path).unwrap();
        let m: ArcStorageBackend =
            crate::SqliteStorageBackend::open(&path).unwrap();

        for _ in 0..25 {
            let (x, y) = tokio::join!(
                acquire(&a, GateRole::Server),
                acquire(&m, GateRole::Migration)
            );
            let winners: Vec<_> = [x, y].into_iter().flatten().collect();
            assert_eq!(winners.len(), 1, "exactly one role may hold the store");
            winners.into_iter().next().unwrap().release().await.unwrap();
            assert_eq!(status(&m).await.unwrap(), GateState::Unlocked);
        }

        let owner = acquire(&a, GateRole::Server)
            .await
            .unwrap()
            .holder()
            .owner_id
            .clone();
        assert!(acquire(&m, GateRole::Migration).await.is_err());
        assert!(break_gate(&m, "someone-else").await.is_err());
        break_gate(&m, &owner).await.unwrap();
        assert_eq!(status(&a).await.unwrap(), GateState::Unlocked);
        drop((a, m));
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Mutual exclusion on a live PostgreSQL store (real create-only insert and
    /// version CAS). The gate slot is fixed, so each run uses a fresh database.
    /// #[ignore] + env-gated on DELTAFORGE_IT_PG_DSN.
    #[cfg(feature = "postgres")]
    #[tokio::test]
    #[ignore = "requires a live PostgreSQL (DELTAFORGE_IT_PG_DSN)"]
    async fn pg_gate_is_mutually_exclusive() {
        use deadpool_postgres::tokio_postgres::{self, NoTls};
        let dsn = std::env::var("DELTAFORGE_IT_PG_DSN")
            .expect("DELTAFORGE_IT_PG_DSN must be set to run this test");
        let db = format!("gate_it_{}", uuid::Uuid::new_v4().simple());
        let (admin, conn) = tokio_postgres::connect(&dsn, NoTls).await.unwrap();
        tokio::spawn(conn);
        admin
            .batch_execute(&format!("CREATE DATABASE {db}"))
            .await
            .unwrap();
        let b: ArcStorageBackend = crate::PostgresStorageBackend::connect(
            &format!("{dsn} dbname={db}"),
        )
        .await
        .unwrap();

        for _ in 0..25 {
            let (a, m) = tokio::join!(
                acquire(&b, GateRole::Server),
                acquire(&b, GateRole::Migration)
            );
            let winners: Vec<_> = [a, m].into_iter().flatten().collect();
            assert_eq!(winners.len(), 1, "exactly one role may hold the store");
            winners.into_iter().next().unwrap().release().await.unwrap();
            assert_eq!(status(&b).await.unwrap(), GateState::Unlocked);
        }

        let owner = acquire(&b, GateRole::Server)
            .await
            .unwrap()
            .holder()
            .owner_id
            .clone();
        assert!(acquire(&b, GateRole::Migration).await.is_err());
        break_gate(&b, &owner).await.unwrap();
        assert_eq!(status(&b).await.unwrap(), GateState::Unlocked);

        drop(b);
        let _ = admin
            .batch_execute(&format!("DROP DATABASE {db} WITH (FORCE)"))
            .await;
    }

    #[tokio::test]
    async fn unsupported_records_are_refused() {
        let b = backend();
        b.slot_create(
            NS,
            KEY,
            &serde_json::to_vec(
                &serde_json::json!({"format_version": 9, "holder": null}),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        assert!(matches!(
            status(&b).await.unwrap_err(),
            GateError::Corrupt(_)
        ));
        assert!(matches!(
            acquire(&b, GateRole::Server).await.unwrap_err(),
            GateError::Corrupt(_)
        ));
    }
}
