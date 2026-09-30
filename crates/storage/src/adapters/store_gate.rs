//! Durable, mutually exclusive store gate shared by the DeltaForge server and
//! the schema migration command.
//!
//! Exactly one record (`store_gate/gate`) holds the gate state and it changes
//! only by create-only insert or version compare-and-swap, so a server start
//! and a migration apply can never both acquire it. The server acquires it
//! before any schema access and releases it only after a clean shutdown; the
//! migration command acquires it before planning and releases it when done.
//!
//! Nothing expires. A process that dies while holding the gate leaves it held;
//! only an explicit [`break_gate`] naming the recorded owner releases it, after
//! an operator has verified that the owner is no longer alive. This favours
//! correctness over automatic stale-lock recovery.

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
}

impl std::fmt::Display for GateHolder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let since =
            chrono::DateTime::from_timestamp_millis(self.acquired_at_ms)
                .map(|t| t.to_rfc3339())
                .unwrap_or_else(|| self.acquired_at_ms.to_string());
        write!(
            f,
            "{} (owner {}, host {}, pid {}, since {since})",
            self.role, self.owner_id, self.hostname, self.pid
        )
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

#[derive(Debug, thiserror::Error)]
pub enum GateError {
    #[error(
        "the store is held by {0}. If that process is no longer running (for \
         example it crashed), verify it and release the gate with \
         `deltaforge store-gate break --owner {owner}`",
        owner = .0.owner_id
    )]
    Held(GateHolder),

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
        break_gate(&self.backend, &self.holder.owner_id)
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

/// Acquire the gate for `role`. Fails with [`GateError::Held`] if anyone (any
/// role) holds it.
pub async fn acquire(
    backend: &ArcStorageBackend,
    role: GateRole,
) -> Result<GateGuard, GateError> {
    let holder = GateHolder {
        role,
        owner_id: uuid::Uuid::new_v4().to_string(),
        hostname: hostname(),
        pid: std::process::id(),
        acquired_at_ms: Utc::now().timestamp_millis(),
    };
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

/// Release the gate if, and only if, `owner_id` still holds it. Used by a
/// clean release and by the operator break after a crash. Returns the holder
/// that was released.
pub async fn break_gate(
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
