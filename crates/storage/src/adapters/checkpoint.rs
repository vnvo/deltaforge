//! Thin adapter: [`CheckpointStore`] → [`StorageBackend`] KV primitive.
//!
//! Existing pipeline code that calls `checkpoint_store.get_raw()` /
//! `put_raw()` continues to work unchanged. Data is stored under the
//! `"checkpoints"` namespace.

use async_trait::async_trait;
use checkpoints::{
    CasOutcome, CheckpointError, CheckpointResult, CheckpointStore,
    SnapshotStateStore,
};

use crate::{ArcStorageBackend, CHECKPOINTS_NS};

/// Namespace for snapshot-generation allocation slots, kept separate from the
/// `"checkpoints"` KV namespace.
const SNAPSHOT_STATE_NS: &str = "snapshot_state";

/// Implements [`CheckpointStore`] on top of any [`StorageBackend`].
pub struct BackendCheckpointStore {
    backend: ArcStorageBackend,
}

impl BackendCheckpointStore {
    pub fn new(backend: ArcStorageBackend) -> Self {
        Self { backend }
    }
}

fn map_err(e: anyhow::Error) -> CheckpointError {
    CheckpointError::Database(e.to_string())
}

#[async_trait]
impl CheckpointStore for BackendCheckpointStore {
    async fn get_raw(
        &self,
        source_id: &str,
    ) -> CheckpointResult<Option<Vec<u8>>> {
        self.backend
            .kv_get(CHECKPOINTS_NS, source_id)
            .await
            .map_err(map_err)
    }

    async fn put_raw(
        &self,
        source_id: &str,
        bytes: &[u8],
    ) -> CheckpointResult<()> {
        self.backend
            .kv_put(CHECKPOINTS_NS, source_id, bytes)
            .await
            .map_err(map_err)
    }

    async fn delete(&self, source_id: &str) -> CheckpointResult<bool> {
        self.backend
            .kv_delete(CHECKPOINTS_NS, source_id)
            .await
            .map_err(map_err)
    }

    async fn list(&self) -> CheckpointResult<Vec<String>> {
        self.backend
            .kv_list(CHECKPOINTS_NS, None)
            .await
            .map_err(map_err)
    }

    /// Only the keys under `prefix`, selected by the backend: the default
    /// implementation would list every checkpoint key in the store (every
    /// source's) and filter in memory, on the resume/feedback hot path.
    async fn list_with_prefix(
        &self,
        prefix: &str,
    ) -> CheckpointResult<Vec<String>> {
        self.backend
            .kv_list(CHECKPOINTS_NS, Some(prefix))
            .await
            .map_err(map_err)
    }

    fn supports_versioning(&self) -> bool {
        // Versioning is handled at the schema log level, not the checkpoint level.
        false
    }
}

/// Atomic snapshot-generation allocation, delegated to the backend's native
/// versioned slot primitives (`slot_create` / `slot_cas` / `slot_get`). This
/// gives real multi-writer atomicity on transactional backends (Postgres,
/// SQLite).
#[async_trait]
impl SnapshotStateStore for BackendCheckpointStore {
    async fn get_versioned(
        &self,
        key: &str,
    ) -> CheckpointResult<Option<(u64, Vec<u8>)>> {
        self.backend
            .slot_get(SNAPSHOT_STATE_NS, key)
            .await
            .map_err(map_err)
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        expected_version: Option<u64>,
        value: &[u8],
    ) -> CheckpointResult<CasOutcome> {
        match expected_version {
            // Expect-absent: atomic create-only.
            None => {
                match self
                    .backend
                    .slot_create(SNAPSHOT_STATE_NS, key, value)
                    .await
                    .map_err(map_err)?
                {
                    Some(version) => Ok(CasOutcome::Committed { version }),
                    None => Ok(CasOutcome::Mismatch {
                        current: self
                            .backend
                            .slot_get(SNAPSHOT_STATE_NS, key)
                            .await
                            .map_err(map_err)?,
                    }),
                }
            }
            // Expect a specific version: native CAS (bumps version by one).
            Some(v) => {
                if self
                    .backend
                    .slot_cas(SNAPSHOT_STATE_NS, key, v, value)
                    .await
                    .map_err(map_err)?
                {
                    Ok(CasOutcome::Committed { version: v + 1 })
                } else {
                    Ok(CasOutcome::Mismatch {
                        current: self
                            .backend
                            .slot_get(SNAPSHOT_STATE_NS, key)
                            .await
                            .map_err(map_err)?,
                    })
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::MemoryStorageBackend;
    use std::sync::Arc;

    /// The prefix reaches the backend (the checkpoint namespace is never
    /// listed whole), and colliding source ids stay apart.
    #[tokio::test]
    async fn list_with_prefix_is_scoped_by_the_backend() {
        use crate::adapters::test_util::FaultBackend;
        use checkpoints::CheckpointStore;

        let fault = Arc::new(FaultBackend::new());
        let store = BackendCheckpointStore::new(fault.clone());
        for key in [
            "orders::sink::kafka",
            "orders::sink::s3",
            "orders-archive::sink::kafka",
            "orders",
            "ord_rs::sink::kafka",
            "ordXrs::sink::kafka",
        ] {
            store.put_raw(key, b"cp").await.unwrap();
        }

        let mut got = store.list_with_prefix("orders::sink::").await.unwrap();
        got.sort();
        assert_eq!(got, ["orders::sink::kafka", "orders::sink::s3"]);
        assert_eq!(
            store
                .list_with_prefix("orders-archive::sink::")
                .await
                .unwrap(),
            ["orders-archive::sink::kafka"]
        );
        assert_eq!(
            store.list_with_prefix("ord_rs::sink::").await.unwrap(),
            ["ord_rs::sink::kafka"]
        );

        let calls = fault.kv_list_calls.lock().unwrap().clone();
        assert_eq!(calls.len(), 3);
        assert!(
            calls.iter().all(|(ns, prefix)| ns == "checkpoints"
                && prefix.as_deref().is_some_and(|p| p.ends_with("::sink::"))),
            "the checkpoint namespace was listed without the prefix: {calls:?}"
        );
    }

    #[tokio::test]
    async fn backend_snapshot_state_contract() {
        let store =
            BackendCheckpointStore::new(Arc::new(MemoryStorageBackend::new()));
        checkpoints::assert_snapshot_state_contract(&store).await;
    }
}
