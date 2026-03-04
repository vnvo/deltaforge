//! Thin adapter: [`CheckpointStore`] → [`StorageBackend`] KV primitive.
//!
//! Existing pipeline code that calls `checkpoint_store.get_raw()` /
//! `put_raw()` continues to work unchanged. Data is stored under the
//! `"checkpoints"` namespace.

use async_trait::async_trait;
use checkpoints::{CheckpointError, CheckpointResult, CheckpointStore};

use crate::ArcStorageBackend;

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
            .kv_get("checkpoints", source_id)
            .await
            .map_err(map_err)
    }

    async fn put_raw(
        &self,
        source_id: &str,
        bytes: &[u8],
    ) -> CheckpointResult<()> {
        self.backend
            .kv_put("checkpoints", source_id, bytes)
            .await
            .map_err(map_err)
    }

    async fn delete(&self, source_id: &str) -> CheckpointResult<bool> {
        self.backend
            .kv_delete("checkpoints", source_id)
            .await
            .map_err(map_err)
    }

    async fn list(&self) -> CheckpointResult<Vec<String>> {
        self.backend
            .kv_list("checkpoints", None)
            .await
            .map_err(map_err)
    }

    fn supports_versioning(&self) -> bool {
        // Versioning is handled at the schema log level, not the checkpoint level.
        false
    }
}
