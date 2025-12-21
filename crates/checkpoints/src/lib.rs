//! Provides checkpoint persistence with optional versioning

use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};

mod errors;
mod file_store;
mod mem_store;

#[cfg(feature = "cp-sqlite")]
mod sqlite_store;

pub use errors::{CheckpointError, CheckpointResult};
pub use file_store::FileCheckpointStore;
pub use mem_store::MemCheckpointStore;

#[cfg(feature = "cp-sqlite")]
pub use sqlite_store::SqliteCheckpointStore;

/// Checkpoint storage trait.
/// Backends may optionally support versioning - check `supports_versioning()`.
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    /// Get raw checkpoint bytes.
    async fn get_raw(
        &self,
        source_id: &str,
    ) -> CheckpointResult<Option<Vec<u8>>>;

    /// Store raw checkpoint bytes.
    async fn put_raw(
        &self,
        source_id: &str,
        bytes: &[u8],
    ) -> CheckpointResult<()>;

    /// Delete checkpoint (all versions if versioned).
    async fn delete(&self, source_id: &str) -> CheckpointResult<bool>;

    /// List all checkpoint keys.
    async fn list(&self) -> CheckpointResult<Vec<String>>;

    // ========== Versioning (optional) ==========

    /// Whether this backend supports versioning.
    fn supports_versioning(&self) -> bool {
        false
    }

    /// Store and return version number (for versioned backends).
    async fn put_raw_versioned(
        &self,
        source_id: &str,
        bytes: &[u8],
    ) -> CheckpointResult<Option<u64>> {
        self.put_raw(source_id, bytes).await?;
        Ok(None)
    }

    /// Get a specific version (if supported).
    async fn get_version_raw(
        &self,
        _source_id: &str,
        _version: u64,
    ) -> CheckpointResult<Option<Vec<u8>>> {
        Ok(None)
    }

    /// List versions for a key (newest first).
    async fn list_versions(
        &self,
        _source_id: &str,
    ) -> CheckpointResult<Vec<VersionInfo>> {
        Ok(vec![])
    }
}

/// Extension trait for typed checkpoint access.
#[async_trait]
pub trait CheckpointStoreExt: CheckpointStore {
    /// Get typed checkpoint.
    async fn get<T>(&self, source_id: &str) -> CheckpointResult<Option<T>>
    where
        T: DeserializeOwned + Send,
    {
        match self.get_raw(source_id).await? {
            Some(buf) => Ok(Some(serde_json::from_slice(&buf)?)),
            None => Ok(None),
        }
    }

    /// Store typed checkpoint.
    async fn put<T>(
        &self,
        source_id: &str,
        checkpoint: T,
    ) -> CheckpointResult<()>
    where
        T: Serialize + Send + 'static,
    {
        let buf = serde_json::to_vec(&checkpoint)?;
        drop(checkpoint);
        self.put_raw(source_id, &buf).await
    }

    /// Store typed checkpoint and return version (for versioned backends).
    async fn put_versioned<T>(
        &self,
        source_id: &str,
        checkpoint: T,
    ) -> CheckpointResult<Option<u64>>
    where
        T: Serialize + Send + 'static,
    {
        let buf = serde_json::to_vec(&checkpoint)?;
        drop(checkpoint);
        self.put_raw_versioned(source_id, &buf).await
    }

    /// Get specific version as typed checkpoint.
    async fn get_version<T>(
        &self,
        source_id: &str,
        version: u64,
    ) -> CheckpointResult<Option<T>>
    where
        T: DeserializeOwned + Send,
    {
        match self.get_version_raw(source_id, version).await? {
            Some(buf) => Ok(Some(serde_json::from_slice(&buf)?)),
            None => Ok(None),
        }
    }

    /// Rollback to a specific version (copies old version as new latest).
    async fn rollback(
        &self,
        source_id: &str,
        version: u64,
    ) -> CheckpointResult<Option<u64>> {
        if !self.supports_versioning() {
            return Err(CheckpointError::NotSupported(
                "rollback requires versioning".into(),
            ));
        }
        match self.get_version_raw(source_id, version).await? {
            Some(buf) => self.put_raw_versioned(source_id, &buf).await,
            None => Ok(None),
        }
    }
}

impl<T: CheckpointStore + ?Sized> CheckpointStoreExt for T {}

/// Version metadata.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VersionInfo {
    pub version: u64,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub size_bytes: usize,
}
