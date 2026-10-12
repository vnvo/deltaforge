//! Provides checkpoint persistence with optional versioning

use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};

mod errors;
mod file_store;
mod mem_store;
mod snapshot_state;

#[cfg(feature = "cp-sqlite")]
mod sqlite_store;

pub use errors::{CheckpointError, CheckpointResult};
pub use file_store::FileCheckpointStore;
pub use mem_store::MemCheckpointStore;
pub use snapshot_state::{CasOutcome, SnapshotStateStore};

#[cfg(any(test, feature = "test-support"))]
pub use snapshot_state::assert_snapshot_state_contract;

#[cfg(feature = "cp-sqlite")]
pub use sqlite_store::SqliteCheckpointStore;

/// A monotonic counter of durable checkpoint changes. The delivery layer bumps
/// it after each successful checkpoint persistence; readers subscribe and mark
/// the current generation seen *before* reading the store, so a change that
/// lands during the read is still observed afterwards. Changes coalesce, and
/// the newest generation is always the one a reader sees next.
#[derive(Clone, Debug)]
pub struct CheckpointGeneration(
    std::sync::Arc<tokio::sync::watch::Sender<u64>>,
);

impl CheckpointGeneration {
    pub fn new() -> Self {
        Self(std::sync::Arc::new(tokio::sync::watch::Sender::new(0)))
    }

    /// Record that durable checkpoint state changed.
    pub fn bump(&self) {
        self.0.send_modify(|g| *g = g.wrapping_add(1));
    }

    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        self.0.subscribe()
    }
}

impl Default for CheckpointGeneration {
    fn default() -> Self {
        Self::new()
    }
}

/// How long a reader waits for a published change before re-reading anyway: a
/// long bound when the store publishes changes (a lost or coalesced change is
/// still read eventually), a short one when it publishes none.
pub const CHANGE_FALLBACK_PUBLISHED: std::time::Duration =
    std::time::Duration::from_secs(30);
pub const CHANGE_FALLBACK_UNPUBLISHED: std::time::Duration =
    std::time::Duration::from_millis(500);

/// A reader's subscription to a store's durable checkpoint changes. Call
/// [`Self::mark_seen`] immediately before reading the store; a change that
/// lands after it is then reported by [`Self::has_changed`] and
/// [`Self::changed`], never lost between the read and the wait.
pub struct CheckpointChanges {
    rx: Option<tokio::sync::watch::Receiver<u64>>,
    fallback: std::time::Duration,
}

impl CheckpointChanges {
    pub fn new(store: &dyn CheckpointStore) -> Self {
        let rx = store.checkpoint_changes();
        let fallback = if rx.is_some() {
            CHANGE_FALLBACK_PUBLISHED
        } else {
            CHANGE_FALLBACK_UNPUBLISHED
        };
        Self { rx, fallback }
    }

    /// The longest a reader goes without re-reading the store.
    pub fn fallback(&self) -> std::time::Duration {
        self.fallback
    }

    /// Mark the newest generation seen. Call before reading the store.
    pub fn mark_seen(&mut self) {
        if let Some(rx) = &mut self.rx {
            rx.mark_unchanged();
        }
    }

    /// Whether a generation newer than the last seen one was published.
    pub fn has_changed(&self) -> bool {
        self.rx
            .as_ref()
            .is_some_and(|rx| rx.has_changed().unwrap_or(false))
    }

    /// Resolves once a newer generation is published, or after the fallback.
    /// Cancellation-safe.
    pub async fn changed(&mut self) {
        let fallback = tokio::time::sleep(self.fallback);
        tokio::pin!(fallback);
        if let Some(rx) = &mut self.rx {
            tokio::select! {
                r = rx.changed() => match r {
                    Ok(()) => return,
                    // The publisher is gone: no further changes will be
                    // published, so only the fallback re-reads.
                    Err(_) => self.rx = None,
                },
                _ = &mut fallback => return,
            }
        }
        fallback.await;
    }
}

/// Checkpoint storage trait.
/// Backends may optionally support versioning - check `supports_versioning()`.
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    /// Whether this store derives a source's resume position from **per-sink**
    /// checkpoints written by the delivery layer (the coordinator), rather than from a
    /// single source-written checkpoint.
    ///
    /// When `true`, the source must NOT persist its own read position as the aggregate
    /// checkpoint: doing so could resume ahead of what every required sink has durably
    /// acknowledged (losing un-acked events on restart). The per-sink minimum, written
    /// only after sink acknowledgement, is authoritative. Defaults to `false` for plain
    /// single-writer stores (the source owns its checkpoint).
    fn manages_per_sink_checkpoints(&self) -> bool {
        false
    }

    /// Changes to the durable checkpoint set, when this store publishes them
    /// (the per-sink proxy, fed by the coordinator after each persisted
    /// commit). `None`: the store publishes no changes and a reader must
    /// re-read on its own schedule.
    fn checkpoint_changes(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        None
    }

    /// Before a source delivers anything: give every configured sink that
    /// has no checkpoint yet `checkpoint`, the position the source starts
    /// from, durably. A sink without one is outside the resume minimum, so
    /// batches it failed before its first acknowledgement would never be
    /// replayed to it. Existing checkpoints are never changed. An error
    /// must stop the source before it delivers (fail closed). A store that
    /// does not manage per-sink checkpoints has nothing to initialize.
    async fn initialize_sink_checkpoints(
        &self,
        _source_id: &str,
        _checkpoint: &[u8],
    ) -> CheckpointResult<()> {
        Ok(())
    }

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

    /// Store multiple checkpoint entries atomically (single transaction).
    /// Used for per-sink checkpoint commits — all sink checkpoints from one
    /// batch are written together to avoid partial state.
    ///
    /// **Note:** The default implementation is NOT atomic — it writes entries
    /// sequentially with individual `put_raw` calls. Backends that support
    /// transactions (SQLite, PostgreSQL) should override this for true atomicity.
    // TODO: FileCheckpointStore should override this with rename-based atomicity.
    async fn put_raw_multi(
        &self,
        entries: &[(&str, &[u8])],
    ) -> CheckpointResult<()> {
        for &(key, bytes) in entries {
            self.put_raw(key, bytes).await?;
        }
        Ok(())
    }

    /// Delete checkpoint (all versions if versioned).
    async fn delete(&self, source_id: &str) -> CheckpointResult<bool>;

    /// List all checkpoint keys.
    async fn list(&self) -> CheckpointResult<Vec<String>>;

    /// List checkpoint keys matching a prefix.
    /// Default implementation filters `list()`. Backends may override for
    /// efficiency (e.g. SQL `WHERE key LIKE 'prefix%'`).
    async fn list_with_prefix(
        &self,
        prefix: &str,
    ) -> CheckpointResult<Vec<String>> {
        let all = self.list().await?;
        Ok(all.into_iter().filter(|k| k.starts_with(prefix)).collect())
    }

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

#[cfg(test)]
mod change_tests {
    use super::*;
    use std::time::Duration;

    /// A store publishing a generation, and the store state it stands for.
    struct Published(CheckpointGeneration);

    #[async_trait]
    impl CheckpointStore for Published {
        fn checkpoint_changes(
            &self,
        ) -> Option<tokio::sync::watch::Receiver<u64>> {
            Some(self.0.subscribe())
        }
        async fn get_raw(&self, _: &str) -> CheckpointResult<Option<Vec<u8>>> {
            Ok(None)
        }
        async fn put_raw(&self, _: &str, _: &[u8]) -> CheckpointResult<()> {
            Ok(())
        }
        async fn delete(&self, _: &str) -> CheckpointResult<bool> {
            Ok(false)
        }
        async fn list(&self) -> CheckpointResult<Vec<String>> {
            Ok(vec![])
        }
    }

    #[tokio::test]
    async fn a_change_between_the_read_and_the_wait_is_not_lost() {
        let generation = CheckpointGeneration::new();
        let mut changes =
            CheckpointChanges::new(&Published(generation.clone()));
        assert_eq!(changes.fallback(), CHANGE_FALLBACK_PUBLISHED);
        changes.mark_seen();
        // ... the reader reads the store; a commit is published meanwhile ...
        generation.bump();
        generation.bump();
        assert!(changes.has_changed());
        tokio::time::timeout(Duration::from_secs(1), changes.changed())
            .await
            .expect("the coalesced change is still pending");
        assert!(!changes.has_changed());
    }

    #[tokio::test(start_paused = true)]
    async fn without_a_publisher_the_reader_falls_back_to_the_interval() {
        let mut gone = {
            let generation = CheckpointGeneration::new();
            CheckpointChanges::new(&Published(generation))
        };
        let t0 = tokio::time::Instant::now();
        gone.changed().await;
        assert!(t0.elapsed() >= CHANGE_FALLBACK_PUBLISHED);

        let mut unpublished =
            CheckpointChanges::new(&MemCheckpointStore::new().unwrap());
        assert_eq!(unpublished.fallback(), CHANGE_FALLBACK_UNPUBLISHED);
        assert!(!unpublished.has_changed());
        let t0 = tokio::time::Instant::now();
        unpublished.changed().await;
        assert!(t0.elapsed() >= CHANGE_FALLBACK_UNPUBLISHED);
    }
}
