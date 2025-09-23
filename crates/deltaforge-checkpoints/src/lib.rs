use anyhow::Result;
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};

#[async_trait]
pub trait CheckpointStore: Send + Sync {
    async fn get_raw(&self, source_id: &str) -> Result<Option<Vec<u8>>>;
    async fn put_raw(&self, source_id: &str, bytes: &[u8]) -> Result<()>;
    // delete a checkpoint for a source
    async fn delete(&self, source_id: &str) -> Result<bool>;
    // list all source IDs that have checkpoints
    async fn list(&self) -> Result<Vec<String>>;
}

#[async_trait]
pub trait CheckpointStoreExt: CheckpointStore {
    async fn get<T>(&self, source_id: &str) -> Result<Option<T>>
    where
        T: DeserializeOwned + Send,
    {
        match self.get_raw(source_id).await? {
            Some(buf) => Ok(Some(serde_json::from_slice(&buf)?)),
            None => Ok(None),
        }
    }

    async fn put<T>(&self, source_id: &str, checkpoint: T) -> Result<()>
    where
        T: Serialize + Send + 'static,
    {
        let buf = serde_json::to_vec(&checkpoint)?;
        drop(checkpoint);
        self.put_raw(source_id, &buf).await
    }
}
impl<T: CheckpointStore + ?Sized> CheckpointStoreExt for T {}

pub use file_store::FileCheckpointStore;

mod file_store;
