use anyhow::Result;
use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};

mod errors;
mod file_store;
mod mem_store;

pub use errors::CheckpointResult;
pub use file_store::FileCheckpointStore;
pub use mem_store::MemCheckpointStore;

#[async_trait]
pub trait CheckpointStore: Send + Sync {
    async fn get_raw(
        &self,
        source_id: &str,
    ) -> CheckpointResult<Option<Vec<u8>>>;
    
    async fn put_raw(
        &self,
        source_id: &str,
        bytes: &[u8],
    ) -> CheckpointResult<()>;

    async fn delete(&self, source_id: &str) -> CheckpointResult<bool>;
    
    async fn list(&self) -> CheckpointResult<Vec<String>>;
}

#[async_trait]
pub trait CheckpointStoreExt: CheckpointStore {
    async fn get<T>(&self, source_id: &str) -> CheckpointResult<Option<T>>
    where
        T: DeserializeOwned + Send,
    {
        match self.get_raw(source_id).await? {
            Some(buf) => Ok(Some(serde_json::from_slice(&buf)?)),
            None => Ok(None),
        }
    }

    async fn put<T>(&self, source_id: &str, checkpoint: T) -> CheckpointResult<()>
    where
        T: Serialize + Send + 'static,
    {
        let buf = serde_json::to_vec(&checkpoint)?;
        drop(checkpoint);
        self.put_raw(source_id, &buf).await
    }
}
impl<T: CheckpointStore + ?Sized> CheckpointStoreExt for T {}
