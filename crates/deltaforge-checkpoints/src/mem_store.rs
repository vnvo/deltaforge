use super::{CheckpointStore, Result};
use async_trait::async_trait;
use std::collections::HashMap;
use tokio::sync::RwLock;

#[derive(Default)]
pub struct MemCheckpointStore {
    map: RwLock<HashMap<String, Vec<u8>>>,
}

impl MemCheckpointStore {
    pub fn new() -> Result<Self> {
        Ok(Self::default())
    }
}

#[async_trait]
impl CheckpointStore for MemCheckpointStore {
    async fn get_raw(&self, source_id: &str) -> Result<Option<Vec<u8>>> {
        let map = self.map.read().await;
        Ok(map.get(source_id).cloned())
    }

    async fn put_raw(&self, source_id: &str, bytes: &[u8]) -> Result<()> {
        let mut map = self.map.write().await;
        map.insert(source_id.to_string(), bytes.to_vec());
        Ok(())
    }

    async fn delete(&self, source_id: &str) -> Result<bool> {
        let mut map = self.map.write().await;
        Ok(map.remove(source_id).is_some())
    }

    async fn list(&self) -> Result<Vec<String>> {
        Ok(self.map.read().await.keys().cloned().collect())
    }
}
