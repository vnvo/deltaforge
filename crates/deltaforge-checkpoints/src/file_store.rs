use super::{CheckpointStore, Result};
use async_trait::async_trait;
use std::{collections::HashMap, path::{Path, PathBuf}};
use tokio::sync::Mutex;

pub struct FileCheckpointStore {
    path: PathBuf,
    guard: Mutex<()>,
}

impl FileCheckpointStore {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        Ok(Self { path: path.as_ref().to_path_buf(), guard: Mutex::new(()) })
    }

    async fn load(&self) -> Result<HashMap<String, Vec<u8>>> {
        if !tokio::fs::try_exists(&self.path).await? {
            return Ok(HashMap::new());
        }
        let bytes = tokio::fs::read(&self.path).await?;
        let map: HashMap<String, Vec<u8>> = serde_json::from_slice(&bytes)?;
        Ok(map)
    }

    async fn save(&self, map: &HashMap<String, Vec<u8>>) -> Result<()> {
        let bytes = serde_json::to_vec_pretty(map)?;
        let tmp = self.path.with_extension("tmp");
        tokio::fs::write(&tmp, &bytes).await?;
        tokio::fs::rename(&tmp, &self.path).await?;
        Ok(())
    }
}

#[async_trait]
impl CheckpointStore for FileCheckpointStore {
    async fn get_raw(&self, source_id: &str) -> Result<Option<Vec<u8>>> {
        let _g = self.guard.lock().await;
        let mut map = self.load().await?;
        Ok(map.remove(source_id))
    }

    async fn put_raw(&self, source_id: &str, bytes: &[u8]) -> Result<()> {
        let _g = self.guard.lock().await;
        let mut map = self.load().await?;
        map.insert(source_id.to_string(), bytes.to_vec());
        self.save(&map).await
    }

    async fn delete(&self, source_id: &str) -> Result<bool> {
        let _g = self.guard.lock().await;
        let mut map = self.load().await?;
        let existed = map.remove(source_id).is_some();
        if existed {
            self.save(&map).await?;
        }
        Ok(existed)
    }

    async fn list(&self) -> Result<Vec<String>> {
        let _g = self.guard.lock().await;
        let map = self.load().await?;
        Ok(map.keys().cloned().collect())
    }
}
