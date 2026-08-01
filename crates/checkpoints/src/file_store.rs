use super::CheckpointResult;
use super::CheckpointStore;
use async_trait::async_trait;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};
use tokio::sync::Mutex;

pub struct FileCheckpointStore {
    path: PathBuf,
    guard: Mutex<()>,
}

impl FileCheckpointStore {
    pub fn new<P: AsRef<Path>>(path: P) -> CheckpointResult<Self> {
        Ok(Self {
            path: path.as_ref().to_path_buf(),
            guard: Mutex::new(()),
        })
    }

    async fn load(&self) -> CheckpointResult<HashMap<String, Vec<u8>>> {
        if !tokio::fs::try_exists(&self.path).await? {
            return Ok(HashMap::new());
        }
        let bytes = tokio::fs::read(&self.path).await?;
        let map: HashMap<String, Vec<u8>> = serde_json::from_slice(&bytes)?;
        Ok(map)
    }

    async fn save(
        &self,
        map: &HashMap<String, Vec<u8>>,
    ) -> CheckpointResult<()> {
        let bytes = serde_json::to_vec_pretty(map)?;
        let tmp = self.path.with_extension("tmp");
        tokio::fs::write(&tmp, &bytes).await?;
        tokio::fs::rename(&tmp, &self.path).await?;
        Ok(())
    }
}

#[async_trait]
impl CheckpointStore for FileCheckpointStore {
    async fn get_raw(
        &self,
        source_id: &str,
    ) -> CheckpointResult<Option<Vec<u8>>> {
        let _g = self.guard.lock().await;
        let mut map = self.load().await?;
        Ok(map.remove(source_id))
    }

    async fn put_raw(
        &self,
        source_id: &str,
        bytes: &[u8],
    ) -> CheckpointResult<()> {
        let _g = self.guard.lock().await;
        let mut map = self.load().await?;
        map.insert(source_id.to_string(), bytes.to_vec());
        self.save(&map).await
    }

    async fn delete(&self, source_id: &str) -> CheckpointResult<bool> {
        let _g = self.guard.lock().await;
        let mut map = self.load().await?;
        let existed = map.remove(source_id).is_some();
        if existed {
            self.save(&map).await?;
        }
        Ok(existed)
    }

    async fn list(&self) -> CheckpointResult<Vec<String>> {
        let _g = self.guard.lock().await;
        let map = self.load().await?;
        Ok(map.keys().cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_path() -> PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut p = std::env::temp_dir();
        p.push(format!("df_ckpt_{}_{}.json", std::process::id(), n));
        p
    }

    #[tokio::test]
    async fn put_then_get_roundtrips() {
        let path = temp_path();
        let store = FileCheckpointStore::new(&path).unwrap();
        store.put_raw("src-1", b"pos-42").await.unwrap();
        // Reading back an EXISTING file must return the data. Pins the
        // `!try_exists` guard: deleting the `!` would treat an existing file
        // as absent and return an empty map → silent checkpoint loss.
        let got = store.get_raw("src-1").await.unwrap();
        assert_eq!(got.as_deref(), Some(&b"pos-42"[..]));
        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn checkpoint_survives_store_reopen() {
        // The actual resume scenario: a fresh store on the same path must
        // load the previously persisted checkpoint.
        let path = temp_path();
        {
            let s = FileCheckpointStore::new(&path).unwrap();
            s.put_raw("s", b"v1").await.unwrap();
        }
        let reopened = FileCheckpointStore::new(&path).unwrap();
        assert_eq!(
            reopened.get_raw("s").await.unwrap().as_deref(),
            Some(&b"v1"[..])
        );
        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn get_missing_source_is_none() {
        let path = temp_path();
        let store = FileCheckpointStore::new(&path).unwrap();
        assert_eq!(store.get_raw("absent").await.unwrap(), None);
        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn list_and_delete() {
        let path = temp_path();
        let store = FileCheckpointStore::new(&path).unwrap();
        store.put_raw("a", b"1").await.unwrap();
        store.put_raw("b", b"2").await.unwrap();
        let mut keys = store.list().await.unwrap();
        keys.sort();
        assert_eq!(keys, vec!["a".to_string(), "b".to_string()]);

        assert!(store.delete("a").await.unwrap(), "delete reports existed");
        assert!(!store.delete("a").await.unwrap(), "second delete is a no-op");
        assert_eq!(store.list().await.unwrap(), vec!["b".to_string()]);
        let _ = tokio::fs::remove_file(&path).await;
    }
}
