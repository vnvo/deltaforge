use super::CheckpointResult;
use super::CheckpointStore;
use crate::snapshot_state::{
    CasOutcome, SnapshotStateStore, VersionedRecord, cas_decision,
};
use async_trait::async_trait;
use std::collections::HashMap;
use tokio::sync::RwLock;

#[derive(Default)]
pub struct MemCheckpointStore {
    map: RwLock<HashMap<String, Vec<u8>>>,
}

impl MemCheckpointStore {
    pub fn new() -> CheckpointResult<Self> {
        Ok(Self::default())
    }
}

#[async_trait]
impl CheckpointStore for MemCheckpointStore {
    async fn get_raw(
        &self,
        source_id: &str,
    ) -> CheckpointResult<Option<Vec<u8>>> {
        let map = self.map.read().await;
        Ok(map.get(source_id).cloned())
    }

    async fn put_raw(
        &self,
        source_id: &str,
        bytes: &[u8],
    ) -> CheckpointResult<()> {
        let mut map = self.map.write().await;
        map.insert(source_id.to_string(), bytes.to_vec());
        Ok(())
    }

    async fn delete(&self, source_id: &str) -> CheckpointResult<bool> {
        let mut map = self.map.write().await;
        Ok(map.remove(source_id).is_some())
    }

    async fn list(&self) -> CheckpointResult<Vec<String>> {
        Ok(self.map.read().await.keys().cloned().collect())
    }
}

#[async_trait]
impl SnapshotStateStore for MemCheckpointStore {
    async fn get_versioned(
        &self,
        key: &str,
    ) -> CheckpointResult<Option<(u64, Vec<u8>)>> {
        let map = self.map.read().await;
        match map.get(key) {
            Some(bytes) => Ok(Some(VersionedRecord::decode(bytes)?)),
            None => Ok(None),
        }
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        expected_version: Option<u64>,
        value: &[u8],
    ) -> CheckpointResult<CasOutcome> {
        // Hold the write lock across the read-modify-write so allocation is
        // atomic (no interleaving reader/writer can observe a torn version).
        let mut map = self.map.write().await;
        let current = match map.get(key) {
            Some(bytes) => Some(VersionedRecord::decode(bytes)?),
            None => None,
        };
        match cas_decision(&current, expected_version) {
            Some(new_version) => {
                map.insert(
                    key.to_string(),
                    VersionedRecord::encode(new_version, value)?,
                );
                Ok(CasOutcome::Committed {
                    version: new_version,
                })
            }
            None => Ok(CasOutcome::Mismatch { current }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn snapshot_state_contract() {
        let store = MemCheckpointStore::new().unwrap();
        crate::snapshot_state::assert_snapshot_state_contract(&store).await;
    }

    #[tokio::test]
    async fn concurrent_create_allocates_exactly_one() {
        // Many tasks race create-if-absent; exactly one wins v1, the rest see
        // the winner's record. Proves atomic allocation (no double-mint).
        let store = Arc::new(MemCheckpointStore::new().unwrap());
        let key = "snapshot_generation:race";
        let mut handles = Vec::new();
        for i in 0..16u32 {
            let s = store.clone();
            handles.push(tokio::spawn(async move {
                s.compare_and_swap(key, None, format!("g-{i}").as_bytes())
                    .await
                    .unwrap()
            }));
        }
        let mut committed = 0;
        let mut mismatched = 0;
        for h in handles {
            match h.await.unwrap() {
                CasOutcome::Committed { version } => {
                    assert_eq!(version, 1);
                    committed += 1;
                }
                CasOutcome::Mismatch { current } => {
                    assert_eq!(current.map(|(v, _)| v), Some(1));
                    mismatched += 1;
                }
            }
        }
        assert_eq!(committed, 1, "exactly one create must win");
        assert_eq!(mismatched, 15);
    }
}
