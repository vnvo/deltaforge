//! The deployment identity and the durable store of destination ownership
//! records (see [`deltaforge_core::ownership`]), over the state store's
//! versioned slots.

use anyhow::Context;
use async_trait::async_trait;
use deltaforge_core::ownership::{
    OwnershipRecord, OwnershipStore, is_token, random_token,
};
use storage::ArcStorageBackend;

/// The state-store namespace of ownership records.
pub const NAMESPACE: &str = "sink_destination_ownership";
/// Where the deployment identity is kept.
pub const DEPLOYMENT_NAMESPACE: &str = "deployment";
pub const DEPLOYMENT_KEY: &str = "identity";

/// The deployment identity of the state store `backend`: created once
/// (atomically, by whichever process comes first) and read ever after. Two
/// separately created state stores never share one; a copied store keeps
/// it. A malformed identity fails closed.
pub async fn deployment_identity(
    backend: &ArcStorageBackend,
) -> anyhow::Result<String> {
    let fresh = random_token();
    if backend
        .slot_create(DEPLOYMENT_NAMESPACE, DEPLOYMENT_KEY, fresh.as_bytes())
        .await
        .context("create the deployment identity")?
        .is_some()
    {
        return Ok(fresh);
    }
    let (_, bytes) = backend
        .slot_get(DEPLOYMENT_NAMESPACE, DEPLOYMENT_KEY)
        .await
        .context("read the deployment identity")?
        .ok_or_else(|| {
            anyhow::anyhow!("the deployment identity was created and is gone")
        })?;
    let id = String::from_utf8(bytes)
        .ok()
        .filter(|s| is_token(s))
        .ok_or_else(|| {
            anyhow::anyhow!("the deployment identity is malformed")
        })?;
    Ok(id)
}

pub struct StorageOwnershipStore {
    backend: ArcStorageBackend,
}

impl StorageOwnershipStore {
    pub fn new(backend: ArcStorageBackend) -> Self {
        Self { backend }
    }

    /// Remove every ownership record of `pipeline` (its sinks' records):
    /// only with the reset of those sinks' checkpoints, in the same
    /// operation (pipeline deletion).
    pub async fn remove_pipeline(&self, pipeline: &str) -> anyhow::Result<u64> {
        let prefix = sinks::ownership_prefix(pipeline);
        let mut removed = 0;
        loop {
            let page = self
                .backend
                .slot_list(
                    NAMESPACE,
                    Some(&prefix),
                    None,
                    storage::SLOT_LIST_MAX_LIMIT,
                )
                .await?;
            if page.records.is_empty() {
                return Ok(removed);
            }
            for r in page.records {
                if self.backend.slot_delete(NAMESPACE, &r.key).await? {
                    removed += 1;
                }
            }
        }
    }
}

fn decode(bytes: &[u8]) -> anyhow::Result<OwnershipRecord> {
    // A record that does not decode fails closed: never treated as absent.
    serde_json::from_slice(bytes).context("ownership record is malformed")
}

#[async_trait]
impl OwnershipStore for StorageOwnershipStore {
    async fn get(
        &self,
        key: &str,
    ) -> anyhow::Result<Option<(u64, OwnershipRecord)>> {
        match self.backend.slot_get(NAMESPACE, key).await? {
            None => Ok(None),
            Some((v, bytes)) => Ok(Some((v, decode(&bytes)?))),
        }
    }

    async fn create(
        &self,
        key: &str,
        rec: &OwnershipRecord,
    ) -> anyhow::Result<Option<u64>> {
        Ok(self
            .backend
            .slot_create(NAMESPACE, key, &serde_json::to_vec(rec)?)
            .await?)
    }

    async fn cas(
        &self,
        key: &str,
        version: u64,
        rec: &OwnershipRecord,
    ) -> anyhow::Result<Option<u64>> {
        Ok(self
            .backend
            .slot_cas(NAMESPACE, key, version, &serde_json::to_vec(rec)?)
            .await?
            .then_some(version + 1))
    }

    async fn remove(&self, key: &str) -> anyhow::Result<bool> {
        Ok(self.backend.slot_delete(NAMESPACE, key).await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn backend() -> ArcStorageBackend {
        Arc::new(storage::MemoryStorageBackend::new())
    }

    #[tokio::test]
    async fn a_store_has_one_deployment_identity_and_separate_stores_differ() {
        let b = backend();
        let (x, y) =
            tokio::join!(deployment_identity(&b), deployment_identity(&b));
        let (x, y) = (x.unwrap(), y.unwrap());
        assert_eq!(x, y, "concurrent first starts agree");
        assert_eq!(
            deployment_identity(&b).await.unwrap(),
            x,
            "a restart reads it"
        );
        assert_ne!(deployment_identity(&backend()).await.unwrap(), x);
        // Malformed: fails closed.
        let bad = backend();
        bad.slot_create(DEPLOYMENT_NAMESPACE, DEPLOYMENT_KEY, b"not-a-token")
            .await
            .unwrap();
        assert!(deployment_identity(&bad).await.is_err());
    }

    fn rec(fence: &str) -> OwnershipRecord {
        OwnershipRecord {
            destination_instance: random_token(),
            fence: Some(fence.into()),
            pending: None,
            server_identity: None,
        }
    }

    #[tokio::test]
    async fn records_are_create_only_and_compare_and_swap() {
        let store = StorageOwnershipStore::new(backend());
        let v = store.create("p/s/d", &rec("a")).await.unwrap().unwrap();
        assert_eq!(store.create("p/s/d", &rec("b")).await.unwrap(), None);
        let v2 = store.cas("p/s/d", v, &rec("c")).await.unwrap().unwrap();
        assert_eq!(
            store.cas("p/s/d", v, &rec("d")).await.unwrap(),
            None,
            "stale"
        );
        let (got_v, got) = store.get("p/s/d").await.unwrap().unwrap();
        assert_eq!((got_v, got.fence.as_deref()), (v2, Some("c")));
    }

    #[tokio::test]
    async fn a_corrupt_record_fails_closed() {
        let b = backend();
        b.slot_upsert(NAMESPACE, "p/s/d", b"not json")
            .await
            .unwrap();
        assert!(StorageOwnershipStore::new(b).get("p/s/d").await.is_err());
    }

    #[tokio::test]
    async fn pipeline_removal_removes_only_its_records() {
        let store = StorageOwnershipStore::new(backend());
        for k in ["orders/es/a", "orders/ch/b", "orders-2/es/a", "other/es/a"] {
            store.create(k, &rec("f")).await.unwrap();
        }
        assert_eq!(store.remove_pipeline("orders").await.unwrap(), 2);
        assert!(store.get("orders-2/es/a").await.unwrap().is_some());
        assert!(store.get("other/es/a").await.unwrap().is_some());
        assert!(store.get("orders/es/a").await.unwrap().is_none());
    }
}
