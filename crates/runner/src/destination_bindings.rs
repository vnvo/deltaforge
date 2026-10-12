//! The durable store of sink destination bindings: the state store's
//! create-only slots, so a binding outlives restarts and is made once,
//! atomically, by whichever writer of the deployment comes first.

use async_trait::async_trait;
use deltaforge_core::destination::{
    DestinationBinding, DestinationBindingStore,
};
use storage::ArcStorageBackend;

/// The state-store namespace of destination bindings.
pub const NAMESPACE: &str = "sink_destination_bindings";

pub struct StorageDestinationBindings {
    backend: ArcStorageBackend,
}

impl StorageDestinationBindings {
    pub fn new(backend: ArcStorageBackend) -> Self {
        Self { backend }
    }
}

#[async_trait]
impl DestinationBindingStore for StorageDestinationBindings {
    async fn get(
        &self,
        destination: &str,
    ) -> anyhow::Result<Option<DestinationBinding>> {
        match self.backend.slot_get(NAMESPACE, destination).await? {
            None => Ok(None),
            // A binding that does not decode fails closed: never treated as
            // absent.
            Some((_, bytes)) => Ok(Some(serde_json::from_slice(&bytes)?)),
        }
    }

    async fn bind(
        &self,
        destination: &str,
        binding: &DestinationBinding,
    ) -> anyhow::Result<DestinationBinding> {
        let bytes = serde_json::to_vec(binding)?;
        if self
            .backend
            .slot_create(NAMESPACE, destination, &bytes)
            .await?
            .is_some()
        {
            return Ok(binding.clone());
        }
        // Another writer bound it first: its binding is in force.
        self.get(destination).await?.ok_or_else(|| {
            anyhow::anyhow!(
                "destination binding of {destination} was created and is gone"
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn binding(relation: &str) -> DestinationBinding {
        DestinationBinding {
            relation: relation.into(),
            target_uid: "u".into(),
        }
    }

    /// Concurrent first binders over one state store agree on one binding;
    /// a later store over the same backend (a restart) reads it.
    #[tokio::test]
    async fn the_first_binding_wins_and_persists() {
        let backend: ArcStorageBackend =
            Arc::new(storage::MemoryStorageBackend::new());
        let mut tasks = Vec::new();
        for i in 0..16 {
            let store = StorageDestinationBindings::new(Arc::clone(&backend));
            tasks.push(tokio::spawn(async move {
                store.bind("idx", &binding(&format!("r{i}"))).await.unwrap()
            }));
        }
        let mut won = std::collections::BTreeSet::new();
        for t in tasks {
            won.insert(t.await.unwrap().relation);
        }
        assert_eq!(won.len(), 1, "{won:?}");
        let restarted = StorageDestinationBindings::new(Arc::clone(&backend));
        let bound = restarted.get("idx").await.unwrap().unwrap();
        assert!(won.contains(&bound.relation));
    }

    /// A binding that does not decode is an error, never "unbound".
    #[tokio::test]
    async fn a_corrupt_binding_fails_closed() {
        let backend: ArcStorageBackend =
            Arc::new(storage::MemoryStorageBackend::new());
        backend
            .slot_upsert(NAMESPACE, "idx", b"not json")
            .await
            .unwrap();
        let store = StorageDestinationBindings::new(backend);
        assert!(store.get("idx").await.is_err());
        assert!(store.bind("idx", &binding("r")).await.is_err());
    }
}
