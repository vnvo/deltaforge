use anyhow::Result;
use async_trait::async_trait;
use deltaforge_core::SchemaRegistry;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Clone, Default)]
pub struct InMemoryRegistry {
    inner: Arc<RwLock<HashMap<(String, String, String), Vec<(i32, String, Value)>>>>,
}

#[async_trait]
impl SchemaRegistry for InMemoryRegistry {
    async fn register(
        &self,
        tenant: &str,
        db: &str,
        table: &str,
        hash: &str,
        schema_json: &serde_json::Value,
    ) -> Result<i32> {
        let key = (tenant.to_string(), db.to_string(), table.to_string());
        let mut guard = self.inner.write().unwrap();
        let entry = guard.entry(key).or_default();

        if let Some((v, _h, _)) = entry.iter().find(|(_v, h, _s)| h == hash) {
            return Ok(*v);
        }

        let new_v = (entry.len() as i32) + 1;
        entry.push((new_v, hash.to_string(), schema_json.clone()));
        Ok(new_v)
    }

    async fn latest(&self, tenant: &str, db: &str, table: &str) -> Result<Option<(i32, String)>> {
        let key = (tenant.to_string(), db.to_string(), table.to_string());
        let guard = self.inner.read().unwrap();
        Ok(guard
            .get(&key)
            .and_then(|v| v.last().map(|(ver, hash, _)| (*ver, hash.clone()))))
    }
}
