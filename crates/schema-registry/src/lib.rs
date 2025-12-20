use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use deltaforge_core::SchemaRegistry;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

mod source_schema;
pub use source_schema::{SourceSchema, compute_fingerprint};

/// Schema version with metadata.
#[derive(Debug, Clone)]
pub struct SchemaVersion {
    pub version: i32,
    pub hash: String,
    pub schema_json: Value,
    pub registered_at: DateTime<Utc>,
}

type MemRegistry =
    Arc<RwLock<HashMap<(String, String, String), Vec<SchemaVersion>>>>;

#[derive(Clone, Default, Debug)]
pub struct InMemoryRegistry {
    inner: MemRegistry,
}

impl InMemoryRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get schema version that was active at a given timestamp.
    ///
    /// Returns the most recent schema registered before or at the timestamp.
    /// This is critical for replay: events must be interpreted with the
    /// schema that was active when they were produced.
    pub fn get_at_timestamp(
        &self,
        tenant: &str,
        db: &str,
        table: &str,
        timestamp: DateTime<Utc>,
    ) -> Option<SchemaVersion> {
        let key = (tenant.to_string(), db.to_string(), table.to_string());
        let guard = self.inner.read().unwrap();

        guard.get(&key).and_then(|versions| {
            versions
                .iter()
                .filter(|v| v.registered_at <= timestamp)
                .max_by_key(|v| v.registered_at)
                .cloned()
        })
    }

    /// Get all versions for a table.
    pub fn list_versions(
        &self,
        tenant: &str,
        db: &str,
        table: &str,
    ) -> Vec<SchemaVersion> {
        let key = (tenant.to_string(), db.to_string(), table.to_string());
        let guard = self.inner.read().unwrap();
        guard.get(&key).cloned().unwrap_or_default()
    }

    /// Get schema by version number.
    pub fn get_version(
        &self,
        tenant: &str,
        db: &str,
        table: &str,
        version: i32,
    ) -> Option<SchemaVersion> {
        let key = (tenant.to_string(), db.to_string(), table.to_string());
        let guard = self.inner.read().unwrap();
        guard.get(&key).and_then(|versions| {
            versions.iter().find(|v| v.version == version).cloned()
        })
    }

    /// Get latest schema with full metadata.
    pub fn get_latest(
        &self,
        tenant: &str,
        db: &str,
        table: &str,
    ) -> Option<SchemaVersion> {
        let key = (tenant.to_string(), db.to_string(), table.to_string());
        let guard = self.inner.read().unwrap();
        guard.get(&key).and_then(|v| v.last().cloned())
    }
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

        // Check if hash already exists
        if let Some(existing) = entry.iter().find(|v| v.hash == hash) {
            return Ok(existing.version);
        }

        let new_version = (entry.len() as i32) + 1;
        entry.push(SchemaVersion {
            version: new_version,
            hash: hash.to_string(),
            schema_json: schema_json.clone(),
            registered_at: Utc::now(),
        });

        Ok(new_version)
    }

    async fn latest(
        &self,
        tenant: &str,
        db: &str,
        table: &str,
    ) -> Result<Option<(i32, String)>> {
        let key = (tenant.to_string(), db.to_string(), table.to_string());
        let guard = self.inner.read().unwrap();
        Ok(guard
            .get(&key)
            .and_then(|v| v.last().map(|sv| (sv.version, sv.hash.clone()))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_basic_register() {
        let reg = InMemoryRegistry::new();
        let v1 = reg
            .register(
                "t",
                "db",
                "tbl",
                "hash1",
                &serde_json::json!({"cols": ["a"]}),
            )
            .await
            .unwrap();
        assert_eq!(v1, 1);

        // Same hash returns same version
        let v2 = reg
            .register(
                "t",
                "db",
                "tbl",
                "hash1",
                &serde_json::json!({"cols": ["a"]}),
            )
            .await
            .unwrap();
        assert_eq!(v2, 1);

        // Different hash gets new version
        let v3 = reg
            .register(
                "t",
                "db",
                "tbl",
                "hash2",
                &serde_json::json!({"cols": ["a", "b"]}),
            )
            .await
            .unwrap();
        assert_eq!(v3, 2);
    }

    #[tokio::test]
    async fn test_get_at_timestamp() {
        let reg = InMemoryRegistry::new();

        // Register first schema
        reg.register("t", "db", "tbl", "h1", &serde_json::json!({"v": 1}))
            .await
            .unwrap();
        let t1 = Utc::now();

        tokio::time::sleep(Duration::from_millis(10)).await;

        // Register second schema
        reg.register("t", "db", "tbl", "h2", &serde_json::json!({"v": 2}))
            .await
            .unwrap();
        let t2 = Utc::now();

        // Query at t1 should return v1
        let sv = reg.get_at_timestamp("t", "db", "tbl", t1);
        assert_eq!(sv.unwrap().version, 1);

        // Query at t2 should return v2
        let sv = reg.get_at_timestamp("t", "db", "tbl", t2);
        assert_eq!(sv.unwrap().version, 2);
    }

    #[tokio::test]
    async fn test_list_versions() {
        let reg = InMemoryRegistry::new();

        reg.register("t", "db", "tbl", "h1", &serde_json::json!({}))
            .await
            .unwrap();
        reg.register("t", "db", "tbl", "h2", &serde_json::json!({}))
            .await
            .unwrap();
        reg.register("t", "db", "tbl", "h3", &serde_json::json!({}))
            .await
            .unwrap();

        let versions = reg.list_versions("t", "db", "tbl");
        assert_eq!(versions.len(), 3);
    }
}
