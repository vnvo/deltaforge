use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use deltaforge_core::SchemaRegistry;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

mod source_schema;
pub use source_schema::{SourceSchema, compute_fingerprint};

/// Schema version with metadata.
#[derive(Debug, Clone)]
pub struct SchemaVersion {
    /// Schema version number (per-table, starts at 1)
    pub version: i32,

    /// Content hash for deduplication
    pub hash: String,

    /// Full schema as JSON
    pub schema_json: Value,

    /// When this version was registered
    pub registered_at: DateTime<Utc>,

    /// Global monotonic sequence number (for replay ordering)
    pub sequence: u64,

    /// Source checkpoint when schema was registered (for correlation)
    /// Contains serialized source-specific checkpoint (e.g., MySqlCheckpoint)
    pub checkpoint: Option<Vec<u8>>,
}

type MemRegistry =
    Arc<RwLock<HashMap<(String, String, String), Vec<SchemaVersion>>>>;

#[derive(Debug)]
pub struct InMemoryRegistry {
    inner: MemRegistry,
    sequence: AtomicU64,
}

impl Default for InMemoryRegistry {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            sequence: AtomicU64::new(0),
        }
    }
}

impl InMemoryRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get schema that was active at or before a given sequence number.
    ///
    /// For replay: events carry a sequence number, find the schema
    /// that was registered at or before that sequence.
    pub fn get_at_sequence(
        &self,
        tenant: &str,
        db: &str,
        table: &str,
        sequence: u64,
    ) -> Option<SchemaVersion> {
        let key = (tenant.to_string(), db.to_string(), table.to_string());
        let guard = self.inner.read().unwrap();

        guard.get(&key).and_then(|versions| {
            versions
                .iter()
                .filter(|v| v.sequence <= sequence)
                .max_by_key(|v| v.sequence)
                .cloned()
        })
    }

    /// Get current sequence number (for attaching to events)
    pub fn current_sequence(&self) -> u64 {
        self.sequence.load(Ordering::SeqCst)
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

    /// Register schema with optional checkpoint correlation.
    pub async fn register_with_checkpoint(
        &self,
        tenant: &str,
        db: &str,
        table: &str,
        hash: &str,
        schema_json: &serde_json::Value,
        checkpoint: Option<&[u8]>,
    ) -> Result<i32> {
        let key = (tenant.to_string(), db.to_string(), table.to_string());
        let mut guard = self.inner.write().unwrap();
        let entry = guard.entry(key).or_default();

        // Check if hash already exists (idempotent)
        if let Some(existing) = entry.iter().find(|v| v.hash == hash) {
            return Ok(existing.version);
        }

        // Allocate next global sequence
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;

        let new_version = (entry.len() as i32) + 1;
        entry.push(SchemaVersion {
            version: new_version,
            hash: hash.to_string(),
            schema_json: schema_json.clone(),
            registered_at: Utc::now(),
            sequence: seq,
            checkpoint: checkpoint.map(|c| c.to_vec()),
        });

        Ok(new_version)
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
        self.register_with_checkpoint(
            tenant,
            db,
            table,
            hash,
            schema_json,
            None,
        )
        .await
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
    use serde_json::json;

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

    #[tokio::test]
    async fn test_sequence_ordering() {
        let reg = InMemoryRegistry::new();

        // Register schemas for different tables
        reg.register("t", "db", "tbl1", "h1", &json!({}))
            .await
            .unwrap();
        reg.register("t", "db", "tbl2", "h2", &json!({}))
            .await
            .unwrap();
        reg.register("t", "db", "tbl1", "h3", &json!({}))
            .await
            .unwrap(); // new version of tbl1

        let versions1 = reg.list_versions("t", "db", "tbl1");
        let versions2 = reg.list_versions("t", "db", "tbl2");

        // Sequences are globally ordered
        assert_eq!(versions1[0].sequence, 1); // tbl1 v1
        assert_eq!(versions2[0].sequence, 2); // tbl2 v1
        assert_eq!(versions1[1].sequence, 3); // tbl1 v2
    }

    #[tokio::test]
    async fn test_get_at_sequence() {
        let reg = InMemoryRegistry::new();

        reg.register("t", "db", "tbl", "h1", &json!({"v": 1}))
            .await
            .unwrap();
        reg.register("t", "db", "tbl", "h2", &json!({"v": 2}))
            .await
            .unwrap();
        reg.register("t", "db", "tbl", "h3", &json!({"v": 3}))
            .await
            .unwrap();

        // Query at sequence 1 returns v1
        let sv = reg.get_at_sequence("t", "db", "tbl", 1).unwrap();
        assert_eq!(sv.version, 1);

        // Query at sequence 2 returns v2
        let sv = reg.get_at_sequence("t", "db", "tbl", 2).unwrap();
        assert_eq!(sv.version, 2);

        // Query at sequence 5 returns v3 (latest before 5)
        let sv = reg.get_at_sequence("t", "db", "tbl", 5).unwrap();
        assert_eq!(sv.version, 3);
    }

    #[tokio::test]
    async fn test_checkpoint_correlation() {
        let reg = InMemoryRegistry::new();

        let checkpoint = b"mysql-bin.000001:12345";
        reg.register_with_checkpoint(
            "t",
            "db",
            "tbl",
            "h1",
            &json!({"cols": ["id"]}),
            Some(checkpoint),
        )
        .await
        .unwrap();

        let sv = reg.get_latest("t", "db", "tbl").unwrap();
        assert_eq!(sv.checkpoint.as_deref(), Some(checkpoint.as_slice()));
    }
}
