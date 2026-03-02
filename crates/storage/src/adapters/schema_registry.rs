//! Durable schema registry backed by the [`StorageBackend`] Log primitive.
//!
//! On startup it replays the log to populate an in-memory cache identical to
//! [`InMemoryRegistry`], so hot-path reads are the same performance as before.
//! All writes go through the log - cold-start reconstruction is always possible.
//!
//! # Namespace
//! Uses `ns = "schemas"`, `key = "{tenant}/{db}/{table}"`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use deltaforge_core::SchemaRegistry;
use schema_registry::SchemaVersion;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::info;

use crate::ArcStorageBackend;

const NS: &str = "schemas";

/// Serialization format for log entries.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LogEntry {
    hash: String,
    schema_json: serde_json::Value,
    registered_at: chrono::DateTime<chrono::Utc>,
    checkpoint: Option<Vec<u8>>,
}

type Cache = HashMap<(String, String, String), Vec<SchemaVersion>>;

/// Durable schema registry that survives process restarts.
///
/// Identical API to `InMemoryRegistry` but persisted via [`StorageBackend`].
pub struct DurableSchemaRegistry {
    backend: ArcStorageBackend,
    cache: RwLock<Cache>,
    /// Mirrors backend global seq for in-process reads.
    sequence: AtomicU64,
}

impl DurableSchemaRegistry {
    /// Load from an existing backend, replaying the log to populate cache.
    pub async fn new(backend: ArcStorageBackend) -> Result<Arc<Self>> {
        let registry = Arc::new(Self {
            backend,
            cache: RwLock::new(HashMap::new()),
            sequence: AtomicU64::new(0),
        });
        registry.load_from_backend().await?;
        Ok(registry)
    }

    /// Replay the full log into the in-memory cache.
    async fn load_from_backend(&self) -> Result<()> {
        // List all schema keys by listing the "schemas" namespace.
        let keys = self.backend.kv_list(NS, None).await?;
        // kv_list gives KV keys — but schemas use the Log primitive.
        // We need a different approach: list known log keys.
        // Since we can't enumerate log keys without a separate index,
        // we use the KV namespace as a key index: each schema key is
        // registered in KV as well (tombstone-style, value = "1").
        //
        // This avoids adding a new primitive while enabling enumeration.
        let mut max_seq = 0u64;
        let mut cache = self.cache.write().await;

        for raw_key in keys {
            let entries = self.backend.log_list(NS, &raw_key).await?;
            if entries.is_empty() {
                continue;
            }
            let (tenant, db, table) = parse_schema_key(&raw_key)?;
            let map_key = (tenant, db, table);
            let mut versions: Vec<SchemaVersion> = Vec::new();

            for (seq, val) in entries {
                max_seq = max_seq.max(seq);
                let entry: LogEntry = serde_json::from_slice(&val)?;
                let version = (versions.len() as i32) + 1;
                versions.push(SchemaVersion {
                    version,
                    hash: entry.hash,
                    schema_json: entry.schema_json,
                    registered_at: entry.registered_at,
                    sequence: seq,
                    checkpoint: entry.checkpoint,
                });
            }
            cache.insert(map_key, versions);
        }
        self.sequence.store(max_seq, Ordering::SeqCst);
        info!(
            entries = max_seq,
            "DurableSchemaRegistry: replayed log from backend"
        );
        Ok(())
    }

    /// Schema key to backend key.
    fn backend_key(tenant: &str, db: &str, table: &str) -> String {
        format!("{tenant}/{db}/{table}")
    }

    pub fn current_sequence(&self) -> u64 {
        self.sequence.load(Ordering::SeqCst)
    }

    pub fn get_latest(
        &self,
        tenant: &str,
        db: &str,
        table: &str,
    ) -> Option<SchemaVersion> {
        let cache = self.cache.try_read().ok()?;
        let key = (tenant.to_string(), db.to_string(), table.to_string());
        cache.get(&key).and_then(|v| v.last().cloned())
    }

    pub fn get_at_sequence(
        &self,
        tenant: &str,
        db: &str,
        table: &str,
        sequence: u64,
    ) -> Option<SchemaVersion> {
        let cache = self.cache.try_read().ok()?;
        let key = (tenant.to_string(), db.to_string(), table.to_string());
        cache.get(&key).and_then(|versions| {
            versions
                .iter()
                .filter(|v| v.sequence <= sequence)
                .max_by_key(|v| v.sequence)
                .cloned()
        })
    }

    pub fn list_versions(
        &self,
        tenant: &str,
        db: &str,
        table: &str,
    ) -> Vec<SchemaVersion> {
        self.cache
            .try_read()
            .ok()
            .and_then(|c| {
                let key =
                    (tenant.to_string(), db.to_string(), table.to_string());
                c.get(&key).cloned()
            })
            .unwrap_or_default()
    }

    /// Register with optional checkpoint correlation.
    pub async fn register_with_checkpoint(
        &self,
        tenant: &str,
        db: &str,
        table: &str,
        hash: &str,
        schema_json: &serde_json::Value,
        checkpoint: Option<&[u8]>,
    ) -> Result<i32> {
        let map_key = (tenant.to_string(), db.to_string(), table.to_string());

        // Check idempotency in cache first (fast path).
        {
            let cache = self.cache.read().await;
            if let Some(existing) = cache
                .get(&map_key)
                .and_then(|v| v.iter().find(|sv| sv.hash == hash))
            {
                return Ok(existing.version);
            }
        }

        // Persist to log.
        let entry = LogEntry {
            hash: hash.to_string(),
            schema_json: schema_json.clone(),
            registered_at: Utc::now(),
            checkpoint: checkpoint.map(|c| c.to_vec()),
        };
        let val = serde_json::to_vec(&entry)?;
        let backend_key = Self::backend_key(tenant, db, table);
        let seq = self.backend.log_append(NS, &backend_key, &val).await?;
        // Also register the key in KV so enumeration works on cold start.
        self.backend.kv_put(NS, &backend_key, b"1").await?;

        // Update cache.
        let mut cache = self.cache.write().await;
        let versions = cache.entry(map_key).or_default();
        let version = (versions.len() as i32) + 1;
        versions.push(SchemaVersion {
            version,
            hash: hash.to_string(),
            schema_json: schema_json.clone(),
            registered_at: entry.registered_at,
            sequence: seq,
            checkpoint: checkpoint.map(|c| c.to_vec()),
        });
        self.sequence.fetch_max(seq, Ordering::SeqCst);
        Ok(version)
    }
}

#[async_trait]
impl SchemaRegistry for DurableSchemaRegistry {
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
        let cache = self.cache.read().await;
        let key = (tenant.to_string(), db.to_string(), table.to_string());
        Ok(cache
            .get(&key)
            .and_then(|v| v.last().map(|sv| (sv.version, sv.hash.clone()))))
    }
}

fn parse_schema_key(raw_key: &str) -> Result<(String, String, String)> {
    let mut parts = raw_key.splitn(3, '/');
    let tenant = parts
        .next()
        .ok_or_else(|| anyhow::anyhow!("invalid schema key: {raw_key}"))?;
    let db = parts
        .next()
        .ok_or_else(|| anyhow::anyhow!("invalid schema key: {raw_key}"))?;
    let table = parts
        .next()
        .ok_or_else(|| anyhow::anyhow!("invalid schema key: {raw_key}"))?;
    Ok((tenant.to_string(), db.to_string(), table.to_string()))
}
