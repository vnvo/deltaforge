//! In-memory `StorageBackend` - for testing and development only.
//!
//! State is lost on drop. Not suitable for production.

use crate::StorageBackend;
use anyhow::Result;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use tokio::sync::RwLock;

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[derive(Debug, Clone)]
struct KvEntry {
    value: Vec<u8>,
    expires_at: Option<u64>,
}

impl KvEntry {
    fn is_expired(&self) -> bool {
        self.expires_at.is_some_and(|exp| exp <= now_secs())
    }
}

#[derive(Debug, Default)]
struct KvStore(HashMap<(String, String), KvEntry>);

#[derive(Debug, Default)]
struct LogStore(BTreeMap<(String, String), Vec<(u64, Vec<u8>)>>);

#[derive(Debug, Default)]
struct SlotStore(HashMap<(String, String), (u64, Vec<u8>)>);

#[derive(Debug, Default)]
struct QueueStore(HashMap<(String, String), VecDeque<(u64, Vec<u8>)>>);

/// In-memory storage backend. All state is lost on drop.
#[derive(Debug, Default)]
pub struct MemoryStorageBackend {
    kv: RwLock<KvStore>,
    log: RwLock<LogStore>,
    slot: RwLock<SlotStore>,
    queue: RwLock<QueueStore>,
    global_seq: AtomicU64,
    queue_id: AtomicU64,
}

impl MemoryStorageBackend {
    pub fn new() -> Self {
        Self::default()
    }

    fn next_seq(&self) -> u64 {
        self.global_seq.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn next_queue_id(&self) -> u64 {
        self.queue_id.fetch_add(1, Ordering::SeqCst) + 1
    }
}

#[async_trait]
impl StorageBackend for MemoryStorageBackend {
    // ── KV ──────────────────────────────────────────────────────────────────

    async fn kv_get(&self, ns: &str, key: &str) -> Result<Option<Vec<u8>>> {
        let store = self.kv.read().await;
        let k = (ns.to_string(), key.to_string());
        Ok(store.0.get(&k).and_then(|e| {
            if e.is_expired() {
                None
            } else {
                Some(e.value.clone())
            }
        }))
    }

    async fn kv_put(&self, ns: &str, key: &str, value: &[u8]) -> Result<()> {
        let mut store = self.kv.write().await;
        store.0.insert(
            (ns.to_string(), key.to_string()),
            KvEntry {
                value: value.to_vec(),
                expires_at: None,
            },
        );
        Ok(())
    }

    async fn kv_put_with_ttl(
        &self,
        ns: &str,
        key: &str,
        value: &[u8],
        ttl_secs: u64,
    ) -> Result<()> {
        let mut store = self.kv.write().await;
        store.0.insert(
            (ns.to_string(), key.to_string()),
            KvEntry {
                value: value.to_vec(),
                expires_at: Some(now_secs() + ttl_secs),
            },
        );
        Ok(())
    }

    async fn kv_delete(&self, ns: &str, key: &str) -> Result<bool> {
        let mut store = self.kv.write().await;
        Ok(store.0.remove(&(ns.to_string(), key.to_string())).is_some())
    }

    async fn kv_list(
        &self,
        ns: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<String>> {
        let store = self.kv.read().await;
        let now = now_secs();
        let mut keys: Vec<String> = store
            .0
            .iter()
            .filter(|((n, k), e)| {
                n == ns
                    && e.expires_at.is_none_or(|exp| exp > now)
                    && prefix.is_none_or(|p| k.starts_with(p))
            })
            .map(|((_, k), _)| k.clone())
            .collect();
        keys.sort();
        Ok(keys)
    }

    // ── Log ─────────────────────────────────────────────────────────────────

    async fn log_append(
        &self,
        ns: &str,
        key: &str,
        value: &[u8],
    ) -> Result<u64> {
        let seq = self.next_seq();
        let mut store = self.log.write().await;
        store
            .0
            .entry((ns.to_string(), key.to_string()))
            .or_default()
            .push((seq, value.to_vec()));
        Ok(seq)
    }

    async fn log_list(
        &self,
        ns: &str,
        key: &str,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        let store = self.log.read().await;
        Ok(store
            .0
            .get(&(ns.to_string(), key.to_string()))
            .cloned()
            .unwrap_or_default())
    }

    async fn log_since(
        &self,
        ns: &str,
        key: &str,
        since_seq: u64,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        let store = self.log.read().await;
        Ok(store
            .0
            .get(&(ns.to_string(), key.to_string()))
            .map(|entries| {
                entries
                    .iter()
                    .filter(|(seq, _)| *seq > since_seq)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn log_latest(
        &self,
        ns: &str,
        key: &str,
    ) -> Result<Option<(u64, Vec<u8>)>> {
        let store = self.log.read().await;
        Ok(store
            .0
            .get(&(ns.to_string(), key.to_string()))
            .and_then(|entries| entries.last().cloned()))
    }

    // ── Slot ────────────────────────────────────────────────────────────────

    async fn slot_upsert(
        &self,
        ns: &str,
        key: &str,
        state: &[u8],
    ) -> Result<u64> {
        let mut store = self.slot.write().await;
        let k = (ns.to_string(), key.to_string());
        let version = store.0.get(&k).map(|(v, _)| v + 1).unwrap_or(1);
        store.0.insert(k, (version, state.to_vec()));
        Ok(version)
    }

    async fn slot_get(
        &self,
        ns: &str,
        key: &str,
    ) -> Result<Option<(u64, Vec<u8>)>> {
        let store = self.slot.read().await;
        Ok(store.0.get(&(ns.to_string(), key.to_string())).cloned())
    }

    async fn slot_cas(
        &self,
        ns: &str,
        key: &str,
        expected_version: u64,
        state: &[u8],
    ) -> Result<bool> {
        let mut store = self.slot.write().await;
        let k = (ns.to_string(), key.to_string());
        match store.0.get(&k) {
            Some((v, _)) if *v == expected_version => {
                store.0.insert(k, (expected_version + 1, state.to_vec()));
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn slot_delete(&self, ns: &str, key: &str) -> Result<bool> {
        let mut store = self.slot.write().await;
        Ok(store.0.remove(&(ns.to_string(), key.to_string())).is_some())
    }

    // ── Queue ────────────────────────────────────────────────────────────────

    async fn queue_push(
        &self,
        ns: &str,
        key: &str,
        value: &[u8],
    ) -> Result<u64> {
        let id = self.next_queue_id();
        let mut store = self.queue.write().await;
        store
            .0
            .entry((ns.to_string(), key.to_string()))
            .or_default()
            .push_back((id, value.to_vec()));
        Ok(id)
    }

    async fn queue_peek(
        &self,
        ns: &str,
        key: &str,
        limit: usize,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        let store = self.queue.read().await;
        Ok(store
            .0
            .get(&(ns.to_string(), key.to_string()))
            .map(|q| q.iter().take(limit).cloned().collect())
            .unwrap_or_default())
    }

    async fn queue_ack(
        &self,
        ns: &str,
        key: &str,
        up_to_id: u64,
    ) -> Result<usize> {
        let mut store = self.queue.write().await;
        let k = (ns.to_string(), key.to_string());
        let q = store.0.entry(k).or_default();
        let before = q.len();
        q.retain(|(id, _)| *id > up_to_id);
        Ok(before - q.len())
    }

    async fn queue_len(&self, ns: &str, key: &str) -> Result<u64> {
        let store = self.queue.read().await;
        Ok(store
            .0
            .get(&(ns.to_string(), key.to_string()))
            .map(|q| q.len() as u64)
            .unwrap_or(0))
    }

    async fn queue_drop_oldest(
        &self,
        ns: &str,
        key: &str,
        count: usize,
    ) -> Result<usize> {
        let mut store = self.queue.write().await;
        let k = (ns.to_string(), key.to_string());
        let q = store.0.entry(k).or_default();
        let to_drop = count.min(q.len());
        for _ in 0..to_drop {
            q.pop_front();
        }
        Ok(to_drop)
    }
}
