//! In-memory `StorageBackend` - for testing and development only.
//!
//! State is lost on drop. Not suitable for production.

use crate::{
    AppendStatus, LogAppendOutcome, LogError, LogStreamMeta,
    LogTruncateOutcome, LogTruncateRequest, StorageBackend, content_digest,
};
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

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
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

type NsKey = (String, String);
type QueueEntry = (u64, Vec<u8>);

#[derive(Debug, Default)]
struct KvStore(HashMap<NsKey, KvEntry>);

#[derive(Debug, Clone)]
struct MemLogEntry {
    seq: u64,
    value: Vec<u8>,
    capture_id: Option<String>,
    content_hash: Option<String>,
    ts_ms: i64,
}

#[derive(Debug, Clone, Copy)]
struct MemLogMeta {
    min_valid_from_seq: u64,
    head_seq: u64,
}

/// Entries are kept per `(ns, key)` in ascending-seq order (append order, since the
/// global seq is monotonic). `meta` is the durable stream metadata, created lazily.
#[derive(Debug, Default)]
struct LogStore {
    entries: BTreeMap<NsKey, Vec<MemLogEntry>>,
    meta: HashMap<NsKey, MemLogMeta>,
}

impl LogStore {
    /// Lazily initialize the metadata row from current entries (head = highest seq
    /// present, horizon = 0). Non-destructive; never rewrites entries.
    fn ensure_meta(&mut self, k: &NsKey) -> MemLogMeta {
        if let Some(m) = self.meta.get(k) {
            return *m;
        }
        let head_seq = self
            .entries
            .get(k)
            .and_then(|v| v.last())
            .map_or(0, |e| e.seq);
        let m = MemLogMeta {
            min_valid_from_seq: 0,
            head_seq,
        };
        self.meta.insert(k.clone(), m);
        m
    }
}

#[derive(Debug, Default)]
struct SlotStore(HashMap<NsKey, (u64, Vec<u8>)>);

#[derive(Debug, Default)]
struct QueueStore(HashMap<NsKey, VecDeque<QueueEntry>>);

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
        let k = (ns.to_string(), key.to_string());
        store
            .entries
            .entry(k.clone())
            .or_default()
            .push(MemLogEntry {
                seq,
                value: value.to_vec(),
                capture_id: None,
                content_hash: None,
                ts_ms: now_ms(),
            });
        // Maintain head_seq atomically where stream metadata exists.
        if let Some(m) = store.meta.get_mut(&k) {
            m.head_seq = m.head_seq.max(seq);
        }
        Ok(seq)
    }

    async fn log_list(
        &self,
        ns: &str,
        key: &str,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        let store = self.log.read().await;
        Ok(store
            .entries
            .get(&(ns.to_string(), key.to_string()))
            .map(|v| v.iter().map(|e| (e.seq, e.value.clone())).collect())
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
            .entries
            .get(&(ns.to_string(), key.to_string()))
            .map(|entries| {
                entries
                    .iter()
                    .filter(|e| e.seq > since_seq)
                    .map(|e| (e.seq, e.value.clone()))
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
            .entries
            .get(&(ns.to_string(), key.to_string()))
            .and_then(|entries| entries.last())
            .map(|e| (e.seq, e.value.clone())))
    }

    async fn log_append_if_absent(
        &self,
        ns: &str,
        key: &str,
        capture_id: &str,
        value: &[u8],
    ) -> Result<LogAppendOutcome> {
        let mut store = self.log.write().await;
        let k = (ns.to_string(), key.to_string());
        store.ensure_meta(&k);

        // Idempotent / conflict check: does this capture_id already exist?
        if let Some(existing) = store.entries.get(&k).and_then(|v| {
            v.iter()
                .find(|e| e.capture_id.as_deref() == Some(capture_id))
        }) {
            let digest = content_digest(value);
            // Cheap digest check, then authoritative exact-bytes comparison.
            if existing.content_hash.as_deref() == Some(digest.as_str())
                && existing.value == value
            {
                return Ok(LogAppendOutcome {
                    seq: existing.seq,
                    status: AppendStatus::AlreadyPresent,
                });
            }
            return Err(LogError::CaptureIdentityConflict {
                capture_id: capture_id.to_string(),
            }
            .into());
        }

        let seq = self.next_seq();
        let digest = content_digest(value);
        store
            .entries
            .entry(k.clone())
            .or_default()
            .push(MemLogEntry {
                seq,
                value: value.to_vec(),
                capture_id: Some(capture_id.to_string()),
                content_hash: Some(digest),
                ts_ms: now_ms(),
            });
        if let Some(m) = store.meta.get_mut(&k) {
            m.head_seq = m.head_seq.max(seq);
        }
        Ok(LogAppendOutcome {
            seq,
            status: AppendStatus::Inserted,
        })
    }

    async fn log_truncate(
        &self,
        ns: &str,
        key: &str,
        req: LogTruncateRequest,
    ) -> Result<LogTruncateOutcome> {
        let mut store = self.log.write().await;
        let k = (ns.to_string(), key.to_string());
        let mut meta = store.ensure_meta(&k);

        let (
            removed,
            removed_bytes,
            highest_removed_seq,
            oldest_seq,
            capacity_pinned,
        ) = {
            let entries = store.entries.entry(k.clone()).or_default();
            let n = entries.len();
            // Entries are ascending by seq; the removable window is the leading run
            // with seq < pin_seq.
            let below_pin =
                entries.iter().take_while(|e| e.seq < req.pin_seq).count();

            let age_remove = match req.older_than_ms {
                Some(cutoff) => {
                    entries.iter().take_while(|e| e.ts_ms < cutoff).count()
                }
                None => 0,
            };
            let entries_cap_remove = match req.max_entries {
                Some(max) => n.saturating_sub(max as usize),
                None => 0,
            };
            let bytes_cap_remove = match req.max_bytes {
                Some(max) => {
                    let total: u64 =
                        entries.iter().map(|e| e.value.len() as u64).sum();
                    let mut over = total.saturating_sub(max);
                    let mut cnt = 0usize;
                    for e in entries.iter() {
                        if over == 0 {
                            break;
                        }
                        over = over.saturating_sub(e.value.len() as u64);
                        cnt += 1;
                    }
                    cnt
                }
                None => 0,
            };

            let desired =
                age_remove.max(entries_cap_remove).max(bytes_cap_remove);
            let actual = desired.min(below_pin);
            // A capacity cap (not age) wanted to remove more than pin_seq allowed.
            let capacity_pinned =
                entries_cap_remove.max(bytes_cap_remove) > below_pin;

            let removed_bytes: u64 = entries
                .iter()
                .take(actual)
                .map(|e| e.value.len() as u64)
                .sum();
            let highest_removed_seq = if actual > 0 {
                Some(entries[actual - 1].seq)
            } else {
                None
            };
            entries.drain(0..actual);
            let oldest_seq = entries.first().map(|e| e.seq);
            (
                actual,
                removed_bytes,
                highest_removed_seq,
                oldest_seq,
                capacity_pinned,
            )
        };

        if let Some(h) = highest_removed_seq {
            meta.min_valid_from_seq = meta.min_valid_from_seq.max(h);
        }
        store.meta.insert(k, meta);

        Ok(LogTruncateOutcome {
            removed,
            removed_bytes,
            oldest_seq,
            highest_removed_seq,
            min_valid_from_seq: meta.min_valid_from_seq,
            head_seq: meta.head_seq,
            capacity_pinned,
        })
    }

    async fn log_stream_meta(
        &self,
        ns: &str,
        key: &str,
    ) -> Result<LogStreamMeta> {
        let mut store = self.log.write().await;
        let k = (ns.to_string(), key.to_string());
        let meta = store.ensure_meta(&k);
        let entries = store.entries.get(&k);
        let oldest_seq = entries.and_then(|v| v.first()).map(|e| e.seq);
        let len = entries.map_or(0, |v| v.len() as u64);
        Ok(LogStreamMeta {
            min_valid_from_seq: meta.min_valid_from_seq,
            oldest_seq,
            head_seq: meta.head_seq,
            len,
        })
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

    async fn slot_create(
        &self,
        ns: &str,
        key: &str,
        state: &[u8],
    ) -> Result<Option<u64>> {
        use std::collections::hash_map::Entry;
        let mut store = self.slot.write().await;
        let k = (ns.to_string(), key.to_string());
        match store.0.entry(k) {
            Entry::Occupied(_) => Ok(None),
            Entry::Vacant(v) => {
                v.insert((1, state.to_vec()));
                Ok(Some(1))
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn be() -> Arc<dyn StorageBackend> {
        Arc::new(MemoryStorageBackend::new())
    }

    #[tokio::test]
    async fn idempotency() {
        crate::log_contract_suite::idempotency(be()).await;
    }
    #[tokio::test]
    async fn conflict_rejected() {
        crate::log_contract_suite::conflict_rejected(be()).await;
    }
    #[tokio::test]
    async fn horizon_with_global_gaps() {
        crate::log_contract_suite::horizon_with_global_gaps(be()).await;
    }
    #[tokio::test]
    async fn pin_invariant() {
        crate::log_contract_suite::pin_invariant(be()).await;
    }
    #[tokio::test]
    async fn empty_vs_truncated() {
        crate::log_contract_suite::empty_vs_truncated(be()).await;
    }
    #[tokio::test]
    async fn concurrent_appends() {
        crate::log_contract_suite::concurrent_appends(be()).await;
    }
}
