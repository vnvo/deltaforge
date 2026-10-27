//! In-memory `StorageBackend` - for testing and development only.
//!
//! State is lost on drop. Not suitable for production.

use crate::{
    AppendStatus, AtomicBatch, BatchFault, BatchFaults, BatchOp, BatchOutcome,
    BatchRead, BatchReadResult, BatchValue, LogAppendOutcome, LogEntryMeta,
    LogError, LogStreamMeta, LogTruncateOutcome, LogTruncateRequest,
    Precondition, StorageBackend, content_digest, max_seq_value, render_seq,
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

/// Ordered by `(ns, key)` so `slot_list` can range-iterate a prefix with bounded
/// (`O(limit)`) temporary memory instead of materializing and sorting all matches.
#[derive(Debug, Default)]
struct SlotStore(BTreeMap<NsKey, (u64, Vec<u8>)>);

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
    /// Test-only fault point of `apply_batch` (one shot).
    batch_fault: std::sync::Mutex<Option<BatchFault>>,
    /// Test-only: when set, `apply_batch` checks, the moment it publishes a
    /// commit marker and while it still holds the locks, that the batch's
    /// logs, kv writes and global sequence are already visible, and records
    /// the verdict.
    #[cfg(test)]
    verify_on_marker: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    marker_verdicts: std::sync::Mutex<Vec<bool>>,
    /// Commit markers by batch id.
    commits: std::sync::Mutex<
        std::collections::HashMap<String, crate::BatchCommitRecord>,
    >,
}

impl BatchFaults for MemoryStorageBackend {
    fn set_batch_fault(&self, fault: Option<BatchFault>) {
        *self.batch_fault.lock().expect("batch fault") = fault;
    }
}

impl MemoryStorageBackend {
    /// Whether a fault set with [`BatchFaults::set_batch_fault`] has not
    /// fired yet (tests check it reached the batch they aimed at).
    pub fn batch_fault_armed(&self) -> bool {
        self.batch_fault.lock().expect("batch fault").is_some()
    }

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
    async fn log_read_streams(
        &self,
        streams: &[(String, String)],
    ) -> Result<Vec<Vec<LogEntryMeta>>> {
        // One snapshot: the log held for every stream.
        let store = self.log.read().await;
        Ok(streams
            .iter()
            .map(|(ns, key)| {
                store
                    .entries
                    .get(&(ns.clone(), key.clone()))
                    .map(|entries| {
                        entries
                            .iter()
                            .map(|e| LogEntryMeta {
                                seq: e.seq,
                                stored_at_ms: e.ts_ms,
                                capture_id: e.capture_id.clone(),
                                content_hash: e.content_hash.clone(),
                                value: e.value.clone(),
                            })
                            .collect()
                    })
                    .unwrap_or_default()
            })
            .collect())
    }

    async fn read_batch(
        &self,
        reads: &[BatchRead],
    ) -> Result<Vec<BatchReadResult>> {
        // One snapshot: both stores held for the whole batch.
        let log = self.log.read().await;
        let kv = self.kv.read().await;
        Ok(reads
            .iter()
            .map(|r| match r {
                BatchRead::KvGet { ns, key } => BatchReadResult::Kv(
                    kv.0.get(&(ns.clone(), key.clone()))
                        .filter(|e| !e.is_expired())
                        .map(|e| e.value.clone()),
                ),
                BatchRead::LogLatest { ns, key } => BatchReadResult::Latest(
                    log.entries
                        .get(&(ns.clone(), key.clone()))
                        .and_then(|v| v.last())
                        .map(|e| (e.seq, e.value.clone())),
                ),
                BatchRead::LogHead { ns, key } => BatchReadResult::Head(
                    log.entries
                        .get(&(ns.clone(), key.clone()))
                        .and_then(|v| v.last())
                        .map_or(0, |e| e.seq),
                ),
                BatchRead::LogLen { ns, key } => BatchReadResult::Len(
                    log.entries
                        .get(&(ns.clone(), key.clone()))
                        .map_or(0, |v| v.len() as u64),
                ),
            })
            .collect())
    }

    async fn apply_batch(&self, batch: &AtomicBatch) -> Result<BatchOutcome> {
        batch.validate()?;
        let fault = self.batch_fault.lock().expect("batch fault").take();
        if fault == Some(BatchFault::BeforeTransaction) {
            anyhow::bail!("injected failure before the transaction");
        }
        // One critical section: the log lock is held across allocation and
        // application, so the range is contiguous and nothing interleaves.
        let mut log = self.log.write().await;
        let mut kv = self.kv.write().await;
        let kv_value = |kv: &KvStore, ns: &str, key: &str| {
            kv.0.get(&(ns.to_string(), key.to_string()))
                .filter(|e| !e.is_expired())
                .map(|e| e.value.clone())
        };
        for (i, p) in batch.preconditions.iter().enumerate() {
            let holds = match p {
                Precondition::LogHead { ns, key, seq } => {
                    log.entries
                        .get(&(ns.clone(), key.clone()))
                        .and_then(|v| v.last())
                        .map_or(0, |e| e.seq)
                        == *seq
                }
                Precondition::KvIs { ns, key, value } => {
                    kv_value(&kv, ns, key) == *value
                }
            };
            if !holds {
                return Ok(BatchOutcome::Conflict {
                    why: format!("precondition {i} no longer holds ({p:?})"),
                });
            }
        }
        // Stage every write; apply only once all are staged.
        let base = self.global_seq.load(Ordering::SeqCst);
        let mut seqs: Vec<Option<u64>> = vec![None; batch.ops.len()];
        let mut next = base;
        let mut staged_logs: Vec<(NsKey, MemLogEntry)> = Vec::new();
        let mut staged_kv: Vec<(NsKey, Vec<u8>)> = Vec::new();
        let mut staged_marker: Option<(String, crate::BatchCommitRecord)> =
            None;
        for (i, op) in batch.ops.iter().enumerate() {
            if fault == Some(BatchFault::AfterWrites(i)) {
                anyhow::bail!("injected failure after {i} staged writes");
            }
            match op {
                BatchOp::LogAppendIfAbsent {
                    ns,
                    key,
                    capture_id,
                    value,
                } => {
                    let k = (ns.clone(), key.clone());
                    let exists = log.entries.get(&k).is_some_and(|v| {
                        v.iter().any(|e| {
                            e.capture_id.as_deref() == Some(capture_id)
                        })
                    }) || staged_logs.iter().any(|(sk, e)| {
                        sk == &k && e.capture_id.as_deref() == Some(capture_id)
                    });
                    if exists {
                        return Ok(BatchOutcome::Conflict {
                            why: format!(
                                "{capture_id} already exists in {key}"
                            ),
                        });
                    }
                    next += 1;
                    seqs[i] = Some(next);
                    staged_logs.push((
                        k,
                        MemLogEntry {
                            seq: next,
                            value: value.clone(),
                            capture_id: Some(capture_id.clone()),
                            content_hash: Some(content_digest(value)),
                            ts_ms: now_ms(),
                        },
                    ));
                }
                BatchOp::KvPut { ns, key, value } => {
                    let bytes = match value {
                        BatchValue::Bytes(b) => b.clone(),
                        BatchValue::SeqTemplate { template, of } => {
                            render_seq(template, seqs[*of].expect("validated"))
                        }
                    };
                    staged_kv.push(((ns.clone(), key.clone()), bytes));
                }
                BatchOp::KvMaxSeq { .. } | BatchOp::KvMax { .. } => {
                    let (ns, key, n) = match op {
                        BatchOp::KvMaxSeq { ns, key, of } => {
                            (ns, key, seqs[*of].expect("validated"))
                        }
                        BatchOp::KvMax { ns, key, value } => (ns, key, *value),
                        _ => unreachable!(),
                    };
                    let k = (ns.clone(), key.clone());
                    let current = staged_kv
                        .iter()
                        .rev()
                        .find(|(sk, _)| sk == &k)
                        .map(|(_, v)| v.clone())
                        .or_else(|| kv_value(&kv, ns, key));
                    staged_kv.push((k, max_seq_value(current.as_deref(), n)?));
                }
                BatchOp::CommitMarker { batch_id, digest } => {
                    let result = crate::encode_batch_result(
                        &batch.ops[..i],
                        &seqs[..i],
                    )?;
                    staged_marker = Some((
                        batch_id.clone(),
                        crate::BatchCommitRecord {
                            digest: digest.clone(),
                            result,
                            committed_at_ms: now_ms(),
                        },
                    ));
                }
            }
        }
        if fault == Some(BatchFault::AfterWrites(batch.ops.len())) {
            anyhow::bail!("injected failure after every write was staged");
        }
        // Commit: the batch's state first and its marker last, all under
        // the log and kv locks, so a visible marker always means the whole
        // batch is visible. A duplicate marker id is refused before anything
        // is applied.
        if let Some((id, _)) = &staged_marker {
            anyhow::ensure!(
                !self.commits.lock().expect("commits").contains_key(id),
                "commit marker {id} already exists"
            );
        }
        for (k, e) in staged_logs.iter().cloned() {
            let seq = e.seq;
            log.ensure_meta(&k);
            log.entries.entry(k.clone()).or_default().push(e);
            if let Some(m) = log.meta.get_mut(&k) {
                m.head_seq = m.head_seq.max(seq);
            }
        }
        for (k, v) in staged_kv.iter().cloned() {
            kv.0.insert(
                k,
                KvEntry {
                    value: v,
                    expires_at: None,
                },
            );
        }
        self.global_seq.store(next, Ordering::SeqCst);
        if let Some((id, marker)) = staged_marker {
            self.commits.lock().expect("commits").insert(id, marker);
            #[cfg(test)]
            if self.verify_on_marker.load(Ordering::SeqCst) {
                let visible = staged_logs.iter().all(|(k, e)| {
                    log.entries
                        .get(k)
                        .is_some_and(|v| v.iter().any(|x| x.seq == e.seq))
                }) && staged_kv
                    .iter()
                    .all(|(k, v)| kv.0.get(k).is_some_and(|x| &x.value == v))
                    && self.global_seq.load(Ordering::SeqCst) == next;
                self.marker_verdicts.lock().unwrap().push(visible);
            }
        }
        if fault == Some(BatchFault::AfterCommit) {
            anyhow::bail!("injected failure after the commit");
        }
        Ok(BatchOutcome::Committed { seqs })
    }

    async fn batch_commit_get(
        &self,
        batch_id: &str,
    ) -> Result<Option<crate::BatchCommitRecord>> {
        Ok(self.commits.lock().expect("commits").get(batch_id).cloned())
    }

    async fn batch_commit_sweep(
        &self,
        before_ms: i64,
        limit: usize,
    ) -> Result<usize> {
        let mut commits = self.commits.lock().expect("commits");
        let mut old: Vec<(i64, String)> = commits
            .iter()
            .filter(|(_, m)| m.committed_at_ms < before_ms)
            .map(|(id, m)| (m.committed_at_ms, id.clone()))
            .collect();
        old.sort();
        old.truncate(limit);
        for (_, id) in &old {
            commits.remove(id);
        }
        Ok(old.len())
    }

    async fn log_find_capture(
        &self,
        ns: &str,
        key: &str,
        capture_id: &str,
    ) -> Result<Option<(u64, Vec<u8>)>> {
        let log = self.log.read().await;
        Ok(log
            .entries
            .get(&(ns.to_string(), key.to_string()))
            .and_then(|v| {
                v.iter()
                    .find(|e| e.capture_id.as_deref() == Some(capture_id))
            })
            .map(|e| (e.seq, e.value.clone())))
    }

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
        anyhow::ensure!(
            !crate::is_guarded_namespace(ns),
            "a key in guarded namespace {ns} cannot carry a TTL"
        );
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
        // Allocated under the log lock: entries are stored in sequence order.
        let mut store = self.log.write().await;
        let seq = self.next_seq();
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

    async fn log_ns_max_seq(&self, ns: &str) -> Result<u64> {
        let store = self.log.read().await;
        Ok(store
            .entries
            .iter()
            .filter(|((n, _), _)| n == ns)
            .filter_map(|(_, v)| v.last().map(|e| e.seq))
            .max()
            .unwrap_or(0))
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

    async fn log_read_meta_since(
        &self,
        ns: &str,
        key: &str,
        since_seq: u64,
        limit: usize,
    ) -> Result<Vec<LogEntryMeta>> {
        let store = self.log.read().await;
        Ok(store
            .entries
            .get(&(ns.to_string(), key.to_string()))
            .map(|entries| {
                entries
                    .iter()
                    .filter(|e| e.seq > since_seq)
                    .take(limit)
                    .map(|e| LogEntryMeta {
                        seq: e.seq,
                        stored_at_ms: e.ts_ms,
                        capture_id: e.capture_id.clone(),
                        content_hash: e.content_hash.clone(),
                        value: e.value.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default())
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
        use std::collections::btree_map::Entry;
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

    async fn slot_list(
        &self,
        ns: &str,
        prefix: Option<&str>,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<crate::SlotPage> {
        let prefix_norm = prefix.unwrap_or("");
        let after = match cursor {
            Some(c) => Some(crate::decode_slot_cursor(c, ns, prefix_norm)?),
            None => None,
        };
        let eff = limit.min(crate::SLOT_LIST_MAX_LIMIT);
        if eff == 0 {
            return Ok(crate::SlotPage {
                records: Vec::new(),
                next_cursor: None,
            });
        }

        // Range-scan from the exclusive cursor (or the prefix start) and stop at the
        // first non-match; the BTreeMap is ordered by `(ns, key)`, so matches are
        // contiguous and we keep at most `eff + 1` records in memory.
        use std::ops::Bound;
        let start = match &after {
            Some(a) => Bound::Excluded((ns.to_string(), a.clone())),
            None => Bound::Included((ns.to_string(), prefix_norm.to_string())),
        };
        let store = self.slot.read().await;
        let mut records: Vec<crate::SlotRecord> = Vec::new();
        for ((n, k), (v, val)) in store.0.range((start, Bound::Unbounded)) {
            if n != ns || !k.starts_with(prefix_norm) {
                break;
            }
            records.push(crate::SlotRecord {
                key: k.clone(),
                version: *v,
                value: val.clone(),
            });
            if records.len() > eff {
                break; // one extra fetched -> "more" detected
            }
        }

        let has_more = records.len() > eff;
        records.truncate(eff);
        let next_cursor = if has_more {
            records
                .last()
                .map(|r| crate::encode_slot_cursor(ns, prefix_norm, &r.key))
        } else {
            None
        };
        Ok(crate::SlotPage {
            records,
            next_cursor,
        })
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
    /// The commit marker is the batch's last visible write: when it becomes
    /// visible, every staged log entry, kv write and the global sequence are
    /// already visible.
    #[tokio::test]
    async fn the_commit_marker_is_published_after_the_batch_state() {
        use crate::{AtomicBatch, BatchOp, BatchValue, StorageBackend};
        let be = super::MemoryStorageBackend::new();
        be.verify_on_marker
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let batch = AtomicBatch {
            preconditions: Vec::new(),
            ops: vec![
                BatchOp::LogAppendIfAbsent {
                    ns: "schemas.v1".into(),
                    key: "s".into(),
                    capture_id: "c1".into(),
                    value: b"e".to_vec(),
                },
                BatchOp::KvPut {
                    ns: "schemas.v1.index".into(),
                    key: "k".into(),
                    value: BatchValue::Bytes(b"v".to_vec()),
                },
                BatchOp::CommitMarker {
                    batch_id: "b1".into(),
                    digest: vec![1],
                },
            ],
        };
        be.apply_batch(&batch).await.unwrap();
        assert_eq!(
            *be.marker_verdicts.lock().unwrap(),
            [true],
            "the marker was visible before the batch's state"
        );
    }

    use super::*;
    use std::sync::Arc;

    fn be() -> Arc<dyn StorageBackend> {
        Arc::new(MemoryStorageBackend::new())
    }

    #[tokio::test]
    async fn idempotency() {
        crate::log_contract_suite::idempotency(be(), "journal").await;
    }
    #[tokio::test]
    async fn conflict_rejected() {
        crate::log_contract_suite::conflict_rejected(be(), "journal").await;
    }
    #[tokio::test]
    async fn horizon_with_global_gaps() {
        crate::log_contract_suite::horizon_with_global_gaps(be(), "journal")
            .await;
    }
    #[tokio::test]
    async fn pin_invariant() {
        crate::log_contract_suite::pin_invariant(be(), "journal").await;
    }
    #[tokio::test]
    async fn empty_vs_truncated() {
        crate::log_contract_suite::empty_vs_truncated(be(), "journal").await;
    }
    #[tokio::test]
    async fn concurrent_appends() {
        crate::log_contract_suite::concurrent_appends(be(), "journal").await;
        crate::log_contract_suite::ns_max_seq_scoped(be(), "nsmax").await;
    }

    #[tokio::test]
    async fn kv_list_prefix_is_literal() {
        crate::kv_list_contract_suite::literal_prefix(be(), "kvp").await;
    }

    // slot_list contract suite.
    #[tokio::test]
    async fn slot_list_namespace_isolation() {
        crate::slot_list_contract_suite::namespace_isolation(be(), "slots")
            .await;
    }
    #[tokio::test]
    async fn slot_list_prefix_correctness() {
        crate::slot_list_contract_suite::prefix_correctness(be(), "slots")
            .await;
    }
    #[tokio::test]
    async fn slot_list_pagination_without_duplicates() {
        crate::slot_list_contract_suite::pagination_without_duplicates(
            be(),
            "slots",
        )
        .await;
    }
    #[tokio::test]
    async fn slot_list_concurrent_mutation_tolerance() {
        crate::slot_list_contract_suite::concurrent_mutation_tolerance(
            be(),
            "slots",
        )
        .await;
    }
    #[tokio::test]
    async fn slot_list_malformed_cursor_rejected() {
        crate::slot_list_contract_suite::malformed_cursor_rejected(
            be(),
            "slots",
        )
        .await;
    }
    #[tokio::test]
    async fn slot_list_zero_limit_is_empty() {
        crate::slot_list_contract_suite::zero_limit_is_empty(be(), "slots")
            .await;
    }
    #[tokio::test]
    async fn slot_list_unicode_ordering() {
        crate::slot_list_contract_suite::unicode_ordering_and_pagination(
            be(),
            "slots",
        )
        .await;
    }
    #[tokio::test]
    async fn slot_list_prefix_utf8_length_boundary() {
        crate::slot_list_contract_suite::prefix_across_utf8_length_boundary(
            be(),
            "slots",
        )
        .await;
    }
    #[tokio::test]
    async fn slot_list_cursor_validation() {
        crate::slot_list_contract_suite::cursor_validation(be(), "slots").await;
    }

    /// The shared atomic-batch conformance suite.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn atomic_batch_conformance() {
        let b = Arc::new(MemoryStorageBackend::new());
        crate::atomic_batch_suite::all(b.clone(), b.as_ref(), "batch").await;
    }
}
