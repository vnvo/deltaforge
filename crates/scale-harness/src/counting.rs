//! A storage backend wrapper that counts every primitive call and the bytes
//! it returns, and (on demand) records which keys were read.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Result;
use async_trait::async_trait;
use serde::Serialize;
use storage::{
    ArcStorageBackend, LogAppendOutcome, LogEntryMeta, LogStreamMeta,
    LogTruncateOutcome, LogTruncateRequest, SlotPage, StorageBackend,
};

/// Primitives that enumerate a namespace or read a whole stream without a
/// bound. None may occur on the registry's startup or lookup paths.
pub const ENUMERATION: &[&str] = &[
    "kv_list",
    "log_list",
    "log_since",
    "log_ns_max_seq",
    "slot_list",
];

/// Calls and bytes for one primitive.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct OpStat {
    pub calls: u64,
    pub bytes_returned: u64,
    /// `bytes_returned` minus the length of each returned record's
    /// `registered_at` timestamp, when normalization is on (else equal to
    /// `bytes_returned`). Registration time is written by the real
    /// registration path and serialized with a variable number of fractional
    /// digits, so equal tables in two fixtures can differ by a few bytes.
    pub bytes_returned_normalized: u64,
    pub bytes_written: u64,
}

/// The complete per-primitive operation vector.
pub type OpVector = BTreeMap<&'static str, OpStat>;

/// One recorded read: namespace and key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct KeyRead {
    pub primitive: &'static str,
    pub ns: String,
    pub key: String,
}

#[derive(Debug)]
pub struct CountingBackend {
    inner: ArcStorageBackend,
    ops: Mutex<OpVector>,
    record_keys: AtomicBool,
    normalize: AtomicBool,
    reads: Mutex<Vec<KeyRead>>,
}

impl CountingBackend {
    pub fn new(inner: ArcStorageBackend) -> Self {
        Self {
            inner,
            ops: Mutex::new(BTreeMap::new()),
            record_keys: AtomicBool::new(false),
            normalize: AtomicBool::new(false),
            reads: Mutex::new(Vec::new()),
        }
    }

    /// Zero every counter and forget recorded reads.
    pub fn reset(&self) {
        self.ops.lock().unwrap().clear();
        self.reads.lock().unwrap().clear();
    }

    /// Record the key of every read from now on (off by default: a large run
    /// would otherwise distort memory measurements).
    pub fn record_keys(&self, on: bool) {
        self.record_keys.store(on, Ordering::SeqCst);
    }

    /// Compute `bytes_returned_normalized` (parses every returned record; off
    /// by default so scale runs do not pay for it).
    pub fn normalize_timestamps(&self, on: bool) {
        self.normalize.store(on, Ordering::SeqCst);
    }

    fn normalized(&self, b: &[u8]) -> usize {
        if !self.normalize.load(Ordering::SeqCst) {
            return b.len();
        }
        match serde_json::from_slice::<serde_json::Value>(b) {
            Ok(serde_json::Value::Object(m)) => match m.get("registered_at") {
                Some(serde_json::Value::String(ts)) => b.len() - ts.len(),
                _ => b.len(),
            },
            _ => b.len(),
        }
    }

    pub fn ops(&self) -> OpVector {
        self.ops.lock().unwrap().clone()
    }

    pub fn reads(&self) -> Vec<KeyRead> {
        self.reads.lock().unwrap().clone()
    }

    /// Calls of enumeration primitives in `ops`.
    pub fn enumeration_calls(ops: &OpVector) -> u64 {
        ENUMERATION
            .iter()
            .filter_map(|p| ops.get(p))
            .map(|s| s.calls)
            .sum()
    }

    fn count(&self, primitive: &'static str, returned: usize, written: usize) {
        self.count_n(primitive, returned, returned, written);
    }

    fn count_n(
        &self,
        primitive: &'static str,
        returned: usize,
        normalized: usize,
        written: usize,
    ) {
        let mut ops = self.ops.lock().unwrap();
        let s = ops.entry(primitive).or_default();
        s.calls += 1;
        s.bytes_returned += returned as u64;
        s.bytes_returned_normalized += normalized as u64;
        s.bytes_written += written as u64;
    }

    fn records<'a>(
        &self,
        values: impl Iterator<Item = &'a [u8]>,
    ) -> (usize, usize) {
        values.fold((0, 0), |(raw, norm), v| {
            (raw + v.len(), norm + self.normalized(v))
        })
    }

    fn read(&self, primitive: &'static str, ns: &str, key: &str) {
        if self.record_keys.load(Ordering::SeqCst) {
            self.reads.lock().unwrap().push(KeyRead {
                primitive,
                ns: ns.to_string(),
                key: key.to_string(),
            });
        }
    }
}

#[async_trait]
impl StorageBackend for CountingBackend {
    async fn kv_get(&self, ns: &str, key: &str) -> Result<Option<Vec<u8>>> {
        self.read("kv_get", ns, key);
        let r = self.inner.kv_get(ns, key).await?;
        let (raw, norm) = self.records(r.iter().map(Vec::as_slice));
        self.count_n("kv_get", raw, norm, 0);
        Ok(r)
    }
    async fn kv_put(&self, ns: &str, key: &str, value: &[u8]) -> Result<()> {
        self.count("kv_put", 0, value.len());
        self.inner.kv_put(ns, key, value).await
    }
    async fn kv_put_with_ttl(
        &self,
        ns: &str,
        key: &str,
        value: &[u8],
        ttl_secs: u64,
    ) -> Result<()> {
        self.count("kv_put_with_ttl", 0, value.len());
        self.inner.kv_put_with_ttl(ns, key, value, ttl_secs).await
    }
    async fn kv_delete(&self, ns: &str, key: &str) -> Result<bool> {
        self.count("kv_delete", 0, 0);
        self.inner.kv_delete(ns, key).await
    }
    async fn kv_list(
        &self,
        ns: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<String>> {
        self.read("kv_list", ns, prefix.unwrap_or(""));
        let r = self.inner.kv_list(ns, prefix).await?;
        self.count("kv_list", r.iter().map(String::len).sum(), 0);
        Ok(r)
    }
    async fn log_append(
        &self,
        ns: &str,
        key: &str,
        value: &[u8],
    ) -> Result<u64> {
        self.count("log_append", 0, value.len());
        self.inner.log_append(ns, key, value).await
    }
    async fn log_list(
        &self,
        ns: &str,
        key: &str,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        self.read("log_list", ns, key);
        let r = self.inner.log_list(ns, key).await?;
        let (raw, norm) = self.records(r.iter().map(|(_, b)| b.as_slice()));
        self.count_n("log_list", raw, norm, 0);
        Ok(r)
    }
    async fn log_since(
        &self,
        ns: &str,
        key: &str,
        since_seq: u64,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        self.read("log_since", ns, key);
        let r = self.inner.log_since(ns, key, since_seq).await?;
        let (raw, norm) = self.records(r.iter().map(|(_, b)| b.as_slice()));
        self.count_n("log_since", raw, norm, 0);
        Ok(r)
    }
    async fn log_latest(
        &self,
        ns: &str,
        key: &str,
    ) -> Result<Option<(u64, Vec<u8>)>> {
        self.read("log_latest", ns, key);
        let r = self.inner.log_latest(ns, key).await?;
        let (raw, norm) = self.records(r.iter().map(|(_, b)| b.as_slice()));
        self.count_n("log_latest", raw, norm, 0);
        Ok(r)
    }
    async fn log_ns_max_seq(&self, ns: &str) -> Result<u64> {
        self.read("log_ns_max_seq", ns, "");
        self.count("log_ns_max_seq", 8, 0);
        self.inner.log_ns_max_seq(ns).await
    }
    async fn log_append_if_absent(
        &self,
        ns: &str,
        key: &str,
        capture_id: &str,
        value: &[u8],
    ) -> Result<LogAppendOutcome> {
        self.count("log_append_if_absent", 0, value.len());
        self.inner
            .log_append_if_absent(ns, key, capture_id, value)
            .await
    }
    async fn log_truncate(
        &self,
        ns: &str,
        key: &str,
        req: LogTruncateRequest,
    ) -> Result<LogTruncateOutcome> {
        self.count("log_truncate", 0, 0);
        self.inner.log_truncate(ns, key, req).await
    }
    async fn log_stream_meta(
        &self,
        ns: &str,
        key: &str,
    ) -> Result<LogStreamMeta> {
        self.read("log_stream_meta", ns, key);
        self.count("log_stream_meta", 0, 0);
        self.inner.log_stream_meta(ns, key).await
    }
    async fn log_read_meta_since(
        &self,
        ns: &str,
        key: &str,
        since_seq: u64,
        limit: usize,
    ) -> Result<Vec<LogEntryMeta>> {
        self.read("log_read_meta_since", ns, key);
        let r = self
            .inner
            .log_read_meta_since(ns, key, since_seq, limit)
            .await?;
        let (raw, norm) = self.records(r.iter().map(|e| e.value.as_slice()));
        self.count_n("log_read_meta_since", raw, norm, 0);
        Ok(r)
    }
    async fn slot_upsert(
        &self,
        ns: &str,
        key: &str,
        state: &[u8],
    ) -> Result<u64> {
        self.count("slot_upsert", 0, state.len());
        self.inner.slot_upsert(ns, key, state).await
    }
    async fn slot_get(
        &self,
        ns: &str,
        key: &str,
    ) -> Result<Option<(u64, Vec<u8>)>> {
        self.read("slot_get", ns, key);
        let r = self.inner.slot_get(ns, key).await?;
        let (raw, norm) = self.records(r.iter().map(|(_, b)| b.as_slice()));
        self.count_n("slot_get", raw, norm, 0);
        Ok(r)
    }
    async fn slot_cas(
        &self,
        ns: &str,
        key: &str,
        expected_version: u64,
        state: &[u8],
    ) -> Result<bool> {
        self.count("slot_cas", 0, state.len());
        self.inner.slot_cas(ns, key, expected_version, state).await
    }
    async fn slot_create(
        &self,
        ns: &str,
        key: &str,
        state: &[u8],
    ) -> Result<Option<u64>> {
        self.count("slot_create", 0, state.len());
        self.inner.slot_create(ns, key, state).await
    }
    async fn slot_delete(&self, ns: &str, key: &str) -> Result<bool> {
        self.count("slot_delete", 0, 0);
        self.inner.slot_delete(ns, key).await
    }
    async fn slot_list(
        &self,
        ns: &str,
        prefix: Option<&str>,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<SlotPage> {
        self.read("slot_list", ns, prefix.unwrap_or(""));
        let r = self.inner.slot_list(ns, prefix, cursor, limit).await?;
        let (raw, norm) =
            self.records(r.records.iter().map(|x| x.value.as_slice()));
        self.count_n("slot_list", raw, norm, 0);
        Ok(r)
    }
    async fn queue_push(
        &self,
        ns: &str,
        key: &str,
        value: &[u8],
    ) -> Result<u64> {
        self.count("queue_push", 0, value.len());
        self.inner.queue_push(ns, key, value).await
    }
    async fn queue_peek(
        &self,
        ns: &str,
        key: &str,
        limit: usize,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        self.read("queue_peek", ns, key);
        let r = self.inner.queue_peek(ns, key, limit).await?;
        let (raw, norm) = self.records(r.iter().map(|(_, b)| b.as_slice()));
        self.count_n("queue_peek", raw, norm, 0);
        Ok(r)
    }
    async fn queue_ack(
        &self,
        ns: &str,
        key: &str,
        up_to_id: u64,
    ) -> Result<usize> {
        self.count("queue_ack", 0, 0);
        self.inner.queue_ack(ns, key, up_to_id).await
    }
    async fn queue_len(&self, ns: &str, key: &str) -> Result<u64> {
        self.count("queue_len", 0, 0);
        self.inner.queue_len(ns, key).await
    }
    async fn queue_drop_oldest(
        &self,
        ns: &str,
        key: &str,
        count: usize,
    ) -> Result<usize> {
        self.count("queue_drop_oldest", 0, 0);
        self.inner.queue_drop_oldest(ns, key, count).await
    }
}
