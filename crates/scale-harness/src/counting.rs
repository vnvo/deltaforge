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
    /// `registered_at` timestamp, with a registration checkpoint counted by
    /// its bytes rather than their JSON rendering, when normalization is on
    /// (else equal to `bytes_returned`). Registration time is written by the real
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
    /// Keys whose `kv_get` / prefixes whose `kv_list` are not counted
    /// (time-driven reads).
    uncounted: Mutex<Vec<(String, String)>>,
    /// Slot primitive calls per namespace.
    slots: Mutex<BTreeMap<(&'static str, String), u64>>,
}

impl CountingBackend {
    pub fn new(inner: ArcStorageBackend) -> Self {
        Self {
            inner,
            ops: Mutex::new(BTreeMap::new()),
            record_keys: AtomicBool::new(false),
            normalize: AtomicBool::new(false),
            reads: Mutex::new(Vec::new()),
            uncounted: Mutex::new(Vec::new()),
            slots: Mutex::new(BTreeMap::new()),
        }
    }

    /// Slot primitive calls per `(primitive, namespace)`.
    pub fn slot_ops(&self) -> BTreeMap<(&'static str, String), u64> {
        self.slots.lock().unwrap().clone()
    }

    fn slot(&self, primitive: &'static str, ns: &str) {
        *self
            .slots
            .lock()
            .unwrap()
            .entry((primitive, ns.to_string()))
            .or_default() += 1;
    }

    /// Stop counting `kv_get` of `ns`/`key` and `kv_list` of `ns` with prefix
    /// `key`: for a read whose number depends on elapsed time rather than on
    /// the work measured.
    pub fn uncount(&self, ns: &str, key: &str) {
        self.uncounted
            .lock()
            .unwrap()
            .push((ns.to_string(), key.to_string()));
    }

    /// Zero every counter and forget recorded reads.
    pub fn reset(&self) {
        self.ops.lock().unwrap().clear();
        self.reads.lock().unwrap().clear();
        self.slots.lock().unwrap().clear();
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
        normalized_len(b)
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

    fn is_uncounted(&self, ns: &str, key: &str) -> bool {
        self.uncounted
            .lock()
            .unwrap()
            .iter()
            .any(|(n, k)| n == ns && k == key)
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
        if self.is_uncounted(ns, key) {
            return self.inner.kv_get(ns, key).await;
        }
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
        if let Some(p) = prefix
            && self.is_uncounted(ns, p)
        {
            return self.inner.kv_list(ns, prefix).await;
        }
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
        self.slot("slot_upsert", ns);
        self.count("slot_upsert", 0, state.len());
        self.inner.slot_upsert(ns, key, state).await
    }
    async fn slot_get(
        &self,
        ns: &str,
        key: &str,
    ) -> Result<Option<(u64, Vec<u8>)>> {
        self.slot("slot_get", ns);
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
        self.slot("slot_cas", ns);
        self.count("slot_cas", 0, state.len());
        self.inner.slot_cas(ns, key, expected_version, state).await
    }
    async fn slot_create(
        &self,
        ns: &str,
        key: &str,
        state: &[u8],
    ) -> Result<Option<u64>> {
        self.slot("slot_create", ns);
        self.count("slot_create", 0, state.len());
        self.inner.slot_create(ns, key, state).await
    }
    async fn slot_delete(&self, ns: &str, key: &str) -> Result<bool> {
        self.slot("slot_delete", ns);
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
        self.slot("slot_list", ns);
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

/// A returned record's length without the bytes whose width depends on when
/// or where it was written rather than on how much was read: timestamps
/// (`*_at`, rendered with variable sub-second precision), WAL positions
/// (`lsn`, `*_lsn`, hex of varying width), and a checkpoint stored as a JSON
/// array of byte values (its bytes counted, not their decimal rendering, and
/// positions inside it normalized the same way). Any depth.
pub fn normalized_len(b: &[u8]) -> usize {
    match serde_json::from_slice::<serde_json::Value>(b) {
        Ok(v @ serde_json::Value::Object(_)) => b.len() - variable_width(&v),
        _ => b.len(),
    }
}

/// Bytes of `v`'s rendering that [`normalized_len`] leaves out.
fn variable_width(v: &serde_json::Value) -> usize {
    use serde_json::Value;
    match v {
        Value::Object(m) => m
            .iter()
            .map(|(k, x)| match x {
                Value::String(s)
                    if k.ends_with("_at")
                        || k == "lsn"
                        || k.ends_with("_lsn") =>
                {
                    s.len()
                }
                Value::Array(bytes)
                    if k == "checkpoint"
                        && bytes
                            .iter()
                            .all(|b| b.as_u64().is_some_and(|b| b <= 255)) =>
                {
                    let raw: Vec<u8> = bytes
                        .iter()
                        .filter_map(|b| b.as_u64())
                        .map(|b| b as u8)
                        .collect();
                    let rendered =
                        serde_json::to_vec(bytes).map_or(0, |r| r.len());
                    rendered - raw.len() + (raw.len() - normalized_len(&raw))
                }
                _ => variable_width(x),
            })
            .sum(),
        Value::Array(xs) => xs.iter().map(variable_width).sum(),
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::normalized_len;
    use serde_json::json;

    fn len(v: serde_json::Value) -> usize {
        normalized_len(&serde_json::to_vec(&v).unwrap())
    }

    /// The live_ttfce failure: a catalog annotation captured at a time whose
    /// nanoseconds end in 000 renders 3 digits shorter (chrono's AutoSi), and
    /// a WAL position can gain a hex digit. Neither is work.
    #[test]
    fn timestamp_precision_and_position_width_are_not_counted() {
        let annotation = |at: &str, lsn: &str| json!({"captured_at": at, "captured_lsn": lsn, "relation": "public.app_events"});
        assert_eq!(
            len(annotation(
                "2026-10-13T12:37:00.232998099+00:00",
                "0/FFFFF8"
            )),
            len(annotation(
                "2026-10-13T12:37:05.927926000+00:00",
                "0/1000008"
            )),
        );
        assert_eq!(
            len(annotation(
                "2026-10-13T12:37:00.232998099+00:00",
                "0/2722568"
            )),
            len(annotation("2026-10-13T12:37:05.927926+00:00", "0/2722998")),
        );
    }

    #[test]
    fn positions_inside_a_checkpoint_are_not_counted() {
        let registration = |lsn: &str| {
            let cp = serde_json::to_vec(&json!({"lsn": lsn, "tx_id": null}))
                .unwrap();
            json!({"version": 1, "checkpoint": cp, "registered_at": "2026-10-13T12:37:00Z"})
        };
        assert_eq!(
            len(registration("0/FFFFF8")),
            len(registration("0/10000008"))
        );
    }

    #[test]
    fn more_content_still_counts() {
        assert!(
            len(json!({"columns": ["a", "b", "c"], "captured_at": "x"}))
                > len(json!({"columns": ["a"], "captured_at": "x"}))
        );
        // Not JSON: counted as is.
        assert_eq!(normalized_len(b"raw bytes"), 9);
    }
}
