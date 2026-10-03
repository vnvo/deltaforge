//! Shared fault-injecting storage backend for adapter unit tests.

use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Result;

use crate::{
    ArcStorageBackend, LogAppendOutcome, LogEntryMeta, LogStreamMeta,
    LogTruncateOutcome, LogTruncateRequest, MemoryStorageBackend, SlotPage,
    StorageBackend,
};

/// Delegates to another backend (in-memory by default); each flag makes one
/// primitive fail.
#[derive(Debug)]
pub struct FaultBackend {
    inner: ArcStorageBackend,
    pub fail_log_list: AtomicBool,
    pub fail_log_latest: AtomicBool,
    pub fail_kv_get: AtomicBool,
    pub fail_kv_put: AtomicBool,
    pub fail_log_read_meta: AtomicBool,
    /// Writes still allowed before every further write fails (crash model).
    pub writes_left: std::sync::atomic::AtomicU64,
    /// When set, only the first write past the budget fails; later writes
    /// succeed again (a transient failure).
    one_shot: AtomicBool,
    /// Every `kv_list` call: (namespace, prefix).
    pub kv_list_calls: std::sync::Mutex<Vec<(String, Option<String>)>>,
    /// Every write to one of these namespaces fails while it is listed.
    pub fail_writes_to: std::sync::Mutex<Vec<String>>,
    /// `(namespace, n)`: allow `n` more writes to the namespace, fail the
    /// next one, then clear (a crash at that write).
    pub fail_after_writes_to: std::sync::Mutex<Option<(String, u64)>>,
}

impl Default for FaultBackend {
    fn default() -> Self {
        Self::wrap(std::sync::Arc::new(MemoryStorageBackend::new()))
    }
}

impl FaultBackend {
    pub fn new() -> Self {
        Self::default()
    }

    /// Inject faults in front of `inner` (for example a SQLite store shared
    /// with another process).
    pub fn wrap(inner: ArcStorageBackend) -> Self {
        Self {
            inner,
            fail_log_list: AtomicBool::new(false),
            fail_log_latest: AtomicBool::new(false),
            fail_kv_get: AtomicBool::new(false),
            fail_kv_put: AtomicBool::new(false),
            fail_log_read_meta: AtomicBool::new(false),
            writes_left: std::sync::atomic::AtomicU64::new(u64::MAX),
            one_shot: AtomicBool::new(false),
            kv_list_calls: std::sync::Mutex::new(Vec::new()),
            fail_writes_to: std::sync::Mutex::new(Vec::new()),
            fail_after_writes_to: std::sync::Mutex::new(None),
        }
    }

    /// Allow exactly `n` more writes, fail the next one, then succeed again.
    pub fn fail_one_write_after(&self, n: u64) {
        self.one_shot.store(true, Ordering::SeqCst);
        self.writes_left.store(n, Ordering::SeqCst);
    }

    /// Allow exactly `n` more writes, then fail every write (a crash at that
    /// boundary). `u64::MAX` removes the limit.
    pub fn allow_writes(&self, n: u64) {
        self.one_shot.store(false, Ordering::SeqCst);
        self.writes_left.store(n, Ordering::SeqCst);
    }

    fn write(&self, ns: &str) -> Result<()> {
        anyhow::ensure!(
            !self.fail_writes_to.lock().unwrap().iter().any(|n| n == ns),
            "injected write failure in namespace {ns}"
        );
        {
            let mut after = self.fail_after_writes_to.lock().unwrap();
            if let Some((target, left)) = after.as_mut() {
                if target == ns {
                    if *left == 0 {
                        *after = None;
                        anyhow::bail!("injected crash at a write in {ns}");
                    }
                    *left -= 1;
                }
            }
        }
        let left = self.writes_left.load(Ordering::SeqCst);
        if left == 0 && self.one_shot.swap(false, Ordering::SeqCst) {
            self.writes_left.store(u64::MAX, Ordering::SeqCst);
            anyhow::bail!("injected transient write failure");
        }
        anyhow::ensure!(left > 0, "injected crash: write budget exhausted");
        if left != u64::MAX {
            self.writes_left.store(left - 1, Ordering::SeqCst);
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl StorageBackend for FaultBackend {
    async fn kv_get(&self, ns: &str, key: &str) -> Result<Option<Vec<u8>>> {
        anyhow::ensure!(
            !self.fail_kv_get.load(Ordering::SeqCst),
            "injected kv_get failure"
        );
        self.inner.kv_get(ns, key).await
    }
    async fn kv_put(&self, ns: &str, key: &str, value: &[u8]) -> Result<()> {
        self.write(ns)?;
        anyhow::ensure!(
            !self.fail_kv_put.load(Ordering::SeqCst),
            "injected kv_put failure"
        );
        self.inner.kv_put(ns, key, value).await
    }
    async fn kv_put_with_ttl(
        &self,
        ns: &str,
        key: &str,
        value: &[u8],
        ttl: u64,
    ) -> Result<()> {
        self.write(ns)?;
        self.inner.kv_put_with_ttl(ns, key, value, ttl).await
    }
    async fn kv_delete(&self, ns: &str, key: &str) -> Result<bool> {
        self.write(ns)?;
        self.inner.kv_delete(ns, key).await
    }
    async fn kv_list(
        &self,
        ns: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<String>> {
        self.kv_list_calls
            .lock()
            .unwrap()
            .push((ns.to_string(), prefix.map(str::to_string)));
        self.inner.kv_list(ns, prefix).await
    }
    async fn log_append(
        &self,
        ns: &str,
        key: &str,
        value: &[u8],
    ) -> Result<u64> {
        self.write(ns)?;
        self.inner.log_append(ns, key, value).await
    }
    async fn log_list(
        &self,
        ns: &str,
        key: &str,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        anyhow::ensure!(
            !self.fail_log_list.load(Ordering::SeqCst),
            "log_list must not be used: full-history materialization"
        );
        self.inner.log_list(ns, key).await
    }
    async fn log_since(
        &self,
        ns: &str,
        key: &str,
        since: u64,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        self.inner.log_since(ns, key, since).await
    }
    async fn log_latest(
        &self,
        ns: &str,
        key: &str,
    ) -> Result<Option<(u64, Vec<u8>)>> {
        anyhow::ensure!(
            !self.fail_log_latest.load(Ordering::SeqCst),
            "injected log_latest failure"
        );
        self.inner.log_latest(ns, key).await
    }
    async fn log_ns_max_seq(&self, ns: &str) -> Result<u64> {
        self.inner.log_ns_max_seq(ns).await
    }
    async fn log_append_if_absent(
        &self,
        ns: &str,
        key: &str,
        capture_id: &str,
        value: &[u8],
    ) -> Result<LogAppendOutcome> {
        self.write(ns)?;
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
        self.write(ns)?;
        self.inner.log_truncate(ns, key, req).await
    }
    async fn log_stream_meta(
        &self,
        ns: &str,
        key: &str,
    ) -> Result<LogStreamMeta> {
        self.inner.log_stream_meta(ns, key).await
    }
    async fn log_read_meta_since(
        &self,
        ns: &str,
        key: &str,
        since: u64,
        limit: usize,
    ) -> Result<Vec<LogEntryMeta>> {
        anyhow::ensure!(
            !self.fail_log_read_meta.load(Ordering::SeqCst),
            "injected log_read_meta_since failure"
        );
        self.inner.log_read_meta_since(ns, key, since, limit).await
    }
    async fn slot_upsert(
        &self,
        ns: &str,
        key: &str,
        state: &[u8],
    ) -> Result<u64> {
        self.write(ns)?;
        self.inner.slot_upsert(ns, key, state).await
    }
    async fn slot_get(
        &self,
        ns: &str,
        key: &str,
    ) -> Result<Option<(u64, Vec<u8>)>> {
        self.inner.slot_get(ns, key).await
    }
    async fn slot_cas(
        &self,
        ns: &str,
        key: &str,
        expected: u64,
        state: &[u8],
    ) -> Result<bool> {
        self.write(ns)?;
        self.inner.slot_cas(ns, key, expected, state).await
    }
    async fn slot_create(
        &self,
        ns: &str,
        key: &str,
        state: &[u8],
    ) -> Result<Option<u64>> {
        self.write(ns)?;
        self.inner.slot_create(ns, key, state).await
    }
    async fn slot_delete(&self, ns: &str, key: &str) -> Result<bool> {
        self.write(ns)?;
        self.inner.slot_delete(ns, key).await
    }
    async fn slot_list(
        &self,
        ns: &str,
        prefix: Option<&str>,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<SlotPage> {
        self.inner.slot_list(ns, prefix, cursor, limit).await
    }
    async fn queue_push(
        &self,
        ns: &str,
        key: &str,
        value: &[u8],
    ) -> Result<u64> {
        self.write(ns)?;
        self.inner.queue_push(ns, key, value).await
    }
    async fn queue_peek(
        &self,
        ns: &str,
        key: &str,
        limit: usize,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        self.inner.queue_peek(ns, key, limit).await
    }
    async fn queue_ack(
        &self,
        ns: &str,
        key: &str,
        up_to: u64,
    ) -> Result<usize> {
        self.write(ns)?;
        self.inner.queue_ack(ns, key, up_to).await
    }
    async fn queue_len(&self, ns: &str, key: &str) -> Result<u64> {
        self.inner.queue_len(ns, key).await
    }
    async fn queue_drop_oldest(
        &self,
        ns: &str,
        key: &str,
        count: usize,
    ) -> Result<usize> {
        self.write(ns)?;
        self.inner.queue_drop_oldest(ns, key, count).await
    }
}

/// Rewrite a source's stored lineage record (tests that must force a field,
/// e.g. a repeated `established_at_ms`).
pub async fn rewrite_lineage_record(
    backend: &ArcStorageBackend,
    tenant: &str,
    source_id: &str,
    edit: impl FnOnce(&mut super::source_lineage::SourceLineageRecord),
) -> Result<()> {
    let mut record = super::source_lineage::load(backend, tenant, source_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("no lineage record"))?;
    edit(&mut record);
    backend
        .kv_put(
            super::source_lineage::NS,
            &super::source_lineage::record_key(tenant, source_id),
            &serde_json::to_vec(&record)?,
        )
        .await
}
