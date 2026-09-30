//! Shared fault-injecting storage backend for adapter unit tests.

use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Result;

use crate::{
    LogAppendOutcome, LogEntryMeta, LogStreamMeta, LogTruncateOutcome,
    LogTruncateRequest, MemoryStorageBackend, SlotPage, StorageBackend,
};

/// Delegates to an in-memory backend; each flag makes one primitive fail.
#[derive(Debug, Default)]
pub struct FaultBackend {
    inner: MemoryStorageBackend,
    pub fail_log_list: AtomicBool,
    pub fail_log_latest: AtomicBool,
    pub fail_kv_get: AtomicBool,
    pub fail_kv_put: AtomicBool,
    pub fail_log_read_meta: AtomicBool,
    /// Writes still allowed before every further write fails (crash model).
    pub writes_left: std::sync::atomic::AtomicU64,
}

impl FaultBackend {
    pub fn new() -> Self {
        let b = Self::default();
        b.writes_left.store(u64::MAX, Ordering::SeqCst);
        b
    }

    /// Allow exactly `n` more writes, then fail every write (a crash at that
    /// boundary). `u64::MAX` removes the limit.
    pub fn allow_writes(&self, n: u64) {
        self.writes_left.store(n, Ordering::SeqCst);
    }

    fn write(&self) -> Result<()> {
        let left = self.writes_left.load(Ordering::SeqCst);
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
        self.write()?;
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
        self.write()?;
        self.inner.kv_put_with_ttl(ns, key, value, ttl).await
    }
    async fn kv_delete(&self, ns: &str, key: &str) -> Result<bool> {
        self.write()?;
        self.inner.kv_delete(ns, key).await
    }
    async fn kv_list(
        &self,
        ns: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<String>> {
        self.inner.kv_list(ns, prefix).await
    }
    async fn log_append(
        &self,
        ns: &str,
        key: &str,
        value: &[u8],
    ) -> Result<u64> {
        self.write()?;
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
        self.write()?;
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
        self.write()?;
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
        self.write()?;
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
        self.write()?;
        self.inner.slot_cas(ns, key, expected, state).await
    }
    async fn slot_create(
        &self,
        ns: &str,
        key: &str,
        state: &[u8],
    ) -> Result<Option<u64>> {
        self.write()?;
        self.inner.slot_create(ns, key, state).await
    }
    async fn slot_delete(&self, ns: &str, key: &str) -> Result<bool> {
        self.write()?;
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
        self.write()?;
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
        self.write()?;
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
        self.write()?;
        self.inner.queue_drop_oldest(ns, key, count).await
    }
}
