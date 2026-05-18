//! `FileFormat` trait — the shared interface for Parquet and JSON Lines writers.
//!
//! Both formats share the same rolling, multipart, atomic-commit, DLQ, and
//! metrics infrastructure (built in Phases 1d-1f). The format itself only
//! decides:
//!   1. The file extension and `Content-Type`
//!   2. How rows are serialized into the on-the-wire bytes
//!
//! Phase 1b keeps the API single-shot (`write_rows` → file appears). Phase 1d
//! introduces `FileWriter` for incremental append.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use object_store::{ObjectStore, path::Path};

use super::SimpleRow;

/// Outcome of writing a single file.
#[derive(Debug, Clone, Copy)]
pub struct WriteResult {
    pub rows_written: u64,
    pub bytes_written: u64,
}

/// Compression to apply on the output stream.
///
/// Format-specific: Parquet uses these as internal page/column compression,
/// JSON Lines wraps the whole file in a single encoded stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None,
    Snappy,
    Gzip,
    Zstd,
}

/// A pluggable file format. Implementations are stateless and cheap to clone.
#[async_trait]
pub trait FileFormat: Send + Sync {
    /// File extension *without* the dot (e.g. `"parquet"`, `"jsonl"`, `"jsonl.gz"`).
    fn extension(&self) -> &'static str;

    /// MIME type for the `Content-Type` header set on object PUTs.
    fn content_type(&self) -> &'static str;

    /// Format identifier for metrics labels.
    fn label(&self) -> &'static str;

    /// Write `rows` to a single file at `path` in this format.
    /// Returns the row + byte counts on success.
    async fn write_rows(
        &self,
        store: Arc<dyn ObjectStore>,
        path: &Path,
        rows: &[SimpleRow],
    ) -> Result<WriteResult>;
}
