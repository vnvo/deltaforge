//! `FileFormat` + `FileWriter` traits — the shared interface for Parquet and
//! JSON Lines writers.
//!
//! `FileFormat` is the factory: given an object store + path + Arrow schema,
//! it opens a `FileWriter`. `FileWriter` is the per-file state holder that
//! ingests `Event` batches incrementally and finalizes on `close`.
//!
//! Both formats share the same rolling, partitioning, multipart, atomic-commit,
//! DLQ and metrics machinery (built in this and subsequent phases). The trait
//! isolates serialization concerns from coordination concerns.

use std::sync::Arc;

use anyhow::Result;
use arrow_schema::Schema;
use async_trait::async_trait;
use deltaforge_core::Event;
use object_store::{ObjectStore, path::Path};

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

/// A pluggable file format. Implementations are stateless factories — the
/// actual write state lives in the `FileWriter` they produce.
#[async_trait]
pub trait FileFormat: Send + Sync {
    /// File extension *without* the dot (e.g. `"parquet"`, `"jsonl"`, `"jsonl.gz"`).
    fn extension(&self) -> &'static str;

    /// MIME type for the `Content-Type` header set on object PUTs.
    fn content_type(&self) -> &'static str;

    /// Format identifier for metrics labels.
    fn label(&self) -> &'static str;

    /// Open a new writer targeting `path`. The schema is used by Parquet and
    /// ignored by JSON Lines. The returned writer accepts incremental
    /// `append` calls and must be `close`d to finalize the upload.
    async fn open_writer(
        &self,
        store: Arc<dyn ObjectStore>,
        path: Path,
        schema: Arc<Schema>,
    ) -> Result<Box<dyn FileWriter + Send>>;
}

/// An open file writer. Appends are incremental; `close` finalizes (Parquet
/// footer + multipart complete, or JSONL flush + put).
///
/// Implementations are expected to be `!Sync` — writers are owned exclusively
/// by the partition they belong to and never shared across tasks.
#[async_trait]
pub trait FileWriter: Send {
    /// Append a batch of events to this writer.
    ///
    /// Per-event coercion failures bubble up as an error and fail the whole
    /// append; per-row DLQ routing lands in Phase 1f via a return-type change.
    async fn append(&mut self, events: &[Event]) -> Result<()>;

    /// Bytes accumulated so far. For Parquet this is an estimate (the final
    /// size is known only after the footer is written), for JSONL it is the
    /// in-memory buffer size before any compression.
    fn bytes_written(&self) -> u64;

    /// Events written so far across all `append` calls.
    fn events_written(&self) -> u64;

    /// Close the writer, flush the footer (Parquet) or final blob (JSONL),
    /// complete the multipart upload, and return the final result.
    async fn close(self: Box<Self>) -> Result<WriteResult>;
}
