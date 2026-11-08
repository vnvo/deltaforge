//! S3 / object-storage sink.
//!
//! This sink writes CDC events as Parquet or JSON Lines files to S3-compatible
//! object storage (S3, MinIO, GCS, Azure Blob, local filesystem).
//!
//! See `docs/specs/s3-parquet-sink.md` for the full design.
//!
//! Phase 1a/1b status: file format plumbing only (Parquet + JSON Lines). Sink
//! trait wire-up and full feature set follow in later phases.

mod batch_upload;
mod compaction;
mod durable_encode;
mod durable_sink;
mod encoder;
mod equivalence;
#[cfg(test)]
mod fault_matrix;
mod file_format;
mod gc;
mod head;
mod jsonl_writer;
mod keys;
mod manifest;
mod object_writer;
mod parquet_writer;
mod reconcile;
mod rolling;
mod rollup;
mod router;
#[cfg(test)]
mod s3_server_contract;
#[cfg(test)]
mod s3_server_it;
mod sink;
mod store_cond;
mod writer_pool;

pub use durable_sink::{
    DurableFormat, DurableS3Args, DurableS3Sink, build_durable_s3_sink,
};
pub use encoder::events_to_record_batch;
pub use file_format::{Compression, FileFormat, FileWriter, WriteResult};
pub use jsonl_writer::JsonLinesFormat;
pub use object_writer::{ObjectStoreParams, build_object_store};
pub use parquet_writer::{ParquetFormat, ParquetSinkWriter, SimpleRow};
pub use rolling::{RollReason, RollingConfig};
pub use router::{PartitionKey, partition_for};
pub use sink::{S3Sink, S3SinkArgs, build_s3_sink};
pub use writer_pool::SchemaResolver;
pub use writer_pool::{CommittedFile, WriterPool, WriterPoolConfig};

/// Whether the durable_v2 sink `cfg` of `pipeline` already has state (a
/// HEAD) in its bucket. A read only: a local store whose root does not
/// exist has none, and nothing is created. An error means it cannot be
/// told.
pub async fn durable_state_exists(
    cfg: &deltaforge_config::S3SinkCfg,
    creds: &crate::ResolvedSinkCreds,
    pipeline: &str,
) -> anyhow::Result<bool> {
    let params = object_writer::s3_object_store_params(cfg, creds)?;
    if params.local && !std::path::Path::new(&params.bucket).exists() {
        return Ok(false);
    }
    let store = object_writer::build_object_store(&params)?;
    head::head_exists(store.as_ref(), &cfg.prefix, pipeline)
        .await
        .map_err(|e| anyhow::anyhow!("read the durable_v2 HEAD: {e}"))
}
