//! S3 / object-storage sink.
//!
//! This sink writes CDC events as Parquet or JSON Lines files to S3-compatible
//! object storage (S3, MinIO, GCS, Azure Blob, local filesystem).
//!
//! See `docs/specs/s3-parquet-sink.md` for the full design.
//!
//! Phase 1a/1b status: file format plumbing only (Parquet + JSON Lines). Sink
//! trait wire-up and full feature set follow in later phases.

mod encoder;
mod file_format;
mod jsonl_writer;
mod object_writer;
mod parquet_writer;
mod router;

pub use encoder::events_to_record_batch;

pub use file_format::{Compression, FileFormat, WriteResult};
pub use jsonl_writer::JsonLinesFormat;
pub use object_writer::{ObjectStoreParams, build_object_store};
pub use parquet_writer::{ParquetFormat, ParquetSinkWriter, SimpleRow};
pub use router::{PartitionKey, partition_for};
