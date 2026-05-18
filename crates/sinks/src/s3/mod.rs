//! S3 / object-storage sink.
//!
//! This sink writes CDC events as Parquet or JSON Lines files to S3-compatible
//! object storage (S3, MinIO, GCS, Azure Blob, local filesystem).
//!
//! See `docs/specs/s3-parquet-sink.md` for the full design.
//!
//! Phase 1a status: object_store + parquet plumbing only. Sink trait wire-up
//! and full feature set follow in later phases.

mod object_writer;
mod parquet_writer;

pub use object_writer::{ObjectStoreParams, build_object_store};
pub use parquet_writer::{ParquetSinkWriter, SimpleRow};
