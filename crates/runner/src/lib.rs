pub mod coordinator;
pub mod pipeline_manager;

// re-export for benchmarks
pub use coordinator::{Coordinator, CommitCpFn, ProcessBatchFn, ProcessedBatch};