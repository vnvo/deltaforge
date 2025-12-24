pub mod coordinator;
pub mod pipeline_manager;

mod drift_detector;
mod schema_provider;

pub use coordinator::{
    CommitCpFn, Coordinator, ProcessBatchFn, ProcessedBatch, SchemaSensorState,
    build_batch_processor, build_commit_fn,
};

pub use pipeline_manager::PipelineManager;
