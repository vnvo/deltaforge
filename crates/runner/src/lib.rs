pub mod coordinator;
pub mod drift_detector;
pub mod pipeline_manager;
pub mod schema_provider;

pub use coordinator::{
    CommitCpFn, Coordinator, ProcessBatchFn, ProcessedBatch, SchemaSensorState,
    build_batch_processor, build_commit_fn,
};

pub use drift_detector::{
    DriftDetector, DriftEvent, DriftSummary, DriftType, TableDriftTracker,
};

pub use pipeline_manager::PipelineManager;

pub use schema_provider::{
    ArcSchemaProvider, ColumnSchemaInfo, SchemaProvider, TableSchemaInfo,
};
