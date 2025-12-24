pub mod coordinator;
pub mod pipeline_manager;

mod drift_detector;
mod schema_provider;

pub use schema_provider::{
    ArcSchemaProvider, ColumnSchemaInfo, SchemaLoaderAdapter, SchemaProvider,
    TableSchemaInfo, is_json_type, might_be_json,
};

pub use coordinator::{
    CommitCpFn, Coordinator, ProcessBatchFn, ProcessedBatch, SchemaSensorState,
    build_batch_processor, build_commit_fn,
};

pub use pipeline_manager::PipelineManager;
