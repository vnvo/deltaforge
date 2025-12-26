pub mod coordinator;
pub mod drift_detector;
pub mod pipeline_manager;
mod schema_api;
mod schema_provider;
mod sensing_api;

pub use schema_provider::{
    ArcSchemaProvider, ColumnSchemaInfo, SchemaLoaderAdapter, SchemaProvider,
    TableSchemaInfo, is_json_type, might_be_json,
};

pub use coordinator::{
    CommitCpFn, Coordinator, ProcessBatchFn, ProcessedBatch, SchemaSensorState,
    build_batch_processor, build_commit_fn,
};

pub use pipeline_manager::PipelineManager;
pub use schema_api::SchemaApi;
pub use sensing_api::SensingApi;
