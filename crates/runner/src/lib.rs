pub mod coordinator;
pub mod dlq;
pub mod drift_detector;
pub mod pipeline_manager;
pub mod replay_controller;
pub mod replay_gate;
pub mod replay_job;
pub mod replay_journal;
pub mod replay_worker;
mod schema_api;
mod schema_provider;
mod sensing_api;

pub use schema_provider::{
    ArcSchemaProvider, AvroSchemaProviderImpl, ColumnSchemaInfo,
    SchemaLoaderAdapter, SchemaProvider, TableSchemaInfo,
    build_arrow_schema_resolver, is_json_type, might_be_json,
};

pub use coordinator::{
    CommitCpFn, Coordinator, ProcessBatchFn, ProcessedBatch, SchemaSensorState,
    build_batch_processor, build_commit_fn,
};

pub use pipeline_manager::PipelineManager;
pub use schema_api::SchemaApi;
pub use sensing_api::SensingApi;
