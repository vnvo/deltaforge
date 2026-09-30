//! Elasticsearch sink: mirrors CDC events into an ES index as an idempotent,
//! order-correct current-state document set (upsert + `version_type=external`).
//!
//! Modules are added task-by-task per `docs/specs/elasticsearch-sink-plan.md`.

pub mod bulk;
pub mod client;
pub mod id;
pub mod index;
pub mod mapping;
pub mod normalize;
pub mod sink;
pub mod version;

#[cfg(test)]
pub(crate) mod test_support;

use std::sync::Arc;

/// Reuse the ClickHouse resolver output — source columns (types) + primary key.
pub use crate::clickhouse::TableColumns;
pub use sink::{ElasticsearchSink, build_elasticsearch_sink};

/// Resolve a source table (`"namespace.table"`) to its columns + PK.
///
/// Built by the runner from its `SchemaProvider` and injected into the sink, so
/// the `sinks` crate never depends on `runner`. Same inversion the ClickHouse
/// and S3 sinks use.
/// `Ok(None)`: no schema is known for the table yet. `Err`: the schema cannot
/// be read right now (for example the source has not verified its server yet);
/// the error carries the reason. Both are retryable and never cached.
pub type EsSchemaResolver =
    Arc<dyn Fn(&str) -> anyhow::Result<Option<TableColumns>> + Send + Sync>;
