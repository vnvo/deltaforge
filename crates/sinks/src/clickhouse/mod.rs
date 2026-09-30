//! ClickHouse sink: streams CDC events into ClickHouse over HTTP + RowBinary.
//!
//! Modules are added task-by-task per `docs/specs/clickhouse-sink-plan.md`.

pub mod client;
pub mod ddl;
pub mod project;
pub mod rowbinary;
pub mod sink;
pub mod types;
pub mod version;

pub use sink::{ClickHouseSink, build_clickhouse_sink};

use std::sync::Arc;
use types::ColDesc;

/// The user columns (in declared order) and primary key of a source table.
pub struct TableColumns {
    pub columns: Vec<ColDesc>,
    pub primary_key: Vec<String>,
}

/// Resolve a source table (`"namespace.table"`) to its columns + PK.
///
/// Built by the runner from its `SchemaProvider` and passed into the sink — the
/// same inversion the S3 sink uses for its Arrow schema resolver, so the `sinks`
/// crate never depends on `runner`.
/// `Ok(None)`: no schema is known for the table yet. `Err`: the schema cannot
/// be read right now (for example the source has not verified its server yet);
/// the error carries the reason. Both are retryable and never cached.
pub type ClickHouseSchemaResolver =
    Arc<dyn Fn(&str) -> anyhow::Result<Option<TableColumns>> + Send + Sync>;
