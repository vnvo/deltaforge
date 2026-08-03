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

pub use sink::{build_clickhouse_sink, ClickHouseSink};

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
pub type ClickHouseSchemaResolver =
    Arc<dyn Fn(&str) -> Option<TableColumns> + Send + Sync>;
