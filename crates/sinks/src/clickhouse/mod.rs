//! ClickHouse sink: streams CDC events into ClickHouse over HTTP + RowBinary.
//!
//! Modules are added task-by-task per `docs/specs/clickhouse-sink-plan.md`.

pub mod rowbinary;
pub mod types;
pub mod version;
