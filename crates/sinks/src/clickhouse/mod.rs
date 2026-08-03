//! ClickHouse sink: streams CDC events into ClickHouse over HTTP + RowBinary.
//!
//! Modules are added task-by-task per `docs/specs/clickhouse-sink-plan.md`.

pub mod types;
pub mod version;
