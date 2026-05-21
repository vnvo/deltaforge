// Scenario metadata registry (single source of truth for CLI + pre-flight).
pub mod meta;

// Generic scenarios — parameterised over SourceBackend, run for any source.
pub mod crash_recovery;
pub mod dlq_poison;
pub mod exactly_once;
pub mod network_partition;
pub mod schema_drift;
pub mod sink_outage;

// MySQL-specific scenarios.
pub mod binlog_purge;
pub mod failover;
pub mod soak;

// Heavy endurance / benchmark scenarios — not included in `--scenario all`.
// Each prints a requirements preamble before running (or before failing if
// the scenario is not yet implemented).
pub mod backlog_drain;
pub mod tpc_di;
pub mod tpc_e;
pub mod tpcc;

// Avro / Schema Registry scenarios.
pub mod sr_outage;

// S3 / Lakehouse scenarios.
pub mod s3_outage;
pub mod s3_soak;

// PostgreSQL-specific scenarios.
pub mod pg_failover;
pub mod slot_dropped;
