// Generic scenarios — parameterised over SourceBackend, run for any source.
pub mod crash_recovery;
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

// PostgreSQL-specific scenarios.
pub mod pg_failover;
pub mod slot_dropped;
