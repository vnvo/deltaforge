// Generic scenarios — parameterised over SourceBackend, run for any source.
pub mod network_partition;
pub mod sink_outage;
pub mod crash_recovery;
pub mod schema_drift;

// MySQL-specific scenarios.
pub mod failover;
pub mod binlog_purge;

// PostgreSQL-specific scenarios.
pub mod pg_failover;
pub mod slot_dropped;