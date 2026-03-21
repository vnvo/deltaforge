// Generic scenarios — parameterised over SourceBackend, run for any source.
pub mod crash_recovery;
pub mod network_partition;
pub mod schema_drift;
pub mod sink_outage;

// MySQL-specific scenarios.
pub mod binlog_purge;
pub mod failover;

// PostgreSQL-specific scenarios.
pub mod pg_failover;
pub mod slot_dropped;
