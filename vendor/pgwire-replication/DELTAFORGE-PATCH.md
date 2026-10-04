# DeltaForge patch of pgwire-replication 0.4.0

Upstream: https://crates.io/crates/pgwire-replication (MIT OR Apache-2.0,
license files kept). Used through `[patch.crates-io]` in the workspace
manifest. Removed: the CI configuration (`.github/`, `.cargo/`) and the
examples, benches and tests (with their manifest sections; the tests
carried TLS fixture keys), none of which DeltaForge builds.

Change: `ReplicationConfig::ack_lsn` / `with_ack_lsn`. The client initializes
the position it reports to the server as flushed (standby status updates) from
`start_lsn`. A consumer that resumes reading from a position it has not made
durable (DeltaForge reconnects at its last handed-on commit, ahead of the
durable checkpoint) would therefore advance the slot's `confirmed_flush_lsn`
past what it can recover after a crash. `ack_lsn` sets that initial reported
position independently of where reading starts; `None` keeps the upstream
behavior. Files: `src/config.rs`, `src/client/tokio_client.rs`,
`src/client/worker.rs`.
