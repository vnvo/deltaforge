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

Change: `ReplicationClient::wait_started` (with `WorkerState::notify_started`).
`connect` returns as soon as the background worker is spawned; connecting,
authentication and START_REPLICATION happen afterwards. A consumer that must
verify the replication slot while it is held by its session (DeltaForge
rereads the slot bounds after START_REPLICATION, before consuming any event)
needs to know when the server accepted START_REPLICATION. `wait_started`
resolves then, or returns the worker's error if it ended first. Files:
`src/client/worker.rs`, `src/client/tokio_client.rs`.

Change: `ReplicationClient::connect_gated`, `simple_query`, `identify_system`,
`timeline_history` and `start` (with `GateCommand` / `QueryRow` in the
worker). `connect_gated` connects and authenticates like `connect`, then the
worker waits instead of sending START_REPLICATION: queries (SQL or replication
commands) run on that authenticated session until `start` sends
START_REPLICATION; dropping the client first ends the session with Terminate.
DeltaForge proves on the session that then streams which server, timeline and
slot state it is on (IDENTIFY_SYSTEM, TIMELINE_HISTORY, the slot row) before
the stream starts, so nothing can change between the proof and the stream.
A server error answer leaves the session usable; any other failure ends it.
Files: `src/client/worker.rs`, `src/client/tokio_client.rs`,
`src/client/mod.rs`.
