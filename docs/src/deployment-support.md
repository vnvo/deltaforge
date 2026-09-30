# Supported Deployment Envelope

This page defines the **bounded support envelope for the current DeltaForge release**: the configurations DeltaForge is validated for, what operators must provide, and what is explicitly out of scope. Anything not listed here as supported should be treated as unvalidated - test it in a disposable environment first, and talk to us before relying on it.

The delivery and correctness guarantees referenced below are defined in [Guarantees & Correctness](guarantees.md). Read that page together with this one.

## Build and freeze

- Pin your deployment to a specific release commit or tag rather than tracking `main`, so your environment does not drift.
- Toolchain: built with the Rust **stable** toolchain, edition **2024**. Single static binary; no JVM, no runtime GC.
- Report issues against the exact commit/tag so we can reproduce them.

## Supported versions

| Component | Validated version | Notes |
|-----------|------------------------|-------|
| PostgreSQL (source) | **17** | Logical replication (`wal_level = logical`). There is no hard server-version gate in code; other recent majors likely work but are not part of the validated envelope. Validate before relying on a different major. |
| MySQL (source) | **8.4** | Native binlog CDC. **MariaDB is not supported.** |
| Kafka (sink) | Tested on **Confluent Platform 7.5 and 7.7** (`cp-kafka`) | The transactional producer (`exactly_once: true`) needs a broker with transaction support (the Kafka 2.5+ protocol floor). Other broker versions and distributions are untested - validate during onboarding. |

Other sinks (Redis, NATS, HTTP, S3-compatible object storage, ClickHouse, Elasticsearch) are supported as documented on their per-sink pages; run your target versions through a disposable pipeline during onboarding.

## Topology: single owner per source

Run **one DeltaForge instance per source (per replication slot / binlog reader)**. DeltaForge does not yet provide a cluster-wide lock or lease that prevents two instances from being started against the same source; single ownership is enforced at the slot and producer level, not globally.

**Single-instance requirement.** For the supported single-instance deployment, run exactly **one** DeltaForge process against a given checkpoint/state store. Within one process, lifecycle operations (start, stop, delete, patch, resume) are serialized and a durable per-source-id claim rejects a second pipeline that reuses an active source id, so two pipelines can never share a source id and corrupt each other's checkpoints (checkpoints are keyed by source id). That claim is **not** a cross-process lock: two DeltaForge processes sharing the same state store are not protected against each other and are unsupported. Deploy DeltaForge as a single instance (a single replica; if orchestrated, `replicas: 1` with `strategy: Recreate`, not rolling), and do not point a second process at the same state store.

- **PostgreSQL**: the replication slot has durable, DeltaForge-recorded ownership. DeltaForge only drops or recreates a slot it can prove it owns, whose lineage matches, and that is inactive; a foreign, ambiguously owned, or active slot **fails closed** with remediation rather than being taken over. A slot created outside DeltaForge (no ownership record) fails closed on re-snapshot - drop it and let DeltaForge recreate it, or run `snapshot.mode = never`.
- **Kafka**: with `exactly_once: true`, a second producer using the same `transactional.id` fences the first. Fencing is a fatal error that stops the pipeline. This is a safety net, not a substitute for running a single instance.
- **MySQL**: each instance derives its replication `server_id` from the source `id`; give each source a unique `id` to avoid `server_id` collisions on the same MySQL server.
- **Failover**: for planned HA, use GTID (MySQL) and a slot-aware HA tool (for example Patroni `permanent_slots`) so the replication position survives a primary change. See [Failover Handling](failover.md).

## Deployment preflight

Before deploying a pipeline, validate its config against the live source:

```
deltaforge preflight <config-file-or-dir>      # human-readable report
deltaforge preflight <config-file-or-dir> --json   # machine-readable report
```

Preflight resolves the config's secrets and source DSN exactly as startup does, connects to the source, and runs the source checks plus local config validation. It exits non-zero (fail closed) if any check fails, so it is safe to gate a deploy on it in CI/CD.

- **PostgreSQL**: `wal_level = logical`; the publication exists and has tables; WAL-retention capacity vs estimated snapshot size. **Slot validation is mode-aware**: DeltaForge creates and owns the replication slot in every supported mode, so an absent slot on a fresh deployment is reported as informational, not a failure. An *existing* slot is checked under the same ownership rules as startup - a slot that is invalidated, WAL-lost, currently active (another consumer), or **foreign** (no matching ownership record for this server/database) fails closed.
- **MySQL**: `log_bin` on, `binlog_format = ROW`, `gtid_mode = ON`, the `RELOAD` privilege (when a snapshot will run), captured tables on InnoDB, binlog retention window.
- **Both**: connectivity; **source and sink credential references** are resolved (a missing/invalid secret or an inline+reference conflict fails here); commit-policy validity vs sink count; at least one sink configured; and **source ids are unique** across the supplied config(s).

Wildcard table patterns are reported as not validated per-table (server-level checks still run). Preflight validates **credentials and configuration**, not endpoints: **sink endpoint reachability is not probed** (a documented follow-up), and the slot-ownership check requires pointing preflight at the deployment's storage backend (the same `--storage-*` flags the server uses).

## Required privileges

### PostgreSQL

- A role with `LOGIN REPLICATION`.
- `GRANT CONNECT ON DATABASE <db>`.
- `GRANT USAGE ON SCHEMA <schema>` and `GRANT SELECT` on the tables to be captured (snapshot reads and schema introspection).
- `pg_hba.conf` must permit both regular and `replication` connections for the role/host.
- The **publication is not auto-created** - create it before starting the pipeline. The replication slot is created by DeltaForge.
- `REPLICA IDENTITY FULL` is recommended on captured tables for complete before-images.

### MySQL

- `REPLICATION REPLICA, REPLICATION CLIENT ON *.*` (always).
- `SELECT, SHOW VIEW` on the captured database(s) for reads and introspection.
- `RELOAD ON *.*` **only when an initial snapshot runs** (needed for the brief `FLUSH TABLES WITH READ LOCK` that anchors the snapshot). A CDC-only pipeline (`snapshot.mode = never`) does not need `RELOAD`.
- Server settings enforced fail-closed at snapshot preflight: `gtid_mode = ON`, `enforce_gtid_consistency = ON`, `binlog_format = ROW`, and captured tables on InnoDB. `binlog_row_image = FULL` is recommended.
- On managed MySQL (for example RDS/Aurora) where `RELOAD` / `FLUSH TABLES WITH READ LOCK` is unavailable, the snapshot **fails closed** with a typed error - there is no silent unsafe fallback. Grant `RELOAD`, or run `snapshot.mode = never` and perform the initial load out of band.

### Sinks

Sink credentials and auth modes are documented per sink (see [Sinks](sinks/README.md)). Where a sink is configured to auto-create its target object (for example ClickHouse tables or Elasticsearch indices/templates), the sink user needs the corresponding create privilege on the target system; otherwise disable auto-create and provision the target yourself.

## Supported credential modes

DeltaForge resolves credentials from typed secret references and never stores resolved secrets in serialized config. See [Secrets & Credentials](secrets.md).

- **Secret providers**: `env`, `file`, and `vault` (Vault KV v2). Kubernetes is not a separate provider - inject secrets as env (`secretKeyRef`) or as files (projected volume) and reference them with the `env` or `file` provider.
- **Vault**: static KV references are supported (the runner must be built with the `vault` feature; a Vault reference fails closed at startup otherwise). **Dynamic Vault database credentials (lease-driven reconnect) are not live in the current release** - the lease lifecycle exists but does not yet drive a DSN swap.
- **Rotation: treat credential rotation as restart-required.** Change the secret, then restart the affected pipeline or the runner for it to take effect. An opt-in live-reconnect path for file-backed *source* database credentials exists in the code (MySQL requires GTID mode), but it is **outside the supported deployment envelope** - validate it in a disposable environment before considering it. Environment-variable credentials are process-immutable and never rotate live; sink credentials are restart-required.

## Network and TLS

Plan the network on the basis that **DeltaForge's source database connections and its own HTTP endpoints are not encrypted or authenticated in this release**. Deploy accordingly.

- **Source DB connections are not encrypted.** Neither the PostgreSQL nor the MySQL source establishes TLS to the database in this build. Run DeltaForge on a trusted/private network segment with the source database, or place an encrypted tunnel (for example a service mesh sidecar, stunnel, or a cloud private link) between DeltaForge and the database. Do not run source traffic across an untrusted network.
- **REST API is unauthenticated and binds all interfaces by default.** The default `--api-addr` is `0.0.0.0:8080` (every interface), so an unrestricted deployment exposes the unauthenticated API on the network. Bind it to loopback (`--api-addr 127.0.0.1:8080`) where the client is local, or confine it with a firewall / Kubernetes NetworkPolicy; do not expose it publicly.
- **Metrics endpoint is unauthenticated** and defaults to `0.0.0.0:9000` (all interfaces). Bind it to loopback (`--metrics-addr 127.0.0.1:9000`) where the scraper is local, or confine it with a firewall/NetworkPolicy. See [Observability](observability.md#metrics-endpoint-address-and-exposure).
- **Sink TLS/auth is supported** where the sink provides it: Kafka (SASL + `SASL_SSL` via `client_conf`), Elasticsearch (`https://` with `tls.ca_file`, basic/API-key auth), HTTP (`https://` with header-based auth), NATS (TLS + credentials/token). Confirm Redis and ClickHouse TLS in your environment before relying on it.

## State and backup requirements

DeltaForge keeps all runtime state (checkpoints, schema registry, snapshot progress, DLQ/journal) in one storage backend. See [Storage](storage.md) and [Checkpoints](checkpoints.md).

- **Backends**: `sqlite` (default; single-instance production) and `postgres` (a shared storage backend, **beta** - not yet given the same crash/recovery validation as SQLite). The PostgreSQL backend lets multiple processes share one state store, but it does **not** make source processing highly available and does **not** provide ownership fencing between instances - the single-owner-per-source rule above still applies. Use with caution. `memory` is for testing only and is lost on restart.
- **Back up the state store regularly.** For SQLite the store is the `deltaforge.db` file (default under `./data/`), which holds both checkpoints and schema history; losing it means losing resume position and schema lineage. The store runs in **WAL mode**, so do not copy `deltaforge.db` on its own while DeltaForge is running - committed data may still be in the `-wal` file, and a bare file copy can be inconsistent. Use one of:
  - SQLite's online-backup API or `VACUUM INTO 'backup.db'` against the live database;
  - stop DeltaForge cleanly, checkpoint the WAL (`PRAGMA wal_checkpoint(TRUNCATE)`), then copy the file;
  - a storage-level consistent snapshot that captures the database together with its `-wal` and `-shm` files.

  For the PostgreSQL backend, back up that database with your normal PostgreSQL backup tooling (`pg_dump` or a consistent base backup).
- **Durability**: the SQLite store runs in WAL mode with `synchronous = NORMAL` and survives `SIGKILL` without a graceful shutdown; a checkpoint is persisted only after the sink acknowledges delivery and the commit policy is satisfied (the basis of at-least-once).
- Restoring from a backup rewinds to that backup's position; expect at-least-once re-delivery (duplicates) of everything after the backup point, which consumers dedup on the event `id`.

## Known limitations

These are documented in full on [Guarantees & Correctness](guarantees.md); the summary:

- **Delivery is at-least-once.** No sink is end-to-end exactly-once. Kafka `exactly_once: true` adds transactional atomic-batch delivery (a `read_committed` consumer never sees a partial batch) but is still at-least-once across a restart. Consumers must dedup on the event `id` for exactly-once end to end.
- **Snapshot to CDC has a bounded at-least-once overlap** - rows committed between the anchor and the snapshot export are delivered by both the snapshot and CDC. Current-state sinks converge; append-only sinks see duplicates.
- **No cross-table global ordering.** Per-primary-key ordering within a table is guaranteed; different tables may interleave.
- **No stateful stream processing** (no joins/aggregations/windowing). Do that downstream (Flink, ksqlDB, Kafka Streams).
- **Optional sinks (`required: false`) have no in-session retry.** A failed optional sink is re-delivered only on the next restart via source replay; a chronic optional-sink outage accumulates a growing replay gap.
- **S3 sink is at-least-once at file granularity** and requires an `AbortIncompleteMultipartUpload` bucket lifecycle policy in production to reclaim orphaned multiparts.
- **DLQ must be enabled** (`journal.enabled: true`) to isolate poison events; without it a single unprocessable event blocks the pipeline.
- **PostgreSQL `start_position` is not implemented** - a newly created slot always starts at the current WAL position.
- **Changing a pipeline's sink set via `PATCH` is disabled.** Adding or removing a sink changes the per-sink checkpoint keys, which is not crash-safe; a `PATCH` that alters the sink set is rejected. Patches that leave the sink set unchanged are allowed. To change sinks, delete and recreate the pipeline.
- **Deleting a pipeline is fail-closed.** Delete stops the source, cleans up its checkpoints, and only then releases the source-id claim; if checkpoint cleanup fails, the delete fails and the source id stays locked (safe: it blocks reuse rather than exposing stale checkpoints). Retry the delete once the store is reachable.
- Cross-primary position safety at failover depends on GTID (MySQL) and slot-aware HA (PostgreSQL); see [Failover Handling](failover.md).
- **MySQL cannot replay binlog rows across a DDL.** If a restart replays rows written before a DDL on the table, the source stops fail-closed (no event, no checkpoint advance) instead of decoding them against the newer schema. Safe recovery is a **re-snapshot** (restart once with snapshot mode `always`). Moving the source position past the DDL intentionally abandons all retained changes in between, for every captured table, and requires an operator assessment of that loss. Set `binlog_row_metadata=FULL` so column names are verified as well as count and types. See [Guarantees](guarantees.md#mysql-replay-across-a-schema-change).

## Out of scope

Not part of the supported deployment envelope (do not rely on these):

- Multi-instance HA for a single source (no cluster-wide ownership lock/lease yet).
- TLS directly to the source database (use a trusted network or an external tunnel).
- Authenticated REST API or metrics endpoint (use network controls).
- Live/dynamic credential rotation as a supported contract (treat rotation as restart-required; dynamic Vault DB credentials are not live).
- End-to-end exactly-once delivery, deterministic snapshot+replay rebuild, and any feature marked planned on the [Roadmap](roadmap.md).
