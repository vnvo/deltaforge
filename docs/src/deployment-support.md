# Supported Deployment Envelope

This page defines the **bounded support envelope for the current DeltaForge release**: the configurations DeltaForge is validated for, what operators must provide, and what is explicitly out of scope. Anything not listed here as supported should be treated as unvalidated - test it in a disposable environment first, and talk to us before relying on it.

The delivery and correctness guarantees referenced below are defined in [Guarantees & Correctness](guarantees.md). Read that page together with this one. The release gate that decides each item's status is the [Release Checklist](https://github.com/vnvo/deltaforge/blob/main/docs/ops/release-checklist.md).

## Release scope at a glance

### Demonstrated behavior

Shown by the release's tests and gates (see the Release Checklist for the evidence):

- PostgreSQL 17 and MySQL 8.4 sources: CDC, initial snapshots, restarts and failover handling, all failing closed when a position, identity or schema cannot be proven.
- MySQL rows decoded only with the schema version proven for their position, including across DDL when that version is provable; a MySQL activation window never admits a table its capture does not contain.
- At-least-once delivery with checkpoints persisted only after the commit policy is satisfied; required sinks hold the checkpoint on failure.
- One server per state store, enforced by the store gate; serialized pipeline lifecycle.
- The SQLite state store, including survival of `SIGKILL`.
- The Kafka sink against Confluent Platform 7.5 and 7.7 without TLS, and the other sinks as covered by their suites in the release-tier gate.

### Known defect in v0.1.0-rc.1: MySQL table-ID churn

A MySQL source keeps every binlog table map it receives and never releases it, including the table maps of tables its filters exclude. MySQL can give a table a new table ID each time it evicts the table's definition and later reopens it, for example when the tables receiving writes outnumber what the server's table caches (`table_definition_cache`, `table_open_cache`) hold. Under sustained churn, DeltaForge's memory grows until the process is OOM-killed. In testing, 40,000 tables receiving writes reached a 6 GiB container limit in 13 minutes.

- This can occur within the documented 40,000-table catalog size. Exposure depends on how often MySQL evicts and reopens table definitions, not on catalog size; exceeding a configured cache size does not by itself imply a particular churn rate.
- Table filters do not remove the exposure: table maps of excluded tables are retained too.
- The growth rate depends on the change rate, how often table IDs change, and the tables' column definitions.
- Raising `table_definition_cache` and `table_open_cache` above the number of tables receiving writes is an untested mitigation, not a supported remedy.
- Sign of exposure: MySQL's `Opened_table_definitions` keeps rising while DeltaForge's memory grows.
- Workloads that can cause sustained table-ID churn should not use this release candidate until a fix is released.

### Accepted limitations

Known boundaries with an operator control, detailed below and in [Known limitations](#known-limitations):

- At-least-once delivery: consumers dedup on the event `id`.
- No automatic restart: a pipeline that stops fail-closed stays stopped until an operator restarts it.
- No transport encryption to source databases, a PostgreSQL state store, or Kafka and Redis sinks, and Kafka authentication limited to SASL `PLAIN` sent in clear: accepted for this beta **only on trusted, isolated networks**. The REST API and metrics are unauthenticated ([Network and TLS](#network-and-tls)).
- Preflight does not cover every startup requirement ([Deployment preflight](#deployment-preflight)).
- MySQL replay across an unprovable DDL stops fail-closed.
- SQLite backup needs the WAL-safe procedure.
- Capacity guidance is a starting point, not a tested ceiling ([Capacity & Resource Envelope](capacity-envelope.md)).

### Retained-state compatibility

Recreating a pipeline with the same name and source id resumes from the state
retained for that source. Since this release that state is adopted only by a
spec compatible with the one that wrote it (Release Checklist C10): the first
start records a binding, and a later create or PATCH that differs in a
recorded field is refused before anything is claimed or written. See
[Restarts and recreating a pipeline](apireference.md#restarts-and-recreating-a-pipeline).

- **Upgrade step:** a source id with retained state from an earlier release
  is refused (`RETAINED_STATE_UNBOUND`) until it is adopted once with
  `deltaforge state-binding adopt`.
- **A pre-created PostgreSQL slot** (made by an operator before the first
  start) is retained state: that first start is refused until the slot is
  adopted or removed.
- **Selection changes refuse in both directions:** adding or removing a table
  pattern, sink or processor needs a new source id.
- **A refused `--config` pipeline stops server startup** with the refusal, as
  any start error of a configured pipeline does.

### Unqualified deployments

**Not suitable** for this release: any deployment that requires transport encryption between DeltaForge and its source databases, its state store, Kafka or Redis.

Not validated for this release. They may work, but they are outside the supported envelope until qualified; validate them in a disposable environment first:

- Source versions other than PostgreSQL 17 and MySQL 8.4 (MySQL 8.0.19 or later is accepted by the code; MariaDB is refused), and managed database services.
- MySQL catalogs, active sets and fleets larger than the MySQL qualification run (one server, 40,000 tables, one pipeline); more than a few pipelines per instance.
- The PostgreSQL state store (beta). Its qualification is deferred: the release's qualification runs use the SQLite state store and do not cover it.
- Live credential rotation and dynamic Vault database credentials.
- Sink versions other than those the release-tier gate runs.

## Build and freeze

- Pin your deployment to a specific release commit or tag rather than tracking `main`, so your environment does not drift.
- Toolchain: built with the Rust **stable** toolchain, edition **2024**. Single static binary; no JVM, no runtime GC.
- Report issues against the exact commit/tag so we can reproduce them.

## Supported versions

| Component | Validated version | Notes |
|-----------|------------------------|-------|
| PostgreSQL (source) | **17** | Logical replication (`wal_level = logical`). There is no hard server-version gate in code; other recent majors likely work but are not part of the validated envelope. Validate before relying on a different major. |
| MySQL (source) | **8.4** | Native binlog CDC. **Minimum MySQL 8.0.19**: the source refuses an older server, and MariaDB, before it reads anything (its schema capture uses `VALUES ROW`, added in 8.0.19). The minimum is what the source's statements need; only 8.4 is validated. **MariaDB is not supported.** |
| Kafka (sink) | Tested on **Confluent Platform 7.5 and 7.7** (`cp-kafka`) | The transactional producer (`exactly_once: true`) needs a broker with transaction support (the Kafka 2.5+ protocol floor). Other broker versions and distributions are untested - validate during onboarding. |

Other sinks (Redis, NATS, HTTP, S3-compatible object storage, ClickHouse, Elasticsearch) are supported as documented on their per-sink pages; run your target versions through a disposable pipeline during onboarding.

## Topology: single owner per source

Run **one DeltaForge instance per source (per replication slot / binlog reader)**. The store gate stops a second server on the same state store, but nothing stops two instances with *different* state stores from reading the same source; single ownership of a source is enforced at the slot and producer level, not globally.

**Single-instance requirement.** For the supported single-instance deployment, run exactly **one** DeltaForge process against a given checkpoint/state store. Within one process, lifecycle operations (start, stop, delete, patch, resume) are serialized and a durable per-source-id claim rejects a second pipeline that reuses an active source id, so two pipelines can never share a source id and corrupt each other's checkpoints (checkpoints are keyed by source id). Across processes, the [store gate](#store-gate) makes a second server against the same state store refuse to start. Deploy DeltaForge as a single instance (the Helm chart's StatefulSet with `replicaCount: 1`), and do not point a second process at the same state store.

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
- **MySQL**: `log_bin` on, `binlog_format = ROW`, `gtid_mode = ON`, the `RELOAD` privilege, captured tables on InnoDB, binlog retention window. The preflight command requires `RELOAD` and `gtid_mode = ON` for every pipeline, including a CDC-only one that does not need them at runtime.
- **Both**: connectivity; **source and sink credential references** are resolved (a missing/invalid secret or an inline+reference conflict fails here); commit-policy validity vs sink count; at least one sink configured; and **source ids are unique** across the supplied config(s).

Wildcard table patterns are reported as not validated per-table (server-level checks still run). Preflight validates **credentials and configuration**, not endpoints: **sink endpoint reachability is not probed** (a documented follow-up), and the slot-ownership check requires pointing preflight at the deployment's storage backend (the same `--storage-*` flags the server uses).

**Startup checks that preflight does not run.** The source checks these when it starts (and MySQL again on every reconnect) and refuses to run if one fails; preflight does not report them:

- MySQL version 8.0.19 or later, and not MariaDB.
- MySQL `log_replica_updates = ON` (or `log_slave_updates`), on every MySQL source, primary or replica.
- PostgreSQL publication registration (`deltaforge pg-publication register`; see [Required privileges](#postgresql)).

## Store gate

> **Breaking change on upgrade.** Earlier releases let several processes share one PostgreSQL-backed state store. That topology now refuses to start; give each process its own store. Upgrade by stopping the old version cleanly before starting the new one; if the old process crashed instead, release its gate with `deltaforge store-gate break` as described below.

The state store carries one durable **store gate**. A DeltaForge server acquires it at startup, before it reads any schema history, and holds it until it shuts down cleanly; `deltaforge schema-migrate --apply` holds it while it writes. Only one holder can exist, so a second server, or a migration apply while a server runs, refuses to start with an error that names the holder (role, owner id, host, pid, since) and the command to release it.

- **Clean shutdown** (`SIGTERM`/`SIGINT`) stops the API, stops every pipeline and waits for its tasks, then releases the gate. This also holds for a signal that arrives while the server is still starting: the signal handlers are installed before the gate is acquired, and startup stops at the next step. Give the process enough time to do this (the Helm chart's `terminationGracePeriodSeconds` is 30).
- **A crash keeps the gate held.** Nothing expires. After an OOM kill, a `SIGKILL` (including one sent after the grace period), or a host loss, the next start fails until an operator releases the gate. In Kubernetes this shows as the restarted container exiting with the store-gate error (CrashLoopBackOff). This is deliberate: DeltaForge will not guess that the previous holder is gone.
- **Recovery:**

  ```
  deltaforge store-gate status            # who holds it (add --json for scripts)
  deltaforge store-gate break --owner <owner-id>
  ```

  Before breaking, confirm that the recorded process (host and pid) is no longer running. `break` releases the gate only if that owner still holds it, so a stale or mistyped owner id changes nothing. Pass the same `--storage-backend`/`--storage-path`/`--storage-dsn` flags the server uses.
- **A migration's gate cannot be broken.** A schema migration that failed or crashed may have written part of its plan, and a server must never read partially migrated history, so `store-gate break` refuses it. Finish the migration instead (see below); the error message prints the exact command.

## Migrating pre-upgrade schema history

Schema history written by earlier releases is kept but not used automatically: a table's history is only trusted once it is tied to the source's verified lineage (PostgreSQL system identifier and database OID; MySQL server UUID). `deltaforge schema-migrate` adopts it into the lineage-scoped history for tables you list explicitly. It never deletes the old records.

1. Start the pipeline once on the new release so it records the source lineage, then stop the server. `deltaforge schema-migrate` needs the lineage hash of each source; the dry run prints the recorded one.
2. Write a mapping file that lists each table (no wildcards):

   ```yaml
   mappings:
     - tenant: acme
       source_id: orders-pg
       lineage_hash: <recorded lineage hash>
       tables:
         - { db: public, table: orders }
         - { db: public, table: order_items }
   ```

3. Dry run (the default; read-only, safe while the server runs):

   ```
   deltaforge schema-migrate --mapping mapping.yaml [--tenant T] [--source S] [--json]
   ```

   It classifies every table as `migrate`, `already migrated`, `ambiguous` or `rejected` (with the reason: lineage missing or different from the asserted hash, empty or corrupt history, a version that conflicts with existing history, or a marker from a different migration), and prints a **proof** digest over everything it would adopt.
4. With the server stopped, apply exactly that plan:

   ```
   deltaforge schema-migrate --mapping mapping.yaml --apply --expect-proof <proof>
   ```

   The apply recomputes the plan under the store gate and refuses if the proof differs, so anything that changed since the review stops it (nothing is written and the gate is released). Tables already migrated by a *different* reviewed proof are reported as rejected, with that migration's details, rather than counted as done.
5. **If the apply fails or is interrupted**, the store gate stays held (role `migration`, recording the proof) and the server cannot start, because part of the plan may already be written. After confirming the failed process is no longer running and fixing the cause, finish the same plan:

   ```
   deltaforge schema-migrate --mapping mapping.yaml --apply --expect-proof <proof> --resume-owner <owner-id>
   ```

   The owner id is in the error message and in `deltaforge store-gate status`. The resume takes the gate over in one step (it is never unlocked in between), accepts only the recorded owner and the same proof, treats the work already done as progress, and releases the gate once the migration completes.

## Required privileges

### PostgreSQL

- A role with `LOGIN REPLICATION`.
- `GRANT CONNECT ON DATABASE <db>`.
- `GRANT USAGE ON SCHEMA <schema>` and `GRANT SELECT` on the tables to be captured (snapshot reads and schema introspection).
- `pg_hba.conf` must permit both regular and `replication` connections for the role/host.
- The **publication is not auto-created** - create it before starting the pipeline. The replication slot is created by DeltaForge.
- **The publication must be registered** with `deltaforge pg-publication register`, run by a superuser. Registration installs event triggers that refuse, in that database, every `ALTER PUBLICATION` and `DROP PUBLICATION` and any drop of a registered publication's tables, so changing a publication becomes a maintenance procedure with every source stopped. The source refuses to start (and to reconnect) on a publication that is not registered, has changed since registration, or whose triggers were disabled. See [PostgreSQL source](sources/postgres.md).
- `REPLICA IDENTITY FULL` is **required** on every captured table with a TOAST-capable column when a sink needs complete rows (Avro on any sink, S3 Parquet, ClickHouse, Elasticsearch): the source refuses to start otherwise. For other pipelines it is recommended, for complete before-images.

### MySQL

- `REPLICATION SLAVE, REPLICATION CLIENT ON *.*` (always).
- `BACKUP_ADMIN ON *.*` and `SELECT ON performance_schema.log_status` for a first start without a checkpoint and without a snapshot: the capture boundary (binlog position and GTID set) is read there in one consistent query. Without them that start fails closed. Validated on MySQL 8.4; managed services are not yet qualified for these grants.
- `SELECT, SHOW VIEW` on the captured database(s) for reads and introspection.
- `RELOAD ON *.*` **only when an initial snapshot runs** (needed for the brief `FLUSH TABLES WITH READ LOCK` that anchors the snapshot). A CDC-only pipeline (`snapshot.mode = never`) does not need `RELOAD`.
- Server settings enforced fail-closed at snapshot preflight: `gtid_mode = ON`, `binlog_format = ROW`, and captured tables on InnoDB (MySQL itself requires `enforce_gtid_consistency = ON` before `gtid_mode = ON`). `binlog_row_image = FULL` is recommended (a warning otherwise).
- Server settings enforced fail-closed at every start and reconnect: MySQL 8.0.19 or later (not MariaDB) and `log_replica_updates = ON`.
- `binlog_row_metadata = FULL` is recommended: under `MINIMAL`, more DDL situations stop the source fail-closed (see [Known limitations](#known-limitations)).
- GTID mode is required for snapshots, failover and live credential rotation; CDC alone also runs with file and position coordinates.
- On managed MySQL (for example RDS/Aurora) where `RELOAD` / `FLUSH TABLES WITH READ LOCK` is unavailable, the snapshot **fails closed** with a typed error - there is no silent unsafe fallback. Grant `RELOAD`, or run `snapshot.mode = never` and perform the initial load out of band.

### Sinks

Sink credentials and auth modes are documented per sink (see [Sinks](sinks/README.md)). Where a sink is configured to auto-create its target object (for example ClickHouse tables or Elasticsearch indices/templates), the sink user needs the corresponding create privilege on the target system; otherwise disable auto-create and provision the target yourself.

## Supported credential modes

DeltaForge resolves credentials from typed secret references and never stores resolved secrets in serialized config. See [Secrets & Credentials](secrets.md).

- **Secret providers**: `env`, `file`, and `vault` (Vault KV v2). Kubernetes is not a separate provider - inject secrets as env (`secretKeyRef`) or as files (projected volume) and reference them with the `env` or `file` provider.
- **Vault**: static KV references are supported (the runner must be built with the `vault` feature; a Vault reference fails closed at startup otherwise). The repository's `Dockerfile` builds without it. **Dynamic Vault database credentials (lease-driven reconnect) are not live in the current release** - the lease lifecycle exists but does not yet drive a DSN swap.
- **Rotation: treat credential rotation as restart-required.** Change the secret, then restart the affected pipeline or the runner for it to take effect. An opt-in live-reconnect path for file-backed *source* database credentials exists in the code (MySQL requires GTID mode), but it is **outside the supported deployment envelope** - validate it in a disposable environment before considering it. Environment-variable credentials are process-immutable and never rotate live; sink credentials are restart-required.

## Network and TLS

Plan the network on the basis that **DeltaForge's source database connections, its PostgreSQL state-store connection, its Kafka and Redis sink connections and its own HTTP endpoints are not encrypted, and its HTTP endpoints are not authenticated, in this release**. This is accepted for this beta only on **trusted, isolated networks**: do not deploy DeltaForge where transport encryption is required.

> **Warning: security settings that are accepted but not applied.** These settings pass configuration validation but change nothing, so do not rely on them:
>
> - **ClickHouse `tls.ca_file`** is ignored: a custom CA is never loaded, so a server signed by a private CA fails verification.
> - **ClickHouse and Elasticsearch `tls.enabled`** are ignored: the URL scheme alone decides. `tls.enabled: true` with an `http://` URL connects **in plaintext without an error**; `tls.enabled: false` with `https://` still uses TLS.
> - **TLS parameters in a source DSN** (for example PostgreSQL `sslmode`, MySQL SSL options) do not enable TLS in this build; whether a given parameter is refused or silently ignored has not been verified, so assume plaintext.

- **Source DB connections are not encrypted.** Neither the PostgreSQL nor the MySQL source establishes TLS to the database in this build (the MySQL client is built without TLS, and every PostgreSQL connection, the replication stream included, is opened without TLS). Run DeltaForge on a trusted/private network segment with the source database, or place an encrypted tunnel (for example a service mesh sidecar, stunnel, or a cloud private link) between DeltaForge and the database. Do not run source traffic across an untrusted network. Do not rely on TLS parameters in a DSN: they do not enable TLS in this build.
- **The PostgreSQL state store connection is not encrypted** either; keep that database on the same trusted network.
- **REST API is unauthenticated and binds all interfaces by default.** The default `--api-addr` is `0.0.0.0:8080` (every interface), so an unrestricted deployment exposes the unauthenticated API on the network. Bind it to loopback (`--api-addr 127.0.0.1:8080`) where the client is local, or confine it with a firewall / Kubernetes NetworkPolicy; do not expose it publicly.
- **Metrics endpoint is unauthenticated** and defaults to `0.0.0.0:9000` (all interfaces). Bind it to loopback (`--metrics-addr 127.0.0.1:9000`) where the scraper is local, or confine it with a firewall/NetworkPolicy. **`/metrics` is also served on the API listener**, so restricting `--metrics-addr` alone does not hide it. See [Observability](observability.md#metrics-endpoint-address-and-exposure).
- **Recovery admin listener**: `deltaforge recover` uses a separate listener (`--admin-addr`, default `127.0.0.1:9091`, loopback only) that starts only when `--admin-token-file` is given and requires that bearer token.
- **Sink TLS in this build:**
  - **Kafka: no TLS, and SASL `PLAIN` only.** The Kafka client is built without OpenSSL and without Kerberos, so `security.protocol` `SSL` and `SASL_SSL` are unavailable, and so are the SASL mechanisms `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER` (including OIDC) and `GSSAPI` (Kerberos). The only options are `PLAINTEXT` and `SASL_PLAINTEXT` with `PLAIN`, which sends the username and password in clear. Use them only on a trusted, isolated network.
  - **Redis: no TLS.** `rediss://` URLs fail.
  - **Elasticsearch**: `https://`, with `tls.ca_file` and basic or API-key authentication. `tls.enabled` is ignored (see the warning above).
  - **ClickHouse**: `https://` with the system's trusted roots; `tls.insecure_skip_verify` applies. `tls.ca_file` and `tls.enabled` are ignored (see the warning above).
  - **HTTP and S3**: `https://` with the system's trusted roots.
  - **NATS**: credentials-file, user and password, or token authentication; no CA or client-certificate option is exposed, and TLS to NATS is not validated in this release.

## State and backup requirements

DeltaForge keeps all runtime state (checkpoints, schema registry, snapshot progress, DLQ/journal) in one storage backend. See [Storage](storage.md) and [Checkpoints](checkpoints.md).

- **Backends**: `sqlite` (default; single-instance production) and `postgres` (a shared storage backend, **beta** - not yet given the same crash/recovery validation as SQLite). An unrecognized `--storage-backend` value falls back to `sqlite`, so check the flag's spelling. One state store serves exactly one DeltaForge server at a time (enforced by the [store gate](#store-gate)); the PostgreSQL backend does **not** make source processing highly available. Use with caution. `memory` is for testing only and is lost on restart.
- **Back up the state store regularly.** For SQLite the store is the `deltaforge.db` file (default under `./data/`), which holds both checkpoints and schema history; losing it means losing resume position and schema lineage. The store runs in **WAL mode**, so do not copy `deltaforge.db` on its own while DeltaForge is running - committed data may still be in the `-wal` file, and a bare file copy can be inconsistent. Use one of:
  - SQLite's online-backup API or `VACUUM INTO 'backup.db'` against the live database;
  - stop DeltaForge cleanly, checkpoint the WAL (`PRAGMA wal_checkpoint(TRUNCATE)`), then copy the file;
  - a storage-level consistent snapshot that captures the database together with its `-wal` and `-shm` files.

  For the PostgreSQL backend, back up that database with your normal PostgreSQL backup tooling (`pg_dump` or a consistent base backup).
- **Durability**: the SQLite store runs in WAL mode with `synchronous = NORMAL` and survives `SIGKILL` without a graceful shutdown; a checkpoint is persisted only after the sink acknowledges delivery and the commit policy is satisfied (the basis of at-least-once).
- Restoring from a backup rewinds to that backup's position; expect at-least-once re-delivery (duplicates) of everything after the backup point, which consumers dedup on the event `id`.

## Known limitations

These are documented in full on [Guarantees & Correctness](guarantees.md); the summary:

- **v0.1.0-rc.1: unbounded memory growth under MySQL table-ID churn.** Workloads that can cause sustained table-ID churn should not use this release candidate; see [Known defect in v0.1.0-rc.1](#known-defect-in-v010-rc1-mysql-table-id-churn).
- **Delivery is at-least-once.** No sink is end-to-end exactly-once. Kafka `exactly_once: true` adds transactional atomic-batch delivery (a `read_committed` consumer never sees a partial batch) but is still at-least-once across a restart. Consumers must dedup on the event `id` for exactly-once end to end.
- **Snapshot to CDC has a bounded at-least-once overlap** - rows committed between the anchor and the snapshot export are delivered by both the snapshot and CDC. Current-state sinks converge; append-only sinks see duplicates.
- **No cross-table global ordering.** Per-primary-key ordering within a table is guaranteed; different tables may interleave.
- **No stateful stream processing** (no joins/aggregations/windowing). Do that downstream (Flink, ksqlDB, Kafka Streams).
- **Optional sinks (`required: false`) have no in-session retry.** A failed optional sink is re-delivered only on the next restart via source replay; a chronic optional-sink outage accumulates a growing replay gap.
- **S3 sink is at-least-once at file granularity** and requires an `AbortIncompleteMultipartUpload` bucket lifecycle policy in production to reclaim orphaned multiparts.
- **DLQ must be enabled** (`journal.enabled: true`) to isolate poison events; without it a single unprocessable event blocks the pipeline.
- **PostgreSQL `start_position` is not implemented** - a newly created slot always starts at its consistent point (the current WAL position when it is created).
- **Changing a pipeline's sink set via `PATCH` is disabled.** Adding or removing a sink changes the per-sink checkpoint keys, which is not crash-safe; a `PATCH` that alters the sink set is rejected. Patches that leave the sink set unchanged are allowed. To change sinks, delete and recreate the pipeline.
- **API-created pipelines are not restored on restart.** Recreating the same name and source id resumes from the retained checkpoints (no fresh start, no snapshot, no backfill of newly selected tables), and only a spec compatible with the recorded binding may do so (see [Retained-state compatibility](#retained-state-compatibility)). To start fresh, use a new source id. See [Restarts and recreating a pipeline](apireference.md#restarts-and-recreating-a-pipeline).
- **Endpoint parameters must be classified.** HTTP, Elasticsearch, NATS and S3 endpoint query parameters, HTTP headers other than the built-in ones, and Kafka `client_conf` keys outside the built-in lists are rejected; so is a non-credential header or key supplied through `secret_refs`.
- **Deleting a pipeline is fail-closed.** Delete stops the source, cleans up its checkpoints, and only then releases the source-id claim; if checkpoint cleanup fails, the delete fails and the source id stays locked (safe: it blocks reuse rather than exposing stale checkpoints). Retry the delete once the store is reachable. Delete removes only the replay incarnation, the per-sink checkpoints, the destination ownership records and the claim; retained identity, lineage, continuity and snapshot records may remain, so Delete is not a fresh start. To start fresh, use a new source id (see [Delete Pipeline](apireference.md#delete-pipeline)).
- Cross-primary position safety at failover depends on GTID (MySQL) and slot-aware HA (PostgreSQL); see [Failover Handling](failover.md).
- **MySQL replays binlog rows across a DDL only when the schema in effect can be proven.** Rows are decoded with the version proven for their position. When none can be proven (retained rows older than a DDL the source never observed, a statement that cannot be attributed to tables such as a versioned-comment DDL, or two DDLs of one table before the source reads the first), the source stops fail-closed (no event, no checkpoint advance). The immediate remediation is a **re-snapshot** (restart once with snapshot mode `always`); after an unattributable statement, the table's next rows (once its definition is captured and proven unchanged since them) or a later proven DDL of the table also restore proof. Moving the source position past the DDL intentionally abandons all retained changes in between, for every captured table, and requires an operator assessment of that loss. Set `binlog_row_metadata=FULL` to remove most of these stops. See [Guarantees](guarantees.md#mysql-replay-across-a-schema-change).

## Out of scope

Not part of the supported deployment envelope (do not rely on these):

- Multi-instance HA for a single source (no cluster-wide ownership lock/lease yet).
- TLS directly to the source database or a PostgreSQL state store (use a trusted network or an external tunnel).
- TLS to Kafka or Redis sinks, and Kafka SASL mechanisms other than `PLAIN`.
- Any deployment that requires transport encryption.
- Authenticated REST API or metrics endpoint (use network controls).
- Live/dynamic credential rotation as a supported contract (treat rotation as restart-required; dynamic Vault DB credentials are not live).
- End-to-end exactly-once delivery, deterministic snapshot+replay rebuild, and any feature marked planned on the [Roadmap](roadmap.md).
