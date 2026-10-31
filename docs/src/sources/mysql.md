<p align="center">
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/mysql/mysql-original.svg" alt="MySQL" width="80" height="80">
</p>

# MySQL source

DeltaForge tails the MySQL binlog to capture row-level changes with automatic checkpointing and schema tracking.

## Prerequisites

### MySQL Server Configuration

Ensure your MySQL server has binary logging enabled with row-based format:

```sql
-- Required server settings (my.cnf or SET GLOBAL)
log_bin = ON
binlog_format = ROW
gtid_mode = ON                    -- required when the initial snapshot runs
enforce_gtid_consistency = ON     -- required with gtid_mode = ON
log_replica_updates = ON          -- required (the default); see below
binlog_row_image = FULL  -- Recommended for complete before-images
binlog_row_metadata = FULL  -- Recommended: decodes rows across unobserved schema changes
```

**`log_replica_updates = ON` is required on every source, primary or replica** (`log_slave_updates` before MySQL 8.0.26; the default since 8.0). With it OFF, transactions the server applies through replication - from a channel configured now or at any later time - change its tables and advance its executed GTID set without reaching the binlog DeltaForge reads, so changes would be missed and a schema change could go unseen. DeltaForge refuses such a server before it writes anything durable, reads any table or runs a snapshot, and checks again each time a binlog stream opens (startup, every reconnect, credential rotation) before reading from it, and before a failover candidate is reconciled. The setting is read-only: changing it requires a server restart, which ends the stream, so the reconnect afterwards is checked. Changing it on a running server is otherwise not possible and not supported. The check fails closed when the setting cannot be read.

If `binlog_row_image` is not `FULL`, DeltaForge will warn at startup and before-images on UPDATE/DELETE events may be incomplete. For `binlog_row_metadata`, see [Schema Tracking](#schema-tracking).

**Initial-snapshot requirements.** When a snapshot runs (`snapshot.mode` = `initial` or `always`), DeltaForge fails closed at preflight unless: `gtid_mode = ON`, `binlog_format = ROW`, every snapshotted table uses the **InnoDB** storage engine, and the user holds the global **RELOAD** privilege (see [Snapshot](#snapshot-initial-load)). These are checked only when a snapshot actually runs; a CDC-only pipeline (`snapshot.mode = never`) is never rejected for them.

### User Privileges

Create a dedicated replication user with the required grants:

```sql
-- Create the user with the server's default authentication plugin
-- (caching_sha2_password on MySQL 8.x)
CREATE USER 'deltaforge'@'%' IDENTIFIED BY 'your_password';

-- Replication privileges (required)
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'deltaforge'@'%';

-- Global RELOAD (required only when the initial snapshot runs; used for the
-- brief FLUSH TABLES WITH READ LOCK that brackets the consistent anchor)
GRANT RELOAD ON *.* TO 'deltaforge'@'%';

-- Schema introspection (required for table discovery)
GRANT SELECT, SHOW VIEW ON your_database.* TO 'deltaforge'@'%';

FLUSH PRIVILEGES;
```

For capturing all databases, grant `SELECT` on `*.*` instead. `RELOAD` is only required if the pipeline will run an initial snapshot; a CDC-only pipeline (`snapshot.mode = never`) does not need it.

## Configuration

Set `spec.source.type` to `mysql` and provide a config object:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | Yes | Unique identifier used for checkpoints, server_id derivation, and metrics |
| `dsn` | string | Yes | MySQL connection string with replication privileges |
| `tables` | array | Yes | Table patterns to capture. The key is required, but an empty list `[]` captures all user tables. |
| `proofs` | object | No | Bounds of the schema proofs: see [Schema proof bounds](#schema-proof-bounds). |

### Table Patterns

The `tables` field takes table patterns; the initial snapshot and CDC select exactly the same tables:

```yaml
tables:
  - shop.orders          # exact match: database "shop", table "orders"
  - shop.order_%         # prefix: tables starting with "order_" in "shop" (same as shop.order_*)
  - analytics.*          # wildcard: all tables in "analytics" database
  - %.audit_log          # cross-database: "audit_log" table in any database
  # use an empty list [] to capture all user tables (excludes system schemas)
```

A wildcard (`*` or `%`) only counts as the last character of a part (a prefix match); `_` and any other character are literal. A pattern without a qualifier matches that table name in every database/schema. The initial snapshot copies exactly the tables these patterns capture during CDC.

System schemas (`mysql`, `information_schema`, `performance_schema`, `sys`) are always excluded.

### Example

```yaml
source:
  type: mysql
  config:
    id: orders-mysql
    dsn: ${MYSQL_DSN}
    tables:
      - shop.orders
      - shop.order_items
```

### Schema proof bounds

`proofs` bounds the work that proves the schema version of a table's first
rows (see [Rows are decoded with the definition in effect when they were
written](#rows-are-decoded-with-the-definition-in-effect-when-they-were-written)).
Every field is optional. A value outside its range is refused when the
configuration is loaded:

| Field | Default | Range | Description |
|-------|---------|-------|-------------|
| `activation_window_ms` | `50` | 1 to 1,000 | How long after its first event was received an activation window reads ahead to batch the first rows of newly active tables. The window holds the stream's dispatch meanwhile. |
| `activation_window_events` | `10000` | 1 to 100,000 | At most this many events buffered by a window. |
| `activation_window_bytes` | `16777216` | 65,536 to 268,435,456 | At most this many bytes of events buffered by a window. |
| `capture_lock_wait_secs` | `30` | 1 to 300 | How long a window's capture waits for each table's metadata lock, on each of its three attempts. |
| `max_seek_ms` | `10000` | 100 to 600,000 | A proof scan with no known start offset fails closed when the server sends no transaction within this bound. |
| `control_connections` | `4` | 2 to 16 | The source's control-plane pool: at most this many SQL connections at once for schema captures, catalog reads, position queries and health checks. |

```yaml
source:
  type: mysql
  config:
    id: orders-mysql
    dsn: ${MYSQL_DSN}
    tables: ["shop.*"]
    proofs:
      activation_window_ms: 50
      max_seek_ms: 10000
```

### Activation histories

A table's schema version at its rows is chosen from its activation
timeline: the records of its own stream and the barriers that apply to it
(its lineage's and its database's), kept in the state store.

- **Cached and bounded:** a source caches at most 4,096 tables' timelines
  and 256 MiB of them (records, barrier streams once each, and the schemas
  it read), whichever comes first. The bytes cover resident memory.
  Admission evicts the least recently used entries first; an evicted table
  is read again exactly from the store.
- **Read in batches:** a table the cache does not hold is read with the
  other uncached tables the stream is about to reach (the events an
  activation window read ahead or holds, or a short read-ahead within the
  window's bounds), at most 256 tables per statement, each barrier stream
  once. Known tables returning after eviction cost a few store calls per
  batch, not per table.
- **Barriers:** a barrier invalidates only the cached timelines and
  selections of the tables it applies to: a database barrier, that
  database's; a lineage barrier, every table's.
- **The per-timeline limit:** a timeline is never loaded whole beyond
  100,000 records or 64 MiB (stored bytes), its barrier streams included.
  A table over the limit stops the source with a `mysql_timeline_too_large`
  incident before any of its rows is dispatched; the checkpoint stays
  before them. It needs an operator: the timeline does not shrink by
  itself. Before upgrading, this read-only query on the state store
  (PostgreSQL or SQLite) lists the activation and barrier streams that are
  large, as records and approximate bytes:

  ```sql
  SELECT ns, key, COUNT(*) AS records,
         SUM(LENGTH(val) + COALESCE(LENGTH(capture_id), 0)
             + COALESCE(LENGTH(content_hash), 0) + 32) AS bytes
  FROM df_log
  WHERE ns IN ('schemas.v1.activation', 'schemas.v1.activation.barrier')
  GROUP BY ns, key
  HAVING COUNT(*) > 10000 OR SUM(LENGTH(val)) > 8388608
  ORDER BY bytes DESC;
  ```

  A table's timeline is its activation stream (`key` is the table's key)
  plus its lineage's and its database's barrier streams (keys that prefix
  it).

### Control-plane connections

Every SQL connection a MySQL source opens besides its binlog stream and its
snapshot copy (schema captures, catalog reads, position and identity
queries, health checks) comes from the source's pool of
`proofs.control_connections`, within one budget for the whole process,
`--max-mysql-control-connections` (default `32`, of which `4` are reserved
for cancelling captures):

- A connection holds its permits from before it opens until it has closed.
- A returned connection is kept as the source's one idle connection for at
  most 10 s; any other closes. When the process budget is full, every idle
  connection closes at once to make room.
- Waits are first come, first served. A capture waits at most its
  `capture_lock_wait_secs`; a position query or a health check at most 5 s,
  one health check at a time per source; a catalog read at most 30 s.
- An activation window's, a failover drift check's or a DDL's forward
  proof capture that gets no connection within its wait stops the source
  with a `mysql_control_plane_saturated` incident and is retried by the
  restart: without it, rows could not be decoded, drift decided, or the
  rows after the DDL proven. A forward proof stops before its DDL is
  emitted or checkpointed; the restart replays the DDL and proves it.
- A snapshot baseline that gets no connection keeps its existing safe
  deferral, as for any failed capture: it only proves early what the
  table's lazy proof proves at its next rows, so the table is proven there
  before any of those rows is delivered. Each deferral is logged and
  counted in `deltaforge_mysql_control_saturation_deferrals_total{kind}`
  (`kind="baseline"`); it never raises an incident.
- Cancelling a capture never waits for a pool: it draws on the reserve.
- After a credential rotation or a failover, connections opened before it
  are never reused: the idle one closes at once, the others when returned.

`deltaforge_mysql_connections{source, kind}` reports a source's open
connections by kind (`replication`, `control`, `cancellation`,
`snapshot_copy`). `deltaforge_mysql_control_permit_wait_seconds{class}`,
`deltaforge_mysql_control_permit_timeouts_total{class, scope}` and
`deltaforge_mysql_control_idle_evictions_total` report the waits.

### A DDL's forward proof

When the stream reads a DDL on a tracked table, the source proves the
table's new shape (a capture of it and a scan of the binlog since the DDL)
before the DDL event is emitted. A proof that fails for a transient reason
is retried in place, with the stream paused at the DDL:

- **Retried:** a metadata-lock wait timeout or deadlock (another session
  holds or queues the table's lock), busy capture slots, and connection
  failures (refused, reset, timed out, closed, server shutting down).
- **Bounds:** at most 4 attempts within 120 s from the first, with 1, 4 and
  16 s between them. A capture starts only with at least 1 s left, and
  waits for the lock at most 30 s or the time left. Each attempt captures
  and scans afresh; a table proven in one attempt is committed then and not
  captured again.
- **Exhausted:** the source stops before the DDL is emitted or
  checkpointed, with a `mysql_forward_proof_unavailable` incident. A
  restart replays the DDL and proves it again; tables proven earlier are
  recognized by their recorded binding. While the proof retries, the same
  incident is open as `auto_retry`; it is resolved when the proof
  succeeds.
- **Not retried:** an authentication or authorization failure stops the
  source the same way at once (operator action). A connection to another
  server stops it as a lineage error, and a saturated control plane as
  `mysql_control_plane_saturated`, as before. A refusal on the evidence (a
  DDL on the table in the scanned interval, a purged binlog, a scan bound)
  leaves the DDL pending as before; the table's next rows are then proven
  by its lazy proof, which can still succeed.
- **Deadline:** no capture, scan, or store batch of the proof starts after
  its deadline. A batch already started finishes (including the
  resolution of an unacknowledged commit), so the proof can end shortly
  after the deadline. A capture or scan cut at the deadline is ended on
  the server.

`deltaforge_mysql_forward_proof_retries_total{class}` counts retried
failures (`lock_wait_timeout`, `deadlock`, `capture_slots_busy`,
`transport`, `read`, `not_reached`, `deadline`), and
`deltaforge_mysql_forward_proof_outcomes_total{outcome}` the ends of proofs
that retried (`recovered`, `ended_deferred`, `exhausted`, `cancelled`,
`ended_by_terminal`).

## Resume Behavior

DeltaForge automatically checkpoints progress and resumes from the last position on restart. The resume strategy follows this priority:

1. **GTID**: Preferred if the MySQL server has GTID enabled. Provides the most reliable resume across binlog rotations and failovers.
2. **File:position**: Used when GTID is not available. Resumes from the exact binlog file and byte offset.
3. **Binlog tail**: On first run with no checkpoint, starts from the current end of the binlog (no historical replay).

Checkpoints are stored using the `id` field as the key.

### GTID sets are exact

A GTID checkpoint holds exactly the transactions DeltaForge has seen or resumed past, gaps included. Gaps in a server's executed set are normal:

- **Group Replication and InnoDB Cluster** members assign GTIDs of the group's UUID from their own blocks (`group_replication_gtid_assignment_block_size`, 1,000,000 by default). A group whose members both write has an executed set such as `group:1-3:1000001`, and the gap fills as the first member commits more.
- **An explicitly assigned GTID** (`SET gtid_next`) leaves the numbers below it free for later transactions.

A checkpoint that claimed such a gap would make the server skip the transactions that fill it on resume. Earlier versions stored `group:1-3:1000001` as `group:1-1000001`; on a group, every row the first member committed after a restart was then lost. Such checkpoints are refused (see [MySQL GTID state from earlier versions](../checkpoints.md#mysql-gtid-state-from-earlier-versions)).

## Snapshot (Initial Load)

DeltaForge can perform a consistent initial snapshot of existing table data before
starting binlog streaming. This is the recommended approach for migrating existing
tables without manual backfills.

### How it works

DeltaForge establishes a single consistent anchor under a **brief** global read
lock. It holds `FLUSH TABLES WITH READ LOCK` on a dedicated connection only while
it opens a bounded pool of `REPEATABLE READ` `START TRANSACTION WITH CONSISTENT
SNAPSHOT` workers and captures the binlog position + GTID set, then releases the
lock. Because no transaction can commit while the lock is held, every worker
shares one consistent view that matches the captured position exactly - so a row
committed during snapshot setup is never lost from both the snapshot and the CDC
stream. Workers then read in parallel (bounded by `max_parallel_tables`), each
reading its tables under its single snapshot and committing once.

The lock window is bounded by `snapshot.lock_timeout_secs` (default 10s); if the
lock cannot be acquired in time, the snapshot fails closed rather than stalling
writes. The lock is always released - the dedicated lock connection is dropped on
any error, panic, or timeout, which releases it server-side.

> **Correctness note.** This closes a real initial-snapshot data-loss window in
> earlier versions, where per-worker snapshots were opened independently and the
> position was captured afterward, so a change committed during snapshot setup
> could be absent from both the snapshot and the CDC stream.

**Requirements (fail closed at preflight when a snapshot runs):** `gtid_mode =
ON`, `binlog_format = ROW`, all snapshotted tables **InnoDB**, and the global
**RELOAD** privilege.

**Managed MySQL (RDS/Aurora and similar).** Where `FLUSH TABLES WITH READ LOCK`
is unavailable (RELOAD restricted), the snapshot **fails closed** with a typed
permission error - DeltaForge does **not** silently fall back to an unsafe
anchor. Remediation:

- grant the global `RELOAD` privilege if your provider allows it; or
- run without an initial load by setting `snapshot.mode = never` to stream only
  changes from the current position (this is **not** a complete initial load); or
- perform the initial load out of band (e.g. a provider snapshot/dump) and start
  DeltaForge in `never` mode.

### Additional privileges
```sql
-- Required for snapshot: SELECT (introspection + reads) and global RELOAD.
GRANT SELECT ON your_database.* TO 'deltaforge'@'%';
GRANT RELOAD ON *.* TO 'deltaforge'@'%';
```

### Configuration
```yaml
source:
  type: mysql
  config:
    id: orders-mysql
    dsn: ${MYSQL_DSN}
    tables:
      - shop.orders
    snapshot:
      mode: initial           # initial | always | never (default: never)
      max_parallel_tables: 8  # tables snapshotted concurrently (also caps workers under the lock)
      chunk_size: 10000       # rows per chunk for integer-PK tables
      lock_timeout_secs: 10   # bound on the brief FLUSH TABLES WITH READ LOCK setup window
```

| Field | Default | Description |
|-------|---------|-------------|
| `mode` | `never` | `initial`: run once if no checkpoint exists; `always`: re-snapshot on every restart; `never`: skip |
| `max_parallel_tables` | `8` | Tables snapshotted concurrently; also bounds the worker connections opened under the read lock |
| `lock_timeout_secs` | `10` | Upper bound on acquiring the consistent-anchor lock; the snapshot fails closed if exceeded |
| `chunk_size` | `10000` | Rows per range chunk (integer single-column PK tables only; others do a full scan) |
| `discovery_page_size` | `1000` | Tables read per catalog query during discovery; also the most plan entries held in memory |
| `max_snapshot_connections` | `max_parallel_tables x max_parallel_chunks + 2` | This source's share of the process-wide snapshot connection cap (`--max-snapshot-connections`, default 64) |
| `max_anchor_age_secs` | `86400` | How long a generation may hold its anchor; blocks at the limit |
| `max_plan_bytes` / `max_plan_items` | 256 MiB / `1000000` | Bounds of the durable plan; blocks before sealing at either limit |

### Snapshot events

Snapshot rows are emitted as `Op::Read` events (Debezium `op: "r"`), distinguishable
from live CDC `Op::Create` events. The binlog position captured at snapshot time becomes
the CDC resume point, so no rows are missed. A snapshot copied again after a
restart delivers its rows again (at-least-once).

### Resume after interruption

An interrupted snapshot is **copied again in full** on the next restart, as a
new [snapshot generation](../snapshots.md#generations) with a new anchor; it is
never resumed table by table. Losing any worker's consistent-snapshot connection
loses the generation's read view and replaces it the same way.

A snapshot counts as complete only once its terminal barrier is committed by the
sinks the commit policy requires; a restart after that completes it without
copying. Completion, lagging sinks, bounds and blocking are described in
[Initial Snapshots](../snapshots.md).

### Binlog retention safety

DeltaForge validates binlog retention before starting a snapshot and monitors
it throughout. This prevents the silent failure mode where a long snapshot
completes successfully but CDC startup then fails because the captured binlog
position was purged.

**Preflight checks (before any rows are read):**
- Fails hard if `log_bin=0` or `binlog_format != ROW`
- Estimates snapshot duration from table sizes and `max_parallel_tables`
- Warns at ≥50% of `binlog_expire_logs_seconds` usage; HIGH RISK at ≥80%

**During snapshot:**
- Background task polls `SHOW BINARY LOGS` every 30s
- Cancels the snapshot immediately if the captured file is purged

**After all tables complete:**
- Synchronous final check before the generation records its rows as produced:
  a snapshot whose anchor binlog file was purged never completes, and blocks
  with `snapshot_anchor_unavailable` (see
  [Bounds and blocking](../snapshots.md#bounds-and-blocking))

If you see retention risk warnings, the recommended actions are:
1. Increase `binlog_expire_logs_seconds` to cover the estimated snapshot duration
2. Use a read replica as the snapshot source to avoid load on the primary

## Server ID Handling

MySQL replication requires each replica to have a unique `server_id`. DeltaForge derives this automatically from the source `id` using a CRC32 hash:

```
server_id = 1 + (CRC32(id) % 4,000,000,000)
```

When running multiple DeltaForge instances against the same MySQL server, ensure each has a unique `id` to avoid server_id conflicts.

## Schema Tracking

DeltaForge has a built-in schema registry to track table schemas, per source. For MySQL source:

- Schemas are loaded when a table is first used, not at startup: startup work does not grow with the number of matched tables (only an initial snapshot reads the tables it copies)
- Each schema is fingerprinted using SHA-256 for change detection
- Events carry `schema_version` (the fingerprint of the version the row was decoded with) and `schema_sequence` (that version's registry sequence)
- Schema-to-checkpoint correlation enables reliable replay

A DDL forgets the cached shapes of the tables it affects; their new versions are registered only by a positional proof (below), never reloaded from the live catalog. See [Schema Registry: DDL Handling](../schemaregistry.md#ddl-handling).

### DDL events

Each DDL statement emits one event (`source.table = "_ddl"`, operation `r`). Its payload (format 2) identifies what the statement acts on:

```json
{
  "ddl_format": 2,
  "scope": "tables",
  "objects": [
    {"kind": "table", "database": "shop", "name": "orders"},
    {"kind": "table", "database": "archive", "name": "orders"}
  ],
  "database": null,
  "session_database": "shop",
  "sql": "RENAME TABLE shop.orders TO archive.orders"
}
```

- **`scope`:** what the statement does to base-table shapes:
  - `tables`: it may change the listed tables;
  - `same_shape`: TRUNCATE;
  - `database`: a database barrier;
  - `lineage`: unattributable;
  - `none`: no base table changes (views, account statements).
- **`objects`:** `{kind, database, name}` for each table, view or barrier database, in statement order. A rename lists source then target. The list is empty when the statement cannot be attributed. Under `lower_case_table_names = 2` table statements are applied as lineage barriers but keep their objects.
- **`database` and `source.db`:** name the one database every object belongs to. A cross-database statement or one without objects has `database: null` and `source.db: ""`.
- **`session_database`:** the connection's active database when the statement ran (`null` when none). It is context only, never the attribution.
- **Account statements** (`CREATE USER`, `ALTER USER`, `SET PASSWORD`) are `scope: none` with no objects. Their SQL is never emitted (`operation`, `redacted: true` instead).
- **No DDL event, as before:** plain `GRANT` and `REVOKE`.


### Rows are decoded with the definition in effect when they were written

Binlog row events carry values by column position, not by name. DeltaForge decodes each rows event with the schema version proven for its position in the binlog, never simply with the table's current definition, so replaying retained binlog across a DDL (after a restart from an older checkpoint, or while catching up) puts every value under the right column. A version is proven by:

- the table's definition captured when its first rows without other proof arrive, and proven unchanged between those rows and the capture (no DDL of the table and no unattributable statement in between);
- a DDL's resulting definition, captured right after the source reads the DDL and proven unchanged since;
- with `binlog_row_metadata = FULL` and neither of the above: the single recorded definition that exactly matches the row's binlog metadata (column names, types, signedness, charsets, primary key), recorded durably before the row is emitted.

Capturing a definition never requires the server to be idle. In one read-only transaction the source takes the table's shared metadata lock (the lock any `SELECT` takes: inserts, updates and deletes proceed, DDL of that table or its database waits), then reads the binlog position, the table's definition (several INFORMATION_SCHEMA queries) and the position again, then releases the lock. A DDL already running on the table finishes before the lock is granted, so the definition read is never older than the position; a DDL issued meanwhile waits a few milliseconds and lands after the capture. The source then proves from the binlog that nothing between the two positions could have changed that table, such as an unattributable statement. Ordinary writes, to any table, and DDL of other tables do not matter, so a busy server proves tables as readily as a quiet one. Only a relevant statement inside that short interval causes a retry; a binlog that cannot be read (purged, unknown events, scan limits) or a lock not granted within 30 seconds stops the proof instead. The capture needs `SELECT` on the table. Table and database names are compared as `lower_case_table_names` defines: exactly with `0`, case-insensitively with `1`, and with `2` (names stored as written, compared case-insensitively) every DDL counts as relevant.

When no version can be proven, the source **stops** with a schema error naming the table, emits nothing for those rows and does not advance its checkpoint. Under the default `binlog_row_metadata = MINIMAL` this happens when:

- retained rows predate a DDL the source never observed (for example, a checkpoint restored from before a schema change made by an earlier deployment);
- a statement DeltaForge cannot attribute to specific tables was executed (versioned comments such as `/*!50100 ALTER TABLE ... */`, several statements in one event, `ALTER DATABASE` or `DROP DATABASE` of a tracked database, among others): every table it may affect stays unproven until later proof for that table exists - usually its next rows, once the source captures the table's definition and proves nothing changed it since them, or a later DDL of the table that the source proves - or a re-snapshot;
- a table's first rows (or its first rows after an unattributable statement) are followed, before the source reads them, by a DDL of that table or another unattributable statement: the definition those rows were written under can no longer be captured (this needs the source to lag behind the database);
- two DDLs of one table both happen before the source reads the first, with rows written between them (the definition between the two was never captured; with `binlog_row_metadata = FULL` those rows proceed only if that definition was already recorded and is the single exact match).

Re-snapshot is the immediate remediation that always works.

**Many newly active tables at once.** When rows of a table without proof
arrive, the source first reads ahead (an activation window, at most
`proofs.activation_window_ms`, without advancing its position or emitting
anything) and collects every other table whose first rows follow. A
statement that can change a schema, or an idle stream, closes the window
early. One read-only transaction then captures all of these tables: it
locks their metadata in sorted order, which avoids deadlocking against a
multi-table DDL, and reads every definition between the same two binlog
positions. Meanwhile the source keeps reading the stream, holding what
arrives for the next window without emitting it, so tables first touched
during an admission are not held for a second full window. The next
window's capture and proof scan also start while the current window is
still being admitted. It is decided and written only after the current
window commits, in stream order: tables it captured that turn out to be
proven already are dropped, and any table it should have captured but did
not stops the source (an internal error, never repaired by guessing). At
most two windows exist at a time. One binlog
scan proves them all, and each table is admitted or
refused exactly as it would be on its own. The admitted tables' schema
versions and proofs are then written in one state-store transaction: all of
them or none. A failover drift check runs for every table of the window
first, so a drift incident stops the whole window with nothing of it
written. If the store cannot commit the window (after a few retries of the
whole transaction), the source stops; it never falls back to admitting the
tables one by one. If the locks are not granted
within `proofs.capture_lock_wait_secs` on three attempts, the source stops
with a `mysql_capture_contention` incident: nothing of the window is
emitted, the checkpoint stays before it, and a restart retries once the
schema change holding the lock is done. There is no fallback to one
capture per table.

**Proof scans start where their interval starts.** A proof scan normally
opens the binlog at a position the source observed in its own stream,
either the transaction of the rows or the end of the last scan, and checks
the first events the server sends before trusting it. Without such a
position, for example a snapshot baseline, the server seeks from the start
of the binlog file. That seek is bounded by `proofs.max_seek_ms`: a seek
that exceeds it stops the proof rather than wait, and the position the scan
then observes positions the next proof at the same boundary. GTID
membership still decides what a proof contains, so the start position
never changes a proof, only how long it takes.

**Recommended:** set `binlog_row_metadata = FULL` on the server. It removes the first two cases whenever the row's definition is still recorded, at a small cost in binlog size.

Schema versions record per-column detail (character octet length, collation, fractional-seconds precision, primary-key prefix). After upgrading from a release that did not, the first capture of each table may register one new version even without a DDL; events then carry that version's `schema_version` and `schema_sequence`.

## Timeouts and Heartbeats

| Behavior | Value | Description |
|----------|-------|-------------|
| Heartbeat interval | 15s | Server sends heartbeat if no events |
| Read timeout | 90s | Maximum wait for next binlog event |
| Inactivity timeout | 60s | Triggers reconnect if no data received |
| Connect timeout | 30s | Maximum time to establish connection |

These values are currently fixed. Reconnection uses exponential backoff with jitter.

## Event Format

Each captured row change produces an event with:

- `op`: `insert`, `update`, or `delete`
- `before`: Previous row state (updates and deletes only, requires `binlog_row_image = FULL`)
- `after`: New row state (inserts and updates only)
- `table`: Fully qualified table name (`database.table`)
- `tx_id`: GTID if available
- `checkpoint`: Binlog position for resume
- `schema_version`: Schema fingerprint
- `schema_sequence`: Monotonic sequence for schema correlation

## Troubleshooting

### Connection Issues

DeltaForge authenticates with the server's default plugin (`caching_sha2_password` on MySQL 8.x); no particular plugin is required. MySQL 8.4 disables `mysql_native_password` by default, so a user created `IDENTIFIED WITH mysql_native_password` fails to log in with an error mentioning that plugin. Move the user to the server's default plugin:
```sql
ALTER USER 'deltaforge'@'%' IDENTIFIED BY 'password';
```

### Missing Before-Images

If UPDATE/DELETE events have incomplete `before` data:
```sql
SET GLOBAL binlog_row_image = 'FULL';
```

### Binlog Not Enabled

Check binary logging status:
```sql
SHOW VARIABLES LIKE 'log_bin';
SHOW VARIABLES LIKE 'binlog_format';
SHOW BINARY LOG STATUS;  -- or SHOW MASTER STATUS on older versions
```

### Privilege Issues

Verify grants for your user:
```sql
SHOW GRANTS FOR 'deltaforge'@'%';
```

Required grants include `REPLICATION SLAVE` and `REPLICATION CLIENT`.