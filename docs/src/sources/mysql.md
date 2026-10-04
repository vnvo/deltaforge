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
binlog_row_image = FULL  -- Recommended for complete before-images
binlog_row_metadata = FULL  -- Recommended: decodes rows across unobserved schema changes
```

If `binlog_row_image` is not `FULL`, DeltaForge will warn at startup and before-images on UPDATE/DELETE events may be incomplete. For `binlog_row_metadata`, see [Schema Tracking](#schema-tracking).

**Initial-snapshot requirements.** When a snapshot runs (`snapshot.mode` = `initial` or `always`), DeltaForge fails closed at preflight unless: `gtid_mode = ON`, `binlog_format = ROW`, every snapshotted table uses the **InnoDB** storage engine, and the user holds the global **RELOAD** privilege (see [Snapshot](#snapshot-initial-load)). These are checked only when a snapshot actually runs; a CDC-only pipeline (`snapshot.mode = never`) is never rejected for them.

### User Privileges

Create a dedicated replication user with the required grants:

```sql
-- Create user with mysql_native_password (required by binlog connector)
CREATE USER 'deltaforge'@'%' IDENTIFIED WITH mysql_native_password BY 'your_password';

-- Replication privileges (required)
GRANT REPLICATION REPLICA, REPLICATION CLIENT ON *.* TO 'deltaforge'@'%';

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

## Resume Behavior

DeltaForge automatically checkpoints progress and resumes from the last position on restart. The resume strategy follows this priority:

1. **GTID**: Preferred if the MySQL server has GTID enabled. Provides the most reliable resume across binlog rotations and failovers.
2. **File:position**: Used when GTID is not available. Resumes from the exact binlog file and byte offset.
3. **Binlog tail**: On first run with no checkpoint, starts from the current end of the binlog (no historical replay).

Checkpoints are stored using the `id` field as the key.

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

### Snapshot events

Snapshot rows are emitted as `Op::Read` events (Debezium `op: "r"`), distinguishable
from live CDC `Op::Create` events. The binlog position captured at snapshot time becomes
the CDC resume point, so no rows are missed or duplicated.

### Resume after interruption

If the snapshot is interrupted, DeltaForge resumes at table granularity on the next
restart - already-completed tables are skipped.

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
- Synchronous final check before writing `finished=true`
- `finished=true` means the position is confirmed valid for CDC resume,
  not just that rows were emitted

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

Schema changes (DDL) trigger automatic reload of affected table schemas.

### Rows are decoded with the definition in effect when they were written

Binlog row events carry values by column position, not by name. DeltaForge decodes each rows event with the schema version proven for its position in the binlog, never simply with the table's current definition, so replaying retained binlog across a DDL (after a restart from an older checkpoint, or while catching up) puts every value under the right column. A version is proven by:

- the table's definition captured when its first rows without other proof arrive, and proven unchanged between those rows and the capture (no DDL of the table and no unattributable statement in between);
- a DDL's resulting definition, captured right after the source reads the DDL and proven unchanged since;
- with `binlog_row_metadata = FULL` and neither of the above: the single recorded definition that exactly matches the row's binlog metadata (column names, types, signedness, charsets, primary key), recorded durably before the row is emitted.

When no version can be proven, the source **stops** with a schema error naming the table, emits nothing for those rows and does not advance its checkpoint. Under the default `binlog_row_metadata = MINIMAL` this happens when:

- retained rows predate a DDL the source never observed (for example, a checkpoint restored from before a schema change made by an earlier deployment);
- a statement DeltaForge cannot attribute to specific tables was executed (versioned comments such as `/*!50100 ALTER TABLE ... */`, several statements in one event, `ALTER DATABASE` or `DROP DATABASE` of a tracked database, among others): every table it may affect stays unproven until later proof for that table exists - usually its next rows, once the source captures the table's definition and proves nothing changed it since them, or a later DDL of the table that the source proves - or a re-snapshot;
- a table's first rows (or its first rows after an unattributable statement) are followed, before the source reads them, by a DDL of that table or another unattributable statement: the definition those rows were written under can no longer be captured (this needs the source to lag behind the database);
- two DDLs of one table both happen before the source reads the first, with rows written between them (the definition between the two was never captured; with `binlog_row_metadata = FULL` those rows proceed only if that definition was already recorded and is the single exact match).

Re-snapshot is the immediate remediation that always works.

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

If you see authentication errors mentioning `mysql_native_password`:
```sql
ALTER USER 'deltaforge'@'%' IDENTIFIED WITH mysql_native_password BY 'password';
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

Required grants include `REPLICATION REPLICA` and `REPLICATION CLIENT`.