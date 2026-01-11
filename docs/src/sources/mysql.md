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
binlog_row_image = FULL  -- Recommended for complete before-images
```

If `binlog_row_image` is not `FULL`, DeltaForge will warn at startup and before-images on UPDATE/DELETE events may be incomplete.

### User Privileges

Create a dedicated replication user with the required grants:

```sql
-- Create user with mysql_native_password (required by binlog connector)
CREATE USER 'deltaforge'@'%' IDENTIFIED WITH mysql_native_password BY 'your_password';

-- Replication privileges (required)
GRANT REPLICATION REPLICA, REPLICATION CLIENT ON *.* TO 'deltaforge'@'%';

-- Schema introspection (required for table discovery)
GRANT SELECT, SHOW VIEW ON your_database.* TO 'deltaforge'@'%';

FLUSH PRIVILEGES;
```

For capturing all databases, grant `SELECT` on `*.*` instead.

## Configuration

Set `spec.source.type` to `mysql` and provide a config object:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | Yes | Unique identifier used for checkpoints, server_id derivation, and metrics |
| `dsn` | string | Yes | MySQL connection string with replication privileges |
| `tables` | array | No | Table patterns to capture; omit or leave empty to capture all user tables |

### Table Patterns

The `tables` field supports flexible pattern matching using SQL LIKE syntax:

```yaml
tables:
  - shop.orders          # exact match: database "shop", table "orders"
  - shop.order_%         # LIKE pattern: tables starting with "order_" in "shop"
  - analytics.*          # wildcard: all tables in "analytics" database
  - %.audit_log          # cross-database: "audit_log" table in any database
  # omit entirely to capture all user tables (excludes system schemas)
```

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

1. **GTID** - Preferred if the MySQL server has GTID enabled. Provides the most reliable resume across binlog rotations and failovers.
2. **File:position** - Used when GTID is not available. Resumes from the exact binlog file and byte offset.
3. **Binlog tail** - On first run with no checkpoint, starts from the current end of the binlog (no historical replay).

Checkpoints are stored using the `id` field as the key.

## Server ID Handling

MySQL replication requires each replica to have a unique `server_id`. DeltaForge derives this automatically from the source `id` using a CRC32 hash:

```
server_id = 1 + (CRC32(id) % 4,000,000,000)
```

When running multiple DeltaForge instances against the same MySQL server, ensure each has a unique `id` to avoid server_id conflicts.

## Schema Tracking

DeltaForge has a built-in schema registry to track table schemas, per source. For MySQL source:

- Schemas are preloaded at startup by querying `INFORMATION_SCHEMA`
- Each schema is fingerprinted using SHA-256 for change detection
- Events carry `schema_version` (fingerprint) and `schema_sequence` (monotonic counter)
- Schema-to-checkpoint correlation enables reliable replay

Schema changes (DDL) trigger automatic reload of affected table schemas.

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