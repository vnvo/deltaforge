# Turso Source

> ⚠️ **STATUS: EXPERIMENTAL / PAUSED**
>
> The Turso source is not yet ready for production use. Native CDC in Turso/libSQL
> is still evolving and has limitations:
> - CDC is per-connection (only changes from the enabling connection are captured)
> - File locking prevents concurrent access
> - sqld Docker image doesn't have CDC support yet
>
> This documentation is retained for reference. The code exists but is not officially supported.

---

The Turso source captures changes from Turso and libSQL databases. It supports multiple CDC modes to work with different database configurations and Turso versions.

## CDC Modes

DeltaForge supports four CDC modes for Turso/libSQL:

| Mode | Description | Requirements |
|------|-------------|--------------|
| `native` | Uses Turso's built-in CDC via `turso_cdc` table | Turso v0.1.2+ with CDC enabled |
| `triggers` | Shadow tables populated by database triggers | Standard SQLite/libSQL |
| `polling` | Tracks changes via rowid comparison | Any SQLite/libSQL (inserts only) |
| `auto` | Automatic fallback: native → triggers → polling | Any |

### Native Mode

Native mode uses Turso's built-in CDC capabilities. This is the most efficient mode when available.

**Requirements:**
- Turso database with CDC enabled
- Turso server v0.1.2 or later

**How it works:**
1. Queries the `turso_cdc` system table for changes
2. Uses `bin_record_json_object()` to extract row data as JSON
3. Tracks position via change ID in checkpoints

### Triggers Mode

Triggers mode uses shadow tables and database triggers to capture changes. This works with standard SQLite and libSQL without requiring native CDC support.

**How it works:**
1. Creates shadow tables (`_df_cdc_{table}`) for each tracked table
2. Installs INSERT/UPDATE/DELETE triggers that write to shadow tables
3. Polls shadow tables for new change records
4. Cleans up processed records periodically

### Polling Mode

Polling mode uses rowid tracking to detect new rows. This is the simplest mode but only captures inserts (not updates or deletes).

**How it works:**
1. Tracks the maximum rowid seen per table
2. Queries for rows with rowid greater than last seen
3. Emits insert events for new rows

**Limitations:**
- Only captures INSERT operations
- Cannot detect UPDATE or DELETE
- Requires tables to have accessible rowid (not WITHOUT ROWID tables)

### Auto Mode

Auto mode tries each CDC mode in order and uses the first one that works:

1. Try native mode (check for `turso_cdc` table)
2. Try triggers mode (check for existing CDC triggers)
3. Fall back to polling mode

This is useful for deployments where the database capabilities may vary.

## Configuration

```yaml
source:
  type: turso
  config:
    id: turso-main
    url: "libsql://your-db.turso.io"
    auth_token: "${TURSO_AUTH_TOKEN}"
    tables: ["users", "orders", "order_items"]
    cdc_mode: auto
    poll_interval_ms: 1000
    native_cdc:
      level: data
```

### Configuration Options

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `id` | string | Yes | — | Logical identifier for metrics and logging |
| `url` | string | Yes | — | Database URL (`libsql://`, `http://`, or file path) |
| `auth_token` | string | No | — | Authentication token for Turso cloud |
| `tables` | array | Yes | — | Tables to track (supports wildcards) |
| `cdc_mode` | string | No | `auto` | CDC mode: `native`, `triggers`, `polling`, `auto` |
| `poll_interval_ms` | integer | No | `1000` | Polling interval in milliseconds |
| `native_cdc.level` | string | No | `data` | Native CDC level: `binlog` or `data` |

### Table Patterns

The `tables` field supports patterns:

```yaml
tables:
  - users              # Exact match
  - order%             # Prefix match (order, orders, order_items)
  - "*"                # All tables (excluding system tables)
```

System tables and DeltaForge infrastructure tables are automatically excluded:
- `sqlite_*` — SQLite system tables
- `_df_*` — DeltaForge CDC shadow tables
- `_litestream*` — Litestream replication tables
- `_turso*` — Turso internal tables
- `turso_cdc` — Turso CDC system table

### Native CDC Levels

When using native mode, you can choose the CDC level:

| Level | Description |
|-------|-------------|
| `data` | Only row data changes (default, more efficient) |
| `binlog` | Full binlog-style events with additional metadata |

## Examples

### Local Development

```yaml
source:
  type: turso
  config:
    id: local-dev
    url: "http://127.0.0.1:8080"
    tables: ["users", "orders"]
    cdc_mode: auto
    poll_interval_ms: 500
```

### Turso Cloud

```yaml
source:
  type: turso
  config:
    id: turso-prod
    url: "libsql://mydb-myorg.turso.io"
    auth_token: "${TURSO_AUTH_TOKEN}"
    tables: ["*"]
    cdc_mode: native
    poll_interval_ms: 1000
```

### SQLite File (Polling Only)

```yaml
source:
  type: turso
  config:
    id: sqlite-file
    url: "file:./data/myapp.db"
    tables: ["events", "audit_log"]
    cdc_mode: polling
    poll_interval_ms: 2000
```

## Checkpoints

Turso checkpoints track position differently depending on the CDC mode:

```json
{
  "mode": "native",
  "last_change_id": 12345,
  "table_positions": {}
}
```

For polling mode, positions are tracked per-table:

```json
{
  "mode": "polling",
  "last_change_id": null,
  "table_positions": {
    "users": 1000,
    "orders": 2500
  }
}
```

## Schema Loading

The Turso source includes a schema loader that:

- Queries `PRAGMA table_info()` for column metadata
- Detects SQLite type affinities (INTEGER, TEXT, REAL, BLOB, NUMERIC)
- Identifies primary keys and autoincrement columns
- Handles WITHOUT ROWID tables
- Checks for existing CDC triggers

Schema information is available via the REST API at `/pipelines/{name}/schemas`.

## Notes

- **WITHOUT ROWID tables**: Polling mode cannot track WITHOUT ROWID tables. Use triggers or native mode instead.
- **Type affinity**: SQLite uses type affinity rather than strict types. The schema loader maps declared types to SQLite affinities.
- **Trigger cleanup**: In triggers mode, processed change records are cleaned up automatically based on checkpoint position.
- **Connection handling**: The source maintains a single connection and reconnects automatically on failure.