<p align="center">
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/postgresql/postgresql-original.svg" alt="PostgreSQL" width="80" height="80">
</p>

# PostgreSQL source

DeltaForge captures row-level changes from PostgreSQL using logical replication with the pgoutput plugin.

## Prerequisites

### PostgreSQL Server Configuration

Enable logical replication in `postgresql.conf`:

```ini
# Required settings
wal_level = logical
max_replication_slots = 10    # At least 1 per DeltaForge pipeline
max_wal_senders = 10          # At least 1 per DeltaForge pipeline
```

Restart PostgreSQL after changing these settings.

### User Privileges

Create a replication user with the required privileges:

```sql
-- Create user with replication capability
CREATE ROLE deltaforge WITH LOGIN REPLICATION PASSWORD 'your_password';

-- Grant connect access
GRANT CONNECT ON DATABASE your_database TO deltaforge;

-- Grant schema usage and table access for schema introspection
GRANT USAGE ON SCHEMA public TO deltaforge;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO deltaforge;

-- For automatic publication/slot creation (optional)
-- If you prefer manual setup, skip this and create them yourself
ALTER ROLE deltaforge SUPERUSER;  -- Or use manual setup below
```

### pg_hba.conf

Ensure your `pg_hba.conf` allows replication connections:

```
# TYPE  DATABASE        USER            ADDRESS                 METHOD
host    replication     deltaforge      0.0.0.0/0               scram-sha-256
host    your_database   deltaforge      0.0.0.0/0               scram-sha-256
```

### Replication Slot and Publication

DeltaForge can automatically create the replication slot and publication on first run. Alternatively, create them manually:

```sql
-- Create publication for specific tables
CREATE PUBLICATION my_pub FOR TABLE public.orders, public.order_items;

-- Or for all tables
CREATE PUBLICATION my_pub FOR ALL TABLES;

-- Create replication slot
SELECT pg_create_logical_replication_slot('my_slot', 'pgoutput');
```

### Replica Identity

For complete before-images on UPDATE and DELETE operations, set tables to `REPLICA IDENTITY FULL`:

```sql
ALTER TABLE public.orders REPLICA IDENTITY FULL;
ALTER TABLE public.order_items REPLICA IDENTITY FULL;
```

Without this setting:
- **FULL**: Complete row data in before-images
- **DEFAULT** (primary key): Only primary key columns in before-images
- **NOTHING**: No before-images at all

DeltaForge warns at startup if tables don't have `REPLICA IDENTITY FULL`.

## Configuration

Set `spec.source.type` to `postgres` and provide a config object:

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `id` | string | Yes | — | Unique identifier for checkpoints and metrics |
| `dsn` | string | Yes | — | PostgreSQL connection string |
| `slot` | string | Yes | — | Replication slot name |
| `publication` | string | Yes | — | Publication name |
| `tables` | array | Yes | — | Table patterns to capture |
| `start_position` | string/object | No | `earliest` | Where to start when no checkpoint exists |

### DSN Formats

DeltaForge accepts both URL-style and key=value DSN formats:

```yaml
# URL style
dsn: "postgres://user:pass@localhost:5432/mydb"

# Key=value style
dsn: "host=localhost port=5432 user=deltaforge password=pass dbname=mydb"
```

### Table Patterns

The `tables` field supports flexible pattern matching:

```yaml
tables:
  - public.orders          # exact match: schema "public", table "orders"
  - public.order_%         # LIKE pattern: tables starting with "order_"
  - myschema.*             # wildcard: all tables in "myschema"
  - %.audit_log            # cross-schema: "audit_log" table in any schema
  - orders                 # defaults to public schema: "public.orders"
```

System schemas (`pg_catalog`, `information_schema`, `pg_toast`) are always excluded.

### Start Position

Controls where replication begins when no checkpoint exists:

```yaml
# Start from the earliest available position (slot's restart_lsn)
start_position: earliest

# Start from current WAL position (skip existing data)
start_position: latest

# Start from a specific LSN
start_position:
  lsn: "0/16B6C50"
```

### Example

```yaml
source:
  type: postgres
  config:
    id: orders-postgres
    dsn: ${POSTGRES_DSN}
    slot: deltaforge_orders
    publication: orders_pub
    tables:
      - public.orders
      - public.order_items
    start_position: earliest
```

## Resume Behavior

DeltaForge checkpoints progress using PostgreSQL's LSN (Log Sequence Number):

1. **With checkpoint**: Resumes from the stored LSN
2. **Without checkpoint**: Uses the slot's `confirmed_flush_lsn` or `restart_lsn`
3. **New slot**: Starts from `pg_current_wal_lsn()` or the configured `start_position`

Checkpoints are stored using the `id` field as the key.

## Snapshot (Initial Load)

DeltaForge performs a consistent initial snapshot using PostgreSQL's exported
snapshot mechanism before starting logical replication.

### How it works

A coordinator connection exports a snapshot and captures the current WAL LSN
in a single round trip. Worker connections each import the shared snapshot into
their own `REPEATABLE READ` transaction - all workers see the same consistent
DB state with no locks held on the source.

Tables with a single integer primary key use PK-range chunking. All others
fall back to ctid page-range chunking.

### Configuration
```yaml
source:
  type: postgres
  config:
    id: orders-postgres
    dsn: ${POSTGRES_DSN}
    slot: deltaforge_orders
    publication: orders_pub
    tables:
      - public.orders
    snapshot:
      mode: initial           # initial | always | never (default: never)
      max_parallel_tables: 8  # tables snapshotted concurrently
      chunk_size: 10000       # rows per chunk for integer-PK tables
```

| Field | Default | Description |
|-------|---------|-------------|
| `mode` | `never` | `initial`: run once if no checkpoint exists; `always`: re-snapshot on every restart; `never`: skip |
| `max_parallel_tables` | `8` | Tables snapshotted concurrently |
| `chunk_size` | `10000` | Rows per range chunk (integer PK tables only; others use ctid chunking) |

### Snapshot events

Snapshot rows are emitted as `Op::Read` events (Debezium `op: "r"`),
distinguishable from live CDC `Op::Create` events. The WAL LSN captured at
snapshot time becomes the CDC resume point - no rows are missed or duplicated.

### Resume after interruption

If the snapshot is interrupted, DeltaForge resumes at table granularity on
the next restart - already-completed tables are skipped.

### WAL slot retention safety

DeltaForge validates replication slot health before starting a snapshot and
monitors it throughout. This prevents the slot from being invalidated during
a long snapshot, which would make the captured LSN unreachable for CDC resume.

**Preflight checks (before any rows are read):**
- Fails hard if the slot does not exist or is already invalidated
- Fails hard if `wal_status=lost`
- Warns if `wal_status=unreserved` (WAL retention no longer guaranteed)
- Estimates WAL generated during snapshot (~2× data size) against
  `max_slot_wal_keep_size`; warns at ≥50%, HIGH RISK at ≥80%

**During snapshot:**
- Background task polls `pg_replication_slots` every 30s
- Cancels immediately on slot invalidation or disappearance
- Warns but continues on `wal_status=unreserved`

**After all tables complete:**
- Synchronous final check before writing `finished=true`
- `finished=true` means the position is confirmed valid for CDC resume,
  not just that rows were emitted

If you see WAL retention risk warnings:
```sql
ALTER SYSTEM SET max_slot_wal_keep_size = '10GB';
SELECT pg_reload_conf();
```

## Type Handling

DeltaForge preserves PostgreSQL's native type semantics:

| PostgreSQL Type | JSON Representation |
|-----------------|---------------------|
| `boolean` | `true` / `false` |
| `integer`, `bigint` | JSON number |
| `real`, `double precision` | JSON number |
| `numeric` | JSON string (preserves precision) |
| `text`, `varchar` | JSON string |
| `json`, `jsonb` | Parsed JSON object/array |
| `bytea` | `{"_base64": "..."}` |
| `uuid` | JSON string |
| `timestamp`, `date`, `time` | ISO 8601 string |
| Arrays (`int[]`, `text[]`, etc.) | JSON array |
| TOAST unchanged | `{"_unchanged": true}` |

## Event Format

Each captured row change produces an event with:

- `op`: `insert`, `update`, `delete`, or `truncate`
- `before`: Previous row state (updates and deletes, requires appropriate replica identity)
- `after`: New row state (inserts and updates)
- `table`: Fully qualified table name (`schema.table`)
- `tx_id`: PostgreSQL transaction ID (xid)
- `checkpoint`: LSN position for resume
- `schema_version`: Schema fingerprint
- `schema_sequence`: Monotonic sequence for schema correlation

## WAL Management

Logical replication slots prevent WAL segments from being recycled until the consumer confirms receipt. To avoid disk space issues:

1. **Monitor slot lag**: Check `pg_replication_slots.restart_lsn` vs `pg_current_wal_lsn()`
2. **Set retention limits**: Configure `max_slot_wal_keep_size` (PostgreSQL 13+)
3. **Handle stale slots**: Drop unused slots with `pg_drop_replication_slot('slot_name')`

```sql
-- Check slot status and lag
SELECT slot_name, 
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as lag
FROM pg_replication_slots;
```

## Troubleshooting

### Connection Issues

If you see authentication errors:
```sql
-- Verify user has replication privilege
SELECT rolname, rolreplication FROM pg_roles WHERE rolname = 'deltaforge';

-- Check pg_hba.conf allows replication connections
-- Ensure the line type includes "replication" database
```

### Missing Before-Images

If UPDATE/DELETE events have incomplete `before` data:
```sql
-- Check current replica identity
SELECT relname, relreplident 
FROM pg_class 
WHERE relname = 'your_table';
-- d = default, n = nothing, f = full, i = index

-- Set to FULL for complete before-images
ALTER TABLE your_table REPLICA IDENTITY FULL;
```

### Slot/Publication Not Found

```sql
-- List existing publications
SELECT * FROM pg_publication;

-- List existing slots
SELECT * FROM pg_replication_slots;

-- Create if missing
CREATE PUBLICATION my_pub FOR TABLE public.orders;
SELECT pg_create_logical_replication_slot('my_slot', 'pgoutput');
```

### WAL Disk Usage Growing

```sql
-- Check slot lag
SELECT slot_name, 
       active,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as lag
FROM pg_replication_slots;

-- If slot is inactive and not needed, drop it
SELECT pg_drop_replication_slot('unused_slot');
```

### Logical Replication Not Enabled

```sql
-- Check wal_level
SHOW wal_level;  -- Should be 'logical'

-- If not, update postgresql.conf and restart PostgreSQL
-- wal_level = logical
```