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

-- The replication SLOT is auto-created on first run using the REPLICATION
-- attribute granted above. The PUBLICATION is NOT auto-created — create it
-- yourself (see "Replication Slot and Publication" below).
```

The same role reads the catalog for capture-time annotations and checks.
Each such read first proves, in its own snapshot, that it reaches the node
and the live walsender serving the stream: it reads `pg_replication_slots`,
`pg_stat_activity` (the walsender's row; visible to the same role, otherwise
grant `pg_read_all_stats`) and `pg_control_system()`. A capture takes the
table's `ACCESS SHARE` lock (granted by `SELECT` on the table) for a few
milliseconds: inserts, updates and deletes are not blocked, DDL on that table
waits until the capture ends.

**Unsupported:** a tracked table with a stored generated column
(`GENERATED ALWAYS AS ... STORED`) stops the source before its next row
(`pg_table_unsupported`): pgoutput does not publish generated columns.
Exclude the table or drop the generation.

### pg_hba.conf

Ensure your `pg_hba.conf` allows replication connections:

```
# TYPE  DATABASE        USER            ADDRESS                 METHOD
host    replication     deltaforge      0.0.0.0/0               scram-sha-256
host    your_database   deltaforge      0.0.0.0/0               scram-sha-256
```

### Replication Slot and Publication

DeltaForge automatically creates the replication **slot** on first run and records
durable **ownership** of it (bound to the server's `system_identifier`, the
database, the slot name, and the plugin). The **publication** is not auto-created —
you must create it yourself. Create both manually if you prefer:

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
| `start_position` | string/object | No | — | ⚠️ Parsed but not yet implemented — a new slot always starts at the current WAL LSN |

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
  - public.order_%         # prefix: tables starting with "order_" (same as public.order_*)
  - myschema.*             # wildcard: all tables in "myschema"
  - %.audit_log            # cross-schema: "audit_log" table in any schema
  - orders                 # table "orders" in every schema
```

A wildcard (`*` or `%`) only counts as the last character of a part (a prefix match); `_` and any other character are literal. A pattern without a qualifier matches that table name in every database/schema. The initial snapshot copies exactly the tables these patterns capture during CDC.

System schemas (`pg_catalog`, `information_schema`, `pg_toast`) are always excluded.

### Start Position

> ⚠️ **Not yet implemented.** `start_position` is accepted by the config parser but currently ignored — a new slot always starts from the current WAL position (`pg_current_wal_lsn()`). The forms below are aspirational.

Intended to control where replication begins when no checkpoint exists:

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
```

## Resume Behavior

DeltaForge checkpoints progress using PostgreSQL's LSN (Log Sequence Number):

1. **With checkpoint**: Resumes from the stored LSN
2. **Without checkpoint**: Uses the slot's `confirmed_flush_lsn` or `restart_lsn`
3. **New slot**: Starts from `pg_current_wal_lsn()` (current WAL position)

Checkpoints are stored using the `id` field as the key.

## Snapshot (Initial Load)

DeltaForge performs a consistent initial snapshot using PostgreSQL's exported
snapshot mechanism before starting logical replication.

### How it works

DeltaForge anchors CDC at the replication slot's **consistent point** `C` - the
LSN `pg_create_logical_replication_slot` returns when the slot is created - rather
than a separately sampled `pg_current_wal_lsn()`. A coordinator connection then
exports an MVCC snapshot for worker mutual consistency; worker connections import
it into their own `REPEATABLE READ` transactions, so all workers see one
consistent DB state with no locks held on the source. Tables with a single
integer primary key use PK-range chunking; others fall back to ctid page-range
chunking.

Anchoring at `C` (which precedes the snapshot read) closes the snapshot-to-CDC
seam: **no committed row is lost**. The trade-off is a **bounded at-least-once
overlap** - a change committed between `C` and the snapshot export is present in
both the snapshot and the CDC stream from `C`, so it is delivered more than once.
This is at-least-once, **not** exactly-once (see [Snapshot events](#snapshot-events)).

> **Correctness note.** Earlier versions sampled `pg_current_wal_lsn()` decoupled
> from the exported snapshot, which admitted a (small) window where a row could be
> absent from both the snapshot and the CDC stream. The slot-consistent-point
> anchor removes that window, trading the loss risk for the bounded overlap above.

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
| `discovery_page_size` | `1000` | Tables read per catalog query during discovery; also the most plan entries held in memory |
| `max_snapshot_connections` | `max_parallel_tables x max_parallel_chunks + 2` | This source's share of the process-wide snapshot connection cap (`--max-snapshot-connections`, default 64) |
| `max_anchor_age_secs` | `86400` | How long a generation may hold its anchor; blocks at the limit |
| `max_plan_bytes` / `max_plan_items` | 256 MiB / `1000000` | Bounds of the durable plan; blocks before sealing at either limit |

### Snapshot events

Snapshot rows are emitted as `Op::Read` events (Debezium `op: "r"`),
distinguishable from live CDC `Op::Create` events. The slot's consistent point
`C` is the CDC resume position, so **no rows are missed**. Rows committed in
`(C, snapshot-export]` are delivered by both the snapshot (as `Op::Read`) and the
CDC stream (as their change op) - the **bounded at-least-once overlap**. The two
copies carry distinct event identities:

- **Current-state / idempotent sinks** (Elasticsearch, ClickHouse `upsert`, with
  `version_source: source_position`) converge to the correct current state - the
  duplicate is absorbed by last-writer-wins.
- **Append-only sinks** (Kafka, Redis, HTTP, ClickHouse `changelog`, legacy S3)
  receive the overlapping rows twice, by design.

This is at-least-once delivery; DeltaForge does not claim exactly-once for the
snapshot-to-CDC boundary.

### Legacy-anchor warning and metric

A pipeline whose initial snapshot was completed under the older (pre-hardening)
anchor is flagged so you can decide to re-snapshot:

- a structured `WARN` at startup, and
- the gauge `deltaforge_snapshot_unsafe_anchor{pipeline,source}` held at `1`.

Re-snapshotting under this version records the safe anchor and resets the gauge to
`0`. See [Upgrade guidance](#upgrade-guidance).

### Resume after interruption

An interrupted snapshot is **copied again in full** on the next restart, as a
new [snapshot generation](../snapshots.md#generations) with a new anchor; it is
never resumed table by table. When DeltaForge can prove it owns the now-inactive
slot, it **re-anchors** (drops and recreates its slot for a fresh `C`).

A snapshot counts as complete only once its terminal barrier is committed by the
sinks the commit policy requires; a restart after that completes it without
copying. Completion, lagging sinks, bounds and blocking are described in
[Initial Snapshots](../snapshots.md).

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
- Synchronous final check before the generation records its rows as produced:
  a snapshot whose slot is lost, invalidated or missing never completes, and
  blocks with `snapshot_anchor_unavailable` (see
  [Bounds and blocking](../snapshots.md#bounds-and-blocking))

If you see WAL retention risk warnings:
```sql
ALTER SYSTEM SET max_slot_wal_keep_size = '10GB';
SELECT pg_reload_conf();
```

## Upgrade guidance

The slot-consistent-point anchor and durable slot ownership change how existing
slots are handled when a snapshot runs. **Steady-state CDC resume from a
checkpoint is unaffected** - the notes below apply only when a snapshot runs.

- **Existing slots created before this version** have no ownership record. A
  re-snapshot on such a slot (`mode: always`, or `mode: initial` after clearing
  the checkpoint) cannot prove ownership and **fails closed** with remediation:
  drop the slot manually (`SELECT pg_drop_replication_slot('<slot>')`) so
  DeltaForge recreates it with ownership, then restart. New pipelines and
  first-run snapshots are unaffected.
- **Ownership ambiguity / foreign slots.** If the slot exists but DeltaForge
  cannot prove exclusive ownership (missing, partial, or mismatched record - e.g.
  a different server `system_identifier` after a restore/failover) or the slot is
  **active**, it **fails closed** and never drops the slot. Remediation: confirm
  no other consumer uses it, drop it, and restart; or set `snapshot.mode = never`
  to stream from the current position without an initial load.
- **Interrupted snapshots.** An owned, inactive slot left by a snapshot that was
  interrupted before its first checkpoint is re-anchored (dropped and recreated
  for a fresh consistent point) and fully re-snapshotted automatically.
- **Safe re-snapshotting.** To take a fresh, correctly-anchored snapshot, stop the
  pipeline and apply the [`resnapshot` recovery operation](../recovery.md#resnapshot)
  (or set `mode: always`), then resume. Checkpoints are kept; a slot this source
  owns is re-anchored, and a lost one is recreated only under the operation's
  explicit authorization. A completed, safely-anchored snapshot resets
  `deltaforge_snapshot_unsafe_anchor` to `0`.

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