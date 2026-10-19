# ClickHouse Sink

Streams CDC events into [ClickHouse](https://clickhouse.com) over the HTTP
interface using the compact `RowBinary` format. It supports two consumption
shapes with **one uniform write path** — the target table's engine decides
whether you get a change-log or a mirrored current-state table.

```yaml
sinks:
  - type: clickhouse
    config:
      id: ch-orders
      url: "https://clickhouse:8443"     # HTTP(S) endpoint
      database: analytics
      table: orders
      mode: upsert                       # upsert | changelog
      user: default
      password: "${CLICKHOUSE_PASSWORD}" # ${ENV} expansion
```

## Configuration

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `id` | yes | — | Unique sink identifier |
| `url` | yes | — | ClickHouse HTTP(S) endpoint (e.g. `http://host:8123`) |
| `database` | yes | — | Target database |
| `table` | yes | — | Target table |
| `mode` | no | `changelog` | `upsert` (current state) or `changelog` (retain all changes) |
| `user` | no | — | ClickHouse user (`${ENV}` supported) |
| `password` | no | — | Password/key (`${ENV}` supported) |
| `tls.enabled` | no | `true` | TLS for `https://` endpoints |
| `tls.insecure_skip_verify` | no | `false` | Skip certificate verification |
| `version_source` | no | `source_position` | `_version` source: `source_position` (LSN/binlog) or `ts_ms` |
| `auto_create` | no | `true` | Must be `true`: the sink writes only to a table it creates (see [Destination ownership](#destination-ownership-and-fencing)); `false` is rejected |
| `send_timeout_secs` | no | `30` | Per-batch insert timeout (timeouts → backpressure) |
| `required` | no | `true` | Required (blocks) vs best-effort (log + continue) |

## Modes

The sink always writes the same row shape — your source columns plus the meta
columns `_op`, `_version`, `_deleted`, `_source_ts`. What differs is the target
table's **engine**, which the sink auto-creates for you (or you pre-create it
and set `auto_create: false`).

### `upsert` — mirror current state

```sql
CREATE TABLE analytics.orders
( id Int64, amount Decimal(12,2),
  _op LowCardinality(String), _version UInt64, _deleted UInt8,
  _source_ts DateTime64(3) )
ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY id;                        -- ORDER BY = source primary key
```

ClickHouse's merge keeps the row with the max `_version` per key and drops
deletes. Query current state with `FINAL`:

```sql
SELECT * FROM analytics.orders FINAL;
```

### `changelog` — retain every change

Same columns, `ENGINE = MergeTree ORDER BY (id, _version)`. Every insert/update/
delete is kept as a row (audit / streaming). Reconstruct current state at read
time:

```sql
SELECT argMax(amount, _version) FROM analytics.orders
WHERE _deleted = 0 GROUP BY id;
```

## Delivery guarantees

**At-least-once.** The checkpoint advances only after an insert acks, so no
events are lost; a crash between ack and checkpoint can replay a batch. On top of
that:

- **`upsert` mode is idempotent** — duplicate or out-of-order inserts collapse by
  `(key, _version)`, so the mirrored table converges to exactly the source's
  current state regardless of retries. Read with `FINAL` for a consistent view.
  This is the recommended mode when correctness matters most.
- **`changelog` mode is at-least-once** — retries are suppressed by ClickHouse's
  `insert_deduplication_token` for simple replays (within the dedup window;
  Replicated tables, or non-replicated with `non_replicated_deduplication_window`),
  but that is best-effort. Dedup at read time via `argMax(_version)` for exact
  current state.

End-to-end exactly-once (Kafka-EOS style) is **not** claimed — ClickHouse inserts
are not transactional with the DeltaForge checkpoint.

`_version` defaults to the source LSN/binlog position (monotonic), which is what
makes `ReplacingMergeTree` replacement correct. Only fall back to `ts_ms` when the
source lacks a usable position (millisecond ties per key are undefined).

## Destination ownership and fencing

A ClickHouse sink writes one target table, which holds one source table.

**Supported topology.** The sink's guarantees cover one ClickHouse server holding a local `MergeTree` (`changelog`) or `ReplacingMergeTree` (`upsert`) table that the sink created itself, written with synchronous inserts. Refused:

- `Replicated*` engines, `Distributed` tables and every other engine (`unsupported_topology`);
- an endpoint whose `serverUUID()` differs from the one recorded when the table was created (`server_changed`). A load balancer over several servers is unsupported.

Every insert runs with `async_insert=0`, and every claim proves that this takes effect: a settings profile that pins `async_insert` on is refused (`fencing_unenforced`). An asynchronous insert would be acknowledged before the fence below is checked.

**The sink writes only to a table it creates.** It runs `CREATE TABLE` without `IF NOT EXISTS`. The table has:

- an immutable ownership marker as its comment, naming:
  - the owner (the deployment, which is the state store's identity, plus the pipeline and the sink);
  - the source table;
  - the target database and table;
  - the encoding (layout, type mapping, mode, extensions column);
  - a random instance;
- the reserved last column `_df_fence LowCardinality(String)`;
- the constraint `df_fence_<fence> CHECK _df_fence = '<fence>'`.

Every row carries its writer's fence. A table the sink did not create is refused, with no adoption, as is a source column named `_df_fence`.

**Claims.** Before a session first writes, and again after a reconnect or a client recreation, the sink *claims* the table:

1. It checks the marker, the record in the state store, the engine, the reserved column (exactly one, last, of the exact type), and that exactly one well-formed fence constraint exists.
2. It rotates the fence with one `ALTER TABLE … DROP CONSTRAINT df_fence_<old>, ADD CONSTRAINT df_fence_<new> …`. That statement fails as a whole if the old constraint is gone: a compare-and-swap.

Between claims, inserts make no ownership request.

**The fencing guarantee.** After a claim has rotated the fence, every newly evaluated insert carrying the old fence is rejected by the server (`VIOLATED_CONSTRAINT`). An insert the server had already accepted may still complete, so writes around an ownership change are at least once.

A rejected batch fails whole as `sink_destination_conflict` (class `fenced`):

- it is never dead-lettered;
- the checkpoint does not advance;
- the pipeline stops.

**Cost.** The fence adds 33 bytes per row to each insert. Stored, it compresses to under 0.01 bytes per row.

**Clone and restore.**

- **Two separately created state stores** are two deployments (`owned_by_other`).
- **A copied state store** has the same identity. The first copy to claim fences the other, whose next insert fails at the server.
- **A state store restored from an older backup** is refused (`fenced`).
- **A dropped table** is never recreated (`destination_missing`). Its ownership record is removed only by deleting the pipeline, which resets the sink's checkpoints in the same operation.
- **Not distinguishable:** a copy of the table, with its comment and constraint, on a server cloned from the same data directory (the same `serverUUID()`). This is outside the guarantee.

**Refusals** are the `sink_destination_conflict` incident: nothing of the batch is acknowledged and the checkpoint stays. The incident names the table, the source table and the class; it never contains row data or credentials. The classes:

- `marker_missing`, `marker_malformed`, `marker_unsupported_version`;
- `owned_by_other`, `bound_to_another_relation`;
- `destination_identity_mismatch`, `encoding_mismatch`;
- `fenced`, `fence_malformed`;
- `destination_missing`, `unrecorded_destination`;
- `server_changed`, `unsupported_topology`, `fencing_unenforced`.

## Requirements

The sink needs the **source table's column types + primary key** (RowBinary is
positional and auto-create derives the DDL from them). It loads these **on
demand** the first time it sees a table: the schema resolver queries the source
database's catalog (MySQL `INFORMATION_SCHEMA`, Postgres `pg_catalog`) and caches
the result. So the sink works under **any snapshot mode**, including
`snapshot: never` — no snapshot or schema sensing is required. The only
prerequisite is that the source database is reachable and the table exists, which
is already true for CDC. The v1 sink targets a **single table**; capture one
source table per ClickHouse sink (not a wildcard).

## Auto table creation

The sink creates the target table on the first event, deriving column types from
the source DDL and the engine/`ORDER BY` from the mode + primary key, with its
ownership marker, fence column and fence constraint (see
[Destination ownership](#destination-ownership-and-fencing)). A pre-created table
is refused. The sink's user needs `CREATE TABLE`, `ALTER TABLE` (constraints),
`INSERT` and `SELECT` on the target database.

## Type mapping

| Source | ClickHouse |
| --- | --- |
| `bigint` / `bigint unsigned` | `Int64` / `UInt64` |
| `int`, `smallint`, `tinyint` | `Int32`, `Int16`, `UInt8` |
| `decimal(p,s)` | `Decimal(p, s)` (exact) |
| `float` / `double` | `Float64` |
| `boolean` | `UInt8` |
| `date` / `datetime` / `timestamp` | `DateTime64(3)` |
| `varchar` / `text` / `json` / other | `String` |

Nullable source columns map to `Nullable(T)`. JSON is stored as text in v1.

## Examples

**Self-hosted — change-log (audit) table over plain HTTP, auto-created:**

```yaml
sinks:
  - type: clickhouse
    config:
      id: ch-audit
      url: "http://clickhouse:8123"
      database: analytics
      table: orders_changelog
      mode: changelog                # MergeTree: retains every change
```

**Current-state mirror into a pre-created `ReplacingMergeTree`:**

```yaml
sinks:
  - type: clickhouse
    config:
      id: ch-orders-current
      url: "https://clickhouse:8443"
      database: analytics
      table: orders
      mode: upsert
      user: cdc
      password: "${CLICKHOUSE_PASSWORD}"
      auto_create: false             # operator pre-creates the table
```
