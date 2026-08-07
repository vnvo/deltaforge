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
| `auto_create` | no | `true` | Auto-create the target table on first event |
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

## Auto table creation

By default the sink creates the target table on the first event, deriving column
types from the source DDL and the engine/`ORDER BY` from the mode + primary key.
Set `auto_create: false` to require a pre-created table (locked-down setups).

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
