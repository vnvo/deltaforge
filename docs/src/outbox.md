# Outbox Pattern

The [transactional outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html) guarantees that domain events are published whenever the corresponding database change is committed - no two-phase commit required. DeltaForge supports this natively for both MySQL and PostgreSQL with zero application-side polling.

## How It Works

```
┌─────────────────┐     ┌──────────────┐     ┌──────────────────┐     ┌──────┐
│  Application    │────>│   Database   │────>│   DeltaForge     │────>│ Sink │
│  (writes data   │     │  (outbox     │     │  (captures +     │     │      │
│   + outbox msg) │     │   table/WAL) │     │   transforms)    │     │      │
└─────────────────┘     └──────────────┘     └──────────────────┘     └──────┘
```

1. Your application writes business data **and** an outbox message in the same transaction.
2. DeltaForge captures the outbox event through the database's native replication stream.
3. The `OutboxProcessor` extracts the payload, resolves the destination topic, and sets routing headers.
4. The transformed event flows to sinks like any other CDC event.

Because the outbox write is part of the application transaction, the event is guaranteed to be published if and only if the transaction commits.

## Source Configuration

Each database uses its native mechanism — there is nothing to install or poll.

### PostgreSQL — WAL Messages

PostgreSQL uses `pg_logical_emit_message()` to write messages directly into the WAL. No table is needed.

```sql
-- In your application transaction:
BEGIN;
INSERT INTO orders (id, total) VALUES (42, 99.99);
SELECT pg_logical_emit_message(
  true,     -- transactional: tied to the enclosing TX
  'outbox', -- prefix: matched by DeltaForge
  '{"aggregate_type":"Order","aggregate_id":"42","event_type":"OrderCreated","payload":{"total":99.99}}'
);
COMMIT;
```

**Source config:**

```yaml
source:
  type: postgres
  config:
    id: orders-pg
    dsn: ${POSTGRES_DSN}
    slot: deltaforge_orders
    publication: orders_pub
    tables: [public.orders]
    outbox:
      prefixes: [outbox]
```

| Field | Type | Description |
|-------|------|-------------|
| `outbox.prefixes` | array | WAL message prefixes to capture. Supports glob patterns: `outbox` (exact), `outbox_%` (prefix match), `*` (all). |

The message prefix becomes `source.table` on the event, so processors can filter by prefix when multiple outbox channels share a pipeline.

### MySQL — Table Inserts

MySQL uses a regular table. For production, use the `BLACKHOLE` storage engine so rows are written to the binlog but never stored on disk.

```sql
-- Create outbox table (BLACKHOLE = no disk storage, binlog only)
CREATE TABLE outbox (
  id        INT AUTO_INCREMENT PRIMARY KEY,
  aggregate_type VARCHAR(64),
  aggregate_id   VARCHAR(64),
  event_type     VARCHAR(64),
  payload        JSON
) ENGINE=BLACKHOLE;
```

```sql
-- In your application transaction:
BEGIN;
INSERT INTO orders (id, total) VALUES (42, 99.99);
INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload)
VALUES ('Order', '42', 'OrderCreated', '{"total": 99.99}');
COMMIT;
```

**Source config:**

```yaml
source:
  type: mysql
  config:
    id: orders-mysql
    dsn: ${MYSQL_DSN}
    tables:
      - shop.orders
      - shop.outbox
    outbox:
      tables: ["shop.outbox"]
```

| Field | Type | Description |
|-------|------|-------------|
| `outbox.tables` | array | Table patterns to tag as outbox events. Supports globs: `shop.outbox` (exact), `*.outbox` (any database), `shop.outbox_%` (prefix). |

> **Note:** The outbox table must be included in the source's `tables` list so DeltaForge subscribes to its binlog events. Only INSERTs are captured - UPDATE and DELETE on the outbox table are ignored.

## Outbox Processor

The `OutboxProcessor` transforms raw outbox events into routed, sink-ready events. Add it to your `processors` list:

```yaml
processors:
  - type: outbox
    topic: "${aggregate_type}.${event_type}"
    default_topic: "events.unrouted"
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | string | `"outbox"` | Processor identifier |
| `tables` | array | `[]` | Filter: only process outbox events matching these patterns. Empty = all outbox events. |
| `topic` | string | - | Topic template resolved against the raw payload using `${field}` placeholders |
| `default_topic` | string | - | Fallback topic when template resolution fails and no `topic` column exists |
| `columns` | object | *(see below)* | Column name mappings for extracting outbox fields |
| `additional_headers` | map | `{}` | Forward extra payload fields as routing headers. Key = header name, value = column name. |
| `raw_payload` | bool | `false` | When true, deliver the extracted payload as-is to sinks, bypassing envelope wrapping (native/debezium/cloudevents). Metadata is still available via routing headers. |

### Column Mappings

Column mappings control **header extraction and payload rewriting** - they tell the processor which fields correspond to `aggregate_type`, `aggregate_id`, etc. for setting `df-*` headers. The **topic template** resolves directly against the raw payload, so you reference your actual column names there.

If your outbox payload uses non-default field names, override them:

```yaml
processors:
  - type: outbox
    topic: "${aggregate_type}.${event_type}"
    columns:
      payload: data           # default: "payload"
      aggregate_type: type    # default: "aggregate_type"
      aggregate_id: key       # default: "aggregate_id"
      event_type: action      # default: "event_type"
      topic: destination      # default: "topic"
```

### Additional Headers

Forward arbitrary payload fields as routing headers. This is useful when migrating from Debezium's `table.fields.additional.placement` or when downstream consumers need extra metadata:

```yaml
processors:
  - type: outbox
    topic: "${aggregate_type}.${event_type}"
    additional_headers:
      x-trace-id: trace_id
      x-correlation-id: correlation_id
      x-source-region: region
```

Each key becomes a header name, each value is the column name in the outbox payload. Missing columns are silently skipped — no error if a row doesn't contain the field.

### What the Processor Does

1. **Identifies** outbox events by the `__outbox` sentinel on `source.schema` (set by the source).
2. **Extracts** `aggregate_type`, `aggregate_id`, `event_type`, and `payload` from `event.after`.
3. **Resolves the topic** using a three-step cascade:
   - Template resolved against the **raw payload** (e.g. `${domain}.${action}` — use your actual column names)
   - Column value (a `topic` field in the payload, configurable via `columns.topic`)
   - `default_topic` fallback
4. **Rewrites** `event.after` to just the `payload` content.
5. **Sets routing headers**: `df-aggregate-type`, `df-aggregate-id`, `df-event-type`, plus any `additional_headers` mappings.
6. **Marks raw delivery** if `raw_payload: true` — sinks serialize `event.after` directly, skipping envelope wrapping.
7. **Clears** the `__outbox` sentinel so the event looks like a normal CDC event to sinks.
8. **Drops** non-INSERT outbox events (UPDATE/DELETE on the outbox table are meaningless).
9. **Passes through** all non-outbox events unchanged.

### Multi-Outbox Routing

When a source captures multiple outbox channels, use the `tables` filter to scope each processor:

```yaml
processors:
  - type: outbox
    tables: [orders_outbox]
    topic: "orders.${event_type}"

  - type: outbox
    tables: [payments_outbox]
    topic: "payments.${event_type}"
    columns:
      payload: data
```

## Complete Examples

### PostgreSQL → Kafka

```yaml
apiVersion: deltaforge/v1
kind: Pipeline
metadata:
  name: order-events
  tenant: acme
spec:
  source:
    type: postgres
    config:
      id: pg1
      dsn: ${POSTGRES_DSN}
      publication: orders_pub
      slot: orders_slot
      tables: [public.orders]
      outbox:
        prefixes: [outbox]

  processors:
    - type: outbox
      topic: "${aggregate_type}.${event_type}"
      default_topic: "events.unrouted"
      raw_payload: true

  sinks:
    - type: kafka
      config:
        id: k1
        brokers: ${KAFKA_BROKERS}
        topic: "events.fallback"
```

The `raw_payload: true` means outbox events hit the wire as the extracted payload JSON. Regular CDC events (from `public.orders`) still use the sink's configured envelope.

### MySQL → Kafka (Multi-Outbox)

```yaml
apiVersion: deltaforge/v1
kind: Pipeline
metadata:
  name: shop-events
  tenant: acme
spec:
  source:
    type: mysql
    config:
      id: m1
      dsn: ${MYSQL_DSN}
      tables:
        - shop.orders
        - shop.orders_outbox
        - shop.payments_outbox
      outbox:
        tables: ["shop.*_outbox"]

  processors:
    - type: outbox
      tables: [orders_outbox]
      topic: "orders.${event_type}"
      raw_payload: true

    - type: outbox
      tables: [payments_outbox]
      topic: "payments.${event_type}"
      raw_payload: true

  sinks:
    - type: kafka
      config:
        id: k1
        brokers: ${KAFKA_BROKERS}
        topic: "events.default"
```

## Migrating from Debezium

If you're using Debezium's outbox event router with a custom schema, DeltaForge's column mappings and `additional_headers` map directly. For example, given this Debezium-style outbox table:

```sql
CREATE TABLE outbox_events (
  id UUID PRIMARY KEY,
  aggregatetype VARCHAR(64),
  aggregateid VARCHAR(64),
  type VARCHAR(64),
  payload JSONB,
  traceid VARCHAR(64),
  tenant VARCHAR(32)
);
```

Debezium config:
```properties
transforms.outbox.table.field.event.id=id
transforms.outbox.table.field.event.key=aggregateid
transforms.outbox.table.field.event.type=type
transforms.outbox.table.field.event.payload=payload
transforms.outbox.table.fields.additional.placement=traceid:header,tenant:header
```

DeltaForge equivalent:
```yaml
processors:
  - type: outbox
    topic: "${aggregatetype}.${type}"
    raw_payload: true
    columns:
      aggregate_type: aggregatetype
      aggregate_id: aggregateid
      event_type: type
      payload: payload
    additional_headers:
      x-trace-id: traceid
      x-tenant: tenant
```

Key differences from Debezium:
- Topic template references **raw column names** directly — `${aggregatetype}`, not `${aggregate_type}`
- `raw_payload: true` delivers the extracted payload as-is — same behavior as Debezium's outbox event router
- Column mappings only affect header extraction (`df-*` headers) and payload rewriting
- `additional_headers` replaces `table.fields.additional.placement`
- No SMT chain — everything is in one processor config

## Tips

- **PostgreSQL WAL messages are lightweight.** No table, no index, no vacuum — just a single WAL entry per message. Prefer this over table-based outbox when using PostgreSQL.
- **MySQL BLACKHOLE engine** avoids storing outbox rows on disk. The row is written to the binlog and immediately discarded. Use it in production to avoid unbounded table growth.
- **Outbox events coexist with normal CDC.** The processor passes through all non-outbox events untouched, so you can mix regular table capture and outbox in the same pipeline.
- **Topic templates use `${field}` syntax**, same as [dynamic routing](routing.md). The template resolves directly against the raw outbox payload columns — use your actual column names like `${domain}.${action}`, no remapping needed.
- **At-least-once delivery** applies to outbox events just like regular CDC events. Downstream consumers should be idempotent — use `aggregate_id` + `event_type` as a deduplication key.