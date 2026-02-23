# Outbox Pattern

This example demonstrates the transactional outbox pattern - writing business data and a domain event in the same database transaction, then streaming the event to Kafka via DeltaForge.

## Overview

| Component | Configuration |
|-----------|---------------|
| **Source** | MySQL binlog CDC |
| **Processor** | Outbox (extract + route) |
| **Sink** | Kafka (per-aggregate topics) |
| **Pattern** | Transactional outbox with raw payload delivery |

## Use Case

Your application writes orders and needs to publish `OrderCreated`, `OrderShipped`, etc. events to Kafka. You want:

- **Atomicity**: event published if and only if the transaction commits
- **Clean payloads**: consumers receive the application's JSON, not CDC envelopes
- **Per-aggregate routing**: events land on `Order.OrderCreated`, `Payment.PaymentReceived`, etc.
- **Zero polling**: DeltaForge tails the binlog, no application-side outbox relay

## Database Setup

```sql
-- Business table
CREATE TABLE orders (
  id         INT AUTO_INCREMENT PRIMARY KEY,
  customer   VARCHAR(64),
  total      DECIMAL(10,2),
  status     VARCHAR(32),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Outbox table (BLACKHOLE = binlog only, no disk storage)
CREATE TABLE outbox (
  id             INT AUTO_INCREMENT PRIMARY KEY,
  aggregate_type VARCHAR(64),
  aggregate_id   VARCHAR(64),
  event_type     VARCHAR(64),
  payload        JSON
) ENGINE=BLACKHOLE;
```

## Application Code

Write both in the same transaction:

```sql
BEGIN;

INSERT INTO orders (customer, total, status)
VALUES ('alice', 149.99, 'pending');

INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload)
VALUES (
  'Order',
  LAST_INSERT_ID(),
  'OrderCreated',
  JSON_OBJECT('customer', 'alice', 'total', 149.99, 'status', 'pending')
);

COMMIT;
```

If the transaction rolls back, neither the order nor the event exist. If it commits, both do.

## Pipeline Configuration

```yaml
apiVersion: deltaforge/v1
kind: Pipeline
metadata:
  name: orders-outbox
  tenant: acme

spec:
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

  processors:
    - type: outbox
      topic: "${aggregate_type}.${event_type}"
      default_topic: events.unrouted
      raw_payload: true

  sinks:
    - type: kafka
      config:
        id: events-kafka
        brokers: ${KAFKA_BROKERS}
        topic: "cdc.${source.db}.${source.table}"
        envelope:
          type: debezium
        encoding: json
        required: true

  batch:
    max_events: 500
    max_ms: 500
    respect_source_tx: true

  commit_policy:
    mode: required
```

### What lands on Kafka

**Outbox event** --> topic `Order.OrderCreated`:
```json
{"customer": "alice", "total": 149.99, "status": "pending"}
```

Raw payload, no envelope. Kafka headers carry metadata: `df-aggregate-type: Order`, `df-aggregate-id: 1`, `df-event-type: OrderCreated`.

**CDC event** --> topic `cdc.shop.orders` (from sink's topic template):
```json
{
  "payload": {
    "before": null,
    "after": {"id": 1, "customer": "alice", "total": 149.99, "status": "pending"},
    "source": {"connector": "mysql", "db": "shop", "table": "orders"},
    "op": "c",
    "ts_ms": 1700000000000
  }
}
```

Full Debezium envelope. Both flow through the same pipeline - the outbox processor only touches events tagged as outbox.

## Running the Example

### 1. Start Infrastructure

```bash
./dev.sh up
./dev.sh k-create Order.OrderCreated 3
./dev.sh k-create Order.OrderShipped 3
./dev.sh k-create cdc.shop.orders 3
```

### 2. Set Environment Variables

```bash
export MYSQL_DSN="mysql://deltaforge:dfpw@localhost:3306/shop"
export KAFKA_BROKERS="localhost:9092"
```

### 3. Start DeltaForge

```bash
cargo run -p runner -- --config outbox.yaml
```

### 4. Insert Test Data

```sql
BEGIN;
INSERT INTO shop.orders (customer, total, status) VALUES ('alice', 149.99, 'pending');
INSERT INTO shop.outbox (aggregate_type, aggregate_id, event_type, payload)
VALUES ('Order', LAST_INSERT_ID(), 'OrderCreated',
        '{"customer":"alice","total":149.99,"status":"pending"}');
COMMIT;
```

### 5. Verify

```bash
# Outbox event (raw payload, per-aggregate topic)
./dev.sh k-inspect Order.OrderCreated

# CDC event (Debezium envelope, per-table topic)
./dev.sh k-inspect cdc.shop.orders
```

## Variations

### PostgreSQL with WAL Messages

PostgreSQL doesn't need an outbox table - write directly to the WAL:

```sql
BEGIN;
INSERT INTO orders (customer, total, status) VALUES ('alice', 149.99, 'pending');
SELECT pg_logical_emit_message(
  true, 'outbox',
  '{"aggregate_type":"Order","aggregate_id":"1","event_type":"OrderCreated","payload":{"customer":"alice","total":149.99}}'
);
COMMIT;
```

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

No table, no index, no vacuum - just a WAL entry.

### Multiple Outbox Channels

Route order events and payment events to different topic hierarchies:

```yaml
processors:
  - type: outbox
    tables: [orders_outbox]
    topic: "orders.${event_type}"
    raw_payload: true

  - type: outbox
    tables: [payments_outbox]
    topic: "payments.${event_type}"
    raw_payload: true
    columns:
      payload: data
```

### Additional Headers for Tracing

Forward trace and correlation IDs as Kafka headers:

```yaml
processors:
  - type: outbox
    topic: "${aggregate_type}.${event_type}"
    raw_payload: true
    additional_headers:
      x-trace-id: trace_id
      x-correlation-id: correlation_id
```

```sql
INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload, trace_id, correlation_id)
VALUES ('Order', '42', 'OrderCreated', '{"total":99.99}', 'abc-123', 'req-456');
```

### Migrating from Debezium

If your outbox table uses Debezium's column names (`aggregatetype`, `aggregateid`, `type`):

```yaml
processors:
  - type: outbox
    topic: "${aggregatetype}.${type}"
    raw_payload: true
    columns:
      aggregate_type: aggregatetype
      aggregate_id: aggregateid
      event_type: type
    additional_headers:
      x-trace-id: traceid
      x-tenant: tenant
```

Column mappings control header extraction. The topic template uses raw column names directly.

## Key Concepts Demonstrated

- **Transactional outbox**: atomicity without distributed transactions
- **BLACKHOLE engine**: binlog-only storage for zero-cost outbox tables on MySQL
- **Raw payload delivery**: `raw_payload: true` bypasses envelope wrapping - consumers get exactly what the application wrote
- **Mixed pipeline**: outbox events and CDC events coexist in the same pipeline with different serialization
- **Per-aggregate routing**: topic template routes events by `aggregate_type` and `event_type`

## Related Documentation

- [Outbox Pattern](../outbox.md) - Full outbox reference with configuration details
- [MySQL Source](../sources/mysql.md) - Binlog prerequisites
- [PostgreSQL Source](../sources/postgres.md) - WAL message setup
- [Dynamic Routing](../routing.md) - Template syntax for topic/key resolution
- [Envelopes](../envelopes.md) - Native, Debezium, CloudEvents formats