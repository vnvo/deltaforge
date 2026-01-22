# Multi-Sink Fan-Out

This example demonstrates streaming changes to multiple destinations simultaneously, each with a different envelope format tailored to its consumers.

## Overview

| Component | Configuration |
|-----------|---------------|
| **Source** | MySQL binlog CDC |
| **Processor** | JavaScript enrichment |
| **Sinks** | Kafka (Debezium) + Redis (Native) + NATS (CloudEvents) |
| **Pattern** | Fan-out with format adaptation |

## Use Case

You have a MySQL database and need to:
- Send to Kafka Connect (requires Debezium format)
- Populate a Redis cache (native format for efficiency)
- Trigger serverless functions via NATS (CloudEvents format)
- Handle sink failures independently without blocking the pipeline

## Pipeline Configuration

```yaml
apiVersion: deltaforge/v1
kind: Pipeline
metadata:
  name: orders-fan-out
  tenant: acme

spec:
  source:
    type: mysql
    config:
      id: orders-mysql
      dsn: ${MYSQL_DSN}
      tables:
        - shop.orders
        - shop.order_items

  processors:
    - type: javascript
      id: enrich
      inline: |
        function processBatch(events) {
          return events.map(event => {
            // Add routing hints via tags (tags is a valid Event field)
            event.tags = event.tags || [];
            
            if (event.table.includes('orders')) {
              event.tags.push('entity:order');
              // High-value orders get priority tag
              if (event.after && event.after.total > 1000) {
                event.tags.push('priority:high');
              }
            }
            
            event.tags.push('enriched');
            return event;
          });
        }
      limits:
        timeout_ms: 500

  sinks:
    # Primary: Kafka for data warehouse / Kafka Connect
    # Uses Debezium format for ecosystem compatibility
    - type: kafka
      config:
        id: warehouse-kafka
        brokers: ${KAFKA_BROKERS}
        topic: orders.cdc
        envelope:
          type: debezium
        encoding: json
        required: true           # Must succeed for checkpoint
        exactly_once: false
        client_conf:
          acks: "all"

    # Secondary: Redis for real-time cache
    # Uses native format for minimal overhead
    - type: redis
      config:
        id: cache-redis
        uri: ${REDIS_URI}
        stream: orders:cache
        envelope:
          type: native
        encoding: json
        required: false          # Best-effort, won't block pipeline

    # Tertiary: NATS for serverless triggers
    # Uses CloudEvents for Lambda/Functions compatibility
    - type: nats
      config:
        id: serverless-nats
        url: ${NATS_URL}
        subject: orders.events
        stream: ORDERS
        envelope:
          type: cloudevents
          type_prefix: "com.acme.orders"
        encoding: json
        required: false          # Best-effort

  batch:
    max_events: 500
    max_bytes: 1048576
    max_ms: 500
    respect_source_tx: true

  commit_policy:
    mode: required    # Only wait for required sinks (Kafka)
```

## How It Works

### Checkpoint Behavior

With `commit_policy.mode: required`:

```
Source Event
    │
    ▼
┌─────────────────────────────────────────────────────┐
│              Parallel Sink Delivery                 │
├─────────────────┬─────────────────┬─────────────────┤
│ Kafka           │ Redis           │ NATS            │
│ required: true  │ required: false │ required: false │
│                 │                 │                 │
│ ✓ Must succeed  │ ✓ Best-effort   │ ✓ Best-effort   │
└────────┬────────┴────────┬────────┴──────┬──────────┘
         │                 │               │
         ▼                 │               │
    Checkpoint <───────────┘               │
    advances                               │
    (even if Redis/NATS fail)              │
```

### Output Formats

**Kafka (Debezium):**
```json
{
  "payload": {
    "before": null,
    "after": {"id": 1, "total": 1500.00, "status": "pending"},
    "source": {"connector": "mysql", "db": "shop", "table": "orders"},
    "op": "c",
    "ts_ms": 1700000000000
  }
}
```

**Redis (Native):**
```json
{
  "before": null,
  "after": {"id": 1, "total": 1500.00, "status": "pending"},
  "source": {"connector": "mysql", "db": "shop", "table": "orders"},
  "op": "c",
  "ts_ms": 1700000000000
}
```

**NATS (CloudEvents):**
```json
{
  "specversion": "1.0",
  "id": "evt-123",
  "source": "deltaforge/orders-mysql/shop.orders",
  "type": "com.acme.orders.created",
  "time": "2025-01-15T10:30:00.000Z",
  "data": {
    "before": null,
    "after": {"id": 1, "total": 1500.00, "status": "pending"},
    "op": "c"
  }
}
```

## Running the Example

### 1. Set Environment Variables

```bash
export MYSQL_DSN="mysql://user:password@localhost:3306/shop"
export KAFKA_BROKERS="localhost:9092"
export REDIS_URI="redis://localhost:6379"
export NATS_URL="nats://localhost:4222"
```

### 2. Start Infrastructure

```bash
./dev.sh up
./dev.sh k-create orders.cdc 6
./dev.sh nats-stream-add ORDERS 'orders.>'
```

### 3. Start DeltaForge

```bash
cargo run -p runner -- --config fan-out.yaml
```

### 4. Insert Test Data

```sql
INSERT INTO shop.orders (customer_id, total, status)
VALUES (1, 1500.00, 'pending');
```

### 5. Verify Each Sink

```bash
# Kafka
./dev.sh k-consume orders.cdc --from-beginning

# Redis
./dev.sh redis-read orders:cache 10

# NATS
./dev.sh nats-sub 'orders.>'
```

## Variations

### Quorum Mode

Require 2 of 3 sinks to succeed:

```yaml
sinks:
  - type: kafka
    config:
      id: kafka-1
      required: true   # Counts toward quorum
  - type: redis
    config:
      id: redis-1
      required: true   # Counts toward quorum
  - type: nats
    config:
      id: nats-1
      required: true   # Counts toward quorum

commit_policy:
  mode: quorum
  quorum: 2            # Any 2 must succeed
```

### All-or-Nothing

Require all sinks to succeed (strongest consistency):

```yaml
commit_policy:
  mode: all
```

> ⚠️ **Warning**: `mode: all` means any sink failure blocks the entire pipeline. Use only when all destinations are equally critical.

## Key Concepts Demonstrated

- **Multi-Sink Fan-Out**: Single source to multiple destinations
- **Format Adaptation**: Different envelope per consumer requirement
- **Selective Checkpointing**: `required` flag controls which sinks gate progress
- **Failure Isolation**: Non-critical sinks don't block the pipeline
- **Tag-Based Enrichment**: Use `event.tags` for routing metadata

> **Processor Constraints**: JavaScript processors can only modify `event.before`, `event.after`, and `event.tags`. Arbitrary top-level fields would be lost during serialization.

## Related Documentation

- [Sinks Overview](../sinks/README.md) - Multi-sink patterns and commit policies
- [Envelopes](../envelopes.md) - Output format options
- [Commit Policy](../configuration.md#commit-policy) - Checkpoint gating modes
- [Configuration Reference](../configuration.md) - Full spec documentation