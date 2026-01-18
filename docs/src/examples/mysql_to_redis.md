# MySQL to Redis

This example streams MySQL binlog events into a Redis stream with an inline JavaScript transformation for PII redaction.

## Overview

| Component | Configuration |
|-----------|---------------|
| **Source** | MySQL binlog CDC |
| **Processor** | JavaScript email redaction |
| **Sink** | Redis Streams |
| **Envelope** | Native (configurable) |

## Pipeline Configuration

```yaml
metadata:
  name: orders-mysql-to-redis
  tenant: acme

spec:
  source:
    type: mysql
    config:
      id: orders-mysql
      dsn: ${MYSQL_DSN}
      tables:
        - shop.orders

  processors:
    - type: javascript
      id: redact-email
      inline: |
        function processBatch(events) {
          return events.map((event) => {
            if (event.after && event.after.email) {
              event.after.email = "[redacted]";
            }
            return event;
          });
        }
      limits:
        timeout_ms: 500

  sinks:
    - type: redis
      config:
        id: orders-redis
        uri: ${REDIS_URI}
        stream: orders
        envelope:
          type: native
        encoding: json
        required: true

  batch:
    max_events: 500
    max_bytes: 1048576
    max_ms: 1000

  commit_policy:
    mode: required
```

## Running the Example

### 1. Set Environment Variables

```bash
export MYSQL_DSN="mysql://user:password@localhost:3306/shop"
export REDIS_URI="redis://localhost:6379"
```

### 2. Start DeltaForge

```bash
# Save config as mysql-redis.yaml
cargo run -p runner -- --config mysql-redis.yaml
```

### 3. Insert Test Data

```sql
INSERT INTO shop.orders (email, total, status)
VALUES ('alice@example.com', 99.99, 'pending');
```

### 4. Verify in Redis

```bash
./dev.sh redis-read orders 10
```

You should see the event with the email redacted:

```json
{
  "before": null,
  "after": {
    "id": 1,
    "email": "[redacted]",
    "total": 99.99,
    "status": "pending"
  },
  "op": "c",
  "ts_ms": 1700000000000
}
```

## Variations

### With Debezium Envelope

For Kafka Connect compatibility downstream:

```yaml
sinks:
  - type: redis
    config:
      id: orders-redis
      uri: ${REDIS_URI}
      stream: orders
      envelope:
        type: debezium
```

### Multi-Sink Fan-Out

Add Kafka alongside Redis for durability:

```yaml
sinks:
  - type: kafka
    config:
      id: orders-kafka
      brokers: ${KAFKA_BROKERS}
      topic: orders
      envelope:
        type: debezium
      required: true    # Critical path

  - type: redis
    config:
      id: orders-redis
      uri: ${REDIS_URI}
      stream: orders
      envelope:
        type: native
      required: false   # Best-effort
```

With this configuration, checkpoints only wait for Kafka. Redis failures won't block the pipeline.

### With Schema Sensing

Automatically track schema changes:

```yaml
spec:
  # ... source and sinks config ...

  schema_sensing:
    enabled: true
    deep_inspect:
      enabled: true
      max_depth: 3
    sampling:
      warmup_events: 100
      sample_rate: 10
```

## Key Concepts Demonstrated

- **JavaScript Processors**: Transform events in-flight with custom logic
- **PII Redaction**: Mask sensitive data before it reaches downstream systems
- **Envelope Configuration**: Choose output format based on consumer needs
- **Commit Policy**: Control checkpoint behavior with `required` flag

## Related Documentation

- [MySQL Source](../sources/mysql.md) - Prerequisites and configuration
- [Redis Sink](../sinks/redis.md) - Connection options and batching
- [Envelopes](../envelopes.md) - Output format options
- [Configuration Reference](../configuration.md) - Full spec documentation