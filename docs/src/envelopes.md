# Envelopes and Encodings

DeltaForge supports configurable **envelope formats** and **wire encodings** for sink output. This allows you to match the output format expected by your downstream consumers without forcing code changes on them.

## Overview

Every CDC event flows through two stages before being written to a sink:

```
Event -> Envelope (structure) -> Encoding (bytes) -> Sink
```

- **Envelope**: Controls the JSON structure of the output (what fields exist, how they're nested)
- **Encoding**: Controls the wire format (JSON bytes, future: Avro, Protobuf)

## Envelope Formats

### Native (default)

The native envelope serializes events directly with minimal overhead. This is DeltaForge's own format, optimized for efficiency and practical use cases.

>[!NOTE] 
> The native envelope format may evolve over time as we adapt to user needs and optimize for the lowest possible overhead. If you need a stable, standardized format, consider using `debezium` or `cloudevents` envelopes which follow their respective established specifications.

```yaml
sinks:
  - type: kafka
    config:
      id: events-kafka
      brokers: localhost:9092
      topic: events
      envelope:
        type: native
```

**Output:**

```json
{
  "before": null,
  "after": {"id": 1, "name": "Alice", "email": "alice@example.com"},
  "source": {
    "version": "0.1.0",
    "connector": "mysql",
    "name": "orders-db",
    "ts_ms": 1700000000000,
    "db": "shop",
    "table": "customers",
    "server_id": 1,
    "file": "mysql-bin.000003",
    "pos": 12345
  },
  "op": "c",
  "ts_ms": 1700000000000
}
```

**When to use:**
- Maximum performance with lowest overhead
- Custom consumers that parse the payload directly
- When format stability is less important than efficiency
- Internal systems where you control both producer and consumer

### Debezium

The Debezium envelope wraps the event in a `{"schema": null, "payload": ...}` structure, 
following the [Debezium event format specification](https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-events). 

This uses **schemaless mode** (`schema: null`), which is equivalent to Debezium's 
`JsonConverter` with `schemas.enable=false`. This is the recommended configuration 
for most production deployments as it avoids the overhead of inline schemas.
```yaml
sinks:
  - type: kafka
    config:
      id: events-kafka
      brokers: localhost:9092
      topic: events
      envelope:
        type: debezium
```

**Output:**
```json
{
  "schema": null,
  "payload": {
    "before": null,
    "after": {"id": 1, "name": "Alice", "email": "alice@example.com"},
    "source": {
      "version": "0.1.0",
      "connector": "mysql",
      "name": "orders-db",
      "ts_ms": 1700000000000,
      "db": "shop",
      "table": "customers"
    },
    "op": "c",
    "ts_ms": 1700000000000
  }
}
```

**When to use:**
- Kafka Connect consumers expecting full Debezium format
- Existing Debezium-based pipelines you're migrating from
- Tools that specifically parse the `payload` wrapper
- When you need a stable, well-documented format with broad ecosystem support

> **Note:** For Schema Registry integration with Avro encoding (planned), schema handling 
> will move to the encoding layer where schema IDs are embedded in the wire format.

### CloudEvents

The CloudEvents envelope restructures events to the [CloudEvents 1.0 specification](https://cloudevents.io/), a CNCF project that defines a vendor-neutral format for event data. This format strictly follows the CloudEvents spec and is guaranteed to remain compliant.

```yaml
sinks:
  - type: kafka
    config:
      id: events-kafka
      brokers: localhost:9092
      topic: events
      envelope:
        type: cloudevents
        type_prefix: "com.example.cdc"
```

**Output:**

```json
{
  "specversion": "1.0",
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "source": "deltaforge/orders-db/shop.customers",
  "type": "com.example.cdc.created",
  "time": "2024-01-15T10:30:00.000Z",
  "datacontenttype": "application/json",
  "subject": "shop.customers",
  "data": {
    "before": null,
    "after": {"id": 1, "name": "Alice", "email": "alice@example.com"},
    "op": "c"
  }
}
```

The `type` field is constructed from your `type_prefix` plus the operation:
- `com.example.cdc.created` (INSERT)
- `com.example.cdc.updated` (UPDATE)
- `com.example.cdc.deleted` (DELETE)
- `com.example.cdc.snapshot` (READ/snapshot)
- `com.example.cdc.truncated` (TRUNCATE)

**When to use:**
- AWS EventBridge, Azure Event Grid, or other CloudEvents-native platforms
- Serverless architectures (Lambda, Cloud Functions)
- Event-driven microservices using CloudEvents SDKs
- Standardized event routing based on `type` field
- When you need a vendor-neutral, CNCF-backed standard format

## Wire Encodings

### JSON (default)

Standard UTF-8 JSON encoding. Human-readable and widely supported.

```yaml
sinks:
  - type: kafka
    config:
      id: events-kafka
      brokers: localhost:9092
      topic: events
      encoding: json
```

**Content-Type:** `application/json`

**When to use:**
- Development and debugging
- Consumers that expect JSON
- When human readability matters
- Most use cases (good default)

### Future: Avro

> **Coming soon**: Avro encoding with Schema Registry integration for compact binary serialization and schema evolution support.

```yaml
# Future configuration (not yet implemented)
sinks:
  - type: kafka
    config:
      id: events-kafka
      brokers: localhost:9092
      topic: events
      encoding:
        type: avro
        schema_registry: http://schema-registry:8081
```

## Configuration Examples

### Kafka with CloudEvents

```yaml
sinks:
  - type: kafka
    config:
      id: orders-kafka
      brokers: ${KAFKA_BROKERS}
      topic: order-events
      envelope:
        type: cloudevents
        type_prefix: "com.acme.orders"
      encoding: json
      required: true
```

### Redis with Debezium envelope

```yaml
sinks:
  - type: redis
    config:
      id: orders-redis
      uri: ${REDIS_URI}
      stream: orders
      envelope:
        type: debezium
      encoding: json
```

### NATS with native envelope

```yaml
sinks:
  - type: nats
    config:
      id: orders-nats
      url: ${NATS_URL}
      subject: orders.events
      stream: ORDERS
      envelope:
        type: native
      encoding: json
```

### Multi-sink with different formats

Different consumers may expect different formats. Configure each sink independently:

```yaml
sinks:
  # Kafka Connect expects Debezium format
  - type: kafka
    config:
      id: connect-sink
      brokers: ${KAFKA_BROKERS}
      topic: connect-events
      envelope:
        type: debezium
      required: true

  # Lambda expects CloudEvents
  - type: kafka
    config:
      id: lambda-sink
      brokers: ${KAFKA_BROKERS}
      topic: lambda-events
      envelope:
        type: cloudevents
        type_prefix: "com.acme.cdc"
      required: false

  # Analytics wants raw events
  - type: redis
    config:
      id: analytics-redis
      uri: ${REDIS_URI}
      stream: analytics
      envelope:
        type: native
```

## Operation Mapping

DeltaForge uses Debezium-compatible operation codes:

| Operation | Code | Description |
|-----------|------|-------------|
| Create/Insert | `c` | New row inserted |
| Update | `u` | Existing row modified |
| Delete | `d` | Row deleted |
| Read | `r` | Snapshot read (initial load) |
| Truncate | `t` | Table truncated |

These codes appear in the `op` field regardless of envelope format.

## Performance Considerations

| Envelope | Overhead | Format Stability | Use Case |
|----------|----------|------------------|----------|
| Native | Baseline (minimal) | May evolve | High-throughput, internal systems |
| Debezium | ~14 bytes | Stable (follows Debezium spec) | Kafka Connect, Debezium ecosystem |
| CloudEvents | ~150-200 bytes | Stable (follows CNCF spec) | Serverless, event-driven architectures |

The native envelope is recommended for maximum throughput when you control both ends of the pipeline. For interoperability with external systems or when format stability is critical, use `debezium` or `cloudevents`.

## Defaults

If not specified, sinks use:
- **Envelope**: `native`
- **Encoding**: `json`

The native envelope provides the lowest overhead for high-throughput scenarios. If you need format stability guarantees, use `debezium` or `cloudevents` which adhere to their respective established specifications.