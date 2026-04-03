# Envelopes and Encodings

DeltaForge supports configurable **envelope formats** and **wire encodings** for sink output. This allows you to match the output format expected by your downstream consumers without forcing code changes on them.

## Overview

Every CDC event flows through two stages before being written to a sink:

```
Event -> Envelope (structure) -> Encoding (bytes) -> Sink
```

- **Envelope**: Controls the JSON structure of the output (what fields exist, how they're nested)
- **Encoding**: Controls the wire format (JSON bytes, Avro binary with Schema Registry)

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

> **Note:** When using Avro encoding with Schema Registry, schema handling is at the encoding layer — schema IDs are embedded in the [Confluent wire format](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format).

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

### Avro (with Schema Registry)

Avro encoding produces compact binary payloads using the [Confluent wire format](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format):

```
[0x00][4-byte schema ID (big-endian)][Avro binary payload]
```

This format is natively understood by Kafka Connect, ksqlDB, Apache Flink, and any Confluent-compatible consumer.

```yaml
sinks:
  - type: kafka
    config:
      id: events-kafka
      brokers: localhost:9092
      topic: events
      encoding:
        type: avro
        schema_registry_url: "http://schema-registry:8081"
        subject_strategy: topic_name   # default
```

**Content-Type:** `application/avro`

#### Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `schema_registry_url` | string | — | Confluent-compatible Schema Registry URL |
| `subject_strategy` | string | `topic_name` | Subject naming strategy (see below) |
| `username` | string | — | Basic auth username for Schema Registry |
| `password` | string | — | Basic auth password for Schema Registry |
| `unsigned_bigint_mode` | string | `string` | How to map MySQL `BIGINT UNSIGNED` (`string` or `long`) |
| `enum_mode` | string | `string` | How to map ENUM types (`string` or `enum`) |
| `naive_timestamp_mode` | string | `string` | How to map naive timestamps (`string` or `timestamp`) |

#### Subject naming strategies

| Strategy | Subject pattern | Use case |
|----------|----------------|----------|
| `topic_name` | `{topic}-value` | One schema per Kafka topic (default, most common) |
| `record_name` | `{record_name}` | One schema per record type, shared across topics |
| `topic_record_name` | `{topic}-{record_name}` | Per-topic, per-record schema |

#### Schema source

When source DDL is available (MySQL `INFORMATION_SCHEMA`, PostgreSQL `pg_catalog`), DeltaForge derives precise Avro schemas from the actual column types and nullability. This is the recommended path for production.

When DDL is not available (e.g., processor-created synthetic events), DeltaForge falls back to inferring the Avro schema from the JSON event structure. This is less precise (no distinction between `int`/`bigint`, all fields nullable).

Schema IDs are cached per subject — only the first event per table triggers a Schema Registry HTTP call.

#### Type conversion policies

DeltaForge defaults to **safe** type mappings that prioritize correctness over convenience:

| Source type | Default Avro type | Why | Override |
|-------------|-------------------|-----|----------|
| MySQL `BIGINT UNSIGNED` | `string` | Values ≥ 2^63 overflow Avro `long` | `unsigned_bigint_mode: long` |
| MySQL/PG `ENUM` | `string` | Avro enum symbol changes break compatibility | `enum_mode: enum` |
| MySQL `DATETIME` | `string` (ISO-8601) | Not a UTC instant — Avro `timestamp-millis` is semantically wrong | `naive_timestamp_mode: timestamp` |
| PG `timestamp` (no tz) | `string` (ISO-8601) | Same as above — naive local time, not an instant | `naive_timestamp_mode: timestamp` |
| MySQL `TIMESTAMP` | `timestamp-millis` | Stored as UTC — safe to use logical type | — |
| PG `timestamptz` | `timestamp-micros` | Stored as UTC — safe to use logical type | — |
| `DECIMAL(p,s)` | `decimal` logical type | Uses exact precision/scale from DDL | — |
| PG `numeric` (no precision) | `string` | Unbounded precision can't be expressed in Avro `decimal` | — |

**Full type mapping tables** for MySQL and PostgreSQL are documented in the [Avro Schema Registry RFC](https://github.com/deltaforge/deltaforge/blob/main/docs/specs/avro-schema-registry.md).

#### Schema evolution

When the source table schema changes (column added/removed/altered), DeltaForge derives a new Avro schema and registers it as a new version. The Schema Registry's compatibility rules control acceptance:

- **Default:** `BACKWARD` — new schema can read old data (consumers can upgrade first)
- DeltaForge respects existing subject compatibility settings in the SR

#### Schema Registry failure handling

| Condition | Behavior |
|-----------|----------|
| SR unavailable, schema cached | Continue encoding with cached schema ID. Metric: `deltaforge_avro_sr_cache_fallback_total` |
| SR unavailable, no cache | Fail the batch — cannot encode without a schema ID |
| SR rejects new schema (compatibility) | Try encoding with cached schema; if encoding fails → DLQ |

#### When to use

- Kafka Connect sinks expecting Avro (JDBC Sink, S3 Sink, Elasticsearch Sink)
- ksqlDB streams and tables
- Apache Flink CDC consumers
- When you need compact binary payloads (~40-60% smaller than JSON)
- When you want schema evolution enforcement via Schema Registry compatibility rules
- Production pipelines where schema governance matters

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

### Kafka with Avro encoding

```yaml
sinks:
  - type: kafka
    config:
      id: events-avro
      brokers: ${KAFKA_BROKERS}
      topic: cdc-events
      envelope:
        type: debezium
      encoding:
        type: avro
        schema_registry_url: "http://schema-registry:8081"
        subject_strategy: topic_name
      required: true
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

| Encoding | Size vs JSON | CPU cost | Schema governance |
|----------|-------------|----------|-------------------|
| JSON | Baseline | Lowest | None |
| Avro | ~40-60% smaller | Moderate (schema lookup + binary encoding) | Schema Registry enforced |

The native envelope is recommended for maximum throughput when you control both ends of the pipeline. For interoperability with external systems or when format stability is critical, use `debezium` or `cloudevents`.

## Defaults

If not specified, sinks use:
- **Envelope**: `native`
- **Encoding**: `json`

The native envelope provides the lowest overhead for high-throughput scenarios. If you need format stability guarantees, use `debezium` or `cloudevents` which adhere to their respective established specifications.

For Kafka pipelines where schema governance and compact payloads matter, use `avro` encoding with the Debezium envelope — this is the standard pattern for production Kafka Connect integration.