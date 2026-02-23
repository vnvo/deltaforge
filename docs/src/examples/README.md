# Examples

Complete pipeline configurations demonstrating common DeltaForge use cases. Each example is ready to run with minimal modifications.

## Available Examples

| Example | Source | Sink(s) | Key Features |
|---------|--------|---------|--------------|
| [MySQL to Redis](mysql_to_redis.md) | MySQL | Redis | JavaScript processor, PII redaction |
| [Turso to Kafka](turso_to_kafka.md) | Turso/libSQL | Kafka | Native CDC, CloudEvents envelope |
| [PostgreSQL to NATS](postgres_to_nats.md) | PostgreSQL | NATS | Logical replication, CloudEvents |
| [Multi-Sink Fan-Out](multi_sink_fanout.md) | MySQL | Kafka + Redis + NATS | Multiple envelopes, selective checkpointing |
| [Event Filtering](event_filtering.md) | MySQL | Kafka | JavaScript filtering, PII redaction |
| [Schema Sensing](schema_sensing.md) | PostgreSQL | Kafka | JSON schema inference, drift detection |
| [Production Kafka](kafka_production.md) | PostgreSQL | Kafka | SASL/SSL auth, exactly-once, tuning |
| [Cache Invalidation](cache_invalidation.md) | MySQL | Redis | CDC stream for cache invalidation workers |
| [Audit Trail](audit_trail.md) | PostgreSQL | Kafka | Compliance logging, PII redaction |
| [Analytics Preprocessing](analytics_preprocessing.md) | MySQL | Kafka + Redis | Metrics enrichment, analytics stream |
| [Outbox Pattern](outbox_mysql_kafka.md) | MySQL | Kafka | Transactional outbox, raw payload, per-aggregate routing |

## Quick Start

1. **Set environment variables** for your database and sink connections
2. **Copy the example** to a `.yaml` file
3. **Run DeltaForge**:
   ```bash
   cargo run -p runner -- --config your-pipeline.yaml
   ```

## Examples by Category

### Getting Started
| Example | Description |
|---------|-------------|
| [MySQL to Redis](mysql_to_redis.md) | Simple pipeline with JavaScript transformation |
| [Turso to Kafka](turso_to_kafka.md) | Edge database to Kafka with CloudEvents |
| [PostgreSQL to NATS](postgres_to_nats.md) | PostgreSQL logical replication to NATS |

### Production Patterns
| Example | Description |
|---------|-------------|
| [Production Kafka](kafka_production.md) | Authentication, exactly-once, performance tuning |
| [Multi-Sink Fan-Out](multi_sink_fanout.md) | Multiple sinks with different formats |
| [Cache Invalidation](cache_invalidation.md) | CDC stream for cache invalidation |
| [Outbox Pattern](outbox_mysql_kafka.md) | Transactional outbox with raw payload delivery |

### Data Processing
| Example | Description |
|---------|-------------|
| [Event Filtering](event_filtering.md) | Filter, drop, and redact events |
| [Schema Sensing](schema_sensing.md) | Automatic JSON schema discovery |
| [Analytics Preprocessing](analytics_preprocessing.md) | Prepare events for analytics platforms |

### Compliance & Auditing
| Example | Description |
|---------|-------------|
| [Audit Trail](audit_trail.md) | SOC2/HIPAA/GDPR-compliant change tracking |

## Examples by Feature

### Envelope Formats

| Format | Example |
|--------|---------|
| Native | [MySQL to Redis](mysql_to_redis.md), [Event Filtering](event_filtering.md), [Cache Invalidation](cache_invalidation.md) |
| Debezium | [Schema Sensing](schema_sensing.md), [Multi-Sink Fan-Out](multi_sink_fanout.md), [Production Kafka](kafka_production.md) |
| CloudEvents | [Turso to Kafka](turso_to_kafka.md), [PostgreSQL to NATS](postgres_to_nats.md) |

### JavaScript Processors

| Use Case | Example |
|----------|---------|
| PII Redaction | [MySQL to Redis](mysql_to_redis.md), [Audit Trail](audit_trail.md) |
| Event Filtering | [Event Filtering](event_filtering.md) |
| Enrichment | [Turso to Kafka](turso_to_kafka.md), [Analytics Preprocessing](analytics_preprocessing.md) |
| Cache Key Generation | [Cache Invalidation](cache_invalidation.md) |
| Audit Metadata | [Audit Trail](audit_trail.md) |

### Multi-Sink Patterns

| Pattern | Example |
|---------|---------|
| Fan-out with different formats | [Multi-Sink Fan-Out](multi_sink_fanout.md) |
| Primary + best-effort secondary | [Multi-Sink Fan-Out](multi_sink_fanout.md), [Analytics Preprocessing](analytics_preprocessing.md) |

## Customizing Examples

### Change Envelope Format

All sinks support configurable envelopes. Add to any sink config:

```yaml
sinks:
  - type: kafka
    config:
      # ... other config
      envelope:
        type: cloudevents          # or: native, debezium
        type_prefix: "com.example" # required for cloudevents
      encoding: json
```

See [Envelopes and Encodings](../envelopes.md) for details.

### Add Multiple Sinks

Fan out to multiple destinations with different formats:

```yaml
sinks:
  - type: kafka
    config:
      id: primary-kafka
      envelope:
        type: debezium
      required: true    # Must succeed for checkpoint
  - type: redis
    config:
      id: cache-redis
      envelope:
        type: native
      required: false   # Best-effort, won't block checkpoint
```

See [Sinks documentation](../sinks/README.md) for multi-sink patterns.

### Enable Schema Sensing

Automatically discover JSON structure in your data:

```yaml
schema_sensing:
  enabled: true
  deep_inspect:
    enabled: true
    max_depth: 3
  sampling:
    warmup_events: 100
    sample_rate: 10
```

See [Schema Sensing](../schemasensing.md) for configuration options.

## More Resources

- [Configuration Reference](../configuration.md) - Full spec documentation
- [Quickstart Guide](../quickstart.md) - Get running in minutes
- [Troubleshooting](../troubleshooting.md) - Common issues and solutions