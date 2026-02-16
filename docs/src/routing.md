# Dynamic Routing

Dynamic routing controls where each CDC event is delivered - which Kafka topic, Redis stream, or NATS subject receives it. **By default, all events go to the single destination configured in the sink (static routing).** With dynamic routing, events can be split across destinations based on their content or other attributes of events, pipeline and etc.

## Overview

There are two routing mechanisms, and they compose naturally:

1. **Template strings** in sink config - resolve per-event from event fields
2. **JavaScript `ev.route()`** - programmatic per-event routing in processors

When both are used, `ev.route()` overrides take highest priority, then template resolution, then the static config value.

## Template Routing

Replace static topic/stream/subject strings with templates containing `${...}` variables. Templates are compiled once at startup and resolved per-event with zero regex overhead.

### Kafka

```yaml
sinks:
  - type: kafka
    config:
      id: kafka-routed
      brokers: ${KAFKA_BROKERS}
      topic: "cdc.${source.db}.${source.table}"
      key: "${after.customer_id}"
      envelope:
        type: debezium
```

Events from `shop.orders` -> topic `cdc.shop.orders`, partitioned by `customer_id`.

### Redis

```yaml
sinks:
  - type: redis
    config:
      id: redis-routed
      uri: ${REDIS_URI}
      stream: "events:${source.table}"
      key: "${after.id}"
```

Events from `orders` -> stream `events:orders`. The `key` value appears as the `df-key` field in each stream entry.

### NATS

```yaml
sinks:
  - type: nats
    config:
      id: nats-routed
      url: ${NATS_URL}
      subject: "cdc.${source.db}.${source.table}"
      key: "${after.id}"
      stream: CDC
```

Events from `shop.orders` -> subject `cdc.shop.orders`. The `key` value appears as the `df-key` NATS header.

### Available Variables

| Variable | Description | Example value |
|----------|-------------|---------------|
| `${source.table}` | Table name | `orders` |
| `${source.db}` | Database name | `shop` |
| `${source.schema}` | Schema name (PostgreSQL) | `public` |
| `${source.connector}` | Source type | `mysql` |
| `${op}` | Operation code | `c`, `u`, `d`, `r`, `t` |
| `${after.<field>}` | Field from after image | `42`, `cust-abc` |
| `${before.<field>}` | Field from before image | `old-value` |
| `${tenant_id}` | Pipeline tenant ID | `acme` |

**Missing fields** resolve to an empty string. A warning is logged once per unique template, not per event.

**Static strings** (no `${...}`) are detected at parse time and have zero overhead on the hot path - no allocation, no resolution.

### Env Vars vs Templates

Both use `${...}` syntax. The config loader expands environment variables first. Unknown variables pass through as templates for runtime resolution:

```yaml
brokers: ${KAFKA_BROKERS}      # env var - expanded at load time
topic: "cdc.${source.table}"   # template - passed through to runtime
key: "${after.customer_id}"    # template - resolved per-event
```

## JavaScript Routing

For routing logic that goes beyond field substitution, use `ev.route()` in a JavaScript processor. This lets you make conditional routing decisions based on event content.

```yaml
processors:
  - type: javascript
    id: smart-router
    inline: |
      function processBatch(events) {
        for (const ev of events) {
          if (!ev.after) continue;

          if (ev.after.total_amount > 10000) {
            ev.route({
              topic: "orders.priority",
              key: String(ev.after.customer_id),
              headers: {
                "x-tier": "high-value",
                "x-amount": String(ev.after.total_amount)
              }
            });
          } else {
            ev.route({
              topic: "orders.standard",
              key: String(ev.after.customer_id)
            });
          }
        }
        return events;
      }
```

### ev.route() fields

| Field | Type | Description |
|-------|------|-------------|
| `topic` | string | Override destination (topic, stream, or subject) |
| `key` | string | Override message/partition key |
| `headers` | object | Key-value pairs added to the message |

All fields are optional. Only set fields override; omitted fields fall through to config templates or static values.

Calling `ev.route()` **replaces** any previous routing on that event - it does not merge.

### How headers are delivered

| Sink | Key delivery | Header delivery |
|------|-------------|-----------------|
| Kafka | Kafka message key | Kafka message headers |
| Redis | `df-key` field in stream entry | `df-headers` field (JSON) |
| NATS | `df-key` NATS header | Individual NATS headers |

## Resolution Order

For each event, the destination is resolved in priority order:

```
ev.route() override  →  config template  →  static config value
```

Specifically:

1. If the event has `routing.topic` set (via `ev.route()` or programmatically), use it
2. If the sink config contains a template (has `${...}`), resolve it from event fields
3. Otherwise, use the static config string

The same order applies independently to `key` and `headers`.

## Examples

See the complete example configurations:

- [Dynamic Routing](examples/dynamic-routing.md) - template-based routing across Kafka, Redis, and NATS
- [JavaScript Routing](examples/dynamic-js-routing.md) - conditional routing with `ev.route()` based on business logic

## Related

- [Configuration Reference](configuration.md) — full sink config fields
- [JavaScript Processors](processors/javascript.md) — processor API reference
- [Sinks Overview](sinks/README.md) — multi-sink patterns and commit policies