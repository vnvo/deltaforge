# Configuration

Pipelines are defined as YAML documents that map directly to the `PipelineSpec` type. Environment variables are expanded before parsing using `${VAR}` syntax, so secrets and connection strings can be injected at runtime.

## Document structure

```yaml
apiVersion: deltaforge/v1
kind: Pipeline
metadata:
  name: <pipeline-name>
  tenant: <tenant-id>
spec:
  source: { ... }
  processors: [ ... ]
  sinks: [ ... ]
  batch: { ... }
  commit_policy: { ... }
  schema_sensing: { ... }
```

## Metadata

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Unique pipeline identifier. Used in API routes and metrics. |
| `tenant` | string | Yes | Business-oriented tenant label for multi-tenancy. |

## Spec fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source` | object | Yes | Database source configuration. See [Sources](#sources). |
| `processors` | array | No | Ordered list of processors. See [Processors](#processors). |
| `sinks` | array | Yes (at least one) | One or more sinks that receive each batch. See [Sinks](#sinks). |
| `sharding` | object | No | Optional hint for downstream distribution. |
| `connection_policy` | object | No | How the runtime establishes upstream connections. |
| `batch` | object | No | Commit unit thresholds. See [Batching](#batching). |
| `commit_policy` | object | No | How sink acknowledgements gate checkpoints. See [Commit policy](#commit-policy). |
| `schema_sensing` | object | No | Automatic schema inference from event payloads. See [Schema sensing](#schema-sensing). |

---

## Sources

### MySQL

```yaml
source:
  type: mysql
  config:
    id: orders-mysql
    dsn: ${MYSQL_DSN}
    tables:
      - shop.orders
      - shop.order_items
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | Yes | Logical identifier for metrics and logging |
| `dsn` | string | Yes | MySQL connection string |
| `tables` | array | Yes | Tables to capture (supports wildcards like `shop.order%`) |

### Turso

```yaml
source:
  type: turso
  config:
    id: turso-main
    url: "libsql://your-db.turso.io"
    auth_token: ${TURSO_AUTH_TOKEN}
    tables: ["users", "orders"]
    cdc_mode: auto
    poll_interval_ms: 1000
    native_cdc:
      level: data
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `id` | string | Yes | — | Logical identifier for metrics and logging |
| `url` | string | Yes | — | Database URL (`libsql://`, `http://`, or file path) |
| `auth_token` | string | No | — | Authentication token for Turso cloud |
| `tables` | array | Yes | — | Tables to track (supports wildcards) |
| `cdc_mode` | string | No | `auto` | CDC mode: `native`, `triggers`, `polling`, `auto` |
| `poll_interval_ms` | integer | No | `1000` | Polling interval in milliseconds |
| `native_cdc.level` | string | No | `data` | Native CDC level: `binlog` or `data` |

### PostgreSQL

> **Note:** PostgreSQL configuration is parsed but the source implementation is not yet complete.

```yaml
source:
  type: postgres
  config:
    id: pg-main
    dsn: ${POSTGRES_DSN}
    publication: my_pub
    slot: my_slot
    tables:
      - public.orders
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | Yes | Logical identifier |
| `dsn` | string | Yes | PostgreSQL connection string |
| `publication` | string | No | Logical replication publication name |
| `slot` | string | No | Replication slot name |
| `tables` | array | Yes | Tables to capture |

---

## Processors

Processors transform batches of events before delivery to sinks.

### JavaScript

```yaml
processors:
  - type: javascript
    id: transform
    inline: |
      function processBatch(events) {
        return events.map(event => {
          event.tags = (event.tags || []).concat(["processed"]);
          return event;
        });
      }
    limits:
      cpu_ms: 50
      mem_mb: 128
      timeout_ms: 500
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | Yes | Processor identifier |
| `inline` | string | Yes | JavaScript source defining `processBatch(events)` |
| `limits.cpu_ms` | integer | No | CPU time limit per batch |
| `limits.mem_mb` | integer | No | Memory limit |
| `limits.timeout_ms` | integer | No | Execution timeout |

The `processBatch(events)` function receives an array of events and can return:
- An array of events (modified, filtered, or expanded)
- A single event object (wrapped in array automatically)
- `null` or `undefined` to use the mutated input array
- Empty array `[]` to drop all events

---

## Sinks

### Kafka

```yaml
sinks:
  - type: kafka
    config:
      id: orders-kafka
      brokers: ${KAFKA_BROKERS}
      topic: orders
      required: true
      exactly_once: false
      client_conf:
        message.timeout.ms: "5000"
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `id` | string | Yes | — | Sink identifier |
| `brokers` | string | Yes | — | Comma-separated broker list |
| `topic` | string | Yes | — | Destination topic |
| `required` | bool | No | `true` | Whether this sink gates checkpoints |
| `exactly_once` | bool | No | `false` | Enable EOS semantics |
| `client_conf` | map | No | `{}` | Raw librdkafka configuration overrides |

### Redis

```yaml
sinks:
  - type: redis
    config:
      id: orders-redis
      uri: ${REDIS_URI}
      stream: orders
      required: true
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `id` | string | Yes | — | Sink identifier |
| `uri` | string | Yes | — | Redis connection URI |
| `stream` | string | Yes | — | Redis stream key |
| `required` | bool | No | `true` | Whether this sink gates checkpoints |

---

## Batching

```yaml
batch:
  max_events: 500
  max_bytes: 1048576
  max_ms: 1000
  respect_source_tx: true
  max_inflight: 2
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_events` | integer | `500` | Flush after this many events |
| `max_bytes` | integer | `1048576` | Flush after serialized size reaches this limit |
| `max_ms` | integer | `1000` | Flush after this much time (milliseconds) |
| `respect_source_tx` | bool | `true` | Never split a source transaction across batches |
| `max_inflight` | integer | `2` | Maximum concurrent batches being processed |

---

## Commit policy

```yaml
commit_policy:
  mode: required
```

| Mode | Description |
|------|-------------|
| `all` | Every sink must acknowledge before checkpoint |
| `required` | Only sinks with `required: true` must acknowledge (default) |
| `quorum` | Checkpoint after `quorum` sinks acknowledge |

For quorum mode:
```yaml
commit_policy:
  mode: quorum
  quorum: 2
```

---

## Schema sensing

```yaml
schema_sensing:
  enabled: true
  deep_inspect:
    enabled: true
    max_depth: 3
    max_sample_size: 500
  sampling:
    warmup_events: 50
    sample_rate: 5
    structure_cache: true
    structure_cache_size: 50
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `false` | Enable schema sensing |
| `deep_inspect.enabled` | bool | `true` | Inspect nested JSON structures |
| `deep_inspect.max_depth` | integer | `10` | Maximum nesting depth |
| `deep_inspect.max_sample_size` | integer | `1000` | Max events to sample for deep analysis |
| `sampling.warmup_events` | integer | `1000` | Events to fully analyze before sampling |
| `sampling.sample_rate` | integer | `10` | After warmup, analyze 1 in N events |
| `sampling.structure_cache` | bool | `true` | Cache structure fingerprints |
| `sampling.structure_cache_size` | integer | `100` | Max cached structures per table |

---

## Complete example

```yaml
apiVersion: deltaforge/v1
kind: Pipeline
metadata:
  name: orders-mysql-to-kafka
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
      id: transform
      inline: |
        function processBatch(events) {
          return events.map(event => {
            event.tags = (event.tags || []).concat(["normalized"]);
            return event;
          });
        }
      limits:
        cpu_ms: 50
        mem_mb: 128
        timeout_ms: 500

  sinks:
    - type: kafka
      config:
        id: orders-kafka
        brokers: ${KAFKA_BROKERS}
        topic: orders
        required: true
        exactly_once: false
        client_conf:
          message.timeout.ms: "5000"
    - type: redis
      config:
        id: orders-redis
        uri: ${REDIS_URI}
        stream: orders
        required: false

  batch:
    max_events: 500
    max_bytes: 1048576
    max_ms: 1000
    respect_source_tx: true
    max_inflight: 2

  commit_policy:
    mode: required

  schema_sensing:
    enabled: true
    deep_inspect:
      enabled: true
      max_depth: 3
    sampling:
      warmup_events: 50
      sample_rate: 5
```