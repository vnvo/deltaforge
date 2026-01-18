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

### <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/mysql/mysql-original.svg" width="24" height="24" style="vertical-align: middle;"> MySQL

Captures row-level changes via binlog replication. See [MySQL source documentation](sources/mysql.md) for prerequisites and detailed configuration.

<table>
<tr>
<td>

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

</td>
<td>

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Unique identifier for checkpoints and metrics |
| `dsn` | string | MySQL connection string with replication privileges |
| `tables` | array | Table patterns to capture; omit for all tables |

</td>
</tr>
</table>

**Table patterns** support SQL LIKE syntax:
- `db.table` - exact match
- `db.prefix%` - tables matching prefix
- `db.%` - all tables in database

### <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/postgresql/postgresql-original.svg" width="24" height="24" style="vertical-align: middle;"> PostgreSQL

Captures row-level changes via logical replication. See [PostgreSQL source documentation](sources/postgres.md) for prerequisites and detailed configuration.

<table>
<tr>
<td>

```yaml
source:
  type: postgres
  config:
    id: users-postgres
    dsn: ${POSTGRES_DSN}
    slot: deltaforge_users
    publication: users_pub
    tables:
      - public.users
      - public.sessions
    start_position: earliest
```

</td>
<td>

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Unique identifier |
| `dsn` | string | PostgreSQL connection string |
| `slot` | string | Replication slot name |
| `publication` | string | Publication name |
| `tables` | array | Table patterns to capture |
| `start_position` | string | `earliest`, `latest`, or `lsn` |

</td>
</tr>
</table>

---

## Processors

Processors transform events between source and sinks. They run in order and can filter, enrich, or modify events.

### JavaScript

<table>
<tr>
<td>

```yaml
processors:
  - type: javascript
    id: transform
    inline: |
      function processBatch(events) {
        return events.map(e => {
          e.tags = ["processed"];
          return e;
        });
      }
    limits:
      cpu_ms: 50
      mem_mb: 128
      timeout_ms: 500
```

</td>
<td>

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Processor identifier |
| `inline` | string | JavaScript code |
| `limits.cpu_ms` | int | CPU time limit |
| `limits.mem_mb` | int | Memory limit |
| `limits.timeout_ms` | int | Execution timeout |

</td>
</tr>
</table>

---

## Sinks

Sinks deliver events to downstream systems. Each sink supports configurable [envelope formats and wire encodings](envelopes.md) to match consumer expectations. See the [Sinks documentation](sinks/README.md) for detailed information on multi-sink patterns, commit policies, and failure handling.

### Envelope and encoding

All sinks support these serialization options:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `envelope` | object | `native` | Output structure format. See [Envelopes](envelopes.md). |
| `encoding` | string | `json` | Wire encoding format |

**Envelope types:**
- `native` - Direct Debezium payload structure (default, most efficient)
- `debezium` - Full `{"payload": ...}` wrapper
- `cloudevents` - CloudEvents 1.0 specification (requires `type_prefix`)

```yaml
# Native (default)
envelope:
  type: native

# Debezium wrapper
envelope:
  type: debezium

# CloudEvents
envelope:
  type: cloudevents
  type_prefix: "com.example.cdc"
```

### <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/apachekafka/apachekafka-original.svg" width="24" height="24" style="vertical-align: middle;"> Kafka

See [Kafka sink documentation](sinks/kafka.md) for detailed configuration options and best practices.

<table>
<tr>
<td>

```yaml
sinks:
  - type: kafka
    config:
      id: orders-kafka
      brokers: ${KAFKA_BROKERS}
      topic: orders
      envelope:
        type: debezium
      encoding: json
      required: true
      exactly_once: false
      send_timeout_secs: 30
      client_conf:
        security.protocol: SASL_SSL
```

</td>
<td>

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | string | — | Sink identifier |
| `brokers` | string | — | Kafka broker addresses |
| `topic` | string | — | Target topic |
| `envelope` | object | `native` | Output format |
| `encoding` | string | `json` | Wire encoding |
| `required` | bool | `true` | Gates checkpoints |
| `exactly_once` | bool | `false` | Transactional mode |
| `send_timeout_secs` | int | `30` | Send timeout |
| `client_conf` | map | — | librdkafka overrides |

</td>
</tr>
</table>

**CloudEvents example:**

```yaml
sinks:
  - type: kafka
    config:
      id: events-kafka
      brokers: ${KAFKA_BROKERS}
      topic: events
      envelope:
        type: cloudevents
        type_prefix: "com.acme.cdc"
      encoding: json
```

### <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/redis/redis-original.svg" width="24" height="24" style="vertical-align: middle;"> Redis

See [Redis sink documentation](sinks/redis.md) for detailed configuration options and best practices.

<table>
<tr>
<td>

```yaml
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
```

</td>
<td>

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | string | — | Sink identifier |
| `uri` | string | — | Redis connection URI |
| `stream` | string | — | Redis stream key |
| `envelope` | object | `native` | Output format |
| `encoding` | string | `json` | Wire encoding |
| `required` | bool | `true` | Gates checkpoints |
| `send_timeout_secs` | int | `5` | XADD timeout |
| `batch_timeout_secs` | int | `30` | Pipeline timeout |
| `connect_timeout_secs` | int | `10` | Connection timeout |

</td>
</tr>
</table>

### <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/nats/nats-original.svg" width="24" height="24" style="vertical-align: middle;"> NATS

See [NATS sink documentation](sinks/nats.md) for detailed configuration options and best practices.

<table>
<tr>
<td>

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
      required: true
      send_timeout_secs: 5
      batch_timeout_secs: 30
```

</td>
<td>

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | string | — | Sink identifier |
| `url` | string | — | NATS server URL |
| `subject` | string | — | Subject to publish to |
| `stream` | string | — | JetStream stream name |
| `envelope` | object | `native` | Output format |
| `encoding` | string | `json` | Wire encoding |
| `required` | bool | `true` | Gates checkpoints |
| `send_timeout_secs` | int | `5` | Publish timeout |
| `batch_timeout_secs` | int | `30` | Batch timeout |
| `connect_timeout_secs` | int | `10` | Connection timeout |
| `credentials_file` | string | — | NATS credentials file |
| `username` | string | — | Auth username |
| `password` | string | — | Auth password |
| `token` | string | — | Auth token |

</td>
</tr>
</table>

---

## Batching

<table>
<tr>
<td>

```yaml
batch:
  max_events: 500
  max_bytes: 1048576
  max_ms: 1000
  respect_source_tx: true
  max_inflight: 2
```

</td>
<td>

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_events` | int | `500` | Flush after this many events |
| `max_bytes` | int | `1048576` | Flush after size reaches limit |
| `max_ms` | int | `1000` | Flush after time (ms) |
| `respect_source_tx` | bool | `true` | Never split source transactions |
| `max_inflight` | int | `2` | Max concurrent batches |

</td>
</tr>
</table>

---

## Commit policy

<table>
<tr>
<td>

```yaml
commit_policy:
  mode: required

# For quorum mode:
commit_policy:
  mode: quorum
  quorum: 2
```

</td>
<td>

| Mode | Description |
|------|-------------|
| `all` | Every sink must acknowledge before checkpoint |
| `required` | Only `required: true` sinks must acknowledge (default) |
| `quorum` | Checkpoint after `quorum` sinks acknowledge |

</td>
</tr>
</table>

---

## Schema sensing

Schema sensing automatically infers and tracks schema from event payloads. See the [Schema Sensing documentation](schemasensing.md) for detailed information on how it works, drift detection, and API endpoints.

> **Performance tip**: Schema sensing can be CPU-intensive, especially with deep JSON inspection. Consider your throughput requirements when configuring:
> - Set `enabled: false` if you don't need runtime schema inference
> - Limit `deep_inspect.max_depth` to avoid traversing deeply nested structures
> - Increase `sampling.sample_rate` to analyze fewer events (e.g., 1 in 100 instead of 1 in 10)
> - Reduce `sampling.warmup_events` if you're confident in schema stability

<table>
<tr>
<td>

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

</td>
<td>

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `false` | Enable schema sensing |
| `deep_inspect.enabled` | bool | `true` | Inspect nested JSON |
| `deep_inspect.max_depth` | int | `10` | Max nesting depth |
| `deep_inspect.max_sample_size` | int | `1000` | Max events for deep analysis |
| `sampling.warmup_events` | int | `1000` | Events to fully analyze first |
| `sampling.sample_rate` | int | `10` | After warmup, analyze 1 in N |
| `sampling.structure_cache` | bool | `true` | Cache structure fingerprints |
| `sampling.structure_cache_size` | int | `100` | Max cached structures |

</td>
</tr>
</table>

---

## Complete examples

### MySQL to Kafka with Debezium envelope

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
        envelope:
          type: debezium
        encoding: json
        required: true
        exactly_once: false
        client_conf:
          message.timeout.ms: "5000"

  batch:
    max_events: 500
    max_bytes: 1048576
    max_ms: 1000
    respect_source_tx: true
    max_inflight: 2

  commit_policy:
    mode: required
```

### PostgreSQL to Kafka with CloudEvents

```yaml
apiVersion: deltaforge/v1
kind: Pipeline
metadata:
  name: users-postgres-to-kafka
  tenant: acme

spec:
  source:
    type: postgres
    config:
      id: users-postgres
      dsn: ${POSTGRES_DSN}
      slot: deltaforge_users
      publication: users_pub
      tables:
        - public.users
        - public.user_sessions
      start_position: earliest

  sinks:
    - type: kafka
      config:
        id: users-kafka
        brokers: ${KAFKA_BROKERS}
        topic: user-events
        envelope:
          type: cloudevents
          type_prefix: "com.acme.users"
        encoding: json
        required: true

  batch:
    max_events: 500
    max_ms: 1000
    respect_source_tx: true

  commit_policy:
    mode: required
```

### Multi-sink with different formats

```yaml
apiVersion: deltaforge/v1
kind: Pipeline
metadata:
  name: orders-multi-sink
  tenant: acme

spec:
  source:
    type: mysql
    config:
      id: orders-mysql
      dsn: ${MYSQL_DSN}
      tables:
        - shop.orders

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

    # Redis cache uses native format
    - type: redis
      config:
        id: cache-redis
        uri: ${REDIS_URI}
        stream: orders-cache
        envelope:
          type: native
        required: false

  batch:
    max_events: 500
    max_ms: 1000
    respect_source_tx: true

  commit_policy:
    mode: required
```

### MySQL to NATS

```yaml
apiVersion: deltaforge/v1
kind: Pipeline
metadata:
  name: orders-mysql-to-nats
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
        required: true

  batch:
    max_events: 500
    max_ms: 1000
    respect_source_tx: true

  commit_policy:
    mode: required
```