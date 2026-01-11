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

Captures row-level changes via binlog replication. See [MySQL source documentation](mysql.md) for prerequisites and detailed configuration.

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
- `db.*` - all tables in database
- `%.table` - table in any database

### Turso

<table>
<tr>
<td>

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

</td>
<td>

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | string | — | Logical identifier |
| `url` | string | — | Database URL |
| `auth_token` | string | — | Turso cloud token |
| `tables` | array | — | Tables to track |
| `cdc_mode` | string | `auto` | `native`, `triggers`, `polling`, `auto` |
| `poll_interval_ms` | int | `1000` | Polling interval |
| `native_cdc.level` | string | `data` | `binlog` or `data` |

</td>
</tr>
</table>

### <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/postgresql/postgresql-original.svg" width="24" height="24" style="vertical-align: middle;"> PostgreSQL

Captures row-level changes via logical replication using the pgoutput plugin. See [PostgreSQL source documentation](postgres.md) for prerequisites and detailed configuration.

<table>
<tr>
<td>

```yaml
source:
  type: postgres
  config:
    id: orders-postgres
    dsn: ${POSTGRES_DSN}
    slot: deltaforge_orders
    publication: orders_pub
    tables:
      - public.orders
      - public.order_items
    start_position: earliest
```

</td>
<td>

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | string | — | Unique identifier |
| `dsn` | string | — | PostgreSQL connection string |
| `slot` | string | — | Replication slot name |
| `publication` | string | — | Publication name |
| `tables` | array | — | Table patterns to capture |
| `start_position` | string | `earliest` | `earliest`, `latest`, or `{lsn: "X/Y"}` |

</td>
</tr>
</table>

**Table patterns** support SQL LIKE syntax (same as MySQL):
- `schema.table` - exact match
- `schema.prefix%` - tables matching prefix
- `schema.*` - all tables in schema
- `%.table` - table in any schema
- `table` - defaults to `public.table`

---

## Processors

Processors transform batches of events before delivery to sinks.

### <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/javascript/javascript-original.svg" width="24" height="24" style="vertical-align: middle;"> JavaScript

<table>
<tr>
<td>

```yaml
processors:
  - type: javascript
    id: transform
    inline: |
      function processBatch(events) {
        return events.map(event => {
          event.tags = ["processed"];
          return event;
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
| `inline` | string | JS source defining `processBatch(events)` |
| `limits.cpu_ms` | int | CPU time limit per batch |
| `limits.mem_mb` | int | Memory limit |
| `limits.timeout_ms` | int | Execution timeout |

</td>
</tr>
</table>

The `processBatch(events)` function receives an array of events and can return:
- An array of events (modified, filtered, or expanded)
- A single event object (wrapped in array automatically)
- `null` or `undefined` to use the mutated input array
- Empty array `[]` to drop all events

---

## Sinks

### <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/apachekafka/apachekafka-original.svg" width="24" height="24" style="vertical-align: middle;"> Kafka

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
      required: true
      exactly_once: false
      client_conf:
        message.timeout.ms: "5000"
```

</td>
<td>

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | string | — | Sink identifier |
| `brokers` | string | — | Comma-separated broker list |
| `topic` | string | — | Destination topic |
| `required` | bool | `true` | Whether this sink gates checkpoints |
| `exactly_once` | bool | `false` | Enable EOS semantics |
| `client_conf` | map | `{}` | Raw librdkafka config overrides |

</td>
</tr>
</table>

### <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/redis/redis-original.svg" width="24" height="24" style="vertical-align: middle;"> Redis

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
      required: true
```

</td>
<td>

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | string | — | Sink identifier |
| `uri` | string | — | Redis connection URI |
| `stream` | string | — | Redis stream key |
| `required` | bool | `true` | Whether this sink gates checkpoints |

</td>
</tr>
</table>

### <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/nats/nats-original.svg" width="24" height="24" style="vertical-align: middle;"> NATS

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

## Complete example

### MySQL to Kafka

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

### PostgreSQL to Kafka

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
        required: true

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
        required: true

  batch:
    max_events: 500
    max_ms: 1000
    respect_source_tx: true

  commit_policy:
    mode: required
```