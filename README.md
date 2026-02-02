<p align="center">
  <img src="assets/deltaforge-blc.png" width="150" alt="DeltaForge">
</p>
<p align="center">
  <a href="https://github.com/vnvo/deltaforge/actions/workflows/ci.yml">
    <img src="https://github.com/vnvo/deltaforge/actions/workflows/ci.yml/badge.svg" alt="CI">
  </a>
  <a href="https://github.com/vnvo/deltaforge/releases">
    <img src="https://img.shields.io/github/v/release/vnvo/deltaforge" alt="Release">
  </a>
  <a href="https://vnvo.github.io/deltaforge">
    <img src="https://img.shields.io/badge/docs-online-blue.svg" alt="Docs">
  </a>
  <a href="https://github.com/vnvo/deltaforge/pkgs/container/deltaforge">
    <img src="https://img.shields.io/badge/ghcr.io-deltaforge-blue?logo=docker" alt="GHCR">
  </a>
  <a href="https://hub.docker.com/r/vnvohub/deltaforge">
    <img src="https://img.shields.io/docker/pulls/vnvohub/deltaforge?logo=docker" alt="Docker Pulls">
  </a>
  <img src="https://img.shields.io/badge/arch-amd64%20%7C%20arm64-green" alt="Arch">
  <a href="https://coveralls.io/github/vnvo/deltaforge?branch=main">
    <img src="https://coveralls.io/repos/github/vnvo/deltaforge/badge.svg?branch=main" alt="Coverage Status">
  </a>
  <img src="https://img.shields.io/badge/rustc-1.89+-orange.svg" alt="MSRV">
  <img src="https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg" alt="License">
</p>

> A modular, efficient and config-driven Change Data Capture (CDC) micro-framework.

> ⚠️ **Status:** Active development. APIs, configuration, and semantics may change.

DeltaForge is a lightweight framework for building CDC pipelines that stream database changes into downstream systems such as Kafka, Redis, and NATS. It focuses on:

- **User Control** : Using an embedded JS engine, users can fully control what happens to each event.
- **Config-driven pipelines** : YAML-defined pipelines instead of bespoke code per use-case.
- **Cloud-Native** : CN first design and operation.
- **Extensibility** : add your own sources, processors, and sinks.

However, DeltaForge is NOT a DAG based stream processor.
DeltaForge is meant to replace tools like Debezium and similar.

## Quick Start

Get DeltaForge running in under 5 minutes:

<table>
<tr>
<td width="50%" valign="top">

### Minimal Pipeline Config
```yaml
# pipeline.yaml
apiVersion: deltaforge/v1
kind: Pipeline
metadata:
  name: my-first-pipeline
  tenant: demo

spec:
  source:
    type: mysql
    config:
      id: mysql-src
      dsn: ${MYSQL_DSN}
      tables: [mydb.users]

  processors: []

  sinks:
    - type: kafka
      config:
        id: kafka-sink
        brokers: ${KAFKA_BROKERS}
        topic: users.cdc
```

</td>
<td width="50%" valign="top">

### Run it with Docker
```bash
docker run --rm \
  -e MYSQL_DSN="mysql://user:pass@host:3306/mydb" \
  -e KAFKA_BROKERS="kafka:9092" \
  -v $(pwd)/pipeline.yaml:/etc/deltaforge/pipeline.yaml:ro \
  ghcr.io/vnvo/deltaforge:latest \
  --config /etc/deltaforge/pipeline.yaml
```

That's it! DeltaForge streams changes from `mydb.users` to Kafka.

**Want Debezium-compatible output?**
```yaml
sinks:
  - type: kafka
    config:
      id: kafka-sink
      brokers: ${KAFKA_BROKERS}
      topic: users.cdc
      envelope:
        type: debezium
```

Output: `{"schema":null,"payload":{...}}`

📘 [Full docs](https://vnvo.github.io/deltaforge) · [Configuration reference](#configuration-schema)

</td>
</tr>
</table>

## The Tech

<table>
  <tr>
    <td align="center" width="140"><b>Built with</b></td>
    <td align="center" width="140"><b>Sources</b></td>
    <td align="center" width="140"><b>Processors</b></td>
    <td align="center" width="140"><b>Sinks</b></td>
    <td align="center" width="140"><b>Output Formats</b></td>
  </tr>
  <tr>
    <td align="center">
      <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/rust/rust-original.svg" width="40" height="40" alt="Rust">
      <br><sub>Rust</sub>
    </td>
    <td align="center">
      <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/mysql/mysql-original.svg" width="40" height="40" alt="MySQL">
      <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/postgresql/postgresql-original.svg" width="40" height="40" alt="PostgreSQL">
      <br><sub>MySQL · PostgreSQL</sub>
    </td>
    <td align="center">
      <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/javascript/javascript-original.svg" width="40" height="40" alt="JavaScript">
      <br><sub>JavaScript</sub>
    </td>
    <td align="center">
      <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/apachekafka/apachekafka-original.svg" width="40" height="40" alt="Kafka">
      <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/redis/redis-original.svg" width="40" height="40" alt="Redis">
      <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/nats/nats-original.svg" width="40" height="40" alt="NATS">
      <br><sub>Kafka · Redis · NATS</sub>
    </td>
    <td align="center">
      <img src="https://img.shields.io/badge/Native-red?style=flat-square" alt="Native">
      <img src="https://img.shields.io/badge/Debezium-green?style=flat-square" alt="Debezium">
      <img src="https://img.shields.io/badge/CloudEvents-blue?style=flat-square" alt="CloudEvents">
    </td>
  </tr>
</table>

## Features

- **Sources**
  - MySQL binlog CDC with GTID support
  - PostgreSQL logical replication via pgoutput
  - Turso/libSQL CDC (experimental, behind `turso` feature flag)

- **Schema Registry**
  - Source-owned schema types (source native semantics)
  - Schema change detection and versioning
  - SHA-256 fingerprinting for stable change detection

- **Schema Sensing**
  - Automatic schema inference from JSON event payloads
  - Deep inspection for nested JSON structures
  - High-cardinality key detection (session IDs, trace IDs, dynamic maps)
  - Configurable sampling with warmup and cache optimization
  - Drift detection comparing DB schema vs observed data
  - JSON Schema export for downstream consumers

- **Checkpoints**
  - Pluggable backends (File, SQLite with versioning, in-memory)
  - Configurable commit policies (all, required, quorum)
  - Transaction boundary preservation (best effort)

- **Processors**
  - JavaScript processors using `deno_core`:
    - Run user defined functions (UDFs) in JS to transform batches of events

- **Sinks**
  - Kafka producer sink (via `rdkafka`)
  - Redis stream sink
  - NATS JetStream sink (via `async_nats`)
  - Configurable envelope formats: Native, Debezium, CloudEvents
  - JSON wire encoding (Avro planned and more to come)

### Event Output Formats

DeltaForge supports multiple envelope formats for ecosystem compatibility:

| Format | Output | Use Case |
|--------|--------|----------|
| `native` | `{"op":"c","after":{...},"source":{...}}` | Lowest overhead, DeltaForge consumers |
| `debezium` | `{"schema":null,"payload":{...}}` | Drop-in Debezium replacement |
| `cloudevents` | `{"specversion":"1.0","type":"...","data":{...}}` | CNCF-standard, event-driven systems |

🔄 **Debezium Compatibility**: DeltaForge uses Debezium's **schemaless mode** (`schema: null`), which matches Debezium's `JsonConverter` with `schemas.enable=false` - the recommended configuration for most Kafka deployments. This provides wire compatibility with existing Debezium consumers without the overhead of inline schemas (~500+ bytes per message).

> 💡 **Migrating from Debezium?** If your consumers already use `schemas.enable=false`, configure `envelope: { type: debezium }` on your sinks for drop-in compatibility. For consumers expecting inline schemas, you'll need Schema Registry integration (Avro encoding - planned).

See [Envelope Formats](docs/src/envelopes.md) for detailed examples and wire format specifications.

## Documentation

- 📘 Online docs: <https://vnvo.github.io/deltaforge>
- 🛠 Local: `mdbook serve docs` (browse at <http://localhost:3000>)

## Local development helper

Use the bundled `dev.sh` CLI to spin up the dependency stack and run common workflows consistently:

```bash
./dev.sh up     # start Postgres, MySQL, Kafka, Redis, NATS from docker-compose.dev.yml
./dev.sh ps     # view container status
./dev.sh check  # fmt --check + clippy + tests (matches CI)
```

See the [Development guide](docs/src/development.md) for the full layout and additional info.

## Container image

Pre-built multi-arch images (amd64/arm64) are available:
```bash
# From GitHub Container Registry
docker pull ghcr.io/vnvo/deltaforge:latest

# From Docker Hub
docker pull vnvohub/deltaforge:latest

# Debug variant (includes shell)
docker pull ghcr.io/vnvo/deltaforge:latest-debug
```

Or build locally:
```bash
docker build -t deltaforge:local .
```

Run it by mounting your pipeline specs (environment variables are expanded inside the YAML) and exposing the API and metrics ports:

```bash
docker run --rm \
  -p 8080:8080 -p 9000:9000 \
  -v $(pwd)/examples/dev.yaml:/etc/deltaforge/pipelines.yaml:ro \
  -v deltaforge-checkpoints:/app/data \
  deltaforge:local \
  --config /etc/deltaforge/pipelines.yaml
```

or with env variables to be expanded inside the provided config:
```bash
# pull the container
docker pull ghcr.io/vnvo/deltaforge:latest

# run it
docker run --rm \
  -p 8080:8080 -p 9000:9000 \
  -e MYSQL_DSN="mysql://user:pass@host:3306/db" \
  -e KAFKA_BROKERS="kafka:9092" \
  -v $(pwd)/pipeline.yaml:/etc/deltaforge/pipeline.yaml:ro \
  -v deltaforge-checkpoints:/app/data \
  ghcr.io/vnvo/deltaforge:latest \
  --config /etc/deltaforge/pipeline.yaml
```

The container runs as a non-root user, writes checkpoints to `/app/data/df_checkpoints.json`, and listens on `0.0.0.0:8080` for the control plane API with metrics served on `:9000`.


## Architecture Highlights

### At-least-once and Checkpoint Timing Guarantees

DeltaForge guarantees at-least-once delivery through careful checkpoint ordering:

```
Source → Processor → Sink (deliver) → Checkpoint (save)
                           │
                    Sink acknowledges
                    successful delivery
                           │
                    THEN checkpoint saved
```

Checkpoints are never saved before events are delivered. A crash between delivery and checkpoint causes replay (duplicates possible), but never loss.

### Schema-Checkpoint Correlation

The schema registry tracks schema versions with sequence numbers and optional checkpoint correlation. During replay, events are interpreted with the schema that was active when they were produced - even if the table structure has since changed.

### Source-Owned Schemas

Unlike tools that normalize all databases to a universal type system, DeltaForge lets each source define its own schema semantics. MySQL schemas capture MySQL types (`bigint(20) unsigned`, `json`), PostgreSQL schemas preserve arrays and custom types. No lossy normalization, no universal type maintenance burden.


## API

The REST API exposes JSON endpoints for liveness, readiness, and pipeline lifecycle
management. Routes key pipelines by the `metadata.name` field from their specs and
return `PipeInfo` payloads that include the pipeline name, status, and full
configuration.

### Health

- `GET /healthz` - lightweight liveness probe returning `ok`.
- `GET /readyz` - readiness view returning `{"status":"ready","pipelines":[...]}`
  with the current pipeline states.

### Pipeline management

- `GET /pipelines` - list all pipelines with their current status and config.
- `POST /pipelines` - create a new pipeline from a full `PipelineSpec` document.
- `GET /pipelines/{name}` - get a single pipeline by name.
- `PATCH /pipelines/{name}` - apply a partial JSON patch to an existing pipeline
  (e.g., adjust batch or connection settings) and restart it with the merged spec.
- `DELETE /pipelines/{name}` - permanently delete a pipeline.
- `POST /pipelines/{name}/pause` - pause ingestion and processing for the pipeline.
- `POST /pipelines/{name}/resume` - resume a paused pipeline.
- `POST /pipelines/{name}/stop` - stop a running pipeline.

### Schema endpoints

- `GET /pipelines/{name}/schemas` - list DB schemas for the pipeline.
- `GET /pipelines/{name}/sensing/schemas` - list inferred schemas (from sensing).
- `GET /pipelines/{name}/sensing/schemas/{table}` - get inferred schema details.
- `GET /pipelines/{name}/sensing/schemas/{table}/json-schema` - export as JSON Schema.
- `GET /pipelines/{name}/sensing/schemas/{table}/classifications` - get dynamic map classifications.
- `GET /pipelines/{name}/drift` - get drift detection results.
- `GET /pipelines/{name}/sensing/stats` - get schema sensing cache statistics.


## Configuration schema

Pipelines are defined as YAML documents that map directly to the internal `PipelineSpec` type. 
Environment variables are expanded before parsing, so secrets and URLs can be injected at runtime.

<table>
<tr>
<td width="50%" valign="top">

### Full Example

```yaml
metadata:
  name: orders-mysql-to-kafka
  tenant: acme

spec:
  sharding:
    mode: hash
    count: 4
    key: customer_id

  source:
    type: mysql
    config:
      id: orders-mysql
      dsn: ${MYSQL_DSN}
      tables:
        - shop.orders

  processors:
    - type: javascript
      id: my-custom-transform
      inline: |
        function processBatch(events) {
          return events;
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
    - type: redis
      config:
        id: orders-redis
        uri: ${REDIS_URI}
        stream: orders
        envelope:
          type: native
        encoding: json
  
  batch:
    max_events: 500
    max_bytes: 1048576
    max_ms: 1000
    respect_source_tx: true

  commit_policy:
    mode: quorum
    quorum: 2

  schema_sensing:
    enabled: true
    deep_inspect:
      enabled: true
      max_depth: 3
    sampling:
      warmup_events: 50
      sample_rate: 5
    high_cardinality:
      enabled: true
      min_events: 100
```

</td>
<td width="50%" valign="top">

### Key fields

| Field | Description |
|-------|-------------|
| **`metadata`** | |
| `name` | Pipeline identifier (used in API routes and metrics) |
| `tenant` | Business-oriented tenant label |
| **`spec.source`** | Database source - [MySQL](docs/src/sources/mysql.md), [PostgreSQL](docs/src/sources/postgres.md), etc. |
| `type` | `mysql`, `postgres`, etc. |
| `config.id` | Unique identifier for checkpoints |
| `config.dsn` | Connection string (supports `${ENV_VAR}`) |
| `config.tables` | Table patterns to capture |
| **`spec.processors`** | Optional transforms - see [Processors](docs/src/configuration.md#processors) |
| `type` | `javascript` |
| `inline` | JavaScript code for batch processing |
| `limits` | CPU, memory, and timeout limits |
| **`spec.sinks`** | One or more sinks - see [Sinks](docs/src/sinks/README.md) |
| `type` | `kafka`, `redis`, or `nats` |
| `config.envelope` | Output format: `native`, `debezium`, or `cloudevents` - see [Envelopes](docs/src/envelopes.md) |
| `config.encoding` | Wire encoding: `json` (default) |
| `config.required` | Whether sink must ack for checkpoint (`true` default) |
| **`spec.batch`** | Commit unit thresholds - see [Batching](docs/src/configuration.md#batching) |
| `max_events` | Flush after N events (default: 500) |
| `max_bytes` | Flush after size limit (default: 1MB) |
| `max_ms` | Flush after time (default: 1000ms) |
| `respect_source_tx` | Keep source transactions intact (`true` default) |
| **`spec.commit_policy`** | Checkpoint gating - see [Commit policy](docs/src/configuration.md#commit-policy) |
| `mode` | `all`, `required` (default), or `quorum` |
| `quorum` | Number of sinks for quorum mode |
| **`spec.schema_sensing`** | Runtime schema inference - see [Schema sensing](docs/src/schemasensing.md) |
| `enabled` | Enable schema sensing (`false` default) |
| `deep_inspect` | Nested JSON inspection settings |
| `sampling` | Sampling rate and warmup config |
| `high_cardinality` | Dynamic key detection settings |

📘 Full reference: [Configuration docs](docs/src/configuration.md)

View actual examples: [Example Configurations](docs/src/examples/README.md)

</td>
</tr>
</table>

## Roadmap

- [ ] Outbox pattern support
- [ ] Persistent schema registry (SQLite, then PostgreSQL)
- [ ] Protobuf encoding
- [ ] PostgreSQL/S3 checkpoint backends for HA
- [ ] MongoDB source
- [ ] ClickHouse sink  
- [ ] Event store for time-based replay
- [ ] Distributed coordination for HA


## License

Licensed under either of

- **MIT License** (see [`LICENSE-MIT`](./LICENSE-MIT))
- **Apache License, Version 2.0** (see [`LICENSE-APACHE`](./LICENSE-APACHE))

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this project by you shall be dual licensed as above, without
additional terms or conditions.