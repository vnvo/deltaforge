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
  <a href="https://vnvo.github.io/deltaforge">
    <img src="https://img.shields.io/badge/docs-online-blue.svg" alt="Docs">
  </a>  
  <img src="https://img.shields.io/badge/rustc-1.89+-orange.svg" alt="MSRV">
  <img src="https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg" alt="License">
</p>

> A modular, efficient and config-driven Change Data Capture (CDC) micro-framework.

> ⚠️ **Status:** Active development. APIs, configuration, and semantics may change.

DeltaForge is a lightweight framework for building CDC pipelines that stream database changes into downstream systems such as Kafka and Redis. It focuses on:

- **User Control** : Using an embedded JS engine, users can fully control what happens to each event.
- **Config-driven pipelines** : YAML-defined pipelines instead of bespoke code per use-case.
- **Cloud-Native** : CN first design and operation.
- **Extensibility** : add your own sources, processors, and sinks.

However, DeltaForge is NOT a DAG based stream processor.
DeltaForge is meant to replace tools like Debezium and similar.

## Supported Technologies

<table>
  <tr>
    <td align="center" width="140"><b>Built with</b></td>
    <td align="center" width="140"><b>Sources</b></td>
    <td align="center" width="140"><b>Processors</b></td>
    <td align="center" width="140"><b>Sinks</b></td>
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
      <br><sub>Kafka · Redis</sub>
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

## Documentation

- 📘 Online docs: <https://vnvo.github.io/deltaforge>
- 🛠 Local: `mdbook serve docs` (browse at <http://localhost:3000>)

## Local development helper

Use the bundled `dev.sh` CLI to spin up the dependency stack and run common workflows consistently:

```bash
./dev.sh up     # start Postgres, MySQL, Kafka, Redis from docker-compose.dev.yml
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

The schema registry tracks schema versions with sequence numbers and optional checkpoint correlation. During replay, events are interpreted with the schema that was active when they were produced — even if the table structure has since changed.

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
- `GET /pipelines/{name}/drift` - get drift detection results.
- `GET /pipelines/{name}/sensing/stats` - get schema sensing cache statistics.


## Configuration schema

Pipelines are defined as YAML documents that map directly to the `PipelineSpec`
type. Environment variables are expanded before parsing, so secrets and URLs can
be injected at runtime.

### MySQL source example

```yaml
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
        - shop.order_items

  sinks:
    - type: kafka
      config:
        id: orders-kafka
        brokers: ${KAFKA_BROKERS}
        topic: orders
```

### PostgreSQL source example

```yaml
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
```

### Full configuration example

```yaml
metadata:
  name: orders-mysql-to-kafka
  tenant: acme

spec:
  # Optional: shard downstream processing
  sharding:
    mode: hash
    count: 4
    key: customer_id

  # Source definition
  source:
    type: mysql
    config:
      id: orders-mysql
      dsn: ${MYSQL_DSN}
      tables:
        - shop.orders

  # Zero or more processors
  processors:
    - type: javascript
      id: transform
      inline: |
        function processBatch(events) {
          return events;
        }
      limits:
        cpu_ms: 50
        mem_mb: 128
        timeout_ms: 500

  # One or more sinks
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
  
  # Batch config
  batch:
    max_events: 500
    max_bytes: 1048576
    max_ms: 1000
    respect_source_tx: true
    max_inflight: 2

  # Commit policy
  commit_policy:
    mode: quorum
    quorum: 2

  # Optional: schema sensing
  schema_sensing:
    enabled: true
    deep_inspect:
      enabled: true
      max_depth: 3
    sampling:
      warmup_events: 50
      sample_rate: 5
```

Key fields:

- `metadata` - required name (used as pipeline identifier) and tenant label.
- `spec.sharding` - optional hint for downstream distribution.
- `spec.source` - required source configuration (MySQL, PostgreSQL, or Turso).
- `spec.processors` - ordered processors; JavaScript is supported today with optional resource limits.
- `spec.sinks` - one or more sinks; Kafka supports `required`, `exactly_once`, and raw `client_conf` overrides; Redis streams are also available.
- `spec.batch` - optional thresholds that define the commit unit.
- `spec.commit_policy` - how sink acknowledgements gate checkpoint commits (`all`, `required` (default), or `quorum`).
- `spec.schema_sensing` - optional automatic schema inference from event payloads.


## Roadmap

- [ ] Persistent schema registry (SQLite, then PostgreSQL)
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