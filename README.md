<p align="center">
  <img src="assets/deltaforge-blc.png" width="150" alt="DeltaForge">
</p>
<p align="center">
  <a href="https://github.com/vnvo/deltaforge/actions/workflows/ci.yml">
    <img src="https://github.com/vnvo/deltaforge/actions/workflows/ci.yml/badge.svg" alt="CI">
  </a>
  <a href="https://coveralls.io/github/vnvo/deltaforge?branch=main">
    <img src="https://coveralls.io/repos/github/vnvo/deltaforge/badge.svg?branch=main" alt="Coverage Status">
  </a>
  <img src="https://img.shields.io/badge/rustc-1.89+-orange.svg" alt="MSRV">
  <img src="https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg" alt="License">
</p>

> A modular, efficient and config-driven Change Data Capture (CDC) micro-framework.

DeltaForge is a lightweight framework for building CDC pipelines that stream database changes into downstream systems such as Kafka and Redis. It focuses on:

- **User Control** : Using an embedded JS engine, users can fully control what happens to each event.
- **Config-driven pipelines** : YAML-defined pipelines instead of bespoke code per use-case.
- **Cloud-Native** : CN first design and operation.
- **Extensibility** : add your own sources, processors, and sinks.
- **Observability** : metrics, structured logging, and panic hooks built in.

However, deltaforge is NOT a DAG based stream processor.
Deltaforge is meant to replace tools like Debezium and similar.


> ⚠️ **Status:** Active developmemt. APIs, configuration, and semantics may change.




## Features

- **Sources**
  - MySQL binlog CDC.
  - Postgres logical replication.

- **Processors**
  - JavaScript processors using `deno_core`:
    - Run user-provided JS to transform batches of events.

- **Sinks**
  - Kafka producer sink (via `rdkafka`).
  - Redis stream sink.

- **Core runtime**
  - Unified `Event` model (`before` / `after` images, DDL payloads, tx id, schema version, timestamps, etc.).
  - Checkpointing.
  - Batch support.
  - Multiple pipeline at the same time.

- **Control plane & observability**
  - HTTP API for health and pipeline config management.
  - Metrics endpoint.


## Documentation

- [mdBook](docs/src/SUMMARY.md) — browse the structured documentation locally with `mdbook serve docs`


## Features

- **Sources**
  - MySQL binlog CDC.
  - Postgres logical replication.

- **Processors**
  - JavaScript processors using `deno_core`:
    - Run user-provided JS to transform batches of events.

- **Sinks**
  - Kafka producer sink (via `rdkafka`).
  - Redis stream sink.

- **Core runtime**
  - Unified `Event` model (`before` / `after` images, DDL payloads, tx id, schema version, timestamps, etc.).
  - Checkpointing.
  - Batch support.
  - Multiple pipeline at the same time.

- **Control plane & observability**
  - HTTP API for health and pipeline config management.
  - Metrics endpoint.



## API

The REST API exposes JSON endpoints for liveness, readiness, and pipeline lifecycle
management. Routes key pipelines by the `metadata.name` field from their specs and
return `PipeInfo` payloads that include the pipeline name, status, and full
configuration.

### Health

- `GET /healthz` — lightweight liveness probe returning `ok`.
- `GET /readyz` — readiness view returning `{"status":"ready","pipelines":[...]}`
  with the current pipeline states.

### Pipeline management

- `GET /pipelines` — list all pipelines with their current status and config.
- `POST /pipelines` — create a new pipeline from a full `PipelineSpec` document.
- `PATCH /pipelines/{name}` — apply a partial JSON patch to an existing pipeline
  (e.g., adjust batch or connection settings) and restart it with the merged spec.
- `POST /pipelines/{name}/pause` — pause ingestion and processing for the pipeline.
- `POST /pipelines/{name}/resume` — resume a paused pipeline.
- `POST /pipelines/{name}/stop` — stop a running pipeline.



## Configuration schema

Pipelines are defined as YAML documents that map directly to the `PipelineSpec`
type. Environment variables are expanded before parsing, so secrets and URLs can
be injected at runtime.

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

  # Source definition (for MySQL)
  source:
    type: mysql
    config:
      id: orders-mysql
      dsn: ${MYSQL_DSN}
      publication: orders_pub
      slot: orders_slot
      tables:
        - public.orders

  # Zero or more processors
  processors:
    - type: javascript
      id: transform
      inline: |
        function process(batch) {
          return batch;
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
  
  # batch config
  batch:
    max_events: 500
    max_bytes: 1048576
    max_ms: 1000
    respect_source_tx: true
    max_inflight: 2

  commit_policy:
    mode: quorum
    quorum: 2
```

Key fields:

- `metadata` — required name (used as pipeline identifier) and tenant label.
- `spec.sharding` — optional hint for downstream distribution.
- `spec.source` — required Postgres or MySQL configuration.
- `spec.processors` — ordered processors; JavaScript is supported today with optional resource limits.
- `spec.sinks` — one or more sinks; Kafka supports `required`, `exactly_once`, and raw `client_conf` overrides; Redis streams are also available.
- `spec.batch` — optional thresholds that define the commit unit.
- `spec.commit_policy` — how sink acknowledgements gate checkpoint commits (`all`, `required` (default), or `quorum`).


## License

Licensed under either of

- **MIT License** (see [`LICENSE-MIT`](./LICENSE-MIT))
- **Apache License, Version 2.0** (see [`LICENSE-APACHE`](./LICENSE-APACHE))

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this crate by you shall be dual licensed as above, without
additional terms or conditions.