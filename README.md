# DeltaForge

> A modular, efficient and config-driven Change Data Capture (CDC) micro-framework.

DeltaForge is a lightweight framework for building CDC pipelines that stream database changes into downstream systems such as Kafka and Redis. It focuses on:

- **User Control** : Using an embedded JS engine, users can full control what happens to each event.
- **Config-driven pipelines** : YAML-defined pipelines instead of bespoke code per use-case.
- **Cloud-Native** : CN first design and operation.
- **Extensibility** : add your own sources, processors, and sinks.
- **Observability** : metrics, structured logging, and panic hooks built in.

However, deltaforge is NOT a DAG based stream processor.
Deltaforge is meant to replace tools like Debezium and similar.


> ⚠️ **Status:** Active developmemt. APIs, configuration, and semantics may change.


---

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

---