# Roadmap

## Completed

- **Avro encoding with Confluent Schema Registry** — DDL-derived Avro schemas, Confluent wire format, all sinks supported, type conversion policies, Schema Registry failure handling with cached fallback
- **HTTP/Webhook sink** — POST/PUT to any URL, URL templates, batch mode, retry with backoff
- **Dead Letter Queue** — per-event failure routing, overflow policies, REST API for inspection
- **Per-sink independent checkpoints** — each sink advances independently, source replays from minimum
- **Exactly-once delivery** — Kafka transactional producer with producer fencing detection
- **Helm chart** — StatefulSet, ConfigMap, PVC, ServiceMonitor, PDB
- **Schema sensing** — automatic schema inference from payloads, high-cardinality key detection

## In Progress

- **Avro Schema Registry Phase 2** — publish sensed schemas to external Schema Registry as catalog metadata (under `sensed.` prefix). See [RFC](https://github.com/deltaforge/deltaforge/blob/main/docs/specs/avro-schema-registry.md).

## Planned

- **MongoDB source** — change streams CDC
- **S3/Parquet sink** — data lake integration
- **Event replay** — replay DLQ entries or historical events
- **Kubernetes operator** — PipelineTemplate + PipelinePool for fleet management
- **OpenAPI spec generation** — auto-generated REST API documentation
