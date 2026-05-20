# Roadmap

## Completed

- **S3 / Parquet / JSON Lines sink** — direct lakehouse path; AWS S3, MinIO, GCS, Azure, local FS via `object_store`. DDL-derived Arrow schemas with native `Decimal128`. Hive-style partitioning by table + UTC date. Atomic multipart commits. File rolling on size/events/age/idle. See [S3 sink docs](sinks/s3.md).
- **Avro encoding with Confluent Schema Registry** — DDL-derived Avro schemas, Confluent wire format, all sinks supported, type conversion policies, Schema Registry failure handling with cached fallback
- **HTTP/Webhook sink** — POST/PUT to any URL, URL templates, batch mode, retry with backoff
- **Dead Letter Queue** — per-event failure routing, overflow policies, REST API for inspection
- **Per-sink independent checkpoints** — each sink advances independently, source replays from minimum
- **Exactly-once delivery** — Kafka transactional producer with producer fencing detection
- **Helm chart** — StatefulSet, ConfigMap, PVC, ServiceMonitor, PDB
- **Schema sensing** — automatic schema inference from payloads, high-cardinality key detection

## In Progress

- **Avro encoding performance** — hot-path optimization (47K events/s steady-state). TD-001 (direct ColumnValue→Avro conversion, eliminating JSON intermediary) planned for further gains.
- **Avro Schema Registry Phase 2** — publish sensed schemas to external Schema Registry as catalog metadata (under `sensed.` prefix). See [RFC](https://github.com/deltaforge/deltaforge/blob/main/docs/specs/avro-schema-registry.md).

## Planned

- **Iceberg / Delta Lake table formats** — exactly-once at event level via atomic snapshot commits; schema evolution and time travel on top of the Phase 1 S3 sink
- **Coordinator-level per-sink deadline** — bound how long the coordinator awaits any one sink, even if its internal timeout misfires
- **MongoDB source** — change streams CDC
- **Event replay** — replay DLQ entries or historical events
- **Kubernetes operator** — PipelineTemplate + PipelinePool for fleet management
- **OpenAPI spec generation** — auto-generated REST API documentation
