# Roadmap

## Completed

- **S3 / Parquet / JSON Lines sink** - direct lakehouse path; AWS S3, MinIO, GCS, Azure, local FS via `object_store`. DDL-derived Arrow schemas with native `Decimal128`. Hive-style partitioning by table + UTC date. File rolling on size/events/age/idle. See [S3 sink docs](sinks/s3.md).
- **Crash-durable S3 acknowledgements (`durable_v2`)** - the default S3 durability mode makes the acknowledgement point the durability point (immutable content-addressed objects + manifest entry + HEAD compare-and-swap), so acknowledged data is never lost across a crash. `legacy_rolling` remains available for rollback. See [durable acks docs](sinks/s3-durable-acks.md).
- **ClickHouse sink** - HTTP + RowBinary; change-log (`MergeTree`) or current-state upsert (`ReplacingMergeTree`); auto table creation from source DDL. See [ClickHouse sink docs](sinks/clickhouse.md).
- **Elasticsearch sink** - `_bulk` API current-state mirror; upsert by PK with `version_type=external`; delete propagation; DDL-derived index mappings. See [Elasticsearch sink docs](sinks/elasticsearch.md).
- **Avro encoding with Confluent Schema Registry** - DDL-derived Avro schemas, Confluent wire format, all sinks supported, type conversion policies, Schema Registry failure handling with cached fallback
- **HTTP/Webhook sink** - POST/PUT to any URL, URL templates, batch mode, retry with backoff
- **Dead Letter Queue** - per-event failure routing, overflow policies, REST API for inspection
- **Transaction-aware batching** - a commit unit contains only whole transactions; the checkpoint only ever lands at a transaction boundary; resume restarts on a clean boundary. Oversized-transaction fail-closed with a typed error. See [Guarantees & Correctness](guarantees.md).
- **Per-sink independent checkpoints** - each sink advances independently, source replays from minimum
- **Exactly-once delivery** - Kafka transactional producer with producer fencing detection
- **Helm chart** - StatefulSet, ConfigMap, PVC, ServiceMonitor, PDB
- **Schema sensing** - automatic schema inference from payloads, high-cardinality key detection

## In Progress

- **Avro encoding performance** - hot-path optimization (47K events/s steady-state). TD-001 (direct ColumnValue→Avro conversion, eliminating JSON intermediary) planned for further gains.
- **Avro Schema Registry Phase 2** - publish sensed schemas to external Schema Registry as catalog metadata (under `sensed.` prefix). See [RFC](https://github.com/deltaforge/deltaforge/blob/main/docs/specs/avro-schema-registry.md).

## Planned

- **Event replay (next)** - re-deliver historical events from the journal log to recover from consumer bugs, backfill new sinks, and re-deliver without resetting the source. Builds on the completed DLQ journal and storage backend; deterministic rebuild (snapshot + replay) follows it.
- **Iceberg / Delta Lake table formats** - exactly-once at event level via atomic snapshot commits; schema evolution and time travel on top of the S3 sink
- **Kubernetes operator** - PipelineTemplate + PipelinePool for fleet management
- **OpenAPI spec generation** - auto-generated REST API documentation
