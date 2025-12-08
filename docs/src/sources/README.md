# Sources

DeltaForge's only fully supported CDC source today is MySQL binlog ingestion. Each source is configured under `spec.source` with a `type` field and a `config` object. Environment variables are expanded before parsing, so DSNs can be supplied at runtime.

Current built-in source type:

- [`mysql`](mysql.md) — MySQL binlog CDC.

The source interface is pluggable; if you experiment with engines like Postgres logical replication, treat them as variations until they ship with formal support and docs.

All sources honor the pipeline's batching and commit policy once events enter the coordinator.
