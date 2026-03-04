# Checkpoints

Checkpoints record pipeline progress so ingestion can resume from the last successfully delivered position. DeltaForge guarantees **at-least-once delivery** by saving checkpoints only after events have been acknowledged by sinks.

## Core Guarantee: At-Least-Once Delivery

> **Checkpoints are only saved after events have been successfully delivered to sinks.**

If a checkpoint were saved before delivery, a crash between those two points would silently lose events. DeltaForge prevents this by always checkpointing after sink acknowledgment.

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Source    │────▶│  Processor  │────▶│    Sink     │────▶│ Checkpoint  │
│   (read)    │     │ (transform) │     │  (deliver)  │     │   (save)    │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
```

On crash: events since the last checkpoint are replayed. Consumers should be idempotent or use deduplication.

## Storage

Checkpoints are stored via the unified `StorageBackend` - see [Storage](storage.md) for backend configuration. All pipelines share the same storage backend; each pipeline's checkpoint is keyed by its source ID.

| Backend | Persistence | Use Case |
|---------|-------------|----------|
| `SqliteStorageBackend` | SQLite file on disk | Single-instance production |
| `MemoryStorageBackend` | None (lost on restart) | Testing |
| PostgreSQL *(planned)* | External database | HA, multi-instance |

## Checkpoint Contents

### MySQL

Tracks binlog position:

```
file:     binlog.000042
pos:      12345
gtid_set: (optional, if GTID replication is enabled)
```

### PostgreSQL

Tracks replication stream LSN:

```
lsn:    0/1A2B3C4D
tx_id:  (optional)
```

## Checkpoint-Schema Correlation

When a schema change is detected, the current checkpoint position is recorded alongside the new schema version. This ensures that during replay, each event is interpreted with the schema that was active when it was produced - even if the table has since been altered.

## Commit Policy

When multiple sinks are configured, the commit policy controls when the checkpoint advances:

| Policy | Behaviour |
|--------|-----------|
| `all` | All sinks must acknowledge |
| `required` | Only sinks marked `required: true` must acknowledge |
| `quorum(n)` | At least `n` sinks must acknowledge |

Set `required: true` only on sinks where delivery is mandatory for correctness. Optional sinks can fail without blocking the checkpoint.

## Operations

### Inspecting Checkpoints

```bash
sqlite3 ./data/deltaforge.db \
  "SELECT key, length(value) FROM kv WHERE namespace = 'checkpoints';"
```

### Resetting a Pipeline

To force a pipeline to re-read from the beginning, delete its checkpoint:

```bash
# Via API
curl -X DELETE http://localhost:8080/pipelines/{name}/checkpoint

# Directly in SQLite
sqlite3 ./data/deltaforge.db \
  "DELETE FROM kv WHERE namespace = 'checkpoints' AND key = '{source-id}';"
```

### Best Practices

- Back up `deltaforge.db` regularly - it contains both checkpoints and schema history
- Monitor checkpoint lag to detect stuck pipelines
- Use smaller batch sizes for more frequent checkpoints at the cost of throughput
- Test recovery by killing the process and verifying no events are lost or duplicated