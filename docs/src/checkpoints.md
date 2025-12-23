# Checkpoints

Checkpoints record pipeline progress so ingestion can resume from the last successfully delivered position. DeltaForge's checkpoint system is designed to guarantee **at-least-once delivery** by carefully coordinating when checkpoints are saved relative to event delivery.

## Core Guarantee: At-Least-Once Delivery

The fundamental rule of DeltaForge checkpointing:

> **Checkpoints are only saved after events have been successfully delivered to sinks.**

This ordering is critical. If a checkpoint were saved before events were delivered, a crash between checkpoint save and delivery would cause those events to be lost - the pipeline would resume from a position past events that were never delivered.

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Source    │────▶│  Processor  │────▶│    Sink     │────▶│ Checkpoint  │
│   (read)    │     │ (transform) │     │  (deliver)  │     │   (save)    │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                                               │
                                               ▼
                                        Sink acknowledges
                                        successful delivery
                                               │
                                               ▼
                                        Then checkpoint
                                        is saved
```

### What This Means in Practice

- **On clean shutdown**: All buffered events are flushed and checkpointed
- **On crash**: Events since the last checkpoint are replayed (hence "at-least-once")
- **Duplicate handling**: Consumers should be idempotent or use deduplication

## Checkpoint Storage

### Storage Backends

DeltaForge supports pluggable checkpoint storage:

| Backend | Description | Use Case |
|---------|-------------|----------|
| `FileCheckpointStore` | JSON file on disk | Development, simple deployments |
| `MemCheckpointStore` | In-memory (ephemeral) | Testing |
| `SqliteCheckpointStore` | SQLite with versioning | Single-instance production |

The default stores checkpoints to `./data/df_checkpoints.json`.

For HA deployments requiring shared state across instances, additional backends (PostgreSQL, S3/GCS) are planned but not yet implemented.

### Storage Interface

All backends implement the `CheckpointStore` trait:

```rust
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    /// Get raw checkpoint bytes
    async fn get_raw(&self, source_id: &str) -> CheckpointResult<Option<Vec<u8>>>;
    
    /// Store raw checkpoint bytes
    async fn put_raw(&self, source_id: &str, bytes: &[u8]) -> CheckpointResult<()>;
    
    /// Delete checkpoint
    async fn delete(&self, source_id: &str) -> CheckpointResult<bool>;
    
    /// List all checkpoint keys
    async fn list(&self) -> CheckpointResult<Vec<String>>;
    
    /// Whether this backend supports versioning
    fn supports_versioning(&self) -> bool;
}
```

### Typed Access

The `CheckpointStoreExt` trait provides convenient typed access:

```rust
// Store typed checkpoint (automatically serialized to JSON)
store.put("pipeline-1", MySqlCheckpoint { 
    file: "binlog.000042".into(),
    pos: 12345,
    gtid_set: None,
}).await?;

// Retrieve typed checkpoint
let cp: Option<MySqlCheckpoint> = store.get("pipeline-1").await?;
```

## Checkpoint Contents

### MySQL Checkpoints

MySQL checkpoints track binlog position:

```rust
pub struct MySqlCheckpoint {
    pub file: String,        // e.g., "binlog.000042"
    pub pos: u64,            // Byte position in binlog file
    pub gtid_set: Option<String>,  // GTID set if enabled
}
```

The checkpoint is taken from the last event in a successfully delivered batch, ensuring resumption starts exactly where delivery left off.

### Checkpoint in Events

Events carry checkpoint metadata for end-to-end tracking:

```rust
pub struct Event {
    // ... other fields ...
    
    /// Checkpoint info from source
    pub checkpoint: Option<CheckpointMeta>,
}

pub enum CheckpointMeta {
    Opaque(Arc<[u8]>),  // Serialized source-specific checkpoint
}
```

Using `Arc<[u8]>` allows zero-copy sharing of checkpoint data across the pipeline without repeated allocations.

## Commit Policy

When multiple sinks are configured, the commit policy determines when checkpoints advance:

| Policy | Behavior |
|--------|----------|
| `all` | Every sink must acknowledge |
| `required` | Only `required: true` sinks must acknowledge |
| `quorum` | At least N sinks must acknowledge |

### Configuration

```yaml
spec:
  batch:
    commit_policy: required  # or: all, quorum
    quorum: 2                # for quorum policy

  sinks:
    - type: kafka
      required: true  # Must succeed for checkpoint
      config: { ... }
    
    - type: redis
      required: false  # Best-effort, doesn't block checkpoint
      config: { ... }
```

### Commit Logic

The coordinator tracks acknowledgments from each sink and only advances the checkpoint when the policy is satisfied:

```rust
// Simplified commit logic
let required_acks = sinks.iter().filter(|s| s.required).count();
let actual_acks = batch.acknowledgments.iter().filter(|a| a.success).count();

if actual_acks >= required_acks {
    checkpoint_store.put(&key, batch.last_checkpoint).await?;
} else {
    warn!("commit policy not satisfied; checkpoint NOT advanced");
}
```

## Batching and Checkpoints

Checkpoints are saved at batch boundaries, not per-event. This provides:

- **Efficiency**: Fewer checkpoint writes
- **Atomicity**: Batch success or failure is all-or-nothing
- **Transaction preservation**: `respect_source_tx: true` keeps source transactions in single batches

### Batch Configuration

```yaml
spec:
  batch:
    max_events: 1000      # Flush after N events
    max_bytes: 8388608    # Flush after 8MB
    max_ms: 200           # Flush after 200ms
    respect_source_tx: true  # Never split source transactions
    max_inflight: 1       # Concurrent batches in flight
```

### Checkpoint Timing in Batches

Within a batch:

1. Events are collected until a threshold is reached
2. Processors transform the batch
3. Sinks receive and deliver events
4. Sinks acknowledge success/failure
5. Commit policy is evaluated
6. If satisfied, checkpoint advances to the last event's position

## Versioned Checkpoints

The SQLite backend supports checkpoint versioning for:

- **Rollback**: Return to a previous checkpoint position
- **Audit**: Track checkpoint progression over time
- **Debugging**: Understand checkpoint history during incident analysis

### Version Operations

```rust
// Store with versioning
let version = store.put_raw_versioned("pipeline-1", bytes).await?;

// Get specific version
let old_bytes = store.get_version_raw("pipeline-1", version - 1).await?;

// List all versions
let versions = store.list_versions("pipeline-1").await?;

// Rollback to previous version
store.rollback("pipeline-1", target_version).await?;
```

### Version Metadata

```rust
pub struct VersionInfo {
    pub version: u64,
    pub created_at: DateTime<Utc>,
    pub size_bytes: usize,
}
```

## Schema-Checkpoint Correlation

For replay scenarios, DeltaForge correlates schemas with checkpoints. When a schema is registered, it can optionally include the current checkpoint position:

```rust
registry.register_with_checkpoint(
    tenant, db, table,
    &fingerprint,
    &schema_json,
    Some(checkpoint_bytes),  // Binlog position when schema was observed
).await?;
```

This enables:

- **Accurate replay**: Events are interpreted with the schema active at their checkpoint position
- **Schema time-travel**: Find what schema was active at any checkpoint
- **Coordinated rollback**: Roll back both checkpoint and schema state together

## Operational Considerations

### Clean Shutdown

Before maintenance, cleanly stop pipelines to flush checkpoints:

```bash
# Pause ingestion
curl -X POST http://localhost:8080/pipelines/{name}/pause

# Wait for in-flight batches to complete
sleep 5

# Stop pipeline
curl -X POST http://localhost:8080/pipelines/{name}/stop
```

### Checkpoint Inspection

View current checkpoint state:

```bash
# List all checkpoints
curl http://localhost:8080/checkpoints

# Get specific pipeline checkpoint
curl http://localhost:8080/checkpoints/{pipeline-name}
```

### Monitoring

Key metrics to monitor:

- `deltaforge_checkpoint_lag_seconds`: Time since last checkpoint
- `deltaforge_checkpoint_bytes`: Size of last checkpoint
- `deltaforge_batch_commit_total`: Successful batch commits
- `deltaforge_batch_commit_failed_total`: Failed commits (policy not satisfied)

### Recovery Scenarios

| Scenario | Behavior |
|----------|----------|
| Process crash | Resume from last checkpoint, replay events |
| Network partition (sink unreachable) | Retry delivery, checkpoint doesn't advance |
| Corrupt checkpoint file | Manual intervention required |
| Source unavailable at checkpoint | Retry connection with backoff |

## Best Practices

1. **Use durable storage** for production checkpoint backends (not in-memory)
2. **Monitor checkpoint lag** to detect stuck pipelines
3. **Configure appropriate batch sizes** — smaller batches mean more frequent checkpoints but more overhead
4. **Set `required: true`** only on sinks that must succeed for correctness
5. **Test recovery** by killing pipelines and verifying no events are lost
6. **Back up checkpoint files** if using file-based storage

## Future Enhancements

Planned checkpoint improvements:

- **PostgreSQL backend** for HA deployments with shared state
- **S3/GCS backends** for cloud-native deployments
- **Distributed coordination** for multi-instance leader election
- **Checkpoint compression** for large state
- **Point-in-time recovery** with event store integration