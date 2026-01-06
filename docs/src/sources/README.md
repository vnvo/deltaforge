# Sources

DeltaForge captures database changes through pluggable source connectors. Each source is configured under `spec.source` with a `type` field and a `config` object. Environment variables are expanded before parsing using `${VAR}` syntax.

## Supported Sources

| Source | Status | Description |
|--------|--------|-------------|
| <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/mysql/mysql-original.svg" width="20" height="20"> [`mysql`](mysql.md) | ✅ Production | MySQL binlog CDC with GTID support |
| <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/postgresql/postgresql-original.svg" width="20" height="20"> [`postgres`](postgres.md) | ✅ Production | PostgreSQL logical replication via pgoutput |
| [`turso`](turso.md) | 🔧 Beta | Turso/libSQL CDC with multiple modes |

## Common Behavior

All sources share these characteristics:

- **Checkpointing**: Progress is automatically saved and resumed on restart
- **Schema tracking**: Table schemas are loaded and fingerprinted for change detection
- **At-least-once delivery**: Events may be redelivered after failures; sinks should be idempotent
- **Batching**: Events are batched according to the pipeline's `batch` configuration
- **Transaction boundaries**: `respect_source_tx: true` (default) keeps source transactions intact

## Source Interface

Sources implement a common trait that provides:

```rust
trait Source {
    fn checkpoint_key(&self) -> &str;
    async fn run(&self, tx: Sender<Event>, checkpoint_store: Arc<dyn CheckpointStore>) -> SourceHandle;
}
```

The returned `SourceHandle` supports pause/resume and graceful cancellation.

## Adding Custom Sources

The source interface is pluggable. To add a new source:

1. Implement the `Source` trait
2. Add configuration parsing in `deltaforge-config`
3. Register the source type in the pipeline builder

See existing sources for implementation patterns.