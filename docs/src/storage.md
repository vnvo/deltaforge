# Storage

DeltaForge uses a unified storage layer that backs all runtime state: checkpoints, schema registry, FSM state, leases, and quarantine queues. A single `StorageBackend` instance is shared across subsystems, so there is one operational concern instead of many.

## Backends

| Backend | Persistence | Use Case |
|---------|-------------|----------|
| `MemoryStorageBackend` | None (lost on restart) | Testing, ephemeral deployments |
| `SqliteStorageBackend` | SQLite file on disk | Single-instance production |
| `PostgresStorageBackend` | External database | HA, multi-instance deployments |

### Configuration

The backend is selected via CLI flags:

```bash
# SQLite (default for production)
deltaforge --config pipeline.yaml --storage-backend sqlite --storage-path ./data/deltaforge.db

# In-memory (testing only — all state lost on restart)
deltaforge --config pipeline.yaml --storage-backend memory
```

Or via the config file:

```yaml
storage:
  backend: sqlite
  path: ./data/deltaforge.db

# or:
storage:
  backend: postgres
  dsn: "host=localhost dbname=deltaforge_storage user=df password=secret"
```

The SQLite backend creates the database file and parent directories automatically on first start. The PostgreSQL backend runs schema migrations on first connect - no manual table creation is needed.

> **PostgreSQL backend** is implemented and available under the `postgres` feature flag, but has not yet received the same chaos/recovery validation as the SQLite backend. Treat it as beta for production use.

## Primitives

The `StorageBackend` trait exposes four primitives. All operations are namespaced - different subsystems share the same backend without key collisions.

### KV (Key-Value with optional TTL)

General-purpose persistent key-value store. Used for checkpoints, FSM state, and leases.

```rust
backend.kv_put("checkpoints", "mysql-pipe1", &bytes).await?;
backend.kv_put_with_ttl("leases", "pipe1", b"alive", ttl_secs).await?;
backend.kv_get("checkpoints", "mysql-pipe1").await?;
backend.kv_delete("checkpoints", "mysql-pipe1").await?;
backend.kv_list("checkpoints", Some("mysql-")).await?; // prefix filter
```

### Log (Append-only, globally sequenced)

Immutable append-only log with a global monotonic sequence counter. Used for the schema registry. The sequence is global across all keys in the namespace - interleaved appends to different keys produce strictly increasing sequence numbers.

```rust
let seq = backend.log_append("schemas", "acme/shop/orders", &entry).await?;
let all  = backend.log_list("schemas", "acme/shop/orders").await?;
let tail = backend.log_since("schemas", "acme/shop/orders", since_seq).await?;
```

### Slot (Versioned, compare-and-swap)

Single versioned value with CAS semantics. Used for snapshot cursors and leader election. Concurrent CAS operations with the same expected version are serialized - exactly one wins.

```rust
let ver = backend.slot_upsert("snapshots", "pipe/orders", &state).await?;
let won = backend.slot_cas("snapshots", "pipe/orders", ver, &new_state).await?;
let cur = backend.slot_get("snapshots", "pipe/orders").await?; // (version, bytes)
```

### Queue (Ordered, ack-based)

Ordered queue with explicit acknowledgement. Used for quarantine and DLQ. Items are not removed until acked; oldest items can be dropped when the queue is full.

```rust
let id = backend.queue_push("quarantine", "pipe/orders", &event_bytes).await?;
let items = backend.queue_peek("quarantine", "pipe/orders", 10).await?;
backend.queue_ack("quarantine", "pipe/orders", id).await?;
backend.queue_drop_oldest("quarantine", "pipe/orders", n).await?;
let len = backend.queue_len("quarantine", "pipe/orders").await?;
```

## Namespace Map

Each subsystem owns a fixed namespace. Keys within a namespace never collide across subsystems.

| Namespace | Primitive | Used by | Key format |
|-----------|-----------|---------|------------|
| `checkpoints` | KV | `BackendCheckpointStore` | `{source-id}` |
| `schemas` | Log | `DurableSchemaRegistry` | `{tenant}/{db}/{table}` |
| `schemas` | KV | `DurableSchemaRegistry` | `{tenant}/{db}/{table}` (index) |
| `snapshots` | Slot | Snapshot/backfill *(planned)* | `{pipeline}/{table}` |
| `fsm` | KV | FSM state *(planned)* | `{pipeline}` |
| `leases` | KV + TTL | Leader election *(planned)* | `{pipeline}` |
| `quarantine` | Queue | Quarantine *(planned)* | `{pipeline}/{table}` |
| `dlq` | Queue | Dead-letter queue *(planned)* | `{pipeline}` |

## Adapters

Existing pipeline code uses higher-level interfaces - it never touches the `StorageBackend` directly.

### BackendCheckpointStore

Implements the `CheckpointStore` trait on top of KV. Drop-in replacement for the old `FileCheckpointStore` and `SqliteCheckpointStore`.

```rust
let store = BackendCheckpointStore::new(Arc::clone(&backend));
// CheckpointStore trait methods work unchanged
store.put_raw("mysql-pipe1", &bytes).await?;
store.get_raw("mysql-pipe1").await?;
```

### DurableSchemaRegistry

Implements the schema registry on top of the Log primitive. On startup it replays the log to populate an in-memory cache, so hot-path reads are identical in performance to the old `InMemoryRegistry`. Writes go through the log, enabling cold-start reconstruction.

```rust
// Production: async construction with log replay
let registry = DurableSchemaRegistry::new(Arc::clone(&backend)).await?;

// Tests: sync construction, no replay (MemoryStorageBackend, empty log)
let registry = DurableSchemaRegistry::for_testing();
```

## PipelineManager Wiring

`PipelineManager::with_backend()` wires both subsystems from a single backend:

```rust
let backend: ArcStorageBackend = SqliteStorageBackend::open("./data/deltaforge.db")?;
let manager = PipelineManager::with_backend(backend).await?;
```

Internally this creates a `BackendCheckpointStore` and a `DurableSchemaRegistry` from the same backend instance. All pipelines share the same storage file.