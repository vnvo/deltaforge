# Architecture

This document describes DeltaForge's internal architecture, design decisions, and how the major components interact.

## Design Principles

### Source-Owned Semantics

DeltaForge avoids imposing a universal data model on all sources. Instead, each database source defines and owns its schema semantics:

- **MySQL** captures MySQL-specific types, collations, and engine information
- **PostgreSQL** captures PostgreSQL-specific types, OIDs, and replica identity
- **Future sources** (MongoDB, ClickHouse, TiDB) will capture their native semantics

This approach means downstream consumers receive schemas that accurately reflect the source database rather than a lowest-common-denominator normalization.

### Delivery Guarantees First

The checkpoint system is designed around a single invariant:

> Checkpoints are only saved after events have been successfully delivered.

This ordering guarantees at-least-once delivery. A crash between checkpoint and delivery would lose events; DeltaForge prevents this by always checkpointing after sink acknowledgment.

### Configuration Over Code

Pipelines are defined declaratively in YAML. This enables:

- Version-controlled pipeline definitions
- Environment-specific configuration via variable expansion
- Rapid iteration without recompilation

## Component Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        DeltaForge Runtime                       │
├─────────────┬─────────────┬─────────────┬─────────────┬─────────┤
│   Sources   │   Schema    │ Coordinator │    Sinks    │ Control │
│             │  Registry   │  + Batch    │             │  Plane  │
├─────────────┼─────────────┼─────────────┼─────────────┼─────────┤
│ MySQL       │ Durable     │ Batching    │ Kafka       │ REST API│
│ PostgreSQL  │ Schema      │ Commit      │ Redis       │ Metrics │
│             │ Registry    │ Policy      │ NATS        │ Health  │
└─────────────┴──────┬──────┴─────────────┴─────────────┴─────────┘
                     │
          ┌──────────┴──────────┐
          │   Storage Backend   │
          │  (SQLite / PG /     │
          │   Memory)           │
          ├─────────────────────┤
          │ KV · Log · Slot     │
          │ Queue               │
          └─────────────────────┘
```

## Data Flow

### Event Lifecycle

```
1. Source reads from database log (binlog/WAL)
        │
        ▼
2. Schema loader maps table_id to schema
        │
        ▼
3. Event constructed with before/after images
        │
        ▼
4. Event sent to coordinator via channel
        │
        ▼
5. Coordinator batches events
        │
        ▼
6. Processors transform batch (JavaScript)
        │
        ▼
7. Sinks deliver batch concurrently
        │
        ▼
8. Commit policy evaluated
        │
        ▼
9. Checkpoint saved (if policy satisfied)
```

### Event Structure

Every CDC event shares a common structure:

```rust
pub struct Event {
    pub source_id: String,          // Source identifier
    pub database: String,           // Database name
    pub table: String,              // Table name
    pub op: Op,                     // Insert, Update, Delete, Ddl
    pub tx_id: Option<u64>,         // Source transaction ID
    pub before: Option<Value>,      // Previous row state
    pub after: Option<Value>,       // New row state
    pub schema_version: Option<String>,  // Schema fingerprint
    pub schema_sequence: Option<u64>,    // For replay lookups
    pub ddl: Option<Value>,         // DDL payload if op == Ddl
    pub timestamp: DateTime<Utc>,   // Event timestamp
    pub checkpoint: Option<CheckpointMeta>,  // Position info
    pub size_bytes: usize,          // For batching
}
```

## Schema Registry

### Role

The schema registry serves three purposes:

1. **Map table IDs to schemas**: Binlog events reference tables by ID; the registry resolves these to full schema metadata
2. **Detect schema changes**: Fingerprint comparison identifies when DDL has modified a table
3. **Enable replay**: Sequence numbers correlate events with the schema active when they were produced

### Schema Sensing

At startup, the schema loader auto-discovers tables via pattern expansion and loads their schemas from the live database catalog before any CDC events arrive.

**Pattern expansion** supports wildcards:

| Pattern | Matches |
|---------|---------|
| `db.table` | Exact table |
| `db.*` | All tables in database |
| `db.prefix%` | Tables matching prefix |
| `%.table` | Table in any database |

**DDL detection** works through cache invalidation. When the binlog delivers a `QueryEvent` (DDL), the affected database's cache is cleared. On the next row event for that table, the schema is re-fetched from `INFORMATION_SCHEMA`, fingerprinted, and registered as a new version. No separate DDL history log is maintained — the live catalog is always the source of truth.

**Failover reconciliation** (verifying schemas against a new primary after server identity change) is planned but not yet implemented.

Schema versions are persisted via `DurableSchemaRegistry`, which uses the `StorageBackend` Log primitive. On startup the log is replayed to populate an in-memory cache, so hot-path reads have the same performance as the previous in-memory implementation. Cold-start reconstruction is always possible from the log alone.

### Schema Registration Flow

```
1. Schema loader fetches from INFORMATION_SCHEMA
        │
        ▼
2. Compute fingerprint (SHA-256 of structure)
        │
        ▼
3. Check registry for existing schema with same fingerprint
        │
        ├── Found: Return existing version (idempotent)
        │
        └── Not found: Allocate new version number
                │
                ▼
4. Store with: version, fingerprint, JSON, timestamp, sequence, checkpoint
```

### Sequence Numbers

The registry maintains a global monotonic counter. Each schema version receives a sequence number at registration. Events carry this sequence, enabling accurate schema lookup during replay:

```
Timeline:
─────────────────────────────────────────────────────────────►
     │              │                    │
Schema v1      Schema v2           Schema v3
(seq=1)        (seq=15)            (seq=42)
     │              │                    │
     └──events 1-14─┘──events 15-41─────┘──events 42+──►

Replay at seq=20: Use schema v2 (registered at seq=15, before seq=42)
```

## Checkpoint Store

### Timing Guarantee

The checkpoint is saved only after sinks acknowledge delivery:

```
┌────────┐   events   ┌────────┐   ack    ┌────────────┐
│ Source │ ─────────▶ │  Sink  │ ───────▶ │ Checkpoint │
└────────┘            └────────┘          │   Store    │
                                          └────────────┘
```

If the process crashes after sending to sink but before checkpoint, events will be replayed. This is the "at-least-once" guarantee — duplicates are possible, but loss is not.

### Storage Backends

Checkpoints are stored via `BackendCheckpointStore`, a thin adapter over the `StorageBackend` KV primitive. See [Storage](storage.md) for backend configuration and the full namespace map.

| Backend | Persistence | Use Case |
|---------|-------------|----------|
| `SqliteStorageBackend` | SQLite file | Single-instance production |
| `MemoryStorageBackend` | None | Testing, ephemeral deployments |
| `PostgresStorageBackend` | External DB | HA, multi-instance |

### Checkpoint-Schema Correlation

When registering schemas, the current checkpoint can be attached:

```rust
registry.register_with_checkpoint(
    tenant, db, table,
    &fingerprint,
    &schema_json,
    Some(&checkpoint_bytes),  // Current binlog position
).await?;
```

This creates a link between schema versions and source positions, enabling accurate schema lookup during replay.

## Coordinator

The coordinator orchestrates event flow between source and sinks:

### Batching

Events are accumulated until a threshold triggers flush:

- `max_events`: Event count limit
- `max_bytes`: Total serialized size limit
- `max_ms`: Time since batch started
- `respect_source_tx`: Never split source transactions

### Commit Policy

When multiple sinks are configured, the commit policy determines when the checkpoint advances:

```rust
match policy {
    All => required_acks == total_sinks,
    Required => required_acks == sinks.filter(|s| s.required).count(),
    Quorum(n) => required_acks >= n,
}
```

### Processor Pipeline

Processors run in declared order, transforming batches:

```
events ──▶ Processor 1 ──▶ Processor 2 ──▶ ... ──▶ transformed events
```

Each processor can filter, transform, or enrich events. The JavaScript processor uses `deno_core` for sandboxed execution.

### Hot Paths

Critical performance paths have been optimized:

1. **Event construction** - Minimal allocations, reuse buffers
2. **Checkpoint serialization** - Opaque bytes avoid repeated JSON encoding
3. **Sink delivery** - Batch operations reduce round trips
4. **Schema lookup** - In-memory cache with stable fingerprints

### Benchmarking

Performance is tracked via:

- **Micro-benchmarks** for specific operations
- **End-to-end benchmarks** using the Coordinator component
- **Regression detection** in CI

## Future Architecture

Planned enhancements:

- **Initial snapshot/backfill** using the Slot primitive for cursor tracking
- **Event store**: time-based replay and schema evolution using the Log primitive
- **Distributed coordination**: leader election via the Slot primitive with TTL-based leases
- **Additional sources**: MongoDB, SQL Server, TiDB
- **PostgreSQL storage validation**: chaos/recovery testing to bring it to production parity with SQLite