# Guarantees & Correctness

This page defines DeltaForge's data delivery guarantees, ordering model, transaction semantics, failure handling, and operational boundaries. Every claim here is backed by the implementation — no aspirational statements.

## Delivery Guarantees

### Per-sink delivery tiers

| Sink | Delivery guarantee | Dedup mechanism | Consumer action required |
|------|-------------------|-----------------|------------------------|
| **Kafka** (`exactly_once: true`) | **End-to-end exactly-once** | Kafka two-phase commit per batch | Set `isolation.level=read_committed` |
| **Kafka** (default) | At-least-once (idempotent producer) | Retries are deduped by rdkafka; crash-replay produces duplicates | Dedup by event ID or idempotency key |
| **NATS JetStream** | At-least-once + server-side dedup | `Nats-Msg-Id` header within `duplicate_window` | Configure `duplicate_window` on stream |
| **Redis Streams** | At-least-once + consumer-side dedup | `idempotency_key` field in XADD payload | Check `idempotency_key` before processing |

**Terminology rule:** "exactly-once" is used only when DeltaForge guarantees no duplicates without consumer cooperation. All other sinks are "at-least-once" with a stated dedup mechanism. This distinction matters — calling NATS or Redis "exactly-once" would be misleading because dedup depends on server configuration or consumer behavior outside DeltaForge's control.

### What "at-least-once" means

- **No data loss**: every event from the source is delivered to the sink at least once. Checkpoints are saved only after the sink acknowledges delivery — never before.
- **Duplicates on crash recovery**: if DeltaForge crashes after delivering a batch but before saving the checkpoint, that batch is replayed on restart. Consumers must handle duplicates (see [Consumer Guidance](#consumer-guidance) below).
- **No silent drops**: events are never discarded. If delivery fails, the batch is retried with exponential backoff until it succeeds or a fatal error stops the pipeline.

### What "exactly-once" means (Kafka)

With `exactly_once: true`, each batch is wrapped in a Kafka transaction (`begin_transaction` / `commit_transaction`). Consumers using `isolation.level=read_committed` only see committed batches — no partial deliveries. If a transaction fails, it is aborted and retried from the same checkpoint position.

Exactly-once overhead is **~7-11%** with properly tuned batch sizes. See the [Performance guide](performance.md#exactly-once-delivery-overhead) for benchmark details.

## Ordering Model

### Within a source

Events are emitted in the source's native order:

- **MySQL**: binlog file + position order. `WriteRowsEvent` batches preserve row order within each binlog event.
- **PostgreSQL**: LSN (Log Sequence Number) order. One WAL message per row change.
- **Turso**: `change_id` order (monotonically increasing).

DeltaForge does not reorder events. The source order is preserved through the pipeline.

### Within a batch

All events in a batch maintain their source order. The delivery task processes batches in FIFO order from a bounded channel — no reordering between batches.

### Per-primary-key ordering (the core guarantee)

**DeltaForge guarantees per-primary-key ordering within a table under non-sharded operation.** This means: for any single row identified by its primary key, all changes (INSERT, UPDATE, DELETE) are delivered to the sink in the exact order they occurred in the source database.

For Kafka specifically: the default message key is the serialized primary key, so events for the same row always go to the same partition and arrive in order. With dynamic routing (`key` template), ordering follows the resolved key — events with the same key are ordered; different keys may land in different partitions.

### Cross-table ordering

There is **no global ordering** across tables. Events from different tables may be interleaved across batches. This is by design — enforcing global ordering would require single-threaded delivery, which would cap throughput.

However, when `batch.respect_source_tx: true` (the default), all rows from a single database transaction are kept in the same batch (see [Transaction Boundaries](#transaction-boundaries) below). This preserves causal ordering within a transaction.

### Ordering under retries

When a batch delivery fails and is retried, the batch is re-delivered as a unit in the same order. No reordering occurs within or across retries. The `max_inflight=1` setting (default) ensures strict ordering; with `max_inflight > 1`, batches are still delivered in FIFO order by the single-threaded delivery task.

### Cross-sink ordering

All sinks receive the same batch simultaneously. The relative order of events is identical across all sinks.

## Transaction Boundaries

### How it works

When `batch.respect_source_tx: true` (the default), the coordinator checks each event's `tx_end` flag before splitting a batch:

- **MySQL**: `tx_end` is set on the last row of each XID (transaction commit) event.
- **PostgreSQL**: `tx_end` is set on the COMMIT WAL record.
- **Turso**: each change is its own transaction (`tx_end` always true).

The batch accumulator will not split a batch at a point that would separate rows from the same transaction. If the batch limit (`max_events` or `max_bytes`) is reached mid-transaction, the batch grows beyond the limit to include all remaining rows in that transaction.

### What this guarantees

- All rows from one database transaction appear in the **same batch**.
- Each batch is delivered **atomically** to each sink (all events in the batch succeed or fail together).
- With Kafka `exactly_once: true`, the entire batch is committed as a single Kafka transaction — consumers see all rows from the DB transaction atomically.
- **Cross-table transactions**: a transaction spanning tables A and B is emitted as a single batch containing events for both tables, tagged with the same `tx_id`. The batch is delivered atomically. This is stronger than "tagged but not grouped" — all events from one DB transaction are in one batch and delivered as a unit.

### Edge cases

- A single database transaction that exceeds `max_events` or `max_bytes` is still kept in one batch. The limits are exceeded rather than the transaction being split.
- With `respect_source_tx: false`, batches are split purely by size/time limits regardless of transaction boundaries. Cross-table transaction atomicity is not preserved in this mode.

## Failure Isolation

### Per-sink independence

All sinks deliver concurrently. One sink's failure does **not** block other sinks:

1. The coordinator dispatches the same batch to all sinks simultaneously.
2. Each sink's delivery result is collected independently.
3. Only sinks that delivered successfully get their checkpoints advanced.
4. Failed sinks remain at their prior checkpoint position — they will receive the same batch again on retry or restart.

### Required vs. optional sinks

Each sink is marked `required: true` (default) or `required: false`:

- **Required**: must succeed for the pipeline to consider the batch delivered. If a required sink fails, no checkpoint advances for any sink.
- **Optional** (best-effort): failures are logged but don't prevent the pipeline from advancing. Optional sinks that fail will catch up on restart via replay from their own checkpoint.

### Commit policy

The commit policy determines when checkpoints advance:

| Policy | Behavior |
|--------|----------|
| `required` (default) | All `required: true` sinks must acknowledge |
| `all` | Every sink (required and optional) must acknowledge |
| `quorum` | At least N sinks must acknowledge |

The policy is checked **before** any checkpoint is committed. If the policy isn't satisfied, no sink advances — this prevents optional sinks from getting ahead of failed required sinks.

### Per-sink checkpoints

Each sink maintains its own checkpoint key (`{source_id}::sink::{sink_id}`). On restart, the source replays from the **minimum** checkpoint across all sinks. This means:

- A fast sink is never held back by a slow one during normal operation.
- A slow or failed sink only causes replay for itself, not re-delivery to sinks that are already ahead.
- Adding a new sink triggers replay from the source's earliest available position for that sink only.

### Fatal errors

Some errors are unrecoverable and stop the pipeline immediately:

- **Kafka ProducerFenced**: another producer instance started with the same `transactional.id`. The broker fences the old producer permanently.
- **Permanent auth revocation**: credentials are invalid and retrying won't help.

Fatal errors return `SinkError::Fatal` and are not retried. The pipeline stops and requires operator intervention.

## Error Classification & Retry

### Retry behavior by sink

All sinks use exponential backoff with jitter. The classification determines whether an error is retried:

**Kafka:**

| Error | Classification | Behavior |
|-------|---------------|----------|
| Queue full | Retryable | Backoff, retry (100ms base, 10s max, 3 attempts) |
| Message timeout | Retryable | Backoff, retry |
| Broker connection failure | Retryable | Backoff, retry |
| Authentication failure | Non-retryable | Fail immediately |
| Message too large | Non-retryable | Fail immediately |
| Producer fenced | **Fatal** | Pipeline stops |
| Transaction commit failure (fatal) | **Fatal** | Pipeline stops |

**NATS:**

| Error | Classification | Behavior |
|-------|---------------|----------|
| Connection failure | Retryable | Backoff, retry (50ms base, 5s max, 3 attempts) |
| Publish timeout | Retryable | Backoff, retry |
| Authentication failure | Non-retryable | Fail immediately |
| No responders | Non-retryable | Fail immediately |

**Redis:**

| Error | Classification | Behavior |
|-------|---------------|----------|
| Connection failure | Retryable | Backoff, retry (50ms base, 5s max, 3 attempts) |
| Command timeout | Retryable | Backoff, retry |
| NOAUTH / WRONGPASS | Non-retryable | Fail immediately |
| Permission denied | Non-retryable | Fail immediately |

### After retry exhaustion

If all retry attempts fail for a retryable error, the error is propagated to the coordinator. The coordinator's behavior depends on the commit policy:

- **Required sink**: the batch is not committed, and the pipeline will retry the entire batch on the next cycle.
- **Optional sink**: the failure is logged, and the pipeline continues with other sinks.

## Checkpoint Semantics

### When checkpoints are saved

The checkpoint commit follows a strict sequence:

```
1. Accumulate events from source into a batch
2. Run processors (transform, filter)
3. Deliver batch to ALL sinks concurrently
4. Check commit policy (required/all/quorum)
5. Commit per-sink checkpoints (only for successful sinks)
```

**Key invariant**: a checkpoint is saved only after the sink has acknowledged delivery AND the commit policy is satisfied. This is the foundation of at-least-once delivery.

### On crash recovery

1. DeltaForge reads per-sink checkpoints from the checkpoint store.
2. The source resumes from the **minimum** checkpoint across all sinks.
3. Sinks that were already ahead of the minimum position receive duplicate events — they must handle these idempotently (or use exactly-once mode).
4. Sinks that were behind receive their missing events.

### Checkpoint storage

Checkpoints are stored in SQLite (default) with WAL mode and `synchronous=NORMAL` for durability. The checkpoint store survives `SIGKILL` — no graceful shutdown required for checkpoint safety.

## Backpressure

DeltaForge implements end-to-end backpressure without dropping events:

```
Source → [event channel] → Accumulator → [batch channel (max_inflight)] → Delivery → Sinks
```

1. **Sink slow**: delivery task blocks waiting for sink acknowledgement.
2. **Batch channel full**: accumulator blocks waiting to enqueue the next batch (bounded by `max_inflight`).
3. **Event channel full**: source blocks waiting to enqueue the next event.
4. **Source slows**: the database connection idles until the channel has capacity.

No events are dropped at any stage. Backpressure propagates from the slowest sink all the way back to the source connection.

`max_inflight` controls the pipeline depth: higher values allow overlapping batch building with delivery (better throughput), lower values reduce memory usage and latency.

## Consumer Guidance

### Idempotency key

Every event has a deterministic idempotency key in the format:

```
{tenant}|{db}.{table}|{tx_id}|{event_id}
```

This key is identical across replays — the same source event always produces the same key.

- **Kafka with `exactly_once: true`**: set `isolation.level=read_committed` on consumers. No application-level dedup needed.
- **Kafka without `exactly_once`**: use the event's `id` field (UUID v7) or the idempotency key to detect duplicates.
- **NATS JetStream**: server-side dedup via `Nats-Msg-Id` header. Configure `duplicate_window` on the stream to cover your maximum expected downtime (default: 2 minutes).
- **Redis Streams**: check the `idempotency_key` field in the stream entry before processing. Use a Redis SET or application-level tracking to remember processed keys.

### Dedup window

How long should consumers remember processed event IDs? Match your maximum expected DeltaForge downtime:

| Scenario | Recommended window |
|----------|-------------------|
| Normal operation (no crashes) | No dedup needed (at-most-once per run) |
| Planned restarts | 5 minutes |
| Unplanned crashes with auto-restart | 15-30 minutes |
| Disaster recovery | Match your RPO |

## Limitations

These are **not guaranteed** and are documented honestly:

- **No cross-table global ordering** — events from different tables may be interleaved. This is by design; enforcing global order would require single-threaded delivery and cap throughput. Use `respect_source_tx: true` to preserve ordering within database transactions.
- **No stateful stream processing** — DeltaForge does not support joins, aggregations, or windowing. For stateful processing, consume DeltaForge's output with Apache Flink, ksqlDB, or Kafka Streams.
- **No dead letter queue (yet)** — a single poison event that consistently fails serialization will block the pipeline. Planned for a future release.
- **No schema registry integration (yet)** — schema sensing detects structural drift and can halt on breaking changes, but there is no Confluent Schema Registry or Avro/Protobuf encoding support. Planned for a future release.
- **Snapshot consistency** — initial snapshots use lock-free parallel reads. The snapshot is eventually consistent with the CDC stream; there may be a brief overlap period where both snapshot rows and CDC events for the same row are delivered. Consumers should use the event timestamp or idempotency key to resolve.
