# Guarantees & Correctness

This page defines DeltaForge's data delivery guarantees, ordering model, transaction semantics, failure handling, and operational boundaries. The guarantees below describe current behavior. Where a verification is planned rather than already in place, the [Correctness Test Matrix](#correctness-test-matrix) marks it explicitly, so read a guarantee together with its test status.

## Delivery Guarantees

### Per-sink delivery tiers

| Sink | Delivery guarantee | Dedup mechanism | Consumer action required |
|------|-------------------|-----------------|------------------------|
| **Kafka** (`exactly_once: true`) | Transactional atomic-batch delivery; **at-least-once across restart** | Each batch is one Kafka transaction, so `read_committed` consumers never see a partial batch; a restart can replay an already-committed batch as a new transaction (duplicate) | Set `isolation.level=read_committed`, **and** dedup by event ID for cross-restart duplicates |
| **Kafka** (default) | At-least-once (idempotent producer) | Retries are deduped by rdkafka; crash-replay produces duplicates | Dedup by event ID or idempotency key |
| **NATS JetStream** | At-least-once + server-side dedup | `Nats-Msg-Id` header within `duplicate_window` | Configure `duplicate_window` on stream |
| **Redis Streams** | At-least-once + consumer-side dedup | `idempotency_key` field in XADD payload | Check `idempotency_key` before processing |
| **HTTP/Webhook** | At-least-once | Retry on 5xx/timeout; no server-side dedup | Consumer must be idempotent (use event `id`) |
| **S3** (Parquet / JSON Lines) | At-least-once at file granularity, atomic at file boundary | Same file may be re-emitted with a different ULID on retry/replay | Dedup downstream via `MERGE INTO` or `event_id` |

**Terminology rule:** DeltaForge does not claim end-to-end exactly-once delivery for any sink. Every sink is at-least-once; the differences are in what each sink adds on top. Kafka `exactly_once: true` adds transactional atomic-batch visibility (a `read_committed` consumer sees a whole batch or none of it) but still delivers at-least-once across a restart, so consumers must dedup to reach exactly-once end to end. NATS and Redis add a dedup mechanism (server-side or consumer-side) whose effectiveness depends on configuration or consumer behavior outside DeltaForge's control. Calling any of these "exactly-once" without the consumer's cooperation would be misleading.

### What "at-least-once" means

- **No data loss**: every event from the source is delivered to the sink at least once. Checkpoints are saved only after the sink acknowledges delivery — never before.
- **Duplicates on crash recovery**: if DeltaForge crashes after delivering a batch but before saving the checkpoint, that batch is replayed on restart. Consumers must handle duplicates (see [Consumer Guidance](#consumer-guidance) below).
- **No silent drops**: events are never discarded. On a transient delivery error a sink applies a bounded in-sink retry (exponential backoff, finite attempts). If those attempts are exhausted on a `required` sink, the checkpoint does not advance and the batch is re-delivered from the source on the next pipeline restart (source replay) - there is no unbounded in-session retry loop. See [Error Classification & Retry](#error-classification--retry).

### What Kafka transactions guarantee (`exactly_once: true`)

`exactly_once: true` gives **transactional atomic-batch delivery**, not end-to-end exactly-once. Each batch is wrapped in a Kafka transaction (`begin_transaction` / `commit_transaction`), so consumers using `isolation.level=read_committed` see a whole batch or none of it - never a partial batch. If a transaction fails before commit, it is aborted and re-attempted from the same checkpoint position.

The restart boundary is still **at-least-once**: DeltaForge advances its own checkpoint only after the Kafka transaction commits, so a crash between the commit and the checkpoint persist replays that batch on restart as a **new** transaction. A `read_committed` consumer then sees the batch a second time. To reach exactly-once end to end, consumers must dedup on the event `id` (or idempotency key), which is stable across replays.

The transactional-producer overhead is **~7-11%** with properly tuned batch sizes. See the [Performance guide](performance.md#transactional-producer-overhead) for benchmark details.

## Ordering Model

### Within a source

Events are emitted in the source's native order:

- **MySQL**: binlog file + position order. `WriteRowsEvent` batches preserve row order within each binlog event.
- **PostgreSQL**: LSN (Log Sequence Number) order. One WAL message per row change.

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

The batch accumulator will not split a batch at a point that would separate rows from the same transaction. If the batch limit (`max_events` or `max_bytes`) is reached mid-transaction, the batch grows beyond the limit to include all remaining rows in that transaction.

### What this guarantees

- All rows from one database transaction appear in the **same batch**.
- A batch is DeltaForge's unit of delivery accounting: a sink's checkpoint advances only if the whole batch succeeds. Whether a *consumer* observes the batch atomically depends on the sink:
  - **Kafka `exactly_once: true`**: the batch is one Kafka transaction, so a `read_committed` consumer sees all rows from the DB transaction together or not at all - within a single delivery. Across a restart the batch can be replayed as a new transaction, so the consumer may see the same transaction twice (at-least-once); dedup on event `id` to collapse it.
  - **Non-transactional sinks** (Redis, HTTP, NATS, S3, ClickHouse, Elasticsearch): DeltaForge does not make the batch atomic for the consumer. A sink failure partway through a batch can leave some rows visible downstream, and a retry or restart re-delivers the batch, producing duplicates. These sinks are at-least-once; use their stated dedup mechanism.
- **Cross-table transactions**: a transaction spanning tables A and B is emitted as a single batch containing events for both tables, tagged with the same `tx_id`, and delivered as a unit. This is stronger than "tagged but not grouped" - all events from one DB transaction are in one batch. Consumer-visible atomicity for that batch still follows the per-sink rule above.

### Precise transaction semantics

To avoid ambiguity, here is exactly what DeltaForge guarantees about transactions:

- Events from one source transaction are emitted **contiguously** within a single batch.
- Multi-table transactions preserve commit grouping — all rows from tables A and B in one DB transaction appear in the same batch.
- **Within a single sink**, the batch is the unit of delivery accounting: the sink's checkpoint advances only if the whole batch succeeds. Consumer-visible atomicity of that batch holds only for a transactional sink (Kafka `exactly_once: true`) and only within one delivery; see [What this guarantees](#what-this-guarantees).
- **Across heterogeneous sinks**, DeltaForge does not guarantee atomic commit. Kafka may commit a transaction while Redis is still retrying. Each sink's checkpoint tracks its own progress independently.
- Retries and restarts do not break transaction grouping (a replayed batch contains the same events in the same order), but they can re-deliver a transaction that was already delivered. Delivery is at-least-once; consumers dedup on event `id`.

### Edge cases

- A single database transaction that exceeds `max_events` or `max_bytes` is still kept in one batch. The limits are exceeded rather than the transaction being split.
- With `respect_source_tx: false`, batches are split purely by size/time limits regardless of transaction boundaries. Cross-table transaction atomicity is not preserved in this mode.

## Initial-snapshot anchoring

The initial snapshot and the CDC stream meet at a single anchor so that **no committed row is lost** across the boundary. The checks below run only when a snapshot runs; CDC-only pipelines (`snapshot.mode = never`) are unaffected.

- **PostgreSQL** anchors CDC at the replication slot's consistent point `C`. Every row committed at or before `C` is in the snapshot; every row after `C` is in the CDC stream. Rows committed in `(C, snapshot-export]` fall in **both** - a **bounded at-least-once overlap**, not exactly-once. Current-state sinks (with `version_source: source_position`) converge via last-writer-wins; append-only sinks receive the overlap twice. A snapshot completed under the older anchor is flagged by the gauge `deltaforge_snapshot_unsafe_anchor = 1` until a safe re-snapshot.
- **MySQL** brackets the anchor under a brief `FLUSH TABLES WITH READ LOCK`: all snapshot workers open their consistent-snapshot transactions and the binlog position + GTID set are captured while the lock is held, so every worker shares one view that matches the captured position exactly. This **closes a real initial-snapshot data-loss window** present in earlier versions (independent per-worker snapshots with the position captured afterward). It requires `gtid_mode = ON`, `binlog_format = ROW`, InnoDB tables, and the global `RELOAD` privilege; managed MySQL without `RELOAD` **fails closed** rather than snapshotting unsafely (no silent fallback).

## Failure Isolation

### Per-sink independence

All sinks deliver concurrently. One sink's failure does **not** block other sinks:

1. The coordinator dispatches the same batch to all sinks simultaneously.
2. Each sink's delivery result is collected independently.
3. Only sinks that delivered successfully get their checkpoints advanced.
4. Failed sinks remain at their prior checkpoint position — they will receive the same batch again on retry or restart.

### Coordinator-level deadline (defense in depth)

In addition to each sink's own internal timeout (e.g. `send_timeout_secs` on the S3/Kafka/Redis/NATS/HTTP sinks), the pipeline supports an outer deadline that the coordinator applies to every `send_batch` call:

```yaml
spec:
  sink_batch_deadline_secs: 60   # outer bound on any one sink's send_batch
```

Behavior:
- Each sink's future is wrapped in `tokio::time::timeout(deadline, ...)`. If a sink exceeds the deadline, the coordinator gives up on it for that batch, classifies it as `SinkError::Backpressure`, and continues with the others.
- A timed-out **required** sink causes its checkpoint to stay behind (same as any failure); the source replays from `MIN(checkpoints)` on restart.
- A timed-out **optional** sink advances none of its state for that batch; same in-session-loss / on-restart-recovery semantics as documented in [Required vs. optional sinks](#required-vs-optional-sinks).
- Metric: `deltaforge_coordinator_sink_timeout_total{pipeline,sink}`.
- Default: `None` (no outer bound). Set this for defense-in-depth against a sink that hangs despite its own internal timeout (bug, deadlock, infinite retry loop in a dependency).

The two timeout layers are complementary:
| Layer | Catches | Granularity |
|-------|---------|-------------|
| `send_timeout_secs` (sink-internal) | Misbehavior of the sink's underlying API/library (e.g. object_store retries, rdkafka queue) | Per sink, per batch |
| `sink_batch_deadline_secs` (coordinator) | Misbehavior of the sink itself (deadlock, slow Rust code, ignored cancellation) | All sinks, per batch |

### Required vs. optional sinks

Each sink is marked `required: true` (default) or `required: false`:

- **Required**: must succeed for the pipeline to consider the batch delivered. Each sink applies bounded sink-local retries (finite attempts with backoff) within a single `send_batch`; if those are exhausted for a required sink, no checkpoint advances for any sink and the pipeline stops. There is no in-session pipeline-level retry of the batch - recovery is by source replay from `MIN(checkpoints)` on the next restart.
- **Optional** (best-effort): failures are logged but don't prevent the pipeline from advancing. The failed sink's own checkpoint stays at its last-successful position; the source's MIN-checkpoint reader doesn't go back, but **on the next pipeline restart**, the source replays from the failed sink's stuck position and the failed sink catches up.

#### What an optional sink failure means in practice

When an optional sink fails for batch B (events at LSN 100):

1. Required sinks succeed → their checkpoints advance to LSN 100.
2. The optional sink's checkpoint **stays at its prior position** (LSN 50, say).
3. The pipeline continues to batch B+1 (events at LSN 101+). The source's in-memory position is now 101, not 50.
4. The optional sink receives batch B+1 with events at LSN 101+. It may succeed or fail again — irrelevant to LSN 100, which was already "passed over" in this session.
5. **Until restart**: if the optional sink stays failing, the source's in-memory position keeps marching forward. The failed events between the sink's stuck checkpoint and "now" are **not in any retry queue** for this session.
6. **On restart**: the source reads `MIN(required_cp, optional_cp) = 50` and replays from 50. The optional sink (back from outage, presumably) catches up. Required sinks see duplicates and dedup or accept (idempotent design).

This is the practical reality of `required: false`:

- ✅ A failed optional sink **does not stall the pipeline**.
- ✅ A failed optional sink **does not lose data permanently** — restart-replay recovers it.
- ❌ A failed optional sink **does not catch up in-flight** — events between failure and restart are only delivered on restart.
- ❌ A long-running pipeline with a chronically failing optional sink **accumulates a growing replay gap** — the longer between failures and restart, the more events the failed sink will re-deliver after restart.

For the S3 sink specifically (where extended outages are realistic — S3 throttling, regional issues), the operational choice is:

| Goal | Configuration |
|------|---------------|
| **S3 must not miss any event in-flight, accept source backpressure** | `required: true` (default). Slow/down S3 backpressures the source. |
| **Kafka must keep flowing during S3 outages, accept replay-on-restart latency for S3 catch-up** | `required: false` on S3, `required: true` on Kafka. Pipeline keeps moving for Kafka; S3 catches up after the next restart. |
| **Truly independent throughput** | Run two pipelines with the same source DSN — one for Kafka, one for S3. Each has its own coordinator and backpressure. Or use a Kappa-style architecture (Kafka → second pipeline → S3) with Kafka's retention as the natural retry buffer. |

#### Multi-batch retries vs in-batch retries

Two distinct retry layers exist and should not be conflated:

1. **In-batch retries** (within a single `send_batch` call): each sink's internal retry policy (exponential backoff, finite attempts). These are fast (<1s typical) and apply to transient errors like `Queue full` or `Connection timeout`.
2. **Pipeline-level retries** (across batches): triggered by `required: true` failures. The coordinator does NOT auto-retry the same batch from the source channel — instead, the source-side replay mechanism handles recovery on restart.

`required: false` skips pipeline-level retries for the failing sink. There is no "background queue" that retries the failed batch later in the same session.

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

- A fast sink is never held back by a slow one during normal operation: while the pipeline keeps running, the source's in-memory read position does not rewind for a lagging sink.
- Recovery is **not** selective per sink. On restart the source resumes from the `MIN` checkpoint across all sinks, so every sink - including ones already ahead - is re-delivered the events from that minimum forward. Sinks that were ahead receive duplicates and must dedup (see [On crash recovery](#on-crash-recovery)).
- Adding a new sink lowers the `MIN` to that sink's earliest position, so on the next restart the source replays from there and the existing sinks are re-delivered those events too (they dedup). The new sink is not backfilled in isolation.

### Fatal errors

Some errors are unrecoverable and stop the pipeline immediately:

- **Kafka ProducerFenced**: another producer instance started with the same `transactional.id`. The broker fences the old producer permanently.
- **Permanent auth revocation**: credentials are invalid and retrying won't help.

Fatal errors return `SinkError::Fatal` and are not retried. The pipeline stops and requires operator intervention.

### S3 sink atomicity guarantees

The S3 sink commits at **file granularity**, not event granularity. Specifically:

- **File-level atomicity**: a Parquet or JSONL file is only visible at its final S3 key after the multipart-complete call returns success. Readers never see a partial file. Verified by tests (`abandon_all_produces_no_visible_file`, `drop_pool_without_close_produces_no_visible_file`).
- **At-least-once per file**: if `send_batch` succeeds and the process crashes before the source checkpoint is committed, the same events will be re-delivered on restart and produce a **new file with a different ULID**. The original file is also retained — there is no automatic deduplication. Downstream consumers must dedup via `MERGE INTO` or `event_id`.
- **Mid-upload crash**: a process killed during a multipart upload leaves orphan parts on S3 (not a visible file). The bucket lifecycle policy `AbortIncompleteMultipartUpload: DaysAfterInitiation=1` reclaims them within 24h. **This policy is an operational prerequisite** — without it, abandoned uploads accumulate storage cost. See [deployment](deployment.md#s3-sink-prerequisites).
- **Per-row DLQ**: encoder failures are isolated per row via a slow-path retry on the same writer. The bad row appears in `BatchResult.dlq_failures` and is routed to the DLQ; the rest of the batch is written normally. The fast path (success) costs nothing extra — per-event retry only triggers on encoder errors. ParquetFileWriter and JsonLinesFileWriter both roll back their state on per-call failure so the writer remains safe to reuse during the retry loop.

For exactly-once at the event level (i.e., dedup without consumer cooperation), the planned Iceberg sink in Phase 2 uses atomic snapshot commits to make file appearance and event commit equivalent operations.

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

**S3:**

| Error | Classification | Behavior |
|-------|---------------|----------|
| Network timeout / partition | Retryable (in-batch) | `object_store` retries the part upload with its built-in backoff; if still failing, `send_batch` returns `SinkError::Io` |
| `503 SlowDown` (throttling) | Retryable (in-batch) | `object_store` honors `Retry-After`; `send_batch` waits |
| `send_timeout_secs` exceeded | Bounded wait | After `send_timeout_secs` (default 60s), `send_batch` returns `SinkError::Backpressure`. Caps the worst-case latency a single batch can contribute. Coordinator routes per `required` — block (default) or log+continue. Useful when upstream `object_store` retries would otherwise stack into multi-minute waits. |
| `403 AccessDenied` / SigV4 mismatch | Non-retryable | `SinkError::Io` immediately; check credentials and region |
| `NoSuchBucket` | Non-retryable | `SinkError::Io`; create the bucket or fix the config |
| Encoder failure (e.g. value doesn't fit Decimal128 precision) | Per-row isolated | Slow-path per-event retry isolates the bad row; it lands in `BatchResult.dlq_failures` and is routed to the DLQ. The other events in the batch are written normally. |
| Mid-multipart abandon (process killed) | N/A | File never becomes visible at the destination key. Orphan parts cleaned by the bucket's lifecycle policy (operational prerequisite — see [deployment](deployment.md#s3-sink-prerequisites)) |

### After retry exhaustion

If all retry attempts fail for a retryable error, the error is propagated to the coordinator. The coordinator's behavior depends on the commit policy:

- **Required sink**: the batch is not committed and its checkpoint stays put. The coordinator does not re-inject the same batch from the source channel in-session; recovery happens by source replay from `MIN(checkpoints)` on the next restart (see [Multi-batch retries vs in-batch retries](#multi-batch-retries-vs-in-batch-retries)).
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
3. Sinks that were already ahead of the minimum position receive duplicate events - they must handle these idempotently (Kafka `read_committed` still sees the replayed batch as a new transaction, so dedup on event `id` there too).
4. Sinks that were behind receive their missing events.

### Checkpoint storage

Checkpoints are stored in SQLite (default) with WAL mode and `synchronous=NORMAL` for durability. The checkpoint store survives `SIGKILL` — no graceful shutdown required for checkpoint safety.

## Event replay

[Event replay](replay.md) re-delivers captured commit units from the durable journal to
selected sinks. Its correctness guarantees:

- **At-least-once re-delivery.** The durable cursor is advanced only after a sink
  acknowledges an envelope. A crash between the acknowledgement and the cursor persist causes
  the acknowledged envelope to be delivered again on restart, so dedup-capable sinks must
  absorb duplicates (the same idempotency key applies).
- **No checkpoint mutation during replay.** A running job never advances or rewinds the source
  or per-sink checkpoints; only normal live delivery moves checkpoints after the handoff.
- **Pause and handoff.** Selected sinks are excluded from live delivery and from commit-policy
  evaluation while a job runs, and rejoin the live set only at an acknowledged handoff to a
  frozen tail `H`; the first live delivery to a restored sink is strictly after `H`.
- **Fail-closed capture.** A commit unit that cannot be captured verbatim aborts the pipeline
  rather than advancing the checkpoint past uncaptured data.

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

## Correctness Test Matrix

This matrix maps guarantees to their verification. Rows marked **Exists** have a test in the suite today; rows marked **Planned** do not yet - treat those guarantees as design intent pending verification, not as tested behavior.

| Guarantee | Test | Type | Status |
|-----------|------|------|--------|
| No data loss (at-least-once) across restart | `kafka_outage_across_restart_loses_no_events_and_holds_checkpoint` (`kafka_outage_restart_e2e`) | Integration | Exists |
| Kafka transactional atomic-batch delivery | `kafka_sink_exactly_once_*` | Integration | Exists |
| Backlog-drain throughput (PostgreSQL → Kafka) | `pg_to_kafka_backlog_drain_throughput` (`throughput_e2e`) | Integration (perf) | Exists |
| Producer fencing detection | `kafka_sink_exactly_once_producer_fencing` | Integration | Exists |
| Per-primary-key ordering | Events keyed by PK → same Kafka partition | By design | Verified via Kafka partition assignment |
| Transaction boundary preservation | `respect_source_tx` + `check_and_split` coordinator logic | Unit | Exists |
| Per-sink checkpoint independence | `test_per_sink_checkpoint_only_advances_on_success` | Unit | Exists |
| Per-sink checkpoint legacy fallback | `per_sink_proxy_falls_back_to_legacy_key` | Unit | Exists |
| Commit policy gate before checkpoint | `test_per_sink_checkpoint_only_advances_on_success` | Unit | Exists |
| DLQ routes per-event failures | `test_dlq_routes_failed_events_and_pipeline_continues` | Unit | Exists |
| DLQ all-fail batch | `test_dlq_all_events_fail_no_send` | Unit | Exists |
| DLQ overflow (drop_oldest) | `dlq::overflow_drop_oldest` | Unit | Exists |
| DLQ overflow (reject) | `dlq::overflow_reject_drops_new` | Unit | Exists |
| DLQ overflow (block) | `dlq::overflow_block_waits_for_ack` | Unit | Exists |
| DLQ cleanup expired | `dlq::cleanup_expired_removes_old_entries` | Unit | Exists |
| Partial batch timer flush | `test_partial_batch_flushed_by_timer` | Unit | Exists |
| Sink unreachable across restart (no loss, checkpoint held) | `kafka_outage_across_restart_loses_no_events_and_holds_checkpoint` | Integration | Exists |
| Sink outage / lagging sink recovery | `pg_two_sink_restart_resumes_from_slowest_sink` | Integration | Exists |
| Schema drift handling (adapt / halt / in-stream) | `pg_schema_drift_adapt_*`, `pg_schema_drift_halt_*`, `mysql_failover_schema_drift_*` | Integration | Exists |
| MySQL failover detection | `mysql_failover_position_lost_stops_source`, `mysql_failover_streaming_resumes_after_identity_change` | Integration | Exists |
| Postgres failover detection | `postgres_failover_slot_absent_stops_source`, `postgres_failover_streaming_resumes_after_identity_change` | Integration | Exists |
| Replication slot absent detection | `postgres_failover_slot_absent_stops_source` | Integration | Exists |
| Source-side network partition (mid-stream) | dedicated e2e | Integration | Planned |
| MySQL binlog purge detection | dedicated e2e | Integration | Planned |
| NATS dedup within window | Verify `Nats-Msg-Id` prevents duplicates | Integration | Planned |
| Redis idempotency key | Verify consumer-side dedup via key | Integration | Planned |
| Snapshot → CDC handoff | No gaps; engine-specific documented overlap | Integration | Planned |

## Limitations

These are **not guaranteed** and are documented honestly:

- **No cross-table global ordering** — events from different tables may be interleaved. This is by design; enforcing global order would require single-threaded delivery and cap throughput. Use `respect_source_tx: true` to preserve ordering within database transactions.
- **No stateful stream processing** — DeltaForge does not support joins, aggregations, or windowing. For stateful processing, consume DeltaForge's output with Apache Flink, ksqlDB, or Kafka Streams.
- **Dead letter queue** — when `journal.enabled: true`, poison events (serialization/routing failures) are routed to a DLQ instead of blocking the pipeline. Without DLQ enabled, a single bad event will still block. See the [DLQ page](dlq.md).
- **No in-session retry for optional sinks** — when `required: false` and a sink fails, the failed batch is **not retried in the same session**. The failed sink's checkpoint stays at its prior position; events are re-delivered only on pipeline restart via source replay. For sinks with realistic outage windows (e.g., S3 throttling, cross-region issues), the operator must weigh source backpressure (`required: true`) against replay-on-restart latency (`required: false`). See [Required vs. optional sinks](#required-vs-optional-sinks).
- **S3 sink — at-least-once at file granularity** — duplicate events across a crash boundary appear in two files with different ULIDs. Per-row DLQ isolates encoder failures, but does not provide exactly-once delivery on its own; that requires the Phase 2 Iceberg sink (atomic snapshot commits). Consumers must dedup downstream via `MERGE INTO` or `event_id`. See [S3 sink atomicity guarantees](#s3-sink-atomicity-guarantees).
- **S3 sink — lifecycle policy required for production** — DeltaForge does not track multipart upload IDs externally. Abandoned multiparts are reclaimed by the bucket's `AbortIncompleteMultipartUpload` lifecycle rule. Without this rule, S3 storage cost accumulates on every aborted batch.
- **Snapshot consistency** — initial snapshots use lock-free parallel reads. The snapshot is eventually consistent with the CDC stream; there may be a brief overlap period where both snapshot rows and CDC events for the same row are delivered. Consumers should use the event timestamp or idempotency key to resolve.
