# Change Data Capture (CDC)

Change Data Capture (CDC) is the practice of streaming database mutations as ordered events so downstream systems can stay in sync without periodic full loads. Rather than asking "what does my data look like now?", CDC answers "what changed, and when?"

DeltaForge is a CDC engine built to make log-based change streams reliable, observable, and operationally simple in modern, containerized environments. It focuses on **row-level CDC** from MySQL binlog and Postgres logical replication to keep consumers accurate and latency-aware while minimizing impact on source databases.

> **In short**: DeltaForge tails committed transactions from MySQL and Postgres logs, preserves ordering and transaction boundaries, and delivers events to Kafka and Redis with checkpointed delivery, configurable batching, and Prometheus metrics - without requiring a JVM or distributed coordinator.

---

## Why CDC matters

Traditional data integration relies on periodic batch jobs that query source systems, compare snapshots, and push differences downstream. This approach worked for decades, but modern architectures demand something better.

**The batch ETL problem**: A nightly sync means your analytics are always a day stale. An hourly sync still leaves gaps and hammers your production database with expensive `SELECT *` queries during business hours. As data volumes grow, these jobs take longer, fail more often, and compete for the same resources your customers need.

CDC flips the model. Instead of pulling data on a schedule, you subscribe to changes as they happen.

| Aspect | Batch ETL | CDC |
|--------|-----------|-----|
| Latency | Minutes to hours | Seconds to milliseconds |
| Source load | High (repeated scans) | Minimal (log tailing) |
| Data freshness | Stale between runs | Near real-time |
| Failure recovery | Re-run entire job | Resume from checkpoint |
| Change detection | Diff comparison | Native from source |

These benefits compound as systems scale and teams decentralize:

- **Deterministic replay**: Ordered events allow consumers to reconstruct state or power exactly-once delivery with checkpointing.
- **Polyglot delivery**: The same change feed can serve caches, queues, warehouses, and search indexes simultaneously without additional source queries.

---

## How CDC works

All CDC implementations share a common goal: detect changes and emit them as events. The approaches differ in how they detect those changes.

### Log-based CDC

Databases maintain transaction logs (MySQL binlog, Postgres WAL) that record every committed change for durability and replication. Log-based CDC reads these logs directly, capturing changes without touching application tables.

```
┌─────────────┐    commits    ┌─────────────┐    tails     ┌─────────────┐
│ Application │──────────────▶│  Database   │─────────────▶│ CDC Engine  │
└─────────────┘               │   + WAL     │              └──────┬──────┘
                              └─────────────┘                     │
                                                                  ▼
                                                           ┌─────────────┐
                                                           │   Kafka /   │
                                                           │   Redis     │
                                                           └─────────────┘
```

**Advantages**:
- Zero impact on source table performance
- Captures all changes including those from triggers and stored procedures
- Preserves transaction boundaries and ordering
- Can capture deletes without soft-delete columns

**Trade-offs**:
- Requires database configuration (replication slots, binlog retention)
- Schema changes need careful handling
- Log retention limits how far back you can replay

**DeltaForge uses log-based CDC exclusively.** This allows DeltaForge to provide stronger ordering guarantees, lower source impact, and simpler operational semantics than hybrid approaches that mix log tailing with polling or triggers.

### Trigger-based CDC

Database triggers fire on INSERT, UPDATE, and DELETE operations, writing change records to a shadow table that a separate process polls.

**Advantages**:
- Works on databases without accessible transaction logs
- Can capture application-level context unavailable in logs

**Trade-offs**:
- Adds write overhead to every transaction
- Triggers can be disabled or forgotten during schema migrations
- Shadow tables require maintenance and can grow unbounded

### Polling-based CDC

A process periodically queries tables for rows modified since the last check, typically using an `updated_at` timestamp or incrementing ID.

**Advantages**:
- Simple to implement
- No special database configuration required

**Trade-offs**:
- Cannot reliably detect deletes
- Requires `updated_at` columns on every table
- Polling frequency trades off latency against database load
- Clock skew and transaction visibility can cause missed or duplicate events

---

## Anatomy of a CDC event

A well-designed CDC event contains everything downstream consumers need to process the change correctly.

```json
{
  "id": "evt_01J7K9X2M3N4P5Q6R7S8T9U0V1",
  "source": {
    "database": "shop",
    "table": "orders",
    "server_id": "mysql-prod-1"
  },
  "operation": "update",
  "timestamp": "2025-01-15T14:32:01.847Z",
  "transaction": {
    "id": "gtid:3E11FA47-71CA-11E1-9E33-C80AA9429562:42",
    "position": 15847293
  },
  "before": {
    "id": 12345,
    "status": "pending",
    "total": 99.99,
    "updated_at": "2025-01-15T14:30:00.000Z"
  },
  "after": {
    "id": 12345,
    "status": "shipped",
    "total": 99.99,
    "updated_at": "2025-01-15T14:32:01.000Z"
  },
  "schema_version": "v3"
}
```

Key components:

| Field | Purpose |
|-------|---------|
| `operation` | INSERT, UPDATE, DELETE, or DDL |
| `before` / `after` | Row state before and after the change (enables diff logic) |
| `transaction` | Groups changes from the same database transaction |
| `timestamp` | When the change was committed at the source |
| `schema_version` | Helps consumers handle schema evolution |

Not all fields are present for every operation-`before` is omitted for INSERTs, `after` is omitted for DELETEs - but the event envelope and metadata fields are consistent across MySQL and Postgres sources.

---

## Real-world use cases

### Cache invalidation and population

**Problem**: Your Redis cache serves product catalog data, but cache invalidation logic is scattered across dozens of services. Some forget to invalidate, others invalidate too aggressively, and debugging staleness issues takes hours.

**CDC solution**: Stream changes from the `products` table to a message queue. A dedicated consumer reads the stream and updates or deletes cache keys based on the operation type. This centralizes invalidation logic, provides replay capability for cache rebuilds, and removes cache concerns from application code entirely.

```
┌──────────┐     CDC      ┌─────────────┐   consumer   ┌─────────────┐
│  MySQL   │─────────────▶│   Stream    │─────────────▶│ Cache Keys  │
│ products │              │  (Kafka/    │              │ product:123 │
└──────────┘              │   Redis)    │              └─────────────┘
                          └─────────────┘
```

### Event-driven microservices

**Problem**: Your order service needs to notify inventory, shipping, billing, and analytics whenever an order changes state. Direct service-to-service calls create tight coupling and cascade failures.

**CDC solution**: Publish order changes to a durable message queue. Each downstream service subscribes independently, processes at its own pace, and can replay events after failures or during onboarding. The order service doesn't need to know about its consumers.

```
┌─────────────┐     CDC      ┌─────────────┐
│   Orders    │─────────────▶│   Message   │
│   Database  │              │   Queue     │
└─────────────┘              └──────┬──────┘
                    ┌───────────────┼───────────────┐
                    ▼               ▼               ▼
             ┌───────────┐   ┌───────────┐   ┌───────────┐
             │ Inventory │   │ Shipping  │   │ Analytics │
             └───────────┘   └───────────┘   └───────────┘
```

### Search index synchronization

**Problem**: Your Elasticsearch index drifts from the source of truth. Full reindexing takes hours and blocks search updates during the process.

**CDC solution**: Stream changes continuously to keep the index synchronized. Use the `before` image to remove old documents and the `after` image to index new content.

### Data warehouse incremental loads

**Problem**: Nightly full loads into your warehouse take 6 hours and block analysts until noon. Business users complain that dashboards show yesterday's numbers.

**CDC solution**: Stream changes to a staging topic, then micro-batch into your warehouse every few minutes. Analysts get near real-time data without impacting source systems.

### Audit logging and compliance

**Problem**: Regulations require you to maintain a complete history of changes to sensitive data. Application-level audit logging is inconsistent and can be bypassed.

**CDC solution**: The transaction log captures every committed change regardless of how it was made - application code, admin scripts, or direct SQL. Stream these events to immutable storage for compliance.

### Cross-region replication

**Problem**: You need to replicate data to a secondary region for disaster recovery, but built-in replication doesn't support the transformations you need.

**CDC solution**: Stream changes through a CDC pipeline that filters, transforms, and routes events to the target region's databases or message queues.

---

## Architecture patterns

### The outbox pattern

When you need to update a database and publish an event atomically, the outbox pattern provides exactly-once semantics without distributed transactions.

```
┌─────────────────────────────────────────┐
│              Single Transaction         │
│  ┌─────────────┐    ┌─────────────────┐ │
│  │ UPDATE      │    │ INSERT INTO     │ │
│  │ orders SET  │ +  │ outbox (event)  │ │
│  │ status=...  │    │                 │ │
│  └─────────────┘    └─────────────────┘ │
└─────────────────────────────────────────┘
                      │
                      ▼ CDC
               ┌─────────────┐
               │   Kafka     │
               └─────────────┘
```

1. The application writes business data and an event record in the same transaction.
2. CDC tails the outbox table and publishes events to Kafka.
3. A cleanup process (or CDC itself) removes processed outbox rows.

This guarantees that events are published if and only if the transaction commits.

**When to skip the outbox**: If your only requirement is to react to committed database state (not application intent), direct CDC from business tables is often simpler than introducing an outbox. The outbox pattern adds value when you need custom event payloads, explicit event versioning, or when the event schema differs significantly from table structure.

### Event sourcing integration

CDC complements event sourcing by bridging legacy systems that weren't built event-first. Stream changes from existing tables into event stores, then gradually migrate to native event sourcing.

### CQRS (Command Query Responsibility Segregation)

CDC naturally supports CQRS by populating read-optimized projections from the write model. Changes flow through the CDC pipeline to update denormalized views, search indexes, or cache layers.

---

## Challenges and solutions

### Schema evolution

Databases change. Columns get added, renamed, or removed. Types change. CDC pipelines need to handle this gracefully.

**Strategies**:
- **Schema registry**: Store and version schemas centrally (e.g., Confluent Schema Registry with Avro/Protobuf).
- **Forward compatibility**: Add columns as nullable; avoid removing columns that consumers depend on.
- **Consumer tolerance**: Design consumers to ignore unknown fields and handle missing optional fields.
- **Processor transforms**: Use DeltaForge's JavaScript processors to normalize schemas before sinks.

### Ordering guarantees

Events must arrive in the correct order for consumers to reconstruct state accurately. A DELETE arriving before its corresponding INSERT would be catastrophic.

**DeltaForge guarantees**:
- Source-order preservation per table partition (always enabled)
- Transaction boundary preservation when `respect_source_tx: true` is configured in batch settings

Kafka sink uses consistent partitioning by primary key to maintain ordering within a partition at the consumer.

### Exactly-once delivery

Network failures, process crashes, and consumer restarts can cause duplicates or gaps. True exactly-once semantics require coordination between source, pipeline, and sink.

**DeltaForge approach**:
- Checkpoints track the last committed position in the source log.
- Configurable commit policies (`all`, `required`, `quorum`) control when checkpoints advance.
- Kafka sink supports idempotent producers; transactional writes available via `exactly_once: true`.

**Default behavior**: DeltaForge provides at-least-once delivery out of the box. Exactly-once semantics require sink support and explicit configuration.

### High availability

Production CDC pipelines need to handle failures without data loss or extended downtime.

**Best practices**:
- Run multiple pipeline instances with leader election.
- Store checkpoints in durable storage (DeltaForge persists to local files, mountable volumes in containers).
- Monitor lag between source position and checkpoint position.
- Set up alerts for pipeline failures and excessive lag.

**Expectations**: DeltaForge checkpoints ensure no data loss on restart, but does not currently include built-in leader election. For HA deployments, use external coordination (Kubernetes leader election, etcd locks) or run active-passive with health-check-based failover.

### Backpressure

When sinks can't keep up with the change rate, pipelines need to slow down gracefully rather than dropping events or exhausting memory.

**DeltaForge handles backpressure through**:
- Configurable batch sizes (`max_events`, `max_bytes`, `max_ms`).
- In-flight limits (`max_inflight`) that bound concurrent sink writes.
- Blocking reads from source when batches queue up.

---

## Performance considerations

### Batching trade-offs

| Setting | Low value | High value |
|---------|-----------|------------|
| `max_events` | Lower latency, more overhead | Higher throughput, more latency |
| `max_ms` | Faster flush, smaller batches | Larger batches, delayed flush |
| `max_bytes` | Memory-safe, frequent commits | Efficient for large rows |

Start with DeltaForge defaults and tune based on observed latency and throughput.

### Source database impact

Log-based CDC has minimal impact, but consider:
- **Replication slot retention**: Paused pipelines cause WAL/binlog accumulation.
- **Connection limits**: Each pipeline holds a replication connection.
- **Network bandwidth**: High-volume tables generate significant log traffic.

### Sink throughput

- **Kafka**: Tune `batch.size`, `linger.ms`, and compression in `client_conf`.
- **Redis**: Use pipelining and connection pooling for high-volume streams.

---

## Monitoring and observability

CDC pipelines are long-running, stateful processes; without metrics and alerts, failures are silent by default. A healthy pipeline requires visibility into lag, throughput, and errors.

### Key metrics to track

| Metric | Description | Alert threshold |
|--------|-------------|-----------------|
| `cdc_lag_seconds` | Time between event timestamp and processing | > 60s |
| `events_processed_total` | Throughput counter | Sudden drops |
| `checkpoint_lag_events` | Events since last checkpoint | > 10,000 |
| `sink_errors_total` | Failed sink writes | Any sustained errors |
| `batch_size_avg` | Events per batch | Outside expected range |

DeltaForge exposes Prometheus metrics on the configurable metrics endpoint (default `:9000`).

### Health checks

- `GET /healthz`: Liveness probe - is the process running?
- `GET /readyz`: Readiness probe - are pipelines connected and processing?
- `GET /pipelines`: Detailed status of each pipeline including configuration.

---

## Choosing a CDC solution

When evaluating CDC tools, consider:

| Factor | Questions to ask |
|--------|------------------|
| **Source support** | Does it support your databases? MySQL binlog? Postgres logical replication? |
| **Sink flexibility** | Can it write to your target systems? Kafka, Redis, HTTP, custom? |
| **Transformation** | Can you filter, enrich, or reshape events in-flight? |
| **Operational overhead** | How much infrastructure does it require? JVM? Distributed coordinator? |
| **Resource efficiency** | What's the memory/CPU footprint per pipeline? |
| **Cloud-native** | Does it containerize cleanly? Support health checks? Emit metrics? |

### Where DeltaForge fits

DeltaForge intentionally avoids the operational complexity of JVM-based CDC stacks (Kafka Connect-style deployments with Zookeeper, Connect workers, and converter configurations) while remaining compatible with Kafka-centric architectures.

**DeltaForge is designed for teams that want**:
- **Lightweight runtime**: Single binary, minimal memory footprint, no JVM warmup.
- **Config-driven pipelines**: YAML specs instead of code for common patterns.
- **Inline transformation**: JavaScript processors for custom logic without recompilation.
- **Container-native operations**: Built for Kubernetes with health endpoints and Prometheus metrics.

**DeltaForge is not designed for**:
- Complex DAG-based stream processing with windowed aggregations
- Stateful joins across multiple streams  
- Sources beyond MySQL and Postgres (currently)
- Built-in schema registry integration (use external registries)

If you need those capabilities, consider dedicated stream processors or the broader Kafka ecosystem. DeltaForge excels at getting data out of databases and into your event infrastructure reliably and efficiently.

---

## Getting started

Ready to try CDC with DeltaForge? Head to the [Quickstart guide](quickstart.md) to run your first pipeline in minutes.

For production deployments, review the [Development guide](development.md) for container builds and operational best practices.