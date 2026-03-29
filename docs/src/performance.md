# Performance Tuning

This guide covers throughput optimization for DeltaForge CDC pipelines, based on profiling and benchmarking with the chaos test suite.

> **Note:** These results and recommendations are a starting point. Every deployment has unique requirements — hardware, network topology, database workload patterns, event sizes, and downstream consumer capacity all affect real-world throughput. Profile your own workload and iterate.

## Benchmark Results

Measured on Docker containers on a single developer machine (not dedicated infrastructure), draining a 1-10M row backlog to a single-node Kafka broker.

### With tuned batching (recommended)

`batch.max_events=16000`, `batch.max_bytes=16MB`, `batch.max_inflight=4`, `linger.ms=0`

| Source | Mode | Avg (events/s) | Peak (events/s) |
|--------|------|----------------|-----------------|
| MySQL | at-least-once | **151K** | **159K** |
| MySQL | exactly-once | **134K** | **143K** |
| Postgres | at-least-once | **57K** | **64K** |
| Postgres | exactly-once | **53K** | **55K** |

Exactly-once overhead is **~7-11%** when batch sizes are properly tuned.

### Why `max_bytes` matters

These results show the impact of a small `max_bytes` (3MB) with `max_events=8000`:

| Source | Mode | Avg (events/s) | Peak (events/s) |
|--------|------|----------------|-----------------|
| MySQL | at-least-once | **110K** | **122K** |
| MySQL | exactly-once | **48K** | **57K** |

A 3MB byte limit caps batches at ~6,000 events regardless of `max_events`, making transaction commits proportionally expensive. The default `max_bytes` is 16MB — sufficient for batches up to ~32K events at typical event sizes.

Your numbers will differ based on hardware, network latency, event size, and Kafka/database configuration.

## Key Tuning Parameters

### Batch Size (`batch.max_events` + `batch.max_bytes`)

The single most impactful setting. Larger batches amortize per-batch overhead (Kafka produce, transaction commit, checkpoint write, metrics recording) across more events.

**Important:** `max_events` and `max_bytes` both cap batch size — whichever triggers first wins. If you set `max_bytes` too low, it will silently cap your batches regardless of `max_events`. The default (16MB) accommodates batch sizes up to ~32K events at typical event sizes.

Recommended for high-throughput drain/catch-up:

```yaml
spec:
  batch:
    max_events: 16000
    max_bytes: 16777216   # 16MB — room for 16K events
    max_ms: 100
    max_inflight: 4
```

For steady-state pipelines with lower latency requirements, the defaults (`max_events=2000, max_bytes=3MB`) are fine.

### Kafka Linger (`linger.ms`)

Controls how long rdkafka waits before sending a produce request. **This is the most common throughput bottleneck when left at high values.**

- `linger.ms=20`: each small batch waits 20ms before sending, capping throughput
- `linger.ms=5` (sink built-in default): good balance for steady-state
- `linger.ms=0`: maximum throughput for drain/catch-up/high-intensity workloads

The internal coordinator enqueues entire batches (hundreds to thousands of messages) in a tight loop, so rdkafka batches naturally without needing linger time. Higher linger values only add idle wait per produce.

Override via `client_conf` in the sink config:

```yaml
sinks:
  - type: kafka
    config:
      client_conf:
        linger.ms: "0"
```

### Batch Pipelining (`batch.max_inflight`)

Controls how many batches can be queued between the accumulation loop and the delivery task. Higher values overlap batch building with Kafka delivery.

- `max_inflight=1`: sequential delivery (default)
- `max_inflight=4`: recommended for high-throughput workloads (adjust per need)

The delivery task processes batches in FIFO order, so checkpoint and event delivery ordering is always preserved regardless of the inflight setting. 
This config essentially allows the read from source to continue without waiting for processing in other parts of the pipline.

```yaml
spec:
  batch:
    max_events: 4000
    max_ms: 100
    max_inflight: 4
```

### Schema Sensing

Disable schema sensing during drain/catch-up for maximum throughput:

```yaml
spec:
  schema_sensing:
    enabled: false
```

Re-enable for steady-state operation when schema tracking is needed. Be mindful, schema sensing is a CPU-intensive task.

### Proxy Bypass - Chaos/Bench Testing

When running with Toxiproxy (chaos testing), use `--no-proxy` to bypass the proxy for direct database and Kafka connections. The proxy adds measurable overhead to throughput.

## Source-Specific Tuning

### MySQL

MySQL binlog is inherently efficient because `WriteRowsEvent` batches multiple rows into a single event.

**MySQL server settings that affect CDC throughput:**

- **`binlog-row-image=FULL`** — required for CDC but sends all columns per row. If your use case allows it, `MINIMAL` reduces binlog event size significantly (only changed columns are sent).
- **`binlog_transaction_dependency_tracking=WRITESET`** — enables parallel replication metadata. While DeltaForge reads sequentially, this can reduce replication lag on replicas feeding DeltaForge.
- **`max_allowed_packet`** — increase if you have large blob/text columns. The default (64MB) is usually sufficient.
- **`binlog_expire_logs_seconds`** — set high enough that DeltaForge can recover from outages without losing its checkpoint position. 7 days is a safe starting point.

**DeltaForge settings for MySQL:**

- **`tables`** — be specific. Subscribing to `*.*` forces DeltaForge to process table map events for every table, even those it discards.
- **`snapshot.chunk_size`** — for initial snapshots of large tables, increase chunk size (default 10,000) to reduce round trips.

### PostgreSQL

Postgres logical replication (pgoutput) sends one WAL message per row change, making it more per-message intensive than MySQL binlog.

**PostgreSQL server settings that affect CDC throughput:**

- **`wal_level=logical`** — required. No throughput impact vs. `replica`.
- **`max_wal_senders`** — ensure enough slots for DeltaForge plus any replicas. Default (10) is usually sufficient.
- **`wal_sender_timeout`** — increase from the default (60s) if DeltaForge pauses processing for extended periods (e.g., during pipeline restarts). `300s` is a safer value.
- **`wal_keep_size`** — set large enough to cover outage windows. If DeltaForge disconnects and WAL is recycled, the replication slot becomes invalid and requires re-snapshot.
- **Replica identity** — `ALTER TABLE ... REPLICA IDENTITY FULL` sends full row images for updates/deletes. `DEFAULT` (primary key only) reduces WAL message size but limits the `before` image in CDC events.
- **Publication scope** — create publications with explicit table lists (`FOR TABLE ...`) rather than `FOR ALL TABLES` to reduce WAL decoding overhead on the server.

**DeltaForge settings for PostgreSQL:**

- **Batch writes in transactions** — if your writer can group inserts into `BEGIN; INSERT ...; INSERT ...; COMMIT`, the server sends fewer BEGIN/COMMIT WAL messages, reducing per-event overhead.
- **`tables`** — use specific patterns. Broad patterns force schema loading and filtering for unneeded tables.

The throughput gap between Postgres and MySQL is primarily due to protocol-level differences (one WAL message per row vs. batched rows), not code inefficiency.

## Exactly-Once Delivery Overhead

Enabling `exactly_once: true` on sinks adds per-batch transaction overhead:

### Kafka Transactional Producer

Each batch is wrapped in `begin_transaction()` / `commit_transaction()`. The transaction commit adds a constant ~1-3ms per batch (broker-side two-phase commit), so larger batches amortize the cost better.

**Measured overhead** (Docker containers, single developer machine, 1-10M row drain):

| Source | at-least-once | exactly-once | Overhead |
|--------|--------------|-------------|----------|
| MySQL | 151K events/s | 134K events/s | ~11% |
| Postgres | 57K events/s | 53K events/s | ~7% |

These numbers use tuned batch settings (`max_events=16000, max_bytes=16MB`). With a small `max_bytes` (e.g. 3MB), batches are capped at ~6K events regardless of `max_events`, making the transaction commit disproportionately expensive. The default `max_bytes` (16MB) is sufficient for most workloads.

Key considerations:

- **`transaction.timeout.ms`** (set to 60s by DeltaForge): if a batch takes longer than this to deliver, the broker aborts the transaction. Increase for very large batches or high-latency networks.
- **`transactional.id`** must be unique per pipeline-sink pair. DeltaForge sets this automatically to `deltaforge-{pipeline_id}-{sink_id}`.
- **Producer fencing**: if two producers share the same `transactional.id`, the broker fences (kills) the older one. This is detected as a `Fatal` error and stops the pipeline. Ensure only one instance of each pipeline runs at a time.

### NATS JetStream

Deduplication uses the `Nats-Msg-Id` header (server-side). No client-side transaction overhead, but the server maintains a dedup window. Configure `duplicate_window` on the stream to match your replay window (default 2 minutes).

### Redis Streams

Idempotency keys are embedded in the XADD payload. No transaction overhead on the Redis side — deduplication is the consumer's responsibility. This is a Tier 2 guarantee (at-least-once with idempotency keys for consumer-side dedup).

### Per-Sink Checkpoints

Each sink maintains its own checkpoint, committed independently after successful delivery. The source replays from the minimum checkpoint across all sinks. This means:

- **Faster sinks are not held back** by slower ones — they advance their own checkpoints independently.
- **Adding a new sink** to an existing pipeline triggers a replay from the source's earliest available position for that sink only.
- **Checkpoint storage overhead** scales linearly with the number of sinks (one key per sink per source).

## Profiling

Use the chaos UI's built-in CPU profiler to capture flamegraphs during drain runs:

1. Start a drain scenario from the chaos UI
2. Once the drain phase starts (step 5/6), click **Record** on the target container
3. The generated flamegraph SVG includes pipeline config, batch settings, and connection mode in the subtitle automatically

Or from the command line:

```bash
# Start drain in terminal 1
cargo run -p chaos --release -- --scenario backlog-drain --source mysql --no-proxy

# Capture flamegraph in terminal 2 (after drain phase starts)
docker exec <container-name> perf record -F 99 -p 1 -g --call-graph dwarf -o /tmp/perf.data -- sleep 30
```

Requires the profiling image (`deltaforge:dev-profile`) which includes `perf` and debug symbols.

Key areas to watch in flamegraphs:

| Area | What it means |
|------|---------------|
| `serialize_event` / `format_escaped_str` | JSON serialization — consider smaller batches if dominant |
| `recv` / `[unknown]` kernel stacks | I/O wait for source data — protocol-bound |
| `_rjem_je_*` | jemalloc allocation pressure — large batches increase this |
| `rd_kafka_*` / `LZ4_compress` | Kafka produce and compression overhead |
| `check_and_split` | Coordinator batch accumulation |
| `epoll_wait` / `park_timeout` | Idle time — pipeline is I/O bound, not CPU bound |

## Running the Drain Benchmark

The backlog drain benchmark measures catch-up throughput: how fast DeltaForge replays a pre-built backlog of 1M rows.

```bash
# MySQL — requires the soak compose profile
docker compose -f docker-compose.chaos.yml --profile soak up -d
cargo run -p chaos --release -- --scenario backlog-drain --source mysql --no-proxy \
  --drain-max-events 4000 --drain-max-ms 100 --drain-kafka-conf linger.ms=0

# Postgres — requires the pg-soak compose profile
docker compose -f docker-compose.chaos.yml --profile pg-soak up -d
cargo run -p chaos --release -- --scenario backlog-drain --source postgres --no-proxy \
  --drain-max-events 4000 --drain-max-ms 100 --drain-kafka-conf linger.ms=0
```

The benchmark:
1. Stops the pipeline and saves its checkpoint
2. Writes 1M rows to the source database using 32 concurrent writers
3. Resumes the pipeline and measures how fast events appear in Kafka
4. Reports avg/p50/peak events/s with full configuration in the output

Tune `--drain-max-events`, `--drain-max-ms`, and `--drain-kafka-conf` to experiment with different settings. The chaos UI also exposes these as form fields for interactive tuning.
