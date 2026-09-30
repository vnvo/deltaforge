# Capacity & Resource Envelope (Pilot)

This page states DeltaForge's resource behaviour for the pilot so operators can size a deployment conservatively. It is a description of how the engine behaves today, not a set of guaranteed maximums.

**Every bound below is tagged with how we know it:**

- **[measured]** - observed in a test/benchmark run (environment-dependent; not a guarantee).
- **[code-derived]** - a constant or algorithm in the source; exact, but a mechanism, not a tested ceiling.
- **[external]** - enforced by the source/sink database, not by DeltaForge.
- **[operator]** - a configuration default the operator can change.
- **[unknown]** - not yet benchmarked; treat with caution.

> These are not product ceilings. Where a number is **[code-derived]** or **[operator]** it describes a default or a mechanism, not a validated limit. Do not quote them as maximums. Run the [preflight command](pilot-support.md#deployment-preflight) and a disposable soak in your environment before committing to any number.

## At-a-glance conservative pilot guidance

| Dimension | Conservative pilot guidance | Basis |
|---|---|---|
| Pipelines (source units) per instance | Start at **1-10**; single instance only | [unknown] scale; single-owner is [code-derived] |
| Tables per source | Tens to low hundreds; watch metric cardinality and startup cost | [code-derived] O(tables²) enumeration, [unknown] at scale |
| Memory | Provision for channel-depth × event-size + in-flight batch bytes + full schema cache; **no aggregate cap exists** | [code-derived] / [unknown] |
| Throughput | Benchmark per environment; do not assume a headline number | [measured] dev-only / [unknown] |

Everything else is detailed below.

## Memory and backpressure

DeltaForge has **no aggregate (pipeline-wide or process-wide) memory or byte budget** [code-derived - confirmed absent]. Resident memory is bounded only indirectly by the levers below, so you must provision headroom rather than rely on a cap.

- **Source→coordinator channel: 32,768 items, item-count bound only, no byte cap** [code-derived]. A burst of large change events can hold up to 32,768 `SourceItem`s resident with no byte ceiling. This is the primary backpressure lever and is **not operator-configurable**. Worst-case channel memory ≈ `32768 × (largest event size)`; size RAM for your widest rows/transactions accordingly.
- **In-flight batch bytes**: bounded per batch by `batch.max_bytes` (default **16 MiB** [operator]) and per source transaction by `batch.max_tx_bytes` (default **512 MiB** [operator], only when `respect_source_tx = true`). If an operator sets these to unset/`None`, the effective cap becomes unbounded (`usize::MAX`) [code-derived] - do not disable them in the pilot.
- **Schema cache**: the full schema registry is held in memory (see [Checkpoint-store and schema-registry load](#checkpoint-store-and-schema-registry-load)); grows with tables × schema versions [code-derived, unbounded in that dimension].
- **Guidance**: budget ≈ (channel depth × typical event size) + `max_tx_bytes` per active pipeline + schema-cache growth, with generous headroom. There is no backstop if you under-provision. Actual RSS under load is **[unknown]** pending a soak in your environment.

## Transaction and batch size

`BatchConfig` defaults [operator], all overridable:

| Setting | Default | Meaning |
|---|---|---|
| `max_events` | 2000 | events per batch |
| `max_bytes` | 16 MiB | serialized bytes per batch |
| `max_ms` | 50 ms | batch flush interval |
| `respect_source_tx` | true | align batches to source transactions |
| `max_inflight` | 1 | batch depth (see [Concurrency](#concurrency-and-in-flight)) |
| `max_tx_events` | 1,000,000 | events in one source transaction |
| `max_tx_bytes` | 512 MiB | serialized bytes in one source transaction |
| `oversized_tx` | Fail | behaviour when a transaction exceeds the caps |

A single source transaction larger than `max_tx_events`/`max_tx_bytes` is failed closed by default (`oversized_tx = Fail`). Very large transactions on the source are the main way to blow past the byte budget - keep the defaults for the pilot.

## Concurrency and in-flight

- `max_inflight` default **1** [operator]. It bounds **pipeline batch depth**, not concurrent sink delivery: delivery is processed FIFO by a single delivery task to preserve checkpoint ordering, so raising it does **not** parallelize sinks [code-derived]. With `respect_source_tx = true` (the default), `max_inflight` **must** be 1 (startup fails otherwise) [code-derived]. So the effective default is single-in-flight.
- Snapshot parallelism [operator]: `max_parallel_tables` **8**, `chunk_size` **10,000** rows, `intra_table_parallel` **false**, `max_parallel_chunks` **4** (only when intra-table parallel), `lock_timeout_secs` **10**. Effective table concurrency is `min(max_parallel_tables, table_count)`.

## Queue and DLQ bytes

- DLQ (when `journal.enabled = true`, default **off**): `max_entries` **10,000** [operator], `max_age_secs` **7 days** [operator], `overflow_policy` **DropOldest** [operator] (`Reject`/`Block` also available). The `Block` policy waits at most **60 s** before failing closed [code-derived]. Journal `max_event_bytes` default **256 KB** [operator].
- Replay stream (default off): `max_entries`/`max_bytes` default **0 = unbounded** [operator]; `max_envelope_bytes` **8 MiB** [operator]; `retention_secs` 24 h.
- Secret material cap: **1 MiB** per secret [code-derived].

## Source-log retention

Retention windows are **[external]** (enforced by the source DB); preflight only estimates risk.

- **PostgreSQL**: one replication slot retains WAL. `max_slot_wal_keep_size` governs the cap. Preflight estimates snapshot WAL as ≈ **2× data bytes** and warns at **≥50%** / flags HIGH risk at **≥80%** of `max_slot_wal_keep_size` [code-derived heuristic]. `wal_status = lost/unreserved` is reported. Set `max_slot_wal_keep_size` generously or the slot can be invalidated mid-snapshot.
- **MySQL**: `binlog_expire_logs_seconds` (fallback `expire_logs_days`) governs binlog retention. Preflight estimates snapshot duration and warns at **≥50%** / HIGH at **≥80%** of the retention window [code-derived heuristic]; post-snapshot it fails closed if the captured binlog file was purged during the snapshot.
- Snapshot throughput estimate used by both: **20 MB/s per worker** [code-derived heuristic, not measured].

## Database connections

Per **pipeline** [code-derived; scales with snapshot config]:

- **PostgreSQL** - snapshot: `1` anchor + `min(max_parallel_tables, tables)` workers (up to 8) + up to `×max_parallel_chunks` per worker if `intra_table_parallel` (worst case with defaults + intra-table ≈ **~41**). Steady CDC: **1** replication connection (= 1 walsender) + occasional short-lived control connections. No source-side connection pool - each is an individual connection.
- **MySQL** - snapshot: `1` lock connection + `min(max_parallel_tables, tables)` workers + `1` binlog-position guard. Steady CDC: **1** binlog stream + ad-hoc short-lived `mysql_async` pools for control queries (library-default pool sizing, **not explicitly capped in DeltaForge code** [unknown]).
- **Storage backend (PostgreSQL)**: one connection pool per backend instance (shared across pipelines), `max_size` = deadpool default **≈ CPU × 4** [library-default], plus one background TTL-sweep task.

Size `max_connections` on the source and on the PostgreSQL storage DB for the sum across all pipelines at their snapshot peak, not steady state.

## PostgreSQL slots and walsenders

- **One replication slot and one walsender per PostgreSQL source** [code-derived]. Provision `max_replication_slots` and `max_wal_senders` ≥ number of PG source pipelines on that server, with headroom for other consumers.

## Checkpoint-store and schema-registry load

- **Checkpoint writes: one `put_raw` per required sink per committed batch, with no coalescing** [code-derived]. Write volume ≈ (batches/sec) × (required sink count). At the `max_ms = 50` floor that is up to ~20 batch-commits/s per pipeline × sinks. Size the checkpoint/storage backend for that write rate across all pipelines. Reads are change-driven (the source is notified on commit), not polled.
- **Schema registry loads ALL namespaces and ALL versions at startup** [code-derived]: a full `kv_list` + per-key log replay into an in-memory cache. Startup cost and memory grow with (tables × schema versions) and are **[unknown]** at large catalog sizes - a concern for many-table deployments.

## Metric cardinality

- Five schema-sensing metrics carry a **per-table** label (`deltaforge_schema_events_total`, `deltaforge_schema_sensing_seconds`, `deltaforge_schema_sensing_cache_hits_total`, `deltaforge_schema_sensing_cache_misses_total`, `deltaforge_schema_evolutions_total`) [code-derived]. Series count scales **linearly with table count** (× histogram buckets for the timing metric) and is **not bounded** [unknown at scale]. Other metrics are per-pipeline/per-sink/per-source (bounded). With many tables, budget Prometheus cardinality accordingly or restrict scraping of the schema-sensing metrics.

## Table and source-unit counts

- **No coded limit on tables per source or pipelines per instance** [confirmed absent]. Practical limits come from: the O(tables²) table-enumeration dedup during schema load [code-derived], per-table metric cardinality, the full schema-registry load at startup, and connection/slot math above.
- **Single-instance requirement**: run exactly one DeltaForge process against a given state store (see [Pilot Support Envelope](pilot-support.md#topology-single-owner-per-source)) [code-derived containment].
- **Conservative pilot recommendation**: start with a small number of pipelines (single digits to ~10) and tens-to-low-hundreds of tables per source, and grow only after a soak in your environment. Larger catalogs are **[unknown]** pending benchmark.

## Throughput

Throughput is environment-dependent and is **[measured] only on a developer machine** (single runs, heavy desktop contention observed), so no headline number is published as a guarantee. Treat sustained throughput as **[unknown]** for your environment until you run a soak. The engine's backpressure (the 32,768-item channel and batch caps above) is what bounds memory when a sink cannot keep up.

## Known scaling caveats (pending benchmark)

These are **[unknown]** and should be validated before scaling a pilot up:

- Aggregate/RSS memory under sustained load (no coded cap).
- Startup time and memory for large schema catalogs (full registry load).
- Per-table metric cardinality at hundreds/thousands of tables.
- O(tables²) schema-enumeration cost at large table counts.
- MySQL `mysql_async` control-pool sizing under many concurrent pipelines.
