# Capacity & Resource Envelope

This page states DeltaForge's resource behaviour so operators can size a deployment conservatively. It is a description of how the engine behaves today, not a set of guaranteed maximums.

**Every bound below is tagged with how we know it:**

- **[measured]** - observed in a test/benchmark run (environment-dependent; not a guarantee).
- **[code-derived]** - a constant or algorithm in the source; exact, but a mechanism, not a tested ceiling.
- **[external]** - enforced by the source/sink database, not by DeltaForge.
- **[operator]** - a configuration default the operator can change.
- **[unknown]** - not yet benchmarked; treat with caution.

> These are not product ceilings. Where a number is **[code-derived]** or **[operator]** it describes a default or a mechanism, not a validated limit. Do not quote them as maximums. Run the [preflight command](deployment-support.md#deployment-preflight) and a disposable soak in your environment before committing to any number.

## At-a-glance conservative starting guidance

| Dimension | Conservative starting guidance | Basis |
|---|---|---|
| Pipelines (source units) per instance | Unvalidated starting point: a small number (single digits); single instance only. Not a supported ceiling either way. | [unknown] scale; single-owner is [code-derived] |
| Tables per source | Tens to low hundreds; watch snapshot cost. A CDC start neither enumerates the catalog nor loads schemas up front, the schema registry's cache is bounded, and each source's resident schemas are capped (a compact per-table record still grows with the tables used; see [Schema registry at scale](#schema-registry-at-scale)); an initial snapshot's work still grows with the catalog, linearly (see [Initial Snapshots](snapshots.md#scale)) | registry [measured] to 1M tables; CDC restart [measured] flat from 20 to 1,000 matched tables; initial snapshot [measured] near-linear from 1,000 to 10,000 tables, beyond that [unknown] |
| Memory | Provision for channel-depth × event-size + in-flight batch bytes + the schema cache budget (default 64 MiB); **no aggregate cap exists** | [code-derived] / [unknown] |
| Throughput | Benchmark per environment; do not assume a headline number | [measured] dev-only / [unknown] |

Everything else is detailed below.

## Memory and backpressure

DeltaForge has **no aggregate (pipeline-wide or process-wide) memory or byte budget** [code-derived - confirmed absent]. Resident memory is bounded only indirectly by the levers below, so you must provision headroom rather than rely on a cap.

- **Source→coordinator channel: 32,768 items, item-count bound only, no byte cap** [code-derived]. A burst of large change events can hold up to 32,768 `SourceItem`s resident with no byte ceiling. This is the primary backpressure lever and is **not operator-configurable**. Worst-case channel memory ≈ `32768 × (largest event size)`; size RAM for your widest rows/transactions accordingly.
- **In-flight batch bytes**: bounded per batch by `batch.max_bytes` (default **16 MiB** [operator]) and per source transaction by `batch.max_tx_bytes` (default **512 MiB** [operator], only when `respect_source_tx = true`). If an operator sets these to unset/`None`, the effective cap becomes unbounded (`usize::MAX`) [code-derived] - do not disable them.
- **Schema cache**: only the latest version of tables in use is cached, process-wide, bounded by `--schema-cache-max-bytes` (default **64 MiB**, a conservative estimate of resident bytes) and `--schema-cache-max-entries` (default **50,000**) [operator]. Older versions and unused tables stay in the state store and are read on demand. See [Schema registry at scale](#schema-registry-at-scale).
- **Guidance**: budget ≈ (channel depth × typical event size) + `max_tx_bytes` per active pipeline + the schema cache budget, with generous headroom. There is no backstop if you under-provision. Actual RSS under load is **[unknown]** pending a soak in your environment.

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

A single source transaction larger than `max_tx_events`/`max_tx_bytes` is failed closed by default (`oversized_tx = Fail`). Very large transactions on the source are the main way to blow past the byte budget - keep the defaults.

## Concurrency and in-flight

- `max_inflight` default **1** [operator]. It bounds **pipeline batch depth**, not concurrent sink delivery: delivery is processed FIFO by a single delivery task to preserve checkpoint ordering, so raising it does **not** parallelize sinks [code-derived]. With `respect_source_tx = true` (the default), `max_inflight` **must** be 1 (startup fails otherwise) [code-derived]. So the effective default is single-in-flight.
- Snapshot parallelism [operator]: `discovery_page_size` **1,000** (catalog rows per discovery query), `max_parallel_tables` **8**, `chunk_size` **10,000** rows, `intra_table_parallel` **false**, `max_parallel_chunks` **4** (only when intra-table parallel), `lock_timeout_secs` **10**. Effective table concurrency is `min(max_parallel_tables, table_count)`.

## Queue and DLQ bytes

- DLQ (when `journal.enabled = true`, default **off**): `max_entries` **10,000** [operator], `max_age_secs` **7 days** [operator], `overflow_policy` **DropOldest** [operator] (`Reject`/`Block` also available). The `Block` policy waits at most **60 s** before failing closed [code-derived]. Journal `max_event_bytes` default **256 KB** [operator].
- Replay stream (default off): `max_entries`/`max_bytes` default **0 = unbounded** [operator]; `max_envelope_bytes` **8 MiB** [operator]; `retention_secs` 24 h.
- Secret material cap: **1 MiB** per secret [code-derived].

## Source-log retention

Retention windows are **[external]** (enforced by the source DB); preflight only estimates risk.

- **PostgreSQL**: one replication slot retains WAL. `max_slot_wal_keep_size` governs the cap. Preflight estimates snapshot WAL as ≈ **2× data bytes** and warns at **≥50%** / flags HIGH risk at **≥80%** of `max_slot_wal_keep_size` [code-derived heuristic]. `wal_status = lost/unreserved` is reported. Set `max_slot_wal_keep_size` generously or the slot can be invalidated mid-snapshot.
- **MySQL**: `binlog_expire_logs_seconds` (fallback `expire_logs_days`) governs binlog retention. Preflight estimates snapshot duration and warns at **≥50%** / HIGH at **≥80%** of the retention window [code-derived heuristic]; post-snapshot it fails closed if the captured binlog file was purged during the snapshot.
- Snapshot throughput estimate used by both: **20 MB/s per worker** [code-derived heuristic, not measured].
- During a snapshot, retention the anchor needs (the PostgreSQL slot, the MySQL anchor binlog file) and the anchor's age (`snapshot.max_anchor_age_secs`, default 24 h) are checked continuously: a warning incident at 80%, and at the limit the generation stops and **blocks** before anything is lost (`snapshot_anchor_unavailable`) [code-derived, tested]. See [Bounds and blocking](snapshots.md#bounds-and-blocking).

## Database connections

Per **pipeline** [code-derived; scales with snapshot config]:

- **Snapshot connection cap** [operator]: every snapshot connection, across all pipelines of the process, is taken from one pool of `--max-snapshot-connections` (default **64**); each source's share is `snapshot.max_snapshot_connections` (default `max_parallel_tables x max_parallel_chunks + 2`). A snapshot that cannot get its 3 base connections queues holding nothing (`deltaforge_snapshot_queued`). See [Connection limits](snapshots.md#connection-limits).
- **PostgreSQL** - snapshot: `1` anchor + `min(max_parallel_tables, tables)` workers (up to 8) + up to `×max_parallel_chunks` per worker if `intra_table_parallel` (worst case with defaults + intra-table ≈ **~41**), within the source's share. Steady CDC: **1** replication connection (= 1 walsender) + occasional short-lived control connections. No source-side connection pool - each is an individual connection.
- **MySQL** - snapshot: `1` lock connection + `min(max_parallel_tables, tables)` workers + `1` binlog-position guard, within the source's share. Steady CDC: **1** binlog stream + ad-hoc short-lived `mysql_async` pools for control queries (library-default pool sizing, **not explicitly capped in DeltaForge code** [unknown]).
- **Storage backend (PostgreSQL)**: one connection pool per backend instance (shared across pipelines), `max_size` = deadpool default **≈ CPU × 4** [library-default], plus one background TTL-sweep task.

Size `max_connections` on the source and on the PostgreSQL storage DB for the snapshot peak, not steady state: at most `--max-snapshot-connections` snapshot connections across the process.

## PostgreSQL slots and walsenders

- **One replication slot and one walsender per PostgreSQL source** [code-derived]. Provision `max_replication_slots` and `max_wal_senders` ≥ number of PG source pipelines on that server, with headroom for other consumers.

## Checkpoint-store and schema-registry load

- **Checkpoint writes: one `put_raw` per checkpointed sink per committed batch, with no coalescing** [code-derived]:

  ```
  checkpoint writes/sec = committed batches/sec × checkpointed sinks
  ```

  This is workload-dependent and has **no configured QPS ceiling**. `max_ms = 50` is a time-based *flush ceiling*, not a rate cap: batches also flush on `max_events`, `max_bytes`, and transaction boundaries, so committed-batches/sec can be much higher than `1000/max_ms` under load. Size the checkpoint/storage backend for the actual committed-batch rate × checkpointed sinks across all pipelines. Reads are change-driven (the source is notified on commit), not polled.
- **Checkpoint reads are scoped to the source** [code-derived, measured]: a source reads its per-sink checkpoints by key prefix, answered by the store as a bounded key range (PostgreSQL: a partial index on the checkpoint namespace). Cost does not grow with the number of other sources or pipelines sharing the store.
- **Schema registry startup reads one record** [measured]: no namespace scan and no history replay; each table's latest schema is read on first use and cached within the budget above. See below.

## Schema registry at scale

Measured with the dense-catalog harness (`crates/scale-harness`, `registry-scale`; see its README to reproduce) at commit `664838a`, on a developer machine, single runs, SQLite state store. The numbers are indicative, not guarantees; the shape (what does and does not grow) is the point.

Synthetic registries: *N* tables per source × 2 sources with the same table names (colliding across sources), 2-3 versions each, generated through the normal registration path. 100 active tables are looked up.

| | 100K tables × 3 versions | 1M tables × 2 versions |
|---|---|---|
| Registry startup | 1 key read, 0.04 ms | 1 key read, 0.04 ms |
| First lookup of a table (cold) | 1 read; p50 31 µs, p99 230 µs | 1 read; p50 21 µs, p99 35 µs |
| Repeat lookup (cached) | no store access; p50 3 µs | no store access; p50 2 µs |
| Working set 4× the cache budget | stays within budget, evicts | stays within budget, evicts |
| 64 concurrent first lookups of one table | 1 store read | 1 store read |
| Reads of another source's records | 0 | 0 |
| Time to first CDC event after a restart (PostgreSQL / MySQL) | 34 ms / 21 ms | 45 ms / 21 ms |

- **PostgreSQL state store** (100K tables × 2 versions × 2 sources, local container): the same operation counts as SQLite - startup 1 key read (1.4 ms), a cold lookup 1 read (p50 205 µs, p99 816 µs, network round trips), cached lookups no store access, 1 read for 64 concurrent first lookups, 0 reads of other sources. Migration of 10K tables: ~56-60 s at 1 version and ~185 s at 5 versions per table (vs ~4 s / ~14 s on SQLite); memory the same ~2.4-2.5 KB per mapped table. Registrations ran at ~250/s against ~3,600/s on SQLite.
- **Time to first CDC event**: the source's own registry holds *N* synthetic tables (they are not in the source database); a row committed while the pipeline was stopped is timed from source start until the sink receives it. The storage calls in that window are identical at 100K and 1M tables (PostgreSQL: 12 key reads, 8 prefix-scoped checkpoint listings, 1 write, 1 schema read; MySQL: 5, 1, 1, 1) with no registry scan. This isolates the registry.
- **CDC restart against a large live catalog** [measured]: the source's table pattern matches 20 or 1,000 real tables in the database (only one of them changes). A CDC-only restart does the same work at both sizes, measured up to the arrival of the first event: the same storage operations, calls and returned and written bytes per primitive (PostgreSQL 11 key reads, 3 latest-version reads; MySQL 9 key reads; the timer-driven rereads of the sink checkpoints for WAL feedback are not counted), no registry enumeration, and the same number of statements on the server (PostgreSQL 9, MySQL 27); no catalog enumeration and no schema load for tables without changes. Both runs create the same 1,000 tables; only how many the pattern matches differs. Reproduce with `cargo test -p scale-harness --test live_ttfce -- --include-ignored` or `registry-scale --live postgres,mysql --live-catalog-tables N`.
- **Per-source schema cache** [code-derived]: the resident (heavyweight) table schemas are capped at 4,096 tables or about 64 MiB of serialized schema per source, whichever is reached first, with least-recently-used eviction. The limit is fixed (not an operator setting). An evicted table is rebuilt from durable history as exactly the version it resolved to, so the source keeps a compact record per table resolved in the current lineage generation (table key, version, sequence, fingerprint). That record is not bounded: it grows with the distinct tables used since the source started (or last changed lineage), and the 64 MiB does not include it or the cache's key and map overhead. Bounding or paging these records is a later capacity improvement.
- **MySQL schema selection caches** [code-derived]: each MySQL source's validated table timelines and row-schema selections share one budget of 4,096 entries, evicted least recently used first (`deltaforge_mysql_selection_cache_evictions_total{pipeline,source}`). An evicted entry is rebuilt from the durable registry and activation records, never from the live catalog. The budget is fixed (not an operator setting).
- **Pre-upgrade schema history migration** (`deltaforge schema-migrate`) [measured]: memory grows with the number of **mapped tables**, not with history length: about 2.4-2.8 KB per mapped table (10K tables: ~39 MiB peak; 100K tables: ~243 MiB peak), unchanged between 1 and 5 versions per table and between page sizes. Time: 100K tables took ~36 s at 1 version and ~135 s at 5 versions per table. For very large catalogs, split the migration with `--tenant` / `--source` or several mapping files.

## Metric cardinality

- **Default: independent of the table count** [measured]. No metric carries a table label unless the pipeline enables `metrics.per_table`; a live test records identical series sets for pipelines over 10 and 1,000 tables (apart from batch flush causes, a closed set that varies with timing). See [Observability](observability.md#metric-cardinality-and-per-table-detail).
- **Opt-in per-table detail** [code-derived, measured]: at most `max_tables + 1` table values (default 100, up to 10,000) per metric and remaining label set; the per-table lag holds at most `max_tables + 1` series per pipeline and drops a table's series after `lag_idle_secs` without events. The histogram (`deltaforge_schema_sensing_seconds`) multiplies this by its buckets. Budget Prometheus accordingly before raising `max_tables`.

## Table and source-unit counts

- **No coded limit on tables per source or pipelines per instance** [confirmed absent]. Practical limits come from: initial snapshot time (linear in the tables and rows copied; the durable plan is bounded by `snapshot.max_plan_bytes` / `max_plan_items`), and connection/slot math above. A CDC start does no per-table work (see below). The schema registry is no longer one of them ([measured] to 1M tables per source).
- **Single-instance requirement**: run exactly one DeltaForge process against a given state store (see [Supported Deployment Envelope](deployment-support.md#topology-single-owner-per-source)) [code-derived containment].
- **Conservative starting configuration (unvalidated, not a supported limit)**: a small number of pipelines (single digits) and tens-to-low-hundreds of tables per source is a reasonable place to start, and grow only after a soak in your environment. We have not measured enough to claim either that larger configurations are unsupported or that this range is universally safe - both directions are **[unknown]** pending benchmark.

## Throughput

Throughput is environment-dependent and is **[measured] only on a developer machine** (single runs, heavy desktop contention observed), so no headline number is published as a guarantee. The figures in [Performance Tuning](performance.md) are such development-machine observations, useful for the relative effect of settings only; comparative benchmarks are defined in the [benchmark design](https://github.com/vnvo/deltaforge/blob/main/docs/design/benchmarks.md) and run after rc.1. Treat sustained throughput as **[unknown]** for your environment until you run a soak. Backpressure bounds queued item count and batch/transaction payloads where their byte limits are enabled, but it does not provide a process-wide memory bound.

## Known scaling caveats (pending benchmark)

These are **[unknown]** and should be validated before scaling up:

- Aggregate/RSS memory under sustained load (no coded cap).
- Initial snapshot time for sources capturing many tables: discovery reads the catalog in keyset pages (`discovery_page_size`, one query per page) and each table's schema is resolved once, from the schema registry when known or the source catalog otherwise (a CDC restart does neither; see above).
- Initial snapshots of large catalogs [measured to 10,000 tables]: snapshot state is a durable, paged plan with constant-size boundaries and no progress rewrites; resident queue state is bounded by `discovery_page_size` and `max_parallel_tables`. From 1,000 to 10,000 tables, boundary bytes grew 10x and wall time 10.5x (PostgreSQL) and 12.2x (MySQL). Total process memory still grows with the tables captured, through the schema caches (MySQL peak RSS growth 17.9 MiB at 1K, 48.4 MiB at 10K). Figures, environment and scope in [Initial Snapshots](snapshots.md#scale). **Qualification beyond 10,000 tables per source (up to 400,000 per source, and fleets of 50 to 120 million tables) is a milestone after rc.1, not a current support claim.**
- MySQL `mysql_async` control-pool sizing under many concurrent pipelines.
