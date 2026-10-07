# Comparative Benchmarks - Design and Workload Definition

**Status:** Design only. Required before rc.1 (milestone ruling of 2026-10-04); the runs themselves are deferred until after rc.1 and are not a gate.
**Date:** 2026-10-07
**Scope:** rc.1 item `competitive-benchmarks`: how DeltaForge is compared with Debezium on the PostgreSQL->Kafka and MySQL->Kafka paths, which workloads are run, what is measured, how each run is checked for correctness, and how a run is reproduced. Nothing here is a result. Until runs following this design are published with their method and environment, DeltaForge publishes no headline throughput figure ([Capacity Envelope](../src/capacity-envelope.md#throughput)).

## 1. Questions the benchmarks answer

1. **Backlog drain:** how fast does each system deliver an existing backlog of changes to Kafka?
2. **Steady-state latency:** at a fixed write rate the system can sustain, what is the end-to-end delay from commit to a consumer seeing the change (p50, p99)?
3. **Resource cost:** CPU and memory of the CDC process while doing (1) and (2).
4. **Initial load:** how long does an initial snapshot take, for one large table and for many small tables?
5. **Restart:** time from process start to the first change delivered after a CDC-only restart, without and with a large catalog.

Each answer is reported per system and workload, with the full configuration. No question is answered by a single combined score.

## 2. Systems and versions

| Component | Role | Pinning |
|---|---|---|
| DeltaForge | system under test | release image by digest, plus the git commit it was built from |
| Kafka Connect + Debezium PostgreSQL and MySQL connectors | system under test | Connect image and connector plugin versions by digest |
| Apache Kafka (single broker, KRaft) | shared destination | image by digest; the same broker configuration for both systems |
| PostgreSQL 17, MySQL 8.4 | sources | images by digest |
| Workload driver and verifier | load and checks | built from the same commit as the harness |

Versions live in one lock file of the harness (`bench/versions.lock`, created with the harness). A run that uses any other version is a different benchmark and is reported as such.

## 3. Comparison methodology (parity)

The two systems are configured to do the same work, and every remaining difference is recorded with the results.

| Aspect | DeltaForge | Debezium |
|---|---|---|
| Message value | `envelope: { type: native }`, JSON: the change object itself (`before`, `after`, `source`, `op`, `ts_ms`) | `JsonConverter` with `value.converter.schemas.enable=false`: the payload object itself, with no `schema`/`payload` wrapper |
| Message key | `key: "${after.id}"`: the primary key for events with an after image only; a delete resolves to an empty key (DeltaForge has no automatic primary-key key and no before/after-coalescing key expression; its default key is an idempotency key) | the primary key struct for every operation, `key.converter.schemas.enable=false` |
| Topic per table | `topic: "bench.${source.schema}.${source.table}"` (PostgreSQL), `"bench.${source.db}.${source.table}"` (MySQL) | `topic.prefix=bench`, default `<prefix>.<schema>.<table>` / `<prefix>.<db>.<table>` naming |
| Delivery | at-least-once (`exactly_once` unset) | at-least-once (no exactly-once source support enabled) |
| Producer | `acks=all`, idempotence on (sink defaults), compression `lz4` | `producer.override.acks=all`, `enable.idempotence=true`, `compression.type=lz4` |
| Snapshot mode | `snapshot.mode: initial` for the snapshot workloads, `never` otherwise | `snapshot.mode=initial` / `no_data` (PostgreSQL), `initial` / `no_data` (MySQL) |
| Topics | pre-created with the same partition count and replication factor 1 | same |

Key bytes differ (`1` versus `{"id":1}`); parity is on partitioning by primary key, not on byte equality, and holds for inserts and updates only. For deletes, primary-key partitioning parity is not achievable today without a routing override (a processor setting each event's key) or a product change; the benchmark does not add an override, reports the partition of DeltaForge's deletes as a measured difference, and never relies on the message key for correctness (section 6). The `source` block differs in its fields (Debezium carries more connector metadata); its size shows in the payload bytes per message, reported for both systems.

DeltaForge's `debezium` envelope is **not** the parity choice: its `{"schema": null, "payload": {...}}` wrapper corresponds structurally to schemas-enabled Connect output (`schemas.enable=true`), which carries the full schema rather than `null`. With `schemas.enable=false`, Connect emits the payload object directly, which is the shape of DeltaForge's native envelope. DeltaForge's documentation currently states the opposite (see section 12).

Two batching profiles are run for each system:

- **Defaults:** each system as shipped (DeltaForge: `batch.max_events=2000`, `max_bytes=16 MiB`, `max_ms=50`, `max_inflight=1`, producer `linger.ms=5`; Debezium: connector and Connect defaults).
- **Tuned:** settings documented with the results, chosen once per system from a short sweep on the backlog workload and then fixed for every workload (DeltaForge: `batch.max_events`, `max_bytes`, `max_inflight`, `linger.ms`; Debezium: `max.batch.size`, `max.queue.size`, `poll.interval.ms`, producer `linger.ms` and `batch.size`).

## 4. Workloads

### 4.1 Preparation boundary (W1-W4, W5, W8)

A change committed before a system has set up its capture state cannot be captured by it (on PostgreSQL, nothing committed before the replication slot exists is available). Every backlog and restart workload therefore starts from the same prepared state:

1. **Initialize each system fully** against the empty (or, for W8, fully created) schema, with snapshots off (DeltaForge `snapshot.mode: never`; Debezium `snapshot.mode=no_data`): DeltaForge creates its replication slot (PostgreSQL) or records its binlog position (MySQL) and its schema registry state; Debezium creates its slot, offsets and (MySQL) schema history.
2. **Reach an agreed checkpoint:** the driver commits one marker row; the system has reached the checkpoint when the consumer has received the marker **and** the system's durable position covers it (DeltaForge: every sink checkpoint at or after the marker; Debezium: the committed source offset at or after the marker, after an offset flush).
3. **Stop the CDC process cleanly**, retaining that state: DeltaForge with a graceful shutdown of its process (state store kept); Debezium with a graceful stop of the Connect worker (connector configuration, offsets and schema history topics, and the slot kept). Database retention (WAL, binlog) is configured to keep everything after the marker.
4. **Commit the backlog** (W1-W4) or the single **W8 marker change**.
5. **Start the timed run** by starting the CDC process again against the retained state.

**What is timed.** For both systems the clock starts at **process start** (the container start command of DeltaForge, or of the Connect worker with the connector already registered), so JVM, Connect and connector-task startup count for Debezium and process startup counts for DeltaForge. W1-W4 report the drain throughput over the window from the first to the last backlog record, and separately the time from process start to the first record. As a secondary, informative figure, Debezium's connector-task start (task `RUNNING` in the Connect REST API) to first record is also recorded.

**W8 is a CDC-only restart**, never a first start: the system was initialized and stopped through steps 1-3, so no snapshot, slot or offset creation, or schema history bootstrap happens in the timed run. W5 starts from the same prepared state and is measured after its warm-up. W6 and W7 are first starts by definition (DeltaForge `snapshot.mode: initial`, Debezium `snapshot.mode=initial`, data already present) and are timed from process start.

### 4.2 Workload definitions

All tables have an integer primary key `id`, a `version` integer, a `committed_at` timestamp (set by the driver at commit, used for latency), and a payload of the stated size made of mixed column types. The driver writes with a fixed number of connections and a fixed number of rows per transaction.

| Id | Workload | Source data | Driver | Measured |
|---|---|---|---|---|
| W1 | Backlog drain, one table | 1,000,000 inserts, ~200 B rows | committed after the preparation boundary (4.1) | drain throughput, CPU, cgroup memory |
| W2 | Backlog drain, many tables | 1,000,000 inserts over 100 tables | as W1 | drain throughput, CPU, cgroup memory |
| W3 | Backlog drain, large rows | 200,000 inserts, ~4 KiB rows | as W1 | drain throughput, bytes/s, CPU, cgroup memory |
| W4 | Mixed operations | 1,000,000 operations, 60% insert / 30% update / 10% delete over 100,000 keys | as W1 | drain throughput, per-key order |
| W5 | Steady-state latency | inserts and updates at fixed rates: 1,000, 10,000 and 50,000 rows/s, 10 minutes each after 2 minutes of warm-up | started from the preparation boundary (4.1), then running | e2e lag p50/p99, CPU, cgroup memory |
| W6 | Initial snapshot, large table | 10,000,000 rows in one table | first start, snapshot then stream | snapshot duration, CPU, cgroup memory |
| W7 | Initial snapshot, many tables | 1,000 tables x 10,000 rows | first start, snapshot then stream | snapshot duration, CPU, cgroup memory |
| W8 | CDC-only restart | 1 table and 1,000 tables; one change committed after the preparation boundary (4.1) | restart the process against its retained state | process start to that change at the consumer |

A W5 rate is reported only if the system sustains it: the consumer lag stays bounded (does not grow over the measured window). A rate a system cannot sustain is reported as such, not averaged.

Each workload runs on both paths: PostgreSQL->Kafka and MySQL->Kafka.

## 5. Measured outputs

All counting happens at the destination or outside the CDC process, so neither system's own metrics decide its result.

| Output | How |
|---|---|
| Drain throughput (events/s) | a Kafka consumer reads the benchmark topics; from the first record of the backlog to the last. Also reported: time to first record. |
| End-to-end lag p50/p99 | per record: consumer receive time minus `committed_at` (driver, consumer and database on one host clock); summarized with an HDR histogram. |
| CPU | the CDC container's cgroup `cpu.stat` usage over the measured window (cores used). For Debezium, the Connect worker container. |
| Memory | the CDC container's cgroup `memory.peak` and the mean of `memory.current` samples (1 s). Never in-process RSS. |
| Snapshot duration | start of the process to the last snapshot record consumed. |
| Startup | process start to the first change consumed (W1-W4: first backlog record; W8: the marker change). |
| Payload bytes | mean message key and value size per workload. |

Kafka broker, databases and driver run in separate containers whose resources are reported but not attributed to either system.

## 6. Correctness checks (every run)

A run whose checks fail is invalid and is reported as a failure, not as a number.

1. **Completeness:** every operation the driver committed is delivered: the verifier compares the set of `(table, id, version, op)` written with the set consumed. Logical identity (`table`, `id`) comes from `after`, or from `before` for deletes, never from the message key.
2. **Duplicates:** at-least-once allows duplicates; their count is reported for each system and run.
3. **Order:** for each logical row, consumed `version` values never go backwards within a partition (W4, W5). A DeltaForge delete partitioned away from its row's earlier events is reported as a cross-partition reordering, a measured difference of the keying (section 3), not hidden.
4. **Final state:** after the run, applying the consumed changes per logical row in `version` order gives the same final rows as the source (checksum over `id`, `version` and payload).
5. **Snapshot boundary:** for W6/W7 with concurrent writes, every row is present exactly in its latest version after the stream catches up.

## 7. Environment

- **Host:** one dedicated machine, not a developer workstation in use. Recorded with each run: CPU model and core count, RAM, disk model and filesystem, kernel, Docker and cgroup versions.
- **Limits:** the CDC container of either system gets the same CPU and memory limits (initially 4 CPUs, 4 GiB) and its own CPU set; Kafka, the databases and the driver run on separate CPU sets.
- **Quiet host checklist:** no other workloads or containers; CPU frequency governor `performance`; swap state recorded; fresh database and broker volumes for every run; Docker image cache warm (no pull during a run); clocks of all containers from the host.
- **JVM:** the Connect worker's heap set to the same memory limit minus headroom, recorded with the results.

## 8. Procedure and statistics

1. Start the shared services and wait until healthy; create topics.
2. For each system, workload, path and batching profile: fresh volumes, load the source, run, verify, collect.
3. Run order alternates systems (A, B, B, A, ...) so drift on the host does not favor one.
4. Five measured runs per cell after one discarded warm-up run. Reported: median, minimum and maximum; any run excluded is listed with its reason.
5. A difference between systems is described only when the ranges do not overlap.

## 9. Reproducibility and publication

- **Harness:** a `bench/` directory with the compose file (images by digest), `versions.lock`, the DeltaForge pipeline files and Debezium connector configurations for every cell, the driver and verifier, and one command per cell.
- **Results:** one JSON file per run (system, versions, commit, workload, path, profile, environment manifest, every measured output, correctness check results), committed with the raw histograms. A summary table is generated from these files only.
- **Publication:** numbers appear in the documentation only together with this method, the environment manifest and the raw results. The capacity envelope does not quote them as limits or guarantees.

## 10. Relation to existing tools

| Tool | Use | Not used for |
|---|---|---|
| `runner/throughput_e2e`, `runner/mysql_throughput_e2e` | regression floors on a single-table drain (in-process measurement) | comparisons: their RSS includes the test harness |
| chaos `backlog-drain` | development tuning against the chaos stack | published numbers: it reports from a developer machine and stdout notes |
| Criterion benches (`processors`, `runner/pipeline_e2e`, `schema-sensing`) | component costs | end-to-end throughput |
| `scale-harness` | storage-operation shape at catalog scale | throughput |

## 11. Phasing (after rc.1)

1. Harness, driver and verifier; W1 and W5 on both paths for both systems.
2. W2-W4 and W6-W8.
3. Publication of the first results with this method; reconcile `docs/src/performance.md` against them.

## 12. Found while preparing this design (follow-ups, not part of the design)

- **No before/after-coalescing message key (product and documentation follow-up):** a key template such as `${after.id}` resolves to an empty key on deletes (the Kafka sink's lenient resolver does not fall back to the default key when a template is configured, contrary to its code comment), so primary-key keys consistent across inserts, updates and deletes cannot be configured. Needed: a declarative key expression taking `after`, else `before`, and documentation of the current behavior in `docs/src/sinks/kafka.md` and `docs/src/routing.md`.

- **Incorrect compatibility claim (documentation follow-up):** `README.md` (Debezium compatibility note and the migration tip) and `docs/src/envelopes.md` (Debezium envelope) state that the `debezium` envelope's `{"schema": null, "payload": ...}` output matches `JsonConverter` with `schemas.enable=false`, and advise `envelope: { type: debezium }` for drop-in compatibility with such consumers. With `schemas.enable=false`, Connect emits the payload object without a wrapper, which matches the native envelope's shape; the wrapped form matches schemas-enabled output structurally, which carries a full schema instead of `null`. Both documents need correcting.

- `docs/src/sinks/kafka.md` does not document the sink's `key`, `envelope`, `encoding` or topic templates.
- The chaos documentation refers to compose profiles that do not exist (`soak`, `pg-soak`, `avro-soak`) and gives a stale `--drain-max-events` default.
- `crates/processors/benches/flatten_processor_bench.rs` benchmarks the outbox processor, not flatten.
- `docs/specs/roadmap-competitive.md` quotes competitor throughput and memory figures that no in-repository benchmark supports; they are not to be used until runs following this design exist.
