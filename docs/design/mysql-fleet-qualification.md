# Single-Instance MySQL Fleet Qualification - Design

**Status:** Design for review (revision 2: resource inventory, qualification profiles, active-set matrix, physical state-store qualification, correlated failures, repetition rules, key-independent correctness, scope of synthetic state). No harness or fix is implemented until this design is reviewed.
**Date:** 2026-10-07
**Scope:** qualify one DeltaForge process (one worker, one state store) capturing many large MySQL clusters, for the production fleet below. This design defines the target, what "qualified" means, the known risks in the current code, the harness tiers, the scenarios and their pass criteria, and the inputs still needed from the owner. It does not change the rc.1 candidate. Until a qualification run passes, the capacity envelope keeps stating that work beyond 10,000 tables per source is not a support claim ([Capacity Envelope](../src/capacity-envelope.md#known-scaling-caveats-pending-benchmark)).

## 1. Target

| Quantity | Production | First qualification target |
|---|---|---|
| MySQL clusters | 300 | 25 per DeltaForge instance (12 instances for the fleet) |
| Customer databases per cluster | 1,700-2,000 | same |
| Tables per customer database | 150-200 | same |
| Tables per cluster | 255,000-400,000 | same |
| Tables per DeltaForge instance | - | 6,375,000-10,000,000 |
| Tables in the fleet | 76.5M-120M | - |

**Unit of work.** One pipeline per MySQL cluster: one binlog stream from the cluster's writable primary, a table pattern covering its customer databases, and one source id. Twenty-five such pipelines run in one process against one state store (the single-owner rule and the store gate are unchanged, [Deployment Support](../src/deployment-support.md)). Placement is manual: each instance's configuration names its 25 clusters. A control plane is out of scope.

**Worst case qualified.** Every dimension is qualified at the top of its range (2,000 databases x 200 tables, 400,000 tables per cluster, 10M per instance). The low end is reported but not separately qualified.

## 2. What "qualified" means

An instance configuration is qualified for a **profile** when every scenario of that profile (section 5) passes its criteria at the target scale, on the reference environment, with the correctness checks of section 4.4 clean, under the repetition rules of section 5.3. The numeric budgets marked **(owner)** are inputs to be confirmed before the qualification run (section 8); the values given are proposals.

### 2.1 Profiles

| Profile | Covers | Excludes |
|---|---|---|
| **CDC-qualified** | streaming from an initialized position: steady CDC, the active-set matrix, DDL storms, lifecycle, restarts, failover, correlated failures, the state store | any full initial snapshot of a cluster |
| **Snapshot-qualified** | everything in CDC-qualified, plus the full initial snapshot of a 400,000-table cluster and its lock-window requirement | - |

The profiles are qualified and published separately: a snapshot that cannot meet its lock window at 400,000 tables (R1) does not invalidate a CDC-qualified result, and the published envelope names the profile that passed. Whether production needs the snapshot profile is an owner input (section 8); if it does not, only CDC-qualified is pursued, and tables enter DeltaForge's state through their first change (lazy schema resolution).

### 2.2 Criteria

| Dimension | Criterion |
|---|---|
| Correctness | no lost change, no checkpoint advanced past undelivered data, no wrong-schema decode, and no change of a logical row out of order **within the configured Kafka partition**, in every scenario (section 4.4). Until a before/after-coalescing key exists, ordering of a row's delete relative to its earlier events across partitions is **reported, not guaranteed** |
| Steady CDC | sustains the production change profile with lag p99 under the budget **(owner; proposed 5 s)**, with lag not growing over a 6-hour run |
| Memory | process cgroup peak under the budget **(owner; proposed 16 GiB)** at every point of the active-set matrix, explained by the resource inventory (section 3.4) and memory model (section 3.5) |
| Connections | per cluster: 1 binlog session plus a stated, bounded number of control and snapshot connections; per instance: a stated total |
| Restart and recovery | a CDC-only restart of all 25 pipelines, and recovery from each correlated failure, reaches first change on 50%, 90% and 100% of the pipelines within the budgets **(owner; proposed 2 min for 100%)**, with no per-table work |
| Snapshot (snapshot profile only) | a full initial snapshot of one 400,000-table cluster completes, with any global lock held under the budget **(owner)**; concurrent snapshots are bounded by the process snapshot connection cap |
| DDL storms | a customer migration applied to every customer database of a cluster is processed with bounded lag and memory, and correct decoding before and after |
| Lifecycle | customer onboarding (create database, 200 tables) and offboarding (drop database) are processed correctly with bounded cost |
| Failover | a primary failover of one cluster continues from the proven position without affecting the other 24 pipelines |
| State store | growth per day and per DDL stated; operation latency stable over the run; no contention that stalls other pipelines |
| Metrics | series count independent of table count (already merged: no table labels by default) |

## 3. Known risks in the current code

These are hypotheses from reading the code (cited), to be measured by the harness, not conclusions. Each is assigned the scenario that exposes it. A risk the measurements confirm becomes its own reviewed fix, or a documented limit, before the qualification run.

### 3.1 Snapshot

- **R1 Global lock window.** The MySQL snapshot verifies the whole plan while holding the global read lock: `verify_plan_under_lock` re-runs the paged discovery and re-reads every planned table's shape (`crates/sources/src/mysql/mysql_snapshot.rs:561-628`), inside `timeout(lock_timeout_secs)` with a 10 s default (`mysql_snapshot.rs:700-714`, `snapshot_cfg.rs`). At 400,000 tables that is about 400 discovery pages and 400,000 shape reads under a server-wide read lock. Exposed by S6. Expected outcome: not viable on a production primary as is.
- **R2 Preparation time.** Preparation is per table (registry warm, live schema load, identity resolution, `crates/sources/src/mysql/mod.rs:482-530`): measured 11.0 s at 1,000 tables and 133.6 s at 10,000 ([Initial Snapshots](../src/snapshots.md#scale)). A linear extrapolation gives about 90 minutes at 400,000 tables, before rows are copied. Followed by per-table baselines, each a stable capture plus a binlog scan (`mod.rs:1660-1690`, `mysql_baseline.rs:140-300`). Exposed by S6.
- **R3 Snapshot memory.** MySQL peak RSS growth during a snapshot was 17.9 MiB at 1,000 and 48.4 MiB at 10,000 tables, attributed to schema state ([Initial Snapshots](../src/snapshots.md#scale)). Its growth at 400,000 tables is unmeasured. Exposed by S6.

### 3.2 Streaming and DDL

- **R4 Re-proof after a lineage barrier.** A start that writes a lineage-wide barrier (a snapshot, or a start not continuing from the committed position, `mod.rs:1645-1648, 2394-2425`) makes every tracked table need a new positional proof at its first rows: a lazy baseline with a binlog scan per table (`mysql_baseline.rs:303-385`, `mysql_selection.rs:661`), or the FULL-metadata fallback. With 400,000 tables per cluster, the first change of each table pays this cost. Exposed by S8, and after the snapshot in S6.
- **R5 DDL cost.** Per DDL statement, a durable activation record per attributed table and, for tracked non-drop tables, a forward proof: a stable capture plus one binlog scan (`mysql_event.rs:1189-1312`, `mysql_forward_proof.rs:93-172`). A schema reload follows (`mysql_event.rs:1128-1152`), without an allow check, so DDL on uncaptured tables is loaded too. A migration across 2,000 customer databases is thousands of statements, each with these costs. Exposed by S3.
- **R6 Unbounded per-run state.** `table_map` holds one entry per binlog table id seen and is never pruned within a run (`mysql_event.rs:191`); MySQL reassigns table ids when tables are reopened, so stale ids accumulate. `drift_checked` holds one entry per table checked after a failover (`mysql_failover_drift.rs:689`, cleared only on an identity change). Schema pins (compact, about 100 B) grow with the distinct tables resolved in a lineage generation (`registry_scope.rs:195-197`, documented as unbounded). At 400,000 tables per cluster and 25 clusters these are measured, not assumed small. Exposed by S2 (long run), S3, S7.
- **R7 Durable streams that only grow.** Activation streams are never compacted; the lineage and database barrier streams are read whole on a timeline load (`mysql_activation.rs:610-692`). Their growth per DDL and per discontinuous restart determines timeline load cost over months. Exposed by S3, S5 (long-run projection).

### 3.3 Process-wide resources

- **R8 Connections.** Each source's schema loader uses a `mysql_async` pool with the library default of 10 to 100 connections (no limit is set, `mysql_schema_loader.rs:118`; `mysql_async` `DEFAULT_POOL_CONSTRAINTS`), plus short-lived pools for lineage capture, health checks and binlog scans, and extra replication sessions for proofs. Twenty-five sources could open up to 2,500 control connections. Exposed by S2, S3, S6.
- **R9 State store.** The SQLite backend serializes every operation of all 25 pipelines on one connection mutex (`crates/storage/src/sqlite.rs:3, 79`); the PostgreSQL backend is beta ([Deployment Support](../src/deployment-support.md)) and registered schemas at about 250 per second in the registry measurement (about 27 minutes for 400,000 first registrations). Schema registration also takes a process-global high-water lock (`schema_registry.rs:1162-1171`). Exposed by S1, S3, S4, S6.
- **R10 Shared registry cache.** The process-wide registry cache (default 50,000 entries, 64 MiB) is shared by all sources; each source also keeps its resident schema cache (4,096 tables or 64 MiB) and the selection caches (4,096 entries). Hit rates under a fleet change profile are unmeasured. Exposed by S2.
- **R11 Server settings.** The documentation does not cover MySQL settings that matter at 400,000 tables per server (`table_open_cache`, `table_definition_cache`, `open_files_limit`, `information_schema_stats_expiry`). The harness records them and the qualification states the values used.

### 3.4 Static resource inventory

From the code on main (cited), for the reference pipeline: one MySQL source, one Kafka sink, JSON, default configuration (no journal, replay, rotation, schema sensing or per-table metrics), streaming (no snapshot running). `N` is the number of sources (pipelines), `B` the number of Kafka brokers a producer connects to, `C` the CPU count. Counts are what the code allows; the harness measures what is used.

**Per pipeline.**

| Resource | Per pipeline | Source | Bounded |
|---|---|---|---|
| MySQL binlog session | 1 (2 briefly during a reconnect: the new stream opens before the old one is dropped) | `mysql/mod.rs:1670`, `:2260` | yes |
| Schema-loader pool (run loop) | idle connections kept for the pool's life (about 1 in practice: the run loop is sequential); `mysql_async` defaults min 10 / max 100, no idle TTL, DeltaForge sets no limits | `mysql_schema_loader.rs:118` | by library default only (100) |
| Schema-loader pool (runner side) | a second, independent pool; connects only when a resolver uses it (Avro, S3/Parquet, ClickHouse, Elasticsearch sinks, schema API); 0 for a JSON Kafka sink | `pipeline_manager.rs:1015` | by library default only (100) |
| Transient MySQL connections | at most 1 control connection or 1 scan replication session, plus 1 capture connection, at a time (startup, reconnect, proofs, health helpers) | `mysql_session.rs:223`, `mysql_binlog_scan.rs:356, 591` | yes |
| Snapshot connections (snapshot profile) | 1 lock + up to `max_parallel_tables` (8) workers + 1 guard per tick + up to 8 pooled loader connections; permits cover lock, guard and workers, **not** the pooled ones | `mysql_snapshot.rs:657, 681, 288`, `snapshot_permits.rs` | yes, partly outside the permits |
| Long-lived tokio tasks | 5: source run, source monitor, coordinator, delivery, pool recycler (+1 recycler per further pool in use) | `mysql/mod.rs:1977`, `pipeline_manager.rs:1117, 1449`, `coordinator.rs:1518` | yes |
| Timers | coordinator tick every `batch.max_ms` (default 50 ms, so 20 wake-ups/s even when idle); binlog heartbeat 15 s; inactivity watchdog 60 s; Kafka statistics 5 s | `coordinator.rs:1552`, `mysql/mod.rs:170`, `kafka.rs:63` | yes |
| Source to coordinator channel | 32,768 items, no byte bound | `pipeline_manager.rs:1078` | items only |
| Batches | 1 building + `max_inflight` (default 1) queued + 1 in delivery; each up to 2,000 events / 16 MiB, a whole source transaction up to 512 MiB when `respect_source_tx` | `deltaforge-config/src/lib.rs:652-664`, `coordinator.rs:1499` | yes (bytes per batch) |
| Kafka producer | 1 per sink; librdkafka threads: main, internal, 1 per broker, plus rdkafka's polling thread (about `3 + B`); queue default 100,000 messages / 1 GiB (not set by DeltaForge), effectively one in-flight batch since each batch is awaited | `kafka.rs:249`, librdkafka defaults | yes (large default ceiling) |
| Resident schema cache | 4,096 entries or 64 MiB **per loader**; 1 loader in use for a JSON Kafka sink, up to 3 (runner, run loop, snapshot) | `registry_scope.rs:199-209` | yes, per loader |
| Selection cache | 4,096 entries (timelines and selections) | `mysql_selection.rs:74` | yes |
| Schema pins | about 100 B per table resolved in the lineage generation | `registry_scope.rs:195-197` | **no** |
| `table_map` | one `TableMapEvent` per binlog table id seen in the run (ids change when MySQL reopens tables); the binlog parser keeps its own map per stream | `mysql_event.rs:191`, vendored `binlog_parser.rs:22` | **no** |
| `drift_checked` | one entry per table checked after a failover | `mysql_failover_drift.rs:689` | by tracked tables |
| File descriptors | MySQL sockets as above, about `B` Kafka sockets | - | yes |
| State-store writes | 1 checkpoint write per sink per committed batch (at most about 20/s per busy pipeline at the default `max_ms`) | `coordinator.rs:2766-2776` | yes |

**Process-wide.**

| Resource | Count | Source |
|---|---|---|
| Registry cache | 50,000 entries / 64 MiB, shared by all sources, plus per-source accounting | `schema_registry.rs:273-281` |
| State-store connections | SQLite: 1 connection behind one mutex (about 3 descriptors); PostgreSQL: deadpool default `2 x C`, lazily opened | `sqlite.rs:79`, `postgres.rs:143`, deadpool `util.rs` |
| Tasks | metrics listener, API server, signal handler, store TTL sweep (60 s); admin listener and Vault renewal when configured | `main.rs`, `sqlite.rs:183`, `postgres.rs:161` |
| Tokio runtime | `C` workers, blocking pool up to 512 threads | `main.rs:213` |
| Listeners | API, metrics, optional admin (2-3 sockets) | `main.rs` |
| Locks and semaphores | manager lifecycle mutex (serializes every start, stop, patch, resume and delete in the process); registry high-water lock (every schema registration); registry cache mutex; snapshot connection semaphore (64 by default, `--max-snapshot-connections`); table-metrics registry lock; lag collector mutex | `pipeline_manager.rs:806`, `schema_registry.rs:411-414, 1166`, `snapshot_permits.rs:18, 76`, `table_metrics.rs`, `table_lag.rs` |

**Formulas and values** (reference pipeline, `B = 3`; "ceiling" is what the code permits, "expected" is the steady use above). These are **planning estimates** read from the code and library defaults, to be verified by the harness; in particular the MySQL connection counts and the Kafka thread counts (librdkafka's thread model) are not runtime guarantees:

| Resource | Formula | N = 1 | 25 | 50 | 100 | 300 |
|---|---|---|---|---|---|---|
| MySQL connections, expected steady (estimate) | `2N` | 2 | 50 | 100 | 200 | 600 |
| MySQL connections, transient peak, all sources reconnecting (estimate) | `5N` | 5 | 125 | 250 | 500 | 1,500 |
| MySQL connections, ceiling: two loader pools at the library maximum, plus binlog (estimate) | `201N` | 201 | 5,025 | 10,050 | 20,100 | 60,300 |
| Long-lived tokio tasks | `5N + 4` | 9 | 129 | 254 | 504 | 1,504 |
| Coordinator wake-ups per second when idle | `20N` | 20 | 500 | 1,000 | 2,000 | 6,000 |
| Kafka producer OS threads (estimate) | `N(3 + B)` | 6 | 150 | 300 | 600 | 1,800 |
| Queued source items (channel capacity) | `32,768N` | 33K | 819K | 1.6M | 3.3M | 9.8M |
| Batch bytes, nominal (3 batches of 16 MiB) | `48 MiB x N` | 48 MiB | 1.2 GiB | 2.3 GiB | 4.7 GiB | 14 GiB |
| Resident schema cache ceiling (1 loader) | `64 MiB x N + 64 MiB` | 128 MiB | 1.6 GiB | 3.2 GiB | 6.3 GiB | 18.8 GiB |
| Schema pins if every table of a 400K-table cluster is resolved | `40 MB x N` | 40 MB | 1 GB | 2 GB | 4 GB | 12 GB |
| `table_map` entries | per table id seen | measured (section 4.1) | | | | |
| Kafka sockets | `N x B` | 3 | 75 | 150 | 300 | 900 |
| File descriptors, steady (MySQL + Kafka + process) | `N(2 + B) + 2C + 3` | 5 + 2C + 3 | 125 + ... | 250 + ... | 500 + ... | 1,500 + ... |
| Checkpoint writes per second, all pipelines busy | `20N` | 20 | 500 | 1,000 | 2,000 | 6,000 |

The queued-items row is bounded in count, not bytes: at 1 KiB per event it is 32 MiB per pipeline (9.6 GiB at 300). The schema cache and pin rows are ceilings and worst cases; the active-set matrix (section 5.2) measures the actual values. 300 sources per process is outside the first target and is listed to show which terms would dominate.

**Findings from the inventory** (each to be fixed in its own reviewed PR, or accepted and documented, before T3):

- **F1 DLQ cleanup task leak:** with the journal enabled, each pipeline start spawns a DLQ cleanup loop whose handle is discarded and which never exits (`pipeline_manager.rs:1257`, `dlq.rs:403`): one leaked task, polling the store every 60 s, per start or restart. Exposed by S16 with the journal enabled.
- **F2 Loader pools without limits:** schema-loader pools use the `mysql_async` defaults (up to 100 connections, idle connections never reaped), and a pipeline can have 2-3 independent loaders, each with its own 64 MiB resident cache budget (R8, R10).
- **F3 Snapshot permits do not count pooled connections** used by snapshot workers, so the process cap of 64 does not bound all snapshot connections.
- **F4 Loose byte bounds:** the source channel bounds items only, and the Kafka producer keeps librdkafka's 1 GiB queue default.
- **F5 Idle wake-ups:** each pipeline's coordinator ticks every 50 ms when idle; at 25 pipelines this is 500 wake-ups per second.
- **F6 Documentation error:** the capacity envelope gives the PostgreSQL store pool as about `CPU x 4`; deadpool's default is `CPU x 2`.

### 3.5 Memory model

The qualification fits and reports, per instance:

`M_process = M_base + sum over pipelines (M_pipeline + M_schema_cache + M_selection + M_pins(tables resolved) + M_table_map(table ids seen) + M_channel + M_batches)`

with each term measured at the tiers and checked against the full-scale run. Each term corresponds to a row of the inventory (section 3.4): `M_schema_cache` counts every loader in use (one for a JSON Kafka sink, up to three), and `M_pins` and `M_table_map` are fitted from the active-set matrix and the in-process tests, never from synthetic registry state (section 4.1). A term that grows with tables, not with assigned work or change volume, is reported with its growth rate; whether it is acceptable at the target is a review decision.

## 4. Harness

No harness exists for many databases per server, many pipelines per process, or DDL rates (the existing tools are the registry harness with synthetic registry tables, the CDC-restart test and the single-database snapshot measurement). The harness is built in tiers so problems surface on cheap tiers first.

### 4.1 Tiers

| Tier | MySQL | Tables | DeltaForge | Proves |
|---|---|---|---|---|
| T1 one cluster, full scale | 1 server | 2,000 databases x 200 tables (400,000) | 1 pipeline | per-cluster behavior at full table count: snapshot (R1-R3), DDL storms (R5), re-proof (R4), per-run state (R6), server settings (R11) |
| T2 one instance, reduced tables | 25 servers | 2,000 databases x 10 tables each (20,000 per server, 500,000 total); registry state for the remaining tables generated synthetically through the registration path | 25 pipelines | process-wide behavior: connections (R8), state store contention (R9), shared caches (R10), memory model (3.5), restart of all pipelines, failover isolation |
| T3 one instance, full scale | 25 servers (on several hosts) | 25 x 400,000 (10M) | 25 pipelines | the qualification run itself |

T1 and T2 run before T3; T3 runs only after the risks they confirm are fixed or accepted.

**What T2's synthetic state proves, and what it does not.** The synthetic registry records of T2 (written through the registration path for tables that do not exist on the T2 servers) prove the **shared state store and index behavior** at full record count: store size, query plans and latency, contention between pipelines. They do **not** prove the in-process cost of per-table runtime state, because the 25 sources never load those records: schema pins, `table_map` entries, `drift_checked` entries, selection and schema cache contents exist only for tables a source actually sees changes on. Those costs are assigned to:

- the T1 and T3 runs of the active-set matrix (section 5.2), where real tables change, and
- dedicated in-process tests that drive one source's per-table structures to a stated size (for example 400,000 distinct table ids and pins) and measure bytes per entry and lookup cost, so the memory model's per-table terms are measured, not inferred.

### 4.2 Fixture

- **Schema generator:** a fixed set of 200 table definitions per customer database (mixed column types, primary keys, secondary indexes, some JSON and DECIMAL columns), parameterized by database count and tables per database, with the customer database naming of production **(owner)**.
- **Build once, reuse:** a fixture is built once per tier and kept as a data volume image, since creating 400,000 tables takes hours. Building time is recorded.
- **Storage layout:** production layout **(owner)**; if file-per-table at 400,000 tables per server exceeds the reference disks, a shared tablespace is used and recorded as a deviation.
- **Server settings:** recorded per run (R11), including `binlog_row_metadata`, `binlog_row_image`, binlog retention and GTID settings.

### 4.3 Workload driver

- **Change profile:** customers and tables chosen by a skewed (Zipf) distribution, with a configured fraction of tables changed per minute and a mix of inserts, updates and deletes, at a configured rate per cluster. Defaults are placeholders until the production profile is supplied **(owner)**: 2,000 changes/s average and 10,000 peak per cluster, 1% of tables changed per minute.
- **DDL events:** a customer migration (the same `ALTER TABLE`s applied to every customer database of a cluster, sequentially, at a configured pace); occasional single-customer DDL.
- **Lifecycle events:** onboarding (`CREATE DATABASE` and 200 `CREATE TABLE`s, then rows) and offboarding (`DROP DATABASE`).
- **Failover:** each T2/T3 cluster that runs S7 has a GTID replica that is promoted.
- **Markers:** every changed row carries a `version` and a `committed_at` column, as in the [benchmark design](benchmarks.md).

### 4.4 Correctness verification

A verifying consumer reads the sink (Kafka, as production **(owner)**) and checks, per cluster:

1. **Completeness:** every committed change is delivered.
2. **Duplicates:** counted and reported (at-least-once).
3. **Order per logical row within a partition:** `version` never goes backwards for a row among the events Kafka orders together (one partition). This is the only ordering the qualification guarantees while key templates cannot key deletes; order across partitions is measured and reported.
4. **Final state:** applying the consumed changes per logical row in `version` order gives the source's final rows.
5. **Schema across DDL:** rows written right before and after each migration statement carry values only the right schema decodes correctly.

**Identity never comes from the message key.** The verifier derives a change's logical row (`database`, `table`, primary key) from `after`, or from `before` for deletes. DeltaForge's key templates resolve only for events with an after image (a delete's `${after.<pk>}` is an empty key, and no before/after-coalescing key expression exists), so with a primary-key template a row's delete can land on another partition than its earlier events. The verifier reports such cross-partition deletes, and the order check applies within partitions.

**If Kafka partitioning by primary key for every operation is an acceptance requirement** **(owner)**, a product prerequisite comes first: a declarative key expression that takes the primary key from `after`, else `before`, reviewed and merged before the qualification runs. Without it, the qualification states that per-row order across operations holds only as far as the key configuration provides it.

A scenario fails if any check fails.

### 4.5 Measurements

Process cgroup CPU and memory (1 s samples, peak and mean), per-pipeline lag and throughput from the metrics endpoint and the consumer, connection counts per cluster from each server (`performance_schema` / processlist by user), state store size and operation latency, time to first change per pipeline on restart, snapshot phase durations and lock hold time, and the memory model terms (heap profiles at the end of each tier run). All results are written as JSON per run with the environment manifest and versions, as in the benchmark design.

### 4.6 Physical state-store qualification

The runs above write the state store only for tables that change or are snapshotted, so they do not guarantee a store holding every table's records at the target scale. The store is therefore qualified separately, on the production backend (PostgreSQL **(owner)**; SQLite is measured once for comparison and is expected to be unsuitable, R9).

**Matrix.**

| Dimension | Values |
|---|---|
| Tables with registry records | 400,000 (one cluster) and 10,000,000 (one instance) |
| Schema versions per changed table | 1, 3, 10, 50 (the changed fraction is a parameter: 1%, 10%, 100%) |
| Activation history per table | baseline only; plus one DDL record per version; plus FULL observations |
| Barrier streams | lineage and database barriers per cluster: 0, 100, 10,000 |
| Sources sharing the store | 1 and 25 |

**Measured for every cell.**

- Table and index sizes (per relation, and total).
- Latency p50, p95 and p99 of the hot point reads (latest version, version by number, activation timeline head, checkpoint read) and of paged history reads (version history, activation stream pages, barrier streams), cold and warm.
- `EXPLAIN (ANALYZE, BUFFERS)` of each hot query, kept with the results, confirming index use and stable buffer counts as the store grows.
- Autovacuum and analyze behavior under the write rates of S1 and S3: dead tuples, bloat, vacuum duration, and whether statistics stay current.
- Backup (base backup) and restore duration and size.
- DeltaForge restart and reconnect against the fully populated store: S5 timings, and recovery after a store restart (S10).

**Evidence classes.** Two kinds of fixture are used, and every result names its class:

- **Production-path generation:** records written by DeltaForge's own code paths (registration, activation appends, checkpoints), as far as time allows (to 400,000 tables and their versions). This is evidence for write-path throughput and for the records' exact content.
- **Byte-equivalent bulk fixtures:** for 10M tables and up to 50 versions, rows bulk-loaded into the same schema. Their bytes are produced by DeltaForge's own serializers and checked against a production-path sample (identical row bytes for identical inputs), so they are equivalent for storage, index and query behavior. They are **not** evidence for write-path throughput or for how records accumulate over time.

## 5. Scenarios

### 5.1 Scenario list

Profile: **C** counts toward CDC-qualified (and therefore snapshot-qualified), **S** toward snapshot-qualified only. "Recovery" means first change delivered after the disruption; it is reported as the time until 50%, 90% and 100% of the affected pipelines recover, with the slowest pipeline named.

| Id | Scenario | Profile | Tier | Pass criteria |
|---|---|---|---|---|
| S1 | Steady CDC, all pipelines, production change profile, 6 hours | C | T2, T3 | lag p99 within budget and not growing; memory within budget and flat after warm-up; correctness |
| S2 | Long-run state growth: 24 hours of S1 with the profile's table churn | C | T1, T2 | per-run state (R6) and cache behavior (R10) measured; growth rate per term reported |
| S3 | Customer migration storm: one migration across all 2,000 databases of a cluster during S1 | C | T1, T2 | correct decoding before and after; lag returns within the budget; DDL cost per statement and durable growth (R5, R7) reported; other pipelines' lag unaffected beyond the budget |
| S4 | Onboarding and offboarding: 20 customers created and 20 dropped per cluster during S1 | C | T1, T2 | rows of new customers delivered with the right schemas; dropped customers stop cleanly; costs reported |
| S5 | CDC-only restart of all 25 pipelines after S1 | C | T2, T3 | recovery 50/90/100% within budget; no catalog enumeration, no schema preload, no per-table proofs |
| S6 | Initial snapshot of one full cluster (and two concurrently) | S | T1, T3 | completes; lock hold time within budget; preparation, copy and baseline durations, memory and connections reported (R1-R3) |
| S7 | Primary failover of one cluster during S1 | C | T2, T3 | that pipeline continues from the proven position; others unaffected; post-failover drift checks bounded (R6) |
| S8 | First change after a lineage barrier, across many tables | C | T1 | cost of re-proof per table measured (R4); behavior with `binlog_row_metadata=FULL` and without |
| S9 | Sink outage of 10 minutes during S1 | C | T2, T3 | backpressure without unbounded memory; recovery 50/90/100% within budget; no loss |
| S10 | State store restart (PostgreSQL backend) during S1 | C | T2, T3 | pipelines fail closed or continue per the documented behavior; no inconsistency; recovery 50/90/100% |
| S11 | All 25 MySQL endpoints disconnected together for 5 minutes, then restored together | C | T2, T3 | recovery 50/90/100% within budget; no reconnect storm beyond the connection inventory (section 3.4); memory within budget |
| S12 | Credential rotation across all 25 sources within one minute | C | T2, T3 | every source reconnects with its new credential; recovery 50/90/100%; no event lost or duplicated beyond at-least-once |
| S13 | One hot cluster (10x the profile rate) while 24 are quiet | C | T2, T3 | the hot pipeline's lag within budget or its saturation point reported; quiet pipelines' lag and memory unaffected |
| S14 | Several hot clusters (5 at 5x) competing for the shared caches and the state store | C | T2, T3 | lag per pipeline within budget; shared-cache hit rates, state-store latency and contention reported |
| S15 | Clean shutdown of the process under peak load, then restart | C | T2, T3 | shutdown within its budget **(owner)**, no lost acknowledged change, checkpoints consistent; restart recovery 50/90/100% |
| S16 | Repeated lifecycle operations: 100 cycles of stop, start and patch across pipelines during S1 | C | T2 | operations complete without stalling other pipelines; the process-wide lifecycle serialization's queueing delay measured; no leaked tasks, connections or memory across cycles |
| S17 | Metrics scraping every 5 s during peak recovery (S11 and S15) | C | T2, T3 | scrape latency and size bounded; scraping does not delay recovery beyond its budget |

### 5.2 Active-set matrix

Per-table runtime state (schema pins, `table_map`, the schema and selection caches, the registry cache, selection rebuilds) depends on how many distinct tables change, not on the change rate. At 25 clusters (10M tables), S1 runs at each active fraction, with the per-cluster equivalent on T1:

| Id | Active tables (per instance) | Per cluster (T1) | Pattern |
|---|---|---|---|
| A1 | 0.01% (1,000) | 40 | fixed set |
| A2 | 0.1% (10,000) | 400 | fixed set |
| A3 | 1% (100,000) | 4,000 | fixed set |
| A4 | 10% (1,000,000) | 40,000 | fixed set |
| A5 | 1% at a time, moving to a disjoint set every 30 minutes for 12 hours (12% of tables touched overall) | 4,000 at a time | moving |

Measured at every point: pins, `table_map` entries and `drift_checked` entries per source (counts and bytes); hit, miss and eviction rates of the schema cache, the selection cache and the registry cache; selection and schema rebuild counts and their cost; registry reads per second; lag; process memory. A5 shows whether state from earlier active sets is retained (growth with tables ever touched) or released (bounded by the current set). The memory model's per-table terms are fitted from A1-A4 and checked on A5.

### 5.3 Repetition and comparison rules

- **Short scenarios** (minutes: S5, S6, S8, S11, S12, S15, S17 and each store measurement): one discarded warm-up run, then at least 5 measured runs on fresh state; reported as median, minimum and maximum. A criterion passes only if the **worst** measured run meets it.
- **Long soaks** (hours: S1, S2, the active-set runs): at least 2 runs, and each run contains repeated disruptions at fixed intervals (a pipeline restart every 2 hours, a sink outage and an endpoint disconnect once per run), so recovery is measured several times under accumulated state. Every disruption's recovery must meet its criterion.
- **Comparison between configurations or versions** (for example before and after a fix): the same fixture, environment and run count, interleaved runs; a difference is claimed only when the ranges do not overlap.
- **No single favorable run:** a run excluded from the results is listed with its reason; a failing run is never replaced by a rerun without recording both.

## 6. Environment and reproducibility

- **Reference environment** **(owner)**: the instance shape intended for production (CPU, memory, disk, network), with the 25 MySQL servers on separate hosts sized like production replicas, and a Kafka cluster shaped like production.
- **Pinned versions** of DeltaForge (commit and image), MySQL, Kafka and the harness; fixtures referenced by build recipe and checksum.
- **Results** as JSON per run, with raw time series, committed alongside a summary generated from them.
- **Publication:** the capacity envelope is updated only from a passing T3 run, with the configuration and environment it qualified.

## 7. Sequencing

1. Review of this design; owner inputs (section 8), including the profile to pursue and whether primary-key partitioning is required.
2. If primary-key partitioning for every operation is required: the before/after-coalescing key expression, as its own reviewed product PR (section 4.4).
3. Harness: fixture generator, driver, verifier, measurement collection, store-matrix tooling and the in-process per-table state tests (one reviewed PR, no product changes).
4. In-process per-table state tests and the physical state-store matrix (section 4.6); report the per-table terms and the store's behavior.
5. T1 runs (S2, S3, S4, S8, the active-set matrix per cluster, and S6 for the snapshot profile); report which risks are confirmed.
6. One reviewed fix PR per confirmed risk that blocks the target, or a documented limit approved in review.
7. T2 runs (S1-S5, S7, S9-S17, the active-set matrix); memory model (section 3.5) fitted against the resource inventory (section 3.4).
8. T3 qualification run per profile; capacity envelope and deployment support updated from its results, naming the profile that passed.

## 8. Inputs needed from the owner

1. **Profile:** whether production needs the snapshot-qualified profile, or CDC-qualified only (tables enter through their first change, any backfill handled outside DeltaForge).
2. **Change profile** per cluster: average and peak changes per second, the fraction of tables active per hour and how the active set moves, operation mix, row sizes.
3. **DDL practice:** how customer migrations roll out (all databases at once or staged, statements per migration, frequency), and onboarding/offboarding rates.
4. **Snapshot needs** (snapshot profile only): whether a global read lock on a primary is acceptable or snapshots run from a replica, and the acceptable lock window.
5. **MySQL configuration** in production: `binlog_row_metadata`, `binlog_row_image`, binlog retention, storage layout, and the table cache settings.
6. **Budgets:** instance memory and CPU, lag p99, recovery times (50/90/100%), shutdown time, connections per cluster.
7. **Sink and keys:** Kafka as the sink, and whether consumers require primary-key partitioning for every operation including deletes (which needs the product prerequisite of section 4.4).
8. **State store:** PostgreSQL (SQLite is expected to be unsuitable at this scale, R9), its instance shape, and backup and restore expectations.
9. **Failover topology** per cluster (replicas, promotion tooling) and the credential rotation practice.
