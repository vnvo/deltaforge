# Single-Instance MySQL Fleet Qualification - Design

**Status:** Design for review. No harness or fix is implemented until this design is reviewed.
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

An instance configuration is qualified when every scenario in section 5 passes its criteria at the target scale, on the reference environment, with the correctness checks of section 4.4 clean. The numeric budgets marked **(owner)** are inputs to be confirmed before the qualification run (section 8); the values given are proposals.

| Dimension | Criterion |
|---|---|
| Correctness | no lost or reordered change per key, no checkpoint advanced past undelivered data, no wrong-schema decode, in every scenario |
| Steady CDC | sustains the production change profile with lag p99 under the budget **(owner; proposed 5 s)**, with lag not growing over a 6-hour run |
| Memory | process cgroup peak under the budget **(owner; proposed 16 GiB)**, explained by a per-pipeline formula fitted from the tiers (section 3.3) |
| Connections | per cluster: 1 binlog session plus a stated, bounded number of control and snapshot connections; per instance: a stated total |
| Restart | a CDC-only restart of all 25 pipelines reaches first change on every pipeline within the budget **(owner; proposed 2 min)**, with no per-table work |
| Snapshot | a full initial snapshot of one 400,000-table cluster completes, with any global lock held under the budget **(owner)**; concurrent snapshots are bounded by the process snapshot connection cap |
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

### 3.4 Memory model

The qualification fits and reports, per instance:

`M_process = M_base + sum over pipelines (M_pipeline + M_schema_cache + M_selection + M_pins(tables resolved) + M_table_map(table ids seen) + M_channel + M_batches)`

with each term measured at the tiers and checked against the full-scale run. A term that grows with tables, not with assigned work or change volume, is reported with its growth rate; whether it is acceptable at the target is a review decision.

## 4. Harness

No harness exists for many databases per server, many pipelines per process, or DDL rates (the existing tools are the registry harness with synthetic registry tables, the CDC-restart test and the single-database snapshot measurement). The harness is built in tiers so problems surface on cheap tiers first.

### 4.1 Tiers

| Tier | MySQL | Tables | DeltaForge | Proves |
|---|---|---|---|---|
| T1 one cluster, full scale | 1 server | 2,000 databases x 200 tables (400,000) | 1 pipeline | per-cluster behavior at full table count: snapshot (R1-R3), DDL storms (R5), re-proof (R4), per-run state (R6), server settings (R11) |
| T2 one instance, reduced tables | 25 servers | 2,000 databases x 10 tables each (20,000 per server, 500,000 total); registry state for the remaining tables generated synthetically through the registration path | 25 pipelines | process-wide behavior: connections (R8), state store contention (R9), shared caches (R10), memory model (3.4), restart of all pipelines, failover isolation |
| T3 one instance, full scale | 25 servers (on several hosts) | 25 x 400,000 (10M) | 25 pipelines | the qualification run itself |

T1 and T2 run before T3; T3 runs only after the risks they confirm are fixed or accepted.

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

A verifying consumer reads the sink (Kafka, as production **(owner)**) and checks, per cluster: completeness of committed changes, duplicates (reported), per-key order, final state against the source, and decoding with the right schema across DDL (rows written right before and after each migration statement carry values only the right schema decodes correctly). A scenario fails if any check fails.

### 4.5 Measurements

Process cgroup CPU and memory (1 s samples, peak and mean), per-pipeline lag and throughput from the metrics endpoint and the consumer, connection counts per cluster from each server (`performance_schema` / processlist by user), state store size and operation latency, time to first change per pipeline on restart, snapshot phase durations and lock hold time, and the memory model terms (heap profiles at the end of each tier run). All results are written as JSON per run with the environment manifest and versions, as in the benchmark design.

## 5. Scenarios

| Id | Scenario | Tier | Pass criteria |
|---|---|---|---|
| S1 | Steady CDC, all pipelines, production change profile, 6 hours | T2, T3 | lag p99 within budget and not growing; memory within budget and flat after warm-up; correctness |
| S2 | Long-run state growth: 24 hours of S1 with table churn of the profile | T1, T2 | per-run state (R6) and cache behavior (R10) measured; growth rate per term reported |
| S3 | Customer migration storm: one migration across all 2,000 databases of a cluster during S1 | T1, T2 | correct decoding before and after; lag returns within the budget; DDL cost per statement and durable growth (R5, R7) reported; other pipelines' lag unaffected beyond the budget |
| S4 | Onboarding and offboarding: 20 customers created and 20 dropped per cluster during S1 | T1, T2 | rows of new customers delivered with the right schemas; dropped customers stop cleanly; costs reported |
| S5 | CDC-only restart of all 25 pipelines after S1 | T2, T3 | first change on every pipeline within the budget; no catalog enumeration, no schema preload, no per-table proofs |
| S6 | Initial snapshot of one full cluster (and two concurrently) | T1, T3 | completes; lock hold time within budget; preparation, copy and baseline durations, memory and connections reported (R1-R3) |
| S7 | Primary failover of one cluster during S1 | T2, T3 | that pipeline continues from the proven position; others unaffected; post-failover drift checks bounded (R6) |
| S8 | First change after a lineage barrier, across many tables | T1 | cost of re-proof per table measured (R4); behavior with `binlog_row_metadata=FULL` and without |
| S9 | Sink outage of 10 minutes during S1 | T2 | backpressure without unbounded memory; recovery without loss |
| S10 | State store restart (PostgreSQL backend) during S1 | T2 | pipelines fail closed or continue per the documented behavior; no inconsistency |

## 6. Environment and reproducibility

- **Reference environment** **(owner)**: the instance shape intended for production (CPU, memory, disk, network), with the 25 MySQL servers on separate hosts sized like production replicas, and a Kafka cluster shaped like production.
- **Pinned versions** of DeltaForge (commit and image), MySQL, Kafka and the harness; fixtures referenced by build recipe and checksum.
- **Results** as JSON per run, with raw time series, committed alongside a summary generated from them.
- **Publication:** the capacity envelope is updated only from a passing T3 run, with the configuration and environment it qualified.

## 7. Sequencing

1. Review of this design; owner inputs (section 8).
2. Harness: fixture generator, driver, verifier, measurement collection (one reviewed PR, no product changes).
3. T1 runs (S2, S3, S4, S6, S8); report which risks are confirmed.
4. One reviewed fix PR per confirmed risk that blocks the target, or a documented limit approved in review.
5. T2 runs (S1-S5, S7, S9, S10); memory model fitted.
6. T3 qualification run; capacity envelope and deployment support updated from its results.

## 8. Inputs needed from the owner

1. Production change profile per cluster: average and peak changes per second, fraction of tables active per hour, operation mix, row sizes.
2. DDL practice: how customer migrations roll out (all databases at once or staged, statements per migration, frequency), and onboarding/offboarding rates.
3. Snapshot needs: whether initial snapshots of full clusters are required in production, and whether a global read lock on a primary is acceptable (or snapshots run from a replica), with the acceptable lock window.
4. MySQL configuration in production: `binlog_row_metadata`, `binlog_row_image`, binlog retention, storage layout, and the relevant table cache settings.
5. Budgets: instance memory and CPU, lag p99, restart time, connections per cluster.
6. Sink and state store: Kafka as the sink; PostgreSQL as the state store (SQLite is expected to be unsuitable at this scale, R9).
7. Failover topology per cluster (replicas, promotion tooling).
