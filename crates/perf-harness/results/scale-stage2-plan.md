# Scaling study, stage 2: plan

Stage 2 of `scale-study-design.md`: 10,000 and 40,000 tables at 1%, 20% and
50% active. Fixed before any result. Stages 3 to 5 (100% active, rotation,
capacity sweeps) are not part of it.

It also measures how the number of activation windows grows with the active
set, which the 2026-10-30 ruling on the mass-invalidation diagnosis asked
for: bounded work per window still produces more windows as fleets grow.

- **Runner:** `b0adf611` (main after #190), release build.
- **Harness:** `crates/perf-harness` at the merge of the harness changes
  listed at the end, which are reviewed before the first measured run.
- **Decisions taken in review (2026-10-31):**
  - the main matrix uses a SQLite state store, as stage 1 did;
  - a PostgreSQL state store is its own arm (D), DeltaForge only;
  - the historical 2x burst gap is carried as open, with no extra control
    (below).

## Arms

Every point runs from a fresh binary-log file (offset 0) unless stated.
Points are named as the harness names them (`t<tables>-a<percent>-<load>-o<MiB>`).

| Arm | Tables | Active | Load | Binary log | Systems | Store | Points | Reps | Runs |
|---|---|---|---|---|---|---|---|---|---|
| C. Anchor | 1,000 | 1, 20, 50% | steady, burst | offset 0 | DeltaForge, Debezium | SQLite | 6 | 3 | 36 |
| A. Main matrix | 10,000 and 40,000 | 1, 20, 50% | steady, burst | offset 0 | DeltaForge, Debezium | SQLite | 12 | 3 | 72 |
| B. Position | 1,000, 10,000 and 40,000 | 20% | steady | 256 MiB, small padding | DeltaForge, Debezium | SQLite | 3 | 3 | 18 |
| D. PostgreSQL store | 1,000, 10,000 and 40,000 | 1, 20, 50% | burst | offset 0 | DeltaForge | PostgreSQL | 9 | 3 | 27 |

153 runs before expansions.

- **Matching modes.** Every comparison of growth across table counts uses
  the same active fraction, load and binary-log position at each size:
  - arms C and A together give 1, 20 and 50% at 1,000, 10,000 and 40,000
    tables, steady and burst, on the same revision and harness. The active
    tables then run from 10 (1% of 1,000) to 20,000 (50% of 40,000);
  - arm B has its own 1,000-table point at 256 MiB;
  - arm D has its own 1,000-table points.
- **Arm C includes Debezium,** so both systems' curves come from one harness
  and one drain rule. Stage 1's medians are not mixed into any curve: stage 1
  ran on runner `f0171b4e` with the earlier drain rule.
- **Order:** C, A, B, D, as separate invocations. The fixture is rebuilt at
  the start of each invocation (arm B and arm D rebuild the 1,000-, 10,000-
  and 40,000-table fixtures; the build time is recorded).

## Activation counts and the projection

**Counted per run:**
- **selected:** the fixture's tables, all of which the source's pattern
  (`scale_*.*`) selects;
- **touched:** tables with at least one committed row in the run;
- **activated:** touched tables whose first row DeltaForge handed on after
  its window committed (the proof trace's `release` records), cross-checked
  against touched tables whose first row arrived in Kafka;
- **windows:** their number, tables per window (p50, p99, maximum), atomic
  plans per window (applies plus conflicts), cuts, and commit time;
- **time to the last activation:** from the start of the load to the arrival
  of the last activated table's first row.

**The projection is checked against the actual workload.** The
mass-invalidation design projected about 44 plans of up to 910 tables, about
40 s of schema reads and about 900 store statements for activating all
40,000 tables. Stage 2 activates at most 20,000 (50% of 40,000). The results
compare plans and schema work with the activated count of each run, scaled
by the same per-table figures (910 narrow tables per plan, the measured
schema-read cost per table), and never present a run as a full-catalog cold
start.

## Correctness

Every run is judged on:
- zero missing rows, zero duplicates and zero reordered rows;
- no row from outside the run's id range and no row of an unknown table;
- the drain completed (below).

A run that fails any of them is a failed run. It is kept, reported and
investigated, never treated as an outlier.

## Measured failures

These fail the run, which is kept in the results with its cause. They are
investigated before the expansion list is frozen:
- control-plane permit timeouts (`deltaforge_mysql_control_permit_timeouts_total`);
- saturation deferrals (`deltaforge_mysql_control_saturation_deferrals_total`);
- activation-window deferrals and overflows
  (`deltaforge_mysql_activation_window_deferred_total`,
  `deltaforge_mysql_activation_window_overflow_total`);
- failed proof rounds and capture-kill failures
  (`deltaforge_mysql_proof_rounds_failed_total`,
  `deltaforge_mysql_capture_kill_failures_total`);
- a pipeline or connector failure, a drain timeout, or a resource-floor stop.

A generator-bound run (writers below 95% of the offered rate) is marked so,
as in stage 1, and does not count as a result.

## Bounds established elsewhere, and what stage 2 observes

Stage 2 does not verify these bounds. It reports observations next to them.

**Store statements.** The bound is 54 statements per atomic plan of an
activation window (`docs/design/mysql-mass-invalidation.md`, section 1). Its
scope:
- **counted:** the plan's two read transactions (their begin and commit, and
  every read chunk) and its write transaction (begin, the store lock, the
  sequence reservation, the `KvMaxSeq` read and write, every write chunk, the
  commit marker and the commit);
- **not counted:** checkpoint writes, history and freshness reads,
  commit-marker resolution after an ambiguous commit, incident writes and
  any other store traffic of the run;
- **retries:** a conflict replans, and each attempt is a plan of its own,
  within the bound. A storage retry of the write transaction repeats it, and
  is not within the one plan's count.

The bound is established at every cap at once by the gated
`storage/pg_statement_counts` lane. Arm D reports, per run, its total store
statements and commits from `pg_stat_statements` (one database per run) and
the plans from the proof trace. That total includes checkpoint and other
traffic, so it supports an average per plan as an observation, never a
per-window maximum.

**Connections.** The pool limits cover control-plane connections only: by
default 4 per source (2 to 16) and 32 per runner, of which 4 are reserved
for cancellations (`KILL` connections draw only on that reserve). Replication
sessions (the stream and proof scans) and snapshot connections are outside
the budget. Stage 2
samples DeltaForge's `deltaforge_mysql_connections` gauge by kind every
250 ms, and MySQL's own count of the system's connections. A sampled maximum
is a lower bound on the true peak.

**Memory.** Reported separately, with no bound claimed for RSS:
- **cache accounting:** the selection cache's estimated bytes and timelines
  (`deltaforge_mysql_selection_cache_bytes` and
  `deltaforge_mysql_selection_cache_timelines`, new, sampled every 250 ms),
  against its limits of 4,096 tables and 256 MiB;
- **in-flight working memory:** the largest activation window's buffered
  binary-log events (the proof trace's window `bytes`). A plan's write
  payload (at most 64 MiB) and a history load in flight (at most one
  timeline's limit) are bounded by construction and not exported, so they
  are not measured;
- **process:** RSS sampled every 250 ms, and jemalloc's allocation totals.

## Drain completion

A run's load ends, then:

1. **Delivery and frontier.** Wait until every committed row has arrived and
   the system's durable frontier covers MySQL's final `gtid_executed`:
   - DeltaForge: every sink checkpoint's GTID set
     (`GET /pipelines/<name>/checkpoints`);
   - Debezium: the connector is stopped (its tasks commit their offsets),
     then its committed offset's GTID set (`GET /connectors/<name>/offsets`).
2. **Shutdown.** Stop the system: DeltaForge's pipeline is deleted and its
   process stopped; Debezium's connector is deleted.
3. **Consumer drain.** Read the topic to its end offsets, taken after the
   shutdown.
4. **Ledger.** Check the ledger again over everything read: every committed
   row exactly once, in commit order per table, and nothing else.

A timeout in step 1 (300 s) or step 3 (60 s) fails the run. The fixed
two-second wait for late duplicates is removed.

## Resource controls

- **Floors:** 25 GiB free on the disk that holds Docker's data and the
  harness output (one filesystem here), and 4 GiB of available memory. The
  50 GiB start rule applies to core gates, not to the study.
- **Continuous:** a monitor samples both every second from the start of an
  invocation to its end: fixture creation, binary-log padding, runs and
  drains.
- **On a breach:** the monitor cancels the current step. The run in progress
  is stopped (its system shut down, its writers halted), recorded as failed
  with the floor and the values seen, and the invocation ends. Everything
  already written stays.
- **Recorded per run:** the lowest free disk and available memory seen.

## Reuse, ordering and contamination

- **One system at a time.** A run starts only after the previous run's
  system is stopped. Kafka Connect's worker stays up between runs with no
  connector while DeltaForge runs; it is recorded as such.
- **Order.** Each run records its sequence number, its place in the
  invocation, the system and its wall-clock start and end. Repetitions
  alternate which system runs first, as in stage 1.
- **Per-run state reset:**
  - DeltaForge: a new process with a new state directory, or a new
    PostgreSQL database in arm D, dropped after its counts are read; a new
    pipeline name and topic;
  - Debezium: a new connector name, server id, schema-history topic and
    topic. The offsets topic is the worker's, keyed by connector name.
- **Reused data.** The fixture's tables keep the rows of earlier runs. Each
  run writes ids of its own range, and both systems start with no position
  and no snapshot, so neither reads earlier rows. Before each run the binary
  log is flushed and the older files purged, so no earlier run's events are
  readable. A row outside the run's id range fails the run.
- **Fixture growth.** Before and after each run: MySQL's data directory
  size, and the rows the run committed. The total is reported with the
  results, beside each run's position in the order.
- **Warm state.** MySQL's buffer pool is shared across runs. Alternating the
  system order is the control, as in stage 1.

## Kept open: the historical 2x burst gap

The diagnosis run of 2026-10-30 found PostgreSQL proofs about twice as slow
per DDL in its 4,200-DDL eviction phase (79 to 95 ms against 43 ms). It was
never decomposed, and its raw data and harness are lost. The rebuilt
`actdiag` comparison did not reproduce it: on a backlog of DDLs, old and
main differed by about 9%, and the transaction change came in below both.
Stage 2's workload is inserts only, so it can neither confirm nor explain
the gap. The stage 2 results carry it as an open item. No extra control is
run.

## Kept visible: the SQLite restart observation

`act-restart` on SQLite was about 10 ms slower at p50 and 14 ms at p99 on
main and on the transaction change than on the older build, inside the
50 ms tolerance. It predates the transaction change and coincides with
main's restart path doing no history loads where the older build did 200.
An earlier confounded pair (518 to 669 ms p50) was never re-measured.

Stage 2 runs start with no committed position. The catch-up phase stops the
pipeline for 15 s and resumes it from its durable position, so restart
behavior appears there. For each SQLite run the results report: resume to
serving, catch-up time, and the latency of rows of tables activated before
the stop. That shows whether the difference grows with table count; it does
not explain it.

## Expansion and reporting

- **First pass:** arms C, A, B and D at 3 repetitions each.
- **Expansion rule** (the design's): a point goes to 5 repetitions when the
  two systems' medians are within 10%, a run failed, or a repetition is more
  than 25% from its point's median. In arm D (one system) only the last two
  apply.
- **Frozen list:** derived from the first pass and returned for review
  before any expansion runs. The expansion is one fresh invocation per arm;
  its runs replace the point's 3-repetition estimate and are not combined
  with it.
- **No claim:** stage 2 makes no fleet claim. Latency against Debezium,
  start-up and the window-count curve are measured and reported, with no
  pass or fail threshold.

## Evidence

Each invocation is copied to
`~/deltaforge-evidence/scale-stage2-<arm>-b0adf611/` (runs, raw rows,
phases, proof traces, monitor samples, fixture records) and verified against
its manifest before any of its resources are removed. Containers and
volumes left by an invocation are stopped, inventoried and removed only with
approval.

## Harness changes before the first run

1. **PostgreSQL state store:** a scaling run on the PostgreSQL store gets a
   database of its own, and reports its store statements and commits. (The
   driver always started DeltaForge on SQLite while labelling such a run
   "PostgreSQL".)
2. **Activation counts and windows:** selected, touched, activated, windows,
   plans, cuts and time to the last activation, from the commits, arrivals
   and the proof trace, which a DeltaForge run now requires.
3. **Observations and measured failures:** connections by kind, cache
   accounting, window payload and RSS sampled; the counters above fail a run.
4. **Cache accounting gauges** in the runner:
   `deltaforge_mysql_selection_cache_bytes{pipeline,source}` and
   `deltaforge_mysql_selection_cache_timelines{pipeline,source}`, from the
   cache's own accounting. This is the one change outside the harness.
5. **Drain completion** as above, for both systems.
6. **Continuous resource monitor** as above, with cancellation and
   evidence preservation.
7. **Order, reset and contamination records** as above. A run's system is
   now stopped on every exit path, including errors.
