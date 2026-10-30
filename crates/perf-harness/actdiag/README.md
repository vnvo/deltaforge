# actdiag: forward-proof diagnosis comparisons

The measurement harness of `docs/design/mysql-ddl-proof-transactions.md`
(sections 6 to 8). It compares runner builds on MySQL DDL and activation
workloads. It is measurement tooling: not part of the core gate, and it
changes no product behavior. Its own check is `smoke.sh`.

## Files

| File | What it does |
|---|---|
| `main.rs` | The `actdiag` binary (`cargo build --release -p perf-harness --bin actdiag`). |
| `build.sh` | Builds one runner, `plain` or with the `perf-phases` feature, in a dedicated worktree, and records its identity (`identity.json`). |
| `backport_phases.py` | Applies the `perf-phases` instrumentation to f0171b4e or 3ff3773c, which predate it. |
| `patches/` | That instrumentation as a patch per revision. |
| `analyze.py` | Summarizes a series and judges it by the section 7 rule. |
| `smoke.sh` | The harness's unit tests and a 10-DDL run of one build against itself. |
| `retain.sh` | Manifest, durable copy and verification of a completed series. |

## Comparisons

Every repetition gets a fresh MySQL 8.4 server and a fresh state store,
each with its data on tmpfs, plus a new runner process and Kafka topic. All
images are pinned by digest. Builds alternate in rotated order (ABC, BCA,
CAB).

| Comparison | Fixture | Load during the phase | Phase |
|---|---|---|---|
| `ddl-unloaded` | 100 tables | none | 300 `CREATE TABLE`, each followed by a row in the new table |
| `ddl-loaded` | 1,000 tables | 2,000 rows/s on 150 busy tables | 1,000 DDLs alternating `CREATE TABLE` (with a row) and `ALTER TABLE .. ADD COLUMN` of a cached idle table, each altered once (with a row through its writer) |
| `ddl-burst` | 150 tables | 2,000 rows/s on all 150 | 4,200 `CREATE TABLE`, each with a row: 4,350 tables pass the 4,096-table cache |
| `act-fresh` | 200 tables | - | first rows at 400/s, one per table, then 10 s of ordinary rows |
| `act-burst` | 200 tables | - | all first rows at once, then ordinary rows |
| `act-restart` | 200 tables | - | activated, drained and restarted, then first rows at 400/s |

Busy tables, and the tables the ALTERs change, get one row each before the
phase, so their timelines are cached when the DDLs start. No table is
altered twice: a table altered again before its forward proof runs keeps
the earlier DDL pending, and without `binlog_row_metadata = FULL` its next
rows stop the source (the known forward-proof failure follow-up), a
different path from the one measured here.

A restart re-creates the pipeline with the same spec (the runner does not
keep API-created pipelines); the same source id resumes from its durable
state. The DDLs run on one connection as fast as the
server accepts them, so the runner works through a backlog.

## What a run measures

- **Phase completion:** from the first DDL to the arrival of the last DDL's
  row; for activation, from the first first-row commit to the last first
  row's arrival.
- **Per-DDL cost:** phase completion divided by the DDL count.
- **Busy-table lag:** commit to arrival of the busy tables' rows committed
  during the phase (p50, p99, max).
- **Activation latency:** commit to arrival of each table's first row;
  ordinary rows separately.
- **Drain:** see below. Reported as the time from the end of the load.
- **Phase distributions:** with a `phases` build, the growth of
  `deltaforge_perf_phase_latency_seconds{phase}` over the phase. These are
  distributions per phase (count, sum, buckets), not traces of single
  DDLs.
- **State store (PostgreSQL):** committed transactions, statements and
  store-lock calls during the phase (`pg_stat_statements`).

## Drain and correctness

A run drains authoritatively. After the load stops, it waits until both of
these hold:

- every sink's durable checkpoint (`GET /pipelines/{name}/checkpoints`)
  covers MySQL's final `gtid_executed` (`GTID_SUBSET`);
- the delivered rows equal the ledger of every row the harness committed
  from the pipeline's start: each exactly once, no other row, and each
  table's rows in commit order.

A drain that does not complete within the comparison's timeout (600 s for
DDL comparisons, 300 s for activation) fails the run. The failed run is
recorded and kept.

## Running a series

1. Build each runner in a dedicated worktree, never one shared with another
   worktree's target directory:

   ```
   build.sh <worktree> <rev> plain|phases target/perf/actdiag-bin/<rev>-<variant>
   ```

2. Run `smoke.sh <build-dir> <out>`.
3. Run the series:

   ```
   target/release/actdiag --build old=... --build main=... --build change=... \
       --comparisons ddl-unloaded,ddl-loaded,ddl-burst --store postgres --reps 3 \
       --out target/perf/evidence/<series>
   ```

4. Analyze it:

   ```
   analyze.py target/perf/evidence/<series> --old old --new change --also main
   ```

5. Retain it with `retain.sh target/perf/evidence/<series>`, before removing
   anything of it.

## What a series retains

- `invocation.json`:
  - the command and its arguments;
  - the harness revision, its cleanliness and its executable's SHA-256;
  - every build's identity (`build.sh`);
  - the pinned images;
  - the host: kernel, CPU, CPU count, governor, memory, disk, Docker;
  - the MySQL settings the fixture sets.
- `runs.jsonl`: one line per run, with every measurement, the seed, the
  runner's PID and SHA-256, the order, and the run's container IDs.
- `runs/<comparison>-<build>-r<rep>/`:
  - `pipeline.json`;
  - the runner's logs;
  - `rows.csv` (every committed row and its arrival);
  - the final metrics scrape.
- `summary.md` and `verdicts.json` (`analyze.py`), and `MANIFEST.sha256`
  (`retain.sh`).
