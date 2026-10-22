# DeltaForge and Debezium: comparison results, October 2026

Measured with this crate's `canonical-perf` on one host. Every number here
is a single-instance result on that host. None of it is a fleet
qualification.

## Conditions

- **Host:** Intel i7-1355U laptop (2 performance and 8 efficiency cores,
  15 W), 31 GiB RAM.
- **DeltaForge:** a release build of `cd716061` (main after #166), the same
  executable in every run.
- **Debezium:** 3.6.3.Final on Kafka Connect 4.3 (OpenJDK 25), image pinned
  by digest. The `tuned` profile was used throughout, unless stated:
  - `poll.interval.ms=50`, `max.batch.size=2048`;
  - `snapshot.max.threads=8`;
  - producer `acks=all`, idempotence, LZ4, `linger.ms=5`.
- **Shared:** MySQL 8.4 and Kafka 7.5.0. Avro goes through the same schema
  registry. Parquet goes to the same RustFS 1.0.1 server.
- **Method:**
  - every cell runs 5 repetitions, interleaved between the systems, after
    an unrecorded warm-up;
  - medians are reported, with per-repetition values where they differ;
  - CPU is the process's (DeltaForge) or the worker container's
    (Debezium); the Kafka broker's CPU is not counted for either system.
- **Correctness:** a run counts distinct row ids and duplicates. In every
  successful run below no row was missing. Duplicates appear only in the
  crash cells.
- **Raw results:** kept on the measurement host under `target/perf/<dir>`,
  as named in each section, with `invocation.json` (arguments, pinned
  artifacts, harness executable SHA-256, working-tree revision and
  cleanliness), `runs.jsonl` and per-run configurations and logs. Every
  invocation used here ran on a clean tree; its revision is given.

## 1. Snapshots (direct, except as noted)

200,000 narrow rows. `snap-tuned` and `snap-defaults` (`6fde22b5`).

| Cell | DeltaForge | Debezium tuned | Debezium defaults |
|---|---|---|---|
| 1 table | 229.2k rows/s, 1.85 CPU s, 301 MiB | 94.3k, 8.53 CPU s, 1,197 MiB | - |
| 8 tables | 215.7k rows/s, 4.30 CPU s, 593 MiB | 99.2k, 16.18 CPU s, 1,173 MiB | 72.0k |
| 1 table, 4 chunk readers (architectural) | 237.4k rows/s, 3.74 CPU s, 420 MiB | 96.1k, 15.82 CPU s, 1,186 MiB | 71.6k |

- The 8-table cell is direct: both systems read up to 8 tables in parallel.
- The chunk-reader cell is architectural: DeltaForge reads one table with
  parallel chunk readers, while Debezium reads a table on one thread.
- MySQL's CPU over an 8-table snapshot was 0.62 s for DeltaForge and 0.87 s
  for Debezium.

## 2. Catch-up after downtime (direct)

A 200,000-row backlog written while the source was down. `catchup-tuned`
and `restart-tuned` (`fc0c6138`).

| Cell | DeltaForge | Debezium |
|---|---|---|
| Pipeline (connector) stopped, then resumed: first row / all rows | 0.10 s / 1.36 s (146.9k rows/s) | 0.76 s / 3.24 s (61.8k rows/s) |
| Process (worker) stopped, then started: first row / all rows | 0.25 s / 1.49 s | 12.95 s / 18.77 s |

## 3. DDL during streaming (direct)

200 transactions/s, with `ALTER TABLE ... ADD COLUMN` every 1,000
transactions (2 per run). `ddl-tuned` (`48b48851`). Latencies are p50, p95,
p99 and max, in ms. "After DDL" is the worst latency of the rows committed
within 2 s after a DDL. In every JSON run, the last row carried every added
column.

| Cell | DeltaForge | Debezium |
|---|---|---|
| JSON | 28 / 46 / 48 / 68 | 34 / 57 / 59 / 60 |
| JSON, after DDL | 68 | 59 |
| Avro | 24 / 43 / 45 / 97 | 34 / 57 / 59 / 81 |
| Avro, after DDL | 97 | 81 |

DeltaForge's worst latency right after a DDL is higher than Debezium's
(finding 2).

## 4. Crash and recovery (direct)

A 200,000-row backlog. Once half has arrived, the system is killed (SIGKILL
for DeltaForge, `docker kill` for the Kafka Connect worker) and started
again. Completion is by distinct row id.

- DeltaForge's restart includes the operator step `store-gate break`
  (finding 1).
- Debezium is crashed only after its worker has committed an offset (see
  finding 3).

`cmp-crash` (`5e864eb8`):

| | DeltaForge | Debezium, 60 s offset commits (default) |
|---|---|---|
| Kill to serving again | 0.18 s | 12.45 s |
| Kill to first new row | 0.28 s | 13.08 s |
| Whole backlog | 1.73 s | 18.16 s |
| Duplicates per repetition | 0, 1,030, 1,029, 0, 0 | 105,204, 107,704, 118,851, 105,178, 104,448 |
| CPU, including the restart | 1.85 s | 108.5 s |
| Peak RSS | 228 MiB | 1,061 MiB |

DeltaForge checkpoints after each delivered batch, so a crash replays at most
what was in flight (up to 2,000 rows here). Debezium replays everything
delivered since its last offset commit.

**Shorter offset commits.** `cmp-crash-flush1s` and `cmp-crash-flush60s`
(`d800a7ef`) ran the crash cell again with Debezium's worker committing
every 1 s (`--debezium-offset-flush-ms 1000`) and at the default 60 s. Both
also ran a saturated CDC cell (200,000 rows, JSON) for the steady-state
cost.

| Debezium | 60 s commits | 1 s commits |
|---|---|---|
| Crash: duplicates per repetition | 106,496 to 119,858 (median 110,529) | 23,266 to 91,128 (median 37,066) |
| Crash: kill to serving / first new row | 12.95 s / 13.94 s | 12.96 s / 14.00 s |
| Saturated CDC: offset commits per run | 0 | 3 (1.2/s) |
| Saturated CDC: Kafka requests per run | 1,861 | 1,862 |
| Saturated CDC: CPU / throughput | 12.44 s / 72.2k rows/s | 11.79 s / 76.4k rows/s |

Shorter commits cut the replay by about two thirds but not to a batch: the
backlog arrives in under 2 s, so even a 1 s interval leaves much of it
uncommitted at the kill. Recovery time does not change (the worker's
restart dominates). Over these 2.6 s runs the extra commits cost no
measurable CPU or Kafka requests. In both runs DeltaForge replayed 0 to
2,000 rows per crash. The delivery counters recorded for the crash cells of
these two runs are invalid (they restart with the process); the harness no
longer records them for a crash.

## 5. JavaScript filtering and routing (direct)

Both systems keep the rows with an even id and route each to one of two
topics by `id % 4`. DeltaForge does it in a JavaScript processor, Debezium
with its `Filter` and `ContentBasedRouter` transformations (GraalJS). The
consumer checks every row's topic. A row that should have been filtered out,
or that arrives on the wrong topic, fails the run. `cmp-route` and
`cmp-route-lat` (`8225b7d2`).

| Cell | DeltaForge | Debezium |
|---|---|---|
| Saturated, 50,000-row backlog (25,000 kept) | 54.8k rows/s, 1.1 CPU s, 281 MiB | failed in 6 of 6 attempts, each on a fresh worker: GraalJS ran out of Java heap (`-Xmx2G`, the image default) |
| 200 transactions/s, ms | 29 / 48 / 50 / 51, 1.5 CPU s, 66 MiB | 59 / 97 / 112 / 150, 11.1 CPU s, 1,940 MiB |

- Debezium's JavaScript runs on GraalJS's interpreter: its image's OpenJDK
  has no optimizing Truffle runtime.
- Debezium's latency row is from `cmp-route-lat`, run on its own on a
  warmed-up worker. In `cmp-route` every latency repetition followed a
  saturated failure, so it ran on a freshly replaced (cold) worker:
  64 / 131 / 163 / 184 ms there.
- A larger heap was not tried. It would be a different Debezium profile.

## 6. Parquet end to end (architectural)

Rows to Parquet objects on S3.

- **DeltaForge:** its durable S3 sink writes directly, one object per
  2,000-row batch.
- **Debezium:** it writes Avro to Kafka, and Confluent's S3 sink connector
  12.1.13 writes the topic as Parquet. The connector is configured to match:
  2,000 rows per object, and a partial object every second.

The columns differ: DeltaForge writes a flat envelope, Debezium a nested
one. `cmp-parquet` (`33e29015`).

| Cell | DeltaForge | Debezium |
|---|---|---|
| Narrow, 200,000 rows: all but the last object / every row visible | 3.9 s / 3.9 s | 8.3 s / 45.3 s |
| Narrow: objects, bytes | 100, 16.6 MB | 107, 12.8 MB |
| Narrow: CPU, peak RSS | 1.9 s, 197 MiB | 16.8 s, 1,082 MiB |
| Wide, 50,000 rows: all but the last object / every row visible | 2.3 s / 2.3 s | 3.7 s / 44.6 s |
| Wide: CPU, peak RSS | 1.5 s, 555 MiB | 9.9 s, 1,212 MiB |

The connector checks its scheduled rotation only when records arrive, or at
the worker's offset commit (60 s). Once the backlog stops, its last partial
object waits for that commit, which accounts for most of Debezium's
40-second tail. Its CPU excludes the Kafka broker's.

## Findings

1. **DeltaForge, restart after a crash needs an operator step.** The
   server's store gate does not expire, by design, so a killed server
   cannot start until `store-gate break --owner` is run for the dead owner.
   The harness performs and records that step, and its time is counted.
   Follow-up for review.
2. **DeltaForge, latency right after a DDL.** Its worst post-DDL latency (68
   ms JSON, 97 ms Avro) is above Debezium's (59 and 81 ms), although its
   p50 to p99 are lower. Follow-up for review.
3. **Debezium, a crash before the first offset commit skips rows.** A
   `snapshot.mode=no_data` connector killed before its worker's first
   offset commit (60 s by default) restarts with no offset, at the binary
   log's end. The undelivered backlog is skipped: 26,625 of 50,001 rows
   arrived in the one run where this happened (`crash-smoke`, before the
   harness waited for a committed offset). This was observed once and is
   reported, not used as a result.
4. **Debezium, saturated JavaScript runs out of memory** (section 5).

## Invalid runs

Both were discarded whole, never in part. Each directory keeps the run and
an `INVALID.txt`.

- **`cmp-crash-contaminated`** (`cc2547eb`). A `cargo build` of the harness
  ran on the host during repetitions 0 and 1, by mistake. The run was
  stopped during repetition 1 and rerun as `cmp-crash` with nothing else
  running. The containers it left were removed, and its 9 anonymous volumes
  were removed by exact ID with approval.
- **`cmp-route-contaminated`** (`5e864eb8`). Debezium's worker ran out of
  heap in the saturated warm-up but still answered, so it was not
  replaced. It kept using CPU while DeltaForge's repetition 0 ran (16.5k
  rows/s against 51.8k in the warm-up), and every later Debezium run
  failed against the dead worker. The harness now replaces the worker after
  any failed Debezium run (`8225b7d2`). Rerun as `cmp-route`.
