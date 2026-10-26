# Scaling study, stage 1: 100 and 1,000 tables, October 2026

Stage 1 of `scale-study-design.md`. One DeltaForge MySQL source and one tuned
Debezium connector ran 100- and 1,000-table fixtures at 1, 20, 50 and 100%
active, under steady and burst load (2,000 changes/s on average), from a fresh
binary-log file. These are single-instance results on one laptop host, not a
fleet qualification.

## Result

DeltaForge is correct and resource-efficient at 100 to 1,000 tables:
- every run delivered every row once and in order;
- it uses 0.17-0.34 cores and 51-117 MiB, against Debezium's 0.25-0.47 cores
  and about 1 GiB;
- it is ready in 0.16 s, with its first event at 0.25 s, against Debezium's
  first event at 5.3-5.5 s;
- its steady-load p99 is 46-49 ms, against Debezium's 55-58 ms.

It is slower to activate tables. Each table touched for the first time waits
for a schema proof:
- activation p50 is 130-200 ms (Debezium: 28-56 ms);
- in 1,000-table bursts that first touch hundreds of tables, ordinary rows
  queue behind the proof windows. Steady p99 is then 265, 526 and 839 ms at
  20, 50 and 100% active, against Debezium's 92-96 ms.

The activation diagnosis that followed (its design record is under review) found:
- every stage-1 run starts a pipeline with no committed position, so it
  measures the cold-start case, in which every active table needs a proof;
- ordinary rotation, restarts and reconnects need no new proof;
- events that invalidate many tables at once still have per-table
  control-plane costs (state-store round trips, MySQL connections, serial
  history reads). Projected to large fleets, those costs are unacceptable.

Stage 2 (10,000 and 40,000 tables) is held until that work is done, so that it
validates bounded behavior rather than confirming a known linear cost.

## Invocations

- **Original stage 1:** `scale-stage1r-f0171b4e`, runner and harness
  `f0171b4e`, 96 measured runs (16 points, 3 repetitions per system,
  interleaved), all correct.
- **Expansion:** `scale-stage1x-f0171b4e`, the same runner with harness
  `c9cd68b1` (`--scale-points` only), 80 measured runs, all correct.
  - Eight points met the design's expansion rule. The rule was applied to
    steady p99 and activation p99, for either system: medians within 10%, or
    a repetition more than 25% from its median.
  - The list was frozen before the expansion ran. Each point was re-run at 5
    repetitions in one fresh invocation. Those runs replace the point's
    3-repetition estimate (marked `*` below) and were not combined with it.
  - The 3-repetition medians of these points stay in the original invocation.
    Between the two estimates, steady p99 differs by up to 11% (DeltaForge,
    `t100-a20-burst`: 69 against 78 ms), and activation p99 by up to 24%
    (DeltaForge, `t1000-a50-steady`: 286 against 231 ms). No conclusion
    changes.
- **Not used:** `scale-stage1-f0171b4e`, an earlier attempt that stopped at
  the harness's 25 GiB disk floor after 68 runs. It is kept unchanged and
  marked incomplete.

## Results (medians)

Activation is the latency of each table's first row. Where fewer than 20
tables were activated, its p99 is a sparse maximum, shown as `max`.

| Point | Runs | System | Ready s | First event s | Steady p50/p99 ms | Activation p50/p99 ms | CPU cores | RSS MiB |
|---|---|---|---|---|---|---|---|---|
| t100-a1-burst-o0 | 3 | DeltaForge | 0.16 | 0.25 | 27/46 | 6/max 6 | 0.18 | 59 |
| t100-a1-burst-o0 | 3 | Debezium | 0.26 | 5.40 | 30/57 | 39/max 39 | 0.25 | 954 |
| t100-a20-burst-o0 | 5* | DeltaForge | 0.16 | 0.24 | 32/69 | 158/160 | 0.17 | 60 |
| t100-a20-burst-o0 | 5* | Debezium | 0.27 | 5.29 | 50/81 | 41/42 | 0.30 | 948 |
| t100-a50-burst-o0 | 5* | DeltaForge | 0.16 | 0.24 | 33/98 | 168/169 | 0.17 | 62 |
| t100-a50-burst-o0 | 5* | Debezium | 0.26 | 5.24 | 56/89 | 40/53 | 0.32 | 957 |
| t100-a100-burst-o0 | 5* | DeltaForge | 0.15 | 0.24 | 33/141 | 189/228 | 0.17 | 64 |
| t100-a100-burst-o0 | 5* | Debezium | 0.26 | 5.25 | 57/91 | 39/72 | 0.32 | 959 |
| t100-a1-steady-o0 | 5* | DeltaForge | 0.16 | 0.25 | 27/47 | 6/max 6 | 0.31 | 55 |
| t100-a1-steady-o0 | 5* | Debezium | 0.27 | 5.32 | 30/56 | 36/max 36 | 0.39 | 823 |
| t100-a20-steady-o0 | 3 | DeltaForge | 0.16 | 0.26 | 28/48 | 133/134 | 0.32 | 58 |
| t100-a20-steady-o0 | 3 | Debezium | 0.27 | 5.37 | 32/57 | 33/33 | 0.47 | 939 |
| t100-a50-steady-o0 | 5* | DeltaForge | 0.16 | 0.24 | 27/47 | 140/142 | 0.18 | 55 |
| t100-a50-steady-o0 | 5* | Debezium | 0.27 | 5.38 | 30/56 | 30/33 | 0.30 | 940 |
| t100-a100-steady-o0 | 3 | DeltaForge | 0.17 | 0.26 | 28/48 | 162/163 | 0.34 | 58 |
| t100-a100-steady-o0 | 3 | Debezium | 0.27 | 5.48 | 32/58 | 30/53 | 0.46 | 953 |
| t1000-a1-burst-o0 | 5* | DeltaForge | 0.15 | 0.25 | 31/61 | 161/max 161 | 0.18 | 61 |
| t1000-a1-burst-o0 | 5* | Debezium | 0.26 | 5.48 | 44/73 | 41/max 41 | 0.33 | 952 |
| t1000-a20-burst-o0 | 3 | DeltaForge | 0.16 | 0.24 | 34/265 | 264/294 | 0.18 | 73 |
| t1000-a20-burst-o0 | 3 | Debezium | 0.26 | 5.33 | 58/92 | 44/83 | 0.32 | 1148 |
| t1000-a50-burst-o0 | 3 | DeltaForge | 0.16 | 0.25 | 34/526 | 469/589 | 0.17 | 92 |
| t1000-a50-burst-o0 | 3 | Debezium | 0.25 | 5.33 | 60/94 | 45/76 | 0.32 | 1149 |
| t1000-a100-burst-o0 | 3 | DeltaForge | 0.16 | 0.26 | 35/839 | 665/879 | 0.18 | 117 |
| t1000-a100-burst-o0 | 3 | Debezium | 0.26 | 5.38 | 61/96 | 56/104 | 0.33 | 1149 |
| t1000-a1-steady-o0 | 3 | DeltaForge | 0.16 | 0.26 | 28/48 | 134/max 134 | 0.32 | 52 |
| t1000-a1-steady-o0 | 3 | Debezium | 0.26 | 5.36 | 31/57 | 30/max 32 | 0.45 | 1105 |
| t1000-a20-steady-o0 | 3 | DeltaForge | 0.17 | 0.26 | 28/48 | 188/222 | 0.33 | 65 |
| t1000-a20-steady-o0 | 3 | Debezium | 0.26 | 5.37 | 32/58 | 31/64 | 0.47 | 1107 |
| t1000-a50-steady-o0 | 5* | DeltaForge | 0.16 | 0.25 | 27/48 | 213/286 | 0.18 | 72 |
| t1000-a50-steady-o0 | 5* | Debezium | 0.27 | 5.35 | 29/56 | 33/62 | 0.32 | 922 |
| t1000-a100-steady-o0 | 5* | DeltaForge | 0.15 | 0.26 | 27/86 | 303/405 | 0.18 | 86 |
| t1000-a100-steady-o0 | 5* | Debezium | 0.26 | 5.42 | 30/56 | 35/71 | 0.32 | 934 |

`*` five-repetition estimate from the expansion invocation.

## Conditions

- **Host:** one Intel i7-1355U laptop, shared by MySQL 8.4, Kafka, the load
  generator and both systems.
- **Load:**
  - 64 writer connections;
  - a source MySQL that does not sync its logs (load-generation settings, not
    recommendations);
  - 120 s of load per run, with the design's catch-up phase.
- **DeltaForge side:** SQLite state store, JSON to one Kafka topic.
- **Debezium side:** `snapshot.mode=no_data`, its tuned profile, every table
  routed to one topic.
