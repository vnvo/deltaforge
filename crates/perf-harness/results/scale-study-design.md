# Per-instance scaling study: design

Fixed before any result. It compares one DeltaForge MySQL source with one
Debezium MySQL connector as the number of tables and the share that changes
grow. The results are single-instance scaling curves on one host, never a
fleet qualification.

## Topology

- **DeltaForge:** one pipeline with one MySQL source. Its tables are given
  as a database-prefix pattern (`scale_*.*`). It writes JSON to one Kafka
  topic.
- **Debezium:** one connector with a regular-expression include list
  (`database.include.list=scale_.*`), `snapshot.mode=no_data`, JSON, and the
  tuned profile of the comparison. `ByLogicalTableRouter` routes every table
  to one topic, as DeltaForge's sink writes them. Debezium's default
  topic-per-table topology would need up to 40,000 Kafka topics, which
  measures Kafka rather than the CDC system. A connector per database, or a
  topic per table, is a separate fleet architecture for later.
- **Shared:** both systems read the same fixture on the same MySQL server,
  one at a time.

## Fixture

- **Tables:** 100, 1,000, 10,000 and 40,000 narrow tables (an id and three
  columns), 100 tables per database (`scale_<d>.t<k>`).
- **Reuse:** a fixture is built once per table count and shared by every
  run of both systems. Each run writes ids of its own, so correctness is
  counted per run.

## Active sets

- **Fractions:** 1% (the sparse reference), 20%, 50% and 100% of the tables.
- **Choice:** the active tables are drawn with a fixed seed, so every run
  of a point uses the same set.
- **Static:** the set does not change during the run.
- **Rotating** (20% and 50% only): every 10 s, 10% of the active set is
  replaced by tables drawn from those not currently active. A run continues
  until at least half of the initially inactive tables have been activated,
  and for at least 6 intervals. At 100% rotation has no meaning and is not
  run.

## Workload modes

**Fixed aggregate load.** 2,000 changes per second in total, whatever the
active fraction, each change a one-row insert committed on its own, into an
active table drawn at random (fixed seed). It isolates the cost of the
table count and the active set. It is not a throughput measurement.

- **Steady:** 2,000 changes/s for 120 s.
- **Burst:** the existing burst shape scaled to the same average: 1 s at
  10,000 changes/s, then 4 s idle, for 120 s.
- **Writers:** several MySQL connections share the offered rate, because
  one connection cannot commit 2,000 single-row transactions a second. A
  run whose writers fall below 95% of the offered rate is marked
  generator-bound, not counted as a result.

**Capacity sweep.** Run at 20%, 50% and 100% active, for the 1,000- and
40,000-table fixtures. The offered load starts at 2,000 changes/s and
doubles every 30 s. The declared limits:

- **Sustained:** a step is sustained when all three hold:
  - delivery over the step is at least 95% of the offered rate;
  - p99 commit-to-arrival latency is at most 5 s;
  - at the step's end, the last committed row arrives within 5 s.
- **Capacity:** the highest sustained step. The first failed step ends the
  sweep.
- **Generator limit:** a step where the writers fall below 95% of the
  offered rate ends the sweep as generator-bound. Capacity is then "at
  least" the last sustained step.

## Phases of every run

1. **Start:** the system starts on the fixture. Recorded:
   - startup-to-ready: until the pipeline (connector task) runs;
   - time to the first event: until the first marker row arrives.
2. **Steady state:** the load runs.
3. **Catch-up:** 30 s into the load the pipeline (connector) is stopped for
   15 s while the writers continue, then resumed. Recorded: the time until
   every row committed before the resume has arrived.
4. **Drain:** after the load ends, the time until the last row arrives.

## Latency

Commit to arrival, per row.

- **Activation latency:** the latency of the first row of each table in
  the run (static sets: the first touch since start; rotating sets: the
  first row after activation).
- **Steady-state latency:** the latency of all other rows.
- Both are reported as p50, p95, p99 and max. Rows that arrive during the
  catch-up phase are reported apart.

## Recorded for both systems

- **Correctness:** distinct ids and duplicates; missing rows; order (each
  table's ids arrive in commit order).
- **Startup:** startup-to-ready, time to the first event, catch-up and
  drain time.
- **Load:** offered and achieved rates; p50, p95 and p99 latency.
- **Resources:**
  - CPU and RSS;
  - DeltaForge's allocations (jemalloc);
  - Debezium's garbage-collection count and time (JMX).
- **Connections:** MySQL connections held by the system during the run;
  state-store connections (DeltaForge's SQLite has none; Debezium's store
  is Kafka).
- **MySQL:**
  - its CPU;
  - rows and bytes read (`Innodb_rows_read`, `Bytes_sent`);
  - binary log written and retained (`SHOW BINARY LOGS`).
- **Kafka:** requests and bytes produced.
- **Checkpoint writes:** DeltaForge's checkpoints; Debezium's offset
  commits.
- **Catalog work:**
  - DeltaForge: schema loads, proof requests and the bytes they scan.
    Amplification is proof bytes scanned per binary-log byte written.
  - Debezium: schema-history records and bytes, and its startup schema
    capture.
- **Rotation:** activation latency against steady latency, and how each
  evolves over the run.

## Staging and repetitions

1. 100 and 1,000 tables at all four fractions.
2. 10,000 and 40,000 tables at 1%, 20% and 50%.
3. 100% at 10,000 and 40,000 only if resource floors (disk at least 25 GiB
   free, memory at least 4 GiB available) and correctness stay healthy.
4. Rotation at 10,000 and 40,000 tables.
5. Capacity sweeps last.

Every point runs 3 repetitions, interleaved between the systems. A point is
expanded to 5 repetitions when any of these holds:

- the two systems' medians are within 10%;
- a run failed;
- a repetition differs from its point's median by more than 25%.
