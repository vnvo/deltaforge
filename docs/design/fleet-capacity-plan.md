# Bounded Fleet-Capacity Plan (after v0.1.0-rc.1)

**Status:** proposal for review. No runs and no product changes until it is approved.

## Purpose

v0.1.0-rc.1 is qualified for one instance at the scale of its qualification runs: one MySQL 8.4 server with 40,000 tables and one pipeline at active sets up to 1% (G3), and one PostgreSQL 17 source with 1,000 tables (G4). Everything larger is unqualified (release checklist S2).

This plan answers three questions with a fixed number of runs, in order, and stops for a ruling after each stage. It is not the full fleet qualification of the [fleet qualification design](mysql-fleet-qualification.md) (25 clusters of 400,000 tables per instance): that target does not fit the available host, and it needs owner inputs this plan does not assume.

## Questions

1. **Dense activity on one source.** Does one pipeline sustain 20%, 50% and 100% of 40,000 tables active, and a rotating active set, with cold activation and warm restart measured separately?
2. **Several pipelines in one process.** How do connections, the shared caches, the state store and process memory behave with several sources in one instance, and is per-pipeline cost additive?
3. **A larger catalog per source.** What changes between 40,000 and a larger catalog on one source (cache behavior, state-store size, activation)?

Each answer is a measured statement for the configuration run, never an extrapolated ceiling.

## Known risks going in

These are recorded findings, not new claims. Stage 1 measures them as they are; any fix is its own reviewed PR afterwards.

- **Activation pacing.** New tables are activated in windows run one after another, and window time is dominated by the schema capture (2 to 6 ms per table measured at 40,000 tables). Dense or rotating activity may leave rows of already-active tables waiting behind the window chain.
- **An unexplained delivery stall** of about 23 s, seen in some runs of the 10,000-table, 50% steady point (3 of 10 in one comparison), recovered only by the scheduled stop.
- **Process memory.** Only cache-owned memory is accounted and bounded; process RSS is not, and it was about 150 MiB higher at 20,000 tables with the byte-budget cache. There is no aggregate memory cap.
- **Broad schema invalidation** pauses are provisionally accepted (about 120 ms for a barrier over 90% of a database's tables); frequent broad invalidation is unqualified.

## Stages

Every stage uses the procedures of the release qualification: pinned image and harness builds with recorded hashes, a fresh source id and state store per scenario verified before the start, at least 50 GiB free at the start (byte-exact) and a 25 GiB floor during the run, a deadline per invocation with a graceful then forced stop, stop at the first correctness failure, no rerun to obtain a pass, and evidence kept with a verified manifest.

### Stage 1: dense activity, one source (question 1)

- **Fixture:** the existing G3 fixture (one MySQL 8.4 server, 200 databases x 200 tables).
- **Scenarios** (one run each, 30 minutes after a 2-minute warm-up, the G3 traffic profile):
  - D20, D50, D100: fixed active sets of 20%, 50% and 100%;
  - R20: a moving 20% set that shifts to a disjoint set every 6 minutes, reaching 100% cumulatively;
  - after each scenario, a CDC-only restart of the pipeline (warm restart).
- **Measured and reported:** correctness (no missing, duplicate or out-of-order event); time to activate the active set (cold) and time to first change after the restart (warm); lag p50/p99 over the run; the window, capture and scan timings; process RSS peak and cache-owned memory; state-store growth; any stall.
- **Pass:** correctness only. Lag and memory are reported against proposed budgets (lag p99 5 s, process memory 16 GiB) without a pass/fail verdict, since the budgets are owner inputs.
- **Bound:** 4 scenarios plus 4 restarts, about 5 hours of runs. Then stop for a ruling.

### Stage 2: several pipelines, one process (question 2)

Only after the Stage 1 ruling.

- **Fixture:** 4 MySQL servers of 10,000 tables each (40,000 tables in all), one pipeline per server, one process, one SQLite state store. The server count is sized for the available host (12 cores, 31 GB) and is confirmed against Stage 1's measured memory before the stage starts.
- **Scenarios:** steady CDC at a 1% and a 20% active set (30 minutes each); a restart of all four pipelines; one server restarted under load.
- **Measured and reported:** as Stage 1 per pipeline, plus connections per server and in all, state-store operation latency, and whether process memory grows with the number of pipelines or with the active tables.
- **Bound:** 3 scenarios, about 3 hours. Then stop for a ruling.

### Stage 3: a larger catalog, one source (question 3)

Decided after Stage 2: the catalog size (100,000 or 200,000 tables) depends on fixture build time and disk, which Stages 1 and 2 measure.

## Out of scope

- The full fleet target (25 clusters x 400,000 tables per instance), and multi-host topologies.
- The PostgreSQL state store, and initial snapshots of large catalogs (the snapshot profile).
- Product changes. A risk confirmed by a stage is fixed in its own reviewed PR, or documented as a limit, before any later stage depends on it.

## Publication

The capacity envelope and the deployment support pages change only from completed stages, stating exactly the configuration run (servers, tables, active sets, pipelines, host). No result is generalized beyond it.

## Decisions needed before Stage 1

1. Whether the G3 traffic profile (2,000 changes/s average, 10,000 at peak, per source) is the right Stage 1 workload, or production numbers are available.
2. The 6-minute rotation period of R20, or another.
3. Whether to run Stage 1 on the code as released (recommended: it measures the known risks as they are), or to address the activation-pacing risk first.
