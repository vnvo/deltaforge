# Single-Source Capacity Frontier: First Campaign

**Status:** proposal for review. No runs until it is approved.

## Goal

Measure where one MySQL source on one DeltaForge instance stops keeping up: first as the catalog grows, then as traffic rises at a chosen working set. The result is a measured frontier (the highest viable point and the lowest saturated point of each sweep, or an unresolved limit), not a qualification matrix and not a support claim beyond the configuration measured.

The campaign reuses the fleet harness and the release qualification procedures. It changes no product code, and it does not fix the limits it finds: each limit is reported, and whether to work on it is a separate decision.

## Baseline (current findings)

- **Release qualification (G3):** one MySQL 8.4 server, 200 databases x 200 tables (40,000), one pipeline, active sets up to 1%, about 2,000 changes/s offered and achieved, a DDL storm: no loss, duplicate or reordering, no pipeline failure.
- **Activation after #196** (the capture A/B on `279ee777`, 40,000 tables): capture 0.7 to 1.5 ms per table; a 20% set (8,000 tables) finished activating in 29 to 49 s, a 50% burst (20,000 tables) in 86 to 125 s.
- **The earlier delivery stall** was a pipeline failure (a capture behind a table's rows), addressed by #198 and #205; G3's S3 passed on the fixed build.
- **Fixture build:** 40,000 tables in 91 s (16 connections), about 440 tables/s.

No throughput limit has been measured: every run so far offered a fixed rate and achieved it.

## Fixed parameters

- One MySQL 8.4 server, one pipeline, one Kafka sink (JSON), the SQLite state store, the release image (`deltaforge:qual-3f63a814`, pinned) and a pinned harness.
- Catalog shape: up to 2,000 databases x 150 tables. Active set: 40% of the catalog, fixed (no rotation).
- Host safety limits, enforced by the run wrapper: at least 50 GiB free at the start (byte-exact) and a 25 GiB floor during a run; a run is stopped when available memory falls below 4 GiB (the established floor) or any container is OOM-killed; a deadline per run, with a graceful stop and then a forced one.
- No latency or memory budget: lag, throughput and resources are reported as measured.

## Saturation, defined before any run

A step is **saturated** when, over its measurement window, any of these holds:

1. **Committed traffic not delivered:** DeltaForge delivers less than 95% of the rate MySQL committed.
2. **Sustained backlog growth:** the backlog (committed minus delivered) grows across the whole window, by more than one window-second of offered traffic.
3. **Resource exhaustion:** a host safety limit is reached (disk floor, available memory under 4 GiB, an OOM kill).
4. **Correctness or health failure:** any missing, duplicate or out-of-order event, a pipeline failure or an incident. This also ends the campaign.

CPU at a process's allocation is a diagnostic signal, recorded and used in attribution, never saturation by itself: a fully used component may still sustain the traffic.

Two ratios are kept apart for every step: **offered to committed** (the generator and MySQL) and **committed to delivered** (DeltaForge and the sink). Criteria 1 and 2 use only the second.

**Attribution.** A saturated step is attributed before it counts as a DeltaForge limit:

- **Generator limit:** MySQL committed less than 95% of the offered rate while MySQL is not saturated. Remedy, once per sweep: more writers or more rows per transaction, then the step is repeated. Until it is, the step measures nothing about DeltaForge.
- **MySQL limit:** the commit rate falls short with MySQL at its CPU allocation or with commit latency rising.
- **DeltaForge limit:** MySQL commits the offered rate, but the source's binlog position falls further behind the binlog head, or delivery falls short with Kafka unsaturated.
- **Sink limit:** produce latency or broker CPU saturates while DeltaForge keeps up with the binlog.

Only a DeltaForge limit bounds the frontier. A generator, MySQL or sink limit is reported as the ceiling of the environment at that point. When the evidence does not single out one component, the step is reported as an **unresolved environment limit**, never as a DeltaForge limit.

## Campaign (eight hours in all, including fixture builds)

Deadlines are fixed before the start; a phase that reaches its deadline stops and reports what it has.

| Phase | Deadline |
|---|---|
| Fixture builds and checks | 45 minutes |
| A: catalog growth | 3 hours |
| B: traffic staircase | 2 hours 30 minutes |
| C: confirmation | 1 hour 30 minutes |
| Report | 15 minutes |
| **Total cap** | **8 hours** |

Throughput is evaluated only after the active set has finished activating: activation is reported on its own.

### Phase A: catalog growth

At a fixed offered rate (the G3 profile, 2,000 changes/s), one screening run per catalog size:

| Point | Catalog | Active (40%) |
|---|---|---|
| A100 | 2,000 x 50 = 100,000 tables | 40,000 |
| A200 | 2,000 x 100 = 200,000 | 80,000 |
| A300 | 2,000 x 150 = 300,000 | 120,000 |

Writes are uniform over the active set (no skew), so every active table is touched. Touching all of them takes about n·ln n random changes (about 1.4 million for 120,000 tables, 12 minutes at 2,000/s), so activation is reported as activated tables over time and an activation rate, as well as its completion. Each run lasts until the active set has finished activating, then 10 measured minutes more. Recorded:
- time to activate the active set;
- lag through activation and after it;
- delivered and committed rates;
- process RSS and cache-owned memory;
- state-store size and growth;
- MySQL, Kafka and DeltaForge CPU.

**Stop increasing the size** at the first point that is saturated, that cannot finish activating within the phase deadline, or whose fixture does not fit the disk limits. The last viable size is the Phase A result. If a later size fails, the bracket is that pair.

**Disk.** Each empty table with `innodb_file_per_table` takes on the order of 100 KiB, so 300,000 tables may need about 30 GiB before any row. This estimate is measured at A100. If A300 would cross the 25 GiB floor, it is not built, and the limit is reported as the host's, not DeltaForge's.

### Phase B: traffic staircase

At the largest viable Phase A size (or a smaller one if activation dominates the day), with its 40% working set activated once, the offered aggregate rate rises in steps within one run: 2,000, 4,000, 8,000, 16,000, 32,000 and 64,000 changes/s, each step held 5 minutes (2 minutes settling, 3 measured). For every step, recorded:
- offered, committed and delivered events/s;
- delivered bytes/s;
- end-to-end lag p50/p99;
- backlog slope;
- per-process CPU and memory.

The staircase stops at the first saturated step, or at 64,000 if none saturates (then reported as "not reached").

The harness offers a fixed rate per run today. A rate schedule within a run (a list of `at_secs`, rate pairs) is the one harness change this needs, as its own reviewed harness PR. It touches no product code. Without it, each step would be a separate run, and every new pipeline would activate its working set again.

### Phase C: one sustained confirmation

The highest viable Phase B step (the last unsaturated one), on a fresh pipeline at the same size and working set: 45 measured minutes after its activation has finished, with the saturation criteria applied to the whole measured span. If activation plus 45 minutes cannot fit the phase deadline, Phase C is not run and the Phase B result stands unconfirmed.

### Time bound

The eight-hour cap is absolute. When it is reached, the campaign stops and reports what it has: a bracket, or the limit as unresolved. It is not extended.

## Report

- **Phase A:** one row per size: activation time, lag, rates, memory, store growth, CPU, and saturation with its attribution.
- **Phase B:** one row per step, and the frontier as (highest viable rate, lowest saturated rate) with the limiting component.
- **Phase C:** the confirmation result.

Every number states the configuration it was measured on. Nothing is extrapolated beyond it.

## Later, after the single-source limits are understood

Rotation of the active set, restarts (warm and cold) at the frontier, several sources in one process, catalogs beyond 300,000 tables, and the PostgreSQL state store. None is part of this campaign.

## Decisions needed before the campaign

1. Approval of the harness rate-schedule PR (harness only) for Phase B.

If A300 does not fit the disk limits, Phase A stops at the largest viable size (ruled 2026-11-08); no cleanup or hardware work is done to reach it.
