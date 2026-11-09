# Single-Source Capacity Frontier: First Campaign

**Status:** direction approved; execution recipe for review. No runs until it is approved.

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

- One MySQL 8.4 server, one pipeline, one Kafka sink (JSON), the SQLite state store, the published release candidate image (`ghcr.io/vnvo/deltaforge@sha256:63823feac0c30805072b1b3b4a87544d64b77af22a2a6bbf43ece5e7f919b0d9`, v0.1.0-rc.1) and the harness built from `f89cfbbb` (the merge of the rate-schedule PR), pinned separately.
- Catalog shape: up to 2,000 databases x 150 tables. Active set: 40% of the catalog, fixed (no rotation).
- Host safety limits, enforced by the run wrapper: at least 50 GiB free before each fixture build and 40 GiB before each measured run (byte-exact), and a 25 GiB floor throughout; a run is stopped when available memory falls below 4 GiB (the established floor) or any container is OOM-killed; a deadline per run, with a graceful stop and then a forced one.
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

The staircase is the harness's F1 scenario (`traffic.rate_steps`, `traffic.rate_step_secs`), so the working set activates once per run.

### Phase C: one sustained confirmation

The highest viable Phase B step (the last unsaturated one), on a fresh pipeline at the same size and working set: 45 measured minutes after its activation has finished, with the saturation criteria applied to the whole measured span. If activation plus 45 minutes cannot fit the phase deadline, Phase C is not run and the Phase B result stands unconfirmed.

### Time bound

The eight-hour cap is absolute. When it is reached, the campaign stops and reports what it has: a bracket, or the limit as unresolved. It is not extended.

## Report

- **Phase A:** one row per size: activation time, lag, rates, memory, store growth, CPU, and saturation with its attribution.
- **Phase B:** one row per step; the highest demonstrated viable rate, reported separately; and the frontier as (highest viable rate below the first stop, lowest saturated rate) with the limiting component. Only a saturated step brackets a ceiling. A step with incomplete evidence gives no conclusion at its rate, and the rate below it is not reported as the ceiling.
- **Phase C:** the confirmation result.

Every number states the configuration it was measured on. Nothing is extrapolated beyond it.

## Later, after the single-source limits are understood

Rotation of the active set, restarts (warm and cold) at the frontier, several sources in one process, catalogs beyond 300,000 tables, and the PostgreSQL state store. None is part of this campaign.

## Execution recipe

Everything below is fixed before the campaign starts; nothing is tuned while it runs. The kit is `crates/fleet-harness/capacity/`:

- `render.sh` writes every configuration;
- `campaign.sh` drives each step and enforces the floors and deadlines;
- `watch.sh` observes each run;
- `frontier.py` evaluates each run.

### Identities

| Pinned | Identity |
|---|---|
| Product | `ghcr.io/vnvo/deltaforge@sha256:63823feac0c30805072b1b3b4a87544d64b77af22a2a6bbf43ece5e7f919b0d9` (v0.1.0-rc.1 as published, the same digest on Docker Hub); pulled, never rebuilt; the amd64 image id is recorded at setup |
| Harness | `fleet-harness` built from `f89cfbbb`, sha256 `eba1a7908ea7e3388dd32309fe359a59bf42dcb6dd068a0dbee89587c45fb6ea`, copied into the campaign directory |
| MySQL, Kafka | G3's digests: `mysql@sha256:c36050af...` (8.4), `cp-kafka@sha256:fbbb6fa1...` (7.5.0) |
| Kit and configurations | this commit; `CONFIGS.sha256` lists the 27 rendered configurations |

`campaign.sh setup` does the following:

- copies the binary and the kit;
- renders the configurations and refuses if they differ from `CONFIGS.sha256`;
- writes `IDENTITY.sha256` (binary, kit, configurations);
- starts Kafka;
- records T0.

Every later step verifies `IDENTITY.sha256` and the local image digest, and refuses on any difference.

### Environment

The G3 allocations, on one Docker network `cap`:

| Component | Allocation | Notes |
|---|---|---|
| MySQL | 3 CPUs, 6 GiB | G3's flags |
| Kafka | 2 CPUs, 3 GiB | KRaft, topic auto-creation off |
| DeltaForge | 4 CPUs, 6 GiB | SQLite store |

The host has 12 CPUs and 31 GiB of memory. Each run gets:

- a new DeltaForge container;
- a new store;
- a run-named source, pipeline and topic, so every pipeline starts fresh.

Before every step, the preflight refuses if a cargo process or any container other than the campaign's, k3d or buildkit is running.

### Frozen configurations

| Run | Catalog | Scenario | Warm-up | Measured duration | Rate | Deadline | Drain max |
|---|---|---|---|---|---|---|---|
| `a100` | 2,000 x 50 | F0 | 0 | 1,380 s | 2,000 | 2,880 s | 600 s |
| `a200` | 2,000 x 100 | F0 | 0 | 2,100 s | 2,000 | 3,600 s | 600 s |
| `a300` | 2,000 x 150 | F0 | 0 | 2,820 s | 2,000 | 4,320 s | 600 s |
| `b<size>` | as A | F1, 2k-64k, 300 s each | 780 / 1,500 / 2,220 s | 1,800 s | steps | warm-up + 4,500 s | 1,800 s |
| `br<size>` | as A | F1, 8 rows per transaction | as `b` | 1,800 s | steps | as `b` | 1,800 s |
| `c<size>-<rate>` | as A | F0 | 900 s | 2,700 s | each step rate | 5,400 s | 900 s |

Common to all runs:

- **Working set:** 40% active set, fixed, with uniform writes (`zipf_exponent: 0`).
- **Workload:** 32 writers, 1 row per transaction (except `br`), op mix 60/30/10, 256-byte rows.
- **Sink:** JSON to Kafka.

How the numbers were fixed:

- **Activation allowance.** Each Phase A run's allowance is its duration minus the 600 measured seconds. That is about three times the expected activation: touching every active table takes n(ln n + 0.58) uniform changes, which is 223, 475 and 736 s at 2,000/s.
- **Phase A deadlines.** The three deadlines (duration, 600 s drain and 900 s setup and verification) sum to the 3-hour phase deadline.
- **Phase B warm-up.** Each size's Phase B warm-up is its Phase A allowance, so a size viable in Phase A activates before its first step.
- **Phase C deadline.** The Phase C deadline equals its phase deadline.

### Activation completion

Completion is established from distinct table identities in the run's retained Kafka events, not from a sum of per-window counts. A sum, even one equal to the target, cannot exclude repeated activations and untouched tables.

- **Coverage pass.** After the run, before any deletion, `frontier.py coverage` reads the run's whole topic with the pinned Kafka image's console consumer and records, for each source table (`source.db`, `source.table`), the earliest record timestamp. The record count must equal the sum of the topic's end offsets. The consumer must report its total, and every record must parse.
- **Completion** is the latest of those first appearances, once exactly the active set has appeared: 40,000, 80,000 or 120,000 distinct tables, exactly ceil(0.4 x catalog). A record is produced only after its table's activation, so the boundary is never earlier than activation.
- **Why distinct delivery means coverage.** The writers only touch the active set, and the completeness check proves every committed change was delivered with nothing unexpected. So exactly that many distinct delivered tables means every active table was captured.
- **Not established:** a different number of distinct tables leaves completion not established. That run's throughput is not evaluated and its topic is kept.
- **Incomplete:** a short read, a missing consumer total or an unreadable record leaves the evidence incomplete. The run is inconclusive and its topic is kept.
- **Reported only:** the product's `window` records (`df.log`) are summarized, never used as the boundary.

How each phase uses it:

- **Phase A:** the measured window is from completion to the end of writes, and must be at least 600 s. Otherwise the size did not activate within its allowance and is not viable.
- **Phase B:** a step is measured only if completion precedes its measured window. Live mode follows the topic with the same parser only until coverage is complete, which happens during warm-up, so no reader runs during the measured steps.
- **Phase C:** the measured window starts at the later of warm-up end and completion, and must be 2,700 s. Otherwise the confirmation is reported as unconfirmed.

### Step transitions

F1 records only planned times, so the evaluator records the actual ones:

- **Steps 2 to 6:** the start is solved from `offered_ops` within the sample interval that straddles each rate change. This is exact for a rate that is constant within each step.
- **Step 1:** it continues the warm-up rate, so its start is step 2's minus 300 s.
- **Measured window:** each step's window runs from its start plus 120 s to the next step's start, and only samples inside it count. The straddling interval and the settling time are therefore excluded from every rate comparison.
- **Too short:** a window with less than 150 s of samples is reported as not measured.

On a synthetic staircase, the solved transitions land within the simulation's 0.1 s tick.

### Evaluation (`frontier.py`)

Each window records:

- the offered, committed and delivered rates, and both ratios;
- the backlog (committed minus delivered) at the start and end, and its slope;
- lag p50 and p99, and the lag's slope;
- DeltaForge memory;
- each container's CPU against its allocation, and the harness's CPU;
- the minimum available host memory.

The criteria are those defined above:

1. **Committed traffic not delivered:** committed to delivered below 0.95.
2. **Backlog growth:** the backlog grew by more than one second of the window's offered rate, with a positive slope. In other words, the pipeline's delay grew by over a second across the window.
3. **Resource exhaustion:** `MEMFLOOR`, `OOM` or the disk floor.
4. **Correctness or health failure:** the result's correctness or evidence fails; any missing, duplicate, unexpected, final-state, ordering, decode or key error; a failed pipeline; any incident; or an instance restart.

**Unknown is never zero.**

- **Delivery samples:** a window is judged only when every sample in it carries delivery counters and no gap exceeds 30 s. Otherwise its delivery is unknown: Phase A and C are inconclusive, and in Phase B the step is "evidence incomplete" and caps the frontier below it.
- **End state:** incidents are read from the final metrics scrape with HTTP, read and parse errors kept. An HTTP error, an unparseable line, or a scrape without `deltaforge_sink_events_total` leaves incidents unknown, with the error recorded. The incident counter exists only once raised, so its absence counts as zero only in a complete scrape. The scrape is saved as `metrics-end.txt`. An unreadable restart state is unknown in the same way.
- **Result:** an absent completeness check or stream summary is incomplete, not a pass.
- **Verdict:** any unknown leaves the run inconclusive with `evidence_complete: false`. It is never a pass and never a failure.

**Generator check:** committed below 95% of offered is reported as a generator limit, or as a MySQL limit when MySQL is at 90% or more of its 3 CPUs. It is never saturation.

**Attribution** of a saturated step:

- **DeltaForge limit:** lag rising, with Kafka below 90% of its CPUs.
- **Sink limit:** Kafka at 90% or more, with lag flat.
- **Unresolved environment limit:** anything else.

The harness measures neither commit latency nor produce latency, so CPU and lag are the attribution signals, and the report says so.

**Exit codes:**

| Exit | Meaning |
|---|---|
| 0 | viable |
| 1 | saturated |
| 2 | correctness or health failure (ends the campaign) |
| 3 | inconclusive |

**Live mode (Phase B).** The evaluator follows the run and sends SIGINT after the first measured step that is saturated or generator-limited. It also sends SIGINT when its reader has passed step 2's window start without complete coverage (`ACTIVATION-STOP`), since no later step could then be measured. A step stays undecided while coverage is unknown. The harness's abort path still stops the writers, drains, and checks completeness.

**Self-tests on synthetic runs** (a docker shim stands in for the Kafka tools; no container is touched):

- **Saturation:** a staircase saturated at 32,000 was reported viable through 16,000, saturated at 32,000, and a DeltaForge limit; transitions were exact.
- **Coverage:** repeated tables were counted once and the earliest timestamp kept. Thirty tables activated repeatedly did not reach 40. A short read, a missing consumer total and an unreadable record were each incomplete.
- **Activation:** product windows summing to the target, with fewer distinct tables, did not establish activation. Steps before coverage were not measured.
- **Unknowns:** a window with missing delivery samples was evidence incomplete and capped the frontier below it. Unreadable incidents, an unknown restart state, an incomplete coverage read and an absent completeness check were inconclusive. A raised incident was a failure.
- **End-state reader** (the script in `campaign.sh`, against a local server): a refused connection, HTTP 500, an unparseable body and a scrape without the sink counter were all unknown. A complete scrape without incidents read zero, and raised incidents were summed.
- **Live:** stopped in the saturated step. Stopped on activation when its reader passed step 2 with 30 of 40 tables. A lagging reader did not stop the run.

### Floors and deadlines (enforced by `campaign.sh`)

- **Disk.** Every fixture build and every run checks two things:
  - **At the start:** before a fixture build, at least 53,687,091,200 bytes (50 GiB) free on the campaign directory's and Docker's filesystems; before a measured run, at least 42,949,672,960 bytes (40 GiB). Both byte-exact.
  - **While running:** it runs under `floor-watch.sh` with `FLOOR_GIB=26` and its deadline: SIGINT, then SIGKILL after 120 s.
- **Memory.**
  - **At the start:** refused below 4 GiB available.
  - **While running:** `watch.sh` stops the harness below 4 GiB, when a campaign container is OOM-killed or stops, or when the pipeline fails.
- **Deadlines.** An invocation starts only if its deadline fits both of these. Otherwise it is refused (exit 6) and the phase reports what it has.
  - **Phase budget:** the rest of its phase's budget, where fixtures share 2,700 s, Phase A 10,800 s, Phase B with any remedy 9,000 s, and Phase C 5,400 s;
  - **Total cap:** T0 plus 7 h 45 min, since the last 15 minutes belong to the report.
- **Stopped runs** still drain and check completeness, and their evidence is kept.

### Sequence

1. **Setup:** the preconditions below, then `campaign.sh setup` (T0).
2. **A100:** `mysql 100`, `fixture 100` (measures bytes per table), `run A a100`.
3. **Larger sizes.** If `a100` is viable: `drop 100`, `mysql 200`, `fixture 200`, `run A a200`; then the same for 300.
   - **Fit check:** `mysql` refuses a size whose fixture would leave less than 40 GiB free, so its run could not start. It uses A100's measured bytes per table, or the 110 KiB prior estimate before A100 is measured.
   - **Stop rule:** Phase A stops at the first size that is saturated, inconclusive on activation, refused by the fit check or a floor, or out of phase time.
   - **Result:** the largest viable size. No cleanup is done to make room.
4. **Phase B** at the largest viable size S:
   - **Rebuild:** if S's MySQL was dropped (a larger size was built and failed), S is rebuilt first, counted against the fixture budget.
   - **Run:** `run B bS`.
   - **Remedy:** after a generator-limited stop, `run BR brS` once, if it fits the Phase B budget. Otherwise the generator ceiling is reported.
5. **Phase C:** `run C cS-<highest viable rate>`, if it fits. Otherwise the Phase B result stands unconfirmed.
6. **Report** from the `verdict.json` files.

Exit 2 at any step ends the campaign.

### Evidence

**Per run,** `runs/<name>/` holds:

- the harness result (`result.json`, `samples.jsonl`, proof trace);
- `df.log`, `resources.jsonl`, `run.log`, `live.log`;
- `verdict.json`, `end-state.json`, `coverage.json`, `metrics-end.txt`, the container inspections;
- `MANIFEST.sha256`.

**Per campaign:** `campaign.log`, `clock.log`, `identity.txt`, `IDENTITY.sha256` and the fixture logs.

**Containers and topics:** DeltaForge containers are kept stopped (`cap-df-<run>`). After each run, in this order:

1. the end state, logs and inspections;
2. the coverage pass over the retained topic;
3. the evaluation;
4. `MANIFEST.sha256`, then verified with `sha256sum -c`;
5. the topic is deleted only if the manifest verifies and the verdict is `topic_deletable`: no failure, evidence complete, coverage established. A failed, incomplete or unestablished run keeps its topic. Each deletion or retention is logged.

**MySQL fixtures:** `drop <size>` removes the campaign's MySQL container and volume by the ids recorded at creation. It does so only after that size's fixture log exists and every run of that size verifies against its manifest.

### Decisions (reviewed)

- **Grants.** `mysql` creates `df` with the documented grants, including the fresh CDC start's `BACKUP_ADMIN` and `SELECT` on `performance_schema.log_status`, and refuses unless `SHOW GRANTS` shows both.
- **Disk floors (this campaign only).** 50 GiB free before each fixture build, 40 GiB before each measured run, the continuous 25 GiB floor and the 4 GiB memory floor. A size whose projected fixture growth would prevent its run from starting is not built.
- **Approved cleanup before setup:**
  - G3's stopped `g3-mysql` (`8c2583bcb53e`, volume `g3-mysql`) and `g3-kafka` (`9eae6edcb912`, volume `47d5feb87e0e`), after verifying full ids, mounts and preserved evidence;
  - `df-capacity-plan/target`, after the build, open-handle and artifact checks.
- **Within the campaign:** `drop` of campaign fixtures by recorded ids after their evidence verifies, and topic deletion as above.
- **Host limit.** If disk prevents a size, or stops the staircase, that is reported as a host limit. Floors are not weakened and no further cleanup is done to force progress. Phase B is expected to reach the disk floor in the 32,000 step (25 GiB of headroom at 1.8 KiB per change is about 14.5 million changes), unless DeltaForge saturates first.
- **Host sharing:** the k3d test cluster is allowed by the preflight and recorded.
- **Cap:** the eight-hour campaign cap is unchanged.
- **Tooling validation before the campaign.** One short real-container run in its own campaign directory: size `001` (20 databases of 5 tables, 40 active), `fixture 001`, `run V a001` (F0 at 2,000 changes/s for 6 minutes, a 10-minute deadline, with the live coverage reader), then `drop 001`. It checks the driver end to end and the console consumer's record format. It is tooling validation, not capacity evidence; its result is kept.
  - **First validation (2026-11-09, kept in `~/deltaforge-evidence/capacity-rc1-validation`).** The record format, grants, end state, coverage (544,591 of 544,591 records, 40 distinct tables), manifest, topic deletion and `drop` all worked. It found two driver defects. The harness PID was looked up by command line, which matched floor-watch's short-lived subshells, so `watch.sh` and the live reader attached to a process that had already exited. A live consumer started after its stop was left running. Fixed: the PID is now matched by process name and executable (a run without it is stopped and fails as `NOHARNESS`), the live reader stops a consumer that starts late, and the driver stops `cap-cov-live` after every run.

## Result of the first campaign (2026-11-09)

The campaign ended at its first measured run. Its finding is a memory scaling limit of the published RC, not a throughput ceiling.

- **A100** (100,000 tables, 40,000 active, 2,000 changes/s offered):
  - **Fixture:** built in 218 s at 146,597 bytes per table.
  - **Activation:** all 40,000 active tables were covered at 6 minutes.
  - **Memory:** DeltaForge grew about 3.7 KB per delivered event and kept growing after activation, while lag stayed under 1 s. It was OOM-killed at its 6 GiB limit after 13 minutes.
  - **MySQL:** at its 3-CPU allocation, committing about 94% of the offered rate.
  - **Measurement:** no window completed.
- **Cause (read-only investigation, `~/deltaforge-evidence/capacity-rc1-memory-investigation/REPORT.md`):**
  - DeltaForge keeps two maps from binlog `table_id` to the full table map event, and neither is ever pruned: the vendored parser's map and the MySQL source's `RunCtx.table_map`.
  - MySQL assigns a new `table_id` whenever a table is reopened after leaving its table definition cache. This run had a 2,000-entry definition cache; its binlogs show about 83% of row events under a new `table_id`.
  - The growth therefore depends on an active set larger than the server's table definition cache, not on catalog size. It applies inside the qualified 40,000-table catalog as well.
  - The 40-table validation runs stayed flat.
- **Standing restriction:** this is a capacity restriction of v0.1.0-rc.1 alongside its documented scope. A MySQL source whose written-to active set exceeds the server's table definition cache grows without bound, at roughly 4 KB per change on a table outside that cache.
- **Next:** a targeted reproduction and fix, before any further capacity run.

## A100 confirmation of the fix (2026-11-09)

The original OOM workload was run again on the merged fix (#212, `c9257fe1`): the same catalog, active set, traffic, MySQL table caches (2,000 definitions, 4,000 open tables) and DeltaForge limits (4 CPUs, 6 GiB), for 2,100 s (`a100c`). All five criteria, frozen before the run, passed. Evidence: `~/deltaforge-evidence/confirm-a100-c9257fe1-run` (`CRITERIA-RESULT.md`, run manifest).

| | v0.1.0-rc.1 (`a100`) | Fix (`a100c`) |
|---|---|---|
| Outcome | OOM-killed at 6 GiB after 13 minutes | completed 2,100 s, no stop, no incident, no restart |
| DeltaForge memory | about 0.4 GiB/min, kept rising after activation | 816 -> 809 MiB from 5 minutes after activation to the end; slope over the last 15 minutes -0.17 MiB/min; peak 920 MiB |
| Table maps held (sampled every 5 s) | not observable | parser 1, source 0 or 1, in all 281 samples |
| Correctness | 4,470 missing after the kill | 0 missing, duplicate, unexpected, final-state or ordering errors |
| Activation | 40,000 tables at 6 minutes | 40,000 tables at 317 s, then 1,783 s of writes |

**What it shows:** the table-map leak is resolved on this workload. Memory and correctness held across 100,000 catalog tables with 40,000 receiving writes.

**What it does not show:**
- **Throughput:** this is not a throughput result. MySQL committed 1,864 of the 2,000 offered changes/s at about 2.76 of its 3 CPUs.
- **Table maps:** the map conclusion covers sampled values only. Five-second samples do not establish the maximum between scrapes, and the parser gauge refreshes on the main read path only.

## Resuming the campaign from A100

**Carried forward:**
- **A100 memory and correctness:** the confirmation above.
- **A100 throughput:** not established at 2,000 changes/s, because the generator (MySQL) limited it.

A100 is not repeated only to reproduce the pass.

**1. Attribute the generator limit.**
- In both A100 runs, MySQL used nearly all of its 3 CPUs.
- The leading hypothesis is MySQL's own table cache. With 40,000 active tables against 2,000 table definitions and 4,000 open tables, about 83% of row events were written under a newly assigned table id (binlogs of the first run), so MySQL reopened a table for most statements.
- `watch.sh` will record MySQL's `Opened_table_definitions`, `Opened_tables`, `Table_open_cache_misses` and `Com_insert`/`Com_update`/`Com_delete` with each sample, alongside host and container CPU. This changes the observer only.

**2. Remove the limit before the staircase.** One generator calibration at A100 on the preserved fixture, using the existing Phase B run and its generator check. The live evaluator stops it at the first generator-limited step, and DeltaForge's limits are unchanged. The remedies are applied in order, each only if the previous one leaves the generator below the staircase's rates:
- **R1:** size MySQL's table caches to the active set (`table_definition_cache` and `table_open_cache` 50,000). This is configuration only. It also removes table-id churn, which the confirmation above already covered; the staircase then measures DeltaForge without churn.
- **R2:** raise MySQL to 5 CPUs. The host has 12: MySQL 5, DeltaForge 4 and Kafka 2, with the harness and the k3d cluster sharing the rest, all recorded.
- **R3:** 8 rows per transaction (`br100`, already in the kit). This changes the workload shape, so it is a last resort, reported as such.

**3. Phase B at A100 (`b100`)** with the chosen remedy. The rates and step durations are unchanged. The evaluator reports the highest demonstrated viable rate separately, and a step with incomplete evidence gives no conclusion at its rate. At about 1.8 KiB of disk per change, a run starting at 40 GiB is expected to reach the 25 GiB floor in the 32,000 step unless DeltaForge saturates first; that is a host limit.

**4. Phase C** at the highest viable rate, if it fits the remaining time and disk.

**Phase A at 200,000 and 300,000 tables** needs about 29 GB and 44 GB of fixture plus the 40 GiB run floor. With the A100 fixture kept, the host has about 36 GB free, so both are a host limit unless more space is made available.

**Before any of this runs (for review):**
- **Kit:**
  - the `mysql` step takes the R1/R2 settings;
  - `watch.sh` records the MySQL counters;
  - a calibration configuration.
- **Disk:** the run start floor is 42,949,672,960 bytes, and free disk is about 36.5e9 with the A100 fixture kept. A cleanup proposal is needed.
- **Clock:** a fresh eight-hour cap.

## Resumed campaign: stopped before a throughput frontier (2026-11-09)

The resumed campaign (`~/deltaforge-evidence/capacity-resume`, product `c9257fe1`) stopped during generator calibration. No throughput frontier was established.

- **`cal100`, MySQL 3 CPUs and 6 GiB, R1 caches 50,000.**
  - MySQL was OOM-killed at its limit 16 s in.
  - DeltaForge failed closed (`source_connect`).
- **`cal100m`.** Void: the campaign's Kafka had been left stopped, so the run was stopped before traffic.
- **`cal100n`, MySQL raised to 10 GiB, otherwise unchanged.**
  - MySQL peaked at 9.98 GiB and stayed at its 3 CPUs.
  - It committed 97.6% of 2,000 changes/s from 30 to 120 s.
  - DeltaForge stopped fail-closed at 124 s, before any step was measured.
- **Cause of the `cal100n` stop** (`~/deltaforge-evidence/cal100n-activation-investigation`): a behind-capture deferral of an activation window's head table does not hold back the head rows event. The lazy and FULL paths then recapture without the bounded wait, and the FULL check fails fatally.
  - The trigger was MySQL's position pair: the binlog offset ran ahead of the GTID set, by up to about 32 transactions under concurrent commits. This occurred in the rc.1 A100 run as well.
- **Limits of these results:**
  - Fail-closed protected the checkpoint, but this run failed; no recovery or reconciliation followed.
  - MySQL was resource-constrained in these configurations; the host's maximum generation capacity is unproven.

Capacity runs stay paused until the activation defect is fixed.

## Recovery check after the activation fix (2026-11-10)

The fix (#214, merged as `025373ab`) is supported by its regression tests and its core gate on `428c5b44`: a red-then-green reproduction of the `cal100n` stop, plus held-head, repeated-deferral, timeout, cancellation and FULL-fallback cases. That is the current fix evidence.

The 100,000-table recovery workload is inconclusive and is not retried:
- **`cal100r`:** a setup failure. Scenario `F1` was rendered without rate steps; no traffic ran.
- **`cal100s`:** stopped at 3 min 50 s. MySQL was OOM-killed at its 10 GiB limit, and DeltaForge then failed closed (`source_connect`). By then 39,996 of 40,000 tables were admitted. DeltaForge peaked at 927 MiB.
  - The fix's path did not occur: no behind-capture deferral and no held head.
- **Evidence:** `~/deltaforge-evidence/capacity-resume` (`RECOVERY-CRITERIA.md`, `RECOVERY-RESULT.md`).

The A100 memory confirmation (`a100c`, above) stands.

## Laptop campaign (single shared host, for review)

The laptop is the only host. One host runs:
- the generator (the harness);
- MySQL;
- Kafka;
- DeltaForge;
- the harness's own verifier.

Every result here is therefore a result of this shared laptop under the recorded allocations. None is a production capacity ceiling.

Each run's report gives the constraints of the generator, MySQL, Kafka and DeltaForge separately: offered and committed rates, CPU against each allocation, memory, MySQL table-open rates, harness CPU, lag and backlog, all from the existing samples and resource records.

**Product, harness and evaluator**
- **Product:** `deltaforge:recovery-025373ab` (image id `sha256:c3b6ba28...`, built from `025373ab`).
- **Harness:** the pinned binary (`2caef8ac...`).
- **Evaluator:** `frontier.py`, unchanged.

**Kit changes, configuration only:**
- **`render.sh`:**
  - an optional active-set fraction;
  - the configs `a003` (fixture), `g003`, `b003`, `t003-<rate>` and `k100`;
  - every earlier config renders byte-identical.
- **`campaign.sh`:**
  - a 4-hour clock (`TOTAL_CAP` 14,400 s, the last 15 minutes for the report);
  - size `003` (2,500 tables, 1,000 active, a 120 s warm-up);
  - each run's active-table count read from its configuration;
  - phase `GEN`, the calibration readout with no live rule: no R2, no generator stop and no remedy, so its `remedy: R1` label means the base configuration.
- **Validated before review:** every new config passes `fleet-harness check` and `render`, and the harness's own scenario planner gives 3 steps for `g003`, 6 for `b003` and 0 for each constant-rate run.

**Allocations:**
- MySQL: 3 CPUs, 6 GiB, default table caches. The 50,000-entry caches are never applied.
- Kafka: 2 CPUs, 3 GiB.
- DeltaForge: 4 CPUs, 6 GiB.

**Floors (unchanged):**
- disk: 42,949,672,960 bytes free to start a run, 26,843,545,600 continuously;
- memory: 4 GiB available on the host.

### Throughput track: 2,500 tables, 1,000 active

A fresh small fixture (50 databases of 50 tables). Its 1,000 active tables fit MySQL's default table caches (2,000 definitions, 4,000 open tables), so there is no table-id churn.

1. **Generator check (`g003`, phase `GEN`):** 2,000, 4,000 and 8,000 changes/s, 300 s each, at most 20 minutes.
   - **Adequate** if the readout reports no failure and keeps up (committed at least 95% of offered) at 4,000 changes/s or more.
   - **Otherwise:** the throughput track ends as a generator limit. No remedy is applied and there is no tuning round.
2. **Staircase (`b003`, phase B)** with its live evaluator: 2,000 up to 64,000 changes/s at 300 s per step. It stops at the first saturated or generator-limited step, or at the disk floor.
3. **Confirmation (`t003-<rate>`, evaluated as Phase A):** 30 minutes at a constant rate.
   - **Rate:** the staircase's highest viable step, lowered to the highest step whose projected disk use fits: rate x 1,800 s x 1,843 bytes must be at most free space at start minus the floor minus 2 GiB.
   - **When it is skipped:** if the staircase has no viable step.
   - **Window:** the Phase A evaluator judges from activation to the end of writes.

### Catalog track: the kept 100,000-table fixture at low traffic

**Run (`k100`, evaluated as Phase A):** 1,000 active tables (1%), 500 changes/s, 20 minutes. MySQL at 6 GiB with default caches. Its 1,000-table active set and default caches match the throughput track's, so a comparison with that track isolates the catalog's effect on DeltaForge: startup, activation, memory and lag.

**What it adds:** the 40,000-active, 2,000 changes/s point at 100,000 tables is the existing `a100c` result.

**200,000 and 300,000 tables are a host limit here.** The fixture needs about 29.3e9 or 44.0e9 bytes beyond the 100,000-table one. With that one removed (about 16.7e9 back), the host would have about 57e9 free. The 200,000 fixture would then leave about 28e9, under the 42.9e9 run floor.

### Disk budget (bytes; 1,843 bytes per change, measured at A100)

| Item | Bytes |
|---|---|
| Free now (2026-11-10) | 43,613,179,904 |
| Standing cleanups before setup: main worktree `target/debug/{incremental,deps}`; `df-capacity-plan/target` | +2,190,761,940, +2,524,392,914; +2,341,303,156 |
| Binlog purge of the 100,000-table fixture before `k100` (standing approval; volume 16.71e9 against about 14.66e9 of tables) | about +2.0e9, measured and recorded when done |
| Expected free at setup | about 50.67e9 |
| `a003` fixture: 2,500 x 146,597 | about 0.37e9 |
| `g003`: up to 4.2M changes | up to 7.7e9; topic deleted when its verdict allows, binlogs purged before the next run |
| `b003`: cumulative by the end of each step: 2k 1.1e9, 4k 3.3e9, 8k 7.7e9, 16k 16.6e9, 32k 34.3e9 | starting near 50e9, the continuous floor stops it in the 32,000 step. That is a disk limit, reported as such, not a DeltaForge result |
| `t003-<rate>`, 30 minutes: 2k 6.6e9, 4k 13.3e9, 8k 26.5e9 | 8,000 fits only from about 55.4e9 free; otherwise the confirmation is at most 4,000 |
| `k100`: 0.6M changes | about 1.1e9 |
| `a003` fixture dropped after the throughput track (`drop 003`, evidence verified first) | about +0.4e9 back |

**Not touched:** the Docker images, the remaining build cache, the earlier campaigns' kept containers, volumes and topics, and all evidence.

### Schedule (at most 4 hours; every run bounded by its own deadline, and the clock refuses a run that would cross 3 h 45 min)

| Start | Step | Bound |
|---|---|---|
| 0:00 | Standing cleanups. Rename the kept containers out of the way (non-destructive): `cap-kafka` to `cap-kafka-capacity-resume`, `cap-mysql` to `cap-mysql-a100kept`. `setup`, `mysql 003`, `fixture 003` | 10 min |
| 0:10 | `run GEN g003` | 20 min |
| 0:30 | If adequate: purge binlogs, `run B b003` | at most 77 min (warm-up 120 + 1,800 + drain 1,800 + 900) |
| 1:47 | If the staircase found a viable step: purge binlogs, `run A t003-<rate>` | at most 55 min (1,800 + 600 + 900) |
| 2:42 | `drop 003`. Rename `cap-mysql-a100kept` back to `cap-mysql`. Restore its memory limit to 6 GiB (it was raised to 10 GiB for `cal100m`). `adopt 100`, purge binlogs | 8 min |
| 2:50 | `run A k100` | at most 45 min (1,200 + 600 + 900) |
| 3:35 | Report | 25 min |

**Stop rules:**
- Any correctness or health failure, including a fail-closed pipeline, stops the campaign with its evidence kept. It is not retried.
- A container OOM, or a floor or deadline stop, ends that run as an environment limit. It is not retried either.
- If the generator check fails, the throughput track ends with no tuning round, and the catalog track still runs.

### Commands (after review; `C=~/deltaforge-evidence/capacity-laptop`, `K=<#209 checkout>/crates/fleet-harness/capacity`)

```
docker rename cap-kafka cap-kafka-capacity-resume
docker rename cap-mysql cap-mysql-a100kept
CAP_IMAGE=deltaforge:recovery-025373ab $K/campaign.sh setup $C ~/deltaforge-evidence/capacity-resume/bin/fleet-harness
export CAMPAIGN_DIR=$C
$C/kit/campaign.sh mysql 003
$C/kit/campaign.sh fixture 003
$C/kit/campaign.sh run GEN g003
$C/kit/campaign.sh run B b003                  # only if g003 is adequate
$C/kit/campaign.sh run A t003-<rate>           # rate by the rule above
$C/kit/campaign.sh drop 003
docker rename cap-mysql-a100kept cap-mysql
docker update --memory 6g --memory-swap 12g cap-mysql
$C/kit/campaign.sh adopt 100 d6fcbc43b0b471e34910f9a5a0c102f482b52694a07001135968cc1be1eaaac2
$C/kit/campaign.sh run A k100
```

**Binlog purges between runs** use the standing approval for the campaign's own fixture (`PURGE BINARY LOGS TO` the current file), recorded with the inventory, GTID state and free bytes.
