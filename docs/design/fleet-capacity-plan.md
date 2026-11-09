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

The staircase is the harness's F1 scenario (`traffic.rate_steps`, `traffic.rate_step_secs`), so the working set activates once per run.

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

DeltaForge logs a `window` proof-trace record for each activation window. Its `tables` field counts the tables the window activated, with deferred tables excluded, and the record carries the commit time.

- **Completion** is the commit time of the window at which the running sum reaches the active set: 40,000, 80,000 or 120,000 tables, exactly ceil(0.4 x catalog).
- **Not established:** a final sum different from the active set leaves completion not established, and that run's throughput is not evaluated.
- **Checked on G3:** in G3's S3 log, the window sums equal its 400-table active set.
- **Where it is read:** from the container's log (`df.log`, saved per run). The harness's trace file keeps only proof records, and they carry no time.

How each phase uses it:

- **Phase A:** the measured window is from completion to the end of writes, and must be at least 600 s. Otherwise the size did not activate within its allowance and is not viable.
- **Phase B:** a step is measured only if completion precedes its measured window.
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

**Live mode (Phase B).** The evaluator follows the run and sends SIGINT after the first measured step that is saturated or generator-limited. The harness's abort path still stops the writers, drains, and checks completeness.

**Self-tests on synthetic runs:**

- **Saturation:** a staircase saturated at 32,000 was reported viable through 16,000, saturated at 32,000, and a DeltaForge limit.
- **Activation:** an over-counted activation was inconclusive.
- **Stops:** a deadline stop was evaluated; an aborted run without a stop marker failed.
- **Live:** live mode stopped the run in the saturated step.

### Floors and deadlines (enforced by `campaign.sh`)

- **Disk.** Every fixture build and every run checks two things:
  - **At the start:** at least 53,687,091,200 bytes free on the campaign directory's and Docker's filesystems (byte-exact).
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
   - **Fit check:** `mysql` refuses a size whose fixture, at A100's measured bytes per table, would leave less than 50 GiB free.
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
- `verdict.json`, `end-state.json`, the container inspections;
- `MANIFEST.sha256`.

**Per campaign:** `campaign.log`, `clock.log`, `identity.txt`, `IDENTITY.sha256` and the fixture logs.

**Containers and topics:** DeltaForge containers are kept stopped (`cap-df-<run>`). Each run's Kafka topic is deleted after its completeness check.

### Before the campaign: inputs to review

1. **Disk does not reach the start floor today.**
   - **Free now:** 14.8 GB. The standing approval (`deltaforge-build/target/debug/{incremental,deps}`, 30.1 GB) brings it to 44.9 GB (41.8 GiB).
   - **Needed before each size's fixture**, at about 110 KiB per empty table (measured at A100), with the previous size dropped: about 60.5 GiB for `a100`, 71 GiB for `a200`, 82 GiB for `a300`.
   - **Candidate removals needing approval:** G3's stopped `g3-mysql` (`8c2583bcb53e`, volume `g3-mysql`, 14.99 GB) and `g3-kafka` (`9eae6edcb912`, volume `47d5feb87e0e`, 4.2 GB). G3 is accepted and its evidence is kept in `~/deltaforge-evidence/qualification-42d3f77e`. Also `target/release` (2.66 GB), after the harness copy.
   - **With those removals:** about 62 GiB, so A100 fits and A200 does not under the rule as implemented. No further cleanup is proposed.
2. **Reading of the 50 GiB start floor.** As implemented, it applies to every invocation. If it applied only at each size's fixture build, with runs bounded by the 25 GiB floor, A200 would also fit (about 39 GiB free after its fixture). This needs a decision.
3. **Removals inside the campaign:** approval for `campaign.sh drop <size>` (the campaign's own MySQL container and volume, by the ids logged at creation) and for deleting each checked run's topic.
4. **Phase B is likely to end on disk.** At 1.8 KiB of disk per committed change (G3 A2 and A3), a run starting at 50 GiB has 25 GiB of headroom, about 14.5 million changes. At A100, the warm-up and the steps through 16,000 take about 10.6 million. Unless DeltaForge saturates earlier, the staircase is expected to reach the disk floor in the 32,000 step, which is a host limit.
5. **Host sharing:** the k3d test cluster runs on this host and is allowed by the preflight, which records it. Stopping it is your call.
