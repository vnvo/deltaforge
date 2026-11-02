# Selection-Cache Diagnosis - Measurement Instrumentation

**Status:** Implemented on branch `perf-cache-diagnosis` (instrumentation only). No measurement has run with it; the plan in section 5 is for review.
**Date:** 2026-11-02
**Scope:** the residual post-activation latency seen in stage 2 at every DeltaForge point whose active set exceeds the selection cache's 4,096-timeline bound (`crates/perf-harness/results/scale-stage2-plan.md`; evidence `scale-stage2-review-775c6ec3*`). Stage 2 showed the correlation (evictions are zero below the bound and 119k to 547k above it), not the cause: history reads were not observable in the release build and the counters were whole-run totals. Activation interference (stream-wide blocking behind the activation-window chain) is a separate problem and is not instrumented here.

## 0. Rulings this implements

1. A separate PR with normal review, focused tests, CI and a full core gate.
2. Instrumentation and the cache override exist only with the `perf-phases` feature; a default build ignores the override.
3. Diagnostic builds have their own identities, separate from the pinned stage 2 binaries, which stay untouched with their evidence. Feature flags, configuration and binary hashes are recorded.
4. Timeline misses, selection misses and actual store loads are distinguished. Sampled counters cover the post-activation interval, with the boundary uncertainty stated.
5. Capacity settings interleave on identical workloads with fresh state. A partial improvement is a possible result; each setting is compared with the matching workload and backend baseline, not a universal target.
6. Instrumentation overhead is measured between a default and a `perf-phases` build of the same revision, with the same harness, against tolerances fixed before running (section 5). The pinned 775c6ec3 runner is compared separately, as a historical bridge only: it predates the start-up capture-boundary fix and cannot isolate instrumentation overhead.
7. Counters are named for what they count (section 2.1); no store-statement latency is claimed from them.
8. Every promised duration has a timing guard, including the batched version read-ahead.
9. Interval uncertainty uses each sample's recorded scrape times, not a nominal period; intervals with missing or reset counters are rejected.
10. A long run with under 80 usable seconds is reported as an insufficient observation, never silently left out.

## 1. What a row's schema selection does

`select_for_rows` (`crates/sources/src/mysql/mysql_selection.rs`), for each rows event:

1. **Timeline**: the table's validated history (`timeline`). A miss reads it from the state store (`install`: `mysql_history::read_each`, batched with upcoming tables the cache has room for), validates and installs it, then reads ahead, in one batched call, the versions the installed tables' last proofs name.
2. **Selection**: the schema selected for this position (table, decisive record, TableMap signature). A hit returns it.
3. **Version**: on a selection miss, the registered version. A miss reads it from the store (`get_version`).

Activation windows also read timelines ahead (`load_timelines`) for the tables they admit. All three kinds of entry share one LRU bounded by 4,096 tables and 256 MiB; stage 2 held 13.6 to 15.7 MiB at the table bound, so the table bound is the one that binds.

## 2. Instrumentation (`perf-phases` only)

| Metric | Labels | Meaning |
|---|---|---|
| `deltaforge_perf_selection_lookups_total` | `entry` = `timeline` \| `selection` \| `version`, `result` = `hit` \| `miss` | One lookup at steps 1 to 3 above. A timeline miss is a table whose history is not cached; a selection miss is a position/TableMap not yet selected; a version miss is a version not cached. |
| `deltaforge_perf_selection_evictions_total` | `entry` | An entry evicted to make room, by kind (the default build's `deltaforge_mysql_selection_cache_evictions_total` stays, unlabelled by kind). |
| `deltaforge_perf_selection_store_reads_total` | `read` = `history` \| `history_prefetch` \| `versions` \| `version` | One **logical loader call** a miss caused (section 2.1): timelines a row needed, timelines a window read ahead, the batched version read-ahead, one version a selection needed. |
| `deltaforge_perf_selection_store_tables_total` | `read` | Tables (or versions) those calls asked for. |
| `deltaforge_perf_selection_store_bytes_total` | `read` | For `history` and `history_prefetch`: the timeline-byte accounting of the timelines the call returned (section 2.1). Zero for `versions` and `version`. |
| `deltaforge_perf_phase_latency_seconds` | `phase` = `history_load` \| `history_prefetch` \| `version_prefetch` \| `version_load` (the last three new) | Elapsed time of each call (existing histogram; `history_load` existed). Nesting in section 2.2. |
| `deltaforge_perf_selection_cache_limit` | `bound` = `tables` \| `bytes` | The bounds the cache was built with. |

A timeline miss always causes one `history` call (with other tables batched in); a selection miss causes a `version` call only when its version is not cached.

### 2.1 What the store counters do not measure

- **Calls, not backend reads or SQL statements.** A `history` or `history_prefetch` call is one `mysql_history::read_each`, which issues bounded batches of at most `BATCH_MAX_TABLES` tables plus the barrier streams not yet read, and reads alone a table its batch could not take whole. A `versions` call is one `get_versions`: two batched backend reads, plus one more read for each version that is not its stream's latest. A `version` call is one `get_version`: an index lookup and a log read. The number of backend reads or statements per call is not counted.
- **Calls attempted, not calls that succeeded.** A `versions` call is counted before it runs; if it fails (it is best-effort, and refuses more than `VERSION_HASHES_MAX` versions), it still counts, and the versions are read later by `version` calls.
- **Accounting bytes, not transferred or installed bytes.** History bytes are the `bytes` the history reader reports for each timeline it returned whole (its accounting against the timeline byte limit), summed over the call. They include timelines that then fail validation and are not installed, and they are not the bytes moved over the wire or held in memory. Version calls report zero bytes: version sizes are not measured.
- **No store-statement latency.** The counters and phase timings describe loader calls. They support statements such as "N history calls of T tables took X s in total"; they do not support per-statement or per-backend-read latency.

### 2.2 What the phase timings cover

- `history_load` spans a row's whole `install`: the history read, decoding, validation, installing into the cache and the batched version read-ahead.
- `history_prefetch` spans the same work for an activation window's read-ahead.
- `version_prefetch` spans the batched `get_versions` call and installing the versions it returned. It is nested inside `history_load` or `history_prefetch`, so the two must not be added together; the history read and validation alone are the outer timing minus the inner one, per call.
- `version_load` spans one `get_version` call and decoding its schema.

`version_prefetch` is checked by `perf_the_version_read_ahead_is_timed_and_counted`, which drives a real timeline read on an in-memory store and fails without the guard.

## 3. Cache override (`perf-phases` only)

`DELTAFORGE_PERF_SELECTION_CACHE_TABLES` and `DELTAFORGE_PERF_SELECTION_CACHE_BYTES` set the bounds of a measurement build's cache (either may be left out). Precedence: a test's bounds (`stream_probe`), then these variables, then the code's constants. A default build compiles the override out and ignores the variables (`perf_default_builds_ignore_the_cache_override`). An unparseable or zero value is ignored with an error log, and the published bound shows what applied.

## 4. Harness (`crates/perf-harness`, scaling study)

- **`--scale-cache-tables N1,N2,...`**: each DeltaForge run of a point runs once per bound, in repetition order ascending for even repetitions and descending for odd ones, each on a fresh runner state (its own state store or database, as every run). The run's tag carries `-c<N>`; `runs.jsonl` records `cache_tables`. Before a run measures, the runner must report that bound (`deltaforge_perf_selection_cache_limit{bound="tables"}`); otherwise the run fails as an invocation error, so a default build is never measured as if it had the bound.
- **Samples**: every DeltaForge run writes `samples.jsonl`, one line per metrics scrape the harness already makes during the load and the drain (no extra scrape): `t_start_ms` and `t_end_ms` (when the scrape started and ended, on the load clock), the phase (`load` or `drain`), the runner's CPU seconds so far (read after the scrape), and the series of section 2 plus the cache gauges, eviction, window and proof counters. A failed scrape is a line with `scrape_failed` and no values. `phases.json` gains `load_start_unix_ms`, to align the proof trace's wall clock.
- **Identity**: `runs.jsonl` records each run's binary SHA-256 and the bounds the runner reported (`cache_limit`, absent from a default build).
- **Warm-ups**: every warm-up's full record goes to `warmups.jsonl`, marked `"warmup": true`. It has the same fields as a measured run: correctness and ordering counts, the drain verdict and durable frontier, touched and activated tables, every counter delta including the measured-failure counters, the binary SHA-256, and the requested and reported bounds. Warm-ups are never measured results. `runs.jsonl`, `scale-summary.md` and its run counts take only measured runs.
- **Exported counters**: the runner exports a counter from its first increment on, so a study counter it never incremented reads 0 in a result's `deltaforge` deltas without having been read. `deltaforge_exported` lists the study counters the closing scrape found (`null` when that scrape failed); a counter missing from it is unknown, not 0.
- **Early exits**: a run that exits with an error keeps the error chain and its last retained sample (`last_sample`, `null` when it failed before sampling). Samples carry the measured-failure counters and the pipeline's status, so the last one holds the run's failure counters as of its last scrape. The run's directory keeps the runner's log.

### 4.1 Sampling gaps and interval rules

The loop sleeps 250 ms after its other work (such as connection and memory sampling, the scrape and, during the load, latency judging and the binlog size query), so consecutive samples are at least 250 ms apart and often more. The analysis therefore uses the recorded times, not a nominal period:

- An interval's counter delta is the difference of the samples bracketing it: the last sample whose `t_end_ms` is at or before the interval's start, and the first whose `t_start_ms` is at or after its end. Each end's uncertainty is the gap from the bracketing sample to the boundary plus that sample's scrape duration (`t_end_ms - t_start_ms`); both are reported with every delta, as are the largest gap between consecutive samples in the interval and the number of samples.
- The interval's own boundaries (last activation, catch-up) come from the row timestamps.
- An interval is **rejected**, with the reason reported, when a bracketing sample is missing (none before or after it), when a `scrape_failed` line falls inside it, when a counter it uses is absent from a bracketing sample, or when any counter it uses decreases between consecutive samples (a reset). Rejected intervals are counted in the results, not dropped.

## 5. Measurement plan (for review; nothing has run)

**Builds** (separate identities; the stage 2 binaries in `target/perf/stage2-builds-775c6ec3` and their evidence are not touched). From one clean checkout of revision `<M>` (this PR's merge commit), into `target/perf/diag-builds-<M>/`:
- `default/runner`: `cargo build --release -p runner`;
- `perf-phases/runner`: `cargo build --release -p runner --features perf-phases`;
- `canonical-perf` and the harness from the same revision;
- `build-identity.txt`: revision, clean-tree check, each command with its features, and the SHA-256 of every binary.

Every invocation below uses the harness built at `<M>`, SQLite store unless stated, and records the binary hashes per run.

**Step 1, instrumentation overhead** (before any diagnosis run): `--binary default=... --binary diag=...` (both from `<M>`, no override), interleaved in one invocation with alternating order, 5 repetitions each, on t1000-a20-burst-o0 (no evictions) and t10000-a50-steady-o0 (evictions). 20 runs.

Tolerances, fixed now. Each metric compares the median of the five `diag` runs with the median of the five `default` runs, at each point:

| Metric | Tolerance (diag minus default) | Stage 2 range over 5 reps (775c6ec3): t10000-a50-steady, then t1000-a20-burst |
|---|---|---|
| Post-activation p50 (after last activation, outside catch-up) | at most +3% or +1 ms, whichever is larger | 0.2 ms (51.9-52.1) and 0.4 ms (27.9-28.3) |
| Post-activation p99 | at most +3% or +2 ms, whichever is larger | 0.3 ms (87.8-88.1) and 0.4 ms (46.9-47.3) |
| Runner CPU seconds | at most +5% | 4.9% (22.56-23.67 s) and 1.7% (9.22-9.38 s) |
| Selection-cache evictions | within 2% (both zero at t1000-a20) | 1.1% (119,643-120,974) and 0 |

The tolerances are one-sided for latency and CPU. If diag is lower than default by more than the same margin, that is reported as an anomaly and the plan returns for review. Any metric outside its tolerance at either point means the overhead is reported, and the plan returns for review before any diagnosis run. Within tolerance, the diagnostic build's measurements stand for the default build's behaviour at `<M>`, within those margins.

**Step 1b, historical bridge** (descriptive; no acceptance criterion): `--binary stage2=<pinned 775c6ec3 runner> --binary default=<M default>`, same two points, 5 repetitions, interleaved. 20 runs. It relates step 2's results to stage 2's evidence. The differences combine the start-up capture-boundary fix and every other change between the revisions. The harness at `<M>` waits for the capture boundary, which the 775c6ec3 runner does not publish. So the bridge is not used to attribute or bound instrumentation overhead.

**Step 2, capacity intervention** (diagnostic build only, 5 repetitions per bound, interleaved, fresh state):

| Point | Store | Bounds | Purpose |
|---|---|---|---|
| t10000-a50-steady-o0 | SQLite | 4096, 8192 | removes evictions (5,000 active) |
| t40000-a20-steady-o0 | SQLite | 4096, 8192 | removes evictions (8,000 active) |
| t10000-a20-burst-o0 | SQLite | 1024, 4096 | induces evictions where none occur (2,000 active) |
| t10000-a50-burst-o0 | PostgreSQL | 4096, 8192 | the PostgreSQL store's amplification |

Each bound is compared with the same point, store, harness and build at 4,096 (the matching baseline), not with a fixed latency. Possible outcomes:
- Evictions and store loads fall to about zero, and the post-activation latency returns to the point's no-eviction level: cache churn explains it.
- Evictions fall but latency stays, partly or wholly: churn explains part of it or none.
- Lowering the bound reproduces the penalty at t10000-a20: this supports causation.

40 runs.

**Step 3, longer runs** (after step 2): t40000-a20-steady-o0 and t40000-a50-burst-o0, SQLite, bounds 4096 and 32768 (above the 20,000 active tables), `--scale-seconds 240`, 5 repetitions, diagnostic build. 20 runs.

Every run is reported with its usable post-activation seconds and rows: traffic after the last activation and outside catch-up, by commit and arrival time.
- **Sufficient:** a run with at least 80 usable seconds counts toward the step's comparisons.
- **Insufficient:** a run below 80 s is reported as an insufficient observation in the same table, with its usable seconds, rows, and the latency and counter values over what it has. It is marked as such and kept out of the medians, never omitted.
- **Conclusions:** if fewer than three runs of a point and bound are sufficient, the step reports that and draws no conclusion for that pair.

**Runs:** step 1: 20; step 1b: 20; step 2: 40; step 3: 20.
