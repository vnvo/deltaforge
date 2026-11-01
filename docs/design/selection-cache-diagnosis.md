# Selection-Cache Diagnosis - Measurement Instrumentation

**Status:** Implemented on branch `perf-cache-diagnosis` (instrumentation only). No measurement has run with it; the plan in section 5 is for review.
**Date:** 2026-11-02
**Scope:** the residual post-activation latency seen in stage 2 at every DeltaForge point whose active set exceeds the selection cache's 4,096-timeline bound (`crates/perf-harness/results/scale-stage2-plan.md`; evidence `scale-stage2-review-775c6ec3*`). Stage 2 showed the correlation (evictions are zero below the bound and 119k to 547k above it), not the cause: history reads were not observable in the release build and the counters were whole-run totals. Activation interference (stream-wide blocking behind the activation-window chain) is a separate problem and is not instrumented here.

## 0. Rulings this implements

1. A separate PR with normal review, focused tests, CI and a full core gate.
2. Instrumentation and the cache override exist only with the `perf-phases` feature; a default build ignores the override.
3. Diagnostic builds have their own identities, separate from the pinned stage 2 binaries, which stay untouched with their evidence. Feature flags, configuration and binary hashes are recorded; overhead is checked against the pinned build.
4. Timeline misses, selection misses and actual store loads are distinguished. Sampled counters cover the post-activation interval, with the boundary uncertainty stated.
5. Capacity settings interleave on identical workloads with fresh state. A partial improvement is a possible result; each setting is compared with the matching workload and backend baseline, not a universal target.

## 1. What a row's schema selection does

`select_for_rows` (`crates/sources/src/mysql/mysql_selection.rs`), for each rows event:

1. **Timeline**: the table's validated history (`timeline`). A miss reads it from the state store (`install`: `mysql_history::read_each`, batched with upcoming tables the cache has room for), then reads ahead, in one batch, the versions the installed tables' last proofs name.
2. **Selection**: the schema selected for this position (table, decisive record, TableMap signature). A hit returns it.
3. **Version**: on a selection miss, the registered version. A miss reads it from the store (`get_version`).

Activation windows also read timelines ahead (`load_timelines`) for the tables they admit. All three kinds of entry share one LRU bounded by 4,096 tables and 256 MiB; stage 2 held 13.6 to 15.7 MiB at the table bound, so the table bound is the one that binds.

## 2. Instrumentation (`perf-phases` only)

| Metric | Labels | Meaning |
|---|---|---|
| `deltaforge_perf_selection_lookups_total` | `entry` = `timeline` \| `selection` \| `version`, `result` = `hit` \| `miss` | One lookup at steps 1 to 3 above. A timeline miss is a table whose history is not cached; a selection miss is a position/TableMap not yet selected; a version miss is a version not cached. |
| `deltaforge_perf_selection_evictions_total` | `entry` | An entry evicted to make room, by kind (the default build's `deltaforge_mysql_selection_cache_evictions_total` stays, unlabelled by kind). |
| `deltaforge_perf_selection_store_reads_total` | `read` = `history` \| `history_prefetch` \| `versions` \| `version` | A state-store read a miss caused: timelines a row needed, timelines a window read ahead, versions read ahead with timelines, one version a selection needed. |
| `deltaforge_perf_selection_store_tables_total` | `read` | Tables (or versions) those reads asked for. |
| `deltaforge_perf_selection_store_bytes_total` | `read` | Stored bytes of the timelines installed. |
| `deltaforge_perf_phase_latency_seconds` | `phase` = `history_load` \| `history_prefetch` \| `version_load` (new) | Time of each read (existing histogram; `history_load` existed). |
| `deltaforge_perf_selection_cache_limit` | `bound` = `tables` \| `bytes` | The bounds the cache was built with. |

A store load is therefore a `store_reads_total` increment: a timeline miss always causes one `history` read (with other tables batched in); a selection miss causes a `version` read only when its version is not cached.

## 3. Cache override (`perf-phases` only)

`DELTAFORGE_PERF_SELECTION_CACHE_TABLES` and `DELTAFORGE_PERF_SELECTION_CACHE_BYTES` set the bounds of a measurement build's cache (either may be left out). Precedence: a test's bounds (`stream_probe`), then these variables, then the code's constants. A default build compiles the override out and ignores the variables (`perf_default_builds_ignore_the_cache_override`). An unparseable or zero value is ignored with an error log, and the published bound shows what applied.

## 4. Harness (`crates/perf-harness`, scaling study)

- **`--scale-cache-tables N1,N2,...`**: each DeltaForge run of a point runs once per bound, in repetition order ascending for even repetitions and descending for odd ones, each on a fresh runner state (its own state store or database, as every run). The run's tag carries `-c<N>`; `runs.jsonl` records `cache_tables`. Before a run measures, the runner must report that bound (`deltaforge_perf_selection_cache_limit{bound="tables"}`); otherwise the run fails as an invocation error, so a default build is never measured as if it had the bound.
- **Samples**: every DeltaForge run writes `samples.jsonl`, one line per metrics scrape the harness already makes every 250 ms during the load and the drain (no extra scrape): the time on the load clock, the phase (`load` or `drain`), the runner's CPU seconds so far, and the series of section 2 plus the cache gauges, eviction, window and proof counters. `phases.json` gains `load_start_unix_ms`, to align the proof trace's wall clock.
- **Identity**: `runs.jsonl` records each run's binary SHA-256 and the bounds the runner reported (`cache_limit`, absent from a default build).
- **Boundary uncertainty**: an interval's counter delta is the difference of the samples bracketing it. Each end is uncertain by one sampling interval (250 ms) plus the scrape time; the interval's own boundaries (last activation, catch-up) come from the row timestamps. Reported deltas state the bracketing samples' times.

## 5. Measurement plan (for review; nothing has run)

**Builds** (separate identities; the stage 2 binaries in `target/perf/stage2-builds-775c6ec3` and their evidence are not touched):
- `target/perf/diag-builds-<M>-perf-phases/`: `runner` built with `cargo build --release -p runner --features perf-phases` and `canonical-perf` from the same clean revision `<M>` (this PR's merge commit), with `build-identity.txt` (revision, features, commands, SHA-256 of both).
- The pinned stage 2 runner `dbc15402...` (775c6ec3) as the overhead baseline.

**Step 1, overhead check** (before any diagnosis run): pinned runner and diagnostic runner (no override) interleaved in one invocation (`--binary stage2=... --binary diag=...`), 5 repetitions each, on t1000-a20-burst-o0 (no eviction) and t10000-a50-steady-o0 (evictions), SQLite store. Acceptance for using the diagnostic build: post-activation p50 and p99, CPU and evictions within the points' observed repetition spread; otherwise the overhead is reported and the plan returns for review.

**Step 2, capacity intervention** (DeltaForge diagnostic build only, 5 repetitions per bound, interleaved, fresh state):

| Point | Store | Bounds | Purpose |
|---|---|---|---|
| t10000-a50-steady-o0 | SQLite | 4096, 8192 | removes evictions (5,000 active) |
| t40000-a20-steady-o0 | SQLite | 4096, 8192 | removes evictions (8,000 active) |
| t10000-a20-burst-o0 | SQLite | 1024, 4096 | induces evictions where none occur (2,000 active) |
| t10000-a50-burst-o0 | PostgreSQL | 4096, 8192 | the PostgreSQL store's amplification |

Each bound is compared with the same point, store and harness at 4,096 (the matching baseline), not with a fixed latency. Outcomes: evictions and store loads fall to about zero and the post-activation latency returns to the point's no-eviction level (cache churn explains it); evictions fall but latency stays partly or wholly (churn explains part or none); lowering the bound reproduces the penalty at t10000-a20 (supports causation).

**Step 3, longer runs** (after step 2): t40000-a20-steady-o0 and t40000-a50-burst-o0, SQLite, bounds 4096 and 32768 (above the 20,000 active tables), `--scale-seconds 240`, 5 repetitions. A run counts only with at least 80 s of post-activation traffic outside catch-up (by commit and arrival time); the analysis reports the usable seconds and rows of every run and leaves out, with the reason, a run below 80 s.

Runs: step 1 about 20, step 2 about 40, step 3 about 20.
